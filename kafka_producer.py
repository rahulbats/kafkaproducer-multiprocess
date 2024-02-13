import asyncio
import atexit
import json
import os
import sys
import threading
import time
import uuid
import jsonpath_ng
from typing import Any, Awaitable, Optional, Union
from abc import ABC, abstractmethod
from enum import Enum
from confluent_kafka import Consumer, Producer, avro
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka import Producer
from ddtrace import tracer
import logging
from confluent_kafka.schema_registry.avro import AvroSerializer
from encrypter import Encryptor
from ecrypted_paths import encrypted_field_paths
import time
from datetime import datetime
from data_dog import *
from threading import Thread
from confluent_kafka import KafkaException
from multiprocessing import Process, Pipe
import signal
from shared_queue import address, authkey
from multiprocessing.managers import BaseManager


kafka_metrics_reporter = get_kafka_metrics_reporter()
kafka_instance_id = uuid.uuid4().hex.upper()



def parse(dsn) -> dict[str, str]:
    return dict([d.split("=", 1) for d in dsn.split(" ")])

def load_key_schema(name):
    return {"default": json.dumps({"name": "id", "type": "string"})}[name]

def load_value_schema(name):
    with open(os.path.join(os.path.dirname(__file__), f"avro/{name}.avsc")) as f:
        return f.read()

def get_base_settings(auth: dict, settings) -> dict[str, Any]:
    settings = {
        k.replace("_", "."): v for k, v in settings.items()
    }
    producer_conf = {'bootstrap.servers': auth["url"],
                    'security.protocol': 'SASL_SSL',
                    'sasl.mechanisms': 'PLAIN',
                    'sasl.username':auth["key"],
                    'sasl.password':auth["secret"],
                    'stats_cb': lambda metrics_json: kafka_metrics_reporter.report(
                        kafka_instance_id, metrics_json
                    ),
                    'statistics.interval.ms':'100',
                    'delivery.timeout.ms':'2000',
                    'request.timeout.ms':'2000'
                    }
    final_conf = {**producer_conf, **settings}
    return final_conf

class AbstractSerializer(ABC):
    """Abstract class for deserializing messages using a schema registry."""

    @abstractmethod
    def init_from_registry(self, auth, compression_enabled=False) -> None:
        """Initialize the serializer from the schema registry."""
        pass

    @abstractmethod
    def decode_message(self, message) -> Any:
        """Decode the message using the serializer."""
        pass



class KafkaSerializer(AbstractSerializer):
    """Process singleton, used to deserialize messages using an avro schema registry
    compression_enabled should always be false as it is not supported by this api
    """

    def __init__(self):
        self.message_serializer = None

    def init_from_registry(self, auth, compression_enabled=False) -> None:
        if self.message_serializer:
            return

        basic = f"{auth['key']}:{auth['secret']}"
        schema_registry_settings = {
            "url": auth["url"],
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": basic,
        }
        schema_registry = CachedSchemaRegistryClient(schema_registry_settings)
        self.message_serializer = MessageSerializer(schema_registry)

    def decode_message(self, message) -> Any:
        """Use the serializer if initialized, else return the message as is."""
        if self.message_serializer:
            return self.message_serializer.decode_message(message)
        else:
            return message

_kafka_serializer = KafkaSerializer()

class DeadletterException(Exception):
    pass


class MessageQueueType(Enum):
    CONFLUENT_KAFKA = 1
    AZURE_EVENTHUB = 2

    def get_serializer(self) -> AbstractSerializer:
        return _kafka_serializer

class AbstractAsyncKafkaProducer:
    def __init__(
        self,
        value_schema: str,
        *,
        auth,
        schema_registry_auth,
        should_compress=False,
        dlq_topic: Optional[str] = None,
        dlq_auth: Optional[str] = None,
        dlq_schema_registry_auth: Optional[str] = None,
        dlq_type: MessageQueueType = MessageQueueType.CONFLUENT_KAFKA,
        **settings,
    ):
        ...

    async def start(self):
        ...

    async def close(self):
        ...

    def serializer_for_queue_type(
        self, parsed_schema_registry_auth, queue_type: MessageQueueType
    ) -> Union[MessageSerializer, AvroSerializer]:
        """
        Because we use confluent_kafka for both kafka and azure eventhub DLQs, breaking this
        out into a separate function.
        """
        if queue_type == MessageQueueType.CONFLUENT_KAFKA:
            basic = f"{parsed_schema_registry_auth['key']}:{parsed_schema_registry_auth['secret']}"
            schema_registry_settings = {
                "url": parsed_schema_registry_auth["url"],
                "basic.auth.credentials.source": "USER_INFO",
                "basic.auth.user.info": basic,
            }
            return MessageSerializer(CachedSchemaRegistryClient(schema_registry_settings))
        


    @abstractmethod
    async def produce(self, topic: str, value) -> Awaitable:
        raise NotImplementedError
    
    



class AsyncKafkaProducer(AbstractAsyncKafkaProducer):
    def __init__(
        self,
        value_schema,
        *,
        auth,
        schema_registry_auth,
        should_compress=False,
        dlq_topic: Optional[str] = None,
        dlq_auth: Optional[str] = None,
        dlq_schema_registry_auth: Optional[str] = None,
        dlq_type: MessageQueueType = MessageQueueType.CONFLUENT_KAFKA,
        **settings,
    ):
        self._loop = asyncio.get_event_loop()
        #self._loop.add_signal_handler(signal.SIGUSR1, handle_signal)
        self.encrypted_field_paths=encrypted_field_paths
        
        self.encryptor = Encryptor()   
        
        auth = parse(auth)
        
        base_settings = get_base_settings(settings=settings,auth=auth)
        parsed_schema_registry_auth = parse(schema_registry_auth)

        
        self._poll_thread = Thread(target=self._poll_loop)

        """if utils.is_datadog_statsd_enabled():
            # TODO: Add kafka reporter metrics
            pass"""
        self.serializer: MessageSerializer = self.serializer_for_queue_type(
            parsed_schema_registry_auth, MessageQueueType.CONFLUENT_KAFKA
        )

        self.value_schema = avro.loads(load_value_schema(value_schema))

        self.producer =  Producer(base_settings)

       

        self.dlq_producer = None
        self.dlq_topic = dlq_topic
        if dlq_topic:
            
            self.dlq_serializer = self.serializer_for_queue_type(
                parse(dlq_schema_registry_auth), dlq_type
            )
            self.dlq_producer =  Producer(parse(dlq_auth))
        self.started = False

    def _poll_loop(self):
        while  self.started:
            self.producer.poll(0.1)
            #time.sleep(1)
            if self.dlq_topic:
                self.dlq_producer.poll(0.1)


    async def start(self):
        
        """
        Explicitly start the producer. This is done automatically by the `produce` method if needed.
        """
        """await self.producer.start()
        if self.dlq_producer:
            await self.dlq_producer.start()"""
        await self.encryptor.create_metadata("openAI")
        self.started = True
        #self._loop = asyncio.get_event_loop()
        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    async def close(self):
        self.started = False
        self.producer.flush()
       
        if(self.dlq_producer is not None):
            self.dlq_producer.flush()
            
        self._poll_thread.join()

        """self.producer.flush()
    
        #await self.producer.stop()
        if self.dlq_producer:
            self.dlq_producer.flush()
            await self.dlq_producer.stop()"""

    async def encrypt_fields(self,topic:str, obj: dict, encrypted_field_paths: dict, encryptor: Encryptor)->dict:
        for encrypted_field in encrypted_field_paths[topic]:
                jsonpath_expr = jsonpath_ng.parse(encrypted_field)
                matches = jsonpath_expr.find(obj)
                if ( len(matches) > 0):
                    matched_field_to_be_decrypted = matches[0]
                    encrypted_value = await encryptor.encrypt(matched_field_to_be_decrypted,"openAI")
                    jsonpath_expr.update(obj, encrypted_value)
        #print("encrypted version"+json.dumps(obj))
        #json.dumps(obj)
        return obj


    async def produce(self, topic: str, value) -> Awaitable:
        """
        Producer can block when fetching or updating a schema on the registry.
        The schema is cached locally once the schema is sync'ed.

        Returns a future which can be waited upon for confirmation or error of kafka send.
        """
        
        if not self.started:
            await self.start()
        #if(key is not None):
         #   key= self.serializer.encode_record_with_schema(topic, load_key_schema(), key)    
        encrypted_value = await self.encrypt_fields(obj=value,topic=topic, encrypted_field_paths=self.encrypted_field_paths,encryptor=self.encryptor) 
        value = self.serializer.encode_record_with_schema(topic, self.value_schema, encrypted_value)

        result = self._loop.create_future()
        print("producing")
        def ack(err, msg):
            if err:
                print("error")
                #self.dlq_producer.produce(topic, value, on_delivery=ack)
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                print("success")
                self._loop.call_soon_threadsafe(result.set_result, msg)
        try:        
            self.producer.produce(topic, value, on_delivery=ack)
        except Exception as e:
            print(e)
        print("after producing")
        return result
       
         


async def produce_with_dlq(
    producer: AsyncKafkaProducer,
    topic: str,
    value,
    key=None
):
    try:
         return await producer.produce(topic,  value)
         #return await ( await producer.produce(topic, key, value))
         #await producer.close()
         
    except Exception as e:
        if not hasattr(producer, "dlq_producer") or not producer.dlq_producer:
            raise e
        try:
            logging.exception(
                "Failed to produce message to topic",
                extra={
                    "topic": topic,
                    "error": e,
                },
            )
            """datadog.statsd.increment(
                "event_logging.produce_with_dlq.count",
                tags=[f"topic:{topic}"],
            )"""
            # call .start in produce()
            # NOTE: Do not compress the value when sending to DLQ for now
            if isinstance(producer.dlq_serializer, MessageSerializer):
                value = producer.dlq_serializer.encode_record_with_schema(
                    producer.dlq_topic, producer.value_schema, value
                )
            elif isinstance(producer.dlq_serializer, AvroSerializer):
                value = producer.dlq_serializer.serialize(value, schema=producer.value_schema)
            """with datadog.statsd.timed("event_logging.produce_with_dlq.latency"):
                await (await producer.dlq_producer.send(producer.dlq_topic, value))"""
        except Exception as dlq_e:
            error_message = f"Failed to produce message to DLQ topic {producer.dlq_topic}"
            """datadog.statsd.increment(
                "event_logging.produce_with_dlq.error",
                tags=[f"topic:{topic}"],
            )"""
            logging.exception(error_message)
            raise DeadletterException(error_message) from dlq_e    
            

# Define a function to handle the signal
def handle_signal(signum, frame):
    if signum == signal.SIGUSR1:
        print("Received SIGUSR1 signal inside producer.")
        try:
                print("ID of shared_queue in producer:", id(queue))
                # Process the arguments from the queue
                while not queue.empty():
                    arguments = queue.get()
                    print(arguments)
                    
                    result=asyncio.run( producer.produce(topic=arguments['topic'],  value=arguments['value']) )
                
                #return await ( await producer.produce(topic, key, value))
                #await producer.close()
         
        except Exception as e:
                print(e)
                if not hasattr(producer, "dlq_producer") or not producer.dlq_producer:
                    raise e
                try:
                    logging.exception(
                        "Failed to produce message to topic",
                        extra={
                            "topic": topic,
                            "error": e,
                        },
                    )
                    """datadog.statsd.increment(
                        "event_logging.produce_with_dlq.count",
                        tags=[f"topic:{topic}"],
                    )"""
                    # call .start in produce()
                    # NOTE: Do not compress the value when sending to DLQ for now
                    if isinstance(producer.dlq_serializer, MessageSerializer):
                        value = producer.dlq_serializer.encode_record_with_schema(
                            producer.dlq_topic, producer.value_schema, value
                        )
                    elif isinstance(producer.dlq_serializer, AvroSerializer):
                        value = producer.dlq_serializer.serialize(value, schema=producer.value_schema)
                    """with datadog.statsd.timed("event_logging.produce_with_dlq.latency"):
                        await (await producer.dlq_producer.send(producer.dlq_topic, value))"""
                except Exception as dlq_e:
                    error_message = f"Failed to produce message to DLQ topic {producer.dlq_topic}"
                    """datadog.statsd.increment(
                        "event_logging.produce_with_dlq.error",
                        tags=[f"topic:{topic}"],
                    )"""
                    logging.exception(error_message)
                    raise DeadletterException(error_message) from dlq_e    
                    


# Set up signal handling
signal.signal(signal.SIGUSR1, handle_signal)
    
if __name__ == '__main__':
    BaseManager.register("get_queue")
    manager = BaseManager(authkey=authkey, address=address)
    manager.connect()

    queue = manager.get_queue()
    producer=AsyncKafkaProducer(
                                                value_schema="user", 
                                                auth=os.getenv("AUTH"),
                                                schema_registry_auth=os.getenv("SCHEMA_REGISTRY_AUTH"),
                                                linger_ms='500',
                                                batch_num_messages=str(32*1024),
                                                client_id='rahul',
                                                compression_type='zstd'
                                            )
    asyncio.run(producer.start())
    print("PID is "+str(os.getpid()))
