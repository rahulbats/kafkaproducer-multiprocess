import unittest
import asyncio
import json
import os
from kafka_producer import  produce_with_dlq, AsyncKafkaProducer
from kafka_consumer import AsyncKafkaConsumer
import threading
import jq

class TestKafka(unittest.TestCase):


    
        
    def runloop(self, producer: AsyncKafkaProducer, name: str):
        while True:                                          
            asyncio.run(
                        produce_with_dlq(producer=producer, topic="events.user", 
                                                value=json.loads(jq.text( '{ "name": . , "favorite_color": "multi", "address": { "state": "texas", "street": "bayonet" }, "encrypted_fields": [], "encryption_key": "rahul" }', name)),
                                            
                                                ))



    def test_producer_continuos_2_threads(self):

        """
         environment variable 
            AUTH : format url=[BOOTSTRAP_URL] key=[KEY] secret=[SECRET]
            SCHEMA_REGISTRY_AUTH : format url=[SR_URL] key=[KEY] secret=[SECRET]
        """
        
        
        producer=AsyncKafkaProducer(
                                                value_schema="user", 
                                                auth=os.getenv("AUTH"),
                                                schema_registry_auth=os.getenv("SCHEMA_REGISTRY_AUTH"),
                                                linger_ms='500',
                                                batch_num_messages=str(32*1024),
                                                client_id='rahul',
                                                compression_type='zstd'
                                            )
        t1 = threading.Thread(target=self.runloop, args=(producer, 'rahul'))
        t2 = threading.Thread(target=self.runloop, args=(producer, 'rahul2'))
        t1.start()
        t2.start()
        t1.join()
        t2.join()


if __name__ == '__main__':
    unittest.main()