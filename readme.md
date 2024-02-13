*to run the producer 

** First export kafka credentials like this
``` export AUTH="url=BOOTSTRAP_URL key=KEY secret=SECRET" ```

**  export SR credentials like this
``` export SCHEMA_REGISTRY_AUTH="url=SR_URL key=KEY secret=SECRET" ```

* run the queue manager server 
```python3 shared_queue.py```

* run the producer and PID is printed on console 
```python3 kafka_producer.py```

* run the test to produce with the PID from above 
```python3 kafka_test_multi_process.py PID```