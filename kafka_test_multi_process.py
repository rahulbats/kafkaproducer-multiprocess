import psutil
import sys
import os
import signal
import json
from multiprocessing import Process, Queue
import multiprocessing
from shared_queue import address, authkey
from multiprocessing.managers import BaseManager

def call_method_by_pid(pid,  *args, **kwargs):
    try:
        #process = psutil.Process(pid)
        # You may need to handle permission issues if the process is owned by a different user
        # Fetching the parent process's Python executable path
        #python_executable = process.exe()
        BaseManager.register("get_queue")
        manager = BaseManager(authkey=authkey, address=address)
        manager.connect()

        queue = manager.get_queue()
        print(kwargs)
        queue.put(kwargs)
        # Example: Assuming you want to call a method named 'some_method' in the process
        os.kill(pid, signal.SIGUSR1)
            
    
    except Exception as e:
        print(e)
        print("Process with PID", pid, "not found.")


if __name__ == "__main__":
    
    # Example PID
    pid_to_load =  int(sys.argv[1:][0])
    call_method_by_pid(pid_to_load, topic="events.user", 
                                                value=json.loads('{ "name": "rahul_testing9:42", "favorite_color": "multi", "address": { "state": "texas", "street": "bayonet" }, "encrypted_fields": [], "encryption_key": "rahul" }')
                                            )