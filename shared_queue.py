from multiprocessing.managers import BaseManager
from multiprocessing import Queue

address = ('127.0.0.1', 50000)  # you can change this
authkey = b"abc"  # you should change this


class SharedQueue:

    def __init__(self):
        self._queue = Queue()
        #self._queue.put("Something really important!")

    def __call__(self):
        return self._queue


if __name__ == "__main__":
    # Register our queue
    shared_queue = SharedQueue()
    BaseManager.register("get_queue", shared_queue)

    # Start server
    manager = BaseManager(address=address, authkey=authkey)
    srv = manager.get_server()
    srv.serve_forever()