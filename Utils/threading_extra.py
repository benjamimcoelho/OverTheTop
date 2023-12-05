import threading


class RWLock:
    # A lock object that allows many simultaneous "read locks", but
    # only one "write lock."

    # WARNING! : The current implementation is susceptible to starvation!

    def __str__(self):
        return f"<RWLock(#Readers {self.__readers}, {self.__read_ready})>"

    def __init__(self):
        self.__read_ready = threading.Condition(threading.Lock())
        self.__readers = 0

    def acquire_read(self):
        # Acquire a read lock. Blocks only if a thread has acquired the write lock.
        self.__read_ready.acquire()
        try:
            self.__readers += 1
        finally:
            self.__read_ready.release()

    def release_read(self):
        # Release a read lock.
        self.__read_ready.acquire()
        try:
            self.__readers -= 1
            if not self.__readers:
                self.__read_ready.notify_all()
        finally:
            self.__read_ready.release()

    def acquire_write(self):
        # Acquire a write lock. Blocks until there are no acquired read or write locks.
        self.__read_ready.acquire()
        while self.__readers > 0:
            self.__read_ready.wait()

    def release_write(self):
        # Release a write lock.
        self.__read_ready.release()
