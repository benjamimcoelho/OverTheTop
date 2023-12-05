import logging
import pickle
import socket
import threading as thr

from OverTheTop import defaults as c


class Flow_Handler:
    # EXTRAS:
    #       Socket security is on the waiting room and probably wont be developed
    #       however, if this is not true, it's implementation should be done here

    # Note: These buffers provide scale to the transport layer (allows multiple dispatchers and forwarders)
    #       and make handling packets easier in general

    # Input buffer
    # Dispatchers will place Datagram Packets in here
    in_buff = []
    in_cond = thr.Condition()

    # Output buffer
    # Forwarders will launch Datagram Packets from here
    out_buff = []
    out_cond = thr.Condition()

    def __init__(self, address=None, port=c.DEFAULT_PORT, packet_size=c.DEFAULT_PACKET_SIZE,
                 buffer_size=c.DEFAULT_FRAME_BUFFER_SIZE):
        if type(address) is tuple:
            pass
        elif address is None:
            address = (socket.gethostbyname(socket.gethostname()), port)
        else:
            address = (address, port)

        self.__buffer_size = buffer_size
        self.__packet_size = packet_size
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__sock.bind(address)
        self.__whatever = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__stop_event = thr.Event()

    def __str__(self):
        return str(self.__dict__)

    # Private Methods

    def __can_input(self):
        return len(self.in_buff) < self.__packet_size

    def __can_output(self):
        return len(self.out_buff) < self.__packet_size

    def __empty_input(self):
        return len(self.in_buff) == 0

    def __empty_output(self):
        return len(self.out_buff) == 0

    # Public Methods
    @property
    def interface(self):
        return self.__sock.getsockname()

    def send(self, flow_header, data, destination):
        packet = pickle.dumps((flow_header, data))
        try:
            self.out_cond.acquire()
            while not self.__can_output():
                self.out_cond.wait()
            self.out_buff.append((packet, destination))
        finally:
            self.out_cond.notify()
            self.out_cond.release()

    def receive(self):
        try:
            self.in_cond.acquire()
            while self.__empty_input() and not self.__stop_event.is_set():
                self.in_cond.wait()
            packet = self.in_buff.pop(0)
        except IndexError:
            raise ConnectionError
        finally:
            self.in_cond.release()
        return pickle.loads(packet)

    # Runnables and Worker Management

    def terminate(self):
        self.in_cond.acquire()
        self.out_cond.acquire()
        try:
            self.__stop_event.set()
        finally:
            self.in_cond.notify_all()
            self.out_cond.notify_all()
            self.in_cond.release()
            self.out_cond.release()
        self.__sock.close()
        self.__whatever.close()

    def reset(self):
        self.__stop_event.clear()

    def dispatcher(self):
        try:
            logging.debug("Dispatcher Launched")
            while not self.__stop_event.is_set():
                packet = self.__sock.recv(self.__packet_size)
                self.in_cond.acquire()
                try:
                    while (not self.__can_input()) and (not self.__stop_event.is_set()):
                        self.in_cond.wait()
                    self.in_buff.append(packet)
                finally:
                    self.in_cond.notify()
                    self.in_cond.release()
            logging.debug("Dispatcher Death")
        except Exception:
            if not self.__stop_event.is_set():
                logging.exception(f"Exception on flow Dispatcher")
            else:
                logging.debug("Dispatcher Death")

    def forwarder(self):
        try:
            logging.debug("Forwarder Launched")
            while not self.__stop_event.is_set():
                self.out_cond.acquire()
                try:
                    while self.__empty_output() and not self.__stop_event.is_set():
                        self.out_cond.wait()
                    packet, address = self.out_buff.pop(0)
                finally:
                    self.out_cond.release()
                if packet and address:
                    if isinstance(address, (list, set)):
                        for add in address:
                            self.__whatever.sendto(packet, add)
                    else:
                        self.__whatever.sendto(packet, address)
            logging.debug("Forwarder Death")
        except Exception:
            if not self.__stop_event.is_set():
                logging.exception("Exception on flow Forwarder")
            else:
                logging.debug("Forwarder Death")
