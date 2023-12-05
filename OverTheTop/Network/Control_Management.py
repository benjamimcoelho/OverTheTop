import logging
import pickle
import socket
from enum import Enum

from OverTheTop import defaults as c


class Tag(Enum):
    AUTHENTICATION = 0
    AUTHENTICATION_REQUIRED = 10
    DISTANCE_VECTOR = 100
    FLOW_COLLECTION = 210
    FLOW_ANNOUNCE = 220
    FLOW_REQUEST = 230
    FLOW_CANCEL = 240
    FLOW_WITHDRAW = 250
    PING_REQUEST = 300
    PING_RESPONSE = 310


class Control_Frame:
    def __init__(self, tag, data):
        self.__tag = tag
        self.__data = data

    def tag(self):
        return self.__tag

    def data(self):
        return self.__data

    def tag_n_data(self):
        return self.__tag, self.__data


class Control_Connection:

    def __init__(self, sock, address=None, name=None, flow_interface=None):
        self.__sock = sock
        self.__name = name
        if address is None:
            self.__sock = sock.getsockname()
        else:
            self.__address = address
        self.__flow_interface = flow_interface

    def __str__(self):
        return f"<Control_Connection name: {self.__name} sock:{self.__sock}/>"

    def set_name(self, name):
        self.__name = name

    @property
    def name(self):
        return self.__name

    def get_address(self):
        return self.__address

    def set_flow_interface(self, interface):
        self.__flow_interface = interface

    def get_interface(self):
        return self.__flow_interface

    def terminate(self):
        try:
            self.__sock.shutdown(socket.SHUT_RDWR)
            self.__sock.close()
            self.__sock = None
        except socket.error:
            pass

    # Static Methods
    @staticmethod
    def connect(address, port=c.DEFAULT_PORT):
        if type(address) is tuple:
            pass
        elif address is None:
            address = (socket.gethostname(), port)
        else:
            address = (address, port)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(address)
        return Control_Connection(s, address)

    def send(self, obj):
        try:
            data = pickle.dumps(obj)
            length = len(data)
            self.__sock.send(length.to_bytes(c.INTEGER_SIZE, 'little'))
            self.__sock.send(data)
        except socket.error as e:
            logging.exception("WELP")
            raise e

    def receive(self):
        length = int.from_bytes(self.__sock.recv(c.INTEGER_SIZE), 'little')
        data = self.__sock.recv(length)
        return pickle.loads(data)


class Control_Server:

    def __init__(self, address=None, port=c.DEFAULT_PORT):
        if type(address) is tuple:
            pass
        elif address is None:
            address = (socket.gethostbyname(socket.gethostname()), port)
        else:
            address = (address, port)

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(address)
        # Don't know how "listens" starts the server (aka does it take over the current thread or not?)

    def get_address(self):
        return self.server.getsockname()

    def listen(self, backlog=c.DEFAULT_BACKLOG):
        self.server.listen(backlog)

    def terminate(self):
        self.server.close()

    # Public Methods
    def accept(self):
        sock, address = self.server.accept()
        return Control_Connection(sock, address)
