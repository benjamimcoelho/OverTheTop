import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import OverTheTop.defaults
from OverTheTop.Content.Player_Manager import Player_Handler
from OverTheTop.Content.Streamer import MPEG_Streamer, Streamer, InvalidExtension
from OverTheTop.Network.Control_Management import *
from OverTheTop.Network.Flow_Management import Flow_Handler
from OverTheTop.Network.Node import Node
from OverTheTop.Network.Node.Flow_Data import InvalidFlow
from Utils import Scaling_Method, Scaling_Method_Library
from Utils.threading_extra import RWLock


class InvalidNeighbour(LookupError):
    pass


class OTT:

    def __init__(self, max_connections=None,
                 bind_port=None, bind_address=None, name=None,
                 max_authentication_tries=None,
                 max_reconnect_tries=None
                 ):
        max_reconnect_tries = max_reconnect_tries or defaults.MAX_RECONNECTION_TRIES
        max_authentication_tries = max_authentication_tries or defaults.MAX_AUTHENTICATION_TRIES
        max_connections = max_connections or c.DEFAULT_MAX_CONNECTIONS
        bind_port = bind_port or c.DEFAULT_PORT
        bind_address = bind_address or socket.gethostbyname(socket.gethostname())
        logging.info(f"Attempting to bind to {bind_address}:{bind_port}")
        # Thread Pools
        self.__stop_event = threading.Event()
        self.__flow_handler_pool = ThreadPoolExecutor()
        self.__control_connection_pool = ThreadPoolExecutor(max_connections)
        self.__streamer_pool = ThreadPoolExecutor()
        # Main Instances
        self.__flow_handler = Flow_Handler(address=bind_address, port=bind_port)
        self.__control_server = Control_Server(address=bind_address, port=bind_port)
        self.__max_reconnect_tries = max_reconnect_tries
        self.__max_auth_tries = max_authentication_tries
        self.__connections = {}
        self.__connections_lock = RWLock()
        self.__icu = {}
        self.__icu_lock = threading.Condition()
        self.__player_handler = Player_Handler()
        self.__node = Node(uuid.uuid4().hex, name)
        self.__flow_event = threading.Event()
        self.__overlay_event = threading.Event()

    # Private Methods -------------------------------------------------------------------------------------------------

    @property
    def flow_event(self):
        return self.__flow_event

    @property
    def overlay_event(self):
        return self.__overlay_event

    def __doctor_enabled(self):
        return self.__max_reconnect_tries > 0

    def __send_control(self, neighbour, control_tag: Tag, control_data):
        self.__connections[neighbour].send(Control_Frame(control_tag, control_data))

    def __authentication(self, connection: Control_Connection):
        fails = 0
        connection.send(
            Control_Frame(Tag.AUTHENTICATION,
                          (self.__node.node_id, self.__flow_handler.interface, self.__node.name)))
        while fails < defaults.MAX_AUTHENTICATION_TRIES:
            try:
                frame = connection.receive()
                if frame.tag() == Tag.AUTHENTICATION:
                    neighbour_id, flow_interface, name = frame.data()
                    if name:
                        connection.set_name(name)
                    if flow_interface:
                        connection.set_flow_interface(flow_interface)
                    return neighbour_id
                elif frame.tag() == Tag.AUTHENTICATION_REQUIRED:
                    connection.send(
                        Control_Frame(Tag.AUTHENTICATION,
                                      (self.__node.node_id, self.__flow_handler.interface, self.__node.name)))
                    connection.send(Control_Frame(Tag.AUTHENTICATION_REQUIRED, None))
                else:
                    fails += 1
                    connection.send(Control_Frame(Tag.AUTHENTICATION_REQUIRED, None))
            except (NameError, TypeError):
                fails += 1
        return None

    def __register_in_icu(self, neighbour_id, connection, max_retries=None):
        max_retries = max_retries or self.__max_reconnect_tries
        self.__icu_lock.acquire()
        try:
            self.__icu[neighbour_id] = (connection, max_retries + 1, -1)
        finally:
            self.__icu_lock.notify_all()
            self.__icu_lock.release()

    # Times out a connection
    def __time_out_neighbour(self, neighbour_id):
        try:
            self.__connections_lock.acquire_write()
            try:
                self.__connections[neighbour_id].terminate()
                connection = self.__connections.pop(neighbour_id)
            except KeyError:  # Already dealt with
                return
            finally:
                self.__connections_lock.release_write()
            if self.__doctor_enabled():
                self.__process_update(self.__node.time_out(neighbour_id), neighbour_id)
                self.__register_in_icu(neighbour_id, connection)
                self.__overlay_event.set()
                logging.warning(f"{neighbour_id} Timeout. Reconnection submitted to ICU.")
            else:
                self.__process_update(self.__node.rm_neighbour(neighbour_id), neighbour_id)
                logging.error(f"{neighbour_id} Timeout. Connection closed.")
        except Exception:
            logging.exception(f"Couldn't time out neighbour {neighbour_id}.")

    def __discharge_neighbour(self, neighbour):
        self.__icu_lock.acquire()
        try:
            self.__icu.pop(neighbour)
            logging.info(f"Discharged {neighbour} from ICU.")
        except KeyError:
            pass
        finally:
            self.__icu_lock.release()

    # Registers a control connection
    def __register_connection(self, connection: Control_Connection):
        neighbour_id = None
        self.__icu_lock.acquire()
        try:
            neighbour_id = self.__authentication(connection)
            self.__icu.pop(neighbour_id)
        except KeyError:
            pass
        finally:
            self.__icu_lock.release()

        if neighbour_id is None:
            logging.info(f"Authentication failed with {connection.get_address()}")
            return None
        if self.__doctor_enabled():
            self.__discharge_neighbour(neighbour_id)
        # Register Connection
        self.__connections_lock.acquire_write()
        try:
            self.__connections[neighbour_id] = connection
        finally:
            self.__connections_lock.release_write()
        self.__process_update(self.__node.new_neighbour(neighbour_id), neighbour_id)
        logging.info(f"Registered {connection.get_address()} as neighbour {neighbour_id}.")

        return neighbour_id

    def __recover_flows(self, losses):
        for flow_key in losses:
            gateway, request = self.__node.flow_recovery(flow_key)
            if gateway:
                self.__send_control(gateway, Tag.FLOW_REQUEST, request)
            else:
                self.__player_handler.cancel_key(flow_key)

    def __process_update(self, update_record, source=None):
        if update_record is not None:
            losses, collection = update_record
            self.__overlay_event.set()
            self.__flow_event.set()

            self.__connections_lock.acquire_read()
            try:
                neighbours = set(self.__connections.keys())
                b = source in neighbours
                if b:
                    neighbours.remove(source)
                for neighbour, vector in self.__node.gen_distance_vectors(neighbours).items():
                    self.__send_control(neighbour, Tag.DISTANCE_VECTOR, vector)
                if b and collection:
                    self.__send_control(source, Tag.FLOW_COLLECTION, collection)
            finally:
                self.__connections_lock.release_read()
            if source and collection and len(collection) > 0:
                self.__recover_flows(losses)

    # Forwards a flow chunk to the corresponding neighbours based on the current flow destinations
    # If the current machine is among the destinations, the chunk is also forwarded to the video player
    def __forward_flow(self, flow_key, destinations, chunk):
        headers = {}
        gateways, local = self.__node.next_gateways(destinations)

        self.__connections_lock.acquire_read()
        try:
            for g in gateways:
                if g in self.__connections:
                    headers[self.__connections[g].get_interface()] = (flow_key, gateways[g])
        finally:
            self.__connections_lock.release_read()
        for address in headers:
            self.__flow_handler.send(headers[address], chunk, address)
        if local:
            self.__player_handler.insert_chunk(flow_key, chunk)

    def __request_flow(self, flow_id):
        gateway, flow_key, flow_request = self.__node.flow_request(flow_id)
        if gateway:
            self.__send_control(gateway, Tag.FLOW_REQUEST, flow_request)
        return flow_key

    def __announce(self, flow_key):
        self.__connections_lock.acquire_read()
        try:
            for connection in self.__connections:
                self.__send_control(connection, Tag.FLOW_ANNOUNCE, flow_key)
        finally:
            self.__connections_lock.release_read()

    # Interprets a distance vector, updates the neighbours if needed
    def __process_distance_vector(self, neighbour_id, distance_vector):
        self.__process_update(self.__node.receive_distance_vector(neighbour_id, distance_vector), neighbour_id)

    def __general_flood(self, tag, data, source=None):
        self.__connections_lock.acquire_read()
        try:
            for neighbour in self.__connections:
                if neighbour is not source:
                    self.__send_control(neighbour, tag, data)
        finally:
            self.__connections_lock.release_read()
        self.__flow_event.set()

    def __handle_flow_announce(self, neighbour_id, flow_key):
        if self.__node.announcement(flow_key):
            self.__general_flood(Tag.FLOW_ANNOUNCE, flow_key, neighbour_id)

    def __handle_flow_collection(self, neighbour_id, flow_data):
        collection = self.__node.receive_flow_collection(flow_data)
        if collection:
            self.__general_flood(Tag.FLOW_COLLECTION, collection, neighbour_id)

    def __send_withdraw(self, flow_key):
        self.__general_flood(Tag.FLOW_WITHDRAW, flow_key)

    def __handle_flow_request(self, flow_request):
        gateway = self.__node.handle_flow_request(flow_request)
        self.__flow_event.set()
        if gateway:
            self.__send_control(gateway, Tag.FLOW_REQUEST, flow_request)

    def __handle_flow_cancel(self, flow_request):
        gateway = self.__node.handle_flow_cancel(flow_request)
        self.__flow_event.set()
        if gateway:
            self.__send_control(gateway, Tag.FLOW_CANCEL, flow_request)

    def __disconnect_neighbour(self, neighbour_id):
        self.__connections_lock.acquire_write()
        try:
            self.__connections[neighbour_id].terminate()
            self.__connections.pop(neighbour_id).get_interface()
            self.__process_update(self.__node.rm_neighbour(neighbour_id), neighbour_id)
        finally:
            self.__connections_lock.release_write()
        logging.info(f"Neighbour {neighbour_id} has disconnected.")

    # Please keep this as the last method of the (private - workers) method list :)
    # Interprets a control message and executes the task associated with it
    def __process_control_frame(self, neighbour_id, frame):
        tag, data = frame.tag_n_data()
        logging.debug(f"Received {tag} frame from {neighbour_id}")
        if tag == Tag.DISTANCE_VECTOR:
            self.__process_distance_vector(neighbour_id, data)
        elif tag == Tag.FLOW_COLLECTION:
            self.__handle_flow_collection(neighbour_id, data)
        elif tag == Tag.FLOW_ANNOUNCE:
            self.__handle_flow_announce(neighbour_id, data)
        elif tag == Tag.FLOW_REQUEST:
            self.__handle_flow_request(data)
        elif tag == Tag.FLOW_CANCEL:
            self.__handle_flow_cancel(data)
        elif tag == Tag.FLOW_WITHDRAW:
            self.withdraw_flow(data)
        elif tag == Tag.AUTHENTICATION_REQUIRED:
            self.__send_control(neighbour_id, Tag.AUTHENTICATION,
                                (self.__node.node_id, self.__flow_handler.interface, self.__node.name))
        # Discarded [Authentication]

    # --- Workers -----------------------------------------------------------------------------------------------------

    # Engages the dispatchers/forwarders responsible for managing incoming/outgoing flow frames
    # The worker count is cumulative, therefore the total amount of workers issued corresponds to the sum of all the
    # workers engaged util the current moment
    def __engage_global_workers(
            self,
            forwarder_count=c.DEFAULT_FLOW_FORWARDER_COUNT,
            dispatcher_count=c.DEFAULT_FLOW_DISPATCHER_COUNT,
            processor_count=c.DEFAULT_FLOW_PROCESSOR_COUNT,
            doctor_count=c.DEFAULT_DOCTOR_COUNT
    ):
        for x in range(forwarder_count):
            self.__flow_handler_pool.submit(self.__flow_handler.dispatcher)
        for x in range(dispatcher_count):
            self.__flow_handler_pool.submit(self.__flow_handler.forwarder)
        for x in range(processor_count):
            self.__flow_handler_pool.submit(self.__flow_processor)
        for x in range(doctor_count):
            self.__flow_handler_pool.submit(self.__connection_doctor)

    def __flow_processor(self):
        try:
            logging.debug("Engaged Flow Processor")
            while not self.__stop_event.is_set():
                flow_header, chunk = self.__flow_handler.receive()
                flow_key, destinations = flow_header
                self.__forward_flow(flow_key, destinations, chunk)
        except ConnectionError:
            logging.debug("Flow processor death")
        except Exception:
            logging.exception("Exception in flow processor")

    def __flow_streamer(self, source, flow_key):
        try:
            streamer = Streamer.get_streamer(source)
            while not self.__stop_event.is_set():
                try:
                    self.__node.await_active(flow_key)
                    self.__forward_flow(flow_key, self.__node.destinations(flow_key), streamer.next_chunk())
                except InvalidFlow:
                    return
        except InvalidExtension:
            logging.exception(f"No streamer available for specific resource")
        except Exception:
            logging.exception(f"Exception in {flow_key}'s streamer")

    def __connection_doctor(self, period=None, max_tries=None, scale_method=None):
        if not self.__doctor_enabled():
            return
        logging.debug("Doctor Launched")
        period = period or defaults.DEFAULT_DOCTOR_PERIOD
        max_tries = max_tries or defaults.MAX_RECONNECTION_TRIES
        scale_method = scale_method or Scaling_Method.LINEAR
        op = Scaling_Method_Library.get_operation(scale_method)
        try:
            while not self.__stop_event.is_set():
                self.__icu_lock.acquire()
                try:
                    while len(self.__icu) <= 0:
                        if self.__stop_event.is_set():
                            break
                        self.__icu_lock.wait()
                finally:
                    self.__icu_lock.release()
                if not self.__stop_event.wait(float(period)):
                    logging.debug("Doctor has started a cycle.")
                    self.__icu_lock.acquire()
                    try:
                        for neighbour in set(self.__icu.keys()):
                            connection, health_points, remaining = self.__icu[neighbour]
                            address = connection.get_interface()
                            remaining -= period
                            if remaining <= 0:
                                try:
                                    self.connect(address)
                                    logging.info(f"Reconnection successful with {address}")
                                    self.__icu.pop(neighbour)
                                except ConnectionRefusedError:
                                    health_points -= 1
                                    if health_points > 0:
                                        logging.warning(
                                            f"Reconnection failed with {address}, {health_points} remaining tr" +
                                            ('ies' if health_points > 1 else 'y'))
                                        self.__icu[neighbour] = (
                                            connection,
                                            health_points,
                                            op(period, max_tries - health_points + 1)
                                        )
                                    else:
                                        logging.error(f"Unable to reconnect with {address}. Discarding Neighbour.")
                                        self.__icu.pop(neighbour)
                                        self.__overlay_event.set()
                            else:
                                self.__icu[neighbour] = (connection, health_points, remaining)
                    finally:
                        self.__icu_lock.release()
            logging.debug("Doctor Death")
        except Exception:
            logging.exception(f"Exception in connection doctor")

    def __welcome_connection(self, neighbour_id, _):
        pre_vector = self.__node.gen_distance_vector(neighbour_id)
        if pre_vector and len(pre_vector) > 0:
            self.__send_control(neighbour_id, Tag.DISTANCE_VECTOR, pre_vector)
        pred_dict = self.__node.flow_collection()
        if pred_dict and len(pred_dict) > 0:
            self.__send_control(neighbour_id, Tag.FLOW_COLLECTION, pred_dict)

    def __connection_worker(self, neighbour_id, connection):
        logging.debug(f"Connection worker started for {neighbour_id}")
        self.__welcome_connection(neighbour_id, connection)
        try:
            while not self.__stop_event.is_set():
                frame = connection.receive()
                if frame:
                    self.__process_control_frame(neighbour_id, frame)
        # except EOFError:  # The neighbour has disconnected
        #    self.__disconnect_neighbour(neighbour_id)
        except (socket.error, ConnectionError, EOFError):  # EOF is to simulate connection errors with disconection
            if not self.__stop_event.is_set():
                self.__time_out_neighbour(neighbour_id)
        except Exception:
            logging.exception(f"Exception in {neighbour_id}'s worker")

    # Public Methods -----------------------------------------------------------------------------------------------

    def run(self):
        try:
            self.__engage_global_workers()
            self.__control_server.listen()
            logging.info("Listening for neighbours")
            while not self.__stop_event.is_set():
                connection = self.__control_server.accept()
                logging.debug(f"Connection received from '{connection.get_address()}'")
                neighbour_id = self.__register_connection(connection)
                if neighbour_id:
                    self.__control_connection_pool.submit(self.__connection_worker, neighbour_id, connection)
                else:
                    connection.terminate()
        except OSError:
            logging.info("OTT Death")
        except Exception:
            logging.critical("Critical error in control server worker", exc_info=True)

    def client(self):
        self.__engage_global_workers()
        self.__control_server.terminate()

    def __signal_all_icu(self):
        self.__icu_lock.acquire()
        try:
            self.__icu_lock.notify_all()
        finally:
            self.__icu_lock.release()

    def stop(self):
        e = self.__overlay_event
        self.__overlay_event = None
        e.set()
        e = self.__flow_event
        self.__flow_event = None
        e.set()
        self.__stop_event.set()
        self.__control_server.terminate()
        self.__connections_lock.acquire_write()
        try:
            for conn in self.__connections:
                self.__connections[conn].terminate()
            self.__connections.clear()
        finally:
            self.__connections_lock.release_write()
        self.__signal_all_icu()
        self.__node.clean()
        self.__flow_handler.terminate()
        self.__streamer_pool.shutdown(False, cancel_futures=True)
        self.__flow_handler_pool.shutdown(False, cancel_futures=True)
        self.__control_connection_pool.shutdown(False, cancel_futures=True)

    # Connects the OverTheTop to a neighbour
    def connect(self, address: tuple):
        connection = Control_Connection.connect(address)
        if connection == self.__control_server.get_address():
            logging.warning("Ignoring attempt to connect to connect to local server. LoopBack features are available "
                            "without loopback connections.")
            return
        logging.debug(f"Attempting to connect to {address}")
        neighbour_id = self.__register_connection(connection)
        if neighbour_id:
            self.__control_connection_pool.submit(self.__connection_worker, neighbour_id, connection)
        else:
            connection.terminate()

    def disconnect(self, neighbour_id):
        """
        Disconnects the OverTheTop from a given neighbour

        :raises InvalidNeighbour: if the OverTheTop isn't connected to the given neighbour
        :type neighbour_id: the neighbour's identification
        """
        logging.debug(f"Attempting to disconnect from {neighbour_id}")
        self.__connections_lock.acquire_write()
        try:
            connection = self.__connections.pop(neighbour_id)
        except KeyError:
            raise InvalidNeighbour(f"{neighbour_id} is invalid")
        finally:
            self.__connections_lock.release_write()
        connection.terminate()
        self.__process_update(self.__node.rm_neighbour(neighbour_id), neighbour_id)

    def get_neighbours(self):
        active, inactive = {}, {}
        self.__icu_lock.acquire()
        try:
            for neighbour in self.__icu:
                inactive[neighbour] = self.__icu[neighbour][0].name
        finally:
            self.__icu_lock.release()
        self.__connections_lock.acquire_read()
        try:
            for neighbour in self.__connections:
                active[neighbour] = self.__connections[neighbour].name
        finally:
            self.__connections_lock.release_read()
        return active, inactive

    def get_available_flows(self):
        return self.__node.get_flow_ids_n_status()

    def new_player(self, flow_id, player):
        flow_key = self.__request_flow(flow_id)
        self.__flow_event.set()
        logging.info(f"Registered player for flow {flow_key}")
        return self.__player_handler.register_player(flow_key, player)

    def remove_player(self, player_id):
        flow_key, player = self.__player_handler.remove_player(player_id)
        gateway, request = self.__node.flow_cancel(flow_key)
        if gateway:
            self.__send_control(gateway, Tag.FLOW_CANCEL, request)
        self.__flow_event.set()
        logging.info(f"Removed player for flow {flow_key}")
        return player

    def yield_flow(self, source):
        flow_key = self.__node.register_flow(source)
        self.__announce(flow_key)
        self.__streamer_pool.submit(self.__flow_streamer, source, flow_key)
        self.__flow_event.set()
        logging.info(f"Flow {flow_key} has been yielded")

    def withdraw_flow(self, flow_key):
        flow_id = self.__node.flow_withdraw(flow_key)
        if flow_id:
            self.__send_withdraw(flow_key)
        self.__player_handler.remove_player(self.__player_handler.get_player_id(flow_id))
        logging.info(f"Flow {flow_key} has been withdrawn")

    def forget_neighbour(self, neighbour_id):
        self.__overlay_event.set()
        self.__icu_lock.acquire()
        try:
            self.__icu.pop(neighbour_id)
        except KeyError:
            pass
        finally:
            self.__icu_lock.release()
