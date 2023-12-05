import logging
from operator import itemgetter

from OverTheTop.Network.Node.Flow_Data import Flow_Table, InvalidFlow
from OverTheTop.Network.Node.Routing_Data import Routing_Table
from Utils.str_extra import add_tabs


def invert_dict(dictionary: dict):
    res = {}
    for k, v in dictionary.items():
        if v in res:
            res[v].add(k)
        else:
            res[v] = {k}
    return res


class Node:

    def __init__(self, node_id, name=None):
        self.__node_id = node_id
        self.__name = name
        self.__flow = Flow_Table()
        self.__route = Routing_Table()

    def set_name(self, name):
        self.__name = name

    @property
    def name(self):
        return self.__name

    @property
    def node_id(self):
        return self.__node_id

    def __str__(self):
        return (f"<Node:\n\tID: {add_tabs(self.__node_id)}\n" +
                f"\tRouting:\n{add_tabs(self.__route, n_tabs=2)}\n\tFlow:\n{add_tabs(self.__flow, n_tabs=2)}\n>")

    def __process_changes(self, dva_changes):
        new, light, heavy, lost = dva_changes
        if len(heavy) > 0 or len(lost) > 0:
            losses = self.__flow.clean_flows(self.__node_id, heavy, lost)
            collection = self.__flow.get_keys(heavy)
            return losses, collection
        if len(light) + len(new) > 0:
            return set(), set()
        return None

    def receive_distance_vector(self, neighbour_id, distance_vector, cost=1):
        dva = self.__route.update(neighbour_id, cost, distance_vector)
        return self.__process_changes(dva)

    def new_neighbour(self, neighbour_id, cost=1):
        return self.__process_changes(self.__route.update(neighbour_id, cost))

    def rm_neighbour(self, neighbour_id):
        dva = self.__route.remove_node(neighbour_id)
        return self.__process_changes(dva)

    def next_gateways(self, destinations):
        try:
            destinations.remove(self.__node_id)
            local = True
        except KeyError:
            local = False
        node_to_gateway, _ = self.__route.next_nodes(destinations)
        return invert_dict(node_to_gateway), local

    def destinations(self, flow_key):
        return self.__flow.get_destinations(flow_key)

    def gen_distance_vector(self, neighbour):
        return self.__route.gen_distance_vector(neighbour)

    def gen_distance_vectors(self, neighbours):
        return self.__route.gen_distance_vectors(neighbours)

    def flow_collection(self):
        return self.__flow.flow_collection()

    def receive_flow_collection(self, flow_collection):
        return self.__flow.merge_flow_collection(flow_collection)

    def __process_flow_request(self, flow_key, destination):
        supplier = self.__flow.flow_request(flow_key, destination, self.__node_id)
        if supplier and supplier != self.__node_id:
            return self.__route.next_node(supplier)
        return None

    def flow_recovery(self, flow_key):
        try:
            return self.__process_flow_request(flow_key, self.__node_id), \
                   (flow_key, self.__node_id)
        except InvalidFlow:
            return None, None

    def flow_request(self, flow_id, origin=None):
        if origin is None:
            origins = self.__flow.get_origins(flow_id)
            if self.__node_id in origins:
                origin = self.__node_id
                logging.debug(f"Requesting flow {flow_id} from local streamer")
            else:
                origins, _ = self.__route.next_nodes_costs(origins)
                origin = min(origins, key=itemgetter(1))
                logging.debug(f"Requesting flow {flow_id} from source {origin}")

        flow_key = Flow_Table.key(flow_id, origin)
        return self.__process_flow_request(flow_key, self.__node_id), flow_key, (flow_key, self.__node_id)

    def handle_flow_request(self, flow_request):
        flow_key, destination = flow_request
        return self.__process_flow_request(flow_key, destination)

    def __process_flow_cancel(self, flow_key, destination):
        supplier = self.__flow.flow_renunciation(flow_key, destination, self.__node_id)
        if supplier is not None:
            return self.__route.next_node(supplier)

    def flow_cancel(self, flow_key):
        return self.__process_flow_cancel(flow_key, self.__node_id), (flow_key, self.__node_id)

    def handle_flow_cancel(self, flow_request):
        flow_key, destination = flow_request
        return self.__process_flow_cancel(flow_key, destination)

    def next_node(self, destination):
        return self.__route.next_node(destination)

    def next_nodes(self, destinations):
        return self.__route.next_nodes(destinations)

    def announcement(self, flow_key) -> bool:
        if not self.__flow.contains_key(flow_key):
            self.__flow.register_supplier(flow_key)
            return True
        return False

    def register_flow(self, flow_id):
        flow_key = (flow_id, self.__node_id)
        self.__flow.register_supplier(flow_key)
        return flow_key

    def get_flow_ids(self):
        return self.__flow.get_flow_ids()

    def get_flow_ids_n_status(self):
        return self.__flow.get_flow_ids_n_status()

    def await_active(self, flow_key):
        self.__flow.await_active(flow_key)

    def flow_withdraw(self, flow_key):
        try:
            return self.__flow.flow_remove(flow_key)
        except InvalidFlow:
            return None

    def time_out(self, neighbour_id):
        return self.rm_neighbour(neighbour_id)

    def clean(self):
        self.__flow.clear()
