from operator import itemgetter
from threading import RLock

from Utils.threading_extra import RWLock


# Table is represented in a matrix form, where the the first key represents the destination
# and the second key represents the next node.
# The value represents the cost of traveling to the second key node through the first key node


class NoRoute(Exception):
    pass


class InvalidNode(LookupError):
    pass


class Routing_Table:

    def __init__(self):
        # The actual routing table
        self.__table = {}
        # The table lock for concurrency control .
        self.__table_lock = RLock()
        # Indicates if the current vector is outdated
        # The global distance vector
        self.__global_distance_vector = {}
        self.__gdv_lock = RWLock()

    # Private Methods
    def __gen_global_vector(self):
        vector = {}
        for n in self.__table:
            vector[n] = min(self.__table[n].items(), key=itemgetter(1, 0))
        return vector

    def __str__(self):
        return f"<RoutingTable(table: {self.__table}, gdv: {self.__global_distance_vector})>"

    def __update_gdv(self, new_gdv: dict):
        sym_diff = {k for k, _ in set(self.__global_distance_vector.items()).symmetric_difference(set(new_gdv.items()))}
        new, light, heavy, lost = set(), set(), set(), set()
        for key in sym_diff:
            if key in self.__global_distance_vector:
                if key not in new_gdv:
                    lost.add(key)
                else:
                    if self.__global_distance_vector[key][1] < new_gdv[key][1]:
                        heavy.add(key)
                    else:
                        light.add(key)
            else:
                new.add(key)
        self.__global_distance_vector = new_gdv

        return new, light, heavy, lost

    # Public Methods

    # Updates the current table based on the distance vector provided from the neighbour with the specific connection
    # Returns the resulting distance vector changes
    def update(self, neighbour_id, connection_cost=0, distance_vector: dict = None):
        if distance_vector is None:
            distance_vector = {}
        else:
            distance_vector = distance_vector.copy()
        self.__table_lock.acquire()
        try:
            # This will make sure that the connection cost is also registered
            distance_vector[neighbour_id] = 0
            # Update the current table values
            for k in set(self.__table.keys()):
                if k not in distance_vector:
                    self.__table[k].pop(neighbour_id, None)
                    if len(self.__table[k]) == 0:
                        self.__table.pop(k)
                else:
                    self.__table[k][neighbour_id] = distance_vector.pop(k) + connection_cost
            # Register new table nodes (aka the remaining keys in the distance vector)
            for k in distance_vector:
                self.__table[k] = {neighbour_id: distance_vector[k] + connection_cost}
        finally:
            new_vector = self.__gen_global_vector()
            self.__gdv_lock.acquire_write()
            self.__table_lock.release()
        try:
            return self.__update_gdv(new_vector)
        finally:
            self.__gdv_lock.release_write()

    # Generates a distance vector for the given neighbour_id
    def gen_distance_vector(self, neighbour_id):
        self.__gdv_lock.acquire_read()
        try:
            pre_vector = self.__global_distance_vector.copy()
        finally:
            self.__gdv_lock.release_read()
        pre_vector.pop(neighbour_id, None)
        vector = {}
        for route, v in pre_vector.items():
            gateway, cost = v
            if gateway != neighbour_id:
                vector[route] = cost
        return vector

    def gen_distance_vectors(self, neighbours):
        self.__gdv_lock.acquire_read()
        try:
            return {n: self.gen_distance_vector(n) for n in neighbours}
        finally:
            self.__gdv_lock.release_read()

    def __next_node_modular(self, destination, tuple_target):
        self.__gdv_lock.acquire_read()
        try:
            return self.__global_distance_vector[destination][tuple_target]
        except (KeyError, TypeError, IndexError):
            raise NoRoute(f"No available route to {destination}")
        finally:
            self.__gdv_lock.release_read()

    # Gets the best next node that leads to the given destination
    def next_node(self, destination):
        return self.__next_node_modular(destination, 0)

    def next_node_cost(self, destination):
        return self.__next_node_modular(destination, 1)

    def __next_nodes_modular(self, destinations, tuple_target):
        self.__gdv_lock.acquire_read()
        try:
            node_map, invalid = {}, set()
            for destination in destinations:
                try:
                    node_map[destination] = self.__global_distance_vector[destination][tuple_target]
                except KeyError:
                    invalid.add(destination)
            return node_map, invalid
        finally:
            self.__gdv_lock.release_read()

    def next_nodes(self, destinations):
        return self.__next_nodes_modular(destinations, 0)

    def next_nodes_costs(self, destinations):
        return self.__next_nodes_modular(destinations, 1)

    def get_all_nodes(self):
        self.__gdv_lock.acquire_read()
        try:
            return set(self.__global_distance_vector.keys())
        finally:
            self.__gdv_lock.release_read()

    def remove_node(self, neighbour_id):
        self.__table_lock.acquire()
        try:
            self.__table.pop(neighbour_id, None)
            for n in set(self.__table.keys()):
                self.__table[n].pop(neighbour_id, None)
                if len(self.__table[n]) == 0:
                    self.__table.pop(n)
        except KeyError:
            raise InvalidNode(f"The neighbour {neighbour_id} is not registered in the routing table")
        else:
            new_vector = self.__gen_global_vector()
            self.__gdv_lock.acquire_write()
        finally:
            self.__table_lock.release()
        try:
            return self.__update_gdv(new_vector)
        finally:
            self.__gdv_lock.release_write()
