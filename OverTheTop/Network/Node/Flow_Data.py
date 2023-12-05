import sys
import threading

from Utils import OrderedEnum
from Utils.threading_extra import RWLock


# Enumeration corresponding to each state a flow can have
class Flow_State(OrderedEnum):
    ACTIVE = 1
    STREAMING = 2
    HOLD = 3
    INVALID = sys.maxsize


# Proxy in Active

class InvalidFlow(LookupError):
    pass


class InvalidState(InvalidFlow):
    pass


class Flow_Entry:
    def __init__(self, destinations=None, state=Flow_State.HOLD):  # Missing Arguments
        self.__state: Flow_State = state  # Origins => Estado
        self.__destinations: set = set(destinations) if destinations else set()  # Destinos
        self.__cond = threading.Condition()

    def __str__(self):
        return f"<FlowEntry({self.__state}, {self.__destinations})/>"

    @property
    def active(self):
        return self.state == Flow_State.ACTIVE

    @property
    def state(self):
        self.__cond.acquire()
        try:
            return self.__state
        finally:
            self.__cond.release()

    @property
    def destinations(self):
        return self.__destinations.copy()

    def set_state(self, state: Flow_State):
        self.__cond.acquire()
        try:
            self.__state = state
        finally:
            self.__cond.notify_all()
            self.__cond.release()

    def await_state(self, foe=Flow_State.HOLD):
        self.__cond.acquire()
        try:
            while self.__state == foe:
                self.__cond.wait()
            if self.__state == Flow_State.INVALID:
                raise InvalidState
        finally:
            self.__cond.release()

    def cancel(self):
        self.__cond.acquire()
        try:
            self.__state = Flow_State.INVALID
            self.__destinations = None
        finally:
            self.__cond.notify_all()
            self.__cond.release()

    def upgrade_state(self, state: Flow_State):
        self.__cond.acquire()
        try:
            if self.__state.value > state.value:
                self.__state = state
                self.__cond.notify_all()
        finally:
            self.__cond.release()

    def add_destination(self, destination, upgrade=False):
        self.__destinations.add(destination)
        if upgrade:
            self.set_state(Flow_State.ACTIVE)
        else:
            self.upgrade_state(Flow_State.STREAMING)

    def strip_destinations(self):
        tmp = self.__destinations
        self.__destinations = set()
        self.set_state(Flow_State.HOLD)
        return tmp

    def rm_destination(self, destination, downgrade=False):
        if type(destination) is iter:
            for dest in destination:
                self.__destinations.remove(dest)
        else:
            self.__destinations.remove(destination)
        if len(self.__destinations) <= 0:
            self.set_state(Flow_State.HOLD)
        elif downgrade:
            self.set_state(Flow_State.STREAMING)


# This table stores the information relative to the different stream data flows that come
# through the node. The keys are the ID's of the flows. Each flow id is connected to the origin«
# of the flow, metric, destinies and its current state
class Flow_Table:

    def __init__(self):
        self.__ids = {}  # Support to __table
        self.__origins = {}  # Support to __table keys and origin
        self.__table = {}  # The actual flow table
        self.__lock = RWLock()  # Concurrency lock

    # Static methods
    @staticmethod
    def key(flow_id, origin):
        return flow_id, origin

    @staticmethod
    def flow_id(key):
        return key[0]

    @staticmethod
    def origin(key):
        return key[1]

    # Private methods

    def __str__(self):
        res = "<FlowTable\n"
        self.__lock.acquire_read()
        try:
            for k in self.__table:
                res += f"\t{k} : {self.__table[k]}\n"
        finally:
            self.__lock.release_read()
        return f"{res}/>"

    def __register_key(self, key) -> None:
        flow_id, origin = key
        if flow_id not in self.__ids:
            self.__ids[flow_id] = {origin}
        else:
            self.__ids[flow_id].add(origin)
        if origin not in self.__origins:
            self.__origins[origin] = {flow_id}
        else:
            self.__origins[origin].add(flow_id)

    def __remove_key(self, key) -> None:
        flow_id, origin = key
        if len(self.__ids[flow_id]) <= 1:
            self.__ids.pop(flow_id)
        else:
            self.__ids[flow_id].remove(origin)
        if len(self.__origins[origin]) <= 1:
            self.__origins.pop(origin)
        else:
            self.__origins[origin].remove(flow_id)

    # Public Methods

    def get_origins(self, flow_id) -> set:
        self.__lock.acquire_read()
        try:
            return self.__ids[flow_id]
        finally:
            self.__lock.release_read()

    def get_flow_ids(self):
        self.__lock.acquire_read()
        try:
            return self.__ids.keys()
        finally:
            self.__lock.release_read()

    def get_flow_ids_n_status(self):
        res = {}
        self.__lock.acquire_read()
        try:
            for flow_id, origins in self.__ids.items():
                res[flow_id] = min({self.__table[self.key(flow_id, o)].state for o in origins})
        finally:
            self.__lock.release_read()
        return res

    def flow_collection(self) -> set:
        self.__lock.acquire_read()
        try:
            return set(self.__table.keys())
        finally:
            self.__lock.release_read()

    def merge_flow_collection(self, flow_collection: set):
        new_flows = set()
        self.__lock.acquire_write()
        try:
            for flow_key in flow_collection:
                if flow_key not in self.__table:
                    self.__register_key(flow_key)
                    self.__table[flow_key] = Flow_Entry()
                    new_flows.add(flow_key)
        finally:
            self.__lock.release_write()
        if len(new_flows) > 0:
            return new_flows
        return None

    def contains_key(self, flow_key):
        self.__lock.acquire_read()
        try:
            return flow_key in self.__table
        finally:
            self.__lock.release_read()

    def register_supplier(self, flow_key, state=Flow_State.HOLD):
        self.__lock.acquire_write()
        try:
            self.__register_key(flow_key)
            self.__table[flow_key] = Flow_Entry(state=state)
        finally:
            self.__lock.release_write()

    def flow_recovery(self, flow_key, destination):
        self.__lock.acquire_write()
        try:
            self.__table[flow_key].add_destination(destination)
        except KeyError:
            raise InvalidFlow(f"No data flow registered for key {flow_key}")
        finally:
            self.__lock.release_write()
        return Flow_Table.origin(flow_key)

    def flow_remove(self, flow_key):
        self.__lock.acquire_write()
        try:
            entry = self.__table.pop(flow_key)
            entry.set_state(Flow_State.INVALID)
        except KeyError:
            raise InvalidFlow(f"No data flow registered for key {flow_key}")
        finally:
            self.__lock.release_write()
        return self.flow_id(flow_key)

    def await_active(self, flow_key):
        self.__lock.acquire_read()
        try:
            entry = self.__table[flow_key]
        except KeyError:
            raise InvalidFlow(f"No data flow registered for key {flow_key}")
        finally:
            self.__lock.release_read()
        return entry.await_state()

    def get_destinations(self, flow_key):
        self.__lock.acquire_read()
        try:
            return self.__table[flow_key].destinations
        except KeyError:
            raise InvalidFlow(f"No data flow registered for key {flow_key}")
        finally:
            self.__lock.release_read()

    def flow_request(self, flow_key, destination, master):
        origin = self.origin(flow_key)
        b = master == destination
        if origin == master or b:
            self.__lock.acquire_write()
            try:
                self.__table[flow_key].add_destination(destination, upgrade=b)
            except KeyError:
                raise InvalidFlow(f"No data flow registered for key {flow_key}")
            finally:
                self.__lock.release_write()
        if origin != master:
            return origin
        return None

    def flow_renunciation(self, flow_key, destination, master):
        origin = self.origin(flow_key)
        x = master == destination
        if destination == origin:
            self.__lock.acquire_write()
            try:
                self.__table[flow_key].rm_destination(destination, downgrade=x)
            except KeyError:
                raise InvalidFlow(f"No data flow registered for key {flow_key}")
            finally:
                self.__lock.release_write()
            return origin

    def clean_flows(self, master, heavy: set, critical: set):
        losses = set()
        if len(heavy) > 0 or len(critical) > 0:
            self.__lock.acquire_write()
            try:
                for node in heavy.union(critical):
                    rm = node in critical
                    if node in self.__origins:
                        for flow_id in self.__origins[node]:
                            key = Flow_Table.key(flow_id, node)
                            if master in self.__table[key].strip_destinations():
                                losses.add(key)
                            if rm:
                                self.__remove_key(key)
                                self.__table.pop(key)
                    for key in self.__table:
                        try:
                            self.__table[key].rm_destination(node)
                        except KeyError:
                            pass
            finally:
                self.__lock.release_write()
        return losses

    def clear(self):
        self.__lock.acquire_write()
        try:
            self.__ids.clear()
            self.__origins.clear()
            for _, entry in self.__table.items():
                entry.cancel()
            self.__table.clear()
        finally:
            self.__lock.release_write()

    def get_keys(self, origins):
        res = set()
        self.__lock.acquire_read()
        try:
            for node in origins:
                try:
                    for flow_id in self.__origins[node]:
                        res.add(self.key(flow_id, node))
                except KeyError:
                    pass
        finally:
            self.__lock.release_read()
        if len(res) > 0:
            return res
        return None
