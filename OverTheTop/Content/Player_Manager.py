from concurrent.futures import ThreadPoolExecutor

from Utils.threading_extra import RWLock


class PlayerInterface:
    def run(self):
        pass

    def stop(self):
        pass

    def insert_chunk(self, chunk):
        pass


class Player_Handler:
    def __init__(self):
        self.players = {}
        self.lock = RWLock()
        self.player_thread_pool = ThreadPoolExecutor()

    def register_player(self, flow_key, player):
        self.lock.acquire_write()
        try:
            if flow_key in self.players:
                self.players[flow_key].stop()
            self.players[flow_key] = player
        finally:
            self.lock.release_write()

        self.player_thread_pool.submit(player.run)
        return flow_key  # Currently flow_key == player_id therefore it can be used as an index to the player

    def remove_player(self, player_id):
        self.lock.acquire_write()
        try:
            player = self.players.pop(player_id)
        finally:
            self.lock.release_write()
        player.stop()
        return player_id, player  # Returns the flow_key which it was assigned, currently flow_key == player_id

    def cancel_key(self, flow_key):
        return self.remove_player(flow_key)

    def insert_chunk(self, flow_key, chunk):
        self.lock.acquire_read()
        try:
            self.players[flow_key].insert_chunk(chunk)
        except KeyError:
            pass
        finally:
            self.lock.release_read()

    def get_flow_id(self, player_id):
        return player_id

    get_player_id = get_flow_id
