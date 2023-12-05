import logging
import sys
import threading
import time
import tkinter as tk
from tkinter import *
from tkinter import ttk

try:
    from ttkthemes import ThemedTk
except ModuleNotFoundError:
    logging.warning("Unable to load ttkthemes")

from OverTheTop import OTT
from OverTheTop.Network.Node.Flow_Data import Flow_State
from UI.PlayerUI import Player

ASSETS_DIR = 'Assets'
MIN_REFRESH_RATE = 1


class UI:

    def __init__(self, ott: OTT, name: str = None):
        self.ott = ott
        self.stop_event = threading.Event()
        if 'ttkthemes' in sys.modules:
            self.top = ThemedTk(theme='Adapta')
        else:
            logging.warning("Unable to load TTKthemes, using default TKinter")
            logging.warning("Unable to load TTKthemes, using default TKinter")
            self.top = Tk()
        # Main Window
        self.top.geometry("900x700")
        self.top.title('Over the Top' + f" - {name}" if name else '')
        self.top.configure()
        self.top.columnconfigure(0, weight=2)
        self.top.columnconfigure(1, weight=1)
        self.top.columnconfigure(2, weight=4)
        self.top.columnconfigure(3, weight=1)
        # Load Assets
        stream = PhotoImage(file=f"{ASSETS_DIR}/stream.png").subsample(20, 20)
        img2 = PhotoImage(file=f"{ASSETS_DIR}/plus.png")
        self.play = PhotoImage(file=f"{ASSETS_DIR}/play.png").subsample(30, 30)
        self.trash = PhotoImage(file=f"{ASSETS_DIR}/trash.png").subsample(30, 30)
        # Static Labels
        flow_label = ttk.Label(self.top, text='Fluxos')
        flow_label.grid(column=0, row=0, sticky=tk.W, padx=5, pady=5)
        rede_label = ttk.Label(self.top, text='rede')
        add_flow = Button(self.top, image=stream, command=self.new_flow_window)
        add_flow.grid(column=1, row=0)
        rede_label.grid(column=2, row=0, padx=5, pady=5)
        img2_resized = img2.subsample(60, 60)
        Button(self.top, image=img2_resized, command=self.create_window).grid(column=3, row=0, padx=5, pady=5)
        # Worker Start
        self.flow_worker = threading.Thread(target=self.flow_worker)
        self.neighbour_worker = threading.Thread(target=self.neighbour_worker)
        self.flow_worker.start()
        self.neighbour_worker.start()
        self.top.mainloop()

    @staticmethod
    def flow_state_paint(state):
        if state == Flow_State.ACTIVE:
            return 'blue'
        elif state == Flow_State.STREAMING:
            return 'green'
        else:
            return 'black'

    @staticmethod
    def destroy_entry(entry):
        for v in entry:
            v.grid_forget()
            v.destroy()

    @staticmethod
    def button_nop():
        logging.debug("Button not Assigned")

    def general_neighbour_entry(self, index=-1):
        label = Label(self.top, text="N/A", foreground='purple')
        button = Button(self.top, image=self.trash, command=self.button_nop)
        label.grid(column=2, row=index + 1, padx=5, pady=5)
        button.grid(column=3, row=index + 1)
        return label, button

    def general_flow_entry(self, index=-1):
        label = Label(self.top, text="N/A", foreground="purple")
        button = Button(self.top, image=self.play, command=self.button_nop)
        label.grid(column=0, row=index + 1, padx=5, pady=5)
        button.grid(column=1, row=index + 1)
        return label, button

    def update_flow_widgets(self, grid=None):
        if grid is None:
            grid = []
        flow_collection = self.ott.get_available_flows()
        # print(flow_collection)
        new_length = len(flow_collection)
        curr_length = len(grid)
        for i in range(curr_length, new_length):
            grid.append(self.general_flow_entry(index=i))
        for i in range(new_length, curr_length):
            self.destroy_entry(grid.pop())

        index = 0
        for flow, state in flow_collection.items():
            label, button = grid[index]
            label.configure(text=flow, foreground=self.flow_state_paint(state))
            # print('configure new flow' + flow)
            button.configure(command=lambda: self.play_stream(flow))
            index += 1
        # print(grid)
        return grid

    def update_network_widgets(self, grid=None):
        if grid is None:
            grid = []
        neighbours, time_out = self.ott.get_neighbours()

        new_length = len(neighbours) + len(time_out)
        curr_length = len(grid)
        for i in range(curr_length, new_length):
            grid.append(self.general_neighbour_entry(index=i))
        # <--- There loops are mutually exclusive --->
        for i in range(new_length, curr_length):
            self.destroy_entry(grid.pop())

        index = 0
        for neighbour, name in neighbours.items():
            label, button = grid[index]
            label.configure(text=f"{name}#{neighbour}" if name else str(neighbour), foreground='black')
            button.configure(command=lambda: self.delete_neighbour(neighbour))
            index += 1

        for neighbour, name in time_out.items():
            label, button = grid[index]
            button.configure(command=lambda: self.forget_neighbour(neighbour))
            label.configure(text=f"{name}#{neighbour}" if name else str(neighbour), foreground="darkorange")
            index += 1

        return grid

    def flow_worker(self):
        try:
            grid = None
            while not self.stop_event.is_set():
                grid = self.update_flow_widgets(grid)
                try:
                    self.ott.flow_event.wait()
                    self.ott.flow_event.clear()
                except AttributeError:
                    logging.debug("UI flow worker death")
                    return
                time.sleep(MIN_REFRESH_RATE)
        except RuntimeError:
            logging.warning("UI flow worker death in runtime")
        except Exception:
            logging.exception("Error in UI flow worker")

    def neighbour_worker(self):
        try:
            grid = None
            while not self.stop_event.is_set():
                grid = self.update_network_widgets(grid)
                try:
                    self.ott.overlay_event.wait()
                    self.ott.overlay_event.clear()
                except AttributeError:
                    logging.debug("UI neighbour worker death")
                    return
                time.sleep(MIN_REFRESH_RATE)
        except RuntimeError:
            logging.warning("UI neighbour worker death in runtime")
        except Exception:
            logging.exception("Error in UI neighbours worker")

    def add_neighbour(self, ip, port):
        self.ott.connect((ip, port))

    def new_stream(self, filepath):
        self.ott.yield_flow(filepath)

    def delete_neighbour(self, neighbour_id):
        self.ott.disconnect(neighbour_id)

    def forget_neighbour(self, neighbour_id):
        self.ott.forget_neighbour(neighbour_id)

    def play_stream(self, flow_id):
        # print("stream iniciada para" + flow_id)
        player = Player(flow_id=flow_id, root=self.top)
        player_id = self.ott.new_player(flow_id, player)
        player.set_exit_op(lambda: self.ott.remove_player(player_id))

    def new_flow_window(self):
        def inputer():
            file = self.E3.get()
            if file is None or len(file) == 0:
                file = 'movie.Mjpeg'
            self.new_stream(file)
            win2.destroy()

        win2 = tk.Toplevel(self.top)

        win2.columnconfigure(0, weight=1)
        win2.columnconfigure(1, weight=1)

        tk.Button(win2, text='Ok', command=inputer).grid(column=0, row=0)

        self.L3 = Label(win2, text="Ficheiro: ")
        self.L3.grid(column=0, row=1)
        self.E3 = Entry(win2, bd=5)
        self.E3.grid(column=1, row=1)

    def create_window(self):
        def inputer():
            self.add_neighbour(self.E1.get(), int(self.E2.get()))
            win2.destroy()

        win2 = tk.Toplevel(self.top)

        win2.columnconfigure(0, weight=1)
        win2.columnconfigure(1, weight=1)

        tk.Button(win2, text='Ok', command=inputer).grid(column=0, row=0)

        self.L1 = Label(win2, text="IP: ")
        self.L1.grid(column=0, row=1)
        self.L2 = Label(win2, text="PORT: ")
        self.L2.grid(column=0, row=2)
        self.E1 = Entry(win2, bd=5)
        self.E1.grid(column=1, row=1)
        self.E2 = Entry(win2, bd=5)
        self.E2.grid(column=1, row=2)

    def destroy(self):
        self.stop_event.set()
        self.top.destroy()
