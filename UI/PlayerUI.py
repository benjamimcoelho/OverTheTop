import logging
import os
import threading
import tkinter as tk
import tkinter.messagebox
from tkinter import *

from PIL import Image
from PIL import ImageTk

from OverTheTop.Content.Player_Manager import PlayerInterface
from Utils import pairing

CACHE_FILE_NAME = "cache-"
ASSETS_DIR = 'Assets'
CACHE_FILE_EXT = ".jpg"
PID = os.getpid()
RESUME_PLAY = 40


class ClientGUI:

    def __init__(self, root, name):
        self.window = tk.Toplevel(root, background='black')
        self.window.title(name)
        self.__lock = threading.RLock()
        self.frame_buffer = []
        self.sessionId = 0
        self.frameNbr = 0
        self.stop_event = threading.Event()
        self.play_event = threading.Event()
        self.buffering_event = threading.Event()
        self.stop_event.clear()
        self.window.protocol('WM_DELETE_WINDOW', self.handler)
        self.__exit_op = None
        self.__createWidgets__()
        self.bufferImg = [ImageTk.PhotoImage(Image.open(f"{ASSETS_DIR}/buffering.jpg")),
                          ImageTk.PhotoImage(Image.open(f"{ASSETS_DIR}/buffering2.jpg")),
                          ImageTk.PhotoImage(Image.open(f"{ASSETS_DIR}/buffering3.jpg")),
                          ImageTk.PhotoImage(Image.open(f"{ASSETS_DIR}/buffering4.jpg"))]

        self.playMovie()

    def set_exit_op(self, operation):
        self.__exit_op = operation

    def __createWidgets__(self):

        # Create Setup button
        self.setup = Button(self.window, width=20, padx=3, pady=3)
        self.setup["text"] = "Setup"
        self.setup["command"] = self.setupMovie
        self.setup.grid(row=1, column=0, padx=2, pady=2)

        # Create Play button
        self.start = Button(self.window, width=20, padx=3, pady=3)
        self.start["text"] = "Play"
        self.start["command"] = self.playMovie
        self.start.grid(row=1, column=1, padx=2, pady=2)

        # Create Pause button
        self.pause = Button(self.window, width=20, padx=3, pady=3)
        self.pause["text"] = "Pause"
        self.pause["command"] = self.pauseMovie
        self.pause.grid(row=1, column=2, padx=2, pady=2)

        # Create Teardown button
        self.teardown = Button(self.window, width=20, padx=3, pady=3)
        self.teardown["text"] = "Teardown"
        self.teardown["command"] = self.exitClient
        self.teardown.grid(row=1, column=3, padx=2, pady=2)

        # Create a label to display the movie
        self.label = Label(self.window, height=19, background='black')
        self.label.grid(row=0, column=0, columnspan=4, sticky=W + E + N + S, padx=5, pady=5)

    def setupMovie(self):
        """Setup button handler."""
        logging.error("Setup not implemented!")

    def exitClient(self):
        """Teardown button handler."""
        self.stop()
        self.window.destroy()  # Close the gui window
        try:
            if self.__exit_op:
                self.__exit_op()
        except Exception:
            pass
        try:
            os.remove(CACHE_FILE_NAME + str(self.sessionId) + CACHE_FILE_EXT)  # Delete the cache image from video
        except (FileNotFoundError, PermissionError):
            pass

    def pauseMovie(self):
        """Pause button handler"""
        self.play_event.clear()
        self.__lock.acquire()
        try:
            self.frame_buffer.clear()
        finally:
            self.__lock.release()

    def playMovie(self):
        """Play button handler."""
        self.play_event.set()

    def run(self):
        # This prevents cache conflicts from multiple threads and multiple processes
        self.sessionId = pairing(threading.get_ident(), PID)
        self.listenForFrames()

    def stop(self):
        self.stop_event.set()
        self.play_event.set()
        self.buffering_event.set()

    def insert_chunk(self, chunk):
        if not self.play_event.is_set():
            return
        self.__lock.acquire()
        try:
            self.frame_buffer.append(chunk)
        finally:
            self.__lock.release()

    @staticmethod
    def wait_curve(buffer_length, min_framerate=30, switch_point=60):
        if buffer_length < switch_point:
            return 1.0 / min_framerate
        else:
            x = (buffer_length - switch_point)
            f = 2 ** (x / 10) + min_framerate
            return 1.0 / f

    def listenForFrames(self):
        """Listen for frames to be stored in buffer"""
        try:
            data = None
            size = -1
            current_frame = 1
            ignore_window = 0
            max_frame = 0
            while not self.stop_event.is_set():
                while not self.play_event.is_set():
                    self.play_event.wait()

                self.__lock.acquire()
                try:
                    while len(self.frame_buffer) == 0 and not self.stop_event.is_set():
                        self.__lock.release()
                        self.buffering()
                        self.__lock.acquire()

                    self.buffering_event.clear()
                    size = len(self.frame_buffer)
                    data = self.frame_buffer.pop(0)
                except IndexError:
                    break
                finally:
                    self.__lock.release()
                if data:
                    frame_number, payload = data
                    if frame_number > max_frame:
                        max_frame = frame_number
                        ignore_window = int(0.1 * max_frame)
                    if frame_number <= current_frame - ignore_window or frame_number >= current_frame:
                        self.updateMovie(self.writeFrame(payload))
                        self.stop_event.wait(self.wait_curve(size))
        except RuntimeError:
            logging.debug("Runtime Error in player", exc_info=True)
        except Exception:
            logging.exception("Exception in movie player")
        finally:
            self.exitClient()

    def buffering(self, ):
        i = 0
        total = len(self.bufferImg)
        wait_time = 1.0 / total
        while len(self.frame_buffer) < RESUME_PLAY and not self.stop_event.is_set():
            self.label.configure(image=self.bufferImg[i], height=350)
            self.label.image = self.bufferImg[i]
            self.stop_event.wait(wait_time)
            i = (i + 1) % total

    def writeFrame(self, data):
        """Write the received frame to a temp image file. Return the image file."""
        cachename = CACHE_FILE_NAME + str(self.sessionId) + CACHE_FILE_EXT
        file = open(cachename, "wb")
        file.write(data)
        file.close()

        return cachename

    def updateMovie(self, imageFile):
        """Update the image file as video frame in the GUI."""
        photo = ImageTk.PhotoImage(Image.open(imageFile))
        self.label.configure(image=photo, height=288)
        self.label.image = photo

    def handler(self):
        """Handler on explicitly closing the GUI window."""
        self.pauseMovie()
        if tkinter.messagebox.askokcancel("Quit?", "Are you sure you want to quit?"):
            self.exitClient()
        else:  # When the user presses cancel, resume playing.
            self.playMovie()


class Player(PlayerInterface):
    def __init__(self, root, flow_id, name=None):
        # Create a new client
        name = name or flow_id
        self.app = ClientGUI(root, name)

    def set_exit_op(self, operation):
        self.app.set_exit_op(operation)

    def run(self):
        self.app.run()

    def stop(self):
        self.app.stop()

    def insert_chunk(self, chunk):
        self.app.insert_chunk(chunk)
