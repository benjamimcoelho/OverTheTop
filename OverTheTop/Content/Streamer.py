import time

from OverTheTop import defaults


class InvalidExtension(RuntimeError):
    pass


class Streamer:
    __stream_lib = {
    }

    @staticmethod
    def get_streamer(source, args=None):
        extension = 'N/A'
        try:
            extension = source.split('.')[1]
            return Streamer.__stream_lib[extension](source, args)
        except IndexError:
            raise InvalidExtension(f"Unable to identify extension for {source}")
        except KeyError:
            raise InvalidExtension(f"No streamer registered for extension {extension}")

    @staticmethod
    def register_streamer(extension, maker):
        Streamer.__stream_lib[extension] = maker

    def __init__(self, filename, args=None):
        pass

    def next_chunk(self):
        pass


class MPEG_Streamer(Streamer):

    def __init__(self, filename, framerate=None):
        super().__init__(filename)
        self.__framerate = framerate or defaults.DEFAULT_FRAME_RATE_SEC
        self.__filename = filename
        try:
            self.__file = open(filename, 'rb')
        except:
            raise IOError
        self.__time_wait = 1.0 / self.__framerate
        self.frameNum = 0

    def next_chunk(self):
        """Get next frame."""
        data = self.__file.read(5)  # Get the framelength from the first 5 bits
        if data is None or data == b'':
            self.__file.seek(0)
            data = self.__file.read(5)
            self.frameNum = 0
            if data is None or data == b'':
                raise IOError
        frame_length = int(data)
        # Read the current frame
        data = self.__file.read(frame_length)
        self.frameNum += 1
        time.sleep(self.__time_wait)

        return self.frameNum, data

    def frame_nbr(self):
        """Get frame number."""
        return self.frameNum

    def set_framerate(self, framerate):
        self.__framerate = framerate

    def get_framerate(self):
        return self.__framerate


Streamer.register_streamer("Mjpeg", MPEG_Streamer)
