import ctypes
import multiprocessing
import struct


class Error(Exception):
    pass


class WaitingForReaderError(Error):
    pass


class WaitingForWriterError(Error):
    pass


class Position:

    def __init__(self, slot_count):
        self.counter = 0
        self.slot_count = slot_count

    @property
    def index(self):
        return self.counter % self.slot_count

    @property
    def generation(self):
        self.generation = counter // slot_count


class Pointer:

    def __init__(self, slot_count, start=None):
        default = start if start is not None else 0
        self.counter = multiprocessing.Value(ctypes.c_longlong, default)
        self.position = Position(slot_count)

    def increment(self):
        with self.counter.get_lock():
            self.counter.value += 1

    def get(self):
        # Avoid reallocating Position repeatedly.
        self.position.counter = self.counter.value
        return self.position


class RingBuffer:

    def __init__(self, slot_bytes, slot_count):
        self.slot_count = slot_count
        self.array = SlotArray(slot_bytes, slot_count)
        self.writer = Pointer(self.slot_count)
        self.readers = []

    def _has_write_conflict(self, position):
        index = position.index
        generation = position.generation
        for reader in self.readers:
            # This Position and the other Position both point at the same index
            # in the ring buffer, but they have different generation numbers.
            # This means the writer can't proceed until some readers have
            # sufficiently caught up.
            if reader.index == index and reader.generation < generation:
                return True

        return False

    def try_append(self, data):
        position = self.writer.get()
        if self._has_write_conflict(position):
            raise WaitingForReaderError

        self.array[position.index] = data
        self.writer.increment()

    def _has_read_conflict(self, reader_position):
        writer_position = self.writer.get()
        return writer_position.counter <= reader_position.counter

    def try_read(self, reader):
        position = reader.get()
        if self._has_read_conflict(position):
            raise WaitingForWriterError

        data = self.array[position.index]
        reader.increment()
        return data


class SlotArray:

    def __init__(self, slot_bytes, slot_count):
        self.slot_bytes = slot_bytes
        self.slot_count = slot_count
        self.length_bytes = 4
        self.total_bytes = (slot_bytes + self.length_bytes) * slot_count
        self.array = multiprocessing.Array(ctypes.c_byte, self.total_bytes)

    def __getitem__(self, i):
        start = i * (self.slot_bytes + self.length_bytes)
        end = start + self.slot_bytes
        data = self.array[start:end]

        length_prefix = data[:self.length_bytes]
        length = struct.unpack('>I', length_prefix)

        return data[self.length_bytes:self.length_bytes + length]

    def __setitem__(self, i, data):
        data_size = len(data)
        if data_size > self.slot_bytes:
            raise ValueError('%d bytes too big for slot' % data_size)

        start = i * (self.slot_bytes + self.length_bytes)
        end = start + data_size

        length_prefix = struct.pack('>I', data_size)
        self.array[start:end] = length_prefix + data
