"""Ring buffers for multiprocessing.

Allows multiple child Python processes started via the multiprocessing module
to read from a shared ring buffer in the parent process. For each child, a
pointer is maintained for the purpose of reading. One pointer is maintained by
the parent for the purpose of writing. Readers will have to wait if the writer
hasn't written anything new. The writer will have to wait if the readers
haven't caught up far enough in the ring buffer to make space.

For more background see:
https://docs.python.org/3/library/multiprocessing.html

Or read the source:
https://github.com/python/cpython/tree/3.5/Lib/multiprocessing
"""

import ctypes
import functools
import multiprocessing
import struct


class Error(Exception):
    pass


class DataTooLargeError(Error, ValueError):
    pass


class WaitingForReaderError(Error):
    pass


class WaitingForWriterError(Error):
    pass


class WriterFinishedError(Error):
    pass


class AlreadyClosedError(Error):
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
        return self.counter // self.slot_count


class Pointer:

    def __init__(self, slot_count, *, start=None):
        default = start if start is not None else 0
        self.counter = multiprocessing.RawValue(ctypes.c_longlong, default)
        self.position = Position(slot_count)

    def increment(self):
        self.counter.value += 1

    def get(self):
        # Avoid reallocating Position repeatedly.
        self.position.counter = self.counter.value
        return self.position


class RingBuffer:

    def __init__(self, *, slot_bytes, slot_count):
        self.slot_count = slot_count
        self.array = SlotArray(slot_bytes=slot_bytes, slot_count=slot_count)
        self.writer = Pointer(self.slot_count)
        self.active = multiprocessing.RawValue(ctypes.c_bool, True)
        self.readers = []
        self.lock = multiprocessing.Lock()
        self.condition = multiprocessing.Condition(self.lock)

    def new_reader(self):
        with self.lock:
            writer_position = self.writer.get()
            reader = Pointer(self.slot_count, start=writer_position.counter)
            self.readers.append(reader)
            return reader

    def _has_write_conflict(self, position):
        index = position.index
        generation = position.generation
        for reader in self.readers:
            # This Position and the other Position both point at the same index
            # in the ring buffer, but they have different generation numbers.
            # This means the writer can't proceed until some readers have
            # sufficiently caught up.
            reader_position = reader.get()
            if (reader_position.index == index and
                    reader_position.generation < generation):
                return True

        return False

    def try_write(self, data):
        with self.condition:
            if not self.active.value:
                raise AlreadyClosedError

            position = self.writer.get()
            if self._has_write_conflict(position):
                raise WaitingForReaderError

            self.array[position.index] = data
            self.writer.increment()

            self.condition.notify_all()

    def _has_read_conflict(self, reader_position):
        writer_position = self.writer.get()
        return writer_position.counter <= reader_position.counter

    def _try_read_no_lock(self, reader):
        position = reader.get()
        if self._has_read_conflict(position):
            if not self.active.value:
                raise WriterFinishedError
            else:
                raise WaitingForWriterError

        data = self.array[position.index]
        reader.increment()
        return data

    def try_read(self, reader):
        with self.lock:
            return self._try_read_no_lock(reader)

    def blocking_read(self, reader):
        with self.condition:
            while True:
                try:
                    return self._try_read_no_lock(reader)
                except WaitingForWriterError:
                    self.condition.wait()

    def force_reader_sync(self):
        # TODO: Force all readers to have the same position as the writer
        # and miss any data they hadn't read yet.
        pass

    def close(self):
        with self.condition:
            self.active.value = False
            self.condition.notify_all()


class SlotArray:

    def __init__(self, *, slot_bytes, slot_count):
        self.slot_bytes = slot_bytes
        self.slot_count = slot_count
        self.length_bytes = 4
        self.slot_type = ctypes.c_byte * (slot_bytes + self.length_bytes)
        self.array = multiprocessing.RawArray(self.slot_type, slot_count)

    def __getitem__(self, i):
        data = memoryview(self.array[i])
        length_prefix = data[:self.length_bytes]
        (length,) = struct.unpack('>I', length_prefix)

        start = self.length_bytes
        # This must create a copy because we want the writer to be able to
        # overwrite this slot as soon as the data has been retrieved by all
        # readers.
        return data[start:start + length].tobytes()

    def __setitem__(self, i, data):
        data_size = len(data)
        if data_size > self.slot_bytes:
            raise DataTooLargeError('%d bytes too big for slot' % data_size)

        # Avoid copying!
        slot_view = memoryview(self.array[i]).cast('B')
        struct.pack_into('>I', slot_view, 0, data_size)
        start = self.length_bytes
        slot_view[start:start + data_size] = data

    def __len__(self):
        return self.slot_count
