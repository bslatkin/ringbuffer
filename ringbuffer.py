"""Ring buffers for multiprocessing.

Allows multiple child Python processes started via the multiprocessing module
to read from a shared ring buffer in the parent process. For each child, a
pointer is maintained for the purpose of reading. One pointer is maintained by
for the purpose of writing. Reads may be issued in blocking or non-blocking
mode. Writes are always in non-blocking mode and will raise an exception
if the buffer is full.
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


class MustCreatedReadersBeforeWritingError(Error):
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

    def set(self, counter):
        self.counter.value = counter


class RingBuffer:
    """Circular buffer class accessible to multiple threads or child processes.

    All methods are thread safe. Multiple readers and writers are permitted.
    Before kicking off multiprocessing.Process instances, first allocate all
    of the readers you'll need with new_reader() and pass the Pointer value
    returned by that method to the multiprocessing.Process constructor along
    with the RingBuffer constructor. Calling new_reader() from a child
    multiprocessing.Process will not work.
    """

    def __init__(self, *, slot_bytes, slot_count):
        """Initializer.

        Args:
            slot_bytes: The maximum size of slots in the buffer.
            slot_count: How many slots should be in the buffer.
        """
        self.slot_count = slot_count
        self.array = SlotArray(slot_bytes=slot_bytes, slot_count=slot_count)
        self.writer = Pointer(self.slot_count)
        self.active = multiprocessing.RawValue(ctypes.c_uint, 0)
        self.readers = []
        self.lock = multiprocessing.Lock()
        self.condition = multiprocessing.Condition(self.lock)

    def new_reader(self):
        """Returns a new unique reader into the buffer.

        This must only be called in the parent process. It must not be
        called in a child multiprocessing.Process. See class docstring. To
        enforce this policy, no readers may be allocated after the first
        write has occurred.
        """
        with self.lock:
            writer_position = self.writer.get()
            if writer_position.counter > 0:
                raise MustCreatedReadersBeforeWritingError

            reader = Pointer(self.slot_count, start=writer_position.counter)
            self.readers.append(reader)
            return reader

    def new_writer(self):
        """Must be called once by each writer before any reads occur.

        Should be paired with a single subsequent call to writer_done() to
        indicate that this writer has finished and will not write any more
        data into the ring.
        """
        with self.lock:
            self.active.value += 1

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
        """Tries to write the next slot, but will not block.

        Once a successful write occurs, all pending blocking_read() calls
        will be woken up to consume the newly written slot.

        Args:
            data: Bytes to write in the next available slot. Must be
                less than or equal to slot_bytes in size.

        Raises:
            WaitingForReaderError: If all of the slots are full and we need
                to wait for readers to catch up before there will be
                sufficient room to write more data. This is a sign that
                the readers can't keep up with the writer. Consider calling
                force_reader_sync() if you need to force the readers to
                catch up, but beware that means they will miss data.
        """
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
        """Tries to read the next slot for a reader, but will not block.

        Args:
            reader: Position previously returned by the call to new_reader().

        Returns:
            bytearray containing a copy of the data from the slot. This
            value is mutable an can be used to back ctypes objects, NumPy
            arrays, etc.

        Raises:
            WriterFinishedError: If the RingBuffer was closed before this
                read operation began.
            WaitingForWriterError: If the given reader has already consumed
                all the data in the ring buffer and would need to block in
                order to wait for new data to arrive.
        """
        with self.lock:
            return self._try_read_no_lock(reader)

    def blocking_read(self, reader):
        """Reads the next slot for a reader, blocking if it isn't filled yet.

        Args:
            reader: Position previously returned by the call to new_reader().

        Returns:
            bytearray containing a copy of the data from the slot. This
            value is mutable an can be used to back ctypes objects, NumPy
            arrays, etc.

        Raises:
            WriterFinishedError: If the RingBuffer was closed while waiting
                to read the next operation.
        """
        with self.condition:
            while True:
                try:
                    return self._try_read_no_lock(reader)
                except WaitingForWriterError:
                    self.condition.wait()

    def force_reader_sync(self):
        """Forces all readers to skip to the position of the writer."""
        with self.condition:
            writer_position = self.writer.get()

            for reader in self.readers:
                reader.set(writer_position.counter)

            for reader in self.readers:
                p = reader.get()
                print('Reader %r: counter: %d' % (reader, p.counter))

            self.condition.notify_all()

    def writer_done(self):
        """Called by the writer when no more data is expected to be written.

        Should be called once for every corresponding call to new_writer().
        Once all writers have called writer_done(), a WriterFinishedError
        exception will be raised by any blocking read calls or subsequent
        calls to read.
        """
        with self.condition:
            self.active.value -= 1
            self.condition.notify_all()


class SlotArray:
    """Fast array of indexable buffers backed by shared memory.

    Assumes locking happens elsewhere.
    """

    def __init__(self, *, slot_bytes, slot_count):
        """Initializer.

        Args:
            slot_bytes: How big each buffer in the array should be.
            slot_count: How many buffers should be in the array.
        """
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
        # readers. But we also want the returned bytes to be mutable so that
        # the returned data can immediately back a ctypes record using the
        # from_buffer() method (instead of from_buffer_copy()).
        return bytearray(data[start:start + length])

    def __setitem__(self, i, data):
        data_view = memoryview(data).cast('@B')
        data_size = len(data_view)
        if data_size > self.slot_bytes:
            raise DataTooLargeError('%d bytes too big for slot' % data_size)

        # Avoid copying the input data! Do only a single copy into the slot.
        slot_view = memoryview(self.array[i]).cast('@B')
        struct.pack_into('>I', slot_view, 0, data_size)
        start = self.length_bytes
        slot_view[start:start + data_size] = data_view

    def __len__(self):
        return self.slot_count
