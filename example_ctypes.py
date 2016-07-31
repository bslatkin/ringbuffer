#!/usr/bin/env python3

"""Simple example with ctypes.Structures."""

import ctypes
import multiprocessing
import os
import random
import time

import ringbuffer


class Record(ctypes.Structure):
    _fields_ = [
        ('write_number', ctypes.c_uint),
        ('timestamp_microseconds', ctypes.c_ulonglong),
        ('length', ctypes.c_uint),
        ('data', ctypes.c_ubyte * 1000),
    ]


def writer(ring, start, count):
    for i in range(start, start + count):
        data = os.urandom(random.randint(1, 1000))
        time_micros = int(time.time() * 10**6)
        record = Record(
            write_number=i,
            timestamp_microseconds=time_micros,
            length=len(data))
        # Note: You can't pass 'data' to the constructor without doing an
        # additional copy to convert the bytes type to a c_ubyte * 1000. So
        # instead, the constructor will initialize the 'data' field's bytes
        # to zero, and then this assignment overwrites the data-sized part.
        record.data[:len(data)] = data

        try:
            ring.try_write(record)
        except ringbuffer.WaitingForReaderError:
            print('Reader is too slow, dropping %d' % i)
            continue

        if i and i % 100 == 0:
            print('Wrote %d so far' % i)

    ring.writer_done()
    print('Writer is done')


def reader(ring, pointer):
    while True:
        try:
            data = ring.blocking_read(pointer)
        except ringbuffer.WriterFinishedError:
            return

        record = Record.from_buffer_copy(data)
        if record.write_number and record.write_number % 100 == 0:
            print('Reader %s saw record %d at timestamp %d with %d bytes' %
                  (id(pointer), record.write_number,
                   record.timestamp_microseconds, record.length))

    print('Reader %r is done' % id(pointer))


def main():
    ring = ringbuffer.RingBuffer(slot_bytes=50000, slot_count=10)
    ring.new_writer()

    processes = [
        multiprocessing.Process(target=reader, args=(ring, ring.new_reader())),
        multiprocessing.Process(target=reader, args=(ring, ring.new_reader())),
        multiprocessing.Process(target=writer, args=(ring, 1, 1000)),
    ]

    for p in processes:
        p.daemon = True
        p.start()

    for p in processes:
        p.join(timeout=20)
        assert not p.is_alive()
        assert p.exitcode == 0


if __name__ == '__main__':
    main()
