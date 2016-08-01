#!/usr/bin/env python3

"""Simple example with numpy arrays."""

import multiprocessing

import numpy
import numpy.matlib
import ringbuffer


def writer(ring):
    for i in range(10000):
        m = numpy.matlib.randn(25, 100)
        x = numpy.ctypeslib.as_ctypes(m)
        try:
            ring.try_write(x)
        except ringbuffer.WaitingForReaderError:
            print('Reader is too slow, dropping %r' % x)
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

        x = numpy.frombuffer(data)
        x.shape = (25, 100)
        x[1, 1] = 1.1  # Verify it's mutable
        m = numpy.matlib.asmatrix(x)
        norm = numpy.linalg.norm(m)

    print('Reader %r is done' % pointer)


def main():
    ring = ringbuffer.RingBuffer(slot_bytes=50000, slot_count=100)
    ring.new_writer()

    processes = [
        multiprocessing.Process(target=writer, args=(ring,)),
    ]
    for i in range(10):
        processes.append(multiprocessing.Process(
            target=reader, args=(ring, ring.new_reader())))

    for p in processes:
        p.start()

    for p in processes:
        p.join(timeout=20)
        assert not p.is_alive()
        assert p.exitcode == 0


if __name__ == '__main__':
    main()
