#!/usr/bin/env python3

"""Performance test of the RingBuffer class."""

import argparse
import collections
import cProfile
import functools
import logging
import multiprocessing
import os
import pstats
import random
import struct
import time
import zlib

import ringbuffer


FLAGS = argparse.ArgumentParser()

FLAGS.add_argument('--debug', action='store_true')

FLAGS.add_argument('--duration-seconds', action='store',
                   type=int, required=True)

FLAGS.add_argument('--slots', action='store', type=int, required=True)

FLAGS.add_argument('--slot-bytes', action='store', type=int, required=True)

FLAGS.add_argument('--readers', action='store',
                   type=int, required=True)

FLAGS.add_argument('--reader-burn-cpu-milliseconds',
                   action='store', type=int, default=0)

FLAGS.add_argument('--writes-per-second', action='store',
                   type=int, required=True)

FLAGS.add_argument('--verify_writes', action='store_true',
                   dest='verify_writes')
FLAGS.add_argument('--no-verify_writes',
                   action='store_false', dest='verify_writes')
FLAGS.set_defaults(verify_writes=True)


def profile(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        profiler = cProfile.Profile()
        try:
            return profiler.runcall(func, *args, **kwargs)
        finally:
            stats = pstats.Stats(profiler)
            stats.strip_dirs()
            stats.sort_stats('tottime')
            stats.print_stats()

    return wrapper


def sleep_generator(duration_seconds, writes_per_second):
    start = time.time()
    end = start + duration_seconds
    target_duration = 1 / writes_per_second

    seen_durations = collections.deque(maxlen=10)

    while True:
        before = time.time()
        if before >= end:
            return

        yield

        after = time.time()
        last_duration = after - before
        # Keep an average duration to better approximate the processing
        # time and keep the sleep time stable. Otherwise the variability will
        # cause this method to oversleep.
        seen_durations.append(last_duration)

        avg_duration = sum(seen_durations) / len(seen_durations)
        next_delay = target_duration - avg_duration

        if next_delay > 0:
            time.sleep(next_delay)


# Using a memoryview prevents copying when random_data slices this value.
_CACHED_RANDOM_DATA = memoryview(100 * os.urandom(10 * 10**6))


def get_random_data(num_bytes):
    """Generate random input data from cached randomness.

    We do this because os.urandom can be very slow and that's not what this
    code is trying to load test.
    """
    index = random.randint(0, len(_CACHED_RANDOM_DATA) - num_bytes)
    return _CACHED_RANDOM_DATA[index:index + num_bytes]


def get_crc32(data):
    return zlib.crc32(data) & 0xffffffff


def generate_verifiable_data(num_bytes):
    random_size = num_bytes - 4
    random_data = get_random_data(random_size)
    crc = get_crc32(random_data)

    result = bytearray(num_bytes)
    result[:random_size] = random_data
    struct.pack_into('>I', result, random_size, crc)

    return result


def verify_data(data):
    random_size = len(data) - 4
    random_data = data[:random_size]
    found_crc = get_crc32(random_data)
    (expected_crc,) = struct.unpack_from('>I', data, random_size)

    assert expected_crc == found_crc, 'Expected crc %r, found crc %r' % (
        expected_crc, found_crc)


class Timing:

    def __init__(self, now=time.time):
        self.now = now
        self.start = None
        self.end = None
        self.duration = None

    def __enter__(self):
        self.start = self.now()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = self.now()
        self.duration = self.end - self.start
        return False


_PRINT_LOCK = multiprocessing.Lock()


def print_process_stats(process, flags, slots, elapsed):
    slots_per_second = slots / elapsed.duration
    mb_per_second = flags.slot_bytes * slots_per_second / 1e6
    delta = slots_per_second - flags.writes_per_second
    percent_wrong = 100 * delta / flags.writes_per_second

    message = """%(action)s
%(slots)d slots in %(duration)f seconds
%(bandwidth)f MBytes per second
%(rate)f slots/second, %(delta).1f%% relative to target
""" % {
        'action': process,
        'slots': slots,
        'duration': elapsed.duration,
        'rate': slots_per_second,
        'bandwidth': mb_per_second,
        'delta': percent_wrong,
    }

    with _PRINT_LOCK:
        print(message)


#@profile
def writer(flags, out_ring):
    print_every = flags.writes_per_second

    with Timing() as elapsed:
        it = sleep_generator(flags.duration_seconds, flags.writes_per_second)
        writes = 0
        for _ in it:
            if flags.verify_writes:
                data = generate_verifiable_data(flags.slot_bytes)
            else:
                data = get_random_data(flags.slot_bytes)

            try:
                out_ring.try_write(data)
            except ringbuffer.WaitingForReaderError:
                logging.error('Write %d is waiting for readers', writes)
                continue

            writes += 1
            if writes and writes % print_every == 0:
                logging.debug('Wrote %d slots so far', writes)

        out_ring.close()

    logging.debug('Exiting writer')
    print_process_stats('Writer', flags, writes, elapsed)


def burn_cpu(milliseconds):
    start = now = time.time()
    end = start + milliseconds / 1000
    while True:
        now = time.time()
        if now >= end:
            break
        for i in range(100):
            random.random() ** 1 / 2


#@profile
def reader(flags, in_ring, reader):
    print_every = flags.writes_per_second
    read_duration = 1 / flags.writes_per_second
    reads = 0

    with Timing() as elapsed:
        while True:
            try:
                data = in_ring.blocking_read(reader)
            except ringbuffer.WriterFinishedError:
                break

            if flags.verify_writes:
                verify_data(data)

            if flags.reader_burn_cpu_milliseconds:
                burn_cpu(flags.reader_burn_cpu_milliseconds)

            reads += 1
            if reads % print_every == 0:
                logging.info('%r read %d slots so far', reader, reads)

    logging.debug('Exiting reader %r', reader)
    print_process_stats('Reader %d' % id(reader), flags, reads, elapsed)


def get_buffer(flags):
    return ringbuffer.RingBuffer(
        slot_bytes=flags.slot_bytes,
        slot_count=flags.slots)


def main():
    flags = FLAGS.parse_args()
    print('Starting performance test with flags: %r' % flags)

    if flags.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    ring = get_buffer(flags)

    processes = [
        multiprocessing.Process(
            target=writer,
            args=(flags, ring))
    ]
    for i in range(flags.readers):
        processes.append(
            multiprocessing.Process(
                target=reader,
                args=(flags, ring, ring.new_reader()))
        )

    for process in processes:
        process.start()

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
