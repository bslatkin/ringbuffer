#!/usr/bin/env python3

"""Performance test of the RingBuffer class.

Example invocation:

./perf_test_ringbufer.py \
    --debug \
    --slot-bytes=1000000 \
    --slots=100 \
    --duration-seconds=10 \
    --writes-per-second=30 \
    --readers=5
"""

import argparse
import collections
import logging
import multiprocessing
import os
import random
import time

import ringbuffer


FLAGS = argparse.ArgumentParser()

FLAGS.add_argument('--debug', action='store_true')

FLAGS.add_argument('--duration-seconds', action='store',
                   type=int, required=True)

FLAGS.add_argument('--slots', action='store', type=int, required=True)

FLAGS.add_argument('--slot-bytes', action='store', type=int, required=True)

FLAGS.add_argument('--readers', action='store', type=int, required=True)

FLAGS.add_argument('--writes-per-second', action='store',
                   type=int, required=True)


def sleep_generator(duration_seconds, writes_per_second):
    start = time.time()
    end = start + duration_seconds
    target_duration = 1 / writes_per_second

    while True:
        before = time.time()
        if before >= end:
            return

        yield

        after = time.time()
        # TODO: Keep an average duration to better approximate the processing
        # time and keep the sleep time stable.
        last_duration = after - before
        next_delay = target_duration - last_duration

        if next_delay > 0:
            time.sleep(next_delay)


RANDOM_DATA = os.urandom(10 * 10**6)


def random_data(num_bytes):
    """Generate random input data from cached randomness.

    We do this because os.urandom can be very slow and that's not what this
    code is trying to load test.
    """
    assert num_bytes < len(RANDOM_DATA)
    index = random.randint(0, len(RANDOM_DATA) - num_bytes)
    return RANDOM_DATA[index:index + num_bytes]


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


def print_writer_stats(flags, writes, elapsed):
    print('Wrote %d slots in %f seconds' % (writes, elapsed.duration))
    writes_per_second = writes / elapsed.duration
    delta = writes_per_second - flags.writes_per_second
    percent_wrong = 100 * delta / flags.writes_per_second
    print('%f writes/second, %.1f%% relative to target' %
          (writes_per_second, percent_wrong))


def writer(flags, out_ring):
    print_every = flags.writes_per_second

    with Timing() as elapsed:
        it = sleep_generator(flags.duration_seconds, flags.writes_per_second)
        for i, _ in enumerate(it):
            data = random_data(flags.slot_bytes)
            try:
                out_ring.try_write(data)
            except ringbuffer.WaitingForReaderError:
                logging.error('Write %d is waiting for readers', i)
            else:
                if i and i % print_every == 0:
                    logging.info('Wrote %d slots so far', i)

        out_ring.close()

    print_writer_stats(flags, i, elapsed)
    logging.debug('Exiting writer')


def reader(flags, in_ring, reader):
    print_every = flags.writes_per_second
    read_duration = 1 / flags.writes_per_second
    reads = 0

    while True:
        try:
            in_ring.try_read(reader)
        except ringbuffer.WaitingForWriterError:
            # TODO: Replace this polling with a condition variable
            time.sleep(read_duration / 2)
            continue
        except ringbuffer.WriterFinishedError:
            break

        reads += 1
        if reads % print_every == 0:
            logging.info('%r read %d slots so far', reader, reads)

    logging.debug('Exiting reader %r', reader)


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
