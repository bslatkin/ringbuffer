#!/usr/bin/env python3

import argparse
import collections
import logging
import multiprocessing
import os
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
    frame_duration = 1 / writes_per_second

    while True:
        before = time.time()
        if before >= end:
            return

        yield

        after = time.time()
        duration = after - before
        next_frame_delay = frame_duration - duration

        if next_frame_delay > 0:
            time.sleep(next_frame_delay)


def writer(flags, out_ring):
    print_every = flags.writes_per_second
    it = sleep_generator(flags.duration_seconds, flags.writes_per_second)

    for i, _ in enumerate(it):
        data = os.urandom(flags.slot_bytes)
        try:
            out_ring.try_write(data)
        except ringbuffer.WaitingForReaderError:
            logging.error('Write %d pending readers', i)
        else:
            if i and i % print_every == 0:
                logging.info('Wrote %d slots so far', i)

    out_ring.close()
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
