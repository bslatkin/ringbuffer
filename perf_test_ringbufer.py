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
FLAGS.add_argument('--slot-bytes', action='store', type=int)
FLAGS.add_argument('--slot-count', action='store', type=int)
FLAGS.add_argument('--duration-seconds', action='store', type=int)
FLAGS.add_argument('--slots-per-second', action='store', type=int)


def sleep_generator(duration_seconds, slots_per_second):
    start = time.time()
    end = start + duration_seconds
    frame_duration = 1 / slots_per_second

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


def fill_first(flags, out_ring):
    print_every = flags.slots_per_second
    it = sleep_generator(flags.duration_seconds, flags.slots_per_second)

    for i, _ in enumerate(it):
        data = os.urandom(flags.slot_bytes)
        try:
            out_ring.try_write(data)
        except ringbuffer.WaitingForReaderError:
            logging.error('Frame %d pending reader', i)
        else:
            if i and i % print_every == 0:
                logging.info('Written %d frames so far', i)

    out_ring.close()
    logging.debug('Exiting fill_first')


def second_top(flags, in_ring, reader):
    frame_duration = 1 / flags.slots_per_second
    frames_seen = 0

    while True:
        try:
            in_ring.try_read(reader)
        except ringbuffer.WaitingForWriterError:
            # TODO: Replace this polling with a condition variable
            time.sleep(frame_duration / 2)
            continue
        except ringbuffer.WriterFinishedError:
            logging.debug('Exiting second_top')
            return

        frames_seen += 1
        if frames_seen % 100 == 0:
            logging.info('Seen %d frames so far', frames_seen)


# def second_bottom(flags, in_ring, out_ring):
    # pass


# def third_fan_in(flags, top_ring, bottom_ring):
#    pass


def get_buffer(flags):
    return ringbuffer.RingBuffer(
        slot_bytes=flags.slot_bytes,
        slot_count=flags.slot_count)


def main():
    flags = FLAGS.parse_args()
    print('Starting performance test with flags: %r', flags)

    if flags.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    first = get_buffer(flags)
    # top = get_buffer(flags)
    # bottom = get_buffer(flags)

    processes = [
        multiprocessing.Process(
            target=fill_first,
            args=(flags, first)),
        multiprocessing.Process(
            target=second_top,
            args=(flags, first, first.new_reader()))
    ]

    for process in processes:
        process.start()

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
