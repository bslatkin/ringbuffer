#!/usr/bin/env python3

import argparse
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


def fill_first(flags, out_ring):
    start = time.time()
    end = start + flags.duration_seconds
    frame_duration = 1 / flags.slots_per_second
    frames_written = 0

    while True:
        now = time.time()
        if now >= end:
            logging.debug('Exiting fill_first')
            break

        for i in range(flags.slots_per_second):
            frame_begin = time.time()

            data = os.urandom(flags.slot_bytes)
            try:
                out_ring.try_write(data)
            except ringbuffer.WaitingForReaderError:
                logging.error('Frame %d at time %f pending reader',
                              i, now - start)
            else:
                frames_written += 1
                if frames_written % 100 == 0:
                    logging.info('Written %d frames so far', frames_written)

            frame_end = time.time()
            last_frame_duration = frame_end - frame_begin
            next_frame_delay = frame_duration - last_frame_duration
            if next_frame_delay <= 0:
                logging.error('Frame %d at time %f is falling behind',
                              i, now - start)

            time.sleep(next_frame_delay)


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
