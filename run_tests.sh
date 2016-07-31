#!/bin/bash

./test_ringbuffer.py || exit 1

./example_numpy.py || exit 1

./perf_test_ringbuffer.py \
    --debug \
    --slot-bytes=1000000 \
    --slots=50 \
    --duration-seconds=10 \
    --writes-per-second=24 \
    --readers=5 \
|| exit 1
