A multiple reader [ring buffer](https://en.wikipedia.org/wiki/Circular_buffer) that allows for high throughput data transfer between [multiproccessing](https://docs.python.org/3/library/multiprocessing.html) Python processes.

The [`RingBuffer`](ringbuffer.py) data structure's performance is primarily bound by the behavior of the [Lock class](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Lock), which is a Kernel semaphore under the covers. The lock is held during all reading and writing operations, meaning lock contention dominates as the number of readers increases. Memory performance isn't an issue because all data is transferred through [mmap'ed buffers](https://en.wikipedia.org/wiki/Mmap#Memory_visibility).

For an example of how it all fits together, look at [perf_test_ringbuffer.py](perf_test_ringbuffer.py).

Example that shows good behavior:

```
./perf_test_ringbuffer.py \
    --debug \
    --slot-bytes=1000000 \
    --slots=50 \
    --duration-seconds=10 \
    --writes-per-second=24 \
    --readers=5
```

Example that shows that too many readers will slow the system down due to lock contention:

```
./perf_test_ringbuffer.py \
    --debug \
    --slot-bytes=1000000 \
    --slots=50 \
    --duration-seconds=10 \
    --writes-per-second=24 \
    --readers=50
```

Example that shows how the writer will fall behind its target rate when the requested data transfer rate is too high for the memory performance of the machine:

```
./perf_test_ringbuffer.py \
    --debug \
    --slot-bytes=1000000 \
    --slots=10 \
    --duration-seconds=10 \
    --writes-per-second=1000 \
    --readers=1
```

Example that shows what happens when the readers can't keep up with the writer:

```
./perf_test_ringbuffer.py \
    --debug \
    --slot-bytes=1000000 \
    --slots=10 \
    --duration-seconds=3 \
    --writes-per-second=24 \
    --readers=4 \
    --reader-burn-cpu-milliseconds=100
```
