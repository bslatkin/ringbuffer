A multiple writer and reader [ring buffer](https://en.wikipedia.org/wiki/Circular_buffer) that allows for high-throughput data transfer between [multiproccessing](https://docs.python.org/3/library/multiprocessing.html) Python processes. The goal is to make it easy to construct bandwidth-heavy pipelines (e.g., for video stream processing) that can utilize multiple cores with Python tools like [OpenCV](http://docs.opencv.org/3.0-last-rst/doc/py_tutorials/py_video/py_table_of_contents_video/py_table_of_contents_video.html#py-table-of-content-video), [Scikit Learn](http://scikit-learn.org/stable/auto_examples/classification/plot_digits_classification.html), and [TensorFlow](https://www.tensorflow.org/versions/r0.10/tutorials/image_recognition/index.html).

The [`RingBuffer`](ringbuffer.py) data structure's performance is primarily bound by the behavior of the [Lock class](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Lock), which is a Kernel semaphore under the covers. The lock is held during all reading and writing operations, meaning lock contention dominates as the number of readers (or writes per second) increases. Memory performance isn't an issue because all data is transferred through [mmap'ed buffers](https://en.wikipedia.org/wiki/Mmap#Memory_visibility).

On an old MacBook, the ring buffer can easily do 2 gigabytes per second of transfer when using large slot sizes (~100MB), a relatively low number of writes per second (~24), and a single reader/writer pair. As you increase the number of reads and writes the performance degrades proportionately.

For an example of how it all fits together, look at [example_numpy.py](example_numpy.py) or [example_ctypes.py](example_ctypes.py).

[![Build Status](https://travis-ci.org/bslatkin/ringbuffer.svg?branch=master)](https://travis-ci.org/bslatkin/ringbuffer)

Works with Python 3.5 and later. Background on why it doesn't work with Python 3.3 and 3.4 [is here](http://bugs.python.org/issue15944).

---

Included is a tool called [perf_test_ringbuffer.py](perf_test_ringbuffer) that tests the performance characteristics of the ring buffer. This can be used to exercise the various ways in which the `RingBuffer` scales, including number of readers, number of writes per second, slow readers, etc.

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

Example that shows how the writer will fall behind its target rate when the locking overhead becomes too large:

```
./perf_test_ringbuffer.py \
    --debug \
    --slot-bytes=1000000 \
    --slots=10 \
    --duration-seconds=10 \
    --writes-per-second=2000 \
    --readers=1
```


Example that shows how the writer will fall behind its target rate when the requested data transfer rate is too high for the memory performance of the machine:

```
./perf_test_ringbuffer.py \
    --debug \
    --slot-bytes=100000000 \
    --slots=3 \
    --duration-seconds=10 \
    --writes-per-second=24 \
    --readers=1 \
    --no-verify_writes
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
