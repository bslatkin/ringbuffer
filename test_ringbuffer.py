#!/usr/bin/env python3

import gc
import logging
import multiprocessing
import queue
import threading
import time
import unittest

import ringbuffer


class SlotArrayTest(unittest.TestCase):

    def setUp(self):
        self.array = ringbuffer.SlotArray(slot_bytes=20, slot_count=10)

    def test_read_empty(self):
        for data in self.array:
            self.assertEqual(b'', data)

    def test_read_write(self):
        self.array[0] = b'hello'
        self.array[1] = b''
        self.array[5] = b'how are'
        self.array[9] = b'you doing today?'

        self.assertEqual(b'hello', self.array[0])
        self.assertEqual(b'', self.array[1])
        self.assertEqual(b'how are', self.array[5])
        self.assertEqual(b'you doing today?', self.array[9])

    def test_write_too_big(self):
        try:
            self.array[3] = b'asdfkljasdlfkajsflkjasdfasdfkljasdf'
            self.fail()
        except ringbuffer.DataTooLargeError:
            pass


class Expecter:

    def __init__(self, ring, pointer, testcase):
        self.ring = ring
        self.pointer = pointer
        self.testcase = testcase

    def expect_index(self, i):
        self.testcase.assertEqual(i, self.pointer.get().index)

    def write(self, data):
        self.ring.try_write(data)

    def _get_read_func(self, blocking):
        if blocking:
            return self.ring.blocking_read
        else:
            return self.ring.try_read

    def expect_read(self, expected_data, blocking=False):
        read = self._get_read_func(blocking)
        data = read(self.pointer)
        self.testcase.assertEqual(expected_data, data)

    def expect_waiting_for_writer(self):
        # There's no blocking version of this because the WaitingForWriterError
        # is what's used to determine when to block on the condition variable.
        self.testcase.assertRaises(
            ringbuffer.WaitingForWriterError,
            self.ring.try_read,
            self.pointer)

    def expect_waiting_for_reader(self):
        self.testcase.assertRaises(
            ringbuffer.WaitingForReaderError,
            self.ring.try_write,
            b'should not work')

    def close(self):
        self.ring.close()

    def expect_writer_finished(self, blocking=False):
        read = self._get_read_func(blocking)
        self.testcase.assertRaises(
            ringbuffer.WriterFinishedError,
            read,
            self.pointer)

    def expect_already_closed(self):
        self.testcase.assertRaises(
            ringbuffer.AlreadyClosedError,
            self.ring.try_write,
            b'should not work')


class AsyncProxy:

    def __init__(self, expecter, in_queue, error_queue):
        self.expecter = expecter
        self.in_queue = in_queue
        self.error_queue = error_queue
        self.runner = None

    def run(self):
        while True:
            item = self.in_queue.get()
            try:
                if item == 'done':
                    logging.debug('Exiting %r', self.runner)
                    return

                name, args, kwargs = item
                logging.debug('Running %s(%r, %r)', name, args, kwargs)
                try:
                    result = getattr(self.expecter, name)(*args, **kwargs)
                except Exception as e:
                    logging.exception(
                        'Problem running %s(*%r, **%r)', name, args, kwargs)
                    self.error_queue.put(e)
            finally:
                self.in_queue.task_done()

    def shutdown(self):
        self.in_queue.put('done')

    def __getattr__(self, name):
        func = getattr(self.expecter, name)

        def proxy(*args, **kwargs):
            self.in_queue.put((name, args, kwargs))
            # Wait for this thread to finish executing the requested behavior
            # before allowing another thread's behavior to run.
            self.in_queue.join()

        return proxy


class RingBufferTestBase:

    def setUp(self):
        self.ring = ringbuffer.RingBuffer(slot_bytes=20, slot_count=10)
        self.proxies = []
        self.error_queue = self.new_queue()

    def tearDown(self):
        for proxy in self.proxies:
            proxy.shutdown()
        for proxy in self.proxies:
            proxy.in_queue.join()
        if not self.error_queue.empty():
            raise self.error_queue.get()

        # Force child processes and pipes to be garbage collected, otherwise
        # we'll run out of file descriptors.
        gc.collect()

    def new_queue(self):
        raise NotImplementedError

    def run_proxy(self, proxy):
        raise NotImplementedError

    def new_reader(self):
        expecter = Expecter(self.ring, self.ring.new_reader(), self)
        proxy = AsyncProxy(expecter, self.new_queue(), self.error_queue)
        self.proxies.append(proxy)
        self.run_proxy(proxy)
        return proxy

    def writer(self):
        expecter = Expecter(self.ring, self.ring.writer, self)
        proxy = AsyncProxy(expecter, self.new_queue(), self.error_queue)
        self.proxies.append(proxy)
        self.run_proxy(proxy)
        return proxy

    def _do_read_single_write(self, blocking):
        reader = self.new_reader()
        writer = self.writer()

        writer.expect_index(0)
        writer.write(b'first write')
        writer.expect_index(1)

        reader.expect_index(0)
        reader.expect_read(b'first write', blocking=blocking)
        reader.expect_index(1)

    def test_read_single_write_blocking(self):
        self._do_read_single_write(True)

    def test_read_single_write_non_blocking(self):
        self._do_read_single_write(False)

    def _do_read_ahead_of_writes(self, blocking):
        reader = self.new_reader()
        writer = self.writer()

        reader.expect_waiting_for_writer()
        writer.write(b'first write')
        reader.expect_read(b'first write', blocking=blocking)

    def test_read_ahead_of_writes_blocking(self):
        self._do_read_ahead_of_writes(True)

    def test_read_ahead_of_writes_non_blocking(self):
        self._do_read_ahead_of_writes(False)

    def _do_two_reads_one_behind_one_ahead(self, blocking):
        r1 = self.new_reader()
        r2 = self.new_reader()

        writer = self.writer()
        writer.write(b'first write')

        r1.expect_read(b'first write', blocking=blocking)
        r1.expect_waiting_for_writer()

        r2.expect_read(b'first write', blocking=blocking)
        r2.expect_waiting_for_writer()

    def test_two_reads_one_behind_one_ahead_blocking(self):
        self._do_two_reads_one_behind_one_ahead(True)

    def test_two_reads_one_behind_one_ahead_non_blocking(self):
        self._do_two_reads_one_behind_one_ahead(False)

    def test_write_conflict__beginning(self):
        reader = self.new_reader()

        writer = self.writer()
        for i in range(self.ring.slot_count):
            writer.write(b'write %d' % i)

        writer.expect_index(0)  # Wrapped around
        writer.expect_waiting_for_reader()

        reader.expect_read(b'write 0')
        writer.write(b'now it works')

        for i in range(1, self.ring.slot_count):
            reader.expect_read(b'write %d' % i)

        reader.expect_index(0)
        reader.expect_read(b'now it works')

    # def test_write_conflict__end(self):
    #    pass

    # def test_write_conflict__middle(self):
    #    pass

    # def test_create_reader_after_writing(self):
    #    pass

    def _do_read_after_close_beginning(self, blocking):
        reader = self.new_reader()
        writer = self.writer()
        writer.close()
        reader.expect_writer_finished(blocking=blocking)

    def test_read_after_close_beginning_blocking(self):
        self._do_read_after_close_beginning(True)

    def test_read_after_close_beginning_non_blocking(self):
        self._do_read_after_close_beginning(False)

    def _do_close_before_read(self, blocking):
        reader = self.new_reader()
        writer = self.writer()

        writer.write(b'fill the buffer')
        writer.close()
        writer.expect_index(1)

        reader.expect_read(b'fill the buffer')
        reader.expect_writer_finished(blocking=blocking)
        reader.expect_index(1)

    def test_close_before_read_blocking(self):
        self._do_close_before_read(True)

    def test_close_before_read_non_blocking(self):
        self._do_close_before_read(False)

    def _do_close_after_read(self, blocking):
        reader = self.new_reader()
        writer = self.writer()

        writer.write(b'fill the buffer')

        reader.expect_read(b'fill the buffer')
        reader.expect_waiting_for_writer()
        reader.expect_index(1)

        writer.close()
        writer.expect_index(1)

        reader.expect_writer_finished(blocking=blocking)

    def test_close_after_read_blocking(self):
        self._do_close_after_read(True)

    def test_close_after_read_non_blocking(self):
        self._do_close_after_read(False)

    def test_close_then_write(self):
        writer = self.writer()
        writer.write(b'one')
        writer.close()
        writer.expect_already_closed()

    def test_blocking_readers_wake_up_after_write(self):
        self.fail()


class LocalTest(RingBufferTestBase, unittest.TestCase):

    def new_queue(self):
        return queue.Queue()

    def run_proxy(self, proxy):
        thread = threading.Thread(target=proxy.run)
        proxy.runner = thread
        thread.daemon = True
        thread.start()


class MultiprocessingTest(RingBufferTestBase, unittest.TestCase):

    def new_queue(self):
        return multiprocessing.JoinableQueue()

    def run_proxy(self, proxy):
        process = multiprocessing.Process(target=proxy.run)
        proxy.runner = process
        process.daemon = True
        process.start()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
