#!/usr/bin/env python3

import ctypes
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


class ReadersWriterLockTest(unittest.TestCase):

    def setUp(self):
        self.lock = ringbuffer.ReadersWriterLock()

    def assert_readers(self, count):
        self.assertEqual(count, self.lock.readers.value)

    def assert_writer(self, held):
        self.assertEqual(held, self.lock.writer.value)

    def test_read_then_write(self):
        self.assert_readers(0)
        self.assert_writer(False)

        with self.lock.for_read():
            self.assert_readers(1)
            self.assert_writer(False)

        self.assert_readers(0)
        self.assert_writer(False)

        with self.lock.for_write():
            self.assert_readers(0)
            self.assert_writer(True)

        self.assert_readers(0)
        self.assert_writer(False)

    def test_concurrent_readers(self):
        # TODO: use multiple processes for this threads
        self.assert_readers(0)
        self.assert_writer(False)

        with self.lock.for_read():
            self.assert_readers(1)
            self.assert_writer(False)

            with self.lock.for_read():
                self.assert_readers(2)
                self.assert_writer(False)

                with self.lock.for_read():
                    self.assert_readers(3)
                    self.assert_writer(False)

                self.assert_readers(2)
                self.assert_writer(False)

            self.assert_readers(1)
            self.assert_writer(False)

        self.assert_readers(0)
        self.assert_writer(False)

    def _async(self, func):
        event = multiprocessing.Event()
        process = multiprocessing.Process(target=func, args=(event,))
        process.start()
        event.wait()
        return process

    def test_writer_blocks_reader(self):
        with self.lock.for_write():
            def test(event):
                self.assert_readers(0)
                self.assert_writer(True)

                event.set()

                with self.lock.for_read():
                    self.assert_readers(1)
                    self.assert_writer(False)

            p = self._async(test)

        p.join()

        self.assert_readers(0)
        self.assert_writer(False)

    def test_writer_blocks_multiple_readers(self):
        pass

    def test_reader_blocks_writer(self):
        pass

    def test_multiple_readers_block_writer(self):
        pass

    def test_multiple_writers_block_each_other(self):
        pass

    def test_wait_for_write(self):
        pass

    def test_wait_for_write_without_lock(self):
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

    def write_memory_view(self, data):
        view = memoryview(data)
        self.ring.try_write(view)

    def write_ctype(self, data):
        data_type = ctypes.c_double * len(data)
        cdata = data_type()
        cdata[:] = data
        self.ring.try_write(cdata)

    def _get_read_func(self, blocking):
        if blocking:
            return self.ring.blocking_read
        else:
            return self.ring.try_read

    def expect_read(self, expected_data, blocking=False):
        read = self._get_read_func(blocking)
        data = read(self.pointer)
        self.testcase.assertEqual(expected_data, data, 'Data was: %r' % data)

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

    def writer_done(self):
        self.ring.writer_done()

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

    def force_reader_sync(self):
        self.ring.force_reader_sync()

    def expect_try_read_type(self, type_or_class):
        data = self.ring.try_read(self.pointer)
        self.testcase.assertTrue(isinstance(data, type_or_class))


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
            self.expecter.testcase.assertTrue(
                self.runner,
                'Must call start_proxies() before setting test expectations')

            # This queue is used to sequence operations between functions
            # that are running asynchronously (threads or processes).
            self.in_queue.put((name, args, kwargs))

            # If this test function is running in blocking mode, that means
            # the locking and sequencing is built into the semantics of the
            # function call itself. That means we can skip waiting for the
            # asynchronous function to consume the queue before letting
            # subsequent test methods run.
            if kwargs.get('blocking'):
                # Allow a context switch so the asynchronous function has
                # a chance to actually start the function call.
                time.sleep(0.1)
            else:
                self.in_queue.join()

        return proxy


class RingBufferTestBase:

    def setUp(self):
        self.ring = ringbuffer.RingBuffer(slot_bytes=100, slot_count=10)
        self.proxies = []
        self.error_queue = self.new_queue()

    def tearDown(self):
        for proxy in self.proxies:
            if proxy.runner:
                proxy.shutdown()
        for proxy in self.proxies:
            if proxy.runner:
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

    def start_proxies(self):
        for proxy in self.proxies:
            self.run_proxy(proxy)

    def new_reader(self):
        expecter = Expecter(self.ring, self.ring.new_reader(), self)
        proxy = AsyncProxy(expecter, self.new_queue(), self.error_queue)
        self.proxies.append(proxy)
        return proxy

    def new_writer(self):
        self.ring.new_writer()
        expecter = Expecter(self.ring, self.ring.writer, self)
        proxy = AsyncProxy(expecter, self.new_queue(), self.error_queue)
        self.proxies.append(proxy)
        return proxy

    def test_write_bytes(self):
        writer = self.new_writer()
        self.start_proxies()
        writer.write(b'this works')

    def test_write_string(self):
        writer = self.new_writer()
        self.start_proxies()
        self.assertTrue(self.error_queue.empty())
        writer.write('this does not work')
        error = self.error_queue.get()
        self.assertTrue(isinstance(error, TypeError))

    def test_write_bytearray(self):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        byte_list = [124, 129, 92, 3, 97]
        data = bytearray(byte_list)
        writer.write(data)

        expected_bytes = b'|\x81\\\x03a'
        self.assertListEqual(list(expected_bytes), byte_list)
        reader.expect_read(expected_bytes)

    def test_write_memoryview(self):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        data = b'|\x81\\\x03a'
        writer.write_memory_view(data)
        reader.expect_read(data)

    def test_write_ctype_array(self):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        data = [
            0.10547615602385774,
            0.7852261064650733,
            0.9641224591137485,
            0.7119325400788387,
            0.0351822948099656,
            0.7533559074003938,
            0.40285734175834087,
            0.9567564883196842,
            0.38539673218346415,
            0.2682555751644704,
        ]
        writer.write_ctype(data)

        expected_bytes = (
            b'\xe0X\xa1@|\x00\xbb?\xf3s\xe7\x7f\x92 \xe9?\xd8q\xe7W\x17\xda'
            b'\xee?)\x19\x13\xc0&\xc8\xe6?\x00\xcd6\xebi\x03\xa2?\x1f\x0f'
            b'\x11\xd9}\x1b\xe8?r\x8e\xf3(j\xc8\xd9?\x044r\xc8\xbf\x9d\xee?'
            b'\xe0\xa5-\x0eW\xaa\xd8?\xbcD\x93n\x19+\xd1?')
        reader.expect_read(expected_bytes)

        data_type = ctypes.c_double * len(data)
        expected = data_type.from_buffer_copy(expected_bytes)
        self.assertEqual(list(expected), data)

    def _do_read_single_write(self, blocking):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        writer.expect_index(0)
        writer.write(b'first write')
        writer.expect_index(1)

        reader.expect_index(0)
        reader.expect_read(b'first write', blocking=blocking)
        reader.expect_index(1)

    def test_read_is_bytes(self):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        writer.write(b'this works')
        reader.expect_try_read_type(bytearray)

    def test_read_single_write_blocking(self):
        self._do_read_single_write(True)

    def test_read_single_write_non_blocking(self):
        self._do_read_single_write(False)

    def _do_read_ahead_of_writes(self, blocking):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

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
        writer = self.new_writer()
        self.start_proxies()

        writer.write(b'first write')

        r1.expect_read(b'first write', blocking=blocking)
        r1.expect_waiting_for_writer()

        r2.expect_read(b'first write', blocking=blocking)
        r2.expect_waiting_for_writer()

    def test_two_reads_one_behind_one_ahead_blocking(self):
        self._do_two_reads_one_behind_one_ahead(True)

    def test_two_reads_one_behind_one_ahead_non_blocking(self):
        self._do_two_reads_one_behind_one_ahead(False)

    def test_write_conflict_first_slot(self):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        for i in range(self.ring.slot_count):
            writer.write(b'write %d' % i)

        # The writer has wrapped around and is now waiting for the reader
        # to free up a slot. They have the same index, but are different
        # generations.
        reader.expect_index(0)
        writer.expect_index(0)
        writer.expect_waiting_for_reader()

        reader.expect_read(b'write 0')
        writer.write(b'now it works')

        for i in range(1, self.ring.slot_count):
            reader.expect_read(b'write %d' % i)

        reader.expect_index(0)
        reader.expect_read(b'now it works')

    def test_write_conflict_last_slot(self):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        last_slot = self.ring.slot_count - 1
        self.assertGreater(last_slot, 0)

        for i in range(last_slot):
            data = b'write %d' % i
            writer.write(data)
            reader.expect_read(data)

        writer.expect_index(last_slot)
        reader.expect_index(last_slot)

        # The reader's pointed at the last slot, now wrap around the writer
        # to catch up. They'll have the same index, but different generation
        # numbers.
        for i in range(self.ring.slot_count):
            data = b'write %d' % (self.ring.slot_count + i)
            writer.write(data)

        reader.expect_index(last_slot)
        writer.expect_index(last_slot)
        writer.expect_waiting_for_reader()

        reader.expect_read(b'write 10')
        writer.write(b'now it works')
        writer.expect_index(0)
        reader.expect_index(0)

    def test_create_reader_after_writing(self):
        writer = self.new_writer()
        self.start_proxies()

        self.new_reader()  # No error because no writes happened yet.

        writer.write(b'hello')
        self.assertRaises(
            ringbuffer.MustCreatedReadersBeforeWritingError,
            self.new_reader)

    def _do_read_after_close_beginning(self, blocking):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        writer.writer_done()
        reader.expect_writer_finished(blocking=blocking)

    def test_read_after_close_beginning_blocking(self):
        self._do_read_after_close_beginning(True)

    def test_read_after_close_beginning_non_blocking(self):
        self._do_read_after_close_beginning(False)

    def _do_close_before_read(self, blocking):
        reader = self.new_reader()
        writer = self.new_writer()
        self.start_proxies()

        writer.write(b'fill the buffer')
        writer.writer_done()
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
        writer = self.new_writer()
        self.start_proxies()

        writer.write(b'fill the buffer')

        reader.expect_read(b'fill the buffer')
        reader.expect_waiting_for_writer()
        reader.expect_index(1)

        writer.writer_done()
        writer.expect_index(1)

        reader.expect_writer_finished(blocking=blocking)

    def test_close_after_read_blocking(self):
        self._do_close_after_read(True)

    def test_close_after_read_non_blocking(self):
        self._do_close_after_read(False)

    def test_close_then_write(self):
        writer = self.new_writer()
        self.start_proxies()

        writer.write(b'one')
        writer.writer_done()
        writer.expect_already_closed()

    def test_blocking_readers_wake_up_after_write(self):
        writer = self.new_writer()
        r1 = self.new_reader()
        r2 = self.new_reader()
        self.start_proxies()

        r1.expect_read(b'write after read', blocking=True)
        r2.expect_read(b'write after read', blocking=True)

        writer.write(b'write after read')

    def test_blocking_readers_wake_up_after_close(self):
        writer = self.new_writer()
        r1 = self.new_reader()
        r2 = self.new_reader()
        self.start_proxies()

        r1.expect_writer_finished(blocking=True)
        r2.expect_writer_finished(blocking=True)

        writer.writer_done()

    def test_force_reader_sync(self):
        writer = self.new_writer()
        r1 = self.new_reader()
        r2 = self.new_reader()
        self.start_proxies()

        writer.write(b'one')
        writer.write(b'two')
        writer.write(b'three')

        writer.expect_index(3)
        r1.expect_index(0)
        r2.expect_index(0)

        writer.force_reader_sync()
        r1.expect_index(3)
        r2.expect_index(3)

    def _do_multiple_writers(self, blocking):
        w1 = self.new_writer()
        w2 = self.new_writer()
        reader = self.new_reader()
        self.start_proxies()

        w1.write(b'aaa')
        w1.expect_index(1)
        w2.expect_index(1)

        w2.write(b'bbb')
        w1.expect_index(2)
        w2.expect_index(2)

        w2.write(b'ccc')
        w1.expect_index(3)
        w2.expect_index(3)

        w1.write(b'ddd')
        w1.expect_index(4)
        w2.expect_index(4)

        reader.expect_read(b'aaa', blocking=blocking)
        reader.expect_read(b'bbb', blocking=blocking)
        reader.expect_read(b'ccc', blocking=blocking)
        reader.expect_read(b'ddd', blocking=blocking)

    def test_multiple_writers_blocking(self):
        self._do_multiple_writers(True)

    def test_multiple_writers_non_blocking(self):
        self._do_multiple_writers(False)

    def _do_test_multiple_writers_close(self, blocking):
        w1 = self.new_writer()
        w2 = self.new_writer()
        reader = self.new_reader()
        self.start_proxies()

        w1.write(b'aaa')
        w1.writer_done()

        w2.write(b'bbb')
        w2.writer_done()

        reader.expect_read(b'aaa', blocking=blocking)
        reader.expect_read(b'bbb', blocking=blocking)
        reader.expect_writer_finished(blocking=blocking)

    def test_multiple_writers_close_blocking(self):
        self._do_test_multiple_writers_close(True)

    def test_multiple_writers_close_non_blocking(self):
        self._do_test_multiple_writers_close(False)

    def _do_start_read_before_writer_setup(self, blocking):
        reader = self.new_reader()
        self.start_proxies()
        reader.expect_writer_finished(blocking=blocking)

    def test_start_read_before_writer_setup_blocking(self):
        self._do_start_read_before_writer_setup(True)

    def test_start_read_before_writer_setup_non_blocking(self):
        self._do_start_read_before_writer_setup(False)


class ThreadingTest(RingBufferTestBase, unittest.TestCase):

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
