#!/usr/bin/env python3

import logging
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

    def expect_read(self, expected_data):
        data = self.ring.try_read(self.pointer)
        self.testcase.assertEqual(expected_data, data)


class AsyncProxy:

    def __init__(self, expecter, in_queue, error_queue):
        self.expecter = expecter
        self.in_queue = in_queue
        self.error_queue = error_queue

    def run(self):
        while True:
            item = self.in_queue.get()
            if item == 'done':
                return

            name, args, kwargs = item
            print('Running %s(%r, %r)' % (name, args, kwargs))
            try:
                result = getattr(self.expecter, name)(*args, **kwargs)
            except Exception as e:
                logging.exception(
                    'Problem running %s(*%r, **%r)', name, args, kwargs)
                self.error_queue.put(e)

            self.in_queue.task_done()

    def shutdown(self):
        self.in_queue.put('done')

    def __getattr__(self, name):
        func = getattr(self.expecter, name)

        def proxy(*args, **kwargs):
            self.in_queue.put((name, args, kwargs))
            # Wait for this thread to finish executing the requested behavior
            # before allowing another thread's behavior to run.
            while not self.in_queue.empty():
                time.sleep(0.01)

        return proxy


class RingBufferTestBase:

    def setUp(self):
        self.ring = ringbuffer.RingBuffer(slot_bytes=20, slot_count=10)
        self.proxies = []
        self.error_queue = self.new_queue()

    def tearDown(self):
        for proxy in self.proxies:
            proxy.shutdown()
        # for proxy in self.proxies:
        #    proxy.in_queue.join()
        if not self.error_queue.empty():
            raise self.error_queue.get()

    def new_queue(self):
        pass

    def run_proxy(self, proxy):
        pass

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

    def test_one_reader__single_write(self):
        reader = self.new_reader()
        writer = self.writer()

        writer.expect_index(0)
        writer.write(b'first write')
        writer.expect_index(1)

        reader.expect_index(0)
        reader.expect_read(b'first write')
        reader.expect_index(1)

    # def test_one_reader__ahead_of_writes(self):
    #    reader = self.ring.new_reader()
    #    self.assertRaises(
    #        ringbuffer.WaitingForWriterError,
    #        self.ring.try_read,
    #        reader)

    #    self.ring.try_write(b'first write')
    #    data = self.ring.try_read(reader)
    #    self.assertEqual(b'first write', data)

    # def test_two_readers__one_behind_one_ahead(self):
    #    r1 = self.ring.new_reader()
    #    r2 = self.ring.new_reader()

    #    self.ring.try_write(b'first write')

    #    self.ring.try_read(r1)
    #    self.assertRaises(
    #        ringbuffer.WaitingForWriterError,
    #        self.ring.try_read,
    #        r1)

    #    self.ring.try_read(r2)
    #    self.assertRaises(
    #        ringbuffer.WaitingForWriterError,
    #        self.ring.try_read,
    #        r2)

    # def test_write_conflict__beginning(self):
    #    reader = self.ring.new_reader()
    #    for i in range(self.ring.slot_count):
    #        self.ring.try_write(b'write %d' % i)

    #    self.assertRaises(
    #        ringbuffer.WaitingForReaderError,
    #        self.ring.try_write,
    #        b'should not work')

    #    data = self.ring.try_read(reader)
    #    self.assertEqual(b'write 0', data)

    #    self.assertEqual(0, self.ring.writer.get().index)  # Wrapped around

    #    self.ring.try_write(b'now it works')
    #    for i in range(1, self.ring.slot_count):
    #        data = self.ring.try_read(reader)
    #        self.assertEqual(b'write %d' % i, data)

    #    self.assertEqual(0, reader.get().index)

    #    data = self.ring.try_read(reader)
    #    self.assertEqual(b'now it works', data)

    # def test_write_conflict__end(self):
    #    pass

    # def test_write_conflict__middle(self):
    #    pass

    # def test_create_reader_after_writing(self):
    #    pass


class LocalTest(RingBufferTestBase, unittest.TestCase):

    def new_queue(self):
        return queue.Queue()

    def run_proxy(self, proxy):
        thread = threading.Thread(target=proxy.run)
        thread.daemon = True
        thread.start()


# class MultiprocessingTest(RingBufferTestBase, unittest.TestCase):
    # pass


if __name__ == '__main__':
    unittest.main()
