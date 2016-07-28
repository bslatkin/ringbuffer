#!/usr/bin/env python3

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


class RingBufferTestBase:

    def setUp(self):
        self.ring = ringbuffer.RingBuffer(slot_bytes=20, slot_count=10)
    #    self.sequence = []

    # def do(self, func):
    #    self.sequence.append((func, *args, **kwargs))

    # def run_all(self):
    #    for func, args, kwargs in self.sequence:

    #    self.sequence = []

    def test_one_reader__single_write(self):
        reader = self.ring.new_reader()
        self.ring.try_write(b'first write')
        data = self.ring.try_read(reader)
        self.assertEqual(b'first write', data)

    def test_one_reader__ahead_of_writes(self):
        reader = self.ring.new_reader()
        self.assertRaises(
            ringbuffer.WaitingForWriterError,
            self.ring.try_read,
            reader)
        self.ring.try_write(b'first write')
        data = self.ring.try_read(reader)
        self.assertEqual(b'first write', data)

    def test_two_readers__one_behind_one_ahead(self):
        r1 = self.ring.new_reader()
        r2 = self.ring.new_reader()

        self.ring.try_write(b'first write')

        self.ring.try_read(r1)
        self.assertRaises(
            ringbuffer.WaitingForWriterError,
            self.ring.try_read,
            r1)

        self.ring.try_read(r2)
        self.assertRaises(
            ringbuffer.WaitingForWriterError,
            self.ring.try_read,
            r2)


class LocalTest(RingBufferTestBase, unittest.TestCase):
    pass


class MultiprocessingTest(RingBufferTestBase, unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
