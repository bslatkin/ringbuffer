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
    pass


class LocalTest(unittest.TestCase, RingBufferTestBase):
    pass


class MultiprocessingTest(unittest.TestCase, RingBufferTestBase):
    pass


if __name__ == '__main__':
    unittest.main()
