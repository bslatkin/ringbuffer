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

        self.assertEqual(0, self.ring.writer.get().index)
        self.ring.try_write(b'first write')
        self.assertEqual(1, self.ring.writer.get().index)

        self.assertEqual(0, reader.get().index)
        data = self.ring.try_read(reader)
        self.assertEqual(1, reader.get().index)

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

    def test_write_conflict__beginning(self):
        reader = self.ring.new_reader()
        for i in range(self.ring.slot_count):
            self.ring.try_write(b'write %d' % i)

        self.assertRaises(
            ringbuffer.WaitingForReaderError,
            self.ring.try_write,
            b'should not work')

        data = self.ring.try_read(reader)
        self.assertEqual(b'write 0', data)

        self.assertEqual(0, self.ring.writer.get().index)  # Wrapped around

        self.ring.try_write(b'now it works')
        for i in range(1, self.ring.slot_count):
            data = self.ring.try_read(reader)
            self.assertEqual(b'write %d' % i, data)

        self.assertEqual(0, reader.get().index)

        data = self.ring.try_read(reader)
        self.assertEqual(b'now it works', data)

    def test_write_conflict__end(self):
        pass

    def test_write_conflict__middle(self):
        pass

    def test_create_reader_after_writing(self):
        pass


class LocalTest(RingBufferTestBase, unittest.TestCase):
    pass


class MultiprocessingTest(RingBufferTestBase, unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
