# These test cases are adapted from python's own test for file validation
# They are licensed under the
# PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
# https://github.com/python/cpython/blob/main/LICENSE

import contextlib
import gc
import json
import pickle
import subprocess
import sys
import unittest
from array import array
from collections import UserList
from pathlib import Path
from weakref import proxy

from gi.repository import GLib, Gio

import gio_pyio


class GioPyIO(unittest.TestCase):

    def setUp(self):
        self.file, stream = Gio.File.new_tmp('TestGFile.XXXXXX')
        stream.close()
        self.f = gio_pyio.open(self.file, 'wb', buffering=0, native=False)

    def tearDown(self):
        if self.f:
            self.f.close()
        with contextlib.suppress(GLib.Error):
            self.file.delete(None)

    def testNative(self):
        self.f.close()
        self.f = gio_pyio.open(self.file, 'wb', buffering=0, native=False)
        self.assertTrue(isinstance(self.f, gio_pyio.StreamWrapper))

    def testContextManager(self):
        self.f.close()
        with gio_pyio.open(self.file, 'wb', buffering=0, native=False) as f:
            f.write(b'Hello!')

    def testPickle(self):
        data = {'test': 'Hello!'}
        pickle.dump(data, self.f)

        new_file = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
        self.assertRaises(TypeError, pickle.dump, new_file, self.f)

    #def testWeakRefs(self):
    #    # verify weak references
    #    p = proxy(self.f)
    #    p.write(b'teststring')
    #    self.assertEqual(self.f.tell(), p.tell())
    #    self.f.close()
    #    self.f = None
    #    gc.collect()
    #    self.assertRaises(ReferenceError, getattr, p, 'tell')

    def testSeekTell(self):
        self.f.write(bytes(range(20)))
        self.assertEqual(self.f.tell(), 20)
        self.f.seek(0)
        self.assertEqual(self.f.tell(), 0)
        self.f.seek(10)
        self.assertEqual(self.f.tell(), 10)
        self.f.seek(5, 1)
        self.assertEqual(self.f.tell(), 15)
        self.f.seek(-5, 1)
        self.assertEqual(self.f.tell(), 10)
        self.f.seek(-5, 2)
        self.assertEqual(self.f.tell(), 15)

    def testAttributes(self):
        # verify expected attributes exist
        self.f.closed   # merely shouldn't blow up

    def testReadAndWrite(self):
        self.f.close()
        self.f = gio_pyio.open(self.file, 'wb', buffering=0, native=False)
        self.f.write(b'Hello World!')
        self.f.flush()
        self.f.close()
        self.f = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
        self.assertEqual(self.f.read(0), b'')
        self.assertEqual(self.f.read(5), b'Hello')
        self.assertEqual(self.f.read(), b' World!')

    def testReadinto(self):
        # verify readinto
        self.f.write(b'12')
        self.f.close()
        a = array('b', b'x' * 10)
        self.f = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
        n = self.f.readinto(a)
        self.assertEqual(b'12', a.tobytes()[:n])

    def testReadinto_text(self):
        # verify readinto refuses text files
        a = array('b', b'x' * 10)
        self.f.close()
        self.f = gio_pyio.open(self.file, encoding='utf-8', native=False)
        if hasattr(self.f, 'readinto'):
            self.assertRaises(TypeError, self.f.readinto, a)

    def testReadintoByteArray(self):
        self.f.write(bytes([1, 2, 0, 255]))
        self.f.close()

        ba = bytearray(b'abcdefgh')
        with gio_pyio.open(self.file, 'rb', buffering=0, native=False) as f:
            n = f.readinto(ba)
        self.assertEqual(ba, b'\x01\x02\x00\xffefgh')
        self.assertEqual(n, 4)

    def testWritelinesUserList(self):
        # verify writelines with instance sequence
        userlist = UserList([b'1', b'2'])
        self.f.writelines(userlist)
        self.f.close()
        self.f = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
        buf = self.f.read()
        self.assertEqual(buf, b'12')

    def testWritelinesIntegers(self):
        # verify writelines with integers
        self.assertRaises(TypeError, self.f.writelines, [1, 2, 3])

    def testWritelinesIntegersUserList(self):
        # verify writelines with integers in UserList
        userlist = UserList([1, 2, 3])
        self.assertRaises(TypeError, self.f.writelines, userlist)

    def testWritelinesNonString(self):
        # verify writelines with non-string object
        class NonString:
            pass

        self.assertRaises(TypeError, self.f.writelines,
                          [NonString(), NonString()])

    def testWritelinesError(self):
        self.assertRaises(TypeError, self.f.writelines, [1, 2, 3])
        self.assertRaises(TypeError, self.f.writelines, None)
        self.assertRaises(TypeError, self.f.writelines, 'abc')

    def testErrors(self):
        f = self.f
        self.assertFalse(f.isatty())
        self.assertFalse(f.closed)

        if hasattr(f, 'readinto'):
            self.assertRaises((OSError, TypeError), f.readinto, '')
        f.close()
        self.assertTrue(f.closed)

    def testReject(self):
        self.assertRaises(TypeError, self.f.write, 'Hello!')

    def testMethods(self):
        methods = [('fileno', ()),
                   ('flush', ()),
                   ('isatty', ()),
                   ('__next__', ()),
                   ('read', ()),
                   ('write', (b'',)),
                   ('readline', ()),
                   ('readlines', ()),
                   ('seek', (0,)),
                   ('tell', ()),
                   ('write', (b'',)),
                   ('writelines', ([],)),
                   ('__iter__', ()),
                   ('truncate', ()),
                   ]

        # __exit__ should close the file
        self.f.__exit__(None, None, None)
        self.assertTrue(self.f.closed)

        for methodname, args in methods:
            method = getattr(self.f, methodname)
            # should raise on closed file
            self.assertRaises(ValueError, method, *args)

        # file is closed, __exit__ shouldn't do anything
        self.assertEqual(self.f.__exit__(None, None, None), None)
        # it must also return None if an exception was given
        try:
            1 / 0
        except ZeroDivisionError:
            self.assertEqual(self.f.__exit__(*sys.exc_info()), None)

    def testReadWhenWriting(self):
        self.assertRaises(OSError, self.f.read)

    def testModeStrings(self):
        # check invalid mode strings
        for mode in ('', 'aU', 'wU+', 'U+', '+U', 'rU+'):
            try:
                f = gio_pyio.open(self.file, mode)
            except ValueError:
                pass
            else:
                f.close()
                self.fail('%r is an invalid file mode' % mode)
        # check valid mode strings
        for mode in ('rt', 'wb', 'a+'):
            f = gio_pyio.open(self.file, mode, native=False)
            f.close()
            f = gio_pyio.open(self.file, mode, native=True)
            f.close()

    def testBadModeArgument(self):
        # verify that we get a sensible error message for bad mode argument
        bad_mode = 'qwerty'
        try:
            f = gio_pyio.open(self.file, bad_mode)
        except ValueError as msg:
            if msg.args[0] != 0:
                s = str(msg)
                if bad_mode not in s:
                    self.fail('bad error message for invalid mode: %s' % s)
            # if msg.args[0] == 0, we're probably on Windows where there may be
            # no obvious way to discover why open() failed.
        else:
            f.close()
            self.fail('no error for invalid mode: %s' % bad_mode)

    def testIteration(self):
        # Test the complex interaction when mixing file-iteration and the
        # various read* methods.
        dataoffset = 16384
        filler = b'ham\n'
        assert not dataoffset % len(filler), \
            'dataoffset must be multiple of len(filler)'
        nchunks = dataoffset // len(filler)
        testlines = [
            b'spam, spam and eggs\n',
            b'eggs, spam, ham and spam\n',
            b'saussages, spam, spam and eggs\n',
            b'spam, ham, spam and eggs\n',
            b'spam, spam, spam, spam, spam, ham, spam\n',
            b'wonderful spaaaaaam.\n',
        ]
        methods = [('readline', ()), ('read', ()), ('readlines', ()),
                   ('readinto', (array('b', b' ' * 100),))]

        # Prepare the testfile
        bag = gio_pyio.open(self.file, 'wb')
        bag.write(filler * nchunks)
        bag.writelines(testlines)
        bag.close()
        # Test for appropriate errors mixing read* and iteration
        for methodname, args in methods:
            f = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
            self.assertEqual(next(f), filler)
            meth = getattr(f, methodname)
            meth(*args)  # This simply shouldn't fail
            f.close()

        # Test to see if harmless (by accident) mixing of read* and
        # iteration still works. This depends on the size of the internal
        # iteration buffer (currently 8192,) but we can test it in a
        # flexible manner.  Each line in the bag o' ham is 4 bytes
        # ("h", "a", "m", "\n"), so 4096 lines of that should get us
        # exactly on the buffer boundary for any power-of-2 buffersize
        # between 4 and 16384 (inclusive).
        f = gio_pyio.open(self.file, 'rb', native=False)
        for _i in range(nchunks):
            next(f)
        testline = testlines.pop(0)
        try:
            line = f.readline()
        except ValueError:
            self.fail('readline() after next() with supposedly empty '
                      'iteration-buffer failed anyway')
        if line != testline:
            self.fail('readline() after next() with empty buffer '
                      'failed. Got %r, expected %r' % (line, testline))
        testline = testlines.pop(0)
        buf = array('b', b'\x00' * len(testline))
        try:
            f.readinto(buf)
        except ValueError:
            self.fail('readinto() after next() with supposedly empty '
                      'iteration-buffer failed anyway')
        line = buf.tobytes()
        if line != testline:
            self.fail('readinto() after next() with empty buffer '
                      'failed. Got %r, expected %r' % (line, testline))

        testline = testlines.pop(0)
        try:
            line = f.read(len(testline))
        except ValueError:
            self.fail('read() after next() with supposedly empty '
                      'iteration-buffer failed anyway')
        if line != testline:
            self.fail('read() after next() with empty buffer '
                      'failed. Got %r, expected %r' % (line, testline))
        try:
            lines = f.readlines()
        except ValueError:
            self.fail('readlines() after next() with supposedly empty '
                      'iteration-buffer failed anyway')
        if lines != testlines:
            self.fail('readlines() after next() with empty buffer '
                      'failed. Got %r, expected %r' % (line, testline))
        f.close()

        # Reading after iteration hit EOF shouldn't hurt either
        f = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
        try:
            for _line in f:
                pass
            try:
                f.readline()
                f.readinto(buf)
                f.read()
                f.readlines()
            except ValueError:
                self.fail('read* failed after next() consumed file')
        finally:
            f.close()

    def testAbles(self):
        try:
            f = gio_pyio.open(self.file, 'wb', buffering=0, native=False)
            self.assertEqual(f.readable(), False)
            self.assertEqual(f.writable(), True)
            self.assertEqual(f.seekable(), True)
            f.close()

            f = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
            self.assertEqual(f.readable(), True)
            self.assertEqual(f.writable(), False)
            self.assertEqual(f.seekable(), True)
            f.close()

            f = gio_pyio.open(self.file, 'a+b', buffering=0, native=False)
            self.assertEqual(f.readable(), True)
            self.assertEqual(f.writable(), True)
            self.assertEqual(f.seekable(), True)
            self.assertEqual(f.isatty(), False)
            f.close()
        finally:
            pass

    def testAppend(self):
        try:
            f = gio_pyio.open(self.file, 'wb', buffering=0, native=False)
            f.write(b'spam')
            f.close()
            f = gio_pyio.open(self.file, 'ab', buffering=0, native=False)
            f.write(b'eggs')
            f.close()
            f = gio_pyio.open(self.file, 'rb', buffering=0, native=False)
            d = f.read()
            f.close()
            self.assertEqual(d, b'spameggs')
        finally:
            pass

    def testJSON(self):
        path = Path(Path(__file__).parent, 'example_data.json')
        file = Gio.File.new_for_path(str(path))
        f = gio_pyio.open(file, 'rb', buffering=0, native=False)
        data = json.load(f)
        f.close()
        self.assertEqual(data['glossary']['title'], 'example glossary')

    def testGResource(self):
        parent = Path(__file__).parent
        subprocess.call(
            'glib-compile-resources' +
            ' --sourcedir=' + str(parent) +
            ' --target=' + self.file.peek_path() +
            ' ' + str(Path(parent, 'example.gresource.xml')),
            shell=True,
        )
        resource = Gio.Resource.load(self.file.peek_path())
        resource._register()

        file = Gio.File.new_for_uri('resource:///example/example_data.json')
        f = gio_pyio.open(file, 'rb', buffering=0)
        self.assertTrue(isinstance(f, gio_pyio.StreamWrapper))
        data = json.load(f)
        f.close()
        self.assertEqual(data['glossary']['title'], 'example glossary')

        f = gio_pyio.open(file, 'rb')

    def testStreamWrapper(self):
        bogus = 'Hello'
        self.assertRaises(TypeError, gio_pyio.StreamWrapper, bogus)
