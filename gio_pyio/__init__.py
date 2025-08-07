"""gio_pyio lib."""
import io
import os

from gi.repository import GLib, Gio

from ._gio_pyio import StreamWrapper

__all__ = ['StreamWrapper']


def open(file, mode='r', buffering=-1, encoding=None, errors=None,
         newline=None, native=True):
    r"""Open the file and create a corresponding `file object`_.

    If the file cannot be opened, an OSError is raised. This behaves analog to
    pythons builtin :external:py:func:`open` function. See
    `Reading and Writing Files`_ for examples of io using python.

    :param Gio.File file:
        The file to open.
    :param str mode:
        Mode in which the file is opened. Defaults to 'r' which means
        open for reading in text mode. Other common values are 'w' for
        writing (truncating the file if it already exists), 'x' for
        exclusive creation of a new file, and 'a' for appending. The
        available modes are:

        ========= ===================================================
        Character Meaning
        --------- ---------------------------------------------------
        'r'       open for reading (default)
        'w'       open for writing, truncating the file first
        'x'       create a new file and open it for writing
        'a'       open for writing, appending to the end of the file
        'b'       binary mode
        't'       text mode (default)
        '+'       open a disk file for updating (reading and writing)
        ========= ===================================================

        The default value is 'r' (open for reading text, a synonym of 'rt').
        For binary random access, the mode 'w+b' opens and truncates the
        file to 0 bytes, while 'r+b' opens the file without truncation. The
        'x' mode implies 'w' and raises an `FileExistsError` if the file
        already exists.

        Python distinguishes between files opened in binary and text mode.
        Files opened in binary mode (appending 'b' to the mode argument)
        return contents as bytes objects without any decoding. In text mode
        (the default, or when 't' is appended to the mode argument), the
        contents of the file are returned as strings, the bytes having
        first been decoded using *encoding*.
    :param int buffering:
        Buffering policy. Pass 0 to switch buffering off (only allowed in
        binary mode), 1 to select line buffering (only usable in text mode),
        and an integer > 1 to indicate the size of a fixed-size chunk
        buffer. When no buffering argument is given.

        Files are buffered in fixed-size chunks; the size of the buffer is
        chosen using a heuristic trying to determine the underlying device's
        blksize and falling back on `io.DEFAULT_BUFFER_SIZE`. The buffer
        will typically be 4096 or 8192 bytes long.
    :param str encoding:
        Encoding used to decode or encode the file. Can only be used in text
        mode. The default encoding is platform dependent, but any encoding
        supported by Python can be passed. See the codecs module for the
        list of supported encodings.
    :param str errors:
        How encoding errors are to be handled. Can not be used in binary
        mode. Pass 'strict' to raise a ValueError exception if there is an
        encoding error (the default of None has the same effect), or pass
        'ignore' to ignore errors. (Note that ignoring encoding errors can
        lead to data loss.) See the documentation for codecs.register for a
        list of the permitted encoding error strings.
    :param str newline:
        How universal newlines work (only applies to text mode). Can be
        None, '', '\n', '\r', and '\r\n'. Works as follows:

        * On input, if newline is None, universal newlines mode is enabled.
          Lines in the input can end in '\n', '\r', or '\r\n', and these are
          translated into '\n' before being returned to the caller. If it is
          '', universal newline mode is enabled, but line endings are
          returned to the caller untranslated. If it has any of the other
          legal values, input lines are only terminated by the given string,
          and the line ending is returned to the caller untranslated.

        * On output, if newline is None, any '\n' characters written are
          translated to the system default line separator, os.linesep. If
          newline is '', no translation takes place. If newline is any of
          the other legal values, any '\n' characters written are translated
          to the given string.
    :param bool native:
        Try and obtain a file descriptor and use python standard io libraries.
        If False, the result will always be a wrapped Gio stream.
    :rtype: file-like
    :returns:
        A new `file object`_. When used to open a file in a text mode ('w',
        'r', 'wt', 'rt', etc.), the object will be a TextIOWrapper. When
        used to open a file in a binary mode, the returned class varies:
        in read binary mode, it will be a BufferedReader; in write binary
        and append binary modes, it will be a BufferedWriter, and in
        read/write mode, it will be a BufferedRandom. If buffering is
        disabled, the object will either be a FileIO or
        :py:class:`StreamWrapper` depending on python native libraries can be
        used.
    :raises TypeError:
        Invalid argument passed.
    :raises ValueError:
        Invalid argument passed.
    :raises OSError:
        Failed to open file.

    .. _file object: https://docs.python.org/3/glossary.html#term-file-object
    .. _Reading and Writing Files: https://docs.python.org/3/tutorial/inputoutput.html#tut-files
    """
    # Argument validation
    if not isinstance(mode, str):
        raise TypeError('invalid mode: %r' % mode)
    if not isinstance(buffering, int):
        raise TypeError('invalid buffering: %r' % buffering)
    modes = set(mode)
    if modes - set('axrwb+t') or len(mode) > len(modes):
        raise ValueError('invalid mode: %r' % mode)
    if encoding is not None and not isinstance(encoding, str):
        raise TypeError('invalid encoding: %r' % encoding)
    if errors is not None and not isinstance(errors, str):
        raise TypeError('invalid errors: %r' % errors)
    creating = 'x' in modes
    reading = 'r' in modes
    writing = 'w' in modes
    appending = 'a' in modes
    updating = '+' in modes
    binary = 'b' in modes
    if binary and 't' in modes:
        raise ValueError("can't have text and binary mode at once")
    if creating + reading + writing + appending > 1:
        raise ValueError('must have exactly one of create/read/write/append'
                         ' mode')
    if not (creating or reading or writing or appending):
        raise ValueError('Must have exactly one of create/read/write/append'
                         ' mode and at most one plus')
    if binary and encoding is not None:
        raise ValueError("binary mode doesn't take an encoding argument")
    if binary and errors is not None:
        raise ValueError("binary mode doesn't take an errors argument")
    if binary and newline is not None:
        raise ValueError("binary mode doesn't take a newline argument")

    # For non-native files we use the result of `file.get_basename()`
    rep_str = file.peek_path() if file.is_native() else file.get_basename()
    file_type = file.query_file_type(Gio.FileQueryInfoFlags.NONE, None)
    if file_type == Gio.FileType.DIRECTORY:
        raise OSError(21, "Is a directory: '%s'" % rep_str)
    if file.query_exists():
        if creating:
            raise OSError(17, "File exists: '%s'" % rep_str)
    elif reading:
        raise OSError(2, "No such file or directory: '%s'" % rep_str)
    if buffering == 0 and not binary:
        raise ValueError("can't have unbuffered text I/O")

    if native and file.is_native():
        file_like = io.FileIO(
            file.peek_path(),
            (creating and 'x' or '') +
            (reading and 'r' or '') +
            (writing and 'w' or '') +
            (appending and 'a' or '') +
            (updating and '+' or ''),
        )
    else:
        stream = None
        # Match given mode to respective opener. All calls are non-async, thus
        # blocking as well as not cancellable.
        if updating:
            if creating:
                stream = file.create_readwrite(Gio.FileCreateFlags.NONE, None)
            elif writing:
                stream = file.replace_readwrite(None, False,
                                                Gio.FileCreateFlags.NONE, None)
            else:
                stream = file.open_readwrite(None)
        else:
            if creating:
                stream = file.create(Gio.FileCreateFlags.NONE, None)
            elif reading:
                stream = file.read(None)
            elif writing:
                stream = file.replace(None, False, Gio.FileCreateFlags.NONE,
                                      None)
            elif appending:
                stream = file.append_to(Gio.FileCreateFlags.NONE, None)

        # at this point stream should not be `None` or input validation has
        # failed substantially
        assert stream is not None
        file_like = StreamWrapper(stream)
    line_buffering = False
    if buffering != 0:
        if buffering == 1:
            buffering = -1
            line_buffering = True
        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE
            try:
                # Try to set buffersize to the blksize of the file system
                blksize = os.fstat(file_like.fileno()).st_blksize
                if blksize > 1:
                    buffering = blksize
            except (OSError, AttributeError):
                pass
        if updating:
            wrapper = io.BufferedRandom
        elif reading:
            wrapper = io.BufferedReader
        else:
            wrapper = io.BufferedWriter
        file_like = wrapper(file_like, buffering)
    if not binary:
        file_like = io.TextIOWrapper(file_like, encoding=encoding,
                                     errors=errors, newline=newline,
                                     line_buffering=line_buffering)
        file_like.mode = mode
    return file_like
