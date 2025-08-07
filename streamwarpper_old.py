class StreamWrapper(io.IOBase):
    """Wrap a stream as a `file object`_.

    See :func:`open` for a convenience method to open a file as a
    `file object`_. Note, that this does not implement buffering, seeking, etc.
    and relies on the capabilities of *stream*.

    :param stream stream:
        A stream to be wrapped.
    :raises TypeError:
        Invalid argument.

    .. _file object: https://docs.python.org/3/glossary.html#term-file-object
    """

    def __init__(self, stream):
        if isinstance(stream, Gio.InputStream):
            self._input_stream = stream
            self._output_stream = None
            self._ref_stream = stream
        elif isinstance(stream, Gio.OutputStream):
            self._input_stream = None
            self._output_stream = stream
            self._ref_stream = stream
        elif isinstance(stream, Gio.IOStream):
            # For some methods, we assume, both stream represent the same
            # object as well as being in sync in regards to seeking, that way
            # we don't need to duplicate logic for most methods.
            self._input_stream = stream.get_input_stream()
            self._output_stream = stream.get_output_stream()
            self._ref_stream = self._input_stream
            # Keep a reference, or the stream might get closed
            self._io_stream = stream
        else:
            raise TypeError('expected stream, got %s' % type(stream))

    def close(self):
        """Flush and close the underlying stream.

        This method has no effect if the underlying stream is already closed.
        Once closed, any operation (e. g. reading or writing) will raise a
        ValueError.

        As a convenience, it is allowed to call this method more than once;
        only the first call, however, will have an effect.
        """
        if hasattr(self, '_io_stream'):
            self._io_stream.close()
            return
        if self.readable():
            self._input_stream.close()
        if self.writable():
            self._output_stream.close()

    @property
    def closed(self):
        """``True`` if the underlying stream is closed."""
        return self._ref_stream.is_closed()

    def fileno(self):
        """Return the underlying file descriptor if it exists.

        :rtype: int
        :returns:
            The underlying file descriptor.
        :raises ValueError:
            If the underlying stream is closed.
        :raises io.UnsupportedOperationException:
            If the underlying stream is not based on a file descriptor.
        """
        self._checkClosed()
        if not (self._ref_stream, 'get_fd'):
            self._unsupported('fileno')
        return self._ref_stream.get_fd()

    def flush(self):
        """Flush the write buffers of the underlying stream if applicable.

        This does nothing for read-only streams.

        :raises ValueError:
            If the underlying stream is closed.
        """
        self._checkClosed()
        if self.writable():
            self._output_stream.flush(None)

    def readable(self):
        """Whether or not the stream is readable.

        :rtype bool:
        :returns:
            Whether or not this wrapper can be read from.
        """
        return self._input_stream is not None

    def read(self, size=-1):
        """Read up to *size* bytes from the underlying stream and return them.

        As a convenience if *size* is unspecified or -1, all bytes until EOF
        are returned. The result may be fewer bytes than requested, if EOF is
        reached.

        :param int size:
            The amount of bytes to read from the underlying stream.
        :rtype: bytes
        :returns:
            Bytes read from the underlying stream.
        :raises ValueError:
            If the underlying stream is closed.
        :raises io.UnsupportedOperationException:
            If the underlying stream is not readable.
        """
        self._checkClosed()
        self._checkReadable()
        if size == 0:
            return b''
        elif size > 0:
            return self._input_stream.read_bytes(size, None).get_data()
        # Try to determine the length of the stream, else fall back on
        # default buffer size for reading
        if isinstance(self._input_stream, Gio.BufferedInputStream):
            def_bufsize = self._input_stream.get_buffer_size()
        else:
            def_bufsize = io.DEFAULT_BUFFER_SIZE
            end = None
            if hasattr(self._input_stream, 'get_size'):
                end = self._input_stream.get_size()
            elif hasattr(self._input_stream, 'query_info'):
                info = self._input_stream.query_info('standard::size', None)
                end = info.get_size()
            if end is not None:
                pos = self._ref_stream.tell()
                if end >= pos:
                    bufsize = end - pos + 1
            else:
                bufsize = def_bufsize
        result = bytearray()
        while True:
            if len(result) >= bufsize:
                bufsize = len(result)
                bufsize += max(bufsize, def_bufsize)
            n = bufsize - len(result)
            chunk = self._input_stream.read_bytes(n, None)
            if chunk.get_size() == 0:  # EOF reached
                break
            result += chunk.get_data()
        return bytes(result)

    read1 = read
    readall = read

    def readinto(self, b):
        """Read bytes into a pre-allocated, writable `bytes-like object`_ *b*.

        :param bytes-like b:
            A pre-allocated object.
        :rtype: int
        :returns:
            Number of bytes written.
        :raises ValueError:
            If the underlying stream is closed.
        :raises io.UnsupportedOperationException:
            If the underlying stream is not readable.

        .. _bytes-like object: https://docs.python.org/3/glossary.html#term-bytes-like-object
        """
        self._checkClosed()
        self._checkReadable()
        view = memoryview(b).cast('B')
        data = self._input_stream.read_bytes(len(view), None)
        size = data.get_size()
        view[:size] = data.get_data()
        return size

    readinto1 = readinto

    def seek(self, offset, whence=os.SEEK_SET):
        """Change the underlying stream position.

        *offset* is interpreted relative to the position indicated by *whence*.

        :param int offset:
            Where to change the stream position to, relative to *whence*
        :param int whence:
            Reference for *offset*. Values are:
            * 0 -- start of stream (the default); offset should'nt be negative
            * 1 -- current stream position; offset may be negative
            * 2 -- end of stream; offset is usually negative
        :rtype: int
        :returns:
            The new absolute position of the underlying stream.
        :raises ValueError:
            If the underlying stream is closed.
        :raises io.UnsupportedOperationException:
            If the underlying stream is not seekable.
        """
        self._checkClosed()
        self._checkSeekable()
        # Enum values in python and Gio:
        # 0: os.SEEK_SET : Gio.SeekType.CUR
        # 1: os.SEEK_CUR : Gio.SeekType.SET
        # 2: os.SEEK_END : Gio.SeekType.END
        # so 1 and 0 need to be switched
        if whence != 2:
            whence = not whence
        if self.readable():
            self._input_stream.seek(offset, whence, None)
        if self.writable():
            self._output_stream.seek(offset, whence, None)
        return self._ref_stream.tell()

    def seekable(self):
        """
        Whether or not the stream is seekable.

        :rtype: bool
        :returns:
            Whether or not the underlying stream supports seeking.
        :raises ValueError:
            If the underlying stream is closed.
        """
        self._checkClosed()
        return self._ref_stream.can_seek()

    def tell(self):
        """
        Tell the current stream position.

        :rtype: int
        :returns:
            The position of the underlying stream.
        :raises ValueError:
            If the underlying stream is closed.
        """
        self._checkClosed()
        return self._ref_stream.tell()

    def truncate(self, size=None):
        """Resize the underlying stream to *size*.

        :param int size:
            The size, the stream should be set to. If ``None`` the current
            position is used.
        :rtype: int
        :returns:
            The new size of the underlying stream.
        :raises ValueError:
            If the underlying stream is closed.
        :raises io.UnsupportedOperationException:
            If the underlying stream can not be written to.
        """
        self._checkClosed()
        if self._output_stream is None or \
                not self._output_stream.can_truncate():
            raise io.UnsupportedOperation('truncate')
        if size is None:
            size = self._output_stream.tell()
        self._output_stream.truncate(size)
        return size

    def writable(self):
        """
        Wheter or not the stream can be written to.

        :rtype: bool
        :returns:
            Whether or not this wrapper can be written to.
        """
        return self._output_stream is not None

    def write(self, b):
        """Write *b* to the underlying stream.

        :param bytes-like b:
            Content to be written to the underlying stream.
        :rtype: int
        :returns:
            The number of bytes written to the underlying stream.
        :raises ValueError:
            If the underlying stream is closed.
        :raises io.UnsupportedOperationException:
            If the underlying stream can not be written to.
        """
        self._checkClosed()
        self._checkWritable()
        if b is None or b == b'':
            return 0
        return self._output_stream.write_bytes(GLib.Bytes(b))
