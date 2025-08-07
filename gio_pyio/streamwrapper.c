#define PY_SSIZE_T_CLEAN
#define DEFAULT_BUF_SIZE 4096
#include "gio_pyio.h"
#include <gio/gfiledescriptorbased.h>
#include <gio/gio.h>
#include <pygobject.h>
#include <unistd.h>

typedef struct
{
  PyObject_HEAD GInputStream *input;
  GOutputStream *output;
  GIOStream *io;
} StreamWrapper;

PyDoc_STRVAR (StreamWrapper_doc,
              "Wrap a stream as a `file object`_.\n"
              "\n"
              "See :func:`open` for a convenience method to open a file as a\n"
              "`file object`_. Note, that this does not implement buffering, "
              "seeking, etc. \n"
              "and relies on the capabilities of *stream*.\n"
              "\n"
              ":param stream stream:\n"
              "   A stream to be wrapped.\n"
              ":raises TypeError:\n"
              "   Invalid argument.\n"
              "\n"
              ".. _file object: "
              "https://docs.python.org/3/glossary.html#term-file-object");

static int
StreamWrapper_init (StreamWrapper *self, PyObject *args, PyObject *kwds)
{
  PyObject *py_stream = NULL;
  if (!PyArg_ParseTuple (args, "O", &py_stream))
    return -1;

  int is_instance = PyObject_IsInstance (py_stream, PyGObjectClass);
  if (is_instance < 0)
    // Error during isinstance check
    return -1;
  if (!is_instance)
    goto typeerr;

  PyGObject *pygobj = (PyGObject *)py_stream;
  if (!pygobj->obj)
    {
      PyErr_SetString (PyExc_ValueError, "Invalid GObject pointer");
      return -1;
    }
  GObject *gobj = pygobj->obj;

  // Determine stream type and take refs
  if (G_IS_INPUT_STREAM (gobj))
    {
      self->input = G_INPUT_STREAM (gobj);
      g_object_ref (self->input);
    }
  else if (G_IS_OUTPUT_STREAM (gobj))
    {
      self->output = G_OUTPUT_STREAM (gobj);
      g_object_ref (self->output);
    }
  else if (G_IS_IO_STREAM (gobj))
    {
      self->io = G_IO_STREAM (gobj);
      g_object_ref (self->io);
      self->input = g_io_stream_get_input_stream (self->io);
      g_object_ref (self->input);
      self->output = g_io_stream_get_output_stream (self->io);
      g_object_ref (self->output);
    }
  else
    goto typeerr;

  return 0;

typeerr:
  PyErr_SetString (PyExc_TypeError, "expected a GIO stream object");
  return -1;
}

static PyObject *
err_unsupported (char *method)
{
  PyErr_SetString (UnsupportedOperation, method);
  return NULL;
}

static gboolean
is_closed (StreamWrapper *self)
{
  gboolean unclosed = TRUE;
  if (self->input)
    unclosed
        = unclosed && !g_input_stream_is_closed (G_INPUT_STREAM (self->input));
  if (self->output)
    unclosed = unclosed
               && !g_output_stream_is_closed (G_OUTPUT_STREAM (self->output));

  return !unclosed;
}

static PyObject *
err_closed (void)
{
  PyErr_SetString (UnsupportedOperation, "I/O operation on closed file");
  return NULL;
}

PyDoc_STRVAR (StreamWrapper_get_closed_doc,
              "``True`` if the underlying stream is closed.");
static PyObject *
StreamWrapper_get_closed (StreamWrapper *self, void *closure)
{
  if (is_closed (self))
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

PyDoc_STRVAR (
    StreamWrapper_close_doc,
    "Flush and close the underlying stream.\n"
    "\n"
    "This method has no effect if the underlying stream is already closed.\n"
    "Once closed, any operation (e. g. reading or writing) will raise a \n"
    "ValueError.\n"
    "As a convenience, it is allowed to call this method more than once;\n"
    "only the first call, however, will have an effect.");
static PyObject *
StreamWrapper_close_impl (StreamWrapper *self, PyObject *Py_UNUSED (ignored))
{
  if (is_closed (self))
    Py_RETURN_NONE;

  GError *error = NULL;

  if (self->io)
    {
      if (!g_io_stream_close (self->io, NULL, &error))
        {
          PyErr_SetString (PyExc_IOError, error->message);
          g_error_free (error);
          return NULL;
        }
      Py_RETURN_NONE;
    }

  if (self->input)
    {
      if (!g_input_stream_close (self->input, NULL, &error))
        {
          PyErr_SetString (PyExc_IOError, error->message);
          g_error_free (error);
          return NULL;
        }
    }

  if (self->output)
    {
      if (!g_output_stream_close (self->output, NULL, &error))
        {
          PyErr_SetString (PyExc_IOError, error->message);
          g_error_free (error);
          return NULL;
        }
    }

  Py_RETURN_NONE;
}

static gboolean
is_readable (StreamWrapper *self)
{
  return self->input != NULL;
}

static PyObject *
err_not_readable (void)
{
  PyErr_SetString (UnsupportedOperation, "Stream is not readable");
  return NULL;
}

PyDoc_STRVAR (StreamWrapper_readable_doc,
              "Whether or not the stream is readable.\n"
              "\n"
              ":rtype bool:\n"
              ":returns:\n"
              "   Whether or not this wrapper can be read from.");
static PyObject *
StreamWrapper_readable_impl (StreamWrapper *self,
                             PyObject *Py_UNUSED (ignored))
{
  if (is_readable (self))
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

static PyObject *
read_until_eof (StreamWrapper *self)
{
  PyObject *result = PyBytes_FromStringAndSize (NULL, 0);
  if (result == NULL)
    return NULL;

  gssize bufsize;

  if (G_IS_BUFFERED_INPUT_STREAM (self->input))
    bufsize = g_buffered_input_stream_get_buffer_size (
        G_BUFFERED_INPUT_STREAM (self->input));
  else
    bufsize = DEFAULT_BUF_SIZE;

  Py_ssize_t total_read = 0;
  char buffer[bufsize];

  while (TRUE)
    {
      GError *error = NULL;
      gssize n
          = g_input_stream_read (self->input, buffer, bufsize, NULL, &error);
      if (n < 0)
        {
          Py_DECREF (result);
          PyErr_SetString (PyExc_IOError,
                           error ? error->message : "Read error");
          g_clear_error (&error);
          return NULL;
        }
      if (n == 0) // EOF
        break;

      if (_PyBytes_Resize (&result, total_read + n) < 0)
        {
          Py_DECREF (result);
          return NULL;
        }
      memcpy (PyBytes_AS_STRING (result) + total_read, buffer, n);
      total_read += n;
    }

  return result;
}

PyDoc_STRVAR (
    StreamWrapper_read_doc,
    "Read up to *size* bytes from the underlying stream and return them.\n"
    "\n"
    "As a convenience if *size* is unspecified or -1, all bytes until EOF\n"
    "are returned. The result may be fewer bytes than requested, if EOF is\n"
    "reached.\n"
    "\n"
    ":param int size:\n"
    "   The amount of bytes to read from the underlying stream."
    ":rtype: bytes\n"
    ":returns:\n"
    "   Bytes read from the underlying stream.\n"
    ":raises ValueError:\n"
    "   If the underlying stream is closed.\n"
    ":raises io.UnsupportedOperationException:\n"
    "   If the underlying stream is not readable.");
static PyObject *
StreamWrapper_read_impl (StreamWrapper *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[] = { "size", NULL };
  Py_ssize_t size = -1;

  if (!PyArg_ParseTupleAndKeywords (args, kwds, "|n", kwlist, &size))
    return NULL;

  if (is_closed (self))
    return err_closed ();

  if (!is_readable (self))
    return err_not_readable ();

  if (size == 0)
    // Return empty bytes
    return PyBytes_FromStringAndSize ("", 0);

  if (!(size > 0))
    return read_until_eof (self);

  // Allocate buffer for fixed-size read
  char *buffer = g_malloc (size);
  if (buffer == NULL)
    {
      PyErr_NoMemory ();
      return NULL;
    }

  GError *error = NULL;
  gssize n;
  if (!g_input_stream_read_all (self->input, buffer, (gssize)size, &n, NULL,
                                &error))
    {
      g_free (buffer);
      PyErr_SetString (PyExc_IOError, error ? error->message : "Read error");
      g_clear_error (&error);
      return NULL;
    }

  PyObject *result = PyBytes_FromStringAndSize (buffer, n);
  g_free (buffer);
  return result;
}

PyDoc_STRVAR (StreamWrapper_readall_doc,
              "Read and return all the bytes from the stream until EOF\n"
              "\n"
              ":rtype: bytes\n"
              ":returns:\n"
              "   Bytes read from the underlying stream.\n"
              ":raises ValueError:\n"
              "   If the underlying stream is closed.\n"
              ":raises io.UnsupportedOperationException:\n"
              "   If the underlying stream is not readable.");
static PyObject *
StreamWrapper_readall_impl (StreamWrapper *self, PyObject *Py_UNUSED (ignored))
{
  if (is_closed (self))
    return err_closed ();

  if (!is_readable (self))
    return err_not_readable ();

  return read_until_eof (self);
}

PyDoc_STRVAR (
    StreamWrapper_readinto_doc,
    "Read bytes into a pre-allocated, writable `bytes-like object`_ *b*.\n"
    "\n"
    ":param bytes-like b:\n"
    "   A pre-allocated object.\n"
    ":rtype: int\n"
    ":returns:\n"
    "   Number of bytes written.\n"
    ":raises ValueError:\n"
    "   If the underlying stream is closed.\n"
    ":raises io.UnsupportedOperationException:\n"
    "   If the underlying stream is not readable.\n"
    "\n"
    ".. _bytes-like object: "
    "https://docs.python.org/3/glossary.html#term-bytes-like-object");
static PyObject *
StreamWrapper_readinto_impl (StreamWrapper *self, PyObject *args)
{
  PyObject *buffer_obj;

  if (!PyArg_ParseTuple (args, "O", &buffer_obj))
    return NULL;

  if (is_closed (self))
    return err_closed ();

  if (!is_readable (self))
    return err_not_readable ();

  Py_buffer view;
  if (PyObject_GetBuffer (buffer_obj, &view, PyBUF_WRITABLE) == -1)
    // Not writable buffer
    return NULL;

  GError *error = NULL;
  gssize n_read;
  if (!g_input_stream_read_all (self->input, (void *)view.buf, (gsize)view.len,
                                &n_read, NULL, &error))
    {
      PyBuffer_Release (&view);
      PyErr_SetString (PyExc_IOError, error ? error->message : "Read error");
      g_clear_error (&error);
      return NULL;
    }

  PyBuffer_Release (&view);

  return PyLong_FromSsize_t (n_read);
}

static gboolean
is_writable (StreamWrapper *self)
{
  return self->output != NULL;
}

static PyObject *
err_not_writable (void)
{
  PyErr_SetString (UnsupportedOperation, "Stream is not writable");
  return NULL;
}

PyDoc_STRVAR (StreamWrapper_writable_doc,
              "Whether or not the stream can be written to.\n"
              "\n"
              ":rtype bool:\n"
              ":returns:\n"
              "   Whether or not this wrapper can be written to.");
static PyObject *
StreamWrapper_writable_impl (StreamWrapper *self,
                             PyObject *Py_UNUSED (ignored))
{
  if (is_writable (self))
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

PyDoc_STRVAR (StreamWrapper_write_doc,
              "Write *b* to the underlying stream.\n"
              "\n"
              ":param bytes-like b:\n"
              "   Content to be written to the underlying stream.\n"
              ":rtype: int\n"
              ":returns:\n"
              "   The number of bytes written to the underlying stream.\n"
              ":raises ValueError:\n"
              "   If the underlying stream is closed.\n"
              ":raises io.UnsupportedOperationException:\n"
              "   If the underlying stream can not be written to.");
static PyObject *
StreamWrapper_write_impl (StreamWrapper *self, PyObject *args)
{
  Py_buffer view;

  if (!PyArg_ParseTuple (args, "y*", &view))
    return NULL;

  if (is_closed (self))
    return err_closed ();

  if (!is_writable (self))
    return err_not_writable ();

  if (view.len == 0)
    {
      // Nothing to write, release buffer and return 0
      PyBuffer_Release (&view);
      return PyLong_FromLong (0);
    }

  // Write all bytes from view.buf of length view.len
  GError *error = NULL;
  gssize bytes_written;
  gboolean success = g_output_stream_write_all (
      self->output, view.buf, view.len, &bytes_written, NULL, &error);

  PyBuffer_Release (&view);

  if (!success)
    {
      PyErr_SetString (PyExc_IOError, error ? error->message : "Write failed");
      g_clear_error (&error);
      return NULL;
    }

  return PyLong_FromSsize_t (bytes_written);
}

PyDoc_STRVAR (
    StreamWrapper_flush_doc,
    "Flush the write buffers of the underlying stream if applicable.\n"
    "\n"
    "This does nothing for read-only streams.\n"
    "\n"
    ":raises ValueError:\n"
    "   If the underlying stream is closed.\n");
static PyObject *
StreamWrapper_flush_impl (StreamWrapper *self, PyObject *Py_UNUSED (ignored))
{
  if (is_closed (self))
    return err_closed ();

  if (!is_writable (self))
    Py_RETURN_NONE;

  GError *error = NULL;
  if (!g_output_stream_flush (self->output, NULL, &error))
    {
      // Implementing flush is not required, causing error to not be set
      if (error)
        {
          PyErr_SetString (PyExc_IOError, error->message);
          g_clear_error (&error);
          return NULL;
        }
    }

  Py_RETURN_NONE;
}

static gboolean
is_seekable (StreamWrapper *self)
{
  gboolean unseekable = TRUE;

  if (self->input)
    unseekable = unseekable
                 && !(G_IS_SEEKABLE (self->input)
                      && g_seekable_can_seek (G_SEEKABLE (self->input)));
  if (self->output)
    unseekable = unseekable
                 && !(G_IS_SEEKABLE (self->output)
                      && g_seekable_can_seek (G_SEEKABLE (self->output)));

  return !unseekable;
}

static PyObject *
err_not_seekable (void)
{
  PyErr_SetString (UnsupportedOperation, "Underlying stream is not seekable");
  return NULL;
}

PyDoc_STRVAR (StreamWrapper_seekable_doc,
              "Whether or not the stream is seekable.\n"
              "\n"
              ":rtype: bool\n"
              ":returns:\n"
              "   Whether or not the underlying stream supports seeking.\n"
              ":raises ValueError:\n"
              "   If the underlying stream is closed.\n"
              ":raises io.UnsupportedOperationException:\n"
              "   If the underlying stream is not seekable.");
static PyObject *
StreamWrapper_seekable_impl (StreamWrapper *self,
                             PyObject *Py_UNUSED (ignored))
{
  if (is_closed (self))
    return err_closed ();

  if (is_seekable (self))
    Py_RETURN_TRUE;
  else
    Py_RETURN_FALSE;
}

static goffset
tell (StreamWrapper *self)
{
  goffset pos;
  if (self->input)
    pos = g_seekable_tell (G_SEEKABLE (self->input));
  if (self->output)
    pos = g_seekable_tell (G_SEEKABLE (self->output));
  return pos;
}

PyDoc_STRVAR (StreamWrapper_tell_doc,
              "Tell the current stream position.\n"
              "\n"
              ":rtype: int\n"
              ":returns:\n"
              "   The position of the underlying stream.\n"
              ":raises ValueError:\n"
              "   If the underlying stream is closed.\n"
              ":raises io.UnsupportedOperationException:\n"
              "   If the underlying stream is not seekable.");
static PyObject *
StreamWrapper_tell_impl (StreamWrapper *self, PyObject Py_UNUSED (ignored))
{
  if (is_closed (self))
    return err_closed ();

  if (!is_seekable (self))
    return err_not_seekable ();

  goffset pos = tell (self);

  return PyLong_FromLongLong (pos);
}

PyDoc_STRVAR (
    StreamWrapper_seek_doc,
    "Change the underlying stream position.\n"
    "\n"
    "*offset* is interpreted relative to the position indicated by *whence*.\n"
    "\n"
    ":param int offset:\n"
    "   Where to change the stream position to, relative to *whence*"
    ":param int whence:\n"
    "   Reference for *offset*. Values are:\n"
    "   * 0 -- start of stream (the default); offset should'nt be negative\n"
    "   * 1 -- current stream position; offset may be negative\n"
    "   * 2 -- end of stream; offset is usually negative\n"
    ":rtype: int\n"
    ":returns:\n"
    "   The new absolute position of the underlying stream.\n"
    ":raises ValueError:\n"
    "   If the underlying stream is closed.\n"
    ":raises io.UnsupportedOperationException:\n"
    "   If the underlying stream is not seekable.");
static PyObject *
StreamWrapper_seek_impl (StreamWrapper *self, PyObject *args, PyObject *kwargs)
{
  static char *kwlist[] = { "offset", "whence", NULL };
  goffset offset;
  int whence = SEEK_SET;

  if (!PyArg_ParseTupleAndKeywords (args, kwargs, "L|i", kwlist, &offset,
                                    &whence))
    return NULL;

  if (is_closed (self))
    return err_closed ();

  if (!is_seekable (self))
    return err_not_seekable ();

  GSeekType seek_type;
  switch (whence)
    {
    case SEEK_SET:
      seek_type = G_SEEK_SET;
      break;
    case SEEK_CUR:
      seek_type = G_SEEK_CUR;
      break;
    case SEEK_END:
      seek_type = G_SEEK_END;
      break;
    default:
      PyErr_SetString (PyExc_ValueError, "Invalid whence value");
      return NULL;
    }

  GError *error = NULL;

  if (is_readable (self))
    {
      if (!g_seekable_seek (G_SEEKABLE (self->input), offset, seek_type, NULL,
                            &error))
        {
          PyErr_SetString (PyExc_IOError, error->message);
          g_error_free (error);
          return NULL;
        }
    }

  if (is_writable (self))
    {
      if (!g_seekable_seek (G_SEEKABLE (self->output), offset, seek_type, NULL,
                            &error))
        {
          PyErr_SetString (PyExc_IOError, error->message);
          g_error_free (error);
          return NULL;
        }
    }

  goffset pos = tell (self);

  return PyLong_FromLongLong (pos);
}

PyDoc_STRVAR (
    StreamWrapper_truncate_doc,
    "Resize the underlying stream to *size*.\n"
    "\n"
    ":param int size:\n"
    "   The size, the stream should be set to. If ``None`` the current\n"
    "   position is used.\n"
    ":rtype: int\n"
    ":returns:\n"
    "   The new size of the underlying stream.\n"
    ":raises ValueError:\n"
    "   If the underlying stream is closed.\n"
    ":raises io.UnsupportedOperationException:\n"
    "   If the underlying stream is not seekable.");
static PyObject *
StreamWrapper_truncate_impl (StreamWrapper *self, PyObject *args)
{
  goffset size;

  if (!PyArg_ParseTuple (args, "|L", &size))
    return NULL;

  if (is_closed (self))
    return err_closed ();

  if (!is_seekable (self))
    return err_not_seekable ();

  if (!g_seekable_can_truncate (G_SEEKABLE (self->output)))
    return err_unsupported ("truncate");

  // If no size provided, use current position
  if (PyTuple_Size (args) == 0)
    size = g_seekable_tell (G_SEEKABLE (self->output));

  GError *error = NULL;
  if (!g_seekable_truncate (G_SEEKABLE (self->output), size, NULL, &error))
    {
      PyErr_SetString (PyExc_IOError, "Failed to truncate");
      g_clear_error (&error);
      return NULL;
    }

  return PyLong_FromLong (size);
}

static gboolean
is_fd_based (StreamWrapper *self)
{
  gboolean not_fd_based = TRUE;

  if (self->input)
    not_fd_based = not_fd_based && G_IS_FILE_DESCRIPTOR_BASED (self->input);
  if (self->output)
    not_fd_based = not_fd_based && G_IS_FILE_DESCRIPTOR_BASED (self->output);

  return !not_fd_based;
}

static int
get_fd (StreamWrapper *self)
{
  int fd;
  if (self->input)
    fd = g_file_descriptor_based_get_fd (
        G_FILE_DESCRIPTOR_BASED (self->input));
  if (self->output)
    fd = g_file_descriptor_based_get_fd (
        G_FILE_DESCRIPTOR_BASED (self->output));
  return fd;
}

PyDoc_STRVAR (
    StreamWrapper_fileno_doc,
    "Return the underlying file descriptor if it exists.\n"
    "\n"
    ":rtype: int\n"
    ":returns:\n"
    "   The underlying file descriptor.\n"
    ":raises ValueError:\n"
    "   If the underlying stream is closed.\n"
    ":raises io.UnsupportedOperationException:\n"
    "   If the underlying stream is not based on a file descriptor.");
static PyObject *
StreamWrapper_fileno_impl (StreamWrapper *self, PyObject *Py_UNUSED (ignored))
{
  if (is_closed (self))
    return err_closed ();

  if (!is_fd_based (self))
    return err_unsupported ("fileno");

  int fd = get_fd (self);

  return PyLong_FromLong (fd);
}

PyDoc_STRVAR (StreamWrapper_isatty_doc,
              "Whether or not the stream represents a tty.\n"
              "\n"
              ":rtype: bool\n"
              ":returns:\n"
              "   Whether or not the stream represents a tty.\n"
              ":raises ValueError:\n"
              "   If the underlying stream is closed.\n");
static PyObject *
StreamWrapper_isatty_impl (StreamWrapper *self, PyObject *Py_UNUSED (ignored))
{
  if (is_closed (self))
    return err_closed ();

  if (!is_fd_based (self))
    Py_RETURN_FALSE;

  if (isatty (get_fd (self)))
    Py_RETURN_TRUE;

  Py_RETURN_FALSE;
}

static void
StreamWrapper_dealloc (StreamWrapper *self)
{
  if (self->input)
    g_object_unref (self->input);
  if (self->output)
    g_object_unref (self->output);
  if (self->io)
    g_object_unref (self->io);
  Py_TYPE (self)->tp_free ((PyObject *)self);
}

static PyMethodDef StreamWrapper_methods[]
    = { { "close", (PyCFunction)StreamWrapper_close_impl, METH_NOARGS,
          StreamWrapper_close_doc },
        { "readable", (PyCFunction)StreamWrapper_readable_impl, METH_NOARGS,
          StreamWrapper_readable_doc },
        { "read", (PyCFunction)StreamWrapper_read_impl,
          METH_VARARGS | METH_KEYWORDS, StreamWrapper_read_doc },
        { "readall", (PyCFunction)StreamWrapper_readall_impl,
          METH_VARARGS | METH_KEYWORDS, StreamWrapper_readall_doc },
        { "readinto", (PyCFunction)StreamWrapper_readinto_impl, METH_VARARGS,
          StreamWrapper_readinto_doc },
        { "writable", (PyCFunction)StreamWrapper_writable_impl, METH_NOARGS,
          StreamWrapper_writable_doc },
        { "write", (PyCFunction)StreamWrapper_write_impl, METH_VARARGS,
          StreamWrapper_write_doc },
        { "flush", (PyCFunction)StreamWrapper_flush_impl, METH_NOARGS,
          StreamWrapper_flush_doc },
        { "seekable", (PyCFunction)StreamWrapper_seekable_impl, METH_NOARGS,
          StreamWrapper_seekable_doc },
        { "tell", (PyCFunction)StreamWrapper_tell_impl, METH_NOARGS,
          StreamWrapper_tell_doc },
        { "seek", (PyCFunction)StreamWrapper_seek_impl,
          METH_VARARGS | METH_KEYWORDS, StreamWrapper_seek_doc },
        { "truncate", (PyCFunction)StreamWrapper_truncate_impl,
          METH_VARARGS | METH_KEYWORDS, StreamWrapper_truncate_doc },
        { "fileno", (PyCFunction)StreamWrapper_fileno_impl, METH_NOARGS,
          StreamWrapper_fileno_doc },
        { "isatty", (PyCFunction)StreamWrapper_isatty_impl, METH_NOARGS,
          StreamWrapper_isatty_doc },
        { NULL, NULL, 0, NULL } };

static PyGetSetDef StreamWrapper_getsetters[]
    = { { "closed", (getter)StreamWrapper_get_closed, NULL,
          StreamWrapper_get_closed_doc, NULL },
        { NULL } };

PyTypeObject StreamWrapperType = {
  PyVarObject_HEAD_INIT (NULL, 0).tp_name = "gio_pyio.StreamWrapper",
  .tp_doc = StreamWrapper_doc,
  .tp_basicsize = sizeof (StreamWrapper),
  .tp_flags = Py_TPFLAGS_DEFAULT,
  .tp_new = PyType_GenericNew,
  .tp_init = (initproc)StreamWrapper_init,
  .tp_dealloc = (destructor)StreamWrapper_dealloc,
  .tp_methods = StreamWrapper_methods,
  .tp_getset = StreamWrapper_getsetters,
};
