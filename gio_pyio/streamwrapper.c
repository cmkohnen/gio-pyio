#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <gio/gio.h>
#include <gio/gfiledescriptorbased.h>
#include <pygobject.h>


typedef struct {
    PyObject_HEAD
    GInputStream *input;
    GOutputStream *output;
    GIOStream *io;
    GSeekable *ref;
} StreamWrapper;


static int
StreamWrapper_init(StreamWrapper *self, PyObject *args, PyObject *kwds)
{
    PyObject *py_stream = NULL;

    if (!PyArg_ParseTuple(args, "O", &py_stream))
        return -1;

    // Import 'gi.repository.GObject' module to get the GObject base type
    PyObject *gi_module = PyImport_ImportModule("gi.repository.GObject");
    if (!gi_module) {
        PyErr_SetString(PyExc_ImportError, "Failed to import gi.repository.GObject");
        return -1;
    }

    PyObject *gobject_type = PyObject_GetAttrString(gi_module, "GObject");
    Py_DECREF(gi_module);
    if (!gobject_type) {
        PyErr_SetString(PyExc_AttributeError, "Failed to get GObject from gi.repository");
        return -1;
    }

    // Check if py_stream is instance of GObject
    int is_instance = PyObject_IsInstance(py_stream, gobject_type);
    Py_DECREF(gobject_type);

    if (is_instance < 0) {
        // Error during isinstance check
        return -1;
    }
    if (!is_instance) {
        PyErr_SetString(PyExc_TypeError, "expected a GObject stream object");
        return -1;
    }

    // Now safe to cast
    PyGObject *pygobj = (PyGObject *)py_stream;

    if (!pygobj->obj) {
        PyErr_SetString(PyExc_ValueError, "Invalid GObject pointer");
        return -1;
    }

    GObject *gobj = pygobj->obj;

    // Determine stream type and take refs
    if (G_IS_INPUT_STREAM(gobj)) {
        self->input = G_INPUT_STREAM(gobj);
        g_object_ref(self->input);
        self->ref = G_SEEKABLE(self->input);
    } else if (G_IS_OUTPUT_STREAM(gobj)) {
        self->output = G_OUTPUT_STREAM(gobj);
        g_object_ref(self->output);
        self->ref = G_SEEKABLE(self->output);
    } else if (G_IS_IO_STREAM(gobj)) {
        self->io = G_IO_STREAM(gobj);
        g_object_ref(self->io);

        self->input = g_io_stream_get_input_stream(self->io);
        if (self->input)
            g_object_ref(self->input);

        self->output = g_io_stream_get_output_stream(self->io);
        if (self->output)
            g_object_ref(self->output);

        self->ref = G_SEEKABLE(self->input);
    } else {
        PyErr_SetString(PyExc_TypeError, "expected a GIO stream object");
        return -1;
    }

    return 0;
}


static gboolean
is_closed(StreamWrapper *self)
{
    gboolean closed = FALSE;
    if (G_IS_OUTPUT_STREAM(self->ref)) {
        closed = g_output_stream_is_closed(G_OUTPUT_STREAM(self->ref));
    } else if (G_IS_INPUT_STREAM(self->ref)) {
        closed = g_input_stream_is_closed(G_INPUT_STREAM(self->ref));
    } else {
        // fallback, treat as not closed
        closed = FALSE;
    }

    return closed;
}

static PyObject *
err_closed(void)
{
    PyErr_SetString(PyExc_ValueError, "I/O operation on closed file");
    return NULL;
}


static PyObject *
StreamWrapper_get_closed(StreamWrapper *self, void *closure)
{
    if (is_closed(self))
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}


static PyObject *
StreamWrapper_close(StreamWrapper *self, PyObject *Py_UNUSED(ignored))
{
    if (is_closed(self)) Py_RETURN_NONE;

    GError *error = NULL;

    // If _io_stream exists, close it
    if (self->io != NULL) {
        if (!g_io_stream_is_closed(self->io)) {
            if (!g_io_stream_close(self->io, NULL, &error)) {
                PyErr_SetString(PyExc_IOError, error->message);
                g_error_free(error);
                return NULL;
            }
        }
        Py_RETURN_NONE;
    }

    // Close input stream if readable and not closed
    if (self->input != NULL && !g_input_stream_is_closed(self->input)) {
        if (!g_input_stream_close(self->input, NULL, &error)) {
            PyErr_SetString(PyExc_IOError, error->message);
            g_error_free(error);
            return NULL;
        }
    }

    // Close output stream if writable and not closed
    if (self->output != NULL && !g_output_stream_is_closed(self->output)) {
        if (!g_output_stream_close(self->output, NULL, &error)) {
            PyErr_SetString(PyExc_IOError, error->message);
            g_error_free(error);
            return NULL;
        }
    }

    Py_RETURN_NONE;
}


static gboolean
is_readable(StreamWrapper *self)
{
    return self->input != NULL;
}


static PyObject *
err_not_readable(void)
{
    PyErr_SetString(PyExc_IOError, "Stream is not readable");
    return NULL;
}


static PyObject *
StreamWrapper_readable(StreamWrapper *self, PyObject *Py_UNUSED(ignored))
{
    if (is_readable(self)) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}


static PyObject *
StreamWrapper_read(StreamWrapper *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"size", NULL};
    Py_ssize_t size = -1;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|n", kwlist, &size)) {
        return NULL;
    }

    if (!is_readable (self)) return err_not_readable ();

    if (is_closed (self)) return err_closed ();

    if (size == 0) {
        // Return empty bytes
        return PyBytes_FromStringAndSize("", 0);
    }

    GError *error = NULL;
    GBytes *gbytes = NULL;
    GInputStream *input = self->input;

    if (size > 0) {
        // Read up to size bytes
        gbytes = g_input_stream_read_bytes(input, (gsize)size, NULL, &error);
        if (gbytes == NULL) {
            PyErr_SetString(PyExc_IOError, error ? error->message : "Read error");
            g_clear_error(&error);
            return NULL;
        }
        // Convert to Python bytes and free GBytes
        PyObject *result = PyBytes_FromStringAndSize((const char *)g_bytes_get_data(gbytes, NULL),
                                                     g_bytes_get_size(gbytes));
        g_bytes_unref(gbytes);
        return result;
    } else {
        // Read until EOF
        PyObject *result = PyBytes_FromStringAndSize(NULL, 0);
        if (result == NULL)
            return NULL;

        Py_ssize_t total_read = 0;
        const Py_ssize_t chunk_size = 4096;
        char buffer[chunk_size];

        while (1) {
            gssize n = g_input_stream_read(input, buffer, chunk_size, NULL, &error);
            if (n < 0) {
                Py_DECREF(result);
                PyErr_SetString(PyExc_IOError, error ? error->message : "Read error");
                g_clear_error(&error);
                return NULL;
            }
            if (n == 0)  // EOF
                break;

            if (_PyBytes_Resize(&result, total_read + n) < 0) {
                Py_DECREF(result);
                return NULL;
            }
            memcpy(PyBytes_AS_STRING(result) + total_read, buffer, n);
            total_read += n;
        }

        return result;
    }
}


static PyObject *
StreamWrapper_readinto(StreamWrapper *self, PyObject *args)
{
    PyObject *buffer_obj;

    if (!PyArg_ParseTuple(args, "O", &buffer_obj))
        return NULL;

    if (!is_readable (self)) return err_not_readable ();

    if (is_closed (self)) return err_closed ();

    // Get writable buffer view
    Py_buffer view;
    if (PyObject_GetBuffer(buffer_obj, &view, PyBUF_WRITABLE) == -1) {
        // Not writable buffer
        return NULL;
    }

    GError *error = NULL;
    gssize n_read = g_input_stream_read(self->input, (gchar *)view.buf, (gsize)view.len, NULL, &error);

    if (n_read < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_IOError, error ? error->message : "Read error");
        g_clear_error(&error);
        return NULL;
    }

    PyBuffer_Release(&view);

    return PyLong_FromSsize_t(n_read);
}


static gboolean
is_writable(StreamWrapper *self)
{
    return self->output != NULL;
}


static PyObject *
err_not_writable(void)
{
    PyErr_SetString(PyExc_IOError, "Stream is not writable");
    return NULL;
}


static PyObject *
StreamWrapper_writable(StreamWrapper *self, PyObject *Py_UNUSED(ignored))
{
    if (is_writable(self)) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}


static PyObject *
StreamWrapper_write(StreamWrapper *self, PyObject *args)
{
    Py_buffer view;
    gssize bytes_written;
    GError *error = NULL;

    if (!is_writable(self)) return err_not_writable ();

    if (is_closed (self)) return err_closed ();

    // Parse one argument: a bytes-like object
    if (!PyArg_ParseTuple(args, "y*", &view)) {
        return NULL;  // Not bytes-like, error set by PyArg_ParseTuple
    }

    if (view.len == 0) {
        // Nothing to write, release buffer and return 0
        PyBuffer_Release(&view);
        return PyLong_FromLong(0);
    }

    // Write all bytes from view.buf of length view.len
    gboolean success = g_output_stream_write_all(self->output,
                                                 view.buf,
                                                 view.len,
                                                 (gsize *)&bytes_written,
                                                 NULL,
                                                 &error);

    PyBuffer_Release(&view);

    if (!success) {
        PyErr_SetString(PyExc_IOError, error ? error->message : "Write failed");
        g_clear_error(&error);
        return NULL;
    }

    return PyLong_FromSsize_t(bytes_written);
}


static PyObject *
StreamWrapper_flush(StreamWrapper *self, PyObject *Py_UNUSED(ignored))
{
    if (self->output == NULL) {
        // No output stream to flush — just return None
        Py_RETURN_NONE;
    }

    if (is_closed (self)) return err_closed ();

    GError *error = NULL;
    gboolean success = g_output_stream_flush(self->output, NULL, &error);

    if (!success) {
        PyErr_SetString(PyExc_IOError, error ? error->message : "Flush failed");
        g_clear_error(&error);
        return NULL;
    }

    Py_RETURN_NONE;
}


static gboolean
is_seakable(StreamWrapper *self)
{
    if (G_IS_SEEKABLE(self->ref)) {
        return g_seekable_can_seek (G_SEEKABLE(self->ref));
    } else {
        return FALSE;
    }
}


static PyObject *
err_not_seekable(void)
{
    PyErr_SetString(PyExc_IOError, "Underlying stream is not seekable");
    return NULL;
}


static PyObject *
StreamWrapper_seekable(StreamWrapper *self, PyObject *Py_UNUSED(ignored))
{
    if (is_seakable(self)) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}


static PyObject *
StreamWrapper_seek(StreamWrapper *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"offset", "whence", NULL};
    gint64 offset;
    int whence = SEEK_SET;
    GSeekType seek_type;
    GError *error = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "L|i", kwlist, &offset, &whence)) {
        return NULL;
    }

    if (is_closed (self)) return err_closed ();

    if (!is_seakable (self)) return err_not_seekable ();

    switch (whence) {
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
            PyErr_SetString(PyExc_ValueError, "Invalid whence value");
            return NULL;
    }

    if (!g_seekable_seek(G_SEEKABLE(self->ref), offset, seek_type, NULL, &error)) {
        PyErr_SetString(PyExc_IOError, error->message);
        g_error_free(error);
        return NULL;
    }

    gint64 pos = g_seekable_tell(G_SEEKABLE(self->ref));
    if (pos < 0) {
        PyErr_SetString(PyExc_IOError, "Failed to get stream position after seek");
        return NULL;
    }

    return PyLong_FromLongLong(pos);
}


static PyObject *
StreamWrapper_tell(StreamWrapper *self, PyObject Py_UNUSED(ignored))
{
    if (is_closed (self)) return err_closed ();

    if (!is_seakable (self)) return err_not_seekable ();

    gint64 pos;

    // Get the current position
    pos = g_seekable_tell(G_SEEKABLE(self->ref));

    if (pos < 0) {
        PyErr_SetString(PyExc_IOError, "Failed to get stream position");
        return NULL;
    }

    return PyLong_FromLongLong(pos);
}


static PyObject *
StreamWrapper_fileno(StreamWrapper *self, PyObject *Py_UNUSED(ignored))
{
    if (is_closed (self)) return err_closed ();

    int fd;

    // Check if ref implements GFileDescriptorBased
    if (!G_IS_FILE_DESCRIPTOR_BASED(self->ref)) {
        PyErr_SetString(PyExc_OSError, "Underlying stream does not expose a file descriptor");
        return NULL;
    }

    fd = g_file_descriptor_based_get_fd(G_FILE_DESCRIPTOR_BASED(self->ref));
    return PyLong_FromLong(fd);
}


static PyObject *
StreamWrapper_isatty(StreamWrapper *self, PyObject *Py_UNUSED(ignored))
{
    if (is_closed (self)) return err_closed ();

    Py_RETURN_FALSE;
}


static void
StreamWrapper_dealloc(StreamWrapper *self) {
    if (self->input) g_object_unref(self->input);
    if (self->output) g_object_unref(self->output);
    if (self->io) g_object_unref(self->io);
    Py_TYPE(self)->tp_free((PyObject *)self);
}


static PyMethodDef StreamWrapper_methods[] = {
    {"readable", (PyCFunction)StreamWrapper_readable, METH_NOARGS,
     "Return whether the stream is readable"},
    {"read", (PyCFunction)StreamWrapper_read, METH_VARARGS | METH_KEYWORDS,
     "Read up to size bytes from the stream"},
    {"readinto", (PyCFunction)StreamWrapper_readinto, METH_VARARGS,
     "Read bytes into a writable buffer"},
    {"writable", (PyCFunction)StreamWrapper_writable, METH_NOARGS,
     "Return whether the stream is writable"},
    {"write", (PyCFunction)StreamWrapper_write, METH_VARARGS,
     "Write bytes to the underlying stream"},
    {"seekable", (PyCFunction)StreamWrapper_seekable, METH_NOARGS,
     "Return True if stream supports seeking"},
    {"flush", (PyCFunction)StreamWrapper_flush, METH_NOARGS,
     "Flush the output stream"},
    {"tell", (PyCFunction)StreamWrapper_tell, METH_NOARGS,
     "Flush and close the underlying stream"},
    {"seek", (PyCFunction)StreamWrapper_seek, METH_VARARGS | METH_KEYWORDS,
     "Flush and close the underlying stream"},
    {"fileno", (PyCFunction)StreamWrapper_fileno, METH_NOARGS,
     "Flush and close the underlying stream"},
    {"close", (PyCFunction)StreamWrapper_close, METH_NOARGS,
     "Flush and close the underlying stream"},
    {"isatty", (PyCFunction)StreamWrapper_isatty, METH_NOARGS,
     "Flush and close the underlying stream"},
    {NULL, NULL, 0, NULL}
};


static PyGetSetDef StreamWrapper_getsetters[] = {
    {"closed", (getter)StreamWrapper_get_closed, NULL,
     "True if the underlying stream is closed", NULL},
    {NULL}
};


PyTypeObject StreamWrapperType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "gio_pyio.StreamWrapper",
    .tp_doc = "GIO StreamWrapper",
    .tp_basicsize = sizeof(StreamWrapper),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_init = (initproc)StreamWrapper_init,
    .tp_dealloc = (destructor)StreamWrapper_dealloc,
    .tp_methods = StreamWrapper_methods,
    .tp_getset = StreamWrapper_getsetters,
};
