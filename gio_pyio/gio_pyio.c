#define PY_SSIZE_T_CLEAN
#include "gio_pyio.h"
#include "streamwrapper.h"
#include <Python.h>

PyObject *UnsupportedOperation = NULL;
PyObject *PyGObjectClass = NULL;

static struct PyModuleDef _gio_pyio_module
    = { PyModuleDef_HEAD_INIT,
        "gio_pyio",
        "Module wrapping GIO streams as Python file objects",
        -1,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL };

PyMODINIT_FUNC
PyInit__gio_pyio (void)
{
  PyObject *m;

  PyObject *io_module = PyImport_ImportModule ("io");
  if (!io_module)
    return NULL;

  UnsupportedOperation
      = PyObject_GetAttrString (io_module, "UnsupportedOperation");
  Py_DECREF (io_module);
  if (!UnsupportedOperation)
    return NULL;
  Py_INCREF (UnsupportedOperation);

  PyObject *gi_module = PyImport_ImportModule ("gi.repository.GObject");
  if (!gi_module)
    return NULL;

  PyGObjectClass = PyObject_GetAttrString (gi_module, "GObject");
  Py_DECREF (gi_module);
  if (!PyGObjectClass)
    return NULL;
  Py_INCREF (PyGObjectClass);

  m = PyModule_Create (&_gio_pyio_module);
  if (m == NULL)
    return NULL;

  PyObject *streamwrapper_type = PyStreamWrapperType_Create ();
  if (!streamwrapper_type)
    return NULL;

  if (PyModule_AddObject (m, "StreamWrapper", streamwrapper_type) < 0)
    {
      Py_DECREF (streamwrapper_type);
      Py_DECREF (m);
      return NULL;
    }

  return m;
}
