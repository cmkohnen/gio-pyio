#define PY_SSIZE_T_CLEAN
#include <Python.h>

extern PyTypeObject StreamWrapperType;


static struct PyModuleDef _gio_pyio_module = {
    PyModuleDef_HEAD_INIT,
    "gio_pyio",
    "Module wrapping GIO streams as Python file objects",
    -1,
    NULL, NULL, NULL, NULL, NULL
};


PyMODINIT_FUNC
PyInit__gio_pyio(void) {
    PyObject *m;

    if (PyType_Ready(&StreamWrapperType) < 0)
        return NULL;

    PyObject *io_module = PyImport_ImportModule("io");
    if (!io_module)
        return NULL;

    PyObject *io_base = PyObject_GetAttrString(io_module, "IOBase");
    Py_DECREF(io_module);
    if (!io_base)
        return NULL;

    PyObject *bases = PyTuple_Pack(1, io_base);
    Py_DECREF(io_base);
    if (!bases)
        return NULL;

    StreamWrapperType.tp_bases = bases;

    m = PyModule_Create(&_gio_pyio_module);
    if (m == NULL)
        return NULL;

    Py_INCREF(&StreamWrapperType);
    PyModule_AddObject(m, "StreamWrapper", (PyObject *)&StreamWrapperType);

    return m;
}
