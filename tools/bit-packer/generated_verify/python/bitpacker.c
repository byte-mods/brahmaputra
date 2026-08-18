#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

typedef struct {
    PyObject_HEAD
    char *buf;
    size_t capacity;
    size_t offset;
    size_t limit; // Not strictly needed if offset tracks write, but useful for read bounds if we passed size
} ZeroCopyByteBuff;

static void
ZeroCopyByteBuff_dealloc(ZeroCopyByteBuff *self)
{
    if (self->buf) {
        free(self->buf);
    }
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject *
ZeroCopyByteBuff_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    ZeroCopyByteBuff *self;
    self = (ZeroCopyByteBuff *) type->tp_alloc(type, 0);
    if (self != NULL) {
        self->buf = NULL;
        self->capacity = 0;
        self->offset = 0;
    }
    return (PyObject *) self;
}

static int
ZeroCopyByteBuff_init(ZeroCopyByteBuff *self, PyObject *args, PyObject *kwds)
{
    PyObject *data = NULL;
    static char *kwlist[] = {"data", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &data))
        return -1;

    if (data && data != Py_None) {
        if (PyBytes_Check(data)) {
            Py_ssize_t size = PyBytes_Size(data);
            self->buf = malloc(size);
            if (!self->buf) {
                PyErr_NoMemory();
                return -1;
            }
            memcpy(self->buf, PyBytes_AsString(data), size);
            self->capacity = size;
            self->offset = 0; // Decoding starts at 0
        } else {
             PyErr_SetString(PyExc_TypeError, "data must be bytes");
             return -1;
        }
    } else {
        // Default 64MB based on our optimization findings
        self->capacity = 64 * 1024 * 1024; 
        self->buf = malloc(self->capacity);
        if (!self->buf) {
            PyErr_NoMemory();
            return -1;
        }
        self->offset = 0;
    }

    return 0;
}

static void ensure_capacity(ZeroCopyByteBuff *self, size_t needed) {
    if (self->offset + needed > self->capacity) {
        size_t new_capacity = self->capacity * 2;
        if (new_capacity < self->offset + needed) {
            new_capacity = self->offset + needed;
        }
        char *new_buf = realloc(self->buf, new_capacity);
        if (!new_buf) {
            return; 
        }
        self->buf = new_buf;
        self->capacity = new_capacity;
    }
}

// --- Write Helpers ---

static PyObject *
ZeroCopyByteBuff_put_varint64(ZeroCopyByteBuff *self, PyObject *args)
{
    long long v;
    if (!PyArg_ParseTuple(args, "L", &v)) // L for long long (64 bit)
        return NULL;
    
    // ZigZag
    unsigned long long zz = (v << 1) ^ (v >> 63);

    ensure_capacity(self, 10);
    char *p = self->buf + self->offset;

    // Unrolled fast path logic similar to Python but in C
    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }
    
    self->offset = p - self->buf;
    Py_RETURN_NONE;
}

// Aliases
static PyObject *ZeroCopyByteBuff_put_int32(ZeroCopyByteBuff *self, PyObject *args) { return ZeroCopyByteBuff_put_varint64(self, args); }
static PyObject *ZeroCopyByteBuff_put_int64(ZeroCopyByteBuff *self, PyObject *args) { return ZeroCopyByteBuff_put_varint64(self, args); }

static PyObject *
ZeroCopyByteBuff_put_float(ZeroCopyByteBuff *self, PyObject *args)
{
    float v;
    if (!PyArg_ParseTuple(args, "f", &v)) return NULL;
    
    // Reuse varint logic: int(v * 10000.0)
    long long iv = (long long)(v * 10000.0f);
    
    unsigned long long zz = (iv << 1) ^ (iv >> 63);

    ensure_capacity(self, 10);
    char *p = self->buf + self->offset;

    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }
    self->offset = p - self->buf;
    Py_RETURN_NONE;
}

static PyObject *
ZeroCopyByteBuff_put_double(ZeroCopyByteBuff *self, PyObject *args)
{
    double v;
    if (!PyArg_ParseTuple(args, "d", &v)) return NULL;
    long long iv = (long long)(v * 10000.0);
    unsigned long long zz = (iv << 1) ^ (iv >> 63);

    ensure_capacity(self, 10);
    char *p = self->buf + self->offset;

    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }
    self->offset = p - self->buf;
    Py_RETURN_NONE;
}

static PyObject *
ZeroCopyByteBuff_put_bool(ZeroCopyByteBuff *self, PyObject *args)
{
    int v; // bool is int in C API
    if (!PyArg_ParseTuple(args, "p", &v)) return NULL;
    ensure_capacity(self, 1);
    self->buf[self->offset++] = v ? 1 : 0;
    Py_RETURN_NONE;
}

static PyObject *
ZeroCopyByteBuff_put_string(ZeroCopyByteBuff *self, PyObject *args)
{
    char *s;
    Py_ssize_t len;
    if (!PyArg_ParseTuple(args, "s#", &s, &len)) return NULL;

    // len varint
    unsigned long long zz = ((unsigned long long)len) << 1; // Always positive

    ensure_capacity(self, len + 10);
    char *p = self->buf + self->offset;

    if (zz < 0x80) {
        *p++ = (char)zz;
    } else if (zz < 0x4000) {
        *p++ = (char)((zz & 0x7F) | 0x80);
        *p++ = (char)(zz >> 7);
    } else {
        while (zz & ~0x7F) {
            *p++ = (char)((zz & 0x7F) | 0x80);
            zz >>= 7;
        }
        *p++ = (char)zz;
    }
    
    memcpy(p, s, len);
    self->offset = (p - self->buf) + len;
    Py_RETURN_NONE;
}

// --- Read Helpers ---

static PyObject *
ZeroCopyByteBuff_get_varint64(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    if (self->offset >= self->capacity) { // Simple bounds check, tough to do exact without decoding
         PyErr_SetString(PyExc_IndexError, "Buffer underflow");
         return NULL;
    }
    
    char *p = self->buf + self->offset;
    unsigned long long result = 0;
    int shift = 0;
    
    while (1) {
        unsigned char b = *p++;
        result |= ((unsigned long long)(b & 0x7F)) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    self->offset = p - self->buf;
    
    long long decoded = (result >> 1) ^ -(long long)(result & 1);
    return PyLong_FromLongLong(decoded);
}

// Aliases
static PyObject *ZeroCopyByteBuff_get_int32(ZeroCopyByteBuff *self, PyObject *a) { return ZeroCopyByteBuff_get_varint64(self, a); }
static PyObject *ZeroCopyByteBuff_get_int64(ZeroCopyByteBuff *self, PyObject *a) { return ZeroCopyByteBuff_get_varint64(self, a); }

static PyObject *
ZeroCopyByteBuff_get_float(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    // Reuse varint logic
    PyObject *val = ZeroCopyByteBuff_get_varint64(self, NULL);
    if (!val) return NULL;
    long long iv = PyLong_AsLongLong(val);
    Py_DECREF(val);
    return PyFloat_FromDouble((double)iv / 10000.0);
}

static PyObject *
ZeroCopyByteBuff_get_double(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    PyObject *val = ZeroCopyByteBuff_get_varint64(self, NULL);
    if (!val) return NULL;
    long long iv = PyLong_AsLongLong(val);
    Py_DECREF(val);
    return PyFloat_FromDouble((double)iv / 10000.0);
}

static PyObject *
ZeroCopyByteBuff_get_bool(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    if (self->offset >= self->capacity) {
         PyErr_SetString(PyExc_IndexError, "Buffer underflow");
         return NULL;
    }
    int val = self->buf[self->offset++] != 0;
    if (val) Py_RETURN_TRUE;
    else Py_RETURN_FALSE;
}

static PyObject *
ZeroCopyByteBuff_get_string(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    // Decode length (varint)
    char *p = self->buf + self->offset;
    unsigned long long result = 0;
    int shift = 0;
    
    while (1) {
        unsigned char b = *p++;
        result |= ((unsigned long long)(b & 0x7F)) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    // Update offset temporarily
    self->offset = p - self->buf;
    
    long long len = (result >> 1); // ZigZag decode positive
    
    if (self->offset + len > self->capacity) {
        PyErr_SetString(PyExc_IndexError, "Buffer underflow (string)");
        return NULL;
    }
    
    PyObject *s = PyUnicode_FromStringAndSize(self->buf + self->offset, len);
    self->offset += len;
    return s;
}

static PyObject *
ZeroCopyByteBuff_get_bytes(ZeroCopyByteBuff *self, PyObject *Py_UNUSED(ignored))
{
    return PyBytes_FromStringAndSize(self->buf, self->offset);
}

static PyObject *
ZeroCopyByteBuff_ensure_capacity(ZeroCopyByteBuff *self, PyObject *args)
{
    long needed;
    if (!PyArg_ParseTuple(args, "l", &needed)) return NULL;
    ensure_capacity(self, (size_t)needed);
    Py_RETURN_NONE;
}

static PyMethodDef ZeroCopyByteBuff_methods[] = {
    {"put_int32", (PyCFunction)ZeroCopyByteBuff_put_int32, METH_VARARGS, "Put int32"},
    {"put_int64", (PyCFunction)ZeroCopyByteBuff_put_int64, METH_VARARGS, "Put int64"},
    {"put_varint64", (PyCFunction)ZeroCopyByteBuff_put_varint64, METH_VARARGS, "Put varint64"},
    {"put_float", (PyCFunction)ZeroCopyByteBuff_put_float, METH_VARARGS, "Put float"},
    {"put_double", (PyCFunction)ZeroCopyByteBuff_put_double, METH_VARARGS, "Put double"},
    {"put_bool", (PyCFunction)ZeroCopyByteBuff_put_bool, METH_VARARGS, "Put bool"},
    {"put_string", (PyCFunction)ZeroCopyByteBuff_put_string, METH_VARARGS, "Put string"},
    {"ensure_capacity", (PyCFunction)ZeroCopyByteBuff_ensure_capacity, METH_VARARGS, "Ensure capacity"},
    
    {"get_int32", (PyCFunction)ZeroCopyByteBuff_get_int32, METH_NOARGS, "Get int32"},
    {"get_int64", (PyCFunction)ZeroCopyByteBuff_get_int64, METH_NOARGS, "Get int64"},
    {"get_varint64", (PyCFunction)ZeroCopyByteBuff_get_varint64, METH_NOARGS, "Get varint64"},
    {"get_float", (PyCFunction)ZeroCopyByteBuff_get_float, METH_NOARGS, "Get float"},
    {"get_double", (PyCFunction)ZeroCopyByteBuff_get_double, METH_NOARGS, "Get double"},
    {"get_bool", (PyCFunction)ZeroCopyByteBuff_get_bool, METH_NOARGS, "Get bool"},
    {"get_string", (PyCFunction)ZeroCopyByteBuff_get_string, METH_NOARGS, "Get string"},
    
    {"get_bytes", (PyCFunction)ZeroCopyByteBuff_get_bytes, METH_NOARGS, "Get internal bytes"},
    {NULL}  /* Sentinel */
};

static PyTypeObject ZeroCopyByteBuffType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_bitpacker.ZeroCopyByteBuff",
    .tp_doc = "ZeroCopyByteBuff implemented in C",
    .tp_basicsize = sizeof(ZeroCopyByteBuff),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = ZeroCopyByteBuff_new,
    .tp_init = (initproc) ZeroCopyByteBuff_init,
    .tp_dealloc = (destructor) ZeroCopyByteBuff_dealloc,
    .tp_methods = ZeroCopyByteBuff_methods,
};

static PyModuleDef bitpackermodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_bitpacker",
    .m_doc = "Example module that creates an extension type.",
    .m_size = -1,
};

PyMODINIT_FUNC
PyInit__bitpacker(void)
{
    PyObject *m;
    if (PyType_Ready(&ZeroCopyByteBuffType) < 0)
        return NULL;

    m = PyModule_Create(&bitpackermodule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&ZeroCopyByteBuffType);
    if (PyModule_AddObject(m, "ZeroCopyByteBuff", (PyObject *) &ZeroCopyByteBuffType) < 0) {
        Py_DECREF(&ZeroCopyByteBuffType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
