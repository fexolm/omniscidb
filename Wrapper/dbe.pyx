from cython.operator cimport dereference as deref
#from __future__ import print_function
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libc.stdint cimport int64_t, uint64_t, uint32_t
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp cimport bool
from libcpp.pair cimport pair
from libcpp.vector cimport vector


import os

# Create a Cython extension type which holds a C++ instance
# as an attribute and create a bunch of forwarding methods
# Python extension type.
# cimport DBEngine
from DBEngine cimport DBEngine
from DBEngine cimport ResultSet


cdef class PyResultSet:
    cdef shared_ptr[ResultSet] c_result_set  #Hold a C++ instance which we're wrapping

    def __cinit__(self):
        self.c_result_set.reset()

#    cdef size_t size(PyResultSet self):
#        return self.c_result_set.get().colCount()

cdef object PyStruct(shared_ptr[ResultSet] PyResultSet_ptr):
    """Python object factory class taking Cpp ResultSet pointer
    as argument
    """
    # Create new PyResultSet object. This does not create
    # new structure but does allocate a null pointer
    cdef PyResultSet py_wrapper = PyResultSet()
    # Set pointer of cdef class to existing struct ptr
    py_wrapper.c_result_set = PyResultSet_ptr
    # Return the wrapped PyResultSet object with PyResultSet_ptr
    return py_wrapper

cdef class PyDbEngine:
    cdef DBEngine* c_dbe  #Hold a C++ instance which we're wrapping

    def __cinit__(self, path):
        self.c_dbe = DBEngine.Create(path)

    def __dealloc__(self):
        self.c_dbe.Reset()
        del self.c_dbe

    def executeDDL(self, query):
        try:
            self.c_dbe.ExecuteDDL(query)
        except Exception, e:
            os.abort()

    def executeDML(self, query):
        """        try:"""
        cdef PyResultSet mypystruct = PyStruct(self.c_dbe.ExecuteDML(query))
        return mypystruct
        """        except Exception, e:
        os.abort()
        """