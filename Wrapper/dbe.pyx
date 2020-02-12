from cython.operator cimport dereference as deref

from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
#from libcpp cimport bool
import os

# Create a Cython extension type which holds a C++ instance
# as an attribute and create a bunch of forwarding methods
# Python extension type.
from DBEngine cimport DBEngine

#def pyExecute(query):
#    return Execute(query)

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
        try:
            self.c_dbe.ExecuteDML(query)
        except Exception, e:
            os.abort()
