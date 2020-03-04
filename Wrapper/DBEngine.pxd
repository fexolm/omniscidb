from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from cython.operator cimport dereference as deref

# Declare the class with cdef
cdef extern from "QueryEngine/ResultSet.h":
    cdef cppclass ResultSet:
        ResultSet()

        string getName() except *

cdef extern from "DBEngine.h" namespace "OmnisciDbEngine":
    cdef cppclass DBEngine:
        void ExecuteDDL(string)
        shared_ptr[ResultSet] ExecuteDML(string)
        void Reset()
        @staticmethod
        DBEngine* Create(string)
