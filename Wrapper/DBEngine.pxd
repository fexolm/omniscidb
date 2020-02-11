from libcpp.string cimport string

# Declare the class with cdef
cdef extern from "DBEngine.h" namespace "OmnisciDbEngine":
    cdef cppclass DBEngine:
        void Execute(string, int)
        void Reset()
        @staticmethod
        DBEngine* Create(string)
