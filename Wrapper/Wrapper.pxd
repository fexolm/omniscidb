from libcpp.string cimport string

# Declare the class with cdef
cdef extern from "DBEngine.h" namespace "Wrapper":
    void run_ddl_statement(string)
    void run_simple_agg(string)
    void init()
    void destroy()
