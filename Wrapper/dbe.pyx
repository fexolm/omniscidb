from cython.operator cimport dereference as deref

from libcpp.memory cimport unique_ptr
from libcpp.string cimport string

cimport Wrapper

def init():
    Wrapper.init()

def destroy():
    Wrapper.destroy()

def run_ddl_statement(query):
    Wrapper.run_ddl_statement(query)

def run_simple_agg(query):
    Wrapper.run_simple_agg(query)