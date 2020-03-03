from __future__ import absolute_import

from libc.stdint cimport int64_t, uint64_t, uint32_t
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp cimport bool, nullptr_t, nullptr
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from cython.operator cimport dereference as deref

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow cimport CStatus

from cpython cimport PyObject
from libcpp.cast cimport dynamic_cast

from pyarrow.includes.libarrow cimport (CArray, CBuffer, CDataType,
                                        CField, CRecordBatch, CSchema,
                                        CTable)
#, CTensor,
#                                        CSparseCSRMatrix, CSparseCOOTensor)
from pyarrow.compat import frombytes

cdef extern from "arrow/python/pyarrow.h" namespace "arrow::py":
    cdef int import_pyarrow() except -1
    cdef object wrap_buffer(const shared_ptr[CBuffer]& buffer)
    cdef object wrap_data_type(const shared_ptr[CDataType]& type)
    cdef object wrap_field(const shared_ptr[CField]& field)
    cdef object wrap_schema(const shared_ptr[CSchema]& schema)
    cdef object wrap_array(const shared_ptr[CArray]& sp_array)
#    cdef object wrap_tensor(const shared_ptr[CTensor]& sp_tensor)
#    cdef object wrap_sparse_tensor_coo(
#        const shared_ptr[CSparseCOOTensor]& sp_sparse_tensor)
#    cdef object wrap_sparse_tensor_csr(
#        const shared_ptr[CSparseCSRMatrix]& sp_sparse_tensor)
    cdef object wrap_table(const shared_ptr[CTable]& ctable)
    cdef object wrap_record_batch(const shared_ptr[CRecordBatch]& cbatch)

#from cpython.buffer cimport PyObject_GetBuffer, PyBUF_FULL
#cdef extern from *:
#    cdef cppclass CRecordBatch:
#        pass
#    cdef shared_ptr[CRecordBatch] cpp_function(Py_buffer *buf)


# Declare the class with cdef

cdef extern from "boost/variant.hpp" namespace "boost":
    cdef cppclass boostvariant "boost::variant" [T1]:
        pass
    cdef cppclass boostvariant2 "boost::variant" [T1, T2]:
        pass
    cdef cppclass boostvariant4 "boost::variant" [T1, T2, T3, T4]:
        pass


ctypedef boostvariant2[string, void*] NullableString

ctypedef boostvariant4[int64_t, double, float, NullableString] ScalarTargetValue

cdef extern from "boost/optional.hpp" namespace "boost":
    cdef cppclass boostoptional "boost::optional" [T]:
        pass

ctypedef boostoptional[vector[ScalarTargetValue]] boost_optional_vector

ctypedef boostvariant[boost_optional_vector] ArrayTargetValue

ctypedef boostvariant2[ScalarTargetValue, ArrayTargetValue] TargetValue

cdef extern from "QueryEngine/TargetValue.h":
    cdef cppclass ResultSet:
        ResultSet()
        string getName() except *
        vector[TargetValue] getNextRow(const bool translate_strings, const bool decimal_to_double)

cdef extern from "QueryEngine/ResultSet.h":
    cdef cppclass ResultSet:
        size_t colCount()
        size_t rowCount(const bool force_parallel)
        vector[TargetValue] getNextRow(const bool translate_strings, const bool decimal_to_double)
        TargetValue getRowAt(const size_t row_idx, const size_t col_idx, const bool translate_strings, const bool decimal_to_double)

cdef extern from "DBEngine.h" namespace "OmnisciDbEngine":
    cdef cppclass Row:
        int64_t GetInt(size_t col);
        double GetDouble(size_t col);
        string GetStr(size_t col);

    cdef cppclass Cursor:
        size_t GetColCount()
        size_t GetRowCount()
        Row GetNextRow()
        int GetColType(uint32_t nPos)
        shared_ptr[CRecordBatch] GetArrowRecordBatch()

    cdef cppclass DBEngine:
        void ExecuteDDL(string)
        Cursor* ExecuteDML(string)
#        shared_ptr[ResultSet] ExecuteDML(string)
        void Reset()
        @staticmethod
        DBEngine* Create(string)


