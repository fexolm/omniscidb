// DbEngine.h

#ifndef __DB_ENGINE_H
#define __DB_ENGINE_H

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <type_traits>
#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "QueryEngine/TargetValue.h"

//#include "QueryEngine/ResultSet.h"

namespace OmnisciDbEngine {

    class Row {
    public:
        Row();
        Row(std::vector<TargetValue>& row);
        int64_t GetInt(size_t col);
        double GetDouble(size_t col);
        std::string GetStr(size_t col);

    private:
         std::vector<TargetValue> m_row;
    };

    class Cursor {
    public:
        size_t GetColCount();
        size_t GetRowCount();
        Row GetNextRow();
        int GetColType(uint32_t nPos);
        std::shared_ptr<arrow::RecordBatch> GetArrowRecordBatch();
    };

    class DBEngine {
    public:
        void Reset();
        void ExecuteDDL(std::string sQuery);
        Cursor* ExecuteDML(std::string sQuery);
        static DBEngine* Create(std::string sPath);

    protected:
        DBEngine() {}
    };
}

#endif // __DB_ENGINE_H