// DbEngine.h

#ifndef __DB_ENGINE_H
#define __DB_ENGINE_H

#include <string>
#include <vector>
#include "QueryEngine/TargetValue.h"
//#include<memory>
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
        int GetColType(int nPos);
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