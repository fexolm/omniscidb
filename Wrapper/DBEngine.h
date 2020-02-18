// DbEngine.h

#ifndef __DB_ENGINE_H
#define __DB_ENGINE_H

#include<string>
#include<memory>
#include "QueryEngine/ResultSet.h"

namespace OmnisciDbEngine {

    class DBEngine {
    public:
        void Reset();
        void ExecuteDDL(std::string sQuery);
        std::shared_ptr<ResultSet> ExecuteDML(std::string sQuery);
        static DBEngine* Create(std::string sPath);

    protected:
        DBEngine() {}
    };

    int TargetValueToInt(const TargetValue *v);
    double TargetValueToDouble(const TargetValue *v);
    std::string TargetValueToString(const TargetValue *v);
}

#endif // __DB_ENGINE_H