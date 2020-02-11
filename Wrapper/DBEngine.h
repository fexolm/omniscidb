// DbEngine.h

#ifndef __DB_ENGINE_H
#define __DB_ENGINE_H

#include<string>

namespace OmnisciDbEngine {
    class DBEngine {
    public:

		void Reset();
		void ExecuteDDL(std::string sQuery);
		void ExecuteDML(std::string sQuery);
		static DBEngine* Create(std::string sPath);

    protected:

		DBEngine() {}
    };
}

#endif // __DB_ENGINE_H