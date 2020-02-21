// DbEngine.cpp

#include "DBEngine.h"
#include <thrift/Thrift.h>
#include "Catalog/Catalog.h"
#include "QueryEngine/ResultSet.h"
#include "QueryRunner/QueryRunner.h"
#include "QueryEngine/CompilationOptions.h"
#include "Import/Importer.h"
#include "Shared/Logger.h"
#include "Shared/mapdpath.h"
#include "Shared/sqltypes.h"
#include <array>
#include <boost/filesystem.hpp>
#include <boost/variant.hpp>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#define CALCITEPORT 3279

namespace OmnisciDbEngine {

    enum class ColumnType : uint32_t {
        eUNK = 0,
        eINT = 1,
        eDBL = 2,
        eFLT = 3,
        eSTR = 4,
        eARR = 5
    };

    class CursorImpl : public Cursor {
    public:
        CursorImpl(std::shared_ptr<ResultSet> resultSet)
        : m_resultSet(resultSet) {
        }

        size_t GetColCount() {
            return m_resultSet->colCount();
        }

        size_t GetRowCount() {
            return m_resultSet->rowCount();
        }

        Row GetNextRow() {
            auto row = m_resultSet->getNextRow(true, false); //std::vector<TargetValue>
            if (row.empty()) {
                std::cout << "Cursor::GetNextRow - Empty row" << std::endl;
                return Row();
            }
            return Row(row);
        }

        ColumnType GetColType(int nPos) {
            if (nPos < GetColCount()) {
                SQLTypeInfo typeInfo = m_resultSet->getColType(nPos);
                switch(typeInfo.get_type()) {
                    case kNUMERIC:
                    case kDECIMAL:
                    case kINT:
                    case kSMALLINT:
                    case kBIGINT:
                        return ColumnType::eINT;

                    case kDOUBLE:
                        return ColumnType::eDBL;

                    case kFLOAT:
                        return ColumnType::eFLT;

                    case kCHAR:
                    case kVARCHAR:
                    case kTEXT:
                        return ColumnType::eSTR;

                    default:
                        return ColumnType::eUNK;
                }
            }
            return ColumnType::eUNK;
        }

    private:
        std::shared_ptr<ResultSet> m_resultSet;
    };

//////////////////////////////////////////////////////////////////////////// DBEngineImp
    class DBEngineImpl : public DBEngine {
    public:
        const std::string OMNISCI_DEFAULT_DB = "omnisci";
        const std::string OMNISCI_ROOT_USER = "admin";
        const std::string OMNISCI_DATA_PATH = "//mapd_data";
        //const int CALCITEPORT = 3279;

        void Reset() {
            std::cout << "DESTRUCTOR DBEngineImpl" << std::endl;
            if (m_pQueryRunner)
            m_pQueryRunner->reset();
        }

        void ExecuteDDL(const std::string& sQuery) {
            std::cout << "START EXECUTE DDL: " << sQuery << std::endl;
            if (m_pQueryRunner != nullptr) {
                m_pQueryRunner->runDDLStatement(sQuery);
            }
            std::cout << "END EXECUTE DDL" << std::endl;
        }

        template <class T>
        T v(const TargetValue& r) {
            auto scalar_r = boost::get<ScalarTargetValue>(&r);
            CHECK(scalar_r);
            auto p = boost::get<T>(scalar_r);
            CHECK(p);
            return *p;
        }

        //std::shared_ptr<ResultSet> ExecuteDML(const std::string& sQuery)
        Cursor* ExecuteDML(const std::string& sQuery) {
            if (m_pQueryRunner != nullptr) {
                auto rs = m_pQueryRunner->runSQL(sQuery, ExecutorDeviceType::CPU);
                m_Cursors.emplace_back(new CursorImpl(rs));
                return m_Cursors.back(); //m_pQueryRunner->runSQL(sQuery, ExecutorDeviceType::CPU);
            }
            std::cout << "Query Runner is NULL" << std::endl;
        }

        DBEngineImpl(const std::string& sBasePath) 
        : m_sBasePath(sBasePath)
        , m_pQueryRunner(nullptr) {
            if (!boost::filesystem::exists(m_sBasePath)) {
                std::cerr << "Catalog basepath " + m_sBasePath + " does not exist.\n";
            } else {
                MapDParameters mapdParms;
                std::string sDataPath = m_sBasePath + OMNISCI_DATA_PATH;
                m_DataMgr = std::make_shared<Data_Namespace::DataMgr>(sDataPath, mapdParms, false, 0);
                auto calcite = std::make_shared<Calcite>(-1, CALCITEPORT, m_sBasePath, 1024);
                auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
                sys_cat.init(m_sBasePath, m_DataMgr, {}, calcite, false, false, {});
                if (!sys_cat.getSqliteConnector()) {
                    std::cerr << "SqliteConnector is null " << std::endl;
                } else {
                    sys_cat.getMetadataForDB(OMNISCI_DEFAULT_DB, m_Database); //TODO: Check
                    auto catalog = Catalog_Namespace::Catalog::get(m_sBasePath, m_Database, m_DataMgr, std::vector<LeafHostInfo>(), calcite, false);
                    sys_cat.getMetadataForUser(OMNISCI_ROOT_USER, m_User);
                    auto session = std::make_unique<Catalog_Namespace::SessionInfo>(catalog, m_User, ExecutorDeviceType::CPU, "");
                    m_pQueryRunner = QueryRunner::QueryRunner::init(session);
                }
            }
        }

    private:
        std::string m_sBasePath;
        std::shared_ptr<Data_Namespace::DataMgr> m_DataMgr;
        Catalog_Namespace::DBMetadata m_Database;
        Catalog_Namespace::UserMetadata m_User;
        QueryRunner::QueryRunner* m_pQueryRunner;
        std::vector<CursorImpl*> m_Cursors;
    };

    DBEngine* DBEngine::Create(std::string sPath) {
	return new DBEngineImpl(sPath);
    }

    // downcasting methods
    inline DBEngineImpl * GetImpl(DBEngine* ptr) { return (DBEngineImpl *)ptr; }
    inline const DBEngineImpl * GetImpl(const DBEngine* ptr) { return (const DBEngineImpl *)ptr; }

    void DBEngine::Reset() {
        DBEngineImpl* pEngine = GetImpl(this);
        //pEngine->Reset();
    }

    void DBEngine::ExecuteDDL(std::string sQuery) {
        DBEngineImpl* pEngine = GetImpl(this);
        pEngine->ExecuteDDL(sQuery);
    }

//    std::shared_ptr<ResultSet> 
    Cursor* DBEngine::ExecuteDML(std::string sQuery) {
        DBEngineImpl* pEngine = GetImpl(this);
        return pEngine->ExecuteDML(sQuery);
    }

    Row::Row() {
    }

    Row::Row(std::vector<TargetValue>& row)
    : m_row(std::move(row)) {
    }

    int64_t Row::GetInt(size_t nCol) {
        if (nCol < m_row.size()) {
            const auto scalarValue = boost::get<ScalarTargetValue>(&m_row[nCol]);
            const auto value = boost::get<int64_t>(scalarValue);
            return *value; // error: invalid conversion from 'const long int*' to 'int64_t {aka long int}' 
        }
        return 0;
    }

    double Row::GetDouble(size_t nCol) {
        if (nCol < m_row.size()) {
            const auto scalarValue = boost::get<ScalarTargetValue>(&m_row[nCol]);
            const auto value = boost::get<double>(scalarValue);
            return *value;
        }
        return 0.;
    }

    std::string Row::GetStr(size_t nCol) {
        if (nCol < m_row.size()) {
            const auto scalarValue = boost::get<ScalarTargetValue>(&m_row[nCol]);
            auto value = boost::get<NullableString>(scalarValue);
            bool is_null = !value || boost::get<void*>(value);
            if (is_null) {
                return "Empty";
            } else {
                auto value_notnull = boost::get<std::string>(value);
                return *value_notnull;
            }
        }
        return "Out of range";
    }


//////////////////////////////////////////////////////////////////////////////////////////////


    // downcasting methods
    inline CursorImpl * GetImpl(Cursor* ptr) { return (CursorImpl *)ptr; }
    inline const CursorImpl * GetImpl(const Cursor* ptr) { return (const CursorImpl *)ptr; }

    size_t Cursor::GetColCount() {
        CursorImpl* pCursor = GetImpl(this);
        return pCursor->GetColCount();
    }

    size_t Cursor::GetRowCount() {
        CursorImpl* pCursor = GetImpl(this);
        return pCursor->GetRowCount();
    }

    Row Cursor::GetNextRow() {
        CursorImpl* pCursor = GetImpl(this);
        return pCursor->GetNextRow();
    }

    int Cursor::GetColType(int nPos) {
        CursorImpl* pCursor = GetImpl(this);
        int nColType = (int)pCursor->GetColType(nPos);
        return nColType;
    }
}