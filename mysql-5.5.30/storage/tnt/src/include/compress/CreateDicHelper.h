/**
* 字典创建助手
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/
#ifndef  _NTSE_CREATE_DIC_HELPER_H_
#define  _NTSE_CREATE_DIC_HELPER_H_

#include "misc/Global.h"
#include "misc/ParFileParser.h"
#include "api/Table.h"
#include "api/Database.h"

namespace ntse {

	class CreateDicHelper {
	public:
		CreateDicHelper(Database *db, Connection *conn, const char *schemeTableStr) {
			assert(NULL != db);
			assert(NULL != conn);
			assert(NULL != schemeTableStr);

			m_db = db;
			m_conn = conn;
			m_schemeTableStr = System::strdup(schemeTableStr);
		}
		~CreateDicHelper() {
			if (NULL != m_schemeTableStr) {
				delete [] m_schemeTableStr;
				m_schemeTableStr = NULL;
			}
		}
		/**
		 * 创建字典
		 * @throw 不是压缩表，已经存在字典，或者加锁超时等
		 */
		bool createDictionary() throw(NtseException) {
			assert(NULL != m_db);
			assert(NULL != m_schemeTableStr);
			assert(NULL != m_conn);

			Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, m_conn);

			checkPath();

			Table *table = NULL;
			std::vector<string> parFiles;
			string path = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_schemeTableStr;
			try {
				ParFileParser parser(path.c_str());
				parser.parseParFile(parFiles);
			} catch (NtseException &e) {
				m_db->getSyslog()->log(EL_PANIC, "Parse the partition table .par file error: %s", e.getMessage());
				throw e;
			}

			try{
				for (uint i = 0; i < parFiles.size(); i++) {
					table = m_db->openTable(session, parFiles[i].c_str());
					m_db->createCompressDic(session, table);
					if (table != NULL) {
						m_db->closeTable(session, table);
					}
				}
			} catch (NtseException &e) {
				if (table != NULL) {
					m_db->closeTable(session, table);
				}
				//释放session资源
				m_db->getSessionManager()->freeSession(session);
				//重抛出异常
				throw e;
			}
			
			//释放session资源
			m_db->getSessionManager()->freeSession(session);
			return true;
		}

	private:
		/**
		 * 检查数据库名及表名是否正确
		 * @throw 
		 */
		void checkPath() throw(NtseException) {
			char *pos = m_schemeTableStr;
			bool hasDelimiter = false;
			uint dotCnt = 0;
			while ('\0' != *pos) {
				if (*pos == '.') {
					*pos = NTSE_PATH_SEP_CHAR;
					hasDelimiter = true;
					dotCnt++;
				}
				pos++;
			}
			if (!hasDelimiter || dotCnt != 1) {
				NTSE_THROW(NTSE_EC_GENERIC, "Table name format should be 'scheme.table_name' but '%s' provided", m_schemeTableStr);
			}
		}
	private:
		Database   *m_db;					/* 数据库。 */
		Connection *m_conn;				    /* 数据库连接 */
		char       *m_schemeTableStr;		/* 数据库表名，格式scheme.table_name。 */
	};

}
#endif