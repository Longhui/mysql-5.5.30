
/**
 * 大对象碎片整理实现。
 *
 * @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#include <string>
#include "api/LobDefraggler.h"
#include "util/SmartPtr.h"
#include "lob/Lob.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/ParFileParser.h"

using namespace std;
using namespace ntse;

/** 
 * 在线大对象碎片整理。
 *
 * @param session	会话。
 * @throw NtseException 执行碎片整理失败。
 */
void Table::defragLobs(Session *session) throw (NtseException) {
	assert(NULL != session);
	assert(NULL != m_db);

	int timeout = m_db->getConfig()->m_tlTimeout;

	//加锁并刷出数据
	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	dualFlush(session, timeout);
	
	//设置表的flushLsn
	m_db->bumpFlushLsn(session, m_tableDef->m_id);


	//整理大对象碎片
	LobStorage *lobStorage = getLobStorage();
	if (NULL == lobStorage) {
		//解锁
		unlockMeta(session, IL_X);
		NTSE_THROW(NTSE_EC_GENERIC, "Failed to get lob sob storage.");
	}	
	lobStorage->defrag(session);

	//解锁
	unlockMeta(session, IL_X);
}

/** 
 * 重做在线大对象碎片整理。
 *
 * @param session	会话。
 * @param lsn		日志号
 * @param log		日志。
 */
void Table::redoMoveLobs(Session *session, LsnType lsn, const LogEntry *log) throw (NtseException) {
	assert(NULL != session);
	assert(NULL != log);
	
	assert(LOG_LOB_MOVE == log->m_logType);
	m_records->getLobStorage()->redoMove(session, lsn, log->m_data, (uint)log->m_size);
}

/** 
 * 构造函数。
 *
 * @param db				数据库。
 * @param conn				数据库连接。
 * @param schemeTableStr	数据库表, 格式为scheme.table_name。
 */
LobDefraggler::LobDefraggler(Database *db, Connection *conn, const char *schemeTableStr) {
	assert(NULL != db);
	assert(NULL != conn);
	assert(NULL != schemeTableStr);

	m_db = db;
	m_conn = conn;
	m_schemeTableStr = System::strdup(schemeTableStr);
}
/** 
 * 析构函数。
 */
LobDefraggler::~LobDefraggler() {
	if (NULL != m_schemeTableStr) {
		delete[] m_schemeTableStr;
		m_schemeTableStr = NULL;
	}
}

/** 
 * 执行大对象数据碎片整理。
 *
 * @throw NtseException 执行碎片整理失败。
 */
void LobDefraggler::startDefrag() throw(NtseException) {
	assert(NULL != m_db);
	assert(NULL != m_schemeTableStr);
	assert(NULL != m_conn);

	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, m_conn);

	char *pos = m_schemeTableStr;
	bool hasDelimiter = false;
	while ('\0' != *pos) {
		if (*pos == '.') {
			*pos = '/';
			hasDelimiter = true;
			break;
		}
		pos++;
	}
	if (!hasDelimiter) {
		NTSE_THROW(NTSE_EC_GENERIC, "Table name format should be 'scheme.table_name' but '%s' provided", m_schemeTableStr);
	}

	//获取LobStorage对象
	string path = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_schemeTableStr;
	Table *table = NULL;

	std::vector<string> parFiles;
	try {
		ParFileParser parser(path.c_str());
		parser.parseParFile(parFiles);
	} catch (NtseException &e) {
		m_db->getSyslog()->log(EL_PANIC, "Parse the partition table .par file error: %s", e.getMessage());
		throw e;
	}

	try {
		for (uint i = 0; i < parFiles.size(); i++) {
			table = m_db->openTable(session, parFiles[i].c_str());
			table->defragLobs(session);
			m_db->closeTable(session, table);
			table = NULL;
		}
	} catch (NtseException &e) {
		if (table) {
			m_db->closeTable(session, table);
		}
		//释放session资源
		m_db->getSessionManager()->freeSession(session);
		//重抛出异常
		throw e;
	}	

	//释放session资源	
	m_db->getSessionManager()->freeSession(session);
}

