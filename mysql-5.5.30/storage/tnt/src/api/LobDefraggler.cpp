
/**
 * �������Ƭ����ʵ�֡�
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
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
 * ���ߴ������Ƭ����
 *
 * @param session	�Ự��
 * @throw NtseException ִ����Ƭ����ʧ�ܡ�
 */
void Table::defragLobs(Session *session) throw (NtseException) {
	assert(NULL != session);
	assert(NULL != m_db);

	int timeout = m_db->getConfig()->m_tlTimeout;

	//������ˢ������
	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	dualFlush(session, timeout);
	
	//���ñ��flushLsn
	m_db->bumpFlushLsn(session, m_tableDef->m_id);


	//����������Ƭ
	LobStorage *lobStorage = getLobStorage();
	if (NULL == lobStorage) {
		//����
		unlockMeta(session, IL_X);
		NTSE_THROW(NTSE_EC_GENERIC, "Failed to get lob sob storage.");
	}	
	lobStorage->defrag(session);

	//����
	unlockMeta(session, IL_X);
}

/** 
 * �������ߴ������Ƭ����
 *
 * @param session	�Ự��
 * @param lsn		��־��
 * @param log		��־��
 */
void Table::redoMoveLobs(Session *session, LsnType lsn, const LogEntry *log) throw (NtseException) {
	assert(NULL != session);
	assert(NULL != log);
	
	assert(LOG_LOB_MOVE == log->m_logType);
	m_records->getLobStorage()->redoMove(session, lsn, log->m_data, (uint)log->m_size);
}

/** 
 * ���캯����
 *
 * @param db				���ݿ⡣
 * @param conn				���ݿ����ӡ�
 * @param schemeTableStr	���ݿ��, ��ʽΪscheme.table_name��
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
 * ����������
 */
LobDefraggler::~LobDefraggler() {
	if (NULL != m_schemeTableStr) {
		delete[] m_schemeTableStr;
		m_schemeTableStr = NULL;
	}
}

/** 
 * ִ�д����������Ƭ����
 *
 * @throw NtseException ִ����Ƭ����ʧ�ܡ�
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

	//��ȡLobStorage����
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
		//�ͷ�session��Դ
		m_db->getSessionManager()->freeSession(session);
		//���׳��쳣
		throw e;
	}	

	//�ͷ�session��Դ	
	m_db->getSessionManager()->freeSession(session);
}

