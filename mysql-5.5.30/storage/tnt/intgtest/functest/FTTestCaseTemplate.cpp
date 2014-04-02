/**
 * ���ܲ��Կ�ܻ���ʵ����
 *
 * @author �ձ�(bsu@corp.netease.com, naturally@163.org)
 */

#include "FTTestCaseTemplate.h"
#include "api/Table.h"
#include "api/Database.h"

using namespace std;
using namespace ntse;
using namespace ntsefunc;

FTTestCaseTemplate::FTTestCaseTemplate(size_t threadNum) {
	m_threadNum = threadNum;
	m_opCnt = 0;
	m_opDataSize = 0;
	m_db = NULL;
	m_config = NULL;
	m_tableInfo = NULL;
	m_tables = 0;
	m_threads = NULL;
}

FTTestCaseTemplate::~FTTestCaseTemplate() {
	if (m_threads != NULL) {
		for (uint i = 0; i < m_threadNum; i++) {
			delete m_threads[i];
		}
		delete[] m_threads;
	}

	if (m_tableInfo != NULL) {
		for (uint i = 0; i < m_tables; i++) {
			delete m_tableInfo[i];
		}
		delete[] m_tableInfo;
	}

	delete m_config;
}

/**
 * ����ִ�й�������
 */
void FTTestCaseTemplate::run() {
	Thread::msleep(10);
}

/** �ر����ݿ� */
/**
 * @param normalClose	�Ƿ�ִ�������ر�
 */
void FTTestCaseTemplate::closeDatabase(bool normalClose) {
	if (m_db) {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("Slave:;closeTable", conn);


		for (uint i = 0; i < m_tables; i++) {
			m_tableInfo[i]->m_table = NULL;
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->close(normalClose, normalClose);
		delete m_db;
		m_db = NULL;
	}
}

/**
 * ��ָ���ı�
 * @param create	�����±�
 * @param recover	�����ݿ�ʱ�Ƿ�ǿ�ƻָ�
 */
void FTTestCaseTemplate::openTable(bool create, int recover) {
	m_db = Database::open(m_config, true, recover);
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("EmptyTestCase::openTable", conn);
	if (create) {
		for (uint i = 0; i < m_tables; i++) {
			m_db->createTable(session, m_tableInfo[i]->m_tableDef->m_name, m_tableInfo[i]->m_tableDef);
		}
	}
	for (uint i = 0; i < m_tables; i++) {
		m_tableInfo[i]->m_table = m_db->openTable(session, m_tableInfo[i]->m_tableDef->m_name);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ���ñ���
 * @param descTableDef	Ŀ�����
 * @param srcTableDef	ԭʼ����
 */
void FTTestCaseTemplate::setTableDef(TableDef **descTableDef, const TableDef *srcTableDef) {
	*descTableDef = (TableDef*)srcTableDef;
}

/** 
	���ñ��������������ݿ����� 
* @param cfg ���� 
*/
void FTTestCaseTemplate::setConfig(const Config *cfg) {
	m_config = new Config(cfg);
}