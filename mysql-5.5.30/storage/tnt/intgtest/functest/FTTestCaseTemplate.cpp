/**
 * 功能测试框架基本实现类
 *
 * @author 苏斌(bsu@corp.netease.com, naturally@163.org)
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
 * 用例执行过程主题
 */
void FTTestCaseTemplate::run() {
	Thread::msleep(10);
}

/** 关闭数据库 */
/**
 * @param normalClose	是否执行正常关闭
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
 * 打开指定的表
 * @param create	创建新表
 * @param recover	打开数据库时是否强制恢复
 */
void FTTestCaseTemplate::openTable(bool create, int recover) {
	m_db = Database::open(m_config, true, recover);
	// 创建Session
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
 * 设置表定义
 * @param descTableDef	目标表定义
 * @param srcTableDef	原始表定义
 */
void FTTestCaseTemplate::setTableDef(TableDef **descTableDef, const TableDef *srcTableDef) {
	*descTableDef = (TableDef*)srcTableDef;
}

/** 
	设置本测试用例的数据库配置 
* @param cfg 配置 
*/
void FTTestCaseTemplate::setConfig(const Config *cfg) {
	m_config = new Config(cfg);
}