#include "EmptyTestCase.h"


#include "misc/Syslog.h"
#include "util/Thread.h"
#include "IntgTestHelper.h"

// Make Source Insight happy
using namespace ntse;
using namespace ntseperf;


EmptyTestCase::EmptyTestCase() {
	m_enableCache = true;
	m_isMemoryCase = false;
	m_totalRecSizeLoaded = 0;
	m_recCntLoaded = 0;
	m_opCnt = 0;
	m_opDataSize = 0;
	m_db = 0;
	m_tableDef = 0;
	m_config = 0;
	m_tableDef = 0;
	m_status = new TestCaseStatus;
}

EmptyTestCase::~EmptyTestCase() {
	delete m_config;
	delete m_tableDef;
	delete m_status;
}

void EmptyTestCase::run() {
	Thread::msleep(10);
}

/**
 * 创建表或者打开表
 * @param create 是否创建表
 * @pre m_config, m_tableDef都已经初始化
 */
void EmptyTestCase::openTable(bool create) {
	m_db = Database::open(m_config, true);
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("EmptyTestCase::openTable", conn);
	if (create) {
		m_db->createTable(session, m_tableDef->m_name, m_tableDef);
		// 创建mysql需要的frm文件
		char frmFileName[255];
		sprintf(frmFileName, "%s/%s.frm", m_config->m_basedir, m_tableDef->m_name);
		if (!createSpecifiedFile(frmFileName))
			cout << "[Warming:] Create frm file failed!" << endl;
	}
	m_table = m_db->openTable(session, m_tableDef->m_name);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 关闭数据库 */
void EmptyTestCase::closeDatabase() {
	if (m_db && m_table) {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("EmptyTestCase::closeDatabase", conn);
		m_db->closeTable(session, m_table);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_table = 0;
		m_db->close();
		delete m_db;
		m_db = 0;
	}
}
/** 禁止数据库IO 操作 */
void EmptyTestCase::disableIo() {
	m_db->setCheckpointEnabled(false);
	m_db->getPageBuffer()->disableScavenger();
	m_db->getTxnlog()->enableFlusher(false);
}

/** 设置本测试用例的表定义 */
void EmptyTestCase::setTableDef(const TableDef *tableDef) {
	m_tableDef = new TableDef(tableDef);
}

/** 设置本测试用例的数据库配置 */
void EmptyTestCase::setConfig(const Config *cfg) {
	m_config = new Config(cfg);
}


/**
* 获取统计信息
* @return 统计信息，返回0表示没有统计信息
*/
const TestCaseStatus* EmptyTestCase::getStatus() const {
	if (!m_db)
		return 0;

	m_status->m_bufferStatus = m_db->getPageBuffer()->getStatus();
	m_status->m_logStatus = m_db->getTxnlog()->getStatus();
	m_status->m_mmsStatus = m_db->getMms()->getStatus();
	return m_status;
}

