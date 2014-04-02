#include "TNTEmptyTestCase.h"


#include "misc/Syslog.h"
#include "util/Thread.h"
#include "IntgTestHelper.h"
#include "TNTTableHelper.h"

// Make Source Insight happy
using namespace tnt;
using namespace tntperf;
using namespace ntseperf;

TNTEmptyTestCase::TNTEmptyTestCase() {
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

TNTEmptyTestCase::~TNTEmptyTestCase() {
//	delete m_config;
	delete m_tableDef;
	delete m_status;
}

void TNTEmptyTestCase::run() {
	Thread::msleep(10);
}

/**
 * ��������ߴ򿪱�
 * @param create �Ƿ񴴽���
 * @pre m_config, m_tableDef���Ѿ���ʼ��
 */
void TNTEmptyTestCase::openTable(bool create) {
	m_db = TNTDatabase::open(m_config, true,0);
	// ����Session
    Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("TNTEmptyTestCase::openTable", conn);
	TNTTransaction *trx = startTrx(m_db->getTransSys(), session);
	if (create) {
		m_db->createTable(session, m_tableDef->m_name, m_tableDef);
		// ����mysql��Ҫ��frm�ļ�
		char frmFileName[255];
		sprintf(frmFileName, "%s/%s.frm", m_config->m_ntseConfig.m_basedir, m_tableDef->m_name);
		if (!createSpecifiedFile(frmFileName))
			cout << "[Warming:] Create frm file failed!" << endl;
	}
	m_table = m_db->openTable(session, m_tableDef->m_name);
	commitTrx(m_db->getTransSys(), trx);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}

/** �ر����ݿ� */
void TNTEmptyTestCase::closeDatabase() {
	if (m_db && m_table) {
		Connection *conn = m_db->getNtseDb()->getConnection(false);
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("EmptyTestCase::closeDatabase", conn);
		m_db->closeTable(session, m_table);
		m_db->getNtseDb()->getSessionManager()->freeSession(session);
		m_db->getNtseDb()->freeConnection(conn);
		m_table = 0;
		m_db->close();
		delete m_db;
		m_db = 0;
	}
}
/** ��ֹ���ݿ�IO ���� */
void TNTEmptyTestCase::disableIo() {
	Database *ntsedb = m_db->getNtseDb();
	ntsedb->setCheckpointEnabled(false);    
	ntsedb->getPageBuffer()->disableScavenger();
	ntsedb->getTxnlog()->enableFlusher(false);
}

/** ���ñ����������ı��� */
void TNTEmptyTestCase::setTableDef(const TableDef *tableDef) {
	m_tableDef = new TableDef(tableDef);
}

/** ���ñ��������������ݿ����� */
void TNTEmptyTestCase::setConfig(TNTConfig *cfg) {
	m_config = cfg;
}


/**
* ��ȡͳ����Ϣ
* @return ͳ����Ϣ������0��ʾû��ͳ����Ϣ
*/
const TestCaseStatus* TNTEmptyTestCase::getStatus() const {
	if (!m_db)
		return 0;

	m_status->m_bufferStatus = m_db->getNtseDb()->getPageBuffer()->getStatus();
	m_status->m_logStatus = m_db->getNtseDb()->getTxnlog()->getStatus();
	m_status->m_mmsStatus = m_db->getNtseDb()->getMms()->getStatus();
	return m_status;
}

