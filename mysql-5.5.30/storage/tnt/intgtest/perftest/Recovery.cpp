/**
 * ���ݿ�ָ����ܲ���ʵ��
 *
 * ����Ŀ�ģ����������ָ�����
 * ��ģʽ��Blog
 * �������ã�NTSE���ã�NC_MEDIUM record_size = 200
 * �������ݣ����������
 *
 * �������̣�
 *		1.	���������¼�����ݿ��СΪ200M��
 *		2.	��һ��checkpoint����¼����LSN:ckLsn1���ر����ݿ�
 *		3.	�������ݿ�Ŀ¼��backupĿ¼
 *		4.	����10000����¼
 *		5.	����10000����¼
 *		6.	ɾ��10000����¼
 *		7.	�ر����ݿ⣬������backupĿ¼�е����ݿ��ļ��������ļ������ݿ�Ŀ¼��
 *		8.	�����ݿ���лָ�������ʱT
 *		9.	�ָ��ٶ�30000/T(rec/s), (tailLsn �C ckLsn1)/T/1024/1024(MB/s)
 *
 * @author �ձ�(bsu@corp.netease.com naturally@163.org)
 */

#include "Recovery.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/Buffer.h"
#include "misc/ControlFile.h"
#include "util/System.h"
#include "util/File.h"
#include "BlogTable.h"
#include "Random.h"
#include "IntgTestHelper.h"
#include <sstream>

using namespace ntse;
using namespace ntseperf;
using namespace std;

DBRecoveryTest::DBRecoveryTest(u64 dataSize) {
	setConfig(CommonDbConfig::getMedium());
	setTableDef(BlogTable::getTableDef(true));
	m_dataSize = dataSize;
	m_totalRecSizeLoaded = dataSize;
}

void DBRecoveryTest::setUp() {
	ts.recv = true;
	vs.idx = true;
	vs.hp = true;
	m_dmlThreads = new Graffiti*[DML_THREAD_NUM];
	for (uint i = 0; i < DML_THREAD_NUM; i++) {
		m_dmlThreads[i] = new Graffiti(i, this);
	}
}

void DBRecoveryTest::tearDown() {
	stopThreads();
	closeDatabase();
}

string DBRecoveryTest::getName() const {
	stringstream ss;
	ss << "DatabaseRecovery(DataSize:";
	ss << m_dataSize / 1024 / 1024;
	ss << "MB)";
	return ss.str();
}


string DBRecoveryTest::getDescription() const {
	return "Database recovery test";
}


void DBRecoveryTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);

	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::loadData", conn);

	assert(m_totalRecSizeLoaded > 0);
	m_recCntLoaded = BlogTable::populate(session, m_table, &m_totalRecSizeLoaded, m_totalRecSizeLoaded / m_table->getTableDef()->m_maxRecSize * 10, 0);
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}


/**
 * ����������ҪԤ��
 * ����ִ�в������̵�2-7������
 */
void DBRecoveryTest::warmUp() {
	openTable(false);

	RandomGen::setSeed((unsigned)time(NULL));

	// ��ִ��һ�μ���
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::warmUp", conn);
	m_db->checkpoint(session);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_ckLsn = m_db->getTxnlog()->tailLsn();
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	// �������ݿ��ļ�
	closeDB();
	backupFiles(m_config->m_tmpdir, TABLE_NAME_BLOG, true, true);
	openDB();
	m_db->setCheckpointEnabled(false);

	// ִ�����DML����
	for (uint i = 0; i < DML_THREAD_NUM; i++) {
		m_dmlThreads[i]->start();
	}

	for (uint i = 0; i < DML_THREAD_NUM; i++) {
		m_dmlThreads[i]->join();
	}

	m_tailLsn = m_db->getTxnlog()->tailLsn();	

	// �ָ������ļ�
	closeDB(false);
	backupFiles(m_config->m_tmpdir, TABLE_NAME_BLOG, true, false);

	return;
}


u64 DBRecoveryTest::getDataSize() {
	return m_tailLsn - m_ckLsn;
}

/**
 * �����ݿ���лָ����õ��ָ���ʱ
 * ����ִ�в��Բ����8-9��������
 */
void DBRecoveryTest::run() {
	m_opCnt = TOTAL_DML_UPDATE_TIMES;
	m_opDataSize = m_totalRecSizeLoaded;
	// ʹ����־�ָ�֮ǰ���ݵ����ݿ��ļ�
	openDB(1);	// �����ǿ�ƻָ�
}


/**
 * �����ݿ�
 */
void DBRecoveryTest::openDB(int recover) {
	assert(m_db == NULL);
	Config *cfg = new Config(CommonDbConfig::getMedium());
	m_db = Database::open(cfg, true, recover);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("EmptyTestCase::openTable", conn);
	m_table = m_db->openTable(session, m_tableDef->m_name);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * �ر����ݿ�
 */
void DBRecoveryTest::closeDB(bool normalClose) {
	assert(m_db != NULL);
	m_db->close(normalClose, normalClose);
	delete m_db;
	m_db = NULL;
}


void DBRecoveryTest::stopThreads() {
	if (m_dmlThreads) {
		for (u16 i = 0; i < DML_THREAD_NUM; i++) {
			if (m_dmlThreads[i]) {
				m_dmlThreads[i]->join();
				delete m_dmlThreads[i];
			}
		}

		delete[] m_dmlThreads;
	}
}

/**
 * ������в��롢ɾ�������²�����һ��
 */
void DBRecoveryTest::randomDMLModify(uint threadNo, uint opid) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DBBackupTest::randomDML", conn);

	int opKind = RandomGen::nextInt();
	for (uint i = 0; i < 3; i++) {
		u64 opNo = RandomGen::nextInt(0, MAX_ID);
		switch(opKind % 3) {
			case 0:		// ����
				{
					u64 key = MAX_ID - 1 - DML_THREAD_NUM * opid - threadNo;
					TableDMLHelper::insertRecord(session, m_table, key, opid);
					break;
				}
			case 1:		// ɾ��
				TableDMLHelper::deleteRecord(session, m_table, &opNo);
				break;
			default:	// ����
				TableDMLHelper::updateRecord(session, m_table, &opNo, &opNo, opid, false);
				break;
		}

		opKind++;
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * ִ��DML�������߳̾���ʵ��
 */
void Graffiti::run() {
	for (uint i = 0; i < m_testcase->TOTAL_DML_UPDATE_TIMES / m_testcase->DML_THREAD_NUM; i++) {
		m_testcase->randomDMLModify(m_threadId, i);
	}
}
