/**
 * ���ݿⱸ�����ܲ���ʵ��
 *
 * ����Ŀ�ģ����Ա��ݺͻָ�����
 * ��ģʽ��Blog
 * �������ã�NTSE���ã�NC_MEDIUM record_size = 200
 *	 �������¼��������
 *		C1: û�и���
 *		C2��û�и��أ��������ڴ���
 *		C3: �������ڴ��У�update/delete/insert: 100op/s, select 600op/s
 *		C4: �������ڴ��У�update/delete/insert: 10op/s, select 60op/s
 *
 * �������ݣ����������
 * 
 * �������̣�
 *		1.	���������¼�����ݿ��СΪ200M��
 *		2.	Ԥ�ȡ�����һ���ɨ��
 *		3.	���������̣߳�����Ԥ�����ط������ݿ������
 *		4.	�������ݿ⡣
 *		5.	�����ٶ�=���ݵ�������/��ʱ
 *
 * @author �ձ�(bsu@corp.netease.com naturally@163.org)
 */

#include "Backup.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "util/System.h"
#include "util/File.h"
#include "BlogTable.h"
#include "Random.h"
#include "IntgTestHelper.h"
#include <sstream>
#include "TableHelper.h"

using namespace ntse;
using namespace ntseperf;
using namespace std;

/**
 * ���ݲ�ͬ�Ĳ���ָ��������������C1��C4֮�е�����һ������
 */
DBBackupTest::DBBackupTest(bool warmup, uint readOps, uint writeOps) {
	setConfig(CommonDbConfig::getMedium());
	setTableDef(BlogTable::getTableDef(true));
	m_warmup = warmup;
	m_readOps = readOps;
	m_writeOps = writeOps;
	m_totalRecSizeLoaded = TESTCASE_DATA_SIZE;
	m_threadNum = DEFAULT_THREADS;
	m_troubleThreads = NULL;
	sprintf(m_backupDir, "%s/backup", m_config->m_tmpdir);
}


DBBackupTest::~DBBackupTest() {
}

void DBBackupTest::setUp() {
}

void DBBackupTest::tearDown() {
	closeDatabase();
}

string DBBackupTest::getName() const {
	stringstream ss;
	ss << "DatabaseBackup(";
	if (m_warmup)
		ss << "Warmup, ";
	if (m_readOps != 0 || m_writeOps != 0)
		ss << "With loading, ";
	if (isHeavyLoading())
		ss << "Heavy loading";
	ss << ")";
	return ss.str();
}


string DBBackupTest::getDescription() const {
	return "Database backup test";
}


void DBBackupTest::loadData(u64 *totalRecSize, u64 *recCnt) {
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
 * Ԥ��Blog������
 * @post ������Ӧ�ñ���ȡ���ڴ�
 * ����ִ�в��Բ��赱�еĲ���2��3
 */
void DBBackupTest::warmUp() {
	const char *f = m_backupDir;
	File(f).mkdir();
	openTable(false);
	if (m_warmup)	// ��һ������ҪԤ��
		TableWarmUpHelper::warmUpTableByIndice(m_db, m_table, true, true);

	if (m_readOps != 0 || m_writeOps != 0) {	// ���������߳�ģ�����ݿ���ʳ���
		m_running = true;
		balanceThreadNums();
		m_troubleThreads = new BusyGuy*[m_threadNum];
		for (u16 i = 0; i < m_threadNum; i++)
			m_troubleThreads[i] = new BusyGuy(i, this, m_readOps / m_threadNum, m_writeOps / m_threadNum);
		RandomGen::setSeed((unsigned)time(NULL));
		for (u16 i = 0; i < m_threadNum; i++) {
			m_troubleThreads[i]->start();
		}

		// ��Ҫͣ��һ��ʱ��ȵ�ϵͳ��������ȶ��˲Ž��к�������
		Thread::msleep(10000);
	}
}


/**
 * ���Ա��ݣ�ִ�в��Բ��赱�еĲ���4
 */
void DBBackupTest::run() {
	m_opCnt = m_readOps + m_writeOps;
	m_opDataSize = m_totalRecSizeLoaded;
	// ��ʼ�������ݿⱸ��
	dbBackup();
	// ֹͣ�����߳�
	stopThreads();
}


/**
 * ��������ʹ�õ��߳���
 */
void DBBackupTest::balanceThreadNums() {
	while (true) {
		if (m_readOps / m_threadNum > THREADS_MAX_READS || m_writeOps / m_threadNum > THREADS_MAX_WRITES)
			m_threadNum *= 2;
		else
			break;
	}
}

/**
 * �Ե�ǰ���ݿ�ִ�б��ݲ���
 */
void DBBackupTest::dbBackup() {
	BackupProcess *bp = 0;
	try {
		bp = m_db->initBackup(m_backupDir);
		m_db->doBackup(bp);
		m_db->finishingBackupAndLock(bp);
	} catch(NtseException &e) {
		cout << e.getMessage() << endl;
		m_db->doneBackup(bp);
		return;
	}
	m_db->doneBackup(bp);
}


void DBBackupTest::stopThreads() {
	m_running = false;
	Thread::msleep(10 * 1000);
	
	if (m_troubleThreads) {
		for (u16 i = 0; i < m_threadNum; i++) {
			if (m_troubleThreads[i]) {
				m_troubleThreads[i]->join();
				delete m_troubleThreads[i];
			}
		}

		delete[] m_troubleThreads;
	}
}


void DBBackupTest::randomDML(bool scanOp, uint opid, uint threadId) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DBBackupTest::randomDML", conn);

	u64 opNo = RandomGen::nextInt(0, (int)m_recCntLoaded);

	if (scanOp) {	// ��ѯ
		RowId rowId;
		TableDMLHelper::fetchRecord(session, m_table, opNo, NULL, &rowId, false);
	} else {
		uint updateNo = RandomGen::nextInt(0, 3);
		if (updateNo == 0) {	// ����
			uint key = MAX_ID - 1 - opid * m_threadNum - threadId;
			TableDMLHelper::insertRecord(session, m_table, key, opid);
		} else if (updateNo == 1) {	// ɾ��
			TableDMLHelper::deleteRecord(session, m_table, &opNo);
		} else {	// ����
			TableDMLHelper::updateRecord(session, m_table, &opNo, &opNo, opid, false);
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


void BusyGuy::helpThreadOperation() {
	u64 start = System::currentTimeMillis();

	uint readOps = m_readOps, writeOps = m_writeOps;
	uint totalOps = m_readOps + m_writeOps;

	for (uint i = 0; i < totalOps; i++) {
		uint opKind = RandomGen::nextInt(0, 2);
		if (opKind == 0 && m_readOps == 0 || opKind == 1 && m_writeOps == 1)
			opKind = 1 - opKind;
		if (opKind == 0) {	// ɨ��
			assert(m_readOps > 0);
			m_testcase->randomDML(true, i, m_threadId);
			--readOps;
		} else {	// ����
			assert(m_writeOps > 0);
			m_testcase->randomDML(false, i, m_threadId);
			--writeOps;
		}
	}

	u64 end = System::currentTimeMillis();

	int diff = 1000 - (int)(end - start);

	if (diff >= 0)
		Thread::msleep(diff);
	else {	// ��ӡ������Ϣ������ǰ����Ƶ�ʴﲻ��Ԥ��Ҫ��
		cout << "[Warning]: The database performance is lower than expected: "
			<< "Doing " << m_readOps << " times scan and doing " << m_writeOps << " times update"
			<< "cost " << (end - start) << " ms"
			<< endl;
	}
}


/**
 * ���츺�ص��̣߳�ÿ���̶߳��ᰴ���ƶ��ı������ִ�в�ѯ�͸��²���
 * ÿ���߳�ִ�еĲ�������Ҳ�ǰ����ƶ�������������
 */
void BusyGuy::run() {
	while (m_testcase->isRunning()) {
		helpThreadOperation();
	}
}