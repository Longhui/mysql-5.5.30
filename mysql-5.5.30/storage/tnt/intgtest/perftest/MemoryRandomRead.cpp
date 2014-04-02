#include "MemoryRandomRead.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "CountTable.h"
#include "AccountTable.h"
#include "Random.h"
#include "TableHelper.h"
#include <sstream>

using namespace ntseperf;
using namespace std;

/** 
 * ���췽��
 *
 * @param useMms ʹ��MMS
 * @param isVar �Ƿ�Ϊ�䳤��
 * @param threadCount ���̸߳���
 * @param loopCount �߳���ѭ����
 */
MemoryRandomReadTest::MemoryRandomReadTest(bool useMms, bool isVar, bool zipf, int threadCount, int loopCount) {
	m_useMms = useMms;
	m_isVar = isVar;
	m_zipf = zipf;
	m_nrThreads = threadCount;
	m_loopCount = loopCount;
	m_zipfRandom = NULL;
	m_topTenCount = new Atomic<int>();
}

void MemoryRandomReadTest::setUp() {
	m_threads = new MemoryRandomReadThread *[m_nrThreads];
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i] = new MemoryRandomReadThread(this, m_loopCount);
	// �����������ݻ���
	m_enableCache = true;
	// �ڴ��������
	m_isMemoryCase = true;
	// �������ݿ�����
	setConfig(CommonDbConfig::getMedium());
	// ������ģʽ
	setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : CountTable::getTableDef(m_useMms));
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE / 3;//MEM_RANDOM_READ_REDUCE_RATIO;
	if (m_useMms) {
		if (m_zipf)
			m_config->m_mmsSize = m_totalRecSizeLoaded / 10 / Limits::PAGE_SIZE;
		else
			m_config->m_mmsSize = m_totalRecSizeLoaded * MEM_RANDOM_READ_MMS_MULTIPLE / Limits::PAGE_SIZE;
	}
		
}

/** 
 * ����������
 *
 * @return ������
 */
string MemoryRandomReadTest::getName() const {
	stringstream ss;
	ss << "MemoryRandomReadTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_isVar)
		ss <<"varTable,";
	if (m_zipf)
		ss << "zipf";
	ss << ")";
	return ss.str();
}

/** 
 * ��������������Ϣ
 *
 * @return ������������
 */
string MemoryRandomReadTest::getDescription() const {
	return "Random Read in Memory Performance";
}

/**
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 *
 * @return ������
 */
string MemoryRandomReadTest::getCachedName() const {
	return string("MemoryRandomReadTest-") + m_tableDef->m_name;
}

/** 
 * �����������к���
 */
void MemoryRandomReadTest::run() {
	u64 ioBefore = m_db->getPageBuffer()->getStatus().m_physicalReads
		+ m_db->getPageBuffer()->getStatus().m_physicalWrites;

	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i]->start();

	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i]->join();

	m_opDataSize = m_nrThreads * m_loopCount * m_totalRecSizeLoaded / m_recCntLoaded;
	m_opCnt = m_nrThreads * m_loopCount;

	u64 ioAfter = m_db->getPageBuffer()->getStatus().m_physicalReads
		+ m_db->getPageBuffer()->getStatus().m_physicalWrites;
	NTSE_ASSERT(ioBefore == ioAfter);

//	cout << "Zipf Top 10% Counts: " << m_topTenCount->get() << endl;
//	if (m_tableDef->m_useMms) {
//		cout << "Mms Query Hits: " << m_db->getMms()->getStatus().m_recordQueryHits << endl;
//		cout << "Mms Query Counts: " << m_db->getMms()->getStatus().m_recordQueries << endl;
//	}
}

/** 
 * �����(�������̵߳���)
 *
 * @param count ѭ����
 */
void MemoryRandomReadTest::randomRead(int count) {
	// ����ZipfRandom����
	u64 id = 0;
	Connection *conn = m_db->getConnection(false);
	byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
	TblScan *scanHandle;
	bool scanSucc;

	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordById("0", &id);
	u16 columns[2] = {0, 1};
	u16 columnsVar[3] = {0, 1, 2};
	for (int i = 0; i < count; i++) {
		Session *session = m_db->getSessionManager()->allocSession("MemoryRandomReadTest::randomRead", conn);
		if (m_zipf) {
			assert(m_zipfRandom);
			id = m_zipfRandom->rand();
			if (id * 10 < m_recCntLoaded)
				m_topTenCount->increment();
		} else
			id = RandomGen::nextInt(0, m_recCntLoaded);
		if (m_isVar) {
			key = AccountTable::updateKey(key, id);
			key->m_rowId = INVALID_ROW_ID;
			IndexScanCond cond(0, key, true, true, false);
			scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columnsVar);
		} else {
			key = CountTable::updateKey(key, id);
			key->m_rowId = INVALID_ROW_ID;
			IndexScanCond cond(0, key, true, true, false);
			scanHandle = m_table->indexScan(session, OP_READ, &cond, 2, columns);
		}
		scanSucc = m_table->getNext(scanHandle, buf);
		assert(scanSucc);
		m_table->endScan(scanHandle);
		m_db->getSessionManager()->freeSession(session);
	}
	freeSubRecord(key);
	free(buf);
	m_db->freeConnection(conn);
}

/** 
 * ��������
 *
 * @param totalRecSize [OUT] �ܼ�¼��С
 * @param recCnt [OUT] ��¼����
 * @post ���ݿ�״̬Ϊ�ر�
 */
void MemoryRandomReadTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MemoryRandomReadTest::loadData", conn);
	
	if (m_isVar) { // �䳤��
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else { // ������
		m_recCntLoaded = CountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * Ԥ�ȣ�����һ���ɨ��
 * @post ���ݿ�״̬Ϊopen
 */
void MemoryRandomReadTest::warmUp() {
	openTable();
	if (m_tableDef->m_useMms && m_zipf)
		m_table->getMmsTable()->setMaxRecordCount(m_recCntLoaded / 10); // ��MMS����󻺴��¼������Ϊ���ؼ�¼����1/10
	m_db->getMms()->getPool()->setRebalanceEnabled(false); // �ر�PagePool��Rebalance����
	m_isVar ? AccountTable::scanTable(m_db, m_table) : CountTable::scanTable(m_db, m_table);
	indexFullScan(m_db, m_table);
	scanIndexOneTime();
	if (m_zipf)
		m_zipfRandom = ZipfRandom::createInstVaryScrew(m_recCntLoaded, false); // ����Prop(Top10%) = 90% ��Zipf�������
}

/** 
 * һ������ɨ��
 *
 * @return ɨ���¼����
 */
u64 MemoryRandomReadTest::scanIndexOneTime() {
	u16 columns[2] = {0, 1};
	u64 scanCnt = index0Scan(m_db, m_table, 2, columns);
	if (m_useMms)
		NTSE_ASSERT(m_table->getMmsTable()->getStatus().m_records == scanCnt);
	NTSE_ASSERT(scanCnt == m_recCntLoaded);
	return scanCnt;
}

/** 
 * ���������ر�
 */
void MemoryRandomReadTest::tearDown() {
	EmptyTestCase::tearDown();
	for (int i = 0; i < m_nrThreads; i++)
		delete m_threads[i];
	delete [] m_threads;
	delete m_topTenCount;
	if (m_zipfRandom)
		delete m_zipfRandom;
}

