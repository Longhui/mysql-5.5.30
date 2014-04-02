#include "DiskRandomRead.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include "Random.h"
#include <sstream>

using namespace ntseperf;
using namespace std;

u32 DiskRandomReadTest::VOLUMN_RATIO = 10;
/** 
 * ���캯��
 *
 * @param useMms ʹ��MMS
 * @param isVar �Ƿ�Ϊ�䳤��
 * @param threadCount �̸߳���
 * @param loopCount �߳���ѭ������
 */
DiskRandomReadTest::DiskRandomReadTest(bool useMms, bool isVar, int threadCount, int loopCount) {
	m_useMms = useMms;
	m_isVar = isVar;
	m_nrThreads = threadCount;
	m_loopCount = loopCount;
}

void DiskRandomReadTest::setUp() {
	m_threads = new DiskRandomReadThread *[m_nrThreads];
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i] = new DiskRandomReadThread(this, m_loopCount);
	setConfig(CommonDbConfig::getSmall());
	setTableDef(m_isVar ? AccountTable::getTableDef(m_useMms) : LongCharTable::getTableDef(m_useMms));
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * VOLUMN_RATIO;
}

/** 
 * ��ȡ����������
 *
 * @return ����������
 */
string DiskRandomReadTest::getName() const {
	stringstream ss;
	ss << "DiskRandomReadTest(";
	if (m_useMms)
		ss << "useMMs,";;
	if (m_isVar)
		ss <<"varTable";
	ss << ")";
	return ss.str();
}

/** 
 * ��������������Ϣ
 *
 * @return ������������
 */
string DiskRandomReadTest::getDescription() const {
	return "Random Read on Disk I/O Performance";
}

/**
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 * 
 * @return ������
 */
string DiskRandomReadTest::getCachedName() const {
	return string("DiskRandomReadTest-") + m_tableDef->m_name;
}

/** 
 * �����������к���
 */
void DiskRandomReadTest::run() {
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i]->start();
	for(int i = 0; i < m_nrThreads; i++)
		m_threads[i]->join();

	m_opDataSize = m_nrThreads * m_loopCount * m_totalRecSizeLoaded / m_recCntLoaded;
	m_opCnt = m_nrThreads * m_loopCount;
}

/**
 * �����
 *
 * @param count ѭ������
 */
void DiskRandomReadTest::randomRead(int count) {
	u64 id = 0;
	Connection *conn = m_db->getConnection(false);
	Session *session;
	byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
	TblScan *scanHandle;
	bool scanSucc;

	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordById("0", &id);
	u16 columns[2] = {0, 1};
	u16 columnsVar[3] = {0, 1, 2};
	key->m_rowId = INVALID_ROW_ID;
	for (int i = 0; i < count; i++) {
		session = m_db->getSessionManager()->allocSession("DiskRandomReadTest::randomRead", conn);
		id = RandomGen::nextInt(0, m_recCntLoaded);
		if (m_isVar) {
			key = AccountTable::updateKey(key, id);
			IndexScanCond cond(0, key, true, true, false);
			scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columnsVar);
		} else {
			key = LongCharTable::updateKey(key, id);
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
void DiskRandomReadTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	// �������ݿ�
	openTable(true);
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DiskRandomReadTest::loadData", conn);
	
	if (m_isVar) { // �䳤��
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else { // ������
		m_recCntLoaded = LongCharTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * Ԥ�ȣ�����һ������ɨ��
 * @post ���ݿ�״̬Ϊopen
 */
void DiskRandomReadTest::warmUp() {
	openTable(false);
	scanIndexOneTime();
}

/** 
 * һ������ɨ��
 */
void DiskRandomReadTest::scanIndexOneTime() {
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MemoryIndexScanTest::scanIndexOneTime", conn);
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	u16 columns[2] = {0, 1};
	byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
	IndexScanCond cond(0, NULL, true, true, false);
	TblScan *scanHandle = m_table->indexScan(session, OP_READ, &cond, 2, columns);
	for (uint n = 0; n < m_opCnt; n++) {
		m_table->getNext(scanHandle, buf);
		assert(m_table->getMmsTable()->getStatus().m_records == n + 1);
	}
	m_table->endScan(scanHandle);
	free(buf);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	
}

/** 
 * �رղ�������
 */
void DiskRandomReadTest::tearDown() {
	EmptyTestCase::tearDown();
	for (int i = 0; i < m_nrThreads; i++)
		delete m_threads[i];
	delete [] m_threads;
}

