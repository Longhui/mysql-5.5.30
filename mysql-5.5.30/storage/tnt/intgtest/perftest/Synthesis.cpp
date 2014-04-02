#include "Synthesis.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "BlogTable.h"
#include "AccountTable.h"
#include "LongCharTable.h"
#include "Random.h"
#include <sstream>

using namespace ntseperf;
using namespace std;

/** 
 * ���캯��
 *
 * @param isFixLen �Ƿ�Ϊ������
 * @param useBlob ���Ա����Ƿ���ڴ����
 * @param threadCount �̸߳���
 */
SynthesisTest::SynthesisTest(bool isFixLen, bool useBlob, int threadCount, int taskCount) {
	m_isFixLen = isFixLen;
	m_useBlob = useBlob;
	m_nrThreads = threadCount;
	m_taskCount = taskCount;
}

void SynthesisTest::setUp() {
	m_threads = new SynthesisThread *[m_nrThreads];
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i] = new SynthesisThread(this, m_isFixLen, m_useBlob);
	setConfig(CommonDbConfig::getMedium());
	if (m_isFixLen)
		setTableDef(LongCharTable::getTableDef(true));
	else
		setTableDef(m_useBlob ? BlogTable::getTableDef(true) : AccountTable::getTableDef(true));
	m_totalRecSizeLoaded = m_config->m_pageBufSize * Limits::PAGE_SIZE * SYNTHESIS_VOLUMN_RATIO;
	m_opDataSize = m_totalRecSizeLoaded; // ���������� = ��������

	m_maxId = 1 << 20;  // TODO: ���ⲿ�ṩ��
}

/** 
 * ����������
 * 
 * @return ����������
 */
string SynthesisTest::getName() const {
	stringstream ss;
	ss << "SynthesisTest(";
	if (m_isFixLen)
		ss << "isFixLen, ";
	if (m_useBlob)
		ss <<"useBlob";
	ss << ")";
	return ss.str();
}

/** 
 * ��������������Ϣ
 *
 * @return ������������
 */
string SynthesisTest::getDescription() const {
	return "Synthesis Performance";
}

/**
 * �������ݻ�����(Ĭ���ǲ���������)
 *	��ͬ�����Ĳ�������ͬ�Ĳ�������
 * 
 * @return ������
 */
string SynthesisTest::getCachedName() const {
	return string("SynthesisTest-") + m_tableDef->m_name;
}

/** 
 * �����������к���
 */
void SynthesisTest::run() {
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i]->start();
	for (int i = 0; i < m_nrThreads; i++)
		m_threads[i]->join();
}

/**
 * LongCharTable����ʵ��
 */
void SynthesisTest::testLongCharTable() {
	Connection *conn = m_db->getConnection(false);
	Session *session;
	byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
	memset(buf, 0, m_table->getTableDef()->m_maxRecSize);
	TblScan *scanHandle;
	bool insertSucc, readSucc, updateSucc, deleteSucc;
	u64 id = 0;
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName(LONGCHAR_ID, &id);
	Record *rec = LongCharTable::createRecord(0);

	for (int taskNum = 0; taskNum < m_taskCount; taskNum++) {
		for (int loop = 0; loop < 15; loop++) {
			session = m_db->getSessionManager()->allocSession("SynthesisTest::testLongCharTable", conn);
			int k = RandomGen::nextInt(0, m_maxId);
			if (loop < 2) { // ���������¼
				rec = LongCharTable::updateId(rec, k);
				insertSucc = LongCharTable::insertRecord(session, m_table, rec);
			} else if (loop < 12) { // ��ȡ�����¼
				key = LongCharTable::updateKey(key, k);
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[2] = {LONGCHAR_ID_CNO, LONGCHAR_NAME_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_READ, &cond, 2, columns);
				readSucc = m_table->getNext(scanHandle, buf);
				m_table->endScan(scanHandle);
			} else if (loop < 14) { // ���������¼
				key = LongCharTable::updateKey(key, k);	
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[2] = {LONGCHAR_ID_CNO, LONGCHAR_NAME_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, 2, columns);
				scanHandle->setUpdateColumns(2, columns);
				updateSucc = m_table->getNext(scanHandle, buf);
				if (updateSucc) {
					rec = LongCharTable::updateId(rec, k);
					m_table->updateCurrent(scanHandle, rec->m_data);
				}
				m_table->endScan(scanHandle);
			} else { // ɾ�������¼
				key = LongCharTable::updateKey(key, k);
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[2] = {LONGCHAR_ID_CNO, LONGCHAR_NAME_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_DELETE, &cond, 2, columns);
				deleteSucc = m_table->getNext(scanHandle, buf);
				if (deleteSucc)
					m_table->deleteCurrent(scanHandle);
				m_table->endScan(scanHandle);
			}
			m_db->getSessionManager()->freeSession(session);
		}
	}
	freeRecord(rec);
	freeSubRecord(key);
	free(buf);
	m_db->freeConnection(conn);
}

/**
* AccountTable����ʵ��
*/
void SynthesisTest::testAccountTable() {
	Connection *conn = m_db->getConnection(false);
	Session *session;
	byte *buf = (byte *)malloc(m_table->getTableDef()->m_maxRecSize);
	memset(buf, 0, m_table->getTableDef()->m_maxRecSize);
	TblScan *scanHandle;
	bool insertSucc, readSucc, updateSucc, deleteSucc;
	u64 id = 0;
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordByName(ACCOUNT_ID, &id);
	Record *rec = AccountTable::createRecord(0);

	for (int taskNum = 0; taskNum < m_taskCount; taskNum++) {
		for (int loop = 0; loop < 15; loop++) {
			session = m_db->getSessionManager()->allocSession("SynthesisTest::testAccountTable", conn);
			int k = RandomGen::nextInt(0, m_maxId);
			if (loop < 2) { // ���������¼
				rec = AccountTable::updateRecord(rec, k);
				insertSucc = AccountTable::insertRecord(session, m_table, rec);	
			} else if (loop < 12) { // ��ȡ�����¼
				key = AccountTable::updateKey(key, k);				
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[3] = {ACCOUNT_ID_CNO, ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_READ, &cond, 3, columns);
				readSucc = m_table->getNext(scanHandle, buf);
				m_table->endScan(scanHandle);
			} else if (loop < 14) { // ���������¼
				key = AccountTable::updateKey(key, k);	
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[3] = {ACCOUNT_ID_CNO, ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, 3, columns);
				scanHandle->setUpdateColumns(3, columns);
				updateSucc = m_table->getNext(scanHandle, buf);
				if (updateSucc) {
					rec = AccountTable::updateRecord(rec, k);
					m_table->updateCurrent(scanHandle, rec->m_data);
				}
				m_table->endScan(scanHandle);
			} else { // ɾ�������¼
				key = AccountTable::updateKey(key, k);	
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[3] = {ACCOUNT_ID_CNO, ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_DELETE, &cond, 3, columns);
				deleteSucc = m_table->getNext(scanHandle, buf);
				if (deleteSucc)
					m_table->deleteCurrent(scanHandle);
				m_table->endScan(scanHandle);
			}
			m_db->getSessionManager()->freeSession(session);
		}
	}
	freeRecord(rec);
	freeSubRecord(key);
	free(buf);
	m_db->freeConnection(conn);
}

/**
* BlogTable����ʵ��
*/
void SynthesisTest::testBlogTable() {
	Connection *conn = m_db->getConnection(false);
	Session *session;
	byte *buf = new byte[m_table->getTableDef()->m_maxRecSize];
	memset(buf, 0, sizeof(m_table->getTableDef()->m_maxRecSize));
	TblScan *scanHandle = NULL;
	bool insertSucc, readSucc, fullReadSucc, updateSucc, deleteSucc;
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	u64 id = 0;
	SubRecord *key = keyBuilder.createSubRecordByName(BLOG_ID, &id);
	RedRecord *rRecord = new RedRecord(BlogTable::getTableDef(false));
	// RedRecord *rRecord = BlogTable::createRedRecord(0);
	int blbMaxSize = 32 * 1024;
	char *blbBuffer = new char[blbMaxSize];
	for (int i = 0; i < blbMaxSize; i++)
		*(blbBuffer + i) = (char)(RandomGen::nextInt(0, 100) + 'a');
	//memset(blbBuffer, 0, blbMaxSize);

	for (int taskNum = 0; taskNum < m_taskCount; taskNum++) {
		for (int loop = 0; loop < 15; loop++) {
			session = m_db->getSessionManager()->allocSession("SynthesisTest::testBlogTable", conn);
			int k = RandomGen::nextInt(0, m_maxId);
			if (loop >= 0 && loop < 2) { // ���������¼
				insertSucc = BlogTable::insertRecord(session, m_table, k, blbBuffer, rRecord);	
			} else if (loop < 11) { // ��
				BlogTable::updateKey(key, k);
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[5] = {BLOG_ID_CNO, BLOG_USERID_CNO, BLOG_PUBLISHTIME_CNO, BLOG_TITLE_CNO, BLOG_PERMALINK_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_READ, &cond, 5, columns);
				readSucc = m_table->getNext(scanHandle, buf);
				m_table->endScan(scanHandle);
			} else if (loop < 12) {
				BlogTable::updateKey(key, k);
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[7] = {BLOG_ID_CNO, BLOG_USERID_CNO, BLOG_PUBLISHTIME_CNO, 
					BLOG_TITLE_CNO, BLOG_ABSTRACT_CNO, BLOG_CONTENT_CNO, BLOG_PERMALINK_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, 7, columns);
				fullReadSucc = m_table->getNext(scanHandle, buf);
				// ������ֶ�
				if (fullReadSucc) {
					// abstract�ֶ�
					uint lobSize = RecordOper::readLobSize(buf, m_table->getTableDef()->m_columns[BLOG_ABSTRACT_CNO]);
					byte *lob = RecordOper::readLob(buf, m_table->getTableDef()->m_columns[BLOG_ABSTRACT_CNO]);
					// content�ֶ�
					lobSize = RecordOper::readLobSize(buf, m_table->getTableDef()->m_columns[BLOG_CONTENT_CNO]);
					lob = RecordOper::readLob(buf, m_table->getTableDef()->m_columns[BLOG_CONTENT_CNO]);
				}
				m_table->endScan(scanHandle);
			} else if (loop < 14) { 
				BlogTable::updateKey(key, k);
				key->m_rowId = INVALID_ROW_ID;
				IndexScanCond cond(0, key, true, true, false);

				if (RandomGen::tossCoin(10)) {
					u16 columns[1] = {BLOG_CONTENT_CNO};
					scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, 1, columns);
					scanHandle->setUpdateColumns(1, columns);
					updateSucc = m_table->getNext(scanHandle, buf);
					if (updateSucc) {
						rRecord->writeLob(BLOG_CONTENT_CNO, (byte *)blbBuffer, RandomGen::randNorm((int)BlogTable::DEFAULT_AVG_REC_SIZE, (int)BlogTable::DEFAULT_MIN_REC_SIZE));
						m_table->updateCurrent(scanHandle, rRecord->getRecord()->m_data);
					}
				} else {
					u16 columns[1] = {BLOG_PUBLISHTIME_CNO};
					scanHandle = m_table->indexScan(session, OP_UPDATE, &cond, 1, columns);
					scanHandle->setUpdateColumns(1, columns);
					updateSucc = m_table->getNext(scanHandle, buf);
					if (updateSucc) {
						rRecord->writeNumber(BLOG_PUBLISHTIME_CNO, System::currentTimeMillis());
						m_table->updateCurrent(scanHandle, rRecord->getRecord()->m_data);
					}
				}
				m_table->endScan(scanHandle);
			} else {
				BlogTable::updateKey(key, k);
				key->m_rowId = INVALID_ROW_ID;
				u16 columns[1] = {BLOG_ID_CNO};
				IndexScanCond cond(0, key, true, true, false);
				scanHandle = m_table->indexScan(session, OP_DELETE, &cond, 1, columns);
				deleteSucc = m_table->getNext(scanHandle, buf);
				if (deleteSucc)
					m_table->deleteCurrent(scanHandle);
				m_table->endScan(scanHandle);
			}
			m_db->getSessionManager()->freeSession(session);
		}
	}
	freeSubRecord(key);
	delete [] buf;
	delete [] blbBuffer;
	rRecord->writeLob(BLOG_CONTENT_CNO, NULL, 0); // ɾ��RedRecordǰ�����ô�����ֶ�ΪNULL
	delete rRecord;

	m_db->freeConnection(conn);
}

/** 
 * ��������
 *
 * @param totalRecSize [OUT] �ܼ�¼��С
 * @param recCnt [OUT] ��¼����
 * @post ���ݿ�״̬Ϊ�ر�
 */
void SynthesisTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SynthesisTest::loadData", conn);
	
	if (m_isFixLen)
		m_recCntLoaded = LongCharTable::populate(session, m_table, &m_totalRecSizeLoaded);
	else if (m_useBlob) { // ������
		m_recCntLoaded = BlogTable::populate(session, m_table, &m_totalRecSizeLoaded);
	} else { // �Ǵ�����
		m_recCntLoaded = AccountTable::populate(session, m_table, &m_totalRecSizeLoaded);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
}

/** 
 * ����һ������ɨ��
 * @post ���ݿ�״̬Ϊopen
 */
void SynthesisTest::warmUp() {
	openTable(false);
	scanIndexOneTime();
}

/** 
 * һ������ɨ��
 */
void SynthesisTest::scanIndexOneTime() {
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SynthesisTest::scanIndexOneTime", conn);
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	SubRecord *key = keyBuilder.createSubRecordById("");
	key->m_rowId = INVALID_ROW_ID;
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
	freeSubRecord(key);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	
}

/**
 * �رղ�������
 */
void SynthesisTest::tearDown() {
	EmptyTestCase::tearDown();
	for (int i = 0; i < m_nrThreads; i++)
		delete m_threads[i];
	delete [] m_threads;
}

/** 
 * �ۺϲ����߳����к���
 */
void SynthesisThread::run() {
	if (m_isFixLen)
		m_testCase->testLongCharTable();
	else if (m_useBlob)
		m_testCase->testBlogTable();
	else
		m_testCase->testAccountTable();
}

