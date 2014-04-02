/**
* MMS�ȶ��Բ���
*
* @author �۷�(shaofeng@corp.netease.com, sf@163.org)
*/

#include <iostream>
#include "TestSblMms.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "Random.h"
#include "AccountTable.h"
#include "CountTable.h"
#include "util/File.h"
#include "btree/IndexBPTree.h"
#include "heap/Heap.h"

using namespace std;

// �������Ȩ��
const uint MmsSblTestCase::SELECT_WEIGHT = 33;
const uint MmsSblTestCase::DELETE_WEIGHT = 33;
//const uint MmsSblTestCase::GET_BY_RID_WEIGHT = 50;
const uint MmsSblTestCase::UPDATE_WEIGHT_WHEN_SELECT = 90;
const uint MmsSblTestCase::INSERT_MMS_WEIGHT_WHEN_SELECT = 90;
//const uint MmsSblTestCase::PRIMARY_KEY_UPDATE_WEIGHT = 50;  // ʵ��Ӧ�ò��ṩ��������

// �����ݹ�ģ
const uint MmsSblTestCase::MAX_REC_COUNT_IN_TABLE = 1024 * 100;
const uint MmsSblTestCase::TABLE_NUM_IN_SAME_TEMPLATE = 5;

// ���Ի�����Ϣ
const uint MmsSblTestCase::WORKING_THREAD_NUM = 100;
const uint MmsSblTestCase::VERIFY_DURATION = 5 * 60 * 1000;
//const uint MmsSblTestCase::CHECKPOINT_DURATION = 120 * 1000;
const uint MmsSblTestCase::FOREVER_DURATION = (uint) -1;
const uint MmsSblTestCase::RUN_DURATION = 10 * 60 * 1000;

/** 
 * ��ȡ������
 *
 * @return ������
 */
const char* MmsSblTestCase::getName() {
	return "Mms Stability Test";
}

/** 
 * ��ȡ��������
 *
 * @return ��������
 */
const char* MmsSblTestCase::getDescription() {
	return "Stability Test for Mms Module";
}

/**
 * �Ƿ�Ϊ���������
 *
 * @return �Ƿ�Ϊ���������
 */
bool MmsSblTestCase::isBig() {
	return true;
}

/** 
 * ��ʼ
 * ����:	1. ɾ�������ݿ⣬���������ݿ�
 *			2. �������Ŷ�����Ͷ��ű䳤��
 *			3. ������֤�̡߳������̺߳���־���
 */
void MmsSblTestCase::setUp() {
	// ���ݳ�ʼ��
	m_cfg = new Config();
	Database::drop(m_cfg->m_basedir);
	File dir(m_cfg->m_basedir);
	dir.mkdir();
	NTSE_ASSERT(m_db = Database::open(m_cfg, true));
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::MmsSblTestCase", conn);

	int tableCount = TABLE_NUM_IN_SAME_TEMPLATE;
	m_numTables = 2 * tableCount;
	m_mms = m_db->getMms();  // ȫ��MMS��m_db�ṩ�����С��cfgָ��

	m_mmsTables = new MmsTable* [m_numTables];
	m_heaps = new DrsHeap* [m_numTables];
	m_tables = new Table* [m_numTables];
	m_memHeaps = new MemHeap* [m_numTables];
	m_tableDefs = new TableDef* [m_numTables];

	stringstream name;
	for (int i = 0; i < tableCount; i++) { // Count��
		name.str("");
		name << i;
		try {
			string basePath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + name.str().c_str();
			string fullPath = basePath + Limits::NAME_HEAP_EXT;
			DrsHeap::drop(fullPath.c_str());
			fullPath = basePath + Limits::NAME_IDX_EXT;
			DrsIndice::drop(fullPath.c_str());
		} catch(NtseException ex) {
		}
		m_tableDefs[i] = new TableDef(CountTable::getTableDef(true));
		m_db->createTable(session, name.str().c_str(), m_tableDefs[i]);
		m_tables[i] = m_db->openTable(session, name.str().c_str());
		m_heaps[i] = m_tables[i]->getHeap();
		m_mmsTables[i] = m_tables[i]->getMmsTable();
		m_memHeaps[i] = new MemHeap(MAX_REC_COUNT_IN_TABLE, m_heaps[i]->getTableDef());
	}
	for (int i = tableCount; i < 2 * tableCount; i++) {
		name.str("");
		name << i;
		try {
			string basePath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + name.str().c_str();
			string fullPath = basePath + Limits::NAME_HEAP_EXT;
			DrsHeap::drop(fullPath.c_str());
			fullPath = basePath + Limits::NAME_IDX_EXT;
			DrsIndice::drop(fullPath.c_str());
		} catch(NtseException ex) {
		}
		m_tableDefs[i] = new TableDef(AccountTable::getTableDef(true));
		m_db->createTable(session, name.str().c_str(), m_tableDefs[i]);
		m_tables[i] = m_db->openTable(session, name.str().c_str());
		m_heaps[i] = m_tables[i]->getHeap();
		m_mmsTables[i] = m_tables[i]->getMmsTable();
		m_memHeaps[i] = new MemHeap(MAX_REC_COUNT_IN_TABLE, m_heaps[i]->getTableDef());
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_numVerify = 0;
	m_numSelect = 0;
	m_numDelete = 0;
	m_numInsert = 0;
	m_numUpdate = 0;
	m_numPreSelect = 0;
	m_numPreDelete = 0;
	m_numPreInsert = 0;
	m_numPreUpdate = 0;

	m_mmsRecordQueries = 0;				
	m_mmsRecordQueryHits = 0;
	m_mmsRecordInserts = 0;				
	m_mmsRecordDeletes = 0;		
	m_mmsRecordUpdates = 0;	
	m_mmsRecordVictims = 0;    
	m_mmsPageVictims = 0;		
	m_mmsOccupiedPages = 0;	
	m_mmsFreePages = 0;			

	// �̳߳�ʼ��
	// m_timerThread = new MmsSblTimer(this, CHECKPOINT_DURATION);
	m_verifierThread = new MmsSblVerifier(this, VERIFY_DURATION);
	m_nrWorkThreads = WORKING_THREAD_NUM;
	m_workerThreads = new MmsSblWorker *[m_nrWorkThreads];
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsSblWorker(this);

	m_resLogger = new ResLogger(m_db, 1800, "[Mms]DbResource.txt");
}

/** 
 * ����
 * ���裺	1. ɾ����־��ء�ֹͣ�����̺߳���֤�߳�
 *			2. ɾ����֤�̺߳͹����߳�ʵ��
 *			3. �رղ�ɾ������ڴ��
 *			4. ɾ�����ݿ⼰������
 */
void MmsSblTestCase::tearDown() {
	delete m_resLogger;

	stopThreads();
	m_verifierThread->stop();
	m_verifierThread->join();
	
	//delete m_timerThread;
	//m_timerThread = NULL;
	delete m_verifierThread;
	m_verifierThread = NULL;
	for (int i = 0; i < m_nrWorkThreads; i++)
		delete m_workerThreads[i];
	delete [] m_workerThreads;
	m_workerThreads = NULL;

	// �ͷ���Դ
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::~MmsSblTestCase", conn);
	stringstream name;
	for (int i = 0; i < m_numTables; i++) {
		name.str("");
		name << i;
		delete m_memHeaps[i];
		//m_mmsTables[i]->close(session, true); // ����Ҫ����Table::close���
		//delete m_mmsTables[i];
		//m_heaps[i]->close(session, true);
		//delete m_heaps[i];
		m_db->closeTable(session, m_tables[i]);
		m_db->dropTable(session, name.str().c_str());
		delete m_tableDefs[i];
	}
	delete [] m_memHeaps;
	delete [] m_mmsTables;
	delete [] m_heaps;
	delete [] m_tables;
	delete [] m_tableDefs;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_db->close();
	delete m_db;

	delete m_cfg;
}

/** 
 * MMS�ȶ�����������ں���
 * ���裺	1. ���������̺߳���֤�߳�
 *			2. ���߳�ѭ���ȴ�
 */
void MmsSblTestCase::testMmsStability() {
	// �߳�����
	startThreads();
	m_verifierThread->start();
	
	if (RUN_DURATION == FOREVER_DURATION) { // ��Զ��ֹͣ!
		while(true)
			Thread::msleep(10000000);
	} else {
		Thread::msleep(RUN_DURATION);
		m_verifierThread->stop();
		m_verifierThread->join();
		stopThreads();
	}
}

/** 
 * �������й����߳�
 */
void MmsSblTestCase::startThreads() {
	this->m_beginTime = System::fastTime();
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i]->start();
}

/** 
 * ֹͣ���й����߳� 
 */
void MmsSblTestCase::stopThreads() {
	for (int i = 0; i < m_nrWorkThreads; i++) {
		m_workerThreads[i]->stop();
		m_workerThreads[i]->join();
	}
	this->m_endTime = System::fastTime();
}

/** 
 * ʵ�ʹ������� 
 */
void MmsSblTestCase::doWork() {
	switch(getTaskType()) {
		case WORK_TASK_TYPE_INSERT:
			doInsert();
			break;
		case WORK_TASK_TYPE_DELETE:
			doDelete();
			break;
		case WORK_TASK_TYPE_SELECT:
			doSelect();
			break;
		default:
			assert(false); // �����ܳ���
	}
}

/** 
 * ��֤����
 * ����:	1. ֹͣ���й����߳�
 *			2. ��ѯÿ���ڴ�ѵ�ÿ����¼�����ÿ���ǿռ�¼�ִ�����²���
 *			   2.1 ��ȡ��¼���RowID
 *			   2.2 ����RowID����ѯ��Ӧ��MMS�������ȡ��MMS��¼��Ϊ�գ�
 *				   ��Ƚ�MMS��¼���ݺ��ڴ�������Ƿ�һ��
 *			3. �ж�MMS�����жϼ�¼�������MMS�������м�¼������Ƿ�һ��
 *			4. ͳ����Ϣ��ӡ���������й����߳�
 */
void MmsSblTestCase::doVerify() {
	// ��ͣ���й����߳�
	stopThreads();
	for (int i = 0; i < m_nrWorkThreads; i++)
		delete m_workerThreads[i];

	RowLockHandle *rlh = NULL;
	RowId rowId;
	int	verifiedNrRecords;
	MmsRecord *mmsRecord;
	Record record;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::doVerify", conn);
	u64 nrVictims = 0;

	for (int i = 0; i < m_numTables; i++) {
		verifiedNrRecords = 0;
		record.m_size = m_heaps[i]->getTableDef()->m_maxRecSize;
		record.m_data = new byte [record.m_size];
		unsigned maxSize = m_memHeaps[i]->getMaxRecCount();
		for (MemHeapRid j = 0; j < maxSize; j++) {
			MemHeapRecord *mhRecord = m_memHeaps[i]->recordAt(session, j, &rlh, Shared);
			if (mhRecord) {
				rowId = mhRecord->getRowId();
				mmsRecord = m_mmsTables[i]->getByRid(session, rowId, false, NULL, None);
				if (mmsRecord) {
					m_mmsTables[i]->getRecord(mmsRecord, &record);
					if (!mhRecord->compare(session, &record)) // ������¼��֤��ȷ��
						assert(false);
					verifiedNrRecords++;
					m_mmsTables[i]->unpinRecord(session, mmsRecord);
				}
				session->unlockRow(&rlh);
			}
		}
		if (m_mmsTables[i]->getStatus().m_records != verifiedNrRecords) // ����֤��¼����MMS�м�¼����һ��
			assert(false);
		delete [] record.m_data;
		nrVictims += m_mmsTables[i]->getStatus().m_pageVictims + m_mmsTables[i]->getStatus().m_recordVictims;
	}

	const MmsStatus& mmsStats = m_db->getMms()->getStatus();
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// ͳ����Ϣ��ӡ
	cout << "---------------- TEST STATS ------------------------------------" << endl;
	cout << "Verify Count: " << m_numVerify << endl;
	cout << "Insert Count: " << m_numInsert << endl;
	cout << "Insert Performance (Count/Sec): " << (m_numInsert - m_numPreInsert)  / (m_endTime - m_beginTime) << endl;
	cout << "Delete Count: " << m_numDelete << endl;
	cout << "Delete Performance (Count/Sec): " << (m_numDelete - m_numPreDelete)  / (m_endTime - m_beginTime) << endl;
	cout << "Select Count: " << m_numSelect << endl;
	cout << "Select Performance (Count/Sec): " << (m_numSelect - m_numPreSelect)  / (m_endTime - m_beginTime) << endl;
	cout << "Update Count: " << m_numUpdate << endl;
	cout << "Update Performance (Count/Sec): " << (m_numUpdate - m_numPreUpdate)  / (m_endTime - m_beginTime) << endl;
	cout << "Total Performance(Count/Sec) " << (m_numInsert - m_numPreInsert + m_numDelete - m_numPreDelete + m_numSelect - m_numPreSelect) / (m_endTime - m_beginTime) << endl;

	m_numPreInsert = m_numInsert;
	m_numPreDelete = m_numDelete;
	m_numPreSelect = m_numSelect;
	m_numPreUpdate = m_numUpdate;

	// MMS ���ͳ��
	cout << "---------------- MMS STATS ------------------------------------" << endl;
	cout << " MMS Query Count: " << mmsStats.m_recordQueries << endl;
	cout << " MMS Query Performance: " << (mmsStats.m_recordQueries - m_mmsRecordQueries) / (m_endTime - m_beginTime) << endl;
	cout << " MMS Query Count Hit: " << mmsStats.m_recordQueryHits << endl;
	cout << " MMS Insert Count: " << mmsStats.m_recordInserts << endl;
	cout << " MMS Insert Performance: " << (mmsStats.m_recordInserts - m_mmsRecordInserts) / (m_endTime - m_beginTime) << endl;
	cout << " MMS Delete Count: " << mmsStats.m_recordDeletes << endl;
	cout << " MMS Delete Performance: " << (mmsStats.m_recordDeletes - m_mmsRecordDeletes) / (m_endTime - m_beginTime) << endl;
	cout << " MMS Update Count: " << mmsStats.m_recordUpdates << endl;
	cout << " MMS Update Performance: " << (mmsStats.m_recordUpdates - m_mmsRecordUpdates) / (m_endTime - m_beginTime) << endl;
	cout << " MMS Victim Count: " << mmsStats.m_recordVictims << endl;
	cout << " MMS Victim Performance: " << (mmsStats.m_recordVictims - m_mmsRecordVictims) / (m_endTime - m_beginTime) << endl;
	cout << " MMS PageVm Count: " << mmsStats.m_pageVictims << endl;
	cout << " MMS PageVm Performance: " << (mmsStats.m_pageVictims - m_mmsPageVictims) / (m_endTime - m_beginTime) << endl;
	cout << " MMS Pages: " << mmsStats.m_occupiedPages << endl;

	m_mmsRecordQueries = mmsStats.m_recordQueries;
	m_mmsRecordQueryHits = mmsStats.m_recordQueryHits;
	m_mmsRecordInserts = mmsStats.m_recordInserts;
	m_mmsRecordDeletes = mmsStats.m_recordDeletes;
	m_mmsRecordUpdates = mmsStats.m_recordUpdates;
	m_mmsRecordVictims = mmsStats.m_recordVictims;
	m_mmsPageVictims = mmsStats.m_pageVictims;


	// ��ӡ����Ϣ
	RWLockUsage::printAll(cout);
	//for (int i = 0; i < m_numTables; i++) {
	//	cout << "MmsTable " << i << " Table Lock " << endl;
	//	m_mmsTables[i]->getTableLock().getUsage()->printAll(cout); // ��ӡ������Ϣ
	//	int nr;
	//	RWLock **locks = m_mmsTables[i]->getPartLocks(&nr);
	//	for (int j = 0; j < nr; j++) {
	//		cout << "MmsTable " << i << " Partition Lock " << j << endl;
	//		locks[j]->getUsage()->printAll(cout);
	//	}
	//}
	
	m_numVerify++;

	// �ָ����й����߳�
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsSblWorker(this);
	startThreads();
}

/** 
 * ����һ����¼
 * ���裺	1. ���ѡ��Ŀ�������
 *			2. ѡ���ڴ�ѿ��вۣ������ݲۺŲ�����¼����
 *			3. ������Ӧ��¼
 *			4. ��Ӽ�¼��Drs�ѣ�������RowID
 *			5. ����RowID, ��Ӽ�¼���ڴ�Ѻ�MMS��
 */
void MmsSblTestCase::doInsert() {
	RowId rowId;
	u64	id;
	Record *record;
	Record *record2;
	MmsRecord *mmsRecord;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::doInsert", conn, 0);
	if (!session) { // ��ȡSESSIONʧ��
		m_db->freeConnection(conn);
		return;
	}
	RowLockHandle *rlh = NULL;
	int k = RandomGen::nextInt(0, m_numTables); // ���ѡ�������

	id = (u64)m_memHeaps[k]->reserveRecord();   // �������ڴ�Ѳۺž���
	if (id == MemHeapRecord::INVALID_ID) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		return;
	}
	if (m_heaps[k]->getTableDef()->m_recFormat == REC_FIXLEN) {
		record = CountTable::createRecord(id, RandomGen::nextInt());
		record->m_format = REC_FIXLEN;
	} else {
		assert(m_heaps[k]->getTableDef()->m_recFormat == REC_VARLEN);
		record2 = AccountTable::createRecord(id);
		record = new Record();
		record->m_size = m_heaps[k]->getTableDef()->m_maxRecSize;
		record->m_data = new byte[record->m_size];
		record->m_format = REC_VARLEN;
		RecordOper::convertRecordRV(m_heaps[k]->getTableDef(), record2, record);
	}
	rowId = m_heaps[k]->insert(session, record, &rlh);  // ����DRS��, ��¼Ϊ�䳤/������ʽ
	assert(rowId != INVALID_ROW_ID);
	record->m_rowId = rowId;
	if (rowId != INVALID_ROW_ID) {
		// �����ڴ�ѣ���¼Ϊ�����ʽ
		if (m_heaps[k]->getTableDef()->m_recFormat == REC_FIXLEN)
			NTSE_ASSERT(m_memHeaps[k]->insertAt(session, (MemHeapRid)id, rowId, record->m_data)); 
		else
			NTSE_ASSERT(m_memHeaps[k]->insertAt(session, (MemHeapRid)id, rowId, record2->m_data)); 
		mmsRecord = m_mmsTables[k]->putIfNotExist(session, record); // ����MMS��
		if (mmsRecord) {
			m_mmsTables[k]->unpinRecord(session, mmsRecord);
			session->unlockRow(&rlh);
		} else {
			m_heaps[k]->del(session, rowId); // ��DRS����ɾ����¼
			m_memHeaps[k]->deleteRecord(session, (MemHeapRid)id); // ���ڴ����ɾ����¼
			session->unlockRow(&rlh);
			goto end;
		}
	} else
		assert(false);
	m_numInsert++;
end:
	if (m_heaps[k]->getTableDef()->m_recFormat == REC_FIXLEN) {
		freeRecord(record);
	} else {
		assert(m_heaps[k]->getTableDef()->m_recFormat == REC_VARLEN);
		delete [] record->m_data;
		delete record;
		freeRecord(record2);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
 * ɾ��һ����¼
 * ���裺	1. ���ѡ�������
 *			2. �ڴ���л�ȡ�����¼��
 *			3. ��MMS���в�ѯ�ü�¼��(����RowID),���������ɾ���ü�¼��
 *			4. ɾ��DRS���ϵļ�¼��
 */
void MmsSblTestCase::doDelete() {
	RowId rowId;
	MmsRecord *mmsRecord;
	MemHeapRid mhRid;
	MemHeapRecord *mhRecord;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::doDelete", conn, 0);
	if (!session) { // ��ȡSESSIONʧ��
		m_db->freeConnection(conn);
		return;
	}
	RowLockHandle *rlh = NULL;
	int k = RandomGen::nextInt(0, m_numTables); // ѡ���

	mhRecord = m_memHeaps[k]->getRandRecord(session, &rlh, Exclusived);
	if (mhRecord) {
		rowId = mhRecord->getRowId();
		mhRid = mhRecord->getId();
		mmsRecord = m_mmsTables[k]->getByRid(session, rowId, false, NULL, None);
		if (mmsRecord)
			m_mmsTables[k]->del(session, mmsRecord); // ��MMS��ɾ����¼
		NTSE_ASSERT(m_heaps[k]->del(session, rowId)); // ��DRS����ɾ����¼
		NTSE_ASSERT(m_memHeaps[k]->deleteRecord(session, mhRid)); // ���ڴ����ɾ����¼
		session->unlockRow(&rlh);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_numDelete++;
}

/** 
 * ִ��Select����
 * ���裺	1. ���ѡ��������MMS���ѯ����(������ѯ��RowID��ѯ)
 *			2. ���ڴ�������ȡһ����¼��,����ѯMMS��
 *			3. �����¼��MMS����
 *				3.1. �Ƚϼ�¼��MMS���ڴ���е�һ����
 *				3.2. ���ѡ���Ƿ���Ҫ�����Լ����·�ʽ(�������¡�����������)
 *				3.3. ִ����Ӧ����
 *			4. �����¼����MMS����
 *				4.1. ��ѯ��Ӧ��¼���Ƿ���DRS����
 *				4.2. �����¼��DRS���У������һ��������ӵ�MMS��
 */
void MmsSblTestCase::doSelect() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::doSelect", conn, 0);
	if (!session) { // �޷���ȡSESSION
		m_db->freeConnection(conn);
		return;
	}
	Record *updRecord;
	RowId rowId = INVALID_ROW_ID;
	RowLockHandle *rlh = NULL;
	SubRecord subRecord;
	Record record;
	MmsRecord *mmsRecord;
	MemHeapRid mhRid;
	MemHeapRecord *mhRecord;
	bool isCountTbl;
	u64 id;
	//byte *pkey;
	//byte pkeySize;
	//TableDef *tableDef;
	u16 cols_count[2] = {COUNT_ID_CNO, COUNT_COUNT_CNO};
	u16 cols_account[3] = {ACCOUNT_ID_CNO, ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
	u16 cols_count_without_pk[1] = {COUNT_COUNT_CNO};
	u16 cols_account_without_pk[2] = {ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
	int k = RandomGen::nextInt(0, m_numTables); // ���ѡ�������
	//bool byRid = RandomGen::tossCoin(GET_BY_RID_WEIGHT);

	const TableDef *tableDef = m_heaps[k]->getTableDef();
	if (tableDef->m_recFormat == REC_FIXLEN) {
		isCountTbl = true;
	} else {
		assert(tableDef->m_recFormat == REC_VARLEN);
		isCountTbl = false;
	}
	rowId = m_memHeaps[k]->getRandRowId();
	if (rowId == INVALID_ROW_ID) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		return;
	}
	mmsRecord = m_mmsTables[k]->getByRid(session, rowId, true, &rlh, Exclusived);
	m_numSelect++;
	if (mmsRecord) { // ��¼������MMS��
		record.m_data = new byte[m_tables[k]->getTableDef()->m_maxRecSize];
		m_mmsTables[k]->getRecord(mmsRecord, &record);
		// �Ƚ�MMS���ڴ���е������Ƿ�һ��
		SubRecord keyRecord;
		keyRecord.m_format = KEY_NATURAL;

		keyRecord.m_numCols = tableDef->m_pkey->m_numCols;
		keyRecord.m_columns = tableDef->m_pkey->m_columns;
		keyRecord.m_data = new byte[tableDef->m_pkey->m_maxKeySize];
		keyRecord.m_size = tableDef->m_pkey->m_maxKeySize;
		if (REC_FIXLEN == tableDef->m_recFormat) 
			RecordOper::extractKeyFN(tableDef, &record, &keyRecord);
		else
			RecordOper::extractKeyVN(tableDef, &record, &keyRecord);
		assert(keyRecord.m_size == sizeof(u64));
		id = *(u64*)keyRecord.m_data;
		mhRid = (MemHeapRid)id;
		delete [] keyRecord.m_data;
		mhRecord = m_memHeaps[k]->recordAt(session, mhRid);
		NTSE_ASSERT(mhRecord);
		NTSE_ASSERT(mhRecord->compare(session, &record));
		delete [] record.m_data;
		
		if (RandomGen::tossCoin(UPDATE_WEIGHT_WHEN_SELECT)) { // ���²���
			if (isCountTbl)
				updRecord = CountTable::createRecord(0, RandomGen::nextInt());
			else
				updRecord = AccountTable::createRecord(0);
			subRecord.m_format = REC_REDUNDANT;
			subRecord.m_numCols = tableDef->m_numCols - 1;
			if (isCountTbl)
				subRecord.m_columns = cols_count_without_pk;
			else
				subRecord.m_columns = cols_account_without_pk;	
			subRecord.m_size = updRecord->m_size;
			subRecord.m_data = updRecord->m_data;
			subRecord.m_rowId = mhRecord->getRowId();
			u16 recSize;
			if (m_mmsTables[k]->canUpdate(mmsRecord, &subRecord, &recSize)) {
				m_mmsTables[k]->update(session, mmsRecord, &subRecord, recSize);
			} else {
				m_mmsTables[k]->flushAndDel(session, mmsRecord);
				m_heaps[k]->update(session, subRecord.m_rowId, &subRecord);
			}
			//m_mmsTables[k]->update(session, mmsRecord, &subRecord);
			mhRecord->update(session, subRecord.m_numCols, subRecord.m_columns, subRecord.m_data);
			freeRecord(updRecord);
		} else
			m_mmsTables[k]->unpinRecord(session, mmsRecord);
		m_numUpdate++;
	} else { // ��¼��������MMS��
		record.m_format = tableDef->m_recFormat;
		record.m_data = new byte[tableDef->m_maxRecSize];
		if (m_heaps[k]->getRecord(session, rowId, &record, Exclusived, &rlh)) {
			if (RandomGen::tossCoin(INSERT_MMS_WEIGHT_WHEN_SELECT)) {
				mmsRecord = m_mmsTables[k]->putIfNotExist(session, &record);
				if (mmsRecord)
					m_mmsTables[k]->unpinRecord(session, mmsRecord);
			}
		} else { // ��¼������
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			delete [] record.m_data;
			return;
		}
		delete [] record.m_data;
	}

	session->unlockRow(&rlh);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
 * ��ȡ������������
 *
 * @return �������� 
 */
int MmsSblTestCase::getTaskType() {
	int selectProb = SELECT_WEIGHT;
	int deleteProb = DELETE_WEIGHT * 100 / (100 - selectProb); 

	if (RandomGen::tossCoin(selectProb))
		return MmsSblTestCase::WORK_TASK_TYPE_SELECT;
	if (RandomGen::tossCoin(deleteProb))
		return MmsSblTestCase::WORK_TASK_TYPE_DELETE;
	return MmsSblTestCase::WORK_TASK_TYPE_INSERT;
}

