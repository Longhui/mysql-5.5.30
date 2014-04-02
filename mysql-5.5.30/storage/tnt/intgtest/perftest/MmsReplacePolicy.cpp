/**
* MMS�滻���Բ���
*
* @author �۷�(shaofeng@corp.netease.com, sf@163.org)
*/

#include "MmsReplacePolicy.h"
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
const uint MmsReplacePolicyTest::UPDATE_IF_EXIST = 66;
const uint MmsReplacePolicyTest::DELETE_IF_EXIST = 0;
const uint MmsReplacePolicyTest::GET_IF_EXIST = 34;

const uint MmsReplacePolicyTest::INSERT_IF_NO_EXIST = 80;
const uint MmsReplacePolicyTest::NOTHING_IF_NO_EXIST = 20;

// �����ݹ�ģ
const uint MmsReplacePolicyTest::MAX_REC_COUNT_IN_TABLE = 1024 * 100;
const uint MmsReplacePolicyTest::TABLE_NUM_IN_SAME_TEMPLATE = 10;

// ���Ի�����Ϣ
const uint MmsReplacePolicyTest::WORKING_THREAD_NUM = 1;
const uint MmsReplacePolicyTest::VERIFY_DURATION = 5 * 60 * 1000;
const uint MmsReplacePolicyTest::FOREVER_DURATION = (uint) -1;
const uint MmsReplacePolicyTest::RUN_DURATION = 10 * 60 * 60 * 1000;

const uint MmsReplacePolicyTest::PAGE_NUM = 350;
const uint MmsReplacePolicyTest::SLOT_NUM = 203;

MmsReplacePolicyTest::MmsReplacePolicyTest() {
}

/** 
* ��ȡ������
*
* @return ������
*/
string MmsReplacePolicyTest::getName() const {
	return "Mms replace policy test";
}

/** 
* ��ȡ��������
*
* @return ��������
*/
string MmsReplacePolicyTest::getDescription() const {
	return "Replace policy test for mms module";
}

void MmsReplacePolicyTest::run() {
	testMmsReplace();
}

/**
* �Ƿ�Ϊ���������
*
* @return �Ƿ�Ϊ���������
*/
bool MmsReplacePolicyTest::isBig() {
	return true;
}

/** 
* ��ʼ
* ����:	1. ɾ�������ݿ⣬���������ݿ�
*			2. �������Ŷ�����Ͷ��ű䳤��
*			3. ������֤�̡߳������̺߳���־���
*/
void MmsReplacePolicyTest::setUp() {
	// ���ݳ�ʼ��
	m_cfg = new Config();
	Database::drop(m_cfg->m_basedir);
	File dir(m_cfg->m_basedir);
	dir.mkdir();
	NTSE_ASSERT(m_db = Database::open(m_cfg, true));
	// ����Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsReplacePolicyTest::setUp", conn);

	m_numTables = TABLE_NUM_IN_SAME_TEMPLATE;
	m_mms = m_db->getMms();  // ȫ��MMS��m_db�ṩ�����С��cfgָ��

	m_mmsTables = new MmsTable* [m_numTables];
	m_heaps = new DrsHeap* [m_numTables];
	m_tables = new Table* [m_numTables];
	m_tableDefs = new TableDef* [m_numTables];

	stringstream name;
	for (int i = 0; i < m_numTables; i++) { // Count��
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
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_numVerify = 0;
	m_numSelect = 0;
	m_numDelete = 0;
	m_numInsert = 0;
	m_numUpdate = 0;
	m_numGet = 0;
	m_numNothing = 0;
	m_numPreSelect = 0;
	m_numPreDelete = 0;
	m_numPreInsert = 0;
	m_numPreUpdate = 0;
	m_numPreGet = 0;
	m_numPreNothing = 0;

	m_mmsRecordQueries = 0;				
	m_mmsRecordQueryHits = 0;
	m_mmsRecordInserts = 0;				
	m_mmsRecordDeletes = 0;		
	m_mmsRecordUpdates = 0;	
	m_mmsRecordVictims = 0;    
	m_mmsPageVictims = 0;		
	m_mmsOccupiedPages = 0;
	m_mmsFreePages = 0;
	m_dataVolume = 0;

	m_rowIdTables = NULL;

	// �̳߳�ʼ��
	m_verifierThread = new MmsReplaceVerifier(this, VERIFY_DURATION);
	m_nrWorkThreads = WORKING_THREAD_NUM;
	m_workerThreads = new MmsReplaceWorker *[m_nrWorkThreads];
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsReplaceWorker(this);
	m_zipfRandoms = new ZipfRandom* [m_numTables];
}

/**
*  Ԥ��
*/
void MmsReplacePolicyTest::warm() {
	m_rowIdTables = new RowId* [m_numTables];

	// װ��ʵ�����ݵ�Heap
	Record *record;
	int m_recCount = 10 * PAGE_NUM * SLOT_NUM / m_numTables;
	for (int i = 0; i < m_numTables; i++)
		m_rowIdTables[i] = new RowId[m_recCount];   // ��¼����С�������Ϊһ��bigint
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsReplacePolicyTest::warm", conn);
	for (int k = 0; k < m_numTables; k++) {
		for (int j = 0; j < m_recCount; j++) {
			RowLockHandle *rlh = NULL;
			assert(m_heaps[k]->getTableDef()->m_recFormat == REC_FIXLEN);
			record = CountTable::createRecord(RandomGen::nextInt(), RandomGen::nextInt());
			record->m_format = REC_FIXLEN;
			m_dataVolume += (u64)record->m_size;
			m_rowIdTables[k][j] = m_heaps[k]->insert(session, record, &rlh);  // ����DRS��, ��¼Ϊ�䳤/������ʽ
			session->unlockRow(&rlh);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	for (int i = 0; i < m_numTables; i++)
		m_zipfRandoms[i] = ZipfRandom::createInstVaryScrew(m_recCount, false); // ����Prop(Top10%) = 90% ��Zipf�������

	cout << "Total Data Volume is " << m_dataVolume << "bytes" << endl;
}

/** 
* ����
* ���裺	1. ɾ����־��ء�ֹͣ�����̺߳���֤�߳�
*			2. ɾ����֤�̺߳͹����߳�ʵ��
*			3. �رղ�ɾ������ڴ��
*			4. ɾ�����ݿ⼰������
*/
void MmsReplacePolicyTest::tearDown() {
	stopThreads();
	m_verifierThread->stop();
	m_verifierThread->join();

	delete m_verifierThread;
	m_verifierThread = NULL;
	for (int i = 0; i < m_nrWorkThreads; i++)
		delete m_workerThreads[i];
	delete [] m_workerThreads;
	m_workerThreads = NULL;

	// �ͷ���Դ
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsReplacePolicyTest::tearDown", conn);
	stringstream name;
	for (int i = 0; i < m_numTables; i++) {
		name.str("");
		name << i;
		m_db->closeTable(session, m_tables[i]);
		m_db->dropTable(session, name.str().c_str());
		delete m_tableDefs[i];
	}
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

void MmsReplacePolicyTest::testMmsReplace() {
	warm();

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
void MmsReplacePolicyTest::startThreads() {
	this->m_beginTime = System::fastTime();
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i]->start();
}

/** 
* ֹͣ���й����߳� 
*/
void MmsReplacePolicyTest::stopThreads() {
	for (int i = 0; i < m_nrWorkThreads; i++) {
		m_workerThreads[i]->stop();
		m_workerThreads[i]->join();
	}
	m_endTime = System::fastTime();
}

/** 
* ʵ�ʹ������� 
*/
void MmsReplacePolicyTest::doWork() {
	int k = RandomGen::nextInt(0, m_numTables); // ���ѡ�������
	u64 id;

	id = (u64)m_zipfRandoms[k]->rand();

	doSelect(k, id);
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
void MmsReplacePolicyTest::doVerify() {
	// ��ͣ���й����߳�
	stopThreads();
	for (int i = 0; i < m_nrWorkThreads; i++)
		delete m_workerThreads[i];

	const MmsStatus& mmsStats = m_db->getMms()->getStatus();
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
	cout << "Get Count: " << m_numGet << endl;
	cout << "Get Performance (Count/Sec): " << (m_numGet - m_numPreGet)  / (m_endTime - m_beginTime) << endl;
	cout << "NULL Count: " << m_numNothing << endl;
	cout << "NULL Performance (Count/Sec): " << (m_numNothing - m_numPreNothing)  / (m_endTime - m_beginTime) << endl;
	cout << "Total Performance(Count/Sec) " << (m_numSelect - m_numPreSelect) / (m_endTime - m_beginTime) << endl;

	m_numPreInsert = m_numInsert;
	m_numPreDelete = m_numDelete;
	m_numPreSelect = m_numSelect;
	m_numPreUpdate = m_numUpdate;
	m_numPreGet	= m_numGet;
	m_numPreNothing = m_numNothing;

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
	
	int totalRecordCount = 0;
	for (int i = 0; i < m_numTables; i++)
		totalRecordCount += m_mmsTables[i]->getStatus().m_records;
	cout << " MMS Records: " << totalRecordCount << endl;
	cout << " Avg MMS Query Hit Ratio: " << ((double)mmsStats.m_recordQueryHits / (double)mmsStats.m_recordQueries) * 100.0 << "%" << endl;
	cout << " Curr MMS Query Hit Ratio: " << ((double)(mmsStats.m_recordQueryHits - m_mmsRecordQueryHits)  / (double)(mmsStats.m_recordQueries - m_mmsRecordQueries)) * 100.0 << "%" << endl;

	// BUFFER ���ͳ��
	cout << "---------------- BUFFER STATS ------------------------------------" << endl;
	m_db->getPageBuffer()->printStatus(cout);

	m_mmsRecordQueries = mmsStats.m_recordQueries;
	m_mmsRecordQueryHits = mmsStats.m_recordQueryHits;
	m_mmsRecordInserts = mmsStats.m_recordInserts;
	m_mmsRecordDeletes = mmsStats.m_recordDeletes;
	m_mmsRecordUpdates = mmsStats.m_recordUpdates;
	m_mmsRecordVictims = mmsStats.m_recordVictims;
	m_mmsPageVictims = mmsStats.m_pageVictims;


	// ��ӡ����Ϣ
	RWLockUsage::printAll(cout);

	// ��ͻ�����ͳ��
	cout << "---------------- MMS CONFLICT STATS ------------------------------------" << endl;
	double avgConflictLen;
	size_t maxConflictLen;
	for (int i = 0; i < m_numTables; i++) {
		int ridZones = m_mmsTables[i]->getPartitionNumber();
		for (int j = 0; j < ridZones; j++) {
			m_mmsTables[i]->getRidHashConflictStatus(j, &avgConflictLen, &maxConflictLen);
			cout << "MmsTable[" << i << "][" << j << "] Conflict Status: " << avgConflictLen << ", " << maxConflictLen << endl;
		}
	}

	m_numVerify++;

	// �ָ����й����߳�
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsReplaceWorker(this);
	startThreads();
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
void MmsReplacePolicyTest::doSelect(int k, u64 id) {
	int workIfExist, workIfNotExist;
	bool shareMode;
	shareMode = getTaskType(&workIfExist, &workIfNotExist);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsPerfTestCase::doSelect", conn, 0);
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
	//TableDef *tableDef;
	u16 cols_count[2] = {COUNT_ID_CNO, COUNT_COUNT_CNO};
	u16 cols_account[3] = {ACCOUNT_ID_CNO, ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};
	u16 cols_count_without_pk[1] = {COUNT_COUNT_CNO};

	const TableDef *tableDef = m_heaps[k]->getTableDef();
	rowId = m_rowIdTables[k][id];
	if (shareMode)
		mmsRecord = m_mmsTables[k]->getByRid(session, rowId, true, &rlh, Shared);
	else
		mmsRecord = m_mmsTables[k]->getByRid(session, rowId, true, &rlh, Exclusived);
	m_numSelect++;
	if (mmsRecord) { // ��¼������MMS��
		if (workIfExist == WORK_UPDATE_IF_EXIST) {
			m_numUpdate++;
			updRecord = CountTable::createRecord(0, RandomGen::nextInt());
			subRecord.m_format = REC_REDUNDANT;
			subRecord.m_numCols = tableDef->m_numCols - 1;
			subRecord.m_columns = cols_count_without_pk;
			subRecord.m_size = updRecord->m_size;
			subRecord.m_data = updRecord->m_data;
			subRecord.m_rowId = rowId;
			u16 recSize;
			if (m_mmsTables[k]->canUpdate(mmsRecord, &subRecord, &recSize)) {
				m_mmsTables[k]->update(session, mmsRecord, &subRecord, recSize);
			} else {
				m_mmsTables[k]->flushAndDel(session, mmsRecord);
				m_heaps[k]->update(session, subRecord.m_rowId, &subRecord);
			}
			freeRecord(updRecord);
		} else if (workIfExist == WORK_DELETE_IF_EXIST) {
			m_numDelete++;
			m_mmsTables[k]->del(session, mmsRecord);
		} else {
			m_numGet++;
			assert(workIfExist == WORK_GET_IF_EXIST);
			record.m_data = new byte[m_tables[k]->getTableDef()->m_maxRecSize];
			m_mmsTables[k]->getRecord(mmsRecord, &record);
			m_mmsTables[k]->unpinRecord(session, mmsRecord);
			delete [] record.m_data;	
		}
	} else {
		if (workIfNotExist == WORK_INSERT_IF_NOT_EXIST) {
			m_numInsert++;
			record.m_format = tableDef->m_recFormat;
			record.m_data = new byte[tableDef->m_maxRecSize];
			bool succ = m_heaps[k]->getRecord(session, rowId, &record, Exclusived, &rlh);
			assert(succ);
			mmsRecord = m_mmsTables[k]->putIfNotExist(session, &record);
			if (mmsRecord)
				m_mmsTables[k]->unpinRecord(session, mmsRecord);
			delete [] record.m_data;
		} else {
			m_numNothing++;
			assert(workIfNotExist == WORK_NOTHING_IF_NOT_EXIST);
		}
	}
	if (rlh)
		session->unlockRow(&rlh);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
* ��ȡ������������
*
* @param workIfExist INOUT ����¼��MMSʱ�Ĺ�������
* @param workIfNotExist INOUT ����¼����MMSʱ�Ĺ�������
* @return �Ƿ�ӹ�����������true�ӹ�����������ӻ�����
*/
bool MmsReplacePolicyTest::getTaskType(int *workIfExist, int *workIfNotExist) {
	if (RandomGen::tossCoin(UPDATE_IF_EXIST))
		*workIfExist = WORK_UPDATE_IF_EXIST;
	else if (RandomGen::tossCoin(DELETE_IF_EXIST * 100 / (100 - UPDATE_IF_EXIST)))
		*workIfExist = WORK_DELETE_IF_EXIST;
	else
		*workIfExist = WORK_GET_IF_EXIST;

	if (RandomGen::tossCoin(INSERT_IF_NO_EXIST))
		*workIfNotExist = WORK_INSERT_IF_NOT_EXIST;
	else
		*workIfNotExist = WORK_NOTHING_IF_NOT_EXIST;


	if (*workIfExist == WORK_GET_IF_EXIST && *workIfNotExist == WORK_NOTHING_IF_NOT_EXIST)
		return true;
	else
		return false;
}

