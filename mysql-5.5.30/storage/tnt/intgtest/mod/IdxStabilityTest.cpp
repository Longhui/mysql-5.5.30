/**
 *	����ģ���ȶ��Բ���ʵ��
 *
 *	@author �ձ�(bsu@corp.netease.com, naturally@163.org)
 */

#include <iostream>
#include <string>
#include <sstream>
#include <cppunit/config/SourcePrefix.h>
#include "DbConfigs.h"
#include "MemHeap.h"
#include "IdxStabilityTest.h"
#include "BlogTable.h"
#include "misc/Session.h"
#include "misc/IndiceLockManager.h"
#include "api/Database.h"
#include "misc/RecordHelper.h"
#include "Random.h"
#include "util/Thread.h"
#include "util/File.h"

using namespace std;
using namespace ntse;

#define EXCPT_OPER(op)									\
	try {												\
		op;												\
	} catch (NtseException &e) {						\
		cout << "Error: " << e.getMessage() << endl;	\
		CPPUNIT_FAIL(e.getMessage());					\
		throw e;										\
	}


// �������Ȩ��
const uint TotalOpGenerator::IDX_INSERT_WEIGHT = 35;
const uint TotalOpGenerator::IDX_DELETE_WEIGHT = 5;
const uint TotalOpGenerator::IDX_UPDATE_WEIGHT = 10;
const uint TotalOpGenerator::IDX_SCAN_WEIGHT = 50;

// ɨ�����Ȩ��
const uint ScanOpGenerator::IDX_SCAN_SINGLE_WEIGHT = 20;
const uint ScanOpGenerator::IDX_SCAN_RANGE_FORWARD_WEIGHT = 40;
const uint ScanOpGenerator::IDX_SCAN_RANGE_BACKWARD_WEIGHT = 40;

// ɾ������Ȩ��
const uint UpdateOpGenerator::IDX_UPDATE_FROM_TABLE_WEIGHT = 5;
const uint UpdateOpGenerator::IDX_UPDATE_SINGLE_WEIGHT = 55;
const uint UpdateOpGenerator::IDX_UPDATE_RANGE_FORWARD_WEIGHT = 20;
const uint UpdateOpGenerator::IDX_UPDATE_RANGE_BACKWARD_WEIGHT = 20;

// ���²���Ȩ��
const uint DeleteOpGenerator::IDX_DELETE_FROM_TABLE_WEIGHT = 10;
const uint DeleteOpGenerator::IDX_DELETE_SINGLE_WEIGHT = 50;
const uint DeleteOpGenerator::IDX_DELETE_RANGE_FORWARD_WEIGHT = 20;
const uint DeleteOpGenerator::IDX_DELETE_RANGE_BACKWARD_WEIGHT = 20;

// �����ݹ�ģ
const uint TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS = 16;
const uint TestEnvVars::BLOG_TABLE_MAX_REC_NUM = (1 << TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS) - 1;

// ���Ի�����Ϣ
const uint TestEnvVars::INDEX_MAX_RANGE = 50;
const uint TestEnvVars::WORKING_THREAD_NUM = 500;
const uint TestEnvVars::CHECK_DURATION = 20 * 60 * 1000;
const double TestEnvVars::NULL_RATIO = 0.001;
const double TestEnvVars::DELETE_RANGE_RATIO = 0.005;
const double TestEnvVars::UPDATE_RANGE_RATIO = 0.01;

const uint TestEnvVars::STATUS_DURATION = 30 * 1000;

const char* StatusThread::STATUS_LOG = "IdxStatus.log";

const char* IndexStabilityTestCase::getName() {
	return "Index stability test case";
}


const char* IndexStabilityTestCase::getDescription() {
	return "Test all kinds of index operation time to time by multi-threads and check correctness periodically";
}


bool IndexStabilityTestCase::isBig() {
	return true;
}

void IndexStabilityTestCase::setUp() {
	m_totalGenerator = new TotalOpGenerator();
	m_scanGenerator = new ScanOpGenerator();
	m_deleteGenerator = new DeleteOpGenerator();
	m_updateGenerator = new UpdateOpGenerator();
	m_keepWorking = true;

	m_config = new Config(CommonDbConfig::getMedium());

	Database::drop(m_config->m_basedir);
	File dir(m_config->m_basedir);
	dir.mkdir();
	EXCPT_OPER(m_db = Database::open(m_config, true));

	u16 numCkCols = 0;
	u16 *ckCols = new u16[Limits::MAX_COL_NUM];

	m_tables = 1;	// ��ʱֻʹ��Blog��
	m_tableInfo = new ISTableInfo*[m_tables];
	// ����Blog����Ϣ
	ckCols[0] = BLOG_TITLE_CNO;
	m_tableInfo[0] = new ISTableInfo(m_db, BlogTable::getTableDef(false));
	CPPUNIT_ASSERT(m_tableInfo[0]->createHeap(TestEnvVars::BLOG_TABLE_MAX_REC_NUM, numCkCols, ckCols));
	CPPUNIT_ASSERT(m_tableInfo[0]->createIndex());
	vs.idx = true;
	//ts.idx = true;
	//ts.irl = true;

	delete [] ckCols;

	srand((unsigned)time(NULL));
	
	m_resLogger = new ResLogger(m_db, 1800, "[Idx]DbResource.txt");
}


void IndexStabilityTestCase::tearDown() {
	delete m_resLogger;

	if (m_tableInfo != NULL) {
		for (uint i = 0; i < m_tables; i++)
			delete m_tableInfo[i];
		delete [] m_tableInfo;
	}
	if (m_db != NULL) {
		m_db->close();
		delete m_db;
		m_db = NULL;
		Database::drop(".");
	}
	if (m_config != NULL)
		delete m_config;

	delete m_totalGenerator;
	delete m_scanGenerator;
	delete m_deleteGenerator;
	delete m_updateGenerator;
}


/**
 * ��ʼ�������̵߳�
 */
void IndexStabilityTestCase::init() {
	m_workingThread = new WorkingThread*[TestEnvVars::WORKING_THREAD_NUM];
	for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++) {
		m_workingThread[i] = new WorkingThread(this, m_tableInfo[i - i / m_tables * m_tables]);
		m_workingThread[i]->start();
	}
	m_statusThread = new StatusThread(this, TestEnvVars::STATUS_DURATION);
	m_statusThread->start();
}

/**
 * ������
 */
void IndexStabilityTestCase::clean() {
	for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++)
		delete m_workingThread[i];

	delete [] m_workingThread;
}

/**
 * ��ͣ�����߳�
 */
bool IndexStabilityTestCase::suspendWorkingThreads() {
	while (true) {
		uint stillAlive = 0;
		for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++) {
			m_workingThread[i]->pause();
			Thread::msleep(10);
			if (m_workingThread[i]->isRunning())
				stillAlive++;
		}
		
		if (stillAlive == 0) {
			cout << "All threads suspended" << endl;
			break;
		}

		cout << "Still " << stillAlive << " threads alive" << endl;
		stillAlive = 0;
	}

	return true;
}

/**
 * �ù����̼߳���
 */
bool IndexStabilityTestCase::resumeWorkingThreads() {
	for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++)
		m_workingThread[i]->resume();

	return true;
}

/**
 * ֹͣ���й����߳�
 */
bool IndexStabilityTestCase::stopWorkingThreads() {
	for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++)
		m_workingThread[i]->stop();

	return true;
}

/**
 * ���Թ�������
 */
void IndexStabilityTestCase::testStability() {
	init();

	while (true) {
		Thread::msleep(TestEnvVars::CHECK_DURATION);
		if (!m_keepWorking) {	// �ⲿָ���˳�
			stopWorkingThreads();
			break;
		}
		cout << "Start verify" << endl;
		suspendWorkingThreads();
		if (!verify(false)) {
			cout << "Index and mem-heap are not consistency" << endl;
			::exit(0);
		}
		cout << "Finish verify, test will go on" << endl;
		resumeWorkingThreads();

	}

	clean();
}

/**
 * �Ӳ��������õ�һ�����ݿ�����
 */
Connection* IndexStabilityTestCase::getAConnection() {
	return m_db->getConnection(false);
}


/**
 * �ر�һ������
 * @param conn ���Ӿ��
 */
void IndexStabilityTestCase::closeAConnection(Connection *conn) {
	m_db->freeConnection(conn);
}


/**
 * ��ָ���ļ�¼����������£�����Ҫ��֤һ���ᵼ�¸��¸ü�¼��Ӧ��ĳ������
 * @param session		�Ự���
 * @param memoryContext	�ڴ�������
 * @param tableDef		����
 * @param indexDef		����ɨ��ʹ�õ���������
 * @param indice		��������
 * @param memRec		�ڴ�Ѽ�¼
 * @param rowId			��ID��Ϣ
 */
void IndexStabilityTestCase::randomUpdateRecord(Session *session, MemoryContext *memoryContext, TableDef *tableDef, IndexDef *indexDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId) {
	Record *record = RecordHelper::generateRecord(tableDef, rowId);
	memRec->toRecord(session, record->m_data);

	SubRecord *before, *after;
	RecordHelper::formUpdateSubRecords(tableDef, indexDef, record, &before, &after);

	session->startTransaction(TXN_UPDATE, tableDef->m_id);
	uint dupIndex;
	if (indice->updateIndexEntries(session, before, after, &dupIndex))
		memRec->update(session, after->m_numCols, after->m_columns, after->m_data);
	session->endTransaction(true);

	freeSubRecord(before);
	freeSubRecord(after);
	freeRecord(record);
}


/**
 * ������������ɾ��ָ����������ֵ
 * @oaram session		�Ự���
 * @param memoryContext	�ڴ�������
 * @param tableDef		����
 * @param indice		��������
 * @param memRec		�ڴ�Ѽ�¼
 * @param rowId			��ID��Ϣ
 * @param handle		����ɨ����
 */
void IndexStabilityTestCase::deleteSpecifiedIndexKey(Session *session, MemoryContext *memoryContext, TableDef *tableDef, DrsIndice *indice, MemHeapRecord *memRec, RowId rowId, IndexScanHandle *handle) {
	Record *record = RecordHelper::generateRecord(tableDef, rowId);
	memRec->toRecord(session, record->m_data);

	session->startTransaction(TXN_DELETE, tableDef->m_id);
	indice->deleteIndexEntries(session, record, handle);
	session->endTransaction(true);
	freeRecord(record);
}

/**
 * ���ִ�и��ֲ���
 * @param conn			������Ϣ
 * @param isTableInfo	����Ϣ�ṹ
 */
void IndexStabilityTestCase::test(Connection *conn, ISTableInfo *isTableInfo) {
	// ���ִ�и��ֲ���
	uint opkind = m_totalGenerator->getOp();
	switch (opkind) {
		case TotalOpGenerator::IDX_INSERT:
			doInsert(conn, isTableInfo);
			break;
		case TotalOpGenerator::IDX_DELETE:
			doDelete(conn, isTableInfo);
			break;
		case TotalOpGenerator::IDX_SCAN:
			doScan(conn, isTableInfo);
			break;
		case TotalOpGenerator::IDX_UPDATE:
			doUpdate(conn, isTableInfo);
			break;
		default:
			assert(false);
	}
}


/**
 * ��ӡ��ǰ����״̬ͳ����Ϣ��ָ���ļ�
 * @param logFile	��־�ļ���
 */
void IndexStabilityTestCase::printStatus(const char *logFile) {
	u64 errNo;
	File file(logFile);
	if (!File::isExist(logFile)) {	// �Ѿ����ڣ������ļ�����Ϊ0
		errNo = file.create(false, false);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			return;
	} else {
		errNo = file.open(false);
		assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	}
	
	u64 fileoffset;
	errNo = file.getSize(&fileoffset);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	// �õ�ʱ��
	time_t now = time(NULL);
	char timestr[30];
	System::formatTime(timestr, sizeof(timestr), &now);

	// д�뵱ǰ������������Ϣ��ָ����
	for (uint i = 0; i < m_tables; i++) {
		TableDef *tableDef = (TableDef*)m_tableInfo[i]->getTableDef();
		DrsIndice *indice = m_tableInfo[i]->getIndice();
		for (uint j = 0; j < indice->getIndexNum(); j++) {
			DrsIndex *index = indice->getIndex(j);
			const struct IndexStatus *status = &(index->getStatus());
			DBObjStats *dbobjStats = index->getDBObjStats();
			ostringstream oss;
			oss << endl << timestr << endl 
				<< "*********************************************" << endl
				<< tableDef->m_indice[j]->m_name << endl
				<< "Data Length: " << status->m_dataLength << endl
				<< "Free Length: " << status->m_freeLength << endl
				<< "Operations:" << endl
				<< "Insert: " << dbobjStats->m_statArr[DBOBJ_ITEM_INSERT] << endl
				<< "Update: " << dbobjStats->m_statArr[DBOBJ_ITEM_UPDATE] << endl
				<< "Delete: " << dbobjStats->m_statArr[DBOBJ_ITEM_DELETE] << endl
				<< "Scans: " << dbobjStats->m_statArr[DBOBJ_SCAN] << endl
				<< "Rows scans: " << dbobjStats->m_statArr[DBOBJ_SCAN_ITEM] << endl
				<< "Back scans: " << status->m_backwardScans << endl
				<< "Rows bscans: " << status->m_rowsBScanned << endl
				<< "Insert SMO: " << status->m_numSplit << endl
				<< "Delete SMO: " << status->m_numMerge << endl
				<< "Row lock restarts: " << status->m_numRLRestarts << endl
				<< "Idx lock restarts for insert: " << status->m_numILRestartsForI << endl
				<< "Idx lock restarts for delete: " << status->m_numILRestartsForD << endl
				<< "Latch conflicts: " << status->m_numLatchesConflicts << endl
				<< "Deadlock restarts: " << status->m_numDeadLockRestarts.get() << endl
				<< endl << "*********************************************" << endl;

			size_t size = oss.str().size();
			errNo = file.setSize(fileoffset + size);
			assert(File::getNtseError(errNo) == File::E_NO_ERROR);
			errNo = file.write(fileoffset, (u32)size, oss.str().c_str());
			assert(File::getNtseError(errNo) == File::E_NO_ERROR);
			fileoffset += size;
		}
	}

	// д��������Ϣ���ļ�
	{
		IndiceLockTableStatus *status = &(m_db->getIndicesLockManager()->getStatus());
		ostringstream oss;
		oss << endl << "Indice lock manager status: " << endl
			<< "Locks: " << status->m_locks << endl
			<< "Try locks: " << status->m_trylocks << endl
			<< "Unlocks: " << status->m_unlocks << endl
			<< "Unlock alls: " << status->m_unlockAlls << endl
			<< "Deadlock: " << status->m_deadlocks << endl
			<< "Waits: " << status ->m_waits << endl
			<< "S wait time: " << status->m_sWaitTime << endl
			<< "F wait time: " << status->m_fWaitTime << endl
			<< "Inuse entries: " << status->m_inuseEntries << endl
			<< "Avg clen: " << status->m_avgConflictLen << endl
			<< "Max clen: " << status->m_maxConflictLen << endl
			<< endl << "################################################" << endl;

		size_t size = oss.str().size();
		errNo = file.setSize(fileoffset + size);
		assert(File::getNtseError(errNo) == File::E_NO_ERROR);
		errNo = file.write(fileoffset, (u32)size, oss.str().c_str());
		assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	}

	errNo = file.sync();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	errNo = file.close();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
}


/**
 * ��֤�����������ڴ�ѵ�һ����
 * @param simple	����ֻ֤��֤��������֮������һ��
 * @return trueһ��/false��һ��
 */
bool IndexStabilityTestCase::verify(bool simple) {
	ftrace(ts.rl, tout << "Verify");
	// ���������ڴ��,����˫����֤
	Connection *conn = m_db->getConnection(false);
	RowLockHandle *rlh;
	RowId rowId;

	for (uint i = 0; i < m_tables; i++) {
		ISTableInfo *isTableInfo = m_tableInfo[i];
		assert(isTableInfo != NULL);
		TableDef *tableDef = (TableDef*)isTableInfo->getTableDef();
		MemHeap *memHeap = isTableInfo->getMemHeap();
		DrsIndice *indice = isTableInfo->getIndice();

		uint totalMemHeapRecs = 0;
		uint indexNum = indice->getIndexNum();
		unsigned maxSize = memHeap->getMaxRecCount();

		Session *session = m_db->getSessionManager()->allocSession("IndexStabilityTestCase::verify", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		// ����ȷ�ϸ���������������һ��
		uint indexSingleKeys = 0;
		for (uint j = 0; j < indexNum; j++) {
			uint foundKeys = 0;
			DrsIndex *index = indice->getIndex(j);
			IndexDef *indexDef = tableDef->m_indice[j];
			//DrsIndexScanHandle *handle = index->beginScan(session, NULL, true, true, Shared, &rlh, NULL);
			IndexScanHandle *handle = index->beginScan(session, NULL, true, true, Shared, &rlh, NULL);
			while (index->getNext(handle, NULL)) {
				foundKeys++;
				RowId rowId = handle->getRowId();
				MemHeapRid rid = getSlotNoFromRowId(rowId);
				MemHeapRecord *memRec = memHeap->recordAt(session, rid);
				assert(memRec != NULL && memRec->getRowId() == rowId);
				session->unlockRow(&rlh);
			}

			index->endScan(handle);
			if (indexSingleKeys == 0 && j == 0) {	// ����ֵ
				indexSingleKeys = foundKeys;
			} else {
				if (indexSingleKeys != foundKeys) {
					cout << "Index " << j << "'s keys are unconformity: " <<
						"Found " << foundKeys << " Others " << indexSingleKeys << endl;
					assert(false);
				}
			}
		}

		if (simple) {
			m_db->getSessionManager()->freeSession(session);
			continue;
		}

		for (uint j = 0; j < maxSize; j++) {	// memHeap->indice
			MemHeapRecord *memRec = memHeap->recordAt(session, j, &rlh, Shared);
			if (memRec != NULL) {
				totalMemHeapRecs++;
				for (uint k = 0; k < indexNum; k++) {
					DrsIndex *index = indice->getIndex(k);
					IndexDef *indexDef = tableDef->m_indice[k];
					SubRecord *findKey = RecordHelper::formFindKeyFromRecord(session, tableDef, indexDef, memRec);
					if (!index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL)) {
						findKey = RecordHelper::formFindKeyFromRecord(session, tableDef, indexDef, memRec);
						index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
						assert(false);
					}
					freeSubRecord(findKey);
					assert(rowId == memRec->getRowId());
					if (rowId != memRec->getRowId())
						return false;
				}
				session->unlockRow(&rlh);
			}
		}

		// indice->memHeap
		// ����ǰ�����֤,����ֻ��Ҫ��֤����ȡ���rowId��ӦmemHeap���������Ч����,ͬʱ������Ӧ�����
		for (uint j = 0; j < indexNum; j++) {
			uint foundKeys = 0;
			DrsIndex *index = indice->getIndex(j);
			IndexDef *indexDef = tableDef->m_indice[j];
			//DrsIndexScanHandle *handle = index->beginScan(session, NULL, true, true, Shared, &rlh, NULL);
			IndexScanHandle *handle = index->beginScan(session, NULL, true, true, Shared, &rlh, NULL);
			while (index->getNext(handle, NULL)) {
				foundKeys++;
				RowId rowId = handle->getRowId();
				MemHeapRid rid = getSlotNoFromRowId(rowId);
				MemHeapRecord *memRec = memHeap->recordAt(session, rid);
				assert(memRec != NULL && memRec->getRowId() == rowId);
				session->unlockRow(&rlh);
			}

			index->endScan(handle);
			assert(foundKeys == totalMemHeapRecs);
			if (foundKeys != totalMemHeapRecs) {
				cout << "Found keys: " << foundKeys << endl
					<< "MemHeap Records: " << totalMemHeapRecs << endl;
				assert(false);
			}
		}

		m_db->getSessionManager()->freeSession(session);
	}

	m_db->freeConnection(conn);

	return true;
}


/**
 * ִ���ڴ�Ѻ������Ĳ������
 * @param conn			������Ϣ
 * @param isTableInfo	����Ϣ�ṹ
 */
void IndexStabilityTestCase::doInsert(Connection *conn, ISTableInfo *isTableInfo) {
	ftrace(ts.rl, tout << "Insert");

	MemHeap *memHeap = isTableInfo->getMemHeap();
	DrsIndice *indice = isTableInfo->getIndice();
	const TableDef *tableDef = isTableInfo->getTableDef();

	MemHeapRid id = memHeap->reserveRecord();
	if (id == MemHeapRecord::INVALID_ID)
		return;

	RowId rid = generateRowIdFromSlotNo(id);
	Session *session = m_db->getSessionManager()->allocSession("IndexStabilityTestCase::doInsert", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	session->startTransaction(TXN_INSERT, tableDef->m_id);
	
	RowLockHandle *rhl = LOCK_ROW(session, tableDef->m_id, rid, Exclusived);
	Record *record = RecordHelper::generateNewRecord((TableDef*)tableDef, rid);
	uint dupIndex;
	if (indice->insertIndexEntries(session, record, &dupIndex)) {
		NTSE_ASSERT(memHeap->insertAt(session, id, rid, record->m_data));
	} else {
		memHeap->unreserve(id);
	}
	
	session->unlockRow(&rhl);

	freeRecord(record);
	session->endTransaction(true);
	m_db->getSessionManager()->freeSession(session);

	cout << conn->getId() << ": Done insert" << endl;

	//verify(true);
}


/**
 * ִ���ڴ�Ѻ������ĸ��²���
 * @param conn			������Ϣ
 * @param isTableInfo	����Ϣ�ṹ
 */
void IndexStabilityTestCase::doUpdate(Connection *conn, ISTableInfo *isTableInfo) {
	ftrace(ts.rl, tout << "Update");

	MemHeap *memHeap = isTableInfo->getMemHeap();
	TableDef *tableDef = (TableDef*)isTableInfo->getTableDef();
	DrsIndice *indice = isTableInfo->getIndice();
	uint searchIdxNo = RandomGen::nextInt(0, indice->getIndexNum());
	DrsIndex *index = indice->getIndex(searchIdxNo);
	IndexDef *indexDef = tableDef->m_indice[searchIdxNo];
	uint totalUpdate = 1;

	Session *session = m_db->getSessionManager()->allocSession("IndexStabilityTestCase::doUpdate", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	RowLockHandle *rlh;
	SubRecord *findKey;
	RowId rowId;

	uint updateType = m_updateGenerator->getOp();
	if (updateType == UpdateOpGenerator::UPDATE_FROM_TABLE) {	// ֱ����MemHeap�漴��һ����¼����
		RowLockHandle *mhRlh = NULL;
		MemHeapRecord *memRec = getRandMemRecord(session, memHeap, &mhRlh, Exclusived);
		if (memRec == NULL)
			goto Finish;
		findKey = RecordHelper::formFindKeyFromRecord(session, tableDef, indexDef, memRec);
		if (index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL)) {
			randomUpdateRecord(session, memoryContext, tableDef, indexDef, indice, memRec, rowId);
			session->unlockRow(&mhRlh);
		} else {
			assert(false);
		}
	} else {
		uint updateNum = (updateType == UpdateOpGenerator::UPDATE_SINGLE) ? 1 : RandomGen::nextInt(0, max(1, (int)(memHeap->getUsedRecCount() * TestEnvVars::UPDATE_RANGE_RATIO)));
		bool forward = (updateType == UpdateOpGenerator::UPDATE_RANGE_FORWARD ? true : updateType == UpdateOpGenerator::UPDATE_RANGE_BACKWARD ? false : true);
		findKey = RecordHelper::makeRandomFindKey(tableDef, indexDef);
		//DrsIndexScanHandle *handle = index->beginScan(session, findKey, forward, true, Exclusived, &rlh, NULL);
		IndexScanHandle *handle = index->beginScan(session, findKey, forward, true, Exclusived, &rlh, NULL);
		
		uint count = 0;
		while (count < updateNum && index->getNext(handle, NULL)) {
			rowId = handle->getRowId();
			MemHeapRid rid = getSlotNoFromRowId(rowId);
			MemHeapRecord *memRec = memHeap->recordAt(session, rid);
			assert(memRec);
			randomUpdateRecord(session, memoryContext, tableDef, indexDef, indice, memRec, rowId);
			session->unlockRow(&rlh);
			count++;
		}
		
		index->endScan(handle);
		totalUpdate = updateNum;
	}

	freeSubRecord(findKey);

Finish:
	m_db->getSessionManager()->freeSession(session);
	//verify(true);
	cout << conn->getId() << ": Done update " << totalUpdate << endl;
}

/**
 * ִ���ڴ�Ѻ�������ɾ������
 * @param conn			������Ϣ
 * @param isTableInfo	����Ϣ�ṹ
 */
void IndexStabilityTestCase::doDelete(Connection *conn, ISTableInfo *isTableInfo) {
	ftrace(ts.rl, tout << "Delete");

	MemHeap *memHeap = isTableInfo->getMemHeap();
	TableDef *tableDef = (TableDef*)isTableInfo->getTableDef();
	DrsIndice *indice = isTableInfo->getIndice();
	uint searchIdxNo = RandomGen::nextInt(0, indice->getIndexNum());
	DrsIndex *index = indice->getIndex(searchIdxNo);
	IndexDef *indexDef = tableDef->m_indice[searchIdxNo];
	uint totalDeletes = 1;

	Session *session = m_db->getSessionManager()->allocSession("IndexStabilityTestCase::doDelete", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	RowLockHandle *rlh;
	SubRecord *findKey;
	RowId rowId;

	uint deleteType = m_deleteGenerator->getOp();
	if (deleteType == DeleteOpGenerator::DELETE_FROM_TABLE) {
		RowLockHandle *mhRlh = NULL;
		MemHeapRecord *memRec = getRandMemRecord(session, memHeap, &mhRlh, Exclusived);
		if (memRec == NULL)
			goto Finish;
		findKey = RecordHelper::formFindKeyFromRecord(session, tableDef, indexDef, memRec);
		if (index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL)) {
			deleteSpecifiedIndexKey(session, memoryContext, tableDef, indice, memRec, rowId, NULL);
			memHeap->deleteRecord(session, getSlotNoFromRowId(rowId));
			session->unlockRow(&mhRlh);
		} else {
			findKey = RecordHelper::formFindKeyFromRecord(session, tableDef, indexDef, memRec);
			index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			assert(false);
		}
	} else {
		uint deleteNum = (deleteType == DeleteOpGenerator::DELETE_SINGLE) ? 1 : RandomGen::nextInt(0, max(1, (int)(memHeap->getUsedRecCount() * TestEnvVars::DELETE_RANGE_RATIO)));
		bool forward = (deleteType == DeleteOpGenerator::DELETE_RANGE_FORWARD ? true : deleteType == DeleteOpGenerator::DELETE_RANGE_BACKWARD ? false : true);
		findKey = RecordHelper::makeRandomFindKey(tableDef, indexDef);
		//DrsIndexScanHandle *handle = index->beginScan(session, findKey, forward, true, Exclusived, &rlh, NULL);
		IndexScanHandle *handle = index->beginScan(session, findKey, forward, true, Exclusived, &rlh, NULL);

		uint count = 0;
		while (count < deleteNum && index->getNext(handle, NULL)) {
			rowId = handle->getRowId();
			MemHeapRid rid = getSlotNoFromRowId(rowId);
			MemHeapRecord *memRec = memHeap->recordAt(session, rid);
			assert(memRec);
			deleteSpecifiedIndexKey(session, memoryContext, tableDef, indice, memRec, rowId, handle);
			memHeap->deleteRecord(session, rid);
			session->unlockRow(&rlh);
			count++;
		}

		index->endScan(handle);
		totalDeletes = deleteNum;
	}
	freeSubRecord(findKey);

Finish:
	m_db->getSessionManager()->freeSession(session);
	//verify(true);
	cout << conn->getId() << ": Done delete " << totalDeletes << endl;
}

/**
 * ִ���ڴ�Ѻ�������ɨ�����
 * @param conn			������Ϣ
 * @param isTableInfo	����Ϣ�ṹ
 */
void IndexStabilityTestCase::doScan(Connection *conn, ISTableInfo *isTableInfo) {
	ftrace(ts.rl, tout << "Scan");

	MemHeap *memHeap = isTableInfo->getMemHeap();
	TableDef *tableDef = (TableDef*)isTableInfo->getTableDef();
	DrsIndice *indice = isTableInfo->getIndice();
	uint searchIdxNo = RandomGen::nextInt(0, indice->getIndexNum());
	DrsIndex *index = indice->getIndex(searchIdxNo);
	IndexDef *indexDef = tableDef->m_indice[searchIdxNo];

	Session *session = m_db->getSessionManager()->allocSession("IndexStabilityTestCase::doScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	RowLockHandle *rlh;

	uint scanType = m_scanGenerator->getOp();
	SubRecord *findKey = RecordHelper::makeRandomFindKey(tableDef, indexDef);
	uint scanNum;
	if (scanType == ScanOpGenerator::SCAN_SINGLE) {
		RowId rowId;
		if (index->getByUniqueKey(session, findKey, Shared, &rowId, NULL, &rlh, NULL)) {
			assert(rlh != NULL);
			// WHETHER TODO: ��������memHeap����
			session->unlockRow(&rlh);
			scanNum = 1;
		}
	} else {
		scanNum = RandomGen::nextInt(0, TestEnvVars::INDEX_MAX_RANGE) + 1;
		bool forward = ScanOpGenerator::SCAN_RANGE_FORWARD;
		//DrsIndexScanHandle *handle = index->beginScan(session, findKey, forward, true, Shared, &rlh, NULL);
		IndexScanHandle *handle = index->beginScan(session, findKey, forward, true, Shared, &rlh, NULL);

		uint count = 0;
		while (count < scanNum && index->getNext(handle, NULL)) {
			//RowId rowId = handle->getRowId();
			//MemHeapRid rid = getSlotNoFromRowId(rowId);
			//MemHeapRecord *memRec = memHeap->recordAt(session, rid);
			//assert(memRec);
			// WHETHER TODO: ��������memHeap����
			session->unlockRow(&rlh);
			count++;
		}

		scanNum = count;
		index->endScan(handle);
	}
	freeSubRecord(findKey);
	m_db->getSessionManager()->freeSession(session);

	cout << conn->getId() << ": Done scan " << scanNum << endl;
}


/**
 * ʵ���ڴ����ŵ�rowId��ת��
 * @param slotNo	�ڴ�ѵ�����
 * return rowId
 */
RowId IndexStabilityTestCase::generateRowIdFromSlotNo(uint SlotNo) {
	u64 randomUpper = m_atomic.incrementAndGet();
	return ((RowId)(randomUpper << TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS) | SlotNo);
}

/**
 * ʵ��rowId���ڴ�ѱ�ŵ�ת��
 * @param rowId	�к�ID
 * @return slotNo
 */
uint IndexStabilityTestCase::getSlotNoFromRowId(RowId rowId) {
	u64 mask = (1 << TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS) - 1;
	return (uint)(rowId & mask);
}

/**
 * ��ָ���ڴ���У�����õ�һ����¼�����ұ�֤�Ѿ�������
 * @param session	�Ự���
 * @param memHeap	�ڴ��
 * @param rlh		out �������
 * @param lockMode	����ģʽ
 * @return ��Ч�ļ��������ڴ�Ѽ�¼��������������ɴ�֮����û���ҵ���Ч��¼������NULL
 */
MemHeapRecord* IndexStabilityTestCase::getRandMemRecord(Session *session, MemHeap *memHeap, RowLockHandle **rlh, LockMode lockMode) {
	assert(*rlh == NULL);
	MemHeapRecord *memRec = NULL;
	uint maxTryTimes = 1000;
	do {
		maxTryTimes--;
		if (maxTryTimes == 0)
			return NULL;
		if (*rlh != NULL)
			session->unlockRow(rlh);
		MemHeapRid rid = memHeap->getRandId();
		if (rid == (MemHeapRid)-1)
			continue;
		memRec = memHeap->recordAt(session, rid, rlh, Exclusived);
	} while (memRec == NULL);

	return memRec;
}


/**
 * �����ñ��Ӧ���ڴ������
 * @param maxRecs	�ڴ���������������
 * @param numCkCols	��checksum������
 * @param ckCols	��checksum���к�
 * @return true �����ɹ�/false ����ʧ��
 */
bool ISTableInfo::createHeap(uint maxRecs, u16 numCkCols, u16 *ckCols) {
	if (m_memHeap != NULL)
		return true;
	m_memHeap = new MemHeap(maxRecs, m_tableDef, numCkCols, ckCols);
	return true;
}

/**
 * �����ñ��Ӧ����������
 * @param db	���ݿ����
 * @return true �����ɹ�/ false ����ʧ��
 */
bool ISTableInfo::createIndex() {
	if (m_memHeap == NULL)
		return false;
	if (m_indice != NULL)
		return true;
	char indexpath[255];
	sprintf(indexpath, "%s//%s.nsi", m_db->getConfig()->m_basedir, m_tableDef->m_name);
	if (File::isExist(indexpath)) {
		File ifile(indexpath);
		NTSE_ASSERT(File::getNtseError(ifile.remove()) == File::E_NO_ERROR);
	}
	EXCPT_OPER(DrsIndice::create(indexpath, m_tableDef));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("ISTableInfo::createIndex", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	EXCPT_OPER(m_indice = DrsIndice::open(m_db, session, indexpath, m_tableDef));

	for (uint i = 0; i < m_tableDef->m_numIndice; i++) {
		try {
			//m_indice->createIndexPhaseOne(session, m_tableDef->m_indice[i], NULL);
			m_indice->createIndexPhaseOne(session, m_tableDef->m_indice[i], m_tableDef, NULL);
			m_indice->createIndexPhaseTwo(m_tableDef->m_indice[i]);
		} catch (NtseException) {
			assert(false);
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	return true;
}


/**
 * �õ��������
 * @param weights	���ֲ�����Ȩ������
 * @param opkinds	���ֲ���������
 * @param total		���ֲ���Ȩ�ص��ܺ�
 * @return ���������һ�ֲ���
 */
uint OpGenerator::getOpKind(uint *weights, uint opkinds, uint total) {
	uint op = RandomGen::nextInt(0, total);
	uint kind = 0;
	while (true) {
		assert(op < total);
		if (op >= weights[kind]) {
			op -= weights[kind++];
			continue;
		}

		assert(kind < m_kinds);
		return kind;
	}
}

/**
 * ��������
 */
OpGenerator::~OpGenerator() {
	delete [] m_weights;	
}

/**
 * �ܲ���������
 */
TotalOpGenerator::TotalOpGenerator() {
	m_kinds = 4;
	m_weights = new uint[m_kinds];
	m_weights[0] = IDX_INSERT_WEIGHT;
	m_weights[1] = IDX_DELETE_WEIGHT;
	m_weights[2] = IDX_UPDATE_WEIGHT;
	m_weights[3] = IDX_SCAN_WEIGHT;
}


/**
 * ɾ������������
 */
DeleteOpGenerator::DeleteOpGenerator() {
	m_kinds = 4;
	m_weights = new uint[m_kinds];
	m_weights[0] = IDX_DELETE_FROM_TABLE_WEIGHT;
	m_weights[1] = IDX_DELETE_SINGLE_WEIGHT;
	m_weights[2] = IDX_DELETE_RANGE_FORWARD_WEIGHT;
	m_weights[3] = IDX_DELETE_RANGE_BACKWARD_WEIGHT;
}


/**
 * ���²���������
 */
UpdateOpGenerator::UpdateOpGenerator() {
	m_kinds = 4;
	m_weights = new uint[m_kinds];
	m_weights[0] = IDX_UPDATE_FROM_TABLE_WEIGHT;
	m_weights[1] = IDX_UPDATE_SINGLE_WEIGHT;
	m_weights[2] = IDX_UPDATE_RANGE_FORWARD_WEIGHT;
	m_weights[3] = IDX_UPDATE_RANGE_BACKWARD_WEIGHT;
}

/**
 * ɨ�����������
 */
ScanOpGenerator::ScanOpGenerator() {
	m_kinds = 3;
	m_weights = new uint[m_kinds];
	m_weights[0] = IDX_SCAN_SINGLE_WEIGHT;
	m_weights[1] = IDX_SCAN_RANGE_FORWARD_WEIGHT;
	m_weights[2] = IDX_SCAN_RANGE_BACKWARD_WEIGHT;
}

/**
 * ����ָ��������Ϣ����һ���յ��Ӽ�¼
 * @param tableDef	����
 * @param columns	����Ϣ
 * @param numCols	����
 * @param maxSize	�Ӽ�¼������ݳ���
 * @param format	�Ӽ�¼��ʽ
 * @param rowId		�Ӽ�¼rowId
 * @return ���ص��Ӽ�¼
 */
SubRecord* RecordHelper::generateSubRecord(TableDef *tableDef, u16 *columns, u16 numCols, u16 maxSize, RecFormat format, RowId rowId) {
	ostringstream oss;
	for (uint i = 0; i < numCols - 1; i++)
		oss << columns[i] << " ";
	oss << columns[numCols - 1];

	SubRecordBuilder srb(tableDef, format, rowId);
	return srb.createEmptySbById(maxSize, oss.str().c_str());
}

/**
 * ����һ���ռ�¼
 * @param tableDef	����
 * @param rowId		rowId
 * @return ���ɵĿ�record
 */
Record* RecordHelper::generateRecord(TableDef *tableDef, RowId rowId) {
	RecordBuilder rb(tableDef, rowId, REC_REDUNDANT);
	Record *record = rb.getRecord(tableDef->m_maxRecSize);
	record->m_size = tableDef->m_maxRecSize;
	return record;
}

/**
 * ������ɸ���ǰ������Ӽ�¼
 * @param tableDef	����
 * @param record	��¼
 * @Param before	out ����ǰ��
 * @param after		out ���º���
 */
void RecordHelper::formUpdateSubRecords(TableDef *tableDef, IndexDef *indexDef, Record *record, SubRecord **before, SubRecord **after) {
	Record *old = generateRecord(tableDef, record->m_rowId);
	old->m_size = record->m_size;
	memcpy(old->m_data, record->m_data, record->m_size);
	
	u16 *updateCols = NULL;
	u16 numUpdCols;
	Record *notold = updateRecord(tableDef, record, record->m_rowId, indexDef, &updateCols, &numUpdCols);

	*before = generateSubRecord(tableDef, updateCols, numUpdCols, tableDef->m_maxRecSize, REC_REDUNDANT, record->m_rowId);
	*after = generateSubRecord(tableDef, updateCols, numUpdCols, tableDef->m_maxRecSize, REC_REDUNDANT, record->m_rowId);
	memcpy((*before)->m_data, old->m_data, tableDef->m_maxRecSize);
	memcpy((*after)->m_data, notold->m_data, tableDef->m_maxRecSize);

	delete [] updateCols;
	freeRecord(old);
}

/**
 * ��record�����ָ���Ĳ��Ҽ�
 * @param session	�Ự���
 * @param tableDef	����
 * @param indexDef	��������
 * @param record	�ڴ�Ѽ�¼
 * @return ������Ӽ�¼���Ҽ�
 */
SubRecord* RecordHelper::formFindKeyFromRecord(Session *session, TableDef *tableDef, IndexDef *indexDef, MemHeapRecord *record) {
	Record *rec = RecordHelper::generateRecord(tableDef, record->getRowId());
	record->toRecord(session, rec->m_data);
	SubRecord *findKey = RecordHelper::generateSubRecord(tableDef, indexDef->m_columns, indexDef->m_numCols, indexDef->m_maxKeySize, KEY_PAD);
	RecordOper::extractKeyRP(tableDef, rec, findKey);
	freeRecord(rec);

	return findKey;
}

/**
 * ���»��ߴ�����¼��recΪNULL��Ϊ�����¼�¼��������recΪ�������������¼�¼��rid��ʾrowId
 * @param tableDef		����
 * @param rec			����Ǹ��¼�¼��recΪԭ��¼
 * @param rid			Ҫ���»������ɼ�¼��rowId
 * @param indexDef		��������
 * @param updateCols	out ����Ǹ��²������������ʾ���µ�����Ϣ������ռ�Ϊ������ֶ���������������¼�¼������޸�
 * @param numUpdCols	out ���µ�����
 * @return ���ظ��»��������ɵļ�¼
 */
Record* RecordHelper::updateRecord(TableDef *tableDef, Record *rec, RowId rid, IndexDef *indexDef, u16 **updateCols, u16 *numUpdCols) {
	// �������������������Բ�����ȫ������ϵ����һ����������������ȫ����������һ���������������ԣ���(a, b, c)��(a, c)������
	u16 numCols = tableDef->m_numCols;
	Record *record = rec;
	u16 *candidate = new u16[tableDef->m_numCols];
	u16 *updCols = new u16[tableDef->m_numCols];
	memset(candidate, 0, sizeof(u16) * tableDef->m_numCols);
	memset(updCols, 0, sizeof(u16) * tableDef->m_numCols);
	*numUpdCols = 0;

	// ȷ������һ������Ҫ���µ���������
	u16 mustUpdate = 0;
	for (uint i = 0; i < tableDef->m_numIndice; i++) {
		IndexDef *idxDef = tableDef->m_indice[i];
		for (uint j = 0; j < idxDef->m_numCols; j++) {
			uint k = 0;
			bool canUpdate = true;
			for (; k < indexDef->m_numCols; k++) {
				if (idxDef->m_columns[j] == indexDef->m_columns[k]) {
					canUpdate = false;
					break;
				}
			}
			for (k = 0; k < tableDef->m_pkey->m_numCols; k++) {
				if (idxDef->m_columns[j] == tableDef->m_pkey->m_columns[k]) {
					canUpdate = false;
					break;
				}
			}
			if (canUpdate) {	// ˵���ü�ֵ������ָ�����������У�Ҳ�������������������У����Ա�����
				candidate[idxDef->m_columns[j]] = 1;
				mustUpdate = idxDef->m_columns[j];
			}
		}
	}

	bool updated = false;
	for (uint i = 0; i < numCols; i++) {
		if (candidate[i] == 0)
			continue;
		if (RandomGen::nextInt(0, 2) == 0) {
			if (!(!updated && i == mustUpdate))
				continue;
		}

		updated = true;
		updCols[(*numUpdCols)++] = i;

		ColumnDef *columnDef = tableDef->m_columns[i];
		bool nullable = (RandomGen::nextInt(0, (uint)(10000 / (TestEnvVars::NULL_RATIO * 10000))) == 0);
		nullable |= (columnDef->m_type == CT_SMALLLOB || columnDef->m_type == CT_MEDIUMLOB);
		if (nullable && columnDef->m_nullable) {
			RedRecord::setNull(tableDef, record->m_data, i);
			continue;
		}

		u64 ranINT = RandomGen::nextInt(0, TestEnvVars::BLOG_TABLE_MAX_REC_NUM);
		writeAColumn(tableDef, columnDef, record->m_data, i, ranINT);
	}

	*updateCols = updCols;
	assert(*numUpdCols != 0);

	delete [] candidate;

	return record;
}

/**
 * ��ָ����data��д���µ�ָ���ֶ���Ϣ
 * @param tableDef	����
 * @param columnDef	�ж���
 * @param data		Ҫд����Ϣ��data
 * @param colNo		�����ڱ��ж�Ӧ�����
 * @param ranINT	���������������������Ϣ������
 */
void RecordHelper::writeAColumn(TableDef *tableDef, ColumnDef *columnDef, byte *data, uint colNo, u64 ranINT) {
	switch (columnDef->m_type) {
		case CT_BIGINT:
			{
				u64 value = ranINT;
				RedRecord::writeNumber(tableDef, colNo, data, value);
				break;
			}
		case CT_MEDIUMINT:
			{
				RedRecord::writeMediumInt(tableDef, data, colNo, (int)ranINT);
				break;
			}
		case CT_INT:
			{
				u32 value = (u32)ranINT;
				RedRecord::writeNumber(tableDef, colNo, data, value);
				break;
			}
		case CT_SMALLINT:
			{
				u16 value = (u16)ranINT;
				RedRecord::writeNumber(tableDef, colNo, data, value);
				break;
			}
		case CT_TINYINT:
			{
				u8 value = (u8)ranINT;
				RedRecord::writeNumber(tableDef, colNo, data, value);
				break;
			}
		case CT_VARCHAR:
		case CT_CHAR:
			{
				size_t charLen = (size_t)RandomGen::nextInt(1, columnDef->m_size - columnDef->m_lenBytes);
				byte *value = (byte*)getLongChar(charLen, tableDef, columnDef, (uint)ranINT);
				if (columnDef->m_type == CT_VARCHAR)
					RedRecord::writeVarchar(tableDef, data, colNo, value, charLen);
				else
					RedRecord::writeChar(tableDef, data, colNo, value, charLen);
				delete [] value;
				break;
			}
		default:
			assert(false);
	}
}


/**
 * ����������ȵ�һ�����ַ���
 * @param size		�ַ����ĳ���
 * @param tableDef	����
 * @param columnDef	�ж���
 * @param ranINT	�����ַ���Ψһ��ʾ������
 * @return ���ɵ��ַ���
 */
char* RecordHelper::getLongChar(size_t size, const TableDef *tableDef, const ColumnDef *columnDef, uint ranINT) {
	char fakeChar[255];
	sprintf(fakeChar, "%d %s %s", ranINT, tableDef->m_name, columnDef->m_name);
	size_t len = strlen(fakeChar);
	char *content = new char[size + 1];
	char *cur = content;
	while (size >= len) {
		memcpy(cur, fakeChar, len);
		cur += len;
		size -= len;
	}
	memcpy(cur, fakeChar, size);
	*(cur + size) = '\0';

	return content;
}



/**
 * �漴���ɲ��Ҽ�ֵ,��Ҫ����������һ�����Ե���������
 * @param tableDef	����
 * @param indexDef	��������
 * @return ����������ɵĲ��Ҽ�
 */
SubRecord* RecordHelper::makeRandomFindKey(TableDef *tableDef, IndexDef *indexDef) {
	char colNo[255];
	sprintf(colNo, "%d", *(indexDef->m_columns));

	uint ranINT = RandomGen::nextInt(0, TestEnvVars::BLOG_TABLE_MAX_REC_NUM);
	ColumnDef *columnDef = tableDef->m_columns[*(indexDef->m_columns)];

	SubRecordBuilder keyBuilder(tableDef, KEY_PAD, INVALID_ROW_ID);
	SubRecord *findKey;

	switch (columnDef->m_type) {
		case CT_BIGINT:
			{
				u64 value = ranINT;
				findKey = keyBuilder.createSubRecordById(colNo, &value);
				break;
			}
		case CT_MEDIUMINT:
		case CT_INT:
			{
				u32 value = (u32)ranINT;
				findKey = keyBuilder.createSubRecordById(colNo, &value);
				break;
			}
		case CT_SMALLINT:
			{
				u16 value = (u16)ranINT;
				findKey = keyBuilder.createSubRecordById(colNo, &value);
				break;
			}
		case CT_TINYINT:
			{
				u8 value = (u8)ranINT;
				findKey = keyBuilder.createSubRecordById(colNo, &value);
				break;
			}
		case CT_VARCHAR:
		case CT_CHAR:
			{
				size_t charLen = (size_t)RandomGen::nextInt(1, columnDef->m_size - columnDef->m_lenBytes);
				byte *value = (byte*)RecordHelper::getLongChar(charLen, tableDef, columnDef, (uint)ranINT);
				findKey = keyBuilder.createSubRecordById(colNo, value);
				delete [] value;
				break;
			}
		default:
			assert(false);
	}

	return findKey;
}

/**
 * ����ָ�������rowId�����µı��¼
 * @param tableDef	����
 * @param rid		rowId
 * @return ���ص�record��Ϣ
 */
Record* RecordHelper::generateNewRecord(TableDef *tableDef, RowId rid) {
	u16 numCols = tableDef->m_numCols;
	Record *record = RecordHelper::generateRecord(tableDef, rid);;
	ostringstream oss;

	for (uint i = 0; i < numCols; i++) {
		ColumnDef *columnDef = tableDef->m_columns[i];

		bool nullable = (RandomGen::nextInt(0, (uint)(10000 / (TestEnvVars::NULL_RATIO * 10000))) == 0);
		nullable |= (columnDef->m_type == CT_SMALLLOB || columnDef->m_type == CT_MEDIUMLOB);
		if (nullable && columnDef->m_nullable) {
			RedRecord::setNull(tableDef, record->m_data, i);
			oss << "null ";
			continue;
		}

		u64 ranINT = RandomGen::nextInt(0, TestEnvVars::BLOG_TABLE_MAX_REC_NUM);
		writeAColumn(tableDef, columnDef, record->m_data, i, ranINT);
		oss << ranINT << " ";
	}

	cout << "New Record: " << oss.str().c_str() << endl;

	return record;
}