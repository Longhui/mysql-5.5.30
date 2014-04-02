/**
 *	索引模块稳定性测试实现
 *
 *	@author 苏斌(bsu@corp.netease.com, naturally@163.org)
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


// 各类操作权重
const uint TotalOpGenerator::IDX_INSERT_WEIGHT = 35;
const uint TotalOpGenerator::IDX_DELETE_WEIGHT = 5;
const uint TotalOpGenerator::IDX_UPDATE_WEIGHT = 10;
const uint TotalOpGenerator::IDX_SCAN_WEIGHT = 50;

// 扫描操作权重
const uint ScanOpGenerator::IDX_SCAN_SINGLE_WEIGHT = 20;
const uint ScanOpGenerator::IDX_SCAN_RANGE_FORWARD_WEIGHT = 40;
const uint ScanOpGenerator::IDX_SCAN_RANGE_BACKWARD_WEIGHT = 40;

// 删除操作权重
const uint UpdateOpGenerator::IDX_UPDATE_FROM_TABLE_WEIGHT = 5;
const uint UpdateOpGenerator::IDX_UPDATE_SINGLE_WEIGHT = 55;
const uint UpdateOpGenerator::IDX_UPDATE_RANGE_FORWARD_WEIGHT = 20;
const uint UpdateOpGenerator::IDX_UPDATE_RANGE_BACKWARD_WEIGHT = 20;

// 更新操作权重
const uint DeleteOpGenerator::IDX_DELETE_FROM_TABLE_WEIGHT = 10;
const uint DeleteOpGenerator::IDX_DELETE_SINGLE_WEIGHT = 50;
const uint DeleteOpGenerator::IDX_DELETE_RANGE_FORWARD_WEIGHT = 20;
const uint DeleteOpGenerator::IDX_DELETE_RANGE_BACKWARD_WEIGHT = 20;

// 表数据规模
const uint TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS = 16;
const uint TestEnvVars::BLOG_TABLE_MAX_REC_NUM = (1 << TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS) - 1;

// 测试环境信息
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

	m_tables = 1;	// 暂时只使用Blog表
	m_tableInfo = new ISTableInfo*[m_tables];
	// 创建Blog表信息
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
 * 初始化工作线程等
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
 * 清理环境
 */
void IndexStabilityTestCase::clean() {
	for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++)
		delete m_workingThread[i];

	delete [] m_workingThread;
}

/**
 * 暂停工作线程
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
 * 让工作线程继续
 */
bool IndexStabilityTestCase::resumeWorkingThreads() {
	for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++)
		m_workingThread[i]->resume();

	return true;
}

/**
 * 停止所有工作线程
 */
bool IndexStabilityTestCase::stopWorkingThreads() {
	for (uint i = 0; i < TestEnvVars::WORKING_THREAD_NUM; i++)
		m_workingThread[i]->stop();

	return true;
}

/**
 * 测试过程主体
 */
void IndexStabilityTestCase::testStability() {
	init();

	while (true) {
		Thread::msleep(TestEnvVars::CHECK_DURATION);
		if (!m_keepWorking) {	// 外部指定退出
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
 * 从测试用例得到一个数据库链接
 */
Connection* IndexStabilityTestCase::getAConnection() {
	return m_db->getConnection(false);
}


/**
 * 关闭一个链接
 * @param conn 链接句柄
 */
void IndexStabilityTestCase::closeAConnection(Connection *conn) {
	m_db->freeConnection(conn);
}


/**
 * 对指定的记录进行随机更新，其中要保证一定会导致更新该记录对应的某个索引
 * @param session		会话句柄
 * @param memoryContext	内存上下文
 * @param tableDef		表定义
 * @param indexDef		更新扫描使用的索引定义
 * @param indice		索引对象
 * @param memRec		内存堆记录
 * @param rowId			行ID信息
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
 * 从索引对象中删除指定的索引键值
 * @oaram session		会话句柄
 * @param memoryContext	内存上下文
 * @param tableDef		表定义
 * @param indice		索引对象
 * @param memRec		内存堆记录
 * @param rowId			行ID信息
 * @param handle		索引扫描句柄
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
 * 随机执行各种操作
 * @param conn			链接信息
 * @param isTableInfo	表信息结构
 */
void IndexStabilityTestCase::test(Connection *conn, ISTableInfo *isTableInfo) {
	// 随机执行各种操作
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
 * 打印当前索引状态统计信息到指定文件
 * @param logFile	日志文件名
 */
void IndexStabilityTestCase::printStatus(const char *logFile) {
	u64 errNo;
	File file(logFile);
	if (!File::isExist(logFile)) {	// 已经存在，设置文件长度为0
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

	// 得到时间
	time_t now = time(NULL);
	char timestr[30];
	System::formatTime(timestr, sizeof(timestr), &now);

	// 写入当前各个索引的信息到指定流
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

	// 写入锁表信息到文件
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
 * 验证物理索引和内存堆的一致性
 * @param simple	简单验证只验证个个索引之间项数一致
 * @return true一致/false不一致
 */
bool IndexStabilityTestCase::verify(bool simple) {
	ftrace(ts.rl, tout << "Verify");
	// 遍历各个内存堆,进行双向验证
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

		// 首先确认各个索引的总项数一致
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
			if (indexSingleKeys == 0 && j == 0) {	// 赋初值
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
		// 有了前面的验证,这里只需要验证索引取项的rowId对应memHeap的项存在有效即可,同时总项数应该相等
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
 * 执行内存堆和索引的插入操作
 * @param conn			链接信息
 * @param isTableInfo	表信息结构
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
 * 执行内存堆和索引的更新操作
 * @param conn			链接信息
 * @param isTableInfo	表信息结构
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
	if (updateType == UpdateOpGenerator::UPDATE_FROM_TABLE) {	// 直接在MemHeap随即找一条记录更新
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
 * 执行内存堆和索引的删除操作
 * @param conn			链接信息
 * @param isTableInfo	表信息结构
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
 * 执行内存堆和索引的扫描操作
 * @param conn			链接信息
 * @param isTableInfo	表信息结构
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
			// WHETHER TODO: 检查该项在memHeap存在
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
			// WHETHER TODO: 检查该项在memHeap存在
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
 * 实现内存堆序号到rowId的转换
 * @param slotNo	内存堆的项编号
 * return rowId
 */
RowId IndexStabilityTestCase::generateRowIdFromSlotNo(uint SlotNo) {
	u64 randomUpper = m_atomic.incrementAndGet();
	return ((RowId)(randomUpper << TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS) | SlotNo);
}

/**
 * 实现rowId到内存堆编号的转变
 * @param rowId	行号ID
 * @return slotNo
 */
uint IndexStabilityTestCase::getSlotNoFromRowId(RowId rowId) {
	u64 mask = (1 << TestEnvVars::BLOG_TABLE_MAX_REC_NUM_BITS) - 1;
	return (uint)(rowId & mask);
}

/**
 * 从指定内存堆中，随机得到一条记录，并且保证已经加了锁
 * @param session	会话句柄
 * @param memHeap	内存堆
 * @param rlh		out 行锁句柄
 * @param lockMode	加锁模式
 * @return 有效的加了锁的内存堆记录，如果尝试了若干次之后仍没有找到有效记录，返回NULL
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
 * 创建该表对应的内存堆数据
 * @param maxRecs	内存堆最大容纳数据量
 * @param numCkCols	做checksum的列数
 * @param ckCols	做checksum的列号
 * @return true 创建成功/false 创建失败
 */
bool ISTableInfo::createHeap(uint maxRecs, u16 numCkCols, u16 *ckCols) {
	if (m_memHeap != NULL)
		return true;
	m_memHeap = new MemHeap(maxRecs, m_tableDef, numCkCols, ckCols);
	return true;
}

/**
 * 创建该表对应的索引数据
 * @param db	数据库对象
 * @return true 创建成功/ false 创建失败
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
 * 得到操作序号
 * @param weights	各种操作的权重数组
 * @param opkinds	各种操作的总数
 * @param total		各种操作权重的总和
 * @return 随机产生的一种操作
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
 * 析构函数
 */
OpGenerator::~OpGenerator() {
	delete [] m_weights;	
}

/**
 * 总操作生成器
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
 * 删除操作生成器
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
 * 更新操作生成器
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
 * 扫描操作生成器
 */
ScanOpGenerator::ScanOpGenerator() {
	m_kinds = 3;
	m_weights = new uint[m_kinds];
	m_weights[0] = IDX_SCAN_SINGLE_WEIGHT;
	m_weights[1] = IDX_SCAN_RANGE_FORWARD_WEIGHT;
	m_weights[2] = IDX_SCAN_RANGE_BACKWARD_WEIGHT;
}

/**
 * 根据指定的列信息生成一个空的子记录
 * @param tableDef	表定义
 * @param columns	列信息
 * @param numCols	列数
 * @param maxSize	子记录最大数据长度
 * @param format	子记录格式
 * @param rowId		子记录rowId
 * @return 返回的子记录
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
 * 生成一条空记录
 * @param tableDef	表定义
 * @param rowId		rowId
 * @return 生成的空record
 */
Record* RecordHelper::generateRecord(TableDef *tableDef, RowId rowId) {
	RecordBuilder rb(tableDef, rowId, REC_REDUNDANT);
	Record *record = rb.getRecord(tableDef->m_maxRecSize);
	record->m_size = tableDef->m_maxRecSize;
	return record;
}

/**
 * 随机生成更新前项后项子记录
 * @param tableDef	表定义
 * @param record	记录
 * @Param before	out 更新前项
 * @param after		out 更新后项
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
 * 从record构造出指定的查找键
 * @param session	会话句柄
 * @param tableDef	表定义
 * @param indexDef	索引定义
 * @param record	内存堆记录
 * @return 构造的子记录查找键
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
 * 更新或者创建记录，rec为NULL，为创建新纪录，否则以rec为基础更新生成新纪录，rid表示rowId
 * @param tableDef		表定义
 * @param rec			如果是更新记录，rec为原纪录
 * @param rid			要更新或者生成记录的rowId
 * @param indexDef		索引定义
 * @param updateCols	out 如果是更新操作，该数组表示更新的列信息，分配空间为表最大字段数；如果是生成新纪录，该项不修改
 * @param numUpdCols	out 更新的列数
 * @return 返回更新或者新生成的记录
 */
Record* RecordHelper::updateRecord(TableDef *tableDef, Record *rec, RowId rid, IndexDef *indexDef, u16 **updateCols, u16 *numUpdCols) {
	// 这里假设各个索引的属性不存在全包含关系，即一个索引的所有属性全包含了另外一个索引的所有属性，如(a, b, c)和(a, c)不合理
	u16 numCols = tableDef->m_numCols;
	Record *record = rec;
	u16 *candidate = new u16[tableDef->m_numCols];
	u16 *updCols = new u16[tableDef->m_numCols];
	memset(candidate, 0, sizeof(u16) * tableDef->m_numCols);
	memset(updCols, 0, sizeof(u16) * tableDef->m_numCols);
	*numUpdCols = 0;

	// 确定至少一个必须要更新的索引属性
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
			if (canUpdate) {	// 说明该键值不存在指定的索引当中，也不存在于主键索引当中，可以被更新
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
 * 在指定的data处写入新的指定字段信息
 * @param tableDef	表定义
 * @param columnDef	列定义
 * @param data		要写入信息的data
 * @param colNo		该列在表中对应的序号
 * @param ranINT	随机正数，用来生成新信息的因子
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
 * 返回随机长度的一条长字符串
 * @param size		字符串的长度
 * @param tableDef	表定义
 * @param columnDef	列定义
 * @param ranINT	生成字符串唯一标示的因子
 * @return 生成的字符串
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
 * 随即生成查找键值,主要根据索引第一个属性的内容生成
 * @param tableDef	表定义
 * @param indexDef	索引定义
 * @return 返回随机生成的查找键
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
 * 根据指定表定义和rowId生成新的表记录
 * @param tableDef	表定义
 * @param rid		rowId
 * @return 返回的record信息
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