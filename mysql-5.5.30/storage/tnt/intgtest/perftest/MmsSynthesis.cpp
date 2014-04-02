/**
* MMS性能综合测试
*
* @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
*/

#include "MmsSynthesis.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "Random.h"
#include "AccountTable.h"
#include "CountTable.h"
#include "util/File.h"
#include "btree/IndexBPTree.h"
#include "heap/Heap.h"
#include "misc/Record.h"

using namespace std;

// 各类操作权重
const uint MmsPerfTestCase::UPDATE_IF_EXIST = 33;
const uint MmsPerfTestCase::DELETE_IF_EXIST = 33;
const uint MmsPerfTestCase::GET_IF_EXIST = 34;

const uint MmsPerfTestCase::INSERT_IF_NO_EXIST = 80;
const uint MmsPerfTestCase::NOTHING_IF_NO_EXIST = 20;

// 表数据规模
const uint MmsPerfTestCase::MAX_REC_COUNT_IN_TABLE = 1024 * 100;
const uint MmsPerfTestCase::TABLE_NUM_IN_SAME_TEMPLATE = 5;

// 测试环境信息
const uint MmsPerfTestCase::WORKING_THREAD_NUM = 100;
const uint MmsPerfTestCase::VERIFY_DURATION = 5 * 60 * 1000;
const uint MmsPerfTestCase::FOREVER_DURATION = (uint) -1;
const uint MmsPerfTestCase::RUN_DURATION = 10 * 60 * 60 * 1000;

MmsPerfTestCase::MmsPerfTestCase(bool zipf, bool range) {
	m_zipf = zipf;
	m_range = range;
}

/** 
 * 获取测试名
 *
 * @return 测试名
 */
string MmsPerfTestCase::getName() const {
	return "Mms Performace Test";
}

/** 
 * 获取测试描述
 *
 * @return 测试描述
 */
string MmsPerfTestCase::getDescription() const {
	return "Performance Test for Mms Module";
}

void MmsPerfTestCase::run() {
	this->testMmsPerf();
}

/**
 * 是否为大测试用例
 *
 * @return 是否为大测试用例
 */
bool MmsPerfTestCase::isBig() {
	return true;
}

/** 
 * 开始
 * 步骤:	1. 删除旧数据库，创建新数据库
 *			2. 创建多张定长表和多张变长表
 *			3. 创建验证线程、工作线程和日志监控
 */
void MmsPerfTestCase::setUp() {
	// 数据初始化
	m_cfg = new Config();
	Database::drop(m_cfg->m_basedir);
	File dir(m_cfg->m_basedir);
	dir.mkdir();
	NTSE_ASSERT(NULL != (m_db = Database::open(m_cfg, true)));
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsPerfTestCase::MmsPerfTestCase", conn);

	int tableCount = TABLE_NUM_IN_SAME_TEMPLATE;
	m_numTables = 2 * tableCount;
	m_mms = m_db->getMms();  // 全局MMS由m_db提供，其大小由cfg指定

	m_mmsTables = new MmsTable* [m_numTables];
	m_heaps = new DrsHeap* [m_numTables];
	m_tables = new Table* [m_numTables];
	m_tableDefs = new TableDef* [m_numTables];

	stringstream name;
	for (int i = 0; i < tableCount; i++) { // Count表
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

	// 线程初始化
	m_verifierThread = new MmsPerfVerifier(this, VERIFY_DURATION);
	m_nrWorkThreads = WORKING_THREAD_NUM;
	m_workerThreads = new MmsPerfWorker *[m_nrWorkThreads];
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsPerfWorker(this);
	m_recCounts = new u64[m_numTables];
	m_zipfRandoms = new ZipfRandom* [m_numTables];
}

/**
 *  预热
 */
void MmsPerfTestCase::warm() {
	m_rowIdTables = new RowId* [m_numTables];

	// 装载实际数据到Heap
	Record *record, *record2;
	size_t targetSize = (m_cfg->m_mmsSize + m_cfg->m_pageBufSize) * Limits::PAGE_SIZE * 10 / m_numTables;   // 装载10倍数据
	for (int i = 0; i < m_numTables; i++) {
		if (m_zipf)
			m_rowIdTables[i] = new RowId[targetSize / sizeof (u64)];   // 记录的最小极限情况为一个bigint
		else
			m_rowIdTables[i] = new RowId[MAX_REC_COUNT_IN_TABLE];
	}
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsPerfTestCase::warm", conn);
	for (int k = 0; k < m_numTables; k++) {
		if (m_zipf) {
			m_recCounts[k] = 0;
			m_tables[k]->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
			while (m_tables[k]->getDataLength(session) < targetSize) {
				RowLockHandle *rlh = NULL;
				if (m_heaps[k]->getTableDef()->m_recFormat == REC_FIXLEN) {
					record = CountTable::createRecord(RandomGen::nextInt(), RandomGen::nextInt());
					record->m_format = REC_FIXLEN;
				} else {
					assert(m_heaps[k]->getTableDef()->m_recFormat == REC_VARLEN);
					record2 = AccountTable::createRecord(RandomGen::nextInt());
					record = new Record();
					record->m_size = m_heaps[k]->getTableDef()->m_maxRecSize;
					record->m_data = new byte[record->m_size];
					record->m_format = REC_VARLEN;
					RecordOper::convertRecordRV(m_heaps[k]->getTableDef(), record2, record);
				}
				m_dataVolume += (u64)record->m_size;
				m_rowIdTables[k][m_recCounts[k]++] = m_heaps[k]->insert(session, record, &rlh);  // 插入DRS堆, 记录为变长/定长格式
				session->unlockRow(&rlh);
			}
			m_tables[k]->unlockMeta(session, IL_S);
		} else {
			for (int j = 0; j < MAX_REC_COUNT_IN_TABLE; j++) {
				RowLockHandle *rlh = NULL;
				if (m_heaps[k]->getTableDef()->m_recFormat == REC_FIXLEN) {
					record = CountTable::createRecord(RandomGen::nextInt(), RandomGen::nextInt());
					record->m_format = REC_FIXLEN;
				} else {
					assert(m_heaps[k]->getTableDef()->m_recFormat == REC_VARLEN);
					record2 = AccountTable::createRecord(RandomGen::nextInt());
					record = new Record();
					record->m_size = m_heaps[k]->getTableDef()->m_maxRecSize;
					record->m_data = new byte[record->m_size];
					record->m_format = REC_VARLEN;
					RecordOper::convertRecordRV(m_heaps[k]->getTableDef(), record2, record);
				}
				m_dataVolume += (u64)record->m_size;
				m_rowIdTables[k][j] = m_heaps[k]->insert(session, record, &rlh);  // 插入DRS堆, 记录为变长/定长格式
				session->unlockRow(&rlh);
			}
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	if (m_zipf) {
		for (int i = 0; i < m_numTables; i++)
			m_zipfRandoms[i] = ZipfRandom::createInstVaryScrew(m_recCounts[i], false, 0.5, 2.0, 0.1, 0.99); // 创建Prop(Top10%) = 99% 的Zipf随机对象
	} else {
		for (int i = 0; i < m_numTables; i++)
			m_zipfRandoms[i] = NULL;
	}

	cout << "Total Data Volume is " << m_dataVolume << "bytes" << endl;
}

/** 
 * 结束
 * 步骤：	1. 删除日志监控、停止工作线程和验证线程
 *			2. 删除验证线程和工作线程实例
 *			3. 关闭并删除表和内存堆
 *			4. 删除数据库及其配置
 */
void MmsPerfTestCase::tearDown() {
	stopThreads();
	m_verifierThread->stop();
	m_verifierThread->join();

	delete m_verifierThread;
	m_verifierThread = NULL;
	for (int i = 0; i < m_nrWorkThreads; i++)
		delete m_workerThreads[i];
	delete [] m_workerThreads;
	m_workerThreads = NULL;

	// 释放资源
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsPerfTestCase::~MmsPerfTestCase", conn);
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

void MmsPerfTestCase::testMmsPerf() {
	warm();

	// 线程运行
	startThreads();
	m_verifierThread->start();

	if (RUN_DURATION == FOREVER_DURATION) { // 永远不停止!
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
 * 启动所有工作线程
 */
void MmsPerfTestCase::startThreads() {
	this->m_beginTime = System::fastTime();
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i]->start();
}

/** 
 * 停止所有工作线程 
 */
void MmsPerfTestCase::stopThreads() {
	for (int i = 0; i < m_nrWorkThreads; i++) {
		m_workerThreads[i]->stop();
		m_workerThreads[i]->join();
	}
	this->m_endTime = System::fastTime();
}

/** 
 * 实际工作函数 
 */
void MmsPerfTestCase::doWork() {
	int k = RandomGen::nextInt(0, m_numTables); // 随机选择操作表
	u64 id;
	
	if (m_zipf)
		id = (u64)m_zipfRandoms[k]->rand();
	else
		id = (u64)RandomGen::nextInt(0, MAX_REC_COUNT_IN_TABLE);   // 随机选择ID
	
	if (m_range)
		doSelectEx(k, id);
	else
		doSelect(k, id);
}

/** 
 * 验证函数
 * 步骤:	1. 停止所有工作线程
 *			2. 查询每个内存堆的每个记录项，对于每个非空记录项，执行如下操作
 *			   2.1 获取记录相关RowID
 *			   2.2 根据RowID，查询对应的MMS表，如果获取的MMS记录不为空，
 *				   则比较MMS记录内容和内存堆内容是否一致
 *			3. 判断MMS表已判断记录项个数与MMS表内所有记录项个数是否一致
 *			4. 统计信息打印，重启所有工作线程
 */
void MmsPerfTestCase::doVerify() {
	// 暂停所有工作线程
	stopThreads();
	for (int i = 0; i < m_nrWorkThreads; i++)
		delete m_workerThreads[i];

	const MmsStatus& mmsStats = m_db->getMms()->getStatus();
	// 统计信息打印
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

	// MMS 相关统计
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
	cout << " Avg MMS Query Hit Ratio: " << ((double)mmsStats.m_recordQueryHits / (double)mmsStats.m_recordQueries) * 100.0 << "%" << endl;
	cout << " Curr MMS Query Hit Ratio: " << ((double)(mmsStats.m_recordQueryHits - m_mmsRecordQueryHits)  / (double)(mmsStats.m_recordQueries - m_mmsRecordQueries)) * 100.0 << "%" << endl;

	// BUFFER 相关统计
	cout << "---------------- BUFFER STATS ------------------------------------" << endl;
	m_db->getPageBuffer()->printStatus(cout);

	m_mmsRecordQueries = mmsStats.m_recordQueries;
	m_mmsRecordQueryHits = mmsStats.m_recordQueryHits;
	m_mmsRecordInserts = mmsStats.m_recordInserts;
	m_mmsRecordDeletes = mmsStats.m_recordDeletes;
	m_mmsRecordUpdates = mmsStats.m_recordUpdates;
	m_mmsRecordVictims = mmsStats.m_recordVictims;
	m_mmsPageVictims = mmsStats.m_pageVictims;


	// 打印锁信息
	RWLockUsage::printAll(cout);

	// 冲突率相关统计
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

	// 恢复所有工作线程
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsPerfWorker(this);
	startThreads();
}

void MmsPerfTestCase::doSelectEx(int k, u64 id) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsPerfTestCase::doSelect", conn, 0);
	if (!session) { // 无法获取SESSION
		m_db->freeConnection(conn);
		return;
	}
	RowId rowId = INVALID_ROW_ID;
	SubRecord subRecord;
	Record record;
	bool isCountTbl;
	//TableDef *tableDef;
	u16 cols_count_without_pk[1] = {COUNT_COUNT_CNO};
	u16 cols_account_without_pk[2] = {ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};

	const TableDef *tableDef = m_heaps[k]->getTableDef();
	if (tableDef->m_recFormat == REC_FIXLEN) {
		isCountTbl = true;
	} else {
		assert(tableDef->m_recFormat == REC_VARLEN);
		isCountTbl = false;
	}

	subRecord.m_format = REC_REDUNDANT;
	subRecord.m_numCols = tableDef->m_numCols - 1;
	if (isCountTbl)
		subRecord.m_columns = cols_count_without_pk;
	else
		subRecord.m_columns = cols_account_without_pk;	
	subRecord.m_size = 8192;
	subRecord.m_data = (byte *)malloc(subRecord.m_size);
	SubrecExtractor *extractor = SubrecExtractor::createInst(session->getMemoryContext(), tableDef,
		subRecord.m_numCols, subRecord.m_columns, tableDef->m_recFormat, REC_REDUNDANT, 100);
	for (int i = 0; i < 100 && id + i < m_recCounts[k]; i++) {
		rowId = m_rowIdTables[k][id + i];
		m_mmsTables[k]->getSubRecord(session, rowId, extractor, &subRecord, true, false, 0);
		m_numSelect++;
	}
	free(subRecord.m_data);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
 * 执行Select操作
 * 步骤：	1. 随机选择操作表和MMS表查询类型(主键查询或RowID查询)
 *			2. 在内存堆随机获取一个记录项,并查询MMS表
 *			3. 如果记录在MMS表中
 *				3.1. 比较记录在MMS和内存堆中的一致性
 *				3.2. 随机选择是否需要更新以及更新方式(主键更新、非主键更新)
 *				3.3. 执行相应更新
 *			4. 如果记录不在MMS表中
 *				4.1. 查询相应记录项是否在DRS堆中
 *				4.2. 如果记录在DRS堆中，则根据一定几率添加到MMS中
 */
void MmsPerfTestCase::doSelect(int k, u64 id) {
	int workIfExist, workIfNotExist;
	bool shareMode;
	shareMode = getTaskType(&workIfExist, &workIfNotExist);
	
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsPerfTestCase::doSelect", conn, 0);
	if (!session) { // 无法获取SESSION
		m_db->freeConnection(conn);
		return;
	}
	Record *updRecord;
	RowId rowId = INVALID_ROW_ID;
	RowLockHandle *rlh = NULL;
	SubRecord subRecord;
	Record record;
	MmsRecord *mmsRecord;
	bool isCountTbl;
	//TableDef *tableDef;
	u16 cols_count_without_pk[1] = {COUNT_COUNT_CNO};
	u16 cols_account_without_pk[2] = {ACCOUNT_PASSPORTNAME_CNO, ACCOUNT_USERNAME_CNO};

	const TableDef *tableDef = m_heaps[k]->getTableDef();
	if (tableDef->m_recFormat == REC_FIXLEN) {
		isCountTbl = true;
	} else {
		assert(tableDef->m_recFormat == REC_VARLEN);
		isCountTbl = false;
	}
	rowId = m_rowIdTables[k][id];
	if (shareMode)
		mmsRecord = m_mmsTables[k]->getByRid(session, rowId, true, &rlh, Shared);
	else
		mmsRecord = m_mmsTables[k]->getByRid(session, rowId, true, &rlh, Exclusived);
	m_numSelect++;
	if (mmsRecord) { // 记录存在于MMS中
		if (workIfExist == WORK_UPDATE_IF_EXIST) {
			m_numUpdate++;
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
 * 获取工作任务类型
 *
 * @param workIfExist INOUT 当记录在MMS时的工作任务
 * @param workIfNotExist INOUT 当记录不在MMS时的工作任务
 * @return 是否加共享锁。返回true加共享锁；否则加互斥锁
 */
bool MmsPerfTestCase::getTaskType(int *workIfExist, int *workIfNotExist) {
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

