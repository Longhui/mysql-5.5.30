/**
* MMS稳定性测试
*
* @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
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

// 各类操作权重
const uint MmsSblTestCase::SELECT_WEIGHT = 33;
const uint MmsSblTestCase::DELETE_WEIGHT = 33;
//const uint MmsSblTestCase::GET_BY_RID_WEIGHT = 50;
const uint MmsSblTestCase::UPDATE_WEIGHT_WHEN_SELECT = 90;
const uint MmsSblTestCase::INSERT_MMS_WEIGHT_WHEN_SELECT = 90;
//const uint MmsSblTestCase::PRIMARY_KEY_UPDATE_WEIGHT = 50;  // 实际应用不提供主键更新

// 表数据规模
const uint MmsSblTestCase::MAX_REC_COUNT_IN_TABLE = 1024 * 100;
const uint MmsSblTestCase::TABLE_NUM_IN_SAME_TEMPLATE = 5;

// 测试环境信息
const uint MmsSblTestCase::WORKING_THREAD_NUM = 100;
const uint MmsSblTestCase::VERIFY_DURATION = 5 * 60 * 1000;
//const uint MmsSblTestCase::CHECKPOINT_DURATION = 120 * 1000;
const uint MmsSblTestCase::FOREVER_DURATION = (uint) -1;
const uint MmsSblTestCase::RUN_DURATION = 10 * 60 * 1000;

/** 
 * 获取测试名
 *
 * @return 测试名
 */
const char* MmsSblTestCase::getName() {
	return "Mms Stability Test";
}

/** 
 * 获取测试描述
 *
 * @return 测试描述
 */
const char* MmsSblTestCase::getDescription() {
	return "Stability Test for Mms Module";
}

/**
 * 是否为大测试用例
 *
 * @return 是否为大测试用例
 */
bool MmsSblTestCase::isBig() {
	return true;
}

/** 
 * 开始
 * 步骤:	1. 删除旧数据库，创建新数据库
 *			2. 创建多张定长表和多张变长表
 *			3. 创建验证线程、工作线程和日志监控
 */
void MmsSblTestCase::setUp() {
	// 数据初始化
	m_cfg = new Config();
	Database::drop(m_cfg->m_basedir);
	File dir(m_cfg->m_basedir);
	dir.mkdir();
	NTSE_ASSERT(m_db = Database::open(m_cfg, true));
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::MmsSblTestCase", conn);

	int tableCount = TABLE_NUM_IN_SAME_TEMPLATE;
	m_numTables = 2 * tableCount;
	m_mms = m_db->getMms();  // 全局MMS由m_db提供，其大小由cfg指定

	m_mmsTables = new MmsTable* [m_numTables];
	m_heaps = new DrsHeap* [m_numTables];
	m_tables = new Table* [m_numTables];
	m_memHeaps = new MemHeap* [m_numTables];
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

	// 线程初始化
	// m_timerThread = new MmsSblTimer(this, CHECKPOINT_DURATION);
	m_verifierThread = new MmsSblVerifier(this, VERIFY_DURATION);
	m_nrWorkThreads = WORKING_THREAD_NUM;
	m_workerThreads = new MmsSblWorker *[m_nrWorkThreads];
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsSblWorker(this);

	m_resLogger = new ResLogger(m_db, 1800, "[Mms]DbResource.txt");
}

/** 
 * 结束
 * 步骤：	1. 删除日志监控、停止工作线程和验证线程
 *			2. 删除验证线程和工作线程实例
 *			3. 关闭并删除表和内存堆
 *			4. 删除数据库及其配置
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

	// 释放资源
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::~MmsSblTestCase", conn);
	stringstream name;
	for (int i = 0; i < m_numTables; i++) {
		name.str("");
		name << i;
		delete m_memHeaps[i];
		//m_mmsTables[i]->close(session, true); // 不需要，由Table::close完成
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
 * MMS稳定测试用例入口函数
 * 步骤：	1. 启动工作线程和验证线程
 *			2. 主线程循环等待
 */
void MmsSblTestCase::testMmsStability() {
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
void MmsSblTestCase::startThreads() {
	this->m_beginTime = System::fastTime();
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i]->start();
}

/** 
 * 停止所有工作线程 
 */
void MmsSblTestCase::stopThreads() {
	for (int i = 0; i < m_nrWorkThreads; i++) {
		m_workerThreads[i]->stop();
		m_workerThreads[i]->join();
	}
	this->m_endTime = System::fastTime();
}

/** 
 * 实际工作函数 
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
			assert(false); // 不可能出现
	}
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
void MmsSblTestCase::doVerify() {
	// 暂停所有工作线程
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
					if (!mhRecord->compare(session, &record)) // 单个记录验证正确性
						assert(false);
					verifiedNrRecords++;
					m_mmsTables[i]->unpinRecord(session, mmsRecord);
				}
				session->unlockRow(&rlh);
			}
		}
		if (m_mmsTables[i]->getStatus().m_records != verifiedNrRecords) // 已验证记录数与MMS中记录个数一致
			assert(false);
		delete [] record.m_data;
		nrVictims += m_mmsTables[i]->getStatus().m_pageVictims + m_mmsTables[i]->getStatus().m_recordVictims;
	}

	const MmsStatus& mmsStats = m_db->getMms()->getStatus();
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
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
	cout << "Total Performance(Count/Sec) " << (m_numInsert - m_numPreInsert + m_numDelete - m_numPreDelete + m_numSelect - m_numPreSelect) / (m_endTime - m_beginTime) << endl;

	m_numPreInsert = m_numInsert;
	m_numPreDelete = m_numDelete;
	m_numPreSelect = m_numSelect;
	m_numPreUpdate = m_numUpdate;

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

	m_mmsRecordQueries = mmsStats.m_recordQueries;
	m_mmsRecordQueryHits = mmsStats.m_recordQueryHits;
	m_mmsRecordInserts = mmsStats.m_recordInserts;
	m_mmsRecordDeletes = mmsStats.m_recordDeletes;
	m_mmsRecordUpdates = mmsStats.m_recordUpdates;
	m_mmsRecordVictims = mmsStats.m_recordVictims;
	m_mmsPageVictims = mmsStats.m_pageVictims;


	// 打印锁信息
	RWLockUsage::printAll(cout);
	//for (int i = 0; i < m_numTables; i++) {
	//	cout << "MmsTable " << i << " Table Lock " << endl;
	//	m_mmsTables[i]->getTableLock().getUsage()->printAll(cout); // 打印表锁信息
	//	int nr;
	//	RWLock **locks = m_mmsTables[i]->getPartLocks(&nr);
	//	for (int j = 0; j < nr; j++) {
	//		cout << "MmsTable " << i << " Partition Lock " << j << endl;
	//		locks[j]->getUsage()->printAll(cout);
	//	}
	//}
	
	m_numVerify++;

	// 恢复所有工作线程
	for (int i = 0; i < m_nrWorkThreads; i++)
		m_workerThreads[i] = new MmsSblWorker(this);
	startThreads();
}

/** 
 * 插入一条记录
 * 步骤：	1. 随机选择目标操作表
 *			2. 选择内存堆空闲槽，并根据槽号产生记录主键
 *			3. 生成相应记录
 *			4. 添加记录到Drs堆，并生成RowID
 *			5. 根据RowID, 添加记录到内存堆和MMS表
 */
void MmsSblTestCase::doInsert() {
	RowId rowId;
	u64	id;
	Record *record;
	Record *record2;
	MmsRecord *mmsRecord;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::doInsert", conn, 0);
	if (!session) { // 获取SESSION失败
		m_db->freeConnection(conn);
		return;
	}
	RowLockHandle *rlh = NULL;
	int k = RandomGen::nextInt(0, m_numTables); // 随机选择操作表

	id = (u64)m_memHeaps[k]->reserveRecord();   // 主键由内存堆槽号决定
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
	rowId = m_heaps[k]->insert(session, record, &rlh);  // 插入DRS堆, 记录为变长/定长格式
	assert(rowId != INVALID_ROW_ID);
	record->m_rowId = rowId;
	if (rowId != INVALID_ROW_ID) {
		// 插入内存堆，记录为冗余格式
		if (m_heaps[k]->getTableDef()->m_recFormat == REC_FIXLEN)
			NTSE_ASSERT(m_memHeaps[k]->insertAt(session, (MemHeapRid)id, rowId, record->m_data)); 
		else
			NTSE_ASSERT(m_memHeaps[k]->insertAt(session, (MemHeapRid)id, rowId, record2->m_data)); 
		mmsRecord = m_mmsTables[k]->putIfNotExist(session, record); // 插入MMS堆
		if (mmsRecord) {
			m_mmsTables[k]->unpinRecord(session, mmsRecord);
			session->unlockRow(&rlh);
		} else {
			m_heaps[k]->del(session, rowId); // 从DRS堆中删除记录
			m_memHeaps[k]->deleteRecord(session, (MemHeapRid)id); // 从内存堆中删除记录
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
 * 删除一条记录
 * 步骤：	1. 随机选择操作表
 *			2. 内存堆中获取随机记录项
 *			3. 在MMS表中查询该记录项(根据RowID),如果存在则删除该记录项
 *			4. 删除DRS堆上的记录项
 */
void MmsSblTestCase::doDelete() {
	RowId rowId;
	MmsRecord *mmsRecord;
	MemHeapRid mhRid;
	MemHeapRecord *mhRecord;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::doDelete", conn, 0);
	if (!session) { // 获取SESSION失败
		m_db->freeConnection(conn);
		return;
	}
	RowLockHandle *rlh = NULL;
	int k = RandomGen::nextInt(0, m_numTables); // 选择表

	mhRecord = m_memHeaps[k]->getRandRecord(session, &rlh, Exclusived);
	if (mhRecord) {
		rowId = mhRecord->getRowId();
		mhRid = mhRecord->getId();
		mmsRecord = m_mmsTables[k]->getByRid(session, rowId, false, NULL, None);
		if (mmsRecord)
			m_mmsTables[k]->del(session, mmsRecord); // 从MMS中删除记录
		NTSE_ASSERT(m_heaps[k]->del(session, rowId)); // 从DRS堆中删除记录
		NTSE_ASSERT(m_memHeaps[k]->deleteRecord(session, mhRid)); // 从内存堆中删除记录
		session->unlockRow(&rlh);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_numDelete++;
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
void MmsSblTestCase::doSelect() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("MmsSblTestCase::doSelect", conn, 0);
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
	int k = RandomGen::nextInt(0, m_numTables); // 随机选择操作表
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
	if (mmsRecord) { // 记录存在于MMS中
		record.m_data = new byte[m_tables[k]->getTableDef()->m_maxRecSize];
		m_mmsTables[k]->getRecord(mmsRecord, &record);
		// 比较MMS和内存堆中的内容是否一致
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
		
		if (RandomGen::tossCoin(UPDATE_WEIGHT_WHEN_SELECT)) { // 更新操作
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
	} else { // 记录不存在于MMS中
		record.m_format = tableDef->m_recFormat;
		record.m_data = new byte[tableDef->m_maxRecSize];
		if (m_heaps[k]->getRecord(session, rowId, &record, Exclusived, &rlh)) {
			if (RandomGen::tossCoin(INSERT_MMS_WEIGHT_WHEN_SELECT)) {
				mmsRecord = m_mmsTables[k]->putIfNotExist(session, &record);
				if (mmsRecord)
					m_mmsTables[k]->unpinRecord(session, mmsRecord);
			}
		} else { // 记录不存在
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
 * 获取工作任务类型
 *
 * @return 任务类型 
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

