/**
* MMS测试
*
* @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
*/

#include <iostream>
#include "mms/TestMms.h"
#include "Test.h"
#include "api/Database.h"
#include "api/Table.h"
#include "misc/RecordHelper.h"
#include "util/File.h"
#include "util/Thread.h"
#include "mms/MmsPage.h"
#include "heap/TestFLRHeap.h"
#include <string>

#ifdef NTSE_UNIT_TEST

using namespace std;

#define FIX_TBL_HEAP "fixheap.nsd"
#define FIX_TBL_DEF "fixtbldef.nstd"
#define VAR_TBL_HEAP "varheap.nsd"
#define VAR_TBL_DEF "vartbldef.nstd"
#define HUN_TBL_HEAP "hunheap.nsd"
#define HUN_TBL_DEF "huntbldef.nstd"
#define VAR2_TBL_HEAP "var2heap.nsd"
#define VAR2_TBL_DEF "var2tbldef.nstd"

#define FIX_TBL_HEAP_BACKUP "fixheap.bak"
#define VAR_TBL_HEAP_BACKUP "varheap.bak"
#define HUN_TBL_HEAP_BACKUP	"hunheap.bak"

#define FIX_TBL_TID	1
#define VAR_TBL_TID 2
#define HUN_TBL_TID 3

const char* MmsTestCase::getName() {
	return "Mms test";
}

const char* MmsTestCase::getDescription() {
	return "Test various operations of MMS module";
}

bool MmsTestCase::isBig() {
	return false;
}

void MmsTestCase::setUp() {
	init(100, true, true);
}

void MmsTestCase::tearDown() {
	close();
}

/** 
 * 关闭测试用例
 *
 * @param delHeapFile 是否删除堆文件
 */
void MmsTestCase::close(bool delHeapFile, bool flush) {
	if (m_fMmsTable) {
		EXCPT_OPER(m_fMmsTable->drop(m_session, delHeapFile, flush));
		delete m_fMmsTable;
		m_fMmsTable = NULL;
	}
	if (m_vMmsTable) {
		EXCPT_OPER(m_vMmsTable->drop(m_session, delHeapFile, flush));
		delete m_vMmsTable;
		m_vMmsTable = NULL;
	}
	if (m_vMmsTable2) {
		EXCPT_OPER(m_vMmsTable2->drop(m_session, delHeapFile, flush));
		delete m_vMmsTable2;
		m_vMmsTable2 = NULL;
	}
	if (m_mms) {
		m_pagePool->preDelete();
		EXCPT_OPER(m_mms->close());
		delete m_mms;
		m_mms = NULL;
	}
	if (m_session) {
		m_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}
	if (m_conn) {
		m_db->freeConnection(m_conn);
		m_conn = NULL;
	}
	if (m_db) {
		m_db->close(true, flush);
		if (delHeapFile)
			Database::drop(".");
		delete m_db;
		m_db = NULL;
	}

	if (m_config) {
		delete m_config;
		m_config = NULL;
	}

	if (m_pagePool) {
		delete m_pagePool;
		m_pagePool = NULL;
	}
}

/** 
 * Mms::unregister函数功能测试
 * 测试流程：1. 创建三个MmsTable t1、t2和t3
 *			 2. 在全局MMS中注册t1、t2和t3
 *			 3. 在全局MMS中注销t2和t3
 *			 4. 注：t1的注销在Mms::close阶段完成
 */
void MmsTestCase::testMms() {
	// 创建
	MmsTable *t1 = new MmsTable(m_mms, m_db, m_fMmsTable->getHeap(), m_fMmsTable->getTableDef(), false, 100);
	MmsTable *t2 = new MmsTable(m_mms, m_db, m_fMmsTable->getHeap(), m_fMmsTable->getTableDef(), false, 100);
	MmsTable *t3 = new MmsTable(m_mms, m_db, m_fMmsTable->getHeap(), m_fMmsTable->getTableDef(), false, 100);

	m_mms->registerMmsTable(m_session, t1);
	m_mms->registerMmsTable(m_session, t2);
	m_mms->registerMmsTable(m_session, t3);

	m_mms->unregisterMmsTable(m_session, t2);
	m_mms->unregisterMmsTable(m_session, t3);

	t1->close(m_session, false);
	delete t1;
	t2->close(m_session, false);
	delete t2;
	t3->close(m_session, false);
	delete t3;
}

/** 
 * MmsTable::testPutIfNotExist函数功能测试
 * 测试流程： 1. FTable固定表中插入NR_RECORDS条记录，用于测试put功能
 *			  2. FTable固定表中连续插入NR_RECORDS条记录，用于测试IfNotExist功能
 *			  3. VTable变长表中插入NR_RECORDS条记录
 */
void MmsTestCase::testPutIfNotExist() {
	// 定长记录
	for(int i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);
	}
	
	// 测试putIfNotExist中Exist情况
	m_fMmsTable->insertDouble(m_session, NR_RECORDS, false);

	// 变长记录
	char s[100];

	for (int i = 0; i < NR_RECORDS; i++) {
		sprintf(s, "%d", i);
		m_vMmsTable->insert(m_session, i, s, (int)strlen(s), false);
	}
}

/** 
 * MmsTable::getByRid函数功能测试
 * 测试流程： 1. FTable固定表插入一条记录
 *			  2. RowID查询MmsTable
 *			  3. 根据查询获取的MmsRecord, 构造SubRecord (TODO:比较内容一致性）
 *			  4. unpin记录页，unlock行锁
 */
void MmsTestCase::testGetByRid() {
	MmsRecord *mmsRecord;
	SubRecord subRecord;
	RowLockHandle *rlh = NULL;

	EXCPT_OPER(m_fMmsTable->insert(m_session, 10, false));
	EXCPT_OPER(mmsRecord = m_fMmsTable->selectByRid(m_session, 10, &rlh, Exclusived));
	SubrecExtractor *extractor = SubrecExtractor::createInst(NULL, m_fMmsTable->getTableDef(), subRecord.m_numCols, subRecord.m_columns,
		m_fMmsTable->getTableDef()->m_recFormat, REC_REDUNDANT);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->getSubRecord(mmsRecord, extractor, &subRecord));
	delete extractor;
	EXCPT_OPER(m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord));
	EXCPT_OPER(m_session->unlockRow(&rlh));
}

/** 
 * getSubRecord函数功能测试
 */
void MmsTestCase::testGetSubRecord() {
	SubRecord subRecord;
	RowId rid = 10;

	subRecord.m_format = REC_REDUNDANT;
	subRecord.m_size = Limits::PAGE_SIZE;
	subRecord.m_data = (byte *)malloc(subRecord.m_size);
	subRecord.m_numCols = 2;
	subRecord.m_columns = (u16 *)malloc(sizeof(u16) * subRecord.m_numCols);
	for (int i = 0; i < subRecord.m_numCols; i++)
		subRecord.m_columns[i] = i;
	m_fMmsTable->insert(m_session, rid, false);
	m_fMmsTable->getMmsTable()->getTableLock();
	SubrecExtractor *extractor = SubrecExtractor::createInst(NULL, m_fMmsTable->getTableDef(),
		subRecord.m_numCols, subRecord.m_columns, m_fMmsTable->getTableDef()->m_recFormat, REC_REDUNDANT);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid + 1, extractor, &subRecord, true, false, 0));
	CPPUNIT_ASSERT(m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid, extractor, &subRecord, true, true, 0));
	CPPUNIT_ASSERT(m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid, extractor, &subRecord, true, false, 0));
	free(subRecord.m_data);
	free(subRecord.m_columns);
	delete extractor;
}

/** 
 *  MmsTable::getRecord函数功能测试
 */
void MmsTestCase::testGetRecord() {
	m_fMmsTable->getRecord(m_session, 1, true);
	testGetSubRecord();
	m_fMmsTable->getMmsTable()->setCacheUpdate(true);
	m_fMmsTable->getMmsTable()->setMaxRecordCount(100);
	m_mms->printStatus(cout);
}

/** 
 * MmsTable::update函数功能测试
 */
void MmsTestCase::testUpdate() {
	doTestUpdate(true);
}

/** 
 * update函数测试功能实现
 * 测试流程： 1. 在FTable定长表中插入count条记录，每条记录更新step次
 *			  2. 在VTable变长表中插入count/2条记录，每条记录更新101次 (touch参数为TRUE)
 *			  3. 在VTable变长表中插入count/2条记录，每条记录更新101次 (touch参数为FALSE)
 *
 * @param insertHeap 是否在堆上插入数据 (在部分MMS测试用例中，可以不往堆中写入数据)
 * @param count	更新数
 */
void MmsTestCase::doTestUpdate(bool insertHeap, int count) {
	const int step = 10;
	RowLockHandle *rlh = NULL;
	RowId rowId;
	int i, j;
	
	for (i = 0; i < count; i+=step) {
		EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, i, insertHeap));
		for (j = i; j < i + step; j++) {
			EXCPT_OPER(m_fMmsTable->update(m_session, rowId, j));
		}
	}

	// 变长记录
	char s[1024];

	for (i = 0; i < count / 2; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 10; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s);
	}

	for (; i < count; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 10; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s, false);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s, false);
	}
}

void MmsTestCase::testHeapMove() {
	RowId fid[500];
	RowId vid[500];

	fid[0] = m_vMmsTable->insert(m_session, 0, "a", 1, true);
	Thread::msleep(3000);  // 等待3秒
	for (int i = 0; i < 1; i++) {
		char name[40];
		for (int j = 0; j < 40; j++)
			name[j] = 'a';
		name[39] = '\0';
		vid[i] = m_vMmsTable->insert(m_session, i, name, 40, true);
	}
	for (int i = 0; i < 1; i++) {
		char name[80];
		for (int j = 0; j < 80; j++)
			name[j] = 'a';
		name[79] = '\0';
		vid[i] = m_vMmsTable->insert(m_session, i, name, 80, true);
	}
	for (int i = 0; i < 1; i++) {
		char name[160];
		for (int j = 0; j < 160; j++)
			name[j] = 'a';
		name[159] = '\0';
		vid[i] = m_vMmsTable->insert(m_session, i, name, 160, true);
	}
	for (int i = 1; i < 500; i++)
		fid[i] = m_vMmsTable->insert(m_session, i, "a", 1, true);
	Thread::msleep(3000);  // 等待3秒
	MmsRecord *mmsRecord;
	for (int i = 0; i < 500; i++) {
		mmsRecord = m_vMmsTable->getMmsTable()->getByRid(m_session, fid[i], true, NULL, None);
		m_vMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord);
	}
}

void MmsTestCase::doTestUpdate_simple(bool insertHeap, int count) {
	const int step = 10;
	RowLockHandle *rlh = NULL;
	RowId rowId;
	int i, j;

	for (i = 0; i < count; i+=step) {
		EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, i, insertHeap));
		for (j = i; j < i + step; j++) {
			EXCPT_OPER(m_fMmsTable->update(m_session, rowId, j));
		}
	}

	// 变长记录
	char s[1024];

	for (i = 0; i < count / 2; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 2; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s);
	}

	for (; i < count; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), insertHeap));
		for (j = 0; j < 2; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->update(m_session, rowId, s, false);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->update(m_session, rowId, s, false);
	}
}


/** 
 * update函数测试扩展功能实现
 * 测试流程：1. VTable变长表中插入100条记录
 *			 2. 每条记录更新101次（touch参数为FALSE)
 */
void MmsTestCase::testUpdateEx() {
	RowId rid[100];
	char s[1024];

	for (int i = 0; i < 100; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rid[i] = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true));
	}

	for (int i = 0; i < 100; i++) {
		sprintf(s, "%d", i);
		for (int j = 0; j < 100; j++)
			sprintf(s, "%d%s", i, s);
		m_vMmsTable->update(m_session, rid[i], s, false);
	}
}

/** 
 * FreqHeap测试
 * 测试流程：1. 在VTable变长表中插入90条记录(同一个RpClass)
 *		     2. 再插入2条记录（另一个RpClass）
 *			 3. RowID查询第一个RpClass级别中的记录项
 */
void MmsTestCase::testFreqHeap() {
	char s[1024];
	RowId rid[100];
	MmsRecord *mmsRecord;

	// 一个RPCLASS
	for (int i = 10; i < 100; i++) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rid[i] = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true)); 
	}

	// 另一个RPCLASS
	for (int i = 100; i < 102; i++) {
		sprintf(s, "%d", i);
		for (int j = 0; j < 50; j++) {
			sprintf(s, "%d%s", i, s);
		}
		EXCPT_OPER(m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true)); 
	}

	for (int i = 10; i < 100; i++) {
		EXCPT_OPER(mmsRecord = m_vMmsTable->selectByRid(m_session, rid[i], NULL, None));
		CPPUNIT_ASSERT(mmsRecord);
		m_vMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord);
	}
}

/** 
 * 测试MMS记录删除功能及get相关功能
 * 测试流程：(I)  定长记录项测试
 *			 1. 定长表中插入NR_RECORDS条记录
 *			 2. 查询MMS定长表的当前记录数
 *			 3. 查询(使用getByRid)并删除定长表中50%记录项, 每次删除后进行是否成功删除判断(使用getByPK)
 *			 4. 查询(使用getByPK)并删除定长表中另外50%记录项, 每次删除后进行是否成功删除判断(使用getByRid)
 *			 5. 查询MMS定长表的当前记录数和删除的记录数，并进行判断
 *			 (II) 变长记录项测试
 *			 1. 变长表中插入NR_RECORDS条记录
 *			 2. 查询MMS变长表的当前记录数
 *			 3. 查询(使用getByRid)并删除变长表中50%记录项, 每次删除后进行是否成功删除判断(使用getByPK)
 *			 4. 查询(使用getByPK)并删除变长表中另外50%记录项, 每次删除后进行是否成功删除判断(使用getByRid)
 *			 5. 查询MMS变长表的当前记录数和删除的记录数，并进行判断
 *			 (III) 定长记录项get函数Touch功能测试
 *			 1. 定长表中插入NR_RECORDS条记录
 *			 2. 查询(使用getByRid，touch参数为false)并删除定长表中所有记录项。
 */
void MmsTestCase::testDel() {
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;
	u64 numDels = 0;
	u64 currMmsRecords = 0;

	// 定长表测试
	for(u64 i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);	
	}

	currMmsRecords = m_fMmsTable->getMmsTable()->getStatus().m_records;
	for (u64 j = 0; j < NR_RECORDS; j++) {
		EXCPT_OPER(mmsRecord = m_fMmsTable->selectByRid(m_session, j, &rlh, Exclusived));
		if (mmsRecord) {
			// TODO：在调用mmsTable.del之前，是否必须加记录锁
			EXCPT_OPER(m_session->unlockRow(&rlh));
			EXCPT_OPER(m_fMmsTable->getMmsTable()->del(m_session, mmsRecord));
			numDels++;
			// CPPUNIT_ASSERT(!m_fMmsTable->selectByPK(m_session, j, &rlh, Exclusived));
		}
	}
	CPPUNIT_ASSERT(numDels == currMmsRecords);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getStatus().m_records);

	// 变长记录测试
	char s[100];
	
	numDels = 0;
	for (u64 i = 0; i < NR_RECORDS; i++) {
		sprintf(s, "%d", i);
		m_vMmsTable->insert(m_session, i, s, (int)strlen(s), false);
	}
	currMmsRecords = m_vMmsTable->getMmsTable()->getStatus().m_records;
	
	for (u64 j = NR_RECORDS - 1; (int)j >= 0; j--) {
		EXCPT_OPER(mmsRecord = m_vMmsTable->selectByRid(m_session, j, &rlh, Exclusived));
		if (mmsRecord) {
			EXCPT_OPER(m_session->unlockRow(&rlh));
			EXCPT_OPER(m_vMmsTable->getMmsTable()->del(m_session, mmsRecord));
			numDels++;
		}
	}
	CPPUNIT_ASSERT(numDels == currMmsRecords);
	CPPUNIT_ASSERT(!m_vMmsTable->getMmsTable()->getStatus().m_records);

	for(u64 i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);	
	}

	currMmsRecords = m_fMmsTable->getMmsTable()->getStatus().m_records;
	for (u64 j = 0; j < NR_RECORDS; j++) {
		EXCPT_OPER(mmsRecord = m_fMmsTable->getMmsTable()->getByRid(m_session, j, false, &rlh, Exclusived));
		if (mmsRecord) {
			// TODO：在调用mmsTable.del之前，是否必须加记录锁
			EXCPT_OPER(m_session->unlockRow(&rlh));
			EXCPT_OPER(m_fMmsTable->getMmsTable()->del(m_session, mmsRecord));
			numDels++;
		}
	}
}

/** 
 * 测试MMS检查点刷新功能
 * 测试流程： 1. 调用doTestUpdate函数，进行一组MMS更新操作
 *			  2. 最大缓冲区脏记录项设为50(刷写时,在缓冲区进行sortAndFlush)
 *			  3. 定长表检查点刷新
 *			  4. 变长表检查点刷新
 */
void MmsTestCase::testCheckPointFlush() {
	doTestUpdate(true, 100);
	assert (m_mms->getMaxNrDirtyRecs() > 0);
	m_mms->setMaxNrDirtyRecs(6);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
	EXCPT_OPER(m_vMmsTable->getMmsTable()->flush(m_session, true));
}

/** 
 * 各种设置下的更新功能
 * 测试流程：1. MMS设置为不缓存更新
 *			 2. 执行doTestUpdate操作 (用于检验在不缓存更新状态下，MMS更新功能)
 *			 3. MMS设置为更新缓存并且为所有列更新缓存
 *			 4. 执行doTestUpdate操作 (用于检验在所有列更新缓存状态下，MMS更新功能)
 */
void MmsTestCase::testUpdateWhenVariousSet() {
	if (!isEssentialOnly()) {
		close();
		
		init(1000, false, false);
		doTestUpdate(true);
		close();

		init(1000, true, true);
		doTestUpdate(true);
		close();
	}
}

/** 
 * 测试获取RpClass记录边界大小的对外接口函数
 */
void MmsTestCase::testRpClassRecSize() {
	RowId rid = 1;

	m_fMmsTable->insert(m_session, rid, false);
	MmsTable *mmsTable = m_fMmsTable->getMmsTable();
	int nrRPClasses;
	MmsRPClass** rpClasses = mmsTable->getMmsRPClass(&nrRPClasses);
	u16 sizeMax, sizeMin;

	for (int i = 0; i < nrRPClasses; i++) {
		if (rpClasses[i]) {
			sizeMax = rpClasses[i]->getMaxRecSize();
			sizeMin = rpClasses[i]->getMinRecSize();
			break;
		}
	}
	MmsRecord *mmsRecord = mmsTable->getByRid(m_session, rid, true, NULL, None);
	CPPUNIT_ASSERT(mmsRecord->m_size + sizeof(MmsRecord) <= sizeMax);
	CPPUNIT_ASSERT(mmsRecord->m_size + sizeof(MmsRecord) >= sizeMin);
	mmsTable->unpinRecord(m_session, mmsRecord);
}

/** 
 * 测试Mms::freeSomePages()函数功能
 * 测试流程： 1. 调用testDel, 执行一组删除操作
 *			  2. 调用Mms::freeSomePages, 释放5个记录页
 *			  3. 调用testPutIfNotExist, 执行一组插入操作
 *			  4. 调用Mms::freeSomePages, 再次释放5个记录页
 */
void MmsTestCase::testFreeSomePages() {
	testDel();
	m_mms->freeSomePages(0, 5);
	testPutIfNotExist();
	m_mms->freeSomePages(0, 5);
}

/** REDO测试类型定义 */
enum MMS_REDO_TEST_TYPE {
	MMS_REDO_TEST_SINGLE_FIX = 1,    /** 定长记录单日志 */
	MMS_REDO_TEST_MULT_FIX,          /** 定长记录多日志 */
	MMS_REDO_TEST_MULT_VAR           /** 变长记录多日志 */
};

/** 
 * 测试MMS恢复功能
 * 测试流程： 1. 定长记录单日志测试
 *			  2. 定长记录多日志测试
 *			  3. 变长记录多日志测试
 */
void MmsTestCase::testRecover() {
	//testRedoUpdate(MMS_REDO_TEST_SINGLE_FIX); 去掉single fix	
	testRedoUpdate(MMS_REDO_TEST_MULT_FIX);
	testRedoUpdate(MMS_REDO_TEST_MULT_VAR);
}

/** 
 * 测试MMS恢复功能
 * 测试流程：1. 根据不同类型参数，构建MMS内容
 *			 2. 创建崩溃点
 *		     3. 根据日志文件，调用Mms::redoUpdate重构MMS内容
 *			 4. 判断恢复正确性
 *
 * @param cond 测试类型
 */
void MmsTestCase::testRedoUpdate(int cond) {
	const int step = 10;
	RowLockHandle *rlh = NULL;
	RowId rowId;
	RowId rowIds[100];

	// 构建内容
	if (MMS_REDO_TEST_SINGLE_FIX == cond) {
		for (int i = 0; i < 99; i+=step) {
			EXCPT_OPER(rowIds[i] = m_fMmsTable->insertReal(m_session, i, true));
			for (int j = i; j < i + step; j++) {
				EXCPT_OPER(m_fMmsTable->updateReal(m_session, rowIds[i], j));
			}
		}
	} else if (MMS_REDO_TEST_MULT_FIX == cond) {
		close();
		init(100, false, false);
		EXCPT_OPER(rowId = m_fMmsTable->insertReal(m_session, 0, true));
		EXCPT_OPER(m_fMmsTable->updateReal(m_session, rowId, 1));
		EXCPT_OPER(m_fMmsTable->updateRealEx(m_session, rowId, 2));
	} else if (MMS_REDO_TEST_MULT_VAR == cond) {
		char s[1024];
		
		close();
		init(100, false, false);
		int i = 0;
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insertReal(m_session, i, s, (int)strlen(s), true));
		for (int j = 0; j < 10; j++) {
			sprintf(s, "%d%s", i, s);
			m_vMmsTable->updateReal(m_session, rowId, s);
		}
		sprintf(s, "%d", i);
		m_vMmsTable->updateReal(m_session, rowId, s);
	}

	// 创建崩溃点
	m_fMmsTable->getHeap()->getBuffer()->flushAll();
	doBackupFile(m_fMmsTable->getHeap()->getHeapFile(), FIX_TBL_HEAP_BACKUP);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
	close(false, false);

	// 恢复
	doRestoreFile(FIX_TBL_HEAP_BACKUP, FIX_TBL_HEAP);
	init(1000, true, true, true, false);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);   // 模拟REDO阶段的间隔刷新

	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		const LogEntry *le = logHdl->logEntry();
		switch (le->m_logType) {
			case LOG_MMS_UPDATE:
				if (le->m_tableId == FIX_TBL_TID)
					m_fMmsTable->getMmsTable()->redoUpdate(m_session, le->m_data, (uint)le->m_size);
				else if (le->m_tableId == VAR_TBL_TID)
					m_vMmsTable->getMmsTable()->redoUpdate(m_session, le->m_data, (uint)le->m_size);
				break;
			default:
				break;
		}
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_mms->endRedo();

	// 判断
	if (MMS_REDO_TEST_SINGLE_FIX == cond) {
		for (int i = 0; i < 99; i+=step) {
			u64 j = i + step - 1;
			CPPUNIT_ASSERT(NULL != m_fMmsTable->selectByRid(m_session, rowIds[i], NULL, None));
		}
	} else if (MMS_REDO_TEST_MULT_FIX == cond) {
		CPPUNIT_ASSERT(NULL != m_fMmsTable->selectByRid(m_session, rowId, NULL, None));
	} else if (MMS_REDO_TEST_MULT_VAR == cond) {
		CPPUNIT_ASSERT(NULL != m_vMmsTable->selectByRid(m_session, rowId, NULL, None));
	}
}

/** 
 * 构造备份文件 (调用TestFLRHeap::backupHeapFile实现)
 *
 * @param file 文件句柄
 * @param backupName 备份文件名
 */
void MmsTestCase::doBackupFile(File *file, const char *backupName) {
	backupHeapFile(file, backupName);
}

/** 
 * Restore文件 (调用TestFLRHeap::restoreHeapFile实现)
 *
 * @param backupFile 备份文件
 * @param origFile 原始文件
 */
void MmsTestCase::doRestoreFile(const char *backupFile, const char *origFile) {
	restoreHeapFile(backupFile, origFile);
}

/** 
 * 测试MMS内部替换功能
 * 测试功能： 1. MMS设置为所有更新列缓存
 *			  2. 定长表中插入1000条记录
 *			  3. 变长表中插入100条记录
 */
void MmsTestCase::testReplacement() {
	this->close();
	
	this->init(10, true, true);
	for (int i = 0; i < 1000; i++)
		m_fMmsTable->insert(m_session, i, true);

	char username[1000];
	for (int i = 0; i < 100; i++) {
		sprintf(username, "%d", i);
		m_vMmsTable->insert(m_session, i, username, (int)strlen(username), true);
	}

	testReplaceThread();

	testFPage();

	testAllocFail();
}

/** 
 * 测试MMS内部替换线程功能
 */
void MmsTestCase::testReplaceThread() {
	this->close();
	this->init(10, true, true);
	for (int i = 0; i < 1000; i++)
		m_fMmsTable->insert(m_session, i, true);
	char username[1000];
	for (int i = 0; i < 100; i++) {
		sprintf(username, "%d", i);
		m_vMmsTable->insert(m_session, i, username, (int)strlen(username), true);
	}
	m_mms->runReplacerForce();
}

void MmsTestCase::testUpdateWhenAllocFail() {
	if (isEssentialOnly())
		return;

	close();
	init(10, true, true);
	// 插入一个变长表记录
	char username[1000];
	username[0] = 'a';
	username[1] = '\0';

	RowId rid = m_vMmsTable->insertReal(m_session, 0, username, (int)strlen(username), true);
	
	for (int i = 0; i < 1000; i++)
		if ((RowId)-1 == m_fMmsTable->insertEx(m_session, i, true))
			break;

	char *updateName = new char[100];
	memset(updateName, 1, 99);
	*(updateName + 99) = '\0';
	const char *cols = "1";
	SubRecord *subRecord;
	SubRecordBuilder srb(m_vMmsTable->getTableDef(), REC_REDUNDANT, rid);

	subRecord = srb.createSubRecordById(cols, (const char *)updateName);
	
	MmsRecord *mmsRecord = m_vMmsTable->getMmsTable()->getByRid(m_session, rid, true, NULL, None);
	MmsTable *mmsTable = m_vMmsTable->getMmsTable();
	u16 recSize;
	if (mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		mmsTable->update(m_session, mmsRecord, subRecord, recSize);
	else
		mmsTable->flushAndDel(m_session, mmsRecord);
	delete [] updateName;
	freeSubRecord(subRecord);
}

void MmsTestCase::testRidHashConflict() {
	for(u64 i = 0; i < NR_RECORDS; i++) {
		m_fMmsTable->insert(m_session, i, false);	
	}

	MmsRecord *mmsRecord;
	for (u64 j = 0; j < NR_RECORDS; j++) {
		EXCPT_OPER(mmsRecord = m_fMmsTable->selectByRid(m_session, j, NULL, None));
		m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord);
	}

	int numParts = m_fMmsTable->getMmsTable()->getPartitionNumber();
	double avgConflictLen;
	size_t maxConflictLen;
	for (int i = 0; i < numParts; i++) {
		m_fMmsTable->getMmsTable()->getRidHashConflictStatus(i, &avgConflictLen, &maxConflictLen);
	}
}

/** 
 * MMS内部初始化
 * 
 * @param targetSize	目标大小
 * @param cacheUpdate	缓存更新
 * @param colUpdate		字段更新
 */
void MmsTestCase::init(uint targetSize, bool cacheUpdate, bool colUpdate, bool needRedo, bool delHeapFile, bool varcharPK, bool partialCached, int partitionNr) {
	if (delHeapFile) {
		Database::drop(".");
	}

	m_config = new Config();
	EXCPT_OPER(m_db = Database::open(m_config, true, -1));

	// 创建数据库
	/*TableDefBuilder *builder;*/

	// 创建回话对象
	m_conn = m_db->getConnection(true);
	m_session = m_db->getSessionManager()->allocSession("MmsTestCase::init", m_conn, false);

	m_pagePool = new PagePool(m_config->m_maxSessions + 1, Limits::PAGE_SIZE);
	
	// 创建全局MMS
	m_mms = new Mms (m_db, targetSize, m_pagePool, needRedo);
	m_pagePool->registerUser(m_mms);
	m_pagePool->init();

	// 创建MMS表
	m_fMmsTable = new FMmsTable();
	m_fMmsTable->create(m_session, m_db, m_mms, FIX_TBL_TID, "MmsTest", "FTable", "Mms", 3, cacheUpdate, colUpdate, delHeapFile, partialCached, partitionNr);
	m_vMmsTable = new VMmsTable();
	m_vMmsTable->create(m_session, m_db, m_mms, VAR_TBL_TID, "MmsTest", "VTest", /*VAR_TBL_HEAP, */Limits::MAX_REC_SIZE /3, cacheUpdate, colUpdate, varcharPK, partialCached, delHeapFile, partitionNr);
	m_vMmsTable2 = NULL;
}

void MmsTestCase::testDelWhenDoTouch() {
	MmsTester tester(this, MTT_DEL_DOTOUCH);
	MmsTestHelper helper(this, &tester, MTT_DEL_DOTOUCH);

	tester.enableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.enableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	tester.start();
	helper.start();

	helper.join();
	tester.join();
	tester.disableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.disableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}

void MmsTestCase::doTestDelWhenTouch(MmsTester *tester) {
	m_fMmsTable->insert(m_session, 1, false);
	Thread::msleep(3000);  // 等待3秒
	m_fMmsTable->insert(m_session, 1, false);
}

void MmsTestCase::doHelpTestDelWhenTouch(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_DOTOUCH_LOCK);
	m_fMmsTable->getMmsTable()->lockMmsTable(0);
	tester->disableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_LOCK);
	

	tester->joinSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	m_fMmsTable->getMmsTable()->unlockMmsTable(0);
	m_fMmsTable->getMmsTable()->delCurrRecord();
	tester->disableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}



/** 测试MmsTable::doTouch部分代码
 *  测试流程： 1. 启动doTouch相关同步点
 *			   2. 创建tester线程(run正常流程）
 *                和helper线程（run同步点阻塞流程)
 *			   3. 父线程等待子线程执行完毕
 *			   4. 取消同步点
 */
void MmsTestCase::testDoTouch() {
	MmsTester tester(this, MTT_DOTOUCH);
	MmsTestHelper helper(this, &tester, MTT_DOTOUCH);
	
	tester.enableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.enableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	tester.start();
	helper.start();
	
	helper.join();
	tester.join();
	tester.disableSyncPoint(SP_MMS_DOTOUCH_LOCK);
	tester.disableSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}

void MmsTestCase::testAllocFail() {
	close();
	init(5, true, true, false, true, true, false, 1);

	for (int i = 0; i < 400; i++)
		m_fMmsTable->insert(m_session, i, true);
	m_vMmsTable->insert(m_session, 0, "0", sizeof("0"), true);
}

/** 
 * 测试doTouch同步点功能（被MmsTester::run调用)
 * 测试流程： 1. 定长表中插入一条记录
 *			  2. 根据RowID在MMS中查询该记录
 *			  3. 判断记录是否存在
 *			  4. unpin MMS记录项
 *
 * @param tester 调用者指针
 */
void MmsTestCase::doTestTouch(MmsTester *tester) {
	MmsRecord *mmsRecord;

	m_fMmsTable->insert(m_session, 1, false);
	Thread::msleep(3000);  // 等待3秒
	EXCPT_OPER(mmsRecord = m_fMmsTable->getMmsTable()->getByRid(m_session, 1, true, NULL, Exclusived));
	CPPUNIT_ASSERT(mmsRecord);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord));
}

/** 
 * 测试doTouch同步点功能 (被MmsHelper::run调用)
 * 测试流程： 1. 等待doTouch加锁同步点
 *			  2. 加相应的MMS表锁
 *			  3. 通知等待线程
 *			  4. 等待doTouch解锁同步点
 *			  5. 释放已加的MMS表锁
 *			  6. 通知等待线程
 *
 * 测试目的： 在doTouch中制造一个无法获取MMS表锁的情景
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doHelpTestTouch(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_DOTOUCH_LOCK);
	m_fMmsTable->getMmsTable()->lockMmsTable(0);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_LOCK);
	
	tester->joinSyncPoint(SP_MMS_DOTOUCH_UNLOCK);
	m_fMmsTable->getMmsTable()->unlockMmsTable(0);
	tester->notifySyncPoint(SP_MMS_DOTOUCH_UNLOCK);
}

/** 
 * 测试MmsTable::getByRid和MmsTable::getByPrimaryKey （当其他线程LockRow时)
 */
void MmsTestCase::testGetRidWhenLockRow() {
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_0);
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_1);
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_2);
	testGetRidWhenLockRow(MTT_GETRID_WHEN_LOCKROW_3);
}

/** 
 * MMS中LockRow并发测试
 * 测试流程： 1. 定长表插入一条记录
 *			  2. 开启LockRow同步点
 *			  3. 创建tester和helper两个子线程
 *			  4. 运行子线程并等待
 *			  5. 关闭LockRow同步点
 *
 * @param task 测试任务号
 */
void MmsTestCase::testGetRidWhenLockRow(MmsThreadTask task) {
	// 添加一条记录, RowID为1
	m_fMmsTable->insert(m_session, 1, false);

	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	tester.enableSyncPoint(SP_MMS_RID_LOCKROW);
	tester.enableSyncPoint(SP_MMS_RID_UNLOCKROW);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	tester.disableSyncPoint(SP_MMS_RID_LOCKROW);
	tester.disableSyncPoint(SP_MMS_RID_UNLOCKROW);
}

/** 
 * LockRow并发测试主流程函数 (被MmsTester::run调用)
 * 测试流程：  1. 根据RowID获取MMS记录项
 *			   2. 如果存在则unpin该记录项并unlockRow
 *
 * @param tester 调用者指针
 */
void MmsTestCase::doGetRidWhenLockRow(MmsTester *tester) {
	RowLockHandle *rlh = NULL;
	MmsRecord *mmsRecord;

	EXCPT_OPER(mmsRecord = m_fMmsTable->getMmsTable()->getByRid(m_session, 1, true, &rlh, Shared));
	if (mmsRecord) {
		EXCPT_OPER(m_fMmsTable->getMmsTable()->unpinRecord(m_session, mmsRecord));
		EXCPT_OPER(m_session->unlockRow(&rlh));
	}
}

/** 
 * LockRow并发测试同步点流程函数 (被MmsHelper::run调用)
 * 测试流程：1. 等待LockRow加锁同步点
 *			 2. 对MMS正在处理的记录加行锁
 *			 3. 通知等待线程
 *			 4.1)  任务MTT_GETRID_WHEN_LOCKROW_1，修改页版本号
 *			 4.2)  任务MTT_GETRID_WHEN_LOCKROW_2，修改页版本号和m_valid值（通过删除一个记录项方式)
 *			 4.3)  任务MTT_GETRID_WHEN_LOCKROW_3，修改记录RowID值
 *			 5. 等待解锁同步点
 *			 6. 解行锁
 *			 7. 通知等待线程
 *
 * @param tester 待通知的线程
 * @param task 任务号
 */
void MmsTestCase::doHelpGetRidWhenLockRow(MmsTester *tester, MmsThreadTask task) {
	RowLockHandle *rlh = NULL;
	RowId rid = 1;
	MmsRecord *mmsRecord = NULL;

	tester->joinSyncPoint(SP_MMS_RID_LOCKROW);
	rlh = LOCK_ROW(m_session, m_fMmsTable->getTid(), rid, Exclusived);
	tester->notifySyncPoint(SP_MMS_RID_LOCKROW);

	tester->joinSyncPoint(SP_MMS_RID_UNLOCKROW);
	EXCPT_OPER(m_session->unlockRow(&rlh));
	tester->notifySyncPoint(SP_MMS_RID_UNLOCKROW);
}

/** 
 * 功能类似于testGetRidWhenLockRow
 */
void MmsTestCase::testGetPKWhenLockRow(MmsThreadTask task) {
	// 添加一条记录, RowID为1
	m_fMmsTable->insert(m_session, 1, false);

	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	tester.enableSyncPoint(SP_MMS_PK_LOCKROW);
	tester.enableSyncPoint(SP_MMS_PK_UNLOCKROW);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	tester.disableSyncPoint(SP_MMS_PK_LOCKROW);
	tester.disableSyncPoint(SP_MMS_PK_UNLOCKROW);
}

/** 
 * 测试flushLog的GetSession边际情况（即无法获取session)
 * 测试流程：1. 关闭MMS自动flushLog功能
 *			 2. 设置更新cache间隔为1秒
 *			 3. 等待2秒 (步骤2和3用于flushLog函数中一行代码覆盖)
 *			 4. 执行MTT_GETSESSION_WHEN_FLUSHLOG_1并发任务
 *			 5. 执行MTT_GETSESSION_WHEN_FLUSHLOG_2并发任务
 */
void MmsTestCase::testGetSessionWhenFlushLog() {
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->setUpdateCacheTime(1);
	Thread::msleep(2000);
	testGetSessionWhenFlushLog(MTT_GETSESSION_WHEN_FLUSHLOG_1);
	testGetSessionWhenFlushLog(MTT_GETSESSION_WHEN_FLUSHLOG_2);
}

/** 
 * 测试flushLog的GetSession边际情况(执行并发任务)
 *
 * @param task 并发任务号
 */
void MmsTestCase::testGetSessionWhenFlushLog(MmsThreadTask task) {
	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	tester.enableSyncPoint(SP_BGTASK_GET_SESSION_START);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		tester.enableSyncPoint(SP_BGTASK_GET_SESSION_END);
	tester.enableSyncPoint(SP_MMS_FL);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	tester.disableSyncPoint(SP_BGTASK_GET_SESSION_START);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		tester.disableSyncPoint(SP_BGTASK_GET_SESSION_END);
	tester.disableSyncPoint(SP_MMS_FL);
}

/** 
 * 测试flushLog的GetSession边际情况（主流程）
 * 测试流程： 1. 关闭自动刷新日志
 *			  2. 调用刷写日志操作
 *			  3. 如果任务为MTT_GETSESSION_WHEN_FLUSHLOG_2, 等待在SP_MMS_FL同步点
 *
 * @param tester 待通知的线程
 * @param task	任务号
 */
void MmsTestCase::doGetSessionWhenFlushLog(MmsTester *tester, MmsThreadTask task) {
	// 直接调用更新缓存刷写函数
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_2)
		SYNCHERE(SP_MMS_FL);
}

/** 
 * 测试flushLog的GetSession边际情况（辅助流程）
 * 测试流程：1. 在SP_MMS_FL_SESSION_START之后获取所有session
 *			 2. 在SP_MMS_FL_SESSION_END之后释放一个session （MTT_GETSESSION_WHEN_FLUSHLOG_1任务)
 *			 3. 在SP_MMS_FL之后释放所有未释放session
 *
 * @param tester 待通知的线程
 * @param task 任务号
 */
void MmsTestCase::doHelpGetSessionWhenFlushLog(MmsTester *tester, MmsThreadTask task) {
	int idx = 0;
	Connection* conns[10000];
	Session* sessions[10000];
	// Connection *conn = m_db->getConnection(true);
	char name[100];

	tester->joinSyncPoint(SP_BGTASK_GET_SESSION_START);

	
	// 获取所有的Session
	do {
		sprintf(name, "%d", idx);
		conns[idx] = m_db->getConnection(true);
		assert(conns[idx]);
		sessions[idx] = m_db->getSessionManager()->allocSession(name, conns[idx], false);
	} while (sessions[idx++]);

	tester->notifySyncPoint(SP_BGTASK_GET_SESSION_START);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1) {
		tester->joinSyncPoint(SP_BGTASK_GET_SESSION_END);
		m_db->getSessionManager()->freeSession(sessions[0]);
		tester->notifySyncPoint(SP_BGTASK_GET_SESSION_END);
	}
	tester->joinSyncPoint(SP_MMS_FL);
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		CPPUNIT_ASSERT(!m_db->getSessionManager()->allocSession("0", conns[0], false));
	tester->notifySyncPoint(SP_MMS_FL);

	int i;
	if (task == MTT_GETSESSION_WHEN_FLUSHLOG_1)
		i = 1;
	else
		i = 0;
	for (; i < idx - 1; i++)
		m_db->getSessionManager()->freeSession(sessions[i]);
	for (int i = 0; i < idx; i++) {
		m_db->freeConnection(conns[i]);
	}
}

void MmsTestCase::testFlushLogForce() {
	doTestUpdate(true, 100);
	m_fMmsTable->getMmsTable()->runFlushTimerForce();
	close();
	init(100, true, true);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	m_vMmsTable->getMmsTable()->disableAutoFlushLog();
	doTestUpdate_simple(true, 5);
	m_fMmsTable->getMmsTable()->flushUpdateLog(m_session);
	m_vMmsTable->getMmsTable()->flushUpdateLog(m_session);
}

/** 
 * 测试FlushLog的页类型测试功能
 */
void MmsTestCase::testPageType() {
	close();
	init(100, true, true);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();
	MmsTester tester(this, MTT_FLUSHLOG_VICTIMPAGE);
	MmsTestHelper helper(this, &tester, MTT_FLUSHLOG_VICTIMPAGE);

	tester.enableSyncPoint(SP_MMS_FL_LOCK_PG);
	tester.enableSyncPoint(SP_MMS_FL_UNLOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * 测试FlushLog的页类型测试功能（主流程)
 * 测试流程： 1. 插入并更新100条记录
 *			  2. 调用flushLog
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doTestPageType(MmsTester *tester) {
	RowId rowId;

	for(int i = 0; i < 100; i++) {
		rowId = m_fMmsTable->insert(m_session, i, true);
		m_fMmsTable->update(m_session, rowId, i + 10000);
	}
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
}

/** 
 * 测试FlushLog的页类型测试功能（辅助流程)
 * 测试流程:  1. 在SP_MMS_FL_LOCK_PG同步点，替换相应的MMS记录页
 *			  2. 等待SP_MMS_FL_UNLOCK_PG同步点（注意：如MMS代码逻辑不正确，在该处会造成死锁)
 *			  3. 关闭同步点，并通知相应线程
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doHelpTestPageType(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_FL_LOCK_PG);
	m_fMmsTable->getMmsTable()->evictCurrPage(m_session);
	tester->notifySyncPoint(SP_MMS_FL_LOCK_PG);

	tester->joinSyncPoint(SP_MMS_FL_UNLOCK_PG);
	tester->disableSyncPoint(SP_MMS_FL_LOCK_PG);
	tester->disableSyncPoint(SP_MMS_FL_UNLOCK_PG);
	tester->notifySyncPoint(SP_MMS_FL_UNLOCK_PG);
}

/** 
 * 测试FlushLog的RPClass检查功能
 */
void MmsTestCase::testFlushLogRpClass() {
	close();
	init(100, true, true);
	m_fMmsTable->getMmsTable()->disableAutoFlushLog();

	MmsTester tester(this, MTT_FLUSHLOG_RPCLASS);
	MmsTestHelper helper(this, &tester, MTT_FLUSHLOG_RPCLASS);

	tester.enableSyncPoint(SP_MMS_FL_LOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * 测试FlushLog的RPClass检查功能（主流程)
 * 测试流程： 1. 插入并更新100条记录
 *			  2. 调用flushLog
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doTestFlushLogRpClass(MmsTester *tester) {
	RowId rowId;

	for(int i = 0; i < 100; i++) {
		rowId = m_fMmsTable->insert(m_session, i, true);
		m_fMmsTable->update(m_session, rowId, i + 10000);
	}
	m_fMmsTable->getMmsTable()->flushMmsLog(m_session);
}

/** 
 * 测试FlushLog的RPClass检查功能（辅助流程)
 * 测试流程： 1. 在SP_MMS_FL_LOCK_PG同步点时，设置当前页的rpClass字段为NULL
 *			  2. 关闭SP_MMS_FL_LOCK_PG同步点，并通知相应的等待线程
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doHelpTestFlushLogRpClass(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_FL_LOCK_PG);
	m_fMmsTable->getMmsTable()->setRpClass(NULL);
	tester->disableSyncPoint(SP_MMS_FL_LOCK_PG);
	tester->notifySyncPoint(SP_MMS_FL_LOCK_PG);
}

/** 
 * 测试检查点刷新时页类型判断功能
 */
void MmsTestCase::testPageTypeWhenFlush() {
	MmsTester tester(this, MTT_FLUSH_VICTIMPAGE);
	MmsTestHelper helper(this, &tester, MTT_FLUSH_VICTIMPAGE);

	tester.enableSyncPoint(SP_MMS_SF_LOCK_PG);
	tester.enableSyncPoint(SP_MMS_SF_UNLOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();

	testPageType();
}

/** 
 * 测试检查点刷新时页类型判断功能(主流程)
 * 测试流程： 1. 调用doTestUpdate流程
 *			  2. 定长表检查点刷新
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doTestPageTypeWhenFlush(MmsTester *tester) {
	doTestUpdate(true, 100);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
}

/** 
 * 测试检查点刷新时页类型判断功能(辅助流程)
 * 测试流程： 1. 在SP_MMS_SF_LOCK_PG同步点替换当前页
 *			  2. 关闭SP_MMS_SF_LOCK_PG同步点，并通知等待线程
 *			  3. 等待SP_MMS_SF_UNLOCK_PG同步点
 *			  4. 关闭SP_MMS_SF_UNLOCK_PG同步点，并通知等待线程
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doHelpTestPageTypeWhenFlush(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_SF_LOCK_PG);
	m_fMmsTable->getMmsTable()->evictCurrPage(m_session);
	tester->disableSyncPoint(SP_MMS_SF_LOCK_PG);
	tester->notifySyncPoint(SP_MMS_SF_LOCK_PG);

	tester->joinSyncPoint(SP_MMS_SF_UNLOCK_PG);
	tester->disableSyncPoint(SP_MMS_SF_UNLOCK_PG);
	tester->notifySyncPoint(SP_MMS_SF_UNLOCK_PG);
}

/**
 * 测试sortFlush中的RPClass检测功能
 */
void MmsTestCase::testSortFlushRpClass() {
	MmsTester tester(this, MTT_SORTFLUSH_RPCLASS);
	MmsTestHelper helper(this, &tester, MTT_SORTFLUSH_RPCLASS);

	tester.enableSyncPoint(SP_MMS_SF_LOCK_PG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * 测试sortFlush中的RPClass检测功能(主流程)
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doTestSortFlushRpClass(MmsTester *tester) {
	doTestUpdate(true, 100);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
	m_fMmsTable->getMmsTable()->setMmsTableInRpClass(m_fMmsTable->getMmsTable());
}

/** 
 * 测试sortFlush中的RPClass检测功能(辅助流程)
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doHelpTestSortFlushRpClass(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_SF_LOCK_PG);
	m_fMmsTable->getMmsTable()->setMmsTableInRpClass(NULL);
	tester->disableSyncPoint(SP_MMS_SF_LOCK_PG);
	tester->notifySyncPoint(SP_MMS_SF_LOCK_PG);	
}

/** 
 * 检查点刷新时页类型测试
 */
void MmsTestCase::testPageTypeWhenGetDirRec() {
	MmsTester tester(this, MTT_DIRTY_REC);
	MmsTestHelper helper(this, &tester, MTT_DIRTY_REC);

	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST);
	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);
	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND);
	tester.enableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * 检查点刷新时页类型测试(主流程)
 * 测试流程：1. 插入并更新500条记录
 *			 2. 检查点刷新
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doTestPageTypeWhenGetDirRec(MmsTester *tester) {
	RowId rowId;

	// 插入数据并更新
	for (int i = 0; i < 500; i++) {
		EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, i, true));
		EXCPT_OPER(m_fMmsTable->update(m_session, rowId, i+10));
	}
	// 检查点刷新
	m_fMmsTable->getMmsTable()->flush(m_session, true);
}

/** 
 * 检查点刷新时页类型测试(辅助流程)
 *
 * @param tester 待通知的线程
 */
void MmsTestCase::doHelpTestPageTypeWhenGetDirRec(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_1ST);
	m_fMmsTable->getMmsTable()->evictCurrPage(m_session);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_1ST);

	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_1ST_END);

	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_2ND);
	m_fMmsTable->getMmsTable()->setRpClass(NULL);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_2ND);

	tester->joinSyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);
	tester->disableSyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);
	tester->notifySyncPoint(SP_MMS_GET_DIRTY_REC_2ND_END);
}

/** 
 * 测试MMS替换时顶页被pin情况
 */
void MmsTestCase::testReplaceWhenPinTopPage() {
	MmsTester tester(this, MTT_GETPIN_REPLACEMENT);
	MmsTestHelper helper(this, &tester, MTT_GETPIN_REPLACEMENT);

	tester.enableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	tester.enableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/** 
 * 测试MMS替换时顶页被pin情况（主线程）
 *
 * @param tester 待通知线程
 */
void MmsTestCase::doReplaceWhenPinTopPage(MmsTester *tester) {
	close();
	init(7, true, true);
	for (int i = 0; i < 2000; i++)
		m_fMmsTable->insert(m_session, i, true);
}

/** 
 * 测试MMS替换时顶页被pin情况（辅助线程）
 *
 * @param tester 待通知线程
 */
void MmsTestCase::doHelpReplaceWhenPinTopPage(MmsTester *tester) {
	tester->joinSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	m_fMmsTable->getMmsTable()->pinCurrPage();
	tester->notifySyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	tester->joinSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);
	m_fMmsTable->getMmsTable()->unpinCurrPage();
	
	tester->disableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT);
	tester->disableSyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);
	tester->notifySyncPoint(SP_MMS_GET_PIN_WHEN_REPLACEMENT_END);
}

/** 
 * 测试缓存更新时间获取/设置
 */
void MmsTestCase::testCacheUpdateTime() {
	uint tm = m_fMmsTable->getMmsTable()->getUpdateCacheTime();
	m_fMmsTable->getMmsTable()->setUpdateCacheTime(tm * 2);
	CPPUNIT_ASSERT(tm * 2 == m_fMmsTable->getMmsTable()->getUpdateCacheTime());

	/*close();
	init(100, false, false);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getUpdateCacheTime());*/
}

/** 
 * 测试统计信息
 */
void MmsTestCase::testMmsStats() {
	doTestUpdate(true, 100);
	assert (m_mms->getMaxNrDirtyRecs() > 0);
	m_mms->setMaxNrDirtyRecs(6);
	EXCPT_OPER(m_fMmsTable->getMmsTable()->flush(m_session, true));
	EXCPT_OPER(m_vMmsTable->getMmsTable()->flush(m_session, true));

	u64 info;

	info = m_mms->getStatus().m_recordQueries;
	info = m_mms->getStatus().m_recordQueryHits;
	info = m_mms->getStatus().m_recordInserts;
	info = m_mms->getStatus().m_recordDeletes;
	info = m_mms->getStatus().m_recordUpdates;
	info = m_mms->getStatus().m_recordVictims;
	info = m_mms->getStatus().m_pageVictims;
	info = m_mms->getStatus().m_occupiedPages;

	MmsTable *mmsTable = m_fMmsTable->getMmsTable();

	info = mmsTable->getStatus().m_recordQueries;
	info = mmsTable->getStatus().m_recordQueryHits;
	info = mmsTable->getStatus().m_records;
	info = mmsTable->getStatus().m_recordPages;
	info = mmsTable->getStatus().m_recordInserts;
	info = mmsTable->getStatus().m_recordDeletes;
	info = mmsTable->getStatus().m_recordUpdates;
	info = mmsTable->getStatus().m_recordVictims;
	info = mmsTable->getStatus().m_pageVictims;

	int nrRPClass;
	MmsRPClass **rpClass = mmsTable->getMmsRPClass(&nrRPClass);

	for (int i = 0; i < nrRPClass; i++) {
		if (rpClass[i]) {
			info = rpClass[i]->getStatus().m_records;
			info = rpClass[i]->getStatus().m_recordPages;
			info = rpClass[i]->getStatus().m_freePages;
			info = rpClass[i]->getStatus().m_recordInserts;
			info = rpClass[i]->getStatus().m_recordDeletes;
			info = rpClass[i]->getStatus().m_recordUpdates;
			info = rpClass[i]->getStatus().m_recordVictims;
			info = rpClass[i]->getStatus().m_pageInserts;
			info = rpClass[i]->getStatus().m_pageDeletes;
			info = rpClass[i]->getStatus().m_pageVictims;
		}
	}

	// test mms unregister
	testMms();

	// update for paritial cached
	testUpdateWhenParitalCached();
}

/**
 * 非主键更新测试
 */
void MmsTestCase::testUpdateNonPK() {
	close();

	init(1000, true, true, false, true, false);
	this->doTestUpdate(true);
}

/** 
 * 测试用例为VMmsTable, 更新缓存字段2和4,
 * 更新序列1为：{ 4 } --> { 2, 4 } --> { 2 } --> { 1 } --> { 2 } --> { 3, 4 } --> { 2 } --> { 1, 2, 3, 4}
 * 更新序列2为：{ 1, 2, 3, 4 }
 *
 * 测试用例为FMmsTable, 更新缓存字段为2和4
 * 更新序列2为: { 2 } --> { 1, 2, 3, 4 }
 */
void MmsTestCase::testUpdateWhenParitalCached() {
	RowId rowId;
	char s[1024];

	close();

	init(1000, true, true, false, true, true, true);

	for (int i = 0; i < NR_RECORDS;) {
		sprintf(s, "%d", i);
		EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true));
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn3(m_session, rowId, i);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn13(m_session, rowId, i, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn0(m_session, rowId, i);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn23(m_session, rowId, i);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn0123(m_session, rowId, i, s);
		sprintf(s, "%d%s", i++, s);
		m_vMmsTable->updateColumn1(m_session, rowId, s);
	}

	close();
	init(1000, true, true, false, true, true, true);
	int j = 0; 

	sprintf(s, "%d", j);
	EXCPT_OPER(rowId = m_vMmsTable->insert(m_session, j, s, (int)strlen(s), true));
	sprintf(s, "%d%s", j++, s);
	m_vMmsTable->updateColumn0123(m_session, rowId, j, s);

	j = 0;

	EXCPT_OPER(rowId = m_fMmsTable->insert(m_session, j++, true));
	m_fMmsTable->updateColumn1(m_session, rowId, j++);
	m_fMmsTable->update(m_session, rowId, j++);
}

/** 
 * allocMmsPage测试
 */
void MmsTestCase::testAllocMmsPage() {
	testAllocMmsPage(MTT_AMP_GET_TBL);
	testAllocMmsPage(MTT_AMP_GET_PG);
	testAllocMmsPage(MTT_AMP_PIN_PG);
}

/** 
* getSubRecord中记录页失效情况的测试
*/
void MmsTestCase::testDisablePgWhenGetSubRecord() {
	MmsTester tester(this, MTT_GETSUBRECORD_DISABLEPG);
	MmsTestHelper helper(this, &tester, MTT_GETSUBRECORD_DISABLEPG);

	tester.enableSyncPoint(SP_MMS_SUBRECORD_DISABLEPG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestDisablePgWhenGetSubRecord(MmsTester *tester, MmsThreadTask task) {
	SubRecord subRecord;
	RowId rid = 10;

	subRecord.m_format = REC_REDUNDANT;
	subRecord.m_size = Limits::PAGE_SIZE;
	subRecord.m_data = (byte *)malloc(subRecord.m_size);
	subRecord.m_numCols = 2;
	subRecord.m_columns = (u16 *)malloc(sizeof(u16) * subRecord.m_numCols);
	for (int i = 0; i < subRecord.m_numCols; i++)
		subRecord.m_columns[i] = i;
	m_fMmsTable->insert(m_session, rid, false);

	SubrecExtractor *extractor = SubrecExtractor::createInst(m_session->getMemoryContext(), m_fMmsTable->getTableDef(),
		subRecord.m_numCols, subRecord.m_columns, m_fMmsTable->getTableDef()->m_recFormat, REC_REDUNDANT);
	CPPUNIT_ASSERT(!m_fMmsTable->getMmsTable()->getSubRecord(m_session, rid, extractor, &subRecord, true, false, 0));
	free(subRecord.m_data);
	free(subRecord.m_columns);
}

void MmsTestCase::doHelpDisablePgWhenGetSubRecord(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_SUBRECORD_DISABLEPG);
	m_fMmsTable->getMmsTable()->disableCurrPage();
	tester->disableSyncPoint(SP_MMS_SUBRECORD_DISABLEPG);
	tester->notifySyncPoint(SP_MMS_SUBRECORD_DISABLEPG);
}

void MmsTestCase::testTryLockWhenGetByRid() {
	MmsTester tester(this, MTT_GETRID_TRYLOCK);
	MmsTestHelper helper(this, &tester, MTT_GETRID_TRYLOCK);

	tester.enableSyncPoint(SP_MMS_RID_TRYLOCK);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestTryLockWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	m_fMmsTable->insert(m_session, 10, false);
	m_fMmsTable->selectByRid(m_session, 10, NULL, None);
}

void MmsTestCase::doHelpTryLockWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_RID_TRYLOCK);
	m_fMmsTable->getMmsTable()->lockCurrPage(m_session);
	tester->disableSyncPoint(SP_MMS_RID_TRYLOCK);
	tester->notifySyncPoint(SP_MMS_RID_TRYLOCK);
	m_fMmsTable->getMmsTable()->unlockCurrPage(m_session);
}


/** 
 * GetByRid中记录页失效情况的测试
 */
void MmsTestCase::testDisablePgWhenGetByRid() {
	MmsTester tester(this, MTT_GETRID_DISABLEPG);
	MmsTestHelper helper(this, &tester, MTT_GETRID_DISABLEPG);

	tester.enableSyncPoint(SP_MMS_RID_DISABLEPG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestDisablePgWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	m_fMmsTable->insert(m_session, 10, false);
	CPPUNIT_ASSERT(NULL == m_fMmsTable->selectByRid(m_session, 10, NULL, None));
}

void MmsTestCase::doHelpDisablePgWhenGetByRid(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_RID_DISABLEPG);
	m_fMmsTable->getMmsTable()->disableCurrPage();
	tester->disableSyncPoint(SP_MMS_RID_DISABLEPG);
	tester->notifySyncPoint(SP_MMS_RID_DISABLEPG);
}

/** 
* PutIfNotExist中记录页失效情况的测试
*/
void MmsTestCase::testDisablePgWhenPutIfNotExist() {
	MmsTester tester(this, MTT_PUT_DISABLEPG);
	MmsTestHelper helper(this, &tester, MTT_PUT_DISABLEPG);

	tester.enableSyncPoint(SP_MMS_PUT_DISABLEPG);

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

void MmsTestCase::doTestDisablePgWhenPutIfNotExist(MmsTester *tester, MmsThreadTask task) {
	m_fMmsTable->insert(m_session, 10, false);
	// 再次插入相同记录
	m_fMmsTable->insert(m_session, 10, false);
}

void MmsTestCase::doHelpDisablePgWhenPutIfNotExist(MmsTester *tester, MmsThreadTask task) {
	tester->joinSyncPoint(SP_MMS_PUT_DISABLEPG);
	m_fMmsTable->getMmsTable()->disableCurrPage();
	tester->disableSyncPoint(SP_MMS_PUT_DISABLEPG);
	tester->notifySyncPoint(SP_MMS_PUT_DISABLEPG);
}

/** 
 * allocMmsPage测试（并发情况)
 *
 * @param task 任务号
 */
void MmsTestCase::testAllocMmsPage(MmsThreadTask task) {
	MmsTester tester(this, task);
	MmsTestHelper helper(this, &tester, task);

	switch(task) {
	case MTT_AMP_GET_TBL:
		tester.enableSyncPoint(SP_MMS_AMP_GET_TABLE);
		tester.enableSyncPoint(SP_MMS_AMP_GET_TABLE_END);
		break;
	case MTT_AMP_GET_PG:
		tester.enableSyncPoint(SP_MMS_AMP_GET_PAGE);
		tester.enableSyncPoint(SP_MMS_AMP_GET_PAGE_END);
		break;
	case MTT_AMP_PIN_PG:
		tester.enableSyncPoint(SP_MMS_AMP_PIN_PAGE);
		tester.enableSyncPoint(SP_MMS_AMP_PIN_PAGE_END);
		break;
	}

	tester.start();
	helper.start();

	tester.join();
	helper.join();
}

/**
 * allocMmsPage测试(主流程）
 *
 * @param tester 待通知的线程
 * @param task 任务号
 */
void MmsTestCase::doTestAllocMmsPage(MmsTester *tester, MmsThreadTask task) {
	close();
	init(100, true, true);
	testFreeSomePages();
}

/**
 * allocMmsPage测试(辅助流程)
 *
 * @param tester 待通知的线程
 * @param task 任务号
 */
void MmsTestCase::doHelpAllocMmsPage(MmsTester *tester, MmsThreadTask task) {
	if (MTT_AMP_GET_TBL == task) {
		tester->joinSyncPoint(SP_MMS_AMP_GET_TABLE);
		m_mms->lockMmsTable(0);
		tester->disableSyncPoint(SP_MMS_AMP_GET_TABLE);
		tester->notifySyncPoint(SP_MMS_AMP_GET_TABLE);

		tester->joinSyncPoint(SP_MMS_AMP_GET_TABLE_END);
		m_mms->unlockMmsTable(0);
		tester->disableSyncPoint(SP_MMS_AMP_GET_TABLE_END);
		tester->notifySyncPoint(SP_MMS_AMP_GET_TABLE_END);
	} else if (MTT_AMP_GET_PG == task) {
		tester->joinSyncPoint(SP_MMS_AMP_GET_PAGE);
		m_mms->lockRecPage(m_session);
		tester->disableSyncPoint(SP_MMS_AMP_GET_PAGE);
		tester->notifySyncPoint(SP_MMS_AMP_GET_PAGE);

		tester->joinSyncPoint(SP_MMS_AMP_GET_PAGE_END);
		m_mms->unlockRecPage(m_session);
		tester->disableSyncPoint(SP_MMS_AMP_GET_PAGE_END);
		tester->notifySyncPoint(SP_MMS_AMP_GET_PAGE_END);	
	} else if (MTT_AMP_PIN_PG == task) {
		tester->joinSyncPoint(SP_MMS_AMP_PIN_PAGE);
		m_mms->pinRecPage();
		tester->disableSyncPoint(SP_MMS_AMP_PIN_PAGE);
		tester->notifySyncPoint(SP_MMS_AMP_PIN_PAGE);

		tester->joinSyncPoint(SP_MMS_AMP_PIN_PAGE_END);
		m_mms->unpinRecPage();
		tester->disableSyncPoint(SP_MMS_AMP_PIN_PAGE_END);
		tester->notifySyncPoint(SP_MMS_AMP_PIN_PAGE_END);	
	}
}

/** 
 * 测试FPage功能
 */
void MmsTestCase::testFPage() {
	char s[1024];
	
	close();
	init(20, true, true);

	// 默认设置页替换率为1
	CPPUNIT_ASSERT(1 == m_mms->getPageReplaceRatio());

	// 设置失败！
	CPPUNIT_ASSERT(!m_mms->setPageReplaceRatio(1.5));

	// 设置成功
	CPPUNIT_ASSERT(m_mms->setPageReplaceRatio((float)0.001));

	for (int i = 0; i < 100; i++)
		m_fMmsTable->insert(m_session, i, true);

	for (int i = 0; i < 1000; i++) {
		sprintf(s, "%d", i);
		m_vMmsTable->insert(m_session, i, s, (int)strlen(s), true);
	}
}

/**
 *						测试用例介绍
 *
 *	1. 测试表说明：
 *	   提供三种测试表：定长表、变长表和百字段表(100个字段)，分别用FMmsTable、VMmsTable和HMmsTable三个类实现
 *
 *  2. 测试线程说明：
 *	   提供三种测试线程：生产者、消费者和更改者，分别用MmsProducer、MmsConsumer和MmsChanger三个类实现
 *	   生产者：负责记录插入
 *	   消费者：负责记录删除
 *	   更改者：负责记录更改
 *
 * 
 */
/************************************************************************/
/*                     定长表FMmsTable实现                              */
/************************************************************************/

/** 
 * 创建定长表 (构造函数后调用）
 *
 * @param session		回话对象
 * @param db			所属数据库
 * @param tid			表ID
 * @param schema		模式名
 * @param tablename		表名
 * @param namePrefix	名字前缀
 * @param prefixSize	前缀长度
 * @param cacheUpdate	更新缓存
 * @param colUpdate		字段更新缓存
 */
void FMmsTable::create(Session *session, 
					   Database *db,
					   Mms *mms,
					   int tid, 
					   const char *schema, 
					   const char *tablename, 
					   const char *namePrefix, 
					   int prefixSize,
					   bool cacheUpdate,
					   bool colUpdate, 
					   bool delHeapFile,
					   bool partialCached,
					   int partitionNr) {
	m_db = db;
	m_tid = tid;
	m_namePrefix = namePrefix;
	m_prefixSize = prefixSize;
	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
	}
	// 创建定长堆
	if (delHeapFile) {
		File fHeap(FIX_TBL_HEAP);
		fHeap.remove();
		TableDef::drop(FIX_TBL_DEF);
	}
	TableDefBuilder *builder = new TableDefBuilder(tid, schema, tablename);
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, prefixSize + (u16)sizeof(u64), false);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, NULL);
	if (delHeapFile) {
		m_tblDef = builder->getTableDef();
		EXCPT_OPER(m_tblDef->writeFile(FIX_TBL_DEF));
		EXCPT_OPER(DrsHeap::create(m_db, FIX_TBL_HEAP, m_tblDef));
	} else {
		EXCPT_OPER(m_tblDef = TableDef::open(FIX_TBL_DEF));
	}

	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, FIX_TBL_HEAP, m_tblDef));
	delete builder;

	if(cacheUpdate) {
		m_tblDef->m_cacheUpdate = true;
		m_tblDef->m_updateCacheTime = 10;
	} else {
		m_tblDef->m_cacheUpdate = false;
		m_tblDef->m_updateCacheTime = 10;
	}

	if (colUpdate) {
		if (partialCached) {
			m_tblDef->m_columns[0]->m_cacheUpdate = false;
			m_tblDef->m_columns[1]->m_cacheUpdate = true;
			m_tblDef->m_columns[2]->m_cacheUpdate = false;
			m_tblDef->m_columns[3]->m_cacheUpdate = true;
		} else {
			for (u16 i = 0; i < m_tblDef->m_numCols; i++)
				m_tblDef->m_columns[i]->m_cacheUpdate = true;
		}
	}

	// 创建MMS表
	m_mmsTable = new MmsTable(mms, m_db, m_heap, m_tblDef, m_tblDef->m_cacheUpdate, m_tblDef->m_updateCacheTime, partitionNr);
	mms->registerMmsTable(session, m_mmsTable);
	m_mms = mms;
}

/** 
 * 销毁定长表 （析构函数前调用）
 *
 * @param session 回话对象
 * @param delHeapFile 删除堆文件
 */
void FMmsTable::drop(Session *session, bool delHeapFile, bool flushDirty) {
	if (m_mmsTable != NULL) {
		EXCPT_OPER(m_mmsTable->close(session, flushDirty));
		EXCPT_OPER(m_mms->unregisterMmsTable(session, m_mmsTable));
		delete m_mmsTable;
		m_mmsTable = NULL;
	}
	if (m_heap != NULL) {
		EXCPT_OPER(m_heap->close(session, true));
		if (delHeapFile) {
			EXCPT_OPER(DrsHeap::drop(FIX_TBL_HEAP));
		}
		delete m_heap;
		m_heap = NULL;
	}

	if (m_tblDef != NULL) {
		if (delHeapFile) {
			EXCPT_OPER(TableDef::drop(FIX_TBL_DEF));
		}
		delete m_tblDef;
		m_tblDef = NULL;
	}
}

/** 
 * 添加一条记录
 *
 * @param session	会话对象
 * @param userId	用户ID
 * @param insertHeap 写入DRS堆
 * @return RowID
 */
RowId FMmsTable::insert(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 对不对？
	record = rb.getRecord(0);
	
	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // 伪造userId为rowId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

RowId MmsTestCase::fixInsert(MmsTable *mmsTable, Session *session, u64 userId, bool insertHeap, DrsHeap *heap, const TableDef *tblDef) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(tblDef, 0, tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, "mms", 3);
	memcpy(username + 3, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 对不对？
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // 伪造userId为rowId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
* 添加一条记录
*
* @param session	会话对象
* @param userId	用户ID
* @param insertHeap 写入DRS堆
* @return RowID
*/
RowId FMmsTable::insertEx(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 对不对？
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // 伪造userId为rowId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		freeRecord(record);
		if (insertHeap)
			session->unlockRow(&rlh);
		return (RowId)-1;
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
* 添加一条真实记录（主键为Natural格式)
*
* @param session	会话对象
* @param userId	用户ID
* @param insertHeap 写入DRS堆
* @return RowID
*/
RowId FMmsTable::insertReal(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 对不对？
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // 伪造userId为rowId
	rowId = record->m_rowId;

	SubRecord keyRecord;

	keyRecord.m_format = KEY_NATURAL;
	keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
	keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
	keyRecord.m_size = m_tblDef->m_maxRecSize;
	keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	RecordOper::extractKeyFN(m_tblDef, m_tblDef->m_pkey, record, &keyRecord);

	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	free(keyRecord.m_data);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
 * 插入记录两次
 *
 * @param session 会话
 * @param userId  用户ID
 * @param insertHeap 是否插入堆
 * @return RowID
 */
RowId FMmsTable::insertDouble(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32 balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId;
	rowId = record->m_rowId;
	for (int i = 0; i < 2; i++) {
		EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
		if (mmsRecord) {
			EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
		} else {
			;//EXCPT_OPER(NTSE_THROW);
		}
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/**
 * 获取MMS记录
 *
 * @param session 会话
 * @param userId 用户ID
 * @param insertHeap 是否插入堆
 */
void FMmsTable::getRecord(Session *session, u64 userId, bool insertHeap) {
	RowId rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32 balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	char username[Limits::PAGE_SIZE];
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &userId, sizeof(u64));
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	record = rb.getRecord(0);

	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId;
	rowId = record->m_rowId;
	for (int i = 0; i < 2; i++) {
		EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
		if (mmsRecord) {
			EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
		} else {
			;//EXCPT_OPER(NTSE_THROW);
		}
	}
	if (insertHeap)
		session->unlockRow(&rlh);

	// 从MMS获取记录
	Record recordNew;

	recordNew.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	
	CPPUNIT_ASSERT(mmsRecord = m_mmsTable->getByRid(session, rowId, false, NULL, None));

	m_mmsTable->getRecord(mmsRecord, &recordNew);

	CPPUNIT_ASSERT(RecordOper::isRecordEq(m_tblDef, record, &recordNew));

	free(recordNew.m_data);
	recordNew.m_data = NULL;

	// 不拷贝数据
	CPPUNIT_ASSERT(mmsRecord = m_mmsTable->getByRid(session, rowId, false, NULL, None));

	m_mmsTable->getRecord(mmsRecord, &recordNew, false);

	CPPUNIT_ASSERT(RecordOper::isRecordEq(m_tblDef, record, &recordNew));

	freeRecord(record);
} 

/** 
 * 删除一条记录
 *
 * @param session	会话对象
 * @param userId	用户ID
 * @return 是否成功
 */
bool FMmsTable::del(Session *session, u64 userId) {
	bool succ = false;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	if (mmsRecord = this->selectByRid(session, userId, &rlh, Exclusived)) {
		EXCPT_OPER(m_mmsTable->del(session, mmsRecord));
		session->unlockRow(&rlh);
		return true;
	} else 
		return false;
}

/** 
 * 更新一条记录
 *
 * @param session	会话对象
 * @param userId	用户ID
 * @param updateId	更新ID
 */
void FMmsTable::update(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2 3";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 更新第2个字段
 *
 * @param session 会话
 * @param rowId RowID
 * @param updateId 更新ID
 */
void FMmsTable::updateColumn1(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		//EXCPT_OPER(m_mmsTable->update(session, mmsRecord, subRecord));
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
* 更新一条真实记录（主键为Natural格式)
*
* @param session	会话对象
* @param userId	用户ID
* @param updateId	更新ID
*/
void FMmsTable::updateReal(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2 3";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		Record record;
		record.m_rowId = rowId;
		record.m_format = REC_FIXLEN;

		record.m_data = (byte *)malloc(mmsRecord->m_size);
		memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
		record.m_size = mmsRecord->m_size;

		RecordOper::updateRecordFR(m_tblDef, &record, subRecord);

		SubRecord keyRecord;

		keyRecord.m_format = KEY_NATURAL;
		keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
		keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
		keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
		keyRecord.m_size = m_tblDef->m_maxRecSize;
		RecordOper::extractKeyFN(m_tblDef, m_tblDef->m_pkey, &record, &keyRecord);

		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else {
			m_mmsTable->flushAndDel(session, mmsRecord);
			// TODO: 实际堆更新
		}
		session->unlockRow(&rlh);
		free(keyRecord.m_data);
		free(record.m_data);
	}
	freeSubRecord(subRecord);
}

/** 
* 更新一条真实记录（扩展版本，主键为Natural格式)
*
* @param session	会话对象
* @param userId	用户ID
* @param updateId	更新ID
*/
void FMmsTable::updateRealEx(Session *session, u64 rowId, u64 updateId) {
	//u64 rowId = userId;
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2";
	RowLockHandle *rlh = NULL;

	memcpy(username, m_namePrefix, m_prefixSize);
	memcpy(username + m_prefixSize, &updateId, sizeof(u64));
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		Record record;
		record.m_rowId = rowId;
		record.m_format = REC_FIXLEN;

		record.m_data = (byte *)malloc(mmsRecord->m_size);
		memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
		record.m_size = mmsRecord->m_size;

		RecordOper::updateRecordFR(m_tblDef, &record, subRecord);

		SubRecord keyRecord;

		keyRecord.m_format = KEY_NATURAL;
		keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
		keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
		keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
		keyRecord.m_size = m_tblDef->m_maxRecSize;
		RecordOper::extractKeyFN(m_tblDef, m_tblDef->m_pkey, &record, &keyRecord);

		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else {
			m_mmsTable->flushAndDel(session, mmsRecord);
			// TODO: 实际堆更新
		}
		session->unlockRow(&rlh);
		free(keyRecord.m_data);
		free(record.m_data);
	}
	freeSubRecord(subRecord);
}

/** 
 * 根据RID获取一条记录
 * 
 * @param session	会话对象
 * @param rowId		RID
 * @param rlh		INOUT 行锁句柄
 * @param lockMode	锁模式
 */
MmsRecord* FMmsTable::selectByRid(Session *session, u64 rowId, RowLockHandle **rlh, LockMode lockMode) {
	MmsRecord *mmsRecord;

	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, rlh, lockMode));
	return mmsRecord;
}

/** 
 * 释放记录相关锁和pin
 *
 * @param session	会话对象
 * @param mmsRecord	MMS记录, 输入为NULL, 则不需要释放pin
 * @param rlh		行锁句柄, 输入为NULL，则不需要释放行锁
 */
void FMmsTable::release(Session *session, MmsRecord *mmsRecord, RowLockHandle **rlh) {
	if (mmsRecord)
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	if (rlh)
		session->unlockRow(rlh);
}

/** 
 * 获取MMS表
 *
 * @return MMS表
 */
MmsTable* FMmsTable::getMmsTable() {
	return m_mmsTable;
}

/**
 * 获取TID
 *
 * @return TID
 */
int FMmsTable::getTid() {
	return m_tid;
}

TableDef* FMmsTable::getTableDef() {
	return m_tblDef;
}
/** 
 * 获取堆指针
 *
 * @return 堆指针
 */
DrsHeap* FMmsTable::getHeap() {
	return m_heap;
}


/************************************************************************/
/*                     变长表VMmsTable实现                              */
/************************************************************************/

/** 
 * 创建变长表 (构造函数后调用）
 *
 * @param session   会话对象
 * @param db		所属数据库
 * @param tid		表ID
 * @param schema    模式名
 * @param tablename	表名
 * @param cacheUpdate 缓存更新
 * @param colUpdate 字段更新
 * @param varcharPK 变长主键
 */
void VMmsTable::create(Session *session, 
					   Database *db, 
					   Mms *mms, 
					   int tid, 
					   const char *schema, 
					   const char *tablename, 
					   /*const char *heapfilename,*/
					   int maxNameSize,
					   bool cacheUpdate,
					   bool colUpdate,
					   bool varcharPK,
					   bool paritalUpdate, 
					   bool delHeapFile,
					   int partitionNr) {
	m_db = db;
	m_tid = tid;
	m_maxNameSize = maxNameSize;
	m_varcharPK = varcharPK;

	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
	}

	// 创建变长堆
	if (delHeapFile) {
		File fHeap(VAR_TBL_HEAP);
		fHeap.remove();
		TableDef::drop(VAR_TBL_DEF);
	}
	TableDefBuilder *builder = new TableDefBuilder(tid, schema, tablename);
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_VARCHAR, maxNameSize, false, false);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	if (varcharPK)
		builder->addIndex("PRIMARY", true, true, false, "UserName", 0, NULL);
	else
		builder->addIndex("PRIMARY", true, true, false, "UserId", 0, NULL);
	if (delHeapFile) {
		m_tblDef = builder->getTableDef();
		EXCPT_OPER(m_tblDef->writeFile(VAR_TBL_DEF));
		EXCPT_OPER(DrsHeap::create(m_db, VAR_TBL_HEAP, m_tblDef));
	} else {
		m_tblDef = TableDef::open(VAR_TBL_DEF);
	}
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, VAR_TBL_HEAP, m_tblDef));
	delete builder;

	if(cacheUpdate) {
		m_tblDef->m_cacheUpdate = true;
		m_tblDef->m_updateCacheTime = 10;
	} else {
		m_tblDef->m_cacheUpdate = false;
		m_tblDef->m_updateCacheTime = 10;
	}

	if (colUpdate) {
		if (paritalUpdate) {
			m_tblDef->m_columns[0]->m_cacheUpdate = false;
			m_tblDef->m_columns[1]->m_cacheUpdate = true;
			m_tblDef->m_columns[2]->m_cacheUpdate = false;
			m_tblDef->m_columns[3]->m_cacheUpdate = true;
		} else {
			for (u16 i = 0; i < m_tblDef->m_numCols; i++)
				m_tblDef->m_columns[i]->m_cacheUpdate = true;
		}
	}

	// 创建MMS表
	m_mmsTable = new MmsTable(mms, m_db, m_heap, m_tblDef, m_tblDef->m_cacheUpdate, m_tblDef->m_updateCacheTime, partitionNr);
	mms->registerMmsTable(session, m_mmsTable);	
	m_mms = mms;
}

/** 
 * 销毁变长表 （析构函数前调用）
 *
 * @param session 会话对象
 * @param delHeapFile 删除堆文件
 */
void VMmsTable::drop(Session *session, bool delHeapFile, bool flushDirty) {
	if (m_mmsTable) {
		EXCPT_OPER(m_mmsTable->close(session, flushDirty));
		EXCPT_OPER(m_mms->unregisterMmsTable(session, m_mmsTable));
		delete m_mmsTable;
		m_mmsTable = NULL;
	}
	if (m_heap) {
		EXCPT_OPER(m_heap->close(session, true));
		if (delHeapFile) {
			EXCPT_OPER(DrsHeap::drop(VAR_TBL_HEAP));
		}
		delete m_heap;
		m_heap = NULL;
	}

	if (m_tblDef != NULL) {
		if (delHeapFile) {
			TableDef::drop(VAR_TBL_DEF);
		}
		delete m_tblDef;
		m_tblDef = NULL;
	}
}

/** 
 * 添加一条记录
 *
 * @param session	会话对象
 * @param userId	用户ID
 * @param username  用户名
 * @param nameSize	用户名长度
 * @param insertHeap 插入DRS堆
 * @return RowID
 */
RowId VMmsTable::insert(Session *session, u64 userId, const char *username, int nameSize, bool insertHeap) {
	u64 rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	rb.appendVarchar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 
	record = rb.getRecord(0);
	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // 伪造rowId为userId
	rowId = record->m_rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		rowId = RID_READ(mmsRecord->m_rid);
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
* 添加一条真实记录(主键为Natural格式)
*
* @param session	会话对象
* @param userId	用户ID
* @param username  用户名
* @param nameSize	用户名长度
* @param insertHeap 插入DRS堆
* @return RowID
*/
RowId VMmsTable::insertReal(Session *session, u64 userId, const char *username, int nameSize, bool insertHeap) {
	u64 rowId;
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	Record *record;
	RecordBuilder rb(m_tblDef, 0, m_tblDef->m_recFormat);
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	rb.appendBigInt(userId);
	rb.appendVarchar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	// TODO: 
	record = rb.getRecord(0);
	if (insertHeap)
		record->m_rowId = m_heap->insert(session, record, &rlh);
	else
		record->m_rowId = userId; // 伪造rowId为userId
	rowId = record->m_rowId;
	assert(m_varcharPK);

	SubRecord keyRecord;

	keyRecord.m_format = KEY_NATURAL;
	keyRecord.m_numCols = m_tblDef->m_pkey->m_numCols;
	keyRecord.m_columns = m_tblDef->m_pkey->m_columns;
	keyRecord.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	keyRecord.m_size = m_tblDef->m_maxRecSize;
	RecordOper::extractKeyVN(m_tblDef, m_tblDef->m_pkey, record, NULL, &keyRecord);
		
	EXCPT_OPER(mmsRecord = m_mmsTable->putIfNotExist(session, record));
	if (mmsRecord) {
		EXCPT_OPER(m_mmsTable->unpinRecord(session, mmsRecord));
	} else {
		;//EXCPT_OPER(NTSE_THROW);	
	}
	freeRecord(record);
	free(keyRecord.m_data);
	if (insertHeap)
		session->unlockRow(&rlh);
	return rowId;
}

/** 
 * 删除一条记录
 *
 * @param session	会话对象
 * @param rowId		RID
 * @return 是否成功
 */
bool VMmsTable::del(Session *session, u64 rowId) {
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;
	
	if (mmsRecord = selectByRid(session, rowId, &rlh, Exclusived)) {
		EXCPT_OPER(m_mmsTable->del(session, mmsRecord));
		session->unlockRow(&rlh);
		return true;
	} else
		return false;
}

/** 
 * 删除一条记录
 *
 * @param session	会话对象
 * @param username	用户名
 * @param nameSize  用户名长度
 * @return 是否成功
 */
//bool VMmsTable::del(Session *session, const char *username, int nameSize) {
//	MmsRecord *mmsRecord;
//	RowLockHandle *rlh = NULL;
//
//	assert(m_varcharPK);
//	if (mmsRecord = selectByPK(session, username, nameSize, &rlh, Exclusived)) {
//		EXCPT_OPER(m_mmsTable->del(mmsRecord));
//		session->unlockRow(&rlh);
//		return true;
//	} else
//		return false;
//}

/** 
* 更新一条记录
*
* @param session	会话对象
* @param userId	用户ID
* @param updateId	更新ID
*/
void VMmsTable::update(Session *session, u64 rowId, u64 updateId) {
	byte username[Limits::PAGE_SIZE];
	u64 bankacc = updateId + ((u64)updateId << 32);
	u32	balance = (u32)((u64)(-1) - updateId);	
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "0 1 2 3";
	RowLockHandle *rlh;
	int nameSize = (int) (updateId % (m_maxNameSize - 1) + 1);  // 名字长度最小为1
 
	memset(username, 1, nameSize);
	username[nameSize] = '\0';
	subRecord = srb.createSubRecordById(cols, &updateId, username, &bankacc, &balance);
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 普通更新
 *
 * @param session 会话
 * @param rowId RowID
 * @param username 用户名
 */
void VMmsTable::update(Session *session, u64 rowId, char *username) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 普通更新
 *
 * @param session 会话
 * @param rowId RowID
 * @param username 用户名
 * @param touch 是否更改时间戳
 */
void VMmsTable::update(Session *session, u64 rowId, char *username, bool touch) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, touch, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 实际更新
 *
 * @param session 会话
 * @param rowId RowID
 * @param username 用户名
 */
void VMmsTable::updateReal(Session *session, u64 rowId, const char *username) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	const char *cols = "1";
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (!mmsRecord) {
		freeSubRecord(subRecord);
		return;//TODO: 实现heap更新
	}
	assert(m_varcharPK);
	
	Record record;
	record.m_rowId = rowId;
	record.m_format = REC_VARLEN;
	record.m_data = (byte *)malloc(m_tblDef->m_maxRecSize);
	memcpy(record.m_data, (byte *)mmsRecord + sizeof(MmsRecord), mmsRecord->m_size);
	record.m_size = mmsRecord->m_size;

	RecordOper::updateRecordVRInPlace(m_tblDef, &record, subRecord, m_tblDef->m_maxRecSize);
	
	u16 recSize;
	if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		m_mmsTable->update(session, mmsRecord, subRecord, recSize);
	else
		m_mmsTable->flushAndDel(session, mmsRecord);
	session->unlockRow(&rlh);
	freeSubRecord(subRecord);
	free(record.m_data);
}

/** 
 * 更新变长表第1字段
 *
 * @param session 会话
 * @param rowId RowID
 * @param userId 用户名ID
 */
void VMmsTable::updateColumn0(Session *session, u64 rowId, u64 userId) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "0";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &userId);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 更新变长表第2字段
 *
 * @param session 会话
 * @param rowId RowID
 * @param username 用户名
 */
void VMmsTable::updateColumn1(Session *session, u64 rowId, char *username) {
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "1";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 更新变长表第4字段
 *
 * @param session 会话
 * @param rowId RowID
 * @param userId 用户名ID
 */
void VMmsTable::updateColumn3(Session *session, u64 rowId, u64 userId) {
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	u16 recSize;
	if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		m_mmsTable->update(session, mmsRecord, subRecord, recSize);
	else
		m_mmsTable->flushAndDel(session, mmsRecord);
	session->unlockRow(&rlh);
	freeSubRecord(subRecord);
}

/** 
 * 更新变长表2、4字段
 *
 * @param session 会话
 * @param rowId RowID
 * @param userId 用户名ID
 * @param username 用户名
 */
void VMmsTable::updateColumn13(Session *session, u64 rowId, u64 userId, char *username) {
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "1 3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, username, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	u16 recSize;
	if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
		m_mmsTable->update(session, mmsRecord, subRecord, recSize);
	else
		m_mmsTable->flushAndDel(session, mmsRecord);
	session->unlockRow(&rlh);
	freeSubRecord(subRecord);
}

/** 
 * 更新变长表3、4字段
 *
 * @param session 会话
 * @param rowId	RowID
 * @param userId 用户ID
 */
void VMmsTable::updateColumn23(Session *session, u64 rowId, u64 userId) {
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "2 3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 更新变长表1-4字段
 *
 * @param session 会话
 * @param rowId RowID
 * @param userId 用户名ID
 * @param username 用户名
 */
void VMmsTable::updateColumn0123(Session *session, u64 rowId, u64 userId, char *username) {
	u64 bankacc = userId + ((u64)userId << 32);
	u32	balance = (u32)((u64)(-1) - userId);
	SubRecordBuilder srb(m_tblDef, REC_REDUNDANT, rowId);
	const char *cols = "0 1 2 3";
	SubRecord *subRecord;
	MmsRecord *mmsRecord;
	RowLockHandle *rlh = NULL;

	subRecord = srb.createSubRecordById(cols, &userId, username, &bankacc, &balance);
	subRecord->m_rowId = rowId;
	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, &rlh, Exclusived));
	if (mmsRecord) {
		u16 recSize;
		if (m_mmsTable->canUpdate(mmsRecord, subRecord, &recSize))
			m_mmsTable->update(session, mmsRecord, subRecord, recSize);
		else
			m_mmsTable->flushAndDel(session, mmsRecord);
		session->unlockRow(&rlh);
	}
	freeSubRecord(subRecord);
}

/** 
 * 根据RID获取一条记录
 * 
 * @param session	会话对象
 * @param rowId		RID
 * @param rlh		INOUT 行锁句柄
 * @param lockMode   行锁类型
 * @return MMS记录项
 */
MmsRecord* VMmsTable::selectByRid(Session *session, u64 rowId, RowLockHandle **rlh, LockMode lockMode) {
	MmsRecord *mmsRecord;

	EXCPT_OPER(mmsRecord = m_mmsTable->getByRid(session, rowId, true, rlh, lockMode));
	return mmsRecord;
}

/** 
 * 根据主键获取一条记录
 *
 * @param session	会话对象
 * @param userName	用户名
 * @param nameSize	用户名长度
 * @param rlh		INOUT 行锁句柄
 * @param lockMode	行锁类型
 * @return MMS记录项
 */
//MmsRecord* VMmsTable::selectByPK(Session *session, const char* userName, int nameSize, RowLockHandle **rlh, LockMode lockMode) {
//	MmsRecord *mmsRecord;
//	
//	assert(m_varcharPK);
//	EXCPT_OPER(mmsRecord = m_mmsTable->getByPrimaryKey(session, (const byte *)userName, nameSize, rlh, lockMode));
//	return mmsRecord;
//}

/** 
 * 根据实际主键值查询MMS记录项
 *
 * @param session 会话
 * @param userName 用户名
 * @param nameSize 用户名长度
 * @param rlh 记录锁句柄
 * @param lockMode 记录锁类型
 * @return MMS记录项
 */
//MmsRecord* VMmsTable::selectByPKReal(Session *session, const char* userName, int nameSize, RowLockHandle **rlh, LockMode lockMode) {
//	SubRecordBuilder srb(m_tblDef, KEY_NATURAL);
//	SubRecord *subRecord;
//	MmsRecord *mmsRecord;
//	char *cols = "1";
//
//	subRecord = srb.createSubRecordById(cols, userName);
//	EXCPT_OPER(mmsRecord = m_mmsTable->getByPrimaryKey(session, subRecord->m_data, subRecord->m_size, rlh, lockMode));
//	freeSubRecord(subRecord);
//	return mmsRecord;
//}

/** 
 * 获取MMS表
 *
 * @param MMS表
 */
MmsTable* VMmsTable::getMmsTable() {
	return m_mmsTable;
}

TableDef* VMmsTable::getTableDef() {
	return m_tblDef;
}

/** 
 * 获取堆指针
 *
 * @return 堆指针
 */
DrsHeap* VMmsTable::getHeap() {
	return m_heap;
}

/************************************************************************/
/*                     测试者MmsTester实现                              */
/************************************************************************/

// 主测试线程
void MmsTester::run() {
	switch(m_task) {
	case MTT_DOTOUCH:
		m_testCase->doTestTouch(this);
		break;
	case MTT_DEL_DOTOUCH:
		m_testCase->doTestDelWhenTouch(this);
		break;
	case MTT_GETRID_WHEN_LOCKROW_0:
	case MTT_GETRID_WHEN_LOCKROW_1:
	case MTT_GETRID_WHEN_LOCKROW_2:
	case MTT_GETRID_WHEN_LOCKROW_3:
		m_testCase->doGetRidWhenLockRow(this);
		break;
	case MTT_GETRID_TRYLOCK:
		m_testCase->doTestTryLockWhenGetByRid(this, m_task);
		break;
	case MTT_GETRID_DISABLEPG:
		m_testCase->doTestDisablePgWhenGetByRid(this, m_task);
		break;
	case MTT_PUT_DISABLEPG:
		m_testCase->doTestDisablePgWhenPutIfNotExist(this, m_task);
		break;
	case MTT_GETSUBRECORD_DISABLEPG:
		m_testCase->doTestDisablePgWhenGetSubRecord(this, m_task);
		break;
	case MTT_GETSESSION_WHEN_FLUSHLOG_1:
	case MTT_GETSESSION_WHEN_FLUSHLOG_2:
		m_testCase->doGetSessionWhenFlushLog(this, m_task);
		break;
	case MTT_FLUSHLOG_VICTIMPAGE:
		m_testCase->doTestPageType(this);
		break;
	case MTT_FLUSH_VICTIMPAGE:
		m_testCase->doTestPageTypeWhenFlush(this);
		break;
	case MTT_DIRTY_REC:
		m_testCase->doTestPageTypeWhenGetDirRec(this);
		break;
	case MTT_GETPIN_REPLACEMENT:
		m_testCase->doReplaceWhenPinTopPage(this);
		break;
	case MTT_AMP_GET_TBL:
	case MTT_AMP_GET_PG:
	case MTT_AMP_PIN_PG:
		m_testCase->doTestAllocMmsPage(this, m_task);
		break;
	case MTT_FLUSHLOG_RPCLASS:
		m_testCase->doTestFlushLogRpClass(this);
		break;
	case MTT_SORTFLUSH_RPCLASS:
		m_testCase->doTestSortFlushRpClass(this);
		break;
	default:
		break;
	}
}

// 辅助测试线程
void MmsTestHelper::run() {
	switch(m_task) {
	case MTT_DOTOUCH:
		m_testCase->doHelpTestTouch(m_tester);
		break;
	case MTT_DEL_DOTOUCH:
		m_testCase->doHelpTestDelWhenTouch(m_tester);
		break;
	case MTT_GETRID_WHEN_LOCKROW_0:
	case MTT_GETRID_WHEN_LOCKROW_1:
	case MTT_GETRID_WHEN_LOCKROW_2:
	case MTT_GETRID_WHEN_LOCKROW_3:
		m_testCase->doHelpGetRidWhenLockRow(m_tester, m_task);
		break;
	case MTT_GETRID_TRYLOCK:
		m_testCase->doHelpTryLockWhenGetByRid(m_tester, m_task);
		break;
	case MTT_GETRID_DISABLEPG:
		m_testCase->doHelpDisablePgWhenGetByRid(m_tester, m_task);
		break;
	case MTT_PUT_DISABLEPG:
		m_testCase->doHelpDisablePgWhenPutIfNotExist(m_tester, m_task);
		break;
	case MTT_GETSUBRECORD_DISABLEPG:
		m_testCase->doHelpDisablePgWhenGetSubRecord(m_tester, m_task);
		break;
	case MTT_GETSESSION_WHEN_FLUSHLOG_1:
	case MTT_GETSESSION_WHEN_FLUSHLOG_2:
		m_testCase->doHelpGetSessionWhenFlushLog(m_tester, m_task);
		break;
	case MTT_FLUSHLOG_VICTIMPAGE:
		m_testCase->doHelpTestPageType(m_tester);
		break;
	case MTT_FLUSH_VICTIMPAGE:
		m_testCase->doHelpTestPageTypeWhenFlush(m_tester);
		break;
	case MTT_DIRTY_REC:
		m_testCase->doHelpTestPageTypeWhenGetDirRec(m_tester);
		break;
	case MTT_GETPIN_REPLACEMENT:
		m_testCase->doHelpReplaceWhenPinTopPage(m_tester);
		break;
	case MTT_AMP_GET_TBL:
	case MTT_AMP_GET_PG:
	case MTT_AMP_PIN_PG:
		m_testCase->doHelpAllocMmsPage(m_tester, m_task);
		break;
	case MTT_FLUSHLOG_RPCLASS:
		m_testCase->doHelpTestFlushLogRpClass(m_tester);
		break;
	case MTT_SORTFLUSH_RPCLASS:
		m_testCase->doHelpTestSortFlushRpClass(m_tester);
		break;
	default:
		break;
	}
}

#endif

