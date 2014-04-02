/**
 * 测试索引操作类
 *
 * @author 苏斌(bsu@corp.netease.com, naturally@163.org)
 */

#include <cppunit/config/SourcePrefix.h>
#include "btree/TestIndexOperation.h"
#include "Test.h"
#include "btree/Index.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"
#include "api/Table.h"
#include "misc/Global.h"
#include "api/Database.h"
#include "heap/Heap.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include "misc/RecordHelper.h"
#include "misc/Session.h"
#include "misc/Trace.h"
#include <stdlib.h>
#include <iostream>
#include <vector>

using namespace std;
using namespace ntse;
#define INDEX_NAME	"testIndex.nti"			/** 索引文件名 */
#define HEAP_NAME	"testIndex.ntd"			/** 堆文件名 */
#define TBLDEF_NAME "testIndex.nttd"        /** TableDef定义文件*/
#define DATA_SCALE	2000					/** 堆和索引的数据规模 */
#define HOLE		1000					/** 用于控制索引数据第一个键值的起始值，保证即使是唯一性约束的索引，索引数据起始值之前还能容纳若干键值插入 */
#define SAME_ID		1200					/** 指定的相同ID的键值 */
#define SAME_SCALE	20						/** 相同ID键值的重复个数 */
#define DUPLICATE_TIMES	3					/** 如果使用，将会对堆中的每一条不同记录，产生指定个数的副本，测试非唯一性索引 */
#define END_SCALE	500						/** 在索引和堆数据默认范围之外，再指定一个数据范围 */
#define FAKE_LONG_CHAR234 "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" \
	"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
#define LONG_KEY_LENGTH	250					/** 长键值属性的最大长度 */

RowId rowids[DATA_SCALE];					/** 用于存储已经插入堆当中数据的rowid信息，最多就是DATA_SCALE个数据，超过不管 */

#define MT_TEST_DATA_SCALE	1000000			/** 多线程测试的数据规模 */
#define MT_TEST_THREAD_NUM	100				/** 多线程测试线程数 */
#define MT_TEST_REPEAT_TIME	100000			/** 多线程重复DML操作次数 */
#define MT_TEST_INIT_SCALE	60000			/** 多线程测试初始索引规模 */

#define HEAP_BACKUP_NAME	"testIndexHeapBackup.ntd"		/** 备份的堆文件名 */
#define INDEX_BACKUP_NAME	"testIndexIDXBackup.nti"		/** 备份的索引文件名 */
#define TBLDEF_BACKUP_NAME  "testIndexTblDefBackup.nttd"    /** 备份的tabledef文件名*/


extern void backupHeapFile(File *file, const char *backupName);
extern void backupHeapFile(const char *origName, const char *backupName);
extern void clearRSFile(Config *cfg);
extern void backupTblDefFile(const char *origName, const char *backupName);

/**
 * 备份一个文件，指定原文件名和备份后的文件名
 */
void myrestoreHeapFile(const char *backupHeapFile, const char *origFile) {
	u64 errCode;
	File bk(backupHeapFile);
	File orig(origFile);
	errCode = orig.remove();
	if (File::getNtseError(errCode) != File::E_NO_ERROR) {
		cout << File::explainErrno(errCode) << endl;
		return;
	}
	while (true) {
		errCode = bk.move(origFile);
		if (File::getNtseError(errCode) != File::E_NO_ERROR) {
			//cout << File::explainErrno(errCode) << endl;
			continue;
		}
		return;
	}
}


/**
 * 根据指定键值，在索引0里面查找
 */
static bool fetchByKeyFromIndex0(Session *session, DrsIndex *index, TableDef *tableDef, u64 userId, u64 bankaccount, RowId rowId) {
	IndexDef *indexDef = tableDef->m_indice[0];
	SubRecordBuilder sb(tableDef, KEY_PAD);
	SubRecord *findKey = sb.createSubRecordByName("UserId"" ""BankAccount", &userId, &bankaccount);
	findKey->m_rowId = rowId;

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tableDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, indexDef->m_numCols, indexDef->m_columns, KEY_COMPRESS, KEY_PAD, 1);

	RowId rowId1;
	bool found = index->getByUniqueKey(session, findKey, None, &rowId1, NULL, NULL, extractor);
	bool succ = found && rowId == findKey->m_rowId;

	freeSubRecord(findKey);

	return succ;
}

/**
 * 范围正向扫描验证索引的总项数和指定项数相等，并且索引最极端的两个叶页面的前驱后继链接正确
 */
bool rangeScanForwardCheck(Session *session, TableDef *tableDef, DrsIndex *index, SubRecord *findKey, SubRecord *getKey, uint totalKeys, bool forceCheck = false) {
	if (isEssentialOnly() && !forceCheck)
		return true;

	IndexDef *indexDef = (IndexDef*)((DrsBPTreeIndex*)index)->getIndexDef();
	SubToSubExtractor *extractor = (getKey == NULL) ? NULL : SubToSubExtractor::createInst(session->getMemoryContext(), tableDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, getKey->m_numCols, getKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);
	IndexScanHandle *indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
	int count = 0;
	while (index->getNext(indexHandle, getKey)) {
		if (count == 0) {
			DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)indexHandle;
			PageHandle *pageHandle = handle->getScanInfo()->m_pageHandle;
			IndexPage *page = (IndexPage*)pageHandle->getPage();
			CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		}
		count++;
	}
	{
		DrsIndexRangeScanHandle *handle = (DrsIndexRangeScanHandle*)indexHandle;
		PageHandle *pageHandle = handle->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
	}
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	return true;
}


/**
 * 范围反向扫描验证索引的总项数相等
 */
bool rangeScanBackwardCheck(Session *session, TableDef *tableDef, DrsIndex *index, SubRecord *findKey, SubRecord *getKey, uint totalKeys, bool forceCheck = false) {
	if (isEssentialOnly() && !forceCheck)
		return true;

	IndexDef *indexDef = (IndexDef*)((DrsBPTreeIndex*)index)->getIndexDef();
	SubToSubExtractor *extractor = (getKey == NULL) ? NULL : SubToSubExtractor::createInst(session->getMemoryContext(), tableDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, getKey->m_numCols, getKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);
	IndexScanHandle *indexHandle;
	indexHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
	uint count = 0;
	while (index->getNext(indexHandle, getKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	return true;
}


/**
 * 创建一条指定记录，用于插入堆或者类似操作使用
 * @post 使用者要释放空间
 */
Record* createRecord(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance) {
	RecordBuilder rb(tableDef, rowid, REC_FIXLEN);
	rb.appendBigInt(userid);
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	return rb.getRecord(tableDef->m_maxRecSize);
}


/**
 * 根据指定信息创建一个子记录，只包含用户名，用于测试更新操作
 * 该函数完全依赖于本测试文件当中对索引的定义
 * @post 使用者要释放空间
 */
SubRecord* createUpdateAfterRecordRedundant2(const TableDef *tableDef, u64 rowid, const char *username) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserName", username);
}


/**
 * 根据指定信息创建一个子记录，只包含用户id，用于测试更新操作
 * 该函数完全依赖于本测试文件当中对索引的定义
 * @post 使用者要释放空间
 */
SubRecord* createUpdateAfterRecordRedundant1(const TableDef *tableDef, u64 rowid, u64 userid) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserId", &userid);
}


/**
 * 根据指定信息创建一个子记录，只包含用户id，bankaccount字段，用于测试更新操作
 * 该函数完全依赖于本测试文件当中对索引的定义
 * @post 使用者要释放空间
 */
SubRecord* createUpdateAfterRecordRedundant3(const TableDef *tableDef, u64 rowid, u64 userid, u64 bankaccount) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserId"" ""BankAccount", &userid, &bankaccount);
}


/**
 * 创建一条完整的子记录，包含所有属性，用于测试更新
 * 该函数完全依赖于本测试文件当中对索引的定义
 * @post 使用者要释放空间
 */
SubRecord* createUpdateAfterRecordRedundantWhole(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance) {
	SubRecordBuilder srb(tableDef, REC_REDUNDANT, rowid);
	return srb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount"" ""Balance", &userid, username, &bankacc, &balance);
}


/**
 * 创建一个冗余格式的指定的记录
 * @post 使用者要释放空间
 */
Record* createRecordRedundant(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance) {
	RecordBuilder rb(tableDef, rowid, REC_REDUNDANT);
	rb.appendBigInt(userid);
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	return rb.getRecord(tableDef->m_maxRecSize);
}


/**
 * 创建一个完整的查找键值子记录
 * 该函数完全依赖于本文件对索引的定义，而且该查找键值一定是对包含用户名字段的索引的查找
 * 因为:1该索引查找内容清楚直观便于调试，2该索引可以测试长键值，3该索引可以测试唯一性约束等等....
 */
SubRecord* makeFindKeyWhole(int count, DrsHeap *heap, const TableDef *tblDef, bool testBig = false, bool testPrefix = false) {
	char name[LONG_KEY_LENGTH];
	if (testBig) {
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", count);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, count);
	} else
		sprintf(name, "Kenneth Tse Jr. %d \0", count);
	u64 userid = count;
	u64 bankaccount = count + ((u64)count << 32);

	SubRecordBuilder sb(tblDef, KEY_PAD);
	return sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
}


/**
 * 测试索引模块稳定性的简单用例
 * 运行多个线程，并发执行DML操作
 */

const char* IndexOPStabilityTestCase::getName() {
	return "Index stability test";
}

const char* IndexOPStabilityTestCase::getDescription() {
	return "Test Index stability(Multi-threads test).";
}

bool IndexOPStabilityTestCase::isBig() {
	return true;
}

void IndexOPStabilityTestCase::setUp() {
	m_heap = NULL;
	m_index = NULL;
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::setUp", m_conn);
	char name[255] = "testIndex.nti";
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	clearRSFile(&m_cfg);
	ts.idx = true;
	vs.idx = true;
}

void IndexOPStabilityTestCase::tearDown() {
	// 丢索引
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// 丢表
	if (m_heap != NULL) {
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		TableDef::drop(TBLDEF_NAME);
		m_tblDef = NULL;
	}

	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		Database::drop(".");
		m_db = NULL;
	}

	ts.idx = false;
	vs.idx = false;
}


/**
* 根据指定的堆和索引定义，创建相关索引
*/
void IndexOPStabilityTestCase::createIndex(char *path, DrsHeap *heap, TableDef *tblDef) throw(NtseException) {
	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(m_db, m_session, path, tblDef, NULL);
	try {
		for (uint i = 0; i < tblDef->m_numIndice; i++) {
			m_index->createIndexPhaseOne(m_session, tblDef->m_indice[i], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}
}


/**
* 关闭使用中的小堆
*/
void IndexOPStabilityTestCase::closeSmallHeap(DrsHeap *heap) {
	EXCPT_OPER(m_heap->close(m_session, true));
	delete m_heap;
}




/**
* 创建非唯一性索引，测试多线程并发的DML操作
* 随机生成操作方式，之后执行操作
*/
void IndexOPStabilityTestCase::testMultiThreadsDML() {
	uint step = 4;
	// 测试前缀压缩超过128字节
	uint totalKeys;
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	totalKeys = buildHeapWithSame(m_heap, m_tblDef, MT_TEST_INIT_SCALE, step);
	EXCPT_OPER(createIndex(m_path, m_heap, m_tblDef));

	u64 start = System::currentTimeMillis();
	cout << "Start time: " << start << endl;

	HalfHeartedMan *pool[MT_TEST_THREAD_NUM];
	Connection *connPool[MT_TEST_THREAD_NUM];
	for (uint i = 0; i < MT_TEST_THREAD_NUM; i++) {
		connPool[i] = m_db->getConnection(false);
		pool[i] = new HalfHeartedMan("I'm force to be of two minds, help...", m_db, connPool[i], m_heap, m_tblDef, m_index, 0, INVALID_ROW_ID, MT_TEST_REPEAT_TIME);
		pool[i]->start();
	}

	for (uint i = 0; i < MT_TEST_THREAD_NUM; i++) {
		pool[i]->join();
		delete pool[i];
		m_db->freeConnection(connPool[i]);
	}

	{	// 需要检验索引和堆的数据一致
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TestMultithreadDML", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;

		IndexDef *indexDef = m_tblDef->m_indice[1];
		// TODO idxKey的属性号不是递增的
		DrsHeapScanHandle *scanHandle = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, idxKey), None, NULL, false);
		int count = 0;
		while (m_heap->getNext(scanHandle, idxKey)) {	// 对每一条记录到索引中寻找，必定能找到
			idxKey->m_format = KEY_PAD;
			RowId rowId;
			bool found = index->getByUniqueKey(session, idxKey, None, &rowId, NULL, NULL, NULL);
			if (!found) {
				Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
				record->m_format = REC_FIXLEN;
				m_heap->getRecord(session, idxKey->m_rowId, record, None, NULL);
				index->getByUniqueKey(session, idxKey, None, &rowId, NULL, NULL, NULL);
			}
			CPPUNIT_ASSERT(found);
			CPPUNIT_ASSERT(rowId == idxKey->m_rowId);
			idxKey->m_format = REC_REDUNDANT;
		}
		m_heap->endScan(scanHandle);
	}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	u64 end = System::currentTimeMillis();
	cout << "End time: " << end << endl;
	cout << "Duration: " << end - start << endl;
}


TableDef* IndexOPStabilityTestCase::createSmallTableDefWithSame() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));

	delete builder;

	return tableDef;
}
/**
* 创建一个小堆定义以及相关索引定义
* 两个索引都是非唯一性索引
*/
DrsHeap* IndexOPStabilityTestCase::createSmallHeapWithSame(TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}


/**
* 在指定堆中插入数据，最后要保证插入若干个重复键值
* 主要为了测试索引唯一性冲突相关的操作处理
*/
uint IndexOPStabilityTestCase::buildHeapWithSame(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		for (int j = 0; j < DUPLICATE_TIMES; j++) {
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		}
		freeRecord(record);
	}

	int i = SAME_ID;
	for (int j = 0; j < SAME_SCALE; j++) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
	}

	return size * DUPLICATE_TIMES / step + SAME_SCALE;

}


/************************************************************************/
/* 测试索引基本操作类
/* 测试用例将使用一个小堆执行，创建两个索引定义，第一个索引是非唯一性的
/* 第二个索引是唯一性索引，便于有需要测试唯一性冲突使用
/* 有的用例可能会用到大堆，要测试索引对长键值的处理
/* 有的测试用例可能会用到两个索引都是非唯一性索引的小堆，具体可以看测试用例
/* 测试用例当中所有用到的索引都是采用前述的两个，至于操作哪一个索引执行
/* 具体的操作根据测试用例自己的需求选择
*/
/************************************************************************/

const char* IndexOperationTestCase::getName() {
	return "Index operation test";
}

const char* IndexOperationTestCase::getDescription() {
	return "Test Index operations(Unique scan/RangeScan/Insert/Delete/Update and so on).";
}

bool IndexOperationTestCase::isBig() {
	return false;
}

void IndexOperationTestCase::setUp() {
	m_heap = NULL;
	m_index = NULL;
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::setUp", m_conn);
	char name[255] = "testIndex.nti";
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	ts.idx = true;
	vs.idx = true;
}

void IndexOperationTestCase::tearDown() {
	// 丢索引
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// 丢表
	if (m_heap != NULL) {
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		TableDef::drop(TBLDEF_NAME);
		m_tblDef = NULL;
	}

	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		Database::drop(".");
		m_db = NULL;
	}

	vs.idx = false;
	ts.idx = false;
}


/**
 * 根据指定的堆和索引定义，创建相关索引
 */
void IndexOperationTestCase::createIndex(char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException) {
	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(m_db, m_session, path, tblDef, NULL);
	try {
		for (uint i = 0; i < tblDef->m_numIndice; i++) {
			m_index->createIndexPhaseOne(m_session, tblDef->m_indice[i], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}
}

TableDef* IndexOperationTestCase::createSmallTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50, false, false);
	builder->addColumn("BankAccount", CT_BIGINT, false)->addColumn("Balance", CT_INT, false);
	builder->addIndex("NOCONOSTRAIN", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));

	delete builder;

	return tableDef;
}

/**
 * 创建一个小堆定义以及相关的索引定义
 * 这里的索引定义第一个是非唯一性索引，第二个是唯一性索引
 */
DrsHeap* IndexOperationTestCase::createSmallHeap(const TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}

TableDef *IndexOperationTestCase::createSmallTableDefWithSame() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE1", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("RANGE2", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));

	delete builder;

	return tableDef;
}

/**
 * 创建一个小堆定义以及相关索引定义
 * 两个索引都是非唯一性索引
 */
DrsHeap* IndexOperationTestCase::createSmallHeapWithSame(const TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}



/**
 * 关闭使用中的小堆
 */
void IndexOperationTestCase::closeSmallHeap(DrsHeap *heap) {
	EXCPT_OPER(m_heap->close(m_session, true));
	delete m_heap;
}


/**
 * 创建大表数据，该表为了测试索引键值超过128而建立
 * 因此表中设计索引的字符串长度必须达到甚至超过128
 */
uint IndexOperationTestCase::buildBigHeap(DrsHeap *heap, const TableDef *tblDef, bool testPrefix, uint size, uint step) {
	Record *record;
	uint count = 0;
	for (uint i = HOLE; i < HOLE + size; i += step) {
		char name[LONG_KEY_LENGTH];
		RowId rid;
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}

TableDef* IndexOperationTestCase::createBigTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, LONG_KEY_LENGTH);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));
	delete builder;

	return tableDef;
}

/**
 * 创建大堆，记录长度超过100多
 * 其中只包含一个索引，并且是非唯一性的索引
 */
DrsHeap* IndexOperationTestCase::createBigHeap(const TableDef *tableDef) {
	DrsHeap *smallHeap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(smallHeap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return smallHeap;
}


/**
 * 根据指定的数据范围，在指定的堆中插入数据
 */
uint IndexOperationTestCase::buildHeap(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	uint count = 0;
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		if (i - HOLE < DATA_SCALE)
			rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}


/**
 * 在指定堆中插入数据，最后要保证插入若干个重复键值
 * 主要为了测试索引唯一性冲突相关的操作处理
 */
uint IndexOperationTestCase::buildHeapWithSame(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		for (int j = 0; j < DUPLICATE_TIMES; j++) {
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		}
		freeRecord(record);
	}

	int i = SAME_ID;
	for (int j = 0; j < SAME_SCALE; j++) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
	}

	return size * DUPLICATE_TIMES / step + SAME_SCALE;
}



/**
 *  测试长键值，超过256的键值，造成索引键值存储长度位需要两位的情况
 */
void IndexOperationTestCase::testLongKey() {
	uint step = 2;
	uint testScale = DATA_SCALE;
#if NTSE_PAGE_SIZE < 8192
	testScale = DATA_SCALE / 10;
#endif
	// 测试前缀压缩超过128字节
	uint totalKeys;
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	totalKeys = buildBigHeap(m_heap, m_tblDef, true, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	DrsIndex *index = m_index->getIndex(0);

	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);


	// 测试长记录索引的插入
	{
		for (uint i = HOLE + 1; i < HOLE + testScale * 3; i += step) {
			char name[LONG_KEY_LENGTH];
			RowId rid;
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
			Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));

			freeRecord(insertrecord);

			insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			insertrecord->m_rowId = rid;

			bool successful;
			uint dupIndex;
			session->startTransaction(TXN_INSERT, m_tblDef->m_id);
			EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
			session->endTransaction(true);
			CPPUNIT_ASSERT(successful);
			totalKeys++;

			freeRecord(insertrecord);
		}

		// 验证索引完整
		checkIndex(session, memoryContext, totalKeys);

		DrsIndex *index = m_index->getIndex(0);

		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	}


	// 测试长记录索引的删除
	{
//		SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		record->m_columns = m_tblDef->m_indice[0]->m_columns;
		record->m_numCols = m_tblDef->m_indice[0]->m_numCols;
		RowId rowId;
		SubRecord *findKey;
		Record *heapRec = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
		index = m_index->getIndex(0);
		IndexDef *indexDef = m_tblDef->m_indice[0];

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		uint foundKeys = 0;
		for (int i = HOLE; i < HOLE + testScale * 3; i++) {
			{
				char name[LONG_KEY_LENGTH];
				sprintf(name, FAKE_LONG_CHAR234 "%d", i);
				u64 userid = i;

				SubRecordBuilder sb(m_tblDef, KEY_PAD);
				findKey = sb.createSubRecordByName("UserId"" ""UserName", &userid, name);
			}

			findKey->m_rowId = INVALID_ROW_ID;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
			if (found) {	// 读取表记录，执行删除操作
				CPPUNIT_ASSERT(record->m_rowId != INVALID_ROW_ID);

				foundKeys++;

				heapRec->m_format = REC_FIXLEN;
				m_heap->getRecord(session, record->m_rowId, heapRec);
				heapRec->m_format = REC_REDUNDANT;

				session->startTransaction(TXN_DELETE, m_tblDef->m_id);
				EXCPT_OPER(m_index->deleteIndexEntries(session, heapRec, NULL));
				session->endTransaction(true);
				totalKeys--;
			}

			freeSubRecord(findKey);
		}
		CPPUNIT_ASSERT(foundKeys != 0);

		// 验证索引完整
		checkIndex(session, memoryContext, totalKeys);

		DrsIndex *index = m_index->getIndex(0);

		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	}


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	// 测试后缀压缩超过128字节
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	totalKeys = buildBigHeap(m_heap, m_tblDef, false, 100, step);
	createIndex(m_path, m_heap, m_tblDef);

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	index = m_index->getIndex(0);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 测试各种扫描操作，在核心测试的时候启用
 * 具体测试项目，参见testUniqueScan和testRangeScan的说明
 */
void IndexOperationTestCase::testScan() {
	if (!isEssentialOnly())
		return;

	uint step = 1;

	uint testScale = DATA_SCALE / 4;
	// 建立测试索引
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, testScale, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	// 唯一性扫描测试部分
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// 对索引0查找测试覆盖利用可快速比较键值唯一查找的功能
		{
			DrsIndex *index = m_index->getIndex(0);
			CPPUNIT_ASSERT(fetchByKeyFromIndex0(session, index, m_tblDef, HOLE, HOLE + ((u64)HOLE << 32), 8192));
		}

		// 对索引1进行查找
		SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		record->m_columns = m_tblDef->m_indice[1]->m_columns;
		record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
		RowLockHandle *rlh;
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		// 验证所有插入的键值都能被找到，同时测试加锁
		rlh = NULL;
		for (int i = HOLE; i < testScale + HOLE; i += step) {
			CPPUNIT_ASSERT(findAKey(session, index, &rlh, i, extractor));
			if (rlh != NULL)
				session->unlockRow(&rlh);
		}

		// 验证没有插入的键值肯定不会被找到
		for (int i = HOLE - 2; i < HOLE; i++)
			CPPUNIT_ASSERT(!findAKey(session, index, NULL, i, extractor));
		for (int i = testScale + HOLE; i < testScale + HOLE + 2; i++)
			CPPUNIT_ASSERT(!findAKey(session, index, NULL, i, extractor));

		if (step > 1) {
			for (int i = HOLE + 1; i < testScale + HOLE + HOLE; i += step)
				CPPUNIT_ASSERT(!findAKey(session, index, NULL, i, extractor));
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	// 范围扫描测试部分
	{
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRangeScan", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// 对索引1进行查找
		SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		record->m_columns = m_tblDef->m_indice[1]->m_columns;
		record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
		SubRecord *findKey;

		uint count;
		IndexScanHandle *scanHandle;
		RowLockHandle *rlh;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		/**********************************************************************************************************/
		// 加锁
		scanHandle = index->beginScan(session, NULL, true, false, Shared, &rlh, extractor);
		count = 0;
		while (index->getNext(scanHandle, record)) {
			count++;
			session->unlockRow(&rlh);
		}
		CPPUNIT_ASSERT(count == totalKeys);
		index->endScan(scanHandle);
		//2.反向全扫描
		scanHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record)) {
			count++;
		}
		CPPUNIT_ASSERT(count == totalKeys);
		index->endScan(scanHandle);

		/**********************************************************************************************************/


		/**********************************************************************************************************/
		// 做部分索引RangeScan测试，随机产生若干个键值(最后必定包含两个键值在索引范围外)，对每个键值做>=/>/</<=四项测试
		uint keyShouldFound;
		uint start, end;
		start = 0;
		end = 20;
		for (uint i = start/*DATA_SCALE + HOLE / 2*/; i < end/*DATA_SCALE + HOLE + 2*/; i++) {
			uint num = rand() % testScale + HOLE;
			//uint num = 1009;
			if (i == end - 1)	// 这三个判断保证测试能测试到索引范围外的键值以及做大范围相同键值测试
				num = HOLE - 1;
			else if (i == end - 2)
				num = testScale + HOLE;

			findKey = makeFindKeyWhole(num, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;

			// >=测试
			scanHandle = index->beginScan(session, findKey, true, true, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num != SAME_ID) {
				if (num < SAME_ID)
					keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES + SAME_SCALE;
				else
					keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (testScale + HOLE - SAME_ID);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			// >测试
			scanHandle = index->beginScan(session, findKey, true, false, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num != SAME_ID) {
				if (num < SAME_ID)
					keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES + SAME_SCALE;
				else
					keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = DUPLICATE_TIMES * (testScale + HOLE - SAME_ID - 1);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			// <=测试
			scanHandle = index->beginScan(session, findKey, false, true, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num != SAME_ID) {
				uint keyShouldFound;
				if (num > SAME_ID)
					keyShouldFound = totalKeys - (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
				else
					keyShouldFound = (num - HOLE + 1) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (SAME_ID - HOLE + 1);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			// <测试
			scanHandle = index->beginScan(session, findKey, false, false, None, NULL, extractor);
			count = 0;
			while (index->getNext(scanHandle, record))
				count++;
			if (num < HOLE)
				CPPUNIT_ASSERT(count == 0);
			else if (num >= testScale + HOLE)
				CPPUNIT_ASSERT(count == totalKeys);
			else if (num != SAME_ID) {
				uint keyShouldFound;
				if (num > SAME_ID)
					keyShouldFound = totalKeys - (testScale + HOLE - num) * DUPLICATE_TIMES;
				else
					keyShouldFound = (num - HOLE) * DUPLICATE_TIMES;
				CPPUNIT_ASSERT(count == keyShouldFound);
			} else {
				keyShouldFound = DUPLICATE_TIMES * (SAME_ID - HOLE);
				CPPUNIT_ASSERT(count == keyShouldFound);
			}
			index->endScan(scanHandle);

			freeSubRecord(findKey);
		}
		/**********************************************************************************************************/

		m_index->close(m_session, true);
		delete m_index;
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

	}
}


/**
 * 测试唯一性查询包括加锁
 * @case1: 建立一个带有批量数据的索引
 *			验证索引当中的每一个键值都能找到
 *			验证每个介于任意索引相连的两个记录之间的不存在于索引当中的记录都不能被找到
 *			验证索引包含键值范围外的记录都不能被找到
 *			测试查询的同时需要加锁
 */
void IndexOperationTestCase::testUniqueScan() {
	if (isEssentialOnly())
		return;

	uint step = 5;
	uint testScale = DATA_SCALE / 5;
	// 建立测试索引
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	buildHeap(m_heap, m_tblDef, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// 对索引1进行查找
//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[1]->m_columns;
	record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	RowId rowId;
	SubRecord *findKey;
	RowLockHandle *rlh;
	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

	// 验证所有插入的键值都能被找到，同时测试加锁
	rlh = NULL;
	for (int i = HOLE; i < testScale + HOLE; i += step) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		CPPUNIT_ASSERT(rlh == NULL);
		bool found = index->getByUniqueKey(session, findKey, Exclusived, &rowId, record, &rlh, extractor);
		session->unlockRow(&rlh);
		CPPUNIT_ASSERT(found);
		CPPUNIT_ASSERT(rowId == record->m_rowId);
		freeSubRecord(findKey);
	}

	// 验证没有插入的键值肯定不会被找到
	for (int i = HOLE - 2; i < HOLE; i++) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
		CPPUNIT_ASSERT(!found);
		freeSubRecord(findKey);
	}
	for (int i = testScale + HOLE; i < testScale + HOLE + 2; i++) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
		CPPUNIT_ASSERT(!found);
		freeSubRecord(findKey);
	}

	if (step > 1 && !isEssentialOnly()) {
		for (int i = HOLE + 1; i < testScale + HOLE + HOLE; i += step) {
			findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);
		}
	}

	m_index->close(m_session, true);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
}


/**
 * 测试范围查询
 * @case1: 建立一个完整的索引，包含批量数据
 *			对索引执行全范围的正向/反向/分别都加锁的四种扫描，验证扫描结果正确
 * @case2: 在case1索引的基础上，随机产生若干个键值，使用该键值对索引执行>/>=/</<=的四种查询
 *			验证每次查询的结果都正确。要特别验证查找的键值在索引包含键值范围之外的查询
 */
void IndexOperationTestCase::testRangeScan() {
	if (isEssentialOnly())
		return;

	uint step = 1;

	uint testScale = DATA_SCALE / 4;
	// 建立测试索引
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, testScale, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRangeScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// 对索引1进行查找
//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[1]->m_columns;
	record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	SubRecord *findKey;

	uint count;
	IndexScanHandle *scanHandle;
	RowLockHandle *rlh;

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	/**********************************************************************************************************/
	// 做全索引range测试
	//// 1.正向全扫描
	//scanHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
	//count = 0;
	//while (index->getNext(scanHandle, record)) {
	//	count++;
	//	CPPUNIT_ASSERT(scanHandle->getRowId() != INVALID_ROW_ID);
	//}
	//CPPUNIT_ASSERT(count == totalKeys);
	//index->endScan(scanHandle);

	// 加锁
	scanHandle = index->beginScan(session, NULL, true, false, Shared, &rlh, extractor);
	count = 0;
	while (index->getNext(scanHandle, record)) {
		count++;
		session->unlockRow(&rlh);
	}
	CPPUNIT_ASSERT(count == totalKeys);
	index->endScan(scanHandle);
	//2.反向全扫描
	scanHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(scanHandle, record)) {
		count++;
	}
	CPPUNIT_ASSERT(count == totalKeys);
	index->endScan(scanHandle);

	//// 加锁
	//scanHandle = index->beginScan(session, NULL, false, false, Exclusived, &rlh, extractor);
	//count = 0;
	//while (index->getNext(scanHandle, record)) {
	//	count++;
	//	session->unlockRow(&rlh);
	//}
	//CPPUNIT_ASSERT(count == totalKeys);
	//index->endScan(scanHandle);
	/**********************************************************************************************************/


	/**********************************************************************************************************/
	// 做部分索引RangeScan测试，随机产生若干个键值(最后必定包含两个键值在索引范围外)，对每个键值做>=/>/</<=四项测试
	uint keyShouldFound;
	uint start, end;
	start = 0;
	end = 20;
	for (uint i = start/*DATA_SCALE + HOLE / 2*/; i < end/*DATA_SCALE + HOLE + 2*/; i++) {
		uint num = rand() % testScale + HOLE;
		//uint num = 1009;
		if (i == end - 1)	// 这三个判断保证测试能测试到索引范围外的键值以及做大范围相同键值测试
			num = HOLE - 1;
		else if (i == end - 2)
			num = testScale + HOLE;

		findKey = makeFindKeyWhole(num, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;

		// >=测试
		scanHandle = index->beginScan(session, findKey, true, true, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num != SAME_ID) {
			if (num < SAME_ID)
				keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES + SAME_SCALE;
			else
				keyShouldFound = (testScale + HOLE - num) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (testScale + HOLE - SAME_ID);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		// >测试
		scanHandle = index->beginScan(session, findKey, true, false, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num != SAME_ID) {
			if (num < SAME_ID)
				keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES + SAME_SCALE;
			else
				keyShouldFound = (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = DUPLICATE_TIMES * (testScale + HOLE - SAME_ID - 1);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		// <=测试
		scanHandle = index->beginScan(session, findKey, false, true, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num != SAME_ID) {
			uint keyShouldFound;
			if (num > SAME_ID)
				keyShouldFound = totalKeys - (testScale + HOLE - num - 1) * DUPLICATE_TIMES;
			else
				keyShouldFound = (num - HOLE + 1) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = SAME_SCALE + DUPLICATE_TIMES * (SAME_ID - HOLE + 1);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		// <测试
		scanHandle = index->beginScan(session, findKey, false, false, None, NULL, extractor);
		count = 0;
		while (index->getNext(scanHandle, record))
			count++;
		if (num < HOLE)
			CPPUNIT_ASSERT(count == 0);
		else if (num >= testScale + HOLE)
			CPPUNIT_ASSERT(count == totalKeys);
		else if (num != SAME_ID) {
			uint keyShouldFound;
			if (num > SAME_ID)
				keyShouldFound = totalKeys - (testScale + HOLE - num) * DUPLICATE_TIMES;
			else
				keyShouldFound = (num - HOLE) * DUPLICATE_TIMES;
			CPPUNIT_ASSERT(count == keyShouldFound);
		} else {
			keyShouldFound = DUPLICATE_TIMES * (SAME_ID - HOLE);
			CPPUNIT_ASSERT(count == keyShouldFound);
		}
		index->endScan(scanHandle);

		freeSubRecord(findKey);

//		printf("%d passed\n", i);
		// TODO：范围的=测试
	}
	/**********************************************************************************************************/

	m_index->close(m_session, true);
	delete m_index;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
}


/**
 * 测试索引插入相关操作
 * @case1: 首先创建一个索引具有一定的数据量，且是唯一索引
 *			其次插入索引键值范围内的在当前索引当中不存在的键值，验证都能被成功插入
 *			插入当前范围外的索引记录，保证都能被插入
 *			做tableScan，得到若干条记录，验证已经存在的记录无法被插入，保证索引唯一性约束判断正确
 *			验证经过所有插入之后，索引的键值数量和表的键值数量相等，说明插入都正确
 */
void IndexOperationTestCase::testInsert() {
	/***************************************************************************************/
	uint testScale = DATA_SCALE / 5;
	// 测试唯一性索引插入
	uint totalKeys;
	uint step = 3;
	// 建立测试索引
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testInsert", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	// 对索引1进行查找
	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	uint dupIndex;

	// 验证所有索引没有的键值都能被插入
	for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
		// 首先插入堆
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}
	for (int i = HOLE + 2; i < testScale + HOLE + END_SCALE; i += step) {	// 再附加插入范围外的数据
		// 首先插入堆
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}

//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
	u16 *columns = new u16[m_tblDef->m_numCols];
	for (int k = 0; k < m_tblDef->m_numCols; k++)
		columns[k] = (u16)k;

	// 验证插入之后索引总的项数相等
	// 做索引扫描，得到总项数
	DrsIndex *index = m_index->getIndex(0);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// 验证已经插入的键值都不能被插入
	// 做tablescan，得到若干个record，验证都不能被插入
	uint reinsertNums = 20;
	IndexDef *indexDef = m_tblDef->m_indice[1];
	idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	idxKey->m_columns = columns;
	idxKey->m_numCols = m_tblDef->m_numCols;
	DrsHeapScanHandle *scanHandle = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, idxKey), None, NULL, false);
	int count = 0;
	while (true) {
		if (m_heap->getNext(scanHandle, idxKey)) {
			if (!m_heap->getNext(scanHandle, idxKey))
				break;
		} else
			break;

		record->m_size = idxKey->m_size;
		record->m_rowId = idxKey->m_rowId;
		memcpy(record->m_data, idxKey->m_data, idxKey->m_size);

		try {
			session->startTransaction(TXN_INSERT, m_tblDef->m_id);
			CPPUNIT_ASSERT(!m_index->insertIndexEntries(session, record, &dupIndex));
			session->endTransaction(true);
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
		}

		checkIndex(session, memoryContext, totalKeys);

		count++;

		if (count > reinsertNums)
			break;
	}
	m_heap->endScan(scanHandle);

	// 验证插入之后索引总的项数相等
	// 做索引扫描，得到总项数
	idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	delete[] columns;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	/***************************************************************************************/
}


/**
 * 测试索引删除相关操作
 * @case1: 建立一个表的两个索引，包含批量数据，然后按照隔两个键值记录删除一个的方式，正向遍历删除索引的记录，
 *			无限循环，直到索引为空，验证每次删除都是成功
 * @case2: 建立一个表的两个索引，包含批量数据，然后按照隔两个键值记录删除一个的方式，反向遍历删除索引的记录，
 *			无限循环，直到索引为空，验证每次删除都是成功
 * @说明：前两个测试用例为的是测试两种删除方式带来的不同删除SMO的方式不同，最大程度覆盖代码
 * @case4: 执行范围删除，将整个索引删空，验证索引为空
 */
void IndexOperationTestCase::testDelete() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDelete", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	uint testScale = DATA_SCALE / 5;

	/***************************************************************************************/
	// 测试删除
	{
		uint totalKeys, items;
		uint step = 1;
		// 建立测试索引
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		checkIndex(session, memoryContext, totalKeys);

		// 对索引进行删除
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		// 建立一个有序记录集，按照隔两条记录删除一条的方式进行删除，循环到数组为空
		DrsIndex *index = m_index->getIndex(0);
		int no[DATA_SCALE];
		int seq = 0;
		for (int i = HOLE; i < HOLE + testScale; i++) {
			no[i - HOLE] = 1;
		}
		for (int i = HOLE; i < HOLE + testScale; i++) {
			// 首先确定当前要删除的记录
			for (int j = 0; j < 2; j++) {
				while (true) {
					seq++;
					if (seq == testScale)
						seq = 0;
					if (no[seq] != 0)
						break;
				}
			}
			no[seq] = 0;
			int key = seq + HOLE;

			char name[50];
			sprintf(name, "Kenneth Tse Jr. %d \0", key);
			record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
			record->m_rowId = rowids[seq];
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			EXCPT_OPER(m_index->deleteIndexEntries(session, record, NULL));
			session->endTransaction(true);

			freeRecord(record);

			checkIndex(session, memoryContext, totalKeys);

			items--;
		}

		// 范围扫描验证索引为空
		IndexScanHandle *indexHandle;
		//SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->m_indice[0],
			m_tblDef->m_indice[0]->m_numCols, m_tblDef->m_indice[0]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);
		indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
		int count = 0;
		while (index->getNext(indexHandle, idxKey))
			count++;
		PageHandle *pageHandle = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
		index->endScan(indexHandle);
		CPPUNIT_ASSERT(count == items);
		//CPPUNIT_ASSERT(count == 0);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{
		uint totalKeys, items;
		uint step = 1;
		// 建立测试索引
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		checkIndex(session, memoryContext, totalKeys);

		// 对索引进行删除
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		IndexScanHandle *indexHandle;
	//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->m_indice[0],
			m_tblDef->m_indice[0]->m_numCols, m_tblDef->m_indice[0]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		// 建立一个有序记录集，按照隔两条记录删除一条的方式进行删除，循环到数组为空
		DrsIndex *index = m_index->getIndex(0);
		int no[DATA_SCALE];
		int seq = testScale - 1;
		for (int i = HOLE; i < HOLE + testScale; i++) {
			no[i - HOLE] = 1;
		}
		for (int i = HOLE; i < HOLE + testScale; i++) {
			// 首先确定当前要删除的记录
			for (int j = 0; j < 2; j++) {
				while (true) {
					seq--;
					if (seq == -1)
						seq = testScale - 1;
					if (no[seq] != 0)
						break;
				}
			}
			no[seq] = 0;
			int key = seq + HOLE;

			char name[50];
			sprintf(name, "Kenneth Tse Jr. %d \0", key);
			record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
			record->m_rowId = rowids[seq];
			try {
				session->startTransaction(TXN_DELETE, m_tblDef->m_id);
				m_index->deleteIndexEntries(session, record, NULL);
				session->endTransaction(true);
			} catch (NtseException &e) {
				cout << e.getMessage() << endl;
			}

			freeRecord(record);

			checkIndex(session, memoryContext, totalKeys);

			items--;
		}

		indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
		int count = 0;
		while (index->getNext(indexHandle, idxKey))
			count++;
		PageHandle *pageHandle = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
		index->endScan(indexHandle);
		CPPUNIT_ASSERT(count == items);
		//CPPUNIT_ASSERT(count == 0);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{
		uint testRangeScale = 30;
		uint totalKeys, items;
		uint step = 1;
		// 建立测试索引
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testRangeScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		checkIndex(session, memoryContext, totalKeys);

		// 对索引进行删除
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		IndexScanHandle *indexHandle;
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->m_indice[0],
			m_tblDef->m_indice[0]->m_numCols, m_tblDef->m_indice[0]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		u64 savePoint = memoryContext->setSavepoint();
		RowLockHandle *rlh;
		//	范围删除全部数据
		DrsIndex *rangeIndex = m_index->getIndex(0);
		indexHandle = rangeIndex->beginScan(session, NULL, true, true, Exclusived, &rlh, extractor);
		int count = 0;
		while (rangeIndex->getNext(indexHandle, idxKey)) {
			// 去对应Heap取记录
			record->m_format = REC_FIXLEN;
			m_heap->getRecord(m_session, idxKey->m_rowId, record);
			record->m_format = REC_REDUNDANT;
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			m_index->deleteIndexEntries(session, record, indexHandle);
			session->endTransaction(true);
			session->unlockRow(&rlh);
			count++;

			checkIndex(session, memoryContext, totalKeys);
		}
		rangeIndex->endScan(indexHandle);
		CPPUNIT_ASSERT(count == totalKeys);

		memoryContext->resetToSavepoint(savePoint);

		// 验证索引为空
		DrsIndex *index = m_index->getIndex(0);
		IndexDef *indexDef = m_tblDef->m_indice[0];
		indexHandle = index->beginScan(session, NULL, true, true, Exclusived, &rlh, extractor);
		CPPUNIT_ASSERT(!index->getNext(indexHandle, idxKey));
		PageHandle *pageHandle = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageHandle;
		IndexPage *page = (IndexPage*)pageHandle->getPage();
		CPPUNIT_ASSERT(page->m_prevPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_nextPage == INVALID_ROW_ID);
		CPPUNIT_ASSERT(page->m_pageCount == 0);
		index->endScan(indexHandle);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


u64 getFirstLeafPageLSN(Session *session, MemoryContext *memoryContext, DrsIndex *index) {
	u64 lsn;
	IndexScanHandle *indexHandle;
	indexHandle = index->beginScan(session, NULL, true, true, None, NULL, NULL);
	while (index->getNext(indexHandle, NULL)) {
		lsn = ((DrsIndexRangeScanHandle*)indexHandle)->getScanInfo()->m_pageLSN;
		break;
	}
	index->endScan(indexHandle);
	return lsn;
}


/**
 * 测试索引更新操作
 * @case1: 建立包含批量数据的索引，将索引的数据内容全部更新，而且更新第一个属性，导致索引的更新会造成大范围的数据移动
 *			验证更新之后的键值都能被查询找到，更新之前的都不行
 * @case2: 在前面更新的基础上，将第一个属性的键值进行加1更新，也就是将相邻的两个索引键值的前面那个记录的键值更新为和后面记录键值相等
 *			由于索引是唯一性索引，因此这样的操作都不会成功，除了索引的最后一个键值会成功。最后验证只有一项更新成功
 */
void IndexOperationTestCase::testUpdate() {
	uint testScale = DATA_SCALE / 10;
	{
		uint totalKeys, items;
		uint step = 1;
		// 建立测试索引
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;

		/**
		// 批量更新两个索引的第一个键值，造成索引插入删除位置有较大偏移的情况
		// 原来表包含userid, name, bankaccount, balance,其中userid从HOLE~~testScale+HOLE-1，后续属性使用userid的值
		// 现在只将userid更新，将原来的userid加上testScale+HOLE，但是其他属性的值不变，仍旧使用HOLE~~testScale+HOLE
		 */
		{
			const struct IndexStatus *status = &(m_index->getIndex(1)->getStatus());

			SubRecord *before, *after;
			before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

			u16 *columns = new u16[m_tblDef->m_numCols];
			for (int k = 0; k < m_tblDef->m_numCols; k++)
				columns[k] = (u16)k;

			before->m_numCols = m_tblDef->m_numCols;
			before->m_columns = columns;

			Record *oldrecord;
			uint updated = 0;
			for (uint i = HOLE; i < testScale + HOLE; i++) {
				// 构造新键值
				char name[50];
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				after = createUpdateAfterRecordRedundant1(m_tblDef, rowids[i - HOLE], i + testScale + HOLE);

				// 构造旧键值
				oldrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
				oldrecord->m_rowId = rowids[i - HOLE];
				before->m_size = oldrecord->m_size;
				before->m_data = oldrecord->m_data;
				before->m_rowId = oldrecord->m_rowId;

				session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
				// 进行更新
				uint dupIndex = (uint)-1;
				if (m_index->updateIndexEntries(session, before, after, false, &dupIndex))
					updated++;
				session->endTransaction(true);

				CPPUNIT_ASSERT(dupIndex == (uint)-1);

				freeSubRecord(after);
				freeRecord(oldrecord);

				checkIndex(session, memoryContext, totalKeys);
			}

			delete[] columns;

			CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == updated);
		}

		// 验证更新后的键值都能找到，更新之前的都找不到
		{
			SubRecord *findKey;

			IndexDef *indexDef = m_tblDef->m_indice[1];
			DrsIndex *index = m_index->getIndex(1);
//			SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
			SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
			record->m_columns = m_tblDef->m_indice[1]->m_columns;
			record->m_numCols = m_tblDef->m_indice[1]->m_numCols;

			SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
				indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

			for (uint i = HOLE; i < testScale + HOLE; i++) {
				char name[50];
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				u64 userid = i;
				u64 bankaccount = i + ((u64)i << 32);

				SubRecordBuilder sb(m_tblDef, KEY_PAD);
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;

				RowId rowId;
				bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(!found);

				freeSubRecord(findKey);

				userid = userid + testScale + HOLE;
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;
				found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(found);
				freeSubRecord(findKey);
			}

			rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
			rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		}


		/**
		// 测试更新失败回退，在前面更新的基础上，将前一条记录更新成顺序的后一条记录，在索引当中会出现冲突，
		// 除了最后一条索引记录可以更新成功，其他都不会成功
		// 例如：将userid为HOLE + testScale + HOLE的键值完全更新为HOLE+testScale+HOLE+1的键值内容
		 */
		{
			const struct IndexStatus *status = &(m_index->getIndex(1)->getStatus());

			SubRecord *before, *after;
			before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

			u16 *columns = new u16[m_tblDef->m_numCols];
			for (int k = 0; k < m_tblDef->m_numCols; k++)
				columns[k] = (u16)k;

			before->m_numCols = m_tblDef->m_numCols;
			before->m_columns = columns;

			Record *oldrecord;
			uint totalUpdated = (uint)status->m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE];
			for (uint i = HOLE; i < testScale + HOLE; i++) {
				// 构造新键值
				char name[50];
				uint newi = i + testScale + HOLE + 1;
				sprintf(name, "Kenneth Tse Jr. %d \0", i + 1);
				after = createUpdateAfterRecordRedundantWhole(m_tblDef, rowids[i - HOLE], newi, name, (i + 1) + ((u64)(i + 1) << 32), (u32)(-1) - (i + 1));

				// 构造旧键值
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				oldrecord = createRecord(m_tblDef, 0, i + testScale + HOLE, name, i + ((u64)i << 32), (u32)(-1) - i);
				oldrecord->m_rowId = rowids[i - HOLE];
				before->m_size = oldrecord->m_size;
				before->m_data = oldrecord->m_data;
				before->m_rowId = oldrecord->m_rowId;

				session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
				// 进行更新
				uint dupIndex = (uint)-1;
				bool successful = m_index->updateIndexEntries(session, before, after, false, &dupIndex);
				session->endTransaction(true);
				CPPUNIT_ASSERT(i == testScale + HOLE - 1 || dupIndex == 1);
				if (i < testScale + HOLE - 1)
					CPPUNIT_ASSERT(!successful);
				else
					CPPUNIT_ASSERT(successful);

				if (successful)
					totalUpdated++;

				//checkIndex(session, memoryContext, totalKeys);

				freeSubRecord(after);
				freeRecord(oldrecord);
			}

			delete[] columns;

			CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == totalUpdated);
		}

		// 验证更新后的键值都能找到，更新之前的都找不到
		{
			SubRecord *findKey;

			DrsIndex *index = m_index->getIndex(1);
			IndexDef *indexDef = m_tblDef->m_indice[1];
		//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
			SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
			record->m_columns = m_tblDef->m_indice[1]->m_columns;
			record->m_numCols = m_tblDef->m_indice[1]->m_numCols;

			SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
				indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

			uint i = HOLE + testScale - 1;
			{
				char name[50];
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				u64 userid = i + testScale + HOLE;
				u64 bankaccount = i + ((u64)i << 32);

				SubRecordBuilder sb(m_tblDef, KEY_PAD);
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;

				RowId rowId;
				bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(!found);
				freeSubRecord(findKey);

				uint newi = i + 1;
				sprintf(name, "Kenneth Tse Jr. %d \0", newi);
				userid = newi + testScale + HOLE;
				bankaccount = newi + ((u64)newi << 32);
				findKey = sb.createSubRecordByName("UserId"" ""UserName"" ""BankAccount", &userid, name, &bankaccount);
				findKey->m_rowId = INVALID_ROW_ID;
				found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
				CPPUNIT_ASSERT(found);

				freeSubRecord(findKey);
			}

			rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
			rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		}


		{	// 只更新一个索引，有一个不需要更新
			// 由于索引1和2的定义只相差了name这一个属性，只更新这一属性，其中一个索引不会涉及到更新
			SubRecord *before, *after;
			before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

			u16 *columns = new u16[m_tblDef->m_numCols];
			for (int k = 0; k < m_tblDef->m_numCols; k++)
				columns[k] = (u16)k;

			before->m_numCols = m_tblDef->m_numCols;
			before->m_columns = columns;

			Record *oldrecord;
			uint i = HOLE;
			{
				// 构造新键值
				char name[50];
				uint newi = i + testScale + HOLE + 1;
				sprintf(name, "Kenneth Tse Jr. %d \0", i + testScale + HOLE + testScale + HOLE);
				//after = createUpdateAfterRecordRedundantWhole(tableDef, rowids[i - HOLE], newi, name, (i + 1) + ((u64)(i + 1) << 32), (u32)(-1) - (i + 1));
				after = createUpdateAfterRecordRedundant2(m_tblDef, rowids[i - HOLE], name);

				// 构造旧键值
				sprintf(name, "Kenneth Tse Jr. %d \0", i);
				oldrecord = createRecord(m_tblDef, 0, i + testScale + HOLE, name, i + ((u64)i << 32), (u32)(-1) - i);
				oldrecord->m_rowId = rowids[i - HOLE];
				before->m_size = oldrecord->m_size;
				before->m_data = oldrecord->m_data;
				before->m_rowId = oldrecord->m_rowId;

				// 得到两个索引如果更新相应页面的LSN
				u64 oldlsn1 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(0));
				u64 oldlsn2 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(1));

				session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
				// 进行更新
				uint dupIndex = (uint)-1;
				bool updated = m_index->updateIndexEntries(session, before, after, false, &dupIndex);
				session->endTransaction(true);
				CPPUNIT_ASSERT(updated);
				CPPUNIT_ASSERT(dupIndex == (uint)-1);

				// 重新得到两个索引如果更新相应页面的LSN
				u64 newlsn1 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(0));
				u64 newlsn2 = getFirstLeafPageLSN(session, memoryContext, m_index->getIndex(1));

				// 验证不更新两个LSN相等，表示该索引没有被更新
				CPPUNIT_ASSERT(oldlsn1 == newlsn1);
				// 验证更新的索引LSN改变，表示有更新
				CPPUNIT_ASSERT(oldlsn2 != newlsn2);

				//checkIndex(session, memoryContext, totalKeys);

				freeSubRecord(after);
				freeRecord(oldrecord);
			}

			delete[] columns;
		}

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * 建立一个空索引，测试空索引上的操作，同时增加覆盖率
 * 首先在空索引上面做唯一查询和范围查询，验证查找不到结果
 * 在空索引上插入若干项数据，保证叶结点的个数超过两个然后删除第一个叶结点的最大项
 * 之后插入若干项比当前索引当中键值都小的记录，保证第一个叶结点会被分裂，测试插入SMO操作的某个路径
 */
void IndexOperationTestCase::testOpsOnEmptyIndex() {
	uint step = 1;

	// 建立测试索引
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testOpsOnEmptyIndex", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(0);
	IndexDef *indexDef = m_tblDef->m_indice[0];

	// 对索引1进行查找
	//SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[0]->m_columns;
	record->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	SubRecord *findKey;

	uint count;
	IndexScanHandle *scanHandle;
	RowId rowId;

	// Unique Scan，无论什么键值肯定找不到数据
	findKey = makeFindKeyWhole(HOLE + 1, m_heap, m_tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor);
	CPPUNIT_ASSERT(!found);

	// Range Scan，无论什么范围肯定找不到数据
	scanHandle = index->beginScan(session, findKey, true, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(scanHandle, record))
		count++;
	CPPUNIT_ASSERT(count == 0);
	index->endScan(scanHandle);
	freeSubRecord(findKey);

	// 根据测试模式，选择删除数据内容和插入数据内容
	u16 maxKeyNum;
	u16 nextinsertScale;
	{
		maxKeyNum = 1005;	// incorrect
		nextinsertScale = 10;
#if NTSE_PAGE_SIZE == 8192
		maxKeyNum = 1116;
		nextinsertScale = 20;
#endif
	}

	int totalKeys = 0;
	// 空索引插入批量数据
	RowId deleteRowId =  INVALID_ROW_ID;
	for (int i = HOLE; i < DATA_SCALE + HOLE; i++) {
		Record *insertedRecord = insertSpecifiedRecord(session, memoryContext, i);
		if (i == maxKeyNum)	// 这个值需要根据索引不同调整，不是固定的
			deleteRowId = insertedRecord->m_rowId;

		totalKeys++;

		freeRecord(insertedRecord);
	}

	// 删除第一个子页面的最大项，再插入若干项导致该页面分裂，测试分裂过程父页面键值需要保留的情况
	CPPUNIT_ASSERT(deleteRowId != INVALID_ROW_ID);
	int key = maxKeyNum;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *deleterecord = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
	deleterecord->m_rowId = deleteRowId;
	try {
		session->startTransaction(TXN_DELETE, m_tblDef->m_id);
		m_index->deleteIndexEntries(session, deleterecord, NULL);
		session->endTransaction(true);
		totalKeys--;
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
	}

	for (int i = HOLE - nextinsertScale; i < HOLE; i++) {
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		totalKeys++;

		freeRecord(temp);
	}


	checkIndex(session, memoryContext, totalKeys);

	freeRecord(deleterecord);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 测试索引结构正确性
 * 需要建立一个3层以上的索引，主要测试索引的某个非叶节点可能出现的键值项被删除的情况，
 * 在这个情况下，可能有新的插入或者查询会执行到该节点，同时，查询或者插入的键值
 * 会比当前层的所有值都大，正确的定位是定位到该页面的最后一项，然后继续向下定位
 * 这样会造成的就是最后一项可能出现儿子数据比父亲节点大的情况
 */
void IndexOperationTestCase::testIndexStructure() {
	uint totalKeys, items;
	uint step = 2;
	// 建立测试索引
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE * 10, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	DrsIndex *index = m_index->getIndex(1);

	insertSpecifiedRecord(session, memoryContext, 1283);	// 把前一个页面插满，避免SMO的进行
	SubRecord *findKey = makeFindKeyWhole(1310, m_heap, m_tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	RowLockHandle *rlh;
	IndexScanHandle *handle = index->beginScan(session, findKey, false, true, Exclusived, &rlh, NULL);
	uint count = 0;
	while (index->getNext(handle, NULL) && count <= 13) {
		index->deleteCurrent(handle);
		count++;
		session->unlockRow(&rlh);
	}
	index->endScan(handle);

	getRangeRecords(session, index, NULL, true);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 得到某个索引指定范围之后包含多少个键值
 */
int IndexOperationTestCase::getRangeRecords(Session *session, DrsIndex *index, SubRecord *findKey, bool includekey) {
	IndexScanHandle *indexHandle = index->beginScan(session, findKey, true, includekey, None, NULL, NULL);
	int count = 0;
	while (index->getNext(indexHandle, NULL))
		count++;
	index->endScan(indexHandle);

	return count;
}



/**
 * 测试估算范围内数据有效性
 * 构造各种范围进行测试
 * 需要增加读取真实范围的流程，对比得出每次估算误差
 */
void IndexOperationTestCase::testRecordsInRange() {
	uint step = 1;

	uint testScal = DATA_SCALE;
	{
	// 建立测试索引
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, testScal, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// 索引键值从HOLE-HOLE+DATASCALE，并且每个键值重复3次，SAME_ID重复了SAME_SCALE次
	SubRecord *minKey, *maxKey;
	minKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	maxKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	minKey->m_format = maxKey->m_format = KEY_PAD;
	minKey->m_numCols = maxKey->m_numCols = 1;
	minKey->m_rowId = maxKey->m_rowId = INVALID_ROW_ID;
	u16 cols = 0;
	minKey->m_columns = &cols;
	maxKey->m_columns = &cols;
	int realRows;
	u64 id;
	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
	SubRecord *findKey;
	// 开始测试各个范围
	u64 savePoint = memoryContext->setSavepoint();
	// 1.测试NULL-NULL
	int rows1 = (int)index->recordsInRange(session, NULL, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	int rows2 = (int)index->recordsInRange(session, NULL, false, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows3 = (int)index->recordsInRange(session, NULL, true, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows4 = (int)index->recordsInRange(session, NULL, false, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	CPPUNIT_ASSERT(rows1 == rows2 && rows1 == rows3 && rows1 == rows4);

	realRows = getRangeRecords(session, index, NULL, true);
	cout << "NULL	NULL: " << "Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
	memoryContext->resetToSavepoint(savePoint);

	// 2. 测试HOLE-NULL
	id = HOLE;
	findKey = keyBuilder.createSubRecordById("0", &id);
	minKey->m_size = findKey->m_size;
	memcpy(minKey->m_data, findKey->m_data, findKey->m_size);
	freeSubRecord(findKey);
	rows1 = (int)index->recordsInRange(session, minKey, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	realRows = getRangeRecords(session, index, NULL, true);
	cout << "HOLE	NULL: Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows - abs(realRows - rows1)) * 1.0 / realRows << endl;
	memoryContext->resetToSavepoint(savePoint);

	// 3. 测试>= HOLE / <= DATA_SCALE * 5 + HOLE
	calcSpecifiedRange(session, m_tblDef, index, HOLE, HOLE + testScal, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 4. 测试> HOLE / < DATA_SCALE * 5 + HOLE
	calcSpecifiedRange(session, m_tblDef, index, HOLE, HOLE + testScal, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 5. 测试 >= HOLE / < SAME_ID
	calcSpecifiedRange(session, m_tblDef, index, HOLE, SAME_ID, true, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 6. 测试 > HOLE / <= SAME_ID
	calcSpecifiedRange(session, m_tblDef, index, HOLE, SAME_ID, false, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 7. 测试 >= SAME_ID / <= SAME_ID
	calcSpecifiedRange(session, m_tblDef, index, SAME_ID, SAME_ID, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 8. 测试 >= HOLE + 5 / <= HOLE + 5
	calcSpecifiedRange(session, m_tblDef, index, HOLE + 5, HOLE + 5, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 9. 测试 >= SAME_ID / < SAME_ID + 1
	calcSpecifiedRange(session, m_tblDef, index, SAME_ID, SAME_ID + 1, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 10. 测试 > SAME_ID + 1 / <= SAME_ID + 2
	calcSpecifiedRange(session, m_tblDef, index, SAME_ID + 1, SAME_ID + 2, false, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 11. 测试 > 2000 / < 8000
	calcSpecifiedRange(session, m_tblDef, index, 2000, 8000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 12. 测试 > 3000 / < 12000
	calcSpecifiedRange(session, m_tblDef, index, 3000, 12000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 13. 测试 < 0 / < 500
	calcSpecifiedRange(session, m_tblDef, index, 0, 500, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 14. 测试 > 100000 / < 110000
	calcSpecifiedRange(session, m_tblDef, index, 100000, 110000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 15. 测试 > 0 / < 2000
	calcSpecifiedRange(session, m_tblDef, index, 0, 2000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 16. 测试 > 8000 / < 100000
	calcSpecifiedRange(session, m_tblDef, index, 8000, 100000, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	index = m_index->getIndex(0);
	indexDef = m_tblDef->m_indice[0];
	// 17. 测试可以进行C比较的索引的估计
	{
		checkIndex(session, memoryContext, totalKeys);

		SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
		SubRecord *findKey;
		u64 key1 = 8000;
		u64 key2 = 100000000;
		u64 zero = 0;
		minKey->m_numCols = maxKey->m_numCols = 2;
		u16 cols0 = 0, cols2 = 2;
		minKey->m_columns[0] = cols0;
		maxKey->m_columns[0] = cols0;
		minKey->m_columns[1] = cols2;
		maxKey->m_columns[1] = cols2;

		findKey = keyBuilder.createSubRecordById("0 2", &key1, &zero);
		minKey->m_size = findKey->m_size;
		memcpy(minKey->m_data, findKey->m_data, findKey->m_size);
		freeSubRecord(findKey);
		findKey = keyBuilder.createSubRecordById("0 2", &key2, &zero);
		maxKey->m_size = findKey->m_size;
		memcpy(maxKey->m_data, findKey->m_data, findKey->m_size);
		freeSubRecord(findKey);
		int rows1 = (int)index->recordsInRange(session, minKey, true, maxKey, true);
		int realRows = getRangeRecords(session, index, minKey, true) - getRangeRecords(session, index, maxKey, false);
		cout << key1 << "	" << key2 << ": Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
		m_tblDef->m_columns[2]->m_nullable = true;
	}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	}

	/***************************************************************************************/

	{
	// 再测试小索引的范围估值，一个页面，包括等值键值
	// 建立测试索引
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	Record *insertRec;
	insertRec = insertSpecifiedRecord(session, memoryContext, 0);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 0);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 1);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 2);
	freeRecord(insertRec);
	insertRec = insertSpecifiedRecord(session, memoryContext, 5);
	freeRecord(insertRec);

	SubRecord *minKey, *maxKey;
	minKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	maxKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	minKey->m_format = maxKey->m_format = KEY_PAD;
	minKey->m_numCols = maxKey->m_numCols = 1;
	minKey->m_rowId = maxKey->m_rowId = INVALID_ROW_ID;
	u16 cols = 0;
	minKey->m_columns = &cols;
	maxKey->m_columns = &cols;
	int realRows;
	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
	// 开始测试各个范围
	u64 savePoint = memoryContext->setSavepoint();
	// 1.测试NULL-NULL
	int rows1 = (int)index->recordsInRange(session, NULL, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	int rows2 = (int)index->recordsInRange(session, NULL, false, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows3 = (int)index->recordsInRange(session, NULL, true, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows4 = (int)index->recordsInRange(session, NULL, false, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	CPPUNIT_ASSERT(rows1 == rows2 && rows1 == rows3 && rows1 == rows4);

	realRows = getRangeRecords(session, index, NULL, true);
	cout << "NULL	NULL: " << "Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
	memoryContext->resetToSavepoint(savePoint);

	// 3. 测试>=0 / <=6
	calcSpecifiedRange(session, m_tblDef, index, 0, 6, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 4. 测试>0 / <6
	calcSpecifiedRange(session, m_tblDef, index, 0, 6, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 5. 测试 >= 1 / < 2
	calcSpecifiedRange(session, m_tblDef, index, 1, 2, true, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 6. 测试 >= 1 / <= 1
	calcSpecifiedRange(session, m_tblDef, index, 1, 1, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 7. 测试 >= 5 / <= 5
	calcSpecifiedRange(session, m_tblDef, index, 5, 5, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 8. 测试 >= 3 / <= 4
	calcSpecifiedRange(session, m_tblDef, index, 3, 4, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 9. 测试 > 3 / < 4
	calcSpecifiedRange(session, m_tblDef, index, 3, 4, false, false, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 10. 测试 >= 3 / <= 3
	calcSpecifiedRange(session, m_tblDef, index, 3, 3, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 11. 测试 > 4 / <= 5
	calcSpecifiedRange(session, m_tblDef, index, 4, 5, false, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 12. 测试 >= 3 / <= 5
	calcSpecifiedRange(session, m_tblDef, index, 3, 5, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	// 13. 测试 >= 2 / <= 2
	calcSpecifiedRange(session, m_tblDef, index, 2, 2, true, true, minKey, maxKey);
	memoryContext->resetToSavepoint(savePoint);

	m_index->close(m_session, true);
	delete m_index;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
	}

	/*******************************************************************************/
	// 再测试空索引的范围估值
	// 建立测试索引
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	SubRecord *minKey, *maxKey;
	minKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	maxKey = IndexKey::allocSubRecordRED(memoryContext, indexDef->m_maxKeySize);
	minKey->m_format = maxKey->m_format = KEY_PAD;
	minKey->m_numCols = maxKey->m_numCols = 1;
	u16 cols = 0;
	minKey->m_columns = &cols;
	maxKey->m_columns = &cols;
	int realRows;
	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD);
	// 开始测试各个范围
	u64 savePoint = memoryContext->setSavepoint();
	// 1.测试NULL-NULL
	int rows1 = (int)index->recordsInRange(session, NULL, true, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	int rows2 = (int)index->recordsInRange(session, NULL, false, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows3 = (int)index->recordsInRange(session, NULL, true, NULL, true);
	memoryContext->resetToSavepoint(savePoint);
	int rows4 = (int)index->recordsInRange(session, NULL, false, NULL, false);
	memoryContext->resetToSavepoint(savePoint);
	CPPUNIT_ASSERT(rows1 == rows2 && rows1 == rows3 && rows1 == rows4);

	realRows = getRangeRecords(session, index, NULL, true);
	cout << "NULL	NULL: " << "Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
	memoryContext->resetToSavepoint(savePoint);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 计算指定范围的键值数
 * @param session		会话句柄
 * @param tableDef		表定义
 * @param index			要计算的索引对象
 * @param key1			范围下限大小
 * @param key2			范围上限大小
 * @param includekey1	是否是>=查询
 * @param includekey2	是否是<=查询
 * @param minKey		最小键值
 * @param maxKey		最大键值
 */
void IndexOperationTestCase::calcSpecifiedRange(Session *session, TableDef *tableDef, DrsIndex *index, u64 key1, u64 key2, bool includekey1, bool includekey2, SubRecord *minKey, SubRecord *maxKey) {
	SubRecordBuilder keyBuilder(tableDef, KEY_PAD);
	SubRecord *findKey;
	findKey = keyBuilder.createSubRecordById("0", &key1);
	minKey->m_size = findKey->m_size;
	memcpy(minKey->m_data, findKey->m_data, findKey->m_size);
	freeSubRecord(findKey);
	findKey = keyBuilder.createSubRecordById("0", &key2);
	maxKey->m_size = findKey->m_size;
	memcpy(maxKey->m_data, findKey->m_data, findKey->m_size);
	freeSubRecord(findKey);
	int rows1 = (int)index->recordsInRange(session, minKey, includekey1, maxKey, includekey2);
	int realRows = getRangeRecords(session, index, minKey, includekey1) - getRangeRecords(session, index, maxKey, !includekey2);
	cout << key1 << "	" << key2 << ": Estimate: " << rows1 << "	Real: " << realRows << "	Diff: " << (realRows == 0 ? 1.0 : (abs(realRows - rows1)) * 1.0 / realRows) << endl;
}


/**
 * 测试索引状态获取
 */
void IndexOperationTestCase::testGetStatus() {
	uint step = 1;
	uint testScale = DATA_SCALE / 10;

	// 建立测试索引
	m_tblDef = createSmallTableDefWithSame();
	m_heap = createSmallHeapWithSame(m_tblDef);
	uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, 20, step);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	const struct IndexStatus *status = &(index->getStatus());
	// 可以确认索引大小最多为128个页面
	CPPUNIT_ASSERT(status->m_dataLength <= 128 * Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(status->m_dataLength >= status->m_freeLength);

	// 对索引执行一系列操作，然后获取统计信息
	// 首先执行范围扫描，正向反向各一次
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	CPPUNIT_ASSERT(status->m_backwardScans == (isEssentialOnly() ? 0 : 1));
	CPPUNIT_ASSERT(status->m_rowsBScanned == (isEssentialOnly() ? 0 : totalKeys));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN] == (isEssentialOnly() ? 0 : 2));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN_ITEM] == (isEssentialOnly() ? 0 : totalKeys * 2));

	// 测试插入
	uint ops = 0;
	for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);
		totalKeys++;
		ops++;
		freeRecord(temp);
	}

	// 验证空闲页面减少
	status = &(index->getStatus());
	CPPUNIT_ASSERT(status->m_dataLength - status->m_freeLength > Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == ops);

	// 范围删除
	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	RowLockHandle *rlh;
	IndexScanHandle *indexHandle = index->beginScan(session, NULL, true, true, Exclusived, &rlh, NULL);
	int count = 0;
	while (index->getNext(indexHandle, NULL)) {
		// 去对应Heap取记录
		RowId rowId = indexHandle->getRowId();
		record->m_format = REC_FIXLEN;
		m_heap->getRecord(m_session, rowId, record);
		record->m_format = REC_REDUNDANT;
		session->startTransaction(TXN_DELETE, m_tblDef->m_id);
		m_index->deleteIndexEntries(session, record, indexHandle);
		session->endTransaction(true);
		session->unlockRow(&rlh);
		count++;
	}
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == count);

	// 再次范围扫描结果为0
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, 0);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, 0);
	CPPUNIT_ASSERT(status->m_backwardScans == (isEssentialOnly() ? 0 : 2));
	CPPUNIT_ASSERT(status->m_rowsBScanned == (isEssentialOnly() ? 0 : count - ops));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN] == (isEssentialOnly() ? 1 : 5));
	CPPUNIT_ASSERT(status->m_dboStats->m_statArr[DBOBJ_SCAN_ITEM] == (isEssentialOnly() ? count : 3 * (count - ops) + ops));

	// 当前只有一个根页面在使用中
	status = &(index->getStatus());
	CPPUNIT_ASSERT(status->m_dataLength == status->m_freeLength + Limits::PAGE_SIZE);

	// 测试增加死锁统计接口
	((DrsBPTreeIndex*)index)->statisticDL();

	// 测试获取DB统计信息接口
	index->getDBObjStats();

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 测试deleteIndexEntry中LSN判断不相等时处理过程
 * 需要创建两个线程完成这项工程，一个删除线程，一个干扰线程
 * 删除线程首先启动范围删除，但是在真正执行删除之前，等在某个同步点
 * 此时启动干扰线程，修改删除线程要删除数据所在页面的内容，保证该页面LSN改变，同时结束干扰线程
 * 此时唤醒删除线程，继续执行删除，会导致需要重定位，也就覆盖了重定位代码段
 */
void IndexOperationTestCase::testMTDeleteRelocate() {
	{	// 测试范围删除扫描后删除时可能出现需要重定位的情况
		// 建立测试索引
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, 100, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		TroubleMaker trouble("I'm a trouble maker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 3, rowids[3]);
		SimpleMan simpleman("I'm a simple man", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, INVALID_ROW_ID);

		simpleman.enableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);
		trouble.enableSyncPoint(SP_IDX_FINISH_A_DELETE);

		// 启动正常范围删除线程，在真正执行删除之前等待
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		// 启动干扰线程，提前改动范围删除线程要操作页面
		trouble.enableSyncPoint(SP_IDX_FINISH_A_DELETE);
		trouble.start();
		trouble.joinSyncPoint(SP_IDX_FINISH_A_DELETE);
		// 让正常删除线程继续，此时该线程必须重定位删除记录
		trouble.disableSyncPoint(SP_IDX_FINISH_A_DELETE);
		trouble.notifySyncPoint(SP_IDX_FINISH_A_DELETE);

		simpleman.disableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);
		simpleman.notifySyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		trouble.join();
		simpleman.join();

		// 验证结果
		{	// 删除的键值不存在
			DrsIndex *index = m_index->getIndex(1);
			SubRecord *findKey = makeFindKeyWhole(HOLE, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			RowId rowId;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);

			findKey = makeFindKeyWhole(HOLE + 1, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);

			findKey = makeFindKeyWhole(HOLE + 3, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(!found);
			freeSubRecord(findKey);

			// 没有被删除的键值应该存在
			findKey = makeFindKeyWhole(HOLE + 2, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			found = index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL);
			CPPUNIT_ASSERT(found);
			freeSubRecord(findKey);

			// 索引结构完整
			checkIndex(session, memoryContext, totalKeys);
		}

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * 该测试用来测试deleteSMO函数中唯一的一次可能出现父结点改变需要重定位的情况
 * 该工程需要两个线程，一个删除线程，一个干扰线程，这里选择干扰线程做插入操作
 * 删除线程批量删除导致会有删除SMO被触发，在SMO执行之前，等在某个同步点
 * 启动干扰线程做范围插入，导致删除线程触发SMO的页面的父页面会被分裂，结束干扰线程
 * 启动删除线程继续SMO操作，这个时候由于父结点被改变，需要重新定位父结点，覆盖相关代码段
 * 注意totalScale是索引起始规模大小，至少要保证有2个页面，才存在删除合并的可能，同时插入才有正确的位置进行
 *		preDeleteNum控制将第一个叶页面删除到只剩下一项，到这里为止都不会触发SMO
 *		insertNo	应该要保证插入在第二个叶页面或者后续的叶页面
 *		insertNum	保证插入的量会触发插入操作的SMO
 */
void IndexOperationTestCase::testMTDeleteSMORelocate() {
	{
		u16 totalScale = 100;
		u16 preDeleteNum = 11;
		u16 insertNo = 30;
		u16 insertNum = 15;

#if NTSE_PAGE_SIZE == 8192
		totalScale = 300;
		preDeleteNum = 115;
		insertNo = 301;
		insertNum = 120;
#endif

		// 建立测试索引
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		// 先将索引第一个页面删除到只剩一项，再继续删除会触发SMO操作
		for (uint i = HOLE; i < HOLE + preDeleteNum; i++) {
			uint key = i;
			char name[50];
			sprintf(name, "Kenneth Tse Jr. %d \0", key);
			Record *record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
			record->m_rowId = rowids[key - HOLE];
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			EXCPT_OPER(m_index->deleteIndexEntries(session, record, NULL));
			session->endTransaction(true);
			totalKeys--;

			freeRecord(record);
		}

		// 启动线程，simpleman线程进行范围删除，再使用一个插入者线程在simpleman触发SMO之前，首先造成父结点改变
		SimpleMan simpleman("I'm a simple man", m_db, conn, m_heap, m_tblDef, m_index, HOLE + preDeleteNum, true, INVALID_ROW_ID);
		Bee bee("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + insertNo, insertNum, INVALID_ROW_ID);

		simpleman.enableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		// 启动正常范围删除线程，在真正执行删除之前等待
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		// 蜜蜂线程开始插入一批数据造成索引树分裂
		bee.start();
		bee.join();
		totalKeys += insertNum;

		simpleman.disableSyncPoint(SP_IDX_CHECK_BEFORE_DELETE);
		simpleman.notifySyncPoint(SP_IDX_CHECK_BEFORE_DELETE);

		simpleman.join();
		totalKeys -= 2;

		// 验证结果正确性
		checkIndex(session, memoryContext, totalKeys);
		DrsIndex *index = m_index->getIndex(1);
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * 测试读写操作之间的冲突，包括以下几个：
 * case1. 范围读getNextRecord跨页面回头判断LSN不一致
 *			该工程需要两个志愿者，还需要一个删除志愿者帮忙删除第一个页面的最大项，便于后续插入
 *			一个志愿者负责做范围查询，在做到要跨页面的时候等候在同步点上
 *			此时启动另一个志愿者线程进行插入，该插入会插入在跨页面的第一个页面的最后一项，然后结束线程
 *			继续范围查询线程，会发现第一个页面被修改，需要重新定位，并且此时可以定位到前一个线程插入的项，最后验证读取数据正确
 * 2. 扫描遇到SMO操作页面等待SMO_Bit复位
 *			该工程需要两个志愿者共同完成
 *			首先一个插入志愿者积极的插入大量数据，导致索引数进行分裂，但是在分裂结束还没有恢复SMO位之前，该线程等在同步点上
 *			此时启动一个读数据线程，读取插入范围内的数据，会发现索引正在执行SMO操作，会等在某个SMO页面
 *			过一段时间之后继续执行插入线程使插入操作执行结束，然后继续读数据线程操作
 *			最后能够验证读数据操作能够正确读到数据，同时等待SMO操作代码也被覆盖测试
 * 3. 同2，区别在于初始化索引的大小，造成的不同是SMO操作是否会导致索引根结点进行分裂
 * 4. 扫描与修改操作导致latch冲突
 *			该工程需要三个线程
 *			首先构造一个大小和规模如下面代码中所描述的索引
 *			首先启动范围扫描线程，读取到跨页面释放页面锁的时刻，等候在一个同步点上
 *			这时候启动两个插入线程，分别在扫描线程释放的页面和要加锁的页面分别插入数据，结束插入
 *			继续运行扫描线程，会经历两次需要重新判断LSN的情况
 * 5. 测试范围扫描过程中遇到某个页面LSN改变
 *			首先建立一个小索引，只需要一个页面
 *			开始一个范围扫描，从的一项开始扫描，扫描一项之后等候在一个同步电
 *			启动插入线程对该页面执行插入，改变页面LSN，结束该线程
 *			继续范围扫描，此时需要判断重定位
 *			再启动一个简洁主义者线程，删光索引树，结束
 *			继续范围扫描，此时判断索引树为空
 */
void IndexOperationTestCase::testMTRWConflict() {
	vs.buf = true;
	{	// 范围读getNextRecord跨页面判断LSN不一致
		// 建立测试索引
		u16 totalScale;
		u16 deleteNo;
		u16 insertNo;

		{
			totalScale = 100;
			deleteNo = 24;
			insertNo = 23;
			#if NTSE_PAGE_SIZE == 8192
				totalScale = 500;
				deleteNo = 232;
				insertNo = 231;
			#endif
		}

		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		// 先启动线程删除索引第一个子页面的最后一项
		SimpleMan simpleman("I'm a simpleman", m_db, conn, m_heap, m_tblDef, m_index, HOLE + deleteNo, false, rowids[deleteNo]);
		simpleman.start();
		simpleman.join();
		totalKeys--;

		Bee bee("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + insertNo, 1, INVALID_ROW_ID);	// 这里为了保证插入在第一个页面的末尾
		ReadingLover readinglover("I'm a reading fun", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);

		// 让读线程启动并且等待在跨页面状态
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE1);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE1);

		bee.start();
		bee.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE1);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE1);
		readinglover.join();

		// 检验结果
		uint gotCount = readinglover.getReadNum();
		CPPUNIT_ASSERT(gotCount == totalKeys);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	{	// 测试SMO等待代码
		// 建立小索引，由线程批量插入数据导致索引分裂，同时触发读线程读数据，造成等待SMO_Bit被释放的情况

		u16 totalScale;

		{
			totalScale = 20;
			#if NTSE_PAGE_SIZE == 8192
				totalScale = 300;
			#endif
		}


		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		Bee bee("I'm a laborer", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 30, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 2, false, false, NULL, INVALID_ROW_ID);

		// 开始插入导致SMO，等待在要清除SMO_Bit之前的时刻
		bee.enableSyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);
		bee.start();
		bee.joinSyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);

		// 读线程开始查找，必定会等在SMO_Bit的判断上
		readinglover.enableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		// SMO_Bit等待开始，允许插入线程继续执行完毕
		bee.disableSyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);
		bee.notifySyncPoint(SP_IDX_BEFORE_CLEAR_SMO_BIT);

		// 此时需要重定位读操作，确保能读到结果
		readinglover.disableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		readinglover.join();
		bee.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	if (!isEssentialOnly())
	{	// 测试SMO等待代码
		// 建立大索引，由线程批量插入数据导致索引分裂，同时触发读线程读数据，造成等待SMO_Bit被释放的情况
		// 该用例和前一个用例的不同是前面的小索引SMO会导致根页面分裂，这里大索引保证根页面不会分裂
		// 这里模拟修改操作的SMO锁，采用直接对扫描线程使用的叶页面设置SMO位的方法，降低索引数据量

		uint totalScale;

		{
			totalScale = 5000;
#if NTSE_PAGE_SIZE == 8192
			totalScale = 200000;
#endif
		}

		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTRWConflict", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 2, false, false, NULL, INVALID_ROW_ID);

		// 采用一个范围扫描，获取索引子页面的第一个页面，后面需要对它加SMO位
		DrsIndex *index = m_index->getIndex(1);
		IndexScanHandle *scanHandle = index->beginScan(session, NULL, true, true, None, NULL, NULL);
		PageId pageId ;
		while (index->getNext(scanHandle, NULL)) {
			DrsIndexRangeScanHandle *rangeHandle = (DrsIndexRangeScanHandle*)scanHandle;
			pageId = rangeHandle->getScanInfo()->m_pageId;	// 该页面此时被pin住
			break;
		}
		index->endScan(scanHandle);

		// 加锁设置SMO位
		PageHandle *handle = GET_PAGE(session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, pageId, Exclusived, m_index->getDBObjStats(), NULL);
		IndexPage *page = (IndexPage*)handle->getPage();
		page->setSMOBit(session, pageId, ((DrsBPTreeIndice*)m_index)->getLogger());
		session->markDirty(handle);
		session->unlockPage(&handle);

		// 读线程开始查找，必定会等在SMO_Bit的判断上
		readinglover.enableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		// SMO_Bit等待开始，清除SMO位
		LOCK_PAGE_HANDLE(session, handle, Exclusived);
		page = (IndexPage*)handle->getPage();
		page->clearSMOBit(session, pageId, ((DrsBPTreeIndice*)m_index)->getLogger());
		session->markDirty(handle);
		session->releasePage(&handle);

		// 此时需要重定位读操作，确保能读到结果
		readinglover.disableSyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_SMO_BIT);

		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	u16 totalScale;

	{
		totalScale = 30;
#if NTSE_PAGE_SIZE == 8192
		totalScale = 300;
#endif
	}

	{	// 测试latchHoldingLatch加不上锁LSN判断不等的情况
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		// 此时的索引应该是两层三个页面，如下所示：
		//
		//						|---------|Page1 pageId132
		//						|---------|
		//						/		\
		//	Page2 pageId130|-------|	|--------|Page3 pageId131
		//				   |-------|	|--------|
		// 此时需要反向范围扫描从page3跨页面到page2，在跨页面之前page2正在被修改线程操作，
		// 扫描线程要放锁再加锁，再重新加原页面锁的时候，原页面又需要被另一个修改线程修改
		// 测试LSN改变的情况

		Bee bee1("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 1, INVALID_ROW_ID);
		Bee bee2("I'm also a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + totalScale, 1, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);

		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);	// 略过定位叶结点出现的一次等待
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);

		bee1.enableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.start();
		bee1.joinSyncPoint(SP_IDX_WAIT_FOR_INSERT);

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE4);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);

		bee1.disableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.notifySyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE4);

		bee2.start();
		bee2.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE4);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE4);
		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == totalKeys - 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	{	// 测试latchHoldingLatch加不上锁LSN判断相等的情况
		uint totalKeys;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, totalScale, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		// 此时的索引应该是两层三个页面，如下所示：
		//
		//						|---------|Page1 pageId132
		//						|---------|
		//						/		\
		//	Page2 pageId130|-------|	|--------|Page3 pageId131
		//				   |-------|	|--------|
		//	其中pageId3>pageId2>pageId1
		// 此时需要反向范围扫描从page3跨页面到page2，在跨页面之前page2正在被修改线程操作，
		// 扫描线程要放锁再加锁，跨页面之前持有的原页面内容没有改动，测试LSN不变的情况

		Bee bee1("I'm a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 1, INVALID_ROW_ID);
		Bee bee2("I'm also a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + totalScale, 1, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);

		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);	// 略过定位叶结点出现的一次等待
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);

		bee1.enableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.start();
		bee1.joinSyncPoint(SP_IDX_WAIT_FOR_INSERT);

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.enableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE2);
		readinglover.joinSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);

		bee1.disableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.notifySyncPoint(SP_IDX_WAIT_FOR_INSERT);
		bee1.join();
		totalKeys++;

		readinglover.disableSyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.notifySyncPoint(SP_IDX_WANT_TO_GET_PAGE3);
		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == totalKeys - 1);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}

	// case 5
	// 测试范围扫描取下一项的重定位
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		buildHeap(m_heap, m_tblDef, 10, 2);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);

		Bee bee2("I'm also a hardworker", m_db, conn, m_heap, m_tblDef, m_index, HOLE + 1, 0, INVALID_ROW_ID);
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, false, NULL, INVALID_ROW_ID);
		SimpleMan simpleman("I'm a simpleman", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, rowids[0], true);

		// 开始范围扫描，取到一项之后，
		readinglover.enableSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);

		// 执行一次插入
		bee2.start();
		bee2.join();

		// 此时取项就要判断，然后继续等在下一项上
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.joinSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);

		// 删光索引
		simpleman.start();
		simpleman.join();

		// 再判断一次，结束操作
		readinglover.disableSyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.notifySyncPoint(SP_IDX_WAIT_FOR_GET_NEXT);
		readinglover.join();

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->freeConnection(conn);
	}
}



/**
 * 测试持有latch资源加锁加不上需要放latch资源的情况
 * @case1:	该工程需要三个志愿者完成
 *			首先启动一个志愿者线程读取数据确认指定数据存在，在程序外部将该数据行加锁
 *			启动真正的读数据线程，该线程读取相同数据，同时也需要加锁，此时会加不上锁，释放页面Latch，等在某个同步点
 *			启动一个删除线程，将前面要查询的数据删除，执行结束放锁
 *			继续前面的查询线程，此时再加锁成功，但是发现页面被改变，重新查找，应该找不到数据
 */
void IndexOperationTestCase::testMTLockHoldingLatch() {
	uint totalKeys;
	uint step = 2;
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	totalKeys = buildHeap(m_heap, m_tblDef, 30, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch", conn);
	Connection *conn1 = m_db->getConnection(false);
	Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch1", conn1);

	{	// 正向扫描
		RowLockHandle *rlh = NULL;
		ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, false, false, &rlh, INVALID_ROW_ID);
		ReadingLover readinglover1("I also like reading", m_db, conn, m_heap, m_tblDef, m_index, HOLE, false, false, NULL, INVALID_ROW_ID);
		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE, false, rowids[0]);

		// 先读取数据确认存在
		readinglover1.start();
		readinglover1.join();
		CPPUNIT_ASSERT(readinglover1.getReadNum() == 1);

		// 保证读线程加不上锁
		RowLockHandle *lockhandle = LOCK_ROW(session1, m_tblDef->m_id, rowids[0], Exclusived);
		readinglover.enableSyncPoint(SP_IDX_WAIT_TO_LOCK);
		readinglover.start();
		readinglover.joinSyncPoint(SP_IDX_WAIT_TO_LOCK);

		// 外部放锁，同时删除要查找的数据
		session1->unlockRow(&lockhandle);
		simpleman.start();
		simpleman.join();

		readinglover.disableSyncPoint(SP_IDX_WAIT_TO_LOCK);
		readinglover.notifySyncPoint(SP_IDX_WAIT_TO_LOCK);
		readinglover.join();

		CPPUNIT_ASSERT(readinglover.getReadNum() == 0);
		CPPUNIT_ASSERT(rlh == NULL);
	}


	//{	// 反向扫描
	//	RowLockHandle *rlh = NULL, *rlh1 = NULL;
	//	ReadingLover readinglover("I like reading", m_db, conn, m_heap, m_index, HOLE + step * 2, true, true, &rlh, INVALID_ROW_ID, 1);
	//	ReadingLover readinglover1("I also like reading", m_db, conn, m_heap, m_index, HOLE + step * 2, false, true, &rlh1, INVALID_ROW_ID);
	//	SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_index, HOLE + step * 2, false, rowids[step * 2]);

	//	// 先读取数据确认存在
	//	readinglover1.start();
	//	readinglover1.join();
	//	CPPUNIT_ASSERT(readinglover1.getReadNum() == 1);

	//	// 保证读线程加不上锁
	//	RowLockHandle *lockhandle = LOCK_ROW(session1, m_tblDef->m_id, rowids[step * 2], Exclusived);
	//	readinglover.enableSyncPoint(SP_IDX_WAIT_TO_LOCK);
	//	readinglover.start();
	//	readinglover.joinSyncPoint(SP_IDX_WAIT_TO_LOCK);

	//	// 外部放锁，同时删除要查找的数据
	//	session1->unlockRow(&lockhandle);
	//	simpleman.start();
	//	simpleman.join();

	//	readinglover.disableSyncPoint(SP_IDX_WAIT_TO_LOCK);
	//	readinglover.notifySyncPoint(SP_IDX_WAIT_TO_LOCK);
	//	readinglover.join();

	//	CPPUNIT_ASSERT(readinglover.getReadNum() == 1);
	//	CPPUNIT_ASSERT(rlh == NULL);
	//}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->getSessionManager()->freeSession(session1);
	m_db->freeConnection(conn);
	m_db->freeConnection(conn1);
}


/**
 * 测试加索引页面锁的多线程测试
 * @case1 测试插入加页面锁的过程中，加页面锁失败，同时页面被另一个线程修改，但是线程一操作的项还在该页面当中
 * @case2 测试插入加页面锁的过程中，加页面锁失败，同时页面被另一个线程修改，产生了SMO操作，必须重新搜索再加锁的场景
 * @case3 类似case2，只不过第一个线程执行删除操作
 */
void IndexOperationTestCase::testMTLockIdxObj() {
	uint totalKeys;
	uint step = 3;
	uint makeSMOSize = 30;
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	// 设置50%的分裂方式，保证该用例测试有效
	m_tblDef->m_indice[0]->m_splitFactor = 50;
	m_tblDef->m_indice[1]->m_splitFactor = 50;
	totalKeys = buildHeap(m_heap, m_tblDef, 690, step);
	createIndex(m_path, m_heap, m_tblDef);

	DrsIndex *index = m_index->getIndex(1);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch", conn);
	Connection *conn1 = m_db->getConnection(false);
	Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testMTLockHoldingLatch1", conn1);

	uint firstPageLastId = 345;

	/************************ case 1 *****************************/
	{
		// 首先该线程先尝试插入231项，然后等在页面锁上
		Bee bee1("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId - 1, 0, INVALID_ROW_ID);
		u64 firstPageId = 257;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | firstPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		bee1.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee1.start();
		bee1.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// 执行一个插入，导致257页面发生了改变，需要重定位，但是没有SMO发生
		Bee bee2("I'm also a hardworker, surely", m_db, conn1, m_heap, m_tblDef, m_index, HOLE + 1, 0, INVALID_ROW_ID);
		bee2.start();
		bee2.join();

		// 重新唤醒插入线程，插入应该成功
		bee1.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee1.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee1.join();

		totalKeys += 2;
	}

	/************************ case 2 *****************************/
	{
		// 启动另外一个线程在257页面进行插入操作，然后依旧等锁
		u64 firstPageId = 257;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | firstPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		Bee bee3("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId - 2, 0, INVALID_ROW_ID);
		bee3.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee3.start();
		bee3.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// 执行一个批量插入，导致257页面SMO发生
		for (int i = HOLE + 2; i < makeSMOSize * step + HOLE; i += step) {
			// 首先插入堆
			Record *temp = insertSpecifiedRecord(session1, session1->getMemoryContext(), i);
			totalKeys++;
			freeRecord(temp);
		}

		// 继续唤醒插入线程，插入成功，但是需要重启动
		bee3.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee3.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		bee3.join();

		totalKeys += 1;
	}

	/************************ case 3 *****************************/
	{
		// 启动另外一个线程在258页面进行删除操作，然后依旧等锁，类似case2
		u64 secondPageId = 258;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | secondPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId * 2 - step, false, rowids[firstPageLastId * 2 - step]);
		simpleman.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// 执行一个批量插入，导致258页面SMO发生
		for (int i = HOLE + firstPageLastId + step + 1; i < makeSMOSize * step + HOLE + firstPageLastId + step + 1; i += step) {
			// 首先插入堆
			Record *temp = insertSpecifiedRecord(session1, session1->getMemoryContext(), i);
			totalKeys++;
			freeRecord(temp);
		}

		// 继续唤醒插入线程，插入成功，但是需要重启动
		simpleman.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.join();

		totalKeys -= 1;
	}

	/************************ case 4 *****************************/
	{
		// 启动另外一个线程在258页面进行删除操作，然后依旧等锁，
		// 新的线程只是删除页面的某一项，不导致SMO产生
		u64 secondPageId = 258;
		u64 lockObjId = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | secondPageId;
		CPPUNIT_ASSERT(session->lockIdxObject(lockObjId));
		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE + firstPageLastId + step , true, rowids[firstPageLastId + step]);
		simpleman.enableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		CPPUNIT_ASSERT(session->unlockIdxObject(lockObjId));

		// 执行一个插入，导致258页面LSN改变
		for (int i = HOLE + firstPageLastId + step + 2; i < HOLE + firstPageLastId + step * 2 + 2; i += step) {
			// 首先插入堆
			Record *temp = insertSpecifiedRecord(session1, session1->getMemoryContext(), i);
			totalKeys++;
			freeRecord(temp);
		}

		// 继续唤醒插入线程，插入成功，但是需要重启动
		simpleman.disableSyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.notifySyncPoint(SP_IDX_WAIT_FOR_PAGE_LOCK);
		simpleman.join();

		totalKeys -= 1;
	}

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->getSessionManager()->freeSession(session1);
	m_db->freeConnection(conn);
	m_db->freeConnection(conn1);
}


/**
 * 测试SMO操作过程中需要重新搜索父接点的情况
 * @case1 首先建立一个大堆索引，两层，保证索引的根页面基本快满，设置分裂系数为50%
 *			在索引的倒数第二个叶页面插入一片数据，直到SMO产生，等待在搜索父节点的地方
 *			启动另一个线程，在索引的尾部执行大批量插入，导致父节点也分裂，结束线程
 *			继续第一个插入线程，就需要重新搜索父节点
 */
void IndexOperationTestCase::testMTSearchParentInSMO() {
	uint step = 1;
	uint totalKeys;
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	totalKeys = buildBigHeap(m_heap, m_tblDef, false, 700, step);
	m_tblDef->m_indice[0]->m_splitFactor = 50;
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueScan", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(0);

	//rangeScanBackwardCheck(session, tableDef, index, NULL, NULL, totalKeys);
	uint insertPos = 1500;
	// 某个线程开始插入若干项，导致分裂，等待在寻找父节点位置
	Bee bee("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, insertPos, 100, INVALID_ROW_ID);
	bee.enableSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.start();
	bee.joinSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.notifySyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.joinSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.notifySyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.joinSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);

	// 这里开始执行大批量的尾部插入，导致根结点分裂
	for (uint i = 1700; i < 1820; i++) {
		char name[LONG_KEY_LENGTH];
		RowId rid;
		sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));

		freeRecord(insertrecord);

		insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		insertrecord->m_rowId = rid;

		bool successful;
		uint dupIndex;
		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(successful);
		totalKeys++;

		freeRecord(insertrecord);
	}

	// 继续前一个线程，此时要重定位父节点
	bee.disableSyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.notifySyncPoint(SP_IDX_RESEARCH_PARENT_IN_SMO);
	bee.join();

	totalKeys += 100;
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);
}

Record * IndexOperationTestCase::insertHeapSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i) {
	char name[50];
	RowId rid;
	sprintf(name, "Kenneth Tse Jr. %d \0", i);
	Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));

	freeRecord(insertrecord);

	insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	insertrecord->m_rowId = rid;

	return insertrecord;
}

void IndexOperationTestCase::insertIndexSpecifiedRecord(Session *session, MemoryContext *memoryContext, Record *insertrecord) {
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	bool successful;
	uint dupIndex;
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	CPPUNIT_ASSERT(successful);
	session->endTransaction(true);
}

/**
 * 创建指定值的表记录，同时在堆和索引中都插入
 */
Record* IndexOperationTestCase::insertSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i) {
	Record *insertRecord = insertHeapSpecifiedRecord(session, memoryContext, i);
	insertIndexSpecifiedRecord(session, memoryContext, insertRecord);
	return insertRecord;
}


/**
 * 对建立的索引进行索引结构验证，保证结构完整，而且同一个页面里面的数据大小排列有序
 */
void IndexOperationTestCase::checkIndex(Session *session, MemoryContext *memoryContext, uint keys) {
	SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *pkey0 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_PAD);
	DrsIndex *index = m_index->getIndex(0);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	if (m_tblDef->m_numIndice == 1)
		return;

	SubRecord *key3 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *key4 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *pkey1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_PAD);
	index = m_index->getIndex(1);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key3, key4, pkey1, true));
}

/**
 * 测试索引模块采样功能
 */
void IndexOperationTestCase::testSample() {
	uint step = 1;

	{
		cout << endl << endl << "Test large index tress sampling" << endl;
		// 建立测试索引
		m_tblDef = createSmallTableDefWithSame();
		m_heap = createSmallHeapWithSame(m_tblDef);
		uint totalKeys = buildHeapWithSame(m_heap, m_tblDef, DATA_SCALE * 5, step);
		try {
			createIndex(m_path, m_heap, m_tblDef);
		} catch (NtseException) {
			CPPUNIT_ASSERT(false);
		}

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// 开始执行各种采样规模的统计
		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 10);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 10);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 1000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 1000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 2000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 2000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 3000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 3000);
		cout << endl;

		// 删除部分范围,继续测试
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

		IndexScanHandle *indexHandle;
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		RowLockHandle *rlh;
		DrsIndex *rangeIndex = m_index->getIndex(0);
		indexHandle = rangeIndex->beginScan(session, NULL, true, true, Exclusived, &rlh, extractor);
		int count = 0;
		while (rangeIndex->getNext(indexHandle, idxKey)) {
			// 去对应Heap取记录
			record->m_format = REC_FIXLEN;
			m_heap->getRecord(m_session, idxKey->m_rowId, record);
			record->m_format = REC_REDUNDANT;
			session->startTransaction(TXN_DELETE, m_tblDef->m_id);
			m_index->deleteIndexEntries(session, record, indexHandle);
			session->endTransaction(true);
			session->unlockRow(&rlh);
			count++;
			if (count > 500)
				break;
		}
		rangeIndex->endScan(indexHandle);

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 10);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 10);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 1000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 1000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 2000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 2000);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 3000);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 3000);
		cout << endl;

		// 调用索引提供的接口
		cout << "Use Index interface: " << indexDef->m_name << endl;
		index->updateExtendStatus(session, 30);
		IndexStatusEx status = index->getStatusEx();
		cout << "Result by 30: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;
		index->updateExtendStatus(session, 50);
		status = index->getStatusEx();
		cout << "Result by 50: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;
		index->updateExtendStatus(session, 80);
		status = index->getStatusEx();
		cout << "Result by 80: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	{
		cout << endl << endl << "Test very little index tress sampling" << endl;
		// 建立测试索引
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		uint totalKeys = buildHeap(m_heap, m_tblDef, 2, step);
		try {
			createIndex(m_path, m_heap, m_tblDef);
		} catch (NtseException) {
			CPPUNIT_ASSERT(false);
		}

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// 开始执行各种采样规模的统计
		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 10);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 10);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		// 调用索引提供的接口
		cout << "Use Index interface: " << indexDef->m_name << endl;
		index->updateExtendStatus(session, 30);
		IndexStatusEx status = index->getStatusEx();
		cout << "Result by 30: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;
		index->updateExtendStatus(session, 50);
		status = index->getStatusEx();
		cout << "Result by 50: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	{
		cout << endl << endl << "Test empty index tress sampling" << endl;
		// 建立测试索引
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		try {
			createIndex(m_path, m_heap, m_tblDef);
		} catch (NtseException) {
			CPPUNIT_ASSERT(false);
		}

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(1);
		IndexDef *indexDef = m_tblDef->m_indice[1];

		// 开始执行各种采样规模的统计
		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 40);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 40);
		cout << endl;

		sampleIndex(m_tblDef->m_indice[0]->m_name, m_index->getIndex(0), 100);
		sampleIndex(m_tblDef->m_indice[1]->m_name, m_index->getIndex(1), 100);
		cout << endl;

		// 调用索引提供的接口
		cout << "Use Index interface: " << indexDef->m_name << endl;
		index->updateExtendStatus(session, 50);
		IndexStatusEx status = index->getStatusEx();
		cout << "Result by 50: " << status.m_pctUsed << "	" << status.m_compressRatio << endl;

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}

/**
 * 对指定的一个索引进行采样
 * @param name	索引名称
 * @param index	采样索引
 * @param samplePages	采样页面数
 */
void IndexOperationTestCase::sampleIndex(const char *name, DrsIndex *index, uint samplePages) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::sampleIndex", conn);
	SampleResult *result = SampleAnalyse::sampleAnalyse(session, index, samplePages, 0);
	cout << "Index: " << name << "	 Sample range: " << samplePages << endl;
	cout << "field num is " << result->m_numFields << endl;
	cout << "sample num is " << result->m_numSamples << endl;
	cout << "pctUsed is " << result->m_fieldCalc[0].m_average * 1.0 / INDEX_PAGE_SIZE << endl;
	cout << "compress ratio is " << result->m_fieldCalc[2].m_average * 1.0 / result->m_fieldCalc[1].m_average << endl;
	delete result;

	result = SampleAnalyse::sampleAnalyse(session, index, samplePages, 50, false);
	cout << "Index: " << name << "	 Sample range: " << samplePages << endl;
	cout << "field num is " << result->m_numFields << endl;
	cout << "sample num is " << result->m_numSamples << endl;
	cout << "pctUsed is " << result->m_fieldCalc[0].m_average * 1.0 / INDEX_PAGE_SIZE << endl;
	cout << "compress ratio is " << result->m_fieldCalc[2].m_average * 1.0 / result->m_fieldCalc[1].m_average << endl;
	delete result;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 测试这么一种情况：类似初始建立好一个两层的主键索引，根页面R，左右儿子P1，P2
 * P1的最大值k是R当中的第一个键值，此时执行了一次P1最大键值k的删除操作，
 * 之后有操作将某个键值k'的主键更新成对应k的主键，但是k'的rowId大于k，
 * 更新之后，k'必须存在在P2页面——因为rowId的原因，它比R当中的第一项还大，虽然键值相等。
 * 这个时候再拿着k的主键来进行查询，应该保证能找到k'这个键值记录
 *
 * 由于插入的时候有比较rowId，所以索引会严格按照key-rowId的整体大小排序，
 * 但是查询的时候可能只有主键，没有rowId，这样定位到叶页面的时候可能会出现定位不准确，需要特殊处理
 * 在前面的例子，如果没有特殊处理的话，定位k'的时候只会定位到P1，然后就回返回找不到键值
 * 该bug是在功能测试1.1.2用例发现的，详见http://172.20.0.53:3000/issues/show/667
 */
void IndexOperationTestCase::testStrangePKFetch() {
	// 建立测试索引
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	uint totalKeys = buildHeap(m_heap, m_tblDef, 150, 1);
	try {
		createIndex(m_path, m_heap, m_tblDef);
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testRecordsInRange", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(1);
	IndexDef *indexDef = m_tblDef->m_indice[1];

	// 此时第一个叶节点的最后一个键值的主键是1099，删除掉该键值，然后插入相同键值，不过rowId更大的键值，这时候应该插入在第二个叶页面
	u64 key;

#if (NTSE_PAGE_SIZE == 1024)
	key = 1012;
#else
	key = 1115;
#endif
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *record = createRecordRedundant(m_tblDef, 0, key, name, (key + ((u64)key << 32)), (u32)((u32)(-1) - key));

	SubRecordBuilder keyBuilder(m_tblDef, KEY_PAD, INVALID_ROW_ID);
	SubRecord *findKey = keyBuilder.createSubRecordById("0", &key);
	RowId rowId;
	CPPUNIT_ASSERT(index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL));
	record->m_rowId = rowId;

	RowLockHandle *rlh;
	IndexScanHandle *handle = index->beginScan(session, findKey, true, true, Exclusived, &rlh, NULL);
	CPPUNIT_ASSERT(index->getNext(handle, NULL));
	session->startTransaction(TXN_DELETE, m_tblDef->m_id);
	m_index->deleteIndexEntries(session, record, handle);
	session->endTransaction(true);
	session->unlockRow(&rlh);
	index->endScan(handle);

	CPPUNIT_ASSERT(!index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL));

	record->m_rowId = INVALID_ROW_ID - 1;
	uint dupIndex;
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	CPPUNIT_ASSERT(m_index->insertIndexEntries(session, record, &dupIndex));
	session->endTransaction(true);

	CPPUNIT_ASSERT(index->getByUniqueKey(session, findKey, None, &rowId, NULL, NULL, NULL));

	freeRecord(record);
	freeSubRecord(findKey);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 在唯一索引上查找一个制定建制的内容
 */
bool IndexOperationTestCase::findAKey(Session *session, DrsIndex *index, RowLockHandle **rlh, uint i, SubToSubExtractor *extractor) {
	SubRecord *findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	RowId rowId;
	bool found = index->getByUniqueKey(session, findKey, (rlh == NULL ? None : Exclusived), &rowId, NULL, rlh, extractor);
	freeSubRecord(findKey);
	return found;
}


/**
 * 测试唯一性插入和更新失败之后的回退部分代码
 * @case1 建立一个小堆，包含1条记录即可，但是该堆的索引必须包含三个索引，前两个是唯一性索引
 *			尝试插入一个相同的记录，该记录只有在第二个唯一性索引插入时才会出错，这样应该会导致第一个索引插入回退
 * @case2 测试过程同case1，不同的是操作换成更新操作，测试更新的回退
 */
void IndexOperationTestCase::testUniqueKeyConflict() {
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	uint totalKeys = buildHeap(m_heap, m_tblDef, 2, 1);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUniqueKeyConflict", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	{	// 增加一个唯一性索引
		assert(memcmp(m_tblDef->m_columns[2]->m_name, "BankAccount", sizeof("BankAccount")) == 0);
		u32 prefixesArr[] = {0};
		IndexDef indexDef("AnotherUnique", 1, &m_tblDef->m_columns[2], prefixesArr, true, false);
		m_tblDef->addIndex(&indexDef);
		try {
			m_index->createIndexPhaseOne(m_session, m_tblDef->m_indice[2], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[2]);
		} catch (NtseException) { NTSE_ASSERT(false); }
	}

	// case1 测试部分
	// 开始插入一个会导致新索引2，即BankAccount索引唯一性冲突的键值
	uint newKey = HOLE + 2;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", newKey);
	// 该记录创建保证BankAccount字段是冲突的，其他字段不冲突
	Record *insertrecord = createRecordRedundant(m_tblDef, 0, newKey, name, HOLE + ((u64)HOLE << 32), (u32)(-1) - newKey);
	insertrecord->m_rowId = 12345;	// 插入不会成功，rowId可以伪造

	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	bool successful;
	uint dupIndex = (uint)-1;
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	CPPUNIT_ASSERT(!successful && dupIndex == 2);
	session->endTransaction(true);

	freeRecord(insertrecord);

	// 验证插入不成功，应该还是只有1项
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(2), NULL, NULL, totalKeys, true);
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);

	/****************************************************************************************************/

	// case2 测试部分
	// 执行更新操作，更新两个字段，一个是userId字段，这个字段保证能够更新成功
	// 另一个更新bankaccount字段，这个要测试会冲突的情况
	SubRecord *before, *after;
	before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

	u16 *columns = new u16[m_tblDef->m_numCols];
	for (int k = 0; k < m_tblDef->m_numCols; k++)
		columns[k] = (u16)k;

	before->m_numCols = m_tblDef->m_numCols;
	before->m_columns = columns;

	Record *oldrecord;
	uint updated = 0;
	// 构造新键值
	sprintf(name, "Kenneth Tse Jr. %d \0", newKey);
	after = createUpdateAfterRecordRedundant3(m_tblDef, rowids[0], newKey, HOLE + 1 + ((u64)(HOLE + 1) << 32));

	// 构造旧键值
	sprintf(name, "Kenneth Tse Jr. %d \0", HOLE);
	oldrecord = createRecord(m_tblDef, 0, HOLE, name, HOLE + ((u64)HOLE << 32), (u32)(-1) - HOLE);
	oldrecord->m_rowId = rowids[0];
	before->m_size = oldrecord->m_size;
	before->m_data = oldrecord->m_data;
	before->m_rowId = oldrecord->m_rowId;

	session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
	// 进行更新
	dupIndex = (uint)-1;
	CPPUNIT_ASSERT(!m_index->updateIndexEntries(session, before, after, false, &dupIndex));
	session->endTransaction(true);

	CPPUNIT_ASSERT(dupIndex == 2);

	freeSubRecord(after);
	freeRecord(oldrecord);

	// 验证插入不成功，应该还是只有1项
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(2), NULL, NULL, totalKeys, true);
	rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);

	delete [] columns;

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


class LockOnlyThread : public Thread {
public:
	LockOnlyThread(Session *session) : Thread("LockOnlyThread"), m_mutex("LockOnlyThreadMutex", __FILE__, __LINE__) {
		m_session = session;
		m_lockObj = (u64)-1;
		m_exit = false;
		m_lastSucc = false;
	}

	void run() {
		while (true) {
			u64 lockObj = (u64)-1;
			if (m_exit)
				break;
			LOCK(&m_mutex);
			if (m_lockObj != (u64)-1) {
				lockObj = m_lockObj;
				m_lockObj = (u64)-1;
			}
			UNLOCK(&m_mutex);
			if (lockObj != (u64)-1)
				m_lastSucc = m_session->lockIdxObject(lockObj);
		}

		m_session->unlockIdxAllObjects();
	}

	void setLock(u64 lockObj) {
		LOCK(&m_mutex);
		m_lockObj = lockObj;
		m_lastSucc = false;
		UNLOCK(&m_mutex);
	}

	void setExit() {
		m_exit = true;
	}

	bool getLastSucc() {
		return m_lastSucc;
	}

private:
	Session *m_session;
	u64		m_lockObj;
	Mutex	m_mutex;
	bool	m_exit;
	volatile bool	m_lastSucc;
};


/**
 * 通过构造死锁的场景，测试DML的更新操作过程中对死锁的处理流程的正确性
 * @case1 测试插入过程中的死锁，先建立一个索引，只包含几条记录，执行批量插入，构造死锁场景
 * @case2 测试删除过程中的死锁，同case1的步骤类似
 */
void IndexOperationTestCase::testDMLDeadLocks() {
	{	// case 1 测试插入的死锁
		uint testScale = 80;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		uint totalKeys = buildHeap(m_heap, m_tblDef, testScale, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn);
		Connection *conn1 = m_db->getConnection(false);
		Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn1);
		MemoryContext *memoryContext = session->getMemoryContext();

		Bee bee("I'm also a hardworker, of course", m_db, conn, m_heap, m_tblDef, m_index, HOLE + testScale, 50, INVALID_ROW_ID);

		// 此时索引树应该只有一个根页面，连续插入之后会导致SMO操作
		// 首先由另一个线程，加SMO锁，插入线程一直操作直到准备加SMO锁，
		// 另一个线程此时等待加页面锁，让插入线程继续，这时候插入线程应该会死锁回退
		// 专门加锁的线程此时释放所有的锁，使得插入线程可以完成操作
		// 最后验证正确性

		// 先加上SMO锁
		u64 smoId = 2;
		u64 realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | smoId;
		LockOnlyThread lot(session1);
		lot.start();
		lot.setLock(realLockObj);
		while (!lot.getLastSucc());	// 这里加锁肯定可以成功

		// 插入线程准备SMO
		bee.enableSyncPoint(SP_IDX_TO_LOCK_SMO);
		bee.start();
		bee.joinSyncPoint(SP_IDX_TO_LOCK_SMO);

		// 再预先加上页面锁
		u64 pageId = 257;
		realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | pageId;			
		lot.setLock(realLockObj);
		Thread::msleep(100);
		CPPUNIT_ASSERT(!lot.getLastSucc());	// 这个时候加不上锁

		// 让插入线程继续加锁插入，导致死锁回退
		bee.disableSyncPoint(SP_IDX_TO_LOCK_SMO);
		bee.notifySyncPoint(SP_IDX_TO_LOCK_SMO);

		// 此时bee线程死锁回退，加锁线程可以获得锁，指定它结束，保证插入可以继续进行
		while (!lot.getLastSucc());
		lot.setExit();

		bee.join();

		totalKeys += 50;

		// 验证索引正确性
		rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);
		
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->getSessionManager()->freeSession(session1);
		m_db->freeConnection(conn1);

		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{	// case 2 测试删除的死锁
		uint testScale = 80;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		uint totalKeys = buildHeap(m_heap, m_tblDef, testScale, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn);
		Connection *conn1 = m_db->getConnection(false);
		Session *session1 = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testDMLDeadLocks", conn1);
		MemoryContext *memoryContext = session->getMemoryContext();

		SimpleMan simpleman("I like simple", m_db, conn, m_heap, m_tblDef, m_index, HOLE, true, INVALID_ROW_ID, true);

		// 此时索引树应该只有一个根页面，连续删除之后会导致SMO操作
		// 首先由另一个线程，加SMO锁，删除线程一直操作直到准备加SMO锁，
		// 另一个线程此时等待加页面锁，让删除线程继续，这时候删除线程应该会死锁回退
		// 专门加锁的线程此时释放所有的锁，使得删除线程可以完成操作
		// 最后验证正确性

		// 先加上SMO锁
		u64 smoId = 1;
		u64 realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | smoId;
		LockOnlyThread lot(session1);
		lot.start();
		lot.setLock(realLockObj);
		while (!lot.getLastSucc());	// 这里加锁肯定可以成功

		// 删除线程准备SMO
		simpleman.enableSyncPoint(SP_IDX_TO_LOCK_SMO);
		simpleman.start();
		simpleman.joinSyncPoint(SP_IDX_TO_LOCK_SMO);

		// 再预先加上页面锁
		u64 pageId = 129;
		realLockObj = ((u64)m_tblDef->m_id << ((sizeof(u64) - sizeof(m_tblDef->m_id)) * 8)) | pageId;			
		lot.setLock(realLockObj);
		Thread::msleep(100);
		CPPUNIT_ASSERT(!lot.getLastSucc());	// 这个时候加不上锁

		// 让插入线程继续加锁插入，导致死锁回退
		simpleman.disableSyncPoint(SP_IDX_TO_LOCK_SMO);
		simpleman.notifySyncPoint(SP_IDX_TO_LOCK_SMO);

		// 此时bee线程死锁回退，加锁线程可以获得锁，指定它结束，保证插入可以继续进行
		while (!lot.getLastSucc());
		lot.setExit();

		simpleman.join();

		totalKeys -= totalKeys;

		// 验证索引正确性
		rangeScanForwardCheck(session, m_tblDef, m_index->getIndex(1), NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->getSessionManager()->freeSession(session1);
		m_db->freeConnection(conn1);

		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}
}
/*
void IndexOperationTestCase::testInsertAfterSearch() {
	uint testScale = DATA_SCALE / 5;
	// 测试唯一性索引插入
	uint totalKeys;
	uint step = 3;
	// 建立测试索引
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
	createIndex(m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testInsert", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	checkIndex(session, memoryContext, totalKeys);

	// 对索引1进行查找
	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);

	DrsIndex *uniqueIndex = m_index->getIndex(1);

	// 验证所有索引没有的键值都能被插入
	for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
		// 首先插入堆
		Record *temp  = insertHeapSpecifiedRecord(session, memoryContext, i);

		SubRecord *key1 = IndexKey::allocSubRecord(session->getMemoryContext(), false, temp, 
				m_tblDef, m_tblDef->m_indice[1]);

		DrsIndexScanHandleInfo *scanHdlInfo = NULL;
		CPPUNIT_ASSERT(!uniqueIndex->checkDuplicate(session, key1, &scanHdlInfo));
		CPPUNIT_ASSERT(NULL != scanHdlInfo);

		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		//插入唯一性索引键值
		CPPUNIT_ASSERT(uniqueIndex->insertGotPage(scanHdlInfo));		
		SubRecord *key0 = IndexKey::allocSubRecord(session->getMemoryContext(), false, temp, m_tblDef, 
			m_tblDef->m_indice[0]);
		//插入非唯一性索引键值
		m_index->getIndex(0)->insertNoCheckDuplicate(session, key0);		
		session->endTransaction(true);

		CPPUNIT_ASSERT(uniqueIndex->checkDuplicate(session, key1, NULL));

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}
	for (int i = HOLE + 2; i < testScale + HOLE + END_SCALE; i += step) {	// 再附加插入范围外的数据
		// 首先插入堆
		Record *temp = insertSpecifiedRecord(session, memoryContext, i);

		checkIndex(session, memoryContext, totalKeys);

		totalKeys++;

		freeRecord(temp);
	}

	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	u16 *columns = new u16[m_tblDef->m_numCols];
	for (int k = 0; k < m_tblDef->m_numCols; k++)
		columns[k] = (u16)k;

	// 验证插入之后索引总的项数相等
	// 做索引扫描，得到总项数
	DrsIndex *index = m_index->getIndex(0);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// 验证已经插入的键值都不能被插入
	// 做tablescan，得到若干个record，验证都不能被插入
	uint reinsertNums = 20;
	idxKey->m_columns = columns;
	idxKey->m_numCols = m_tblDef->m_numCols;
	IndexDef *indexDef = m_tblDef->m_indice[1];
	DrsHeapScanHandle *scanHandle = m_heap->beginScan(session, SubrecExtractor::createInst(session, m_tblDef, idxKey), None, NULL, false);
	
	int count = 0;
	while (true) {
		if (m_heap->getNext(scanHandle, idxKey)) {
			if (!m_heap->getNext(scanHandle, idxKey))
				break;
		} else
			break;

		record->m_size = idxKey->m_size;
		record->m_rowId = idxKey->m_rowId;
		memcpy(record->m_data, idxKey->m_data, idxKey->m_size);

		u64 savepoint = session->getMemoryContext()->setSavepoint();
		SubRecord *key = IndexKey::allocSubRecord(session->getMemoryContext(), false, record, m_tblDef, indexDef);
		CPPUNIT_ASSERT(uniqueIndex->checkDuplicate(session, key, NULL));
		session->getMemoryContext()->resetToSavepoint(savepoint);

		checkIndex(session, memoryContext, totalKeys);

		count++;

		if (count > reinsertNums)
			break;
	}
	m_heap->endScan(scanHandle);

	// 验证插入之后索引总的项数相等
	// 做索引扫描，得到总项数
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;

	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	delete[] columns;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}
*/
/////////////////////////////////////////////////////////////////////////////////////
/************************************************************************/
/* 索引恢复单元测试
/* 测试用例将使用一个小堆执行，创建两个索引定义，第一个索引是非唯一性的
/* 第二个索引是唯一性索引，便于有需要测试唯一性冲突使用
/* 测试用例当中所有用到的索引都是采用前述的两个，至于操作哪一个索引执行
/* 具体的操作根据测试用例自己的需求选择
/************************************************************************/


const char* IndexRecoveryTestCase::getName() {
	return "Index recovery test";
}

const char* IndexRecoveryTestCase::getDescription() {
	return "Test Index recovery(recovery of all kinds of operations, including redo, undo and redo compensation logs).";
}

bool IndexRecoveryTestCase::isBig() {
	return false;
}

void IndexRecoveryTestCase::setUp() {
	m_index = NULL;
	m_heap = NULL;
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::setUp", m_conn);
	char name[255];	// = "testIndex.nti"
	sprintf(name, "%s", INDEX_NAME);
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	clearRSFile(&m_cfg);
	ts.idx = true;
	vs.idx = true;
}

void IndexRecoveryTestCase::tearDown() {
	// 丢索引
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// 丢表
	if (m_heap != NULL) {
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		m_db = NULL;
		Database::drop(".");
	}

	ts.idx = false;
	vs.idx = false;
}


/**
 * 根据表定义创建两个带数据的索引
 */
void IndexRecoveryTestCase::createIndex(char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException) {
	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(m_db, m_session, path, tblDef, NULL);
	try {
		m_index->createIndexPhaseOne(m_session, tblDef->m_indice[0], tblDef, heap);
		m_index->createIndexPhaseTwo(tblDef->m_indice[0]);
		if (tblDef->m_numIndice > 1) {
			m_index->createIndexPhaseOne(m_session, tblDef->m_indice[1], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[1]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}
}

TableDef* IndexRecoveryTestCase::createSmallTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50, false, false);
	builder->addColumn("BankAccount", CT_BIGINT, false)->addColumn("Balance", CT_INT, false);
	builder->addIndex("NOCONOSTRAIN", false, false, false, "UserId", 0, "BankAccount", 0, NULL);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));
	delete builder;

	return tableDef;
}
/**
 * 创建一个小堆，不带数据
 */
DrsHeap* IndexRecoveryTestCase::createSmallHeap(TableDef *tableDef) {
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();
	DrsHeap *heap = NULL;

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(heap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return heap;
}



/**
 * 关闭创建的堆
 */
void IndexRecoveryTestCase::closeSmallHeap(DrsHeap *heap) {
	EXCPT_OPER(heap->close(m_session, true));
	delete heap;
}


/**
 * 在堆中插入指定规模的数据
 */
uint IndexRecoveryTestCase::buildHeap(DrsHeap *heap, const TableDef *tblDef, uint size, uint step) {
	assert(step > 0);
	uint count = 0;
	Record *record;
	for (int i = HOLE; i < size + HOLE; i += step) {
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}


/**
 * 创建大表数据，该表为了测试索引键值超过128而建立
 * 因此表中设计索引的字符串长度必须达到甚至超过128
 */
uint IndexRecoveryTestCase::buildBigHeap(DrsHeap *heap, const TableDef *tblDef, bool testPrefix, uint size, uint step) {
	Record *record;
	uint count = 0;
	for (uint i = HOLE; i < HOLE + size; i += step) {
		char name[LONG_KEY_LENGTH];
		RowId rid;
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		rowids[i - HOLE] = rid;
		count++;
		freeRecord(record);
	}

	return count;
}

TableDef* IndexRecoveryTestCase::createBigTableDef() {
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, LONG_KEY_LENGTH);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));
	delete builder;

	return tableDef;
}

/**
 * 创建大堆，记录长度超过100多
 * 其中只包含一个索引，并且是非唯一性的索引
 */
DrsHeap* IndexRecoveryTestCase::createBigHeap(TableDef* tableDef) {
	DrsHeap *heap;
	char tableName[255] = HEAP_NAME;
	
	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(heap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return heap;
}


/**
 * 测试新索引恢复当中需要使用的补充回复接口以及记录恢复过程更新结束日志接口
 * @case1	直接测试更新结束日志接口，提高覆盖率
 * @case2	对一个空索引执行插入，插入之前，先伪装索引信息为只有N－1个索引。
 *			插入之后，再调用不重插入恢复接口，最后测试所有索引项数一样。
 * @case3	和case2类似，不同的是测试补充删除接口
 * @case4	和case2类似，不同的是测试补充更新接口
 * @case4	和case4类似，不同的是测试补充更新缺少插入的接口接口
 */
void IndexRecoveryTestCase::testCPSTDMLInRecv() {
	RowId rowId;
	uint totalKeys;
	/********************* case 1 & case 2 ***************************/
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = 0;/*buildHeap(m_heap, 150, 1);*/
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);
		
		// 测试记录日志接口
		m_index->logDMLDoneInRecv(session, 0, true);

		// 修改索引的个数为1个，然后执行插入一条记录的操作
		((DrsBPTreeIndice*)m_index)->decIndexNum();
		Record *temp = insertSpecifiedRecord(session, memoryContext, 0);
		rowId = temp->m_rowId;
		// 修改索引数为2，执行补充插入接口
		((DrsBPTreeIndice*)m_index)->incIndexNum();
		m_index->recvInsertIndexEntries(session, temp, 0, 0);
		totalKeys++;
		freeRecord(temp);

		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	/********************* case 3 ***************************/
	{	// 和前一个测试用例相结合
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);

		// 修改索引的个数为1个，然后执行删除一条记录的操作
		((DrsBPTreeIndice*)m_index)->decIndexNum();
		char name[50];
		sprintf(name, "Kenneth Tse Jr. %d \0", 0);
		Record *record = createRecordRedundant(m_tblDef, 0, 0, name, 0 + ((u64)0 << 32), (u32)(-1) - 0);
		record->m_rowId = rowId;
		session->startTransaction(TXN_DELETE, m_tblDef->m_id);
		EXCPT_OPER(m_index->deleteIndexEntries(session, record, NULL));
		session->endTransaction(true);
		((DrsBPTreeIndice*)m_index)->incIndexNum();
		m_index->recvDeleteIndexEntries(session, record, 0, 0);
		freeRecord(record);

		totalKeys--;
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	/********************* case 4 ***************************/
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, 1, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);
		RowId rowId = 8192;

		// 修改索引的个数为1个，然后执行插入一条记录的操作
		((DrsBPTreeIndice*)m_index)->decIndexNum();

		SubRecord *before, *after;
		u64 oldKey = HOLE, newKey = 1 + HOLE;
		before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

		u16 *columns = new u16[m_tblDef->m_numCols];
		for (int k = 0; k < m_tblDef->m_numCols; k++)
			columns[k] = (u16)k;

		before->m_numCols = m_tblDef->m_numCols;
		before->m_columns = columns;

		Record *oldrecord;

		// 构造新键值
		char name[50];
		sprintf(name, "Kenneth Tse Jr. %d \0", oldKey);
		after = createUpdateAfterRecordRedundant1(m_tblDef, rowId, newKey);

		// 构造旧键值
		oldrecord = createRecord(m_tblDef, rowId, oldKey, name, oldKey + ((u64)oldKey << 32), (u32)(-1) - (u32)oldKey);
		oldrecord->m_rowId = rowId;
		before->m_size = oldrecord->m_size;
		before->m_data = oldrecord->m_data;
		before->m_rowId = oldrecord->m_rowId;

		session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
		// 进行更新
		uint dupIndex = (uint)-1;
		CPPUNIT_ASSERT(m_index->updateIndexEntries(session, before, after, false, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(dupIndex == (uint)-1);

		// 修改索引数为2，执行补充更新接口
		((DrsBPTreeIndice*)m_index)->incIndexNum();
		m_index->recvUpdateIndexEntries(session, before, after, 0, 0, true);

		// 清除环境
		freeSubRecord(after);
		freeRecord(oldrecord);
		delete[] columns;

		// 验证索引0被更新了
		CPPUNIT_ASSERT(fetchByKeyFromIndex0(session, index, m_tblDef, newKey, HOLE + ((u64)HOLE << 32), 8192));
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	/********************* case 5 ***************************/
	{
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		totalKeys = buildHeap(m_heap, m_tblDef, 1, 1);
		createIndex(m_path, m_heap, m_tblDef);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testCPSTDMLInRecv", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		DrsIndex *index = m_index->getIndex(0);
		RowId rowId = 8192;

		// 修改索引的个数为1个，然后执行插入一条记录的操作
		((DrsBPTreeIndice*)m_index)->decIndexNum();

		SubRecord *before, *after;
		u64 oldKey = HOLE, newKey = 1 + HOLE;
		before = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);

		u16 *columns = new u16[m_tblDef->m_numCols];
		for (int k = 0; k < m_tblDef->m_numCols; k++)
			columns[k] = (u16)k;

		before->m_numCols = m_tblDef->m_numCols;
		before->m_columns = columns;

		Record *oldrecord;

		// 构造新键值
		char name[50];
		sprintf(name, "Kenneth Tse Jr. %d \0", oldKey);
		after = createUpdateAfterRecordRedundant1(m_tblDef, rowId, newKey);

		// 构造旧键值
		oldrecord = createRecord(m_tblDef, rowId, oldKey, name, oldKey + ((u64)oldKey << 32), (u32)(-1) - (u32)oldKey);
		oldrecord->m_rowId = rowId;
		before->m_size = oldrecord->m_size;
		before->m_data = oldrecord->m_data;
		before->m_rowId = oldrecord->m_rowId;

		session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
		// 进行更新
		uint dupIndex = (uint)-1;
		CPPUNIT_ASSERT(m_index->updateIndexEntries(session, before, after, false, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(dupIndex == (uint)-1);

		// 修改索引数为2，对第二个索引进行一次删除操作，实际上是删光
		((DrsBPTreeIndice*)m_index)->incIndexNum();

		IndexDef *indexDef = m_tblDef->m_indice[0];
		//	删除索引数据，模拟索引处于更新只只行了删除操作的状态
		uint count = (uint)oldKey;
		sprintf(name, "Kenneth Tse Jr. %d \0", count);
		Record *deleterecord = createRecord(m_tblDef, rowId, count, name, count + ((u64)count << 32), (u32)(-1) - count);
		deleterecord->m_format = REC_REDUNDANT;
		bool keyNeedCompress = RecordOper::isFastCCComparable(m_tblDef, indexDef, indexDef->m_numCols, indexDef->m_columns);
		SubRecord *deletekey = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, deleterecord, NULL, m_tblDef, indexDef);
		index->del(session, deletekey);

		// 执行更新当中插入操作的补充操作
		uint i = (uint)newKey;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		Record *insertrecord = createRecord(m_tblDef, rowId, newKey, name, count + ((u64)count << 32), (u32)(-1) - count);
		insertrecord->m_format = REC_REDUNDANT;
//		SubRecord *insertkey = IndexKey::allocSubRecord(memoryContext, keyNeedCompress, insertrecord, NULL, m_tblDef, indexDef);
		SubRecord insertkey( REC_REDUNDANT, indexDef->m_numCols, indexDef->m_columns, insertrecord->m_data, m_tblDef->m_maxRecSize, insertrecord->m_rowId);

		NTSE_ASSERT(m_index->recvCompleteHalfUpdate(session, &insertkey, 0, 0, NULL, true) == 1);
		// TODO: 测试后两个参数不为0和NULL的情况

		// 清除环境
		freeSubRecord(after);
		freeRecord(oldrecord);
		delete[] columns;
		freeRecord(insertrecord);
		freeRecord(deleterecord);

		// 验证索引0被更新了
		CPPUNIT_ASSERT(fetchByKeyFromIndex0(session, index, m_tblDef, newKey, HOLE + ((u64)HOLE << 32), 8192));
		rangeScanForwardCheck(session, m_tblDef, index, NULL, NULL, totalKeys, true);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}

/**
 * 测试创建索引相关的恢复操作
 * case1:	创建两个索引，第一个不带唯一性约束，第二个索引有唯一性约束，保证数据当中存在唯一性违背
 *			在创建两个索引结束之后，由于第二个索引会因为违反约束被丢弃，因此只有一个索引建立成功
 *			在未创建索引的数据基础上，重做前面建立索引的所有日志，应该能保证最后只有一个索引被创建
 * case2:	和case1类似，只是redo操作的时候索引的头页面和位图页已经是前面创建索引操作执行之后的状态，
 *			为的是测试在索引文件内容不正确的时候，能够正确处理恢复之后内存当中索引的相关内容
 * case3:	创建两个完整的索引，备份，构造出一条创建索引日志，用于将第二个索引创建操作undo，模拟第二个索引创建未完成系统崩溃的场景
 *			测试undoCreateIndex接口；之后恢复备份的数据，构造undoCreateIndex写的补偿日志，执行redoCPSTCreateIndex
 */
void IndexRecoveryTestCase::testCreateIndexRecovery() {
	// 测试索引创建过程的恢复，首先测试违反索引唯一性约束之后，索引日志正常redo，此时索引建立完全结束
	{
		uint totalKeys, items;
		uint step = 1;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE / 10, step);
		{	// 确认数据当中有重复项
			int dup = HOLE + totalKeys - 1;
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", dup);
			Record *record = createRecord(m_tblDef, 0, dup, name, dup + ((u64)dup << 32), (u32)(-1) - dup);
			EXCPT_OPER(rid = m_heap->insert(m_session, record, NULL));
			rowids[totalKeys] = rid;
			items++;
			totalKeys++;
			freeRecord(record);
		}

		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		u64 startLsn = m_session->getLastLsn();

		// 关闭数据库备份数据文件和索引文件，然后重新打开
		closeEnv();

		// 备份当前状态数据和索引文件
		backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
		backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

		openEnv();

		// 需要开启事务做索引创建操作
		m_session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

		// 建立不允许重复的索引，会造成最后全部回退。
		try {	// 创建带有唯一性约束的索引，最后必须会丢弃索引
			m_index->createIndexPhaseOne(m_session, m_tblDef->m_indice[0], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[0]);
			m_index->createIndexPhaseOne(m_session, m_tblDef->m_indice[1], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[1]);
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
		}

		// 关闭事务
		m_session->endTransaction(true);

		// 关闭数据库回复原始文件，重新打开准备恢复重做
		closeEnv();

		myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
		myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

		openEnv();

		IndexRecoveryManager manager(m_db, m_heap, m_tblDef, m_index);
		uint count = 0;
		// 根据记录的LSN，读取日志，重做创建索引操作
		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(startLsn, Txnlog::MAX_LSN);
		while (m_db->getTxnlog()->getNext(logHdl)) {
			LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
			//redo该日志
			manager.redoALog(m_session, logEntry, logHdl->curLsn());
			count++;
		}
		m_db->getTxnlog()->endScan(logHdl);

		// 验证索引文件状态
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// 读取头页面和位图页验证结果正确
		PageHandle *headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		PageHandle *bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		IndexFileBitMapPage *bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		byte *start = (byte*)bmPage + sizeof(Page);
		byte *end = start + 10;
		bool firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// 结束测试，关闭环境
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{	// 测试在创建索引失败之前，索引文件头页面和位图页面已经被写入到磁盘，重启恢复的时候会读到无效的索引数据记录在头页面
		uint totalKeys, items;
		uint step = 1;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE / 10, step);
		{	// 确认数据当中有重复项
			int dup = HOLE + totalKeys - 1;
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", dup);
			Record *record = createRecord(m_tblDef, 0, dup, name, dup + ((u64)dup << 32), (u32)(-1) - dup);
			EXCPT_OPER(rid = m_heap->insert(m_session, record, NULL));
			rowids[totalKeys] = rid;
			items++;
			totalKeys++;
			freeRecord(record);
		}

		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		u64 startLsn = m_session->getLastLsn();

		// 关闭数据库备份数据文件和索引文件，然后重新打开
		closeEnv();

		// 备份当前状态数据和索引文件
		backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
		backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

		openEnv();

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		BigProject bigProject("I need create lots of indices", m_db, conn, m_heap, m_tblDef, m_index, 0);

		bigProject.enableSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.start();
		bigProject.joinSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);	// 创建两个索引，关注第二个索引创建
		bigProject.notifySyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.joinSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);

		// 此时必定是创建了两个索引的根页面信息，并且线程内部已经对索引文件头页面页和位图页放锁
		// 拷贝保存这两个页面的内容
		byte headerbkPage[Limits::PAGE_SIZE];
		byte bitmapbkPage[Limits::PAGE_SIZE];

		PageHandle *headerHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		PageHandle *bitmapHandle = GET_PAGE(session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);

		byte *start1 = (byte*)headerHandle->getPage();
		byte *start2 = (byte*)bitmapHandle->getPage();

		memcpy(headerbkPage, start1, Limits::PAGE_SIZE);
		memcpy(bitmapbkPage, start2, Limits::PAGE_SIZE);

		session->releasePage(&headerHandle);
		session->releasePage(&bitmapHandle);

		bigProject.disableSyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.notifySyncPoint(SP_IDX_ALLOCED_ROOT_PAGE);
		bigProject.join();

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		// 创建索引线程结束，开始恢复测试
		closeEnv();

		// 备份当前状态数据和索引文件
		backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
		backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

		// 把备份的两个页面，写入磁盘文件
		File file(INDEX_NAME);
		CPPUNIT_ASSERT(file.open(false) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(file.write(0, Limits::PAGE_SIZE, headerbkPage) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(file.write(Limits::PAGE_SIZE, Limits::PAGE_SIZE, bitmapbkPage) == File::E_NO_ERROR);
		CPPUNIT_ASSERT(file.close() == File::E_NO_ERROR);

		// 重新打开文件读取环境，准备恢复
		openEnv();

		IndexRecoveryManager manager(m_db, m_heap, m_tblDef, m_index);

		uint count = 0;
		// 根据记录的LSN，读取日志，重做创建索引操作
		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(startLsn, Txnlog::MAX_LSN);
		while (m_db->getTxnlog()->getNext(logHdl)) {
			LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
			//redo该日志
			manager.redoALog(m_session, logEntry, logHdl->curLsn());
			count++;
		}
		m_db->getTxnlog()->endScan(logHdl);

		// 验证索引文件状态
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// 读取头页面和位图页验证结果正确
		PageHandle *headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		PageHandle *bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		IndexFileBitMapPage *bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		byte *start = (byte*)bmPage + sizeof(Page);
		byte *end = start + 10;
		bool firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// 结束测试，关闭环境
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);
	}

	{	// 测试undoCreateIndex接口和redoCPSTCreateIndex接口
		uint totalKeys, items;
		uint step = 1;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, DATA_SCALE / 10, step);

		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		m_index = DrsIndice::open(m_db, m_session, INDEX_NAME, m_tblDef, NULL);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

		try {
			m_index->createIndexPhaseOne(session, m_tblDef->m_indice[0], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[0]);
			m_index->createIndexPhaseOne(session, m_tblDef->m_indice[1], m_tblDef, m_heap);
			m_index->createIndexPhaseTwo(m_tblDef->m_indice[1]);
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
		}

		session->endTransaction(true);

		u64 startLsn = session->getLastLsn();

		{	// 备份一份数据，用于测试redoCPSTCreateIndex接口
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			// 创建索引线程结束，开始恢复测试
			closeEnv();

			// 备份当前状态数据和索引文件
			backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
			backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
			backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);

			openEnv();

			conn = m_db->getConnection(false);
			session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		}

		// 需要构造一条创建索引日志用于回退
		byte savedIndexId = 0x02;

		session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

		m_index->undoCreateIndex(session, 0, &savedIndexId, 1, true);

		session->endTransaction(true);

		// 验证索引被彻底丢弃
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// 读取头页面和位图页验证结果正确
		PageHandle *headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		IndexHeaderPage *headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		PageHandle *bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		IndexFileBitMapPage *bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		byte *start = (byte*)bmPage + sizeof(Page);
		byte *end = start + 10;
		bool firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// 关闭数据库回复原始文件，重新打开准备恢复重做
		closeEnv();

		myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
		myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

		openEnv();

		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testUpdate", conn);
		// 根据记录的LSN，读取日志，找到补偿日志内容
		bool redoneCPST = false;
		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(startLsn, Txnlog::MAX_LSN);
		while (m_db->getTxnlog()->getNext(logHdl)) {
			LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
			if (logEntry->m_logType == LOG_IDX_ADD_INDEX_CPST) {
				m_index->redoCpstCreateIndex(session, logHdl->curLsn(), logEntry->m_data, logEntry->m_size);
				redoneCPST = true;
			}
		}
		m_db->getTxnlog()->endScan(logHdl);
		CPPUNIT_ASSERT(redoneCPST);

		// 验证索引被彻底丢弃
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		// 读取头页面和位图页验证结果正确
		headerPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		headerPage = (IndexHeaderPage*)(headerPageHandle->getPage());
		CPPUNIT_ASSERT(headerPage->m_indexNum == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[0] == 1);
		CPPUNIT_ASSERT(headerPage->m_indexIds[1] == 0);
		CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 3);
		m_session->releasePage(&headerPageHandle);
		bmPageHandle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
		bmPage = (IndexFileBitMapPage*)bmPageHandle->getPage();
		start = (byte*)bmPage + sizeof(Page);
		end = start + 10;
		firstIndexExists = false;
		while (start != end) {
			CPPUNIT_ASSERT(*start != 0x02);
			if (*start == 0x01)
				firstIndexExists = true;
			++start;
		}
		CPPUNIT_ASSERT(firstIndexExists);
		m_session->releasePage(&bmPageHandle);

		// 结束测试，关闭环境
		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}


/**
 * 测试DML操作的恢复的redo相关操作
 * case1:	测试redo操作正确性
 *			首先建立一个带有批量数据的索引，保存一份当前索引的文件数据备份
 *			开始执行若干插入和删除操作保存操作的所有日志
 *			将索引文件恢复到未执行DML操作之前，将所有操作日志redo到索引上
 *			验证这个时候所有redo都成功，索引完整正确
 */
void IndexRecoveryTestCase::testDMLRecoveryRedo() {
	IndexRecoveryManager *manager;
	// 模仿创建索引恢复第一个测试用例，创建索引后先进行若干DML操作，redo所有DML日志，
	uint testScale = DATA_SCALE / 4;
	
	// Part1: 创建索引，插入若干数据，保存当前状态的索引和数据文件
	uint totalKeys, items;
	uint step = 2;
	m_tblDef = createSmallTableDef();
	m_heap = createSmallHeap(m_tblDef);
	items = totalKeys = buildHeap(m_heap, m_tblDef, testScale, step);
	File oldindexfile(m_path);
	oldindexfile.remove();
	DrsIndice::create(m_path, m_tblDef);

	u16 tableId = m_tblDef->m_id;

	createIndex(m_path, m_heap, m_tblDef);

	u64 startLsn = m_session->getLastLsn();

	// 关闭数据库备份数据文件和索引文件，然后重新打开
	closeEnv();

	// 备份当前状态数据和索引文件
	backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
	backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
	backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);


	// Part2:在建立索引的基础上，继续插入若干项，为的是得到插入操作的日志
	openEnv();

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	{	// 继续插入若干项
		Record *insertrecord = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
		RowId deleteRowId =  INVALID_ROW_ID;
		for (int i = HOLE + 1; i < testScale + HOLE; i += step) {
			Record *temp = insertSpecifiedRecord(session, memoryContext, i);
			totalKeys++;

			freeRecord(temp);
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeEnv();

	// Part3:还原文件到刚建立完索引的场景，读取日志内容，重做之前的插入日志
	myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
	myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

	openEnv();

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	memoryContext = session->getMemoryContext();

	// 验证索引结构完整
	checkIndex(session, memoryContext, totalKeys);

	// 重做前面所有DML日志
	manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
	manager->redoFromSpecifiedLogs(session, startLsn);
	delete manager;

	// Part4:验证索引结构完整
	uint count = 0;
	checkIndex(session, memoryContext, totalKeys);

	DrsIndex *index = m_index->getIndex(1);

	// 正反向遍历索引验证项数
	//SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	IndexScanHandle *indexHandle;
	IndexDef *indexDef = m_tblDef->m_indice[1];

	idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;
 
	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	indexHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);
	indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);

	u64 startLsn1 = m_db->getTxnlog()->tailLsn();	// 保留插入结束LSN待redo使用


	// 关闭数据库备份数据文件和索引文件，然后重新打开
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeEnv();

	// 备份当前状态数据和索引文件
	backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
	backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
	backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);


	openEnv();

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	memoryContext = session->getMemoryContext();
	// 至此插入操作日志的redo操作结束
	// 以下部分主要测试删除操作的redo
	// Part 5:删除若干项，得到删除日志
//	SubRecord *record = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *record = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	record->m_columns = m_tblDef->m_indice[1]->m_columns;
	record->m_numCols = m_tblDef->m_indice[1]->m_numCols;
	RowId rowId;
	SubRecord *findKey;
	Record *heapRec = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	index = m_index->getIndex(1);
	indexDef = m_tblDef->getIndexDef(1);

	SubToSubExtractor *extractor1 = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
		m_tblDef->m_indice[1]->m_numCols, m_tblDef->m_indice[1]->m_columns, record->m_numCols, record->m_columns, KEY_COMPRESS, KEY_PAD, 1);

	uint foundKeys = 0;
	for (int i = HOLE; i < testScale + HOLE; i += step) {
		findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
		findKey->m_rowId = INVALID_ROW_ID;
		bool found = index->getByUniqueKey(session, findKey, None, &rowId, record, NULL, extractor1);
		if (found) {	// 读取表记录，执行删除操作
			CPPUNIT_ASSERT(record->m_rowId != INVALID_ROW_ID);

			foundKeys++;

			heapRec->m_format = REC_FIXLEN;
			m_heap->getRecord(session, record->m_rowId, heapRec);
			heapRec->m_format = REC_REDUNDANT;

			session->startTransaction(TXN_DELETE, tableId);
			EXCPT_OPER(m_index->deleteIndexEntries(session, heapRec, NULL));
			session->endTransaction(true);
			totalKeys--;
		}
		freeSubRecord(findKey);
	}

	CPPUNIT_ASSERT(foundKeys != 0);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeEnv();

	// Part6:还原文件到插入结束时的索引状态，读取日志内容，重做之前的删除日志
	myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
	myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

	openEnv();

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	memoryContext = session->getMemoryContext();

	// 重做前面所有DML日志
	manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
	manager->redoFromSpecifiedLogs(session, startLsn1);
	delete manager;

	// Part7:验证索引结构完整
	checkIndex(session, memoryContext, totalKeys);

	index = m_index->getIndex(1);
	// 正反向遍历索引验证项数
//	idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
	idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;

	SubToSubExtractor *extractor2 = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, m_tblDef->getIndexDef(1),
		m_tblDef->m_indice[1]->m_numCols, m_tblDef->m_indice[1]->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	indexHandle = index->beginScan(session, NULL, true, true, None, NULL, extractor2);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);

	indexHandle = index->beginScan(session, NULL, false, true, None, NULL, extractor2);
	count = 0;
	while (index->getNext(indexHandle, idxKey))
		count++;
	index->endScan(indexHandle);
	CPPUNIT_ASSERT(count == totalKeys);

	// 结束redo测试，关闭环境
	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/*
 * 测试DML操作的恢复的undo相关操作
 * case1:	测试undo操作正确性
 *			首先建立一个带有批量数据的索引
 *			在索引上执行若干DML操作，包括插入和删除，每执行完一次操作，将该操作的相关日志都undo
 *			这时候相当于DML操作没有执行，继续再重做一遍DML操作使得DML操作真正应用于索引
 *			具体国城市先执行插入操作的undo测试，再执行删除操作的udno测试，最后索引树被删空
 *			验证索引正确性
 */
void IndexRecoveryTestCase::testDMLRecoveryUndo() {
	IndexRecoveryManager *manager = NULL;
	uint testScale = DATA_SCALE / 4;
	m_db->setCheckpointEnabled(false);
	if (!isEssentialOnly())
	{	// 该部分测试日志的undo恢复
		// Part1:建立一个小索引，后面陆续插入数据删除数据，便于测试SMO的恢复
		uint origSize = 30;
		uint totalKeys, items;
		uint step = 2;
		m_tblDef = createSmallTableDef();
		m_heap = createSmallHeap(m_tblDef);
		items = totalKeys = buildHeap(m_heap, m_tblDef, origSize, step);
		File oldindexfile(m_path);
		oldindexfile.remove();
		DrsIndice::create(m_path, m_tblDef);

		u16 tableId = m_tblDef->m_id;

		createIndex(m_path, m_heap, m_tblDef);

		u64 startLsn = m_db->getTxnlog()->tailLsn();;

		DrsIndex *index;

		manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
		MemoryContext *memoryContext = session->getMemoryContext();

		manager->setTxnType(TXN_INSERT);
		for (uint i = HOLE + 1; i < HOLE + origSize * 20; i += step) {
			// 插入一条新纪录
			session->startTransaction(TXN_INSERT, tableId);
			startLsn = session->getLastLsn();
			session->endTransaction(true);
			Record *insertedRecord = insertSpecifiedRecord(session, memoryContext, i);
			u64 curLsn = session->getLastLsn();
			CPPUNIT_ASSERT(curLsn > startLsn);
			// undo刚才的插入，验证undo正确性
			manager->undoFromSpecifiedLogs(session, startLsn, curLsn);

			// 检查undo的结果保证正确
			checkIndex(session, memoryContext, totalKeys);

			// 再重做一次刚才的插入，保证插入有效
			bool successful;
			uint dupIndex;
			session->startTransaction(TXN_INSERT, tableId);
			EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertedRecord, &dupIndex));
			session->endTransaction(true);
			CPPUNIT_ASSERT(successful);

			totalKeys++;

			freeRecord(insertedRecord);
		}

		// 检查索引
		checkIndex(session, memoryContext, totalKeys);
		index = m_index->getIndex(1);
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
		idxKey->m_columns = m_tblDef->m_indice[1]->m_columns;
		idxKey->m_numCols = m_tblDef->m_indice[1]->m_numCols;
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);


		// Part2:在刚才建立插入的基础上，将索引删空，每删一项都进行undo操作
		manager->setTxnType(TXN_DELETE);
		Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
//		SubRecord *findrecord = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *findrecord = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(1), KEY_PAD);
		findrecord->m_columns = m_tblDef->m_indice[1]->m_columns;
		findrecord->m_numCols = m_tblDef->m_indice[1]->m_numCols;

		IndexDef *indexDef = m_tblDef->m_indice[1];
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			m_tblDef->m_indice[1]->m_numCols, m_tblDef->m_indice[1]->m_columns, findrecord->m_numCols, findrecord->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		for (uint i = HOLE; i < HOLE + origSize * 20; i++) {
			SubRecord *findKey = makeFindKeyWhole(i, m_heap, m_tblDef);
			findKey->m_rowId = INVALID_ROW_ID;
			RowId rowId;
			bool found = index->getByUniqueKey(session, findKey, None, &rowId, findrecord, NULL, extractor);
			if (found) {	// 该项存在，进行测试
				record->m_format = REC_FIXLEN;
				CPPUNIT_ASSERT(m_heap->getRecord(session, rowId, record));
				record->m_format = REC_REDUNDANT;
				session->startTransaction(TXN_DELETE, tableId);
				startLsn = session->getLastLsn();
				m_index->deleteIndexEntries(session, record, NULL);
				session->endTransaction(true);

				u64 curLsn = session->getLastLsn();
				CPPUNIT_ASSERT(curLsn > startLsn);
				// undo刚才的插入，验证undo正确性
				manager->undoFromSpecifiedLogs(session, startLsn, curLsn);

				// 检查undo的结果保证正确
				checkIndex(session, memoryContext, totalKeys);

				session->startTransaction(TXN_DELETE, tableId);
				m_index->deleteIndexEntries(session, record, NULL);
				session->endTransaction(true);
				totalKeys--;

				checkIndex(session, memoryContext, totalKeys);
				index = m_index->getIndex(1);
				CPPUNIT_ASSERT(rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
				CPPUNIT_ASSERT(rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
			}

			freeSubRecord(findKey);
		}

		CPPUNIT_ASSERT(totalKeys == 0);

		// 检查索引被清空
		checkIndex(session, memoryContext, totalKeys);
		index = m_index->getIndex(1);
		rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
		rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

		m_index->close(m_session, true);
		delete m_index;
		DrsIndice::drop(m_path);
		m_index = NULL;
		closeSmallHeap(m_heap);
		DrsHeap::drop(HEAP_NAME);
		m_heap = NULL;
		delete m_tblDef;
		m_tblDef = NULL;
		TableDef::drop(TBLDEF_NAME);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

	if (manager != NULL)
		delete manager;
}


/*
 * 测试DML操作的恢复的redoCPST相关操作
 * case1:	改用例主要测试redo补偿日志的操作
 *			流程和redoUndo非常相似，只是在undo完日志的基础上，在redo一遍undo的日志以及undo过程产生的补偿日志
 *			此时实际上还是将DML修改操作undo掉了，然后重做一遍真正有效的DML操作，其它一样
 *
 * 注意，该测试用例为了提高性能，对堆的操作，不进行恢复备份。因此基本上堆和索引都是不一致的
 */
void IndexRecoveryTestCase::testDMLRecoveryRedoCPST() {
	uint testScale = DATA_SCALE / 4;

	// 测试undo之后的redo补偿日志操作
	// 过程和undo测试很类似，只是做完undo之后还需要从操作原始状态redo所有正常日志和补偿日志
	// 该测试应该是用大堆，提高测试覆盖率
	uint origSize = 10;
	uint totalKeys, items;
	uint step = 2;
	m_tblDef = createBigTableDef();
	m_heap = createBigHeap(m_tblDef);
	items = totalKeys = buildBigHeap(m_heap, m_tblDef, false, origSize * 5, step);
	File oldindexfile(m_path);
	oldindexfile.remove();
	DrsIndice::create(m_path, m_tblDef);

	u16 tableId = m_tblDef->m_id;

	createIndex(m_path, m_heap, m_tblDef);

	u64 startLsn = m_db->getTxnlog()->tailLsn();;

	DrsIndex *index;

	IndexRecoveryManager *manager = NULL;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	for (uint i = HOLE + 1; i < HOLE + origSize * 3; i += step) {
		// 关闭数据库备份数据文件和索引文件，然后重新打开
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		closeEnv();

		// 备份当前状态数据和索引文件
		//backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
		backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);

		openEnv();

		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
		memoryContext = session->getMemoryContext();

		manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
		manager->setTxnType(TXN_INSERT);

		// 插入一条新纪录
		u64 startLsn = m_db->getTxnlog()->tailLsn();
		Record *insertedRecord = insertSpecifiedKey(session, memoryContext, i, true);
		freeRecord(insertedRecord);
		u64 curLsn = session->getLastLsn();
		CPPUNIT_ASSERT(curLsn > startLsn);
		// undo刚才的插入，验证undo正确性
		manager->undoFromSpecifiedLogs(session, startLsn, curLsn);
		delete manager;
		manager = NULL;

		// 检查undo的结果保证正确
		checkIndex(session, memoryContext, totalKeys);

		// 此时得到补偿日志，将索引状态恢复到本次插入之前，redo原插入操作日志以及补偿日志
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		closeEnv();

		// 还原文件到插入结束时的索引状态，读取日志内容，重做之前的删除日志
		//myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
		myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

		openEnv();

		conn = m_db->getConnection(false);
		session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
		memoryContext = session->getMemoryContext();

		// 重做前面所有DML日志
		manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
		manager->setTxnType(TXN_INSERT);
		manager->redoFromSpecifiedLogs(session, startLsn);
		delete manager;
		manager = NULL;

		// 检查redo的结果保证正确
		checkIndex(session, memoryContext, totalKeys);

		// 再重做一次刚才的插入，保证插入有效
		Record *temp = insertSpecifiedKey(session, memoryContext, i, true);
		freeRecord(temp);

		totalKeys++;
	}

	// 检查索引
	checkIndex(session, memoryContext, totalKeys);
	index = m_index->getIndex(0);
//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
	idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
	idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// Part2:在刚才建立插入的基础上，将索引删空，每删一项都进行undo操作
	SubRecord *findrecord;
	for (uint i = HOLE; i < HOLE + origSize * 5; i++) {
		SubRecord *findKey = makeFindKeyWhole(i, m_heap, m_tblDef, true);
		findKey->m_rowId = INVALID_ROW_ID;
		RowId rowId;
//		findrecord = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		findrecord = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
		findrecord->m_columns = m_tblDef->m_indice[0]->m_columns;
		findrecord->m_numCols = m_tblDef->m_indice[0]->m_numCols;

		IndexDef *indexDef = m_tblDef->m_indice[0];
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, findrecord->m_numCols, findrecord->m_columns, KEY_COMPRESS, KEY_PAD, 1);

		bool found = index->getByUniqueKey(session, findKey, None, &rowId, findrecord, NULL, extractor);
		if (found) {	// 该项存在，进行测试
			// 关闭数据库备份数据文件和索引文件，然后重新打开
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			closeEnv();

			// 备份当前状态数据和索引文件
			//backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
			backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);

			openEnv();

			conn = m_db->getConnection(false);
			session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
			memoryContext = session->getMemoryContext();

			Record *record = makeRecordForIAndD(i, true, false);
			record->m_rowId = rowId;
			// 执行一次删除操作
			startLsn = m_db->getTxnlog()->tailLsn();;
			session->startTransaction(TXN_DELETE, tableId);
			m_index->deleteIndexEntries(session, record, NULL);
			session->endTransaction(true);

			u64 curLsn = session->getLastLsn();
			CPPUNIT_ASSERT(curLsn > startLsn);
			// undo刚才的插入，验证undo正确性
			manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
			manager->setTxnType(TXN_DELETE);
			manager->undoFromSpecifiedLogs(session, startLsn, curLsn);
			delete manager;
			manager = NULL;

			// 检查undo的结果保证正确
			checkIndex(session, memoryContext, totalKeys);

			// 此时得到补偿日志，将索引状态恢复到本次插入之前，redo原插入操作日志以及补偿日志
			m_db->getSessionManager()->freeSession(session);
			m_db->freeConnection(conn);
			closeEnv();

			// 还原文件到插入结束时的索引状态，读取日志内容，重做之前的删除日志
			//myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
			myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);

			openEnv();

			conn = m_db->getConnection(false);
			session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecoveryRedoCPST", conn);
			memoryContext = session->getMemoryContext();

			// 重做前面所有DML日志
			manager = new IndexRecoveryManager(m_db, m_heap, m_tblDef, m_index);
			manager->setTxnType(TXN_DELETE);
			manager->redoFromSpecifiedLogs(session, startLsn);
			delete manager;
			manager = NULL;

			// 检查redo的结果保证正确
			checkIndex(session, memoryContext, totalKeys);

			session->startTransaction(TXN_DELETE, tableId);
			m_index->deleteIndexEntries(session, record, NULL);
			session->endTransaction(true);
			freeRecord(record);
			totalKeys--;

			checkIndex(session, memoryContext, totalKeys);
			index = m_index->getIndex(0);
//			idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
			idxKey = IndexKey::allocSubRecord(memoryContext, m_tblDef->getIndexDef(0), KEY_PAD);
			idxKey->m_columns = m_tblDef->m_indice[0]->m_columns;
			idxKey->m_numCols = m_tblDef->m_indice[0]->m_numCols;
			CPPUNIT_ASSERT(rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
			CPPUNIT_ASSERT(rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys));
		}
		freeSubRecord(findKey);
	}

	CPPUNIT_ASSERT(totalKeys == 0);

	// 检查索引被清空
	checkIndex(session, memoryContext, totalKeys);
	index = m_index->getIndex(0);
	rangeScanBackwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);
	rangeScanForwardCheck(session, m_tblDef, index, NULL, idxKey, totalKeys);

	// 结束redo测试，关闭环境
	m_index->close(m_session, true);
	delete m_index;
	DrsIndice::drop(m_path);
	m_index = NULL;
	closeSmallHeap(m_heap);
	DrsHeap::drop(HEAP_NAME);
	m_heap = NULL;
	delete m_tblDef;
	m_tblDef = NULL;
	TableDef::drop(TBLDEF_NAME);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 检查两个创建索引的结构正确性
 */
void IndexRecoveryTestCase::checkIndex(Session *session, MemoryContext *memoryContext, uint keys) {
	if (isEssentialOnly())
		return;

	SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *pkey0 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[0], KEY_PAD);

	DrsIndex *index = m_index->getIndex(0);
	CPPUNIT_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	if (m_index->getIndexNum() <= 1)
		return;

	SubRecord *key3 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *key4 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_COMPRESS);
	SubRecord *pkey1 = IndexKey::allocSubRecord(memoryContext, m_tblDef->m_indice[1], KEY_PAD);

	index = m_index->getIndex(1);
	CPPUNIT_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key3, key4, pkey1, true));
}


/**
 * 将指定的记录插入堆和索引
 */
Record* IndexRecoveryTestCase::insertSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i, bool testBig, bool testPrefix) {
	char name[LONG_KEY_LENGTH];
	RowId rid;
	if (testBig) {
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
	} else
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
	Record *insertrecord = createRecord(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	EXCPT_OPER(rid = m_heap->insert(m_session, insertrecord, NULL));
	freeRecord(insertrecord);
	insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	insertrecord->m_rowId = rid;

	bool successful;
	uint dupIndex;
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	session->endTransaction(true);
	CPPUNIT_ASSERT(successful);

	return insertrecord;
}

/**
 * 将指定的记录插入索引
 */
Record* IndexRecoveryTestCase::insertSpecifiedKey(Session *session, MemoryContext *memoryContext, uint i, bool testBig, bool testPrefix) {
	Record *insertrecord = makeRecordForIAndD(i, testBig, testPrefix);
	insertrecord->m_rowId = i;

	bool successful;
	uint dupIndex;
	session->startTransaction(TXN_INSERT, m_tblDef->m_id);
	EXCPT_OPER(successful = m_index->insertIndexEntries(session, insertrecord, &dupIndex));
	session->endTransaction(true);
	CPPUNIT_ASSERT(successful);

	return insertrecord;
}



Record* IndexRecoveryTestCase::makeRecordForIAndD( uint i, bool testBig /*= false*/, bool testPrefix /*= false*/ ) {
	char name[LONG_KEY_LENGTH];
	if (testBig) {
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
	} else
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
	Record *insertrecord = createRecordRedundant(m_tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
	return insertrecord;
}


/**
 * 给定一个会话和起始日志LSN
 * 从起始日志LSN开始redo所有索引相关的日志直到日志文件结尾
 */
void IndexRecoveryManager::redoFromSpecifiedLogs(Session *session, u64 startLsn) {
	uint count = 0;
	// 根据记录的LSN，读取日志，重做创建索引操作
	Txnlog *txnLog = m_db->getTxnlog();
	LogScanHandle *logHdl = txnLog->beginScan(startLsn, Txnlog::MAX_LSN);
	while (txnLog->getNext(logHdl)) {
		LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
		//redo该日志
		redoALog(session, logEntry, logHdl->curLsn());
		count++;
	}
	txnLog->endScan(logHdl);
}


/**
 * 给定一个会话和起始、结束LSN
 * 在起始到结束LSN的范围内，undo所有索引相关的日志
 */
void IndexRecoveryManager::undoFromSpecifiedLogs(Session *session, u64 startLsn, u64 targetLsn) {
	LogEntry logs[1000];	// 假设最多1000条undo日志
	u64 lsns[1000];
	uint count = 0;
	// 首先保证要undo的日志被回刷到磁盘
	Txnlog *txnLog = m_db->getTxnlog();
	txnLog->flush(targetLsn);
	// 首先读取之前操作的所有日志
	LogScanHandle *logHdl = txnLog->beginScan(startLsn, Txnlog::MAX_LSN);
	while (txnLog->getNext(logHdl)) {
		LogEntry *logEntry = (LogEntry*)logHdl->logEntry();
		lsns[count] = logHdl->curLsn();
		// 备份日志内容
		memcpy(&logs[count], (byte*)logEntry, sizeof(LogEntry));
		logs[count].m_data = new byte[logEntry->m_size];
		memcpy(logs[count].m_data, logEntry->m_data, logEntry->m_size);
		count++;
	}
	txnLog->endScan(logHdl);

	uint totalLogs = count;
	// 倒着undo日志，回退刚才的插入
	while (count > 0) {
		count--;
		LogEntry *logEntry = &logs[count];
		undoALog(session, logEntry, lsns[count]);
	}

	// 释放日志内容空间
	for (uint i = 0; i < totalLogs; i++) {
		delete[] logs[i].m_data;
	}
}


/**
 * 重做一条指定的日志，如果是索引相关的话
 * 具体的处理和相关的日志类型可以参见代码当中的case判断
 */
void IndexRecoveryManager::redoALog(Session *session, LogEntry *logEntry, u64 lsn) {
	LogType type = logEntry->m_logType;
	byte *log = logEntry->m_data;
	uint size = (uint)logEntry->m_size;
	switch (type) {
		case LOG_IDX_DROP_INDEX:	/** 丢弃指定索引 */
			m_indice->redoDropIndex(session, lsn, log, size);
			break;
		case LOG_IDX_DML:			/** 索引键值插入、删除和Append添加操作 */
			m_indice->redoDML(session, lsn, log, size);
			break;
		case LOG_IDX_SMO:			/** 索引SMO操作 */
			m_indice->redoSMO(session, lsn, log, size);
			break;
		case LOG_IDX_SET_PAGE:		/** 索引页面修改操作 */
			m_indice->redoPageSet(session, lsn, log, size);
			break;
		case LOG_IDX_DML_CPST:		/** 与哦因键值插入、删除和Append操作的补偿日志 */
			m_indice->redoCpstDML(session, lsn, log, size);
			break;
		case LOG_IDX_SMO_CPST:		/** 索引SMO操作补偿日志 */
			m_indice->redoCpstSMO(session, lsn, log, size);
			break;
		case LOG_IDX_SET_PAGE_CPST:	/** 索引页面修改操作补偿日志 */
			m_indice->redoCpstPageSet(session, lsn, log, size);
			break;
		case LOG_IDX_ADD_INDEX_CPST:	/** 创建索引补偿日志 */
			m_indice->redoCpstCreateIndex(session, lsn, log, size);
			break;
		case LOG_IDX_CREATE_BEGIN:		/** 开始索引创建 */
			break;
		case LOG_IDX_CREATE_END:		/** 结束索引创建 */
			m_indice->redoCreateIndexEnd(log, size);
			break;
		case LOG_IDX_DML_BEGIN:		/** 开始索引DML修改操作 */
			// 此时索引必定是完整的，可以验证完整性
			break;
		case LOG_IDX_DML_END:		/** 结束索引DML修改操作 */
			// 此时索引必定是完整的，可以验证完整性
			m_indice->isIdxDMLSucceed(log, size);
			break;
		case LOG_TXN_START:
			TxnType type;
			type = Session::parseTxnStartLog(logEntry);
			session->startTransaction(type, m_tblDef->m_id, false);
			break;
		case LOG_TXN_END:
			session->endTransaction(true, false);
			break;
		case LOG_IDX_DMLDONE_IDXNO:
			m_indice->getLastUpdatedIdxNo(log, size);
			break;
		default:
			break;
	}
}


/**
 * undo一条指定的日志，日志需要是索引相关的
 * 相关的类型和重做的流程可以参见具体代码
 */
void IndexRecoveryManager::undoALog(Session *session, LogEntry *logEntry, u64 lsn) {
	LogType type = logEntry->m_logType;
	switch (type) {
		case LOG_IDX_DROP_INDEX:	/** 丢弃指定索引 */
			break;
		case LOG_IDX_DML:			/** 索引键值插入、删除和Append添加操作 */
			m_indice->undoDML(session, lsn, logEntry->m_data, (uint)logEntry->m_size, true);
			break;
		case LOG_IDX_SMO:			/** 索引SMO操作 */
			m_indice->undoSMO(session, lsn, logEntry->m_data, (uint)logEntry->m_size, true);
			break;
		case LOG_IDX_SET_PAGE:		/** 索引页面修改操作 */
			m_indice->undoPageSet(session, lsn, logEntry->m_data, (uint)logEntry->m_size, true);
			break;
		case LOG_IDX_DML_CPST:		/** 与索引键值插入、删除和Append操作的补偿日志 */
			break;
		case LOG_IDX_SMO_CPST:		/** 索引SMO操作补偿日志 */
			break;
		case LOG_IDX_SET_PAGE_CPST:	/** 索引页面修改操作补偿日志 */
			break;
		case LOG_IDX_ADD_INDEX_CPST:	/** 创建索引补偿日志 */
			break;
		case LOG_IDX_CREATE_BEGIN:		/** 开始索引创建 */
			break;
		case LOG_IDX_CREATE_END:		/** 结束索引创建 */
			break;
		case LOG_IDX_DML_BEGIN:		/** 开始索引DML修改操作 */
			break;
		case LOG_IDX_DML_END:		/** 结束索引DML修改操作 */
			break;

			// undo过程开始和提交事务顺序应该是颠倒的
		case LOG_TXN_START:
			session->endTransaction(true, false);
			break;
		case LOG_TXN_END:
			session->startTransaction(m_txnType, m_tblDef->m_id, false);
			break;
		default:
			break;
	}

}


/**
 * 关闭当前的测试环境，关闭堆和索引文件、关闭数据库。不删除任何资源
 * 只刷出日志，不刷数据
 */
void IndexRecoveryTestCase::closeEnv() {
	m_heap->close(m_session, true);
	delete m_heap;
	m_heap = NULL;
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		m_index = NULL;
	}
	if (m_tblDef != NULL) {
		delete m_tblDef;
		m_tblDef = NULL;
	}
	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
}


/**
 * 打开当前的测试环境
 * 打开数据库、堆文件和索引文件
 */
void IndexRecoveryTestCase::openEnv() {
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	m_session = m_db->getSessionManager()->allocSession("FLRHeapTestCase::testRedoInsert", m_conn);
	EXCPT_OPER(m_tblDef = TableDef::open(TBLDEF_NAME));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, m_session, HEAP_NAME, m_tblDef));
	m_index = DrsIndice::open(m_db, m_session, INDEX_NAME, m_tblDef, NULL);
}



static bool deleteUnique(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowId) {
	MemoryContext *memoryContext = session->getMemoryContext();

	uint key = recordid;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *record = createRecordRedundant(tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
	record->m_rowId = rowId;
	session->startTransaction(TXN_DELETE, tblDef->m_id);
	EXCPT_OPER(indice->deleteIndexEntries(session, record, NULL));
	session->endTransaction(true);
	CPPUNIT_ASSERT(rowId != INVALID_ROW_ID);
	heap->del(session, rowId);
	freeRecord(record);

	return true;
}

static uint deleteRange(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, bool deleteAll = false) {
	MemoryContext *memoryContext = session->getMemoryContext();

	Record *record = IndexKey::allocRecord(memoryContext, tblDef->m_maxRecSize);

	IndexScanHandle *indexHandle;
//	SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, tblDef->m_maxRecSize);
	SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, tblDef->getIndexDef(1), KEY_PAD);
	idxKey->m_columns = tblDef->m_indice[1]->m_columns;
	idxKey->m_numCols = tblDef->m_indice[1]->m_numCols;
	IndexDef *indexDef = tblDef->m_indice[1];

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tblDef, indexDef,
		indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

	u64 savePoint = memoryContext->setSavepoint();
	RowLockHandle *rlh;
	//	范围删除连续两条记录
	DrsIndex *index = indice->getIndex(1);
	SubRecord *findKey = makeFindKeyWhole(recordid, heap, tblDef);
	findKey->m_rowId = INVALID_ROW_ID;
	indexHandle = index->beginScan(session, findKey, true, true, Exclusived, &rlh, extractor);
	uint count = 0;
	while (index->getNext(indexHandle, idxKey)) {
		// 去对应Heap取记录
		record->m_format = REC_FIXLEN;
		heap->getRecord(session, idxKey->m_rowId, record);
		record->m_format = REC_REDUNDANT;
		session->startTransaction(TXN_DELETE, tblDef->m_id);
		indice->deleteIndexEntries(session, record, indexHandle);
		session->endTransaction(true);
		heap->del(session, idxKey->m_rowId);
		session->unlockRow(&rlh);
		count++;
		if (!deleteAll && count == 2)
			break;
	}
	index->endScan(indexHandle);
	freeSubRecord(findKey);

	return count;
}

static bool findUnique(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowId, RowLockHandle **rowLockHandle) {
	MemoryContext *memoryContext = session->getMemoryContext();

	SubRecord *findKey = makeFindKeyWhole(recordid, heap, tblDef);
	findKey->m_rowId = rowId;
	DrsIndex *index = indice->getIndex(1);
	RowId gotrowId;
	bool found;
	if (rowLockHandle == NULL)
		found = index->getByUniqueKey(session, findKey, None, &gotrowId, NULL, NULL, NULL);
	else
		found = index->getByUniqueKey(session, findKey, Exclusived, &gotrowId, NULL, rowLockHandle, NULL);

	if (found && rowLockHandle != NULL)
		session->unlockRow(rowLockHandle);
	freeSubRecord(findKey);

	return found;
}

static uint findRange(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowId, bool upsidedown, RowLockHandle **rowLockHandle = NULL, u64 rangeSize = (u64)-1) {
	MemoryContext *memoryContext = session->getMemoryContext();

	SubRecord *findKey = makeFindKeyWhole(recordid, heap, tblDef);
	findKey->m_rowId = rowId;
	bool forward = !upsidedown;

	DrsIndex *index = indice->getIndex(1);
	IndexScanHandle *scanHandle = index->beginScan(session, findKey, forward, true, (rowLockHandle == NULL ? None : Shared), rowLockHandle, NULL);
	uint count = 0;
	while (index->getNext(scanHandle, NULL)) {
		count++;
		if (rowLockHandle != NULL)
			session->unlockRow(rowLockHandle);
		if (count >= rangeSize)
			break;
	}
	index->endScan(scanHandle);
	freeSubRecord(findKey);

	return count;
}

static void insert(Database *db, Session *session, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, uint rangeSize) {
	if (rangeSize == 0)
		rangeSize++;

	MemoryContext *memoryContext = session->getMemoryContext();

	for (uint i = 0; i < rangeSize; i++) {
		uint key = i + recordid;
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", key);
		Record *insertrecord = createRecord(tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		EXCPT_OPER(rid = heap->insert(session, insertrecord, NULL));
		freeRecord(insertrecord);
		insertrecord = createRecordRedundant(tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		insertrecord->m_rowId = rid;

		bool successful;
		uint dupIndex;
		session->startTransaction(TXN_INSERT, tblDef->m_id);
		EXCPT_OPER(successful = indice->insertIndexEntries(session, insertrecord, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(successful);
		freeRecord(insertrecord);
	}
}




/**
 * 捣蛋鬼捣蛋过程全解析
 * 在指定位置删除指定的一条记录
 */
void TroubleMaker::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	uint key = m_recordid;
	char name[50];
	sprintf(name, "Kenneth Tse Jr. %d \0", key);
	Record *record = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
	record->m_rowId = m_rowId;
	session->startTransaction(TXN_DELETE, m_tblDef->m_id);
	EXCPT_OPER(m_indice->deleteIndexEntries(session, record, NULL));
	session->endTransaction(true);

	freeRecord(record);

	CPPUNIT_ASSERT(m_rowId != INVALID_ROW_ID);
	m_heap->del(session, m_rowId);

	SYNCHERE(SP_IDX_FINISH_A_DELETE);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 简洁主义者洁癖细节大曝光
 * 根据指定的删除方式，选择是范围删除还是唯一删除指定的索引记录，范围删除会连续删除两条索引数据，根据指定锁内容决定加不加锁
 * 唯一删除必定不加索
 */
void SimpleMan::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("SimpleMan::run", conn);
	if (m_captious) {	// 范围删除
		deleteRange(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_deleteAll);
	} else {	// 逐个删除
		deleteUnique(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rowId);
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 勤劳的蜜蜂线程，根据指定的范围大小，插入若干数据到索引
 */
void Bee::run() {
	if (m_rangeSize == 0)
		m_rangeSize++;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	for (uint i = 0; i < m_rangeSize; i++) {
		uint key = i + m_recordid;
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", key + 1);	// 这里构造的插入键值和其他地方不一样，多加1
		Record *insertrecord = createRecord(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		EXCPT_OPER(rid = m_heap->insert(session, insertrecord, NULL));
		freeRecord(insertrecord);
		insertrecord = createRecordRedundant(m_tblDef, 0, key, name, key + ((u64)key << 32), (u32)(-1) - key);
		insertrecord->m_rowId = rid;

		bool successful;
		uint dupIndex;
		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		EXCPT_OPER(successful = m_indice->insertIndexEntries(session, insertrecord, &dupIndex));
		session->endTransaction(true);
		CPPUNIT_ASSERT(successful);
		freeRecord(insertrecord);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 勤劳的变异的蜜蜂线程，根据指定的范围大小，插入若干数据到索引
 */
void VariantBee::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("VariantBee::run", conn);
	insert(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rangeSize);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}



/**
 * 疯狂的书痴
 * 根据指定的方式执行索引扫描，范围扫描和唯一性查询
 * 范围查询不加索，唯一性查询根据指定条件加锁
 * 必须在m_readNum记录查找到的记录数，便于调用者统计
 */
void ReadingLover::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("ReadingLover::run", conn);
	if (m_crazy) {	// 做范围正向扫描
		m_readNum = findRange(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rowId, m_upsidedown, m_rowLockHandle, m_rangeSize);
	} else {	// 唯一查询
		bool found = findUnique(m_db, session, m_heap, m_tblDef, m_indice, m_recordid, m_rowId, m_rowLockHandle);
		m_readNum = found ? 1 : 0;
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 这可是个大工程
 * 根据指定的堆，创建两个相关索引，为了安全正确起见，最好开事务执行
 */
void BigProject::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexRecoveryTestCase::testDMLRecovery", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	// 需要开启事务做索引创建操作
	session->startTransaction(TXN_ADD_INDEX, m_tblDef->m_id);

	// 建立不允许重复的索引，会造成最后全部回退。
	try {	// 创建带有唯一性约束的索引，最后必须会丢弃索引
		m_indice->createIndexPhaseOne(session, m_tblDef->m_indice[0], m_tblDef, m_heap);
		m_indice->createIndexPhaseTwo(m_tblDef->m_indice[0]);
		m_indice->createIndexPhaseOne(session, m_tblDef->m_indice[1], m_tblDef, m_heap);
		m_indice->createIndexPhaseTwo(m_tblDef->m_indice[1]);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
	}

	session->endTransaction(true);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 三心二意者的花心生活
 * 根据指定重复次数，重复执行若干次DML操作
 * 每次DML操作都会创建一个新线程执行
 * 为了模拟真实操作环境，这个人还是会略带喜爱阅读倾向，做查询的机会会相对较大
 */
void HalfHeartedMan::run() {
	Connection *conn = NULL;
	Session *session = NULL;
	for (u64 i = 0; i < m_times; i++) {
		// 产生随机数，决定DML类型，大部分应该是查询操作
		if (i % 100 == 0) {	// 打开一个新的连接
			if (conn != NULL) {
				if (session != NULL)
					m_db->getSessionManager()->freeSession(session);
				m_db->freeConnection(conn);
			}
			conn = m_db->getConnection(false);
			if (conn == NULL)
				continue;
			session = m_db->getSessionManager()->allocSession("HalfHeartedMan::run", conn);
		}
		uint type = rand() % 6;
		if (type <= 1) {	// 插入，呼唤勤劳的蜜蜂帮忙
			// 为了简单并且能提高测试并发性，基本上只执行单个插入
			uint insertNo = rand() % MT_TEST_DATA_SCALE;
			insert(m_db, session, m_heap, m_tblDef, m_indice, insertNo, 1);
			cout << this->getId() << " " << insertNo << " inserted" << endl;
		}

		if (type == 2) {	// 删除，必须容忍简洁主义者的洁癖
			uint deleteNo = rand() % MT_TEST_DATA_SCALE;	// 删除的起始数据
			deleteRange(m_db, session, m_heap, m_tblDef, m_indice, deleteNo);
			cout << this->getId() << " done a range delete from " << deleteNo << endl;
		}

		if (type > 2) {	// 查询，reading is my favorite thing
			uint readNo = rand() & MT_TEST_DATA_SCALE;
			bool forward;
			forward = (rand() % 2 == 0);
			findRange(m_db, session, m_heap, m_tblDef, m_indice, readNo, INVALID_ROW_ID, !forward);
			cout << this->getId() << " done a range scan from " << readNo << endl;
		}

		Thread::msleep(100);
	}
}

/************************************************************************/
/* 测试大规模的SMO操作对索引结构正确性的验证                            */
/************************************************************************/

void IndexSMOTestCase::setUp() {
	m_heap = NULL;
	m_index = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	char name[255] = "testIndex.nti";
	memcpy(m_path, name, sizeof(name));
	srand((unsigned)time(NULL));
	clearRSFile(&m_cfg);
	ts.idx = true;
	vs.idx = true;
}

void IndexSMOTestCase::tearDown() {
	// 丢索引
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexSMOTestCase::tearDown", conn);

	if (m_index != NULL) {
		m_index->close(session, true);
		delete m_index;
		DrsIndice::drop(m_path);
	}
	// 丢表
	if (m_heap != NULL) {
		EXCPT_OPER(m_heap->close(session, true));
		delete m_heap;
		DrsHeap::drop(HEAP_NAME);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close();
	delete m_db;
	Database::drop(".");
	m_db = NULL;

	ts.idx = false;
	vs.idx = false;
}

/**
 * 测试单线程多更新操作，导致索引疯狂的SMO
 */
void IndexSMOTestCase::testLotsUpdates() {
	uint userNum = 200000;

	m_tblDef = createTableDef();
	m_heap = createHeapAndLoadData(m_db,m_tblDef, userNum);
	m_index = createIndex(m_db, m_path, m_heap, m_tblDef);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexSMOTestCase::testLotsUpdates", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	DrsIndex *index = m_index->getIndex(0);
	IndexDef *indexDef = m_tblDef->m_indice[0];

	Record *record = IndexKey::allocRecord(memoryContext, m_tblDef->m_maxRecSize);
	record->m_format = REC_FIXLEN;
	u64 count = 0;
	while (true) {
		count++;
		u64 savePoint = memoryContext->setSavepoint();

		// 随机抽取一个ID值，做索引扫描，将它的AccessCount加1保存
		uint idplus = rand() % userNum;
		u64 userId = USER_MIN_ID + idplus;

		// 开始一个索引扫描，对每一条取得的记录，修改堆和索引
		SubRecord *findKey = makeFindKeyByUserId(m_tblDef, userId);
//		SubRecord *idxKey = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		SubRecord *idxKey = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_PAD);
		idxKey->m_columns = indexDef->m_columns;
		idxKey->m_numCols = indexDef->m_numCols;

		SubRecord *updateSub = IndexKey::allocSubRecordRED(memoryContext, m_tblDef->m_maxRecSize);
		u16 colno = 2;
		updateSub->m_columns = &colno;
		updateSub->m_numCols = 1;

		SubToSubExtractor *extractor = SubToSubExtractor::createInst(memoryContext, m_tblDef, indexDef,
			indexDef->m_numCols, indexDef->m_columns, idxKey->m_numCols, idxKey->m_columns, KEY_COMPRESS, KEY_PAD, 1000);

		RowLockHandle *rlh;

		IndexScanHandle *indexHandle = index->beginScan(session, findKey, true, true, Exclusived, &rlh, extractor);

		std::vector<RowId> rowIds;

		while (index->getNext(indexHandle, idxKey)) {
			session->unlockRow(&rlh);

			u64 keyUserId = RedRecord::readBigInt(m_tblDef, idxKey->m_data, 1);
			if (keyUserId != userId)
				break;

			rowIds.push_back(idxKey->m_rowId);
		}

		index->endScan(indexHandle);

		for (uint i = 0; i < rowIds.size(); i++) {
			// 取得对应堆的记录
			record->m_format = REC_FIXLEN;
			m_heap->getRecord(session, rowIds[i], record);
			record->m_format = REC_REDUNDANT;

			memcpy(idxKey->m_data, record->m_data, record->m_size);
			idxKey->m_rowId = rowIds[i];
			updateSub->m_rowId = rowIds[i];

			NTSE_ASSERT(RedRecord::readBigInt(m_tblDef, idxKey->m_data, 1) == userId);

			uint accessCount = RedRecord::readInt(m_tblDef, record->m_data, colno);
			RedRecord::writeNumber(m_tblDef, colno, updateSub->m_data, accessCount + 1);

			// 执行更新操作，首先堆扫描得到扫描句柄，然后更新堆
			NTSE_ASSERT(m_heap->update(session, rowIds[i], updateSub));
			uint dupIndex;
			NTSE_ASSERT(m_index->updateIndexEntries(session, idxKey, updateSub, false, &dupIndex));
		}

		memoryContext->resetToSavepoint(savePoint);
		freeSubRecord(findKey);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

TableDef* IndexSMOTestCase::createTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tblDef;

	// 创建堆
	builder = new TableDefBuilder(1, "Photo", "UserActive");
	builder->addColumn("ID", CT_BIGINT, false)->addColumn("UserID", CT_BIGINT, false);
	builder->addColumn("AccessCount", CT_INT, false)->addColumnS("UserName", CT_CHAR, 100, false, false);
	builder->addIndex("Active", false, false, false, "UserID", 0, "AccessCount", 0, "UserName", 0, NULL);
	tblDef = builder->getTableDef();
	delete builder;
	EXCPT_OPER(tblDef->writeFile(TBLDEF_NAME));

	return tblDef;
}

DrsHeap* IndexSMOTestCase::createHeapAndLoadData(Database *db, const TableDef *tblDef, uint size) {
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();
	DrsHeap *heap = NULL;
	
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("IndexSMOTestCase::createHeapAndLoadData", conn);

	EXCPT_OPER(DrsHeap::create(db, tableName, tblDef));
	EXCPT_OPER(heap = DrsHeap::open(db, session, tableName, tblDef));

	// 装载数据
	const char userName[100] = "Mr. all rights, but now he is not good. Some bugs are bothering him. ";
	uint duplicatePerUser = 100;	// 每个用户40个信息
	u64 totalIds = 0;
	for (uint i = 0; i < size; i++) {
		for (uint j = 0; j < duplicatePerUser; j++) {
			RecordBuilder rb(tblDef, -1, REC_FIXLEN);
			rb.appendBigInt(totalIds++);
			rb.appendBigInt(USER_MIN_ID + i);
			rb.appendInt(0);
			rb.appendChar(userName);
			Record *record = rb.getRecord(tblDef->m_maxRecSize);
			EXCPT_OPER(heap->insert(session, record, NULL));
			freeRecord(record);
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	return heap;
}

DrsIndice* IndexSMOTestCase::createIndex( Database *db, char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException) {
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("IndexSMOTestCase::createHeapAndLoadData", conn);

	File oldindexfile(path);
	oldindexfile.remove();

	DrsIndice::create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = DrsIndice::open(db, session, path, tblDef, NULL);
	try {
		for (uint i = 0; i < tblDef->m_numIndice; i++) {
			m_index->createIndexPhaseOne(session, tblDef->m_indice[i], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	return m_index;
}

/**
 * 插入键值比当前页面中所有键值都大时，测试SMO正确性
 * TODO: 用例不够稳定，无法当页面大小发生变化
 *		改进方法是在用例外部套一个循环， 尝试多次，每次是不同的记录
 */
void IndexSMOTestCase::testInsertMaxKeySMO() {
	
	const int INDEX_PAGE_SIZE = (IndexPage::INDEXPAGE_INIT_MAX_USAGE - sizeof(IndexPage));
	const int KEY_SIZE =  INDEX_PAGE_SIZE / 20;	// 普通键值长度
	const int NAME_SIZE = KEY_SIZE - 8;				// NAME字段长度
	const int PROFILE_SIZE = 3 * KEY_SIZE;			// PROFILE字段长度
	const int FULL_KEY_SIZE = KEY_SIZE + PROFILE_SIZE;	// 长键值长度
	AutoPtr<TableDefBuilder> builder(new TableDefBuilder(99, "INDEX_BUGS", "MaxKeySMO"));
	builder->addColumn("UserId", CT_BIGINT, false)
		->addColumnS("UserName", CT_CHAR, NAME_SIZE)
		->addColumnS("Profile", CT_CHAR, PROFILE_SIZE, false)
		->addIndex("MyIndex", false, false, false, "UserId", 0, "UserName", 0, "Profile", 0, NULL);
	AutoPtr<TableDef> tableDef(builder->getTableDef());

	int recordPerPage = INDEX_PAGE_SIZE / KEY_SIZE;
	try {
		for (int vitim = recordPerPage - 1; vitim <= recordPerPage + 1; ++vitim) {
			tearDown();
			setUp();
			File oldsmallfile(HEAP_NAME);
			oldsmallfile.remove();

			Connection *conn = m_db->getConnection(false);
			Session *s = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testInsertMaxKeySMO", conn);

			// 创建堆
			EXCPT_OPER(DrsHeap::create(m_db, HEAP_NAME, tableDef));
			DrsHeap *heap = NULL;
			EXCPT_OPER(heap = DrsHeap::open(m_db, s, HEAP_NAME, (TableDef *)tableDef));


			// 插入记录, 创建索引
			char randomString[Limits::PAGE_SIZE];
			memset(randomString, 'X', sizeof(randomString));
			randomString[sizeof(randomString) - 1] = '\0';

			for (int i = 1; i <= recordPerPage * 2; ++i) {
				RecordBuilder rb(tableDef, INVALID_ROW_ID, REC_FIXLEN);
				rb.appendBigInt( ((u64)i) << 56); // 键值不能压缩
				rb.appendChar(randomString, NAME_SIZE);
				rb.appendNull();
				Record *rec = rb.getRecord(tableDef->m_maxRecSize);
				heap->insert(s, rec, NULL);
				freeRecord(rec);
			}
			createIndex(m_db, m_path, heap, (TableDef *)tableDef);

			// 期望是删除了第1个索引页面的最后一项
			RowId rowId = INVALID_ROW_ID;
			byte findKeyBuf[Limits::PAGE_SIZE];
			SubRecord findKey(KEY_PAD, tableDef->m_indice[0]->m_numCols, tableDef->m_indice[0]->m_columns
				, findKeyBuf, tableDef->m_maxRecSize, INVALID_ROW_ID);
			RedRecord lastRecOnFirstPage(tableDef, REC_REDUNDANT);
			lastRecOnFirstPage.writeNumber(0, ((u64)vitim) << 56);
			lastRecOnFirstPage.writeChar(1, randomString, NAME_SIZE);
			lastRecOnFirstPage.setNull(2);
			RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], lastRecOnFirstPage.getRecord(), NULL, &findKey);
			CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
			lastRecOnFirstPage.setRowId(rowId);
			s->startTransaction(TXN_DELETE, tableDef->m_id);
			EXCPT_OPER(m_index->deleteIndexEntries(s, (Record *)lastRecOnFirstPage.getRecord(), NULL));
			s->endTransaction(true);
			


			// 插入大量键值一样，但是rowid不同的记录，导致分裂
			RedRecord largerKey(tableDef, REC_REDUNDANT);
			for (int i = 0; i < KEY_SIZE / 4; ++i) {
				largerKey.setRowId(i + 1); // 一个很小的rowid
				largerKey.writeNumber(0, ((u64)vitim) << 56);
				largerKey.writeChar(1, randomString, NAME_SIZE); 
				largerKey.setNull(2);
				s->startTransaction(TXN_INSERT, tableDef->m_id);
				EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
				s->endTransaction(true);

				// 根据索引查找新插入的记录，验证查找成功
				RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], largerKey.getRecord(), NULL, &findKey);
				CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
			}
			

			// 清理
			heap->close(s, false);
			DrsHeap::drop(HEAP_NAME);
			m_db->getSessionManager()->freeSession(s);
			m_db->getSessionManager()->freeConnection(conn);
		}
	} catch( NtseException &e) {
		cout << e.getMessage() << endl;
	}

}

void IndexSMOTestCase::testBugNTSETNT39() {
	const int INDEX_PAGE_SIZE = (IndexPage::INDEXPAGE_INIT_MAX_USAGE - sizeof(IndexPage));
	const int KEY_SIZE =  INDEX_PAGE_SIZE / 20;	// 普通键值长度
	const int NAME_SIZE = KEY_SIZE - 8;				// NAME字段长度
	const int PROFILE_SIZE = 8 * KEY_SIZE;			// PROFILE字段长度
	const int FULL_KEY_SIZE = KEY_SIZE + PROFILE_SIZE;	// 长键值长度
	AutoPtr<TableDefBuilder> builder(new TableDefBuilder(99, "INDEX_BUGS", "NTSETNT39"));
	builder->addColumn("UserId", CT_BIGINT, false)
		->addColumnS("UserName", CT_CHAR, NAME_SIZE)
		->addColumnS("Profile", CT_CHAR, PROFILE_SIZE, false)
		->addIndex("MyIndex", false, false, false, "UserId", 0, "UserName", 0, "Profile", 0, NULL);
	AutoPtr<TableDef> tableDef(builder->getTableDef());

	int recordPerPage = INDEX_PAGE_SIZE / KEY_SIZE;

	try {	
		tearDown();
		setUp();
		File oldsmallfile(HEAP_NAME);
		oldsmallfile.remove();

		Connection *conn = m_db->getConnection(false);
		Session *s = m_db->getSessionManager()->allocSession("IndexOperationTestCase::testBugNTSETNT39", conn);

		// 创建堆
		EXCPT_OPER(DrsHeap::create(m_db, HEAP_NAME, tableDef));
		DrsHeap *heap = NULL;
		EXCPT_OPER(heap = DrsHeap::open(m_db, s, HEAP_NAME, (TableDef *)tableDef));


		// 插入记录, 创建索引
		char randomString[Limits::PAGE_SIZE];
		memset(randomString, 'X', sizeof(randomString));
		randomString[sizeof(randomString) - 1] = '\0';

		createIndex(m_db, m_path, heap, (TableDef *)tableDef);

		RowId rowId = INVALID_ROW_ID;
		byte findKeyBuf[Limits::PAGE_SIZE];
		SubRecord findKey(KEY_PAD, tableDef->m_indice[0]->m_numCols, tableDef->m_indice[0]->m_columns
			, findKeyBuf, tableDef->m_maxRecSize, INVALID_ROW_ID);

		// 构造5层结构的索引树（二叉）
		RedRecord largerKey(tableDef, REC_REDUNDANT);
		for (int i = 1; i < 7; ++i) {
			largerKey.setRowId(10 * i ); // 一个很小的rowid
			largerKey.writeNumber(0, (u64)(10 * i ));
			largerKey.writeChar(1, randomString, NAME_SIZE); 
			//largerKey.setNull(2);
			char *randomProfile = randomStr(PROFILE_SIZE);
			largerKey.writeChar(2, randomProfile, PROFILE_SIZE);
			s->startTransaction(TXN_INSERT, tableDef->m_id);
			EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
			s->endTransaction(true);
			delete[] randomProfile;
			// 根据索引查找新插入的记录，验证查找成功
			RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], largerKey.getRecord(), NULL, &findKey);
			CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
		}

		//起线程做插入最右节点分裂操作
		IndexSMOThread smoThread(m_db, tableDef, m_index);
		smoThread.enableSyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		smoThread.start();
		smoThread.joinSyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		
		//提前插至与最右节点共同的并且PageLevel为第2的父节点并导致分裂，此处要保证插入的叶节点不能和最右叶节点相邻
		for (int i = 1; i < 4; ++i) {
			largerKey.setRowId(30 - i); 
			largerKey.writeNumber(0, (u64)(30 - i));
			largerKey.writeChar(1, randomString, NAME_SIZE); 
			//largerKey.setNull(2);
			char *randomProfile = randomStr(PROFILE_SIZE);
			largerKey.writeChar(2, randomProfile, PROFILE_SIZE);
			s->startTransaction(TXN_INSERT, tableDef->m_id);
			EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
			s->endTransaction(true);
			delete[] randomProfile;
			// 根据索引查找新插入的记录，验证查找成功
			RecordOper::extractKeyRP((TableDef *)tableDef, tableDef->m_indice[0], largerKey.getRecord(), NULL, &findKey);
			CPPUNIT_ASSERT(m_index->getIndex(0)->getByUniqueKey(s, &findKey, None, &rowId, NULL, NULL, NULL));
		}

		smoThread.disableSyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		smoThread.notifySyncPoint(SP_IDX_WAIT_TO_FIND_SPECIAL_LEVEL_PAGE);
		smoThread.join(-1);
		
		// 清理
		heap->close(s, false);
		DrsHeap::drop(HEAP_NAME);
		m_db->getSessionManager()->freeSession(s);
		m_db->getSessionManager()->freeConnection(conn);
		
	} catch( NtseException &e) {
		cout << e.getMessage() << endl;
	}
}

IndexSMOThread::IndexSMOThread(Database *db, TableDef * tableDef, DrsIndice *index):Thread("InsertSMOThread") {
	m_db = db;
	m_tableDef = tableDef;
	m_index = index;
}

void IndexSMOThread::run() {
	const int INDEX_PAGE_SIZE = (IndexPage::INDEXPAGE_INIT_MAX_USAGE - sizeof(IndexPage));
	const int KEY_SIZE =  INDEX_PAGE_SIZE / 20;	// 普通键值长度
	const int NAME_SIZE = KEY_SIZE - 8;				// NAME字段长度
	const int PROFILE_SIZE = 8 * KEY_SIZE;			// PROFILE字段长度
	const int FULL_KEY_SIZE = KEY_SIZE + PROFILE_SIZE;	// 长键值长度

	Connection *conn = m_db->getConnection(false);
	Session *s = m_db->getSessionManager()->allocSession("IndexSMOThread", conn);
	
	RedRecord largerKey(m_tableDef, REC_REDUNDANT);
	char randomString[Limits::PAGE_SIZE];
	memset(randomString, 'X', sizeof(randomString));
	randomString[sizeof(randomString) - 1] = '\0';

	//插入最右叶节点导致级联分裂至根节点
	largerKey.setRowId(70); // 一个很小的rowid
	largerKey.writeNumber(0, (u64)(70));
	largerKey.writeChar(1, randomString, NAME_SIZE); 
	//largerKey.setNull(2);
	char *randomProfile = randomStr(PROFILE_SIZE);
	largerKey.writeChar(2, randomProfile, PROFILE_SIZE);
	s->startTransaction(TXN_INSERT, m_tableDef->m_id);
	EXCPT_OPER(m_index->insertIndexEntries(s, (Record *)largerKey.getRecord(), NULL));
	s->endTransaction(true);	
	delete[] randomProfile;

	m_db->getSessionManager()->freeSession(s);
	m_db->getSessionManager()->freeConnection(conn);
}


SubRecord* IndexSMOTestCase::makeFindKeyByUserId(const TableDef *tableDef, u64 userId ) {
	SubRecordBuilder sb(tableDef, KEY_PAD);
	return sb.createSubRecordByName("UserID", &userId);
}

const char* IndexSMOTestCase::getName() {
	return "Test index SMO";
}

const char* IndexSMOTestCase::getDescription() {
	return "Test Index smo by updating index again and againd";
}

bool IndexSMOTestCase::isBig() {
	return true;
}

/**********************************************************************************************/
/** QA平台60214bug的修改回归用例
* 该用例主要处理对于更新操作过程中插入操作被死锁回退导致的删除无法一起回退的问题
*/

u64 getRealObjectId(const TableDef *tableDef, u64 objectId) {
	return ((u64)tableDef->m_id << ((sizeof(u64) - sizeof(tableDef->m_id)) * 8)) | objectId;
}

class BugsVolunteer : public Thread {
public:
	BugsVolunteer(const char *name, Database *db, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice) : Thread(name) {
		m_db = db;
		m_indice = indice;
		m_heap = heap;
		m_tblDef = tblDef;
	}

	virtual void run() {
		// 从事对某个数据进行一个更新的操作
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);

		// 构造更新前后的数据内容
		char commonPrefix[500];
		for (uint i = 0; i < 400; i++)
			commonPrefix[i] = 'a';
		commonPrefix[400] = '\0';
		const IndexDef *indexDef = ((DrsBPTreeIndex*)m_indice->getIndex(0))->getIndexDef();
		SubRecord *before, *after;
		u32 updateCount = 8;

		RowId rowId = 8192 + 1000 * updateCount;
		char userName[500];
		sprintf(userName, "%d %s", updateCount, commonPrefix);
		char newUserName[500];
		sprintf(newUserName, "%s %d %d", commonPrefix, 55, 55);

		SubRecordBuilder srbBefore(m_tblDef, REC_REDUNDANT, rowId);
		before = srbBefore.createSubRecordByName("BookName", userName);
		SubRecordBuilder srbAfter(m_tblDef, REC_REDUNDANT, rowId);
		after = srbAfter.createSubRecordByName("BookName", newUserName);

		// 执行更新操作
		session->startTransaction(TXN_UPDATE, m_tblDef->m_id);
		// 进行更新
		uint dupIndex = (uint)-1;
		NTSE_ASSERT(m_indice->updateIndexEntries(session, before, after, false, &dupIndex));
		session->endTransaction(true);

		freeSubRecord(before);
		freeSubRecord(after);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

private:
	Database *m_db;
	DrsIndice *m_indice;
	DrsHeap *m_heap;
	const TableDef *m_tblDef;
};

class BugsLockObjectThread : public Thread {
public:
	BugsLockObjectThread(Session *session, const TableDef *tableDef, u64 objectId) : Thread("BugsLockObjectThread") {
		m_session = session;
		m_objectId = objectId;
		m_tableDef = tableDef;
	}

	virtual void run() {
		NTSE_ASSERT(m_session->lockIdxObject(getRealObjectId(m_tableDef, m_objectId)));
		NTSE_ASSERT(m_session->unlockIdxObject(getRealObjectId(m_tableDef, m_objectId)));
	}

private:
	Session *m_session;
	u64 m_objectId;
	const TableDef *m_tableDef;
};


void IndexBugsTestCase::testBugQA60214() {
	prepareDataBug60214();

	/****************************  测试该bug的正常操作流程  ********************************/
	// 备份当前数据文件
	// 关闭数据库备份数据文件和索引文件，然后重新打开
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);
	m_heap->close(session, true);
	delete m_heap;
	m_indice->close(session, true);
	delete m_indice;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close(true, true);
	delete m_db;
	delete m_tblDef;
	// 备份当前状态数据和索引文件
	backupHeapFile(HEAP_NAME, HEAP_BACKUP_NAME);
	backupHeapFile(INDEX_NAME, INDEX_BACKUP_NAME);
	backupTblDefFile(TBLDEF_NAME, TBLDEF_BACKUP_NAME);
	// 打开数据库准备操作
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);
	EXCPT_OPER(m_tblDef = TableDef::open(TBLDEF_NAME));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, HEAP_NAME, m_tblDef));
	m_indice = DrsIndice::open(m_db, session, INDEX_NAME, m_tblDef, NULL);
	
	u64 txnStartLsn = (u64)-1, txnEndLsn = (u64)-1;
	txnStartLsn = m_db->getTxnlog()->tailLsn();

	// 执行一次会导致失败的更新操作
	// 备份当前页面
	byte backupRootPage[Limits::PAGE_SIZE];
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageHandle *handle = GET_PAGE(session, file, PAGE_INDEX, IndicePageManager::NON_DATA_PAGE_NUM, Exclusived, m_indice->getDBObjStats(), NULL);
	IndexPage *rootPage = (IndexPage*)handle->getPage();;
	memcpy(backupRootPage, (byte*)rootPage, Limits::PAGE_SIZE);
	session->releasePage(&handle);

	// 先加索引SMO锁，为制造死锁作准备
	NTSE_ASSERT(session->lockIdxObject(getRealObjectId(m_tblDef, (u64)((DrsBPTreeIndex*)m_indice->getIndex(0))->getIndexId())));

	// 启动更新线程，等待在更新插入的加锁阶段之前
	BugsVolunteer *volunteer = new BugsVolunteer("BugsVolunteer", m_db, m_heap, m_tblDef, m_indice);
	volunteer->enableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
	volunteer->start();
	volunteer->joinSyncPoint(SP_IDX_WAIT_FOR_INSERT);

	// 加锁制造死锁，但是本线程的加锁要成功，期待的是更新线程出现死锁
	BugsLockObjectThread *bloThread = new BugsLockObjectThread(session, m_tblDef, IndicePageManager::NON_DATA_PAGE_NUM);
	bloThread->start();
	Thread::msleep(2000);	//	确保加锁线程已经执行

	// 更新线程继续执行，更新操作应该能成功
	volunteer->disableSyncPoint(SP_IDX_WAIT_FOR_INSERT);
	volunteer->notifySyncPoint(SP_IDX_WAIT_FOR_INSERT);

	// 及时释放锁资源，更新操作可以成功
	bloThread->join();
	NTSE_ASSERT(session->unlockIdxObject(getRealObjectId(m_tblDef, (u64)((DrsBPTreeIndex*)m_indice->getIndex(0))->getIndexId())));

	volunteer->join();

	// 对比更新结果
	SubRecord *key1 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *key2 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	SubRecord *pkey0 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_PAD);
	DrsIndex *index = m_indice->getIndex(0);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	txnEndLsn = m_db->getTxnlog()->tailLsn();

	/*****************************  测试该bug相关修改的redo和undo操作  *********************************/
	// 读取备份数据文件，准备测试恢复
	m_heap->close(session, true);
	delete m_heap;
	m_indice->close(session, true);
	delete m_indice;
	delete m_tblDef;
	m_tblDef = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->close(true, false);
	delete m_db;
	// 恢复当前状态数据和索引文件
	myrestoreHeapFile(HEAP_BACKUP_NAME, HEAP_NAME);
	myrestoreHeapFile(INDEX_BACKUP_NAME, INDEX_NAME);
	myrestoreHeapFile(TBLDEF_BACKUP_NAME, TBLDEF_NAME);
	// 打开数据库准备操作
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);
	EXCPT_OPER(m_tblDef = TableDef::open(TBLDEF_NAME));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, HEAP_NAME, m_tblDef));
	m_indice = DrsIndice::open(m_db, session, INDEX_NAME, m_tblDef, NULL);

	conn = m_db->getConnection(false);
	session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::testBugQA60214", conn);

	// 首先从事务开始处redo操作
	IndexRecoveryManager manager(m_db, m_heap, m_tblDef, m_indice);
	manager.setTxnType(TXN_UPDATE);
	manager.redoFromSpecifiedLogs(session, txnStartLsn);
	// 检验数据正确性
	key1 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	key2 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_COMPRESS);
	pkey0 = IndexKey::allocSubRecord(session->getMemoryContext(), m_tblDef->m_indice[0], KEY_PAD);
	index = m_indice->getIndex(0);
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

	// 再从事务结束处，undo所有操作
	manager.undoFromSpecifiedLogs(session, txnStartLsn, txnEndLsn);
	// 验证正确性
	NTSE_ASSERT(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));
	file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	handle = GET_PAGE(session, file, PAGE_INDEX, IndicePageManager::NON_DATA_PAGE_NUM, Exclusived, m_indice->getDBObjStats(), NULL);
	rootPage = (IndexPage*)handle->getPage();
	IndexPage *oldRootPage = (IndexPage*)backupRootPage;
	NTSE_ASSERT(!memcmp((byte*)backupRootPage + sizeof(IndexPage), (byte*)rootPage + sizeof(IndexPage), Limits::PAGE_SIZE - sizeof(IndexPage)));
	session->releasePage(&handle);

	delete volunteer;
	delete bloThread;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	cleanDataBug60214();
}

const char* IndexBugsTestCase::getName() {
	return "IndexBugsTestCase";
}

const char* IndexBugsTestCase::getDescription() {
	return "Test all index bugs";
}

bool IndexBugsTestCase::isBig() {
	return false;
}

void IndexBugsTestCase::setUp() {
	m_tblDef = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
}

void IndexBugsTestCase::tearDown() {
	if (m_db != NULL) {
		m_db->close();
		delete m_db;
		Database::drop(".");
		m_db = NULL;
	}
}

bool IndexBugsTestCase::prepareDataBug60214() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::prepareDataBug60214", conn);

	// 创建堆和索引
	char tableName[255] = HEAP_NAME;
	File oldheapfile(tableName);
	oldheapfile.remove();
	TableDefBuilder *builder;
	//TableDef *tableDef;
	builder = new TableDefBuilder(1, "BookStore", "Book");
	builder->addColumn("BookId", CT_BIGINT, false)->addColumnS("BookName", CT_VARCHAR, 500, false, false);
	builder->addColumnS("Author", CT_VARCHAR, 128)->addColumn("Pages", CT_INT)->addColumn("Price", CT_INT);
	builder->addIndex("BookName", true, true, false, "BookName", 0, NULL);
	m_tblDef = builder->getTableDef();
	EXCPT_OPER(m_tblDef->writeFile(TBLDEF_NAME));
	EXCPT_OPER(DrsHeap::create(m_db, tableName, m_tblDef));
	EXCPT_OPER(m_heap = DrsHeap::open(m_db, session, tableName, m_tblDef));
	delete builder;
	//delete tableDef;

	char indexName[255] = INDEX_NAME;
	File oldindexfile(indexName);
	oldindexfile.remove();
	DrsIndice::create(indexName, m_tblDef);
	m_indice = DrsIndice::open(m_db, session, indexName, m_tblDef, NULL);
	try {
		for (uint i = 0; i < m_tblDef->m_numIndice; i++) {
			m_indice->createIndexPhaseOne(session, m_tblDef->m_indice[i], m_tblDef, m_heap);
			m_indice->createIndexPhaseTwo(m_tblDef->m_indice[i]);
		}
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	char commonPrefix[500];
	for (uint i = 0; i < 400; i++)
		commonPrefix[i] = 'a';
	commonPrefix[400] = '\0';
	// 插入索引数据，绕过堆本身
	// 这里首先插入的记录显示基本不可前缀压缩，保证插入数据在不到1个页面
	// 再批量插入可以大量前缀压缩的数据，构造一个很“紧凑”的MiniPage，并且是满的
	// 期望值是这里插入的记录数据和前面的数据都不可前缀压缩，因此理论上插入15条记录就可以
	uint count = 0;
	uint separating = 17, nextBegining = 50, nextScale = 15;
	uint keys = 0;
	while (true) {
		RowId rowId = 8192 + 1000 * count;
		u64 userId = count * 5;
		char bookName[500];
		if (count <= separating)
			sprintf(bookName, "%d %s", count, commonPrefix);
		else
			sprintf(bookName, "%s %d", commonPrefix, count);
		char author[100] = "eagle";
		u32 pages = System::currentTimeMillis() % 1000;
		u32 price = pages / (System::currentOSThreadID() % 10 + 1);
		RecordBuilder rb(m_tblDef, rowId, REC_REDUNDANT);
		rb.appendBigInt(userId);
		rb.appendVarchar(bookName);
		rb.appendVarchar(author);
		rb.appendInt(pages);
		rb.appendInt(price);

		Record *record = rb.getRecord();
		record->m_rowId = rowId;
		session->startTransaction(TXN_INSERT, m_tblDef->m_id);
		bool successful;
		uint dupIndex;
		EXCPT_OPER(successful = m_indice->insertIndexEntries(session, record, &dupIndex));
		CPPUNIT_ASSERT(successful);
		session->endTransaction(true);
		freeRecord(record);
		
		++keys;

		if (count >= separating) {
			if (count < nextBegining)
				count = nextBegining;
			else
				count = count + 2;
		} else {
			count++;
		}

		if (count >= nextScale * 2 + nextBegining)
			break;
	}

	// 验证数据的正确性
	File *file = ((DrsBPTreeIndice*)m_indice)->getFileDesc();
	PageHandle *handle = GET_PAGE(session, file, PAGE_INDEX, IndicePageManager::NON_DATA_PAGE_NUM, Exclusived, m_indice->getDBObjStats(), NULL);
	IndexPage *rootPage = (IndexPage*)handle->getPage();;
	NTSE_ASSERT(rootPage->m_pageCount == keys);
	NTSE_ASSERT(rootPage->isPageLeaf());
	NTSE_ASSERT(rootPage->m_freeSpace < 200);
	NTSE_ASSERT(*((byte*)rootPage + rootPage->m_dirStart + (rootPage->m_miniPageNum - 1) * IndexPage::MINI_PAGE_DIR_SIZE + 2) == IndexPage::MINI_PAGE_MAX_ITEMS);
	session->releasePage(&handle);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	return true;
}

void IndexBugsTestCase::cleanDataBug60214() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexBugsTestCase::prepareDataBug60214", conn);
	// 丢索引
	if (m_indice != NULL) {
		m_indice->close(session, true);
		delete m_indice;
		DrsIndice::drop("testIndex.nti");
	}
	// 丢表
	if (m_heap != NULL) {
		EXCPT_OPER(m_heap->close(session, true));
		delete m_heap;
		DrsHeap::drop(HEAP_NAME);
	}

	if (m_tblDef != NULL) {
		TableDef::drop(TBLDEF_NAME);
		delete m_tblDef;
		m_tblDef = NULL;
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}
