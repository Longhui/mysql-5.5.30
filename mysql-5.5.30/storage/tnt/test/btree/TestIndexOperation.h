/**
 * 测试索引操作类
 *
 * @author 苏斌(bsu@corp.netease.com, naturally@163.org)
 */

#ifndef _NTSETEST_INDEX_OPERATION_H_
#define _NTSETEST_INDEX_OPERATION_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "api/Table.h"
#include "heap/Heap.h"
#include "misc/Session.h"
#include "util/Thread.h"

using namespace ntse;

class IndexOPStabilityTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndexOPStabilityTestCase);
	CPPUNIT_TEST(testMultiThreadsDML);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testMultiThreadsDML();

private:
	TableDef *createSmallTableDefWithSame();
	DrsHeap* createSmallHeapWithSame(TableDef *tableDef);
	uint buildHeapWithSame(DrsHeap *heap, const TableDef *tblDef, uint size, uint step);
	void closeSmallHeap(DrsHeap *heap);
	void createIndex(char *path, DrsHeap *heap, TableDef *tblDef) throw(NtseException);

private:
	Config m_cfg;
	Database *m_db;
	Session *m_session;
	Connection *m_conn;
	DrsIndice *m_index;
	DrsHeap *m_heap;
	TableDef *m_tblDef;
	char m_path[255];
};

class IndexOperationTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndexOperationTestCase);
	CPPUNIT_TEST(testScan);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testDelete);
	CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testOpsOnEmptyIndex);
	CPPUNIT_TEST(testLongKey);
	CPPUNIT_TEST(testMTDeleteRelocate);
	CPPUNIT_TEST(testMTDeleteSMORelocate);
	CPPUNIT_TEST(testMTRWConflict);
	CPPUNIT_TEST(testMTLockHoldingLatch);
	CPPUNIT_TEST(testMTLockIdxObj);
	CPPUNIT_TEST(testMTSearchParentInSMO);
	CPPUNIT_TEST(testRecordsInRange);
	CPPUNIT_TEST(testGetStatus);
	CPPUNIT_TEST(testSample);
	CPPUNIT_TEST(testStrangePKFetch);
	CPPUNIT_TEST(testUniqueKeyConflict);
	CPPUNIT_TEST(testDMLDeadLocks);
	//CPPUNIT_TEST(testInsertAfterSearch);
	
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testScan();
	void testUniqueScan();
	void testRangeScan();
	void testInsert();
	void testDelete();
	void testUpdate();
	void testOpsOnEmptyIndex();
	void testLongKey();
	void testMTDeleteRelocate();
	void testMTDeleteSMORelocate();
	void testMTRWConflict();
	void testMTLockHoldingLatch();
	void testMTLockIdxObj();
	void testMTSearchParentInSMO();
	void testRecordsInRange();
	void testGetStatus();
	void testIndexStructure();
	void testSample();
	void testStrangePKFetch();
	void testUniqueKeyConflict();
	void testDMLDeadLocks();
	void testBugQA60214();
	//void testInsertAfterSearch();	

private:
	TableDef* createBigTableDef();
	DrsHeap* createBigHeap(const TableDef *tableDef);
	uint buildBigHeap(DrsHeap *heap, const TableDef *tblDef, bool testPrefix, uint size, uint step);

	TableDef* createSmallTableDef();
	DrsHeap* createSmallHeap(const TableDef *tableDef);
	void closeSmallHeap(DrsHeap *heap);
	uint buildHeap(DrsHeap *heap, const TableDef *tblDef, uint size, uint step);
	void createIndex(char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException);
	TableDef* createSmallTableDefWithSame();
	DrsHeap* createSmallHeapWithSame(const TableDef *tableDef);
	uint buildHeapWithSame(DrsHeap *heap, const TableDef *tblDef, uint size, uint step);

	void insertIndexSpecifiedRecord(Session *session, MemoryContext *memoryContext, Record *insertrecord);
	Record* insertHeapSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i);
	Record* insertSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i);

	void checkIndex(Session *session, MemoryContext *memoryContext, uint keys);

	int getRangeRecords(Session *session, DrsIndex *index, SubRecord *findKey, bool includekey);
	void calcSpecifiedRange(Session *session, TableDef *tableDef, DrsIndex *index, u64 key1, u64 key2, bool includekey1, bool includekey2, SubRecord *minKey, SubRecord *maxKey);

	void sampleIndex(const char *name, DrsIndex *index, uint samplePages);

	bool findAKey(Session *session, DrsIndex *index, RowLockHandle **rlh, uint i, SubToSubExtractor *extractor);

private:
	Config m_cfg;
	Database *m_db;
	Session *m_session;
	Connection *m_conn;
	DrsIndice *m_index;
	DrsHeap *m_heap;
	TableDef *m_tblDef;
	char m_path[255];
};

class IndexBugsTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndexBugsTestCase);
	CPPUNIT_TEST(testBugQA60214);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBugQA60214();

private:
	bool prepareDataBug60214();
	void cleanDataBug60214();

private:
	Config m_cfg;
	Database *m_db;
	DrsIndice *m_indice;
	DrsHeap *m_heap;
	TableDef *m_tblDef;
};

class Volunteer : public Thread {
public:
	Volunteer(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowid) : Thread(name) {
		m_name = name;
		m_db = db;
		m_conn = conn;
		m_heap = heap;
		m_tblDef = tblDef;
		m_indice = indice;
		m_recordid = recordid;
		m_rowId = rowid;
	}

protected:
	uint m_recordid;
	const char *m_name;
	Database *m_db;
	Connection *m_conn;
	DrsHeap *m_heap;
	const TableDef *m_tblDef;
	DrsIndice *m_indice;
	RowId m_rowId;
};

/**
 * 专门从事在别的线程在享受数据阅读操修改利的时候进行索引数据破坏的线程
 */
class TroubleMaker : public Volunteer {
public:
	TroubleMaker(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowid) :
	  Volunteer(name, db, conn, heap, tblDef, indice, recordid, rowid) {}

private:
	virtual void run();
};


/**
 * 简洁主义者，从事各种删除操作
 */
class SimpleMan : public Volunteer {
public:
	SimpleMan(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, bool captious, RowId rowid, bool deleteAll = false) :
		Volunteer(name, db, conn, heap, tblDef, indice, recordid, rowid) {
			m_captious = captious;
			m_deleteAll = deleteAll;
		}

private:
	virtual void run();

private:
	bool m_captious;		// 为true表示范围删除，false表示只需要删除若干个键值
	bool m_deleteAll;		// 在范围删除的情况下，指定删除所有键值
};


/**
 * 喜欢阅读的人，从事索引范围查询
 */
class ReadingLover : public Volunteer {
public:
	ReadingLover(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, bool crazy, bool upsidedown, RowLockHandle **rlh, RowId rowid, u64 rangeSize = (u64)-1) :
		Volunteer(name, db, conn, heap, tblDef, indice, recordid, rowid) {
			m_crazy = crazy;
			m_upsidedown = upsidedown;
			m_rowLockHandle = rlh;
			m_rangeSize = rangeSize;
		}

		uint getReadNum() {
			return m_readNum;
		}

private:
	virtual void run();

private:
	bool m_crazy;		// 为true表示是个疯狂读者，范围扫描，false表示唯一查询
	uint m_readNum;		// 扫描得到的结果数，唯一查询没结果用0表示
	u64 m_rangeSize;	// 范围扫描可以指定扫描项数
	bool m_upsidedown;	// 颠倒读，范围扫描的时候进行反响扫描
	RowLockHandle **m_rowLockHandle;	// 加锁时候需要指定锁句柄
};


/**
 * 勤劳的蜜蜂，专门从事插入索引键值操作
 */
class Bee : public Volunteer {
public:
	Bee(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, uint rangeSize, RowId rowid) :
		Volunteer(name, db, conn, heap, tblDef, indice, recordid, rowid) {
			m_rangeSize = rangeSize;
		}

private:
	virtual void run();

private:
	uint m_rangeSize;		// 插入数值的范围，如果为0，就只表示插入recordid这项
};


/**
 * 变异的蜜蜂，同样勤劳，专门从事插入索引键值操作
 */
class VariantBee : public Volunteer {
public:
	VariantBee(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, uint rangeSize, RowId rowid) :
	  Volunteer(name, db, conn, heap, tblDef, indice, recordid, rowid) {
		  m_rangeSize = rangeSize;
	  }

private:
	virtual void run();

private:
	uint m_rangeSize;		// 插入数值的范围，如果为0，就只表示插入recordid这项
};



/**
 * 专门负责创建索引的线程，it's really a big project
 */
class BigProject : public Volunteer {
public:
	BigProject(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid) :
	  Volunteer(name, db, conn, heap, tblDef, indice, recordid, INVALID_ROW_ID) {}

private:
	virtual void run();
};


/**
 * 稍微偏爱reading的三心二意的志愿者，DML操作无所不玩的人
 */
class HalfHeartedMan : public Volunteer {
public:
	HalfHeartedMan(const char *name, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice, uint recordid, RowId rowid, u64 repeatTimes) :
	  Volunteer(name, db, conn, heap, tblDef, indice, recordid, rowid) {
		  m_times = repeatTimes;
	  }

private:
	virtual void run();

private:
	u64 m_times;
};


class IndexRecoveryManager {
public:
	IndexRecoveryManager(Database *db, DrsHeap *heap, const TableDef *tblDef, DrsIndice *indice) {
		m_db = db;
		m_heap = heap;
		m_indice = indice;
		m_tblDef = tblDef;
	}

	void setTxnType(TxnType txnType) {
		m_txnType = txnType;
	}

	void redoFromSpecifiedLogs(Session *session, u64 startLsn);
	void undoFromSpecifiedLogs(Session *session, u64 startLsn, u64 targetLsn);

	void redoALog(Session *session, LogEntry *logEntry, u64 lsn);
	void undoALog(Session *session, LogEntry *logEntry, u64 lsn);

private:
	Database *m_db;
	DrsHeap *m_heap;
	const TableDef *m_tblDef;
	DrsIndice *m_indice;
	TxnType m_txnType;
};

class IndexRecoveryTestCase : public  CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndexRecoveryTestCase);
	CPPUNIT_TEST(testCPSTDMLInRecv);
	CPPUNIT_TEST(testCreateIndexRecovery);
	CPPUNIT_TEST(testDMLRecoveryRedo);
	CPPUNIT_TEST(testDMLRecoveryUndo);
	CPPUNIT_TEST(testDMLRecoveryRedoCPST);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

public:
	void testCPSTDMLInRecv();
	void testCreateIndexRecovery();
	void testDMLRecoveryRedo();
	void testDMLRecoveryUndo();
	void testDMLRecoveryRedoCPST();

private:
	TableDef* createBigTableDef();
	DrsHeap* createBigHeap(TableDef *tableDef);
	uint buildBigHeap(DrsHeap *heap, const TableDef *tblDef, bool testPrefix, uint size, uint step);

	TableDef* createSmallTableDef();
	DrsHeap* createSmallHeap(TableDef *tableDef);
	void closeSmallHeap(DrsHeap *heap);
	uint buildHeap(DrsHeap *heap, const TableDef *tblDef, uint size, uint step);
	void createIndex(char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException);

	Record* insertSpecifiedRecord(Session *session, MemoryContext *memoryContext, uint i, bool testBig = false, bool testPrefix = false);
	Record* insertSpecifiedKey(Session *session, MemoryContext *memoryContext, uint i, bool testBig = false, bool testPrefix = false);
	Record* makeRecordForIAndD(uint i, bool testBig = false, bool testPrefix = false);

	void closeEnv();
	void openEnv();

	void checkIndex(Session *session, MemoryContext *memoryContext, uint keys);

private:
	Config m_cfg;
	Database *m_db;
	Session *m_session;
	Connection *m_conn;
	DrsIndice *m_index;
	DrsHeap *m_heap;
	TableDef *m_tblDef;
	char m_path[255];
};


class IndexSMOTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndexSMOTestCase);
	CPPUNIT_TEST(testLotsUpdates);
	CPPUNIT_TEST(testInsertMaxKeySMO);
	CPPUNIT_TEST(testBugNTSETNT39);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

public:
	void testLotsUpdates();	// 通过大量的更新操作，来导致SMO，测试SMO的正确性
	void testInsertMaxKeySMO();
	void testBugNTSETNT39();

private:
	TableDef* createTableDef();
	DrsHeap* createHeapAndLoadData(Database *db, const TableDef *tblDef, uint size);
	DrsIndice* createIndex(Database *db, char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException);
	void closeHeap(DrsHeap *heap);

	SubRecord* makeFindKeyByUserId(const TableDef *tableDef, u64 userId);

private:
	DrsIndice *m_index;
	DrsHeap *m_heap;
	Config m_cfg;
	Database *m_db;
	const TableDef *m_tblDef;
	char m_path[255];

	static const uint MAX_USERS = 1000000;	/** 用户数量 */
	static const uint USER_RECORDS = 30;	/** 同一个用户的记录数 */
	static const u64 USER_MIN_ID = 4394956736;	/** 用户ID的最小值 */
};


class IndexSMOThread :public Thread {
public:
	IndexSMOThread(Database *db, TableDef *tableDef, DrsIndice *index);
	void run();

private:
 	Database			*m_db;
	TableDef			*m_tableDef;
	Session				*m_session;
	Connection			*m_conn;
	DrsIndice			*m_index;
};


#endif

