/**
 * 测试TNT索引统一接口
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef _NTSETEST_TNT_INDEX_H_
#define _NTSETEST_TNT_INDEX_H_

#include <cppunit/extensions/HelperMacros.h>
#include "btree/TNTIndex.h"
#include "btree/IndexBLinkTree.h"
#include "heap/Heap.h"

using namespace tnt;

class TNTIndiceTestBuilder {
public:
	TNTIndiceTestBuilder();
	virtual ~TNTIndiceTestBuilder() {}

	virtual void init(uint tntBufSize = 1024);
	virtual void clear();

	DrsHeap* createSmallHeap() throw(NtseException);
	void closeSmallHeap(DrsHeap *heap);
	void buildHeap(DrsHeap *heap, uint size, bool keepSequence, bool hasSame);
	Record* createSmallHeapRecord(u64 rowid, u64 userid, const char *username, u64 bankacc, 
		u32 balance, RecFormat format = REC_FIXLEN);
	TableDef* getTableDef() const;
	void createIndice();
	void dropIndice();
	void createAllTwoIndex(DrsHeap *heap);

protected:
	virtual TableDef* generateTableDef();

protected:
	TNTConfig   m_config;
	TNTDatabase *m_db;
	Connection  *m_conn;
	Session     *m_session;
	TableDef    *m_tableDef;

	DrsIndice   *m_drsIndice;
	TNTIndice   *m_tntIndice;
};

/**
 * 唯一性键值锁加锁测试者
 */
class UniqueKeyLockTester : public Thread {
public:
	UniqueKeyLockTester(TNTDatabase *db, TNTIndice *indice, const char *name);
	~UniqueKeyLockTester();
	void setLockInfo(vector<Record*> *rec, bool expectSucc, uint expectDupIndex = (uint)-1);
	virtual void run();
	void unlockAll();

protected:
	TNTDatabase     *m_db;
	Connection      *m_conn;
	TNTIndice       *m_indice;
	Session         *m_session;
	vector<Record*> *m_recArr;
	bool        m_expectSucc;
	uint        m_expectDupIndex;
};

/** TNT索引管理测试用例 */
class TNTIndiceTestCase : public CPPUNIT_NS::TestFixture, public TNTIndiceTestBuilder {
	CPPUNIT_TEST_SUITE(TNTIndiceTestCase);
	CPPUNIT_TEST(testCreateDropOpenClose);
	CPPUNIT_TEST(testCreateAndDropIndex);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testDelete);
	CPPUNIT_TEST(testCheckDup);
	CPPUNIT_TEST(testUniqueKeyLock);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testCreateDropOpenClose();
	void testCreateAndDropIndex();
	void testInsert();
	void testUpdate();
	void testDelete();
	void testCheckDup();
	void testUniqueKeyLock();

private:
};

/** TNT索引测试用例 */
class TNTIndexTestCase : public CPPUNIT_NS::TestFixture, public TNTIndiceTestBuilder {
	CPPUNIT_TEST_SUITE(TNTIndexTestCase);
	CPPUNIT_TEST(testUniqueFetch);
	CPPUNIT_TEST(testRangeScan);
	CPPUNIT_TEST(testPurge);
	CPPUNIT_TEST(testRollback);
	CPPUNIT_TEST(testFetchMaxKey);
	CPPUNIT_TEST(testScanLock);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testUniqueFetch();
	void testRangeScan();
	void testPurge();
	void testRollback();
	void testFetchMaxKey();
	void testScanLock();

private:
	DrsHeap *m_heap;
	HashIndex *m_hashIndex;
};

/////////////////////////////////////////////////

class MIndexWorker : public Thread {
public:
	const static uint THREAD_CONCURRENCE = 10;
public:
	MIndexWorker(const char *name, u16 jobNo, u8 indexNo, const TableDef *tableDef, 
		TNTDatabase *db, TNTIndice *tntIndice, TNTIndiceTestBuilder *testBuilder);
	~MIndexWorker();
	virtual void run();

protected:
	SubRecord* createTestKey(uint kv);
	void checkKeyInTree(BLinkTree *bltree, SubRecord *padKey, u8 delBit);

protected:
	u16 m_jobNo;
	u8 m_indexNo;
	const TableDef *m_tableDef;
	const IndexDef *m_indexDef;
	TNTDatabase    *m_db;
	Connection     *m_conn;
	Session        *m_session;
	TNTIndice      *m_tntIndice;
	TNTIndiceTestBuilder *m_testBuilder;
};

/**
 * TNT索引稳定性测试用例(多线程)
 */
class MIndexOPStabilityTestCase : public CPPUNIT_NS::TestFixture, public TNTIndiceTestBuilder {
	CPPUNIT_TEST_SUITE(MIndexOPStabilityTestCase);
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
	DrsHeap *m_heap;
};



class TNTIndexOPThread : public Thread , public TNTIndiceTestBuilder{
public:
	TNTIndexOPThread(const char *name, TableDef *tableDef, 
		TNTDatabase *db, TNTIndice *tntIndice);

protected:
	

};

class TNTIndexTestScanLockThread: public TNTIndexOPThread {
public:
	TNTIndexTestScanLockThread(const char *name, TableDef *tableDef, 
		TNTDatabase *db, TNTIndice *tntIndice):TNTIndexOPThread(name, tableDef, db, tntIndice){}
private:
	virtual void run();
};
#endif



class TNTIndexTestScanThread: public TNTIndexOPThread {
public:
	TNTIndexTestScanThread(const char *name, TableDef *tableDef, 
		TNTDatabase *db, TNTIndice *tntIndice):TNTIndexOPThread(name, tableDef, db, tntIndice){}
private:
	virtual void run();
};