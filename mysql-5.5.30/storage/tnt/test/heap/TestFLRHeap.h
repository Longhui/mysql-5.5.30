/**
 * 测试定长堆及堆公用功能
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 * @author 谢可(ken@163.org)
 */

#ifndef _NTSETEST_FLRHEAP_H_
#define _NTSETEST_FLRHEAP_H_

#include <cppunit/extensions/HelperMacros.h>
#include <heap/FixedLengthRecordHeap.h>
#include "api/Database.h"
#include "util/Thread.h"
#include <vector>

using namespace std;
using namespace ntse;

class FLRHeapTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(FLRHeapTestCase);
	CPPUNIT_TEST(testDrop);
	CPPUNIT_TEST(testOpen);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testGet);
	CPPUNIT_TEST(testDel);
	CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testTableScan);
	CPPUNIT_TEST(testLSN);
	CPPUNIT_TEST(testRedoCreate);
	CPPUNIT_TEST(testRedoInsert);
	CPPUNIT_TEST(testRedoUpdate);
	CPPUNIT_TEST(testRedoDelete);
	CPPUNIT_TEST(testSample);
	//CPPUNIT_TEST(testHash);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testDrop();
	void testOpen();
	void testInsert();
	void testGet();
	void testDel();
	void testUpdate();
	void testTableScan();
	void testLSN();
	void testRedoCreate();
	void testRedoInsert();

	void testRedoUpdate();
	void testRedoDelete();
	void testSample();
	void testHash();

	void testMTInsert();
private:
	Database *m_db;		/** 数据库 */
	Connection *m_conn;	/** 数据库连接 */
	DrsHeap *m_heap;	/** 定长记录堆 */
	TableDef *m_tblDef;
	Config m_config;	/** 数据库配置 */
	TableDef *createSmallTableDef();
	DrsHeap *createSmallHeap(TableDef *tableDef);
	void dropSmallHeap();
	void closeSmallHeap();
	void initHeapInvalidPage( FLRHeapRecordPageInfo * page, DrsHeap *heap ,const TableDef *tblDef);
};

/*** 定长堆的性能测试 ***/
class FLRHeapPerformanceTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(FLRHeapPerformanceTestCase);
	CPPUNIT_TEST(testTableScanPerf);
	CPPUNIT_TEST(testInsertPerf);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testTableScanPerf();
	void testInsertPerf();
private:
	Database *m_db;			/** 数据库 */
	Connection *m_conn;		/** 数据连接 */
	DrsHeap *m_heap;		/** 定长堆 */
	TableDef *m_tblDef;
	Config m_config;		/** 数据库配置 */
	DrsHeap *createHeap(TableDef *tblDef);
	TableDef *createTableDef();
	void dropHeap();
	void dropTableDef();
	void closeHeap();
};

/* 线程 */

class Robot : public ntse::Thread {
public:
	Robot(Database *db, Connection *conn, TableDef *tblDef, DrsHeap *heap, const char *name, void *data): Thread("Robot") {
		m_name = name;
		m_db = db;
		m_conn = conn;
		m_tblDef = tblDef;
		m_heap = heap;
		m_data = data;
	}
protected:
	const char *m_name;
	Database *m_db;
	Connection *m_conn;
	TableDef *m_tblDef;
	DrsHeap *m_heap;
	void *m_data;
};


Record* createRecord(u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance, TableDef *tableDef);
RowId insertRecord(uint number, Record **rec, Database *db, Connection *conn, DrsHeap *heap, TableDef *tableDef);
bool updateRecord(RowId rid, SubRecord *subRec, Database *db, Connection *conn, DrsHeap *heap);
bool updateRecord(RowId rid, Record *record, Database *db, Connection *conn, DrsHeap *heap);
bool deleteRecord(RowId rid, Database *db, Connection *conn, DrsHeap *heap);
bool readRecord(RowId rid, Record *record, Database *db, Connection *conn, DrsHeap *heap);
bool readSubRecord(RowId rid, SubRecord *subRec, Database *db, Connection *conn, DrsHeap *heap, const TableDef *tblDef);


void backupPage(Session *session, DrsHeap *heap, u64 pageNum, byte *pageBuffer);
void restorePage(Session *session, DrsHeap *heap, u64 pageNum, byte *pageBuffer);
void backupHeapFile(File *file, const char *backupName, Buffer *buffer = NULL);
void backupHeapFile(const char *origName, const char *backupName);
void backupTblDefFile(const char *origName, const char *backupName);
void restoreHeapFile(const char *backupHeapFile, const char *origFile);
int compareFile(File *file1, File *file2, int startPage = 0, int *diffIdx = NULL);




/* 工具类 */

template<class T>
class svector { /** 线程安全的vector */
public:
	svector(): m_lock("svector::lock", __FILE__, __LINE__) {}

	void push_back(const T item) {
		m_lock.lock(__FILE__, __LINE__);
		m_vec.push_back(item);
		m_lock.unlock();
	}
	T& operator[](int idx) {
		m_lock.lock(__FILE__, __LINE__);
		T &tmp = m_vec[idx];
		m_lock.unlock();
		return tmp;
	}
	int size() {
		int size;
		m_lock.lock(__FILE__, __LINE__);
		size = (int)m_vec.size();
		m_lock.unlock();
		return size;
	}
	void pop_back() {
		m_lock.lock(__FILE__, __LINE__);
		m_vec.pop_back();
		m_lock.unlock();
	}
private:
	vector<T> m_vec;
	ntse::Mutex	m_lock;
};

#endif
