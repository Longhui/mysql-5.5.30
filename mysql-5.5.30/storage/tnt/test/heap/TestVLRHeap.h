/**
 * 测试变长堆
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 * @author 谢可(ken@163.org)
 */

#ifndef _NTSETEST_VLRHEAP_H_
#define _NTSETEST_VLRHEAP_H_

#include <cppunit/extensions/HelperMacros.h>

#include <heap/VariableLengthRecordHeap.h>
#include "api/Database.h"
#include "util/Thread.h"

using namespace ntse;
class VLRHeapTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(VLRHeapTestCase);
	CPPUNIT_TEST(testOpen);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testGet);
	CPPUNIT_TEST(testDelete);
	//CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testTableScan);
	CPPUNIT_TEST(testRedoCreate);
	CPPUNIT_TEST(testRedoInsert);
	CPPUNIT_TEST(testRedoDelete);
	CPPUNIT_TEST(testRedoUpdate);
	CPPUNIT_TEST(testSample);
	//CPPUNIT_TEST(testSample2);
	//CPPUNIT_TEST(testRecord);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testInsert();
	void testGet();
	void testDelete();
	void testUpdate();
	void testOpen();
	//void testTruncate();
	void testTableScan();
	void testRedoCreate();
	//void testRedoTruncate();
	void testRedoInsert();
	void testRedoDelete();
	void testRedoUpdate();
	void testRecord();
	void testSample();
	void testSample2();

private:
	Database *m_db;
	Connection *m_conn;
	DrsHeap *m_heap;
	TableDef *m_tblDef;
	Config m_config;
	VariableLengthRecordHeap *m_vheap;
	//void createData4TableScan();

};


/* 线程 */
class Worker : public ntse::Thread {
public:
	Worker(Database *db, Connection *conn, DrsHeap *heap, TableDef *tblDef, const char *name, void *data): Thread(name) {
		m_name = name;
		m_db = db;
		m_conn = conn;
		m_heap = heap;
		m_tblDef = tblDef;
		m_data = data;
	}
protected:
	const char *m_name;
	Database *m_db;
	Connection *m_conn;
	DrsHeap *m_heap;
	TableDef *m_tblDef;
	void *m_data;
};


/* 工具函数 */
Record* createRecord(TableDef *tableDef, int random = 0);
Record* createRecordSmall(TableDef *tableDef, int random = 0);
Record* createRecordLong(TableDef *tableDef, int random = 0);
Record* createTinyRawRecord(int i);
Record* createShortRawRecord(int i);
Record* createLongRawRecord(int i);



#endif
