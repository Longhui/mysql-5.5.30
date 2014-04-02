#ifndef _TNTTEST_MHEAP_H_
#define _TNTTEST_MHEAP_H_

#include <cppunit/extensions/HelperMacros.h>
#include "heap/MHeap.h"

using namespace ntse;
using namespace tnt;

//Record* createStudentRec(TableDef *tableDef, RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade);

class MHeapTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MHeapTestCase);
	CPPUNIT_TEST(testAddAndRemoveFromFreeList);
	CPPUNIT_TEST(testAllocRecordPage);
	CPPUNIT_TEST(testFreeRecordPage);
	CPPUNIT_TEST(testFreeSomePage);
	CPPUNIT_TEST(testFreeSomePageLarge);
	CPPUNIT_TEST(testFreeAllPage);
	CPPUNIT_TEST(testDefrag);
	CPPUNIT_TEST(testAllocAndDefrag);
	CPPUNIT_TEST(testUsePageAndDefrag);
	CPPUNIT_TEST(testInsertHeapRecord);
	CPPUNIT_TEST(testUpdateHeapRecord);
	CPPUNIT_TEST(testRemoveHeapRecord);
	CPPUNIT_TEST(testGetHeapRedRecord);
	CPPUNIT_TEST(testGetHeapRedRecordSafe);

	CPPUNIT_TEST(testMulThreadInsert);
	CPPUNIT_TEST(testMulThreadUpdate);

	CPPUNIT_TEST(testGetHeapRecAndRemove);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testAddAndRemoveFromFreeList();
	void testAllocRecordPage();
	void testFreeRecordPage();
	void testFreeSomePage();
	void testFreeSomePageLarge();
	void testFreeAllPage();
	void testDefrag();
	void testAllocAndDefrag();
	void testUsePageAndDefrag();

	void testInsertHeapRecord();
	void testUpdateHeapRecord();
	void testRemoveHeapRecord();

	void testGetHeapRedRecord();
	void testGetHeapRedRecordSafe();

	void testMulThreadInsert();
	void testMulThreadUpdate();

	void testGetHeapRecAndRemove();
private:
	TableDef* createStudentTableDef();

	MHeap         *m_mheap;

	Connection    *m_conn;
	Session       *m_session;

	Config		  m_config;
	Database      *m_db;
	TableDef      *m_tableDef;

	TNTIMPageManager *m_pageManager;
	PagePool         *m_pool;
	HashIndex        *m_hashIndex;
};
#endif
