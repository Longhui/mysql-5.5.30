/**
 * 内存堆单元测试用例
 */
#ifndef _NTSETEST_MEMHEAP_TEST_H_
#define _NTSETEST_MEMHEAP_TEST_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "MemHeap.h"
#include "misc/RecordHelper.h"
#include <vector>

using namespace std;
using namespace ntse;



class MemHeapTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MemHeapTestCase);
	CPPUNIT_TEST(testInsertAt);
	CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testGet);
	CPPUNIT_TEST(testRecordCompare);
	CPPUNIT_TEST_SUITE_END();

public:

	void setUp();
	void tearDown();

protected:
	void testInsertAt();
	void testUpdate();
	void testGet();
	//////////////////////////////////////////////////////////////////////////
	//// MemHeapRecord
	//////////////////////////////////////////////////////////////////////////
	void testRecordCompare();

	void populateMemHeap(MemHeap *heap, unsigned numRecs, vector<MemHeapRid> *idvec, vector<RedRecord *> *recordVec);

private:
	Database *m_db;
	Connection *m_conn;
	Session *m_session;
	TableDef *m_tableDef;
	Config m_config;
};


#endif // _NTSETEST_MEMHEAP_TEST_H_
