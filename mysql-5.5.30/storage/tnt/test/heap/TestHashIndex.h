#ifndef _TNTTEST_HASHINDEX_H_
#define _TNTTEST_HASHINDEX_H_

#include <cppunit/extensions/HelperMacros.h>
//#include "heap/HashIndex.h"
#include "api/TNTDatabase.h"

using namespace std;
using namespace ntse;
using namespace tnt;

//TODO 需要添加hashIndex的多线程测试
class HashIndexTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(HashIndexTestCase);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testRemove);
	CPPUNIT_TEST(testInsertOrUpdate);
	CPPUNIT_TEST(testUpdate);
	CPPUNIT_TEST(testCount);
	CPPUNIT_TEST(testDefrag);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testInsert();
	void testRemove();
	void testInsertOrUpdate();
	void testUpdate();
	void testCount();
	void testDefrag();
private:
	void insert2HashIndex(HashIndex *hashIndex, u32 total);
	static TNTTransaction *startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode = TL_NO, bool log = false);
	static void commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
	
	TNTIMPageManager *m_pageManager;
	PagePool         *m_pool;
	MemoryContext    *m_ctx;
};
#endif