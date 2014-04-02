#ifndef _TNTTEST_TXNRECMANAGER_H_
#define _TNTTEST_TXNRECMANAGER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "heap/TxnRecManager.h"

using namespace ntse;
using namespace tnt;

class TxnRecManagerTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TxnRecManagerTestCase);
	CPPUNIT_TEST(testPush);
	CPPUNIT_TEST(testDefrag);
	CPPUNIT_TEST(testPageAllocAndFree);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testPush();
	void testDefrag();
	void testPageAllocAndFree();
private:
	TNTIMPageManager *m_pageManager;
	PagePool         *m_pool;
	TxnRecManager    *m_txnRecManager;

	Connection       *m_conn;
	Session          *m_session;
	Database	     *m_db;
	Config		     m_config;
};
#endif