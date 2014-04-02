/**
 * TNT事务管理模块测试
 * 
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef _NTSETEST_TNT_TRANSACTION_H_
#define _NTSETEST_TNT_TRANSACTION_H_

#include <cppunit/extensions/HelperMacros.h>
#include "trx/TNTTransaction.h"
#include "api/TNTDatabase.h"

using namespace ntse;
using namespace tnt;

class TNTTransactionTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TNTTransactionTestCase);
	CPPUNIT_TEST(testCommon);
	CPPUNIT_TEST(testXA);
	CPPUNIT_TEST(testVisable);
	CPPUNIT_TEST(testTLock);
	CPPUNIT_TEST(testReadOnly);
	CPPUNIT_TEST(testIsActive);
	CPPUNIT_TEST(testGetActiveTrxs);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testCommon();
	void testXA();
	void testVisable();
	void testTLock();
	void testReadOnly();
	void testIsActive();
	void testGetActiveTrxs();

private:

private:
	TNTConfig m_config;
	TNTDatabase *m_db;
	TNTTrxSys   *m_trxSys;
	Connection  *m_conn;
};

#endif