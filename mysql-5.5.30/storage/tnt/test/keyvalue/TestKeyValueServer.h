/*
*	测试KeyValueServer的api
*
*	@author	廖定柏(liaodingbai@corp.netease.com)
*/

#ifndef _NTSETEST_KEYVALUE_SERVER_H_
#define _NTSETEST_KEYVALUE_SERVER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "keyvalue/KeyValueServer.h"
#include "api/Database.h"
#include "api/TestHelper.h"

using namespace ntse;
using namespace std;

/**
*	KeyValueServer的测试类
*/
class KeyValueServerTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(KeyValueServerTestCase);

	//CPPUNIT_TEST(testSimpleRun);
	CPPUNIT_TEST(testThreadPoolRun);
	CPPUNIT_TEST(testThreadedRun);
	CPPUNIT_TEST(testClose);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	
	void testSimpleRun();
	void testThreadPoolRun();
	void testThreadedRun();

	void testClose();

private:
	Database	*m_db;				/** ntse数据库 */
	Config		m_config;			/** 数据库相关配置 */
	TblInterface	*m_table;		/** 当前操作的数据库表 */

	static const string baseDir;	/** 根目录 */
};

#endif