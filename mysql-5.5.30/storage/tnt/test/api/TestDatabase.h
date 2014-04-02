/**
 * 测试数据库管理接口
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_DATABASE_H_
#define _NTSETEST_DATABASE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include <string>
#include <util/System.h>
#include <util/Sync.h>

using namespace std;
using namespace ntse;

class DatabaseTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(DatabaseTestCase);
	CPPUNIT_TEST(testBasic);
	CPPUNIT_TEST(testBufferDistr);
	CPPUNIT_TEST(testIsNtseFile);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBasic();
	void testBufferDistr();
	void testIsNtseFile();

private:
	void printBufUsages(Array<BufUsage *> *usageArr);
	void freeBufUsages(Array<BufUsage *> *usageArr);
	u64 getBufPages(Array<BufUsage *> *usageArr, u16 tblId, DBObjType type, const char *idxName = "");

	Config		m_config;
	Database	*m_db;
	Table		*m_table;
};

#endif

