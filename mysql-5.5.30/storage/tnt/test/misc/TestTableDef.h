/**
 * 测试表定义
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef _NTSETEST_TABLE_DEF_H_
#define _NTSETEST_TABLE_DEF_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/TestTable.h"

using namespace ntse;

class TableDefTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TableDefTestCase);
	CPPUNIT_TEST(testEqual);
	CPPUNIT_TEST(testColumnWrite);
	CPPUNIT_TEST(testColumnRead);
	CPPUNIT_TEST(testIndexWrite);
	CPPUNIT_TEST(testIndexRead);
	CPPUNIT_TEST(testTableDefWrite);
	CPPUNIT_TEST(testTableDefRead);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testEqual();
	void testColumnWrite();
	void testColumnRead();
	void testIndexWrite();
	void testIndexRead();
	void testTableDefWrite();
	void testTableDefRead();

private:
	void compareColumn(ColumnDef *columnDef1, ColumnDef *columnDef2);
	void exchangeIndex(TableDef *tblDef, u16 first, u16 second);

private:
	TableDef *m_tableDef;
};

#endif