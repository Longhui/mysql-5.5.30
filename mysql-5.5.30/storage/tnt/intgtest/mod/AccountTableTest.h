/**
 * 内存堆单元测试用例
 */
#ifndef _NTSETEST_ACCOUNT_TABLE_TEST_H_
#define _NTSETEST_ACCOUNT_TABLE_TEST_H_

#include <cppunit/extensions/HelperMacros.h>


class AccountTableTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(AccountTableTestCase);
	CPPUNIT_TEST(testCreateRecord);
	CPPUNIT_TEST_SUITE_END();

public:
	void testCreateRecord();
	

protected:
	

private:

};

#endif // _NTSETEST_ACCOUNT_TABLE_H_
