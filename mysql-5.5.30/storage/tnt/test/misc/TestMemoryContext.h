/**
 * 测试内存分配上下文
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_MEMORYCONTEXT_H_
#define _NTSETEST_MEMORYCONTEXT_H_

#include <cppunit/extensions/HelperMacros.h>
#include "misc/Session.h"
using namespace ntse;
class MemoryContextTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MemoryContextTestCase);
	CPPUNIT_TEST(testMemoryContext);
	CPPUNIT_TEST(testMemoryContextUsePool);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testMemoryContext();
	void testMemoryContextUsePool();
};

#endif
