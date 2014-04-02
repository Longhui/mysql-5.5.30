/**
 * 测试时一些常用功能
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_TEST_H_
#define _NTSETEST_TEST_H_
#include <string.h>
#include "misc/Global.h"
#include "util/File.h"

/** 在测试用例中，可能抛出NtseException异常的操作用这个宏
 * 括起来，这样在异常发生时CppUnit框架可以打印出异常信息
 */
#define EXCPT_OPER(op)									\
	try {												\
	    op;												\
	} catch (NtseException &e) {						\
		cout << "Error: " << e.getMessage() << endl;	\
		CPPUNIT_FAIL(e.getMessage());					\
		throw e;										\
	}

/**
 * 对于需要抛出NtseException异常的操作用这个宏括起来，如果
 *　没有异常，则打印断言失败信息
 */
#define NEED_EXCPT(op) do {                         \
	bool expt = false;                              \
	try {                                           \
		op;                                         \
	} catch (NtseException &e) {                    \
		UNREFERENCED_PARAMETER(e);                  \
		expt = true;                                \
	}                                               \
	CPPUNIT_ASSERT_MESSAGE("Operation should throw ntse exception but not.", expt);                           \
} while(0)      

using namespace ntse;

bool compareFile(const char *f1, const char *f2) throw(NtseException);
bool compareFile(File *file1, File *file2, u64 offset = 0, u64 len = (u64)-1, bool ignoreLsn = false) throw(NtseException);

char* randomStr(size_t size);

bool isEssentialOnly();
void setEssentialOnly(bool v);

#endif
