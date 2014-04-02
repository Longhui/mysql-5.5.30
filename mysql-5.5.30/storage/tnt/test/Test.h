/**
 * ����ʱһЩ���ù���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_TEST_H_
#define _NTSETEST_TEST_H_
#include <string.h>
#include "misc/Global.h"
#include "util/File.h"

/** �ڲ��������У������׳�NtseException�쳣�Ĳ����������
 * ���������������쳣����ʱCppUnit��ܿ��Դ�ӡ���쳣��Ϣ
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
 * ������Ҫ�׳�NtseException�쳣�Ĳ���������������������
 *��û���쳣�����ӡ����ʧ����Ϣ
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
