/**
 * 测试DDL操作的恢复流程
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_RECVDDL_H_
#define _NTSETEST_RECVDDL_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "api/TestRecover.h"
#include <string>
#include <util/System.h>
#include <util/Sync.h>

using namespace std;
using namespace ntse;

/** DDL操作恢复流程测试用例 */
class RecvDDLTestCase: public RecoverTestCase {
	CPPUNIT_TEST_SUITE(RecvDDLTestCase);
	CPPUNIT_TEST(testRecvCreateTableSucc);
	CPPUNIT_TEST(testRecvCreateTableOldCtrlFile);
	CPPUNIT_TEST(testRecvDropTable);
	CPPUNIT_TEST(testRecvTruncate);
	CPPUNIT_TEST(testRecvRenameBefore);
	CPPUNIT_TEST(testRecvRenameAfter);
	CPPUNIT_TEST(testRecvAddIndexSucc);
	CPPUNIT_TEST(testRecvAddIndexFail);
	CPPUNIT_TEST(testRecvAddIndexReadonly);
	CPPUNIT_TEST(testRecvAddIndexOnline);
	CPPUNIT_TEST(testRecvDropIndex);
	CPPUNIT_TEST(testMisc);
	CPPUNIT_TEST(bug657);
	CPPUNIT_TEST(bug700);
	CPPUNIT_TEST(testUpdateWhenAddingIndex);
	CPPUNIT_TEST(testRecvOptimize);
	CPPUNIT_TEST(testRecvAlterIndex);
	CPPUNIT_TEST(testRecvCreateDictonary);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testRecvCreateTableSucc();
	void testRecvCreateTableOldCtrlFile();
	void testRecvDropTable();
	void testRecvTruncate();
	void testRecvRenameBefore();
	void testRecvRenameAfter();
	void testRecvAddIndexSucc();
	void testRecvAddIndexFail();
	void testRecvAddIndexReadonly();
	void testRecvAddIndexOnline();
	void testRecvDropIndex();
	void testMisc();
	void bug657();
	void bug700();
	void testUpdateWhenAddingIndex();
	void testRecvOptimize();
	void testRecvAlterIndex();
	void testRecvCreateDictonary();

private:
	void doAddIndexTest(LogType truncateOnThisLog, bool succ);
};

#endif

