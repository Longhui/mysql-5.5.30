/**
 * 数据库恢复性能测试
 *
 * @author 苏斌(bsu@corp.netease.com naturally@163.org)
 */

#ifndef _NTSETEST_RECOVERY_H_
#define _NTSETEST_RECOVERY_H_

#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "util/Thread.h"

using namespace ntse;

namespace ntseperf {

class Graffiti;
class DBRecoveryTest : public EmptyTestCase {
public:
	DBRecoveryTest(u64 dataSize);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual u64 getDataSize();
	virtual void setUp();
	virtual void tearDown();

	void randomDMLModify(uint threadNo, uint opid);

private:
	void openDB(int recover = 0);
	void closeDB(bool normalClose = true);
	void stopThreads();

private:
	u64 m_ckLsn;				/** 做检查点的LSN */
	u64 m_tailLsn;				/** 结束恢复的日志LSN */
	u64 m_dataSize;				/** 传入的数据量大小 */

	Graffiti **m_dmlThreads;		/** 执行数据库修改操作的线程 */

public:
	static const int TOTAL_DML_UPDATE_TIMES = 30000;
	static const int DML_THREAD_NUM = 10;
	static const int MAX_ID = 1000000;
};


/**
 * 模拟操作线程
 */
class Graffiti : public Thread {
public:
	Graffiti(uint threadId, DBRecoveryTest *testcase)
		: Thread("DBRecoveryTest"), m_testcase(testcase), m_threadId(threadId) {}

	void run();

private:
	DBRecoveryTest *m_testcase;
	uint m_threadId;
};

}

#endif