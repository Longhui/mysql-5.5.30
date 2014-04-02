/**
 * 数据库备份性能测试
 *
 * @author 苏斌(bsu@corp.netease.com naturally@163.org)
 */

#ifndef _NTSETEST_BACKUP_H_
#define _NTSETEST_BACKUP_H_

#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "util/Thread.h"

using namespace ntse;

namespace ntseperf {

class BusyGuy;
class DBBackupTest : public EmptyTestCase {
public:
	DBBackupTest(bool warmup, uint readOps, uint writeOps);
	~DBBackupTest();

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual void setUp();
	virtual void tearDown();

	void randomDML(bool scanOp, uint opid, uint threadId);
	void stopThreads();

	bool isRunning() {
		return m_running;
	}

	bool isHeavyLoading() const {
		return (m_readOps >= 500 || m_writeOps >= 100);
	}

private:
	void balanceThreadNums();
	void dbBackup();

private:
	bool m_warmup;				/** 测试用例是否要预热 */
	uint m_readOps;				/** 负载每秒钟读取操作数 */
	uint m_writeOps;			/** 负载每秒钟修改操作数 */

	BusyGuy **m_troubleThreads;	/** 制造负载的线程 */
	u16 m_threadNum;			/** 启动的线程数 */
	bool m_running;				/** 用于控制子线程状态的变量 */
	char m_backupDir[255];		/** 备份目录 */

private:
	static const u16 DEFAULT_THREADS = 10;		/** 默认执行线程数 */
	static const u16 THREADS_MAX_READS = 100;	/** 每个线程最大读操作数 */
	static const u16 THREADS_MAX_WRITES = 50;	/** 每个线程最大写操作数 */
	static const u64 TESTCASE_DATA_SIZE = 200 * 1024 * 1024;	/** 用例使用的数据量 */

public:
	static const uint MAX_ID = 100000;		/** 用例使用最大键值数 */
};


/**
 * 模拟负载线程
 */
class BusyGuy : public Thread {
public:
	BusyGuy(uint threadId, DBBackupTest *testcase, uint readOps, uint writeOps)
		: Thread("DBBackupTest"), m_testcase(testcase), m_readOps(readOps), m_writeOps(writeOps), m_threadId(threadId) {}

	void run();

private:
	void helpThreadOperation();

private:
	DBBackupTest *m_testcase;
	uint m_readOps;
	uint m_writeOps;
	uint m_threadId;
};


}

#endif