/**
 * 磁盘随机读性能测试
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_DISK_RANDOMREAD_H_
#define _NTSETEST_DISK_RANDOMREAD_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "util/Thread.h"

using namespace ntse;


namespace ntseperf {

/** 热点数据比例 */
#define DISK_RANDOM_READ_HOT_PERCENT 0.1f

class DiskRandomReadThread;
class DiskRandomReadTest: public EmptyTestCase {
public:
	DiskRandomReadTest(bool useMms, bool isVar, int threadCount, int loopCount);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	void setUp();
	void tearDown();
	void randomRead(int count);

	/** 数据量比例 */
	static u32 VOLUMN_RATIO;
	
private:
	void scanIndexOneTime();

private:
	bool m_useMms;		/** 表是否使用Mms */
	bool m_isVar;		/** 是否为变长表 */
	DiskRandomReadThread **m_threads;
	int m_nrThreads;
	int m_loopCount;
};

class DiskRandomReadThread : public Thread {
public:
	DiskRandomReadThread(DiskRandomReadTest *testcase, int loopCount) 
		: Thread("DiskRandomReadThread"), m_testcase(testcase), m_loopCount(loopCount) {
	}

	void run() {
		m_testcase->randomRead(m_loopCount);
	}

private:
	DiskRandomReadTest *m_testcase;
	int m_loopCount;
};

}

#endif // _NTSETEST_DISK_RANDOMREAD_H_
