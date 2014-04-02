/**
 * 内存随机读性能测试
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_MEMORY_RANDOMREAD_H_
#define _NTSETEST_MEMORY_RANDOMREAD_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "util/Thread.h"
#include "Random.h"

using namespace ntse;

namespace ntseperf {

/** 数据量缩减比例 */
#define MEM_RANDOM_READ_REDUCE_RATIO 10
/** 热点数据比例 */
#define DISK_RANDOM_READ_HOT_PERCENT 0.1f

#define MEM_RANDOM_READ_MMS_MULTIPLE 10

class MemoryRandomReadThread;
class MemoryRandomReadTest: public EmptyTestCase {
public:
	MemoryRandomReadTest(bool useMms, bool isVar, bool isZipf, int threadCount, int loopCount);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	void setUp();
	void tearDown();

	void randomRead(int count);

private:
	void zipfRandomInit();
	void zipfRandomDestroy();
	void zipfRandom();
	u64 scanIndexOneTime();

	bool m_useMms;		/** 表是否使用Mms */
	bool m_isVar;		/** 是否为变长表 */
	bool m_zipf;		/** 数据按照zipf分布 */
	ZipfRandom *m_zipfRandom; /** zipf随机产生器 */
	int	m_nrThreads;
	int m_loopCount;
	MemoryRandomReadThread **m_threads;
	Atomic<int> *m_topTenCount;
};

class MemoryRandomReadThread : public Thread {
public:
	MemoryRandomReadThread(MemoryRandomReadTest *testcase, int loopCount) 
		: Thread("MemoryRandomReadThread"), m_testcase(testcase), m_loopCount(loopCount) {
	}

	void run() {
		m_testcase->randomRead(m_loopCount);
	}
private:
	MemoryRandomReadTest *m_testcase;
	int	m_loopCount;
};

}

#endif // _NTSETEST_MEMORY_RANDOMREAD_H_
