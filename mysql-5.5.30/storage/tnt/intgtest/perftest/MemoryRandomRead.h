/**
 * �ڴ���������ܲ���
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_MEMORY_RANDOMREAD_H_
#define _NTSETEST_MEMORY_RANDOMREAD_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "util/Thread.h"
#include "Random.h"

using namespace ntse;

namespace ntseperf {

/** �������������� */
#define MEM_RANDOM_READ_REDUCE_RATIO 10
/** �ȵ����ݱ��� */
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

	bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
	bool m_zipf;		/** ���ݰ���zipf�ֲ� */
	ZipfRandom *m_zipfRandom; /** zipf��������� */
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
