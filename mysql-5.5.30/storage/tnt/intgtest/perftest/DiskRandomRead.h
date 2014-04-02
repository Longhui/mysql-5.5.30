/**
 * ������������ܲ���
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */
#ifndef _NTSETEST_DISK_RANDOMREAD_H_
#define _NTSETEST_DISK_RANDOMREAD_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "util/Thread.h"

using namespace ntse;


namespace ntseperf {

/** �ȵ����ݱ��� */
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

	/** ���������� */
	static u32 VOLUMN_RATIO;
	
private:
	void scanIndexOneTime();

private:
	bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	bool m_isVar;		/** �Ƿ�Ϊ�䳤�� */
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
