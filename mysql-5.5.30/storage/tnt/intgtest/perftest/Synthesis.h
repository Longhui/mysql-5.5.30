/**
 * 综合性能测试
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */
#ifndef _NTSETEST_SYNTHESIS_H_
#define _NTSETEST_SYNTHESIS_H_


#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "util/Thread.h"

using namespace ntse;


namespace ntseperf {

#define SYNTHESIS_VOLUMN_RATIO 5

class SynthesisThread;
class SynthesisTest: public EmptyTestCase {
public:
	SynthesisTest(bool isFixLen, bool useBlob, int threadCount, int taskCount);

	string getName() const;
	string getDescription() const;
	virtual string getCachedName() const;
	
	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	void setUp();
	void tearDown();

	void testLongCharTable();
	void testAccountTable();
	void testBlogTable();

private: 
	void scanIndexOneTime();

private:
	bool m_isFixLen;
	bool m_useBlob;		/** 是否使用大对象 */
	int	m_nrThreads;
	int m_taskCount;
	SynthesisThread **m_threads;
	int m_maxId;
};

class SynthesisThread : public Thread {
public:
	SynthesisThread(SynthesisTest *testCase, bool isFixLen, bool useBlob) 
		: Thread("SynthesisThread"), m_testCase(testCase), m_isFixLen(isFixLen), m_useBlob(useBlob) {
	}

	void run();

private:
	SynthesisTest *m_testCase;
	bool m_isFixLen;
	bool m_useBlob;
};

}

#endif // _NTSETEST_MEMORY_INDEXSCAN_H_
