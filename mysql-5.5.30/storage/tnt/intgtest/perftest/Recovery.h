/**
 * ���ݿ�ָ����ܲ���
 *
 * @author �ձ�(bsu@corp.netease.com naturally@163.org)
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
	u64 m_ckLsn;				/** �������LSN */
	u64 m_tailLsn;				/** �����ָ�����־LSN */
	u64 m_dataSize;				/** �������������С */

	Graffiti **m_dmlThreads;		/** ִ�����ݿ��޸Ĳ������߳� */

public:
	static const int TOTAL_DML_UPDATE_TIMES = 30000;
	static const int DML_THREAD_NUM = 10;
	static const int MAX_ID = 1000000;
};


/**
 * ģ������߳�
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