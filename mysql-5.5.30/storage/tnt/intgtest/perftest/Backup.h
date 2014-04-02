/**
 * ���ݿⱸ�����ܲ���
 *
 * @author �ձ�(bsu@corp.netease.com naturally@163.org)
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
	bool m_warmup;				/** ���������Ƿ�ҪԤ�� */
	uint m_readOps;				/** ����ÿ���Ӷ�ȡ������ */
	uint m_writeOps;			/** ����ÿ�����޸Ĳ����� */

	BusyGuy **m_troubleThreads;	/** ���츺�ص��߳� */
	u16 m_threadNum;			/** �������߳��� */
	bool m_running;				/** ���ڿ������߳�״̬�ı��� */
	char m_backupDir[255];		/** ����Ŀ¼ */

private:
	static const u16 DEFAULT_THREADS = 10;		/** Ĭ��ִ���߳��� */
	static const u16 THREADS_MAX_READS = 100;	/** ÿ���߳����������� */
	static const u16 THREADS_MAX_WRITES = 50;	/** ÿ���߳����д������ */
	static const u64 TESTCASE_DATA_SIZE = 200 * 1024 * 1024;	/** ����ʹ�õ������� */

public:
	static const uint MAX_ID = 100000;		/** ����ʹ������ֵ�� */
};


/**
 * ģ�⸺���߳�
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