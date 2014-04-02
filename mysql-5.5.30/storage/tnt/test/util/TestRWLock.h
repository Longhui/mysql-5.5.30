/**
 * <code>RWLock</code>�Ĳ���
 *
 * @author  ������(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#ifndef _NTSE_TESTNEWRWLOCK_H_
#define _NTSE_TESTNEWRWLOCK_H_

#include <cppunit/extensions/HelperMacros.h>

class RWLockTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RWLockTestCase);
	CPPUNIT_TEST(testRWLock);
	CPPUNIT_TEST(testRWLockGuard);
	CPPUNIT_TEST(testRWLockConflictGain);
	CPPUNIT_TEST(testPrintUsage);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:	
	void testRWLock();
	void testRWLockGuard();	
	void testRWLockConflictGain();
	void testPrintUsage();
};

class RWLockBigTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RWLockBigTestCase);

	CPPUNIT_TEST(testRWLockSTPerf);	
	CPPUNIT_TEST(testRWLockMTAllReadPerf);
	CPPUNIT_TEST(testRWLockMTPerf);
	CPPUNIT_TEST(testRWLockMTTimedPerf);
	
	CPPUNIT_TEST(testRWLockVsInnoRWLock);
	CPPUNIT_TEST(testRWLockMTStable);
	CPPUNIT_TEST(testAllRead);
	CPPUNIT_TEST(testAllWrite);
	CPPUNIT_TEST(testPercentWrite);
	CPPUNIT_TEST(testPercentWritePreferWrite);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

	#ifdef NTSE_MEM_CHECK
	static const int NUM_THREADS = 100;/* �����߳���Ŀ */
	#else
	static const int NUM_THREADS = 1000;/* �����߳���Ŀ */
	#endif
	static const int NUM_TEST = 95;		/* ����RWLock��Ŀ */
	static int *testNumArr;	/* ���Զ��̵߳Ĺ��������� */
	static const int NUM_WLOCK_CHECK_VALUE = 87658; /* д������ʱ�ļ��ֵ */
	static const int NUM_RLOCK_CHECK_VALUE = 41234; /* д������ʱ�ļ��ֵ */
	static const int USE_RWLOCK = 0; 
	static const int USE_INNO_RWLOCK = 1;
protected:

	
	void testRWLockSTPerf();
	void testRWLockMTAllReadPerf();
	void testRWLockMTPerf();
	void testRWLockMTTimedPerf();

	void testRWLockVsInnoRWLock();
	void testRWLockMTStable();
	
	void testAllRead();
	void testAllWrite();
	void testPercentWrite();
	void testPercentWritePreferWrite();


private:
	void doRWLockMTTest(int totalTime, int writePercent, int timedPercent, int timeoutMs);
	void doRWLockMTTest(int totalTime, int runs, int writePercent, int timedPercent, int timeoutMs, int useLockType = USE_RWLOCK, bool preferWrite = false);
	
};


#endif
