
#ifndef _NTSETEST_DLD_LOCK_H_
#define _NTSETEST_DLD_LOCK_H_

#include <cppunit/extensions/HelperMacros.h>
#include "misc/DLDLockTable.h"
#include "util/Thread.h"

using namespace tnt;

class DldLockTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(DldLockTestCase);
	CPPUNIT_TEST(testSimpleLockUnlock);
	CPPUNIT_TEST(testLockMemory);
	CPPUNIT_TEST(testLockHoldingLock);
	CPPUNIT_TEST(testUnlockAll);
	CPPUNIT_TEST(testLockConfict);
	CPPUNIT_TEST(testLockTimeout);
	CPPUNIT_TEST(testcheckDeadLock);
	CPPUNIT_TEST(testWaitLock);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testSimpleLockUnlock();
	void testLockMemory();
	void testLockHoldingLock();
	void testUnlockAll();
	void testLockConfict();
	void testLockTimeout();
	void testcheckDeadLock();
	void testWaitLock();

private:
	void init(uint maxLockNum = 1024, int timeoutMs = 5000);
	void cleanUp();

	DldLockTable *m_lockTable;
};

class DldLockTester : public Thread, public DldLockOwner {
public:
	DldLockTester(u64 trxId) : Thread("DldLockTester"), DldLockOwner(trxId) {
	}
	virtual ~DldLockTester() {}

	void setLockParameters(u64 lockKey, DldLockMode lockMode, DldLockTable *lockTable);

public:
	u64 m_lockKey;
	DldLockMode m_lockMode;
	DldLockTable *m_lockTable;
};

class DldLockWaiter : public DldLockTester {
public:
	DldLockWaiter(u64 trxId) : DldLockTester(trxId) {}
	~DldLockWaiter() {}
	void run();
};

class DldLockDeadlockTester : public DldLockTester {
public:
	DldLockDeadlockTester(u64 trxId) : DldLockTester(trxId) {}
	~DldLockDeadlockTester() {}
	void run();
};

#endif