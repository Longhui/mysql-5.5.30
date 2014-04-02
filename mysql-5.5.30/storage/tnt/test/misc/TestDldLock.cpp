#include "misc/TestDldLock.h"
#include <iostream>

const char* DldLockTestCase::getName() {
	return "Deadlock detected lock test";
}

const char* DldLockTestCase::getDescription() {
	return "Functional test for deadlock detected lock";
}

bool DldLockTestCase::isBig() {
	return false;
}

void DldLockTestCase::setUp() {
	m_lockTable = NULL;
}

void DldLockTestCase::tearDown() {
	m_lockTable = NULL;
}

void DldLockTestCase::init(uint maxLockNum, int timeoutMs) {
	m_lockTable = new DldLockTable(maxLockNum, timeoutMs);
}

void DldLockTestCase::cleanUp() {
	delete m_lockTable;
}

void DldLockTestCase::testSimpleLockUnlock() {
	init();

	u64 trxId = 1;
	const int maxLocks = 4096;
	DldLockOwner owner(trxId);

	for (int i = 0; i < maxLocks; i++) {
		if (i % 2) {
			CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->lock(&owner, i, TL_S));
		} else {
			CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->tryLock(&owner, i, TL_S));
		}

		CPPUNIT_ASSERT_EQUAL(true, m_lockTable->pickLock(i, true));
		CPPUNIT_ASSERT_EQUAL(true, m_lockTable->pickLock(i, false));
	}

	for (int i = maxLocks - 1; i >= 0; i--) {
		CPPUNIT_ASSERT_EQUAL(true, m_lockTable->unlock(&owner, i, TL_S));
		CPPUNIT_ASSERT_EQUAL(false, m_lockTable->pickLock(i, true));
	}
	
	cleanUp();
}

void DldLockTestCase::testLockMemory() {
	init();
	
	u64 trxId = 1;
	u64 key = 1;
	int count = 10000;
	DldLockOwner owner(trxId);
	u64 sp1 = owner.m_memctx->setSavepoint();
	CPPUNIT_ASSERT(DLD_LOCK_SUCCESS == m_lockTable->lock(&owner, key, TL_S));
	CPPUNIT_ASSERT(1 == owner.m_lockHoldingList.getSize());
	//第一个lock解锁
	CPPUNIT_ASSERT(true == m_lockTable->unlock(&owner, key, TL_S));
	CPPUNIT_ASSERT(sp1 == owner.m_memctx->setSavepoint());
	CPPUNIT_ASSERT(0 == owner.m_lockHoldingList.getSize());

	u64 sp2 = 0;
	for (int i = 1; i < count; i++) {
		sp2 = owner.m_memctx->setSavepoint();
		CPPUNIT_ASSERT(DLD_LOCK_SUCCESS == m_lockTable->lock(&owner, key + i, TL_S));
		CPPUNIT_ASSERT(i == owner.m_lockHoldingList.getSize());
	}
	u64 sp3 = owner.m_memctx->setSavepoint();
	//最后一个lock解锁
	CPPUNIT_ASSERT(true == m_lockTable->unlock(&owner, key + count - 1, TL_S));
	CPPUNIT_ASSERT((count - 2) == owner.m_lockHoldingList.getSize());
	CPPUNIT_ASSERT(sp3 != owner.m_memctx->setSavepoint());
	CPPUNIT_ASSERT(sp2 == owner.m_memctx->setSavepoint());

	for (int i = count - 2; i >= 1; i--) {
		CPPUNIT_ASSERT(true == m_lockTable->unlock(&owner, key + i, TL_S));
		CPPUNIT_ASSERT((i - 1) == owner.m_lockHoldingList.getSize());
		CPPUNIT_ASSERT(sp2 == owner.m_memctx->setSavepoint());
	}

	cleanUp();
}

void DldLockTestCase::testLockHoldingLock() {
	init();

	u64 trxId = 1;
	u64 testKey = 1;
	DldLockOwner owner(trxId);

	for (uint i = TL_X; i > TL_NO; i--) {
		// 加一个高模式的锁
		DldLockMode lockMode = (DldLockMode)i;
		CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->lock(&owner, testKey, lockMode));
		// 请求加低模式的锁都能成功
		for (uint j = i; j > TL_NO; j--) {
			DldLockResult result = m_lockTable->lock(&owner, testKey, (DldLockMode)j);
			CPPUNIT_ASSERT(DLD_LOCK_IMPLICITY == result || DLD_LOCK_SUCCESS == result);
			if (DLD_LOCK_SUCCESS == result) {
				CPPUNIT_ASSERT_EQUAL(true, m_lockTable->unlock(&owner, testKey, (DldLockMode)j));
			}
		}
		CPPUNIT_ASSERT_EQUAL(true, m_lockTable->unlock(&owner, testKey, lockMode));
	}

	cleanUp();
}

void DldLockTestCase::testLockTimeout() {
	int timeoutMs = 5000;
	init(1024, timeoutMs);

	u64 trxId1 = 1;
	u64 trxId2 = 2;
	u64 testLockKey = 1234;
	const uint maxLocks = 4096;
	DldLockOwner owner1(trxId1);
	DldLockOwner owner2(trxId2);

	// 用户1先加X锁
	CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->lock(&owner1, testLockKey, TL_X));

	std::cout << std::endl << std::endl << "The lock timeout test may take some time, please wait...";

	// 用户2再加X锁肯定都是需要等待的，并且会超时
	uint testTimes = 10;
	u64 waitTimeMs = 0;
	for (uint i = 0; i < testTimes; i++) {
		std::cout << ".";
		u64 begin = System::currentTimeMillis();
		DldLockResult result = m_lockTable->lock(&owner2, testLockKey, TL_X);
		CPPUNIT_ASSERT_EQUAL(DLD_LOCK_TIMEOUT, result);
		waitTimeMs += System::currentTimeMillis() - begin;
	}
	std::cout << std::endl << "done!" << std::endl;

	CPPUNIT_ASSERT_EQUAL(true, m_lockTable->unlock(&owner1, testLockKey, TL_X));

	CPPUNIT_ASSERT((double)waitTimeMs <= timeoutMs * testTimes * 1.1);
	CPPUNIT_ASSERT((double)waitTimeMs >= timeoutMs * testTimes * 0.9);

	cleanUp();
}

void DldLockTestCase::testUnlockAll() {
	init();

	u64 trxId = 1;
	const uint maxLocks = 4096;
	DldLockOwner owner(trxId);

	for (u64 i = 0; i < maxLocks; i++) {
		CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->lock(&owner, i, TL_S));
	}

	CPPUNIT_ASSERT_EQUAL(true, m_lockTable->releaseAllLocks(&owner));
	CPPUNIT_ASSERT_EQUAL((uint)0, owner.getHoldingList()->getSize());

	for (u64 i = 0; i < maxLocks; i++) {
		CPPUNIT_ASSERT(!m_lockTable->isLocked(&owner, i, TL_S));
	}
	
	cleanUp();
}

void DldLockTestCase::testLockConfict() {
	init();

	u64 testKey = 1;
	const u64 trxId1 = 1;
	DldLockOwner owner(trxId1);

	CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->lock(&owner, testKey, TL_X));

	const u64 trxId2 = 2;
	DldLockWaiter tester1(trxId2);
	tester1.setLockParameters(testKey, TL_S, m_lockTable);
	tester1.start();

	while (m_lockTable->getWaiterList()->getSize() == 0) {
		Thread::msleep(50);
	}
	// 此时应该已经加上更高级别的锁
	CPPUNIT_ASSERT_EQUAL(true, m_lockTable->hasLockImplicity(&owner, testKey, TL_S));
	CPPUNIT_ASSERT_EQUAL((uint)1, m_lockTable->getWaiterList()->getSize());

	CPPUNIT_ASSERT_EQUAL(true, m_lockTable->unlock(&owner, testKey, TL_X));

	tester1.join(-1);

	CPPUNIT_ASSERT_EQUAL((uint)0, m_lockTable->getWaiterList()->getSize());

	cleanUp();
}

void DldLockTestCase::testcheckDeadLock() {
	init();

	u64 testKey = 1;
	const u64 trxId1 = 1;
	DldLockOwner owner(trxId1);

	// 对第二个对象加X锁
	CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->lock(&owner, testKey + 1, TL_X));
	
	const u64 trxId2 = 2;
	DldLockDeadlockTester tester1(trxId2);
	//tester1.enableSyncPoint(SP_DLD_LOCK_LOCK_FIRST);

	tester1.setLockParameters(testKey, TL_X, m_lockTable);
	tester1.start();

	// 等待
	while (m_lockTable->getWaiterList()->getSize() == 0) {
		Thread::msleep(50);
	}
	CPPUNIT_ASSERT_EQUAL((uint)1, m_lockTable->getWaiterList()->getSize());

	//tester1.notifySyncPoint(SP_DLD_LOCK_LOCK_FIRST);

	// 对第一个对象加锁，此时检测到死锁
	CPPUNIT_ASSERT_EQUAL(DLD_LOCK_SUCCESS, m_lockTable->lock(&owner, testKey, TL_X));

	CPPUNIT_ASSERT_EQUAL((uint)2, owner.getHoldingList()->getSize());

	CPPUNIT_ASSERT_EQUAL(true, m_lockTable->releaseAllLocks(&owner));

	tester1.join(-1);

	CPPUNIT_ASSERT_EQUAL((uint)0, m_lockTable->getWaiterList()->getSize());

	cleanUp();
}

class DldLocker : public Thread {
public:
	DldLocker(DldLockOwner *lockOwner, DldLockTable *lockTable, DldLockMode lockMode, u64 key) : Thread("DldLocker") {
		m_lockOwner = lockOwner;
		m_lockTable = lockTable;
		m_lockMode = lockMode;
		m_lockKey = key;
	}

	~DldLocker() {}

	void run() {
		CPPUNIT_ASSERT(DLD_LOCK_SUCCESS == m_lockTable->lock(m_lockOwner, m_lockKey, m_lockMode));
		CPPUNIT_ASSERT(m_lockTable->unlock(m_lockOwner, m_lockKey, m_lockMode));
	}

public:
	u64          m_lockKey;
	DldLockMode  m_lockMode;
	DldLockTable *m_lockTable;
	DldLockOwner *m_lockOwner;
};

void DldLockTestCase::testWaitLock() {
	init();

	u64 key = 1001;
	DldLockMode lockMode = TL_X;

	TrxId trxId1 = 1005;
	DldLockOwner owner1(trxId1);

	TrxId trxId2 = 1006;
	DldLockOwner owner2(trxId2);

	CPPUNIT_ASSERT(DLD_LOCK_SUCCESS == m_lockTable->lock(&owner1, key, lockMode));
	DldLocker otherLocker(&owner2, m_lockTable, lockMode, key);
	otherLocker.enableSyncPoint(SP_DLD_LOCK_ENQUEUE_BEFORE_WAIT);
	otherLocker.start();
	otherLocker.joinSyncPoint(SP_DLD_LOCK_ENQUEUE_BEFORE_WAIT);
	CPPUNIT_ASSERT(m_lockTable->unlock(&owner1, key, lockMode));

	otherLocker.disableSyncPoint(SP_DLD_LOCK_ENQUEUE_BEFORE_WAIT);
	otherLocker.notifySyncPoint(SP_DLD_LOCK_ENQUEUE_BEFORE_WAIT);
	otherLocker.join(-1);

	cleanUp();
}

////////////////////////////////////////////////////////////////////////////////////////

void DldLockTester::setLockParameters(u64 lockKey, DldLockMode lockMode, DldLockTable *lockTable) {
	m_lockKey = lockKey;
	m_lockMode = lockMode;
	m_lockTable = lockTable;
}

void DldLockWaiter::run() {
	try {
		NTSE_ASSERT(DLD_LOCK_SUCCESS == m_lockTable->lock(dynamic_cast<DldLockOwner*>(this), 
			m_lockKey, m_lockMode));
		NTSE_ASSERT(m_lockTable->unlock(dynamic_cast<DldLockOwner*>(this), m_lockKey, m_lockMode));
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		NTSE_ASSERT(false);
	}
}

void DldLockDeadlockTester::run() {
	try {
		NTSE_ASSERT(DLD_LOCK_SUCCESS == m_lockTable->lock(
			dynamic_cast<DldLockOwner*>(this), m_lockKey, m_lockMode));
		
		//SYNCHERE(SP_DLD_LOCK_LOCK_FIRST);

		NTSE_ASSERT(DLD_LOCK_DEAD_LOCK == m_lockTable->lock(
			dynamic_cast<DldLockOwner*>(this), m_lockKey + 1, m_lockMode));

		NTSE_ASSERT(m_lockTable->releaseAllLocks(dynamic_cast<DldLockOwner*>(this)));
	} catch (NtseException &) {
		NTSE_ASSERT(false);
	}
}