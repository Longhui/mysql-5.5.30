/**
 * TNT事务管理模块测试
 * 
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#include "trx/TestTNTTransaction.h"
#include "Test.h"

const char* TNTTransactionTestCase::getName() {
	return "TNT Transaction test";
}

const char* TNTTransactionTestCase::getDescription() {
	return "Functional test for TNT transaction";
}

 
bool TNTTransactionTestCase::isBig() {
	return false;
}

void TNTTransactionTestCase::setUp() {
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	m_config.setNtseBasedir("testdb");
	m_config.setTntBasedir("testdb");
	m_config.setTntDumpdir("testdb");
	m_config.m_tntLogLevel = EL_WARN;
	m_config.m_tntBufSize = 50;
	//m_config.m_tntLogfileSize = 1024 * 1024 * 16;
	m_config.m_ntseConfig.m_logFileSize = 16 << 20;
	m_config.m_verpoolCnt = 2;

	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, true, 0));
	m_conn = m_db->getNtseDb()->getConnection(false, "TNTTransactionTestCase");
	m_trxSys = m_db->getTransSys();
}

void TNTTransactionTestCase::tearDown() {
	m_trxSys = NULL;

	m_db->close(false, false);
	delete m_db;
	m_db = NULL;

	File dir("testdb");
	dir.rmdir(true);
}

void TNTTransactionTestCase::testCommon() {
	vector<TNTTransaction*> trxArr;
	
	for (uint i = 0; i < 512; i++) {
		TNTTransaction *trx = m_trxSys->allocTrx();
		CPPUNIT_ASSERT_EQUAL(TRX_NOT_START, trx->getTrxState());
		CPPUNIT_ASSERT_EQUAL(false, trx->isTrxStarted());

		// 开启事务
		trx->startTrxIfNotStarted();
		CPPUNIT_ASSERT_EQUAL(TRX_ACTIVE, trx->getTrxState());
		CPPUNIT_ASSERT_EQUAL(true, trx->isTrxStarted());

		CPPUNIT_ASSERT(!trx->getReadView());
		CPPUNIT_ASSERT(!trx->getGlobalReadView());

		// 测试分配视图
		EXCPT_OPER(trx->trxAssignReadView());
		CPPUNIT_ASSERT(trx->getReadView());
		CPPUNIT_ASSERT(trx->getGlobalReadView());

		m_trxSys->closeReadViewForMysql(trx);
		CPPUNIT_ASSERT(!trx->getReadView());
		CPPUNIT_ASSERT(!trx->getGlobalReadView());

		// 测试logging
		CPPUNIT_ASSERT_EQUAL(true, trx->isLogging());
		trx->disableLogging();
		CPPUNIT_ASSERT_EQUAL(false, trx->isLogging());
		trx->enableLogging();
		CPPUNIT_ASSERT_EQUAL(true, trx->isLogging());

		trxArr.push_back(trx);
	}

	for (uint i = 0; i < trxArr.size(); i++) {
		TNTTransaction *trx = trxArr[i];

		// 当前的最小活跃事务ID就是我的事务ID，因为在我之前的事务都已经提交
		// CPPUNIT_ASSERT_EQUAL(trx->getTrxId(), m_trxSys->findMinReadViewInActiveTrxs());

		// 提交事务
		trx->commitTrx(CS_NORMAL);
		CPPUNIT_ASSERT_EQUAL(TRX_NOT_START, trx->getTrxState());
		CPPUNIT_ASSERT_EQUAL(INVALID_TRX_ID, trx->getTrxId());
		CPPUNIT_ASSERT(trx->commitCompleteForMysql());
		m_trxSys->freeTrx(trx);

		trxArr[i] = NULL;
	}
}

void TNTTransactionTestCase::testXA() {
	TNTTransaction *trx = NULL;
	
	for (uint i = 0; i < 512; i++) {
		trx = m_trxSys->allocTrx();
		CPPUNIT_ASSERT_EQUAL(TRX_NOT_START, trx->getTrxState());

		trx->startTrxIfNotStarted();
		trx->prepareForMysql();
		CPPUNIT_ASSERT_EQUAL(TRX_PREPARED, trx->getTrxState());
		
		trx->commitTrx(CS_NORMAL);
		CPPUNIT_ASSERT_EQUAL(TRX_NOT_START, trx->getTrxState());
		m_trxSys->freeTrx(trx);
	}
}

void TNTTransactionTestCase::testVisable() {
	// 创建一批活跃事务
	vector<TNTTransaction*> trxArr;
	vector<TrxId> trxIdArr;

	for (uint i = 0; i < 16; i++) {
		TNTTransaction *trx = m_trxSys->allocTrx();
		trx->startTrxIfNotStarted();
		CPPUNIT_ASSERT(trx->isTrxStarted());
		trxArr.push_back(trx);
		trxIdArr.push_back(trx->getTrxId());
	}
	
	// 再新建一个事务，
	TNTTransaction *testTrx = m_trxSys->allocTrx();
	testTrx->startTrxIfNotStarted();
	testTrx->trxAssignReadView();

	// 事务对自己的修改总是可见
	CPPUNIT_ASSERT(testTrx->trxJudgeVisible(testTrx->getTrxId()));
	
	// 之前的未提交事务的修改都不可见
	for (uint i = 0; i < trxArr.size(); i++) {
		CPPUNIT_ASSERT(!testTrx->trxJudgeVisible(trxIdArr[i]));
	}
	m_trxSys->closeReadViewForMysql(testTrx);

	// 提交之前的活跃事务
	for (uint i = 0; i < trxArr.size(); i++) {
		TNTTransaction *t = trxArr[i];
		t->commitTrx(CS_NORMAL);
	}
	
	// 再次分配视图
	testTrx->trxAssignReadView();

	// 已提交事务都可见
	for (uint i = 0; i < trxArr.size(); i++) {
		CPPUNIT_ASSERT(testTrx->trxJudgeVisible(trxIdArr[i]));
	}

	// 已提交事务对新建事务都可见
	testTrx->commitTrx(CS_NORMAL);
	m_trxSys->freeTrx(testTrx);
	testTrx = m_trxSys->allocTrx();
	testTrx->startTrxIfNotStarted();
	testTrx->trxAssignReadView();
	for (uint i = 0; i < trxArr.size(); i++) {
		CPPUNIT_ASSERT(testTrx->trxJudgeVisible(trxIdArr[i]));
	}

	// 回收所有事务
	testTrx->commitTrx(CS_NORMAL);
	m_trxSys->freeTrx(testTrx);
	testTrx = NULL;

	for (uint i = 0; i < trxArr.size(); i++) {
		m_trxSys->freeTrx(trxArr[i]);
		trxArr[i] = NULL;
	}
}

void TNTTransactionTestCase::testTLock() {
	CPPUNIT_ASSERT(NULL != m_trxSys->getLockSys());

	try {
		vector<TNTTransaction*> trxArr;
		for (uint i = 0; i < 16; i++) {
			TNTTransaction *trx = m_trxSys->allocTrx();
			trx->startTrxIfNotStarted();
			CPPUNIT_ASSERT_EQUAL(true, trx->isTrxStarted());
			trxArr.push_back(trx);
		}

		// 测试表锁
		TableId testTbId = 1;
		for (uint i = 0; i < trxArr.size(); i++) {
			TNTTransaction *trx = trxArr[i];
			// 加S锁
			EXCPT_OPER(trx->lockTable(TL_S, testTbId));
			if (0 == i) {// 之前没有人加过跟我不兼容的锁
				CPPUNIT_ASSERT_EQUAL(true, trx->tryLockTable(TL_X, testTbId));

				// 测试锁的可重入
				for (uint j = 0; j < 100; j++) {
					CPPUNIT_ASSERT_EQUAL(true, trx->tryLockTable(TL_S, testTbId));
					trx->lockTable(TL_S, testTbId);
					CPPUNIT_ASSERT_EQUAL(true, trx->tryLockTable(TL_X, testTbId));
					trx->lockTable(TL_X, testTbId);
				}

				CPPUNIT_ASSERT_EQUAL(true, trx->unlockTable(TL_X, testTbId));
			} else {// 之前已经有不兼容的锁，尝试加X锁失败
				CPPUNIT_ASSERT_EQUAL(false, trx->tryLockTable(TL_X, testTbId));
			}
		}

		// 测试行锁
		RowId testRowId = 1122;
		for (uint i = 0; i < trxArr.size(); i++) {		
			TNTTransaction *trx = trxArr[i];
			// 兼容锁
			EXCPT_OPER(trx->lockRow(TL_S, testRowId, testTbId));
			if (0 == i) {// 之前没有人加过跟我不兼容的锁
				CPPUNIT_ASSERT_EQUAL(true, trx->tryLockRow(TL_X, testRowId, testTbId));
				CPPUNIT_ASSERT_EQUAL(true, trx->unlockRow(TL_X, testRowId, testTbId));
			} else {// 之前已经有不兼容的锁，尝试加X锁失败
				CPPUNIT_ASSERT_EQUAL(true, trx->pickRowLock(testRowId, testTbId));
				CPPUNIT_ASSERT_EQUAL(false, trx->tryLockRow(TL_X, testRowId, testTbId));
			}
		}

		// 测试自增序列锁
		u64 testAutoIncr = 1;
		for (uint i = 0; i < trxArr.size(); i++) {		
			TNTTransaction *trx = trxArr[i];
			// 兼容锁
			EXCPT_OPER(trx->lockAutoIncr(TL_S, testAutoIncr));
			if (0 == i) {// 之前没有人加过跟我不兼容的锁
				CPPUNIT_ASSERT_EQUAL(true, trx->tryLockAutoIncr(TL_X, testAutoIncr));
				CPPUNIT_ASSERT_EQUAL(true, trx->unlockAutoIncr(TL_X, testAutoIncr));
			} else {// 之前已经有不兼容的锁，尝试加X锁失败
				CPPUNIT_ASSERT_EQUAL(false, trx->tryLockAutoIncr(TL_X, testAutoIncr));
			}
		}

		for (uint i = 0; i < trxArr.size(); i++) {
			TNTTransaction *trx = trxArr[i];
			trx->releaseLocks();
			m_trxSys->freeTrx(trx);
		}
	} catch (NtseException &e) {
		printf("Error: %s\n", e.getMessage());
		NTSE_ASSERT(false);
	}
}

void TNTTransactionTestCase::testReadOnly() {
	TNTTransaction *trx = m_trxSys->allocTrx();
	trx->markSqlStatEnd();
	CPPUNIT_ASSERT(trx->getTrxLastLsn() == INVALID_LSN);
	CPPUNIT_ASSERT(trx->getLastStmtLsn() == INVALID_LSN);
	trx->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(trx->isReadOnly());
	trx->commitTrx(CS_NORMAL);
	CPPUNIT_ASSERT(trx->isReadOnly());
	m_trxSys->freeTrx(trx);

	trx = m_trxSys->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(trx->isReadOnly());
	trx->rollbackTrx(RBS_NORMAL);
	CPPUNIT_ASSERT(trx->isReadOnly());
	m_trxSys->freeTrx(trx);

	trx = m_trxSys->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(trx->isReadOnly());
	trx->rollbackLastStmt(RBS_NORMAL);
	trx->markSqlStatEnd();
	CPPUNIT_ASSERT(trx->getTrxLastLsn() == INVALID_LSN);
	CPPUNIT_ASSERT(trx->getLastStmtLsn() == INVALID_LSN);
	CPPUNIT_ASSERT(trx->isReadOnly());
	m_trxSys->freeTrx(trx);

	trx = m_trxSys->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(trx->isReadOnly());
	trx->prepareForMysql();
	trx->markSqlStatEnd();
	CPPUNIT_ASSERT(trx->getTrxLastLsn() == INVALID_LSN);
	CPPUNIT_ASSERT(trx->getLastStmtLsn() == INVALID_LSN);
	CPPUNIT_ASSERT(trx->isReadOnly());
	m_trxSys->freeTrx(trx);
}

void TNTTransactionTestCase::testIsActive() {
	TrxId trxId1;
	TNTTransaction *trx1 = m_trxSys->allocTrx();
	trx1->startTrxIfNotStarted(m_conn);
	trxId1 = trx1->getTrxId();
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trx1->getTrxId()));
	trx1->prepareForMysql();
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trx1->getTrxId()));
	trx1->commitTrx(CS_NORMAL);
	CPPUNIT_ASSERT(!m_trxSys->isTrxActive(trx1->getTrxId()));
	m_trxSys->freeTrx(trx1);

	TNTTransaction *trx2 = m_trxSys->allocTrx();
	trx2->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(!m_trxSys->isTrxActive(trxId1));
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trx2->getTrxId()));
	CPPUNIT_ASSERT(trx2->rollbackTrx(RBS_NORMAL));
	//重复调用rollbackTrx
	CPPUNIT_ASSERT(trx2->rollbackTrx(RBS_NORMAL));
	CPPUNIT_ASSERT(!m_trxSys->isTrxActive(trx2->getTrxId()));
	m_trxSys->freeTrx(trx2);

	TNTTransaction *trx3 = m_trxSys->allocTrx();
	trx3->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trx3->getTrxId()));
	CPPUNIT_ASSERT(trx3->rollbackLastStmt(RBS_NORMAL));
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trx3->getTrxId()));
	trx3->commitTrx(CS_NORMAL);
	//重复调用rollbackLastStatement
	CPPUNIT_ASSERT(trx3->rollbackLastStmt(RBS_NORMAL));
	CPPUNIT_ASSERT(!m_trxSys->isTrxActive(trx3->getTrxId()));
	m_trxSys->freeTrx(trx3);
}

void TNTTransactionTestCase::testGetActiveTrxs() {
	TNTTransaction* trxs[3];

	trxs[0] = m_trxSys->allocTrx();
	trxs[0]->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trxs[0]->getTrxId()));
	trxs[0]->prepareForMysql();
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trxs[0]->getTrxId()));

	trxs[1] = m_trxSys->allocTrx();
	trxs[1]->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trxs[1]->getTrxId()));

	trxs[2] = m_trxSys->allocTrx();
	trxs[2]->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trxs[2]->getTrxId()));
	trxs[2]->rollbackLastStmt(RBS_NORMAL);
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trxs[2]->getTrxId()));

	TNTTransaction *trx = m_trxSys->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trx->getTrxId()));
	trx->prepareForMysql();
	CPPUNIT_ASSERT(m_trxSys->isTrxActive(trx->getTrxId()));
	trx->commitTrx(CS_NORMAL);
	CPPUNIT_ASSERT(!m_trxSys->isTrxActive(trx->getTrxId()));

	vector<TrxId> trxIds;
	m_trxSys->getActiveTrxIds(&trxIds);
	CPPUNIT_ASSERT(trxIds.size() == 3);
	m_trxSys->freeTrx(trx);
	for (uint i = 0; i < trxIds.size(); i++) {
		CPPUNIT_ASSERT(trxs[2 - i]->getTrxId() == trxIds[i]);
		m_trxSys->freeTrx(trxs[2 - i]);
	}
}