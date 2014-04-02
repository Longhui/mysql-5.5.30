#include "TestConfig.h"
#include "Test.h"

const char *ConfigTestCase::getName(void) {
	return "Config test";
}

const char *ConfigTestCase::getDescription(void) {
	return "Test Config";
}

bool ConfigTestCase::isBig() {
	return false;
}

void ConfigTestCase::setUp() {
}

void ConfigTestCase::tearDown() {
}

void ConfigTestCase::testWriteAndRead() {
	Config config;
	config.setBasedir("dbtestdir1");
	config.setTmpdir("dbtestdir2");
	config.setLogdir("dbtestdir3");
	config.m_backupBeforeRecover = false;
//	config.m_chkptInterval = 90;
	config.m_commonPoolSize = 100;
	config.m_directIo = true;
	config.m_enableMmsCacheUpdate = false;
	config.m_internalSessions = 125;
	config.m_localConfigs.m_accurateTblScan = true;
	config.m_localConfigs.setCompressSampleStrategy("sque");
	config.m_localConfigs.m_smplTrieBatchDelSize = 25;
	config.m_localConfigs.m_smplTrieCte = 20;
	config.m_localConfigs.m_tblSmplPct = 51;
	config.m_localConfigs.m_tblSmplWinDetectTimes = 52;
	config.m_localConfigs.m_tblSmplWinMemLevel = 25;
	config.m_localConfigs.m_tblSmplWinSize = 1024;
	config.m_logBufSize = 10240;
	config.m_logFileCntHwm = 20;
	config.m_logFileSize = 20480;
	config.m_logLevel = EL_PANIC;
	config.m_maxFlushPagesInScavenger = 21;
	config.m_maxSessions = 245;
	config.m_mmsSize = 21;
	config.m_pageBufSize = 20480;
	config.m_tlTimeout = 12;
	config.m_verifyAfterRecover = false;

	size_t bufSize = 0;
	byte *buf = config.write(&bufSize);
	Config *config1 = Config::read(buf, bufSize);
	CPPUNIT_ASSERT(config.isEqual(config1));

	delete [] buf;
	delete config1;
}

const char *TNTConfigTestCase::getName(void) {
	return "TNTConfig test";
}

const char *TNTConfigTestCase::getDescription(void) {
	return "Test TNTConfig";
}

bool TNTConfigTestCase::isBig() {
	return false;
}

void TNTConfigTestCase::setUp() {
}

void TNTConfigTestCase::tearDown() {
}

void TNTConfigTestCase::testWriteAndRead() {
	TNTConfig tntConfig;
	tntConfig.setNtseBasedir("dbtestdir1");
	tntConfig.setNtseTmpdir("dbtestdir2");
	tntConfig.setTxnLogdir("dbtestdir3");
	tntConfig.m_ntseConfig.m_backupBeforeRecover = false;
//	tntConfig.m_ntseConfig.m_chkptInterval = 90;
	tntConfig.m_ntseConfig.m_commonPoolSize = 100;
	tntConfig.m_ntseConfig.m_directIo = true;
	tntConfig.m_ntseConfig.m_enableMmsCacheUpdate = false;
	tntConfig.m_ntseConfig.m_internalSessions = 125;
	tntConfig.m_ntseConfig.m_localConfigs.m_accurateTblScan = true;
	tntConfig.m_ntseConfig.m_localConfigs.setCompressSampleStrategy("sque");
	tntConfig.m_ntseConfig.m_localConfigs.m_smplTrieBatchDelSize = 25;
	tntConfig.m_ntseConfig.m_localConfigs.m_smplTrieCte = 20;
	tntConfig.m_ntseConfig.m_localConfigs.m_tblSmplPct = 51;
	tntConfig.m_ntseConfig.m_localConfigs.m_tblSmplWinDetectTimes = 52;
	tntConfig.m_ntseConfig.m_localConfigs.m_tblSmplWinMemLevel = 25;
	tntConfig.m_ntseConfig.m_localConfigs.m_tblSmplWinSize = 1024;
	tntConfig.m_ntseConfig.m_logBufSize = 10240;
	tntConfig.m_ntseConfig.m_logFileCntHwm = 20;
	tntConfig.m_ntseConfig.m_logFileSize = 20480;
	tntConfig.m_ntseConfig.m_logLevel = EL_PANIC;
	tntConfig.m_ntseConfig.m_maxFlushPagesInScavenger = 21;
	tntConfig.m_ntseConfig.m_maxSessions = 245;
	tntConfig.m_ntseConfig.m_mmsSize = 21;
	tntConfig.m_ntseConfig.m_pageBufSize = 20480;
	tntConfig.m_ntseConfig.m_tlTimeout = 12;
	tntConfig.m_ntseConfig.m_verifyAfterRecover = false;
	tntConfig.m_backupBeforeRec = false;
	tntConfig.m_dumpBeforeClose = true;
	tntConfig.setTntBasedir("tntbasedir");
	tntConfig.setTntDumpdir("tntdumpdir");
	tntConfig.m_dumpInterval = 25;
	tntConfig.m_dumponRedoSize = 20456;
	tntConfig.m_dumpReserveCnt = 8;
	tntConfig.m_purgeAfterRecover = false;
	tntConfig.m_purgeBeforeClose = false;
	tntConfig.m_purgeEnough = 90;
	tntConfig.m_purgeInterval = 36;
	tntConfig.m_purgeThreshold = 92;
	tntConfig.m_tntBufSize = 51200;
	tntConfig.m_tntLogLevel = EL_ERROR;
	tntConfig.m_trxFlushMode = TFM_FLUSH_NOSYNC;
	tntConfig.m_txnLockTimeout = 25;
	tntConfig.m_verifyAfterRec = false;
	tntConfig.m_verpoolCnt = 8;
	tntConfig.m_verpoolFileSize = 20480;

	size_t bufSize = 0;
	byte *buf = tntConfig.write(&bufSize);
	TNTConfig *tntConfig1 = TNTConfig::read(buf, bufSize);
	CPPUNIT_ASSERT(tntConfig.isEqual(tntConfig1));

	delete [] buf;
	delete tntConfig1;
}
