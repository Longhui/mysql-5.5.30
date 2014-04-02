#include "misc/TestTxnLog.h"
#include "Test.h"
#include "util/File.h"
#include "util/Thread.h"
#include "misc/ControlFile.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "api/Table.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
using namespace std;

static bool isLogEntryEq(const LogEntry *e1, const LogEntry *e2);
static LsnType writeLog(Txnlog *log, const LogEntry *e);
static LogEntry* makeCpstLogEntry(u16 txnId, LogType logType, u16 tableId, size_t size, LsnType targetLsn);
static LogEntry* makeLogEntry(u16 txnId, LogType logType, u16 tableId, size_t size);
static void freeLogEntry(LogEntry *e);
static string makeFileName(string basename, uint index);

const char* TxnLogTestCase::getName() {
	return "TxnLog basic tests";
}
const char* TxnLogTestCase::getDescription() {
	return "Test log create/write/read/flush";
}
bool TxnLogTestCase::isBig() {
	return false;
}
void TxnLogTestCase::setUp() {
	m_config.m_logFileSize = Limits::PAGE_SIZE * 9;
	m_config.m_logBufSize = Limits::PAGE_SIZE * 6;
	m_config.setBasedir("testtxnlog");
	File dir(m_config.m_basedir);
	dir.rmdir(true);
	dir.mkdir();
}

void TxnLogTestCase::tearDown() {
	File(m_config.m_basedir).rmdir(true);
}

void TxnLogTestCase::init() {
	Database::drop(m_config.m_basedir);
	EXCPT_OPER(m_db = Database::open(&m_config, true));
	m_db->setCheckpointEnabled(false);
	m_txnLog = m_db->getTxnlog();
}
void TxnLogTestCase::destroy() {
	//m_db->setCheckpointEnabled(true);
	m_db->close();
	delete m_db;
	m_db = 0;
	Database::drop(m_config.m_basedir);
}
void TxnLogTestCase::mount() {
	EXCPT_OPER(m_db = Database::open(&m_config, false, -1));
	//m_db->setCheckpointEnabled(false);
	m_txnLog = m_db->getTxnlog();
}
void TxnLogTestCase::unmount() {
	//m_db->setCheckpointEnabled(true);
	m_db->close(true, false);
	delete m_db;
	m_db = 0;
}
/**
 * 创建和删除日志正确性
 * 测试流程：
 *	1.创建日志，检查文件系统中是否有两个正确的日志文件
 *	2.删除日志，检查日志文件确实已被删除
 *	3.日志文件已经存在的情况下，创建日志文件，验证创建日志失败
 *	4.打开一个不存在的日志，验证打开失败
 */
void TxnLogTestCase::testCreateDrop() {
	string logFileName = "testlog";
	uint fileSize = 1 << 20;
	EXCPT_OPER(Txnlog::drop(".", logFileName.c_str(), LogConfig::DEFAULT_NUM_LOGS));
	EXCPT_OPER(Txnlog::create(".", logFileName.c_str(), fileSize,  m_config.m_logFileCntHwm));
	// 验证是否创建了两个日志文件
	for (uint i = 0; i < LogConfig::DEFAULT_NUM_LOGS; ++i) {
		File file(makeFileName(logFileName, i).c_str());
		bool exist = false;
		file.isExist(&exist);
		CPPUNIT_ASSERT(exist);
		file.open(true);
		u64 size = 0;
		file.getSize(&size);
		CPPUNIT_ASSERT(size == fileSize);
	}
	EXCPT_OPER(Txnlog::drop(".", logFileName.c_str(), LogConfig::DEFAULT_NUM_LOGS));
	// 验证是否删除两个日志文件
	for (uint i = 0; i < LogConfig::DEFAULT_NUM_LOGS; ++i) {
		File file(makeFileName(logFileName, i).c_str());
		bool exist = true;
		file.isExist(&exist);
		CPPUNIT_ASSERT(!exist);
	}
	{ // 文件已经存在，创建日志文件
		File file(makeFileName(logFileName, 0).c_str());
		file.create(false, false);
		file.close();
		EXCPT_OPER(Txnlog::create(".", logFileName.c_str(), fileSize, m_config.m_logFileCntHwm));
		EXCPT_OPER(Txnlog::drop(".", logFileName.c_str(), LogConfig::DEFAULT_NUM_LOGS));
	}
	{ // open一个不存在的日志文件
		init();
		bool fail = false; // open是否失败
		try {
			Txnlog::open(m_db, "no exists", "foo", 2, 1024, 1024 * 100, 0);
		} catch (NtseException) {
			fail = true;
		}
		destroy();
		CPPUNIT_ASSERT(fail);
	}
}


/**
 * 只有写一条日志记录，验证日志写入正确性
 * 测试流程：
 *	1.写入1条日志
 *	2.flush日志
 *	3.读取日志，验证日志的正确性
 *	4.重启数据库
 *	5.读取日志，验证日志的正确性
 */
void TxnLogTestCase::testOneLogRecord() {
	size_t recordSizes[] = {0, 1, LogConfig::LOG_PAGE_SIZE - 32,
		LogConfig::LOG_PAGE_SIZE, 2 * LogConfig::LOG_PAGE_SIZE, 3 * LogConfig::LOG_PAGE_SIZE };

	for (uint i = 0; i < sizeof(recordSizes)/sizeof(size_t); ++i) {
		init();
		LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, recordSizes[i]);
		LsnType lsn = writeLog(m_txnLog, le);
		m_txnLog->flush(lsn);
		CPPUNIT_ASSERT(m_txnLog->lastLsn() == lsn);
		{ // 只读取当前日志
			LogScanHandle *handle = m_txnLog->beginScan(lsn, m_txnLog->tailLsn());
			CPPUNIT_ASSERT(m_txnLog->getNext(handle));
			CPPUNIT_ASSERT(handle->curLsn() == lsn);
			CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
			CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
			m_txnLog->endScan(handle);
		}
		unmount();
		mount();
		{ // 读取所有日志
			LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
			CPPUNIT_ASSERT(m_txnLog->getNext(handle));
			CPPUNIT_ASSERT(handle->curLsn() == lsn);
			CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
			CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
			m_txnLog->endScan(handle);
		}
		freeLogEntry(le);
		destroy();
	}
}

/**
 * 写入日志，读取日志验证正确性
 * @param recCnt 日志项个数
 * @param logRecSize 日志项长度
 */
void TxnLogTestCase::writeLogAndVerify(uint recCnt, size_t logRecSize) {
	LogStatus beforeStats = m_txnLog->getStatus();
	vector<LsnType> lsnVec;
	LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, logRecSize);
	for (uint i = 0; i < recCnt; ++i) {
		lsnVec.push_back(writeLog(m_txnLog, le));
	}


	LogStatus endStats = m_txnLog->getStatus();
	CPPUNIT_ASSERT(endStats.m_writeCnt - beforeStats.m_writeCnt == recCnt);
	CPPUNIT_ASSERT(endStats.m_writeSize - beforeStats.m_writeSize == recCnt * logRecSize);
	scanVerify(lsnVec, le, lsnVec.front());

	freeLogEntry(le);
}
/**
 * 写多条日志记录，验证日志写入正确性
 * 测试流程：
 *	1.写入多条日志,并记录LSN到数组
 *	2.flush日志
 *	3.验证日志统计信息正确性
 *	4.读取日志，验证日志记录内容和LSN的正确性
 */
void TxnLogTestCase::testMultiLogRecord() {
	init();
	const uint recCnt = 15;
	size_t logRecSize = LogConfig::LOG_PAGE_SIZE / 3;
	writeLogAndVerify(recCnt, logRecSize);
	destroy();
}
/**
 * 日志文件切换测试
 * 测试流程：
 *	1.写入recCnt条日志，每条日志的长度为logSize
 *	2.flush日志
 *  3.读取日志，验证日志的正确性
 *	4.重启数据库
 *	5.读取日志，验证日志的正确性
 */
void TxnLogTestCase::doTestSwitchLogFile(u16 recCnt, size_t logSize) {
	init();

	const LogType logType = LOG_HEAP_UPDATE;
	const u16 tableId = 99;

	vector<LsnType> lsnVec;
	for (u16 i = 0; i < recCnt; ++i) {
		LogEntry *le = makeLogEntry(i, logType, tableId, logSize);
		lsnVec.push_back(writeLog(m_txnLog, le));
		freeLogEntry(le);
	}

	{
		LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		for (u16 i = 0; i < recCnt; ++i) {
			LogEntry *le = makeLogEntry(i, logType, tableId, logSize);
			CPPUNIT_ASSERT(m_txnLog->getNext(handle));
			CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
			CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
			freeLogEntry(le);
		}
		CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
		m_txnLog->endScan(handle);
	}
	unmount();
	mount();
	{
		LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		for (u16 i = 0; i < recCnt; ++i) {
			LogEntry *le = makeLogEntry(i, logType, tableId, logSize);
			CPPUNIT_ASSERT(m_txnLog->getNext(handle));
			CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
			CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
			freeLogEntry(le);
		}
		CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
		m_txnLog->endScan(handle);
	}

	destroy();
}
/**
 * 测试日志文件切换的正确性
 *	case 1:写入10个日志记录，每个日志记录的长度为日志文件的1/2
 *	case 2:写入50个日志记录，每个日志记录的长度为日志文件的1/3
 *	验证case1和case2中写入日志记录的正确性，具体流程见doTestSwitchLogFile注释。
 */
void TxnLogTestCase::testSwitchLogFile() {
	doTestSwitchLogFile(10, m_config.m_logFileSize / 2);
	doTestSwitchLogFile(50, m_config.m_logFileSize / 3);
}
/**
 * 验证写补偿日志的正确性
 * 测试流程：
 *	1.写入一些补偿日志
 *	2.flush日志
 *	3.读取日志，验证日志的正确性
 */
void TxnLogTestCase::testCpstLog() {
	init();

	const u16 recCnt = 200;
	const LogType logType = LOG_IDX_DML_CPST;
	const u16 tableId = 99;
	const size_t logSize = m_config.m_logFileSize / 2;
	vector<LsnType> lsnVec;
	const LsnType prevLsn = 0x12346;
	for (uint i = 0; i < recCnt; ++i) {
		LogEntry *le = makeCpstLogEntry(i, logType, tableId, logSize, prevLsn);
		lsnVec.push_back(writeLog(m_txnLog, le));
		freeLogEntry(le);
	}



	LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	for (uint i = 0; i < recCnt; ++i) {
		LogEntry *le = makeCpstLogEntry(i, logType, tableId, logSize, prevLsn);
		CPPUNIT_ASSERT(m_txnLog->getNext(handle));
		CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
		CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
		freeLogEntry(le);
	}
	CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
	m_txnLog->endScan(handle);

	destroy();
}
/**
 * 日志文件末尾刚好和日志文件末尾重叠时(#342)，打开日志/读写日志的正确性
 * 测试流程：
 *	1.写满第一个日志文件
 *	2.重启数据库
 *	3.验证日志的正确性
 *	4.写入额外的日志文件
 *	5.验证额外日志的正确性
 */
void TxnLogTestCase::testLogtailAtFileEnd() {
	init();
	// recCnt = 第一个日志文件能够包含的日志页数
	uint recCnt = m_config.m_logFileSize / LogConfig::LOG_PAGE_SIZE - 1;

	vector<LsnType> lsnVec;
	size_t logRecSize = LogConfig::LOG_PAGE_SIZE / 3;
	LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, logRecSize);
	fillLogFile(lsnVec, le, 1);
	// 此时日志文件末尾刚好和日志文件末尾重叠
	// 重启数据库应该不会出错
	unmount();
	mount();
	// 验证上次写入日志的正确性
	scanVerify(lsnVec, le);


	// 再写入15条日志
	vector<LsnType> addLsnVec;
	uint addCnt = 15;
	for (uint i = 0; i < addCnt; ++i)
		addLsnVec.push_back(writeLog(m_txnLog, le));
	m_txnLog->flush(addLsnVec.back());
	// 验证新写入日志的正确性
	scanVerify(addLsnVec, le, addLsnVec[0]);
	lsnVec.insert(lsnVec.end(), addLsnVec.begin(), addLsnVec.end());
	scanVerify(lsnVec, le, lsnVec[0]);
	freeLogEntry(le);

	destroy();
}

/**
 * 备份恢复之后，日志末尾刚好处在日志文件末尾
 *	对应bug(630)
 *	1.写满4个日志文件
 *	2.备份，并根据备份恢复数据库
 *	3.验证备份的尾LSN 等于 恢复出来数据库的 尾LSN
 *	4.验证日志读取正确性
 */
void TxnLogTestCase::testRecoverdLogtailAtFileEnd() {
	init();

	// 创建Account表
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "rpmms", "Account");
	builder->addColumnS("Mobile", CT_VARCHAR, 100, false, false)->addColumn("UserID", CT_BIGINT, false);
	TableDef *tableDef = builder->getTableDef();
	delete builder;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TxnLogTestCase::testRecoverdLogtailAtFileEnd", conn);
	string path = "Account";// string(m_config.m_basedir) + NTSE_PATH_SEP + "Account";
	m_db->createTable(session, path.c_str(), tableDef);
	string frmpath = string(m_config.m_basedir) + NTSE_PATH_SEP + path + ".frm";
	File frmFile(frmpath.c_str());
	frmFile.create(false, false);
	frmFile.close();
	// 写入数据
	Table *table = m_db->openTable(session,  path.c_str());
	uint recPerFile = m_config.m_logFileSize / LogConfig::LOG_PAGE_SIZE - 1;
	RedRecord redReccord(tableDef);
	uint dupIndex;
	for (uint i = 0; i < recPerFile * (m_config.m_logFileCntHwm + 1); ++i) {
		redReccord.writeVarchar(0, "13588");
		redReccord.writeNumber(1, (u64)i);
		CPPUNIT_ASSERT(table->insert(session, redReccord.getRecord()->m_data, true, &dupIndex) != INVALID_ROW_ID);
		m_txnLog->flush(m_txnLog->lastLsn());
	}
	
	delete tableDef;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	const char *backupDir = "backup";
	File dir(backupDir);
	dir.rmdir(true);
	dir.mkdir();
	
	m_db->getTxnlog()->setCheckpointLsn(Txnlog::MIN_LSN);

	BackupProcess *bp = 0;
	try {
		bp = m_db->initBackup(backupDir);
		m_db->doBackup(bp);
		m_db->finishingBackupAndLock(bp);
	} catch(NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		CPPUNIT_ASSERT(false);
		m_db->doneBackup(bp);
	}
	LsnType backupTailLsn = bp->m_tailLsn;
	m_db->doneBackup(bp);
	
	unmount();
	Database::restore(backupDir, m_config.m_basedir);
	mount();
	CPPUNIT_ASSERT(m_db->getTxnlog()->tailLsn() == backupTailLsn);

	destroy();
	dir.rmdir(true);
}

enum TruncType {
	AFTER_HEAD,
	AFTER_CONTINUE,
	BEFORE_TAIL
};

/**
 * @param isLong true：长日志，超过3页； false: 中等日志，2页
 * @param truncType 阶段日志方式
 */
void TxnLogTestCase::doTestIncompleteLogEntry(bool isLong, int truncType) {
	init();
	m_txnLog->enableFlusher(false);	
	vector<LsnType> lsnVec;
	// 写入日志
	LogEntry *mle = makeLogEntry(13, LOG_HEAP_UPDATE, 2, LogConfig::LOG_PAGE_SIZE);
	lsnVec.push_back(writeLog(m_txnLog, mle));
	LogEntry *sle = makeLogEntry(12, LOG_HEAP_UPDATE, 1, 10);
	lsnVec.push_back(writeLog(m_txnLog, sle));
	LogEntry *le = makeLogEntry(14, LOG_HEAP_UPDATE, 3, LogConfig::LOG_PAGE_SIZE * 3);
	lsnVec.push_back(writeLog(m_txnLog, le));
	lsnVec.push_back(writeLog(m_txnLog, sle));
	m_txnLog->enableFlusher(true);	
	m_txnLog->flush(lsnVec.back());

	int index = (int)lsnVec.size() - 2;
	LsnType pageAfterHead = (lsnVec[index] + LogConfig::LOG_PAGE_SIZE - 1)
		/ LogConfig::LOG_PAGE_SIZE * LogConfig::LOG_PAGE_SIZE;

	LsnType truncateLsn = INVALID_LSN;
	LsnType trueTail = pageAfterHead;
	if (truncType == AFTER_HEAD) { // 只留HEAD日志记录
		truncateLsn = pageAfterHead;
	} else if (truncType == AFTER_CONTINUE) { // 留有CONTINUE日志记录
		assert(isLong);
		truncateLsn = pageAfterHead + LogConfig::LOG_PAGE_SIZE;
	} else if (truncType == BEFORE_TAIL) { // TAIL之前截断
		LsnType tailPage = lsnVec[index + 1] / LogConfig::LOG_PAGE_SIZE * LogConfig::LOG_PAGE_SIZE;
		truncateLsn = tailPage;
	} else {
		assert(false);
	}
	m_txnLog->truncate(truncateLsn);
	unmount();
	mount();
	CPPUNIT_ASSERT(m_txnLog->tailLsn() == trueTail);

	freeLogEntry(sle);
	freeLogEntry(mle);
	freeLogEntry(le);
	writeLogAndVerify(150, Limits::PAGE_SIZE / 100);
	destroy();
}
/**
 * 不完整日志记录测试
 *	1.写入一条大日志
 *	2.截断日志
 *	3.验证日志尾正确性
 *	4.验证读写日志正确性
 */
void TxnLogTestCase::testIncompleteLogEntry() {
	// 1. 截断最长日志
	doTestIncompleteLogEntry(true, AFTER_HEAD);
	doTestIncompleteLogEntry(true, AFTER_CONTINUE);
	doTestIncompleteLogEntry(true, BEFORE_TAIL);
	//  2. 两页的中等日志
	doTestIncompleteLogEntry(false, AFTER_HEAD);
	doTestIncompleteLogEntry(false, BEFORE_TAIL);
}

/**
 * 测试更改日志文件大小功能
 * 一、
 *	1.创建数据库
 *	2.删除日志文件
 *	3.打开数据库
 *	4.验证读写日志正确性
 *
 * 二
 *	1.创建数据库
 *	2.设置检查点LSN
 *	3.删除日志文件
 *	4.打开数据库
 *	5.验证读写日志正确性
 */
void TxnLogTestCase::testChangeLogfileSize() {
	{
		init();

		uint fileCnt = m_db->getControlFile()->getNumTxnlogs();
		unmount();
		for (uint i = 0; i < fileCnt; ++i) {
			string path = string(m_config.m_basedir) + NTSE_PATH_SEP + Limits::NAME_TXNLOG;
			File file(makeFileName(path.c_str(), i).c_str());
			file.remove();
		}
		mount();
		writeLogAndVerify(150, Limits::PAGE_SIZE / 10);
		destroy();
	}
	{
		init();
		LsnType ckptLsn = Txnlog::MIN_LSN + Limits::PAGE_SIZE * 12 / 5;
		ckptLsn = (ckptLsn / 8) * 8;
		m_db->getTxnlog()->setCheckpointLsn(ckptLsn);
		uint fileCnt = m_db->getControlFile()->getNumTxnlogs();
		unmount();
		for (uint i = 0; i < fileCnt; ++i) {
			string path = string(m_config.m_basedir) + NTSE_PATH_SEP + Limits::NAME_TXNLOG;
			File file(makeFileName(path.c_str(), i).c_str());
			file.remove();
		}
		mount();
		writeLogAndVerify(150, Limits::PAGE_SIZE / 10);
		destroy();
	}
}


/**
 * 检查点刚好在日志末尾
 * 测试流程
 *	1. 写满第一个日志文件
 *	2. 设置检查点为第二个日志文件的末尾
 *	3. 启动数据库
 *	4. 验证读取和写入日志是否正常
 */
void TxnLogTestCase::testCkptAtFileEnd() {
	init();
	size_t logRecSize =  LogConfig::LOG_PAGE_SIZE / 2;
	LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, logRecSize);
	vector<LsnType> lsnVec;
	fillLogFile(lsnVec, le, 2);
	m_db->getTxnlog()->setCheckpointLsn(m_txnLog->tailLsn());
	// 此时日志文件末尾刚好和日志文件末尾重叠
	// 重启数据库应该不会出错
	unmount();
	mount();
	vector<LsnType> addLsnVec;
	uint addCnt = 19;
	for (uint i = 0; i < addCnt; ++i)
		addLsnVec.push_back(writeLog(m_txnLog, le));
	// 验证新写入日志的正确性
	scanVerify(addLsnVec, le);
	freeLogEntry(le);
	destroy();
}

/**
 * 扫描日志，验证日志正确性。
 * （整个日志包含相同的日志项）
 * @param lsnVec LSN数组
 * @param le 日志项
 */
void TxnLogTestCase::scanVerify(const vector<LsnType> lsnVec, const LogEntry *le, LsnType startLsn, LsnType endLsn) {
	LogScanHandle *handle = m_txnLog->beginScan(startLsn, endLsn);
	for (uint i = 0; i < lsnVec.size(); ++i) {
		LogScanHandle *hdl = m_txnLog->getNext(handle);
		CPPUNIT_ASSERT(hdl);
		CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
		CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
	}
	CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
	m_txnLog->endScan(handle);
}

/**
 * 写满日志文件
 * @param lsnVec [out] 已写入日志的lsn
 * @param le 待写入日志项
 * @param fileCnt 日志文件个数
 */
void TxnLogTestCase::fillLogFile(vector<LsnType> &lsnVec, const LogEntry *le, uint fileCnt) {
	// recCnt = 第一个日志文件能够包含的日志页数
	uint recPerFile = m_config.m_logFileSize / LogConfig::LOG_PAGE_SIZE - 1;
	for (uint i = 0; i < recPerFile * fileCnt; ++i) {
		lsnVec.push_back(writeLog(m_txnLog, le));
		// flush日志，结束当前页
		m_txnLog->flush(lsnVec.back());
	}
}
/**
 * 测试日志文件回收功能
 * 流程：
 *	创建10个日志文件
 *	重用第一个日志文件
 *	回收日志文件
 *	验证日志文件个数是否正确，读取日志是否正常
 *	写入日志，验证日志读写是否正常
 *	重启数据库，验证日志读取是否正常
 */
void TxnLogTestCase::testReclaimOverflowSpace() {
	init();

	// recCnt = 第一个日志文件能够包含的日志页数
	uint recPerFile = m_config.m_logFileSize / LogConfig::LOG_PAGE_SIZE - 1;
	uint fileCnt = 10;

	vector<LsnType> lsnVec;
	size_t logRecSize = LogConfig::LOG_PAGE_SIZE / 3;
	LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, logRecSize);
	fillLogFile(lsnVec, le, fileCnt);
	CPPUNIT_ASSERT(m_db->getControlFile()->getNumTxnlogs() == fileCnt);
	// 检查点在第一个日志文件，最后一个日志文件还在使用
	m_db->getTxnlog()->setCheckpointLsn(lsnVec[0]);
	m_txnLog->reclaimOverflowSpace();
	CPPUNIT_ASSERT(m_db->getControlFile()->getNumTxnlogs() == fileCnt);

	// 检查点在最后一个日志文件，最后一个日志文件还在使用
	m_db->getTxnlog()->setCheckpointLsn(lsnVec[recPerFile * fileCnt - 1]);
	m_txnLog->reclaimOverflowSpace();
	CPPUNIT_ASSERT(m_db->getControlFile()->getNumTxnlogs() == fileCnt);

	// 复用并写满第一个日志文件
	lsnVec.clear();
	fileCnt = 1;
	uint oldFileCnt = m_db->getControlFile()->getNumTxnlogs();
	fillLogFile(lsnVec, le, fileCnt);
	// 检查点在第一个日志文件，回收所有可回收的日志文件
	m_db->getTxnlog()->setCheckpointLsn(lsnVec[0]);
	m_txnLog->reclaimOverflowSpace();
	CPPUNIT_ASSERT(m_db->getConfig()->m_logFileCntHwm == m_db->getControlFile()->getNumTxnlogs());
	// 验证读取日志正确性
	scanVerify(lsnVec, le);

	// 验证读写日志的正确性
	lsnVec.clear();
	for (uint i = 0; i < m_config.m_logFileSize / logRecSize; ++i) {
		lsnVec.push_back(writeLog(m_txnLog, le));
	}
	m_txnLog->flush(lsnVec.back());
	m_db->getTxnlog()->setCheckpointLsn(lsnVec[0]);
	scanVerify(lsnVec, le);
	// 重启数据库，读取日志是否正常
	unmount();
	mount();
	scanVerify(lsnVec, le);

	freeLogEntry(le);

	destroy();
}

/**
 * 测试“设置在线lsn”功能
 * 1. 正确读取在线LSN之后,检查点之前的日志
 * 2. 在线LSN之后的日志不会被回收
 */
void TxnLogTestCase::testSetOnlineLsn() {
	init();


	vector<LsnType> lsnVec;
	size_t logRecSize = LogConfig::LOG_PAGE_SIZE / 3;
	LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, logRecSize);
	int recCnt = m_config.m_logFileSize * 4 / logRecSize;
	for (int i = 0; i < recCnt; ++i)
		lsnVec.push_back(writeLog(m_txnLog, le));
	m_txnLog->flush(lsnVec.back());
	
	m_db->getTxnlog()->setCheckpointLsn(lsnVec[0]);
	{
		// 过小的lsn
		CPPUNIT_ASSERT(-1 == m_txnLog->setOnlineLsn(lsnVec[0] - 1));
		// 正常lsn
		CPPUNIT_ASSERT(0 == m_txnLog->setOnlineLsn(lsnVec[0]));
		// 相同lsn
		CPPUNIT_ASSERT(1 == m_txnLog->setOnlineLsn(lsnVec[0]));
		// 大一号的lsn
		CPPUNIT_ASSERT(2 == m_txnLog->setOnlineLsn(lsnVec[0] + 1));
		m_txnLog->clearOnlineLsn(2);
		m_txnLog->clearOnlineLsn(2); // 重复clear看看会不会挂
		m_txnLog->clearOnlineLsn(1);
		m_txnLog->clearOnlineLsn(0);
	}


	int loop = 2;
	for (int i = 0; i < loop; ++i)
		CPPUNIT_ASSERT(i == m_txnLog->setOnlineLsn(lsnVec[i + 1]));
	
	m_db->getTxnlog()->setCheckpointLsn(lsnVec[2]);

	for (int i = 0; i < loop; ++i) {
		// 正确读取检查点之前的日志
		vector<LsnType> validLsn(lsnVec.begin() + i + 1, lsnVec.end());
		scanVerify(validLsn, le);

		u32 numFilesBefore = m_db->getControlFile()->getNumTxnlogs();
		m_db->getTxnlog()->setCheckpointLsn(lsnVec.back());
		vector<LsnType> newLsnVec;
		fillLogFile(newLsnVec, le, 1);
		lsnVec.insert(lsnVec.end(), newLsnVec.begin(), newLsnVec.end());
		u32 numFilesAfter = m_db->getControlFile()->getNumTxnlogs();
		// 不回收利用日志
		CPPUNIT_ASSERT(numFilesBefore + 1 == numFilesAfter);

		m_txnLog->clearOnlineLsn(i);
	}

	u32 numFilesBefore = m_db->getControlFile()->getNumTxnlogs();
	m_db->getTxnlog()->setOnlineLsn(lsnVec.back());
	lsnVec.clear();
	fillLogFile(lsnVec, le, 1);
	u32 numFilesAfter = m_db->getControlFile()->getNumTxnlogs();
	// 回收利用日志
	CPPUNIT_ASSERT(numFilesBefore == numFilesAfter);

	freeLogEntry(le);

	destroy();
}

/**
 * 测试日志极限情况
 *
 * 测试流程:
 * 1. 写入和读取一条最长的日志记录
 */
void TxnLogTestCase::testLimits() {
	m_config.m_logBufSize = LogConfig::MIN_LOG_BUFFER_SIZE;
	m_config.m_logFileSize = LogConfig::MIN_LOGFILE_SIZE;
	cout << endl << "LogConfig::MIN_LOG_BUFFER_SIZE(MB): "	<< LogConfig::MIN_LOG_BUFFER_SIZE / 1024 / 1024;
	cout << endl << "LogConfig::MIN_LOGFILE_SIZE(MB): "		<< LogConfig::MIN_LOGFILE_SIZE / 1024 / 1024;
	cout << endl << "LogConfig::MAX_LOG_RECORD_SIZE(MB): "	<< LogConfig::MAX_LOG_RECORD_SIZE / 1024 / 1024;
	cout << endl;

	init();

	LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, LogConfig::MAX_LOG_RECORD_SIZE);
	vector<LsnType> lsnVec;
	for (int i = 0; i < 9; ++i) {
		lsnVec.push_back(writeLog(m_txnLog, le));
	}
	scanVerify(lsnVec, le);
	freeLogEntry(le);
	destroy();
}

/**
 * 测试日志truncate功能的正确性
 * 测试流程：
 *	1.写入recCnt条日志记录
 *  2.截断日志,日志文件中只剩下前remainRecCnt条日志记录
 *	2.重启数据库
 *	3.验证日志文件中只有remainRecCnt条日志记录,且日志记录内容正确
 *	4.再写入额外的日志记录
 *	5.验证额外写入日志的正确性
 */
void TxnLogTestCase::testTruncate() {
	init();

	const uint recCnt = 15;
	const uint remainRecCnt = recCnt / 3;
	vector<LsnType> lsnVec;
	size_t logRecSize = LogConfig::LOG_PAGE_SIZE * 4 / 3;
	LogEntry *le = makeLogEntry(13, LOG_HEAP_UPDATE, 5, logRecSize);
	for (uint i = 0; i < recCnt; ++i) {
		lsnVec.push_back(writeLog(m_txnLog, le));
	}
	unmount();
	mount();

	m_txnLog->truncate(lsnVec[remainRecCnt]);

	// 验证读取的正确性
	LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	for (uint i = 0; i < remainRecCnt; ++i) {
		CPPUNIT_ASSERT(m_txnLog->getNext(handle));
		CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
		CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
	}
	CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
	m_txnLog->endScan(handle);

	// 验证写入正确性
	for (uint i = remainRecCnt; i < recCnt; ++i)
		lsnVec[i] = writeLog(m_txnLog, le);
	
	handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	for (uint i = 0; i < recCnt; ++i) {
		CPPUNIT_ASSERT(m_txnLog->getNext(handle));
		CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
		CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
	}
	CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
	m_txnLog->endScan(handle);
	freeLogEntry(le);

	destroy();
}


//////////////////////////////////////////////////////////////////////////
const char* TxnLogBigTest::getName() {
	return "TxnLog big tests";
}
const char* TxnLogBigTest::getDescription() {
	return "Test log function and performance";
}
bool TxnLogBigTest::isBig() {
	return true;
}
void TxnLogBigTest::setUp() {
	m_config.m_logFileSize = 1024 * 1024;
}
void TxnLogBigTest::tearDown() {

}
void TxnLogBigTest::init() {
	Database::drop(m_config.m_basedir);
	EXCPT_OPER(m_db = Database::open(&m_config, true));
	m_txnLog = m_db->getTxnlog();
}
void TxnLogBigTest::destroy() {
	m_db->close();
	delete m_db;
	m_db = 0;
	Database::drop(m_config.m_basedir);
}
void TxnLogBigTest::mount() {
	EXCPT_OPER(m_db = Database::open(&m_config, false, -1));
	m_txnLog = m_db->getTxnlog();
}
void TxnLogBigTest::unmount() {
	m_db->close(true, false);
	delete m_db;
	m_db = 0;
}
/** 
 * 大数据量日志测试
 *	测试流程：
 *	1.写入5000条日志
 *	2.重启数据库
 *	3.再写入5000条日志
 *	4.flush日志
 *	5.读取所有日志，验证日志记录正确性
 *	6.重启数据库
 *	7.读取所有日志，验证日志记录正确性
 */
void TxnLogBigTest::testLogOpers() {
	init();

	const u16 recCnt = (u16)10000;
	const LogType logType = LOG_HEAP_UPDATE;
	const u16 tableId = 99;
	const size_t logSize = 73;

	vector<LsnType> lsnVec;
	lsnVec.reserve(recCnt);
	LogEntry *le = makeLogEntry(0, logType, tableId, logSize);
	for (u16 i = 0; i < recCnt; ++i) {
		le->m_txnId = i;
		lsnVec.push_back(writeLog(m_txnLog, le));
		if (i == recCnt / 2) {
			unmount();
			mount();
		}
	}
	m_txnLog->flush(lsnVec.back());
	{
		LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		for (u16 i = 0; i < recCnt; ++i) {
			le->m_txnId = i;
			CPPUNIT_ASSERT(m_txnLog->getNext(handle));
			CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
			CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
		}
		CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
		m_txnLog->endScan(handle);
	}
	unmount();
	mount();
	{
		LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		for (u16 i = 0; i < recCnt; ++i) {
			le->m_txnId = i;
			CPPUNIT_ASSERT(m_txnLog->getNext(handle));
			CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
			CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
		}
		CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
		m_txnLog->endScan(handle);
	}
	freeLogEntry(le);
	destroy();
}

class WriteLogThread: public Thread {
public:
	WriteLogThread(Txnlog *txnLog, const LogEntry *le, int count)
		: Thread("WriteLogThread"), m_txnLog(txnLog), m_count(count), m_logEntry(le) {
		m_lsnVec.reserve(count);
	}
	void run() {
		for (int i = 0; i < m_count; ++i)
			m_lsnVec.push_back(writeLog(m_txnLog, m_logEntry));
		m_txnLog->flush(m_lsnVec.back());
	}
public:
	vector<LsnType> m_lsnVec;
private:
	int m_count;
	Txnlog *m_txnLog;
	const LogEntry *m_logEntry;
};

/** 
 * 测试多线程写日志
 *	测试流程：
 *	1.运行100个线程
 *	2.每个线程写入1000条日志，之后调用flush日志
 *	3.汇总和排序每个线程中保存的LSN
 *	4.读取所有日志，验证日志记录正确性
 *	5.统计读取和写入日志性能
 */
void TxnLogBigTest::testMT() {
	init();
	const size_t size = 101;
	LogEntry *le = makeLogEntry(0, LOG_HEAP_UPDATE, 99, size);
	const int loopCnt = 10000;
	const int threadCnt = 100;
	WriteLogThread *workers[threadCnt];

	for (size_t i = 0; i < threadCnt; i++)
		workers[i] = new WriteLogThread(m_txnLog, le, loopCnt);

	u64 before = System::currentTimeMillis();
	for (size_t i = 0; i < threadCnt; i++)
		workers[i]->start();
	for (size_t i = 0; i < threadCnt; i++)
		workers[i]->join();
	u64 after = System::currentTimeMillis();

	u64 interval = after - before;
	int ops = loopCnt * threadCnt;
	cout << "  Time: " << interval << " ms" << endl;
	cout << "  Writes: " << ops << ", " << size * ops << " B" << endl;
	cout << "  Through put: " << ops / interval << " ops/ms, " << endl;

	m_db->setCheckpointEnabled(false);
	LsnType ckptLsn = m_db->getControlFile()->getCheckpointLSN();
	// 排序LSN
	vector<LsnType> lsnVec;
	lsnVec.reserve(loopCnt * threadCnt);
	for (size_t i = 0; i < threadCnt; i++) {
		lsnVec.insert(lsnVec.end()
			, lower_bound(workers[i]->m_lsnVec.begin(), workers[i]->m_lsnVec.end(), ckptLsn)
			, workers[i]->m_lsnVec.end());
	}
	sort(lsnVec.begin(), lsnVec.end());

	// 读取日志, 测试性能
	{
		size_t ops = 0;
		u64 before = System::currentTimeMillis();
		LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		while(m_txnLog->getNext(handle))
			++ops;
		m_txnLog->endScan(handle);
		u64 after = System::currentTimeMillis();
		u64 interval = after - before;
		CPPUNIT_ASSERT(ops == lsnVec.size());
		cout << "  Time: " << interval << " ms" << endl;
		cout << "  Reads: " << ops << ", " << size * ops << " B" << endl;
		cout << "  Through put: " << ops / interval << " ops/ms, " << endl;
	}

	// 读取日志, 验证正确性
	LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	for (size_t i = 0; i < lsnVec.size(); ++i) {
		CPPUNIT_ASSERT(m_txnLog->getNext(handle));
		CPPUNIT_ASSERT(handle->curLsn() == lsnVec[i]);
		CPPUNIT_ASSERT(isLogEntryEq(handle->logEntry(), le));
	}
	CPPUNIT_ASSERT(!m_txnLog->getNext(handle));
	m_txnLog->endScan(handle);

	for (size_t i = 0; i < threadCnt; i++)
		delete workers[i];
	destroy();
}

class TraceReader {
public:

	~TraceReader() {
		if (m_file.is_open())
			m_file.close();
	}

	bool open(const char *path) {
		assert(!m_file.is_open());
		m_file.open(path, ios_base::in);
		return m_file.is_open();
	}

	LsnType readNext(LogEntry *le) {
		char line[1024];
		if (!m_file.getline(line, sizeof(line)))
			return INVALID_LSN;
		stringstream ss(line);
		LsnType lsn = INVALID_LSN;
		string tmp;
		ss >> tmp >> tmp;
		ss >> lsn;	// 第3列是LSN
		ss >> tmp;
		ss >> tmp;
		string strTxnId = parseKV(tmp, "TxnID");
		le->m_txnId = (u16)atoi(strTxnId.c_str());

		ss >> tmp;
		string strTableId = parseKV(tmp, "TblID");
		le->m_tableId = (u16)atoi(strTableId.c_str());

		ss >> tmp;
		string strLogType = parseKV(tmp, "LogType");
		le->m_logType =  Txnlog::parseLogType(strLogType.c_str());

		ss >> tmp;
		string strSize = parseKV(tmp, "Size");
		le->m_size =  (size_t)atoi(strSize.c_str());		

		return lsn;
	}

	static inline string parseKV(const string& str, const char *key) {
		int pos = str.find(':');
		assert(pos > 0);
		assert(str.substr(0, pos) == key);
		return str.substr(pos + 1, str.size() - pos - 1 - 1);
	}

	LsnType readNext(LsnType startLsn, LogEntry *le) {
		LsnType lsn = INVALID_LSN;
		do {
			lsn = readNext(le);
		} while(lsn != INVALID_LSN && lsn < startLsn);
		return lsn;
	}
private:
	ifstream m_file;
};

/**
 * 读取日志模块Trace，验证Trace与日志的一致性
 * 目录： 
 *		errorData 出错的数据库目录
 *		trace.txt 日志trace
 * 注1：数据库配置必须一直
 * 
 * 注2：Trace格式为
 *	事务id 日志类型 长度 表id LSN
 */
void TxnLogBigTest::testTraceVerify() {
	if (!File::isExist("errorData"))
		return;
	TraceReader traceReader;
	if (!traceReader.open((string("errorData") + NTSE_PATH_SEP + "trace.txt").c_str()))
		CPPUNIT_ASSERT(false);
	Config config;
	config.m_logFileSize = 21 * (1<<20);
	config.setBasedir("errorData");
	Database *db = 0;
	try {
		db = Database::open(&config, false, -1);
		LsnType ckptLsn = db->getControlFile()->getCheckpointLSN();
		LogEntry leTrace;
		LsnType traceLsn = traceReader.readNext(ckptLsn, &leTrace);
		LogScanHandle *handle = 0;
		if (traceLsn != INVALID_LSN) {
			handle = db->getTxnlog()->beginScan(max(ckptLsn, traceLsn), Txnlog::MAX_LSN);
			handle = db->getTxnlog()->getNext(handle);
			CPPUNIT_ASSERT(handle);
		} else {
			handle = db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
			handle = db->getTxnlog()->getNext(handle);
			CPPUNIT_ASSERT(!handle);
		}

		while (true) {
			if (!(handle->curLsn() == traceLsn &&
				handle->logEntry()->m_logType == leTrace.m_logType &&
				handle->logEntry()->m_size == leTrace.m_size &&
				handle->logEntry()->m_txnId == leTrace.m_txnId &&
				handle->logEntry()->m_tableId == leTrace.m_tableId)) {
				/*
				while (handle = db->getTxnlog()->getNext(handle)) {
					cout << "Lsn: " << handle->curLsn()
						 << ", " << handle->logEntry()->m_txnId 
						 << ", " << Txnlog::getLogTypeStr(handle->logEntry()->m_logType)
						 << ", " << handle->logEntry()->m_size
						 << ", " << handle->logEntry()->m_tableId
						 << endl;
				}
				*/
				CPPUNIT_ASSERT(false);
			}
			traceLsn = traceReader.readNext(&leTrace);
			handle = db->getTxnlog()->getNext(handle);
			if (handle == 0 || traceLsn == INVALID_LSN) {
				CPPUNIT_ASSERT(handle == 0 && traceLsn == INVALID_LSN);
				break;
			}
		}
	} catch(NtseException &e) {
		if (db) {
			db->close();
			delete db;
		}
		cerr << "Exception : " << e.getMessage() <<  endl;
		CPPUNIT_ASSERT(false);
	}
	db->close();
	delete db;
}
/**
 * 测试写日志性能
 * @param logEntrySize 日志项长度，不包括日志头
 * @param loopCnt 每个线程的循环数
 * @param threadCnt 并发线程数
 */
void TxnLogBigTest::doTestWritePerformance(size_t logEntrySize, int loopCnt, int threadCnt) {
	LogEntry *le = makeLogEntry(0, LOG_HEAP_UPDATE, 99, logEntrySize);
	WriteLogThread **workers = new WriteLogThread *[threadCnt];

	for (int i = 0; i < threadCnt; i++)
		workers[i] = new WriteLogThread(m_txnLog, le, loopCnt);

	u64 before = System::currentTimeMillis();
	for (int i = 0; i < threadCnt; i++)
		workers[i]->start();
	for (int i = 0; i < threadCnt; i++)
		workers[i]->join();
	u64 after = System::currentTimeMillis();

	u64 interval = after - before;
	u64 ops = (u64)loopCnt * threadCnt;
	double totalSize = (double)logEntrySize * ops / 1024 / 1024;
	cout << "  EntrySize " << logEntrySize << ", threadCnt " << threadCnt << ", loopCnt " << loopCnt << endl;
	cout << "  Time: " << interval << " ms" << endl;
	cout << "  TotalSize: " << totalSize << "MB" << endl;
	cout << "  Through put: " << ops * 1000 / interval << " op/s,\t"
		 << totalSize * 1000 / interval << " MB/s" << endl;
}
/**
 * 测试写日志性能
 */
void TxnLogBigTest::testWritePerformance() {
	Config *config = new Config();
	config->m_logBufSize = 5 * 1024 * 1024;
	config->m_logFileSize = 20 * 1024 * 1024;
	Database::drop(config->m_basedir);
	EXCPT_OPER(m_db = Database::open(config, true));
	m_txnLog = m_db->getTxnlog();

	doTestWritePerformance(12, 100000, 100);
	doTestWritePerformance(1000, 2000, 100);

	m_db->close();
	Database::drop(config->m_basedir);
	delete m_db;
}

/**
 * 生成自验证的日志记录
 */
class SelfVerifyLog {
public:

	static  size_t MIN_SIZE; 
	/**
	 * 随机生成自验证日志
	 * 
	 * @param le，日志项，调用者负责分配空间，m_size成员指定已分配的内存空间（输出输入参数）
	 * @param sid, 会话id
	 * @param lastLsn 该会话上一条日志
	 */
	static LogEntry* gen(LogEntry *le, uint sid, LsnType lastLsn) {
		assert(le->m_size > MIN_SIZE);
		le->m_logType = (LogType)(System::random() % LOG_CPST_MIN);
		le->m_tableId = (u16)(System::random() & 0xFFFF);
		le->m_txnId = sid;
		if (System::random() % 10 >= 7)
			le->m_size = System::random() % (le->m_size - MIN_SIZE) + MIN_SIZE;
		else // 70%的日志小于1/4个页面
			le->m_size =  MIN_SIZE + System::random() % (Limits::PAGE_SIZE / 4);

		Stream s(le->m_data, le->m_size);
		try {
			s.write((u32)le->m_size);  
			s.write(le->m_txnId);		
			s.write(le->m_tableId);
			s.write((u32)le->m_logType);
			s.write(lastLsn);
		} catch(NtseException &e) {
			cout << e.getMessage() << endl;
		}
		return le;
	}
	/**
	 * 验证日志正确性
	 * @param le 日志项
	 * @param lastLsn 上一条日志
	 */
	static bool verify(const LogEntry *le, LsnType lastLsn) {
		Stream s(le->m_data, le->m_size);
		u32 size = 0;
		u16 txnId = 0;
		u16 tableId = 0;
		u32 logType = 0;
		LsnType lsn = INVALID_LSN;
		s.read(&size);
		s.read(&txnId);
		s.read(&tableId);
		s.read(&logType);
		s.read(&lsn);
		if (le->m_txnId != txnId)
			return false;
		if (le->m_tableId != tableId)
			return false;
		if (le->m_logType != logType)
			return false;
		if (lastLsn != INVALID_LSN && lsn != lastLsn)
			return false;
		return true;
	}
};

size_t SelfVerifyLog::MIN_SIZE = sizeof(u32) * 2 + sizeof(u16) * 2 + sizeof(LsnType); 

class StabWorkerThread: public Thread {
public:
	StabWorkerThread(Txnlog *txnLog) : Thread("StabWorkerThread") {
		m_txnLog = txnLog;
		m_stopped = false;
		m_maxSize = min(Limits::PAGE_SIZE * 10, m_txnLog->getDatabase()->getConfig()->m_logFileSize * 2 / 3);
		m_lastLsn = INVALID_LSN;
		m_logEntry.m_data = new byte[m_maxSize];
	}


	~StabWorkerThread() {
		delete [] m_logEntry.m_data;
	}

	void run() {
		try {
			while(!m_stopped) {
				m_logEntry.m_size = m_maxSize;
				m_lastLsn = writeLog(m_txnLog, SelfVerifyLog::gen(&m_logEntry, m_id, m_lastLsn));
			}
		} catch(NtseException &e) {
			cout << "Exception in StabWorkerThread: " << e.getMessage() << endl;
		}
	}
	
	void stop() {
		m_stopped = true;
	}



private:
	bool m_stopped;
	LogEntry m_logEntry;
	Txnlog *m_txnLog;
	uint m_maxSize;
	LsnType m_lastLsn;
};

class CleanerTask: public Task {
public:
	CleanerTask(Txnlog *txnLog, uint interval) : Task("CleanerTask", interval) {
		m_txnLog = txnLog;
	}

	void run() {
		m_txnLog->reclaimOverflowSpace();
	}
private:
	Txnlog *m_txnLog;
};

class CkptTask: public Task {
public:
	CkptTask(Txnlog *txnLog, uint interval) : Task("CkptTask", interval) {
		m_txnLog = txnLog;
	}

	void run() {
		LsnType lastLsn = m_txnLog->lastLsn();
		Event env;
		env.wait(500);
		m_txnLog->flush(lastLsn);
		m_txnLog->setCheckpointLsn(lastLsn);
		m_txnLog->reclaimOverflowSpace();
	}
private:
	Txnlog *m_txnLog;
};

/**
 * 稳定性测试(#891)
 *
 */
void TxnLogBigTest::testStability() {
	m_config.m_logFileSize = 10 * (1 << 20);
	init();
	const size_t threadCnt = 100;
	StabWorkerThread *workers[threadCnt];
	CleanerTask *cleaner;
	CkptTask *scavenger;
	while(true) {
		m_txnLog->getDatabase()->setCheckpointEnabled(false);
		for (size_t i = 0; i < threadCnt; ++i) {
			workers[i] = new StabWorkerThread(m_txnLog);
			workers[i]->start();
		}
		cleaner = new CleanerTask(m_txnLog, 1000);
		scavenger = new CkptTask(m_txnLog, 500);
		cleaner->start();
		scavenger->start();
		Event env;
		env.wait(1000 * 300);
		for (size_t i = 0; i < threadCnt; ++i) {
			workers[i]->stop();
			workers[i]->join();
			delete workers[i];
		}
		cleaner->stop();
		cleaner->join();
		delete cleaner;
		scavenger->stop();
		scavenger->join();
		delete scavenger;
		unmount();
		mount();

		// 验证日志正确性
		//	每个线程的日志构成一个单向链表
		map<u32, LsnType> lsnMap;
		
		LogScanHandle *handle = m_txnLog->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		while (m_txnLog->getNext(handle)) {
			u32 tid = handle->logEntry()->m_txnId;
			if (lsnMap.count(tid) == 0)
				lsnMap[tid] = INVALID_LSN;
			CPPUNIT_ASSERT(SelfVerifyLog::verify(handle->logEntry(), lsnMap[tid]));
		}
		m_txnLog->endScan(handle);
	}
	destroy();
}

//////////////////////////////////////////////////////////////////////////
LsnType writeLog(Txnlog *log, const LogEntry *e) {
	if (e->m_logType > LOG_CPST_MIN && e->m_logType < LOG_CPST_MAX)
		return log->logCpst(e->m_txnId, e->m_logType, e->m_tableId, e->m_data, e->m_size, e->m_cpstForLsn);
	return log->log(e->m_txnId, e->m_logType, e->m_tableId, e->m_data, e->m_size);
}

bool isLogEntryEq(const LogEntry *e1, const LogEntry *e2) {
	if (!(e1->m_logType == e2->m_logType
		&& e1->m_txnId == e2->m_txnId
		&& e1->m_tableId == e2->m_tableId
		&& e1->m_size == e2->m_size))
		return false;

	if (memcmp(e1->m_data, e2->m_data, e1->m_size))
		return false;

	return true;
}

static LogEntry* makeCpstLogEntry(u16 txnId, LogType logType,
								  u16 tableId, size_t size, LsnType targetLsn) {
	LogEntry *le = new LogEntry();
	le->m_txnId = txnId;
	le->m_logType = logType;
	le->m_tableId = tableId;
	le->m_size = size;
	le->m_cpstForLsn = targetLsn;
	le->m_data = size ? new byte[size] : 0;
	memset(le->m_data, -1, le->m_size);
	return le;
}
static LogEntry* makeLogEntry(u16 txnId, LogType logType, u16 tableId, size_t size) {
	LogEntry *le = new LogEntry();
	le->m_txnId = txnId;
	le->m_logType = logType;
	le->m_tableId = tableId;
	le->m_size = size;
	le->m_cpstForLsn = INVALID_LSN;
	le->m_data = size ? new byte[size] : 0;
	memset(le->m_data, -1, le->m_size);
	return le;
}

static void freeLogEntry(LogEntry *e) {
	delete[] e->m_data;
	delete e;
}

string makeFileName(string basename, uint index) {
	stringstream ss;
	ss << basename << "." << index;
	return ss.str();
}

