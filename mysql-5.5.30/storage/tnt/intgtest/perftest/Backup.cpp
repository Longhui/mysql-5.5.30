/**
 * 数据库备份性能测试实现
 *
 * 测试目的：测试备份和恢复性能
 * 表模式：Blog
 * 测试配置：NTSE配置：NC_MEDIUM record_size = 200
 *	 测试以下几种情况：
 *		C1: 没有负载
 *		C2：没有负载，数据在内存中
 *		C3: 数据在内存中，update/delete/insert: 100op/s, select 600op/s
 *		C4: 数据在内存中，update/delete/insert: 10op/s, select 60op/s
 *
 * 测试数据：随机新数据
 * 
 * 测试流程：
 *		1.	插入多条记录，数据库大小为200M。
 *		2.	预热。进行一遍表扫描
 *		3.	启动工作线程，按照预定负载发起数据库操作。
 *		4.	备份数据库。
 *		5.	备份速度=备份的数据量/耗时
 *
 * @author 苏斌(bsu@corp.netease.com naturally@163.org)
 */

#include "Backup.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "util/System.h"
#include "util/File.h"
#include "BlogTable.h"
#include "Random.h"
#include "IntgTestHelper.h"
#include <sstream>
#include "TableHelper.h"

using namespace ntse;
using namespace ntseperf;
using namespace std;

/**
 * 根据不同的参数指定测试用例进行C1～C4之中的任意一个测试
 */
DBBackupTest::DBBackupTest(bool warmup, uint readOps, uint writeOps) {
	setConfig(CommonDbConfig::getMedium());
	setTableDef(BlogTable::getTableDef(true));
	m_warmup = warmup;
	m_readOps = readOps;
	m_writeOps = writeOps;
	m_totalRecSizeLoaded = TESTCASE_DATA_SIZE;
	m_threadNum = DEFAULT_THREADS;
	m_troubleThreads = NULL;
	sprintf(m_backupDir, "%s/backup", m_config->m_tmpdir);
}


DBBackupTest::~DBBackupTest() {
}

void DBBackupTest::setUp() {
}

void DBBackupTest::tearDown() {
	closeDatabase();
}

string DBBackupTest::getName() const {
	stringstream ss;
	ss << "DatabaseBackup(";
	if (m_warmup)
		ss << "Warmup, ";
	if (m_readOps != 0 || m_writeOps != 0)
		ss << "With loading, ";
	if (isHeavyLoading())
		ss << "Heavy loading";
	ss << ")";
	return ss.str();
}


string DBBackupTest::getDescription() const {
	return "Database backup test";
}


void DBBackupTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);

	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::loadData", conn);

	assert(m_totalRecSizeLoaded > 0);
	m_recCntLoaded = BlogTable::populate(session, m_table, &m_totalRecSizeLoaded, m_totalRecSizeLoaded / m_table->getTableDef()->m_maxRecSize * 10, 0);
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}


/**
 * 预热Blog表数据
 * @post 表数据应该被读取到内存
 * 这里执行测试步骤当中的操作2、3
 */
void DBBackupTest::warmUp() {
	const char *f = m_backupDir;
	File(f).mkdir();
	openTable(false);
	if (m_warmup)	// 不一定都需要预热
		TableWarmUpHelper::warmUpTableByIndice(m_db, m_table, true, true);

	if (m_readOps != 0 || m_writeOps != 0) {	// 启动负载线程模拟数据库访问场景
		m_running = true;
		balanceThreadNums();
		m_troubleThreads = new BusyGuy*[m_threadNum];
		for (u16 i = 0; i < m_threadNum; i++)
			m_troubleThreads[i] = new BusyGuy(i, this, m_readOps / m_threadNum, m_writeOps / m_threadNum);
		RandomGen::setSeed((unsigned)time(NULL));
		for (u16 i = 0; i < m_threadNum; i++) {
			m_troubleThreads[i]->start();
		}

		// 需要停顿一段时间等到系统负载真的稳定了才进行后续操作
		Thread::msleep(10000);
	}
}


/**
 * 测试备份，执行测试步骤当中的步骤4
 */
void DBBackupTest::run() {
	m_opCnt = m_readOps + m_writeOps;
	m_opDataSize = m_totalRecSizeLoaded;
	// 开始进行数据库备份
	dbBackup();
	// 停止负载线程
	stopThreads();
}


/**
 * 调整用例使用的线程数
 */
void DBBackupTest::balanceThreadNums() {
	while (true) {
		if (m_readOps / m_threadNum > THREADS_MAX_READS || m_writeOps / m_threadNum > THREADS_MAX_WRITES)
			m_threadNum *= 2;
		else
			break;
	}
}

/**
 * 对当前数据库执行备份操作
 */
void DBBackupTest::dbBackup() {
	BackupProcess *bp = 0;
	try {
		bp = m_db->initBackup(m_backupDir);
		m_db->doBackup(bp);
		m_db->finishingBackupAndLock(bp);
	} catch(NtseException &e) {
		cout << e.getMessage() << endl;
		m_db->doneBackup(bp);
		return;
	}
	m_db->doneBackup(bp);
}


void DBBackupTest::stopThreads() {
	m_running = false;
	Thread::msleep(10 * 1000);
	
	if (m_troubleThreads) {
		for (u16 i = 0; i < m_threadNum; i++) {
			if (m_troubleThreads[i]) {
				m_troubleThreads[i]->join();
				delete m_troubleThreads[i];
			}
		}

		delete[] m_troubleThreads;
	}
}


void DBBackupTest::randomDML(bool scanOp, uint opid, uint threadId) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DBBackupTest::randomDML", conn);

	u64 opNo = RandomGen::nextInt(0, (int)m_recCntLoaded);

	if (scanOp) {	// 查询
		RowId rowId;
		TableDMLHelper::fetchRecord(session, m_table, opNo, NULL, &rowId, false);
	} else {
		uint updateNo = RandomGen::nextInt(0, 3);
		if (updateNo == 0) {	// 插入
			uint key = MAX_ID - 1 - opid * m_threadNum - threadId;
			TableDMLHelper::insertRecord(session, m_table, key, opid);
		} else if (updateNo == 1) {	// 删除
			TableDMLHelper::deleteRecord(session, m_table, &opNo);
		} else {	// 更新
			TableDMLHelper::updateRecord(session, m_table, &opNo, &opNo, opid, false);
		}
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


void BusyGuy::helpThreadOperation() {
	u64 start = System::currentTimeMillis();

	uint readOps = m_readOps, writeOps = m_writeOps;
	uint totalOps = m_readOps + m_writeOps;

	for (uint i = 0; i < totalOps; i++) {
		uint opKind = RandomGen::nextInt(0, 2);
		if (opKind == 0 && m_readOps == 0 || opKind == 1 && m_writeOps == 1)
			opKind = 1 - opKind;
		if (opKind == 0) {	// 扫描
			assert(m_readOps > 0);
			m_testcase->randomDML(true, i, m_threadId);
			--readOps;
		} else {	// 更新
			assert(m_writeOps > 0);
			m_testcase->randomDML(false, i, m_threadId);
			--writeOps;
		}
	}

	u64 end = System::currentTimeMillis();

	int diff = 1000 - (int)(end - start);

	if (diff >= 0)
		Thread::msleep(diff);
	else {	// 打印警告信息表明当前操作频率达不到预定要求
		cout << "[Warning]: The database performance is lower than expected: "
			<< "Doing " << m_readOps << " times scan and doing " << m_writeOps << " times update"
			<< "cost " << (end - start) << " ms"
			<< endl;
	}
}


/**
 * 制造负载的线程，每个线程都会按照制定的比例随机执行查询和更新操作
 * 每个线程执行的操作次数也是按照制定负载量来进行
 */
void BusyGuy::run() {
	while (m_testcase->isRunning()) {
		helpThreadOperation();
	}
}