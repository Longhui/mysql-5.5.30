/**
 * 数据库恢复性能测试实现
 *
 * 测试目的：测试重启恢复性能
 * 表模式：Blog
 * 测试配置：NTSE配置：NC_MEDIUM record_size = 200
 * 测试数据：随机新数据
 *
 * 测试流程：
 *		1.	插入多条记录，数据库大小为200M。
 *		2.	做一次checkpoint，记录检查点LSN:ckLsn1并关闭数据库
 *		3.	拷贝数据库目录到backup目录
 *		4.	插入10000条记录
 *		5.	更新10000条记录
 *		6.	删除10000条记录
 *		7.	关闭数据库，并拷贝backup目录中的数据库文件和索引文件到数据库目录。
 *		8.	打开数据库进行恢复，并计时T
 *		9.	恢复速度30000/T(rec/s), (tailLsn – ckLsn1)/T/1024/1024(MB/s)
 *
 * @author 苏斌(bsu@corp.netease.com naturally@163.org)
 */

#include "Recovery.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/Buffer.h"
#include "misc/ControlFile.h"
#include "util/System.h"
#include "util/File.h"
#include "BlogTable.h"
#include "Random.h"
#include "IntgTestHelper.h"
#include <sstream>

using namespace ntse;
using namespace ntseperf;
using namespace std;

DBRecoveryTest::DBRecoveryTest(u64 dataSize) {
	setConfig(CommonDbConfig::getMedium());
	setTableDef(BlogTable::getTableDef(true));
	m_dataSize = dataSize;
	m_totalRecSizeLoaded = dataSize;
}

void DBRecoveryTest::setUp() {
	ts.recv = true;
	vs.idx = true;
	vs.hp = true;
	m_dmlThreads = new Graffiti*[DML_THREAD_NUM];
	for (uint i = 0; i < DML_THREAD_NUM; i++) {
		m_dmlThreads[i] = new Graffiti(i, this);
	}
}

void DBRecoveryTest::tearDown() {
	stopThreads();
	closeDatabase();
}

string DBRecoveryTest::getName() const {
	stringstream ss;
	ss << "DatabaseRecovery(DataSize:";
	ss << m_dataSize / 1024 / 1024;
	ss << "MB)";
	return ss.str();
}


string DBRecoveryTest::getDescription() const {
	return "Database recovery test";
}


void DBRecoveryTest::loadData(u64 *totalRecSize, u64 *recCnt) {
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
 * 该用例不需要预热
 * 这里执行测试流程的2-7步操作
 */
void DBRecoveryTest::warmUp() {
	openTable(false);

	RandomGen::setSeed((unsigned)time(NULL));

	// 先执行一次检查点
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::warmUp", conn);
	m_db->checkpoint(session);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_ckLsn = m_db->getTxnlog()->tailLsn();
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	// 备份数据库文件
	closeDB();
	backupFiles(m_config->m_tmpdir, TABLE_NAME_BLOG, true, true);
	openDB();
	m_db->setCheckpointEnabled(false);

	// 执行随机DML操作
	for (uint i = 0; i < DML_THREAD_NUM; i++) {
		m_dmlThreads[i]->start();
	}

	for (uint i = 0; i < DML_THREAD_NUM; i++) {
		m_dmlThreads[i]->join();
	}

	m_tailLsn = m_db->getTxnlog()->tailLsn();	

	// 恢复数据文件
	closeDB(false);
	backupFiles(m_config->m_tmpdir, TABLE_NAME_BLOG, true, false);

	return;
}


u64 DBRecoveryTest::getDataSize() {
	return m_tailLsn - m_ckLsn;
}

/**
 * 打开数据库进行恢复并得到恢复耗时
 * 这里执行测试步骤的8-9两步操作
 */
void DBRecoveryTest::run() {
	m_opCnt = TOTAL_DML_UPDATE_TIMES;
	m_opDataSize = m_totalRecSizeLoaded;
	// 使用日志恢复之前备份的数据库文件
	openDB(1);	// 这里会强制恢复
}


/**
 * 打开数据库
 */
void DBRecoveryTest::openDB(int recover) {
	assert(m_db == NULL);
	Config *cfg = new Config(CommonDbConfig::getMedium());
	m_db = Database::open(cfg, true, recover);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("EmptyTestCase::openTable", conn);
	m_table = m_db->openTable(session, m_tableDef->m_name);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 关闭数据库
 */
void DBRecoveryTest::closeDB(bool normalClose) {
	assert(m_db != NULL);
	m_db->close(normalClose, normalClose);
	delete m_db;
	m_db = NULL;
}


void DBRecoveryTest::stopThreads() {
	if (m_dmlThreads) {
		for (u16 i = 0; i < DML_THREAD_NUM; i++) {
			if (m_dmlThreads[i]) {
				m_dmlThreads[i]->join();
				delete m_dmlThreads[i];
			}
		}

		delete[] m_dmlThreads;
	}
}

/**
 * 随机进行插入、删除、更新操作各一次
 */
void DBRecoveryTest::randomDMLModify(uint threadNo, uint opid) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("DBBackupTest::randomDML", conn);

	int opKind = RandomGen::nextInt();
	for (uint i = 0; i < 3; i++) {
		u64 opNo = RandomGen::nextInt(0, MAX_ID);
		switch(opKind % 3) {
			case 0:		// 插入
				{
					u64 key = MAX_ID - 1 - DML_THREAD_NUM * opid - threadNo;
					TableDMLHelper::insertRecord(session, m_table, key, opid);
					break;
				}
			case 1:		// 删除
				TableDMLHelper::deleteRecord(session, m_table, &opNo);
				break;
			default:	// 更新
				TableDMLHelper::updateRecord(session, m_table, &opNo, &opNo, opid, false);
				break;
		}

		opKind++;
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 执行DML操作的线程具体实现
 */
void Graffiti::run() {
	for (uint i = 0; i < m_testcase->TOTAL_DML_UPDATE_TIMES / m_testcase->DML_THREAD_NUM; i++) {
		m_testcase->randomDMLModify(m_threadId, i);
	}
}
