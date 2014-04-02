/**
* 集成测试5.2.1测试数据库稳定性用例
*
* 测试目的：稳定性测试，执行各类操作，尽量多覆盖NTSE代码
* 表模式：Blog
* 测试配置：NTSE配置：NC_SMALL / data_size = ntse_buf_size * 50 / thread_count = 100
* 测试数据：初始数据：随机新数据，ID在0和MAXID之间随机生成，操作唯一记录生成方法OpUniqRecord(opid, id)：给定一个操作ID:opid和关键字id，生成的记录必须满足“除了ID列之外，所有列的内容都必须opid一一对应”
* 测试流程：
*	1.	装载数据
*	2.	运行thread_count个更新任务线程，每个线程执行如下操作：
*		a)INSERT。从[0,MAXID]之间随机选择一个随机数k，获取当前操作ID：opid,根据opid构造一条ID=k的记录，插入该记录
*		b) UPDATE。从[0,MAXID]之间随机选择一个随机数k，找到满足ID>=k的第一条记录R，获取当前操作ID：opid，令R’=OpUniqRecord(opid, (3*opid + 1)%MAXID)，更新这条记录R’
*		c) DELETE。从[0,MAXID]之间随机选择一个随机数k，随机删除 关键词为k的记录
*		d) SELECT。从[0,MAXID]之间随机选择一个随机数k，读取k对应的记录，验证R的所有列（除了ID之外）都对应同一个操作ID:opid
*	3.	运行一个读取线程，该线程不停的进行TableScan。对于扫描到的每条记录R，验证R的所有列（除了ID之外）都对应同一个操作ID:opid，并扫描各索引，验证R存在与索引中。
*	4.	为每个索引idx开启一个索引扫描线程，扫描该索引，对于扫描到的每条记录R，验证R的所有列（除了ID之外）都对应同一个操作ID:opid。
*	5.	定时启动大对象碎片整理。
*	6.	启动一个备份线程，定时进行数据库备份操作。
*	7.	运行过程中，不间断的记录，内存、cpu占有率、io统计信息，以及每秒钟执行的更新任务数。
*
* 苏斌(...)
*/

#include "Stability.h"
#include "DbConfigs.h"
#include "BlogTable.h"
#include "CountTable.h"
#include "Random.h"
#include "IntgTestHelper.h"
#include "lob/Lob.h"
#include "util/System.h"
#include "util/File.h"
#include <stdlib.h>
#include <string>
#include <iostream>
#include <sstream>

using namespace std;
using namespace ntsefunc;

bool Stability::m_isDmlRunning = true;

/** 得到用例名字 */
string Stability::getName() const {
	return "Stability test case";
}

/** 用例描述 */
string Stability::getDescription() const {
	return "Test whether the database is stable, all kinds of operations will run";
}

/**
* 启动所有检验线程以及负载线程进行稳定性测试，最后象征性的等待结束各个线程
*/
void Stability::run() {
	ts.idx = true;
	// 创建其他线程并运行
	Table *table = m_tableInfo[0]->m_table;
	m_tableChecker = new TableChecker(m_db, table, (uint)-1);
	m_lobDefrager = new LobDefrager(m_db, table, m_defragPeriod);
	m_envStatusRecorder = new EnvStatusRecorder(m_db, table, m_statusPeriod, this, m_startTime);
	m_backuper = new Backuper(m_db, table, m_backupPeriod);

	uint indexNum = table->getTableDef()->m_numIndice;
	m_indexChecker = new StableHelperThread*[indexNum];
	for (uint i = 0; i < indexNum; i++) {
		m_indexChecker[i] = new IndexChecker(m_db, table, (uint)-1, i);
		m_indexChecker[i]->start();
	}

	m_tableChecker->start();
	m_lobDefrager->start();
	m_backuper->start();
	m_envStatusRecorder->start();

	for (size_t i = 0; i < m_threadNum; i++) {
		m_threads[i]->join();
	}

	m_tableChecker->join();
	m_lobDefrager->join();
	m_backuper->join();
	m_envStatusRecorder->join();
	for (uint i = 0; i < indexNum; i++) {
		m_indexChecker[i]->join();
	}
}

/**
* 随机的进行各种操作
* @param param 线程参数
*/
void Stability::mtOperation(void *param) {
	UNREFERENCED_PARAMETER(param);

	Table *table = m_tableInfo[0]->m_table;
	u64 count = 0;

	while (true) {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("Stability::mtOperation", conn);
		++count;
		uint opKind = RandomGen::nextInt(0, 4);
		u64 key = RandomGen::nextInt(0, MAX_ID);
		TblOpResult tor;
		switch (opKind) {
			case 0:		// scan
				tor = TableDMLHelper::fetchRecord(session, table, key, NULL, NULL, true);
				//cout << "fetch: " << key << "	" << tor << endl;
				break;
			case 1:		// update
				{
					u64 updatekey = RandomGen::nextInt(0, MAX_ID);
					tor = TableDMLHelper::updateRecord(session, table, &key, &updatekey, (uint)count, false);
					//cout << "Update: " << key << "	" << updatekey << "	" << tor << endl;
					break;
				}
			case 2:		// delete
				tor = TableDMLHelper::deleteRecord(session, table, &key);
				//cout << "delete: " << key << "	" << tor << endl;
				break;
			default:	// insert
				tor = TableDMLHelper::insertRecord(session, table, key, (uint)count);
				//cout << "insert: " << key << "	" << tor << endl;
				break;
		}

		if (opKind != 0)
			m_totalUpdateOp.increment();
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}

/**
* 生成表数据
* @param totalRecSize	IN/OUT 要生成数据的大小，返回生成后的真实大小
* @param recCnt		IN/OUT 生成记录的个数，返回生成后的真实记录数
*/
void Stability::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("Stability::loadData", conn);

	assert(m_totalRecSizeLoaded > 0);
	m_tableInfo[0]->m_recCntLoaded = BlogTable::populate(session, m_tableInfo[0]->m_table, &m_totalRecSizeLoaded);
	NTSE_ASSERT(ResultChecker::checkTableToIndice(m_db, m_tableInfo[0]->m_table, true, true));
	for (uint i = 0; i < m_tableInfo[0]->m_table->getTableDef()->m_numIndice; i++) {
		NTSE_ASSERT(ResultChecker::checkIndexToTable(m_db, m_tableInfo[0]->m_table, i, true));
	}

	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_tableInfo[0]->m_recCntLoaded;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}

/**
* 启动工作线程开始进行稳定性测试
*/
void Stability::warmUp() {
	openTable();

	m_threads = new TestOperationThread*[m_threadNum];
	for (uint i = 0; i < m_threadNum; i++) {
		m_threads[i] = new TestOperationThread(this, i);
	}

	m_startTime = System::currentTimeMillis();

	for (size_t i = 0; i < m_threadNum; i++) {
		m_threads[i]->start();
	}

	Thread::msleep(5000);

	return;
}


/**
 * 验证函数，本用例不需要
 * @return true
 */
bool Stability::verify() {
	return true;
}

/**
* 检查表到索引映射的线程
*/
void TableChecker::run() {
	while (true) {
		if (!ResultChecker::checkTableToIndice(m_db, m_table, true, true)) {
			cout << "Error when checking table" << endl;
			//{
			//	Connection *conn = m_db->getConnection(false);
			//	Session *session = m_db->getSessionManager()->allocSession("TableChecker::run", conn);
			//	TableDef *tableDef = m_table->getTableDef();
			//	IndexDef *indexDef = tableDef->m_indice[0];
			//	byte *buf = new byte[tableDef->m_maxRecSize];
			//	memset(buf, 0, tableDef->m_maxRecSize);
			//	IndexScanCond cond(0, NULL, true, true, false);
			//	ScanHandle *indexHandle = m_table->indexScan(session, SI_READ, &cond, indexDef->m_numCols, indexDef->m_columns);
			//	while (m_table->getNext(indexHandle, buf)) {
			//		cout << "error:		";
			//		u64 key = RedRecord::readBigInt(tableDef, buf, 0);
			//		cout << key << endl;
			//	}
			//	m_table->endScan(indexHandle);
			//	m_db->getSessionManager()->freeSession(session);
			//	m_db->freeConnection(conn);

			//	for (uint i = 0; i < tableDef->m_numIndice; i++) {
			//		conn = m_db->getConnection(false);
			//		session = m_db->getSessionManager()->allocSession("TableChecker::run", conn);
			//		IndexScanCond cond(i, NULL, true, true, false);
			//		ScanHandle *indexHandle = m_table->indexScan(session, SI_READ, &cond, indexDef->m_numCols, indexDef->m_columns);
			//		u64 count = 0;
			//		while (m_table->getNext(indexHandle, buf)) {
			//			count++;
			//		}
			//		cout << "Total: " << count << endl;
			//		m_table->endScan(indexHandle);
			//		m_db->getSessionManager()->freeSession(session);
			//		m_db->freeConnection(conn);
			//	}

			//}
			::exit(0);
		}

		if (m_period != uint(-1))
			Thread::msleep(m_period);
	}
}

/**
* 执行一个索引到表数据映射的线程
*/
void IndexChecker::run() {
	IndexDef *indexDef = m_table->getTableDef()->m_indice[m_indexNo];

	while (true) {
		if (!ResultChecker::checkIndexToTable(m_db, m_table, m_indexNo, true)) {
			cout << "Error when checking index " << indexDef->m_name << endl;
			::exit(0);
		}

		if (m_period != uint(-1))
			Thread::msleep(m_period);
	}
}

/**
* 执行大对象碎片整理的线程
*/
void LobDefrager::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefrager::run", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	while (true) {
		if (m_period != uint(-1))
			Thread::msleep(m_period);

		u64 savePoint = memoryContext->setSavepoint();
		// 暂时先调用lob的接口，等待table实现
		LobStorage *lobStorage = m_table->getLobStorage();

		lobStorage->defrag(session);

		memoryContext->resetToSavepoint(savePoint);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
* 进行备份的线程
*/
void Backuper::run() {
	// 创建backup目录
	char n[10] = "backup";
	string path = std::string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + n;
	u64 errCode;
	File backupDir(path.c_str());
	
	// 创建frm文件（MYSQL相关)
	string frmFileName = std::string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_table->getTableDef()->m_name + System::strdup(".frm");
	File frmFile(frmFileName.c_str());
	try {
		frmFile.remove();
	} catch (NtseException &) {
	}
	errCode = frmFile.create(true, false);
	assert(errCode == File::E_NO_ERROR);
	
	while (true) {
		if (m_period != uint(-1))
			Thread::msleep(m_period);

		BackupProcess *bp = 0;
		try {
			backupDir.rmdir(true);
		} catch (NtseException &) {
		}
		errCode = backupDir.mkdir();
		assert(errCode == File::E_NO_ERROR);

		try {
			//bp = m_db->initBackup(m_db->getConfig()->m_tmpdir);
			bp = m_db->initBackup(path.c_str());
			m_db->doBackup(bp);
			m_db->finishingBackupAndLock(bp);
		} catch(NtseException &e) {
			cout << e.getMessage() << endl;
			m_db->doneBackup(bp);
			return;
		}
		m_db->doneBackup(bp);
	}
}

/**
* 记录环境统计信息的线程
*/
void EnvStatusRecorder::run() {
#ifdef WIN32
	return;
#else
	// Linux环境，执行统计信息
	/************************************************************************/
	/* 统计信息包括：内存、CPU、IO、每秒操作次数
	通过三个取道获取这些信息：
	1) iostat
	2) vmstat
	3) 统计测试用例总操作数以及操作时间
	*/
	/************************************************************************/ 
	if (!createSpecifiedFile(STATUS_FILE)) {	// 文件创建不了显示错误返回
		cout << "Create status file failed!" << endl;
		return;
	}

	while (true) {
		// 首先写入该次统计的起始时间
		u64 errNo;
		File file(STATUS_FILE);
		errNo = file.open(false);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			goto fail;

		char buffer[255];
		char timebuffer[255];
		time_t time = System::currentTimeMillis();
		System::formatTime(timebuffer, 255, (time_t*)&time);
		sprintf(buffer, "\n\n/************************************************************************************************/\n%s\n\n", timebuffer);
		u64 filesize;
		errNo = file.getSize(&filesize);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			goto fail;
	
		errNo = file.setSize(filesize + strlen(buffer));
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			goto fail;

		errNo = file.write(filesize, strlen(buffer), buffer);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			goto fail;

		filesize += strlen(buffer);

		// 计算每秒更新次数
		u64 curTime = System::currentTimeMillis();
		uint operations = m_testcase->getTotalUpdateOp();
		sprintf(buffer, "Current OP/s: %s\n\n", operations / (curTime - m_startTime) / 1000);

		errNo = file.setSize(filesize + strlen(buffer));
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			goto fail;

		errNo = file.write(filesize, strlen(buffer), buffer);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			goto fail;

		errNo = file.close();
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			goto fail;

		// 执行iostat和vmstat获得数据
		if (!executeShellCMD("iostat") || !executeShellCMD("vmstat")) {
			cout << "Run iostat/vmstat failed!" << endl;
			return;
		}

		// 等待下一个周期
		if (m_period != (uint)-1)
			Thread::msleep(m_period);
	}

fail:
	cout << "operate status file failed" << endl;
	return;

#endif
}

#ifndef WIN32

bool EnvStatusRecorder::executeShellCMD(const char *command) {
	stringstream ss;
	ss << command << " >> "
		<< STATUS_FILE
		<< endl;

	int ret = system(ss.str().c_str());
	if (ret == -1)
		return false;

	return true;
}

#endif
