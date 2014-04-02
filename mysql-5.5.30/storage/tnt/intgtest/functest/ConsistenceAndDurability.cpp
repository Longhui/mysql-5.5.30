/**
 * 验证数据库的持久性和一致性测试用例
 *
 * 测试目的：验证NTSE持久性和一致性
 * 表模式：Blog和Count
 * 测试配置：NTSE配置：NC_SMALL / data_size = ntse_buf_size * 50 / thread_count = 100
 * 测试数据：初始数据：随机新数据，ID在0和MAXID之间随机生成操作唯一记录生成方法OpUniqRecord(opid, id)：给定一个操作ID:opid和关键字id，生成的记录必须满足“除了ID列之外，所有列的内容都必须opid一一对应”
 * 测试流程：
 *	1.	装载数据
 *	2.	执行10次如下slave操作序列
 *		2.1 打开数据库，指定不恢复，读取数据库的tailLSN，根据tailLSN截断op.log文件，删除lsn>=tailLSN的所有操作。然后再次打开数据库执行恢复操作
 *		2.2 slave中为每张表分别运行thread_count/2个更新任务线程，每个线程tid维护一个操作序列ops[tid]，每个线程执行1000次任务，每个任务包括如下操作：
 *			a)INSERT。从[0,MAXID]之间随机选择一个随机数k，获取当前操作ID：opid，根据opid构造一条ID=k的记录，插入该记录，获取操作对应的lsn，记录(lsn, INSERT, opid, k)到本地操作序列
 *			b)UPDATE。从[0,MAXID]之间随机选择一个随机数k和k’，找到满足ID>=k的第一条记录R，获取当前操作ID：opid，根据opid和k’生成新的记录内容R’，更新这条记录R ，获取操作对应的lsn，记录(lsn, update, opid, k,k’)到本地操作序列
 *			c)DELETE。从[0,MAXID]之间随机选择一个随机数k，随机删除 关键词为k的记录，获取操作对应的lsn，记录(lsn, DELETE, opid, k) 到本地操作序列
 *			注：这里的操作对应lsn指的是对于以上操作“事务”的日志当中，存在一个点，事务日志记录超过这个点，无论该事务日志是否完整，事务都必须被redo成功，本用例记录的lsn就是这样一个点
 *		2.3 在某个随机时刻外部主线程会指示停止slave操作，归并各线程的操作序列，使得总操作序列按照lsn升序排列。保存总操作保存到磁盘文件op.log
 *	3.	打开数据库。
 *	4.	得到日志的末尾tailLSN
 *	5.	扫描表，设当前记录为R，通过扫描索引验证R出现在所有索引中。
 *	6.	对表上的所有索引进行索引扫描，对于每个索引，验证所有记录能够正确读取。
 *	7.	从后往前扫描op.log，验证lsn<tailLsn的操作opid都已经成功：对于delete操作，验证记录确实被删除；对于update操作，验证记录值是否该操作opid的修改结果；对于insert操作，确认记录存在，且记录值等于opid的修改结果。
 *
 * 苏斌(bsu@corp.netease.com, naturally@163.org)
 */


#include "ConsistenceAndDurability.h"
#include "DbConfigs.h"
#include "BlogTable.h"
#include "CountTable.h"
#include "Random.h"
#include "IntgTestHelper.h"
#include "btree/OuterSorter.h"
#include "util/File.h"
#include "misc/Trace.h"
#include <set>

using namespace std;
using namespace ntsefunc;


class LsnHeap : public MinHeap {
public:
	LsnHeap() {
		_init(DEFAULT_HEAP_SIZE);
	}

	LsnHeap(uint size) {
		_init(size);
	}
private:
	virtual s32 compare(void *content1, void *content2);
};

/**
 * lsn最小堆的比较函数
 * @param content1	第一个lsn
 * @param content2	第二个lsn
 * @return 比较结果
 */
s32 LsnHeap::compare(void *content1, void *content2) {
	u64 lsn1 = *((u64*)content1);
	u64 lsn2 = *((u64*)content2);

	return lsn1 > lsn2 ? 1 : lsn1 == lsn2 ? 0 : -1;
}

/** 得到用例名字 */
string CDTest::getName() const {
	return "Consistency and durability test case";
}

/** 用例描述 */
string CDTest::getDescription() const {
	return "Test whether the database is consistency and durability by random modifing and checking at last";
}


/**
 * 测试主体
 * 主控制线程，执行步骤2-7，控制各个操作写线程
 * 注意这里关闭数据库的时候采用只刷日志不刷数据的方式，模拟数据库访问的中途崩溃场景
 */
void CDTest::run() {
	vs.idx = true;
	vs.lsn = true;
	vs.hp = true;
	ts.recv = true;
	ts.hp = true;
	createSpecifiedFile(OP_LOG_FILE);

	for (uint times = 0; times < SLAVE_RUN_TIMES; times++) {
		cout << "Start " << times << " test" << endl;

		m_threadSignal = true;
		m_threads = new TestOperationThread*[m_threadNum];
		for (uint i = 0; i < m_threadNum; i++) {
			m_threads[i] = new SlaveThread(this, i);
		}

		for (size_t i = 0; i < m_threadNum; i++) {
			m_threads[i]->start();
		}

		Thread::msleep(RandomGen::nextInt(0, 30000) + 60000);
		m_threadSignal = false;

		for (size_t i = 0; i < m_threadNum; i++) {
			m_threads[i]->join();
		}

		mergeMTSOpInfoAndSaveToFile();

		for (size_t i = 0; i < m_threadNum; i++) {
			delete m_threads[i];
		}
		delete[] m_threads;
		m_threads = NULL;

		closeDatabase(false);

		// 这里执行一次数据文件备份，用于调试
		backupDBFiles();

		// 首先通过强制不恢复打开数据库，得到tailLSN，避免undo操作写补偿日志得到错误LSN
		m_db = Database::open(m_config, false, -1);
		u64 tailLsn = getRealTailLSN();
		m_db->close(false, false);
		m_db = NULL;

		// 这次打开数据库再进行真正的恢复操作
		openTable(false, 1);

		discardTailOpInfo(tailLsn);

		closeDatabase(true);
		if (!verify()) {
			cout << "Test case failed" << endl;
			NTSE_ASSERT(false);
			return;
		}
		openTable(false);

		cout << "Done " << times << " test successfully" << endl;
	}

	closeDatabase(true);
}

/**
 * 生成表数据
 * @param totalRecSize	IN/OUT 要生成数据的大小，返回生成后的真实大小
 * @param recCnt		IN/OUT 生成记录的个数，返回生成后的真实记录数
 */
void CDTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::loadData", conn);

	assert(m_totalRecSizeLoaded > 0);
	m_tableInfo[0]->m_recCntLoaded = BlogTable::populate(session, m_tableInfo[0]->m_table, &m_totalRecSizeLoaded, CDTest::MAX_ID, 0);
	m_tableInfo[1]->m_recCntLoaded = CountTable::populate(session, m_tableInfo[1]->m_table, &m_totalRecSizeLoaded, CDTest::MAX_ID, 0);

	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_tableInfo[0]->m_recCntLoaded + m_tableInfo[1]->m_recCntLoaded;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}

/**
 * 预热函数
 */
void CDTest::warmUp() {
	openTable();
	return;
}

/**
 * 进行表到索引以及索引到表的双向验证
 */
bool CDTest::verify() {
	openTable(false);
	for (uint i = 0; i < m_tables; i++) {
		Table *table = m_tableInfo[i]->m_table;
		cout << endl << "Check table: " << table->getTableDef()->m_name << endl;

		// 检查各个索引之间的项数结构一致
		if (!ResultChecker::checkIndice(m_db, table))
			return false;
		cout << "Check indice successfully" << endl;

		// 检查堆本身的结构正确性
		if (!ResultChecker::checkHeap(m_db, table))
			return false;
		cout << "Check heap successfully" << endl;

		// 检查索引到堆的一致性
		if (!ResultChecker::checkIndiceToTable(m_db, table, true, false)) {
			// 不一致的情况下，再检查堆到各个索引是否一致
			if (!ResultChecker::checkTableToIndice(m_db, table, true, true))
				return false;
			cout << "Check table to indice successfully" << endl;
			return false;
		}
		cout << "Check indice to table successfully" << endl;

		// 检查堆到各个索引的一致性
		if (!ResultChecker::checkTableToIndice(m_db, table, true, true))
			return false;
		cout << "Check table to indice successfully" << endl;
	}
	
	u64 tailLsn = m_db->getTxnlog()->tailLsn();
	bool succ = checkOpLogs(tailLsn);
	NTSE_ASSERT(succ);
	closeDatabase(true);

	return succ;
}

/**
 * 多线程操作函数
 * @param param	函数参数
 */
void CDTest::mtOperation(void *param) {
	UNREFERENCED_PARAMETER(param);
	return;
}


/**
 * 丢弃opInfo日志当中，lsn大于当前日志文件tailLSN的日志信息
 * @param tailLsn	日志结束lsn
 */
void CDTest::discardTailOpInfo(u64 tailLsn) {
	assert(m_db != NULL);
	cout << "DiscardTailOpInfo: TailLSN - " << tailLsn << endl;

	File file(OP_LOG_FILE);
	u64 errNo;
	errNo = file.open(false);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	// 首先确定tailLsn对应的文件位置偏移
	uint logLen = sizeof(OperationInfo);
	const uint blockSize = logLen * 100;
	byte *buffer = new byte[blockSize];
	u64 fileend;
	errNo = file.getSize(&fileend);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	bool discarded = false;
	while (true) {
		if (fileend == 0)	// 所有日志都无效
			break;
		
		uint readSize = (uint)(fileend > blockSize ? blockSize : fileend);
		fileend -= readSize;
		errNo = file.read(fileend, readSize, buffer);
		assert(File::getNtseError(errNo) == File::E_NO_ERROR);
		s32 pos = (s32)(blockSize - logLen);
		bool found = false;
		while (pos >= 0) {
			OperationInfo *info = (OperationInfo*)(buffer + pos);

			if (info->m_lsn <= tailLsn) {
				found = true;
				break;
			}
			//cout << "Discard log : " << info->m_lsn << "	" << info->m_succ << ((info->m_opKind == OP_INSERT) ? "Insert" : (info->m_opKind == OP_DELETE) ? "Delete" : "Update") << "	" << info->m_opKey << "	" << info->m_updateKey << endl;
			pos -= logLen;
			discarded = true;
		}

		if (found) {
			fileend = fileend + pos + logLen;
			break;
		}
	}

	if (discarded)
		cout << "Some op logs were discarded." << endl;

	// 将多余日志截断，极端情况导致文件晴空
	errNo = file.setSize(fileend);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	errNo = file.sync();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	errNo = file.close();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	delete[] buffer;
}

/**
 * 归并各线程的opInfo数组，同时存入指定日志文件末尾
 */
void CDTest::mergeMTSOpInfoAndSaveToFile() {
	OperationInfo **opinfos = new OperationInfo*[m_threadNum];
	uint *sizes = new uint[m_threadNum];
	uint *useds = new uint[m_threadNum];
	memset(useds, 0, sizeof(uint) * m_threadNum);
	uint totalOps = 0;
	for (uint i = 0; i < m_threadNum; i++) {
		opinfos[i] = ((SlaveThread*)m_threads[i])->getOpInfo(&(sizes[i]));
		totalOps += sizes[i];
	}

	// 创建最小堆排序各个线程的操作信息
	LsnHeap lsnHeap((uint)m_threadNum);
	for (uint i = 0; i < m_threadNum; i++) {
		OperationInfo *opinfo = opinfos[i];
		if (sizes[i] != 0)
			lsnHeap.push(i, &(opinfo[0].m_lsn));
	}

	// 分批次归并排序操作信息并输入操作信息文件
	uint logLen = sizeof(OperationInfo);
	const uint blockSize = logLen * 100;
	byte *buffer = new byte[blockSize];
	byte *cur = buffer;

	u64 errNo;
	File file(OP_LOG_FILE);
	errNo = file.open(false);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	u64 filesize;
	errNo = file.getSize(&filesize);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	u64 lastLsn = 0;
	uint sortedOps = 0;
	while (true) {
		uint threadNo = lsnHeap.pop();
		if (threadNo == -1 || cur >= buffer + blockSize) {	// 结束或者buffer满都需要刷出当前缓存日志
			u64 extendSize = (u64)(cur - buffer);
			errNo = file.setSize(filesize + extendSize);
			assert(File::getNtseError(errNo) == File::E_NO_ERROR);
			errNo = file.write(filesize, (u32)extendSize, buffer);
			assert(File::getNtseError(errNo) == File::E_NO_ERROR);
			filesize += extendSize;
			cur = buffer;

			if (threadNo == -1)
				break;
		}

		OperationInfo *opinfo = opinfos[threadNo];
		memcpy(cur, &(opinfo[useds[threadNo]]), logLen);
		{
			OperationInfo *curInfo = (OperationInfo*)cur;
			assert(curInfo->m_lsn > lastLsn);
			lastLsn = curInfo->m_lsn;
		}
		cur = cur + logLen;
		++useds[threadNo];
		++sortedOps;

		if (useds[threadNo] < sizes[threadNo]) {	// 继续取下一项进行排序
			lsnHeap.push(threadNo, &(opinfo[useds[threadNo]].m_lsn));
		}
	}

	assert(sortedOps == totalOps);

	errNo = file.sync();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	errNo = file.close();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	delete[] buffer;

	delete[] opinfos;
	delete[] sizes;
	delete[] useds;
}


/**
 * 根据日志进行验证
 * @param tailLsn	日志结束lsn
 * @return 返回检查结果正确与否
 */
bool CDTest::checkOpLogs(u64 tailLsn) {
	File file(OP_LOG_FILE);
	u64 errNo;
	errNo = file.open(false);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	uint logLen = sizeof(OperationInfo);
	uint batchLogs = 100;
	const uint blockSize = logLen * batchLogs;
	byte *buffer = new byte[blockSize];
	u64 fileend;
	errNo = file.getSize(&fileend);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("IndexCreationTest::loadData", conn);

	set <u64> *doneOps = new set <u64>[m_tables];
	while (fileend != 0) {
		// 读取文件当前位置的前一个文件块
		u32 readSize = (fileend < blockSize) ? (u32)fileend : blockSize;
		errNo = file.read(fileend - readSize, readSize, buffer);
		assert(File::getNtseError(errNo) == File::E_NO_ERROR);
		fileend -= readSize;

		// 检查当前日志块内的日志
		uint pos = blockSize - logLen;
		for (uint i = batchLogs; i > 0; i--) {
			OperationInfo *log = (OperationInfo*)(&buffer[pos]);
			pos -= logLen;
			//cout << "Check log lsn: " << log->m_lsn << "	" << log->m_succ << "	" << ((log->m_opKind == OP_INSERT) ? "Insert" : (log->m_opKind == OP_DELETE) ? "Delete" : "Update") << "	" << log->m_opKey << "	" << log->m_updateKey << endl;

			if (log->m_succ) {	// 检查日志对应操作成功
				assert(log->m_lsn < tailLsn);
				set <u64> *curSet = &doneOps[log->m_tableNo];
				bool checkOK = true;
				// 如果该项还未检查，执行检查操作
				if (curSet->insert(log->m_opKey).second) {
					switch (log->m_opKind) {
						case OP_INSERT:	// 确认该项存在
							checkOK = (TableDMLHelper::fetchRecord(session, m_tableInfo[log->m_tableNo]->m_table, log->m_opKey, NULL, NULL, true) == SUCCESS);
							break;
						default:
							assert(log->m_opKind == OP_DELETE || log->m_opKind == OP_UPDATE);
							if (!(log->m_opKind == OP_UPDATE && log->m_opKey == log->m_updateKey))	// 更新操作不更新主键，不验证原始键值
								checkOK = (TableDMLHelper::fetchRecord(session, m_tableInfo[log->m_tableNo]->m_table, log->m_opKey, NULL, NULL, true) == FAIL);
							break;
					}
				}

				if (checkOK && log->m_opKind == OP_UPDATE) {
					if (curSet->insert(log->m_updateKey).second)	// 检查update后项的正确性，必须存在
						checkOK = (TableDMLHelper::fetchRecord(session, m_tableInfo[log->m_tableNo]->m_table, log->m_updateKey, NULL, NULL, true) == SUCCESS);
				}

				if (!checkOK) {
					cout << "Check operation log failed" 
						<< "error log information:"
						<< "table" << log->m_tableNo << "	"
						<< "lsn: " << log->m_lsn << "	"
						<< "opid: " << log->m_opId << "	"
						<< "opKind: " << ((log->m_opKind == OP_INSERT) ? "Insert" : (log->m_opKind == OP_DELETE) ? "Delete" : "Update") << "	"
						<< "opKey: " << log->m_opKey << "	"
						<< "opUpdateKey: " << log->m_updateKey << "	"
						<< endl;

					errNo = file.close();
					assert(File::getNtseError(errNo) == File::E_NO_ERROR);

					m_db->getSessionManager()->freeSession(session);
					m_db->freeConnection(conn);
					
					delete [] buffer;
					delete [] doneOps;

					return false;
				}
			}
		}
	}

	errNo = file.close();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	delete [] buffer;
	delete [] doneOps;

	return true;
}



/**************************************************
 * 线程实现
 *************************************************/

/**
 * 实现随机对数据库的访问，同时维护访问序列信息
 */
void SlaveThread::run() {
	Database *db = ((CDTest*)m_testcase)->getDatabase();
	Connection *conn = db->getConnection(false);

	uint tableNo = ((CDTest*)m_testcase)->chooseTableNo((uint)m_threadId);
	Table *opTable = ((CDTest*)m_testcase)->getTable(tableNo);

	uint count = 0, ops = 0;
	for (; count < OPERATION_TIMES; count++) {
		Session *session = db->getSessionManager()->allocSession("SlaveThread::run", conn);
		// 随机修改操作
		u8 opKind = (u8)RandomGen::nextInt(0, 3);
		u64 k = RandomGen::nextInt(0, CDTest::MAX_ID);
		u64 kplus = 0;
		TblOpResult tbr;
		switch (opKind) {
			case OP_INSERT:	// 插入
				tbr = TableDMLHelper::insertRecord(session, opTable, k, count);
				//if (tbr == SUCCESS) {
				//	cout << "insert: " << tableNo << "	" << k << "	" << tbr << endl;
				//}
				break;
			case OP_UPDATE:	// 更新
				kplus = RandomGen::nextInt(0, CDTest::MAX_ID);
				tbr = TableDMLHelper::updateRecord(session, opTable, &k, &kplus, count, false);
				//if (tbr == SUCCESS) {
				//	cout << "update: " << tableNo << "	" << k << "	" << kplus << "	" << tbr << endl;
				//}
				break;
			default: // 删除
				tbr = TableDMLHelper::deleteRecord(session, opTable, &k);
				//if (tbr == SUCCESS) {
				//	cout << "delete: " << tableNo << "	" << k << "	" << tbr << endl;
				//}
				break;
		}
		u64 lsn = session->getTxnDurableLsn();
		if (tbr == SUCCESS || tbr == FAIL)
			saveLastOpInfo((tbr == SUCCESS), tableNo, ops++, lsn, opKind, count, (uint)k, (uint)kplus);
		db->getSessionManager()->freeSession(session);

		if (((CDTest*)m_testcase)->getSignal() == false)
			break;
	}

	assert(ops <= OPERATION_TIMES);
	m_runTimes = ops;

	db->freeConnection(conn);
}


/**
 * 保存指定的操作信息日志
 * @param succ		日志对应操作是否成功
 * @param tableNo	操作的表编号
 * @param opCount	操作的序号
 * @param lsn		操作的lsn
 * @param opKind	操作类型
 * @param opId		操作的键值根据该数值生成
 * @param k			操作记录的主键
 * @param kplus		如果是更新操作，为更新后记录的主键，否则为0
 */
void SlaveThread::saveLastOpInfo(bool succ, uint tableNo, uint opCount, u64 lsn, u8 opKind, uint opId, uint k, uint kplus) {
	assert(opCount < OPERATION_TIMES);
	assert(opCount == 0 || lsn > m_opLog[opCount - 1].m_lsn);
	m_opLog[opCount].m_succ = succ;
	m_opLog[opCount].m_tableNo = tableNo;
	m_opLog[opCount].m_lsn = lsn;
	m_opLog[opCount].m_opId = opId;
	m_opLog[opCount].m_opKind = opKind;
	m_opLog[opCount].m_opKey = k;
	m_opLog[opCount].m_updateKey = kplus;
	ftrace(ts.recv, tout << succ << lsn << opKind << k << kplus);
}

/**
 * 得到并返回当前日志文件的真实LSN
 */
u64 ntsefunc::CDTest::getRealTailLSN() {
	Txnlog *txnLog = m_db->getTxnlog();
	u64 endLsn = txnLog->tailLsn();
	u64 startLsn = Txnlog::MIN_LSN;
	LogScanHandle *handle = txnLog->beginScan(startLsn, endLsn);
	u64 tailLsn = Txnlog::MIN_LSN;
	while (true) {
		LogScanHandle *hdl = txnLog->getNext(handle);
		if (hdl == NULL)
			break;
		tailLsn = hdl->curLsn();
	}
	txnLog->endScan(handle);

	return tailLsn;
}

/**
 * 备份整个db所有文件
 */
void ntsefunc::CDTest::backupDBFiles() {
	backupFiles(m_config->m_tmpdir, TABLE_NAME_BLOG, useMms, true);
	backupFiles(m_config->m_tmpdir, TABLE_NAME_COUNT, useMms, true);
	u64 errNo;
	errNo = File::copyFile("testdb/ntse_ctrl1", "testdb/ntse_ctrl", true);
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		cout << File::explainErrno(errNo) << endl;
		return;
	}

	for (uint i = 0; i < 20; i++) {
		char logname[80];
		char logbkname[80];
		sprintf(logname, "testdb/ntse_log.%d", i);
		sprintf(logbkname, "testdb/ntse_log1.%d", i);
		errNo = File::copyFile(logbkname, logname, true);
		if (File::getNtseError(errNo) != File::E_NO_ERROR) {
			cout << File::explainErrno(errNo) << endl;
			continue;
		}
	}
}