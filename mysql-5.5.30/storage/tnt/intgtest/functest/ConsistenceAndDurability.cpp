/**
 * ��֤���ݿ�ĳ־��Ժ�һ���Բ�������
 *
 * ����Ŀ�ģ���֤NTSE�־��Ժ�һ����
 * ��ģʽ��Blog��Count
 * �������ã�NTSE���ã�NC_SMALL / data_size = ntse_buf_size * 50 / thread_count = 100
 * �������ݣ���ʼ���ݣ���������ݣ�ID��0��MAXID֮��������ɲ���Ψһ��¼���ɷ���OpUniqRecord(opid, id)������һ������ID:opid�͹ؼ���id�����ɵļ�¼�������㡰����ID��֮�⣬�����е����ݶ�����opidһһ��Ӧ��
 * �������̣�
 *	1.	װ������
 *	2.	ִ��10������slave��������
 *		2.1 �����ݿ⣬ָ�����ָ�����ȡ���ݿ��tailLSN������tailLSN�ض�op.log�ļ���ɾ��lsn>=tailLSN�����в�����Ȼ���ٴδ����ݿ�ִ�лָ�����
 *		2.2 slave��Ϊÿ�ű�ֱ�����thread_count/2�����������̣߳�ÿ���߳�tidά��һ����������ops[tid]��ÿ���߳�ִ��1000������ÿ������������²�����
 *			a)INSERT����[0,MAXID]֮�����ѡ��һ�������k����ȡ��ǰ����ID��opid������opid����һ��ID=k�ļ�¼������ü�¼����ȡ������Ӧ��lsn����¼(lsn, INSERT, opid, k)�����ز�������
 *			b)UPDATE����[0,MAXID]֮�����ѡ��һ�������k��k�����ҵ�����ID>=k�ĵ�һ����¼R����ȡ��ǰ����ID��opid������opid��k�������µļ�¼����R��������������¼R ����ȡ������Ӧ��lsn����¼(lsn, update, opid, k,k��)�����ز�������
 *			c)DELETE����[0,MAXID]֮�����ѡ��һ�������k�����ɾ�� �ؼ���Ϊk�ļ�¼����ȡ������Ӧ��lsn����¼(lsn, DELETE, opid, k) �����ز�������
 *			ע������Ĳ�����Ӧlsnָ���Ƕ������ϲ��������񡱵���־���У�����һ���㣬������־��¼��������㣬���۸�������־�Ƿ����������񶼱��뱻redo�ɹ�����������¼��lsn��������һ����
 *		2.3 ��ĳ�����ʱ���ⲿ���̻߳�ָʾֹͣslave�������鲢���̵߳Ĳ������У�ʹ���ܲ������а���lsn�������С������ܲ������浽�����ļ�op.log
 *	3.	�����ݿ⡣
 *	4.	�õ���־��ĩβtailLSN
 *	5.	ɨ����赱ǰ��¼ΪR��ͨ��ɨ��������֤R���������������С�
 *	6.	�Ա��ϵ�����������������ɨ�裬����ÿ����������֤���м�¼�ܹ���ȷ��ȡ��
 *	7.	�Ӻ���ǰɨ��op.log����֤lsn<tailLsn�Ĳ���opid���Ѿ��ɹ�������delete��������֤��¼ȷʵ��ɾ��������update��������֤��¼ֵ�Ƿ�ò���opid���޸Ľ��������insert������ȷ�ϼ�¼���ڣ��Ҽ�¼ֵ����opid���޸Ľ����
 *
 * �ձ�(bsu@corp.netease.com, naturally@163.org)
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
 * lsn��С�ѵıȽϺ���
 * @param content1	��һ��lsn
 * @param content2	�ڶ���lsn
 * @return �ȽϽ��
 */
s32 LsnHeap::compare(void *content1, void *content2) {
	u64 lsn1 = *((u64*)content1);
	u64 lsn2 = *((u64*)content2);

	return lsn1 > lsn2 ? 1 : lsn1 == lsn2 ? 0 : -1;
}

/** �õ��������� */
string CDTest::getName() const {
	return "Consistency and durability test case";
}

/** �������� */
string CDTest::getDescription() const {
	return "Test whether the database is consistency and durability by random modifing and checking at last";
}


/**
 * ��������
 * �������̣߳�ִ�в���2-7�����Ƹ�������д�߳�
 * ע������ر����ݿ��ʱ�����ֻˢ��־��ˢ���ݵķ�ʽ��ģ�����ݿ���ʵ���;��������
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

		// ����ִ��һ�������ļ����ݣ����ڵ���
		backupDBFiles();

		// ����ͨ��ǿ�Ʋ��ָ������ݿ⣬�õ�tailLSN������undo����д������־�õ�����LSN
		m_db = Database::open(m_config, false, -1);
		u64 tailLsn = getRealTailLSN();
		m_db->close(false, false);
		m_db = NULL;

		// ��δ����ݿ��ٽ��������Ļָ�����
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
 * ���ɱ�����
 * @param totalRecSize	IN/OUT Ҫ�������ݵĴ�С���������ɺ����ʵ��С
 * @param recCnt		IN/OUT ���ɼ�¼�ĸ������������ɺ����ʵ��¼��
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
 * Ԥ�Ⱥ���
 */
void CDTest::warmUp() {
	openTable();
	return;
}

/**
 * ���б������Լ����������˫����֤
 */
bool CDTest::verify() {
	openTable(false);
	for (uint i = 0; i < m_tables; i++) {
		Table *table = m_tableInfo[i]->m_table;
		cout << endl << "Check table: " << table->getTableDef()->m_name << endl;

		// ����������֮��������ṹһ��
		if (!ResultChecker::checkIndice(m_db, table))
			return false;
		cout << "Check indice successfully" << endl;

		// ���ѱ���Ľṹ��ȷ��
		if (!ResultChecker::checkHeap(m_db, table))
			return false;
		cout << "Check heap successfully" << endl;

		// ����������ѵ�һ����
		if (!ResultChecker::checkIndiceToTable(m_db, table, true, false)) {
			// ��һ�µ�����£��ټ��ѵ����������Ƿ�һ��
			if (!ResultChecker::checkTableToIndice(m_db, table, true, true))
				return false;
			cout << "Check table to indice successfully" << endl;
			return false;
		}
		cout << "Check indice to table successfully" << endl;

		// ���ѵ�����������һ����
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
 * ���̲߳�������
 * @param param	��������
 */
void CDTest::mtOperation(void *param) {
	UNREFERENCED_PARAMETER(param);
	return;
}


/**
 * ����opInfo��־���У�lsn���ڵ�ǰ��־�ļ�tailLSN����־��Ϣ
 * @param tailLsn	��־����lsn
 */
void CDTest::discardTailOpInfo(u64 tailLsn) {
	assert(m_db != NULL);
	cout << "DiscardTailOpInfo: TailLSN - " << tailLsn << endl;

	File file(OP_LOG_FILE);
	u64 errNo;
	errNo = file.open(false);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	// ����ȷ��tailLsn��Ӧ���ļ�λ��ƫ��
	uint logLen = sizeof(OperationInfo);
	const uint blockSize = logLen * 100;
	byte *buffer = new byte[blockSize];
	u64 fileend;
	errNo = file.getSize(&fileend);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	bool discarded = false;
	while (true) {
		if (fileend == 0)	// ������־����Ч
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

	// ��������־�ضϣ�������������ļ����
	errNo = file.setSize(fileend);
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	errNo = file.sync();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);
	errNo = file.close();
	assert(File::getNtseError(errNo) == File::E_NO_ERROR);

	delete[] buffer;
}

/**
 * �鲢���̵߳�opInfo���飬ͬʱ����ָ����־�ļ�ĩβ
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

	// ������С����������̵߳Ĳ�����Ϣ
	LsnHeap lsnHeap((uint)m_threadNum);
	for (uint i = 0; i < m_threadNum; i++) {
		OperationInfo *opinfo = opinfos[i];
		if (sizes[i] != 0)
			lsnHeap.push(i, &(opinfo[0].m_lsn));
	}

	// �����ι鲢���������Ϣ�����������Ϣ�ļ�
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
		if (threadNo == -1 || cur >= buffer + blockSize) {	// ��������buffer������Ҫˢ����ǰ������־
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

		if (useds[threadNo] < sizes[threadNo]) {	// ����ȡ��һ���������
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
 * ������־������֤
 * @param tailLsn	��־����lsn
 * @return ���ؼ������ȷ���
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
		// ��ȡ�ļ���ǰλ�õ�ǰһ���ļ���
		u32 readSize = (fileend < blockSize) ? (u32)fileend : blockSize;
		errNo = file.read(fileend - readSize, readSize, buffer);
		assert(File::getNtseError(errNo) == File::E_NO_ERROR);
		fileend -= readSize;

		// ��鵱ǰ��־���ڵ���־
		uint pos = blockSize - logLen;
		for (uint i = batchLogs; i > 0; i--) {
			OperationInfo *log = (OperationInfo*)(&buffer[pos]);
			pos -= logLen;
			//cout << "Check log lsn: " << log->m_lsn << "	" << log->m_succ << "	" << ((log->m_opKind == OP_INSERT) ? "Insert" : (log->m_opKind == OP_DELETE) ? "Delete" : "Update") << "	" << log->m_opKey << "	" << log->m_updateKey << endl;

			if (log->m_succ) {	// �����־��Ӧ�����ɹ�
				assert(log->m_lsn < tailLsn);
				set <u64> *curSet = &doneOps[log->m_tableNo];
				bool checkOK = true;
				// ������δ��飬ִ�м�����
				if (curSet->insert(log->m_opKey).second) {
					switch (log->m_opKind) {
						case OP_INSERT:	// ȷ�ϸ������
							checkOK = (TableDMLHelper::fetchRecord(session, m_tableInfo[log->m_tableNo]->m_table, log->m_opKey, NULL, NULL, true) == SUCCESS);
							break;
						default:
							assert(log->m_opKind == OP_DELETE || log->m_opKind == OP_UPDATE);
							if (!(log->m_opKind == OP_UPDATE && log->m_opKey == log->m_updateKey))	// ���²�������������������֤ԭʼ��ֵ
								checkOK = (TableDMLHelper::fetchRecord(session, m_tableInfo[log->m_tableNo]->m_table, log->m_opKey, NULL, NULL, true) == FAIL);
							break;
					}
				}

				if (checkOK && log->m_opKind == OP_UPDATE) {
					if (curSet->insert(log->m_updateKey).second)	// ���update�������ȷ�ԣ��������
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
 * �߳�ʵ��
 *************************************************/

/**
 * ʵ����������ݿ�ķ��ʣ�ͬʱά������������Ϣ
 */
void SlaveThread::run() {
	Database *db = ((CDTest*)m_testcase)->getDatabase();
	Connection *conn = db->getConnection(false);

	uint tableNo = ((CDTest*)m_testcase)->chooseTableNo((uint)m_threadId);
	Table *opTable = ((CDTest*)m_testcase)->getTable(tableNo);

	uint count = 0, ops = 0;
	for (; count < OPERATION_TIMES; count++) {
		Session *session = db->getSessionManager()->allocSession("SlaveThread::run", conn);
		// ����޸Ĳ���
		u8 opKind = (u8)RandomGen::nextInt(0, 3);
		u64 k = RandomGen::nextInt(0, CDTest::MAX_ID);
		u64 kplus = 0;
		TblOpResult tbr;
		switch (opKind) {
			case OP_INSERT:	// ����
				tbr = TableDMLHelper::insertRecord(session, opTable, k, count);
				//if (tbr == SUCCESS) {
				//	cout << "insert: " << tableNo << "	" << k << "	" << tbr << endl;
				//}
				break;
			case OP_UPDATE:	// ����
				kplus = RandomGen::nextInt(0, CDTest::MAX_ID);
				tbr = TableDMLHelper::updateRecord(session, opTable, &k, &kplus, count, false);
				//if (tbr == SUCCESS) {
				//	cout << "update: " << tableNo << "	" << k << "	" << kplus << "	" << tbr << endl;
				//}
				break;
			default: // ɾ��
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
 * ����ָ���Ĳ�����Ϣ��־
 * @param succ		��־��Ӧ�����Ƿ�ɹ�
 * @param tableNo	�����ı���
 * @param opCount	���������
 * @param lsn		������lsn
 * @param opKind	��������
 * @param opId		�����ļ�ֵ���ݸ���ֵ����
 * @param k			������¼������
 * @param kplus		����Ǹ��²�����Ϊ���º��¼������������Ϊ0
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
 * �õ������ص�ǰ��־�ļ�����ʵLSN
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
 * ��������db�����ļ�
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