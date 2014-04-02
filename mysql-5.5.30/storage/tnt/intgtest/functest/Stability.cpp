/**
* ���ɲ���5.2.1�������ݿ��ȶ�������
*
* ����Ŀ�ģ��ȶ��Բ��ԣ�ִ�и�������������า��NTSE����
* ��ģʽ��Blog
* �������ã�NTSE���ã�NC_SMALL / data_size = ntse_buf_size * 50 / thread_count = 100
* �������ݣ���ʼ���ݣ���������ݣ�ID��0��MAXID֮��������ɣ�����Ψһ��¼���ɷ���OpUniqRecord(opid, id)������һ������ID:opid�͹ؼ���id�����ɵļ�¼�������㡰����ID��֮�⣬�����е����ݶ�����opidһһ��Ӧ��
* �������̣�
*	1.	װ������
*	2.	����thread_count�����������̣߳�ÿ���߳�ִ�����²�����
*		a)INSERT����[0,MAXID]֮�����ѡ��һ�������k����ȡ��ǰ����ID��opid,����opid����һ��ID=k�ļ�¼������ü�¼
*		b) UPDATE����[0,MAXID]֮�����ѡ��һ�������k���ҵ�����ID>=k�ĵ�һ����¼R����ȡ��ǰ����ID��opid����R��=OpUniqRecord(opid, (3*opid + 1)%MAXID)������������¼R��
*		c) DELETE����[0,MAXID]֮�����ѡ��һ�������k�����ɾ�� �ؼ���Ϊk�ļ�¼
*		d) SELECT����[0,MAXID]֮�����ѡ��һ�������k����ȡk��Ӧ�ļ�¼����֤R�������У�����ID֮�⣩����Ӧͬһ������ID:opid
*	3.	����һ����ȡ�̣߳����̲߳�ͣ�Ľ���TableScan������ɨ�赽��ÿ����¼R����֤R�������У�����ID֮�⣩����Ӧͬһ������ID:opid����ɨ�����������֤R�����������С�
*	4.	Ϊÿ������idx����һ������ɨ���̣߳�ɨ�������������ɨ�赽��ÿ����¼R����֤R�������У�����ID֮�⣩����Ӧͬһ������ID:opid��
*	5.	��ʱ�����������Ƭ����
*	6.	����һ�������̣߳���ʱ�������ݿⱸ�ݲ�����
*	7.	���й����У�����ϵļ�¼���ڴ桢cpuռ���ʡ�ioͳ����Ϣ���Լ�ÿ����ִ�еĸ�����������
*
* �ձ�(...)
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

/** �õ��������� */
string Stability::getName() const {
	return "Stability test case";
}

/** �������� */
string Stability::getDescription() const {
	return "Test whether the database is stable, all kinds of operations will run";
}

/**
* �������м����߳��Լ������߳̽����ȶ��Բ��ԣ���������Եĵȴ����������߳�
*/
void Stability::run() {
	ts.idx = true;
	// ���������̲߳�����
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
* ����Ľ��и��ֲ���
* @param param �̲߳���
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
* ���ɱ�����
* @param totalRecSize	IN/OUT Ҫ�������ݵĴ�С���������ɺ����ʵ��С
* @param recCnt		IN/OUT ���ɼ�¼�ĸ������������ɺ����ʵ��¼��
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
* ���������߳̿�ʼ�����ȶ��Բ���
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
 * ��֤����������������Ҫ
 * @return true
 */
bool Stability::verify() {
	return true;
}

/**
* ��������ӳ����߳�
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
* ִ��һ��������������ӳ����߳�
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
* ִ�д������Ƭ������߳�
*/
void LobDefrager::run() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefrager::run", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	while (true) {
		if (m_period != uint(-1))
			Thread::msleep(m_period);

		u64 savePoint = memoryContext->setSavepoint();
		// ��ʱ�ȵ���lob�Ľӿڣ��ȴ�tableʵ��
		LobStorage *lobStorage = m_table->getLobStorage();

		lobStorage->defrag(session);

		memoryContext->resetToSavepoint(savePoint);
	}

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
* ���б��ݵ��߳�
*/
void Backuper::run() {
	// ����backupĿ¼
	char n[10] = "backup";
	string path = std::string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + n;
	u64 errCode;
	File backupDir(path.c_str());
	
	// ����frm�ļ���MYSQL���)
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
* ��¼����ͳ����Ϣ���߳�
*/
void EnvStatusRecorder::run() {
#ifdef WIN32
	return;
#else
	// Linux������ִ��ͳ����Ϣ
	/************************************************************************/
	/* ͳ����Ϣ�������ڴ桢CPU��IO��ÿ���������
	ͨ������ȡ����ȡ��Щ��Ϣ��
	1) iostat
	2) vmstat
	3) ͳ�Ʋ��������ܲ������Լ�����ʱ��
	*/
	/************************************************************************/ 
	if (!createSpecifiedFile(STATUS_FILE)) {	// �ļ�����������ʾ���󷵻�
		cout << "Create status file failed!" << endl;
		return;
	}

	while (true) {
		// ����д��ô�ͳ�Ƶ���ʼʱ��
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

		// ����ÿ����´���
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

		// ִ��iostat��vmstat�������
		if (!executeShellCMD("iostat") || !executeShellCMD("vmstat")) {
			cout << "Run iostat/vmstat failed!" << endl;
			return;
		}

		// �ȴ���һ������
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
