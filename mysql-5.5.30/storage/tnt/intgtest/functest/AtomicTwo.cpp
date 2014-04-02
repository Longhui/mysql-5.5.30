/**
 * ���ɲ���5.1.2���Լ�¼����ԭ����
 *
 * ����Ŀ�ģ���֤NTSE��¼������ԭ���ԣ��ο����ݷ���������������
 * ��ģʽ��Blog��Count
 * �������ã�NTSE���ã�NC_SMALL thread_count = 500
 * �������ݣ�ͬ5.1.1
 * �������̣�
 *	1.	����100����¼����¼�Ĺؼ���ID��[0,MAXID]֮��
 *	2.	����һ����СΪMAXID������A�������һ�����ɵļ�¼��A��
 *	3.	Ϊÿ�ű�ֱ�����thread_count/2�����������̣߳�ÿ���߳�ִ��10000������ÿ������������²�����
 *		a)��[0, MAXID]֮��ѡ��һ�������id
 *		b)ͨ��IndexScan��λ����ID>=id�ĵ�һ����¼R
 *		c)���ѡ��R��3��C1,C2,C3��������Щ��
 *		d)��Ӧ�ĸ���A[id]
 *		e)���ID�б�����Ϊid��, ��ô�ƶ�A[id]��A[id��]
 *		f)����IndexScan
 *	4.	�����������������֮�󣬽���һ�α�ɨ�裬���ڵ�ǰ��¼ΪR����֤R�� A[R.ID]��ȫ��ͬ
 *
 * �ձ�(...)
 */

#include "AtomicTwo.h"
#include "DbConfigs.h"
#include "Random.h"
#include "IntgTestHelper.h"
#include "btree/Index.h"
#include <sstream>
#include "util/File.h"
#include "util/Thread.h"

using namespace std;
using namespace ntsefunc;

/** �õ��������� */
string AtomicTwo::getName() const {
	return "Atomic test: update primary key";
}

/** �������� */
string AtomicTwo::getDescription() const {
	return "Test whether the database operation is atomic by random updating and checking at last";
}

/**
 * ����ִ�к���
 */
void AtomicTwo::run() {
	ts.intg = true;
	initMirror();
	m_threads = new TestOperationThread*[m_threadNum];
	// ���ɸ����߳�ִ�в���
	for (uint i = 0; i < m_threadNum; i++) {
		m_threads[i] = new TestOperationThread((TestCase*)this, i);
	}

	for (size_t i = 0; i < m_threadNum; i++) {
		m_threads[i]->start();
	}

	for (size_t i = 0; i < m_threadNum; i++) {
		m_threads[i]->join();
	}
}

/**
* ���ɱ�����
* @param totalRecSize	IN/OUT Ҫ�������ݵĴ�С���������ɺ����ʵ��С
* @param recCnt		IN/OUT ���ɼ�¼�ĸ������������ɺ����ʵ��¼��
*/
void AtomicTwo::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("AtomicOne::loadData", conn);

	u64 dataSize1 = m_recCount * m_tableInfo[0]->m_table->getTableDef()->m_maxRecSize;
	m_tableInfo[0]->m_recCntLoaded = BlogTable::populate(session, m_tableInfo[0]->m_table, &dataSize1);
	u64 dataSize2 = m_recCount * m_tableInfo[1]->m_table->getTableDef()->m_maxRecSize;
	m_tableInfo[1]->m_recCntLoaded = CountTable::populate(session, m_tableInfo[1]->m_table, &dataSize2);

	*totalRecSize = dataSize1 + dataSize2;
	*recCnt = m_recCount;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}

/**
 * Ԥ�Ⱥ���
 */
void AtomicTwo::warmUp() {
	openTable();
	return;
}


/**
 * ��֤�ڴ澵����������ݵ�һ����
 * @param minTable	������С���
 * @param maxTable	���������
 * @return �Ƿ�һ��
 */
bool AtomicTwo::verifyrange(uint minTable, uint maxTable) {
	assert(minTable <= maxTable);
	// ��һ��֤ÿ�ű���ڴ����ݵ�һ����
	if (minTable == (uint)-1 && maxTable == (uint)-1) {
		minTable = 0;
		maxTable = m_tables;
	}
	for (uint i = minTable; i < maxTable; i++) {
		Table *table = m_tableInfo[i]->m_table;
		TableDef *tableDef = table->getTableDef();
		u16 *columns = RecordHelper::getAllColumns(tableDef);
		byte *buf = new byte[tableDef->m_maxRecSize];

		//// ��֤�Ѻ�������һ����
		/*
		assert(ResultChecker::checkIndice(m_db, table));
		assert(ResultChecker::checkIndiceToTable(m_db, table, true, false));
		assert(ResultChecker::checkTableToIndice(m_db, table, true, false));
		*/

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("AtomicOne::verify", conn);

		TblScan *handle = table->tableScan(session, OP_READ, tableDef->m_numCols, columns);
		while (table->getNext(handle, buf)) {
			// ��ȡ������ֵ֮��Ա��ڴ澵��
			u64 key = RedRecord::readBigInt(tableDef, buf, 0);
			assert(key < MAX_ID);
			/*
			if (m_mirror[i][key][0] == 0) {
				Connection *conn = m_db->getConnection(false);
				Session *session = m_db->getSessionManager()->allocSession("AtomicOne::verify", conn);

				SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD, INVALID_ROW_ID);
				SubRecord *findKey = keyBuilder.createSubRecordById("0", &key);

				IndexScanCond cond(0, findKey, true, true, true);
				u16 *columns = RecordHelper::getAllColumns(tableDef);
				TblScan *scanHandle = table->indexScan(session, SI_READ, &cond, tableDef->m_numCols, columns);
				byte *buf = new byte [tableDef->m_maxRecSize];

				//assert(table->getNext(scanHandle, buf));
				bool exist = table->getNext(scanHandle, buf);

				if (!exist) {
					byte recordbuf[Limits::PAGE_SIZE];
					Record record;
					record.m_data = recordbuf;
					record.m_format = table->getTableDef()->m_recFormat;

					exist = table->getHeap()->getRecord(session, handle->getCurrentRid(), &record);
					assert(exist);
					u64 heapKey = RedRecord::readBigInt(table->getTableDef(), recordbuf, 0);
					assert(key == heapKey);
				}



				delete [] buf;

				delete [] columns;

				freeSubRecord(findKey);
				m_db->getSessionManager()->freeSession(session);
				m_db->freeConnection(conn);

   			}
			*/
			assert(m_mirror[i][key][0] != 0);
			if (!ResultChecker::checkRecordToRecord(tableDef, &m_mirror[i][key][1], buf)) {
				cout << "Check table " << tableDef->m_name << " failed, because of unconsistency between physical data and memory data" << endl;
				table->endScan(handle);
				m_db->getSessionManager()->freeSession(session);
				m_db->freeConnection(conn);
				return false;
			}
		}
		table->endScan(handle);

		/*** ������֤ ***/
		for (u64 itkey = 0; itkey < MAX_ID; ++itkey) {
			if (m_mirror[i][itkey][0] != 0) {
				SubRecordBuilder keyBuilder(tableDef, KEY_PAD, INVALID_ROW_ID);
				SubRecord *findKey = keyBuilder.createSubRecordById("0", &itkey);
				assert(RedRecord::readBigInt(tableDef, &m_mirror[i][itkey][1], 0) == itkey);
				IndexScanCond cond(0, findKey, true, true, true);
				TblScan *scanHandle = table->indexScan(session, OP_READ, &cond, tableDef->m_numCols, columns);
				bool exist = table->getNext(scanHandle, buf);
				table->endScan(scanHandle);
				assert(exist);
				assert(RedRecord::readBigInt(tableDef, buf, 0) == itkey);
				if (!ResultChecker::checkRecordToRecord(tableDef, &m_mirror[i][itkey][1], buf)) {
					cout << "Check table " << tableDef->m_name << " failed, because of unconsistency between physical data and memory data" << endl;
					m_db->getSessionManager()->freeSession(session);
					m_db->freeConnection(conn);
					return false;
				}
				freeSubRecord(findKey);
			}
		}
		delete [] columns;


		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		delete [] buf;
	}

	return true;
}


/**
 * ���ݲ���3�Ķ���ִ�в�������
 * @param param	�̲߳���
 */
void AtomicTwo::mtOperation(void *param) {
	u64 mark = *((u64*)param);
	uint tableNo = chooseTableNo((uint)mark);
	Table *table = m_tableInfo[tableNo]->m_table;
	TableDef *tableDef = table->getTableDef();
	u16 *columns = RecordHelper::getAllColumns(tableDef);
	byte *buf = new byte[tableDef->m_maxRecSize];

	Connection *conn = m_db->getConnection(false);
	Connection *conn2 = m_db->getConnection(false);
	Connection *conn3 = m_db->getConnection(false);
	Connection *conn4 = m_db->getConnection(false);
	Session *session1 = m_db->getSessionManager()->allocSession("AtomicOne::mtOperation", conn);
	Session *session2 = m_db->getSessionManager()->allocSession("AtomicOne::mtOperation", conn2);
	Session *session3 = m_db->getSessionManager()->allocSession("AtomicOne::mtOperation", conn3);
	Session *session4 = m_db->getSessionManager()->allocSession("AtomicOne::mtOperation", conn4);
	MemoryContext *memoryContext = new MemoryContext(Limits::PAGE_SIZE, 1);

	for (uint i = 0; i < TASK_OPERATION_NUM; i++) {
		//if (mark == 1)
			//cout << "mark = " << mark << ", i = " << i << endl;
		u64 savePoint = memoryContext->setSavepoint();
		u64 sp1 = session1->getMemoryContext()->setSavepoint();
		u64 sp2 = session2->getMemoryContext()->setSavepoint();
		u64 sp3 = session3->getMemoryContext()->setSavepoint();
		u64 sp4 = session4->getMemoryContext()->setSavepoint();


		Record *updateRecord = NULL;
		u64 getKey, newKey;
		u16 *updColumns;
		bool canUpdate = false, updSucc;
		RowLockHandle *rlh = NULL, *rlh1 = NULL;
		uint updatedCols = (RANDOM_UPDATE_COLUMNS > tableDef->m_numCols) ? tableDef->m_numCols : RANDOM_UPDATE_COLUMNS;

		// ������£�����ͬ���ڴ澵��
		u64 key = RandomGen::nextInt(0, MAX_ID);

		SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD);
		SubRecord *findKey = keyBuilder.createSubRecordById("0", &key);
		findKey->m_rowId = INVALID_ROW_ID;

		// ͨ�������õ�>=key����
		IndexScanCond cond(0, findKey, true, true, false);

		RWLOCK(m_tblLock[tableNo], Shared);

		TblScan *handle = table->indexScan(session1, OP_UPDATE, &cond, tableDef->m_numCols, columns);
		if (!table->getNext(handle, buf))
			goto Update_Finish;

		// �����ҵ�����>=����Ҫ����keyʹ֮׼ȷ
		getKey = RedRecord::readBigInt(tableDef, buf, 0);
		assert(getKey >= key);
		key = getKey;

		assert(&m_mirror[tableNo][key][0] != 0);
		assert(ResultChecker::checkRecordToRecord(tableDef, &m_mirror[tableNo][key][1], buf));

		// ����Ҫ����סkey��Ӧ�����ڣ���ֹ���������߳̿�������
		rlh = TRY_LOCK_ROW(session3, tableDef->m_id, key, Exclusived);
		if (rlh == NULL) {
			goto Update_Finish;
		}

		// ��ʱ�Ѿ��Լ�¼������������½�����޸��ڴ澵��֮�󣬸�������������
		newKey = RandomGen::nextInt(0, MAX_ID);
		updateRecord = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, REC_MYSQL, tableDef->m_maxRecSize);
		memcpy(updateRecord->m_data, buf, tableDef->m_maxRecSize);
		updColumns = TableDMLHelper::updateSomeColumnsOfRecord(memoryContext, table, updateRecord, newKey, i, &updatedCols);

		for (int idx = 0; idx < updatedCols; ++idx) {
			if (updColumns[idx] == 0) {
				assert(RedRecord::readBigInt(tableDef, updateRecord->m_data, 0) == newKey);
			}
		}

		// �õ���������֮���¼������������ǰ��Ľӿڲ�һ���ᱣ֤����������
		newKey = RedRecord::readBigInt(tableDef, updateRecord->m_data, 0);
		// ����Ҫ�ȶ�Ҫ���µ���һ�м�������������ı�Ļ�
		if (key != newKey) {
			rlh1 = TRY_LOCK_ROW(session2, tableDef->m_id, newKey, Exclusived);
			if (rlh1 == NULL) {
				session3->unlockRow(&rlh);
				goto Update_Finish;
			}
		}

		// �����ڴ澵��
		//if (updSucc) {
		assert(canUpdate == false);
		if (key == newKey || m_mirror[tableNo][newKey][0] == 0) {	// ֻ�����ʱ��������¿��ܳɹ���������Ҫͬʱ�����ڴ棬����ֻ����������£����Ҹ��»�ʧ��
			if (key == newKey) {
				assert(m_mirror[tableNo][key][0] != 0);
				assert(key == RedRecord::readBigInt(tableDef, updateRecord->m_data, 0));
				memcpy(&m_mirror[tableNo][key][1], updateRecord->m_data, tableDef->m_maxRecSize);
			} else {
				assert(m_mirror[tableNo][newKey][0] == 0);
				memcpy(&m_mirror[tableNo][newKey][1], updateRecord->m_data, tableDef->m_maxRecSize);
				//memset(m_mirror[tableNo][key], 0, tableDef->m_maxRecSize);
				m_mirror[tableNo][key][0] = 0;
				m_mirror[tableNo][newKey][0] = '1';
			}
			canUpdate = true;
		}
		//}

		// ���������¼
		handle->setUpdateColumns((u16)updatedCols, updColumns);
		assert(RedRecord::readBigInt(tableDef, updateRecord->m_data, 0) == newKey);
		updSucc = table->updateCurrent(handle, updateRecord->m_data);
		assert(updSucc == canUpdate);

		if (key != newKey && canUpdate) {
			assert(FAIL == TableDMLHelper::fetchRecord(session4, table, key, NULL, NULL, true));
		}
		//Thread::msleep(300);
		if (updSucc) {
			nftrace(ts.intg, tout << "Updated: " << key << " to " << newKey;);
			//assert(1 == TableDMLHelper::fetchRecord(session4, table, newKey, NULL, NULL, true));
			if (TableDMLHelper::fetchRecord(session4, table, newKey, NULL, NULL, true) != SUCCESS) {
				Record tmpRec;
				tmpRec.m_format = REC_FIXLEN;
				tmpRec.m_data = buf;
				bool existinheap = table->getHeap()->getRecord(session4, 16400, &tmpRec);
				u64 recKey = RedRecord::readBigInt(tableDef, tmpRec.m_data, 0);
				if (existinheap)
					assert(recKey == newKey);
			}
		}



		/*
		if (updSucc) {
			cout << tableNo << "    " << handle->getCurrentRid() <<"     "<< key << "    " << newKey << " "<<endl;
			cout.flush();
		}
		*/

		// ����
		if (rlh != NULL)
			session3->unlockRow(&rlh);
		if (rlh1 != NULL)
			session2->unlockRow(&rlh1);

Update_Finish:
		table->endScan(handle);
		RWUNLOCK(m_tblLock[tableNo], Shared);

		//assert(verifyrange(tableNo, tableNo + 1));
		if (RWTRYLOCK(m_tblLock[tableNo], Exclusived)) {
			assert(verifyrange(tableNo, tableNo + 1));
			RWUNLOCK(m_tblLock[tableNo], Exclusived);
		} else {
			if (m_threadNum <= 2)
				assert(false);
		}

		freeSubRecord(findKey);
		if (updateRecord != NULL)
			freeRecord(updateRecord);

		session1->getMemoryContext()->resetToSavepoint(sp1);
		session2->getMemoryContext()->resetToSavepoint(sp2);
		session3->getMemoryContext()->resetToSavepoint(sp3);
		session4->getMemoryContext()->resetToSavepoint(sp4);

		memoryContext->resetToSavepoint(savePoint);
	}

	delete [] buf;
	delete [] columns;
	delete memoryContext;

	m_db->getSessionManager()->freeSession(session1);
	m_db->getSessionManager()->freeSession(session2);
	m_db->getSessionManager()->freeSession(session3);
	m_db->getSessionManager()->freeSession(session4);
	m_db->freeConnection(conn);
	m_db->freeConnection(conn2);
	m_db->freeConnection(conn3);
	m_db->freeConnection(conn4);
	//uphis.close();
}


/**
 * ��ʼ������֮�󣬳�ʼ���ڴ澵�񣬶�ȡ�����ݵ�����
 */
void AtomicTwo::initMirror() {
	m_mirror = new byte**[m_tables];
	for (uint i = 0; i < m_tables; i++) {
		Table *table = m_tableInfo[i]->m_table;
		TableDef *tableDef = table->getTableDef();

		// �����ڴ澵������
		m_mirror[i] = new byte*[MAX_ID];
		for (uint j = 0; j < MAX_ID; j++) {
			m_mirror[i][j] = new byte[tableDef->m_maxRecSize + 1];
			memset(m_mirror[i][j], 0, tableDef->m_maxRecSize + 1);
			m_mirror[i][j][0] = '\0';	// ���㲢�����ó���
		}

		// ��ȡ�����ݣ���ʼ�����ڴ澵��
		byte *buf = new byte[tableDef->m_maxRecSize];
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("AtomicTwo::initMirror", conn);
		u16 *columns = RecordHelper::getAllColumns(tableDef);
		TblScan *scanHandle = table->tableScan(session, OP_READ, tableDef->m_numCols, columns);

		while (table->getNext(scanHandle, buf)) {
			u64 key = RedRecord::readBigInt(tableDef, buf, 0);	// Ĭ��0��������
			assert(key < MAX_ID);
			memcpy(&m_mirror[i][key][1], buf, tableDef->m_maxRecSize);
			m_mirror[i][key][0] = '1';
		}

		table->endScan(scanHandle);

		delete [] buf;
		delete [] columns;

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
}
