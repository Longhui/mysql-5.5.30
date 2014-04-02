/**
 * 集成测试5.1.2测试记录操作原子性
 *
 * 测试目的：验证NTSE记录操作的原子性（参考数据方法，更新主键）
 * 表模式：Blog和Count
 * 测试配置：NTSE配置：NC_SMALL thread_count = 500
 * 测试数据：同5.1.1
 * 测试流程：
 *	1.	生成100条记录，记录的关键字ID在[0,MAXID]之间
 *	2.	构造一个大小为MAXID的数组A，保存第一步生成的记录到A中
 *	3.	为每张表分别运行thread_count/2个更新任务线程，每个线程执行10000次任务，每个任务包括如下操作：
 *		a)在[0, MAXID]之间选择一个随机数id
 *		b)通过IndexScan定位满足ID>=id的第一条记录R
 *		c)随机选择R的3列C1,C2,C3，更新这些列
 *		d)响应的更新A[id]
 *		e)如果ID列被更新为id’, 那么移动A[id]到A[id’]
 *		f)结束IndexScan
 *	4.	待更新任务运行完成之后，进行一次表扫描，对于当前记录为R，验证R与 A[R.ID]完全相同
 *
 * 苏斌(...)
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

/** 得到用例名字 */
string AtomicTwo::getName() const {
	return "Atomic test: update primary key";
}

/** 用例描述 */
string AtomicTwo::getDescription() const {
	return "Test whether the database operation is atomic by random updating and checking at last";
}

/**
 * 用例执行函数
 */
void AtomicTwo::run() {
	ts.intg = true;
	initMirror();
	m_threads = new TestOperationThread*[m_threadNum];
	// 生成各个线程执行操作
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
* 生成表数据
* @param totalRecSize	IN/OUT 要生成数据的大小，返回生成后的真实大小
* @param recCnt		IN/OUT 生成记录的个数，返回生成后的真实记录数
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
 * 预热函数
 */
void AtomicTwo::warmUp() {
	openTable();
	return;
}


/**
 * 验证内存镜像和物理数据的一致性
 * @param minTable	检查的最小表号
 * @param maxTable	检查的最大表号
 * @return 是否一致
 */
bool AtomicTwo::verifyrange(uint minTable, uint maxTable) {
	assert(minTable <= maxTable);
	// 逐一验证每张表和内存数据的一致性
	if (minTable == (uint)-1 && maxTable == (uint)-1) {
		minTable = 0;
		maxTable = m_tables;
	}
	for (uint i = minTable; i < maxTable; i++) {
		Table *table = m_tableInfo[i]->m_table;
		TableDef *tableDef = table->getTableDef();
		u16 *columns = RecordHelper::getAllColumns(tableDef);
		byte *buf = new byte[tableDef->m_maxRecSize];

		//// 保证堆和索引的一致性
		/*
		assert(ResultChecker::checkIndice(m_db, table));
		assert(ResultChecker::checkIndiceToTable(m_db, table, true, false));
		assert(ResultChecker::checkTableToIndice(m_db, table, true, false));
		*/

		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("AtomicOne::verify", conn);

		TblScan *handle = table->tableScan(session, OP_READ, tableDef->m_numCols, columns);
		while (table->getNext(handle, buf)) {
			// 读取主键键值之后对比内存镜像
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

		/*** 反向验证 ***/
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
 * 根据步骤3的定义执行并发更新
 * @param param	线程参数
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

		// 随机更新，并且同步内存镜像
		u64 key = RandomGen::nextInt(0, MAX_ID);

		SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD);
		SubRecord *findKey = keyBuilder.createSubRecordById("0", &key);
		findKey->m_rowId = INVALID_ROW_ID;

		// 通过索引得到>=key的项
		IndexScanCond cond(0, findKey, true, true, false);

		RWLOCK(m_tblLock[tableNo], Shared);

		TblScan *handle = table->indexScan(session1, OP_UPDATE, &cond, tableDef->m_numCols, columns);
		if (!table->getNext(handle, buf))
			goto Update_Finish;

		// 由于找到的是>=，需要更新key使之准确
		getKey = RedRecord::readBigInt(tableDef, buf, 0);
		assert(getKey >= key);
		key = getKey;

		assert(&m_mirror[tableNo][key][0] != 0);
		assert(ResultChecker::checkRecordToRecord(tableDef, &m_mirror[tableNo][key][1], buf));

		// 这里要先锁住key对应项的入口，防止其他更新线程看到幻象
		rlh = TRY_LOCK_ROW(session3, tableDef->m_id, key, Exclusived);
		if (rlh == NULL) {
			goto Update_Finish;
		}

		// 此时已经对记录加锁，计算更新结果，修改内存镜像之后，更新真正的数据
		newKey = RandomGen::nextInt(0, MAX_ID);
		updateRecord = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, REC_MYSQL, tableDef->m_maxRecSize);
		memcpy(updateRecord->m_data, buf, tableDef->m_maxRecSize);
		updColumns = TableDMLHelper::updateSomeColumnsOfRecord(memoryContext, table, updateRecord, newKey, i, &updatedCols);

		for (int idx = 0; idx < updatedCols; ++idx) {
			if (updColumns[idx] == 0) {
				assert(RedRecord::readBigInt(tableDef, updateRecord->m_data, 0) == newKey);
			}
		}

		// 得到真正更新之后记录的主键，调用前面的接口不一定会保证主键被更新
		newKey = RedRecord::readBigInt(tableDef, updateRecord->m_data, 0);
		// 这里要先对要更新的那一行加锁，如果主键改变的话
		if (key != newKey) {
			rlh1 = TRY_LOCK_ROW(session2, tableDef->m_id, newKey, Exclusived);
			if (rlh1 == NULL) {
				session3->unlockRow(&rlh);
				goto Update_Finish;
			}
		}

		// 更新内存镜像
		//if (updSucc) {
		assert(canUpdate == false);
		if (key == newKey || m_mirror[tableNo][newKey][0] == 0) {	// 只有这个时候物理更新可能成功，并且需要同时更新内存，否则只进行物理更新，并且更新会失败
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

		// 更新物理记录
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

		// 放锁
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
 * 初始化数据之后，初始化内存镜像，读取表数据到镜像
 */
void AtomicTwo::initMirror() {
	m_mirror = new byte**[m_tables];
	for (uint i = 0; i < m_tables; i++) {
		Table *table = m_tableInfo[i]->m_table;
		TableDef *tableDef = table->getTableDef();

		// 生成内存镜像数组
		m_mirror[i] = new byte*[MAX_ID];
		for (uint j = 0; j < MAX_ID; j++) {
			m_mirror[i][j] = new byte[tableDef->m_maxRecSize + 1];
			memset(m_mirror[i][j], 0, tableDef->m_maxRecSize + 1);
			m_mirror[i][j][0] = '\0';	// 清零并且设置长度
		}

		// 读取表数据，初始化到内存镜像
		byte *buf = new byte[tableDef->m_maxRecSize];
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("AtomicTwo::initMirror", conn);
		u16 *columns = RecordHelper::getAllColumns(tableDef);
		TblScan *scanHandle = table->tableScan(session, OP_READ, tableDef->m_numCols, columns);

		while (table->getNext(scanHandle, buf)) {
			u64 key = RedRecord::readBigInt(tableDef, buf, 0);	// 默认0列是主键
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
