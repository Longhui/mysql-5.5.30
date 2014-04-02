/**
* 对给定的Table执行DML操作的实现
*
* 苏斌(bsu@corp.netease.com, naturally@163.org)
*/

#include "IntgTestHelper.h"
#include "misc/RecordHelper.h"
#include "util/File.h"
#include "Random.h"
#include "btree/Index.h"
#include "heap/Heap.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"

using namespace std;
using namespace ntse;

/**
* 这里操作的表暂时局限于集成测试使用的表，每个表第一个索引是主键索引
*/

/**
 * 在对应表中插入一条记录，记录由key和opid构造
 * @pre 表的第一个索引为主键索引，并且键值是bigint
 * @param session	会话句柄
 * @param table		操作的表对象
 * @param key		插入键值的主键
 * @param opid		该记录键值使用的opid
 * @return 返回插入成功SUCCESS，或者键值冲突失败FAIL
 */
TblOpResult TableDMLHelper::insertRecord(Session *session, Table *table, u64 key, uint opid) {
	MemoryContext *memoryContext = session->getMemoryContext();
	u64 savePoint = memoryContext->setSavepoint();
	TableDef *tableDef = table->getTableDef();
	Record *newRecord = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, REC_MYSQL, tableDef->m_maxRecSize);
	uint updatedCols = tableDef->m_numCols;
	updateSomeColumnsOfRecord(memoryContext, table, newRecord, key, opid, &updatedCols);
	uint dupIndex;
	RowId rowId = table->insert(session, newRecord->m_data, &dupIndex);
	freeRecord(newRecord);
	memoryContext->resetToSavepoint(savePoint);
	return (rowId != INVALID_ROW_ID) ? SUCCESS : FAIL;
}


/**
* 更新对应表中一条主键>=k的第一条记录，记录由kplus和opid构成
* @pre 表的第一个索引为主键索引，并且键值是bigint
* @param session	会话句柄
* @param table		操作表对象
* @param key		IN/OUT 要更新的主键，这里是更新第一个>=key的记录，返回真正更新记录的主键
* @param kplus		IN/OUT 更新操作将选择性的将原始主键更新成该值，返回的是更新之后新主键的值
* @param opid		更新记录使用的opid
* @param check		指定是否需要检查记录的一致性，如果该表的操作满足所有属性被统一更新，则允许检查记录一致性，否则不可以
* @post 外部传入的key和kplus如果是相同数值（及可能就是相同对象），那么此时虽然会经过两次修改同一对象，但是不可能出现返回错误，如果不同数值，只会分别修改两个对象
* @return 更新成功SUCCESS，或者更新出现唯一性冲突返回FAIL/找不到键值返回out_of_bound，验证记录键值失败返回check_fail
*/
TblOpResult TableDMLHelper::updateRecord(Session *session, Table *table, u64 *key, u64 *kplus, uint opid, bool check) {
	TableDef *tableDef = table->getTableDef();
	assert(tableDef->m_indice != NULL);

	SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD, INVALID_ROW_ID);
	SubRecord *findKey = keyBuilder.createSubRecordById("0", key);

	IndexScanCond cond(0, findKey, true, true, false);
	u16 *columns = RecordHelper::getAllColumns(tableDef);
	TblScan *scanHandle = table->indexScan(session, OP_UPDATE, &cond, tableDef->m_numCols, columns);
	byte *buf = new byte [tableDef->m_maxRecSize];
	memset(buf, 0, tableDef->m_maxRecSize);
	Record *newRecord;
	u16 *updateColumns;
	uint updatedCols = tableDef->m_numCols;;
	bool succ = true;
	bool outOfBound = false;
	bool checkFail = false;
	MemoryContext *memoryContext = new MemoryContext(Limits::PAGE_SIZE, 4);

	if (table->getNext(scanHandle, buf) == false) {
		outOfBound = true;
		goto Fail;
	}

	if (check) {
		if (!ResultChecker::checkRecord(table, buf, scanHandle->getCurrentRid(), NULL)) {
			checkFail = true;
			goto Fail;
		}
	}

	if (*key == *kplus) {	// 说明本次更新不更新主键，这里要调整参数正确
		*kplus = RedRecord::readBigInt(tableDef, buf, 0);
	}

	// 更新键值
	newRecord = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, REC_MYSQL, tableDef->m_maxRecSize);
	updateColumns = updateSomeColumnsOfRecord(memoryContext, table, newRecord, *kplus, opid, &updatedCols);
	scanHandle->setUpdateColumns((u16)updatedCols, updateColumns);
	succ = table->updateCurrent(scanHandle, newRecord->m_data);
	table->endScan(scanHandle);

	if (succ) {
		// 保存真正删除的键值key
		*key = RedRecord::readBigInt(tableDef, buf, 0);
	}

	delete [] buf;
	delete [] columns;
	freeSubRecord(findKey);
	freeRecord(newRecord);
	delete memoryContext;
	return succ ? SUCCESS : FAIL;

Fail:
	table->endScan(scanHandle);
	delete memoryContext;
	delete [] buf;
	delete [] columns;
	freeSubRecord(findKey);
	if (outOfBound)
		return OUT_OF_BOUND;
	if (checkFail)
		return CHECK_FAIL;
	return FAIL;
}

/**
* 删除指定表中的一条主键>=key的第一条记录
* @pre 表的第一个索引为主键索引，并且键值是bigint
* @param session	会话句柄
* @param table		操作表对象
* @param key		IN/OUT 删除>=key的第一条记录，返回key为真正删除记录的主键
* @return 是否存在这么一条记录并且删除成功，可能不存在记录，返回OUT_OF_BOUND
*/
TblOpResult TableDMLHelper::deleteRecord(Session *session, Table *table, u64 *key) {
	TableDef *tableDef = table->getTableDef();
	assert(tableDef->m_indice != NULL);
	IndexDef *indexDef = tableDef->m_indice[0];

	SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD, INVALID_ROW_ID);
	SubRecord *findKey = keyBuilder.createSubRecordById("0", key);

	IndexScanCond cond(0, findKey, true, true, false);
	TblScan *scanHandle = table->indexScan(session, OP_DELETE, &cond, indexDef->m_numCols, indexDef->m_columns);
	byte *buf = new byte [tableDef->m_maxRecSize];
	memset(buf, 0, tableDef->m_maxRecSize);
	if (!table->getNext(scanHandle, buf)) {
		table->endScan(scanHandle);
		delete [] buf;
		freeSubRecord(findKey);
		return OUT_OF_BOUND;
	}

	// 保存真正删除的键值key
	*key = RedRecord::readBigInt(tableDef, buf, 0);

	table->deleteCurrent(scanHandle);
	table->endScan(scanHandle);
	delete [] buf;
	freeSubRecord(findKey);

	return SUCCESS;
}

/**
* 扫描获取指定表中主键>=key的第一条记录，由参数指定
* @pre 表的第一个索引为主键索引，并且键值是bigint
* @post record out 保存了取得的记录内容，可以为NULL，不返回
* @param session	会话句柄
* @param table		操作表对象
* @param key		根据key来获取第一条>=key的主键
* @param record		OUT	如果不为NULL，返回获取记录的键值内容
* @param rowId		OUT 返回的rowId信息，可以为NULL，不返回
* @param precise	当前是=查询TRUE还是>=查询FALSE
* @return 扫描结果不为空，返回SUCCESS，否则返回FAIL
*/
TblOpResult TableDMLHelper::fetchRecord(Session *session, Table *table, u64 key, byte *record, RowId *rowId, bool precise) {
	TableDef *tableDef = table->getTableDef();
	assert(tableDef->m_indice != NULL);

	SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD, INVALID_ROW_ID);
	SubRecord *findKey = keyBuilder.createSubRecordById("0", &key);

	IndexScanCond cond(0, findKey, true, true, precise);
	u16 *columns = RecordHelper::getAllColumns(tableDef);
	TblScan *scanHandle = table->indexScan(session, OP_READ, &cond, tableDef->m_numCols, columns);
	byte *buf = new byte [tableDef->m_maxRecSize];
	memset(buf, 0, tableDef->m_maxRecSize);
	bool gotit = table->getNext(scanHandle, buf);
	if (gotit) {
		if (record)
			memcpy(record, buf, tableDef->m_maxRecSize);
		if (rowId)
			*rowId = scanHandle->getCurrentRid();
		if (precise)
			assert(RedRecord::readBigInt(tableDef, buf, 0) == (s64)key);
	}
	table->endScan(scanHandle);
	delete [] columns;
	delete [] buf;
	freeSubRecord(findKey);

	return gotit ? SUCCESS : FAIL;
}



/**
* 根据key和opid构造一条新键值
* @param memoryContext	内存上下文句柄
* @param table			操作表对象
* @param record			更新记录原始内容
* @param key			如果更新主键，采用该值作为更新后的主键
* @param opid			更新的键值采用opid构造
* @param updateCols		IN/OUT 指定更新的列数，返回实际更新的列数
* @return 返回更新属性列数组，空间外部必须释放
*/
u16* TableDMLHelper::updateSomeColumnsOfRecord(MemoryContext *memoryContext, Table *table, Record *record, u64 key, uint opid, uint *updateCols) {
	TableDef *tableDef = table->getTableDef();
	IndexDef *indexDef = tableDef->m_pkey;
	u16 *columns = (u16*)memoryContext->alloc(sizeof(u16) * *updateCols);
	memset(columns, 0, *updateCols * sizeof(u16));

	uint updated = 0;
	for (u16 i = 0; i < tableDef->m_numCols && updated < *updateCols; i++) {
		assert(record->m_size == tableDef->m_maxRecSize);
		assert(record->m_format == REC_MYSQL);
		// 决定该列是否要更新
		// 有50%的机会可以更新该列，否则，需要判断不更新该列是否会导致对本记录的更新达不到总更新要求
		if (RandomGen::nextInt() % 2 == 0 && updated + tableDef->m_numCols - i > *updateCols)
			continue;

		assert(updated < *updateCols);
		columns[updated++] = i;
		ColumnDef *columnDef = tableDef->m_columns[i];
		// 首先判断该属性是不是主键，主键用key更新
		if (RecordHelper::isColPrimaryKey(i, indexDef)) {
			RedRecord::writeNumber(tableDef, i, record->m_data, key);
			continue;
		}

		switch (columnDef->m_type) {
			case CT_TINYINT:
				{
					byte value = (byte)opid;
					RedRecord::writeNumber(tableDef, i, record->m_data, value);
					break;
				}
			case CT_SMALLINT:
				{
					short value = (short)opid;
					RedRecord::writeNumber(tableDef, i, record->m_data, value);
					break;
				}
			case CT_MEDIUMINT:
				{
					int value = (int)opid;
					RedRecord::writeMediumInt(tableDef, record->m_data, i,  value);
					break;
				}
			case CT_INT:
				{
					uint value = opid;
					RedRecord::writeNumber(tableDef, i, record->m_data, value);
					break;
				}
			case CT_BIGINT:
				{
					u64 value = (u64)opid;
					RedRecord::writeNumber(tableDef, i, record->m_data, value);
					break;
				}
			case CT_CHAR:
			case CT_VARCHAR:
				{
					u16 charLen = (u16)RandomGen::nextInt(1, columnDef->m_size - columnDef->m_lenBytes);
					byte *value = (byte*)RecordHelper::getLongChar(memoryContext, charLen, tableDef, columnDef, opid);
					RedRecord::writeVarchar(tableDef, record->m_data, i, value, charLen);
					break;
				}
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				{
					size_t size = RandomGen::nextInt(0, columnDef->m_size - columnDef->m_lenBytes);
					byte *value = (byte*)RecordHelper::getLongChar(memoryContext, size, tableDef, columnDef, opid);
					RedRecord::writeLob(tableDef, record->m_data, i, value, size);
					break;
				}
			default:
				break;
		}
	}

	assert(updated == *updateCols || updated == tableDef->m_numCols && tableDef->m_numCols < *updateCols);
	assert(record->m_size == tableDef->m_maxRecSize);
	assert(record->m_format == REC_MYSQL);
	if (*updateCols > tableDef->m_numCols)
		*updateCols = tableDef->m_numCols;
	return columns;
}

/**
 * 打印索引统计信息
 * @para table	索引所属的表
 */
void TableDMLHelper::printStatus(Table *table) {
	DrsIndice *indice = table->getIndice();
	TableDef *tableDef = table->getTableDef();
	uint indexNum = indice->getIndexNum();
	for (uint i = 0; i < indexNum; i++) {
		DrsIndex *index = indice->getIndex(i);
		const struct IndexStatus *status = &(index->getStatus());
		DBObjStats *dbobjStats = index->getDBObjStats();
		cout << endl << "*********************************************" << endl
			<< tableDef->m_name << ": " << tableDef->m_indice[i]->m_name << endl
			<< "Data Length: " << status->m_dataLength << endl
			<< "Free Length: " << status->m_freeLength << endl
			<< "Operations:" << endl
			<< "Insert: " << dbobjStats->m_statArr[DBOBJ_ITEM_INSERT] << endl
			<< "Update: " << dbobjStats->m_statArr[DBOBJ_ITEM_UPDATE] << endl
			<< "Delete: " << dbobjStats->m_statArr[DBOBJ_ITEM_DELETE] << endl
			<< "Scans: " << dbobjStats->m_statArr[DBOBJ_SCAN] << endl
			<< "Rows scans: " << dbobjStats->m_statArr[DBOBJ_SCAN_ITEM] << endl
			<< "Back scans: " << status->m_backwardScans << endl
			<< "Rows bscans: " << status->m_rowsBScanned << endl
			<< "Row lock restarts: " << status->m_numRLRestarts << endl
			<< "Idx lock restarts for insert: " << status->m_numILRestartsForI << endl
			<< "Idx lock restarts for delete: " << status->m_numILRestartsForD << endl
			<< "Latch conflicts: " << status->m_numLatchesConflicts << endl
			<< "DeadLock conflicts: " << status->m_numDeadLockRestarts.get() << endl
			<< endl << "*********************************************" << endl;
	}
}

/**
* 检查从table到各个index的映射，同时可能还需要检测记录本身是否一致
* @param db			数据库
* @param table		操作表对象
* @param checkTable	是否执行表到索引的验证
* @param checkRecord是否验证记录本身正确性
* @return 检查都成功true，任何失败false
*/
bool ResultChecker::checkTableToIndice(Database *db, Table *table, bool checkTable, bool checkRecord) {
	TableDef *tableDef = table->getTableDef();
	byte *buf = new byte[tableDef->m_maxRecSize];
	byte *idxbuf = new byte [tableDef->m_maxRecSize];
	memset(buf, 0, tableDef->m_maxRecSize);
	memset(idxbuf, 0, tableDef->m_maxRecSize);
	Connection *conn = db->getConnection(false);
	Connection *idxConn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("ResultChecker::checkTableToIndex_table", conn);

	u16 *columns = RecordHelper::getAllColumns(tableDef);
	TblScan *handle = table->tableScan(session, OP_UPDATE, tableDef->m_numCols, columns);
	assert(handle != NULL);

	while (table->getNext(handle, buf)) {
		RowId rowId1 = handle->getCurrentRid();
		// 检查记录本身的一致性
		if (checkRecord && !ResultChecker::checkRecord(table, buf, rowId1, NULL)) {
			cout << "Check table " << table->getPath() << " failed" << endl;
			table->endScan(handle);
			goto Fail;
		}

		if (checkTable) {	// 对每条记录扫描各个索引验证记录存在
			u64 key = RedRecord::readBigInt(tableDef, buf, 0);
			// TODO: findKey可以利用memoryContext或者事先全部分配好提高效率
			for (u16 i = 0; i < tableDef->m_numIndice; i++) {
				IndexDef *indexDef = tableDef->m_indice[i];
				SubRecord *findKey = RecordHelper::formIdxKeyFromData(tableDef, indexDef, rowId1, buf);
				findKey->m_rowId = rowId1;
				DrsIndex *index = table->getIndice()->getIndex(i);
				Session *sessionIndex = db->getSessionManager()->allocSession("ResultChecker::checkTableToIndex_index", idxConn);
				RowId rowId2;
				bool gotit = index->getByUniqueKey(sessionIndex, findKey, None, &rowId2, NULL, NULL, NULL);

				db->getSessionManager()->freeSession(sessionIndex);
				freeSubRecord(findKey);

				if (!gotit || rowId2 != rowId1) {
					cout << "Check table " << table->getPath() << " failed when checking index " << indexDef->m_name << "	key: " << key << "	rowId: " << rowId1 << endl;
					{
						// 尝试使用范围扫描看能否查找到键值，因为在堆已经加锁，该键值不会被删除修改
						Session *sessionIndex = db->getSessionManager()->allocSession("ResultChecker::checkTableToIndex_index", idxConn);
						//DrsIndexScanHandle *scanHandle;
						IndexScanHandle *scanHandle = NULL;
						//SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tableDef,
						//	indexDef->m_numCols, indexDef->m_columns, findKey->m_numCols, findKey->m_columns, KEY_COMPRESS, REC_REDUNDANT, 1000);

						scanHandle = index->beginScan(session, NULL, true, true, None, NULL, NULL);
						SubRecord *out = IndexKey::allocSubRecordRED(sessionIndex->getMemoryContext(), indexDef->m_maxKeySize);
						bool got = false;
						while (index->getNext(scanHandle, out)) {
							if (out->m_rowId == findKey->m_rowId) {
								got = true;
								break;
							}
						}
						index->endScan(scanHandle);
						db->getSessionManager()->freeSession(sessionIndex);
						NTSE_ASSERT(got == gotit);
						NTSE_ASSERT(false);
					}
					table->endScan(handle);
					goto Fail;
				}
			}
		}
	}

	table->endScan(handle);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	db->freeConnection(idxConn);
	delete [] buf;
	delete [] idxbuf;
	delete [] columns;

	return true;
Fail:
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	db->freeConnection(idxConn);
	delete [] buf;
	delete [] idxbuf;
	delete [] columns;

	return false;
}


/**
* 检查从各个index到table的映射，同时可能还需要检测键值本身是否一致
* @param db			数据库
* @param table		操作表对象
* @param checkIndex 是否验证各个索引
* @param checkKey	是否检查索引的键值
* @return 返回检查是否正确
*/
bool ResultChecker::checkIndiceToTable(Database *db, Table *table, bool checkIndex, bool checkKey) {
	if (!checkIndex)
		return true;

	TableDef *tableDef = table->getTableDef();
	for (uint i = 0; i < tableDef->m_numIndice; i++) {
		if (!checkIndexToTable(db, table, i, checkKey))
			return false;
	}

	return true;
}

/**
 * 执行指定索引到表的检查
 * @param db		数据库
 * @param table		表对象
 * @param indexNo	要验证的索引
 * @param checkKey	检查索引键值本身
 * @return 返回是否检查正确
 */
bool ResultChecker::checkIndexToTable(Database *db, Table *table, uint indexNo, bool checkKey) {
	TableDef *tableDef = table->getTableDef();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("ResultChecker::checkIndexToTable_index", conn);
	byte *tblbuf = new byte[tableDef->m_maxRecSize];
	memset(tblbuf, 0, tableDef->m_maxRecSize);
	u16 *columns = RecordHelper::getAllColumns(tableDef);
	SubRecord *idxSb = new SubRecord(REC_REDUNDANT, tableDef->m_numCols, columns, new byte[tableDef->m_maxRecSize], tableDef->m_maxRecSize);
	// 为了保证任何时刻都能进行一致性检查，外层准备采用索引模块的扫描，内层使用table的接口
	// 这样既能加锁成功，同时又保证两重扫描读到的都是最新数据，如果采用heap的接口，可能数据存在于mms中未刷出
	Connection *tblConn = db->getConnection(false);
	DrsIndex *index = table->getIndice()->getIndex(indexNo);
	IndexDef *indexDef = tableDef->m_indice[indexNo];
	RowLockHandle *rlh;
	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tableDef,
		indexDef->m_numCols, indexDef->m_columns, idxSb->m_numCols, idxSb->m_columns, KEY_COMPRESS, REC_REDUNDANT, 1000);
	//DrsIndexScanHandle *idxHandle = index->beginScan(session, NULL, true, true, Shared, &rlh, extractor);
	IndexScanHandle *idxHandle = index->beginScan(session, NULL, true, true, Shared, &rlh, extractor);
	assert(idxHandle != NULL);
	while (index->getNext(idxHandle, idxSb)) {
		RowId rowId = idxHandle->getRowId();
		//// 对于主键索引打印当前索引的主键
		//if (indexNo == 0) {
		//	cout << RedRecord::readBigInt(tableDef, idxSb->m_data, 0) << endl;
		//}

		// 检查索引的键值
		if (checkKey && !ResultChecker::checkRecord(table, idxSb->m_data, rowId, indexDef)) {
			ResultChecker::checkRecord(table, idxSb->m_data, rowId, indexDef);
			cout << "Check index " << indexDef->m_name << " failed, " << rowId << " key can't be consistency" << endl;
			session->unlockRow(&rlh);
			index->endScan(idxHandle);
			goto Fail;
		}

		// 通过table得到堆中的记录
		Session *sessionHeap = db->getSessionManager()->allocSession("ResultChecker::checkIndexToTable", tblConn);
		TblScan *tHandle = table->positionScan(sessionHeap, OP_READ, tableDef->m_numCols, columns);
		if (!table->getNext(tHandle, tblbuf, rowId)) {
			table->endScan(tHandle);
			db->getSessionManager()->freeSession(sessionHeap);
			session->unlockRow(&rlh);
			index->endScan(idxHandle);
			cout << "Check index " << indexDef->m_name << " failed, " << rowId << " can't be found in table" << endl;
			goto Fail;
		} 
		
		table->endScan(tHandle);
		db->getSessionManager()->freeSession(sessionHeap);

		// 检查record和索引键值的一致性
		if (!ResultChecker::checkKeyToRecord(tableDef, indexDef, idxSb->m_data, tblbuf)) {
			ResultChecker::checkKeyToRecord(tableDef, indexDef, idxSb->m_data, tblbuf);
			index->endScan(idxHandle);
			cout << "Check index " << indexDef->m_name << " failed" << endl;
			session->unlockRow(&rlh);
			goto Fail;
		}

		session->unlockRow(&rlh);
	}

	index->endScan(idxHandle);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	db->freeConnection(tblConn);
	freeSubRecord(idxSb);
	delete [] tblbuf;
	return true;

Fail:
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	db->freeConnection(tblConn);
	freeSubRecord(idxSb);
	delete [] tblbuf;
	return false;
}

/**
* 检查指定记录内容是否满足一致性
* 这里只检查非主键的smallint/tiny int/int/bigint/char/varchar类型
* @pre record长度必须是tableDef->m_maxRecSize
* @param table		操作表对象
* @param record		要检查的记录内容
* @param rowId		记录的rowId
* @param indexDef	非NULL，表示当前要检查的记录是该索引的键值，是不完整的记录，否则为表记录内容
* @return 返回检查结果正确性
*/
bool ResultChecker::checkRecord(Table *table, byte *record, RowId rowId, IndexDef *indexDef) {
	bool checkTableRecord = (indexDef == NULL);
	TableDef *tableDef = table->getTableDef();
	Record *newRecord = RecordHelper::formRecordFromData(tableDef, rowId, record);

	s64 opid = -1;
	uint size = 0;
	// 确认从记录当中可能得到的最大的opid值——一般opid都是INT，但是如果表内都是小整形，则必须确认当前最大的保存形式
	u16 checkColumns = checkTableRecord ? tableDef->m_numCols : indexDef->m_numCols;
	for (u16 i = 0; i < checkColumns; i++) {
		u16 colNo = checkTableRecord ? i : indexDef->m_columns[i];
		ColumnDef *columnDef = tableDef->m_columns[colNo];
		if (RecordHelper::isColPrimaryKey(colNo, tableDef->m_pkey))
			continue;	// 需要略过主键

		if (RedRecord::isNull(tableDef, record, colNo))
			continue;	// 略过null列

		switch (columnDef->m_type) {
			case CT_TINYINT:
				size = 1;
				if (size > 1)
					break;
				opid = (uint)RedRecord::readTinyInt(tableDef, newRecord->m_data, colNo);
				break;
			case CT_SMALLINT:
				if (size > 2)
					break;
				size = 2;
				opid = (uint)RedRecord::readSmallInt(tableDef, newRecord->m_data, colNo);
				break;
			case CT_INT:
				if (size > 4)
					break;
				size = 4;
				opid = (uint)RedRecord::readInt(tableDef, newRecord->m_data, colNo);
				break;
			case CT_BIGINT:
				size = 8;
				opid = (uint)RedRecord::readBigInt(tableDef, newRecord->m_data, colNo);
				break;
			default:
				break;
		}

		if (size >= 4)
			break;
	}

	if (opid == -1) {	// 没有数值列，默认一致
		freeRecord(newRecord);
		return true;
	}

	// 得到opid的最大形式之后确认每个属性都满足
	for (u16 i = 0; i < checkColumns; i++) {
		u16 colNo = checkTableRecord ? i : indexDef->m_columns[i];
		ColumnDef *columnDef = tableDef->m_columns[colNo];
		if (RecordHelper::isColPrimaryKey(colNo, tableDef->m_pkey))
			continue;	// 需要略过主键

		if (RedRecord::isNull(tableDef, record, colNo))
			continue;	// 略过null列

		switch (columnDef->m_type) {
			case CT_TINYINT:
				if ((s8)opid != RedRecord::readTinyInt(tableDef, newRecord->m_data, colNo))
					goto Fail;
				break;
			case CT_SMALLINT:
				if ((s16)opid != RedRecord::readSmallInt(tableDef, newRecord->m_data, colNo))
					goto Fail;
				break;
			case CT_INT:
				if ((s32)opid != RedRecord::readInt(tableDef, newRecord->m_data, colNo))
					goto Fail;
				break;
			case CT_BIGINT:
				if ((s64)opid != RedRecord::readBigInt(tableDef, newRecord->m_data, colNo))
					goto Fail;
				break;
			case CT_CHAR:
				// TODO: 现在必须保证这里得到的opid是32位的，否则在字符串当中就无法正确判断
				{
					byte *chars;
					size_t size;
					RedRecord::readChar(tableDef, newRecord->m_data, colNo, (void**)&chars, &size);
					if (!RecordHelper::checkLongChar(tableDef, columnDef, (uint)opid, size, chars))
						goto Fail;
					break;
				}
			case CT_VARCHAR:
				// TODO: 现在必须保证这里得到的opid是32位的，否则在字符串当中就无法正确判断
				{
					byte *varchar;
					size_t size;
					RedRecord::readVarchar(tableDef, newRecord->m_data, colNo, (void**)&varchar, &size);
					if (!RecordHelper::checkLongChar(tableDef, columnDef, (uint)opid, size, varchar))
						goto Fail;
					break;
				}
			default:
				break;
		}
	}

	freeRecord(newRecord);
	return true;

Fail:
	freeRecord(newRecord);
	return false;
}

/**
 * 验证各个索引的正确性，索引本身结构正确性，以及各个索引包含的键值数应该相等
 * @pre 假设此时对整个表没有任何操作，否则这里验证将会不准确
 * @param db	数据库
 * @param table	要检查索引所属的表
 * @return 返回各个索引是否一致
 */
bool ResultChecker::checkIndice(Database *db, Table *table) {
	TableDef *tableDef = table->getTableDef();
	uint numIndice = tableDef->m_numIndice;
	DrsIndice *indice = (DrsBPTreeIndice*)table->getIndice();
	uint tpl = 0;
	for (uint i = 0; i < numIndice; i++) {
		Connection *conn = db->getConnection(false);
		Session *session = db->getSessionManager()->allocSession("ResultChecker::checkIndice", conn);
		MemoryContext *memoryContext = session->getMemoryContext();
		IndexDef *indexDef = tableDef->m_indice[i];
		DrsIndex *index = indice->getIndex(i);

		// 检查索引结构正确
		SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_COMPRESS);
		SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_COMPRESS);
		SubRecord *pkey0 = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_PAD);
		assert(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

		// 检查各个索引项数一致
		RowLockHandle *rlh;
		//DrsIndexScanHandle *handle = index->beginScan(session, NULL, true, true, Shared, &rlh, NULL);
		IndexScanHandle *handle = index->beginScan(session, NULL, true, true, Shared, &rlh, NULL);
		uint count = 0;
		while (index->getNext(handle, NULL)) {
			session->unlockRow(&rlh);
			count++;
		}
		index->endScan(handle);
		if (i == 0) {
			tpl = count;
			cout << "Index keys: " << count << endl;
		} else {
			assert(tpl == count);
			if (tpl != count) {
				db->getSessionManager()->freeSession(session);
				db->freeConnection(conn);
				return false;
			}
		}

		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);
	}

	return true;
}

/**
 * 做一次全表扫描，得到表当中的记录数
 * @param db		数据库
 * @param table		检查的操作表
 * @return 返回检查结果是否正确，只要堆能够正常遍历，都为true
 */
bool ResultChecker::checkHeap(Database *db, Table *table) {
	TableDef *tableDef = table->getTableDef();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("ResultChecker::checkHeap", conn);
	u16 readCols = 0;
	TblScan *handle = table->tableScan(session, OP_READ, 1, &readCols);
	byte *buf = new byte[tableDef->m_maxRecSize];
	u16 count = 0;
	while (table->getNext(handle, buf)) {
		count++;
	}
	table->endScan(handle);
	delete [] buf;
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);

	cout << "Heap records: " << count << endl;
	return true;
}

/**
 * 执行索引键值到堆记录的一致性验证
 * @param tableDef		表定义
 * @param indexDef		索引定义
 * @param key			索引键值内容
 * @param record		记录内容
 * @return 返回验证一致性与否
 */
bool ResultChecker::checkKeyToRecord(TableDef *tableDef, IndexDef *indexDef, byte *key, byte *record) {
	assert(key != NULL && record != NULL);
	u16 numIdxCols = indexDef->m_numCols;
	for (uint i = 0; i < numIdxCols; i++) {
		u16 colNo = indexDef->m_columns[i];
		if (!checkColumn(tableDef, colNo, key, record))
			return false;
	}

	return true;
}

/**
 * 检查同一个表的两个record是否一致
 * @param tableDef	表定义
 * @param rec1		记录1
 * @param rec2		记录2
 * @return 是否一致
 */
bool ResultChecker::checkRecordToRecord(TableDef *tableDef, byte *rec1, byte *rec2) {
	assert(rec1 != NULL && rec2 != NULL);
	u16 numCols = tableDef->m_numCols;
	for (u16 i = 0; i < numCols; i++) {
		if (!checkColumn(tableDef, i, rec1, rec2))
			return false;
	}

	return true;
}


/**
 * 验证指定的两个列的键值是否正确
 * @param tableDef	表定义
 * @param colNo		要验证的REDUNDANT记录的列号
 * @param cols1		第一个列
 * @param cols2		第二个列
 * @return 检查列验证的正确性
 */
bool ResultChecker::checkColumn(TableDef *tableDef, u16 colNo, byte *cols1, byte *cols2) {
	if (RedRecord::isNull(tableDef, cols1, colNo)) {
		if (!RedRecord::isNull(tableDef, cols2, colNo))
			return false;
		return true;
	} else {
		if (RedRecord::isNull(tableDef, cols2, colNo))
			return false;
	}

	ColumnDef *columnDef = tableDef->m_columns[colNo];
	switch (columnDef->m_type) {
		case CT_BIGINT:
			if (RedRecord::readBigInt(tableDef, cols1, colNo) != RedRecord::readBigInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_INT:
			if (RedRecord::readInt(tableDef, cols1, colNo) != RedRecord::readInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_SMALLINT:
			if (RedRecord::readSmallInt(tableDef, cols1, colNo) != RedRecord::readSmallInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_TINYINT:
			if (RedRecord::readTinyInt(tableDef, cols1, colNo) != RedRecord::readTinyInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_VARCHAR:
			{
				byte *keyVC = NULL, *recVC = NULL;
				size_t keySize, recSize;
				RedRecord::readVarchar(tableDef, cols1, colNo, (void**)&keyVC, &keySize);
				RedRecord::readVarchar(tableDef, cols2, colNo, (void**)&recVC, &recSize);
				if (keySize != recSize || memcmp((void*)keyVC, (void*)recVC, keySize) != 0) {
					cout << keySize << "	" << recSize << endl;
					for (uint i = 0; i < keySize; i++)
						cout << keyVC[i];
					cout << endl;
					for (uint i = 0; i < recSize; i++)
						cout << recVC[i];
					cout << endl;

					return false;
				}
			}
			break;
		case CT_CHAR:
			{
				byte *keyC, *recC;
				size_t keySize, recSize;
				RedRecord::readChar(tableDef, cols1, colNo, (void**)&keyC, &keySize);
				RedRecord::readChar(tableDef, cols2, colNo, (void**)&recC, &recSize);
				if (keySize != recSize || memcmp((void*)keyC, (void*)recC, keySize) != 0)
					return false;
			}
			break;
		default:
			return true;
	}

	return true;
}

/**
* 创造指定长度的字符串，供CHAR/VARCHAR/LOB属性使用
* @param memoryContext	内存上下文
* @param size			指定生成字符串长度
* @param tableDef		表定义
* @param columnDef		列定义
* @param opid			根据该opid生成字符串
* @return 返回生成的字符串，空间外部负责释放
*/
char* RecordHelper::getLongChar(MemoryContext *memoryContext, size_t size, const TableDef *tableDef, const ColumnDef *columnDef, uint opId) {
	char fakeChar[255];
	sprintf(fakeChar, "%d %s %s", opId, tableDef->m_name, columnDef->m_name);
	size_t len = strlen(fakeChar);
	char *content = (char*)memoryContext->alloc(size + 1);
	memset(content, 0, size + 1);
	char *cur = content;
	while (size >= len) {
		memcpy(cur, fakeChar, len);
		cur += len;
		size -= len;
	}
	memcpy(cur, fakeChar, size);
	*(cur + size) = '\0';

	return content;
}


/**
* 根据某个opid检查长字符串形数据的正确性
* @param tableDef		表定义
* @param columnDef		列定义
* @param opid			该字符串是根据该opid生成
* @param size			字符串长度
* @param buf			字符串内容
* @return 返回检查正确与否
*/
bool RecordHelper::checkLongChar(const TableDef *tableDef, const ColumnDef *columnDef, uint opid, size_t size, byte *buf) {
	char fakeChar[255];
	sprintf(fakeChar, "%d %s %s", opid, tableDef->m_name, columnDef->m_name);

	uint len = (uint)size;
	uint checkpoint = 0;
	uint checklen = (uint)strlen(fakeChar);
	while (len > 0) {
		if (len < checklen)
			checklen = len;
		if (memcmp(fakeChar, buf + checkpoint, checklen))
			return false;
		checkpoint += checklen;
		len -= checklen;
	}

	return true;
}


/**
* 得到某张表的所有属性数组
* @param tableDef	表定义
* @return 得到表当中所有的列
*/
u16* RecordHelper::getAllColumns(TableDef *tableDef) {
	u16 *columns = new u16[tableDef->m_numCols];
	for (u16 i = 0; i < tableDef->m_numCols; i++)
		columns[i] = i;

	return columns;
}


/**
* 判断某列是否主键列
* @param colNo		列号
* @param indexDef	要判断主键的索引
* @return 是否是主键
*/
bool RecordHelper::isColPrimaryKey(uint colNo, const IndexDef *indexDef) {
	for (uint j = 0; j < indexDef->m_numCols; j++) {
		if (indexDef->m_columns[j] == (u16)colNo)
			return true;
	}

	return false;
}

/**
 * 从指定的REC_REDUNDANT格式的记录生成对应的record
 * @param tableDef	表定义
 * @Param rowId		记录rowId
 * @param record	记录值内容
 * @return 生成的数据记录
 */
Record* RecordHelper::formRecordFromData(TableDef *tableDef, RowId rowId, byte *record) {
	Record *newRecord = RecordBuilder::createEmptyRecord(rowId, REC_REDUNDANT, tableDef->m_maxRecSize);
	memcpy(newRecord->m_data, record, tableDef->m_maxRecSize);

	return newRecord;
}


/**
 * 从指定的REC_REDUNDANT格式的记录生成索引键值subrecord
 * @param tableDef	表定义
 * @param indexDef	索引定义
 * @param rowId		记录的rowId
 * @param record	记录内容
 * @return 生成的索引subrecord键值，外部负责释放
 */
SubRecord* RecordHelper::formIdxKeyFromData(TableDef *tableDef, IndexDef *indexDef, RowId rowId, byte *record) {
	Record *newRecord = formRecordFromData(tableDef, rowId, record);
	SubRecord *subRecord = new SubRecord();
	subRecord->m_columns = new u16[indexDef->m_numCols];
	memcpy(subRecord->m_columns, indexDef->m_columns, indexDef->m_numCols * sizeof(u16));
	subRecord->m_data = new byte[indexDef->m_maxKeySize];
	subRecord->m_size = indexDef->m_maxKeySize;
	subRecord->m_numCols = indexDef->m_numCols;
	subRecord->m_format = KEY_PAD;
	subRecord->m_rowId = rowId;
	memset(subRecord->m_data, 0, indexDef->m_maxKeySize);

	RecordOper::extractKeyRP(tableDef, newRecord, subRecord);
	freeRecord(newRecord);
	return subRecord;
}

/**
 * 判断索引记录内容是否和标记录内容一致
 * @pre	两个记录内容都应该是REC_REDUNDANT格式的
 * @param tableDef	表定义
 * @param indexDef	索引定义
 * @param record	表记录内容
 * @Param key		索引键值内容
 */
s32 RecordHelper::checkEqual(const TableDef *tableDef, const IndexDef *indexDef, const byte *record, const byte *key) {
	SubRecord *pkey = RecordHelper::formIdxKeyFromData((TableDef*)tableDef, (IndexDef*)indexDef, INVALID_ROW_ID, (byte*)record);
	
	SubRecord *ckey = new SubRecord();
	ckey->m_columns = new u16[indexDef->m_numCols];
	memcpy(ckey->m_columns, indexDef->m_columns, indexDef->m_numCols * sizeof(u16));
	ckey->m_data = new byte[indexDef->m_maxKeySize];
	ckey->m_size = indexDef->m_maxKeySize;
	ckey->m_numCols = indexDef->m_numCols;
	ckey->m_format = KEY_COMPRESS;
	ckey->m_rowId = INVALID_ROW_ID;
	memset(ckey->m_data, 0, indexDef->m_maxKeySize);
	RecordOper::convertKeyPC((TableDef*)tableDef, pkey, ckey);

	SubRecord *idxpkey = RecordHelper::formIdxKeyFromData((TableDef*)tableDef, (IndexDef*)indexDef, INVALID_ROW_ID, (byte*)key);

	int result = RecordOper::compareKeyPC((TableDef*)tableDef, idxpkey, ckey);

	freeSubRecord(pkey);
	freeSubRecord(ckey);
	freeSubRecord(idxpkey);

	return result;
}

/**
* 预热指定表，通过索引预热，指定预热0号主键索引还是所有索引，以及是否预热表数据
* @param db			数据库
* @param table		操作的表
* @param allIndice	是否预热所有的索引，如果不是，只预热主键索引
* @param readTableRecord	是否还要根据索引读取标记录预热表
*/
void TableWarmUpHelper::warmUpTableByIndice(Database *db, Table *table, bool allIndice, bool readTableRecord) {
	TableDef *tableDef = table->getTableDef();
	uint warmUpIndiceNum = tableDef->m_numIndice;
	if (!allIndice)
		warmUpIndiceNum = 1;

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TableWarmUpHelper::warmUpTableByIndice", conn);
	Connection *connHeap = db->getConnection(false);

	byte *buf = new byte[tableDef->m_maxRecSize];
	memset(buf, 0, tableDef->m_maxRecSize);
	bool tableWarmUped = false;
	// 预热各个索引
	for (uint i = 0; i < warmUpIndiceNum; i++) {
		Session *sessionHeap = db->getSessionManager()->allocSession("TableWarmUpHelper::warmUpTableByIndice", connHeap);
		Record *record = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, tableDef->m_recFormat, tableDef->m_maxRecSize);
		IndexDef *indexDef = tableDef->m_indice[i];
		IndexScanCond cond((u16)i, NULL, true, true, false);
		TblScan *indexHandle = table->indexScan(session, OP_READ, &cond, indexDef->m_numCols, indexDef->m_columns);
		while (table->getNext(indexHandle, buf)) {
			if (readTableRecord && !tableWarmUped) {
				RowId rowId = indexHandle->getCurrentRid();
				assert(rowId != INVALID_ROW_ID);
				DrsHeap *heap = table->getHeap();
				bool found = heap->getRecord(sessionHeap, rowId, record);
				if (!found) {
					table->endScan(indexHandle);
					db->getSessionManager()->freeSession(sessionHeap);
					cout << "Check index " << indexDef->m_name << " failed" << endl;
					goto Fail;
				}
			}
		}
		db->getSessionManager()->freeSession(sessionHeap);
		table->endScan(indexHandle);
		tableWarmUped = true;
	}

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	db->freeConnection(connHeap);
	delete [] buf;
	return;

Fail:
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	db->freeConnection(connHeap);
	delete [] buf;
	return;
}

/**
 * 创建指定的文件，如果存在，则清空
 * @param filename	文件名
 * @return 返回创建成功与否
 */
bool createSpecifiedFile(const char *filename) {
	u64 errNo;
	File file(filename);
	if (File::isExist(filename)) {	// 已经存在，设置文件长度为0
		errNo = file.open(false);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			return false;
		errNo = file.setSize(0);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			return false;
	} else {	// 创建文件
		errNo = file.create(false, false);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			return false;
	}

	errNo = file.close();
	if (File::getNtseError(errNo) != File::E_NO_ERROR)
		return false;

	return true;
}


/**
 * 备份一个文件
 * @param backupHeapFile	备份到的文件
 * @param origFile			源文件
 */
void backupAFile(char *backupHeapFile, char *origFile) {
	u64 errCode;
	errCode = File::copyFile(backupHeapFile, origFile, true);
	if (File::getNtseError(errCode) != File::E_NO_ERROR) {
		cout << File::explainErrno(errCode) << endl;
		return;
	}
}


/**
* 备份一组NTSE数据库文件
* @param path	备份文件路径
* @param tableName	表名
* @param useMms		是否使用mms
* @param backup true表示备份过程，false表示恢复备份文件
*/
void backupFiles(const char *path, const char *tableName, bool useMms, bool backup) {
	for (uint i = 0; i < MAX_FILES; i++) {
		char origFileName[255];
		char bkFileName[255];
		if (useMms) {
			sprintf(origFileName, "%s/%sUseMms%s", path, tableName, postfixes[i]);
			sprintf(bkFileName, "%s/%sUseMms%s1", path, tableName, postfixes[i]);
		} else {
			sprintf(origFileName, "%s/%sNotUseMms%s", path, tableName, postfixes[i]);
			sprintf(bkFileName, "%s/%sNotUseMms%s1", path, tableName, postfixes[i]);
		}
		if (backup)
			backupAFile(bkFileName, origFileName);
		else
			backupAFile(origFileName, bkFileName);
	}
}