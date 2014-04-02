/**
* �Ը�����Tableִ��DML������ʵ��
*
* �ձ�(bsu@corp.netease.com, naturally@163.org)
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
* ��������ı���ʱ�����ڼ��ɲ���ʹ�õı�ÿ�����һ����������������
*/

/**
 * �ڶ�Ӧ���в���һ����¼����¼��key��opid����
 * @pre ��ĵ�һ������Ϊ�������������Ҽ�ֵ��bigint
 * @param session	�Ự���
 * @param table		�����ı����
 * @param key		�����ֵ������
 * @param opid		�ü�¼��ֵʹ�õ�opid
 * @return ���ز���ɹ�SUCCESS�����߼�ֵ��ͻʧ��FAIL
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
* ���¶�Ӧ����һ������>=k�ĵ�һ����¼����¼��kplus��opid����
* @pre ��ĵ�һ������Ϊ�������������Ҽ�ֵ��bigint
* @param session	�Ự���
* @param table		���������
* @param key		IN/OUT Ҫ���µ������������Ǹ��µ�һ��>=key�ļ�¼�������������¼�¼������
* @param kplus		IN/OUT ���²�����ѡ���ԵĽ�ԭʼ�������³ɸ�ֵ�����ص��Ǹ���֮����������ֵ
* @param opid		���¼�¼ʹ�õ�opid
* @param check		ָ���Ƿ���Ҫ����¼��һ���ԣ�����ñ�Ĳ��������������Ա�ͳһ���£����������¼һ���ԣ����򲻿���
* @post �ⲿ�����key��kplus�������ͬ��ֵ�������ܾ�����ͬ���󣩣���ô��ʱ��Ȼ�ᾭ�������޸�ͬһ���󣬵��ǲ����ܳ��ַ��ش��������ͬ��ֵ��ֻ��ֱ��޸���������
* @return ���³ɹ�SUCCESS�����߸��³���Ψһ�Գ�ͻ����FAIL/�Ҳ�����ֵ����out_of_bound����֤��¼��ֵʧ�ܷ���check_fail
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

	if (*key == *kplus) {	// ˵�����θ��²���������������Ҫ����������ȷ
		*kplus = RedRecord::readBigInt(tableDef, buf, 0);
	}

	// ���¼�ֵ
	newRecord = RecordBuilder::createEmptyRecord(INVALID_ROW_ID, REC_MYSQL, tableDef->m_maxRecSize);
	updateColumns = updateSomeColumnsOfRecord(memoryContext, table, newRecord, *kplus, opid, &updatedCols);
	scanHandle->setUpdateColumns((u16)updatedCols, updateColumns);
	succ = table->updateCurrent(scanHandle, newRecord->m_data);
	table->endScan(scanHandle);

	if (succ) {
		// ��������ɾ���ļ�ֵkey
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
* ɾ��ָ�����е�һ������>=key�ĵ�һ����¼
* @pre ��ĵ�һ������Ϊ�������������Ҽ�ֵ��bigint
* @param session	�Ự���
* @param table		���������
* @param key		IN/OUT ɾ��>=key�ĵ�һ����¼������keyΪ����ɾ����¼������
* @return �Ƿ������ôһ����¼����ɾ���ɹ������ܲ����ڼ�¼������OUT_OF_BOUND
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

	// ��������ɾ���ļ�ֵkey
	*key = RedRecord::readBigInt(tableDef, buf, 0);

	table->deleteCurrent(scanHandle);
	table->endScan(scanHandle);
	delete [] buf;
	freeSubRecord(findKey);

	return SUCCESS;
}

/**
* ɨ���ȡָ����������>=key�ĵ�һ����¼���ɲ���ָ��
* @pre ��ĵ�һ������Ϊ�������������Ҽ�ֵ��bigint
* @post record out ������ȡ�õļ�¼���ݣ�����ΪNULL��������
* @param session	�Ự���
* @param table		���������
* @param key		����key����ȡ��һ��>=key������
* @param record		OUT	�����ΪNULL�����ػ�ȡ��¼�ļ�ֵ����
* @param rowId		OUT ���ص�rowId��Ϣ������ΪNULL��������
* @param precise	��ǰ��=��ѯTRUE����>=��ѯFALSE
* @return ɨ������Ϊ�գ�����SUCCESS�����򷵻�FAIL
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
* ����key��opid����һ���¼�ֵ
* @param memoryContext	�ڴ������ľ��
* @param table			���������
* @param record			���¼�¼ԭʼ����
* @param key			����������������ø�ֵ��Ϊ���º������
* @param opid			���µļ�ֵ����opid����
* @param updateCols		IN/OUT ָ�����µ�����������ʵ�ʸ��µ�����
* @return ���ظ������������飬�ռ��ⲿ�����ͷ�
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
		// ���������Ƿ�Ҫ����
		// ��50%�Ļ�����Ը��¸��У�������Ҫ�жϲ����¸����Ƿ�ᵼ�¶Ա���¼�ĸ��´ﲻ���ܸ���Ҫ��
		if (RandomGen::nextInt() % 2 == 0 && updated + tableDef->m_numCols - i > *updateCols)
			continue;

		assert(updated < *updateCols);
		columns[updated++] = i;
		ColumnDef *columnDef = tableDef->m_columns[i];
		// �����жϸ������ǲ���������������key����
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
 * ��ӡ����ͳ����Ϣ
 * @para table	���������ı�
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
* ����table������index��ӳ�䣬ͬʱ���ܻ���Ҫ����¼�����Ƿ�һ��
* @param db			���ݿ�
* @param table		���������
* @param checkTable	�Ƿ�ִ�б���������֤
* @param checkRecord�Ƿ���֤��¼������ȷ��
* @return ��鶼�ɹ�true���κ�ʧ��false
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
		// ����¼�����һ����
		if (checkRecord && !ResultChecker::checkRecord(table, buf, rowId1, NULL)) {
			cout << "Check table " << table->getPath() << " failed" << endl;
			table->endScan(handle);
			goto Fail;
		}

		if (checkTable) {	// ��ÿ����¼ɨ�����������֤��¼����
			u64 key = RedRecord::readBigInt(tableDef, buf, 0);
			// TODO: findKey��������memoryContext��������ȫ����������Ч��
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
						// ����ʹ�÷�Χɨ�迴�ܷ���ҵ���ֵ����Ϊ�ڶ��Ѿ��������ü�ֵ���ᱻɾ���޸�
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
* ���Ӹ���index��table��ӳ�䣬ͬʱ���ܻ���Ҫ����ֵ�����Ƿ�һ��
* @param db			���ݿ�
* @param table		���������
* @param checkIndex �Ƿ���֤��������
* @param checkKey	�Ƿ��������ļ�ֵ
* @return ���ؼ���Ƿ���ȷ
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
 * ִ��ָ����������ļ��
 * @param db		���ݿ�
 * @param table		�����
 * @param indexNo	Ҫ��֤������
 * @param checkKey	���������ֵ����
 * @return �����Ƿ�����ȷ
 */
bool ResultChecker::checkIndexToTable(Database *db, Table *table, uint indexNo, bool checkKey) {
	TableDef *tableDef = table->getTableDef();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("ResultChecker::checkIndexToTable_index", conn);
	byte *tblbuf = new byte[tableDef->m_maxRecSize];
	memset(tblbuf, 0, tableDef->m_maxRecSize);
	u16 *columns = RecordHelper::getAllColumns(tableDef);
	SubRecord *idxSb = new SubRecord(REC_REDUNDANT, tableDef->m_numCols, columns, new byte[tableDef->m_maxRecSize], tableDef->m_maxRecSize);
	// Ϊ�˱�֤�κ�ʱ�̶��ܽ���һ���Լ�飬���׼����������ģ���ɨ�裬�ڲ�ʹ��table�Ľӿ�
	// �������ܼ����ɹ���ͬʱ�ֱ�֤����ɨ������Ķ����������ݣ��������heap�Ľӿڣ��������ݴ�����mms��δˢ��
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
		//// ��������������ӡ��ǰ����������
		//if (indexNo == 0) {
		//	cout << RedRecord::readBigInt(tableDef, idxSb->m_data, 0) << endl;
		//}

		// ��������ļ�ֵ
		if (checkKey && !ResultChecker::checkRecord(table, idxSb->m_data, rowId, indexDef)) {
			ResultChecker::checkRecord(table, idxSb->m_data, rowId, indexDef);
			cout << "Check index " << indexDef->m_name << " failed, " << rowId << " key can't be consistency" << endl;
			session->unlockRow(&rlh);
			index->endScan(idxHandle);
			goto Fail;
		}

		// ͨ��table�õ����еļ�¼
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

		// ���record��������ֵ��һ����
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
* ���ָ����¼�����Ƿ�����һ����
* ����ֻ����������smallint/tiny int/int/bigint/char/varchar����
* @pre record���ȱ�����tableDef->m_maxRecSize
* @param table		���������
* @param record		Ҫ���ļ�¼����
* @param rowId		��¼��rowId
* @param indexDef	��NULL����ʾ��ǰҪ���ļ�¼�Ǹ������ļ�ֵ���ǲ������ļ�¼������Ϊ���¼����
* @return ���ؼ������ȷ��
*/
bool ResultChecker::checkRecord(Table *table, byte *record, RowId rowId, IndexDef *indexDef) {
	bool checkTableRecord = (indexDef == NULL);
	TableDef *tableDef = table->getTableDef();
	Record *newRecord = RecordHelper::formRecordFromData(tableDef, rowId, record);

	s64 opid = -1;
	uint size = 0;
	// ȷ�ϴӼ�¼���п��ܵõ�������opidֵ����һ��opid����INT������������ڶ���С���Σ������ȷ�ϵ�ǰ���ı�����ʽ
	u16 checkColumns = checkTableRecord ? tableDef->m_numCols : indexDef->m_numCols;
	for (u16 i = 0; i < checkColumns; i++) {
		u16 colNo = checkTableRecord ? i : indexDef->m_columns[i];
		ColumnDef *columnDef = tableDef->m_columns[colNo];
		if (RecordHelper::isColPrimaryKey(colNo, tableDef->m_pkey))
			continue;	// ��Ҫ�Թ�����

		if (RedRecord::isNull(tableDef, record, colNo))
			continue;	// �Թ�null��

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

	if (opid == -1) {	// û����ֵ�У�Ĭ��һ��
		freeRecord(newRecord);
		return true;
	}

	// �õ�opid�������ʽ֮��ȷ��ÿ�����Զ�����
	for (u16 i = 0; i < checkColumns; i++) {
		u16 colNo = checkTableRecord ? i : indexDef->m_columns[i];
		ColumnDef *columnDef = tableDef->m_columns[colNo];
		if (RecordHelper::isColPrimaryKey(colNo, tableDef->m_pkey))
			continue;	// ��Ҫ�Թ�����

		if (RedRecord::isNull(tableDef, record, colNo))
			continue;	// �Թ�null��

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
				// TODO: ���ڱ��뱣֤����õ���opid��32λ�ģ��������ַ������о��޷���ȷ�ж�
				{
					byte *chars;
					size_t size;
					RedRecord::readChar(tableDef, newRecord->m_data, colNo, (void**)&chars, &size);
					if (!RecordHelper::checkLongChar(tableDef, columnDef, (uint)opid, size, chars))
						goto Fail;
					break;
				}
			case CT_VARCHAR:
				// TODO: ���ڱ��뱣֤����õ���opid��32λ�ģ��������ַ������о��޷���ȷ�ж�
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
 * ��֤������������ȷ�ԣ���������ṹ��ȷ�ԣ��Լ��������������ļ�ֵ��Ӧ�����
 * @pre �����ʱ��������û���κβ���������������֤���᲻׼ȷ
 * @param db	���ݿ�
 * @param table	Ҫ������������ı�
 * @return ���ظ��������Ƿ�һ��
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

		// ��������ṹ��ȷ
		SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_COMPRESS);
		SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_COMPRESS);
		SubRecord *pkey0 = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_PAD);
		assert(((DrsBPTreeIndex*)index)->verify(session, key1, key2, pkey0, true));

		// ��������������һ��
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
 * ��һ��ȫ��ɨ�裬�õ����еļ�¼��
 * @param db		���ݿ�
 * @param table		���Ĳ�����
 * @return ���ؼ�����Ƿ���ȷ��ֻҪ���ܹ�������������Ϊtrue
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
 * ִ��������ֵ���Ѽ�¼��һ������֤
 * @param tableDef		����
 * @param indexDef		��������
 * @param key			������ֵ����
 * @param record		��¼����
 * @return ������֤һ�������
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
 * ���ͬһ���������record�Ƿ�һ��
 * @param tableDef	����
 * @param rec1		��¼1
 * @param rec2		��¼2
 * @return �Ƿ�һ��
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
 * ��ָ֤���������еļ�ֵ�Ƿ���ȷ
 * @param tableDef	����
 * @param colNo		Ҫ��֤��REDUNDANT��¼���к�
 * @param cols1		��һ����
 * @param cols2		�ڶ�����
 * @return �������֤����ȷ��
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
* ����ָ�����ȵ��ַ�������CHAR/VARCHAR/LOB����ʹ��
* @param memoryContext	�ڴ�������
* @param size			ָ�������ַ�������
* @param tableDef		����
* @param columnDef		�ж���
* @param opid			���ݸ�opid�����ַ���
* @return �������ɵ��ַ������ռ��ⲿ�����ͷ�
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
* ����ĳ��opid��鳤�ַ��������ݵ���ȷ��
* @param tableDef		����
* @param columnDef		�ж���
* @param opid			���ַ����Ǹ��ݸ�opid����
* @param size			�ַ�������
* @param buf			�ַ�������
* @return ���ؼ����ȷ���
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
* �õ�ĳ�ű��������������
* @param tableDef	����
* @return �õ��������е���
*/
u16* RecordHelper::getAllColumns(TableDef *tableDef) {
	u16 *columns = new u16[tableDef->m_numCols];
	for (u16 i = 0; i < tableDef->m_numCols; i++)
		columns[i] = i;

	return columns;
}


/**
* �ж�ĳ���Ƿ�������
* @param colNo		�к�
* @param indexDef	Ҫ�ж�����������
* @return �Ƿ�������
*/
bool RecordHelper::isColPrimaryKey(uint colNo, const IndexDef *indexDef) {
	for (uint j = 0; j < indexDef->m_numCols; j++) {
		if (indexDef->m_columns[j] == (u16)colNo)
			return true;
	}

	return false;
}

/**
 * ��ָ����REC_REDUNDANT��ʽ�ļ�¼���ɶ�Ӧ��record
 * @param tableDef	����
 * @Param rowId		��¼rowId
 * @param record	��¼ֵ����
 * @return ���ɵ����ݼ�¼
 */
Record* RecordHelper::formRecordFromData(TableDef *tableDef, RowId rowId, byte *record) {
	Record *newRecord = RecordBuilder::createEmptyRecord(rowId, REC_REDUNDANT, tableDef->m_maxRecSize);
	memcpy(newRecord->m_data, record, tableDef->m_maxRecSize);

	return newRecord;
}


/**
 * ��ָ����REC_REDUNDANT��ʽ�ļ�¼����������ֵsubrecord
 * @param tableDef	����
 * @param indexDef	��������
 * @param rowId		��¼��rowId
 * @param record	��¼����
 * @return ���ɵ�����subrecord��ֵ���ⲿ�����ͷ�
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
 * �ж�������¼�����Ƿ�ͱ��¼����һ��
 * @pre	������¼���ݶ�Ӧ����REC_REDUNDANT��ʽ��
 * @param tableDef	����
 * @param indexDef	��������
 * @param record	���¼����
 * @Param key		������ֵ����
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
* Ԥ��ָ����ͨ������Ԥ�ȣ�ָ��Ԥ��0�������������������������Լ��Ƿ�Ԥ�ȱ�����
* @param db			���ݿ�
* @param table		�����ı�
* @param allIndice	�Ƿ�Ԥ�����е�������������ǣ�ֻԤ����������
* @param readTableRecord	�Ƿ�Ҫ����������ȡ���¼Ԥ�ȱ�
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
	// Ԥ�ȸ�������
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
 * ����ָ�����ļ���������ڣ������
 * @param filename	�ļ���
 * @return ���ش����ɹ����
 */
bool createSpecifiedFile(const char *filename) {
	u64 errNo;
	File file(filename);
	if (File::isExist(filename)) {	// �Ѿ����ڣ������ļ�����Ϊ0
		errNo = file.open(false);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			return false;
		errNo = file.setSize(0);
		if (File::getNtseError(errNo) != File::E_NO_ERROR)
			return false;
	} else {	// �����ļ�
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
 * ����һ���ļ�
 * @param backupHeapFile	���ݵ����ļ�
 * @param origFile			Դ�ļ�
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
* ����һ��NTSE���ݿ��ļ�
* @param path	�����ļ�·��
* @param tableName	����
* @param useMms		�Ƿ�ʹ��mms
* @param backup true��ʾ���ݹ��̣�false��ʾ�ָ������ļ�
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