/**
 * �����ģ����������������ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "api/Table.h"
#include "mms/Mms.h"
#include "btree/Index.h"
#include "misc/RecordHelper.h"

using namespace ntse;

namespace ntse {


///////////////////////////////////////////////////////////////////////////////
// Table //////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
 * �ж��������Ƿ����������������
 *
 * @param table ����
 * @param index ��������
 * @param cols ָ����Ҫ������
 * @return �������Ƿ����������������
 */
bool Table::isCoverageIndex(const TableDef *table, const IndexDef *index, const ColList &cols) {
	byte *mask = (byte *)alloca(sizeof(byte) * table->m_numCols);
	memset(mask, 0, sizeof(byte) * table->m_numCols);

	for (u16 i = 0; i < index->m_numCols; i++) {
		if (index->m_prefixLens[i])
			mask[index->m_columns[i]] = 2;	// ǰ׺������
		else
			mask[index->m_columns[i]] = 1;	// ��ͨ������
	}
	for (u16 i = 0; i < cols.m_size; i++)
		if (mask[cols.m_cols[i]] != 1) // �����ȡǰ׺�У����������ֶΣ�������еȣ���������������
			return false;			   // ע�� �����ֶε��ٽ�ֵһ������768�� ���һ����ǰ׺��
	return true;
} 

/** ֹͣ�ײ��ɨ��
 * @param scan ��ɨ��
 */
void Table::stopUnderlyingScan(TblScan *scan) {
	if (scan->m_indexScan) {
		scan->m_index->endScan(scan->m_indexScan);
		scan->m_indexScan = NULL;
	}
	if (scan->m_recInfo) {
		scan->m_recInfo->end();
		scan->m_recInfo = NULL;
	}
}

/**
 * ��֤��������һ���ԡ��������¼��
 * - �������м�¼�����������д洢��Ӧ��ֵ
 * - ����������ֵָ�������ȷ�ļ�¼
 * - �������д����IDָ����ȷ�Ĵ����
 * - �������м�¼������MMS�д����ҷ��࣬����������һ��
 * TODO
 * - �����洢�����д���󱻶�������ֻ��һ����¼����
 * - MMS�����м�¼�ڶ��д洢��Ӧ��¼������������������һ��
 *
 * ��֤�����л�ӱ�����S����ֹд����
 *
 * @param session �Ự
 * @throw NtseException ���ݲ�һ��
 */
void Table::verify(Session *session) throw(NtseException) {
	assert(m_db->getStat() == DB_RUNNING);

	try {
		lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	} catch (NtseException &) { assert(false); }
	if (m_tableDef->m_indexOnly) {
		unlockMeta(session, IL_S);
		return;
	}
	try {
		lock(session, IL_S, -1, __FILE__, __LINE__);
	} catch (NtseException &) { assert(false); }

	assert_always(m_mmsCallback->m_data == this);
	
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcSave(mc);
	RowLockHandle *rlh = NULL;
	u16 readCols[1] = {0};
	SubRecord readSub(REC_REDUNDANT, 1, readCols, (byte *)mc->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);

	m_db->getSyslog()->log(EL_LOG, "Verify table %s phase 1: heap scan", m_path);
	Records::Scan *recScan = m_records->beginScan(session, OP_READ, ColList(1, readCols), NULL, Shared, &rlh, true);
	u64 n = 0;
	uint oldTime = System::fastTime();
	while (recScan->getNext(readSub.m_data)) {
		readSub.m_rowId = recScan->getRedRow()->m_rowId;
		try {
			verifyRecord(session, readSub.m_rowId, NULL);
		} catch (NtseException &e) {
			session->unlockRow(&rlh);
			recScan->end();
			unlock(session, IL_S);
			unlockMeta(session, IL_S);
			throw e;
		}
		session->unlockRow(&rlh);
		n++;
		if ((System::fastTime() - oldTime) >= 10) {
			oldTime = System::fastTime();
			m_db->getSyslog()->log(EL_LOG, "Finished "I64FORMAT"u records", n);
		}
	}
	recScan->end();
	recScan = NULL;
	u64 numRecsInHeap = n;
	bool consistency = true;
	m_db->getSyslog()->log(EL_LOG, "Found "I64FORMAT"u records in heap", numRecsInHeap);

	m_db->getSyslog()->log(EL_LOG, "Verify table %s phase 1: index scan", m_path);
	for (u16 idx = 0; idx < m_tableDef->m_numIndice; idx++) {
		IndexDef *indexDef = m_tableDef->m_indice[idx];
		DrsIndex *index = m_indice->getIndex(idx);

		m_db->getSyslog()->log(EL_LOG, "Scan index %s", indexDef->m_name);

		// ���ڶ�ɨ��ʱ�Ѿ���֤�˶��и���¼���������ж�Ӧ������ֻҪ����ɨ�践�ص������������
		// ��¼������ȣ���һ����һ�µġ���һ������֤��������Զѵ��������
		n = 0;
		IndexScanHandle *idxScan = index->beginScan(session, NULL, true, true, Shared, &rlh, NULL);
		oldTime = System::fastTime();
		while (index->getNext(idxScan, NULL)) {
			session->unlockRow(&rlh);
			n++;
			if ((System::fastTime() - oldTime) >= 10) {
				oldTime = System::fastTime();
				m_db->getSyslog()->log(EL_LOG, "Finished "I64FORMAT"u records", n);
			}
		}
		index->endScan(idxScan);
		if (n == numRecsInHeap)
			continue;

		consistency = false;
		// ������֤���ֲ�һ��ʱ���ٴ�ɨ�貢��֤ÿ����¼�Է�����������¼
		m_db->getSyslog()->log(EL_LOG, "Index %s has different entries ("I64FORMAT"u) with heap, rescan to find out what's wrong", indexDef->m_name, n); 

		u16 *sortedIdxCols = (u16 *)mc->dup(indexDef->m_columns, sizeof(u16) * indexDef->m_numCols);
		std::sort(sortedIdxCols, sortedIdxCols + indexDef->m_numCols);

		SubRecord idxSub(REC_REDUNDANT, indexDef->m_numCols, sortedIdxCols,
			(byte *)mc->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
		SubRecord recSub(REC_REDUNDANT, indexDef->m_numCols, sortedIdxCols,
			(byte *)mc->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
		SubToSubExtractor *idxExtractor = SubToSubExtractor::createInst(mc, m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns,
			indexDef->m_numCols, idxSub.m_columns, KEY_COMPRESS, REC_REDUNDANT, 1000);
		SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tableDef, &recSub, (uint)-1, m_records->getRowCompressMng());
		idxScan = index->beginScan(session, NULL, true, true, Shared, &rlh, idxExtractor);

		n = 0;
		oldTime = System::fastTime();
		while (index->getNext(idxScan, &idxSub)) {
			bool exist = m_records->getSubRecord(session, idxSub.m_rowId, &recSub, srExtractor);
			// ����������ֲ�һ�²���ֹͣ����Ҫ����ɨ��õ����в�һ�µ�����ڹ۲����
			if (!exist) {
				m_db->getSyslog()->log(EL_LOG, "One index entry is not found in heap, whose rowId is "I64FORMAT"", idxSub.m_rowId); 
			} else if (!RecordOper::isSubRecordEq(m_tableDef, &idxSub, &recSub)) {
				m_db->getSyslog()->log(EL_LOG, "One index entry is not consistency with heap, whose rowId is "I64FORMAT"", idxSub.m_rowId); 
			}
			session->unlockRow(&rlh);
			n++;
			if ((System::fastTime() - oldTime) >= 10) {
				oldTime = System::fastTime();
				m_db->getSyslog()->log(EL_LOG, "Finished "I64FORMAT"u records", n);
			}
		}
		index->endScan(idxScan);
	}

	unlock(session, IL_S);
	unlockMeta(session, IL_S);

	if (!consistency)
		NTSE_THROW(NTSE_EC_CORRUPTED, "Heap and index inconsistent");
}

/** ���ָ����¼������һ����
 *
 * @param session �Ự
 * @param rid Ҫ���ļ�¼RID
 * @param expected ����ΪNULL������ΪNULL������ָ֤�����������ȣ�ΪREC_MYSQL��ʽ
 * @throw NtseException ���ݲ�һ��
 */
void Table::verifyRecord(Session *session, RowId rid, const SubRecord *expected) throw(NtseException) {
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcSave(mc);

	if (m_tableDef->m_indexOnly)
		return;

	m_records->verifyRecord(session, rid, expected);
	// ��ȡ��¼
	ColList allCols = ColList::generateAscColList(mc, 0, m_tableDef->m_numCols);
	SubRecord readSub(REC_REDUNDANT, m_tableDef->m_numCols, allCols.m_cols, 
		(byte *)mc->calloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
	m_records->getSubRecord(session, rid, &readSub);
	Record readRec(readSub.m_rowId, REC_REDUNDANT, readSub.m_data, m_tableDef->m_maxRecSize);

	// �Ƿ��ڸ��������ж�Ӧ��ֵ
	for (u16 idx = 0; idx < m_tableDef->m_numIndice; idx++) {
		McSavepoint lobSavepoint(session->getLobContext());
		IndexDef *indexDef = m_tableDef->m_indice[idx];
		DrsIndex *index = m_indice->getIndex(idx);

		SubRecord key(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, (byte *)mc->calloc(indexDef->m_maxKeySize),
			indexDef->m_maxKeySize, rid);

		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, getLobStorage(), &readRec, &lobArray);
		}
		RecordOper::extractKeyRP(m_tableDef, indexDef, &readRec, &lobArray, &key);

		u16 *sortedIdxCols = (u16 *)mc->dup(indexDef->m_columns, sizeof(u16) * indexDef->m_numCols);
		std::sort(sortedIdxCols, sortedIdxCols + indexDef->m_numCols);
		SubRecord idxReadKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns,
			(byte *)mc->calloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(mc, m_tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns,
			idxReadKey.m_numCols, sortedIdxCols, KEY_COMPRESS, KEY_PAD, 1000);
		IndexScanHandle *idxScan = index->beginScan(session, &key, true, true, None, NULL, extractor);
		if (!index->getNext(idxScan, &idxReadKey)) {
			index->endScan(idxScan);
			m_db->getSyslog()->log(EL_LOG, "Heap record can't be found in index %d, whose rowId is "I64FORMAT"u", idx, rid); 
			NTSE_THROW(NTSE_EC_CORRUPTED, "Referenced index entry not found");
		}

		SubRecord heapRecKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns,
			(byte *)mc->calloc(indexDef->m_maxKeySize), indexDef->m_maxKeySize);

		RecordOper::extractKeyRP(m_tableDef, indexDef, &readRec, &lobArray, &heapRecKey);

		if (!RecordOper::isSubRecordEq(m_tableDef, &heapRecKey, &idxReadKey, indexDef)) {
			index->endScan(idxScan);
			m_db->getSyslog()->log(EL_LOG, "Heap record is found in index %d, but value is not equal, whose rowId is "I64FORMAT"u", idx, rid); 
			NTSE_THROW(NTSE_EC_CORRUPTED, "Referenced index entry not found");
		}

		if (index->getNext(idxScan, &idxReadKey) && idxReadKey.m_rowId == rid) {
			index->endScan(idxScan);
			m_db->getSyslog()->log(EL_LOG, "Heap record is found multiple times in index %d, whose rowId is "I64FORMAT"u", idx, rid); 
			NTSE_THROW(NTSE_EC_CORRUPTED, "Referenced index entry found multiple times");
		}

		index->endScan(idxScan);
	}
}

/** ���ָ����¼������һ����
 *
 * @param session �Ự
 * @param expected ��֤��¼���������ȣ�ΪREC_MYSQL��ʽ
 */
void Table::verifyRecord(Session *session, const Record *expected) {
	assert(expected->m_format == REC_MYSQL);

	McSavepoint mcSave(session->getMemoryContext());
	ColList allCols = ColList::generateAscColList(session->getMemoryContext(), 0, m_tableDef->m_numCols);
	SubRecord subRec(REC_MYSQL, m_tableDef->m_numCols, allCols.m_cols, expected->m_data, m_tableDef->m_maxRecSize,
		expected->m_rowId);
	try {
		verifyRecord(session, expected->m_rowId, &subRec);
	} catch (NtseException &e) {
		m_db->getSyslog()->log(EL_PANIC, "%s", e.getMessage());
		assert_always(false);
	}
}

/** ��ָ֤���ļ�¼�Ƿ��Ѿ���ɾ��
 *
 * @param session �Ự
 * @param rid ��¼RID
 * @param indexAttrs ���������ԣ�ΪREC_REDUNDANT��ʽ������û��������ΪNULL
 */
void Table::verifyDeleted(Session *session, RowId rid, const SubRecord *indexAttrs) {
	assert(!indexAttrs || indexAttrs->m_format == REC_REDUNDANT);

	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcSave(mc);

	if (!m_tableDef->m_indexOnly)
		m_records->verifyDeleted(session, rid);

	// ������û�ж�Ӧ��ֵ
	for (u16 idx = 0; idx < m_tableDef->m_numIndice; idx++) {
		McSavepoint lobSavepoint(session->getLobContext());
		IndexDef *indexDef = m_tableDef->m_indice[idx];
		DrsIndex *index = m_indice->getIndex(idx);

		Record rec(indexAttrs->m_rowId, REC_REDUNDANT, indexAttrs->m_data, indexAttrs->m_size);
		SubRecord key(KEY_PAD, indexDef->m_numCols, indexDef->m_columns,
			(byte *)mc->alloc(indexDef->m_maxKeySize), indexDef->m_maxKeySize,
			indexAttrs->m_rowId);

		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, m_tableDef, indexDef, getLobStorage(), &rec, &lobArray);
		}
		RecordOper::extractKeyRP(m_tableDef, indexDef, &rec, &lobArray, &key);

		IndexScanHandle *idxScan = index->beginScan(session, &key, true, true, None, NULL, NULL);
		NTSE_ASSERT(!index->getNext(idxScan, NULL) || idxScan->getRowId() != indexAttrs->m_rowId);
		index->endScan(idxScan);
	}
}

/** ɨ��֮ǰ����Ƿ��Ѿ��Ӻ��˱���
 *
 * @param session �Ự
 * @param opType Ҫ���еĲ���
 * @return �Ƿ���˺��ʵı���
 */
bool Table::checkLock(Session *session, OpType opType) {
	if (!(getMetaLock(session) == IL_S || getMetaLock(session) == IL_U || getMetaLock(session) == IL_X))
		return false;
	if (opType == OP_READ) {
		return (getLock(session) == IL_IS || getLock(session) == IL_IX || getLock(session) == IL_SIX
			|| getLock(session) == IL_S || getLock(session) == IL_X);
	} else {
		return (getLock(session) == IL_IX || getLock(session) == IL_SIX || getLock(session) == IL_X);
	}
}

/**
 * ɨ��֮ǰ����Ҫ���еĲ������ͼӺ��ʵı���������Ԫ��������������
 *
 * @param session �Ự
 * @param opType Ҫ���еĲ���
 * @throw NtseException ������ʱ
 */
void Table::lockTable(Session *session, OpType opType) throw(NtseException) {
	lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	try {
		lock(session, opType == OP_READ? IL_IS: IL_IX, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		unlockMeta(session, IL_S);
		throw e;
	}
}

/**
 * ɨ��֮����ݽ��еĲ��������ͷű���������Ԫ��������������
 *
 * @param session �Ự
 * @param opType ���еĲ���
 */
void Table::unlockTable(Session *session, OpType opType) {
	unlock(session, opType == OP_READ? IL_IS: IL_IX);
	unlockMeta(session, IL_S);
}

/** �ӱ�������
 * @pre �����Ѿ�����Ԫ������
 *
 * @param session �Ự
 * @param mode ��ģʽ
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ
 */
void Table::lock(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
		UNREFERENCED_PARAMETER(timeoutMs);
		UNREFERENCED_PARAMETER(file);
		UNREFERENCED_PARAMETER(line);
	}
	else {
#endif 
		assert(getMetaLock(session) != IL_NO);
		ftrace(ts.ddl || ts.dml, tout << session << mode << timeoutMs);
		if (!m_tblLock.lock(session->getId(), mode, timeoutMs, file, line))
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Acquire lock %s on table %s timeout.", IntentionLock::getLockStr(mode), m_tableDef->m_name);
		session->addLock(&m_tblLock);
#ifdef TNT_ENGINE
	}
#endif
}

/** ����������������oldMode��newMode��Ȼ�oldMode�Ǳ�newMode���߼��������򲻽����κβ�����
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ��ʧ�ܣ�NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void Table::upgradeLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	ftrace(ts.ddl || ts.dml, tout << session << oldMode << newMode << timeoutMs);
	if (newMode == oldMode || IntentionLock::isHigher(newMode, oldMode))
		return;
	assert(IntentionLock::isHigher(oldMode, newMode));
	if (!m_tblLock.upgrade(session->getId(), oldMode, newMode, timeoutMs, file, line))
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Upgrade lock %s to %s on table %s timeout.", 
			IntentionLock::getLockStr(oldMode), IntentionLock::getLockStr(newMode), m_tableDef->m_name);
	if (oldMode == IL_NO && newMode != IL_NO)
		session->addLock(&m_tblLock);
}

/** ����������������oldMode��newMode��ȣ���newMode��oldMode�߼��򲻽����κβ���
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 */
void Table::downgradeLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	ftrace(ts.ddl || ts.dml, tout << session << oldMode << newMode);
	if (oldMode == newMode || IntentionLock::isHigher(oldMode, newMode))
		return;
	assert(IntentionLock::isHigher(newMode, oldMode));
	m_tblLock.downgrade(session->getId(), oldMode, newMode, file, line);
	if (oldMode != IL_NO && newMode == IL_NO)
		session->removeLock(&m_tblLock);
}

/** �ͷű�������
 * @pre �������ͷű�Ԫ������֮ǰ����
 *
 * @param session �Ự
 * @param mode ��ģʽ
 */
void Table::unlock(Session *session, ILMode mode) {
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
	} 
	else {
#endif
		assert(getMetaLock(session) != IL_NO);
		ftrace(ts.ddl || ts.dml, tout << session << mode);
		m_tblLock.unlock(session->getId(), mode);
		session->removeLock(&m_tblLock);
#ifdef TNT_ENGINE
	}
#endif
}

/** �õ�ָ���ĻỰ�ӵı�������ģʽ
 * @param session �Ự
 * @return �Ự���ӵı�������û�м����򷵻�IL_NO
 */
ILMode Table::getLock(Session *session) const {
	return m_tblLock.getLock(session->getId());
}

/** �õ���������ʹ�����ͳ����Ϣ
 * @return ����ʹ�����ͳ����Ϣ
 */
const IntentionLockUsage* Table::getLockUsage() const {
	return m_tblLock.getUsage();
}

#ifdef TNT_ENGINE
/** �ӱ�Ԫ������,����session��û��trx,�������
 * @pre ����δ�������������ӱ�Ԫ�����������ڼ�������֮ǰ
 *
 * @param session �Ự
 * @param mode ��ģʽ��ֻ����S��U��X
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ
 */
void Table::tntLockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	assert(mode == IL_S || mode == IL_U || mode == IL_X);
	assert(getLock(session) == IL_NO);
	ftrace(ts.ddl || ts.dml, tout << this << session << mode << timeoutMs);
	if (!m_metaLock.lock(session->getId(), mode, timeoutMs, file, line)) {
		nftrace(ts.ddl || ts.dml, tout << "Lock failed");
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Acquire meta lock %s.", IntentionLock::getLockStr(mode));
	}
	session->addLock(&m_metaLock);
}

/** �ͷű�Ԫ������
 * @pre �������ͷű�������֮�����
 *
 * @param session �Ự
 * @param mode ��ģʽ
 */
void Table::tntUnlockMeta(Session *session, ILMode mode) {
	assert(getLock(session) == IL_NO);
	ftrace(ts.ddl || ts.dml, tout << this << session << mode);
	m_metaLock.unlock(session->getId(), mode);
	session->removeLock(&m_metaLock);
}
#endif

/** �ӱ�Ԫ������
 * @pre ����δ�������������ӱ�Ԫ�����������ڼ�������֮ǰ
 *
 * @param session �Ự
 * @param mode ��ģʽ��ֻ����S��U��X
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ
 */
void Table::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
		UNREFERENCED_PARAMETER(timeoutMs);
		UNREFERENCED_PARAMETER(file);
		UNREFERENCED_PARAMETER(line);
	}
	else {
#endif
		assert(mode == IL_S || mode == IL_U || mode == IL_X);
		assert(getLock(session) == IL_NO);
		ftrace(ts.ddl || ts.dml, tout << this << session << mode << timeoutMs);
		if (!m_metaLock.lock(session->getId(), mode, timeoutMs, file, line)) {
			nftrace(ts.ddl || ts.dml, tout << "Lock failed");
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Acquire meta lock %s.", IntentionLock::getLockStr(mode));
		}
		session->addLock(&m_metaLock);
#ifdef TNT_ENGINE
	}
#endif
}

/** ������Ԫ����������oldMode��newMode��Ȼ�oldMode�Ǳ�newMode���߼��������򲻽����κβ�����
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ��ʧ�ܣ�NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void Table::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(oldMode);
		UNREFERENCED_PARAMETER(newMode);
		UNREFERENCED_PARAMETER(timeoutMs);
		UNREFERENCED_PARAMETER(file);
		UNREFERENCED_PARAMETER(line);
	}
	else {
#endif
		assert(oldMode == IL_NO || oldMode == IL_S || oldMode == IL_U || oldMode == IL_X);
		assert(newMode == IL_NO || newMode == IL_S || newMode == IL_U || newMode == IL_X);
		ftrace(ts.ddl || ts.dml, tout << this << session << oldMode << newMode << timeoutMs);
		if (newMode == oldMode || IntentionLock::isHigher(newMode, oldMode))
			return;
		assert(IntentionLock::isHigher(oldMode, newMode));
		if (!m_metaLock.upgrade(session->getId(), oldMode, newMode, timeoutMs, file, line)) {
			nftrace(ts.ddl || ts.dml, tout << "Upgrade failed");
			NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Upgrade meta lock %s to %s.", 
				IntentionLock::getLockStr(oldMode), IntentionLock::getLockStr(newMode));
		}
		if (oldMode == IL_NO && newMode != IL_NO)
			session->addLock(&m_metaLock);
#ifdef TNT_ENGINE
	}
#endif
}

/** ������Ԫ����������oldMode��newMode��ȣ���newMode��oldMode�߼��򲻽����κβ���
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 */
void Table::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
#ifdef TNT_ENGINE
	if (session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(oldMode);
		UNREFERENCED_PARAMETER(newMode);
		UNREFERENCED_PARAMETER(file);
		UNREFERENCED_PARAMETER(line);
	}
	else {
#endif
		assert(oldMode == IL_NO || oldMode == IL_S || oldMode == IL_U || oldMode == IL_X);
		assert(newMode == IL_NO || newMode == IL_S || newMode == IL_U || newMode == IL_X);
		ftrace(ts.ddl || ts.dml, tout << this << session << oldMode << newMode);
		if (oldMode == newMode || IntentionLock::isHigher(oldMode, newMode))
			return;
		assert(IntentionLock::isHigher(newMode, oldMode));
		m_metaLock.downgrade(session->getId(), oldMode, newMode, file, line);
		if (oldMode != IL_NO && newMode == IL_NO)
			session->removeLock(&m_metaLock);
#ifdef TNT_ENGINE
	}
#endif
}

/** �ͷű�Ԫ������
 * @pre �������ͷű�������֮�����
 *
 * @param session �Ự
 * @param mode ��ģʽ
 */
void Table::unlockMeta(Session *session, ILMode mode) {
#ifdef TNT_ENGINE
	if(session->getTrans() != NULL) {
		UNREFERENCED_PARAMETER(mode);
	}
	else {
#endif
		assert(getLock(session) == IL_NO);
		ftrace(ts.ddl || ts.dml, tout << this << session << mode);
		m_metaLock.unlock(session->getId(), mode);
		session->removeLock(&m_metaLock);
#ifdef TNT_ENGINE
	}	
#endif
}

/** �õ�ָ���ĻỰ�ӵı�Ԫ������ģʽ
 * @param session �Ự
 * @return �Ự���ӵı�Ԫ����������û�м����򷵻�IL_NO
 */
ILMode Table::getMetaLock(Session *session) const {
	return m_metaLock.getLock(session->getId());
}

/** �õ���Ԫ������ʹ�����ͳ����Ϣ
 * @return ����ʹ�����ͳ����Ϣ
 */
const IntentionLockUsage* Table::getMetaLockUsage() const {
	return m_metaLock.getUsage();
}

/** �滻���и����ڴ����
 * @param another �滻�����
 */
void Table::replaceComponents(const Table *another) {
	m_records = another->m_records;
	m_tableDef = another->m_tableDef;
	m_indice = another->m_indice;
	m_path = another->m_path;
	m_tableDefWithAddingIndice = m_tableDef;
	m_hasCprsDict = another->m_hasCprsDict;

	m_mmsCallback = another->m_mmsCallback;
	m_mmsCallback->m_data = this;	
}

/** �޸ı�ID
 * @param session �Ự
 * @param tableId �µı�ID
 */
void Table::setTableId(Session *session, u16 tableId) {
	m_records->setTableId(session, tableId);
	writeTableDef();
}

///////////////////////////////////////////////////////////////////////////////
// TblScan /////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** ����UPDATE/DELETE����޸Ĳ�����Ϣ
 *
 * @param session �Ự
 * @param tableDef ����
 * @param updCols �����µ����ԣ���ʱʹ�ã��������
 * @param readCols ����ȡ�����ԣ���ʱʹ�ã��������
 */
TSModInfo::TSModInfo(Session *session, const TableDef *tableDef, const ColList &updCols, const ColList &readCols) {
	MemoryContext *mc = session->getMemoryContext();
	ColList updatedIndexAttrs(0, NULL);
	for (u16 i = 0; i < tableDef->m_numIndice; i++) {
		const IndexDef *indexDef = tableDef->m_indice[i];
		ColList indexCols = ColList(indexDef->m_numCols, indexDef->m_columns).sort(mc);
		if (indexCols.hasIntersect(updCols))
			updatedIndexAttrs = updatedIndexAttrs.merge(mc, indexCols);
	}

	m_updIndex = updatedIndexAttrs.m_size > 0;
	if (m_updIndex) {
		void *p = mc->alloc(sizeof(SubRecord));
		m_indexPreImage = new (p)SubRecord(REC_REDUNDANT, updatedIndexAttrs.m_size, updatedIndexAttrs.m_cols, NULL, tableDef->m_maxRecSize);
	} else
		m_indexPreImage = NULL;
	
	m_missCols = updatedIndexAttrs.except(mc, readCols);
}

/** ׼�����»�ɾ��һ����¼
 * @param redSr Ҫ���»�ɾ���ļ�¼��ǰ��
 */
void TSModInfo::prepareForUpdate(const SubRecord *redSr) {
	if (m_indexPreImage) {
		m_indexPreImage->m_rowId = redSr->m_rowId;
		m_indexPreImage->m_data = redSr->m_data;
	}
}

/**
 * ����һ��ɨ����
 *
 * @param session �Ự
 * @param numCols Ҫ��ȡ��������������Ϊ0
 * @param columns Ҫ��ȡ�������б�����ΪNULL
 *   �ڴ�ʹ��Լ��������
 * @param opType ��������
 * @param tblLock ɨ��ʱ�Ƿ�ӱ���
 * @return ɨ�����������ڴ��session��MemoryContext�з���
 */
TblScan* Table::allocScan(Session *session, u16 numCols, u16 *columns, OpType opType, bool tblLock) {
	assert(numCols && columns);

	void *p = session->getMemoryContext()->calloc(sizeof(TblScan));
	TblScan *scan = new (p)TblScan();
	scan->m_session = session;
	scan->m_bof = true;
	scan->m_tblLock = tblLock;
	scan->m_opType = opType;
	scan->m_table = this;
	scan->m_tableDef = m_tableDef;
	scan->m_pkey = m_tableDef->m_pkey;
	scan->m_readCols.m_size = numCols;
	scan->m_readCols.m_cols = (u16 *)session->getMemoryContext()->dup(columns, sizeof(u16) * numCols);
	return scan;
}

/** ����ɨ�������� */
TblScan::TblScan() {
	memset(this, 0, sizeof(TblScan));
}

/** ִ��DELETE���ʱ��ʼ���޸Ĳ�����Ϣ */
void TblScan::initDelInfo() {
	assert(m_opType == OP_DELETE || m_opType == OP_WRITE);
	void *p = m_session->getMemoryContext()->calloc(sizeof(TSModInfo));
	m_delInfo = new (p)TSModInfo(m_session, m_table->m_tableDefWithAddingIndice,
		ColList::generateAscColList(m_session->getMemoryContext(), 0, m_tableDef->m_numCols),
		m_readCols);
	m_recInfo->setDeleteInfo(m_delInfo->m_missCols);
}

/** ִ��UPDATE���ʱ������Ҫ���µ�����
 *
 * @param numCols Ҫ���µ����Ը���
 * @param columns ��Ҫ���µ����Ժţ�ֱ�����ã����������������
 */
void TblScan::setUpdateColumns(u16 numCols, u16 *columns) {
	assert(m_opType == OP_UPDATE || m_opType == OP_WRITE);
	assert(ColList(numCols, columns).isAsc());
	assert(!m_updInfo);	// �����ظ�����

	void *p = m_session->getMemoryContext()->alloc(sizeof(TSModInfo));
	m_updInfo = new (p)TSModInfo(m_session, m_table->m_tableDefWithAddingIndice, ColList(numCols, columns), m_readCols);
	m_recInfo->setUpdateInfo(ColList(numCols, columns), m_updInfo->m_missCols);
}

/** ����tnt purgeʱ����del��������Ҫ���µ�����(����Ҫ���´����)
 *
 * @param numCols Ҫ���µ����Ը���
 * @param columns ��Ҫ���µ����Ժţ�ֱ�����ã����������������
 */
void TblScan::setPurgeUpdateColumns(u16 numCols, u16 *columns) {
	setUpdateColumns(numCols, columns);
	m_recInfo->getUpdInfo()->m_updLob = false;
}

/** ׼������UPDATE����
 * @param mysqlUpdate MySQL��ʽ�ĸ��º���
 * @param oldRow ����ΪNULL����ָ����¼ǰ��ΪREC_MYSQL��ʽ
 * @param isUpdateEngineFormat �����Ƿ���������ʽ���������ַ�������º������purge update
 * @throw NtseException ��¼����
 */
void TblScan::prepareForUpdate(const byte *mysqlUpdate, const byte *oldRow, bool isUpdateEngineFormat) throw(NtseException) {
	Record mysqlUpdateRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)mysqlUpdate, m_tableDef->m_maxMysqlRecSize);
	Record mysqlOldRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)oldRow, m_tableDef->m_maxMysqlRecSize);

	// ������г����䳤�ֶΣ���Ҫ��mysqlRow��ʽ����ת��
	if (!isUpdateEngineFormat && m_tableDef->hasLongVar()) {
		byte *update = (byte *)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
		Record realMysqlUpdateRec(INVALID_ROW_ID, REC_MYSQL, update, m_tableDef->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_tableDef, &mysqlUpdateRec, &realMysqlUpdateRec);	
		mysqlUpdate = update;

		if (oldRow) {
			byte *old = (byte *)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
			Record realMysqlOldRec(INVALID_ROW_ID, REC_MYSQL, old, m_tableDef->m_maxRecSize);
			RecordOper::convertRecordMUpToEngine(m_tableDef, &mysqlOldRec, &realMysqlOldRec);	
			oldRow = old;
		}
	} 
	
	// ���ϴ�ɨ��֮��m_mysqlRow->m_data��¼ָ�����ǰ�������
	// �����ڵ���updateʱ���ϲ��Ѿ���ǰ��ͺ�����ڴ潻������˴�ʱm_mysqlRowʵ��ָ����Ǻ����Ҫ���¸�ֵ
	if (oldRow && m_coverageIndex)
		m_redRow->m_data = (byte *)oldRow;
	if (oldRow) {
		if (m_redRow->m_data == m_mysqlRow->m_data)
			m_redRow->m_data = (byte *)oldRow;
		m_mysqlRow->m_data = (byte *)oldRow;
	}
	m_recInfo->prepareForUpdate(m_redRow, m_mysqlRow, mysqlUpdate);
	m_updInfo->prepareForUpdate(m_redRow);
}

/** ׼������DELETE���� */
void TblScan::prepareForDelete() {
	m_recInfo->prepareForDelete(m_redRow);
	m_delInfo->prepareForUpdate(m_redRow);
}

/** �õ�ɨ�����Ͷ�Ӧ�ı�ɨ�����ͳ������
 * @param scanType ɨ������
 * @return ͳ������
 */
StatType TblScan::getTblStatTypeForScanType(ScanType scanType) {
	if (scanType == ST_TBL_SCAN)
		return OPS_TBL_SCAN;
	else if (scanType == ST_IDX_SCAN)
		return OPS_IDX_SCAN;
	else {
		assert(scanType == ST_POS_SCAN);
		return OPS_POS_SCAN;
	}
}

/** �õ�ɨ�����Ͷ�Ӧ����ɨ�����ͳ������
 * @param scanType ɨ������
 * @return ͳ������
 */
StatType TblScan::getRowStatTypeForScanType(ScanType scanType) {
	if (scanType == ST_TBL_SCAN)
		return OPS_TBL_SCAN_ROWS;
	else if (scanType == ST_IDX_SCAN)
		return OPS_IDX_SCAN_ROWS;
	else {
		assert(scanType == ST_POS_SCAN);
		return OPS_POS_SCAN_ROWS;
	}
}

/**
 * ������������
 * @pre ɨ�����ͣ��������ͣ�Ҫ��ȡ�������Ѿ�����
 */
void TblScan::determineRowLockPolicy() {
	if (m_opType == OP_READ) {
		if ((m_type == ST_TBL_SCAN && m_table->m_records->couldNoRowLockReading(m_readCols))
			|| (m_type == ST_IDX_SCAN && m_coverageIndex)) {
			m_rowLock = None;
			m_pRlh = NULL;
		} else {
			m_rowLock = Shared;
			m_pRlh = &m_rlh;
		}
	} else {
		m_rowLock = Exclusived;
		m_pRlh = &m_rlh;
	}
}

/** ����Ƿ��Ѿ�EOF��
 * @return �Ƿ��Ѿ�EOF
 */
bool TblScan::checkEofOnGetNext() {
	if (m_eof)
		return true;
	if (m_type == ST_IDX_SCAN && m_singleFetch && !m_bof) {
		m_eof = true;
		return true;
	}
	return false;
}

}
