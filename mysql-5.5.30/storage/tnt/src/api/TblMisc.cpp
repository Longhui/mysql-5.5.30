/**
 * 表管理模块非主干流程杂项功能实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
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
 * 判断索引中是否包含所有所需属性
 *
 * @param table 表定义
 * @param index 索引定义
 * @param cols 指定需要的属性
 * @return 索引中是否包含所有所需属性
 */
bool Table::isCoverageIndex(const TableDef *table, const IndexDef *index, const ColList &cols) {
	byte *mask = (byte *)alloca(sizeof(byte) * table->m_numCols);
	memset(mask, 0, sizeof(byte) * table->m_numCols);

	for (u16 i = 0; i < index->m_numCols; i++) {
		if (index->m_prefixLens[i])
			mask[index->m_columns[i]] = 2;	// 前缀索引列
		else
			mask[index->m_columns[i]] = 1;	// 普通索引列
	}
	for (u16 i = 0; i < cols.m_size; i++)
		if (mask[cols.m_cols[i]] != 1) // 如果读取前缀列（包括超长字段，大对象列等）不能走索引覆盖
			return false;			   // 注： 超长字段的临界值一定大于768， 因此一定是前缀列
	return true;
} 

/** 停止底层的扫描
 * @param scan 表扫描
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
 * 验证表中数据一致性。包含以下检查
 * - 堆中所有记录在所有索引中存储对应键值
 * - 所有索引键值指向堆中正确的记录
 * - 堆中所有大对象ID指向正确的大对象
 * - 堆中所有记录，若在MMS中存在且非脏，则两者数据一致
 * TODO
 * - 大对象存储中所有大对象被堆中有且只有一条记录引用
 * - MMS中所有记录在堆中存储对应记录，若非脏则两者数据一致
 *
 * 验证过程中会加表数据S锁禁止写操作
 *
 * @param session 会话
 * @throw NtseException 数据不一致
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

		// 由于堆扫描时已经验证了堆中各记录在索引中有对应的项，因此只要索引扫描返回的索引项数与堆
		// 记录条数相等，则一定是一致的。这一快速验证不会产生对堆的随机访问
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
		// 快速验证发现不一致时，再次扫描并验证每条记录以发现是哪条记录
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
			// 这里如果发现不一致不能停止，需要继续扫描得到所有不一致的项，便于观察调试
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

/** 检查指定记录的数据一致性
 *
 * @param session 会话
 * @param rid 要检查的记录RID
 * @param expected 可能为NULL，若不为NULL，则验证指定属性与此相等，为REC_MYSQL格式
 * @throw NtseException 数据不一致
 */
void Table::verifyRecord(Session *session, RowId rid, const SubRecord *expected) throw(NtseException) {
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcSave(mc);

	if (m_tableDef->m_indexOnly)
		return;

	m_records->verifyRecord(session, rid, expected);
	// 读取记录
	ColList allCols = ColList::generateAscColList(mc, 0, m_tableDef->m_numCols);
	SubRecord readSub(REC_REDUNDANT, m_tableDef->m_numCols, allCols.m_cols, 
		(byte *)mc->calloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
	m_records->getSubRecord(session, rid, &readSub);
	Record readRec(readSub.m_rowId, REC_REDUNDANT, readSub.m_data, m_tableDef->m_maxRecSize);

	// 是否在各索引中有对应键值
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

/** 检查指定记录的数据一致性
 *
 * @param session 会话
 * @param expected 验证记录内容与此相等，为REC_MYSQL格式
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

/** 验证指定的记录是否已经被删除
 *
 * @param session 会话
 * @param rid 记录RID
 * @param indexAttrs 各索引属性，为REC_REDUNDANT格式，若表没有索引则为NULL
 */
void Table::verifyDeleted(Session *session, RowId rid, const SubRecord *indexAttrs) {
	assert(!indexAttrs || indexAttrs->m_format == REC_REDUNDANT);

	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcSave(mc);

	if (!m_tableDef->m_indexOnly)
		m_records->verifyDeleted(session, rid);

	// 索引中没有对应键值
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

/** 扫描之前检查是否已经加好了表锁
 *
 * @param session 会话
 * @param opType 要进行的操作
 * @return 是否加了合适的表锁
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
 * 扫描之前根据要进行的操作类型加合适的表锁，包括元数据锁和数据锁
 *
 * @param session 会话
 * @param opType 要进行的操作
 * @throw NtseException 加锁超时
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
 * 扫描之后根据进行的操作类型释放表锁，包括元数据锁和数据锁
 *
 * @param session 会话
 * @param opType 进行的操作
 */
void Table::unlockTable(Session *session, OpType opType) {
	unlock(session, opType == OP_READ? IL_IS: IL_IX);
	unlockMeta(session, IL_S);
}

/** 加表数据锁
 * @pre 必须已经加了元数据锁
 *
 * @param session 会话
 * @param mode 锁模式
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时
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

/** 升级表数据锁。若oldMode与newMode相等或oldMode是比newMode更高级的锁，则不进行任何操作。
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时或失败，NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
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

/** 降级表数据锁。若oldMode与newMode相等，若newMode比oldMode高级则不进行任何操作
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
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

/** 释放表数据锁
 * @pre 必须在释放表元数据锁之前调用
 *
 * @param session 会话
 * @param mode 锁模式
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

/** 得到指定的会话加的表数据锁模式
 * @param session 会话
 * @return 会话所加的表锁，若没有加锁则返回IL_NO
 */
ILMode Table::getLock(Session *session) const {
	return m_tblLock.getLock(session->getId());
}

/** 得到表数据锁使用情况统计信息
 * @return 表锁使用情况统计信息
 */
const IntentionLockUsage* Table::getLockUsage() const {
	return m_tblLock.getUsage();
}

#ifdef TNT_ENGINE
/** 加表元数据锁,不管session有没有trx,都必须加
 * @pre 必须未加数据锁，即加表元数据锁必须在加数据锁之前
 *
 * @param session 会话
 * @param mode 锁模式，只能是S、U或X
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时
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

/** 释放表元数据锁
 * @pre 必须在释放表数据锁之后调用
 *
 * @param session 会话
 * @param mode 锁模式
 */
void Table::tntUnlockMeta(Session *session, ILMode mode) {
	assert(getLock(session) == IL_NO);
	ftrace(ts.ddl || ts.dml, tout << this << session << mode);
	m_metaLock.unlock(session->getId(), mode);
	session->removeLock(&m_metaLock);
}
#endif

/** 加表元数据锁
 * @pre 必须未加数据锁，即加表元数据锁必须在加数据锁之前
 *
 * @param session 会话
 * @param mode 锁模式，只能是S、U或X
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时
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

/** 升级表元数据锁。若oldMode与newMode相等或oldMode是比newMode更高级的锁，则不进行任何操作。
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时或失败，NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
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

/** 降级表元数据锁。若oldMode与newMode相等，若newMode比oldMode高级则不进行任何操作
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
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

/** 释放表元数据锁
 * @pre 必须在释放表数据锁之后调用
 *
 * @param session 会话
 * @param mode 锁模式
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

/** 得到指定的会话加的表元数据锁模式
 * @param session 会话
 * @return 会话所加的表元数据锁，若没有加锁则返回IL_NO
 */
ILMode Table::getMetaLock(Session *session) const {
	return m_metaLock.getLock(session->getId());
}

/** 得到表元数据锁使用情况统计信息
 * @return 表锁使用情况统计信息
 */
const IntentionLockUsage* Table::getMetaLockUsage() const {
	return m_metaLock.getUsage();
}

/** 替换表中各个内存对象
 * @param another 替换成这个
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

/** 修改表ID
 * @param session 会话
 * @param tableId 新的表ID
 */
void Table::setTableId(Session *session, u16 tableId) {
	m_records->setTableId(session, tableId);
	writeTableDef();
}

///////////////////////////////////////////////////////////////////////////////
// TblScan /////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** 构造UPDATE/DELETE语句修改操作信息
 *
 * @param session 会话
 * @param tableDef 表定义
 * @param updCols 语句更新的属性，临时使用，必须递增
 * @param readCols 语句读取的属性，临时使用，必须递增
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

/** 准备更新或删除一条记录
 * @param redSr 要更新或删除的记录的前像
 */
void TSModInfo::prepareForUpdate(const SubRecord *redSr) {
	if (m_indexPreImage) {
		m_indexPreImage->m_rowId = redSr->m_rowId;
		m_indexPreImage->m_data = redSr->m_data;
	}
}

/**
 * 分配一个扫描句柄
 *
 * @param session 会话
 * @param numCols 要读取的属性数，不能为0
 * @param columns 要读取的属性列表，不能为NULL
 *   内存使用约定：拷贝
 * @param opType 操作类型
 * @param tblLock 扫描时是否加表锁
 * @return 扫描句柄，所有内存从session的MemoryContext中分配
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

/** 构造扫描句柄对象 */
TblScan::TblScan() {
	memset(this, 0, sizeof(TblScan));
}

/** 执行DELETE语句时初始化修改操作信息 */
void TblScan::initDelInfo() {
	assert(m_opType == OP_DELETE || m_opType == OP_WRITE);
	void *p = m_session->getMemoryContext()->calloc(sizeof(TSModInfo));
	m_delInfo = new (p)TSModInfo(m_session, m_table->m_tableDefWithAddingIndice,
		ColList::generateAscColList(m_session->getMemoryContext(), 0, m_tableDef->m_numCols),
		m_readCols);
	m_recInfo->setDeleteInfo(m_delInfo->m_missCols);
}

/** 执行UPDATE语句时，设置要更新的属性
 *
 * @param numCols 要更新的属性个数
 * @param columns 各要更新的属性号，直接引用，不拷贝，必须递增
 */
void TblScan::setUpdateColumns(u16 numCols, u16 *columns) {
	assert(m_opType == OP_UPDATE || m_opType == OP_WRITE);
	assert(ColList(numCols, columns).isAsc());
	assert(!m_updInfo);	// 不能重复设置

	void *p = m_session->getMemoryContext()->alloc(sizeof(TSModInfo));
	m_updInfo = new (p)TSModInfo(m_session, m_table->m_tableDefWithAddingIndice, ColList(numCols, columns), m_readCols);
	m_recInfo->setUpdateInfo(ColList(numCols, columns), m_updInfo->m_missCols);
}

/** 用于tnt purge时，非del操作设置要更新的属性(不需要更新大对象)
 *
 * @param numCols 要更新的属性个数
 * @param columns 各要更新的属性号，直接引用，不拷贝，必须递增
 */
void TblScan::setPurgeUpdateColumns(u16 numCols, u16 *columns) {
	setUpdateColumns(numCols, columns);
	m_recInfo->getUpdInfo()->m_updLob = false;
}

/** 准备进行UPDATE操作
 * @param mysqlUpdate MySQL格式的更新后像
 * @param oldRow 若不为NULL，则指定记录前像，为REC_MYSQL格式
 * @param isUpdateEngineFormat 更新是否是引擎层格式，用于区分非事务更新和事务表purge update
 * @throw NtseException 记录超长
 */
void TblScan::prepareForUpdate(const byte *mysqlUpdate, const byte *oldRow, bool isUpdateEngineFormat) throw(NtseException) {
	Record mysqlUpdateRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)mysqlUpdate, m_tableDef->m_maxMysqlRecSize);
	Record mysqlOldRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)oldRow, m_tableDef->m_maxMysqlRecSize);

	// 如果含有超长变长字段，需要对mysqlRow格式记做转换
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
	
	// 在上次扫描之后m_mysqlRow->m_data记录指向的是前项的内容
	// 但是在调用update时，上层已经将前项和后项的内存交换，因此此时m_mysqlRow实际指向的是后项，需要重新赋值
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

/** 准备进行DELETE操作 */
void TblScan::prepareForDelete() {
	m_recInfo->prepareForDelete(m_redRow);
	m_delInfo->prepareForUpdate(m_redRow);
}

/** 得到扫描类型对应的表扫描操作统计类型
 * @param scanType 扫描类型
 * @return 统计类型
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

/** 得到扫描类型对应的行扫描操作统计类型
 * @param scanType 扫描类型
 * @return 统计类型
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
 * 决定行锁策略
 * @pre 扫描类型，操作类型，要读取的属性已经设置
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

/** 检查是否已经EOF了
 * @return 是否已经EOF
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
