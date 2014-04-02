/** 内存堆的恢复操作
 * author 忻丁峰 xindingfeng@corp.netease.com
 */
#include "rec/MRecords.h"
#include "api/Table.h"

namespace tnt {
/** redo insert操作，就是重做insert操作
 * @param session 会话
 * @param rid 插入记录的rowId
 */
void MRecords::redoInsert(Session *session, RowId rid, TrxId trxId, RedoType redoType) {
	MemoryContext *ctx = session->getMemoryContext();
	HashIndexEntry *entry = m_hashIndex->get(rid, ctx);
	if (entry != NULL) {
		//如果rid存在，说明该rowId是被重用的。
		//即在宕机前，该rowId已经被purge出去，但在redo的时候，purge是最后才做的
		if (entry->m_type == HIT_MHEAPREC) {
			m_heap->removeHeapRecordAndHash(session, rid);
		} else {
			assert(entry->m_type == HIT_TXNID);
			m_hashIndex->remove(rid);
		}
	}

	if (redoType == RT_PREPARE) {
		m_hashIndex->insert(rid, trxId, getVersion(), HIT_TXNID);
	}
}

/** redo首次更新操作
 * @param session 会话
 * @param txnId 事务id
 * @param rid 更新记录的rowId
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 * @param updateRec 更新后像
 */
void MRecords::redoUpdate(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, Record *afterImg) {
	assert(REC_REDUNDANT == afterImg->m_format);
	UNREFERENCED_PARAMETER(rid);
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	bool ret = false;
	Record *rec = convertRVF(afterImg, ctx);
	ret = m_heap->insertHeapRecordAndHash(session, txnId, rollBackId, tableIndex, rec, 0 /**delbit*/, getVersion());
	assert(ret == true);
	UNREFERENCED_PARAMETER(ret);
	return;
}

/** redo非首次更新操作
 * @param session 会话
 * @param txnId 事务id
 * @param rid 更新记录的rowId
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 * @param updateSub 更新字段值
 */
void MRecords::redoUpdate(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *updateSub) {
	bool ret;
	assert(updateSub->m_format == REC_REDUNDANT);
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
//_ReStart:
	HashIndexEntry *entry = m_hashIndex->get(rid, ctx);
	assert(entry != NULL && entry->m_type == HIT_MHEAPREC);
	void *ptr = (void *)entry->m_value;
	//如果记录未被整理的话，ptr仍然有效，可以少一次hash索引查找
	//恢复流程为单线程，所以checkAndLatchPage一定能成功
	//ret = m_heap->checkAndLatchPage(session, ptr, rid, Exclusived);
	//assert(ret == true);
	/*while (!m_heap->checkAndLatchPage(session, ptr, rid, Exclusived)) {
		ctx->resetToSavepoint(sp);
		goto _ReStart;
	}*/

	MHeapRec *heapRec = NULL;
	Record *rec = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
	//m_heap->getHeapRedRecordSafe(session, ptr, rid, rec);
	m_heap->getHeapRedRecord(session, ptr, rid, rec);
	assert(rec != NULL);
	RecordOper::updateRecordRR(*m_tableDef, rec, updateSub);

	Record *rec1 = convertRVF(rec, ctx);
	heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(txnId, rollBackId, tableIndex, rec1, 0/**delBit*/);

	//ret = m_heap->updateHeapRecordAndHash(session, ptr, heapRec);
	ret = m_heap->updateHeapRecordAndHash(session, rid, ptr, heapRec);
	assert(ret == true);
	//m_heap->unLatchPageByPtr(session, ptr, Exclusived);
	
	ctx->resetToSavepoint(sp);
}

/** redo首次删除操作
 * @param session 会话
 * @param txnId 事务id
 * @param rid 删除记录的rowId
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 * @param delRec 删除前像
 */
void MRecords::redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, Record *delRec) {
	//由于purge recover的延后，内存中存在相应的记录，此时进行是首次更新恢复
	//m_heap->removeHeapRecordAndHash(session, rid);
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	assert(rid == delRec->m_rowId);
	assert(delRec->m_format == REC_REDUNDANT);
	Record *rec = convertRVF(delRec, ctx);
	bool ret = m_heap->insertHeapRecordAndHash(session, txnId, rollBackId, tableIndex, rec, 1/**delbit*/, getVersion());
	assert(ret == true);
	UNREFERENCED_PARAMETER(ret);
}

/** redo非首次删除操作
 * @param session 会话
 * @param txnId 事务id
 * @param rid 删除记录的rowId
 * @param rollBackId 回滚记录的rowId
 * @param tableIndex 版本池序号
 */
void MRecords::redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex) {
	bool ret;
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
//_ReStart:
	HashIndexEntry *entry = m_hashIndex->get(rid, ctx);
	assert(entry != NULL && entry->m_type == HIT_MHEAPREC);

	//恢复流程为单线程，所以checkAndLatchPage一定能成功
	//ret = m_heap->checkAndLatchPage(session, (void *)entry->m_value, rid, Exclusived);
	//assert(ret == true);
	/*if (!m_heap->checkAndLatchPage(session, (void *)entry->m_value, rid, Exclusived)) {
		ctx->resetToSavepoint(sp);
		goto _ReStart;
	}*/

	//MHeapRec *heapRec = m_heap->getHeapRecordSafe(session, (void *)entry->m_value, rid);
	MHeapRec *heapRec = m_heap->getHeapRecord(session, (void *)entry->m_value, rid);
	assert(heapRec != NULL);

	heapRec->m_txnId = txnId;
	heapRec->m_rollBackId = rollBackId;
	heapRec->m_vTableIndex = tableIndex;
	heapRec->m_del = FLAG_MHEAPREC_DEL;

	//ret = m_heap->updateHeapRecordAndHash(session, (void *)entry->m_value, heapRec);
	ret = m_heap->updateHeapRecordAndHash(session, rid, (void *)entry->m_value, heapRec);
	assert(ret == true);
	//m_heap->unLatchPageByPtr(session, (void *)entry->m_value, Exclusived);
	
	ctx->resetToSavepoint(sp);
}

/** undo Insert操作。insert操作值插hash索引和事务数组，所以undo的时候只需要移除hash索引
 * @param session 会话
 * @param rid 插入记录的rowId
 */
void MRecords::undoInsert(Session *session, RowId rid) {
	MemoryContext *ctx = NULL;
	assert((ctx = session->getMemoryContext()) != NULL);
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(ctx);
	HashIndexEntry *entry = NULL;
	assert((entry = m_hashIndex->get(rid, ctx)) != NULL && entry->m_type == HIT_TXNID);
	UNREFERENCED_PARAMETER(entry);
	bool ret = m_hashIndex->remove(rid);
	assert(ret);
	UNREFERENCED_PARAMETER(ret);
}

/** undo首次更新操作
 * @param session 会话
 * @param rid 更新记录的rowId
 * return 首次更新前的全像
 */
void MRecords::undoFirUpdate(Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}

/** undo非首次更新操作
 * @param session 会话
 * @param rid 更新记录的rowId
 * return 更新前全像
 */
void MRecords::undoSecUpdate(Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}

/** undo首次删除操作
 * @param session 会话
 * @param rid 删除记录的rowId
 * return 删除前的全像
 */
void MRecords::undoFirRemove(Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}

/** undo非首次删除操作
 * @param session 会话
 * @param rid 删除记录的rowId
 * return 删除前的全像
 */
void MRecords::undoSecRemove(ntse::Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}



/** 回滚钱获得更新后项
 * @param session 会话
 * @param rid 回滚记录的rowId
 * return 回滚前内存记录的后项，REC_REDUNDENT格式
 */
MHeapRec *MRecords::getBeforeAndAfterImageForRollback(Session *session, RowId rid, Record **beforeImg, Record *afterImg) {
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp1 = ctx->setSavepoint();
	*beforeImg = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
	u64 sp2 = ctx->setSavepoint();
	MHeapRec *rollBackRec = NULL;
_ReStart:
	HashIndexEntry* entry = m_hashIndex->get(rid, ctx);
	//因为rollBack事务肯定是未提交事务，所以不肯能被purge出去
	assert(entry != NULL);

	HashIndexType type = entry->m_type;
	if (type == HIT_TXNID) {
		ctx->resetToSavepoint(sp1);
		*beforeImg = NULL;
	} else {
		SYNCHERE(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
		//获取当前的内存堆记录
		MHeapRec *heapRec = m_heap->getHeapRedRecord(session, (void*)entry->m_value, entry->m_rowId, *beforeImg);
		if (heapRec == NULL) {
			ctx->resetToSavepoint(sp2);
			goto _ReStart;
		}
		afterImg->m_format = (*beforeImg)->m_format;
		afterImg->m_rowId = (*beforeImg)->m_rowId;
		afterImg->m_size = (*beforeImg)->m_size;
		memcpy(afterImg->m_data, (*beforeImg)->m_data, (*beforeImg)->m_size);
		if (heapRec->m_rollBackId == 0) {
			ctx->resetToSavepoint(sp1);
			*beforeImg = NULL;
		} else {
			//根据当前内存堆记录及回滚的rowid进行回滚记录
			rollBackRec = m_versionPool->getRollBackHeapRec(session, *m_tableDef, heapRec->m_vTableIndex, &heapRec->m_rec, heapRec->m_rollBackId);
			assert(rollBackRec->m_rec.m_rowId == rid);
			if (rollBackRec->m_rollBackId == INVALID_ROW_ID) {
				*beforeImg = NULL;
			}
		}
	}
	return rollBackRec;
}

/** 回滚记录操作
 * @param session 会话
 * @param rid 回滚记录的rowId
 * return 回滚前内存记录的前像，REC_REDUNDENT格式
 */
void MRecords::rollBackRecord(Session *session, RowId rid, MHeapRec* rollBackRec) {
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	HashIndexEntry* entry = m_hashIndex->get(rid, ctx);
	//因为rollBack事务肯定是未提交事务，所以不肯能被purge出去
	assert(entry != NULL);

	HashIndexType type = entry->m_type;
	if (type == HIT_TXNID) {
		m_hashIndex->remove(rid);
	} else {
		SYNCHERE(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
		//根据当前内存堆记录及回滚的rowid进行回滚记录
		assert(rollBackRec == NULL || rollBackRec->m_rec.m_rowId == rid);
		if (rollBackRec != NULL && rollBackRec->m_rollBackId != INVALID_ROW_ID) {
			if ((*m_tableDef)->m_recFormat == REC_VARLEN || (*m_tableDef)->m_recFormat == REC_COMPRESSED) {
				byte *buf = (byte *)ctx->alloc((*m_tableDef)->m_maxRecSize);
				Record *varRec = new (ctx->alloc(sizeof(Record))) Record(rollBackRec->m_rec.m_rowId, REC_VARLEN, buf, (*m_tableDef)->m_maxRecSize);
				RecordOper::convertRecordRV(*m_tableDef, &rollBackRec->m_rec, varRec);
				memcpy(&rollBackRec->m_rec, varRec, sizeof(Record));
			} else {
				assert((*m_tableDef)->m_recFormat == REC_FIXLEN);
				rollBackRec->m_rec.m_format = REC_FIXLEN;
			}
			rollBackRec->m_size = rollBackRec->getSerializeSize();
			//m_heap->updateHeapRecordAndHash(session, rid, rollBackRec, entry->m_version);
			m_heap->updateHeapRecordAndHash(session, rid, NULL, rollBackRec, entry->m_version);
		} else {
			if (rollBackRec == NULL || rollBackRec->m_txnId == 0) {
				m_heap->removeHeapRecordAndHash(session, rid);
			} else {
				m_heap->updateHeapRec2TxnIDHash(session, rollBackRec->m_rec.m_rowId, rollBackRec->m_txnId);
			}
		}
	}
	ctx->resetToSavepoint(sp);
}
}