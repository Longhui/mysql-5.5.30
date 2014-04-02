/** �ڴ�ѵĻָ�����
 * author �ö��� xindingfeng@corp.netease.com
 */
#include "rec/MRecords.h"
#include "api/Table.h"

namespace tnt {
/** redo insert��������������insert����
 * @param session �Ự
 * @param rid �����¼��rowId
 */
void MRecords::redoInsert(Session *session, RowId rid, TrxId trxId, RedoType redoType) {
	MemoryContext *ctx = session->getMemoryContext();
	HashIndexEntry *entry = m_hashIndex->get(rid, ctx);
	if (entry != NULL) {
		//���rid���ڣ�˵����rowId�Ǳ����õġ�
		//����崻�ǰ����rowId�Ѿ���purge��ȥ������redo��ʱ��purge����������
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

/** redo�״θ��²���
 * @param session �Ự
 * @param txnId ����id
 * @param rid ���¼�¼��rowId
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 * @param updateRec ���º���
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

/** redo���״θ��²���
 * @param session �Ự
 * @param txnId ����id
 * @param rid ���¼�¼��rowId
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 * @param updateSub �����ֶ�ֵ
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
	//�����¼δ������Ļ���ptr��Ȼ��Ч��������һ��hash��������
	//�ָ�����Ϊ���̣߳�����checkAndLatchPageһ���ܳɹ�
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

/** redo�״�ɾ������
 * @param session �Ự
 * @param txnId ����id
 * @param rid ɾ����¼��rowId
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 * @param delRec ɾ��ǰ��
 */
void MRecords::redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, Record *delRec) {
	//����purge recover���Ӻ��ڴ��д�����Ӧ�ļ�¼����ʱ�������״θ��»ָ�
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

/** redo���״�ɾ������
 * @param session �Ự
 * @param txnId ����id
 * @param rid ɾ����¼��rowId
 * @param rollBackId �ع���¼��rowId
 * @param tableIndex �汾�����
 */
void MRecords::redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex) {
	bool ret;
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
//_ReStart:
	HashIndexEntry *entry = m_hashIndex->get(rid, ctx);
	assert(entry != NULL && entry->m_type == HIT_MHEAPREC);

	//�ָ�����Ϊ���̣߳�����checkAndLatchPageһ���ܳɹ�
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

/** undo Insert������insert����ֵ��hash�������������飬����undo��ʱ��ֻ��Ҫ�Ƴ�hash����
 * @param session �Ự
 * @param rid �����¼��rowId
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

/** undo�״θ��²���
 * @param session �Ự
 * @param rid ���¼�¼��rowId
 * return �״θ���ǰ��ȫ��
 */
void MRecords::undoFirUpdate(Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}

/** undo���״θ��²���
 * @param session �Ự
 * @param rid ���¼�¼��rowId
 * return ����ǰȫ��
 */
void MRecords::undoSecUpdate(Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}

/** undo�״�ɾ������
 * @param session �Ự
 * @param rid ɾ����¼��rowId
 * return ɾ��ǰ��ȫ��
 */
void MRecords::undoFirRemove(Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}

/** undo���״�ɾ������
 * @param session �Ự
 * @param rid ɾ����¼��rowId
 * return ɾ��ǰ��ȫ��
 */
void MRecords::undoSecRemove(ntse::Session *session, RowId rid, MHeapRec *rollBackRec) {
	rollBackRecord(session, rid, rollBackRec);
}



/** �ع�Ǯ��ø��º���
 * @param session �Ự
 * @param rid �ع���¼��rowId
 * return �ع�ǰ�ڴ��¼�ĺ��REC_REDUNDENT��ʽ
 */
MHeapRec *MRecords::getBeforeAndAfterImageForRollback(Session *session, RowId rid, Record **beforeImg, Record *afterImg) {
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp1 = ctx->setSavepoint();
	*beforeImg = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
	u64 sp2 = ctx->setSavepoint();
	MHeapRec *rollBackRec = NULL;
_ReStart:
	HashIndexEntry* entry = m_hashIndex->get(rid, ctx);
	//��ΪrollBack����϶���δ�ύ�������Բ����ܱ�purge��ȥ
	assert(entry != NULL);

	HashIndexType type = entry->m_type;
	if (type == HIT_TXNID) {
		ctx->resetToSavepoint(sp1);
		*beforeImg = NULL;
	} else {
		SYNCHERE(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
		//��ȡ��ǰ���ڴ�Ѽ�¼
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
			//���ݵ�ǰ�ڴ�Ѽ�¼���ع���rowid���лع���¼
			rollBackRec = m_versionPool->getRollBackHeapRec(session, *m_tableDef, heapRec->m_vTableIndex, &heapRec->m_rec, heapRec->m_rollBackId);
			assert(rollBackRec->m_rec.m_rowId == rid);
			if (rollBackRec->m_rollBackId == INVALID_ROW_ID) {
				*beforeImg = NULL;
			}
		}
	}
	return rollBackRec;
}

/** �ع���¼����
 * @param session �Ự
 * @param rid �ع���¼��rowId
 * return �ع�ǰ�ڴ��¼��ǰ��REC_REDUNDENT��ʽ
 */
void MRecords::rollBackRecord(Session *session, RowId rid, MHeapRec* rollBackRec) {
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	HashIndexEntry* entry = m_hashIndex->get(rid, ctx);
	//��ΪrollBack����϶���δ�ύ�������Բ����ܱ�purge��ȥ
	assert(entry != NULL);

	HashIndexType type = entry->m_type;
	if (type == HIT_TXNID) {
		m_hashIndex->remove(rid);
	} else {
		SYNCHERE(SP_MRECS_ROLLBACK_BEFORE_GETHEAPREC);
		//���ݵ�ǰ�ڴ�Ѽ�¼���ع���rowid���лع���¼
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