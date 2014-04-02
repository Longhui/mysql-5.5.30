#include "rec/MRecords.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include "misc/RecordHelper.h"
namespace tnt {
MRecords::MRecords(TableDef **tableDef, TNTIMPageManager *pageManager, HashIndex *hashIndex,
				   MHeap *heap, VersionPool *versionPool) {
	m_tableDef = tableDef;
	m_pageManager = pageManager;
	m_hashIndex = hashIndex;
	m_heap = heap;
	m_versionPool = versionPool;
	m_version = 1;
}

MRecords::~MRecords(void) {
	delete m_hashIndex;
	delete m_heap;
}

MRecords *MRecords::open(TableDef **tableDef, TNTIMPageManager *pageManager, VersionPool *versionPool) {
	HashIndex *hashIndex = new HashIndex(pageManager);
	MHeap *heap = new MHeap(tableDef, pageManager, hashIndex);

	return new MRecords(tableDef, pageManager, hashIndex, heap, versionPool);
}

void MRecords::close(Session *session, bool flush) {
	if (flush) {
		//TODO
		//purge(session, 0);
	}
	
	m_hashIndex->clear();
	m_heap->freeAllPage(session);
}

int MRecords::freeSomePage(Session *session, int target) {
	return m_heap->freeSomePage(session, target);
}

void MRecords::replaceComponents(TableDef **tableDef) {
	m_tableDef = tableDef;
	m_heap->replaceComponents(tableDef);
}

void MRecords::replaceHashIndex(HashIndex *hashIdx) {
	m_hashIndex = hashIdx;
	m_heap->m_hashIndexOperPolicy = hashIdx;
}

void MRecords::drop(Session *session) {
	m_hashIndex->clear();
	m_heap->freeAllPage(session);
}

RowIdVersion MRecords::getVersion() {
	return (RowIdVersion)m_version.get();
}

void MRecords::autoIncrementVersion() {
	if (m_version.get() == 0xFFFF) {
		m_version = 1;
		return;
	} else {
		m_version.increment();
	}
}

/** ��ʼpurge����
 * @param session �Ự
 * return purge��������
 */
MHeapScanContext *MRecords::beginPurgePhase1(Session *session) throw(NtseException) {
	autoIncrementVersion();
	m_heap->beginPurge();
	return m_heap->beginScan(session);
}

/** ����purge�����Ļ�ȡpurge����һ��ɼ���¼��
 * @param ctx purge������
 * @param readView �ɼ����ж�
 * return ����purge����һ���¼����true�����򷵻�false
 */
bool MRecords::purgeNext(MHeapScanContext *ctx, ReadView *readView, HeapRecStat *stat) {
	/*do {
		if (!m_heap->purgeNext(ctx, readView)) {
			return false;
		}

		if (!readView->isVisible(ctx->m_heapRec->m_txnId)) {
			ctx->m_heapRec = m_heap->getVersionHeapRedRecord(ctx->m_session, ctx->m_heapRec, readView, m_versionPool);
		}
	} while (!ctx->m_heapRec);*/
	bool visible = true;
	bool ret = m_heap->getNext(ctx, readView, &visible);
	if(ret && !visible) 
		ctx->m_heapRec = m_versionPool->getVersionRecord(ctx->m_session, *m_tableDef, ctx->m_heapRec, readView, &(ctx->m_heapRec->m_rec), stat);
	
	return ret;
}

/** ����purge����rowid������purge
 * @param ctx purge������
 * @param readView �ɼ����ж�
 * return ����purge����һ���¼����true�����򷵻�false
 */
bool MRecords::purgeCompensate(MHeapScanContext *ctx, ReadView *readView, HeapRecStat *stat) {
	RowId rid = INVALID_ROW_ID;
	Array<RowId> *compensateRows = m_heap->getCompensateRows();
	//�������rowidΪ��
	if (compensateRows->isEmpty()) {
		return false;
	}
	
	rid = compensateRows->last();
	compensateRows->pop();

	Session *session = ctx->m_session;
	MemoryContext *memCtx = session->getMemoryContext();
	Record *rec = new (memCtx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)memCtx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
	u64 sp = memCtx->setSavepoint();
_ReStart:
	HashIndexEntry *entry = m_hashIndex->get(rid, memCtx);
	NTSE_ASSERT(NULL != entry);
	//�����������£�Ȼ��delete���ٱ�purge��ȥ��Ȼ��insert����
	if (entry->m_type == HIT_TXNID) {
		ctx->m_heapRec = NULL;
		return true;
	}
	NTSE_ASSERT(entry->m_type == HIT_MHEAPREC);
	ctx->m_heapRec = m_heap->getHeapRedRecord(session, (void *)entry->m_value, rid, rec);
	if (ctx->m_heapRec == NULL) {
		memCtx->resetToSavepoint(sp);
		goto _ReStart;
	}

	if (!readView->isVisible(ctx->m_heapRec->m_txnId)) {
		ctx->m_heapRec = m_versionPool->getVersionRecord(ctx->m_session, *m_tableDef, ctx->m_heapRec, readView, &(ctx->m_heapRec->m_rec), stat);
	}

	return true;
}

/** ������һ�׶�purge
 * @param ctx purge������
 */
void MRecords::endPurgePhase1(MHeapScanContext *ctx) {
	m_heap->endScan(ctx);
	m_heap->finishPurgeCompensate();
}

size_t MRecords::getCompensateRowSize() {
	return m_heap->getCompensateRows()->getSize();
}

/** �ڴ�ѿ�ʼscan
 * @param session �Ự
 */
MHeapScanContext *MRecords::beginScan(Session *session) throw(NtseException) {
	return m_heap->beginScan(session);
}

/** ��ȡ�ڴ�ѵ���һ����¼
 * @param ctx scan������
 * return ���ڷ���true�����򷵻�false
 */
bool MRecords::getNext(MHeapScanContext *ctx, ReadView *readView) {
	return m_heap->getNext(ctx, readView);
}

void MRecords::endScan(MHeapScanContext *ctx) {
	return m_heap->endScan(ctx);
}

/** �����ڶ��׶�purge
 * @param session �Ự
 * @param readView �ɼ����ж�
 * return ��purge��ȥ�ļ�¼��
 */
u64 MRecords::purgePhase2(Session *session, ReadView *readView) {
	return m_heap->purgePhase2(session, readView);
}

/** ���ڴ�ѿɼ���¼(���ն��ڴ��)dump��ĳ���ļ������ڻָ�
 * @param readView  �汾�ɼ����жϵ�ʵ��
 * @param file dump���ļ����
 * @param offset in,out ��ǰ�ļ�ָ�������ļ���ƫ����������ǰ�������dump���ļ��󣬽��ļ�β��λ�Ƹ�ֵ��offset
 */
void MRecords::dump(Session *session, ReadView *readView, File *file, u64 *offset) throw (NtseException) {
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	try {
		m_heap->beginDump(session, file, offset);
		m_heap->dumpRealWork(session, readView, file, offset, m_versionPool);
		SYNCHERE(SP_MRECS_DUMP_AFTER_DUMPREALWORK);
	} catch (NtseException &e) {
		m_heap->resetStatus();
		m_heap->finishDumpCompensate();
		throw e;
	}

	m_heap->resetStatus();
	try {
		session->getTNTDb()->getTNTSyslog()->log(EL_LOG, "Need dump compensate row size is %d", m_heap->getCompensateRows()->getSize());
		m_heap->dumpCompensateRow(session, readView, file, offset, m_versionPool);
		m_heap->writeDumpEndPage(session, file, offset);
	} catch (NtseException &e) {
		m_heap->finishDumpCompensate();
		throw e;
	}
	
	m_heap->finishDumpCompensate();
}

/** ��dump�ļ��ж�ȡdumpҳ�������ڴ��
 * @param file dump�����ļ�
 * @param offset in,out ��ǰ�ļ�ָ���ƫ��������ȡ����ǰ��dump���ݽ���ʱ������ǰ��ƫ������ֵ��offset
 * return ���������������dump����ʱ������true�����򷵻�false
 */
bool MRecords::readDump(Session *session, File *file, u64 *offset) {
	RowIdVersion version = getVersion();
	u64 fileSize = 0;
	u64 code = file->getSize(&fileSize);
	if (code != File::E_NO_ERROR) {
		return false;
	}
	return m_heap->readDump(session, version, file, offset, fileSize);
}

/** ��ȡ��Ӧ��record��¼
 * @param scan ɨ���������ִ�п��ն�,readView����ΪNULL
 * @param version �����Ϊ0��˵����ͨ���ڴ���������λ��
 * @param ntseVisible out ����еļ�¼�Ե�ǰ�����Ƿ�ɼ�
 * return �ڴ��д��ڿɼ��汾������true�����򷵻�false
 */
bool MRecords::getRecord(TNTTblScan *scan, RowIdVersion version, bool *ntseVisible) {
	bool ret = true;
	HeapRecStat stat;
	RowId rowId = scan->getCurrentRid();
	ReadView *readView = scan->getMTransaction()->getReadView();
	MemoryContext *ctx = scan->getSession()->getMemoryContext();
	McSavepoint msp(ctx);
	MHeapRec *heapRec = NULL;
#ifndef NTSE_UNIT_TEST
	assert(scan->getSession()->isRowLocked((*m_tableDef)->m_id, rowId, Shared) || scan->getSession()->isRowLocked((*m_tableDef)->m_id, rowId, Exclusived));
#endif
	UNREFERENCED_PARAMETER(version);
	u64 sp1 = ctx->setSavepoint();
_ReStart:
	HashIndexEntry* entry = m_hashIndex->get(rowId, ctx);

	//����versionֵ�жϸ��������Ƿ�Ϊ������
	if (!entry/* || (version != INVALID_VERSION && entry->m_version != version)*/) {
		*ntseVisible = true;
		return false;
	}

	SYNCHERE(SP_MRECS_GETRECORD_PTR_MODIFY);

	u64 value = entry->m_value;
	HashIndexType type = entry->m_type;
	if (type == HIT_TXNID) {
		if (scan->getRowLockMode() == TL_NO && !readView->isVisible(value)) { //!readView || readView->isVisible(value)) {
			*ntseVisible = false;
		} else {
			*ntseVisible = true;
		}

		ret = false;
	} else {
		assert(HIT_MHEAPREC == type);
		void *ptr = (void *)value;
		heapRec = m_heap->getHeapRedRecord(scan->getSession(), ptr, rowId, scan->getRecord());
		if (heapRec == NULL) {
			ctx->resetToSavepoint(sp1);
			goto _ReStart;
		}

		if (scan->getRowLockMode() == TL_NO && !(readView->isVisible(heapRec->m_txnId))) {
			heapRec = m_versionPool->getVersionRecord(scan->getSession(), *m_tableDef, heapRec, readView, scan->getRecord(), &stat);
		} else {
			if (heapRec->m_del == FLAG_MHEAPREC_DEL) {
				stat = DELETED;
			} else {
				stat = VALID;
			}
		}

		if (stat == NTSE_VISIBLE) {
			*ntseVisible = true;
			ret = false;
		} else if (stat == DELETED || stat == NTSE_UNVISIBLE) {
			*ntseVisible = false;
			ret = false;
		} else {
			assert(stat == VALID);
			ret = true;
		}
	}
	return ret;
}

/** �����¼
 * @param session �Ự
 * @param txnId ����id
 * @param rowId ��¼rowId
 * return �����Ƿ�ɹ�
 */
bool MRecords::insert(Session *session, TrxId txnId, RowId rowId) {
	//����rowId�����ã������ڴ��¼��δ��ժ������purge��һ�׶���ɣ��ڶ��׶�δ��ɣ���ʱ�Ż᷵��true
	m_heap->removeHeapRecordAndHash(session, rowId);

	while(!m_hashIndex->insert(rowId, txnId, getVersion(), HIT_TXNID)) {
		session->getTNTDb()->freeMem(__FILE__, __LINE__);
		Thread::yield();
	}
	m_heap->m_stat.m_insert++;
	return true;
}

/** ���»�ɾ��ǰ��Ԥ��������Ƿ��һ�θ��»���ɾ����������ǣ�����Ҫ������ǰȫ��
 * @param scan ɨ��������Ҫ���scan��ptr��type��value��������ǵ�һ�θ��»���ɾ��������Ҫ������ȫ��
 * return ��һ�θ��»���ɾ������true�����򷵻�false
 */
bool MRecords::prepareUD(TNTTblScan *scan) {
	bool first = true;
	MemoryContext *ctx = scan->getSession()->getMemoryContext();
	McSavepoint msp(ctx);
_ReStart:
	HashIndexEntry *entry = m_hashIndex->get(scan->getCurrentRid(), ctx);
	if (!entry || entry->m_type == HIT_TXNID) {
		first = true;
		scan->setPtrType(false);
		if (!entry) {
			scan->setRowPtr(NULL);
			scan->setVersion(getVersion());
		} else {
			scan->setRowPtr(entry->m_value);
			scan->setVersion(entry->m_version);
		}
	} else {
		assert(entry->m_type == HIT_MHEAPREC);
		SYNCHERE(SP_MRECS_PREPAREUD_PTR_MODIFY);
		if (!m_heap->getHeapRedRecord(scan->getSession(), (void *)entry->m_value, scan->getCurrentRid(), scan->getRecord())) {
			goto _ReStart;
		}

		scan->setPtrType(true);
		scan->setRowPtr(entry->m_value);
		scan->setVersion(entry->m_version);
		first = false;
	}
	return first;
}

/** ���¼�¼��updateSubΪREC_REDUNDANT��ʽ
 * @param scan ɨ����
 * @param updateSub ���º���ΪREC_REDUNDANT��ʽ
 * ���³ɹ�����true�����򷵻�false
 */
bool MRecords::update(TNTTblScan *scan, SubRecord *updateSub) {
	assert(scan->getRecord()->m_format == REC_REDUNDANT && updateSub->m_format == REC_REDUNDANT);
	bool ret = true;
	HashIndexEntry *entry = NULL;
	TrxId txnId = scan->getMTransaction()->getTrxId();

	Session *session = scan->getSession();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	//TrxId prevTxnId = 0;
	SubRecord *preImage = NULL;
	RowId rollBackId = 0;
	RowId rid = scan->getCurrentRid();
	u8 vTableIndex = scan->getMTransaction()->getVersionPoolId();
	Record *rec = NULL;
	Record afterImg(rid, updateSub->m_format, updateSub->m_data, updateSub->m_size);

	bool ptrType = scan->getPtrType();
	u64 value = scan->getRowPtr();

	// �������undo��¼������汾��
	if (!ptrType) {
		SubRecord *fulRow= scan->getFullRow();
		if (value > 0) {
			preImage = new (ctx->alloc(sizeof(SubRecord))) SubRecord(fulRow->m_format, 0, NULL, NULL, 0, INVALID_ROW_ID);
			rollBackId = m_versionPool->insert(session, vTableIndex, *m_tableDef, INVALID_ROW_ID/**rollBackId*/, 0/**vTableIndex*/, (TrxId)value, preImage, 0/**delbit*/, txnId/*push TxnId*/);
		}
		rec = convertRVF(&afterImg, ctx);
	} else {
		SYNCHERE(SP_MRECS_UPDATE_PTR_MODIFY);
		void *ptr = (void *)value;
		//�����¼δ������Ļ���ptr��Ȼ��Ч��������һ��hash��������
		while (!m_heap->checkAndLatchPage(scan->getSession(), ptr, rid, Shared)) {
			u64 sp1 = ctx->setSavepoint();
			entry = m_hashIndex->get(rid, ctx);
			ptr = (void *)entry->m_value;
			ctx->resetToSavepoint(sp1);
		}

		if ((u64)ptr != scan->getRowPtr()) {
			scan->setRowPtr((u64)ptr);
		}

		//�����Ѿ��Ż���scan->getRecord()������Ȼ������Ч�ļ�¼
		MHeapRec *heapRec = m_heap->getHeapRecordSafe(scan->getSession(), ptr, rid);
		memcpy(&heapRec->m_rec, scan->getRecord(), sizeof(Record));
		m_heap->unLatchPageByPtr(session, ptr, Shared);

		preImage = new (ctx->alloc(sizeof(SubRecord))) SubRecord(heapRec->m_rec.m_format, updateSub->m_numCols, updateSub->m_columns, heapRec->m_rec.m_data, heapRec->m_rec.m_size, heapRec->m_rec.m_rowId);
		rollBackId = m_versionPool->insert(session, vTableIndex, *m_tableDef, heapRec->m_rollBackId, heapRec->m_vTableIndex, heapRec->m_txnId, preImage, heapRec->m_del, txnId/*push TxnId*/);
		
		rec = convertRVF(&afterImg, ctx);
	}

	// ��θ���TNTIM�ڴ�
	if(!updateMem(scan, rec, txnId, rollBackId, vTableIndex, 0))
		return false;

	// ���дredo��־,����δ���������񲻻ᱻpurge��ȥ��������־�ڲ������ǰ�ȫ��
	if(!ptrType) {
		writeFirUpdateLog(scan->getSession(), txnId, scan->getMTransaction()->getTrxLastLsn(), rid, rollBackId, vTableIndex, updateSub);
		m_heap->m_stat.m_update_first++;
	} else {
		writeSecUpdateLog(session, txnId, scan->getMTransaction()->getTrxLastLsn(), rid, rollBackId, vTableIndex, updateSub);
		m_heap->m_stat.m_update_second++;
	}

	return ret;
}

/** ���ݸ��º���Ͳ���汾�ص���Ϣ�����ڴ�Ѽ�¼
 * @param scan ɨ����
 * @param rec ���º���,ΪREC_VARLEN����REC_FIXLEN
 * @param txnId ����id
 * @param rollBackId �ع���¼rowId
 * @param tableIndex �ع���¼���ڰ汾�ص����
 * @param delBit ɾ����ʶλ
 * return ���³ɹ�����true�����򷵻�false
 */
bool MRecords::updateMem(TNTTblScan *scan, Record *rec, TrxId txnId, RowId rollBackId, u8 vTableIndex, u8 delBit) {
	bool ret = true;
	assert(rec->m_rowId == scan->getCurrentRid());
	assert(rec->m_format == REC_VARLEN || rec->m_format == REC_FIXLEN);
	Session *session = scan->getSession();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);

	bool ptrType = scan->getPtrType();
	u64 value = scan->getRowPtr();

	/*if (!ptrType && !value) {
		version = getVersion();
	}*/

	if (!ptrType) {
		ret = m_heap->insertHeapRecordAndHash(session, txnId, rollBackId, vTableIndex, rec, delBit, scan->getVersion());
	} else {
		//�����ڴ��
		MHeapRec *heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(txnId, rollBackId, vTableIndex, rec, delBit);
		/*HashIndexEntry *entry = NULL;
		SYNCHERE(SP_MREC_UPDATEMEM_PTR_MODIFY);
		//�����¼δ������Ļ���ptr��Ȼ��Ч��������һ��hash��������
		void *ptr = (void *)value;
		while (!m_heap->checkAndLatchPage(scan->getSession(), ptr, scan->getCurrentRid(), Exclusived)) {
			u64 sp1 = ctx->setSavepoint();
			entry = m_hashIndex->get(scan->getCurrentRid(), ctx);
			ptr = (void *)entry->m_value;
			ctx->resetToSavepoint(sp1);
		}

		ret = m_heap->updateHeapRecordAndHash(session, ptr, heapRec);
		assert(ret == true);
		m_heap->unLatchPageByPtr(scan->getSession(), ptr, Exclusived);*/
		SYNCHERE(SP_MRECS_UPDATEMEM_PTR_MODIFY);
		ret = m_heap->updateHeapRecordAndHash(session, scan->getCurrentRid(), (void *)value, heapRec);
	}
	return ret;
}

/** �ӵ�ǰ�ڴ����ɾ����¼
 *  ���ɾ������ִ�в��룬��purgeǰ����rowId���ᱻ���á����Ե����˰汾���в������delbitΪ1�İ汾��¼
 * @param scan ɨ����
 * return �Ƿ�ɾ���ɹ�
 */
bool MRecords::remove(TNTTblScan *scan) {
	bool ret = true;
	HashIndexEntry *entry = NULL;
	TrxId txnId = scan->getMTransaction()->getTrxId();
	RowId rid = scan->getCurrentRid();
	u8 vTableIndex = scan->getMTransaction()->getVersionPoolId();
	Session *session = scan->getSession();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	SubRecord *preImage = NULL;
	RowId rollBackId = 0;
	TrxId prevTxnId = 0;

	Record *rec = NULL;
	u64 value = scan->getRowPtr();
	bool heapPtr = scan->getPtrType();
	if (!heapPtr) {
		assert(scan->getRecord() != NULL);
		rec = convertRVF(scan->getRecord(), ctx);
		prevTxnId = value;
		if (prevTxnId > 0) {
			preImage = createEmptySubRecord(*m_tableDef, REC_REDUNDANT, rid, ctx);
			rollBackId = m_versionPool->insert(session, vTableIndex, *m_tableDef, INVALID_ROW_ID/**rollBackId*/, 0/**vTableIndex*/, prevTxnId, preImage, 0/**delbit*/, txnId/*push TxnId*/);
		}
		if (!m_heap->insertHeapRecordAndHash(session, txnId, rollBackId, vTableIndex, rec, 1/**delbit*/, scan->getVersion()))
			return false;
		//дlog��־
		writeFirRemoveLog(session, txnId, scan->getMTransaction()->getTrxLastLsn(), rid, rollBackId, vTableIndex);
		m_heap->m_stat.m_delete_first++;
		return ret;
	}

	void *ptr = (void *)value;
	//��ȡ��heapRec->m_recΪ�������߱䳤��ʽ
	MHeapRec *heapRec = NULL;
	SYNCHERE(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_VERSIONPOOL);
	while ((heapRec = m_heap->getHeapRecord(session, ptr, rid)) == NULL) {
		entry = m_hashIndex->get(rid, ctx);
		ptr = (void *)entry->m_value;
	}

	RecordOper::convertRecordVFR(*m_tableDef, &heapRec->m_rec, scan->getRecord());

	preImage = new (ctx->alloc(sizeof(SubRecord))) SubRecord(scan->getRecord()->m_format, 0, NULL, scan->getRecord()->m_data, scan->getRecord()->m_size, scan->getRecord()->m_rowId);
	rollBackId = m_versionPool->insert(session, vTableIndex, *m_tableDef, heapRec->m_rollBackId, heapRec->m_vTableIndex, heapRec->m_txnId, preImage, heapRec->m_del, txnId/*push TxnId*/);
	
	heapRec->m_txnId = txnId;
	heapRec->m_rollBackId = rollBackId;
	heapRec->m_vTableIndex = vTableIndex;
	heapRec->m_del = FLAG_MHEAPREC_DEL;

	SYNCHERE(SP_MRECS_REMOVE_PTR_MODIFY_BEFORE_MHEAP);
	//�����¼δ������Ļ���ptr��Ȼ��Ч��������һ��hash��������
	/*while (!m_heap->checkAndLatchPage(scan->getSession(), ptr, rid, Exclusived)) {
		u64 sp1 = ctx->setSavepoint();
		entry = m_hashIndex->get(rid, ctx);
		ptr = (void *)entry->m_value;
		ctx->resetToSavepoint(sp1);
	}

	ret = m_heap->updateHeapRecordAndHash(session, ptr, heapRec);
	m_heap->unLatchPageByPtr(scan->getSession(), ptr, Exclusived);*/
	if (!m_heap->updateHeapRecordAndHash(session, rid, ptr, heapRec))
		return false;

	//дlog��־
	writeSecRemoveLog(session, txnId, scan->getMTransaction()->getTrxLastLsn(), rid, rollBackId, vTableIndex);
	m_heap->m_stat.m_delete_second++;

	return ret;
}

SubRecord *MRecords::createEmptySubRecord(TableDef *tblDef, RecFormat format, RowId rid, MemoryContext *ctx) {
	SubRecord *subRecord = new (ctx->alloc(sizeof(SubRecord))) SubRecord(format, 0, NULL, (byte *)ctx->alloc(tblDef->m_maxRecSize), tblDef->m_maxRecSize, rid);

	return subRecord;
}

Record *MRecords::convertRVF(Record *redRec, MemoryContext *ctx) {
	assert(redRec->m_format == REC_REDUNDANT);
	Record *rec = NULL;
	if ((*m_tableDef)->m_recFormat == REC_VARLEN || (*m_tableDef)->m_recFormat == REC_COMPRESSED) {
		rec = new (ctx->alloc(sizeof(Record))) Record(redRec->m_rowId, REC_VARLEN, (byte *)ctx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
		RecordOper::convertRecordRV(*m_tableDef, redRec, rec);
	} else {
		assert((*m_tableDef)->m_recFormat == REC_FIXLEN);
		rec = new (ctx->alloc(sizeof(Record))) Record(redRec->m_rowId, REC_FIXLEN, redRec->m_data, redRec->m_size);
	}
	return rec;
}
}