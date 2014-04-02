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

/** 开始purge操作
 * @param session 会话
 * return purge的上下文
 */
MHeapScanContext *MRecords::beginPurgePhase1(Session *session) throw(NtseException) {
	autoIncrementVersion();
	m_heap->beginPurge();
	return m_heap->beginScan(session);
}

/** 根据purge上下文获取purge的下一项可见记录项
 * @param ctx purge上下文
 * @param readView 可见性判断
 * return 存在purge的下一项记录返回true，否则返回false
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

/** 根据purge补偿rowid集进行purge
 * @param ctx purge上下文
 * @param readView 可见性判断
 * return 存在purge的下一项记录返回true，否则返回false
 */
bool MRecords::purgeCompensate(MHeapScanContext *ctx, ReadView *readView, HeapRecStat *stat) {
	RowId rid = INVALID_ROW_ID;
	Array<RowId> *compensateRows = m_heap->getCompensateRows();
	//如果补偿rowid为空
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
	//发生增长更新，然后delete，再被purge出去，然后insert重用
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

/** 结束第一阶段purge
 * @param ctx purge上下文
 */
void MRecords::endPurgePhase1(MHeapScanContext *ctx) {
	m_heap->endScan(ctx);
	m_heap->finishPurgeCompensate();
}

size_t MRecords::getCompensateRowSize() {
	return m_heap->getCompensateRows()->getSize();
}

/** 内存堆开始scan
 * @param session 会话
 */
MHeapScanContext *MRecords::beginScan(Session *session) throw(NtseException) {
	return m_heap->beginScan(session);
}

/** 获取内存堆的下一个记录
 * @param ctx scan上下文
 * return 存在返回true，否则返回false
 */
bool MRecords::getNext(MHeapScanContext *ctx, ReadView *readView) {
	return m_heap->getNext(ctx, readView);
}

void MRecords::endScan(MHeapScanContext *ctx) {
	return m_heap->endScan(ctx);
}

/** 结束第二阶段purge
 * @param session 会话
 * @param readView 可见性判断
 * return 被purge出去的记录数
 */
u64 MRecords::purgePhase2(Session *session, ReadView *readView) {
	return m_heap->purgePhase2(session, readView);
}

/** 将内存堆可见记录(快照读内存堆)dump到某个文件中用于恢复
 * @param readView  版本可见性判断的实例
 * @param file dump的文件句柄
 * @param offset in,out 当前文件指针所在文件的偏移量。将当前表的数据dump到文件后，将文件尾的位移赋值给offset
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

/** 从dump文件中读取dump页，构建内存堆
 * @param file dump数据文件
 * @param offset in,out 当前文件指针的偏移量。读取到当前表dump数据结束时，将当前的偏移量赋值给offset
 * return 如果还存在另外表的dump数据时，返回true，否则返回false
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

/** 获取相应的record记录
 * @param scan 扫描句柄。如果执行快照读,readView不能为NULL
 * @param version 如果不为0，说明是通过内存索引来定位的
 * @param ntseVisible out 外存中的记录对当前事务是否可见
 * return 内存中存在可见版本，返回true，否则返回false
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

	//根据version值判断该索引项是否为重用项
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

/** 插入记录
 * @param session 会话
 * @param txnId 事务id
 * @param rowId 记录rowId
 * return 插入是否成功
 */
bool MRecords::insert(Session *session, TrxId txnId, RowId rowId) {
	//仅在rowId是重用，而且内存记录还未被摘除，即purge第一阶段完成，第二阶段未完成，此时才会返回true
	m_heap->removeHeapRecordAndHash(session, rowId);

	while(!m_hashIndex->insert(rowId, txnId, getVersion(), HIT_TXNID)) {
		session->getTNTDb()->freeMem(__FILE__, __LINE__);
		Thread::yield();
	}
	m_heap->m_stat.m_insert++;
	return true;
}

/** 更新或删除前的预处理。检查是否第一次更新或者删除，如果不是，则需要填充更新前全像
 * @param scan 扫描句柄，需要填充scan的ptr的type与value，如果不是第一次更新或者删除，还需要填充更新全像
 * return 第一次更新或者删除返回true，否则返回false
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

/** 更新记录。updateSub为REC_REDUNDANT格式
 * @param scan 扫描句柄
 * @param updateSub 更新后像，为REC_REDUNDANT格式
 * 更新成功返回true，否则返回false
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

	// 首先填充undo记录并插入版本池
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
		//如果记录未被整理的话，ptr仍然有效，可以少一次hash索引查找
		while (!m_heap->checkAndLatchPage(scan->getSession(), ptr, rid, Shared)) {
			u64 sp1 = ctx->setSavepoint();
			entry = m_hashIndex->get(rid, ctx);
			ptr = (void *)entry->m_value;
			ctx->resetToSavepoint(sp1);
		}

		if ((u64)ptr != scan->getRowPtr()) {
			scan->setRowPtr((u64)ptr);
		}

		//这里已经优化。scan->getRecord()可能仍然还是有效的记录
		MHeapRec *heapRec = m_heap->getHeapRecordSafe(scan->getSession(), ptr, rid);
		memcpy(&heapRec->m_rec, scan->getRecord(), sizeof(Record));
		m_heap->unLatchPageByPtr(session, ptr, Shared);

		preImage = new (ctx->alloc(sizeof(SubRecord))) SubRecord(heapRec->m_rec.m_format, updateSub->m_numCols, updateSub->m_columns, heapRec->m_rec.m_data, heapRec->m_rec.m_size, heapRec->m_rec.m_rowId);
		rollBackId = m_versionPool->insert(session, vTableIndex, *m_tableDef, heapRec->m_rollBackId, heapRec->m_vTableIndex, heapRec->m_txnId, preImage, heapRec->m_del, txnId/*push TxnId*/);
		
		rec = convertRVF(&afterImg, ctx);
	}

	// 其次更新TNTIM内存
	if(!updateMem(scan, rec, txnId, rollBackId, vTableIndex, 0))
		return false;

	// 最后写redo日志,由于未结束的事务不会被purge出去，所以日志在操作后是安全的
	if(!ptrType) {
		writeFirUpdateLog(scan->getSession(), txnId, scan->getMTransaction()->getTrxLastLsn(), rid, rollBackId, vTableIndex, updateSub);
		m_heap->m_stat.m_update_first++;
	} else {
		writeSecUpdateLog(session, txnId, scan->getMTransaction()->getTrxLastLsn(), rid, rollBackId, vTableIndex, updateSub);
		m_heap->m_stat.m_update_second++;
	}

	return ret;
}

/** 根据更新后像和插入版本池的信息更新内存堆记录
 * @param scan 扫描句柄
 * @param rec 更新后像,为REC_VARLEN或者REC_FIXLEN
 * @param txnId 事务id
 * @param rollBackId 回滚记录rowId
 * @param tableIndex 回滚记录所在版本池的序号
 * @param delBit 删除标识位
 * return 更新成功返回true，否则返回false
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
		//更新内存堆
		MHeapRec *heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(txnId, rollBackId, vTableIndex, rec, delBit);
		/*HashIndexEntry *entry = NULL;
		SYNCHERE(SP_MREC_UPDATEMEM_PTR_MODIFY);
		//如果记录未被整理的话，ptr仍然有效，可以少一次hash索引查找
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

/** 从当前内存堆中删除记录
 *  如果删除后再执行插入，在purge前，该rowId不会被重用。所以导致了版本池中不会出现delbit为1的版本记录
 * @param scan 扫描句柄
 * return 是否删除成功
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
		//写log日志
		writeFirRemoveLog(session, txnId, scan->getMTransaction()->getTrxLastLsn(), rid, rollBackId, vTableIndex);
		m_heap->m_stat.m_delete_first++;
		return ret;
	}

	void *ptr = (void *)value;
	//获取的heapRec->m_rec为定长或者变长格式
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
	//如果记录未被整理的话，ptr仍然有效，可以少一次hash索引查找
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

	//写log日志
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