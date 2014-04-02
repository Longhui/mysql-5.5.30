/**
 * 记录管理实现。
 *
 * @author 汪源（wy@163.org, wangyuan@corp.netease.com）
 */

#include "rec/Records.h"
#include "api/Database.h"

using namespace ntse;

namespace ntse {


/////////////////////////////////////////////////////////////////////////////
// RSModInfo & RSUpdateInfo /////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/** 构造一个RSModInfo对象
 *
 * @param session 会话
 * @param records 记录管理器
 * @param updCols 语句更新的属性，临时使用，必须递增
 * @param readCols 扫描时已经读取的属性，直接引用，必须递增
 * @param extraMissCols 更新之前额外要读取的属性，必须递增，直接引用
 */
RSModInfo::RSModInfo(Session *session, Records *records, const ColList &updCols, const ColList &readCols, const ColList &extraMissCols) {
	m_session = session;
	m_records = records;
	m_tableDef = records->m_tableDef;

	MemoryContext *mc = m_session->getMemoryContext();

	m_updLobCols.m_size = 0;
	m_updLobCols.m_cols = (u16 *)mc->alloc(m_tableDef->getNumLobColumns() * sizeof(u16));
	for (u16 i = 0; i < updCols.m_size; i++)
		if (m_tableDef->m_columns[updCols.m_cols[i]]->isLob())
			m_updLobCols.m_cols[m_updLobCols.m_size++] = updCols.m_cols[i];
	m_myMissCols = m_updLobCols.except(mc, readCols);
	m_extraMissCols = extraMissCols;
	m_allMissCols = m_myMissCols.merge(mc, extraMissCols);
	m_allRead = readCols.merge(mc, m_allMissCols);
	if (m_allMissCols.m_size) {
		void *p = mc->alloc(sizeof(SubRecord));
		m_missSr = new (p)SubRecord(REC_REDUNDANT, m_allMissCols.m_size, m_allMissCols.m_cols,
			(byte *)mc->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
		m_missExtractor = SubrecExtractor::createInst(m_session, m_tableDef, m_missSr, (uint) -1, m_records->m_rowCompressMng);
	} else {
		m_missExtractor = NULL;
		m_missSr = NULL;
	}
	new (&m_redRow)SubRecord(REC_REDUNDANT, m_allRead.m_size, m_allRead.m_cols, NULL, m_tableDef->m_maxRecSize);
}

void RSModInfo::setRow(RowId rid, const byte *redSr) {
	m_redRow.m_rowId = rid;
	m_redRow.m_data = (byte *)redSr;
}

void RSModInfo::readMissCols(MmsRecord *mmsRec) {
	if (m_allMissCols.m_size == 0)
		return;
	if (mmsRec)
		m_records->m_mms->getSubRecord(mmsRec, m_missExtractor, m_missSr);
	else {
		NTSE_ASSERT(m_records->m_heap->getSubRecord(m_session, m_redRow.m_rowId, m_missExtractor, m_missSr));
	}
	Record rec(m_redRow.m_rowId, REC_REDUNDANT, m_redRow.m_data, m_tableDef->m_maxRecSize);
	RecordOper::updateRecordRR(m_tableDef, &rec, m_missSr);
}


/** 构造一个TSUpdateInfo对象
 *
 * @param session 会话
 * @param records 记录管理器
 * @param updCols 语句更新的属性，直接引用，不拷贝，必须递增
 * @param readCols 扫描时已经读取的属性，必须递增
 * @param extraMissCols 更新前额外要读取的属性，直接引用，必须递增
 */
RSUpdateInfo::RSUpdateInfo(Session *session, Records *records, const ColList &updCols, const ColList &readCols, const ColList &extraMissCols)
	:RSModInfo(session, records, updCols, readCols, extraMissCols),
	m_updateMysql(REC_MYSQL, updCols.m_size, updCols.m_cols, NULL, m_tableDef->m_maxRecSize),
	m_updateRed(REC_REDUNDANT, updCols.m_size, updCols.m_cols, NULL, m_tableDef->m_maxRecSize) {
	m_updateCols = updCols;
	m_updLob = m_updLobCols.m_size > 0;
	m_updCached = !m_updLob && m_tableDef->isUpdateCached(updCols.m_size, updCols.m_cols);
	// 只要记录的最大长度可能超长，更新即可能导致记录超长，即使只更新了定长字段，因为可能NULL更新成非NULL而变长
	m_couldTooLong = m_tableDef->m_maxRecSize > Limits::MAX_REC_SIZE;

	MemoryContext *mc = session->getMemoryContext();
	new (&m_updateRed)SubRecord(REC_REDUNDANT, updCols.m_size, updCols.m_cols, NULL, m_tableDef->m_maxRecSize);
	m_updateRed.m_data = m_updLob? (byte *)mc->alloc(m_tableDef->m_maxRecSize): NULL;
}

/** 准备更新一条记录
 * @post updateMysql/updateRe都包含了参数给出的内容
 *
 * @param rid 记录RID
 * @param updateMysql MySQL格式的更新内容
 */
void RSUpdateInfo::prepareForUpdate(RowId rid, const byte *updateMysql) {
	m_updateMysql.m_rowId = m_updateRed.m_rowId = rid;
	m_updateMysql.m_data = (byte *)updateMysql;
	m_updateRed.m_rowId = rid;
	if (!m_updLob) {
		m_updateRed.m_data = (byte *)updateMysql;
	} else {
		// TODO 只拷贝被更新的属性
		memcpy(m_updateRed.m_data, updateMysql, m_tableDef->m_maxRecSize);
	}
	m_newRecSize = 0;
}

/////////////////////////////////////////////////////////////////////////////
// Various BulkOperations ///////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

Records::BulkOperation::BulkOperation(Session *session, Records *records, OpType opType, const ColList &readCols, LockMode lockMode, bool scan) {
	m_session = session;
	m_records = records;
	m_tableDef = records->m_tableDef;
	m_opType = opType;
	m_mmsRec = NULL;
	m_rowLock = lockMode;
	m_delInfo = NULL;
	m_updInfo = NULL;
	m_tryUpdateRecCache = NULL;

	MemoryContext *mc = session->getMemoryContext();
	m_initRead = readCols.copy(mc);

	m_shouldPutToMms = m_tableDef->m_useMms && !scan && m_opType != OP_DELETE && lockMode != None;
	if (m_shouldPutToMms) {
		void *p = mc->alloc(sizeof(Record));
		byte *data = (byte *)mc->alloc(m_tableDef->m_maxRecSize);
		m_heapRec = new (p)Record(INVALID_ROW_ID, m_tableDef->m_recFormat, data, m_tableDef->m_maxRecSize);
	} else
		m_heapRec = NULL;
}

/** 设置UPDATE信息
 *
 * @param updCols 要更新的属性，必须递增，直接引用，不拷贝
 * @param extraMissCols 更新之前需要额外读取的属性，必须递增，直接引用，不拷贝
 */
void Records::BulkOperation::setUpdateInfo(const ColList &updCols, const ColList &extraMissCols) {
	assert(updCols.isAsc() && extraMissCols.isAsc());
	assert(m_opType == OP_UPDATE || m_opType == OP_WRITE);
	assert(!m_updInfo);
	void *p = m_session->getMemoryContext()->calloc(sizeof(RSUpdateInfo));
	m_updInfo = new (p)RSUpdateInfo(m_session, m_records, updCols, m_initRead, extraMissCols);
}

ColList Records::BulkOperation::getUpdateColumns() const {
	assert(m_opType == OP_UPDATE || m_opType == OP_WRITE);
	return m_updInfo->m_updateCols;
}

/** 设置DELETE信息
 *
 * @param extraMissCols 更新之前需要额外读取的属性，必须递增，直接引用，不拷贝
 */
void Records::BulkOperation::setDeleteInfo(const ColList &extraMissCols) {
	assert(extraMissCols.isAsc());
	assert(m_opType == OP_DELETE || m_opType == OP_WRITE);
	assert(!m_delInfo);
	MemoryContext *mc = m_session->getMemoryContext();
	void *p = mc->alloc(sizeof(RSModInfo));
	m_delInfo = new (p)RSModInfo(m_session, m_records, ColList::generateAscColList(mc, 0, m_tableDef->m_numCols), m_initRead, extraMissCols);
}

void Records::BulkOperation::end() {
	releaseLastRow(true);
}

/** 释放上一行占用的资源
 * @param retainLobs 是否保留大对象
 */
void Records::BulkOperation::releaseLastRow(bool retainLobs) {
	UNREFERENCED_PARAMETER(retainLobs);
	if (m_mmsRec) {
		m_records->m_mms->unpinRecord(m_session, m_mmsRec);
		m_mmsRec = NULL;
	}
}

/** 从堆中读取记录并插入到MMS中
 * @param rid 记录RID
 * @param lockMode 要加的行锁，可能为None
 * @param rlh 行锁句柄
 * @return 记录是否存在
 */
bool Records::BulkOperation::fillMms(RowId rid, LockMode lockMode, RowLockHandle **rlh) {
	bool exist = m_records->m_heap->getRecord(m_session, rid, m_heapRec, lockMode, rlh);
	if (!exist)
		return false;
	assert(m_session->isRowLocked(m_tableDef->m_id, rid, m_rowLock));
	m_mmsRec = m_records->m_mms->putIfNotExist(m_session, m_heapRec);
	return true;
}

/** 准备更新一条记录
 * @param redSr 要更新的记录的前像，REC_REDUNDANT格式
 * @param mysqlSr 要更新的记录的前像，REC_MYSQL格式
 * @param updateMysql 更新后像
 * @throw NtseException 记录超长
 */
void Records::BulkOperation::prepareForUpdate(const SubRecord *redSr, const SubRecord *mysqlSr, const byte *updateMysql) throw(NtseException) {
	UNREFERENCED_PARAMETER(mysqlSr);
	prepareForMod(redSr, m_updInfo, OP_UPDATE);
	
	m_updInfo->prepareForUpdate(redSr->m_rowId, updateMysql);
	
	MemoryContext *mtx = m_session->getMemoryContext();
	bool canDoMmsUpdate = true;
	uint evaluateSize = 0;
	if (m_updInfo->m_couldTooLong) {
		assert(m_tableDef->m_recFormat == REC_VARLEN || m_tableDef->m_recFormat == REC_COMPRESSED);
		Record vlRec(INVALID_ROW_ID, REC_VARLEN, NULL, Limits::MAX_REC_SIZE);
		if (m_mmsRec) {
			m_records->m_mms->getRecord(m_mmsRec, &vlRec, false);
		} else {
			vlRec.m_data = (byte *)mtx->alloc(vlRec.m_size);
			m_records->m_heap->getRecord(m_session, redSr->m_rowId, &vlRec);
		}

		if (m_records->hasValidDictionary()) {
			//如果表中含有压缩字典，并且更新涉及大对象字段，则不能进行MMS更新
			if (!m_updInfo->m_updLob) {
				preUpdateRecWithDic(&vlRec, &m_updInfo->m_updateRed);
				evaluateSize = m_updInfo->m_newRecSize;
			} else {
				m_updInfo->m_newRecSize = RecordOper::getUpdateSizeNoCompress(mtx, m_tableDef, 
					m_records->m_rowCompressMng, &vlRec, &m_updInfo->m_updateRed);
				m_tryUpdateRecCache = NULL;
				canDoMmsUpdate = false;
			}
		} else {
			assert(vlRec.m_format == REC_VARLEN);
			m_updInfo->m_newRecSize = RecordOper::getUpdateSizeVR(m_tableDef, &vlRec, &m_updInfo->m_updateRed);
			evaluateSize = m_updInfo->m_newRecSize;
		}

		if (evaluateSize > Limits::MAX_REC_SIZE) {
			NTSE_THROW(NTSE_EC_ROW_TOO_LONG, "Record too long, %d, max is %d.", evaluateSize, Limits::MAX_REC_SIZE);
		}
	}

	if (m_tableDef->m_useMms && m_mmsRec) {
		// 如果不能进行MMS更新，则将记录从MMS中删除。这是为了保证在MMS update过程中不会产生TXN_UPDATE_HEAP写事务
		if (canDoMmsUpdate) {
			if (evaluateSize)
				canDoMmsUpdate = m_records->m_mms->canUpdate(m_mmsRec, (u16)evaluateSize);
			else {
				if (m_records->hasValidDictionary()) {
					assert(m_records->m_rowCompressMng);
					Record oldRec(INVALID_ROW_ID, REC_VARLEN, NULL, Limits::MAX_REC_SIZE);
					m_records->m_mms->getRecord(m_mmsRec, &oldRec, false);

					if (!m_updInfo->m_updLob) {
						preUpdateRecWithDic(&oldRec, &m_updInfo->m_updateRed);
						evaluateSize = m_updInfo->m_newRecSize;

						canDoMmsUpdate = m_records->m_mms->canUpdate(m_mmsRec, (u16)evaluateSize);
					} else {
						m_updInfo->m_newRecSize = RecordOper::getUpdateSizeNoCompress(mtx, m_tableDef, 
							m_records->m_rowCompressMng, &oldRec, &m_updInfo->m_updateRed);
						m_tryUpdateRecCache = NULL;
						canDoMmsUpdate = false;
					}					
				} else {
					canDoMmsUpdate = m_records->m_mms->canUpdate(m_mmsRec, &m_updInfo->m_updateRed, 
						&m_updInfo->m_newRecSize);
				}
			}
		}

		if (!canDoMmsUpdate) {
			m_records->m_mms->flushAndDel(m_session, m_mmsRec);
			m_mmsRec = NULL;
		}
	}
}

/**
 * 尝试更新并压缩记录
 *
 * 本次尝试压缩后的结果会被缓存，真正进行更新的时候不再进行第二次尝试压缩
 * 因上层会对记录加互斥锁，所以这样做不会导致不一致；更新后像可能是压缩格式的也可能是变长格式的;
 *
 * @pre 表含有可用的全局字典
 * @param oldRcd 更新的前像，所有的属性都必须被填充（可能是压缩的，也可能是变长的)
 * @param update 更新后像，冗余格式
 * @param uncompressSize 非压缩的长度
 */
void Records::BulkOperation::preUpdateRecWithDic(const Record *oldRec, const SubRecord *update) {
	MemoryContext *mtx = m_session->getMemoryContext();

	void *data = mtx->alloc(sizeof(Record));
	uint bufSize = m_tableDef->m_maxRecSize << 1;
	m_tryUpdateRecCache = new (data)Record(oldRec->m_rowId, REC_VARLEN, (byte *)mtx->alloc(bufSize), bufSize);

	RecordOper::updateRcdWithDic(m_session->getMemoryContext(), m_tableDef, 
		m_records->m_rowCompressMng, oldRec, update, m_tryUpdateRecCache);

	m_updInfo->m_newRecSize = (u16)m_tryUpdateRecCache->m_size;
}

void Records::BulkOperation::prepareForMod(const SubRecord *redSr, RSModInfo *modInfo, OpType modType) {
	modInfo->setRow(redSr->m_rowId, redSr->m_data);
	checkMmsForMod(modInfo, modType);	
	modInfo->readMissCols(m_mmsRec);
}

void Records::BulkOperation::checkMmsForMod(RSModInfo *modInfo, OpType modType) {
	assert(modInfo->m_redRow.m_rowId != INVALID_ROW_ID);
	if (m_tableDef->m_useMms && shouldCheckMmsBeforeUpdate()) {
		assert(!m_mmsRec);
		m_mmsRec = m_records->m_mms->getByRid(m_session, modInfo->m_redRow.m_rowId, modType != OP_DELETE, NULL, None);
		if (!m_mmsRec && m_shouldPutToMms && modType != OP_DELETE)
			fillMms(modInfo->m_redRow.m_rowId, None, NULL);
	}
}

bool Records::BulkOperation::tryMmsCachedUpdate() {
	if (!m_mmsRec || !m_updInfo->m_updCached)
		return false;
	assert(m_updInfo->m_newRecSize > 0 && !m_updInfo->m_updLob);
	u64 lsn = m_session->getLastLsn();
	m_records->m_mms->update(m_session, m_mmsRec, &m_updInfo->m_updateRed, m_updInfo->m_newRecSize, m_tryUpdateRecCache);
	m_mmsRec = NULL;
	m_tryUpdateRecCache = NULL;
	NTSE_ASSERT(m_session->getLastLsn() == lsn);
	return true;
}

void Records::BulkOperation::updateRow() {
	if (m_updInfo->m_updLob)
		m_records->updateLobs(m_session, &m_updInfo->m_redRow, &m_updInfo->m_updateMysql, &m_updInfo->m_updateRed);

	if (m_mmsRec) {
		assert(m_updInfo->m_newRecSize > 0);
		m_records->m_mms->update(m_session, m_mmsRec, &m_updInfo->m_updateRed, m_updInfo->m_newRecSize, m_tryUpdateRecCache);
		m_mmsRec = NULL;
		m_tryUpdateRecCache = NULL;
	} else {
		if (m_tryUpdateRecCache != NULL) {
			assert(m_tableDef->m_recFormat != REC_FIXLEN);
			if (getHeapScan()) {
				m_records->m_heap->updateCurrent(getHeapScan(), m_tryUpdateRecCache);
			} else {
				NTSE_ASSERT(m_records->m_heap->update(m_session, m_updInfo->m_updateRed.m_rowId, m_tryUpdateRecCache));
			}
			m_tryUpdateRecCache = NULL;
 		} else {
			if (getHeapScan()) {
				m_records->m_heap->updateCurrent(getHeapScan(), &m_updInfo->m_updateRed);
			} else {
				NTSE_ASSERT(m_records->m_heap->update(m_session, m_updInfo->m_updateRed.m_rowId, &m_updInfo->m_updateRed));
			}
 		}		
	}
}

/** 准备删除一条记录
 * @param redSr REC_REDUNDANT格式的当前记录，包含m_initRead中的属性
 */
void Records::BulkOperation::prepareForDelete(const SubRecord *redSr) {
	prepareForMod(redSr, m_delInfo, OP_DELETE);
	if (m_mmsRec) {
		m_records->m_mms->del(m_session, m_mmsRec);
		m_mmsRec = NULL;
	}
}

void Records::BulkOperation::deleteRow() {
//TNT中的大对象单独调用命令删除
#ifdef TNT_ENGINE
	if(m_session->getTrans() == NULL)
		if (m_tableDef->hasLob())
			m_records->deleteLobs(m_session, m_delInfo->m_redRow.m_data);
#endif
	if (getHeapScan()) {
		m_records->m_heap->deleteCurrent(getHeapScan());
	} else {
		NTSE_ASSERT(m_records->m_heap->del(m_session, m_delInfo->m_redRow.m_rowId));
	}
}

Records::BulkFetch::BulkFetch(Session *session, Records *records, bool scan, OpType opType,
	const ColList &readCols, MemoryContext *externalLobMc, LockMode lockMode):
	BulkOperation(session, records, opType, readCols, lockMode, scan) {
	m_lobMc = externalLobMc? externalLobMc : session->getLobContext();
	m_externalLobMc = externalLobMc != NULL;
	m_readMask = 0xFFFFFFFF;	// TODO should calc by readCols and missCols

	MemoryContext *mc = session->getMemoryContext();

	new (&m_redRow)SubRecord(REC_REDUNDANT, m_initRead.m_size, m_initRead.m_cols, NULL, m_tableDef->m_maxRecSize);
	new (&m_mysqlRow)SubRecord(REC_MYSQL, m_initRead.m_size, m_initRead.m_cols, NULL, m_tableDef->m_maxRecSize);
	if (m_initRead.m_size) {
		m_srExtractor = SubrecExtractor::createInst(session, m_tableDef, &m_redRow, (uint)-1, m_records->m_rowCompressMng);
	}

	m_readLob = m_tableDef->hasLob(readCols.m_size, readCols.m_cols);
	if (m_readLob)
		m_redRow.m_data = (byte *)mc->alloc(m_redRow.m_size);
}

/** 设置REC_MYSQL格式输出记录
 * @param buf 存储输出记录的内存
 */
void Records::BulkFetch::setMysqlRow(byte *row) {
	m_mysqlRow.m_data = row;
	if (!m_readLob)
		m_redRow.m_data = row;
}

/** 读取下一条记录
 * @pre 若rowLock为None则上层要保证记录已经加上锁
 *
 * @param rid 要读取的记录的RID
 * @param mysqlRow OUT，存储记录内容，存储为REC_MYSQL格式
 * @param lockMode 要加的记录锁，可为NULL
 * @param rlh 记录锁句柄指针，若beginBulkFetch时指定锁模式为None则可为NULL
 */
bool Records::BulkFetch::getNext(RowId rid, byte *mysqlRow, LockMode lockMode, RowLockHandle **rlh) {
	McSavepoint savepoint(m_session->getMemoryContext());
	
	// 如果需要加行锁，则在访问MMS前先加锁 , 见JIRA平台NTSETNT-253号BUG
	// 加锁后先检查MMS，再检查Heap，能保证读取数据的一致性，不需要再对Heap做double check
	if (lockMode != None)
		*rlh = LOCK_ROW(m_session, m_tableDef->m_id, rid, lockMode);
	setMysqlRow(mysqlRow);

	bool hitMms = false;
	if (m_tableDef->m_useMms) {
		if (m_opType == OP_READ && lockMode == None) {
			hitMms = m_records->m_mms->getSubRecord(m_session, rid, m_srExtractor, &m_redRow, m_opType != OP_DELETE, false, m_readMask);
		} else {
			m_mmsRec = m_records->m_mms->getByRid(m_session, rid, m_opType != OP_DELETE, NULL, None);
			if (m_mmsRec) {
				hitMms = true;
				m_records->m_mms->getSubRecord(m_mmsRec, m_srExtractor, &m_redRow);
			}
		}
		SYNCHERE(SP_REC_BF_AFTER_SEARCH_MMS);
	}
	if (!hitMms) {
		if (m_shouldPutToMms) {
			bool exist = fillMms(rid, None, NULL);
			if (!exist) {
				if (rlh) m_session->unlockRow(rlh);			
				return false;
			}
			m_srExtractor->extract(m_heapRec, &m_redRow);
		} else {
			bool exist = m_records->m_heap->getSubRecord(m_session, rid, m_srExtractor, &m_redRow, None, NULL);
			if (!exist) {
				if (rlh) m_session->unlockRow(rlh);			
				return false;
			}
		}
	}
#ifdef TNT_ENGINE
	if (m_session->getTrans() == NULL) {
#endif
		afterFetch();
#ifdef TNT_ENGINE
	}
#endif

	return true;
}

#ifdef TNT_ENGINE

/** 专门提供给TNT GetNext操作的函数，索引扫描/Position扫描，读取完整的NTSE记录；
* @param	rid				指定读取的RowId
* @param	fullRow			完整记录读取到的位置
* @param	recExtractor	完整记录的提取器
* @return	找到返回true；未找到返回false
*/
bool Records::BulkFetch::getFullRecbyRowId(RowId rid, SubRecord *fullRow, SubrecExtractor *recExtractor) {
	McSavepoint savepoint(m_session->getMemoryContext());

	bool hitMms = false;
	if (m_tableDef->m_useMms) {
		if (m_opType == OP_READ) {
			hitMms = m_records->m_mms->getSubRecord(m_session, rid, recExtractor, fullRow, true, false, m_readMask);
		} else {
			m_mmsRec = m_records->m_mms->getByRid(m_session, rid, true, NULL, None);
			if (m_mmsRec) {
				hitMms = true;
				m_records->m_mms->getSubRecord(m_mmsRec, recExtractor, fullRow);
			}
		}
		SYNCHERE(SP_REC_BF_AFTER_SEARCH_MMS);
	}
	if (!hitMms) {
		if (m_shouldPutToMms) {
			bool exist = fillMms(rid, None, NULL);
			if (!exist)
				return false;
			recExtractor->extract(m_heapRec, fullRow);
		} else {
			bool exist = m_records->m_heap->getSubRecord(m_session, rid, recExtractor, fullRow, None, NULL);
			if (!exist)
				return false;
		}
	}

	return true;
}

#endif 

/** 检查MMS中是否有新版本 */
bool Records::BulkFetch::checkMmsForNewer() {
	assert(m_tableDef->m_useMms && !m_mmsRec);
	RowId rid = m_redRow.m_rowId;
	MmsTable *mms = m_records->m_mms;
	bool exist = 0;
	if (m_opType == OP_READ) {
		exist = mms->getSubRecord(m_session, rid, m_srExtractor, &m_redRow, false, true, m_readMask);
	} else {
		m_mmsRec = mms->getByRid(m_session, rid, m_opType != OP_DELETE, NULL, None);
		if (m_mmsRec && mms->isDirty(m_mmsRec))	// TODO should check readMask
			mms->getSubRecord(m_mmsRec, m_srExtractor, &m_redRow);
		exist = (m_mmsRec != NULL);
	}
	return exist;
}


/** 检查DRS中是否有新版本 */
void Records::BulkFetch::checkDrsForNewer() {
	// 调用之前必须确保持有m_redRow的行锁
	assert(m_tableDef->m_useMms);
	RowId rid = m_redRow.m_rowId;
	m_records->m_heap->getSubRecord(m_session, rid, m_srExtractor, &m_redRow);
}

/** 完成读取记录在m_currentRowIn之后的操作 */
void Records::BulkFetch::afterFetch() {
	m_mysqlRow.m_rowId = m_redRow.m_rowId;
	if (m_readLob) {
		memcpy(m_mysqlRow.m_data, m_redRow.m_data, m_tableDef->m_maxRecSize);
		m_records->readLobs(m_session, m_lobMc, &m_redRow, &m_mysqlRow, m_shouldPutToMms);
	}
}

#ifdef TNT_ENGINE
void Records::BulkFetch::readLob(const SubRecord *redSr, SubRecord *mysqlSr) {	
	m_records->readLobs(m_session, m_lobMc, redSr, mysqlSr, m_shouldPutToMms);
}
#endif

void Records::BulkFetch::releaseLastRow(bool retainLobs) {
	BulkOperation::releaseLastRow(retainLobs);
	if (!retainLobs && !m_externalLobMc)
		m_lobMc->reset();
}

void Records::BulkFetch::prepareForUpdate(const SubRecord *redSr, const SubRecord *mysqlSr, const byte *updateMysql) throw(NtseException) {
	setMysqlRow(mysqlSr->m_data);
	BulkOperation::prepareForUpdate(redSr, mysqlSr, updateMysql);
}

Records::Scan::Scan(Session *session, Records *records, OpType opType, const ColList &readCols,
	MemoryContext *externalLobMc, LockMode lockMode):
	BulkFetch(session, records, true, opType, readCols, externalLobMc, lockMode) {
	m_heapScan = NULL;
}

/** 定位到扫描的下一条记录。
 * @post currentRowIn和currentRowOut分别保存了REC_REDUNDANT和REC_MYSQL格式的记录内容
 * @post 若指定加行锁，则记录已经加上行锁
 *
 * @param mysqlRow OUT，存储记录内容，存储为REC_MYSQL格式
 * @return 是否定位到一条记录
 */
bool Records::Scan::getNext(byte *mysqlRow) {
	McSavepoint savepoint(m_session->getMemoryContext());
	
	setMysqlRow(mysqlRow);
	bool exist = m_records->m_heap->getNext(m_heapScan, &m_redRow);
	if (!exist)
		return false;
	// 如果启用MMS，则先检测MMS中是否有新数据，如果没有再回HEAP做doubleCheck，见JIRA平台NTSETNT-251
	if (m_tableDef->m_useMms && !checkMmsForNewer()) {
		checkDrsForNewer();
	}

#ifdef TNT_ENGINE
	if (m_session->getTrans() == NULL) {
#endif
		afterFetch();
#ifdef TNT_ENGINE
	}
#endif

	return true;
}

void Records::Scan::end() {
	BulkFetch::end();
	m_records->m_heap->endScan(m_heapScan);
	m_heapScan = NULL;
}

Records::BulkUpdate::BulkUpdate(Session *session, Records *records, OpType opType, const ColList &readCols):
	BulkOperation(session, records, opType, readCols, Exclusived, false) {
}

/////////////////////////////////////////////////////////////////////////////
// Records //////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
Records::Records(Database *db, DrsHeap *heap, TableDef *tableDef) {
	m_db = db;
	m_heap = heap;
	m_mms = NULL;
	m_lobStorage = NULL;
	m_rowCompressMng = NULL;
	m_tableDef = tableDef;
}

/** 创建记录管理器
 *
 * @param db 数据库
 * @param path 文件路径，完整路径，但不包含后缀
 * @param tableDef 表定义
 * @throw NtseException 文件操作失败，表定义不符合要求等
 */
void Records::create(Database *db, const char *path, const TableDef *tableDef) throw (NtseException) {
	DrsHeap::create(db, (string(path) + Limits::NAME_HEAP_EXT).c_str(), tableDef);
	if (tableDef->hasLob()) {
		try {
			LobStorage::create(db, tableDef, path);
		} catch (NtseException &e) {
			DrsHeap::drop((string(path) + Limits::NAME_HEAP_EXT).c_str());
			throw e;
		}
	}
}

/** 删除记录管理器
 * @param path 文件路径，完整路径，但不包含后缀
 * @throw NtseException 文件操作失败，表定义不符合要求等
 */
void Records::drop(const char *path) throw(NtseException) {
	DrsHeap::drop((string(path) + Limits::NAME_HEAP_EXT).c_str());
	LobStorage::drop(path);
	RowCompressMng::drop((string(path) + Limits::NAME_GLBL_DIC_EXT).c_str());
}

/** 打开记录管理器
 * @param db 数据库
 * @param session 会话
 * @param path 不包含后缀的全路径
 * @param hasCprsDic 是否含有压缩字典
 * @throw NtseException 文件不存在，格式不正确等
 */
Records* Records::open(Database *db, Session *session, const char *path, TableDef *tableDef, bool hasCprsDic) throw(NtseException) {
	Records *r = NULL;

	string heapPath = string(path) + Limits::NAME_HEAP_EXT;
	DrsHeap *heap = DrsHeap::open(db, session, heapPath.c_str(), tableDef);

	r = new Records(db, heap, tableDef);
	try {
		if (hasCprsDic && r->m_tableDef->m_rowCompressCfg) {//如果表含有全局压缩字典，先打开记录压缩管理模块
			r->m_rowCompressMng = RowCompressMng::open(db, r->m_tableDef, path);
			assert(r->m_rowCompressMng != NULL);
			heap->setCompressRcdExtrator(r->m_rowCompressMng);
		} else {
			r->m_rowCompressMng = NULL;
		}

		if (r->m_tableDef->hasLob())
			r->m_lobStorage = LobStorage::open(db, session, r->m_tableDef, path, r->m_tableDef->m_useMms);
		if (r->m_tableDef->m_useMms) {
			r->openMms(session);
			if (hasCprsDic) {
				assert(r->m_mms != NULL);
				//assert(r->m_rowCompressMng != NULL);
				r->m_mms->setCprsRecordExtrator(r->m_rowCompressMng);
			}
		}
	} catch (NtseException &e) {
		r->close(session, false);
		delete r;
		throw e;
	}
	return r;
}

void Records::openMms(Session *session) {
	m_mms = new MmsTable(m_db->getMms(), m_db, m_heap, m_tableDef, m_db->getConfig()->m_enableMmsCacheUpdate && m_tableDef->m_cacheUpdate, m_tableDef->m_updateCacheTime);
	m_db->getMms()->registerMmsTable(session, m_mms);
}

/** 关闭记录管理器
 * @param session 会话
 * @param flushDirty 是否刷出脏数据
 */
void Records::close(Session *session, bool flushDirty) {
	if (NULL != m_rowCompressMng)
		closeRowCompressMng();

	if (m_lobStorage) {
		m_lobStorage->close(session, flushDirty);
		delete m_lobStorage;
		m_lobStorage = NULL;
	}
	if (m_mms)
		closeMms(session, flushDirty);
	if (m_heap) {
		m_heap->close(session, flushDirty);
		delete m_heap;
		m_heap = NULL;
	}
	m_tableDef = NULL;
}

/**
 * 关闭记录压缩管理模块
 * @pre 记录压缩模块之前被打开
 */
void Records::closeRowCompressMng() {
	assert(m_rowCompressMng);
	m_rowCompressMng->close();
	delete m_rowCompressMng;
	m_rowCompressMng = NULL;
}

void Records::closeMms(Session *session, bool flushDirty) {
	m_db->getMms()->unregisterMmsTable(session, m_mms);
	m_mms->close(session, flushDirty);
	delete m_mms;
	m_mms = NULL;
}

/** 刷出脏数据
 * @param session 会话
 * @param flushHeap 是否刷堆
 * @param flushMms 是否刷MMS，仅指普通记录的MMS
 * @param flushLob 是否刷大对象，若为true则也刷小型大对象的MMS，即使flushMms为false
 */
void Records::flush(Session *session, bool flushHeap, bool flushMms, bool flushLob) {
	if (flushHeap)
		m_heap->flush(session);
	if (m_mms && flushMms)
		m_mms->flush(session, true);
	if (m_lobStorage && flushLob)
		m_lobStorage->flush(session);
}

/** 修改表ID
 * @param session 会话
 * @param tableId 新表ID
 */
void Records::setTableId(Session *session, u16 tableId) {
	m_tableDef->m_id = tableId;
	if (m_lobStorage)
		m_lobStorage->setTableId(session, tableId);
	if (m_rowCompressMng)
		m_rowCompressMng->setTableId(tableId);
}

void Records::alterUseMms(Session *session, bool useMms) {
	assert (useMms != m_tableDef->m_useMms);
	m_tableDef->setUseMms(useMms);

	if (!useMms)
		closeMms(session, false);
	else {
		openMms(session);
		m_mms->setBinlogCallback(m_mmsCallback);
		if (hasValidDictionary())
			m_mms->setCprsRecordExtrator(m_rowCompressMng);
	}
	if (m_lobStorage)
		m_lobStorage->setMmsTable(session, useMms, false);
}

void Records::alterMmsCacheUpdate(bool cacheUpdate) {
	assert(cacheUpdate != m_tableDef->m_cacheUpdate && m_mms);
	m_mms->setCacheUpdate(cacheUpdate);
	m_tableDef->setCacheUpdate(cacheUpdate);
}

void Records::alterMmsUpdateCacheTime(u16 interval) {
	assert(m_mms && interval != m_tableDef->m_updateCacheTime);
	m_mms->setUpdateCacheTime(interval);
	m_tableDef->m_updateCacheTime = interval;
}

void Records::alterMmsCachedColumns(Session *session, u16 numCols, u16 *cols, bool cached) {
	assert(m_mms);
	// 修改配置
	for (u16 i = 0; i < numCols; ++i) {
		u16 cno = cols[i];
		m_tableDef->m_columns[cno]->m_cacheUpdate = cached;
	}
	//关闭MMS再打开
	closeMms(session, false);
	openMms(session);
	m_mms->setBinlogCallback(m_mmsCallback);
	if (hasValidDictionary())
		m_mms->setCprsRecordExtrator(m_rowCompressMng);
}

void Records::alterPctFree(Session *session, u8 pctFree) throw(NtseException) {
	m_heap->setPctFree(session, pctFree);
	assert(m_tableDef->m_pctFree == pctFree);
}

/** 得到记录数据占用的磁盘空间
 * @return 记录数据占用的磁盘空间
 */
u64 Records::getDataLength() {
	u64 size = m_heap->getUsedSize();
	if (m_lobStorage) {
		size += m_lobStorage->getStatus().m_blobStatus.m_datLength;
		size += m_lobStorage->getStatus().m_blobStatus.m_idxLength;
		size += m_lobStorage->getSLHeap()->getUsedSize();
	}
	return size;
}

/** 获得记录管理相关的数据库对象统计信息
 * @stats OUT，记录管理相关的数据库对象统计信息加入到此动态数组中
 */
void Records::getDBObjStats(Array<DBObjStats*>* stats) {
	if (m_heap)
		stats->push(m_heap->getDBObjStats());
	if (m_lobStorage) {
		stats->push(m_lobStorage->getSLHeap()->getDBObjStats());
		stats->push(m_lobStorage->getLLobDirStats());
		stats->push(m_lobStorage->getLLobDatStats());	
	}
}

/** 开始堆扫描
 *
 * @param session 会话
 * @param opType 操作类型
 * @param readCols 要读取的属性个数,必须递增。内存使用约定: 拷贝
 * @param externalLobMc 外部指定用于存储返回的大对象所用内存的内存分配上下文，若为NULL则使用Session的大对象内存上下文
 * @param lockMode 对返回的记录需要加的锁模式，可以为None即不加锁
 * @param rlh OUT，用于存储记录锁句柄，若lockMode则None则为NULL
 * @param returnLinkSrc 对变长记录，扫描时是在遇到链接源还是链接目的时返回记录。在遇到链接目的
 *   时返回记录性能较高，但可能出现一条记录被返回多次或不会被返回问题，适用于对数据精确性不高
 *   场合
 * @return 堆扫描操作
 */
Records::Scan* Records::beginScan(Session *session, OpType opType, const ColList &readCols, MemoryContext *externalLobMc,
		LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc) {
	void *p = session->getMemoryContext()->alloc(sizeof(Scan));
	Scan *scan = new (p)Scan(session, this, opType, readCols, externalLobMc, lockMode);
	scan->m_heapScan = m_heap->beginScan(session, scan->m_srExtractor, lockMode, rlh, returnLinkSrc && opType == OP_READ);
	return scan;
}

/** 开始批量读取记录(可能再更新)操作
 * @param session 会话
 * @param opType 操作类型
 * @param readCols 要读取的属性个数,必须递增。内存使用约定: 拷贝
 * @param externalLobMc 外部指定用于存储返回的大对象所用内存的内存分配上下文，见beginScan说明
 * @param lockMode 对读取的记录将会加的锁
 * @return 批量读取操作
 */
Records::BulkFetch* Records::beginBulkFetch(Session *session, OpType opType, const ColList &readCols, MemoryContext *externalLobMc, LockMode lockMode) {
	void *p = session->getMemoryContext()->alloc(sizeof(BulkFetch));
	BulkFetch *fetch = new (p)BulkFetch(session, this, false, opType, readCols, externalLobMc, lockMode);
	return fetch;
}

/** 开始批量修改记录操作
 * @param session 会话
 * @param opType 操作类型
 * @param readCols 要读取的属性个数,必须递增。内存使用约定: 拷贝
 * @return 批量更新操作
 */
Records::BulkUpdate* Records::beginBulkUpdate(ntse::Session *session, ntse::OpType opType, const ntse::ColList &readCols) {
	void *p = session->getMemoryContext()->alloc(sizeof(BulkUpdate));
	BulkUpdate *update = new (p)BulkUpdate(session, this, opType, readCols);
	return update;
}

/** 判断读取指定属性时是否可以不加行锁读
 * @param readCols 要读取的属性
 */
bool Records::couldNoRowLockReading(const ColList &readCols) const {
	return !m_tableDef->hasLob(readCols.m_size, readCols.m_cols);
}

/** 准备插入记录
 * @param mysqlRec 要插入的记录内容，REC_MYSQL格式
 * @throw NtseException 记录超长
 */
void Records::prepareForInsert(const Record *mysqlRec) throw (NtseException) {
	if (m_tableDef->m_maxRecSize > Limits::MAX_REC_SIZE) {
		assert(m_tableDef->m_recFormat == REC_VARLEN || m_tableDef->m_recFormat == REC_COMPRESSED);
		u16 varSize = RecordOper::getRecordSizeRV(m_tableDef, mysqlRec);
		if (varSize > Limits::MAX_REC_SIZE)
			NTSE_THROW(NTSE_EC_ROW_TOO_LONG, "Record too long %d, max is %d.", varSize, Limits::MAX_REC_SIZE);
	}
}

/** 插入记录
 * @param session 会话
 * @param mysqlRec MySQL格式的记录内容
 * @return REC_REDUNDANT格式的记录内容
 */
Record* Records::insert(ntse::Session *session, Record *mysqlRec, RowLockHandle **rlh) throw(NtseException) {
	assert(mysqlRec->m_format == REC_MYSQL);

	MemoryContext *mtx = session->getMemoryContext();

	Record *redRec = new (mtx->alloc(sizeof(Record)))Record(mysqlRec->m_rowId,
		REC_REDUNDANT, mysqlRec->m_data, m_tableDef->m_maxRecSize);
	if (m_tableDef->hasLob()) {
		redRec->m_data = (byte *)mtx->alloc(m_tableDef->m_maxRecSize);
		memcpy(redRec->m_data, mysqlRec->m_data, m_tableDef->m_maxRecSize);
		insertLobs(session, mysqlRec, redRec);
	}

	McSavepoint mcp(mtx);//redRec还需要返回给上层，所以分配redRec的内存还不能释放
	Record heapRec;
	heapRec.m_rowId = mysqlRec->m_rowId;

	if (m_tableDef->m_isCompressedTbl && hasValidDictionary()) {//如果是压缩表并且字典可用则先尝试进行压缩
		byte *tmpBuf = (byte *)mtx->alloc(m_tableDef->m_maxRecSize);
		size_t *rcdSegs = (size_t *)mtx->alloc((m_tableDef->m_numColGrps + 1) * sizeof(size_t));
		CompressOrderRecord dummyCpsRcd(mysqlRec->m_rowId, tmpBuf, m_tableDef->m_maxRecSize, 
			m_tableDef->m_numColGrps, rcdSegs);
		RecordOper::convRecordRedToCO(m_tableDef, redRec, &dummyCpsRcd);

		//有可能压缩后的记录会比不压缩更长，但是按照目前的编码方式不会超过记录最大大小的两倍
		uint maxCompressedSize = m_tableDef->m_maxRecSize << 1;
		heapRec.m_data = (byte *)mtx->alloc(maxCompressedSize);
		heapRec.m_size = maxCompressedSize;
		heapRec.m_format = REC_COMPRESSED;

		double realCprsRatio = 100 * m_rowCompressMng->compressRecord(&dummyCpsRcd, &heapRec);
		if (realCprsRatio > m_tableDef->m_rowCompressCfg->compressThreshold()) {//如果不能达到理想的压缩比, 按变长格式存储
			heapRec.m_format = REC_VARLEN;
			heapRec.m_size = maxCompressedSize;
			RecordOper::convertRecordRV(m_tableDef, redRec, &heapRec);
		}
	} else {//如果不是压缩表或者全局字典还没创建，则按定长或变长格式存储
		if (m_tableDef->m_recFormat == REC_FIXLEN) {
			heapRec.m_data = redRec->m_data;
			heapRec.m_size = redRec->m_size;
			heapRec.m_format = REC_FIXLEN;
		} else if (m_tableDef->m_recFormat == REC_VARLEN || m_tableDef->m_recFormat == REC_COMPRESSED) {
			heapRec.m_data = (byte *)mtx->alloc(m_tableDef->m_maxRecSize);
			heapRec.m_size = m_tableDef->m_maxRecSize;
			heapRec.m_format = REC_VARLEN;
			RecordOper::convertRecordRV(m_tableDef, redRec, &heapRec);
		} else 
			NTSE_ASSERT(false);
	}
	try {
		RowId rid = m_heap->insert(session, &heapRec, rlh);
		mysqlRec->m_rowId = redRec->m_rowId = rid;
	} catch(NtseException &e) {
		throw e;
	}
	
	return redRec;
}

/** 回退插入的记录
 * @param session 会话
 * @param redRec 记录内容，为REC_REDUNDANT格式
 */
void Records::undoInsert(Session *session, const Record *redRec) {
	assert(redRec->m_format == REC_REDUNDANT);
	NTSE_ASSERT(m_heap->del(session, redRec->m_rowId));
	if (m_tableDef->hasLob())
		deleteLobs(session, redRec->m_data);
}

/** 检查指定记录的数据一致性，包括: 记录在堆中存在；若在MMS中且非脏则MMS与堆中记录数据一致；各引用的大对象数据正确；
 *
 * @param session 会话
 * @param rid 要检查的记录RID
 * @param expected 可能为NULL，若不为NULL，则验证指定属性与此相等，为REC_MYSQL格式
 * @throw NtseException 数据不一致
 */
void Records::verifyRecord(Session *session, RowId rid, const SubRecord *expected) throw(NtseException) {
	assert(!expected || expected->m_format == REC_MYSQL);
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcSave(mc);
	McSavepoint mcSaveLob(session->getLobContext());

	ColList allCols = ColList::generateAscColList(mc, 0, m_tableDef->m_numCols);
	SubRecord readSub(REC_REDUNDANT, m_tableDef->m_numCols, allCols.m_cols, 
		(byte *)mc->calloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
	SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, m_tableDef, &readSub, (uint)-1, m_rowCompressMng);

	Record readRec(INVALID_ROW_ID, REC_REDUNDANT, readSub.m_data, readSub.m_size);

	// 堆中是否存在指定记录
	NTSE_ASSERT(m_heap->getSubRecord(session, rid, srExtractor, &readSub, None, NULL));
	readRec.m_rowId = readSub.m_rowId;
	// MMS与堆是否一致
	if (m_tableDef->m_useMms) {
		MmsRecord *mmsRec = m_mms->getByRid(session, rid, false, NULL, None);
		if (mmsRec) {
			SubRecord mmsSub(REC_REDUNDANT, m_tableDef->m_numCols, allCols.m_cols, 
				(byte *)mc->calloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
			m_mms->getSubRecord(mmsRec, srExtractor, &mmsSub);
			if (!m_mms->isDirty(mmsRec)) {
				if (!RecordOper::isSubRecordEq(m_tableDef, &mmsSub, &readSub)) {
					m_mms->unpinRecord(session, mmsRec);
					NTSE_THROW(NTSE_EC_CORRUPTED, "Heap and MMS inconsistent");
				}
			}
			m_mms->unpinRecord(session, mmsRec);
			readSub.m_data = readRec.m_data = mmsSub.m_data;
		}
	}
	// 读取并验证大对象数据
	if (m_tableDef->hasLob()) {
		for (u16 i = 0; i < m_tableDef->m_numCols; i++) {
			ColumnDef *columnDef = m_tableDef->m_columns[i];
			if (!columnDef->isLob())
				continue;
			uint lobSize = 0;
			byte *lob = NULL;
			if (!RecordOper::isNullR(m_tableDef, &readRec, i)) {
				LobId lid = RecordOper::readLobId(readSub.m_data, columnDef);
				lob = m_lobStorage->get(session, session->getLobContext(), lid, &lobSize, false);
				if (!lob)
					NTSE_THROW(NTSE_EC_CORRUPTED, "Referenced lob not found");
			}
			RecordOper::writeLobSize(readRec.m_data, columnDef, lobSize);
			RecordOper::writeLob(readRec.m_data, columnDef, lob);
		}
	}
	// 是否与参数中指定的数据一致
	if (expected) {
		SubRecord actual(REC_MYSQL, expected->m_numCols, expected->m_columns, readRec.m_data, m_tableDef->m_maxRecSize, readRec.m_rowId);
		if (!RecordOper::isSubRecordEq(m_tableDef, &actual, expected))
			NTSE_THROW(NTSE_EC_CORRUPTED, "Actual and expected inconsistent");
	}
}

/** 验证指定的记录已经被删除
 *
 * @param session 会话
 * @param rid 记录RID
 */
void Records::verifyDeleted(Session *session, RowId rid) {
	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcSave(mc);

	Record rec(rid, m_tableDef->m_recFormat, (byte *)mc->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
	NTSE_ASSERT(!m_heap->getRecord(session, rid, &rec, None, NULL));
	if (m_tableDef->m_useMms) {
		NTSE_ASSERT(!m_mms->getByRid(session, rid, false, NULL, None));
	}
}

/** 读取指定的记录
 * @param session 会话
 * @param rid 记录RID
 * @param redSr OUT，存储存储的记录内容
 * @param extractor 用于提取子记录的提取器，可以为NULL
 * @return 是否成功
 */
bool Records::getSubRecord(ntse::Session *session, ntse::RowId rid, ntse::SubRecord *redSr, SubrecExtractor *extractor) {
	McSavepoint mcSave(session->getMemoryContext());

	if (!extractor) {
		if (hasValidDictionary()) {//并且字典可用
			assert(m_rowCompressMng);
			extractor = SubrecExtractor::createInst(session, m_tableDef, redSr, (uint)-1, m_rowCompressMng);
		} else
			extractor = SubrecExtractor::createInst(session, m_tableDef, redSr);
	}
	if (m_tableDef->m_useMms) {
		if (m_mms->getSubRecord(session, rid, extractor, redSr, false, false, 0xFFFFFFFF))
			return true;
	}
	return m_heap->getSubRecord(session, rid, extractor, redSr, None, NULL);
}

/** 根据rowId获取记录，记录的格式为REC_FIXLEN或者REC_VARLEN
 * @param rowId
 * @param record out 返回记录
 * @param 
 */
bool Records::getRecord(Session *session, RowId rowId, Record *record, LockMode lockMode, RowLockHandle **rlh) {
	bool            hitMms = false;
	MmsRecord		*mmsRec = NULL;
	if (m_tableDef->m_useMms) {
		mmsRec = m_mms->getByRid(session, rowId, true, rlh, lockMode);
		if (mmsRec) {
			hitMms = true;
			m_mms->getRecord(mmsRec, record);
		}
	}

	if (!hitMms) {
		bool exist = m_heap->getRecord(session, rowId, record, lockMode, rlh);
		if (!exist)
			return false;
		m_mms->putIfNotExist(session, record);
	}

	return true;
}

/// 大对象
/*
大对象记录存储在MySQL和NTSE中占用的空间相同，但格式不同。
MySQL格式中大对象为2-3个字节的大小及8字节的指向大对象内容的指针，
NTSE格式中大对象为2-3字节的大小，统一设为0的占位符及8字节的大对象ID。
*/

/** 插入大对象数据，将记录转化为NTSE内部格式
 *
 * @param session 会话
 * @param mysqlRec 包含大对象数据的，以REC_MYSQL格式存储的记录
 * @param redRec OUT，REC_REDUNDANT格式表示的记录，用于存储大对象ID
 */
void Records::insertLobs(Session *session, const Record *mysqlRec, Record *redRec) {
	assert(mysqlRec->m_format == REC_MYSQL && redRec->m_format == REC_REDUNDANT);

	for (u16 col = 0; col < m_tableDef->m_numCols; col++) {
		ColumnDef *columnDef = m_tableDef->m_columns[col];
		if (!columnDef->isLob())
			continue;
		LobId lobId = INVALID_LOB_ID;
		if (!RecordOper::isNullR(m_tableDef, mysqlRec, col)) {
			byte *lob = RecordOper::readLob(mysqlRec->m_data, columnDef);
			if (!lob)	// 非NULL，但值是''时
				lob = (byte *)session->getMemoryContext()->alloc(1);
			uint lobSize = RecordOper::readLobSize(mysqlRec->m_data, columnDef);
			lobId = m_lobStorage->insert(session, lob, lobSize, m_tableDef->m_compressLobs);
			assert(lobId != INVALID_LOB_ID);
		}
		RecordOper::writeLobId(redRec->m_data, columnDef, lobId);
		RecordOper::writeLobSize(redRec->m_data, columnDef, 0);
	}
}

/** 删除记录包含的所有大对象。在插入记录失败和删除记录时调用
 * 注: 删除顺序是倒着的
 *
 * @param session 会话
 * @param redRow 要删除的记录，REC_REDUNDANT格式，一定包含所有大对象属性的值
 */
void Records::deleteLobs(Session *session, const byte *redRow) {
	Record rec(INVALID_ROW_ID, REC_REDUNDANT, (byte *)redRow, m_tableDef->m_maxRecSize);
	for (s16 col = m_tableDef->m_numCols - 1; col >= 0; col--) {
		ColumnDef *columnDef = m_tableDef->m_columns[col];
		if (!columnDef->isLob())
			continue;
		if (RecordOper::isNullR(m_tableDef, &rec, col))
			continue;
		LobId lobId = RecordOper::readLobId(redRow, columnDef);
		assert(lobId != INVALID_LOB_ID);
		m_lobStorage->del(session, lobId);
	}
}

/** 读取大对象内容，并存储到MySQL要求的记录存储区中
 *
 * @param session 会话
 * @param ctx 用于存储所返回的大对象内容的内存分配上下文
 * @param redSr NTSE内部格式表示的部分记录
 * @param mysqlSr OUT，输出记录内容给MySQL
 * @param intoMms 是否使用MMS缓存(小型)大对象
 */
void Records::readLobs(Session *session, MemoryContext *ctx, const SubRecord *redSr, SubRecord *mysqlSr, bool intoMms) {
	assert(redSr->m_format == REC_REDUNDANT);
	assert(mysqlSr->m_format == REC_MYSQL);

	for (u16 col = 0; col < redSr->m_numCols; col++) {
		u16 cno = redSr->m_columns[col];
		ColumnDef *columnDef = m_tableDef->m_columns[cno];
		if (!columnDef->isLob())
			continue;
		uint lobSize = 0;
		byte *lob = NULL;
		if (!RecordOper::isNullR(m_tableDef, redSr, cno)) {
			LobId lobId = RecordOper::readLobId(redSr->m_data, columnDef);
			assert(lobId != INVALID_LOB_ID);
			assert(RecordOper::readLobSize(redSr->m_data, columnDef) == 0);
			lob = m_lobStorage->get(session, ctx, lobId, &lobSize, intoMms);
			assert(lob);
		}
		RecordOper::writeLobSize(mysqlSr->m_data, columnDef, lobSize);
		RecordOper::writeLob(mysqlSr->m_data, columnDef, lob);
	}
}

/** 
 * 不精确判断更新后大对象id是否会发生变化
 * @param old 原记录，为REC_REDUNDANT格式
 * @param mysqlSr 要更新的属性，为MySQL格式
 * @param redSr 内部格式（REC_REDUNDANT）存储的大对象数据
 * @return 如果真正更新时lobId发生变化，肯定返回true，
 * 如果真正更新时lobId没有发生变化，可能返回true也可能返回false（受大对象压缩影响）
 */
bool Records::couldLobIdsChange(const SubRecord *old, const SubRecord *mysqlSr, SubRecord *redSr) {
	assert(old->m_format == REC_REDUNDANT || mysqlSr->m_format == REC_MYSQL || redSr->m_format == REC_REDUNDANT);

	for (u16 col = 0; col < mysqlSr->m_numCols; col++) {
		u16 cno = mysqlSr->m_columns[col];
		ColumnDef *columnDef = m_tableDef->m_columns[cno];
		if (!columnDef->isLob())
			continue;

		bool oldIsNull = RecordOper::isNullR(m_tableDef, old, cno);
		bool newIsNull = RecordOper::isNullR(m_tableDef, redSr, cno);
		if (oldIsNull && newIsNull)
			continue;
		LobId lobId = INVALID_LOB_ID;
		if (!oldIsNull) {
			lobId = RecordOper::readLobId(old->m_data, columnDef);
			assert(lobId != INVALID_LOB_ID);
		}
		if (!oldIsNull && !newIsNull) {	// 更新之前和之后都不是NULL
			uint lobSize = RecordOper::readLobSize(mysqlSr->m_data, columnDef);
			if (m_lobStorage->couldLobIdChanged(lobId, lobSize))
				return true;
		} else { // 将NULL更新为非NULL或者将非NULL更新为NULL
			return true;
		}			
	}//for
	return false;
}


/** 更新大对象，并转化为内部存储格式
 *
 * @param session 会话
 * @param old 原记录，为REC_REDUNDANT格式
 * @param mysqlSr 要更新的属性，为MySQL格式
 * @param redSr OUT，内部格式（REC_REDUNDANT）存储的大对象数据
 */
void Records::updateLobs(Session *session, const SubRecord *old, const SubRecord *mysqlSr, SubRecord *redSr) {
	assert(old->m_format == REC_REDUNDANT || mysqlSr->m_format == REC_MYSQL || redSr->m_format == REC_REDUNDANT);

	for (u16 col = 0; col < mysqlSr->m_numCols; col++) {
		u16 cno = mysqlSr->m_columns[col];
		ColumnDef *columnDef = m_tableDef->m_columns[cno];
		
		if (!columnDef->isLob())
			continue;
	
		bool oldIsNull = RecordOper::isNullR(m_tableDef, old, cno);
		bool newIsNull = RecordOper::isNullR(m_tableDef, redSr, cno);
		if (oldIsNull && newIsNull)
			continue;
		LobId lobId = INVALID_LOB_ID;
		if (!oldIsNull) {
			lobId = RecordOper::readLobId(old->m_data, columnDef);
			assert(lobId != INVALID_LOB_ID);
		}

		byte *newLob = NULL;
		if (!newIsNull) {
			newLob = RecordOper::readLob(mysqlSr->m_data, columnDef);
			if (!newLob)	// MySQL对非NULL但长度为0的大对象会用NULL指针，为简化处理，NTSE也分配内存
				newLob = (byte *)session->getMemoryContext()->alloc(1);
		}
		LobId newLobId = INVALID_LOB_ID;
		
		if (!oldIsNull && !newIsNull) {	// 更新之前和之后都不是NULL
			uint lobSize = RecordOper::readLobSize(mysqlSr->m_data, columnDef);
			newLobId = m_lobStorage->update(session, lobId, newLob, lobSize, m_tableDef->m_compressLobs);
		} else if (oldIsNull && !newIsNull) {	// 将NULL更新为非NULL
			uint lobSize = RecordOper::readLobSize(mysqlSr->m_data, columnDef);
			newLobId = m_lobStorage->insert(session, newLob, lobSize, m_tableDef->m_compressLobs);
		} else {	// 将非NULL更新为NULL
			assert(!oldIsNull && newIsNull);
			m_lobStorage->del(session, lobId);
		}
		assert(newIsNull || newLobId != INVALID_LOB_ID);

		RecordOper::writeLobId(redSr->m_data, columnDef, newLobId);
		RecordOper::writeLobSize(redSr->m_data, columnDef, 0);
	}//for
}

/** 
 * 设置记录管理模块的全局字典
 * @pre 表被定义为压缩表
 * @pre 上层保证已经对表加X锁
 * @param path          完整的字典文件路径，不包含后缀
 * @NtseException       删除旧全局字典文件失败
 */
void Records::resetCompressComponent(const char *path) throw(NtseException) {
	assert(m_tableDef->m_isCompressedTbl);
	assert(NULL == m_rowCompressMng);

	string basePath = string(path);
	string tmpDictFilePath = basePath + Limits::NAME_TEMP_GLBL_DIC_EXT;
	string newDictFilePath = basePath + Limits::NAME_GLBL_DIC_EXT;

	try {
		u64 errCode = File::copyFile(newDictFilePath.c_str(), tmpDictFilePath.c_str(), true);//拷贝出新字典文件，临时字典文件只能在修改完Database的控制文件之后删除
		if (errCode != File::E_NO_ERROR)
			NTSE_THROW(errCode, "Failed to copy temporary dictionary file.\n");

		m_rowCompressMng = RowCompressMng::open(m_db, m_tableDef, path);
		assert(m_rowCompressMng && m_rowCompressMng->getDictionary()) ;

		//重新设置heap和mms中的压缩记录提取器
		m_heap->setCompressRcdExtrator(m_rowCompressMng);
		if (m_tableDef->m_useMms)
			m_mms->setCprsRecordExtrator(m_rowCompressMng);
	} catch (NtseException &e) {
		if (NULL != m_rowCompressMng) {
			delete m_rowCompressMng;
			m_rowCompressMng = NULL;
		}
		throw e;
	}
}

/**
 * 创建临时字典文件
 * @param dicFullPath   临时字典文件路径，含后缀
 * @param tmpDict       压缩字典
 * @throw NtseException 文件操作出错
 */
void Records::createTmpDictFile(const char *dicFullPath, const RCDictionary *tmpDict) throw(NtseException) {
	try {
		RowCompressMng::create(dicFullPath, tmpDict);//如果创建不成功，则保证不会有垃圾文件
	} catch (NtseException &e) {
		m_db->getSyslog()->log(EL_ERROR, "Failed to create temporary dictionary file!");
		throw e;
	}
}

extern const char* getScanTypeStr(ScanType type) {
	switch (type) {
		case ST_TBL_SCAN:
			return "ST_TBL_SCAN";
		case ST_IDX_SCAN:
			return "ST_IDX_SCAN";
		default:
			assert(type == ST_POS_SCAN);
			return "ST_POS_SCAN";
	}
}

extern const char* getOpTypeStr(OpType type) {
	switch (type) {
		case OP_READ:
			return "OP_READ";
		case OP_UPDATE:
			return "OP_UPDATE";
		case OP_DELETE:
			return "OP_DELETE";
		default:
			assert(type == OP_WRITE);
			return "OP_WRITE";
	}
}

}

