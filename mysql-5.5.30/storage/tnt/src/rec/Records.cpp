/**
 * ��¼����ʵ�֡�
 *
 * @author ��Դ��wy@163.org, wangyuan@corp.netease.com��
 */

#include "rec/Records.h"
#include "api/Database.h"

using namespace ntse;

namespace ntse {


/////////////////////////////////////////////////////////////////////////////
// RSModInfo & RSUpdateInfo /////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/** ����һ��RSModInfo����
 *
 * @param session �Ự
 * @param records ��¼������
 * @param updCols �����µ����ԣ���ʱʹ�ã��������
 * @param readCols ɨ��ʱ�Ѿ���ȡ�����ԣ�ֱ�����ã��������
 * @param extraMissCols ����֮ǰ����Ҫ��ȡ�����ԣ����������ֱ������
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


/** ����һ��TSUpdateInfo����
 *
 * @param session �Ự
 * @param records ��¼������
 * @param updCols �����µ����ԣ�ֱ�����ã����������������
 * @param readCols ɨ��ʱ�Ѿ���ȡ�����ԣ��������
 * @param extraMissCols ����ǰ����Ҫ��ȡ�����ԣ�ֱ�����ã��������
 */
RSUpdateInfo::RSUpdateInfo(Session *session, Records *records, const ColList &updCols, const ColList &readCols, const ColList &extraMissCols)
	:RSModInfo(session, records, updCols, readCols, extraMissCols),
	m_updateMysql(REC_MYSQL, updCols.m_size, updCols.m_cols, NULL, m_tableDef->m_maxRecSize),
	m_updateRed(REC_REDUNDANT, updCols.m_size, updCols.m_cols, NULL, m_tableDef->m_maxRecSize) {
	m_updateCols = updCols;
	m_updLob = m_updLobCols.m_size > 0;
	m_updCached = !m_updLob && m_tableDef->isUpdateCached(updCols.m_size, updCols.m_cols);
	// ֻҪ��¼����󳤶ȿ��ܳ��������¼����ܵ��¼�¼��������ʹֻ�����˶����ֶΣ���Ϊ����NULL���³ɷ�NULL���䳤
	m_couldTooLong = m_tableDef->m_maxRecSize > Limits::MAX_REC_SIZE;

	MemoryContext *mc = session->getMemoryContext();
	new (&m_updateRed)SubRecord(REC_REDUNDANT, updCols.m_size, updCols.m_cols, NULL, m_tableDef->m_maxRecSize);
	m_updateRed.m_data = m_updLob? (byte *)mc->alloc(m_tableDef->m_maxRecSize): NULL;
}

/** ׼������һ����¼
 * @post updateMysql/updateRe�������˲�������������
 *
 * @param rid ��¼RID
 * @param updateMysql MySQL��ʽ�ĸ�������
 */
void RSUpdateInfo::prepareForUpdate(RowId rid, const byte *updateMysql) {
	m_updateMysql.m_rowId = m_updateRed.m_rowId = rid;
	m_updateMysql.m_data = (byte *)updateMysql;
	m_updateRed.m_rowId = rid;
	if (!m_updLob) {
		m_updateRed.m_data = (byte *)updateMysql;
	} else {
		// TODO ֻ���������µ�����
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

/** ����UPDATE��Ϣ
 *
 * @param updCols Ҫ���µ����ԣ����������ֱ�����ã�������
 * @param extraMissCols ����֮ǰ��Ҫ�����ȡ�����ԣ����������ֱ�����ã�������
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

/** ����DELETE��Ϣ
 *
 * @param extraMissCols ����֮ǰ��Ҫ�����ȡ�����ԣ����������ֱ�����ã�������
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

/** �ͷ���һ��ռ�õ���Դ
 * @param retainLobs �Ƿ��������
 */
void Records::BulkOperation::releaseLastRow(bool retainLobs) {
	UNREFERENCED_PARAMETER(retainLobs);
	if (m_mmsRec) {
		m_records->m_mms->unpinRecord(m_session, m_mmsRec);
		m_mmsRec = NULL;
	}
}

/** �Ӷ��ж�ȡ��¼�����뵽MMS��
 * @param rid ��¼RID
 * @param lockMode Ҫ�ӵ�����������ΪNone
 * @param rlh �������
 * @return ��¼�Ƿ����
 */
bool Records::BulkOperation::fillMms(RowId rid, LockMode lockMode, RowLockHandle **rlh) {
	bool exist = m_records->m_heap->getRecord(m_session, rid, m_heapRec, lockMode, rlh);
	if (!exist)
		return false;
	assert(m_session->isRowLocked(m_tableDef->m_id, rid, m_rowLock));
	m_mmsRec = m_records->m_mms->putIfNotExist(m_session, m_heapRec);
	return true;
}

/** ׼������һ����¼
 * @param redSr Ҫ���µļ�¼��ǰ��REC_REDUNDANT��ʽ
 * @param mysqlSr Ҫ���µļ�¼��ǰ��REC_MYSQL��ʽ
 * @param updateMysql ���º���
 * @throw NtseException ��¼����
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
			//������к���ѹ���ֵ䣬���Ҹ����漰������ֶΣ����ܽ���MMS����
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
		// ������ܽ���MMS���£��򽫼�¼��MMS��ɾ��������Ϊ�˱�֤��MMS update�����в������TXN_UPDATE_HEAPд����
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
 * ���Ը��²�ѹ����¼
 *
 * ���γ���ѹ����Ľ���ᱻ���棬�������и��µ�ʱ���ٽ��еڶ��γ���ѹ��
 * ���ϲ��Լ�¼�ӻ��������������������ᵼ�²�һ�£����º��������ѹ����ʽ��Ҳ�����Ǳ䳤��ʽ��;
 *
 * @pre ���п��õ�ȫ���ֵ�
 * @param oldRcd ���µ�ǰ�����е����Զ����뱻��䣨������ѹ���ģ�Ҳ�����Ǳ䳤��)
 * @param update ���º��������ʽ
 * @param uncompressSize ��ѹ���ĳ���
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

/** ׼��ɾ��һ����¼
 * @param redSr REC_REDUNDANT��ʽ�ĵ�ǰ��¼������m_initRead�е�����
 */
void Records::BulkOperation::prepareForDelete(const SubRecord *redSr) {
	prepareForMod(redSr, m_delInfo, OP_DELETE);
	if (m_mmsRec) {
		m_records->m_mms->del(m_session, m_mmsRec);
		m_mmsRec = NULL;
	}
}

void Records::BulkOperation::deleteRow() {
//TNT�еĴ���󵥶���������ɾ��
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

/** ����REC_MYSQL��ʽ�����¼
 * @param buf �洢�����¼���ڴ�
 */
void Records::BulkFetch::setMysqlRow(byte *row) {
	m_mysqlRow.m_data = row;
	if (!m_readLob)
		m_redRow.m_data = row;
}

/** ��ȡ��һ����¼
 * @pre ��rowLockΪNone���ϲ�Ҫ��֤��¼�Ѿ�������
 *
 * @param rid Ҫ��ȡ�ļ�¼��RID
 * @param mysqlRow OUT���洢��¼���ݣ��洢ΪREC_MYSQL��ʽ
 * @param lockMode Ҫ�ӵļ�¼������ΪNULL
 * @param rlh ��¼�����ָ�룬��beginBulkFetchʱָ����ģʽΪNone���ΪNULL
 */
bool Records::BulkFetch::getNext(RowId rid, byte *mysqlRow, LockMode lockMode, RowLockHandle **rlh) {
	McSavepoint savepoint(m_session->getMemoryContext());
	
	// �����Ҫ�����������ڷ���MMSǰ�ȼ��� , ��JIRAƽ̨NTSETNT-253��BUG
	// �������ȼ��MMS���ټ��Heap���ܱ�֤��ȡ���ݵ�һ���ԣ�����Ҫ�ٶ�Heap��double check
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

/** ר���ṩ��TNT GetNext�����ĺ���������ɨ��/Positionɨ�裬��ȡ������NTSE��¼��
* @param	rid				ָ����ȡ��RowId
* @param	fullRow			������¼��ȡ����λ��
* @param	recExtractor	������¼����ȡ��
* @return	�ҵ�����true��δ�ҵ�����false
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

/** ���MMS���Ƿ����°汾 */
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


/** ���DRS���Ƿ����°汾 */
void Records::BulkFetch::checkDrsForNewer() {
	// ����֮ǰ����ȷ������m_redRow������
	assert(m_tableDef->m_useMms);
	RowId rid = m_redRow.m_rowId;
	m_records->m_heap->getSubRecord(m_session, rid, m_srExtractor, &m_redRow);
}

/** ��ɶ�ȡ��¼��m_currentRowIn֮��Ĳ��� */
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

/** ��λ��ɨ�����һ����¼��
 * @post currentRowIn��currentRowOut�ֱ𱣴���REC_REDUNDANT��REC_MYSQL��ʽ�ļ�¼����
 * @post ��ָ�������������¼�Ѿ���������
 *
 * @param mysqlRow OUT���洢��¼���ݣ��洢ΪREC_MYSQL��ʽ
 * @return �Ƿ�λ��һ����¼
 */
bool Records::Scan::getNext(byte *mysqlRow) {
	McSavepoint savepoint(m_session->getMemoryContext());
	
	setMysqlRow(mysqlRow);
	bool exist = m_records->m_heap->getNext(m_heapScan, &m_redRow);
	if (!exist)
		return false;
	// �������MMS�����ȼ��MMS���Ƿ��������ݣ����û���ٻ�HEAP��doubleCheck����JIRAƽ̨NTSETNT-251
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

/** ������¼������
 *
 * @param db ���ݿ�
 * @param path �ļ�·��������·��������������׺
 * @param tableDef ����
 * @throw NtseException �ļ�����ʧ�ܣ����岻����Ҫ���
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

/** ɾ����¼������
 * @param path �ļ�·��������·��������������׺
 * @throw NtseException �ļ�����ʧ�ܣ����岻����Ҫ���
 */
void Records::drop(const char *path) throw(NtseException) {
	DrsHeap::drop((string(path) + Limits::NAME_HEAP_EXT).c_str());
	LobStorage::drop(path);
	RowCompressMng::drop((string(path) + Limits::NAME_GLBL_DIC_EXT).c_str());
}

/** �򿪼�¼������
 * @param db ���ݿ�
 * @param session �Ự
 * @param path ��������׺��ȫ·��
 * @param hasCprsDic �Ƿ���ѹ���ֵ�
 * @throw NtseException �ļ������ڣ���ʽ����ȷ��
 */
Records* Records::open(Database *db, Session *session, const char *path, TableDef *tableDef, bool hasCprsDic) throw(NtseException) {
	Records *r = NULL;

	string heapPath = string(path) + Limits::NAME_HEAP_EXT;
	DrsHeap *heap = DrsHeap::open(db, session, heapPath.c_str(), tableDef);

	r = new Records(db, heap, tableDef);
	try {
		if (hasCprsDic && r->m_tableDef->m_rowCompressCfg) {//�������ȫ��ѹ���ֵ䣬�ȴ򿪼�¼ѹ������ģ��
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

/** �رռ�¼������
 * @param session �Ự
 * @param flushDirty �Ƿ�ˢ��������
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
 * �رռ�¼ѹ������ģ��
 * @pre ��¼ѹ��ģ��֮ǰ����
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

/** ˢ��������
 * @param session �Ự
 * @param flushHeap �Ƿ�ˢ��
 * @param flushMms �Ƿ�ˢMMS����ָ��ͨ��¼��MMS
 * @param flushLob �Ƿ�ˢ�������Ϊtrue��ҲˢС�ʹ�����MMS����ʹflushMmsΪfalse
 */
void Records::flush(Session *session, bool flushHeap, bool flushMms, bool flushLob) {
	if (flushHeap)
		m_heap->flush(session);
	if (m_mms && flushMms)
		m_mms->flush(session, true);
	if (m_lobStorage && flushLob)
		m_lobStorage->flush(session);
}

/** �޸ı�ID
 * @param session �Ự
 * @param tableId �±�ID
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
	// �޸�����
	for (u16 i = 0; i < numCols; ++i) {
		u16 cno = cols[i];
		m_tableDef->m_columns[cno]->m_cacheUpdate = cached;
	}
	//�ر�MMS�ٴ�
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

/** �õ���¼����ռ�õĴ��̿ռ�
 * @return ��¼����ռ�õĴ��̿ռ�
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

/** ��ü�¼������ص����ݿ����ͳ����Ϣ
 * @stats OUT����¼������ص����ݿ����ͳ����Ϣ���뵽�˶�̬������
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

/** ��ʼ��ɨ��
 *
 * @param session �Ự
 * @param opType ��������
 * @param readCols Ҫ��ȡ�����Ը���,����������ڴ�ʹ��Լ��: ����
 * @param externalLobMc �ⲿָ�����ڴ洢���صĴ���������ڴ���ڴ���������ģ���ΪNULL��ʹ��Session�Ĵ�����ڴ�������
 * @param lockMode �Է��صļ�¼��Ҫ�ӵ���ģʽ������ΪNone��������
 * @param rlh OUT�����ڴ洢��¼���������lockMode��None��ΪNULL
 * @param returnLinkSrc �Ա䳤��¼��ɨ��ʱ������������Դ��������Ŀ��ʱ���ؼ�¼������������Ŀ��
 *   ʱ���ؼ�¼���ܽϸߣ������ܳ���һ����¼�����ض�λ򲻻ᱻ�������⣬�����ڶ����ݾ�ȷ�Բ���
 *   ����
 * @return ��ɨ�����
 */
Records::Scan* Records::beginScan(Session *session, OpType opType, const ColList &readCols, MemoryContext *externalLobMc,
		LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc) {
	void *p = session->getMemoryContext()->alloc(sizeof(Scan));
	Scan *scan = new (p)Scan(session, this, opType, readCols, externalLobMc, lockMode);
	scan->m_heapScan = m_heap->beginScan(session, scan->m_srExtractor, lockMode, rlh, returnLinkSrc && opType == OP_READ);
	return scan;
}

/** ��ʼ������ȡ��¼(�����ٸ���)����
 * @param session �Ự
 * @param opType ��������
 * @param readCols Ҫ��ȡ�����Ը���,����������ڴ�ʹ��Լ��: ����
 * @param externalLobMc �ⲿָ�����ڴ洢���صĴ���������ڴ���ڴ���������ģ���beginScan˵��
 * @param lockMode �Զ�ȡ�ļ�¼����ӵ���
 * @return ������ȡ����
 */
Records::BulkFetch* Records::beginBulkFetch(Session *session, OpType opType, const ColList &readCols, MemoryContext *externalLobMc, LockMode lockMode) {
	void *p = session->getMemoryContext()->alloc(sizeof(BulkFetch));
	BulkFetch *fetch = new (p)BulkFetch(session, this, false, opType, readCols, externalLobMc, lockMode);
	return fetch;
}

/** ��ʼ�����޸ļ�¼����
 * @param session �Ự
 * @param opType ��������
 * @param readCols Ҫ��ȡ�����Ը���,����������ڴ�ʹ��Լ��: ����
 * @return �������²���
 */
Records::BulkUpdate* Records::beginBulkUpdate(ntse::Session *session, ntse::OpType opType, const ntse::ColList &readCols) {
	void *p = session->getMemoryContext()->alloc(sizeof(BulkUpdate));
	BulkUpdate *update = new (p)BulkUpdate(session, this, opType, readCols);
	return update;
}

/** �ж϶�ȡָ������ʱ�Ƿ���Բ���������
 * @param readCols Ҫ��ȡ������
 */
bool Records::couldNoRowLockReading(const ColList &readCols) const {
	return !m_tableDef->hasLob(readCols.m_size, readCols.m_cols);
}

/** ׼�������¼
 * @param mysqlRec Ҫ����ļ�¼���ݣ�REC_MYSQL��ʽ
 * @throw NtseException ��¼����
 */
void Records::prepareForInsert(const Record *mysqlRec) throw (NtseException) {
	if (m_tableDef->m_maxRecSize > Limits::MAX_REC_SIZE) {
		assert(m_tableDef->m_recFormat == REC_VARLEN || m_tableDef->m_recFormat == REC_COMPRESSED);
		u16 varSize = RecordOper::getRecordSizeRV(m_tableDef, mysqlRec);
		if (varSize > Limits::MAX_REC_SIZE)
			NTSE_THROW(NTSE_EC_ROW_TOO_LONG, "Record too long %d, max is %d.", varSize, Limits::MAX_REC_SIZE);
	}
}

/** �����¼
 * @param session �Ự
 * @param mysqlRec MySQL��ʽ�ļ�¼����
 * @return REC_REDUNDANT��ʽ�ļ�¼����
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

	McSavepoint mcp(mtx);//redRec����Ҫ���ظ��ϲ㣬���Է���redRec���ڴ滹�����ͷ�
	Record heapRec;
	heapRec.m_rowId = mysqlRec->m_rowId;

	if (m_tableDef->m_isCompressedTbl && hasValidDictionary()) {//�����ѹ�������ֵ�������ȳ��Խ���ѹ��
		byte *tmpBuf = (byte *)mtx->alloc(m_tableDef->m_maxRecSize);
		size_t *rcdSegs = (size_t *)mtx->alloc((m_tableDef->m_numColGrps + 1) * sizeof(size_t));
		CompressOrderRecord dummyCpsRcd(mysqlRec->m_rowId, tmpBuf, m_tableDef->m_maxRecSize, 
			m_tableDef->m_numColGrps, rcdSegs);
		RecordOper::convRecordRedToCO(m_tableDef, redRec, &dummyCpsRcd);

		//�п���ѹ����ļ�¼��Ȳ�ѹ�����������ǰ���Ŀǰ�ı��뷽ʽ���ᳬ����¼����С������
		uint maxCompressedSize = m_tableDef->m_maxRecSize << 1;
		heapRec.m_data = (byte *)mtx->alloc(maxCompressedSize);
		heapRec.m_size = maxCompressedSize;
		heapRec.m_format = REC_COMPRESSED;

		double realCprsRatio = 100 * m_rowCompressMng->compressRecord(&dummyCpsRcd, &heapRec);
		if (realCprsRatio > m_tableDef->m_rowCompressCfg->compressThreshold()) {//������ܴﵽ�����ѹ����, ���䳤��ʽ�洢
			heapRec.m_format = REC_VARLEN;
			heapRec.m_size = maxCompressedSize;
			RecordOper::convertRecordRV(m_tableDef, redRec, &heapRec);
		}
	} else {//�������ѹ�������ȫ���ֵ仹û�������򰴶�����䳤��ʽ�洢
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

/** ���˲���ļ�¼
 * @param session �Ự
 * @param redRec ��¼���ݣ�ΪREC_REDUNDANT��ʽ
 */
void Records::undoInsert(Session *session, const Record *redRec) {
	assert(redRec->m_format == REC_REDUNDANT);
	NTSE_ASSERT(m_heap->del(session, redRec->m_rowId));
	if (m_tableDef->hasLob())
		deleteLobs(session, redRec->m_data);
}

/** ���ָ����¼������һ���ԣ�����: ��¼�ڶ��д��ڣ�����MMS���ҷ�����MMS����м�¼����һ�£������õĴ����������ȷ��
 *
 * @param session �Ự
 * @param rid Ҫ���ļ�¼RID
 * @param expected ����ΪNULL������ΪNULL������ָ֤�����������ȣ�ΪREC_MYSQL��ʽ
 * @throw NtseException ���ݲ�һ��
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

	// �����Ƿ����ָ����¼
	NTSE_ASSERT(m_heap->getSubRecord(session, rid, srExtractor, &readSub, None, NULL));
	readRec.m_rowId = readSub.m_rowId;
	// MMS����Ƿ�һ��
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
	// ��ȡ����֤���������
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
	// �Ƿ��������ָ��������һ��
	if (expected) {
		SubRecord actual(REC_MYSQL, expected->m_numCols, expected->m_columns, readRec.m_data, m_tableDef->m_maxRecSize, readRec.m_rowId);
		if (!RecordOper::isSubRecordEq(m_tableDef, &actual, expected))
			NTSE_THROW(NTSE_EC_CORRUPTED, "Actual and expected inconsistent");
	}
}

/** ��ָ֤���ļ�¼�Ѿ���ɾ��
 *
 * @param session �Ự
 * @param rid ��¼RID
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

/** ��ȡָ���ļ�¼
 * @param session �Ự
 * @param rid ��¼RID
 * @param redSr OUT���洢�洢�ļ�¼����
 * @param extractor ������ȡ�Ӽ�¼����ȡ��������ΪNULL
 * @return �Ƿ�ɹ�
 */
bool Records::getSubRecord(ntse::Session *session, ntse::RowId rid, ntse::SubRecord *redSr, SubrecExtractor *extractor) {
	McSavepoint mcSave(session->getMemoryContext());

	if (!extractor) {
		if (hasValidDictionary()) {//�����ֵ����
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

/** ����rowId��ȡ��¼����¼�ĸ�ʽΪREC_FIXLEN����REC_VARLEN
 * @param rowId
 * @param record out ���ؼ�¼
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

/// �����
/*
������¼�洢��MySQL��NTSE��ռ�õĿռ���ͬ������ʽ��ͬ��
MySQL��ʽ�д����Ϊ2-3���ֽڵĴ�С��8�ֽڵ�ָ���������ݵ�ָ�룬
NTSE��ʽ�д����Ϊ2-3�ֽڵĴ�С��ͳһ��Ϊ0��ռλ����8�ֽڵĴ����ID��
*/

/** �����������ݣ�����¼ת��ΪNTSE�ڲ���ʽ
 *
 * @param session �Ự
 * @param mysqlRec ������������ݵģ���REC_MYSQL��ʽ�洢�ļ�¼
 * @param redRec OUT��REC_REDUNDANT��ʽ��ʾ�ļ�¼�����ڴ洢�����ID
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
			if (!lob)	// ��NULL����ֵ��''ʱ
				lob = (byte *)session->getMemoryContext()->alloc(1);
			uint lobSize = RecordOper::readLobSize(mysqlRec->m_data, columnDef);
			lobId = m_lobStorage->insert(session, lob, lobSize, m_tableDef->m_compressLobs);
			assert(lobId != INVALID_LOB_ID);
		}
		RecordOper::writeLobId(redRec->m_data, columnDef, lobId);
		RecordOper::writeLobSize(redRec->m_data, columnDef, 0);
	}
}

/** ɾ����¼���������д�����ڲ����¼ʧ�ܺ�ɾ����¼ʱ����
 * ע: ɾ��˳���ǵ��ŵ�
 *
 * @param session �Ự
 * @param redRow Ҫɾ���ļ�¼��REC_REDUNDANT��ʽ��һ���������д�������Ե�ֵ
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

/** ��ȡ��������ݣ����洢��MySQLҪ��ļ�¼�洢����
 *
 * @param session �Ự
 * @param ctx ���ڴ洢�����صĴ�������ݵ��ڴ����������
 * @param redSr NTSE�ڲ���ʽ��ʾ�Ĳ��ּ�¼
 * @param mysqlSr OUT�������¼���ݸ�MySQL
 * @param intoMms �Ƿ�ʹ��MMS����(С��)�����
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
 * ����ȷ�жϸ��º�����id�Ƿ�ᷢ���仯
 * @param old ԭ��¼��ΪREC_REDUNDANT��ʽ
 * @param mysqlSr Ҫ���µ����ԣ�ΪMySQL��ʽ
 * @param redSr �ڲ���ʽ��REC_REDUNDANT���洢�Ĵ��������
 * @return �����������ʱlobId�����仯���϶�����true��
 * �����������ʱlobIdû�з����仯�����ܷ���trueҲ���ܷ���false���ܴ����ѹ��Ӱ�죩
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
		if (!oldIsNull && !newIsNull) {	// ����֮ǰ��֮�󶼲���NULL
			uint lobSize = RecordOper::readLobSize(mysqlSr->m_data, columnDef);
			if (m_lobStorage->couldLobIdChanged(lobId, lobSize))
				return true;
		} else { // ��NULL����Ϊ��NULL���߽���NULL����ΪNULL
			return true;
		}			
	}//for
	return false;
}


/** ���´���󣬲�ת��Ϊ�ڲ��洢��ʽ
 *
 * @param session �Ự
 * @param old ԭ��¼��ΪREC_REDUNDANT��ʽ
 * @param mysqlSr Ҫ���µ����ԣ�ΪMySQL��ʽ
 * @param redSr OUT���ڲ���ʽ��REC_REDUNDANT���洢�Ĵ��������
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
			if (!newLob)	// MySQL�Է�NULL������Ϊ0�Ĵ�������NULLָ�룬Ϊ�򻯴���NTSEҲ�����ڴ�
				newLob = (byte *)session->getMemoryContext()->alloc(1);
		}
		LobId newLobId = INVALID_LOB_ID;
		
		if (!oldIsNull && !newIsNull) {	// ����֮ǰ��֮�󶼲���NULL
			uint lobSize = RecordOper::readLobSize(mysqlSr->m_data, columnDef);
			newLobId = m_lobStorage->update(session, lobId, newLob, lobSize, m_tableDef->m_compressLobs);
		} else if (oldIsNull && !newIsNull) {	// ��NULL����Ϊ��NULL
			uint lobSize = RecordOper::readLobSize(mysqlSr->m_data, columnDef);
			newLobId = m_lobStorage->insert(session, newLob, lobSize, m_tableDef->m_compressLobs);
		} else {	// ����NULL����ΪNULL
			assert(!oldIsNull && newIsNull);
			m_lobStorage->del(session, lobId);
		}
		assert(newIsNull || newLobId != INVALID_LOB_ID);

		RecordOper::writeLobId(redSr->m_data, columnDef, newLobId);
		RecordOper::writeLobSize(redSr->m_data, columnDef, 0);
	}//for
}

/** 
 * ���ü�¼����ģ���ȫ���ֵ�
 * @pre ������Ϊѹ����
 * @pre �ϲ㱣֤�Ѿ��Ա��X��
 * @param path          �������ֵ��ļ�·������������׺
 * @NtseException       ɾ����ȫ���ֵ��ļ�ʧ��
 */
void Records::resetCompressComponent(const char *path) throw(NtseException) {
	assert(m_tableDef->m_isCompressedTbl);
	assert(NULL == m_rowCompressMng);

	string basePath = string(path);
	string tmpDictFilePath = basePath + Limits::NAME_TEMP_GLBL_DIC_EXT;
	string newDictFilePath = basePath + Limits::NAME_GLBL_DIC_EXT;

	try {
		u64 errCode = File::copyFile(newDictFilePath.c_str(), tmpDictFilePath.c_str(), true);//���������ֵ��ļ�����ʱ�ֵ��ļ�ֻ�����޸���Database�Ŀ����ļ�֮��ɾ��
		if (errCode != File::E_NO_ERROR)
			NTSE_THROW(errCode, "Failed to copy temporary dictionary file.\n");

		m_rowCompressMng = RowCompressMng::open(m_db, m_tableDef, path);
		assert(m_rowCompressMng && m_rowCompressMng->getDictionary()) ;

		//��������heap��mms�е�ѹ����¼��ȡ��
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
 * ������ʱ�ֵ��ļ�
 * @param dicFullPath   ��ʱ�ֵ��ļ�·��������׺
 * @param tmpDict       ѹ���ֵ�
 * @throw NtseException �ļ���������
 */
void Records::createTmpDictFile(const char *dicFullPath, const RCDictionary *tmpDict) throw(NtseException) {
	try {
		RowCompressMng::create(dicFullPath, tmpDict);//����������ɹ�����֤�����������ļ�
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

