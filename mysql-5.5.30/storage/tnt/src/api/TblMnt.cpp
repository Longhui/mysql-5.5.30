/**
 * ������ά������ʵ��
 *
 * @author л��(xieke@corp.netease.com, ken@163.org)
 */

#include "api/Table.h"
#include "api/Transaction.h"
#include "api/TblMnt.h"
#include "misc/ControlFile.h"
#include "misc/MemCtx.h"
#include "misc/Txnlog.h"
#include "misc/RecordHelper.h"
#include "heap/Heap.h"
#include "btree/Index.h"
#include "misc/Session.h"
#include <algorithm>
#include "util/File.h"
#include "compress/RCMSampleTbl.h"
#include "misc/Global.h"
#include "util/SmartPtr.h"
#include "trx/TNTTransaction.h"

using namespace tnt;
using namespace std;

namespace ntse {

/**
 * ������־����
 * @param memctx	�ڴ����������
 * @param logEntry	ԭʼ��־
 * @return			���ɵ���־�������ڴ�Ϊmemctx�з�������
 */
LogCopy* logCopy(ntse::MemoryContext *memctx, const LogEntry *logEntry) {
	LogCopy *p = (LogCopy *)memctx->alloc(sizeof(LogCopy));
	memcpy(&(p->m_logEntry), logEntry, sizeof(LogEntry));
	p->m_logEntry.m_data = (byte *)memctx->alloc(logEntry->m_size);
	memcpy(p->m_logEntry.m_data, logEntry->m_data, logEntry->m_size);
	p->m_next = NULL;
	return p;
}

/**
 * ���ݵ�һ����־��������һ��TxnLogList����
 * @param memctx       �ڴ�������
 * @param firstLog     ��һ����־����
 * @param txnId        ��־Id
 * @return             TxnLogList����ָ��
 */
TxnLogList* getTxnLogList(MemoryContext *memctx, LogCopy *firstLog, u16 txnId, LsnType startLsn) {
	assert(firstLog->m_logEntry.m_logType == LOG_TXN_START);
	TxnLogList *p = (TxnLogList *)memctx->alloc(sizeof(TxnLogList));
	p->m_txnId = txnId;
	p->m_type = Session::parseTxnStartLog(&(firstLog->m_logEntry));
	p->m_first = p->m_last = firstLog;
	p->m_valid = true;
	p->m_rowId = INVALID_ROW_ID;
	p->m_startLsn = startLsn;
	return p;
}

/** ��������־�б�������һ������
 *
 * @param list            ��־�������б�
 * @param logcopy         ��־����
 */
void appendCopy(TxnLogList *list, LogCopy *logcopy) {
	assert(list->m_txnId == logcopy->m_logEntry.m_txnId);
	list->m_last->m_next = logcopy;
	list->m_last = logcopy;
}

/** �ж�����TxnLogList��˳�����ݼ�¼rowid��С����
 * @param tl1           ������־�б�1
 * @param tl2           ������־�б�2
 * @return              tl1����tl2����true
 */
bool txnless(const TxnLogList *tl1, const TxnLogList *tl2) {
	return tl1->m_rowId < tl2->m_rowId;
}

/**
 * ����һ����־�ط���
 * @param table			�ط���־�ı�
 * @param db			���ݿ�
 * @param startLSN		�طŵ���ʼLSN
 * @param endLSN		�طŵ��ս�LSN
 * @param logBufSize	��¼��������С��Ĭ��Ϊ32M
 */
LogReplay::LogReplay(Table *table, Database *db, LsnType startLSN, LsnType endLSN, size_t logBufSize)
  : m_table(table), m_db(db), m_lsnStart(startLSN), m_lsnEnd(endLSN), m_bufSize(logBufSize),
  m_scanHdl(NULL), m_vecIdx(0) {
	m_memCtx = new MemoryContext(Limits::PAGE_SIZE * 128, 1);
	m_memCtxBak = new MemoryContext(Limits::PAGE_SIZE * 128, 1);
	m_txnList = new list<TxnLogList *>;
	m_bakList = new list<TxnLogList *>;
	m_unfinished = new map<u16, TxnLogList *>;
	m_bakMap = new map<u16, TxnLogList *>;
}

/** ��־�ط����������� */
LogReplay::~LogReplay() {
	m_unfinished->clear();
	delete m_unfinished;

	m_bakMap->clear();
	delete m_bakMap;

	m_txnList->clear();
	delete m_txnList;
	
	m_bakList->clear();
	delete m_bakList;

	m_memCtx->reset();
	delete m_memCtx;
	
	m_memCtxBak->reset();
	delete m_memCtxBak;
}

/** ��ʼ��־�ط� */
void LogReplay::start() {
	assert(!m_scanHdl);
	m_scanHdl = m_db->getTxnlog()->beginScan(m_lsnStart, m_lsnEnd);
	m_logScaned = false;
#ifdef NTSE_UNIT_TEST
	// ����
	m_returnCnt = 0;
	m_shlNextCnt = 0;
	m_validStartCnt = 0;
#endif
}

/** ������־�ط� */
void LogReplay::end() {
	assert(m_scanHdl);
	m_db->getTxnlog()->endScan(m_scanHdl);
}

/** �л��ڴ���Դ */
void LogReplay::switchMemResource() {
	// ֻ�е����ϵ�ntse����ȫ���ط���ɺ�����л�����
	assert(m_unfinished->size() == 0);
	assert(m_txnList->size() == 0);

	m_memCtx->reset();
	m_txnList->clear();
	// �������� memCtx <-> memCtxBak, m_txnList <-> m_bakList, m_unfinished <-> m_bakMap
	// LogReplay������memCtx/m_txnList/m_unfinishedʼ��Ϊ����memCtxBak/m_bakList/m_bakMapΪ��
	MemoryContext *tmpMemCtx = m_memCtx;
	m_memCtx = m_memCtxBak;
	m_memCtxBak = tmpMemCtx;

	list<TxnLogList*> *tmpList = m_txnList;
	m_txnList = m_bakList;
	m_bakList = tmpList;

	map<u16, TxnLogList*> *tmpMap = m_unfinished;
	m_unfinished = m_bakMap;
	m_bakMap = tmpMap;
}

/** ��ȡδ�����������С��lsn 
 * return
 */
LsnType LogReplay::getMinUnfinishTxnLsn() {
	LsnType minLsn = INVALID_LSN;
	map<u16, TxnLogList *>::iterator iter = m_unfinished->begin();
	for (; iter != m_unfinished->end(); iter++) {
		if (iter->second->m_startLsn < minLsn) {
			minLsn = iter->second->m_startLsn;
		}
	}

	for (iter = m_bakMap->begin(); iter != m_bakMap->end(); iter++) {
		if (iter->second->m_startLsn < minLsn) {
			minLsn = iter->second->m_startLsn;
		}
	}

	return minLsn;
}

/** ��ȡ��һ�������б�
 * @param session �Ự
 * @return ��һ�������б�����һ������NULL
 */
TxnLogList* LogReplay::getNextTxn(Session *session) {
	if (m_vecIdx < m_orderedTxn.size()) { // �����������п�������������
		assert(m_orderedTxn[m_vecIdx]->m_valid);
		assert(m_orderedTxn[m_vecIdx]->m_first->m_logEntry.m_logType == LOG_TXN_START);
		assert(m_orderedTxn[m_vecIdx]->m_last->m_logEntry.m_logType == LOG_TXN_END);
#ifdef NTSE_UNIT_TEST
		++m_returnCnt;
#endif
		return m_orderedTxn[m_vecIdx++];
	}

	if (m_logScaned) {
		return NULL; // �Ѿ�ɨ����
	}
	
	m_orderedTxn.clear();
	m_vecIdx = 0;
	// ������ϵ������Ѿ�ȫ����ɣ�����������
	assert(m_unfinished->size() == 0);
	if (m_unfinished->size() == 0) {
		switchMemResource();
	}

	bool underLimit = true; // ��ʼʱ������������δ��������
	LogScanHandle *scanHit = NULL;

	while ((scanHit = m_db->getTxnlog()->getNext(m_scanHdl)) != NULL) {
#ifdef NTSE_UNIT_TEST
		++m_shlNextCnt;
#endif
		// ɨ��ֱ��m_memctxbak����
		u16 txnId = m_scanHdl->logEntry()->m_txnId;
		u16 tableId = m_scanHdl->logEntry()->m_tableId;
		if (TableDef::tableIdIsVirtualLob(tableId))
			tableId = TableDef::getNormalTableId(tableId);
		if (tableId != m_table->getTableDef(true, session)->m_id) {
			continue; // ���Ǳ������־����Ҫ����
		}

		if (m_scanHdl->logEntry()->m_logType == LOG_TXN_START) {
			TxnType txnType = Session::parseTxnStartLog(m_scanHdl->logEntry());
			// ĳЩ���͵���������Թ���ֻ��Ҫ�ط��������͵�������־
			//	TXN_INSERT,			/** �����¼���� */
			//	TXN_UPDATE,			/** ���¼�¼���� */
			//	TXN_DELETE,			/** ɾ����¼���� */
			if (!(txnType == TXN_INSERT || txnType == TXN_UPDATE || txnType == TXN_DELETE)) {
				continue;
			}

#ifdef NTSE_UNIT_TEST
			++m_validStartCnt;
#endif

			assert(m_unfinished->find(txnId) == m_unfinished->end());
			assert(m_bakMap->find(txnId) == m_bakMap->end());

			if (underLimit) { // �������ݲ��뵽���׼��У�ʹ��memctx
				TxnLogList *txnLogs = getTxnLogList(m_memCtx, logCopy(m_memCtx, m_scanHdl->logEntry()), txnId, m_scanHdl->curLsn());
				// ����list�У�����δ��ɼ�����
				(*m_unfinished)[txnId] = txnLogs;
				//m_txnList->push_back(txnLogs);
			} else { // ���Ѿ�����Ԥ���ڴ��С���µ�LOG_TXN_START���뵽����map��
				TxnLogList *txnlogs = getTxnLogList(m_memCtxBak, logCopy(m_memCtxBak, m_scanHdl->logEntry()), txnId, m_scanHdl->curLsn());
				(*m_bakMap)[txnId] = txnlogs;
				//m_bakList->push_back(txnlogs);
			}
		} else { // ���ǿ�ʼ�������־������Ҫ�ж��Ƿ���map�ڣ�����ֱ���Թ�
			assert(m_scanHdl->logEntry()->m_logType != LOG_TXN_START);
			bool inbakup = false; //�ڱ���������
			if (m_unfinished->find(txnId) == m_unfinished->end()) { // ������map��
				/* ǰ�������в��Ǳ������־�������˵��� */
				/*
				 *	Modified: liaodingbai@corp.netease.coom
				 *	������map��Ҳ�п��ܲ��ڴ�map������������������TXN_UPDATE_HEAP, TXN_MMS_FLUSH_DIRTY��
				 *	TXN_ADD_INDEX��
				 */
				if (m_bakMap->find(txnId) != m_bakMap->end()) {
					inbakup = true;
				} else {
					continue;
				}
			}
			TxnLogList *logList = inbakup ? (*m_bakMap)[txnId] : (*m_unfinished)[txnId];
			assert(logList);
			// ������־�������ж�
			switch (m_scanHdl->logEntry()->m_logType) {
				case LOG_TXN_END:
					{
						// ��δ����б���ɾ���������Ƿ�ɹ�����־��
						map<u16, TxnLogList *> *unfinish = inbakup? m_bakMap : m_unfinished;
						list<TxnLogList *> *txnList = inbakup? m_bakList: m_txnList;

						map<u16, TxnLogList *>::iterator iter = unfinish->find(txnId);
						assert(iter != unfinish->end());
						assert(logList == iter->second);
						unfinish->erase(iter);
						txnList->push_back(logList);

						if (Session::parseTxnEndLog(m_scanHdl->logEntry())) {
							// �ɹ�������
							assert(logList->m_valid);
						} else {  // ʧ�ܵ�δ�ύ����
							assert(logList->m_valid);
							logList->m_valid = false;
							break; //�����־����Ҫ�ٸ�����
						}
					}
					goto default_action;
				// �����������ߵ���һ������־�У�
				// LOG_LOB_INSERT
				// LOG_LOB_DELETE
				// LOG_HEAP_INSERT
				// LOG_HEAP_DELETE
				// LOG_PRE_DELETE
				// LOG_PRE_UPDATE
				// LOG_LOB_UPDATE
				// LOG_MMS_UPDATE
				// LOG_HEAP_UPDATE
				// LOG_IDX_XXXXXX
				case LOG_HEAP_INSERT:
					if (logList->m_type == TXN_INSERT && !TableDef::tableIdIsVirtualLob(m_scanHdl->logEntry()->m_tableId)) {
						assert(logList->m_rowId == INVALID_ROW_ID); // ��ʱ��δ���Ĺ�
						// ����heap insert��־��ȡ��RowId
						logList->m_rowId = DrsHeap::getRowIdFromInsLog(m_scanHdl->logEntry());
					} else {
						assert(true);
					}
					goto default_action;
				case LOG_PRE_DELETE:
					assert(logList->m_type == TXN_DELETE);
					// ����pre delete��־��ȡ��RowId
					logList->m_rowId = m_table->getRidFromPreDeleteLog(m_scanHdl->logEntry());
					goto default_action;
				case LOG_PRE_UPDATE:
					assert(logList->m_type == TXN_UPDATE);
					// ����pre update��־��ȡ��RowId
					logList->m_rowId = Session::getRidFromPreUpdateLog(m_scanHdl->logEntry());
					goto default_action;
				default:
default_action:
					// ������־��list
					appendCopy(logList, logCopy(inbakup ? m_memCtxBak : m_memCtx, m_scanHdl->logEntry()));
					break;
			}
		}
		// һ����־������ϣ��ж��Ƿ���Ҫ��������
		if (underLimit) {
			if (m_memCtx->getMemUsage() >= m_bufSize)
				underLimit = false; //���������ڴ�����ʱ��Ӧ�ú����begin��������뱸��
		} else { //�Ѿ���������¼���ɨ�裬ֱ������unfinish������ȫ����end start��־Ϊֹ
			if (m_unfinished->size() == 0) {
				assert(scanHit);
				break; // ����������ɨ��ѭ������scanhit��Ϊ��
			}
		}
	}

	//assert(m_unfinished->size() == 0); // ��δ���map�п϶�û��δ�����
	assert(m_orderedTxn.size() == 0);
	// �������Ѿ�end��ntse����ȫ����������ط�
	for(list<TxnLogList*>::iterator it = m_txnList->begin(); it != m_txnList->end(); ++it) {
		if ((*it)->m_valid) {
			assert((*it)->m_last->m_logEntry.m_logType == LOG_TXN_END);
			assert((*it)->m_rowId != INVALID_ROW_ID); // RowId�Ѿ������
			m_orderedTxn.push_back(*it);
		}
	}
	m_txnList->clear();

	if (scanHit != NULL) { // ���������־δ��ɨ�裬��ô�ض���������Դ�����ľ�
		assert(m_unfinished->size() == 0);
		assert(!underLimit);
	} else {
		//��־�Ѿ�ɨ���������ʱ�����м������
		// 1.ֻ��������memctx/list/map��û�ñ���memctx/list/map����ʱm_unfinish�ĸ����п����ǲ�Ϊ��
		// 2.����������memctx/list/map�������˱���memctx/list/map����ʱm_unfinish�ĸ����п��ܲ�Ϊ�㣬m_bakMap����Ҳ�п��ܲ�Ϊ��
		//��ʱ�ѱ��������ϵ��������Ҳһ�����ط�
		for(list<TxnLogList*>::iterator it = m_bakList->begin(); it != m_bakList->end(); ++it) {
			if ((*it)->m_valid) {
				assert((*it)->m_last->m_logEntry.m_logType == LOG_TXN_END);
				assert((*it)->m_rowId != INVALID_ROW_ID); // RowId�Ѿ������
				m_orderedTxn.push_back(*it);
			}
		}
		m_bakList->clear();
		assert(!m_logScaned);
		m_logScaned = true;
	}

	if (m_orderedTxn.size() == 0) {
		return NULL;
	}
	// ��orderlist�����ȶ�����
	stable_sort(m_orderedTxn.begin(), m_orderedTxn.end(), txnless);
	assert(m_vecIdx == 0);
#ifdef NTSE_UNIT_TEST
	++m_returnCnt;
#endif
	return m_orderedTxn[m_vecIdx++];
}

/**
 * �����޸�����ά��
 * @pre ���ܼӱ�Ԫ���ݺ�������
 *
 * @param session           �Ự
 * @return                  �ɹ���������true
 * @throw                   ������ʱ�������������󣬻����ļ������ļ����������
 */
bool TblMntAlterIndex::alterTable(Session *session) throw(NtseException) {
	int olLsnHdl = 0, oldOlLsnHdl;
	Database *db = m_table->m_db;
	ILMode oldMetaLock = m_table->getMetaLock(session);
	upgradeMetaLock(session, oldMetaLock, IL_U, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);	// ���ܳ�ʱ��ֱ���׳��쳣
	TableDef *tableDefOld = m_table->getTableDef(true, session);
	SYNCHERE(SP_TBL_ALTIDX_AFTER_U_METALOCK);

	try {
		//Ȩ�޼��
		m_table->checkPermission(session, false);
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	//��ǰ����ֻ��Ӳ����ڵ���������
	for (uint i = 0; i < m_numAddIdx; i++)  {
		assert(m_addIndice[i]->m_online);
		const IndexDef *indexDef = tableDefOld->getIndexDef(m_addIndice[i]->m_name);
		if ((indexDef != NULL) && (indexDef->m_online)) {
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_GENERIC, "Add online index %s has already exist and not completely.", m_addIndice[i]->m_name);
		}
		if ((indexDef != NULL) && (!indexDef->m_online)) {
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_GENERIC, "Add online index %s has already exist and completely.", m_addIndice[i]->m_name);
		}
	}

	bool *idxDeleted = (bool *)session->getMemoryContext()->calloc(sizeof(bool) * tableDefOld->m_numIndice);

	// ��֤ÿһ����ͼɾ���������������ڵ�
	for (u16 i = 0; i < m_numDelIdx; ++i) {
		int idxNo = tableDefOld->getIndexNo(m_delIndice[i]->m_name);
		if (idxNo < 0) {
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_NONEINDEX, "Index %s not found.", m_delIndice[i]->m_name);
		}
		assert(*tableDefOld->m_indice[idxNo] == *m_delIndice[i]);
		idxDeleted[idxNo] = true;
	}

	// ��֤ÿһ����ͼ���ӵ������������ڣ����ֶ����ظ�������Ψһ������
	for (u16 i = 0; i < m_numAddIdx; ++i) {
		if (m_addIndice[i]->m_unique) {
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_DUPINDEX, "Index %s is unique, can not be add online.", m_addIndice[i]->m_name);
		}
		int idxNo = tableDefOld->getIndexNo(m_addIndice[i]->m_name);
		if (idxNo >= 0 && !idxDeleted[idxNo]) {
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_DUPINDEX, "Index %s already exists.", m_addIndice[i]->m_name);
		}
	}

	// �������������ȸ���ԭ������δ��ɾ���������������������������
	m_numNewIndice = tableDefOld->m_numIndice + m_numAddIdx - m_numDelIdx;
	m_newIndice = new IndexDef *[m_numNewIndice];
	u16 idx = 0;
	for (u16 i = 0; i < m_table->getTableDef(true, session)->m_numIndice; ++i) {
		if (idxDeleted[i])
			continue;
		m_newIndice[idx] = new IndexDef(m_table->getTableDef(true, session)->m_indice[i]);
		++idx;
	}
	for (u16 i = 0; i < m_numAddIdx; ++i) {
		m_newIndice[idx] = new IndexDef(m_addIndice[i]);
		++idx;
	}
	assert(idx == m_numNewIndice);

	// ����һ��TableDef��û��������
	TableDef *tempTbdf = tempCopyTableDef(0, NULL);
	// �ر�session��log
	disableLogging(session);

	u64 lsn = m_table->m_db->getTxnlog()->tailLsn();
	char* lsnBuffer = (char*)alloca(Limits::LSN_BUFFER_SIZE*sizeof(char));
	sprintf(lsnBuffer, I64FORMAT"u", lsn);
	// ����������
	string tablePath = string(m_table->getPath());
	string fullPath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_table->getPath();
	DrsIndice *drsIndice;
	string fullIdxPath = fullPath + lsnBuffer + Limits::NAME_TMP_IDX_EXT;
	string relativeIdxPath = string(m_table->getPath()) + lsnBuffer + Limits::NAME_TMP_IDX_EXT;
	try {
		/* TODO: enable this if no system-wide recover cleaning available. */
		File tmpIdx(fullIdxPath.c_str());
		tmpIdx.remove();
		DrsIndice::create(fullIdxPath.c_str(), tempTbdf);
		drsIndice = DrsIndice::open(db, session, fullIdxPath.c_str(), tempTbdf, m_table->getRecords()->getLobStorage());
	} catch (NtseException &e) {
		File tmpIdx(fullIdxPath.c_str());
		tmpIdx.remove();
		assert(m_table->getMetaLock(session) == IL_U);
		downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		delTempTableDef(tempTbdf);
		for (u16 i = 0; i < m_numNewIndice; ++i) {
			delete m_newIndice[i];
		}
		delete [] m_newIndice;
		enableLogging(session);
		throw e;
	}

	u64 startLsn = INVALID_LSN;
	// ����online adding indice
	SYNCHERE(SP_MNT_ALTERINDICE_BEFORE_ADDIND_ONLINE_INDICE);
	
	while (true) {
		try {
			upgradeMetaLock(session, IL_U, IL_X, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &) {
			SYNCHERE(SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL);
		}
	}
	// �ڳ���meta lock X������£���ζ�������ڸñ��ntse�����Ѿ�ȫ���ύ
	// ��ȡlsnstart����������onlinelsn
	startLsn = getLsnPoint(session, true, true, &olLsnHdl);
	if (m_numAddIdx > 0) {
		m_table->setOnlineAddingIndice(session, m_numAddIdx, m_addIndice);
	}
	downgradeMetaLock(session, IL_X, IL_U, __FILE__, __LINE__);
	SYNCHERE(SP_TBL_ALTIDX_BEFORE_GET_LSNSTART);
	
	SYNCHERE(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	// flush mms��ʹ�ö������ݶ���startLsn��˵�Ǹ��¹���
	m_table->flushComponent(session, false, false, true, false);

	/* ������������ */
	assert(drsIndice);
	assert(m_table->getMetaLock(session) == IL_U);
	assert(session->getTrans() == NULL || m_table->getLock(session) == IL_NO);
	// ���Ʋ�ȥ������Ψһ������
	uint *idxUnique = new uint[m_numNewIndice];
	for (u16 i = 0; i < m_numNewIndice; ++i) {
		if (m_newIndice[i]->m_unique) {
			if (m_newIndice[i]->m_primaryKey) {
				idxUnique[i] = 2;
				m_newIndice[i]->m_primaryKey = false;
			}
			else
				idxUnique[i] = 1;
			m_newIndice[i]->m_unique = false;
		} else
			idxUnique[i] = 0;
	}

	// TODO������������Ӧ�ò���

	assert(tempTbdf->m_numIndice == 0);

	try {
		// ���μ�������ĺϷ���
		for (u16 i = 0; i < m_numNewIndice; ++i) 
			m_newIndice[i]->check(tempTbdf);
		// �������ɸ�������
		for (u16 i = 0; i < m_numNewIndice; ++i) {
			drsIndice->createIndexPhaseOne(session, m_newIndice[i], m_table->getTableDef(), m_table->getHeap());
			drsIndice->createIndexPhaseTwo(m_newIndice[i]);
			tempTbdf->addIndex(m_newIndice[i]);
		}
	} catch (NtseException &e) {
		db->getTxnlog()->clearOnlineLsn(olLsnHdl);
		// del temp idx file
		drsIndice->close(session, false);
		delete drsIndice;
		File tmpIdx(fullIdxPath.c_str());
		tmpIdx.remove();
		assert(m_table->getMetaLock(session) == IL_U);
		assert(session->getTrans() == NULL || m_table->getLock(session) == IL_NO);
		if (m_numAddIdx > 0) {
			while (true) {
				try {
					upgradeMetaLock(session, IL_U, IL_X, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
					break;
				} catch (NtseException &e) {
					UNREFERENCED_PARAMETER(e);
				}
			}
			m_table->resetOnlineAddingIndice(session);
		} 
		downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		delTempTableDef(tempTbdf);
		delete [] idxUnique;
		for (u16 i = 0; i < m_numNewIndice; ++i) {
			delete m_newIndice[i];
		}
		delete [] m_newIndice;

		enableLogging(session);
		throw e;
	}
	
	assert(tempTbdf->m_numIndice == m_numNewIndice);
	// ��¼lsnend
	SYNCHERE(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	u64 endLsn = getLsnPoint(session, true, false);

	// �ط�lsnstart��lsnend֮�����־��Ҳ���Ǵ��������ڼ�����Ĳ�����־
	LogReplay replay(m_table, db, startLsn, endLsn);
	replay.start();
	SimpleRidMapping ridmap; // alterIndexʹ��SimpleRidMapping

	db->getSyslog()->log(EL_DEBUG, "start processing log...");
	processLogToIndice(session, &replay, tempTbdf, drsIndice, &ridmap);

	//��ʱ���ܻ�����δ��ɵ����񣬵�replay end�������󣬻Ὣδ��ɵ���������Ҳ�ͷŵ�
	//���Ա����¼��Сδ��������begin trx lsn
	LsnType minUnfinishTxnLsn = replay.getMinUnfinishTxnLsn();
	if (minUnfinishTxnLsn != INVALID_LSN) {
		assert(minUnfinishTxnLsn <= endLsn);
		endLsn = minUnfinishTxnLsn;
	}
	replay.end();

	// �ͷ�lsnռ��
	startLsn = endLsn;
	oldOlLsnHdl = olLsnHdl;
	olLsnHdl = db->getTxnlog()->setOnlineLsn(startLsn); // ��Ϊlsnstart��ԭ��ֵС������һ����ɹ���
	assert(olLsnHdl >= 0);
	db->getTxnlog()->clearOnlineLsn(oldOlLsnHdl);

	// ���ˣ�Ӧ������Ψһ��������������Ψһ��Լ����
	// ���ٴλطŹ���δ���ǰ���Ψһ��Լ���ģ���Ϊ���ǰ�RowId����������־
	for (u16 i = 0; i < m_numNewIndice; ++i) {
		switch (idxUnique[i]) {
			case 2:
				m_newIndice[i]->m_primaryKey = true;
				assert(!tempTbdf->m_indice[i]->m_primaryKey);
				tempTbdf->m_indice[i]->m_primaryKey = true;
				tempTbdf->m_pkey = tempTbdf->m_indice[i];
			case 1:
				m_newIndice[i]->m_unique = true;
				assert(!tempTbdf->m_indice[i]->m_unique);
				tempTbdf->m_indice[i]->m_unique = true;
			default:
				break;
		}
	}
	delete [] idxUnique; //������

	for (u16 i = 0; i < m_numNewIndice; ++i) {
		delete m_newIndice[i];
	}
	delete [] m_newIndice;

	u32 time1 = 0;
	u32 time2 = 0;
	int loop = 0;
	bool startflush = false;
	do {
		time1 = System::fastTime();
		// ˢ������
		if (startflush) {
			m_table->flush(session);
			drsIndice->flush(session);
		}
		// �ٴμ�¼lsn
		endLsn = getLsnPoint(session, true, false);
		// �ط�start��end�����־
		LogReplay replayAgain(m_table, db, startLsn, endLsn);
		replayAgain.start();
		processLogToIndice(session, &replayAgain, tempTbdf, drsIndice, &ridmap);

		//��ʱ���ܻ�����δ��ɵ����񣬵�replay end�������󣬻Ὣδ��ɵ���������Ҳ�ͷŵ�
		//���Ա����¼��Сδ��������begin trx lsn
		LsnType minUnfinishTxnLsn = replayAgain.getMinUnfinishTxnLsn();
		if (minUnfinishTxnLsn != INVALID_LSN) {
			assert(minUnfinishTxnLsn <= endLsn);
			endLsn = minUnfinishTxnLsn;
		}
		replayAgain.end();
		time2 = System::fastTime();
		// ����lsnstart���ͷ��Ѿ��طŵ���־
		startLsn = endLsn;
		oldOlLsnHdl = olLsnHdl;
		olLsnHdl = db->getTxnlog()->setOnlineLsn(startLsn);
		db->getTxnlog()->clearOnlineLsn(oldOlLsnHdl);
		assert(olLsnHdl >= 0);

		db->getSyslog()->log(EL_DEBUG, "time2 - time1 = %d, loop count is %d.", time2 - time1, loop);

		++loop;
		if (!startflush && (loop >= 4 || time2 - time1 <= 15)) {
			startflush = true;
			continue;
		}
		if (startflush && (loop >= 6 || time2 - time1 <= 15)) {
			//X����Ԫ������
			SYNCHERE(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
			ILMode oldMode = m_table->getMetaLock(session);
			//while (true) {
			//	assert(oldMode == IL_U);
			//	try {
			upgradeMetaLock(session, oldMode, IL_X, -1, __FILE__, __LINE__);
			//		break;
			//	} catch (NtseException &e) {
			//		SYNCHERE(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);
			//		UNREFERENCED_PARAMETER(e);
			//	}
			//}

			TableStatus oldTblStatus = m_table->getTableDef()->getTableStatus();
			assert(oldTblStatus == TS_TRX || oldTblStatus == TS_NON_TRX);
			m_table->getTableDef()->setTableStatus(TS_ONLINE_DDL);
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMode, __FILE__, __LINE__);

			while (true) {
				try {
					if (!session->getTrans()) {
						m_table->lock(session, IL_S, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
					} else {
						//��Ϊ�������δʵ����������ֻ�ܼ�X��
						session->getTrans()->lockTable(TL_X, m_table->getTableDef()->m_id);
					}
					break;
				} catch (NtseException &e) {
					UNREFERENCED_PARAMETER(e);
				}
			}

			upgradeMetaLock(session, IL_U, IL_X, -1, __FILE__, __LINE__);
			m_table->getTableDef()->setTableStatus(oldTblStatus);
			break;
		}
	} while(true);
	// ���ط�
	assert(session->getTrans() != NULL || m_table->getLock(session) == IL_S);
	assert(m_table->getMetaLock(session) >= IL_U);
	m_table->flush(session);
	endLsn = getLsnPoint(session, false, false);
	LogReplay replayFinal(m_table, db, startLsn, endLsn);
	replayFinal.start();
	processLogToIndice(session, &replayFinal, tempTbdf, drsIndice, &ridmap);
	//��ʱ�ض�������δ��ɵ�ntse����
	assert(replayFinal.getMinUnfinishTxnLsn() == INVALID_LSN);
	replayFinal.end();
	drsIndice->flush(session);

	// ����onlineLSN�ͷ�
	db->getTxnlog()->clearOnlineLsn(olLsnHdl);

	db->getSyslog()->log(EL_DEBUG, "finish processing log, start replacing table.");

	// X����������
	while (true) {
		//���������������
		if (NULL != session->getTrans()) {
			break;
		}
		assert(m_table->getLock(session) == IL_S);
		try {
			m_table->upgradeLock(session, IL_S, IL_X, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
		}
	}

	// ��loggging����¼��־
	assert(!session->isLogging());
	enableLogging(session);
	// bumpFlushLSN
	db->bumpFlushLsn(session, m_table->m_tableDef->m_id);
	// д��־
	m_table->writeAlterIndiceLog(session, tempTbdf, relativeIdxPath.c_str());
	SYNCHERE(SP_MNT_ALTERINDICE_JUST_WRITE_LOG);

	// ��onlineAddingIndex�����øĻ���
	if (m_numAddIdx > 0)
		m_table->resetOnlineAddingIndice(session);

	TableDef *oldDef = m_table->m_tableDef;
	m_table->setTableDef(tempTbdf);
	// ��tabledefˢ�µ���
	m_table->writeTableDef();
	SYNCHERE(SP_MNT_ALTERINDICE_JUST_WRITE_TABLEDEF);

	additionalAlterIndex(session, oldDef, &tempTbdf, drsIndice, m_addIndice, m_numAddIdx, idxDeleted);

	// �رձ�
	m_table->close(session, true, true);
	delete oldDef;
	oldDef = NULL;

	// �ر�������
	drsIndice->close(session, true);
	delete drsIndice;
	drsIndice = NULL;

	// ɾ���滻����
	string indexFilePath = fullPath + Limits::NAME_IDX_EXT;
	File idxFile(fullIdxPath.c_str());
	idxFile.move(indexFilePath.c_str(), true); // ����
	SYNCHERE(SP_MNT_ALTERINDICE_INDICE_REPLACED);

	// ���´򿪶�
	/*Table *tmpTb = 0;
	try {
		tmpTb = Table::open(db, session, tablePath.c_str(), m_table->hasCompressDict());
	} catch (NtseException &) {
		assert(false);
	}

	// ��֤table������
#ifdef NTSE_UNIT_TEST
	tmpTb->verify(session);
#endif

	// �滻�����
	m_table->replaceComponents(tmpTb);
	delete tmpTb; // ����closeֱ��ɾ*/
	reopenTblAndReplaceComponent(session, tablePath.c_str(), isNewTbleHasDict());

	// �ͷű���
	if (!session->getTrans()) {
		assert(m_table->getLock(session) == IL_X);
		m_table->unlock(session, IL_X);
	}
	assert(m_table->getMetaLock(session) == IL_X);
	downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);

	db->getSyslog()->log(EL_DEBUG, "finish altering table.");

	SYNCHERE(SP_MNT_ALTERINDICE_FINISH);
	return true;
}

bool TblMntAlterIndex::isNewTbleHasDict() {
	return m_table->hasCompressDict();
}

/**
 * ��ʱ����һ�ݲ������TableDef��ʹ���µ��������������ԭ����������
 * @param numIndice           ʹ���µ�������Ŀ��Ĭ��Ϊ0
 * @param indice              ����ʹ�õ�������������
 * @return                    ���ؿ����ı���
 */
TableDef* TableOnlineMaintain::tempCopyTableDef(u16 numIndice, IndexDef **indice) {
	TableDef *origTbDef = m_table->getTableDef();
	TableDef *tblDef = new TableDef();
	*tblDef = *origTbDef; // ȫֵ����

	tblDef->m_name = new char[strlen(origTbDef->m_name) + 1];
	strcpy(tblDef->m_name, origTbDef->m_name);
	tblDef->m_schemaName = new char[strlen(origTbDef->m_schemaName) + 1];
	strcpy(tblDef->m_schemaName, origTbDef->m_schemaName);

	//�����ж���
	tblDef->m_columns = new ColumnDef*[tblDef->m_numCols];
	for (u16 i = 0; i < tblDef->m_numCols; i++) {
		tblDef->m_columns[i] = new ColumnDef(origTbDef->m_columns[i]);
		tblDef->m_columns[i]->m_inIndex = false;
	}
	//����ѹ��������Ϣ�������鶨��
	tblDef->m_rowCompressCfg = origTbDef->m_rowCompressCfg ? new RowCompressCfg(*origTbDef->m_rowCompressCfg) : NULL;
	tblDef->m_numColGrps = origTbDef->m_numColGrps;
	if (origTbDef->m_numColGrps > 0) {
		tblDef->m_colGrps = new ColGroupDef *[origTbDef->m_numColGrps];
		for (uint i = 0; i < origTbDef->m_numColGrps; i++) {
			tblDef->m_colGrps[i] = new ColGroupDef(origTbDef->m_colGrps[i]);
		}
	} else {
		assert(!origTbDef->m_colGrps);
	}
	//�½���������
	tblDef->m_pkey = NULL;
	tblDef->m_numIndice = (u8)numIndice;
	if (indice) {
		tblDef->m_indice = indice;
		for (u16 i = 0; i < numIndice; ++i) {
			IndexDef *idxdef = indice[i];
			for (u16 j = 0; j < idxdef->m_numCols; ++j) {
				tblDef->m_columns[idxdef->m_columns[j]]->m_inIndex = true;
			}
			if (idxdef->m_primaryKey)
				tblDef->m_pkey = idxdef;
		}
	} else {
		tblDef->m_indice = NULL;
	}

	return tblDef;
}

/**
 * ��ȡLsn
 * @param session		�Ự
 * @param lockTable		������
 * @param setOnlineLSN	����OnlieLSNΪ��õ�LSN
 * @return				lsn
 */
u64 TableOnlineMaintain::getLsnPoint(Session *session, bool lockTable, bool setOnlineLSN, int *onlineLsnHdl) {
	assert(m_table->getMetaLock(session) >= IL_U);
	assert(session->getTrans() != NULL || (m_table->getLock(session) == IL_NO && lockTable) || (m_table->getLock(session) != IL_NO && !lockTable));
	assert(!setOnlineLSN || onlineLsnHdl);
	while (lockTable) {
		try {
			m_table->lock(session, IL_S, m_table->m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
		}
	}
	u64 lsn = INVALID_LSN;
	do {
		lsn = m_table->m_db->getTxnlog()->tailLsn();
	} while (setOnlineLSN && ((*onlineLsnHdl = m_table->m_db->getTxnlog()->setOnlineLsn(lsn)) < 0));

	if (lockTable)
		m_table->unlock(session, IL_S);

	return lsn;
}


/**
* �ط���־��ά������
*
* @param session	�Ự
* @param replay		�Ѿ�start��LogReplay����
* @param recconv	��¼ת��
* @param destTb		������
* @param ridmap		��¼ӳ��
*/
void TableOnlineMaintain::processLogToTable(Session *session, LogReplay *replay, RecordConvert *recconv, Table *destTb, NtseRidMapping *ridmap) {
	TxnLogList *txnList = NULL;
	TableDef *origTbdef = m_table->getTableDef();
	TableDef *destTbdef = destTb->getTableDef();
	
	// ��ʱ��session�е�������ΪNULL����ֹ�طŹ�����������дTNT��־
	TNTTransaction *trx = session->getTrans();
	session->setTrans(NULL);

	u64 sp = session->getMemoryContext()->setSavepoint();

	// ���ڶ�ȡ�����
	MemoryContext lobCtx(Limits::PAGE_SIZE, 1);

	Record rec; // ��log�й���ļ�¼�����������ݣ�ΪREC_VARLEN����REC_FIXLEN��ʽ
	rec.m_format = origTbdef->m_recFormat;

	byte *mysqlData = (byte *)session->getMemoryContext()->alloc(origTbdef->m_maxRecSize);
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, mysqlData, origTbdef->m_maxRecSize);

	byte *redData =  (byte *)session->getMemoryContext()->alloc(destTbdef->m_maxRecSize);
	Record redRec(INVALID_ROW_ID, REC_REDUNDANT, redData, destTbdef->m_maxRecSize);

	u16 *origColumns = (u16 *)session->getMemoryContext()->alloc(sizeof(u16) * origTbdef->m_numCols);
	for (u16 i = 0; i < origTbdef->m_numCols; ++i)
		origColumns[i] = i;
	SubRecord subRec(REC_REDUNDANT, origTbdef->m_numCols, origColumns, mysqlData, origTbdef->m_maxRecSize);

	u16 *destColumns = (u16 *)session->getMemoryContext()->alloc(sizeof(u16) * destTbdef->m_numCols);
	for (u16 i = 0; i < destTbdef->m_numCols; ++i)
		destColumns[i] = i;

	size_t lobSize;
	uint dupIndex;

#ifdef NTSE_UNIT_TEST
	uint loop = 0;
	uint insrep = 0;
	uint delrep = 0;
	uint updrep = 0;
#endif
	while ((txnList = replay->getNextTxn(session)) != NULL) {
#ifdef NTSE_UNIT_TEST
		++loop;
#endif
		switch (txnList->m_type) {
			/** insert������ط� */
			case TXN_INSERT:
#ifdef NTSE_UNIT_TEST
				++insrep;
#endif
				{
					McSavepoint msp(session->getMemoryContext());

					RowId rid = ridmap->getMapping(txnList->m_rowId);
					// �ڽ���ת��֮ǰ���������ж��Ƿ���Ҫskip
					if (rid == INVALID_ROW_ID) {
						bool hasLob = false;

						for(LogCopy *logCopy = txnList->m_first; logCopy != NULL; logCopy = logCopy->m_next) {
							if (logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && !TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId)) {
								DrsHeap::getRecordFromInsertlog(&logCopy->m_logEntry, &rec);
								//break; // ��Ϊ�������־�ڶ���־֮ǰ�����Բ��ص���break��������
							} else
								if(logCopy->m_logEntry.m_logType == LOG_LOB_INSERT ||
									(logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId))) {
										hasLob = true;
								}
						}
						u64 sp = session->getMemoryContext()->setSavepoint();
						// ���Ѽ�¼��ʽת��ΪREC_REDUNDANT
						if (origTbdef->m_recFormat == REC_FIXLEN) {
								// û�д���󣬲����Ƕ�����¼��ֱ��ʹ��
								mysqlRec.m_data = rec.m_data;
								mysqlRec.m_size = rec.m_size;
						} else {//���Ϊ�䳤��¼��ѹ����¼
							assert(subRec.m_data == mysqlData);
							assert(subRec.m_size == origTbdef->m_maxRecSize);
							if (rec.m_format == REC_VARLEN) {
								RecordOper::extractSubRecordVR(origTbdef, &rec, &subRec);
							} else {
								assert(m_table->hasCompressDict());
								assert(origTbdef->m_recFormat == REC_COMPRESSED);
								RecordOper::extractSubRecordCompressedR(session->getMemoryContext(), m_table->getRecords()->getRowCompressMng(), 
									origTbdef, &rec, &subRec);
							}
							
							assert(subRec.m_size == origTbdef->m_maxRecSize);
							mysqlRec.m_data = mysqlData;
							mysqlRec.m_size = origTbdef->m_maxRecSize;
						}

						LogCopy *logCopy = txnList->m_first;

						for (u16 col = 0; col < origTbdef->m_numCols; ++col) {
							ColumnDef *colDef = origTbdef->m_columns[col];
							if (!colDef->isLob()) {
								continue;
							}
							if (!RecordOper::isNullR(origTbdef, &mysqlRec, col)) {
								assert(hasLob);
								LobId lobId = RecordOper::readLobId(mysqlRec.m_data, colDef);
								assert(lobId != INVALID_LOB_ID);
								do {
									logCopy = logCopy->m_next;
								} while (!(!logCopy // ɨ�赽ͷ���������ǲ�������������
									|| logCopy->m_logEntry.m_logType == LOG_LOB_INSERT // ���Ǵ��ʹ�������
									|| (logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId)))); // С�ʹ�������
								assert(logCopy);
								assert((logCopy->m_logEntry.m_logType == LOG_LOB_INSERT) ||
									(logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId)));
								byte *lob = m_table->getLobStorage()->parseInsertLog(&logCopy->m_logEntry, lobId, &lobSize, session->getMemoryContext());
								assert(RecordOper::readLobSize(mysqlRec.m_data, colDef) == 0);
								RecordOper::writeLobSize(mysqlRec.m_data, colDef, (uint)lobSize);
								RecordOper::writeLob(mysqlRec.m_data, colDef, lob);
							} else {
								RecordOper::writeLobSize(mysqlRec.m_data, colDef, 0);
								RecordOper::writeLob(mysqlRec.m_data, colDef, NULL);
							}
						}

						// ת��
						byte *destRec = recconv->convertMysqlOrRedRec(&mysqlRec, session->getMemoryContext());

						try {
							rid = destTb->insert(session, destRec, true, &dupIndex);
						} catch (NtseException &e) {
							UNREFERENCED_PARAMETER(e);
						}
						assert(rid != INVALID_ROW_ID);
						ridmap->insertMapping(txnList->m_rowId, rid);

						// �ָ�savepoint
						session->getMemoryContext()->resetToSavepoint(sp);
					} // if (rid != INVALID_ROW_ID) { else
				}
				break;
			/** delete������ط� */
			case  TXN_DELETE:
#ifdef NTSE_UNIT_TEST
				++delrep;
#endif
				{
					RowId rid = ridmap->getMapping(txnList->m_rowId);

					if (rid != INVALID_ROW_ID) {
						// ɾ����¼
						TblScan *posScan = NULL;
						try {
							posScan = destTb->positionScan(session, OP_DELETE, destTb->getTableDef()->m_numCols, destColumns);
						} catch (NtseException &e) {
							UNREFERENCED_PARAMETER(e);
						}
						bool exist = destTb->getNext(posScan, redData, rid);
						assert_always(exist);
						destTb->deleteCurrent(posScan);
						destTb->endScan(posScan);
						// ɾ��ӳ���ϵ
						ridmap->deleteMapping(txnList->m_rowId);
					}
				}
				break;
			/** update������ط� */
			case TXN_UPDATE:
#ifdef NTSE_UNIT_TEST
				++updrep;
#endif
				{
					RowId rid = ridmap->getMapping(txnList->m_rowId);

					if (rid != INVALID_ROW_ID) {
						// ����update��־
						u64 sp = session->getMemoryContext()->setSavepoint();
						u64 lobSp = lobCtx.setSavepoint();

						PreUpdateLog *puLog = NULL;
						for(LogCopy *logCopy = txnList->m_first; logCopy != NULL; logCopy = logCopy->m_next) {
							if (logCopy->m_logEntry.m_logType == LOG_PRE_UPDATE) {
								puLog = session->parsePreUpdateLog(m_table->getTableDef(true, session), &logCopy->m_logEntry);
								break;
							}
						}

						mysqlRec.m_data = puLog->m_subRec->m_data;


						// ������������
						if (puLog->m_numLobs == 0) {
							// TNT��purge�ڹ���Ԥ������־ʱ�����¼���������
							for (u16 i = 0; i < puLog->m_subRec->m_numCols; i++) {
								u16 col = puLog->m_subRec->m_columns[i];
								ColumnDef *colDef = origTbdef->m_columns[col];
								if (!colDef->isLob())
									continue;
								byte *lob = NULL;
								uint lobSize = 0;
								bool isNull = RecordOper::isNullR(m_table->getTableDef(), &mysqlRec, col);
								if(!isNull) {
									LobId lid = RecordOper::readLobId(mysqlRec.m_data, colDef);
									lob = m_table->getLobStorage()->get(session, &lobCtx, lid, &lobSize, false);
								}
								// ���ԭ����Null���߶�ȡ�����������Ѹ�����ΪNull��ȥ������ʱ��
								if (isNull || (lob == NULL && lobSize == 0)) {
									RecordOper::setNullR(destTbdef, &mysqlRec, col, true);
									RecordOper::writeLobSize(mysqlRec.m_data, colDef, 0);
									RecordOper::writeLob(mysqlRec.m_data, colDef, NULL);
								} else {
									RecordOper::writeLobSize(mysqlRec.m_data, colDef, lobSize);
									RecordOper::writeLob(mysqlRec.m_data, colDef, lob);
								}

							}
						} else {
							// Ϊ����ntse��Ԫ����
							for (u16 i = 0; i < puLog->m_numLobs; ++i) {
								u16 col = puLog->m_lobCnos[i];
								ColumnDef *colDef = origTbdef->m_columns[col];
								assert(colDef->isLob());
								uint lobSize = puLog->m_lobSizes[i];
								byte *lobdata = puLog->m_lobs[i];
								RecordOper::writeLobSize(mysqlRec.m_data, colDef, lobSize);
								RecordOper::writeLob(mysqlRec.m_data, colDef, lobdata);
							}
						}
						

						// ת����¼
						u16 outNumCols;
						u16 *outColumns;
						byte *converted = recconv->convertMysqlOrRedRec(&mysqlRec, session->getMemoryContext(),
							puLog->m_subRec->m_numCols, puLog->m_subRec->m_columns, &outNumCols, &outColumns);

						// ���¼�¼
						TblScan *posScan = NULL;
						try {
							posScan = destTb->positionScan(session, OP_UPDATE, outNumCols, outColumns);
						} catch (NtseException &e) {
							UNREFERENCED_PARAMETER(e);
						}

						// ��λ
						bool exist = destTb->getNext(posScan, redData, rid);
						assert_always(exist);

						// ����
						posScan->setUpdateColumns(outNumCols, outColumns);
						// �������ݱض���mysql��ʽ
						NTSE_ASSERT(destTb->updateCurrent(posScan, converted, true, &dupIndex)); // �����쳣����¼����������ת��ʱ�ѽ����

						destTb->endScan(posScan);
						lobCtx.resetToSavepoint(lobSp);
						session->getMemoryContext()->resetToSavepoint(sp);
					} // else of if (rid == INVALID_ROW_ID) {
				}
				break;
				
			default:
				assert(false);
		} // switch (txnlist->m_type) {

	}
	session->getMemoryContext()->resetToSavepoint(sp);
	session->setTrans(trx);
}


/**
 * �ط���־��ά������
 *
 * @param session		�Ự
 * @param replay		�Ѿ�start��LogReplay����
 * @param tbdef			��������Ӧ��TableDef
 * @param indice		������־�طŵ���������
 * @param ridmap		��¼ӳ���ĿǰӦ��Ϊ��ӳ���
 */
void TableOnlineMaintain::processLogToIndice(Session *session, LogReplay *replay, TableDef *tbDef, DrsIndice *indice, SimpleRidMapping *ridmap) {
	TxnLogList *txnList  = NULL;
	Record rec; // ��log�й���ļ�¼�����������ݣ�ΪREC_VARLEN����REC_FIXLEN��ʽ
	Record redRec; // ת����������¼���������������ݣ��䳤��Ҫת��
	SubRecord subRec, key, redSubrec, redSubAfter, keyAfter;

	u64 sp = session->getMemoryContext()->setSavepoint();
	byte *data = (byte *)session->getMemoryContext()->alloc(tbDef->m_maxRecSize/*Limits::DEF_MAX_REC_SIZE*/);
	byte *afterdata = (byte *)session->getMemoryContext()->alloc(tbDef->m_maxRecSize/*Limits::DEF_MAX_REC_SIZE*/);
	byte *keydata = (byte *)session->getMemoryContext()->alloc(tbDef->m_maxRecSize/*Limits::DEF_MAX_REC_SIZE*/);
	byte *keyafterdata = (byte *)session->getMemoryContext()->alloc(tbDef->m_maxRecSize/*Limits::DEF_MAX_REC_SIZE*/);

	rec.m_format = tbDef->m_recFormat;
	subRec.m_format = REC_REDUNDANT;
	subRec.m_numCols = tbDef->m_numCols;
	u16 *columns = (u16 *)session->getMemoryContext()->alloc(sizeof(u16) * tbDef->m_numCols);
	subRec.m_columns = columns;
	for (u16 col = 0; col < tbDef->m_numCols; ++col) {
		subRec.m_columns[col] = col;
	}

	assert(rec.m_format == REC_FIXLEN || rec.m_format == REC_VARLEN || rec.m_format == REC_COMPRESSED);
	redRec.m_format = redSubrec.m_format = redSubAfter.m_format = REC_REDUNDANT;
	key.m_format = keyAfter.m_format = KEY_PAD;

	// Ϊÿ��������ȡ���������columns���飬���ں���������־ʱ��ȡ�Ӽ�¼����
	u16 **ascIndexColumns = (u16 **)session->getMemoryContext()->alloc(sizeof(u16 *) * tbDef->m_numIndice);
	for (u16 i = 0; i < tbDef->m_numIndice; ++i) {
		IndexDef *idxDef = tbDef->getIndexDef(i);
		ascIndexColumns[i] = (u16 *)session->getMemoryContext()->alloc(sizeof(u16) * idxDef->m_numCols);
		memcpy(ascIndexColumns[i], idxDef->m_columns, sizeof(u16) * idxDef->m_numCols);
		std::sort(ascIndexColumns[i], ascIndexColumns[i] + idxDef->m_numCols);
	}

#ifdef NTSE_UNIT_TEST
	uint loop = 0;
	uint insRep = 0;
	uint delRep = 0;
	uint updRep = 0;
#endif
	while ((txnList = replay->getNextTxn(session)) != NULL) {
#ifdef NTSE_UNIT_TEST
		++loop;
#endif
		// �ع�����
		switch (txnList->m_type) {
			/** insert������ */
			case TXN_INSERT:
#ifdef NTSE_UNIT_TEST
				++insRep;
#endif
				{
					McSavepoint msp(session->getMemoryContext());

					for(LogCopy *logCopy = txnList->m_first; logCopy != NULL; logCopy = logCopy->m_next) {
						if (logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && !TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId)) {
							DrsHeap::getRecordFromInsertlog(&logCopy->m_logEntry, &rec);
							break;
						}
					}

					RowId rid = ridmap->getMapping(txnList->m_rowId);

					assert(indice);
					assert(rid != INVALID_ROW_ID); // ��Ϊsimplemapping �϶����᷵��INVALID_ROW_ID��
					assert(indice->getIndexNum() == tbDef->m_numIndice);
					// ��Ҫ����������־������
					for (u16 i = 0; i < tbDef->m_numIndice; ++i) {
						McSavepoint lobSavepoint(session->getLobContext());
						//indice->getIndex(i)->
						IndexDef *idxdef = tbDef->m_indice[i];
						// ������������key
						assert(key.m_format == KEY_PAD);
						key.m_numCols = idxdef->m_numCols;
						key.m_columns = idxdef->m_columns;
						key.m_data = keydata;
						key.m_rowId = INVALID_ROW_ID;
						key.m_size = Limits::PAGE_SIZE;
						
						// ��ȡ����ֵ
						if (rec.m_format == REC_FIXLEN) {
							// Record׼��
							assert(redRec.m_format == REC_REDUNDANT);
							redRec.m_rowId = rid;
							redRec.m_data = rec.m_data;
							redRec.m_size = rec.m_size;
							assert(redRec.m_size == tbDef->m_maxRecSize);
							// SubRecord׼��
							redSubrec.m_rowId = rid;
							assert(redSubrec.m_format == REC_REDUNDANT);
							redSubrec.m_numCols = key.m_numCols;
							redSubrec.m_columns = ascIndexColumns[i];
							redSubrec.m_data = rec.m_data;
							redSubrec.m_size = rec.m_size;
						} else if (rec.m_format == REC_VARLEN) {
							// SubRecord׼��
							assert(redSubrec.m_format == REC_REDUNDANT);
							redSubrec.m_numCols = key.m_numCols;
							redSubrec.m_columns = ascIndexColumns[i];
							redSubrec.m_data = data;
							redSubrec.m_size = Limits::DEF_MAX_REC_SIZE;
							RecordOper::extractSubRecordVR(tbDef, &rec, &redSubrec);
							redSubrec.m_rowId = rid;

							assert(redSubrec.m_size != Limits::PAGE_SIZE);
							// Record׼��
							assert(redRec.m_format == REC_REDUNDANT);
							redRec.m_rowId = rid;
							redRec.m_data = redSubrec.m_data;
							redRec.m_size = redSubrec.m_size;
							assert(redRec.m_size == tbDef->m_maxRecSize);
						} else {
							assert(m_table->hasCompressDict());
							assert(rec.m_format == REC_COMPRESSED);
							assert(redSubrec.m_format == REC_REDUNDANT);
							redSubrec.m_numCols = key.m_numCols;
							redSubrec.m_columns = ascIndexColumns[i];
							redSubrec.m_data = data;
							redSubrec.m_size = Limits::DEF_MAX_REC_SIZE;
							RecordOper::extractSubRecordCompressedR(session->getMemoryContext(), m_table->getRecords()->getRowCompressMng(), 
								m_table->getTableDef(true, session), &rec, &redSubrec, NULL);
							redSubrec.m_rowId = rid;

							assert(redSubrec.m_size != Limits::PAGE_SIZE);
							// Record׼��
							assert(redRec.m_format == REC_REDUNDANT);
							redRec.m_rowId = rid;
							redRec.m_data = redSubrec.m_data;
							redRec.m_size = redSubrec.m_size;
							assert(redRec.m_size == tbDef->m_maxRecSize);
						}

						Array<LobPair*> lobArray;
						if (idxdef->hasLob()) {
							RecordOper::extractLobFromR(session, tbDef, idxdef, m_table->getLobStorage(), &redRec, &lobArray);
						}

						// �����������key
						RecordOper::extractKeyRP(tbDef, idxdef, &redRec, &lobArray, &key);
						assert(key.m_rowId == rid);

						// �����������Ƿ��Ѿ���(rowid, key)���
						RowId outRid = INVALID_ROW_ID;
						assert(key.m_rowId == redSubrec.m_rowId);
						bool exist = indice->getIndex(i)->getByUniqueKey(session, &key, None, &outRid, NULL, NULL, NULL);
						if (!exist) {
							// ��(rowid, key)���뵽������
							assert(outRid == INVALID_ROW_ID);
							bool duplicateKey;
							u64 token = session->getToken(); // TODO:
							indice->getIndex(i)->insert(session, &key, &duplicateKey);
							session->unlockIdxObjects(token); // TODO:

						} else {// ������ڣ�����������
							assert(outRid == key.m_rowId);
						}
					}
				} // case TXN_INSERT
				break;
			/** delete�����ط� */
			case TXN_DELETE:
#ifdef NTSE_UNIT_TEST
				++delRep;
#endif
				{
					u64 savepoint = session->getMemoryContext()->setSavepoint();
					PreDeleteLog *pdLog = NULL;
					for(LogCopy *logCopy = txnList->m_first; logCopy != NULL; logCopy = logCopy->m_next) {
						if (logCopy->m_logEntry.m_logType == LOG_PRE_DELETE) {
							pdLog = m_table->parsePreDeleteLog(session, &logCopy->m_logEntry);
							break;
						}
					}
					RowId rid = ridmap->getMapping(txnList->m_rowId);

					//if (indice) {
					assert(indice);
					assert(pdLog->m_indexPreImage && pdLog->m_indexPreImage->m_format == REC_REDUNDANT);
					// �������ؽ�
					assert(rid != INVALID_ROW_ID);
					// record
					assert(redRec.m_format == REC_REDUNDANT);
					redRec.m_rowId = rid;
					redRec.m_data = pdLog->m_indexPreImage->m_data;
					redRec.m_size = tbDef->m_maxRecSize;
					assert(redRec.m_size == pdLog->m_indexPreImage->m_size);
				
					for (u16 i = 0; i < tbDef->m_numIndice; ++i) {
						McSavepoint lobSavepoint(session->getLobContext());
						IndexDef *idxDef = tbDef->m_indice[i];
						// ���ɲ���key
						assert(key.m_format == KEY_PAD);
						key.m_numCols = idxDef->m_numCols;
						key.m_columns = idxDef->m_columns;
						key.m_data = keydata;
						key.m_rowId = INVALID_ROW_ID;
						key.m_size = Limits::PAGE_SIZE;

						Array<LobPair*> lobArray;
						if (idxDef->hasLob()) {
							RecordOper::extractLobFromR(session, tbDef, idxDef, m_table->getLobStorage(), &redRec, &lobArray);
						}

						// �����������key
						RecordOper::extractKeyRP(tbDef, idxDef, &redRec, &lobArray, &key);
						assert(key.m_rowId == rid);
						
						// �����������Ƿ��Ѿ���(rowid, key)���
						RowId outRid = INVALID_ROW_ID;
						bool exist = indice->getIndex(i)->getByUniqueKey(session, &key, None, &outRid, NULL, NULL, NULL);
						if (exist) {
							// ɾ��֮
							assert(outRid == key.m_rowId);
							u64 token = session->getToken();
							indice->getIndex(i)->del(session, &key);
							session->unlockIdxObjects(token);
						} else {
							// ɶҲ����
						}
					} // for ����������
					session->getMemoryContext()->resetToSavepoint(savepoint);
				}
				break;
			/** update������ */
			case TXN_UPDATE:
#ifdef NTSE_UNIT_TEST
				++updRep;
#endif
				{
					u64 savepoint = session->getMemoryContext()->setSavepoint();
					PreUpdateLog *puLog = NULL;
					for(LogCopy *logCopy = txnList->m_first; logCopy != NULL; logCopy = logCopy->m_next) {
						if (logCopy->m_logEntry.m_logType == LOG_PRE_UPDATE) {
							puLog = session->parsePreUpdateLog(m_table->getTableDef(true, session), &logCopy->m_logEntry);
							break;
						}
					}

					if (!puLog->m_updateIndex) {
						continue;
					}

					RowId rid = ridmap->getMapping(txnList->m_rowId);
					/*
					 *	���߸�������ʱ����������в�������������ʱm_indexPreImageΪnull��Ӧ����ֱ������
					 *	ADD: liaodingbai@corp.netease.com
					 */
					if(puLog->m_indexPreImage == NULL)
						break;

					assert(indice);
					assert(puLog->m_indexPreImage && puLog->m_indexPreImage->m_format == REC_REDUNDANT);
					assert(rid != INVALID_ROW_ID);
					// �����º�����䵽data������
					assert(redRec.m_format == REC_REDUNDANT);
					redRec.m_rowId = rid;
					redRec.m_size = tbDef->m_maxRecSize;
					redRec.m_data = afterdata;
					memcpy(afterdata, puLog->m_indexPreImage->m_data, redRec.m_size);
					// ��ǰ����£�������������ĸ��º��������
					RecordOper::updateRecordRR(tbDef, &redRec, puLog->m_subRec);

					// ����SubRecord
					redSubAfter.m_rowId = rid;
					redSubAfter.m_size = m_table->getTableDef(true, session)->m_maxRecSize;
					redSubAfter.m_data = redRec.m_data;
					redSubAfter.m_numCols = puLog->m_indexPreImage->m_numCols;
					redSubAfter.m_columns = puLog->m_indexPreImage->m_columns;
					// ����ɾ���õ�ǰ��redSubrec
					assert(redSubrec.m_format == REC_REDUNDANT);
					redSubrec.m_rowId = rid;
					redSubrec.m_size = m_table->getTableDef(true, session)->m_maxRecSize; // TODO:
					redSubrec.m_data = puLog->m_indexPreImage->m_data;
					redSubrec.m_numCols = puLog->m_indexPreImage->m_numCols;
					redSubrec.m_columns = puLog->m_indexPreImage->m_columns;

					vector<u16> interSec(tbDef->m_numCols);
					vector<u16>::iterator it;
					for (u16 i = 0; i < tbDef->m_numIndice; ++i) {
						McSavepoint lobSavepoint(session->getLobContext());
						IndexDef *idxDef = tbDef->m_indice[i];

						/*
						 *	�������ǰ�����漰����
						 *	ADD��liaodingbai@corp.netease.com
						 */
						it = set_intersection(ascIndexColumns[i], 
							ascIndexColumns[i] + idxDef->m_numCols,
							puLog->m_indexPreImage->m_columns, 
							puLog->m_indexPreImage->m_columns + puLog->m_indexPreImage->m_numCols,
							interSec.begin());
						int sameCols = int(it - interSec.begin());
						/**	preImage�е��к������в���ȫƥ�䣬��ζ���޸Ĳ��漰������ */
						if (sameCols != idxDef->m_numCols) {
							continue;
						}
						/**	������Ŀ���������������ͬ */
						assert(sameCols == idxDef->m_numCols);
						
						// ���ɲ���key
						assert(key.m_format == KEY_PAD);
						key.m_numCols = idxDef->m_numCols;
						key.m_columns = idxDef->m_columns;
						key.m_data = keydata;
						key.m_rowId = INVALID_ROW_ID;
						key.m_size = Limits::PAGE_SIZE;
						// ������k�� ǰ��
						redRec.m_data = redSubrec.m_data;
						assert(redRec.m_format == REC_REDUNDANT);
						assert(redRec.m_rowId == rid);
						assert(redRec.m_size == m_table->getTableDef(true, session)->m_maxRecSize);

						Array<LobPair*> lobArray;
						if (idxDef->hasLob()) {
							RecordOper::extractLobFromR(session, tbDef, idxDef, m_table->getLobStorage(), &redRec, &lobArray);
						}

						RecordOper::extractKeyRP(tbDef, idxDef, &redRec, &lobArray, &key);
						assert(key.m_rowId == rid);
						// �����������Ƿ��Ѿ���(rowid, key)���
						RowId outRid = INVALID_ROW_ID;
						bool exist = indice->getIndex(i)->getByUniqueKey(session, &key, None, &outRid, NULL, NULL, NULL);
						if (exist) {
							// �鵽��ɾ��
							u64 token = session->getToken();
							indice->getIndex(i)->del(session, &key);

							// ���Һ���û�������֮
							// ���ɺ�����Ҽ�
							assert(keyAfter.m_format == KEY_PAD);
							keyAfter.m_numCols = idxDef->m_numCols;
							keyAfter.m_columns = idxDef->m_columns;
							keyAfter.m_data = keyafterdata;
							keyAfter.m_rowId = INVALID_ROW_ID;
							keyAfter.m_size = Limits::PAGE_SIZE;
							redRec.m_data = redSubAfter.m_data; // ʹ�ú���

							Array<LobPair*> lobArray;
							if (idxDef->hasLob()) {
								RecordOper::extractLobFromR(session, tbDef, idxDef, m_table->getLobStorage(), &redRec, &lobArray);
							}
							// ��������Ҽ�
							RecordOper::extractKeyRP(m_table->getTableDef(true, session), idxDef, &redRec, &lobArray, &keyAfter);

							outRid = INVALID_ROW_ID;
							bool afterKey = indice->getIndex(i)->getByUniqueKey(session, &keyAfter, None, &outRid, NULL, NULL, NULL);
							if (afterKey) {
								// ��������
							} else {
								assert(outRid == INVALID_ROW_ID);
								bool duplicateKey;
								indice->getIndex(i)->insert(session, &keyAfter, &duplicateKey);
							}
							session->unlockIdxObjects(token);
						}
					} // for ����������
					session->getMemoryContext()->resetToSavepoint(savepoint);
				}
				break;
			default:
				break;
		} // switch (...)
	} // while (...)

	session->getMemoryContext()->resetToSavepoint(sp);
}


/**
 * ������쳣���֣�ɾ��tempCopyTableDef���ɵ�TableDef����ɾ���¼ӵ�����
 * @param tmpTbDef              ʹ��copyTempTableDef��õı���
 */
void TableOnlineMaintain::delTempTableDef(TableDef *tmpTbDef) {
	tmpTbDef->m_numIndice = 0;
	tmpTbDef->m_indice = NULL;
	delete tmpTbDef;
}

/**
 * ���´���ʱ���滻ԭ�����
 * @param session
 * @param origTablePath
 * @return 
 */
void TableOnlineMaintain::reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict) {
	session->getConnection()->setStatus("Replace table component");

	Table *tmpTb = NULL;
	try {
		tmpTb = Table::open(session->getNtseDb(), session, origTablePath, hasCprsDict); // �滻��ɺ�·������basePath
	} catch (NtseException &) {
		assert(false);
	}

	// ��֤table������
#ifdef NTSE_UNIT_TEST
	tmpTb->verify(session);
#endif

	// �滻�����
	m_table->replaceComponents(tmpTb);
	delete tmpTb; // ����closeֱ��ɾ
	tmpTb = NULL;
}

/** �ӱ�Ԫ������
 * @pre ����δ�������������ӱ�Ԫ�����������ڼ�������֮ǰ
 *
 * @param session �Ự
 * @param mode ��ģʽ��ֻ����S��U��X
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ
 */
void TableOnlineMaintain::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_table->lockMeta(session, mode, timeoutMs, file, line);
}

/** ������Ԫ����������oldMode��newMode��Ȼ�oldMode�Ǳ�newMode���߼��������򲻽����κβ�����
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ��ʧ�ܣ�NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void TableOnlineMaintain::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_table->upgradeMetaLock(session, oldMode, newMode, timeoutMs, file, line);
}

/** ������Ԫ����������oldMode��newMode��ȣ���newMode��oldMode�߼��򲻽����κβ���
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 */
void TableOnlineMaintain::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	m_table->downgradeMetaLock(session, oldMode, newMode, file, line);
}

/** �ͷű�Ԫ������
 * @pre �������ͷű�������֮�����
 *
 * @param session �Ự
 * @param mode ��ģʽ
 */
void TableOnlineMaintain::unlockMeta(Session *session, ILMode mode) {
	m_table->unlockMeta(session, mode);
}

void TableOnlineMaintain::enableLogging(Session *session) {
	session->enableLogging();
}

void TableOnlineMaintain::disableLogging(Session *session) {
	session->disableLogging();
}

/***************************************
 *                                     *
 * Table�к�����ά����صĽӿ�ʵ��
 *                                     *
 ***************************************/



/**
 * ��������ά��
 * @param session           �Ự
 * @param log               ��־��
 * @param db                ���ݿ�
 * @param tablepath         ��·��
 * @throw                   ���ļ����ʴ���������쳣
 */
void Table::redoAlterIndice(Session *session, const LogEntry *log, Database *db, const char *tablePath) throw (NtseException) {
	UNREFERENCED_PARAMETER(session);
	TableDef *newTbdef = NULL;
	char* relativeIdxPath = NULL;
	Table::parseAlterIndiceLog(log, &newTbdef, &relativeIdxPath);
	// ����ʱ�����ļ����ڣ����ʾ���е�һ�룬δ��������滻ʱ��������ʱ����滻
	// �����ʾ��û�п�ʼ�����滻
	string fullPath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + tablePath;
	string tempIndiceFilePath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + relativeIdxPath;
	if (File::isExist(tempIndiceFilePath.c_str())) {
		File tempIndiceFile(tempIndiceFilePath.c_str());
		string tblDefFilePath = fullPath + Limits::NAME_TBLDEF_EXT;
		newTbdef->writeFile(tblDefFilePath.c_str());
		// ����ʱ�����ļ�����Ϊ�����ļ����Ḳ���ϵ������ļ�
		string indiceFilePath = fullPath + Limits::NAME_IDX_EXT;
		tempIndiceFile.move(indiceFilePath.c_str(), true);
		delete newTbdef;
		newTbdef = NULL;
	}

	if (relativeIdxPath != NULL) {
		delete[] relativeIdxPath;
		relativeIdxPath = NULL;
	}
	
	if (newTbdef != NULL) {
		delete newTbdef;
		newTbdef = NULL;
	}
}

/**
* �����б䶯ά��
* @param session           �Ự
* @param log               ��־��
* @param db                ���ݿ�
* @param tablepath         ��·��
*/
void Table::redoAlterColumn(Session *session, const LogEntry *log, Database *db, const char *tablepath, bool *newHasDict) {
	UNREFERENCED_PARAMETER(session);
	bool hasLob = false;
	char* tmpTblPath = NULL;
	Table::parseAlterColumnLog(log, &tmpTblPath, &hasLob, newHasDict);

	string basePath(db->getConfig()->m_basedir);

	string fullPath(basePath + string(NTSE_PATH_SEP) + string(tablepath));
	// �õ���ʱ��·��
	string tmpFullPath(basePath + NTSE_PATH_SEP + tmpTblPath);

	// �滻�ļ�
	const int numFiles = Limits::EXTNUM;
	std::vector<const char *> removeFileSuffix;
	for (int i = 0; i < numFiles; ++i) {
		if (!(*newHasDict) && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, Limits::EXTS[i])) {//���û���ֵ��ļ���������
			removeFileSuffix.push_back(Limits::EXTS[i]);
			continue;
		}
		if (!hasLob && (!System::stricmp(Limits::NAME_SOBH_EXT, Limits::EXTS[i]) 
			|| !System::stricmp(Limits::NAME_SOBH_TBLDEF_EXT, Limits::EXTS[i]) 
			|| !System::stricmp(Limits::NAME_LOBI_EXT, Limits::EXTS[i]) 
			|| !System::stricmp(Limits::NAME_LOBD_EXT, Limits::EXTS[i]))) {
				removeFileSuffix.push_back(Limits::EXTS[i]);
				continue;
		}
		string oldPath = tmpFullPath + Limits::EXTS[i];
		string newPath = fullPath + Limits::EXTS[i];
		if (File::isExist(oldPath.c_str())) {
			assert((i < Limits::EXTNUM_NOLOB) ? File::isExist(newPath.c_str()) : true);
			File tbfile(oldPath.c_str());
			u64 errCode = tbfile.move(newPath.c_str(), true);
			UNREFERENCED_PARAMETER(errCode);
			assert(errCode == File::E_NO_ERROR);
		}
	}
	for (std::vector<const char *>::iterator it = removeFileSuffix.begin(); it != removeFileSuffix.end(); ++it) {
		string oldFilePath = fullPath + *it;
		File oldTbFile(oldFilePath.c_str());
		oldTbFile.remove();
	}
	if (tmpTblPath != NULL) {
		delete[] tmpTblPath;
		tmpTblPath = NULL;
	}
	// redo finish.
}

/** 
 * ���������ֵ�
 * @param session �Ự
 * @param log ��־��
 * @param db ���ݿ�
 * @param tablePath �����·�� 
 */
void Table::redoCreateDictionary(Session *session, Database *db, const char *tablePath) {
	UNREFERENCED_PARAMETER(session);
	string fullPath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + tablePath;
	
	string newDictFilePath = fullPath + Limits::NAME_GLBL_DIC_EXT;
	string tempDictFilePath = fullPath + Limits::NAME_TEMP_GLBL_DIC_EXT;

	//���ѹ���ֵ���ʱ�ļ����ڣ����ʾ����δ���ѹ��������滻������˵���滻�Ѿ���ɣ���ʱ�ļ��Ѿ���ɾ��
	File tempDictFile(tempDictFilePath.c_str());
	if (File::isExist(tempDictFilePath.c_str())) {
		//�����ֵ��ļ������޸�������ļ�ǰ��ʱ�ֵ��ļ�������ɾ��
		u64 errCode = File::copyFile(newDictFilePath.c_str(), tempDictFilePath.c_str(), true);
		UNREFERENCED_PARAMETER(errCode);
		assert(errCode == File::E_NO_ERROR);
	} else {
		//do nothing
	}
	assert(File::isExist(newDictFilePath.c_str()));
	//ɾ����ʱ�ֵ��ļ�
	u64 errCode = tempDictFile.remove();
	UNREFERENCED_PARAMETER(errCode);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR || File::getNtseError(errCode) == File::E_NOT_EXIST);
}

/**
 * ����һ��������ά��������
 * @param table				���������
 * @param conn				����
 * @param addColNum			����������
 * @param addCol			�����е�����Ϣ����
 * @param delColNum			ɾ��������
 * @param delCol			ɾ���е��ж�������
 * @param cancelFlag        ����ȡ����־
 * @param keepOldDict       �Ƿ���ԭ�ֵ�
 */
TblMntAlterColumn::TblMntAlterColumn(Table *table, Connection *conn, u16 addColNum, 
									 const AddColumnDef *addCol, u16 delColNum, 
									 const ColumnDef **delCol, bool *cancelFlag, 
									 bool keepOldDict) : TableOnlineMaintain(table, cancelFlag) {
		m_db = table->m_db;
		m_numAddCol = addColNum;
		m_numDelCol = delColNum;
		m_addCols = addCol;
		m_delCols = delCol;
		m_conn = conn;
		m_convert = NULL;
		m_keepOldDict = keepOldDict;
		m_newHasDict = false;
}

TblMntAlterColumn::~TblMntAlterColumn() {
	if (NULL != m_convert) {
		delete m_convert;
	}
	m_db = NULL;
	m_conn = NULL;
}

/**
 * �����б任ά����
 *
 * ���ԭ������Ϊѹ���������Ѿ������ֵ䣬���Ƿ����²��������µ��ֵ�ȡ����
 * ntse_keep_dict_on_optimize˽���������ã������û�д����ֵ䣬�򲻻���������ֵ䣻
 * ��û�б�����Ϊѹ�������ֵ䣬���ֵ�ᱻɾ����ԭ���ѹ����¼�ᱻ��ѹ�洢
 *
 * @param session              �Ự
 * @return                     �����ɹ���ɷ���true
 * @throw                      �޸��в������Ϸ���������ʱ������ļ������������������ֵ�ʧ�ܵ�
 */
bool TblMntAlterColumn::alterTable(Session *session) throw(NtseException) {
	MemoryContext *memContext = session->getMemoryContext();
	McSavepoint msp(memContext);

	ILMode oldMetaLock = m_table->getMetaLock(session);
	//����Ԫ����������Ҫ�����
	bool addMetaLockByMe = false;
	if (oldMetaLock == IL_NO) {
		//assert(m_table->getMetaLock(session) == IL_NO);
		lockMeta(session, IL_U, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);	// ���ܳ�ʱ��ֱ���׳��쳣
		SYNCHERE(SP_TBL_ALTCOL_AFTER_U_METALOCK);
		addMetaLockByMe = true;
	} else {
		assert(oldMetaLock == IL_U);
	}

	try {
		//Ȩ�޼��
		m_table->checkPermission(session, false);
		// ����convert�����ܻ����ɾ�������������ȵȣ�
		m_convert = new RecordConvert(m_table->getTableDef(true, session), m_addCols, 
			m_numAddCol, m_delCols, m_numDelCol);
	} catch (NtseException &e) {
		// һ������Ϊ�ж���������
		if (addMetaLockByMe) {
			unlockMeta(session, IL_U);
		}
		throw e;
	}

	// ������־
	disableLogging(session);

	TempTableDefInfo tempTableDefInfo;
	AutoPtr<TableDef> newTbdef(m_convert->getNewTableDef());

	//Ԥ�������
	preAlterTblDef(session, newTbdef, &tempTableDefInfo);

	//������ʱ��
	Table *tmpTb = createTempTable(session, newTbdef);

	// ����RIDӳ���
	NtseRidMapping *ridMap = new NtseRidMapping(m_db, m_table);
	try {
		assert(session->getConnection() == m_conn);
		ridMap->init(session);
	} catch (NtseException &e) {
		if (addMetaLockByMe) {
			unlockMeta(session, IL_U);
		}
		delete ridMap;
		enableLogging(session);
		throw e;
	}
	AutoPtr<NtseRidMapping> ridMapPtr(ridMap);

	assert(m_table->getMetaLock(session) == IL_U);
	while (true) {
		try {
			upgradeMetaLock(session, IL_U, IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &) {}
	}
	// �ڳ���meta lock X������£���ζ�������ڸñ��ntse�����Ѿ�ȫ���ύ
	// ��ó�ʼlsn������onlinelsn
	int olLsnHdl = INVALID_TOKEN;
	u64 lsnStart = INVALID_LSN;
	lsnStart = getLsnPoint(session, true, true, &olLsnHdl);
	downgradeMetaLock(session, IL_X, IL_U, __FILE__, __LINE__);
	SYNCHERE(SP_TBL_ALTCOL_BEFORE_GET_LSNSTART);

	SYNCHERE(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);


	//����
	string basePath = string(m_db->getConfig()->m_basedir);
	string origTablePath = string(m_table->getPath());
	string origTableFullPath = basePath + NTSE_PATH_SEP + origTablePath;
	string tmpTablePath(tmpTb->getPath());
	string tmpTableFullPath = basePath + NTSE_PATH_SEP + tmpTablePath;

	try {
		// ������
		copyTable(session, tmpTb, ridMap, newTbdef);

		// �ط���־����������
		lsnStart = replayLogWithoutIndex(session, tmpTb, ridMap, lsnStart, &olLsnHdl);

		// ������������Ψһ��
		rebuildIndex(session, tmpTb, &tempTableDefInfo);

		// �ط���־������������Ψһ�ԣ�
		lsnStart = replayLogWithIndex(session, tmpTb, ridMap, lsnStart, &olLsnHdl);

		//������Ԫ�������ͱ���
		upgradeTblLock(session);

		//TODO: ��֤������ȷ��

		//��ԭ��������
		restoreTblSetting(tmpTb, &tempTableDefInfo, newTbdef);

		// ��¼bumpLSN
		u16 tableid = m_table->getTableDef(true, session)->m_id;

		enableLogging(session); //����־��¼
		//m_db->bumpFlushLsn(session, tableid);
		//��ʱ�����ڸñ�������Ѿ�ȫ���ύ
		m_db->bumpTntAndNtseFlushLsn(session, tableid);

		tmpTb->setTableId(session, tableid);

		assert(!tmpTb->getMmsTable());

		additionalAlterColumn(session, ridMap);

		// ��¼��־
		bool tmpTbleHasLob = tmpTb->getTableDef()->hasLob();
		m_db->getTxnlog()->flush(m_table->writeAlterColumnLog(session, tmpTb->getPath(), 
			tmpTbleHasLob, isNewTbleHasDict()));
		SYNCHERE(SP_MNT_ALTERCOLUMN_JUST_WRITE_LOG);

		m_table->close(session, false, true);
		tmpTb->close(session, true);
		delete tmpTb;
		tmpTb = NULL;

		// �滻�ļ�
		replaceTableFile(tmpTbleHasLob, origTableFullPath, tmpTableFullPath);

		SYNCHERE(SP_MNT_ALTERCOLUMN_TABLE_REPLACED);

		// ���´򿪱��滻�����
		reopenTblAndReplaceComponent(session, origTablePath.c_str(), isNewTbleHasDict());

	} catch (NtseException &e) {
		m_db->getTxnlog()->clearOnlineLsn(olLsnHdl);

		tmpTb->close(session, false, true);
		Table::drop(m_db, tmpTablePath.c_str());

		ILMode tbLockMode = m_table->getLock(session);
		if (tbLockMode != IL_NO) { 
			m_table->unlock(session, tbLockMode);
		}
		if (addMetaLockByMe) {
			unlockMeta(session, m_table->getMetaLock(session));
		}
		enableLogging(session);
		throw e;
	}

	// �ͷű���
	assert(session->getTrans() != NULL || m_table->getLock(session) == IL_X);
	assert(m_table->getMetaLock(session) == IL_X);
	if (!session->getTrans()) {
		m_table->unlock(session, IL_X);
	}
	if (addMetaLockByMe) {
		unlockMeta(session, IL_X);
	} else {
		downgradeMetaLock(session, IL_X, IL_U, __FILE__, __LINE__);
	}

	// �ͷ���Դ
	m_db->getControlFile()->releaseTempTableId(newTbdef->m_id);

	m_conn->setStatus("Done");

	SYNCHERE(SP_MNT_ALTERCOLUMN_FINISH);
	return true;
}

/**
 * ������Ԫ������
 * @param session
 */
void TblMntAlterColumn::upgradeTblMetaLock(Session *session) {
	m_conn->setStatus("Upgrade table meta lock");
	assert(m_table->getMetaLock(session) == IL_U);
	// X����Ԫ��������������
	SYNCHERE(SP_MNT_ALTERCOLUMN_BEFORE_X_METALOCK);
	while (true) {
		try {
			upgradeMetaLock(session, IL_U, IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &e) {
			SYNCHERE(SP_MNT_ALTERCOLUMN_X_METALOCK_FAIL);
			UNREFERENCED_PARAMETER(e);
		}
	}
	assert(m_table->getMetaLock(session) == IL_X);
}

/**
 * ��������
 * @param session
 */
void TblMntAlterColumn::upgradeTblLock(Session *session) {
	m_conn->setStatus("Upgrade table lock");

	//���������δʵ����������
	if (NULL != session->getTrans()) {
		return;
	}

	assert(m_table->getLock(session) == IL_S);
	while (true) {
		try {
			m_table->upgradeLock(session, IL_S, IL_X, 
				m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
		}
	}
	assert(m_table->getLock(session) == IL_X);
}

/**
 * Ԥ�ȸ����±�Ķ���(�ÿ�������)�����������õ���Ϣ�������������������֮��ָ�����ʹ��
 * @param session
 * @param newTbdef
 * @param tempTableDefInfo
 * @return 
 */
TableDef* TblMntAlterColumn::preAlterTblDef(Session *session, TableDef *newTbdef, 
											TempTableDefInfo *tempTableDefInfo) {
	assert(NULL != newTbdef);

	try {
		// �ı�Table ID����������ͻ
		newTbdef->m_id = m_db->getControlFile()->allocTempTableId();
	} catch (NtseException &) {
		assert(false);
	}

	// ��indice��������������ʱ���á�
	tempTableDefInfo->m_indexDef = newTbdef->m_indice;
	tempTableDefInfo->m_indexNum = newTbdef->m_numIndice;
	tempTableDefInfo->m_newTbpkey = newTbdef->m_pkey;
	tempTableDefInfo->m_newTbUseMms = newTbdef->m_useMms;

	// �ÿ�����
	newTbdef->m_indice = NULL;
	newTbdef->m_pkey = NULL;
	newTbdef->m_numIndice = 0;

	// �ر�mms
	newTbdef->m_useMms = false;
	for (u16 col = 0; col < newTbdef->m_numCols; ++col) {
		// ���ֵ�ں���addIndex��ʱ����Զ��ָ�������Ҫ��¼
		newTbdef->m_columns[col]->m_inIndex = false;
	}

	// ��¼cacheUpdate��Ϣ�������ÿ�cacheUpdate
	tempTableDefInfo->m_cacheUpdateCol = (bool *)session->getMemoryContext()->alloc(sizeof(bool) * newTbdef->m_numCols);
	for (u16 i = 0; i < newTbdef->m_numCols; ++i) {
		tempTableDefInfo->m_cacheUpdateCol[i] = newTbdef->m_columns[i]->m_cacheUpdate;
		newTbdef->m_columns[i]->m_cacheUpdate = false;
	}

	//��ֹcacheUpdate
	newTbdef->m_cacheUpdate = false;

	return newTbdef;
}

/**
 * ������ʱ��
 * @param session
 * @param tempTableDefInfo
 * @param newTbdef
 * @return 
 */
Table* TblMntAlterColumn::createTempTable(Session *session, TableDef *newTbdef) throw(NtseException) {
	// �õ�����·��
	string tablePath(m_table->getPath());
	string basePath(m_db->getConfig()->m_basedir);
	string tableFullPath = basePath + NTSE_PATH_SEP + tablePath;

	u64 lsn = m_table->m_db->getTxnlog()->tailLsn();
	char* lsnBuffer = (char*)alloca(Limits::LSN_BUFFER_SIZE*sizeof(char));
	sprintf(lsnBuffer, I64FORMAT"u", lsn);
	// �õ���ʱ��·��
	string tmpTablePath = tablePath + Limits::NAME_TEMP_TABLE + lsnBuffer; // �����baseDir��·��
	string tmpTableFullPath = basePath + NTSE_PATH_SEP + tmpTablePath;

	// ������ʱ��
	// ɾ������ʱ���ļ�
	for (int i = 0; i < Limits::EXTNUM; ++i) {
		string tmpfilename = tmpTableFullPath + Limits::EXTS[i];
		File file2remove(tmpfilename.c_str());
		file2remove.remove();
	}

	bool tblCreated = false;
	Table *tmpTb;
	try {
		Table::create(m_db, session, tmpTablePath.c_str(), newTbdef);
		tblCreated = true;
		SYNCHERE(SP_MNT_ALTERCOLUMN_TMPTABLE_CREATED);
		tmpTb = Table::open(m_db, session, tmpTablePath.c_str(), false);
	} catch (NtseException &e) {
		unlockMeta(session, IL_U);
		delete newTbdef;
		if (tblCreated)
			Table::drop(m_db, tmpTablePath.c_str());
		throw e;
	}

	return tmpTb;
}

/**
 * ����
 * @param session
 * @param tmpTb
 * @param ridmap
 * @param newTbdef
 */
void TblMntAlterColumn::copyTable(Session *session, Table *tmpTb, NtseRidMapping *ridmap, 
								  TableDef *newTbdef) throw(NtseException) {
	assert(m_table->getMetaLock(session) == IL_U);
	assert(m_table->getLock(session) == IL_NO);

	// ��ʼ����װ��
	ridmap->beginBatchLoad();

	m_conn->setStatus("Acquire table lock");

	SYNCHERE(SP_MNT_ALTERCOLUMN_BEFORE_SCAN_IS_LOCK);
	do {
		try {
			m_table->lock(session, IL_IS, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &e) {
			SYNCHERE(SP_MNT_ALTERCOLUMN_SCAN_IS_LOCK_FAIL);
			UNREFERENCED_PARAMETER(e);
		}
	} while (true);
	SYNCHERE(SP_MNT_ALTERCOLUMN_BEFORE_BEGINSCAN);

	try {
		assert(session->getTrans() != NULL || m_table->getLock(session) == IL_IS);

		//����&�����ֵ䣬�滻��ʱ��ѹ��������ֵ��ļ�
		m_newHasDict = newTbdef->m_isCompressedTbl && m_table->hasCompressDict();//�±��Ƿ����ֵ�	
		if (m_newHasDict) {
			createOrCopyDictForTmpTbl(session, m_table, tmpTb, m_keepOldDict);
		}

		m_conn->setStatus("Copying table");
	
		//ͨ����ɨ�����ԭ���¼����ʱ��Ķ���
		copyRowsByScan(session, tmpTb, ridmap);
	} catch (NtseException &e) {
		assert(session->getTrans() != NULL || m_table->getLock(session) == IL_IS);
		m_table->unlock(session, IL_IS);
		throw e;
	}

	m_table->unlock(session, IL_IS);
	ridmap->endBatchLoad();
}

/**
 * ͨ����ɨ�����ԭ���¼����ʱ��Ķ���
 * @param session
 * @param tmpTb
 * @param ridmap
 */
void TblMntAlterColumn::copyRowsByScan(Session *session, Table *tmpTb, 
									   NtseRidMapping *ridmap) throw(NtseException) {
	MemoryContext *mtx = session->getMemoryContext();
	
	bool connAccuScan = session->getConnection()->getLocalConfig()->m_accurateTblScan;
	session->getConnection()->getLocalConfig()->m_accurateTblScan = true;

	// ��ʱ��session�е�������ΪNULL����ԭ���¼ʱ��Ҫ��ȡ�����
	TNTTransaction *trx = session->getTrans();
	session->setTrans(NULL);

	//��¼��TableDef����
	u16 *columns = (u16 *)mtx->alloc(sizeof(u16) * m_table->getTableDef(true, session)->m_numCols);
	for (u16 i = 0; i < m_table->m_tableDef->m_numCols; ++i) {
		columns[i] = i;
	}

	// ���ɲ�����ʱ��ĸ���session�����б�����
	Session *tmpSess = m_db->getSessionManager()->allocSession("TblMntAlterColumn::alterTable", m_conn);
	tmpSess->disableLogging(); // ������־

	TblScan *scanHdl = m_table->tableScan(session, OP_READ, 
		m_table->getTableDef(true, session)->m_numCols, columns, false); //���ӱ������������쳣

	// ��ʼ��ɨ��
	RowId mapped;
	uint dupIdx;
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)mtx->alloc(m_table->m_tableDef->m_maxRecSize), 
		m_table->m_tableDef->m_maxRecSize);

	try {
		while (m_table->getNext(scanHdl, mysqlRec.m_data) && !isCancel()) {
			u64 sp = tmpSess->getMemoryContext()->setSavepoint();
			byte *convertedRec = m_convert->convertMysqlOrRedRec(&mysqlRec, tmpSess->getMemoryContext());
			mapped = tmpTb->insert(tmpSess, convertedRec, true, &dupIdx);
			assert(mapped != INVALID_ROW_ID);
			ridmap->insertMapping(scanHdl->getCurrentRid(), mapped);
			tmpSess->getMemoryContext()->resetToSavepoint(sp);
		}
	} catch (NtseException &) {
		//��ʱ���������������Բ�����Ψһ�Գ�ͻ
		assert(false);
	}

	m_table->endScan(scanHdl);
	session->getConnection()->getLocalConfig()->m_accurateTblScan = connAccuScan;

	m_db->getSessionManager()->freeSession(tmpSess);
	tmpSess = NULL;

	session->setTrans(trx);

	if (isCancel()) {
		NTSE_THROW(NTSE_EC_CANCELED, "Table maintain operation on table %s is canceled", m_table->getPath());
	}
}

/**
 * ������������־�ط�
 * @param session
 * @param tmpTb
 * @param ridmap
 * @param lsnstart
 * @param olLsnHdl
 * @return 
 */
LsnType TblMntAlterColumn::replayLogWithoutIndex(Session *session, Table *tmpTb, NtseRidMapping *ridMap, 
											  LsnType lsnStart, int *olLsnHdl) {
	SYNCHERE(SP_TBL_ALTCOL_BEFORE_GET_LSNEND);

	m_conn->setStatus("Replay log without indice");

	// ��¼�طŽ�����־
	LsnType lsnEnd = getLsnPoint(session, true, false);

	// ����lsnstart��lsnend֮����־���лطţ���ʱ��������
	LogReplay replay(m_table, m_db, lsnStart, lsnEnd);
	replay.start();

	m_db->getSyslog()->log(EL_DEBUG, "start processing log.");

	processLogToTable(session, &replay, m_convert, tmpTb, ridMap);
	//��ʱ���ܻ�����δ��ɵ����񣬵�replay end�������󣬻Ὣδ��ɵ���������Ҳ�ͷŵ�
	//���Ա����¼��Сδ��������begin trx lsn
	LsnType minUnfinishTxnLsn = replay.getMinUnfinishTxnLsn();
	if (minUnfinishTxnLsn != INVALID_LSN) {
		assert(minUnfinishTxnLsn <= lsnEnd);
		lsnEnd = minUnfinishTxnLsn;
	}
	replay.end();

	// �ͷ�֮ǰ����־
	lsnStart = lsnEnd;
	int oldOlLsnHdl = *olLsnHdl;
	*olLsnHdl = m_db->getTxnlog()->setOnlineLsn(lsnStart);
	assert(olLsnHdl >= 0);
	m_db->getTxnlog()->clearOnlineLsn(oldOlLsnHdl);

	return lsnEnd;
}

/**
 * ����������־�ط�
 * @post ����ʱ���Ѿ����ϱ�IL_S��
 *
 * @param session
 * @param tmpTb
 * @param convert
 * @param ridmap
 * @param lsnstart
 * @param olLsnHdl
 * @return 
 */
LsnType TblMntAlterColumn::replayLogWithIndex(Session *session, Table *tmpTb, NtseRidMapping *ridMap, 
										  LsnType lsnStart, int *olLsnHdl) {

	m_conn->setStatus("Replay log with indice");

	u32 time1 = 0;
	u32 time2 = 0;
	int loop = 0;
	bool startFlush = false;
	LsnType lsnEnd = 0;

	do {
		time1 = System::fastTime();
		// ˢ������
		if (startFlush) {
			m_table->flush(session);
			tmpTb->flush(session);
		}
		// �ٴμ�¼lsn
		lsnEnd = getLsnPoint(session, true, false);
		// �ط�start��end�����־
		LogReplay replayAgain(m_table, m_db, lsnStart, lsnEnd);
		replayAgain.start();
		processLogToTable(session, &replayAgain, m_convert, tmpTb, ridMap);

		//��ʱ���ܻ�����δ��ɵ����񣬵�replay end�������󣬻Ὣδ��ɵ���������Ҳ�ͷŵ�
		//���Ա����¼��Сδ��������begin trx lsn
		LsnType minUnfinishTxnLsn = replayAgain.getMinUnfinishTxnLsn();
		if (minUnfinishTxnLsn != INVALID_LSN) {
			assert(minUnfinishTxnLsn <= lsnEnd);
			lsnEnd = minUnfinishTxnLsn;
		}

		replayAgain.end();
		time2 = System::fastTime();
		// ����lsnstart���ͷ��Ѿ��طŵ���־
		lsnStart = lsnEnd;
		int oldOlLsnHdl = *olLsnHdl;
		*olLsnHdl = m_db->getTxnlog()->setOnlineLsn(lsnStart);
		assert(*olLsnHdl >= 0);
		m_db->getTxnlog()->clearOnlineLsn(oldOlLsnHdl);

		m_db->getSyslog()->log(EL_DEBUG, "time2 - time1 = %d, loop count is %d.", time2 - time1, loop);

		++loop;
		if (!startFlush && (loop >= 4 || time2 - time1 <= 15)) {
			startFlush = true;
			continue;
		}
		SYNCHERE(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);
		if (startFlush && (loop >= 6 || time2 - time1 <= 15)) {
			ILMode oldMode = m_table->getMetaLock(session);
			upgradeTblMetaLock(session);
			TableStatus oldTblStatus = m_table->getTableDef()->getTableStatus();
			assert(oldTblStatus == TS_TRX || oldTblStatus == TS_NON_TRX);
			m_table->getTableDef()->setTableStatus(TS_ONLINE_DDL);
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMode, __FILE__, __LINE__);

			preLockTable();

			while (true) {
				try {
					if (!session->getTrans()) {
						m_table->lock(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
					} else {
						session->getTrans()->lockTable(TL_X, m_table->getTableDef()->m_id);
					}
					break;
				} catch (NtseException &e) {
					SYNCHERE(SP_MNT_ALTERCOLUMN_S_LOCKTABLE_FAIL);
					UNREFERENCED_PARAMETER(e);
				}
			}

			upgradeTblMetaLock(session);
			m_table->getTableDef()->setTableStatus(oldTblStatus);
			break;
		}
	} while(true);

	// ���ط�
	assert(session->getTrans() != NULL || m_table->getLock(session) == IL_S);
	assert(m_table->getMetaLock(session) >= IL_U);
	m_table->flush(session);
	lsnEnd = getLsnPoint(session, false, false);
	LogReplay replayFinal(m_table, m_db, lsnStart, lsnEnd);
	replayFinal.start();
	processLogToTable(session, &replayFinal, m_convert, tmpTb, ridMap);
	//��ʱ���벻����δ��ɵ�ntse����
	assert(replayFinal.getMinUnfinishTxnLsn() == INVALID_LSN);
	replayFinal.end();
	tmpTb->flush(session);

	// ����onlineLSN�ָ�
	m_db->getTxnlog()->clearOnlineLsn(*olLsnHdl);
	*olLsnHdl = INVALID_TOKEN;

	return lsnEnd;
}

/**
 * ���ļ��滻
 * @param tmpTbleHasLob
 * @param origTableFullPath
 * @param tmpTableFullPath
 */
void TblMntAlterColumn::replaceTableFile(bool tmpTbleHasLob, string &origTableFullPath, 
										 string &tmpTableFullPath) {
	m_conn->setStatus("Replace table files");

	// �滻�ļ�
	int numTmpFiles = Limits::EXTNUM;//��ʱ�ļ���Ŀ
	std::vector<const char *> removeFileSuffix;
	for (int i = 0; i < numTmpFiles; ++i) {
		if (!isNewTbleHasDict() && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, Limits::EXTS[i])) {//���û���ֵ��ļ���������
			removeFileSuffix.push_back(Limits::EXTS[i]);
			continue;
		}
		if (!tmpTbleHasLob 
			&& (!System::stricmp(Limits::NAME_SOBH_EXT, Limits::EXTS[i]) 
			|| !System::stricmp(Limits::NAME_SOBH_TBLDEF_EXT, Limits::EXTS[i]) 
			|| !System::stricmp(Limits::NAME_LOBI_EXT, Limits::EXTS[i]) 
			|| !System::stricmp(Limits::NAME_LOBD_EXT, Limits::EXTS[i]))) {
				removeFileSuffix.push_back(Limits::EXTS[i]);
				continue;
		}
		string oldPath = tmpTableFullPath + Limits::EXTS[i];
		string newPath = origTableFullPath + Limits::EXTS[i];
		assert(File::isExist(oldPath.c_str()));
		File tbfile(oldPath.c_str());
		u64 errCode = tbfile.move(newPath.c_str(), true);
		UNREFERENCED_PARAMETER(errCode);
		assert(errCode == File::E_NO_ERROR);
	}

	for (std::vector<const char *>::iterator it = removeFileSuffix.begin(); it != removeFileSuffix.end(); ++it) {
		string oldPath = origTableFullPath + *it;
		File oldTbFile(oldPath.c_str());
		u64 errCode = oldTbFile.remove();
		UNREFERENCED_PARAMETER(errCode);
		//assert(errCode == File::E_NO_ERROR);
	}
}

/**
 * �����ؽ�
 * @param session
 * @param tmpTb
 * @param tempInfo
 * @param convert
 * @param ridmap 
 */
void TblMntAlterColumn::rebuildIndex(Session *session, Table *tmpTb, 
									 TempTableDefInfo *tempInfo) throw(NtseException) {
	m_conn->setStatus("Creating indice");
	m_db->getSyslog()->log(EL_DEBUG, "start creating indice.");

	tempInfo->m_indexUniqueMem = (int *)session->getMemoryContext()->alloc(sizeof(int) * tempInfo->m_indexNum);
	u64 sp = session->getMemoryContext()->setSavepoint();

	u16 newTbNumIndice = tempInfo->m_indexNum;
	IndexDef **newTbIndexDef = tempInfo->m_indexDef;
	int *indexUniqueMem = tempInfo->m_indexUniqueMem;

	for (u8 i = 0; i < newTbNumIndice && !isCancel(); ++i) {
		IndexDef *newDef = newTbIndexDef[i];
		if (newDef->m_unique) {
			if (newDef->m_primaryKey) {
				indexUniqueMem[i] = 2;
				newDef->m_primaryKey = false;
			} else {
				indexUniqueMem[i] = 1;
			}
			newDef->m_unique = false;
		} else {
			assert(!newDef->m_primaryKey);
			indexUniqueMem[i] = 0;
		}
		try {
			newDef->check(tmpTb->m_tableDef);
			tmpTb->m_indice->createIndexPhaseOne(session, newDef, tmpTb->getTableDef(), tmpTb->getHeap());
			tmpTb->m_indice->createIndexPhaseTwo(newDef);
			tmpTb->m_tableDef->addIndex(newDef);
			tmpTb->writeTableDef();
		} catch (NtseException &e) {
			session->getMemoryContext()->resetToSavepoint(sp);
			throw e;
		}
	}

	session->getMemoryContext()->resetToSavepoint(sp);

	if (isCancel()) {
		NTSE_THROW(NTSE_EC_CANCELED, "Table maintain operation on table %s is canceled", m_table->getPath());
	}
}

/**
 * ��ԭ��������
 * @param tmpTb
 * @param tempInfo
 * @param newTbdef
 */
void TblMntAlterColumn::restoreTblSetting(Table *tmpTb, const TempTableDefInfo *tempInfo, 
										  TableDef *newTbdef) {
    assert(NULL != tempInfo->m_indexUniqueMem);

	m_conn->setStatus("Restore table setting");

	// �ָ�������Ψһ��Լ��
	u16 newTbNumIndice = tempInfo->m_indexNum;
	for (u8 i = 0; i < newTbNumIndice; ++i) {
		switch (tempInfo->m_indexUniqueMem[i]) {
			case 2:
				tmpTb->m_tableDef->m_indice[i]->m_primaryKey = true;
				tmpTb->m_tableDef->m_pkey = tmpTb->m_tableDef->m_indice[i];
			case 1:
				tmpTb->m_tableDef->m_indice[i]->m_unique = true;
			case 0:
				break;
		}
	}
	// �ָ��е�cacheUpdate
	assert(newTbdef->m_numCols == tmpTb->m_tableDef->m_numCols);
	tmpTb->m_tableDef->m_cacheUpdate = false;
	bool *cacheUpdateCol = tempInfo->m_cacheUpdateCol;
	for (u16 i = 0; i < newTbdef->m_numCols; ++i) {
		tmpTb->m_tableDef->m_columns[i]->m_cacheUpdate = cacheUpdateCol[i];
		if (cacheUpdateCol[i])
			tmpTb->m_tableDef->m_cacheUpdate = true;
	}

	// �ָ�mms����
	tmpTb->m_tableDef->m_useMms = tempInfo->m_newTbUseMms;
}

/**
 * ������ʱ����ֵ�(���������´���)
 * @param session
 * @param origTb
 * @param tmpTb
 * @param onlyCopy
 */
void TblMntAlterColumn::createOrCopyDictForTmpTbl(Session *session, Table *origTb, Table *tmpTb, 
												  bool onlyCopy) throw(NtseException) {
    m_conn->setStatus("Creating compress dictionary");

	string basePath(m_db->getConfig()->m_basedir);
	string tmpTableFullPath = basePath + NTSE_PATH_SEP + tmpTb->getPath();
	RCDictionary *newDic = NULL;

	try {
		if (onlyCopy) {//�������ֵ�
			assert(m_table->hasCompressDict());
			assert(origTb->m_records->hasValidDictionary());
			newDic = RCDictionary::copy(origTb->m_records->getDictionary(), tmpTb->getTableDef()->m_id);
		} else {//�������ֵ�
			newDic = createNewDictionary(session, m_db, origTb);
		}

		if (NULL != newDic) {
			string tempDictFilePath = tmpTableFullPath + Limits::NAME_TEMP_GLBL_DIC_EXT;
			tmpTb->createTmpCompressDictFile(tempDictFilePath.c_str(), newDic);

			tmpTb->m_records->resetCompressComponent(tmpTableFullPath.c_str());
			newDic->close();
			delete newDic;
			newDic = NULL;

			//ɾ����ʱ�ֵ��ļ�
			File tempDictFile(tempDictFilePath.c_str());
			u64 fileErrCode = tempDictFile.remove();
			UNREFERENCED_PARAMETER(fileErrCode);
			assert(File::getNtseError(fileErrCode) == File::E_NO_ERROR);
		}
	} catch (NtseException &e) {
		if (newDic) {
			newDic->close();
			delete newDic;
			newDic = NULL;
		}
		throw e;
	}
	assert(NULL == newDic);
}

/** 
 * �����������ֵ�
 * @param session       �Ự
 * @param db            ���������ݿ�
 * @param table         Ҫ�����ı�
 * @return              ���ֵ�
 * @throw NtseException ����ʧ��
 */
RCDictionary* TblMntAlterColumn::createNewDictionary(Session *session, Database *db, 
													 Table *table) throw (NtseException) {
	McSavepoint savePoint(session->getMemoryContext());
	RCMSampleTbl *sampleTblHdl = new RCMSampleTbl(session, db, table->getTableDef(true, session), 
		table->m_records, m_convert);

	RCDictionary *newDic = NULL;
	try {
		SmpTblErrCode errCode = sampleTblHdl->beginSampleTbl();
		sampleTblHdl->endSampleTbl();

		if (errCode == SMP_NO_ERR) {
			newDic = sampleTblHdl->createDictionary(session);
		} else {
			NTSE_THROW(NTSE_EC_GENERIC, "Failed to sample table and create a new dictionary " \
				"when altered table columns, %s!", SmplTblErrMsg[errCode]); 
		}
		delete sampleTblHdl;
		sampleTblHdl = NULL;
		assert(newDic);
	} catch (NtseException &e) {
		delete sampleTblHdl;
		sampleTblHdl = NULL;
		if (newDic) {
			newDic->close();
			delete newDic;
			newDic = NULL;
		}
		throw e;
	}
	delete sampleTblHdl;
	sampleTblHdl = NULL;

	return newDic;
}

/**
 * �±��Ƿ����ֵ��ļ�
 * @return 
 */
bool TblMntAlterColumn::isNewTbleHasDict() const {
	return m_newHasDict;
}

/**
 * ���Ķ����Ͳ������캯��
 * @param table Ҫ�����ı�
 * @param conn  ����
 */
TblMntAlterHeapType::TblMntAlterHeapType(Table *table, Connection *conn, bool *cancelFlag) 
	: TblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag) {
}

TblMntAlterHeapType::~TblMntAlterHeapType() {
}

/**
 * ����TblMntAlterColumn::preAlterTblDef, ���¶�����
 * @param session
 * @param newTbdef
 * @param tempTableDefInfo
 * @return 
 */
TableDef* TblMntAlterHeapType::preAlterTblDef(Session *session, TableDef *newTbdef, 
											  TempTableDefInfo *tempTableDefInfo) {
	assert(newTbdef);
	assert(tempTableDefInfo);

	TblMntAlterColumn::preAlterTblDef(session, newTbdef, tempTableDefInfo);
	
	//���Ķ�����
	assert(!newTbdef->m_isCompressedTbl);
	assert(newTbdef->m_recFormat == REC_FIXLEN && newTbdef->m_origRecFormat == REC_FIXLEN);
	newTbdef->m_recFormat = REC_VARLEN;
	newTbdef->m_fixLen = false;

	return newTbdef;
}

/**
 * TblMntOptimizer���캯��
 * @param table Ҫ�����ı�
 * @param conn  ���ݿ�����
 * @param cancelFlag ����ȡ����־
 * @param keepOldDict �Ƿ���ԭ�ֵ�
 */
TblMntOptimizer::TblMntOptimizer(Table *table, Connection *conn, bool *cancelFlag, bool keepOldDict) 
		: TblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag, keepOldDict) {
}

TblMntOptimizer::~TblMntOptimizer() {
}

/**
 * ����ntse������ʵ�ֵ�RowIdӳ���
 * @param db ���ݿ�
 * @param session �Ự
 * @param table Դ���ݱ�
 * @throw NtseException ��ʱ��������
 */
NtseRidMapping::NtseRidMapping(Database *db, Table *table) {
	m_db = db;
	m_table = table;
	m_key = m_subRec = NULL;
	m_redKey = m_rec = NULL;

	m_mapPath = new string(string(m_table->getPath()) + ".mapping");
	m_scanHdl = NULL;
	m_map = NULL;
	m_session = NULL;
}

/** �������� */
NtseRidMapping::~NtseRidMapping() {
	u16 tblId = TableDef::INVALID_TABLEID;

	if (m_map != NULL) {
		try {
			tblId = m_map->getTableDef()->m_id;
			m_map->close(m_session, false);
			delete m_map;
			Table::drop(m_db, m_mapPath->c_str());
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
		}
		m_db->getControlFile()->releaseTempTableId(tblId);
	}

	if (m_session != NULL) {
		assert(!m_session->isLogging());
		m_db->getSessionManager()->freeSession(m_session);
	}

	if (m_key != NULL)
		freeSubRecord(m_key);

	if (m_subRec != NULL)
		freeSubRecord(m_subRec);

	if (m_redKey != NULL)
		freeRecord(m_redKey);

	if (m_rec != NULL)
		freeRecord(m_rec);

	delete m_mapPath;
}

void NtseRidMapping::init(Session *session) throw(NtseException) {
	TableDefBuilder *builder;
	TableDef *mapTbDef;

	string ridMapName = string(m_table->getTableDef(true, session)->m_name) + ".mapping";
	u16 tempTid = m_db->getControlFile()->allocTempTableId();
	builder = new TableDefBuilder(tempTid, m_table->getTableDef(true, session)->m_schemaName, ridMapName.c_str());
	builder->addColumn("OLDRID", CT_BIGINT, false);
	builder->addColumn("NEWRID", CT_BIGINT, false);
	// ��ʱ����������
	mapTbDef = builder->getTableDef();
	assert(mapTbDef->m_id == tempTid);
	mapTbDef->m_useMms = false;
	// ɾ���ϵ���ʱ������еĻ���
	Session *session2 = NULL;
	try {
		session2 = m_db->getSessionManager()->allocSession(__FUNC__, session->getConnection());
		Table::drop(m_db, m_mapPath->c_str());
		m_db->getSessionManager()->freeSession(session2);
	} catch (NtseException &e) {
		delete mapTbDef;
		delete builder;
		m_db->getSessionManager()->freeSession(session2);
		throw e;
	}

	string heapFilePath = *m_mapPath + Limits::NAME_HEAP_EXT;
	File mapHeapFile(heapFilePath.c_str());
	mapHeapFile.remove();
	string indexFilePath = *m_mapPath + Limits::NAME_IDX_EXT;
	File mapIdxFile(indexFilePath.c_str());
	mapIdxFile.remove();

	// �����µ���ʱ��
	m_conn = session->getConnection();
	m_session = m_db->getSessionManager()->allocSession("NtseRidMapping::NtseRidMapping", m_conn);
	m_session->disableLogging();
	try {
		Table::create(m_db, m_session, m_mapPath->c_str(), mapTbDef);
	} catch (NtseException &e) {
		m_db->getSessionManager()->freeSession(m_session);
		delete mapTbDef;
		delete builder;
		throw e;
	}
	// �򿪱�
	try {
		m_map = Table::open(m_db, m_session, m_mapPath->c_str(), false);
	} catch (NtseException &e) {
		m_db->getSyslog()->log(EL_PANIC, "%s", e.getMessage());
		assert(false);
	}

	m_db->getSessionManager()->freeSession(m_session);
	m_session = NULL;
	delete mapTbDef;
	delete builder;

	m_session = m_db->getSessionManager()->allocSession("NtseRidMapping::NtseRidMapping", m_conn);
	m_session->disableLogging();
}

/**
 * ��ʼ�������������ݣ�������������ʱ�����¼�����������������ٶ���ԽϿ�
 */
void NtseRidMapping::beginBatchLoad() {
	RecordBuilder redKeyBuilder(m_map->getTableDef(), INVALID_ROW_ID, REC_REDUNDANT);
	redKeyBuilder.appendBigInt(INVALID_ROW_ID)->appendBigInt(INVALID_ROW_ID);
	m_redKey = redKeyBuilder.getRecord(Limits::DEF_MAX_REC_SIZE);

	SubRecordBuilder keyBuilder(m_map->getTableDef(), KEY_PAD);
	u64 oldRid = INVALID_ROW_ID;
	m_key = keyBuilder.createSubRecordByName("OLDRID", &oldRid);
	m_key->m_rowId = INVALID_ROW_ID;

	SubRecordBuilder srb(m_map->getTableDef(), REC_REDUNDANT);
	m_subRec = srb.createEmptySbByName(m_map->getTableDef()->m_maxRecSize, "OLDRID NEWRID");

	RecordBuilder rb(m_map->getTableDef(), INVALID_ROW_ID, REC_MYSQL);
	rb.appendBigInt(INVALID_ROW_ID);
	rb.appendBigInt(INVALID_ROW_ID);
	m_rec = rb.getRecord();

}

/**
 * ��������������ݣ���������
 */
void NtseRidMapping::endBatchLoad() {
	u64 sp = m_session->getMemoryContext()->setSavepoint();
	u16 numCols = 1;
	ColumnDef **columns = new ColumnDef *[1];
	columns[0] = new ColumnDef(m_map->getTableDef()->m_columns[0]);
	u32 prefixLens[1] = {0};
	IndexDef index("OID", numCols, columns, prefixLens, true, true);
	assert(!m_map->getTableDef()->m_useMms);
	try {
		m_map->getIndice()->createIndexPhaseOne(m_session, &index, m_map->getTableDef(), m_map->getHeap());
		m_map->getIndice()->createIndexPhaseTwo(&index);
		m_map->getTableDef()->addIndex(&index);
		m_map->writeTableDef();
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
	}
	m_session->getMemoryContext()->resetToSavepoint(sp);

	delete columns[0];
	delete [] columns;
}

/**
 * @see SimpleRidMapping::getMapping
 */
RowId NtseRidMapping::getMapping(RowId origRid) {
	assert(!m_session->isLogging());
	u64 sp = m_session->getMemoryContext()->setSavepoint();
	TableDef *tbDef = m_map->getTableDef();
	IndexDef *idxDef = m_map->getTableDef()->m_indice[0];

	RedRecord::writeNumber(m_map->getTableDef(), 0, m_redKey->m_data, (u64)origRid);

	// ӳ������д����
	RecordOper::extractKeyRP(tbDef, idxDef, m_redKey, NULL, m_key);


	IndexScanCond cond(0, m_key, true, true, true);
	TblScan *scanHdl = m_map->indexScan(m_session, OP_READ, &cond, m_subRec->m_numCols, m_subRec->m_columns);

	bool exist = m_map->getNext(scanHdl, m_subRec->m_data);

	m_map->endScan(scanHdl);

	m_session->getMemoryContext()->resetToSavepoint(sp);

	if (exist) {
		return (RowId)RedRecord::readBigInt(m_map->getTableDef(), m_subRec->m_data, 1);
	}

	return INVALID_ROW_ID;
}

/**
* @see SimpleRidMapping::insertMapping
*/
void NtseRidMapping::insertMapping(RowId origRid, RowId mappingRid) {
	assert(!m_session->isLogging());
	assert(origRid != INVALID_ROW_ID && mappingRid != INVALID_ROW_ID); // assert true
	if (origRid == mappingRid) {
		assert(origRid != INVALID_ROW_ID);
	} else {
		assert(origRid != INVALID_ROW_ID);
	}
	u64 sp = m_session->getMemoryContext()->setSavepoint();

	RedRecord::writeNumber(m_map->getTableDef(), 0, m_rec->m_data, origRid);
	RedRecord::writeNumber(m_map->getTableDef(), 1, m_rec->m_data, mappingRid);

	uint dupIdx;
	RowId rid = INVALID_ROW_ID;
	try {
		rid = m_map->insert(m_session, m_rec->m_data, true, &dupIdx);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
	}

	assert(rid != INVALID_ROW_ID);
	m_session->getMemoryContext()->resetToSavepoint(sp);
}

/**
 * @see SimpleRidMapping::deleteMapping
 */
void NtseRidMapping::deleteMapping(RowId origRid) {
	assert(!m_session->isLogging());
	assert(origRid != INVALID_ROW_ID);
	TableDef *tbDef = m_map->getTableDef();
	IndexDef *idxDef = m_map->getTableDef()->m_indice[0];
	u64 sp = m_session->getMemoryContext()->setSavepoint();

	RedRecord::writeNumber(m_map->getTableDef(), 0, m_redKey->m_data, (u64)origRid);

	//ӳ������д����
	RecordOper::extractKeyRP(tbDef, idxDef, m_redKey, NULL, m_key);

	IndexScanCond cond(0, m_key, true, true, true);
	TblScan *scanHdl = m_map->indexScan(m_session, OP_DELETE, &cond, m_subRec->m_numCols, m_subRec->m_columns);

	bool exist = m_map->getNext(scanHdl, m_subRec->m_data);

	if (exist) {
		m_map->deleteCurrent(scanHdl);
	}

	m_map->endScan(scanHdl);
	m_session->getMemoryContext()->resetToSavepoint(sp);
}

/**
 * ��ʼһ��ӳ���ɨ��
 */
void NtseRidMapping::startIter() {
	assert(!m_scanHdl);
	m_scanHdl = m_map->tableScan(m_session, OP_READ, m_subRec->m_numCols, m_subRec->m_columns);
}

/**
 * ��ȡ��һ�����
 * @param mapped out	ӳ��RowIdֵ������
 * @return				ԭRowIdֵ
 */
RowId NtseRidMapping::getNextOrig(RowId *mapped) {
	u64 origRid = INVALID_ROW_ID;
	bool got = m_map->getNext(m_scanHdl, m_subRec->m_data);
	if (got) {
		origRid = RedRecord::readBigInt(m_map->getTableDef(), m_subRec->m_data, 0);
		*mapped = RedRecord::readBigInt(m_map->getTableDef(), m_subRec->m_data, 1);
	} else
		*mapped = origRid = INVALID_ROW_ID;
	return origRid;
}

/**
 * ��ֹ��ɨ��
 */
void NtseRidMapping::endIter() {
	m_map->endScan(m_scanHdl);
	m_scanHdl = NULL;
}

/**
 * ��ɨ��ͳ��ӳ����еļ�¼����
 * @return         ��¼��Ŀ
 */
u64 NtseRidMapping::getCount() {
	u64 count = 0;
	u64 pkey;
	startIter();
	while (getNextOrig(&pkey) != INVALID_ROW_ID) ++count;
	endIter();
	return count;
}

/**
 * ����ӳ��ֵ���ԭֵ
 * @param mapped		ӳ��RowId
 * @return				ԭ��RowIdֵ
 */
RowId NtseRidMapping::getOrig(RowId mapped) {
	m_scanHdl = m_map->tableScan(m_session, OP_READ, m_subRec->m_numCols, m_subRec->m_columns);
	u64 orig = INVALID_ROW_ID;
	while (m_map->getNext(m_scanHdl, m_subRec->m_data)) {
		if (mapped == (RowId)RedRecord::readBigInt(m_map->getTableDef(), m_subRec->m_data, 1)) {
			orig = RedRecord::readBigInt(m_map->getTableDef(), m_subRec->m_data, 0);
			break;
		}
	}
	m_map->endScan(m_scanHdl);
	m_scanHdl = NULL;
	return orig;
}

} //namespace ntse {
