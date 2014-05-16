/**
 * 表在线维护功能实现
 *
 * @author 谢可(xieke@corp.netease.com, ken@163.org)
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
 * 生成日志拷贝
 * @param memctx	内存分配上下文
 * @param logEntry	原始日志
 * @return			生成的日志拷贝，内存为memctx中分配所得
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
 * 根据第一个日志拷贝生成一个TxnLogList对象
 * @param memctx       内存上下文
 * @param firstLog     第一个日志拷贝
 * @param txnId        日志Id
 * @return             TxnLogList对象指针
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

/** 向事务日志列表中增加一个拷贝
 *
 * @param list            日志事务项列表
 * @param logcopy         日志拷贝
 */
void appendCopy(TxnLogList *list, LogCopy *logcopy) {
	assert(list->m_txnId == logcopy->m_logEntry.m_txnId);
	list->m_last->m_next = logcopy;
	list->m_last = logcopy;
}

/** 判断两个TxnLogList的顺序，依据记录rowid从小到大
 * @param tl1           事务日志列表1
 * @param tl2           事务日志列表2
 * @return              tl1先于tl2返回true
 */
bool txnless(const TxnLogList *tl1, const TxnLogList *tl2) {
	return tl1->m_rowId < tl2->m_rowId;
}

/**
 * 创建一个日志回放类
 * @param table			回放日志的表
 * @param db			数据库
 * @param startLSN		回放的起始LSN
 * @param endLSN		回放的终结LSN
 * @param logBufSize	记录缓冲区大小，默认为32M
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

/** 日志回放类析构函数 */
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

/** 开始日志回放 */
void LogReplay::start() {
	assert(!m_scanHdl);
	m_scanHdl = m_db->getTxnlog()->beginScan(m_lsnStart, m_lsnEnd);
	m_logScaned = false;
#ifdef NTSE_UNIT_TEST
	// 计数
	m_returnCnt = 0;
	m_shlNextCnt = 0;
	m_validStartCnt = 0;
#endif
}

/** 结束日志回放 */
void LogReplay::end() {
	assert(m_scanHdl);
	m_db->getTxnlog()->endScan(m_scanHdl);
}

/** 切换内存资源 */
void LogReplay::switchMemResource() {
	// 只有当主上的ntse事务全被回放完成后才能切换到备
	assert(m_unfinished->size() == 0);
	assert(m_txnList->size() == 0);

	m_memCtx->reset();
	m_txnList->clear();
	// 主备交换 memCtx <-> memCtxBak, m_txnList <-> m_bakList, m_unfinished <-> m_bakMap
	// LogReplay过程中memCtx/m_txnList/m_unfinished始终为主，memCtxBak/m_bakList/m_bakMap为备
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

/** 获取未完成事务中最小的lsn 
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

/** 获取下一个事务列表
 * @param session 会话
 * @return 下一个事务列表，无下一个返回NULL
 */
TxnLogList* LogReplay::getNextTxn(Session *session) {
	if (m_vecIdx < m_orderedTxn.size()) { // 缓存区中尚有拷贝，继续供给
		assert(m_orderedTxn[m_vecIdx]->m_valid);
		assert(m_orderedTxn[m_vecIdx]->m_first->m_logEntry.m_logType == LOG_TXN_START);
		assert(m_orderedTxn[m_vecIdx]->m_last->m_logEntry.m_logType == LOG_TXN_END);
#ifdef NTSE_UNIT_TEST
		++m_returnCnt;
#endif
		return m_orderedTxn[m_vecIdx++];
	}

	if (m_logScaned) {
		return NULL; // 已经扫完了
	}
	
	m_orderedTxn.clear();
	m_vecIdx = 0;
	// 如果主上的事务已经全部完成，则主备交换
	assert(m_unfinished->size() == 0);
	if (m_unfinished->size() == 0) {
		switchMemResource();
	}

	bool underLimit = true; // 开始时假设数据量并未超过限制
	LogScanHandle *scanHit = NULL;

	while ((scanHit = m_db->getTxnlog()->getNext(m_scanHdl)) != NULL) {
#ifdef NTSE_UNIT_TEST
		++m_shlNextCnt;
#endif
		// 扫描直到m_memctxbak变满
		u16 txnId = m_scanHdl->logEntry()->m_txnId;
		u16 tableId = m_scanHdl->logEntry()->m_tableId;
		if (TableDef::tableIdIsVirtualLob(tableId))
			tableId = TableDef::getNormalTableId(tableId);
		if (tableId != m_table->getTableDef(true, session)->m_id) {
			continue; // 不是本表的日志不需要处理
		}

		if (m_scanHdl->logEntry()->m_logType == LOG_TXN_START) {
			TxnType txnType = Session::parseTxnStartLog(m_scanHdl->logEntry());
			// 某些类型的事务可以略过，只需要回放以下类型的事务日志
			//	TXN_INSERT,			/** 插入记录事务 */
			//	TXN_UPDATE,			/** 更新记录事务 */
			//	TXN_DELETE,			/** 删除记录事务 */
			if (!(txnType == TXN_INSERT || txnType == TXN_UPDATE || txnType == TXN_DELETE)) {
				continue;
			}

#ifdef NTSE_UNIT_TEST
			++m_validStartCnt;
#endif

			assert(m_unfinished->find(txnId) == m_unfinished->end());
			assert(m_bakMap->find(txnId) == m_bakMap->end());

			if (underLimit) { // 所有数据插入到主套件中，使用memctx
				TxnLogList *txnLogs = getTxnLogList(m_memCtx, logCopy(m_memCtx, m_scanHdl->logEntry()), txnId, m_scanHdl->curLsn());
				// 插入list中，插入未完成集合中
				(*m_unfinished)[txnId] = txnLogs;
				//m_txnList->push_back(txnLogs);
			} else { // 表已经超出预期内存大小，新的LOG_TXN_START插入到备份map中
				TxnLogList *txnlogs = getTxnLogList(m_memCtxBak, logCopy(m_memCtxBak, m_scanHdl->logEntry()), txnId, m_scanHdl->curLsn());
				(*m_bakMap)[txnId] = txnlogs;
				//m_bakList->push_back(txnlogs);
			}
		} else { // 不是开始事务的日志，我们要判断是否在map内，不在直接略过
			assert(m_scanHdl->logEntry()->m_logType != LOG_TXN_START);
			bool inbakup = false; //在备份数据中
			if (m_unfinished->find(txnId) == m_unfinished->end()) { // 不在主map中
				/* 前提是所有不是本表的日志都被过滤掉了 */
				/*
				 *	Modified: liaodingbai@corp.netease.coom
				 *	不在主map，也有可能不在从map，例如其他三种事务TXN_UPDATE_HEAP, TXN_MMS_FLUSH_DIRTY和
				 *	TXN_ADD_INDEX。
				 */
				if (m_bakMap->find(txnId) != m_bakMap->end()) {
					inbakup = true;
				} else {
					continue;
				}
			}
			TxnLogList *logList = inbakup ? (*m_bakMap)[txnId] : (*m_unfinished)[txnId];
			assert(logList);
			// 根据日志类型做判断
			switch (m_scanHdl->logEntry()->m_logType) {
				case LOG_TXN_END:
					{
						// 从未完成列表中删除（无论是否成功的日志）
						map<u16, TxnLogList *> *unfinish = inbakup? m_bakMap : m_unfinished;
						list<TxnLogList *> *txnList = inbakup? m_bakList: m_txnList;

						map<u16, TxnLogList *>::iterator iter = unfinish->find(txnId);
						assert(iter != unfinish->end());
						assert(logList == iter->second);
						unfinish->erase(iter);
						txnList->push_back(logList);

						if (Session::parseTxnEndLog(m_scanHdl->logEntry())) {
							// 成功的事务
							assert(logList->m_valid);
						} else {  // 失败的未提交事务
							assert(logList->m_valid);
							logList->m_valid = false;
							break; //这个日志不需要再复制了
						}
					}
					goto default_action;
				// 其他类型能走到这一步的日志有：
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
						assert(logList->m_rowId == INVALID_ROW_ID); // 此时还未更改过
						// 分析heap insert日志，取出RowId
						logList->m_rowId = DrsHeap::getRowIdFromInsLog(m_scanHdl->logEntry());
					} else {
						assert(true);
					}
					goto default_action;
				case LOG_PRE_DELETE:
					assert(logList->m_type == TXN_DELETE);
					// 分析pre delete日志，取出RowId
					logList->m_rowId = m_table->getRidFromPreDeleteLog(m_scanHdl->logEntry());
					goto default_action;
				case LOG_PRE_UPDATE:
					assert(logList->m_type == TXN_UPDATE);
					// 分析pre update日志，取出RowId
					logList->m_rowId = Session::getRidFromPreUpdateLog(m_scanHdl->logEntry());
					goto default_action;
				default:
default_action:
					// 拷贝日志到list
					appendCopy(logList, logCopy(inbakup ? m_memCtxBak : m_memCtx, m_scanHdl->logEntry()));
					break;
			}
		}
		// 一条日志处理完毕，判断是否需要继续处理
		if (underLimit) {
			if (m_memCtx->getMemUsage() >= m_bufSize)
				underLimit = false; //当主消耗内存过多的时候，应该后面才begin的事务放入备中
		} else { //已经超限情况下继续扫描，直到主中unfinish的事务全读到end start日志为止
			if (m_unfinished->size() == 0) {
				assert(scanHit);
				break; // 这里跳出表扫描循环，则scanhit不为空
			}
		}
	}

	//assert(m_unfinished->size() == 0); // 主未完成map中肯定没有未完成项
	assert(m_orderedTxn.size() == 0);
	// 将所有已经end的ntse事务全部进行排序回放
	for(list<TxnLogList*>::iterator it = m_txnList->begin(); it != m_txnList->end(); ++it) {
		if ((*it)->m_valid) {
			assert((*it)->m_last->m_logEntry.m_logType == LOG_TXN_END);
			assert((*it)->m_rowId != INVALID_ROW_ID); // RowId已经处理过
			m_orderedTxn.push_back(*it);
		}
	}
	m_txnList->clear();

	if (scanHit != NULL) { // 如果还有日志未被扫描，那么必定是主的资源即将耗尽
		assert(m_unfinished->size() == 0);
		assert(!underLimit);
	} else {
		//日志已经扫描结束，此时有下列几种情况
		// 1.只用了主的memctx/list/map，没用备的memctx/list/map，此时m_unfinish的个数有可能是不为零
		// 2.既用了主的memctx/list/map，又用了备的memctx/list/map，此时m_unfinish的个数有可能不为零，m_bakMap个数也有可能不为零
		//此时把备的链表上的完成事务也一起做回放
		for(list<TxnLogList*>::iterator it = m_bakList->begin(); it != m_bakList->end(); ++it) {
			if ((*it)->m_valid) {
				assert((*it)->m_last->m_logEntry.m_logType == LOG_TXN_END);
				assert((*it)->m_rowId != INVALID_ROW_ID); // RowId已经处理过
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
	// 对orderlist进行稳定排序
	stable_sort(m_orderedTxn.begin(), m_orderedTxn.end(), txnless);
	assert(m_vecIdx == 0);
#ifdef NTSE_UNIT_TEST
	++m_returnCnt;
#endif
	return m_orderedTxn[m_vecIdx++];
}

/**
 * 在线修改索引维护
 * @pre 不能加表元数据和数据锁
 *
 * @param session           会话
 * @return                  成功操作返回true
 * @throw                   加锁超时，索引定义有误，或者文件索引文件操作错误等
 */
bool TblMntAlterIndex::alterTable(Session *session) throw(NtseException) {
	int olLsnHdl = 0, oldOlLsnHdl;
	Database *db = m_table->m_db;
	ILMode oldMetaLock = m_table->getMetaLock(session);
	upgradeMetaLock(session, oldMetaLock, IL_U, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);	// 可能超时，直接抛出异常
	TableDef *tableDefOld = m_table->getTableDef(true, session);
	SYNCHERE(SP_TBL_ALTIDX_AFTER_U_METALOCK);

	try {
		//权限检查
		m_table->checkPermission(session, false);
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	//当前操作只添加不存在的在线索引
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

	// 验证每一个意图删除的索引名均存在的
	for (u16 i = 0; i < m_numDelIdx; ++i) {
		int idxNo = tableDefOld->getIndexNo(m_delIndice[i]->m_name);
		if (idxNo < 0) {
			downgradeMetaLock(session, m_table->getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_NONEINDEX, "Index %s not found.", m_delIndice[i]->m_name);
		}
		assert(*tableDefOld->m_indice[idxNo] == *m_delIndice[i]);
		idxDeleted[idxNo] = true;
	}

	// 验证每一个意图增加的索引均不存在，名字都不重复，不是唯一性索引
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

	// 复制新索引，先复制原索引中未被删除的索引，新增的索引都在最后
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

	// 复制一个TableDef，没有索引的
	TableDef *tempTbdf = tempCopyTableDef(0, NULL);
	// 关闭session的log
	disableLogging(session);

	u64 lsn = m_table->m_db->getTxnlog()->tailLsn();
	char* lsnBuffer = (char*)alloca(Limits::LSN_BUFFER_SIZE*sizeof(char));
	sprintf(lsnBuffer, I64FORMAT"u", lsn);
	// 创建新索引
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
	// 设置online adding indice
	SYNCHERE(SP_MNT_ALTERINDICE_BEFORE_ADDIND_ONLINE_INDICE);
	
	while (true) {
		try {
			upgradeMetaLock(session, IL_U, IL_X, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &) {
			SYNCHERE(SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL);
		}
	}
	// 在持有meta lock X的情况下，意味着作用于该表的ntse事务已经全部提交
	// 获取lsnstart，并且设置onlinelsn
	startLsn = getLsnPoint(session, true, true, &olLsnHdl);
	if (m_numAddIdx > 0) {
		m_table->setOnlineAddingIndice(session, m_numAddIdx, m_addIndice);
	}
	downgradeMetaLock(session, IL_X, IL_U, __FILE__, __LINE__);
	SYNCHERE(SP_TBL_ALTIDX_BEFORE_GET_LSNSTART);
	
	SYNCHERE(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	// flush mms，使得堆中数据对于startLsn来说是更新过的
	m_table->flushComponent(session, false, false, true, false);

	/* 创建索引数据 */
	assert(drsIndice);
	assert(m_table->getMetaLock(session) == IL_U);
	assert(session->getTrans() == NULL || m_table->getLock(session) == IL_NO);
	// 复制并去除索引唯一性限制
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

	// TODO，不加数据锁应该不对

	assert(tempTbdf->m_numIndice == 0);

	try {
		// 依次检查索引的合法性
		for (u16 i = 0; i < m_numNewIndice; ++i) 
			m_newIndice[i]->check(tempTbdf);
		// 依次生成各个索引
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
	// 记录lsnend
	SYNCHERE(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	u64 endLsn = getLsnPoint(session, true, false);

	// 回放lsnstart到lsnend之间的日志，也就是创建索引期间产生的操作日志
	LogReplay replay(m_table, db, startLsn, endLsn);
	replay.start();
	SimpleRidMapping ridmap; // alterIndex使用SimpleRidMapping

	db->getSyslog()->log(EL_DEBUG, "start processing log...");
	processLogToIndice(session, &replay, tempTbdf, drsIndice, &ridmap);

	//此时可能还存在未完成的事务，但replay end和析构后，会将未完成的事务数据也释放掉
	//所以必须记录最小未完成事务的begin trx lsn
	LsnType minUnfinishTxnLsn = replay.getMinUnfinishTxnLsn();
	if (minUnfinishTxnLsn != INVALID_LSN) {
		assert(minUnfinishTxnLsn <= endLsn);
		endLsn = minUnfinishTxnLsn;
	}
	replay.end();

	// 释放lsn占用
	startLsn = endLsn;
	oldOlLsnHdl = olLsnHdl;
	olLsnHdl = db->getTxnlog()->setOnlineLsn(startLsn); // 因为lsnstart比原有值小，这里一定会成功。
	assert(olLsnHdl >= 0);
	db->getTxnlog()->clearOnlineLsn(oldOlLsnHdl);

	// 至此，应该所有唯一性索引均可满足唯一性约束，
	// 但再次回放过程未必是按照唯一性约束的，因为我们按RowId排序事务日志
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
	delete [] idxUnique; //不用了

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
		// 刷出数据
		if (startflush) {
			m_table->flush(session);
			drsIndice->flush(session);
		}
		// 再次记录lsn
		endLsn = getLsnPoint(session, true, false);
		// 重放start到end这段日志
		LogReplay replayAgain(m_table, db, startLsn, endLsn);
		replayAgain.start();
		processLogToIndice(session, &replayAgain, tempTbdf, drsIndice, &ridmap);

		//此时可能还存在未完成的事务，但replay end和析构后，会将未完成的事务数据也释放掉
		//所以必须记录最小未完成事务的begin trx lsn
		LsnType minUnfinishTxnLsn = replayAgain.getMinUnfinishTxnLsn();
		if (minUnfinishTxnLsn != INVALID_LSN) {
			assert(minUnfinishTxnLsn <= endLsn);
			endLsn = minUnfinishTxnLsn;
		}
		replayAgain.end();
		time2 = System::fastTime();
		// 设置lsnstart，释放已经回放的日志
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
			//X锁定元数据锁
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
						//因为事务表锁未实现升级，故只能加X锁
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
	// 最后回放
	assert(session->getTrans() != NULL || m_table->getLock(session) == IL_S);
	assert(m_table->getMetaLock(session) >= IL_U);
	m_table->flush(session);
	endLsn = getLsnPoint(session, false, false);
	LogReplay replayFinal(m_table, db, startLsn, endLsn);
	replayFinal.start();
	processLogToIndice(session, &replayFinal, tempTbdf, drsIndice, &ridmap);
	//此时必定不存在未完成的ntse事务
	assert(replayFinal.getMinUnfinishTxnLsn() == INVALID_LSN);
	replayFinal.end();
	drsIndice->flush(session);

	// 设置onlineLSN释放
	db->getTxnlog()->clearOnlineLsn(olLsnHdl);

	db->getSyslog()->log(EL_DEBUG, "finish processing log, start replacing table.");

	// X锁定数据锁
	while (true) {
		//事务表锁不能升级
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

	// 打开loggging，记录日志
	assert(!session->isLogging());
	enableLogging(session);
	// bumpFlushLSN
	db->bumpFlushLsn(session, m_table->m_tableDef->m_id);
	// 写日志
	m_table->writeAlterIndiceLog(session, tempTbdf, relativeIdxPath.c_str());
	SYNCHERE(SP_MNT_ALTERINDICE_JUST_WRITE_LOG);

	// 将onlineAddingIndex的设置改回来
	if (m_numAddIdx > 0)
		m_table->resetOnlineAddingIndice(session);

	TableDef *oldDef = m_table->m_tableDef;
	m_table->setTableDef(tempTbdf);
	// 将tabledef刷新到堆
	m_table->writeTableDef();
	SYNCHERE(SP_MNT_ALTERINDICE_JUST_WRITE_TABLEDEF);

	additionalAlterIndex(session, oldDef, &tempTbdf, drsIndice, m_addIndice, m_numAddIdx, idxDeleted);

	// 关闭表
	m_table->close(session, true, true);
	delete oldDef;
	oldDef = NULL;

	// 关闭新索引
	drsIndice->close(session, true);
	delete drsIndice;
	drsIndice = NULL;

	// 删除替换索引
	string indexFilePath = fullPath + Limits::NAME_IDX_EXT;
	File idxFile(fullIdxPath.c_str());
	idxFile.move(indexFilePath.c_str(), true); // 覆盖
	SYNCHERE(SP_MNT_ALTERINDICE_INDICE_REPLACED);

	// 重新打开堆
	/*Table *tmpTb = 0;
	try {
		tmpTb = Table::open(db, session, tablePath.c_str(), m_table->hasCompressDict());
	} catch (NtseException &) {
		assert(false);
	}

	// 验证table完整性
#ifdef NTSE_UNIT_TEST
	tmpTb->verify(session);
#endif

	// 替换表组件
	m_table->replaceComponents(tmpTb);
	delete tmpTb; // 不用close直接删*/
	reopenTblAndReplaceComponent(session, tablePath.c_str(), isNewTbleHasDict());

	// 释放表锁
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
 * 临时拷贝一份操作表的TableDef，使用新的索引定义而不是原表索引定义
 * @param numIndice           使用新的索引数目，默认为0
 * @param indice              表定义使用的索引定义数组
 * @return                    返回拷贝的表定义
 */
TableDef* TableOnlineMaintain::tempCopyTableDef(u16 numIndice, IndexDef **indice) {
	TableDef *origTbDef = m_table->getTableDef();
	TableDef *tblDef = new TableDef();
	*tblDef = *origTbDef; // 全值拷贝

	tblDef->m_name = new char[strlen(origTbDef->m_name) + 1];
	strcpy(tblDef->m_name, origTbDef->m_name);
	tblDef->m_schemaName = new char[strlen(origTbDef->m_schemaName) + 1];
	strcpy(tblDef->m_schemaName, origTbDef->m_schemaName);

	//拷贝列定义
	tblDef->m_columns = new ColumnDef*[tblDef->m_numCols];
	for (u16 i = 0; i < tblDef->m_numCols; i++) {
		tblDef->m_columns[i] = new ColumnDef(origTbDef->m_columns[i]);
		tblDef->m_columns[i]->m_inIndex = false;
	}
	//拷贝压缩配置信息及属性组定义
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
	//新建索引定义
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
 * 获取Lsn
 * @param session		会话
 * @param lockTable		锁定表
 * @param setOnlineLSN	设置OnlieLSN为获得的LSN
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
* 回放日志，维护索引
*
* @param session	会话
* @param replay		已经start的LogReplay对象
* @param recconv	记录转换
* @param destTb		操作表
* @param ridmap		记录映射
*/
void TableOnlineMaintain::processLogToTable(Session *session, LogReplay *replay, RecordConvert *recconv, Table *destTb, NtseRidMapping *ridmap) {
	TxnLogList *txnList = NULL;
	TableDef *origTbdef = m_table->getTableDef();
	TableDef *destTbdef = destTb->getTableDef();
	
	// 暂时将session中的事务置为NULL，防止重放过程事务锁，写TNT日志
	TNTTransaction *trx = session->getTrans();
	session->setTrans(NULL);

	u64 sp = session->getMemoryContext()->setSavepoint();

	// 用于读取大对象
	MemoryContext lobCtx(Limits::PAGE_SIZE, 1);

	Record rec; // 从log中构造的记录，不拷贝数据，为REC_VARLEN或者REC_FIXLEN格式
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
			/** insert的事务回放 */
			case TXN_INSERT:
#ifdef NTSE_UNIT_TEST
				++insrep;
#endif
				{
					McSavepoint msp(session->getMemoryContext());

					RowId rid = ridmap->getMapping(txnList->m_rowId);
					// 在进行转换之前我们首先判断是否需要skip
					if (rid == INVALID_ROW_ID) {
						bool hasLob = false;

						for(LogCopy *logCopy = txnList->m_first; logCopy != NULL; logCopy = logCopy->m_next) {
							if (logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && !TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId)) {
								DrsHeap::getRecordFromInsertlog(&logCopy->m_logEntry, &rec);
								//break; // 因为大对象日志在堆日志之前，所以不必担心break会跳过。
							} else
								if(logCopy->m_logEntry.m_logType == LOG_LOB_INSERT ||
									(logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId))) {
										hasLob = true;
								}
						}
						u64 sp = session->getMemoryContext()->setSavepoint();
						// 将堆记录格式转化为REC_REDUNDANT
						if (origTbdef->m_recFormat == REC_FIXLEN) {
								// 没有大对象，并且是定长记录，直接使用
								mysqlRec.m_data = rec.m_data;
								mysqlRec.m_size = rec.m_size;
						} else {//如果为变长记录或压缩记录
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
								} while (!(!logCopy // 扫描到头，理论上是不会出现这种情况
									|| logCopy->m_logEntry.m_logType == LOG_LOB_INSERT // 不是大型大对象插入
									|| (logCopy->m_logEntry.m_logType == LOG_HEAP_INSERT && TableDef::tableIdIsVirtualLob(logCopy->m_logEntry.m_tableId)))); // 小型大对象插入
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

						// 转换
						byte *destRec = recconv->convertMysqlOrRedRec(&mysqlRec, session->getMemoryContext());

						try {
							rid = destTb->insert(session, destRec, true, &dupIndex);
						} catch (NtseException &e) {
							UNREFERENCED_PARAMETER(e);
						}
						assert(rid != INVALID_ROW_ID);
						ridmap->insertMapping(txnList->m_rowId, rid);

						// 恢复savepoint
						session->getMemoryContext()->resetToSavepoint(sp);
					} // if (rid != INVALID_ROW_ID) { else
				}
				break;
			/** delete的事务回放 */
			case  TXN_DELETE:
#ifdef NTSE_UNIT_TEST
				++delrep;
#endif
				{
					RowId rid = ridmap->getMapping(txnList->m_rowId);

					if (rid != INVALID_ROW_ID) {
						// 删除记录
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
						// 删除映射关系
						ridmap->deleteMapping(txnList->m_rowId);
					}
				}
				break;
			/** update的事务回放 */
			case TXN_UPDATE:
#ifdef NTSE_UNIT_TEST
				++updrep;
#endif
				{
					RowId rid = ridmap->getMapping(txnList->m_rowId);

					if (rid != INVALID_ROW_ID) {
						// 解析update日志
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


						// 处理大对象内容
						if (puLog->m_numLobs == 0) {
							// TNT的purge在构建预更新日志时不会记录大对象内容
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
								// 如果原列是Null或者读取不到大对象，则把该列置为Null，去更新临时表
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
							// 为兼容ntse单元测试
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
						

						// 转换记录
						u16 outNumCols;
						u16 *outColumns;
						byte *converted = recconv->convertMysqlOrRedRec(&mysqlRec, session->getMemoryContext(),
							puLog->m_subRec->m_numCols, puLog->m_subRec->m_columns, &outNumCols, &outColumns);

						// 更新记录
						TblScan *posScan = NULL;
						try {
							posScan = destTb->positionScan(session, OP_UPDATE, outNumCols, outColumns);
						} catch (NtseException &e) {
							UNREFERENCED_PARAMETER(e);
						}

						// 定位
						bool exist = destTb->getNext(posScan, redData, rid);
						assert_always(exist);

						// 更新
						posScan->setUpdateColumns(outNumCols, outColumns);
						// 更新数据必定是mysql格式
						NTSE_ASSERT(destTb->updateCurrent(posScan, converted, true, &dupIndex)); // 不抛异常，记录超长问题在转换时已解决。

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
 * 回放日志，维护索引
 *
 * @param session		会话
 * @param replay		已经start的LogReplay对象
 * @param tbdef			新索引对应的TableDef
 * @param indice		对照日志回放的索引对象
 * @param ridmap		记录映射表，目前应该为空映射表
 */
void TableOnlineMaintain::processLogToIndice(Session *session, LogReplay *replay, TableDef *tbDef, DrsIndice *indice, SimpleRidMapping *ridmap) {
	TxnLogList *txnList  = NULL;
	Record rec; // 从log中构造的记录，不拷贝数据，为REC_VARLEN或者REC_FIXLEN格式
	Record redRec; // 转化后的冗余记录，定长不拷贝数据，变长需要转换
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

	// 为每个索引提取升序排序的columns数组，用于后续解析日志时提取子记录所用
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
		// 重构索引
		switch (txnList->m_type) {
			/** insert事务处理 */
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
					assert(rid != INVALID_ROW_ID); // 因为simplemapping 肯定不会返回INVALID_ROW_ID的
					assert(indice->getIndexNum() == tbDef->m_numIndice);
					// 需要处理这条日志的索引
					for (u16 i = 0; i < tbDef->m_numIndice; ++i) {
						McSavepoint lobSavepoint(session->getLobContext());
						//indice->getIndex(i)->
						IndexDef *idxdef = tbDef->m_indice[i];
						// 生成索引查找key
						assert(key.m_format == KEY_PAD);
						key.m_numCols = idxdef->m_numCols;
						key.m_columns = idxdef->m_columns;
						key.m_data = keydata;
						key.m_rowId = INVALID_ROW_ID;
						key.m_size = Limits::PAGE_SIZE;
						
						// 提取属性值
						if (rec.m_format == REC_FIXLEN) {
							// Record准备
							assert(redRec.m_format == REC_REDUNDANT);
							redRec.m_rowId = rid;
							redRec.m_data = rec.m_data;
							redRec.m_size = rec.m_size;
							assert(redRec.m_size == tbDef->m_maxRecSize);
							// SubRecord准备
							redSubrec.m_rowId = rid;
							assert(redSubrec.m_format == REC_REDUNDANT);
							redSubrec.m_numCols = key.m_numCols;
							redSubrec.m_columns = ascIndexColumns[i];
							redSubrec.m_data = rec.m_data;
							redSubrec.m_size = rec.m_size;
						} else if (rec.m_format == REC_VARLEN) {
							// SubRecord准备
							assert(redSubrec.m_format == REC_REDUNDANT);
							redSubrec.m_numCols = key.m_numCols;
							redSubrec.m_columns = ascIndexColumns[i];
							redSubrec.m_data = data;
							redSubrec.m_size = Limits::DEF_MAX_REC_SIZE;
							RecordOper::extractSubRecordVR(tbDef, &rec, &redSubrec);
							redSubrec.m_rowId = rid;

							assert(redSubrec.m_size != Limits::PAGE_SIZE);
							// Record准备
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
							// Record准备
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

						// 填充搜索查找key
						RecordOper::extractKeyRP(tbDef, idxdef, &redRec, &lobArray, &key);
						assert(key.m_rowId == rid);

						// 查找索引中是否已经有(rowid, key)配对
						RowId outRid = INVALID_ROW_ID;
						assert(key.m_rowId == redSubrec.m_rowId);
						bool exist = indice->getIndex(i)->getByUniqueKey(session, &key, None, &outRid, NULL, NULL, NULL);
						if (!exist) {
							// 将(rowid, key)插入到索引中
							assert(outRid == INVALID_ROW_ID);
							bool duplicateKey;
							u64 token = session->getToken(); // TODO:
							indice->getIndex(i)->insert(session, &key, &duplicateKey);
							session->unlockIdxObjects(token); // TODO:

						} else {// 如果存在，跳过不处理
							assert(outRid == key.m_rowId);
						}
					}
				} // case TXN_INSERT
				break;
			/** delete事务重放 */
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
					// 有索引重建
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
						// 生成查找key
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

						// 填充搜索查找key
						RecordOper::extractKeyRP(tbDef, idxDef, &redRec, &lobArray, &key);
						assert(key.m_rowId == rid);
						
						// 查找索引中是否已经有(rowid, key)配对
						RowId outRid = INVALID_ROW_ID;
						bool exist = indice->getIndex(i)->getByUniqueKey(session, &key, None, &outRid, NULL, NULL, NULL);
						if (exist) {
							// 删除之
							assert(outRid == key.m_rowId);
							u64 token = session->getToken();
							indice->getIndex(i)->del(session, &key);
							session->unlockIdxObjects(token);
						} else {
							// 啥也不做
						}
					} // for 遍历索引簇
					session->getMemoryContext()->resetToSavepoint(savepoint);
				}
				break;
			/** update事务处理 */
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
					 *	在线更新索引时，如果更新列不包括索引，此时m_indexPreImage为null，应可以直接跳过
					 *	ADD: liaodingbai@corp.netease.com
					 */
					if(puLog->m_indexPreImage == NULL)
						break;

					assert(indice);
					assert(puLog->m_indexPreImage && puLog->m_indexPreImage->m_format == REC_REDUNDANT);
					assert(rid != INVALID_ROW_ID);
					// 将更新后像填充到data数组中
					assert(redRec.m_format == REC_REDUNDANT);
					redRec.m_rowId = rid;
					redRec.m_size = tbDef->m_maxRecSize;
					redRec.m_data = afterdata;
					memcpy(afterdata, puLog->m_indexPreImage->m_data, redRec.m_size);
					// 将前项更新，包含所有所需的更新后的属性域
					RecordOper::updateRecordRR(tbDef, &redRec, puLog->m_subRec);

					// 后像SubRecord
					redSubAfter.m_rowId = rid;
					redSubAfter.m_size = m_table->getTableDef(true, session)->m_maxRecSize;
					redSubAfter.m_data = redRec.m_data;
					redSubAfter.m_numCols = puLog->m_indexPreImage->m_numCols;
					redSubAfter.m_columns = puLog->m_indexPreImage->m_columns;
					// 生成删除用的前像redSubrec
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
						 *	检查索引前像中涉及的列
						 *	ADD：liaodingbai@corp.netease.com
						 */
						it = set_intersection(ascIndexColumns[i], 
							ascIndexColumns[i] + idxDef->m_numCols,
							puLog->m_indexPreImage->m_columns, 
							puLog->m_indexPreImage->m_columns + puLog->m_indexPreImage->m_numCols,
							interSec.begin());
						int sameCols = int(it - interSec.begin());
						/**	preImage中的列和索引列不完全匹配，意味着修改不涉及该索引 */
						if (sameCols != idxDef->m_numCols) {
							continue;
						}
						/**	交集数目必须和索引列数相同 */
						assert(sameCols == idxDef->m_numCols);
						
						// 生成查找key
						assert(key.m_format == KEY_PAD);
						key.m_numCols = idxDef->m_numCols;
						key.m_columns = idxDef->m_columns;
						key.m_data = keydata;
						key.m_rowId = INVALID_ROW_ID;
						key.m_size = Limits::PAGE_SIZE;
						// 填充查找k， 前像
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
						// 查找索引中是否已经有(rowid, key)配对
						RowId outRid = INVALID_ROW_ID;
						bool exist = indice->getIndex(i)->getByUniqueKey(session, &key, None, &outRid, NULL, NULL, NULL);
						if (exist) {
							// 查到，删除
							u64 token = session->getToken();
							indice->getIndex(i)->del(session, &key);

							// 查找后像，没有则插入之
							// 生成后像查找键
							assert(keyAfter.m_format == KEY_PAD);
							keyAfter.m_numCols = idxDef->m_numCols;
							keyAfter.m_columns = idxDef->m_columns;
							keyAfter.m_data = keyafterdata;
							keyAfter.m_rowId = INVALID_ROW_ID;
							keyAfter.m_size = Limits::PAGE_SIZE;
							redRec.m_data = redSubAfter.m_data; // 使用后像

							Array<LobPair*> lobArray;
							if (idxDef->hasLob()) {
								RecordOper::extractLobFromR(session, tbDef, idxDef, m_table->getLobStorage(), &redRec, &lobArray);
							}
							// 填充后像查找键
							RecordOper::extractKeyRP(m_table->getTableDef(true, session), idxDef, &redRec, &lobArray, &keyAfter);

							outRid = INVALID_ROW_ID;
							bool afterKey = indice->getIndex(i)->getByUniqueKey(session, &keyAfter, None, &outRid, NULL, NULL, NULL);
							if (afterKey) {
								// 不做处理
							} else {
								assert(outRid == INVALID_ROW_ID);
								bool duplicateKey;
								indice->getIndex(i)->insert(session, &keyAfter, &duplicateKey);
							}
							session->unlockIdxObjects(token);
						}
					} // for 遍历索引簇
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
 * 如果有异常出现，删除tempCopyTableDef生成的TableDef，不删除新加的索引
 * @param tmpTbDef              使用copyTempTableDef获得的表定义
 */
void TableOnlineMaintain::delTempTableDef(TableDef *tmpTbDef) {
	tmpTbDef->m_numIndice = 0;
	tmpTbDef->m_indice = NULL;
	delete tmpTbDef;
}

/**
 * 重新打开临时表并替换原表组件
 * @param session
 * @param origTablePath
 * @return 
 */
void TableOnlineMaintain::reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict) {
	session->getConnection()->setStatus("Replace table component");

	Table *tmpTb = NULL;
	try {
		tmpTb = Table::open(session->getNtseDb(), session, origTablePath, hasCprsDict); // 替换完成后路径就是basePath
	} catch (NtseException &) {
		assert(false);
	}

	// 验证table完整性
#ifdef NTSE_UNIT_TEST
	tmpTb->verify(session);
#endif

	// 替换表组件
	m_table->replaceComponents(tmpTb);
	delete tmpTb; // 不用close直接删
	tmpTb = NULL;
}

/** 加表元数据锁
 * @pre 必须未加数据锁，即加表元数据锁必须在加数据锁之前
 *
 * @param session 会话
 * @param mode 锁模式，只能是S、U或X
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时
 */
void TableOnlineMaintain::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_table->lockMeta(session, mode, timeoutMs, file, line);
}

/** 升级表元数据锁。若oldMode与newMode相等或oldMode是比newMode更高级的锁，则不进行任何操作。
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时或失败，NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void TableOnlineMaintain::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_table->upgradeMetaLock(session, oldMode, newMode, timeoutMs, file, line);
}

/** 降级表元数据锁。若oldMode与newMode相等，若newMode比oldMode高级则不进行任何操作
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 */
void TableOnlineMaintain::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	m_table->downgradeMetaLock(session, oldMode, newMode, file, line);
}

/** 释放表元数据锁
 * @pre 必须在释放表数据锁之后调用
 *
 * @param session 会话
 * @param mode 锁模式
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
 * Table中和在线维护相关的接口实现
 *                                     *
 ***************************************/



/**
 * 重做索引维护
 * @param session           会话
 * @param log               日志项
 * @param db                数据库
 * @param tablepath         表路径
 * @throw                   堆文件访问错误会上抛异常
 */
void Table::redoAlterIndice(Session *session, const LogEntry *log, Database *db, const char *tablePath) throw (NtseException) {
	UNREFERENCED_PARAMETER(session);
	TableDef *newTbdef = NULL;
	char* relativeIdxPath = NULL;
	Table::parseAlterIndiceLog(log, &newTbdef, &relativeIdxPath);
	// 若临时索引文件存在，则表示进行到一半，未完成索引替换时崩溃，此时完成替换
	// 否则表示还没有开始进行替换
	string fullPath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + tablePath;
	string tempIndiceFilePath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + relativeIdxPath;
	if (File::isExist(tempIndiceFilePath.c_str())) {
		File tempIndiceFile(tempIndiceFilePath.c_str());
		string tblDefFilePath = fullPath + Limits::NAME_TBLDEF_EXT;
		newTbdef->writeFile(tblDefFilePath.c_str());
		// 将临时索引文件改名为索引文件，会覆盖老的索引文件
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
* 重做列变动维护
* @param session           会话
* @param log               日志项
* @param db                数据库
* @param tablepath         表路径
*/
void Table::redoAlterColumn(Session *session, const LogEntry *log, Database *db, const char *tablepath, bool *newHasDict) {
	UNREFERENCED_PARAMETER(session);
	bool hasLob = false;
	char* tmpTblPath = NULL;
	Table::parseAlterColumnLog(log, &tmpTblPath, &hasLob, newHasDict);

	string basePath(db->getConfig()->m_basedir);

	string fullPath(basePath + string(NTSE_PATH_SEP) + string(tablepath));
	// 得到临时表路径
	string tmpFullPath(basePath + NTSE_PATH_SEP + tmpTblPath);

	// 替换文件
	const int numFiles = Limits::EXTNUM;
	std::vector<const char *> removeFileSuffix;
	for (int i = 0; i < numFiles; ++i) {
		if (!(*newHasDict) && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, Limits::EXTS[i])) {//如果没有字典文件，则跳过
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
 * 重做创建字典
 * @param session 会话
 * @param log 日志项
 * @param db 数据库
 * @param tablePath 表相对路径 
 */
void Table::redoCreateDictionary(Session *session, Database *db, const char *tablePath) {
	UNREFERENCED_PARAMETER(session);
	string fullPath = string(db->getConfig()->m_basedir) + NTSE_PATH_SEP + tablePath;
	
	string newDictFilePath = fullPath + Limits::NAME_GLBL_DIC_EXT;
	string tempDictFilePath = fullPath + Limits::NAME_TEMP_GLBL_DIC_EXT;

	//如果压缩字典临时文件存在，则表示可能未完成压缩表组件替换，否则说明替换已经完成，临时文件已经被删除
	File tempDictFile(tempDictFilePath.c_str());
	if (File::isExist(tempDictFilePath.c_str())) {
		//拷贝字典文件，在修改完控制文件前临时字典文件还不能删除
		u64 errCode = File::copyFile(newDictFilePath.c_str(), tempDictFilePath.c_str(), true);
		UNREFERENCED_PARAMETER(errCode);
		assert(errCode == File::E_NO_ERROR);
	} else {
		//do nothing
	}
	assert(File::isExist(newDictFilePath.c_str()));
	//删除临时字典文件
	u64 errCode = tempDictFile.remove();
	UNREFERENCED_PARAMETER(errCode);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR || File::getNtseError(errCode) == File::E_NOT_EXIST);
}

/**
 * 构造一个在线列维护操作类
 * @param table				操作表对象
 * @param conn				联接
 * @param addColNum			增加列数量
 * @param addCol			增加列的列信息数组
 * @param delColNum			删除列数量
 * @param delCol			删除列的列定义数组
 * @param cancelFlag        操作取消标志
 * @param keepOldDict       是否保留原字典
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
 * 在线列变换维护类
 *
 * 如果原表被定义为压缩表，并且已经创建字典，则是否重新采样生成新的字典取决于
 * ntse_keep_dict_on_optimize私有连接配置；如果还没有创建字典，则不会采样生成字典；
 * 如没有被定义为压缩表但有字典，则字典会被删除，原表的压缩记录会被解压存储
 *
 * @param session              会话
 * @return                     操作成功完成返回true
 * @throw                      修改列参数不合法，或者临时表相关文件操作出错，采样生成字典失败等
 */
bool TblMntAlterColumn::alterTable(Session *session) throw(NtseException) {
	MemoryContext *memContext = session->getMemoryContext();
	McSavepoint msp(memContext);

	ILMode oldMetaLock = m_table->getMetaLock(session);
	//检查表元数据锁，需要则加上
	bool addMetaLockByMe = false;
	if (oldMetaLock == IL_NO) {
		//assert(m_table->getMetaLock(session) == IL_NO);
		lockMeta(session, IL_U, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);	// 可能超时，直接抛出异常
		SYNCHERE(SP_TBL_ALTCOL_AFTER_U_METALOCK);
		addMetaLockByMe = true;
	} else {
		assert(oldMetaLock == IL_U);
	}

	try {
		//权限检查
		m_table->checkPermission(session, false);
		// 生成convert，可能会出错（删除的列有索引等等）
		m_convert = new RecordConvert(m_table->getTableDef(true, session), m_addCols, 
			m_numAddCol, m_delCols, m_numDelCol);
	} catch (NtseException &e) {
		// 一定是因为列定义有问题
		if (addMetaLockByMe) {
			unlockMeta(session, IL_U);
		}
		throw e;
	}

	// 不记日志
	disableLogging(session);

	TempTableDefInfo tempTableDefInfo;
	AutoPtr<TableDef> newTbdef(m_convert->getNewTableDef());

	//预处理表定义
	preAlterTblDef(session, newTbdef, &tempTableDefInfo);

	//创建临时表
	Table *tmpTb = createTempTable(session, newTbdef);

	// 生成RID映射表
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
	// 在持有meta lock X的情况下，意味着作用于该表的ntse事务已经全部提交
	// 获得初始lsn并设置onlinelsn
	int olLsnHdl = INVALID_TOKEN;
	u64 lsnStart = INVALID_LSN;
	lsnStart = getLsnPoint(session, true, true, &olLsnHdl);
	downgradeMetaLock(session, IL_X, IL_U, __FILE__, __LINE__);
	SYNCHERE(SP_TBL_ALTCOL_BEFORE_GET_LSNSTART);

	SYNCHERE(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);


	//表拷贝
	string basePath = string(m_db->getConfig()->m_basedir);
	string origTablePath = string(m_table->getPath());
	string origTableFullPath = basePath + NTSE_PATH_SEP + origTablePath;
	string tmpTablePath(tmpTb->getPath());
	string tmpTableFullPath = basePath + NTSE_PATH_SEP + tmpTablePath;

	try {
		// 拷贝表
		copyTable(session, tmpTb, ridMap, newTbdef);

		// 回放日志，不带索引
		lsnStart = replayLogWithoutIndex(session, tmpTb, ridMap, lsnStart, &olLsnHdl);

		// 创建索引，非唯一性
		rebuildIndex(session, tmpTb, &tempTableDefInfo);

		// 回放日志，带索引（非唯一性）
		lsnStart = replayLogWithIndex(session, tmpTb, ridMap, lsnStart, &olLsnHdl);

		//升级表元数据锁和表锁
		upgradeTblLock(session);

		//TODO: 验证数据正确性

		//还原表定义设置
		restoreTblSetting(tmpTb, &tempTableDefInfo, newTbdef);

		// 记录bumpLSN
		u16 tableid = m_table->getTableDef(true, session)->m_id;

		enableLogging(session); //打开日志记录
		//m_db->bumpFlushLsn(session, tableid);
		//此时作用于该表的事务已经全部提交
		m_db->bumpTntAndNtseFlushLsn(session, tableid);

		tmpTb->setTableId(session, tableid);

		assert(!tmpTb->getMmsTable());

		additionalAlterColumn(session, ridMap);

		// 记录日志
		bool tmpTbleHasLob = tmpTb->getTableDef()->hasLob();
		m_db->getTxnlog()->flush(m_table->writeAlterColumnLog(session, tmpTb->getPath(), 
			tmpTbleHasLob, isNewTbleHasDict()));
		SYNCHERE(SP_MNT_ALTERCOLUMN_JUST_WRITE_LOG);

		m_table->close(session, false, true);
		tmpTb->close(session, true);
		delete tmpTb;
		tmpTb = NULL;

		// 替换文件
		replaceTableFile(tmpTbleHasLob, origTableFullPath, tmpTableFullPath);

		SYNCHERE(SP_MNT_ALTERCOLUMN_TABLE_REPLACED);

		// 重新打开表并替换表组件
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

	// 释放表锁
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

	// 释放资源
	m_db->getControlFile()->releaseTempTableId(newTbdef->m_id);

	m_conn->setStatus("Done");

	SYNCHERE(SP_MNT_ALTERCOLUMN_FINISH);
	return true;
}

/**
 * 升级表元数据锁
 * @param session
 */
void TblMntAlterColumn::upgradeTblMetaLock(Session *session) {
	m_conn->setStatus("Upgrade table meta lock");
	assert(m_table->getMetaLock(session) == IL_U);
	// X锁定元数据锁和数据锁
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
 * 升级表锁
 * @param session
 */
void TblMntAlterColumn::upgradeTblLock(Session *session) {
	m_conn->setStatus("Upgrade table lock");

	//事务表锁还未实现升级功能
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
 * 预先更新新表的定义(置空索引等)，并将被重置的信息保存下来，供倒表完成之后恢复表定义使用
 * @param session
 * @param newTbdef
 * @param tempTableDefInfo
 * @return 
 */
TableDef* TblMntAlterColumn::preAlterTblDef(Session *session, TableDef *newTbdef, 
											TempTableDefInfo *tempTableDefInfo) {
	assert(NULL != newTbdef);

	try {
		// 改变Table ID以免行锁冲突
		newTbdef->m_id = m_db->getControlFile()->allocTempTableId();
	} catch (NtseException &) {
		assert(false);
	}

	// 把indice给保留下来，暂时不用。
	tempTableDefInfo->m_indexDef = newTbdef->m_indice;
	tempTableDefInfo->m_indexNum = newTbdef->m_numIndice;
	tempTableDefInfo->m_newTbpkey = newTbdef->m_pkey;
	tempTableDefInfo->m_newTbUseMms = newTbdef->m_useMms;

	// 置空索引
	newTbdef->m_indice = NULL;
	newTbdef->m_pkey = NULL;
	newTbdef->m_numIndice = 0;

	// 关闭mms
	newTbdef->m_useMms = false;
	for (u16 col = 0; col < newTbdef->m_numCols; ++col) {
		// 这个值在后面addIndex的时候会自动恢复，不需要记录
		newTbdef->m_columns[col]->m_inIndex = false;
	}

	// 记录cacheUpdate信息，并且置空cacheUpdate
	tempTableDefInfo->m_cacheUpdateCol = (bool *)session->getMemoryContext()->alloc(sizeof(bool) * newTbdef->m_numCols);
	for (u16 i = 0; i < newTbdef->m_numCols; ++i) {
		tempTableDefInfo->m_cacheUpdateCol[i] = newTbdef->m_columns[i]->m_cacheUpdate;
		newTbdef->m_columns[i]->m_cacheUpdate = false;
	}

	//禁止cacheUpdate
	newTbdef->m_cacheUpdate = false;

	return newTbdef;
}

/**
 * 创建临时表
 * @param session
 * @param tempTableDefInfo
 * @param newTbdef
 * @return 
 */
Table* TblMntAlterColumn::createTempTable(Session *session, TableDef *newTbdef) throw(NtseException) {
	// 得到基础路径
	string tablePath(m_table->getPath());
	string basePath(m_db->getConfig()->m_basedir);
	string tableFullPath = basePath + NTSE_PATH_SEP + tablePath;

	u64 lsn = m_table->m_db->getTxnlog()->tailLsn();
	char* lsnBuffer = (char*)alloca(Limits::LSN_BUFFER_SIZE*sizeof(char));
	sprintf(lsnBuffer, I64FORMAT"u", lsn);
	// 得到临时表路径
	string tmpTablePath = tablePath + Limits::NAME_TEMP_TABLE + lsnBuffer; // 相对于baseDir的路径
	string tmpTableFullPath = basePath + NTSE_PATH_SEP + tmpTablePath;

	// 创建临时表
	// 删除老临时表文件
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
 * 表拷贝
 * @param session
 * @param tmpTb
 * @param ridmap
 * @param newTbdef
 */
void TblMntAlterColumn::copyTable(Session *session, Table *tmpTb, NtseRidMapping *ridmap, 
								  TableDef *newTbdef) throw(NtseException) {
	assert(m_table->getMetaLock(session) == IL_U);
	assert(m_table->getLock(session) == IL_NO);

	// 开始批量装载
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

		//采样&拷贝字典，替换临时表压缩组件和字典文件
		m_newHasDict = newTbdef->m_isCompressedTbl && m_table->hasCompressDict();//新表是否有字典	
		if (m_newHasDict) {
			createOrCopyDictForTmpTbl(session, m_table, tmpTb, m_keepOldDict);
		}

		m_conn->setStatus("Copying table");
	
		//通过表扫描插入原表记录到临时表的堆中
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
 * 通过表扫描插入原表记录到临时表的堆中
 * @param session
 * @param tmpTb
 * @param ridmap
 */
void TblMntAlterColumn::copyRowsByScan(Session *session, Table *tmpTb, 
									   NtseRidMapping *ridmap) throw(NtseException) {
	MemoryContext *mtx = session->getMemoryContext();
	
	bool connAccuScan = session->getConnection()->getLocalConfig()->m_accurateTblScan;
	session->getConnection()->getLocalConfig()->m_accurateTblScan = true;

	// 暂时将session中的事务置为NULL，读原表记录时需要读取大对象
	TNTTransaction *trx = session->getTrans();
	session->setTrans(NULL);

	//记录旧TableDef的列
	u16 *columns = (u16 *)mtx->alloc(sizeof(u16) * m_table->getTableDef(true, session)->m_numCols);
	for (u16 i = 0; i < m_table->m_tableDef->m_numCols; ++i) {
		columns[i] = i;
	}

	// 生成操作临时表的辅助session，并行遍历用
	Session *tmpSess = m_db->getSessionManager()->allocSession("TblMntAlterColumn::alterTable", m_conn);
	tmpSess->disableLogging(); // 不记日志

	TblScan *scanHdl = m_table->tableScan(session, OP_READ, 
		m_table->getTableDef(true, session)->m_numCols, columns, false); //不加表锁，不可能异常

	// 开始表扫描
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
		//临时表还不带索引，所以不会有唯一性冲突
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
 * 不带索引的日志回放
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

	// 记录回放结束日志
	LsnType lsnEnd = getLsnPoint(session, true, false);

	// 对于lsnstart到lsnend之间日志进行回放（此时无索引）
	LogReplay replay(m_table, m_db, lsnStart, lsnEnd);
	replay.start();

	m_db->getSyslog()->log(EL_DEBUG, "start processing log.");

	processLogToTable(session, &replay, m_convert, tmpTb, ridMap);
	//此时可能还存在未完成的事务，但replay end和析构后，会将未完成的事务数据也释放掉
	//所以必须记录最小未完成事务的begin trx lsn
	LsnType minUnfinishTxnLsn = replay.getMinUnfinishTxnLsn();
	if (minUnfinishTxnLsn != INVALID_LSN) {
		assert(minUnfinishTxnLsn <= lsnEnd);
		lsnEnd = minUnfinishTxnLsn;
	}
	replay.end();

	// 释放之前的日志
	lsnStart = lsnEnd;
	int oldOlLsnHdl = *olLsnHdl;
	*olLsnHdl = m_db->getTxnlog()->setOnlineLsn(lsnStart);
	assert(olLsnHdl >= 0);
	m_db->getTxnlog()->clearOnlineLsn(oldOlLsnHdl);

	return lsnEnd;
}

/**
 * 带索引的日志回放
 * @post 返回时表已经加上表IL_S锁
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
		// 刷出数据
		if (startFlush) {
			m_table->flush(session);
			tmpTb->flush(session);
		}
		// 再次记录lsn
		lsnEnd = getLsnPoint(session, true, false);
		// 重放start到end这段日志
		LogReplay replayAgain(m_table, m_db, lsnStart, lsnEnd);
		replayAgain.start();
		processLogToTable(session, &replayAgain, m_convert, tmpTb, ridMap);

		//此时可能还存在未完成的事务，但replay end和析构后，会将未完成的事务数据也释放掉
		//所以必须记录最小未完成事务的begin trx lsn
		LsnType minUnfinishTxnLsn = replayAgain.getMinUnfinishTxnLsn();
		if (minUnfinishTxnLsn != INVALID_LSN) {
			assert(minUnfinishTxnLsn <= lsnEnd);
			lsnEnd = minUnfinishTxnLsn;
		}

		replayAgain.end();
		time2 = System::fastTime();
		// 设置lsnstart，释放已经回放的日志
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

	// 最后回放
	assert(session->getTrans() != NULL || m_table->getLock(session) == IL_S);
	assert(m_table->getMetaLock(session) >= IL_U);
	m_table->flush(session);
	lsnEnd = getLsnPoint(session, false, false);
	LogReplay replayFinal(m_table, m_db, lsnStart, lsnEnd);
	replayFinal.start();
	processLogToTable(session, &replayFinal, m_convert, tmpTb, ridMap);
	//此时必须不存在未完成的ntse事务
	assert(replayFinal.getMinUnfinishTxnLsn() == INVALID_LSN);
	replayFinal.end();
	tmpTb->flush(session);

	// 设置onlineLSN恢复
	m_db->getTxnlog()->clearOnlineLsn(*olLsnHdl);
	*olLsnHdl = INVALID_TOKEN;

	return lsnEnd;
}

/**
 * 表文件替换
 * @param tmpTbleHasLob
 * @param origTableFullPath
 * @param tmpTableFullPath
 */
void TblMntAlterColumn::replaceTableFile(bool tmpTbleHasLob, string &origTableFullPath, 
										 string &tmpTableFullPath) {
	m_conn->setStatus("Replace table files");

	// 替换文件
	int numTmpFiles = Limits::EXTNUM;//临时文件数目
	std::vector<const char *> removeFileSuffix;
	for (int i = 0; i < numTmpFiles; ++i) {
		if (!isNewTbleHasDict() && !System::stricmp(Limits::NAME_GLBL_DIC_EXT, Limits::EXTS[i])) {//如果没有字典文件，则跳过
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
 * 索引重建
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
 * 还原表定义设置
 * @param tmpTb
 * @param tempInfo
 * @param newTbdef
 */
void TblMntAlterColumn::restoreTblSetting(Table *tmpTb, const TempTableDefInfo *tempInfo, 
										  TableDef *newTbdef) {
    assert(NULL != tempInfo->m_indexUniqueMem);

	m_conn->setStatus("Restore table setting");

	// 恢复索引的唯一性约束
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
	// 恢复列的cacheUpdate
	assert(newTbdef->m_numCols == tmpTb->m_tableDef->m_numCols);
	tmpTb->m_tableDef->m_cacheUpdate = false;
	bool *cacheUpdateCol = tempInfo->m_cacheUpdateCol;
	for (u16 i = 0; i < newTbdef->m_numCols; ++i) {
		tmpTb->m_tableDef->m_columns[i]->m_cacheUpdate = cacheUpdateCol[i];
		if (cacheUpdateCol[i])
			tmpTb->m_tableDef->m_cacheUpdate = true;
	}

	// 恢复mms设置
	tmpTb->m_tableDef->m_useMms = tempInfo->m_newTbUseMms;
}

/**
 * 生成临时表的字典(拷贝或重新创建)
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
		if (onlyCopy) {//拷贝旧字典
			assert(m_table->hasCompressDict());
			assert(origTb->m_records->hasValidDictionary());
			newDic = RCDictionary::copy(origTb->m_records->getDictionary(), tmpTb->getTableDef()->m_id);
		} else {//采样新字典
			newDic = createNewDictionary(session, m_db, origTb);
		}

		if (NULL != newDic) {
			string tempDictFilePath = tmpTableFullPath + Limits::NAME_TEMP_GLBL_DIC_EXT;
			tmpTb->createTmpCompressDictFile(tempDictFilePath.c_str(), newDic);

			tmpTb->m_records->resetCompressComponent(tmpTableFullPath.c_str());
			newDic->close();
			delete newDic;
			newDic = NULL;

			//删除临时字典文件
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
 * 采样创建新字典
 * @param session       会话
 * @param db            表所属数据库
 * @param table         要采样的表
 * @return              新字典
 * @throw NtseException 采样失败
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
 * 新表是否含有字典文件
 * @return 
 */
bool TblMntAlterColumn::isNewTbleHasDict() const {
	return m_newHasDict;
}

/**
 * 更改堆类型操作构造函数
 * @param table 要操作的表
 * @param conn  连接
 */
TblMntAlterHeapType::TblMntAlterHeapType(Table *table, Connection *conn, bool *cancelFlag) 
	: TblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag) {
}

TblMntAlterHeapType::~TblMntAlterHeapType() {
}

/**
 * 重载TblMntAlterColumn::preAlterTblDef, 更新堆类型
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
	
	//更改堆类型
	assert(!newTbdef->m_isCompressedTbl);
	assert(newTbdef->m_recFormat == REC_FIXLEN && newTbdef->m_origRecFormat == REC_FIXLEN);
	newTbdef->m_recFormat = REC_VARLEN;
	newTbdef->m_fixLen = false;

	return newTbdef;
}

/**
 * TblMntOptimizer构造函数
 * @param table 要操作的表
 * @param conn  数据库连接
 * @param cancelFlag 操作取消标志
 * @param keepOldDict 是否保留原字典
 */
TblMntOptimizer::TblMntOptimizer(Table *table, Connection *conn, bool *cancelFlag, bool keepOldDict) 
		: TblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag, keepOldDict) {
}

TblMntOptimizer::~TblMntOptimizer() {
}

/**
 * 利用ntse定长表实现的RowId映射表
 * @param db 数据库
 * @param session 会话
 * @param table 源数据表
 * @throw NtseException 临时表创建出错
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

/** 析构函数 */
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
	// 暂时不创建索引
	mapTbDef = builder->getTableDef();
	assert(mapTbDef->m_id == tempTid);
	mapTbDef->m_useMms = false;
	// 删除老的临时表，如果有的话。
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

	// 创建新的临时表
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
	// 打开表
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
 * 初始化批量载入数据，批量载入数据时插入记录不创建索引，所以速度相对较快
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
 * 完成批量载入数据，创建索引
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

	// 映射表不会有大对象
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

	//映射表不会有大对象
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
 * 开始一个映射表扫描
 */
void NtseRidMapping::startIter() {
	assert(!m_scanHdl);
	m_scanHdl = m_map->tableScan(m_session, OP_READ, m_subRec->m_numCols, m_subRec->m_columns);
}

/**
 * 获取下一条配对
 * @param mapped out	映射RowId值，传出
 * @return				原RowId值
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
 * 终止表扫描
 */
void NtseRidMapping::endIter() {
	m_map->endScan(m_scanHdl);
	m_scanHdl = NULL;
}

/**
 * 表扫描统计映射表中的记录个数
 * @return         记录数目
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
 * 根据映射值获得原值
 * @param mapped		映射RowId
 * @return				原的RowId值
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
