/**
* TNT引擎，表处理模块。
*
* @author 何登成
*/

#include "api/TNTTable.h"
#include "api/TblArgAlter.h"
#include "api/Table.h"
#include "api/TNTTblMnt.h"
#include "btree/IndexKey.h"
#include "btree/MIndexKey.h"

namespace tnt {

/**
* 打开一个TNTTableBase对象；若对象第一次打开，则调用实际打开函数；若对象已经打开，则重设其中的NTSE指针
* @param session	session对象
* @param db			TNTDatabase对象
* @param tableDef	NTSE TableDef对象
* @param lobStorage NTSE大对象管理器
* @param drsIndice	NTSE索引对象
*/
void TNTTableBase::open(Session *session, TNTDatabase *db, TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice) throw(NtseException) {
	//TableDef **tableDef = table->getTableDefAddr();
	assert(tableDef != NULL && *tableDef != NULL);
	assert((*tableDef)->getTableStatus() != TS_CHANGING && (*tableDef)->getTableStatus() != TS_SYSTEM
		&& (*tableDef)->getTableStatus() != TS_ONLINE_DDL);
	m_tableId = (*tableDef)->m_id;
	if (m_opened) {
		assert(m_indice != NULL);
		assert(m_records != NULL);

		m_indice->reOpen(tableDef, drsIndice->getLobStorage(), drsIndice);
		m_records->replaceComponents(tableDef);
		return;
	}

	assert(m_indice == NULL && m_records == NULL);
	
	m_records= MRecords::open(tableDef, db->getTNTIMPageManager(), db->getVersionPool());
	m_indice = TNTIndice::open(db, session, tableDef, lobStorage, drsIndice, m_records->getHashIndex());

	// 如果内存索引初始化失败（内存不足）
	if(!m_indice) {
		m_records->close(session, true);
		delete m_records;
		m_records = NULL;
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "Memory index init fail");
	}

	m_opened = true;
	m_closed = false;
}

/**
* 真正关闭一个TNTTableBase内存结构
* @param session	session对象
* @param flush		标识是否要进行flush操作
*
*/
void TNTTableBase::close(Session *session, bool flush) {
	if (m_opened && !m_closed) {
		if (m_indice != NULL) {
			m_indice->close(session);
			delete m_indice;
			m_indice = NULL;
		}
		if (m_records != NULL) {
			m_records->close(session, flush);
			delete m_records;
			m_records = NULL;
		}
		m_tableId = TableDef::INVALID_TABLEID;
	}

	assert(m_indice == NULL && m_records == NULL && TableDef::INVALID_TABLEID == m_tableId);
	m_opened = false;
	m_closed = true;
}

/**
* 在关闭表时，TNTTableBase内存对象并不关闭，此时需要将内存对象中指向NTSE的指针重设
* @param session	session对象
*/
/*
void TNTTableBase::closeNtseRef(Session *session) {
	m_indice->close(session, false);
}
*/


TNTTable::TNTTable() : m_autoincMutex("AutoInc Mutex", __FILE__, __LINE__) {
	m_estimateRows	= 0;
	m_tabLockHead	= NULL;
	m_lockCnt		= 0;
//	m_tabStat		= new TNTTableStat();

	m_autoinc		= 0;
	m_autoincTrx	= NULL;
	m_reqAutoincLocks = 0;
	m_autoincLock	= NULL;
	m_purgeStatus   = PS_NOOP;
}

TNTTable::~TNTTable() {
}

/**	创建TNT内存表
*
*/
void TNTTable::create(TNTDatabase *db, Session *session, const char *path, TableDef *tableDef) throw(NtseException) {
	// 当前，TNTTable不需要支持create操作
	UNREFERENCED_PARAMETER(db);
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(path);
	UNREFERENCED_PARAMETER(tableDef);
}

/**	删除TNT内存表
*	@db		TNT Database对象
*	@path	表路径
*/
void TNTTable::drop(TNTDatabase *db, const char *path) throw(NtseException) {
	// DDL方法移入TNTTableBase
	UNREFERENCED_PARAMETER(db);
	UNREFERENCED_PARAMETER(path);
}

/**	打开TNT内存表
*	@db			TNT Database对象
*	@session	session对象
*	@ntseTable	NTSE表对象
*	@tableBase	TNT表基本结构对象
*
*/
TNTTable* TNTTable::open(TNTDatabase *db, Session *session, Table *ntseTable, TNTTableBase *tableBase) throw(NtseException) {
	assert(tableBase != NULL);
	assert(ntseTable != NULL);
	
	TNTTable *table = new TNTTable();
	try {
		tableBase->open(session, db, ntseTable->getTableDefAddr(), ntseTable->getLobStorage(), ntseTable->getIndice());
	} catch(NtseException &e) {
		delete table;
		throw e;
	}

	// 设置属性信息
	table->m_db			= db;
	table->m_tab		= ntseTable;
	table->m_mrecs		= tableBase->m_records;
	table->m_indice		= tableBase->m_indice;
	table->m_tabBase	= tableBase;	

	return table;
}

/** 在Open表时，重新统计表中的记录数量
*** 目前，先直接调用NTSE Table提供的方法，不统计TNT内存中删除的记录数量
* @param session
* 
*/
void TNTTable::refreshRows(Session* session) {
	m_tab->refreshRows(session);
}

/**	重命名一张内存表
*	@db			TNT Database对象
*	@session	session对象
*	@oldPath	原表名
*	@newPath	新表名
*/
void TNTTable::rename(TNTDatabase *db, Session *session, const char *oldPath, const char *newPath) {
	// DDL方法移入TNTTableBase
	UNREFERENCED_PARAMETER(db);
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(oldPath);
	UNREFERENCED_PARAMETER(newPath);
}

/**	关闭一张TNT内存表
*	@session			session对象
*	@flushDirty			是否将内存更新purge到NTSE
*	@closeComponents	是否将内存对象关闭
*	@声明：				一期，以上两个参数无用
*/
void TNTTable::close(Session *session, bool flushDirty, bool closeComponents) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(flushDirty);
	UNREFERENCED_PARAMETER(closeComponents);
	
	// 将内存TNTTableBase对象中涉及到的NTSE的对象指针设置为NULL
	// m_tabBase->closeNtseRef(NULL, NULL);
	m_tabBase->m_records->replaceComponents(NULL);
}

/**	truncate TNT内存表
*	@session			session对象
*/
void TNTTable::truncate(Session *session) {
	assert(m_tabBase->m_opened && !m_tabBase->m_closed);

	// 关闭TNT内存heap与indice，释放空间
	m_tabBase->close(session, false);
	// 由于m_tabBase地址未发生变化，因此不需要重新创建
	// 直接打开TNT内存heap与indice，此时均为空
	m_tabBase->open(session, m_db, m_tab->getTableDefAddr(), m_tab->getLobStorage(), m_tab->getIndice());

	// 重新设置内存TNTIndice，MRecords
	m_indice = m_tabBase->m_indice;
	m_mrecs	 = m_tabBase->m_records;

	return;
}

/**	创建索引
 *	@param session	session对象
 *	@param indexDefs	新索引定义
 *	@param numIndice	要增加的索引个数，当online为false只能为1
 */
void TNTTable::addIndex(Session *session, const IndexDef **indexDefs, u16 numIndice) throw(NtseException) {
	// 1.当前创建索引是ntse创建索引
	if (indexDefs[0]->m_online) {
		TNTTblMntAlterIndex alter(this, numIndice, indexDefs, 0, NULL);
		alter.alterTable(session);
		return;
	}

	TNTTransaction *trx	= session->getTrans();
	uint timeout = session->getNtseDb()->getConfig()->m_tlTimeout * 1000;
	// 1. 获取Meta Lock
	// 加上U锁，禁止所有其他操作对于表元数据的修改
	ILMode oldMetaLock = getMetaLock(session);
	try {
		upgradeMetaLock(session, oldMetaLock, IL_U, timeout, __FILE__, __LINE__);
	} catch (NtseException &e) {
		throw e;
	}

	//2.当前创建索引是mysql创建索引
	string idxInfo;
	//当前表上是否含有未完成的在线索引
	if (m_tab->hasOnlineIndex(&idxInfo)) {
		if (m_tab->isDoneOnlineIndex(indexDefs, numIndice)) {
			//判断在线建索引顺序是否正确
			if (!m_tab->isOnlineIndexOrder(indexDefs, numIndice)) {
				downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
				NTSE_THROW(NTSE_EC_GENERIC, "Add online index order was wrong, %s.", idxInfo.c_str());
			}
			//完成快速创建在线索引的最后步骤
			session->setTrans(NULL); //将事务置为NULL，因为dualFlushAndBump中需要升级meta lock
			m_tab->dualFlushAndBump(session, timeout); //这个函数会将metaLock从IL_U升为IL_X
			session->setTrans(trx);
			for (uint i = 0; i < numIndice; i++) {
				m_tab->getTableDef()->getIndexDef(indexDefs[i]->m_name)->m_online = false;
			}

			m_tab->writeTableDef();
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		} else {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			NTSE_THROW(NTSE_EC_GENERIC, "Add online index was not completely, %s.", idxInfo.c_str());
		}

		return;
	}
	
	// 2. 加上表数据锁，保证只有只读操作可以访问
	ILMode oldLock = m_tab->getLock(session);
	u16 tableId = m_tab->getTableDef(true, session)->m_id;
	try {
		//在trx为null的情况下，此时是ntse操作，table lock已经在ntse_handler::extern_lock中加锁
		if (trx != NULL) {
			//如果addIndex是由ha_tnt调用，那么此时S表锁已经存在，但表锁允许可重入性
			trx->lockTable(TL_S, tableId);
		} else {
			m_tab->upgradeLock(session, oldLock, IL_S, timeout, __FILE__, __LINE__);
		}
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// 在Meta U Lock的保护下，进行外存创建索引的第一阶段操作
	try {
		m_tab->addIndexPhaseOne(session, numIndice, indexDefs);
	} catch (NtseException &e) {
		m_tab->downgradeLock(session, m_tab->getLock(session), oldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// 升级Meta U Lock至X Lock，修改TableDef信息，以及创建内存索引，此时短暂禁止读操作
	// TODO: 此时是否需要将表锁升级为X锁？
	// 目前任务不需要，因为无论是MySQL上层操作，还是TNT内部操作，在操作表时，都会加上
	// Meta S Lock。此时将Meta Lock升级为X锁，也就意味着表上的其他并发DML操作均不存在
	upgradeMetaLock(session, getMetaLock(session), IL_X, -1, __FILE__, __LINE__);

	try {
		m_tab->addIndexPhaseTwo(session, numIndice, indexDefs);

		addMemIndex(session, numIndice, indexDefs);
	} catch (NtseException &e) {
		m_tab->downgradeLock(session, m_tab->getLock(session), oldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// 3. 释放Lock
	m_tab->downgradeLock(session, m_tab->getLock(session), oldLock, __FILE__, __LINE__);
	downgradeMetaLock(session, IL_X, oldMetaLock, __FILE__, __LINE__);
}

/**	删除索引
 *	@param session	session对象
 *	@param idx		要删除的是第几个索引
 */
void TNTTable::dropIndex(Session *session, uint idx) throw(NtseException) {
	uint timeout = session->getNtseDb()->getConfig()->m_tlTimeout * 1000;
	// 1. 获取Meta Lock和ntse锁模式
	ILMode oldMetaLock = getMetaLock(session);
	ILMode ntseOldLock = m_tab->getLock(session);

	// 2. 升级MetaLock
	try {
		upgradeMetaLock(session, oldMetaLock, IL_U, timeout, __FILE__, __LINE__);
	} catch (NtseException &e) {
		throw e;
	}

	// 3. 获取Table Lock
	u16 tableId	= m_tab->getTableDef(true, session)->m_id;
	TNTTransaction *trx = session->getTrans();
	try {
		if (trx != NULL) {
			//加lock table S为了等待操作过该表的活跃事务的结束
			trx->lockTable(TL_S, tableId);
		}
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// 4. NTSE刷数据
	TableDef * tableDef = m_tab->getTableDef();
	if (tableDef->m_indice[idx]->m_primaryKey && tableDef->m_cacheUpdate) {
		assert(tableDef->m_useMms);
		//升级X锁
		try {
			upgradeMetaLock(session, IL_U, IL_X, timeout, __FILE__, __LINE__);
		} catch(NtseException &e) {
			downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
		try {
			m_tab->dropIndexFlushPhaseOne(session);
		}catch (NtseException &e){
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
		// 此时已经加了表元数据X锁并刷出了脏数据
		assert(getMetaLock(session) == IL_X);
	} else {
		try {
			m_tab->dropIndexFlushPhaseTwo(session, ntseOldLock);
		} catch(NtseException &e) {
			downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
			throw e;
		}
	}
	session->getNtseDb()->bumpFlushLsn(session, tableDef->m_id);
	// 5. 加Meta X锁修改TableDef
	// 降级事务锁
	try {
		if (trx != NULL) {
			trx->lockTable(TL_IS, tableId);
		}
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}
	if (NULL != trx) {
		trx->unlockTable(TL_S, tableId);	//放掉S锁，持有IS锁
	}

	try {
		upgradeMetaLock(session, getMetaLock(session), IL_X, timeout, __FILE__, __LINE__);
		//调用外存表的删除索引第一阶段修改tableDef
		m_tab->dropIndexPhaseOne(session, idx, ntseOldLock);

		//只有等外存索引成功后才能操作内存索引，
		//调用内存表的删索引第一阶段，修改索引数组，并且从内存中删除索引数据
		dropMemIndex(session, idx);
		assert(m_indice->getMemIndice()->getIndexNum() == m_indice->getDrsIndice()->getIndexNum());
	} catch (NtseException &e){
		m_tab->downgradeLock(session, m_tab->getLock(session), ntseOldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		throw e;
	} 

	// 6. 降级meta锁为U锁
	downgradeMetaLock(session, getMetaLock(session), IL_U, __FILE__, __LINE__);

	// 7. 删除外存索引数据
	m_tab->dropIndexPhaseTwo(session, idx, ntseOldLock);

	// 释放DDL Lock
	m_tab->downgradeLock(session, m_tab->getLock(session), ntseOldLock, __FILE__, __LINE__);
	downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
}

/**	创建内存索引
*	@session	session对象
*	@numIndice	要增加的索引个数，当online为false只能为1
*	@indexDefs	新索引定义
*/
void TNTTable::addMemIndex(Session *session, u16 numIndice, const IndexDef **indexDefs) {
	u16 i = 0;
	int idxNo = 0;

	// 调用TNTIndice提供的方法，增加索引
	TableDef *tableDef = m_tab->getTableDef();
	// 传入内存索引的indexDef必须使用tableDef中的indexDef
	for (i = 0; i < numIndice; i++) {
		idxNo = tableDef->m_numIndice - numIndice + i;
		const IndexDef *indexDef = tableDef->getIndexDef(idxNo);
		assert(!strcmp(indexDef->m_name, indexDefs[i]->m_name));
		m_indice->createIndexPhaseTwo(session, indexDef, idxNo);
	}
	assert(m_indice->getIndexNum() == m_indice->getDrsIndice()->getIndexNum());

	//为索引导入数据
	MHeapRec *heapRec = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	u16 *columns = (u16 *)ctx->alloc(tableDef->m_numCols*sizeof(u16));
	for (i = 0; i < tableDef->m_numCols; i++) {
		columns[i] = i;
	}
	MHeapScanContext *scanCtx = m_mrecs->beginScan(session);
	while(true) {
		//此处需要获取堆内最新记录
		if(!m_mrecs->getNext(scanCtx, NULL)) 
			break;
		McSavepoint msp(ctx);
		heapRec = scanCtx->m_heapRec;
		void *buf = ctx->alloc(sizeof(SubRecord));
		SubRecord *SubRec = new (buf)SubRecord(REC_REDUNDANT, tableDef->m_numCols, columns, heapRec->m_rec.m_data, tableDef->m_maxRecSize, heapRec->m_rec.m_rowId);
		RowIdVersion version = m_mrecs->getHashIndex()->get(heapRec->m_rec.m_rowId, ctx)->m_version;
		//插入索引,索引项，这里的version用的是什么？
		for (i = 0; i < numIndice; i++) {
			McSavepoint lobSavepoint(session->getLobContext());

			u8 idxNo = tableDef->m_numIndice - numIndice + i;
			MIndex *mIndex = m_indice->getMemIndice()->getIndex(idxNo);
			IndexDef *indexDef = tableDef->getIndexDef(idxNo);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, tableDef, indexDef, m_tab->getLobStorage(), SubRec, &lobArray);
			}
			// 将记录转化为PAD格式键，并负责填充大对象内容
			SubRecord *key = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), SubRec, &lobArray, tableDef, indexDef);
			mIndex->insert(session, key, version);
		}
	}
	m_mrecs->endScan(scanCtx);
}

/**	删除内存索引
*	@session	session对象
*	@idx		要删除的是第几个索引
*
*/
void TNTTable::dropMemIndex(Session *session, uint idx) {
	// 调用TNTIndice提供的方法，删除索引
	m_indice->dropPhaseOne(session, idx);
	//m_indice->dropPhaseTwo(session, idx);
}

/**	优化表数据
*	@session	session对象
*	@keepDict	是否指定了保留旧字典(如果有全局字典的话)
*	
*/
void TNTTable::optimize(Session *session, bool keepDict, bool *newHasDic, bool *cancelFlag) throw(NtseException) {
	if (m_tab->getTableDef()->m_indexOnly)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index only table can not be optimized");

	// 检测表中原有的索引中是否包含大对象列
	// TODO: 目前暂不支持包含大对象列的在线操作
	for (int i = 0; i < m_tab->getTableDef()->m_numIndice; i++) {
		if (m_tab->getTableDef()->getIndexDef(i)->hasLob()) {
			NTSE_THROW(NTSE_EC_GENERIC, "Table with Lob/LongVar Index can not be optimized");
		}
	}

	// OPTIMIZE通过一个即不增加任何字段也不删除任何字段的在线增删字段操作实现
	TNTTblMntOptimizer optimizer(this, session->getConnection(), cancelFlag, keepDict);
	optimizer.alterTable(session);

	(*newHasDic) = optimizer.isNewTbleHasDict();
}

/**
*
*
*/
void TNTTable::verify(Session *session) throw(NtseException) {
	UNREFERENCED_PARAMETER(session);
	// DDL方法移入TNTTableBase
	return;
}

void TNTTable::flush(Session *session) {
	UNREFERENCED_PARAMETER(session);
	// DDL方法移入TNTTableBase
	return;
}

// 在线创建索引，需要知会TNT，TNT采用purge内存+ntse在线创建索引的方式完成
void TNTTable::setOnlineAddingIndice(Session *session, u16 numIndice, const IndexDef **indice) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(numIndice);
	UNREFERENCED_PARAMETER(indice);
	// DDL方法移入TNTTableBase
	return;
}

void TNTTable::resetOnlineAddingIndice(Session *session) {
	UNREFERENCED_PARAMETER(session);
	// DDL方法移入TNTTableBase
	return;
}

void TNTTable::alterTableArgument(Session *session, const char *name, const char *valueStr, int timeout, bool inLockTables) throw(NtseException) {
	assert(NULL != session);
	assert(NULL != name);
	assert(NULL != valueStr);
	assert(session->getConnection()->isTrx());

	TableArgAlterCmdType cmdType = TableArgAlterHelper::cmdMapTool.getCmdMap()->get(name);
	switch (cmdType) {
		case USEMMS:
			m_tab->alterUseMms(session, Parser::parseBool(valueStr), timeout);
			break;
		case CACHE_UPDATE:
			m_tab->alterCacheUpdate(session, Parser::parseBool(valueStr), timeout);
			break;
		case UPDATE_CACHE_TIME:
			m_tab->alterCacheUpdateTime(session, Parser::parseInt(valueStr), timeout);
			break;
		case CACHED_COLUMNS:
			m_tab->alterCachedColumns(session, valueStr, timeout);
			break;
		case COMPRESS_LOBS:
			m_tab->alterCompressLobs(session, Parser::parseBool(valueStr), timeout);
			break;
		case HEAP_PCT_FREE:
			m_tab->alterHeapPctFree(session, (u8)Parser::parseInt(valueStr), timeout);
			break;
		case SPLIT_FACTORS:
			m_tab->alterSplitFactors(session, valueStr, timeout);
			break;
		case INCR_SIZE:
			m_tab->alterIncrSize(session, (u16)Parser::parseInt(valueStr), timeout);
			break;
		case COMPRESS_ROWS:
			m_tab->alterCompressRows(session, Parser::parseBool(valueStr), timeout);
			break;
		case FIX_LEN:
			alterHeapFixLen(session, Parser::parseBool(valueStr), timeout);
			break;
		case COLUMN_GROUPS:
			m_tab->alterColGrpDef(session, valueStr, timeout);
			break;
		case COMPRESS_DICT_SIZE:
		case COMPRESS_DICT_MIN_LEN:
		case COMPRESS_DICT_MAX_LEN:
		case COMPRESS_THRESHOLD:
			m_tab->alterDictionaryArg(session, cmdType, (uint)Parser::parseInt(valueStr), timeout);
			break;
		case SUPPORT_TRX:
			alterSupportTrxStatus(session, Parser::parseBool(valueStr)? TS_TRX: TS_NON_TRX, timeout, inLockTables);
			break;
		default:
			//Hash表不能匹配时返回 0 即： MIN_TBL_ARG_CMD_TYPE_VALUE
			assert(cmdType == MIN_TBL_ARG_CMD_TYPE_VALUE);
			NTSE_THROW(NTSE_EC_GENERIC, "Wrong Command type.");
			break;
	}
	try {
		m_tab->getTableDef()->check();
	} catch (NtseException &e) {
		m_db->getTNTSyslog()->log(EL_PANIC, "Invalid table definition: %s", e.getMessage());
		assert(false);
	}
}

/**
 * 修改堆类型
 * 
 * 只支持从定长堆到变长堆的单向修改，由于是通过表拷贝去做，可能耗时较长
 * 跟其他表在线修改操作不同的是，修改堆类型不会调用preAlter先刷脏数据和写日志，而
 * 只是在TblMntAlterColumn::alterTable里面写日志，如果在TblMntAlterColumn::alterTable
 * 中写日志之前系统崩溃，不会进行redo修改堆类型的操作(注：TblMntAlterColumn::alterTable
 * 只是在替换表文件之前才写日志)
 *
 * @param session       会话
 * @param fixlen        新堆类型是否是定长类型
 * @param timeout       表锁超时时间，单位秒
 * @throw NtseException 将变长堆改为定长堆，或者在拷贝表时出错
 */
void TNTTable::alterHeapFixLen(Session *session, bool fixlen, int timeout) throw(NtseException) {
	assert(NULL != session);
	assert(!session->getTrans());
	TableDef *tableDef = m_tab->getTableDef();
	//因为此时session中没有trx，所以可以使用TblLockGuard，否则unlock会无效
	TblLockGuard guard;
	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	guard.attach(session, this->getNtseTable());

	m_tab->checkPermission(session, false);
	if (fixlen) {
		if (tableDef->m_recFormat == REC_FIXLEN) {//已经是定长堆
			assert(tableDef->m_fixLen);
			assert(tableDef->m_origRecFormat == tableDef->m_recFormat);
			return;
		} else {//不支持将变长堆改为定长堆
			NTSE_THROW(NTSE_EC_GENERIC, "Can't change variable length heap to fixed length !"); 
		}
	} else {
		if (tableDef->m_recFormat != REC_FIXLEN)//已经不是定长堆
			return;
	}

	//从定长表通过不增删列的表在线修改为变长表
	assert(tableDef->m_recFormat == REC_FIXLEN && tableDef->m_origRecFormat == REC_FIXLEN);
	assert(!fixlen);
	assert(getMetaLock(session) == IL_U);
	try {
		TNTTblMntAlterHeapType tableAlterHeapType(this, session->getConnection());
		tableAlterHeapType.alterTable(session);
	} catch (NtseException &e) {
		assert(getMetaLock(session) == IL_U);
		throw e;
	}
	assert(getMetaLock(session) == IL_U);
}

/**
 * 修改table是否支持事务操作
 *
 * @param session		会话。
 * @param supportTrx	是否支持事务操作。 true 表示支持事务操作，false表示不支持事务操作。
 * @param timeout		表锁超时时间，单位秒。
 * @param inLockTables  操作是否处于Lock Tables 语句之中
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
 */
void TNTTable::alterSupportTrxStatus(Session *session, TableStatus tableStatus, int timeout, bool inLockTables) throw(NtseException) {
	assert(session->getConnection()->isTrx());
	assert(!session->getTrans());
	assert(tableStatus == TS_NON_TRX || tableStatus == TS_TRX);

	//因为此时session中没有trx，所以可以使用TblLockGuard，否则unlock会无效
	TblLockGuard guard;
	m_tab->lockMeta(session, IL_X, timeout * 1000, __FILE__, __LINE__);
	guard.attach(session, m_tab);

	if (inLockTables) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "This Connection is in lock tables, you must unlock tables");
	}
	
	if (TS_TRX != m_tab->getTableDef()->getTableStatus()) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Transaction Connection can operate Transaction Table(%s) Only, Now Table Status is %s",
			m_tab->getTableDef()->m_name, m_tab->getTableDef()->getTableStatusDesc());
	}

	if (tableStatus == m_tab->getTableDef()->getTableStatus()) {
		return;
	}
	//运用加table的X锁等待作用于该表的事务全部结束,同时因为持有meta锁，所以不会有新生事务操作该表
	TNTTransaction *trx = NULL;
	try {
		trx = m_db->getTransSys()->allocTrx();
	} catch (NtseException &e) {
		m_tab->unlockMeta(session, IL_X);
		throw e;
	}
	trx->startTrxIfNotStarted(session->getConnection());
	try {
		trx->lockTable(TL_X, m_tab->getTableDef()->m_id);
	} catch (NtseException &e) {
		trx->commitTrx(CS_INNER);
		m_db->getTransSys()->freeTrx(trx);
		throw e;
	}
	trx->commitTrx(CS_INNER);
	m_db->getTransSys()->freeTrx(trx);

	//由于TS_CHANGING是不序列化至外存，所以恢复过程中不可能出现TS_CHANGING状态
	m_tab->getTableDef()->setTableStatus(TS_CHANGING);
	guard.detach();
	m_tab->unlockMeta(session, IL_X);

	//如果切换成非事务状态，需要内存索引为空
	if (tableStatus == TS_NON_TRX) {
		while (true) {
			if (getMRecSize() == 0) {
				break;
			}
			Thread::msleep(1000);
		}
	}
	m_tab->alterTableStatus(session, tableStatus, timeout, inLockTables);
}

/**	开始表扫描
*	@session		session对象
*	@opType			操作类型
*	@opInfo			本次扫描信息
*	@numReadCols	要读取的属性数，不能为0
*	@readCols		要读取的各属性号，从0开始，递增排好序。不能为NULL
*	@tblLock		是否要加表锁
*	@lobCtx			用于存储所犯获得大对象内容的内存分配上下文，若为NULL，则使用Session::getLobContext
*					当使用Session::getLobContext保存大对象内容时，在获取下一条记录或endScan时，这一内存
*					会被自动释放，若保存大对象内容的内存分配上下文由外部指定，则由外部负责释放
*	@throw	NtseException	加表锁超时
*	@return			扫描句柄
*/
TNTTblScan* TNTTable::tableScan(Session *session, OpType opType, TNTOpInfo *opInfo, u16 numReadCols, u16 *readCols, bool tblLock, MemoryContext *lobCtx) throw(NtseException) {
	UNREFERENCED_PARAMETER(tblLock);

	TNTTblScan *scan = beginScan(session, ST_TBL_SCAN, opType, numReadCols, readCols, lobCtx);
	scan->m_opInfo = opInfo;

	// 设置 TNT 事务锁模式
	scan->determineScanLockMode(opInfo->m_selLockType);

	// 设置 NTSE 行锁模式：一定是Shared
	LockMode ntseRowLockMode = Shared;
	scan->m_pRlh = &scan->m_rlh;

	Records::Scan *recScan = m_tab->getRecords()->beginScan(session, opType, ColList(numReadCols, readCols), lobCtx, 
		ntseRowLockMode, scan->m_pRlh, session->getConnection()->getLocalConfig()->m_accurateTblScan);
	scan->m_recInfo = recScan;
	// 初始化m_mysqlRow
	scan->m_mysqlRow= recScan->getMysqlRow();
	// 初始化m_redRow
	scan->m_redRow	= recScan->getRedRow();

	return scan;
}

/**	开始索引扫描
*	@session	session对象
*	@opType		操作类型
*	@opInfo		本次扫描信息
*	@cond		扫描条件，本函数会浅拷贝一份。若条件为空，则设置cond->m_key为NULL，
*				不允许cond->m_key不为NULL，但其m_data为NULL或n_numCols为0.
*	@numReadCols  要读取的属性数，不能为0	
*	@readCols	要读取的个属性号，从0开始，递增排好序。不能为NULL
*	@unique		是否为unique scan，目前通过range scan模拟实现unique scan
*	@tblLock	是否要加表锁
*	@lobCtx		用于存储所返回的大对象内容的内存分配上下文
*	@throw	NtseException	加表锁超时
*/
TNTTblScan* TNTTable::indexScan(Session *session, OpType opType, TNTOpInfo *opInfo, const IndexScanCond *cond, u16 numReadCols, u16 *readCols, bool unique, bool tblLock, MemoryContext *lobCtx) throw(NtseException) {
	UNREFERENCED_PARAMETER(tblLock);

	TNTTblScan *scan = beginScan(session, ST_IDX_SCAN, opType, numReadCols, readCols, lobCtx);
	scan->m_opInfo = opInfo;

	TableDef *tabDef = scan->m_tableDef;	
	MemoryContext *mc= session->getMemoryContext();

	scan->m_indexKey = cond->m_key;
	scan->m_indexDef = tabDef->m_indice[cond->m_idx];
	scan->m_index	 = m_indice->getTntIndex((u8)cond->m_idx);
	scan->m_singleFetch = cond->m_singleFetch;
	scan->m_coverageIndex = isCoverageIndex(tabDef, scan->m_indexDef, scan->m_readCols);

	scan->determineScanLockMode(opInfo->m_selLockType);

	if (scan->m_coverageIndex) {
		// TODO：这个索引提取器貌似没用
		scan->m_idxExtractor = SubToSubExtractor::createInst(session->getMemoryContext(), tabDef, scan->m_indexDef, scan->m_indexDef->m_numCols, 
			scan->m_indexDef->m_columns, numReadCols, readCols, KEY_COMPRESS, KEY_PAD, 
			cond->m_singleFetch ? 1:1000);
		void *p	= mc->alloc(sizeof(SubRecord));
		scan->m_redRow = new (p)SubRecord(REC_REDUNDANT, numReadCols, readCols, NULL, tabDef->m_maxRecSize); // 分配一个冗余格式子记录，用于覆盖扫描键值提取
	}
	
	void *p			 = mc->alloc(sizeof(SubRecord));
	scan->m_mysqlRow = new (p)SubRecord(REC_MYSQL, numReadCols, readCols, NULL, tabDef->m_maxMysqlRecSize);


	// idxKey 为PAD 格式，必须单独分配内存
	p				= mc->alloc(sizeof(SubRecord));
	byte * data		= (byte*) mc->alloc(scan->m_indexDef->m_maxKeySize + 1);
	scan->m_idxKey = new (p)SubRecord(KEY_PAD, scan->m_indexDef->m_numCols, scan->m_indexDef->m_columns, data, scan->m_indexDef->m_maxKeySize);
	
	// 创建一个BulkFetch对象，用于索引回表时，读取完整NTSE记录
	// 注意：回表读取完整NTSE记录，读到的记录存入fullRow，而非redRow
	ColList fullColList = ColList::generateAscColList(mc, 0, scan->m_tableDef->m_numCols);
	Records::BulkFetch *fetch = m_tab->getRecords()->beginBulkFetch(session, opType, fullColList, lobCtx, Shared);
	scan->m_recInfo = fetch;

	if (cond->m_singleFetch) {
		// TODO：此处后续需要修改，多版本的Unique scan，需要特殊处理
		assert(tabDef->m_indice[cond->m_idx]->m_unique 
			&& cond->m_key->m_numCols == tabDef->m_indice[cond->m_idx]->m_numCols);
		// return scan;
	}

	// 无论是range scan，还是unique scan，目前都用range scan句柄实现
	scan->m_indexScan = scan->m_index->beginScan(session, cond->m_key, scan->m_idxKey, unique, 
		cond->m_forward, cond->m_includeKey, opInfo->m_selLockType, scan->m_idxExtractor);

	return scan;
}

/**	开始指定RID取记录的定位扫描
*	@session		同上
*	@opType			同上
*	@numReadCols	同上
*	@readCols		同上
*	@tblLock		同上
*	@lobCtx			同上
*	@throw NtseException
*	@return 扫描句柄
*/
TNTTblScan* TNTTable::positionScan(Session *session, OpType opType, TNTOpInfo *opInfo, u16 numReadCols, u16 *readCols, bool tblLock, MemoryContext *lobCtx) throw(NtseException) {
	UNREFERENCED_PARAMETER(tblLock);

	MemoryContext *mc = session->getMemoryContext();
	
	TNTTblScan *scan = beginScan(session, ST_POS_SCAN, opType, numReadCols, readCols, lobCtx);
	scan->m_opInfo = opInfo;

	scan->determineScanLockMode(opInfo->m_selLockType);
	
	void *p			= mc->alloc(sizeof(SubRecord));
	scan->m_mysqlRow= new (p)SubRecord(REC_MYSQL, numReadCols, readCols, NULL, m_tab->getTableDef()->m_maxRecSize);

	// 创建一个BulkFetch对象，用于读取NTSE完整记录
	// 注意：回表读取完整NTSE记录，读到的记录存入m_fullRow，而非m_redRow
	ColList fullColList = ColList::generateAscColList(mc, 0, scan->m_tableDef->m_numCols);
	Records::BulkFetch *fetch = m_tab->getRecords()->beginBulkFetch(session, opType, fullColList, lobCtx, Shared);
	scan->m_recInfo = fetch;

	return scan;
}

/**	开始扫描，完成各类扫描共同的初始化
*	@session	session对象
*	@type		扫描类型
*	@opType		操作类型
*	@numReadCols  要读取的属性数，不能为0
*	@readCols	要读取的各属性号
*	@lobCtx		大对象MemoryContext
*/
TNTTblScan* TNTTable::beginScan(Session *session, ScanType type, OpType opType, u16 numReadCols, u16 *readCols, MemoryContext *lobCtx) {
	session->getLobContext()->reset();
	MemoryContext *mc = session->getMemoryContext();
	
	TNTTblScan *scan = allocScan(session, numReadCols, readCols, opType);
	scan->m_scanType = type;
	scan->m_trx = session->getTrans();
	scan->m_scanRowsType = TNTTblScan::getRowStatTypeForScanType(type);
	if (lobCtx == NULL) {
		scan->m_externalLobCtx = false;
		scan->m_lobCtx = session->getMemoryContext();
	}
	else {
		scan->m_externalLobCtx = true;
		scan->m_lobCtx	 = lobCtx;
	}

	// 初始化m_fullRow，所有scan都需要，因此在公共函数中初始化
	// 1. 用于存放undo中间结果，完整记录
	// 2. 用于取NTSE完整记录
	byte *data	= (byte *)mc->alloc(scan->m_tableDef->m_maxRecSize);
	void *p			= mc->alloc(sizeof(SubRecord));
	ColList fullColList = ColList::generateAscColList(mc, 0, scan->m_tableDef->m_numCols);
	scan->m_fullRow = new (p)SubRecord(REC_REDUNDANT, fullColList.m_size, fullColList.m_cols, data, scan->m_tableDef->m_maxRecSize);

	// 初始化m_fullRec，与m_fullRow公用m_data空间
	// 注意：m_fullRow其实是一个SubRecord，而m_fullRec则是完整记录Record
	p = mc->alloc(sizeof(Record));
	scan->m_fullRec = new (p)Record(INVALID_ROW_ID, REC_REDUNDANT, data, scan->m_tableDef->m_maxRecSize);

	if (m_tab->getRecords()->hasValidDictionary()) {//并且字典可用
		assert(m_tab->getRecords()->getRowCompressMng());
		scan->m_subExtractor = SubrecExtractor::createInst(session, scan->m_tableDef, scan->m_fullRow, (uint)-1, m_tab->getRecords()->getRowCompressMng());
	} else
		scan->m_subExtractor = SubrecExtractor::createInst(session, scan->m_tableDef, scan->m_fullRow);

	session->incOpStat(TNTTblScan::getTblStatTypeForScanType(type));

	return scan;
}

/**	分配一个扫描句柄
*	@session	session会话
*	@numCols	要读取的属性数，不能为0
*	@columns	要读取的属性列表，不能为NULL
*	@opType		操作类型
*	@return		扫描句柄，所有的内存从session的MemoryContext中分配
*
*/
TNTTblScan* TNTTable::allocScan(Session *session, u16 numCols, u16 *columns, OpType opType) {
	assert(numCols && columns);

	// 将所有成员初始化为0 or NULL
	void *p = session->getMemoryContext()->calloc(sizeof(TNTTblScan));
	TNTTblScan *scan = new(p)TNTTblScan();

	scan->m_session = session;
	scan->m_bof		= true;
	scan->m_opType	= opType;
	scan->m_lastRecPlace = LAST_REC_NO_WHERE;
	scan->m_rowPtr	= NULL;
	scan->m_tntTable= this;
	scan->m_tableDef= m_tab->getTableDef();
	scan->m_pkey	= scan->m_tableDef->m_pkey;
	scan->m_readCols.m_size = numCols;
	scan->m_readCols.m_cols	= (u16 *)session->getMemoryContext()->dup(columns, sizeof(u16) * numCols);
	scan->m_readLob	= scan->m_tableDef->hasLob(numCols, columns);
	
	return scan;
}

/**	定位到扫描的下一条记录。大对象占用的内存将被自动释放，
*	由于是多版本并发控制，
*	@post		快照读下不加锁
*	@post		当前读需要对满足条件的记录加事务锁，当事务提交或者回滚的时候释放
*
*	@scan		扫描句柄
*	@mysqlRow	OUT，存储为REC_MYSQL格式
*	@rid		可选参数，只在定位扫描时取指定的记录
*	@return		是否定位到一条记录
*/
bool TNTTable::getNext(TNTTblScan *scan, byte *mysqlRowUpperLayer, RowId rid, bool needConvertToUppMysqlFormat) throw(NtseException) {
	assert(scan->m_scanType == ST_POS_SCAN || rid == INVALID_ROW_ID);

	MemoryContext *ctx = scan->m_session->getMemoryContext();
	McSavepoint savepoint(ctx);

	// 转化上层MYSQL格式记录到引擎层MYSQL格式记录 
	byte *mysqlRow = mysqlRowUpperLayer;
	if (m_tab->getTableDef()->hasLongVar() && needConvertToUppMysqlFormat) {
		mysqlRow = (byte *) ctx->alloc(m_tab->getTableDef()->m_maxRecSize);
	}

	// 如果是第一次进入getNext函数，则根据操作类型，判断是加表意向锁，还是生成ReadView
	if (scan->m_opInfo != NULL &&
		scan->m_opInfo->m_sqlStatStart == true &&
		scan->m_opInfo->m_mysqlOper == true &&
		scan->m_scanType != ST_POS_SCAN) {

			scan->m_opInfo->m_sqlStatStart = false;

			if (scan->m_opInfo->m_selLockType != TL_NO) {
				// 当前读，对表加意向锁
				assert(scan->m_tabLockMode == TL_IS || scan->m_tabLockMode == TL_IX);

				scan->m_trx->lockTable(scan->m_tabLockMode, m_tab->getTableDef()->m_id);
			} else {
				// 快照读，创建ReadView
				if (!scan->m_trx->getReadView())
					scan->m_trx->trxAssignReadView();
			}
	}
	
	// 设置memory context的savepoint 1
	u64 sp1 = ctx->setSavepoint();

_ReStart1:
	releaseLastRow(scan, true);
	if (scan->checkEofOnGetNext())
		return false;
	
	// reset to savepoint 1
	ctx->resetToSavepoint(sp1);

	// 判断扫描类型
	bool exist;
	if (scan->m_scanType == ST_TBL_SCAN) {
		// （一）全表扫描，外存读记录；内存读记录最新版本，判断可见性等等
		Records::Scan *rs = (Records::Scan *)(scan->m_recInfo);

		m_tab->getRecords()->getHeap()->storePosAndInfo(rs->getHeapScan());
		BufferPageHandle *pageHdl = rs->getHeapScan()->getPage();
		u64 pageId = INVALID_PAGE_ID;
		if (pageHdl != NULL) {
			pageId = pageHdl->getPageId();
		}

		// 设置memory context的savepoint 2
		u64 sp2 = ctx->setSavepoint();

_ReStart2:
		// reset to savepoint 2
		ctx->resetToSavepoint(sp2);

		// Table Scan，getNext返回之后，取到的记录上一定持有 NTSE Shared Lock

		// NTSE Shared Lock的功能：
		// 1. 用于保护事务锁的获取
		// 2. 保留原有NTSE的并发控制协议
		exist = rs->getNext(mysqlRow);

		if (!exist) {
			stopUnderlyingScan(scan);
			scan->m_eof = true;
			return false;
		}
		// 获取RowId
		scan->m_rowId = rs->getRedRow()->m_rowId;
		TNTTransaction *trx = scan->getSession()->getTrans();
		TLockMode lockMode = scan->getRowLockMode();

		SYNCHERE(SP_ROW_LOCK_BEFORE_LOCK);
		if (lockMode != TL_NO && !trx->tryLockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id)) {
			SYNCHERE(SP_ROW_LOCK_AFTER_TRYLOCK);
			//释放占用资源和行锁
			//releaseLastRow(scan, true); 
			scan->m_session->unlockRow(&scan->m_rlh);
			try {
				trx->lockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id);
			} catch(NtseException &e) {
				throw e;
			}
		
			// 此处lockRow之后，不能立即unlockRow，否则会出现线程被饿死的情况
			// TODO：去掉unlockRow，可能会带来误锁的问题，此问题后续需要解决
			// trx->unlockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id);

			m_tab->getRecords()->getHeap()->restorePosAndInfo(rs->getHeapScan());
			pageHdl = rs->getHeapScan()->getPage();
			assert(pageHdl != NULL);
			if (pageHdl->getPageId() != pageId) {
				assert(pageHdl->isPinned());
				scan->getSession()->unpinPage(&pageHdl);
				rs->getHeapScan()->setPage(NULL);
			}
			releaseLastRow(scan, true);
			goto _ReStart2;
		}
		
		bool tntVisible = false, ntseVisible = false;
	
		tntVisible = m_mrecs->getRecord(scan, scan->m_version, &ntseVisible);
		if (tntVisible || ntseVisible) {
			// 1. 可见版本 or 最新版本在内存中，并且已经返回，需要填充Lob字段
			// 2. 可见版本 or 最新版本在NTSE中
			scan->m_lastRecPlace = tntVisible ? LAST_REC_IN_FULLROW : LAST_REC_IN_REDROW;
			fillMysqlRow(scan, mysqlRow);
		} else {
			// 无可见版本，此时需要NTSE取下一项

			// 在读取下一项前，需要释放本项上的事务锁
			if (lockMode != TL_NO) {
				trx->unlockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id);
			}
			scan->m_session->unlockRow(&scan->m_rlh);
			goto _ReStart1;
		}

		// TNT 的事务锁加锁成功 或者 TNT 快照读不需要加TNT事务锁最后释放NTSE Lock
		assert(scan->m_rlh != NULL);
		scan->m_session->unlockRow(&scan->m_rlh);
	} else if (scan->m_scanType == ST_IDX_SCAN) {
		// （二）索引扫描，读取索引记录；然后通过内存堆，判断可见性
		// 无论是否为coverage scan，index都取完整索引记录，存入m_idxKey->m_data中
		// m_mysqlRow记录为传出的记录
		// 如果是coverage scan, m_redRow可以直接返回上层，这是由前缀索引，大对象不能做覆盖扫描的特性决定的
		scan->m_mysqlRow->m_data = mysqlRow;
		if (scan->m_coverageIndex)
			scan->m_redRow->m_data = mysqlRow;

		// 索引range scan，unique scan，都统一调用range scan方法
		exist = scan->m_index->getNext(scan->m_indexScan);
		
		if (exist)
		{
			bool isSnapshotRead = scan->isSnapshotRead();
			const KeyMVInfo &mvInfo	= scan->m_indexScan->getMVInfo();
			bool ntseReturned	= mvInfo.m_ntseReturned;
			bool isDeleted		= mvInfo.m_delBit == 1 ? true : false;

			scan->m_rowId = scan->m_idxKey->m_rowId;

			bool isRecShouldRet = readRecByRowId(scan, scan->m_rowId, &ntseReturned, isDeleted, mvInfo.m_visable, mvInfo.m_version, isSnapshotRead); 
			// 当前记录不满足可见性判断，跳过，取索引下一条记录
			if (!isRecShouldRet) {
				// 在读取下一项前，需要释放本项上的事务锁
				TNTTransaction *trx = scan->getSession()->getTrans();
				TLockMode lockMode = scan->getRowLockMode();
				if (lockMode != TL_NO)
					trx->unlockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id);
				scan->m_indexScan->unlatchNtseRowBoth();
				goto _ReStart1;
			}

			fillMysqlRow(scan, mysqlRow);
			scan->m_indexScan->unlatchNtseRowBoth();
			// 若为唯一性扫描，停止底层索引scan，返回当前记录
			if (scan->m_singleFetch) 
				stopUnderlyingScan(scan);
		} else {
			scan->m_indexScan->unlatchNtseRowBoth();
		}
	} else {
		// （三）RowId扫描，读取内存堆，判断可见性；如有需要，访问外存堆，取记录
		//		 Rowid为调用传入的rid参数
		assert(scan->m_scanType == ST_POS_SCAN);
		bool tntVisible = false, ntseVisable = false;
		RowLockHandle *rlh = NULL;

		scan->m_rowId	= rid;
		scan->m_mysqlRow->m_data = mysqlRow;

		assert(!scan->m_session->isRowLocked(m_tab->getTableDef()->m_id, rid, Shared));
		// 读取记录时，必须保证NTSE锁已经加上
		rlh = scan->m_session->lockRow(m_tab->getTableDef()->m_id, rid, Shared, __FILE__, __LINE__);

		tntVisible = m_mrecs->getRecord(scan, scan->m_version, &ntseVisable);
		scan->m_lastRecPlace = LAST_REC_IN_FULLROW;
		if (tntVisible || ntseVisable) {
			exist = true;
			// Position scan，指定rowid进行扫描，可能存在找不到记录的情况(tnt,ntse均不存在此记录)
			if (!tntVisible && ntseVisable) {
				exist = readNtseRec(scan);
			}
			if (exist)
				fillMysqlRow(scan, mysqlRow);
		} else {
			// 无可见版本
			exist = false;
		}

		// 在读取记录之后，释放NTSE Row Lock
		scan->m_session->unlockRow(&rlh);
	}

	// 将mysqlrow格式转换成上层的格式
	if (exist && m_tab->getTableDef()->hasLongVar() && needConvertToUppMysqlFormat) {
		SubRecord upperMysqlSubRec(REC_UPPMYSQL, scan->m_readCols.m_size, scan->m_readCols.m_cols, 
			mysqlRowUpperLayer, m_tab->getTableDef()->m_maxMysqlRecSize);
		SubRecord engineMysqlSubRec(REC_MYSQL, scan->m_readCols.m_size, scan->m_readCols.m_cols, 
			mysqlRow, m_tab->getTableDef()->m_maxRecSize);
		RecordOper::convertSubRecordMEngineToUp(m_tab->getTableDef(), &engineMysqlSubRec, &upperMysqlSubRec);
	}

	scan->m_bof = false;
	if (!exist) {
		stopUnderlyingScan(scan);
		scan->m_eof = true;
		return false;
	}

	// NTSE在此处释放行锁，更新统计信息，但是TNT暂时不需要
	scan->m_session->incOpStat(OPS_ROW_READ);
	scan->m_session->incOpStat(scan->m_scanRowsType);
	return true;
}

/**	判断当前的索引扫描，是否为索引覆盖性扫描
*	@table	表定义
*	@index	索引定义
*	@cols	读取的cols
*	@return 覆盖性扫描，返回true；否则返回false
*/
bool TNTTable::isCoverageIndex(const TableDef *table, const IndexDef *index, const ColList &cols) {
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

/**	当前记录读取完毕，返回上层之前，将记录填充到mysqlRow，
*	@scan			TNT扫描句柄
*	@mysqlRow		上层指定的填充返回记录的空间
*/
void TNTTable::fillMysqlRow(TNTTblScan *scan, byte *mysqlRow) {
	UNREFERENCED_PARAMETER(mysqlRow);

	// 根据scan->m_scanType，判断如何将记录copy到mysql指定的空间之中
	if (scan->m_scanType == ST_TBL_SCAN) {
		if (scan->m_lastRecPlace == LAST_REC_IN_REDROW) {
			// 当前记录存储在scan->m_redRow之中
			// 1. 不读取大对象时，m_redRow->m_data = mysqlRow，
			// 2. 读取大对象时，m_redRow->m_data = Records::Scan内部分配空间
			if (scan->m_readLob) {
				memcpy(scan->m_mysqlRow->m_data, scan->m_redRow->m_data, scan->m_tableDef->m_maxRecSize);
				scan->readLobs();
			}
		}
		else {
			assert(scan->m_lastRecPlace == LAST_REC_IN_FULLROW);
			// 当前记录存储在scan->m_fullRow之中
			// 需要从m_fullRow中提取出mysqlRow
			// 测试发现，redundant格式下，Record与SubRecord拥有同样的定义，直接copy data即可
			memcpy(scan->m_mysqlRow->m_data, scan->m_fullRow->m_data, scan->m_tableDef->m_maxRecSize);
			if (scan->m_readLob)
				scan->readLobs();
		}
	}
	else if (scan->m_scanType == ST_IDX_SCAN) {
		// 2. Index scan，根据是否为coverage index scan，来判断如何copy
		if (scan->m_coverageIndex) {
			// 直接从scan->m_idxKey中提取记录
			// 由于默认覆盖扫描不会存在大对象，因此mysqlRow 和 REDUNDANT 格式记录相同
			RecordOper::extractSubRecordPRNoLobColumn(scan->m_tableDef, scan->m_indexDef, scan->m_idxKey, scan->m_redRow);
		}
		else {
			assert(scan->m_lastRecPlace == LAST_REC_IN_FULLROW);
			// 从scan->m_fullRec中copy记录
			memcpy(scan->m_mysqlRow->m_data, scan->m_fullRow->m_data, scan->m_tableDef->m_maxRecSize);
			if (scan->m_readLob)
				scan->readLobs();
		}
	}
	else {
		// 此处将position scan与table range scan分开，是为了以后position scan的优化考虑
		assert(scan->m_scanType == ST_POS_SCAN);
		assert(scan->m_lastRecPlace == LAST_REC_IN_FULLROW);
		memcpy(scan->m_mysqlRow->m_data, scan->m_fullRow->m_data, scan->m_tableDef->m_maxRecSize);
		if (scan->m_readLob)
			scan->readLobs();
	}
	
	// 设置扫描句柄，当前返回行的rowid
	scan->m_mysqlRow->m_rowId = scan->m_rowId;
	scan->m_fullRow->m_rowId = scan->m_rowId;
	scan->m_fullRec->m_rowId = scan->m_rowId;
}

/**	判断索引scan返回的项，是否可见(同时包括snapshot read，current read)
*
*	@scan			TNT扫描句柄
*	@rid			索引扫描返回的RowId
*	@ntseReturned	输入/输出.输入：当前项，是否从NTSE 索引扫描句柄中返回；输出：当前项，是否从NTSE Heap中读取
*	@isDeleted		当前返回项，是否设置Delete_Bit
*	@isVisible		内存索引页面项是否可见
*	@rowVersion		内存索引项对应的Row Version
*	@return			当前索引返回的项，是否可以返回给mysql层
*/
bool TNTTable::readRecByRowId(TNTTblScan *scan, RowId rid, bool* ntseReturned, bool isDeleted, bool isVisible, u16 rowVersion, bool isSnapshotRead) {
	UNREFERENCED_PARAMETER(rid);
	UNREFERENCED_PARAMETER(isSnapshotRead);
	UNREFERENCED_PARAMETER(isVisible);
	UNREFERENCED_PARAMETER(isDeleted);

	// 1. 若为索引覆盖扫描
	// 索引覆盖扫描，做分支优化
	if (scan->m_coverageIndex) {
		// 1.1 内存索引返回，同时通过页面DB_MAX_ID可以判断可见性，直接返回即可
		/*if (!*ntseReturned && isVisible) {
			if (isDeleted) 
				return false;
			else {
				scan->m_lastRecPlace = LAST_REC_IN_IDXROW;
				return true;
			}
		}*/

		// 1.2 进RowId hash，判断可见性
		bool tntVisible = false, ntseVisible = false;

		tntVisible = m_mrecs->getRecord(scan, rowVersion, &ntseVisible);
		scan->m_lastRecPlace = LAST_REC_IN_FULLROW;

		// 内外存均不可见，直接返回不可见
		if (!tntVisible && !ntseVisible) {
			// 必定为snapshot read，current read，一定能找到最新项
			// assert(isSnapshotRead);
			return false;
		}

		// 1.3 外存索引返回，同时经过RowId Hash判断，ntse可见(此时tnt必不可见)，直接返回即可
		if (*ntseReturned && ntseVisible) {
			scan->m_lastRecPlace = LAST_REC_IN_IDXROW;
			return true;
		}
		
		// 1.4 所有其他情况，都需要进行Double Check
		if (ntseVisible) {
			// 读取ntse表记录
			if (readNtseRec(scan)) {
				scan->m_lastRecPlace = LAST_REC_IN_FULLROW;
				*ntseReturned = true;
			} else {
				return false;
			}
		}

		if (doubleCheckRec(scan)) 
			return true;
		else 
			return false;
	} 
	// 2. 非索引覆盖扫描
	else {
		// 2.1 非索引覆盖扫描，TNT第一版中不做优化
		bool tntVisible = false, ntseVisible = false;

		tntVisible = m_mrecs->getRecord(scan, rowVersion, &ntseVisible);
		scan->m_lastRecPlace = LAST_REC_IN_FULLROW;

		if (!tntVisible && !ntseVisible) {
			return false;
		}
		if (!tntVisible && ntseVisible) {
			if (readNtseRec(scan)) {
				scan->m_lastRecPlace = LAST_REC_IN_FULLROW;
				*ntseReturned = true;
			} else {
				return false;
			}
		}

		if (doubleCheckRec(scan))
			return true;
		else
			return false;
	}
}

/**	给定rowid，读取此rowid对应的ntse记录全项
*	@scan	TNT扫描句柄
*	@return	返回指定的Rowid记录是否存在
*/
bool TNTTable::readNtseRec(TNTTblScan *scan) {
	bool exist;
	// 保证调用此方式已加上Ntse S 行锁
	assert(scan->m_session->isRowLocked(m_tab->getTableDef()->m_id, scan->m_rowId, Shared));
	Records::BulkFetch *fetch = (Records::BulkFetch *)(scan->m_recInfo);

	// 在NTSE Test测试中，Unique Scan找到一项之后，直接结束了BulkFetch，导致执行失败
	// 1. 更新操作，一定读取完整记录，因此通过MySQL调用，暂时不存在此情况
	// 2. 只读操作，读取完整的NTSE记录，做Double Check，后续视情况，可做部分改进，只读取Index列+Read Columns

	// 注1：若此函数由getNext调用，那么fetch一定存在，因此读取的记录，根据表是否开启MMS，可能会进入MMS中
	// 注2：若此函数由update/delete调用，那么fetch可能不存在，走else路径，此时记录不会进入MMS，但是会判断MMS中是否存在
	// 注3：Index/Position Scan，fetch读取的一定是全项；Table Scan，fetch读取的是readCols列

	if (fetch != NULL)
		exist = fetch->getFullRecbyRowId(scan->m_rowId, scan->m_fullRow, scan->m_subExtractor);
	else
		exist = m_tab->getRecords()->getSubRecord(scan->m_session, scan->m_rowId, scan->m_fullRow, scan->m_subExtractor);

	if (!exist) {
		// 指定rowid的scan，可能存在找不到记录的情况
		// assert(scan->m_scanType == ST_POS_SCAN);
		return exist;
	}

	scan->m_fullRow->m_rowId = scan->m_rowId;
	scan->m_fullRec->m_rowId = scan->m_rowId;
	assert(exist);
	scan->m_fullRowFetched = true;

	return exist;
}

/**第一阶段purge
 * @param session purge会话
 * @param trx purge事务
 */
void TNTTable::purgePhase1(Session *session, TNTTransaction *trx) {
	MemoryContext *ctx = session->getMemoryContext();
	TableDef *tblDef = m_tab->getTableDef();
	u16 *columns = (u16 *)ctx->alloc(sizeof(u16) * tblDef->m_numCols); //记录旧TableDef的列
	for (u16 i = 0; i < tblDef->m_numCols; ++i) {
		columns[i] = i;
	}

	u32 beginTime = System::fastTime();
	u64 traverseRecCnt = 0;
	u32 count = 0;
	byte *mysqlRow =  (byte *)ctx->alloc(tblDef->m_maxRecSize);
	//Record redRec(INVALID_ROW_ID, REC_REDUNDANT, redData, tblDef->m_maxRecSize);

	MHeapRec *heapRec = NULL;
	MHeapScanContext *scanCtx = m_mrecs->beginPurgePhase1(session);
	m_purgeStatus = PS_PURGEPHASE1;
	while (true) {
		McSavepoint msp(ctx);
		HeapRecStat stat = VALID;
		if (m_purgeStatus == PS_PURGEPHASE1 && !m_mrecs->purgeNext(scanCtx, trx->getReadView(), &stat)) {
			m_purgeStatus = PS_PURGECOMPENSATE;
			m_mrecs->resetMHeapStat();
			m_db->getTNTSyslog()->log(EL_LOG, "Next need purge compensate row size is %d", m_mrecs->getCompensateRowSize());
			continue;
		} else if (m_purgeStatus == PS_PURGECOMPENSATE && !m_mrecs->purgeCompensate(scanCtx, trx->getReadView(), &stat)) {
			break;
		}
		traverseRecCnt++;
		//如果TNT，NTSE均不可见，那回滚出来的记录为NULL，需要跳过
		if(stat == NTSE_VISIBLE || scanCtx->m_heapRec == NULL)
			continue;

		heapRec = scanCtx->m_heapRec;
		OpType opType = (heapRec->m_del == FLAG_MHEAPREC_DEL)? OP_DELETE: OP_UPDATE;
		TblScan *posScan = NULL;
		posScan = m_tab->positionScan(session, opType, tblDef->m_numCols, columns);
		bool exist = m_tab->getNext(posScan, mysqlRow, heapRec->m_rec.m_rowId);

		if (!exist) {
			m_tab->endScan(posScan);
			continue;
		}

		//将rec记录插入ntse中,由于大对象是直接插入ntse的大对象文件中，所以原有的把REC_MYSQL格式的记录插入ntse的接口已经不合适了(原有接口会导致再插一次大对象)
		//如果插入记录格式为定长或者变长，但索引的插入为冗余格式，所以觉得用冗余格式插入数据比较好
		if (heapRec->m_del == FLAG_MHEAPREC_DEL) {
			// 先删除记录,再删除大对象	
			m_tab->deleteCurrent(posScan);

			if(tblDef->m_hasLob){
				SubRecord *ntseSub = posScan->getRedRow();
				Record ntseRec(ntseSub->m_rowId, REC_REDUNDANT, ntseSub->m_data, ntseSub->m_size);
				u16 *updateCols = NULL;
				// 此处只需要比对大对象列即可，其他类型的列没有比较意义
				u16 updateColNum = RecordOper::extractDiffColumns(tblDef, &ntseRec, &(heapRec->m_rec), &updateCols, ctx, true);

				// 删除NTSE外存记录上被更新过的大对象
				for (u16 i = 0; i < updateColNum; i++) {
					ColumnDef *colDef = tblDef->m_columns[updateCols[i]];
					assert(colDef->isLob());
					bool ntseIsNull = RecordOper::isNullR(tblDef, &ntseRec, updateCols[i]);
					if(ntseIsNull)
						continue;
					LobId ntseLobId = RecordOper::readLobId(ntseRec.m_data,colDef);
					if(ntseLobId != INVALID_LOB_ID){
						if (m_db->getStat() == DB_RECOVERING){
							//恢复时要防止因lobId重用导致的误删除现象
							TblLob tblLob;
							tblLob.m_lobId = ntseLobId;
							tblLob.m_tableId = m_tab->getTableDef()->m_id;
							if (m_db->getPurgeInsertLobHash() != NULL && m_db->getPurgeInsertLobHash()->get(&tblLob) != NULL) 
								continue;
							m_tab->deleteLobAllowDupDel(session, ntseLobId);
						} else
							m_tab->deleteLob(session, ntseLobId);
					}
				}

				//删除TNTIM记录中的大对象
				for(u16 colNum = 0; colNum <tblDef->m_numCols; colNum++){
					ColumnDef *colDef = tblDef->m_columns[colNum];
					if(!colDef->isLob())
						continue;

					bool tntIsNull = RecordOper::isNullR(tblDef, &(heapRec->m_rec), colNum);
					if(tntIsNull)
						continue;
					LobId tntLobId = RecordOper::readLobId(heapRec->m_rec.m_data,colDef);
					if(tntLobId != INVALID_LOB_ID){
						if (m_db->getStat() == DB_RECOVERING){
							//恢复时要防止因lobId重用导致的误删除现象
							TblLob tblLob;
							tblLob.m_lobId = tntLobId;
							tblLob.m_tableId = m_tab->getTableDef()->m_id;
							if (m_db->getPurgeInsertLobHash() != NULL && m_db->getPurgeInsertLobHash()->get(&tblLob) != NULL) 
								continue;
							m_tab->deleteLobAllowDupDel(session, tntLobId);
						} else
							m_tab->deleteLob(session, tntLobId);		
					}
				}			
			}

		} else {
			SubRecord *ntseSub = posScan->getRedRow();
			Record ntseRec(ntseSub->m_rowId, REC_REDUNDANT, ntseSub->m_data, ntseSub->m_size);
			u16 *updateCols = NULL;
			u16 updateColNum = RecordOper::extractDiffColumns(tblDef, &ntseRec, &(heapRec->m_rec), &updateCols, ctx, false);
			if (updateColNum > 0) {
				posScan->setPurgeUpdateColumns(updateColNum, updateCols);
				//这里只更新记录，不更新大对象
				//tntTable的updateCurrent能将不符合规则的update给挡掉，所以此时可以认为purge update必定不throw exception
				try {
					// 这里更新的后项必定是引擎层的格式，而不是上层的格式
					NTSE_ASSERT(m_tab->updateCurrent(posScan, heapRec->m_rec.m_data, true));
				} catch (NtseException &e) {
					m_db->getTNTSyslog()->log(EL_ERROR, "purge updateCurrent error %s", e.getMessage());
					assert(false);
				}

				// 删除外存中被更新的大对象
				if (tblDef->hasLob()) {
					for (u16 i = 0; i < updateColNum; i++) {
						ColumnDef *colDef = tblDef->m_columns[updateCols[i]];
						if (!colDef->isLob()) 
							continue;

						// 需要删除外存项中的大对象
						bool tntIsNull = RecordOper::isNullR(tblDef, &ntseRec, updateCols[i]);
						if(tntIsNull)
							continue;
						LobId tntLobId = RecordOper::readLobId(ntseRec.m_data,colDef);
						if(tntLobId != INVALID_LOB_ID){
							if (m_db->getStat() == DB_RECOVERING){
								//恢复时要防止因lobId重用导致的误删除现象
								TblLob tblLob;
								tblLob.m_lobId = tntLobId;
								tblLob.m_tableId = tblDef->m_id;
								if (m_db->getPurgeInsertLobHash() != NULL && m_db->getPurgeInsertLobHash()->get(&tblLob) != NULL) 
									continue;
								m_tab->deleteLobAllowDupDel(session, tntLobId);
							} else
								m_tab->deleteLob(session, tntLobId);	
						}
					}
				}
			}
		}
		m_tab->endScan(posScan);
		count++;
	}
	m_db->getTNTSyslog()->log(EL_LOG, "purge table(%s) Phase1 and record is %d and traverse page cnt is %d and traverse record cnt is "I64FORMAT"u and time is %d s", 
		m_tab->getPath(), count, scanCtx->m_traversePageCnt, traverseRecCnt, System::fastTime() - beginTime);
	m_mrecs->endPurgePhase1(scanCtx);
}

/**第二阶段purge
 * @param session purge会话
 * @param trx purge事务
 */
u64 TNTTable::purgePhase2(Session *session, TNTTransaction *trx) {
	u64 ret = 0;
	u32 beginTime = System::fastTime();
	m_purgeStatus = PS_PURGEPHASE2;
	SYNCHERE(SP_TNT_TABLE_PURGE_PHASE3);
	ret = m_mrecs->purgePhase2(session, trx->getReadView());
	u32 endTime = System::fastTime();
	m_db->getTNTSyslog()->log(EL_LOG, "purge phase two mheap time is %d", endTime - beginTime);
	beginTime = endTime;
	uint indexNum = m_indice->getIndexNum();
	SYNCHERE(SP_TNT_TABLE_PURGE_PHASE2);
	for (u8 i = 0; i < indexNum; i++) {
		m_indice->getTntIndex(i)->purge(session, trx->getReadView());
	}
	endTime = System::fastTime();
	m_db->getTNTSyslog()->log(EL_LOG, "purge phase two mindex time is %d", endTime - beginTime);
	return ret;
}

/** 回收内存索引页面
 * @param session 内存索引回收会话
 */
void TNTTable::reclaimMemIndex(Session *session) {
	uint indexNum = m_indice->getIndexNum();
	for(u8 i = 0; i < indexNum; i++) {
		 m_indice->getTntIndex(i)->reclaimIndex(session, m_db->getTNTConfig()->m_reclaimMemIndexHwm, m_db->getTNTConfig()->m_reclaimMemIndexLwm);
	}
}

/**	给定rowid，对比此rowid的可见版本，与索引返回记录，是否完全一致
*	@scan	TNT扫描句柄
*	@return	记录完全匹配(包括rowid)，返回true；否则返回false
*
*/
bool TNTTable::doubleCheckRec(TNTTblScan *scan) {
	return scan->doubleCheckRecord();
}

/**	释放上一个返回行占用的资源
*	@scan		TNT 扫描句柄
*	@retainLobs	是否保留Lob空间
*
*/
void TNTTable::releaseLastRow(TNTTblScan *scan, bool retainLobs) {
	scan->releaseLastRow(retainLobs);
}

/**	更新当前扫描的记录
*	@pre	记录被用X锁锁定
*	@post	仍旧保持锁，事务提交/回滚时释放
*
*	@scan		扫描句柄
*	@update		要更新的属性极其值，使用REC_MYSQL格式
*	@oldRow		若不为NULL，则指定记录的前像，为REC_MYSQL格式
*	@dupIndex	OUT，发生唯一性索引冲突时返回导致冲突的索引号。若为NULL则不给出
*	@fastCheck	是否采用fast的方式check duplicate violation。默认为true.
*	@throw		NtseExcpetion	记录超长，属性不足等
*/
bool TNTTable::updateCurrent(TNTTblScan *scan, const byte *update, const byte *oldRow, uint *dupIndex, bool fastCheck) throw(NtseException) {
	UNREFERENCED_PARAMETER(oldRow);
	bool tntFirstRound  = false;
	u16	 updateIndexNum = 0;

	if (scan->m_tableDef->m_indexOnly && scan->m_readCols.m_size < scan->m_tableDef->m_numCols)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "All columns must been read before update index only tables.");

	Session *session = scan->m_session;

	McSavepoint savepoint(scan->m_session->getMemoryContext());

	// 1. 判断当前最新纪录的位置：TNTIM or NTSE
	// 若当前最新记录在TNT中，则prepareUD函数取出记录全项
	tntFirstRound = m_mrecs->prepareUD(scan);
	if (tntFirstRound) {
		// 最新纪录在NTSE中，读取记录全项
		if (!scan->m_fullRowFetched) {
			scan->m_rlh = LOCK_ROW(session, m_tab->getTableDef()->m_id, scan->m_rowId, Shared);
			readNtseRec(scan);
			session->unlockRow(&scan->m_rlh);
		}
	} 
	else {
		// 若在TNTIM中，prepareUD调用已经完成全项的读取工作
	}

	try {
		// 如果含有超长变长字段，需要对mysqlRow格式记做转换
		// update操作之前的scan读取的是全项，并且上层为前后项做过合并，传到引擎层的后项是全项
		if (m_tab->getTableDef()->hasLongVar()) {
			Record mysqlUpdateRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)update, m_tab->getTableDef()->m_maxMysqlRecSize);
			byte *realUpdate = (byte *)session->getMemoryContext()->alloc(m_tab->getTableDef()->m_maxRecSize);
			Record realUpdateMysqlRec(INVALID_ROW_ID, REC_MYSQL, realUpdate, m_tab->getTableDef()->m_maxRecSize);
			RecordOper::convertRecordMUpToEngine(m_tab->getTableDef(), &mysqlUpdateRec, &realUpdateMysqlRec);	
			mysqlUpdateRec.m_data = realUpdateMysqlRec.m_data;
			mysqlUpdateRec.m_size = realUpdateMysqlRec.m_size;
			update = realUpdate;
		}

		// 2. 判断是否存在唯一性冲突
		scan->m_updateMysql->m_rowId = scan->m_updateRed->m_rowId = scan->m_rowId;
		scan->m_updateMysql->m_data = (byte *)update;
		
		//由于在下一步checkDuplicate时会修改传入的subRecord，因此针对有大对象的表，就需要先复制一份后项
		if(!scan->m_tableDef->hasLob())
			scan->m_updateRed->m_data = (byte *)update;
		else 
			memcpy(scan->m_updateRed->m_data, update, scan->m_tableDef->m_maxRecSize);
		
		if (checkDuplicate(session, scan->m_tableDef, scan->m_rowId, scan->m_fullRow, scan->m_updateRed, dupIndex, &updateIndexNum, fastCheck)) {
			// 唯一性冲突，报错返回
			session->unlockAllUniqueKey();
			releaseLastRow(scan, true);
			return false;
		}

		// 唯一性不冲突，此时仍旧持有唯一键值锁，可以更新，保证不会冲突
		if (scan->m_lobUpdated)	{
			// 更新大对象，需要首先完成大对象的更新	
			for(u16 colNum = 0; colNum < scan->m_updateRed->m_numCols; colNum++){
				ColumnDef *columnDef =m_tab->getTableDef()->m_columns[scan->m_updateRed->m_columns[colNum]];
				if(!columnDef->isLob())
					continue;
				bool oldIsNull = RecordOper::isNullR(m_tab->getTableDef(),scan->m_fullRec,scan->m_updateRed->m_columns[colNum]);
				bool newIsNull = RecordOper::isNullR(m_tab->getTableDef(),scan->m_updateMysql,scan->m_updateRed->m_columns[colNum]);
				//如果前后版本都为NULL则不做任何更改
				if(oldIsNull && newIsNull)
					continue;

				//在NTSE中新插入一个大对象
				byte * newLob = NULL;
				if (!newIsNull){
					newLob = RecordOper::readLob(scan->m_updateMysql->m_data,columnDef);
					if(!newLob)	//对非NULL但是大小为0的大对象会用NULL指针
						newLob = (byte*)session->getMemoryContext()->alloc(1);
				}
				//开始新的大对象分配
				LobId newLobId = INVALID_LOB_ID;
				//如果不是更新为NULL,那么新插入一个大对象
				if(!newIsNull){
					uint newLobSize = RecordOper::readLobSize(scan->m_updateMysql->m_data,columnDef);
					session->startTransaction(TXN_LOB_INSERT,m_tab->getTableDef()->m_id);
					newLobId = m_tab->getLobStorage()->insert(session, newLob, newLobSize, m_tab->getTableDef()->m_compressLobs);
					session->endTransaction(true);
				}

				//如果将LOB字段更新为NULL则分配InvalidLobId给更新后的版本

				//拼装LOB数据
				RecordOper::writeLobId(scan->m_updateRed->m_data, columnDef, newLobId);
				RecordOper::writeLobSize(scan->m_updateRed->m_data, columnDef, 0);
			}
		}
		if (!m_mrecs->update(scan, scan->m_updateRed))
			NTSE_THROW(NTSE_EC_OUT_OF_MEM, "Memory heap can't get page");
	} catch (NtseException &e) {
		session->unlockAllUniqueKey();
		throw e;
	}
	
	// 需要更新索引
	RowIdVersion version = scan->getVersion();
	m_indice->updateIndexEntries(scan->m_session, scan->m_fullRow, scan->m_updateRed, tntFirstRound, version);
	

	releaseLastRow(scan, true);
	session->incOpStat(OPS_ROW_UPDATE);
	session->unlockAllUniqueKey();

	return true;
}

/**	删除当前扫描的记录
*	@pre	记录被X锁锁定
*	@post	不放锁，等待事务提交or回滚
*
*	@scan		扫描句柄
*/
void TNTTable::deleteCurrent(TNTTblScan *scan) throw(NtseException) {
	assert(!scan->m_eof);
	assert(scan->m_opType == OP_DELETE || scan->m_opType == OP_WRITE);

	bool tntFirstRound = false;

	// PROFILE(PI_Table_deleteCurrent);

	TableDef *tableDef = m_tab->getTableDef();

	if (tableDef->m_indexOnly && scan->m_readCols.m_size < tableDef->m_numCols)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "All columns must been read before update index only tables.");

	McSavepoint savepoint(scan->m_session->getMemoryContext());

	tntFirstRound = m_mrecs->prepareUD(scan);

	if (tntFirstRound) {
		// 第一次delete，读取全项，进入Tnt
		if (!scan->m_fullRowFetched) {
			scan->m_rlh = LOCK_ROW(scan->m_session, tableDef->m_id, scan->m_rowId, Shared);
			readNtseRec(scan);
			scan->m_session->unlockRow(&scan->m_rlh);
		}
	}
	else {
		// Todo：写日志需要有page latch保护
		// 因此将写日志功能下降到MRecords模块完成
	}

	if (!m_mrecs->remove(scan)) {
		releaseLastRow(scan, true);
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "Memory heap can't get page");
	}

	if (scan->m_tableDef->m_numIndice > 0) {
		Record rec(scan->m_rowId, REC_REDUNDANT, scan->m_fullRow->m_data, scan->m_tableDef->m_maxRecSize);
		RowIdVersion version = scan->getVersion();
		m_indice->deleteIndexEntries(scan->m_session, &rec, version);
	}

	releaseLastRow(scan, true);
	scan->m_session->incOpStat(OPS_ROW_DELETE);
	if (m_estimateRows > 0)
		m_estimateRows--;
}

/**	停止底层的扫描
*	@scan	TNT扫描句柄
*
*/
void TNTTable::stopUnderlyingScan(TNTTblScan *scan) {
	if (scan->m_recInfo) {
		scan->m_recInfo->end();
		scan->m_recInfo = NULL;
	}

	if (scan->m_indexScan) {
		scan->m_index->endScan(scan->m_indexScan);
		scan->m_indexScan = NULL;
	}
}

/**	结束一次扫描。结束扫描之前，当前行占用的资源被释放
*	@post	如果加了行锁，则需要等待事务提交or回滚，然后释放
*
*	@scan	扫描句柄，将被释放
*/
void TNTTable::endScan(TNTTblScan *scan) {
	releaseLastRow(scan, false);
	stopUnderlyingScan(scan);
}


/**	插入一条记录
*	
*	@session	session对象
*	@record		要插入的记录，使用REC_MYSQL格式
*	@dupIndex	输出参数，导致冲突的索引序号
*	@opInfo		操作信息
*	@fastCheck	是否采用fast的方式check duplicate violation。默认为true.
*	@throw		NtseException
*	@return		成功返回记录RID
*/
RowId TNTTable::insert(Session *session, const byte *record, uint *dupIndex, TNTOpInfo *opInfo, bool fastCheck) throw(NtseException) {
	RowLockHandle *rlh = NULL;
	// 根据传入的opInfo，判断当前是否为语句第一次执行，并加表意向锁
	if (opInfo->m_sqlStatStart == true &&
		opInfo->m_selLockType != TL_NO) {
			opInfo->m_sqlStatStart = false;

			assert(opInfo->m_selLockType == TL_X);
			
			lockTableOnStatStart(session->getTrans(), TL_IX);
	}

	RowId		rid = INVALID_ROW_ID;
	u16			updateIndexNum = 0;
	TableDef*	tableDef = m_tab->getTableDef();

	McSavepoint savepoint(session->getMemoryContext());

	Record mysqlRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte*)record, m_tab->getTableDef()->m_maxMysqlRecSize);

	// 如果含有超长变长字段，需要对mysqlRow格式记做转换
	if (m_tab->getTableDef()->hasLongVar()) {
		byte *data = (byte *)session->getMemoryContext()->alloc(m_tab->getTableDef()->m_maxRecSize);
		Record realMysqlRec(INVALID_ROW_ID, REC_MYSQL, data, m_tab->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_tab->getTableDef(), &mysqlRec, &realMysqlRec);	
		mysqlRec.m_data = realMysqlRec.m_data;
		mysqlRec.m_size = realMysqlRec.m_size;
	} else {
		mysqlRec.m_format = REC_MYSQL;
	}


	//SubRecord(RecFormat format, u16 numCols, u16 *columns, byte *data, uint size, RowId rowId = INVALID_ROW_ID) 
	ColList fullColList = ColList::generateAscColList(session->getMemoryContext(), 0, tableDef->m_numCols);
	SubRecord subRec(REC_REDUNDANT, fullColList.m_size, fullColList.m_cols, (byte *)mysqlRec.m_data, tableDef->m_maxRecSize);

	try {
		// 0. check unique violation
		if (checkDuplicate(session, tableDef, INVALID_ROW_ID, NULL, &subRec, dupIndex, &updateIndexNum, fastCheck)) {
			session->unlockAllUniqueKey();
			return rid;
		}

		rid = m_tab->insert(session, mysqlRec.m_data, dupIndex, &rlh);
	} catch (NtseException &e) {
		if (rlh != NULL) {
			session->unlockRow(&rlh);
		}
		session->unlockAllUniqueKey();
		throw e;
	}
	
	// insert succ
	session->unlockAllUniqueKey();

	bool tntInsSucc = m_mrecs->insert(session, session->getTrans()->getTrxId(), rid);
	assert(tntInsSucc);
	assert(rlh != NULL);
	session->unlockRow(&rlh);

	return rid;
}

/**	Update，Insert操作，检查unique冲突(快速方式，不回表判断)
*	@session	Session
*	@m_tableDef	表定义
*	@rowId		前项记录的RowId
*	@before		update操作的前镜像
*	@after		update操作需要更新的列值
*	@dupIndex	OUT：若冲突，返回冲突的索引号	
*	@updateIndexNum	OUT：需要更新的索引数
*	@fastCheck	是否采用fast的方式check duplicate violation。默认为true.
*	@return		唯一性冲突，返回true；不冲突返回false
*/
bool TNTTable::checkDuplicate(Session *session, TableDef *m_tableDef, RowId rowId, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum, bool fastCheck) throw(NtseException) {
	UNREFERENCED_PARAMETER(fastCheck);
/*
	if (fastCheck) {
		return checkDuplicateFast(session, m_tableDef, before, after, dupIndex, updateIndexNum);
	} else {
		return checkDuplicateSlow(session, m_tableDef, before, after, dupIndex, updateIndexNum);
	}
*/
	// always do slow check
	return checkDuplicateSlow(session, m_tableDef, rowId, before, after, dupIndex, updateIndexNum);
}

/**	Update，Insert操作，检查unique冲突(快速方式，不回表判断)
*	@session	Session
*	@m_tableDef	表定义
*	@before		update操作的前镜像
*	@after		update操作需要更新的列值
*	@dupIndex	OUT：若冲突，返回冲突的索引号	
*	@updateIndexNum	OUT：需要更新的索引数
*	@return		唯一性冲突，返回true；不冲突返回false
*/
/*
bool TNTTable::checkDuplicateFast(Session *session, TableDef *m_tableDef, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw(NtseException) {
	u16			*updateIndexNoArray;
	u16			updateUniques	= 0, uniqueNo = 0;
	DrsIndice	*drsIndice		= m_indice->getDrsIndice();

	drsIndice->getUpdateIndices(session->getMemoryContext(), after, updateIndexNum, &updateIndexNoArray, 
		&updateUniques);

	// 构造after的record格式
	if (before != NULL) {
		RecordOper::mergeSubRecordRR(m_tableDef, after, before);
		after->m_rowId = before->m_rowId;
	}

	// 不更新unique字段，直接返回即可，不需要余下操作
	if (updateUniques == 0) 
		return false;

	// 从updateIndex数组中构造出unique index数组
	u16 *updateUniqueNos = (u16 *) session->getMemoryContext()->alloc(sizeof(u16) * updateUniques);
	for (uint i = 0; i < *updateIndexNum; i++){
		if (m_tableDef->m_indice[updateIndexNoArray[i]]->m_unique)
			updateUniqueNos[uniqueNo++] = updateIndexNoArray[i];
	}
	assert(uniqueNo == updateUniques);

	Record recAfter(after->m_rowId, REC_REDUNDANT, after->m_data, after->m_size);

	if (!m_indice->lockUpdateUniqueKey(session, &recAfter, updateUniques, updateUniqueNos, dupIndex)) {
		// 加唯一性锁失败，返回冲突
		return true;
	}

	//检查唯一性索引冲突
	if (m_indice->checkDuplicate(session, &recAfter, updateUniques, updateUniqueNos, dupIndex, NULL)) {
		session->unlockAllUniqueKey();
		return true;
	}

	// 唯一性索引检查通过，此时不放锁，真正插入成功之后，才能放锁
	return false;
}
*/

/**	Update，Insert操作，检查unique冲突(慢速方式，当前读，回表判断，针对于insert on duplicate，replace into语法)
*	@session	Session
*	@m_tableDef	表定义
*	@rowId		前项记录的RowId
*	@before		update操作的前镜像
*	@after		update操作需要更新的列值
*	@dupIndex	OUT：若冲突，返回冲突的索引号	
*	@updateIndexNum	OUT：需要更新的索引数
*	@return		唯一性冲突，返回true；不冲突返回false
*/
bool TNTTable::checkDuplicateSlow(Session *session, TableDef *m_tableDef, RowId rowId, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw(NtseException) {
	u16				*updateIndexNoArray;
	u16				updateUniques	= 0, uniqueNo = 0;
	MemoryContext	*mtx			= session->getMemoryContext();
	DrsIndice		*drsIndice		= m_indice->getDrsIndice();
	byte			*mysqlRow		= (byte *)mtx->alloc(m_tableDef->m_maxRecSize);

	drsIndice->getUpdateIndices(session->getMemoryContext(), after, updateIndexNum, &updateIndexNoArray, 
		&updateUniques);

	// 构造after的record格式
	if (before != NULL) {
		RecordOper::mergeSubRecordRR(m_tableDef, after, before);
		after->m_rowId = before->m_rowId;

		// 检查更新后的长度
		size_t realRecSize = 0;
		if (m_tableDef->m_recFormat == REC_FIXLEN) {
			realRecSize = m_tableDef->m_maxRecSize;
		} else {
			Record fullRec(after->m_rowId, REC_REDUNDANT, after->m_data, m_tableDef->m_maxRecSize);
			realRecSize = RecordOper::getRecordSizeRV(m_tableDef, &fullRec);
		}
		if (realRecSize > Limits::MAX_REC_SIZE) {
			NTSE_THROW(NTSE_EC_ROW_TOO_LONG, "Record too long, %d, max is %d.", realRecSize, Limits::MAX_REC_SIZE);
		}
	}

	// 不更新unique字段，直接返回即可，不需要余下操作
	if (updateUniques == 0) 
		return false;

	// 从updateIndex数组中构造出unique index数组
	u16 *updateUniqueNos = (u16 *) mtx->alloc(sizeof(u16) * updateUniques);
	for (uint i = 0; i < *updateIndexNum; i++){
		if (m_tableDef->m_indice[updateIndexNoArray[i]]->m_unique)
			updateUniqueNos[uniqueNo++] = updateIndexNoArray[i];
	}
	assert(uniqueNo == updateUniques);

	// 将after对应的rowid设置为INVALID_ROW_ID，保证底层索引是第一次scan
	RowId afterRid = INVALID_ROW_ID;

	Record recAfter(afterRid, REC_REDUNDANT, after->m_data, after->m_size);

	if (!m_indice->lockUpdateUniqueKey(session, &recAfter, updateUniques, updateUniqueNos, dupIndex)) {
	
		// 加唯一性锁失败，返回冲突
		return true;
	}

	// 遍历表上的所有唯一索引，并检查
	//检查内存索引是否存在重复键值
	for (uint i = 0; i < updateUniques; i++) {
		u16 idxNo		= updateIndexNoArray[i];
		IndexDef *index	= m_tableDef->m_indice[idxNo];
		SubRecord *key	= (SubRecord *)mtx->alloc(sizeof(SubRecord));
		byte *keyDat	= (byte *)mtx->alloc(index->m_maxKeySize);

		new (key)SubRecord(KEY_PAD, index->m_numCols, index->m_columns, keyDat, index->m_maxKeySize);

		// 提取大对象内容，拼装到索引键
		Array<LobPair*> lobArray;
		if (index->hasLob()) {
			RecordOper::extractLobFromM(session, m_tableDef, index, &recAfter, &lobArray);
		}

		bool isNullIncluded = RecordOper::extractKeyRPWithRet(m_tableDef,index, &recAfter, &lobArray, key);

		// 如果解析出的索引键值中包含NULL列，那么不需要进行unique约束检查
		if (isNullIncluded)
			continue;

		// 只获取查询需要的属性，可以做Index Coverage Scan
		IndexScanCond cond((u16 )idxNo, key, true, true, index == m_tableDef->m_pkey);
		TNTOpInfo opInfo;
		opInfo.m_selLockType = TL_X;
		opInfo.m_sqlStatStart= false;
		opInfo.m_mysqlOper	 = false;

		// 将索引的键值顺序转化为表中的键值顺序
		// 对于类似于(key2, key1)这类index，其键值顺序与建表顺序相反，必须转换
		// 否则底层的提取器会报错
		u16 *cols = (u16 *)mtx->alloc(sizeof(u16) * index->m_numCols);
		transIndexCols(m_tableDef->m_numCols, index, cols);

		TNTTblScan *scan = indexScan(session, OP_WRITE, &opInfo, &cond, index->m_numCols, cols, true);
		try {
			if (getNext(scan, mysqlRow)) {
				// 当前读，找到唯一项（此时冲突项上已经加锁）
				if(scan->m_rowId == rowId) {
					// 如果找到的唯一项RowId和（更新）前项的RowId相同
					// 则不认为是唯一性冲突，见JIRA：NTSETNT-308
					endScan(scan);
					return false;
				} else {
					session->unlockAllUniqueKey();
					endScan(scan);
					*dupIndex = idxNo;
					return true;
				}
			} else {
				// 未找到冲突项，关闭scan句柄
				endScan(scan);
			}
		} catch (NtseException &e) {
			endScan(scan);
			throw e;
		}
	}
	
	return false;
}

/** 创建内存堆索引至指定内存索引对象
 * @param session 会话
 * @param indice 需要创建的指定内存索引对象
 */
void TNTTable::buildMemIndexs(Session *session, MIndice *indice) {
	//开始建内存索引
	TableDef *tableDef = m_tab->getTableDef();
	uint idxNum = indice->getIndexNum();

	//遍历内存堆读入数据
	MHeapRec *heapRec = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp1(ctx);
	MHeapScanContext *scanCtx = m_mrecs->beginScan(session);
	while(true) {
		//此处需要获取堆内最新记录
		if(!m_mrecs->getNext(scanCtx, NULL)) 
			break;
		McSavepoint msp2(ctx);
		heapRec = scanCtx->m_heapRec;
		void *buf = ctx->alloc(sizeof(SubRecord));
		SubRecord *SubRec = new (buf)SubRecord(REC_REDUNDANT, 0, NULL, heapRec->m_rec.m_data, tableDef->m_maxRecSize, heapRec->m_rec.m_rowId);
		RowIdVersion version = m_mrecs->getHashIndex()->get(heapRec->m_rec.m_rowId, ctx)->m_version;
		//遍历各个索引
		for(u8 indexNo = 0; indexNo < idxNum; indexNo++) {
			McSavepoint lobSavepoint(session->getLobContext());
			//创建内存索引结构
			MIndex *memIndex = indice->getIndex(indexNo);
			IndexDef *indexDef = tableDef->getIndexDef(indexNo);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, tableDef, indexDef, m_tab->getLobStorage(), SubRec, &lobArray);
			}
			// 将记录转化为PAD格式键，并负责填充大对象内容
			SubRecord *key = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), SubRec, &lobArray, tableDef, indexDef);
			memIndex->insert(session, key, version);
		}				
	}		
}

/** 将Index的列定义数组，转换为按照建表的顺序排列
* 
* @maxCols	表中列的数量
* @index	IndexDef
* @cols		转换后的列定义数组
*
*/
void TNTTable::transIndexCols(u16 maxCols, IndexDef *index, u16 *cols) {
	u16 curr = 0;

	for (u16 i = 0; i < maxCols; i++) {
		for (u16 j = 0; j < index->m_numCols; j++) {
			if (index->m_columns[j] == i) {
				cols[curr] = i;
				curr++;

				break;
			}
		}
	}

	assert(curr == index->m_numCols);
}

/** 执行REPLACE和INSERT ... ON DUPLICATE KEY UPDATE语句时先试着插入记录
* 注: 本函数可能从session的内存分配上下文中分配空间，因此若一个session中
* 调用本函数多次可能导致内存无限增长
*
* @session	会话对象
* @record	要插入的记录，一定是MySQL格式
* @opInfo	操作信息
* @throw NtseException 加表锁超时，记录超长
* @return 成功返回NULL，失败返回IDU操作序列
*/
IUSequence<TNTTblScan *>* TNTTable::insertForDupUpdate(Session *session, const byte *record, TNTOpInfo *opInfo) throw(NtseException) {
	// 根据传入的opInfo，判断当前是否为语句第一次执行，并加表意向锁
	if (opInfo->m_sqlStatStart == true &&
		opInfo->m_selLockType != TL_NO) {
			opInfo->m_sqlStatStart = false;
			
			assert(opInfo->m_selLockType == TL_X);
			lockTableOnStatStart(session->getTrans(), TL_IX);
	}

	while (true) {
		uint		dupIndex;
		TableDef	*m_tableDef = m_tab->getTableDef();
		if (insert(session, record, &dupIndex, opInfo, false) != INVALID_ROW_ID)
			return NULL;
		assert(m_tableDef->m_indice[dupIndex]->m_unique);

		u64 mcSavepoint = session->getMemoryContext()->setSavepoint();

		IUSequence<TNTTblScan *> *iuSeq = (IUSequence<TNTTblScan *> *)session->getMemoryContext()->alloc(sizeof(IUSequence<TNTTblScan *>));
		iuSeq->m_mysqlRow = (byte *)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
		iuSeq->m_tableDef = m_tableDef;
		iuSeq->m_dupIndex = dupIndex;

		IndexDef *index = m_tableDef->m_indice[dupIndex];
		SubRecord *key = (SubRecord *)session->getMemoryContext()->alloc(sizeof(SubRecord));
		byte *keyDat = (byte *)session->getMemoryContext()->alloc(index->m_maxKeySize);
		new (key)SubRecord(KEY_PAD, index->m_numCols, index->m_columns, keyDat, index->m_maxKeySize);

		Record rec(INVALID_ROW_ID, REC_REDUNDANT, (byte *)record, m_tableDef->m_maxRecSize);

		// 提取索引键
		Array<LobPair*> lobArray;
		if (index->hasLob()) {
			RecordOper::extractLobFromM(session, m_tableDef, index, &rec, &lobArray);
		}
		RecordOper::extractKeyRP(m_tableDef, index, &rec, &lobArray, key);

		// TODO 只获取查询需要的属性
		ColList readCols = ColList::generateAscColList(session->getMemoryContext(), 0, m_tableDef->m_numCols);
		IndexScanCond cond((u16 )dupIndex, key, true, true, index == m_tableDef->m_pkey);
		TNTOpInfo opInfo;
		opInfo.m_selLockType = TL_X;
		opInfo.m_mysqlOper	 = false;
		opInfo.m_sqlStatStart= false;

		TNTTblScan *scan = indexScan(session, OP_WRITE, &opInfo, &cond, readCols.m_size, readCols.m_cols, true);
		if (getNext(scan, iuSeq->m_mysqlRow)) {
			iuSeq->m_scan = scan;
			return iuSeq;
		}
		// 由于INSERT的时候没有加锁，可能又找不到记录了
		// 这时再重试插入
		endScan(scan);
		session->getMemoryContext()->resetToSavepoint(mcSavepoint);
	}
}

/** 执行REPLACE和INSERT ... ON DUPLICATE KEY UPDATE语句时，发生冲突后更新记录
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE操作序列
 * @param update 发生冲突的记录将要被更新成的值，为REC_MYSQL格式
 * @param numUpdateCols 要更新的属性个数
 * @param updateCols 要更新的属性
 * @param dupIndex OUT，唯一性冲突时返回导致冲突的索引号，若为NULL则不返回索引号
 * @throw NtseException 记录超长
 * @return 更新成功返回true，由于唯一性索引冲突失败返回false
 */
bool TNTTable::updateDuplicate(IUSequence<TNTTblScan *> *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex) throw(NtseException) {
	bool succ;

	iuSeq->m_scan->setUpdateSubRecord(numUpdateCols, updateCols);

	try {
		succ = updateCurrent(iuSeq->m_scan, update, NULL, dupIndex, false);
	} catch (NtseException &e) {
		endScan(iuSeq->m_scan);
		iuSeq->m_scan = NULL;
		throw e;
	}
	endScan(iuSeq->m_scan);
	iuSeq->m_scan = NULL;
	return succ;
}

/** 执行REPLACE和INSERT ... ON DUPLICATE KEY UPDATE语句时，发生冲突后删除原记录，
 * 在指定了自动生成ID时会调用这一函数而不是updateDuplicate
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE操作序列
 */
void TNTTable::deleteDuplicate(IUSequence<TNTTblScan *> *iuSeq) throw(NtseException){
	assert(iuSeq->m_scan);
	try {
		deleteCurrent(iuSeq->m_scan);
	} catch (NtseException &e) {
		endScan(iuSeq->m_scan);
		iuSeq->m_scan = NULL;
		throw e;
	}
	endScan(iuSeq->m_scan);
	iuSeq->m_scan = NULL;
}

/** 直接释放INSERT ... ON DUPLICATE KEY UPDATE操作序列
 * @param iduSeq INSERT ... ON DUPLICATE KEY UPDATE操作序列
 */
void TNTTable::freeIUSequenceDirect(IUSequence<TNTTblScan *> *iuSeq) {
	
	endScan(iuSeq->m_scan);
	iuSeq->m_scan = NULL;
}

/**
* 在语句开始时，对表加锁
* @param	trx			加锁事务
* @param	lockType	锁模式	
*/
void TNTTable::lockTableOnStatStart(TNTTransaction *trx, TLockMode lockType) throw(NtseException) {
	TableId tid = m_tab->getTableDef()->m_id;

	trx->lockTable(lockType, tid);
}

/**	获取当前自增键值的取值
*	
*	@return	当前自增键值
*/
u64 TNTTable::getAutoinc() {
	assert(m_autoincMutex.isLocked());
	return m_autoinc;
}

/**	根据传入的值，修改自增键值
*	如果传入value > 当前自增键值，则更新
*	
*	@value	传入的新自增键值取值
*/
void TNTTable::updateAutoincIfGreater(u64 value) {
	assert(m_autoincMutex.isLocked());
	if (value > m_autoinc)
		m_autoinc = value;
}

/**	初始化自增键值
*
*	@value	初始化自增键值的取值
*/
void TNTTable::initAutoinc(u64 value) {
	assert(m_autoincMutex.isLocked());
	m_autoinc = value;
}

// 并发控制；mutex是优化路径；lock是正常路径；加lock前必须获得mutex
// lock存在时，enter mutex操作也不能成功，需要等待lock释放
// 通过m_reqAutoincLocks判断当前是否存在grant or req方式的lock

/**	
*
*
*/
void TNTTable::enterAutoincMutex() {
	m_autoincMutex.lock(__FILE__, __LINE__);
}

void TNTTable::exitAutoincMutex() {
	assert(m_autoincMutex.isLocked());
	
	m_autoincMutex.unlock();
}

ColList TNTTable::getUpdateIndexAttrs(const ColList &updCols, MemoryContext *ctx) {
	TableDef *tableDef = m_tab->getTableDef();
	ColList updatedIndexAttrs(0, NULL);
	for (u16 i = 0; i < tableDef->m_numIndice; i++) {
		const IndexDef *indexDef = tableDef->m_indice[i];
		ColList indexCols = ColList(indexDef->m_numCols, indexDef->m_columns).sort(ctx);
		if (indexCols.hasIntersect(updCols)) {
			updatedIndexAttrs = updatedIndexAttrs.merge(ctx, indexCols);
		}
	}

	return updatedIndexAttrs;
}

/** 根据rowId获取ntse记录
 * @param session 会话
 * @param rid 记录rowId
 * return ntse记录
 */
Record *TNTTable::readNtseRec(Session *session, RowId rid) {
	MemoryContext *ctx = session->getMemoryContext();
	TableDef *tableDef = m_tab->getTableDef();
	Record *rec = new(ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc(tableDef->m_maxRecSize), tableDef->m_maxRecSize);
	McSavepoint msp(ctx);
	u16 *columns = (u16 *)ctx->alloc(tableDef->m_numCols*sizeof(u16));
	for (u16 i = 0; i < tableDef->m_numCols; i++) {
		columns[i] = i;
	}
	TblScan *scan = m_tab->positionScan(session, OP_READ, tableDef->m_numCols, columns, true);
	//因为session中有事务，所以getNext后大对象字段为rowid
	if (m_tab->getNext(scan, rec->m_data, rid, false)) {
		rec->m_rowId = rid;
		//这里getnext是全量读
		if (tableDef->hasLob()) {
			memcpy(rec->m_data, scan->getRedRow()->m_data, tableDef->m_maxRecSize);
		}
	}
	m_tab->endScan(scan);
	return rec;
}

/**根据log redo 插入操作
 * @param session 会话
 * @param log 日志记录
 */
void TNTTable::redoInsert(Session *session, const LogEntry *log, RedoType redoType) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid;
	DrsHeap::parseInsertTNTLog(log, &trxId, &preLsn, &rid);
	if (redoType == RT_PREPARE)
		session->getTrans()->lockRow(TL_X, rid, m_tab->getTableDef()->m_id);
	m_mrecs->redoInsert(session, rid, trxId, redoType);
}

/** update的redo操作
 * @param session 会话
 * @param txnId 事务id
 * @param rid 记录id
 * @param rollbackId 回滚记录的rowid
 * @param tableIndex 版本池表序号
 * @param updateSub 更新列
 */
void TNTTable::redoUpdate(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *updateSub, RedoType redoType) {
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	HashIndexEntry *entry = m_mrecs->getHashIndex()->get(rid, ctx);
	if (entry != NULL && HIT_MHEAPREC == entry->m_type) {
		if (redoType == RT_PREPARE)
			session->getTrans()->lockRow(TL_X, rid, m_tab->getTableDef()->m_id);
		m_mrecs->redoUpdate(session, txnId, rid, rollBackId, tableIndex, updateSub);
		return;
	}

	TableDef *tableDef = m_tab->getTableDef();
	Record *rec = readNtseRec(session, rid);
	if (INVALID_ROW_ID == rec->m_rowId) {
		assert(redoType == RT_COMMIT);
		return;
	}
	if (redoType == RT_PREPARE)
		session->getTrans()->lockRow(TL_X, rid, m_tab->getTableDef()->m_id);
	RecordOper::updateRecordRR(tableDef, rec, updateSub);
	m_mrecs->redoUpdate(session, txnId, rid, rollBackId, tableIndex, rec);
}

/** remove的redo操作
 * @param session 会话
 * @param txnId 事务id
 * @param rid 记录id
 * @param rollbackId 回滚记录的rowid
 * @param tableIndex 版本池表序号
 */
void TNTTable::redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, RedoType redoType) {
	MemoryContext *ctx = session->getMemoryContext();
	TableDef *tableDef = m_tab->getTableDef();
	McSavepoint msp(ctx);
	HashIndexEntry *entry = m_mrecs->getHashIndex()->get(rid, ctx);
	if (entry != NULL && HIT_MHEAPREC == entry->m_type) {
		if (redoType == RT_PREPARE)
			session->getTrans()->lockRow(TL_X, rid, tableDef->m_id);
		m_mrecs->redoRemove(session, txnId, rid, rollBackId, tableIndex);
		return;
	}

	Record *rec = readNtseRec(session, rid);
	if (INVALID_ROW_ID == rec->m_rowId) {
		assert(redoType == RT_COMMIT);
		return;
	}
	if (redoType == RT_PREPARE)
		session->getTrans()->lockRow(TL_X, rid, tableDef->m_id);
	m_mrecs->redoRemove(session, txnId, rid, rollBackId, tableIndex, rec);
}

/**根据log redo 首次更新操作
 * @param session 会话
 * @param log 日志记录
 */
void TNTTable::redoFirUpdate(Session *session, const LogEntry *log, RedoType redoType) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	SubRecord *updateSub = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	m_mrecs->parseFirUpdateLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex, &updateSub, ctx);
	redoUpdate(session, trxId, rid, rollBackId, tableIndex, updateSub, redoType);
}

/**根据log redo非首次更新操作
 * @param session 会话
 * @param log 日志记录
 */
void TNTTable::redoSecUpdate(Session *session, const LogEntry *log, RedoType redoType) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	SubRecord *updateSub = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	m_mrecs->parseSecUpdateLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex, &updateSub, ctx);
	redoUpdate(session, trxId, rid, rollBackId, tableIndex, updateSub, redoType);
}

/**根据log redo首次删除操作
 * @param session 会话
 * @param log 日志记录
 */
void TNTTable::redoFirRemove(Session *session, const LogEntry *log, RedoType redoType) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	m_mrecs->parseFirRemoveLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex);
	redoRemove(session, trxId, rid, rollBackId, tableIndex, redoType);
}

/**根据log redo非首次删除操作
 * @param session 会话
 * @param log 日志记录
 */
void TNTTable::redoSecRemove(Session *session, const LogEntry *log, RedoType redoType) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	m_mrecs->parseSecRemoveLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex);
	redoRemove(session, trxId, rid, rollBackId, tableIndex, redoType);
}

LsnType TNTTable::undoInsertLob(Session *session, const LogEntry *log) {
	LsnType preLsn = 0;
	TrxId	trxId;
	LobId	lobId;
	TblLob	tblLob;
	LobStorage::parseTNTInsertLob(log,&trxId,&preLsn,&lobId);
	tblLob.m_lobId = lobId;
	tblLob.m_tableId = log->m_tableId;
	//从NTSE中删除大对象(如果是crash恢复，要检查事务的InsertLob哈希表,并允许重复删除)
	if(m_db->getStat() != DB_RECOVERING) {
		m_tab->deleteLob(session, lobId);
	} else {
		if(session->getTrans()->getRollbackInsertLobHash()== NULL 
			|| session->getTrans()->getRollbackInsertLobHash()->get(&tblLob) == NULL)
			m_tab->deleteLobAllowDupDel(session, lobId);
	}
	return preLsn;
}

/** 根据日志记录undo插入操作
 * @param session 会话
 * @param log     日志记录
 * @param crash   指明是crash recover的undo还是log recover的undo
 * return 同一事务的前一个日志的lsn
 */
LsnType TNTTable::undoInsert(Session *session, const LogEntry *log, const bool crash) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid;
	TableDef *tblDef = m_tab->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	DrsHeap::parseInsertTNTLog(log, &trxId, &preLsn, &rid);
	m_db->getTNTSyslog()->log(EL_DEBUG, "undoInsert rowId = "I64FORMAT"u, trxId = "I64FORMAT"u, preLsn = "I64FORMAT"u\n", rid, trxId, preLsn);
	//从ntse中删除记录
	u16 *columns = (u16 *)ctx->alloc(tblDef->m_numCols*sizeof(u16));
	for (u16 i = 0; i < tblDef->m_numCols; i++) {
		columns[i] = i;
	}
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(tblDef->m_maxRecSize), tblDef->m_maxRecSize);
	TblScan *scan = m_tab->positionScan(session, OP_DELETE, tblDef->m_numCols, columns, true);
	if (m_tab->getNext(scan, mysqlRec.m_data, rid)) {
		//摘除内存索引需要在ntse行锁保护下操作
		if (!crash) {
			//如果是非crash的recover需要对tnt也undo
			m_mrecs->undoInsert(session, rid);
		}
		m_tab->deleteCurrent(scan);
	} else if (!crash) {
		assert(m_mrecs->getHashIndex()->get(rid, ctx) == NULL);
	}
	m_tab->endScan(scan);
	return preLsn;
}

/** 根据日志记录undo首次更新操作
 * @param session 会话
 * @param log     日志记录
 * @param crash   指明是crash recover的undo还是log recover的undo
 * return 同一事务的前一个日志的lsn
 */
LsnType TNTTable::undoFirUpdate(Session *session, const LogEntry *log, const bool crash) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	SubRecord *updateSub = NULL;
	TableDef *tableDef = m_tab->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	Record *beforeImg = NULL;
	Record *afterImg = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc(tableDef->m_maxRecSize), tableDef->m_maxRecSize);
	m_mrecs->parseFirUpdateLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex, &updateSub, ctx);
	//先undo索引，再undo堆记录
	MHeapRec *rollbackRec = m_mrecs->getBeforeAndAfterImageForRollback(session, rid, &beforeImg, afterImg);
	if (!crash) {
		//做索引的undo
		assert(beforeImg == NULL && afterImg != NULL);
		m_indice->undoFirstUpdateOrDeleteIndexEntries(session, afterImg);
	}
	m_mrecs->undoFirUpdate(session, rid, rollbackRec);
	return preLsn;
}

/** 根据日志记录undo非首次更新操作
 * @param session 会话
 * @param log     日志记录
 * @param crash   指明是crash recover的undo还是log recover的undo
 * return 同一事务的前一个日志的lsn
 */
LsnType TNTTable::undoSecUpdate(Session *session, const LogEntry *log, const bool crash) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	SubRecord *updateRec = NULL;
	TableDef *tableDef = m_tab->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	Record *beforeImg = NULL;
	Record *afterImg = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc(tableDef->m_maxRecSize), tableDef->m_maxRecSize);
	m_mrecs->parseSecUpdateLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex, &updateRec, ctx);
	//先undo索引，再undo堆记录
	MHeapRec *rollBackRec = m_mrecs->getBeforeAndAfterImageForRollback(session, rid, &beforeImg, afterImg);
	if (!crash) {
		assert(beforeImg != NULL && afterImg != NULL);
		//做索引的undo
		SubRecord beforeSub(beforeImg->m_format, updateRec->m_numCols, updateRec->m_columns, beforeImg->m_data, beforeImg->m_size, beforeImg->m_rowId);
		SubRecord afterSub(afterImg->m_format, updateRec->m_numCols, updateRec->m_columns, afterImg->m_data, afterImg->m_size, afterImg->m_rowId);
		m_indice->undoSecondUpdateIndexEntries(session, &beforeSub, &afterSub);
	}
	m_mrecs->undoSecUpdate(session, rid, rollBackRec);
	return preLsn;
}

/** 根据日志记录undo首次删除操作
 * @param session 会话
 * @param log     日志记录
 * @param crash   指明是crash recover的undo还是log recover的undo
 * return 同一事务的前一个日志的lsn
 */
LsnType TNTTable::undoFirRemove(Session *session, const LogEntry *log, const bool crash) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	TableDef *tableDef = m_tab->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	Record *beforeImg = NULL;
	Record *afterImg = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc(tableDef->m_maxRecSize), tableDef->m_maxRecSize);
	m_mrecs->parseFirRemoveLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex);
	MHeapRec *rollBackRec = m_mrecs->getBeforeAndAfterImageForRollback(session, rid, &beforeImg, afterImg);	
	if (!crash) {
		//做索引的undo
		assert(beforeImg == NULL && afterImg != NULL);
		m_indice->undoFirstUpdateOrDeleteIndexEntries(session, afterImg);
	}
	m_mrecs->undoFirRemove(session, rid, rollBackRec);
	return preLsn;
}

/** 根据日志记录undo非首次删除操作
 * @param session 会话
 * @param log     日志记录
 * @param crash   指明是crash recover的undo还是log recover的undo
 * return 同一事务的前一个日志的lsn
 */
LsnType TNTTable::undoSecRemove(Session *session, const LogEntry *log, const bool crash) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	TableDef *tableDef = m_tab->getTableDef();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	Record *beforeImg = NULL;
	Record *afterImg = new (ctx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)ctx->alloc(tableDef->m_maxRecSize), tableDef->m_maxRecSize);
	m_mrecs->parseSecRemoveLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex);
	MHeapRec *rollBackRec = m_mrecs->getBeforeAndAfterImageForRollback(session, rid, &beforeImg, afterImg);	
	if (!crash) {
		//做索引的undo
		assert(beforeImg != NULL && afterImg != NULL);
		m_indice->undoSecondDeleteIndexEntries(session, afterImg);
	}
	m_mrecs->undoSecRemove(session, rid, rollBackRec);
	return preLsn;
}

/** 加表元数据锁
 * @pre 必须未加数据锁，即加表元数据锁必须在加数据锁之前
 *
 * @param session 会话
 * @param mode 锁模式，只能是S、U或X
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时
 */
void TNTTable::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	assert(mode == IL_S || mode == IL_U || mode == IL_X);
	ftrace(ts.ddl || ts.dml, tout << this << session << mode << timeoutMs);

	IntentionLock *metaLock = m_tab->getMetaLockInst();
	if (!metaLock->lock(session->getId(), mode, timeoutMs, file, line)) {
		nftrace(ts.ddl || ts.dml, tout << "Lock failed");
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Acquire meta lock %s.", IntentionLock::getLockStr(mode));
	}
	session->addLock(metaLock);
}

/** 升级表元数据锁。若oldMode与newMode相等或oldMode是比newMode更高级的锁，则不进行任何操作。
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时或失败，NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void TNTTable::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	assert(oldMode == IL_NO || oldMode == IL_S || oldMode == IL_U || oldMode == IL_X);
	assert(newMode == IL_NO || newMode == IL_S || newMode == IL_U || newMode == IL_X);
	ftrace(ts.ddl || ts.dml, tout << this << session << oldMode << newMode << timeoutMs);

	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	if (newMode == oldMode || IntentionLock::isHigher(newMode, oldMode))
		return;
	assert(IntentionLock::isHigher(oldMode, newMode));
	if (!m_metaLock->upgrade(session->getId(), oldMode, newMode, timeoutMs, file, line)) {
		nftrace(ts.ddl || ts.dml, tout << "Upgrade failed");
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Upgrade meta lock %s to %s.", 
			IntentionLock::getLockStr(oldMode), IntentionLock::getLockStr(newMode));
	}
	if (oldMode == IL_NO && newMode != IL_NO)
		session->addLock(m_metaLock);
}

/** 降级表元数据锁。若oldMode与newMode相等，若newMode比oldMode高级则不进行任何操作
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 */
void TNTTable::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	assert(oldMode == IL_NO || oldMode == IL_S || oldMode == IL_U || oldMode == IL_X);
	assert(newMode == IL_NO || newMode == IL_S || newMode == IL_U || newMode == IL_X);
	ftrace(ts.ddl || ts.dml, tout << this << session << oldMode << newMode);

	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	if (oldMode == newMode || IntentionLock::isHigher(oldMode, newMode))
		return;
	assert(IntentionLock::isHigher(newMode, oldMode));
	m_metaLock->downgrade(session->getId(), oldMode, newMode, file, line);
	if (oldMode != IL_NO && newMode == IL_NO)
		session->removeLock(m_metaLock);
}

/** 释放表元数据锁
 * @pre 必须在释放表数据锁之后调用
 *
 * @param session 会话
 * @param mode 锁模式
 */
void TNTTable::unlockMeta(Session *session, ILMode mode) {
	ftrace(ts.ddl || ts.dml, tout << this << session << mode);

	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	m_metaLock->unlock(session->getId(), mode);
	session->removeLock(m_metaLock);
}

/**	在TNTTable Instance不存在的情况下，释放元数据锁
*	@session	会话
*	@table		NTSE Table Instance
*	@mode		元数据锁模式
*/
void TNTTable::unlockMetaWithoutInst(Session *session, Table *table, ILMode mode) {
	IntentionLock *m_metaLock = table->getMetaLockInst();
	m_metaLock->unlock(session->getId(), mode);
	session->removeLock(m_metaLock);
}

/** 得到指定的会话加的表元数据锁模式
 * @param session 会话
 * @return 会话所加的表元数据锁，若没有加锁则返回IL_NO
 */
ILMode TNTTable::getMetaLock(Session *session) const {
	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	return m_metaLock->getLock(session->getId());
}

/** 得到表元数据锁使用情况统计信息
 * @return 表锁使用情况统计信息
 */
/*
const ILUsage* TNTTable::getMetaLockUsage() const {
	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	return m_metaLock->getUsage();
}
*/

/** 得到Ntse Table对应的Records对象
*
* @return NTSE Records对象
*/
Records* TNTTable::getRecords() const {
	return m_tab->getRecords();
}

u64 TNTTable::getMRecSize() {
	return m_tabBase->m_records->getHashIndex()->getSize();
}

}