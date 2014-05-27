/**
* TNT���棬����ģ�顣
*
* @author �εǳ�
*/

#include "api/TNTTable.h"
#include "api/TblArgAlter.h"
#include "api/Table.h"
#include "api/TNTTblMnt.h"
#include "btree/IndexKey.h"
#include "btree/MIndexKey.h"

namespace tnt {

/**
* ��һ��TNTTableBase�����������һ�δ򿪣������ʵ�ʴ򿪺������������Ѿ��򿪣����������е�NTSEָ��
* @param session	session����
* @param db			TNTDatabase����
* @param tableDef	NTSE TableDef����
* @param lobStorage NTSE����������
* @param drsIndice	NTSE��������
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

	// ����ڴ�������ʼ��ʧ�ܣ��ڴ治�㣩
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
* �����ر�һ��TNTTableBase�ڴ�ṹ
* @param session	session����
* @param flush		��ʶ�Ƿ�Ҫ����flush����
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
* �ڹرձ�ʱ��TNTTableBase�ڴ���󲢲��رգ���ʱ��Ҫ���ڴ������ָ��NTSE��ָ������
* @param session	session����
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

/**	����TNT�ڴ��
*
*/
void TNTTable::create(TNTDatabase *db, Session *session, const char *path, TableDef *tableDef) throw(NtseException) {
	// ��ǰ��TNTTable����Ҫ֧��create����
	UNREFERENCED_PARAMETER(db);
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(path);
	UNREFERENCED_PARAMETER(tableDef);
}

/**	ɾ��TNT�ڴ��
*	@db		TNT Database����
*	@path	��·��
*/
void TNTTable::drop(TNTDatabase *db, const char *path) throw(NtseException) {
	// DDL��������TNTTableBase
	UNREFERENCED_PARAMETER(db);
	UNREFERENCED_PARAMETER(path);
}

/**	��TNT�ڴ��
*	@db			TNT Database����
*	@session	session����
*	@ntseTable	NTSE�����
*	@tableBase	TNT������ṹ����
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

	// ����������Ϣ
	table->m_db			= db;
	table->m_tab		= ntseTable;
	table->m_mrecs		= tableBase->m_records;
	table->m_indice		= tableBase->m_indice;
	table->m_tabBase	= tableBase;	

	return table;
}

/** ��Open��ʱ������ͳ�Ʊ��еļ�¼����
*** Ŀǰ����ֱ�ӵ���NTSE Table�ṩ�ķ�������ͳ��TNT�ڴ���ɾ���ļ�¼����
* @param session
* 
*/
void TNTTable::refreshRows(Session* session) {
	m_tab->refreshRows(session);
}

/**	������һ���ڴ��
*	@db			TNT Database����
*	@session	session����
*	@oldPath	ԭ����
*	@newPath	�±���
*/
void TNTTable::rename(TNTDatabase *db, Session *session, const char *oldPath, const char *newPath) {
	// DDL��������TNTTableBase
	UNREFERENCED_PARAMETER(db);
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(oldPath);
	UNREFERENCED_PARAMETER(newPath);
}

/**	�ر�һ��TNT�ڴ��
*	@session			session����
*	@flushDirty			�Ƿ��ڴ����purge��NTSE
*	@closeComponents	�Ƿ��ڴ����ر�
*	@������				һ�ڣ�����������������
*/
void TNTTable::close(Session *session, bool flushDirty, bool closeComponents) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(flushDirty);
	UNREFERENCED_PARAMETER(closeComponents);
	
	// ���ڴ�TNTTableBase�������漰����NTSE�Ķ���ָ������ΪNULL
	// m_tabBase->closeNtseRef(NULL, NULL);
	m_tabBase->m_records->replaceComponents(NULL);
}

/**	truncate TNT�ڴ��
*	@session			session����
*/
void TNTTable::truncate(Session *session) {
	assert(m_tabBase->m_opened && !m_tabBase->m_closed);

	// �ر�TNT�ڴ�heap��indice���ͷſռ�
	m_tabBase->close(session, false);
	// ����m_tabBase��ַδ�����仯����˲���Ҫ���´���
	// ֱ�Ӵ�TNT�ڴ�heap��indice����ʱ��Ϊ��
	m_tabBase->open(session, m_db, m_tab->getTableDefAddr(), m_tab->getLobStorage(), m_tab->getIndice());

	// ���������ڴ�TNTIndice��MRecords
	m_indice = m_tabBase->m_indice;
	m_mrecs	 = m_tabBase->m_records;

	return;
}

/**	��������
 *	@param session	session����
 *	@param indexDefs	����������
 *	@param numIndice	Ҫ���ӵ�������������onlineΪfalseֻ��Ϊ1
 */
void TNTTable::addIndex(Session *session, const IndexDef **indexDefs, u16 numIndice) throw(NtseException) {
	// 1.��ǰ����������ntse��������
	if (indexDefs[0]->m_online) {
		TNTTblMntAlterIndex alter(this, numIndice, indexDefs, 0, NULL);
		alter.alterTable(session);
		return;
	}

	TNTTransaction *trx	= session->getTrans();
	uint timeout = session->getNtseDb()->getConfig()->m_tlTimeout * 1000;
	// 1. ��ȡMeta Lock
	// ����U������ֹ���������������ڱ�Ԫ���ݵ��޸�
	ILMode oldMetaLock = getMetaLock(session);
	try {
		upgradeMetaLock(session, oldMetaLock, IL_U, timeout, __FILE__, __LINE__);
	} catch (NtseException &e) {
		throw e;
	}

	//2.��ǰ����������mysql��������
	string idxInfo;
	//��ǰ�����Ƿ���δ��ɵ���������
	if (m_tab->hasOnlineIndex(&idxInfo)) {
		if (m_tab->isDoneOnlineIndex(indexDefs, numIndice)) {
			//�ж����߽�����˳���Ƿ���ȷ
			if (!m_tab->isOnlineIndexOrder(indexDefs, numIndice)) {
				downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
				NTSE_THROW(NTSE_EC_GENERIC, "Add online index order was wrong, %s.", idxInfo.c_str());
			}
			//��ɿ��ٴ������������������
			session->setTrans(NULL); //��������ΪNULL����ΪdualFlushAndBump����Ҫ����meta lock
			m_tab->dualFlushAndBump(session, timeout); //��������ὫmetaLock��IL_U��ΪIL_X
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
	
	// 2. ���ϱ�����������ֻ֤��ֻ���������Է���
	ILMode oldLock = m_tab->getLock(session);
	u16 tableId = m_tab->getTableDef(true, session)->m_id;
	try {
		//��trxΪnull������£���ʱ��ntse������table lock�Ѿ���ntse_handler::extern_lock�м���
		if (trx != NULL) {
			//���addIndex����ha_tnt���ã���ô��ʱS�����Ѿ����ڣ������������������
			trx->lockTable(TL_S, tableId);
		} else {
			m_tab->upgradeLock(session, oldLock, IL_S, timeout, __FILE__, __LINE__);
		}
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// ��Meta U Lock�ı����£�������洴�������ĵ�һ�׶β���
	try {
		m_tab->addIndexPhaseOne(session, numIndice, indexDefs);
	} catch (NtseException &e) {
		m_tab->downgradeLock(session, m_tab->getLock(session), oldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// ����Meta U Lock��X Lock���޸�TableDef��Ϣ���Լ������ڴ���������ʱ���ݽ�ֹ������
	// TODO: ��ʱ�Ƿ���Ҫ����������ΪX����
	// Ŀǰ������Ҫ����Ϊ������MySQL�ϲ����������TNT�ڲ��������ڲ�����ʱ���������
	// Meta S Lock����ʱ��Meta Lock����ΪX����Ҳ����ζ�ű��ϵ���������DML������������
	upgradeMetaLock(session, getMetaLock(session), IL_X, -1, __FILE__, __LINE__);

	try {
		m_tab->addIndexPhaseTwo(session, numIndice, indexDefs);

		addMemIndex(session, numIndice, indexDefs);
	} catch (NtseException &e) {
		m_tab->downgradeLock(session, m_tab->getLock(session), oldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// 3. �ͷ�Lock
	m_tab->downgradeLock(session, m_tab->getLock(session), oldLock, __FILE__, __LINE__);
	downgradeMetaLock(session, IL_X, oldMetaLock, __FILE__, __LINE__);
}

/**	ɾ������
 *	@param session	session����
 *	@param idx		Ҫɾ�����ǵڼ�������
 */
void TNTTable::dropIndex(Session *session, uint idx) throw(NtseException) {
	uint timeout = session->getNtseDb()->getConfig()->m_tlTimeout * 1000;
	// 1. ��ȡMeta Lock��ntse��ģʽ
	ILMode oldMetaLock = getMetaLock(session);
	ILMode ntseOldLock = m_tab->getLock(session);

	// 2. ����MetaLock
	try {
		upgradeMetaLock(session, oldMetaLock, IL_U, timeout, __FILE__, __LINE__);
	} catch (NtseException &e) {
		throw e;
	}

	// 3. ��ȡTable Lock
	u16 tableId	= m_tab->getTableDef(true, session)->m_id;
	TNTTransaction *trx = session->getTrans();
	try {
		if (trx != NULL) {
			//��lock table SΪ�˵ȴ��������ñ�Ļ�Ծ����Ľ���
			trx->lockTable(TL_S, tableId);
		}
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}

	// 4. NTSEˢ����
	TableDef * tableDef = m_tab->getTableDef();
	if (tableDef->m_indice[idx]->m_primaryKey && tableDef->m_cacheUpdate) {
		assert(tableDef->m_useMms);
		//����X��
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
		// ��ʱ�Ѿ����˱�Ԫ����X����ˢ����������
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
	// 5. ��Meta X���޸�TableDef
	// ����������
	try {
		if (trx != NULL) {
			trx->lockTable(TL_IS, tableId);
		}
	} catch (NtseException &e) {
		downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
		throw e;
	}
	if (NULL != trx) {
		trx->unlockTable(TL_S, tableId);	//�ŵ�S��������IS��
	}

	try {
		upgradeMetaLock(session, getMetaLock(session), IL_X, timeout, __FILE__, __LINE__);
		//���������ɾ��������һ�׶��޸�tableDef
		m_tab->dropIndexPhaseOne(session, idx, ntseOldLock);

		//ֻ�е���������ɹ�����ܲ����ڴ�������
		//�����ڴ���ɾ������һ�׶Σ��޸��������飬���Ҵ��ڴ���ɾ����������
		dropMemIndex(session, idx);
		assert(m_indice->getMemIndice()->getIndexNum() == m_indice->getDrsIndice()->getIndexNum());
	} catch (NtseException &e){
		m_tab->downgradeLock(session, m_tab->getLock(session), ntseOldLock, __FILE__, __LINE__);
		downgradeMetaLock(session, getMetaLock(session), oldMetaLock, __FILE__, __LINE__);
		throw e;
	} 

	// 6. ����meta��ΪU��
	downgradeMetaLock(session, getMetaLock(session), IL_U, __FILE__, __LINE__);

	// 7. ɾ�������������
	m_tab->dropIndexPhaseTwo(session, idx, ntseOldLock);

	// �ͷ�DDL Lock
	m_tab->downgradeLock(session, m_tab->getLock(session), ntseOldLock, __FILE__, __LINE__);
	downgradeMetaLock(session, IL_U, oldMetaLock, __FILE__, __LINE__);
}

/**	�����ڴ�����
*	@session	session����
*	@numIndice	Ҫ���ӵ�������������onlineΪfalseֻ��Ϊ1
*	@indexDefs	����������
*/
void TNTTable::addMemIndex(Session *session, u16 numIndice, const IndexDef **indexDefs) {
	u16 i = 0;
	int idxNo = 0;

	// ����TNTIndice�ṩ�ķ�������������
	TableDef *tableDef = m_tab->getTableDef();
	// �����ڴ�������indexDef����ʹ��tableDef�е�indexDef
	for (i = 0; i < numIndice; i++) {
		idxNo = tableDef->m_numIndice - numIndice + i;
		const IndexDef *indexDef = tableDef->getIndexDef(idxNo);
		assert(!strcmp(indexDef->m_name, indexDefs[i]->m_name));
		m_indice->createIndexPhaseTwo(session, indexDef, idxNo);
	}
	assert(m_indice->getIndexNum() == m_indice->getDrsIndice()->getIndexNum());

	//Ϊ������������
	MHeapRec *heapRec = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	u16 *columns = (u16 *)ctx->alloc(tableDef->m_numCols*sizeof(u16));
	for (i = 0; i < tableDef->m_numCols; i++) {
		columns[i] = i;
	}
	MHeapScanContext *scanCtx = m_mrecs->beginScan(session);
	while(true) {
		//�˴���Ҫ��ȡ�������¼�¼
		if(!m_mrecs->getNext(scanCtx, NULL)) 
			break;
		McSavepoint msp(ctx);
		heapRec = scanCtx->m_heapRec;
		void *buf = ctx->alloc(sizeof(SubRecord));
		SubRecord *SubRec = new (buf)SubRecord(REC_REDUNDANT, tableDef->m_numCols, columns, heapRec->m_rec.m_data, tableDef->m_maxRecSize, heapRec->m_rec.m_rowId);
		RowIdVersion version = m_mrecs->getHashIndex()->get(heapRec->m_rec.m_rowId, ctx)->m_version;
		//��������,����������version�õ���ʲô��
		for (i = 0; i < numIndice; i++) {
			McSavepoint lobSavepoint(session->getLobContext());

			u8 idxNo = tableDef->m_numIndice - numIndice + i;
			MIndex *mIndex = m_indice->getMemIndice()->getIndex(idxNo);
			IndexDef *indexDef = tableDef->getIndexDef(idxNo);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, tableDef, indexDef, m_tab->getLobStorage(), SubRec, &lobArray);
			}
			// ����¼ת��ΪPAD��ʽ���������������������
			SubRecord *key = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), SubRec, &lobArray, tableDef, indexDef);
			mIndex->insert(session, key, version);
		}
	}
	m_mrecs->endScan(scanCtx);
}

/**	ɾ���ڴ�����
*	@session	session����
*	@idx		Ҫɾ�����ǵڼ�������
*
*/
void TNTTable::dropMemIndex(Session *session, uint idx) {
	// ����TNTIndice�ṩ�ķ�����ɾ������
	m_indice->dropPhaseOne(session, idx);
	//m_indice->dropPhaseTwo(session, idx);
}

/**	�Ż�������
*	@session	session����
*	@keepDict	�Ƿ�ָ���˱������ֵ�(�����ȫ���ֵ�Ļ�)
*	
*/
void TNTTable::optimize(Session *session, bool keepDict, bool *newHasDic, bool *cancelFlag) throw(NtseException) {
	if (m_tab->getTableDef()->m_indexOnly)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index only table can not be optimized");

	// ������ԭ�е��������Ƿ�����������
	// TODO: Ŀǰ�ݲ�֧�ְ���������е����߲���
	for (int i = 0; i < m_tab->getTableDef()->m_numIndice; i++) {
		if (m_tab->getTableDef()->getIndexDef(i)->hasLob()) {
			NTSE_THROW(NTSE_EC_GENERIC, "Table with Lob/LongVar Index can not be optimized");
		}
	}

	// OPTIMIZEͨ��һ�����������κ��ֶ�Ҳ��ɾ���κ��ֶε�������ɾ�ֶβ���ʵ��
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
	// DDL��������TNTTableBase
	return;
}

void TNTTable::flush(Session *session) {
	UNREFERENCED_PARAMETER(session);
	// DDL��������TNTTableBase
	return;
}

// ���ߴ�����������Ҫ֪��TNT��TNT����purge�ڴ�+ntse���ߴ��������ķ�ʽ���
void TNTTable::setOnlineAddingIndice(Session *session, u16 numIndice, const IndexDef **indice) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(numIndice);
	UNREFERENCED_PARAMETER(indice);
	// DDL��������TNTTableBase
	return;
}

void TNTTable::resetOnlineAddingIndice(Session *session) {
	UNREFERENCED_PARAMETER(session);
	// DDL��������TNTTableBase
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
			//Hash����ƥ��ʱ���� 0 ���� MIN_TBL_ARG_CMD_TYPE_VALUE
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
 * �޸Ķ�����
 * 
 * ֻ֧�ִӶ����ѵ��䳤�ѵĵ����޸ģ�������ͨ������ȥ�������ܺ�ʱ�ϳ�
 * �������������޸Ĳ�����ͬ���ǣ��޸Ķ����Ͳ������preAlter��ˢ�����ݺ�д��־����
 * ֻ����TblMntAlterColumn::alterTable����д��־�������TblMntAlterColumn::alterTable
 * ��д��־֮ǰϵͳ�������������redo�޸Ķ����͵Ĳ���(ע��TblMntAlterColumn::alterTable
 * ֻ�����滻���ļ�֮ǰ��д��־)
 *
 * @param session       �Ự
 * @param fixlen        �¶������Ƿ��Ƕ�������
 * @param timeout       ������ʱʱ�䣬��λ��
 * @throw NtseException ���䳤�Ѹ�Ϊ�����ѣ������ڿ�����ʱ����
 */
void TNTTable::alterHeapFixLen(Session *session, bool fixlen, int timeout) throw(NtseException) {
	assert(NULL != session);
	assert(!session->getTrans());
	TableDef *tableDef = m_tab->getTableDef();
	//��Ϊ��ʱsession��û��trx�����Կ���ʹ��TblLockGuard������unlock����Ч
	TblLockGuard guard;
	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	guard.attach(session, this->getNtseTable());

	m_tab->checkPermission(session, false);
	if (fixlen) {
		if (tableDef->m_recFormat == REC_FIXLEN) {//�Ѿ��Ƕ�����
			assert(tableDef->m_fixLen);
			assert(tableDef->m_origRecFormat == tableDef->m_recFormat);
			return;
		} else {//��֧�ֽ��䳤�Ѹ�Ϊ������
			NTSE_THROW(NTSE_EC_GENERIC, "Can't change variable length heap to fixed length !"); 
		}
	} else {
		if (tableDef->m_recFormat != REC_FIXLEN)//�Ѿ����Ƕ�����
			return;
	}

	//�Ӷ�����ͨ������ɾ�еı������޸�Ϊ�䳤��
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
 * �޸�table�Ƿ�֧���������
 *
 * @param session		�Ự��
 * @param supportTrx	�Ƿ�֧����������� true ��ʾ֧�����������false��ʾ��֧�����������
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param inLockTables  �����Ƿ���Lock Tables ���֮��
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void TNTTable::alterSupportTrxStatus(Session *session, TableStatus tableStatus, int timeout, bool inLockTables) throw(NtseException) {
	assert(session->getConnection()->isTrx());
	assert(!session->getTrans());
	assert(tableStatus == TS_NON_TRX || tableStatus == TS_TRX);

	//��Ϊ��ʱsession��û��trx�����Կ���ʹ��TblLockGuard������unlock����Ч
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
	//���ü�table��X���ȴ������ڸñ������ȫ������,ͬʱ��Ϊ����meta�������Բ�����������������ñ�
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

	//����TS_CHANGING�ǲ����л�����棬���Իָ������в����ܳ���TS_CHANGING״̬
	m_tab->getTableDef()->setTableStatus(TS_CHANGING);
	guard.detach();
	m_tab->unlockMeta(session, IL_X);

	//����л��ɷ�����״̬����Ҫ�ڴ�����Ϊ��
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

/**	��ʼ��ɨ��
*	@session		session����
*	@opType			��������
*	@opInfo			����ɨ����Ϣ
*	@numReadCols	Ҫ��ȡ��������������Ϊ0
*	@readCols		Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź��򡣲���ΪNULL
*	@tblLock		�Ƿ�Ҫ�ӱ���
*	@lobCtx			���ڴ洢������ô�������ݵ��ڴ���������ģ���ΪNULL����ʹ��Session::getLobContext
*					��ʹ��Session::getLobContext������������ʱ���ڻ�ȡ��һ����¼��endScanʱ����һ�ڴ�
*					�ᱻ�Զ��ͷţ��������������ݵ��ڴ�������������ⲿָ���������ⲿ�����ͷ�
*	@throw	NtseException	�ӱ�����ʱ
*	@return			ɨ����
*/
TNTTblScan* TNTTable::tableScan(Session *session, OpType opType, TNTOpInfo *opInfo, u16 numReadCols, u16 *readCols, bool tblLock, MemoryContext *lobCtx) throw(NtseException) {
	UNREFERENCED_PARAMETER(tblLock);

	TNTTblScan *scan = beginScan(session, ST_TBL_SCAN, opType, numReadCols, readCols, lobCtx);
	scan->m_opInfo = opInfo;

	// ���� TNT ������ģʽ
	scan->determineScanLockMode(opInfo->m_selLockType);

	// ���� NTSE ����ģʽ��һ����Shared
	LockMode ntseRowLockMode = Shared;
	scan->m_pRlh = &scan->m_rlh;

	Records::Scan *recScan = m_tab->getRecords()->beginScan(session, opType, ColList(numReadCols, readCols), lobCtx, 
		ntseRowLockMode, scan->m_pRlh, session->getConnection()->getLocalConfig()->m_accurateTblScan);
	scan->m_recInfo = recScan;
	// ��ʼ��m_mysqlRow
	scan->m_mysqlRow= recScan->getMysqlRow();
	// ��ʼ��m_redRow
	scan->m_redRow	= recScan->getRedRow();

	return scan;
}

/**	��ʼ����ɨ��
*	@session	session����
*	@opType		��������
*	@opInfo		����ɨ����Ϣ
*	@cond		ɨ����������������ǳ����һ�ݡ�������Ϊ�գ�������cond->m_keyΪNULL��
*				������cond->m_key��ΪNULL������m_dataΪNULL��n_numColsΪ0.
*	@numReadCols  Ҫ��ȡ��������������Ϊ0	
*	@readCols	Ҫ��ȡ�ĸ����Ժţ���0��ʼ�������ź��򡣲���ΪNULL
*	@unique		�Ƿ�Ϊunique scan��Ŀǰͨ��range scanģ��ʵ��unique scan
*	@tblLock	�Ƿ�Ҫ�ӱ���
*	@lobCtx		���ڴ洢�����صĴ�������ݵ��ڴ����������
*	@throw	NtseException	�ӱ�����ʱ
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
		// TODO�����������ȡ��ò��û��
		scan->m_idxExtractor = SubToSubExtractor::createInst(session->getMemoryContext(), tabDef, scan->m_indexDef, scan->m_indexDef->m_numCols, 
			scan->m_indexDef->m_columns, numReadCols, readCols, KEY_COMPRESS, KEY_PAD, 
			cond->m_singleFetch ? 1:1000);
		void *p	= mc->alloc(sizeof(SubRecord));
		scan->m_redRow = new (p)SubRecord(REC_REDUNDANT, numReadCols, readCols, NULL, tabDef->m_maxRecSize); // ����һ�������ʽ�Ӽ�¼�����ڸ���ɨ���ֵ��ȡ
	}
	
	void *p			 = mc->alloc(sizeof(SubRecord));
	scan->m_mysqlRow = new (p)SubRecord(REC_MYSQL, numReadCols, readCols, NULL, tabDef->m_maxMysqlRecSize);


	// idxKey ΪPAD ��ʽ�����뵥�������ڴ�
	p				= mc->alloc(sizeof(SubRecord));
	byte * data		= (byte*) mc->alloc(scan->m_indexDef->m_maxKeySize + 1);
	scan->m_idxKey = new (p)SubRecord(KEY_PAD, scan->m_indexDef->m_numCols, scan->m_indexDef->m_columns, data, scan->m_indexDef->m_maxKeySize);
	
	// ����һ��BulkFetch�������������ر�ʱ����ȡ����NTSE��¼
	// ע�⣺�ر��ȡ����NTSE��¼�������ļ�¼����fullRow������redRow
	ColList fullColList = ColList::generateAscColList(mc, 0, scan->m_tableDef->m_numCols);
	Records::BulkFetch *fetch = m_tab->getRecords()->beginBulkFetch(session, opType, fullColList, lobCtx, Shared);
	scan->m_recInfo = fetch;

	if (cond->m_singleFetch) {
		// TODO���˴�������Ҫ�޸ģ���汾��Unique scan����Ҫ���⴦��
		assert(tabDef->m_indice[cond->m_idx]->m_unique 
			&& cond->m_key->m_numCols == tabDef->m_indice[cond->m_idx]->m_numCols);
		// return scan;
	}

	// ������range scan������unique scan��Ŀǰ����range scan���ʵ��
	scan->m_indexScan = scan->m_index->beginScan(session, cond->m_key, scan->m_idxKey, unique, 
		cond->m_forward, cond->m_includeKey, opInfo->m_selLockType, scan->m_idxExtractor);

	return scan;
}

/**	��ʼָ��RIDȡ��¼�Ķ�λɨ��
*	@session		ͬ��
*	@opType			ͬ��
*	@numReadCols	ͬ��
*	@readCols		ͬ��
*	@tblLock		ͬ��
*	@lobCtx			ͬ��
*	@throw NtseException
*	@return ɨ����
*/
TNTTblScan* TNTTable::positionScan(Session *session, OpType opType, TNTOpInfo *opInfo, u16 numReadCols, u16 *readCols, bool tblLock, MemoryContext *lobCtx) throw(NtseException) {
	UNREFERENCED_PARAMETER(tblLock);

	MemoryContext *mc = session->getMemoryContext();
	
	TNTTblScan *scan = beginScan(session, ST_POS_SCAN, opType, numReadCols, readCols, lobCtx);
	scan->m_opInfo = opInfo;

	scan->determineScanLockMode(opInfo->m_selLockType);
	
	void *p			= mc->alloc(sizeof(SubRecord));
	scan->m_mysqlRow= new (p)SubRecord(REC_MYSQL, numReadCols, readCols, NULL, m_tab->getTableDef()->m_maxRecSize);

	// ����һ��BulkFetch�������ڶ�ȡNTSE������¼
	// ע�⣺�ر��ȡ����NTSE��¼�������ļ�¼����m_fullRow������m_redRow
	ColList fullColList = ColList::generateAscColList(mc, 0, scan->m_tableDef->m_numCols);
	Records::BulkFetch *fetch = m_tab->getRecords()->beginBulkFetch(session, opType, fullColList, lobCtx, Shared);
	scan->m_recInfo = fetch;

	return scan;
}

/**	��ʼɨ�裬��ɸ���ɨ�蹲ͬ�ĳ�ʼ��
*	@session	session����
*	@type		ɨ������
*	@opType		��������
*	@numReadCols  Ҫ��ȡ��������������Ϊ0
*	@readCols	Ҫ��ȡ�ĸ����Ժ�
*	@lobCtx		�����MemoryContext
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

	// ��ʼ��m_fullRow������scan����Ҫ������ڹ��������г�ʼ��
	// 1. ���ڴ��undo�м�����������¼
	// 2. ����ȡNTSE������¼
	byte *data	= (byte *)mc->alloc(scan->m_tableDef->m_maxRecSize);
	void *p			= mc->alloc(sizeof(SubRecord));
	ColList fullColList = ColList::generateAscColList(mc, 0, scan->m_tableDef->m_numCols);
	scan->m_fullRow = new (p)SubRecord(REC_REDUNDANT, fullColList.m_size, fullColList.m_cols, data, scan->m_tableDef->m_maxRecSize);

	// ��ʼ��m_fullRec����m_fullRow����m_data�ռ�
	// ע�⣺m_fullRow��ʵ��һ��SubRecord����m_fullRec����������¼Record
	p = mc->alloc(sizeof(Record));
	scan->m_fullRec = new (p)Record(INVALID_ROW_ID, REC_REDUNDANT, data, scan->m_tableDef->m_maxRecSize);

	if (m_tab->getRecords()->hasValidDictionary()) {//�����ֵ����
		assert(m_tab->getRecords()->getRowCompressMng());
		scan->m_subExtractor = SubrecExtractor::createInst(session, scan->m_tableDef, scan->m_fullRow, (uint)-1, m_tab->getRecords()->getRowCompressMng());
	} else
		scan->m_subExtractor = SubrecExtractor::createInst(session, scan->m_tableDef, scan->m_fullRow);

	session->incOpStat(TNTTblScan::getTblStatTypeForScanType(type));

	return scan;
}

/**	����һ��ɨ����
*	@session	session�Ự
*	@numCols	Ҫ��ȡ��������������Ϊ0
*	@columns	Ҫ��ȡ�������б�����ΪNULL
*	@opType		��������
*	@return		ɨ���������е��ڴ��session��MemoryContext�з���
*
*/
TNTTblScan* TNTTable::allocScan(Session *session, u16 numCols, u16 *columns, OpType opType) {
	assert(numCols && columns);

	// �����г�Ա��ʼ��Ϊ0 or NULL
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

/**	��λ��ɨ�����һ����¼�������ռ�õ��ڴ潫���Զ��ͷţ�
*	�����Ƕ�汾�������ƣ�
*	@post		���ն��²�����
*	@post		��ǰ����Ҫ�����������ļ�¼�����������������ύ���߻ع���ʱ���ͷ�
*
*	@scan		ɨ����
*	@mysqlRow	OUT���洢ΪREC_MYSQL��ʽ
*	@rid		��ѡ������ֻ�ڶ�λɨ��ʱȡָ���ļ�¼
*	@return		�Ƿ�λ��һ����¼
*/
bool TNTTable::getNext(TNTTblScan *scan, byte *mysqlRowUpperLayer, RowId rid, bool needConvertToUppMysqlFormat) throw(NtseException) {
	assert(scan->m_scanType == ST_POS_SCAN || rid == INVALID_ROW_ID);

	MemoryContext *ctx = scan->m_session->getMemoryContext();
	McSavepoint savepoint(ctx);

	// ת���ϲ�MYSQL��ʽ��¼�������MYSQL��ʽ��¼ 
	byte *mysqlRow = mysqlRowUpperLayer;
	if (m_tab->getTableDef()->hasLongVar() && needConvertToUppMysqlFormat) {
		mysqlRow = (byte *) ctx->alloc(m_tab->getTableDef()->m_maxRecSize);
	}

	// ����ǵ�һ�ν���getNext����������ݲ������ͣ��ж��Ǽӱ�����������������ReadView
	if (scan->m_opInfo != NULL &&
		scan->m_opInfo->m_sqlStatStart == true &&
		scan->m_opInfo->m_mysqlOper == true &&
		scan->m_scanType != ST_POS_SCAN) {

			scan->m_opInfo->m_sqlStatStart = false;

			if (scan->m_opInfo->m_selLockType != TL_NO) {
				// ��ǰ�����Ա��������
				assert(scan->m_tabLockMode == TL_IS || scan->m_tabLockMode == TL_IX);

				scan->m_trx->lockTable(scan->m_tabLockMode, m_tab->getTableDef()->m_id);
			} else {
				// ���ն�������ReadView
				if (!scan->m_trx->getReadView())
					scan->m_trx->trxAssignReadView();
			}
	}
	
	// ����memory context��savepoint 1
	u64 sp1 = ctx->setSavepoint();

_ReStart1:
	releaseLastRow(scan, true);
	if (scan->checkEofOnGetNext())
		return false;
	
	// reset to savepoint 1
	ctx->resetToSavepoint(sp1);

	// �ж�ɨ������
	bool exist;
	if (scan->m_scanType == ST_TBL_SCAN) {
		// ��һ��ȫ��ɨ�裬������¼���ڴ����¼���°汾���жϿɼ��Եȵ�
		Records::Scan *rs = (Records::Scan *)(scan->m_recInfo);

		m_tab->getRecords()->getHeap()->storePosAndInfo(rs->getHeapScan());
		BufferPageHandle *pageHdl = rs->getHeapScan()->getPage();
		u64 pageId = INVALID_PAGE_ID;
		if (pageHdl != NULL) {
			pageId = pageHdl->getPageId();
		}

		// ����memory context��savepoint 2
		u64 sp2 = ctx->setSavepoint();

_ReStart2:
		// reset to savepoint 2
		ctx->resetToSavepoint(sp2);

		// Table Scan��getNext����֮��ȡ���ļ�¼��һ������ NTSE Shared Lock

		// NTSE Shared Lock�Ĺ��ܣ�
		// 1. ���ڱ����������Ļ�ȡ
		// 2. ����ԭ��NTSE�Ĳ�������Э��
		exist = rs->getNext(mysqlRow);

		if (!exist) {
			stopUnderlyingScan(scan);
			scan->m_eof = true;
			return false;
		}
		// ��ȡRowId
		scan->m_rowId = rs->getRedRow()->m_rowId;
		TNTTransaction *trx = scan->getSession()->getTrans();
		TLockMode lockMode = scan->getRowLockMode();

		SYNCHERE(SP_ROW_LOCK_BEFORE_LOCK);
		if (lockMode != TL_NO && !trx->tryLockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id)) {
			SYNCHERE(SP_ROW_LOCK_AFTER_TRYLOCK);
			//�ͷ�ռ����Դ������
			//releaseLastRow(scan, true); 
			scan->m_session->unlockRow(&scan->m_rlh);
			try {
				trx->lockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id);
			} catch(NtseException &e) {
				throw e;
			}
		
			// �˴�lockRow֮�󣬲�������unlockRow�����������̱߳����������
			// TODO��ȥ��unlockRow�����ܻ�������������⣬�����������Ҫ���
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
			// 1. �ɼ��汾 or ���°汾���ڴ��У������Ѿ����أ���Ҫ���Lob�ֶ�
			// 2. �ɼ��汾 or ���°汾��NTSE��
			scan->m_lastRecPlace = tntVisible ? LAST_REC_IN_FULLROW : LAST_REC_IN_REDROW;
			fillMysqlRow(scan, mysqlRow);
		} else {
			// �޿ɼ��汾����ʱ��ҪNTSEȡ��һ��

			// �ڶ�ȡ��һ��ǰ����Ҫ�ͷű����ϵ�������
			if (lockMode != TL_NO) {
				trx->unlockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id);
			}
			scan->m_session->unlockRow(&scan->m_rlh);
			goto _ReStart1;
		}

		// TNT �������������ɹ� ���� TNT ���ն�����Ҫ��TNT����������ͷ�NTSE Lock
		assert(scan->m_rlh != NULL);
		scan->m_session->unlockRow(&scan->m_rlh);
	} else if (scan->m_scanType == ST_IDX_SCAN) {
		// ����������ɨ�裬��ȡ������¼��Ȼ��ͨ���ڴ�ѣ��жϿɼ���
		// �����Ƿ�Ϊcoverage scan��index��ȡ����������¼������m_idxKey->m_data��
		// m_mysqlRow��¼Ϊ�����ļ�¼
		// �����coverage scan, m_redRow����ֱ�ӷ����ϲ㣬������ǰ׺�������������������ɨ������Ծ�����
		scan->m_mysqlRow->m_data = mysqlRow;
		if (scan->m_coverageIndex)
			scan->m_redRow->m_data = mysqlRow;

		// ����range scan��unique scan����ͳһ����range scan����
		exist = scan->m_index->getNext(scan->m_indexScan);
		
		if (exist)
		{
			bool isSnapshotRead = scan->isSnapshotRead();
			const KeyMVInfo &mvInfo	= scan->m_indexScan->getMVInfo();
			bool ntseReturned	= mvInfo.m_ntseReturned;
			bool isDeleted		= mvInfo.m_delBit == 1 ? true : false;

			scan->m_rowId = scan->m_idxKey->m_rowId;

			bool isRecShouldRet = readRecByRowId(scan, scan->m_rowId, &ntseReturned, isDeleted, mvInfo.m_visable, mvInfo.m_version, isSnapshotRead); 
			// ��ǰ��¼������ɼ����жϣ�������ȡ������һ����¼
			if (!isRecShouldRet) {
				// �ڶ�ȡ��һ��ǰ����Ҫ�ͷű����ϵ�������
				TNTTransaction *trx = scan->getSession()->getTrans();
				TLockMode lockMode = scan->getRowLockMode();
				if (lockMode != TL_NO)
					trx->unlockRow(lockMode, scan->m_rowId, m_tab->getTableDef()->m_id);
				scan->m_indexScan->unlatchNtseRowBoth();
				goto _ReStart1;
			}

			fillMysqlRow(scan, mysqlRow);
			scan->m_indexScan->unlatchNtseRowBoth();
			// ��ΪΨһ��ɨ�裬ֹͣ�ײ�����scan�����ص�ǰ��¼
			if (scan->m_singleFetch) 
				stopUnderlyingScan(scan);
		} else {
			scan->m_indexScan->unlatchNtseRowBoth();
		}
	} else {
		// ������RowIdɨ�裬��ȡ�ڴ�ѣ��жϿɼ��ԣ�������Ҫ���������ѣ�ȡ��¼
		//		 RowidΪ���ô����rid����
		assert(scan->m_scanType == ST_POS_SCAN);
		bool tntVisible = false, ntseVisable = false;
		RowLockHandle *rlh = NULL;

		scan->m_rowId	= rid;
		scan->m_mysqlRow->m_data = mysqlRow;

		assert(!scan->m_session->isRowLocked(m_tab->getTableDef()->m_id, rid, Shared));
		// ��ȡ��¼ʱ�����뱣֤NTSE���Ѿ�����
		rlh = scan->m_session->lockRow(m_tab->getTableDef()->m_id, rid, Shared, __FILE__, __LINE__);

		tntVisible = m_mrecs->getRecord(scan, scan->m_version, &ntseVisable);
		scan->m_lastRecPlace = LAST_REC_IN_FULLROW;
		if (tntVisible || ntseVisable) {
			exist = true;
			// Position scan��ָ��rowid����ɨ�裬���ܴ����Ҳ�����¼�����(tnt,ntse�������ڴ˼�¼)
			if (!tntVisible && ntseVisable) {
				exist = readNtseRec(scan);
			}
			if (exist)
				fillMysqlRow(scan, mysqlRow);
		} else {
			// �޿ɼ��汾
			exist = false;
		}

		// �ڶ�ȡ��¼֮���ͷ�NTSE Row Lock
		scan->m_session->unlockRow(&rlh);
	}

	// ��mysqlrow��ʽת�����ϲ�ĸ�ʽ
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

	// NTSE�ڴ˴��ͷ�����������ͳ����Ϣ������TNT��ʱ����Ҫ
	scan->m_session->incOpStat(OPS_ROW_READ);
	scan->m_session->incOpStat(scan->m_scanRowsType);
	return true;
}

/**	�жϵ�ǰ������ɨ�裬�Ƿ�Ϊ����������ɨ��
*	@table	����
*	@index	��������
*	@cols	��ȡ��cols
*	@return ������ɨ�裬����true�����򷵻�false
*/
bool TNTTable::isCoverageIndex(const TableDef *table, const IndexDef *index, const ColList &cols) {
	byte *mask = (byte *)alloca(sizeof(byte) * table->m_numCols);
	memset(mask, 0, sizeof(byte) * table->m_numCols);

	for (u16 i = 0; i < index->m_numCols; i++) {
		if (index->m_prefixLens[i])
			mask[index->m_columns[i]] = 2;	// ǰ׺������
		else
			mask[index->m_columns[i]] = 1;	// ��ͨ������
	}
	for (u16 i = 0; i < cols.m_size; i++)
		if (mask[cols.m_cols[i]] != 1) // �����ȡǰ׺�У����������ֶΣ�������еȣ���������������
			return false;			   // ע�� �����ֶε��ٽ�ֵһ������768�� ���һ����ǰ׺��
	return true;
}

/**	��ǰ��¼��ȡ��ϣ������ϲ�֮ǰ������¼��䵽mysqlRow��
*	@scan			TNTɨ����
*	@mysqlRow		�ϲ�ָ������䷵�ؼ�¼�Ŀռ�
*/
void TNTTable::fillMysqlRow(TNTTblScan *scan, byte *mysqlRow) {
	UNREFERENCED_PARAMETER(mysqlRow);

	// ����scan->m_scanType���ж���ν���¼copy��mysqlָ���Ŀռ�֮��
	if (scan->m_scanType == ST_TBL_SCAN) {
		if (scan->m_lastRecPlace == LAST_REC_IN_REDROW) {
			// ��ǰ��¼�洢��scan->m_redRow֮��
			// 1. ����ȡ�����ʱ��m_redRow->m_data = mysqlRow��
			// 2. ��ȡ�����ʱ��m_redRow->m_data = Records::Scan�ڲ�����ռ�
			if (scan->m_readLob) {
				memcpy(scan->m_mysqlRow->m_data, scan->m_redRow->m_data, scan->m_tableDef->m_maxRecSize);
				scan->readLobs();
			}
		}
		else {
			assert(scan->m_lastRecPlace == LAST_REC_IN_FULLROW);
			// ��ǰ��¼�洢��scan->m_fullRow֮��
			// ��Ҫ��m_fullRow����ȡ��mysqlRow
			// ���Է��֣�redundant��ʽ�£�Record��SubRecordӵ��ͬ���Ķ��壬ֱ��copy data����
			memcpy(scan->m_mysqlRow->m_data, scan->m_fullRow->m_data, scan->m_tableDef->m_maxRecSize);
			if (scan->m_readLob)
				scan->readLobs();
		}
	}
	else if (scan->m_scanType == ST_IDX_SCAN) {
		// 2. Index scan�������Ƿ�Ϊcoverage index scan�����ж����copy
		if (scan->m_coverageIndex) {
			// ֱ�Ӵ�scan->m_idxKey����ȡ��¼
			// ����Ĭ�ϸ���ɨ�費����ڴ�������mysqlRow �� REDUNDANT ��ʽ��¼��ͬ
			RecordOper::extractSubRecordPRNoLobColumn(scan->m_tableDef, scan->m_indexDef, scan->m_idxKey, scan->m_redRow);
		}
		else {
			assert(scan->m_lastRecPlace == LAST_REC_IN_FULLROW);
			// ��scan->m_fullRec��copy��¼
			memcpy(scan->m_mysqlRow->m_data, scan->m_fullRow->m_data, scan->m_tableDef->m_maxRecSize);
			if (scan->m_readLob)
				scan->readLobs();
		}
	}
	else {
		// �˴���position scan��table range scan�ֿ�����Ϊ���Ժ�position scan���Ż�����
		assert(scan->m_scanType == ST_POS_SCAN);
		assert(scan->m_lastRecPlace == LAST_REC_IN_FULLROW);
		memcpy(scan->m_mysqlRow->m_data, scan->m_fullRow->m_data, scan->m_tableDef->m_maxRecSize);
		if (scan->m_readLob)
			scan->readLobs();
	}
	
	// ����ɨ��������ǰ�����е�rowid
	scan->m_mysqlRow->m_rowId = scan->m_rowId;
	scan->m_fullRow->m_rowId = scan->m_rowId;
	scan->m_fullRec->m_rowId = scan->m_rowId;
}

/**	�ж�����scan���ص���Ƿ�ɼ�(ͬʱ����snapshot read��current read)
*
*	@scan			TNTɨ����
*	@rid			����ɨ�践�ص�RowId
*	@ntseReturned	����/���.���룺��ǰ��Ƿ��NTSE ����ɨ�����з��أ��������ǰ��Ƿ��NTSE Heap�ж�ȡ
*	@isDeleted		��ǰ������Ƿ�����Delete_Bit
*	@isVisible		�ڴ�����ҳ�����Ƿ�ɼ�
*	@rowVersion		�ڴ��������Ӧ��Row Version
*	@return			��ǰ�������ص���Ƿ���Է��ظ�mysql��
*/
bool TNTTable::readRecByRowId(TNTTblScan *scan, RowId rid, bool* ntseReturned, bool isDeleted, bool isVisible, u16 rowVersion, bool isSnapshotRead) {
	UNREFERENCED_PARAMETER(rid);
	UNREFERENCED_PARAMETER(isSnapshotRead);
	UNREFERENCED_PARAMETER(isVisible);
	UNREFERENCED_PARAMETER(isDeleted);

	// 1. ��Ϊ��������ɨ��
	// ��������ɨ�裬����֧�Ż�
	if (scan->m_coverageIndex) {
		// 1.1 �ڴ��������أ�ͬʱͨ��ҳ��DB_MAX_ID�����жϿɼ��ԣ�ֱ�ӷ��ؼ���
		/*if (!*ntseReturned && isVisible) {
			if (isDeleted) 
				return false;
			else {
				scan->m_lastRecPlace = LAST_REC_IN_IDXROW;
				return true;
			}
		}*/

		// 1.2 ��RowId hash���жϿɼ���
		bool tntVisible = false, ntseVisible = false;

		tntVisible = m_mrecs->getRecord(scan, rowVersion, &ntseVisible);
		scan->m_lastRecPlace = LAST_REC_IN_FULLROW;

		// ���������ɼ���ֱ�ӷ��ز��ɼ�
		if (!tntVisible && !ntseVisible) {
			// �ض�Ϊsnapshot read��current read��һ�����ҵ�������
			// assert(isSnapshotRead);
			return false;
		}

		// 1.3 ����������أ�ͬʱ����RowId Hash�жϣ�ntse�ɼ�(��ʱtnt�ز��ɼ�)��ֱ�ӷ��ؼ���
		if (*ntseReturned && ntseVisible) {
			scan->m_lastRecPlace = LAST_REC_IN_IDXROW;
			return true;
		}
		
		// 1.4 �����������������Ҫ����Double Check
		if (ntseVisible) {
			// ��ȡntse���¼
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
	// 2. ����������ɨ��
	else {
		// 2.1 ����������ɨ�裬TNT��һ���в����Ż�
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

/**	����rowid����ȡ��rowid��Ӧ��ntse��¼ȫ��
*	@scan	TNTɨ����
*	@return	����ָ����Rowid��¼�Ƿ����
*/
bool TNTTable::readNtseRec(TNTTblScan *scan) {
	bool exist;
	// ��֤���ô˷�ʽ�Ѽ���Ntse S ����
	assert(scan->m_session->isRowLocked(m_tab->getTableDef()->m_id, scan->m_rowId, Shared));
	Records::BulkFetch *fetch = (Records::BulkFetch *)(scan->m_recInfo);

	// ��NTSE Test�����У�Unique Scan�ҵ�һ��֮��ֱ�ӽ�����BulkFetch������ִ��ʧ��
	// 1. ���²�����һ����ȡ������¼�����ͨ��MySQL���ã���ʱ�����ڴ����
	// 2. ֻ����������ȡ������NTSE��¼����Double Check��������������������ָĽ���ֻ��ȡIndex��+Read Columns

	// ע1�����˺�����getNext���ã���ôfetchһ�����ڣ���˶�ȡ�ļ�¼�����ݱ��Ƿ���MMS�����ܻ����MMS��
	// ע2�����˺�����update/delete���ã���ôfetch���ܲ����ڣ���else·������ʱ��¼�������MMS�����ǻ��ж�MMS���Ƿ����
	// ע3��Index/Position Scan��fetch��ȡ��һ����ȫ�Table Scan��fetch��ȡ����readCols��

	if (fetch != NULL)
		exist = fetch->getFullRecbyRowId(scan->m_rowId, scan->m_fullRow, scan->m_subExtractor);
	else
		exist = m_tab->getRecords()->getSubRecord(scan->m_session, scan->m_rowId, scan->m_fullRow, scan->m_subExtractor);

	if (!exist) {
		// ָ��rowid��scan�����ܴ����Ҳ�����¼�����
		// assert(scan->m_scanType == ST_POS_SCAN);
		return exist;
	}

	scan->m_fullRow->m_rowId = scan->m_rowId;
	scan->m_fullRec->m_rowId = scan->m_rowId;
	assert(exist);
	scan->m_fullRowFetched = true;

	return exist;
}

/**��һ�׶�purge
 * @param session purge�Ự
 * @param trx purge����
 */
void TNTTable::purgePhase1(Session *session, TNTTransaction *trx) {
	MemoryContext *ctx = session->getMemoryContext();
	TableDef *tblDef = m_tab->getTableDef();
	u16 *columns = (u16 *)ctx->alloc(sizeof(u16) * tblDef->m_numCols); //��¼��TableDef����
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
		//���TNT��NTSE�����ɼ����ǻع������ļ�¼ΪNULL����Ҫ����
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

		//��rec��¼����ntse��,���ڴ������ֱ�Ӳ���ntse�Ĵ�����ļ��У�����ԭ�еİ�REC_MYSQL��ʽ�ļ�¼����ntse�Ľӿ��Ѿ���������(ԭ�нӿڻᵼ���ٲ�һ�δ����)
		//��������¼��ʽΪ�������߱䳤���������Ĳ���Ϊ�����ʽ�����Ծ����������ʽ�������ݱȽϺ�
		if (heapRec->m_del == FLAG_MHEAPREC_DEL) {
			// ��ɾ����¼,��ɾ�������	
			m_tab->deleteCurrent(posScan);

			if(tblDef->m_hasLob){
				SubRecord *ntseSub = posScan->getRedRow();
				Record ntseRec(ntseSub->m_rowId, REC_REDUNDANT, ntseSub->m_data, ntseSub->m_size);
				u16 *updateCols = NULL;
				// �˴�ֻ��Ҫ�ȶԴ�����м��ɣ��������͵���û�бȽ�����
				u16 updateColNum = RecordOper::extractDiffColumns(tblDef, &ntseRec, &(heapRec->m_rec), &updateCols, ctx, true);

				// ɾ��NTSE����¼�ϱ����¹��Ĵ����
				for (u16 i = 0; i < updateColNum; i++) {
					ColumnDef *colDef = tblDef->m_columns[updateCols[i]];
					assert(colDef->isLob());
					bool ntseIsNull = RecordOper::isNullR(tblDef, &ntseRec, updateCols[i]);
					if(ntseIsNull)
						continue;
					LobId ntseLobId = RecordOper::readLobId(ntseRec.m_data,colDef);
					if(ntseLobId != INVALID_LOB_ID){
						if (m_db->getStat() == DB_RECOVERING){
							//�ָ�ʱҪ��ֹ��lobId���õ��µ���ɾ������
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

				//ɾ��TNTIM��¼�еĴ����
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
							//�ָ�ʱҪ��ֹ��lobId���õ��µ���ɾ������
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
				//����ֻ���¼�¼�������´����
				//tntTable��updateCurrent�ܽ������Ϲ����update�����������Դ�ʱ������Ϊpurge update�ض���throw exception
				try {
					// ������µĺ���ض��������ĸ�ʽ���������ϲ�ĸ�ʽ
					NTSE_ASSERT(m_tab->updateCurrent(posScan, heapRec->m_rec.m_data, true));
				} catch (NtseException &e) {
					m_db->getTNTSyslog()->log(EL_ERROR, "purge updateCurrent error %s", e.getMessage());
					assert(false);
				}

				// ɾ������б����µĴ����
				if (tblDef->hasLob()) {
					for (u16 i = 0; i < updateColNum; i++) {
						ColumnDef *colDef = tblDef->m_columns[updateCols[i]];
						if (!colDef->isLob()) 
							continue;

						// ��Ҫɾ��������еĴ����
						bool tntIsNull = RecordOper::isNullR(tblDef, &ntseRec, updateCols[i]);
						if(tntIsNull)
							continue;
						LobId tntLobId = RecordOper::readLobId(ntseRec.m_data,colDef);
						if(tntLobId != INVALID_LOB_ID){
							if (m_db->getStat() == DB_RECOVERING){
								//�ָ�ʱҪ��ֹ��lobId���õ��µ���ɾ������
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

/**�ڶ��׶�purge
 * @param session purge�Ự
 * @param trx purge����
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

/** �����ڴ�����ҳ��
 * @param session �ڴ��������ջỰ
 */
void TNTTable::reclaimMemIndex(Session *session) {
	uint indexNum = m_indice->getIndexNum();
	for(u8 i = 0; i < indexNum; i++) {
		 m_indice->getTntIndex(i)->reclaimIndex(session, m_db->getTNTConfig()->m_reclaimMemIndexHwm, m_db->getTNTConfig()->m_reclaimMemIndexLwm);
	}
}

/**	����rowid���Աȴ�rowid�Ŀɼ��汾�����������ؼ�¼���Ƿ���ȫһ��
*	@scan	TNTɨ����
*	@return	��¼��ȫƥ��(����rowid)������true�����򷵻�false
*
*/
bool TNTTable::doubleCheckRec(TNTTblScan *scan) {
	return scan->doubleCheckRecord();
}

/**	�ͷ���һ��������ռ�õ���Դ
*	@scan		TNT ɨ����
*	@retainLobs	�Ƿ���Lob�ռ�
*
*/
void TNTTable::releaseLastRow(TNTTblScan *scan, bool retainLobs) {
	scan->releaseLastRow(retainLobs);
}

/**	���µ�ǰɨ��ļ�¼
*	@pre	��¼����X������
*	@post	�Ծɱ������������ύ/�ع�ʱ�ͷ�
*
*	@scan		ɨ����
*	@update		Ҫ���µ����Լ���ֵ��ʹ��REC_MYSQL��ʽ
*	@oldRow		����ΪNULL����ָ����¼��ǰ��ΪREC_MYSQL��ʽ
*	@dupIndex	OUT������Ψһ��������ͻʱ���ص��³�ͻ�������š���ΪNULL�򲻸���
*	@fastCheck	�Ƿ����fast�ķ�ʽcheck duplicate violation��Ĭ��Ϊtrue.
*	@throw		NtseExcpetion	��¼���������Բ����
*/
bool TNTTable::updateCurrent(TNTTblScan *scan, const byte *update, const byte *oldRow, uint *dupIndex, bool fastCheck) throw(NtseException) {
	UNREFERENCED_PARAMETER(oldRow);
	bool tntFirstRound  = false;
	u16	 updateIndexNum = 0;

	if (scan->m_tableDef->m_indexOnly && scan->m_readCols.m_size < scan->m_tableDef->m_numCols)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "All columns must been read before update index only tables.");

	Session *session = scan->m_session;

	McSavepoint savepoint(scan->m_session->getMemoryContext());

	// 1. �жϵ�ǰ���¼�¼��λ�ã�TNTIM or NTSE
	// ����ǰ���¼�¼��TNT�У���prepareUD����ȡ����¼ȫ��
	tntFirstRound = m_mrecs->prepareUD(scan);
	if (tntFirstRound) {
		// ���¼�¼��NTSE�У���ȡ��¼ȫ��
		if (!scan->m_fullRowFetched) {
			scan->m_rlh = LOCK_ROW(session, m_tab->getTableDef()->m_id, scan->m_rowId, Shared);
			readNtseRec(scan);
			session->unlockRow(&scan->m_rlh);
		}
	} 
	else {
		// ����TNTIM�У�prepareUD�����Ѿ����ȫ��Ķ�ȡ����
	}

	try {
		// ������г����䳤�ֶΣ���Ҫ��mysqlRow��ʽ����ת��
		// update����֮ǰ��scan��ȡ����ȫ������ϲ�Ϊǰ���������ϲ������������ĺ�����ȫ��
		if (m_tab->getTableDef()->hasLongVar()) {
			Record mysqlUpdateRec(INVALID_ROW_ID, REC_UPPMYSQL, (byte *)update, m_tab->getTableDef()->m_maxMysqlRecSize);
			byte *realUpdate = (byte *)session->getMemoryContext()->alloc(m_tab->getTableDef()->m_maxRecSize);
			Record realUpdateMysqlRec(INVALID_ROW_ID, REC_MYSQL, realUpdate, m_tab->getTableDef()->m_maxRecSize);
			RecordOper::convertRecordMUpToEngine(m_tab->getTableDef(), &mysqlUpdateRec, &realUpdateMysqlRec);	
			mysqlUpdateRec.m_data = realUpdateMysqlRec.m_data;
			mysqlUpdateRec.m_size = realUpdateMysqlRec.m_size;
			update = realUpdate;
		}

		// 2. �ж��Ƿ����Ψһ�Գ�ͻ
		scan->m_updateMysql->m_rowId = scan->m_updateRed->m_rowId = scan->m_rowId;
		scan->m_updateMysql->m_data = (byte *)update;
		
		//��������һ��checkDuplicateʱ���޸Ĵ����subRecord���������д����ı�����Ҫ�ȸ���һ�ݺ���
		if(!scan->m_tableDef->hasLob())
			scan->m_updateRed->m_data = (byte *)update;
		else 
			memcpy(scan->m_updateRed->m_data, update, scan->m_tableDef->m_maxRecSize);
		
		if (checkDuplicate(session, scan->m_tableDef, scan->m_rowId, scan->m_fullRow, scan->m_updateRed, dupIndex, &updateIndexNum, fastCheck)) {
			// Ψһ�Գ�ͻ��������
			session->unlockAllUniqueKey();
			releaseLastRow(scan, true);
			return false;
		}

		// Ψһ�Բ���ͻ����ʱ�Ծɳ���Ψһ��ֵ�������Ը��£���֤�����ͻ
		if (scan->m_lobUpdated)	{
			// ���´������Ҫ������ɴ����ĸ���	
			for(u16 colNum = 0; colNum < scan->m_updateRed->m_numCols; colNum++){
				ColumnDef *columnDef =m_tab->getTableDef()->m_columns[scan->m_updateRed->m_columns[colNum]];
				if(!columnDef->isLob())
					continue;
				bool oldIsNull = RecordOper::isNullR(m_tab->getTableDef(),scan->m_fullRec,scan->m_updateRed->m_columns[colNum]);
				bool newIsNull = RecordOper::isNullR(m_tab->getTableDef(),scan->m_updateMysql,scan->m_updateRed->m_columns[colNum]);
				//���ǰ��汾��ΪNULL�����κθ���
				if(oldIsNull && newIsNull)
					continue;

				//��NTSE���²���һ�������
				byte * newLob = NULL;
				if (!newIsNull){
					newLob = RecordOper::readLob(scan->m_updateMysql->m_data,columnDef);
					if(!newLob)	//�Է�NULL���Ǵ�СΪ0�Ĵ�������NULLָ��
						newLob = (byte*)session->getMemoryContext()->alloc(1);
				}
				//��ʼ�µĴ�������
				LobId newLobId = INVALID_LOB_ID;
				//������Ǹ���ΪNULL,��ô�²���һ�������
				if(!newIsNull){
					uint newLobSize = RecordOper::readLobSize(scan->m_updateMysql->m_data,columnDef);
					session->startTransaction(TXN_LOB_INSERT,m_tab->getTableDef()->m_id);
					newLobId = m_tab->getLobStorage()->insert(session, newLob, newLobSize, m_tab->getTableDef()->m_compressLobs);
					session->endTransaction(true);
				}

				//�����LOB�ֶθ���ΪNULL�����InvalidLobId�����º�İ汾

				//ƴװLOB����
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
	
	// ��Ҫ��������
	RowIdVersion version = scan->getVersion();
	m_indice->updateIndexEntries(scan->m_session, scan->m_fullRow, scan->m_updateRed, tntFirstRound, version);
	

	releaseLastRow(scan, true);
	session->incOpStat(OPS_ROW_UPDATE);
	session->unlockAllUniqueKey();

	return true;
}

/**	ɾ����ǰɨ��ļ�¼
*	@pre	��¼��X������
*	@post	���������ȴ������ύor�ع�
*
*	@scan		ɨ����
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
		// ��һ��delete����ȡȫ�����Tnt
		if (!scan->m_fullRowFetched) {
			scan->m_rlh = LOCK_ROW(scan->m_session, tableDef->m_id, scan->m_rowId, Shared);
			readNtseRec(scan);
			scan->m_session->unlockRow(&scan->m_rlh);
		}
	}
	else {
		// Todo��д��־��Ҫ��page latch����
		// ��˽�д��־�����½���MRecordsģ�����
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

/**	ֹͣ�ײ��ɨ��
*	@scan	TNTɨ����
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

/**	����һ��ɨ�衣����ɨ��֮ǰ����ǰ��ռ�õ���Դ���ͷ�
*	@post	�����������������Ҫ�ȴ������ύor�ع���Ȼ���ͷ�
*
*	@scan	ɨ�����������ͷ�
*/
void TNTTable::endScan(TNTTblScan *scan) {
	releaseLastRow(scan, false);
	stopUnderlyingScan(scan);
}


/**	����һ����¼
*	
*	@session	session����
*	@record		Ҫ����ļ�¼��ʹ��REC_MYSQL��ʽ
*	@dupIndex	������������³�ͻ���������
*	@opInfo		������Ϣ
*	@fastCheck	�Ƿ����fast�ķ�ʽcheck duplicate violation��Ĭ��Ϊtrue.
*	@throw		NtseException
*	@return		�ɹ����ؼ�¼RID
*/
RowId TNTTable::insert(Session *session, const byte *record, uint *dupIndex, TNTOpInfo *opInfo, bool fastCheck) throw(NtseException) {
	RowLockHandle *rlh = NULL;
	// ���ݴ����opInfo���жϵ�ǰ�Ƿ�Ϊ����һ��ִ�У����ӱ�������
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

	// ������г����䳤�ֶΣ���Ҫ��mysqlRow��ʽ����ת��
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

/**	Update��Insert���������unique��ͻ(���ٷ�ʽ�����ر��ж�)
*	@session	Session
*	@m_tableDef	����
*	@rowId		ǰ���¼��RowId
*	@before		update������ǰ����
*	@after		update������Ҫ���µ���ֵ
*	@dupIndex	OUT������ͻ�����س�ͻ��������	
*	@updateIndexNum	OUT����Ҫ���µ�������
*	@fastCheck	�Ƿ����fast�ķ�ʽcheck duplicate violation��Ĭ��Ϊtrue.
*	@return		Ψһ�Գ�ͻ������true������ͻ����false
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

/**	Update��Insert���������unique��ͻ(���ٷ�ʽ�����ر��ж�)
*	@session	Session
*	@m_tableDef	����
*	@before		update������ǰ����
*	@after		update������Ҫ���µ���ֵ
*	@dupIndex	OUT������ͻ�����س�ͻ��������	
*	@updateIndexNum	OUT����Ҫ���µ�������
*	@return		Ψһ�Գ�ͻ������true������ͻ����false
*/
/*
bool TNTTable::checkDuplicateFast(Session *session, TableDef *m_tableDef, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw(NtseException) {
	u16			*updateIndexNoArray;
	u16			updateUniques	= 0, uniqueNo = 0;
	DrsIndice	*drsIndice		= m_indice->getDrsIndice();

	drsIndice->getUpdateIndices(session->getMemoryContext(), after, updateIndexNum, &updateIndexNoArray, 
		&updateUniques);

	// ����after��record��ʽ
	if (before != NULL) {
		RecordOper::mergeSubRecordRR(m_tableDef, after, before);
		after->m_rowId = before->m_rowId;
	}

	// ������unique�ֶΣ�ֱ�ӷ��ؼ��ɣ�����Ҫ���²���
	if (updateUniques == 0) 
		return false;

	// ��updateIndex�����й����unique index����
	u16 *updateUniqueNos = (u16 *) session->getMemoryContext()->alloc(sizeof(u16) * updateUniques);
	for (uint i = 0; i < *updateIndexNum; i++){
		if (m_tableDef->m_indice[updateIndexNoArray[i]]->m_unique)
			updateUniqueNos[uniqueNo++] = updateIndexNoArray[i];
	}
	assert(uniqueNo == updateUniques);

	Record recAfter(after->m_rowId, REC_REDUNDANT, after->m_data, after->m_size);

	if (!m_indice->lockUpdateUniqueKey(session, &recAfter, updateUniques, updateUniqueNos, dupIndex)) {
		// ��Ψһ����ʧ�ܣ����س�ͻ
		return true;
	}

	//���Ψһ��������ͻ
	if (m_indice->checkDuplicate(session, &recAfter, updateUniques, updateUniqueNos, dupIndex, NULL)) {
		session->unlockAllUniqueKey();
		return true;
	}

	// Ψһ���������ͨ������ʱ����������������ɹ�֮�󣬲��ܷ���
	return false;
}
*/

/**	Update��Insert���������unique��ͻ(���ٷ�ʽ����ǰ�����ر��жϣ������insert on duplicate��replace into�﷨)
*	@session	Session
*	@m_tableDef	����
*	@rowId		ǰ���¼��RowId
*	@before		update������ǰ����
*	@after		update������Ҫ���µ���ֵ
*	@dupIndex	OUT������ͻ�����س�ͻ��������	
*	@updateIndexNum	OUT����Ҫ���µ�������
*	@return		Ψһ�Գ�ͻ������true������ͻ����false
*/
bool TNTTable::checkDuplicateSlow(Session *session, TableDef *m_tableDef, RowId rowId, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw(NtseException) {
	u16				*updateIndexNoArray;
	u16				updateUniques	= 0, uniqueNo = 0;
	MemoryContext	*mtx			= session->getMemoryContext();
	DrsIndice		*drsIndice		= m_indice->getDrsIndice();
	byte			*mysqlRow		= (byte *)mtx->alloc(m_tableDef->m_maxRecSize);

	drsIndice->getUpdateIndices(session->getMemoryContext(), after, updateIndexNum, &updateIndexNoArray, 
		&updateUniques);

	// ����after��record��ʽ
	if (before != NULL) {
		RecordOper::mergeSubRecordRR(m_tableDef, after, before);
		after->m_rowId = before->m_rowId;

		// �����º�ĳ���
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

	// ������unique�ֶΣ�ֱ�ӷ��ؼ��ɣ�����Ҫ���²���
	if (updateUniques == 0) 
		return false;

	// ��updateIndex�����й����unique index����
	u16 *updateUniqueNos = (u16 *) mtx->alloc(sizeof(u16) * updateUniques);
	for (uint i = 0; i < *updateIndexNum; i++){
		if (m_tableDef->m_indice[updateIndexNoArray[i]]->m_unique)
			updateUniqueNos[uniqueNo++] = updateIndexNoArray[i];
	}
	assert(uniqueNo == updateUniques);

	// ��after��Ӧ��rowid����ΪINVALID_ROW_ID����֤�ײ������ǵ�һ��scan
	RowId afterRid = INVALID_ROW_ID;

	Record recAfter(afterRid, REC_REDUNDANT, after->m_data, after->m_size);

	if (!m_indice->lockUpdateUniqueKey(session, &recAfter, updateUniques, updateUniqueNos, dupIndex)) {
	
		// ��Ψһ����ʧ�ܣ����س�ͻ
		return true;
	}

	// �������ϵ�����Ψһ�����������
	//����ڴ������Ƿ�����ظ���ֵ
	for (uint i = 0; i < updateUniques; i++) {
		u16 idxNo		= updateIndexNoArray[i];
		IndexDef *index	= m_tableDef->m_indice[idxNo];
		SubRecord *key	= (SubRecord *)mtx->alloc(sizeof(SubRecord));
		byte *keyDat	= (byte *)mtx->alloc(index->m_maxKeySize);

		new (key)SubRecord(KEY_PAD, index->m_numCols, index->m_columns, keyDat, index->m_maxKeySize);

		// ��ȡ��������ݣ�ƴװ��������
		Array<LobPair*> lobArray;
		if (index->hasLob()) {
			RecordOper::extractLobFromM(session, m_tableDef, index, &recAfter, &lobArray);
		}

		bool isNullIncluded = RecordOper::extractKeyRPWithRet(m_tableDef,index, &recAfter, &lobArray, key);

		// �����������������ֵ�а���NULL�У���ô����Ҫ����uniqueԼ�����
		if (isNullIncluded)
			continue;

		// ֻ��ȡ��ѯ��Ҫ�����ԣ�������Index Coverage Scan
		IndexScanCond cond((u16 )idxNo, key, true, true, index == m_tableDef->m_pkey);
		TNTOpInfo opInfo;
		opInfo.m_selLockType = TL_X;
		opInfo.m_sqlStatStart= false;
		opInfo.m_mysqlOper	 = false;

		// �������ļ�ֵ˳��ת��Ϊ���еļ�ֵ˳��
		// ����������(key2, key1)����index�����ֵ˳���뽨��˳���෴������ת��
		// ����ײ����ȡ���ᱨ��
		u16 *cols = (u16 *)mtx->alloc(sizeof(u16) * index->m_numCols);
		transIndexCols(m_tableDef->m_numCols, index, cols);

		TNTTblScan *scan = indexScan(session, OP_WRITE, &opInfo, &cond, index->m_numCols, cols, true);
		try {
			if (getNext(scan, mysqlRow)) {
				// ��ǰ�����ҵ�Ψһ���ʱ��ͻ�����Ѿ�������
				if(scan->m_rowId == rowId) {
					// ����ҵ���Ψһ��RowId�ͣ����£�ǰ���RowId��ͬ
					// ����Ϊ��Ψһ�Գ�ͻ����JIRA��NTSETNT-308
					endScan(scan);
					return false;
				} else {
					session->unlockAllUniqueKey();
					endScan(scan);
					*dupIndex = idxNo;
					return true;
				}
			} else {
				// δ�ҵ���ͻ��ر�scan���
				endScan(scan);
			}
		} catch (NtseException &e) {
			endScan(scan);
			throw e;
		}
	}
	
	return false;
}

/** �����ڴ��������ָ���ڴ���������
 * @param session �Ự
 * @param indice ��Ҫ������ָ���ڴ���������
 */
void TNTTable::buildMemIndexs(Session *session, MIndice *indice) {
	//��ʼ���ڴ�����
	TableDef *tableDef = m_tab->getTableDef();
	uint idxNum = indice->getIndexNum();

	//�����ڴ�Ѷ�������
	MHeapRec *heapRec = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp1(ctx);
	MHeapScanContext *scanCtx = m_mrecs->beginScan(session);
	while(true) {
		//�˴���Ҫ��ȡ�������¼�¼
		if(!m_mrecs->getNext(scanCtx, NULL)) 
			break;
		McSavepoint msp2(ctx);
		heapRec = scanCtx->m_heapRec;
		void *buf = ctx->alloc(sizeof(SubRecord));
		SubRecord *SubRec = new (buf)SubRecord(REC_REDUNDANT, 0, NULL, heapRec->m_rec.m_data, tableDef->m_maxRecSize, heapRec->m_rec.m_rowId);
		RowIdVersion version = m_mrecs->getHashIndex()->get(heapRec->m_rec.m_rowId, ctx)->m_version;
		//������������
		for(u8 indexNo = 0; indexNo < idxNum; indexNo++) {
			McSavepoint lobSavepoint(session->getLobContext());
			//�����ڴ������ṹ
			MIndex *memIndex = indice->getIndex(indexNo);
			IndexDef *indexDef = tableDef->getIndexDef(indexNo);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, tableDef, indexDef, m_tab->getLobStorage(), SubRec, &lobArray);
			}
			// ����¼ת��ΪPAD��ʽ���������������������
			SubRecord *key = MIndexKeyOper::convertKeyRP(session->getMemoryContext(), SubRec, &lobArray, tableDef, indexDef);
			memIndex->insert(session, key, version);
		}				
	}		
}

/** ��Index���ж������飬ת��Ϊ���ս����˳������
* 
* @maxCols	�����е�����
* @index	IndexDef
* @cols		ת������ж�������
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

/** ִ��REPLACE��INSERT ... ON DUPLICATE KEY UPDATE���ʱ�����Ų����¼
* ע: ���������ܴ�session���ڴ�����������з���ռ䣬�����һ��session��
* ���ñ�������ο��ܵ����ڴ���������
*
* @session	�Ự����
* @record	Ҫ����ļ�¼��һ����MySQL��ʽ
* @opInfo	������Ϣ
* @throw NtseException �ӱ�����ʱ����¼����
* @return �ɹ�����NULL��ʧ�ܷ���IDU��������
*/
IUSequence<TNTTblScan *>* TNTTable::insertForDupUpdate(Session *session, const byte *record, TNTOpInfo *opInfo) throw(NtseException) {
	// ���ݴ����opInfo���жϵ�ǰ�Ƿ�Ϊ����һ��ִ�У����ӱ�������
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

		// ��ȡ������
		Array<LobPair*> lobArray;
		if (index->hasLob()) {
			RecordOper::extractLobFromM(session, m_tableDef, index, &rec, &lobArray);
		}
		RecordOper::extractKeyRP(m_tableDef, index, &rec, &lobArray, key);

		// TODO ֻ��ȡ��ѯ��Ҫ������
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
		// ����INSERT��ʱ��û�м������������Ҳ�����¼��
		// ��ʱ�����Բ���
		endScan(scan);
		session->getMemoryContext()->resetToSavepoint(mcSavepoint);
	}
}

/** ִ��REPLACE��INSERT ... ON DUPLICATE KEY UPDATE���ʱ��������ͻ����¼�¼
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE��������
 * @param update ������ͻ�ļ�¼��Ҫ�����³ɵ�ֵ��ΪREC_MYSQL��ʽ
 * @param numUpdateCols Ҫ���µ����Ը���
 * @param updateCols Ҫ���µ�����
 * @param dupIndex OUT��Ψһ�Գ�ͻʱ���ص��³�ͻ�������ţ���ΪNULL�򲻷���������
 * @throw NtseException ��¼����
 * @return ���³ɹ�����true������Ψһ��������ͻʧ�ܷ���false
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

/** ִ��REPLACE��INSERT ... ON DUPLICATE KEY UPDATE���ʱ��������ͻ��ɾ��ԭ��¼��
 * ��ָ�����Զ�����IDʱ�������һ����������updateDuplicate
 *
 * @param iuSeq INSERT ... ON DUPLICATE KEY UPDATE��������
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

/** ֱ���ͷ�INSERT ... ON DUPLICATE KEY UPDATE��������
 * @param iduSeq INSERT ... ON DUPLICATE KEY UPDATE��������
 */
void TNTTable::freeIUSequenceDirect(IUSequence<TNTTblScan *> *iuSeq) {
	
	endScan(iuSeq->m_scan);
	iuSeq->m_scan = NULL;
}

/**
* ����俪ʼʱ���Ա����
* @param	trx			��������
* @param	lockType	��ģʽ	
*/
void TNTTable::lockTableOnStatStart(TNTTransaction *trx, TLockMode lockType) throw(NtseException) {
	TableId tid = m_tab->getTableDef()->m_id;

	trx->lockTable(lockType, tid);
}

/**	��ȡ��ǰ������ֵ��ȡֵ
*	
*	@return	��ǰ������ֵ
*/
u64 TNTTable::getAutoinc() {
	assert(m_autoincMutex.isLocked());
	return m_autoinc;
}

/**	���ݴ����ֵ���޸�������ֵ
*	�������value > ��ǰ������ֵ�������
*	
*	@value	�������������ֵȡֵ
*/
void TNTTable::updateAutoincIfGreater(u64 value) {
	assert(m_autoincMutex.isLocked());
	if (value > m_autoinc)
		m_autoinc = value;
}

/**	��ʼ��������ֵ
*
*	@value	��ʼ��������ֵ��ȡֵ
*/
void TNTTable::initAutoinc(u64 value) {
	assert(m_autoincMutex.isLocked());
	m_autoinc = value;
}

// �������ƣ�mutex���Ż�·����lock������·������lockǰ������mutex
// lock����ʱ��enter mutex����Ҳ���ܳɹ�����Ҫ�ȴ�lock�ͷ�
// ͨ��m_reqAutoincLocks�жϵ�ǰ�Ƿ����grant or req��ʽ��lock

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

/** ����rowId��ȡntse��¼
 * @param session �Ự
 * @param rid ��¼rowId
 * return ntse��¼
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
	//��Ϊsession������������getNext�������ֶ�Ϊrowid
	if (m_tab->getNext(scan, rec->m_data, rid, false)) {
		rec->m_rowId = rid;
		//����getnext��ȫ����
		if (tableDef->hasLob()) {
			memcpy(rec->m_data, scan->getRedRow()->m_data, tableDef->m_maxRecSize);
		}
	}
	m_tab->endScan(scan);
	return rec;
}

/**����log redo �������
 * @param session �Ự
 * @param log ��־��¼
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

/** update��redo����
 * @param session �Ự
 * @param txnId ����id
 * @param rid ��¼id
 * @param rollbackId �ع���¼��rowid
 * @param tableIndex �汾�ر����
 * @param updateSub ������
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

/** remove��redo����
 * @param session �Ự
 * @param txnId ����id
 * @param rid ��¼id
 * @param rollbackId �ع���¼��rowid
 * @param tableIndex �汾�ر����
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

/**����log redo �״θ��²���
 * @param session �Ự
 * @param log ��־��¼
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

/**����log redo���״θ��²���
 * @param session �Ự
 * @param log ��־��¼
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

/**����log redo�״�ɾ������
 * @param session �Ự
 * @param log ��־��¼
 */
void TNTTable::redoFirRemove(Session *session, const LogEntry *log, RedoType redoType) {
	TrxId trxId;
	LsnType preLsn;
	RowId rid, rollBackId;
	u8 tableIndex;
	m_mrecs->parseFirRemoveLog(log, &trxId, &preLsn, &rid, &rollBackId, &tableIndex);
	redoRemove(session, trxId, rid, rollBackId, tableIndex, redoType);
}

/**����log redo���״�ɾ������
 * @param session �Ự
 * @param log ��־��¼
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
	//��NTSE��ɾ�������(�����crash�ָ���Ҫ��������InsertLob��ϣ��,�������ظ�ɾ��)
	if(m_db->getStat() != DB_RECOVERING) {
		m_tab->deleteLob(session, lobId);
	} else {
		if(session->getTrans()->getRollbackInsertLobHash()== NULL 
			|| session->getTrans()->getRollbackInsertLobHash()->get(&tblLob) == NULL)
			m_tab->deleteLobAllowDupDel(session, lobId);
	}
	return preLsn;
}

/** ������־��¼undo�������
 * @param session �Ự
 * @param log     ��־��¼
 * @param crash   ָ����crash recover��undo����log recover��undo
 * return ͬһ�����ǰһ����־��lsn
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
	//��ntse��ɾ����¼
	u16 *columns = (u16 *)ctx->alloc(tblDef->m_numCols*sizeof(u16));
	for (u16 i = 0; i < tblDef->m_numCols; i++) {
		columns[i] = i;
	}
	Record mysqlRec(INVALID_ROW_ID, REC_MYSQL, (byte *)ctx->alloc(tblDef->m_maxRecSize), tblDef->m_maxRecSize);
	TblScan *scan = m_tab->positionScan(session, OP_DELETE, tblDef->m_numCols, columns, true);
	if (m_tab->getNext(scan, mysqlRec.m_data, rid)) {
		//ժ���ڴ�������Ҫ��ntse���������²���
		if (!crash) {
			//����Ƿ�crash��recover��Ҫ��tntҲundo
			m_mrecs->undoInsert(session, rid);
		}
		m_tab->deleteCurrent(scan);
	} else if (!crash) {
		assert(m_mrecs->getHashIndex()->get(rid, ctx) == NULL);
	}
	m_tab->endScan(scan);
	return preLsn;
}

/** ������־��¼undo�״θ��²���
 * @param session �Ự
 * @param log     ��־��¼
 * @param crash   ָ����crash recover��undo����log recover��undo
 * return ͬһ�����ǰһ����־��lsn
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
	//��undo��������undo�Ѽ�¼
	MHeapRec *rollbackRec = m_mrecs->getBeforeAndAfterImageForRollback(session, rid, &beforeImg, afterImg);
	if (!crash) {
		//��������undo
		assert(beforeImg == NULL && afterImg != NULL);
		m_indice->undoFirstUpdateOrDeleteIndexEntries(session, afterImg);
	}
	m_mrecs->undoFirUpdate(session, rid, rollbackRec);
	return preLsn;
}

/** ������־��¼undo���״θ��²���
 * @param session �Ự
 * @param log     ��־��¼
 * @param crash   ָ����crash recover��undo����log recover��undo
 * return ͬһ�����ǰһ����־��lsn
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
	//��undo��������undo�Ѽ�¼
	MHeapRec *rollBackRec = m_mrecs->getBeforeAndAfterImageForRollback(session, rid, &beforeImg, afterImg);
	if (!crash) {
		assert(beforeImg != NULL && afterImg != NULL);
		//��������undo
		SubRecord beforeSub(beforeImg->m_format, updateRec->m_numCols, updateRec->m_columns, beforeImg->m_data, beforeImg->m_size, beforeImg->m_rowId);
		SubRecord afterSub(afterImg->m_format, updateRec->m_numCols, updateRec->m_columns, afterImg->m_data, afterImg->m_size, afterImg->m_rowId);
		m_indice->undoSecondUpdateIndexEntries(session, &beforeSub, &afterSub);
	}
	m_mrecs->undoSecUpdate(session, rid, rollBackRec);
	return preLsn;
}

/** ������־��¼undo�״�ɾ������
 * @param session �Ự
 * @param log     ��־��¼
 * @param crash   ָ����crash recover��undo����log recover��undo
 * return ͬһ�����ǰһ����־��lsn
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
		//��������undo
		assert(beforeImg == NULL && afterImg != NULL);
		m_indice->undoFirstUpdateOrDeleteIndexEntries(session, afterImg);
	}
	m_mrecs->undoFirRemove(session, rid, rollBackRec);
	return preLsn;
}

/** ������־��¼undo���״�ɾ������
 * @param session �Ự
 * @param log     ��־��¼
 * @param crash   ָ����crash recover��undo����log recover��undo
 * return ͬһ�����ǰһ����־��lsn
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
		//��������undo
		assert(beforeImg != NULL && afterImg != NULL);
		m_indice->undoSecondDeleteIndexEntries(session, afterImg);
	}
	m_mrecs->undoSecRemove(session, rid, rollBackRec);
	return preLsn;
}

/** �ӱ�Ԫ������
 * @pre ����δ�������������ӱ�Ԫ�����������ڼ�������֮ǰ
 *
 * @param session �Ự
 * @param mode ��ģʽ��ֻ����S��U��X
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ
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

/** ������Ԫ����������oldMode��newMode��Ȼ�oldMode�Ǳ�newMode���߼��������򲻽����κβ�����
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ��ʧ�ܣ�NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
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

/** ������Ԫ����������oldMode��newMode��ȣ���newMode��oldMode�߼��򲻽����κβ���
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
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

/** �ͷű�Ԫ������
 * @pre �������ͷű�������֮�����
 *
 * @param session �Ự
 * @param mode ��ģʽ
 */
void TNTTable::unlockMeta(Session *session, ILMode mode) {
	ftrace(ts.ddl || ts.dml, tout << this << session << mode);

	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	m_metaLock->unlock(session->getId(), mode);
	session->removeLock(m_metaLock);
}

/**	��TNTTable Instance�����ڵ�����£��ͷ�Ԫ������
*	@session	�Ự
*	@table		NTSE Table Instance
*	@mode		Ԫ������ģʽ
*/
void TNTTable::unlockMetaWithoutInst(Session *session, Table *table, ILMode mode) {
	IntentionLock *m_metaLock = table->getMetaLockInst();
	m_metaLock->unlock(session->getId(), mode);
	session->removeLock(m_metaLock);
}

/** �õ�ָ���ĻỰ�ӵı�Ԫ������ģʽ
 * @param session �Ự
 * @return �Ự���ӵı�Ԫ����������û�м����򷵻�IL_NO
 */
ILMode TNTTable::getMetaLock(Session *session) const {
	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	return m_metaLock->getLock(session->getId());
}

/** �õ���Ԫ������ʹ�����ͳ����Ϣ
 * @return ����ʹ�����ͳ����Ϣ
 */
/*
const ILUsage* TNTTable::getMetaLockUsage() const {
	IntentionLock *m_metaLock = m_tab->getMetaLockInst();
	return m_metaLock->getUsage();
}
*/

/** �õ�Ntse Table��Ӧ��Records����
*
* @return NTSE Records����
*/
Records* TNTTable::getRecords() const {
	return m_tab->getRecords();
}

u64 TNTTable::getMRecSize() {
	return m_tabBase->m_records->getHashIndex()->getSize();
}

}