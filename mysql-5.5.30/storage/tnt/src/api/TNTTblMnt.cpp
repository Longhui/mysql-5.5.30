#include "api/TNTTblMnt.h"

using namespace ntse;
using namespace std;

namespace tnt {
TNTTblMntAlterIndex::TNTTblMntAlterIndex(TNTTable *table,  const u16 numAddIdx, const IndexDef **addIndice, 
	const u16 numDelIdx, const IndexDef **delIndice, bool *cancelFlag/* = NULL*/):
	TblMntAlterIndex(table->getNtseTable(), numAddIdx, addIndice, numDelIdx, delIndice, cancelFlag) {
	m_tntTable = table;
}

/** 内存建索引和删除索引
 * @param session 会话
 * @param addInidice 需要增加的索引
 * @param numAddIdx 需要增加的索引个数
 * @param delIndice 需要删除的索引
 * @param numDelIdx 需要删除的索引个数
 */
void TNTTblMntAlterIndex::additionalAlterIndex(Session *session, TableDef *oldTblDef, TableDef **newTblDef, DrsIndice *drsIndice,
											 const IndexDef **addIndice, u16 numAddIdx, bool *idxDeleted) {
	s16 idxNo = 0;
	//TblMntAlterIndex alterTable实现是先删索引，再加索引的
	//从后删到前，不会导致dropMemIndex中memmove引起逻辑错误
	for (idxNo = oldTblDef->m_numIndice - 1; idxNo >= 0; idxNo--) {
		if (idxDeleted[idxNo]) {
			m_tntTable->dropMemIndex(session, idxNo);
		}
	}

	m_tntTable->getIndice()->m_tableDef = newTblDef;
	m_tntTable->getIndice()->m_drsIndice = drsIndice;
	m_tntTable->getMRecords()->replaceComponents(newTblDef);

	m_tntTable->addMemIndex(session, numAddIdx, addIndice);
}

/** 加表元数据锁
 * @pre 必须未加数据锁，即加表元数据锁必须在加数据锁之前
 *
 * @param session 会话
 * @param mode 锁模式，只能是S、U或X
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时
 */
void TNTTblMntAlterIndex::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->lockMeta(session, mode, timeoutMs, file, line);
}

/** 升级表元数据锁。若oldMode与newMode相等或oldMode是比newMode更高级的锁，则不进行任何操作。
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时或失败，NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void TNTTblMntAlterIndex::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->upgradeMetaLock(session, oldMode, newMode, timeoutMs, file, line);
}

/** 降级表元数据锁。若oldMode与newMode相等，若newMode比oldMode高级则不进行任何操作
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 */
void TNTTblMntAlterIndex::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	m_tntTable->downgradeMetaLock(session, oldMode, newMode, file, line);
}

/** 释放表元数据锁
 * @pre 必须在释放表数据锁之后调用
 *
 * @param session 会话
 * @param mode 锁模式
 */
void TNTTblMntAlterIndex::unlockMeta(Session *session, ILMode mode) {
	m_tntTable->unlockMeta(session, mode);
}

void TNTTblMntAlterIndex::enableLogging(Session *session) {
	session->enableLogging();
	if (session->getTrans() != NULL) {
		session->getTrans()->enableLogging();
	}
}

void TNTTblMntAlterIndex::disableLogging(Session *session) {
	session->disableLogging();
	if (session->getTrans() != NULL) {
		session->getTrans()->disableLogging();
	}
}

/**
 * 重新打开临时表并替换原表组件
 * @param session
 * @param origTablePath
 * @return 
 */
void TNTTblMntAlterIndex::reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict/* = false*/) {
	TableOnlineMaintain::reopenTblAndReplaceComponent(session, origTablePath, hasCprsDict);
	m_tntTable->m_tabBase->open(session, session->getTNTDb(), m_table->getTableDefAddr(),  m_table->getLobStorage(), m_table->getIndice());
}

TNTTblMntAlterColumn::TNTTblMntAlterColumn(TNTTable *table, Connection *conn, u16 addColNum, const AddColumnDef *addCol, 
		u16 delColNum, const ColumnDef **delCol, bool *cancelFlag/* = NULL*/, bool keepOldDict/* = false*/):
		TblMntAlterColumn(table->getNtseTable(), conn, addColNum, addCol, delColNum, delCol, cancelFlag, keepOldDict) {
	m_tntTable = table;
	m_tntDb = table->m_db;
}

void TNTTblMntAlterColumn::preLockTable() {
	while (true) {
		if (m_tntTable->getMRecSize() == 0) {
			break;
		}

		Thread::msleep(1000);
	}
}

void TNTTblMntAlterColumn::additionalAlterColumn(Session *session, NtseRidMapping *ridmap) {
	bool ret = false;
	u8 i = 0;
	RowId newRid = INVALID_ROW_ID;
	TableDef *tblDef = m_table->getTableDef();
	//重建内存hash索引
	HashIndex *newHashIndex = new HashIndex(m_tntTable->m_db->getTNTIMPageManager());
	HashIndex *oldHashIndex = m_tntTable->getMRecords()->getHashIndex();
	for (i = 0; i < HashIndex::HASHINDEXMAP_SIZE; i++) {
		HashIndexMap *hashIndexMap = oldHashIndex->m_mapEntries[i];
		hashIndexMap->m_lock.lock(Exclusived, __FILE__, __LINE__);
		for (size_t pos = 0; pos < hashIndexMap->m_indexMap.getSize(); pos++) {
			HashIndexEntry *entry = hashIndexMap->m_indexMap.getAt(pos);
			newRid = ridmap->getMapping(entry->m_rowId);
			assert(INVALID_ROW_ID != newRid);
			newHashIndex->insert(newRid, entry->m_value, entry->m_version, entry->m_type);
			if (entry->m_type == HIT_MHEAPREC) {
				ret = m_tntTable->getMRecords()->m_heap->remapHeapRecord(session, (void *)entry->m_value, entry->m_rowId, newRid);
				assert(ret);
			}
		}
		hashIndexMap->clear(false);
		hashIndexMap->m_lock.unlock(Exclusived);
	}
	m_tntTable->getMRecords()->replaceHashIndex(newHashIndex);
	m_tntTable->getIndice()->getMemIndice()->setDoubleChecker(newHashIndex);
	delete oldHashIndex;

	//删除内存索引
	for (s16 idxNo = tblDef->m_numIndice - 1; idxNo >= 0; idxNo--) {
		m_tntTable->dropMemIndex(session, idxNo);
	}
	//重建内存索引
	m_tntTable->addMemIndex(session, tblDef->m_numIndice, (const IndexDef **)tblDef->m_indice);
}

/**
 * 重新打开临时表并替换原表组件
 * @param session
 * @param origTablePath
 * @return 
 */
void TNTTblMntAlterColumn::reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict/* = false*/) {
	TableOnlineMaintain::reopenTblAndReplaceComponent(session, origTablePath, hasCprsDict);
	m_tntTable->m_tabBase->open(session, session->getTNTDb(), m_table->getTableDefAddr(), m_table->getLobStorage(), m_table->getIndice());
}

/** 加表元数据锁
 * @pre 必须未加数据锁，即加表元数据锁必须在加数据锁之前
 *
 * @param session 会话
 * @param mode 锁模式，只能是S、U或X
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时
 */
void TNTTblMntAlterColumn::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->lockMeta(session, mode, timeoutMs, file, line);
}

/** 升级表元数据锁。若oldMode与newMode相等或oldMode是比newMode更高级的锁，则不进行任何操作。
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 * @param timeoutMs >0表示毫秒数的超时时间，=0表示尝试加锁，<0表示不超时
 * @throw NtseException 加锁超时或失败，NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void TNTTblMntAlterColumn::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->upgradeMetaLock(session, oldMode, newMode, timeoutMs, file, line);
}

/** 降级表元数据锁。若oldMode与newMode相等，若newMode比oldMode高级则不进行任何操作
 * @param session 会话
 * @param oldMode 原来加的锁
 * @param newMode 要升级成的锁
 */
void TNTTblMntAlterColumn::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	m_tntTable->downgradeMetaLock(session, oldMode, newMode, file, line);
}

/** 释放表元数据锁
 * @pre 必须在释放表数据锁之后调用
 *
 * @param session 会话
 * @param mode 锁模式
 */
void TNTTblMntAlterColumn::unlockMeta(Session *session, ILMode mode) {
	m_tntTable->unlockMeta(session, mode);
}

void TNTTblMntAlterColumn::enableLogging(Session *session) {
	session->enableLogging();
	if (session->getTrans() != NULL) {
		session->getTrans()->enableLogging();
	}
}

void TNTTblMntAlterColumn::disableLogging(Session *session) {
	session->disableLogging();
	if (session->getTrans() != NULL) {
		session->getTrans()->disableLogging();
	}
}

/**
 * 更改堆类型操作构造函数
 * @param table 要操作的表
 * @param conn  连接
 */
TNTTblMntAlterHeapType::TNTTblMntAlterHeapType(TNTTable *table, Connection *conn, bool *cancelFlag) 
	: TNTTblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag) {
}

TNTTblMntAlterHeapType::~TNTTblMntAlterHeapType() {
}

/**
 * 重载TblMntAlterColumn::preAlterTblDef, 更新堆类型
 * @param session
 * @param newTbdef
 * @param tempTableDefInfo
 * @return 
 */
TableDef* TNTTblMntAlterHeapType::preAlterTblDef(Session *session, TableDef *newTbdef, 
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
TNTTblMntOptimizer::TNTTblMntOptimizer(TNTTable *table, Connection *conn, bool *cancelFlag, bool keepOldDict) 
		: TNTTblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag, keepOldDict) {
}

TNTTblMntOptimizer::~TNTTblMntOptimizer() {
}

}