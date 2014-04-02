#include "api/TNTTblMnt.h"

using namespace ntse;
using namespace std;

namespace tnt {
TNTTblMntAlterIndex::TNTTblMntAlterIndex(TNTTable *table,  const u16 numAddIdx, const IndexDef **addIndice, 
	const u16 numDelIdx, const IndexDef **delIndice, bool *cancelFlag/* = NULL*/):
	TblMntAlterIndex(table->getNtseTable(), numAddIdx, addIndice, numDelIdx, delIndice, cancelFlag) {
	m_tntTable = table;
}

/** �ڴ潨������ɾ������
 * @param session �Ự
 * @param addInidice ��Ҫ���ӵ�����
 * @param numAddIdx ��Ҫ���ӵ���������
 * @param delIndice ��Ҫɾ��������
 * @param numDelIdx ��Ҫɾ������������
 */
void TNTTblMntAlterIndex::additionalAlterIndex(Session *session, TableDef *oldTblDef, TableDef **newTblDef, DrsIndice *drsIndice,
											 const IndexDef **addIndice, u16 numAddIdx, bool *idxDeleted) {
	s16 idxNo = 0;
	//TblMntAlterIndex alterTableʵ������ɾ�������ټ�������
	//�Ӻ�ɾ��ǰ�����ᵼ��dropMemIndex��memmove�����߼�����
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

/** �ӱ�Ԫ������
 * @pre ����δ�������������ӱ�Ԫ�����������ڼ�������֮ǰ
 *
 * @param session �Ự
 * @param mode ��ģʽ��ֻ����S��U��X
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ
 */
void TNTTblMntAlterIndex::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->lockMeta(session, mode, timeoutMs, file, line);
}

/** ������Ԫ����������oldMode��newMode��Ȼ�oldMode�Ǳ�newMode���߼��������򲻽����κβ�����
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ��ʧ�ܣ�NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void TNTTblMntAlterIndex::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->upgradeMetaLock(session, oldMode, newMode, timeoutMs, file, line);
}

/** ������Ԫ����������oldMode��newMode��ȣ���newMode��oldMode�߼��򲻽����κβ���
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 */
void TNTTblMntAlterIndex::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	m_tntTable->downgradeMetaLock(session, oldMode, newMode, file, line);
}

/** �ͷű�Ԫ������
 * @pre �������ͷű�������֮�����
 *
 * @param session �Ự
 * @param mode ��ģʽ
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
 * ���´���ʱ���滻ԭ�����
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
	//�ؽ��ڴ�hash����
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

	//ɾ���ڴ�����
	for (s16 idxNo = tblDef->m_numIndice - 1; idxNo >= 0; idxNo--) {
		m_tntTable->dropMemIndex(session, idxNo);
	}
	//�ؽ��ڴ�����
	m_tntTable->addMemIndex(session, tblDef->m_numIndice, (const IndexDef **)tblDef->m_indice);
}

/**
 * ���´���ʱ���滻ԭ�����
 * @param session
 * @param origTablePath
 * @return 
 */
void TNTTblMntAlterColumn::reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict/* = false*/) {
	TableOnlineMaintain::reopenTblAndReplaceComponent(session, origTablePath, hasCprsDict);
	m_tntTable->m_tabBase->open(session, session->getTNTDb(), m_table->getTableDefAddr(), m_table->getLobStorage(), m_table->getIndice());
}

/** �ӱ�Ԫ������
 * @pre ����δ�������������ӱ�Ԫ�����������ڼ�������֮ǰ
 *
 * @param session �Ự
 * @param mode ��ģʽ��ֻ����S��U��X
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ
 */
void TNTTblMntAlterColumn::lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->lockMeta(session, mode, timeoutMs, file, line);
}

/** ������Ԫ����������oldMode��newMode��Ȼ�oldMode�Ǳ�newMode���߼��������򲻽����κβ�����
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 * @param timeoutMs >0��ʾ�������ĳ�ʱʱ�䣬=0��ʾ���Լ�����<0��ʾ����ʱ
 * @throw NtseException ������ʱ��ʧ�ܣ�NTSE_EC_LOCK_TIMEOUT/NTSE_EC_LOCK_FAIL
 */
void TNTTblMntAlterColumn::upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException) {
	m_tntTable->upgradeMetaLock(session, oldMode, newMode, timeoutMs, file, line);
}

/** ������Ԫ����������oldMode��newMode��ȣ���newMode��oldMode�߼��򲻽����κβ���
 * @param session �Ự
 * @param oldMode ԭ���ӵ���
 * @param newMode Ҫ�����ɵ���
 */
void TNTTblMntAlterColumn::downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line) {
	m_tntTable->downgradeMetaLock(session, oldMode, newMode, file, line);
}

/** �ͷű�Ԫ������
 * @pre �������ͷű�������֮�����
 *
 * @param session �Ự
 * @param mode ��ģʽ
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
 * ���Ķ����Ͳ������캯��
 * @param table Ҫ�����ı�
 * @param conn  ����
 */
TNTTblMntAlterHeapType::TNTTblMntAlterHeapType(TNTTable *table, Connection *conn, bool *cancelFlag) 
	: TNTTblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag) {
}

TNTTblMntAlterHeapType::~TNTTblMntAlterHeapType() {
}

/**
 * ����TblMntAlterColumn::preAlterTblDef, ���¶�����
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
TNTTblMntOptimizer::TNTTblMntOptimizer(TNTTable *table, Connection *conn, bool *cancelFlag, bool keepOldDict) 
		: TNTTblMntAlterColumn(table, conn, 0, NULL, 0, NULL, cancelFlag, keepOldDict) {
}

TNTTblMntOptimizer::~TNTTblMntOptimizer() {
}

}