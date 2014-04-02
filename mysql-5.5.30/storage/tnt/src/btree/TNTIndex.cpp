/**
* ����������ӿڷ�װ
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#include "btree/TNTIndex.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"
#include "misc/IndiceLockManager.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBLinkTree.h"

namespace tnt {

/**
 * �����ڴ�����ɨ���������һ����¼
 */
void TNTIdxScanHandle::saveMemKey() {
	MIndexScanInfoExt *mIndexScanInfo = (MIndexScanInfoExt*)m_memIdxScanHdl->getScanInfo();
	assert(KEY_NATURAL == mIndexScanInfo->m_readKey->m_format);
	m_scanInfo->m_currentKey->m_size = m_scanInfo->m_curKeyBufSize;
	RecordOper::convertKeyNP(m_scanInfo->m_tableDef, m_scanInfo->m_indexDef, mIndexScanInfo->m_readKey, m_scanInfo->m_currentKey);
}

/**
 * �����������ɨ���������һ����¼
 */
void TNTIdxScanHandle::saveDrsKey() {
	DrsIndexScanHandleInfoExt* drsIndexScanInfo = 
		(DrsIndexScanHandleInfoExt*)m_drsIdxScanHdl->getScanInfo();
	assert(KEY_COMPRESS == drsIndexScanInfo->m_readKey->m_format);
	m_scanInfo->m_currentKey->m_size = m_scanInfo->m_curKeyBufSize;
	RecordOper::convertKeyCP(m_scanInfo->m_tableDef, m_scanInfo->m_indexDef, drsIndexScanInfo->m_readKey, m_scanInfo->m_currentKey);
}

bool TNTIdxScanHandle::retMemIndexKey() {
	saveMemKey();

	if (!isOutofRange(m_scanInfo->m_currentKey)) {
		m_scanInfo->m_scanState = ON_MEM_INDEX;
		saveMVInfo(m_memIdxScanHdl->getMVInfo());
		return true;
	}

	return false;
}

void TNTIdxScanHandle::moveMemIndexKey() {
	MIndexScanInfoExt *mScanInfo = (MIndexScanInfoExt*)m_memIdxScanHdl->getScanInfo();
	DrsIndexScanHandleInfoExt* dScanInfo = (DrsIndexScanHandleInfoExt*)m_drsIdxScanHdl->getScanInfo();
	
	if (!isOutofRange(m_scanInfo->m_currentKey)) {
		mScanInfo->moveCursor();
		dScanInfo->moveCursorTo(m_scanInfo->m_currentKey);
	}
}

bool TNTIdxScanHandle::retDrsIndexKey() {
	saveDrsKey();

	if (!isOutofRange(m_scanInfo->m_currentKey)) {
		m_scanInfo->m_scanState = ON_DRS_INDEX;
		saveMVInfo(false, 0, 0, true);
		
		return true;
	}

	return false;
}

void TNTIdxScanHandle::moveDrsIndexKey() {
	MIndexScanInfoExt *mScanInfo = (MIndexScanInfoExt*)m_memIdxScanHdl->getScanInfo();
	DrsIndexScanHandleInfoExt* dScanInfo = (DrsIndexScanHandleInfoExt*)m_drsIdxScanHdl->getScanInfo();

	if (!isOutofRange(m_scanInfo->m_currentKey)) {
		dScanInfo->moveCursor();
		mScanInfo->moveCursorTo(m_scanInfo->m_currentKey);
	}
}

bool TNTIdxScanHandle::retMemDrsKeyEqual() {
	saveMemKey();	
	
	if (!isOutofRange(m_scanInfo->m_currentKey)) {
		m_scanInfo->m_scanState = ON_MEM_INDEX;
		saveMVInfo(m_memIdxScanHdl->getMVInfo());

		return true;
	}

	return false;
}

void TNTIdxScanHandle::moveMemDrsIndexKey() {
	MIndexScanInfoExt *mScanInfo = (MIndexScanInfoExt*)m_memIdxScanHdl->getScanInfo();
	DrsIndexScanHandleInfoExt* dScanInfo = (DrsIndexScanHandleInfoExt*)m_drsIdxScanHdl->getScanInfo();

	if (!isOutofRange(m_scanInfo->m_currentKey)) {
		dScanInfo->moveCursor();
		mScanInfo->moveCursor();
	}
}

/**
 * ��ǰɨ�赽�ļ�ֵ�Ƿ��Ѿ�����Ψһ��ɨ��ļ�ֵ
 * @param tntScanInfo
 * @param key
 * @return
 */
bool TNTIdxScanHandle::isOutofRange(const SubRecord *key) {
	if (m_scanInfo->m_isUnique) {
		int cmp = RecordOper::compareKeyPP(m_scanInfo->m_tableDef, m_scanInfo->m_searchKey, key, m_scanInfo->m_indexDef);
		return 0 == cmp ? false : (cmp > 0) ^ m_scanInfo->m_isForward;
	}
	return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * ����TNTIndice
 * @param db
 * @param tableDef
 * @param lobStorage
 * @param drsIndice
 * @param memIndice
 */
TNTIndice::TNTIndice(TNTDatabase *db, TableDef	**tableDef, LobStorage *lobStorage, DrsIndice *drsIndice, 
					 MIndice *memIndice) : m_db(db), m_tableDef(tableDef), m_lobStorage(lobStorage),
					 m_drsIndice(drsIndice), m_memIndice(memIndice) {
	
	m_tntIndex = new TNTIndex*[Limits::MAX_INDEX_NUM];
	memset(m_tntIndex, 0, Limits::MAX_INDEX_NUM * sizeof(TNTIndex *));

	uint indexNum =  drsIndice->getIndexNum();
	for (uint i = 0; i < indexNum; i++) {
		m_tntIndex[i] = new TNTIndex(db, tableDef, (*tableDef)->m_indice[i], this, 
			m_drsIndice->getIndex(i), m_memIndice->getIndex(i));
	}
}

TNTIndice::~TNTIndice() {
	assert(!m_tntIndex);
	assert(!m_drsIndice);
	assert(!m_memIndice);
}

/**
 * ����һ��TNT�����������ļ�����ʼ��
 * @param path
 * @param tableDef
 * @return 
 */
void TNTIndice::create(const char *path, const TableDef *tableDef) throw(NtseException) {
	DrsIndice::create(path, tableDef);
}


/**
 * ��TNT���Ӧ������
 * @param db
 * @param session
 * @param tableDef
 * @param drsIndice
 * @param doubleChecker
 * @return 
 */
TNTIndice* TNTIndice::open(TNTDatabase *db, Session *session, TableDef **tableDef, LobStorage *lobStorage,
						   DrsIndice *drsIndice, const DoubleChecker *doubleChecker) {
		MIndice *memIndice = MIndice::open(db, session, drsIndice->getIndexNum(), 
			tableDef, lobStorage, doubleChecker);
		if (!memIndice)
			return NULL;
		TNTIndice *tntIndice = new TNTIndice(db, tableDef, lobStorage, drsIndice, memIndice);
		return tntIndice;
}

/**
 * ����TNT��������Ӧ���ļ�
 * @param path
 * @return 
 */
void TNTIndice::drop(const char *path) throw(NtseException) {
	DrsIndice::drop(path);
}

/**
 * �ر�TNT�������
 * @param session
 * @return 
 */
void TNTIndice::close(Session *session, bool closeMIndice/*= true*/) {
	uint indexNum = getIndexNum();
	for (uint i = 0; i < indexNum; i++) {
		delete m_tntIndex[i];
		m_tntIndex[i] = NULL;
	}
	delete [] m_tntIndex;
	m_tntIndex = NULL;

	if (closeMIndice) {
		m_memIndice->close(session);
		delete m_memIndice;
		m_memIndice = NULL;
	} else {
		assert(false);//�˴���ʱ�����ܵ�
		m_memIndice->setTableDef(NULL);
	}

	m_drsIndice = NULL;
	m_tableDef = NULL;
}

void TNTIndice::reOpen(TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice) {
	// �������ñ���
	m_tableDef = tableDef;
	assert(m_memIndice);
	m_memIndice->setTableDef(tableDef);

	// ���������������������
	m_drsIndice = drsIndice;

	// ������������еĴ��������������ڴ�������TNT����
	m_lobStorage = lobStorage;
	m_memIndice->setLobStorage(m_lobStorage);

	assert(tableDef == m_tableDef);
	assert(m_drsIndice->getIndexNum() == m_memIndice->getIndexNum());

	uint indexNum =  drsIndice->getIndexNum();	
	//ɾ��ԭ�е�TNT����
	for (uint i = 0; i < indexNum; i++) {
		delete m_tntIndex[i];
		m_tntIndex[i] = NULL;
	}
	delete [] m_tntIndex;
	m_tntIndex = NULL;


	m_tntIndex = new TNTIndex*[Limits::MAX_INDEX_NUM];
	memset(m_tntIndex, 0, Limits::MAX_INDEX_NUM * sizeof(TNTIndex *));

	
	for (uint i = 0; i < indexNum; i++) {
		m_tntIndex[i] = new TNTIndex(m_db, tableDef, (*tableDef)->m_indice[i], this, 
			m_drsIndice->getIndex(i), m_memIndice->getIndex(i));
	}
}

/**
 * �����е�Ψһ��������Ψһ�Լ�ֵ��
 * @param session �Ự
 * @param record  Ҫ�����ļ�¼��һ����REC_REDUNDANT��ʽ
 * @param dupIndex OUT ����ʧ�ܵ�������
 * @return �Ƿ�����ɹ�
 */
bool TNTIndice::lockAllUniqueKey(Session *session, const Record *record, uint *dupIndex) {
	assert(REC_REDUNDANT == record->m_format);

 	const OrderedIndices *orderIndices = m_drsIndice->getOrderedIndices();
	u16 uniqueIndexNum = 0;
	const u16 *uniqueIndexNo = NULL;
	orderIndices->getOrderUniqueIdxNo(&uniqueIndexNo, &uniqueIndexNum);

	return lockUpdateUniqueKey(session, record, uniqueIndexNum, uniqueIndexNo, dupIndex);
}

/**
 * ����Ҫ���µ�Ψһ��������Ψһ�Լ�ֵ��
 * @param session �Ự
 * @param uniqueKeyManager Ψһ����������
 * @param record  Ҫ�����ļ�¼��һ����REC_REDUNDANT��ʽ
 * @param updateUniques   Ψһ��������Ŀ
 * @param updateUniquesNo Ψһ������ID����
 * @param dupIndex OUT ����ʧ�ܵ�������
 * @return �Ƿ�����ɹ�
 */
bool TNTIndice::lockUpdateUniqueKey(Session *session, const Record *record, u16 updateUniques, 
									const u16 *updateUniquesNo, uint *dupIndex) {
	assert(REC_REDUNDANT == record->m_format);
	for (uint i = 0; i < updateUniques; i++) {
		u8 idxNo = (u8)updateUniquesNo[i];
		const IndexDef *indexDef = (*m_tableDef)->getIndexDef(idxNo);
		UKLockManager *uniqueKeyMgr = m_db->getUniqueKeyLock();

		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromM(session, *m_tableDef, indexDef, record, &lobArray);
		}
		
		// ���ٻ�Ϊ�����������6����12���ֽڣ�����㹻��������ID+����ID����3���ֽڣ�
		SubRecord *uniqueKey = IndexKey::allocSubRecord(session->getMemoryContext(), indexDef, KEY_NATURAL);
		RecordOper::extractKeyRN(*m_tableDef, indexDef, record, &lobArray, uniqueKey);

		RecordOper::appendKeyTblIdAndIdxId(uniqueKey, (*m_tableDef)->m_id, idxNo);

		if (!TRY_LOCK_UNIQUE(session, uniqueKeyMgr, calcCheckSum(uniqueKey))) {
			session->unlockAllUniqueKey();
			*dupIndex = idxNo;
			return false;
		}
	}
	return true;
}

/**
 * ��������Ψһ�������Ƿ����ָ�����ظ���ֵ
 * @param session �Ự
 * @param record  Ҫ���ļ�¼��һ����REC_REDUNDANT��ʽ
 * @param uniquesNum Ҫ����Ψһ��������Ŀ
 * @param uniquesNo  Ҫ����Ψһ��������ID����
 * @param dupIndex       OUT ����Ψһ��������ͻ��������ID
 * @param drsScanHdlInfo INOUT ���Ψһ��������ɨ������Ϣ(���ڰ����Ժ�Ĳ���)�����ΪNULL����Ҫ���
 * @return 
 */
bool TNTIndice::checkDuplicate(Session *session, const Record *record, u16 uniquesNum, 
							   const u16 * uniquesNo, uint *dupIndex, 
							   DrsIndexScanHandleInfo **drsScanHdlInfo) {
	assert(REC_REDUNDANT == record->m_format);
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint lobSavepoint(session->getLobContext());
	//����ڴ������Ƿ�����ظ���ֵ
	for (uint i = 0; i < uniquesNum; i++) {
		u16 idxNo = uniquesNo[i];
		MIndex * memIndex = m_memIndice->getIndex(idxNo);
		IndexDef *indexDef = (*m_tableDef)->getIndexDef(idxNo);
		Array<LobPair*> lobArray;
		if (indexDef->hasLob()) {
			RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, record, &lobArray);
		}

		SubRecord *uniqueKey = IndexKey::allocSubRecord(mtx, (*m_tableDef)->getIndexDef(idxNo), KEY_NATURAL);
		RecordOper::extractKeyRN(*m_tableDef, (*m_tableDef)->getIndexDef(idxNo), record, &lobArray, uniqueKey);

		if (memIndex->checkDuplicate(session, uniqueKey)) {
			*dupIndex = idxNo;
			return true;
		}
	}

	//�����������Ƿ�����ظ���ֵ, �����Ѿ�ִ����һ�α��������Լ��´������ҳ�棬�ͷ�ҳ��latch
	//�����Գ���pin��������������ʱ�����¼�latch�����ҳ��LSN�����ҳ��LSN�����ı䣬�����±��������������ԭҳ��ֱ�Ӳ���
	for (uint i = 0; i < uniquesNum; i++) {
		u16 idxNo = uniquesNo[i];
		DrsIndex * drsIndex = m_drsIndice->getIndex(idxNo);

		const IndexDef *indexDef = (*m_tableDef)->getIndexDef(idxNo);
		bool keyNeedCompress = RecordOper::isFastCCComparable(*m_tableDef, indexDef, indexDef->m_numCols, 
			indexDef->m_columns);
		Array<LobPair*> lobArray;
		if (indexDef->m_prefix) {
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(session, *m_tableDef, indexDef, m_lobStorage, record, &lobArray);
			}
		}
		SubRecord *key = IndexKey::allocSubRecord(mtx, keyNeedCompress, record, &lobArray, *m_tableDef, indexDef);

		if (drsIndex->checkDuplicate(session, key, drsScanHdlInfo ? &drsScanHdlInfo[i] : NULL)) {
			session->unlockIdxAllObjects();
			*dupIndex = i;
			return true;
		}
		session->unlockIdxAllObjects();
	}

	return false;
}

/**
 * TNT����ɾ����¼�ӿ�
 * @param session �Ự
 * @param record Ҫɾ���ļ�¼
 * @param version	����ʱTNT���version��
 */
void TNTIndice::deleteIndexEntries(Session *session, const Record *record, RowIdVersion version) {
	assert(INVALID_ROW_ID != record->m_rowId);

	m_memIndice->deleteIndexEntries(session, record, version);
}



/**
 * TNT�������¼�¼�ӿ�
 * @param session �Ự
 * @param before  ��¼ǰ�������ʽ
 * @param after   ��¼���������ʽ
 * @param isFirstRound	�Ƿ��ǵ�һ�θ���
 * @param version	����ʱTNT���version��
 * @return ���ȫ���������³ɹ�����true��false��ʾ���ظ���ֵ���߷�������
 */
bool TNTIndice::updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool isFirstRound, RowIdVersion version) {
	assert(REC_REDUNDANT == before->m_format && REC_REDUNDANT == after->m_format);

	if (isFirstRound)
		return m_memIndice->insertIndexEntries(session, after, version);
	else
		return m_memIndice->updateIndexEntries(session, before, after);
}


/**
 * ����������һ�׶�
 * @param session �Ự
 * @param idxNo   Ҫ������������ID
 */
void TNTIndice::dropPhaseOne(Session *session, uint idxNo) {
	//��������ɾ��
	uint indexNum = m_memIndice->getIndexNum();
	assert(idxNo < indexNum);
	delete m_tntIndex[idxNo];
	memmove(&m_tntIndex[idxNo], &m_tntIndex[idxNo + 1], (indexNum - idxNo - 1) * sizeof(TNTIndex*));

	m_memIndice->dropIndex(session, idxNo);
	//m_drsIndice->dropPhaseOne(session, idxNo);

	//assert(m_memIndice->getIndexNum() == m_drsIndice->getIndexNum());
}

/**
 * ���������ڶ��׶�
 * @param session �Ự
 * @param idxNo   Ҫ������������ID
 */
void TNTIndice::dropPhaseTwo(Session *session, uint idxNo) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(idxNo);
	//_drsIndice->dropPhaseTwo(session, idxNo);
}

/**
 * ����������һ�׶�
 * @param session
 * @param indexDef
 * @param tblDef
 * @param heap
 */
void TNTIndice::createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, 
									DrsHeap *heap) throw(NtseException) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(indexDef);
	UNREFERENCED_PARAMETER(tblDef);
	UNREFERENCED_PARAMETER(heap);
	//m_drsIndice->createIndexPhaseOne(session, indexDef, tblDef, heap);
}

/**
 * ���������ڶ��׶�
 * @param session
 * @param def
 * @return 
 */
TNTIndex* TNTIndice::createIndexPhaseTwo(Session *session, const IndexDef *def, uint idxNo) {
	//uint indexNo = (*m_tableDef)->getIndexNo(def->m_name);
	DrsIndex * drsIndex = m_drsIndice->getIndex(idxNo);
	u8 indexId = drsIndex->getIndexId();

	MIndex * memIndex = m_memIndice->createIndex(session, def, indexId);
	//assert(m_memIndice->getIndexNum() == m_drsIndice->getIndexNum());
	assert(memIndex->getIndexId() == drsIndex->getIndexId());

	TNTIndex *tntIndex = new TNTIndex(m_db, m_tableDef, def, this, drsIndex, memIndex);
	m_tntIndex[idxNo] = tntIndex;
	
	return tntIndex;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////

TNTIndex::TNTIndex(TNTDatabase *db, TableDef **tableDef, const IndexDef *indexDef, 
				   TNTIndice *tntIndice, DrsIndex *drsIndex, MIndex *memIndex) 
				   : m_tableDef(tableDef), m_indexDef(indexDef), m_tntIndice(tntIndice), 
				   m_drsIndex(drsIndex), m_memIndex(memIndex) {
	//��ʼ��ͳ����Ϣ
	memset(&m_indexStatus, 0, sizeof(m_indexStatus));
}

TNTIndex::~TNTIndex() {

}

/**
 * �Ƚ�����������������Ҽ�(����RowId)�Ĵ�С
 * @param memKey �ڴ��������Ҽ���һ����KEY_NATURAL��ʽ
 * @param drsKey ����������Ҽ���KEY_COMPRESS��ʽ��KEY_PAD��ʽ
 * @return -1С��/0����/1����
 */
int TNTIndex::compareKeys(const SubRecord *memKey, const SubRecord *drsKey) {
	assert(KEY_NATURAL == memKey->m_format);
	assert(KEY_COMPRESS == drsKey->m_format || KEY_PAD == drsKey->m_format);

	int result = ((KEY_COMPRESS == drsKey->m_format) ? 
		RecordOper::compareKeyNC(*m_tableDef, memKey, drsKey, m_indexDef) 
		: RecordOper::compareKeyNP(*m_tableDef, memKey, drsKey, m_indexDef));
	return (0 == result) ? (memKey->m_rowId > drsKey->m_rowId ? 1 : 
		(memKey->m_rowId == drsKey->m_rowId ? 0 : -1)) : result;
}

#ifdef TNT_INDEX_OPTIMIZE
/**
 * ����һ��TNT���������Χɨ��
 * @param session    �Ự
 * @param key        ���Ҽ�, ΪKEY_PAD��ʽ
 * @param forward    �Ƿ�������ɨ��
 * @param includeKey �Ƿ�������Ҽ�
 * @param extractor  �Ӽ�¼��ȡ�������ΪNULL��ʾֻ��Ҫ���RowId
 * @return TNT����ɨ����
 */
TNTIdxScanHandle* TNTIndex::beginScanFast(Session *session,  const SubRecord *key, bool forward, 
									  bool includeKey, SubToSubExtractor *extractor) {
	DrsIndexRangeScanHandle *drsIdxScanHdl = (DrsIndexRangeScanHandle*)m_drsIndex->beginScan(
		session, key, forward, includeKey, None, NULL, NULL);
	MIndexRangeScanHandle *memIdxScanHdl = (MIndexRangeScanHandle*)m_memIndex->beginScanFast(
		session, key, forward, includeKey);

	void *data = session->getMemoryContext()->alloc(sizeof(TNTIdxScanHandleInfo));
	TNTIdxScanHandleInfo *scanInfo = new (data)TNTIdxScanHandleInfo(session, extractor);
		
	data = session->getMemoryContext()->alloc(sizeof(TNTIdxScanHandle));
	TNTIdxScanHandle *tntIndexScanHdl = new (data)TNTIdxScanHandle(drsIdxScanHdl, memIdxScanHdl);
	tntIndexScanHdl->setScanInfo(scanInfo);

	return tntIndexScanHdl;
}

/**
 * ��ȡ������Χɨ�����һ��
 * @param scanHandle ɨ����
 * @param key     INOUT �����ΪNULL���������������������ȡ���Ӽ�¼
 * @return �Ƿ������һ��
 */
bool TNTIndex::getNextFast(TNTIdxScanHandle *scanHandle) {
	TNTIdxScanHandleInfo *tntScanInfo = scanHandle->getScanInfo();
	if (unlikely(!tntScanInfo->m_hasNext)) {
		return false;
	}

	MIndexRangeScanHandle *mIndexScanHdl = scanHandle->getMemIdxScanHdl();
	DrsIndexRangeScanHandle *drsIndexScanHdl = scanHandle->getDrsIndexScanHdl();
	
	MIndexScanInfo *mIndexScanInfo = mIndexScanHdl->getScanInfo();
	DrsIndexScanHandleInfo* drsIndexScanInfo = drsIndexScanHdl->getScanInfo();
	
	bool memGotNext = false;
	bool drsGotNext = false;

	if (tntScanInfo->m_rangeFirst) {
		assert(ON_NONE == tntScanInfo->m_scanState);
		memGotNext = m_memIndex->getNextFast(mIndexScanHdl);
		drsGotNext = m_drsIndex->getNext(drsIndexScanHdl, NULL);
	} else if (ON_MEM_INDEX == tntScanInfo->m_scanState) {
		memGotNext = m_memIndex->getNextFast(mIndexScanHdl);
	} else {
		assert(ON_DRS_INDEX == tntScanInfo->m_scanState);
		drsGotNext = m_drsIndex->getNext(drsIndexScanHdl, NULL);
	}

	if (!drsIndexScanInfo->m_hasNext) {//����Ѿ�ɨ����
		if (likely(memGotNext)) {
			tntScanInfo->m_currentKey = mIndexScanInfo->m_searchKey;
			tntScanInfo->m_scanState = ON_MEM_INDEX;
			goto __succeed;
		} else {
			assert(!mIndexScanInfo->m_hasNext);
			goto __failed;
		}
	} else if (!mIndexScanInfo->m_hasNext) {//�ڴ��Ѿ�ɨ����
		if (likely(drsGotNext)) {
			tntScanInfo->m_currentKey = drsIndexScanInfo->m_findKey;
			tntScanInfo->m_scanState = ON_DRS_INDEX;
			goto __succeed;
		} else {
			assert(!drsIndexScanInfo->m_hasNext);
			goto __failed;
		}
	} else {
		int cmp = compareKeys(mIndexScanInfo->m_searchKey, drsIndexScanInfo->m_findKey);
		if (cmp > 0) {//����α�ָ��ļ�ֵ��С���������������ֵ
			tntScanInfo->m_currentKey = drsIndexScanInfo->m_findKey;
			tntScanInfo->m_scanState = ON_DRS_INDEX;
			goto __succeed;
		} else {
			//�ڴ��α�ָ��ļ�ֵ��С�������ڴ�������ֵ�����ߣ�
			//������α�ȷ��ͬһ��ֵ, �����ڴ�������ֵ���������������Ӧ�ļ�ֵ
			if (cmp == 0) {
				assert(mIndexScanInfo->m_searchKey->m_rowId == drsIndexScanInfo->m_findKey->m_rowId);
				//�������ɨ������Ϣ ? Ӧ�ò���Ҫ��ֱ�ӽ�����������α��Ƶ���һ���
				m_drsIndex->getNext(drsIndexScanHdl, NULL);
			}
			tntScanInfo->m_currentKey = mIndexScanInfo->m_searchKey;
			tntScanInfo->m_scanState = ON_MEM_INDEX;
			goto __succeed;
		}
	}	

__succeed:
	tntScanInfo->m_rangeFirst = false;
	if (ON_MEM_INDEX == tntScanInfo->m_scanState) {
		scanHandle->saveMVInfo(mIndexScanHdl->getMVInfo());
	} else {
		assert(ON_DRS_INDEX == tntScanInfo->m_scanState);
		scanHandle->saveMVInfo(false, 0);
	}
	return true;

__failed:
	tntScanInfo->m_hasNext = false;
 	return false;
}

/**
 * ����TNT���������Χɨ��
 * @param scanHandle
 */
void TNTIndex::endScanFast(TNTIdxScanHandle *scanHandle) {
	//scanHandle���ڴ��MemContext�з��䣬�����ͷ�
	m_memIndex->endScanFast(scanHandle->getMemIdxScanHdl());
	m_drsIndex->endScan(scanHandle->getDrsIndexScanHdl());
}
#endif

/**
 * ����һ��TNT���������Χɨ��
 * @param session    �Ự
 * @param key        ���Ҽ�, ΪKEY_PAD��ʽ
 * @param unique     �Ƿ���Ψһ��ɨ��
 * @param forward    �Ƿ�������ɨ��
 * @param includeKey �Ƿ�������Ҽ�
 * @param extractor  �Ӽ�¼��ȡ�������ΪNULL��ʾֻ��Ҫ���RowId
 * @return TNT����ɨ����
 */
TNTIdxScanHandle* TNTIndex::beginScan(Session *session,  const SubRecord *key, SubRecord *redKey, 
									  bool unique, bool forward, bool includeKey, TLockMode trxLockMode,
									  SubToSubExtractor *extractor) {
    assert(!key || KEY_PAD == key->m_format);

	//ͳ����Ϣ
	m_indexStatus.m_numScans++;
	m_indexStatus.m_backwardScans = m_indexStatus.m_backwardScans + (forward ? 0 : 1);

	void *data = session->getMemoryContext()->alloc(sizeof(TNTIdxScanHandleInfo));
	TNTIdxScanHandleInfo *scanInfo = new (data)TNTIdxScanHandleInfo(session, *m_tableDef, m_indexDef,
		key, redKey, extractor);
	scanInfo->m_isUnique = unique;
	scanInfo->m_isForward = forward;
	scanInfo->m_pDIdxRlh = &scanInfo->m_dIdxRlh;
	scanInfo->m_pMIdxRlh = &scanInfo->m_mIdxRlh;
	scanInfo->m_trxLockMode = trxLockMode;

	// �ײ�NTSEԭ����������TNT���о͵�latch���ˣ�����ֻ��Ҫ��Sharedģʽ����������
	DrsIndexRangeScanHandle *drsIdxScanHdl = (DrsIndexRangeScanHandle*)m_drsIndex->beginScanSecond(
		session, key, forward, includeKey, Shared, scanInfo->m_pDIdxRlh, NULL, trxLockMode);
	MIndexRangeScanHandle *memIdxScanHdl = (MIndexRangeScanHandle*)m_memIndex->beginScan(
		session, key, forward, includeKey, Shared, scanInfo->m_pMIdxRlh, trxLockMode);
		
	data = session->getMemoryContext()->alloc(sizeof(TNTIdxScanHandle));
	TNTIdxScanHandle *tntIndexScanHdl = new (data)TNTIdxScanHandle(drsIdxScanHdl, memIdxScanHdl);
	tntIndexScanHdl->setScanInfo(scanInfo);

	return tntIndexScanHdl;
}

/**
 * TNT������Χɨ���ȡ��һ��
 * @param scanHandle
 * @return �Ƿ������һ��
 */
bool TNTIndex::getNext(TNTIdxScanHandle *scanHandle) throw(NtseException) {
	assert(NULL != scanHandle);

	TNTIdxScanHandleInfo *tntScanInfo = scanHandle->getScanInfo();
	MIndexRangeScanHandle *mIndexScanHdl = scanHandle->getMemIdxScanHdl();
	DrsIndexRangeScanHandle *drsIndexScanHdl = scanHandle->getDrsIndexScanHdl();
	assert(NULL != mIndexScanHdl);
	assert(NULL != drsIndexScanHdl);
	MIndexScanInfoExt *mIndexScanInfo = (MIndexScanInfoExt*)mIndexScanHdl->getScanInfo();
	DrsIndexScanHandleInfoExt* drsIndexScanInfo = (DrsIndexScanHandleInfoExt*)drsIndexScanHdl->getScanInfo();
	Session *session = tntScanInfo->m_session;
	RowId lockRowId = INVALID_ROW_ID;

__loop:
	bool ret = false;
	bool drsMove = false;
	bool memMove = false;
	bool drsHasNext = m_drsIndex->getNextSecond(drsIndexScanHdl);
	bool memHasNext = m_memIndex->getNext(mIndexScanHdl);
	
	if (!drsHasNext) {
		if (memHasNext) {// ֻ���ڴ�����һ��
			//ͳ����Ϣ
			m_indexStatus.m_numMIdxReturn++;
			ret = scanHandle->retMemIndexKey();
			memMove = true;
		}
	} else {
		if (memHasNext) {// ������������һ���Ҫ�Ƚϼ�ֵ��С
			int cmp = compareKeys(mIndexScanInfo->m_readKey, drsIndexScanInfo->m_readKey);
			if (0 == cmp) {
				//ͳ����Ϣ
				m_indexStatus.m_numMIdxReturn++;
				ret = scanHandle->retMemDrsKeyEqual();
				memMove = true;
				drsMove = true;
			} else {
				if ((cmp > 0) ^ (tntScanInfo->m_isForward)) {
					//ͳ����Ϣ
					m_indexStatus.m_numMIdxReturn++;
					ret = scanHandle->retMemIndexKey();
					memMove = true;
				}
				else {
					//ͳ����Ϣ
					m_indexStatus.m_numDrsReturn++;
					ret = scanHandle->retDrsIndexKey();
					drsMove = true;
				//ret = ((cmp > 0) ^ tntScanInfo->m_isForward) ? (scanHandle->retMemIndexKey(), memMove = true) :
				//	(scanHandle->retDrsIndexKey(), drsMove = true);
				}
			}
		} else {// ֻ���������һ��
			//ͳ����Ϣ
			m_indexStatus.m_numDrsReturn++;
			ret = scanHandle->retDrsIndexKey();
			drsMove = true;
		}
	}

	if (ret) {
		TLockMode trxLockMode = tntScanInfo->m_trxLockMode;
		if (trxLockMode != TL_NO) {
			TNTTransaction *trx = session->getTrans();
			/*if (!trx->isRowLocked(tntScanInfo->m_currentKey->m_rowId, m_tableDef->m_id, trxLockMode)) {*/
			try {
				//�����ѭ����������ô��Ҫ�жϴ˴μ������ϴμ����Ƿ���ͬ�������ͬ���ͷ��ϴμ��ϵ�����
				if(lockRowId != INVALID_ROW_ID && lockRowId != tntScanInfo->m_currentKey->m_rowId)
					trx->unlockRow(trxLockMode, lockRowId, (*m_tableDef)->m_id);
				// ��TNT������
				if (trx->tryLockRow(trxLockMode, tntScanInfo->m_currentKey->m_rowId, (*m_tableDef)->m_id)) {
					//���ѭ�������ı�־
					lockRowId = INVALID_ROW_ID;
					//try�������ɹ���ſ����ƶ��α�
					ret = true;
				} else {
					//ͳ����Ϣ
					m_indexStatus.m_numRLRestarts++;

					scanHandle->unlatchNtseRowBoth();
					SYNCHERE(SP_TNT_INDEX_LOCK);
			
					trx->lockRow(trxLockMode, tntScanInfo->m_currentKey->m_rowId, (*m_tableDef)->m_id);
					//ѭ��ǰ��¼������rowid
					lockRowId = tntScanInfo->m_currentKey->m_rowId;

					//��ҳʱҪ����һ��ҳ���pin��������һ��ɨ����������һ��ɨ��ʱû�е�ǰҳ�棩
					if(drsIndexScanInfo->m_pageHandle != drsIndexScanInfo->m_readPageHandle)
						session->unpinPage(&drsIndexScanInfo->m_readPageHandle);

					goto __loop;
				}
			} catch (NtseException &e) {
				// ������ڷ������������ȴ���ʱ�ȵ��¼�������ʧ�ܣ��ͷ�NTSE����
				scanHandle->unlatchNtseRowBoth();
				SYNCHERE(SP_TNT_INDEX_LOCK1);
				throw e;
			}
		}
		
		//�ƶ��α�
		// 1. �����ƶ����
		if(drsMove == true && drsMove != memMove)
			scanHandle->moveDrsIndexKey();
		// 2. ͬʱ�ƶ������
		else if(drsMove == true && drsMove == memMove)
			scanHandle->moveMemDrsIndexKey();
		// 3. �����ƶ��ڴ�
		else
			scanHandle->moveMemIndexKey();

		//ͳ����Ϣ
		m_indexStatus.m_rowsScanned++;
		if (!scanHandle->getScanInfo()->m_isForward)
			++m_indexStatus.m_rowsBScanned;

	}
	

	//scanHandle->unlatchNtseRowBoth();
	return ret;
}

/**
 * ����TNT���������Χɨ��
 * @param scanHandle
 */
void TNTIndex::endScan(TNTIdxScanHandle *scanHandle) {
	//scanHandle->unlatchNtseRowBoth();
	//scanHandle���ڴ��MemContext�з��䣬�����ͷ�
	m_memIndex->endScan(scanHandle->getMemIdxScanHdl());
	m_drsIndex->endScanSecond(scanHandle->getDrsIndexScanHdl());
}

/**
 * ���ݼ�ֵ����Ψһ��λ��ȡ�����
 * @param session   �Ự
 * @param key       ���Ҽ�
 * @param rowId     OUT ����ҵ����������RowId
 * @param subRecord OUT �����Ҫ��ȡ���Ӽ�¼�����ΪNULL��ʾֻ��Ҫ���RowId
 * @param extractor �Ӽ�¼��ȡ��
 * @return 
 */
// bool TNTIndex::getByUniqueKey(Session *session, MTransaction *trx, const SubRecord *key, RowId *rowId, 
// 							  SubRecord *subRecord, SubToSubExtractor *extractor, bool *isVisable) {
// 	if (m_memIndex->getByUniqueKey(session, trx, key, rowId, subRecord, extractor, isVisable)) {
// 		return true;
// 	}
// 	*isVisable = false;
// 	return m_drsIndex->getByUniqueKey(session, key, None, rowId, subRecord, NULL, extractor);
// }

/**
 * ��ȡ�����Ļ���ͳ����Ϣ
 * @return ��������ͳ����Ϣ
 */
const TNTIndexStatus& TNTIndex::getStatus() {
	return m_indexStatus;
}

/**
 * �������������
 * @param session
 * @param readView
 * @return 
 */
u64 TNTIndex::purge(Session *session, const ReadView *readView) {
	return m_memIndex->purge(session, readView);
}

/**
 * ��������ҳ��
 * @param session
 * @param hwm
 * @param lwm
 * @return 
 */
void TNTIndex::reclaimIndex(Session *session, u32 hwm, u32 lwm) {
	m_memIndex->reclaimIndex(session, hwm, lwm);
}


/**
 * �ع���һ���������»�ɾ��
 * @param session
 * @param record
 * @return 
 */
void TNTIndice::undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record) {
	m_memIndice->undoFirstUpdateOrDeleteIndexEntries(session, record);
}

/**
 * �ع��ڶ�����������
 * @param session
 * @param before
 * @param after
 * @return 
 */
bool TNTIndice::undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) {
	return m_memIndice->undoSecondUpdateIndexEntries(session, before, after);
}

/**
 * �ع��ڶ�������ɾ��
 * @param session
 * @param record
 * @return 
 */
void TNTIndice::undoSecondDeleteIndexEntries(Session *session, const Record *record) {
	m_memIndice->undoSecondDeleteIndexEntries(session, record);
}
}