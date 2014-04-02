/**
* 内外存索引接口封装
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "btree/TNTIndex.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"
#include "misc/IndiceLockManager.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBLinkTree.h"

namespace tnt {

/**
 * 保存内存索引扫描读到的下一条记录
 */
void TNTIdxScanHandle::saveMemKey() {
	MIndexScanInfoExt *mIndexScanInfo = (MIndexScanInfoExt*)m_memIdxScanHdl->getScanInfo();
	assert(KEY_NATURAL == mIndexScanInfo->m_readKey->m_format);
	m_scanInfo->m_currentKey->m_size = m_scanInfo->m_curKeyBufSize;
	RecordOper::convertKeyNP(m_scanInfo->m_tableDef, m_scanInfo->m_indexDef, mIndexScanInfo->m_readKey, m_scanInfo->m_currentKey);
}

/**
 * 保存外存索引扫描读到的下一条记录
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
 * 当前扫描到的键值是否已经不是唯一性扫描的键值
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
 * 构造TNTIndice
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
 * 创建一个TNT表的外存索引文件并初始化
 * @param path
 * @param tableDef
 * @return 
 */
void TNTIndice::create(const char *path, const TableDef *tableDef) throw(NtseException) {
	DrsIndice::create(path, tableDef);
}


/**
 * 打开TNT表对应的索引
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
 * 丢弃TNT表索引对应的文件
 * @param path
 * @return 
 */
void TNTIndice::drop(const char *path) throw(NtseException) {
	DrsIndice::drop(path);
}

/**
 * 关闭TNT表的索引
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
		assert(false);//此处暂时不会跑到
		m_memIndice->setTableDef(NULL);
	}

	m_drsIndice = NULL;
	m_tableDef = NULL;
}

void TNTIndice::reOpen(TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice) {
	// 重新设置表定义
	m_tableDef = tableDef;
	assert(m_memIndice);
	m_memIndice->setTableDef(tableDef);

	// 重新设置外存索引管理器
	m_drsIndice = drsIndice;

	// 根据外存索引中的大对象管理器设置内存索引及TNT索引
	m_lobStorage = lobStorage;
	m_memIndice->setLobStorage(m_lobStorage);

	assert(tableDef == m_tableDef);
	assert(m_drsIndice->getIndexNum() == m_memIndice->getIndexNum());

	uint indexNum =  drsIndice->getIndexNum();	
	//删除原有的TNT索引
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
 * 对所有的唯一性索引加唯一性键值锁
 * @param session 会话
 * @param record  要加锁的记录，一定是REC_REDUNDANT格式
 * @param dupIndex OUT 加锁失败的索引号
 * @return 是否加锁成功
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
 * 对需要更新的唯一性索引加唯一性键值锁
 * @param session 会话
 * @param uniqueKeyManager 唯一性锁管理器
 * @param record  要加锁的记录，一定是REC_REDUNDANT格式
 * @param updateUniques   唯一性索引数目
 * @param updateUniquesNo 唯一性索引ID集合
 * @param dupIndex OUT 加锁失败的索引号
 * @return 是否加锁成功
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
		
		// 至少会为索引键多分配6或者12个字节，因此足够用于填充表ID+索引ID（共3个字节）
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
 * 检查内外存唯一性索引是否存在指定的重复键值
 * @param session 会话
 * @param record  要检查的记录，一定是REC_REDUNDANT格式
 * @param uniquesNum 要检查的唯一性索引数目
 * @param uniquesNo  要检查的唯一性索引的ID集合
 * @param dupIndex       OUT 存在唯一性索引冲突的索引的ID
 * @param drsScanHdlInfo INOUT 输出唯一性索引的扫描句柄信息(用于帮助以后的插入)，如果为NULL不需要输出
 * @return 
 */
bool TNTIndice::checkDuplicate(Session *session, const Record *record, u16 uniquesNum, 
							   const u16 * uniquesNo, uint *dupIndex, 
							   DrsIndexScanHandleInfo **drsScanHdlInfo) {
	assert(REC_REDUNDANT == record->m_format);
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint lobSavepoint(session->getLobContext());
	//检查内存索引是否存在重复键值
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

	//检查外存索引是否存在重复键值, 由于已经执行了一次遍历，可以记下待插入的页面，释放页面latch
	//但是仍持有pin，后面真正插入时再重新加latch并检查页面LSN，如果页面LSN发生改变，则重新遍历，否则可以在原页面直接插入
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
 * TNT索引删除记录接口
 * @param session 会话
 * @param record 要删除的记录
 * @param version	更新时TNT表的version号
 */
void TNTIndice::deleteIndexEntries(Session *session, const Record *record, RowIdVersion version) {
	assert(INVALID_ROW_ID != record->m_rowId);

	m_memIndice->deleteIndexEntries(session, record, version);
}



/**
 * TNT索引更新记录接口
 * @param session 会话
 * @param before  记录前像，冗余格式
 * @param after   记录后像，冗余格式
 * @param isFirstRound	是否是第一次更新
 * @param version	更新时TNT表的version号
 * @return 如果全部索引更新成功返回true，false表示有重复键值或者发生死锁
 */
bool TNTIndice::updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool isFirstRound, RowIdVersion version) {
	assert(REC_REDUNDANT == before->m_format && REC_REDUNDANT == after->m_format);

	if (isFirstRound)
		return m_memIndice->insertIndexEntries(session, after, version);
	else
		return m_memIndice->updateIndexEntries(session, before, after);
}


/**
 * 丢弃索引第一阶段
 * @param session 会话
 * @param idxNo   要丢弃的索引的ID
 */
void TNTIndice::dropPhaseOne(Session *session, uint idxNo) {
	//从数组中删除
	uint indexNum = m_memIndice->getIndexNum();
	assert(idxNo < indexNum);
	delete m_tntIndex[idxNo];
	memmove(&m_tntIndex[idxNo], &m_tntIndex[idxNo + 1], (indexNum - idxNo - 1) * sizeof(TNTIndex*));

	m_memIndice->dropIndex(session, idxNo);
	//m_drsIndice->dropPhaseOne(session, idxNo);

	//assert(m_memIndice->getIndexNum() == m_drsIndice->getIndexNum());
}

/**
 * 丢弃索引第二阶段
 * @param session 会话
 * @param idxNo   要丢弃的索引的ID
 */
void TNTIndice::dropPhaseTwo(Session *session, uint idxNo) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(idxNo);
	//_drsIndice->dropPhaseTwo(session, idxNo);
}

/**
 * 创建索引第一阶段
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
 * 创建索引第二阶段
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
	//初始化统计信息
	memset(&m_indexStatus, 0, sizeof(m_indexStatus));
}

TNTIndex::~TNTIndex() {

}

/**
 * 比较两个内外存索引查找键(包含RowId)的大小
 * @param memKey 内存索引查找键，一定是KEY_NATURAL格式
 * @param drsKey 外存索引查找键，KEY_COMPRESS格式或KEY_PAD格式
 * @return -1小于/0等于/1大于
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
 * 开启一个TNT表的索引范围扫描
 * @param session    会话
 * @param key        查找键, 为KEY_PAD格式
 * @param forward    是否是正向扫描
 * @param includeKey 是否包含查找键
 * @param extractor  子记录提取器，如果为NULL表示只需要获得RowId
 * @return TNT索引扫描句柄
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
 * 获取索引范围扫描的下一项
 * @param scanHandle 扫描句柄
 * @param key     INOUT 如果不为NULL将用于输出从索引项中提取的子记录
 * @return 是否存在下一项
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

	if (!drsIndexScanInfo->m_hasNext) {//外存已经扫描完
		if (likely(memGotNext)) {
			tntScanInfo->m_currentKey = mIndexScanInfo->m_searchKey;
			tntScanInfo->m_scanState = ON_MEM_INDEX;
			goto __succeed;
		} else {
			assert(!mIndexScanInfo->m_hasNext);
			goto __failed;
		}
	} else if (!mIndexScanInfo->m_hasNext) {//内存已经扫描完
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
		if (cmp > 0) {//外存游标指向的键值较小，返回外存索引键值
			tntScanInfo->m_currentKey = drsIndexScanInfo->m_findKey;
			tntScanInfo->m_scanState = ON_DRS_INDEX;
			goto __succeed;
		} else {
			//内存游标指向的键值较小，返回内存索引键值，或者：
			//内外存游标确定同一键值, 返回内存索引键值，跳过外存索引对应的键值
			if (cmp == 0) {
				assert(mIndexScanInfo->m_searchKey->m_rowId == drsIndexScanInfo->m_findKey->m_rowId);
				//更新外存扫描句柄信息 ? 应该不需要，直接将外存索引的游标移到下一项即可
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
 * 结束TNT表的索引范围扫描
 * @param scanHandle
 */
void TNTIndex::endScanFast(TNTIdxScanHandle *scanHandle) {
	//scanHandle的内存从MemContext中分配，无须释放
	m_memIndex->endScanFast(scanHandle->getMemIdxScanHdl());
	m_drsIndex->endScan(scanHandle->getDrsIndexScanHdl());
}
#endif

/**
 * 开启一个TNT表的索引范围扫描
 * @param session    会话
 * @param key        查找键, 为KEY_PAD格式
 * @param unique     是否是唯一性扫描
 * @param forward    是否是正向扫描
 * @param includeKey 是否包含查找键
 * @param extractor  子记录提取器，如果为NULL表示只需要获得RowId
 * @return TNT索引扫描句柄
 */
TNTIdxScanHandle* TNTIndex::beginScan(Session *session,  const SubRecord *key, SubRecord *redKey, 
									  bool unique, bool forward, bool includeKey, TLockMode trxLockMode,
									  SubToSubExtractor *extractor) {
    assert(!key || KEY_PAD == key->m_format);

	//统计信息
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

	// 底层NTSE原来的行锁在TNT当中就当latch用了，所以只需要加Shared模式的行锁就行
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
 * TNT索引范围扫描获取下一项
 * @param scanHandle
 * @return 是否存在下一项
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
		if (memHasNext) {// 只有内存有下一项
			//统计信息
			m_indexStatus.m_numMIdxReturn++;
			ret = scanHandle->retMemIndexKey();
			memMove = true;
		}
	} else {
		if (memHasNext) {// 内外存均存在下一项，需要比较键值大小
			int cmp = compareKeys(mIndexScanInfo->m_readKey, drsIndexScanInfo->m_readKey);
			if (0 == cmp) {
				//统计信息
				m_indexStatus.m_numMIdxReturn++;
				ret = scanHandle->retMemDrsKeyEqual();
				memMove = true;
				drsMove = true;
			} else {
				if ((cmp > 0) ^ (tntScanInfo->m_isForward)) {
					//统计信息
					m_indexStatus.m_numMIdxReturn++;
					ret = scanHandle->retMemIndexKey();
					memMove = true;
				}
				else {
					//统计信息
					m_indexStatus.m_numDrsReturn++;
					ret = scanHandle->retDrsIndexKey();
					drsMove = true;
				//ret = ((cmp > 0) ^ tntScanInfo->m_isForward) ? (scanHandle->retMemIndexKey(), memMove = true) :
				//	(scanHandle->retDrsIndexKey(), drsMove = true);
				}
			}
		} else {// 只有外存有下一项
			//统计信息
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
				//如果是循环加锁，那么需要判断此次加锁和上次加锁是否相同，如果不同，释放上次加上的行锁
				if(lockRowId != INVALID_ROW_ID && lockRowId != tntScanInfo->m_currentKey->m_rowId)
					trx->unlockRow(trxLockMode, lockRowId, (*m_tableDef)->m_id);
				// 加TNT事务锁
				if (trx->tryLockRow(trxLockMode, tntScanInfo->m_currentKey->m_rowId, (*m_tableDef)->m_id)) {
					//清除循环加锁的标志
					lockRowId = INVALID_ROW_ID;
					//try事务锁成功后才可以移动游标
					ret = true;
				} else {
					//统计信息
					m_indexStatus.m_numRLRestarts++;

					scanHandle->unlatchNtseRowBoth();
					SYNCHERE(SP_TNT_INDEX_LOCK);
			
					trx->lockRow(trxLockMode, tntScanInfo->m_currentKey->m_rowId, (*m_tableDef)->m_id);
					//循环前记录加锁的rowid
					lockRowId = tntScanInfo->m_currentKey->m_rowId;

					//跨页时要放下一个页面的pin，包括第一次扫描的情况（第一次扫描时没有当前页面）
					if(drsIndexScanInfo->m_pageHandle != drsIndexScanInfo->m_readPageHandle)
						session->unpinPage(&drsIndexScanInfo->m_readPageHandle);

					goto __loop;
				}
			} catch (NtseException &e) {
				// 如果由于发生死锁，锁等待超时等导致加事务锁失败，释放NTSE行锁
				scanHandle->unlatchNtseRowBoth();
				SYNCHERE(SP_TNT_INDEX_LOCK1);
				throw e;
			}
		}
		
		//移动游标
		// 1. 仅仅移动外存
		if(drsMove == true && drsMove != memMove)
			scanHandle->moveDrsIndexKey();
		// 2. 同时移动内外存
		else if(drsMove == true && drsMove == memMove)
			scanHandle->moveMemDrsIndexKey();
		// 3. 仅仅移动内存
		else
			scanHandle->moveMemIndexKey();

		//统计信息
		m_indexStatus.m_rowsScanned++;
		if (!scanHandle->getScanInfo()->m_isForward)
			++m_indexStatus.m_rowsBScanned;

	}
	

	//scanHandle->unlatchNtseRowBoth();
	return ret;
}

/**
 * 结束TNT表的索引范围扫描
 * @param scanHandle
 */
void TNTIndex::endScan(TNTIdxScanHandle *scanHandle) {
	//scanHandle->unlatchNtseRowBoth();
	//scanHandle的内存从MemContext中分配，无须释放
	m_memIndex->endScan(scanHandle->getMemIdxScanHdl());
	m_drsIndex->endScanSecond(scanHandle->getDrsIndexScanHdl());
}

/**
 * 根据键值可以唯一定位的取项操作
 * @param session   会话
 * @param key       查找键
 * @param rowId     OUT 输出找到的索引项的RowId
 * @param subRecord OUT 输出需要提取的子记录，如果为NULL表示只需要获得RowId
 * @param extractor 子记录提取器
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
 * 获取索引的基本统计信息
 * @return 索引基本统计信息
 */
const TNTIndexStatus& TNTIndex::getStatus() {
	return m_indexStatus;
}

/**
 * 清理过期索引项
 * @param session
 * @param readView
 * @return 
 */
u64 TNTIndex::purge(Session *session, const ReadView *readView) {
	return m_memIndex->purge(session, readView);
}

/**
 * 回收索引页面
 * @param session
 * @param hwm
 * @param lwm
 * @return 
 */
void TNTIndex::reclaimIndex(Session *session, u32 hwm, u32 lwm) {
	m_memIndex->reclaimIndex(session, hwm, lwm);
}


/**
 * 回滚第一次索引更新或删除
 * @param session
 * @param record
 * @return 
 */
void TNTIndice::undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record) {
	m_memIndice->undoFirstUpdateOrDeleteIndexEntries(session, record);
}

/**
 * 回滚第二次索引更新
 * @param session
 * @param before
 * @param after
 * @return 
 */
bool TNTIndice::undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) {
	return m_memIndice->undoSecondUpdateIndexEntries(session, before, after);
}

/**
 * 回滚第二次索引删除
 * @param session
 * @param record
 * @return 
 */
void TNTIndice::undoSecondDeleteIndexEntries(Session *session, const Record *record) {
	m_memIndice->undoSecondDeleteIndexEntries(session, record);
}
}