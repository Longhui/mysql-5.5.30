/** 高性能的hash索引
 * author 忻丁峰 xindingfeng@corp.netease.com
 */
#include "heap/HashIndex.h"
#include "misc/Session.h"
#include "trx/TNTTransaction.h"
#include "misc/TableDef.h"

namespace tnt {
void HashIndexMap::clear(bool needLock /*=true*/) {
	if (needLock) {
		m_lock.lock(Exclusived, __FILE__, __LINE__);
	}
	m_indexMap.clear();
	m_valuePool.clear();
	memset(&m_stat, 0, sizeof(HashIndexStat));
	if (needLock) {
		m_lock.unlock(Exclusived);
	}
}

HashIndex::HashIndex(TNTIMPageManager *pageManager)
{
	m_mapEntries = new HashIndexMap*[HASHINDEXMAP_SIZE];
	for (int i = 0; i < HASHINDEXMAP_SIZE; i++) {
		m_mapEntries[i] = new HashIndexMap(pageManager, PAGE_HASH_INDEX);
	}
}

HashIndex::~HashIndex(void)
{
	for (int i = 0; i < HASHINDEXMAP_SIZE; i++) {
		delete m_mapEntries[i];
	}
	delete[] m_mapEntries;
	m_mapEntries = NULL;
}

void HashIndex::clear() {
	for (int i = 0; i < HASHINDEXMAP_SIZE; i++) {
		m_mapEntries[i]->clear();
	}
}

/** 向hash索引中插入索引项
 * @param rowId
 * @param value 指向TNT堆中的某个记录或者事务id
 * @param version 
 * @param type 决定了ptr指向的是事务数组还是TNT堆
 * return 成功返回true，失败返回false
 */
bool HashIndex::insert(RowId rowId, u64 value, RowIdVersion version, HashIndexType type) {
	assert(version > 0);
	assert(type == HIT_MHEAPREC || type == HIT_TXNID);

	bool ret = true;

	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Exclusived, __FILE__, __LINE__);
	//此时有可能purge第一阶段将ntse记录删除，但第二阶段摘内存数据未开始。
	//但插入可能找到删除项的空闲slot，此时便是rowId重用
	assert(m_mapEntries[index]->m_indexMap.get(rowId) == NULL);
	/*HashIndexEntry *entry = m_mapEntries[index]->m_indexMap.remove(rowId);
	if (entry != NULL) {
		assert(entry->m_rowId == rowId);
		m_mapEntries[index]->m_valuePool.free(entry->m_id - 1);
	}*/
	//因为在hash中如果找不到相应的key，返回值为0，所以这时没法区分返回id为0的究竟是找不到相应的key还是相应key对应的value为0
	//这里我们规定value值必须大于0
	HashIndexEntry *entry = NULL;
	u32 id = m_mapEntries[index]->m_valuePool.alloc() + 1;
	entry = &(m_mapEntries[index]->m_valuePool[id - 1]);
	entry->m_id = id;
	entry->m_rowId = rowId;
	entry->m_value = value;
	entry->m_version = version;
	entry->m_type = type;
	ret = m_mapEntries[index]->m_indexMap.put(entry);
	//m_mapEntries[index]->m_valuePool.alloc()一定能分配出内存
	if (!ret) {
		m_mapEntries[index]->m_valuePool.free(id - 1);
	} else {
		m_mapEntries[index]->m_stat.m_count++;
		m_mapEntries[index]->m_stat.m_insertCnt++;

		if (HIT_TXNID == type) {
			m_mapEntries[index]->m_stat.m_trxIdCnt++;
		}
	}
	m_mapEntries[index]->m_lock.unlock(Exclusived);

	return ret;
}

/** 根据rowId删除相应的索引项
 * @param rowId
 */
bool HashIndex::remove(RowId rowId) {
	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Exclusived, __FILE__, __LINE__);
	HashIndexEntry *entry = m_mapEntries[index]->m_indexMap.remove(rowId);
	if (entry != NULL) {
		assert(entry->m_rowId == rowId);
		if (HIT_TXNID == entry->m_type) {
			m_mapEntries[index]->m_stat.m_trxIdCnt--;
		}
		m_mapEntries[index]->m_valuePool.free(entry->m_id - 1);
		m_mapEntries[index]->m_stat.m_count--;
		m_mapEntries[index]->m_stat.m_removeCnt++;
	}
	m_mapEntries[index]->m_lock.unlock(Exclusived);
	if (!entry) {
		return false;
	} else {
		return true;
	}
}

/** 根据rowId删除相应的索引项
 * @param rowId
 */
bool HashIndex::remove(RowId rowId, HashIndexType type, u64 value) {
	bool succ = false;
	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Exclusived, __FILE__, __LINE__);
	HashIndexEntry *entry = m_mapEntries[index]->m_indexMap.remove(rowId);
	if (entry != NULL && entry->m_type == type && entry->m_value == value) {
		assert(entry->m_rowId == rowId);
		if (HIT_TXNID == entry->m_type) {
			m_mapEntries[index]->m_stat.m_trxIdCnt--;
		}
		m_mapEntries[index]->m_valuePool.free(entry->m_id - 1);
		m_mapEntries[index]->m_stat.m_count--;
		m_mapEntries[index]->m_stat.m_removeCnt++;
		succ = true;
	} else {
		assert(false);
	}
	m_mapEntries[index]->m_lock.unlock(Exclusived);
	return succ;
}

/**如果存在相应的索引项则更新之，否则进行插入
 * @param rowid 需要更新或者插入的索引项的rowid
 * @param ptr 索引项中slot指针
 * @param version 索引项的版本号
 * @param type 索引项slot指针的类型
 * @param succ out 如果不为NULL，succ标识insert/update成功与否。如果succ为NULL，则表示不关心返回值
 * return 如果是update，则返回原有索引项的ptr指针，否则(插入操作)返回NULL
 */
u64 HashIndex::insertOrUpdate(RowId rowId, u64 value, RowIdVersion version, HashIndexType type, bool *succ) {
	u64 srcVal = 0;
	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Exclusived, __FILE__, __LINE__);
	HashIndexEntry *entry = m_mapEntries[index]->m_indexMap.get(rowId);
	if (entry != NULL) {
		assert(entry->m_rowId == rowId);
		srcVal = entry->m_value;
		entry->m_value = value;
		if (version > 0) {
			entry->m_version = version;
		}

		if (type == HIT_MHEAPREC || type == HIT_TXNID) {
			if (entry->m_type == HIT_TXNID) {
				m_mapEntries[index]->m_stat.m_trxIdCnt--;
			}
			entry->m_type = type;
			if (type == HIT_TXNID) {
				m_mapEntries[index]->m_stat.m_trxIdCnt++;
			}
		}
		//update一定是返回true的
		if (succ != NULL) {
			*succ = true;
		}
		m_mapEntries[index]->m_stat.m_updateCnt++;
	} else {
		bool ret = true;
		assert(version > 0);
		assert(type == HIT_MHEAPREC || type == HIT_TXNID);
		u32 id = m_mapEntries[index]->m_valuePool.alloc(false) + 1;
		if(id == 0) {
			//page pool 空间不足无法分配出页面
			ret = false;
			goto _END_;
		}
		entry = &(m_mapEntries[index]->m_valuePool[id - 1]);
		entry->m_id = id;
		entry->m_rowId = rowId;
		entry->m_value = value;
		entry->m_version = version;
		entry->m_type = type;
		ret = m_mapEntries[index]->m_indexMap.put(entry);
		if (!ret) {
			m_mapEntries[index]->m_valuePool.free(id - 1);
		} else {
			m_mapEntries[index]->m_stat.m_count++;
			m_mapEntries[index]->m_stat.m_insertCnt++;

			if (HIT_TXNID == type) {
				m_mapEntries[index]->m_stat.m_trxIdCnt++;
			}
		}
_END_:
		if (succ != NULL) {
			*succ = ret;
		}
	}
	m_mapEntries[index]->m_lock.unlock(Exclusived);
	return srcVal;
}

/**更新相应的索引项中ptr指针
 * @param rowid 需要更新或的索引项的rowid
 * @param ptr 更新索引项中ptr的指针值
 * @param version 更新版本号。版本号必须大于零，如果为-1表示不更新版本号
 * @param type 更新索引项类型，如果为HIT_NONE，表示不更新
 * return 原有索引项的ptr指针
 */
bool HashIndex::update(RowId rowId, u64 value, RowIdVersion version, HashIndexType type) {
	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Exclusived, __FILE__, __LINE__);
	HashIndexEntry *entry = m_mapEntries[index]->m_indexMap.get(rowId);
	if (likely(entry != NULL)) {
		assert(entry->m_rowId == rowId);
		entry->m_value = value;
		if (version > 0) {
			entry->m_version = version;
		}

		if (type == HIT_MHEAPREC || type == HIT_TXNID) {
			if (entry->m_type == HIT_TXNID) {
				m_mapEntries[index]->m_stat.m_trxIdCnt--;
			}
			entry->m_type = type;
			if (type == HIT_TXNID) {
				m_mapEntries[index]->m_stat.m_trxIdCnt++;
			}
		}
		m_mapEntries[index]->m_stat.m_updateCnt++;
	} else {
		assert(false);
	}
	m_mapEntries[index]->m_lock.unlock(Exclusived);
	return true;
}

/** 根据rowId返回相应的索引项
 * @param rowId
 * @param ctx 为返回索引项分配空间的上下文
 * return 索引项
 */
HashIndexEntry* HashIndex::get(RowId rowId, MemoryContext *ctx) {
	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Shared, __FILE__, __LINE__);
	HashIndexEntry *entry = NULL;
	HashIndexEntry *ptr = m_mapEntries[index]->m_indexMap.get(rowId);
	if (likely(ptr != NULL)) {
		assert(ptr->m_rowId == rowId);
		entry = (HashIndexEntry *)ctx->alloc(sizeof(HashIndexEntry));
		memcpy(entry, ptr, sizeof(HashIndexEntry));
	}
	m_mapEntries[index]->m_stat.m_readCnt++;
	m_mapEntries[index]->m_lock.unlock(Shared);

	return entry;
}

/** 根据rowId获取某个map的序号
 * @param rowId
 * return 某个map的序号
 */
u8 HashIndex::getMapIndex(RowId rowId) const {
	u64 pageNum = RID_GET_PAGE(rowId);
	//return ((u8)rowId) & HASHINDEXMAP_MASK;
	return ((u8)pageNum) & HASHINDEXMAP_MASK;
}

bool HashIndex::isRowIdValid(RowId rowId, RowIdVersion version) const {
	bool ret = false;
	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Shared, __FILE__, __LINE__);
	HashIndexEntry *entry = m_mapEntries[index]->m_indexMap.get(rowId);
	if (entry != NULL && entry->m_version == version) {
		ret = true;
	}
	m_mapEntries[index]->m_stat.m_readCnt++;
	m_mapEntries[index]->m_lock.unlock(Shared);

	return ret;
}

void HashIndex::getHashIndexStats(Array<HashIndexStat> *stats) {
	for (int i = 0; i < HASHINDEXMAP_SIZE; i++) {
		//采样暂时不加锁，脏读
		//m_mapEntries[i]->m_lock.lock(Shared, __FILE__, __LINE__);
		stats->push(m_mapEntries[i]->m_stat);
		//m_mapEntries[i]->m_lock.unlock(Shared);
	}
}

/** 回收hash索引项，对于insert项，如果对所有事务可见，可以将hash索引项删除以节约内存
 * @param minReadView 回收hash索引项的最小事务id
 */
u32 HashIndex::defrag(const Session *session, u16 tableId, const TrxId minReadView, u32 maxTraverseCnt) const {
	assert(TableDef::INVALID_TABLEID != tableId);
	u32 ret = 0;
	u32 cnt = 0;
	u32 realCnt = 0;
	u32 repeatPerMap = maxTraverseCnt/(HASHINDEXMAP_SIZE*DEFRAG_HASHINDEX_TRAVERSECNT);
	repeatPerMap = (repeatPerMap == 0? 1: repeatPerMap);
	u32 traverseCnt = DEFRAG_HASHINDEX_TRAVERSECNT;
	HashIndexEntry *entry = NULL;
	RowId *rids = (RowId *)alloca(traverseCnt*sizeof(RowId));
	size_t size = 0;
	for (u8 idx = 0; idx < HASHINDEXMAP_SIZE; idx++) {
		m_mapEntries[idx]->m_lock.lock(Shared, __FILE__, __LINE__);
		u64 mapCnt = m_mapEntries[idx]->m_stat.m_count;
		if (mapCnt > OPTIMIZE_SIZE_PER_MAP && mapCnt <= (m_mapEntries[idx]->m_stat.m_trxIdCnt << 1)) {
			repeatPerMap = max(repeatPerMap, (u32)(mapCnt/DEFRAG_HASHINDEX_TRAVERSECNT));
		}
		m_mapEntries[idx]->m_lock.unlock(Shared);
		for (u32 repeat = 0; repeat < repeatPerMap; repeat++) {
			size_t startPos = 0;
			bool   defragNext = false;
			cnt = 0;
			RWLockGuard guard(&m_mapEntries[idx]->m_lock, Exclusived, __FILE__, __LINE__);
			size = m_mapEntries[idx]->m_indexMap.getSize();
			if (size == 0) {
				break;
			}
			memset(rids, 0, traverseCnt*sizeof(RowId));
			realCnt = 0;
			//如果size大于需要遍历的个数，那么只遍历其中的traverseCnt个
			if (size > traverseCnt) {
				startPos = System::random()%size;
			} else { //如果size小于需要遍历的个数，则只需要遍历一次就够了
				defragNext = true;
			}

			u32 total = min((const u32)size, traverseCnt);
			for (cnt = 0; cnt < total; cnt++) {
				entry = m_mapEntries[idx]->m_indexMap.getAt(startPos++);
				if (entry->m_type == HIT_TXNID && (TrxId)entry->m_value < minReadView 
					&& !session->getTrans()->pickRowLock(entry->m_rowId, tableId)) {
					rids[realCnt++] = entry->m_rowId;
				}
				if (unlikely(startPos == size)) {
					startPos = 0;
				}
			}

			//进行回收hash索引项操作
			for (u32 i = 0; i < realCnt; i++) {
				entry = m_mapEntries[idx]->m_indexMap.remove(rids[i]);
				assert(entry != NULL);
				assert(entry->m_rowId == rids[i]);
				m_mapEntries[idx]->m_valuePool.free(entry->m_id - 1);
				m_mapEntries[idx]->m_stat.m_count--;
				m_mapEntries[idx]->m_stat.m_trxIdCnt--;
				m_mapEntries[idx]->m_stat.m_defragCnt++;
			}
			ret += realCnt;

			if (defragNext)
				break;
		}
	}

	return ret;
}

u64 HashIndex::getSize(bool safe) {
	u64 count = 0;
	for (u8 i = 0; i < HASHINDEXMAP_SIZE; i++) {
		if (safe)
			m_mapEntries[i]->m_lock.lock(Shared, __FILE__, __LINE__);
		count += m_mapEntries[i]->m_indexMap.getSize();
		if (safe)
			m_mapEntries[i]->m_lock.unlock(Shared);
	}

	return count;
}
}
