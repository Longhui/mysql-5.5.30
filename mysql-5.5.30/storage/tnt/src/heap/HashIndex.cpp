/** �����ܵ�hash����
 * author �ö��� xindingfeng@corp.netease.com
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

/** ��hash�����в���������
 * @param rowId
 * @param value ָ��TNT���е�ĳ����¼��������id
 * @param version 
 * @param type ������ptrָ������������黹��TNT��
 * return �ɹ�����true��ʧ�ܷ���false
 */
bool HashIndex::insert(RowId rowId, u64 value, RowIdVersion version, HashIndexType type) {
	assert(version > 0);
	assert(type == HIT_MHEAPREC || type == HIT_TXNID);

	bool ret = true;

	u8 index = getMapIndex(rowId);
	m_mapEntries[index]->m_lock.lock(Exclusived, __FILE__, __LINE__);
	//��ʱ�п���purge��һ�׶ν�ntse��¼ɾ�������ڶ��׶�ժ�ڴ�����δ��ʼ��
	//����������ҵ�ɾ����Ŀ���slot����ʱ����rowId����
	assert(m_mapEntries[index]->m_indexMap.get(rowId) == NULL);
	/*HashIndexEntry *entry = m_mapEntries[index]->m_indexMap.remove(rowId);
	if (entry != NULL) {
		assert(entry->m_rowId == rowId);
		m_mapEntries[index]->m_valuePool.free(entry->m_id - 1);
	}*/
	//��Ϊ��hash������Ҳ�����Ӧ��key������ֵΪ0��������ʱû�����ַ���idΪ0�ľ������Ҳ�����Ӧ��key������Ӧkey��Ӧ��valueΪ0
	//�������ǹ涨valueֵ�������0
	HashIndexEntry *entry = NULL;
	u32 id = m_mapEntries[index]->m_valuePool.alloc() + 1;
	entry = &(m_mapEntries[index]->m_valuePool[id - 1]);
	entry->m_id = id;
	entry->m_rowId = rowId;
	entry->m_value = value;
	entry->m_version = version;
	entry->m_type = type;
	ret = m_mapEntries[index]->m_indexMap.put(entry);
	//m_mapEntries[index]->m_valuePool.alloc()һ���ܷ�����ڴ�
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

/** ����rowIdɾ����Ӧ��������
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

/** ����rowIdɾ����Ӧ��������
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

/**���������Ӧ�������������֮��������в���
 * @param rowid ��Ҫ���»��߲�����������rowid
 * @param ptr ��������slotָ��
 * @param version ������İ汾��
 * @param type ������slotָ�������
 * @param succ out �����ΪNULL��succ��ʶinsert/update�ɹ�������succΪNULL�����ʾ�����ķ���ֵ
 * return �����update���򷵻�ԭ���������ptrָ�룬����(�������)����NULL
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
		//updateһ���Ƿ���true��
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
			//page pool �ռ䲻���޷������ҳ��
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

/**������Ӧ����������ptrָ��
 * @param rowid ��Ҫ���»���������rowid
 * @param ptr ������������ptr��ָ��ֵ
 * @param version ���°汾�š��汾�ű�������㣬���Ϊ-1��ʾ�����°汾��
 * @param type �������������ͣ����ΪHIT_NONE����ʾ������
 * return ԭ���������ptrָ��
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

/** ����rowId������Ӧ��������
 * @param rowId
 * @param ctx Ϊ�������������ռ��������
 * return ������
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

/** ����rowId��ȡĳ��map�����
 * @param rowId
 * return ĳ��map�����
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
		//������ʱ�����������
		//m_mapEntries[i]->m_lock.lock(Shared, __FILE__, __LINE__);
		stats->push(m_mapEntries[i]->m_stat);
		//m_mapEntries[i]->m_lock.unlock(Shared);
	}
}

/** ����hash���������insert��������������ɼ������Խ�hash������ɾ���Խ�Լ�ڴ�
 * @param minReadView ����hash���������С����id
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
			//���size������Ҫ�����ĸ�������ôֻ�������е�traverseCnt��
			if (size > traverseCnt) {
				startPos = System::random()%size;
			} else { //���sizeС����Ҫ�����ĸ�������ֻ��Ҫ����һ�ξ͹���
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

			//���л���hash���������
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
