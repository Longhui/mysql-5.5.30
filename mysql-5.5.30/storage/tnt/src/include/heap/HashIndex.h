/**
 * 实现内存堆的hash索引
 */
#ifndef _TNT_HASHINDEX_H_
#define _TNT_HASHINDEX_H_

#include "util/Hash.h"
#include "misc/TNTIMPageManager.h"
#include "misc/MemCtx.h"
#include "misc/DoubleChecker.h"
using namespace ntse;

namespace tnt {

enum HashIndexType {
	HIT_NONE = 0,
	HIT_TXNID,
	HIT_MHEAPREC
};

//具体的索引实体
struct HashIndexEntry {
	HashIndexEntry() {}

	HashIndexEntry(u64 rowId, u64 value, RowIdVersion version, HashIndexType type) {
		m_rowId = rowId;
		m_value = value;
		m_version = version;
		m_type = type;
	}

	u32              m_id;
	RowId			 m_rowId;  //记录rowId
	u64  		     m_value;   //指向内存堆或者事务id
	RowIdVersion	 m_version;//主要用于del项被purge到外存后，内存索引项还未被摘掉，而此时rowId被重用导致不正确结果
	HashIndexType    m_type;   //标识value指向内存堆还是事务id
};

class HashIndexEntryHasher {
public:
	inline unsigned int operator()(const HashIndexEntry *entry) const {
		return RidHasher::hashCode(entry->m_rowId);
	}
};

template<typename T1, typename T2>
class HashIndexEqualer {
public:
	inline bool operator()(const T1 &v1, const T2 &v2) const {
		return equals(v1, v2);
	}

private:
	static bool equals(const RowId &rid, const HashIndexEntry* entry) {
		return rid == entry->m_rowId;
	}
};

struct HashIndexStat {
	u64 m_count;     //HashIndexMap有多少个hashIndexEntry
	u64 m_trxIdCnt;  //HashIndexMap存在事务id的项数
	u64 m_readCnt;   //HashIndexMap中进行了多少次读操作
	u64 m_insertCnt; //HashIndexMap中进行了多少次insert操作
	u64 m_updateCnt; //HashIndexMap中进行了多少次update操作
	u64 m_removeCnt; //HashIndexMap中进行了多少次remove操作
	u64 m_defragCnt; //HashIndexMap在defrag时摘除了多少记录
};

typedef DynHash<RowId, HashIndexEntry*, HashIndexEntryHasher, RidHasher, HashIndexEqualer<RowId, HashIndexEntry*> > DynHashIndex;

struct HashIndexMap {
	DynHashIndex                  m_indexMap; //存储hash索引
	ObjectPool<HashIndexEntry>    m_valuePool;  //map中所有的value都存储于此pool中
	RWLock                        m_lock;       //保护map
	HashIndexStat                 m_stat;
	
	HashIndexMap(): m_lock("HashIndexMap", __FILE__, __LINE__) {}

	HashIndexMap(TNTIMPageManager *pageManager, PageType pageType): m_indexMap(pageManager, pageType),
		m_valuePool(pageManager, pageType, true), m_lock("HashIndexMap", __FILE__, __LINE__) {
			memset(&m_stat, 0, sizeof(HashIndexStat));
	}

	~HashIndexMap() {
	}

	void clear(bool needLock = true);
};

class HashIndexOperPolicy {
public:
	virtual bool insert(RowId rowId, u64 value, RowIdVersion version, HashIndexType type) = 0;
	virtual bool remove(RowId rowId) = 0;
	virtual bool remove(RowId rid, HashIndexType type, u64 value) = 0;
	virtual u64  insertOrUpdate(RowId rowId, u64 value, RowIdVersion version = 0, HashIndexType type = HIT_NONE, bool *succ = NULL) = 0;
	virtual bool update(RowId rowId, u64 value, RowIdVersion version = 0, HashIndexType type = HIT_NONE) = 0;
	virtual HashIndexEntry* get(RowId rowId, MemoryContext *ctx) = 0;
	virtual ~HashIndexOperPolicy() {};
};

class HashIndex: public HashIndexOperPolicy, public DoubleChecker
{
public:
	HashIndex(TNTIMPageManager  *pageManager);
	bool  insert(RowId rowId, u64 value, RowIdVersion version, HashIndexType type);
	bool  remove(RowId rowId);
	bool  remove(RowId rid, HashIndexType type, u64 value);
	u64   insertOrUpdate(RowId rowId, u64 value, RowIdVersion version = 0, HashIndexType type = HIT_NONE, bool *succ = NULL);
	bool  update(RowId rowId, u64 value, RowIdVersion version = 0, HashIndexType type = HIT_NONE);
	HashIndexEntry* get(RowId rowId, MemoryContext *ctx);

	bool isRowIdValid(RowId rowId, RowIdVersion version) const;
	u32 defrag(const Session *session, u16 tableId, const TrxId minReadView, u32 maxTraverseCnt) const;
	void clear();
	~HashIndex(void);

	void getHashIndexStats(Array<HashIndexStat> *stats);
	u64  getSize(bool safe = true);
#ifndef NTSE_UNIT_TEST
private:
#endif
	u8 getMapIndex(RowId rowId) const;

	HashIndexMap   **m_mapEntries;

	static const u8  HASHINDEXMAP_SIZE = 32;  //hash索引中map的总个数
	static const u8  HASHINDEXMAP_MASK = (HASHINDEXMAP_SIZE - 1);
	static const u32 DEFRAG_HASHINDEX_TRAVERSECNT = 1000; //defrag时每个hash需要遍历的个数
	static const u32 OPTIMIZE_SIZE_PER_MAP = 20000;       //每个map中最优的大小

	friend class TNTTblMntAlterColumn;
};
}
#endif
