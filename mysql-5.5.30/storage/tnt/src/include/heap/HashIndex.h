/**
 * ʵ���ڴ�ѵ�hash����
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

//���������ʵ��
struct HashIndexEntry {
	HashIndexEntry() {}

	HashIndexEntry(u64 rowId, u64 value, RowIdVersion version, HashIndexType type) {
		m_rowId = rowId;
		m_value = value;
		m_version = version;
		m_type = type;
	}

	u32              m_id;
	RowId			 m_rowId;  //��¼rowId
	u64  		     m_value;   //ָ���ڴ�ѻ�������id
	RowIdVersion	 m_version;//��Ҫ����del�purge�������ڴ������δ��ժ��������ʱrowId�����õ��²���ȷ���
	HashIndexType    m_type;   //��ʶvalueָ���ڴ�ѻ�������id
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
	u64 m_count;     //HashIndexMap�ж��ٸ�hashIndexEntry
	u64 m_trxIdCnt;  //HashIndexMap��������id������
	u64 m_readCnt;   //HashIndexMap�н����˶��ٴζ�����
	u64 m_insertCnt; //HashIndexMap�н����˶��ٴ�insert����
	u64 m_updateCnt; //HashIndexMap�н����˶��ٴ�update����
	u64 m_removeCnt; //HashIndexMap�н����˶��ٴ�remove����
	u64 m_defragCnt; //HashIndexMap��defragʱժ���˶��ټ�¼
};

typedef DynHash<RowId, HashIndexEntry*, HashIndexEntryHasher, RidHasher, HashIndexEqualer<RowId, HashIndexEntry*> > DynHashIndex;

struct HashIndexMap {
	DynHashIndex                  m_indexMap; //�洢hash����
	ObjectPool<HashIndexEntry>    m_valuePool;  //map�����е�value���洢�ڴ�pool��
	RWLock                        m_lock;       //����map
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

	static const u8  HASHINDEXMAP_SIZE = 32;  //hash������map���ܸ���
	static const u8  HASHINDEXMAP_MASK = (HASHINDEXMAP_SIZE - 1);
	static const u32 DEFRAG_HASHINDEX_TRAVERSECNT = 1000; //defragʱÿ��hash��Ҫ�����ĸ���
	static const u32 OPTIMIZE_SIZE_PER_MAP = 20000;       //ÿ��map�����ŵĴ�С

	friend class TNTTblMntAlterColumn;
};
}
#endif
