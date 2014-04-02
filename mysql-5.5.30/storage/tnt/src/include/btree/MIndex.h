/**
* �ڴ������ӿ�
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _TNT_M_INDEX_
#define _TNT_M_INDEX_

#include "api/TNTDatabase.h"
#include "btree/IndexCommon.h"
#include "misc/Session.h"
#include "misc/TableDef.h"
#include "misc/Record.h"
#include "misc/DoubleChecker.h"

using namespace ntse;

namespace tnt {

class TNTDatabase;
class MIndex;


/** ��������ͳ����Ϣ����һ����ȷ�� */
struct MIndexStatus {
//	DBObjStats	*m_dboStats;	/** �������ݿ�����е�ͳ����Ϣ */
	u64	m_dataLength;			/** ������ռ�õ����ݴ�С����λ�ֽ��� */
	u64 m_freeLength;			/** ��ռ�ÿռ��п��е����ݴ�С����λ�ֽ��� */
	u64	m_numInsert;			/** ��¼������� */
	u64	m_numDelete;			/** ��¼ɾ������ */
 	u64	m_numScans;				/** ɨ����� */
	u64	m_rowsScanned;			/** ɨ��������� */
	u64	m_backwardScans;		/** ����ɨ����� */
	u64	m_rowsBScanned;			/** ����ɨ��������� */
	u64	m_numSplit;				/** ҳ����Ѵ��� */
	u64	m_numMerge;				/** ҳ��ϲ����� */
	u64 m_numRedistribute;		/** ҳ���ط������*/
	u64 m_numRestarts;		/** �����Ĵ��� */
	u64 m_numLatchesConflicts;	/** ��Latch���ֳ�ͻ�Ĵ��� */
	u64 m_numRepairUnderflow; /** �޸�����ҳ��Ĵ��� */
	u64 m_numRepairOverflow;	/** �޸�����ҳ��Ĵ��� */
	u64 m_numIncreaseTreeHeight;/** �������߶Ȳ����Ĵ��� */
	u64 m_numDecreaseTreeHeight;/** �������߶Ȳ����Ĵ��� */
	Atomic<long> m_numAllocPage;			/** ����ҳ����ҳ���� */
	Atomic<long> m_numFreePage;			/** �ͷ�ҳ��ҳ���� */
};


/** ������ɼ�����Ϣ */
class KeyMVInfo {
public:
	bool m_visable; /** �Ƿ��ͨ������ҳ��ֱ���жϿɼ��У����������ĸ����Ϊfalse */
	u8   m_delBit;  /** ������ɾ����־λ�����������ĸ����Ϊ0 */
	RowIdVersion m_version; /** ������RowId�汾�����������ĸ����Ϊ0 */
	bool m_ntseReturned;	/** �������Ǵ�NTSE�������з��ظ���Ϊtrue�������TNT������Ϊfalse*/
};

/** DRS����ɨ���� */
class MIndexScanHandle {
public:
	/**
	 * ���ص�ǰ�е�RID
	 *
	 * @return ��ǰ�е�RID
	 */
	virtual RowId getRowId() const = 0;
	virtual ~MIndexScanHandle() {}
};

/**
* �ڴ������������ӿ�
*/
class MIndice {
public:
	virtual ~MIndice() {}
	// ���������ӿ�
	static MIndice* open(TNTDatabase *db, Session *session, uint indexNum, TableDef **tableDef, LobStorage *lobStorage, 
		const DoubleChecker *doubleChecker);
	virtual bool init(Session *session) = 0;
	virtual void close(Session *session) = 0;

	virtual void deleteIndexEntries(Session *session, const Record *record, RowIdVersion version) = 0;
	virtual bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) = 0;
	virtual bool insertIndexEntries(Session *session, SubRecord *after, RowIdVersion version) = 0;

	virtual void undoFirstUpdateOrDeleteIndexEntries(Session *session, const Record *record) = 0;
	virtual bool undoSecondUpdateIndexEntries(Session *session, const SubRecord *before, SubRecord *after) = 0;
	virtual void undoSecondDeleteIndexEntries(Session *session, const Record *record) = 0;

	virtual MIndex* createIndex(Session *session, const IndexDef *def, u8 indexId) = 0;
	virtual void dropIndex(Session *session, uint idxNo) = 0;

	virtual void setTableDef(TableDef **tableDef) = 0;
	virtual uint getIndexNum() const = 0;
	virtual MIndex* getIndex(uint index) const = 0;
	virtual u64 getMemUsed(bool includeMeta = true) = 0;
	virtual void setLobStorage(LobStorage *lobStorage) = 0;

	virtual void setDoubleChecker(const DoubleChecker *doubleChecker) = 0;
};

/**
 * �ڴ���������ӿ�
 */
class MIndex : public IndexBase {
public:
	virtual ~MIndex() {}

	static MIndex* open(TNTDatabase *db, Session *session, MIndice *mIndice, TableDef **tableDef, 
		const IndexDef *indexDef, u8 indexId, const DoubleChecker *doubleChecker);
	virtual bool init(Session *session, bool waitSuccess) = 0;
	virtual void close(Session *session) = 0;

	virtual void insert(Session *session, const SubRecord *key, RowIdVersion version) = 0;
	virtual bool delByMark(Session *session, const SubRecord *key, RowIdVersion *version) = 0;

	virtual void delRec(Session *session, const SubRecord *key, RowIdVersion *version) = 0;
	virtual bool undoDelByMark(Session *session, const SubRecord *key, RowIdVersion version) = 0;

// 	virtual bool getByUniqueKey(Session *session, const SubRecord *key, RowId *rowId, 
// 		SubRecord *subRecord, SubToSubExtractor *extractor) = 0;

#ifdef TNT_INDEX_OPTIMIZE
	virtual MIndexScanHandle* beginScanFast(Session *session, const SubRecord *key, 
		bool forward, bool includeKey) = 0;
	virtual bool getNextFast(MIndexScanHandle *scanHandle) = 0;
	virtual void endScanFast(MIndexScanHandle *scanHandle) = 0;
#endif
	virtual void setTableDef(TableDef **tableDef) = 0;
	virtual void setIndexDef(const IndexDef *indexDef) = 0;
	virtual u8 getIndexId() const = 0;
	virtual MIndexScanHandle* beginScan(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, LockMode ntseRlMode, RowLockHandle **rowHdl, TLockMode trxLockMode) = 0;
	virtual bool getNext(MIndexScanHandle *scanHandle) throw(NtseException) = 0;
	virtual void endScan(MIndexScanHandle *scanHandle) = 0;

	virtual u64 recordsInRange(Session *session, const SubRecord *min, bool includeKeyMin, 
		const SubRecord *max, bool includeKeyMax) = 0;

	virtual u64 purge(Session *session, const ReadView *readView) = 0;
	virtual void reclaimIndex(Session *session, u32 hwm, u32 lwm) = 0;
	virtual bool checkDuplicate(Session *session, const SubRecord *key) = 0;

	virtual const MIndexStatus& getStatus() = 0;
};

}

#endif