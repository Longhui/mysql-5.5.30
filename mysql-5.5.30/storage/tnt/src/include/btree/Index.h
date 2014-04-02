/**
* ��������ӿ�
*
* @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
*/

#ifndef _NTSE_INDEX_H_
#define _NTSE_INDEX_H_

#include "btree/IndexCommon.h"
#include "misc/Global.h"
#include "util/Sync.h"
#include "util/PagePool.h"
#include "misc/Buffer.h"
#include "misc/Sample.h"
#include "misc/Session.h"

#ifdef TNT_ENGINE
#include "trx/TLock.h"
#endif

namespace ntse {

class Session;
class Database;
class TableDef;
class IndexDef;
class DrsHeap;
class Record;
class SubRecord;
class File;
class DrsIndex;
class MemoryContext;
class IndexScanHandle;
class RowLockHandle;
class OrderedIndices;
class DrsIndexScanHandleInfo;

/** ��������ͳ����Ϣ����һ����ȷ�� */
struct IndexStatus {
	DBObjStats	*m_dboStats;	/** �������ݿ�����е�ͳ����Ϣ */
	u64	m_dataLength;			/** ������ռ�õ����ݴ�С����λ�ֽ��� */
	u64 m_freeLength;			/** ��ռ�ÿռ��п��е����ݴ�С����λ�ֽ��� */
	u64	m_numInsert;			/** ��¼������� */
	u64	m_numUpdate;			/** ��¼���´��� */
	u64	m_numDelete;			/** ��¼ɾ������ */
	u64	m_numScans;				/** ɨ����� */
	u64	m_rowsScanned;			/** ɨ��������� */
	u64	m_backwardScans;		/** ����ɨ����� */
	u64	m_rowsBScanned;			/** ����ɨ��������� */
	u64	m_numSplit;				/** ҳ����Ѵ��� */
	u64	m_numMerge;				/** ҳ��ϲ����� */
	u64 m_numRLRestarts;		/** ���������ֳ�ͻ�Ĵ��� */
	u64 m_numILRestartsForI;	/** ������ҳ�������ֳ�ͻ�Ĵ�������ͻ���ɲ���������� */
	u64 m_numILRestartsForD;	/** ������ҳ�������ֳ�ͻ�Ĵ�������ͻ����ɾ���������� */
	u64 m_numLatchesConflicts;	/** ��Latch���ֳ�ͻ�Ĵ��� */
	Atomic<long> m_numDeadLockRestarts;	/** DML���̵��г���������������ҪrestartDML�޸ĵ�ͳ�ƴ��� */
};

/** ������չͳ����Ϣ��ͨ��������ã�����ȷ�� */
struct IndexStatusEx {
	double	m_pctUsed;			/** ҳ�������� */
	double	m_compressRatio;	/** ѹ���ȣ�ѹ����/ѹ��ǰ��С֮�ȣ� */
	static const int m_fieldNum = 3;/** ��ͳ����Ϣ������ */
};

/** DRS����ɨ���� */
class IndexScanHandle {
public:
	/**
	 * ���ص�ǰ�е�RID
	 *
	 * @return ��ǰ�е�RID
	 */
	virtual RowId getRowId() const = 0;
	virtual ~IndexScanHandle() {}
};

/** ��������һ���������DRS���� */
class DrsIndice {
public:
	virtual ~DrsIndice() {};

	// ���������ӿ�
	static void create(const char *path, const TableDef *tableDef) throw(NtseException);
	static DrsIndice* open(Database *db, Session *session, const char *path, const TableDef *tableDef, LobStorage *lobStorage) throw(NtseException);
	static void drop(const char *path) throw(NtseException);

	virtual void close(Session *session, bool flushDirty) = 0;
	virtual void flush(Session *session) = 0;

	virtual bool insertIndexEntries(Session *session, const Record *record, uint *dupIndex) = 0;
#ifdef TNT_ENGINE
	virtual void insertIndexNoDupEntries(Session *session, const Record *record) = 0;
#endif
	virtual void deleteIndexEntries(Session *session, const Record *record, IndexScanHandle* scanHandle) = 0;
	virtual bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool updateLob, uint *dupIndex) = 0;
	virtual void dropPhaseOne(Session *session, uint idxNo) = 0;
	virtual void dropPhaseTwo(Session *session, uint idxNo) = 0;
	virtual void createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, 
		DrsHeap *heap) throw(NtseException) = 0;
	virtual DrsIndex* createIndexPhaseTwo(const IndexDef *def) = 0;

	virtual DrsIndex* getIndex(uint index) = 0;
	virtual int getFiles(File** files, PageType* pageTypes, int numFile) = 0;
	virtual uint getIndexNum() const = 0;
	virtual uint getUniqueIndexNum() const = 0;
	virtual u64 getDataLength(bool includeMeta = true) = 0;
	virtual int getIndexNo(const BufferPageHdr *page, u64 pageId) = 0;
	virtual const OrderedIndices* getOrderedIndices() const { return NULL; };
	virtual LobStorage* getLobStorage() = 0;

	// �ָ��ӿ�
	// redo
	static void redoCreate(const char *path, const TableDef *tableDef) throw(NtseException);
	virtual void redoCreateIndexEnd(const byte *log, uint size) = 0;		// ����������������Ҫͬ���ڴ���ļ����е�����״̬
	virtual s32 redoDropIndex(Session *session, u64 lsn, const byte *log, uint size) = 0;			// ����һ��ָ����������־��¼������ 

	virtual void redoDML(Session *session, u64 lsn, const byte *log, uint size) = 0;
	virtual void redoSMO(Session *session, u64 lsn, const byte *log, uint size) = 0;		// ����ҳ��ϲ��ͷ���
	virtual void redoPageSet(Session *session, u64 lsn, const byte *log, uint size) = 0;	// ����SMOλ�����Լ�ҳ���ʼ����λͼҳ��ͷҳ��ʹ�ü�¼
	virtual bool isIdxDMLSucceed(const byte *log, uint size) = 0;
	virtual u8 getLastUpdatedIdxNo(const byte *log, uint size) = 0;

	// undo
	virtual void undoDML(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;
	virtual void undoSMO(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;
	virtual void undoPageSet(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;	// ����SMOλ�����Լ�ҳ���ʼ��
	virtual void undoCreateIndex(Session *session, u64 lsn, const byte *log, uint size, bool logCPST) = 0;

	// redo������־
	virtual void redoCpstDML(Session *session, u64 lsn, const byte *log, uint size) = 0;
	virtual void redoCpstCreateIndex(Session *session, u64 lsn, const byte *log, uint size) = 0;
	virtual void redoCpstSMO(Session *session, u64 lsn, const byte *log, uint size) = 0;		// ����ҳ��ϲ��ͷ���
	virtual void redoCpstPageSet(Session *session, u64 lsn, const byte *log, uint size) = 0;	// ����SMOλ�����Լ�ҳ���ʼ����λͼҳ��ͷҳ��

	virtual DBObjStats* getDBObjStats() = 0;

	// ����ָ��ӿ�
	virtual void recvInsertIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) = 0;
	virtual void recvDeleteIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN) = 0;
	virtual void recvUpdateIndexEntries(Session *session, const SubRecord *before, const SubRecord *after, 
		s16 lastDoneIdxNo, u64 beginLSN, bool isUpdateLob) = 0;
	virtual u8 recvCompleteHalfUpdate(Session *session, const SubRecord *record, s16 lastDoneIdxNo, 
		u16 updateIdxNum, u16 *updateIndices, bool isUpdateLob) = 0;

	virtual void logDMLDoneInRecv(Session *session, u64 beginLSN, bool succ = false) = 0;
	virtual void getUpdateIndices(MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, 
		u16 **updateIndices, u16 *updateUniques) = 0;

	static const uint IDX_MAX_KEY_LEN = Limits::PAGE_SIZE / 3;
	static const uint IDX_MAX_KEY_PART_LEN = 767;
};

class SubToSubExtractor;
/** һ��DRS���� */
class DrsIndex : public Analysable, public IndexBase {
public:
	virtual bool insert(Session *session, const SubRecord *key, bool *duplicateKey, bool checkDuplicate = true) = 0;
	virtual bool insertGotPage(DrsIndexScanHandleInfo *info) = 0;
	virtual void insertNoCheckDuplicate(Session *session, const SubRecord *key) = 0;

	virtual bool del(Session *session, const SubRecord *key) = 0;
	virtual bool getByUniqueKey(Session *session, const SubRecord *key, LockMode lockMode, RowId *rowId, 
		SubRecord *subRecord, RowLockHandle **rlh, SubToSubExtractor *extractor) = 0;
	virtual IndexScanHandle* beginScan(Session *session, const SubRecord *key, bool forward, bool includeKey, 
		LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor) = 0;
	virtual bool getNext(IndexScanHandle *scanHandle, SubRecord *key) = 0;
	virtual bool deleteCurrent(IndexScanHandle *scanHandle) = 0;
	virtual void endScan(IndexScanHandle *scanHandle) = 0;

#ifdef TNT_ENGINE
	virtual IndexScanHandle* beginScanSecond(Session *session, const SubRecord *key, bool forward, 
		bool includeKey, LockMode lockMode, RowLockHandle **rlh, SubToSubExtractor *extractor, 
		TLockMode trxLockMode) = 0;
	virtual bool getNextSecond(IndexScanHandle *scanHandle) throw(NtseException) = 0;
	virtual void endScanSecond(IndexScanHandle *scanHandle) = 0;

	virtual bool checkDuplicate(Session *session, const SubRecord *key, DrsIndexScanHandleInfo **info) = 0;
//	virtual void setTableDef(const TableDef* tableDef) = 0;
	virtual bool locateLastLeafPageAndFindMaxKey(Session *session, SubRecord *foundKey) = 0;
	virtual u64 recordsInRangeSecond(Session *session, const SubRecord *min, bool includeKeyMin, const SubRecord *max,
		bool includeKeyMax) = 0;
#endif
	virtual u8 getIndexId() = 0;
	virtual u64 recordsInRange(Session *session, const SubRecord *min, bool includeKeyMin, const SubRecord *max,
		bool includeKeyMax) = 0;
	virtual const IndexStatus& getStatus() = 0;
	virtual void updateExtendStatus(Session *session, uint maxSamplePages) = 0;
	virtual const IndexStatusEx& getStatusEx() = 0;
	virtual void setSplitFactor(s8 splitFactor) = 0;
};


}

#endif