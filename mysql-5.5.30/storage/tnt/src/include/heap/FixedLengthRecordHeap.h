/**
 * ������¼��
 *
 * @author л��(ken@163.org)
 */

#ifndef _NTSE_FIXEDLENGTHRECORDHEAP_H_
#define _NTSE_FIXEDLENGTHRECORDHEAP_H_

#include "Heap.h"


namespace ntse {



class Session;
class File;
class BufferPageHandle;
class Database;
class TableDef;
class FixedLengthRecordHeap;

/** ������ͷҳͷ�ṹ */
struct FLRHeapHeaderPageInfo {
	HeapHeaderPageInfo	m_hhpi;		/** ��ͷҳ����ṹ */
	u64	m_firstFreePageNum;			/** ���е�һ����������ҳ��0��ʾû�� */
};

#pragma pack(1)

/** ����������ҳͷ�ṹ */
struct FLRHeapRecordPageInfo {
	BufferPageHdr	m_bph;	/** ����ҳ����ͷ�ṹ */
	u16		m_recordNum;	/** ��¼�� */
	s16		m_firstFreeSlot;/** ��һ�����м�¼�� */
	u64		m_nextFreePageNum: PAGE_BITS;	/** ��һ����ҳ */
	bool	m_inFreePageList: 1;			/** �Ƿ��ڿ���ҳ������ */ 
};

/** ��¼��ͷ���ṹ */
struct FLRSlotInfo {
	bool	m_free: 1;			/** �Ƿ�Ϊ���м�¼�� */
	union {
		s16		m_nextFreeSlot;	/** ��һ���м�¼�ۣ�ֻ�ڱ���Ϊ���м�¼��ʱ���� */
		byte	m_data[1];		/** ���ݣ�ֻ�ڲ�Ϊ���м�¼��ʱ���� */
	} u;
};
#pragma pack()

/* ������¼�� */
class FixedLengthRecordHeap : public DrsHeap {
public:
	FixedLengthRecordHeap(Database *db, const TableDef *tableDef, File *heapFile, BufferPageHdr *headerPage, DBObjStats* dbObjStats) throw(NtseException);

	// ������
	void setPctFree(Session *session, u8 pctFree) throw(NtseException) {
		UNREFERENCED_PARAMETER(session);
		UNREFERENCED_PARAMETER(pctFree);
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Fixed length heap doesn't support setting heap_pct_free.");
	};

	// ��¼����
	bool getSubRecord(Session *session, RowId rowId, SubrecExtractor *extractor, SubRecord *subRecord, LockMode lockMode = None, RowLockHandle **rlh = NULL);
	bool getRecord(Session *session, RowId rowId, Record *record,
		LockMode lockMode = None, RowLockHandle **rlh = NULL,
		bool duringRedo = false);
	RowId insert(Session *session, const Record *record, RowLockHandle **rlh) throw(NtseException);
	bool del(Session *session, RowId rowId);
	bool update(Session *session, RowId rowId, const SubRecord *subRecord);
	bool update(Session *session, RowId rowId, const Record *record);

	// ��ɨ��
	DrsHeapScanHandle* beginScan(Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc);
	bool getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRecord);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy);
	void deleteCurrent(DrsHeapScanHandle *scanHandle);
	void endScan(DrsHeapScanHandle *scanHandle);

	void storePosAndInfo(DrsHeapScanHandle *scanHandle);
	void restorePosAndInfo(DrsHeapScanHandle *scanHandle);

	/*** redo���� ***/
	RowId redoInsert(Session *session, u64 lsn, const byte *log, uint size, Record *record);
	void redoUpdate(Session *session, u64 lsn, const byte *log, uint size, const SubRecord *update);
	void redoDelete(Session *session, u64 lsn, const byte *log, uint size);

#ifdef NTSE_VERIFY_EX
	void redoFinish(Session *session);
	void verifyFreePageList(Session *session);
#endif

	/* ���� */
	virtual bool isPageEmpty(Session *session, u64 pageNum);
	static void getRecordFromInsertlog(LogEntry *log, Record *outRec);

	/* ͳ����Ϣ */
	void updateExtendStatus(Session *session, uint maxSamplePages);

	/** ��ʼ��ҳ�׽ṹ */
	static void initHeader(BufferPageHdr *headerPage) throw(NtseException);

#ifdef NTSE_UNIT_TEST	// ������
	void printOtherInfo() {
		printf("*    number of records in one page is %d .\n", m_recPerPage);
		printf("*    free records down limit of free page is %d .\n", m_freePageRecLimit);
	}
	u64 getFirstFreePageNum() { return ((FLRHeapHeaderPageInfo *)m_headerPage)->m_firstFreePageNum; }
	u64 getRecPerPage() { return m_recPerPage; }
#endif

protected:
	/* ����ͳ�� */
	bool isSamplable(u64 pageNum);
	Sample *sampleBufferPage(Session *session, BufferPageHdr *page);
	void selectPage(u64 *outPages, int wantNum, u64 min, u64 regionSize);
public:
	u64 metaDataPgCnt() { return (u64)1; }
protected:


private:
	BufferPageHandle* findFreePage(Session *session, u64 *pageNum);
	void initExtendedNewPages(Session *session, uint size);
	void afterExtendHeap(uint extendSize);

	/* ��¼�Ͳ۲��� */

	/**
	 * ��ȡָ����¼��ͷ��Ϣ
	 *
	 * @param page ҳ��
	 * @param slotNum ��¼�ۺ�
	 * @return ָ����¼��ͷ��Ϣ
	 */
	FLRSlotInfo* getSlot(FLRHeapRecordPageInfo *page, s16 slotNum) { return (FLRSlotInfo*)((byte*)page + OFFSET_PAGE_RECORD + slotNum * m_slotLength); }
	void writeRecord(FLRHeapRecordPageInfo *page, s16 slotNum, const Record *record);
	bool readRecord(FLRHeapRecordPageInfo *page, s16 slotNum, Record *record);
	bool readSubRecord(FLRHeapRecordPageInfo *page, s16 slotNum, SubrecExtractor *extractor, SubRecord *subRecord);
	bool writeSubRecord(FLRHeapRecordPageInfo *page, s16 slotNum, const SubRecord *subRecord);
	bool deleteRecord(FLRHeapRecordPageInfo *page, s16 slotNum);

	/* ��־���� */
#ifndef NTSE_VERIFY_EX
	u64 writeInsertLog(Session *session, RowId rid, const Record *record, bool headerPageModified, u64 newListHeader);
	u64 writeDeleteLog(Session *session, RowId rid, bool headerPageModified, u64 oldListHeader);
	u64 writeUpdateLog(Session *session, RowId rowId);
	void parseInsertLog(const byte *log, uint logSize, RowId *rid, Record *record, bool *headerPageModified, u64 *newListHeader);
	void parseDeleteLog(const byte *log, uint logSize, RowId *rid, bool *headerPageModified, u64 *oldListHeader);
	void parseUpdateLog(const byte *log, uint logSize, RowId *rid);
#else
	u64 writeInsertLog(Session *session, RowId rid, const Record *record, bool headerPageModified, u64 newListHeader, u64 oldLSN, u64 hdOldLSN);
	u64 writeDeleteLog(Session *session, RowId rid, bool headerPageModified, u64 oldListHeader, u64 oldLSN, u64 hdOldLSN);
	u64 writeUpdateLog(Session *session, RowId rowId, u64 oldLSN);
	void parseInsertLog(const byte *log, uint logSize, RowId *rid, Record *record, bool *headerPageModified, u64 *newListHeader, u64 *oldLSN, u64 *hdOldLSN);
	void parseDeleteLog(const byte *log, uint logSize, RowId *rid, bool *headerPageModified, u64 *oldListHeader, u64 *oldLSN, u64 *hdOldLSN);
	void parseUpdateLog(const byte *log, uint logSize, RowId *rid, u64 *oldLSN);
#endif
	void constructRecord(FLRHeapRecordPageInfo *page, s16 slotNum, Record *record);

	uint	m_slotLength;		/** ��¼�۴�С */
	uint	m_recPerPage;		/** ÿҳ�洢�ļ�¼�� */
	uint	m_freePageRecLimit;	/** �ж��������м�¼ʱ��ҳ��Ϊ����ҳ */

	static const uint FREE_PAGE_RATIO = 5;	/** ��ҳ����1/FREE_PAGE_RATIO�Ŀ��пռ�ʱ��Ϊ�ɴ洢�¼�¼�Ŀ���ҳ */

	/*** ��¼�۱�־λ ***/
	static const u8 SLOT_FLAG_FREE = 0x01;	/** ��¼��Ϊ�� */
	
    static const uint SLOT_FLAG_LENGTH = sizeof(u8);  /** ��¼�۱�־�ĳ��ȣ�һ��byte */
	static const uint OFFSET_PAGE_RECORD = sizeof(FLRHeapRecordPageInfo); /** ��¼����������ҳ����ʼƫ�� */

public:
	bool verifyPage(FLRHeapRecordPageInfo *page);

	friend Tracer& operator << (Tracer& tracer, FixedLengthRecordHeap *heap);
};


}

#endif /*FIXEDLENGTHRECORDHEAP_H_*/

