/**
 * 定长记录堆
 *
 * @author 谢可(ken@163.org)
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

/** 定长堆头页头结构 */
struct FLRHeapHeaderPageInfo {
	HeapHeaderPageInfo	m_hhpi;		/** 堆头页共享结构 */
	u64	m_firstFreePageNum;			/** 堆中第一个空闲数据页，0表示没有 */
};

#pragma pack(1)

/** 定长堆数据页头结构 */
struct FLRHeapRecordPageInfo {
	BufferPageHdr	m_bph;	/** 缓存页公用头结构 */
	u16		m_recordNum;	/** 记录数 */
	s16		m_firstFreeSlot;/** 第一个空闲记录槽 */
	u64		m_nextFreePageNum: PAGE_BITS;	/** 下一空闲页 */
	bool	m_inFreePageList: 1;			/** 是否在空闲页链表中 */ 
};

/** 记录槽头部结构 */
struct FLRSlotInfo {
	bool	m_free: 1;			/** 是否为空闲记录槽 */
	union {
		s16		m_nextFreeSlot;	/** 下一空闲记录槽，只在本身为空闲记录槽时有用 */
		byte	m_data[1];		/** 数据，只在不为空闲记录槽时有用 */
	} u;
};
#pragma pack()

/* 定长记录堆 */
class FixedLengthRecordHeap : public DrsHeap {
public:
	FixedLengthRecordHeap(Database *db, const TableDef *tableDef, File *heapFile, BufferPageHdr *headerPage, DBObjStats* dbObjStats) throw(NtseException);

	// 堆设置
	void setPctFree(Session *session, u8 pctFree) throw(NtseException) {
		UNREFERENCED_PARAMETER(session);
		UNREFERENCED_PARAMETER(pctFree);
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Fixed length heap doesn't support setting heap_pct_free.");
	};

	// 记录操作
	bool getSubRecord(Session *session, RowId rowId, SubrecExtractor *extractor, SubRecord *subRecord, LockMode lockMode = None, RowLockHandle **rlh = NULL);
	bool getRecord(Session *session, RowId rowId, Record *record,
		LockMode lockMode = None, RowLockHandle **rlh = NULL,
		bool duringRedo = false);
	RowId insert(Session *session, const Record *record, RowLockHandle **rlh) throw(NtseException);
	bool del(Session *session, RowId rowId);
	bool update(Session *session, RowId rowId, const SubRecord *subRecord);
	bool update(Session *session, RowId rowId, const Record *record);

	// 表扫描
	DrsHeapScanHandle* beginScan(Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc);
	bool getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRecord);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy);
	void deleteCurrent(DrsHeapScanHandle *scanHandle);
	void endScan(DrsHeapScanHandle *scanHandle);

	void storePosAndInfo(DrsHeapScanHandle *scanHandle);
	void restorePosAndInfo(DrsHeapScanHandle *scanHandle);

	/*** redo函数 ***/
	RowId redoInsert(Session *session, u64 lsn, const byte *log, uint size, Record *record);
	void redoUpdate(Session *session, u64 lsn, const byte *log, uint size, const SubRecord *update);
	void redoDelete(Session *session, u64 lsn, const byte *log, uint size);

#ifdef NTSE_VERIFY_EX
	void redoFinish(Session *session);
	void verifyFreePageList(Session *session);
#endif

	/* 其他 */
	virtual bool isPageEmpty(Session *session, u64 pageNum);
	static void getRecordFromInsertlog(LogEntry *log, Record *outRec);

	/* 统计信息 */
	void updateExtendStatus(Session *session, uint maxSamplePages);

	/** 初始化页首结构 */
	static void initHeader(BufferPageHdr *headerPage) throw(NtseException);

#ifdef NTSE_UNIT_TEST	// 调试用
	void printOtherInfo() {
		printf("*    number of records in one page is %d .\n", m_recPerPage);
		printf("*    free records down limit of free page is %d .\n", m_freePageRecLimit);
	}
	u64 getFirstFreePageNum() { return ((FLRHeapHeaderPageInfo *)m_headerPage)->m_firstFreePageNum; }
	u64 getRecPerPage() { return m_recPerPage; }
#endif

protected:
	/* 采样统计 */
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

	/* 记录和槽操作 */

	/**
	 * 获取指定记录槽头信息
	 *
	 * @param page 页面
	 * @param slotNum 记录槽号
	 * @return 指定记录槽头信息
	 */
	FLRSlotInfo* getSlot(FLRHeapRecordPageInfo *page, s16 slotNum) { return (FLRSlotInfo*)((byte*)page + OFFSET_PAGE_RECORD + slotNum * m_slotLength); }
	void writeRecord(FLRHeapRecordPageInfo *page, s16 slotNum, const Record *record);
	bool readRecord(FLRHeapRecordPageInfo *page, s16 slotNum, Record *record);
	bool readSubRecord(FLRHeapRecordPageInfo *page, s16 slotNum, SubrecExtractor *extractor, SubRecord *subRecord);
	bool writeSubRecord(FLRHeapRecordPageInfo *page, s16 slotNum, const SubRecord *subRecord);
	bool deleteRecord(FLRHeapRecordPageInfo *page, s16 slotNum);

	/* 日志操作 */
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

	uint	m_slotLength;		/** 记录槽大小 */
	uint	m_recPerPage;		/** 每页存储的记录数 */
	uint	m_freePageRecLimit;	/** 有多少条空闲记录时将页作为空闲页 */

	static const uint FREE_PAGE_RATIO = 5;	/** 当页中有1/FREE_PAGE_RATIO的空闲空间时作为可存储新记录的空闲页 */

	/*** 记录槽标志位 ***/
	static const u8 SLOT_FLAG_FREE = 0x01;	/** 记录槽为空 */
	
    static const uint SLOT_FLAG_LENGTH = sizeof(u8);  /** 记录槽标志的长度，一个byte */
	static const uint OFFSET_PAGE_RECORD = sizeof(FLRHeapRecordPageInfo); /** 记录数据在数据页的起始偏移 */

public:
	bool verifyPage(FLRHeapRecordPageInfo *page);

	friend Tracer& operator << (Tracer& tracer, FixedLengthRecordHeap *heap);
};


}

#endif /*FIXEDLENGTHRECORDHEAP_H_*/

