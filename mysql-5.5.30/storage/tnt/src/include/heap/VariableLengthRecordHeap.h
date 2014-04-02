/**
 * 变长记录堆
 *
 * @author 谢可(ken@163.org)
 */

#ifndef _NTSE_VARIABLELENGTHRECORDHEAP_H_
#define _NTSE_VARIABLELENGTHRECORDHEAP_H_

#include "Heap.h"


namespace ntse{

class Database;
class File;
class BufferPageHandle;
struct BufferPageHdr;
class VariableLengthRecordHeap;
struct Mutex;

/**
在NTSE变长堆中，每个页面中的空闲空间大小使用2bit来表示，分为四种级别。
分别用00, 01, 10, 11表示，数字越大，页内空闲空间越多。
页面对应的空闲级别位图信息存储在专门的位图页和中央位图页中。
当页面空闲级别发生变化时，需要修改位图页，这一代价是比较大的。
为防止对页面反复进行插入/删除操作导致反复修改位图页的行为。在页面中
插入数据和删除数据时，调整页面空闲级别设定的阈值是不一样的。
插入时的阈值总是比删除时的阈值要小。以下这六个常数即分别定义了插入
和删除时调整页面空闲级别的阈值。
*/

/** 新页面空闲级别初始为11 */
#define FLAG_10_LIMIT 0.7	/** 插入时导致页内空闲空间低于70%则修改空闲级别为10 */ /* 注意MAX_REC_LENGTH的定义和该值有关 */
#define FLAG_01_LIMIT 0.4	/** 插入时导致页内空闲空间低于40%则修改空闲级别为01 */
#define FLAG_00_LIMIT 0.1	/** 插入时导致页内空闲空间低于10%则修改空闲级别为00 */

#define FLAG_11_REGAIN_LIMIT 0.8	/** 删除时导致页内空闲空间高于80%则修改空闲级别为11 */
#define FLAG_10_REGAIN_LIMIT 0.5	/** 删除时导致页内空闲空间高于50%则修改空闲级别为10 */
#define FLAG_01_REGAIN_LIMIT 0.2	/** 删除时导致页内空闲空间高于20%则修改空闲级别为01 */

/**
 * 变长记录堆首页结构
 */
struct VLRHeapHeaderPageInfo {
	HeapHeaderPageInfo m_hhpi;	/** 堆首页公用信息 */
	u8 m_centralBmpNum: 7; 		/** 中央位图用的页面数，默认是1 */
	bool m_cleanClosed: 1; 		/** 上次是否正常关闭，否的话需要做中央位图重组 */
};


/**
 * 普通位图页首信息
 */
struct BitmapPageHeader {
	BufferPageHdr	m_bph;	/** 缓存页共用头结构 */
	u16 m_pageCount[4];		/** x级页面的数目为m_pageCount[x], x = 00, 01, 10, 11, 11无意义(因为不准确，
							    如果是最后一个位图页，则每次都需要重新计算，因为当堆扩展的时候，会生
								成新的11页面，但是不影响这个计数 */
};

#pragma pack(8)
/**
 * 普通位图页，简化位图存取和查找等操作。
 * 位图有2bit，表示页面空闲级别，最小空闲空间越少
 */
class BitmapPage {
public:
	/** 页内位图内容的起始偏移 */
	static const uint MAP_OFFSET = (sizeof(BitmapPageHeader) + 7) & (~7);
	/** 一个位图页面最多管理的记录页，即页群大小 */
	static const uint CAPACITY = (Limits::PAGE_SIZE - MAP_OFFSET) * 4;
public:
	union {
		BitmapPageHeader m_header;		/** 首头 */
		byte m_fill8width[MAP_OFFSET];	/** 填充到8字节对齐 */
	} u;
private:
	byte m_map[Limits::PAGE_SIZE - MAP_OFFSET];	/** 位图 */

public:
	/**
	 * 按下标取位图数据(2bits)，无法做到存，因为位域不能取地址，无法做左值
	 * @param idx  下标索引
	 * @return  标志位，2bits的一个u8
	 */
	u8 operator[](int idx) {
		return (u8)(m_map[idx >> 2] >> ((idx % 4) << 1)) & 0x3;
	}

	/** 按下标设置位图 
	 *
	 * @param idx  下标索引
	 * @param flag  新的位图标志(2bit)
	 * @param bitSet OUT  更改后的哦位图在中央位图中的标志(4bit)
	 * @param mapSize  位图有效表示的大小
	 * @return  需要修改中央位图为true，不需要返回false
	 */
	bool setBits(int idx, u8 flag, u8 *bitSet, uint mapSize);

	/** 设置位图中两处下标的值
	 *
	 * @param idx1,idx2  下标索引
	 * @param flag1,flag2  新的位图标志(2bit)
	 * @param bitSet OUT  更改后的哦位图在中央位图中的标志(4bit)
	 * @param mapSize  位图有效表示的大小
	 * @return  需要修改中央位图为true，不需要返回false
	 */
	bool setBits(u8 *bitSet, uint mapSize, int idx1, u8 flag1, int idx2, u8 flag2);

	/**
	 * 初始化一个位图页
	 */
	void init();

	/**
	 * 在位图页中寻找符合要求的可用页面
	 * @param spaceFlag  需要的页面类型
	 * @param bitmapSize  所查找的位图的大小
	 * @param startPos  开始查找的位置（这个不影响最终是否找到，因为如果后向查找未果，是会从头查找的)
	 */
	int findFreeInBMP(u8 spaceFlag, uint bitmapSize, int startPos = 0);

#ifdef NTSE_TRACE
	friend Tracer& operator << (Tracer& tracer, BitmapPage *bmp);
#endif
};
#pragma pack()

#pragma pack(1)
/**
 * 记录头结构，4字节。记录是链接时，链接源和目的的RID存储在记录数据中，不存储在记录头
 */
struct VLRHeader {
	u16	m_offset: 15; 		/** 记录的偏移量，单位是相对于记录区起始位置的（包括记录头区） */
	u16 m_ifCompressed: 1;  /** 记录是否是压缩格式的 */  
	u16 m_size: 13; 	    /** 记录的长度，最少是6 */
	u16 m_ifEmpty: 1;	    /** 空闲记录项 */
	u16 m_ifLink: 1;	    /** 链接源 */
	u16 m_ifTarget: 1;	    /** 链接目的 */
};


/**
 * 变长记录堆记录页面结构
 */
class VLRHeapRecordPageInfo {
public:
	BufferPageHdr m_bph;	/** 缓存页共用头结构 */
	u16 m_recordNum; 		/** 页面内的记录数目 */
	u16 m_freeSpaceSize; 	/** 剩余空间大小，单位是byte */
	u16 m_lastHeaderPos; 	/** 最后一个记录头的位置，是从页首开始的偏移量 */
	u16 m_freeSpaceTailPos; /** 连续空闲空间的后面一个byte的地址（这个地址本身unavailable） */
	u8 m_pageBitmapFlag;	/** 该页在位图页中的空闲级别标志，是否需要修改位图页由此而来 */
	u8 m_pad;				/** 填充到按2字节对齐，优化访问记录头的效率 */
	static const u16 LINK_SIZE = RID_BYTES;	/** 记录链接信息占用的存储空间 */
public:
	VLRHeader* getVLRHeader(uint slotNum);
	uint getEmptySlot();
	byte* getRecordData(VLRHeader *vlrHeader);
	void defrag();
	uint insertRecord(const Record *record, RowId srcRid = 0);
	void insertIntoSlot(const Record *record, u16 slotNum, RowId srcRid = 0);
	bool spaceIsEnough(uint slotNum, u16 dataSizeWithHeader);
	bool deleteSlot(uint slotNum);
	bool linklizeSlot(RowId tagRid, uint slotNum);
	void updateLocalRecord(Record *record, uint slotNum);

	void readRecord(VLRHeader *recHdr, Record *record);
	void readSubRecord(VLRHeader *recHdr, SubrecExtractor *extractor, SubRecord *subRecord);
	void constructRecord(VLRHeader *recHdr, Record *record);

#ifdef NTSE_VERIFY_EX
	bool verifyPage();
#endif
};


/**
 * 中央位图控制，简化中央位图的存取，查找等工作
 */
class CentralBitmap {
public:
	/**
	 * 按下标取位图信息
	 * @param idx  下标
	 * @return  位图缩略图，一个字节，只有4bits是实际有效的
	 */
	u8 operator[](int idx) {
		return ((u8 *)(m_pageHdrArray[idx / (Limits::PAGE_SIZE - MAP_OFFSET)]) + MAP_OFFSET)[idx % (Limits::PAGE_SIZE - MAP_OFFSET)];
	}

	/**
	 * 设置下标为idx处的位图页标志
	 * @param idx           目的下标
	 * @param bitmapFlag    下标
	 */
	void setBitmap(int idx, u8 bitmapFlag);

	/**
	 * 查找第一个可能含有符合要求的可用页面的位图页的位置
	 * 
	 * @param spaceFlag  需求页面类型
	 * @param startPos  起始查找位置
	 * @return 找到的位图页的下标
	 */
	int findFirstFitBitmap(u8 spaceFlag, int startPos);

private:
	VariableLengthRecordHeap	*m_vlrHeap;	/** 所属变长堆 */
	BufferPageHdr	**m_pageHdrArray;		/** 各中央位图页，始终pin在页面缓存中 */
	static const int MAP_OFFSET = (sizeof(BufferPageHdr) + 7) & (~7); /** 页内位图信息起始偏移 */

	friend class VariableLengthRecordHeap;
};

#pragma pack()


/* 变长记录堆 */
class VariableLengthRecordHeap : public DrsHeap {
public:
	VariableLengthRecordHeap(Database *db, Session *session, const TableDef *tableDef, File *heapFile, BufferPageHdr *headerPageHdl, DBObjStats* dbObjStats)  throw(NtseException);
	void close(Session *session, bool flushDirty);

	// 堆设置
	void setPctFree(Session *session, u8 pctFree) throw(NtseException);

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
	bool getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRec);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy);
	void deleteCurrent(DrsHeapScanHandle *scanHandle);
	void endScan(DrsHeapScanHandle *scanHandle);

	void storePosAndInfo(DrsHeapScanHandle *scanHandle);
	void restorePosAndInfo(DrsHeapScanHandle *scanHandle);

	// redo函数
	RowId redoInsert(Session *session, u64 logLSN, const byte *log, uint size, Record *record);
	void redoUpdate(Session *session, u64 logLSN, const byte *log, uint size, const SubRecord *update);
	void redoDelete(Session *session, u64 logLSN, const byte *log, uint size);

	void redoFinish(Session *session);

	/* 统计信息 */
	void updateExtendStatus(Session *session, uint maxSamplePages);

	/* 其他 */
	virtual bool isPageEmpty(Session *session, u64 pageNum);
	bool isPageBmp(u64 pageNum) { return pageIsBitmap(pageNum); }

	static void getRecordFromInsertlog(LogEntry *log, Record *outRec);

	static void initHeader(BufferPageHdr *headerPage, BufferPageHdr **additionalPages, uint *additionalPageNum) throw(NtseException);



#ifdef NTSE_UNIT_TEST // 调试用
	uint getCBMCount() { return m_centralBmpNum; }
	BufferPageHdr *getCBMPage(int idx);
	CentralBitmap &getCBM() { return m_cbm; }
	static int getSpaceLimit(int flag) {
		switch (flag) {
			case 0:
				return VariableLengthRecordHeap::FLAG00_SPACE_LIMIT;
			case 1:
				return VariableLengthRecordHeap::FLAG01_SPACE_LIMIT;
			case 2:
				return VariableLengthRecordHeap::FLAG10_SPACE_LIMIT;
			default:
				return 0;
		}
	}
	u8 getPageFlag(Session *session, u64 pageNum);
	u16 getRecordOffset(Session *session, RowId rid);
	RowId getTargetRowId(Session *session, RowId rid);
	u16 getPageFreeSpace(Session *session, u64 pageNum);
	u64 getMaxBmpNum() { return m_lastBmpNum; }

	int CBfindFirstFitBitmap(u8 spaceFlag, int startPos) {
		return m_cbm.findFirstFitBitmap(spaceFlag, startPos);
	}
#endif

protected:
	/* 采样统计 */
	bool isSamplable(u64 pageNum);
	Sample *sampleBufferPage(Session *session, BufferPageHdr *page);
	void selectPage(u64 *outPages, int wantNum, u64 min, u64 regionSize);
public:
	u64 metaDataPgCnt() { return m_centralBmpNum + 1; }
protected:
	uint unsamplablePagesBetween(u64 downPgn, u64 regionSize);

#ifdef NTSE_UNIT_TEST
public:
#else
private:
#endif
	// 重要过程函数
	bool doUpdate(Session *session, RowId rowId, const SubRecord *subRecord, const Record *record);
	bool doGet(Session *session, RowId rowId, void *dest, bool destIsRecord, SubrecExtractor *extractor,
		LockMode lockMode = None, RowLockHandle **rlh = NULL,
		bool duringRedo = false);
	bool doGetTarget(Session *session, RowId rowId, RowId srcRid, void *dest, bool destIsRecord, SubrecExtractor *extractor);
	void extractTarget(Session *session, RowId rowId, RowId srcRid, SubRecord *output, SubrecExtractor *extractor);
	BufferPageHandle* findFreePage(Session *session, u16 spaceRequest, u64 *pageNum, LockMode lockMode = Exclusived, u64 lastFalse = 0);
	void updateBitmap(Session *session, u64 recPageNum, u8 newFlag, u64 lsn, BufferPageHandle* bitmapHandle);
	void updateSameBitmap(Session *session, u64 lsn, u64 recPageNum1, u8 newFlag1, u64 recPageNum2, u8 newFlag2, BufferPageHandle* bitmapHandle);
	void initExtendedNewPages(Session *session, uint size);
	void afterExtendHeap(uint extendSize);
	/* 重建中央位图 */
	void redoCentralBitmap(Session *session);

	/** 辅助函数 **/
	/**
	 * @see DrsHeap::getReservedPages
	 */
	virtual uint getReservedPages() {
		return 1 + m_centralBmpNum;
	}
	/**
	 * 判断一个页面是否是位图页
	 * @param pageNum  页面号
	 * @return  是位图页返回true
	 */
	bool pageIsBitmap(u64 pageNum) {
		return !((pageNum - m_centralBmpNum - 1) % (BitmapPage::CAPACITY + 1));
	}

	/**
	* 判断一个页面是否是位图页，连续判断
	* @param pageNum      页面号
	* @param nextBmpNum   下一个位图号
	* @return  是位图页返回true
	*/
	bool pageIsBitmap(u64 pageNum, u64& nextBmpNum) {
		return (pageNum < nextBmpNum) ? false : ((nextBmpNum += (BitmapPage::CAPACITY + 1)), true);
	}

	/**
	 * 将bitmap在中央位图中的index转为页面号。
	 * @param bmpIdx   位图在中央位图中的下标
	 * @return         页面号
	 */
	u64 bmpIdx2Num(int bmpIdx) {
		return bmpIdx * (BitmapPage::CAPACITY + 1) + 1 + m_centralBmpNum;
	}
	
	/**
	 * 将bitmap页面的页面号转为在中央位图中的下标值。
	 * @param bmpPageNum   页面号
	 * @return             位图在中央位图中的下标
	 */
	int bmpNum2Idx(u64 bmpPageNum) {
		assert(pageIsBitmap(bmpPageNum));
		return (int)((bmpPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1));
	}

	/**
	 * 由位图和页面的下标值得到页面号。
	 * @param bitmapIdx      位图下标
	 * @param pageIdx        页面下标
	 * @return               页面号
	 */
	u64 getPageNumByIdx(int bitmapIdx, int pageIdx) {
		return bmpIdx2Num(bitmapIdx) + pageIdx + 1;
	}
	/**
	 * 由记录页面号得到对应的bitmap页号
	 * @param pageNum     页面号
     * @return            对应的位图页号
	 */
	u64 getBmpNum(u64 pageNum) {
		return pageNum - (pageNum - 1 - m_centralBmpNum) % (BitmapPage::CAPACITY + 1);
	}

#ifdef NTSE_UNIT_TEST  //测试用
public:
	void syncMataPages(Session *session);
public:
#endif


	BufferPageHandle* getBitmap(Session *session, int num, LockMode lockMode);
	void getTwoBitmap(Session *session, u64 bmpPgN1, u64 bmpPgN2, BufferPageHandle **bmpHdl1, BufferPageHandle **bmpHdl2, LockMode lockMode = Exclusived);
	u8 getRecordPageFlag(u8 oldFlag, u16 freeSpaceSize, bool freeSpaceIncrease);
	u8 spaceResquestToFlag(uint spaceRequest);
	bool lockSecondPage(Session *session, u64 firstPageNum, u64 secondPageNum, BufferPageHandle **firstPageHdl, BufferPageHandle **secondPageHdl, LockMode lockMode = Exclusived);
	bool lockThirdMinPage(Session *session, u64 fPgNum, u64 sPgNum, u64 tPgNum, BufferPageHandle **fPageHdl, BufferPageHandle **sPageHdl, BufferPageHandle **tPageHdl);
	void redoVerifyBitmap(Session *session, u64 pageNum, u8 pageFlag, u64 logLSN);	
	void redoVerifySameBitmap(Session *session, u64 logLSN, u64 pageNum1, u8 pageFlag1, u64 pageNum2, u8 pageFlag2);

	/* log操作 */
#ifndef NTSE_VERIFY_EX
	u64 writeInsertLog(Session *session, RowId rid, const Record *record, bool bitmapModified);
	u64 writeDeleteLog(Session *session, RowId rid, bool bitmapModified, RowId tagRid, bool tagBitmapModified);
	u64 writeUpdateLog(Session *session, RowId rid, const Record *record, bool bitmapModified, bool hasNewTarget, bool hasOldTarget, bool updateInOldTag,
				  u64 newTagRowId, bool newTagBmpModified, u64 oldTagRowId, bool oldTagBmpModified);
	void parseInsertLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, Record *record);
	void parseDeleteLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, RowId *tagRid, bool *tagBitmapModified);
	void parseUpdateLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified,
						bool *hasNewTarget, bool *hasOldTarget, bool *updateInOldTag,
						u64 *newTagRowId, bool *newTagBmpModified, u64 *oldTagRowId, bool *oldTagBmpModified,
						bool *hasRecord, Record *record);
#else
	u64 writeInsertLog(Session *session, RowId rid, const Record *record, bool bitmapModified, u64 oldLSN);
	u64 writeDeleteLog(Session *session, RowId rid, bool bitmapModified, RowId tagRid, bool tagBitmapModified, u64 oldSrcLSN, u64 oldTagLSN);
	u64 writeUpdateLog(Session *session, RowId rid, const Record *record, bool bitmapModified, bool hasNewTarget, bool hasOldTarget, bool updateInOldTag,
		u64 newTagRowId, bool newTagBmpModified, u64 oldTagRowId, bool oldTagBmpModified, u64 srcOldLSN, u64 oldTagOldLSN, u64 newTagOldLSN);
	void parseInsertLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, Record *record, u64 *oldLSN);
	void parseDeleteLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified, RowId *tagRid, bool *tagBitmapModified, u64 *oldSrcLSN, u64 *oldTagLSN);
	void parseUpdateLog(const byte *log, uint logSize, RowId *rowId, bool *bitmapModified,
		bool *hasNewTarget, bool *hasOldTarget, bool *updateInOldTag,
		u64 *newTagRowId, bool *newTagBmpModified, u64 *oldTagRowId, bool *oldTagBmpModified,
		bool *hasRecord, Record *record, u64 *srcOldLSN, u64 *oldTagOldLSN, u64 *newTagOldLSN);
#endif


	/* 其他 */
#ifdef NTSE_VERIFY_EX
	bool heavyVerify(VLRHeapRecordPageInfo *page);
#endif

	static const u16 LINK_SIZE = VLRHeapRecordPageInfo::LINK_SIZE;				/** 链接内容的大小 */
	static const uint DEFAULT_CBITMAP_SIZE = 1;									/** 中央位图的默认大小 */
	//static const uint OFFSET_HEADER_TABLEDEF = sizeof(VLRHeapHeaderPageInfo);	/** 表定义偏移量 */
	/** 空闲空间等级 */
	static const uint FLAG10_SPACE_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_10_LIMIT);
	static const uint FLAG01_SPACE_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_01_LIMIT);
	static const uint FLAG00_SPACE_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_00_LIMIT);
	/** 空闲空间降级等级 */
	static const uint FLAG11_SPACE_DOWN_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_11_REGAIN_LIMIT);
	static const uint FLAG10_SPACE_DOWN_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_10_REGAIN_LIMIT);
	static const uint FLAG01_SPACE_DOWN_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_01_REGAIN_LIMIT);


	uint	m_centralBmpNum;	            /** 本堆的中央位图大小，单位是页面数 */
	BufferPageHdr	**m_centralBitmapHdr;	/** 中央位图页的句柄们 */
	CentralBitmap	m_cbm;		            /** 中央位图控制 */
	u64		m_bitmapNum;		            /** 地方位图页个数，根据页面数可计算出，不需要持久化 */
	u64		m_lastBmpNum;		            /** 最后一个位图页号 */
	uint 	m_reserveSize; 		            /** 预留空间大小，字节数 */

	struct {
		int m_bitmapIdx;	/** 地方位图页中中央位图页中的下标 */
		int m_pageIdx;		/** 数据页在地方位图页中的下标 */
	} m_position[4];		/** 上次获得的某种类型的页面位置 */
	RWLock m_posLock;		/** 保护上述位置信息的锁 */

	friend class CentralBitmap;
public:
	static const u16 MAX_VLR_LENGTH = (FLAG10_SPACE_LIMIT - 1) - sizeof(VLRHeader); /** 最大变长记录长度 */

	friend Tracer& operator << (Tracer& tracer, VariableLengthRecordHeap *heap);
};

} // namespace ntse;



#endif
