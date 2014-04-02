/**
 * �䳤��¼��
 *
 * @author л��(ken@163.org)
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
��NTSE�䳤���У�ÿ��ҳ���еĿ��пռ��Сʹ��2bit����ʾ����Ϊ���ּ���
�ֱ���00, 01, 10, 11��ʾ������Խ��ҳ�ڿ��пռ�Խ�ࡣ
ҳ���Ӧ�Ŀ��м���λͼ��Ϣ�洢��ר�ŵ�λͼҳ������λͼҳ�С�
��ҳ����м������仯ʱ����Ҫ�޸�λͼҳ����һ�����ǱȽϴ�ġ�
Ϊ��ֹ��ҳ�淴�����в���/ɾ���������·����޸�λͼҳ����Ϊ����ҳ����
�������ݺ�ɾ������ʱ������ҳ����м����趨����ֵ�ǲ�һ���ġ�
����ʱ����ֵ���Ǳ�ɾ��ʱ����ֵҪС�������������������ֱ����˲���
��ɾ��ʱ����ҳ����м������ֵ��
*/

/** ��ҳ����м����ʼΪ11 */
#define FLAG_10_LIMIT 0.7	/** ����ʱ����ҳ�ڿ��пռ����70%���޸Ŀ��м���Ϊ10 */ /* ע��MAX_REC_LENGTH�Ķ���͸�ֵ�й� */
#define FLAG_01_LIMIT 0.4	/** ����ʱ����ҳ�ڿ��пռ����40%���޸Ŀ��м���Ϊ01 */
#define FLAG_00_LIMIT 0.1	/** ����ʱ����ҳ�ڿ��пռ����10%���޸Ŀ��м���Ϊ00 */

#define FLAG_11_REGAIN_LIMIT 0.8	/** ɾ��ʱ����ҳ�ڿ��пռ����80%���޸Ŀ��м���Ϊ11 */
#define FLAG_10_REGAIN_LIMIT 0.5	/** ɾ��ʱ����ҳ�ڿ��пռ����50%���޸Ŀ��м���Ϊ10 */
#define FLAG_01_REGAIN_LIMIT 0.2	/** ɾ��ʱ����ҳ�ڿ��пռ����20%���޸Ŀ��м���Ϊ01 */

/**
 * �䳤��¼����ҳ�ṹ
 */
struct VLRHeapHeaderPageInfo {
	HeapHeaderPageInfo m_hhpi;	/** ����ҳ������Ϣ */
	u8 m_centralBmpNum: 7; 		/** ����λͼ�õ�ҳ������Ĭ����1 */
	bool m_cleanClosed: 1; 		/** �ϴ��Ƿ������رգ���Ļ���Ҫ������λͼ���� */
};


/**
 * ��ͨλͼҳ����Ϣ
 */
struct BitmapPageHeader {
	BufferPageHdr	m_bph;	/** ����ҳ����ͷ�ṹ */
	u16 m_pageCount[4];		/** x��ҳ�����ĿΪm_pageCount[x], x = 00, 01, 10, 11, 11������(��Ϊ��׼ȷ��
							    ��������һ��λͼҳ����ÿ�ζ���Ҫ���¼��㣬��Ϊ������չ��ʱ�򣬻���
								���µ�11ҳ�棬���ǲ�Ӱ��������� */
};

#pragma pack(8)
/**
 * ��ͨλͼҳ����λͼ��ȡ�Ͳ��ҵȲ�����
 * λͼ��2bit����ʾҳ����м�����С���пռ�Խ��
 */
class BitmapPage {
public:
	/** ҳ��λͼ���ݵ���ʼƫ�� */
	static const uint MAP_OFFSET = (sizeof(BitmapPageHeader) + 7) & (~7);
	/** һ��λͼҳ��������ļ�¼ҳ����ҳȺ��С */
	static const uint CAPACITY = (Limits::PAGE_SIZE - MAP_OFFSET) * 4;
public:
	union {
		BitmapPageHeader m_header;		/** ��ͷ */
		byte m_fill8width[MAP_OFFSET];	/** ��䵽8�ֽڶ��� */
	} u;
private:
	byte m_map[Limits::PAGE_SIZE - MAP_OFFSET];	/** λͼ */

public:
	/**
	 * ���±�ȡλͼ����(2bits)���޷������棬��Ϊλ����ȡ��ַ���޷�����ֵ
	 * @param idx  �±�����
	 * @return  ��־λ��2bits��һ��u8
	 */
	u8 operator[](int idx) {
		return (u8)(m_map[idx >> 2] >> ((idx % 4) << 1)) & 0x3;
	}

	/** ���±�����λͼ 
	 *
	 * @param idx  �±�����
	 * @param flag  �µ�λͼ��־(2bit)
	 * @param bitSet OUT  ���ĺ��Ŷλͼ������λͼ�еı�־(4bit)
	 * @param mapSize  λͼ��Ч��ʾ�Ĵ�С
	 * @return  ��Ҫ�޸�����λͼΪtrue������Ҫ����false
	 */
	bool setBits(int idx, u8 flag, u8 *bitSet, uint mapSize);

	/** ����λͼ�������±��ֵ
	 *
	 * @param idx1,idx2  �±�����
	 * @param flag1,flag2  �µ�λͼ��־(2bit)
	 * @param bitSet OUT  ���ĺ��Ŷλͼ������λͼ�еı�־(4bit)
	 * @param mapSize  λͼ��Ч��ʾ�Ĵ�С
	 * @return  ��Ҫ�޸�����λͼΪtrue������Ҫ����false
	 */
	bool setBits(u8 *bitSet, uint mapSize, int idx1, u8 flag1, int idx2, u8 flag2);

	/**
	 * ��ʼ��һ��λͼҳ
	 */
	void init();

	/**
	 * ��λͼҳ��Ѱ�ҷ���Ҫ��Ŀ���ҳ��
	 * @param spaceFlag  ��Ҫ��ҳ������
	 * @param bitmapSize  �����ҵ�λͼ�Ĵ�С
	 * @param startPos  ��ʼ���ҵ�λ�ã������Ӱ�������Ƿ��ҵ�����Ϊ����������δ�����ǻ��ͷ���ҵ�)
	 */
	int findFreeInBMP(u8 spaceFlag, uint bitmapSize, int startPos = 0);

#ifdef NTSE_TRACE
	friend Tracer& operator << (Tracer& tracer, BitmapPage *bmp);
#endif
};
#pragma pack()

#pragma pack(1)
/**
 * ��¼ͷ�ṹ��4�ֽڡ���¼������ʱ������Դ��Ŀ�ĵ�RID�洢�ڼ�¼�����У����洢�ڼ�¼ͷ
 */
struct VLRHeader {
	u16	m_offset: 15; 		/** ��¼��ƫ��������λ������ڼ�¼����ʼλ�õģ�������¼ͷ���� */
	u16 m_ifCompressed: 1;  /** ��¼�Ƿ���ѹ����ʽ�� */  
	u16 m_size: 13; 	    /** ��¼�ĳ��ȣ�������6 */
	u16 m_ifEmpty: 1;	    /** ���м�¼�� */
	u16 m_ifLink: 1;	    /** ����Դ */
	u16 m_ifTarget: 1;	    /** ����Ŀ�� */
};


/**
 * �䳤��¼�Ѽ�¼ҳ��ṹ
 */
class VLRHeapRecordPageInfo {
public:
	BufferPageHdr m_bph;	/** ����ҳ����ͷ�ṹ */
	u16 m_recordNum; 		/** ҳ���ڵļ�¼��Ŀ */
	u16 m_freeSpaceSize; 	/** ʣ��ռ��С����λ��byte */
	u16 m_lastHeaderPos; 	/** ���һ����¼ͷ��λ�ã��Ǵ�ҳ�׿�ʼ��ƫ���� */
	u16 m_freeSpaceTailPos; /** �������пռ�ĺ���һ��byte�ĵ�ַ�������ַ����unavailable�� */
	u8 m_pageBitmapFlag;	/** ��ҳ��λͼҳ�еĿ��м����־���Ƿ���Ҫ�޸�λͼҳ�ɴ˶��� */
	u8 m_pad;				/** ��䵽��2�ֽڶ��룬�Ż����ʼ�¼ͷ��Ч�� */
	static const u16 LINK_SIZE = RID_BYTES;	/** ��¼������Ϣռ�õĴ洢�ռ� */
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
 * ����λͼ���ƣ�������λͼ�Ĵ�ȡ�����ҵȹ���
 */
class CentralBitmap {
public:
	/**
	 * ���±�ȡλͼ��Ϣ
	 * @param idx  �±�
	 * @return  λͼ����ͼ��һ���ֽڣ�ֻ��4bits��ʵ����Ч��
	 */
	u8 operator[](int idx) {
		return ((u8 *)(m_pageHdrArray[idx / (Limits::PAGE_SIZE - MAP_OFFSET)]) + MAP_OFFSET)[idx % (Limits::PAGE_SIZE - MAP_OFFSET)];
	}

	/**
	 * �����±�Ϊidx����λͼҳ��־
	 * @param idx           Ŀ���±�
	 * @param bitmapFlag    �±�
	 */
	void setBitmap(int idx, u8 bitmapFlag);

	/**
	 * ���ҵ�һ�����ܺ��з���Ҫ��Ŀ���ҳ���λͼҳ��λ��
	 * 
	 * @param spaceFlag  ����ҳ������
	 * @param startPos  ��ʼ����λ��
	 * @return �ҵ���λͼҳ���±�
	 */
	int findFirstFitBitmap(u8 spaceFlag, int startPos);

private:
	VariableLengthRecordHeap	*m_vlrHeap;	/** �����䳤�� */
	BufferPageHdr	**m_pageHdrArray;		/** ������λͼҳ��ʼ��pin��ҳ�滺���� */
	static const int MAP_OFFSET = (sizeof(BufferPageHdr) + 7) & (~7); /** ҳ��λͼ��Ϣ��ʼƫ�� */

	friend class VariableLengthRecordHeap;
};

#pragma pack()


/* �䳤��¼�� */
class VariableLengthRecordHeap : public DrsHeap {
public:
	VariableLengthRecordHeap(Database *db, Session *session, const TableDef *tableDef, File *heapFile, BufferPageHdr *headerPageHdl, DBObjStats* dbObjStats)  throw(NtseException);
	void close(Session *session, bool flushDirty);

	// ������
	void setPctFree(Session *session, u8 pctFree) throw(NtseException);

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
	bool getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRec);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord);
	void updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy);
	void deleteCurrent(DrsHeapScanHandle *scanHandle);
	void endScan(DrsHeapScanHandle *scanHandle);

	void storePosAndInfo(DrsHeapScanHandle *scanHandle);
	void restorePosAndInfo(DrsHeapScanHandle *scanHandle);

	// redo����
	RowId redoInsert(Session *session, u64 logLSN, const byte *log, uint size, Record *record);
	void redoUpdate(Session *session, u64 logLSN, const byte *log, uint size, const SubRecord *update);
	void redoDelete(Session *session, u64 logLSN, const byte *log, uint size);

	void redoFinish(Session *session);

	/* ͳ����Ϣ */
	void updateExtendStatus(Session *session, uint maxSamplePages);

	/* ���� */
	virtual bool isPageEmpty(Session *session, u64 pageNum);
	bool isPageBmp(u64 pageNum) { return pageIsBitmap(pageNum); }

	static void getRecordFromInsertlog(LogEntry *log, Record *outRec);

	static void initHeader(BufferPageHdr *headerPage, BufferPageHdr **additionalPages, uint *additionalPageNum) throw(NtseException);



#ifdef NTSE_UNIT_TEST // ������
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
	/* ����ͳ�� */
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
	// ��Ҫ���̺���
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
	/* �ؽ�����λͼ */
	void redoCentralBitmap(Session *session);

	/** �������� **/
	/**
	 * @see DrsHeap::getReservedPages
	 */
	virtual uint getReservedPages() {
		return 1 + m_centralBmpNum;
	}
	/**
	 * �ж�һ��ҳ���Ƿ���λͼҳ
	 * @param pageNum  ҳ���
	 * @return  ��λͼҳ����true
	 */
	bool pageIsBitmap(u64 pageNum) {
		return !((pageNum - m_centralBmpNum - 1) % (BitmapPage::CAPACITY + 1));
	}

	/**
	* �ж�һ��ҳ���Ƿ���λͼҳ�������ж�
	* @param pageNum      ҳ���
	* @param nextBmpNum   ��һ��λͼ��
	* @return  ��λͼҳ����true
	*/
	bool pageIsBitmap(u64 pageNum, u64& nextBmpNum) {
		return (pageNum < nextBmpNum) ? false : ((nextBmpNum += (BitmapPage::CAPACITY + 1)), true);
	}

	/**
	 * ��bitmap������λͼ�е�indexתΪҳ��š�
	 * @param bmpIdx   λͼ������λͼ�е��±�
	 * @return         ҳ���
	 */
	u64 bmpIdx2Num(int bmpIdx) {
		return bmpIdx * (BitmapPage::CAPACITY + 1) + 1 + m_centralBmpNum;
	}
	
	/**
	 * ��bitmapҳ���ҳ���תΪ������λͼ�е��±�ֵ��
	 * @param bmpPageNum   ҳ���
	 * @return             λͼ������λͼ�е��±�
	 */
	int bmpNum2Idx(u64 bmpPageNum) {
		assert(pageIsBitmap(bmpPageNum));
		return (int)((bmpPageNum - 1 - m_centralBmpNum) / (BitmapPage::CAPACITY + 1));
	}

	/**
	 * ��λͼ��ҳ����±�ֵ�õ�ҳ��š�
	 * @param bitmapIdx      λͼ�±�
	 * @param pageIdx        ҳ���±�
	 * @return               ҳ���
	 */
	u64 getPageNumByIdx(int bitmapIdx, int pageIdx) {
		return bmpIdx2Num(bitmapIdx) + pageIdx + 1;
	}
	/**
	 * �ɼ�¼ҳ��ŵõ���Ӧ��bitmapҳ��
	 * @param pageNum     ҳ���
     * @return            ��Ӧ��λͼҳ��
	 */
	u64 getBmpNum(u64 pageNum) {
		return pageNum - (pageNum - 1 - m_centralBmpNum) % (BitmapPage::CAPACITY + 1);
	}

#ifdef NTSE_UNIT_TEST  //������
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

	/* log���� */
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


	/* ���� */
#ifdef NTSE_VERIFY_EX
	bool heavyVerify(VLRHeapRecordPageInfo *page);
#endif

	static const u16 LINK_SIZE = VLRHeapRecordPageInfo::LINK_SIZE;				/** �������ݵĴ�С */
	static const uint DEFAULT_CBITMAP_SIZE = 1;									/** ����λͼ��Ĭ�ϴ�С */
	//static const uint OFFSET_HEADER_TABLEDEF = sizeof(VLRHeapHeaderPageInfo);	/** ����ƫ���� */
	/** ���пռ�ȼ� */
	static const uint FLAG10_SPACE_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_10_LIMIT);
	static const uint FLAG01_SPACE_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_01_LIMIT);
	static const uint FLAG00_SPACE_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_00_LIMIT);
	/** ���пռ併���ȼ� */
	static const uint FLAG11_SPACE_DOWN_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_11_REGAIN_LIMIT);
	static const uint FLAG10_SPACE_DOWN_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_10_REGAIN_LIMIT);
	static const uint FLAG01_SPACE_DOWN_LIMIT = (uint)((Limits::PAGE_SIZE - sizeof(VLRHeapRecordPageInfo)) * FLAG_01_REGAIN_LIMIT);


	uint	m_centralBmpNum;	            /** ���ѵ�����λͼ��С����λ��ҳ���� */
	BufferPageHdr	**m_centralBitmapHdr;	/** ����λͼҳ�ľ���� */
	CentralBitmap	m_cbm;		            /** ����λͼ���� */
	u64		m_bitmapNum;		            /** �ط�λͼҳ����������ҳ�����ɼ����������Ҫ�־û� */
	u64		m_lastBmpNum;		            /** ���һ��λͼҳ�� */
	uint 	m_reserveSize; 		            /** Ԥ���ռ��С���ֽ��� */

	struct {
		int m_bitmapIdx;	/** �ط�λͼҳ������λͼҳ�е��±� */
		int m_pageIdx;		/** ����ҳ�ڵط�λͼҳ�е��±� */
	} m_position[4];		/** �ϴλ�õ�ĳ�����͵�ҳ��λ�� */
	RWLock m_posLock;		/** ��������λ����Ϣ���� */

	friend class CentralBitmap;
public:
	static const u16 MAX_VLR_LENGTH = (FLAG10_SPACE_LIMIT - 1) - sizeof(VLRHeader); /** ���䳤��¼���� */

	friend Tracer& operator << (Tracer& tracer, VariableLengthRecordHeap *heap);
};

} // namespace ntse;



#endif
