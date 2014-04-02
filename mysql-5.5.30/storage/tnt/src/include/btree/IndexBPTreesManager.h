/**
* NTSE B+������������
* 
* author: naturally (naturally@163.org)
*/

#ifndef _NTSE_INDEX_BPTREESMANAGER_H_
#define _NTSE_INDEX_BPTREESMANAGER_H_

#include "Index.h"
#include "IndexPage.h"
#include "misc/Buffer.h"

namespace ntse {

class IndexHeaderPage;
class IndexLog;
class Database;
class TableDef;
class DrsHeap;
class SubRecord;
class Record;
class File;
class Session;
class IndicePageManager;
class BufferPageHandle;
class DrsIndexScanHandleInfo;
class Syslog;
struct Mutex;
struct BufferPageHdr;
class vector;
struct DBObjStats;

#define Page BufferPageHdr
#define PageHandle BufferPageHandle


class BPTreeCreateTrace {
public:
	SubRecord *m_idxKey1, *m_idxKey2;					/** �䵱��ʱ���� */
	SubRecord *m_lastRecord[Limits::MAX_BTREE_LEVEL];	/** �������һ����ֵ */
	PageHandle *m_pageHandle[Limits::MAX_BTREE_LEVEL];	/** �����PageHandle */
	PageId	m_pageId[Limits::MAX_BTREE_LEVEL];			/** �����pageId */
	u32 m_pageMark;										/** ���������ĸ���ҳ��ͳһ��־ */
	u8 m_indexId;										/** ����������ID */
	u8 m_indexLevel;									/** ��ǰ���� */
};

/** ��������ά��һ���ڴ�����˳�򣬸�˳��Ĺ��Ϊ��
*	1. ����Ψһ�����������ڷ�Ψһ������֮ǰ
*	2. ������Ψһ�������Ƿ�Ψһ���������ǵ�˳�򶼰��������ڲ���ID��С�����򣬱�֤�����Ĵ�ϵͳ���������˳�����ȶ���
* ���磺����idΪ1��3��5����Ψһ������2��4���Ƿ�Ψһ������������ά��ʱ��洢Ϊ(1 3 5 2 4)
* �Ը�����еķ��ʣ���Ҫͨ���ϲ��ṹ�޸���ͬ��
* ����ͨ��1��֤�޸�������ʱ�������޸ģ���߲����ȣ�ͬʱ��2����֤�ָ���ȷ����Ϊ�����չ̶���˳������insert��update�Ļָ����ܻ᲻��ȷ
*/
class OrderedIndices {
public:
	OrderedIndices();

	bool add(u16 order, u8 idxId, bool unique);
	bool remove(u16 order, bool unique);
	u16 getOrder(u16 no) const;
	u16 getUniqueIdxNum() const;
	u16 find(u16 order) const;
	void getOrderUniqueIdxNo(const u16 ** const orderUniqueIdxNo, u16 *uniqueIdxNum) const;

private:
	u16		m_orderedIdxNo[Limits::MAX_INDEX_NUM];	/** ����������ţ�������Ψһ�Ժ��Ψһ����
													open��ʱ���ʼ��������/ɾ��������ʱ����Ҫ���� */
	u8		m_orderedIdxId[Limits::MAX_INDEX_NUM];	/** ��������������Ӧ��idֵ�����ڱȽ�˳�� */
	u16		m_uniqueIdxNum;		/** ���������а�����Ψһ�������� */
	u8		m_indexNum;			/** ��ǰ���������ĸ��� */
};

class DrsBPTreeIndice : public DrsIndice {
public:
	DrsBPTreeIndice(Database *db, const TableDef *tableDef, File *file, LobStorage *lobStorage, Page *headerPage, DBObjStats *dbObjStats);

	~DrsBPTreeIndice() {}

	// �����ӿ�ʵ��
	bool insertIndexEntries(Session *session, const Record *record, uint *dupIndex);
#ifdef TNT_ENGINE
	void insertIndexNoDupEntries(Session *session, const Record *record);
#endif
	void deleteIndexEntries(Session *session, const Record *record, IndexScanHandle* scanHandle);
		bool updateIndexEntries(Session *session, const SubRecord *before, SubRecord *after, bool updateLob, uint *dupIndex);
	void close(Session *session, bool flushDirty);
	void flush(Session *session);
	void dropPhaseOne(Session *session, uint idxNo);
	void dropPhaseTwo(Session *session, uint idxNo);
	void createIndexPhaseOne(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap) throw(NtseException);
	DrsIndex* createIndexPhaseTwo(const IndexDef *def);
	DrsIndex* getIndex(uint index);
	uint getIndexNum() const;
	uint getUniqueIndexNum() const;
	int getFiles(File** files, PageType* pageTypes, int numFile);
	u64 getDataLength(bool includeMeta = true);
	int getIndexNo(const BufferPageHdr *page, u64 pageId);
	LobStorage* getLobStorage();

	// �ָ��ӿ�ʵ��
	void redoCreateIndexEnd(const byte *log, uint size);
	s32 redoDropIndex(Session *session, u64 lsn, const byte *log, uint size);

	void redoDML(Session *session, u64 lsn, const byte *log, uint size);
	void redoSMO(Session *session, u64 lsn, const byte *log, uint size);
	void redoPageSet(Session *session, u64 lsn, const byte *log, uint size);
	bool isIdxDMLSucceed(const byte *log, uint size);
	u8 getLastUpdatedIdxNo(const byte *log, uint size);

	// undo
	void undoDML(Session *session, u64 lsn, const byte *log, uint size, bool logCPST);
	void undoSMO(Session *session, u64 lsn, const byte *log, uint size, bool logCPST);
	void undoPageSet(Session *session, u64 lsn, const byte *log, uint size, bool logCPST);
	void undoCreateIndex(Session *session, u64 lsn, const byte *log, uint size, bool logCPST);

	// redo������־
	void redoCpstDML(Session *session, u64 lsn, const byte *log, uint size);
	void redoCpstCreateIndex(Session *session, u64 lsn, const byte *log, uint size);
	void redoCpstSMO(Session *session, u64 lsn, const byte *log, uint size);
	void redoCpstPageSet(Session *session, u64 lsn, const byte *log, uint size);

	// ����ָ��ӿ�
	void recvInsertIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN);
	void recvDeleteIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN);
	void recvUpdateIndexEntries(Session *session, const SubRecord *before, const SubRecord *after, s16 lastDoneIdxNo, u64 beginLSN, bool isUpdateLob);
	u8 recvCompleteHalfUpdate(Session *session, const SubRecord *record, s16 lastDoneIdxNo, u16 updateIdxNum, u16 *updateIndices, bool isUpdateLob);

	void logDMLDoneInRecv(Session *session, u64 beginLSN, bool succ = false);
	void getUpdateIndices(MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, u16 **updateIndices, u16 *updateUniques);

	// �ڲ�ʹ��
	Database* getDatabase();
	File* getFileDesc();
	u16 getTableId();
	IndicePageManager* getPagesManager();
	IndexLog* getLogger();
	const OrderedIndices* getOrderedIndices() const;

	DBObjStats* getDBObjStats();

#ifdef NTSE_UNIT_TEST
	void decIndexNum() { m_indexNum--; }

	void incIndexNum() { m_indexNum++; }
#endif

private:
	void recvMemSync(Session *session, u16 indexId);

	// ����������غ���
	u8 createNewIndex(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap) throw(NtseException);
	BPTreeCreateTrace* allocCreateTrace(MemoryContext *memoryContext, const IndexDef *def, u8 indexId);
	void appendIndexFromHeapBySort(Session *session, const IndexDef *indexDef, const TableDef *tblDef, DrsHeap *heap, u8 indexId, PageId rootPageId, PageHandle *rootHandle) throw(NtseException); 
	PageHandle* createNewPageAndAppend(Session *session, BPTreeCreateTrace *createTrace, u8 level, const SubRecord *appendKey, PageId *pageId, IndexPageType pageType);
	void appendIndexNonLeafPage(Session *session, BPTreeCreateTrace *createTrace);
	void updateLevelInfo(Session *session, BPTreeCreateTrace *createTrace, u16 level, PageId pageId, PageHandle *pageHandle);
	void completeIndexCreation(Session *session, BPTreeCreateTrace *createTrace);
	void unlatchAllTracePages(Session *session, BPTreeCreateTrace *createTrace);
	void latchPage(Session *session, PageHandle *pageHandle, LockMode lockMode);
	void markDirtyAndUnlatch(Session *session, PageHandle *pageHandle);
	void unPinAllTracePages(Session *session, BPTreeCreateTrace *createTrace);
	bool isKeyEqual(const IndexDef *indexDef, SubRecord *key1, SubRecord *key2, SubRecord *padKey, bool isFastable);
	void swapKeys(SubRecord *lastRecord, SubRecord *appendKey, Page *page);
	void createNewRoot(Session *session, BPTreeCreateTrace *createTrace, SubRecord *appendKey);

	static bool bufferCallBackSpecifiedIDX(Page *page, PageId pageId, uint indexId);

private:
	Database			*m_db;				/** ���ݿ� */
	const TableDef		*m_tableDef;		/** ������Ķ��� */
	File				*m_file;			/** �����ļ� */
	LobStorage			*m_lobStorage;		/** �����洢 */
	DrsIndex			**m_indice;			/** �������� */
	u16					m_indexNum;			/** �������� */
	IndicePageManager	*m_pagesManager;	/** ������䡢���ո�������ҳ�� */
	IndexLog			*m_logger;			/** ��־������ */
	Mutex				*m_mutex;			/** ͬ�������ļ��޸Ĳ����Ļ����� */
	s16					m_newIndexId;		/** �����½���������ID�ţ��ڴ�����������������ʱ����
											ִ��createIndexPhaseOne֮��ɹ�Ϊ����������Ϊ-1*/
	DrsIndex			*m_droppedIndex;	/** ����׼��Ҫ����������ָ�룬����dropIndexPhaseOne֮��
											��PhaseTwo�����л���Ҫʹ�ø�����ָ�룬����ʹ�ý�����delete*/
	DBObjStats			*m_dboStats;		/** ����ҳ�����ͼ���ݶ���״̬�ṹ */
	OrderedIndices		m_orderedIndices;	/** �ڴ浱��ά���������������� */
};


/**
* �������ļ����з������ҳ������Ĺ�����
* ��Ҫע��������ʹ�ø����ʱ�����ȼ���ĳ��ҳ�������Ȼ��Ҫ���ͷ�ҳ�����ֱ��Ҫ���ĳ����ҳ�����
* �����������ڲ��ļ����߼��������ȼ���ͷҳ�������Ȼ���ټ�λͼҳ��������֮���У���������
* �������ϼ��裬�����ڲ���ͬ��������������ͷҳ��ļӻ��������
*/
class IndicePageManager {
public:
	IndicePageManager(const TableDef *tableDef, File *file, Page *headerPage, Syslog *syslog, DBObjStats* dbObjStats);

	u64 calcPageBlock(IndexLog *logger, Session *session, u8 indexId);
	PageHandle* allocPageBlock(IndexLog *logger, Session *session, u64 blockOffset, u8 indexId);
	PageHandle* allocPage(IndexLog *logger, Session *session, u8 indexId, PageId *pageId);
	PageHandle* createNewIndexRoot(IndexLog *logger, Session *session, u8 *indexNo, u8 *indexId, PageId *rootPageId);
	void freePage(IndexLog *logger, Session *session, PageId pageId, Page *indexPage);
	u32 freePageBlocksByIDXId(Session *session, u8 indexId, bool setZero, u32 maxFreeBit, u64 opLsn, bool isRedo);
	void discardIndexByIDXId(Session *session, IndexLog *logger, u8 indexId, s32 idxNo);
	void discardIndexByIDXIdRecv(Session *session, u8 indexId, bool isRedo, u64 opLsn);

	PageId getPageBlockStartId(PageId bmPageId, u16 offset);
	u8 getIndexNo(u8 indexId);
	bool isPageByteMapPage(PageId pageId);
	bool extendFileIfNecessary(PageId blockStartPageId);
	void updateIndexRootPageId(IndexLog *logger, Session *session, u8 indexNo, PageId rootPageId);

	void updateNewStatus(u8 indexId, struct IndexStatus *status);

	u64 getFileSize() { return m_fileSize; }
	IndexHeaderPage* getFileHeaderPage() { return m_headerPage; }

	static void initIndexFileHeaderAndBitmap(File *file);

private:
	const TableDef	*m_tableDef;/** ���������ı��� */
	File		*m_file;		/** �����ļ� */
	IndexHeaderPage	*m_headerPage;	/** �����ļ�ͷҳ�� */
	Syslog		*m_logger;		/** ϵͳ��־ */
	u64			m_fileSize;		/** �����ļ���С */
	DBObjStats 	*m_dbObjStats;	/**	���ݶ���״̬ */

public:
	/** 
	 *	�����ļ�ͷ��λͼ��س�����λͼʵ�ʿɱ�ʾ��Χ=BITMAP_PAGE_SIZE*1024*8*FILE_BLOCK_SIZE
	 *		=(128*8*1024*8bit)*(128*8K)=8T��Ӧ���㹻ʹ��
	 */
	static const uint HEADER_PAGE_NUM = 1;											/** ����ͷ��Ϣҳ������������һ��ҳ���㹻д�� */
	//#ifdef NTSE_MYSQL_TEST
	//	// ����mysql_testʹ�õİ汾������������ñȽ�С��Ϊ��ʹ����������ɾ�����Ч���ܹ��������μ�QA23956
	//	static const uint PAGE_BLOCK_NUM = 16;											/** �����ļ���һ������λͼλ��ʾ�ķ�Χ��ҳ����� */
	//	static const uint BITMAP_PAGE_NUM = 2;											/** �����ļ�����λͼ��ռҳ���� */
	//#else
	//	// ����ʹ��ʱ��Ϊ�˱�֤�����Ժ����ܣ�Ӧ�����ñȽϴ�
	//	static const uint PAGE_BLOCK_NUM = 128;											/** �����ļ���һ������λͼλ��ʾ�ķ�Χ��ҳ����� */
	//	static const uint BITMAP_PAGE_NUM = 128;										/** �����ļ�����λͼ��ռҳ���� */
	//#endif
	static const uint PAGE_BLOCK_NUM = 128;											/** �����ļ���һ������λͼλ��ʾ�ķ�Χ��ҳ����� */
	static const uint BITMAP_PAGE_NUM = 128;										/** �����ļ�����λͼ��ռҳ���� */
	static const uint PAGE_BLOCK_SIZE = PAGE_BLOCK_NUM * Limits::PAGE_SIZE;			/** �����ļ�һ������λͼλ�����ҳ�淶Χ��С */
	static const uint BITMAP_PAGE_SIZE = BITMAP_PAGE_NUM * Limits::PAGE_SIZE;								/** �����ļ�����λͼ��ռҳ�淶Χ��С */
	static const uint BITMAP_PAGE_REPRESENT_PAGES = (Limits::PAGE_SIZE - sizeof(Page)) * PAGE_BLOCK_NUM;	/** һ������λͼҳ�ɱ�ʾ������ҳ���� */
	static const uint NON_DATA_PAGE_NUM = HEADER_PAGE_NUM + BITMAP_PAGE_NUM;								/** �����ļ�ͷ��������ҳ��ҳ���� */
	static const uint MAX_FREE_BITMAP = NON_DATA_PAGE_NUM * Limits::PAGE_SIZE;								/** ����λͼλ���ֵ */
};

}


#endif

