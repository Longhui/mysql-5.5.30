/**
* NTSE B+树索引管理类
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
	SubRecord *m_idxKey1, *m_idxKey2;					/** 充当临时变量 */
	SubRecord *m_lastRecord[Limits::MAX_BTREE_LEVEL];	/** 各层的上一个键值 */
	PageHandle *m_pageHandle[Limits::MAX_BTREE_LEVEL];	/** 各层的PageHandle */
	PageId	m_pageId[Limits::MAX_BTREE_LEVEL];			/** 各层的pageId */
	u32 m_pageMark;										/** 创建索引的各个页面统一标志 */
	u8 m_indexId;										/** 创建索引的ID */
	u8 m_indexLevel;									/** 当前层数 */
};

/** 该类用来维护一个内存索引顺序，该顺序的规规为：
*	1. 所有唯一性索引排列在非唯一性索引之前
*	2. 无论是唯一索引还是非唯一索引，它们的顺序都按照索引内部的ID大小来排序，保证无论哪次系统启动，这个顺序都是稳定的
* 例如：假设id为1，3，5的是唯一索引，2，4的是非唯一索引，在有序维护时会存储为(1 3 5 2 4)
* 对该类进行的访问，需要通过上层表结构修改锁同步
* 该类通过1保证修改索引的时候有序修改，提高并发度，同时由2来保证恢复正确，因为不按照固定的顺序排序，insert和update的恢复可能会不正确
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
	u16		m_orderedIdxNo[Limits::MAX_INDEX_NUM];	/** 保存索引序号，按照先唯一性后非唯一排序，
													open的时候初始化，增加/删除索引的时候需要调整 */
	u8		m_orderedIdxId[Limits::MAX_INDEX_NUM];	/** 各个有序索引对应的id值，用于比较顺序 */
	u16		m_uniqueIdxNum;		/** 索引集当中包含的唯一索引个数 */
	u8		m_indexNum;			/** 当前保存索引的个数 */
};

class DrsBPTreeIndice : public DrsIndice {
public:
	DrsBPTreeIndice(Database *db, const TableDef *tableDef, File *file, LobStorage *lobStorage, Page *headerPage, DBObjStats *dbObjStats);

	~DrsBPTreeIndice() {}

	// 操作接口实现
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

	// 恢复接口实现
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

	// redo补偿日志
	void redoCpstDML(Session *session, u64 lsn, const byte *log, uint size);
	void redoCpstCreateIndex(Session *session, u64 lsn, const byte *log, uint size);
	void redoCpstSMO(Session *session, u64 lsn, const byte *log, uint size);
	void redoCpstPageSet(Session *session, u64 lsn, const byte *log, uint size);

	// 补充恢复接口
	void recvInsertIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN);
	void recvDeleteIndexEntries(Session *session, const Record *record, s16 lastDoneIdxNo, u64 beginLSN);
	void recvUpdateIndexEntries(Session *session, const SubRecord *before, const SubRecord *after, s16 lastDoneIdxNo, u64 beginLSN, bool isUpdateLob);
	u8 recvCompleteHalfUpdate(Session *session, const SubRecord *record, s16 lastDoneIdxNo, u16 updateIdxNum, u16 *updateIndices, bool isUpdateLob);

	void logDMLDoneInRecv(Session *session, u64 beginLSN, bool succ = false);
	void getUpdateIndices(MemoryContext *memoryContext, const SubRecord *update, u16 *updateNum, u16 **updateIndices, u16 *updateUniques);

	// 内部使用
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

	// 创建索引相关函数
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
	Database			*m_db;				/** 数据库 */
	const TableDef		*m_tableDef;		/** 所属表的定义 */
	File				*m_file;			/** 索引文件 */
	LobStorage			*m_lobStorage;		/** 大对象存储 */
	DrsIndex			**m_indice;			/** 各个索引 */
	u16					m_indexNum;			/** 索引个数 */
	IndicePageManager	*m_pagesManager;	/** 管理分配、回收各个索引页面 */
	IndexLog			*m_logger;			/** 日志管理器 */
	Mutex				*m_mutex;			/** 同步索引文件修改操作的互斥量 */
	s16					m_newIndexId;		/** 保存新建的索引的ID号，在创建索引过程中做临时变量
											执行createIndexPhaseOne之后成功为正数，否则为-1*/
	DrsIndex			*m_droppedIndex;	/** 保存准备要丢弃的索引指针，用于dropIndexPhaseOne之后
											在PhaseTwo过程中还需要使用该索引指针，并且使用结束才delete*/
	DBObjStats			*m_dboStats;		/** 索引页面分配图数据对象状态结构 */
	OrderedIndices		m_orderedIndices;	/** 内存当中维护的有序索引队列 */
};


/**
* 对索引文件进行分配回收页面操作的管理类
* 需要注意的是外层使用该类的时候是先加了某个页面的锁，然后要求释放页面或者直接要求加某个新页面的锁
* 而本管理类内部的加锁逻辑必须是先加了头页面的锁，然后再加位图页的锁，反之不行，避免死锁
* 基于以上假设，该类内部的同步基本都依靠对头页面的加互斥锁完成
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
	const TableDef	*m_tableDef;/** 索引所属的表定义 */
	File		*m_file;		/** 索引文件 */
	IndexHeaderPage	*m_headerPage;	/** 索引文件头页面 */
	Syslog		*m_logger;		/** 系统日志 */
	u64			m_fileSize;		/** 索引文件大小 */
	DBObjStats 	*m_dbObjStats;	/**	数据对象状态 */

public:
	/** 
	 *	索引文件头、位图相关常量，位图实际可表示范围=BITMAP_PAGE_SIZE*1024*8*FILE_BLOCK_SIZE
	 *		=(128*8*1024*8bit)*(128*8K)=8T，应该足够使用
	 */
	static const uint HEADER_PAGE_NUM = 1;											/** 索引头信息页面数——假设一个页面足够写入 */
	//#ifdef NTSE_MYSQL_TEST
	//	// 对于mysql_test使用的版本，这里参数设置比较小，为了使连续创建和删除表的效率能够提升，参见QA23956
	//	static const uint PAGE_BLOCK_NUM = 16;											/** 索引文件中一个控制位图位表示的范围内页面个数 */
	//	static const uint BITMAP_PAGE_NUM = 2;											/** 索引文件控制位图所占页面数 */
	//#else
	//	// 正常使用时，为了保证可用性和性能，应该设置比较大
	//	static const uint PAGE_BLOCK_NUM = 128;											/** 索引文件中一个控制位图位表示的范围内页面个数 */
	//	static const uint BITMAP_PAGE_NUM = 128;										/** 索引文件控制位图所占页面数 */
	//#endif
	static const uint PAGE_BLOCK_NUM = 128;											/** 索引文件中一个控制位图位表示的范围内页面个数 */
	static const uint BITMAP_PAGE_NUM = 128;										/** 索引文件控制位图所占页面数 */
	static const uint PAGE_BLOCK_SIZE = PAGE_BLOCK_NUM * Limits::PAGE_SIZE;			/** 索引文件一个控制位图位代表的页面范围大小 */
	static const uint BITMAP_PAGE_SIZE = BITMAP_PAGE_NUM * Limits::PAGE_SIZE;								/** 索引文件控制位图所占页面范围大小 */
	static const uint BITMAP_PAGE_REPRESENT_PAGES = (Limits::PAGE_SIZE - sizeof(Page)) * PAGE_BLOCK_NUM;	/** 一个索引位图页可表示的索引页面数 */
	static const uint NON_DATA_PAGE_NUM = HEADER_PAGE_NUM + BITMAP_PAGE_NUM;								/** 索引文件头部非数据页面页面数 */
	static const uint MAX_FREE_BITMAP = NON_DATA_PAGE_NUM * Limits::PAGE_SIZE;								/** 索引位图位最大值 */
};

}


#endif

