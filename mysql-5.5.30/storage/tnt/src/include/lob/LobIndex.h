/**
 * 大型大对象的目录文件管理
 *
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */
#ifndef _NTSE_LOBINDEX_H_
#define _NTSE_LOBINDEX_H_

#include "misc/Buffer.h"
#include "util/Portable.h"
#include "misc/Sample.h"
#include "misc/Session.h"


namespace ntse {

class Database;
class MemoryContext;
class BufferPageHandle;
class Connection;

#define LOB_INDEX_FILE_EXTEND 16    /** 目录文件扩张时候增加的页数 */
#define LOB_INDEX_FREE_BITS	1		/** 是否空闲的标志位占用位数 */
#define LOB_INDEX_PAGE_BITS	31	    /** 页号占用位数 */

/** 根据目录项得到首位的标志位 */
#define NID_GET_FREE(lid)	((u8)((lid) >>PAGE_BITS))
/** 根据目录项得到PageID，PageID为u32类型，但实际上只能表示2^31个目录页 */
#define NID_GET_PAGE(lid)	((u32)((lid) & (u32)0x7FFFFFFF))
/** 根据页号和标志位得到目录项内容 */
#define NID(free, page)		((u32)(((free) << PAGE_BITS) | (u32)page)))

/** 目录文件首页结构 */
struct LIFileHeaderPageInfo {
	BufferPageHdr m_bph;				/** 页共享结构 */
	u32		m_fileLen;					/** 目录文件长度 */
	u32		m_firstFreePageNum;			/** 堆中第一个空闲数据页，0表示没有 */
	u32		m_blobFileLen;              /** 大对象文件长度 */
	u32		m_blobFileTail;             /** 大对象文件已经分配的长度 */ 
	u32		m_blobFileFreeLen;          /** 大对象空闲块的总长度 */
	u32		m_blobFileFreeBlock;        /** 大对象文件空闲块的数目 */ 
	u16		m_tableId;                  /** 所属表的ID,为写日志用 */  
};

#pragma pack(1) // 1字节对齐
/** 目录文件的页头结构 */
struct LIFilePageInfo {
	BufferPageHdr	m_bph;	    /** 缓存页公用头结构 */
	s16		m_firstFreeSlot;    /** 第一个空闲记录槽 */
	bool	m_inFreePageList: 1;/** 是否在空闲页链表中 */ 
	u16     m_freeSlotNum;      /** 页中空闲的槽数 */ 
	u32		m_nextFreePageNum;  /** 下个空闲页面 */	
};

/** 记录槽头部结构 */
struct LiFileSlotInfo {
	bool	m_free: 1;			/** 是否为空闲记录槽 */
	union {
		s16		m_nextFreeSlot;	/** 下一空闲记录槽，只在本身为空闲记录槽时有用 */
		u32 	m_pageId;		/** 数据表示大对象起始页号，只在不为空闲记录槽时有用 */
	} u;
};
#pragma pack()

/** 大型大对象索引管理类 */
class LobIndex : public Analysable {
public :
	LobIndex(Database *db, File *file, LIFileHeaderPageInfo *liHeaderPage, DBObjStats *dbObjStats);
	~LobIndex();

	static void create(const char *path, Database *db, const TableDef *tableDef) throw(NtseException);
	static void redoCreate(Database *db, const TableDef *tableDef, const char * path, u16 tid);
	static LobIndex* open (const char *path, Database *db, Session *session) throw(NtseException);
	void close(Session *session, bool flushDirty);
	void setTableId(Session *session, u16 tableId);

	LobId getFreeSlot(Session *session, BufferPageHandle **reFreePageHdl, bool *isHeaderPageLock, BufferPageHandle **reHeaderPageHdl, u32 *newFreeHeader, u32 pageId);

	//得到文件句柄
	inline File* getFile () {
		return m_file;
	}
	
	/** 采样接口 */
	virtual SampleHandle *beginSample(Session *session,uint maxSampleNum, bool fastSample);
	virtual Sample * sampleNext(SampleHandle *handle);
	virtual void endSample(SampleHandle *handle);
	u32 getMaxUsedPage(Session *session);
	u64 metaDataPgCnt() { return (u64)1; }

private:
	u16 getTableID();
	BufferPageHandle* getFreePage(Session *session, u64 *pageNum);
	LiFileSlotInfo* getSlot(LIFilePageInfo *pageRecord, u16 slotNum);
	byte* createNewPagesInMem(uint size, u32 fileLen);
	void extendIndexFile(Session *session, LIFileHeaderPageInfo *headerPage, uint extendSize, u32 indexFileLen);
	void unlockHeaderPage(Session *session, BufferPageHandle *bufferHdl);
	BufferPageHandle* lockHeaderPage(Session *session, LockMode lockMode);
	void verifyPage(Session *session, u32 pid);
	
	/** 下面是状态采样所用 */
	static const uint SAMPLE_FIELDS_NUM = 1;
	void selectPage(u64 *outPages, uint wantNum, u64 min, u64 regionSize);
	uint unsamplablePagesBetween(u64 minPage, u64 regionSize);
	Sample* sample(u64 pageId, Session *session);
	Sample* sampleBufferPage(Session *session, BufferPageHdr *page);

	/** 记录槽大小:这里每个槽是5个字节 */
	static const uint INDEX_SLOT_LENGTH = sizeof(LiFileSlotInfo);	
	/** 目录文件在首页信息起始偏移 */
	static const uint OFFSET_HEADER_INFO = sizeof(LIFileHeaderPageInfo);
	/** 记录数据在数据页的起始偏移 */
	static const uint OFFSET_PAGE_RECORD = sizeof(LIFilePageInfo);
	/** 一个目录页最多可以包含多少个目录项 */
	static const uint MAX_SLOT_PER_PAGE = (Limits::PAGE_SIZE - OFFSET_PAGE_RECORD) / INDEX_SLOT_LENGTH;

	Database *m_db;			             /** 数据库 */
	Buffer 	*m_buffer;		             /** 页面缓存管理器 */
	File  *m_file;                       /** 对应的文件 */  
	LIFileHeaderPageInfo *m_headerPage;  /** 目录文件首页，要PIN在内存中 */
	u16 m_tableId;                       /** 对应的母表ID */ 
	u32 m_maxUsedPage;                   /** 目前最大的使用的页 */
	DBObjStats *m_dboStats;			 	 /** 大型大对象目录状态 */

	friend class BigLobStorage;
	friend class LobStorage;

#ifdef NTSE_UNIT_TEST
	friend class ::LobOperTestCase;
	friend class ::LobDefragTestCase;
#endif
};

struct BufScanHandle;
class LobIndexSampleHandle : public SampleHandle { 
public:
	LobIndexSampleHandle(Session * session, uint maxSampleNum, bool fastSample) : SampleHandle(session, maxSampleNum, fastSample), m_blockPages(NULL), m_bufScanHdl(NULL) {
		m_minPage = 0;
		m_maxPage = 0;
		m_regionSize = 0;
		m_blockNum = 0;
		m_curBlock = 0;
		m_blockPages = NULL;
		m_blockSize = 0;
		m_curIdxInBlock = 0;
	}
	
	/* buffer 采样句柄 */
	/* 以下变量是为目录文件采样 */
	u64 m_minPage;		/** 最小的采样页范围 */
	u64 m_maxPage;		/** 最大的采样页范围 */
	u64 m_regionSize;   /** 一个采样区的范围 */
	uint m_blockNum, m_curBlock;
	u64 *m_blockPages;
	uint m_blockSize, m_curIdxInBlock;
	BufScanHandle *m_bufScanHdl;
	
};

}

#endif

