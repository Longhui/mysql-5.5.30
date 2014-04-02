/**
 * 大型大对象管理
 *
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */
#ifndef _NTSE_BIGLOB_H_
#define _NTSE_BIGLOB_H_

#include "lob/Lob.h"
#include "lob/BigLob.h"
#include "lob/LobIndex.h"
#include "misc/Buffer.h"
#include "util/Portable.h"
#include "misc/Sample.h"
#include "misc/Session.h"

namespace ntse {

class Database;
class MemoryContext;
class BufferPageHandle;
struct Stream;


#define LOB_LEN_EXTEND sizeof(u32)		/** 大对象长度占有长度 */
#define LOB_PAGE_BITS	32				/** 页号占用位数 */
#define LOB_SLOT_BITS	16				/** 记录槽号占用位数 */
//#define LOB_FILE_EXTEND 1024			/** 大对象文件每次扩张的页数 */

/** 根据LOBID得到页号，页号为u64类型，但实际上只能表示2^32个页 */
#define LID_GET_PAGE(lid)	((u32)(((lid) >> LOB_SLOT_BITS) & ((((u64)1) << LOB_PAGE_BITS) - 1)))
/** 根据LOBID得到记录槽号，记录槽号为u16类型，但实际上只能表示2^16个记录 */
#define LID_GET_SLOT(lid)	((u16)((lid) & (u64)0x1FFF))
 
/** 根据页号和记录槽号得到LobId */
#define LID(page, slot)		((LobId)((((u64)page) << LOB_SLOT_BITS) | (u64)slot))


#pragma pack(1) // 1字节对齐
/** 大型大对象块的首页头 */
struct LobBlockFirstPage {  
	BufferPageHdr m_bph;		/** 页共享结构 */
	bool	m_isFirstPage: 1;	/** 是否是块首页，假如是就为1 */
	bool	m_isFree: 1;		/** 块是否空闲 */
	LobId	m_lid;				/** 大对象的ID */	
	u32		m_len;				/** 大对象长度 */
	u32		m_srcLen;			/** 不压缩的长度 */
};

/** 大型大对象其他的页头 */
struct LobBlockOtherPage {
	BufferPageHdr m_bph;	/** 页共享结构 */
	bool m_isFirstPage: 1;	/** 是否是块首页，这里为0 */ 
};
#pragma pack()


/** 大型大对象 */
class BigLobStorage : public Analysable{
public:
	BigLobStorage (Database *db, File *file, LobIndex *lobi, DBObjStats *datStats);
	~BigLobStorage();

	// DDL操作
	static void create(Database *db, const TableDef *tableDef, const char *basePath) throw(NtseException);
	static BigLobStorage* open(Database *db, Session *session, const char *basePath) throw(NtseException);
	static void drop(const char *basePath) throw(NtseException);
	void close(Session *session, bool flushDirty);
	void flush(Session *session);
	void defrag(Session *session, const TableDef *tableDef);
	int getFiles(File** files, PageType* pageTypes, int numFile);
	void setTableId(Session *session, u16 tableId);
	
	// DML操作
	LobId insert(Session *session, const TableDef *tableDef, const byte *lob, u32 size, u32 orgSize);
	byte* read(Session *session, MemoryContext *mc, LobId lobId, uint *size, uint *orgSize);
	void del(Session *session, LobId lid);
#ifdef TNT_ENGINE
	void delAtCrash(Session *session, LobId lid);
#endif
	void update(Session *session, const TableDef *tableDef, LobId lobId, const byte *lob, uint size, uint orgSize);
	
	// 恢复相关
	static void redoCreate(Database *db, const TableDef *tableDef, const char *basePath, u16 tid) throw(NtseException);
	LobId redoInsert(Session *session, const TableDef * tableDef, u64 lsn, const byte *log, uint size);
	void redoDelete(Session *session, u64 lsn, const byte *log, uint size);
	void redoUpdate(Session *session, const TableDef *tableDef, LobId lobId, u64 lsn, const byte *log, uint logSize, const byte *lob, uint lobSize);
	void redoMove(Session *session, const TableDef *tableDef, u64 lsn, const byte *log, uint logSize);

	// 统计信息
	const BLobStatus& getStatus();
	const BLobStatusEx &getStatusEx();
	void updateExtendStatus(Session *session, uint maxSamplePages);

	/** 
	 * 返回大型大对象目录文件的统计信息
	 * @return 大型大对象目录文件的统计信息
	 */
	DBObjStats* getDirStats() {
		m_lobi->m_dboStats->m_statArr[DBOBJ_ITEM_READ] = m_dboStats->m_statArr[DBOBJ_ITEM_READ];
		m_lobi->m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] = m_dboStats->m_statArr[DBOBJ_ITEM_INSERT];
		m_lobi->m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] = m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE];
		m_lobi->m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] = m_dboStats->m_statArr[DBOBJ_ITEM_DELETE];
		return m_lobi->m_dboStats;
	}

	/** 
	 * 返回大型大对象数据文件的统计信息
	 * @return 大型大对象数据文件的统计信息
	 */
	DBObjStats* getDatStats() {
		return m_dboStats;
	}

	virtual SampleHandle* beginSample(Session *session, uint maxSampleNum, bool fastSample);
	virtual Sample* sampleNext(SampleHandle *handle);
	virtual void endSample(SampleHandle *handle);

private:
	static void createLobFile(Database *db, const TableDef *tableDef, const char *blobPath) throw(NtseException);
	uint getBlockSize(uint size);
	uint getLobSize(uint size);
	uint getPageNum( uint lobLen);
	uint getLobContent(Session *session, byte *data, LobBlockFirstPage *blockHeaderPage, u32 pid);
	void writeLob (Session *session, u32 pid, uint pageNum, uint offset, u64 lsn, const byte *lob, uint size); 
	void extendNewBlock(Session *session, u32 fileLen, u16 size);
	BufferPageHandle* getStartPageIDAndPage(const TableDef *tableDef, u32 pageNum, Session *session, u32 *pageId);  
	bool updateBlockAndCheck(Session *session, const TableDef *tableDef, u32 pid, LobId lid, const byte *lob, uint size, uint orgSize);
	bool readBlockAndCheck(Session *session, MemoryContext *mc, u32 pid, LobId lid, byte **dataSteam, uint *size, uint *org_size);
	bool delBlockAndCheck(Session *session, u32 pid, LobId lid, u16 tableid);
	u8 isNextFreeBlockEnough(Session *session, u32 pid, u32 pageNum, u32 needLen, u32 *newBlockLen);
	u64 logDelete(Session *session, LobId lid, u32 pid, bool headerPageModified, u32 oldListHeader, u16 tid);
	u64 logInsert(Session *session, LobId lid, u32 pid, bool headerPageModified, u32 newListHeader, u16 tid, u32 orgSize,u32 lobSize, const byte* lobData, MemoryContext *mc);
	u64 logUpdate(Session *session, LobId lid, u32 pid, bool isNewPageId, u32 newPageId, bool isNewFreeBlock, u32 freeBlockPageId, u32 freeBlockLen, u32 lobSize, u16 tid, u32 org_size);
	
	// 下面这些是碎片整理时候用
	BufferPageHandle* getFirstFreeBlockAndNextNotFreeBlock(Session *session, BufferPageHandle **firstNotFreeBlock, u32 *firstFreePid, u32 *firstNotFreePid, u32 *notFreeBlockLen, u32 fileTail);
	u32 getBlobFileTail(Session *session);
	void putLobToTail(Session *session, const TableDef *tableDef, u32 notFreePid, BufferPageHandle *notFreeBlockFirstPageHdl, MemoryContext *mc);
	void moveLob(Session *session, u32 freePid, u32 notFreePid, BufferPageHandle *freeBlockPageHdl, BufferPageHandle *notFreeBlockPageHdl, MemoryContext *mc);
	u64 logMove(Session *session, LobId lid, u32 oldPid, u32 newPid, u32 org_len, u32 lobSize, byte *data, u16 tid, MemoryContext *mc);
	
	void createNewFreeBlock(Session *session, BufferPageHandle *blockHeaderPageHdl, LobBlockFirstPage *lobFirstPage, u64 lsn, u32 freePageNum);
	void redoWriteOtherPages (Session *session, u64 lsn, const byte *lobContent, uint offset, u32 pageNum, u32 pageId, u32 dataLen);
	void extendLobFile(Session *session, const TableDef *tableDef, u32 pageId, u32 pageNum, u32 fileLen, LIFileHeaderPageInfo *headerPage);
	// verify用
	void verifyLobPage(Session *session, u32 pid);

	// 采样统计
	Sample *sample(u64 pageNum);
	u32 metaDataPgCnt() {return (u32)0;}
	u32 getRegionSize(u32 averageLen);
	BufferPageHandle* unsamplablePagesBetween(Session *session, u64 minPage, u64 regionSize, u32 *startPid);
	Sample* sampleRegionPage(BufferPageHandle *page, SampleHandle *handle, u32 pid);
	Sample* sampleBufferPage(BufferPageHdr *page, SampleHandle *handle);
	static const uint SAMPLE_FIELDS_NUM = 3;

	Database	*m_db;				/** 数据库 */
	Buffer		*m_buffer;			/** 页面缓存管理器 */
	File		*m_file;			/** 对应的文件 */
	LobIndex	*m_lobi;			/** 对应的目录文件 */
	BLobStatus	m_status;			/** 基本统计信息 */
	BLobStatusEx	m_statusEx;		/** 扩展统计信息 */
	u32			m_lobAverageLen;	/** 平均大对象长度，采样估算所得 */
	double		m_sampleRatio;		/** 采样的比例 */
	DBObjStats *m_dboStats;			/** 大型大对象数据状态信息 */

	const static uint OFFSET_BLOCK_FIRST = sizeof(LobBlockFirstPage);
	const static uint OFFSET_BLOCK_OTHER = sizeof(LobBlockOtherPage);

	friend class LobStorage;
#ifdef NTSE_UNIT_TEST
	friend class ::LobOperTestCase;
	friend class ::LobDefragTestCase;
	File* getIndexFile() {
		return m_lobi->m_file;
	}
#endif
};

struct BufScanHandle;

/** 大对象采用句柄类 */
class BigLobSampleHandle : public SampleHandle { 
public:
	BigLobSampleHandle(Session *session, uint maxSampleNum, bool fastSample) : SampleHandle(session, maxSampleNum, fastSample), m_bufScanHdl(NULL){
		m_minPage = 0;
		m_maxPage = 0;
		m_regionSize = 0;
		m_blockNum = 0;
		m_curBlock = 0;
		m_blockSize = 0;
		m_lastFreePages = 0;
	}
	/* buffer 采样句柄 */
	BufScanHandle *m_bufScanHdl;
	
	Session *m_session;
	/* 以下变量是为目录文件采样 */
	u64 m_minPage;		/** 最小的采样页范围 */
	u64 m_maxPage;		/** 最大的采样页范围 */
	u64 m_regionSize;	/** 采样的分区大小 */
	int m_blockNum, m_curBlock;
	int m_blockSize;
	u32 m_lastFreePages; /**上次采样的，采样得到的空闲页，这里主要为了查看是否在末尾*/

	friend class BigLobStorage;
};

}

#endif // _NTSE_BIGLOB_H_

