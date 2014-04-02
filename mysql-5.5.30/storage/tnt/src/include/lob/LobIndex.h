/**
 * ���ʹ�����Ŀ¼�ļ�����
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

#define LOB_INDEX_FILE_EXTEND 16    /** Ŀ¼�ļ�����ʱ�����ӵ�ҳ�� */
#define LOB_INDEX_FREE_BITS	1		/** �Ƿ���еı�־λռ��λ�� */
#define LOB_INDEX_PAGE_BITS	31	    /** ҳ��ռ��λ�� */

/** ����Ŀ¼��õ���λ�ı�־λ */
#define NID_GET_FREE(lid)	((u8)((lid) >>PAGE_BITS))
/** ����Ŀ¼��õ�PageID��PageIDΪu32���ͣ���ʵ����ֻ�ܱ�ʾ2^31��Ŀ¼ҳ */
#define NID_GET_PAGE(lid)	((u32)((lid) & (u32)0x7FFFFFFF))
/** ����ҳ�źͱ�־λ�õ�Ŀ¼������ */
#define NID(free, page)		((u32)(((free) << PAGE_BITS) | (u32)page)))

/** Ŀ¼�ļ���ҳ�ṹ */
struct LIFileHeaderPageInfo {
	BufferPageHdr m_bph;				/** ҳ����ṹ */
	u32		m_fileLen;					/** Ŀ¼�ļ����� */
	u32		m_firstFreePageNum;			/** ���е�һ����������ҳ��0��ʾû�� */
	u32		m_blobFileLen;              /** ������ļ����� */
	u32		m_blobFileTail;             /** ������ļ��Ѿ�����ĳ��� */ 
	u32		m_blobFileFreeLen;          /** �������п���ܳ��� */
	u32		m_blobFileFreeBlock;        /** ������ļ����п����Ŀ */ 
	u16		m_tableId;                  /** �������ID,Ϊд��־�� */  
};

#pragma pack(1) // 1�ֽڶ���
/** Ŀ¼�ļ���ҳͷ�ṹ */
struct LIFilePageInfo {
	BufferPageHdr	m_bph;	    /** ����ҳ����ͷ�ṹ */
	s16		m_firstFreeSlot;    /** ��һ�����м�¼�� */
	bool	m_inFreePageList: 1;/** �Ƿ��ڿ���ҳ������ */ 
	u16     m_freeSlotNum;      /** ҳ�п��еĲ��� */ 
	u32		m_nextFreePageNum;  /** �¸�����ҳ�� */	
};

/** ��¼��ͷ���ṹ */
struct LiFileSlotInfo {
	bool	m_free: 1;			/** �Ƿ�Ϊ���м�¼�� */
	union {
		s16		m_nextFreeSlot;	/** ��һ���м�¼�ۣ�ֻ�ڱ���Ϊ���м�¼��ʱ���� */
		u32 	m_pageId;		/** ���ݱ�ʾ�������ʼҳ�ţ�ֻ�ڲ�Ϊ���м�¼��ʱ���� */
	} u;
};
#pragma pack()

/** ���ʹ�������������� */
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

	//�õ��ļ����
	inline File* getFile () {
		return m_file;
	}
	
	/** �����ӿ� */
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
	
	/** ������״̬�������� */
	static const uint SAMPLE_FIELDS_NUM = 1;
	void selectPage(u64 *outPages, uint wantNum, u64 min, u64 regionSize);
	uint unsamplablePagesBetween(u64 minPage, u64 regionSize);
	Sample* sample(u64 pageId, Session *session);
	Sample* sampleBufferPage(Session *session, BufferPageHdr *page);

	/** ��¼�۴�С:����ÿ������5���ֽ� */
	static const uint INDEX_SLOT_LENGTH = sizeof(LiFileSlotInfo);	
	/** Ŀ¼�ļ�����ҳ��Ϣ��ʼƫ�� */
	static const uint OFFSET_HEADER_INFO = sizeof(LIFileHeaderPageInfo);
	/** ��¼����������ҳ����ʼƫ�� */
	static const uint OFFSET_PAGE_RECORD = sizeof(LIFilePageInfo);
	/** һ��Ŀ¼ҳ�����԰������ٸ�Ŀ¼�� */
	static const uint MAX_SLOT_PER_PAGE = (Limits::PAGE_SIZE - OFFSET_PAGE_RECORD) / INDEX_SLOT_LENGTH;

	Database *m_db;			             /** ���ݿ� */
	Buffer 	*m_buffer;		             /** ҳ�滺������� */
	File  *m_file;                       /** ��Ӧ���ļ� */  
	LIFileHeaderPageInfo *m_headerPage;  /** Ŀ¼�ļ���ҳ��ҪPIN���ڴ��� */
	u16 m_tableId;                       /** ��Ӧ��ĸ��ID */ 
	u32 m_maxUsedPage;                   /** Ŀǰ����ʹ�õ�ҳ */
	DBObjStats *m_dboStats;			 	 /** ���ʹ����Ŀ¼״̬ */

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
	
	/* buffer ������� */
	/* ���±�����ΪĿ¼�ļ����� */
	u64 m_minPage;		/** ��С�Ĳ���ҳ��Χ */
	u64 m_maxPage;		/** ���Ĳ���ҳ��Χ */
	u64 m_regionSize;   /** һ���������ķ�Χ */
	uint m_blockNum, m_curBlock;
	u64 *m_blockPages;
	uint m_blockSize, m_curIdxInBlock;
	BufScanHandle *m_bufScanHdl;
	
};

}

#endif

