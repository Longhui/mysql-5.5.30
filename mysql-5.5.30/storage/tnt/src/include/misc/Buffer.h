/**
 * ҳ�滺�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_BUFFER_H_
#define _NTSE_BUFFER_H_

#include <iostream>
#include "misc/Global.h"
#include "util/PagePool.h"
#include "util/System.h"
#include "util/Hash.h"
#include "misc/Trace.h"
#include "misc/Verify.h"
#include "misc/ResourcePool.h"
#include <vector>
#include <set>

using namespace std;

#ifdef NTSE_UNIT_TEST
class BufferTestCase;
class BufferBigTest;
#endif

namespace ntse {

#pragma pack(1)
/** ����ҳ�ṹ */
struct BufferPageHdr {
	u64	m_lsn;		/** ��ҳ�����һ���޸Ĳ�����Ӧ����־LSN */
	u32	m_checksum;	/** ҳУ��ͣ����ڼ���ҳ�������Ƿ��ƻ���Ϊ0��ʾ��ʹ�ü���� */
	/** ��ʾ��ҳ�治ʹ��checksum��ϵͳ���κ�ʱ�򶼲���ȥ���»���֤��
	 * checksum��Ŀǰֻ���ڱ䳤�ѵ�����λͼҳ��
	 */
	static const u32 CHECKSUM_DISABLED = 0xFFFFFFFF;
	/** ��ʾҳ��û�м���У��ͣ���ϵͳ����Ҫ��ʱ�����������͡�
	 */
	static const u32 CHECKSUM_NO = 0;
};
#pragma pack()

class File;
/** һ������ҳ��Ψһ��ʶ */
struct PageKey {
	File		*m_file;		/** ҳ�����ļ� */
	PageType	m_pageType;		/** ҳ������ */
	u64			m_pageId;		/** ҳ�� */

	/** �ṩĬ�Ϲ��캯����Ϊ������һЩģ����������ʹ�� */
	PageKey() {
	}

	/** ����һ��PageKey����
	 *
	 * @param file �ļ�
	 * @param pageType ҳ������
	 * @param pageId ҳ��
	 */
	PageKey(File *file, PageType pageType, u64 pageId) {
		m_file = file;
		m_pageType = pageType;
		m_pageId = pageId;
	}
};

struct DBObjStats;

/** ������ƿ�Buffer control block��ÿ������ҳ�Ŀ�����Ϣ
 * ע��ÿ��BCB�в���Ҫ����ҳ������ҳ�������������ڴ�ҳ��ģ���ṩ
 */
struct Bcb {
	Mutex		m_metaLock;		/** ���ڱ�֤�Ա����ƿ���Ϣ�Ĳ������� */
	PageKey		m_pageKey;		/** Ψһ��ʶ */
	bool		m_dirty;		/** �Ƿ�����ҳ */
	bool		m_isWriting;	/** �Ƿ����ڱ�д��*/
	uint		m_pinCount;		/** ��pin�Ĵ��� */
	BufferPageHdr	*m_page;	/** ҳ������ */
	size_t		m_poolEntry;	/** �ڻ�����ƿ���е������ */
	DLink<Bcb *>	m_lruLink;	/** ��LRU�����е������ */
	DLink<Bcb *>	m_link;		/** ������ҳ�������е������ */
	DLink<Bcb *>	m_fileLink;	/** ���ļ�ҳ���б������� */
	u64			m_readSeq;		/** ҳ�汻����Ĵ���Ԥ����ʱ�����ã�ֻ��Ӧ����������ʱ�����á�
								 * ҳ����������Ϣ��Ԥ�����ʱʹ��
								 */
	u64			m_lruSeq;		/** �ϴε���LRU����λ��ʱ�Ĵ��� */

	DBObjStats *m_dbObjStats;	/** ���ݶ���ͳ�ƽṹ */
#ifdef NTSE_VERIFY_EX
	bool		m_checksumValid;/** ������Ƿ�������һ�� */
	bool		m_dirtyMarked;	/** �ڼ�/����֮���Ƿ������markDirty */
#endif

	/** �޲ι��캯��������������ڳ�ʼ���·���Ķ��� */
	Bcb(): m_metaLock("Bcb::metaLock", __FILE__, __LINE__){
			// ���������Ĺ��캯��������������ڳ�ʼ���·���Ķ���
			init(NULL, PAGE_EMPTY, INVALID_PAGE_ID);
	}
	/**
	 * ����һ��������ƿ�
	 *
	 * @param file ��Ӧҳ���������ļ�
	 * @param pageType ��Ӧҳ�������
	 * @param pageId ��Ӧҳ���ҳ��
	 */
	void init(File *file, PageType pageType, u64 pageId) {
		m_pageKey.m_file = file;
		m_pageKey.m_pageType = pageType;
		m_pageKey.m_pageId = pageId;
		m_dirty = false;
		m_isWriting = false;
		m_pinCount = 0;
		m_link.set(this);
		m_lruLink.set(this);
		m_fileLink.set(this);
		m_readSeq = 0;
#ifdef NTSE_VERIFY_EX
		m_checksumValid = false;
		m_dirtyMarked = false;
#endif
		m_dbObjStats = NULL;
	}
};

/** ���ݻ�����ƿ�����ϣֵ�ĺ������� */
class BcbHasher {
public:
	/**
	 * �����ϣֵ��BCB�Ĺ�ϣֵ�ɶ�Ӧҳ�����ļ���ҳ��ȷ��
	 *
	 * @param bcb ������ƿ�
	 * @return ��ϣֵ
	 */
	inline unsigned int operator()(const Bcb *bcb) const {
		return (unsigned int)((long)(u64)bcb->m_pageKey.m_file ^ (long)(bcb->m_pageKey.m_pageId));
	}
};

/** ���ݻ���ҳ��ʶ�����ϣֵ�ĺ������� */
class PageKeyHasher {
public:
	/**
	 * �����ϣֵ
	 *
	 * @param pageKey ����ҳ��ʶ
	 * @return ��ϣֵ
	 */
	inline unsigned int operator()(const PageKey *pageKey) const {
		return (unsigned int)((long)(u64)pageKey->m_file ^ (long)(pageKey->m_pageId));
	}
};

/** �Ƚϻ�����ƿ��Ӧ��ҳ���Ƿ���ָ��ҳ���ʶΪͬһҳ��ĺ������� */
class BcbKeyEqualer {
public:
	/**
	 * �жϻ���ҳ��ʶ�ͻ�����ƿ��Ƿ��Ӧͬһ����ҳ
	 *
	 * @param pageKey ����ҳ��ʶ
	 * @param bcb ������ƿ�
	 * @return ����ҳ��ʶ�ͻ�����ƿ��Ƿ��Ӧͬһ����ҳ
	 */
	inline bool operator()(const PageKey *pageKey, const Bcb *bcb) const {
		return bcb->m_pageKey.m_file == pageKey->m_file && bcb->m_pageKey.m_pageId == pageKey->m_pageId;
	}
};

#pragma pack(4)
/** ����ҳ���ʶ��ҳ�����ݵĶ�Ԫ�� */
struct KeyPagePair {
	PageKey			m_pageKey;	/** ҳ���ʶ */
	BufferPageHdr	*m_page;	/** ҳ������ */

	/**
	 * ���캯��
	 *
	 * @param pageKey ҳ���ʶ
	 * @param page ҳ������
	 */
	KeyPagePair(const PageKey &pageKey, BufferPageHdr *page):
		m_pageKey(pageKey.m_file, pageKey.m_pageType, pageKey.m_pageId) {
		m_page = page;
	}

	/**
	 * �Ƚϴ�С
	 *
	 * @param another ��һ��KeyPagePair����
	 * @return ���Ƿ�С��another
	 */
	bool operator < (const KeyPagePair &another) const {
		if (m_pageKey.m_file == another.m_pageKey.m_file)
			return m_pageKey.m_pageId < another.m_pageKey.m_pageId;
		else
			return m_pageKey.m_file < another.m_pageKey.m_file;
	}
};
#pragma pack()

/** ҳ�滺����չʹ��״̬����չʹ��״̬����ʵʱͳ�ƣ����ǰ�����㣬
 * ����ʱ����һ���Ŀ�����
 */
struct BufferStatusEx {
	u64		m_dirtyPages;			/** ��ҳ�� */
	u64		m_pinnedPages;			/** pinס��ҳ����� */
	u64		m_rlockedPages;			/** �ù�������ס��ҳ����� */
	u64		m_wlockedPages;			/** �û�������ס��ҳ����� */
	double	m_avgHashConflict;		/** ҳ���ϣ��ƽ����ͻ������ */
	u64		m_maxHashConflict;		/** ҳ���ϣ������ͻ������ */
};

/** ҳ�滺��ʹ��״̬ */
struct BufferStatus {
	u64 		m_logicalReads;		/** �߼����������� */
	u64 		m_physicalReads;	/** �������������������Ԥ���� */
	u64 		m_logicalWrites;	/** �߼�д�������� */
	u64 		m_physicalWrites;	/** ����д��������������scavengerWrites/flushWrites */
	u64 		m_scavengerWrites;	/** ��̨ˢ��ҳ�߳�д��ҳ���� */
	u64 		m_flushWrites;		/** ˢд������ҳ�����㣩ʱд������ҳ�� */
	u64			m_extendWrites; /** ����չʱд������ҳ��Ŀ */
	u64 		m_prefetches;		/** Ԥ������ */
	u64			m_batchPrefetch;	/** ����Ԥ������ */
	u64			m_nonbatchPrefetch;	/** ������Ԥ������ */
	u64			m_prefetchPages;	/** Ԥ�������ҳ���� */
	u64			m_pageCreates;		/** �½�ҳ��������������ҳ��򴴽�ҳ�� */
	u64			m_allocBlockFail;	/** ����ҳ��ʧ�ܣ���ֵ���߿��ܱ�ʾ��������������� */
	u64			m_replaceSearches;	/** ҳ���滻����LRU������� */
	u64			m_replaceSearchLen;	/** ҳ���滻����LRU�����ܳ��� */
	u64			m_unsafeLockFails;	/** ����ȫ�ļ�������lockPageIfTypeʧ�ܴ��� */
	u64         m_firstTouch;       /** ��ҳ��ռ��ؽ�buffer�����lru����ı� */
	u64         m_laterTouch;       /** ��ҳ���ڹ�m_correlatedRefPeriod�κ������lru����ı� */
	u64			m_flushAllBufferLockCount; /** ȫ��flush����buffer�ռ���ҳ�ļ��������������realclose�� */
	u64			m_flushAllBufferLockTime;  /** ȫ��flush����buffer�ռ���ҳ����ʱ�� */
	Atomic<long>	m_pendingReads;	/** ���ڽ����еĶ��������� */
	Atomic<long>	m_pendingWrites;/** ���ڽ����е�д�������� */
	Atomic<long>	m_pendingAllocs;/** ���ڽ����еķ���ҳ����߳��� */
	u64			m_readTime;			/** ����������ʱ�䣬��λ���� */
	u64			m_writeTime;		/** д��������ʱ�䣬��λ���� */
	BufferStatusEx	m_statusEx;		/** ��չͳ����Ϣ */
	const RWLockUsage	*m_globalLockUsage;	/** ȫ����ʹ��ͳ����Ϣ */
	u64			m_realScavengeCnt;	 /** ���������ˢҳ��Ĵ��� */
	Atomic<long>	m_curDirtyPages;/** ϵͳ����ҳ���� */
};



struct BufferFlushStatus {
	uint		m_iter;					/** ϵͳ����ˢ��ҳ���� */
	u64			m_prevLoopNtseLogSize;	/** ��һ��ѭ��ʱntselog��С */
	u64			m_avgNtseLogRate;		/** ƽ���Ĳ���NTSE��־������ */
	int			m_avgDirtyPagesRate;	/** ƽ���Ĳ�����ҳ������ */
	uint		m_loopWritePageCnt;		/** ��ǰѭ����ˢ��ҳ����Ŀ */
	uint		m_avgPageRate;			/** ��ѭ���е�ƽ��ˢ��ҳ���� */
	uint		m_prevLoopDirtyPages;	/** ǰһ��ѭ��ʱϵͳ��ҳ�� */
	uint		m_prevSecDirtyPages;	/** ǰһ���ϵͳ��ҳ��*/
	uint		m_prevSecFlushPageCnt;	/** ǰһ��ˢ��ҳ���� */
};


///////////////////////////////////////////////

typedef class ResourceUser IoBufferUser;

/** ����һ��VirtualAlloc�������ڴ�ռ� */
class IoBuffer : public Resource {
public:
	/** ���쳤��Ϊָ���ֽ�����IoBuffer */
	IoBuffer(uint pageCnt, uint pageSize);
	virtual ~IoBuffer();
	/** ��ȡ�����ַ������ */
	byte* getBuffer();
	/** ���泤�� */
	size_t getSize() const;
	/** ҳ�� */
	uint getPageCnt() const;

private:
	uint   m_pageSize; /** ����ҳ���С */
	byte  *m_data;     /** �ڴ��� */
	size_t m_size;     /** �����С */
};

class BatchIoBufPool : public Pool {
public:
	static const uint DFL_INSTANCE_NUM = 4;
#ifdef NTSE_UNIT_TEST
	static const uint DFL_PAGE_CNT = 8;
#else
	static const uint DFL_PAGE_CNT = 128;
#endif
	static const uint DFL_PAGE_SIZE = Limits::PAGE_SIZE;

public:
	BatchIoBufPool(uint instNum = DFL_INSTANCE_NUM, uint pageCntPerInst = DFL_PAGE_CNT, 
		uint pageSize = DFL_PAGE_SIZE);
	~BatchIoBufPool() {}
};

///////////////////////////////////////////////

/** �����ļ���Ӧ��ҳ������ */
class FilePageList {
public:
	FilePageList() :m_file(NULL), m_pendingFlushCount(0) {}

	inline void add(Bcb *bcb) {
		assert(bcb->m_pageKey.m_file == m_file);
		m_bcbList.addLast(&bcb->m_fileLink);
	}

	inline File* getFile() const {
		return m_file;
	}

	inline void setFile(File *file) {
		m_file = file;
	}

	inline DList<Bcb *>* getBcbList() {
		return &m_bcbList;
	}

	inline int getPendingFlushCount() {
		return m_pendingFlushCount;
	}

	void incPendingFlushCount() {
		++m_pendingFlushCount;
	}
	
	void decPendingFlushCount() {
		--m_pendingFlushCount;
	}
private:
	DList<Bcb *> m_bcbList;		/** ���ƿ����� */
	File *m_file;				/** �ļ�ָ��*/
	int m_pendingFlushCount;
};


/** File�Լ�FilePageList�Ĺ�ϣ���� */
class FileHasher {
public:
	/**
	 * ����File��ϣֵ
	 *
	 * @param file
	 * @return ��ϣֵ
	 */
	inline unsigned int operator()(const File *file) const {
		return (unsigned int)reinterpret_cast<long>(file);
	}

	/**
	 * ����File��ϣֵ
	 *
	 * @param file
	 * @return ��ϣֵ
	 */
	inline unsigned int operator()(const FilePageList *pageList) const {
		return (unsigned int)reinterpret_cast<long>(pageList->getFile());
	}
};


class FilePageListEqualer {
public:
	/**
	 * �ж�ҳ�������Ƿ������ļ�
	 *
	 * @param file �ļ�ָ��
	 * @param pageList ҳ������
	 * @return ҳ�������Ƿ������ļ�
	 */
	inline bool operator()(const File *file, const FilePageList *pageList) const {
		return pageList->getFile() == file;
	}
};

struct BufScanHandle;
class Scavenger;
class Prefetcher;
class File;
class AioArray;
class Syslog;
class Session;
class Txnlog;
class Database;
struct DBObjStats;
/** ҳ�滺�� */
class Buffer: public PagePoolUser {
public:
	Buffer(Database *db, uint numPages, PagePool *pagePool, Syslog *syslog, Txnlog *txnlog);
	~Buffer();
	virtual uint freeSomePages(u16 userId, uint numPages);
	BufferPageHdr* getPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, DBObjStats* dbObjStats, BufferPageHdr *guess = NULL);
	BufferPageHdr* newPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, DBObjStats* dbObjStats);
	BufferPageHdr* tryGetPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, DBObjStats* dbObjStats);
	void releasePage(Session *session, BufferPageHdr *page, LockMode lockMode);

	/**
	 * ����һ��ҳ
	 *
	 * @param userId �û�ID
	 * @param page ҳ���ַ
	 * @param lockMode ��ģʽ
	 * @param touch �Ƿ��¼����ʱ�䲢���ǵ���LRU
	 */
	inline void lockPage(u16 userId, BufferPageHdr *page, LockMode lockMode, bool touch) {
		ftrace(ts.buf, tout << userId << (Bcb *)m_pool->getInfo(page) << lockMode << touch);
		m_pool->lockPage(userId, page, lockMode, __FILE__, __LINE__);
#ifdef NTSE_VERIFY_EX
		syncChecksum((Bcb *)m_pool->getInfo(page), lockMode);
#endif
		if (touch)
			touchBlock(userId, (Bcb *)m_pool->getInfo(page));
	}

	/**
	 * �ͷ�һ��ҳ�ϵ����������ͷ�pin
	 *
	 * @param userId �û�ID
	 * @param page ҳ���ַ
	 * @param lockMode ��ģʽ
	 */
	inline void unlockPage(u16 userId, BufferPageHdr *page, LockMode lockMode) {
		ftrace(ts.buf, tout << userId << (Bcb *)m_pool->getInfo(page) << lockMode);
#ifdef NTSE_VERIFY_EX
		verifyChecksum(page, lockMode);
#endif
		m_pool->unlockPage(userId, page, lockMode);
	}

	void upgradePageLock(Session *session, BufferPageHdr *page);
	void unpinPage(BufferPageHdr *page);
	void markDirty(Session *session, BufferPageHdr *page);
	bool isDirty(BufferPageHdr *page);
	void writePage(Session *session, BufferPageHdr *page);
	void batchWrite(Session *session, File *file, PageType pageType, u64 minPid, u64 maxPid);
	void freePages(Session *session, File *file, bool writeDirty);
	void freePages(Session *session, File *file, uint indexId, bool (*fn)(BufferPageHdr *page, PageId pageId, uint indexId));
	void flushAll(Session *session = NULL) throw(NtseException);
#ifndef WIN32
	bool writePageUseAio(Session *session, BufferPageHdr *page, AioArray *array);
	void waitWritePageUseAioComplete(Session *session, AioArray* array, bool waitAll, bool freePage);
	void flushAllUseAio(Session *session, AioArray *array) throw(NtseException);
	void flushDirtyPagesUseAio(Session *session, File *file, u64 searchLength = (u64)-1,
		u64 maxPages = (u64)-1, bool ignoreCancel = true, AioArray *array = NULL) throw(NtseException);
#endif
	void flushDirtyPages(Session *session, File *file, u64 searchLength = (u64)-1,
		u64 maxPages = (u64)-1, bool ignoreCancel = true) throw(NtseException);
	void prefetchPages(Session *session, u16 userId, std::vector<Bcb *> *pagesToRead);
	void canclePrefetch(u16 userId, std::vector<Bcb *> *pagesToRead);
	u32 checksumPage(BufferPageHdr *page);
	bool hasPage(File *file, PageType pageType, u64 pageId);
	static BatchIoBufPool* getBatchIoBufferPool();

	void disableScavenger();
	void enableScavenger();

	/////////////////////////////////////////////////////////////////////////
	// ��Щ�����ò���                                             //
	/////////////////////////////////////////////////////////////////////////
	bool isChecksumed();
	void setChecksumed(bool checksumed);
	uint getPrefetchSize();
	void setPrefetchSize(uint prefetchSize);
	double getPrefetchRatio();
	void setPrefetchRatio(double prefetchRatio);
	uint getScavengerInterval();
	void setScavengerInterval(uint interval);
	double getScavengerMaxPagesRatio();
	void setScavengerMaxPagesRatio(double maxPagesRatio);
	double getScavengerMultiplier();
	void setScavengerMultiplier(double multiplier);
	uint getMaxScavengerPages();
	void setMaxScavengerPages(uint maxScavengerPages);
	/////////////////////////////////////////////////////////////////////////
	// ��Щ������״̬                                             //
	/////////////////////////////////////////////////////////////////////////
	void updateExtendStatus();
	uint getRecommandFlushSize();
	void resetFlushStatus();
	const BufferStatus& getStatus();
	void printStatus(ostream &out) const;
	BufScanHandle* beginScan(u16 userId, File *file);
	const Bcb* getNext(BufScanHandle *h);
	void releaseCurrent(BufScanHandle *h);
	void endScan(BufScanHandle *h);

	void signalScavenger();


private:
	BufferPageHdr* doGetPage(Session *session, File *file, PageType pageType, u64 pageId, LockMode lockMode, bool readFromFile, DBObjStats *dbObjStats);
	Bcb* tryAllocBlock(u16 userId, File *file, PageType pageType, u64 pageId);
	void postAlloc(Session *session, Bcb *bcb, bool readFromFile, LockMode lockMode);
	Bcb* replaceInList(u16 userId, DList<Bcb *> *list);
	void freeBlock(u16 userId, Bcb *bcb);
	void readBlock(Session *session, Bcb *bcb, bool prefetch);
	void pinBlock(u16 userId, Bcb *bcb, bool touch);
	void unpinBlock(Bcb *bcb);
	void touchBlock(u16 userId, Bcb *bcb);
	void checkPrefetch(u16 userId, Bcb *bcb);
	void getDirtyPagesFromList(std::vector<KeyPagePair> *dirtyPages, DList<Bcb *> *list, File *file, u64 searchLength, u64 maxPages, bool skipLocked);
	void getFreePagesFromList(std::vector<KeyPagePair> *freePages, DList<Bcb *> *list, u64 maxPages);
	void getDirtyPagesOfFile(vector<KeyPagePair> *dirtyPages, File *file, u64 searchLength, u64 maxPages, bool skipLocked);
	void syncChecksum(Bcb *bcb, LockMode lockMode);
	void verifyChecksum(BufferPageHdr *page, LockMode lockMode);
	void updateChecksum(BufferPageHdr *page);
	void batchWritePages(Session *session, File *file, u64 startPid, std::vector<Bcb *>* bcbs);
	void replaceDBObjStats(Bcb *bcb, DBObjStats *dbObjStats);
	void doFlushDirtyPages(Session *session, File *file, vector<KeyPagePair> *dirtyPagesVec,
		bool ignoreCancel, bool flushAllPages) throw (NtseException);

	void syncFiles(set<File *> fileSet);
public:
	/** �Ϸִ�ռ�����С�ı��� */
	static const double OLD_GEN_RATIO;
	/** �����Ϸִ���ʵ�ʴ�С��Ŀ���С֮����������� */
	static const uint OLD_GEN_SHIFT = 20;
	/** ʱ����С����һ��ֵ�ķ��ʱ���Ϊ������Է��ʣ���λΪ�� */
	static const uint CORRELATED_REF_PERIOD = 10;
	/** ��ҳˢд�߳�Ĭ������ʱ��������λΪ���� */
	static const uint SCAVENGER_DEFAULT_INTERVAL = 1000;
	/** ��ҳˢд�߳�Ĭ��д����ҳ�������������С����һ���� */
	static const double SCAVENGER_DEFAULT_MAX_PAGES_RATIO;
	/** ��ҳˢд�߳�һ��д����ҳ������Сֵ */
	static const uint SCAVENGER_MIN_PAGES = 10;
	/** ��ҳˢд�߳�Ĭ��ɨ���LRU���������ϴ����������������½�ҳ�ı��� */
	static const double SCAVENGER_DEFAULT_SEARCH_LENGTH_MULTIPLIER;
	/** ��ҳˢд�߳�һ��ɨ���LRU��������Сֵ */
	static const uint SCAVENGER_MIN_SEARCH_LEGNTH = 100;
#ifdef TNT_ENGINE
	/** ��ҳˢд�̳߳���ά����LRU����β��CLEAN PAGESռ����LRU����ĳ���(�ٷֱ�) */
	static const double SCAVENGER_DEFAULT_CLEAN_PAGES_LEN;
	static const uint SCAVENGER_SIGNALED_AFTER_LRU_SEARCH_LEN = 2000;
#endif
	/** Ԥ����Ĭ�ϴ�С */
	static const uint DEFAULT_PREFETCH_SIZE = 4 * 1024 * 1024;
	/** ��Ԥ��������ô���ҳ�ڻ����е�����Ԥ�� */
	static const double DEFAULT_PREFETCH_RATIO;
	/** ��Ԥ��������ô���ҳ����˳����Ԥ��˳��һ��ʱ����Ԥ�� */
	static const double PREFETCH_RIGHT_ORDER_RATIO;
	/** Ԥ��������д�С */
	static const uint PREFETCH_QUEUE_SIZE = 4;
	/** �����п��ܵ���������ҳ������ʱ�ĳ�ʱʱ�䣬��λ���� */
	static const uint UNSAFE_LOCK_TIMEOUT = 10;
	/** ��ȡҳ���ͻʱ��˯��ʱ�䣬��λ���� */
	static const uint GET_PAGE_SLEEP = 10;
	/** Ԥ��ʱ����ǰ�� */
	static const uint PREFETCH_AHEAD = 128;
	/** ����IO������������ */
	static const uint BATCH_IO_SIZE = 1024 * 1024;

private:
	/** ����ҳ���ʶ��BCB�Ĺ�ϣ�� */
	DynHash<PageKey *, Bcb *, BcbHasher, PageKeyHasher, BcbKeyEqualer>	m_pageHash;
	/** �����ļ����ļ���Ӧҳ��Ĺ�ϣ�� */
	DynHash<File *, FilePageList *, FileHasher, FileHasher, FilePageListEqualer> m_filePageHash;
	Database		*m_db;			/** �������ݿ� */
	Syslog			*m_syslog;		/** ϵͳ��־ */
	Txnlog			*m_txnlog;		/** ������־ */
	RWLock			m_lock;			/** ���ڱ�����ϣ��LRU�����ȫ�ֽṹ���� */
	ObjectPool<Bcb>	m_bcbPool;		/** ������ƿ�� */
	ObjectPool<FilePageList> m_filePageListPool;	/** �ļ�ҳ���������� */
	u64				m_correlatedRefPeriod;	/** �������С����һ��ֵ�ķ��ʱ���Ϊ������Է��ʣ�������LRU���� */
	DList<Bcb *>	m_oldGen;		/** �Ϸִ�LRU����������ʵ�ҳ�濿������β */
	DList<Bcb *>	m_newGen;		/** �·ִ�LRU����������ʵ�ҳ�濿������β */
	double	m_oldGenRatio;			/** �Ϸִ����� */
	DList<Bcb *>	m_allPages;		/** ����ҳ������ */
	Prefetcher		*m_prefetcher;	/** ����Ԥ�����߳� */
	Scavenger		*m_scavenger;	/** ��̨д��ҳ�߳� */
	u64				m_readSeq;		/** ҳ���ȡ���� */
	BufferStatus	m_status;		/** ����״̬ */
	BufferFlushStatus m_flushStatus;/** ˢ��ҳ���״̬ͳ�� */
	bool			m_checksumed;	/** �Ƿ�������� */
	uint			m_prefetchSize;	/** Ԥ������С */
	uint			m_prefetchPages;/** Ԥ����ҳ�� */
	double			m_prefetchRatio;/** ��������Ԥ�������� */
	double			m_skipPrefetchRatio;/** ������һ������ҳ���Ѿ��ڻ�����ʱ��Ԥ��ʱ��Ծ�Ե�Ԥ���������ڻ����е�ҳ */
	static BatchIoBufPool  m_batchIOBufPool;/** ����IO����� */

#ifdef NTSE_UNIT_TEST
public:
	friend class ::BufferTestCase;
	friend class ::BufferBigTest;
#endif
};

}

#endif
