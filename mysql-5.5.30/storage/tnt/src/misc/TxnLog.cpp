/**
 * 内存分配上下文实现
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */
#include <iostream>
#include "misc/Txnlog.h"
#include "util/Sync.h"
#include "util/File.h"
#include "util/Hash.h"
#include "util/DList.h"
#include "util/Array.h"
#include "util/Thread.h"
#include "util/SmartPtr.h"
#include "util/PagePool.h"
#include "misc/Syslog.h"
#include "api/Database.h"
#include "misc/ControlFile.h"
#include "misc/Trace.h"
#include "misc/Buffer.h"
#include <stddef.h>
#include <algorithm>
#include <sstream>
#include <vector>
#include <set>

#ifdef TNT_ENGINE
#include "api/TNTDatabase.h"
#endif


// 魔数开关：日志记录头中放入魔数等字段，验证日志记录正确性
// #define TXNLOG_DEBUG

#undef min
#undef max
using namespace std;

namespace ntse {

/*!
 @brief 日志类型
*/
enum LogRecordType
{
	LR_NORMAL=0,		/** 普通日志记录						*/
	LR_HEAD,			/** 或者是某条长日志记录的前面部分		*/
	LR_CONTINUE,		/** 日志记录是某条日志记录的后续部分	*/
	LR_TAIL,			/** 日志记录是某条日志记录的后续结束部分*/
	LR_MAX,				/** 我是最大的，不能超过我				*/
};

/** 日志页头 */
struct LogPageHdr {
	u64		m_checksum;	/** 校验和								*/
	LsnType m_startLsn; /** 日志页起始 LSN						*/
	u16		m_used;		/** 页面已使用字节数					*/
	u8		m_pad[6];	/** 填充字节, 填充结构大小为8字节整数倍 */
};


/** 日志记录头 */
struct LogRecordHdr {
#ifdef TXNLOG_DEBUG
	u64		m_checksum;		/** 日志记录校验									*/
	LsnType m_lsn;			/** 日志LSN，用于校验                               */
	u32		m_magic;		/** 魔术，用于验证记录正确性						*/
#endif
	u16		m_txnId;		/* txnId 事务ID，为0表示不在事务中					*/
	u16		m_tableId;		/** 表ID											*/
	u16		m_size;			/** 日志记录长度，不包括日志头；记录长度不能超出一页*/
	u8		m_logType;		/** 日志类型, LogType枚举类型, 最多支持256种类型	*/
	u8		m_type;			/** 日志记录类型, LogRecordType枚举类型				*/

	static const u32 MAGIC = 0xcd12e30;
};

/** 日志文件控制页面(第一页） */
struct LogFileCtrlPage {
	u64		m_checksum;	/** 校验和							*/
	LsnType	m_startLsn;	/** 该文件的起始LSN					*/
	LsnType	m_endLsn;	/** 该文件的结束LSN					*/
	u64		m_size;		/** 日志文件大小					*/
	LsnType	m_tailHint;	/** 从这里开始查找最后一条日志记录	*/
	u8		m_full;		/** 日志文件是否已满				*/
	u8		m_pad[7];	/** 填充字节						*/
};
/**
 * 备份文件控制页
 */
class LogBackupCtrlPage {
public:
	u64		m_checksum;	/** 校验和							*/
	u64		m_size;		/** 日志文件大小					*/
	u64		m_startLsn;	/** 备份起点						*/
};
/** 取整函数,八字节对齐 */
template<typename T> inline T alignRound(T v) {
	return (v + 7) & ~((T)7);
}

const size_t LOG_PAGE_SIZE = LogConfig::LOG_PAGE_SIZE;						/** 日志页大小	*/
const size_t LOG_PAGE_HDR_SIZE = alignRound(offsetof(LogPageHdr, m_pad));	/** 页头大小	*/
const size_t LOG_RECORD_HDR_SIZE = (sizeof(LogRecordHdr));					/** 记录头大小	*/
const size_t LOG_CTRL_PAGE_SIZE = LOG_PAGE_SIZE;	/** 日志文件控制页面(第一个页面）大小 */
/* 单个日志页的有效负载长度 */
const size_t PAGE_MAX_PAYLOAD_SIZE = LOG_PAGE_SIZE - LOG_PAGE_HDR_SIZE - LOG_RECORD_HDR_SIZE;

static size_t lsnToOffInPage(LsnType lsn);
static LsnType lsnCeiling(LsnType lsn);
static LsnType lsnFloor(LsnType lsn);
static void setLogEntryHdr(LogEntry *e, const LogRecordHdr *rec);
static size_t computeMinlogBufferSize(size_t logSize);
static size_t computeMinlogFileSize(size_t logSize);
#ifndef NDEBUG
static bool isLogRecMetaEq(const LogEntry *e, const LogRecordHdr *rec);
static bool isLogRecValid(const LogRecordHdr *rec, const LogPageHdr *page = 0);
#endif
#ifdef TXNLOG_DEBUG
static void verifyPage(const LogPageHdr *page);
#endif
Tracer& operator << (Tracer &tracer, const LogRecordHdr *recHdr);

/** 日志文件 */
class LogFile {
public:
	static LogFile* createInMem(Syslog *sysLog, LsnType startLsn, const char *filename, u64 size, bool directIo);
	static LogFile* open(Syslog *sysLog, const char *filename, bool directIo = true) throw (NtseException);
	static void createLogFile(const char *filename, u64 size) throw (NtseException);
	void close();

	void writePages(LsnType lsn, void *page, size_t pageCnt);
	u64 doWritePages(LsnType lsn, void *page, size_t pageCnt);
	u64 doWriteChecksumedPages(LsnType lsn, const void *page, size_t pageCnt);
	void readPages(LsnType lsn, void *page, size_t pageCnt);
	void reset(LsnType nextLsn);
	void setTailHint(LsnType lsn);
	LsnType getTailHint() const;
	LsnType getStartLsn() const;
	LsnType getEndLsn(bool lock = true);
	void setEndLsn(LsnType endLsn);
	void setStartLsn(LsnType startLsn);
	u64 getSize() const;
	u64 payloadSize() const;
	const char* getPath() const;
	bool full();
	void full(bool isFull);
	void reuse(LsnType startLsn);
	void writeCtrlPage();
	static void checksumPages(LogPageHdr *page, size_t pageCnt);
	void initPages(LsnType lsn, size_t pageCnt);
	void initLogfile();
	void truncate(LsnType lsn, LsnType notZeroPageLsn = INVALID_LSN);
#ifdef TNT_ENGINE
	inline void sync() {
		m_file->sync();
	}
#endif
private:
	LogFile(Syslog* sysLog, const char *filename, bool directIo);
	~LogFile();

	void readCtrlPage();
	static void checksumPage(LogPageHdr *page);
	static void checksumCtrlPage(LogFileCtrlPage *page);
	void verify() throw (NtseException);
	static u64 initLogfile(File *file, u64 size);
private:
	RWLock	m_lock;
	File	*m_file;	/** 日志文件 */
	LogFileCtrlPage *m_ctrlPage; /** 控制页面，日志文件的第一个页面 */
	bool m_memOnly;	/** 文件是否只存在内存，而不在外存 */
	bool	m_directIo;	/** 是否启用directIo */
	Syslog *m_sysLog;
	/** 
	 * 预分配一段IO缓存，以避免将来内存分配失败
	 * 该缓存主要用于初始化日志文件，缓存被所有LogFile共享
	 */
	static IoBuffer m_ioBuffer; 
};
/** 日志文件管理器 */
class LogFileMngr {
public:
	static LogFileMngr* open(Txnlog *txnLog, const char *basename, uint numLogFile) throw (NtseException);
	static void create(const char *basename, uint numLogFile, size_t fileSize) throw (NtseException);
	static LsnType recreate(const char *basename, uint numLogFile, size_t fileSize, LsnType startLsn) throw (NtseException);
	static void drop(const char *basename, uint numLogFile) throw (NtseException);

	size_t readPages(LsnType lsn, void *page, size_t pageCnt);
	LogFile* curLogFile() const;
	LogFile* getNewLogFile(LsnType startLsn);
	void switchLogFile();
	void close();
	void reset(LsnType nextLsn);
	LogFile* findLogFile(LsnType lsn, LockMode lock = None);
	void setCurLogFile(LsnType lsn);
	LsnType initOnFirstOpen();
	u64 getLogFileSize() const;
	static std::string makeFileName(const char *basename, uint index);
	void reclaimOverflowSpace(uint fileCntHwm);
	int setOnlineLsn(LsnType lsn);
	void clearOnlineLsn(int token);
	LsnType getOnlineLsn(LockMode mode = Shared);
	void setCheckpointLsn(LsnType lsn);
private:
	~LogFileMngr();
	LogFile* findUnusedLogFile();
	LogFileMngr(const char *basename, Syslog *sysLog, ControlFile *ctrFile, bool directIo);
#ifdef TNT_ENGINE
	LogFileMngr(const char *basename, Syslog *sysLog, TNTControlFile *tntCtrlFile, ControlFile *ctrFile, bool directIo);
#endif
	std::string makeFileName(uint index);

private:
	vector<LogFile *> m_logFiles;		/** 日志文件数组 */
	LogFile *m_curLogFile;				/** 当前日志文件 */
	u64 m_fileSize;						/** 日志文件大小 */
	RWLock m_lock;
	std::string m_basename;
	Syslog *m_sysLog;					/** 系统日志类  */
#ifdef TNT_ENGINE
	TNTControlFile *m_tntCtrlFile;      /** tnt控制文件 */
	bool            m_sync;
#endif
	ControlFile *m_ctrlFile;			/** 数据库控制文件 */
	bool m_directIo;					/** 是否directIo */
	LsnType m_onlineLsn;				/** 最小在线LSN, 之后的日志都不被回收 */
	/**
	 * 记录当前设置过的onlineLsn
	 * m_onlineLsn = min{lsn in m_onlineLsnSettings}
	 */
	vector<LsnType> m_onlineLsnSettings;
};

/** 页面回写句柄 */
struct FlushHandle {
	inline LsnType endLsn() const {
		return m_startPage->m_startLsn + m_pageCnt * LOG_PAGE_SIZE;
	}
	inline LsnType firstLsn() const {
		return m_startPage->m_startLsn;
	}
	/** 验证日志页面的正确性 */
	inline void verify() {
#ifdef TXNLOG_DEBUG
		for (size_t i = 0; i < m_pageCnt; ++i)
			verifyPage((LogPageHdr *)((byte *)m_startPage + i * LOG_PAGE_SIZE));
#endif
	}


	LogPageHdr	*m_startPage;	/** 起始页面	*/
	size_t		m_pageCnt;		/** 页数		*/
};
/** Flush等待者 */
class FlushWaiter {
public:
	FlushWaiter(LsnType targetLsn)
		: m_targetLsn(targetLsn)
	{}
	inline void wait() {
		m_evt.wait(-1);
	}
	inline void signal() {
		m_evt.signal();
	}
	inline LsnType getTargetLsn() const {
		return m_targetLsn;
	}
private:
	Event	m_evt;			/** 事件			*/
	LsnType	m_targetLsn;	/** Flush目标LSN	*/
};

/** 日志回写线程 */
class LogFlusher : public Task {
public:
	LogFlusher(Txnlog *txnLog, uint flushInterval)
		: Task("LogFlusher", flushInterval), m_lock("LogFlusher::lock", __FILE__, __LINE__)
		, m_txnLog(txnLog), m_targetLsn(0), m_writenPages(0), m_enable(true) {
#ifdef TNT_ENGINE
			m_sync = (NULL != txnLog->m_tntDb? TFM_FLUSH_SYNC == txnLog->m_tntDb->getTNTConfig()->m_trxFlushMode: false);
#endif
	}
	~LogFlusher();

	void flush(LsnType targetLsn, FlushSource fs);
	void waitFlush(LsnType targetTailLsn);
	void run();
	void enable(bool isEnable);
private:
	LsnType getTargetLsn(void);
	void signalWaiters(LsnType lsn);
	FlushHandle* getNextPagesToFlush(FlushHandle *fh, FlushHandle *toFlush);

private:
	Mutex	m_lock;
	Txnlog  *m_txnLog;
	LsnType m_targetLsn;			/** 要回写的目标LSN			*/
	DList<FlushWaiter *> m_waiters;	/** 等待刷新的线程			*/
	size_t  m_writenPages;			/** 上次以来，已写入字节数	*/
	bool    m_enable;				/** 是否开启回写线程         */
#ifdef TNT_ENGINE
	bool    m_sync;					/** 日志flush后是否需要sync */
#endif
};

/** 日志写缓存 */
class LogWriteBuffer {
public:
	LogWriteBuffer(Txnlog *txnLog, LsnType writeNext, uint pageCnt);
	~LogWriteBuffer();
	LsnType write(const LogEntry* logEntry);

	FlushHandle* nextUnflushedPages(FlushHandle *handle);
	LsnType finishFlush(FlushHandle *handle);
	LsnType getLastFlushedLsn();
	LsnType getLastWrittenLsn();
	LsnType getWriteNextLsn();
	void setLastFlushedLsn(LsnType lastFlushed);
	void setHighWatermark(double usageRatio);
	LsnType prepareFlush(LsnType flushTarget);
	void flushAll();
	LsnType getMinLsn();
	bool getPage(LsnType pageLsn, LogPageHdr **page);
	void releasePage(LogPageHdr *page);
	bool readPages(LsnType lsn, byte *buf, uint *pageCnt, bool finishCurPage);
private:
	void waitFlushOnFull(LsnType lastFlush);
	LsnType getTailLsn(LsnType flushTarget);
	LogPageHdr* lsnToPageAddr(LsnType lsn);
	LogRecordHdr* lsnToAddr(LsnType lsn);
	size_t calRecordTailLsn(size_t recordSize,  LsnType *lsnTail);
	LsnType writeSimpleRecord(const LogEntry *logEntry, LsnType lsn, LogRecordType recordType);
	LsnType writeMultiPageRecord(const LogEntry *LogEntry, LsnType lsn);

private:

	LsnType m_writeNext;	/** 下一条日志可能的LSN		*/
	LsnType m_writeLast;	/** 最后一条日志的LSN		*/
	RWLock	m_writeLock;	/** 保护m_writeLast			*/
	LsnType m_flushLast;	/** 上次回写末尾			*/
	RWLock	m_flushLock;	/** 保护m_flushLast			*/
	LsnType m_minPageLsn;	/** 缓存中日志页的最小LSN	*/
	byte	*m_buffer;		/** 缓存					*/
	size_t	m_bufferSize;	/** 缓存字节数				*/
	uint	m_pageCnt;		/** 缓存页数				*/
	LogFile	*m_curLogFile;	/** 当前日志文件			*/
	double  m_bufHwm;		/** 缓存高水线，占用率超出高水线时，应启动回写线程 */
	Txnlog  *m_txnLog;
};

/** 哈希函数 */
struct LogPageHasher {
	inline unsigned int operator () (const LogPageHdr *page) const {
		return (unsigned int)page->m_startLsn;
	}
};
/** 哈希表的Equal函数 */
struct LsnLogPageEqualer {
	inline bool operator() (LsnType lsn, const LogPageHdr *page) const {
		return page->m_startLsn == lsn;
	}
};
/** 日志读缓存 */
class LogReadBuffer : public PagePoolUser {
	/** 日志页相关信息 */
	struct PageInfo {
		LogPageHdr* m_page;			/** 日志页					*/
		DLink<PageInfo *> m_dlink;	/** DLINK，用以构造LRU链表	*/
		size_t m_poolEntry;			/** ObjectPool的本对象句柄	*/
	};

public:
	LogReadBuffer(PagePool *pool, LogFileMngr *logFileMngr, uint pageCnt, uint ioPages);
	~LogReadBuffer();
	LogPageHdr* getPage(LsnType pageLsn);
	void releasePage(LogPageHdr *pageHdr);
	uint freeSomePages(u16 userId, uint numPages);
	void truncate(LsnType lsn);

private:
	bool loadPages(LsnType lsn, uint pageCnt);
	void unloadPage(LsnType pageLsn);
	PageInfo*  getFreePage();
	void touchPage(LogPageHdr *page);
private:
	/** 日志页面哈希表 */
	DynHash<LsnType, LogPageHdr*, LogPageHasher, Hasher<LsnType>, LsnLogPageEqualer> m_pageHash;
	DList<PageInfo *>	m_lruList;		/** 页面LRU链表			*/
	LsnType				m_starLsn;		/** 缓存中第一页的LSN	*/
	uint				m_ioPages;		/** 一次读取几个页		*/
	RWLock				m_lock;			/** 用于保护全局结构	*/
	LogFileMngr			*m_logFileMngr; /** 日志文件管理器		*/
	PagePool			*m_pool;		/** 独占的PagePool		*/
	ObjectPool<PageInfo> m_pageInfoPool;/** 链表节点池			*/
};

/** 日志缓存（组合日志读写缓存) */
class LogBuffer {
public:
	LogBuffer(LogReadBuffer *readBuffer, LogWriteBuffer *writeBuffer);

	LogPageHdr* getPage(LsnType pageLsn);
	void releasePage(LogPageHdr *pageHdr);
private:
	LogReadBuffer	*m_readBuffer;		/** 日志读缓存						*/
	LogWriteBuffer	*m_writeBuffer;		/** 日志写缓存						*/
	LsnType			m_minLsnWriteBuf;	/** 日志写缓存的最小LSN（不精确）	*/
};

/** 日志阅读器 */
class LogReader {
public:
	LogReader(LogBuffer *readBuffer, Syslog *log);
	LogScanHandle* beginScan(LsnType startLsn, LsnType endLsn) const;
	LogScanHandle* getNext(LogScanHandle *handle) const;
	void endScan(LogScanHandle *handle) const;
	static LsnType getIncompleteLogEntryLSNBetween(
		LogReadBuffer *readBuffer, LsnType startPageLsn, LsnType endPageLsn);
	~LogReader();
private:
	static LsnType prepareLsnForRead(LsnType nextLsn);
private:
	LogBuffer *m_readBuffer;	/** 读缓存 */
	Syslog *m_log;
};
//////////////////////////////////////////////////////////////////////////


/**
 * 初始化日志文件，日志文件清零
 */
void LogFile::initLogfile() {
	assert(!m_memOnly);
	u64 size = m_ctrlPage->m_size;
	memset(m_ctrlPage, 0, LOG_CTRL_PAGE_SIZE);
	m_ctrlPage->m_size = size;
	NTSE_ASSERT(File::E_NO_ERROR == initLogfile(m_file, getSize()));
}



/**
 * 截断本日志文件，从lsn位置开始截断
 * @param lsn 截断位置
 * @param notZeroPageLsn 第一个无需清零的页面
 *	，用于限定清零区域，lsn之后，notZeroPageLsn之前的页面需要清零
 */
void LogFile::truncate(LsnType lsn, LsnType notZeroPageLsn) {
	assert(notZeroPageLsn);
	assert(lsn <= notZeroPageLsn);
	assert(notZeroPageLsn == INVALID_LSN || notZeroPageLsn <= getEndLsn());
	LsnType pageLsn = lsnFloor(lsn);
	if (pageLsn != lsn) {	// truncate本页面
		byte *page = (byte *)System::virtualAlloc(LOG_PAGE_SIZE);
		readPages(pageLsn, page, 1);
		LogPageHdr *pageHdr = (LogPageHdr *)page;
		size_t used = (size_t)(lsn - pageLsn);
		pageHdr->m_used = (u16)used;
		memset(page + used, 0, LOG_PAGE_SIZE - used);
		writePages(pageLsn, page, 1);
		System::virtualFree(page);

		pageLsn += LOG_PAGE_SIZE; // pageLsn指向下一页
	}
	LsnType endLsn = min(notZeroPageLsn, getEndLsn());
	if (pageLsn <= endLsn) {
		setTailHint(pageLsn);
		// 日志尾以后的日志页清零
		initPages(pageLsn, (size_t)((endLsn - pageLsn) / LOG_PAGE_SIZE));
		writeCtrlPage();
	}
}
/** 验证日志文件的有效性 */
void LogFile::verify() throw (NtseException) {
	u64 size = 0;
	m_file->getSize(&size);
	if (m_ctrlPage->m_size != size) {
		NTSE_THROW(NTSE_EC_CORRUPTED_LOGFILE, "Corrupted log file %s"
			", declared file size "I64FORMAT"u"
			", actual size "I64FORMAT"u.",
			m_file->getPath(), m_ctrlPage->m_size, size);
	}
}
/**
 * 打开一个日志文件
 * @param sysLog 系统日志
 * @param filename 日志文件名
 * @param directIo 是否directIo
 * @return 新打开的日志文件
 * @throw NtseException 打开日志文件失败
 */
LogFile* LogFile::open(Syslog *sysLog, const char *filename, bool directIo) throw (NtseException) {
	LogFile* logFile = new LogFile(sysLog, filename, directIo);
	try {
		u64 errCode = logFile->m_file->open(directIo);
		if (errCode != File::E_NO_ERROR)
			NTSE_THROW(errCode, "Open log file %s failed.", filename);
		logFile->readCtrlPage();
		assert(!logFile->getEndLsn() == 0 || logFile->getSize() > LOG_CTRL_PAGE_SIZE);
		logFile->m_memOnly = false;
		logFile->verify();
	} catch(NtseException &e) {
		logFile->close();
		throw e;
	}
	return logFile;
}
/**
 * 创建一个内存LogFile,无需在文件系统中创建日志文件
 * @param sysLog 系统日志
 * @param startLsn 该文件的起始LSN
 * @param filename 日志文件名
 * @param size 日志文件长度
 * @param directIo 是否启用directIo
 * @return 新创建的内存日志文件
 */
LogFile* LogFile::createInMem(Syslog *sysLog, LsnType startLsn, const char *filename, u64 size, bool directIo) {
	assert(size > LOG_CTRL_PAGE_SIZE);
	LogFile *logFile = new LogFile(sysLog, filename, directIo);
	logFile->m_memOnly = true;
	logFile->m_ctrlPage->m_startLsn = startLsn;
	logFile->m_ctrlPage->m_size = size;
	logFile->m_ctrlPage->m_tailHint = 0;
	logFile->m_ctrlPage->m_endLsn = startLsn + logFile->payloadSize();
	return logFile;
}

/**
 * 初始化日志文件，日志文件清零
 * @param file 日志文件
 * @param size 日志文件长度
 * @return 文件操作错误码
 */
u64 LogFile::initLogfile(File *file, u64 size) {
	assert(size > LOG_CTRL_PAGE_SIZE);
	assert((size - LOG_CTRL_PAGE_SIZE) % LOG_PAGE_SIZE == 0);

	IoBuffer *ioBuffer = (IoBuffer *)Buffer::getBatchIoBufferPool()->getInst();
	memset(ioBuffer->getBuffer(), 0, LOG_CTRL_PAGE_SIZE);
	LogFileCtrlPage* pageHdr = (LogFileCtrlPage *)ioBuffer->getBuffer();
	pageHdr->m_size = size;
	checksumCtrlPage(pageHdr);
	// 初始化控制页
	u64 errCode = file->write(0, LOG_CTRL_PAGE_SIZE, ioBuffer->getBuffer());
	if (File::E_NO_ERROR != errCode)
		return errCode;

	// 整个文件清零
	memset(ioBuffer->getBuffer(), 0, ioBuffer->getSize());
	assert(ioBuffer->getSize() % LOG_PAGE_SIZE == 0);
	size_t bufPageCnt = ioBuffer->getSize()/LOG_PAGE_SIZE;
	checksumPages((LogPageHdr *)ioBuffer->getBuffer(), bufPageCnt);
	u64 offset = LOG_CTRL_PAGE_SIZE;
	while (offset < size) {
		u32 curSize = min((u32)(size - offset), (u32)ioBuffer->getSize());
		if ((errCode = file->write(offset, curSize, ioBuffer->getBuffer())) != File::E_NO_ERROR) {
			return errCode;
		}
		offset += curSize;
	}
	Buffer::getBatchIoBufferPool()->reclaimInst(ioBuffer);
	return File::E_NO_ERROR;
}
/**
 * 在磁盘上创建一个日志文件, 日志文件状态为未使用（startLsn == 0), 所有页面都初始化为0
 * @param filename 日志文件名
 * @param size 日志文件长度
 * @throw NtseException 创建或者初始化日志文件失败
 */
void LogFile::createLogFile(const char *filename, u64 size) throw (NtseException) {
	File *file = new File(filename);
	u64 errCode;
_CREATE:
	errCode = file->create(false, false);
	if (File::E_EXIST == File::getNtseError(errCode)) {
		errCode = file->remove();
		if (File::E_NO_ERROR != errCode)
			goto _EXCEPTION;
		goto _CREATE;
	} else if (File::E_NO_ERROR != errCode) {
		goto _EXCEPTION;
	}

	errCode = file->setSize(size);
	if (File::E_NO_ERROR != errCode)
		goto _EXCEPTION;

	errCode = initLogfile(file, size);
	if (errCode != File::E_NO_ERROR)
		goto _EXCEPTION;

	file->close();
	delete file;
	return;
_EXCEPTION:
	delete file;
	NTSE_THROW(errCode, "Create log file %s failed.", filename);
}


/**
 * 读取多个连续页面
 * @param lsn 起始页的LSN
 * @param page 缓存地址
 * @param pageCnt 待读取的页数
 */
void LogFile::readPages(LsnType lsn, void *page, size_t pageCnt) {
	assert(lsn == lsnFloor(lsn));
	assert(lsn + pageCnt * LOG_PAGE_SIZE <= getEndLsn());
	u64 offset = lsn - getStartLsn() + LOG_CTRL_PAGE_SIZE;
	u64 errCode = m_file->read(offset, (u32)pageCnt * LOG_PAGE_SIZE, page);
	if (errCode != File::E_NO_ERROR)
		m_sysLog->fopPanic(errCode, "Read log file %s failed.", m_file->getPath());
	for (uint i = 0; i < pageCnt; ++i) {
		byte *curPage = (byte *)page + i*LOG_PAGE_SIZE;
		u64 v = checksum64(curPage + sizeof(u64), LOG_PAGE_SIZE - sizeof(u64));
		if (v != ((LogPageHdr*)curPage)->m_checksum)
			m_sysLog->log(EL_PANIC, "Log file %s have bad page checksum at %u."
				, m_file->getPath(), offset + i * LOG_PAGE_SIZE);
	}
}
/**
 * 写多个连续日志页面到磁盘, 并计算和填充日页志校验和
 * @param lsn 页面起始LSN
 * @param page 页面数据
 * @param pageCnt 页数
 * @return ErrorCode
 */
u64 LogFile::doWritePages(LsnType lsn, void *page, size_t pageCnt) {
	checksumPages((LogPageHdr *)page, pageCnt);
	return doWriteChecksumedPages(lsn, page, pageCnt);
}
/**
 * 写多个连续日志页面到磁盘, 不计算日页志校验和
 * @param lsn 页面起始LSN
 * @param page 页面数据
 * @param pageCnt 页数
 * @return ErrorCode
 */
u64 LogFile::doWriteChecksumedPages(LsnType lsn, const void *page, size_t pageCnt) {
	assert(lsn == lsnFloor(lsn));
	assert(lsn >= getStartLsn());
	// reset的时候，会重写整个文件
	// 因此以下assert不成立
	// assert(lsn + pageCnt * LOG_PAGE_SIZE <= getEndLsn());
	assert(lsn + pageCnt * LOG_PAGE_SIZE <= getStartLsn() + payloadSize());

	u64 offset = lsn - getStartLsn() + LOG_CTRL_PAGE_SIZE;
#ifndef NDEBUG
	u64 fileSize;
	m_file->getSize(&fileSize);
	assert(offset <= fileSize - LOG_PAGE_SIZE);
#endif
#ifdef NTSE_TRACE
	char buf[6];
	memset(buf, 0, sizeof(buf));
	const char *path = m_file->getPath();
	size_t len = strlen(path);
	if (len <= sizeof(buf) - 1)
		strncpy(buf, path, len);
	else
		strncpy(buf, path + len - sizeof(buf) + 1, sizeof(buf) - 1);
	ftrace(ts.log, tout << buf << offset << lsn << page << (u32)pageCnt);
#endif
	return m_file->write(offset, (u32)pageCnt * LOG_PAGE_SIZE, page);
}

/**
 * 写多个连续日志页面到磁盘，计算和更新页校验和
 * @pre lsn必须页对齐
 * @param lsn 写入位置的LSN
 * @param page 待写入的页
 * @param pageCnt 页数
 */
void LogFile::writePages(LsnType lsn, void *page, size_t pageCnt) {
	u64 errCode = doWritePages(lsn, page, pageCnt);
	if (errCode != File::E_NO_ERROR)
		m_sysLog->fopPanic(errCode, "Write log file %s failed.", m_file->getPath());
}
/**
 * 更新多页校验和
 * @param page 待写入的页
 * @param pageCnt 页数
 */
void LogFile::checksumPages(LogPageHdr *page, size_t pageCnt) {
	for (uint i = 0; i < pageCnt; ++i)
		checksumPage((LogPageHdr*)((byte *)page + i * LOG_PAGE_SIZE));
}
/**
 * 更新单页校验和
 * @param page 待写入的页
 * @param pageCnt 页数
 */
void LogFile::checksumPage(LogPageHdr *page) {
	u64 v = checksum64((byte*)page + sizeof(u64), LOG_PAGE_SIZE - sizeof(u64));
	page->m_checksum = v;
}
/**
 * 更新控制页校验和
 * @param page 待写入的页
 * @param pageCnt 页数
 */
void LogFile::checksumCtrlPage(LogFileCtrlPage *page) {
	u64 v = checksum64((byte*)page + sizeof(u64), LOG_CTRL_PAGE_SIZE - sizeof(u64));
	page->m_checksum = v;
}
/** 日志文件的起始LSN */
LsnType LogFile::getStartLsn() const {
	return m_ctrlPage->m_startLsn;
}
/**
 * 日志文件的结束LSN
 * @param lock 是否需要枷锁
 */
LsnType LogFile::getEndLsn(bool lock) {
	if (lock) {
		RWLOCK(&m_lock, Shared);
		LsnType lsn = m_ctrlPage->m_endLsn;
		RWUNLOCK(&m_lock, Shared);
		return lsn;
	} else {
		return m_ctrlPage->m_endLsn;
	}
}
/**
 * 设置日志文件的结尾LSN,
 * 当前日志文件不足以写下一整条日志记录时，调用本函数，提前结束当前日志文件
 * @param endLsn 结尾LSN
 */
void LogFile::setEndLsn(LsnType endLsn) {
	assert(endLsn == lsnFloor(endLsn));
	RWLOCK(&m_lock, Exclusived);
	m_ctrlPage->m_endLsn = endLsn;
	m_ctrlPage->m_full = 1;
	RWUNLOCK(&m_lock, Exclusived);
	setTailHint(endLsn - LOG_PAGE_SIZE);
	m_sysLog->log(EL_DEBUG, "Set end LSN of log file %s to "I64FORMAT"u.", getPath(), endLsn);
}
/**
 * 设置起始LSN,并重新计算结尾LSN
 *	调用时机有二：
 *	其一，创建日志文件之后，初次打开日志文件之时，日志文件仍处在未初始化状态(startLsn == 0)
 *	其二，备份恢复时
 * @param startLsn 起始LSN
 */
void LogFile::setStartLsn(LsnType startLsn) {
	// 第一次打开数据库时，日志文件的endLsn为0
	// 前n-1次open数据库都没有写日志，那么第n次open：startLsn = Txnlog::MIN_LSN
	assert(m_ctrlPage->m_endLsn == 0 || m_ctrlPage->m_startLsn == Txnlog::MIN_LSN);
	assert(!m_memOnly);
	assert(startLsn == lsnFloor(startLsn));
	m_ctrlPage->m_startLsn = startLsn;
	m_ctrlPage->m_endLsn = startLsn + payloadSize();
}
/** 一个日志文件实际用作存储日志的空间 */
u64 LogFile::payloadSize() const {
	return m_ctrlPage->m_size - LOG_CTRL_PAGE_SIZE;
}
/** 获取日志文件大小 */
u64 LogFile::getSize() const {
	return m_ctrlPage->m_size;
}
/** 获取日志文件全路径 */
const char* LogFile::getPath() const {
	return m_file->getPath();
}
/**
 * 重置日志文件, 使得日志文件可以从nextLsn开始写入
 *	如果当前是内存日志文件，则创建物理日志文件
 * @param nextLsn 起始LSN地址，必须页对齐
 */
void LogFile::reset(LsnType nextLsn){
	assert(nextLsn == lsnFloor(nextLsn));
	assert(nextLsn >= getStartLsn() && nextLsn < getEndLsn());
	if (m_memOnly) { // 日志文件不存在磁盘上，此时创建一个日志文件
		try {
			createLogFile(m_file->getPath(), m_ctrlPage->m_size);
			u64 errNo = m_file->open(m_directIo);
			if (File::getNtseError(errNo) != File::E_NO_ERROR)
				NTSE_THROW(errNo, "Open log file %s failed", m_file->getPath());
			m_memOnly = false;
		} catch(NtseException &e) {
			m_sysLog->log(EL_PANIC, "Create log file %s failed. Exception message: %s"
				, m_file->getPath(), e.getMessage());
		}
	} else {
		u32 remainPageCnt = (u32)((getStartLsn() + payloadSize() - nextLsn)/LOG_PAGE_SIZE);
		u32 pageCnt = min(remainPageCnt, (u32)LogConfig::DEFAULT_MAX_IO_PAGES);
		initPages(nextLsn, pageCnt);
	}
	// 写入控制页面
	writeCtrlPage();
}
/**
 * 初始化页面
 * @param lsn 起始页lsn
 * @param pageCnt 页数
 */
void LogFile::initPages(LsnType lsn, size_t pageCnt) {
	assert(lsn == lsnFloor(lsn));
	assert(getStartLsn() + payloadSize() >= lsn + pageCnt * LOG_PAGE_SIZE);
	
	IoBuffer *ioBuffer = (IoBuffer *)Buffer::getBatchIoBufferPool()->getInst();
	memset(ioBuffer->getBuffer(), 0, ioBuffer->getSize());
	assert(ioBuffer->getSize() % LOG_PAGE_SIZE == 0);
	size_t bufPageCnt = ioBuffer->getSize()/LOG_PAGE_SIZE;
	checksumPages((LogPageHdr *)ioBuffer->getBuffer(), bufPageCnt);

	size_t remainPageCnt = pageCnt;
	while (remainPageCnt > 0) {
		size_t cnt = min(remainPageCnt, bufPageCnt);
		writePages(lsn, ioBuffer->getBuffer(), cnt);
		lsn += cnt * LOG_PAGE_SIZE;
		remainPageCnt -= cnt;
	}
	Buffer::getBatchIoBufferPool()->reclaimInst(ioBuffer);
}
/**
 * 设置tail hint
 * @param lsn 必须包含在[startLsn, endLsn)之中
 */
void LogFile::setTailHint(LsnType lsn) {
	m_ctrlPage->m_tailHint = lsn;
}
/** 获取tail hint */
LsnType LogFile::getTailHint() const {
	return m_ctrlPage->m_tailHint;
}
/**
 * 判断日志文件是否已经用尽
 *	用以加速寻找日志末尾的效率
 * 返回true时，日志文件必定用尽
 * 返回false时，日志文件不一定用尽
 */
bool LogFile::full() {
	return m_ctrlPage->m_full == 1;
}
/** 设置日志文件full标记 */
void LogFile::full(bool isFull) {
	m_ctrlPage->m_full = (u8)isFull;
}
/** 构造一个日志文件对象 */
LogFile::LogFile(Syslog *sysLog, const char *filename, bool directIo)
	: m_lock("LogFile::lock", __FILE__, __LINE__), m_file(0), m_ctrlPage(0)
	, m_memOnly(false), m_directIo(directIo), m_sysLog(sysLog) {
	m_file = new File(filename);
	m_ctrlPage = (LogFileCtrlPage *)System::virtualAlloc(LOG_CTRL_PAGE_SIZE);
	memset(m_ctrlPage, 0, LOG_CTRL_PAGE_SIZE);
}
/** 关闭日志文件，回收内存 */
void LogFile::close() {
	if (m_file) {
		m_file->close();
		delete m_file;
		m_file = 0;
	}
	if (m_ctrlPage) {
		System::virtualFree(m_ctrlPage);
		m_ctrlPage = 0;
	}
	delete this;
}

/**
 * 重用日志文件
 * @param startLsn 日志文件新的起始LSN
 */
void LogFile::reuse(LsnType startLsn) {
	assert(!m_memOnly);
	assert(m_ctrlPage->m_endLsn <= startLsn);

	m_ctrlPage->m_startLsn = startLsn;
	m_ctrlPage->m_endLsn = m_ctrlPage->m_startLsn + payloadSize();
	m_ctrlPage->m_full = false;
	m_ctrlPage->m_tailHint = 0;
}
LogFile::~LogFile() {
}
/** 读取控制页 */
void LogFile::readCtrlPage() {
	u64 errCode = m_file->read(0, LOG_CTRL_PAGE_SIZE, m_ctrlPage);
	u64 v = checksum64((byte*)m_ctrlPage + sizeof(u64), LOG_CTRL_PAGE_SIZE - sizeof(u64));
	if (v != m_ctrlPage->m_checksum)
		m_sysLog->log(EL_PANIC, "Log file %s have bad control page checksum.", m_file->getPath());
	if (errCode != File::E_NO_ERROR)
		m_sysLog->fopPanic(errCode, "read control page of %s failed.", m_file->getPath());
}
/** 写入控制页 */
void LogFile::writeCtrlPage() {
	m_ctrlPage->m_checksum = checksum64((byte*)m_ctrlPage + sizeof(u64), LOG_CTRL_PAGE_SIZE - sizeof(u64));
	u64 errCode = m_file->write(0, LOG_CTRL_PAGE_SIZE, m_ctrlPage);
	if (errCode != File::E_NO_ERROR)
		m_sysLog->fopPanic(errCode, "write control page of %s failed.", m_file->getPath());
}

//////////////////////////////////////////////////////////////////////////
/**
 * 构造一个日志文件管理器
 * @param basename 日志文件名，不包括后缀
 * @param sysLog 系统日志
 * @param ctrlFile 控制文件
 * @param 是否开启directIo
 */
LogFileMngr::LogFileMngr(const char *basename, Syslog *sysLog, ControlFile *ctrlFile, bool directIo)
	: m_curLogFile(0), m_fileSize(0), m_lock("LogFileMngr::lock", __FILE__, __LINE__)
	, m_basename(basename), m_sysLog(sysLog), m_ctrlFile(ctrlFile), m_directIo(directIo), m_onlineLsn(INVALID_LSN)
	 {
	m_onlineLsnSettings.push_back(INVALID_LSN);	// 保留一个token位置;
#ifdef TNT_ENGINE
	m_tntCtrlFile = NULL;
#endif
}

#ifdef TNT_ENGINE
LogFileMngr::LogFileMngr(const char *basename, Syslog *sysLog, TNTControlFile *tntCtrlFile, ControlFile *ctrlFile, bool directIo)
	: m_curLogFile(0), m_fileSize(0), m_lock("LogFileMngr::lock", __FILE__, __LINE__)
	, m_basename(basename), m_sysLog(sysLog), m_tntCtrlFile(tntCtrlFile), m_ctrlFile(ctrlFile), m_directIo(directIo), m_onlineLsn(INVALID_LSN)
	 {
	m_onlineLsnSettings.push_back(INVALID_LSN);	// 保留一个token位置;
}
#endif
/**
 * 打开日志文件管理器
 * @param txnLog 所需日志管理器
 * @param basename 日志文件名，不包括后缀
 * @param numLogFile 当前日志文件个数
 * @return 日志文件管理器
 * @throw NtseException 打开日志文件管理器失败
 */
LogFileMngr* LogFileMngr::open(Txnlog *txnLog, const char *basename, uint numLogFile) throw (NtseException) {
	Syslog *sysLog = NULL;
	LogFileMngr *lfm = NULL;
	Database *db = NULL;
#ifdef TNT_ENGINE
	if (NULL != txnLog->m_tntDb) {
		db = txnLog->m_tntDb->getNtseDb();
	} else {
#endif
		db = txnLog->m_db;
#ifdef TNT_ENGINE
	}
#endif
	sysLog = db->getSyslog();

#ifdef TNT_ENGINE
	if (txnLog->m_tntDb != NULL) {
		lfm = new LogFileMngr(basename, sysLog, txnLog->m_tntDb->getTNTControlFile(),
			db->getControlFile(), true);
		lfm->m_sync = (NULL != txnLog->m_tntDb? TFM_FLUSH_SYNC == txnLog->m_tntDb->getTNTConfig()->m_trxFlushMode: false);
	} else {
#endif
		lfm = new LogFileMngr(basename, sysLog, db->getControlFile(), true);
#ifdef TNT_ENGINE
		lfm->m_sync = false;
	}
#endif

	lfm->m_fileSize = 0;
	try {
		// 检查是否存在日志文件，如果不存在日志文件，则需自动创建
		bool fileExists = false;
		for (uint i = 0; i < numLogFile && !fileExists; ++i)
			fileExists = File::isExist(lfm->makeFileName(i).c_str());
		if (!fileExists)
			NTSE_THROW(NTSE_EC_MISSING_LOGFILE, "Log file don't exists!");

		// 打开所有日志文件
		for (uint i = 0; i < numLogFile; ++i) {
			LogFile* file = LogFile::open(sysLog, lfm->makeFileName(i).c_str(), false);

			if (lfm->m_fileSize) {
				if (lfm->m_fileSize != file->getSize())
					NTSE_THROW(NTSE_EC_CORRUPTED_LOGFILE, "Log files have different size.");
			} else {
				lfm->m_fileSize = file->getSize();
			}
			lfm->m_logFiles.push_back(file);
		}
	} catch(NtseException &e) {
		lfm->close();
		throw e;
	}
	return lfm;
}
/**
 * 创建日志文件管理器
 * @param basename 日志文件名，不包括后缀
 * @param numLogFile 需要创建的日志文件个数
 * @param fileSize 日志文件长度
 * @throw NtseException 创建日志文件管理器失败
 */
void LogFileMngr::create(const char *basename, uint numLogFile, size_t fileSize) throw (NtseException) {
	for (uint i = 0; i < numLogFile; ++i)
		LogFile::createLogFile(makeFileName(basename, i).c_str(), fileSize);
}

/**
 * 重新创建日志文件管理器
 * @param basename 日志文件名，不包括后缀
 * @param numLogFile 需要创建的日志文件个数
 * @param fileSize 日志文件长度
 * @param startLsn 日志起始LSN，一般为原有检查点LSN
 * @return 实际的日志起始LSN
 * @throw NtseException 创建日志文件管理器失败
 */
LsnType LogFileMngr::recreate(const char *basename, uint numLogFile
						, size_t fileSize, LsnType startLsn) throw (NtseException) {
	create(basename, numLogFile, fileSize);
	if (startLsn) {
		startLsn = lsnCeiling(startLsn);
		LogFile* logFile = LogFile::open(NULL, makeFileName(basename, 0).c_str());
		logFile->setStartLsn(startLsn);
		logFile->writeCtrlPage();
		logFile->close();
	}

	return startLsn;
}
/**
 * @param basename 日志文件名，不包括后缀
 * @param numLogFile 需要创建的日志文件个数
 * @throw NtseException 创建日志文件管理器失败
 */
void LogFileMngr::drop(const char *basename, uint numLogFile) throw (NtseException) {
	for (uint i  = 0; i < numLogFile; ++i) {
		File *file = new File(LogFileMngr::makeFileName(basename, i).c_str());
		u64 errCode = file->remove();
		if (errCode != File::E_NO_ERROR && File::getNtseError(errCode) != File::E_NOT_EXIST)
			NTSE_THROW(errCode, "Remove file %s failed.", file->getPath());
		delete file;
	}
}
/**
 * 从磁盘上读取连续的几个页
 * @param lsn 第一个页的LSN
 * @param page 页起始地址
 * @param pageCnt 需要读取的页数
 * @return 成功读取的页数，只有读到日志末尾的时候返回值才不等于pageCnt
 */
size_t LogFileMngr::readPages(LsnType lsn, void *page, size_t pageCnt) {
	assert(lsn == lsnFloor(lsn)); // 按照页对齐
	size_t total = pageCnt;
	RWLOCK(&m_lock, Shared);
	while (pageCnt > 0) {
		LogFile *logFile = findLogFile(lsn);
		if (!logFile)
			break;
		LsnType endLsn = logFile->getEndLsn();
		size_t remainPageCnt = (size_t)(endLsn - lsn) / LOG_PAGE_SIZE;
		size_t readCnt = min(pageCnt, remainPageCnt);
		pageCnt -= readCnt;
		logFile->readPages(lsn, page, readCnt);
		size_t readBytes = readCnt * LOG_PAGE_SIZE;
		page = (byte*)page + readBytes;
		lsn += readBytes;
		if (logFile == m_curLogFile) // 当前日志文件
			break;
	}
	RWUNLOCK(&m_lock, Shared);
	return total - pageCnt;
}
/** 获取当前日志文件 */
LogFile* LogFileMngr::curLogFile() const {
	return m_curLogFile;
}
/** 获取日志文件长度 */
u64 LogFileMngr::getLogFileSize() const {
	return m_fileSize;
}

/**
 * 创建一个日志文件对象，但是不负责在磁盘上创建和初始化日志文件
 * @param startLsn 新文件的起始LSN
 * @return 日志文件对象
 */
LogFile* LogFileMngr::getNewLogFile(LsnType startLsn) {
	RWLOCK(&m_lock, Exclusived);
	LogFile *logFile = findUnusedLogFile();
	if (!logFile) { // 如果找不到可用日志文件，则创建新的日志文件对象
		// 磁盘上日志末尾还不在本文件中，因此该文件根本不应该在磁盘上存在;
		// 因此这里只创建一个内存中的日志文件;
		logFile = LogFile::createInMem(m_sysLog, startLsn
			, makeFileName((uint)m_logFiles.size()).c_str(), m_fileSize
			, m_directIo);
		m_sysLog->log(EL_LOG, "Create log file %s in memory, start LSN "I64FORMAT"u."
			, logFile->getPath(), startLsn);
		m_logFiles.push_back(logFile);
	} else { // 利用旧的日志文件对象
		m_sysLog->log(EL_LOG, "Reuse log file %s, new start LSN "I64FORMAT"u."
			, logFile->getPath(), startLsn);
		logFile->reuse(startLsn);
		if (m_curLogFile == logFile) {
			// 总共只有一个日志文件时，可能会重用当前文件
			// 重用当前文件时，更改了m_curLogFile->getEndLsn()，导致switchLogFile不干活
			// 因此需要在此提前reset
#ifdef TNT_ENGINE
			if (m_tntCtrlFile != NULL) {
				assert(m_tntCtrlFile->getDumpLsn() == startLsn);
			}
#endif
			assert(m_ctrlFile->getCheckpointLSN() == startLsn);
			m_sysLog->log(EL_LOG, "Reset log file %s, start LSN "I64FORMAT"u.", m_curLogFile->getPath(), startLsn);
			logFile->reset(startLsn);
		}
	}
	RWUNLOCK(&m_lock, Exclusived);
	return logFile;
}
/**
 * 切换日志文件(结束当前日志文件，并初始化新文件作为新的当前日志文件)
 *	当回写日志页的LSN超出当前文件末尾时，本函数被日志回写线程调用
 */
void LogFileMngr::switchLogFile() {
	assert(m_curLogFile->full());
	LsnType lsn = m_curLogFile->getEndLsn();
	m_sysLog->log(EL_LOG, "Switch log file %s, end LSN "I64FORMAT"u. ", m_curLogFile->getPath(), lsn);
	m_curLogFile->writeCtrlPage();
#ifdef TNT_ENGINE
	if (m_sync) {
		m_curLogFile->sync();
	}
#endif
	reset(lsn);
}
/**
 * 创建日志之后，第一次打开日志模块之时的初始化动作
 * @param startLsn 初始LSN，按照页对齐
 * @return 返回第一条日志的LSN
 */
LsnType LogFileMngr::initOnFirstOpen() {
	m_curLogFile = m_logFiles[0];
	m_curLogFile->setStartLsn(Txnlog::MIN_LSN);
	m_curLogFile->writeCtrlPage();
	m_curLogFile->reset(Txnlog::MIN_LSN);
	return Txnlog::MIN_LSN;
}
/**
 * 寻找一个未使用的日志文件
 * @pre 已经加上互斥锁
 * @return 可重复利用的日志文件
 */
LogFile* LogFileMngr::findUnusedLogFile() {
	assert(m_lock.isLocked(Exclusived));
	LsnType onlineLsn = getOnlineLsn(None);
	for (uint i = 0; i < m_logFiles.size(); ++i) {
		if (m_logFiles[i]->getEndLsn() <= onlineLsn)
			return m_logFiles[i];
	}
	return 0;
}
/**
 * 找到一个包含lsn的日志文件
 * @param lsn 目标lsn，日志文件必须包含该lsn
 * @param mode 锁模式
 * @return 包含lsn的日志文件
 */
LogFile* LogFileMngr::findLogFile(LsnType lsn, LockMode mode) {
	if (mode != None)
		RWLOCK(&m_lock, mode);
	LogFile *logFile = 0;
	for (uint i = 0; i < m_logFiles.size(); ++i) {
		if (lsn >= m_logFiles[i]->getStartLsn() && lsn < m_logFiles[i]->getEndLsn()) {
			logFile = m_logFiles[i];
			goto _ret;
		}
	}
_ret:
	if (mode != None)
		RWUNLOCK(&m_lock, mode);
	return logFile;
}
/**
 * 设置当前日志文件为包含lsn的日志文件
 * 打开日志模块，或者截断日志时，都需调用本函数设置当前日志文件
 */
void LogFileMngr::setCurLogFile(LsnType lsn) {
	LogFile *logFile = findLogFile(lsn);
	assert(logFile);
#ifndef NTSE_UNIT_TEST
	assert(!m_curLogFile);
#endif
	m_curLogFile = logFile;
}
/** 关闭日志文件管理器，并删除内存 */
void LogFileMngr::close() {
	RWLOCK(&m_lock, Exclusived);
	for (uint i = 0; i < m_logFiles.size(); ++i)
		m_logFiles[i]->close();
	m_logFiles.clear();
	RWUNLOCK(&m_lock, Exclusived);
	delete this;
}
/**
 * 重置日志(不能并发调用）
 *	重新初始化日志文件nextLsn之后部分
 *	并设置当前日志文件
 * @param nextLsn	日志文件中最后一条有效日志记录的末尾，下一条日志记录可能的起始地址
 *					nextLsn必须页对齐
 */
void LogFileMngr::reset(LsnType nextLsn) {
	RWLOCK(&m_lock, Exclusived);
	LogFile *lf = findLogFile(nextLsn);
	assert(lf);
	m_sysLog->log(EL_LOG, "Reset log file %s, start LSN "I64FORMAT"u.", lf->getPath(), nextLsn);
	lf->reset(nextLsn);
	m_curLogFile = lf;
	m_ctrlFile->setNumTxnlogs((u32)m_logFiles.size());
	RWUNLOCK(&m_lock, Exclusived);
}
LogFileMngr::~LogFileMngr() {
}
std::string LogFileMngr::makeFileName(uint index) {
	return makeFileName(m_basename.c_str(), index);
}
/**
 * 构造日志文件名
 * @param basename 日志文件名，不包括后缀
 * @param index 日志文件序号，表示第几个日志文件
 * @param 日志文件名
 */
std::string LogFileMngr::makeFileName(const char* basename, uint index) {
	std::stringstream ss;
	ss << basename << "." << index;
	return ss.str();
}

/**
 * 回收溢出日志文件
 * @param fileCntHwm 日志文件数目高水线
 */
void LogFileMngr::reclaimOverflowSpace(uint fileCntHwm) {
	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	LsnType onlineLsn = getOnlineLsn(None);
	while(m_logFiles.size() > fileCntHwm) {
		LogFile *lastFile = m_logFiles.back();
		LsnType endLsn = lastFile->getEndLsn(true);
		if (onlineLsn > endLsn) {
			m_logFiles.pop_back();
			m_ctrlFile->setNumTxnlogs((u32)m_logFiles.size());

			string path = lastFile->getPath();
			lastFile->close();
			u64 ec = File(path.c_str()).remove();
			if (ec != File::E_NO_ERROR) {
				m_sysLog->log(EL_ERROR, "Remove log file %s failed, reason %s. Please remove manually."
					, path.c_str(), File::explainErrno(ec));
				break;
			}
			m_sysLog->log(EL_LOG, "Reclaim log file %s. endLsn: "I64FORMAT"u", path.c_str(), endLsn);
		} else {
			break;
		}
	}
}

/**
 * 设置最小在线日志lsn， 日志模块保证lsn之后的日志都能够读取
 *
 * @param lsn 最小在线lsn
 * @return 更新成功返回一个句柄，更新失败返回值小于0
 */
int LogFileMngr::setOnlineLsn(LsnType lsn) {
	assert(lsn != INVALID_LSN);
	LsnType beginLsn;
#ifdef TNT_ENGINE
	if (m_tntCtrlFile != NULL) {
		beginLsn = m_tntCtrlFile->getDumpLsn();
		beginLsn = min(beginLsn, m_ctrlFile->getCheckpointLSN());
	} else {
#endif
		assert(m_tntCtrlFile == NULL);
		beginLsn = m_ctrlFile->getCheckpointLSN();
#ifdef TNT_ENGINE
	}
#endif
	// 加锁保证lsn对应的日志文件不被回收
	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	// lsn不能太小
	if (lsn < min(beginLsn, m_onlineLsn))
		return -1;
	// 寻找空闲位置
	int token = 0;
	for (token = 0; token < (int)m_onlineLsnSettings.size(); ++token) {
		if (m_onlineLsnSettings[token] == INVALID_LSN) { // 找到
			m_onlineLsnSettings[token] = lsn;
			break;
		}
	}
	// 如果没有找到空闲位置, 则增加一个token
	if (token == (int)m_onlineLsnSettings.size())
		m_onlineLsnSettings.push_back(lsn);
	// 调整onlineLsn
	if (m_onlineLsn > lsn)
		m_onlineLsn = lsn;

	return token;
}

/**
 * 清除onlineLsn设置
 *
 * @param token setOnlineLsn返回的token
 */
void LogFileMngr::clearOnlineLsn(int token) {
	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	assert(m_onlineLsn != INVALID_LSN);
	// 检测有效性
	if (token < 0 || token >= (int)m_onlineLsnSettings.size()
		|| m_onlineLsnSettings[token] == INVALID_LSN)
			return;
	LsnType lsn = m_onlineLsnSettings[token];
	// 清除设置
	if (token == (int)m_onlineLsnSettings.size() - 1 && m_onlineLsnSettings.size() > 1) {
		// 减小m_onlineLsnSettings数组，但是最少保持一个元素
		do {
			m_onlineLsnSettings.pop_back();
		} while (m_onlineLsnSettings.size() > 1 && m_onlineLsnSettings.back() == INVALID_LSN);
	} else {
		m_onlineLsnSettings[token] = INVALID_LSN;
	}
	// 判断是否需要重新计算m_onlineLsn
	if (lsn == m_onlineLsn)
		m_onlineLsn = *min_element(m_onlineLsnSettings.begin(), m_onlineLsnSettings.end());
}

/**
 * 得到在线LSN
 *	只有在线LSN之前的日志文件可以被重用、回收
 *	
 * @param mode 锁模式
 * @return 在线LSN
 */
LsnType LogFileMngr::getOnlineLsn(LockMode mode) {
	LsnType beginLsn;
	assert(m_ctrlFile != NULL);
#ifdef TNT_ENGINE
	if (m_tntCtrlFile != NULL) {
		beginLsn = m_tntCtrlFile->getDumpLsn();
		beginLsn = min(beginLsn, m_ctrlFile->getCheckpointLSN());
	} else {
#endif
		beginLsn = m_ctrlFile->getCheckpointLSN();
#ifdef TNT_ENGINE
	}
#endif
	if (mode != None) {
		RWLockGuard guard(&m_lock, mode, __FILE__, __LINE__);
		return min(beginLsn, m_onlineLsn);
	} else {
		return min(beginLsn, m_onlineLsn);
	}
}


/**
 * 设置检查点LSN
 * @param lsn 新的检查点Lsn
 */
void LogFileMngr::setCheckpointLsn(LsnType lsn)  {
	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	m_ctrlFile->setCheckpointLSN(lsn);
}

/**void LogFileMngr::setDumpLsn(LsnType lsn) {
	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	m_tntCtrlFile->setDumpLsn(lsn);
}*/

//////////////////////////////////////////////////////////////////////////
LogFlusher::~LogFlusher() {
	assert(m_waiters.getSize() == 0);
	stop();
	join();
}
/**
 * 唤醒回写线程，并等待"尾LSN"在targetTailLsn之前的日志记录回写到磁盘
 *	日志记录LSN是指日志记录头对应的LSN，
 *	日志记录尾LSN是指日志记录末尾对应的LSN
 */
void LogFlusher::waitFlush(LsnType targetTailLsn) {
	FlushWaiter waiter(targetTailLsn);
	DLink<FlushWaiter *> node(&waiter);
	bool inQueue = false; // 是否已经在等待队列中

	while(true) {
		LOCK(&m_lock);
		LsnType lastFlushed = m_txnLog->m_writeBuffer->getLastFlushedLsn();
		if (targetTailLsn <= lastFlushed) {// 再次判断日志是否被刷出
			if (inQueue) // 从等待队列删除
				node.unLink();
			UNLOCK(&m_lock);
			return;	// 日志已经被刷出，返回
		}
		// 更新回写目标
		if (targetTailLsn > m_targetLsn)
			m_targetLsn = targetTailLsn;

		if (!inQueue) { // 加入到等待队列
			m_waiters.addLast(&node);
			inQueue = true;
		}
		UNLOCK(&m_lock);
		signal();
		waiter.wait();
	}
}
/**
 * 刷写LSN在targetLSN之前的日志记录到磁盘
 * @param targetLsn 回写目标,必须正确指向日志记录头部
 */
void LogFlusher::flush(LsnType targetLsn, FlushSource fs) {
	if (targetLsn == 0) // Buffer会调用flush(0)
		return;
	LogWriteBuffer* writeBuffer = m_txnLog->m_writeBuffer;
	// 如果需要回写当前页，提前结束当前写入页
	// 并得到当前targetLsn日志记录对于的尾LSN
	targetLsn = writeBuffer->prepareFlush(targetLsn);
	if (targetLsn == INVALID_LSN) // 该日志已经不再缓存中
		return;
	LsnType lastFlushed = writeBuffer->getLastFlushedLsn();
	if (targetLsn <= lastFlushed) // 日志已经被刷出，直接返回
		return;
	m_txnLog->updateFlushSourceStats(fs);
	// 进入等待
	waitFlush(targetLsn);
}

/**
 * 日志线程， 一次Flush多个脏页面
 * 写日志失败时候，系统不能继续运行
 * 直接抛出NtseException
 */
void LogFlusher::run() {
	FlushHandle fh;
	LogWriteBuffer *writeBuffer = m_txnLog->m_writeBuffer;
	LogFileMngr *fileMngr = m_txnLog->m_fileMngr;
	try {
		while(m_enable) {
			LsnType targetLsn = getTargetLsn();
			//具体问题参见NTSETNT-40
			if (writeBuffer->nextUnflushedPages(&fh)) {
				m_txnLog->getSyslog()->log(EL_DEBUG, "Flush log ("I64FORMAT"x, %u)", fh.firstLsn(), fh.m_pageCnt);
				FlushHandle toFlush;
				// 分多次回写页面
				//	每次最多回写m_maxIoPages个页面
				//	且不能跨文件写
				LsnType endLsn = fh.endLsn();
				while(getNextPagesToFlush(&fh, &toFlush)) {
					toFlush.verify();
					fileMngr->curLogFile()->writePages(toFlush.firstLsn(), toFlush.m_startPage, toFlush.m_pageCnt);
					m_txnLog->updateFlushStats(toFlush.m_pageCnt);
					m_writenPages += toFlush.m_pageCnt;

#ifdef TNT_ENGINE
					if (m_sync) {
						m_txnLog->m_fileMngr->curLogFile()->sync();
					}
#endif
					signalWaiters(writeBuffer->finishFlush(&toFlush));
				}
				// 间歇性设置tailHint
				if (m_writenPages >= m_txnLog->m_cfg.m_tailHintAcc) {
					fileMngr->curLogFile()->setTailHint(endLsn - LOG_PAGE_SIZE);
					fileMngr->curLogFile()->writeCtrlPage();
					m_writenPages = 0;
				}
			}
			if (targetLsn == getTargetLsn())	// 是否设置了更大的回写目标
				break;
		}
	} catch(NtseException &e) {
		m_txnLog->getSyslog()->log(EL_PANIC, "Exception in LogFlusher, Message %s.", e.getMessage());
	}
}
/**
 * 获取下一次回写的页面，在必要的情况下切换日志文件
 * @param fh 原始回写句柄
 * @param toFlush 本次回写句柄
 * return 本次回写句柄
 */
FlushHandle* LogFlusher::getNextPagesToFlush(FlushHandle *fh, FlushHandle *toFlush) {
	if (fh->m_pageCnt == 0)
		return 0;

	LogFileMngr *fileMngr = m_txnLog->m_fileMngr;
	LsnType lastLsn = fh->endLsn(); // 待回写页面的结尾

_RESTART:
	// 写日志线程可能正在setEndLsn, 因此getEndLsn需要加锁
	LsnType endLsn = fileMngr->curLogFile()->getEndLsn();	// 当前日志文件的末尾
	if (lastLsn <= endLsn) { // 没有超出当前日志文件
		toFlush->m_startPage = fh->m_startPage;
		toFlush->m_pageCnt = min(fh->m_pageCnt, (size_t)LogConfig::DEFAULT_MAX_IO_PAGES);
	} else if (endLsn > fh->m_startPage->m_startLsn) { // 部分超出当前日志文件大小，部分写入到当前日志文件
		toFlush->m_startPage = fh->m_startPage;
		toFlush->m_pageCnt = min(fh->m_pageCnt, (size_t)LogConfig::DEFAULT_MAX_IO_PAGES);
		size_t thisFilePageCnt = (size_t)((endLsn - fh->firstLsn()) / LOG_PAGE_SIZE);
		toFlush->m_pageCnt = min(toFlush->m_pageCnt, thisFilePageCnt);
	} else if (endLsn <= fh->m_startPage->m_startLsn) { // 全部超出当前日志文件
		assert(endLsn == fh->m_startPage->m_startLsn);
		fileMngr->switchLogFile(); // 切换日志文件之后从头来过
		goto _RESTART;
	}
	fh->m_pageCnt -= toFlush->m_pageCnt;
	fh->m_startPage = fh->m_pageCnt ?
		(LogPageHdr *)((byte *)toFlush->m_startPage + toFlush->m_pageCnt * LOG_PAGE_SIZE) :	0;
	return toFlush;
}

/** 获取回写目标LSN */
LsnType LogFlusher::getTargetLsn(void) {
	LsnType lsn;
	LOCK(&m_lock);
	lsn = m_targetLsn;
	UNLOCK(&m_lock);
	return lsn;
}
/** 开启关闭回写任务 */
void LogFlusher::enable(bool isEnable) {
	m_enable = isEnable;
	if (!isEnable) {
		pause(true);
	} else {
		resume();
	}
}
/** 唤醒等待回写的线程 */
void LogFlusher::signalWaiters(LsnType lsn) {
	LOCK(&m_lock);
	DLink<FlushWaiter *> *cur = m_waiters.getHeader()->getNext();
	while (cur != m_waiters.getHeader()) {
		DLink<FlushWaiter *> *next = cur;
		next = cur->getNext();
		if (cur->get()->getTargetLsn() <= lsn)
			cur->get()->signal();
		cur = next;
	}
	UNLOCK(&m_lock);
}

//////////////////////////////////////////////////////////////////////////
LogWriteBuffer::LogWriteBuffer(Txnlog *txnLog, LsnType writeNext, uint pageCnt)
	:  m_writeNext(writeNext),m_writeLast(INVALID_LSN), m_writeLock("LogWriteBuffer::writeLock", __FILE__, __LINE__)
	, m_flushLast(writeNext), m_flushLock("LogWriteBuffer::flushLock", __FILE__, __LINE__),  m_minPageLsn(writeNext)
	, m_bufferSize(pageCnt * LOG_PAGE_SIZE), m_pageCnt(pageCnt), m_bufHwm(LogConfig::DEFAULT_BUFFER_HIGH_WATERMARK), m_txnLog(txnLog) {
	m_curLogFile = txnLog->m_fileMngr->curLogFile();
	m_buffer = (byte *)System::virtualAlloc(m_bufferSize);
	if (!m_buffer)
		txnLog->getSyslog()->log(EL_PANIC, "Unable to alloc %d bytes", m_bufferSize);
	memset(m_buffer, 0, m_bufferSize);
	assert(writeNext == lsnFloor(writeNext));
}
/** 回写所有日志 */
void LogWriteBuffer::flushAll() {
//  此处不能加锁，flush可能会对m_writeLock加X锁
// 	RWLockGuard guard(&m_writeLock, Shared);
	if (m_writeLast == INVALID_LSN)
		return;
	m_txnLog->flush(m_writeLast);
}
LogWriteBuffer::~LogWriteBuffer() {
	System::virtualFree(m_buffer);
}
/** 写一条日志到写日志缓存 */
LsnType LogWriteBuffer::write(const LogEntry* logEntry) {
	// 检测日志长度是否满足限制条件
	// 此处检测日志长度，相比在open时检测的好处是，允许使用很小的日志文件，便于测试.
	NTSE_ASSERT(logEntry->m_size <= LogConfig::MAX_LOG_RECORD_SIZE
				&& computeMinlogBufferSize(logEntry->m_size) <= m_bufferSize
				&& computeMinlogFileSize(logEntry->m_size) <= m_txnLog->m_fileSize);
	RWLOCK(&m_writeLock, Exclusived);
	ftrace(ts.log, tout << logEntry);
_RESTART:
	// 调整下一条日志的LSN"m_writeNext"
	size_t pageUsed = lsnToOffInPage(m_writeNext);
	bool useCurPage = true;
	if (pageUsed + LOG_PAGE_HDR_SIZE >= LOG_PAGE_SIZE) { // 当前页没有可利用的空间
		m_writeNext = lsnCeiling(m_writeNext) + LOG_PAGE_HDR_SIZE;
		useCurPage = false;
	} else if (pageUsed <= LOG_PAGE_HDR_SIZE) { // 当前页是个新页
		// 只有跳转到_RESTART重新执行时pageUsed才有可能 == LOG_PAGE_HDR_SIZE
		assert(pageUsed == 0 || pageUsed == LOG_PAGE_HDR_SIZE);
		useCurPage = false;
		if (pageUsed == 0)
			m_writeNext += LOG_PAGE_HDR_SIZE;
	}
	LsnType nextLsn, curLsn;
	curLsn = m_writeNext;
	// 预先计算当前记录的尾LSN，以及需要的新日志页数目
	size_t newPageCnt = calRecordTailLsn(logEntry->m_size, &nextLsn);
	if (useCurPage && newPageCnt == 0) { // 可以直接写入当前页面
		m_writeLast = curLsn;
		m_writeNext = nextLsn;
		LsnType tailLsn = writeSimpleRecord(logEntry, curLsn, LR_NORMAL);
		UNREFERENCED_PARAMETER(tailLsn); // tailLsn只用来assert
		assert(tailLsn == m_writeNext);
		RWUNLOCK(&m_writeLock, Exclusived);
		// 更新统计信息
		m_txnLog->updateWriteStats(logEntry->m_size);
		return curLsn;
	}
	// 是否超出当前日志文件的容量
	if (nextLsn > m_curLogFile->getEndLsn(false)) {
		LsnType endLsn = useCurPage ? lsnCeiling(curLsn) : lsnFloor(curLsn);
		m_curLogFile->setEndLsn(endLsn);
		// 启动flush线程
		m_txnLog->m_flusher->signal();
		// 创建一个新的日志文件
		m_curLogFile = m_txnLog->m_fileMngr->getNewLogFile(m_curLogFile->getEndLsn(false));
		m_writeNext = m_curLogFile->getStartLsn();
		// 重新尝试写日志
		goto _RESTART;
	}
	// 获取待覆盖的最后一页，判断写缓存空间是否足够
	// 当nextLsn处在页首，该页实际上未使用,
	// 因此这里要用(nextLsn - 1)而不是nextLsn
	LogPageHdr *ph = lsnToPageAddr(nextLsn - 1);
	LsnType lastFlush = getLastFlushedLsn();
	if (ph->m_startLsn >= lastFlush) { // 写日志缓存已满，空间不足
		waitFlushOnFull(lastFlush);
		goto _RESTART;	// 重新尝试写日志
	} else if (ph->m_startLsn) { // 更新最小页LSN
		// ph->m_startLsn不为零时, 我们将覆盖ph这一日志页, 因此ph的下一页有最小LSN
		assert(ph->m_startLsn >= m_minPageLsn);
		LsnType nextPageLsn = ph->m_startLsn + LOG_PAGE_SIZE; // 新的minPageLsn
		// 检查被替换页的lsn是否等于nextPageLsn
		// nextPageLsn页可能等于m_minPageLsn(套圈了,一条日志记录就写满整个缓存)
		assert(lsnToPageAddr(nextPageLsn)->m_startLsn == m_minPageLsn || lsnToPageAddr(nextPageLsn)->m_startLsn == nextPageLsn);
		m_minPageLsn = nextPageLsn;
	}
	{ // 写入日志
		LsnType tailLsn = newPageCnt ? writeMultiPageRecord(logEntry, curLsn)
			: writeSimpleRecord(logEntry, curLsn, LR_NORMAL);
		UNREFERENCED_PARAMETER(tailLsn); // tailLsn只用来assert
		assert(tailLsn == nextLsn);
	}
	m_writeLast = curLsn;
	m_writeNext = nextLsn;
	RWUNLOCK(&m_writeLock, Exclusived);
	// 缓存占用超过1/3，启动刷新线程
	if (nextLsn > getLastFlushedLsn() + m_bufferSize / 3)
		m_txnLog->m_flusher->signal();
	// 更新统计信息
	m_txnLog->updateWriteStats(logEntry->m_size);
	return curLsn;
}
/**
 * 获取下一组待回写日志页
 *	此函数只被回写线程调用，而且m_flushLast只会被回写线程更改, 因此读取m_flushLast无需加锁
 *
 * @param handle 刷新句柄
 * @return 如果有待回写页面，返回一组待回写页面，否则返回0
 */
FlushHandle* LogWriteBuffer::nextUnflushedPages(FlushHandle *handle) {

	do {
		LsnType startPageLsn = m_flushLast;	// 待回写的第一页
		LsnType endPageLsn = lsnFloor(getWriteNextLsn());// 待回写最后一页的下一页
		 // 缓存最高点对应的PageLsn
		LsnType bufferEndPageLsn = (startPageLsn + m_bufferSize - 1 ) / m_bufferSize * m_bufferSize;
		if (startPageLsn != bufferEndPageLsn)
			endPageLsn= min(bufferEndPageLsn, endPageLsn); // 不能跨越缓存最高点

		if (endPageLsn <= startPageLsn) { // 再也没有待回写整页面
			// 必须用trylock，因为当日志写缓存满时，持有m_writeLock上的读锁回写日志；
			if (!RWTRYLOCK(&m_writeLock, Exclusived))
				goto _NODATA;
			if (lsnFloor(m_writeNext) > m_flushLast) { // 加锁之前，有新增日志页面
				RWUNLOCK(&m_writeLock, Exclusived);
				continue;	// 重头再来
			} else { // 处理最后一个半满页
				assert(lsnFloor(m_writeNext) == m_flushLast);
				if (m_writeNext - m_flushLast > LOG_PAGE_HDR_SIZE) { // 当前页面非空
					m_txnLog->m_stat.m_flushPaddingSize += lsnCeiling(m_writeNext) - m_writeNext;
					m_writeNext = lsnCeiling(m_writeNext); // 提前结束当前页面
					handle->m_pageCnt = 1;
					handle->m_startPage = lsnToPageAddr(m_flushLast);
					RWUNLOCK(&m_writeLock, Exclusived);
					break;
				} else { // 当前页为空
					RWUNLOCK(&m_writeLock, Exclusived);
					goto _NODATA;
				}
			}
		} else {
			handle->m_startPage = lsnToPageAddr(startPageLsn);
			handle->m_pageCnt = (size_t)((endPageLsn - startPageLsn) / LOG_PAGE_SIZE);
			assert(handle->m_pageCnt != 0);
			assert(handle->m_startPage != 0);
			break;
		}
	} while(true);

	return handle;
_NODATA:
	handle->m_pageCnt = 0;
	handle->m_startPage = 0;
	return 0;
}
/**
 * 回写完成一组页面
 *	此函数只被回写线程调用
 * @param handle 回写句柄
 * @return 上次回写末尾
 */
LsnType LogWriteBuffer::finishFlush(FlushHandle *handle) {
	setLastFlushedLsn(m_flushLast +  handle->m_pageCnt * LOG_PAGE_SIZE);
	handle->m_pageCnt = 0;
	handle->m_startPage = 0;
	return m_flushLast;
}

/**
 * 写缓存满时，回写一定量的日志
 * @param lastFlush 上次已回写的LSN
 */
void LogWriteBuffer::waitFlushOnFull(LsnType lastFlush) {
	m_txnLog->getSyslog()->log(EL_DEBUG, "Log write buffer full, wait flush...");
	RWUNLOCK(&m_writeLock, Exclusived); // 锁降级为读, 不降级会导致死锁
	RWLOCK(&m_writeLock, Shared);
	// 等待日志回写
	if (m_writeLast > lastFlush) { // m_writeLast是上一条日志的头，因此有可能小于lastFlush
		// 回写1/3日志
		m_txnLog->m_flusher->waitFlush(m_writeLast - 2 * (m_writeLast - lastFlush) / 3);
	} else {
		// 回写全部日志
		m_txnLog->m_flusher->waitFlush(m_writeLast);
	}
	RWUNLOCK(&m_writeLock, Shared);
	RWLOCK(&m_writeLock, Exclusived); // 锁升级
	m_txnLog->getSyslog()->log(EL_DEBUG, "Log flush finished, continue to write log.");
}
/**
 * 获取并用锁定日志页(目前实际锁住整个日志写缓存)
 * @param pageLsn 带读取日志页LSN
 * @param page [out] 日志页面指针，页不存在返回NULL
 * @return false日志页在读缓存中，否则true
 */
bool LogWriteBuffer::getPage(LsnType pageLsn, LogPageHdr **page) {
	assert(pageLsn == lsnFloor(pageLsn));
	RWLOCK(&m_writeLock, Shared);
	if (m_minPageLsn > pageLsn) { // 待读取页不在写缓存中，在读缓存中
		RWUNLOCK(&m_writeLock, Shared);
		return false;
	}
	LsnType maxPageLsn = lsnFloor(m_writeNext);
	if (pageLsn < maxPageLsn 
		|| (pageLsn == maxPageLsn && (m_writeNext - maxPageLsn) > LOG_PAGE_HDR_SIZE)) { // 该页存在
		*page = lsnToPageAddr(pageLsn);
		assert((*page)->m_startLsn == pageLsn);
	} else { // 该页不存在
		RWUNLOCK(&m_writeLock, Shared);
		*page = NULL;
	}
	return true;
}

void LogWriteBuffer::releasePage(LogPageHdr *page) {
	UNREFERENCED_PARAMETER(page);
	assert(m_writeLock.isLocked(Shared));
	assert(page->m_startLsn <= m_writeNext);
	assert(m_minPageLsn <= page->m_startLsn);
	RWUNLOCK(&m_writeLock, Shared);
}

/**
 * 读取日志页
 * @param lsn 待读取的日志页LSN
 * @param buf 缓存空间,保证能够容纳pageCnt个日志页
 * @param pageCnt in:最多读取的日志页数/out:实际读取的页数
 * @param finishCurPage 是否要结束当前日志页
 * @return false日志页在读缓存中，否则true
 */
bool LogWriteBuffer::readPages(LsnType lsn, byte *buf, uint *pageCnt, bool finishCurPage) {
	assert(lsn == lsnFloor(lsn));
	LockMode mode = Shared;
	RWLOCK(&m_writeLock, mode);
	if (m_minPageLsn > lsn ) { // 待读取页不在写缓存中，在读缓存中
		RWUNLOCK(&m_writeLock, mode);
		return false;
	}
	assert(lsn <= m_writeNext);
	LsnType curPageLsn = lsnFloor(m_writeNext);
	uint cnt = 0;
	while (cnt < *pageCnt) {
		if (lsn == curPageLsn) { // 已经读到日志的当前页
			if (!finishCurPage) // 不需要读取当前页面
				break;
			if (m_writeNext - curPageLsn <= LOG_PAGE_HDR_SIZE) { // 当前页是一个新页
				// 此时调用者保证不会有写日志线程，m_writeNext == curPageLsn
				assert(m_writeNext == curPageLsn);
				break;
			} else { // 结束当前页
				RWUNLOCK(&m_writeLock, mode);
				mode = Exclusived;
				RWLOCK(&m_writeLock, mode);
				m_writeNext = lsnCeiling(m_writeNext);
				curPageLsn = m_writeNext;
				continue;
			}
		}
		LogPageHdr *pageHdr = lsnToPageAddr(lsn);
		assert(pageHdr->m_startLsn == lsn);
		memcpy(buf + cnt * LOG_PAGE_SIZE, pageHdr, LOG_PAGE_SIZE);
		lsn += LOG_PAGE_SIZE;
		++cnt;
	}
	RWUNLOCK(&m_writeLock, mode);
	LogFile::checksumPages((LogPageHdr *)buf, cnt);
	*pageCnt = cnt;
	return true;
}
/** 获取最小LSN */
LsnType LogWriteBuffer::getMinLsn() {
	RWLOCK(&m_writeLock, Shared);
	LsnType minLsn = m_minPageLsn;
	RWUNLOCK(&m_writeLock, Shared);
	return minLsn;
}
/** 获取最后一条日志记录末尾的LSN */
LsnType LogWriteBuffer::getWriteNextLsn() {
	LsnType lsn;
	RWLOCK(&m_writeLock, Shared);
	lsn = m_writeNext;
	RWUNLOCK(&m_writeLock, Shared);
	return lsn;
}
/** 获取最后一条日志记录的LSN */
LsnType LogWriteBuffer::getLastWrittenLsn() {
	LsnType lsn;
	RWLOCK(&m_flushLock, Shared);
	lsn = m_writeLast;
	RWUNLOCK(&m_flushLock, Shared);
	return lsn;
}
/** 最后一条已回写日志记录的LSN */
LsnType LogWriteBuffer::getLastFlushedLsn() {
	LsnType lsn;
	RWLOCK(&m_flushLock, Shared);
	lsn = m_flushLast;
	RWUNLOCK(&m_flushLock, Shared);
	return lsn;
}
/** 设置m_flushLast */
void LogWriteBuffer::setLastFlushedLsn(LsnType lastFlushed) {
	RWLOCK(&m_flushLock, Exclusived);
	m_flushLast = lastFlushed;
	RWUNLOCK(&m_flushLock, Exclusived);
}
/**
 * 写入一条简单日志项（不跨页）
 * @param logEntry 日志项
 * @param lsn 该日志项的lsn
 * @param recordType 日志记录类型
 * @return 日志尾LSN
 */
LsnType LogWriteBuffer::writeSimpleRecord(const LogEntry* logEntry, LsnType lsn, LogRecordType recordType) {
	LogPageHdr *pageHdr = lsnToPageAddr(lsn);
	LsnType pageLsn = lsnFloor(lsn);
	size_t offset;
	if (lsnToOffInPage(lsn) <= LOG_PAGE_HDR_SIZE) { // 需要初始化新页面
		memset(pageHdr, 0, LOG_PAGE_SIZE);
		pageHdr->m_startLsn = pageLsn;
		offset = pageHdr->m_used = (u16)LOG_PAGE_HDR_SIZE;
	} else {
		assert(pageHdr->m_startLsn == pageLsn);
		assert(lsn - pageLsn == pageHdr->m_used);
		assert(LOG_PAGE_SIZE >= logEntry->m_size + LOG_RECORD_HDR_SIZE + pageHdr->m_used);
		offset = pageHdr->m_used;
		assert(offset == alignRound(offset));
	}
	LogRecordHdr *recHdr = (LogRecordHdr *)((byte *)pageHdr + offset);
	recHdr->m_logType = (u8)logEntry->m_logType;
	recHdr->m_size = (u16)logEntry->m_size;
	recHdr->m_txnId = logEntry->m_txnId;
	recHdr->m_tableId = logEntry->m_tableId;
	recHdr->m_type = (u8)recordType;
#ifdef TXNLOG_DEBUG
	recHdr->m_lsn = lsn;
	recHdr->m_magic = LogRecordHdr::MAGIC;
#endif
	memcpy((byte *)recHdr + LOG_RECORD_HDR_SIZE, logEntry->m_data, logEntry->m_size);
#ifdef TXNLOG_DEBUG
	recHdr->m_checksum = checksum64((byte *)recHdr + 8, LOG_RECORD_HDR_SIZE + logEntry->m_size - 8);
#endif
	pageHdr->m_used = (u16)(offset + alignRound(LOG_RECORD_HDR_SIZE + logEntry->m_size));
	nftrace(ts.log, tout << "writeSimpleRecord " << recHdr);
	return pageHdr->m_startLsn + pageHdr->m_used;
}
/**
 * 写入一条复杂日志项（跨页）
 * @param logEntry 日志项
 * @param lsn 该日志项的lsn
 * @return 日志尾LSN
 */
LsnType LogWriteBuffer::writeMultiPageRecord(const LogEntry *logEntry, LsnType lsn) {
	LsnType tailLsn = INVALID_LSN;
	LogEntry le = *logEntry;
	LsnType pageLsn = lsnFloor(lsn);
	size_t size = (size_t)(pageLsn  + LOG_PAGE_SIZE - lsn - LOG_RECORD_HDR_SIZE);
	size_t remain = le.m_size;
	assert(remain > size);
	le.m_size = size;
	tailLsn = writeSimpleRecord(&le, lsn, LR_HEAD);
	remain -= size;

	while(remain > 0) {
		pageLsn += LOG_PAGE_SIZE;
		lsn = pageLsn + LOG_PAGE_HDR_SIZE;
		le.m_data += size;
		size = min(remain, PAGE_MAX_PAYLOAD_SIZE);
		le.m_size = size;
		remain -= size;
		tailLsn = writeSimpleRecord(&le, lsn, remain == 0 ? LR_TAIL : LR_CONTINUE);
	};
	return tailLsn;
}
/** 设置高水线 */
void LogWriteBuffer::setHighWatermark(double usageRatio) {
	m_bufHwm = usageRatio;
}
/**
 * 如果当前写入页是待刷写页，提前结束当前页
 * 此举是为了避免页面读写冲突
 * @param flushTarget 待回写日志记录的LSN
 * @return flush日志记录的末尾LSN
 */
LsnType LogWriteBuffer::prepareFlush(LsnType flushTarget) {
	RWLOCK(&m_writeLock, Shared);
	LsnType tailLsn = getTailLsn(flushTarget);
	if (tailLsn == INVALID_LSN) { // 这条日志已经不在缓存中
		RWUNLOCK(&m_writeLock, Shared);
		return INVALID_LSN;
	}
	LsnType tailPageLsn = lsnFloor(tailLsn);
	LsnType writeNext = m_writeNext;
	LsnType curPage = lsnFloor(writeNext);
	// TODO: 日志回写线程会回写半满页，因此这里无需结束当前半满页面
	RWUNLOCK(&m_writeLock, Shared);
	if (curPage == tailPageLsn)  { // 当前页面是待刷写页
		RWLOCK(&m_writeLock, Exclusived);
		if (lsnFloor(m_writeNext) == tailPageLsn) { // 当前页面是待刷写页
			if (lsnToOffInPage(m_writeNext) > LOG_PAGE_HDR_SIZE) // 当前页面非空，提前结束当前页面
				m_writeNext = lsnCeiling(m_writeNext);
		}
		RWUNLOCK(&m_writeLock, Exclusived);
	}
	return tailLsn;
}
/**
 * 得到某日志记录末尾的LSN
 * @pre 写缓存共享锁
 * @param lsn 目标日志记录的起始lsn
 * @return 该日志记录的末尾lsn
 */
LsnType LogWriteBuffer::getTailLsn(LsnType lsn) {
	assert(m_writeLock.isLocked(Shared));
	if (lsn <= m_minPageLsn || lsn > m_writeLast)
		return INVALID_LSN; // 缓存中的日志记录在[m_minLsn, m_writeLast]之间
	LsnType tail;
	LogRecordHdr* recHdr = lsnToAddr(lsn);
	assert(isLogRecValid(recHdr));
	if (recHdr->m_type == LR_NORMAL) {
		tail = lsn + LOG_RECORD_HDR_SIZE + recHdr->m_size;
	} else if (recHdr->m_type == LR_HEAD) {
		do {
			lsn = lsnCeiling(lsn) + LOG_PAGE_HDR_SIZE;
			recHdr = lsnToAddr(lsn);
			assert(isLogRecValid(recHdr));
			assert(recHdr->m_type == LR_CONTINUE || recHdr->m_type == LR_TAIL);
		} while (recHdr->m_type != LR_TAIL);
		tail = lsn + LOG_RECORD_HDR_SIZE + recHdr->m_size;
	} else {
		assert(false);
		tail = INVALID_LSN;
	}
	assert(m_writeNext >= tail);
	return tail;
}

/**
 * 根据日志项长度，预计算记录尾部的LSN
 * @param recordSize 记录大小,不包括记录头
 * @param lsnTail 输出参数，末尾lsn
 * @return 记录需要的页数
 */
size_t LogWriteBuffer::calRecordTailLsn(size_t recordSize, LsnType *lsnTail) {
	size_t pageUsed = lsnToOffInPage(m_writeNext);
	size_t trueSize = recordSize + LOG_RECORD_HDR_SIZE; // 日志长度，包括头部
	if (pageUsed + trueSize <= LOG_PAGE_SIZE) { // 当前页面能放下本记录
		*lsnTail = alignRound(m_writeNext + trueSize);
		return 0;
	}
	size_t remainSize = trueSize - (LOG_PAGE_SIZE - pageUsed); // 占完当前页，还剩余多少字节
	size_t pageCnt = (remainSize + PAGE_MAX_PAYLOAD_SIZE - 1) / PAGE_MAX_PAYLOAD_SIZE;
	// 计算尾LSN的时候应该补上每页浪费的空间
	*lsnTail = m_writeNext + trueSize + pageCnt * (LOG_PAGE_SIZE - PAGE_MAX_PAYLOAD_SIZE);
	*lsnTail = alignRound(*lsnTail);
	return pageCnt;
}
/**
 * 获取LSN对应的日志页内存
 * @param lsn 保护在日志页中的任何LSN
 * @return 日志页
 */
LogPageHdr* LogWriteBuffer::lsnToPageAddr(LsnType lsn) {
	LsnType pageLsn = lsnFloor(lsn);
	return (LogPageHdr*)(m_buffer + pageLsn % m_bufferSize);
}
/**
 * 获取LSN对应的日志记录内存
 * @param lsn 日志记录lsn
 * @return 日志记录
 */
LogRecordHdr* LogWriteBuffer::lsnToAddr(LsnType lsn) {
#ifndef NDEBUG
	// 检查LSN是否包含在日志页中
	LogPageHdr *pageHdr = lsnToPageAddr(lsn);
	assert(lsn > pageHdr->m_startLsn && lsn < pageHdr->m_startLsn + LOG_PAGE_SIZE);
#endif
	return (LogRecordHdr *)(m_buffer + lsn % m_bufferSize);
}
//////////////////////////////////////////////////////////////////////////
/**
 * 构造一个日志读缓存
 * @param pool 内存页池
 * @param logFileMngr 日志文件管理器
 * @param pageCnt 缓存页数
 * @param ioPages 一次IO的页数
 */
LogReadBuffer::LogReadBuffer(PagePool *pool, LogFileMngr *logFileMngr, uint pageCnt, uint ioPages)
	: PagePoolUser(pageCnt, pool), m_ioPages(ioPages)
	, m_lock("LogReadBuffer::lock", __FILE__, __LINE__), m_logFileMngr(logFileMngr), m_pool(pool) {
	assert(m_ioPages < pageCnt);
}

LogReadBuffer::~LogReadBuffer() {
	m_pool->preDelete();
	delete m_pool;
}

/** PagePool的回调 */
uint LogReadBuffer::freeSomePages(u16 userId, uint numPages) {
	UNREFERENCED_PARAMETER(userId);
	UNREFERENCED_PARAMETER(numPages);
	assert(false); // 我这个页池是专用的，没人会向我要页
	return 0;
}
/**
 * 获取一页，页上加读锁
 * @param pageLsn 日志页的LSN
 */
LogPageHdr* LogReadBuffer::getPage(LsnType pageLsn) {
	assert(pageLsn == lsnFloor(pageLsn));
_RESTART:
	RWLOCK(&m_lock, Shared);
	LogPageHdr *page = m_pageHash.get(pageLsn);
	if (page) { // 该页已经存在
		m_pool->lockPage(0, page, Shared, __FILE__, __LINE__);
		RWUNLOCK(&m_lock, Shared);
	} else {
		RWUNLOCK(&m_lock, Shared);
		if (loadPages(pageLsn, m_ioPages))
			goto _RESTART;
		else // 读取页失败
			return 0;
	}
	touchPage(page);
	return page;
}
/** 释放一页 */
void LogReadBuffer::releasePage(LogPageHdr *pageHdr) {
	m_pool->unlockPage(0, pageHdr, Shared);
}
/** 获取一个空闲页 */
LogReadBuffer::PageInfo* LogReadBuffer::getFreePage() {
	assert(m_lock.isLocked(Exclusived));
	void *data = allocPage(0, PAGE_TXNLOG, 0);
	if (!data) { // 在LRU队列中替换
		DLink<PageInfo *> *oldest = m_lruList.getHeader()->getNext();
		PageInfo* info = NULL;
		for (; oldest != m_lruList.getHeader(); oldest = oldest->getNext()) {
			info = oldest->get();
			//如果该page未被latch
			if (m_pool->getPageLockMode(info->m_page) == None) {
				m_pool->lockPage(0, info->m_page, Exclusived, __FILE__, __LINE__);
				//m_lruList.removeFirst();
				oldest->unLink();
				m_pageHash.remove(info->m_page->m_startLsn);
				m_pool->unlockPage(0, info->m_page, Exclusived);
				break;
			}
			info = NULL;
		}
		//有可能没有可替换的pageInfo
		return info;
	} else {
		m_pool->unlockPage(0, data, Exclusived);
		size_t entry = m_pageInfoPool.alloc();
		PageInfo* info = &m_pageInfoPool[entry];
		info->m_poolEntry = entry;
		info->m_page = (LogPageHdr *)data;
		info->m_dlink.set(info);
		m_pool->setInfoAndType(data, info, PAGE_TXNLOG);
		return info;
	}
}
/**
 * 从缓存中删除.=lsn的所有日志页
 */
void LogReadBuffer::truncate(LsnType lsn) {
	assert(lsn == lsnFloor(lsn));

	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	DLink<PageInfo *> *first = m_lruList.getHeader()->getNext();
	DLink<PageInfo *> *cur = first;
	while (cur != m_lruList.getHeader()) {
		DLink<PageInfo *> *next = cur->getNext();
		LsnType pageLsn = cur->get()->m_page->m_startLsn;
		if (pageLsn >= lsn) // 找到一个待删除页
			unloadPage(pageLsn);
		cur = next;
	}
}
/**
 * 从读缓存删除一页
 * @param pageLsn 待删除页
 */
void LogReadBuffer::unloadPage(LsnType pageLsn) {
	assert(pageLsn == lsnFloor(pageLsn));
	assert(m_lock.isLocked(Exclusived));

	LogPageHdr *page = m_pageHash.remove(pageLsn);
	if (page) {
		m_pool->lockPage(0, page, Exclusived, __FILE__, __LINE__);
		PageInfo* info = (PageInfo *)m_pool->getInfo(page);
		info->m_dlink.unLink(); // LRU链表
		freePage(0, page); // 释放页
		m_pageInfoPool.free(info->m_poolEntry); // 释放PageInfo
	}
	assert(!m_pageHash.get(pageLsn));
}
/**
 * 从磁盘上读取日志页
 * @return 如果读到日志末尾则返回false, 否则返回true
 */
bool LogReadBuffer::loadPages(LsnType lsn, uint pageCnt) {
	//assert(!m_lock.isLocked(Shared) && !m_lock.isLocked(Exclusived));
	byte *buf = (byte *)System::virtualAlloc(LOG_PAGE_SIZE * m_ioPages);
	assert_always(buf);
	pageCnt = (uint)m_logFileMngr->readPages(lsn, buf, m_ioPages);
_ReStart:
	if (pageCnt) {
		RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
		for (uint i = 0; i < pageCnt; ++i) {
			LogPageHdr *curPage = (LogPageHdr *)(buf + i * LOG_PAGE_SIZE);
			if (curPage->m_startLsn == lsn + i * LOG_PAGE_SIZE) { // 一个正常的页面
				PageInfo *pageInfo = getFreePage();
				if (!pageInfo) {
					goto _ReStart;
				}
				memcpy(pageInfo->m_page, curPage, LOG_PAGE_SIZE);
				if (!m_pageHash.get(pageInfo->m_page->m_startLsn)) // 页面不存在
					m_pageHash.put(pageInfo->m_page);
				m_lruList.addLast(&pageInfo->m_dlink);
			} else { // 当前页是无效页面
				if (i == 0) // 所有页都未使用
					goto _FAILURE;
				break;
			}
		}
		System::virtualFree(buf);
		return true;
	}
_FAILURE:
	System::virtualFree(buf);
	return false;
}
/** 调整LRU链表 */
void LogReadBuffer::touchPage(LogPageHdr *page) {
	//assert(!m_lock.isLocked(Shared) && !m_lock.isLocked(Exclusived));
	RWLOCK(&m_lock, Exclusived);
	PageInfo* info = (PageInfo *)m_pool->getInfo(page);
	m_lruList.moveToLast(&info->m_dlink);
	RWUNLOCK(&m_lock, Exclusived);
}

//////////////////////////////////////////////////////////////////////////
//// 日志缓存
//////////////////////////////////////////////////////////////////////////

LogBuffer::LogBuffer(LogReadBuffer *readBuffer, LogWriteBuffer *writeBuffer)
	: m_readBuffer(readBuffer), m_writeBuffer(writeBuffer) {
	m_minLsnWriteBuf = m_writeBuffer->getMinLsn();
}

/**
 * 获取日志页, 页加读锁
 * @param 页LSN
 * @return 页地址
 */
LogPageHdr* LogBuffer::getPage(LsnType pageLsn) {
	while(true) {
		if (pageLsn < m_minLsnWriteBuf) { // 该页肯定不在写缓存中
			return m_readBuffer->getPage(pageLsn);
		} else {
			LogPageHdr *page;
			if (m_writeBuffer->getPage(pageLsn, &page)) {
				return page; // 改页确实在写缓存中
			} else {
				m_minLsnWriteBuf = m_writeBuffer->getMinLsn();
			}
		}
	}

}

/** 释放页面、及其读锁 */
void LogBuffer::releasePage(LogPageHdr *pageHdr) {
	if (pageHdr->m_startLsn < m_minLsnWriteBuf) {
		m_readBuffer->releasePage(pageHdr);
	} else {
		m_writeBuffer->releasePage(pageHdr);
	}
}

//////////////////////////////////////////////////////////////////////////
//// 日志阅读器
//////////////////////////////////////////////////////////////////////////
/**
 * 构造日志阅读器
 * @param readBuffer 日志读缓存
 * @param log 系统日志
 */
LogReader::LogReader(LogBuffer *readBuffer, Syslog *log)
	: m_readBuffer(readBuffer), m_log(log) {
}
LogReader::~LogReader() {

}

/**
 * 开始扫描，获取扫描句柄
 * @param startLsn 扫描起始LSN, startLsn必须指向某条日志记录的头
 * @param endLsn 扫描结束LSN
 * @return 扫描句柄
 */
LogScanHandle* LogReader::beginScan(LsnType startLsn, LsnType endLsn) const {
	startLsn = max(startLsn, Txnlog::MIN_LSN);
	assert(startLsn <= endLsn);
	LogScanHandle *handle = new LogScanHandle();
	handle->m_endLsn = endLsn;
	handle->m_lsn = startLsn;
	handle->m_lsnNext = startLsn;
	memset(&handle->m_logEntry, 0, sizeof(handle->m_logEntry));
	handle->m_logEntry.m_data = handle->m_buf;

	return handle;
}
/**
 * 得到下一条待读取日志记录的起始LSN
 * @param nextLsn 上次读取日志记录的末尾lsn
 * @return 下一条日志记录的起始LSN
 */
LsnType LogReader::prepareLsnForRead(LsnType nextLsn) {
	// alignRound可能会令nextLsn指向下一页，因此先进行对齐操作，然后判断是否新页中
	nextLsn = alignRound(nextLsn);
	LsnType pageLsn = lsnFloor(nextLsn);
	if (nextLsn < pageLsn + LOG_PAGE_HDR_SIZE) { // 记录在一个新页中
		assert(nextLsn == pageLsn);
		nextLsn = pageLsn + LOG_PAGE_HDR_SIZE;
	}
	assert(nextLsn == alignRound(nextLsn));
	return nextLsn;
}
/**
 * 获取下一条日志项，存放到扫描句柄
 * @param handle 扫描句柄
 * @return 如果扫描到日志末尾，返回NULL
 */
LogScanHandle* LogReader::getNext(LogScanHandle *handle) const {
	handle->m_logEntry.m_data = handle->m_buf;
	LsnType toRead;
_RESTART:
	toRead = prepareLsnForRead(handle->m_lsnNext);
	if (toRead >= handle->m_endLsn) // 已经读到末尾了
		return 0;
	LogPageHdr *pageHdr;
	LsnType pageLsn = lsnFloor(toRead);
	pageHdr = m_readBuffer->getPage(pageLsn);
	if (!pageHdr)
		return 0; // 已经读到日志末尾？
	if (pageHdr->m_startLsn + pageHdr->m_used <= toRead) { // 已经读完当前页
		assert(pageHdr->m_startLsn + pageHdr->m_used == toRead);
		handle->m_lsnNext = lsnCeiling(toRead);
		m_readBuffer->releasePage(pageHdr);
		goto _RESTART;
	}
	size_t offset = (size_t)(toRead - pageHdr->m_startLsn);
	LogRecordHdr *recHdr = (LogRecordHdr *)((byte *)pageHdr + offset);
	// 判断日志记录是否正确
	assert(isLogRecValid(recHdr, pageHdr));
#ifdef TXNLOG_DEBUG
	NTSE_ASSERT(toRead == recHdr->m_lsn);
	NTSE_ASSERT(recHdr->m_checksum == checksum64((byte *)recHdr + 8, LOG_RECORD_HDR_SIZE + recHdr->m_size - 8));
#endif
	handle->m_lsn = toRead;
	setLogEntryHdr(&handle->m_logEntry, recHdr);
	memcpy(handle->m_logEntry.m_data, (byte *)recHdr + LOG_RECORD_HDR_SIZE, recHdr->m_size);
	m_readBuffer->releasePage(pageHdr);
	assert(recHdr->m_type == LR_HEAD || recHdr->m_type == LR_NORMAL);
	if (recHdr->m_type == LR_HEAD) { // 长日志
		assert(offset + LOG_RECORD_HDR_SIZE + recHdr->m_size == LOG_PAGE_SIZE); // HEAD记录必定充满日志页
		// 读取日志剩余部分
		size_t logSize = recHdr->m_size;
		while (recHdr->m_type != LR_TAIL) {
			LsnType nextPageLsn = lsnCeiling(toRead);
			pageHdr = m_readBuffer->getPage(nextPageLsn);
			NTSE_ASSERT(pageHdr);
			toRead = prepareLsnForRead(nextPageLsn);
			offset = (size_t)(toRead - pageHdr->m_startLsn);
			recHdr = (LogRecordHdr *)((byte *)pageHdr + offset);
			assert(isLogRecValid(recHdr, pageHdr));
			assert(recHdr->m_type == LR_CONTINUE || recHdr->m_type == LR_TAIL);
			assert (isLogRecMetaEq(&handle->m_logEntry, recHdr));
			memcpy(handle->m_logEntry.m_data + logSize,
				(byte *)recHdr + LOG_RECORD_HDR_SIZE, recHdr->m_size);
			logSize += recHdr->m_size;
			m_readBuffer->releasePage(pageHdr);
			assert(logSize <= LogConfig::MAX_LOG_RECORD_SIZE);
		}
		handle->m_logEntry.m_size = logSize;
	}
	handle->m_lsnNext = toRead + recHdr->m_size + LOG_RECORD_HDR_SIZE;
	
	return handle;
}
/** 结束扫描，释放句柄内存 */
void LogReader::endScan(LogScanHandle *handle) const {
	delete handle;
}

/**
 * 得到[startPageLsn, endPageLsn) 之间的第一条非完整日志
 * @param startPageLsn 首页LSN
 * @param endPageLsn 末页尾LSN
 * @return 小于LSN，且不完整的日志项LSN；
 *	如果不存在不完整的日志项，则返回INVALID_LSN
 */
LsnType LogReader::getIncompleteLogEntryLSNBetween(LogReadBuffer *readBuffer
												, LsnType startPageLsn, LsnType endPageLsn) {
	assert(startPageLsn == lsnFloor(startPageLsn));
	assert(endPageLsn == lsnFloor(endPageLsn));
	LsnType incompletePos = INVALID_LSN;
	LsnType pageLsn = endPageLsn - LOG_PAGE_SIZE;
	bool haveIncompelete = false;
	while (pageLsn >= startPageLsn) {
		LogPageHdr *pageHdr = readBuffer->getPage(pageLsn);
		NTSE_ASSERT(pageHdr);
		LsnType toRead = prepareLsnForRead(pageLsn);
		bool checkPrevPage = true; // 是否需要检查前页
		while (pageHdr->m_startLsn + pageHdr->m_used > toRead) {
			toRead = prepareLsnForRead(toRead);
			size_t offset = (size_t)(toRead - pageHdr->m_startLsn);
			LogRecordHdr *recHdr = (LogRecordHdr *)((byte *)pageHdr + offset);
			assert(isLogRecValid(recHdr, pageHdr));
			if (recHdr->m_type == LR_HEAD) {
				incompletePos = toRead; // 找到不完整日志项
				// HEAD记录必定充满日志页
				assert(offset + LOG_RECORD_HDR_SIZE + recHdr->m_size == LOG_PAGE_SIZE);
				break;
			} else if (recHdr->m_type == LR_TAIL || recHdr->m_type == LR_NORMAL) {
				// 不完整日志记录只能在本页
				checkPrevPage = false;
			} else if (recHdr->m_type == LR_CONTINUE) {
				// 必定是第一条日志记录
				assert(toRead == prepareLsnForRead(pageLsn));
				// CONTINUE记录必定充满日志页
				assert(offset + LOG_RECORD_HDR_SIZE + recHdr->m_size == LOG_PAGE_SIZE);
				// 必定存在不完整日志项，且必定在当前页面之前
				haveIncompelete = true;
			}
			toRead += recHdr->m_size  + LOG_RECORD_HDR_SIZE;
			toRead = prepareLsnForRead(toRead);
		}
		readBuffer->releasePage(pageHdr);
		if (!checkPrevPage)
			break;
		pageLsn -= LOG_PAGE_SIZE;
	}
	assert(!(haveIncompelete && incompletePos == INVALID_LSN));
	return incompletePos;
}


//////////////////////////////////////////////////////////////////////////
/**  当前日志项的起始LSN */
LsnType LogScanHandle::curLsn() const {
	return m_lsn;
}
/** 当前句柄中包含的日志项 */
const LogEntry* LogScanHandle::logEntry() const {
	return &m_logEntry;
}
/** 下一条日志的起始LSN */
LsnType LogScanHandle::nextLsn() const {
	return m_lsnNext;
}
//////////////////////////////////////////////////////////////////////////
const double LogConfig::DEFAULT_BUFFER_HIGH_WATERMARK = 0.4;
/** 日志写缓存至少能放下一条日志 */
const size_t LogConfig::MIN_LOG_BUFFER_SIZE = 
	(computeMinlogBufferSize(LogConfig::MAX_LOG_RECORD_SIZE) + (1<<20) - 1) & ~(size_t)((1<<20) - 1);
/** 日志文件至少能放下一条日志 */
const size_t LogConfig::MIN_LOGFILE_SIZE = 
	(computeMinlogFileSize(LogConfig::MAX_LOG_RECORD_SIZE) + (1<<20) - 1) & ~(size_t)((1<<20) - 1);

/** 构造默认日志配置参数 */
LogConfig::LogConfig() {
#ifdef NTSE_UNIT_TEST
	m_readBufPageCnt = 4;
	m_readBufIoPages = 2;
	m_tailHintAcc = 3;
	m_flushInterval = DEFAULT_FLUSH_INTERVAL;
#else
	m_readBufPageCnt = DEFAULT_READ_BUFFER_PAGES;
	m_readBufIoPages = DEFAULT_READ_BUFFER_IO_PAGES;
	m_tailHintAcc = DEFAULT_LOG_TAILHINT_ACCURACY;
	m_flushInterval = DEFAULT_FLUSH_INTERVAL;
#endif // NTSE_UNIT_TEST
}
/** 检查配置参数正确性 */
bool LogConfig::checkConfig() const {
	if (m_readBufIoPages >= m_readBufPageCnt)
		return false;
	return true;
}
//////////////////////////////////////////////////////////////////////////

const LsnType Txnlog::MIN_LSN = lsnCeiling(LOG_CTRL_PAGE_SIZE); /** 最小的LSN */

/** 构造函数 */
Txnlog::Txnlog() {
	init();
}
/** 初始化 */
void Txnlog::init()  {
	m_flusher = 0;
	m_writeBuffer = 0;
	m_fileMngr = 0;
	m_readBuffer = 0;
	m_buffer = 0;
	m_logReader = 0;
	memset(&m_stat, 0, sizeof(m_stat));
}
/** 析构函数，啥也不干 */
Txnlog::~Txnlog() {
}

#ifdef TNT_ENGINE
bool Txnlog::init(TNTDatabase *db, const char *dir, const char *name, uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg) {
	if (cfg)
		this->m_cfg = *cfg;
	if (!this->m_cfg.checkConfig()) {
		assert(false);
		return false;
	}
	this->m_tntDb = db;
	this->m_db = db->getNtseDb();
	this->m_syslog = m_tntDb->getTNTSyslog();
	this->m_baseName = new char[strlen(dir) + strlen(name) + 2];
	strcpy(this->m_baseName, dir);
	strcat(this->m_baseName, NTSE_PATH_SEP);
	strcat(this->m_baseName, name);
	this->m_numLogFile = numLogFile;
	this->m_fileSize = LOG_CTRL_PAGE_SIZE + (uint)lsnCeiling(fileSize - LOG_CTRL_PAGE_SIZE);
	this->m_writeBufPageCnt = (bufferSize + LOG_PAGE_SIZE - 1) / LOG_PAGE_SIZE;
	return true;
}
#endif
bool Txnlog::init(Database *db, const char *dir, const char *name, uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg) {
	if (cfg)
		this->m_cfg = *cfg;
	if (!this->m_cfg.checkConfig()) {
		assert(false);
		return false;
	}
	this->m_db = db;
#ifdef TNT_ENGINE
	this->m_tntDb = NULL;
#endif
	this->m_syslog = m_db->getSyslog();
	this->m_baseName = new char[strlen(dir) + strlen(name) + 2];
	strcpy(this->m_baseName, dir);
	strcat(this->m_baseName, NTSE_PATH_SEP);
	strcat(this->m_baseName, name);
	this->m_numLogFile = numLogFile;
	this->m_fileSize = LOG_CTRL_PAGE_SIZE + (uint)lsnCeiling(fileSize - LOG_CTRL_PAGE_SIZE);
	this->m_writeBufPageCnt = (bufferSize + LOG_PAGE_SIZE - 1) / LOG_PAGE_SIZE;
	return true;
}
/**
* 初始化日志模块。
*
* @param db 数据库
* @param dir 日志文件所在目录
* @param name 日志文件名
* @param numLogFile 日志文件数目, 日志文件名为dir/name.0001, dir/name.0002, dir/name.numLogFile
* @param fileSize 日志文件大小
* @param bufferSize 日志写缓存大小
* @param cfg 日志配置
* @return 初始化完成的事务日志管理器
*/
Txnlog* Txnlog::open(Database *db, const char *dir, const char *name,
					uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg) throw (NtseException) {
	Txnlog* log = new Txnlog();
	if (!log->init(db, dir, name, numLogFile, fileSize, bufferSize, cfg)) {
		return NULL;
	}
	try {
		log->startUp();
	} catch(NtseException &e) {
		log->close();
		throw e;
	}
	return log;
}
#ifdef TNT_ENGINE
Txnlog* Txnlog::open(TNTDatabase *db, const char *dir, const char *name,
					uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg) throw (NtseException) {
	Txnlog* log = new Txnlog();
	if (!log->init(db, dir, name, numLogFile, fileSize, bufferSize, cfg)) {
		return NULL;
	}
	try {
		log->startUp();
	} catch(NtseException &e) {
		log->close();
		throw e;
	}
	return log;
}
#endif

/**
 * 清除日志末尾不完整的日志项
 * @param tailLsn 日志末尾
 * @return 去除不完整日志项之后的日志末尾
 */
LsnType Txnlog::removeIncompleteLogEntry(LsnType tailLsn) {
	assert(tailLsn == lsnFloor(tailLsn));
	LsnType beginLsn = INVALID_LSN;
#ifdef TNT_ENGINE
	if (m_tntDb != NULL) {
		beginLsn = m_tntDb->getTNTControlFile()->getDumpLsn();
		beginLsn = min(beginLsn, m_db->getControlFile()->getCheckpointLSN());
	} else {
#endif
		assert(m_db != NULL);
		beginLsn = m_db->getControlFile()->getCheckpointLSN();
#ifdef TNT_ENGINE
	}
#endif
	LsnType beginPageLsn = lsnFloor(beginLsn);
	assert(tailLsn >= beginPageLsn);
	if (tailLsn == beginPageLsn || tailLsn == MIN_LSN)
		return tailLsn;

	LsnType realTailLsn = LogReader::getIncompleteLogEntryLSNBetween(
							m_readBuffer, max(beginPageLsn, MIN_LSN), tailLsn);
	if (realTailLsn == INVALID_LSN) // 没有不完整的日志项
		return tailLsn;
	assert(realTailLsn >= beginLsn);
	LogFile *logFile = m_fileMngr->findLogFile(realTailLsn);
	assert(logFile);
	m_syslog->log(EL_LOG, "Remove incomplete log entry at "
		I64FORMAT"u, with log tailLSN "I64FORMAT"u"
		, realTailLsn, tailLsn);

	logFile->truncate(realTailLsn, tailLsn);
	// truncate直接更改了磁盘上的页面，还需清空读缓存
	m_readBuffer->truncate(lsnFloor(realTailLsn));
	return lsnCeiling(realTailLsn);
}

/** 启动日志模块 */
void Txnlog::startUp() throw (NtseException) {
	m_fileMngr = LogFileMngr::open(this, m_baseName, m_numLogFile);
	reclaimOverflowSpace();
	PagePool *pool = new PagePool(0, LOG_PAGE_SIZE);
	m_readBuffer = new LogReadBuffer(
		pool, m_fileMngr, m_cfg.m_readBufPageCnt, m_cfg.m_readBufIoPages);
	pool->registerUser(m_readBuffer);
	pool->init();
	
	// 获取日志尾巴
	LsnType tailLsn = findLogTail();
	if (tailLsn == INVALID_LSN) { // 创建数据库后，第一次启动
		tailLsn = m_fileMngr->initOnFirstOpen();
	} else {
		assert(tailLsn == lsnFloor(tailLsn));
		if (m_fileMngr->findLogFile(tailLsn)) {
			// tailLsn不在日志文件末尾
			m_fileMngr->setCurLogFile(tailLsn);
			m_fileMngr->curLogFile()->reset(tailLsn);
		} else {
			// 当tailLsn在日志文件末尾的时候，tailLsn对应的日志记录实际上已经
			// 在一个新的日志文件中，当然该日志文件根本未被创建，是不存在的!
			// 为了避免以上问题，故而设置包含(tailLsn - 1)的文件为当前文件
			m_fileMngr->setCurLogFile(tailLsn - 1);
		}
	}
	if (m_fileMngr->getLogFileSize() != m_fileSize) {
		NTSE_THROW(NTSE_EC_CORRUPTED_LOGFILE, "Open transaction log error: current log file size is "I64FORMAT"u, but ntse_log_file_size is set "I64FORMAT"u",
			m_fileMngr->getLogFileSize(), (u64)m_fileSize);
	}
	// 去除日志末尾不完整的日志项
	// 因为本方法会改变tailLsn，因此必须在reset之后调用本方法
	// 否则可能导致reset清零的日志页面不到maxIoPages
	tailLsn = removeIncompleteLogEntry(tailLsn);
	assert(tailLsn == lsnFloor(tailLsn));
#ifdef TNT_ENGINE
	if (NULL != m_tntDb) {
		m_syslog->log(EL_LOG, "Open transaction log: dump LSN "I64FORMAT"u, checkpoint LSN "I64FORMAT"u, tailLSN "I64FORMAT"u"
			, m_tntDb->getTNTControlFile()->getDumpLsn(), m_db->getControlFile()->getCheckpointLSN(), tailLsn);
	} else {
#endif
		assert(NULL != m_db);
		m_syslog->log(EL_LOG, "Open transaction log: checkpoint LSN "I64FORMAT"u, tailLSN "I64FORMAT"u"
		, m_db->getControlFile()->getCheckpointLSN(), tailLsn);
#ifdef TNT_ENGINE
	}
#endif
	
	// 初始化写缓存
	m_writeBuffer = new LogWriteBuffer(this, tailLsn, m_writeBufPageCnt);
	m_writeBuffer->setHighWatermark(LogConfig::DEFAULT_BUFFER_HIGH_WATERMARK);
	m_buffer = new LogBuffer(m_readBuffer, m_writeBuffer);
	m_logReader = new LogReader(m_buffer, m_syslog);
	// 初始化日志刷新线程
	m_flusher = new LogFlusher(this, m_cfg.m_flushInterval);
	m_flusher->start();
}

/**
 * 得到指定日志类型的字符串表示
 *
 * @param logType 日志类型
 * @return 表示日志类型的字符串常量
 */
const char* Txnlog::getLogTypeStr(LogType logType) {
	switch (logType) {
	case LOG_CREATE_TABLE:
		return "LOG_CREATE_TABLE";
	case LOG_DROP_TABLE:
		return "LOG_DROP_TABLE";
	case LOG_TRUNCATE:
		return "LOG_TRUNCATE";
	case LOG_RENAME_TABLE:
		return "LOG_RENAME_TABLE";
	case LOG_BUMP_FLUSHLSN:
		return "LOG_BUMP_FLUSHLSN";
	case LOG_ADD_INDEX:
		return "LOG_ADD_INDEX";
	case LOG_HEAP_INSERT:
		return "LOG_HEAP_INSERT";
	case LOG_HEAP_UPDATE:
		return "LOG_HEAP_UPDATE";
	case LOG_HEAP_DELETE:
		return "LOG_HEAP_DELETE";
	case LOG_IDX_DROP_INDEX:
		return "LOG_IDX_DROP_INDEX";
	case LOG_IDX_DML:
		return "LOG_IDX_DML";
	case LOG_IDX_SMO:
		return "LOG_IDX_SMO";
	case LOG_IDX_SET_PAGE:
		return "LOG_IDX_SET_PAGE";
	case LOG_IDX_CREATE_BEGIN:
		return "LOG_IDX_CREATE_BEGIN";
	case LOG_IDX_DMLDONE_IDXNO:
		return "LOG_IDX_DMLDONE_IDXNO";
	case LOG_IDX_DIU_DONE:
		return "LOG_IDX_DIU_DONE";
	case LOG_IDX_CREATE_END:
		return "LOG_IDX_CREATE_END";
	case LOG_IDX_DML_BEGIN:
		return "LOG_IDX_DML_BEGIN";
	case LOG_IDX_DML_END:
		return "LOG_IDX_DML_END";
	case LOG_TXN_END:
		return "LOG_TXN_END";
	case LOG_TXN_START:
		return "LOG_TXN_START";
	case LOG_MMS_UPDATE:
		return "LOG_MMS_UPDATE";
	case LOG_LOB_INSERT:
		return "LOG_LOB_INSERT";
	case LOG_LOB_UPDATE:
		return "LOG_LOB_UPDATE";
	case LOG_LOB_DELETE:
		return "LOG_LOB_DELETE";
	case LOG_LOB_MOVE:
		return "LOG_LOB_MOVE";
	case LOG_PRE_UPDATE:
		return "LOG_PRE_UPDATE";
	case LOG_PRE_UPDATE_HEAP:
		return "LOG_PRE_UPDATE_HEAP";
	case LOG_PRE_DELETE:
		return "LOG_PRE_DELETE";
	case LOG_ALTER_TABLE_ARG:
		return "LOG_ALTER_TABLE_ARG";
	case LOG_ALTER_INDICE:
		return "LOG_ALTER_INDICE";
	case LOG_ALTER_COLUMN:
		return "LOG_ALTER_COLUMN";
	case LOG_CREATE_DICTIONARY:
		return "LOG_CREATE_DICTIONARY";
	case LOG_PRE_LOB_DELETE:
		return "LOG_PRE_LOB_DELETE";
	case LOG_TNT_MIN:
		return "LOG_TNT_MIN";
	case TNT_BEGIN_TRANS:
		return "TNT_BEGIN_TRANS";
	case TNT_COMMIT_TRANS:
		return "TNT_COMMIT_TRANS";
	case TNT_BEGIN_ROLLBACK_TRANS:
		return "TNT_BEGIN_ROLLBACK_TRANS";
	case TNT_END_ROLLBACK_TRANS:
		return "TNT_END_ROLLBACK_TRANS";
	case TNT_PREPARE_TRANS:
		return "TNT_PREPARE_TRANS";
	case TNT_PARTIAL_BEGIN_ROLLBACK:
		return "TNT_PARTIAL_BEGIN_ROLLBACK";
	case TNT_PARTIAL_END_ROLLBACK:
		return "TNT_PARTIAL_END_ROLLBACK";
	case TNT_U_I_LOG:
		return "TNT_U_I_LOG";
	case TNT_U_U_LOG:
		return "TNT_U_U_LOG";
	case TNT_D_I_LOG:
		return "TNT_D_I_LOG";
	case TNT_D_U_LOG:
		return "TNT_D_U_LOG";
	case TNT_UNDO_I_LOG:
		return "TNT_UNDO_I_LOG";
	case TNT_UNDO_LOB_LOG:
		return "TNT_UNDO_LOB_LOG";
	case TNT_BEGIN_PURGE_LOG:
		return "TNT_BEGIN_PURGE_LOG";
	case TNT_PURGE_BEGIN_FIR_PHASE:
		return "TNT_PURGE_BEGIN_FIR_PHASE";
	case TNT_PURGE_BEGIN_SEC_PHASE:
		return "TNT_PURGE_BEGIN_SEC_PHASE";
	case TNT_PURGE_END_HEAP:
		return "TNT_PURGE_END_HEAP";
	case TNT_END_PURGE_LOG:
		return "TNT_END_PURGE_LOG";
	case LOG_TNT_MAX:
		return "LOG_TNT_MAX";
	case LOG_CPST_MIN:
		return "LOG_CPST_MIN";
	case LOG_IDX_DML_CPST:
		return "LOG_IDX_DML_CPST";
	case LOG_IDX_SMO_CPST:
		return "LOG_IDX_SMO_CPST";
	case LOG_IDX_SET_PAGE_CPST:
		return "LOG_IDX_SET_PAGE_CPST";
	case LOG_IDX_ADD_INDEX_CPST:
		return "LOG_IDX_ADD_INDEX_CPST";
	case LOG_CPST_MAX:
		return "LOG_CPST_MAX";
	case LOG_MAX:
		return "LOG_MAX";
	default:
		assert(false);
		return "Unknown";
	}
}

/**
 * 创建日志文件
 *
 * @param db 数据库
 * @param dir 日志文件所在目录
 * @param name 日志文件名
 * @param fileSize 日志文件大小
 * @throw NtseException 创建日志文件管理器失败
 */
#ifdef TNT_ENGINE
void Txnlog::create(const char *dir, const char *name, uint fileSize, uint numFile)  throw(NtseException) {
#else
void Txnlog::create(const char *dir, const char *name, uint fileSize)  throw(NtseException) {
#endif
	assert(fileSize > LOG_CTRL_PAGE_SIZE);
#ifdef TNT_ENGINE
	File logDir(dir);
	bool exist = true;
	u64 errCode = logDir.isExist(&exist);
	assert(errCode == File::E_NO_ERROR);
	if (!exist) 
		logDir.mkdir();
#endif
	fileSize = LOG_CTRL_PAGE_SIZE + (uint)lsnCeiling(fileSize - LOG_CTRL_PAGE_SIZE);
#ifdef TNT_ENGINE
	return LogFileMngr::create((std::string(dir) + NTSE_PATH_SEP + name).c_str(), numFile, fileSize);	
#else
	return LogFileMngr::create((std::string(dir) + NTSE_PATH_SEP + name).c_str(), LogConfig::DEFAULT_NUM_LOGS, fileSize);
#endif
}

/**
 * 重新创建日志文件
 *
 * @param db 数据库
 * @param dir 日志文件所在目录
 * @param name 日志文件名
 * @param fileSize 日志文件大小
 * @param fileCnt 日志文件个数
 * @param startLsn 日志起始LSN，一般为原有检查点LSN
 * @return 实际的日志起始LSN
 * @throw NtseException 创建日志文件管理器失败
 */
LsnType Txnlog::recreate(const char *dir, const char *name
						, uint fileCnt , uint fileSize, LsnType startLsn) throw(NtseException) {
	assert(fileSize > LOG_CTRL_PAGE_SIZE);
	fileSize = LOG_CTRL_PAGE_SIZE + (uint)lsnCeiling(fileSize - LOG_CTRL_PAGE_SIZE);
	return LogFileMngr::recreate((std::string(dir) + NTSE_PATH_SEP + name).c_str(), fileCnt, fileSize, startLsn);
}
/**
 * 删除日志文件
 * @param dir 日志文件所在目录
 * @param name 日志文件名
 * @param numLogFile 日志文件个数
 */
void Txnlog::drop(const char *dir, const char *name, uint numLogFile) throw(NtseException) {
	return LogFileMngr::drop((std::string(dir) + NTSE_PATH_SEP + name).c_str(), numLogFile);
}
/** 回写lsn和lsn之前的日志项 */
void Txnlog::flush(LsnType lsn, FlushSource fs)  {
	m_flusher->flush(lsn, fs);
}
/** 获取日志模块统计信息 */
const LogStatus& Txnlog::getStatus() {
	m_stat.m_tailLsn = tailLsn();
#ifdef TNT_ENGINE
	if (m_tntDb != NULL) {
		m_stat.m_dumpLsn= m_tntDb->getTNTControlFile()->getDumpLsn();
		m_stat.m_ckptLsn = m_db->getControlFile()->getCheckpointLSN();
		m_stat.m_startLsn = min(m_stat.m_dumpLsn, m_stat.m_ckptLsn);
	} else {
#endif
		m_stat.m_ckptLsn = m_db->getControlFile()->getCheckpointLSN();
		m_stat.m_startLsn = m_stat.m_ckptLsn;
#ifdef TNT_ENGINE
	}
#endif
	
	m_stat.m_flushedLsn = m_writeBuffer->getLastFlushedLsn();
	return m_stat;
}
/**
 * 获取日志空间占用率
 * @return 0到1之间，日志占用率
 */
double Txnlog::getUsedRatio() const {
	LsnType beginLsn = 0;
	uint    logFileCntHwm = 0;
#ifdef TNT_ENGINE
	if (m_tntDb != NULL) {
		beginLsn = m_db->getControlFile()->getCheckpointLSN();
		beginLsn = min(beginLsn, m_tntDb->getTNTControlFile()->getDumpLsn());
		logFileCntHwm = m_db->getConfig()->m_logFileCntHwm;
	} else {
		assert(m_tntDb == NULL);
#endif
		beginLsn = m_db->getControlFile()->getCheckpointLSN();
		logFileCntHwm = m_db->getConfig()->m_logFileCntHwm;
#ifdef TNT_ENGINE
	}
#endif
	LsnType tail = tailLsn();
	double ratio = (double)(tail - beginLsn) / logFileCntHwm / m_fileSize;
	return ratio >= 1.0 ? 1.0 : ratio;
}

/**
 * 写一条日志
 *
 * @param txnId 事务ID，为0表示不在事务中
 * @param logType 日志类型
 * @param tableId 表ID，对于事务结束日志指定为0
 * @param data 日志内容
 * @param size 日志内容大小
 * @return 日志LSN
 */
LsnType Txnlog::log(u16 txnId, LogType logType, u16 tableId, const byte *data, size_t size) {
	assert(logType < LOG_CPST_MIN);
	LogEntry le;
	le.m_data = const_cast<byte *>(data);
	le.m_txnId = txnId;
	le.m_logType = logType;
	le.m_size = size;
	le.m_tableId = tableId;
	le.m_cpstForLsn = INVALID_LSN;
	LsnType lsn =  m_writeBuffer->write(&le);
	if (le.isTNTLog()) {
		m_stat.m_tntLogSize += alignRound(le.m_size + LOG_RECORD_HDR_SIZE);
	} else {
		m_stat.m_ntseLogSize += alignRound(le.m_size + LOG_RECORD_HDR_SIZE);
	}
	nftrace(ts.log, tout << "Txnlog::log " << lsn << &le);
	return lsn;
}


/**
 * 写一条日志
 *
 * @param txnId 事务ID，为0表示不在事务中
 * @param logType 日志类型
 * @param tableId 表ID，对于事务结束日志指定为0
 * @param data 日志内容
 * @param size 日志内容大小
 * @param targetLsn 待补偿的日志LSN
 * @return 日志LSN
 */
LsnType Txnlog::logCpst(u16 txnId, LogType logType, u16 tableId,
						const byte *data, size_t size, LsnType targetLsn) {
	assert(logType > LOG_CPST_MIN && logType < LOG_CPST_MAX);
	LogEntry le;
	le.m_size = size + sizeof(LsnType);
	le.m_data = (byte *)alloca(le.m_size);
	memcpy(le.m_data, &targetLsn, sizeof(LsnType));
	memcpy(le.m_data + sizeof(LsnType), data, size);
	le.m_txnId = txnId;
	le.m_logType = logType;
	le.m_tableId = tableId;
	le.m_cpstForLsn = targetLsn;
	LsnType lsn = m_writeBuffer->write(&le);
	m_stat.m_ntseLogSize += alignRound(le.m_size + LOG_RECORD_HDR_SIZE);
	return lsn;
}

/**
* 关闭日志模块
* @param flushLog 是否回写日志
*/
void Txnlog::close(bool flushLog){
	getSyslog()->log(EL_DEBUG, "Log stats: Write "I64FORMAT"u, "I64FORMAT"u(B); "
		"Flushes "I64FORMAT"u, "I64FORMAT"u(Pages);"
		, m_stat.m_writeCnt, m_stat.m_writeSize
		, m_stat.m_flushCnt, m_stat.m_flushedPages);
	if (m_writeBuffer && flushLog)
		m_writeBuffer->flushAll();
	delete m_flusher;
	delete m_writeBuffer;
	delete m_readBuffer;
	delete m_buffer;
	delete m_logReader;
	delete[] m_baseName;
	if (m_fileMngr) {
		reclaimOverflowSpace();
		m_fileMngr->close();
	}
	delete this;
}
/** 获取最后一条日志的LSN */
LsnType Txnlog::lastLsn() const {
	return m_writeBuffer->getLastWrittenLsn();
}
/** 获取日志尾部 */
LsnType Txnlog::tailLsn() const {
	return m_writeBuffer->getWriteNextLsn();
}
/**
 * 数据库重启时，确定日志的末尾LSN
 * @return 最后一条日志记录的末尾，下一条日志记录可能的LSN
 */
LsnType Txnlog::findLogTail() throw (NtseException) {
	// 获取检查点LSN，检查点LSN为0表示没有做过检查点
	LsnType lastCheckPointLsn = 0;
#ifdef TNT_ENGINE
	if (m_tntDb != NULL) {
		lastCheckPointLsn = m_db->getControlFile()->getCheckpointLSN();
		lastCheckPointLsn = min(lastCheckPointLsn, m_tntDb->getTNTControlFile()->getDumpLsn());
	} else {
#endif
		lastCheckPointLsn = m_db->getControlFile()->getCheckpointLSN();
#ifdef TNT_ENGINE
	}
#endif
	LsnType restartLsn = lastCheckPointLsn ? lsnCeiling(lastCheckPointLsn) : Txnlog::MIN_LSN;
	LogFile *logFile;
	do { // 找到第一个可能不满的日志文件
		logFile = m_fileMngr->findLogFile(restartLsn);
		if (!logFile) {
			if (lastCheckPointLsn == 0 && restartLsn == Txnlog::MIN_LSN) // 尚未写过日志
				return INVALID_LSN;
			if (restartLsn != 0 && m_fileMngr->findLogFile(restartLsn - 1))
				// restartLsn处在日志文件末尾（同时是日志末尾）
				// 此时restartLsn就是tailLsn
				break;
			NTSE_THROW(NTSE_EC_CORRUPTED_LOGFILE, "Discover log tail failed!"
				" Can't find log file containing LSN "I64FORMAT"u", restartLsn);
		}
		restartLsn = logFile->full() ? logFile->getEndLsn(false) : max(restartLsn, logFile->getTailHint());
	} while(logFile->full());

	LsnType lsn = restartLsn;
	LogPageHdr *pageHdr = 0;
	while(NULL != (pageHdr = m_readBuffer->getPage(lsn))) {
		m_readBuffer->releasePage(pageHdr);
		lsn += LOG_PAGE_SIZE;
	}
	return lsn;
}


/**
 * 创建日志扫描句柄
 *
 * @param startLsn 起始LSN
 * @param endLsn 结束LSN(不包含endLsn)
 * @return 扫描句柄
 */
LogScanHandle* Txnlog::beginScan(LsnType startLsn, LsnType endLsn) const {
	startLsn = max(startLsn, m_fileMngr->getOnlineLsn());
	return m_logReader->beginScan(startLsn, endLsn);
}

#ifdef TNT_ENGINE
/** 提供修改handle值的函数，用于undo时需要随机定位logEntry提高性能
 * @param handle 扫描句柄
 * @param startLsn 起始的lsn
 * @param endLsn   终止的lsn
 */
void Txnlog::resetLogScanHandle(LogScanHandle *handle, LsnType startLsn, LsnType endLsn) {
	assert(startLsn >= m_fileMngr->getOnlineLsn());
	assert(startLsn >= Txnlog::MIN_LSN);
	assert(startLsn <= endLsn);
	handle->m_endLsn = endLsn;
	handle->m_lsn = startLsn;
	handle->m_lsnNext = startLsn;
	memset(&handle->m_logEntry, 0, sizeof(handle->m_logEntry));
	handle->m_logEntry.m_data = handle->m_buf;
}
#endif

/**
 * 获取下一条日志项，存放到扫描句柄
 * @param handle 扫描句柄
 * @return 如果扫描到日志末尾，返回NULL
 */
LogScanHandle* Txnlog::getNext(LogScanHandle *handle) const {
	if (!m_logReader->getNext(handle))
		return NULL;

	if (handle->m_logEntry.m_logType < LOG_CPST_MAX
		&& handle->m_logEntry.m_logType > LOG_CPST_MIN) { // 补偿日志类型，需要读取目标LSN
		assert(handle->m_logEntry.m_size >= sizeof(LsnType));
		handle->m_logEntry.m_cpstForLsn = *(LsnType *)handle->m_logEntry.m_data;
		handle->m_logEntry.m_data += sizeof(LsnType);
		handle->m_logEntry.m_size -= sizeof(LsnType);
	} else {
		handle->m_logEntry.m_cpstForLsn = INVALID_LSN;
	}
	return handle;
}

/** 结束扫描, 释放扫描句柄 */
void Txnlog::endScan(LogScanHandle *handle) const {
	return m_logReader->endScan(handle);
}


/** 截断日志lsn和lsn之后的所有日志, 调用本函数时请不要进行其它日志操作 */
void Txnlog::truncate(LsnType lsn) {
#ifdef TNT_ENGINE
	if (m_tntDb != NULL) {
		assert(lsn >= m_tntDb->getTNTControlFile()->getDumpLsn());
	}
#endif
	assert(lsn >= m_db->getControlFile()->getCheckpointLSN());
	if (lsn >= tailLsn())
		return;
	// 暂停flush线程
	delete m_flusher;
	//1.清空磁盘数据
	LogFile *logFile = m_fileMngr->findLogFile(lsn);
	LsnType endLsn = logFile->getEndLsn();
	// 1.1 清空当前日志文件之后的所有日志文件
	while (NULL != (logFile = m_fileMngr->findLogFile(endLsn))) {
		endLsn = logFile->getEndLsn();
		logFile->initLogfile();
	}
	// 1.2 truncate当前日志文件
	LogFile *curLogFile = m_fileMngr->findLogFile(lsn);
	curLogFile->truncate(lsn);
	m_fileMngr->setCurLogFile(lsn);
	getSyslog()->log(EL_LOG, "Truncate log, lsn: "I64FORMAT"u", lsn);
	//2.清空读缓存
	delete m_logReader;
	delete m_readBuffer;
	PagePool *pool = new PagePool(0, LOG_PAGE_SIZE);
	m_readBuffer = new LogReadBuffer(pool, m_fileMngr, m_cfg.m_readBufPageCnt, m_cfg.m_readBufIoPages);
	pool->registerUser(m_readBuffer);
	pool->init();
	//3.清空写缓存
	delete m_writeBuffer;
	m_writeBuffer =  new LogWriteBuffer(this, lsnCeiling(lsn), m_writeBufPageCnt);
	m_writeBuffer->setHighWatermark(LogConfig::DEFAULT_BUFFER_HIGH_WATERMARK);

	delete m_buffer;
	m_buffer = new LogBuffer(m_readBuffer, m_writeBuffer);
	m_logReader = new LogReader(m_buffer, m_syslog);

	//4.启动flush线程
	m_flusher = new LogFlusher(this, m_cfg.m_flushInterval);
	m_flusher->start();
}

/** 获取系统日志对象 */
Syslog* Txnlog::getSyslog() const {
	return m_syslog;
}
/**
 * 更新日志统计信息（回写）
 * @param pageFlushed 回写的页数
 */
void Txnlog::updateFlushStats(size_t pageFlushed) {
	++m_stat.m_flushCnt;
	m_stat.m_flushedPages += pageFlushed;
}
/**
 * 更新日志统计信息
 * @param bytesWritten 写入日志的字节数
 */
void Txnlog::updateWriteStats(size_t bytesWritten) {
	++m_stat.m_writeCnt;
	m_stat.m_writeSize += bytesWritten;
}

/**更新flush日志统计信息
 *@param fs flush来源
 */
void Txnlog::updateFlushSourceStats(FlushSource fs) {
	if (FS_SINGLE_WRITE == fs) {
		m_stat.m_flush_single_write_cnt++;
	} else if (FS_BATCH_WRITE == fs) {
		m_stat.m_flush_batch_write_cnt++;
	} else if (FS_PREPARE == fs) {
		m_stat.m_flush_prepare_cnt++;
	} else if (FS_COMMIT == fs) {
		m_stat.m_flush_commit_cnt++;
	} else if (FS_ROLLBACK == fs) {
		m_stat.m_flush_rollback_cnt++;
	} else if (FS_PURGE == fs) {
		m_stat.m_flush_purge_cnt++;
	} else if (FS_CHECK_POINT == fs) {
		m_stat.m_flush_check_point_cnt++;
	} else if (FS_NTSE_CREATE_INDEX == fs) {
		m_stat.m_flush_ntse_create_index_cnt++;
	}
}

/** 获取数据库对象 */
Database* Txnlog::getDatabase() const {
	return m_db;
}
/**
 * 日志回写线程开关
 * @param enabled 是否开启
 */
void Txnlog::enableFlusher(bool enabled) {
	m_flusher->enable(enabled);
}

/**
 * 回收溢出日志文件
 * (open/close时和checkpoint之后调用)
 */
void Txnlog::reclaimOverflowSpace() {
	uint logFileCntHwm;
	logFileCntHwm = m_db->getConfig()->m_logFileCntHwm;
	return m_fileMngr->reclaimOverflowSpace(logFileCntHwm);
}
/**
 * 设置最小在线日志lsn， 日志模块保证lsn之后的日志都能够读取
 * @param lsn 最小在线lsn
 * @return 更新成功返回一个句柄，更新失败返回值小于0
 */
int Txnlog::setOnlineLsn(LsnType lsn) {
	assert(lsn <= tailLsn());
	return m_fileMngr->setOnlineLsn(lsn);
}

/**
 * 清除onlineLsn设置
 *
 * @param token setOnlineLsn返回的token
 */
void Txnlog::clearOnlineLsn(int token) {
	return m_fileMngr->clearOnlineLsn(token);
}

/**
 * 设置检查点LSN
 * @param lsn 新的检查点Lsn
 */
void Txnlog::setCheckpointLsn(LsnType lsn)  {
	m_fileMngr->setCheckpointLsn(lsn);
	reclaimOverflowSpace();
}

/**
* 字符串转化成LogType
* @param s 输入字符串

* @return logType  输出日志类型
*/
LogType Txnlog::parseLogType(const char *s) {
	assert(s);

	if (0 == strcmp(s, "LOG_CREATE_TABLE")) {
		return LOG_CREATE_TABLE;
	} else if (0 == strcmp(s, "LOG_DROP_TABLE")) {
		return LOG_DROP_TABLE;
	} else if (0 == strcmp(s, "LOG_TRUNCATE")) {
		return LOG_TRUNCATE;
	} else if (0 == strcmp(s, "LOG_RENAME_TABLE")) {
		return LOG_RENAME_TABLE;
	} else if (0 == strcmp(s, "LOG_BUMP_FLUSHLSN")) {
		return LOG_BUMP_FLUSHLSN;
	} else if (0 == strcmp(s, "LOG_ADD_INDEX")) {
		return LOG_ADD_INDEX;
	} else if (0 == strcmp(s, "LOG_IDX_DROP_INDEX")) {
		return LOG_IDX_DROP_INDEX;
	} else if (0 == strcmp(s, "LOG_HEAP_INSERT")) {
		return LOG_HEAP_INSERT;
	} else if (0 == strcmp(s, "LOG_HEAP_UPDATE")) {
		return LOG_HEAP_UPDATE;
	} else if (0 == strcmp(s, "LOG_HEAP_DELETE")) {
		return LOG_HEAP_DELETE;
	} else if (0 == strcmp(s, "LOG_IDX_DML")) {
		return LOG_IDX_DML;
	} else if (0 == strcmp(s, "LOG_IDX_SMO")) {
		return LOG_IDX_SMO;
	} else if (0 == strcmp(s, "LOG_IDX_SET_PAGE")) {
		return LOG_IDX_SET_PAGE;
	} else if (0 == strcmp(s, "LOG_IDX_CREATE_BEGIN")) {
		return LOG_IDX_CREATE_BEGIN;
	} else if (0 == strcmp(s, "LOG_IDX_CREATE_END")) {
		return LOG_IDX_CREATE_END;
	} else if (0 == strcmp(s, "LOG_IDX_DML_BEGIN")) {
		return LOG_IDX_DML_BEGIN;
	} else if (0 == strcmp(s, "LOG_IDX_DMLDONE_IDXNO")) {
		return LOG_IDX_DMLDONE_IDXNO;
	} else if (0 == strcmp(s, "LOG_IDX_DIU_DONE")) {
		return LOG_IDX_DIU_DONE;
	} else if (0 == strcmp(s, "LOG_IDX_DML_END")) {
		return LOG_IDX_DML_END;
	} else if (0 == strcmp(s, "LOG_TXN_END")) {
		return LOG_TXN_END;
	} else if (0 == strcmp(s, "LOG_TXN_START")) {
		return LOG_TXN_START;
	} else if (0 == strcmp(s, "LOG_MMS_UPDATE")) {
		return LOG_MMS_UPDATE;
	} else if (0 == strcmp(s, "LOG_LOB_INSERT")) {
		return LOG_LOB_INSERT;
	} else if (0 == strcmp(s, "LOG_LOB_UPDATE")) {
		return LOG_LOB_UPDATE;
	} else if (0 == strcmp(s, "LOG_LOB_DELETE")) {
		return LOG_LOB_DELETE;
	} else if (0 == strcmp(s, "LOG_LOB_MOVE")) {
		return LOG_LOB_MOVE;
	} else if (0 == strcmp(s, "LOG_PRE_UPDATE")) {
		return LOG_PRE_UPDATE;
	} else if (0 == strcmp(s, "LOG_PRE_UPDATE_HEAP")) {
		return LOG_PRE_UPDATE_HEAP;
	} else if (0 == strcmp(s, "LOG_PRE_DELETE")) {
		return LOG_PRE_DELETE;
	} else if (0 == strcmp(s, "LOG_ALTER_TABLE_ARG")) {
		return LOG_ALTER_TABLE_ARG;
	} else if (0 == strcmp(s, "LOG_ALTER_INDICE")) {
		return LOG_ALTER_INDICE;
	} else if (0 == strcmp(s, "LOG_ALTER_COLUMN")) {
		return LOG_ALTER_COLUMN;
	} else if (0 == strcmp(s, "LOG_IDX_DML_CPST")) {
		return LOG_IDX_DML_CPST;
	} else if (0 == strcmp(s, "LOG_IDX_SMO_CPST")) {
		return LOG_IDX_SMO_CPST;
	} else if (0 == strcmp(s, "LOG_IDX_SET_PAGE_CPST")) {
		return LOG_IDX_SET_PAGE_CPST;
	} else if (0 == strcmp(s, "LOG_IDX_ADD_INDEX_CPST")) {
		return LOG_IDX_ADD_INDEX_CPST;
	} else {
		assert(false);
		return LOG_MAX;
	}
}

std::istream& operator >> (std::istream &is, LogType &type) {
	string s;
	is >> s;
	type = Txnlog::parseLogType(s.c_str());
	return is;
}

//////////////////////////////////////////////////////////////////////////

/**
 * 构造LogBackuper
 * @param txnlog	事务日志
 * @param startLsn	备份起点(请设置为已备份控制文件中的检查点LSN)
 */
LogBackuper::LogBackuper(Txnlog *txnlog, LsnType startLsn)
	: m_firstTime(true), m_txnlog(txnlog), m_readBuffer(txnlog->m_readBuffer)
	, m_writeBuffer(m_txnlog->m_writeBuffer) {
	m_nextLsn = lsnFloor(startLsn);
	m_nextLsn = max(m_nextLsn, Txnlog::MIN_LSN);
	m_startLsn = m_nextLsn;
	m_minLsnWriteBuf = m_txnlog->m_writeBuffer->getMinLsn();
}
/**
 * 估算日志备份文件长度（字节）
 * 估算的长度必须小于真实的备份文件长度
 */
u64 LogBackuper::getSize() {
	LsnType tailLsn = m_txnlog->tailLsn();
	if (Txnlog::MIN_LSN == tailLsn) // 压根没写过日志
		return LOG_PAGE_SIZE;
	return lsnFloor(tailLsn) - m_startLsn + LOG_PAGE_SIZE;
}
/**
 * 从日志读缓存读取日志页
 * @param buf 页缓存
 * @param pageCnt 带读取页数
 * @return 真正读取的页数
 */
uint LogBackuper::readPagesRB(byte* buf, uint pageCnt) throw(NtseException) {
	assert(m_nextLsn < m_minLsnWriteBuf);
	// 计算能从日志读缓存读取的页数
	uint cnt = pageCnt = min((uint)((m_minLsnWriteBuf - m_nextLsn) / LOG_PAGE_SIZE), pageCnt);
	for (; pageCnt > 0; --pageCnt) {
		LogPageHdr *pageHdr = m_readBuffer->getPage(m_nextLsn);
		if (!pageHdr)
			NTSE_THROW(NTSE_EC_CORRUPTED_LOGFILE,
			"Corrupted log file, reading log page failed! LSN: "I64FORMAT"u",
			m_nextLsn);
		assert(pageHdr->m_startLsn == m_nextLsn);
		memcpy(buf, pageHdr, LOG_PAGE_SIZE);
		buf += LOG_PAGE_SIZE;
		m_nextLsn += LOG_PAGE_SIZE;
		m_readBuffer->releasePage(pageHdr);
	}
	return cnt - pageCnt;
}
/**
 * 获取日志数据
 * @param buf 缓存空间
 * @param pageCnt 缓存页数
 * @param writeDisabled 是否关闭了数据库写操作;
 *	如果关闭了写操作，则读取包括当前日志页在内的所有日志数据
 * @return 实际读取的页数
 */
uint LogBackuper::getPages(byte* buf, uint pageCnt, bool writeDisabled) throw(NtseException) {
	if (m_firstTime) { // 第一次调用，返回备份控制页面
		LogBackupCtrlPage *ctrlPage = (LogBackupCtrlPage *)buf;
		memset(ctrlPage, 0, LOG_PAGE_SIZE);
		ctrlPage->m_size = m_txnlog->m_fileSize;
		ctrlPage->m_startLsn = m_nextLsn;
		ctrlPage->m_checksum = checksum64(buf + sizeof(u64), LOG_PAGE_SIZE - sizeof(u64));
		m_firstTime = false;
		return 1;
	}
	if (m_nextLsn < m_minLsnWriteBuf) { // 待读取页面不在写缓存中
		return readPagesRB(buf, pageCnt);
	} else {
		while (true) {
			m_minLsnWriteBuf = m_writeBuffer->getMinLsn();
			if (m_nextLsn < m_minLsnWriteBuf) { // 日志页在读缓存中
				return readPagesRB(buf, pageCnt);
			} else {
				uint cnt = pageCnt;
				if (m_writeBuffer->readPages(m_nextLsn, buf, &cnt, writeDisabled)) {
#ifdef NTSE_UNIT_TEST
					for (uint i = 0; i < cnt; ++i) {
						LogPageHdr *pageHdr = (LogPageHdr *)(buf + i * LOG_PAGE_SIZE);
						UNREFERENCED_PARAMETER(pageHdr);
						assert(pageHdr->m_startLsn == m_nextLsn + i * LOG_PAGE_SIZE);
					}
#endif
					m_nextLsn += cnt * LOG_PAGE_SIZE;
					return cnt; // 日志页确实在写缓存中
				}
			}
		}
	}
}
//////////////////////////////////////////////////////////////////////////
/**
 * 构造恢复者
 * @param ctrlFile 控制文件
 * @param path 日志文件路径，不包括后缀
 */
LogRestorer::LogRestorer(ControlFile *ctrlFile, const char *path)
	: m_path(System::strdup(path)), m_fileId(0), m_ctrlFile(ctrlFile)
	, m_logFile(0), m_firstTime(true), m_failed(false) {
	m_nextLsn = INVALID_LSN;
	m_ctrlPage = (LogBackupCtrlPage *)new byte[LOG_PAGE_SIZE];
}

LogRestorer::~LogRestorer() {
	if (m_failed) { // 恢复失败
		if (m_logFile)
			m_logFile->close();
		// 清除已创建文件
		for (uint i = 0; i < m_fileId; ++i)
			File(LogFileMngr::makeFileName(m_path, i).c_str()).remove();
	} else {
		if (m_fileId != 0) {
			if (m_logFile) {
				m_logFile->writeCtrlPage();
				m_logFile->close();
			}
			m_ctrlFile->setNumTxnlogs(m_fileId);
		} else {
			// 没有日志时，啥也不做
			// 数据库open时，会创建日志文件
			assert(!m_logFile);
		}
	}

	delete [] m_path;
	delete [] (byte *)m_ctrlPage;
}
/**
 * 根据备份恢复日志文件
 * @param buf 备份日志数据缓存
 * @param pageCnt 日志数据页数
 */
void LogRestorer::doSendPages(const byte *buf, uint pageCnt) throw(NtseException) {
	assert(!m_failed);
	if (m_firstTime) { // 第一次调用本函数
		// 读取备份控制页
		memcpy(m_ctrlPage, buf, LOG_PAGE_SIZE);
		if (m_ctrlPage->m_checksum != checksum64(buf + sizeof(u64), LOG_PAGE_SIZE - sizeof(u64)))
			NTSE_THROW(NTSE_EC_INVALID_BACKUP, "Log backup control page has invalid checksum");

		--pageCnt;
		buf += LOG_PAGE_SIZE;
		m_firstTime = false;
		m_nextLsn = m_ctrlPage->m_startLsn;
	}
	do {
		// 判断当前日志文件是否能容纳部分日志
		if (!m_logFile || m_nextLsn >= m_logFile->getEndLsn()) {
			if (m_logFile) {
				m_logFile->full(true);
				m_logFile->writeCtrlPage();
				m_logFile->close();
				m_logFile = 0;
			}
			string filename = LogFileMngr::makeFileName(m_path, m_fileId++);
			LogFile::createLogFile(filename.c_str(), m_ctrlPage->m_size);
			m_logFile = LogFile::open(0, filename.c_str(), false);
			m_logFile->setStartLsn(m_nextLsn);
		}
		uint cnt = min(pageCnt, (uint)((m_logFile->getEndLsn() - m_nextLsn) / LOG_PAGE_SIZE));
		// 检查页面的正确性
#ifdef NTSE_UNIT_TEST
		for (uint i = 0; i < cnt; ++i) {
			LogPageHdr *pageHdr = (LogPageHdr *)(buf + i * LOG_PAGE_SIZE);
			UNREFERENCED_PARAMETER(pageHdr);
			assert(pageHdr->m_startLsn == m_nextLsn + i * LOG_PAGE_SIZE);
		}
#endif
		u64 errCode = m_logFile->doWriteChecksumedPages(m_nextLsn, buf, cnt);
		if (File::getNtseError(errCode) != File::E_NO_ERROR)
			NTSE_THROW(errCode, "Write log file %s failed", m_logFile->getPath());
		pageCnt -= cnt;
		m_nextLsn += cnt * LOG_PAGE_SIZE;
		buf += cnt * LOG_PAGE_SIZE;
	} while (pageCnt);
}
/**
 * 根据备份恢复日志文件
 * @param buf 备份日志数据缓存
 * @param pageCnt 日志数据页数
 */
void LogRestorer::sendPages(const byte *buf, uint pageCnt) throw(NtseException) {
	try {
		doSendPages(buf, pageCnt);
	} catch(NtseException &e) {
		m_failed = true;
		throw e;
	}
}

//////////////////////////////////////////////////////////////////////////
/** lsn转换到页内偏移 */
size_t lsnToOffInPage(LsnType lsn) {
	return (size_t)(lsn & (LOG_PAGE_SIZE - 1));
}
/** lsn上取整到页面边界 */
LsnType lsnCeiling(LsnType lsn) {
	return (lsn + (LOG_PAGE_SIZE - 1)) & ~((LsnType)LOG_PAGE_SIZE - 1);
}
/** lsn上取整到页面边界 */
LsnType lsnFloor(LsnType lsn) {
	return lsn & ~((LsnType)LOG_PAGE_SIZE - 1);
}

/** 
 * 能够容纳指定长度日志的最小日志写缓存
 * @param logSize 日志长度
 * @return 最小日志写缓存大小
 */
static size_t computeMinlogBufferSize(size_t logSize) {
	return LOG_PAGE_SIZE + (size_t)((u64)logSize * LOG_PAGE_SIZE / PAGE_MAX_PAYLOAD_SIZE);
}

/** 
 * 能够容纳指定长度日志的最小日志文件长度
 * @param logSize 日志长度
 * @return 最小日志文件长度
 */
static size_t computeMinlogFileSize(size_t logSize) {
	return LOG_CTRL_PAGE_SIZE + computeMinlogBufferSize(logSize);
}


#ifndef NDEBUG
/** 检查日志记录的有效性 */
bool isLogRecValid(const LogRecordHdr *rec, const LogPageHdr *page) {
#ifdef TXNLOG_DEBUG
	if (rec->m_magic != LogRecordHdr::MAGIC)
		return false;
#endif
	if (rec->m_size > LOG_PAGE_SIZE - LOG_PAGE_HDR_SIZE - LOG_RECORD_HDR_SIZE)
		return false;
	if (page && rec->m_size + LOG_PAGE_HDR_SIZE + LOG_RECORD_HDR_SIZE > page->m_used)
		return false;
	if (rec->m_logType > LOG_MAX)
		return false;
	if (rec->m_type >= LR_MAX)
		return false;
	return true;
}
#ifdef TXNLOG_DEBUG
void verifyPage(const LogPageHdr *page) {
	u32 offset = (u32)LOG_PAGE_HDR_SIZE;
	while (offset < page->m_used) {
		LogRecordHdr *recHdr = (LogRecordHdr *)((byte *)page + offset);
		NTSE_ASSERT(recHdr->m_magic == LogRecordHdr::MAGIC);
		NTSE_ASSERT(recHdr->m_lsn == page->m_startLsn + offset);
		offset = alignRound(offset + LOG_RECORD_HDR_SIZE + recHdr->m_size);
	}
}
#endif

/** 判断日志记录头是否相同 */
bool isLogRecMetaEq(const LogEntry *e, const LogRecordHdr *rec) {
	return e->m_logType == rec->m_logType && e->m_tableId == rec->m_tableId && e->m_txnId == rec->m_txnId;
}
#endif // NDEBUG
/** 根据日志记录初始化LogEntry */
void setLogEntryHdr(LogEntry *e, const LogRecordHdr *rec) {
	e->m_logType = (LogType)rec->m_logType;
	e->m_size = rec->m_size;
	e->m_tableId = rec->m_tableId;
	e->m_txnId = rec->m_txnId;
}


/** 写入LogRecordHdr信息到TRACE当中
 * @param tracer	指定的trace
 * @param recHdr	日志记录对象
 * @return 返回trace便于级联调用
 */
Tracer& operator << (Tracer &tracer, const LogRecordHdr *recHdr) {
	tracer.beginComplex("LogRecordHdr");

	if (recHdr) {
		tracer << "Addr:" << (void*)recHdr
			<< "TxnID:" << recHdr->m_txnId
			<< "TblID:" << recHdr->m_tableId 
			<< "LogType:" << recHdr->m_logType
			<< "Type:" << recHdr->m_type
			<< "Size:" << (int)recHdr->m_size
#ifdef TXNLOG_DEBUG
			<< "LSN:" << recHdr->m_lsn
			<< "CSUM:" << recHdr->m_checksum
			<< "MAGIC:" << recHdr->m_magic
#endif
			;
			
	} else {
		tracer << "Addr:" << (void*)0;
	}

	tracer.endComplex();
	return tracer;
}

} // namespace ntse
