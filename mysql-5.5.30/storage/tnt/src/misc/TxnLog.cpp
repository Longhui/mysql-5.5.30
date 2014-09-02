/**
 * �ڴ����������ʵ��
 *
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
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


// ħ�����أ���־��¼ͷ�з���ħ�����ֶΣ���֤��־��¼��ȷ��
// #define TXNLOG_DEBUG

#undef min
#undef max
using namespace std;

namespace ntse {

/*!
 @brief ��־����
*/
enum LogRecordType
{
	LR_NORMAL=0,		/** ��ͨ��־��¼						*/
	LR_HEAD,			/** ������ĳ������־��¼��ǰ�沿��		*/
	LR_CONTINUE,		/** ��־��¼��ĳ����־��¼�ĺ�������	*/
	LR_TAIL,			/** ��־��¼��ĳ����־��¼�ĺ�����������*/
	LR_MAX,				/** �������ģ����ܳ�����				*/
};

/** ��־ҳͷ */
struct LogPageHdr {
	u64		m_checksum;	/** У���								*/
	LsnType m_startLsn; /** ��־ҳ��ʼ LSN						*/
	u16		m_used;		/** ҳ����ʹ���ֽ���					*/
	u8		m_pad[6];	/** ����ֽ�, ���ṹ��СΪ8�ֽ������� */
};


/** ��־��¼ͷ */
struct LogRecordHdr {
#ifdef TXNLOG_DEBUG
	u64		m_checksum;		/** ��־��¼У��									*/
	LsnType m_lsn;			/** ��־LSN������У��                               */
	u32		m_magic;		/** ħ����������֤��¼��ȷ��						*/
#endif
	u16		m_txnId;		/* txnId ����ID��Ϊ0��ʾ����������					*/
	u16		m_tableId;		/** ��ID											*/
	u16		m_size;			/** ��־��¼���ȣ���������־ͷ����¼���Ȳ��ܳ���һҳ*/
	u8		m_logType;		/** ��־����, LogTypeö������, ���֧��256������	*/
	u8		m_type;			/** ��־��¼����, LogRecordTypeö������				*/

	static const u32 MAGIC = 0xcd12e30;
};

/** ��־�ļ�����ҳ��(��һҳ�� */
struct LogFileCtrlPage {
	u64		m_checksum;	/** У���							*/
	LsnType	m_startLsn;	/** ���ļ�����ʼLSN					*/
	LsnType	m_endLsn;	/** ���ļ��Ľ���LSN					*/
	u64		m_size;		/** ��־�ļ���С					*/
	LsnType	m_tailHint;	/** �����￪ʼ�������һ����־��¼	*/
	u8		m_full;		/** ��־�ļ��Ƿ�����				*/
	u8		m_pad[7];	/** ����ֽ�						*/
};
/**
 * �����ļ�����ҳ
 */
class LogBackupCtrlPage {
public:
	u64		m_checksum;	/** У���							*/
	u64		m_size;		/** ��־�ļ���С					*/
	u64		m_startLsn;	/** �������						*/
};
/** ȡ������,���ֽڶ��� */
template<typename T> inline T alignRound(T v) {
	return (v + 7) & ~((T)7);
}

const size_t LOG_PAGE_SIZE = LogConfig::LOG_PAGE_SIZE;						/** ��־ҳ��С	*/
const size_t LOG_PAGE_HDR_SIZE = alignRound(offsetof(LogPageHdr, m_pad));	/** ҳͷ��С	*/
const size_t LOG_RECORD_HDR_SIZE = (sizeof(LogRecordHdr));					/** ��¼ͷ��С	*/
const size_t LOG_CTRL_PAGE_SIZE = LOG_PAGE_SIZE;	/** ��־�ļ�����ҳ��(��һ��ҳ�棩��С */
/* ������־ҳ����Ч���س��� */
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

/** ��־�ļ� */
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
	File	*m_file;	/** ��־�ļ� */
	LogFileCtrlPage *m_ctrlPage; /** ����ҳ�棬��־�ļ��ĵ�һ��ҳ�� */
	bool m_memOnly;	/** �ļ��Ƿ�ֻ�����ڴ棬��������� */
	bool	m_directIo;	/** �Ƿ�����directIo */
	Syslog *m_sysLog;
	/** 
	 * Ԥ����һ��IO���棬�Ա��⽫���ڴ����ʧ��
	 * �û�����Ҫ���ڳ�ʼ����־�ļ������汻����LogFile����
	 */
	static IoBuffer m_ioBuffer; 
};
/** ��־�ļ������� */
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
	vector<LogFile *> m_logFiles;		/** ��־�ļ����� */
	LogFile *m_curLogFile;				/** ��ǰ��־�ļ� */
	u64 m_fileSize;						/** ��־�ļ���С */
	RWLock m_lock;
	std::string m_basename;
	Syslog *m_sysLog;					/** ϵͳ��־��  */
#ifdef TNT_ENGINE
	TNTControlFile *m_tntCtrlFile;      /** tnt�����ļ� */
	bool            m_sync;
#endif
	ControlFile *m_ctrlFile;			/** ���ݿ�����ļ� */
	bool m_directIo;					/** �Ƿ�directIo */
	LsnType m_onlineLsn;				/** ��С����LSN, ֮�����־���������� */
	/**
	 * ��¼��ǰ���ù���onlineLsn
	 * m_onlineLsn = min{lsn in m_onlineLsnSettings}
	 */
	vector<LsnType> m_onlineLsnSettings;
};

/** ҳ���д��� */
struct FlushHandle {
	inline LsnType endLsn() const {
		return m_startPage->m_startLsn + m_pageCnt * LOG_PAGE_SIZE;
	}
	inline LsnType firstLsn() const {
		return m_startPage->m_startLsn;
	}
	/** ��֤��־ҳ�����ȷ�� */
	inline void verify() {
#ifdef TXNLOG_DEBUG
		for (size_t i = 0; i < m_pageCnt; ++i)
			verifyPage((LogPageHdr *)((byte *)m_startPage + i * LOG_PAGE_SIZE));
#endif
	}


	LogPageHdr	*m_startPage;	/** ��ʼҳ��	*/
	size_t		m_pageCnt;		/** ҳ��		*/
};
/** Flush�ȴ��� */
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
	Event	m_evt;			/** �¼�			*/
	LsnType	m_targetLsn;	/** FlushĿ��LSN	*/
};

/** ��־��д�߳� */
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
	LsnType m_targetLsn;			/** Ҫ��д��Ŀ��LSN			*/
	DList<FlushWaiter *> m_waiters;	/** �ȴ�ˢ�µ��߳�			*/
	size_t  m_writenPages;			/** �ϴ���������д���ֽ���	*/
	bool    m_enable;				/** �Ƿ�����д�߳�         */
#ifdef TNT_ENGINE
	bool    m_sync;					/** ��־flush���Ƿ���Ҫsync */
#endif
};

/** ��־д���� */
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

	LsnType m_writeNext;	/** ��һ����־���ܵ�LSN		*/
	LsnType m_writeLast;	/** ���һ����־��LSN		*/
	RWLock	m_writeLock;	/** ����m_writeLast			*/
	LsnType m_flushLast;	/** �ϴλ�дĩβ			*/
	RWLock	m_flushLock;	/** ����m_flushLast			*/
	LsnType m_minPageLsn;	/** ��������־ҳ����СLSN	*/
	byte	*m_buffer;		/** ����					*/
	size_t	m_bufferSize;	/** �����ֽ���				*/
	uint	m_pageCnt;		/** ����ҳ��				*/
	LogFile	*m_curLogFile;	/** ��ǰ��־�ļ�			*/
	double  m_bufHwm;		/** �����ˮ�ߣ�ռ���ʳ�����ˮ��ʱ��Ӧ������д�߳� */
	Txnlog  *m_txnLog;
};

/** ��ϣ���� */
struct LogPageHasher {
	inline unsigned int operator () (const LogPageHdr *page) const {
		return (unsigned int)page->m_startLsn;
	}
};
/** ��ϣ���Equal���� */
struct LsnLogPageEqualer {
	inline bool operator() (LsnType lsn, const LogPageHdr *page) const {
		return page->m_startLsn == lsn;
	}
};
/** ��־������ */
class LogReadBuffer : public PagePoolUser {
	/** ��־ҳ�����Ϣ */
	struct PageInfo {
		LogPageHdr* m_page;			/** ��־ҳ					*/
		DLink<PageInfo *> m_dlink;	/** DLINK�����Թ���LRU����	*/
		size_t m_poolEntry;			/** ObjectPool�ı�������	*/
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
	/** ��־ҳ���ϣ�� */
	DynHash<LsnType, LogPageHdr*, LogPageHasher, Hasher<LsnType>, LsnLogPageEqualer> m_pageHash;
	DList<PageInfo *>	m_lruList;		/** ҳ��LRU����			*/
	LsnType				m_starLsn;		/** �����е�һҳ��LSN	*/
	uint				m_ioPages;		/** һ�ζ�ȡ����ҳ		*/
	RWLock				m_lock;			/** ���ڱ���ȫ�ֽṹ	*/
	LogFileMngr			*m_logFileMngr; /** ��־�ļ�������		*/
	PagePool			*m_pool;		/** ��ռ��PagePool		*/
	ObjectPool<PageInfo> m_pageInfoPool;/** ����ڵ��			*/
};

/** ��־���棨�����־��д����) */
class LogBuffer {
public:
	LogBuffer(LogReadBuffer *readBuffer, LogWriteBuffer *writeBuffer);

	LogPageHdr* getPage(LsnType pageLsn);
	void releasePage(LogPageHdr *pageHdr);
private:
	LogReadBuffer	*m_readBuffer;		/** ��־������						*/
	LogWriteBuffer	*m_writeBuffer;		/** ��־д����						*/
	LsnType			m_minLsnWriteBuf;	/** ��־д�������СLSN������ȷ��	*/
};

/** ��־�Ķ��� */
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
	LogBuffer *m_readBuffer;	/** ������ */
	Syslog *m_log;
};
//////////////////////////////////////////////////////////////////////////


/**
 * ��ʼ����־�ļ�����־�ļ�����
 */
void LogFile::initLogfile() {
	assert(!m_memOnly);
	u64 size = m_ctrlPage->m_size;
	memset(m_ctrlPage, 0, LOG_CTRL_PAGE_SIZE);
	m_ctrlPage->m_size = size;
	NTSE_ASSERT(File::E_NO_ERROR == initLogfile(m_file, getSize()));
}



/**
 * �ضϱ���־�ļ�����lsnλ�ÿ�ʼ�ض�
 * @param lsn �ض�λ��
 * @param notZeroPageLsn ��һ�����������ҳ��
 *	�������޶���������lsn֮��notZeroPageLsn֮ǰ��ҳ����Ҫ����
 */
void LogFile::truncate(LsnType lsn, LsnType notZeroPageLsn) {
	assert(notZeroPageLsn);
	assert(lsn <= notZeroPageLsn);
	assert(notZeroPageLsn == INVALID_LSN || notZeroPageLsn <= getEndLsn());
	LsnType pageLsn = lsnFloor(lsn);
	if (pageLsn != lsn) {	// truncate��ҳ��
		byte *page = (byte *)System::virtualAlloc(LOG_PAGE_SIZE);
		readPages(pageLsn, page, 1);
		LogPageHdr *pageHdr = (LogPageHdr *)page;
		size_t used = (size_t)(lsn - pageLsn);
		pageHdr->m_used = (u16)used;
		memset(page + used, 0, LOG_PAGE_SIZE - used);
		writePages(pageLsn, page, 1);
		System::virtualFree(page);

		pageLsn += LOG_PAGE_SIZE; // pageLsnָ����һҳ
	}
	LsnType endLsn = min(notZeroPageLsn, getEndLsn());
	if (pageLsn <= endLsn) {
		setTailHint(pageLsn);
		// ��־β�Ժ����־ҳ����
		initPages(pageLsn, (size_t)((endLsn - pageLsn) / LOG_PAGE_SIZE));
		writeCtrlPage();
	}
}
/** ��֤��־�ļ�����Ч�� */
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
 * ��һ����־�ļ�
 * @param sysLog ϵͳ��־
 * @param filename ��־�ļ���
 * @param directIo �Ƿ�directIo
 * @return �´򿪵���־�ļ�
 * @throw NtseException ����־�ļ�ʧ��
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
 * ����һ���ڴ�LogFile,�������ļ�ϵͳ�д�����־�ļ�
 * @param sysLog ϵͳ��־
 * @param startLsn ���ļ�����ʼLSN
 * @param filename ��־�ļ���
 * @param size ��־�ļ�����
 * @param directIo �Ƿ�����directIo
 * @return �´������ڴ���־�ļ�
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
 * ��ʼ����־�ļ�����־�ļ�����
 * @param file ��־�ļ�
 * @param size ��־�ļ�����
 * @return �ļ�����������
 */
u64 LogFile::initLogfile(File *file, u64 size) {
	assert(size > LOG_CTRL_PAGE_SIZE);
	assert((size - LOG_CTRL_PAGE_SIZE) % LOG_PAGE_SIZE == 0);

	IoBuffer *ioBuffer = (IoBuffer *)Buffer::getBatchIoBufferPool()->getInst();
	memset(ioBuffer->getBuffer(), 0, LOG_CTRL_PAGE_SIZE);
	LogFileCtrlPage* pageHdr = (LogFileCtrlPage *)ioBuffer->getBuffer();
	pageHdr->m_size = size;
	checksumCtrlPage(pageHdr);
	// ��ʼ������ҳ
	u64 errCode = file->write(0, LOG_CTRL_PAGE_SIZE, ioBuffer->getBuffer());
	if (File::E_NO_ERROR != errCode)
		return errCode;

	// �����ļ�����
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
 * �ڴ����ϴ���һ����־�ļ�, ��־�ļ�״̬Ϊδʹ�ã�startLsn == 0), ����ҳ�涼��ʼ��Ϊ0
 * @param filename ��־�ļ���
 * @param size ��־�ļ�����
 * @throw NtseException �������߳�ʼ����־�ļ�ʧ��
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
 * ��ȡ�������ҳ��
 * @param lsn ��ʼҳ��LSN
 * @param page �����ַ
 * @param pageCnt ����ȡ��ҳ��
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
 * д���������־ҳ�浽����, ������������ҳ־У���
 * @param lsn ҳ����ʼLSN
 * @param page ҳ������
 * @param pageCnt ҳ��
 * @return ErrorCode
 */
u64 LogFile::doWritePages(LsnType lsn, void *page, size_t pageCnt) {
	checksumPages((LogPageHdr *)page, pageCnt);
	return doWriteChecksumedPages(lsn, page, pageCnt);
}
/**
 * д���������־ҳ�浽����, ��������ҳ־У���
 * @param lsn ҳ����ʼLSN
 * @param page ҳ������
 * @param pageCnt ҳ��
 * @return ErrorCode
 */
u64 LogFile::doWriteChecksumedPages(LsnType lsn, const void *page, size_t pageCnt) {
	assert(lsn == lsnFloor(lsn));
	assert(lsn >= getStartLsn());
	// reset��ʱ�򣬻���д�����ļ�
	// �������assert������
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
 * д���������־ҳ�浽���̣�����͸���ҳУ���
 * @pre lsn����ҳ����
 * @param lsn д��λ�õ�LSN
 * @param page ��д���ҳ
 * @param pageCnt ҳ��
 */
void LogFile::writePages(LsnType lsn, void *page, size_t pageCnt) {
	u64 errCode = doWritePages(lsn, page, pageCnt);
	if (errCode != File::E_NO_ERROR)
		m_sysLog->fopPanic(errCode, "Write log file %s failed.", m_file->getPath());
}
/**
 * ���¶�ҳУ���
 * @param page ��д���ҳ
 * @param pageCnt ҳ��
 */
void LogFile::checksumPages(LogPageHdr *page, size_t pageCnt) {
	for (uint i = 0; i < pageCnt; ++i)
		checksumPage((LogPageHdr*)((byte *)page + i * LOG_PAGE_SIZE));
}
/**
 * ���µ�ҳУ���
 * @param page ��д���ҳ
 * @param pageCnt ҳ��
 */
void LogFile::checksumPage(LogPageHdr *page) {
	u64 v = checksum64((byte*)page + sizeof(u64), LOG_PAGE_SIZE - sizeof(u64));
	page->m_checksum = v;
}
/**
 * ���¿���ҳУ���
 * @param page ��д���ҳ
 * @param pageCnt ҳ��
 */
void LogFile::checksumCtrlPage(LogFileCtrlPage *page) {
	u64 v = checksum64((byte*)page + sizeof(u64), LOG_CTRL_PAGE_SIZE - sizeof(u64));
	page->m_checksum = v;
}
/** ��־�ļ�����ʼLSN */
LsnType LogFile::getStartLsn() const {
	return m_ctrlPage->m_startLsn;
}
/**
 * ��־�ļ��Ľ���LSN
 * @param lock �Ƿ���Ҫ����
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
 * ������־�ļ��Ľ�βLSN,
 * ��ǰ��־�ļ�������д��һ������־��¼ʱ�����ñ���������ǰ������ǰ��־�ļ�
 * @param endLsn ��βLSN
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
 * ������ʼLSN,�����¼����βLSN
 *	����ʱ���ж���
 *	��һ��������־�ļ�֮�󣬳��δ���־�ļ�֮ʱ����־�ļ��Դ���δ��ʼ��״̬(startLsn == 0)
 *	��������ݻָ�ʱ
 * @param startLsn ��ʼLSN
 */
void LogFile::setStartLsn(LsnType startLsn) {
	// ��һ�δ����ݿ�ʱ����־�ļ���endLsnΪ0
	// ǰn-1��open���ݿⶼû��д��־����ô��n��open��startLsn = Txnlog::MIN_LSN
	assert(m_ctrlPage->m_endLsn == 0 || m_ctrlPage->m_startLsn == Txnlog::MIN_LSN);
	assert(!m_memOnly);
	assert(startLsn == lsnFloor(startLsn));
	m_ctrlPage->m_startLsn = startLsn;
	m_ctrlPage->m_endLsn = startLsn + payloadSize();
}
/** һ����־�ļ�ʵ�������洢��־�Ŀռ� */
u64 LogFile::payloadSize() const {
	return m_ctrlPage->m_size - LOG_CTRL_PAGE_SIZE;
}
/** ��ȡ��־�ļ���С */
u64 LogFile::getSize() const {
	return m_ctrlPage->m_size;
}
/** ��ȡ��־�ļ�ȫ·�� */
const char* LogFile::getPath() const {
	return m_file->getPath();
}
/**
 * ������־�ļ�, ʹ����־�ļ����Դ�nextLsn��ʼд��
 *	�����ǰ���ڴ���־�ļ����򴴽�������־�ļ�
 * @param nextLsn ��ʼLSN��ַ������ҳ����
 */
void LogFile::reset(LsnType nextLsn){
	assert(nextLsn == lsnFloor(nextLsn));
	assert(nextLsn >= getStartLsn() && nextLsn < getEndLsn());
	if (m_memOnly) { // ��־�ļ������ڴ����ϣ���ʱ����һ����־�ļ�
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
	// д�����ҳ��
	writeCtrlPage();
}
/**
 * ��ʼ��ҳ��
 * @param lsn ��ʼҳlsn
 * @param pageCnt ҳ��
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
 * ����tail hint
 * @param lsn ���������[startLsn, endLsn)֮��
 */
void LogFile::setTailHint(LsnType lsn) {
	m_ctrlPage->m_tailHint = lsn;
}
/** ��ȡtail hint */
LsnType LogFile::getTailHint() const {
	return m_ctrlPage->m_tailHint;
}
/**
 * �ж���־�ļ��Ƿ��Ѿ��þ�
 *	���Լ���Ѱ����־ĩβ��Ч��
 * ����trueʱ����־�ļ��ض��þ�
 * ����falseʱ����־�ļ���һ���þ�
 */
bool LogFile::full() {
	return m_ctrlPage->m_full == 1;
}
/** ������־�ļ�full��� */
void LogFile::full(bool isFull) {
	m_ctrlPage->m_full = (u8)isFull;
}
/** ����һ����־�ļ����� */
LogFile::LogFile(Syslog *sysLog, const char *filename, bool directIo)
	: m_lock("LogFile::lock", __FILE__, __LINE__), m_file(0), m_ctrlPage(0)
	, m_memOnly(false), m_directIo(directIo), m_sysLog(sysLog) {
	m_file = new File(filename);
	m_ctrlPage = (LogFileCtrlPage *)System::virtualAlloc(LOG_CTRL_PAGE_SIZE);
	memset(m_ctrlPage, 0, LOG_CTRL_PAGE_SIZE);
}
/** �ر���־�ļ��������ڴ� */
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
 * ������־�ļ�
 * @param startLsn ��־�ļ��µ���ʼLSN
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
/** ��ȡ����ҳ */
void LogFile::readCtrlPage() {
	u64 errCode = m_file->read(0, LOG_CTRL_PAGE_SIZE, m_ctrlPage);
	u64 v = checksum64((byte*)m_ctrlPage + sizeof(u64), LOG_CTRL_PAGE_SIZE - sizeof(u64));
	if (v != m_ctrlPage->m_checksum)
		m_sysLog->log(EL_PANIC, "Log file %s have bad control page checksum.", m_file->getPath());
	if (errCode != File::E_NO_ERROR)
		m_sysLog->fopPanic(errCode, "read control page of %s failed.", m_file->getPath());
}
/** д�����ҳ */
void LogFile::writeCtrlPage() {
	m_ctrlPage->m_checksum = checksum64((byte*)m_ctrlPage + sizeof(u64), LOG_CTRL_PAGE_SIZE - sizeof(u64));
	u64 errCode = m_file->write(0, LOG_CTRL_PAGE_SIZE, m_ctrlPage);
	if (errCode != File::E_NO_ERROR)
		m_sysLog->fopPanic(errCode, "write control page of %s failed.", m_file->getPath());
}

//////////////////////////////////////////////////////////////////////////
/**
 * ����һ����־�ļ�������
 * @param basename ��־�ļ�������������׺
 * @param sysLog ϵͳ��־
 * @param ctrlFile �����ļ�
 * @param �Ƿ���directIo
 */
LogFileMngr::LogFileMngr(const char *basename, Syslog *sysLog, ControlFile *ctrlFile, bool directIo)
	: m_curLogFile(0), m_fileSize(0), m_lock("LogFileMngr::lock", __FILE__, __LINE__)
	, m_basename(basename), m_sysLog(sysLog), m_ctrlFile(ctrlFile), m_directIo(directIo), m_onlineLsn(INVALID_LSN)
	 {
	m_onlineLsnSettings.push_back(INVALID_LSN);	// ����һ��tokenλ��;
#ifdef TNT_ENGINE
	m_tntCtrlFile = NULL;
#endif
}

#ifdef TNT_ENGINE
LogFileMngr::LogFileMngr(const char *basename, Syslog *sysLog, TNTControlFile *tntCtrlFile, ControlFile *ctrlFile, bool directIo)
	: m_curLogFile(0), m_fileSize(0), m_lock("LogFileMngr::lock", __FILE__, __LINE__)
	, m_basename(basename), m_sysLog(sysLog), m_tntCtrlFile(tntCtrlFile), m_ctrlFile(ctrlFile), m_directIo(directIo), m_onlineLsn(INVALID_LSN)
	 {
	m_onlineLsnSettings.push_back(INVALID_LSN);	// ����һ��tokenλ��;
}
#endif
/**
 * ����־�ļ�������
 * @param txnLog ������־������
 * @param basename ��־�ļ�������������׺
 * @param numLogFile ��ǰ��־�ļ�����
 * @return ��־�ļ�������
 * @throw NtseException ����־�ļ�������ʧ��
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
		// ����Ƿ������־�ļ��������������־�ļ��������Զ�����
		bool fileExists = false;
		for (uint i = 0; i < numLogFile && !fileExists; ++i)
			fileExists = File::isExist(lfm->makeFileName(i).c_str());
		if (!fileExists)
			NTSE_THROW(NTSE_EC_MISSING_LOGFILE, "Log file don't exists!");

		// ��������־�ļ�
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
 * ������־�ļ�������
 * @param basename ��־�ļ�������������׺
 * @param numLogFile ��Ҫ��������־�ļ�����
 * @param fileSize ��־�ļ�����
 * @throw NtseException ������־�ļ�������ʧ��
 */
void LogFileMngr::create(const char *basename, uint numLogFile, size_t fileSize) throw (NtseException) {
	for (uint i = 0; i < numLogFile; ++i)
		LogFile::createLogFile(makeFileName(basename, i).c_str(), fileSize);
}

/**
 * ���´�����־�ļ�������
 * @param basename ��־�ļ�������������׺
 * @param numLogFile ��Ҫ��������־�ļ�����
 * @param fileSize ��־�ļ�����
 * @param startLsn ��־��ʼLSN��һ��Ϊԭ�м���LSN
 * @return ʵ�ʵ���־��ʼLSN
 * @throw NtseException ������־�ļ�������ʧ��
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
 * @param basename ��־�ļ�������������׺
 * @param numLogFile ��Ҫ��������־�ļ�����
 * @throw NtseException ������־�ļ�������ʧ��
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
 * �Ӵ����϶�ȡ�����ļ���ҳ
 * @param lsn ��һ��ҳ��LSN
 * @param page ҳ��ʼ��ַ
 * @param pageCnt ��Ҫ��ȡ��ҳ��
 * @return �ɹ���ȡ��ҳ����ֻ�ж�����־ĩβ��ʱ�򷵻�ֵ�Ų�����pageCnt
 */
size_t LogFileMngr::readPages(LsnType lsn, void *page, size_t pageCnt) {
	assert(lsn == lsnFloor(lsn)); // ����ҳ����
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
		if (logFile == m_curLogFile) // ��ǰ��־�ļ�
			break;
	}
	RWUNLOCK(&m_lock, Shared);
	return total - pageCnt;
}
/** ��ȡ��ǰ��־�ļ� */
LogFile* LogFileMngr::curLogFile() const {
	return m_curLogFile;
}
/** ��ȡ��־�ļ����� */
u64 LogFileMngr::getLogFileSize() const {
	return m_fileSize;
}

/**
 * ����һ����־�ļ����󣬵��ǲ������ڴ����ϴ����ͳ�ʼ����־�ļ�
 * @param startLsn ���ļ�����ʼLSN
 * @return ��־�ļ�����
 */
LogFile* LogFileMngr::getNewLogFile(LsnType startLsn) {
	RWLOCK(&m_lock, Exclusived);
	LogFile *logFile = findUnusedLogFile();
	if (!logFile) { // ����Ҳ���������־�ļ����򴴽��µ���־�ļ�����
		// ��������־ĩβ�����ڱ��ļ��У���˸��ļ�������Ӧ���ڴ����ϴ���;
		// �������ֻ����һ���ڴ��е���־�ļ�;
		logFile = LogFile::createInMem(m_sysLog, startLsn
			, makeFileName((uint)m_logFiles.size()).c_str(), m_fileSize
			, m_directIo);
		m_sysLog->log(EL_LOG, "Create log file %s in memory, start LSN "I64FORMAT"u."
			, logFile->getPath(), startLsn);
		m_logFiles.push_back(logFile);
	} else { // ���þɵ���־�ļ�����
		m_sysLog->log(EL_LOG, "Reuse log file %s, new start LSN "I64FORMAT"u."
			, logFile->getPath(), startLsn);
		logFile->reuse(startLsn);
		if (m_curLogFile == logFile) {
			// �ܹ�ֻ��һ����־�ļ�ʱ�����ܻ����õ�ǰ�ļ�
			// ���õ�ǰ�ļ�ʱ��������m_curLogFile->getEndLsn()������switchLogFile���ɻ�
			// �����Ҫ�ڴ���ǰreset
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
 * �л���־�ļ�(������ǰ��־�ļ�������ʼ�����ļ���Ϊ�µĵ�ǰ��־�ļ�)
 *	����д��־ҳ��LSN������ǰ�ļ�ĩβʱ������������־��д�̵߳���
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
 * ������־֮�󣬵�һ�δ���־ģ��֮ʱ�ĳ�ʼ������
 * @param startLsn ��ʼLSN������ҳ����
 * @return ���ص�һ����־��LSN
 */
LsnType LogFileMngr::initOnFirstOpen() {
	m_curLogFile = m_logFiles[0];
	m_curLogFile->setStartLsn(Txnlog::MIN_LSN);
	m_curLogFile->writeCtrlPage();
	m_curLogFile->reset(Txnlog::MIN_LSN);
	return Txnlog::MIN_LSN;
}
/**
 * Ѱ��һ��δʹ�õ���־�ļ�
 * @pre �Ѿ����ϻ�����
 * @return ���ظ����õ���־�ļ�
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
 * �ҵ�һ������lsn����־�ļ�
 * @param lsn Ŀ��lsn����־�ļ����������lsn
 * @param mode ��ģʽ
 * @return ����lsn����־�ļ�
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
 * ���õ�ǰ��־�ļ�Ϊ����lsn����־�ļ�
 * ����־ģ�飬���߽ض���־ʱ��������ñ��������õ�ǰ��־�ļ�
 */
void LogFileMngr::setCurLogFile(LsnType lsn) {
	LogFile *logFile = findLogFile(lsn);
	assert(logFile);
#ifndef NTSE_UNIT_TEST
	assert(!m_curLogFile);
#endif
	m_curLogFile = logFile;
}
/** �ر���־�ļ�����������ɾ���ڴ� */
void LogFileMngr::close() {
	RWLOCK(&m_lock, Exclusived);
	for (uint i = 0; i < m_logFiles.size(); ++i)
		m_logFiles[i]->close();
	m_logFiles.clear();
	RWUNLOCK(&m_lock, Exclusived);
	delete this;
}
/**
 * ������־(���ܲ������ã�
 *	���³�ʼ����־�ļ�nextLsn֮�󲿷�
 *	�����õ�ǰ��־�ļ�
 * @param nextLsn	��־�ļ������һ����Ч��־��¼��ĩβ����һ����־��¼���ܵ���ʼ��ַ
 *					nextLsn����ҳ����
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
 * ������־�ļ���
 * @param basename ��־�ļ�������������׺
 * @param index ��־�ļ���ţ���ʾ�ڼ�����־�ļ�
 * @param ��־�ļ���
 */
std::string LogFileMngr::makeFileName(const char* basename, uint index) {
	std::stringstream ss;
	ss << basename << "." << index;
	return ss.str();
}

/**
 * ���������־�ļ�
 * @param fileCntHwm ��־�ļ���Ŀ��ˮ��
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
 * ������С������־lsn�� ��־ģ�鱣֤lsn֮�����־���ܹ���ȡ
 *
 * @param lsn ��С����lsn
 * @return ���³ɹ�����һ�����������ʧ�ܷ���ֵС��0
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
	// ������֤lsn��Ӧ����־�ļ���������
	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	// lsn����̫С
	if (lsn < min(beginLsn, m_onlineLsn))
		return -1;
	// Ѱ�ҿ���λ��
	int token = 0;
	for (token = 0; token < (int)m_onlineLsnSettings.size(); ++token) {
		if (m_onlineLsnSettings[token] == INVALID_LSN) { // �ҵ�
			m_onlineLsnSettings[token] = lsn;
			break;
		}
	}
	// ���û���ҵ�����λ��, ������һ��token
	if (token == (int)m_onlineLsnSettings.size())
		m_onlineLsnSettings.push_back(lsn);
	// ����onlineLsn
	if (m_onlineLsn > lsn)
		m_onlineLsn = lsn;

	return token;
}

/**
 * ���onlineLsn����
 *
 * @param token setOnlineLsn���ص�token
 */
void LogFileMngr::clearOnlineLsn(int token) {
	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	assert(m_onlineLsn != INVALID_LSN);
	// �����Ч��
	if (token < 0 || token >= (int)m_onlineLsnSettings.size()
		|| m_onlineLsnSettings[token] == INVALID_LSN)
			return;
	LsnType lsn = m_onlineLsnSettings[token];
	// �������
	if (token == (int)m_onlineLsnSettings.size() - 1 && m_onlineLsnSettings.size() > 1) {
		// ��Сm_onlineLsnSettings���飬�������ٱ���һ��Ԫ��
		do {
			m_onlineLsnSettings.pop_back();
		} while (m_onlineLsnSettings.size() > 1 && m_onlineLsnSettings.back() == INVALID_LSN);
	} else {
		m_onlineLsnSettings[token] = INVALID_LSN;
	}
	// �ж��Ƿ���Ҫ���¼���m_onlineLsn
	if (lsn == m_onlineLsn)
		m_onlineLsn = *min_element(m_onlineLsnSettings.begin(), m_onlineLsnSettings.end());
}

/**
 * �õ�����LSN
 *	ֻ������LSN֮ǰ����־�ļ����Ա����á�����
 *	
 * @param mode ��ģʽ
 * @return ����LSN
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
 * ���ü���LSN
 * @param lsn �µļ���Lsn
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
 * ���ѻ�д�̣߳����ȴ�"βLSN"��targetTailLsn֮ǰ����־��¼��д������
 *	��־��¼LSN��ָ��־��¼ͷ��Ӧ��LSN��
 *	��־��¼βLSN��ָ��־��¼ĩβ��Ӧ��LSN
 */
void LogFlusher::waitFlush(LsnType targetTailLsn) {
	FlushWaiter waiter(targetTailLsn);
	DLink<FlushWaiter *> node(&waiter);
	bool inQueue = false; // �Ƿ��Ѿ��ڵȴ�������

	while(true) {
		LOCK(&m_lock);
		LsnType lastFlushed = m_txnLog->m_writeBuffer->getLastFlushedLsn();
		if (targetTailLsn <= lastFlushed) {// �ٴ��ж���־�Ƿ�ˢ��
			if (inQueue) // �ӵȴ�����ɾ��
				node.unLink();
			UNLOCK(&m_lock);
			return;	// ��־�Ѿ���ˢ��������
		}
		// ���»�дĿ��
		if (targetTailLsn > m_targetLsn)
			m_targetLsn = targetTailLsn;

		if (!inQueue) { // ���뵽�ȴ�����
			m_waiters.addLast(&node);
			inQueue = true;
		}
		UNLOCK(&m_lock);
		signal();
		waiter.wait();
	}
}
/**
 * ˢдLSN��targetLSN֮ǰ����־��¼������
 * @param targetLsn ��дĿ��,������ȷָ����־��¼ͷ��
 */
void LogFlusher::flush(LsnType targetLsn, FlushSource fs) {
	if (targetLsn == 0) // Buffer�����flush(0)
		return;
	LogWriteBuffer* writeBuffer = m_txnLog->m_writeBuffer;
	// �����Ҫ��д��ǰҳ����ǰ������ǰд��ҳ
	// ���õ���ǰtargetLsn��־��¼���ڵ�βLSN
	targetLsn = writeBuffer->prepareFlush(targetLsn);
	if (targetLsn == INVALID_LSN) // ����־�Ѿ����ٻ�����
		return;
	LsnType lastFlushed = writeBuffer->getLastFlushedLsn();
	if (targetLsn <= lastFlushed) // ��־�Ѿ���ˢ����ֱ�ӷ���
		return;
	m_txnLog->updateFlushSourceStats(fs);
	// ����ȴ�
	waitFlush(targetLsn);
}

/**
 * ��־�̣߳� һ��Flush�����ҳ��
 * д��־ʧ��ʱ��ϵͳ���ܼ�������
 * ֱ���׳�NtseException
 */
void LogFlusher::run() {
	FlushHandle fh;
	LogWriteBuffer *writeBuffer = m_txnLog->m_writeBuffer;
	LogFileMngr *fileMngr = m_txnLog->m_fileMngr;
	try {
		while(m_enable) {
			LsnType targetLsn = getTargetLsn();
			//��������μ�NTSETNT-40
			if (writeBuffer->nextUnflushedPages(&fh)) {
				m_txnLog->getSyslog()->log(EL_DEBUG, "Flush log ("I64FORMAT"x, %u)", fh.firstLsn(), fh.m_pageCnt);
				FlushHandle toFlush;
				// �ֶ�λ�дҳ��
				//	ÿ������дm_maxIoPages��ҳ��
				//	�Ҳ��ܿ��ļ�д
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
				// ��Ъ������tailHint
				if (m_writenPages >= m_txnLog->m_cfg.m_tailHintAcc) {
					fileMngr->curLogFile()->setTailHint(endLsn - LOG_PAGE_SIZE);
					fileMngr->curLogFile()->writeCtrlPage();
					m_writenPages = 0;
				}
			}
			if (targetLsn == getTargetLsn())	// �Ƿ������˸���Ļ�дĿ��
				break;
		}
	} catch(NtseException &e) {
		m_txnLog->getSyslog()->log(EL_PANIC, "Exception in LogFlusher, Message %s.", e.getMessage());
	}
}
/**
 * ��ȡ��һ�λ�д��ҳ�棬�ڱ�Ҫ��������л���־�ļ�
 * @param fh ԭʼ��д���
 * @param toFlush ���λ�д���
 * return ���λ�д���
 */
FlushHandle* LogFlusher::getNextPagesToFlush(FlushHandle *fh, FlushHandle *toFlush) {
	if (fh->m_pageCnt == 0)
		return 0;

	LogFileMngr *fileMngr = m_txnLog->m_fileMngr;
	LsnType lastLsn = fh->endLsn(); // ����дҳ��Ľ�β

_RESTART:
	// д��־�߳̿�������setEndLsn, ���getEndLsn��Ҫ����
	LsnType endLsn = fileMngr->curLogFile()->getEndLsn();	// ��ǰ��־�ļ���ĩβ
	if (lastLsn <= endLsn) { // û�г�����ǰ��־�ļ�
		toFlush->m_startPage = fh->m_startPage;
		toFlush->m_pageCnt = min(fh->m_pageCnt, (size_t)LogConfig::DEFAULT_MAX_IO_PAGES);
	} else if (endLsn > fh->m_startPage->m_startLsn) { // ���ֳ�����ǰ��־�ļ���С������д�뵽��ǰ��־�ļ�
		toFlush->m_startPage = fh->m_startPage;
		toFlush->m_pageCnt = min(fh->m_pageCnt, (size_t)LogConfig::DEFAULT_MAX_IO_PAGES);
		size_t thisFilePageCnt = (size_t)((endLsn - fh->firstLsn()) / LOG_PAGE_SIZE);
		toFlush->m_pageCnt = min(toFlush->m_pageCnt, thisFilePageCnt);
	} else if (endLsn <= fh->m_startPage->m_startLsn) { // ȫ��������ǰ��־�ļ�
		assert(endLsn == fh->m_startPage->m_startLsn);
		fileMngr->switchLogFile(); // �л���־�ļ�֮���ͷ����
		goto _RESTART;
	}
	fh->m_pageCnt -= toFlush->m_pageCnt;
	fh->m_startPage = fh->m_pageCnt ?
		(LogPageHdr *)((byte *)toFlush->m_startPage + toFlush->m_pageCnt * LOG_PAGE_SIZE) :	0;
	return toFlush;
}

/** ��ȡ��дĿ��LSN */
LsnType LogFlusher::getTargetLsn(void) {
	LsnType lsn;
	LOCK(&m_lock);
	lsn = m_targetLsn;
	UNLOCK(&m_lock);
	return lsn;
}
/** �����رջ�д���� */
void LogFlusher::enable(bool isEnable) {
	m_enable = isEnable;
	if (!isEnable) {
		pause(true);
	} else {
		resume();
	}
}
/** ���ѵȴ���д���߳� */
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
/** ��д������־ */
void LogWriteBuffer::flushAll() {
//  �˴����ܼ�����flush���ܻ��m_writeLock��X��
// 	RWLockGuard guard(&m_writeLock, Shared);
	if (m_writeLast == INVALID_LSN)
		return;
	m_txnLog->flush(m_writeLast);
}
LogWriteBuffer::~LogWriteBuffer() {
	System::virtualFree(m_buffer);
}
/** дһ����־��д��־���� */
LsnType LogWriteBuffer::write(const LogEntry* logEntry) {
	// �����־�����Ƿ�������������
	// �˴������־���ȣ������openʱ���ĺô��ǣ�����ʹ�ú�С����־�ļ������ڲ���.
	NTSE_ASSERT(logEntry->m_size <= LogConfig::MAX_LOG_RECORD_SIZE
				&& computeMinlogBufferSize(logEntry->m_size) <= m_bufferSize
				&& computeMinlogFileSize(logEntry->m_size) <= m_txnLog->m_fileSize);
	RWLOCK(&m_writeLock, Exclusived);
	ftrace(ts.log, tout << logEntry);
_RESTART:
	// ������һ����־��LSN"m_writeNext"
	size_t pageUsed = lsnToOffInPage(m_writeNext);
	bool useCurPage = true;
	if (pageUsed + LOG_PAGE_HDR_SIZE >= LOG_PAGE_SIZE) { // ��ǰҳû�п����õĿռ�
		m_writeNext = lsnCeiling(m_writeNext) + LOG_PAGE_HDR_SIZE;
		useCurPage = false;
	} else if (pageUsed <= LOG_PAGE_HDR_SIZE) { // ��ǰҳ�Ǹ���ҳ
		// ֻ����ת��_RESTART����ִ��ʱpageUsed���п��� == LOG_PAGE_HDR_SIZE
		assert(pageUsed == 0 || pageUsed == LOG_PAGE_HDR_SIZE);
		useCurPage = false;
		if (pageUsed == 0)
			m_writeNext += LOG_PAGE_HDR_SIZE;
	}
	LsnType nextLsn, curLsn;
	curLsn = m_writeNext;
	// Ԥ�ȼ��㵱ǰ��¼��βLSN���Լ���Ҫ������־ҳ��Ŀ
	size_t newPageCnt = calRecordTailLsn(logEntry->m_size, &nextLsn);
	if (useCurPage && newPageCnt == 0) { // ����ֱ��д�뵱ǰҳ��
		m_writeLast = curLsn;
		m_writeNext = nextLsn;
		LsnType tailLsn = writeSimpleRecord(logEntry, curLsn, LR_NORMAL);
		UNREFERENCED_PARAMETER(tailLsn); // tailLsnֻ����assert
		assert(tailLsn == m_writeNext);
		RWUNLOCK(&m_writeLock, Exclusived);
		// ����ͳ����Ϣ
		m_txnLog->updateWriteStats(logEntry->m_size);
		return curLsn;
	}
	// �Ƿ񳬳���ǰ��־�ļ�������
	if (nextLsn > m_curLogFile->getEndLsn(false)) {
		LsnType endLsn = useCurPage ? lsnCeiling(curLsn) : lsnFloor(curLsn);
		m_curLogFile->setEndLsn(endLsn);
		// ����flush�߳�
		m_txnLog->m_flusher->signal();
		// ����һ���µ���־�ļ�
		m_curLogFile = m_txnLog->m_fileMngr->getNewLogFile(m_curLogFile->getEndLsn(false));
		m_writeNext = m_curLogFile->getStartLsn();
		// ���³���д��־
		goto _RESTART;
	}
	// ��ȡ�����ǵ����һҳ���ж�д����ռ��Ƿ��㹻
	// ��nextLsn����ҳ�ף���ҳʵ����δʹ��,
	// �������Ҫ��(nextLsn - 1)������nextLsn
	LogPageHdr *ph = lsnToPageAddr(nextLsn - 1);
	LsnType lastFlush = getLastFlushedLsn();
	if (ph->m_startLsn >= lastFlush) { // д��־�����������ռ䲻��
		waitFlushOnFull(lastFlush);
		goto _RESTART;	// ���³���д��־
	} else if (ph->m_startLsn) { // ������СҳLSN
		// ph->m_startLsn��Ϊ��ʱ, ���ǽ�����ph��һ��־ҳ, ���ph����һҳ����СLSN
		assert(ph->m_startLsn >= m_minPageLsn);
		LsnType nextPageLsn = ph->m_startLsn + LOG_PAGE_SIZE; // �µ�minPageLsn
		// ��鱻�滻ҳ��lsn�Ƿ����nextPageLsn
		// nextPageLsnҳ���ܵ���m_minPageLsn(��Ȧ��,һ����־��¼��д����������)
		assert(lsnToPageAddr(nextPageLsn)->m_startLsn == m_minPageLsn || lsnToPageAddr(nextPageLsn)->m_startLsn == nextPageLsn);
		m_minPageLsn = nextPageLsn;
	}
	{ // д����־
		LsnType tailLsn = newPageCnt ? writeMultiPageRecord(logEntry, curLsn)
			: writeSimpleRecord(logEntry, curLsn, LR_NORMAL);
		UNREFERENCED_PARAMETER(tailLsn); // tailLsnֻ����assert
		assert(tailLsn == nextLsn);
	}
	m_writeLast = curLsn;
	m_writeNext = nextLsn;
	RWUNLOCK(&m_writeLock, Exclusived);
	// ����ռ�ó���1/3������ˢ���߳�
	if (nextLsn > getLastFlushedLsn() + m_bufferSize / 3)
		m_txnLog->m_flusher->signal();
	// ����ͳ����Ϣ
	m_txnLog->updateWriteStats(logEntry->m_size);
	return curLsn;
}
/**
 * ��ȡ��һ�����д��־ҳ
 *	�˺���ֻ����д�̵߳��ã�����m_flushLastֻ�ᱻ��д�̸߳���, ��˶�ȡm_flushLast�������
 *
 * @param handle ˢ�¾��
 * @return ����д���дҳ�棬����һ�����дҳ�棬���򷵻�0
 */
FlushHandle* LogWriteBuffer::nextUnflushedPages(FlushHandle *handle) {

	do {
		LsnType startPageLsn = m_flushLast;	// ����д�ĵ�һҳ
		LsnType endPageLsn = lsnFloor(getWriteNextLsn());// ����д���һҳ����һҳ
		 // ������ߵ��Ӧ��PageLsn
		LsnType bufferEndPageLsn = (startPageLsn + m_bufferSize - 1 ) / m_bufferSize * m_bufferSize;
		if (startPageLsn != bufferEndPageLsn)
			endPageLsn= min(bufferEndPageLsn, endPageLsn); // ���ܿ�Խ������ߵ�

		if (endPageLsn <= startPageLsn) { // ��Ҳû�д���д��ҳ��
			// ������trylock����Ϊ����־д������ʱ������m_writeLock�ϵĶ�����д��־��
			if (!RWTRYLOCK(&m_writeLock, Exclusived))
				goto _NODATA;
			if (lsnFloor(m_writeNext) > m_flushLast) { // ����֮ǰ����������־ҳ��
				RWUNLOCK(&m_writeLock, Exclusived);
				continue;	// ��ͷ����
			} else { // �������һ������ҳ
				assert(lsnFloor(m_writeNext) == m_flushLast);
				if (m_writeNext - m_flushLast > LOG_PAGE_HDR_SIZE) { // ��ǰҳ��ǿ�
					m_txnLog->m_stat.m_flushPaddingSize += lsnCeiling(m_writeNext) - m_writeNext;
					m_writeNext = lsnCeiling(m_writeNext); // ��ǰ������ǰҳ��
					handle->m_pageCnt = 1;
					handle->m_startPage = lsnToPageAddr(m_flushLast);
					RWUNLOCK(&m_writeLock, Exclusived);
					break;
				} else { // ��ǰҳΪ��
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
 * ��д���һ��ҳ��
 *	�˺���ֻ����д�̵߳���
 * @param handle ��д���
 * @return �ϴλ�дĩβ
 */
LsnType LogWriteBuffer::finishFlush(FlushHandle *handle) {
	setLastFlushedLsn(m_flushLast +  handle->m_pageCnt * LOG_PAGE_SIZE);
	handle->m_pageCnt = 0;
	handle->m_startPage = 0;
	return m_flushLast;
}

/**
 * д������ʱ����дһ��������־
 * @param lastFlush �ϴ��ѻ�д��LSN
 */
void LogWriteBuffer::waitFlushOnFull(LsnType lastFlush) {
	m_txnLog->getSyslog()->log(EL_DEBUG, "Log write buffer full, wait flush...");
	RWUNLOCK(&m_writeLock, Exclusived); // ������Ϊ��, �������ᵼ������
	RWLOCK(&m_writeLock, Shared);
	// �ȴ���־��д
	if (m_writeLast > lastFlush) { // m_writeLast����һ����־��ͷ������п���С��lastFlush
		// ��д1/3��־
		m_txnLog->m_flusher->waitFlush(m_writeLast - 2 * (m_writeLast - lastFlush) / 3);
	} else {
		// ��дȫ����־
		m_txnLog->m_flusher->waitFlush(m_writeLast);
	}
	RWUNLOCK(&m_writeLock, Shared);
	RWLOCK(&m_writeLock, Exclusived); // ������
	m_txnLog->getSyslog()->log(EL_DEBUG, "Log flush finished, continue to write log.");
}
/**
 * ��ȡ����������־ҳ(Ŀǰʵ����ס������־д����)
 * @param pageLsn ����ȡ��־ҳLSN
 * @param page [out] ��־ҳ��ָ�룬ҳ�����ڷ���NULL
 * @return false��־ҳ�ڶ������У�����true
 */
bool LogWriteBuffer::getPage(LsnType pageLsn, LogPageHdr **page) {
	assert(pageLsn == lsnFloor(pageLsn));
	RWLOCK(&m_writeLock, Shared);
	if (m_minPageLsn > pageLsn) { // ����ȡҳ����д�����У��ڶ�������
		RWUNLOCK(&m_writeLock, Shared);
		return false;
	}
	LsnType maxPageLsn = lsnFloor(m_writeNext);
	if (pageLsn < maxPageLsn 
		|| (pageLsn == maxPageLsn && (m_writeNext - maxPageLsn) > LOG_PAGE_HDR_SIZE)) { // ��ҳ����
		*page = lsnToPageAddr(pageLsn);
		assert((*page)->m_startLsn == pageLsn);
	} else { // ��ҳ������
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
 * ��ȡ��־ҳ
 * @param lsn ����ȡ����־ҳLSN
 * @param buf ����ռ�,��֤�ܹ�����pageCnt����־ҳ
 * @param pageCnt in:����ȡ����־ҳ��/out:ʵ�ʶ�ȡ��ҳ��
 * @param finishCurPage �Ƿ�Ҫ������ǰ��־ҳ
 * @return false��־ҳ�ڶ������У�����true
 */
bool LogWriteBuffer::readPages(LsnType lsn, byte *buf, uint *pageCnt, bool finishCurPage) {
	assert(lsn == lsnFloor(lsn));
	LockMode mode = Shared;
	RWLOCK(&m_writeLock, mode);
	if (m_minPageLsn > lsn ) { // ����ȡҳ����д�����У��ڶ�������
		RWUNLOCK(&m_writeLock, mode);
		return false;
	}
	assert(lsn <= m_writeNext);
	LsnType curPageLsn = lsnFloor(m_writeNext);
	uint cnt = 0;
	while (cnt < *pageCnt) {
		if (lsn == curPageLsn) { // �Ѿ�������־�ĵ�ǰҳ
			if (!finishCurPage) // ����Ҫ��ȡ��ǰҳ��
				break;
			if (m_writeNext - curPageLsn <= LOG_PAGE_HDR_SIZE) { // ��ǰҳ��һ����ҳ
				// ��ʱ�����߱�֤������д��־�̣߳�m_writeNext == curPageLsn
				assert(m_writeNext == curPageLsn);
				break;
			} else { // ������ǰҳ
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
/** ��ȡ��СLSN */
LsnType LogWriteBuffer::getMinLsn() {
	RWLOCK(&m_writeLock, Shared);
	LsnType minLsn = m_minPageLsn;
	RWUNLOCK(&m_writeLock, Shared);
	return minLsn;
}
/** ��ȡ���һ����־��¼ĩβ��LSN */
LsnType LogWriteBuffer::getWriteNextLsn() {
	LsnType lsn;
	RWLOCK(&m_writeLock, Shared);
	lsn = m_writeNext;
	RWUNLOCK(&m_writeLock, Shared);
	return lsn;
}
/** ��ȡ���һ����־��¼��LSN */
LsnType LogWriteBuffer::getLastWrittenLsn() {
	LsnType lsn;
	RWLOCK(&m_flushLock, Shared);
	lsn = m_writeLast;
	RWUNLOCK(&m_flushLock, Shared);
	return lsn;
}
/** ���һ���ѻ�д��־��¼��LSN */
LsnType LogWriteBuffer::getLastFlushedLsn() {
	LsnType lsn;
	RWLOCK(&m_flushLock, Shared);
	lsn = m_flushLast;
	RWUNLOCK(&m_flushLock, Shared);
	return lsn;
}
/** ����m_flushLast */
void LogWriteBuffer::setLastFlushedLsn(LsnType lastFlushed) {
	RWLOCK(&m_flushLock, Exclusived);
	m_flushLast = lastFlushed;
	RWUNLOCK(&m_flushLock, Exclusived);
}
/**
 * д��һ������־�����ҳ��
 * @param logEntry ��־��
 * @param lsn ����־���lsn
 * @param recordType ��־��¼����
 * @return ��־βLSN
 */
LsnType LogWriteBuffer::writeSimpleRecord(const LogEntry* logEntry, LsnType lsn, LogRecordType recordType) {
	LogPageHdr *pageHdr = lsnToPageAddr(lsn);
	LsnType pageLsn = lsnFloor(lsn);
	size_t offset;
	if (lsnToOffInPage(lsn) <= LOG_PAGE_HDR_SIZE) { // ��Ҫ��ʼ����ҳ��
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
 * д��һ��������־���ҳ��
 * @param logEntry ��־��
 * @param lsn ����־���lsn
 * @return ��־βLSN
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
/** ���ø�ˮ�� */
void LogWriteBuffer::setHighWatermark(double usageRatio) {
	m_bufHwm = usageRatio;
}
/**
 * �����ǰд��ҳ�Ǵ�ˢдҳ����ǰ������ǰҳ
 * �˾���Ϊ�˱���ҳ���д��ͻ
 * @param flushTarget ����д��־��¼��LSN
 * @return flush��־��¼��ĩβLSN
 */
LsnType LogWriteBuffer::prepareFlush(LsnType flushTarget) {
	RWLOCK(&m_writeLock, Shared);
	LsnType tailLsn = getTailLsn(flushTarget);
	if (tailLsn == INVALID_LSN) { // ������־�Ѿ����ڻ�����
		RWUNLOCK(&m_writeLock, Shared);
		return INVALID_LSN;
	}
	LsnType tailPageLsn = lsnFloor(tailLsn);
	LsnType writeNext = m_writeNext;
	LsnType curPage = lsnFloor(writeNext);
	// TODO: ��־��д�̻߳��д����ҳ������������������ǰ����ҳ��
	RWUNLOCK(&m_writeLock, Shared);
	if (curPage == tailPageLsn)  { // ��ǰҳ���Ǵ�ˢдҳ
		RWLOCK(&m_writeLock, Exclusived);
		if (lsnFloor(m_writeNext) == tailPageLsn) { // ��ǰҳ���Ǵ�ˢдҳ
			if (lsnToOffInPage(m_writeNext) > LOG_PAGE_HDR_SIZE) // ��ǰҳ��ǿգ���ǰ������ǰҳ��
				m_writeNext = lsnCeiling(m_writeNext);
		}
		RWUNLOCK(&m_writeLock, Exclusived);
	}
	return tailLsn;
}
/**
 * �õ�ĳ��־��¼ĩβ��LSN
 * @pre д���湲����
 * @param lsn Ŀ����־��¼����ʼlsn
 * @return ����־��¼��ĩβlsn
 */
LsnType LogWriteBuffer::getTailLsn(LsnType lsn) {
	assert(m_writeLock.isLocked(Shared));
	if (lsn <= m_minPageLsn || lsn > m_writeLast)
		return INVALID_LSN; // �����е���־��¼��[m_minLsn, m_writeLast]֮��
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
 * ������־��ȣ�Ԥ�����¼β����LSN
 * @param recordSize ��¼��С,��������¼ͷ
 * @param lsnTail ���������ĩβlsn
 * @return ��¼��Ҫ��ҳ��
 */
size_t LogWriteBuffer::calRecordTailLsn(size_t recordSize, LsnType *lsnTail) {
	size_t pageUsed = lsnToOffInPage(m_writeNext);
	size_t trueSize = recordSize + LOG_RECORD_HDR_SIZE; // ��־���ȣ�����ͷ��
	if (pageUsed + trueSize <= LOG_PAGE_SIZE) { // ��ǰҳ���ܷ��±���¼
		*lsnTail = alignRound(m_writeNext + trueSize);
		return 0;
	}
	size_t remainSize = trueSize - (LOG_PAGE_SIZE - pageUsed); // ռ�굱ǰҳ����ʣ������ֽ�
	size_t pageCnt = (remainSize + PAGE_MAX_PAYLOAD_SIZE - 1) / PAGE_MAX_PAYLOAD_SIZE;
	// ����βLSN��ʱ��Ӧ�ò���ÿҳ�˷ѵĿռ�
	*lsnTail = m_writeNext + trueSize + pageCnt * (LOG_PAGE_SIZE - PAGE_MAX_PAYLOAD_SIZE);
	*lsnTail = alignRound(*lsnTail);
	return pageCnt;
}
/**
 * ��ȡLSN��Ӧ����־ҳ�ڴ�
 * @param lsn ��������־ҳ�е��κ�LSN
 * @return ��־ҳ
 */
LogPageHdr* LogWriteBuffer::lsnToPageAddr(LsnType lsn) {
	LsnType pageLsn = lsnFloor(lsn);
	return (LogPageHdr*)(m_buffer + pageLsn % m_bufferSize);
}
/**
 * ��ȡLSN��Ӧ����־��¼�ڴ�
 * @param lsn ��־��¼lsn
 * @return ��־��¼
 */
LogRecordHdr* LogWriteBuffer::lsnToAddr(LsnType lsn) {
#ifndef NDEBUG
	// ���LSN�Ƿ��������־ҳ��
	LogPageHdr *pageHdr = lsnToPageAddr(lsn);
	assert(lsn > pageHdr->m_startLsn && lsn < pageHdr->m_startLsn + LOG_PAGE_SIZE);
#endif
	return (LogRecordHdr *)(m_buffer + lsn % m_bufferSize);
}
//////////////////////////////////////////////////////////////////////////
/**
 * ����һ����־������
 * @param pool �ڴ�ҳ��
 * @param logFileMngr ��־�ļ�������
 * @param pageCnt ����ҳ��
 * @param ioPages һ��IO��ҳ��
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

/** PagePool�Ļص� */
uint LogReadBuffer::freeSomePages(u16 userId, uint numPages) {
	UNREFERENCED_PARAMETER(userId);
	UNREFERENCED_PARAMETER(numPages);
	assert(false); // �����ҳ����ר�õģ�û�˻�����Ҫҳ
	return 0;
}
/**
 * ��ȡһҳ��ҳ�ϼӶ���
 * @param pageLsn ��־ҳ��LSN
 */
LogPageHdr* LogReadBuffer::getPage(LsnType pageLsn) {
	assert(pageLsn == lsnFloor(pageLsn));
_RESTART:
	RWLOCK(&m_lock, Shared);
	LogPageHdr *page = m_pageHash.get(pageLsn);
	if (page) { // ��ҳ�Ѿ�����
		m_pool->lockPage(0, page, Shared, __FILE__, __LINE__);
		RWUNLOCK(&m_lock, Shared);
	} else {
		RWUNLOCK(&m_lock, Shared);
		if (loadPages(pageLsn, m_ioPages))
			goto _RESTART;
		else // ��ȡҳʧ��
			return 0;
	}
	touchPage(page);
	return page;
}
/** �ͷ�һҳ */
void LogReadBuffer::releasePage(LogPageHdr *pageHdr) {
	m_pool->unlockPage(0, pageHdr, Shared);
}
/** ��ȡһ������ҳ */
LogReadBuffer::PageInfo* LogReadBuffer::getFreePage() {
	assert(m_lock.isLocked(Exclusived));
	void *data = allocPage(0, PAGE_TXNLOG, 0);
	if (!data) { // ��LRU�������滻
		DLink<PageInfo *> *oldest = m_lruList.getHeader()->getNext();
		PageInfo* info = NULL;
		for (; oldest != m_lruList.getHeader(); oldest = oldest->getNext()) {
			info = oldest->get();
			//�����pageδ��latch
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
		//�п���û�п��滻��pageInfo
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
 * �ӻ�����ɾ��.=lsn��������־ҳ
 */
void LogReadBuffer::truncate(LsnType lsn) {
	assert(lsn == lsnFloor(lsn));

	RWLockGuard guard(&m_lock, Exclusived, __FILE__, __LINE__);
	DLink<PageInfo *> *first = m_lruList.getHeader()->getNext();
	DLink<PageInfo *> *cur = first;
	while (cur != m_lruList.getHeader()) {
		DLink<PageInfo *> *next = cur->getNext();
		LsnType pageLsn = cur->get()->m_page->m_startLsn;
		if (pageLsn >= lsn) // �ҵ�һ����ɾ��ҳ
			unloadPage(pageLsn);
		cur = next;
	}
}
/**
 * �Ӷ�����ɾ��һҳ
 * @param pageLsn ��ɾ��ҳ
 */
void LogReadBuffer::unloadPage(LsnType pageLsn) {
	assert(pageLsn == lsnFloor(pageLsn));
	assert(m_lock.isLocked(Exclusived));

	LogPageHdr *page = m_pageHash.remove(pageLsn);
	if (page) {
		m_pool->lockPage(0, page, Exclusived, __FILE__, __LINE__);
		PageInfo* info = (PageInfo *)m_pool->getInfo(page);
		info->m_dlink.unLink(); // LRU����
		freePage(0, page); // �ͷ�ҳ
		m_pageInfoPool.free(info->m_poolEntry); // �ͷ�PageInfo
	}
	assert(!m_pageHash.get(pageLsn));
}
/**
 * �Ӵ����϶�ȡ��־ҳ
 * @return ���������־ĩβ�򷵻�false, ���򷵻�true
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
			if (curPage->m_startLsn == lsn + i * LOG_PAGE_SIZE) { // һ��������ҳ��
				PageInfo *pageInfo = getFreePage();
				if (!pageInfo) {
					goto _ReStart;
				}
				memcpy(pageInfo->m_page, curPage, LOG_PAGE_SIZE);
				if (!m_pageHash.get(pageInfo->m_page->m_startLsn)) // ҳ�治����
					m_pageHash.put(pageInfo->m_page);
				m_lruList.addLast(&pageInfo->m_dlink);
			} else { // ��ǰҳ����Чҳ��
				if (i == 0) // ����ҳ��δʹ��
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
/** ����LRU���� */
void LogReadBuffer::touchPage(LogPageHdr *page) {
	//assert(!m_lock.isLocked(Shared) && !m_lock.isLocked(Exclusived));
	RWLOCK(&m_lock, Exclusived);
	PageInfo* info = (PageInfo *)m_pool->getInfo(page);
	m_lruList.moveToLast(&info->m_dlink);
	RWUNLOCK(&m_lock, Exclusived);
}

//////////////////////////////////////////////////////////////////////////
//// ��־����
//////////////////////////////////////////////////////////////////////////

LogBuffer::LogBuffer(LogReadBuffer *readBuffer, LogWriteBuffer *writeBuffer)
	: m_readBuffer(readBuffer), m_writeBuffer(writeBuffer) {
	m_minLsnWriteBuf = m_writeBuffer->getMinLsn();
}

/**
 * ��ȡ��־ҳ, ҳ�Ӷ���
 * @param ҳLSN
 * @return ҳ��ַ
 */
LogPageHdr* LogBuffer::getPage(LsnType pageLsn) {
	while(true) {
		if (pageLsn < m_minLsnWriteBuf) { // ��ҳ�϶�����д������
			return m_readBuffer->getPage(pageLsn);
		} else {
			LogPageHdr *page;
			if (m_writeBuffer->getPage(pageLsn, &page)) {
				return page; // ��ҳȷʵ��д������
			} else {
				m_minLsnWriteBuf = m_writeBuffer->getMinLsn();
			}
		}
	}

}

/** �ͷ�ҳ�桢������� */
void LogBuffer::releasePage(LogPageHdr *pageHdr) {
	if (pageHdr->m_startLsn < m_minLsnWriteBuf) {
		m_readBuffer->releasePage(pageHdr);
	} else {
		m_writeBuffer->releasePage(pageHdr);
	}
}

//////////////////////////////////////////////////////////////////////////
//// ��־�Ķ���
//////////////////////////////////////////////////////////////////////////
/**
 * ������־�Ķ���
 * @param readBuffer ��־������
 * @param log ϵͳ��־
 */
LogReader::LogReader(LogBuffer *readBuffer, Syslog *log)
	: m_readBuffer(readBuffer), m_log(log) {
}
LogReader::~LogReader() {

}

/**
 * ��ʼɨ�裬��ȡɨ����
 * @param startLsn ɨ����ʼLSN, startLsn����ָ��ĳ����־��¼��ͷ
 * @param endLsn ɨ�����LSN
 * @return ɨ����
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
 * �õ���һ������ȡ��־��¼����ʼLSN
 * @param nextLsn �ϴζ�ȡ��־��¼��ĩβlsn
 * @return ��һ����־��¼����ʼLSN
 */
LsnType LogReader::prepareLsnForRead(LsnType nextLsn) {
	// alignRound���ܻ���nextLsnָ����һҳ������Ƚ��ж��������Ȼ���ж��Ƿ���ҳ��
	nextLsn = alignRound(nextLsn);
	LsnType pageLsn = lsnFloor(nextLsn);
	if (nextLsn < pageLsn + LOG_PAGE_HDR_SIZE) { // ��¼��һ����ҳ��
		assert(nextLsn == pageLsn);
		nextLsn = pageLsn + LOG_PAGE_HDR_SIZE;
	}
	assert(nextLsn == alignRound(nextLsn));
	return nextLsn;
}
/**
 * ��ȡ��һ����־���ŵ�ɨ����
 * @param handle ɨ����
 * @return ���ɨ�赽��־ĩβ������NULL
 */
LogScanHandle* LogReader::getNext(LogScanHandle *handle) const {
	handle->m_logEntry.m_data = handle->m_buf;
	LsnType toRead;
_RESTART:
	toRead = prepareLsnForRead(handle->m_lsnNext);
	if (toRead >= handle->m_endLsn) // �Ѿ�����ĩβ��
		return 0;
	LogPageHdr *pageHdr;
	LsnType pageLsn = lsnFloor(toRead);
	pageHdr = m_readBuffer->getPage(pageLsn);
	if (!pageHdr)
		return 0; // �Ѿ�������־ĩβ��
	if (pageHdr->m_startLsn + pageHdr->m_used <= toRead) { // �Ѿ����굱ǰҳ
		assert(pageHdr->m_startLsn + pageHdr->m_used == toRead);
		handle->m_lsnNext = lsnCeiling(toRead);
		m_readBuffer->releasePage(pageHdr);
		goto _RESTART;
	}
	size_t offset = (size_t)(toRead - pageHdr->m_startLsn);
	LogRecordHdr *recHdr = (LogRecordHdr *)((byte *)pageHdr + offset);
	// �ж���־��¼�Ƿ���ȷ
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
	if (recHdr->m_type == LR_HEAD) { // ����־
		assert(offset + LOG_RECORD_HDR_SIZE + recHdr->m_size == LOG_PAGE_SIZE); // HEAD��¼�ض�������־ҳ
		// ��ȡ��־ʣ�ಿ��
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
/** ����ɨ�裬�ͷž���ڴ� */
void LogReader::endScan(LogScanHandle *handle) const {
	delete handle;
}

/**
 * �õ�[startPageLsn, endPageLsn) ֮��ĵ�һ����������־
 * @param startPageLsn ��ҳLSN
 * @param endPageLsn ĩҳβLSN
 * @return С��LSN���Ҳ���������־��LSN��
 *	��������ڲ���������־��򷵻�INVALID_LSN
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
		bool checkPrevPage = true; // �Ƿ���Ҫ���ǰҳ
		while (pageHdr->m_startLsn + pageHdr->m_used > toRead) {
			toRead = prepareLsnForRead(toRead);
			size_t offset = (size_t)(toRead - pageHdr->m_startLsn);
			LogRecordHdr *recHdr = (LogRecordHdr *)((byte *)pageHdr + offset);
			assert(isLogRecValid(recHdr, pageHdr));
			if (recHdr->m_type == LR_HEAD) {
				incompletePos = toRead; // �ҵ���������־��
				// HEAD��¼�ض�������־ҳ
				assert(offset + LOG_RECORD_HDR_SIZE + recHdr->m_size == LOG_PAGE_SIZE);
				break;
			} else if (recHdr->m_type == LR_TAIL || recHdr->m_type == LR_NORMAL) {
				// ��������־��¼ֻ���ڱ�ҳ
				checkPrevPage = false;
			} else if (recHdr->m_type == LR_CONTINUE) {
				// �ض��ǵ�һ����־��¼
				assert(toRead == prepareLsnForRead(pageLsn));
				// CONTINUE��¼�ض�������־ҳ
				assert(offset + LOG_RECORD_HDR_SIZE + recHdr->m_size == LOG_PAGE_SIZE);
				// �ض����ڲ�������־��ұض��ڵ�ǰҳ��֮ǰ
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
/**  ��ǰ��־�����ʼLSN */
LsnType LogScanHandle::curLsn() const {
	return m_lsn;
}
/** ��ǰ����а�������־�� */
const LogEntry* LogScanHandle::logEntry() const {
	return &m_logEntry;
}
/** ��һ����־����ʼLSN */
LsnType LogScanHandle::nextLsn() const {
	return m_lsnNext;
}
//////////////////////////////////////////////////////////////////////////
const double LogConfig::DEFAULT_BUFFER_HIGH_WATERMARK = 0.4;
/** ��־д���������ܷ���һ����־ */
const size_t LogConfig::MIN_LOG_BUFFER_SIZE = 
	(computeMinlogBufferSize(LogConfig::MAX_LOG_RECORD_SIZE) + (1<<20) - 1) & ~(size_t)((1<<20) - 1);
/** ��־�ļ������ܷ���һ����־ */
const size_t LogConfig::MIN_LOGFILE_SIZE = 
	(computeMinlogFileSize(LogConfig::MAX_LOG_RECORD_SIZE) + (1<<20) - 1) & ~(size_t)((1<<20) - 1);

/** ����Ĭ����־���ò��� */
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
/** ������ò�����ȷ�� */
bool LogConfig::checkConfig() const {
	if (m_readBufIoPages >= m_readBufPageCnt)
		return false;
	return true;
}
//////////////////////////////////////////////////////////////////////////

const LsnType Txnlog::MIN_LSN = lsnCeiling(LOG_CTRL_PAGE_SIZE); /** ��С��LSN */

/** ���캯�� */
Txnlog::Txnlog() {
	init();
}
/** ��ʼ�� */
void Txnlog::init()  {
	m_flusher = 0;
	m_writeBuffer = 0;
	m_fileMngr = 0;
	m_readBuffer = 0;
	m_buffer = 0;
	m_logReader = 0;
	memset(&m_stat, 0, sizeof(m_stat));
}
/** ����������ɶҲ���� */
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
* ��ʼ����־ģ�顣
*
* @param db ���ݿ�
* @param dir ��־�ļ�����Ŀ¼
* @param name ��־�ļ���
* @param numLogFile ��־�ļ���Ŀ, ��־�ļ���Ϊdir/name.0001, dir/name.0002, dir/name.numLogFile
* @param fileSize ��־�ļ���С
* @param bufferSize ��־д�����С
* @param cfg ��־����
* @return ��ʼ����ɵ�������־������
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
 * �����־ĩβ����������־��
 * @param tailLsn ��־ĩβ
 * @return ȥ����������־��֮�����־ĩβ
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
	if (realTailLsn == INVALID_LSN) // û�в���������־��
		return tailLsn;
	assert(realTailLsn >= beginLsn);
	LogFile *logFile = m_fileMngr->findLogFile(realTailLsn);
	assert(logFile);
	m_syslog->log(EL_LOG, "Remove incomplete log entry at "
		I64FORMAT"u, with log tailLSN "I64FORMAT"u"
		, realTailLsn, tailLsn);

	logFile->truncate(realTailLsn, tailLsn);
	// truncateֱ�Ӹ����˴����ϵ�ҳ�棬������ն�����
	m_readBuffer->truncate(lsnFloor(realTailLsn));
	return lsnCeiling(realTailLsn);
}

/** ������־ģ�� */
void Txnlog::startUp() throw (NtseException) {
	m_fileMngr = LogFileMngr::open(this, m_baseName, m_numLogFile);
	reclaimOverflowSpace();
	PagePool *pool = new PagePool(0, LOG_PAGE_SIZE);
	m_readBuffer = new LogReadBuffer(
		pool, m_fileMngr, m_cfg.m_readBufPageCnt, m_cfg.m_readBufIoPages);
	pool->registerUser(m_readBuffer);
	pool->init();
	
	// ��ȡ��־β��
	LsnType tailLsn = findLogTail();
	if (tailLsn == INVALID_LSN) { // �������ݿ�󣬵�һ������
		tailLsn = m_fileMngr->initOnFirstOpen();
	} else {
		assert(tailLsn == lsnFloor(tailLsn));
		if (m_fileMngr->findLogFile(tailLsn)) {
			// tailLsn������־�ļ�ĩβ
			m_fileMngr->setCurLogFile(tailLsn);
			m_fileMngr->curLogFile()->reset(tailLsn);
		} else {
			// ��tailLsn����־�ļ�ĩβ��ʱ��tailLsn��Ӧ����־��¼ʵ�����Ѿ�
			// ��һ���µ���־�ļ��У���Ȼ����־�ļ�����δ���������ǲ����ڵ�!
			// Ϊ�˱����������⣬�ʶ����ð���(tailLsn - 1)���ļ�Ϊ��ǰ�ļ�
			m_fileMngr->setCurLogFile(tailLsn - 1);
		}
	}
	if (m_fileMngr->getLogFileSize() != m_fileSize) {
		NTSE_THROW(NTSE_EC_CORRUPTED_LOGFILE, "Open transaction log error: current log file size is "I64FORMAT"u, but ntse_log_file_size is set "I64FORMAT"u",
			m_fileMngr->getLogFileSize(), (u64)m_fileSize);
	}
	// ȥ����־ĩβ����������־��
	// ��Ϊ��������ı�tailLsn����˱�����reset֮����ñ�����
	// ������ܵ���reset�������־ҳ�治��maxIoPages
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
	
	// ��ʼ��д����
	m_writeBuffer = new LogWriteBuffer(this, tailLsn, m_writeBufPageCnt);
	m_writeBuffer->setHighWatermark(LogConfig::DEFAULT_BUFFER_HIGH_WATERMARK);
	m_buffer = new LogBuffer(m_readBuffer, m_writeBuffer);
	m_logReader = new LogReader(m_buffer, m_syslog);
	// ��ʼ����־ˢ���߳�
	m_flusher = new LogFlusher(this, m_cfg.m_flushInterval);
	m_flusher->start();
}

/**
 * �õ�ָ����־���͵��ַ�����ʾ
 *
 * @param logType ��־����
 * @return ��ʾ��־���͵��ַ�������
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
 * ������־�ļ�
 *
 * @param db ���ݿ�
 * @param dir ��־�ļ�����Ŀ¼
 * @param name ��־�ļ���
 * @param fileSize ��־�ļ���С
 * @throw NtseException ������־�ļ�������ʧ��
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
 * ���´�����־�ļ�
 *
 * @param db ���ݿ�
 * @param dir ��־�ļ�����Ŀ¼
 * @param name ��־�ļ���
 * @param fileSize ��־�ļ���С
 * @param fileCnt ��־�ļ�����
 * @param startLsn ��־��ʼLSN��һ��Ϊԭ�м���LSN
 * @return ʵ�ʵ���־��ʼLSN
 * @throw NtseException ������־�ļ�������ʧ��
 */
LsnType Txnlog::recreate(const char *dir, const char *name
						, uint fileCnt , uint fileSize, LsnType startLsn) throw(NtseException) {
	assert(fileSize > LOG_CTRL_PAGE_SIZE);
	fileSize = LOG_CTRL_PAGE_SIZE + (uint)lsnCeiling(fileSize - LOG_CTRL_PAGE_SIZE);
	return LogFileMngr::recreate((std::string(dir) + NTSE_PATH_SEP + name).c_str(), fileCnt, fileSize, startLsn);
}
/**
 * ɾ����־�ļ�
 * @param dir ��־�ļ�����Ŀ¼
 * @param name ��־�ļ���
 * @param numLogFile ��־�ļ�����
 */
void Txnlog::drop(const char *dir, const char *name, uint numLogFile) throw(NtseException) {
	return LogFileMngr::drop((std::string(dir) + NTSE_PATH_SEP + name).c_str(), numLogFile);
}
/** ��дlsn��lsn֮ǰ����־�� */
void Txnlog::flush(LsnType lsn, FlushSource fs)  {
	m_flusher->flush(lsn, fs);
}
/** ��ȡ��־ģ��ͳ����Ϣ */
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
 * ��ȡ��־�ռ�ռ����
 * @return 0��1֮�䣬��־ռ����
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
 * дһ����־
 *
 * @param txnId ����ID��Ϊ0��ʾ����������
 * @param logType ��־����
 * @param tableId ��ID���������������־ָ��Ϊ0
 * @param data ��־����
 * @param size ��־���ݴ�С
 * @return ��־LSN
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
 * дһ����־
 *
 * @param txnId ����ID��Ϊ0��ʾ����������
 * @param logType ��־����
 * @param tableId ��ID���������������־ָ��Ϊ0
 * @param data ��־����
 * @param size ��־���ݴ�С
 * @param targetLsn ����������־LSN
 * @return ��־LSN
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
* �ر���־ģ��
* @param flushLog �Ƿ��д��־
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
/** ��ȡ���һ����־��LSN */
LsnType Txnlog::lastLsn() const {
	return m_writeBuffer->getLastWrittenLsn();
}
/** ��ȡ��־β�� */
LsnType Txnlog::tailLsn() const {
	return m_writeBuffer->getWriteNextLsn();
}
/**
 * ���ݿ�����ʱ��ȷ����־��ĩβLSN
 * @return ���һ����־��¼��ĩβ����һ����־��¼���ܵ�LSN
 */
LsnType Txnlog::findLogTail() throw (NtseException) {
	// ��ȡ����LSN������LSNΪ0��ʾû����������
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
	do { // �ҵ���һ�����ܲ�������־�ļ�
		logFile = m_fileMngr->findLogFile(restartLsn);
		if (!logFile) {
			if (lastCheckPointLsn == 0 && restartLsn == Txnlog::MIN_LSN) // ��δд����־
				return INVALID_LSN;
			if (restartLsn != 0 && m_fileMngr->findLogFile(restartLsn - 1))
				// restartLsn������־�ļ�ĩβ��ͬʱ����־ĩβ��
				// ��ʱrestartLsn����tailLsn
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
 * ������־ɨ����
 *
 * @param startLsn ��ʼLSN
 * @param endLsn ����LSN(������endLsn)
 * @return ɨ����
 */
LogScanHandle* Txnlog::beginScan(LsnType startLsn, LsnType endLsn) const {
	startLsn = max(startLsn, m_fileMngr->getOnlineLsn());
	return m_logReader->beginScan(startLsn, endLsn);
}

#ifdef TNT_ENGINE
/** �ṩ�޸�handleֵ�ĺ���������undoʱ��Ҫ�����λlogEntry�������
 * @param handle ɨ����
 * @param startLsn ��ʼ��lsn
 * @param endLsn   ��ֹ��lsn
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
 * ��ȡ��һ����־���ŵ�ɨ����
 * @param handle ɨ����
 * @return ���ɨ�赽��־ĩβ������NULL
 */
LogScanHandle* Txnlog::getNext(LogScanHandle *handle) const {
	if (!m_logReader->getNext(handle))
		return NULL;

	if (handle->m_logEntry.m_logType < LOG_CPST_MAX
		&& handle->m_logEntry.m_logType > LOG_CPST_MIN) { // ������־���ͣ���Ҫ��ȡĿ��LSN
		assert(handle->m_logEntry.m_size >= sizeof(LsnType));
		handle->m_logEntry.m_cpstForLsn = *(LsnType *)handle->m_logEntry.m_data;
		handle->m_logEntry.m_data += sizeof(LsnType);
		handle->m_logEntry.m_size -= sizeof(LsnType);
	} else {
		handle->m_logEntry.m_cpstForLsn = INVALID_LSN;
	}
	return handle;
}

/** ����ɨ��, �ͷ�ɨ���� */
void Txnlog::endScan(LogScanHandle *handle) const {
	return m_logReader->endScan(handle);
}


/** �ض���־lsn��lsn֮���������־, ���ñ�����ʱ�벻Ҫ����������־���� */
void Txnlog::truncate(LsnType lsn) {
#ifdef TNT_ENGINE
	if (m_tntDb != NULL) {
		assert(lsn >= m_tntDb->getTNTControlFile()->getDumpLsn());
	}
#endif
	assert(lsn >= m_db->getControlFile()->getCheckpointLSN());
	if (lsn >= tailLsn())
		return;
	// ��ͣflush�߳�
	delete m_flusher;
	//1.��մ�������
	LogFile *logFile = m_fileMngr->findLogFile(lsn);
	LsnType endLsn = logFile->getEndLsn();
	// 1.1 ��յ�ǰ��־�ļ�֮���������־�ļ�
	while (NULL != (logFile = m_fileMngr->findLogFile(endLsn))) {
		endLsn = logFile->getEndLsn();
		logFile->initLogfile();
	}
	// 1.2 truncate��ǰ��־�ļ�
	LogFile *curLogFile = m_fileMngr->findLogFile(lsn);
	curLogFile->truncate(lsn);
	m_fileMngr->setCurLogFile(lsn);
	getSyslog()->log(EL_LOG, "Truncate log, lsn: "I64FORMAT"u", lsn);
	//2.��ն�����
	delete m_logReader;
	delete m_readBuffer;
	PagePool *pool = new PagePool(0, LOG_PAGE_SIZE);
	m_readBuffer = new LogReadBuffer(pool, m_fileMngr, m_cfg.m_readBufPageCnt, m_cfg.m_readBufIoPages);
	pool->registerUser(m_readBuffer);
	pool->init();
	//3.���д����
	delete m_writeBuffer;
	m_writeBuffer =  new LogWriteBuffer(this, lsnCeiling(lsn), m_writeBufPageCnt);
	m_writeBuffer->setHighWatermark(LogConfig::DEFAULT_BUFFER_HIGH_WATERMARK);

	delete m_buffer;
	m_buffer = new LogBuffer(m_readBuffer, m_writeBuffer);
	m_logReader = new LogReader(m_buffer, m_syslog);

	//4.����flush�߳�
	m_flusher = new LogFlusher(this, m_cfg.m_flushInterval);
	m_flusher->start();
}

/** ��ȡϵͳ��־���� */
Syslog* Txnlog::getSyslog() const {
	return m_syslog;
}
/**
 * ������־ͳ����Ϣ����д��
 * @param pageFlushed ��д��ҳ��
 */
void Txnlog::updateFlushStats(size_t pageFlushed) {
	++m_stat.m_flushCnt;
	m_stat.m_flushedPages += pageFlushed;
}
/**
 * ������־ͳ����Ϣ
 * @param bytesWritten д����־���ֽ���
 */
void Txnlog::updateWriteStats(size_t bytesWritten) {
	++m_stat.m_writeCnt;
	m_stat.m_writeSize += bytesWritten;
}

/**����flush��־ͳ����Ϣ
 *@param fs flush��Դ
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

/** ��ȡ���ݿ���� */
Database* Txnlog::getDatabase() const {
	return m_db;
}
/**
 * ��־��д�߳̿���
 * @param enabled �Ƿ���
 */
void Txnlog::enableFlusher(bool enabled) {
	m_flusher->enable(enabled);
}

/**
 * ���������־�ļ�
 * (open/closeʱ��checkpoint֮�����)
 */
void Txnlog::reclaimOverflowSpace() {
	uint logFileCntHwm;
	logFileCntHwm = m_db->getConfig()->m_logFileCntHwm;
	return m_fileMngr->reclaimOverflowSpace(logFileCntHwm);
}
/**
 * ������С������־lsn�� ��־ģ�鱣֤lsn֮�����־���ܹ���ȡ
 * @param lsn ��С����lsn
 * @return ���³ɹ�����һ�����������ʧ�ܷ���ֵС��0
 */
int Txnlog::setOnlineLsn(LsnType lsn) {
	assert(lsn <= tailLsn());
	return m_fileMngr->setOnlineLsn(lsn);
}

/**
 * ���onlineLsn����
 *
 * @param token setOnlineLsn���ص�token
 */
void Txnlog::clearOnlineLsn(int token) {
	return m_fileMngr->clearOnlineLsn(token);
}

/**
 * ���ü���LSN
 * @param lsn �µļ���Lsn
 */
void Txnlog::setCheckpointLsn(LsnType lsn)  {
	m_fileMngr->setCheckpointLsn(lsn);
	reclaimOverflowSpace();
}

/**
* �ַ���ת����LogType
* @param s �����ַ���

* @return logType  �����־����
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
 * ����LogBackuper
 * @param txnlog	������־
 * @param startLsn	�������(������Ϊ�ѱ��ݿ����ļ��еļ���LSN)
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
 * ������־�����ļ����ȣ��ֽڣ�
 * ����ĳ��ȱ���С����ʵ�ı����ļ�����
 */
u64 LogBackuper::getSize() {
	LsnType tailLsn = m_txnlog->tailLsn();
	if (Txnlog::MIN_LSN == tailLsn) // ѹ��ûд����־
		return LOG_PAGE_SIZE;
	return lsnFloor(tailLsn) - m_startLsn + LOG_PAGE_SIZE;
}
/**
 * ����־�������ȡ��־ҳ
 * @param buf ҳ����
 * @param pageCnt ����ȡҳ��
 * @return ������ȡ��ҳ��
 */
uint LogBackuper::readPagesRB(byte* buf, uint pageCnt) throw(NtseException) {
	assert(m_nextLsn < m_minLsnWriteBuf);
	// �����ܴ���־�������ȡ��ҳ��
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
 * ��ȡ��־����
 * @param buf ����ռ�
 * @param pageCnt ����ҳ��
 * @param writeDisabled �Ƿ�ر������ݿ�д����;
 *	����ر���д���������ȡ������ǰ��־ҳ���ڵ�������־����
 * @return ʵ�ʶ�ȡ��ҳ��
 */
uint LogBackuper::getPages(byte* buf, uint pageCnt, bool writeDisabled) throw(NtseException) {
	if (m_firstTime) { // ��һ�ε��ã����ر��ݿ���ҳ��
		LogBackupCtrlPage *ctrlPage = (LogBackupCtrlPage *)buf;
		memset(ctrlPage, 0, LOG_PAGE_SIZE);
		ctrlPage->m_size = m_txnlog->m_fileSize;
		ctrlPage->m_startLsn = m_nextLsn;
		ctrlPage->m_checksum = checksum64(buf + sizeof(u64), LOG_PAGE_SIZE - sizeof(u64));
		m_firstTime = false;
		return 1;
	}
	if (m_nextLsn < m_minLsnWriteBuf) { // ����ȡҳ�治��д������
		return readPagesRB(buf, pageCnt);
	} else {
		while (true) {
			m_minLsnWriteBuf = m_writeBuffer->getMinLsn();
			if (m_nextLsn < m_minLsnWriteBuf) { // ��־ҳ�ڶ�������
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
					return cnt; // ��־ҳȷʵ��д������
				}
			}
		}
	}
}
//////////////////////////////////////////////////////////////////////////
/**
 * ����ָ���
 * @param ctrlFile �����ļ�
 * @param path ��־�ļ�·������������׺
 */
LogRestorer::LogRestorer(ControlFile *ctrlFile, const char *path)
	: m_path(System::strdup(path)), m_fileId(0), m_ctrlFile(ctrlFile)
	, m_logFile(0), m_firstTime(true), m_failed(false) {
	m_nextLsn = INVALID_LSN;
	m_ctrlPage = (LogBackupCtrlPage *)new byte[LOG_PAGE_SIZE];
}

LogRestorer::~LogRestorer() {
	if (m_failed) { // �ָ�ʧ��
		if (m_logFile)
			m_logFile->close();
		// ����Ѵ����ļ�
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
			// û����־ʱ��ɶҲ����
			// ���ݿ�openʱ���ᴴ����־�ļ�
			assert(!m_logFile);
		}
	}

	delete [] m_path;
	delete [] (byte *)m_ctrlPage;
}
/**
 * ���ݱ��ݻָ���־�ļ�
 * @param buf ������־���ݻ���
 * @param pageCnt ��־����ҳ��
 */
void LogRestorer::doSendPages(const byte *buf, uint pageCnt) throw(NtseException) {
	assert(!m_failed);
	if (m_firstTime) { // ��һ�ε��ñ�����
		// ��ȡ���ݿ���ҳ
		memcpy(m_ctrlPage, buf, LOG_PAGE_SIZE);
		if (m_ctrlPage->m_checksum != checksum64(buf + sizeof(u64), LOG_PAGE_SIZE - sizeof(u64)))
			NTSE_THROW(NTSE_EC_INVALID_BACKUP, "Log backup control page has invalid checksum");

		--pageCnt;
		buf += LOG_PAGE_SIZE;
		m_firstTime = false;
		m_nextLsn = m_ctrlPage->m_startLsn;
	}
	do {
		// �жϵ�ǰ��־�ļ��Ƿ������ɲ�����־
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
		// ���ҳ�����ȷ��
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
 * ���ݱ��ݻָ���־�ļ�
 * @param buf ������־���ݻ���
 * @param pageCnt ��־����ҳ��
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
/** lsnת����ҳ��ƫ�� */
size_t lsnToOffInPage(LsnType lsn) {
	return (size_t)(lsn & (LOG_PAGE_SIZE - 1));
}
/** lsn��ȡ����ҳ��߽� */
LsnType lsnCeiling(LsnType lsn) {
	return (lsn + (LOG_PAGE_SIZE - 1)) & ~((LsnType)LOG_PAGE_SIZE - 1);
}
/** lsn��ȡ����ҳ��߽� */
LsnType lsnFloor(LsnType lsn) {
	return lsn & ~((LsnType)LOG_PAGE_SIZE - 1);
}

/** 
 * �ܹ�����ָ��������־����С��־д����
 * @param logSize ��־����
 * @return ��С��־д�����С
 */
static size_t computeMinlogBufferSize(size_t logSize) {
	return LOG_PAGE_SIZE + (size_t)((u64)logSize * LOG_PAGE_SIZE / PAGE_MAX_PAYLOAD_SIZE);
}

/** 
 * �ܹ�����ָ��������־����С��־�ļ�����
 * @param logSize ��־����
 * @return ��С��־�ļ�����
 */
static size_t computeMinlogFileSize(size_t logSize) {
	return LOG_CTRL_PAGE_SIZE + computeMinlogBufferSize(logSize);
}


#ifndef NDEBUG
/** �����־��¼����Ч�� */
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

/** �ж���־��¼ͷ�Ƿ���ͬ */
bool isLogRecMetaEq(const LogEntry *e, const LogRecordHdr *rec) {
	return e->m_logType == rec->m_logType && e->m_tableId == rec->m_tableId && e->m_txnId == rec->m_txnId;
}
#endif // NDEBUG
/** ������־��¼��ʼ��LogEntry */
void setLogEntryHdr(LogEntry *e, const LogRecordHdr *rec) {
	e->m_logType = (LogType)rec->m_logType;
	e->m_size = rec->m_size;
	e->m_tableId = rec->m_tableId;
	e->m_txnId = rec->m_txnId;
}


/** д��LogRecordHdr��Ϣ��TRACE����
 * @param tracer	ָ����trace
 * @param recHdr	��־��¼����
 * @return ����trace���ڼ�������
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
