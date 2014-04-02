/**
 * 事务日志管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TXNLOG_H_
#define _NTSE_TXNLOG_H_

#include "Global.h"
#include "misc/Buffer.h"
#include <stddef.h> // for size_t
#include <istream>

#ifdef TNT_ENGINE
namespace tnt {
class TNTDatabase;
class TNTControlFile;
}
using namespace tnt;
#endif

namespace ntse {
/** LSN 类型 */
typedef u64 LsnType;
/** 日志类型 */
enum LogType {
	LOG_CREATE_TABLE,	/** CREATE TABLE操作 */
	LOG_DROP_TABLE,		/** DROP TABLE操作 */
	LOG_TRUNCATE,		/** TRUNCATE操作 */
	LOG_RENAME_TABLE,	/** RENAME TABLE操作 */
	LOG_BUMP_FLUSHLSN,	/** 设置表的flushLsn操作 */
	LOG_ADD_INDEX,		/** 创建索引操作 */
	LOG_HEAP_INSERT,	/** 向堆中插入一条记录 */
	LOG_HEAP_UPDATE,	/** 堆中更新一条记录 */
	LOG_HEAP_DELETE,	/** 堆中删除一条记录 */
	LOG_IDX_DROP_INDEX,	/** 丢弃指定索引 */
	LOG_IDX_DML,		/** 索引键值插入、删除和Append添加操作 */
	LOG_IDX_SMO,		/** 索引SMO操作 */
	LOG_IDX_SET_PAGE,	/** 索引页面修改操作 */
	LOG_IDX_CREATE_BEGIN,	/** 开始索引创建 */
	LOG_IDX_CREATE_END,	/** 结束索引创建 */
	LOG_IDX_DML_BEGIN,	/** 开始索引DML修改操作 */
	LOG_IDX_DMLDONE_IDXNO,	/** 在一个DML操作当中，标志某个索引的DML操作完成 */
	LOG_IDX_DIU_DONE,	/** 标志非唯一性索引更新操作当中，删除操作执行成功的日志 */
	LOG_IDX_DML_END,	/** 结束索引DML修改操作 */
	LOG_TXN_END,		/** 事务结束日志 */
	LOG_TXN_START,		/** 事务开始日志 */
	LOG_MMS_UPDATE,		/** MMS更新日志 */
	LOG_LOB_INSERT,		/** 大型大对象插入日志 */
	LOG_LOB_UPDATE,		/** 大型大对象更新日志 */
	LOG_LOB_DELETE,		/** 大型大对象删除日志 */
	LOG_LOB_MOVE,		/** 大型大对象移动日志 */
	LOG_PRE_UPDATE,		/** 预更新日志 */
	LOG_PRE_UPDATE_HEAP,/** 准备更新MMS脏记录到堆 */
	LOG_PRE_DELETE,		/** 预删除日志 */
	LOG_ALTER_TABLE_ARG,/** 表参数修改日志 */
	LOG_ALTER_INDICE,	/** 修改索引日志 */
	LOG_ALTER_COLUMN,	/** 修改列定义日志 */
	LOG_CREATE_DICTIONARY,/** 创建全局字典日志 */
	LOG_PRE_LOB_DELETE,  /** TNT引擎增加预删除大对象日志	*/

	// 下面定义的，是TNT引擎所增加的日志类型
	LOG_TNT_MIN = 100,
	TNT_BEGIN_TRANS, /** 事务开始 */
	TNT_COMMIT_TRANS,       /** 事务提交 */
	TNT_BEGIN_ROLLBACK_TRANS,/** 事务开始回滚 */
	TNT_END_ROLLBACK_TRANS,  /** 事务结束回滚 */
	TNT_PREPARE_TRANS,      /** 事务预提交 */

	TNT_PARTIAL_BEGIN_ROLLBACK, /**事务开始语句回滚*/
	TNT_PARTIAL_END_ROLLBACK,   /**事务结束语句回滚*/
	LOG_TNT_TRX_MAX,  /** 事务日志最大值 */

	TNT_U_I_LOG,            /** 首次更新记录 */
	TNT_U_U_LOG,            /** 非首次更新记录 */
	TNT_D_I_LOG,            /** 首次移除记录 */
	TNT_D_U_LOG,            /** 非首次移除记录 */
	TNT_UNDO_I_LOG,         /** 插入记录 */
	TNT_UNDO_LOB_LOG,       /** 插入大对象 */

	TNT_BEGIN_PURGE_LOG,       /** purge开始 */
	TNT_PURGE_BEGIN_FIR_PHASE, /** 表purge的第一阶段开始 */
	TNT_PURGE_BEGIN_SEC_PHASE, /** 表purge的第二阶段开始 */
	TNT_PURGE_END_HEAP,        /** 表purge的结束 */
	TNT_END_PURGE_LOG,         /** purge结束 */

	LOG_TNT_MAX,
	//TNT日志结束

	LOG_CPST_MIN,		/** 最小的补偿日志		*/
	/** 补偿日志类型都写在这里 */
	LOG_IDX_DML_CPST,	/** 索引键值插入、删除和Append操作的补偿日志 */
	LOG_IDX_SMO_CPST,	/** 索引SMO操作补偿日志 */
	LOG_IDX_SET_PAGE_CPST,	/** 索引页面修改操作补偿日志 */
	LOG_IDX_ADD_INDEX_CPST,	/** 创建索引补偿日志 */

	LOG_CPST_MAX,		/** 最大的补偿日志		*/
	LOG_MAX			/** 最大日志类型		*/

	
};

enum FlushSource {
	FS_IGNORE,
	FS_SINGLE_WRITE,
	FS_BATCH_WRITE,
	FS_COMMIT,
	FS_PREPARE,
	FS_ROLLBACK,
	FS_PURGE,
	FS_CHECK_POINT,
	FS_NTSE_CREATE_INDEX
};

struct Tracer;
Tracer& operator << (Tracer &tracer, LogType type);
std::istream& operator >> (std::istream &is, LogType &type);

/** 日志统计信息 */
struct LogStatus {
	u64	m_flushCnt;		/** 写磁盘次数		*/
	u64 m_flushedPages; /** 写出总页数		*/
	u64 m_writeCnt;		/** 写日志次数		*/
	u64 m_writeSize;	/** 写日志字节数	*/
	LsnType m_tailLsn;	/** 日志尾			*/
	LsnType m_startLsn;	/** 起始LSN(最小LSN)*/
	LsnType m_ckptLsn;	/** 检查点			*/
#ifdef TNT_ENGINE
	LsnType m_dumpLsn;  /** tnt dump Lsn*/
	u64 m_ntseLogSize;  /** ntse日志的字节数 */
	u64 m_tntLogSize;   /** tnt日志字节数 */
	u64 m_flushPaddingSize;  /** flush page时，如果page未被填满，需要padding的字节数 */
#endif
	LsnType m_flushedLsn;/** 已持久化的LSN	*/
	u64     m_flush_single_write_cnt;  /** 写单个页面引起触发flush log的次数 */
	u64     m_flush_batch_write_cnt;   /** 写多个页面引起触发flush log的次数 */
	u64     m_flush_commit_cnt;        /** commit引起触发flush log的次数 */
	u64     m_flush_prepare_cnt;       /** prepare引起触发flush log的次数 */
	u64     m_flush_rollback_cnt;      /** rollback引起触发flush log的次数 */
	u64     m_flush_purge_cnt;         /** purge引起触发flush log的次数 */
	u64     m_flush_check_point_cnt;   /** check point引起触发flush log的次数 */
	u64     m_flush_ntse_create_index_cnt; /** create index引起触发flush log的次数 */
};

/** 日志项 */
struct LogEntry {
	u16		m_txnId;		/** txnId 事务ID，为0表示不在事务中	*/
	u16		m_tableId;		/** 表ID							*/
	LogType m_logType;		/** 日志类型						*/
	byte	*m_data;		/** 日志内容						*/
	size_t	m_size;			/** 日志内容长度					*/
#ifdef TNT_ENGINE
	union {
		LsnType m_lsn;     /** 用来描述log的lsn*/
#endif
		LsnType	m_cpstForLsn;	/** 待补偿日志LSN, 补偿日志时才有效 */
#ifdef TNT_ENGINE
	};
	inline bool isTNTLog() const {
		return m_logType > LOG_TNT_MIN && m_logType < LOG_TNT_MAX;
	}
#endif
};

class Syslog;
class Database;
class LogReader;
class LogScanHandle;
class LogBuffer;
class LogReadBuffer;
class LogWriteBuffer;
class LogFileMngr;
class LogFlusher;
class LogBackuper;
class LogRestorer;
class Txnlog;
/** 日志模块配置信息 */
struct LogConfig {
	/** 日志页大小(字节数) */
#ifdef TNT_ENGINE
	static const size_t LOG_PAGE_SIZE = 512;
#else
	static const size_t LOG_PAGE_SIZE = Limits::PAGE_SIZE;
#endif
	/** 日志记录长度上限（大对象的长度最多16M）*/
	static const size_t MAX_LOG_RECORD_SIZE = 20 * 1024 * 1024;
	/** 最小日志缓存长度 */
	static const size_t MIN_LOG_BUFFER_SIZE;
	/** 最小日志文件长度 */
	static const size_t MIN_LOGFILE_SIZE;


	/** 默认日志文件个数 */
	static const uint DEFAULT_NUM_LOGS = 2;
	/** 日志默认flush时间间隔，单位为毫秒 */
	static const uint DEFAULT_FLUSH_INTERVAL = 1000;

	/** 写日志缓存高水线，缓存占用率超出高水线时，应启动回写线程 */
	static const double DEFAULT_BUFFER_HIGH_WATERMARK;

	/** 日志尾指针最多能够误差的页数 */
	static const uint DEFAULT_LOG_TAILHINT_ACCURACY = 2048;
	/** 默认日志文件个数 */
	static const u32 DEFAULT_NUM_TXNLOGS = 2;
	/** 读日志缓存大小，单位为页 */
	static const u32 DEFAULT_READ_BUFFER_PAGES = 256;
	/** 读日志缓存一次读取的页数 */
	static const u32 DEFAULT_READ_BUFFER_IO_PAGES = 64;

	static const u32 DEFAULT_MAX_IO_PAGES = Buffer::BATCH_IO_SIZE / LOG_PAGE_SIZE;

	LogConfig();
	bool checkConfig() const;

	uint	m_readBufPageCnt;	/** 日志读缓存页数				*/
	uint	m_readBufIoPages;	/** 一次性最多读取的日志页面数	*/
	uint	m_tailHintAcc;		/** 日志尾指针精确度			*/
	uint	m_flushInterval;	/** 回写日志时间间隔			*/
};

/** 事务日志管理 */
class Txnlog {
	friend class LogFlusher;
	friend class LogFileMngr;
	friend class LogWriteBuffer;
	friend class LogBackuper;
public:
#ifdef TNT_ENGINE
	static Txnlog* open(TNTDatabase *db, const char *dir, const char *name,
		uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg = 0) throw(NtseException);
#endif
	static Txnlog* open(Database *db, const char *dir, const char *name,
		uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg = 0) throw(NtseException);
#ifdef TNT_ENGINE
	static void create(const char *dir, const char *name, uint fileSize, uint numFile)  throw(NtseException);
#else
	static void create(const char *dir, const char *name, uint fileSize)  throw(NtseException);
#endif
	static LsnType recreate(const char *dir, const char *name
		, uint numLogFile, uint fileSize, LsnType startLsn)  throw(NtseException);
	static void drop(const char *dir, const char *name, uint numLogFile) throw(NtseException);
	void close(bool flushLog = true);

	LsnType log(u16 txnId, LogType logType, u16 tableId, const byte *data, size_t size);
	LsnType logCpst(u16 txnId, LogType logType, u16 tableId, const byte *data, size_t size, LsnType targetLsn);

	void flush(LsnType lsn, FlushSource fs = FS_IGNORE);

	LsnType lastLsn() const;
	LsnType tailLsn() const;

	LogScanHandle* beginScan(LsnType startLsn, LsnType endLsn) const;
#ifdef TNT_ENGINE
	void resetLogScanHandle(LogScanHandle *handle, LsnType startLsn, LsnType endLsn);
#endif
	LogScanHandle* getNext(LogScanHandle *handle) const;
	void endScan(LogScanHandle *handle) const;

	int setOnlineLsn(LsnType lsn);
	void clearOnlineLsn(int token);
	void setCheckpointLsn(LsnType lsn);

	Syslog* getSyslog() const;
	Database* getDatabase() const;
	const LogStatus& getStatus();
	double getUsedRatio() const;

	void enableFlusher(bool enabled);
	void reclaimOverflowSpace();

	static const char* getLogTypeStr(LogType logType);
	static LogType parseLogType(const char* s);
	void truncate(LsnType lsn);

public:
	/** 最小LSN */
	static const LsnType MIN_LSN;
	/** 最大LSN */
	static const LsnType MAX_LSN = (LsnType)-1;
private:
	Txnlog();
	~Txnlog();
	void init();
#ifdef TNT_ENGINE
	bool init(TNTDatabase *db, const char *dir, const char *name, uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg);
#endif
	bool init(Database *db, const char *dir, const char *name, uint numLogFile, uint fileSize, uint bufferSize, const LogConfig *cfg);
	LsnType findLogTail() throw (NtseException);
	void startUp() throw (NtseException);

	LsnType removeIncompleteLogEntry(LsnType tailLsn);
	void updateFlushStats(size_t pageFlushed);
	void updateWriteStats(size_t bytesWritten);
	void updateFlushSourceStats(FlushSource fs);
	void updateFlushRunCnt(u64 incrCnt = 1);

private:
	char	*m_baseName;		/** 日志文件名前缀					*/
	size_t	m_fileSize;			/** 日志文件大小					*/
	uint	m_numLogFile;		/** 日志文件个数					*/
	uint	m_writeBufPageCnt;	/** 日志文件缓存大小(写缓存页数)	*/

	LogStatus m_stat;
#ifdef TNT_ENGINE
	TNTDatabase     *m_tntDb;
#endif
	Database        *m_db;
	Syslog          *m_syslog;
	LogWriteBuffer	*m_writeBuffer; /** 写缓存				*/
	LogFileMngr		*m_fileMngr;	/** 日志文件管理器		*/
	LogFlusher		*m_flusher;		/** 日志回写线程		*/
	LogReadBuffer	*m_readBuffer;	/** 日志读缓存			*/
	LogReader		*m_logReader;	/** 日志阅读器			*/
	LogBuffer		*m_buffer;		/** 日志读写缓存		*/

	LogConfig m_cfg;				/** 参数配置信息		*/
};

/**
 * 日志扫描句柄
 * TODO: 将来可以考虑缓存几个页面在扫描句柄中，提高扫描效率
 */
class LogScanHandle {
	friend class LogReader;
	friend class Txnlog;
public:

	/** 获取当前日志项 */
	const LogEntry* logEntry() const;
	/** 当前日志记录的LSN */
	LsnType curLsn() const;

private:
	LogScanHandle() {}
	/** 当前日志记录末尾，下一条日志记录可能LSN */
	LsnType nextLsn() const;

private:
	LogEntry	m_logEntry; /** 当前日志项 */
	LsnType		m_lsn;		/** 当前日志项LSN */
	LsnType		m_lsnNext;	/** 当前日志项的末尾，下一条日志可能的LSN */
	LsnType		m_endLsn;	/** 结束LSN */
	byte		m_buf[LogConfig::MAX_LOG_RECORD_SIZE];	/** 日志记录缓存 */
};
class LogBackupCtrlPage;
/** 日志备份者 */
class LogBackuper {
public:
	LogBackuper(Txnlog *txnlog, LsnType startLsn);
	u64 getSize();
	uint getPages(byte* buf, uint pageCnt, bool writeDisabled = false) throw(NtseException);
private:
	uint readPagesRB(byte* buf, uint pageCnt) throw(NtseException);
private:
	bool			m_firstTime;		/** 第一次调用getPages				*/
	LsnType			m_nextLsn;			/** 下一个待读取的日志页			*/
	LsnType			m_startLsn;			/** 开始备份的日志点				*/
	LsnType			m_minLsnWriteBuf;	/** 日志写缓存的最小LSN（不精确）	*/
	Txnlog			*m_txnlog;
	LogReadBuffer	*m_readBuffer;
	LogWriteBuffer	*m_writeBuffer;
};
class LogFile;
class ControlFile;
/** 日志恢复者 */
class LogRestorer {
public:
	LogRestorer(ControlFile *ctrlFile, const char *path);
	~LogRestorer();
	void sendPages(const byte *buf, uint pageCnt) throw(NtseException);
private:
	void doSendPages(const byte *buf, uint pageCnt) throw(NtseException);
private:
	char		*m_path;		/** 日志根目录				*/
	uint		m_fileId;		/** 当前日志文件号			*/
	ControlFile *m_ctrlFile;	/** 控制文件				*/
	LsnType		m_nextLsn;		/** 下一个待写入的日志页	*/
	LogFile		*m_logFile;		/** 当前日志文件对象		*/
	bool		m_firstTime;	/** 第一次调用sendPages		*/
	LogBackupCtrlPage	*m_ctrlPage;/** 备份文件控制页		*/
	bool		m_failed;		/** 恢复过程中是否出错		*/
};

}

#endif
