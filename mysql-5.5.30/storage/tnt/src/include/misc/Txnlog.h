/**
 * ������־����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
/** LSN ���� */
typedef u64 LsnType;
/** ��־���� */
enum LogType {
	LOG_CREATE_TABLE,	/** CREATE TABLE���� */
	LOG_DROP_TABLE,		/** DROP TABLE���� */
	LOG_TRUNCATE,		/** TRUNCATE���� */
	LOG_RENAME_TABLE,	/** RENAME TABLE���� */
	LOG_BUMP_FLUSHLSN,	/** ���ñ��flushLsn���� */
	LOG_ADD_INDEX,		/** ������������ */
	LOG_HEAP_INSERT,	/** ����в���һ����¼ */
	LOG_HEAP_UPDATE,	/** ���и���һ����¼ */
	LOG_HEAP_DELETE,	/** ����ɾ��һ����¼ */
	LOG_IDX_DROP_INDEX,	/** ����ָ������ */
	LOG_IDX_DML,		/** ������ֵ���롢ɾ����Append��Ӳ��� */
	LOG_IDX_SMO,		/** ����SMO���� */
	LOG_IDX_SET_PAGE,	/** ����ҳ���޸Ĳ��� */
	LOG_IDX_CREATE_BEGIN,	/** ��ʼ�������� */
	LOG_IDX_CREATE_END,	/** ������������ */
	LOG_IDX_DML_BEGIN,	/** ��ʼ����DML�޸Ĳ��� */
	LOG_IDX_DMLDONE_IDXNO,	/** ��һ��DML�������У���־ĳ��������DML������� */
	LOG_IDX_DIU_DONE,	/** ��־��Ψһ���������²������У�ɾ������ִ�гɹ�����־ */
	LOG_IDX_DML_END,	/** ��������DML�޸Ĳ��� */
	LOG_TXN_END,		/** ���������־ */
	LOG_TXN_START,		/** ����ʼ��־ */
	LOG_MMS_UPDATE,		/** MMS������־ */
	LOG_LOB_INSERT,		/** ���ʹ���������־ */
	LOG_LOB_UPDATE,		/** ���ʹ���������־ */
	LOG_LOB_DELETE,		/** ���ʹ����ɾ����־ */
	LOG_LOB_MOVE,		/** ���ʹ�����ƶ���־ */
	LOG_PRE_UPDATE,		/** Ԥ������־ */
	LOG_PRE_UPDATE_HEAP,/** ׼������MMS���¼���� */
	LOG_PRE_DELETE,		/** Ԥɾ����־ */
	LOG_ALTER_TABLE_ARG,/** ������޸���־ */
	LOG_ALTER_INDICE,	/** �޸�������־ */
	LOG_ALTER_COLUMN,	/** �޸��ж�����־ */
	LOG_CREATE_DICTIONARY,/** ����ȫ���ֵ���־ */
	LOG_PRE_LOB_DELETE,  /** TNT��������Ԥɾ���������־	*/

	// ���涨��ģ���TNT���������ӵ���־����
	LOG_TNT_MIN = 100,
	TNT_BEGIN_TRANS, /** ����ʼ */
	TNT_COMMIT_TRANS,       /** �����ύ */
	TNT_BEGIN_ROLLBACK_TRANS,/** ����ʼ�ع� */
	TNT_END_ROLLBACK_TRANS,  /** ��������ع� */
	TNT_PREPARE_TRANS,      /** ����Ԥ�ύ */

	TNT_PARTIAL_BEGIN_ROLLBACK, /**����ʼ���ع�*/
	TNT_PARTIAL_END_ROLLBACK,   /**����������ع�*/
	LOG_TNT_TRX_MAX,  /** ������־���ֵ */

	TNT_U_I_LOG,            /** �״θ��¼�¼ */
	TNT_U_U_LOG,            /** ���״θ��¼�¼ */
	TNT_D_I_LOG,            /** �״��Ƴ���¼ */
	TNT_D_U_LOG,            /** ���״��Ƴ���¼ */
	TNT_UNDO_I_LOG,         /** �����¼ */
	TNT_UNDO_LOB_LOG,       /** �������� */

	TNT_BEGIN_PURGE_LOG,       /** purge��ʼ */
	TNT_PURGE_BEGIN_FIR_PHASE, /** ��purge�ĵ�һ�׶ο�ʼ */
	TNT_PURGE_BEGIN_SEC_PHASE, /** ��purge�ĵڶ��׶ο�ʼ */
	TNT_PURGE_END_HEAP,        /** ��purge�Ľ��� */
	TNT_END_PURGE_LOG,         /** purge���� */

	LOG_TNT_MAX,
	//TNT��־����

	LOG_CPST_MIN,		/** ��С�Ĳ�����־		*/
	/** ������־���Ͷ�д������ */
	LOG_IDX_DML_CPST,	/** ������ֵ���롢ɾ����Append�����Ĳ�����־ */
	LOG_IDX_SMO_CPST,	/** ����SMO����������־ */
	LOG_IDX_SET_PAGE_CPST,	/** ����ҳ���޸Ĳ���������־ */
	LOG_IDX_ADD_INDEX_CPST,	/** ��������������־ */

	LOG_CPST_MAX,		/** ���Ĳ�����־		*/
	LOG_MAX			/** �����־����		*/

	
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

/** ��־ͳ����Ϣ */
struct LogStatus {
	u64	m_flushCnt;		/** д���̴���		*/
	u64 m_flushedPages; /** д����ҳ��		*/
	u64 m_writeCnt;		/** д��־����		*/
	u64 m_writeSize;	/** д��־�ֽ���	*/
	LsnType m_tailLsn;	/** ��־β			*/
	LsnType m_startLsn;	/** ��ʼLSN(��СLSN)*/
	LsnType m_ckptLsn;	/** ����			*/
#ifdef TNT_ENGINE
	LsnType m_dumpLsn;  /** tnt dump Lsn*/
	u64 m_ntseLogSize;  /** ntse��־���ֽ��� */
	u64 m_tntLogSize;   /** tnt��־�ֽ��� */
	u64 m_flushPaddingSize;  /** flush pageʱ�����pageδ����������Ҫpadding���ֽ��� */
#endif
	LsnType m_flushedLsn;/** �ѳ־û���LSN	*/
	u64     m_flush_single_write_cnt;  /** д����ҳ�����𴥷�flush log�Ĵ��� */
	u64     m_flush_batch_write_cnt;   /** д���ҳ�����𴥷�flush log�Ĵ��� */
	u64     m_flush_commit_cnt;        /** commit���𴥷�flush log�Ĵ��� */
	u64     m_flush_prepare_cnt;       /** prepare���𴥷�flush log�Ĵ��� */
	u64     m_flush_rollback_cnt;      /** rollback���𴥷�flush log�Ĵ��� */
	u64     m_flush_purge_cnt;         /** purge���𴥷�flush log�Ĵ��� */
	u64     m_flush_check_point_cnt;   /** check point���𴥷�flush log�Ĵ��� */
	u64     m_flush_ntse_create_index_cnt; /** create index���𴥷�flush log�Ĵ��� */
};

/** ��־�� */
struct LogEntry {
	u16		m_txnId;		/** txnId ����ID��Ϊ0��ʾ����������	*/
	u16		m_tableId;		/** ��ID							*/
	LogType m_logType;		/** ��־����						*/
	byte	*m_data;		/** ��־����						*/
	size_t	m_size;			/** ��־���ݳ���					*/
#ifdef TNT_ENGINE
	union {
		LsnType m_lsn;     /** ��������log��lsn*/
#endif
		LsnType	m_cpstForLsn;	/** ��������־LSN, ������־ʱ����Ч */
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
/** ��־ģ��������Ϣ */
struct LogConfig {
	/** ��־ҳ��С(�ֽ���) */
#ifdef TNT_ENGINE
	static const size_t LOG_PAGE_SIZE = 512;
#else
	static const size_t LOG_PAGE_SIZE = Limits::PAGE_SIZE;
#endif
	/** ��־��¼�������ޣ������ĳ������16M��*/
	static const size_t MAX_LOG_RECORD_SIZE = 20 * 1024 * 1024;
	/** ��С��־���泤�� */
	static const size_t MIN_LOG_BUFFER_SIZE;
	/** ��С��־�ļ����� */
	static const size_t MIN_LOGFILE_SIZE;


	/** Ĭ����־�ļ����� */
	static const uint DEFAULT_NUM_LOGS = 2;
	/** ��־Ĭ��flushʱ��������λΪ���� */
	static const uint DEFAULT_FLUSH_INTERVAL = 1000;

	/** д��־�����ˮ�ߣ�����ռ���ʳ�����ˮ��ʱ��Ӧ������д�߳� */
	static const double DEFAULT_BUFFER_HIGH_WATERMARK;

	/** ��־βָ������ܹ�����ҳ�� */
	static const uint DEFAULT_LOG_TAILHINT_ACCURACY = 2048;
	/** Ĭ����־�ļ����� */
	static const u32 DEFAULT_NUM_TXNLOGS = 2;
	/** ����־�����С����λΪҳ */
	static const u32 DEFAULT_READ_BUFFER_PAGES = 256;
	/** ����־����һ�ζ�ȡ��ҳ�� */
	static const u32 DEFAULT_READ_BUFFER_IO_PAGES = 64;

	static const u32 DEFAULT_MAX_IO_PAGES = Buffer::BATCH_IO_SIZE / LOG_PAGE_SIZE;

	LogConfig();
	bool checkConfig() const;

	uint	m_readBufPageCnt;	/** ��־������ҳ��				*/
	uint	m_readBufIoPages;	/** һ��������ȡ����־ҳ����	*/
	uint	m_tailHintAcc;		/** ��־βָ�뾫ȷ��			*/
	uint	m_flushInterval;	/** ��д��־ʱ����			*/
};

/** ������־���� */
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
	/** ��СLSN */
	static const LsnType MIN_LSN;
	/** ���LSN */
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
	char	*m_baseName;		/** ��־�ļ���ǰ׺					*/
	size_t	m_fileSize;			/** ��־�ļ���С					*/
	uint	m_numLogFile;		/** ��־�ļ�����					*/
	uint	m_writeBufPageCnt;	/** ��־�ļ������С(д����ҳ��)	*/

	LogStatus m_stat;
#ifdef TNT_ENGINE
	TNTDatabase     *m_tntDb;
#endif
	Database        *m_db;
	Syslog          *m_syslog;
	LogWriteBuffer	*m_writeBuffer; /** д����				*/
	LogFileMngr		*m_fileMngr;	/** ��־�ļ�������		*/
	LogFlusher		*m_flusher;		/** ��־��д�߳�		*/
	LogReadBuffer	*m_readBuffer;	/** ��־������			*/
	LogReader		*m_logReader;	/** ��־�Ķ���			*/
	LogBuffer		*m_buffer;		/** ��־��д����		*/

	LogConfig m_cfg;				/** ����������Ϣ		*/
};

/**
 * ��־ɨ����
 * TODO: �������Կ��ǻ��漸��ҳ����ɨ�����У����ɨ��Ч��
 */
class LogScanHandle {
	friend class LogReader;
	friend class Txnlog;
public:

	/** ��ȡ��ǰ��־�� */
	const LogEntry* logEntry() const;
	/** ��ǰ��־��¼��LSN */
	LsnType curLsn() const;

private:
	LogScanHandle() {}
	/** ��ǰ��־��¼ĩβ����һ����־��¼����LSN */
	LsnType nextLsn() const;

private:
	LogEntry	m_logEntry; /** ��ǰ��־�� */
	LsnType		m_lsn;		/** ��ǰ��־��LSN */
	LsnType		m_lsnNext;	/** ��ǰ��־���ĩβ����һ����־���ܵ�LSN */
	LsnType		m_endLsn;	/** ����LSN */
	byte		m_buf[LogConfig::MAX_LOG_RECORD_SIZE];	/** ��־��¼���� */
};
class LogBackupCtrlPage;
/** ��־������ */
class LogBackuper {
public:
	LogBackuper(Txnlog *txnlog, LsnType startLsn);
	u64 getSize();
	uint getPages(byte* buf, uint pageCnt, bool writeDisabled = false) throw(NtseException);
private:
	uint readPagesRB(byte* buf, uint pageCnt) throw(NtseException);
private:
	bool			m_firstTime;		/** ��һ�ε���getPages				*/
	LsnType			m_nextLsn;			/** ��һ������ȡ����־ҳ			*/
	LsnType			m_startLsn;			/** ��ʼ���ݵ���־��				*/
	LsnType			m_minLsnWriteBuf;	/** ��־д�������СLSN������ȷ��	*/
	Txnlog			*m_txnlog;
	LogReadBuffer	*m_readBuffer;
	LogWriteBuffer	*m_writeBuffer;
};
class LogFile;
class ControlFile;
/** ��־�ָ��� */
class LogRestorer {
public:
	LogRestorer(ControlFile *ctrlFile, const char *path);
	~LogRestorer();
	void sendPages(const byte *buf, uint pageCnt) throw(NtseException);
private:
	void doSendPages(const byte *buf, uint pageCnt) throw(NtseException);
private:
	char		*m_path;		/** ��־��Ŀ¼				*/
	uint		m_fileId;		/** ��ǰ��־�ļ���			*/
	ControlFile *m_ctrlFile;	/** �����ļ�				*/
	LsnType		m_nextLsn;		/** ��һ����д�����־ҳ	*/
	LogFile		*m_logFile;		/** ��ǰ��־�ļ�����		*/
	bool		m_firstTime;	/** ��һ�ε���sendPages		*/
	LogBackupCtrlPage	*m_ctrlPage;/** �����ļ�����ҳ		*/
	bool		m_failed;		/** �ָ��������Ƿ����		*/
};

}

#endif
