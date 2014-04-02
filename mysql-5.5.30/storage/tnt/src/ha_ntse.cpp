#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#ifdef NTSE_KEYVALUE_SERVER
#include "keyvalue/KeyValueServer.h"
#endif

#define MYSQL_SERVER 1
#include <vector>
#include <sstream>
#ifdef WIN32
#include "mysql_priv.h"
#include "ha_ntse.h"
#include <mysql/plugin.h>
#endif
#include "api/Database.h"
#include "misc/Parser.h"
#include "misc/Session.h"
#include "misc/Syslog.h"
#include "misc/RecordHelper.h"
#include "misc/Record.h"
#include "misc/ColumnGroupParser.h"
#include "btree/Index.h"
#include "util/SmartPtr.h"
#include "util/File.h"
#include "misc/GlobalFactory.h"
#include "misc/Global.h"
#include "misc/ParFileParser.h"

#ifndef WIN32
#include "mysql_priv.h"
#include "ha_ntse.h"
#include <mysql/plugin.h>
#endif
#include "ntse_binlog.h"
#include "mysys_err.h"
#include "RowCache.h"
#include "ntse_version.h"

#ifdef NTSE_PROFILE
#include "misc/Profile.h"
#endif

using namespace ntse;

class AutoIncrement {
public:
	AutoIncrement(u64 autoIncr) : m_mutex("AutoIncrement mutex", __FILE__, __LINE__) { m_autoIncr = (autoIncr == 0)? 1 : autoIncr; }

	void setAutoIncr(u64 autoIncr) {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		m_autoIncr = (autoIncr == 0)? 1 : autoIncr;
	}

	void setAutoIncrIfBigger(u64 autoIncr) {
		assert(m_mutex.isLocked());
		if (autoIncr > m_autoIncr)
			m_autoIncr = autoIncr;
	}

	u64 getAutoIncr() {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		return m_autoIncr;
	}

	u64 reserve() {
		m_mutex.lock(__FILE__, __LINE__);
		return m_autoIncr;
	}

	void unreserve() {
		m_mutex.unlock();
	}

private:
	void lock() {
		m_mutex.lock(__FILE__, __LINE__);
	}

	void unlock() {
		m_mutex.unlock();
	}

private:
	u64				m_autoIncr;	/** 表所包含的自增字段值 */
	ntse::Mutex			m_mutex;	/** 保护m_autoIncr改变的互斥锁 */
};

/** 使用中的表的额外信息 */
struct TableInfoEx {
	char			*m_path;	/** 表路径，没有转换目录分隔符 */
	uint			m_refCnt;	/** 引用次数 */
	THR_LOCK		m_lock;		/** MySQL用此实现表锁 */
	Table			*m_table;	/** NTSE内部表对象 */
	AutoIncrement	m_autoIncr;	/** 管理表的自增对象 */

public:
	TableInfoEx(const char *path, Table *table) : m_autoIncr(0) {
		m_path = System::strdup(path);
		m_table = table;
		thr_lock_init(&m_lock);
		m_refCnt = 0;
	}

	~TableInfoEx() {
		delete []m_path;
		thr_lock_delete(&m_lock);
		m_path = NULL;
		m_table = NULL;
	}
};

/** 比较路径与TableInfoEx是否相等的函数对象 */
class InfoExEqualer {
public:
	inline bool operator()(const char *path, const TableInfoEx *infoEx) const {
		return strcmp(path, infoEx->m_path) == 0;
	}
};

/** 计算TableInfoEx对象哈希值的函数对象 */
class InfoExHasher {
public:
	inline unsigned int operator()(const TableInfoEx *infoEx) const {
		return m_strHasher.operator ()(infoEx->m_path);
	}

private:
	Hasher<char *>	m_strHasher;
};

handlerton		*ntse_hton;			/** Handler singleton */
static Config	ntse_config;		/** 数据库配置 */
Database		*ntse_db = NULL;			/** 全局唯一的数据库 */
NTSEBinlog		*ntse_binlog = NULL;		/** 全局唯一的binlog记录器 */
CmdExecutor		*cmd_executor = NULL;		/** 全局唯一的命令执行器 */
typedef DynHash<const char *, TableInfoEx *, InfoExHasher, Hasher<const char *>, InfoExEqualer> TblHash;
static TblHash	openTables;			/** 已经打开的表 */
static ntse::Mutex	openLock("openLock", __FILE__, __LINE__);			/** 保护已经打开的表的锁 */

HandlerInfo *HandlerInfo::m_instance = NULL;
HandlerInfo *handlerInfo = NULL;
static bool isNtseLogBin = false;

#ifdef NTSE_KEYVALUE_SERVER
KeyValueServer	*keyvalueServer = NULL;	/** keyvalue服务端线程 */
static ThriftConfig keyvalueConfig;	/** keyvalue服务端配置 */
#endif

///////////////////////////////////////////////////////////////////////////////
// 配置参数与状态变量 //////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
// 以下是全局配置，参见Config类说明
static char	*ntse_tmpdir = (char *)".";
static char	*ntse_version = NULL;
static ulong	ntse_log_file_size = 0;
static ulong	ntse_log_buf_size = 0;
static unsigned long long ntse_page_buf_size = 0;
static unsigned long long ntse_rec_buf_size = 0;
static ulong	ntse_max_sessions = 0;
static ulong	ntse_table_lock_timeout = 0;
static ulong	ntse_checkpoint_interval = 0;
ulong			ntse_sample_max_pages = 1000;
static ulong	ntse_incr_size;	/** 堆、索引或大对象文件扩展页面数 */
static ulong	ntse_binlog_buffer_size = 10 * 1024 * 1024;	/** binlog允许使用缓存的最大大小 */
static char 	*ntse_binlog_method = (char *)"direct";	/** 设置ntse写binlog的方式,包括"mysql"表示by_mysql/"direct"表示direct/"cached"表示cached三种 */
#ifdef NTSE_PROFILE
static int 		ntse_profile_summary = 0; /** 通过设置-1/0/1 来控制全局Profile */
static int		ntse_thread_profile_autorun = 0; /** 通过设置0/1 来控制线程Profile是否自动开启*/
#endif
static char		ntse_backup_before_recover = 0;
static char		ntse_verify_after_recover = 0;
static char		ntse_enable_mms_cache_update = 0;
static char		ntse_enable_syslog = 1;	/** NTSE的log是否输出到mysqld.log */
static char		ntse_directio = 1; /** 是否使用directio */

static void set_buf_page_from_para(uint *pages, const unsigned long long *para){
	unsigned long long bufSize = *para;
	if (sizeof(unsigned long int) == 4) {
		if (bufSize > (0xFFFFFFFFL)) {
			*pages = 1024;
		} else {
			*pages = (u32) bufSize / NTSE_PAGE_SIZE;
		}	
	} else {
		*pages = (u32)(bufSize / NTSE_PAGE_SIZE);
	}
}
#ifdef NTSE_KEYVALUE_SERVER
static int	ntse_keyvalue_port = 0;			/** keyvalue服务的端口 */
static int	ntse_keyvalue_servertype = 0;	/** keyvalue服务类型 */
static int	ntse_keyvalue_threadnum = 0;	/** 当keyvalue服务需要线程时，设置线程数目 */
#endif
static void update_page_buf_size(MYSQL_THD thd, struct st_mysql_sys_var *var,
								 void *var_ptr, const void *save) {
	uint pageBufSize = 0;
	set_buf_page_from_para(&pageBufSize,(unsigned long long*)save);
	if (ntse_db->getPageBuffer()->setTargetSize((uint)pageBufSize)) {
		*((unsigned long long*)var_ptr) = *(unsigned long long*)save;
		ntse_db->getConfig()->m_pageBufSize = pageBufSize;
	} else {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "%s", "Update ntse_page_buf_size failed, the value is too large.");
	}
}


static void update_rec_buf_size(MYSQL_THD thd, struct st_mysql_sys_var *var,
								void *var_ptr, const void *save) {
	uint recBufSize = 0;
	set_buf_page_from_para(&recBufSize,(unsigned long long*)save);
	if (ntse_db->getMms()->setTargetSize((uint)recBufSize)) {
		*((unsigned long long*)var_ptr) = *(unsigned long long*)save;
		ntse_db->getConfig()->m_mmsSize = recBufSize;
	} else {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "%s", "Update ntse_rec_buf_size failed, the value is too large.");
	}
}


static void update_table_lock_timeout(MYSQL_THD thd, struct st_mysql_sys_var *var,
								void *var_ptr, const void *save) {
	ulong lockTimeout = *((ulong *)save);
	*((ulong *)var_ptr) = lockTimeout;
	ntse_db->getConfig()->m_tlTimeout = lockTimeout;
}

static void update_checkpoint_interval(MYSQL_THD thd, struct st_mysql_sys_var *var,
								void *var_ptr, const void *save) {
	ulong value = *((ulong *)save);
	*((ulong *)var_ptr) = value;
	ntse_db->getConfig()->m_chkptInterval = value;
	ntse_db->setCheckpointInterval((uint)value);
}


#ifdef NTSE_PROFILE
static void update_ntse_profile_summary(MYSQL_THD thd, struct st_mysql_sys_var *var,
								void *var_ptr, const void *save){
	int ctrlValue = *((int *)save);
	*((int *)var_ptr) = ctrlValue;
	switch (ctrlValue) {
		case -1:
			g_profiler.shutdownGlobalClean();
			break;
		case 0:
			g_profiler.shutdownGlobalKeep();
			break;
		case 1:
			g_profiler.openGlobalProfile();
			break;
		default:
			break;
	}
}
static void update_ntse_thread_profile_autorun(MYSQL_THD thd, struct st_mysql_sys_var *var,
								void *var_ptr, const void *save){
	int ctrlValue = *((int *)save);
	*((int *)var_ptr) = ctrlValue;
	switch (ctrlValue) {
		case 0:
			g_profiler.setThreadProfileAutorun(false);
			break;
		case 1:
			g_profiler.setThreadProfileAutorun(true);
			break;
		default:
			break;
	}
}
#endif

/** ntse_command配置检查函数，检查命令及调用次序是否正确
 *
 * @param thd 发起命令的线程
 * @param var ntse_command配置
 * @param save 若通过检查，允许设置时，将用于接下来用于调用更新函数的值存储于此，
 *    内存必须在线程私有存储中分配，使得语句结束时自动释放
 * @param value 用户指定的值
 * @return 通过检查，允许设置时返回0，否则返回1
 */
static int check_command(MYSQL_THD thd, struct st_mysql_sys_var *var,
						 void *save, struct st_mysql_value *value) {
	THDInfo *thdInfo = checkTHDInfo(thd);
	int len = 0;
	const char *cmd = value->val_str(value, NULL, &len);
	// 保证备份相关命令按顺序调用
	if (Parser::isCommand(cmd, "finishing", "backup", "and", "lock", NULL) ||
		Parser::isCommand(cmd, "end", "backup", NULL)) {
		if (!thdInfo->m_pendingOper || thdInfo->m_pendingOpType != POT_BACKUP) {
			my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "Backup process not started yet");
			return 1;
		}
		BackupProcess *backup = (BackupProcess *)thdInfo->m_pendingOper;
		assert(backup->m_stat == BS_FAILED || backup->m_stat == BS_COPYED || backup->m_stat == BS_LOCKED);
		if (Parser::isCommand(cmd, "finishing", "backup", "and", "lock", NULL)) {
			if (backup->m_stat == BS_FAILED) {
				my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "Backup has failed, you should use 'end backup' to end it");
				return 1;
			}
			if (backup->m_stat == BS_LOCKED) {
				my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "Database has already been locked, you should use 'end backup' to end the backup process");
				return 1;
			}
		}
		if (Parser::isCommand(cmd, "end", "backup", NULL)) {
			if (backup->m_stat == BS_COPYED) {
				my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "Use 'finishing backup and lock' first");
				return 1;
			}
		}
	} else if (thdInfo->m_pendingOper) {
		my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "A pending operation is executing");
		return 1;
	}

	*((char **)save) = thd_strmake(thd, cmd, len);
	return 0;
}

static void update_command(MYSQL_THD thd, struct st_mysql_sys_var *var,
	void *var_ptr, const void *save) {
	// 这段代码参考update_func_str函数
	char *old = *((char **)var_ptr);
	if (old) {
		my_free(old, MYF(0));
		*((char **)var_ptr) = NULL;
	}
	char *cmd = *((char **)save);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_cmdInfo->setCommand(cmd);
	cmd_executor->doCommand(thdInfo, thdInfo->m_cmdInfo);

	if (thdInfo->m_cmdInfo->getStatus() == CS_FAILED) {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_ERROR, ER_UNKNOWN_ERROR, "%s", thdInfo->m_cmdInfo->getInfo());
		// 这里没法用my_printf_error，因为本函数无法返回操作失败，因此上层在执行结束时会assert出错标志位
		// 设置，而调用了my_printf_error后就设置了标志位
		//my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), thdInfo->m_cmdInfo->getInfo());
	}

	*((char **)var_ptr) = my_strdup(*(char **) save, MYF(0));
}

/** 对于ULONG等数值类型的定义，最后一个参数是blk_size，其具体含义应该是：
 * 无论外部设置的参数N是多少，都会转化为N - N % blk_size作为传入的参数值
 * 如果不希望受到这种“对齐”的影响，可以设置为0
 */
static MYSQL_SYSVAR_STR(version, ntse_version,
						PLUGIN_VAR_READONLY,
						"NTSE version",
						NULL, NULL, NULL);

static MYSQL_SYSVAR_STR(tmpdir, ntse_tmpdir,
						PLUGIN_VAR_READONLY,
						"The directory to store temparory files used by NTSE (used only in index building currently).",
						NULL, NULL, NULL);

static MYSQL_SYSVAR_ULONG(log_file_size, ntse_log_file_size,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Size of log file in bytes.",
						  NULL, NULL, LogConfig::MIN_LOGFILE_SIZE, LogConfig::MIN_LOGFILE_SIZE, ~0, 0);

static MYSQL_SYSVAR_ULONG(log_buf_size, ntse_log_buf_size,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Size of log buffer in bytes.",
						  NULL, NULL, LogConfig::MIN_LOG_BUFFER_SIZE, LogConfig::MIN_LOG_BUFFER_SIZE, ~0, 0);

static MYSQL_SYSVAR_ULONGLONG(page_buf_size, ntse_page_buf_size,
						  PLUGIN_VAR_RQCMDARG,
						  "Size of page buffer in bytes.",
						  NULL, &update_page_buf_size, 1024 * NTSE_PAGE_SIZE, 64 * NTSE_PAGE_SIZE, ~0, NTSE_PAGE_SIZE);

static MYSQL_SYSVAR_ULONGLONG(rec_buf_size, ntse_rec_buf_size,
						  PLUGIN_VAR_RQCMDARG,
						  "Size of record buffer in bytes.",
						  NULL, &update_rec_buf_size, 1024 * NTSE_PAGE_SIZE, 64 * NTSE_PAGE_SIZE, ~0, NTSE_PAGE_SIZE);

static MYSQL_SYSVAR_ULONG(max_sessions, ntse_max_sessions,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Max number of concurrent sessions.",
						  NULL, NULL, 1024, 108, 10000, 0);

static MYSQL_SYSVAR_ULONG(table_lock_timeout, ntse_table_lock_timeout,
						  PLUGIN_VAR_RQCMDARG,
						  "Timeout of table lock in seconds.",
						  NULL, &update_table_lock_timeout, 5, 0, 10000, 0);

static MYSQL_SYSVAR_ULONG(checkpoint_interval, ntse_checkpoint_interval,
						  PLUGIN_VAR_RQCMDARG,
						  "Checkpoint interval in seconds.",
						  NULL, &update_checkpoint_interval, 900, 5, 24 * 3600, 0);


static MYSQL_SYSVAR_ULONG(sample_max_pages, ntse_sample_max_pages,
						  PLUGIN_VAR_RQCMDARG,
						  "Maximal number of pages used in sampling in obtaining extended status.",
						  NULL, NULL, 1000, 1, 10000, 0);

static MYSQL_THDVAR_BOOL(accurate_tblscan, PLUGIN_VAR_OPCMDARG,
						 "Table scan should produce accurate result. If set to false, table scan will skip record cache, thus may be inaccurate but much faster.",
						 NULL, NULL, TRUE);

static MYSQL_THDVAR_STR(command, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
						"Used to send command to NTSE kernel.",
						&check_command, &update_command, NULL);

static MYSQL_SYSVAR_ULONG(incr_size, ntse_incr_size,
						  PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
						  "Number of pages when heap/index/lob file extends.",
						  NULL, NULL, 1024, TableDef::MIN_INCR_SIZE, TableDef::MAX_INCR_SIZE, 0);

static MYSQL_THDVAR_ULONG(deferred_read_cache_size, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
						 "Max size of cache used in deferred read, used mainly in filesort.",
						 NULL, NULL, 1024 * 1024, 32 * 1024, 1024 * 1024 * 1024, 32 * 1024);

static MYSQL_SYSVAR_BOOL(backup_before_recover, ntse_backup_before_recover,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Backup database before recovery.",
						  NULL, NULL, true);

static MYSQL_SYSVAR_BOOL(verify_after_recover, ntse_verify_after_recover,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Vefiry database after crash recovery.",
						  NULL, NULL, true);

static MYSQL_SYSVAR_BOOL(enable_mms_cache_update, ntse_enable_mms_cache_update,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Enable experimental mms cache update feature.",
						  NULL, NULL, false);

static MYSQL_SYSVAR_ULONG(binlog_buffer_size, ntse_binlog_buffer_size,
						 PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						 "Max size of buffer used to buffer ntse binlog.",
						 NULL, NULL, 10 * 1024 * 1024, 8 * 1024 * 1024, 32 * 1024 * 1024, 0);

static MYSQL_SYSVAR_STR(binlog_method, ntse_binlog_method,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Used to decide the way of writing ntse binlog(\"mysql\"/ \"direct\" / \"cached\").",
						  NULL, NULL, NULL);

static MYSQL_SYSVAR_BOOL(enable_syslog, ntse_enable_syslog,
						 PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						 "write ntse syslog to mysql log",
						 NULL, NULL, false);

static MYSQL_SYSVAR_BOOL(directio, ntse_directio,
						 PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						 "flush data use O_DIRECT or not",
						 NULL, NULL, true);

#ifdef NTSE_KEYVALUE_SERVER
static MYSQL_SYSVAR_INT(keyvalue_port, ntse_keyvalue_port,
						PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						"Port of keyvalue thrift server.",
						NULL, NULL, 9090, 1024, 65535, 0);
static MYSQL_SYSVAR_INT(keyvalue_servertype, ntse_keyvalue_servertype,
						PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						"Type of keyvalue thrift server.",
						NULL, NULL, 1, 0, 10, 0);
static MYSQL_SYSVAR_INT(keyvalue_threadnum, ntse_keyvalue_threadnum,
						PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						"The thread number in keyvalue thrift server.",
						NULL, NULL, 10, 0, 65535, 0);
#endif


/** 
 * ntse_compress_sample_strategy私有连接配置参数检查函数 
 */
static int check_compress_sample_strategy(MYSQL_THD thd, struct st_mysql_sys_var *var, 
										  void *save, struct st_mysql_value *value) {
	int len = 0;
	const char *strategy = value->val_str(value, NULL, &len);
	if (System::stricmp(strategy, "sequence") 
		&& System::stricmp(strategy, "parted")
		&& System::stricmp(strategy, "discrete")) {
		my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "Sample strategy should be 'sequence' or 'parted' or 'discrete'");
		return 1;
	}
	*((char **)save) = thd_strmake(thd, strategy, len);
	return 0;
}

/** 
 * ntse_compress_sample_strategy私有连接配置参数更新函数 
 */
static void update_compress_sample_strategy(MYSQL_THD thd, struct st_mysql_sys_var *var,
											void *var_ptr, const void *save) {
	char *old = *((char **)var_ptr);
	if (old) {
		my_free(old, MYF(0));
		*((char **)var_ptr) = NULL;
	}
	char *strategy = *((char **)save);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->setCompressSampleStrategy(strategy);

	*((char **)var_ptr) = my_strdup(*(char **) save, MYF(0));
}

static void update_compress_sample_pct(MYSQL_THD thd, struct st_mysql_sys_var *var, 
									   void *var_ptr, const void *save) {
	uint pct = *((uint*)save);
	assert(pct <= 100);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_tblSmplPct = (u8)pct;

	*((uint*)var_ptr) = pct;
}

static void update_compress_batch_del_size(MYSQL_THD thd, struct st_mysql_sys_var *var,			
										   void *var_ptr, const void *save) {
	uint delSize = *((uint*)save);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_smplTrieBatchDelSize = delSize;

	*((uint*)var_ptr) = delSize;
}

static void update_compress_cte(MYSQL_THD thd, struct st_mysql_sys_var *var,			
								void *var_ptr, const void *save) {
	uint cte = *((uint*)save);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_smplTrieCte = cte;

	*((uint*)var_ptr) = cte;
}

static void update_compress_win_size(MYSQL_THD thd, struct st_mysql_sys_var *var,	
									 void *var_ptr, const void *save) { 
	uint winSize = *((uint*)save);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_tblSmplWinSize = winSize;

	*((uint*)var_ptr) = winSize;
}

static void update_compress_win_detect_times(MYSQL_THD thd, struct st_mysql_sys_var *var,
											 void *var_ptr, const void *save) { 
	uint detectTimes = *((uint*)save);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_tblSmplWinDetectTimes = detectTimes;

	*((uint*)var_ptr) = detectTimes;
}

static void update_compress_win_mem_level(MYSQL_THD thd, struct st_mysql_sys_var *var,	
										  void *var_ptr, const void *save) { 
	uint memLevel = *((uint*)save);

	THDInfo *thdInfo = checkTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_tblSmplWinMemLevel = memLevel;

	*((uint*)var_ptr) = memLevel;
}

static MYSQL_THDVAR_STR(compress_sample_strategy, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
						"Strategy of sample table used to create compression dictionary.",
						&check_compress_sample_strategy, &update_compress_sample_strategy, "sequence");

static MYSQL_THDVAR_UINT(compress_sample_pct, PLUGIN_VAR_OPCMDARG, 
						 "Percentage of records to sample when create global dictionary",
						 NULL, &update_compress_sample_pct, 10, 1, 100, 0);

static MYSQL_THDVAR_UINT(compress_batch_del_size, PLUGIN_VAR_OPCMDARG,
						 "Num of pages to sample when create a global dictionary",
						 NULL, &update_compress_batch_del_size, 32768, 1, 1024 * 1024, 0);

static MYSQL_THDVAR_UINT(compress_cte, PLUGIN_VAR_OPCMDARG,
						 "Num of pages to sample when create a global dictionary",
						 NULL, &update_compress_cte, 8, 1, 64, 0);

static MYSQL_THDVAR_UINT(compress_win_size, PLUGIN_VAR_OPCMDARG,
						 "Num of pages to sample when create a global dictionary",
						 NULL, &update_compress_win_size, 256 * 1024, 1024, 256 * 1024 * 8, 0);

static MYSQL_THDVAR_UINT(compress_win_detect_times, PLUGIN_VAR_OPCMDARG,
						 "Num of pages to sample when create a global dictionary",
						 NULL, &update_compress_win_detect_times, 2, 1, 8, 0);

static MYSQL_THDVAR_UINT(compress_win_mem_level, PLUGIN_VAR_OPCMDARG,
						 "Num of pages to sample when create a global dictionary",
						 NULL, &update_compress_win_mem_level, 12, 10, 14, 0);


static MYSQL_THDVAR_BOOL(keep_dict_on_optimize, PLUGIN_VAR_OPCMDARG,
						"Whether keep old dictionary when optimize table, can be on/true or off/false. " \
						"It's only useful to a row-compressed table that has a compressed dictionary.",
						NULL, NULL, TRUE);

#ifdef NTSE_PROFILE
static MYSQL_SYSVAR_INT(profile_summary, ntse_profile_summary,
						PLUGIN_VAR_RQCMDARG,
						"Global Profile open shutdown control parameter.",
						NULL, &update_ntse_profile_summary, 0, -1, 1, 1);
static MYSQL_SYSVAR_INT(thread_profile_autorun, ntse_thread_profile_autorun,
						PLUGIN_VAR_RQCMDARG,
						"Thread profile autorun control parameter.",
						NULL, &update_ntse_thread_profile_autorun, 0, 0, 1, 1);
#endif

static struct st_mysql_sys_var* ntseVariables[] = {
	MYSQL_SYSVAR(version),
	MYSQL_SYSVAR(tmpdir),
	MYSQL_SYSVAR(log_file_size),
	MYSQL_SYSVAR(log_buf_size),
	MYSQL_SYSVAR(page_buf_size),
	MYSQL_SYSVAR(rec_buf_size),
	MYSQL_SYSVAR(max_sessions),
	MYSQL_SYSVAR(table_lock_timeout),
	MYSQL_SYSVAR(checkpoint_interval),
	MYSQL_SYSVAR(accurate_tblscan),
	MYSQL_SYSVAR(command),
	MYSQL_SYSVAR(incr_size),
	MYSQL_SYSVAR(deferred_read_cache_size),
	MYSQL_SYSVAR(sample_max_pages),
	MYSQL_SYSVAR(backup_before_recover),
	MYSQL_SYSVAR(verify_after_recover),
	MYSQL_SYSVAR(enable_mms_cache_update),
	MYSQL_SYSVAR(enable_syslog),
	MYSQL_SYSVAR(directio),
	MYSQL_SYSVAR(binlog_buffer_size),
	MYSQL_SYSVAR(binlog_method),
	MYSQL_SYSVAR(compress_sample_strategy),
	MYSQL_SYSVAR(compress_sample_pct),
	MYSQL_SYSVAR(compress_batch_del_size),
	MYSQL_SYSVAR(compress_cte),
	MYSQL_SYSVAR(compress_win_size),
	MYSQL_SYSVAR(compress_win_detect_times),
	MYSQL_SYSVAR(compress_win_mem_level),
	MYSQL_SYSVAR(keep_dict_on_optimize),
#ifdef NTSE_KEYVALUE_SERVER
	MYSQL_SYSVAR(keyvalue_port),
	MYSQL_SYSVAR(keyvalue_servertype),
	MYSQL_SYSVAR(keyvalue_threadnum),
#endif
#ifdef NTSE_PROFILE
	MYSQL_SYSVAR(profile_summary),
	MYSQL_SYSVAR(thread_profile_autorun),
#endif
	NULL
};

static u64	ntse_handler_use_count = 0;
static u64	ntse_handler_use_time = 0;
static u64	ntse_binlog_num_trans = 0;
static u64	ntse_binlog_num_logs = 0;
static u64	ntse_binlog_num_insert_logs = 0;
static u64	ntse_binlog_num_update_logs = 0;
static u64	ntse_binlog_num_delete_logs = 0;
static u64	ntse_binlog_cache_size = 0;
static u64	ntse_binlog_cached_logs = 0;
static Status	ntse_status;	/** NTSE全局统计信息 */

static SHOW_VAR ntse_status_variables[] = {
	{"buf_size", (char *)&ntse_status.m_pageBufSize, SHOW_INT},
	{"buf_log_reads", (char *)&ntse_status.m_bufStat.m_logicalReads, SHOW_LONGLONG},
	{"buf_phy_reads", (char *)&ntse_status.m_bufStat.m_physicalReads, SHOW_LONGLONG},
	{"buf_pending_reads", (char *)&ntse_status.m_bufPendingReads, SHOW_LONGLONG},
	{"buf_read_time", (char *)&ntse_status.m_bufStat.m_readTime, SHOW_LONGLONG},
	{"buf_prefetches", (char *)&ntse_status.m_bufStat.m_prefetches, SHOW_LONGLONG},
	{"buf_batch_prefetches", (char *)&ntse_status.m_bufStat.m_batchPrefetch, SHOW_LONGLONG},
	{"buf_nonbatch_prefetches", (char *)&ntse_status.m_bufStat.m_nonbatchPrefetch, SHOW_LONGLONG},
	{"buf_prefetch_pages", (char *)&ntse_status.m_bufStat.m_prefetchPages, SHOW_LONGLONG},
	{"buf_log_writes", (char *)&ntse_status.m_bufStat.m_logicalWrites, SHOW_LONGLONG},
	{"buf_phy_writes", (char *)&ntse_status.m_bufStat.m_physicalWrites, SHOW_LONGLONG},
	{"buf_pending_writes", (char *)&ntse_status.m_bufPendingWrites, SHOW_LONGLONG},
	{"buf_write_time", (char *)&ntse_status.m_bufStat.m_writeTime, SHOW_LONGLONG},
	{"buf_scavenger_writes", (char *)&ntse_status.m_bufStat.m_scavengerWrites, SHOW_LONGLONG},
	{"buf_flush_writes", (char *)&ntse_status.m_bufStat.m_flushWrites, SHOW_LONGLONG},
	{"buf_page_creates", (char *)&ntse_status.m_bufStat.m_pageCreates, SHOW_LONGLONG},
	{"buf_alloc_block_fails", (char *)&ntse_status.m_bufStat.m_allocBlockFail, SHOW_LONGLONG},
	{"buf_replace_searches", (char *)&ntse_status.m_bufStat.m_replaceSearches, SHOW_LONGLONG},
	{"buf_replace_search_len", (char *)&ntse_status.m_bufStat.m_replaceSearchLen, SHOW_LONGLONG},
	{"buf_dirty_pages", (char *)&ntse_status.m_bufStat.m_statusEx.m_dirtyPages, SHOW_LONGLONG},
	{"buf_pinned_pages", (char *)&ntse_status.m_bufStat.m_statusEx.m_pinnedPages, SHOW_LONGLONG},
	{"buf_rlocked_pages", (char *)&ntse_status.m_bufStat.m_statusEx.m_rlockedPages, SHOW_LONGLONG},
	{"buf_wlocked_pages", (char *)&ntse_status.m_bufStat.m_statusEx.m_wlockedPages, SHOW_LONGLONG},
	{"buf_avg_hash_conflict", (char *)&ntse_status.m_bufStat.m_statusEx.m_avgHashConflict, SHOW_DOUBLE},
	{"buf_max_hash_conflict", (char *)&ntse_status.m_bufStat.m_statusEx.m_maxHashConflict, SHOW_LONGLONG},
	{"buf_unsafe_lock_fails", (char *)&ntse_status.m_bufStat.m_unsafeLockFails, SHOW_LONGLONG},
	{"log_writes", (char *)&ntse_status.m_logStat.m_writeCnt, SHOW_LONGLONG},
	{"log_write_size", (char *)&ntse_status.m_logStat.m_writeSize, SHOW_LONGLONG},
	{"log_flushes", (char *)&ntse_status.m_logStat.m_flushCnt, SHOW_LONGLONG},
	{"log_flush_pages", (char *)&ntse_status.m_logStat.m_flushedPages, SHOW_LONGLONG},
	{"log_tail_lsn", (char *)&ntse_status.m_logStat.m_tailLsn, SHOW_LONGLONG},
	{"log_start_lsn", (char *)&ntse_status.m_logStat.m_startLsn, SHOW_LONGLONG},
	{"log_checkpoint_lsn", (char *)&ntse_status.m_logStat.m_ckptLsn, SHOW_LONGLONG},
	{"log_flushed_lsn", (char *)&ntse_status.m_logStat.m_fushedLsn, SHOW_LONGLONG},
	{"mms_size", (char *)&ntse_status.m_mmsStat.m_occupiedPages, SHOW_LONGLONG},
	{"mms_rec_pages", (char *)&ntse_status.m_mmsStat.m_recordPages, SHOW_LONGLONG},
	{"mms_queries", (char *)&ntse_status.m_mmsStat.m_recordQueries, SHOW_LONGLONG},
	{"mms_query_hits", (char *)&ntse_status.m_mmsStat.m_recordQueryHits, SHOW_LONGLONG},
	{"mms_inserts", (char *)&ntse_status.m_mmsStat.m_recordInserts, SHOW_LONGLONG},
	{"mms_deletes", (char *)&ntse_status.m_mmsStat.m_recordDeletes, SHOW_LONGLONG},
	{"mms_updates", (char *)&ntse_status.m_mmsStat.m_recordUpdates, SHOW_LONGLONG},
	{"mms_rec_replaces", (char *)&ntse_status.m_mmsStat.m_recordVictims, SHOW_LONGLONG},
	{"mms_page_replaces", (char *)&ntse_status.m_mmsStat.m_pageVictims, SHOW_LONGLONG},
	{"rows_reads", (char *)&ntse_status.m_opStat.m_statArr[OPS_ROW_READ], SHOW_LONGLONG},
	{"rows_inserts", (char *)&ntse_status.m_opStat.m_statArr[OPS_ROW_INSERT], SHOW_LONGLONG},
	{"rows_updates", (char *)&ntse_status.m_opStat.m_statArr[OPS_ROW_UPDATE], SHOW_LONGLONG},
	{"rows_deletes", (char *)&ntse_status.m_opStat.m_statArr[OPS_ROW_DELETE], SHOW_LONGLONG},
	{"rowlock_rlocks", (char *)&ntse_status.m_rowlockStat.m_rlockCnt, SHOW_LONGLONG},
	{"rowlock_wlocks", (char *)&ntse_status.m_rowlockStat.m_wlockCnt, SHOW_LONGLONG},
	{"rowlock_spins", (char *)&ntse_status.m_rowlockStat.m_spinCnt, SHOW_LONGLONG},
	{"rowlock_waits", (char *)&ntse_status.m_rowlockStat.m_waitCnt, SHOW_LONGLONG},
	{"rowlock_wait_time", (char *)&ntse_status.m_rowlockStat.m_waitTime, SHOW_LONGLONG},
	{"rowlock_active_readers", (char *)&ntse_status.m_rowlockStat.m_activeReaders, SHOW_LONGLONG},
	{"rowlock_active_writers", (char *)&ntse_status.m_rowlockStat.m_activeWriters, SHOW_LONGLONG},
	{"rowlock_avg_hash_conflict", (char *)&ntse_status.m_rowlockStat.m_avgConflictLen, SHOW_DOUBLE},
	{"rowlock_max_hash_conflict", (char *)&ntse_status.m_rowlockStat.m_maxConflictLen, SHOW_LONGLONG},
	{"binlog_num_logs", (char *)&ntse_binlog_num_logs, SHOW_LONGLONG},
	{"binlog_num_trans", (char *)&ntse_binlog_num_trans, SHOW_LONGLONG},
	{"binlog_num_insert_logs", (char *)&ntse_binlog_num_insert_logs, SHOW_LONGLONG},
	{"binlog_num_update_logs", (char *)&ntse_binlog_num_update_logs, SHOW_LONGLONG},
	{"binlog_num_delete_logs", (char *)&ntse_binlog_num_delete_logs, SHOW_LONGLONG},
	{"binlog_cache_size", (char *)&ntse_binlog_cache_size, SHOW_LONGLONG},
	{"binlog_cached_logs", (char *)&ntse_binlog_cached_logs, SHOW_LONGLONG},
	{"handler_use_count", (char *)&ntse_handler_use_count, SHOW_LONGLONG},
	{"handler_use_time", (char *)&ntse_handler_use_time, SHOW_LONGLONG},
	{NullS, NullS, SHOW_LONG}
};

static int show_ntse_vars(THD *thd, SHOW_VAR *var, char *buff) {
	// NTSE全局统计信息都不是线程私有的
	ntse_db->reportStatus(&ntse_status);
	if (ntse_binlog) {
		const BinlogWriterStatus writerStatus = ntse_binlog->getBinlogWriterStatus();
		ntse_binlog_num_logs = writerStatus.m_totalWrites;
		ntse_binlog_num_trans = writerStatus.m_transNum;
		ntse_binlog_num_insert_logs = writerStatus.m_insertLogs;
		ntse_binlog_num_update_logs = writerStatus.m_updateLogs;
		ntse_binlog_num_delete_logs = writerStatus.m_deleteLogs;
		const BinlogBufferStatus bufferStatus = ntse_binlog->getBinlogBufferStatus();
		ntse_binlog_cache_size = bufferStatus.m_writeBufferSize + bufferStatus.m_readBufferUnflushSize;
		ntse_binlog_cached_logs = bufferStatus.m_writeBufferCount + bufferStatus.m_readBufferUnflushCount;
	}
	var->type = SHOW_ARRAY;
	var->value = (char *)&ntse_status_variables;
	return 0;
}

static SHOW_VAR ntseStatus[] = {
	{"Ntse", (char*)&show_ntse_vars, SHOW_FUNC},
	{NullS, NullS, SHOW_LONG}
};

THDInfo::THDInfo(const THD *thd) {
	m_conn = ntse_db->getConnection(false);
	m_conn->setThdID(thd_get_thread_id(thd));
#ifdef NTSE_PROFILE
	g_tlsProfileInfo.prepareProfile(thd_get_thread_id(thd), CONN_THREAD, g_profiler.getThreadProfileAutorun());
#endif
	m_cmdInfo = new CmdInfo();
	m_pendingOper = NULL;
	m_pendingOpType = POT_NONE;
	m_nextCreateArgs = NULL;
}

THDInfo::~THDInfo() {
	assert(!m_pendingOper);
	ntse_db->freeConnection(m_conn);
#ifdef NTSE_PROFILE
	g_tlsProfileInfo.endProfile();
#endif
	delete m_cmdInfo;
}

/** 设置进行到一半的操作信息
 * @param pendingOper 进行到一半的操作
 * @param type 操作类型
 */
void THDInfo::setPendingOper(void *pendingOper, PendingOperType type) {
	assert(!m_pendingOper && pendingOper && type != POT_NONE);
	m_pendingOper = pendingOper;
	m_pendingOpType = type;
}

/** 重置进行到一半的操作信息 */
void THDInfo::resetPendingOper() {
	assert(m_pendingOper);
	m_pendingOper = NULL;
	m_pendingOpType = POT_NONE;
}

/** 设置下一个建表语句所使用的非标准表定义信息
 * @param createArgs 非标准表定义信息
 */
void THDInfo::setNextCreateArgs(const char *createArgs) {
	delete []m_nextCreateArgs;
	m_nextCreateArgs = System::strdup(createArgs);
}

/** 重置非标准表定义信息 */
void THDInfo::resetNextCreateArgs() {
	if (m_nextCreateArgs) {
		delete []m_nextCreateArgs;
		m_nextCreateArgs = NULL;
	}
}


/**
 * 获取连接信息
 *
 * @return 连接信息
 */
THDInfo* getTHDInfo(THD *thd) {
	return *((THDInfo **)thd_ha_data(thd, ntse_hton));
}

/**
 * 设置连接信息
 *
 * @param THD 连接
 * @param info 连接信息
 */
void setTHDInfo(THD *thd, THDInfo *info) {
	*((THDInfo **)thd_ha_data(thd, ntse_hton)) = info;
}

/**
 * 检查连接信息是否设置，若没有则创建
 *
 * @param thd THD对象
 */
THDInfo* checkTHDInfo(THD *thd) {
	THDInfo *info = getTHDInfo(thd);
	if (!info) {
		info = new THDInfo(thd);
		setTHDInfo(thd, info);
	}
	return info;
}

/**
 * 创建一个NTSE handler
 *
 * @param hton NTSE存储引擎实例
 * @param table handler要操作的表
 * @param mem_root 在此创建
 * @return 新创建的handler
 */
static handler* ntse_create_handler(handlerton *hton,
	TABLE_SHARE *table,
	MEM_ROOT *mem_root) {
	ftrace(ts.mysql, tout << hton << table << mem_root);
	return new (mem_root) ha_ntse(hton, table);
}

/**
 * 关闭连接时通知NTSE。该函数只有在连接信息不为NULL时才会调用
 *
 * @param hton NTSE存储引擎实例
 * @param thd 要关闭的连接
 * @return 总是返回0
 */
static int ntse_close_connection(handlerton *hton, THD *thd) {
	ftrace(ts.mysql, tout << hton << thd);
	DBUG_ENTER("ntse_close_connection");

	THDInfo *info = getTHDInfo(thd);
	assert(info);
	if (info->m_pendingOper) {
		if (info->m_pendingOpType == POT_BACKUP) {
			BackupProcess *backup = (BackupProcess *)info->m_pendingOper;
			ntse_db->doneBackup(backup);
			info->resetPendingOper();
		}
	}
	delete info;

	DBUG_RETURN(0);
}

/**
 * 初始化NTSE存储引擎，打开数据库
 *
 * @param p handlerton
 * @return 成功返回0，失败返回1
 */
int ha_ntse::init(void *p) {
	DBUG_ENTER("ha_ntse::init");

	rationalConfigs();
	handlerInfo = HandlerInfo::getInstance();
	isNtseLogBin = (opt_bin_log && isBinlogSupportable());

	GlobalFactory::getInstance();
	Tracer::init();

	ntse_hton = (handlerton *)p;

	ntse_version = new char[50];
	System::snprintf(ntse_version, 50, "%s r%d", NTSE_VERSION, NTSE_REVISION);
	ntse_config.setBasedir(".");	// 与MySQL配合使用时只能设置为.，即为MySQL的datadir
	ntse_config.setTmpdir(ntse_tmpdir);
	ntse_config.m_logFileSize = ntse_log_file_size;
	ntse_config.m_logBufSize = ntse_log_buf_size;
	set_buf_page_from_para(&ntse_config.m_pageBufSize, &ntse_page_buf_size);
	set_buf_page_from_para(&ntse_config.m_mmsSize, &ntse_rec_buf_size);
	ntse_config.m_maxSessions = (u16)ntse_max_sessions;
	ntse_config.m_tlTimeout = ntse_table_lock_timeout;
	ntse_config.m_chkptInterval = ntse_checkpoint_interval;
	ntse_config.m_backupBeforeRecover = (bool)ntse_backup_before_recover;
	ntse_config.m_verifyAfterRecover = (bool)ntse_verify_after_recover;
	ntse_config.m_enableMmsCacheUpdate = true;	// 为了能正确恢复,先设置true,参见Bug:QA35421
	ntse_config.m_printToStdout = ntse_enable_syslog;
	ntse_config.m_directIo = ntse_directio;
	ntse_config.m_localConfigs.m_accurateTblScan = THDVAR(NULL, accurate_tblscan);
	ntse_config.m_localConfigs.setCompressSampleStrategy(THDVAR(NULL, compress_sample_strategy));
	ntse_config.m_localConfigs.m_tblSmplPct = THDVAR(NULL, compress_sample_pct);
	ntse_config.m_localConfigs.m_smplTrieBatchDelSize = THDVAR(NULL, compress_batch_del_size);
	ntse_config.m_localConfigs.m_smplTrieCte = THDVAR(NULL, compress_cte);
	ntse_config.m_localConfigs.m_tblSmplWinSize = THDVAR(NULL, compress_win_size);
	ntse_config.m_localConfigs.m_tblSmplWinDetectTimes = THDVAR(NULL, compress_win_detect_times);
	ntse_config.m_localConfigs.m_tblSmplWinMemLevel = THDVAR(NULL, compress_win_mem_level);

	try {
		ntse_db = ntse::Database::open(&ntse_config, true, 0, isNtseLogBin);
		ntse_config.m_enableMmsCacheUpdate = (bool)ntse_enable_mms_cache_update;
	} catch (NtseException &e) {
		fprintf(stderr, "%s\n", e.getMessage());
		::abort();
		DBUG_RETURN(1);
	}

	if (isNtseLogBin) {
		ntse_binlog = NTSEBinlogFactory::getInstance()->getNTSEBinlog(ntse_binlog_method, (size_t)ntse_binlog_buffer_size);
		assert(ntse_binlog != NULL);
		ntse_binlog->registerNTSECallbacks(ntse_db);
		ntse_hton->binlog_func = ntseBinlogFunc;

		if (ntse_config.m_enableMmsCacheUpdate) { 
			if (System::stricmp(ntse_binlog_method, "mysql") && !opt_log_slave_updates) {
				//如果ntse_binlog_method!=mysql， 并且指定了log_slave_updates为false
				sql_print_warning("ntse_enable_mms_cache_update can't be set to true and now is reset "
					"to false when log_slave_updates is set to false.");
				ntse_config.m_enableMmsCacheUpdate = false;
				ntse_enable_mms_cache_update = 0;
			} else if (!System::stricmp(ntse_binlog_method, "direct")) {
				// 如果指定了ntse_binlog_method=direct
				sql_print_warning("ntse_enable_mms_cache_update can't be set to true and now is reset "
					"to false when ntse_binlog_method is set to \"direct\".");
				ntse_config.m_enableMmsCacheUpdate = false;
				ntse_enable_mms_cache_update = 0;
			}
		}
	}
	

	ntse_hton->state = SHOW_OPTION_YES;
	ntse_hton->create = ntse_create_handler;
	ntse_hton->close_connection = ntse_close_connection;
	ntse_hton->flags = HTON_NO_FLAGS;

	cmd_executor = new CmdExecutor();

#ifdef NTSE_KEYVALUE_SERVER
	keyvalueConfig.port = ntse_keyvalue_port;
	keyvalueConfig.serverType = ntse_keyvalue_servertype;
	keyvalueConfig.threadNum = ntse_keyvalue_threadnum;
	
	keyvalueServer = new KeyValueServer(&keyvalueConfig, ntse_db);
	keyvalueServer->start();
#endif

	DBUG_RETURN(0);
}

/**
 * 卸载NTSE存储引擎，释放所有资源
 *
 * @param p handlerton
 * @return 总是成功，返回0
 */
int ha_ntse::exit(void *p) {
	ftrace(ts.mysql, tout << p);
	DBUG_ENTER("ha_ntse::exit");

	// 按照上层推出的逻辑，应该是先清理binlog相关信息，在关闭存储引擎，因此这里可以断言ntse_binlog已经停止使用
	NTSE_ASSERT(ntse_binlog == NULL);

	HandlerInfo::freeInstance();

#ifdef NTSE_KEYVALUE_SERVER
	KeyValueServer::clearCachedTable();
	delete keyvalueServer;
#endif

	if (isNtseLogBin)
		NTSEBinlogFactory::freeInstance();

	if (ntse_db) {
		ntse_db->close();
		delete ntse_db;
	}
	delete cmd_executor;

	delete []ntse_version;
	Tracer::exit();
	GlobalFactory::freeInstance();

	DBUG_RETURN(0);
}

ha_ntse::ha_ntse(handlerton *hton, TABLE_SHARE *table_arg): handler(hton, table_arg) {
	ftrace(ts.mysql, tout << this << hton << table_arg);
	m_session = NULL;
	m_tblInfo = NULL;
	m_table = NULL;
	m_scan = NULL;
	m_session = NULL;
	m_errno = 0;
	m_errmsg[0] = '\0';
	ref_length = RID_BYTES + 1;
	m_wantLock = m_gotLock = IL_NO;
	m_replace = false;
	m_iuSeq = NULL;
	m_lastRow = INVALID_ROW_ID;
	m_beginTime = 0;
	m_thd = NULL;
	m_conn = NULL;
	m_isRndScan = false;
	m_rowCache = NULL;
	m_thdInfoLnk.set(this);
	m_hdlInfoLnk.set(this);
	m_lobCtx = new MemoryContext(Limits::PAGE_SIZE, 1);
	m_isReadAll = false;
	m_increment = 1;
	m_offset = 0;
}

/**
 * 返回存储引擎执行ALTER TABLE时的能力
 *
 * @param NTSE存储引擎执行ALTER TABLE时的能力
 */
uint ha_ntse::alter_table_flags(uint flags) {
	ftrace(ts.mysql, tout << this << flags);
	// 可以在线创建、删除各种索引，在线增加非唯一性索引期间也允许进行写操作
	uint r = HA_ONLINE_ADD_INDEX;
	r |= HA_ONLINE_DROP_INDEX_NO_WRITES;
	r |= HA_ONLINE_ADD_PK_INDEX_NO_WRITES;
	r |= HA_ONLINE_DROP_PK_INDEX_NO_WRITES;
	r |= HA_ONLINE_ADD_UNIQUE_INDEX_NO_WRITES;
	r |= HA_ONLINE_DROP_UNIQUE_INDEX_NO_WRITES;
	return r;
}

/** 判断能不能进行在线表模式修改
 * @param create_info 新的表模式
 * @param table_changes  反映新旧表模式之间区别的参数
 * @return 能否进行在线表模式修改
 */
bool ha_ntse::check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes) {
	ftrace(ts.mysql, tout << this << create_info << table_changes);
	if (table_changes != IS_EQUAL_YES)
		return COMPATIBLE_DATA_NO;
	/* Check that auto_increment value was not changed */
	if ((create_info->used_fields & HA_CREATE_USED_AUTO) && create_info->auto_increment_value != 0)
		return COMPATIBLE_DATA_NO;
	return COMPATIBLE_DATA_YES;
}

/**
 * 返回表类型
 * @return 总是返回NTSE
 */
const char* ha_ntse::table_type() const {
	ftrace(ts.mysql, tout << this);
	return "NTSE";
}

/** 返回指定索引的类型
 * @param inx 表中的第几个索引，从0开始编号。
 * @return 索引类型，NTSE使用B+树索引，总是返回"BTREE"
 */
const char* ha_ntse::index_type(uint inx) {
	ftrace(ts.mysql, tout << this << inx);
	return "BTREE";
}

/**
 * 存储引擎使用的扩展名。默认的重命名和删除表实现将使用这一扩展名
 * 来重命名或删除一张表对应的所有文件。
 * 不过NTSE需要自己实现重命名和删除表功能，因为还要处理数据字典。
 * 注: 扩展名中即包含.分隔符
 */
static const char *ha_ntse_exts[] = {
	Limits::NAME_IDX_EXT,
	Limits::NAME_HEAP_EXT,
	Limits::NAME_TBLDEF_EXT,
	Limits::NAME_SOBH_EXT,
	Limits::NAME_SOBH_TBLDEF_EXT,
	Limits::NAME_LOBI_EXT,
	Limits::NAME_LOBD_EXT,
	Limits::NAME_GLBL_DIC_EXT,
	NullS
};

/**
* 存储引擎使用的空扩展名。仅针对判定repair table ... use_frm表维护语法使用
*/
static const char *ha_ntse_nullExts[] = {
	NullS, NullS
};

/**
 * 返回存储使用的文件扩展名数组
 *
 * @return NTSE使用的文件扩展名数组
 */
const char** ha_ntse::bas_ext() const {
	ftrace(ts.mysql, tout << this);
	try {
		/** 检查bas_ext的调用是否是repair table ... use_frm产生。 */
		if (m_thd && TT_USEFRM == m_thd->lex->check_opt.sql_flags && SQLCOM_REPAIR == m_thd->lex->sql_command) {
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "'repair table ... use_frm' statement is not supported by ntse");
		}

		return ha_ntse_exts;
	} catch (NtseException &e) {
		/**
		 *	由于bas_ext为const成员函数，而reportError非const，需要强制转化
		 */
		const_cast<ha_ntse* const>(this)->reportError(e.getErrorCode(), e.getMessage(), false, true);

		return ha_ntse_nullExts;
	}
}

/**
 * 返回标识存储引擎能力的一系列标志
 *
 * @return 表示NTSE存储引擎能力的标志
 */
ulonglong ha_ntse::table_flags() const {
	ftrace(ts.mysql, tout << this);
	ulonglong flags = HA_NO_TRANSACTIONS;	// 不支持事务
	flags |= HA_PARTIAL_COLUMN_READ;	// 可以只返回记录中的某几个属性
	flags |= HA_NULL_IN_KEY;			// 非主键索引中的键值是否可能为NULL
	flags |= HA_DUPLICATE_POS;			// REPLACE发生冲突时通过position/rnd_pos系列调用处理
	flags &= ~HA_NO_PREFIX_CHAR_KEYS;	// 虽然NTSE不支持前缀索引，但不能设置HA_NO_PREFIX_CHAR_KEYS标志，
										// 否则MySQL将默默的将前缀索引转化为非前缀索引，这可能带来
										// 不可预料的后果。更好的解决方案是在试图建立前缀索引时报错。
	flags |= HA_FILE_BASED;				// 每张表的数据存储在单独文件中
	flags |= HA_BINLOG_ROW_CAPABLE;		// NTSE使用行级复制而非语句级复制
	flags |= HA_REC_NOT_IN_SEQ;			// 表扫描时不会自动把RID填充到ref_pos中，必须调用position
	flags |= HA_PRIMARY_KEY_REQUIRED_FOR_DELETE;	// 告诉上层在UPDATE/DELETE时要读取记录的主键，
										// 以保证复制时的正确性。如果表没有主键，这时上层会要求读取
										// 所有属性
	if (isBinlogSupportable())
		flags |= HA_HAS_OWN_BINLOGGING;	// 表示NTSE引擎将自主记录binlog
	// 一些意义不太明显的标志的说明
	// HA_CAN_SQL_HANDLER: 支持通过HANDLER语句直接操作存储引擎
	return flags;
}

/**
 * 返回标识存储引擎索引实现能力的一系列标志
 *
 * @param inx 第几个索引
 * @param part 索引扫描条件包含前多少个索引属性
 * @param all_parts 条件是否包含所有索引属性
 * @return 标识NTSE存储引擎索引实现能力的标志
 */
ulong ha_ntse::index_flags(uint inx, uint part, bool all_parts) const {
	ftrace(ts.mysql, tout << this << inx << part << all_parts);
	ulong flags = HA_READ_NEXT;		// 索引可以后向扫描
	flags |= HA_READ_PREV;			// 索引可以前向扫描
	flags |= HA_READ_ORDER;			// 索引输出有序结果
	flags |= HA_READ_RANGE;			// 支持索引范围扫描
	flags |= HA_KEYREAD_ONLY;		// 可以只扫描索引不访问表
	return flags;
}

/**
 * 返回NTSE存储引擎支持的最大记录长度，不包含大对象
 * 这一限制根据表定义计算的最大长度，实际能存储的最大长度会比这小
 *
 * @return NTSE存储引擎支持的最大记录长度，不包含大对象
 */
uint ha_ntse::max_supported_record_length() const {
	ftrace(ts.mysql, tout << this);
	return Limits::DEF_MAX_REC_SIZE;
}

/**
 * 返回一张表最多可能有多少个索引
 *
 * @return NTSE中一张表最多可能有多少个索引
 */
uint ha_ntse::max_supported_keys() const {
	ftrace(ts.mysql, tout << this);
	return Limits::MAX_INDEX_NUM;
}

/**
 * 返回索引最多包含多少属性
 *
 * @return NTSE中索引最多包含多少属性
 */
uint ha_ntse::max_supported_key_parts() const {
	ftrace(ts.mysql, tout << this);
	return Limits::MAX_INDEX_KEYS;
}

/**
 * 返回NTSE存储引擎支持的最大索引键长度
 *
 * @return NTSE存储引擎支持的最大索引键长度
 */
uint ha_ntse::max_supported_key_length() const {
	ftrace(ts.mysql, tout << this);
	return DrsIndice::IDX_MAX_KEY_LEN;
}

/** 返回NTSE存储引擎支持的最大索引键属性长度
 * @return NTSE存储引擎支持的最大索引键属性长度
 */
uint ha_ntse::max_supported_key_part_length() const {
	ftrace(ts.mysql, tout << this);
	return DrsIndice::IDX_MAX_KEY_LEN;
}

/**
 * 返回表扫描代价。目前使用存储引擎示例中的默认实现
 *
 * @return 表扫描代价
 */
double ha_ntse::scan_time() {
	ftrace(ts.mysql, tout << this);
	return (double) (stats.records + stats.deleted) / 20.0+10;
}

/**
 * 返回读取指定条数记录的代价。目前使用存储引擎示例中的默认实现
 *
 * @return 读取代价
 */
double ha_ntse::read_time(ha_rows rows) {
	ftrace(ts.mysql, tout << this << rows);
	return (double) rows /  20.0 + 1;
}

/**
 * 根据表名，查找对应的共享结构，其中包含表级锁信息。
 * 每个handler打开时，都会调用一次这个函数。
 * 为实现正确的表锁行为，系统维护一个已经打开的表对应的共享结构
 * 哈希表，一个表第一次被打开时创建一个新的共享结构，其它情况下
 * 重用这一结构并增加引用计数。
 *
 * 其实在NTSE内部也维护了被打开的表，但由于表共享结构中
 * 包括了MySQL中特有的THR_LOCK等结构，需要在这里再维护一次。
 *
 * @param table_name 表名
 * @param eno OUT，失败时返回错误号
 * @post 如果打开成功，会设置ha_ntse的m_table和m_tblInfo两个结构
 * @return true表示打开成功，false表示打开失败
 */
bool ha_ntse::openTable(const char *table_name, int *eno) {
	st_table *mysqlTable = table;
	LOCK(&openLock);

	if (!(m_tblInfo = openTables.get(table_name))) {
			assert(!m_session);
			m_session = ntse_db->getSessionManager()->allocSession("ha_ntse::get_share", m_conn, 100);
			if (!m_session) {
				*eno = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
				goto error;
			}
			char *name_copy = System::strdup(table_name);
			normalizePathSeperator(name_copy);
			Table *table = NULL;
			try {
				table = ntse_db->openTable(m_session, name_copy);
				table->setMysqlTmpTable(table_share->get_table_ref_type() != TABLE_REF_BASE_TABLE);
				table->lockMeta(m_session, IL_S, 1000, __FILE__, __LINE__);
				table->refreshRows(m_session);
				table->unlockMeta(m_session, IL_S);
			} catch (NtseException &e) {
				if (table != NULL) {
					if (table->getMetaLock(m_session) != IL_NO) {
						table->unlockMeta(m_session, table->getMetaLock(m_session));
					}
					ntse_db->closeTable(m_session, table);
				}

				delete []name_copy;
				ntse_db->getSessionManager()->freeSession(m_session);
				m_session = NULL;
				*eno = reportError(e.getErrorCode(), e.getMessage(), false);
				goto error;
			}
			delete []name_copy;

			m_tblInfo = new TableInfoEx(table_name, table);

			m_table = m_tblInfo->m_table;
			try {
				if (mysqlTable->found_next_number_field != NULL) {
					m_table->lockMeta(m_session, IL_S, 1000, __FILE__, __LINE__);
					m_table->lock(m_session, IL_IS, 1000, __FILE__, __LINE__);
					m_tblInfo->m_autoIncr.setAutoIncr(getMaxAutoIncr(m_session));
					m_table->unlock(m_session, IL_IS);
					m_table->unlockMeta(m_session, IL_S);
				}
			} catch (NtseException &e) {
				if (m_table->getLock(m_session) != IL_NO)
					m_table->unlock(m_session, m_table->getLock(m_session));
				if (table->getMetaLock(m_session) != IL_NO)
					table->unlockMeta(m_session, table->getMetaLock(m_session));
				ntse_db->closeTable(m_session, m_table);
				delete m_tblInfo;
				ntse_db->getSessionManager()->freeSession(m_session);
				m_session = NULL;
				*eno = reportError(e.getErrorCode(), e.getMessage(), false);
				goto error;
			}

			openTables.put(m_tblInfo);
			ntse_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
	}
	m_tblInfo->m_refCnt++;
	m_table = m_tblInfo->m_table;
	UNLOCK(&openLock);
	return true;

error:
	UNLOCK(&openLock);
	return false;
}


/**
 * 释放表共享结构，在每个handler被关闭时，系统会调用这一函数。
 * 一般情况下减少引用计数，若引用计数变成0则释放表共享结构
 *
 * @param share 表共享结构
 * @return 总是返回0
 */
int ha_ntse::closeTable(TableInfoEx *share) {
	assert(!m_session);
	m_session = ntse_db->getSessionManager()->allocSession("ha_ntse::get_share", m_conn);
	LOCK(&openLock);
	
	if (!--share->m_refCnt) {
		m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
		u16 ntseTableId = getNtseTableId();
		if (ntse_binlog) {
			if (m_table->getTableDef()->m_cacheUpdate)
				m_table->flushComponent(m_session,false, false, true, false);
			ntse_binlog->flushBinlog(ntseTableId);
			handlerInfo->unregisterHandler(this, ntseTableId);
		}
		m_table->unlockMeta(m_session, IL_S);
		ntse_db->closeTable(m_session, share->m_table);
		openTables.remove(share->m_path);
		delete share;
	} else {
		m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
		u16 ntseTableId = getNtseTableId();
		if (ntse_binlog)
			handlerInfo->unregisterHandler(this, ntseTableId);
		m_table->unlockMeta(m_session, IL_S);
	}
	
	UNLOCK(&openLock);
	ntse_db->getSessionManager()->freeSession(m_session);
	m_session = NULL;
	return 0;
}

/**
 * 打开表。在上层操作某张表之前，会调用这一函数要求存储引擎打开这张表。
 * (根据我的猜测)
 * MySQL会缓存handler实例，一个handler实例在同一时刻只用于操作一张表，
 * 在handler实例用于操作一张表之前，会调用open打开表，handler实例被
 * 关闭，或转而用于操作另一张表时，会调用close关闭表。转而用于操作另一张
 * 表时，会再调用open打开另一张表，因此一个handler实例在不同时刻可能用于
 * 操作不同的表。
 *
 * @param name 表文件路径，不包含后缀名。
 * @param mode 这个参数不知道什么意思，看InnoDB的实现是不使用这一参数
 * @param test_if_locked 这个参数基本上也没什么用，按ha_open的说明，其中
 *  只需要关心HA_OPEN_WAIT_IF_LOCKED是否被设置，若没有则在表被锁定时
 *  不等待。但由于NTSE自己不维护表级锁，因此也像InnoDB一样不考虑这一参数。
 * @return 成功返回0，失败返回错误码
 */
int ha_ntse::open(const char *name, int mode, uint test_if_locked) {
	ftrace(ts.mysql, tout << this << name << mode << test_if_locked);
	DBUG_ENTER("ha_ntse::open");

	attachToTHD(ha_thd());

	// TODO: 处理openTable找不到表失败，由于在线修改表模式过程中失败导致表
	// 定义不一致情况
	int ret = 0;
	if (openTable(name, &ret)) {
		assert(!m_session);
		m_session = ntse_db->getSessionManager()->allocSession("ha_ntse::get_share", m_conn, 100);
		if (!m_session) {
			ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
		} else {
			thr_lock_data_init(&m_tblInfo->m_lock, &m_mysqlLock, NULL);
			try {
				m_table->lockMeta(m_session, IL_S, 1000, __FILE__, __LINE__);
				bitmap_init(&m_readSet, m_readSetBuf, m_table->getTableDef()->m_numCols, FALSE);	// 真正修改读取的时候，肯定只有一个会话（线程在使用）
				m_table->unlockMeta(m_session, IL_S);
			} catch (NtseException &e) {
				UNREFERENCED_PARAMETER(e);
				ret = reportError(NTSE_EC_LOCK_TIMEOUT, "Lock time out", false);
			}
			ntse_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
		}
	}

	detachFromTHD();

	if (!ret && ntse_binlog && m_tblInfo)
		handlerInfo->registerHandler(this);

	DBUG_RETURN(ret);
}

/**
 * 重置handler状态
 *
 * @return 总是返回0
 */
int ha_ntse::reset() {
	ftrace(ts.mysql, tout << this);
	assert(!m_scan);
	m_errno = 0;
	m_errmsg[0] = '\0';
	m_ignoreDup = false;
	m_replace = false;
	m_iuSeq = NULL;
	m_lastRow = INVALID_ROW_ID;
	m_isRndScan = false;
	m_lobCtx->reset();
	bitmap_clear_all(&m_readSet);
	return 0;
}

/**
 * 关闭handler当前正在用的表。
 *
 * @return 成功返回0，失败返回1，不过NTSE总是返回0
 */
int ha_ntse::close(void) {
	ftrace(ts.mysql, tout << this);
	assert(!m_session && m_table);
	DBUG_ENTER("ha_ntse::close");

	THD *thd = ha_thd();
	if (thd)
		attachToTHD(thd);
	else {	// handler长时间未用过清理时没有THD
		assert(!m_thd && !m_conn);
		m_conn = ntse_db->getConnection(true, "ha_ntse::close");
	}

	NTSE_ASSERT(closeTable(m_tblInfo) == 0);
	m_table = NULL;

	if (thd)
		detachFromTHD();
	else {
		ntse_db->freeConnection(m_conn);
		m_conn = NULL;
	}

	DBUG_RETURN(0);
}

/**
 * 插入一行
 *
 * @param buf 插入行的内容
 * @return 成功返回0，由于唯一性冲突失败返回HA_ERR_FOUND_DUPP_KEY，
 *   由于记录超长失败返回相应错误码
 */
int ha_ntse::write_row(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session);
	DBUG_ENTER("ha_ntse::write_row");
	ha_statistic_increment(&SSV::ha_write_count);

	int code = checkSQLLogBin();
	assert(m_thd->lex->sql_command != SQLCOM_SELECT);
	if (code) {
		DBUG_RETURN(code);
	}

	if (!m_thd->sql_log_bin_toplevel) {
		code = reportError(NTSE_EC_NOT_SUPPORT, "SQL_LOG_BIN variable is not supported by NTSE, please set to 1.", false);
		DBUG_RETURN(code);
	}

	if (table->next_number_field && buf == table->record[0]) {
		// 处理自增
		if ((code = update_auto_increment()))
			// 对于非事务的支持，直接返回错误码
			DBUG_RETURN(code);
	}

	try {
		if (!m_replace) {
			// 即使是INSERT IGNORE在唯一性冲突时也需要返回HA_ERR_FOUND_DUPP_KEY，
			// 用于上层统计成功插入和冲突的各有多少条记录
			m_dupIdx = -1;
			if (m_table->insert(m_session, buf, &m_dupIdx, false, (void*)this) == INVALID_ROW_ID)
				code = HA_ERR_FOUND_DUPP_KEY;
		} else {
			assert(m_ignoreDup);
			if (m_iuSeq) {
				// INSERT ... ON DUPLICATE KEY UPDATE在发生冲突后，若发现要UPDATE的新值与原记录相等
				// 则不会调用update_row，这样在INSERT ... SELECT ... ON DUPLICATE ...时会连接调用
				// write_row
				m_table->freeIUSequenceDirect(m_iuSeq);
				m_iuSeq = NULL;
			}
			m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
			m_iuSeq = m_table->insertForDupUpdate(m_session, buf, false, (void*)this);
			if (m_iuSeq)
				code = HA_ERR_FOUND_DUPP_KEY;
		}
	} catch (NtseException &e) {
		assert(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		code = reportError(e.getErrorCode(), e.getMessage(), false);
	}

	// 设置新的自增值
	setAutoIncrIfNecessary(buf);
	// TODO: 支持事务的时候，可能需要参考Innodb处理自增字段

	DBUG_RETURN(code);
}

/**
 * 更新一条记录。new_data为更新后的属性，但不包括auto_increment类型或自动更新的timestamp
 * 类型的属性值
 * 在调用rnd_next/rnd_pos/index_next之后，MySQL会立即调用这一函数更新满足条件的记录，
 * 因此NTSE总是更新当前扫描的记录，不需要old_data的内容
 *
 * @param old_data 原记录内容，NTSE不用
 * @param new_data 新记录内容
 * @return 成功返回0，唯一性索引冲突失败返回HA_ERR_FOUND_DUPP_KEY，记录超长或
 * 其它原因导致失败时返回相应错误码
 */
int ha_ntse::update_row(const uchar *old_data, uchar *new_data) {
	ftrace(ts.mysql, tout << this << old_data << new_data);
	assert(m_session);
	if (opt_bin_log && !isBinlogSupportable() && !m_isReadAll)
		reportError(NTSE_EC_GENERIC, "Should read all columns when binlog is enabled and written by MySQL", false);

	DBUG_ENTER("ha_ntse::update_row");
	ha_statistic_increment(&SSV::ha_update_count);

	try {
		if (m_scan) {
			if (!m_scan->isUpdateColumnsSet()) {
				u16 numUpdateCols;
				u16 *updateCols = transCols(table->write_set, &numUpdateCols);
				m_scan->setUpdateColumns(numUpdateCols, updateCols);
			}
			if (!m_table->updateCurrent(m_scan, new_data, &m_dupIdx, old_data, (void*)this))
				DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
		} else if (m_iuSeq) {
			u16 numUpdateCols;
			u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			bool succ = m_table->updateDuplicate(m_iuSeq, new_data, numUpdateCols, updateCols, &m_dupIdx, (void*)this);
			m_iuSeq = NULL;
			m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
			if (!succ)
				DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
		} else {
			// 1. 在以下用例中会在结束扫描之后再调用update_row
			// create table t1 (a int not null, b int, primary key (a));
			// create table t2 (a int not null, b int, primary key (a));
			// insert into t1 values (10, 20);
			// insert into t2 values (10, 20);
			// update t1, t2 set t1.b = 150, t2.b = t1.b where t2.a = t1.a and t1.a = 10;
			// 2. 行级复制作为SLAVE执行UPDATE事件时
			if (!IntentionLock::isConflict(m_gotLock, IL_IX)) {
				int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not update row after scan ended if table is not locked.", false);
				DBUG_RETURN(code);
			}
			assert(m_lastRow != INVALID_ROW_ID);
			McSavepoint mcSave(m_session->getMemoryContext());

			u16 numReadCols;
			u16 *readCols = transCols(table->read_set, &numReadCols);
			m_scan = m_table->positionScan(m_session, OP_UPDATE, numReadCols, readCols, false);	// 不加表锁时不可能出异常
			byte *buf = (byte *)alloca(m_table->getTableDef(true, m_session)->m_maxRecSize);
			NTSE_ASSERT(m_table->getNext(m_scan, buf, m_lastRow));

			u16 numUpdateCols;
			u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			m_scan->setUpdateColumns(numUpdateCols, updateCols);
			bool succ;
			try {
				succ = m_table->updateCurrent(m_scan, new_data, &m_dupIdx, old_data, (void*)this);
			} catch (NtseException &e) {
				endScan();
				throw e;
			}

			endScan();
			if (!succ)
				DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
		}
	} catch (NtseException &e) {
		assert(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		DBUG_RETURN(reportError(e.getErrorCode(), e.getMessage(), false));
	}

	// 这里的更新自增值，可以参看InnoDB的实现注释，效果一样
	setAutoIncrIfNecessary(new_data);

	DBUG_RETURN(0);
}

/**
 * 删除一条记录。
 * 在调用rnd_next/rnd_pos/index_next之后，MySQL会立即调用这一函数删除满足条件的记录，
 * 因此NTSE总是更新当前扫描的记录，不需要buf的内容
 * 设置了insert_id时若REPLACE时发生冲突也会调用delete_row
 *
 * @param buf 当前记录内容，NTSE不用
 * @return 总是返回0
 */
int ha_ntse::delete_row(const uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session);
	DBUG_ENTER("ha_ntse::delete_row");
	ha_statistic_increment(&SSV::ha_delete_count);

	if (m_scan) {
		/*
		 *	如果分区表不存在大对象存储，m_scan扫描的记录空间指向buf，而不是内部分配的内存。
		 *  分区表处理更新记录时，会将某一条更新记录先写入到另一个分区，然后在原分区采用删
		 *	除操作。如果m_scan扫描记录的空间指向buf，m_scan扫描记录会被更新到新记录内容，分
		 *	区插入操作完成之后，再回到原分区，用m_scan扫描时会发生记录不存在的错误
		 */
		if (!m_table->getTableDef()->hasLob()) {
			m_scan->setCurrentData((byte*)buf);
		}
		
		m_table->deleteCurrent(m_scan, (void*)this);
	}
	else if (m_iuSeq) {
		m_table->deleteDuplicate(m_iuSeq, (void*)this);
		m_iuSeq = NULL;
		m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
	} else {
		assert(IntentionLock::isConflict(m_gotLock, IL_IX) && m_lastRow != INVALID_ROW_ID);
		McSavepoint mcSave(m_session->getMemoryContext());

		u16 numReadCols;
		u16 *readCols = transCols(table->read_set, &numReadCols);
		m_scan = m_table->positionScan(m_session, OP_DELETE, numReadCols, readCols, false);	// 不加表锁时不可能出异常
		byte *buf = (byte *)alloca(m_table->getTableDef(true, m_session)->m_maxRecSize);
		NTSE_ASSERT(m_table->getNext(m_scan, buf, m_lastRow));
		m_table->deleteCurrent(m_scan, (void*)this);
		endScan();
	}

	DBUG_RETURN(0);
}

/**
 * 索引扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @param key 索引搜索键
 * @param keypart_map 搜索键中包含哪些索引属性
 * @param find_flag 搜索条件类型，如方向，是否包含起始条件等
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_ntse::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
	enum ha_rkey_function find_flag) {
	ftrace(ts.mysql, tout << this << buf << key << (u64)keypart_map << find_flag);
	assert(m_session);
	DBUG_ENTER("ha_ntse::index_read_map");
	ha_statistic_increment(&SSV::ha_read_key_count);

	if (m_iuSeq) { // 分区表INSERT FOR DUPLICATE UPDATE时，使用index_read_map而不是rnd_pos取记录
		NTSE_ASSERT(find_flag == HA_READ_KEY_EXACT);
		NTSE_ASSERT(active_index == m_iuSeq->getDupIndex());
		transKey(active_index, key, keypart_map, &m_indexKey);
		NTSE_ASSERT(recordHasSameKey(m_iuSeq->getMysqlRow(), m_table->getTableDef(true, m_session)->m_indice[active_index], &m_indexKey));
		m_iuSeq->getDupRow(buf);
		DBUG_RETURN(0);
	}

	bool forward, includeKey;
	switch (find_flag) {
	case HA_READ_KEY_EXACT:
		forward = true;
		includeKey = true;
		break;
	case HA_READ_KEY_OR_NEXT:
		forward = true;
		includeKey = true;
		break;
	case HA_READ_KEY_OR_PREV:
		forward = false;
		includeKey = true;
		break;
	case HA_READ_AFTER_KEY:
		forward = true;
		includeKey = false;
		break;
	case HA_READ_BEFORE_KEY:
		forward = false;
		includeKey = false;
		break;
	/** TODO: 以下这三种情况NTSE并不真正支持，NTSE不会丢失结果，但可能会返回
	 * 过多的结果，经调试发现MySQL上层会把NTSE返回的错误结果过滤掉
	 */
	case HA_READ_PREFIX:
		forward = true;
		includeKey = true;
		break;
	case HA_READ_PREFIX_LAST:
		forward = false;
		includeKey = true;
		break;
	default:
		assert(find_flag == HA_READ_PREFIX_LAST_OR_PREV);
		forward = false;
		includeKey = true;
		break;
	}

	DBUG_RETURN(indexRead(buf, key, keypart_map, forward, includeKey, find_flag == HA_READ_KEY_EXACT));
}

/**
 * 索引反向扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @param key 索引搜索键
 * @param keypart_map 搜索键中包含哪些索引属性
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_ntse::index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map) {
	ftrace(ts.mysql, tout << this << buf << key << (u64)keypart_map);
	DBUG_ENTER("ha_ntse::index_read_last_map");
	ha_statistic_increment(&SSV::ha_read_last_count);
	DBUG_RETURN(indexRead(buf, key, keypart_map, false, true, false));
}

/**
 * 索引正向全扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_ntse::index_first(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	DBUG_ENTER("ha_ntse::index_first");
	ha_statistic_increment(&SSV::ha_read_first_count);
	DBUG_RETURN(indexRead(buf, NULL, 0, true, true, false));
}

/**
 * 索引反向全扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_ntse::index_last(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	DBUG_ENTER("ha_ntse::index_last");
	ha_statistic_increment(&SSV::ha_read_last_count);
	DBUG_RETURN(indexRead(buf, NULL, 0, false, true, false));
}

/**
 * 读取下一条记录
 *
 * @param buf 用于存储输入记录内容
 * @return 成功返回0，没有记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_ntse::index_next(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_scan && m_scan->getType() == ST_IDX_SCAN);
	DBUG_ENTER("ha_ntse::index_next");
	ha_statistic_increment(&SSV::ha_read_next_count);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	DBUG_RETURN(fetchNext(buf));
}

/**
 * 读取前一条记录
 *
 * @param buf 用于存储输入记录内容
 * @return 成功返回0，没有记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_ntse::index_prev(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_scan && m_scan->getType() == ST_IDX_SCAN);
	DBUG_ENTER("ha_ntse::index_prev");
	ha_statistic_increment(&SSV::ha_read_prev_count);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	DBUG_RETURN(fetchNext(buf));
}

/**
 * 结束索引扫描，释放扫描句柄及资源
 *
 * @return 总是返回0
 */
int ha_ntse::index_end() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ha_ntse::index_end");
	if (m_session)
		endScan();
	active_index = MAX_KEY;
	DBUG_RETURN(0);
}

/**
 * 初始化表扫描
 *
 * @param scan 为true时才需要处理
 * @return 一定会成功返回0
 */
int ha_ntse::rnd_init(bool scan) {
	ftrace(ts.mysql, tout << this << scan);
	assert(m_session);
	DBUG_ENTER("ha_ntse::rnd_init");

	endScan();	// 作为子查询时可能会扫描多遍
	//fix bug#23575，删除代码	if (scan && m_rowCache) {delete m_rowCache;	m_rowCache = NULL;}。
	//将m_rowCache清除工作交给ha_ntse::external_lock，单个sql语句内部重用m_rowCache，避免bug中
	//position->rnd_init->rnd_pos调用顺序时，访问被清除的m_rowCache.

	int code = checkSQLLogBin();
	if (code) {
		DBUG_RETURN(code);
	}

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
	m_isRndScan = scan;
	bitmap_copy(&m_readSet, table->read_set);

	DBUG_RETURN(code);
}

/**
 * 结束表扫描
 *
 * @return 返回0
 */
int ha_ntse::rnd_end() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ha_ntse::rnd_end");
	if (m_session)
		endScan();
	DBUG_RETURN(0);
}

/**
 * 返回表中下一条记录
 *
 * @param buf 存储记录输出内容
 * @return 成功返回0，没有下一次记录返回HA_ERR_END_OF_FILE
 */
int ha_ntse::rnd_next(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session);
	DBUG_ENTER("ha_ntse::rnd_next");
	ha_statistic_increment(&SSV::ha_read_rnd_next_count);
	if (m_table->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do table scan on index only table", false);
		DBUG_RETURN(code);
	}

	// 为了能正确的得到read_set的信息,在这里才初始化扫描句柄
	if (!m_scan) {
		initReadSet(false);
		beginTblScan();
	} else {
		assert(bitmap_is_subset(table->read_set, &m_readSet));
	}
	DBUG_RETURN(fetchNext(buf));
}


/**
 * 检测Mysql是两趟FileSort算法： 即先取排序和条件属性，再取结果集所需其它属性
 * 若结果集所有列长度之和大于max_length_for_sort_data， 或者结果集包含大对象时，Mysql使用两趟算法（参见QA89930）
 * 
 * @return true表示MySQL使用了两趟FileSort算法
 */
bool ha_ntse::is_twophase_filesort() {
	bool twophase = true;
	if (m_thd != NULL && m_thd->lex != NULL) {
		MY_BITMAP tmp_set;
		uint32 tmp_set_buf[Limits::MAX_COL_NUM / 8 + 1];	/** 位图所需要的缓存 */
		bitmap_init(&tmp_set, tmp_set_buf, m_table->getTableDef()->m_numCols, FALSE);
		// 记住旧的read_set
		MY_BITMAP *save_read_set= table->read_set;
		bitmap_clear_all(&tmp_set);
		table->read_set= &tmp_set;

		List_iterator<Item> list_iter(m_thd->lex->select_lex.item_list);
		Item *curr;
		while ((curr = list_iter++) != NULL) {
			curr->walk(&Item::register_field_in_read_map, 1, (uchar*) table);
		}
		
		// 若堆或者索引扫描readSet不能包含查询所需列
		// TODO: 如何精确判断查询所需的列集，最好能询问Mysql社区
		if (bitmap_is_subset(table->read_set, &m_readSet))
			twophase = false;
		// 恢复read_set
		table->read_set = save_read_set;
	}
	return twophase;
}

/**
 * 记录当前记录的位置，对于NTSE来说即为RID。本函数在非流水化更新时调用
 * 以记录将来要更新的记录的位置，若在Filesort时调用
 *
 * @param record 没什么用
 */
void ha_ntse::position(const uchar *record) {
	ftrace(ts.mysql, tout << this << record);
	assert(m_session && (m_scan || m_gotLock == IL_SIX || m_gotLock == IL_X));
	assert(m_lastRow != INVALID_ROW_ID);
	DBUG_ENTER("ha_ntse::position");

	// 如果可使用记录缓存则尝试加到记录缓存中，若成功则不用进行锁升级
	// 否则需要进行锁升级。若表锁已经升级过则不尝试加到记录缓存中了
	if (m_gotLock == IL_IS) {
		if (!m_rowCache) { // 尝试初始化rowCache
			if (m_isRndScan || !is_twophase_filesort()) { 
				// 随机扫描以及单趟filesort时才启用rowCache
				// TODO: 单元filesort时， 虽然mysql调用了position，但是不会调用rnd_pos，
				//	此时启用rowcache没有意义，但是为了安全起见，暂时保留rowCache
				u16 numReadCols;
				u16 *readCols =	transCols(&m_readSet, &numReadCols);
				m_rowCache = new RowCache(THDVAR(ha_thd(), deferred_read_cache_size), m_table->getTableDef(true, m_session),
					numReadCols, readCols, &m_readSet);
			}
		}
		if (m_rowCache) {
			assert(m_rowCache->hasAttrs(&m_readSet));
			long id = m_rowCache->put(m_lastRow, record);
			if (id >= 0) {
				ref[0] = REF_CACHED_ROW_ID;
				RID_WRITE((RowId)id, ref + 1);
				DBUG_VOID_RETURN;
			}
		}
	}
	ref[0] = REF_ROWID;
	RID_WRITE(m_lastRow, ref + 1);
	if (m_gotLock == IL_IS)
		m_wantLock = IL_S;
	else if (m_gotLock == IL_IX)
		m_wantLock = IL_SIX;

	DBUG_VOID_RETURN;
}

/**
 * 根据记录位置读取记录
 *
 * @param buf 存储记录输出内容
 * @param pos 记录位置
 * @return 总是返回0
 */
int ha_ntse::rnd_pos(uchar *buf, uchar *pos) {
	ftrace(ts.mysql, tout << this << buf << pos);
	assert(m_session);
	DBUG_ENTER("ha_ntse::rnd_pos");
	ha_statistic_increment(&SSV::ha_read_rnd_count);

	if (m_table->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do positional scan on index only table", false);
		DBUG_RETURN(code);
	}

	if (!m_iuSeq) {
		// 为了能正确的得到read_set的信息,在这里才初始化扫描句柄
		if (!m_scan) {
			initReadSet(false, true);
			beginTblScan();
		} else {
			assert(bitmap_is_subset(table->read_set, &m_readSet));
		}

		if (pos[0] == REF_CACHED_ROW_ID) {
			assert(m_rowCache);
			if (!m_rowCache->hasAttrs(&m_readSet)) {
				ntse_db->getSyslog()->log(EL_LOG, "Can not get request attributes from cache. SQL:\n%s", currentSQL());
				int code = reportError(NTSE_EC_GENERIC, "Can not get request attributes from cache", false);
				DBUG_RETURN(code);
			}
			long id = (long)RID_READ(pos + 1);
			m_rowCache->get(id, buf);
		} else {
			RowId rid = RID_READ(pos + 1);
			NTSE_ASSERT(m_table->getNext(m_scan, buf, rid));
		}
	} else {
		assert(pos[0] == REF_ROWID && RID_READ(pos + 1) == m_iuSeq->getScanHandle()->getCurrentRid());
		m_iuSeq->getDupRow(buf);
	}
	table->status = 0;

	DBUG_RETURN(0);
}

/**
 * 返回一些杂项信息
 *
 * @param flag 要返回的信息类型
 * @return 总是返回0
 */
int ha_ntse::info(uint flag) {
	ftrace(ts.mysql, tout << this << flag);

	DBUG_ENTER("ha_ntse::info");
	if (flag & HA_STATUS_VARIABLE) {
		stats.records = m_table->getRows();
		stats.deleted = 0;
		if (m_session && m_table->getMetaLock(m_session) != IL_NO) {
			// 一般情况下如进行正常的查询处理时是加了表锁的
			stats.data_file_length = m_table->getDataLength(m_session);
			stats.index_file_length = m_table->getIndexLength(m_session, false);
		} else {
			// SHOW TABLE STATUS时没有加表锁
			stats.data_file_length = 0;
			stats.index_file_length = 0;
			Session *session = m_session;
			if (!session) {
				attachToTHD(ha_thd());
				session = ntse_db->getSessionManager()->allocSession("ha_ntse::info", m_conn, 1000);
				if (!session)
					detachFromTHD();
			}

			if (session) {
				try {
					m_table->lockMeta(session, IL_S, 1000, __FILE__, __LINE__);
					stats.data_file_length = m_table->getDataLength(session);
					stats.index_file_length = m_table->getIndexLength(session, false);
					m_table->unlockMeta(session, IL_S);
				} catch (NtseException &) {
					// 忽略异常
				}
				if (session != m_session) {
					ntse_db->getSessionManager()->freeSession(session);
					detachFromTHD();
				}
			}
		}
		stats.delete_length = 0;
		stats.check_time = 0;
		if (stats.records == 0) {
			stats.mean_rec_length = 0;
		} else {
			stats.mean_rec_length = (ulong) (stats.data_file_length / stats.records);
		}
	}
	if (flag & HA_STATUS_ERRKEY) {
		// REPLACE或INSERT/UPDATE语句中用于获取导致冲突的记录的RID
		if (m_iuSeq) {
			assert(m_session);
			dup_ref[0] = REF_ROWID;
			RID_WRITE(m_iuSeq->getScanHandle()->getCurrentRid(), dup_ref + 1);
			errkey = m_iuSeq->getDupIndex();
		} else
			errkey = m_dupIdx;
	}
	if ((flag & HA_STATUS_AUTO) && table->found_next_number_field) {
		stats.auto_increment_value = m_tblInfo->m_autoIncr.getAutoIncr();
	}
	DBUG_RETURN(0);
}

/**
 * 给存储引擎一些提示
 *
 * @param operation 提示信息
 * @return 总是返回0
 */
int ha_ntse::extra(enum ha_extra_function operation) {
	ftrace(ts.mysql, tout << this << operation);
	DBUG_ENTER("ha_ntse::extra");
	if (operation == HA_EXTRA_IGNORE_DUP_KEY)
		m_ignoreDup = true;
	else if (operation == HA_EXTRA_NO_IGNORE_DUP_KEY)
		m_ignoreDup = false;
	else if (operation == HA_EXTRA_WRITE_CAN_REPLACE || operation == HA_EXTRA_INSERT_WITH_UPDATE)
		m_replace = true;
	else if (operation == HA_EXTRA_WRITE_CANNOT_REPLACE)
		m_replace = false;
	DBUG_RETURN(0);
}

/**
 * 删除所有记录
 * 用于实现truncate，delete from TABLE_NAME命令也可能调用此接口
 *
 * @return 成功返回0，失败返回错误码
 */
int ha_ntse::delete_all_rows() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ha_ntse::delete_all_rows");

	assert(m_tblInfo->m_table->getMetaLock(m_session) == IL_S);
	try {
		m_tblInfo->m_table->upgradeMetaLock(m_session, IL_S, IL_X, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		reportError(e.getErrorCode(), e.getMessage(), false);
		DBUG_RETURN(HA_ERR_LOCK_WAIT_TIMEOUT);
	}
	assert(m_gotLock == IL_IX || m_gotLock == IL_X);
	if (m_gotLock == IL_IX) {
		m_wantLock = IL_X;
		try {
			m_tblInfo->m_table->upgradeLock(m_session, IL_IX, IL_X, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			reportError(e.getErrorCode(), e.getMessage(), false);
			DBUG_RETURN(HA_ERR_LOCK_WAIT_TIMEOUT);
		}
		m_gotLock = IL_X;
	}
	try {
		//truncate和delete from TABLE_NAME都有可能调用本函数，
		//而delete from TABLE_NAME不需要删除存在的压缩字典
		bool isTruncateOper = (m_thd->lex->sql_command == SQLCOM_TRUNCATE);
		ntse_db->truncateTable(m_session, m_table, false, isTruncateOper);
	} catch (NtseException &e) {
		int code = reportError(e.getErrorCode(), e.getMessage(), false);
		DBUG_RETURN(code);
	}

	// 如果有自增字段的情况，自增值设为1
	if (m_tblInfo->m_autoIncr.getAutoIncr() != 0)
		m_tblInfo->m_autoIncr.setAutoIncr(1);

	DBUG_RETURN(0);
}

/**
 * MySQL上层调用这一函数由存储引擎决定对将要执行的语句需要加什么样的表锁。
 * 比如支持行锁的存储引擎可以选择将表上的写锁改为读锁等；或者完全不要
 * MySQL的表锁，自己实现表锁；或者加多个表的锁（比如MERGE存储引擎）。
 *
 * 在查询处理中途提前放锁时MySQL也会调用这一函数，比如在SELECT时，对那些
 * 只返回一条记录的const表。但在语句结束后释放所有锁时不会调用本函数。
 *
 * 注: table->in_use可能指向另一个线程（在mysql_lock_abort_for_thread()中调用时）
 *
 * 相关代码: thr_lock.c, get_lock_data()
 *
 * @param thd 当前连接
 * @param to 输出参数，用于存储锁对象
 * @param lock_type 锁模式
 * @return 锁对象数组尾指针
 */
THR_LOCK_DATA** ha_ntse::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
	ftrace(ts.mysql, tout << this << thd << to << lock_type);
	if (lock_type != TL_IGNORE && m_mysqlLock.type == TL_UNLOCK) {
		enum_sql_command sql_command = thd->lex->sql_command;
		if (lock_type >= TL_WRITE_ALLOW_WRITE) {
			// 允许最常用的INSERT/UPDATE/DELETE语句并行执行，其它的复杂情况
			// 不处理，并发度差一点也没问题，因为不常用，还是保证安全的好
			if (sql_command == SQLCOM_LOCK_TABLES || (thd_in_lock_tables(thd) && !thd->lex->requires_prelocking())) {
				// 如果是LOCK TABLES语句则加X锁，有些情况下不是LOCk TABLES语句，但如果是在
				// LOCK TABLES语句之内还要加锁，一定是要恢复当初LOCK TABLES语句要加的锁，
				// 因此也按照LOCK TABLES语句一样处理
				// 这样的一个例子是:
				// lock table t1 write, t3 read;
				// alter table t1 modify i int default 1;
				//修复bug#28175，#28180
				//1.对于trigger引起的“假”的lock tables，thd->lex->requires_prelocking()为true，
				//对于真的lock tables，为false
                //2.对于存储过程时，导致thd_in_lock_tables(thd)为true，但这时
				//thd->lex->requires_prelocking()也会为true，识别出是假的lock tables。
                //修复bug#28380增强断言，会话设置了low_priority_updates=1,LOCK TABLE WRITE的时候会导致lock_type=TL_WRITE_LOW_PRIORITY
				assert(lock_type == TL_WRITE || lock_type == TL_WRITE_LOW_PRIORITY);
				m_wantLock = IL_X;
			} else if (sql_command == SQLCOM_UPDATE
				|| sql_command == SQLCOM_DELETE
				|| sql_command == SQLCOM_INSERT || sql_command == SQLCOM_INSERT_SELECT) {
				m_wantLock = IL_IX;
				lock_type = TL_WRITE_ALLOW_WRITE;
			} else if (sql_command == SQLCOM_REPLACE || sql_command == SQLCOM_REPLACE_SELECT) {
				char *dbName = thd->lex->query_tables->get_db_name();				
				char *tableName = thd->lex->query_tables->get_table_name();				
				string path = string(ntse_db->getConfig()->m_basedir) + NTSE_PATH_SEP + dbName + NTSE_PATH_SEP + tableName;
				ParFileParser parser(path.c_str());
				m_wantLock = parser.isPartitionByLogicTabPath() ? IL_X : IL_IX;										
				lock_type = TL_WRITE_ALLOW_WRITE;
			} else if (sql_command == SQLCOM_TRUNCATE)
				m_wantLock = IL_X;
			else if (sql_command == SQLCOM_DELETE_MULTI || sql_command == SQLCOM_UPDATE_MULTI) {
				// 多表UPDATE/DELETE用SIX锁的原因是形如
				// DELETE t1 FROM t1 WHERE...
				// 之类的多表DELETE语句会在结束扫描之后再来调用position取当前记录RID，
				// 而这时行锁已经释放，因此只能用表锁来保证
				// 注意这可能导致行锁与表锁之间产生死锁，产生死锁的场景为
				// 需要缓存RID的SELECT操作，在持有S行锁的情况下升级表锁IS->S
				// 多表DELETE语句在持有SIX表锁的情况下加X行锁
				// 这一死锁通过表锁升级的超时解决
				m_wantLock = IL_SIX;
				lock_type = TL_WRITE_ALLOW_READ;
			} else if (sql_command == SQLCOM_ALTER_TABLE || sql_command == SQLCOM_CREATE_INDEX
				|| sql_command == SQLCOM_DROP_INDEX)
				m_wantLock = IL_IS;
			else {
				// 处理当前服务是Slave的情况，这个时候要对表家SIX锁
				// 主要是处理Slave在复制update操作可能会出现的先scan，再结束scan
				// 得到rowId之后会直接来update，这个时候为了能定位，会直接使用m_lastRow的信息
				// 为了保证这个信息可靠，需要加表锁，禁止其他写操作
				// 这个时候sql_command传入的是SQLCOM_END，基于这个值，判断分支会走到这里
				// 对于Slave的读操作，不应该走到这里
				// 由于m_thd未赋值，需要使用参数thd判断是否slave
				if (isSlaveThread(thd))
					m_wantLock = IL_SIX;
				else
					m_wantLock = IL_X;
			}
		} else {
			if (sql_command == SQLCOM_LOCK_TABLES || (thd_in_lock_tables(thd) && !thd->lex->requires_prelocking())) {
				m_wantLock = IL_S;
			} else
				m_wantLock = IL_IS;
		}
		m_mysqlLock.type = lock_type;
	}
	*to++ = &m_mysqlLock;
	m_beginTime = System::microTime();
	return to;
}

/**
 * 一般情况下，任何语句开始操作一张表之前都会调用这一函数让存储引擎加锁，
 * 语句结束后会调用这一函数让存储引擎解锁。
 * 但如果语句用LOCK TABLES/UNLOCK TABLES括起，则在LOCK TABLES时调用store_lock和external_lock加锁，
 * UNLOCK TABLES时放锁，期间不再会调用store_lock和external_lock函数。
 *
 * @param thd 当前连接
 * @param lock_type 为F_UNLCK表示解锁，其它情况为读写锁
 * @return 成功返回0，等锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT，取不到会话返回HA_ERR_NO_CONNECTION
 */
int ha_ntse::external_lock(THD *thd, int lock_type) {
	ftrace(ts.mysql, tout << this << thd << lock_type);
	DBUG_ENTER("ha_ntse::external_lock");
	if (lock_type == F_UNLCK) {
		if (m_iuSeq) {
			m_table->freeIUSequenceDirect(m_iuSeq);
			m_iuSeq = NULL;
		}

		if (!m_thd)	// mysql_admin_table中没有检查加锁是否成功
			DBUG_RETURN(0);
		assert(m_session);

		endScan();

		// 在进行OPTIMIZE等操作时已经释放了表锁
		if (m_gotLock != IL_NO) {
			m_table->unlock(m_session, m_gotLock);
			m_gotLock = IL_NO;
		}
		if (m_table->getMetaLock(m_session) != IL_NO)
			m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;

		assert(m_beginTime);
		ntse_handler_use_count++;
		ntse_handler_use_time += System::microTime() - m_beginTime;
		m_beginTime = 0;

		if (getTHDInfo(m_thd)->m_handlers.getSize() == 1) {
			m_conn->setStatus("Idle");
			getTHDInfo(m_thd)->resetNextCreateArgs();
		}

		if (m_rowCache) {
			delete m_rowCache;
			m_rowCache = NULL;
		}

		detachFromTHD();
	} else {
		assert(m_gotLock == IL_NO);
		assert(!m_rowCache);

		// ALTER TABLE时的临时表不会调用store_lock，在这里修正下加锁策略
		if (m_wantLock == IL_NO) {
			if (lock_type == F_RDLCK)
				m_wantLock = IL_S;
			else
				m_wantLock = IL_X;
		}

		attachToTHD(thd);

		// 分配会话
		assert(!m_session);
		m_session = ntse_db->getSessionManager()->allocSession("ha_ntse::external_lock", m_conn, 100);
		if (!m_session) {
			detachFromTHD();
			DBUG_RETURN(HA_ERR_NO_CONNECTION);
		}

		// 加表锁
		m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
		try {
			if (m_wantLock == IL_X)
				ntse_db->getSyslog()->log(EL_LOG, "acquire IL_X on table %s. SQL:\n%s", m_table->getTableDef()->m_name, currentSQL());
			m_table->lock(m_session, m_wantLock, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			m_gotLock = m_wantLock;
		} catch (NtseException &e) {
			reportError(e.getErrorCode(), e.getMessage(), false);
			m_table->unlockMeta(m_session, IL_S);
			ntse_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			detachFromTHD();
			DBUG_RETURN(HA_ERR_LOCK_WAIT_TIMEOUT);
		}
		if (getTHDInfo(m_thd)->m_handlers.getSize() == 1) {
			m_conn->setStatus("Busy");
			m_conn->getLocalStatus()->countIt(OPS_OPER);
		}

		if (!m_beginTime)
			m_beginTime = System::microTime();
	}
	DBUG_RETURN(0);
}

/** 在LOCK TABLES括起的语句中MySQL在语句开始时会调用此函数，语句结束时不会调用
 *
 * @param thd 连接
 * @param lock_type 对表加的锁
 * @return 总是返回0
 */
int ha_ntse::start_stmt(THD *thd, thr_lock_type lock_type) {
	ftrace(ts.mysql, tout << this << thd << lock_type);
	DBUG_ENTER("ha_ntse::start_stmt");
	if (thd != m_thd)
		DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "THD changed during sql processing", false));
	THDInfo *info = getTHDInfo(m_thd);
	if (info->m_handlers.getHeader()->getNext()->get() == this) {
		m_conn->setStatus("Busy");
		m_conn->getLocalStatus()->countIt(OPS_OPER);
	}
	DBUG_RETURN(0);
}

/**
 * 删除表
 * 在删除表之前，所有对表的引用都已经被关闭
 *
 * @param name 表文件路径，不含后缀名
 * @return 成功返回0，表不存在返回HA_ERR_NO_SUCH_TABLE，其它错误返回HA_ERR_GENERIC
 */
int ha_ntse::delete_table(const char *name) {
	ftrace(ts.mysql, tout << this << name);
	DBUG_ENTER("ha_ntse::delete_table");

	attachToTHD(ha_thd());

	int ret = 0;

	m_session = ntse_db->getSessionManager()->allocSession("ha_ntse::delete_table", m_conn, 100);
	if (!m_session) {
		ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
	} else {
		char *name_copy = System::strdup(name);
		normalizePathSeperator(name_copy);
		try {
			ntse_db->dropTable(m_session, name_copy, ntse_table_lock_timeout);
		} catch (NtseException &e) {
			ret = reportError(e.getErrorCode(), e.getMessage(), false);
		}
		delete []name_copy;
	}
	if (m_session) {
		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}

	detachFromTHD();

	DBUG_RETURN(ret);
}

/**
 * 重命名表。调用此函数时handler对象为刚刚创建没有open的对象
 *
 * @param from 表文件原路径
 * @param to 表文件新路径
 */
int ha_ntse::rename_table(const char *from, const char *to) {
	ftrace(ts.mysql, tout << this << from << to);
	DBUG_ENTER("ha_ntse::rename_table ");
	assert(!m_session);

	attachToTHD(ha_thd());

	int ret = 0;

	m_session = ntse_db->getSessionManager()->allocSession("ha_ntse::rename_table", m_conn, 100);
	if (!m_session) {
		ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
	} else {
		char *from_copy = System::strdup(from);
		char *to_copy = System::strdup(to);
		normalizePathSeperator(from_copy);
		normalizePathSeperator(to_copy);
		try {
			ntse_db->renameTable(m_session, from_copy, to_copy, ntse_table_lock_timeout);
		} catch (NtseException &e) {
			ret = reportError(e.getErrorCode(), e.getMessage(), false);
		}
		delete []from_copy;
		delete []to_copy;
	}
	if (m_session) {
		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}

	detachFromTHD();

	DBUG_RETURN(ret);
}

/**
 * 估计指定索引中在[min_key, max_key]之间的记录数
 *
 * @param inx 第几个索引
 * @param min_key 下限，可能为NULL
 * @param max_key 上限，可能为NULL
 */
ha_rows ha_ntse::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
	ftrace(ts.mysql, tout << this << inx << min_key << max_key);
	DBUG_ENTER("ha_ntse::records_in_range");

	SubRecord minSr, maxSr;
	SubRecord *pMinSr = NULL, *pMaxSr = NULL;
	bool includeMin = false, includeMax = false;
	if (min_key) {
		transKey(inx, min_key->key, min_key->keypart_map, &minSr);
		pMinSr = &minSr;
		includeMin = isRkeyFuncInclusived(min_key->flag, true);
	}
	if (max_key) {
		transKey(inx, max_key->key, max_key->keypart_map, &maxSr);
		pMaxSr = &maxSr;
		includeMax = isRkeyFuncInclusived(max_key->flag, false);
	}
	u64 rows = m_table->getIndice()->getIndex(inx)->recordsInRange(m_session,
		pMinSr, includeMin, pMaxSr, includeMax);
	DBUG_RETURN(rows);
}

/** NTSE非标准索引定义信息 */
struct IdxDefEx {
	const char	*m_name;		/** 索引名 */
	s8			m_splitFactor;	/** 分裂系数 */

	IdxDefEx(const char *name, s8 splitFactor) {
		m_name = System::strdup(name);
		m_splitFactor = splitFactor;
	}
};

/** NTSE非标准表定义信息 */
struct TblDefEx {
	bool	m_useMms;			/** 是否启用MMS */
	bool	m_cacheUpdate;		/** 是否缓存更新 */
	u32		m_updateCacheTime;	/** 更新缓存时间，单位秒 */
	u8		m_pctFree;			/** 变长堆预留空间百分比 */
	Array<ColGroupDef *>  *m_colGrpDef;/* 属性组定义*/
	bool	m_compressLobs;		/** 是否压缩大对象 */
	bool    m_compressRows;     /** 是否启用记录压缩 */
	u8      m_compressThreshold;/** 记录压缩阀值 */
	uint    m_dictionarySize;   /** 记录压缩全局字典大小 */
	u16     m_dictionaryMinLen; /** 记录压缩全局字典项最小长度 */
	u16     m_dictionaryMaxLen; /** 记录压缩全局字典项最大长度*/
	bool    m_fixLen;           /** 是否使用定长堆 */
	Array<char *>	*m_cachedCols;	/** 缓存更新的属性 */
	Array<IdxDefEx *>	*m_idxDefEx;/** 非标准索引定义信息 */
	u16		m_incrSize;			/** 文件扩展页面数 */
	bool	m_indexOnly;		/** 只有索引的表 */

	TblDefEx() {
		m_useMms = TableDef::DEFAULT_USE_MMS;
		m_cacheUpdate = TableDef::DEFAULT_CACHE_UPDATE;
		m_updateCacheTime = TableDef::DEFAULT_UPDATE_CACHE_TIME;
		m_colGrpDef = NULL;
		m_pctFree = TableDef::DEFAULT_PCT_FREE;
		m_compressLobs = TableDef::DEFAULT_COMPRESS_LOBS;
		m_compressRows = TableDef::DEFAULT_COMPRESS_ROW;
		m_compressThreshold = RowCompressCfg::DEFAULT_ROW_COMPRESS_THRESHOLD;
		m_dictionarySize = RowCompressCfg::DEFAULT_DICTIONARY_SIZE;
		m_dictionaryMinLen = RowCompressCfg::DEFAULT_DIC_ITEM_MIN_LEN;
		m_dictionaryMaxLen = RowCompressCfg::DEFAULT_DIC_ITEM_MAX_LEN;
		m_fixLen = TableDef::DEFAULT_FIX_LEN;
		m_cachedCols = NULL;
		m_incrSize = (u16)ntse_incr_size;
		m_idxDefEx = NULL;
		m_indexOnly = TableDef::DEFAULT_INDEX_ONLY;
	}

	~TblDefEx() {
		if (m_cachedCols) {
			for (size_t i = 0; i < m_cachedCols->getSize(); i++)
				delete []((*m_cachedCols)[i]);
			delete m_cachedCols;
		}
		if (m_idxDefEx) {
			for (size_t i = 0; i < m_idxDefEx->getSize(); i++)
				delete (*m_idxDefEx)[i];
			delete m_idxDefEx;
		}
		if (m_colGrpDef) {		
			for (size_t i = 0; i < m_colGrpDef->getSize(); i++) {
				delete (*m_colGrpDef)[i];
			}
			delete m_colGrpDef;
		}
	}
};

/**
 * 创建表(但不打开表)
 *
 * @param name 表文件路径，不包含后缀
 * @param table_arg 表定义
 * @param create_info 额外的建表选项，包括完整的建表语句，注释等
 * @return 成功返回0，失败返回非0
 */
int ha_ntse::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {
	ftrace(ts.mysql, tout << this << name << table_arg << create_info);
	DBUG_ENTER("ha_ntse::create");
	char *name_copy = NULL, *schema_name = NULL, *table_name = NULL;
	Session *session = NULL;
	char *comment = NULL;
	TblDefEx *extraArgs = NULL;
	TableDef *tableDef = NULL;
	int ret = 0;
	
	string tableName(name);
	string firstParName, lastParName;
	try {
		if (!ParFileParser::isPartitionByPhyicTabName(tableName.c_str())) {
			lastParName = tableName;
		} else {
			std::vector<string> parFileStrings;
			ParFileParser parser(tableName.c_str());
			parser.parseParFile(parFileStrings);
			firstParName = parFileStrings[0].substr(parFileStrings[0].find(PARTITION_TABLE_LABLE));
			lastParName = parFileStrings[parFileStrings.size() - 1];
			if (parFileStrings.size() == 1) {
				lastParName = tableName;
			}			
		}		
				
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), true);
	}

	//对于auto_increment在建表之初就指定基准值，ntse暂不支持均从1开始累进不支持跳变
	//TODO：支持设置从指定自增值开始累进；
	try {
		if (create_info->auto_increment_value > 0)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Setting auto_increment variable is not supported");
	} catch (NtseException &e) {
		reportError(e.getErrorCode(), e.getMessage(), false, true);
	}

	attachToTHD(ha_thd());

	name_copy = System::strdup(name);
	normalizePathSeperator(name_copy);

	Table::getSchemaTableFromPath(name_copy, &schema_name, &table_name);

	TableDefBuilder tdb(0, schema_name, table_name);

	try {
		// 增加字段定义
		for (uint i = 0; i < table_arg->s->fields; i++) {
			Field *field = table_arg->field[i];
			bool notnull = !field->maybe_null();
			if (field->flags & PRI_KEY_FLAG) {
				assert(notnull);	// MySQL会自动将所有主键属性变成NOT NULL
			}

			bool isValueType = true; // 是否数值类型
			PrType prtype;
			if (field->flags & UNSIGNED_FLAG)
				prtype.setUnsigned();

			ColumnType type;
			switch (field->real_type()) {
			case MYSQL_TYPE_ENUM:
				if (field->row_pack_length() == 1)
					type = CT_TINYINT;
				else {
					assert(field->row_pack_length() == 2);
					type = CT_SMALLINT;
				}
				break;
			case MYSQL_TYPE_TINY:
			case MYSQL_TYPE_YEAR:
				type = CT_TINYINT;
				break;
			case MYSQL_TYPE_SHORT:
				type = CT_SMALLINT;
				break;
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_NEWDATE:
			case MYSQL_TYPE_TIME:
			case MYSQL_TYPE_INT24:
				type = CT_MEDIUMINT;
				break;
			case MYSQL_TYPE_TIMESTAMP:
				if (table_arg->timestamp_field == field && ((Field_timestamp*)field)->get_auto_set_type() != TIMESTAMP_NO_AUTO_SET)
					NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Auto update timestamp is not supported");
			case MYSQL_TYPE_LONG:
				type = CT_INT;
				break;
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_LONGLONG:
				type = CT_BIGINT;
				break;
			case MYSQL_TYPE_FLOAT:
				type = CT_FLOAT;
				break;
			case MYSQL_TYPE_DOUBLE:
				type = CT_DOUBLE;
				break;
			case MYSQL_TYPE_DECIMAL:
			case MYSQL_TYPE_NEWDECIMAL:
				type = CT_DECIMAL;
				prtype.m_precision = ((Field_new_decimal *)field)->precision;
				prtype.m_deicmal = field->decimals();
				break;
			case MYSQL_TYPE_VARCHAR:
			case MYSQL_TYPE_VAR_STRING:
				if (field->binary()) { // varbinary类型
					type = CT_VARBINARY;
				} else {
					type = CT_VARCHAR;
				}
				isValueType = false;
				break;
			case MYSQL_TYPE_STRING:
				if (field->binary()) { // binary类型
					type = CT_BINARY;
				} else {
					type = CT_CHAR;
				}
				isValueType = false;
				break;
			case MYSQL_TYPE_MEDIUM_BLOB:
				type = CT_MEDIUMLOB;
				isValueType = false;
				break;
			case MYSQL_TYPE_BLOB: {
					uint32 pack_len = ((Field_blob *)field)->pack_length_no_ptr();
					if (pack_len == 2)
						type = CT_SMALLLOB;
					else if (pack_len == 3)
						type = CT_MEDIUMLOB;
					else
						NTSE_THROW(NTSE_EC_NOT_SUPPORT, "%s is not supported", pack_len == 1? "TINYBLOB/TINYTEXT": "LONGBLOB/LONGTEXT");
					isValueType = false;
				}
				break;
			default:
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unsupported data type of field ", field->field_name);
				break;
			}
			if (type == CT_CHAR || type == CT_VARCHAR || type == CT_BINARY || type == CT_VARBINARY) {
				if (field->field_length == 0)
					NTSE_THROW(NTSE_EC_NOT_SUPPORT, "char/varchar/binary/varbinary with zero size is not supported: %s.", field->field_name);
				tdb.addColumnS(field->field_name, type, field->field_length, !notnull, getCollation(field->charset()));
			} else if (!isValueType) {
				tdb.addColumn(field->field_name, type, !notnull);
			} else {
				tdb.addColumnN(field->field_name, type, prtype, !notnull);
			}
		}
		// 增加索引定义
		for (uint i = 0; i < table_arg->s->keys; i++) {
			KEY *key = table_arg->key_info + i;
			Array<u16> indexColumns;
			bool unique = key->flags & HA_NOSAME;
			bool primary = table_arg->s->primary_key == i;
			for (uint j = 0; j < key->key_parts; j++) {
				KEY_PART_INFO *key_part = key->key_part + j;
				if (isPrefix(table_arg, key_part))
					NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Prefix key is not supported: %s", key->name);
				indexColumns.push(key->key_part[j].field->field_index);
			}
			tdb.addIndex(key->name, primary, unique, false, indexColumns);
		}
		tableDef = tdb.getTableDef();
		if (tableDef->m_maxRecSize != table_arg->s->reclength)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unexpected record size %d, expected %d",
				table_arg->s->reclength, tableDef->m_maxRecSize);
		if (create_info->row_type != ROW_TYPE_DEFAULT) {
			if (tableDef->m_recFormat == REC_FIXLEN && create_info->row_type != ROW_TYPE_FIXED)
				DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "Row type of fixed length table should be ROW_TYPE_DEFAULT or ROW_TYPE_FIXED.", false));
			else if (tableDef->m_recFormat == REC_VARLEN && create_info->row_type != ROW_TYPE_DYNAMIC)
				DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "Row type of variable length table should be ROW_TYPE_DEFAULT or ROW_TYPE_DYNAMIC.", false));
		}

		session = ntse_db->getSessionManager()->allocSession("ha_ntse::create", m_conn);
		
		do {
			if (SQLCOM_ALTER_TABLE == m_thd->lex->sql_command) {
				//如果是ALTER TABLE操作，则先获得原表的非标准表定义信息，firstParName用于支持分区表，如为非分区表该字符串为空；
				char *schemaName = System::strdup(m_thd->lex->query_tables->get_db_name());
				assert(schemaName && strlen(schemaName) > 0);
				char *sourceTblName = System::strdup(m_thd->lex->query_tables->get_table_name());
				assert(sourceTblName && strlen(sourceTblName) > 0);
				string path = string(ntse_db->getConfig()->m_basedir) + NTSE_PATH_SEP + schemaName + NTSE_PATH_SEP + sourceTblName + firstParName;
				try {
					bool hasDict = false;
					pair<TblDefEx*, TableDef*> *tableDefPair = queryTableDef(session, path.c_str(), &hasDict);
					if (hasDict) {
						sql_print_warning("The format of records may be changed to uncompressed when execute " \
							"ALTER TABLE operation.");
					}
					extraArgs = tableDefPair->first;
					fixedTblDefEx(tableDef, tableDefPair->second, extraArgs);
					delete tableDefPair->second;
					delete tableDefPair;
				} catch(NtseException &e) {
					delete []schemaName;
					delete [] sourceTblName;
					if (e.getErrorCode() != NTSE_EC_FILE_NOT_EXIST) {
						throw e;
					} else {//可能原表不是使用NTSE引擎，如果是找不到表则忽略这个异常
						break;
					}
				}
				delete [] schemaName;
				delete [] sourceTblName;
			} else {
				//解析非标准表定义参数
				extraArgs = parseCreateArgs(tableDef, getTHDInfo(ha_thd())->m_nextCreateArgs);
			}
			assert(extraArgs);
			applyCreateArgs(tableDef, extraArgs);
			delete extraArgs;
			extraArgs = NULL;
		} while (0);

		delete []comment;
		comment = NULL;

		if (tableDef->m_useMms && tableDef->m_cacheUpdate && !ntse_enable_mms_cache_update)
			reportError(NTSE_EC_GENERIC, "Mms cache update feature has been disabled(Notice if ntse_binlog_method is \"direct\" or log_slave_updates is false, "
			"this feature is automatically disabled.)", false, true);

		ntse_db->createTable(session, name_copy, tableDef);

		delete tableDef;
		tableDef = NULL;
		if (tableName == lastParName) {
			//非分区表或最后一张分区表创建成功后，重置默认非标准表定义参数
			getTHDInfo(ha_thd())->resetNextCreateArgs();
		}

		

	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
		if (comment) {
			delete []comment;
			comment = NULL;
		}
		if (extraArgs) {
			delete extraArgs;
			extraArgs = NULL;
		}
		if (tableDef) {
			delete tableDef;
			tableDef = NULL;
		}
		getTHDInfo(ha_thd())->resetNextCreateArgs();
	}
	if (session)
		ntse_db->getSessionManager()->freeSession(session);

	detachFromTHD();

	delete []name_copy;
	delete []schema_name;
	delete []table_name;
	DBUG_RETURN(ret);
}

/**
 * 查询标准表定义和非标准表定义
 * @param session       会话 
 * @param sourceTblPath 表路径(相对于基础路径)
 * @param hasDict OUT 表是否含有字典
 * @throw NtseException 打开表出错或加元数据锁超时
 * @return 标准表定义和非标准表定义pair对象
 */
pair<TblDefEx*, TableDef*>* ha_ntse::queryTableDef(Session *session, const char *sourceTblPath, 
												   bool *hasDict /*=NULL*/) throw(NtseException) {
	assert(session && sourceTblPath);
	Table *tableInfo = NULL;
	TblDefEx *tblDefEx = NULL;
	try {
		tableInfo = ntse_db->openTable(session, sourceTblPath);
		tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
	
		if (hasDict)
			*hasDict = tableInfo->hasCompressDict();
		TableDef *tblDef = tableInfo->getTableDef(true, session);
		tblDefEx = new TblDefEx();
		tblDefEx->m_useMms = tblDef->m_useMms;
		tblDefEx->m_cacheUpdate = tblDef->m_cacheUpdate;
		tblDefEx->m_updateCacheTime = tblDef->m_updateCacheTime;
		//cached column
		for (u16 i = 0; i < tblDef->m_numCols; i++) {
			ColumnDef *c = tblDef->m_columns[i];
			if (c->m_cacheUpdate) {
				if (!tblDefEx->m_cachedCols)
					tblDefEx->m_cachedCols = new Array<char *>();
				tblDefEx->m_cachedCols->push(System::strdup(c->m_name));
			}
		}
		tblDefEx->m_compressLobs = tblDef->m_compressLobs;
		tblDefEx->m_pctFree = (u8)tblDef->m_pctFree;
		tblDefEx->m_indexOnly = tblDef->m_indexOnly;
		tblDefEx->m_fixLen = tblDef->m_fixLen;
		//split_factors
		if (tblDef->m_numIndice > 0) {
			for (u16 i = 0; i < tblDef->m_numIndice; i++) {
				IndexDef *idx = tblDef->m_indice[i];
				IdxDefEx *idxDefEx = new IdxDefEx(idx->m_name, (int)idx->m_splitFactor);
				if (!tblDefEx->m_idxDefEx)
					tblDefEx->m_idxDefEx = new Array<IdxDefEx *>();
				tblDefEx->m_idxDefEx->push(idxDefEx);
			}
		}
		tblDefEx->m_incrSize = tblDef->m_incrSize;
		tblDefEx->m_compressRows = tblDef->m_isCompressedTbl;
		tblDefEx->m_fixLen = (tblDef->m_recFormat == REC_FIXLEN);
		if (tblDef->m_rowCompressCfg) {
			tblDefEx->m_compressThreshold = tblDef->m_rowCompressCfg->compressThreshold();
			tblDefEx->m_dictionarySize = tblDef->m_rowCompressCfg->dicSize();
			tblDefEx->m_dictionaryMinLen = tblDef->m_rowCompressCfg->dicItemMinLen();
			tblDefEx->m_dictionaryMaxLen = tblDef->m_rowCompressCfg->dicItemMaxLen();
		}
		//column groups
		if (tblDef->m_colGrps) {
			assert(tblDef->m_numColGrps > 0);
			tblDefEx->m_colGrpDef = new Array<ColGroupDef *>();
			for (u16 i = 0; i < tblDef->m_numColGrps; i++) {
				tblDefEx->m_colGrpDef->push(new ColGroupDef(tblDef->m_colGrps[i]));
			}
		}
	} catch (NtseException &e) {
		if (tableInfo)
			ntse_db->closeTable(session, tableInfo);
		throw e;
	}
	TableDef *oldTblDefCopy = new TableDef(tableInfo->getTableDef());
	tableInfo->unlockMeta(session, IL_S);
	ntse_db->closeTable(session, tableInfo);

	pair<TblDefEx*, TableDef*> *r = new pair<TblDefEx*, TableDef*>(tblDefEx, oldTblDefCopy);
	return r;
}

/** 解析CREATE TABLE非标准信息
 * @param tableDef 表定义
 * @param str 字符串表示的非标准信息，可能为NULL
 * @return 若指定了非标准信息则返回解析结果，否则返回默认值
 * @throw NtseException 格式错误
 */
TblDefEx* ha_ntse::parseCreateArgs(TableDef *tableDef, const char *str) throw(NtseException) {
	if (!str || strlen(str) == 0)
		return new TblDefEx();
	TblDefEx *args = new TblDefEx();
	Array<char *> *items = Parser::parseList(str, ';');
	try {
		for (size_t i = 0; i < items->getSize(); i++) {
			char *name, *value;
			Parser::parsePair((*items)[i], ':', &name, &value);
			try {
				if (!strcmp(name, "usemms")) {
					args->m_useMms = Parser::parseBool(value);
				} else if (!strcmp(name, "cache_update")) {
					args->m_cacheUpdate = Parser::parseBool(value);
				} else if (!strcmp(name, "update_cache_time")) {
					args->m_updateCacheTime = Parser::parseInt(value, TableDef::MIN_UPDATE_CACHE_TIME, TableDef::MAX_UPDATE_CACHE_TIME);
				} else if (!strcmp(name, "cached_columns")) {
					if (args->m_cachedCols)
						NTSE_THROW(NTSE_EC_GENERIC, "Can not set cached_columns multiple times.");
					args->m_cachedCols = Parser::parseList(value, ',');
				} else if (!strcmp(name, "column_groups")) {
					args->m_colGrpDef = ColumnGroupParser::parse(tableDef, value);
					if (0 == args->m_colGrpDef->getSize()) 
						NTSE_THROW(NTSE_EC_GENERIC, "Could not define empty column group.");
				} else if (!strcmp(name, "compress_lobs")) {
					args->m_compressLobs = Parser::parseBool(value);
				} else if (!strcmp(name, "compress_rows")) {
					args->m_compressRows = Parser::parseBool(value);
				} else if (!strcmp(name, "compress_threshold")) {
					uint threshold = (uint)Parser::parseInt(value);
					if (!RowCompressCfg::validateCompressThreshold(threshold))
						NTSE_THROW(NTSE_EC_GENERIC, INVALID_COMPRESS_THRESHOLD_INFO, threshold);
					args->m_compressThreshold = (u8)threshold;
				} else if (!strcmp(name, "dictionary_size")) {
					uint dicSize = (uint)Parser::parseInt(value);
					if (!RowCompressCfg::validateDictSize(dicSize))
						NTSE_THROW(NTSE_EC_GENERIC, INVALID_DIC_SIZE_INFO, dicSize, RowCompressCfg::MIN_DICTIONARY_SIZE, RowCompressCfg::MAX_DICTIONARY_SIZE);
					args->m_dictionarySize = dicSize;
				} else if (!strcmp(name, "dictionary_min_len")) {
					uint minLen = (uint)Parser::parseInt(value);
					if (!RowCompressCfg::validateDictMinLen(minLen))
						NTSE_THROW(NTSE_EC_GENERIC, INVALID_DIC_MIN_LEN_INFO, minLen, RowCompressCfg::MIN_DIC_ITEM_MIN_LEN, RowCompressCfg::MAX_DIC_ITEM_MIN_LEN);
					args->m_dictionaryMinLen = (u16)minLen;
				} else if (!strcmp(name, "dictionary_max_len")) {
					uint maxLen = (uint)Parser::parseInt(value);
					if (!RowCompressCfg::validateDictMaxLen(maxLen))
						NTSE_THROW(NTSE_EC_GENERIC, INVALID_DIC_MAX_LEN_INFO, maxLen, RowCompressCfg::MIN_DIC_ITEM_MAX_LEN, RowCompressCfg::MAX_DIC_ITEM_MAX_LEN);
					args->m_dictionaryMaxLen = (u16)maxLen;
				} else if (!strcmp(name, "fix_len")) {
					args->m_fixLen = Parser::parseBool(value);
				} else if (!strcmp(name, "heap_pct_free")) {
					args->m_pctFree = Parser::parseInt(value, TableDef::MIN_PCT_FREE, TableDef::MAX_PCT_FREE);
				} else if (!strcmp(name, "index_only")) {
					args->m_indexOnly = Parser::parseBool(value);
				} else if (!strcmp(name, "split_factors")) {
					if (args->m_idxDefEx)
						NTSE_THROW(NTSE_EC_GENERIC, "Can not set split_factors multiple times.");
					Array<char *> *pairs = Parser::parseList(value, ',');
					args->m_idxDefEx = new Array<IdxDefEx *>();
					try {
						for (size_t i = 0; i < pairs->getSize(); i++) {
							char *str = (*pairs)[i];
							char *idxName, *splitFactorStr;
							Parser::parsePair(str, ':', &idxName, &splitFactorStr);
							try {
								s8 splitFactor = (s8)Parser::parseInt(splitFactorStr, IndexDef::SMART_SPLIT_FACTOR, IndexDef::MAX_SPLIT_FACTOR);
								if (splitFactor < IndexDef::MIN_SPLIT_FACTOR && splitFactor != IndexDef::SMART_SPLIT_FACTOR)
									NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Invalid SPLIT_FACTOR: %d, should be in [%d, %d] or %d.",
										splitFactor, IndexDef::MIN_SPLIT_FACTOR, IndexDef::MAX_SPLIT_FACTOR, IndexDef::SMART_SPLIT_FACTOR);
								args->m_idxDefEx->push(new IdxDefEx(idxName, splitFactor));
							} catch (NtseException &e) {
								delete []idxName;
								delete []splitFactorStr;
								throw e;
							}
							delete []idxName;
							delete []splitFactorStr;
						}
					} catch (NtseException &e) {
						for (size_t i = 0; i < pairs->getSize(); i++)
							delete []((*pairs)[i]);
						delete pairs;
						throw e;
					}
				} else if (!strcmp(name, "incr_size")) {
					args->m_incrSize = Parser::parseInt(value, TableDef::MIN_INCR_SIZE, TableDef::MAX_INCR_SIZE);
					if (!TableDef::isIncrSizeValid(args->m_incrSize)) {
						NTSE_THROW(NTSE_EC_GENERIC, "Invalid INCR_SIZE: %d, should be in range [%d, %d].", args->m_incrSize, TableDef::MIN_INCR_SIZE, TableDef::MAX_INCR_SIZE);
					}
				} else
					NTSE_THROW(NTSE_EC_GENERIC, "%s is not a valid setting", name);
			} catch (NtseException &e) {
				delete []name;
				delete []value;
				throw e;
			}
			delete []name;
			delete []value;
		}
	} catch (NtseException &e) {
		for (size_t i = 0; i < items->getSize(); i++)
			delete []((*items)[i]);
		delete items;
		delete args;
		throw e;
	}
	return args;
}

/**
 * 根据新旧表定义修正非标准表信息
 * @param newTableDef 新表定义
 * @param oldTableDef 旧表定义
 * @param args        INOUT，非标准表信息
 */
void ha_ntse::fixedTblDefEx(const TableDef *newTableDef, const TableDef *oldTableDef, TblDefEx *args) {
	if (args->m_cachedCols) {
		Array<char *> *newCachedColArr = new Array<char *>();
		for (size_t i = 0; i < args->m_cachedCols->getSize(); i++) {
			char *name = (*args->m_cachedCols)[i];
			ColumnDef *col = newTableDef->getColumnDef(name);
			if (col) {
				newCachedColArr->push(name);
			} else {//cached update的列已经被删除
				delete [] name;
			}
		}
		delete args->m_cachedCols;
		args->m_cachedCols = newCachedColArr;
	}
	if (args->m_idxDefEx) {
		Array<IdxDefEx *> *newIdxArr = new Array<IdxDefEx *>();
		for (size_t i = 0; i < args->m_idxDefEx->getSize(); i++) {
			IdxDefEx *idxDefEx = (*args->m_idxDefEx)[i];
			IndexDef *idx = newTableDef->getIndexDef(idxDefEx->m_name);
			if (!idx) {//索引已经被删除
				delete idxDefEx;
			} else {
				newIdxArr->push(idxDefEx);
			}
		}
		delete args->m_idxDefEx;
		args->m_idxDefEx = newIdxArr;
	}	
	//修正属性组定义
	if (args->m_colGrpDef) {
		args->m_colGrpDef = ColGroupDef::buildNewColGrpDef(newTableDef, oldTableDef, args->m_colGrpDef);
	}
}

/** 作用非标准建表信息到表定义上
 * @param tableDef IN/OUT，表定义
 * @param args 非标准建表信息
 * @throw NtseException 语文错误, 非标准表定义冲突
 */
void ha_ntse::applyCreateArgs(TableDef *tableDef, const TblDefEx *args) throw(NtseException) {
	tableDef->m_useMms = args->m_useMms;
	tableDef->m_cacheUpdate = args->m_cacheUpdate;
	tableDef->m_updateCacheTime = args->m_updateCacheTime;
	tableDef->m_compressLobs = args->m_compressLobs;
	tableDef->m_isCompressedTbl = args->m_compressRows;
	tableDef->m_pctFree = args->m_pctFree;
	tableDef->m_incrSize = args->m_incrSize;
	tableDef->m_indexOnly = args->m_indexOnly;
	tableDef->m_fixLen = args->m_fixLen;
	if (args->m_cachedCols) {
		for (size_t i = 0; i < args->m_cachedCols->getSize(); i++) {
			char *name = (*args->m_cachedCols)[i];
			ColumnDef *col = tableDef->getColumnDef(name);
			if (!col) {
				NTSE_THROW(NTSE_EC_GENERIC, "Column %s not found.", name);
			}
			col->m_cacheUpdate = true;
		}
	}
	if (args->m_idxDefEx) {
		for (size_t i = 0; i < args->m_idxDefEx->getSize(); i++) {
			IdxDefEx *idxDefEx = (*args->m_idxDefEx)[i];
			IndexDef *idx = tableDef->getIndexDef(idxDefEx->m_name);
			if (!idx) {
				NTSE_THROW(NTSE_EC_GENERIC, "Index %s not found.", idxDefEx->m_name);
			}
			idx->m_splitFactor = idxDefEx->m_splitFactor;
		}
	}
	//如果表指定了fixlen属性为false，并且根据各个列计算出来的表格式是定长格式，则强制使用变长格式
	if (!args->m_fixLen && tableDef->m_origRecFormat == REC_FIXLEN) {
		tableDef->m_recFormat = REC_VARLEN;
	} 
	//设置属性组
	if (args->m_colGrpDef) {
		assert(args->m_colGrpDef->getSize() > 0);
		tableDef->setColGrps(args->m_colGrpDef, true);
	}
	if (args->m_compressRows) {
		if (tableDef->m_recFormat == REC_FIXLEN)
			NTSE_THROW(NTSE_EC_GENERIC, "Can't set \"compress_rows\" = \"true\" when table uses fix length heap, please set argument \"fix_len\" = \"false\"");
		tableDef->m_recFormat = REC_COMPRESSED;
		//设置压缩字典配置
		assert(!tableDef->m_rowCompressCfg);
		tableDef->m_rowCompressCfg = new RowCompressCfg(args->m_dictionarySize, args->m_dictionaryMinLen, 
			args->m_dictionaryMaxLen, args->m_compressThreshold);
		if (0 == tableDef->m_numColGrps) 
			tableDef->setDefaultColGrps();
	}
}

/** 判断一个索引属性是否为前缀
 * @param table_arg 表定义
 * @param key_part 索引属性
 * @return 是否为前缀
 */
bool ha_ntse::isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part) {
	// 本来应该通过HA_PART_KEY_SEG标志来判断是否是为前缀，但MySQL并没有正确的设置这个值，
	// 这里参考InnoDB中的判断逻辑
	Field *field = table_arg->field[key_part->field->field_index];
	assert(!strcmp(field->field_name, key_part->field->field_name));
	if ((key_part->length < field->pack_length() && field->type() != MYSQL_TYPE_VARCHAR)
		|| (field->type() == MYSQL_TYPE_VARCHAR && key_part->length < field->pack_length() - ((Field_varstring*)field)->length_bytes))
		return true;
	return false;
}

// TODO:
// 删除索引失败将导致NTSE内部表定义与MySQL不一致，但这一问题非常难解决。经测试，InnoDB plugin
// 在创建或删除索引过程中崩溃也会导致该表不能恢复。这是由于创建与删除索引涉及到MySQL上层维护的表定义
// 与存储引擎内部维护的表定义两个资源的同步修改，没有分布式事务机制，这两者无法保证同步修改，则两者
// 不一致将导致表不能操作，操作则可能导致服务器崩溃

/**
 * 在线建索引
 *
 * @param table_arg 表定义
 * @param key_info 索引定义数组
 * @param num_of_keys 待建的索引个数
 * @return 成功返回0，失败返回错误码
 */
int ha_ntse::add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys) {
	ftrace(ts.mysql, tout << this << table_arg << key_info << num_of_keys);
	DBUG_ENTER("ha_ntse::add_index");
	assert(m_conn && m_session);

	int i = 0, j = 0;
	bool online = false;

	McSavepoint mc(m_session->getMemoryContext());
	IndexDef **indexDefs = (IndexDef **)m_session->getMemoryContext()->alloc(sizeof(IndexDef *) * num_of_keys);
	int ret = 0;
	try {
		if (m_table->getTableDef(true, m_session)->m_indexOnly) {
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not add index to index only table.");
		}

		if (num_of_keys > 1 && isSlaveThread()) {
			// 对于Slave的情况，只要Master执行成功，本地也要配合执行成功，因为ntse_index_build_algorithm只是会话私有变量
			// 复制过程中不会被复制过来，详见QA38567
			online = true;
			sql_print_warning("The create multiple index is from Master to Slave, temporary reset it to online to build multiple indices.");
		}

		for (i = 0; i < (int)num_of_keys; i++) {
			KEY *key = key_info + i;
			bool unique = (key->flags & HA_NOSAME) != 0;

			// 这样来决定是否是主键是太恶心了，不过看InnoDB plugin就是这么实现的
			bool primaryKey = !my_strcasecmp(system_charset_info, key->name, "PRIMARY");
			// 由于MySQL认为主键总是第一个索引，则NTSE目前没有这么处理，因此目前强制要求主键只能作为
			// 第一个索引
			if (primaryKey && m_table->getTableDef(true, m_session)->m_numIndice)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Primary key must be the first index.");

			// 由于MySQL认为非唯一性索引总是在唯一性索引之后，NTSE目前没有这么处理，为保证正确性，要求唯一性索引
			// 必须在非唯一性索引之前创建
			if (unique && m_table->getTableDef(true, m_session)->getNumIndice(false) > 0)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unique index must be created before non-unique index.");

			ColumnDef **columns = new (m_session->getMemoryContext()->alloc(sizeof(ColumnDef *) * key->key_parts)) ColumnDef *[key->key_parts];
			for (uint j = 0; j < key->key_parts; j++) {
				if (isPrefix(table_arg, key->key_part + j))
					NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Prefix key is not supported: %s", key->name);
				columns[j] = m_table->getTableDef(true, m_session)->m_columns[key->key_part[j].field->field_index];
			}

			indexDefs[i] = new IndexDef(key->name, key->key_parts, columns, unique, primaryKey, online);
		}

		// 释放表锁，便于NTSE底层的处理
		m_table->unlock(m_session, m_gotLock);
		m_gotLock = IL_NO;
		m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

		ntse_db->addIndex(m_session, m_table, (u16)num_of_keys, (const IndexDef **)indexDefs);
		if (online) {
			for (j = 0; j < (int)num_of_keys; j++) {
				indexDefs[j]->m_online = false;
			}
			ntse_db->addIndex(m_session, m_table, (u16)num_of_keys, (const IndexDef **)indexDefs);
		}
	} catch (NtseException &e) {
		if (e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION)
			m_dupIdx = MAX_KEY;
		ret = reportError(e.getErrorCode(), e.getMessage(), false, e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION);
	}

	for (i--; i >= 0; i--) {
		delete indexDefs[i];
	}

	DBUG_RETURN(ret);
}

/**
 * 准备删除索引，在NTSE中其实已经把索引给删了
 *
 * @param table_arg 表定义
 * @param key_num 要删除的索引序号数组，经验证一定是顺序排好序的
 * @param num_of_keys 要删除的索引个数
 */
int ha_ntse::prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys) {
	ftrace(ts.mysql, tout << this << table_arg << key_num << num_of_keys);
	DBUG_ENTER("ha_ntse::prepare_drop_index");
	assert(m_conn && m_session);

	int ret = 0;
	try {
		if (num_of_keys > 1)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can't drop multiple indice in one statement.");
		if (m_table->getTableDef(true, m_session)->m_indexOnly)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not drop index of index only table.");
		ntse_db->dropIndex(m_session, m_table, key_num[0]);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}

	DBUG_RETURN(ret);
}

/**
 * 完成删除索引操作，在NTSE中什么也不干
 *
 * @param table_arg 表定义
 * @return 总是返回0
 */
int ha_ntse::final_drop_index(TABLE *table_arg) {
	ftrace(ts.mysql, tout << this << table_arg);
	return 0;
}

/** 更新统计信息
 *
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁时返回HA_ADMIN_FAILED
 */
int ha_ntse::analyze(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_ntse::analyze");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	m_table->getHeap()->updateExtendStatus(m_session, ntse_sample_max_pages);
	for (u16 i = 0; i < m_table->getTableDef(true, m_session)->m_numIndice; i++) {
		m_table->getIndice()->getIndex(i)->updateExtendStatus(m_session, ntse_sample_max_pages);
	}
	if (m_table->getLobStorage())
		m_table->getLobStorage()->updateExtendStatus(m_session, ntse_sample_max_pages);

	DBUG_RETURN(HA_ADMIN_OK);
}

/** 优化表数据
 *
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁或失败时返回HA_ADMIN_FAILED
 */
int ha_ntse::optimize(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_ntse::optimize");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	//检查连接私有属性ntse_keep_compress_dictionary_on_optimize
	bool keepDict = THDVAR(m_thd, keep_dict_on_optimize);

	// 释放表锁，便于NTSE底层的处理
	m_table->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
	m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

	int ret = 0;
	try {
		ntse_db->optimizeTable(m_session, m_table, keepDict, false);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}
	DBUG_RETURN(!ret ? HA_ADMIN_OK: HA_ADMIN_FAILED);
}

/** 检查表数据一致性
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁或失败时返回HA_ADMIN_FAILED，数据不一致返回HA_ADMIN_CORRUPT
 */
int ha_ntse::check(THD* thd, HA_CHECK_OPT* check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_ntse::check");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	// 释放表锁，便于NTSE底层的处理
	m_table->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
	m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

	int ret = 0;
	try {
		m_table->verify(m_session);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}
	DBUG_RETURN(!ret ? HA_ADMIN_OK: HA_ADMIN_CORRUPT);
}

/** 修正表数据中的不一致
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁或失败时返回HA_ADMIN_FAILED
 */
int ha_ntse::repair(THD* thd, HA_CHECK_OPT* check_opt) {
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_ntse::repair");

	// OPTIMIZE即可完成重建索引，修正堆与索引的不一致
	int ret = optimize(thd, check_opt);

	DBUG_RETURN(ret);
}

/**
 * 得到错误消息
 *
 * @error 上一次操作时返回的错误号
 * @param buf OUT，输出错误消息给MySQL
 * @return 是否是暂时错误
 */
bool ha_ntse::get_error_message(int error, String* buf) {
	ftrace(ts.mysql, tout << this << error << buf);
	if (error != m_errno)
		return false;
	buf->append(m_errmsg);
	m_errno = 0;
	return true;
}

/** 将路径中的目录分隔符统一转化成/ */
void ha_ntse::normalizePathSeperator(char *path) {
	for (char *p = path; *p; ) {
		char *slash = strchr(p, '\\');
		if (slash) {
			*slash = '/';
			p = slash + 1;
		} else
			break;
	}
}

/**
 * 根据MySQL的字符集得到NTSE使用的Collation
 *
 * @param charset 字符集
 * @return Collation
 * @throw NtseException 不支持的字符集
 */
CollType ha_ntse::getCollation(CHARSET_INFO *charset) throw(NtseException) {
	if (!charset->sort_order)
		return COLL_BIN;
	else if (charset == &my_charset_gbk_chinese_ci)
		return COLL_GBK;
	else if (charset == &my_charset_utf8_general_ci)
		return COLL_UTF8;
	else if (charset == &my_charset_latin1)
		return COLL_LATIN1;
	else
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unsupported charset %s", charset->name);
}

/**
 * 报告错误，确实是先记录下来等待get_error_message时才返回
 *
 * @param errCode 异常码
 * @param msg 错误信息
 * @param fatal 是否为严重错误，严重错误将导致系统退出
 * @param warnOrErr 若为true，则调用push_warning_printf报告警告，若为false则调用my_printf_error报告错误
 * @return 返回给MySQL的错误码
 */
int ha_ntse::reportError(ErrorCode errCode, const char *msg, bool fatal, bool warnOrErr) {
	if (ntse_db)
		ntse_db->getSyslog()->log(fatal? EL_PANIC: EL_ERROR, "%s", msg);
	if (warnOrErr)
		push_warning_printf(ha_thd(), MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "%s", msg);
	else
		my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), msg);
	strncpy(m_errmsg, msg, sizeof(m_errmsg) - 1);
	m_errmsg[sizeof(m_errmsg) - 1] = '\0';
	if (fatal)
		::exit(-1);
	m_errno = excptToMySqlError(errCode);
	return m_errno;
}

/**
 * 将NTSE内部异常码转化为MySQL错误码
 *
 * @param code NTSE内部异常码
 * @return MySQL错误码
 */
int ha_ntse::excptToMySqlError(ErrorCode code) {
	if (code == NTSE_EC_INDEX_UNQIUE_VIOLATION)
		return HA_ERR_FOUND_DUPP_KEY;
	else if (code == NTSE_EC_ROW_TOO_LONG)
		return HA_ERR_TO_BIG_ROW;
	else if (code == NTSE_EC_LOCK_TIMEOUT)
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	else if (code == NTSE_EC_NOT_SUPPORT)
		return HA_ERR_UNSUPPORTED;
	else
		return HA_ERR_LAST + (int)code + 1;
}

/** 
 * 获取当前SQL
 * @return 当前SQL
 */
const char* ha_ntse::currentSQL() {
	return (m_thd == NULL) ? "NULL" : (m_thd->query() == NULL ? "NULL" : m_thd->query());
}
/**
 * 检查是否需要升级表锁，需要时加锁
 *
 * @return 是否成功
 */
bool ha_ntse::checkTableLock() {
	if (m_wantLock != m_gotLock) {
		assert(m_wantLock > m_gotLock);
		ntse_db->getSyslog()->log(EL_LOG, "Upgrade lock %s to %s on table %s. SQL:\n%s",
			IntentionLock::getLockStr(m_gotLock), IntentionLock::getLockStr(m_wantLock),
			m_tblInfo->m_table->getTableDef()->m_name, currentSQL());

		try {
			if (m_gotLock == IL_IS && m_wantLock == IL_S) {
				m_tblInfo->m_table->upgradeLock(m_session, m_gotLock, m_wantLock, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			} else if (m_gotLock == IL_IX && m_wantLock == IL_SIX) {
				m_tblInfo->m_table->downgradeLock(m_session, IL_IX, IL_IS, __FILE__, __LINE__);
				m_tblInfo->m_table->upgradeLock(m_session, IL_IS, IL_SIX, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			} else {
				char buf[100];
				System::snprintf(buf, sizeof(buf), "Upgrade table lock from %s to %s is unexpected.",
						IntentionLock::getLockStr(m_gotLock), IntentionLock::getLockStr(m_wantLock));
				reportError(NTSE_EC_NOT_SUPPORT, buf, false);
				return false;
			}
		} catch (NtseException &e) {
			reportError(e.getErrorCode(), e.getMessage(), false);
			return false;
		}
		m_gotLock = m_wantLock;
	}
	return true;
}

/**
 * 转换MySQL用位图表示的操作涉及属性集为NTSE格式
 *
 * @param bitmap MySQL属性操作位图
 * @param numCols OUT，属性个数
 * @return 各属性号，从会话的MemoryContext中分配
 */
u16* ha_ntse::transCols(MY_BITMAP *bitmap, u16 *numCols) {
	// TODO 更好的处理不读取任何属性的情况
	int columns = bitmap_bits_set(bitmap);
	if (columns == 0) {
		*numCols = 1;
		u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16));
		r[0] = 0;
		return r;
	}
	*numCols = (u16)columns;
	u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16) * columns);
	u16 idx = 0, i = 0;
	Field **field;
	for (field = table->field; *field; field++, i++) {
		if (bitmap_is_set(bitmap, (*field)->field_index)) {
			r[idx++] = i;
		}
	}
	assert(idx == columns);
	return r;
}

/** 判断指定的搜索方式是否包含键值本身
 * @param flag 搜索方式
 * @param lowerBound true表示是扫描范围的下界，false表示是上界
 * @return 是否包含键值本身
 */
bool ha_ntse::isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound) {
	switch (flag) {
	case HA_READ_KEY_EXACT:
	case HA_READ_KEY_OR_NEXT:
	case HA_READ_KEY_OR_PREV:
	case HA_READ_PREFIX:
	case HA_READ_PREFIX_LAST:
	case HA_READ_PREFIX_LAST_OR_PREV:
		return true;
	case HA_READ_AFTER_KEY:
		return !lowerBound;
	default:
		return false;
	};
}

/**
 * 转换MySQL索引搜索键(KEY_MYSQL)为NTSE内部格式(KEY_PAD)格式，存储于out中
 * @pre 已经加了表元数据锁
 *
 * @param idx 第几个索引
 * @param key MySQL上层给出的搜索键，为KEY_MYSQL格式
 * @param keypart_map 搜索键中包含哪些索引属性
 * @param out 存储转换后的结果，内存从m_session的内存上下文中分配
 */
void ha_ntse::transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out) {
	assert(idx < m_table->getTableDef(true, m_session)->m_numIndice);
	IndexDef *indexDef = m_table->getTableDef(true, m_session)->m_indice[idx];

	SubRecord mysqlKey;
	mysqlKey.m_format = KEY_MYSQL;
	mysqlKey.m_data = (byte *)key;
	mysqlKey.m_columns = indexDef->m_columns;
	parseKeyparMap(idx, keypart_map, &mysqlKey.m_size, &mysqlKey.m_numCols);

	out->m_columns = indexDef->m_columns;
	out->m_data = (byte *)m_session->getMemoryContext()->alloc(indexDef->m_maxKeySize);
	out->m_numCols = mysqlKey.m_numCols;
	out->m_format = KEY_PAD;
	out->m_size = indexDef->m_maxKeySize;
	out->m_rowId = INVALID_ROW_ID;

	RecordOper::convertKeyMP(m_table->getTableDef(true, m_session), &mysqlKey, out);
}

/**
 * 解析用key_part_map结构表示的搜索键中包含哪些索引属性信息
 *
 * @param idx 第几个索引
 * @param keypart_map 搜索键中包含哪些索引属性信息
 * @param key_len OUT，搜索键长度
 * @param num_cols OUT，搜索键中包含几个索引属性
 */
void ha_ntse::parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols) {
	DBUG_ASSERT(((keypart_map + 1) & keypart_map) == 0);	// 只支持前n个属性

	KEY *key_info = table->s->key_info + idx;
	KEY_PART_INFO *key_part = key_info->key_part;
	KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;
	uint length = 0, cnt = 0;

	while (key_part < end_key_part && keypart_map) {
		length += key_part->store_length;
		keypart_map >>= 1;
		key_part++;
		cnt++;
	}

	*key_len = length;
	*num_cols = (u16)cnt;
}

/** 得到操作类型
 * @param return 操作类型
 */
OpType ha_ntse::getOpType() {
	// 无法根据当前语句类型判断操作类型，这是因为存在以下两类特殊情况
	// 1. 分区表UPDATE可能导致DELETE
	// 2. 作为SLAVE时，当前语句是SQLCOM_END
	if (m_gotLock == IL_IS || m_gotLock == IL_S)
		return OP_READ;
	else
		return OP_WRITE;
}

/** 表扫描或索引扫描时获取下一条记录（索引反向扫描时实际上是前一个记录）
 * @post m_lastRow中记录了刚获取成功的记录的RID
 *
 * @param buf OUT，存储获取的记录内容
 * @return 成功返回0，失败返回错误码
 */
int ha_ntse::fetchNext(uchar *buf) {
	table->status = STATUS_NOT_FOUND;
	if (!checkTableLock())
		return HA_ERR_LOCK_WAIT_TIMEOUT;

	m_lobCtx->reset();
	if (!m_table->getNext(m_scan, buf))
		return HA_ERR_END_OF_FILE;
	if (m_scan->getType() == ST_IDX_SCAN
		&& m_checkSameKeyAfterIndexScan
		&& !recordHasSameKey(buf, m_table->getTableDef(true, m_session)->m_indice[active_index], &m_indexKey))
		return HA_ERR_END_OF_FILE;
	m_lastRow = m_scan->getCurrentRid();
	table->status = 0;
	return 0;
}

/** 若正在进行扫描则结束之
 * @post 若正在进行扫描，则底层的扫描已经结束，会话的MemoryContext被重置
 *   到开始扫描前的状态
 */
void ha_ntse::endScan() {
	assert(m_session);
	m_isRndScan = false;
	if (m_scan) {
		m_table->endScan(m_scan);
		m_scan = NULL;
		m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
	}
	bitmap_clear_all(&m_readSet);
}

/** 开始索引扫描并返回第一条记录
 *
 * @param buf 保存输出记录的缓冲区
 * @param key 索引搜索键
 * @param keypart_map 搜索键中包含哪些索引属性
 * @param forward 搜索方向
 * @param includeKey 是否包含等于key的记录
 * @param exact 是否只返回等于key的记录
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_ntse::indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact) {
	m_lobCtx->reset();
	endScan();	// 多区域范围查询时可能会扫描多次，不调用index_end

	int code = checkSQLLogBin();
	if (code) {
		return code;
	}

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();

	initReadSet(true);

	bool singleFetch;
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols);
	transKey(active_index, key, keypart_map, &m_indexKey);
	if (exact
		&& m_table->getTableDef(true, m_session)->m_indice[active_index]->m_unique
		&& m_indexKey.m_numCols == m_table->getTableDef(true, m_session)->m_indice[active_index]->m_numCols)
		singleFetch = true;
	else
		singleFetch = false;

	OpType opType = getOpType();
	try {
		IndexScanCond cond(active_index, m_indexKey.m_numCols? &m_indexKey: NULL, forward, includeKey, singleFetch);
		m_scan = m_table->indexScan(m_session, opType, &cond, numReadCols, readCols, false, m_lobCtx);
	} catch(NtseException &e) {
		NTSE_ASSERT(e.getErrorCode() == NTSE_EC_LOCK_TIMEOUT);
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	}
	// 由于NTSE索引暂不支持返回只等于指定搜索键的记录，如果上层搜索方式是HA_READ_KEY_EXACT，
	// 但又不是singleFetch，则NTSE会返回实际上大于指定搜索键的记录。由于上层指定了
	// HA_READ_KEY_EXACT，因此上层认为底层只会返回等于指定搜索键的记录，不会再去检查，
	// 因此这里需要过滤掉那些不符合条件的记录
	m_checkSameKeyAfterIndexScan = exact && !singleFetch;

	code = fetchNext(buf);
	return code;
}

/** 保留指定个数的自增值
 * @param offset			自增起始偏移量
 * @param increment			自增步长
 * @param nb_desired_values	需要保留多少个自增值
 * @param first_value		out 保留的第一个自增值
 * @param nb_reserved_value	out 保留了多少个自增值
 */
void ha_ntse::get_auto_increment( ulonglong offset, ulonglong increment, ulonglong nb_desired_values, ulonglong *first_value, ulonglong *nb_reserved_values ) {
	AutoIncrement *ai = &m_tblInfo->m_autoIncr;

	if (table->s->next_number_key_offset) {
		// Autoincrement 字段不是索引的第一个字段
		*first_value = (~(ulonglong) 0);
		return;
	}

	u64 autoIncr = ai->reserve();

	// TODO：检查是否数值过大
	// TODO：支持事务的实现需要补充

	if (autoIncr == 0) {
		*first_value = 1;
	} else {
		*first_value = autoIncr;
	}

	m_offset = offset;
	m_increment = increment;
	*nb_reserved_values = 1;

	u64 nextValue = calcNextAutoIncr(*first_value, m_increment, m_offset, (u64)-1);
	m_tblInfo->m_autoIncr.setAutoIncrIfBigger(nextValue);
	ai->unreserve();
}

/** 更新HA_CREATE_INFO信息
 * @param create_info	信息对象
 */
void ha_ntse::update_create_info( HA_CREATE_INFO *create_info ) {
	if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
		ha_ntse::info(HA_STATUS_AUTO);
		create_info->auto_increment_value = stats.auto_increment_value;
	}
}

/** 重设自增值为指定的值
 * @param value	指定的新自增值
 */
int ha_ntse::reset_auto_increment( ulonglong value ) {
	m_tblInfo->m_autoIncr.setAutoIncr(value);
	return 0;
}


/** 比较记录的键值与指定键值是否相等
 *
 * @param buf 记录
 * @param indexDef 索引定义
 * @param key KEY_PAD格式的键值
 * @return 是否相等
 */
bool ha_ntse::recordHasSameKey(const byte *buf, const IndexDef *indexDef, const SubRecord *key) {
	assert(key->m_format == KEY_PAD);
	SubRecord key2(KEY_PAD, key->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->alloc(indexDef->m_maxKeySize),
		indexDef->m_maxKeySize);
	TableDef *tableDef = m_table->getTableDef(true, m_session);
	Record rec(INVALID_ROW_ID, REC_REDUNDANT, (byte *)buf, tableDef->m_maxRecSize);
	RecordOper::extractKeyRP(tableDef, &rec, &key2);
	return key2.m_size == key->m_size && !RecordOper::compareKeyPP(tableDef, &key2, key, indexDef);
}

/**
 * 将handler关联到THD对象
 *
 * @param thd THD对象
 */
void ha_ntse::attachToTHD(THD *thd) {
	assert(!m_thd && thd);
	checkTHDInfo(thd);
	m_thd = thd;
	m_conn = getTHDInfo(thd)->m_conn;
	getTHDInfo(thd)->m_handlers.addLast(&m_thdInfoLnk);
}

/**
 * 脱离handler与THD的关联
 */
void ha_ntse::detachFromTHD() {
	assert(m_thd);
	m_thd = NULL;
	m_conn = NULL;
	m_thdInfoLnk.unLink();
}


/**
 * 初始化表扫描句柄
 * @pre m_readSet应该要设置正确
 */
void ha_ntse::beginTblScan() {
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols);

	OpType opType = getOpType();
	try {
		if (m_isRndScan) {
			m_conn->getLocalConfig()->m_accurateTblScan = THDVAR(ha_thd(), accurate_tblscan);
			m_scan = m_table->tableScan(m_session, opType, numReadCols, readCols, false, m_lobCtx);
		} else {
			m_scan = m_table->positionScan(m_session, opType, numReadCols, readCols, false, m_lobCtx);
		}
	} catch(NtseException &) {
		NTSE_ASSERT(false);		// 不加表锁时不可能会失败
	}
}

/** 得到ntse内部表的表ID信息
 * @return 内部表表ID
 */
u16 ha_ntse::getNtseTableId() {
	return m_table->getTableDef()->m_id;
}

/**
 * 得到mysql上层的表对象
 * @return mysql上层st_table对象
 */
st_table* ha_ntse::getMysqlTable() {
	return table;
}

/** 得到handler信息链接对象
 * @return handler信息链接对象
 */
DLink<ha_ntse *>* ha_ntse::getHdlInfoLnk() {
	return &m_hdlInfoLnk;
}

/**
 * ntse实现的存储引擎binlog事件处理函数
 * @param hton	存储引擎句柄
 * @param thd	当前线程对象
 * @param fn	binlog事件类型
 * @param arg	参数列表
 * @return 0表示成功，非0表示异常
 */
int ha_ntse::ntseBinlogFunc( handlerton *hton, THD *thd, enum_binlog_func fn, void *arg ) {
	switch (fn) {
		case BFN_RESET_LOGS:
		case BFN_RESET_SLAVE:
		case BFN_BINLOG_WAIT:
		case BFN_BINLOG_PURGE_FILE:
			break;
		case BFN_BINLOG_END:
			ntse_binlog->unregisterNTSECallbacks(ntse_db);
			NTSEBinlogFactory::getInstance()->freeNTSEBinlog(&ntse_binlog);
			break;
		default:
			break;
	}

	return 0;
}

/**
 * 是否有必要支持binlog,这项由配置参数ntse_binlog_method决定,如果该参数为"mysql"表示由mysql上层记录binlog,ntse不需要支持binlog
 * @return true表示需要支持,false表示不需要
 */
bool ha_ntse::isBinlogSupportable() {
	return System::stricmp(ntse_binlog_method, "mysql");
}

/** 用于各种扫描之前初始化m_readSet的信息
 * @param idxScan	true表示索引扫描，false表示堆扫描
 * @param posScan	true表示是rnd_pos扫描，false可以是其他表扫描或者索引扫描
 */
void ha_ntse::initReadSet( bool idxScan, bool posScan/* = false */) {
	assert(m_thd);
	assert(!(idxScan && posScan));	// 必须不可能既是索引扫描又是rnd_pos扫描

	bool isReadAll = false;
	if ((opt_bin_log && !isBinlogSupportable()) || posScan) {
		// 对于自主记录binlog的rnd_pos扫描以及采用mysql的行级binlog的操作，采用相同的判断流程，slave复制的逻辑也可以一样
		// 对于rnd_pos的处理详见QA70175，这里的处理保证对于更新操作都能读取所有属性，可能多读但不会漏读
		enum_sql_command sql_command = m_thd->lex->sql_command;
		bool isRead = (getOpType() == OP_READ);
		if (isSlaveThread()) {
			// Slave执行binlog时的语句类型是SQLCOM_END，无法判断是否为UPDATE，因此只要是更新操作，都读取所有属性
			// TODO：如果Slave写binlog后项用的是Master的binlog的内容，应该可以不读取所有属性
			isReadAll = !isRead;
		} else {
			// Master，对于update需要读取所有属性
			isReadAll = ((sql_command == SQLCOM_UPDATE) || (sql_command == SQLCOM_UPDATE_MULTI && !isRead));
		}
	}

	if (isReadAll)
		bitmap_set_all(&m_readSet);
	else {
		if (idxScan)
			bitmap_copy(&m_readSet, table->read_set);
		else {
			// 表扫描时rnd_init和第一次rnd_next时给出的read_set不一致，为安全起见
			// 使用rnd_init和第一次rnd_next的read_set之合集
			bitmap_union(&m_readSet, table->read_set);
		}
	}
	m_isReadAll = bitmap_is_set_all(&m_readSet);
}

/** 得到句柄使用的表的AUTO_INCREMENT字段的当前最大值
 * 内部实现是扫描AUTO_INCREMENT字段所在的索引来得到自增值最大值
 * @pre 此时是第一次打开表才会调用，不会有并发访问
 * @param session	会话句柄
 * @return 返回AUTO_INCREMENT当前的最大值
 */
u64 ha_ntse::getMaxAutoIncr(Session *session) {
	u64	autoIncr = 1;
	const Field* field = table->found_next_number_field;

	if (field == NULL) {
		/* We have no idea what's been passed in to us as the
		autoinc column. We set it to the 0, effectively disabling
		updates to the table. */
		nftrace(ts.mysql, tout << "Ntse can't find auto_increment column in table: " << m_table->getTableDef()->m_name);
		return autoIncr;
	}

	// 初始化m_indexKey
	m_indexKey.m_numCols = 0;
	m_indexKey.m_format = KEY_PAD;

	// 使用索引扫描得到自增最大值
	u64 savePoint = session->getMemoryContext()->setSavepoint();

	active_index = table->s->next_number_index;
	TableDef *tableDef = m_table->getTableDef();
	IndexDef *indexDef = tableDef->m_indice[active_index];
	try {
		IndexScanCond cond(active_index, NULL, false, true, false);
		m_scan = m_table->indexScan(session, OP_READ, &cond, indexDef->m_numCols, indexDef->m_columns, false, m_lobCtx);
		m_checkSameKeyAfterIndexScan = false;
	} catch(NtseException &e) {
		NTSE_ASSERT(e.getErrorCode() == NTSE_EC_LOCK_TIMEOUT);
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	}

	uchar *buf = (uchar*)session->getMemoryContext()->alloc(indexDef->m_maxKeySize);
	int r = fetchNext(buf);
	m_table->endScan(m_scan);
	m_scan = NULL;

	if (r == 0) {
		uint i = indexDef->m_columns[0];
		// 自增字段必须是索引的第一个属性
		ColumnDef *colDef = tableDef->m_columns[i];
		assert(strcmp(colDef->m_name, field->field_name) == 0);

		s64 oldAutoIncr = 0;
		// 读取属性出来设置当前的autoIncr值
		switch (colDef->m_type) {
		case CT_TINYINT:
			oldAutoIncr = RedRecord::readTinyInt(tableDef, buf, i);
			break;
		case CT_SMALLINT:
			oldAutoIncr = RedRecord::readSmallInt(tableDef, buf, i);
			break;
		case CT_MEDIUMINT:
			oldAutoIncr = RedRecord::readMediumInt(tableDef, buf, i);
			break;
		case CT_INT:
			oldAutoIncr = RedRecord::readInt(tableDef, buf, i);
			break;
		case CT_BIGINT:
			oldAutoIncr = RedRecord::readBigInt(tableDef, buf, i);
			break;
		case CT_FLOAT:
			oldAutoIncr = (s64)RedRecord::readFloat(tableDef, buf, i);
			break;
		case CT_DOUBLE:
			oldAutoIncr = (s64)RedRecord::readDouble(tableDef, buf, i);
			break;
		default:
			NTSE_ASSERT(false);
		}

		if (oldAutoIncr >= 0)
			// 加1保证不会重复
			autoIncr = oldAutoIncr + 1;
		// 否则，自增字段为0表示禁用
	} else {
		// 对于返回HA_ERR_LOCK_WAIT_TIMEOUT/HA_ERR_END_OF_FILE的情况，不需要处理，返回0禁用自增字段
		nftrace(ts.mysql, tout << "Can not find specified key to get Auto_increment value : " << m_table->getTableDef()->m_name);
		return autoIncr;
	}

	session->getMemoryContext()->resetToSavepoint(savePoint);

	return autoIncr;
}

/** 如果自增字段修改了，并且是增大的，保存该值作为下一次自增基值
 * @param buf	当前插入或者更新的记录缓存
 */
void ha_ntse::setAutoIncrIfNecessary( uchar *buf ) {
	if (table->next_number_field && buf == table->record[0]) {
		s64 autoIncr = table->next_number_field->val_int();
		if (autoIncr > 0) {	// 修改了自增字段
			u64 nextValue = calcNextAutoIncr(autoIncr, m_increment, m_offset, (u64)-1);
			m_tblInfo->m_autoIncr.reserve();
			m_tblInfo->m_autoIncr.setAutoIncrIfBigger(nextValue);
			m_tblInfo->m_autoIncr.unreserve();
		}
	}
}

/** 计算下一个自增字段值
 * @param current	当前值
 * @param increment	每次自增步长
 * @param offset	自增偏移量
 * @param maxValue	自增变量类型的最大可能值
 * @description 参看InnoDB的innobase_next_autoinc的实现
 */
u64 ha_ntse::calcNextAutoIncr( u64 current, u64 increment, u64 offset, u64 maxValue ) {
	// 按照手册文档，offset如果比increment大，忽略offset
	if (offset > increment) {
		offset = 0;
	}

	u64 nextValue = 0;
	if (maxValue <= current) {
		nextValue = maxValue;
	} else if (offset <= 1) {
		// 对于0和1的情况是相同的，系统中至少有1个mysql节点
		if (maxValue - current <= increment) {
			// 加上步长超过范围，下一个值就是最大值
			nextValue = maxValue;
		} else {
			// 否则简单的加上步长即可
			nextValue = current + increment;
		}
	} else {
		u64 times;
		// 计算current和offset之间需要几个步长
		if (current > offset) {
			times = ((current - offset) / increment) + 1;
		} else {
			times = ((offset - current) / increment) + 1;
		}

		if (increment > (maxValue / times)) {
			// 超过范围，用maxValue
			nextValue = maxValue;
		} else {
			nextValue = increment * times;
			assert(maxValue >= nextValue);

			// 确保加了offset不会溢出
			if (maxValue - nextValue <= offset) {
				nextValue = maxValue;
			} else {
				nextValue += offset;
			}
		}
	}

	NTSE_ASSERT(nextValue <= maxValue);
	return nextValue;
}

/** 得到当前句柄的THD对象
* @return 返回m_thd对象
*/
THD* ha_ntse::getTHD() {
	return m_thd;
}

/** 检验ntse_xxx之类的参数合法性，在ntse初始化时调用
 * 如果有参数不合法，给出提示，并且退出服务
 */
void ha_ntse::rationalConfigs() {
	// 检查binlog参数的合法性
	if (System::stricmp(ntse_binlog_method, "direct") && System::stricmp(ntse_binlog_method, "mysql") && 
		System::stricmp(ntse_binlog_method, "cached")) {
		sql_print_warning("ntse_binlog_method = %s is invalid and automatically changed to \"direct\"", ntse_binlog_method);
		ntse_binlog_method = (char *)"direct";
	}
	// 检查日志参数的合法性
	if (ntse_log_buf_size < LogConfig::MIN_LOG_BUFFER_SIZE) {
		sql_print_warning("ntse_log_buf_size = %lu is invalid because of out of range and it is automatically changed to %lu", ntse_log_buf_size, LogConfig::MIN_LOG_BUFFER_SIZE);
		ntse_log_buf_size = LogConfig::MIN_LOG_BUFFER_SIZE;
	}
	if (ntse_log_file_size < LogConfig::MIN_LOGFILE_SIZE) {
		sql_print_warning("ntse_log_file_size = %lu is invalid because of out of range and it is automatically changed to %lu", ntse_log_file_size, LogConfig::MIN_LOGFILE_SIZE);
		ntse_log_file_size = LogConfig::MIN_LOGFILE_SIZE;
	}
}


/** 判断当前是不是Slave SQL线程的操作
 * @return true表示是Slave SQL线程，false表示不是
 */
bool ha_ntse::isSlaveThread() const {
	return isSlaveThread(m_thd);
}


/** 判断当前是不是Slave SQL线程的操作
 * @return true表示是Slave SQL线程，false表示不是
 */
bool ha_ntse::isSlaveThread(THD *thd) const {
	return (thd != NULL && thd->slave_thread);
}

/** 对于更新操作（insert/delete/update）检查SQL_LOG_BIN参数是否有被设置，如果被设置给出警告
 * @return 0表示参数未被设置，无警告；非0表示参数被设置，给出相应的警告信息
 */
int ha_ntse::checkSQLLogBin() {
	int code = 0;
	if (m_thd->lex->sql_command != SQLCOM_SELECT && !m_thd->sql_log_bin_toplevel)
		code = reportError(NTSE_EC_NOT_SUPPORT, "SQL_LOG_BIN variable is not supported by NTSE, please set to 1.", false);
	
	return code;
}


struct st_mysql_storage_engine ntse_storage_engine = {MYSQL_HANDLERTON_INTERFACE_VERSION};
static st_mysql_information_schema ntse_mutex_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_rwlock_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_buf_distr = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_conns = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_mms = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_mms_rpcls = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_tbl_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_heap_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_idx_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_heap_stats_ex = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_idx_stats_ex = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_lob_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_lob_stats_ex = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_command_return = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_tbl_def_ex = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_col_def_ex = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_idx_def_ex = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_mms_ridhash_conflicts = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_dbobj_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_compress_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#ifdef NTSE_PROFILE
static st_mysql_information_schema ntse_prof_summ = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_bgthd_prof = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_conn_prof = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#ifdef NTSE_KEYVALUE_SERVER
static st_mysql_information_schema ntse_keyvalue_prof = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#endif
#endif

#ifdef NTSE_KEYVALUE_SERVER
static st_mysql_information_schema ntse_keyvalue_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema ntse_keyvalue_threadinfo = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#endif


mysql_declare_plugin(ntse) {
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&ntse_storage_engine,
	"Ntse",
	"NetEase Corporation",
	"Non-transactional storage engine",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::init,							/** 插件初始化 */
	ha_ntse::exit,							/** 安全退出 */
	0x0010,								/** 版本号0.1 */
	ntseStatus,								/** 状态变量 */
	ntseVariables,							/** 配置参数 */
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_mutex_stats,
	"NTSE_MUTEX_STATS",
	"NetEase  Corporation",
	"Statistics of Mutex objects in NTSE.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISMutexStats,
	ha_ntse::deinitISMutexStats,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_rwlock_stats,
	"NTSE_RWLOCK_STATS",
	"NetEase  Corporation",
	"Statistics of read/write lock objects in NTSE.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISRWLockStats,
	ha_ntse::deinitISRWLockStats,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_buf_distr,
	"NTSE_BUF_DISTRIBUTION",
	"NetEase  Corporation",
	"Distribution of buffer pages.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISBufDistr,
	ha_ntse::deinitISBufDistr,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_conns,
	"NTSE_CONNECTIONS",
	"NetEase  Corporation",
	"Per connection statistics.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISConns,
	ha_ntse::deinitISConns,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_mms,
	"NTSE_MMS_STATS",
	"NetEase  Corporation",
	"MMS statistics per table.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISMms,
	ha_ntse::deinitISMms,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_mms_rpcls,
	"NTSE_MMS_RPCLS_STATS",
	"NetEase  Corporation",
	"Statistics of each record page class of MMS.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISMmsRPCls,
	ha_ntse::deinitISMmsRPCls,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_tbl_stats,
	"NTSE_TABLE_STATS",
	"NetEase  Corporation",
	"Statistics of each opened table.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISTable,
	ha_ntse::deinitISTable,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_heap_stats,
	"NTSE_HEAP_STATS",
	"NetEase  Corporation",
	"Statistics of each heap of opened tables.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISHeap,
	ha_ntse::deinitISHeap,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_heap_stats_ex,
	"NTSE_HEAP_STATS_EX",
	"NetEase  Corporation",
	"Extended statistics of each heap of opened tables.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISHeapEx,
	ha_ntse::deinitISHeapEx,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_idx_stats,
	"NTSE_INDEX_STATS",
	"NetEase  Corporation",
	"Statistics of each index of opened tables.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISIndex,
	ha_ntse::deinitISIndex,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_idx_stats_ex,
	"NTSE_INDEX_STATS_EX",
	"NetEase  Corporation",
	"Extended statistics of each index of opened tables.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISIndexEx,
	ha_ntse::deinitISIndexEx,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_lob_stats,
	"NTSE_LOB_STATS",
	"NetEase  Corporation",
	"Statistics of each lob storage of opened tables.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISLob,
	ha_ntse::deinitISLob,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_lob_stats_ex,
	"NTSE_LOB_STATS_EX",
	"NetEase  Corporation",
	"Extended statistics of each lob storage of opened tables.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISLobEx,
	ha_ntse::deinitISLobEx,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_tbl_def_ex,
	"NTSE_TABLE_DEF_EX",
	"NetEase Corporation",
	"NTSE specific table definition informations.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISTblDefEx,
	ha_ntse::deinitISTblDefEx,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_col_def_ex,
	"NTSE_COLUMN_DEF_EX",
	"NetEase Corporation",
	"NTSE specific column definition informations.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISColDefEx,
	ha_ntse::deinitISColDefEx,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_idx_def_ex,
	"NTSE_INDEX_DEF_EX",
	"NetEase Corporation",
	"NTSE specific index definition informations.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISIdxDefEx,
	ha_ntse::deinitISIdxDefEx,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_command_return,
	"NTSE_COMMAND_RETURN",
	"NetEase  Corporation",
	"Status of NTSE's internal command for current thread.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISCmdReturn,
	ha_ntse::deinitISCmdReturn,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_mms_ridhash_conflicts,
	"NTSE_MMS_RIDHASH_CONFLICTS",
	"Netease Corporation",
	"NTSE MMS RID Hash Conflict Status.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISMmsRidHashConflicts,
	ha_ntse::deinitISMmsRidHashConflicts,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_dbobj_stats,
	"NTSE_DBOBJ_STATS",
	"Netease Corporation",
	"NTSE DB Object Status.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISDBObjStats,
	ha_ntse::deinitISDBObjStats,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_compress_stats,
	"NTSE_COMPRESS_STATS",
	"Netease Corporation",
	"NTSE Row Compression Status",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISCompressStats,
	ha_ntse::deinitISCompressStats,
	0x0010,
	NULL,
	NULL,
	NULL
}
#ifdef NTSE_PROFILE
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_prof_summ,
	"NTSE_PROFILE_SUMMARY",
	"NetEase  Corporation",
	"NTSE Internal Performence Profile Summary.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISProfSumm,
	ha_ntse::deinitISProfSumm,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_bgthd_prof,
	"NTSE_BGTHREAD_PROFILES",
	"NetEase  Corporation",
	"NTSE BG Thread Profiles.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISBGThdProfile,
	ha_ntse::deinitISBGThdProfile,
	0x0010,
	NULL,
	NULL,
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
	&ntse_conn_prof,
	"NTSE_CONN_PROFILES",
	"NetEase  Corporation",
	"NTSE Connection Profiles.",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_ntse::initISConnThdProfile,
	ha_ntse::deinitISConnThdProfile,
	0x0010,
	NULL,
	NULL,
	NULL
}
#ifdef NTSE_KEYVALUE_SERVER
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&ntse_keyvalue_prof,
		"NTSE_KEYVALUE_PROFILES",
		"NetEase  Corporation",
		"NTSE Keyvalue Profiles.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_ntse::initKVProfSumm,
		ha_ntse::deinitKVProfSumm,
		0x0010,
		NULL,
		NULL,
		NULL
}
#endif // NTSE_KEYVALUE_SERVER
#endif // NTSE_PROFILE

#ifdef NTSE_KEYVALUE_SERVER
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&ntse_keyvalue_stats,
		"NTSE_KEYVALUE_STATISTIC",
		"NetEase  Corporation",
		"NTSE Keyvalue Statistics.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_ntse::initKVStats,
		ha_ntse::deinitKVStats,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&ntse_keyvalue_threadinfo,
		"NTSE_KEYVALUE_THREADINFO",
		"NetEase  Corporation",
		"NTSE Keyvalue Thread information.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_ntse::initKVThreadInfo,
		ha_ntse::deinitKVThreadInfo,
		0x0010,
		NULL,
		NULL,
		NULL
}
#endif // NTSE_KEYVALUE_SERVER

mysql_declare_plugin_end;


/** 返回HandlerInfo实例对象
 * @return m_instance实例
 */
HandlerInfo* HandlerInfo::getInstance() {
	if (m_instance == NULL)
		m_instance = new HandlerInfo();
	return m_instance;
}

/** 释放HandlerInfo实例对象
 */
void HandlerInfo::freeInstance() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}

/** 保留指定的表handler,调用者使用期间,该handler不会被改变
 * @param tblId	handler所属表的ntse内部id
 * @return 使用tblId指定表的某个handler对象
 */
ha_ntse* HandlerInfo::reserveSpecifiedHandler( u16 tblId ) {
	HandlerList *hList;
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		map<u16, HandlerList*>::iterator itor = m_handlers.find(tblId);
		assert(itor != m_handlers.end());
		hList = itor->second;
		if (hList->m_handlers.isEmpty())
			return NULL;
		LOCK(&hList->m_mutex);
	}

	hList->m_reserved = hList->m_handlers.getHeader()->getNext()->get();
	UNLOCK(&hList->m_mutex);
	return hList->m_reserved;
}

/** 保留者归还指定的handler对象
 * @param handler 要归还的handler对象
 */
void HandlerInfo::returnHandler( ha_ntse *handler ) {
	assert(handler != NULL);

	HandlerList *hList;
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		map<u16, HandlerList*>::iterator itor = m_handlers.find(handler->getNtseTableId());
		assert(itor != m_handlers.end());
		hList = itor->second;
		LOCK(&hList->m_mutex);
	}

	hList->m_reserved = NULL;
	hList->m_event.signal();
	UNLOCK(&hList->m_mutex);
}

/** 往信息对象当中注册已经打开的一个handler
 * @param handler	要注册的handler
 */
typedef pair<u16, HandlerInfo::HandlerList*> hdlPair;
void HandlerInfo::registerHandler( ha_ntse *handler ) {
	assert(handler != NULL);
	HandlerList *hList = NULL;
	map<u16, HandlerList*>::iterator itor;
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		itor = m_handlers.find(handler->getNtseTableId());
		if (itor != m_handlers.end()) {
			hList = itor->second;
			LOCK(&hList->m_mutex);
		}
	}

	if (hList != NULL) {
		hList->m_handlers.addLast(handler->getHdlInfoLnk());
		UNLOCK(&hList->m_mutex);
		return;
	} else {
		// 找不到就准备生成新的链表对象插入
		HandlerList *hList = new HandlerList();
		hList->m_handlers.addLast(handler->getHdlInfoLnk());
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		if (!(m_handlers.insert(hdlPair(handler->getNtseTableId(), hList))).second) {
			// 如果失败，表明此时有线程插入生成了该表对应的handler链表信息
			// 将要处理的handler加入已有的链表
			handler->getHdlInfoLnk()->unLink();
			delete hList;
			itor = m_handlers.find(handler->getNtseTableId());
			MutexGuard guard1(&hList->m_mutex, __FILE__, __LINE__);
			hList->m_handlers.addLast(handler->getHdlInfoLnk());
		}
	}
}

/** 注销一个即将要关闭的handler
 * @param handler		要注销的handler
 * @param ntseTableId	关闭的handler包含的ntse表的内部ID
 */
void HandlerInfo::unregisterHandler( ha_ntse *handler, u16 ntseTableId ) {
	assert(handler != NULL);
	map<u16, HandlerList*>::iterator itor;
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		itor = m_handlers.find(ntseTableId);
		assert(itor != m_handlers.end());
	}

	HandlerList *hList = itor->second;
	long sigToken = 0;
	while (true) {
		LOCK(&hList->m_mutex);
		if (hList->m_reserved == NULL) {
			handler->getHdlInfoLnk()->unLink();
			hList->m_event.signal();

			// 此时如果属于该表的handler都关闭，删除该表对应的handler列表信息
			if (hList->m_handlers.isEmpty()) {
				UNLOCK(&hList->m_mutex);
				// 修改全局handler信息结构，需要加全局mutex
				MutexGuard guard(&m_mutex, __FILE__, __LINE__);
				itor = m_handlers.find(ntseTableId);
				hList = itor->second;
				// 重新找到之后还要再判断一遍，加锁保证不会有其他线程插入新的handler
				bool isEmpty = false;
				{
					MutexGuard guard1(&hList->m_mutex, __FILE__, __LINE__);
					if (hList->m_handlers.isEmpty()) {
						isEmpty = true;
						m_handlers.erase(itor);
					}
				}
				// 从m_handlers内部移开之后，不应该有人会看到这个链表，可以安全删除
				if (isEmpty)
					delete hList;
			} else
				UNLOCK(&hList->m_mutex);

			return;
		}
		UNLOCK(&hList->m_mutex);

		sigToken = hList->m_event.reset();
		hList->m_event.wait(-1, sigToken);
	}
}

/** 判断某个handler是否处于注册过的状态
 * @param handler	要查询的handler
 * @return 注册过的返回true，否则返回false
 */
bool HandlerInfo::isHandlerIn( ha_ntse *handler ) {
	assert(handler != NULL);
	HandlerList *hList;
	map<u16, HandlerList*>::iterator itor;
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		itor = m_handlers.find(handler->getNtseTableId());
		if (itor == m_handlers.end())
			return false;

		hList = itor->second;
		if (hList->m_handlers.isEmpty())
			return false;

		LOCK(&hList->m_mutex);
	}

	DLink<ha_ntse*> *header = hList->m_handlers.getHeader();
	DLink<ha_ntse*> *cur = header->getNext();
	while (cur != header) {
		if (cur->get() == handler) {
			UNLOCK(&hList->m_mutex);
			return true;
		}
	}

	UNLOCK(&hList->m_mutex);
	return false;
}
