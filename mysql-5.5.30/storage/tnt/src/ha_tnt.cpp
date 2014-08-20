/**
* TNT存储引擎handle类。
*
* @author 何登成
*/
#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#ifdef NTSE_KEYVALUE_SERVER
#include "keyvalue/KeyValueServer.h"
#endif

#define MYSQL_SERVER 1
#include <vector>
#include <sstream>
#ifdef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <mysqld_error.h>
#include <mysys_err.h>
#include <m_ctype.h>
#include <my_sys.h>
#include "ha_tnt.h"
#include <mysql/plugin.h>
#endif //WIN32

#include "misc/Global.h"
#include "misc/GlobalFactory.h"
#include "misc/RecordHelper.h"
#include "misc/Parser.h"
#include "misc/ColumnGroupParser.h"
#include "misc/ParFileParser.h"

#ifndef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <mysqld_error.h>
#include <mysys_err.h>
#include <m_ctype.h>
#include <my_sys.h>
#include "ha_tnt.h"
#include <mysql/plugin.h>
#endif //WIN32

#include "tnt_version.h"
#include "ntse_version.h"

#ifdef NTSE_PROFILE
#include "misc/Profile.h"
#endif //NTSE_PROFILE

/*#define DBUG_SWITCH_PRE_CHECK_OPER(thd, dml, read, fun) \
	bool _trxConn = THDVAR(thd, is_trx_connection); \
	int _code = checkOperatePermission(thd, dml, read); \
	if (0 != _code) { \
		DBUG_RETURN(_code); \
	} else if (!_trxConn) { \
		assert(!m_scan && !m_iuSeq); \
		_code = fun; \
		DBUG_RETURN(_code); \
	} \
	assert(!m_ntseScan && !m_ntseIuSeq)*/

#define DBUG_SWITCH_PRE_CHECK_GENERAL(thd, fun, code) \
	bool _trxConn = THDVAR(thd, is_trx_connection); \
	code = checkGeneralPermission(thd); \
	if (!code && !_trxConn) { \
		assert(!m_scan && !m_iuSeq); \
		code = fun; \
		DBUG_RETURN(code); \
	} \
	assert(!m_ntseScan && !m_ntseIuSeq)

#define DBUG_SWITCH_NON_CHECK(thd, fun) \
	int _code = 0; \
	bool _trxConn = THDVAR(thd, is_trx_connection); \
	if (!_trxConn) { \
		_code = fun; \
		DBUG_RETURN(_code); \
	} 

#define DBUG_SWITCH_NON_RETURN(thd, fun) \
	bool _trxConn = THDVAR(thd, is_trx_connection); \
	if (!_trxConn) { \
		fun; \
		DBUG_VOID_RETURN; \
	} \

// TODO：所有出错返回MySQL上层的地方，都需要重新验证处理方式，
// 不能与NTSE完全一致(事务引擎与非事务引擎的区别)

/** 比较路径与TNTShare是否相等的函数对象 */
class ShareEqualer {
public:
	inline bool operator()(const char *path, const TNTShare *share) const {
		return strcmp(path, share->m_path) == 0;
	}
};

/** 计算TableInfoEx对象哈希值的函数对象 */
class ShareHasher {
public:
	inline unsigned int operator()(const TNTShare *share) const {
		return m_strHasher.operator ()(share->m_path);
	}

private:
	Hasher<char *>	m_strHasher;
};

#ifdef NTSE_KEYVALUE_SERVER
KeyValueServer	*keyvalueServer = NULL;	/** keyvalue服务端线程 */
static ThriftConfig keyvalueConfig;	/** keyvalue服务端配置 */
#endif

/************************************************************************/
/* TNT引擎全局变量                                                                     */
/************************************************************************/
handlerton		*tnt_hton;			/** Handler singleton */
static TNTConfig tnt_config;
TNTDatabase		*tnt_db = NULL;		/** 全局唯一的数据库 */
CmdExecutor		*cmd_exec = NULL;	/** 全局唯一的命令执行器 */
TNTTransaction	*dummy_trx = NULL;	/** 假的事务，创建session时若不需要事务支持，则传入此值 */

Mutex			*prepareCommitMutex;/** 全局Mutex，控制事务prepare->commit的顺序*/

typedef DynHash<const char *, TNTShare *, ShareHasher, Hasher<const char *>, ShareEqualer> TblHash;
static TblHash	openTables;			/** 已经打开的表 */
static Mutex	openLock("openLock", __FILE__, __LINE__);			/** 保护已经打开的表的锁 */

///////////////////////////////////////////////////////////////////////////////
// 配置参数与状态变量：For TNT  ///////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
static char		*tnt_version  = NULL;
// static char		*tnt_dump_dir = (char *)".";	// TNT dump文件路径
// static uint     tnt_dump_reserve_cnt = 10;      // TNT dump保留文件夹个数
static unsigned long long tnt_buf_size;	// TNT需要分配的内存大小
static ulong	tnt_lock_timeout;		// 事务锁，等待超时时间设置	
static uint		tnt_max_trx_cnt;		// 事务系统拥有的最大事务数量
static uint     tnt_trx_flush_mode;     // 事务刷日志的方式
static char     tnt_auto_upgrade_from_ntse;

// TNT purge操作可设置属性
//static ulong	tnt_purge_threshold;	// 内存利用率达到多少之后(0,100)，需要purge：TNT_Buf_Size的百分比
//static ulong	tnt_purge_enough;		// Purge回收多少内存pages之后，可以认为purge成功：TNT_Buf_Size的百分比
//static char		tnt_auto_purge;			// TNT引擎是否允许进行auto purge，在系统空闲时
static ulong	tnt_purge_interval;		// 两次purge操作之间的时间间隔：Second
//static char		tnt_purge_before_close;	// TNT引擎关闭前，是否需要进行最后一次purge；默认不需要
//static char     tnt_purge_after_recover;//TNT recover后是否进行purge；默认为需要

// TNT dump操作可设置属性
static ulong	tnt_dump_interval;		// 几次purge操作做一次dump
//static ulong	tnt_dump_interval;		// 两次dump操作之间的时间间隔：Second
//static ulong	tnt_dump_on_redo_size;	// 上次dump以来，写入多少tnt redo日志，需要dump
//static char		tnt_auto_dump;			// TNT引擎是否允许进行auto dump，在系统空闲时
//static char		tnt_dump_before_close;	// TNT引擎关闭前，是否需要进行最后一次dump；默认需要
// TNT version pool相关属性

static ulong	tnt_verpool_file_size;	// 版本池单个文件大小：G
static ulong	tnt_verpool_cnt;		// 版本池数量

static ulong    tnt_defrag_hashIndex_max_traverse_cnt; //defrag hash index时最多遍历的个数
static ulong	tnt_open_table_cnt;

static ulong	tnt_max_trx_run_time;
static ulong	tnt_max_trx_locks;
static ulong	tnt_max_trx_common_idle_time;
static ulong	tnt_max_trx_prepare_idle_time;

static ulong	tnt_mem_index_reclaim_hwm;
static ulong	tnt_mem_index_reclaim_lwm;

///////////////////////////////////////////////////////////////////////////////
// 配置参数与状态变量：For NTSE ///////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
// 以下是全局配置，参见Config类说明
static char		*ntse_tmpdir = (char *)".";
static char		*ntse_logdir = (char *)".";
static char		*ntse_version = NULL;
static ulong	ntse_log_file_cnt_hwm = 0;
static ulong	ntse_log_file_size = 0;
static ulong	ntse_log_buf_size = 0;
static unsigned long long ntse_page_buf_size = 0;
static unsigned long long ntse_rec_buf_size = 0;
static unsigned long long ntse_common_pool_size = 0;
static ulong	ntse_max_sessions = 0;
static ulong	ntse_table_lock_timeout = 0;
static ulong	ntse_scavenger_max_pages = 0;
ulong			ntse_sample_max_pages = 1000;
static ulong	ntse_incr_size;	/** 堆、索引或大对象文件扩展页面数 */
#ifdef NTSE_PROFILE
static int 		ntse_profile_summary = 0; /** 通过设置-1/0/1 来控制全局Profile */
static int		ntse_thread_profile_autorun = 0; /** 通过设置0/1 来控制线程Profile是否自动开启*/
#endif
static char		ntse_backup_before_recover = 0;
static char		ntse_verify_after_recover = 0;
static char		ntse_enable_syslog = 1;	/** NTSE的log是否输出到mysqld.log */
static char		ntse_directio = 1; /** 是否使用directio */
static char		ntse_aio = 1;	/** 是否使用aio */
static ulong	ntse_system_io_capacity = 0; /** 当前系统的最大每秒的IO能力，单位为页面数 */

static TNTGlobalStatus tnt_status;	/** TNT的全局统计信息 */
static Status tnt_ntse_status;		/** TNT的NTSE全局统计信息 */

#ifdef NTSE_KEYVALUE_SERVER
static int	ntse_keyvalue_port = 0;			/** keyvalue服务的端口 */
static int	ntse_keyvalue_servertype = 0;	/** keyvalue服务类型 */
static int	ntse_keyvalue_threadnum = 0;	/** 当keyvalue服务需要线程时，设置线程数目 */
#endif

static SHOW_VAR tnt_ntse_status_variables[] = {
	{"buf_size", (char *)&tnt_ntse_status.m_pageBufSize, SHOW_INT},
	{"buf_log_reads", (char *)&tnt_ntse_status.m_bufStat.m_logicalReads, SHOW_LONGLONG},
	{"buf_phy_reads", (char *)&tnt_ntse_status.m_bufStat.m_physicalReads, SHOW_LONGLONG},
	{"buf_pending_reads", (char *)&tnt_ntse_status.m_bufPendingReads, SHOW_LONGLONG},
	{"buf_read_time", (char *)&tnt_ntse_status.m_bufStat.m_readTime, SHOW_LONGLONG},
	{"buf_prefetches", (char *)&tnt_ntse_status.m_bufStat.m_prefetches, SHOW_LONGLONG},
	{"buf_batch_prefetches", (char *)&tnt_ntse_status.m_bufStat.m_batchPrefetch, SHOW_LONGLONG},
	{"buf_nonbatch_prefetches", (char *)&tnt_ntse_status.m_bufStat.m_nonbatchPrefetch, SHOW_LONGLONG},
	{"buf_prefetch_pages", (char *)&tnt_ntse_status.m_bufStat.m_prefetchPages, SHOW_LONGLONG},
	{"buf_log_writes", (char *)&tnt_ntse_status.m_bufStat.m_logicalWrites, SHOW_LONGLONG},
	{"buf_phy_writes", (char *)&tnt_ntse_status.m_bufStat.m_physicalWrites, SHOW_LONGLONG},
	{"buf_pending_writes", (char *)&tnt_ntse_status.m_bufPendingWrites, SHOW_LONGLONG},
	{"buf_write_time", (char *)&tnt_ntse_status.m_bufStat.m_writeTime, SHOW_LONGLONG},
	{"buf_scavenger_writes", (char *)&tnt_ntse_status.m_bufStat.m_scavengerWrites, SHOW_LONGLONG},
	{"buf_extend_writes", (char *)&tnt_ntse_status.m_bufStat.m_extendWrites, SHOW_LONGLONG},
	{"buf_flush_writes", (char *)&tnt_ntse_status.m_bufStat.m_flushWrites, SHOW_LONGLONG},
	{"buf_page_creates", (char *)&tnt_ntse_status.m_bufStat.m_pageCreates, SHOW_LONGLONG},
	{"buf_alloc_block_fails", (char *)&tnt_ntse_status.m_bufStat.m_allocBlockFail, SHOW_LONGLONG},
	{"buf_replace_searches", (char *)&tnt_ntse_status.m_bufStat.m_replaceSearches, SHOW_LONGLONG},
	{"buf_replace_search_len", (char *)&tnt_ntse_status.m_bufStat.m_replaceSearchLen, SHOW_LONGLONG},
	{"buf_dirty_pages", (char *)&tnt_ntse_status.m_bufStat.m_statusEx.m_dirtyPages, SHOW_LONGLONG},
	{"buf_pinned_pages", (char *)&tnt_ntse_status.m_bufStat.m_statusEx.m_pinnedPages, SHOW_LONGLONG},
	{"buf_rlocked_pages", (char *)&tnt_ntse_status.m_bufStat.m_statusEx.m_rlockedPages, SHOW_LONGLONG},
	{"buf_wlocked_pages", (char *)&tnt_ntse_status.m_bufStat.m_statusEx.m_wlockedPages, SHOW_LONGLONG},
	{"buf_avg_hash_conflict", (char *)&tnt_ntse_status.m_bufStat.m_statusEx.m_avgHashConflict, SHOW_DOUBLE},
	{"buf_max_hash_conflict", (char *)&tnt_ntse_status.m_bufStat.m_statusEx.m_maxHashConflict, SHOW_LONGLONG},
	{"buf_unsafe_lock_fails", (char *)&tnt_ntse_status.m_bufStat.m_unsafeLockFails, SHOW_LONGLONG},
	{"buf_first_touch", (char *)&tnt_ntse_status.m_bufStat.m_firstTouch, SHOW_LONGLONG},
	{"buf_later_touch", (char *)&tnt_ntse_status.m_bufStat.m_laterTouch, SHOW_LONGLONG},
	{"buf_flushall_lock_count", (char *)&tnt_ntse_status.m_bufStat.m_flushAllBufferLockCount, SHOW_LONGLONG},
	{"buf_flushall_lock_time", (char *)&tnt_ntse_status.m_bufStat.m_flushAllBufferLockTime, SHOW_LONGLONG},
	{"buf_scavenger_real_flush_time", (char *)&tnt_ntse_status.m_bufStat.m_realScavengeCnt, SHOW_LONGLONG},
	{"buf_real_dirty_pages", (char *)&tnt_ntse_status.m_realDirtyPages, SHOW_LONGLONG},
	{"log_writes", (char *)&tnt_ntse_status.m_logStat.m_writeCnt, SHOW_LONGLONG},
	{"log_write_size", (char *)&tnt_ntse_status.m_logStat.m_writeSize, SHOW_LONGLONG},
	{"log_flushes", (char *)&tnt_ntse_status.m_logStat.m_flushCnt, SHOW_LONGLONG},
	{"log_flush_pages", (char *)&tnt_ntse_status.m_logStat.m_flushedPages, SHOW_LONGLONG},
	{"log_tail_lsn", (char *)&tnt_ntse_status.m_logStat.m_tailLsn, SHOW_LONGLONG},
	{"log_start_lsn", (char *)&tnt_ntse_status.m_logStat.m_startLsn, SHOW_LONGLONG},
	{"log_checkpoint_lsn", (char *)&tnt_ntse_status.m_logStat.m_ckptLsn, SHOW_LONGLONG},
	{"log_dump_lsn", (char *)&tnt_ntse_status.m_logStat.m_dumpLsn, SHOW_LONGLONG},
	{"log_ntse_size", (char *)&tnt_ntse_status.m_logStat.m_ntseLogSize, SHOW_LONGLONG},
	{"log_tnt_size", (char *)&tnt_ntse_status.m_logStat.m_tntLogSize, SHOW_LONGLONG},
	{"log_flush_padding_size", (char *)&tnt_ntse_status.m_logStat.m_flushPaddingSize, SHOW_LONGLONG},
	{"log_flushed_lsn", (char *)&tnt_ntse_status.m_logStat.m_flushedLsn, SHOW_LONGLONG},
	{"log_flush_single_write_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_single_write_cnt, SHOW_LONGLONG},
	{"log_flush_batch_write_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_batch_write_cnt, SHOW_LONGLONG},
	{"log_flush_check_point_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_check_point_cnt, SHOW_LONGLONG},
	{"log_flush_commit_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_commit_cnt, SHOW_LONGLONG},
	{"log_flush_prepare_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_prepare_cnt, SHOW_LONGLONG},
	{"log_flush_rollback_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_rollback_cnt, SHOW_LONGLONG},
	{"log_flush_purge_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_purge_cnt, SHOW_LONGLONG},
	{"log_flush_ntse_create_index_cnt", (char *)&tnt_ntse_status.m_logStat.m_flush_ntse_create_index_cnt, SHOW_LONGLONG},
	{"mms_size", (char *)&tnt_ntse_status.m_mmsStat.m_occupiedPages, SHOW_LONGLONG},
	{"mms_rec_pages", (char *)&tnt_ntse_status.m_mmsStat.m_recordPages, SHOW_LONGLONG},
	{"mms_queries", (char *)&tnt_ntse_status.m_mmsStat.m_recordQueries, SHOW_LONGLONG},
	{"mms_query_hits", (char *)&tnt_ntse_status.m_mmsStat.m_recordQueryHits, SHOW_LONGLONG},
	{"mms_inserts", (char *)&tnt_ntse_status.m_mmsStat.m_recordInserts, SHOW_LONGLONG},
	{"mms_deletes", (char *)&tnt_ntse_status.m_mmsStat.m_recordDeletes, SHOW_LONGLONG},
	{"mms_updates", (char *)&tnt_ntse_status.m_mmsStat.m_recordUpdates, SHOW_LONGLONG},
	{"mms_rec_replaces", (char *)&tnt_ntse_status.m_mmsStat.m_recordVictims, SHOW_LONGLONG},
	{"mms_page_replaces", (char *)&tnt_ntse_status.m_mmsStat.m_pageVictims, SHOW_LONGLONG},
	{"rows_reads", (char *)&tnt_ntse_status.m_opStat.m_statArr[OPS_ROW_READ], SHOW_LONGLONG},
	{"rows_inserts", (char *)&tnt_ntse_status.m_opStat.m_statArr[OPS_ROW_INSERT], SHOW_LONGLONG},
	{"rows_updates", (char *)&tnt_ntse_status.m_opStat.m_statArr[OPS_ROW_UPDATE], SHOW_LONGLONG},
	{"rows_deletes", (char *)&tnt_ntse_status.m_opStat.m_statArr[OPS_ROW_DELETE], SHOW_LONGLONG},
	{"rowlock_rlocks", (char *)&tnt_ntse_status.m_rowlockStat.m_rlockCnt, SHOW_LONGLONG},
	{"rowlock_wlocks", (char *)&tnt_ntse_status.m_rowlockStat.m_wlockCnt, SHOW_LONGLONG},
	{"rowlock_spins", (char *)&tnt_ntse_status.m_rowlockStat.m_spinCnt, SHOW_LONGLONG},
	{"rowlock_waits", (char *)&tnt_ntse_status.m_rowlockStat.m_waitCnt, SHOW_LONGLONG},
	{"rowlock_wait_time", (char *)&tnt_ntse_status.m_rowlockStat.m_waitTime, SHOW_LONGLONG},
	{"rowlock_active_readers", (char *)&tnt_ntse_status.m_rowlockStat.m_activeReaders, SHOW_LONGLONG},
	{"rowlock_active_writers", (char *)&tnt_ntse_status.m_rowlockStat.m_activeWriters, SHOW_LONGLONG},
	{"rowlock_avg_hash_conflict", (char *)&tnt_ntse_status.m_rowlockStat.m_avgConflictLen, SHOW_DOUBLE},
	{"rowlock_max_hash_conflict", (char *)&tnt_ntse_status.m_rowlockStat.m_maxConflictLen, SHOW_LONGLONG},
	{"checkpoint_count", (char *)&tnt_ntse_status.m_checkpointCnt, SHOW_LONGLONG},
	{"tnt_buf_size", (char *)&tnt_status.m_pageBufSize, SHOW_INT},
	{"tnt_free_buf_size", (char *)&tnt_status.m_freeBufSize, SHOW_INT},
	{"tnt_purge_count", (char *)&tnt_status.m_purgeCnt, SHOW_INT},
	{"tnt_purge_max_time", (char *)&tnt_status.m_purgeMaxTime, SHOW_INT},
	{"tnt_purge_max_begin_time", (char *)&tnt_status.m_purgeMaxBeginTime, SHOW_CHAR},
	{"tnt_purge_avg_time", (char *)&tnt_status.m_purgeAvgTime, SHOW_INT},
	{"tnt_purge_last_time", (char *)&tnt_status.m_purgeLastTime, SHOW_INT},
	{"tnt_purge_skip_tables", (char *)&tnt_status.m_numPurgeSkipTable, SHOW_INT},
	{"tnt_defrag_hashindex_count", (char *)&tnt_status.m_defragHashCnt, SHOW_INT},
	{"tnt_defrag_hashindex_skip_tables", (char *)&tnt_status.m_numDefragHashSkipTable, SHOW_INT},
	{"tnt_dump_count", (char *)&tnt_status.m_dumpCnt, SHOW_INT},
	{"tnt_open_table_count", (char *)&tnt_status.m_openTableCnt, SHOW_LONGLONG},
	{"tnt_real_open_table_count", (char *)&tnt_ntse_status.m_realOpenTableCnt, SHOW_LONGLONG},
	{"tnt_close_table_count", (char *)&tnt_status.m_closeTableCnt, SHOW_LONGLONG},
	{"tnt_real_close_table_count", (char *)&tnt_ntse_status.m_realCloseTableCnt, SHOW_LONGLONG},
	{"tnt_free_mem_call_count", (char *)&tnt_status.m_freeMemCallCnt, SHOW_LONGLONG},
	{"tnt_verpool_switch_count", (char *)&tnt_status.m_switchVersionPoolCnt, SHOW_LONGLONG},
	{"tnt_verpool_switch_last_time", (char *)&tnt_status.m_switchVerPoolLastTime, SHOW_CHAR},
	{"tnt_verpool_reclaim_count", (char *)&tnt_status.m_reclaimVersionPoolCnt, SHOW_LONGLONG},
	{"tnt_verpool_reclaim_last_begin_time", (char *)&tnt_status.m_reclaimVerPoolLastBeginTime, SHOW_CHAR},
	{"tnt_verpool_reclaim_last_end_time", (char *)&tnt_status.m_reclaimVerPoolLastEndTime, SHOW_CHAR},
	{"tnt_verpool_lob_reclaim_count", (char *)&tnt_status.m_verpoolStatus.m_reclaimLobCnt, SHOW_LONGLONG},
	{"tnt_verpool_lob_reclaim_time", (char *)&tnt_status.m_verpoolStatus.m_relaimLobTime, SHOW_LONGLONG},
	{NullS, NullS, SHOW_LONG}
};

static int show_tnt_vars(THD *thd, SHOW_VAR *var, char *buff) {
	// NTSE全局统计信息都不是线程私有的
	tnt_db->getNtseDb()->reportStatus(&tnt_ntse_status);
	tnt_db->reportStatus(&tnt_status);
	
	var->type = SHOW_ARRAY;
	var->value = (char *)&tnt_ntse_status_variables;
	return 0;
}

static SHOW_VAR tntStatus[] = {
	{"Tnt", (char*)&show_tnt_vars, SHOW_FUNC},
	{NullS, NullS, SHOW_LONG}
};


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

static void update_page_buf_size(MYSQL_THD thd, struct st_mysql_sys_var *var,
								 void *var_ptr, const void *save) {
									 uint pageBufSize = 0;
									 Database *ntse_db = tnt_db->getNtseDb();

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
									Database *ntse_db = tnt_db->getNtseDb();

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
										  Database *ntse_db = tnt_db->getNtseDb();

										  ntse_db->getConfig()->m_tlTimeout = lockTimeout;
}


static void update_scavenger_pages(MYSQL_THD thd, struct st_mysql_sys_var *var,
									void *var_ptr, const void *save) {
										ulong scavengerPages = *((ulong *)save);
										*((ulong *)var_ptr) = scavengerPages;
										Database *ntse_db = tnt_db->getNtseDb();

										ntse_db->getConfig()->m_maxFlushPagesInScavenger = scavengerPages;
										ntse_db->getPageBuffer()->setMaxScavengerPages(scavengerPages);
}

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
	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	int len = 0;
	const char *cmd = value->val_str(value, NULL, &len);
	// 保证备份相关命令按顺序调用
	if (Parser::isCommand(cmd, "finishing", "backup", "and", "lock", NULL) ||
		Parser::isCommand(cmd, "end", "backup", NULL)) {
		if (!thdInfo->m_pendingOper || thdInfo->m_pendingOpType != POT_BACKUP) {
			my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "Backup process not started yet");
			return 1;
		}
		TNTBackupProcess *backup = (TNTBackupProcess *)thdInfo->m_pendingOper;
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

static void update_tnt_lock_timeout(MYSQL_THD thd, struct st_mysql_sys_var *var,
									void *var_ptr, const void *save) {
										ulong lockTimeout = *((ulong *)save);
										*((ulong *)var_ptr) = lockTimeout;

										tnt_db->getConfig()->m_txnLockTimeout = lockTimeout;
										tnt_db->getTransSys()->getLockSys()->setLockTimeout((int)lockTimeout);
}

static void update_tnt_dump_interval(MYSQL_THD thd, struct st_mysql_sys_var *var,
									 void *var_ptr, const void *save) {
	ulong dumpInterval = *((ulong *)save);
	*((ulong *)var_ptr) = dumpInterval;
	tnt_db->getTNTConfig()->setDumpInterval(dumpInterval);
}


static void update_tnt_open_table_cnt(MYSQL_THD thd, struct st_mysql_sys_var *var,
									  void *var_ptr, const void *save) {
	ulong tableOpenCnt = *((ulong *)save);
	*((ulong *)var_ptr) = tableOpenCnt;
	tnt_db->setOpenTableCnt(tableOpenCnt);
}


static void update_tnt_connection(MYSQL_THD thd, struct st_mysql_sys_var *var,
								  void *var_ptr, const void *save) {
	bool isTrx = false;
	if (*(my_bool *)save) {
		isTrx = true;
	}
	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	//保证引擎层Connection事务非事务状态与上层一致
	thdInfo->m_conn->setTrx(*((bool *)var_ptr));
	//本身持有的状态与改变的目标状态一致，则不需要改变
	if (thdInfo->m_conn->isTrx() == isTrx) {
		return;
	} else if (!isTrx && thdInfo->m_trx != NULL && thdInfo->m_trx->isTrxStarted()) {
		//事务切换成非事务，将该连接上的未提交事务进行commit
		//ha_tnt::tnt_commit_trx(tnt_hton, thd, true);
		//事务切换成非事务，必须保证当前连接上没有活跃事务
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "This Connection has an active transaction, you must commit/rollback it");
		return;
	} else if (thdInfo->m_inLockTables) {
		//切换连接状态必须保证不再lock tables语句之中
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "This Connection is in lock tables, you must unlock tables");
		return;
	}
	thdInfo->m_conn->setTrx(isTrx);
	*((bool *)var_ptr) = isTrx;
}

static void update_tnt_mindex_reclaim_hwm(MYSQL_THD thd, struct st_mysql_sys_var *var,
										  void *var_ptr, const void *save) {
	ulong hwm = *((ulong *)save);
	// 如果hwm小于lwm，抛错
	if (hwm < tnt_db->getTNTConfig()->m_reclaimMemIndexLwm) {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "high water mark must not smaller than low water mark");
		return;
	}
	tnt_db->getTNTConfig()->m_reclaimMemIndexHwm = hwm;
	*((ulong *)var_ptr) = hwm;	
}

static void update_tnt_mindex_reclaim_lwm(MYSQL_THD thd, struct st_mysql_sys_var *var,
										  void *var_ptr, const void *save) {
	ulong lwm = *((ulong *)save);
	// 如果lwm大于hwm，抛错
	if (lwm > tnt_db->getTNTConfig()->m_reclaimMemIndexHwm) {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "low water mark must not larger than high water mark");
		return;
	}
	tnt_db->getTNTConfig()->m_reclaimMemIndexLwm = lwm;
	*((ulong *)var_ptr) = lwm;	
}

/** 对于ULONG等数值类型的定义，最后一个参数是blk_size，其具体含义应该是：
 * 无论外部设置的参数N是多少，都会转化为N - N % blk_size作为传入的参数值
 * 如果不希望受到这种“对齐”的影响，可以设置为0
 */

// TNT 可设置系统参数
static MYSQL_SYSVAR_STR(tnt_version, tnt_version,
						PLUGIN_VAR_READONLY,
						"TNT version",
						NULL, NULL, NULL);

/*
static MYSQL_SYSVAR_STR(tnt_dump_dir, tnt_dump_dir,
						PLUGIN_VAR_READONLY,
						"The directory to store TNT dump files.",
						NULL, NULL, NULL
						);


static MYSQL_SYSVAR_UINT(tnt_dump_reserve_cnt, tnt_dump_reserve_cnt, 
						PLUGIN_VAR_READONLY,
						"TNT Dump Reserve count.",
						NULL, NULL, 10, 1, ~0, 0);
*/

static MYSQL_SYSVAR_ULONGLONG(tnt_buf_size, tnt_buf_size,
							  PLUGIN_VAR_READONLY,
							  "Size of tnt page buffer in bytes",
							  NULL, NULL, 1024 * NTSE_PAGE_SIZE, 64 * NTSE_PAGE_SIZE, ~0, NTSE_PAGE_SIZE);

static MYSQL_SYSVAR_ULONG(tnt_lock_timeout, tnt_lock_timeout, 
						  PLUGIN_VAR_RQCMDARG,
						  "TNT Transaction Lock Time Out Threshold.",
						  NULL, update_tnt_lock_timeout, 20000, 0, 200000000, 0);

static MYSQL_SYSVAR_UINT(tnt_max_trx_cnt, tnt_max_trx_cnt, 
						 PLUGIN_VAR_READONLY,
						 "TNT Transaction Flush mode.",
						 NULL, NULL, 512, 512, 51200, 0);


static MYSQL_SYSVAR_UINT(tnt_trx_flush_mode, tnt_trx_flush_mode, 
						  PLUGIN_VAR_READONLY,
						  "TNT Transaction Flush mode.",
						  NULL, NULL, 1, 0, 2, 0);

static MYSQL_SYSVAR_BOOL(tnt_auto_upgrade_from_ntse, tnt_auto_upgrade_from_ntse, 
						 PLUGIN_VAR_READONLY, 
						 "Should TNT auto upgrade from ntse.",
						 NULL, NULL, false);
/*
static MYSQL_SYSVAR_ULONG(tnt_purge_threshold, tnt_purge_threshold,
						  PLUGIN_VAR_READONLY,
						  "TNT's purge threshold. (1,99)",
						  NULL, NULL, 80, 1, 99, 0);

static MYSQL_SYSVAR_ULONG(tnt_purge_enough, tnt_purge_enough,
						  PLUGIN_VAR_READONLY,
						  "TNT's purge limit.",
						  NULL, NULL, 50, 0, 80, 0);

static MYSQL_SYSVAR_BOOL(tnt_auto_purge, tnt_auto_purge, 
						 PLUGIN_VAR_OPCMDARG, 
						 "Should TNT do auto purge.",
						 NULL, NULL, true);

static MYSQL_SYSVAR_ULONG(tnt_purge_interval, tnt_purge_interval, 
						  PLUGIN_VAR_READONLY, 
						  "Time interval should we do an auto purge.",
						  NULL, NULL, 10, 0, ~0, 0);

static MYSQL_SYSVAR_BOOL(tnt_purge_before_close, tnt_purge_before_close, 
						 PLUGIN_VAR_READONLY, 
						 "Should TNT do a purge operation before database closed.",
						 NULL, NULL, false);

static MYSQL_SYSVAR_BOOL(tnt_purge_after_recover, tnt_purge_after_recover, 
						 PLUGIN_VAR_READONLY, 
						 "Should TNT do a purge operation after database recover.",
						 NULL, NULL, false);

static MYSQL_SYSVAR_ULONG(tnt_dump_interval, tnt_dump_interval, 
						  PLUGIN_VAR_READONLY, 
						  "Time interval should we do an auto dump.",
						  NULL, NULL, 86400, 3600, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_dump_on_redo_size, tnt_dump_on_redo_size, 
						  PLUGIN_VAR_READONLY, 
						  "A dump should start when TNT write these many redo log.", 
						  NULL, NULL, 104857600, 10485760, ~0, 0);

static MYSQL_SYSVAR_BOOL(tnt_auto_dump, tnt_auto_dump, 
						 PLUGIN_VAR_READONLY, 
						 "Should TNT do auto dump.", 
						 NULL, NULL, true);

static MYSQL_SYSVAR_BOOL(tnt_dump_before_close, tnt_dump_before_close, 
						 PLUGIN_VAR_READONLY, 
						 "Should TNT do a dump operation before database closed.", 
						 NULL, NULL, false);
*/
static MYSQL_SYSVAR_ULONG(tnt_purge_interval, tnt_purge_interval, 
						  PLUGIN_VAR_READONLY, 
						  "Time interval should we do an auto purge.",
						  NULL, NULL, 3, 1, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_dump_interval, tnt_dump_interval, 
						  PLUGIN_VAR_RQCMDARG, 
						  "Time interval should we do an auto dump.",
						  NULL, update_tnt_dump_interval, 1, 0, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_verpool_file_size, tnt_verpool_file_size, 
						  PLUGIN_VAR_READONLY, 
						  "Size of TNT version pool in Bytes.", 
						  NULL, NULL, 12800 * NTSE_PAGE_SIZE, 128 * NTSE_PAGE_SIZE, ~0, NTSE_PAGE_SIZE);

static MYSQL_SYSVAR_ULONG(tnt_verpool_cnt, tnt_verpool_cnt, 
						  PLUGIN_VAR_READONLY, 
						  "Number of TNT version pools.",
						  NULL, NULL, 2, 2, 250, 0);

static MYSQL_SYSVAR_ULONG(tnt_defrag_hashIndex_max_traverse_cnt, tnt_defrag_hashIndex_max_traverse_cnt, 
						  PLUGIN_VAR_READONLY, 
						  "Defrag hash index max traverse count.",
						  NULL, NULL, 32000, 32000, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_open_table_cnt, tnt_open_table_cnt, 
						  PLUGIN_VAR_RQCMDARG, 
						  "Number of Table Opened.",
						  NULL, update_tnt_open_table_cnt, 256, 0, 512*1024L, 0);

static MYSQL_THDVAR_BOOL(is_trx_connection, PLUGIN_VAR_OPCMDARG, 
						 "whether the connnection is trx",
						 NULL, update_tnt_connection, TRUE);

static MYSQL_THDVAR_BOOL(support_xa, PLUGIN_VAR_OPCMDARG,
						 "表示TNT当前线程是否支持MySQL的二阶段提交",
						 NULL, NULL, TRUE);

static MYSQL_SYSVAR_ULONG(tnt_max_trx_run_time, tnt_max_trx_run_time, 
						  PLUGIN_VAR_READONLY, 
						  "max time tnt transaction can run",
						  NULL, NULL, 3600, 0, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_max_trx_locks, tnt_max_trx_locks, 
						  PLUGIN_VAR_READONLY, 
						  "max transaction locks per tnt transaction can hold",
						  NULL, NULL, 200000, 0, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_max_trx_common_idle_time, tnt_max_trx_common_idle_time, 
						  PLUGIN_VAR_READONLY, 
						  "max time tnt transaction can idle",
						  NULL, NULL, 600, 0, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_max_trx_prepare_idle_time, tnt_max_trx_prepare_idle_time, 
						  PLUGIN_VAR_READONLY, 
						  "max time tnt prepared transaction can idle",
						  NULL, NULL, 1200, 0, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_mem_index_reclaim_hwm, tnt_mem_index_reclaim_hwm,
						  PLUGIN_VAR_RQCMDARG,
						  "Count of pages will be reclaimed in meory index high water mark",
						  NULL, update_tnt_mindex_reclaim_hwm, 1000, 0, ~0, 0);

static MYSQL_SYSVAR_ULONG(tnt_mem_index_reclaim_lwm, tnt_mem_index_reclaim_lwm,
						  PLUGIN_VAR_RQCMDARG,
						  "Count of pages will be reclaimed in meory index low water mark",
						  NULL, update_tnt_mindex_reclaim_lwm, 800, 0, ~0, 0);


// NTSE 可设置系统参数
static MYSQL_SYSVAR_STR(version, ntse_version,
						PLUGIN_VAR_READONLY,
						"NTSE version",
						NULL, NULL, NULL);

static MYSQL_SYSVAR_STR(tmpdir, ntse_tmpdir,
						PLUGIN_VAR_READONLY,
						"The directory to store temparory files used by NTSE (used only in index building currently).",
						NULL, NULL, NULL);

static MYSQL_SYSVAR_STR(logdir, ntse_logdir,
						PLUGIN_VAR_READONLY,
						"The directory to store log files used by NTSE (used only in index building currently).",
						NULL, NULL, NULL);

static MYSQL_SYSVAR_ULONG(log_file_cnt_hwm, ntse_log_file_cnt_hwm,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Count of log file high water mark",
						  NULL, NULL, 3, 2, ~0, 0);

static MYSQL_SYSVAR_ULONG(log_file_size, ntse_log_file_size,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Size of log file in bytes.",
						  NULL, NULL, 64 * 1024 * 1024, LogConfig::MIN_LOGFILE_SIZE, ~0, 0);

static MYSQL_SYSVAR_ULONG(log_buf_size, ntse_log_buf_size,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Size of log buffer in bytes.",
						  NULL, NULL, 32 * 1024 * 1024, LogConfig::MIN_LOG_BUFFER_SIZE, ~0, 0);

static MYSQL_SYSVAR_ULONGLONG(page_buf_size, ntse_page_buf_size,
						  PLUGIN_VAR_RQCMDARG,
						  "Size of page buffer in bytes.",
						  NULL, &update_page_buf_size, 1024 * NTSE_PAGE_SIZE, 64 * NTSE_PAGE_SIZE, ~0, NTSE_PAGE_SIZE);

static MYSQL_SYSVAR_ULONGLONG(rec_buf_size, ntse_rec_buf_size,
						  PLUGIN_VAR_RQCMDARG,
						  "Size of record buffer in bytes.",
						  NULL, &update_rec_buf_size, 1024 * NTSE_PAGE_SIZE, 64 * NTSE_PAGE_SIZE, ~0, NTSE_PAGE_SIZE);

static MYSQL_SYSVAR_ULONGLONG(common_pool_size, ntse_common_pool_size,
							  PLUGIN_VAR_RQCMDARG,
							  "Size of record buffer in bytes.",
							  NULL, NULL, 4096 * NTSE_PAGE_SIZE, 4096 * NTSE_PAGE_SIZE, ~0, NTSE_PAGE_SIZE);

static MYSQL_SYSVAR_ULONG(max_sessions, ntse_max_sessions,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Max number of concurrent sessions.",
						  NULL, NULL, 1024, 108, 10000, 0);

static MYSQL_SYSVAR_ULONG(table_lock_timeout, ntse_table_lock_timeout,
						  PLUGIN_VAR_RQCMDARG,
						  "Timeout of table lock in seconds.",
						  NULL, &update_table_lock_timeout, 5, 0, 10000, 0);

static MYSQL_SYSVAR_ULONG(scavenger_pages, ntse_scavenger_max_pages,
						  PLUGIN_VAR_RQCMDARG,
						  "Scavenger Pages in one Round.",
						  NULL, update_scavenger_pages, 200, 50, 1000000, 0);

static MYSQL_SYSVAR_ULONG(sample_max_pages, ntse_sample_max_pages,
						  PLUGIN_VAR_RQCMDARG,
						  "Maximal number of pages used in sampling in obtaining extended status.",
						  NULL, NULL, 1000, 1, 10000, 0);

static MYSQL_SYSVAR_ULONG(incr_size, ntse_incr_size,
						  PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
						  "Number of pages when heap/index/lob file extends.",
						  NULL, NULL, 512, TableDef::MIN_INCR_SIZE, TableDef::MAX_INCR_SIZE, 0);

static MYSQL_THDVAR_UINT(deferred_read_cache_size, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
						 "Max size of cache used in deferred read, used mainly in filesort.",
						 NULL, NULL, 1024 * 1024, 32 * 1024, 1024 * 1024 * 1024, 32 * 1024);

static MYSQL_SYSVAR_BOOL(backup_before_recover, ntse_backup_before_recover,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Backup database before recovery.",
						  NULL, NULL, true);

static MYSQL_SYSVAR_BOOL(verify_after_recover, ntse_verify_after_recover,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Vefiry database after crash recovery.",
						  NULL, NULL, false);

static MYSQL_SYSVAR_BOOL(enable_syslog, ntse_enable_syslog,
						 PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						 "write ntse syslog to mysql log",
						 NULL, NULL, false);

static MYSQL_SYSVAR_BOOL(directio, ntse_directio,
						 PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						 "flush data use O_DIRECT or not",
						 NULL, NULL, true);

static MYSQL_SYSVAR_BOOL(aio, ntse_aio,
						 PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						 "flush page use Aio or not",
						 NULL, NULL, true);

static MYSQL_SYSVAR_ULONG(system_io_capacity, ntse_system_io_capacity,
						  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
						  "Size of log file in bytes.",
						  NULL, NULL, 256, 0, ~0, 0);

static void update_command(MYSQL_THD thd, struct st_mysql_sys_var *var,
	void *var_ptr, const void *save) {
	// 这段代码参考update_func_str函数
	char *cmd = *((char **)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_cmdInfo->setCommand(cmd);
	cmd_exec->doCommand(thdInfo, thdInfo->m_cmdInfo);

	if (thdInfo->m_cmdInfo->getStatus() == CS_FAILED) {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "%s", thdInfo->m_cmdInfo->getInfo());
		// 这里没法用my_printf_error，因为本函数无法返回操作失败，因此上层在执行结束时会assert出错标志位
		// 设置，而调用了my_printf_error后就设置了标志位
		//my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), thdInfo->m_cmdInfo->getInfo());
	}

	*(char **)var_ptr = *(char **)save;
}

static MYSQL_THDVAR_STR(command, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
						"Used to send command to NTSE kernel.",
						&check_command, &update_command, NULL);

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
		my_free(old);
		*((char **)var_ptr) = NULL;
	}
	char *strategy = *((char **)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->setCompressSampleStrategy(strategy);

	*((char **)var_ptr) = my_strdup(*(char **) save, MYF(0));
}

static void update_compress_sample_pct(MYSQL_THD thd, struct st_mysql_sys_var *var, 
									   void *var_ptr, const void *save) {
	uint pct = *((uint*)save);
	assert(pct <= 100);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_tblSmplPct = (u8)pct;

	*((uint*)var_ptr) = pct;
}

static void update_compress_batch_del_size(MYSQL_THD thd, struct st_mysql_sys_var *var,			
										   void *var_ptr, const void *save) {
	uint delSize = *((uint*)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_smplTrieBatchDelSize = delSize;

	*((uint*)var_ptr) = delSize;
}

static void update_compress_cte(MYSQL_THD thd, struct st_mysql_sys_var *var,			
								void *var_ptr, const void *save) {
	uint cte = *((uint*)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_smplTrieCte = cte;

	*((uint*)var_ptr) = cte;
}

static void update_compress_win_size(MYSQL_THD thd, struct st_mysql_sys_var *var,	
									 void *var_ptr, const void *save) { 
	uint winSize = *((uint*)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_tblSmplWinSize = winSize;

	*((uint*)var_ptr) = winSize;
}

static void update_compress_win_detect_times(MYSQL_THD thd, struct st_mysql_sys_var *var,
											 void *var_ptr, const void *save) { 
	uint detectTimes = *((uint*)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_conn->getLocalConfig()->m_tblSmplWinDetectTimes = detectTimes;

	*((uint*)var_ptr) = detectTimes;
}

static void update_compress_win_mem_level(MYSQL_THD thd, struct st_mysql_sys_var *var,	
										  void *var_ptr, const void *save) { 
	uint memLevel = *((uint*)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
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


TNTTHDInfo::TNTTHDInfo(const THD *thd) {
	m_conn = tnt_db->getNtseDb()->getConnection(false);
	m_conn->setThdID(thd_get_thread_id(thd));

	m_trx = NULL;
	m_netWaitTimeout = thd->variables.net_wait_timeout;
#ifdef NTSE_PROFILE
	g_tlsProfileInfo.prepareProfile(thd_get_thread_id(thd), CONN_THREAD, g_profiler.getThreadProfileAutorun());
#endif
}

TNTTHDInfo::~TNTTHDInfo() {
	tnt_db->getNtseDb()->freeConnection(m_conn);
	m_conn = NULL;

	m_trx = NULL;
#ifdef NTSE_PROFILE
	g_tlsProfileInfo.endProfile();
#endif
}

/**
 * 获取连接信息
 *
 * @return 连接信息
 */
TNTTHDInfo* getTntTHDInfo(THD *thd) {
	return *((TNTTHDInfo **)thd_ha_data(thd, tnt_hton));
}

/**
 * 设置连接信息
 *
 * @param THD 连接
 * @param info 连接信息
 */
void setTntTHDInfo(THD *thd, TNTTHDInfo *info) {
	*((TNTTHDInfo **)thd_ha_data(thd, tnt_hton)) = info;
}

/**
 * 检查连接信息是否设置，若没有则创建
 *
 * @param thd THD对象
 */
TNTTHDInfo* checkTntTHDInfo(THD *thd) {
	TNTTHDInfo *info = getTntTHDInfo(thd);
	if (!info) {
		info = new TNTTHDInfo(thd);
		setTntTHDInfo(thd, info);
	}
 	info->m_inLockTables = ((Open_tables_state*)thd)->locked_tables_mode == LTM_LOCK_TABLES;
	return info;
}

// public方法，提供给mysql上层调用
ha_tnt::ha_tnt(handlerton *hton, TABLE_SHARE *table_arg) : ntse_handler(hton, table_arg) {
	ftrace(ts.mysql, tout << this << hton << table_arg);

	m_share		= NULL;
	m_scan		= NULL;
	m_opInfo.m_selLockType = TL_NO;
	m_opInfo.m_mysqlHasLocked = false;
	m_isRndScan	= false;
	//m_thd		= NULL;
	m_table		= NULL;
	//m_conn		= NULL;
	//m_session	= NULL;
	//m_lobCtx	= new MemoryContext(Limits::PAGE_SIZE, 1);
	m_trans		= NULL;
	m_selLockType = TL_NO;
	//m_errno		= 0;
	//m_errmsg[0] = '\0';
	//m_ignoreDup	= false;
	//m_replace	= false;
	m_iuSeq		= NULL;
	//m_lastRow	= INVALID_ROW_ID;
	//m_checkSameKeyAfterIndexScan = false;
	//m_beginTime	= 0;
	//m_isReadAll	= false;
	m_autoInc.m_autoincIncrement = 1;
	m_autoInc.m_autoincOffset = 0;
	m_autoInc.m_autoincLastVal = 0;
}

/**
 * 返回TNT存储引擎执行ALTER TABLE时的能力
 *
 * @param TNT存储引擎执行ALTER TABLE时的能力
 */
uint ha_tnt::alter_table_flags(uint flags) {
	ftrace(ts.mysql, tout << this << flags);
	// Inplace创建、删除索引
	// 约束：目前无法Inplace创建Unique、Primary Index，退化到拷表方式
	uint r = HA_INPLACE_ADD_INDEX_NO_READ_WRITE;
	r |= HA_INPLACE_ADD_INDEX_NO_WRITE;
	r |= HA_INPLACE_DROP_INDEX_NO_READ_WRITE;
	// r |= HA_ONLINE_ADD_PK_INDEX_NO_WRITES;
	r |= HA_INPLACE_DROP_PK_INDEX_NO_READ_WRITE;
	// r |= HA_ONLINE_ADD_UNIQUE_INDEX_NO_WRITES;
	r |= HA_INPLACE_DROP_UNIQUE_INDEX_NO_READ_WRITE;
	return r;
}

/** 检验ntse_xxx之类的参数合法性，在ntse初始化时调用
 * 如果有参数不合法，给出提示，并且退出服务
 */
void ha_tnt::rationalConfigs() {
	// 检查日志参数的合法性
	if (ntse_log_buf_size < LogConfig::MIN_LOG_BUFFER_SIZE) {
		sql_print_warning("ntse_log_buf_size = %lu is invalid because of out of range and it is automatically changed to %lu", ntse_log_buf_size, LogConfig::MIN_LOG_BUFFER_SIZE);
		ntse_log_buf_size = LogConfig::MIN_LOG_BUFFER_SIZE;
	}
	if (ntse_log_file_size < LogConfig::MIN_LOGFILE_SIZE) {
		sql_print_warning("ntse_log_file_size = %lu is invalid because of out of range and it is automatically changed to %lu", ntse_log_file_size, LogConfig::MIN_LOGFILE_SIZE);
		ntse_log_file_size = LogConfig::MIN_LOGFILE_SIZE;
	}

	if (tnt_mem_index_reclaim_hwm < tnt_mem_index_reclaim_lwm) {
		sql_print_warning("tnt_mem_index_reclaim_hwm is invalid because of smaller than lwm and it is automatically changed to %lu", tnt_mem_index_reclaim_lwm);
		tnt_mem_index_reclaim_hwm = tnt_mem_index_reclaim_lwm;
	}
}

/**
* TNT存储引擎初始化
* @param p	TNT存储引擎实例
* @return 成功返回0；失败返回1
*/
int ha_tnt::init(void *p) {
	DBUG_ENTER("ha_tnt::init");

	// 检查初始化参数设置是否ok
	rationalConfigs();

	GlobalFactory::getInstance();
	Tracer::init();

	tnt_hton = (handlerton *)p;

	// 属于TNT的全局与局部参数
	tnt_version = new char[50];
	System::snprintf_mine(tnt_version, 50, "%s r%d", TNT_VERSION, TNT_REVISION);
	
	tnt_config.setTntBasedir(".");
// 	tnt_config.setTntDumpdir(tnt_dump_dir);
// 	tnt_config.setTntDumpReserveCnt(tnt_dump_reserve_cnt);
	set_buf_page_from_para(&tnt_config.m_tntBufSize, &tnt_buf_size);
	tnt_config.m_txnLockTimeout	= tnt_lock_timeout;	
	tnt_config.m_maxTrxCnt = tnt_max_trx_cnt;
	tnt_config.m_verifyAfterRec	= (bool)ntse_verify_after_recover;	// Verify after recover暂时采用NTSE的设置
	tnt_config.m_trxFlushMode = (TrxFlushMode)tnt_trx_flush_mode;
	// purge操作相关配置
	tnt_config.m_purgeInterval	= tnt_purge_interval;
	// dump操作相关配置
	tnt_config.m_dumpInterval = tnt_dump_interval;

/*
	tnt_config.m_purgeThreshold	= tnt_purge_threshold;
	tnt_config.m_purgeEnough	= tnt_purge_enough;
	tnt_config.m_autoPurge		= (bool)tnt_auto_purge;
	tnt_config.m_purgeBeforeClose = (bool)tnt_purge_before_close;
	tnt_config.m_purgeAfterRecover = (bool)tnt_purge_after_recover;
	// dump操作相关配置
	tnt_config.m_dumpInterval	= tnt_dump_interval;
	tnt_config.m_dumponRedoSize	= tnt_dump_on_redo_size;
	tnt_config.m_dumpBeforeClose= (bool)tnt_dump_before_close;
	tnt_config.m_autoDump		= (bool)tnt_auto_dump;
*/
	// version pool相关配置
	tnt_config.m_verpoolFileSize= tnt_verpool_file_size;
	tnt_config.m_verpoolCnt		= (u8)tnt_verpool_cnt;

	tnt_config.m_defragHashIndexMaxTraverseCnt = tnt_defrag_hashIndex_max_traverse_cnt;
	tnt_config.m_openTableCnt = tnt_open_table_cnt;

	tnt_config.m_maxTrxRunTime = tnt_max_trx_run_time;
	tnt_config.m_maxTrxLocks = tnt_max_trx_locks;
	tnt_config.m_maxTrxCommonIdleTime = tnt_max_trx_common_idle_time;
	tnt_config.m_maxTrxPrepareIdleTime = tnt_max_trx_prepare_idle_time;

	tnt_config.m_reclaimMemIndexHwm = tnt_mem_index_reclaim_hwm;
	tnt_config.m_reclaimMemIndexLwm = tnt_mem_index_reclaim_lwm;


	// 属于NTSE的全局与局部参数
	ntse_version = new char[50];
	System::snprintf_mine(ntse_version, 50, "%s r%d", NTSE_VERSION, NTSE_REVISION);

	Config& ntse_config = tnt_config.m_ntseConfig;

	ntse_config.setBasedir(".");	// 与MySQL配合使用时只能设置为.，即为MySQL的datadir
	ntse_config.setTmpdir(ntse_tmpdir);
	ntse_config.setLogdir(ntse_logdir);
	ntse_config.m_logFileCntHwm = ntse_log_file_cnt_hwm;
	ntse_config.m_logFileSize = ntse_log_file_size;
	ntse_config.m_logBufSize = ntse_log_buf_size;
	set_buf_page_from_para(&ntse_config.m_pageBufSize, &ntse_page_buf_size);
	set_buf_page_from_para(&ntse_config.m_mmsSize, &ntse_rec_buf_size);
	set_buf_page_from_para(&ntse_config.m_commonPoolSize, &ntse_common_pool_size);
	ntse_config.m_maxSessions = (u16)ntse_max_sessions;
	ntse_config.m_tlTimeout = ntse_table_lock_timeout;
	ntse_config.m_maxFlushPagesInScavenger = ntse_scavenger_max_pages;
	ntse_config.m_backupBeforeRecover = (bool)ntse_backup_before_recover;
	ntse_config.m_verifyAfterRecover = (bool)ntse_verify_after_recover;
	ntse_config.m_printToStdout = ntse_enable_syslog;
	ntse_config.m_directIo = ntse_directio;
	ntse_config.m_aio = (bool)ntse_aio;
	ntse_config.m_systemIoCapacity = ntse_system_io_capacity;
	//ntse_config.m_enableMmsCacheUpdate = true;	// 为了能正确恢复,先设置true,参见Bug:QA35421
	ntse_config.m_localConfigs.m_accurateTblScan = true;
	ntse_config.m_localConfigs.setCompressSampleStrategy(THDVAR(NULL, compress_sample_strategy));
	ntse_config.m_localConfigs.m_tblSmplPct = THDVAR(NULL, compress_sample_pct);
	ntse_config.m_localConfigs.m_smplTrieBatchDelSize = THDVAR(NULL, compress_batch_del_size);
	ntse_config.m_localConfigs.m_smplTrieCte = THDVAR(NULL, compress_cte);
	ntse_config.m_localConfigs.m_tblSmplWinSize = THDVAR(NULL, compress_win_size);
	ntse_config.m_localConfigs.m_tblSmplWinDetectTimes = THDVAR(NULL, compress_win_detect_times);
	ntse_config.m_localConfigs.m_tblSmplWinMemLevel = THDVAR(NULL, compress_win_mem_level);

	u32 flag = TNT_AUTO_CREATE;
	if (tnt_auto_upgrade_from_ntse) {
		flag |= TNT_UPGRADE_FROM_NTSE;
	}
	try {
		tnt_db = tnt::TNTDatabase::open(&tnt_config, flag, 0);
	} catch (NtseException &e) {
		fprintf(stderr, "%s\n", e.getMessage());
		::abort();
		DBUG_RETURN(1);
	}
	//初始化ntse_handler
	ntse_handler::init(tnt_db->getNtseDb(), &tnt_config.m_ntseConfig);

	tnt_hton->state = SHOW_OPTION_YES;
	tnt_hton->db_type = DB_TYPE_TNT;
	tnt_hton->create = tnt_create_handler;
	tnt_hton->close_connection = tnt_close_connection;
	tnt_hton->drop_database = tnt_drop_database;
#ifdef EXTENDED_FOR_COMMIT_ORDERED
	tnt_hton->commit_ordered=tnt_commit_ordered;
#endif

	tnt_hton->commit = tnt_commit_trx;
	tnt_hton->rollback = tnt_rollback_trx;
	tnt_hton->prepare = tnt_xa_prepare;
	tnt_hton->recover = tnt_xa_recover;
	tnt_hton->commit_by_xid = tnt_trx_commit_by_xid;
	tnt_hton->rollback_by_xid = tnt_trx_rollback_by_xid;

	// 初始化全局参数
	prepareCommitMutex = new Mutex("TNT Prepare Commit Mutex", __FILE__, __LINE__);

	tnt_hton->flags = HTON_NO_FLAGS;

	cmd_exec = new CmdExecutor();

#ifdef NTSE_KEYVALUE_SERVER
	keyvalueConfig.port = ntse_keyvalue_port;
	keyvalueConfig.serverType = ntse_keyvalue_servertype;
	keyvalueConfig.threadNum = ntse_keyvalue_threadnum;

	keyvalueServer = new KeyValueServer(&keyvalueConfig, ntse_db);
	keyvalueServer->start();
#endif

	// 创建一个假的事务对象；只需要事务对象，事务本身不需要开始
	dummy_trx = tnt_db->getTransSys()->allocTrx();

	DBUG_RETURN(0);
}

/**
* 关闭TNT存储引擎
* @param p	TNT存储引擎实例
* @return 成功返回0；失败返回1
*/
int ha_tnt::exit(void *p) {
	ftrace(ts.mysql, tout << p);
	DBUG_ENTER("ha_tnt::exit");
	//先调用ntse_handler exit
	ntse_handler::exit(p);

#ifdef NTSE_KEYVALUE_SERVER
	KeyValueServer::clearCachedTable();
	delete keyvalueServer;
#endif

	if (tnt_db) {
		tnt_db->getTransSys()->freeTrx(dummy_trx);
		tnt_db->close();
		delete tnt_db;
		tnt_db = NULL;
	}
	delete cmd_exec;

	delete []tnt_version;
	delete []ntse_version;

	Tracer::exit();
	// TODO：暂时不知下面的实例在TNT中是否仍旧需要
	GlobalFactory::freeInstance();

	// 释放全局参数
	delete prepareCommitMutex;

	DBUG_RETURN(0);
}


/**
 * 将handler关联到THD对象
 *
 * @param thd THD对象
 * @param name 标识当前THD是由哪个函数attach的
 */
void ha_tnt::attachToTHD(THD *thd, const char *name) {
	assert(!m_thd && thd);

	checkTntTHDInfo(thd);
	m_thd	= thd;
	m_conn	= getTntTHDInfo(thd)->m_conn;
	m_conn->setTrx(THDVAR(thd, is_trx_connection));
	m_name	= System::strdup(name);
}

/**
 * 脱离handler与THD的关联
 */
void ha_tnt::detachFromTHD() {
	assert(m_thd);

	m_thd	= NULL;
	m_conn	= NULL;
	delete	[]m_name;
	m_name	= NULL;
}

/** 判断能不能进行在线表模式修改
 * @param create_info 新的表模式
 * @param table_changes  反映新旧表模式之间区别的参数
 * @return 能否进行在线表模式修改
 */
bool ha_tnt::check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes) {
	ftrace(ts.mysql, tout << this << create_info << table_changes);

	if (table_changes != IS_EQUAL_YES)
		return COMPATIBLE_DATA_NO;
	
	// 检查auto increment列是否发生变化
	// 目前，auto increment列不支持Inplace修改
	if ((create_info->used_fields & HA_CREATE_USED_AUTO) && create_info->auto_increment_value != 0)
		return COMPATIBLE_DATA_NO;
	return COMPATIBLE_DATA_YES;
}

/**
 * 返回表类型
 * @return 总是返回TNT
 */
const char* ha_tnt::table_type() const {
	return "TNT";
}

/** 返回指定索引的类型
 * @param inx 表中的第几个索引，从0开始编号。
 * @return 索引类型，TNT使用B+树索引，总是返回"BTREE"
 */
const char* ha_tnt::index_type(uint inx) {
	return "BTREE";
}

/**
 * 存储引擎使用的扩展名。TNT引擎与NTSE引擎完全一致。
 */
static const char *ha_tnt_exts[] = {
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
* 存储引擎使用的空扩展名。与NTSE引擎一致。
*/
static const char *ha_tnt_nullExts[] = {
	NullS, NullS
};


/**
 * 返回存储使用的文件扩展名数组
 *
 * @return TNT使用的文件扩展名数组
 */
const char** ha_tnt::bas_ext() const {
	try {
		/** 检查bas_ext的调用是否是repair table ... use_frm产生。 */
		if (m_thd && TT_USEFRM == m_thd->lex->check_opt.sql_flags && SQLCOM_REPAIR == m_thd->lex->sql_command) {
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "'repair table ... use_frm' statement is not supported by tnt");
		}

		return ha_tnt_exts;
	} catch (NtseException &e) {
		/**
		 *	由于bas_ext为const成员函数，而reportError非const，需要强制转化
		 */
		const_cast<ha_tnt* const>(this)->reportError(e.getErrorCode(), e.getMessage(), NULL, true);

		return ha_tnt_nullExts;
	}
}

/**
 * 返回标识存储引擎能力的一系列标志
 *
 * @return 表示TNT存储引擎能力的标志
 */
ulonglong ha_tnt::table_flags() const {
	ulonglong flags = HA_PARTIAL_COLUMN_READ;
	flags |= HA_NULL_IN_KEY;
	flags |= HA_DUPLICATE_POS;
	flags &= ~HA_NO_PREFIX_CHAR_KEYS;
	flags |= HA_FILE_BASED;
	flags |= HA_CAN_INDEX_BLOBS;
	flags |= HA_BINLOG_ROW_CAPABLE;
	flags |= HA_REC_NOT_IN_SEQ;
	flags |= HA_PRIMARY_KEY_REQUIRED_FOR_DELETE;
	// 目前，TNT不支持语句级binlog复制
	// flags |= HA_BINLOG_STMT_CAPABLE;

	return flags;
}

/**
 * 返回标识存储引擎索引实现能力的一系列标志
 *
 * @param inx 第几个索引
 * @param part 索引扫描条件包含前多少个索引属性
 * @param all_parts 条件是否包含所有索引属性
 * @return 标识TNT存储引擎索引实现能力的标志
 */
ulong ha_tnt::index_flags(uint inx, uint part, bool all_parts) const {
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
 * @return TNT存储引擎支持的最大记录长度，不包含大对象
 */
uint ha_tnt::max_supported_record_length() const {
	return Limits::DEF_MAX_REC_SIZE;
}

/**
 * 返回一张表最多可能有多少个索引
 *
 * @return TNT中一张表最多可能有多少个索引
 */
uint ha_tnt::max_supported_keys() const {
	return Limits::MAX_INDEX_NUM;
}

/**
 * 返回索引最多包含多少属性
 *
 * @return TNT中索引最多包含多少属性
 */
uint ha_tnt::max_supported_key_parts() const {
	return Limits::MAX_INDEX_KEYS;
}

/**
 * 返回NTSE存储引擎支持的最大索引键长度
 *
 * @return TNT存储引擎支持的最大索引键长度
 */
uint ha_tnt::max_supported_key_length() const {
	return DrsIndice::IDX_MAX_KEY_LEN;
}

/** 返回NTSE存储引擎支持的最大索引键属性长度
 * @return TNT存储引擎支持的最大索引键属性长度
 */
uint ha_tnt::max_supported_key_part_length() const {
	return DrsIndice::IDX_MAX_KEY_PART_LEN;
}

/** 判断当前是不是Slave SQL线程的操作
 * @return true表示是Slave SQL线程，false表示不是
 */
bool ha_tnt::isSlaveThread() const {
	return isSlaveThread(m_thd);
}


/** 判断当前是不是Slave SQL线程的操作
 * @return true表示是Slave SQL线程，false表示不是
 */
bool ha_tnt::isSlaveThread(THD *thd) const {
	return (thd != NULL && thd->slave_thread);
}

/**
 * 返回表扫描代价。目前使用存储引擎示例中的默认实现
 * @post 默认实现的代价偏大，应该根据表的页面数估计
 * @return 表扫描代价
 */
double ha_tnt::scan_time() {
	return (double) (stats.records + stats.deleted) / 20.0+10;
}


/**
 * 返回表数据上限，用于上层做filesort
 * @post 必须大于等于表中真实数据值
 * @return 表记录上限数目
 */
ha_rows ha_tnt::estimate_rows_upper_bound() {
	TableDef *tableDef =  m_table->getNtseTable()->getTableDef();
	u16 recLen = 0;
	u64 estimate = 0;
	u64 pageNum = 0;
	ColumnDef *colDef = NULL;
	assert(m_table->getMetaLock(m_session) != IL_NO);
	// 是否是压缩表不能通过表定义的recFormat判断，只能通过是否有压缩字典来判断
	if (m_table->getNtseTable()->getRecords()->hasValidDictionary()) {
		// 压缩表无法预估记录数目，且压缩表一定是一张大表
		estimate = (u64)-1;
	} else {
		pageNum = m_table->getNtseTable()->getHeap()->getMaxPageNum();
			for(u16 i = 0; i < tableDef->m_numCols; i++) {
				colDef = tableDef->getColumnDef(i);
				if (colDef->varSized() || colDef->isLob())
					recLen += colDef->m_size < 128 ? 1 : 2;
				else
					recLen += colDef->m_size;
			}
		// 加上null bitmap的字节数
		recLen += tableDef->m_bmBytes;
		estimate = 2 * pageNum * NTSE_PAGE_SIZE / recLen;	
	}
	return (ha_rows)estimate;
}

/**
 * 返回读取指定条数记录的代价。目前使用存储引擎示例中的默认实现
 * @post 默认实现的代价未考虑索引覆盖扫描与索引非覆盖扫描的区别
 * @return 读取代价
 */
double ha_tnt::read_time(uint index, uint ranges, ha_rows rows) {

	/*
	if (strcmp(m_table->getNtseTable()->getTableDef()->m_name, "order_line") == 0) {
		double index_read_time;
		double table_read_time;

		index_read_time = (double) rows /  20.0 + 1;
		table_read_time = (double) (stats.records + stats.deleted) / 20.0+10;

		if (index_read_time >= table_read_time)	{
			printf("stat error\n");
			printf("table stat: rows = %d, deleted = %d, read_time = %f\n", stats.records, stats.deleted, table_read_time);
			printf("index stat: rows = %d, read_time = %f\n", rows, index_read_time);

			// NTSE_ASSERT(0);
		}

		index_read_time = (double) (ranges + rows);

		if (index_read_time >= table_read_time)	{
			printf("stat error\n");
			printf("table stat: rows = %d, deleted = %d, read_time = %f\n", stats.records, stats.deleted, table_read_time);
			printf("index stat: rows = %d, read_time = %f\n", rows, index_read_time);

			// NTSE_ASSERT(0);
		}

	}
	*/

	return (double) (ranges + rows);
}

/**
* 此函数主要用于返回TNT表的各种统计信息，用于MySQL查询优化使用
* 除此之外，还返回其他的一些杂项信息
*  
* @param flag 需要返回的信息类型
* @return 总是return 0
*/
int	ha_tnt::info(uint flag) {
	ftrace(ts.mysql, tout << this << flag);

	DBUG_ENTER("ha_tnt::info");

	// TODO：Info函数暂时直接采用NTSE的方法，后续改进

	Table *ntseTable = m_table->getNtseTable();

	if (flag & HA_STATUS_VARIABLE) {
		stats.records = ntseTable->getRows();
		ha_rows estimateRecords = 0;
		stats.deleted = 0;
		if (m_session && m_table->getMetaLock(m_session) != IL_NO) {
			// 一般情况下如进行正常的查询处理时是加了表锁的
			stats.data_file_length = ntseTable->getDataLength(m_session);
			stats.index_file_length = ntseTable->getIndexLength(m_session, false);
			// 按照堆页面数除以最长长度得到一个预估的记录数
			estimateRecords = ntseTable->getRecords()->getHeap()->getUsedSize() / ntseTable->getTableDef()->m_maxRecSize;
		} else {
			// SHOW TABLE STATUS时没有加表锁
			stats.data_file_length = 0;
			stats.index_file_length = 0;
			Session *session = m_session;
			if (!session) {
				attachToTHD(ha_thd(), "ha_tnt::info");
	
				session = createSessionAndAssignTrx("ha_tnt::info", m_conn, dummy_trx, 1000);
				if (!session)
					detachFromTHD();
			}

			if (session) {
				try {
					m_table->lockMeta(session, IL_S, 1000, __FILE__, __LINE__);
					stats.data_file_length = ntseTable->getDataLength(session);
					stats.index_file_length = ntseTable->getIndexLength(session, false);
					// 按照堆页面数除以最长长度得到一个预估的记录数
					estimateRecords = ntseTable->getRecords()->getHeap()->getUsedSize() / ntseTable->getTableDef()->m_maxRecSize;
					m_table->unlockMeta(session, IL_S);
				} catch (NtseException &) {
					// 忽略异常
				}
				if (session != m_session) {
					tnt_db->getNtseDb()->getSessionManager()->freeSession(session);
					detachFromTHD();
				}
			}
		}
		stats.delete_length = 0;
		stats.check_time = 0;
		// 为了防止大量删除操作使得页面采样的时候采到大量空页估错表内记录数，从而导致执行计划走错
		// 这里需要取采样统计值和 堆文件大小/最大记录长度 的较大值
		stats.records = max(stats.records, estimateRecords);
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
			// TNT是事务引擎，因此所见到的列已经加锁，不需要cache到Handler层面，保存rowid即可，下次访问一定存在
			// TNT是堆表，因此保留RowId即可
			RID_WRITE(m_iuSeq->getScanHandle()->getCurrentRid(), dup_ref);
			errkey = m_iuSeq->getDupIndex();
		} else
			errkey = m_dupIdx;
	}
	if ((flag & HA_STATUS_AUTO) && table->found_next_number_field) {
		m_table->enterAutoincMutex();
		stats.auto_increment_value = m_table->getAutoinc();
		m_table->exitAutoincMutex();
	}
	DBUG_RETURN(0);
}

/**
 * 给存储引擎一些提示
 *
 * @param operation 提示信息
 * @return 总是返回0
 */
int ha_tnt::extra(enum ha_extra_function operation) {
	DBUG_ENTER("ha_tnt::extra");
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
 * MySQL上层调用这一函数由存储引擎决定对将要执行的语句需要加什么样的表锁(MySQL上层锁)。
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
THR_LOCK_DATA** ha_tnt::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
	//非事务连接操作事务表或者非事务表都走ntse流程
	if (!THDVAR(thd, is_trx_connection)) {
		return ntse_handler::store_lock(thd, to, lock_type);
	}

	// 0. 将lock_type == TL_IGNORE拿出来单独处理
	// 首先，下面的处理逻辑都将TL_IGNORE排除在外
	// 其次，TL_IGNORE模式下，attachToTHD函数处理有问题
	// 最后，TL_IGNORE出现的流程是：JOIN::optimize -> store_lock -> external_lock -> store_lock(TL_IGNORE);针对语句：select * from *** where id = **;
	if (lock_type == TL_IGNORE) {
		*to++= &m_mysqlLock;

		return(to);
	}
	
	TrxIsolationLevel isoLevel;

	// 1. 获取事务，设置事务的隔离级别，以及对底层行记录的加锁模式
	attachToTHD(thd, "ha_tnt::store_lock");

	m_trans = getTransForCurrTHD(thd);

	if (lock_type != TL_IGNORE && m_trans->getTableInUse() == 0) {
		isoLevel = getIsolationLevel((enum_tx_isolation) thd_tx_isolation(thd));
		m_trans->setIsolationLevel(isoLevel);

		if (isoLevel <= TRX_ISO_READ_COMMITTED) {
			// 若隔离级别小于READ_COMMITTED，则采用语句级ReadView
			// TODO:...
		}
	}

	const bool in_lock_tables = thd_in_lock_tables(thd);
	enum_sql_command sql_command = thd->lex->sql_command;

	// 2. 根据命令，部分设置TNT层面的加锁模式

	if (sql_command == SQLCOM_DROP_TABLE) {
		// 2.1 drop table操作，此处不设置加锁模式

	} else if ((lock_type == TL_READ && in_lock_tables)
		   || (lock_type == TL_READ_HIGH_PRIORITY && in_lock_tables)
		   || lock_type == TL_READ_WITH_SHARED_LOCKS
		   || lock_type == TL_READ_NO_INSERT
		   || (lock_type != TL_IGNORE && sql_command != SQLCOM_SELECT)) {
				
				// 2.2 MySQL的语句是lock tables ... read local
			    // 或者 ...
			    // 或者 当前的语句是select ... in share mode
			    // 或者 当前的语句是insert into ... select ...，同时Binlog需要使用lock read；
			    //		当前的语句是lock tables ... read
			    // 或者 非select语句，由TNT决定行锁模式，同时此模式在external_lock函数中可能会被增强

				isoLevel = m_trans->getIsolationLevel();
				if (isoLevel <= TRX_ISO_READ_COMMITTED
					&& (lock_type == TL_READ || lock_type == TL_READ_NO_INSERT)
					&& (sql_command == SQLCOM_INSERT_SELECT
					|| sql_command == SQLCOM_REPLACE_SELECT
					|| sql_command == SQLCOM_UPDATE
					|| sql_command == SQLCOM_CREATE_TABLE)) {
						
						// 2.2.1 若当前的隔离级别是READ_COMMITTED
						// 同时 MySQL正在进行的操作是insert into ... select 或者 replace into ... select 
						//		或者update ... = (select ...) 或者create table ... select ...
						// 同时这些操作的select语句都没有指定for update (in share mode)，那么直接使用快照读
						m_selLockType	= TL_NO;
		} else if (sql_command == SQLCOM_CHECKSUM) {

			// 2.2.2 对于checksum table ...使用快照读
			m_selLockType	= TL_NO;
		} else {

			// 2.2.3 其他操作，暂时设置为对行加S锁，此模式在external_lock函数中可能会被升级为X锁
			m_selLockType	= TL_S;
		}
	} else if (lock_type != TL_IGNORE) {

		// 2.3 对于可能的行X锁，在external_lock函数中统一设置
		m_selLockType	= TL_NO;
	}

	// 3. 根据不同的操作类型，适当将MySQL上层的锁模式降级(或者升级)
	if (lock_type != TL_IGNORE &&m_mysqlLock.type == TL_UNLOCK) {

		if (lock_type == TL_READ && sql_command == SQLCOM_LOCK_TABLES) {
			// 3.1 若当前是lock tables ... read local，则将锁模式升级，与InnoDB/MyISAM引擎的行为保持一致
			lock_type = TL_READ_NO_INSERT;
		}
		
		if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE)
		    && !(in_lock_tables
			 && sql_command == SQLCOM_LOCK_TABLES)
		    && sql_command != SQLCOM_TRUNCATE
		    && sql_command != SQLCOM_OPTIMIZE
		    && sql_command != SQLCOM_CREATE_TABLE) {

				// 3.2 若当前的语句不是lock/truncate/optimize/create table
				// 那么可以将更高限制的写锁转化为支持并发写锁
				lock_type = TL_WRITE_ALLOW_WRITE;
		}

		if (lock_type == TL_READ_NO_INSERT && sql_command != SQLCOM_LOCK_TABLES) {
			// 3.3 对于MySQL上层指定的TL_READ_NO_INSERT(insert into t1 select ... from t2，
			// 此时会指定此模式)，适当降级此模式，保证表t2上可以同时进行insert操作
			// 同时，对于stored procedure call，此时也可以降级模式；
			// lock tables ... 语法，模式不可改变；
			lock_type = TL_READ;
		}
		m_mysqlLock.type = lock_type;
	}

	*to++= &m_mysqlLock;
	
	// 将扫描加锁模式，复制到TNTOpInfo中，可以通过参数传递到下层
	m_opInfo.m_selLockType = m_selLockType;

	m_beginTime = System::microTime();

	detachFromTHD();

	return(to);
}

/**
 * 一般情况下，任何语句开始操作一张表之前都会调用这一函数通知存储引擎MySQL上层准备加锁，
 * 语句结束，MySQL会调用此函数，通知存储引擎MySQL上层已经完成解锁动作
 * 但如果语句用LOCK TABLES/UNLOCK TABLES括起，则在LOCK TABLES时调用store_lock和external_lock加锁，
 * UNLOCK TABLES时放锁，期间不再会调用store_lock和external_lock函数。
 *
 * @param thd 当前连接
 * @param lock_type 为F_UNLCK表示解锁，其它情况为读写锁
 * @return 成功返回0，等锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT，取不到会话返回HA_ERR_NO_CONNECTION
 */
int ha_tnt::external_lock(THD *thd, int lock_type) {
	DBUG_ENTER("ha_tnt::external_lock");
	int code = 0;
	if (lock_type == F_UNLCK) {
		DBUG_SWITCH_NON_CHECK(thd, ntse_handler::external_lock(thd, lock_type));

		// 处理语句结束时的情况
		if (m_iuSeq) {
			m_table->freeIUSequenceDirect(m_iuSeq);
			m_iuSeq = NULL;
		}
		
		// TODO：这个判断的作用是？
		if (!m_thd)	// mysql_admin_table中没有检查加锁是否成功
			DBUG_RETURN(code);

		assert(m_session);

		endScan();

		// 处理事务中的表信息
		assert(m_thd == thd);
		m_trans = getTransInTHD(thd);
		assert(m_trans != NULL);

		m_trans->decTablesInUse();
		m_opInfo.m_mysqlHasLocked = false;

		if (m_trans->getTableInUse() == 0) {
			// m_trans->setTablesLocked(0);

			if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
				// 快照读事务，MySQL上层不会调用commit，因此需要在external_lock函数中提交；
				// 当前读事务(select lock in share mode/select for update)，同样在external_lock函数中
				// 提交事务，但是MySQL上层接下来会调用ha_autocommit_or_rollback函数。
				if (m_trans->isTrxStarted()) {
					// 验证thd中的事务，与当前handler中的事务一定一致
					// 同时m_trans空间，同样在tnt_commit_trx中释放
					assert(m_trans == getTransInTHD(thd));

					tnt_commit_trx(tnt_hton, thd, true);

					m_trans = NULL;
				}

				// 若当前m_trans未开始，则直接回收此事务
				// tnt_db->getTransSys()->releaseTrx(m_trans);
				// m_trans = NULL;
			} else {
				// 当前不是autocommit事务，根据事务的隔离级别，可将可见性从事务级降低为语句级
				// TODO:
				
			}
		}

		// 完成事务处理之后，释放表上的MetaLock，并关闭session
		// TODO：每个语句结束时，关闭session是否合适？
		// 目前暂时这么实现，不会有问题，等测试之后，根据性能反馈调整
		if (m_table->getMetaLock(m_session) != IL_NO)
			m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

		tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
		m_session = NULL;

		detachFromTHD();

	} else {
		attachToTHD(thd, "ha_tnt::external_lock");
		// 分配会话
		assert(!m_session);
		m_session = createSession("ha_tnt::external_lock", m_conn, 100);
		if (!m_session) {
			detachFromTHD();
			DBUG_RETURN(HA_ERR_NO_CONNECTION);
		}
		// 对当前表，加MetaLock
		m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
		DBUG_SWITCH_PRE_CHECK_GENERAL(thd, ntse_handler::external_lock(thd, lock_type), code);
		if (code != 0) {
			m_table->unlockMeta(m_session, IL_S);
			tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			detachFromTHD();
			DBUG_RETURN(code);
		}

		// 处理tnt语句开始时的情况
		// 设置语句开始标识，TNTTable模块可以将此标识与m_selLockType结合，
		// 判断是对表加意向锁，还是创建ReadView
		m_opInfo.m_sqlStatStart = true;
		m_opInfo.m_mysqlOper	= true;

		// 获取事务
		m_trans = getTransForCurrTHD(thd);
		m_session->setTrans(m_trans);

		// 不同于NTSE，table lock不需要在此处显示加锁
		// 本次操作需要写锁，若为select，则对应的语句是
		// update table ... 或者是 select *** for update
		if (lock_type == F_WRLCK) {
			m_selLockType = TL_X;
		}

		// 若当前是Serializable隔离级别的查询，则使用当前读，对记录加S锁
		if (m_trans->getIsolationLevel() == TRX_ISO_SERIALIZABLE
			&& m_selLockType == TL_NO
			&& thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
				m_selLockType = TL_S;
		}

		m_opInfo.m_selLockType = m_selLockType;

		// 若当前查询需要对行记录加锁，则判断是否需要处理lock tables逻辑
		if (m_opInfo.m_selLockType != TL_NO) {
			if ((thd_sql_command(thd) == SQLCOM_LOCK_TABLES
				// && THDVAR(thd, table_locks)
				&& thd_test_options(thd, OPTION_NOT_AUTOCOMMIT)
				&& thd_in_lock_tables(thd)) 
				|| thd_sql_command(thd) == SQLCOM_ALTER_TABLE 
				|| thd_sql_command(thd) == SQLCOM_CREATE_TABLE) {
					// 加表锁，此时的表锁模式，即为select lock type，不需要转换
					try {
						// 直接读取m_id，因为已经加上meta S锁
						TableId tid = m_table->getNtseTable()->getTableDef()->m_id;
						
						m_trans->lockTable(m_opInfo.m_selLockType, tid);

						// 加表锁成功之后，设置当前事务处于Lock Tables命令保护之中
						m_trans->setInLockTables(true);
					} catch (NtseException &e) {
						// 加表锁失败，释放已持有资源，并返回
						// TODO：此时报错的类型，可能是超时；也可能是死锁，此时需要rollback当前事务
						reportError(e.getErrorCode(), e.getMessage(), thd, false);

						m_table->unlockMeta(m_session, IL_S);

						// 不能与NTSE同样处理，TNT出错之后，由MySQL上层调用rollback完成进行事务回滚
						// 因此此时不需要回滚事务，但是由于Session对象在Handler上，因此需要销毁
						tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
						m_session = NULL;
						detachFromTHD();

						DBUG_RETURN(m_errno);
					}
			}
		}

		if (!m_beginTime) {
			m_beginTime = System::microTime();
		}

		m_trans->incTablesInUse();

		// 语句执行成功external_lock之后，将以下参数设置为true
		// 用于start_stmt函数中区分当前是temp table还是正常table
		m_opInfo.m_mysqlHasLocked = true;
	}

	DBUG_RETURN(0);
}

/** 在LOCK TABLES括起的语句中MySQL在语句开始时会调用此函数，语句结束时不会调用
 *
 * @param thd 连接
 * @param lock_type 对表加的锁
 * @return 总是返回0
 */
int ha_tnt::start_stmt(THD *thd, thr_lock_type lock_type) {
	DBUG_ENTER("ha_tnt::start_stmt");
	
	// 若sql执行过程中，thd发生改变，那么直接报错，目前TNT不支持
	if (thd != m_thd)
		DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "THD changed during sql processing", thd, false));

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::start_stmt(thd, lock_type));

	// 语句开始，设置开始标识，TNTTable模块可以将此标识与m_selLockType结合，
	// 判断是对表加意向锁，还是创建ReadView
	m_opInfo.m_sqlStatStart = true;
	m_opInfo.m_mysqlOper	= true;

	// 获取事务
	m_trans = getTransForCurrTHD(thd);

	if (!m_opInfo.m_mysqlHasLocked) {
		// 若未设置此参数，说明当前handler对应的是一个在lock tables语句内部创建的临时表
		// 这样的临时表，MySQL并不调用external_lock；因此必须加行级x锁，针对可能的更新
		m_selLockType = TL_X;

	} else if (m_trans->getIsolationLevel() != TRX_ISO_SERIALIZABLE
		&& thd_sql_command(thd) == SQLCOM_SELECT
		&& lock_type == TL_READ) {
			// 对于非临时表的只读操作，使用一致读
			m_selLockType = TL_NO;

	} else {
		// 对于其他非一致读操作，保持加锁模式不变即可

	}

	m_opInfo.m_selLockType = m_selLockType;

	// 将事务注册到MySQL 2PC中
	tntRegisterTrx(tnt_hton, thd, m_trans);

	DBUG_RETURN(0);
}

/**	将TNT事务注册到MySQL的XA事务中，同一事务，可重复注册
*	@param	hton 
*	@param	thd	 MySQL的thread
*	@param	trx	 当前TNT事务
*/
void ha_tnt::tntRegisterTrx(handlerton *hton, THD *thd, TNTTransaction *trx) {
	// 首先注册到statement链表中
	trans_register_ha(thd, false, hton);

	// 若为第一次注册，并且当前不是autocommit，则还需要注册到all链表
	if (!trx->getTrxIsRegistered()
		&& thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
			trans_register_ha(thd, true, hton);
	}

	// 设置已注册状态
	trxRegisterFor2PC(trx);
}

/**	标识一个TNT事务已经注册到MySQL的XA事务中
*	
*	@param trx	TNT事务
*
*/
void ha_tnt::trxRegisterFor2PC(TNTTransaction *trx) {
	trx->setTrxRegistered(true);
}

/**	标识一个TNT事务从MySQL的XA事务中取消注册
*	
*	@param trx	TNT事务
*
*/
void ha_tnt::trxDeregisterFrom2PC(TNTTransaction *trx) {
	trx->setTrxRegistered(false);
	trx->setCalledCommitOrdered(false);
}

/**	将上层定义的隔离级别，转化为TNT层面的事务隔离级别
*	@param	iso	MySQL上层定义的隔离级别
*	@return	返回TNT层面定义的事务隔离级别
*/
TrxIsolationLevel ha_tnt::getIsolationLevel(enum_tx_isolation iso) {
	switch(iso) {
		case ISO_REPEATABLE_READ: 
			return(TRX_ISO_REPEATABLE_READ);
		case ISO_READ_COMMITTED: 
			return(TRX_ISO_READ_COMMITTED);
		case ISO_SERIALIZABLE: 
			return(TRX_ISO_SERIALIZABLE);
		case ISO_READ_UNCOMMITTED: 
			return(TRX_ISO_READ_UNCOMMITTED);
		default: 
			return(TRX_ISO_UNDEFINED);
	}
}

/**	获取当前thd对应的事务，若事务不存在则新建
*	@param	thd
*	@return
*/
TNTTransaction* ha_tnt::getTransForCurrTHD(THD *thd) {
	TNTTHDInfo *info = getTntTHDInfo(thd);
	assert(info != NULL);
	if (!info->m_conn->isTrx()) {
		return NULL;
	}

	TNTTransaction *trx = info->m_trx;
	if (!trx) {
		try {
			trx = tnt_db->getTransSys()->allocTrx();
			assert(trx != NULL);

			// 将新建的事务与MySQL的THD绑定
			info->m_trx = trx;
		} catch (NtseException &e){
			// 当前系统中有过多的事务，目前处理为直接退出进程
			reportError(e.getErrorCode(), e.getMessage(), thd, true);
			return NULL;
		}
	}

	// TODO：TNT的实现，暂时在创建事务的时候直接将事务开始
	bool innerTrx = false;
	if (thd_sql_command(thd) == SQLCOM_CREATE_TABLE || thd_sql_command(thd) == SQLCOM_CREATE_INDEX
		|| thd_sql_command(thd) == SQLCOM_ALTER_TABLE || thd_sql_command(thd) == SQLCOM_TRUNCATE 
		|| thd_sql_command(thd) == SQLCOM_DROP_INDEX)
		innerTrx = true;
	trx->startTrxIfNotStarted(info->m_conn, innerTrx);
	// 将事务注册到MySQL 2PC
	tntRegisterTrx(tnt_hton, thd, trx);
	// 将事务和当前thd绑定
	trx->setThd(thd);


	if (trx->getTrxState() == TRX_PREPARED) {
		if (thd->variables.net_wait_timeout > tnt_db->getTNTConfig()->m_maxTrxPrepareIdleTime) {
			thd->variables.net_wait_timeout = tnt_db->getTNTConfig()->m_maxTrxPrepareIdleTime;
		}
	} else if (thd->variables.net_wait_timeout > tnt_db->getTNTConfig()->m_maxTrxCommonIdleTime) {
		thd->variables.net_wait_timeout = tnt_db->getTNTConfig()->m_maxTrxCommonIdleTime;
	}

	return trx;
}

/**	获取当前thd对应的事务，事务必定存在
*	@param	thd
*	@return
*/
TNTTransaction* ha_tnt::getTransInTHD(THD *thd) {
	TNTTHDInfo *info = getTntTHDInfo(thd);

	assert(info != NULL);

	TNTTransaction *trx = info->m_trx;

	assert(trx != NULL);

	return trx;
}

/**
 * 打开表。在上层操作某张表之前，会调用这一函数要求存储引擎打开这张表。
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
 *  不等待。像InnoDB一样不考虑这一参数。
 * @return 成功返回0，失败返回错误码
 */
int ha_tnt::open(const char *name, int mode, uint test_if_locked) {
	DBUG_ENTER("ha_tnt::open");

	attachToTHD(ha_thd(), "ha_tnt::open");

	// TODO: 处理openTable找不到表失败，由于在线修改表模式过程中失败导致表
	int ret = 0;
	if (openTable(name, &ret)) {
		assert(!m_session);
		assert(!m_trans);

		m_session = createSessionAndAssignTrx("ha_tnt::open", m_conn, dummy_trx, 100);
		if (!m_session) {
			closeTable(m_share);
			ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", m_thd, false);
		} else {
			thr_lock_data_init(&m_share->m_lock, &m_mysqlLock, NULL);
			try {
				m_table->lockMeta(m_session, IL_S, 1000, __FILE__, __LINE__);
				// 真正修改读取的时候，肯定只有一个会话（线程在使用）
				bitmap_init(&m_readSet, m_readSetBuf, m_table->getNtseTable()->getTableDef()->m_numCols, FALSE);
				m_table->unlockMeta(m_session, IL_S);
				tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
				m_session = NULL;
			} catch (NtseException &e) {
				UNREFERENCED_PARAMETER(e);
				if (m_table->getMetaLock(m_session) != IL_NO) {
					m_table->unlockMeta(m_session, IL_S);
				}
				tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
				m_session = NULL;
				closeTable(m_share);
				ret = reportError(NTSE_EC_LOCK_TIMEOUT, "Lock time out", m_thd, false);
			}
		}
	}

	detachFromTHD();

	DBUG_RETURN(ret);
}

/**
 * 重置handler状态
 *
 * @return 总是返回0
 */
int ha_tnt::reset() {
	ntse_handler::reset();
	assert(!m_scan);
	//m_errno = 0;
	//m_errmsg[0] = '\0';
	//m_ignoreDup = false;
	//m_replace = false;
	m_iuSeq = NULL;
	//m_lastRow = INVALID_ROW_ID;
	//m_isRndScan = false;
	//m_lobCtx->reset();
	//bitmap_clear_all(&m_readSet);
	return 0;
}

/**
 * 关闭handler当前正在用的表。
 *
 * @return 成功返回0，失败返回1，不过TNT总是返回0
 */
int ha_tnt::close(void) {
	assert(!m_session && m_table);
	DBUG_ENTER("ha_tnt::close");
	ntse_handler::close();
	THD *thd = ha_thd();

	if (!thd) {
		// handler长时间未用过清理时没有THD
		assert(!m_thd && !m_conn);
		m_conn = tnt_db->getNtseDb()->getConnection(true, "ha_tnt::close");
	} else {
		attachToTHD(thd, "ha_tnt::close");
	}

	NTSE_ASSERT(closeTable(m_share) == 0);
	m_table = NULL;

	if (!thd) {
		tnt_db->getNtseDb()->freeConnection(m_conn);
		m_conn = NULL;
	} else {
		detachFromTHD();
	}

	DBUG_RETURN(0);
}

/**
 * 根据表名，查找对应的共享结构，其中包含表级锁信息。
 * 每个handler打开时，都会调用一次这个函数。
 * 为实现正确的表锁行为，系统维护一个已经打开的表对应的共享结构
 * 哈希表，一个表第一次被打开时创建一个新的共享结构，其它情况下
 * 重用这一结构并增加引用计数。
 *
 * 其实在TNT内部也维护了被打开的表，但由于表共享结构中
 * 包括了MySQL中特有的THR_LOCK等结构，需要在这里再维护一次。
 *
 * @param table_name 表名
 * @param eno OUT，失败时返回错误号
 * @post 如果打开成功，会设置ha_tnt的m_table和m_share两个结构
 * @return true表示打开成功，false表示打开失败
 */
bool ha_tnt::openTable(const char *table_name, int *eno) {
	struct TABLE *mysqlTable = table;

	LOCK(&openLock);

	if (!(m_share = openTables.get(table_name))) {
		assert(!m_session);
	
		m_session = createSessionAndAssignTrx("ha_tnt::open_table", m_conn, dummy_trx, 100);
		if (!m_session) {
			*eno = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", m_thd, false);
			goto error;
		}
		char *name_copy = System::strdup(table_name);
		normalizePathSeperator(name_copy);
		TNTTable *tntTable = NULL;
		try {
			tntTable = tnt_db->openTable(m_session, name_copy);
			tntTable->setMysqlTmpTable(table_share->get_table_ref_type() != TABLE_REF_BASE_TABLE);
			tntTable->lockMeta(m_session, IL_S, 1000, __FILE__, __LINE__);
			tntTable->refreshRows(m_session);
			tntTable->unlockMeta(m_session, IL_S);
		} catch (NtseException &e) {
			if (tntTable != NULL) {
				if (tntTable->getMetaLock(m_session) != IL_NO) {
					tntTable->unlockMeta(m_session, tntTable->getMetaLock(m_session));
				}
				tnt_db->closeTable(m_session, tntTable);
			}

			delete []name_copy;
			tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			*eno = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
			goto error;
		}
		delete []name_copy;

		m_share = new TNTShare(table_name, tntTable);
		m_table = m_share->m_table;
		
		/** 自增序列相关 */
		try {
			if (mysqlTable->found_next_number_field != NULL) {
				m_table->enterAutoincMutex();

				/** 若表被TNT内部应用打开，同时未初始化自增序列，则在此初始化 */
				if (m_table->getAutoinc() == 0) {
					m_table->lockMeta(m_session, IL_S, 1000, __FILE__, __LINE__);
					//TODO 暂不加表锁，不加表锁的后果是什么？？
					//m_table->lockTableOnStatStart(dummy_trx, TL_IS);
					m_table->updateAutoincIfGreater(initAutoinc(m_session));
					m_table->unlockMeta(m_session, IL_S);
				}

				m_table->exitAutoincMutex();
			}
		} catch (NtseException &e) {
			if (m_table->getMetaLock(m_session) != IL_NO)
				m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));
			tnt_db->closeTable(m_session, m_table);
			delete m_share;
			tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			*eno = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
			goto error;
		}

		openTables.put(m_share);
		tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}
	m_share->m_refCnt++;
	m_table = m_share->m_table;
	setNtseTable(m_table->getNtseTable());
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
int ha_tnt::closeTable(TNTShare *share) {
	
	LOCK(&openLock);
	if (!--share->m_refCnt) {
		assert(!m_session);
		m_session = createSessionAndAssignTrx("ha_tnt::closeTable", m_conn, dummy_trx);

		tnt_db->closeTable(m_session, share->m_table);

		tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
		m_session = NULL;
		openTables.remove(share->m_path);
		delete share;
	} 

	UNLOCK(&openLock);

	return 0;
}

/** 得到TNT内部表的表ID信息
 * @return 内部表表ID
 */
u16 ha_tnt::getTntTableId() {
	return m_table->getNtseTable()->getTableDef()->m_id;
}

/**	创建Session，并且将事务绑定到新建的Session上
*
*	@return	新建的Session对象
*/
Session* ha_tnt::createSessionAndAssignTrx(const char *name, Connection *conn, TNTTransaction *trx, int timeoutMs /* = -1 */) {
	Session *sess = tnt_db->getNtseDb()->getSessionManager()->allocSession(name, conn, timeoutMs);

	if (sess != NULL)
		sess->setTrans(trx);

	return sess;
}

/** 创建session
 * @param name session的名称
 * @param connection  连接对象
 * @param timeoutMs 超时时间
 * @return 新建Session对象
 */
Session* ha_tnt::createSession(const char *name, Connection *conn, int timeoutMs /*=-1*/) {
	return tnt_db->getNtseDb()->getSessionManager()->allocSession(name, conn, timeoutMs);
}


/**
 * 插入一行
 *
 * @param buf 插入行的内容
 * @return 成功返回0，由于唯一性冲突失败返回HA_ERR_FOUND_DUPP_KEY，
 *   由于记录超长失败返回相应错误码
 */
int ha_tnt::write_row(uchar *buf) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::write_row");
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::write_row(buf));

	int code = 0;
	if (!m_trans->isInnerTrx()) {
		if (System::fastTime() - m_trans->getBeginTime() > tnt_db->getTNTConfig()->m_maxTrxRunTime || m_trans->getHoldingLockCnt() > tnt_db->getTNTConfig()->m_maxTrxLocks) {
				char errMsg[100];
				if (m_trans->getHoldingLockCnt() > tnt_db->getTNTConfig()->m_maxTrxLocks) {
					System::snprintf_mine(errMsg, 100, "TNT Transaction can't hold over %d lock", tnt_db->getTNTConfig()->m_maxTrxLocks);
				} else {
					System::snprintf_mine(errMsg, 100, "TNT Transaction can't run over %d s time", tnt_db->getTNTConfig()->m_maxTrxRunTime);
				}
				code = reportError(NTSE_EC_TRX_ABORT, errMsg, m_thd, false);
				DBUG_RETURN(code);
		}
	}	

	assert(m_thd->lex->sql_command != SQLCOM_SELECT);
	ha_statistic_increment(&SSV::ha_write_count);

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
			if (m_table->insert(m_session, buf, &m_dupIdx, &m_opInfo, true) == INVALID_ROW_ID) {
				// Insert不加Next Key，因此不会产生加锁超时/死锁等情况
				code = HA_ERR_FOUND_DUPP_KEY;
			}
		} else {
			assert(m_ignoreDup);
			if (m_iuSeq) {
				// INSERT ... ON DUPLICATE KEY UPDATE在发生冲突后，若发现要UPDATE的新值与原记录相等
				// 则不会调用update_row，这样在INSERT ... SELECT ... ON DUPLICATE ...时会连续调用
				// write_row
				m_table->freeIUSequenceDirect(m_iuSeq);
				m_iuSeq = NULL;
			}
			m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
			m_iuSeq = m_table->insertForDupUpdate(m_session, buf, &m_opInfo);
			if (m_iuSeq)
				code = HA_ERR_FOUND_DUPP_KEY;
		}
	} catch (NtseException &e) {
		code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
	}

	// 设置新的自增值
	// TODO：可以参考InnoDB，区分Insert操作失败的情况，只有部分失败情况下，需要设置autoInc
	if (code == 0) {
		setAutoIncrIfNecessary(buf);
	}
	
	DBUG_RETURN(code);
}

/**
 * 更新一条记录。new_data为更新后的属性，但不包括auto_increment类型或自动更新的timestamp
 * 类型的属性值
 * 在调用rnd_next/rnd_pos/index_next之后，MySQL会立即调用这一函数更新满足条件的记录，
 * 因此TNT总是更新当前扫描的记录，不需要old_data的内容
 *
 * @param old_data 原记录内容，TNT不用
 * @param new_data 新记录内容
 * @return 成功返回0，唯一性索引冲突失败返回HA_ERR_FOUND_DUPP_KEY，记录超长或
 * 其它原因导致失败时返回相应错误码
 */
int ha_tnt::update_row(const uchar *old_data, uchar *new_data) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::update_row");
	ha_statistic_increment(&SSV::ha_update_count);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::update_row(old_data, new_data));

	int code = 0;
	try {
		if (m_scan) {

			// 重新设置update SubRecord，每次都对比old_data与new_data
			// 只有真正发生变化的列，才需要更新，而不是根据write_set无条件更新
			// 这么实现的优势：
			// 1. update t1 set id = 1 where id = 1; 不会报Unique 冲突
			// 2. 在scan中，由于TNT已经取出了完整的记录，因此可以逐个对比所有列
			u16 numUpdateCols;
			u16 *updateCols = transColsWithDiffVals(new_data, old_data, &numUpdateCols);

			if (numUpdateCols > 0) {
				if (!m_scan->isUpdateSubRecordSet()) {
					m_scan->setUpdateSubRecord(numUpdateCols, updateCols, true);
				} else {
					m_scan->setUpdateSubRecord(numUpdateCols, updateCols, false);
				}
				if (!m_table->updateCurrent(m_scan, new_data, old_data, &m_dupIdx, true)) {
					code = HA_ERR_FOUND_DUPP_KEY;
				}
			} else {
				code = HA_ERR_RECORD_IS_THE_SAME;
			}
		} else if (m_iuSeq) {
			u16 numUpdateCols;
			// TODO：此处不能使用table->write_set，因为针对insert on duplicate key update的语法
			// 此时的write_set为表的所有列，但是实际上，更新的列可能没有这么多，需要对比之后确定哪些列需要更新

			// u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			u16 *updateCols = transColsWithDiffVals(new_data, old_data, &numUpdateCols);

			bool succ = true;
			
			// 若经过对比，更新后项与前项完全一致，则直接返回成功，不需要进行实际更新
			if (numUpdateCols == 0) {
				m_table->freeIUSequenceDirect(m_iuSeq);
				code = HA_ERR_RECORD_IS_THE_SAME;
			} else {
				try {
					succ = m_table->updateDuplicate(m_iuSeq, new_data, numUpdateCols, updateCols, &m_dupIdx);
				} catch (NtseException &e) {
					m_iuSeq = NULL;
					m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
					throw e;
				}
			}
			
			m_iuSeq = NULL;
			m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
			if (!succ)
				code = HA_ERR_FOUND_DUPP_KEY;
		} else {
			// 1. 在以下用例中会在结束扫描之后再调用update_row
			// create table t1 (a int not null, b int, primary key (a)) engine=tnt;
			// create table t2 (a int not null, b int, primary key (a)) engine=tnt;
			// insert into t1 values (10, 20);
			// insert into t2 values (10, 20);
			// update t1, t2 set t1.b = 150, t2.b = t1.b where t2.a = t1.a and t1.a = 10;
			// 2. 行级复制作为SLAVE执行UPDATE事件时

			assert(m_lastRow != INVALID_ROW_ID);
			McSavepoint mcSave(m_session->getMemoryContext());

			u16 numReadCols;
			u16 *readCols = transCols(table->read_set, &numReadCols);
			
			m_scan = m_table->positionScan(m_session, OP_UPDATE, &m_opInfo, numReadCols, readCols, false);

			byte *buf = (byte *)alloca(m_table->getNtseTable()->getTableDef(true, m_session)->m_maxRecSize);
			try {
				NTSE_ASSERT(m_table->getNext(m_scan, buf, m_lastRow, false));
			} catch (NtseException &e) {
				// position scan 不加事务行锁，不抛错
				UNREFERENCED_PARAMETER(e);
				assert(false);
			}
			
			bool succ = true;

			u16 numUpdateCols = 0;
			u16 *updateCols = transColsWithDiffVals(old_data, new_data, &numUpdateCols);
			if (numUpdateCols > 0) {
				m_scan->setUpdateSubRecord(numUpdateCols, updateCols, true);
				try {
					succ = m_table->updateCurrent(m_scan, new_data, old_data, &m_dupIdx, true);
				} catch (NtseException &e) {
					endScan();
					throw e;
				}
			} else {
				code = HA_ERR_RECORD_IS_THE_SAME;
			}

			endScan();
			if (!succ)
				code = HA_ERR_FOUND_DUPP_KEY;
		}
	} catch (NtseException &e) {
		assert(m_thd == current_thd);
		code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
	}

	// 针对以下的特殊update操作，需要处理autoInc列：
	// insert into t (c1,c2) values (x, y) on duplicate key update ...
	// 若语句更新了autoInc列，则需要设置autoInc列的next values
	if (code == 0 
		&& thd_sql_command(m_thd) == SQLCOM_INSERT
		&& m_ignoreDup == true) {
			setAutoIncrIfNecessary(new_data);
	}

	DBUG_RETURN(code);
}

/**
 * 删除一条记录。
 * 在调用rnd_next/rnd_pos/index_next之后，MySQL会立即调用这一函数删除满足条件的记录，
 * 因此TNT总是更新当前扫描的记录，不需要buf的内容
 * 设置了insert_id时若REPLACE时发生冲突也会调用delete_row
 *
 * @param buf 当前记录内容，TNT不用
 * @return 总是返回0
 */

int ha_tnt::delete_row(const uchar *buf) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::delete_row");
	ha_statistic_increment(&SSV::ha_delete_count);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::delete_row(buf));
	int code = 0;
	
	if (m_scan) {
		/** 当表上不存在大对象时，TNT在读取记录时直接使用MySQL上层给定的空间，并不自己分配空间
		*	此时重新set一次，防止MySQL上层给定的buf空间发生变化，导致底层指向错误的地址
		*/
		/*
		if (!m_table->getNtseTable()->getTableDef()->hasLob()) {
			m_scan->setCurrentData((byte*)buf);
		}
		*/
		try {
			m_table->deleteCurrent(m_scan);
		} catch (NtseException &e) {
			code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		}
	}
	else if (m_iuSeq) {
		try {
			m_table->deleteDuplicate(m_iuSeq);
		} catch (NtseException &e) {
			code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		}
		m_iuSeq = NULL;
		m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
	} else {
		McSavepoint mcSave(m_session->getMemoryContext());

		u16 numReadCols;
		u16 *readCols = transCols(table->read_set, &numReadCols);

		m_scan = m_table->positionScan(m_session, OP_DELETE, &m_opInfo, numReadCols, readCols, false);

		byte *buf = (byte *)alloca(m_table->getNtseTable()->getTableDef(true, m_session)->m_maxRecSize);
		try {
			NTSE_ASSERT(m_table->getNext(m_scan, buf, m_lastRow, false));
		} catch(NtseException) {
			// position scan 不加事务行锁,不抛错
			assert(false);
		}
		
		try {
			m_table->deleteCurrent(m_scan);
		} catch (NtseException &e) {
			code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		}
		endScan();
	}

	DBUG_RETURN(code);
}

void ha_tnt::deleteAllRowsReal() throw(NtseException) {
	//truncate和delete from TABLE_NAME都有可能调用本函数，
	//而delete from TABLE_NAME不需要删除存在的压缩字典
	bool isTruncateOper = (m_thd->lex->sql_command == SQLCOM_TRUNCATE);
	tnt_db->truncateTable(m_session, m_table, isTruncateOper);
}

/**
 * 删除所有记录
 * 用于实现truncate
 * 5.1版本的旧接口delete_all_rows()不再使用,由于TNT不支持statement格式binlog，上层不会再调用此接口
 *
 * @return 成功返回0，失败返回错误码
 */
int ha_tnt::truncate() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ha_tnt::truncate");
	assert(m_session);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::truncate());

	// truncate操作之前，如果已经存在更新的活跃事务，则报错，回滚当前语句
	TNTTransaction *trx = m_session->getTrans();
	assert(trx != NULL && trx->getTrxState() == TRX_ACTIVE);
	if (!trx->isReadOnly()) {
		DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "Current connection has active write transaction, you must commit it", m_thd, false));
	}

	assert(m_table->getMetaLock(m_session) == IL_S);
	try {
		m_table->upgradeMetaLock(m_session, IL_S, IL_X, tnt_db->getNtseDb()->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		DBUG_RETURN(HA_ERR_LOCK_WAIT_TIMEOUT);
	}

	// 起内部事务，然后start，完成DDL操作
	trx = startInnerTransaction(TL_X);
	m_session->setTrans(trx);

	try {
		trx->lockTable(TL_X, m_table->getNtseTable()->getTableDef()->m_id);
		deleteAllRowsReal();
	} catch (NtseException &e) {
		// Truncate Table操作失败，回滚内部事务，并返回
		rollbackInnerTransaction(trx);
		int code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		DBUG_RETURN(code);
	}

	// 如果有自增字段的情况，自增值设为1
	reset_auto_increment_if_nozero(1);
	// 提交内部事务，并返回
	commitInnerTransaction(trx);

	DBUG_RETURN(0);
}


void ha_tnt::dropTableReal(const char *name) throw(NtseException) {
	char *name_copy = System::strdup(name);
	normalizePathSeperator(name_copy);
	try {
		tnt_db->dropTable(m_session, name_copy, ntse_table_lock_timeout);
	} catch (NtseException &e) {
		delete []name_copy;
		throw e;
	}
	delete []name_copy;
}
/**
 * 删除表
 * 在删除表之前，所有对表的引用都已经被关闭
 *
 * @param name 表文件路径，不含后缀名
 * @return 成功返回0，表不存在返回HA_ERR_NO_SUCH_TABLE，其它错误返回HA_ERR_GENERIC
 */
int ha_tnt::delete_table(const char *name) {
	ftrace(ts.mysql, tout << this << name);
	DBUG_ENTER("ha_tnt::delete_table");
	assert(!m_session);

	DBUG_SWITCH_NON_CHECK(ha_thd(), ntse_handler::delete_table(name));

	int code = 0;
	attachToTHD(ha_thd(), "ha_tnt::delete_table");
	m_session = createSession("ha_tnt::delete_table", m_conn, 100);
	if (!m_session) {
		code = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", m_thd, false);
		detachFromTHD();
		DBUG_RETURN(code);
	} 

	TNTTransaction *trx = startInnerTransaction(TL_X);
	m_session->setTrans(trx);

	try {
		dropTableReal(name);
	} catch (NtseException &e) {
		// rollback inner transaction
		rollbackInnerTransaction(trx);
		trx = NULL;
		code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
	}

	// 成功，则commit inner transaction
	if (trx != NULL) {
		commitInnerTransaction(trx);
	}

	tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_session = NULL;
	detachFromTHD();

	DBUG_RETURN(code);
}


void ha_tnt::renameTableReal(const char *oldPath, const char *newPath) throw(NtseException) {
	try {
		tnt_db->renameTable(m_session, oldPath, newPath, ntse_table_lock_timeout);
	} catch (NtseException &e){
		throw e;
	}
}

/**
 * 重命名表。调用此函数时handler对象为刚刚创建没有open的对象
 *
 * @param from 表文件原路径
 * @param to 表文件新路径
 */
int ha_tnt::rename_table(const char *from, const char *to) {
	ftrace(ts.mysql, tout << this << from << to);
	DBUG_ENTER("ha_tnt::rename_table ");
	assert(!m_session);

	DBUG_SWITCH_NON_CHECK(ha_thd(), ntse_handler::rename_table(from, to));

	int code = 0;
	attachToTHD(ha_thd(), "ha_tnt::rename_table");
	m_session = createSession("ha_tnt::rename_table", m_conn, 100);
	if (!m_session) {
		code = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", m_thd, false);
		detachFromTHD();
		DBUG_RETURN(code);
	} 
	char *from_copy = System::strdup(from);
	char *to_copy = System::strdup(to);
	normalizePathSeperator(from_copy);
	normalizePathSeperator(to_copy);
	
	TNTTransaction *trx = startInnerTransaction(TL_X);
	m_session->setTrans(trx);

	try {
		tnt_db->renameTable(m_session, from_copy, to_copy, ntse_table_lock_timeout);
	} catch (NtseException &e) {
		rollbackInnerTransaction(trx);
		trx = NULL;
		code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
	}
	delete []from_copy;
	delete []to_copy;
	
	// 成功，则commit inner transaction
	if (trx != NULL) {
		commitInnerTransaction(trx);
	}

	tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_session = NULL;
	detachFromTHD();

	DBUG_RETURN(code);
}

/** TNT非标准索引定义信息 */
struct IdxDefEx {
	const char	*m_name;		/** 索引名 */
	s8			m_splitFactor;	/** 分裂系数 */

	IdxDefEx(const char *name, s8 splitFactor) {
		m_name = System::strdup(name);
		m_splitFactor = splitFactor;
	}
};

/** TNT非标准表定义信息 */
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
int ha_tnt::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) {
	ftrace(ts.mysql, tout << this << name << table_arg << create_info);
	DBUG_ENTER("ha_tnt::create");
	char *name_copy = NULL, *schema_name = NULL, *table_name = NULL;
	Session *session = NULL;
	TNTTransaction *trx = NULL;
	char *comment = NULL;
	TblDefEx *extraArgs = NULL;
	TableDef *tableDef = NULL;
	int ret = 0;

	string tableName(name);
	string firstParName, lastParName;
	try {
		if (table_arg->s->fields > Limits::MAX_COL_NUM)
			NTSE_THROW(NTSE_EC_TOO_MANY_COLUMN, "Too Many Columns, Columns Limit is %d", Limits::MAX_COL_NUM);
	} catch (NtseException &e){
			DBUG_RETURN(reportError(e.getErrorCode(),e.getMessage(), m_thd, false));
	}
	

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
		ret = reportError(e.getErrorCode(), e.getMessage(), NULL, true);
	}

	try {
		if (create_info->auto_increment_value > 0)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Setting auto_increment variable is not supported");
	} catch (NtseException &e) {
		reportError(e.getErrorCode(), e.getMessage(), ha_thd(), false, true);
	}

	attachToTHD(ha_thd(), "ha_tnt::create");

	name_copy = System::strdup(name);
	normalizePathSeperator(name_copy);

	Table::getSchemaTableFromPath(name_copy, &schema_name, &table_name);

	TableDefBuilder tdb(0, schema_name, table_name);

	Database *ntse_db = tnt_db->getNtseDb();

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
				tdb.addColumnS(field->field_name, type, field->field_length, true, !notnull, getCollation(field->charset()));
			} else if (!isValueType) {
				tdb.addColumn(field->field_name, type, !notnull, getCollation(field->charset()));
			} else {
				tdb.addColumnN(field->field_name, type, prtype, !notnull);
			}
		}
		// 增加索引定义
		for (uint i = 0; i < table_arg->s->keys; i++) {
			KEY *key = table_arg->key_info + i;
			Array<u16> indexColumns;
			Array<u32> prefixLenArr;
			bool unique = key->flags & HA_NOSAME;
			bool primary = table_arg->s->primary_key == i;
			bool prefix = false;
			for (uint j = 0; j < key->key_parts; j++) {
				KEY_PART_INFO *key_part = key->key_part + j;
				u32 prefixLen = 0;
				if (isPrefix(table_arg, key_part)) {
					// 如果类型不支持前缀索引，抛错
					enum_field_types colType = key_part->field->real_type();
					if (colType != MYSQL_TYPE_VARCHAR &&
						colType != MYSQL_TYPE_VAR_STRING &&
						colType != MYSQL_TYPE_STRING &&
						colType != MYSQL_TYPE_MEDIUM_BLOB &&
						colType != MYSQL_TYPE_BLOB) {
							NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Type of column (%s) not support prefix index", key->name);
					}
					prefixLen = key_part->length;
					prefix = true;	
				}
				indexColumns.push(key->key_part[j].field->field_index);
				prefixLenArr.push(prefixLen);
			}
			tdb.addIndex(key->name, primary, unique, false, indexColumns, prefixLenArr);
		}
		tableDef = tdb.getTableDef();
		if (tableDef->m_maxMysqlRecSize != table_arg->s->reclength) {
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unexpected record size %d, expected %d",
				table_arg->s->reclength, tableDef->m_maxMysqlRecSize);
		}

		if (create_info->row_type != ROW_TYPE_DEFAULT) {
			if (tableDef->m_recFormat == REC_FIXLEN && create_info->row_type != ROW_TYPE_FIXED)
				DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "Row type of fixed length table should be ROW_TYPE_DEFAULT or ROW_TYPE_FIXED.", m_thd, false));
			else if (tableDef->m_recFormat == REC_VARLEN && create_info->row_type != ROW_TYPE_DYNAMIC)
				DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "Row type of variable length table should be ROW_TYPE_DEFAULT or ROW_TYPE_DYNAMIC.", m_thd, false));
		}

		//事务连接创建事务表，非事务连接创建非事务表
		if (THDVAR(m_thd, is_trx_connection)) {
			tableDef->setTableStatus(TS_TRX);
			session = createSession("ha_tnt::create", m_conn);
			// 开启内部事务
			trx = startInnerTransaction(TL_X);
			session->setTrans(trx);
		} else {
			tableDef->setTableStatus(TS_NON_TRX);
			//dummy_trx是一个假事务，压根就没start起来过，
			//session中绑定这个假事务，是为了避免在TNTTable加了ats或者ddl锁后，table层再加ats锁或者ddl锁，
			//因为table层加ddl锁是根据session是否绑定trx来判断的
			session = createSessionAndAssignTrx("ha_tnt::create", m_conn, dummy_trx);
		}
		
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
					} else {//可能原表不是使用TNT引擎，如果是找不到表则忽略这个异常
						break;
					}
				}
				delete [] schemaName;
				delete [] sourceTblName;
			} else {
				//解析非标准表定义参数
				extraArgs = parseCreateArgs(tableDef, getTntTHDInfo(ha_thd())->m_nextCreateArgs);
			}
			assert(extraArgs);
			applyCreateArgs(tableDef, extraArgs);
			delete extraArgs;
			extraArgs = NULL;
		} while (0);

		delete []comment;
		comment = NULL;

		if (tableDef->m_useMms && tableDef->m_cacheUpdate)
			reportError(NTSE_EC_GENERIC, "Mms cache update is disable in tnt", m_thd, false);

		tnt_db->createTable(session, name_copy, tableDef);

		delete tableDef;
		tableDef = NULL;
		if (tableName == lastParName) {
			//非分区表或最后一张分区表创建成功后，重置默认非标准表定义参数
			getTntTHDInfo(ha_thd())->resetNextCreateArgs();
		}
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
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
		getTntTHDInfo(ha_thd())->resetNextCreateArgs();

		// 回滚内部事务
		if (trx) {
			rollbackInnerTransaction(trx);
			trx = NULL;
			session->setTrans(NULL);
		}
	}

	// 提交内部事务
	if (trx) {
		commitInnerTransaction(trx);
		trx = NULL;
		session->setTrans(NULL);
	}

	if (session)
		ntse_db->getSessionManager()->freeSession(session);

	detachFromTHD();

	delete []name_copy;
	delete []schema_name;
	delete []table_name;
	DBUG_RETURN(ret);
}

/** 更新HA_CREATE_INFO信息
 * @param create_info	信息对象
 */
void ha_tnt::update_create_info( HA_CREATE_INFO *create_info ) {
	if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
		ha_tnt::info(HA_STATUS_AUTO);
		create_info->auto_increment_value = stats.auto_increment_value;
	}
}

/** 重设自增值为指定的值
 * @param value	指定的新自增值
 */
int ha_tnt::reset_auto_increment( ulonglong value ) {
	m_table->enterAutoincMutex();
	m_table->updateAutoincIfGreater(value);
	m_table->exitAutoincMutex();

	return 0;
}

int ha_tnt::reset_auto_increment_if_nozero(ulonglong value) {
	m_table->enterAutoincMutex();
	if (m_table->getAutoinc() != 0)
		m_table->initAutoinc(1);
	m_table->exitAutoincMutex();

	return 0;
}

void ha_tnt::prepareAddIndexReal() throw(NtseException) {
	//TODO 目前不支持inplace create unique index
	//加表锁，等待该表上所有的活跃事务提交
	/*m_session->getTrans()->lockTable(TL_S, m_table->getNtseTable()->getTableDef()->m_id);

	// 目前TNT只支持Inplace Add Index，创建索引的过程中，不允许更新操作
	// 需要等待内存被Purge空之后，才能进行以下操作，
	// 否则，创建Unique索引可能存在问题
	while (true) {
		if (m_table->getMRecSize() == 0) {
			break;
		}

		Thread::msleep(1000);
	}*/
}

void ha_tnt::addIndexReal(TABLE *table_arg, KEY *key_info, uint num_of_keys,
						  handler_add_index **add) throw(NtseException) {
	int i = 0;
	bool online = false;
	*add = new handler_add_index(table_arg, key_info, num_of_keys);

	Table *ntseTable = m_table->getNtseTable();
	TableStatus tblStatus = ntseTable->getTableDef()->getTableStatus();
	assert(tblStatus == TS_NON_TRX || tblStatus == TS_TRX);
	McSavepoint mc(m_session->getMemoryContext());
	IndexDef **indexDefs = (IndexDef **)m_session->getMemoryContext()->alloc(sizeof(IndexDef *) * num_of_keys);
	int ret = 0;
	try {
		if (ntseTable->getTableDef(true, m_session)->m_indexOnly) {
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

			// 检测即将添加的索引是否存在重名, 参考Innodb的实现
			// 检测要添加的索引不存在同名，目前TNT/NTSE一次只能添加一个索引，因此不会进循环
			for(uint j = 0; (int)j < i; j++) {
				KEY *key2 = key_info + j;
				if (0 == my_strcasecmp(system_charset_info, key->name, key2->name)) {
					NTSE_THROW(NTSE_EC_DUPINDEX, "Incorrect index name %s", key->name);
				}
			}

			// 检测即将添加的索引和已存在的索引是否重名，参考Innodb的实现
			// 详见JIRA：NTSETNT-299
			// Mysql-5.6.13中已不需要在引擎层检测
			TableDef *tableDef = m_table->getNtseTable()->getTableDef();
			for (uint j = 0; j < tableDef->m_numIndice; j++) {
				IndexDef *indexDef = tableDef->getIndexDef(j);
				if (!indexDef->m_online && 0 == my_strcasecmp(system_charset_info, key->name, indexDef->m_name)){
					NTSE_THROW(NTSE_EC_DUPINDEX, "Incorrect index name %s", key->name);
				}
			}

			// 这样来决定是否是主键是太恶心了，不过看InnoDB plugin就是这么实现的
			bool primaryKey = !my_strcasecmp(system_charset_info, key->name, "PRIMARY");
			// 由于MySQL认为主键总是第一个索引，则NTSE目前没有这么处理，因此目前强制要求主键只能作为
			// 第一个索引
			if (primaryKey && ntseTable->getTableDef(true, m_session)->m_numIndice)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Primary key must be the first index.");

			// 由于MySQL认为非唯一性索引总是在唯一性索引之后，TNT目前没有这么处理，为保证正确性，要求唯一性索引
			// 必须在非唯一性索引之前创建
			if (unique && ntseTable->getTableDef(true, m_session)->getNumIndice(false) > 0)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unique index must be created before non-unique index.");

			ColumnDef **columns = new (m_session->getMemoryContext()->alloc(sizeof(ColumnDef *) * key->key_parts)) ColumnDef *[key->key_parts];
			u32 *prefixLens = new (m_session->getMemoryContext()->alloc(sizeof(u32) * key->key_parts)) u32[key->key_parts];
			for (uint j = 0; j < key->key_parts; j++) {
				if (isPrefix(table_arg, key->key_part + j)) {
					prefixLens[j] = key->key_part[j].length;
				} else {
					prefixLens[j] = 0;
				}
				columns[j] = ntseTable->getTableDef(true, m_session)->m_columns[key->key_part[j].field->field_index];
			}

			indexDefs[i] = new IndexDef(key->name, key->key_parts, columns, prefixLens, unique, primaryKey, online);
		}

		if (THDVAR(m_thd, is_trx_connection)) {
			prepareAddIndexReal();
		} else {
			ntse_handler::prepareAddIndexReal();
		}
		tnt_db->addIndex(m_session, m_table, (u16)num_of_keys, (const IndexDef **)indexDefs);
		if (online) {
			for (uint j = 0; j < num_of_keys; j++) {
				indexDefs[j]->m_online = false;
			}
			tnt_db->addIndex(m_session, m_table, (u16)num_of_keys, (const IndexDef **)indexDefs);
		}
	} catch (NtseException &e) {
		for (i--; i >= 0; i--) {
			delete indexDefs[i];
		}

		throw e;
	}

	for (i--; i >= 0; i--) {
		delete indexDefs[i];
	}
}

/**
 * 在线建索引
 *
 * @param table_arg 表定义
 * @param key_info 索引定义数组
 * @param num_of_keys 待建的索引个数
 * @return 成功返回0，失败返回错误码
 */
int ha_tnt::add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys,
					  handler_add_index **add) {
	ftrace(ts.mysql, tout << this << table_arg << key_info << num_of_keys);
	DBUG_ENTER("ha_tnt::add_index");
	assert(m_conn && m_session);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::add_index(table_arg, key_info, num_of_keys, add));

	TNTTransaction *parentTrx = m_session->getTrans();
	TNTTransaction *trx = startInnerTransaction(TL_X);
	m_session->setTrans(trx);
	int ret = 0;
	try {
		addIndexReal(table_arg, key_info, num_of_keys, add);
	} catch (NtseException &e) {
		if (e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION)
			m_dupIdx = MAX_KEY;
		ret = reportError(e.getErrorCode(), e.getMessage(), m_thd, false, e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION);

		if (trx) {
			rollbackInnerTransaction(trx);
			trx = NULL;
		}
	}

	if (trx) {
		commitInnerTransaction(trx);
		trx = NULL;
	}

	m_session->setTrans(parentTrx);
		
	DBUG_RETURN(ret);
}


/**
 * 完成创建索引操作步骤，或提交完成或回滚之前已创建的索引
 *
 * @param  add 由add_index操作创建的新建索引上下文对象，主要用于回滚
 * @param  commit true代表commit, false代表需要rollback
 * @return 成功返回0，失败返回错误码
 */
void ha_tnt::finalAddIndexReal(handler_add_index *add, bool commit) throw(NtseException) {
	if (!commit) {
		// 如果失败，需要回滚这次加索引的操作
		assert(add);
		KEY *key_info = add->key_info;
		uint num_of_keys = add->num_of_keys;
	
		uint numIndice = m_table->getNtseTable()->getTableDef()->m_numIndice;
		try {
			for (uint i = 0; i < num_of_keys; i++) {
				KEY *key = key_info + i;
				if (numIndice > 0)
					for (uint j = 0; j < numIndice; j++)
						if (!System::stricmp(key->name, m_table->getNtseTable()->getTableDef()->getIndexDef(j)->m_name))
							tnt_db->dropIndex(m_session, m_table, j);
			}
		}catch (NtseException &e) {
			throw e;
		}
	}
}


/**
 * 完成创建索引操作步骤，或提交完成或回滚之前已创建的索引
 *
 * @param  add 由add_index操作创建的新建索引上下文对象，主要用于回滚
 * @param  commit true代表commit, false代表需要rollback
 * @return 总是返回 0
 */
int ha_tnt::final_add_index(handler_add_index *add, bool commit) {
	assert(m_conn && m_session);
	DBUG_ENTER("ha_tnt::final_add_index");
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::final_add_index(add, commit));
	TNTTransaction *parentTrx = m_session->getTrans();
	TNTTransaction *trx = startInnerTransaction(TL_X);
	m_session->setTrans(trx);
	int ret = 0;
	try {
		finalAddIndexReal(add, commit);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		if (trx) {
			rollbackInnerTransaction(trx);
			trx = NULL;
		}
	}
	if (trx) {
		commitInnerTransaction(trx);
		trx = NULL;
	}
	m_session->setTrans(parentTrx);
	delete add;
	DBUG_RETURN(ret);
}


void ha_tnt::dropIndexReal(TABLE *table_arg, uint *key_num, uint num_of_keys) throw(NtseException) {
	try {
		if (num_of_keys > 1)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can't drop multiple indice in one statement.");
		if (m_table->getNtseTable()->getTableDef(true, m_session)->m_indexOnly)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not drop index of index only table.");

		tnt_db->dropIndex(m_session, m_table, key_num[0]);
	} catch (NtseException &e) {
		throw e;
	}
}

/**
 * 准备删除索引，在TNT中其实已经把索引给删了
 *
 * @param table_arg 表定义
 * @param key_num 要删除的索引序号数组，经验证一定是顺序排好序的
 * @param num_of_keys 要删除的索引个数
 */
int ha_tnt::prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys) {
	ftrace(ts.mysql, tout << this << table_arg << key_num << num_of_keys);
	DBUG_ENTER("ha_tnt::prepare_drop_index");
	assert(m_conn && m_session);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::prepare_drop_index(table_arg, key_num, num_of_keys));

	TNTTransaction *parentTrx = m_session->getTrans();
	TNTTransaction *trx = startInnerTransaction(TL_X);
	m_session->setTrans(trx);

	int ret = 0;
	try {
		dropIndexReal(table_arg, key_num, num_of_keys);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		if (trx) {
			rollbackInnerTransaction(trx);
			trx = NULL;
		}
	}

	if (trx) {
		commitInnerTransaction(trx);
		trx = NULL;
	}

	m_session->setTrans(parentTrx);

	DBUG_RETURN(ret);
}

/**
 * 完成删除索引操作，在TNT中什么也不干
 *
 * @param table_arg 表定义
 * @return 总是返回0
 */
int ha_tnt::final_drop_index(TABLE *table_arg) {
	ftrace(ts.mysql, tout << this << table_arg);

	return 0;
}

/** 更新统计信息
 *
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁时返回HA_ADMIN_FAILED
 */
int ha_tnt::analyze(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_tnt::analyze");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	Table *ntseTable = m_table->getNtseTable();

	// TODO：暂时TNT只实现了NTSE的统计信息收集
	ntseTable->getHeap()->updateExtendStatus(m_session, ntse_sample_max_pages);
	for (u16 i = 0; i < ntseTable->getTableDef(true, m_session)->m_numIndice; i++) {
		ntseTable->getIndice()->getIndex(i)->updateExtendStatus(m_session, ntse_sample_max_pages);
	}
	if (ntseTable->getLobStorage())
		ntseTable->getLobStorage()->updateExtendStatus(m_session, ntse_sample_max_pages);

	DBUG_RETURN(HA_ADMIN_OK);
}

void ha_tnt::optimizeTableReal(bool keepDict, bool waitForFinish) throw(NtseException) {
	tnt_db->optimizeTable(m_session, m_table, keepDict, waitForFinish);
}

/** 优化表数据
 *
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁或失败时返回HA_ADMIN_FAILED
 */
int ha_tnt::optimize(THD *thd, HA_CHECK_OPT *check_opt) {
	DBUG_ENTER("ha_tnt::optimize");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::optimize(thd, check_opt));

	//DBUG_RETURN(HA_ADMIN_OK);

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	//检查连接私有属性ntse_keep_compress_dictionary_on_optimize
	// TODO：optimize方法，TNTDatabase模块目前并未实现
	bool keepDict = THDVAR(m_thd, keep_dict_on_optimize);

	m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

	int ret = 0;
	try {
		optimizeTableReal(keepDict, false);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), thd, false);
	}

	DBUG_RETURN(!ret ? HA_ADMIN_OK: HA_ADMIN_FAILED);
}

/** 检查表数据一致性
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁或失败时返回HA_ADMIN_FAILED，数据不一致返回HA_ADMIN_CORRUPT
 */
int ha_tnt::check(THD* thd, HA_CHECK_OPT* check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_tnt::check");
	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::check(thd, check_opt));

	m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

	int ret = 0;
	try {
		// TODO：verify方法，目前TNTTable并未实现
		m_table->verify(m_session);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), thd, false);
	}
	DBUG_RETURN(!ret ? HA_ADMIN_OK: HA_ADMIN_CORRUPT);
}

/** 修正表数据中的不一致
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁或失败时返回HA_ADMIN_FAILED
 */
int ha_tnt::repair(THD* thd, HA_CHECK_OPT* check_opt) {
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_tnt::repair");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::repair(thd, check_opt));

	// OPTIMIZE即可完成重建索引，修正堆与索引的不一致
	int ret = optimize(thd, check_opt);
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
pair<TblDefEx*, TableDef*>* ha_tnt::queryTableDef(Session *session, const char *sourceTblPath, 
												   bool *hasDict /*=NULL*/) throw(NtseException) {
	assert(session && sourceTblPath);
	Table *tableInfo	= NULL;
	TblDefEx *tblDefEx	= NULL;
	TNTTable *tntTable	= NULL;

	try {
		tntTable = tnt_db->openTable(session, sourceTblPath);
		tntTable->lockMeta(session, IL_S, 0, __FILE__, __LINE__);

		tableInfo = tntTable->getNtseTable();
	
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
		if (tntTable) {
			if (tntTable->getMetaLock(session) == IL_S) {
				tntTable->unlockMeta(session, IL_S);
			}
			tnt_db->closeTable(session, tntTable);
		}
		throw e;
	}
	TableDef *oldTblDefCopy = new TableDef(tableInfo->getTableDef());
	tntTable->unlockMeta(session, IL_S);
	tnt_db->closeTable(session, tntTable);

	pair<TblDefEx*, TableDef*> *r = new pair<TblDefEx*, TableDef*>(tblDefEx, oldTblDefCopy);
	return r;
}

/** 解析CREATE TABLE非标准信息
 * @param tableDef 表定义
 * @param str 字符串表示的非标准信息，可能为NULL
 * @return 若指定了非标准信息则返回解析结果，否则返回默认值
 * @throw NtseException 格式错误
 */
TblDefEx* ha_tnt::parseCreateArgs(TableDef *tableDef, const char *str) throw(NtseException) {
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
				}/* else if (!strcmp(name, "cache_update")) {
					args->m_cacheUpdate = Parser::parseBool(value);
				} else if (!strcmp(name, "update_cache_time")) {
					args->m_updateCacheTime = Parser::parseInt(value, TableDef::MIN_UPDATE_CACHE_TIME, TableDef::MAX_UPDATE_CACHE_TIME);
				} else if (!strcmp(name, "cached_columns")) {
					if (args->m_cachedCols)
						NTSE_THROW(NTSE_EC_GENERIC, "Can not set cached_columns multiple times.");
					args->m_cachedCols = Parser::parseList(value, ',');
				} */else if (!strcmp(name, "column_groups")) {
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
void ha_tnt::fixedTblDefEx(const TableDef *newTableDef, const TableDef *oldTableDef, TblDefEx *args) {
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
void ha_tnt::applyCreateArgs(TableDef *tableDef, const TblDefEx *args) throw(NtseException) {
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
/*bool ha_tnt::isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part) {
	// 本来应该通过HA_PART_KEY_SEG标志来判断是否是为前缀，但MySQL并没有正确的设置这个值，
	// 这里参考InnoDB中的判断逻辑
	Field *field = table_arg->field[key_part->field->field_index];
	assert(!strcmp(field->field_name, key_part->field->field_name));
	if ((key_part->length < field->pack_length() && field->type() != MYSQL_TYPE_VARCHAR)
		|| (field->type() == MYSQL_TYPE_VARCHAR && key_part->length < field->pack_length() - ((Field_varstring*)field)->length_bytes))
		return true;
	return false;
}*/

/**
* 开始一个内部事务
* @param lockMode 加锁模式
*/
TNTTransaction* ha_tnt::startInnerTransaction(TLockMode lockMode) {
	TNTTrxSys *trxsys = tnt_db->getTransSys();

	TNTTransaction *trx = NULL;
	try {
		trx = trxsys->allocTrx();
	} catch (NtseException &e) {
		// 当前系统中有过多的事务，目前处理为直接退出进程
		reportError(e.getErrorCode(), e.getMessage(), m_thd, true);
		return NULL;
	}
	trx->startTrxIfNotStarted(m_conn, true);
	
	trx->setThd(m_thd);

	return trx;
}

/**
* 提交一个内部事务
* @param 待提交事务
*/
void ha_tnt::commitInnerTransaction(TNTTransaction *trx) {
	TNTTrxSys *trxsys = tnt_db->getTransSys();

	trx->commitTrx(CS_INNER);

	trxsys->freeTrx(trx);
}

/**
* 回滚一个内部事务
* @param 待回滚事务
*/
void ha_tnt::rollbackInnerTransaction(TNTTransaction *trx) {
	TNTTrxSys *trxsys = tnt_db->getTransSys();

	trx->rollbackTrx(RBS_INNER);

	trxsys->freeTrx(trx);
}


/** 若正在进行扫描则结束之
 * @post 若正在进行扫描，则底层的扫描已经结束，会话的MemoryContext被重置
 *   到开始扫描前的状态
 */
void ha_tnt::endScan() {
	assert(m_session);
	m_isRndScan = false;
	if (m_scan) {
		m_table->endScan(m_scan);
		m_scan = NULL;
		m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
	}
	bitmap_clear_all(&m_readSet);
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
int ha_tnt::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::index_read_map");
	ha_statistic_increment(&SSV::ha_read_key_count);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_read_map(buf, key, keypart_map, find_flag));

	if (m_iuSeq) { // 分区表INSERT FOR DUPLICATE UPDATE时，使用index_read_map而不是rnd_pos取记录
		NTSE_ASSERT(find_flag == HA_READ_KEY_EXACT);
		NTSE_ASSERT(active_index == m_iuSeq->getDupIndex());
		transKey(active_index, key, keypart_map, &m_indexKey);
		NTSE_ASSERT(recordHasSameKey(m_table->getNtseTable()->getTableDef(true, m_session)->m_indice[active_index], m_iuSeq->getScanHandle()->getIdxKey(), &m_indexKey));
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
	/** TODO: 以下这三种情况TNT并不真正支持，TNT不会丢失结果，但可能会返回
	 * 过多的结果，经调试发现MySQL上层会把TNT返回的错误结果过滤掉
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
int ha_tnt::index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map) {
	DBUG_ENTER("ha_tnt::index_read_last_map");
	ha_statistic_increment(&SSV::ha_read_last_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_read_last_map(buf, key, keypart_map));
	DBUG_RETURN(indexRead(buf, key, keypart_map, false, true, false));
}

/**
 * 索引正向全扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_tnt::index_first(uchar *buf) {
	DBUG_ENTER("ha_tnt::index_first");
	ha_statistic_increment(&SSV::ha_read_first_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_first(buf));
	DBUG_RETURN(indexRead(buf, NULL, 0, true, true, false));
}

/**
 * 索引反向全扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */

int ha_tnt::index_last(uchar *buf) {
	DBUG_ENTER("ha_tnt::index_last");
	ha_statistic_increment(&SSV::ha_read_last_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_last(buf));
	DBUG_RETURN(indexRead(buf, NULL, 0, false, true, false));
}

/**
 * 读取下一条记录
 *
 * @param buf 用于存储输入记录内容
 * @return 成功返回0，没有记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_tnt::index_next(uchar *buf) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::index_next");
	ha_statistic_increment(&SSV::ha_read_next_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_next(buf));

	assert(m_scan && m_scan->getType() == ST_IDX_SCAN);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	DBUG_RETURN(fetchNext(buf));
}

/**
 * 读取前一条记录
 *
 * @param buf 用于存储输入记录内容
 * @return 成功返回0，没有记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_tnt::index_prev(uchar *buf) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::index_prev");
	ha_statistic_increment(&SSV::ha_read_prev_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_prev(buf));

	assert(m_scan && m_scan->getType() == ST_IDX_SCAN);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	DBUG_RETURN(fetchNext(buf));
}

/**
 * 结束索引扫描，释放扫描句柄及资源
 *
 * @return 总是返回0
 */

int ha_tnt::index_end() {
	DBUG_ENTER("ha_tnt::index_end");
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_end());

	if (m_session)
		endScan();
	active_index = MAX_KEY;
	DBUG_RETURN(0);
}

/**
 * 初始化全表扫描
 *
 * @param scan 为true时才需要处理
 * @return 一定会成功返回0
 */
int ha_tnt::rnd_init(bool scan) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::rnd_init");
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::rnd_init(scan));

	endScan();	// 作为子查询时可能会扫描多遍

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
	m_isRndScan = scan;
	bitmap_copy(&m_readSet, table->read_set);

	DBUG_RETURN(0);
}

/** 判断指定的搜索方式是否包含键值本身
 * @param flag 搜索方式
 * @param lowerBound true表示是扫描范围的下界，false表示是上界
 * @return 是否包含键值本身
 */
/*bool ha_tnt::isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound) {
	switch (flag) {
	// 这些条件，都会包含Equal
	case HA_READ_KEY_EXACT:
	case HA_READ_KEY_OR_NEXT:
	case HA_READ_KEY_OR_PREV:
	case HA_READ_PREFIX:
	case HA_READ_PREFIX_LAST:
	case HA_READ_PREFIX_LAST_OR_PREV:
		return true;
	// 以下两个条件，不包含Equal
	case HA_READ_AFTER_KEY:
		return !lowerBound;
	case HA_READ_BEFORE_KEY:
		return lowerBound;
	default:
		return false;
	};
}*/

/**
 * 估计指定索引中在[min_key, max_key]之间的记录数
 *
 * @param inx 第几个索引
 * @param min_key 下限，可能为NULL
 * @param max_key 上限，可能为NULL
 */
ha_rows ha_tnt::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
	ftrace(ts.mysql, tout << this << inx << min_key << max_key);
	DBUG_ENTER("ha_tnt::records_in_range");
	// TODO：暂时直接使用NTSE的方法，后续改进
	DBUG_RETURN(ntse_handler::records_in_range(inx, min_key, max_key));

	/*SubRecord minSr, maxSr;
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

	u64 rows = m_table->getNtseTable()->getIndice()->getIndex(inx)->recordsInRangeSecond(m_session,
		pMinSr, includeMin, pMaxSr, includeMax);*/

	/*
	// 测试代码
	if ((rows > 100) && (strcmp(m_table->getNtseTable()->getTableDef()->m_name, "order_line") == 0) && (m_opInfo.m_selLockType == TL_X)) {
		FILE *file;

		file = fopen("records_in_range.txt", "a+");

		fprintf(file, "rows = %d\n", rows);
		fprintf(file, "IncludeMin = %d SubRec Size = %d\n", includeMin, minSr.m_size);
		fprintf(file, "SubRec Data = ");
		for (uint i = 0; i < minSr.m_size; i++) {
			fprintf(file, "%x", minSr.m_data[i]);
		}
		fprintf(file, "\n");
		

		fprintf(file, "IncludeMax = %d SubRec Size = %d\n", includeMax, maxSr.m_size);
		fprintf(file, "SubRec Data = ");
		for (uint i = 0; i < maxSr.m_size; i++) {
			fprintf(file, "%x", maxSr.m_data[i]);
		}
		fprintf(file, "\n");

		fclose(file);

	}
	*/

	//DBUG_RETURN(rows);
}

/**
 * 结束全表扫描
 *
 * @return 返回0
 */
int ha_tnt::rnd_end() {
	DBUG_ENTER("ha_tnt::rnd_end");
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::rnd_end());
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
int ha_tnt::rnd_next(uchar *buf) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::rnd_next");
	ha_statistic_increment(&SSV::ha_read_rnd_next_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::rnd_next(buf));

	if (m_table->getNtseTable()->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do table scan on index only table", m_thd, false);
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
 * 根据记录位置读取记录
 *
 * @param buf 存储记录输出内容
 * @param pos 记录位置
 * @return 总是返回0
 */
int ha_tnt::rnd_pos(uchar *buf, uchar *pos) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::rnd_pos");
	ha_statistic_increment(&SSV::ha_read_rnd_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::rnd_pos(buf, pos));

	if (m_table->getNtseTable()->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do positional scan on index only table", m_thd, false);
		DBUG_RETURN(code);
	}

	if (!m_iuSeq) {
		// 为了能正确的得到read_set的信息,在这里才初始化扫描句柄
		if (!m_scan) {
			initReadSet(false, true);
			beginTblScan();
		} else {
			// 对于update/delete操作，一定读取整行，不根据read_set读取记录
			if (m_opInfo.m_selLockType != TL_X)
				assert(bitmap_is_subset(table->read_set, &m_readSet));
		}
		// 读取RowId
		RowId rid = RID_READ(pos);
		try {
			NTSE_ASSERT(m_table->getNext(m_scan, buf, rid, true));
		}catch (NtseException &e) {
			// position scan 不加事务行锁，不抛错
			UNREFERENCED_PARAMETER(e);
			assert(false);
		}
		
	} else {
		assert(RID_READ(pos) == m_iuSeq->getScanHandle()->getCurrentRid());
		m_iuSeq->getDupRow(buf);
	}
	table->status = 0;

	DBUG_RETURN(0);
}

/**
 * 检测Mysql是两趟FileSort算法： 即先取排序和条件属性，再取结果集所需其它属性
 * 若结果集所有列长度之和大于max_length_for_sort_data， 或者结果集包含大对象时，Mysql使用两趟算法（参见QA89930）
 * 
 * @return true表示MySQL使用了两趟FileSort算法
 */
/*bool ha_tnt::is_twophase_filesort() {
	bool twophase = true;
	if (m_thd != NULL && m_thd->lex != NULL) {
		MY_BITMAP tmp_set;
		uint32 tmp_set_buf[Limits::MAX_COL_NUM / 8 + 1];	//位图所需要的缓存
		bitmap_init(&tmp_set, tmp_set_buf, m_table->getNtseTable()->getTableDef()->m_numCols, FALSE);
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
}*/

/**
 * 记录当前记录的RowID存到ref中，对于TNT来说即为RID。本函数在非流水化更新时调用
 * 以记录将来要更新的记录的位置，若在Filesort时调用
 *
 * @param record 没什么用
 */
void ha_tnt::position(const uchar *record) {
	assert(m_session);
	assert(m_lastRow != INVALID_ROW_ID);
	DBUG_ENTER("ha_tnt::position");
	if (m_deferred_read_cache_size != 0) {
		m_deferred_read_cache_size = THDVAR(ha_thd(), deferred_read_cache_size);
	}
	DBUG_SWITCH_NON_RETURN(m_thd, ntse_handler::position(record));

	RID_WRITE(m_lastRow, ref);

	DBUG_VOID_RETURN;
}

/**
 * 初始化全表扫描句柄
 * @pre m_readSet应该要设置正确
 */
void ha_tnt::beginTblScan() {
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols);

	OpType opType = getOpType();
	try {
		if (m_isRndScan) {
			/** NTSE支持非精确表扫描，能提高表扫描性能；TNT暂时禁用此参数 */
			// m_conn->getLocalConfig()->m_accurateTblScan = THDVAR(ha_thd(), accurate_tblscan);
			m_scan = m_table->tableScan(m_session, opType, &m_opInfo, numReadCols, readCols, false, m_lobCtx);
		} else {
			m_scan = m_table->positionScan(m_session, opType, &m_opInfo, numReadCols, readCols, false, m_lobCtx);
		}
	} catch(NtseException &) {
		NTSE_ASSERT(false);		// 不加表锁时不可能会失败
	}
}

/** 表扫描或索引扫描时获取下一条记录（索引反向扫描时实际上是前一个记录）
 * @post m_lastRow中记录了刚获取成功的记录的RID
 *
 * @param buf OUT，存储获取的记录内容
 * @return 成功返回0，失败返回错误码
 */
int ha_tnt::fetchNext(uchar *buf) {

	bool hasNext = false;
	int	 ret = 0;

	table->status = STATUS_NOT_FOUND;

	m_lobCtx->reset();

	/** getNext函数有两类判断逻辑
	 ** 逻辑一：根据getNext函数的返回值，判断是否读到表的结束位置
	 ** 逻辑二：根据getNext函数的throw Exception，判断出错类型，并作相应处理
	*/
	try {
		if (!m_trans->isInnerTrx()) {
			if (System::fastTime() - m_trans->getBeginTime() > tnt_db->getTNTConfig()->m_maxTrxRunTime){
				NTSE_THROW(NTSE_EC_TRX_ABORT, "TNT Transaction can't run over %d s time", tnt_db->getTNTConfig()->m_maxTrxRunTime);
			} else if (m_trans->getHoldingLockCnt() > tnt_db->getTNTConfig()->m_maxTrxLocks) {
				NTSE_THROW(NTSE_EC_TRX_ABORT, "TNT Transaction can't hold over %d lock", tnt_db->getTNTConfig()->m_maxTrxLocks);
			}
		}
		hasNext = m_table->getNext(m_scan, buf, INVALID_ROW_ID, true);
		if (!hasNext) {
			ret = HA_ERR_END_OF_FILE;
		}
		else {
			if (m_scan->getType() == ST_IDX_SCAN
				&& m_checkSameKeyAfterIndexScan
				&& !recordHasSameKey(m_table->getNtseTable()->getTableDef(true, m_session)->m_indice[active_index], m_scan->getIdxKey(), &m_indexKey)) {
					ret = HA_ERR_END_OF_FILE;
			}
		}	
	} catch (NtseException &e) {
		// getNext函数抛出异常，可能存在的异常包括：死锁/加锁超时/锁表空间不足等
		// 需要根据不同的异常类型，判断直接返回，或者是回滚整个事务

		assert(m_thd == current_thd);
		ret = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
	}
	
	// 若存在NextKey，则读取
	if (ret == 0) {
		m_lastRow = m_scan->getCurrentRid();
		table->status = 0;
	}

	return ret;
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
int ha_tnt::indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact) {
	m_lobCtx->reset();
	endScan();	// 多区域范围查询时可能会扫描多次，不调用index_end

	int code = 0;

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();

	initReadSet(true);

	bool singleFetch;
	u16 numReadCols;

	// NTSETNT-285，TNT/NTSE的select count（*）语句没有走上全表扫描
	u16 *readCols = transCols(&m_readSet, &numReadCols, true, active_index);

	bool nullIncluded = transKey(active_index, key, keypart_map, &m_indexKey);

	// 如果等值查询的键值中存在NULL列，那么一定不是Single Fetch
	if (exact && !nullIncluded
		&& m_table->getNtseTable()->getTableDef(true, m_session)->m_indice[active_index]->m_unique
		&& m_indexKey.m_numCols == m_table->getNtseTable()->getTableDef(true, m_session)->m_indice[active_index]->m_numCols)
		singleFetch = true;
	else
		singleFetch = false;

	OpType opType = getOpType();
	try {
		// 由于TNTIndex一定需要传入一个SubRecord，因此无论是否指定IndexKey，都将m_indexKey传入
		// IndexScanCond cond(active_index, m_indexKey.m_numCols? &m_indexKey: NULL, forward, includeKey, singleFetch);
		// IndexScanCond cond(active_index, &m_indexKey, forward, includeKey, singleFetch);

		IndexScanCond cond(active_index, m_indexKey.m_numCols? &m_indexKey: NULL, forward, includeKey, singleFetch);
		m_scan = m_table->indexScan(m_session, opType, &m_opInfo, &cond, numReadCols, readCols, singleFetch, false, m_lobCtx);
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

/** 得到操作类型
 * @param return 操作类型
 */
OpType ha_tnt::getOpType() {
	//非事务连接无论操作事务表还是非事务表都是走ntse流程
	if (!THDVAR(m_thd, is_trx_connection)) {
		return ntse_handler::getOpType();
	}

	OpType ret;
	// 快照读不加锁；lock in share mode加S锁；都是读操作
	if (m_opInfo.m_selLockType == TL_NO || m_opInfo.m_selLockType == TL_S) {
		ret = OP_READ;
	}
	// 当前读，U/I/D加X锁；写操作
	else {
		assert((m_opInfo.m_selLockType == TL_X));
		ret = OP_WRITE;
	}

	return ret;
}

/** 比较记录的键值与指定键值是否相等
 *
 * @param buf 记录
 * @param indexDef 索引定义
 * @param key KEY_PAD格式的键值
 * @return 是否相等
 */
/*bool ha_tnt::recordHasSameKey(const byte *buf, const IndexDef *indexDef, const SubRecord *key) {
	assert(key->m_format == KEY_PAD);
	SubRecord key2(KEY_PAD, key->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->alloc(indexDef->m_maxKeySize),
		indexDef->m_maxKeySize);
	TableDef *tableDef = m_table->getNtseTable()->getTableDef(true, m_session);
	Record rec(INVALID_ROW_ID, REC_REDUNDANT, (byte *)buf, tableDef->m_maxRecSize);
	RecordOper::extractKeyRP(tableDef, &rec, &key2);
	return key2.m_size == key->m_size && !RecordOper::compareKeyPP(tableDef, &key2, key, indexDef);
}*/

/** 用于各种扫描之前初始化m_readSet的信息
 * @param idxScan	true表示索引扫描，false表示堆扫描
 * @param posScan	true表示是rnd_pos扫描，false可以是其他表扫描或者索引扫描
 */
/*void ha_tnt::initReadSet( bool idxScan, bool posScan) {
	assert(m_thd);
	assert(!(idxScan && posScan));	// 必须不可能既是索引扫描又是rnd_pos扫描
	
	if (idxScan)
		bitmap_copy(&m_readSet, table->read_set);
	else {
		// 表扫描时rnd_init和第一次rnd_next时给出的read_set不一致，为安全起见
		// 使用rnd_init和第一次rnd_next的read_set之合集
		bitmap_union(&m_readSet, table->read_set);
	}
	
	m_isReadAll = bitmap_is_set_all(&m_readSet);
}*/

/**
 * 转换MySQL用位图表示的操作涉及属性集为TNT格式
 *
 * @param bitmap MySQL属性操作位图
 * @param numCols OUT，属性个数
 * @return 各属性号，从会话的MemoryContext中分配
 */
/*u16* ha_tnt::transCols(MY_BITMAP *bitmap, u16 *numCols) {
	// TNT由MySQL上层写binlog，同时TNT目前仅支持行级binlog
	// 在此情况下，MySQL在做update/delete操作时，要求获取
	// 记录的完整全项，作为binlog中的前镜像。此时，不能够
	// 根据read_set来判断读取的列，而是需要强制读取所有列
	// 目前，采用与InnoDB类似的处理方式，若判断出当前语句
	// 需要加X锁，则读取记录的全项
	if (THDVAR(m_thd, is_trx_connection) && m_opInfo.m_selLockType == TL_X) {
		uint columns = table->s->fields;
		*numCols = columns;
		u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16) * columns);
		for (uint i = 0; i < columns; i++) {
			r[i] = i;
		}

		return r;
	}

	return ntse_handler::transCols(bitmap, numCols);
}*/

/**
 * 对比MySQL传入的old_row与new_row，取出其中取值不同的列，作为update列
 * @pre	目前这个函数针对的是 insert on duplicate key update 语句，传入的
 *		old_row是冲突项的全项；new_row是old_row加上update指定列的新取值
 * @param	old_row	冲突键值的全项
 * @param	new_row	冲突键值合并update指定列之后的全项
 * @param	numcols	一共有多少不同列
 *
 * @return	根据old_row与new_row，获取真正update的列
 */
u16* ha_tnt::transColsWithDiffVals(const uchar *old_row, const uchar *new_row, u16 *numCols) {
	Field*		field;
	enum_field_types fieldMysqlType;
	uint		nFields;
	u32			oldColLen;
	u32			newColLen;
	u32			colPackLen;
	const byte*	oldColPtr;
	const byte*	newColPtr;
	u32			nDiff = 0;

	nFields = table->s->fields;
	TableDef *tableDef = m_table->getNtseTable()->getTableDef();

	u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16) * nFields);

	// 循环遍历记录的所有列
	for (uint i = 0; i < nFields; i++) {
		field = table->field[i];

		ColumnDef *colDef = tableDef->m_columns[i];

		// 当前列，在old记录与new记录中的起始偏移地址，由于是MySQL格式，因此偏移是一样的
		oldColPtr = (const byte*) old_row + (field->ptr - table->record[0]);
		newColPtr = (const byte*) new_row + (field->ptr - table->record[0]);

		colPackLen = field->pack_length();

		// 当前列，在old记录与new记录中的长度
		oldColLen = colPackLen;
		newColLen = colPackLen;

		fieldMysqlType = field->type();

		switch (colDef->m_type) {

		case CT_SMALLLOB:
		case CT_MEDIUMLOB:
			if (!colDef->isLongVar()) {
				oldColPtr = rowReadLobCol(&oldColLen, oldColPtr);
				newColPtr = rowReadLobCol(&newColLen, newColPtr);
			} else if (fieldMysqlType == MYSQL_TYPE_VARCHAR) { 
				// 如果是超长字段，这里按照varchar的方式进行处理
				oldColPtr = rowReadVarcharCol(&oldColLen, oldColPtr, (u32)(((Field_varstring*)field)->length_bytes));
				newColPtr = rowReadVarcharCol(&newColLen, newColPtr, (u32)(((Field_varstring*)field)->length_bytes));
			}
			break;
		case CT_VARCHAR:
		case CT_BINARY:
		case CT_VARBINARY:
			if (fieldMysqlType == MYSQL_TYPE_VARCHAR) {
				
				oldColPtr = rowReadVarcharCol(&oldColLen, oldColPtr, (u32)(((Field_varstring*)field)->length_bytes));
				newColPtr = rowReadVarcharCol(&newColLen, newColPtr, (u32)(((Field_varstring*)field)->length_bytes));
			}

			break;
		default:
			;
		}

		if (field->null_ptr) {
			if (isColDataNull(field, old_row)) {
				oldColLen = (u32)-1;
			}

			if (isColDataNull(field, new_row)) {
				newColLen = (u32)-1;
			}
		}

		if (oldColLen != newColLen || (oldColLen != (u32)-1 && 0 != memcmp(oldColPtr, newColPtr, oldColLen))) {

			r[nDiff++] = i;
		}
	}

	// replace into t1 (gesuchnr,benutzer_id) values (1,1);
	// replace into t1 (gesuchnr,benutzer_id) values (1,1);
	// 在以上的语句下，第二个replace，会传入与第一个一模一样的记录
	// 此时，计算出来的nDiff = 0，因此实际上不需要做任何更新操作
	// assert(nDiff != 0);

	*numCols = nDiff;

	return r;
}

/**
* 读取一个MySQL格式记录中的lob字段
* @param	colLen	输入输出/lob字段的长度
* @param	colOffset lob字段的offset
* @return	返回lob字段的起始位置，以及长度
*/
const byte* ha_tnt::rowReadLobCol(u32 *colLen, const byte *colOffset) {
	byte*	data;
	u32		len = *colLen;

	// len为lob字段在record中占用的长度；而lob字段最后8 bytes，为lob指针，因此8 bytes之前的，均为lob size，可为1/2/3/4
	*colLen = readFromNLittleEndian(colOffset, len - 8);

	memcpy(&data, colOffset + len - 8, sizeof(data));

	return(data);
}

/**
* 读取lob字段的长度信息 + 8字节lob对象指针
* MySQL格式，lob列的内容是：lob长度 + 指针；根据lob类型的不同，长度可以占用1/2/3/4 bytes
* @param colOffset	lob字段在record中的起始偏移
* @param colLen		lob字段在record中的占用长度
* @return lob字段的实际占用长度
*/
u32 ha_tnt::readFromNLittleEndian(const byte *colOffset, u32 colLen) {
	u32	nbytes	= 0;
	const byte*	ptr;

	ptr = colOffset + colLen;

	for (;;) {
		ptr--;

		nbytes = nbytes << 8;

		nbytes += (u32)(*ptr);

		if (ptr == colOffset) {
			break;
		}
	}

	return(nbytes);
}

/**
* 读取一个MySQL格式记录中的varchar字段
* @param	colLen 输入输出/varchar字段的长度
* @param	colOffset varchar字段的offset
* @param	lenLen	  表示varchar长度信息的长度
*
*/
const byte* ha_tnt::rowReadVarcharCol(u32 *colLen, const byte *colOffset, u32 lenLen) {
	if (lenLen == 2) {
		*colLen = ((u32)colOffset[0] | ((u32)colOffset[1] << 8));

		return(colOffset + 2);
	}

	assert(lenLen == 1);

	*colLen = (u32)colOffset[0];

	return(colOffset + 1);
}

/**
* 判断记录中的一个给定字段，是否为NULL
* @param field	给定记录中的一列
* @param record 记录
* @return NULL返回true；NOT NULL返回false
*/
bool ha_tnt::isColDataNull(Field *field, const uchar *record) {
	int	nullOffset;

	if (!field->null_ptr) {

		return(false);
	}

	nullOffset = (uint) ((char*) field->null_ptr
		- (char*) table->record[0]);

	if (record[nullOffset] & field->null_bit) {

		return(true);
	}

	return(false);
}

/**
 * 转换MySQL索引搜索键(KEY_MYSQL)为TNT内部格式(KEY_PAD)格式，存储于out中
 * @pre 已经加了表元数据锁
 *
 * @param idx 第几个索引
 * @param key MySQL上层给出的搜索键，为KEY_MYSQL格式
 * @param keypart_map 搜索键中包含哪些索引属性
 * @param out 存储转换后的结果，内存从m_session的内存上下文中分配
 * 
 * @return search key中含有NULL列，返回true；否则返回false
 */
/*bool ha_tnt::transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out) {
	assert(idx < m_table->getNtseTable()->getTableDef(true, m_session)->m_numIndice);
	IndexDef *indexDef = m_table->getNtseTable()->getTableDef(true, m_session)->m_indice[idx];

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

	return RecordOper::convertKeyMP(m_table->getNtseTable()->getTableDef(true, m_session), &mysqlKey, out);
}*/

/**
 * 解析用key_part_map结构表示的搜索键中包含哪些索引属性信息
 *
 * @param idx 第几个索引
 * @param keypart_map 搜索键中包含哪些索引属性信息
 * @param key_len OUT，搜索键长度
 * @param num_cols OUT，搜索键中包含几个索引属性
 */
/*void ha_tnt::parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols) {
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
}*/

/** 将路径中的目录分隔符统一转化成/ */
/*void ha_tnt::normalizePathSeperator(char *path) {
	for (char *p = path; *p; ) {
		char *slash = strchr(p, '\\');
		if (slash) {
			*slash = '/';
			p = slash + 1;
		} else
			break;
	}
}*/

/**
 * 根据MySQL的字符集得到TNT使用的Collation
 *
 * @param charset 字符集
 * @return Collation
 * @throw NtseException 不支持的字符集
 */
/*CollType ha_tnt::getCollation(CHARSET_INFO *charset) throw(NtseException) {
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
}*/

/**
* 根据底层返回的错误，判断是否需要rollback整个事务，并做相应的rollback操作
* 
* @param errCode TNT内部错误码
* @param thd 出错的MySQL线程标识
*/
void ha_tnt::rollbackTrxIfNecessary(ErrorCode errCode, THD *thd) {
	// 目前，针对底层死锁错误/加锁超时错误/唯一性冲突/记录过长/事务中途被中断，需要回滚整个事务
	// 其他的错误，不用回滚整个事务，由上层回滚当前语句即可
	RollBackStat stat = RBS_NONE;
	if (errCode == NTSE_EC_LOCK_TIMEOUT) {
		stat = RBS_TIMEOUT;
	} else if (errCode == NTSE_EC_DEADLOCK) {
		stat = RBS_DEADLOCK;
	} else if (errCode == NTSE_EC_INDEX_UNQIUE_VIOLATION) {
		stat = RBS_DUPLICATE_KEY;
	} else if (errCode == NTSE_EC_ROW_TOO_LONG) {
		stat = RBS_ROW_TOO_LONG;
	} else if (errCode == NTSE_EC_TRX_ABORT) {
		stat = RBS_ABORT;
	} else if (errCode == NTSE_EC_ONLINE_DDL) {
		stat = RBS_ONLINE_DDL;
	} else if (errCode == NTSE_EC_OUT_OF_MEM) {
		stat = RBS_OUT_OF_MEMORY;
	}

	TNTTransaction *trx = NULL;
	if (stat != RBS_NONE && (trx = getTransForCurrTHD(thd)) != NULL) {
		trx->rollbackTrx(stat);
		//当前事务已经回滚，通知MySQL上层，清理本事务对应的binlog
		thd_mark_transaction_to_rollback(thd, true);
	}
}

/**
 * 报告错误，确实是先记录下来等待get_error_message时才返回
 *
 * @param errCode 异常码
 * @param msg 错误信息
 * @param thd 出错的MySQL线程标识
 * @param fatal 是否为严重错误，严重错误将导致系统退出
 * @param warnOrErr 若为true，则调用push_warning_printf报告警告，若为false则调用my_printf_error报告错误
 * @return 返回给MySQL的错误码
 */
int ha_tnt::reportError(ErrorCode errCode, const char *msg, THD *thd, bool fatal, bool warnOrErr) {
	if (tnt_db)
		tnt_db->getTNTSyslog()->log(fatal? EL_PANIC: EL_ERROR, "%s", msg);

	m_errno = excptToMySqlError(errCode);
	int textNo = getMysqlErrCodeTextNo(m_errno);
	if(textNo != 0) {
		if (warnOrErr)
			push_warning_printf(ha_thd(), MYSQL_ERROR::WARN_LEVEL_WARN, textNo, "%s", msg);
		else
			my_printf_error(textNo, "%s", MYF(0), msg);
	}

	strncpy(m_errmsg, msg, sizeof(m_errmsg) - 1);
	m_errmsg[sizeof(m_errmsg) - 1] = '\0';
	if (fatal)
		::exit(-1);
	rollbackTrxIfNecessary(errCode, thd);
	
	return m_errno;
}

/**
 * 得到错误消息
 *
 * @error 上一次操作时返回的错误号
 * @param buf OUT，输出错误消息给MySQL
 * @return 是否是暂时错误
 */
bool ha_tnt::get_error_message(int error, String* buf) {
	ftrace(ts.mysql, tout << this << error << buf);
	if (error != m_errno)
		return false;
	buf->append(m_errmsg);
	m_errno = 0;
	return true;
}

/** 保留指定个数的自增值
 * @param offset			自增起始偏移量。多主环境下，当前主库的编号；单主下为1
 * @param increment			自增步长。多主环境下，表示一共有多少主节点；单主下为1
 * @param nb_desired_values	需要保留多少个自增值
 * @param first_value		输入/输出；若输入的first_value小于当前系统autoinc取值，则重设first_value
							否则直接使用first_value值；同时需要检查first_value的范围
 * @param nb_reserved_value	输出；保留了多少个自增值
 */
void ha_tnt::get_auto_increment( ulonglong offset, ulonglong increment, ulonglong nb_desired_values, ulonglong *first_value, ulonglong *nb_reserved_values ) {
	u64		autoInc;

	// autoInc列的类型设置最值
	u64		autoIncMax = getIntColMaxValue();

	// 读取表上的autoinc值，并且加上autoInc mutex
	m_table->enterAutoincMutex();
	autoInc = m_table->getAutoinc();

	// 在调用此函数之前，表上的autoInc取值一定已经初始化完毕
	assert(autoInc > 0);

	if (table->s->next_number_key_offset) {
		// autoInc 字段不是索引的第一个字段
		*first_value = (~(ulonglong) 0);
		return;
	}

	// TODO：TNT一期版本中，不实现autoInc值的预取功能，nb_reserved_value = 1
	// TODO：TNT一期版本中，不准备支持statement binlog，只支持row binlog，因此只需要mutex控制autoInc的分配即可
	//		 据目前的测试所得，*first_value取值应该在statement binlog复制的slave环境下起作用，因此可简单不处理

	// 由于一期不做缓存，因此first_value就等于autoInc
	*first_value = autoInc;

	// 暂时不缓存autoInc取值
	*nb_reserved_values = 1;

	m_autoInc.m_autoincIncrement = increment;
	m_autoInc.m_autoincOffset = offset;	 
	
	// 计算next value，然后设置
	u64 current = *first_value > autoIncMax ? autoInc : *first_value;
	u64 next_value = calcNextAutoIncr(current, increment, offset, autoIncMax);

	if (next_value < *first_value) {
		*first_value = (~(ulonglong) 0);
	} 
	else {
		// 设置autoInc字段的最大值
		m_table->updateAutoincIfGreater(next_value);
	}

	m_table->exitAutoincMutex();
}

/**
*	获取autoInc列的最大可取值
*	@return 返回最大可取值
*/
u64 ha_tnt::getIntColMaxValue() {
	u64	autoIncMax = 0;
	active_index = table->s->next_number_index;

	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	IndexDef *indexDef = tableDef->m_indice[active_index];
	
	uint i = indexDef->m_columns[0];
	// 自增字段必须是索引的第一个属性
	ColumnDef *colDef = tableDef->m_columns[i];

	switch (colDef->m_type) {
		case CT_TINYINT:
			if (colDef->m_prtype.isUnsigned())
				autoIncMax = 0xFFULL;
			else
				autoIncMax = 0x7FULL;
			break;
		case CT_SMALLINT:	
			if (colDef->m_prtype.isUnsigned())
				autoIncMax = 0xFFFFULL;
			else
				autoIncMax = 0x7FFFULL;
			break;
		case CT_MEDIUMINT:		
			if (colDef->m_prtype.isUnsigned())
				autoIncMax = 0xFFFFFFULL;
			else
				autoIncMax = 0x7FFFFFULL;
			break;
		case CT_INT:	
			if (colDef->m_prtype.isUnsigned())
				autoIncMax = 0xFFFFFFFFULL;
			else
				autoIncMax = 0x7FFFFFFFULL;
			break;
		case CT_BIGINT:		
			if (colDef->m_prtype.isUnsigned())
				autoIncMax = 0xFFFFFFFFFFFFFFFFULL;
			else
				autoIncMax = 0x7FFFFFFFFFFFFFFFULL;
			break;
		case CT_FLOAT:
			autoIncMax = 0x1000000ULL;
			break;
		case CT_DOUBLE:
			autoIncMax = 0x20000000000000ULL;
			break;
		default:
			NTSE_ASSERT(false);
	}

	return autoIncMax;
}

/**
*	初始化表的autoinc字段，只在表real open的时候调用一次
*	在autoInc字段的索引上，进行一次索引扫描，读取最大的值
*	@param	session
*/
u64 ha_tnt::initAutoinc(Session *session) {
	u64	autoIncr = 1;
	const Field* field = table->found_next_number_field;

	if (field == NULL) {
		/* We have no idea what's been passed in to us as the autoinc column. We set it to the 0, effectively disabling updates to the table. */
		nftrace(ts.mysql, tout << "TNT can't find auto_increment column in table: " << m_table->getNtseTable()->getTableDef()->m_name);
		return autoIncr;
	}

	// 使用索引扫描得到自增最大值
	u64 savePoint = session->getMemoryContext()->setSavepoint();

	active_index = table->s->next_number_index;

	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	IndexDef *indexDef = tableDef->m_indice[active_index];
	IndexScanCond cond(active_index, NULL, false, true, false);

	TNTOpInfo opInfo;
	opInfo.m_selLockType	= TL_NO;
	opInfo.m_mysqlOper		= false;
	opInfo.m_sqlStatStart	= false;

	m_checkSameKeyAfterIndexScan = false;

	void *p	= m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	byte *buf = (byte*)m_session->getMemoryContext()->alloc(tableDef->m_maxRecSize);
	SubRecord *foundKey	= new (p)SubRecord(REC_REDUNDANT,indexDef->m_numCols, indexDef->m_columns, buf, tableDef->m_maxRecSize);
	bool result = m_table->getIndice()->getDrsIndex(active_index)->locateLastLeafPageAndFindMaxKey(session,foundKey);
	if (result == true) {
		uint i = indexDef->m_columns[0];
		// 自增字段必须是索引的第一个属性
		ColumnDef *colDef = tableDef->m_columns[i];
		// NTSETNT-120：tnt-autoinc-44030.test测试用例报错，改列名不会下降到引擎层面；
		//assert(strcmp(colDef->m_name, field->field_name) == 0);

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
	} else 
		// 当前表中无数据，则将autoInc设置为1
		autoIncr = 1;

	session->getMemoryContext()->resetToSavepoint(savePoint);

	return autoIncr;
}

/** 计算下一个自增字段值
 * @param current	当前值
 * @param increment	每次自增步长
 * @param offset	自增偏移量
 * @param maxValue	自增变量类型的最大可能值
 * @description		参看InnoDB的innobase_next_autoinc的实现
 */
u64 ha_tnt::calcNextAutoIncr( u64 current, u64 increment, u64 offset, u64 maxValue ) {
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
		// 计算current值，是步长increment的多少倍
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

/**
*	用户指定了autoInc的取值，则需要根据情况设置当前表中autoInc的next value
*	@param buf	上层给定的数据块
*	
*/
void ha_tnt::setAutoIncrIfNecessary(uchar * buf) {
	if (table->next_number_field && buf == table->record[0]) {
		// 由于auto increment可取的最大值为64位无符号数，因此此处应该使用u64，而非s64
		u64 autoIncr = table->next_number_field->val_int();
		if (autoIncr > 0) {	// 修改了自增字段

			m_table->enterAutoincMutex();

			u64 autoIncMax = getIntColMaxValue();

			// 处理用户手动设置autoinc值为负数的情况
			if (autoIncr <= autoIncMax) {
				u64 nextValue = calcNextAutoIncr(autoIncr, m_autoInc.m_autoincIncrement, m_autoInc.m_autoincOffset, autoIncMax);
				m_table->updateAutoincIfGreater(nextValue);
			}

			m_table->exitAutoincMutex();
		}
	}
}

/**
 * 关闭连接时通知TNT。该函数只有在连接信息不为NULL时才会调用
 *
 * @param hton TNT存储引擎实例
 * @param thd 要关闭的连接
 * @return 总是返回0
 */
int ha_tnt::tnt_close_connection(handlerton *hton, THD* thd) {
	ftrace(ts.mysql, tout << hton << thd);
	DBUG_ENTER("tnt_close_connection");

	NTSE_ASSERT(hton == tnt_hton);

	TNTTHDInfo *info = getTntTHDInfo(thd);
	assert(info);

	if (info->m_pendingOper) {
		if (info->m_pendingOpType == POT_BACKUP) {
			TNTBackupProcess *backup = (TNTBackupProcess *)info->m_pendingOper;
			tnt_db->doneBackup(backup);
			info->resetPendingOper();
		}
	}

	TNTTransaction *trx = info->m_trx;

	if (trx != NULL) {
		// TODO：根据事务当前的状态，返回不同的WARNNing信息
		trx->rollbackTrx(RBS_NORMAL);
		tnt_db->getTransSys()->freeTrx(trx);
	}

	delete info;
	thd_set_ha_data(thd, hton, NULL);

	DBUG_RETURN(0);
}

/**
 * 创建一个TNT handler
 *
 * @param hton TNT存储引擎实例
 * @param table handler要操作的表
 * @param mem_root 在此创建
 * @return 新创建的handler
 */
handler* ha_tnt::tnt_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) {
	ftrace(ts.mysql, tout << hton << table << mem_root);
	return new (mem_root) ha_tnt(hton, table);
}

/**
 * 删除指定database目录下的所有TNT表
 *
 * @param hton TNT存储引擎实例
 * @param path database路径
 * @return 
 */
void ha_tnt::tnt_drop_database(handlerton *hton, char* path) {
	// TODO：由于NTSE暂时不支持drop database方法
	// 同时TNT使用的是NTSE的持久化存储，因此TNT也暂时不支持
}


#ifdef EXTENDED_FOR_COMMIT_ORDERED
void ha_tnt::tnt_commit_ordered(handlerton *hton, THD* thd, bool all){
	DBUG_ENTER("tnt_commit_ordered");

	DBUG_SWITCH_NON_RETURN(thd, ntse_handler::ntse_commit_ordered(hton, thd, all));

	TNTTHDInfo *info = getTntTHDInfo(thd);
	TNTTransaction *trx = info->m_trx;
	assert(trx != NULL);

	if (all ||
		(!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
			trx->commitTrxOrdered(CS_NORMAL);
			trx->setActiveTrans(0);
			thd->variables.net_wait_timeout = info->m_netWaitTimeout;
	} else {
		trx->markSqlStatEnd();
	}
	// 设置事务已经调用过commitOrdered，在之后的commit阶段只需要刷日志即可
	trx->setCalledCommitOrdered(true);
	DBUG_VOID_RETURN;
}
#endif


/**
* 提交事务，或者标识当前statement执行完成
* @param hton	TNT存储引擎实例
* @param thd	当前连接
* @param all  	是提交事务，还是标识当前statement结束
* @return 总是返回0
*/
int ha_tnt::tnt_commit_trx(handlerton *hton, THD* thd, bool all) {
	DBUG_ENTER("tnt_commit_trx");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::ntse_commit_trx(hton, thd, all));
	// TNTTransaction *trx = getTransForCurrTHD(thd);

	// 调用getTransInTHD函数，因为事务一定保证存在，无需创建
	TNTTHDInfo *info = getTntTHDInfo(thd);
	TNTTransaction *trx = info->m_trx;
	assert(trx != NULL);

	if (all ||
		(!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {

			// 当前是事务提交，或者是Autocommit语句执行结束，提交整个事务
			/*if (trx->getTrxState() == TRX_ACTIVE || 
				trx->getTrxState() == TRX_PREPARED) {
			}*/

#ifdef EXTENDED_FOR_COMMIT_ORDERED
			// 如果没有调用过commitOrdered（例如binlog没有开的情况）需要先做commit in memory的相关工作
			if (!trx->getCalledCommitOrdered())	
				trx->commitTrxOrdered(CS_NORMAL);
			trx->commitCompleteForMysql();
#else
			trx->commitTrx(CS_NORMAL);
			// commit完成之后，释放prepare mutex
			if (trx->getActiveTrans() == 2) {
				assert(prepareCommitMutex->isLocked());
				prepareCommitMutex->unlock();
			}
#endif
			trx->setActiveTrans(0);
			// 标识当前事务从MySQL的XA事务中取消注册
			trxDeregisterFrom2PC(trx);
			thd->variables.net_wait_timeout = info->m_netWaitTimeout;
	} else {
		// 当前是语句级提交，直接标识当前语句执行结束即可
		trx->markSqlStatEnd();
	}

	DBUG_RETURN(0);
}

/**
* 回滚事务，或者是回滚当前statement
* @param hton	TNT存储引擎实例
* @param thd	当前连接
* @param all	会回滚事务，还是回滚当前statement
* @return 成功返回0；失败返回其他error值
*/
int ha_tnt::tnt_rollback_trx(handlerton *hton, THD *thd, bool all) {
	DBUG_ENTER("tnt_rollback_trx");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::ntse_rollback_trx(hton, thd, all));
	int ret = 0;
	// TNTTransaction *trx = getTransForCurrTHD(thd);

	// 调用getTransInTHD函数，因为事务一定保证存在，无需创建
	TNTTHDInfo *info = getTntTHDInfo(thd);
	TNTTransaction *trx = info->m_trx;
	assert(trx != NULL);

	// 根据rollback的类型，语句级rollback or 事务级rollback，进行相应的操作
	// 内部XA事务，Prepare成功之后一定提交，用户无法控制，prepare事务不会回滚
	// 外部XA事务，由用户控制，prepare完成之后，既可以提交，也可以回滚
	// 内部XA事务prepare阶段加Mutex，外部不加Mutex，因此rollback均无获取Mutex
	if (all ||
		!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
			// 回滚整个事务
			ret = trx->rollbackTrx(RBS_NORMAL);

			trx->setActiveTrans(0);

			// 标识当前事务从MySQL的XA事务中取消注册
			trxDeregisterFrom2PC(trx);

			thd->variables.net_wait_timeout = info->m_netWaitTimeout;
	} else {
		// 回滚当前语句
		ret = trx->rollbackLastStmt(RBS_NORMAL);
	}

	// TODO：此处先假设Rollback一定成功
	DBUG_RETURN(0);
}

/**
* TNT引擎支持XA事务，此函数是XA事务的prepare阶段
* @param hton	TNT存储引擎实例
* @param thd	当前连接
* @param all	提交整个事务，或是仅仅提交当前statement
* @return 成功返回0；失败返回error值
*/
int ha_tnt::tnt_xa_prepare(handlerton *hton, THD* thd, bool all) {
	DBUG_ENTER("tnt_xa_prepare");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::ntse_xa_prepare(hton, thd, all));
	// TNTTransaction *trx = getTransForCurrTHD(thd);

	// 调用getTransInTHD函数，因为事务一定保证存在，无需创建
	TNTTransaction *trx = getTransInTHD(thd);
	assert(trx != NULL);

	int ret = 0;

	if (!THDVAR(thd, support_xa)) {
		return 0;
	}

	// 设置XID
	XID xid;
	thd_get_xid(thd, (MYSQL_XID*)&xid);
	trx->setXId((const XID &)xid);

	if (all ||
		(!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
			// 事务级prepare，或者是Autocommit事务的语句结束，都需要prepare整个事务
			if (trx->getTrxState() == TRX_ACTIVE) {
				trx->prepareForMysql();

				// 与InnoDB 5.1.49类似，如果当前不是外部XA事务的prepare命令
				// 那么在事务级prepare或者是Autocommit语句结束时，需要加prepare锁，
				// 保证prepare -> binlog commit -> commit的顺序性

				// 注意：此时加Mutex，意味着prepare操作是可以并行的，无顺序保证；而binlog commit -> commit是有序的

				if (thd_sql_command(thd) != SQLCOM_XA_PREPARE &&
					(all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
#ifndef EXTENDED_FOR_COMMIT_ORDERED
						prepareCommitMutex->lock(__FILE__, __LINE__);
#endif
						// 设置事务当前已经获取prepareCommitMutex状态
						trx->setActiveTrans(2);
				}

				assert(trx->getTrxState() == TRX_PREPARED);
			}
	} else {
		// 语句级prepare，直接标识语句结束即可
		trx->markSqlStatEnd();
	}

	DBUG_RETURN(ret);
}

/**
* TNT支持XA事务，此函数是XA事务的恢复，获取目前存储引擎中处于prepare状态的事务对应的xid
* @param hton		TNT存储引擎实例
* @param xid_list	in/out参数；out：当前TNT系统中处于prepare状态的事务列表
* @param len		xid_list中有多少slots可用
* @return 返回TNT系统有多少事务处于prepare状态，对应的XID填充到xid_list之中，一次最多返回len个xids
*/
int ha_tnt::tnt_xa_recover(handlerton *hton, XID* xid_list, uint len) {
	DBUG_ENTER("tnt_xa_recover");
	int ret = 0;
	if (xid_list == NULL || len == 0) {
		DBUG_RETURN(0);
	}

	TNTTrxSys *trxsys = tnt_db->getTransSys();
	ret = trxsys->getPreparedTrxForMysql(xid_list, len);
	DBUG_RETURN(ret);
}

/**
* 提交一个处于prepare状态的事务
* @param hton	TNT存储引擎实例
* @param xid	prepare事务id，mysql上层的标识，非tnt引擎事务id，需要转换
* @return 成功返回0；失败返回error值
*/
int ha_tnt::tnt_trx_commit_by_xid(handlerton *hton, XID *xid) {
	DBUG_ENTER("tnt_trx_commit_by_xid");
	int ret = 0;
	TNTTrxSys *trxsys = tnt_db->getTransSys();
	TNTTransaction *trx = trxsys->getTrxByXID(xid);
	if (trx != NULL) {
		trx->commitTrx(CS_NORMAL);
		trxsys->freeTrx(trx);
		ret = XA_OK;
	} else {
		//已经被后台线程kill了
		ret = XA_RBROLLBACK;
	}

	DBUG_RETURN(ret);
}

/**
* 回滚一个处于prepare状态的事务
* @param hton	TNT存储引擎实例
* @param xid	prepare事务id，mysql上层的标识，非tnt引擎事务id，需要转换
* @return 成功返回0；失败返回error值
*/
int ha_tnt::tnt_trx_rollback_by_xid(handlerton *hton, XID* xid) {
	DBUG_ENTER("tnt_trx_rollback_by_xid");
	int ret = 0;
	TNTTrxSys *trxsys = tnt_db->getTransSys();
	TNTTransaction *trx = trxsys->getTrxByXID(xid);
	if (trx != NULL) {
		bool recoverPrepare = false;
		Connection *conn = NULL;
		//为崩溃恢复的prepare事务回滚时需要额外分配一个connection
		if(trx->getConnection() == NULL) {
			conn = tnt_db->getNtseDb()->getConnection(false, "Rollback Prepare Transaction");
			trx->setConnection(conn);
			recoverPrepare = true;
		}
		trx->rollbackTrx(RBS_NORMAL);
		trxsys->freeTrx(trx);
		if(recoverPrepare)
			tnt_db->getNtseDb()->freeConnection(conn);
	} else {
		//已经被后台线程kill了
		ret = XA_OK;
	}

	DBUG_RETURN(ret);

	/**
	* 暂时，TNT在完成Crash Recovery之后，直接将内存数据Purge到NTSE，无法进行回滚，因此用commit方法替代
	* 后续，等到TNT内存支持创建索引之后，Crash Recovery完成后可以选择不Purge到NTSE，此时才可以将Prepare事务回滚
	*/
/*	DBUG_ENTER("tnt_trx_commit_by_xid");

	int ret = 0;

	TNTTrxSys *trxsys = tnt_db->getTransSys();

	TNTTransaction *trx = trxsys->getTrxByXID(xid);

	if (trx != NULL) {
		trx->commitTrx(CS_NORMAL);

		trxsys->freeTrx(trx);

		ret = XA_OK;
	} else {
		ret = XAER_NOTA;
	}

	DBUG_RETURN(ret);
*/
}

/*int ha_tnt::checkOperatePermission(THD *thd, bool dml, bool read) {
	int code = 0;
	bool trxConn = THDVAR(thd, is_trx_connection);
	TableStatus tblStatus = TS_NON_TRX;
	Table *ntseTable = m_table->getNtseTable();
	ILMode mode = ntseTable->getMetaLock(m_session);
	assert(mode != IL_NO);
	tblStatus = ntseTable->getTableDef()->getTableStatus();
	if (tblStatus == TS_CHANGING) {
		code = reportError(NTSE_EC_NOT_SUPPORT, "The table status whether supports transaction is changing", thd, false);
	} else  if (trxConn && tblStatus == TS_NON_TRX) { //事务连接操作非事务表
		code =  reportError(NTSE_EC_NOT_SUPPORT, "transactional connection can't operate on non-transactional table", thd, false);
	} else if (!trxConn && tblStatus == TS_TRX) {
		if (!dml) {
			code = reportError(NTSE_EC_NOT_SUPPORT, "non transactional connection can't operate ddl on transactional table", thd, false);
		} else if (!read) {
			code = reportError(NTSE_EC_NOT_SUPPORT, "non transactional connection can't operate write dml on transactional table", thd, false);
		}
	}

	//TODO
	if (code != 0) {
		ntseTable->unlockMeta(m_session, mode);
	}
	return code;
}*/



/**
* 上层询问tnt 针对一张表的查询能否被cache
* @param thd	T当前连接
* @param table_key	table cache 中关于tableName的指针
* @param key_length tableName的长度
* @param call_back	引擎层的回调函数
* @param engine_data  引擎层的数据（可以是任何数据）
* @return 允许caching 返回TRUE；否则返回FALSE
*/
my_bool
ha_tnt::register_query_cache_table(
	THD*		thd,		
	char*		table_key,	
	uint		key_length,
	qc_engine_callback*
			call_back,	
	ulonglong	*engine_data)	
{
	// TNT表暂时不允许MYSQL上层使用query cache
	*call_back = 0;
	*engine_data = 0;
	return FALSE;
}


int ha_tnt::checkGeneralPermission(THD *thd) {
	int code = 0;
	char msg[256];
	bool trxConn = THDVAR(thd, is_trx_connection);
	TableStatus tblStatus = TS_NON_TRX;
	Table *ntseTable = m_table->getNtseTable();
	assert(ntseTable->getMetaLock(m_session) != IL_NO);
	tblStatus = ntseTable->getTableDef()->getTableStatus();
	if (tblStatus == TS_CHANGING) {
		System::snprintf_mine(msg, 256, "The table(%s) status whether supports transaction is changing", ntseTable->getTableDef()->m_name);
		code = reportError(NTSE_EC_NOT_SUPPORT, msg, thd, false);
	} else if (tblStatus == TS_ONLINE_DDL) {
		System::snprintf_mine(msg, 256, "The table(%s) is online ddl operation", ntseTable->getTableDef()->m_name);
		code = reportError(NTSE_EC_ONLINE_DDL, msg, thd, false);
	} else  if (trxConn && tblStatus == TS_NON_TRX) { //事务连接操作非事务表
		System::snprintf_mine(msg, 256, "Transactional connection can't operate on non-transactional table(%s)", ntseTable->getTableDef()->m_name);
		code =  reportError(NTSE_EC_NOT_SUPPORT, msg, thd, false);
	}
	return code;
}

static struct st_mysql_sys_var* tntVariables[] = {
	// TNT SYS VAR
	MYSQL_SYSVAR(tnt_version),
// 	MYSQL_SYSVAR(tnt_dump_dir),
// 	MYSQL_SYSVAR(tnt_dump_reserve_cnt),
	MYSQL_SYSVAR(tnt_buf_size),
	MYSQL_SYSVAR(tnt_lock_timeout),
	MYSQL_SYSVAR(tnt_max_trx_cnt),
	MYSQL_SYSVAR(tnt_trx_flush_mode),
	MYSQL_SYSVAR(tnt_purge_interval),
	MYSQL_SYSVAR(tnt_dump_interval),
	MYSQL_SYSVAR(tnt_auto_upgrade_from_ntse),
/*
	MYSQL_SYSVAR(tnt_purge_threshold),
	MYSQL_SYSVAR(tnt_purge_enough),
	MYSQL_SYSVAR(tnt_auto_purge),
	MYSQL_SYSVAR(tnt_purge_before_close),
	MYSQL_SYSVAR(tnt_purge_after_recover),
	MYSQL_SYSVAR(tnt_dump_interval),
	MYSQL_SYSVAR(tnt_dump_on_redo_size),
	MYSQL_SYSVAR(tnt_auto_dump),
	MYSQL_SYSVAR(tnt_dump_before_close),
*/
	MYSQL_SYSVAR(tnt_verpool_file_size),
	MYSQL_SYSVAR(tnt_verpool_cnt),
	MYSQL_SYSVAR(tnt_defrag_hashIndex_max_traverse_cnt),
	MYSQL_SYSVAR(tnt_open_table_cnt),
	MYSQL_SYSVAR(is_trx_connection),
	MYSQL_SYSVAR(support_xa),

	MYSQL_SYSVAR(tnt_max_trx_run_time),
	MYSQL_SYSVAR(tnt_max_trx_locks),
	MYSQL_SYSVAR(tnt_max_trx_common_idle_time),
	MYSQL_SYSVAR(tnt_max_trx_prepare_idle_time),

	MYSQL_SYSVAR(tnt_mem_index_reclaim_hwm),
	MYSQL_SYSVAR(tnt_mem_index_reclaim_lwm),

	// NTSE SYS VAR
	MYSQL_SYSVAR(version),
	MYSQL_SYSVAR(tmpdir),
	MYSQL_SYSVAR(logdir),
	MYSQL_SYSVAR(log_file_cnt_hwm),
	MYSQL_SYSVAR(log_file_size),
	MYSQL_SYSVAR(log_buf_size),
	MYSQL_SYSVAR(page_buf_size),
	MYSQL_SYSVAR(rec_buf_size),
	MYSQL_SYSVAR(common_pool_size),
	MYSQL_SYSVAR(max_sessions),
	MYSQL_SYSVAR(table_lock_timeout),
	MYSQL_SYSVAR(scavenger_pages),
	MYSQL_SYSVAR(system_io_capacity),
	MYSQL_SYSVAR(command),
	MYSQL_SYSVAR(incr_size),
	MYSQL_SYSVAR(deferred_read_cache_size),
	MYSQL_SYSVAR(sample_max_pages),
	MYSQL_SYSVAR(backup_before_recover),
	MYSQL_SYSVAR(verify_after_recover),
	MYSQL_SYSVAR(enable_syslog),
	MYSQL_SYSVAR(directio),
	MYSQL_SYSVAR(aio),
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


/**
* 存储引擎，information schema声明
*/
static struct st_mysql_storage_engine tnt_storage_engine = { MYSQL_HANDLERTON_INTERFACE_VERSION };

static st_mysql_information_schema tnt_ntse_mutex_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_intentionlock_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_rwlock_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_buf_distr = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_conns = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_mms = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_mms_rpcls = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_tbl_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_heap_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_idx_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_heap_stats_ex = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_idx_stats_ex = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_lob_stats = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_lob_stats_ex = { MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_command_return = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_tbl_def_ex = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_col_def_ex = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_idx_def_ex = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_mms_ridhash_conflicts = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_dbobj_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_compress_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#ifdef NTSE_PROFILE
static st_mysql_information_schema tnt_ntse_prof_summ = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_bgthd_prof = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_conn_prof = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#ifdef NTSE_KEYVALUE_SERVER
static st_mysql_information_schema tnt_ntse_keyvalue_prof = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#endif
#endif

#ifdef NTSE_KEYVALUE_SERVER
static st_mysql_information_schema tnt_ntse_keyvalue_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_ntse_keyvalue_threadinfo = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
#endif
static st_mysql_information_schema tnt_tnt_mheap_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_tnt_hash_index_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_tnt_transaction_sys_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_tnt_transaction_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_tnt_inner_transaction_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

static st_mysql_information_schema tnt_tnt_memory_index_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};
static st_mysql_information_schema tnt_tnt_index_stats = {MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};


/**
* 注册TNT存储引擎到mysql
*/
mysql_declare_plugin(tnt) {
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&tnt_storage_engine,
	"Tnt",
	"NetEase Corporation",
	"Transactional storage engine",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_tnt::init,							/** 插件初始化 */
	ha_tnt::exit,							/** 安全退出 */
	0x0010,									/** 版本号0.1 */
	tntStatus,								/** 状态变量 */
	tntVariables,							/** 配置参数 */
	NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_mutex_stats,
		"TNT_NTSE_MUTEX_STATS",
		"NetEase  Corporation",
		"Statistics of Mutex objects in NTSE.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISMutexStats,
		ha_tnt::deinitISMutexStats,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_intentionlock_stats,
		"TNT_NTSE_INTENTION_LOCK_STATS",
		"NetEase  Corporation",
		"Statistics of IntentionLock objects in NTSE.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISIntentionLockStats,
		ha_tnt::deinitISIntentionLockStats,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_rwlock_stats,
		"TNT_NTSE_RWLOCK_STATS",
		"NetEase  Corporation",
		"Statistics of read/write lock objects in NTSE.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISRWLockStats,
		ha_tnt::deinitISRWLockStats,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_buf_distr,
		"TNT_NTSE_BUF_DISTRIBUTION",
		"NetEase  Corporation",
		"Distribution of buffer pages.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISBufDistr,
		ha_tnt::deinitISBufDistr,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_conns,
		"TNT_NTSE_CONNECTIONS",
		"NetEase  Corporation",
		"Per connection statistics.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISConns,
		ha_tnt::deinitISConns,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_mms,
		"TNT_NTSE_MMS_STATS",
		"NetEase  Corporation",
		"MMS statistics per table.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISMms,
		ha_tnt::deinitISMms,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_mms_rpcls,
		"TNT_NTSE_MMS_RPCLS_STATS",
		"NetEase  Corporation",
		"Statistics of each record page class of MMS.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISMmsRPCls,
		ha_tnt::deinitISMmsRPCls,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_tbl_stats,
		"TNT_NTSE_TABLE_STATS",
		"NetEase  Corporation",
		"Statistics of each opened table.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTable,
		ha_tnt::deinitISTable,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_heap_stats,
		"TNT_NTSE_HEAP_STATS",
		"NetEase  Corporation",
		"Statistics of each heap of opened tables.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISHeap,
		ha_tnt::deinitISHeap,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_heap_stats_ex,
		"TNT_NTSE_HEAP_STATS_EX",
		"NetEase  Corporation",
		"Extended statistics of each heap of opened tables.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISHeapEx,
		ha_tnt::deinitISHeapEx,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_idx_stats,
		"TNT_NTSE_INDEX_STATS",
		"NetEase  Corporation",
		"Statistics of each index of opened tables.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISIndex,
		ha_tnt::deinitISIndex,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_idx_stats_ex,
		"TNT_NTSE_INDEX_STATS_EX",
		"NetEase  Corporation",
		"Extended statistics of each index of opened tables.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISIndexEx,
		ha_tnt::deinitISIndexEx,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_lob_stats,
		"TNT_NTSE_LOB_STATS",
		"NetEase  Corporation",
		"Statistics of each lob storage of opened tables.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISLob,
		ha_tnt::deinitISLob,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_lob_stats_ex,
		"TNT_NTSE_LOB_STATS_EX",
		"NetEase  Corporation",
		"Extended statistics of each lob storage of opened tables.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISLobEx,
		ha_tnt::deinitISLobEx,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_tbl_def_ex,
		"TNT_NTSE_TABLE_DEF_EX",
		"NetEase Corporation",
		"NTSE specific table definition informations.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTblDefEx,
		ha_tnt::deinitISTblDefEx,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_col_def_ex,
		"TNT_NTSE_COLUMN_DEF_EX",
		"NetEase Corporation",
		"NTSE specific column definition informations.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISColDefEx,
		ha_tnt::deinitISColDefEx,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_idx_def_ex,
		"TNT_NTSE_INDEX_DEF_EX",
		"NetEase Corporation",
		"NTSE specific index definition informations.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISIdxDefEx,
		ha_tnt::deinitISIdxDefEx,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_command_return,
		"TNT_NTSE_COMMAND_RETURN",
		"NetEase  Corporation",
		"Status of NTSE's internal command for current thread.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISCmdReturn,
		ha_tnt::deinitISCmdReturn,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_mms_ridhash_conflicts,
		"TNT_NTSE_MMS_RIDHASH_CONFLICTS",
		"Netease Corporation",
		"NTSE MMS RID Hash Conflict Status.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISMmsRidHashConflicts,
		ha_tnt::deinitISMmsRidHashConflicts,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_dbobj_stats,
		"TNT_NTSE_DBOBJ_STATS",
		"Netease Corporation",
		"NTSE DB Object Status.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISDBObjStats,
		ha_tnt::deinitISDBObjStats,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_compress_stats,
		"TNT_NTSE_COMPRESS_STATS",
		"Netease Corporation",
		"NTSE Row Compression Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISCompressStats,
		ha_tnt::deinitISCompressStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
#ifdef NTSE_PROFILE
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_prof_summ,
		"TNT_NTSE_PROFILE_SUMMARY",
		"NetEase  Corporation",
		"NTSE Internal Performence Profile Summary.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISProfSumm,
		ha_tnt::deinitISProfSumm,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_bgthd_prof,
		"TNT_NTSE_BGTHREAD_PROFILES",
		"NetEase  Corporation",
		"NTSE BG Thread Profiles.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISBGThdProfile,
		ha_tnt::deinitISBGThdProfile,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_conn_prof,
		"TNT_NTSE_CONN_PROFILES",
		"NetEase  Corporation",
		"NTSE Connection Profiles.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISConnThdProfile,
		ha_tnt::deinitISConnThdProfile,
		0x0010,
		NULL,
		NULL,
		NULL
}
#ifdef NTSE_KEYVALUE_SERVER
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_keyvalue_prof,
		"TNT_NTSE_KEYVALUE_PROFILES",
		"NetEase  Corporation",
		"NTSE Keyvalue Profiles.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initKVProfSumm,
		ha_tnt::deinitKVProfSumm,
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
		&tnt_ntse_keyvalue_stats,
		"TNT_NTSE_KEYVALUE_STATISTIC",
		"NetEase  Corporation",
		"NTSE Keyvalue Statistics.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initKVStats,
		ha_tnt::deinitKVStats,
		0x0010,
		NULL,
		NULL,
		NULL
}, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_ntse_keyvalue_threadinfo,
		"TNT_NTSE_KEYVALUE_THREADINFO",
		"NetEase  Corporation",
		"NTSE Keyvalue Thread information.",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initKVThreadInfo,
		ha_tnt::deinitKVThreadInfo,
		0x0010,
		NULL,
		NULL,
		NULL
}
#endif // NTSE_KEYVALUE_SERVER
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_tnt_mheap_stats,
		"TNT_TNT_MHEAP_STATS",
		"Netease Corporation",
		"TNT MHEAP Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTNTMHeapStats,
		ha_tnt::deinitISTNTMHeapStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_tnt_hash_index_stats,
		"TNT_TNT_HASH_INDEX_STATS",
		"Netease Corporation",
		"TNT Hash Index Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTNTHashIndexStats,
		ha_tnt::deinitISTNTHashIndexStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_tnt_transaction_sys_stats,
		"TNT_TNT_TRANSACTION_SYS_STATS",
		"Netease Corporation",
		"TNT Transaction System Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTNTTrxSysStats,
		ha_tnt::deinitISTNTTrxSysStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_tnt_transaction_stats,
		"TNT_TNT_TRANSACTION_STATS",
		"Netease Corporation",
		"TNT Transaction Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTNTTrxStats,
		ha_tnt::deinitISTNTTrxStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_tnt_inner_transaction_stats,
		"TNT_TNT_INNER_TRANSACTION_STATS",
		"Netease Corporation",
		"TNT Inner Transaction Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTNTInnerTrxStats,
		ha_tnt::deinitISTNTInnerTrxStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_tnt_memory_index_stats,
		"TNT_TNT_MEMORY_INDEX_STATS",
		"Netease Corporation",
		"TNT Memory Index Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTNTMemoryIndexStats,
		ha_tnt::deinitISTNTMemoryIndexStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
, {
	MYSQL_INFORMATION_SCHEMA_PLUGIN,
		&tnt_tnt_index_stats,
		"TNT_TNT_INDEX_STATS",
		"Netease Corporation",
		"TNT Index Status",
		PLUGIN_LICENSE_PROPRIETARY,
		ha_tnt::initISTNTIndexStats,
		ha_tnt::deinitISTNTIndexStats,
		0x0010,
		NULL,
		NULL,
		NULL
}
mysql_declare_plugin_end;
