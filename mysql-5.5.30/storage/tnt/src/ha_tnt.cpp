/**
* TNT�洢����handle�ࡣ
*
* @author �εǳ�
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

// TODO�����г�����MySQL�ϲ�ĵط�������Ҫ������֤����ʽ��
// ������NTSE��ȫһ��(������������������������)

/** �Ƚ�·����TNTShare�Ƿ���ȵĺ������� */
class ShareEqualer {
public:
	inline bool operator()(const char *path, const TNTShare *share) const {
		return strcmp(path, share->m_path) == 0;
	}
};

/** ����TableInfoEx�����ϣֵ�ĺ������� */
class ShareHasher {
public:
	inline unsigned int operator()(const TNTShare *share) const {
		return m_strHasher.operator ()(share->m_path);
	}

private:
	Hasher<char *>	m_strHasher;
};

#ifdef NTSE_KEYVALUE_SERVER
KeyValueServer	*keyvalueServer = NULL;	/** keyvalue������߳� */
static ThriftConfig keyvalueConfig;	/** keyvalue��������� */
#endif

/************************************************************************/
/* TNT����ȫ�ֱ���                                                                     */
/************************************************************************/
handlerton		*tnt_hton;			/** Handler singleton */
static TNTConfig tnt_config;
TNTDatabase		*tnt_db = NULL;		/** ȫ��Ψһ�����ݿ� */
CmdExecutor		*cmd_exec = NULL;	/** ȫ��Ψһ������ִ���� */
TNTTransaction	*dummy_trx = NULL;	/** �ٵ����񣬴���sessionʱ������Ҫ����֧�֣������ֵ */

Mutex			*prepareCommitMutex;/** ȫ��Mutex����������prepare->commit��˳��*/

typedef DynHash<const char *, TNTShare *, ShareHasher, Hasher<const char *>, ShareEqualer> TblHash;
static TblHash	openTables;			/** �Ѿ��򿪵ı� */
static Mutex	openLock("openLock", __FILE__, __LINE__);			/** �����Ѿ��򿪵ı���� */

///////////////////////////////////////////////////////////////////////////////
// ���ò�����״̬������For TNT  ///////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
static char		*tnt_version  = NULL;
// static char		*tnt_dump_dir = (char *)".";	// TNT dump�ļ�·��
// static uint     tnt_dump_reserve_cnt = 10;      // TNT dump�����ļ��и���
static unsigned long long tnt_buf_size;	// TNT��Ҫ������ڴ��С
static ulong	tnt_lock_timeout;		// ���������ȴ���ʱʱ������	
static uint		tnt_max_trx_cnt;		// ����ϵͳӵ�е������������
static uint     tnt_trx_flush_mode;     // ����ˢ��־�ķ�ʽ
static char     tnt_auto_upgrade_from_ntse;

// TNT purge��������������
//static ulong	tnt_purge_threshold;	// �ڴ������ʴﵽ����֮��(0,100)����Ҫpurge��TNT_Buf_Size�İٷֱ�
//static ulong	tnt_purge_enough;		// Purge���ն����ڴ�pages֮�󣬿�����Ϊpurge�ɹ���TNT_Buf_Size�İٷֱ�
//static char		tnt_auto_purge;			// TNT�����Ƿ��������auto purge����ϵͳ����ʱ
static ulong	tnt_purge_interval;		// ����purge����֮���ʱ������Second
//static char		tnt_purge_before_close;	// TNT����ر�ǰ���Ƿ���Ҫ�������һ��purge��Ĭ�ϲ���Ҫ
//static char     tnt_purge_after_recover;//TNT recover���Ƿ����purge��Ĭ��Ϊ��Ҫ

// TNT dump��������������
static ulong	tnt_dump_interval;		// ����purge������һ��dump
//static ulong	tnt_dump_interval;		// ����dump����֮���ʱ������Second
//static ulong	tnt_dump_on_redo_size;	// �ϴ�dump������д�����tnt redo��־����Ҫdump
//static char		tnt_auto_dump;			// TNT�����Ƿ��������auto dump����ϵͳ����ʱ
//static char		tnt_dump_before_close;	// TNT����ر�ǰ���Ƿ���Ҫ�������һ��dump��Ĭ����Ҫ
// TNT version pool�������

static ulong	tnt_verpool_file_size;	// �汾�ص����ļ���С��G
static ulong	tnt_verpool_cnt;		// �汾������

static ulong    tnt_defrag_hashIndex_max_traverse_cnt; //defrag hash indexʱ�������ĸ���
static ulong	tnt_open_table_cnt;

static ulong	tnt_max_trx_run_time;
static ulong	tnt_max_trx_locks;
static ulong	tnt_max_trx_common_idle_time;
static ulong	tnt_max_trx_prepare_idle_time;

static ulong	tnt_mem_index_reclaim_hwm;
static ulong	tnt_mem_index_reclaim_lwm;

///////////////////////////////////////////////////////////////////////////////
// ���ò�����״̬������For NTSE ///////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
// ������ȫ�����ã��μ�Config��˵��
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
static ulong	ntse_incr_size;	/** �ѡ������������ļ���չҳ���� */
#ifdef NTSE_PROFILE
static int 		ntse_profile_summary = 0; /** ͨ������-1/0/1 ������ȫ��Profile */
static int		ntse_thread_profile_autorun = 0; /** ͨ������0/1 �������߳�Profile�Ƿ��Զ�����*/
#endif
static char		ntse_backup_before_recover = 0;
static char		ntse_verify_after_recover = 0;
static char		ntse_enable_syslog = 1;	/** NTSE��log�Ƿ������mysqld.log */
static char		ntse_directio = 1; /** �Ƿ�ʹ��directio */
static char		ntse_aio = 1;	/** �Ƿ�ʹ��aio */
static ulong	ntse_system_io_capacity = 0; /** ��ǰϵͳ�����ÿ���IO��������λΪҳ���� */

static TNTGlobalStatus tnt_status;	/** TNT��ȫ��ͳ����Ϣ */
static Status tnt_ntse_status;		/** TNT��NTSEȫ��ͳ����Ϣ */

#ifdef NTSE_KEYVALUE_SERVER
static int	ntse_keyvalue_port = 0;			/** keyvalue����Ķ˿� */
static int	ntse_keyvalue_servertype = 0;	/** keyvalue�������� */
static int	ntse_keyvalue_threadnum = 0;	/** ��keyvalue������Ҫ�߳�ʱ�������߳���Ŀ */
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
	// NTSEȫ��ͳ����Ϣ�������߳�˽�е�
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

/** ntse_command���ü�麯�������������ô����Ƿ���ȷ
 *
 * @param thd ����������߳�
 * @param var ntse_command����
 * @param save ��ͨ����飬��������ʱ�������ڽ��������ڵ��ø��º�����ֵ�洢�ڴˣ�
 *    �ڴ�������߳�˽�д洢�з��䣬ʹ��������ʱ�Զ��ͷ�
 * @param value �û�ָ����ֵ
 * @return ͨ����飬��������ʱ����0�����򷵻�1
 */
static int check_command(MYSQL_THD thd, struct st_mysql_sys_var *var,
						 void *save, struct st_mysql_value *value) {
	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	int len = 0;
	const char *cmd = value->val_str(value, NULL, &len);
	// ��֤����������˳�����
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
	//��֤�����Connection���������״̬���ϲ�һ��
	thdInfo->m_conn->setTrx(*((bool *)var_ptr));
	//������е�״̬��ı��Ŀ��״̬һ�£�����Ҫ�ı�
	if (thdInfo->m_conn->isTrx() == isTrx) {
		return;
	} else if (!isTrx && thdInfo->m_trx != NULL && thdInfo->m_trx->isTrxStarted()) {
		//�����л��ɷ����񣬽��������ϵ�δ�ύ�������commit
		//ha_tnt::tnt_commit_trx(tnt_hton, thd, true);
		//�����л��ɷ����񣬱��뱣֤��ǰ������û�л�Ծ����
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "This Connection has an active transaction, you must commit/rollback it");
		return;
	} else if (thdInfo->m_inLockTables) {
		//�л�����״̬���뱣֤����lock tables���֮��
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "This Connection is in lock tables, you must unlock tables");
		return;
	}
	thdInfo->m_conn->setTrx(isTrx);
	*((bool *)var_ptr) = isTrx;
}

static void update_tnt_mindex_reclaim_hwm(MYSQL_THD thd, struct st_mysql_sys_var *var,
										  void *var_ptr, const void *save) {
	ulong hwm = *((ulong *)save);
	// ���hwmС��lwm���״�
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
	// ���lwm����hwm���״�
	if (lwm > tnt_db->getTNTConfig()->m_reclaimMemIndexHwm) {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "low water mark must not larger than high water mark");
		return;
	}
	tnt_db->getTNTConfig()->m_reclaimMemIndexLwm = lwm;
	*((ulong *)var_ptr) = lwm;	
}

/** ����ULONG����ֵ���͵Ķ��壬���һ��������blk_size������庬��Ӧ���ǣ�
 * �����ⲿ���õĲ���N�Ƕ��٣�����ת��ΪN - N % blk_size��Ϊ����Ĳ���ֵ
 * �����ϣ���ܵ����֡����롱��Ӱ�죬��������Ϊ0
 */

// TNT ������ϵͳ����
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
						 "��ʾTNT��ǰ�߳��Ƿ�֧��MySQL�Ķ��׶��ύ",
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


// NTSE ������ϵͳ����
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
	// ��δ���ο�update_func_str����
	char *cmd = *((char **)save);

	TNTTHDInfo *thdInfo = checkTntTHDInfo(thd);
	thdInfo->m_cmdInfo->setCommand(cmd);
	cmd_exec->doCommand(thdInfo, thdInfo->m_cmdInfo);

	if (thdInfo->m_cmdInfo->getStatus() == CS_FAILED) {
		push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, "%s", thdInfo->m_cmdInfo->getInfo());
		// ����û����my_printf_error����Ϊ�������޷����ز���ʧ�ܣ�����ϲ���ִ�н���ʱ��assert�����־λ
		// ���ã���������my_printf_error��������˱�־λ
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
 * ntse_compress_sample_strategy˽���������ò�����麯�� 
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
 * ntse_compress_sample_strategy˽���������ò������º��� 
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
 * ��ȡ������Ϣ
 *
 * @return ������Ϣ
 */
TNTTHDInfo* getTntTHDInfo(THD *thd) {
	return *((TNTTHDInfo **)thd_ha_data(thd, tnt_hton));
}

/**
 * ����������Ϣ
 *
 * @param THD ����
 * @param info ������Ϣ
 */
void setTntTHDInfo(THD *thd, TNTTHDInfo *info) {
	*((TNTTHDInfo **)thd_ha_data(thd, tnt_hton)) = info;
}

/**
 * ���������Ϣ�Ƿ����ã���û���򴴽�
 *
 * @param thd THD����
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

// public�������ṩ��mysql�ϲ����
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
 * ����TNT�洢����ִ��ALTER TABLEʱ������
 *
 * @param TNT�洢����ִ��ALTER TABLEʱ������
 */
uint ha_tnt::alter_table_flags(uint flags) {
	ftrace(ts.mysql, tout << this << flags);
	// Inplace������ɾ������
	// Լ����Ŀǰ�޷�Inplace����Unique��Primary Index���˻�������ʽ
	uint r = HA_INPLACE_ADD_INDEX_NO_READ_WRITE;
	r |= HA_INPLACE_ADD_INDEX_NO_WRITE;
	r |= HA_INPLACE_DROP_INDEX_NO_READ_WRITE;
	// r |= HA_ONLINE_ADD_PK_INDEX_NO_WRITES;
	r |= HA_INPLACE_DROP_PK_INDEX_NO_READ_WRITE;
	// r |= HA_ONLINE_ADD_UNIQUE_INDEX_NO_WRITES;
	r |= HA_INPLACE_DROP_UNIQUE_INDEX_NO_READ_WRITE;
	return r;
}

/** ����ntse_xxx֮��Ĳ����Ϸ��ԣ���ntse��ʼ��ʱ����
 * ����в������Ϸ���������ʾ�������˳�����
 */
void ha_tnt::rationalConfigs() {
	// �����־�����ĺϷ���
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
* TNT�洢�����ʼ��
* @param p	TNT�洢����ʵ��
* @return �ɹ�����0��ʧ�ܷ���1
*/
int ha_tnt::init(void *p) {
	DBUG_ENTER("ha_tnt::init");

	// ����ʼ�����������Ƿ�ok
	rationalConfigs();

	GlobalFactory::getInstance();
	Tracer::init();

	tnt_hton = (handlerton *)p;

	// ����TNT��ȫ����ֲ�����
	tnt_version = new char[50];
	System::snprintf_mine(tnt_version, 50, "%s r%d", TNT_VERSION, TNT_REVISION);
	
	tnt_config.setTntBasedir(".");
// 	tnt_config.setTntDumpdir(tnt_dump_dir);
// 	tnt_config.setTntDumpReserveCnt(tnt_dump_reserve_cnt);
	set_buf_page_from_para(&tnt_config.m_tntBufSize, &tnt_buf_size);
	tnt_config.m_txnLockTimeout	= tnt_lock_timeout;	
	tnt_config.m_maxTrxCnt = tnt_max_trx_cnt;
	tnt_config.m_verifyAfterRec	= (bool)ntse_verify_after_recover;	// Verify after recover��ʱ����NTSE������
	tnt_config.m_trxFlushMode = (TrxFlushMode)tnt_trx_flush_mode;
	// purge�����������
	tnt_config.m_purgeInterval	= tnt_purge_interval;
	// dump�����������
	tnt_config.m_dumpInterval = tnt_dump_interval;

/*
	tnt_config.m_purgeThreshold	= tnt_purge_threshold;
	tnt_config.m_purgeEnough	= tnt_purge_enough;
	tnt_config.m_autoPurge		= (bool)tnt_auto_purge;
	tnt_config.m_purgeBeforeClose = (bool)tnt_purge_before_close;
	tnt_config.m_purgeAfterRecover = (bool)tnt_purge_after_recover;
	// dump�����������
	tnt_config.m_dumpInterval	= tnt_dump_interval;
	tnt_config.m_dumponRedoSize	= tnt_dump_on_redo_size;
	tnt_config.m_dumpBeforeClose= (bool)tnt_dump_before_close;
	tnt_config.m_autoDump		= (bool)tnt_auto_dump;
*/
	// version pool�������
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


	// ����NTSE��ȫ����ֲ�����
	ntse_version = new char[50];
	System::snprintf_mine(ntse_version, 50, "%s r%d", NTSE_VERSION, NTSE_REVISION);

	Config& ntse_config = tnt_config.m_ntseConfig;

	ntse_config.setBasedir(".");	// ��MySQL���ʹ��ʱֻ������Ϊ.����ΪMySQL��datadir
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
	//ntse_config.m_enableMmsCacheUpdate = true;	// Ϊ������ȷ�ָ�,������true,�μ�Bug:QA35421
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
	//��ʼ��ntse_handler
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

	// ��ʼ��ȫ�ֲ���
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

	// ����һ���ٵ��������ֻ��Ҫ���������������Ҫ��ʼ
	dummy_trx = tnt_db->getTransSys()->allocTrx();

	DBUG_RETURN(0);
}

/**
* �ر�TNT�洢����
* @param p	TNT�洢����ʵ��
* @return �ɹ�����0��ʧ�ܷ���1
*/
int ha_tnt::exit(void *p) {
	ftrace(ts.mysql, tout << p);
	DBUG_ENTER("ha_tnt::exit");
	//�ȵ���ntse_handler exit
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
	// TODO����ʱ��֪�����ʵ����TNT���Ƿ��Ծ���Ҫ
	GlobalFactory::freeInstance();

	// �ͷ�ȫ�ֲ���
	delete prepareCommitMutex;

	DBUG_RETURN(0);
}


/**
 * ��handler������THD����
 *
 * @param thd THD����
 * @param name ��ʶ��ǰTHD�����ĸ�����attach��
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
 * ����handler��THD�Ĺ���
 */
void ha_tnt::detachFromTHD() {
	assert(m_thd);

	m_thd	= NULL;
	m_conn	= NULL;
	delete	[]m_name;
	m_name	= NULL;
}

/** �ж��ܲ��ܽ������߱�ģʽ�޸�
 * @param create_info �µı�ģʽ
 * @param table_changes  ��ӳ�¾ɱ�ģʽ֮������Ĳ���
 * @return �ܷ�������߱�ģʽ�޸�
 */
bool ha_tnt::check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes) {
	ftrace(ts.mysql, tout << this << create_info << table_changes);

	if (table_changes != IS_EQUAL_YES)
		return COMPATIBLE_DATA_NO;
	
	// ���auto increment���Ƿ����仯
	// Ŀǰ��auto increment�в�֧��Inplace�޸�
	if ((create_info->used_fields & HA_CREATE_USED_AUTO) && create_info->auto_increment_value != 0)
		return COMPATIBLE_DATA_NO;
	return COMPATIBLE_DATA_YES;
}

/**
 * ���ر�����
 * @return ���Ƿ���TNT
 */
const char* ha_tnt::table_type() const {
	return "TNT";
}

/** ����ָ������������
 * @param inx ���еĵڼ�����������0��ʼ��š�
 * @return �������ͣ�TNTʹ��B+�����������Ƿ���"BTREE"
 */
const char* ha_tnt::index_type(uint inx) {
	return "BTREE";
}

/**
 * �洢����ʹ�õ���չ����TNT������NTSE������ȫһ�¡�
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
* �洢����ʹ�õĿ���չ������NTSE����һ�¡�
*/
static const char *ha_tnt_nullExts[] = {
	NullS, NullS
};


/**
 * ���ش洢ʹ�õ��ļ���չ������
 *
 * @return TNTʹ�õ��ļ���չ������
 */
const char** ha_tnt::bas_ext() const {
	try {
		/** ���bas_ext�ĵ����Ƿ���repair table ... use_frm������ */
		if (m_thd && TT_USEFRM == m_thd->lex->check_opt.sql_flags && SQLCOM_REPAIR == m_thd->lex->sql_command) {
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "'repair table ... use_frm' statement is not supported by tnt");
		}

		return ha_tnt_exts;
	} catch (NtseException &e) {
		/**
		 *	����bas_extΪconst��Ա��������reportError��const����Ҫǿ��ת��
		 */
		const_cast<ha_tnt* const>(this)->reportError(e.getErrorCode(), e.getMessage(), NULL, true);

		return ha_tnt_nullExts;
	}
}

/**
 * ���ر�ʶ�洢����������һϵ�б�־
 *
 * @return ��ʾTNT�洢���������ı�־
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
	// Ŀǰ��TNT��֧����伶binlog����
	// flags |= HA_BINLOG_STMT_CAPABLE;

	return flags;
}

/**
 * ���ر�ʶ�洢��������ʵ��������һϵ�б�־
 *
 * @param inx �ڼ�������
 * @param part ����ɨ����������ǰ���ٸ���������
 * @param all_parts �����Ƿ����������������
 * @return ��ʶTNT�洢��������ʵ�������ı�־
 */
ulong ha_tnt::index_flags(uint inx, uint part, bool all_parts) const {
	ulong flags = HA_READ_NEXT;		// �������Ժ���ɨ��
	flags |= HA_READ_PREV;			// ��������ǰ��ɨ��
	flags |= HA_READ_ORDER;			// �������������
	flags |= HA_READ_RANGE;			// ֧��������Χɨ��
	flags |= HA_KEYREAD_ONLY;		// ����ֻɨ�����������ʱ�

	return flags;
}


/**
 * ����NTSE�洢����֧�ֵ�����¼���ȣ������������
 * ��һ���Ƹ��ݱ���������󳤶ȣ�ʵ���ܴ洢����󳤶Ȼ����С
 *
 * @return TNT�洢����֧�ֵ�����¼���ȣ������������
 */
uint ha_tnt::max_supported_record_length() const {
	return Limits::DEF_MAX_REC_SIZE;
}

/**
 * ����һ�ű��������ж��ٸ�����
 *
 * @return TNT��һ�ű��������ж��ٸ�����
 */
uint ha_tnt::max_supported_keys() const {
	return Limits::MAX_INDEX_NUM;
}

/**
 * ����������������������
 *
 * @return TNT��������������������
 */
uint ha_tnt::max_supported_key_parts() const {
	return Limits::MAX_INDEX_KEYS;
}

/**
 * ����NTSE�洢����֧�ֵ��������������
 *
 * @return TNT�洢����֧�ֵ��������������
 */
uint ha_tnt::max_supported_key_length() const {
	return DrsIndice::IDX_MAX_KEY_LEN;
}

/** ����NTSE�洢����֧�ֵ�������������Գ���
 * @return TNT�洢����֧�ֵ�������������Գ���
 */
uint ha_tnt::max_supported_key_part_length() const {
	return DrsIndice::IDX_MAX_KEY_PART_LEN;
}

/** �жϵ�ǰ�ǲ���Slave SQL�̵߳Ĳ���
 * @return true��ʾ��Slave SQL�̣߳�false��ʾ����
 */
bool ha_tnt::isSlaveThread() const {
	return isSlaveThread(m_thd);
}


/** �жϵ�ǰ�ǲ���Slave SQL�̵߳Ĳ���
 * @return true��ʾ��Slave SQL�̣߳�false��ʾ����
 */
bool ha_tnt::isSlaveThread(THD *thd) const {
	return (thd != NULL && thd->slave_thread);
}

/**
 * ���ر�ɨ����ۡ�Ŀǰʹ�ô洢����ʾ���е�Ĭ��ʵ��
 * @post Ĭ��ʵ�ֵĴ���ƫ��Ӧ�ø��ݱ��ҳ��������
 * @return ��ɨ�����
 */
double ha_tnt::scan_time() {
	return (double) (stats.records + stats.deleted) / 20.0+10;
}


/**
 * ���ر��������ޣ������ϲ���filesort
 * @post ������ڵ��ڱ�����ʵ����ֵ
 * @return ���¼������Ŀ
 */
ha_rows ha_tnt::estimate_rows_upper_bound() {
	TableDef *tableDef =  m_table->getNtseTable()->getTableDef();
	u16 recLen = 0;
	u64 estimate = 0;
	u64 pageNum = 0;
	ColumnDef *colDef = NULL;
	assert(m_table->getMetaLock(m_session) != IL_NO);
	// �Ƿ���ѹ������ͨ�������recFormat�жϣ�ֻ��ͨ���Ƿ���ѹ���ֵ����ж�
	if (m_table->getNtseTable()->getRecords()->hasValidDictionary()) {
		// ѹ�����޷�Ԥ����¼��Ŀ����ѹ����һ����һ�Ŵ��
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
		// ����null bitmap���ֽ���
		recLen += tableDef->m_bmBytes;
		estimate = 2 * pageNum * NTSE_PAGE_SIZE / recLen;	
	}
	return (ha_rows)estimate;
}

/**
 * ���ض�ȡָ��������¼�Ĵ��ۡ�Ŀǰʹ�ô洢����ʾ���е�Ĭ��ʵ��
 * @post Ĭ��ʵ�ֵĴ���δ������������ɨ���������Ǹ���ɨ�������
 * @return ��ȡ����
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
* �˺�����Ҫ���ڷ���TNT��ĸ���ͳ����Ϣ������MySQL��ѯ�Ż�ʹ��
* ����֮�⣬������������һЩ������Ϣ
*  
* @param flag ��Ҫ���ص���Ϣ����
* @return ����return 0
*/
int	ha_tnt::info(uint flag) {
	ftrace(ts.mysql, tout << this << flag);

	DBUG_ENTER("ha_tnt::info");

	// TODO��Info������ʱֱ�Ӳ���NTSE�ķ����������Ľ�

	Table *ntseTable = m_table->getNtseTable();

	if (flag & HA_STATUS_VARIABLE) {
		stats.records = ntseTable->getRows();
		ha_rows estimateRecords = 0;
		stats.deleted = 0;
		if (m_session && m_table->getMetaLock(m_session) != IL_NO) {
			// һ�����������������Ĳ�ѯ����ʱ�Ǽ��˱�����
			stats.data_file_length = ntseTable->getDataLength(m_session);
			stats.index_file_length = ntseTable->getIndexLength(m_session, false);
			// ���ն�ҳ������������ȵõ�һ��Ԥ���ļ�¼��
			estimateRecords = ntseTable->getRecords()->getHeap()->getUsedSize() / ntseTable->getTableDef()->m_maxRecSize;
		} else {
			// SHOW TABLE STATUSʱû�мӱ���
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
					// ���ն�ҳ������������ȵõ�һ��Ԥ���ļ�¼��
					estimateRecords = ntseTable->getRecords()->getHeap()->getUsedSize() / ntseTable->getTableDef()->m_maxRecSize;
					m_table->unlockMeta(session, IL_S);
				} catch (NtseException &) {
					// �����쳣
				}
				if (session != m_session) {
					tnt_db->getNtseDb()->getSessionManager()->freeSession(session);
					detachFromTHD();
				}
			}
		}
		stats.delete_length = 0;
		stats.check_time = 0;
		// Ϊ�˷�ֹ����ɾ������ʹ��ҳ�������ʱ��ɵ�������ҳ������ڼ�¼�����Ӷ�����ִ�мƻ��ߴ�
		// ������Ҫȡ����ͳ��ֵ�� ���ļ���С/����¼���� �Ľϴ�ֵ
		stats.records = max(stats.records, estimateRecords);
		if (stats.records == 0) {
			stats.mean_rec_length = 0;
		} else {
			stats.mean_rec_length = (ulong) (stats.data_file_length / stats.records);
		}
	}
	if (flag & HA_STATUS_ERRKEY) {
		// REPLACE��INSERT/UPDATE��������ڻ�ȡ���³�ͻ�ļ�¼��RID
		if (m_iuSeq) {
			assert(m_session);
			// TNT���������棬��������������Ѿ�����������Ҫcache��Handler���棬����rowid���ɣ��´η���һ������
			// TNT�Ƕѱ���˱���RowId����
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
 * ���洢����һЩ��ʾ
 *
 * @param operation ��ʾ��Ϣ
 * @return ���Ƿ���0
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
 * MySQL�ϲ������һ�����ɴ洢��������Խ�Ҫִ�е������Ҫ��ʲô���ı���(MySQL�ϲ���)��
 * ����֧�������Ĵ洢�������ѡ�񽫱��ϵ�д����Ϊ�����ȣ�������ȫ��Ҫ
 * MySQL�ı������Լ�ʵ�ֱ��������߼Ӷ�������������MERGE�洢���棩��
 *
 * �ڲ�ѯ������;��ǰ����ʱMySQLҲ�������һ������������SELECTʱ������Щ
 * ֻ����һ����¼��const���������������ͷ�������ʱ������ñ�������
 *
 * ע: table->in_use����ָ����һ���̣߳���mysql_lock_abort_for_thread()�е���ʱ��
 *
 * ��ش���: thr_lock.c, get_lock_data()
 *
 * @param thd ��ǰ����
 * @param to ������������ڴ洢������
 * @param lock_type ��ģʽ
 * @return ����������βָ��
 */
THR_LOCK_DATA** ha_tnt::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
	//���������Ӳ����������߷��������ntse����
	if (!THDVAR(thd, is_trx_connection)) {
		return ntse_handler::store_lock(thd, to, lock_type);
	}

	// 0. ��lock_type == TL_IGNORE�ó�����������
	// ���ȣ�����Ĵ����߼�����TL_IGNORE�ų�����
	// ��Σ�TL_IGNOREģʽ�£�attachToTHD��������������
	// ���TL_IGNORE���ֵ������ǣ�JOIN::optimize -> store_lock -> external_lock -> store_lock(TL_IGNORE);�����䣺select * from *** where id = **;
	if (lock_type == TL_IGNORE) {
		*to++= &m_mysqlLock;

		return(to);
	}
	
	TrxIsolationLevel isoLevel;

	// 1. ��ȡ������������ĸ��뼶���Լ��Եײ��м�¼�ļ���ģʽ
	attachToTHD(thd, "ha_tnt::store_lock");

	m_trans = getTransForCurrTHD(thd);

	if (lock_type != TL_IGNORE && m_trans->getTableInUse() == 0) {
		isoLevel = getIsolationLevel((enum_tx_isolation) thd_tx_isolation(thd));
		m_trans->setIsolationLevel(isoLevel);

		if (isoLevel <= TRX_ISO_READ_COMMITTED) {
			// �����뼶��С��READ_COMMITTED���������伶ReadView
			// TODO:...
		}
	}

	const bool in_lock_tables = thd_in_lock_tables(thd);
	enum_sql_command sql_command = thd->lex->sql_command;

	// 2. ���������������TNT����ļ���ģʽ

	if (sql_command == SQLCOM_DROP_TABLE) {
		// 2.1 drop table�������˴������ü���ģʽ

	} else if ((lock_type == TL_READ && in_lock_tables)
		   || (lock_type == TL_READ_HIGH_PRIORITY && in_lock_tables)
		   || lock_type == TL_READ_WITH_SHARED_LOCKS
		   || lock_type == TL_READ_NO_INSERT
		   || (lock_type != TL_IGNORE && sql_command != SQLCOM_SELECT)) {
				
				// 2.2 MySQL�������lock tables ... read local
			    // ���� ...
			    // ���� ��ǰ�������select ... in share mode
			    // ���� ��ǰ�������insert into ... select ...��ͬʱBinlog��Ҫʹ��lock read��
			    //		��ǰ�������lock tables ... read
			    // ���� ��select��䣬��TNT��������ģʽ��ͬʱ��ģʽ��external_lock�����п��ܻᱻ��ǿ

				isoLevel = m_trans->getIsolationLevel();
				if (isoLevel <= TRX_ISO_READ_COMMITTED
					&& (lock_type == TL_READ || lock_type == TL_READ_NO_INSERT)
					&& (sql_command == SQLCOM_INSERT_SELECT
					|| sql_command == SQLCOM_REPLACE_SELECT
					|| sql_command == SQLCOM_UPDATE
					|| sql_command == SQLCOM_CREATE_TABLE)) {
						
						// 2.2.1 ����ǰ�ĸ��뼶����READ_COMMITTED
						// ͬʱ MySQL���ڽ��еĲ�����insert into ... select ���� replace into ... select 
						//		����update ... = (select ...) ����create table ... select ...
						// ͬʱ��Щ������select��䶼û��ָ��for update (in share mode)����ôֱ��ʹ�ÿ��ն�
						m_selLockType	= TL_NO;
		} else if (sql_command == SQLCOM_CHECKSUM) {

			// 2.2.2 ����checksum table ...ʹ�ÿ��ն�
			m_selLockType	= TL_NO;
		} else {

			// 2.2.3 ������������ʱ����Ϊ���м�S������ģʽ��external_lock�����п��ܻᱻ����ΪX��
			m_selLockType	= TL_S;
		}
	} else if (lock_type != TL_IGNORE) {

		// 2.3 ���ڿ��ܵ���X������external_lock������ͳһ����
		m_selLockType	= TL_NO;
	}

	// 3. ���ݲ�ͬ�Ĳ������ͣ��ʵ���MySQL�ϲ����ģʽ����(��������)
	if (lock_type != TL_IGNORE &&m_mysqlLock.type == TL_UNLOCK) {

		if (lock_type == TL_READ && sql_command == SQLCOM_LOCK_TABLES) {
			// 3.1 ����ǰ��lock tables ... read local������ģʽ��������InnoDB/MyISAM�������Ϊ����һ��
			lock_type = TL_READ_NO_INSERT;
		}
		
		if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE)
		    && !(in_lock_tables
			 && sql_command == SQLCOM_LOCK_TABLES)
		    && sql_command != SQLCOM_TRUNCATE
		    && sql_command != SQLCOM_OPTIMIZE
		    && sql_command != SQLCOM_CREATE_TABLE) {

				// 3.2 ����ǰ����䲻��lock/truncate/optimize/create table
				// ��ô���Խ��������Ƶ�д��ת��Ϊ֧�ֲ���д��
				lock_type = TL_WRITE_ALLOW_WRITE;
		}

		if (lock_type == TL_READ_NO_INSERT && sql_command != SQLCOM_LOCK_TABLES) {
			// 3.3 ����MySQL�ϲ�ָ����TL_READ_NO_INSERT(insert into t1 select ... from t2��
			// ��ʱ��ָ����ģʽ)���ʵ�������ģʽ����֤��t2�Ͽ���ͬʱ����insert����
			// ͬʱ������stored procedure call����ʱҲ���Խ���ģʽ��
			// lock tables ... �﷨��ģʽ���ɸı䣻
			lock_type = TL_READ;
		}
		m_mysqlLock.type = lock_type;
	}

	*to++= &m_mysqlLock;
	
	// ��ɨ�����ģʽ�����Ƶ�TNTOpInfo�У�����ͨ���������ݵ��²�
	m_opInfo.m_selLockType = m_selLockType;

	m_beginTime = System::microTime();

	detachFromTHD();

	return(to);
}

/**
 * һ������£��κ���俪ʼ����һ�ű�֮ǰ���������һ����֪ͨ�洢����MySQL�ϲ�׼��������
 * ��������MySQL����ô˺�����֪ͨ�洢����MySQL�ϲ��Ѿ���ɽ�������
 * ����������LOCK TABLES/UNLOCK TABLES��������LOCK TABLESʱ����store_lock��external_lock������
 * UNLOCK TABLESʱ�������ڼ䲻�ٻ����store_lock��external_lock������
 *
 * @param thd ��ǰ����
 * @param lock_type ΪF_UNLCK��ʾ�������������Ϊ��д��
 * @return �ɹ�����0��������ʱ����HA_ERR_LOCK_WAIT_TIMEOUT��ȡ�����Ự����HA_ERR_NO_CONNECTION
 */
int ha_tnt::external_lock(THD *thd, int lock_type) {
	DBUG_ENTER("ha_tnt::external_lock");
	int code = 0;
	if (lock_type == F_UNLCK) {
		DBUG_SWITCH_NON_CHECK(thd, ntse_handler::external_lock(thd, lock_type));

		// ����������ʱ�����
		if (m_iuSeq) {
			m_table->freeIUSequenceDirect(m_iuSeq);
			m_iuSeq = NULL;
		}
		
		// TODO������жϵ������ǣ�
		if (!m_thd)	// mysql_admin_table��û�м������Ƿ�ɹ�
			DBUG_RETURN(code);

		assert(m_session);

		endScan();

		// ���������еı���Ϣ
		assert(m_thd == thd);
		m_trans = getTransInTHD(thd);
		assert(m_trans != NULL);

		m_trans->decTablesInUse();
		m_opInfo.m_mysqlHasLocked = false;

		if (m_trans->getTableInUse() == 0) {
			// m_trans->setTablesLocked(0);

			if (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
				// ���ն�����MySQL�ϲ㲻�����commit�������Ҫ��external_lock�������ύ��
				// ��ǰ������(select lock in share mode/select for update)��ͬ����external_lock������
				// �ύ���񣬵���MySQL�ϲ�����������ha_autocommit_or_rollback������
				if (m_trans->isTrxStarted()) {
					// ��֤thd�е������뵱ǰhandler�е�����һ��һ��
					// ͬʱm_trans�ռ䣬ͬ����tnt_commit_trx���ͷ�
					assert(m_trans == getTransInTHD(thd));

					tnt_commit_trx(tnt_hton, thd, true);

					m_trans = NULL;
				}

				// ����ǰm_transδ��ʼ����ֱ�ӻ��մ�����
				// tnt_db->getTransSys()->releaseTrx(m_trans);
				// m_trans = NULL;
			} else {
				// ��ǰ����autocommit���񣬸�������ĸ��뼶�𣬿ɽ��ɼ��Դ����񼶽���Ϊ��伶
				// TODO:
				
			}
		}

		// ���������֮���ͷű��ϵ�MetaLock�����ر�session
		// TODO��ÿ��������ʱ���ر�session�Ƿ���ʣ�
		// Ŀǰ��ʱ��ôʵ�֣����������⣬�Ȳ���֮�󣬸������ܷ�������
		if (m_table->getMetaLock(m_session) != IL_NO)
			m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));

		tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
		m_session = NULL;

		detachFromTHD();

	} else {
		attachToTHD(thd, "ha_tnt::external_lock");
		// ����Ự
		assert(!m_session);
		m_session = createSession("ha_tnt::external_lock", m_conn, 100);
		if (!m_session) {
			detachFromTHD();
			DBUG_RETURN(HA_ERR_NO_CONNECTION);
		}
		// �Ե�ǰ����MetaLock
		m_table->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
		DBUG_SWITCH_PRE_CHECK_GENERAL(thd, ntse_handler::external_lock(thd, lock_type), code);
		if (code != 0) {
			m_table->unlockMeta(m_session, IL_S);
			tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			detachFromTHD();
			DBUG_RETURN(code);
		}

		// ����tnt��俪ʼʱ�����
		// ������俪ʼ��ʶ��TNTTableģ����Խ��˱�ʶ��m_selLockType��ϣ�
		// �ж��ǶԱ�������������Ǵ���ReadView
		m_opInfo.m_sqlStatStart = true;
		m_opInfo.m_mysqlOper	= true;

		// ��ȡ����
		m_trans = getTransForCurrTHD(thd);
		m_session->setTrans(m_trans);

		// ��ͬ��NTSE��table lock����Ҫ�ڴ˴���ʾ����
		// ���β�����Ҫд������Ϊselect�����Ӧ�������
		// update table ... ������ select *** for update
		if (lock_type == F_WRLCK) {
			m_selLockType = TL_X;
		}

		// ����ǰ��Serializable���뼶��Ĳ�ѯ����ʹ�õ�ǰ�����Լ�¼��S��
		if (m_trans->getIsolationLevel() == TRX_ISO_SERIALIZABLE
			&& m_selLockType == TL_NO
			&& thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
				m_selLockType = TL_S;
		}

		m_opInfo.m_selLockType = m_selLockType;

		// ����ǰ��ѯ��Ҫ���м�¼���������ж��Ƿ���Ҫ����lock tables�߼�
		if (m_opInfo.m_selLockType != TL_NO) {
			if ((thd_sql_command(thd) == SQLCOM_LOCK_TABLES
				// && THDVAR(thd, table_locks)
				&& thd_test_options(thd, OPTION_NOT_AUTOCOMMIT)
				&& thd_in_lock_tables(thd)) 
				|| thd_sql_command(thd) == SQLCOM_ALTER_TABLE 
				|| thd_sql_command(thd) == SQLCOM_CREATE_TABLE) {
					// �ӱ�������ʱ�ı���ģʽ����Ϊselect lock type������Ҫת��
					try {
						// ֱ�Ӷ�ȡm_id����Ϊ�Ѿ�����meta S��
						TableId tid = m_table->getNtseTable()->getTableDef()->m_id;
						
						m_trans->lockTable(m_opInfo.m_selLockType, tid);

						// �ӱ����ɹ�֮�����õ�ǰ������Lock Tables�����֮��
						m_trans->setInLockTables(true);
					} catch (NtseException &e) {
						// �ӱ���ʧ�ܣ��ͷ��ѳ�����Դ��������
						// TODO����ʱ��������ͣ������ǳ�ʱ��Ҳ��������������ʱ��Ҫrollback��ǰ����
						reportError(e.getErrorCode(), e.getMessage(), thd, false);

						m_table->unlockMeta(m_session, IL_S);

						// ������NTSEͬ������TNT����֮����MySQL�ϲ����rollback��ɽ�������ع�
						// ��˴�ʱ����Ҫ�ع����񣬵�������Session������Handler�ϣ������Ҫ����
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

		// ���ִ�гɹ�external_lock֮�󣬽����²�������Ϊtrue
		// ����start_stmt���������ֵ�ǰ��temp table��������table
		m_opInfo.m_mysqlHasLocked = true;
	}

	DBUG_RETURN(0);
}

/** ��LOCK TABLES����������MySQL����俪ʼʱ����ô˺�����������ʱ�������
 *
 * @param thd ����
 * @param lock_type �Ա�ӵ���
 * @return ���Ƿ���0
 */
int ha_tnt::start_stmt(THD *thd, thr_lock_type lock_type) {
	DBUG_ENTER("ha_tnt::start_stmt");
	
	// ��sqlִ�й����У�thd�����ı䣬��ôֱ�ӱ���ĿǰTNT��֧��
	if (thd != m_thd)
		DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "THD changed during sql processing", thd, false));

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::start_stmt(thd, lock_type));

	// ��俪ʼ�����ÿ�ʼ��ʶ��TNTTableģ����Խ��˱�ʶ��m_selLockType��ϣ�
	// �ж��ǶԱ�������������Ǵ���ReadView
	m_opInfo.m_sqlStatStart = true;
	m_opInfo.m_mysqlOper	= true;

	// ��ȡ����
	m_trans = getTransForCurrTHD(thd);

	if (!m_opInfo.m_mysqlHasLocked) {
		// ��δ���ô˲�����˵����ǰhandler��Ӧ����һ����lock tables����ڲ���������ʱ��
		// ��������ʱ��MySQL��������external_lock����˱�����м�x������Կ��ܵĸ���
		m_selLockType = TL_X;

	} else if (m_trans->getIsolationLevel() != TRX_ISO_SERIALIZABLE
		&& thd_sql_command(thd) == SQLCOM_SELECT
		&& lock_type == TL_READ) {
			// ���ڷ���ʱ���ֻ��������ʹ��һ�¶�
			m_selLockType = TL_NO;

	} else {
		// ����������һ�¶����������ּ���ģʽ���伴��

	}

	m_opInfo.m_selLockType = m_selLockType;

	// ������ע�ᵽMySQL 2PC��
	tntRegisterTrx(tnt_hton, thd, m_trans);

	DBUG_RETURN(0);
}

/**	��TNT����ע�ᵽMySQL��XA�����У�ͬһ���񣬿��ظ�ע��
*	@param	hton 
*	@param	thd	 MySQL��thread
*	@param	trx	 ��ǰTNT����
*/
void ha_tnt::tntRegisterTrx(handlerton *hton, THD *thd, TNTTransaction *trx) {
	// ����ע�ᵽstatement������
	trans_register_ha(thd, false, hton);

	// ��Ϊ��һ��ע�ᣬ���ҵ�ǰ����autocommit������Ҫע�ᵽall����
	if (!trx->getTrxIsRegistered()
		&& thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
			trans_register_ha(thd, true, hton);
	}

	// ������ע��״̬
	trxRegisterFor2PC(trx);
}

/**	��ʶһ��TNT�����Ѿ�ע�ᵽMySQL��XA������
*	
*	@param trx	TNT����
*
*/
void ha_tnt::trxRegisterFor2PC(TNTTransaction *trx) {
	trx->setTrxRegistered(true);
}

/**	��ʶһ��TNT�����MySQL��XA������ȡ��ע��
*	
*	@param trx	TNT����
*
*/
void ha_tnt::trxDeregisterFrom2PC(TNTTransaction *trx) {
	trx->setTrxRegistered(false);
	trx->setCalledCommitOrdered(false);
}

/**	���ϲ㶨��ĸ��뼶��ת��ΪTNT�����������뼶��
*	@param	iso	MySQL�ϲ㶨��ĸ��뼶��
*	@return	����TNT���涨���������뼶��
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

/**	��ȡ��ǰthd��Ӧ�����������񲻴������½�
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

			// ���½���������MySQL��THD��
			info->m_trx = trx;
		} catch (NtseException &e){
			// ��ǰϵͳ���й��������Ŀǰ����Ϊֱ���˳�����
			reportError(e.getErrorCode(), e.getMessage(), thd, true);
			return NULL;
		}
	}

	// TODO��TNT��ʵ�֣���ʱ�ڴ��������ʱ��ֱ�ӽ�����ʼ
	bool innerTrx = false;
	if (thd_sql_command(thd) == SQLCOM_CREATE_TABLE || thd_sql_command(thd) == SQLCOM_CREATE_INDEX
		|| thd_sql_command(thd) == SQLCOM_ALTER_TABLE || thd_sql_command(thd) == SQLCOM_TRUNCATE 
		|| thd_sql_command(thd) == SQLCOM_DROP_INDEX)
		innerTrx = true;
	trx->startTrxIfNotStarted(info->m_conn, innerTrx);
	// ������ע�ᵽMySQL 2PC
	tntRegisterTrx(tnt_hton, thd, trx);
	// ������͵�ǰthd��
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

/**	��ȡ��ǰthd��Ӧ����������ض�����
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
 * �򿪱����ϲ����ĳ�ű�֮ǰ���������һ����Ҫ��洢��������ű�
 * MySQL�Ỻ��handlerʵ����һ��handlerʵ����ͬһʱ��ֻ���ڲ���һ�ű�
 * ��handlerʵ�����ڲ���һ�ű�֮ǰ�������open�򿪱�handlerʵ����
 * �رգ���ת�����ڲ�����һ�ű�ʱ�������close�رձ�ת�����ڲ�����һ��
 * ��ʱ�����ٵ���open����һ�ű����һ��handlerʵ���ڲ�ͬʱ�̿�������
 * ������ͬ�ı�
 *
 * @param name ���ļ�·������������׺����
 * @param mode ���������֪��ʲô��˼����InnoDB��ʵ���ǲ�ʹ����һ����
 * @param test_if_locked �������������Ҳûʲô�ã���ha_open��˵��������
 *  ֻ��Ҫ����HA_OPEN_WAIT_IF_LOCKED�Ƿ����ã���û�����ڱ�����ʱ
 *  ���ȴ�����InnoDBһ����������һ������
 * @return �ɹ�����0��ʧ�ܷ��ش�����
 */
int ha_tnt::open(const char *name, int mode, uint test_if_locked) {
	DBUG_ENTER("ha_tnt::open");

	attachToTHD(ha_thd(), "ha_tnt::open");

	// TODO: ����openTable�Ҳ�����ʧ�ܣ����������޸ı�ģʽ������ʧ�ܵ��±�
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
				// �����޸Ķ�ȡ��ʱ�򣬿϶�ֻ��һ���Ự���߳���ʹ�ã�
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
 * ����handler״̬
 *
 * @return ���Ƿ���0
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
 * �ر�handler��ǰ�����õı�
 *
 * @return �ɹ�����0��ʧ�ܷ���1������TNT���Ƿ���0
 */
int ha_tnt::close(void) {
	assert(!m_session && m_table);
	DBUG_ENTER("ha_tnt::close");
	ntse_handler::close();
	THD *thd = ha_thd();

	if (!thd) {
		// handler��ʱ��δ�ù�����ʱû��THD
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
 * ���ݱ��������Ҷ�Ӧ�Ĺ���ṹ�����а���������Ϣ��
 * ÿ��handler��ʱ���������һ�����������
 * Ϊʵ����ȷ�ı�����Ϊ��ϵͳά��һ���Ѿ��򿪵ı��Ӧ�Ĺ���ṹ
 * ��ϣ��һ�����һ�α���ʱ����һ���µĹ���ṹ�����������
 * ������һ�ṹ���������ü�����
 *
 * ��ʵ��TNT�ڲ�Ҳά���˱��򿪵ı������ڱ���ṹ��
 * ������MySQL�����е�THR_LOCK�Ƚṹ����Ҫ��������ά��һ�Ρ�
 *
 * @param table_name ����
 * @param eno OUT��ʧ��ʱ���ش����
 * @post ����򿪳ɹ���������ha_tnt��m_table��m_share�����ṹ
 * @return true��ʾ�򿪳ɹ���false��ʾ��ʧ��
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
		
		/** ����������� */
		try {
			if (mysqlTable->found_next_number_field != NULL) {
				m_table->enterAutoincMutex();

				/** ����TNT�ڲ�Ӧ�ô򿪣�ͬʱδ��ʼ���������У����ڴ˳�ʼ�� */
				if (m_table->getAutoinc() == 0) {
					m_table->lockMeta(m_session, IL_S, 1000, __FILE__, __LINE__);
					//TODO �ݲ��ӱ��������ӱ����ĺ����ʲô����
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
 * �ͷű���ṹ����ÿ��handler���ر�ʱ��ϵͳ�������һ������
 * һ������¼������ü����������ü������0���ͷű���ṹ
 *
 * @param share ����ṹ
 * @return ���Ƿ���0
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

/** �õ�TNT�ڲ���ı�ID��Ϣ
 * @return �ڲ����ID
 */
u16 ha_tnt::getTntTableId() {
	return m_table->getNtseTable()->getTableDef()->m_id;
}

/**	����Session�����ҽ�����󶨵��½���Session��
*
*	@return	�½���Session����
*/
Session* ha_tnt::createSessionAndAssignTrx(const char *name, Connection *conn, TNTTransaction *trx, int timeoutMs /* = -1 */) {
	Session *sess = tnt_db->getNtseDb()->getSessionManager()->allocSession(name, conn, timeoutMs);

	if (sess != NULL)
		sess->setTrans(trx);

	return sess;
}

/** ����session
 * @param name session������
 * @param connection  ���Ӷ���
 * @param timeoutMs ��ʱʱ��
 * @return �½�Session����
 */
Session* ha_tnt::createSession(const char *name, Connection *conn, int timeoutMs /*=-1*/) {
	return tnt_db->getNtseDb()->getSessionManager()->allocSession(name, conn, timeoutMs);
}


/**
 * ����һ��
 *
 * @param buf �����е�����
 * @return �ɹ�����0������Ψһ�Գ�ͻʧ�ܷ���HA_ERR_FOUND_DUPP_KEY��
 *   ���ڼ�¼����ʧ�ܷ�����Ӧ������
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
		// ��������
		if ((code = update_auto_increment()))
			// ���ڷ������֧�֣�ֱ�ӷ��ش�����
			DBUG_RETURN(code);
	}

	try {
		if (!m_replace) {
			// ��ʹ��INSERT IGNORE��Ψһ�Գ�ͻʱҲ��Ҫ����HA_ERR_FOUND_DUPP_KEY��
			// �����ϲ�ͳ�Ƴɹ�����ͳ�ͻ�ĸ��ж�������¼
			m_dupIdx = -1;
			if (m_table->insert(m_session, buf, &m_dupIdx, &m_opInfo, true) == INVALID_ROW_ID) {
				// Insert����Next Key����˲������������ʱ/���������
				code = HA_ERR_FOUND_DUPP_KEY;
			}
		} else {
			assert(m_ignoreDup);
			if (m_iuSeq) {
				// INSERT ... ON DUPLICATE KEY UPDATE�ڷ�����ͻ��������ҪUPDATE����ֵ��ԭ��¼���
				// �򲻻����update_row��������INSERT ... SELECT ... ON DUPLICATE ...ʱ����������
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

	// �����µ�����ֵ
	// TODO�����Բο�InnoDB������Insert����ʧ�ܵ������ֻ�в���ʧ������£���Ҫ����autoInc
	if (code == 0) {
		setAutoIncrIfNecessary(buf);
	}
	
	DBUG_RETURN(code);
}

/**
 * ����һ����¼��new_dataΪ���º�����ԣ���������auto_increment���ͻ��Զ����µ�timestamp
 * ���͵�����ֵ
 * �ڵ���rnd_next/rnd_pos/index_next֮��MySQL������������һ�����������������ļ�¼��
 * ���TNT���Ǹ��µ�ǰɨ��ļ�¼������Ҫold_data������
 *
 * @param old_data ԭ��¼���ݣ�TNT����
 * @param new_data �¼�¼����
 * @return �ɹ�����0��Ψһ��������ͻʧ�ܷ���HA_ERR_FOUND_DUPP_KEY����¼������
 * ����ԭ����ʧ��ʱ������Ӧ������
 */
int ha_tnt::update_row(const uchar *old_data, uchar *new_data) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::update_row");
	ha_statistic_increment(&SSV::ha_update_count);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::update_row(old_data, new_data));

	int code = 0;
	try {
		if (m_scan) {

			// ��������update SubRecord��ÿ�ζ��Ա�old_data��new_data
			// ֻ�����������仯���У�����Ҫ���£������Ǹ���write_set����������
			// ��ôʵ�ֵ����ƣ�
			// 1. update t1 set id = 1 where id = 1; ���ᱨUnique ��ͻ
			// 2. ��scan�У�����TNT�Ѿ�ȡ���������ļ�¼����˿�������Ա�������
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
			// TODO���˴�����ʹ��table->write_set����Ϊ���insert on duplicate key update���﷨
			// ��ʱ��write_setΪ��������У�����ʵ���ϣ����µ��п���û����ô�࣬��Ҫ�Ա�֮��ȷ����Щ����Ҫ����

			// u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			u16 *updateCols = transColsWithDiffVals(new_data, old_data, &numUpdateCols);

			bool succ = true;
			
			// �������Աȣ����º�����ǰ����ȫһ�£���ֱ�ӷ��سɹ�������Ҫ����ʵ�ʸ���
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
			// 1. �����������л��ڽ���ɨ��֮���ٵ���update_row
			// create table t1 (a int not null, b int, primary key (a)) engine=tnt;
			// create table t2 (a int not null, b int, primary key (a)) engine=tnt;
			// insert into t1 values (10, 20);
			// insert into t2 values (10, 20);
			// update t1, t2 set t1.b = 150, t2.b = t1.b where t2.a = t1.a and t1.a = 10;
			// 2. �м�������ΪSLAVEִ��UPDATE�¼�ʱ

			assert(m_lastRow != INVALID_ROW_ID);
			McSavepoint mcSave(m_session->getMemoryContext());

			u16 numReadCols;
			u16 *readCols = transCols(table->read_set, &numReadCols);
			
			m_scan = m_table->positionScan(m_session, OP_UPDATE, &m_opInfo, numReadCols, readCols, false);

			byte *buf = (byte *)alloca(m_table->getNtseTable()->getTableDef(true, m_session)->m_maxRecSize);
			try {
				NTSE_ASSERT(m_table->getNext(m_scan, buf, m_lastRow, false));
			} catch (NtseException &e) {
				// position scan �����������������״�
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

	// ������µ�����update��������Ҫ����autoInc�У�
	// insert into t (c1,c2) values (x, y) on duplicate key update ...
	// ����������autoInc�У�����Ҫ����autoInc�е�next values
	if (code == 0 
		&& thd_sql_command(m_thd) == SQLCOM_INSERT
		&& m_ignoreDup == true) {
			setAutoIncrIfNecessary(new_data);
	}

	DBUG_RETURN(code);
}

/**
 * ɾ��һ����¼��
 * �ڵ���rnd_next/rnd_pos/index_next֮��MySQL������������һ����ɾ�����������ļ�¼��
 * ���TNT���Ǹ��µ�ǰɨ��ļ�¼������Ҫbuf������
 * ������insert_idʱ��REPLACEʱ������ͻҲ�����delete_row
 *
 * @param buf ��ǰ��¼���ݣ�TNT����
 * @return ���Ƿ���0
 */

int ha_tnt::delete_row(const uchar *buf) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::delete_row");
	ha_statistic_increment(&SSV::ha_delete_count);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::delete_row(buf));
	int code = 0;
	
	if (m_scan) {
		/** �����ϲ����ڴ����ʱ��TNT�ڶ�ȡ��¼ʱֱ��ʹ��MySQL�ϲ�����Ŀռ䣬�����Լ�����ռ�
		*	��ʱ����setһ�Σ���ֹMySQL�ϲ������buf�ռ䷢���仯�����µײ�ָ�����ĵ�ַ
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
			// position scan ������������,���״�
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
	//truncate��delete from TABLE_NAME���п��ܵ��ñ�������
	//��delete from TABLE_NAME����Ҫɾ�����ڵ�ѹ���ֵ�
	bool isTruncateOper = (m_thd->lex->sql_command == SQLCOM_TRUNCATE);
	tnt_db->truncateTable(m_session, m_table, isTruncateOper);
}

/**
 * ɾ�����м�¼
 * ����ʵ��truncate
 * 5.1�汾�ľɽӿ�delete_all_rows()����ʹ��,����TNT��֧��statement��ʽbinlog���ϲ㲻���ٵ��ô˽ӿ�
 *
 * @return �ɹ�����0��ʧ�ܷ��ش�����
 */
int ha_tnt::truncate() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ha_tnt::truncate");
	assert(m_session);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::truncate());

	// truncate����֮ǰ������Ѿ����ڸ��µĻ�Ծ�����򱨴��ع���ǰ���
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

	// ���ڲ�����Ȼ��start�����DDL����
	trx = startInnerTransaction(TL_X);
	m_session->setTrans(trx);

	try {
		trx->lockTable(TL_X, m_table->getNtseTable()->getTableDef()->m_id);
		deleteAllRowsReal();
	} catch (NtseException &e) {
		// Truncate Table����ʧ�ܣ��ع��ڲ����񣬲�����
		rollbackInnerTransaction(trx);
		int code = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
		DBUG_RETURN(code);
	}

	// ����������ֶε����������ֵ��Ϊ1
	reset_auto_increment_if_nozero(1);
	// �ύ�ڲ����񣬲�����
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
 * ɾ����
 * ��ɾ����֮ǰ�����жԱ�����ö��Ѿ����ر�
 *
 * @param name ���ļ�·����������׺��
 * @return �ɹ�����0�������ڷ���HA_ERR_NO_SUCH_TABLE���������󷵻�HA_ERR_GENERIC
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

	// �ɹ�����commit inner transaction
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
 * �����������ô˺���ʱhandler����Ϊ�ոմ���û��open�Ķ���
 *
 * @param from ���ļ�ԭ·��
 * @param to ���ļ���·��
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
	
	// �ɹ�����commit inner transaction
	if (trx != NULL) {
		commitInnerTransaction(trx);
	}

	tnt_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_session = NULL;
	detachFromTHD();

	DBUG_RETURN(code);
}

/** TNT�Ǳ�׼����������Ϣ */
struct IdxDefEx {
	const char	*m_name;		/** ������ */
	s8			m_splitFactor;	/** ����ϵ�� */

	IdxDefEx(const char *name, s8 splitFactor) {
		m_name = System::strdup(name);
		m_splitFactor = splitFactor;
	}
};

/** TNT�Ǳ�׼������Ϣ */
struct TblDefEx {
	bool	m_useMms;			/** �Ƿ�����MMS */
	bool	m_cacheUpdate;		/** �Ƿ񻺴���� */
	u32		m_updateCacheTime;	/** ���»���ʱ�䣬��λ�� */
	u8		m_pctFree;			/** �䳤��Ԥ���ռ�ٷֱ� */
	Array<ColGroupDef *>  *m_colGrpDef;/* �����鶨��*/
	bool	m_compressLobs;		/** �Ƿ�ѹ������� */
	bool    m_compressRows;     /** �Ƿ����ü�¼ѹ�� */
	u8      m_compressThreshold;/** ��¼ѹ����ֵ */
	uint    m_dictionarySize;   /** ��¼ѹ��ȫ���ֵ��С */
	u16     m_dictionaryMinLen; /** ��¼ѹ��ȫ���ֵ�����С���� */
	u16     m_dictionaryMaxLen; /** ��¼ѹ��ȫ���ֵ�����󳤶�*/
	bool    m_fixLen;           /** �Ƿ�ʹ�ö����� */
	Array<char *>	*m_cachedCols;	/** ������µ����� */
	Array<IdxDefEx *>	*m_idxDefEx;/** �Ǳ�׼����������Ϣ */
	u16		m_incrSize;			/** �ļ���չҳ���� */
	bool	m_indexOnly;		/** ֻ�������ı� */

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
 * ������(�����򿪱�)
 *
 * @param name ���ļ�·������������׺
 * @param table_arg ����
 * @param create_info ����Ľ���ѡ����������Ľ�����䣬ע�͵�
 * @return �ɹ�����0��ʧ�ܷ��ط�0
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
		// �����ֶζ���
		for (uint i = 0; i < table_arg->s->fields; i++) {
			Field *field = table_arg->field[i];
			bool notnull = !field->maybe_null();
			if (field->flags & PRI_KEY_FLAG) {
				assert(notnull);	// MySQL���Զ��������������Ա��NOT NULL
			}

			bool isValueType = true; // �Ƿ���ֵ����
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
				if (field->binary()) { // varbinary����
					type = CT_VARBINARY;
				} else {
					type = CT_VARCHAR;
				}
				isValueType = false;
				break;
			case MYSQL_TYPE_STRING:
				if (field->binary()) { // binary����
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
		// ������������
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
					// ������Ͳ�֧��ǰ׺�������״�
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

		//�������Ӵ�����������������Ӵ����������
		if (THDVAR(m_thd, is_trx_connection)) {
			tableDef->setTableStatus(TS_TRX);
			session = createSession("ha_tnt::create", m_conn);
			// �����ڲ�����
			trx = startInnerTransaction(TL_X);
			session->setTrans(trx);
		} else {
			tableDef->setTableStatus(TS_NON_TRX);
			//dummy_trx��һ��������ѹ����ûstart��������
			//session�а������������Ϊ�˱�����TNTTable����ats����ddl����table���ټ�ats������ddl����
			//��Ϊtable���ddl���Ǹ���session�Ƿ��trx���жϵ�
			session = createSessionAndAssignTrx("ha_tnt::create", m_conn, dummy_trx);
		}
		
		do {
			if (SQLCOM_ALTER_TABLE == m_thd->lex->sql_command) {
				//�����ALTER TABLE���������Ȼ��ԭ��ķǱ�׼������Ϣ��firstParName����֧�ַ�������Ϊ�Ƿ�������ַ���Ϊ�գ�
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
					} else {//����ԭ����ʹ��TNT���棬������Ҳ��������������쳣
						break;
					}
				}
				delete [] schemaName;
				delete [] sourceTblName;
			} else {
				//�����Ǳ�׼�������
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
			//�Ƿ���������һ�ŷ��������ɹ�������Ĭ�ϷǱ�׼�������
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

		// �ع��ڲ�����
		if (trx) {
			rollbackInnerTransaction(trx);
			trx = NULL;
			session->setTrans(NULL);
		}
	}

	// �ύ�ڲ�����
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

/** ����HA_CREATE_INFO��Ϣ
 * @param create_info	��Ϣ����
 */
void ha_tnt::update_create_info( HA_CREATE_INFO *create_info ) {
	if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
		ha_tnt::info(HA_STATUS_AUTO);
		create_info->auto_increment_value = stats.auto_increment_value;
	}
}

/** ��������ֵΪָ����ֵ
 * @param value	ָ����������ֵ
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
	//TODO Ŀǰ��֧��inplace create unique index
	//�ӱ������ȴ��ñ������еĻ�Ծ�����ύ
	/*m_session->getTrans()->lockTable(TL_S, m_table->getNtseTable()->getTableDef()->m_id);

	// ĿǰTNTֻ֧��Inplace Add Index�����������Ĺ����У���������²���
	// ��Ҫ�ȴ��ڴ汻Purge��֮�󣬲��ܽ������²�����
	// ���򣬴���Unique�������ܴ�������
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
			// ����Slave�������ֻҪMasterִ�гɹ�������ҲҪ���ִ�гɹ�����Ϊntse_index_build_algorithmֻ�ǻỰ˽�б���
			// ���ƹ����в��ᱻ���ƹ��������QA38567
			online = true;
			sql_print_warning("The create multiple index is from Master to Slave, temporary reset it to online to build multiple indices.");
		}

		for (i = 0; i < (int)num_of_keys; i++) {
			KEY *key = key_info + i;
			bool unique = (key->flags & HA_NOSAME) != 0;

			// ��⼴����ӵ������Ƿ��������, �ο�Innodb��ʵ��
			// ���Ҫ��ӵ�����������ͬ����ĿǰTNT/NTSEһ��ֻ�����һ����������˲����ѭ��
			for(uint j = 0; (int)j < i; j++) {
				KEY *key2 = key_info + j;
				if (0 == my_strcasecmp(system_charset_info, key->name, key2->name)) {
					NTSE_THROW(NTSE_EC_DUPINDEX, "Incorrect index name %s", key->name);
				}
			}

			// ��⼴����ӵ��������Ѵ��ڵ������Ƿ��������ο�Innodb��ʵ��
			// ���JIRA��NTSETNT-299
			// Mysql-5.6.13���Ѳ���Ҫ���������
			TableDef *tableDef = m_table->getNtseTable()->getTableDef();
			for (uint j = 0; j < tableDef->m_numIndice; j++) {
				IndexDef *indexDef = tableDef->getIndexDef(j);
				if (!indexDef->m_online && 0 == my_strcasecmp(system_charset_info, key->name, indexDef->m_name)){
					NTSE_THROW(NTSE_EC_DUPINDEX, "Incorrect index name %s", key->name);
				}
			}

			// �����������Ƿ���������̫�����ˣ�������InnoDB plugin������ôʵ�ֵ�
			bool primaryKey = !my_strcasecmp(system_charset_info, key->name, "PRIMARY");
			// ����MySQL��Ϊ�������ǵ�һ����������NTSEĿǰû����ô�������Ŀǰǿ��Ҫ������ֻ����Ϊ
			// ��һ������
			if (primaryKey && ntseTable->getTableDef(true, m_session)->m_numIndice)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Primary key must be the first index.");

			// ����MySQL��Ϊ��Ψһ������������Ψһ������֮��TNTĿǰû����ô����Ϊ��֤��ȷ�ԣ�Ҫ��Ψһ������
			// �����ڷ�Ψһ������֮ǰ����
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
 * ���߽�����
 *
 * @param table_arg ����
 * @param key_info ������������
 * @param num_of_keys ��������������
 * @return �ɹ�����0��ʧ�ܷ��ش�����
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
 * ��ɴ��������������裬���ύ��ɻ�ع�֮ǰ�Ѵ���������
 *
 * @param  add ��add_index�����������½����������Ķ�����Ҫ���ڻع�
 * @param  commit true����commit, false������Ҫrollback
 * @return �ɹ�����0��ʧ�ܷ��ش�����
 */
void ha_tnt::finalAddIndexReal(handler_add_index *add, bool commit) throw(NtseException) {
	if (!commit) {
		// ���ʧ�ܣ���Ҫ�ع���μ������Ĳ���
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
 * ��ɴ��������������裬���ύ��ɻ�ع�֮ǰ�Ѵ���������
 *
 * @param  add ��add_index�����������½����������Ķ�����Ҫ���ڻع�
 * @param  commit true����commit, false������Ҫrollback
 * @return ���Ƿ��� 0
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
 * ׼��ɾ����������TNT����ʵ�Ѿ���������ɾ��
 *
 * @param table_arg ����
 * @param key_num Ҫɾ��������������飬����֤һ����˳���ź����
 * @param num_of_keys Ҫɾ������������
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
 * ���ɾ��������������TNT��ʲôҲ����
 *
 * @param table_arg ����
 * @return ���Ƿ���0
 */
int ha_tnt::final_drop_index(TABLE *table_arg) {
	ftrace(ts.mysql, tout << this << table_arg);

	return 0;
}

/** ����ͳ����Ϣ
 *
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ���ʱ����HA_ADMIN_FAILED
 */
int ha_tnt::analyze(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_tnt::analyze");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	Table *ntseTable = m_table->getNtseTable();

	// TODO����ʱTNTֻʵ����NTSE��ͳ����Ϣ�ռ�
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

/** �Ż�������
 *
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ�����ʧ��ʱ����HA_ADMIN_FAILED
 */
int ha_tnt::optimize(THD *thd, HA_CHECK_OPT *check_opt) {
	DBUG_ENTER("ha_tnt::optimize");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::optimize(thd, check_opt));

	//DBUG_RETURN(HA_ADMIN_OK);

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	//�������˽������ntse_keep_compress_dictionary_on_optimize
	// TODO��optimize������TNTDatabaseģ��Ŀǰ��δʵ��
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

/** ��������һ����
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ�����ʧ��ʱ����HA_ADMIN_FAILED�����ݲ�һ�·���HA_ADMIN_CORRUPT
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
		// TODO��verify������ĿǰTNTTable��δʵ��
		m_table->verify(m_session);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), thd, false);
	}
	DBUG_RETURN(!ret ? HA_ADMIN_OK: HA_ADMIN_CORRUPT);
}

/** �����������еĲ�һ��
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ�����ʧ��ʱ����HA_ADMIN_FAILED
 */
int ha_tnt::repair(THD* thd, HA_CHECK_OPT* check_opt) {
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ha_tnt::repair");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::repair(thd, check_opt));

	// OPTIMIZE��������ؽ��������������������Ĳ�һ��
	int ret = optimize(thd, check_opt);
	DBUG_RETURN(ret);
}

/**
 * ��ѯ��׼����ͷǱ�׼����
 * @param session       �Ự 
 * @param sourceTblPath ��·��(����ڻ���·��)
 * @param hasDict OUT ���Ƿ����ֵ�
 * @throw NtseException �򿪱������Ԫ��������ʱ
 * @return ��׼����ͷǱ�׼����pair����
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

/** ����CREATE TABLE�Ǳ�׼��Ϣ
 * @param tableDef ����
 * @param str �ַ�����ʾ�ķǱ�׼��Ϣ������ΪNULL
 * @return ��ָ���˷Ǳ�׼��Ϣ�򷵻ؽ�����������򷵻�Ĭ��ֵ
 * @throw NtseException ��ʽ����
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
 * �����¾ɱ��������Ǳ�׼����Ϣ
 * @param newTableDef �±���
 * @param oldTableDef �ɱ���
 * @param args        INOUT���Ǳ�׼����Ϣ
 */
void ha_tnt::fixedTblDefEx(const TableDef *newTableDef, const TableDef *oldTableDef, TblDefEx *args) {
	if (args->m_cachedCols) {
		Array<char *> *newCachedColArr = new Array<char *>();
		for (size_t i = 0; i < args->m_cachedCols->getSize(); i++) {
			char *name = (*args->m_cachedCols)[i];
			ColumnDef *col = newTableDef->getColumnDef(name);
			if (col) {
				newCachedColArr->push(name);
			} else {//cached update�����Ѿ���ɾ��
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
			if (!idx) {//�����Ѿ���ɾ��
				delete idxDefEx;
			} else {
				newIdxArr->push(idxDefEx);
			}
		}
		delete args->m_idxDefEx;
		args->m_idxDefEx = newIdxArr;
	}	
	//���������鶨��
	if (args->m_colGrpDef) {
		args->m_colGrpDef = ColGroupDef::buildNewColGrpDef(newTableDef, oldTableDef, args->m_colGrpDef);
	}
}

/** ���÷Ǳ�׼������Ϣ��������
 * @param tableDef IN/OUT������
 * @param args �Ǳ�׼������Ϣ
 * @throw NtseException ���Ĵ���, �Ǳ�׼�����ͻ
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
	//�����ָ����fixlen����Ϊfalse�����Ҹ��ݸ����м�������ı��ʽ�Ƕ�����ʽ����ǿ��ʹ�ñ䳤��ʽ
	if (!args->m_fixLen && tableDef->m_origRecFormat == REC_FIXLEN) {
		tableDef->m_recFormat = REC_VARLEN;
	} 
	//����������
	if (args->m_colGrpDef) {
		assert(args->m_colGrpDef->getSize() > 0);
		tableDef->setColGrps(args->m_colGrpDef, true);
	}
	if (args->m_compressRows) {
		if (tableDef->m_recFormat == REC_FIXLEN)
			NTSE_THROW(NTSE_EC_GENERIC, "Can't set \"compress_rows\" = \"true\" when table uses fix length heap, please set argument \"fix_len\" = \"false\"");
		tableDef->m_recFormat = REC_COMPRESSED;
		//����ѹ���ֵ�����
		assert(!tableDef->m_rowCompressCfg);
		tableDef->m_rowCompressCfg = new RowCompressCfg(args->m_dictionarySize, args->m_dictionaryMinLen, 
			args->m_dictionaryMaxLen, args->m_compressThreshold);
		if (0 == tableDef->m_numColGrps) 
			tableDef->setDefaultColGrps();
	}
}

/** �ж�һ�����������Ƿ�Ϊǰ׺
 * @param table_arg ����
 * @param key_part ��������
 * @return �Ƿ�Ϊǰ׺
 */
/*bool ha_tnt::isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part) {
	// ����Ӧ��ͨ��HA_PART_KEY_SEG��־���ж��Ƿ���Ϊǰ׺����MySQL��û����ȷ���������ֵ��
	// ����ο�InnoDB�е��ж��߼�
	Field *field = table_arg->field[key_part->field->field_index];
	assert(!strcmp(field->field_name, key_part->field->field_name));
	if ((key_part->length < field->pack_length() && field->type() != MYSQL_TYPE_VARCHAR)
		|| (field->type() == MYSQL_TYPE_VARCHAR && key_part->length < field->pack_length() - ((Field_varstring*)field)->length_bytes))
		return true;
	return false;
}*/

/**
* ��ʼһ���ڲ�����
* @param lockMode ����ģʽ
*/
TNTTransaction* ha_tnt::startInnerTransaction(TLockMode lockMode) {
	TNTTrxSys *trxsys = tnt_db->getTransSys();

	TNTTransaction *trx = NULL;
	try {
		trx = trxsys->allocTrx();
	} catch (NtseException &e) {
		// ��ǰϵͳ���й��������Ŀǰ����Ϊֱ���˳�����
		reportError(e.getErrorCode(), e.getMessage(), m_thd, true);
		return NULL;
	}
	trx->startTrxIfNotStarted(m_conn, true);
	
	trx->setThd(m_thd);

	return trx;
}

/**
* �ύһ���ڲ�����
* @param ���ύ����
*/
void ha_tnt::commitInnerTransaction(TNTTransaction *trx) {
	TNTTrxSys *trxsys = tnt_db->getTransSys();

	trx->commitTrx(CS_INNER);

	trxsys->freeTrx(trx);
}

/**
* �ع�һ���ڲ�����
* @param ���ع�����
*/
void ha_tnt::rollbackInnerTransaction(TNTTransaction *trx) {
	TNTTrxSys *trxsys = tnt_db->getTransSys();

	trx->rollbackTrx(RBS_INNER);

	trxsys->freeTrx(trx);
}


/** �����ڽ���ɨ�������֮
 * @post �����ڽ���ɨ�裬��ײ��ɨ���Ѿ��������Ự��MemoryContext������
 *   ����ʼɨ��ǰ��״̬
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
 * ����ɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @param key ����������
 * @param keypart_map �������а�����Щ��������
 * @param find_flag �����������ͣ��緽���Ƿ������ʼ������
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_tnt::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::index_read_map");
	ha_statistic_increment(&SSV::ha_read_key_count);

	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_read_map(buf, key, keypart_map, find_flag));

	if (m_iuSeq) { // ������INSERT FOR DUPLICATE UPDATEʱ��ʹ��index_read_map������rnd_posȡ��¼
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
	/** TODO: �������������TNT��������֧�֣�TNT���ᶪʧ����������ܻ᷵��
	 * ����Ľ���������Է���MySQL�ϲ���TNT���صĴ��������˵�
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
 * ��������ɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @param key ����������
 * @param keypart_map �������а�����Щ��������
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_tnt::index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map) {
	DBUG_ENTER("ha_tnt::index_read_last_map");
	ha_statistic_increment(&SSV::ha_read_last_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_read_last_map(buf, key, keypart_map));
	DBUG_RETURN(indexRead(buf, key, keypart_map, false, true, false));
}

/**
 * ��������ȫɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_tnt::index_first(uchar *buf) {
	DBUG_ENTER("ha_tnt::index_first");
	ha_statistic_increment(&SSV::ha_read_first_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_first(buf));
	DBUG_RETURN(indexRead(buf, NULL, 0, true, true, false));
}

/**
 * ��������ȫɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */

int ha_tnt::index_last(uchar *buf) {
	DBUG_ENTER("ha_tnt::index_last");
	ha_statistic_increment(&SSV::ha_read_last_count);
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::index_last(buf));
	DBUG_RETURN(indexRead(buf, NULL, 0, false, true, false));
}

/**
 * ��ȡ��һ����¼
 *
 * @param buf ���ڴ洢�����¼����
 * @return �ɹ�����0��û�м�¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
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
 * ��ȡǰһ����¼
 *
 * @param buf ���ڴ洢�����¼����
 * @return �ɹ�����0��û�м�¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
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
 * ��������ɨ�裬�ͷ�ɨ��������Դ
 *
 * @return ���Ƿ���0
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
 * ��ʼ��ȫ��ɨ��
 *
 * @param scan Ϊtrueʱ����Ҫ����
 * @return һ����ɹ�����0
 */
int ha_tnt::rnd_init(bool scan) {
	assert(m_session);
	DBUG_ENTER("ha_tnt::rnd_init");
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::rnd_init(scan));

	endScan();	// ��Ϊ�Ӳ�ѯʱ���ܻ�ɨ����

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
	m_isRndScan = scan;
	bitmap_copy(&m_readSet, table->read_set);

	DBUG_RETURN(0);
}

/** �ж�ָ����������ʽ�Ƿ������ֵ����
 * @param flag ������ʽ
 * @param lowerBound true��ʾ��ɨ�跶Χ���½磬false��ʾ���Ͻ�
 * @return �Ƿ������ֵ����
 */
/*bool ha_tnt::isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound) {
	switch (flag) {
	// ��Щ�������������Equal
	case HA_READ_KEY_EXACT:
	case HA_READ_KEY_OR_NEXT:
	case HA_READ_KEY_OR_PREV:
	case HA_READ_PREFIX:
	case HA_READ_PREFIX_LAST:
	case HA_READ_PREFIX_LAST_OR_PREV:
		return true;
	// ��������������������Equal
	case HA_READ_AFTER_KEY:
		return !lowerBound;
	case HA_READ_BEFORE_KEY:
		return lowerBound;
	default:
		return false;
	};
}*/

/**
 * ����ָ����������[min_key, max_key]֮��ļ�¼��
 *
 * @param inx �ڼ�������
 * @param min_key ���ޣ�����ΪNULL
 * @param max_key ���ޣ�����ΪNULL
 */
ha_rows ha_tnt::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
	ftrace(ts.mysql, tout << this << inx << min_key << max_key);
	DBUG_ENTER("ha_tnt::records_in_range");
	// TODO����ʱֱ��ʹ��NTSE�ķ����������Ľ�
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
	// ���Դ���
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
 * ����ȫ��ɨ��
 *
 * @return ����0
 */
int ha_tnt::rnd_end() {
	DBUG_ENTER("ha_tnt::rnd_end");
	DBUG_SWITCH_NON_CHECK(m_thd, ntse_handler::rnd_end());
	if (m_session)
		endScan();
	DBUG_RETURN(0);
}

/**
 * ���ر�����һ����¼
 *
 * @param buf �洢��¼�������
 * @return �ɹ�����0��û����һ�μ�¼����HA_ERR_END_OF_FILE
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

	// Ϊ������ȷ�ĵõ�read_set����Ϣ,������ų�ʼ��ɨ����
	if (!m_scan) {
		initReadSet(false);
		beginTblScan();
	} else {
		assert(bitmap_is_subset(table->read_set, &m_readSet));
	}
	DBUG_RETURN(fetchNext(buf));
}

/**
 * ���ݼ�¼λ�ö�ȡ��¼
 *
 * @param buf �洢��¼�������
 * @param pos ��¼λ��
 * @return ���Ƿ���0
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
		// Ϊ������ȷ�ĵõ�read_set����Ϣ,������ų�ʼ��ɨ����
		if (!m_scan) {
			initReadSet(false, true);
			beginTblScan();
		} else {
			// ����update/delete������һ����ȡ���У�������read_set��ȡ��¼
			if (m_opInfo.m_selLockType != TL_X)
				assert(bitmap_is_subset(table->read_set, &m_readSet));
		}
		// ��ȡRowId
		RowId rid = RID_READ(pos);
		try {
			NTSE_ASSERT(m_table->getNext(m_scan, buf, rid, true));
		}catch (NtseException &e) {
			// position scan �����������������״�
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
 * ���Mysql������FileSort�㷨�� ����ȡ������������ԣ���ȡ�����������������
 * ������������г���֮�ʹ���max_length_for_sort_data�� ���߽�������������ʱ��Mysqlʹ�������㷨���μ�QA89930��
 * 
 * @return true��ʾMySQLʹ��������FileSort�㷨
 */
/*bool ha_tnt::is_twophase_filesort() {
	bool twophase = true;
	if (m_thd != NULL && m_thd->lex != NULL) {
		MY_BITMAP tmp_set;
		uint32 tmp_set_buf[Limits::MAX_COL_NUM / 8 + 1];	//λͼ����Ҫ�Ļ���
		bitmap_init(&tmp_set, tmp_set_buf, m_table->getNtseTable()->getTableDef()->m_numCols, FALSE);
		// ��ס�ɵ�read_set
		MY_BITMAP *save_read_set= table->read_set;
		bitmap_clear_all(&tmp_set);
		table->read_set= &tmp_set;

		List_iterator<Item> list_iter(m_thd->lex->select_lex.item_list);
		Item *curr;
		while ((curr = list_iter++) != NULL) {
			curr->walk(&Item::register_field_in_read_map, 1, (uchar*) table);
		}
		
		// ���ѻ�������ɨ��readSet���ܰ�����ѯ������
		// TODO: ��ξ�ȷ�жϲ�ѯ������м��������ѯ��Mysql����
		if (bitmap_is_subset(table->read_set, &m_readSet))
			twophase = false;
		// �ָ�read_set
		table->read_set = save_read_set;
	}
	return twophase;
}*/

/**
 * ��¼��ǰ��¼��RowID�浽ref�У�����TNT��˵��ΪRID���������ڷ���ˮ������ʱ����
 * �Լ�¼����Ҫ���µļ�¼��λ�ã�����Filesortʱ����
 *
 * @param record ûʲô��
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
 * ��ʼ��ȫ��ɨ����
 * @pre m_readSetӦ��Ҫ������ȷ
 */
void ha_tnt::beginTblScan() {
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols);

	OpType opType = getOpType();
	try {
		if (m_isRndScan) {
			/** NTSE֧�ַǾ�ȷ��ɨ�裬����߱�ɨ�����ܣ�TNT��ʱ���ô˲��� */
			// m_conn->getLocalConfig()->m_accurateTblScan = THDVAR(ha_thd(), accurate_tblscan);
			m_scan = m_table->tableScan(m_session, opType, &m_opInfo, numReadCols, readCols, false, m_lobCtx);
		} else {
			m_scan = m_table->positionScan(m_session, opType, &m_opInfo, numReadCols, readCols, false, m_lobCtx);
		}
	} catch(NtseException &) {
		NTSE_ASSERT(false);		// ���ӱ���ʱ�����ܻ�ʧ��
	}
}

/** ��ɨ�������ɨ��ʱ��ȡ��һ����¼����������ɨ��ʱʵ������ǰһ����¼��
 * @post m_lastRow�м�¼�˸ջ�ȡ�ɹ��ļ�¼��RID
 *
 * @param buf OUT���洢��ȡ�ļ�¼����
 * @return �ɹ�����0��ʧ�ܷ��ش�����
 */
int ha_tnt::fetchNext(uchar *buf) {

	bool hasNext = false;
	int	 ret = 0;

	table->status = STATUS_NOT_FOUND;

	m_lobCtx->reset();

	/** getNext�����������ж��߼�
	 ** �߼�һ������getNext�����ķ���ֵ���ж��Ƿ������Ľ���λ��
	 ** �߼���������getNext������throw Exception���жϳ������ͣ�������Ӧ����
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
		// getNext�����׳��쳣�����ܴ��ڵ��쳣����������/������ʱ/����ռ䲻���
		// ��Ҫ���ݲ�ͬ���쳣���ͣ��ж�ֱ�ӷ��أ������ǻع���������

		assert(m_thd == current_thd);
		ret = reportError(e.getErrorCode(), e.getMessage(), m_thd, false);
	}
	
	// ������NextKey�����ȡ
	if (ret == 0) {
		m_lastRow = m_scan->getCurrentRid();
		table->status = 0;
	}

	return ret;
}

/** ��ʼ����ɨ�貢���ص�һ����¼
 *
 * @param buf ���������¼�Ļ�����
 * @param key ����������
 * @param keypart_map �������а�����Щ��������
 * @param forward ��������
 * @param includeKey �Ƿ��������key�ļ�¼
 * @param exact �Ƿ�ֻ���ص���key�ļ�¼
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ha_tnt::indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact) {
	m_lobCtx->reset();
	endScan();	// ������Χ��ѯʱ���ܻ�ɨ���Σ�������index_end

	int code = 0;

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();

	initReadSet(true);

	bool singleFetch;
	u16 numReadCols;

	// NTSETNT-285��TNT/NTSE��select count��*�����û������ȫ��ɨ��
	u16 *readCols = transCols(&m_readSet, &numReadCols, true, active_index);

	bool nullIncluded = transKey(active_index, key, keypart_map, &m_indexKey);

	// �����ֵ��ѯ�ļ�ֵ�д���NULL�У���ôһ������Single Fetch
	if (exact && !nullIncluded
		&& m_table->getNtseTable()->getTableDef(true, m_session)->m_indice[active_index]->m_unique
		&& m_indexKey.m_numCols == m_table->getNtseTable()->getTableDef(true, m_session)->m_indice[active_index]->m_numCols)
		singleFetch = true;
	else
		singleFetch = false;

	OpType opType = getOpType();
	try {
		// ����TNTIndexһ����Ҫ����һ��SubRecord����������Ƿ�ָ��IndexKey������m_indexKey����
		// IndexScanCond cond(active_index, m_indexKey.m_numCols? &m_indexKey: NULL, forward, includeKey, singleFetch);
		// IndexScanCond cond(active_index, &m_indexKey, forward, includeKey, singleFetch);

		IndexScanCond cond(active_index, m_indexKey.m_numCols? &m_indexKey: NULL, forward, includeKey, singleFetch);
		m_scan = m_table->indexScan(m_session, opType, &m_opInfo, &cond, numReadCols, readCols, singleFetch, false, m_lobCtx);
	} catch(NtseException &e) {
		NTSE_ASSERT(e.getErrorCode() == NTSE_EC_LOCK_TIMEOUT);
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	}
	// ����NTSE�����ݲ�֧�ַ���ֻ����ָ���������ļ�¼������ϲ�������ʽ��HA_READ_KEY_EXACT��
	// ���ֲ���singleFetch����NTSE�᷵��ʵ���ϴ���ָ���������ļ�¼�������ϲ�ָ����
	// HA_READ_KEY_EXACT������ϲ���Ϊ�ײ�ֻ�᷵�ص���ָ���������ļ�¼��������ȥ��飬
	// ���������Ҫ���˵���Щ�����������ļ�¼
	m_checkSameKeyAfterIndexScan = exact && !singleFetch;

	code = fetchNext(buf);
	return code;
}

/** �õ���������
 * @param return ��������
 */
OpType ha_tnt::getOpType() {
	//�������������۲���������Ƿ����������ntse����
	if (!THDVAR(m_thd, is_trx_connection)) {
		return ntse_handler::getOpType();
	}

	OpType ret;
	// ���ն���������lock in share mode��S�������Ƕ�����
	if (m_opInfo.m_selLockType == TL_NO || m_opInfo.m_selLockType == TL_S) {
		ret = OP_READ;
	}
	// ��ǰ����U/I/D��X����д����
	else {
		assert((m_opInfo.m_selLockType == TL_X));
		ret = OP_WRITE;
	}

	return ret;
}

/** �Ƚϼ�¼�ļ�ֵ��ָ����ֵ�Ƿ����
 *
 * @param buf ��¼
 * @param indexDef ��������
 * @param key KEY_PAD��ʽ�ļ�ֵ
 * @return �Ƿ����
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

/** ���ڸ���ɨ��֮ǰ��ʼ��m_readSet����Ϣ
 * @param idxScan	true��ʾ����ɨ�裬false��ʾ��ɨ��
 * @param posScan	true��ʾ��rnd_posɨ�裬false������������ɨ���������ɨ��
 */
/*void ha_tnt::initReadSet( bool idxScan, bool posScan) {
	assert(m_thd);
	assert(!(idxScan && posScan));	// ���벻���ܼ�������ɨ������rnd_posɨ��
	
	if (idxScan)
		bitmap_copy(&m_readSet, table->read_set);
	else {
		// ��ɨ��ʱrnd_init�͵�һ��rnd_nextʱ������read_set��һ�£�Ϊ��ȫ���
		// ʹ��rnd_init�͵�һ��rnd_next��read_set֮�ϼ�
		bitmap_union(&m_readSet, table->read_set);
	}
	
	m_isReadAll = bitmap_is_set_all(&m_readSet);
}*/

/**
 * ת��MySQL��λͼ��ʾ�Ĳ����漰���Լ�ΪTNT��ʽ
 *
 * @param bitmap MySQL���Բ���λͼ
 * @param numCols OUT�����Ը���
 * @return �����Ժţ��ӻỰ��MemoryContext�з���
 */
/*u16* ha_tnt::transCols(MY_BITMAP *bitmap, u16 *numCols) {
	// TNT��MySQL�ϲ�дbinlog��ͬʱTNTĿǰ��֧���м�binlog
	// �ڴ�����£�MySQL����update/delete����ʱ��Ҫ���ȡ
	// ��¼������ȫ���Ϊbinlog�е�ǰ���񡣴�ʱ�����ܹ�
	// ����read_set���ж϶�ȡ���У�������Ҫǿ�ƶ�ȡ������
	// Ŀǰ��������InnoDB���ƵĴ���ʽ�����жϳ���ǰ���
	// ��Ҫ��X�������ȡ��¼��ȫ��
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
 * �Ա�MySQL�����old_row��new_row��ȡ������ȡֵ��ͬ���У���Ϊupdate��
 * @pre	Ŀǰ���������Ե��� insert on duplicate key update ��䣬�����
 *		old_row�ǳ�ͻ���ȫ�new_row��old_row����updateָ���е���ȡֵ
 * @param	old_row	��ͻ��ֵ��ȫ��
 * @param	new_row	��ͻ��ֵ�ϲ�updateָ����֮���ȫ��
 * @param	numcols	һ���ж��ٲ�ͬ��
 *
 * @return	����old_row��new_row����ȡ����update����
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

	// ѭ��������¼��������
	for (uint i = 0; i < nFields; i++) {
		field = table->field[i];

		ColumnDef *colDef = tableDef->m_columns[i];

		// ��ǰ�У���old��¼��new��¼�е���ʼƫ�Ƶ�ַ��������MySQL��ʽ�����ƫ����һ����
		oldColPtr = (const byte*) old_row + (field->ptr - table->record[0]);
		newColPtr = (const byte*) new_row + (field->ptr - table->record[0]);

		colPackLen = field->pack_length();

		// ��ǰ�У���old��¼��new��¼�еĳ���
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
				// ����ǳ����ֶΣ����ﰴ��varchar�ķ�ʽ���д���
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
	// �����ϵ�����£��ڶ���replace���ᴫ�����һ��һģһ���ļ�¼
	// ��ʱ�����������nDiff = 0�����ʵ���ϲ���Ҫ���κθ��²���
	// assert(nDiff != 0);

	*numCols = nDiff;

	return r;
}

/**
* ��ȡһ��MySQL��ʽ��¼�е�lob�ֶ�
* @param	colLen	�������/lob�ֶεĳ���
* @param	colOffset lob�ֶε�offset
* @return	����lob�ֶε���ʼλ�ã��Լ�����
*/
const byte* ha_tnt::rowReadLobCol(u32 *colLen, const byte *colOffset) {
	byte*	data;
	u32		len = *colLen;

	// lenΪlob�ֶ���record��ռ�õĳ��ȣ���lob�ֶ����8 bytes��Ϊlobָ�룬���8 bytes֮ǰ�ģ���Ϊlob size����Ϊ1/2/3/4
	*colLen = readFromNLittleEndian(colOffset, len - 8);

	memcpy(&data, colOffset + len - 8, sizeof(data));

	return(data);
}

/**
* ��ȡlob�ֶεĳ�����Ϣ + 8�ֽ�lob����ָ��
* MySQL��ʽ��lob�е������ǣ�lob���� + ָ�룻����lob���͵Ĳ�ͬ�����ȿ���ռ��1/2/3/4 bytes
* @param colOffset	lob�ֶ���record�е���ʼƫ��
* @param colLen		lob�ֶ���record�е�ռ�ó���
* @return lob�ֶε�ʵ��ռ�ó���
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
* ��ȡһ��MySQL��ʽ��¼�е�varchar�ֶ�
* @param	colLen �������/varchar�ֶεĳ���
* @param	colOffset varchar�ֶε�offset
* @param	lenLen	  ��ʾvarchar������Ϣ�ĳ���
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
* �жϼ�¼�е�һ�������ֶΣ��Ƿ�ΪNULL
* @param field	������¼�е�һ��
* @param record ��¼
* @return NULL����true��NOT NULL����false
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
 * ת��MySQL����������(KEY_MYSQL)ΪTNT�ڲ���ʽ(KEY_PAD)��ʽ���洢��out��
 * @pre �Ѿ����˱�Ԫ������
 *
 * @param idx �ڼ�������
 * @param key MySQL�ϲ��������������ΪKEY_MYSQL��ʽ
 * @param keypart_map �������а�����Щ��������
 * @param out �洢ת����Ľ�����ڴ��m_session���ڴ��������з���
 * 
 * @return search key�к���NULL�У�����true�����򷵻�false
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
 * ������key_part_map�ṹ��ʾ���������а�����Щ����������Ϣ
 *
 * @param idx �ڼ�������
 * @param keypart_map �������а�����Щ����������Ϣ
 * @param key_len OUT������������
 * @param num_cols OUT���������а���������������
 */
/*void ha_tnt::parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols) {
	DBUG_ASSERT(((keypart_map + 1) & keypart_map) == 0);	// ֻ֧��ǰn������

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

/** ��·���е�Ŀ¼�ָ���ͳһת����/ */
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
 * ����MySQL���ַ����õ�TNTʹ�õ�Collation
 *
 * @param charset �ַ���
 * @return Collation
 * @throw NtseException ��֧�ֵ��ַ���
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
* ���ݵײ㷵�صĴ����ж��Ƿ���Ҫrollback�������񣬲�����Ӧ��rollback����
* 
* @param errCode TNT�ڲ�������
* @param thd �����MySQL�̱߳�ʶ
*/
void ha_tnt::rollbackTrxIfNecessary(ErrorCode errCode, THD *thd) {
	// Ŀǰ����Եײ���������/������ʱ����/Ψһ�Գ�ͻ/��¼����/������;���жϣ���Ҫ�ع���������
	// �����Ĵ��󣬲��ûع������������ϲ�ع���ǰ��伴��
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
		//��ǰ�����Ѿ��ع���֪ͨMySQL�ϲ㣬���������Ӧ��binlog
		thd_mark_transaction_to_rollback(thd, true);
	}
}

/**
 * �������ȷʵ���ȼ�¼�����ȴ�get_error_messageʱ�ŷ���
 *
 * @param errCode �쳣��
 * @param msg ������Ϣ
 * @param thd �����MySQL�̱߳�ʶ
 * @param fatal �Ƿ�Ϊ���ش������ش��󽫵���ϵͳ�˳�
 * @param warnOrErr ��Ϊtrue�������push_warning_printf���澯�棬��Ϊfalse�����my_printf_error�������
 * @return ���ظ�MySQL�Ĵ�����
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
 * �õ�������Ϣ
 *
 * @error ��һ�β���ʱ���صĴ����
 * @param buf OUT�����������Ϣ��MySQL
 * @return �Ƿ�����ʱ����
 */
bool ha_tnt::get_error_message(int error, String* buf) {
	ftrace(ts.mysql, tout << this << error << buf);
	if (error != m_errno)
		return false;
	buf->append(m_errmsg);
	m_errno = 0;
	return true;
}

/** ����ָ������������ֵ
 * @param offset			������ʼƫ���������������£���ǰ����ı�ţ�������Ϊ1
 * @param increment			�������������������£���ʾһ���ж������ڵ㣻������Ϊ1
 * @param nb_desired_values	��Ҫ�������ٸ�����ֵ
 * @param first_value		����/������������first_valueС�ڵ�ǰϵͳautoincȡֵ��������first_value
							����ֱ��ʹ��first_valueֵ��ͬʱ��Ҫ���first_value�ķ�Χ
 * @param nb_reserved_value	����������˶��ٸ�����ֵ
 */
void ha_tnt::get_auto_increment( ulonglong offset, ulonglong increment, ulonglong nb_desired_values, ulonglong *first_value, ulonglong *nb_reserved_values ) {
	u64		autoInc;

	// autoInc�е�����������ֵ
	u64		autoIncMax = getIntColMaxValue();

	// ��ȡ���ϵ�autoincֵ�����Ҽ���autoInc mutex
	m_table->enterAutoincMutex();
	autoInc = m_table->getAutoinc();

	// �ڵ��ô˺���֮ǰ�����ϵ�autoIncȡֵһ���Ѿ���ʼ�����
	assert(autoInc > 0);

	if (table->s->next_number_key_offset) {
		// autoInc �ֶβ��������ĵ�һ���ֶ�
		*first_value = (~(ulonglong) 0);
		return;
	}

	// TODO��TNTһ�ڰ汾�У���ʵ��autoIncֵ��Ԥȡ���ܣ�nb_reserved_value = 1
	// TODO��TNTһ�ڰ汾�У���׼��֧��statement binlog��ֻ֧��row binlog�����ֻ��Ҫmutex����autoInc�ķ��伴��
	//		 ��Ŀǰ�Ĳ������ã�*first_valueȡֵӦ����statement binlog���Ƶ�slave�����������ã���˿ɼ򵥲�����

	// ����һ�ڲ������棬���first_value�͵���autoInc
	*first_value = autoInc;

	// ��ʱ������autoIncȡֵ
	*nb_reserved_values = 1;

	m_autoInc.m_autoincIncrement = increment;
	m_autoInc.m_autoincOffset = offset;	 
	
	// ����next value��Ȼ������
	u64 current = *first_value > autoIncMax ? autoInc : *first_value;
	u64 next_value = calcNextAutoIncr(current, increment, offset, autoIncMax);

	if (next_value < *first_value) {
		*first_value = (~(ulonglong) 0);
	} 
	else {
		// ����autoInc�ֶε����ֵ
		m_table->updateAutoincIfGreater(next_value);
	}

	m_table->exitAutoincMutex();
}

/**
*	��ȡautoInc�е�����ȡֵ
*	@return ��������ȡֵ
*/
u64 ha_tnt::getIntColMaxValue() {
	u64	autoIncMax = 0;
	active_index = table->s->next_number_index;

	TableDef *tableDef = m_table->getNtseTable()->getTableDef();
	IndexDef *indexDef = tableDef->m_indice[active_index];
	
	uint i = indexDef->m_columns[0];
	// �����ֶα����������ĵ�һ������
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
*	��ʼ�����autoinc�ֶΣ�ֻ�ڱ�real open��ʱ�����һ��
*	��autoInc�ֶε������ϣ�����һ������ɨ�裬��ȡ����ֵ
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

	// ʹ������ɨ��õ��������ֵ
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
		// �����ֶα����������ĵ�һ������
		ColumnDef *colDef = tableDef->m_columns[i];
		// NTSETNT-120��tnt-autoinc-44030.test�����������������������½���������棻
		//assert(strcmp(colDef->m_name, field->field_name) == 0);

		s64 oldAutoIncr = 0;
		// ��ȡ���Գ������õ�ǰ��autoIncrֵ
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
			// ��1��֤�����ظ�
			autoIncr = oldAutoIncr + 1;
		// ���������ֶ�Ϊ0��ʾ����
	} else 
		// ��ǰ���������ݣ���autoInc����Ϊ1
		autoIncr = 1;

	session->getMemoryContext()->resetToSavepoint(savePoint);

	return autoIncr;
}

/** ������һ�������ֶ�ֵ
 * @param current	��ǰֵ
 * @param increment	ÿ����������
 * @param offset	����ƫ����
 * @param maxValue	�����������͵�������ֵ
 * @description		�ο�InnoDB��innobase_next_autoinc��ʵ��
 */
u64 ha_tnt::calcNextAutoIncr( u64 current, u64 increment, u64 offset, u64 maxValue ) {
	// �����ֲ��ĵ���offset�����increment�󣬺���offset
	if (offset > increment) {
		offset = 0;
	}

	u64 nextValue = 0;
	if (maxValue <= current) {
		nextValue = maxValue;
	} else if (offset <= 1) {
		// ����0��1���������ͬ�ģ�ϵͳ��������1��mysql�ڵ�
		if (maxValue - current <= increment) {
			// ���ϲ���������Χ����һ��ֵ�������ֵ
			nextValue = maxValue;
		} else {
			// ����򵥵ļ��ϲ�������
			nextValue = current + increment;
		}
	} else {
		u64 times;
		// ����currentֵ���ǲ���increment�Ķ��ٱ�
		if (current > offset) {
			times = ((current - offset) / increment) + 1;
		} else {
			times = ((offset - current) / increment) + 1;
		}

		if (increment > (maxValue / times)) {
			// ������Χ����maxValue
			nextValue = maxValue;
		} else {
			nextValue = increment * times;
			assert(maxValue >= nextValue);

			// ȷ������offset�������
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
*	�û�ָ����autoInc��ȡֵ������Ҫ����������õ�ǰ����autoInc��next value
*	@param buf	�ϲ���������ݿ�
*	
*/
void ha_tnt::setAutoIncrIfNecessary(uchar * buf) {
	if (table->next_number_field && buf == table->record[0]) {
		// ����auto increment��ȡ�����ֵΪ64λ�޷���������˴˴�Ӧ��ʹ��u64������s64
		u64 autoIncr = table->next_number_field->val_int();
		if (autoIncr > 0) {	// �޸��������ֶ�

			m_table->enterAutoincMutex();

			u64 autoIncMax = getIntColMaxValue();

			// �����û��ֶ�����autoincֵΪ���������
			if (autoIncr <= autoIncMax) {
				u64 nextValue = calcNextAutoIncr(autoIncr, m_autoInc.m_autoincIncrement, m_autoInc.m_autoincOffset, autoIncMax);
				m_table->updateAutoincIfGreater(nextValue);
			}

			m_table->exitAutoincMutex();
		}
	}
}

/**
 * �ر�����ʱ֪ͨTNT���ú���ֻ����������Ϣ��ΪNULLʱ�Ż����
 *
 * @param hton TNT�洢����ʵ��
 * @param thd Ҫ�رյ�����
 * @return ���Ƿ���0
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
		// TODO����������ǰ��״̬�����ز�ͬ��WARNNing��Ϣ
		trx->rollbackTrx(RBS_NORMAL);
		tnt_db->getTransSys()->freeTrx(trx);
	}

	delete info;
	thd_set_ha_data(thd, hton, NULL);

	DBUG_RETURN(0);
}

/**
 * ����һ��TNT handler
 *
 * @param hton TNT�洢����ʵ��
 * @param table handlerҪ�����ı�
 * @param mem_root �ڴ˴���
 * @return �´�����handler
 */
handler* ha_tnt::tnt_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) {
	ftrace(ts.mysql, tout << hton << table << mem_root);
	return new (mem_root) ha_tnt(hton, table);
}

/**
 * ɾ��ָ��databaseĿ¼�µ�����TNT��
 *
 * @param hton TNT�洢����ʵ��
 * @param path database·��
 * @return 
 */
void ha_tnt::tnt_drop_database(handlerton *hton, char* path) {
	// TODO������NTSE��ʱ��֧��drop database����
	// ͬʱTNTʹ�õ���NTSE�ĳ־û��洢�����TNTҲ��ʱ��֧��
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
	// ���������Ѿ����ù�commitOrdered����֮���commit�׶�ֻ��Ҫˢ��־����
	trx->setCalledCommitOrdered(true);
	DBUG_VOID_RETURN;
}
#endif


/**
* �ύ���񣬻��߱�ʶ��ǰstatementִ�����
* @param hton	TNT�洢����ʵ��
* @param thd	��ǰ����
* @param all  	���ύ���񣬻��Ǳ�ʶ��ǰstatement����
* @return ���Ƿ���0
*/
int ha_tnt::tnt_commit_trx(handlerton *hton, THD* thd, bool all) {
	DBUG_ENTER("tnt_commit_trx");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::ntse_commit_trx(hton, thd, all));
	// TNTTransaction *trx = getTransForCurrTHD(thd);

	// ����getTransInTHD��������Ϊ����һ����֤���ڣ����贴��
	TNTTHDInfo *info = getTntTHDInfo(thd);
	TNTTransaction *trx = info->m_trx;
	assert(trx != NULL);

	if (all ||
		(!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {

			// ��ǰ�������ύ��������Autocommit���ִ�н������ύ��������
			/*if (trx->getTrxState() == TRX_ACTIVE || 
				trx->getTrxState() == TRX_PREPARED) {
			}*/

#ifdef EXTENDED_FOR_COMMIT_ORDERED
			// ���û�е��ù�commitOrdered������binlogû�п����������Ҫ����commit in memory����ع���
			if (!trx->getCalledCommitOrdered())	
				trx->commitTrxOrdered(CS_NORMAL);
			trx->commitCompleteForMysql();
#else
			trx->commitTrx(CS_NORMAL);
			// commit���֮���ͷ�prepare mutex
			if (trx->getActiveTrans() == 2) {
				assert(prepareCommitMutex->isLocked());
				prepareCommitMutex->unlock();
			}
#endif
			trx->setActiveTrans(0);
			// ��ʶ��ǰ�����MySQL��XA������ȡ��ע��
			trxDeregisterFrom2PC(trx);
			thd->variables.net_wait_timeout = info->m_netWaitTimeout;
	} else {
		// ��ǰ����伶�ύ��ֱ�ӱ�ʶ��ǰ���ִ�н�������
		trx->markSqlStatEnd();
	}

	DBUG_RETURN(0);
}

/**
* �ع����񣬻����ǻع���ǰstatement
* @param hton	TNT�洢����ʵ��
* @param thd	��ǰ����
* @param all	��ع����񣬻��ǻع���ǰstatement
* @return �ɹ�����0��ʧ�ܷ�������errorֵ
*/
int ha_tnt::tnt_rollback_trx(handlerton *hton, THD *thd, bool all) {
	DBUG_ENTER("tnt_rollback_trx");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::ntse_rollback_trx(hton, thd, all));
	int ret = 0;
	// TNTTransaction *trx = getTransForCurrTHD(thd);

	// ����getTransInTHD��������Ϊ����һ����֤���ڣ����贴��
	TNTTHDInfo *info = getTntTHDInfo(thd);
	TNTTransaction *trx = info->m_trx;
	assert(trx != NULL);

	// ����rollback�����ͣ���伶rollback or ����rollback��������Ӧ�Ĳ���
	// �ڲ�XA����Prepare�ɹ�֮��һ���ύ���û��޷����ƣ�prepare���񲻻�ع�
	// �ⲿXA�������û����ƣ�prepare���֮�󣬼ȿ����ύ��Ҳ���Իع�
	// �ڲ�XA����prepare�׶μ�Mutex���ⲿ����Mutex�����rollback���޻�ȡMutex
	if (all ||
		!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
			// �ع���������
			ret = trx->rollbackTrx(RBS_NORMAL);

			trx->setActiveTrans(0);

			// ��ʶ��ǰ�����MySQL��XA������ȡ��ע��
			trxDeregisterFrom2PC(trx);

			thd->variables.net_wait_timeout = info->m_netWaitTimeout;
	} else {
		// �ع���ǰ���
		ret = trx->rollbackLastStmt(RBS_NORMAL);
	}

	// TODO���˴��ȼ���Rollbackһ���ɹ�
	DBUG_RETURN(0);
}

/**
* TNT����֧��XA���񣬴˺�����XA�����prepare�׶�
* @param hton	TNT�洢����ʵ��
* @param thd	��ǰ����
* @param all	�ύ�������񣬻��ǽ����ύ��ǰstatement
* @return �ɹ�����0��ʧ�ܷ���errorֵ
*/
int ha_tnt::tnt_xa_prepare(handlerton *hton, THD* thd, bool all) {
	DBUG_ENTER("tnt_xa_prepare");

	DBUG_SWITCH_NON_CHECK(thd, ntse_handler::ntse_xa_prepare(hton, thd, all));
	// TNTTransaction *trx = getTransForCurrTHD(thd);

	// ����getTransInTHD��������Ϊ����һ����֤���ڣ����贴��
	TNTTransaction *trx = getTransInTHD(thd);
	assert(trx != NULL);

	int ret = 0;

	if (!THDVAR(thd, support_xa)) {
		return 0;
	}

	// ����XID
	XID xid;
	thd_get_xid(thd, (MYSQL_XID*)&xid);
	trx->setXId((const XID &)xid);

	if (all ||
		(!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
			// ����prepare��������Autocommit�����������������Ҫprepare��������
			if (trx->getTrxState() == TRX_ACTIVE) {
				trx->prepareForMysql();

				// ��InnoDB 5.1.49���ƣ������ǰ�����ⲿXA�����prepare����
				// ��ô������prepare������Autocommit������ʱ����Ҫ��prepare����
				// ��֤prepare -> binlog commit -> commit��˳����

				// ע�⣺��ʱ��Mutex����ζ��prepare�����ǿ��Բ��еģ���˳��֤����binlog commit -> commit�������

				if (thd_sql_command(thd) != SQLCOM_XA_PREPARE &&
					(all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
#ifndef EXTENDED_FOR_COMMIT_ORDERED
						prepareCommitMutex->lock(__FILE__, __LINE__);
#endif
						// ��������ǰ�Ѿ���ȡprepareCommitMutex״̬
						trx->setActiveTrans(2);
				}

				assert(trx->getTrxState() == TRX_PREPARED);
			}
	} else {
		// ��伶prepare��ֱ�ӱ�ʶ����������
		trx->markSqlStatEnd();
	}

	DBUG_RETURN(ret);
}

/**
* TNT֧��XA���񣬴˺�����XA����Ļָ�����ȡĿǰ�洢�����д���prepare״̬�������Ӧ��xid
* @param hton		TNT�洢����ʵ��
* @param xid_list	in/out������out����ǰTNTϵͳ�д���prepare״̬�������б�
* @param len		xid_list���ж���slots����
* @return ����TNTϵͳ�ж���������prepare״̬����Ӧ��XID��䵽xid_list֮�У�һ����෵��len��xids
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
* �ύһ������prepare״̬������
* @param hton	TNT�洢����ʵ��
* @param xid	prepare����id��mysql�ϲ�ı�ʶ����tnt��������id����Ҫת��
* @return �ɹ�����0��ʧ�ܷ���errorֵ
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
		//�Ѿ�����̨�߳�kill��
		ret = XA_RBROLLBACK;
	}

	DBUG_RETURN(ret);
}

/**
* �ع�һ������prepare״̬������
* @param hton	TNT�洢����ʵ��
* @param xid	prepare����id��mysql�ϲ�ı�ʶ����tnt��������id����Ҫת��
* @return �ɹ�����0��ʧ�ܷ���errorֵ
*/
int ha_tnt::tnt_trx_rollback_by_xid(handlerton *hton, XID* xid) {
	DBUG_ENTER("tnt_trx_rollback_by_xid");
	int ret = 0;
	TNTTrxSys *trxsys = tnt_db->getTransSys();
	TNTTransaction *trx = trxsys->getTrxByXID(xid);
	if (trx != NULL) {
		bool recoverPrepare = false;
		Connection *conn = NULL;
		//Ϊ�����ָ���prepare����ع�ʱ��Ҫ�������һ��connection
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
		//�Ѿ�����̨�߳�kill��
		ret = XA_OK;
	}

	DBUG_RETURN(ret);

	/**
	* ��ʱ��TNT�����Crash Recovery֮��ֱ�ӽ��ڴ�����Purge��NTSE���޷����лع��������commit�������
	* �������ȵ�TNT�ڴ�֧�ִ�������֮��Crash Recovery��ɺ����ѡ��Purge��NTSE����ʱ�ſ��Խ�Prepare����ع�
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
	} else  if (trxConn && tblStatus == TS_NON_TRX) { //�������Ӳ����������
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
* �ϲ�ѯ��tnt ���һ�ű�Ĳ�ѯ�ܷ�cache
* @param thd	T��ǰ����
* @param table_key	table cache �й���tableName��ָ��
* @param key_length tableName�ĳ���
* @param call_back	�����Ļص�����
* @param engine_data  ���������ݣ��������κ����ݣ�
* @return ����caching ����TRUE�����򷵻�FALSE
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
	// TNT����ʱ������MYSQL�ϲ�ʹ��query cache
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
	} else  if (trxConn && tblStatus == TS_NON_TRX) { //�������Ӳ����������
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
* �洢���棬information schema����
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
* ע��TNT�洢���浽mysql
*/
mysql_declare_plugin(tnt) {
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&tnt_storage_engine,
	"Tnt",
	"NetEase Corporation",
	"Transactional storage engine",
	PLUGIN_LICENSE_PROPRIETARY,
	ha_tnt::init,							/** �����ʼ�� */
	ha_tnt::exit,							/** ��ȫ�˳� */
	0x0010,									/** �汾��0.1 */
	tntStatus,								/** ״̬���� */
	tntVariables,							/** ���ò��� */
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
