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
#include "ntse_handler.h"
#endif
//#include "misc/Parser.h"
//#include "misc/Session.h"
//#include "misc/Syslog.h"
//#include "misc/RecordHelper.h"
//#include "misc/Record.h"
//#include "misc/ColumnGroupParser.h"
//#include "util/SmartPtr.h"
//#include "util/File.h"
//#include "misc/GlobalFactory.h"
//#include "misc/Global.h"
#include "misc/ParFileParser.h"

#ifndef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include "ntse_handler.h"
#endif
#include "api/Database.h"
#include "btree/Index.h"
//#include "ntse_binlog.h"
//#include "mysys_err.h"
#include "RowCache.h"
//#include "ntse_version.h"

#ifdef NTSE_PROFILE
#include "misc/Profile.h"
#endif

using namespace ntse;

//handlerton		*ntse_hton;			/** Handler singleton */
Config			*ntse_config = NULL;		/** 数据库配置 */
Database		*ntse_db = NULL;			/** 全局唯一的数据库 */
//NTSEBinlog		*ntse_binlog = NULL;		/** 全局唯一的binlog记录器 */
//CmdExecutor		*cmd_executor = NULL;		/** 全局唯一的命令执行器 */
//typedef DynHash<const char *, TableInfoEx *, InfoExHasher, Hasher<const char *>, InfoExEqualer> TblHash;
//static TblHash	openTables;			/** 已经打开的表 */
//static ntse::Mutex	openLock("openLock", __FILE__, __LINE__);			/** 保护已经打开的表的锁 */

//HandlerInfo *HandlerInfo::m_instance = NULL;
//HandlerInfo *handlerInfo = NULL;
//static bool isNtseLogBin = false;


///////////////////////////////////////////////////////////////////////////////
// 配置参数与状态变量 //////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
// 以下是全局配置，参见Config类说明
//static char	*ntse_tmpdir = (char *)".";
//static char	*ntse_version = NULL;
//static ulong	ntse_log_file_size = 0;
//static ulong	ntse_log_buf_size = 0;
//static unsigned long long ntse_page_buf_size = 0;
//static unsigned long long ntse_rec_buf_size = 0;
//static ulong	ntse_max_sessions = 0;
//static ulong	ntse_table_lock_timeout = 0;
//static ulong	ntse_checkpoint_interval = 0;
//ulong			ntse_sample_max_pages = 1000;
//static ulong	ntse_incr_size;	/** 堆、索引或大对象文件扩展页面数 */
//static ulong	ntse_binlog_buffer_size = 10 * 1024 * 1024;	/** binlog允许使用缓存的最大大小 */
//static char 	*ntse_binlog_method = (char *)"direct";	/** 设置ntse写binlog的方式,包括"mysql"表示by_mysql/"direct"表示direct/"cached"表示cached三种 */
#ifdef NTSE_PROFILE
//static int 		ntse_profile_summary = 0; /** 通过设置-1/0/1 来控制全局Profile */
//static int		ntse_thread_profile_autorun = 0; /** 通过设置0/1 来控制线程Profile是否自动开启*/
#endif
//static char		ntse_backup_before_recover = 0;
//static char		ntse_verify_after_recover = 0;
//static char		ntse_enable_mms_cache_update = 0;
//static char		ntse_enable_syslog = 1;	/** NTSE的log是否输出到mysqld.log */
//static char		ntse_directio = 1; /** 是否使用directio */

/*static void set_buf_page_from_para(uint *pages, const unsigned long long *para){
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
}*/

static u64	ntse_handler_use_count = 0;
static u64	ntse_handler_use_time = 0;
/*static u64	ntse_binlog_num_trans = 0;
static u64	ntse_binlog_num_logs = 0;
static u64	ntse_binlog_num_insert_logs = 0;
static u64	ntse_binlog_num_update_logs = 0;
static u64	ntse_binlog_num_delete_logs = 0;
static u64	ntse_binlog_cache_size = 0;
static u64	ntse_binlog_cached_logs = 0;*/
static Status	ntse_status;	/** NTSE全局统计信息 */

/**
 * 获取连接信息
 *
 * @return 连接信息
 */
THDInfo* getTHDInfo(THD *thd) {
	return *((THDInfo **)thd_ha_data(thd, tnt_hton));
}

/**
 * 设置连接信息
 *
 * @param THD 连接
 * @param info 连接信息
 */
/*void setTHDInfo(THD *thd, THDInfo *info) {
	*((THDInfo **)thd_ha_data(thd, ntse_hton)) = info;
}*/

/**
 * 检查连接信息是否设置，若没有则创建
 *
 * @param thd THD对象
 */
/*THDInfo* checkTHDInfo(THD *thd) {
	THDInfo *info = getTHDInfo(thd);
	assert(info != NULL);
	return info;
}*/

/**
 * 创建一个NTSE handler
 *
 * @param hton NTSE存储引擎实例
 * @param table handler要操作的表
 * @param mem_root 在此创建
 * @return 新创建的handler
 */
/*static handler* ntse_create_handler(handlerton *hton,
	TABLE_SHARE *table,
	MEM_ROOT *mem_root) {
	ftrace(ts.mysql, tout << hton << table << mem_root);
	return new (mem_root) ntse_handler(hton, table);
}*/

/**
 * 初始化NTSE存储引擎，打开数据库
 *
 * @param p handlerton
 * @return 成功返回0，失败返回1
 */
int ntse_handler::init(Database *db, Config *config) {
	ntse_db = db;
	ntse_config = config;
	return 0;
}

/**
 * 卸载NTSE存储引擎，释放所有资源
 *
 * @param p handlerton
 * @return 总是成功，返回0
 */
int ntse_handler::exit(void *p) {
	ntse_db = NULL;
	ntse_config = NULL;
	return 0;
}

ntse_handler::ntse_handler(handlerton *hton, TABLE_SHARE *table_arg): handler(hton, table_arg) {
	ftrace(ts.mysql, tout << this << hton << table_arg);
	m_session = NULL;
	m_ntseTable = NULL;
	m_ntseScan = NULL;
	m_session = NULL;
	m_errno = 0;
	m_errmsg[0] = '\0';
	ref_length = RID_BYTES + 1;
	m_wantLock = m_gotLock = IL_NO;
	m_replace = false;
	m_ntseIuSeq = NULL;
	m_lastRow = INVALID_ROW_ID;
	m_beginTime = 0;
	m_thd = NULL;
	m_conn = NULL;
	m_isRndScan = false;
	m_rowCache = NULL;
	m_lobCtx = new MemoryContext(Limits::PAGE_SIZE, 1);
	m_isReadAll = false;
	m_increment = 1;
	m_offset = 0;
	m_ignoreDup	= false;
	m_checkSameKeyAfterIndexScan = false;
	m_deferred_read_cache_size = 0;
}


/**
 * 返回表扫描代价。目前使用存储引擎示例中的默认实现
 *
 * @return 表扫描代价
 */
/*double ntse_handler::scan_time() {
	ftrace(ts.mysql, tout << this);
	return (double) (stats.records + stats.deleted) / 20.0+10;
}*/

/**
 * 返回读取指定条数记录的代价。目前使用存储引擎示例中的默认实现
 *
 * @return 读取代价
 */
/*double ntse_handler::read_time(ha_rows rows) {
	ftrace(ts.mysql, tout << this << rows);
	return (double) rows /  20.0 + 1;
}*/

/**
 * 重置handler状态
 *
 * @return 总是返回0
 */
int ntse_handler::reset() {
	ftrace(ts.mysql, tout << this);
	assert(!m_ntseScan);
	m_errno = 0;
	m_errmsg[0] = '\0';
	m_ignoreDup = false;
	m_replace = false;
	m_ntseIuSeq = NULL;
	m_lastRow = INVALID_ROW_ID;
	m_isRndScan = false;
	m_lobCtx->reset();
	bitmap_clear_all(&m_readSet);
	m_deferred_read_cache_size = 0;
	return 0;
}

/**
 * 关闭handler当前正在用的表。
 *
 * @return 成功返回0，失败返回1，不过NTSE总是返回0
 */
int ntse_handler::close(void) {
	ftrace(ts.mysql, tout << this);
	m_ntseTable = NULL;
	return 0;
}

/**
 * 插入一行
 *
 * @param buf 插入行的内容
 * @return 成功返回0，由于唯一性冲突失败返回HA_ERR_FOUND_DUPP_KEY，
 *   由于记录超长失败返回相应错误码
 */
int ntse_handler::write_row(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);

	int code = 0;//checkSQLLogBin();
	assert(m_thd->lex->sql_command != SQLCOM_SELECT);
	/*if (code) {
		return code;
	}*/

	if (table->next_number_field && buf == table->record[0]) {
		// 处理自增
		if ((code = update_auto_increment()))
			// 对于非事务的支持，直接返回错误码
			return code;
	}

	try {
		if (!m_replace) {
			// 即使是INSERT IGNORE在唯一性冲突时也需要返回HA_ERR_FOUND_DUPP_KEY，
			// 用于上层统计成功插入和冲突的各有多少条记录
			m_dupIdx = -1;
			if (m_ntseTable->insert(m_session, buf, false, &m_dupIdx, false, (void*)this) == INVALID_ROW_ID)
				code = HA_ERR_FOUND_DUPP_KEY;
		} else {
			assert(m_ignoreDup);
			if (m_ntseIuSeq) {
				// INSERT ... ON DUPLICATE KEY UPDATE在发生冲突后，若发现要UPDATE的新值与原记录相等
				// 则不会调用update_row，这样在INSERT ... SELECT ... ON DUPLICATE ...时会连接调用
				// write_row
				m_ntseTable->freeIUSequenceDirect(m_ntseIuSeq);
				m_ntseIuSeq = NULL;
			}
			m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
			m_ntseIuSeq = m_ntseTable->insertForDupUpdate(m_session, buf, false, (void*)this);
			if (m_ntseIuSeq)
				code = HA_ERR_FOUND_DUPP_KEY;
		}
	} catch (NtseException &e) {
		assert(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		code = reportError(e.getErrorCode(), e.getMessage(), false);
	}

	// 设置新的自增值
	setAutoIncrIfNecessary(buf);
	// TODO: 支持事务的时候，可能需要参考Innodb处理自增字段

	return code;
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
int ntse_handler::update_row(const uchar *old_data, uchar *new_data) {
	ftrace(ts.mysql, tout << this << old_data << new_data);
	assert(m_session && m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);

	try {
		if (m_ntseScan) {
			if (!m_ntseScan->isUpdateColumnsSet()) {
				u16 numUpdateCols;
				u16 *updateCols = transCols(table->write_set, &numUpdateCols);
				m_ntseScan->setUpdateColumns(numUpdateCols, updateCols);
			}
			if (!m_ntseTable->updateCurrent(m_ntseScan, new_data, false, &m_dupIdx, old_data, (void*)this))
				return HA_ERR_FOUND_DUPP_KEY;
		} else if (m_ntseIuSeq) {
			u16 numUpdateCols;
			u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			bool succ = m_ntseTable->updateDuplicate(m_ntseIuSeq, new_data, numUpdateCols, updateCols, &m_dupIdx, (void*)this);
			m_ntseIuSeq = NULL;
			m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
			if (!succ)
				return HA_ERR_FOUND_DUPP_KEY;
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
				return code;
			}
			assert(m_lastRow != INVALID_ROW_ID);
			McSavepoint mcSave(m_session->getMemoryContext());

			u16 numReadCols;
			u16 *readCols = transCols(table->read_set, &numReadCols);
			m_ntseScan = m_ntseTable->positionScan(m_session, OP_UPDATE, numReadCols, readCols, false);	// 不加表锁时不可能出异常
			byte *buf = (byte *)alloca(m_ntseTable->getTableDef(true, m_session)->m_maxRecSize);
			NTSE_ASSERT(m_ntseTable->getNext(m_ntseScan, buf, m_lastRow, true));

			u16 numUpdateCols;
			u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			m_ntseScan->setUpdateColumns(numUpdateCols, updateCols);
			bool succ;
			try {
				succ = m_ntseTable->updateCurrent(m_ntseScan, new_data, false, &m_dupIdx, old_data, (void*)this);
			} catch (NtseException &e) {
				endScan();
				throw e;
			}

			endScan();
			if (!succ)
				return HA_ERR_FOUND_DUPP_KEY;
		}
	} catch (NtseException &e) {
		assert(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		return reportError(e.getErrorCode(), e.getMessage(), false);
	}

	// 这里的更新自增值，可以参看InnoDB的实现注释，效果一样
	setAutoIncrIfNecessary(new_data);

	return 0;
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
int ntse_handler::delete_row(const uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);

	if (m_ntseScan) {
		/*
		 *	如果分区表不存在大对象存储，m_scan扫描的记录空间指向buf，而不是内部分配的内存。
		 *  分区表处理更新记录时，会将某一条更新记录先写入到另一个分区，然后在原分区采用删
		 *	除操作。如果m_scan扫描记录的空间指向buf，m_scan扫描记录会被更新到新记录内容，分
		 *	区插入操作完成之后，再回到原分区，用m_scan扫描时会发生记录不存在的错误
		 */
		if (!m_ntseTable->getTableDef()->hasLob()) {
			m_ntseScan->setCurrentData((byte*)buf);
		}
		
		m_ntseTable->deleteCurrent(m_ntseScan, (void*)this);
	}
	else if (m_ntseIuSeq) {
		m_ntseTable->deleteDuplicate(m_ntseIuSeq, (void*)this);
		m_ntseIuSeq = NULL;
		m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
	} else {
		assert(IntentionLock::isConflict(m_gotLock, IL_IX) && m_lastRow != INVALID_ROW_ID);
		McSavepoint mcSave(m_session->getMemoryContext());

		u16 numReadCols;
		u16 *readCols = transCols(table->read_set, &numReadCols);
		m_ntseScan = m_ntseTable->positionScan(m_session, OP_DELETE, numReadCols, readCols, false);	// 不加表锁时不可能出异常
		byte *buf = (byte *)alloca(m_ntseTable->getTableDef(true, m_session)->m_maxRecSize);
		NTSE_ASSERT(m_ntseTable->getNext(m_ntseScan, buf, m_lastRow, true));
		m_ntseTable->deleteCurrent(m_ntseScan, (void*)this);
		endScan();
	}

	return 0;
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
int ntse_handler::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
	enum ha_rkey_function find_flag) {
	ftrace(ts.mysql, tout << this << buf << key << (u64)keypart_map << find_flag);
	assert(m_session);

	if (m_ntseIuSeq) { // 分区表INSERT FOR DUPLICATE UPDATE时，使用index_read_map而不是rnd_pos取记录
		NTSE_ASSERT(find_flag == HA_READ_KEY_EXACT);
		NTSE_ASSERT(active_index == m_ntseIuSeq->getDupIndex());
		transKey(active_index, key, keypart_map, &m_indexKey);
		NTSE_ASSERT(recordHasSameKey( m_ntseTable->getTableDef(true, m_session)->m_indice[active_index], m_ntseIuSeq->getScanHandle()->getIdxKey(), &m_indexKey));
		m_ntseIuSeq->getDupRow(buf);
		return 0;
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

	return indexRead(buf, key, keypart_map, forward, includeKey, find_flag == HA_READ_KEY_EXACT);
}

/**
 * 索引反向扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @param key 索引搜索键
 * @param keypart_map 搜索键中包含哪些索引属性
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map) {
	ftrace(ts.mysql, tout << this << buf << key << (u64)keypart_map);
	return indexRead(buf, key, keypart_map, false, true, false);
}

/**
 * 索引正向全扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_first(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	return indexRead(buf, NULL, 0, true, true, false);
}

/**
 * 索引反向全扫描
 *
 * @param buf 保存输出记录的缓冲区
 * @return 成功返回0，找不到记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_last(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	return indexRead(buf, NULL, 0, false, true, false);
}

/**
 * 读取下一条记录
 *
 * @param buf 用于存储输入记录内容
 * @return 成功返回0，没有记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_next(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseScan && m_ntseScan->getType() == ST_IDX_SCAN);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	return fetchNext(buf);
}

/**
 * 读取前一条记录
 *
 * @param buf 用于存储输入记录内容
 * @return 成功返回0，没有记录返回HA_ERR_END_OF_FILE，等表锁超时返回HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_prev(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseScan && m_ntseScan->getType() == ST_IDX_SCAN);
	DBUG_ENTER("ntse_handler::index_prev");
	ha_statistic_increment(&SSV::ha_read_prev_count);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	DBUG_RETURN(fetchNext(buf));
}

/**
 * 结束索引扫描，释放扫描句柄及资源
 *
 * @return 总是返回0
 */
int ntse_handler::index_end() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ntse_handler::index_end");
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
int ntse_handler::rnd_init(bool scan) {
	ftrace(ts.mysql, tout << this << scan);
	assert(m_session);
	DBUG_ENTER("ntse_handler::rnd_init");

	endScan();	// 作为子查询时可能会扫描多遍
	//fix bug#23575，删除代码	if (scan && m_rowCache) {delete m_rowCache;	m_rowCache = NULL;}。
	//将m_rowCache清除工作交给ntse_handler::external_lock，单个sql语句内部重用m_rowCache，避免bug中
	//position->rnd_init->rnd_pos调用顺序时，访问被清除的m_rowCache.

	int code = 0;//checkSQLLogBin();
	/*if (code) {
		DBUG_RETURN(code);
	}*/

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
int ntse_handler::rnd_end() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ntse_handler::rnd_end");
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
int ntse_handler::rnd_next(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session);
	DBUG_ENTER("ntse_handler::rnd_next");
	ha_statistic_increment(&SSV::ha_read_rnd_next_count);
	if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do table scan on index only table", false);
		DBUG_RETURN(code);
	}

	// 为了能正确的得到read_set的信息,在这里才初始化扫描句柄
	if (!m_ntseScan) {
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
bool ntse_handler::is_twophase_filesort() {
	bool twophase = true;
	if (m_thd != NULL && m_thd->lex != NULL) {
		MY_BITMAP tmp_set;
		uint32 tmp_set_buf[Limits::MAX_COL_NUM / 8 + 1];	/** 位图所需要的缓存 */
		bitmap_init(&tmp_set, tmp_set_buf, m_ntseTable->getTableDef()->m_numCols, FALSE);
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
void ntse_handler::position(const uchar *record) {
	ftrace(ts.mysql, tout << this << record);
	assert(m_session && (m_ntseScan || m_gotLock == IL_SIX || m_gotLock == IL_X));
	assert(m_lastRow != INVALID_ROW_ID);

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
				m_rowCache = new RowCache(m_deferred_read_cache_size, m_ntseTable->getTableDef(true, m_session),
					numReadCols, readCols, &m_readSet);
			}
		}
		if (m_rowCache) {
			assert(m_rowCache->hasAttrs(&m_readSet));
			long id = m_rowCache->put(m_lastRow, record);
			if (id >= 0) {
				ref[0] = REF_CACHED_ROW_ID;
				RID_WRITE((RowId)id, ref + 1);
				return;
			}
		}
	}
	ref[0] = REF_ROWID;
	RID_WRITE(m_lastRow, ref + 1);
	if (m_gotLock == IL_IS)
		m_wantLock = IL_S;
	else if (m_gotLock == IL_IX)
		m_wantLock = IL_SIX;
	return;
}

/**
 * 根据记录位置读取记录
 *
 * @param buf 存储记录输出内容
 * @param pos 记录位置
 * @return 总是返回0
 */
int ntse_handler::rnd_pos(uchar *buf, uchar *pos) {
	ftrace(ts.mysql, tout << this << buf << pos);
	assert(m_session);
	DBUG_ENTER("ntse_handler::rnd_pos");
	ha_statistic_increment(&SSV::ha_read_rnd_count);

	if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do positional scan on index only table", false);
		DBUG_RETURN(code);
	}

	if (!m_ntseIuSeq) {
		// 为了能正确的得到read_set的信息,在这里才初始化扫描句柄
		if (!m_ntseScan) {
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
			NTSE_ASSERT(m_ntseTable->getNext(m_ntseScan, buf, rid, true));
		}
	} else {
		assert(pos[0] == REF_ROWID && RID_READ(pos + 1) == m_ntseIuSeq->getScanHandle()->getCurrentRid());
		m_ntseIuSeq->getDupRow(buf);
	}
	table->status = 0;

	DBUG_RETURN(0);
}

/**
 * 删除所有记录
 * 用于实现truncate，delete from TABLE_NAME命令也可能调用此接口
 *
 * @return 成功返回0，失败返回错误码
 */
int ntse_handler::truncate() {
	ftrace(ts.mysql, tout << this);
	assert(m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);
	assert(m_ntseTable->getMetaLock(m_session) == IL_S);
	int code = 0;
	try {
		m_ntseTable->upgradeMetaLock(m_session, IL_S, IL_X, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		reportError(e.getErrorCode(), e.getMessage(), false);
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	}
	assert(m_gotLock == IL_IX || m_gotLock == IL_X);
	if (m_gotLock == IL_IX) {
		m_wantLock = IL_X;
		try {
			m_ntseTable->upgradeLock(m_session, IL_IX, IL_X, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			reportError(e.getErrorCode(), e.getMessage(), false);
			return HA_ERR_LOCK_WAIT_TIMEOUT;
		}
		m_gotLock = IL_X;
	}
	try {
		deleteAllRowsReal();
		//truncate和delete from TABLE_NAME都有可能调用本函数，
		//而delete from TABLE_NAME不需要删除存在的压缩字典
		/*bool isTruncateOper = (m_thd->lex->sql_command == SQLCOM_TRUNCATE);
		ntse_db->truncateTable(m_session, m_ntseTable, false, isTruncateOper);*/
	} catch (NtseException &e) {
		code = reportError(e.getErrorCode(), e.getMessage(), false);
		return code;
	}
	// 如果有自增字段的情况，自增值设为1
	reset_auto_increment_if_nozero(1);

	return code;
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
THR_LOCK_DATA** ntse_handler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
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
				lock_type = TL_WRITE;
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
int ntse_handler::external_lock(THD *thd, int lock_type) {
	ftrace(ts.mysql, tout << this << thd << lock_type);
	DBUG_ENTER("ntse_handler::external_lock");
	int code = 0;
	if (lock_type == F_UNLCK) {
		if (m_ntseIuSeq) {
			m_ntseTable->freeIUSequenceDirect(m_ntseIuSeq);
			m_ntseIuSeq = NULL;
		}

		if (!m_thd)	// mysql_admin_table中没有检查加锁是否成功
			DBUG_RETURN(0);
		assert(m_session);

		endScan();

		// 在进行OPTIMIZE等操作时已经释放了表锁
		if (m_gotLock != IL_NO) {
			m_ntseTable->unlock(m_session, m_gotLock);
			m_gotLock = IL_NO;
		}
		if (m_ntseTable->getMetaLock(m_session) != IL_NO)
			m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;

		assert(m_beginTime);
		ntse_handler_use_count++;
		ntse_handler_use_time += System::microTime() - m_beginTime;
		m_beginTime = 0;

		/*if (getTHDInfo(m_thd)->m_handlers.getSize() == 1) {
			m_conn->setStatus("Idle");
			getTHDInfo(m_thd)->resetNextCreateArgs();
		}*/

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

		//attachToTHD(thd, "ntse_handler::external_lock");

		// 分配会话
		/*assert(!m_session);
		m_session = ntse_db->getSessionManager()->allocSession("ntse_handler::external_lock", m_conn, 100);
		if (!m_session) {
			detachFromTHD();
			DBUG_RETURN(HA_ERR_NO_CONNECTION);
		}*/

		// 加表锁
		assert(m_ntseTable->getMetaLock(m_session) != IL_NO);
		//m_ntseTable->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
		//此时必定是非事务连接，非事务操作事务表，只能加S或者IS表锁
		if (m_ntseTable->getTableDef()->getTableStatus() == TS_TRX && 
			((thd->lex->sql_command != SQLCOM_SELECT && thd->lex->sql_command != SQLCOM_INSERT_SELECT) || m_wantLock != IL_IS)) {
			char msg[256];
			System::snprintf_mine(msg, 256, "Non-Transactional connection only support select transaction table(%s) on IL_IS mode", 
				m_ntseTable->getTableDef()->m_name);
			code = reportError(NTSE_EC_NOT_SUPPORT, msg, false);
			m_ntseTable->unlockMeta(m_session, IL_S);
			ntse_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			detachFromTHD();
			DBUG_RETURN(code);
		}

		try {
			if (m_wantLock == IL_X)
				ntse_db->getSyslog()->log(EL_LOG, "acquire IL_X on table %s. SQL:\n%s", m_ntseTable->getTableDef()->m_name, currentSQL());
			m_ntseTable->lock(m_session, m_wantLock, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			m_gotLock = m_wantLock;
		} catch (NtseException &e) {
			code = reportError(e.getErrorCode(), e.getMessage(), false);
			m_ntseTable->unlockMeta(m_session, IL_S);
			ntse_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			detachFromTHD();
			DBUG_RETURN(code);
		}
		/*if (getTHDInfo(m_thd)->m_handlers.getSize() == 1) {
			m_conn->setStatus("Busy");
			m_conn->getLocalStatus()->countIt(OPS_OPER);
		}*/

		if (!m_beginTime)
			m_beginTime = System::microTime();
	}
	DBUG_RETURN(code);
}

/** 在LOCK TABLES括起的语句中MySQL在语句开始时会调用此函数，语句结束时不会调用
 *
 * @param thd 连接
 * @param lock_type 对表加的锁
 * @return 总是返回0
 */
int ntse_handler::start_stmt(THD *thd, thr_lock_type lock_type) {
	ftrace(ts.mysql, tout << this << thd << lock_type);
	DBUG_ENTER("ntse_handler::start_stmt");
	if (thd != m_thd)
		DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "THD changed during sql processing", false));
	//THDInfo *info = getTHDInfo(m_thd);
	/*if (info->m_handlers.getHeader()->getNext()->get() == this) {
		m_conn->setStatus("Busy");
		m_conn->getLocalStatus()->countIt(OPS_OPER);
	}*/
	DBUG_RETURN(0);
}

/**
 * 删除表
 * 在删除表之前，所有对表的引用都已经被关闭
 *
 * @param name 表文件路径，不含后缀名
 * @return 成功返回0，表不存在返回HA_ERR_NO_SUCH_TABLE，其它错误返回HA_ERR_GENERIC
 */
int ntse_handler::delete_table(const char *name) {
	ftrace(ts.mysql, tout << this << name);

	attachToTHD(ha_thd(), "ntse_handler::delete_table");
	int ret = 0;
	m_session = ntse_db->getSessionManager()->allocSession("ntse_handler::delete_table", m_conn, 100);
	if (!m_session) {
		ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
	} else {
		try {
			dropTableReal(name);
		} catch (NtseException &e) {
			ret = reportError(e.getErrorCode(), e.getMessage(), false);
		}
	}
	if (m_session) {
		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}
	detachFromTHD();

	return ret;
}

/**
 * 重命名表。调用此函数时handler对象为刚刚创建没有open的对象
 *
 * @param from 表文件原路径
 * @param to 表文件新路径
 */
int ntse_handler::rename_table(const char *from, const char *to) {
	ftrace(ts.mysql, tout << this << from << to);
	assert(!m_session);

	attachToTHD(ha_thd(), "ntse_handler::rename_table");
	int ret = 0;
	m_session = ntse_db->getSessionManager()->allocSession("ntse_handler::rename_table", m_conn, 100);
	if (!m_session) {
		ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
	} else {
		char *from_copy = System::strdup(from);
		char *to_copy = System::strdup(to);
		normalizePathSeperator(from_copy);
		normalizePathSeperator(to_copy);
		try {
			renameTableReal(from_copy, to_copy);
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

	return ret;
}

/**
 * 估计指定索引中在[min_key, max_key]之间的记录数
 *
 * @param inx 第几个索引
 * @param min_key 下限，可能为NULL
 * @param max_key 上限，可能为NULL
 */
ha_rows ntse_handler::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
	ftrace(ts.mysql, tout << this << inx << min_key << max_key);

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
	u64 rows = m_ntseTable->getIndice()->getIndex(inx)->recordsInRangeSecond(m_session,
		pMinSr, includeMin, pMaxSr, includeMax);
	
	return rows;
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

/** 判断一个索引属性是否为前缀
 * @param table_arg 表定义
 * @param key_part 索引属性
 * @return 是否为前缀
 */
bool ntse_handler::isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part) {
	// 本来应该通过HA_PART_KEY_SEG标志来判断是否是为前缀，但MySQL并没有正确的设置这个值，
	// 这里参考InnoDB中的判断逻辑
	Field *field = table_arg->field[key_part->field->field_index];
	assert(!strcmp(field->field_name, key_part->field->field_name));
	if (field->type() == MYSQL_TYPE_BLOB || field->type() == MYSQL_TYPE_MEDIUM_BLOB 
		|| (key_part->length < field->pack_length() && field->type() != MYSQL_TYPE_VARCHAR)
		|| (field->type() == MYSQL_TYPE_VARCHAR && key_part->length < field->pack_length() - ((Field_varstring*)field)->length_bytes))
		return true;
	return false;
}

void ntse_handler::prepareAddIndexReal() throw(NtseException) {
	// 释放表锁，便于NTSE底层的处理
	m_ntseTable->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
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
int ntse_handler::add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys,
							handler_add_index **add) {
	ftrace(ts.mysql, tout << this << table_arg << key_info << num_of_keys);

	assert(m_conn && m_session);

	//int i = 0, j = 0;
	//bool online = false;

	//McSavepoint mc(m_session->getMemoryContext());
	//IndexDef **indexDefs = (IndexDef **)m_session->getMemoryContext()->alloc(sizeof(IndexDef *) * num_of_keys);
	int ret = 0;
	try {
		/*if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly) {
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
			if (primaryKey && m_ntseTable->getTableDef(true, m_session)->m_numIndice)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Primary key must be the first index.");

			// 由于MySQL认为非唯一性索引总是在唯一性索引之后，NTSE目前没有这么处理，为保证正确性，要求唯一性索引
			// 必须在非唯一性索引之前创建
			if (unique && m_ntseTable->getTableDef(true, m_session)->getNumIndice(false) > 0)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unique index must be created before non-unique index.");

			ColumnDef **columns = new (m_session->getMemoryContext()->alloc(sizeof(ColumnDef *) * key->key_parts)) ColumnDef *[key->key_parts];
			for (uint j = 0; j < key->key_parts; j++) {
				if (isPrefix(table_arg, key->key_part + j))
					NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Prefix key is not supported: %s", key->name);
				columns[j] = m_ntseTable->getTableDef(true, m_session)->m_columns[key->key_part[j].field->field_index];
			}

			indexDefs[i] = new IndexDef(key->name, key->key_parts, columns, unique, primaryKey, online);
		}

		// 释放表锁，便于NTSE底层的处理
		m_ntseTable->unlock(m_session, m_gotLock);
		m_gotLock = IL_NO;
		m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

		ntse_db->addIndex(m_session, m_ntseTable, (u16)num_of_keys, (const IndexDef **)indexDefs);
		if (online) {
			for (j = 0; j < (int)num_of_keys; j++) {
				indexDefs[j]->m_online = false;
			}
			ntse_db->addIndex(m_session, m_ntseTable, (u16)num_of_keys, (const IndexDef **)indexDefs);
		}*/
		addIndexReal(table_arg, key_info, num_of_keys, add);
	} catch (NtseException &e) {
		if (e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION)
			m_dupIdx = MAX_KEY;
		ret = reportError(e.getErrorCode(), e.getMessage(), false, e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION);
	}

	return ret;
}

/**
 * 完成创建索引操作步骤，或提交完成或回滚之前已创建的索引
 *
 * @param  add 由add_index操作创建的新建索引上下文对象，主要用于回滚
 * @param  commit true代表commit, false代表需要rollback
 * @return 成功返回0，错误返回错误码
 */
int ntse_handler::final_add_index(handler_add_index *add, bool commit) {
	int ret = 0;
	try {
		finalAddIndexReal(add, commit);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}
	delete add;
	return ret;
}



/**
 * 准备删除索引，在NTSE中其实已经把索引给删了
 *
 * @param table_arg 表定义
 * @param key_num 要删除的索引序号数组，经验证一定是顺序排好序的
 * @param num_of_keys 要删除的索引个数
 */
int ntse_handler::prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys) {
	ftrace(ts.mysql, tout << this << table_arg << key_num << num_of_keys);
	assert(m_conn && m_session);

	int ret = 0;
	try {
		/*if (num_of_keys > 1)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can't drop multiple indice in one statement.");
		if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not drop index of index only table.");
		ntse_db->dropIndex(m_session, m_ntseTable, key_num[0]);*/
		dropIndexReal(table_arg, key_num, num_of_keys);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}

	return ret;
}

/**
 * 完成删除索引操作，在NTSE中什么也不干
 *
 * @param table_arg 表定义
 * @return 总是返回0
 */
int ntse_handler::final_drop_index(TABLE *table_arg) {
	ftrace(ts.mysql, tout << this << table_arg);
	return 0;
}

/** 更新统计信息
 *
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁时返回HA_ADMIN_FAILED
 */
/*int ntse_handler::analyze(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::analyze");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	m_ntseTable->getHeap()->updateExtendStatus(m_session, ntse_sample_max_pages);
	for (u16 i = 0; i < m_ntseTable->getTableDef(true, m_session)->m_numIndice; i++) {
		m_ntseTable->getIndice()->getIndex(i)->updateExtendStatus(m_session, ntse_sample_max_pages);
	}
	if (m_ntseTable->getLobStorage())
		m_ntseTable->getLobStorage()->updateExtendStatus(m_session, ntse_sample_max_pages);

	DBUG_RETURN(HA_ADMIN_OK);
}*/

/** 优化表数据
 *
 * @param thd 使用本handler的连接
 * @param check_opt 不用
 * @return 成功返回HA_ADMIN_OK，没有成功加表锁或失败时返回HA_ADMIN_FAILED
 */
int ntse_handler::optimize(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::optimize");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	//检查连接私有属性ntse_keep_compress_dictionary_on_optimize
	//bool keepDict = THDVAR(m_thd, keep_dict_on_optimize);
	//TODO
	bool keepDict = false;//现在TNT暂不支持optimize压缩

	// 释放表锁，便于NTSE底层的处理
	m_ntseTable->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
	m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

	int ret = 0;
	try {
		optimizeTableReal(keepDict, false);
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
int ntse_handler::check(THD* thd, HA_CHECK_OPT* check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::check");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	// 释放表锁，便于NTSE底层的处理
	m_ntseTable->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
	m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

	int ret = 0;
	try {
		m_ntseTable->verify(m_session);
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
int ntse_handler::repair(THD* thd, HA_CHECK_OPT* check_opt) {
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::repair");

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
/*bool ntse_handler::get_error_message(int error, String* buf) {
	ftrace(ts.mysql, tout << this << error << buf);
	if (error != m_errno)
		return false;
	buf->append(m_errmsg);
	m_errno = 0;
	return true;
}*/

/** 将路径中的目录分隔符统一转化成/ */
void ntse_handler::normalizePathSeperator(char *path) {
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
CollType ntse_handler::getCollation(CHARSET_INFO *charset) throw(NtseException) {
	if (!charset->sort_order)
		return COLL_BIN;
	else if (charset == &my_charset_gbk_chinese_ci)
		return COLL_GBK;
	else if (charset == &my_charset_utf8_general_ci)
		return COLL_UTF8;
	else if (charset == &my_charset_utf8mb4_general_ci)
		return COLL_UTF8MB4;
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
int ntse_handler::reportError(ErrorCode errCode, const char *msg, bool fatal, bool warnOrErr) {
	if (ntse_db)
		ntse_db->getSyslog()->log(fatal? EL_PANIC: EL_ERROR, "%s", msg);

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

	return m_errno;
}

/**	将NTSE的Error Code转换为MySQL上层的Error Code，并且标识事务状态
* @param code	TNT层面的Error Code
* @param thd	出错操作对应的MySQL线程
* @return		返回MySQL的Error Code
*/
int ntse_handler::excptToMySqlError(ErrorCode code) {
	if (code == NTSE_EC_INDEX_UNQIUE_VIOLATION)
		return HA_ERR_FOUND_DUPP_KEY;
	else if (code == NTSE_EC_ROW_TOO_LONG)
		return HA_ERR_TO_BIG_ROW;
	else if (code == NTSE_EC_LOCK_TIMEOUT)
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	else if (code == NTSE_EC_DEADLOCK)
		return HA_ERR_LOCK_DEADLOCK;
	else if (code == NTSE_EC_NOT_SUPPORT) 
		return HA_ERR_UNSUPPORTED;
	else if (code == NTSE_EC_FILE_NOT_EXIST)
		return HA_ERR_NO_SUCH_TABLE;
	else
		return HA_ERR_LAST + (int)code + 1;
}

/**	将MySQL上层的HA错误码转换成真正的ERR错误码
* @param code	mysql上层的HA错误码Error Code
* @param thd	出错操作对应的MySQL线程
* @return		返回MySQL的Error Code
*/
int ntse_handler::getMysqlErrCodeTextNo(int code) {
	switch (code) {
		case HA_ERR_FOUND_DUPP_KEY:
			return ER_DUP_ENTRY;
		case HA_ERR_LOCK_WAIT_TIMEOUT:
			return ER_LOCK_WAIT_TIMEOUT;
		case HA_ERR_LOCK_DEADLOCK:
			return ER_LOCK_DEADLOCK;
		case HA_ERR_UNSUPPORTED:
			return ER_UNSUPPORTED_EXTENSION;
		case HA_ERR_NO_SUCH_TABLE:
			return ER_NO_SUCH_TABLE;
		default:
			return 0;	
	}
}


/** 
 * 获取当前SQL
 * @return 当前SQL
 */
const char* ntse_handler::currentSQL() {
	return (m_thd == NULL) ? "NULL" : (m_thd->query() == NULL ? "NULL" : m_thd->query());
}
/**
 * 检查是否需要升级表锁，需要时加锁
 *
 * @return 是否成功
 */
bool ntse_handler::checkTableLock() {
	if (m_wantLock != m_gotLock) {
		assert(m_wantLock > m_gotLock);
		ntse_db->getSyslog()->log(EL_LOG, "Upgrade lock %s to %s on table %s. SQL:\n%s",
			IntentionLock::getLockStr(m_gotLock), IntentionLock::getLockStr(m_wantLock),
			m_ntseTable->getTableDef()->m_name, currentSQL());

		try {
			if (m_gotLock == IL_IS && m_wantLock == IL_S) {
				m_ntseTable->upgradeLock(m_session, m_gotLock, m_wantLock, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			} else if (m_gotLock == IL_IX && m_wantLock == IL_SIX) {
				m_ntseTable->downgradeLock(m_session, IL_IX, IL_IS, __FILE__, __LINE__);
				m_ntseTable->upgradeLock(m_session, IL_IS, IL_SIX, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			} else {
				char buf[100];
				System::snprintf_mine(buf, sizeof(buf), "Upgrade table lock from %s to %s is unexpected.",
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
 * @param idxScan 是否为索引扫描，默认为false
 * @param idx 若为索引扫描，则使用的索引num
 * @return 各属性号，从会话的MemoryContext中分配
 */
u16* ntse_handler::transCols(MY_BITMAP *bitmap, u16 *numCols, bool idxScan, uint idx) {
	
	int columns = bitmap_bits_set(bitmap);

	// TODO 更好的处理不读取任何属性的情况
	if (columns == 0) {
		*numCols = 1;
		u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16));

		if (idxScan) {
			// 索引扫描，读取索引的第一列
			assert(idx < m_ntseTable->getTableDef(true, m_session)->m_numIndice);
			IndexDef *indexDef = m_ntseTable->getTableDef(true, m_session)->m_indice[idx];

			r[0] = indexDef->m_columns[0];
		} else {
			// 全表扫描，读取表的第一列
			r[0] = 0;
		}
		return r;
	}
	*numCols = (u16)columns;
	u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16) * columns);
	u16 readCols = 0, i = 0;
	Field **field;
	for (field = table->field; *field; field++, i++) {
		if (bitmap_is_set(bitmap, (*field)->field_index)) {
			r[readCols++] = i;
		}
	}
	assert(readCols == columns);
	return r;
}

/** 判断指定的搜索方式是否包含键值本身
 * @param flag 搜索方式
 * @param lowerBound true表示是扫描范围的下界，false表示是上界
 * @return 是否包含键值本身
 */
bool ntse_handler::isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound) {
	switch (flag) {
		/*case HA_READ_KEY_EXACT:
		case HA_READ_KEY_OR_NEXT:
		case HA_READ_KEY_OR_PREV:
		case HA_READ_PREFIX:
		case HA_READ_PREFIX_LAST:
		case HA_READ_PREFIX_LAST_OR_PREV:
			return true;
		case HA_READ_AFTER_KEY:
			return !lowerBound;
		default:
			return false;*/

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
bool ntse_handler::transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out) {
	assert(idx < m_ntseTable->getTableDef(true, m_session)->m_numIndice);
	IndexDef *indexDef = m_ntseTable->getTableDef(true, m_session)->m_indice[idx];

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

	return RecordOper::convertKeyMP(m_ntseTable->getTableDef(true, m_session), indexDef, &mysqlKey, out);
}

/**
 * 解析用key_part_map结构表示的搜索键中包含哪些索引属性信息
 *
 * @param idx 第几个索引
 * @param keypart_map 搜索键中包含哪些索引属性信息
 * @param key_len OUT，搜索键长度
 * @param num_cols OUT，搜索键中包含几个索引属性
 */
void ntse_handler::parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols) {
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
OpType ntse_handler::getOpType() {
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
int ntse_handler::fetchNext(uchar *buf) {
	table->status = STATUS_NOT_FOUND;
	if (!checkTableLock())
		return HA_ERR_LOCK_WAIT_TIMEOUT;

	m_lobCtx->reset();
	if (!m_ntseTable->getNext(m_ntseScan, buf, INVALID_ROW_ID, true))
		return HA_ERR_END_OF_FILE;
	if (m_ntseScan->getType() == ST_IDX_SCAN
		&& m_checkSameKeyAfterIndexScan
		&& !recordHasSameKey(m_ntseTable->getTableDef(true, m_session)->m_indice[active_index], m_ntseScan->getIdxKey(), &m_indexKey))
		return HA_ERR_END_OF_FILE;
	m_lastRow = m_ntseScan->getCurrentRid();
	table->status = 0;
	return 0;
}

/** 若正在进行扫描则结束之
 * @post 若正在进行扫描，则底层的扫描已经结束，会话的MemoryContext被重置
 *   到开始扫描前的状态
 */
void ntse_handler::endScan() {
	assert(m_session);
	m_isRndScan = false;
	if (m_ntseScan) {
		m_ntseTable->endScan(m_ntseScan);
		m_ntseScan = NULL;
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
int ntse_handler::indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact) {
	m_lobCtx->reset();
	endScan();	// 多区域范围查询时可能会扫描多次，不调用index_end

	int code;/* = checkSQLLogBin();
	if (code) {
		return code;
	}*/

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();

	initReadSet(true);

	bool singleFetch;
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols, true, active_index);
	transKey(active_index, key, keypart_map, &m_indexKey);
	if (exact
		&& m_ntseTable->getTableDef(true, m_session)->m_indice[active_index]->m_unique
		&& m_indexKey.m_numCols == m_ntseTable->getTableDef(true, m_session)->m_indice[active_index]->m_numCols)
		singleFetch = true;
	else
		singleFetch = false;

	OpType opType = getOpType();
	try {
		IndexScanCond cond(active_index, m_indexKey.m_numCols? &m_indexKey: NULL, forward, includeKey, singleFetch);
		m_ntseScan = m_ntseTable->indexScan(m_session, opType, &cond, numReadCols, readCols, false, m_lobCtx);
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

/** 更新HA_CREATE_INFO信息
 * @param create_info	信息对象
 */
/*void ntse_handler::update_create_info( HA_CREATE_INFO *create_info ) {
	if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
		ntse_handler::info(HA_STATUS_AUTO);
		create_info->auto_increment_value = stats.auto_increment_value;
	}
}*/

/** 比较记录的键值与指定键值是否相等
 *
 * @param buf 记录
 * @param indexDef 索引定义
 * @param key1 KEY_PAD格式的键值
 * @param key2 KEY_PAD格式的键值
 * @return 是否相等
 */
bool ntse_handler::recordHasSameKey(const IndexDef *indexDef, const SubRecord *key1, const SubRecord *key2) {
	assert(key1->m_format == KEY_PAD);
	assert(key2->m_format == KEY_PAD);

	TableDef *tableDef = m_ntseTable->getTableDef(true, m_session);
	return !RecordOper::compareKeyPP(tableDef, key1, key2, indexDef);
}

/**
 * 初始化表扫描句柄
 * @pre m_readSet应该要设置正确
 */
void ntse_handler::beginTblScan() {
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols);

	OpType opType = getOpType();
	try {
		if (m_isRndScan) {
			m_ntseScan = m_ntseTable->tableScan(m_session, opType, numReadCols, readCols, false, m_lobCtx);
		} else {
			m_ntseScan = m_ntseTable->positionScan(m_session, opType, numReadCols, readCols, false, m_lobCtx);
		}
	} catch(NtseException &) {
		NTSE_ASSERT(false);		// 不加表锁时不可能会失败
	}
}

/** 用于各种扫描之前初始化m_readSet的信息
 * @param idxScan	true表示索引扫描，false表示堆扫描
 * @param posScan	true表示是rnd_pos扫描，false可以是其他表扫描或者索引扫描
 */
void ntse_handler::initReadSet( bool idxScan, bool posScan/* = false */) {
	assert(m_thd);
	assert(!(idxScan && posScan));	// 必须不可能既是索引扫描又是rnd_pos扫描

	bool isReadAll = false;
	//if ((opt_bin_log && !isBinlogSupportable()) || posScan) {
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
	//}

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



/** 得到当前句柄的THD对象
* @return 返回m_thd对象
*/
THD* ntse_handler::getTHD() {
	return m_thd;
}

/** 对于更新操作（insert/delete/update）检查SQL_LOG_BIN参数是否有被设置，如果被设置给出警告
 * @return 0表示参数未被设置，无警告；非0表示参数被设置，给出相应的警告信息
 */
/*int ntse_handler::checkSQLLogBin() {
	int code = 0;
	if (m_thd->lex->sql_command != SQLCOM_SELECT && !m_thd->sql_log_bin_toplevel)
		code = reportError(NTSE_EC_NOT_SUPPORT, "SQL_LOG_BIN variable is not supported by NTSE, please set to 1.", false);
	
	return code;
}*/

void ntse_handler::ntse_drop_database(handlerton *hton, char* path) {
	return;
}
#ifdef EXTENDED_FOR_COMMIT_ORDERED
void ntse_handler::ntse_commit_ordered(handlerton *hton, THD *thd, bool all) {
	return;
}
#endif
int ntse_handler::ntse_commit_trx(handlerton *hton, THD* thd, bool all) {
	return 0;
}

int ntse_handler::ntse_rollback_trx(handlerton *hton, THD *thd, bool all) {
	return 0;
}

int ntse_handler::ntse_xa_prepare(handlerton *hton, THD* thd, bool all) {
	return 0;
}
