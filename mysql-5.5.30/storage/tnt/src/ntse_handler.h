/**
 * NTSE存储引擎接口定义
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */
#ifndef _NTSE_HANDLER_H_
#define _NTSE_HANDLER_H_

#include "api/Table.h"
#include "misc/Session.h"
#include "THDInfoBase.h"

using namespace ntse;	// Make source insight happy

/** MySQL连接附加信息 */
struct ConnUserData {
	u32			m_thdId;	/** THD的ID */

	ConnUserData(u32 thdId) {
		m_thdId = thdId;
	}
};

//struct CmdInfo;
//class  ntse_handler;

extern handlerton *tnt_hton;

//struct TableInfoEx;
class RowCache;
//class NTSEBinlog;
/** NTSE存储引擎操作接口 */
class ntse_handler: public handler {
public:
	static int init(Database *db, Config *config);
	static int exit(void *p);

	static int ntse_close_connection(handlerton *hton, THD* thd);
	static void ntse_drop_database(handlerton *hton, char* path);
#ifdef EXTENDED_FOR_COMMIT_ORDERED
	static void ntse_commit_ordered(handlerton *hton, THD* thd, bool all);
#endif
	static int ntse_commit_trx(handlerton *hton, THD* thd, bool all);
	static int ntse_rollback_trx(handlerton *hton, THD *thd, bool all);
	static int ntse_xa_prepare(handlerton *hton, THD* thd, bool all);
	//static int ntseBinlogFunc(handlerton *hton, THD *thd, enum_binlog_func fn, void *arg);

	ntse_handler(handlerton *hton, TABLE_SHARE *table_arg);
	~ntse_handler() {
		delete m_lobCtx;
	}
	//bool check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes);
	//uint alter_table_flags(uint flags);
	//double scan_time();
	double read_time(ha_rows rows);
	//int open(const char *name, int mode, uint test_if_locked);
	int reset();
	int close(void);
	int write_row(uchar *buf);
	int update_row(const uchar *old_data, uchar *new_data);
	int delete_row(const uchar *buf);
	int index_read_map(uchar * buf, const uchar *key, key_part_map keypart_map,
		enum ha_rkey_function find_flag);
	int index_read_last_map(uchar * buf, const uchar *key, key_part_map keypart_map);
	int index_first(uchar *buf);
	int index_last(uchar *buf);
	int index_next(uchar *buf);
	int index_prev(uchar *buf);
	int index_end();
	int rnd_init(bool scan);
	int rnd_end();
	int rnd_next(uchar *buf);
	int rnd_pos(uchar *buf, uchar *pos);
	void position(const uchar *record);
	
	//int info(uint);
	//int extra(enum ha_extra_function operation);
	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);
	int external_lock(THD *thd, int lock_type);
	int start_stmt(THD *thd, thr_lock_type lock_type);
	int truncate(void);
	ha_rows records_in_range(uint inx, key_range *min_key,
		key_range *max_key);
	int delete_table(const char *from);
	int rename_table(const char * from, const char * to);
	//bool get_error_message(int error, String* buf);
	int add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add);
	int final_add_index(handler_add_index *add, bool commit);
	int prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys);
	int final_drop_index(TABLE *table_arg);
	//int analyze(THD *thd, HA_CHECK_OPT *check_opt);
	int optimize(THD *thd, HA_CHECK_OPT *check_opt);
	int check(THD* thd, HA_CHECK_OPT* check_opt);
	int repair(THD* thd, HA_CHECK_OPT* check_opt);

	/*void get_auto_increment(ulonglong offset, ulonglong increment,
		ulonglong nb_desired_values,
		ulonglong *first_value,
		ulonglong *nb_reserved_values);*/
	//void update_create_info(HA_CREATE_INFO *create_info);
	virtual int reset_auto_increment_if_nozero(ulonglong value) = 0;

	//u16 getNtseTableId();
	//st_table* getMysqlTable();
	THD* getTHD();
	virtual bool isSlaveThread() const = 0;
	virtual bool isSlaveThread(THD *thd) const = 0;

	void setNtseTable(Table *ntseTable) {
		m_ntseTable = ntseTable;
	}

protected:
	int excptToMySqlError(ErrorCode code);
	int getMysqlErrCodeTextNo(int code);
	bool transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out);
	void initReadSet(bool idxScan, bool posScan = false);
	u16* transCols(MY_BITMAP *bitmap, u16 *numCols, bool idxScan = false, uint idx = 0);
	void parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols);
	void normalizePathSeperator(char *path);
	CollType getCollation(CHARSET_INFO *charset) throw(NtseException);
	bool isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part);
	bool recordHasSameKey(const IndexDef *indexDef, const SubRecord *key1, const SubRecord *key2);

	virtual void attachToTHD(THD *thd, const char *name) = 0;
	virtual void detachFromTHD() = 0;
	virtual void setAutoIncrIfNecessary(uchar * buf) = 0;
	virtual OpType getOpType();

	void prepareAddIndexReal() throw(NtseException);
	virtual void addIndexReal(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add) throw(NtseException) = 0;
	virtual void finalAddIndexReal(handler_add_index *add, bool commit) throw(NtseException) = 0;
	virtual void dropIndexReal(TABLE *table_arg, uint *key_num, uint num_of_keys) throw(NtseException) = 0;
	virtual void dropTableReal(const char *name) throw(NtseException) = 0;
	virtual void deleteAllRowsReal() throw(NtseException) = 0;

	virtual void optimizeTableReal(bool keepDict, bool waitForFinish) throw(NtseException) = 0;
	virtual void renameTableReal(const char *oldPath, const char *newPath) throw(NtseException) = 0;

private:
	const char* currentSQL();
	bool is_twophase_filesort();
	int reportError(ErrorCode errCode, const char *msg, bool fatal, bool warnOrErr = false);
	//bool openTable(const char *table_name, int *eno);
	//int closeTable(TableInfoEx *share);
	bool checkTableLock();
	bool isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound);
	int fetchNext(uchar *buf);
	void endScan();
	void beginTblScan();
	u64 calcNextAutoIncr(u64 current, u64 increment, u64 offset, u64 maxValue);
	int indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact);
	//int checkSQLLogBin();

protected:
	static const byte REF_CACHED_ROW_ID = 0;	/** 缓存的记录标识是被缓存记录的ID */
	static const byte REF_ROWID = 1;			/** 缓存的记录标识是RID */

	THR_LOCK_DATA	m_mysqlLock;	/** MySQL用的表锁 */
	//TableInfoEx		*m_tblInfo;		/** 操作同一张表的handler共享这一信息 */
	Session			*m_session;		/** 当前会话 */
	MemoryContext	*m_lobCtx;		/** 用于存储查询返回大对象的内存分配上下文 */
	ILMode			m_wantLock;		/** 要加的表锁模式 */
	ILMode			m_gotLock;		/** 已经加的表锁模式 */
	SubRecord		m_indexKey;		/** 索引搜索键 */
	bool			m_checkSameKeyAfterIndexScan;	/** 是否要检查索引扫描的记录是否与搜索键相等 */
	int				m_errno;		/** 最后一次错误号 */
	char			m_errmsg[1024];	/** 最后一次错误信息 */
	bool			m_ignoreDup;	/** 是否忽略唯一性冲突
									 * 若m_ignoreDup为true且m_replace为true，则表示为REPLACE/ON DUPLICATE KEY UPDATE，
									 * 否则为INSERT IGNORE。m_replace为true时，m_ignoreDup一定也为true
									 */
	bool			m_replace;		/** 是否为REPLACE或ON DUPLICATE KEY UPDATE */
	uint			m_dupIdx;		/** INSERT/UPDATE导致唯一性冲突的索引号 */
	u64				m_mcSaveBeforeScan;	/** 开始扫描之前内存分配上下文位置 */
	RowId			m_lastRow;		/** 最后一条记录的RID */
	u64				m_beginTime;	/** 开始使用本handler的时间，单位微秒 */

	TblScan			        *m_ntseScan;	/** 扫描句柄 */
	IUSequence<TblScan *>	*m_ntseIuSeq;	/** REPLACE冲突时的IDU序列 */
	bool			m_isRndScan;	/** 是否正在进行表扫描，即调用rnd_init时scan为true */
	RowCache		*m_rowCache;	/** 记录缓存 */

	THD				*m_thd;			/** 正在使用本handler的THD */
	const char		*m_name;		/**	标识当前THD是由哪个函数调用attach上的 */
	//DLink<ha_ntse *>	m_thdInfoLnk;	/** 在THDInfo handler链表中的链接 */
	//DLink<ha_ntse *>	m_hdlInfoLnk;	/** 在HandlerInfo handler链表中的连接 */
	Connection		*m_conn;		/** NTSE数据库连接 */

	MY_BITMAP		m_readSet;		/** 用来存放扫描read_set的位图 */
	uint32			m_readSetBuf[Limits::MAX_COL_NUM / 8 + 1];	/** 位图所需要的缓存 */
	bool			m_isReadAll;	/** 用来同步当前扫描m_readSet是否被设为读取全部属性 */

	u64				m_increment;	/** 上层设定的本次sql操作的自增步长 */
	u64				m_offset;		/** 上层设定的本次sql操作的自增偏移 */
	u32             m_deferred_read_cache_size; /**rowcache的大小*/
private:
	Table			*m_ntseTable;	/** 当前正在操作的表 */
};

//extern CmdExecutor *cmd_executor;
extern Database	*ntse_db;
//extern ulong ntse_sample_max_pages;
//extern NTSEBinlog *ntse_binlog;
//extern HandlerInfo *handlerInfo;

extern THDInfo* getTHDInfo(THD *thd);
extern void setTHDInfo(THD *thd, THDInfo *info);
//extern THDInfo* checkTHDInfo(THD *thd);
#endif


