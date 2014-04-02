/**
 * TNT存储引擎接口定义
 *
 * @author 何登成(hzhedengcheng@corp.netease.com, he.dengcheng@gmail.com)
 */
#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "api/TNTTable.h"
#include "api/Table.h"
#include "api/TNTDatabase.h"
#include "ntse_handler.h"

using namespace ntse;
using namespace tnt;

extern handlerton *tnt_hton;

class ha_tnt;

extern "C" {
	void thd_mark_transaction_to_rollback(MYSQL_THD thd, bool all);
}

/** TNT存储引擎中每个THD对应的数据结构，包括
 * 连接，事务信息等
 */
struct TNTTHDInfo : public THDInfo {
	TNTTransaction	*m_trx;				/** mysql thd对应的tnt transaction */
	uint             m_netWaitTimeout; /** 保存thd的net_wait_timeout时间，在tnt引擎中，为了防止idle事务，net_wait_timeout有可能会被重设 */
	
	TNTTHDInfo(const THD *thd);
	~TNTTHDInfo();
};

struct TblDefEx;

/**	TNT使用中的表的额外信息
*	对应于NTSE中的TableInfoEx，与InnoDB中的INNOBASE_SHARE
*/
struct TNTShare {
	THR_LOCK	m_lock;
	char		*m_path;
	uint		m_refCnt;
	TNTTable	*m_table;

	TNTShare(const char *path, TNTTable *table) {
		m_path = System::strdup(path);
		m_table = table;
		thr_lock_init(&m_lock);
		m_refCnt = 0;
	}

	~TNTShare() {
		delete []m_path;
		thr_lock_delete(&m_lock);
		m_path = NULL;
		m_table = NULL;
	}
};

struct Autoinc {
	u64			m_autoincOffset;	/** mysql上层传入的autoinc取值时的偏移 */
	u64			m_autoincIncrement;	/** mysql上层传入的autoinc取值时的步长 */
									/** nextval = m_autoincOffset + N * m_autoincIncrement */
	u64			m_autoincLastVal;	/** autoinc的最后取值 */
	uint		m_autoincErr;		/** 取autoinc值出错的原因 */
};

class ha_tnt: public ntse_handler {
public:
	// 静态方法
	static int init(void *p);
	static int exit(void *p);

	static int initISMutexStats(void *p);
	static int deinitISMutexStats(void *p);
	static int fillISMutexStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISIntentionLockStats(void *p);
	static int deinitISIntentionLockStats(void *p);
	static int fillISIntentionLockStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISRWLockStats(void *p);
	static int deinitISRWLockStats(void *p);
	static int fillISRWLockStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISBufDistr(void *p);
	static int deinitISBufDistr(void *p);
	static int fillISBufDistr(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISConns(void *p);
	static int deinitISConns(void *p);
	static int fillISConns(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISMms(void *p);
	static int deinitISMms(void *p);
	static int fillISMms(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISMmsRPCls(void *p);
	static int deinitISMmsRPCls(void *p);
	static int fillISMmsRPCls(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTable(void *p);
	static int deinitISTable(void *p);
	static int fillISTable(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISHeap(void *p);
	static int deinitISHeap(void *p);
	static int fillISHeap(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISHeapEx(void *p);
	static int deinitISHeapEx(void *p);
	static int fillISHeapEx(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISIndex(void *p);
	static int deinitISIndex(void *p);
	static int fillISIndex(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISIndexEx(void *p);
	static int deinitISIndexEx(void *p);
	static int fillISIndexEx(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISLob(void *p);
	static int deinitISLob(void *p);
	static int fillISLob(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISLobEx(void *p);
	static int deinitISLobEx(void *p);
	static int fillISLobEx(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISCmdReturn(void *p);
	static int deinitISCmdReturn(void *p);
	static int fillISCmdReturn(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTblDefEx(void *p);
	static int deinitISTblDefEx(void *p);
	static int fillISTblDefEx(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISColDefEx(void *p);
	static int deinitISColDefEx(void *p);
	static int fillISColDefEx(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISIdxDefEx(void *p);
	static int deinitISIdxDefEx(void *p);
	static int fillISIdxDefEx(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISMmsRidHashConflicts(void *p);
	static int deinitISMmsRidHashConflicts(void *p);
	static int fillISMmsRidHashConflicts(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISDBObjStats(void *p);
	static int deinitISDBObjStats(void *p);
	static int fillISDBObjStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISCompressStats(void *p);
	static int deinitISCompressStats(void *p);
	static int fillISCompressStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTNTMHeapStats(void *p);
	static int deinitISTNTMHeapStats(void *p);
	static int fillISTNTMHeapStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTNTHashIndexStats(void *p);
	static int deinitISTNTHashIndexStats(void *p);
	static int fillISTNTHashIndexStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTNTTrxSysStats(void *p);
	static int deinitISTNTTrxSysStats(void *p);
	static int fillISTNTTrxSysStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTNTTrxStats(void *p);
	static int deinitISTNTTrxStats(void *p);
	static int fillISTNTTrxStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTNTInnerTrxStats(void *p);
	static int deinitISTNTInnerTrxStats(void *p);
	static int fillISTNTInnerTrxStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTNTMemoryIndexStats(void *p);
	static int deinitISTNTMemoryIndexStats(void *p);
	static int fillISTNTMemoryIndexStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISTNTIndexStats(void *p);
	static int deinitISTNTIndexStats(void *p);
	static int fillISTNTIndexStats(THD *thd, TABLE_LIST *tables, COND *cond);

#ifdef NTSE_PROFILE
	static int initISProfSumm(void *p);
	static int deinitISProfSumm(void *p);
	static int fillISProfSumm(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISBGThdProfile(void *p);
	static int deinitISBGThdProfile(void *p);
	static int fillISBGThdProfile(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initISConnThdProfile(void *p);
	static int deinitISConnThdProfile(void *p);
	static int fillISConnThdProfile(THD *thd, TABLE_LIST *tables, COND *cond);
#ifdef NTSE_KEYVALUE_SERVER
	static int initKVProfSumm(void *p);
	static int deinitKVProfSumm(void *p);
	static int fillKVProfSumm(THD *thd, TABLE_LIST *tables, COND *cond);
#endif
#endif

#ifdef NTSE_KEYVALUE_SERVER
	static int initKVStats(void *p);
	static int deinitKVStats(void *p);
	static int fillKVStats(THD *thd, TABLE_LIST *tables, COND *cond);

	static int initKVThreadInfo(void *p);
	static int deinitKVThreadInfo(void *p);
	static int fillKVThreadInfo(THD *thd, TABLE_LIST *tables, COND *cond);
#endif

	static int tnt_close_connection(handlerton *hton, THD* thd);
	static handler* tnt_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);
	static void tnt_drop_database(handlerton *hton, char* path);
	static int tnt_commit_trx(handlerton *hton, THD* thd, bool all);
	static int tnt_rollback_trx(handlerton *hton, THD *thd, bool all);
	static int tnt_xa_prepare(handlerton *hton, THD* thd, bool all);
	static int tnt_xa_recover(handlerton *hton, XID* xid_list, uint len);
	static int tnt_trx_commit_by_xid(handlerton *hton, XID *xid);
	static int tnt_trx_rollback_by_xid(handlerton *hton, XID* xid);

	// public方法，for information schema，暂时未考虑tnt需要哪些information schema
	// 原有ntse的information schema，应该需要继续支持

	// public方法，提供给mysql上层调用
	ha_tnt(handlerton *hton, TABLE_SHARE *table_arg);
	~ha_tnt() {
	}

	uint alter_table_flags(uint flags);
	bool check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes);
	const char* table_type() const;
	const char* index_type(uint inx);
	const char** bas_ext() const;
	ulonglong table_flags() const;
	ulong index_flags(uint inx, uint part, bool all_parts) const;
	uint max_supported_record_length() const;
	uint max_supported_keys() const;
	uint max_supported_key_parts() const;
	uint max_supported_key_length() const;
	uint max_supported_key_part_length() const;

	double scan_time();
	double read_time(uint index, uint ranges, ha_rows rows);
	ha_rows estimate_rows_upper_bound();

	int open(const char *name, int mode, uint test_if_locked);
	int reset();
	int close(void);

	int write_row(uchar *buf);
	int update_row(const uchar *old_data, uchar *new_data);
	int delete_row(const uchar *buf);

	int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
		enum ha_rkey_function find_flag);
	int index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map);
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

	int	info(uint);
	int extra(enum ha_extra_function operation);
	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);
	int external_lock(THD *thd, int lock_type);
	int start_stmt(THD *thd, thr_lock_type lock_type);

	//bool isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound);
	ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
	
	int truncate(void);
	int delete_table(const char *from);
	int rename_table(const char *from, const char *to);
	int create(const char *name, TABLE *from, HA_CREATE_INFO *create_info);
	void update_create_info(HA_CREATE_INFO *create_info);
	bool get_error_message(int error, String *buf);
	int add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add);
	int final_add_index(handler_add_index *add, bool commit);
	int prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys);
	int final_drop_index(TABLE *table_arg);

	int analyze(THD *thd, HA_CHECK_OPT *check_opt);
	int optimize(THD *thd, HA_CHECK_OPT *check_opt);
	int check(THD *thd, HA_CHECK_OPT *check_opt);
	int repair(THD* thd, HA_CHECK_OPT *check_opt);

	void get_auto_increment(ulonglong offerset, ulonglong increment,
		ulonglong nb_desired_values, ulonglong *first_value,
		ulonglong *nb_reserved_values);
	int reset_auto_increment(ulonglong value);
	int reset_auto_increment_if_nozero(ulonglong value);

	my_bool register_query_cache_table(THD *thd, char *table_key,
		uint key_length,
		qc_engine_callback *call_back,
		ulonglong *engine_data);

	// public方法，
	bool isSlaveThread() const;
	bool isSlaveThread(THD *thd) const;
	u16 getTntTableId();
	
	// public方法，gets
	uint getLockType() const;
	uint getStoreLockType() const;
	
	static void rationalConfigs();
protected:
	void attachToTHD(THD *thd, const char *name);
	void detachFromTHD();
	OpType getOpType();

	void prepareAddIndexReal() throw(NtseException);
	void addIndexReal(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add) throw(NtseException);
	void finalAddIndexReal(handler_add_index *add, bool commit) throw(NtseException);
	void dropIndexReal(TABLE *table_arg, uint *key_num, uint num_of_keys) throw(NtseException);
	void dropTableReal(const char *name) throw(NtseException);
	void deleteAllRowsReal() throw(NtseException);

	void optimizeTableReal(bool keepDict, bool waitForFinish) throw(NtseException);
	void renameTableReal(const char *oldPath, const char *newPath) throw(NtseException);

	void setAutoIncrIfNecessary(uchar * buf);

private:
	//int checkOperatePermission(THD *thd, bool dml, bool read);
	int checkGeneralPermission(THD *thd);
	// autoinc 相关
	u64 initAutoinc(Session *session);
	u64 calcNextAutoIncr(u64 current, u64 increment, u64 offset, u64 maxValue);
	u64 getIntColMaxValue();

	//bool is_twophase_filesort();
	//void normalizePathSeperator(char *path);
	//CollType getCollation(CHARSET_INFO *charset) throw(NtseException);
	//bool isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part);
	pair<TblDefEx*, TableDef *> *queryTableDef(Session *session, const char *sourceTablePath, 
		bool *hasDict = NULL) throw(NtseException);
	TblDefEx* parseCreateArgs(TableDef *tableDef, const char *str) throw(NtseException);
	void fixedTblDefEx(const TableDef *newTableDef, const TableDef *oldTableDef, TblDefEx *args);
	void applyCreateArgs(TableDef *tableDef, const TblDefEx *args) throw(NtseException);

	void rollbackTrxIfNecessary(ErrorCode errCode, THD *thd);
	int reportError(ErrorCode errCode, const char *msg, THD *thd, bool fatal, bool warnOrErr = false);
	
	bool openTable(const char *table_name, int *eno);
	int closeTable(TNTShare *share);
	int indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact);
	void endScan();

	//u16* transCols(MY_BITMAP *bitmap, u16 *numCols);

	u16* transColsWithDiffVals(const uchar *old_row, const uchar *new_row, u16 *numCols);
	const byte* rowReadLobCol(u32 *colLen, const byte *colOffset);
	const byte* rowReadVarcharCol(u32 *colLen, const byte *colOffset, u32 lenLen);
	bool isColDataNull(Field *field, const uchar *record);
	u32 readFromNLittleEndian(const byte *colOffset, u32 colLen);

	//bool transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out);
	//void parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols);
	//bool recordHasSameKey(const byte *buf, const IndexDef *indexDef, const SubRecord *key);
	//void initReadSet(bool idxScan, bool posScan = false);
	void beginTblScan();
	int fetchNext(uchar *buf);
	TrxIsolationLevel getIsolationLevel(enum_tx_isolation iso);
	TNTTransaction* getTransForCurrTHD(THD *thd);
	static TNTTransaction* getTransInTHD(THD *thd);
	void tntRegisterTrx(handlerton *hton, THD *thd, TNTTransaction *trx);
	void trxRegisterFor2PC(TNTTransaction *trx);
	static void trxDeregisterFrom2PC(TNTTransaction *trx);
	Session* createSessionAndAssignTrx(const char *name, Connection *conn, TNTTransaction *trx, int timeoutMs = -1);
	Session* createSession(const char *name, Connection *conn, int timeoutMs = -1);

	TNTTransaction* startInnerTransaction(TLockMode lockMode);
	void commitInnerTransaction(TNTTransaction *trx);
	void rollbackInnerTransaction(TNTTransaction *trx);

	void lockTableOnStatStart(TNTTransaction *trx, TNTTable *table, TLockMode lockMode) throw(NtseException);

protected:
	TNTShare		*m_share;		/** 保存table上的THR_LOCK，同一table的所有handler实例共享，mysql上层使用 */
	//THR_LOCK_DATA	m_mysqlLock;	/** THR_LOCK的holder节点，每个handler实例，都有自己的THR_LOCK_DATA结构 */
	TNTTblScan		*m_scan;		/** TNT scan句柄 */
	TNTOpInfo		m_opInfo;		/** 标识本次sql操作的部分信息，用于下层的判断 */
	bool			m_isRndScan;	/** 是否正在进行表扫描，即调用rnd_init时scan为true */
	//THD				*m_thd;			/** mysql thread */
	TNTTable		*m_table;		/** handler对应的TNT Table */
	//Connection		*m_conn;		/** handler对应的NTSE连接 */
	//Session			*m_session;		/** 当前会话 */
	//MemoryContext	*m_lobCtx;		/** 用于存储查询返回大对象的内存分配上下文 */
	TNTTransaction	*m_trans;		/** handler对应的事务 */
	TLockMode		m_selLockType;	/** 扫描的行加锁模式 */
									/** 表锁模式，通过行锁模式推断而来：行锁S->表锁IS；行锁X->表锁IX */

	//int				m_errno;		/** 最后一次错误号 */
	//char			m_errmsg[1024];	/** 最后一次错误信息 */
	//bool			m_ignoreDup;	/** 是否忽略唯一性冲突
									// * 若m_ignoreDup为true且m_replace为true，则表示为REPLACE/ON DUPLICATE KEY UPDATE，
									// * 否则为INSERT IGNORE。m_replace为true时，m_ignoreDup一定也为true
									// */
	//bool			m_replace;		/** 是否为REPLACE或ON DUPLICATE KEY UPDATE */
	IUSequence<TNTTblScan *>	*m_iuSeq;		/** REPLACE冲突时的IDU序列 */
	//uint			m_dupIdx;		/** INSERT/UPDATE导致唯一性冲突的索引号 */

	//u64				m_mcSaveBeforeScan;	/** 开始扫描之前内存分配上下文位置 */
	//RowId			m_lastRow;		/** 最后一条记录的RID */

	//SubRecord		m_indexKey;		/** 索引搜索，给定键值 */
	//bool			m_checkSameKeyAfterIndexScan;
									/** 是否需要检查索引扫描record与搜索键相同与否 */
									/** 等值查询，mysql需要存储引擎自身判断扫描是否结束；非等值查询，mysql上层判断扫描是否结束 */


	//u64				m_beginTime;	/** handler实例创建开始时间 */

	//MY_BITMAP		m_readSet;		/** 用来存放扫描read_set的位图 */
	//uint32			m_readSetBuf[Limits::MAX_COL_NUM / 8 + 1];	/** 位图所需要的缓存 */
	//bool			m_isReadAll;	/** 用来同步当前扫描m_readSet是否被设为读取全部属性 */
	
	// autoinc相关属性
	Autoinc			m_autoInc;		/** 句柄对应的autoinc相关信息 */
};

extern CmdExecutor *cmd_exec;
extern TNTDatabase *tnt_db;
extern ulong ntse_sample_max_pages;

extern TNTTHDInfo* getTntTHDInfo(THD *thd);
extern void setTntTHDInfo(THD *thd, TNTTHDInfo *info);
extern TNTTHDInfo* checkTntTHDInfo(THD *thd);

/* 标识事务的当前语句出现fatal error，事务已经rollback
 * all参数控制rollback级别，语句级 vs 事务级*/
void thd_mark_transaction_to_rollback(MYSQL_THD thd, bool all);