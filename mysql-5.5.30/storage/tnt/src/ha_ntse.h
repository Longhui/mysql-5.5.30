/**
 * NTSE存储引擎接口定义
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */
#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"

using namespace ntse;	// Make source insight happy

/** MySQL连接附加信息 */
struct ConnUserData {
	u32			m_thdId;	/** THD的ID */

	ConnUserData(u32 thdId) {
		m_thdId = thdId;
	}
};

/** 完成到一半的操作类型 */
enum PendingOperType {
	POT_NONE,		/** 没有进行到一半的操作 */
	POT_BACKUP,		/** 备份操作 */
};

class ha_ntse;
struct CmdInfo;
/** NTSE存储引擎中每个THD对应的数据结构，包括
 * 连接，表锁信息等
 */
struct THDInfo {
	Connection		*m_conn;			/** NTSE连接 */
	DList<ha_ntse *>	m_handlers;		/** handler列表 */
	CmdInfo			*m_cmdInfo;			/** 命令执行结果*/
	void			*m_pendingOper;		/** 进行到一半的操作，如备份等 */
	PendingOperType	m_pendingOpType;	/** 进行到一半的操作类型 */
	const char		*m_nextCreateArgs;	/** 非标准建表信息 */	

	THDInfo(const THD *thd);
	~THDInfo();
	void setPendingOper(void *pendingOper, PendingOperType type);
	void resetPendingOper();
	void setNextCreateArgs(const char *createArgs);
	void resetNextCreateArgs();
};

class HandlerInfo {
public:
	struct HandlerList {
		HandlerList() : m_mutex("HandlerInfo::HandlerList", __FILE__, __LINE__), m_event(false) {
			m_reserved = NULL;
		}

		DList<ha_ntse *> m_handlers;		/** 属于同一张表的handlers */
		ha_ntse *m_reserved;				/** 当前被保留着的handler,保留的handler不允许被删除 */
		ntse::Mutex m_mutex;						/** 保护当前队列结构的锁 */
		Event m_event;						/** 用来通知各个操作对象的信号量 */
	};

public:
	static HandlerInfo *getInstance();
	static void freeInstance();

	ha_ntse* reserveSpecifiedHandler(u16 tblId);
	void returnHandler(ha_ntse *handler);
	void registerHandler(ha_ntse *handler);
	void unregisterHandler(ha_ntse *handler, u16 ntseTableId);
	bool isHandlerIn(ha_ntse *handler);

private:
	HandlerInfo() : m_mutex("HandlerInfo", __FILE__, __LINE__) {}

	static HandlerInfo		*m_instance;	/** 句柄信息唯一实例 */
	map<u16, HandlerList*>	m_handlers;		/** 记录当前ntse各个表使用的handler的信息 */
	ntse::Mutex m_mutex;							/** 用来保护m_handlers的锁 */
};

extern handlerton *ntse_hton;

struct TblDefEx;
struct TableInfoEx;
class RowCache;
class NTSEBinlog;
/** NTSE存储引擎操作接口 */
class ha_ntse: public handler {
public:
	static int init(void *p);
	static int exit(void *p);

	static int initISMutexStats(void *p);
	static int deinitISMutexStats(void *p);
	static int fillISMutexStats(THD *thd, TABLE_LIST *tables, COND *cond);
	
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

	static int ntseBinlogFunc(handlerton *hton, THD *thd, enum_binlog_func fn, void *arg);

	ha_ntse(handlerton *hton, TABLE_SHARE *table_arg);
	~ha_ntse() {
		delete m_lobCtx;
	}
	bool check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes);
	uint alter_table_flags(uint flags);
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
	double read_time(ha_rows rows);
	int open(const char *name, int mode, uint test_if_locked);
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
	
	int info(uint);
	int extra(enum ha_extra_function operation);
	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);
	int external_lock(THD *thd, int lock_type);
	int start_stmt(THD *thd, thr_lock_type lock_type);
	int delete_all_rows(void);
	ha_rows records_in_range(uint inx, key_range *min_key,
		key_range *max_key);
	int delete_table(const char *from);
	int rename_table(const char * from, const char * to);
	int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);
	bool get_error_message(int error, String* buf);
	int add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys);
	int prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys);
	int final_drop_index(TABLE *table_arg);
	int analyze(THD *thd, HA_CHECK_OPT *check_opt);
	int optimize(THD *thd, HA_CHECK_OPT *check_opt);
	int check(THD* thd, HA_CHECK_OPT* check_opt);
	int repair(THD* thd, HA_CHECK_OPT* check_opt);

	void get_auto_increment(ulonglong offset, ulonglong increment,
		ulonglong nb_desired_values,
		ulonglong *first_value,
		ulonglong *nb_reserved_values);
	void update_create_info(HA_CREATE_INFO *create_info);
	int reset_auto_increment(ulonglong value);

	u16 getNtseTableId();
	st_table* getMysqlTable();
	DLink<ha_ntse *>* getHdlInfoLnk();
	THD* getTHD();
	bool isSlaveThread() const;
	bool isSlaveThread(THD *thd) const;

private:
	const char* currentSQL();
	bool is_twophase_filesort();
	void normalizePathSeperator(char *path);
	CollType getCollation(CHARSET_INFO *charset) throw(NtseException);
	int reportError(ErrorCode errCode, const char *msg, bool fatal, bool warnOrErr = false);
	bool openTable(const char *table_name, int *eno);
	int closeTable(TableInfoEx *share);
	int excptToMySqlError(ErrorCode code);
	bool checkTableLock();
	u16* transCols(MY_BITMAP *bitmap, u16 *numCols);
	bool isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound);
	void transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out);
	void parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols);
	pair<TblDefEx*, TableDef *> *queryTableDef(Session *session, const char *sourceTablePath, 
		bool *hasDict = NULL) throw(NtseException);
	TblDefEx* parseCreateArgs(TableDef *tableDef, const char *str) throw(NtseException);
	void fixedTblDefEx(const TableDef *newTableDef, const TableDef *oldTableDef, TblDefEx *args);
	void applyCreateArgs(TableDef *tableDef, const TblDefEx *args) throw(NtseException);
	bool isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part);
	OpType getOpType();
	int indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact);
	int fetchNext(uchar *buf);
	void endScan();
	void attachToTHD(THD *thd);
	void detachFromTHD();
	void beginTblScan();
	bool recordHasSameKey(const byte *buf, const IndexDef *indexDef, const SubRecord *key);
	void initReadSet(bool idxScan, bool posScan = false);
	u64 getMaxAutoIncr(Session *session);
	void setAutoIncrIfNecessary(uchar * buf);
	u64 calcNextAutoIncr(u64 current, u64 increment, u64 offset, u64 maxValue);
	int checkSQLLogBin();

	static void rationalConfigs();
	static bool isBinlogSupportable();

private:
	static const byte REF_CACHED_ROW_ID = 0;	/** 缓存的记录标识是被缓存记录的ID */
	static const byte REF_ROWID = 1;			/** 缓存的记录标识是RID */

	THR_LOCK_DATA	m_mysqlLock;	/** MySQL用的表锁 */
	TableInfoEx		*m_tblInfo;		/** 操作同一张表的handler共享这一信息 */
	Table			*m_table;		/** 当前正在操作的表 */
	TblScan			*m_scan;		/** 扫描句柄 */
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
	IUSequence		*m_iuSeq;		/** REPLACE冲突时的IDU序列 */
	uint			m_dupIdx;		/** INSERT/UPDATE导致唯一性冲突的索引号 */
	u64				m_mcSaveBeforeScan;	/** 开始扫描之前内存分配上下文位置 */
	RowId			m_lastRow;		/** 最后一条记录的RID */
	u64				m_beginTime;	/** 开始使用本handler的时间，单位微秒 */

	bool			m_isRndScan;	/** 是否正在进行表扫描，即调用rnd_init时scan为true */
	RowCache		*m_rowCache;	/** 记录缓存 */

	THD				*m_thd;			/** 正在使用本handler的THD */
	DLink<ha_ntse *>	m_thdInfoLnk;	/** 在THDInfo handler链表中的链接 */
	DLink<ha_ntse *>	m_hdlInfoLnk;	/** 在HandlerInfo handler链表中的连接 */
	Connection		*m_conn;		/** NTSE数据库连接 */

	MY_BITMAP		m_readSet;		/** 用来存放扫描read_set的位图 */
	uint32			m_readSetBuf[Limits::MAX_COL_NUM / 8 + 1];	/** 位图所需要的缓存 */
	bool			m_isReadAll;	/** 用来同步当前扫描m_readSet是否被设为读取全部属性 */

	u64				m_increment;	/** 上层设定的本次sql操作的自增步长 */
	u64				m_offset;		/** 上层设定的本次sql操作的自增偏移 */
};

/** 命令执行状态 */
enum CmdStatus {
	CS_SUCCEEED,	/** 命令执行成功 */
	CS_FAILED,		/** 命令执行失败 */
	CS_INIT,		/** 没有执行过命令 */
};

/** 命令执行状态，结果等信息 */
struct CmdInfo {
public:
	CmdInfo();
	~CmdInfo();
	CmdStatus getStatus() const;
	void setStatus(CmdStatus status);
	const char* getCommand() const;
	void setCommand(const char *cmd);
	const char* getInfo() const;
	void setInfo(const char *info);
	static const char* getStatusStr(CmdStatus status);

private:
	unsigned long	m_thdId;		/** 发起命令的连接号 */
	const char		*m_cmd;			/** 命令 */
	CmdStatus		m_status;		/** 执行状态 */
	const char		*m_info;		/** 成功执行时的结果，失败时的错误消息或执行过程中的状态信息 */
	void			*m_data;		/** 扩展数据 */
};

/** 命令执行框架 */
class CmdExecutor {
public:
	CmdExecutor();
	~CmdExecutor();
	void doCommand(THDInfo *thdInfo, CmdInfo *cmdInfo);
	
private:
	bool executeCommand(THDInfo *thdInfo, CmdInfo *cmdInfo) throw(NtseException);
};

extern CmdExecutor *cmd_executor;
extern Database	*ntse_db;
extern ulong ntse_sample_max_pages;
extern NTSEBinlog *ntse_binlog;
extern HandlerInfo *handlerInfo;

extern THDInfo* getTHDInfo(THD *thd);
extern void setTHDInfo(THD *thd, THDInfo *info);
extern THDInfo* checkTHDInfo(THD *thd);


