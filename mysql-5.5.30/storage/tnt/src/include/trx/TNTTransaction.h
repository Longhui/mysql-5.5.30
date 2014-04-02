/**
 * TNT事务管理模块。
 * 
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef _TNT_TTRANSACTION_H_
#define _TNT_TTRANSACTION_H_

#include "util/ObjectPool.h"
#include "trx/TLock.h"
#include "trx/TrxXA.h"
#include "misc/DLDLockTable.h"
#include "misc/Txnlog.h"
#include "misc/Session.h"
#include <vector>

using namespace ntse;

namespace tnt {

class TNTDatabase;

/** 事务状态 */
enum TrxState {
	TRX_NOT_START, /** 事务未开始 */
	TRX_ACTIVE,    /** 活跃状态 */
	TRX_PREPARED,  /** 准备状态 */
	TRX_COMMITTED_IN_MEMORY, /** 事务已提交但是还未持久化 */
};

/** 事务隔离级别 */
enum TrxIsolationLevel {
	TRX_ISO_READ_UNCOMMITTED, /** 脏读 */
	TRX_ISO_READ_COMMITTED,   /** 提交读 */
	TRX_ISO_REPEATABLE_READ,  /** 可重复读 */
	TRX_ISO_SERIALIZABLE,     /** 可串行化 */
	TRX_ISO_UNDEFINED,        /** 未定义 */
};

enum TrxFlushMode {
	TFM_NOFLUSH = 0, //事务提交时不将buffer中日志刷入外存
	TFM_FLUSH_SYNC,  //事务提交时将buffer中日志刷入外存，并fsync
	TFM_FLUSH_NOSYNC //事务提交时将buffer中日志刷入外存，但不调用fsync
};

const static uint TRX_MAGIC_NUM = 91118598; /** 用于校验事务对象是否合法的魔数 */

class TNTTrxLog;
class TNTTrxSys;

/** 可见性视图 */
class ReadView {
public:
	ReadView(TrxId createTrxId, TrxId *trxIds, uint trxCnt);
	~ReadView() {}

	/**
	 * 视图初始化
	 * @param upTrxId
	 * @param lowTrxId
	 * @param trxIds
	 * @param trxCnt
	 */
	inline void init(TrxId upTrxId, TrxId lowTrxId, TrxId *trxIds, uint trxCnt) {
		m_upTrxId = upTrxId;
		m_lowTrxId = lowTrxId;
		m_trxIds = trxIds;
		m_trxCnt = trxCnt;
	}

	/**
	 * 设置视图中的活跃事务数
	 * @param trxCnt
	 */
	inline void setTrxCnt(uint trxCnt) {
		m_trxCnt = trxCnt;
	}

	/**
	 * 设置当前视图中的某一个活跃事务Id
	 * @param n
	 * @param trxId
	 */
	inline void setNthTrxIdViewed(uint n, TrxId trxId) {
		assert(m_trxIds);
		assert(n < m_trxCnt);
		m_trxIds[n] = trxId;
	}

	/**
	 * 获取当前视图中的某一个活跃事务ID
	 * @param n
	 * @return 
	 */
	inline TrxId getNthTrxIdViewed(uint n) {
		assert(m_trxIds);
		assert(n < m_trxCnt);
		return m_trxIds[n];
	}

	/**
	 * 设置不可见事务ID的下限
	 * @param lowTrxId
	 */
	inline void setLowTrxId(TrxId lowTrxId) {
		m_lowTrxId = lowTrxId;
	}

	/**
	 * 设置可见事务ID的上限
	 * @param upTrxId
	 */
	inline void setUpTrxId(TrxId upTrxId) {
		m_upTrxId = upTrxId;
	}

	/**
	 * 获得不可见事务ID的下限
	 * @return 
	 */
	inline TrxId getLowTrxId() const {
		return m_lowTrxId;
	}

	/**
	 * 获得可见事务ID的上限
	 * @return 
	 */
	inline TrxId getUpTrxId() const {
		return m_upTrxId;
	}

	/**
	 * 将视图加入全局视图列表中
	 * @param readViewList
	 * @return 
	 */
	inline void relatedWithList(DList<ReadView*> *readViewList) {
		readViewList->addFirst(&m_rvLink);
	}
	/**
	 * 将视图从全局视图列表中删除
	 */
	inline void excludeFromList() {
		assert(m_rvLink.getList());
		m_rvLink.unLink();
	}

	/**
	 * 设置readView的最小LSN
	 * @param upTrxId
	 */
	inline void setMinStartLsn(LsnType minStartLsn) {
		m_minStartLsn = minStartLsn;
	}


	/**
	 * 获取readView的最小LSN
	 * @param upTrxId
	 */
	inline LsnType getMinStartLsn() {
		return m_minStartLsn;
	}

	bool isVisible(TrxId trxId);

	void print();

private:
	TrxId   m_upTrxId;// 所有 < 此值的事务id所做的修改，当前readview一定可见
	TrxId	m_lowTrxId;// 所有 >= 此值的事务id所做的修改，当前readview均不可见
	TrxId	m_createTrxId;// 创建此readview的事务id
	/** 
	 * 此readview创建时的活跃事务快照列表(本事务除外)，
	 * 此列表中的事务所做的修改，均不可见
	 * 事务id，在列表中按照从大到小的顺序排列
	 * m_trxCnt标识列表中的事务数 */
	uint	m_trxCnt;
	TrxId	*m_trxIds;
	DLink<ReadView*> m_rvLink; // 用于链接全局的所有视图
	LsnType m_minStartLsn;
};

//记录回收大对象时已经被回收的大对象
struct TblLob {
	TblLob() {}

	TblLob(u16 tableId, u64 lobId) {
		m_tableId = tableId;
		m_lobId = lobId;
	}

	u16		m_tableId; //回收大对象所对应的表
	u64     m_lobId;   //回收大对象的id
};

class TblLobHasher {
public:
	inline unsigned int operator()(const TblLob *entry) const {
		return RidHasher::hashCode(entry->m_lobId);
	}
};

template<typename T1, typename T2>
class TblLobEqualer {
public:
	inline bool operator()(const T1 &v1, const T2 &v2) const {
		return equals(v1, v2);
	}

private:
	static bool equals(const TblLob *v1, const TblLob* v2) {
		return (v1->m_lobId == v2->m_lobId && v1->m_tableId == v2->m_tableId);
	}
};
typedef DynHash<TblLob*, TblLob*, TblLobHasher, TblLobHasher, TblLobEqualer<TblLob*, TblLob*> > TblLobHashMap;

enum CommitStat {
	CS_NONE,    //初始值
	CS_NORMAL,  //正常提交事务
	CS_INNER,   //内部事务提交
	CS_RECOVER, //recover中提交的事务
	CS_PURGE,   //Purge事务
	CS_DUMP,	//dump事务
	CS_RECLAIMLOB,  //reclaimLob事务
	CS_DEFRAG_HASHINDEX, //defrag hashIndex事务
	CS_BACKUP       //备份事务
};

enum RollBackStat {
	RBS_NONE,    //初始值
	RBS_NORMAL,  //正常rollback，现在指由上层调用rollback
	RBS_TIMEOUT, //因为timeout引起的rollback
	RBS_DEADLOCK, //死锁检测被选为牺牲者引起的rollback
	RBS_INNER,  //内部事务引起的rollback，现在指ddl
	RBS_RECOVER, //recover过程中引起的rollback
	RBS_DUPLICATE_KEY, //唯一性索引冲突
	RBS_ROW_TOO_LONG,  //记录太长
	RBS_ABORT,         //事务被放弃
	RBS_OUT_OF_MEMORY,  //内存不够引发的rollback
	RBS_ONLINE_DDL
};

/** TNT事务 */
class TNTTransaction {
public:
	TNTTransaction();
	~TNTTransaction() {}

	void init(TNTTrxSys *trxSys, Txnlog *tntLog, MemoryContext *ctx, TrxId trxId, size_t poolId);
	void destory();
	void reset();

	//////////////////////////////////////////////////////////////////////////////////
	///	事务接口
	//////////////////////////////////////////////////////////////////////////////////
	void startTrxIfNotStarted(Connection *conn = NULL, bool inner = false);
	bool isTrxStarted();
	bool commitTrx(CommitStat stat);
	bool commitCompleteForMysql();
	bool rollbackTrx(RollBackStat rollBackStat, Session *session = NULL);
	bool rollbackLastStmt(RollBackStat rollBackStat, Session *session = NULL);
	bool rollbackForRecover(Session *session, DList<LogEntry *> *logs);
	void prepareForMysql();
	void markSqlStatEnd();

	bool trxJudgeVisible(TrxId trxId);
	ReadView* trxAssignReadView() throw(NtseException);
	ReadView* trxAssignPurgeReadView() throw(NtseException);
	
	//////////////////////////////////////////////////////////////////////////////////
	/// 事务锁相关接口
	//////////////////////////////////////////////////////////////////////////////////
	bool pickRowLock(RowId rowId, TableId tabId, bool bePrecise = false);
	bool isRowLocked(RowId rowId, TableId tabId, TLockMode lockMode);
	bool isTableLocked(TableId tabId, TLockMode lockMode);

	bool tryLockRow(TLockMode lockMode, RowId rowId, TableId tabId);
	void lockRow(TLockMode lockMode, RowId rowId, TableId tabId) throw(NtseException);
	bool unlockRow(TLockMode lockMode, RowId rowId, TableId tabId);
	
	bool tryLockTable(TLockMode lockMode, TableId tabId);
	void lockTable(TLockMode lockMode, TableId tabId) throw(NtseException);
	bool unlockTable(TLockMode lockMode, TableId tabId);
	
	bool tryLockAutoIncr(TLockMode lockMode, u64 autoIncr);
	void lockAutoIncr(TLockMode lockMode, u64 autoIncr) throw(NtseException);
	bool unlockAutoIncr(TLockMode lockMode, u64 autoIncr);
	void releaseLocks();

	void printTrxLocks();

	//////////////////////////////////////////////////////////////////////////////////
	/// 日志相关接口
	//////////////////////////////////////////////////////////////////////////////////
	LsnType writeTNTLog(LogType logType, u16 tableId, byte *data, size_t size);
	void flushTNTLog(LsnType lsn, FlushSource fs = FS_IGNORE);
	LsnType writeBeginTrxLog();
	LsnType writeBeginRollBackTrxLog();
	LsnType writeEndRollBackTrxLog();
	LsnType writeCommitTrxLog();
	LsnType writePrepareTrxLog();
	LsnType writePartialBeginRollBackTrxLog();
	LsnType writePartialEndRollBackTrxLog();
	static void parseBeginTrxLog(const LogEntry *log, TrxId *trxId, u8 *versionPoolId);
	static void parsePrepareTrxLog(const LogEntry *log, TrxId *trxId, LsnType *preLsn, XID *xid);
	bool isLogging() const;
	void disableLogging();
	void enableLogging();
	static char *getTrxStateDesc(TrxState stat);

	inline bool isReadOnly() const {
		return m_readOnly;
	}

	//用于恢复
	inline void setReadOnly(bool readOnly) {
		m_readOnly = readOnly;
	}

	/**
	 * 保存上一条日志的LSN
	 * @param lsn
	 */
	inline void setTrxLastLsn(LsnType lsn) {
		m_lastLSN = lsn;
	}

	/**
	 * 获得事务ID
	 * @return 
	 */
	inline TrxId getTrxId() const { 
		return m_trxId;
	}

	/**
	 * 获得事务错误码
	 * @return 
	 */
	inline uint getErrState() const { 
		return m_errState;
	}

	/**
	 * 获得事务正在等待的锁
	 * @return
	 */
	inline TLock* getWaitLock() const { 
		return m_lockOwner->getWaitLock();
	}

	/**
	 * 获得上层mysql的事务id
	 * @return 
	 */
	inline const XID* getTrxXID() const {
		return &m_xid;
	}

	/**
	 * 获得事务启动时的日志序列号
	 * @return 
	 */
	inline LsnType	getTrxRealStartLsn() const {
		return m_realStartLsn;
	}

	/**
	 * 获得事务开始的日志序列号
	 * @return 
	 */
	inline LsnType	getTrxBeginLsn() const {
		return m_beginLSN;
	}

	/**
	 * 获得事务的最后一条日志序列号
	 * @return 
	 */
	inline LsnType getTrxLastLsn() const { 
		return m_lastLSN;
	}

	/**
	 * 获得事务的语句级视图
	 * @return 
	 */
	inline ReadView* getReadView() const { 
		return m_readView;
	}

	/**
	 * 设置事务的语句级视图
	 * @param view 
	 */
	inline void setReadView(ReadView *view) {
		m_readView = view;
	}

	/**
	 * 获得事务级视图
	 * @return 
	 */
	inline ReadView* getGlobalReadView() const {
		return m_globalReadView;
	}

	/**
	 * 设置事务级视图
	 * @param view
	 */
	inline void setGlobalReadView(ReadView *view) {
		m_globalReadView = view;
	}

	/**
	 * 获得事务状态
	 * @return 
	 */
	inline TrxState getTrxState() const {
		return m_trxState;
	}

	/**
	 * 设置事务状态
	 * @param trxState
	 */
	inline void setTrxState(TrxState trxState) {
		m_trxState = trxState;
	}

	/**
	* 获取事务正在使用中的表数量
	* @return
	*/
	inline uint getTableInUse() const {
		return m_tablesInUse;
	}

	/**
	* 获取事务的隔离级别
	* @return
	*/
	inline TrxIsolationLevel getIsolationLevel() const {
		return m_isolationLevel;
	}

	/**
	* 返回当前事务是否已经注册到XA事务中
	* @return
	*/
	inline bool getTrxIsRegistered() const {
		return m_trxIsRegistered;
	}

	/**	
	* 设置事务的隔离级别
	* @return
	*/
	inline void setIsolationLevel(TrxIsolationLevel isoLevel) {
		m_isolationLevel = isoLevel;
	}

	/**
	 * 设置事务开始的日志序列号
	 * @param lsn
	 * @return 
	 */
	inline void setTrxBeginLsn(LsnType lsn) {
		m_beginLSN = lsn; 
	}
	
	/**
	* 设置事务的XA注册标识
	* @param isRegistered
	*/
	inline void setTrxRegistered(bool isRegistered) {
		m_trxIsRegistered = isRegistered;
	}

	/**
	 * 设置binlog位置信息
	 * @param binlogPos
	 * @return 
	 */
	inline void setBinlogPosition(u64 binlogPos) { 
		m_binlogPosition = binlogPos;
	}

	inline void incTablesInUse() {
		m_tablesInUse++;
	}

	inline void decTablesInUse() {
		m_tablesInUse--;
	}

	/**
	 * 获得在事务中加表锁的表数目
	 * @return 
	 */
	inline uint getTablesLocked() const {
		return m_tablesLocked;
	}

	inline bool getInLockTables() const {
		return m_inLockTables;
	}

	inline MemoryContext* getMemoryContext() {
		return m_memctx;
	}

	inline void setXId(const XID& xid) {
		m_xid = xid;
	}

	inline void setActiveTrans(uint activeTrans) {
		assert(activeTrans <= 2);
		m_trxActiveStat = activeTrans;
	}

	uint getActiveTrans() const {
		return m_trxActiveStat;
	}

	/**
	 * 获得事务对应的版本池ID
	 * @return 
	 */
	inline u8 getVersionPoolId() {
		return m_versionPoolId;
	}

	/**
	 * 获得事务使用的版本池ID
	 * @param versionPoolId
	 * @return 
	 */
	inline void setVersionPoolId(u8 versionPoolId) {
		m_versionPoolId = versionPoolId;
	}

	inline void setThd(void *thd) {
		m_thd = thd;
	} 

	inline void* getThd() const{
		return m_thd;
	}
	/**
	 * 获得恢复中回滚事务LobId hash
	 * @return 
	 */
	inline TblLobHashMap* getRollbackInsertLobHash() {
		return m_insertLobs;
	}

	/**
	 * 释放恢复中回滚事务LobId hash
	 * @return 
	 */
	inline void releaseRollbackInsertLobHash() {
		m_insertLobs->~TblLobHashMap();
		m_insertLobs = NULL;
	}

	/**
	 * 初始化回滚事务LobId hash
	 * @return 
	 */
	inline void initRollbackInsertLobHash() {
		m_insertLobs = new (m_memctx->alloc(sizeof(TblLobHashMap))) TblLobHashMap();;
	}
	
	/**
	 * 保存事务开始回滚的日志的LSN
	 * @param lsn
	 */
	inline void setTrxBeginRollbackLsn(LsnType lsn) {
		m_beginRollbackLsn = lsn;
	}

	/**
	 * 获得事务开始回滚的LSN
	 * @return 
	 */
	inline LsnType getBeginRollbackLsn() {
		return m_beginRollbackLsn;
	}

	inline void setLastStmtLsn(LsnType lsn) {
		m_lastStmtLsn = lsn;
	}

	inline LsnType getLastStmtLsn() {
		return m_lastStmtLsn;
	}

	/* 设置标识当前事务处于Lock Tables命令保护之中 */
	inline void setInLockTables(bool inLockTables) {
		m_inLockTables = inLockTables;
	}

	inline Connection * getConnection() const {
		return m_conn;
	}

	inline void setConnection(Connection *conn) {
		m_conn = conn;
	}

	inline u32 getBeginTime() const {
		return m_beginTime;
	}

	inline bool getWaitingLock() const {
		return m_waitingLock;
	}

	inline u32 getWaitStartTime() const {
		return m_waitStartTime;
	}

	inline uint getHoldingLockCnt() const {
		return m_lockOwner->getHoldingList()->getSize();
	}

	inline u32 getRedoCnt() const {
		return m_redoCnt;
	}

	inline bool isHangByRecover() const {
		return m_hangByRecover;
	}

	inline bool isBigTrx() {
		return getHoldingLockCnt() > 1000;
	}

#ifdef NTSE_UNIT_TEST
	inline void setTrxId(TrxId trxId) {
		m_trxId = trxId;
	}
	ReadView* trxAssignReadView(TrxId *trxIds, uint trxCnt) throw(NtseException);
#endif

private:
	void relatedWithTrxList(DList<TNTTransaction*> *trxList);
	void excludeFromTrxList();
	LsnType doWritelog(LogType logType, TrxId trxId, LsnType preLsn, u8 versionPoolId = INVALID_VERSION_POOL_INDEX);

private:
	DLink<TNTTransaction*> 	m_trxLink;// 系统中所有的事务，链接在系统事务表之中
	TNTTrxSys	*m_tranSys; // 事务所属的事务管理器
	const char  *m_opInfo;  // 事务操作信息
	TrxId	    m_trxId;	// 事务id 
	TrxIsolationLevel m_isolationLevel;	// 事务所属隔离级别
	TrxFlushMode m_flushMode; //事务刷日志的方式
	
	/** XA支持所需，上层mysql的事务id，在prepare时获得，并且写入prepare日志；根据此xid，系统崩溃时，
	 *  可以恢复到binlog与innodb日志一致的状态; 同时，也是*_by_xid函数群的一个传入参数; */
	XID		   m_xid;
	bool	   m_supportXA; // 事务是否支持二阶段提交
	TrxState   m_trxState;  // 事务状态，not_active/active/prepare/commit
	void	   *m_thd;	    // 事务所属thread

	uint			m_threadId;// 事务所属的mysql线程id
	uint			m_processId;// 事务所属的操作系统进程id

	MemoryContext   *m_memctx; // 用于本事务分配事务级相关的内存使用
	u64             m_sp;
	DldLockOwner    *m_lockOwner;// 用于事务锁锁表
	Connection		*m_conn;

	// 更新时，如何处理duplicate：DUP_IGNORE，DUP_REPLACE
	// 在extra函数中设置，在write_row等函数中读取
	uint			m_duplicates;
	// 标识活跃事务的状态；innodb中，事务是否持有prepare mutex
	uint			m_trxActiveStat;
	// 当前statement，有多少表在使用；多少表被加锁
	// external_lock函数判断m_tablesInUse，用于确定
	// 当前statement是否结束，是否可以autocommit
	uint			m_tablesInUse;
	uint			m_tablesLocked;

	// 判断当前事务，事务处于Lock Tables命令保护之中
	// 若是的话，则行级锁就不需要加了
	// 注意：m_inLockTables与MySQL上层的Lock Tables命令不一定一致
	// 因为在AutoCommit = 1时，Lock Tables命令无效
	bool			m_inLockTables;

	// 事务所对应的语句，以及语句的长度
	// innodb在创建事务时，将指针指向thd中对应的数据
	char			**m_queryStr;
	uint			*m_queryLen;
	
	ReadView        *m_globalReadView;// 事务级视图
	ReadView		*m_readView;      // 语句级视图

	// autoinc fields
	// autoinc锁结构，以及事务需要的autoinc值的数量
	// TLock			*m_autoIncLock;
    // uint			m_autoIncCnt;

	// 开启binlog状态下，记录binlog的
	// 文件及当前写入位置到trx_sys_header中
	// innodb在trx_commit_off_kernel函数中实现此功能
	// 但是目前还不清楚记录两个值的功能
	const char		*m_binlogFileName;
	u64				m_binlogPosition;
	
	// log fields，只有redo日志，TNT的undo通过版本池实现
	// 事务所做操作，产生的最后一条日志序列号
	// 属于同一事务的redo，链接起来。构成一个反向undo序列
	// 功能1：事务正常undo时，可以沿着链表找到所有日志，进行undo
	// 功能2：crash recovery时，同样可以沿此链表，进行undo
	LsnType			m_lastLSN;
	LsnType         m_lastStmtLsn;

	LsnType			m_realStartLsn; // 事务启动时对应的日志tailLsn，用于设置dumpLSN
	LsnType			m_beginLSN;// 事务开始的日志序列号
	LsnType         m_commitLsn;// 事务提交的日志序列号
	LsnType			m_beginRollbackLsn; //事务开始回滚的日志序列号

	u8				m_versionPoolId;// 事务所属的版本池表ID
	Txnlog          *m_tntLog;// 事务日志模块
	bool            m_logging;// 是否需要写日志
	
	// 出错处理
	// errState，出错代码；errInfo，duplicate出错时，记录出错的index
	// errInfo将在info函数中调用，info传入参数为HA_STATUS_ERRKEY
	uint			m_errState;
	void			*m_errInfo;

	// 标识当前事务是否被注册到MySQL内部XA事务中，true表示已注册；false表示未注册
	bool			m_trxIsRegistered;

	size_t          m_trxPoolId;// 用于表示事务在事务对象池中的ID
	//bool            m_isPurge;  // 是否是Purge事务
	bool            m_valid;    // 事务对象是否有效
	uint            m_magicNum; // 用于校验事务对象是否合法的魔数

	TblLobHashMap   *m_insertLobs;  //记录在事务回滚回收大对象过程中被插入的lobId集合

	bool            m_readOnly;
	DldLockResult   m_lastRowLockStat; //记录上次加行锁返回的状态

	u32             m_beginTime; //事务开始时间
	u32             m_redoCnt;   //事务redo的总个数

	bool			m_waitingLock;		//事务是否正在等锁
	u32				m_waitStartTime;	//事务开始等锁的时间

	bool            m_hangByRecover; //是否为recover后的悬挂事务，即外部xa parepre事务在recover后首先表现为悬挂
	u32             m_hangTime;
	
	friend class TNTTrxSys;
};

struct TNTTrxSysStat {
	u64 m_commit_normal; //commit事务的总个数
	u64 m_commit_inner;  //内部提交事务的总个数
	u64 m_rollback_normal; //用户命令rollback事务的总个数
	u64 m_rollback_timeout; //timeout引起的rollback事务的总个数
	u64 m_rollback_deadlock;//检查deadlock引起rollback事务的总个数
	u64 m_rollback_duplicate_key;//duplicate key引起的rollback事务的总个数
	u64 m_rollback_row_too_long; //记录过长引起的rollback事务的总个数
	u64 m_rollback_abort;  //事务被放弃引起的rollback事务的总个数
	u64 m_rollback_inner; //统计内部事务rollback的个数
	u64 m_rollback_recover;//统计recover过程中rollback事务的个数
	u64 m_rollback_out_of_mem; //统计因内存不足引发的rollback事务的个数
	u64 m_partial_rollback_normal; //用户命令rollback事务的总个数
	u64 m_partial_rollback_timeout; //timeout引起的rollback事务的总个数
	u64 m_partial_rollback_deadlock;//检查deadlock引起rollback事务的总个数

	u32 m_maxTime;  //事务最大响应时间
	u32 m_avgLockCnt; //事务锁平均个数
	u32 m_maxLockCnt; //最大事务锁个数
	u32 m_avgRedoCnt; //事务平均redo记录个数
	u32 m_maxRedoCnt; //事务最大redo记录个数
};

/** TNT事务管理 */
class TNTTrxSys {
public:
	template<typename T>
	class Iterator {
	public:
		Iterator(DList<T*> *list) : m_list(list) {
			m_listHead = list->getHeader();
			m_current = m_listHead->getNext();
		}

		inline bool hasNext() {
			return m_listHead != m_current;
		}

		inline T* next() {
			assert(m_listHead != m_current);
			T *element = m_current->get();
			m_current = m_current->getNext();
			return element;
		}
	public:
		DList<T*> *m_list;
		DLink<T*> *m_listHead;
		DLink<T*> *m_current;
	};

public:
	TNTTrxSys(TNTDatabase *db, uint maxTrxNum, TrxFlushMode trxFlushMode, int lockTimeoutMs);
	~TNTTrxSys();

	// 供恢复流程使用
	uint getPreparedTrxForMysql(XID* list, uint len);
	void setMaxTrxIdIfGreater(TrxId curTrxId);
	void markHangPrepareTrxAfterRecover();

	TrxId findMinReadViewInActiveTrxs();
	TNTTransaction* getTrxByXID(XID* xid);
	LsnType getMinTrxLsn();
	TrxId getMaxDumpTrxId();

	void getActiveTrxIds(std::vector<TrxId> *activeTrxsArr);
	bool isTrxActive(TrxId trxId);

	TNTTransaction* allocTrx(TrxId trxId) throw(NtseException);
	TNTTransaction* allocTrx() throw(NtseException);
	void freeTrx(TNTTransaction *trx);

	ReadView* trxAssignReadView(TNTTransaction *trx) throw(NtseException);
	ReadView* trxAssignPurgeReadView(TNTTransaction *trx) throw(NtseException);
	void closeReadViewForMysql(TNTTransaction *trx);

	LsnType getMinStartLsnFromReadViewList();

	void killHangTrx();

	DList<TNTTransaction *> *getActiveTrxs();
	DList<TNTTransaction *> *getActiveInnerTrxs();

	inline TrxFlushMode getTrxFlushMode() {
		return m_trxFlushMode;
	}

	inline TNTTrxSysStat getTNTTrxSysStat() {
		if (m_trxCnt != 0) {
			m_stat.m_avgLockCnt = (u32)(m_totalLockCnt/m_trxCnt);
			m_stat.m_avgRedoCnt = (u32)(m_totalRedoCnt/m_trxCnt);
		}
		return m_stat;
	}

	inline void lockTrxSysMutex(const char *file = __FILE__, uint line = __LINE__) {
		m_transMutex.lock(file, line);
	}

	inline void unlockTrxSysMutex() {
		m_transMutex.unlock();
	}

	TrxId getMaxTrxId() const {
		return m_maxTrxId;
	}

	/**
	 * 获得事务锁管理器
	 * @return 
	 */
	inline TLockSys* getLockSys() const {
		return m_lockSys;
	}

	/**
	 * 设置事务锁超时时间
	 * @param lockTimeoutMs 事务锁超时时间，单位毫秒
	 */
	inline void setLockTimeout(int lockTimeoutMs) {
		m_lockSys->setLockTimeout(lockTimeoutMs);
	}

	/**
	 * 设置当前正在使用的版本池ID
	 * @param activeVerPoolId
	 * @param needLock
	 * @return 
	 */
	inline void setActiveVersionPoolId(uint activeVerPoolId, bool needLock = true) {
		if (needLock) {
			MutexGuard(&m_transMutex, __FILE__, __LINE__);
			m_activeVerPoolId = activeVerPoolId;
		} else {
			m_activeVerPoolId = activeVerPoolId;
		}
	}

#ifdef NTSE_UNIT_TEST
	void setMaxTrxId(TrxId maxTrxId);
	ReadView* trxAssignReadView(TNTTransaction *trx, TrxId *trxIds, uint trxCnt) throw(NtseException);
#endif

private:
	// 事务管理底层实现
	bool startTrx(TNTTransaction *trx, bool inner);
	void prepareTrxLow(TNTTransaction *trx);
	void commitTrxLow(TNTTransaction *trx, CommitStat stat);
	bool rollbackLow(TNTTransaction *trx, Session *session, RollBackStat rollBackStat, bool partial = false);
	bool rollbackForRecoverLow(TNTTransaction *trx, Session *session, DList<LogEntry *> *logs);

	TNTTransaction* doAllocTrx(TrxId trxId) throw(NtseException);
	ReadView* openReadViewNow(TrxId trxId, MemoryContext *memCtx) throw(NtseException);
	ReadView* createReadView(TrxId trxId, MemoryContext *memCtx) throw(NtseException);
	bool startTrxLow(TNTTransaction* trx);
	u8 assignVersionPool();
	TrxId getNewTrxId();
	void finishRollbackAll(bool needLog, TNTTransaction *trx);

	void sampleLockAndRedo(TNTTransaction *trx);
	void sampleExecuteTime(TNTTransaction *trx);

private:
	Mutex					m_transMutex;/** 保护全局结构的锁 */
	uint                    m_maxTrxNum; /** 支持的最大活跃事务数 */
	TrxFlushMode            m_trxFlushMode; /** 事务刷日志的方式 */
	TLockSys	            *m_lockSys;  /** 全局的事务锁管理器 */
	TrxId					m_maxTrxId;  /** 当前系统最小未使用ID，下次可分配 */
	DList<TNTTransaction *>	m_activeTrxs;/** 活跃事务链表，按照事务id降序排列 */
	DList<TNTTransaction *> m_activeInnerTrxs; /** 活跃的内部事务链表，按照事务id降序排列 */
	DLink<TNTTransaction *> *m_recoverLastTrxPos; /** 仅用于getPreparedTrxForMysql，用于标识下次查找开始的未知*/
	DList<ReadView*>		m_activeReadViews;/** 活跃事务的read_view列表 */
	DList<MemoryContext*>   m_freeMemCtxList;/** 空闲的内存分配上下文列表 */
	TNTDatabase             *m_db;           /** 数据库对象 */
	ObjectPool<TNTTransaction> m_freeTrxPool;/** 用于防止内存碎片，提高事务分配效率 */
	//Connection              *m_dummyConn;    /** 内部连接 */
	//Session                 *m_dummySession; /** 内部会话 */
	uint                    m_activeVerPoolId;/** 当前活跃版本池ID */

	//以下是关于事务的统计信息
	TNTTrxSysStat           m_stat;
	u64                     m_trxCnt;   //rollback和commit事务的总个数
	u64                     m_totalLockCnt; //所有事务锁的总和
	u64                     m_totalRedoCnt; //所有事务redo记录的总和

	bool                    m_hasHangTrx;
friend class TNTTransaction;
};

}
#endif