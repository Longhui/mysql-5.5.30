/**
 * 会话管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_SESSION_H_
#define _NTSE_SESSION_H_

#include <vector>
#include "misc/Global.h"
#include "util/Sync.h"
#include "util/PagePool.h"
#include "misc/Txnlog.h"
#include "api/Transaction.h"
#include "misc/Config.h"
#include "misc/MemCtx.h"
#include <map>

using namespace std;

#ifdef TNT_ENGINE
namespace tnt {
	class TNTTransaction;
	class TNTDatabase;
}
#endif

namespace ntse {

/** 操作统计类型 */
enum StatType {
	OPS_LOG_READ = 0,	/** 逻辑读次数 */
	OPS_LOG_WRITE,		/** 逻辑写次数 */
	OPS_PHY_READ,		/** 物理读次数 */
	OPS_PHY_WRITE,		/** 物理写次数 */
	OPS_ROW_READ,		/** 读取的记录数 */
	OPS_ROW_INSERT,		/** 插入的记录数 */
	OPS_ROW_UPDATE,		/** 更新的记录数 */
	OPS_ROW_DELETE,		/** 删除的记录数 */
	OPS_TBL_SCAN,		/** 表扫描次数 */
	OPS_TBL_SCAN_ROWS,	/** 表扫描经过的行数 */
	OPS_IDX_SCAN,		/** 索引扫描次数 */
	OPS_IDX_SCAN_ROWS,	/** 索引扫描经过的行数 */
	OPS_POS_SCAN,		/** 定位扫描次数 */
	OPS_POS_SCAN_ROWS,	/** 定位扫描经过的行数 */
	OPS_OPER,			/** 操作次数，语句次数或后台线程操作次数 */
	OPS_NUM,
};

/** 操作次数统计 */
struct OpStat {
	u64		m_statArr[OPS_NUM];	/** 各类操作统计信息 */
	OpStat	*m_parent;			/** 父统计对象，其值为子统计对象之和 */

	OpStat(OpStat *parent) {
		reset();
		m_parent = parent;
	}

	void reset() {
		memset(m_statArr, 0, sizeof(m_statArr));
	}

	inline void countIt(StatType type) {
		assert(type < OPS_NUM);
		m_statArr[type]++;
		if (m_parent)
			m_parent->countIt(type);
	}
};

/** 数据库对象类型 */
enum DBObjType {
	DBO_Unknown,	/** 未知类型 */
	DBO_Heap,		/** 堆（不包含小型大对象堆） */
	DBO_TblDef,     /** tabledef元数据文件*/
	DBO_LlobDir,	/** 大型大对象目录 */
	DBO_LlobDat,	/** 大型大对象数据 */
	DBO_Slob,		/** 小型大对象 */
	DBO_STblDef,
	DBO_Indice,		/** 索引中除具体索引之外的数据 */
	DBO_Index,		/** 表中某个具体的索引 */
};

/** 数据库对象逻辑物理读写状态类型 */
enum DBObjStatType {
    DBOBJ_LOG_READ = 0,		/** 逻辑读 */
	DBOBJ_LOG_WRITE,		/** 逻辑写 */
	DBOBJ_PHY_READ,			/** 物理读 */
	DBOBJ_PHY_WRITE,		/** 物理写 */
	DBOBJ_ITEM_READ,		/** 数据项读 */
	DBOBJ_ITEM_INSERT,		/** 数据项插入 */
	DBOBJ_ITEM_UPDATE,		/** 数据项更新 */
	DBOBJ_ITEM_DELETE,		/** 数据项删除 */
	DBOBJ_SCAN,				/** 扫描次数 */
	DBOBJ_SCAN_ITEM,		/** 扫描返回的数据项数 */
	DBOBJ_MAX,
};

/** 数据库对象公有统计结构 */
struct DBObjStats {
	DBObjType	m_type;					/** 对象类型 */
	const char	*m_idxName;				/** 仅在类型为DBO_Index时为索引名，否则为"" */
	u64			m_statArr[DBOBJ_MAX];	/** 数据对象状态数组 */
	bool		m_bufInternal;			/** 是否是页面缓存模块使用的内部统计结构
										 * 数据库对象统计结构是内部分配而不是外部传入的，
										 * 只有刚被预读进来的页面是这样，因为预读进来的页面的统计结构上层没有给出，
										 * 我们不知道是哪一个。当前只有索引才会存在一个文件可能对应多个统计结构
										 * 的情况，但为了以后扩展时的安全性，对所有类型都不作预读进来的页面的统计
										 * 结构与当前页面相同这一假设。
										 * 内部分配的统计结构使用new/delete分配释放，由于预读进来的页面通常会被
										 * 马上访问到，这时内部分配的统计结构就会被释放，因此内部分配的统计结构
										 * 的数量不会太多，不会导致严重的性能问题。
										 */ 
	
	DBObjStats(DBObjType type, bool bufInternal = false) {
		memset(m_statArr, 0, sizeof(m_statArr));
		m_idxName = "";
		m_type = type;
		m_bufInternal = bufInternal;
	}

	/** 更新指定的统计信息项
	 * @param type 统计信息项
	 * @param delta 差值
	 */
	inline void countIt(DBObjStatType type, int delta = 1) {
		assert(type < DBOBJ_MAX);
		m_statArr[type] += delta;
	}

	/** 将another中的统计结果合并进来
	 * @param another 要合并的统计结果
	 */
	void merge(const DBObjStats *another) {
		for (size_t i = 0; i < sizeof(m_statArr) / sizeof(m_statArr[0]); i++)
			m_statArr[i] += another->m_statArr[i];
	}
};

class Session;
/** 数据库连接 */
class Connection {
public:
#ifdef NTSE_UNIT_TEST
	Connection();
#endif
	Connection(const LocalConfig *globalConfig, OpStat *globalStat, bool internal, const char *name = NULL);
	~Connection() {
		delete []m_name;
	}
	/** 得到连接私有配置
	 * @param 连接私有配置
	 */
	LocalConfig* getLocalConfig() const {
		return (LocalConfig *)&m_config;
	}
	/** 得到连接私有统计信息
	 * @param 连接私有统计信息
	 */
	OpStat* getLocalStatus() const {
		return (OpStat *)&m_stat;
	}
	bool isInternal() const;
	void* getUserData() const;
	void setUserData(void *dat);
	uint getId() const;
	uint getDuration() const;
	/** 获得当前状态信息
	 * @return 当前状态信息，为内部信息的拷贝，调用者需要释放
	 */
	char* getStatus() {
		LOCK(&m_lock);
		char *copy = System::strdup(m_status);
		UNLOCK(&m_lock);
		return copy;
	}
	void setStatus(const char *status);
	const char* getName() const;
	/** 设置使用该连接的线程ID。本应该在构造函数中就指定线程ID，
	 * 但为了防止修改大量获取连接的代码，使用本函数来指定，一个
	 * 连接只能设置一次。
	 *
	 * @pre 以前没有设置过线程ID
	 *
	 * @param id 线程ID，必须为正整数
	 */
	void setThdID(uint id) {
		assert(!m_thdId && id > 0);
		m_thdId = id;
	}
	/** 获取使用该连接的线程ID
	 * @pre 线程ID已经被设置
	 *
	 * @return 使用连接的线程ID
	 */
	uint getThdID() const {
		return m_thdId;
	}

	void setTrx(bool trx) {
		m_trx = trx;
	}

	bool isTrx() {
		return m_trx;
	}
	
private:
	const char	*m_name;			/** 连接名称 */
	LocalConfig	m_config;			/** 当前连接私有配置信息 */
	OpStat		m_stat;				/** 当前连接私有统计信息 */
	bool		m_internal;			/** 是否为内部连接 */
	DLink<Connection *>	m_cpLink;	/** 在连接池中的链接 */
	void		*m_userData;		/** 用户自定义信息 */
	uint		m_id;				/** 连接ID */
	uint		m_thdId;			/** 使用该连接的线程ID */
	const char	*m_status;			/** 状态 */
	u32			m_durationBegin;	/** 当前状态起始时间 */
	bool        m_trx;              /** 标识该链接是否为事务链接 */
	Mutex		m_lock;				/** 保护status的锁 */

friend class SessionManager;
};

struct BufferPageHdr;
class File;
/** 缓存页句柄 */
class BufferPageHandle {
public:
	BufferPageHandle() {
		m_valid = false;
		m_page = NULL;
		m_file = NULL;
		m_line = 0;
		m_lockMode = None;
		m_pinned = false;
	}

	inline BufferPageHdr* getPage() {
		return m_page;
	}

	inline LockMode getLockMode() {
		return m_lockMode;
	}

	inline bool isPinned() {
		return m_pinned;
	}
#ifdef TNT_ENGINE
	inline u64 getPageId() {
		return m_pageId;
	}
#endif

private:
	bool			m_valid;	/** 是否在使用中 */
	BufferPageHdr	*m_page;	/** 对应缓存页 */
	const char		*m_path;	/** 文件路径 */
	u64				m_pageId;	/** 页号 */
	const char		*m_file;	/** 调用这一函数代码所在源文件 */
	uint			m_line;		/** 调用这一函数代码行号 */
	LockMode		m_lockMode;	/** 所加的锁 */
	bool			m_pinned;	/** 是否持有pin */

friend class Session;
};

/** 行锁句柄 */
class RowLockHandle {
public:
	RowLockHandle() {
		m_valid = false;
		m_tableId = 0;
		m_rid = 0;
		m_mode = None;
	}

	inline u16 getTableId() const {
		return m_tableId;
	}

	inline RowId getRid() const {
		return m_rid;
	}

	inline LockMode getLockMode() const {
		return m_mode;
	}

private:
	bool		m_valid;	/** 是否在使用中 */
	u16			m_tableId;	/** 我锁住的行所属表ID */
	RowId		m_rid;		/** 我锁住的行的RID */
	LockMode	m_mode;		/** 锁模式 */
	const char	*m_file;	/** 调用这一函数代码所在源文件 */
	uint		m_line;		/** 调用这一函数代码行号 */

friend class Session;
};

class RowLockHandleElem : public RowLockHandle {
public:
	RowLockHandleElem() {
		m_poolId = 0;
		m_link.set(this);
	}

	inline void relatedWithList(DList<RowLockHandleElem*> *list) {
		list->addLast(&m_link);
	}

	inline void excludeFromList() {
		m_link.unLink();
	}

private:
	DLink<RowLockHandleElem *> m_link;
	size_t m_poolId;  /** 在小型内存池中的ID */

friend class Session;
};

class UKLockManager;
class UKLockHandle {
public:
	UKLockHandle() 
		: m_lockMgr(NULL), m_key(0), m_file(NULL), m_line(0), m_poolId(0) {
		m_link.set(this);
	}

	inline void relatedWithList(DList<UKLockHandle*> *list) {
		list->addLast(&m_link);
	}

	inline void excludeFromList() {
		m_link.unLink();
	}

protected:
	UKLockManager *m_lockMgr;
	u64           m_key;
	const char    *m_file;
	uint          m_line;
	size_t        m_poolId;
	DLink<UKLockHandle *> m_link;

friend class Session;
};

struct SesScanHandle;
struct ConnScanHandle;
class Session;
class Database;
class Connection;
/** 会话管理器 */
class SessionManager {
public:
	SessionManager(Database *db, u16 maxSessions, u16 internalSessions);
	~SessionManager();
#ifdef TNT_ENGINE
	void setTNTDb(TNTDatabase *tntDb);
#endif
	Session* allocSession(const char *name, Connection *conn, int timeoutMs = -1);
	Session* getSessionDirect(const char *name, u16 id, Connection *conn);
	void freeSession(Session *session);
	u16 getMaxSessions();
	void dumpBufferPageHandles();
	void dumpRowLockHandles();
	u64 getMinTxnStartLsn();
	u16 getActiveSessions();
	SesScanHandle* scanSessions();
	const Session* getNext(SesScanHandle *h);
	void endScan(SesScanHandle *h);

	Connection* getConnection(bool internal, const LocalConfig *config, OpStat *globalStat, const char *name = NULL);
	void freeConnection(Connection *conn);
	ConnScanHandle* scanConnections();
	const Connection* getNext(ConnScanHandle *h);
	void endScan(ConnScanHandle *h);

	/** 内部会话个数限制，不做成配置参数，因这一限制主要系统
	 * 实现有关，与负载关系不大
	 */
	static const uint INTERNAL_SESSIONS = 8;

private:
	Session* tryAllocSession(const char *name, Connection *conn, int high, int low);

private:
	u16		m_maxSessions;		/** 最大会话数 */
	u16		m_internalSessions;	/** 内部会话数 */
	Session	**m_sessions;		/** 各会话对象 */
	Mutex	m_lock;				/** 保护并发的锁 */
	DList<Connection *>	*m_activeConns;	/** 活跃连接 */
	uint	m_nextConnId;		/** 下一个连接ID */
};

class Database;
class Connection;
class Buffer;
class LockManager;
class IndicesLockManager;
class File;
class MemoryContext;
class Record;
class SubRecord;
struct LogEntry;
class TableDef;
class ColumnDef;
struct Stream;
/** 会话。基本上用于维护一个语句执行过程中NTSE底层的缓存页锁定等资源 */
class Session {
public:
#ifdef NTSE_UNIT_TEST
	Session(Connection *conn, Buffer *buffer);
#endif
	virtual ~Session();
	// 信息获取
	/** 获取会话ID
	 * @return 会话ID，一定>0
	 */
	u16 getId() const {
		assert(m_id > 0);
		return m_id;
	}
	/** 返回会话级内存分配上下文
	 * @return 会话级内存分配上下文
	 */
	MemoryContext* getMemoryContext() const {
		assert(m_inuse);
		return m_memoryContext;
	}
	/** 返回专用于存储查询时所返回的大对象内容的内存分配上下文，注意这一
	 * 内存分配上下文并不用于在IUD操作中的大对象相关处理
	 * @return 会话级内存分配上下文
	 */
	MemoryContext* getLobContext() const {
		assert(m_inuse);
		return m_lobContext;
	}
	/** 返回会话所属的数据库连接
	 * @return 会话所属的数据库连接
	 */
	Connection* getConnection() const {
		assert(m_inuse);
		return m_conn;
	}
	/** 获取会话分配时的时间
	 * @return 会话分配时的时间，单位秒
	 */
	u32 getAllocTime() const {
		assert(m_inuse);
		return m_allocTime;
	}
	/** 增加指定操作的次数
	 * @param type 操作类型
	 */
	void incOpStat(StatType type) {
		m_conn->getLocalStatus()->countIt(type);
	}
	/** 设置会话取消状态
	 * @param canceled 是否取消
	 */
	void setCanceled(bool canceled) {
		m_canceled = canceled;
	}
	/** 获取会话取消状态
	 * @return 是否取消
	 */
	bool isCanceled() const {
		return m_canceled;
	}

#ifdef TNT_ENGINE
	inline tnt::TNTTransaction* getTrans() const {
		return m_trx;
	}
	void setTrans(tnt::TNTTransaction* trans) {
		m_trx = trans;
	}

	inline TNTDatabase *getTNTDb() {
		return m_tntDb;
	}

	inline Database *getNtseDb() {
		return m_db;
	}
#endif

	// 缓存页管理
	BufferPageHandle* newPage(File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats);
	BufferPageHandle* getPage(File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats, BufferPageHdr *guess = NULL);
	BufferPageHandle* tryGetPage(File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats);
	void releasePage(BufferPageHandle **handle);
	BufferPageHandle* lockPage(BufferPageHdr *page, File *file, u64 pageId, LockMode lockMode, const char *sourceFile, uint line);
	void lockPage(BufferPageHandle *handle, LockMode lockMode, const char *sourceFile, uint line);
	void upgradePageLock(BufferPageHandle *handle, const char *sourceFile, uint line);
	void unlockPage(BufferPageHandle **handle);
	void unpinPage(BufferPageHandle **handle);
	void markDirty(BufferPageHandle *handle);
	void freePages(File *file, bool writeDirty);
	void freePages(File *file, uint indexId, bool (*fn)(BufferPageHdr *page, PageId pageId, uint indexId));
	void dumpBufferPageHandles() const;
	// 行锁管理
	RowLockHandle* tryLockRow(u16 tableId, RowId rid, LockMode lockMode, const char *sourceFile, uint line);
	RowLockHandle* lockRow(u16 tableId, RowId rid, LockMode lockMode, const char *sourceFile, uint line);
	bool isRowLocked(u16 tableId, RowId rid, LockMode lockMode);
	void unlockRow(RowLockHandle **handle);
	void dumpRowLockHandles();
	//唯一性键值锁管理
	bool tryLockUniqueKey(UKLockManager *uniqueKeyMgr, u64 keyChecksum, const char *file, uint line);
	bool lockUniqueKey(UKLockManager *uniqueKeyMgr, u64 keyChecksum, const char *file, uint line);
	void unlockUniqueKey(UKLockHandle* ukLockHdl);
	void unlockAllUniqueKey();
	// 索引页面锁管理
	bool tryLockIdxObject(u64 objectId);
	bool lockIdxObject(u64 objectId);
	u64 getToken();
	bool unlockIdxObject(u64 objectId, u64 token = 0);
	bool unlockIdxAllObjects();
	bool unlockIdxObjects(u64 token = 0);
	bool isLocked(u64 objectId);
	bool hasLocks();
	u64 whoIsHolding(u64 objectId);
	// 其它锁对象管理
	void addLock(const Lock *lock);
	void removeLock(const Lock *lock);
	// 事务及日志
	void startTransaction(TxnType type, u16 tableId, bool writeLog = true);
	void endTransaction(bool commit, bool writeLog = true);
	TxnType getTxnStatus() const;
	bool isLogging();
	void disableLogging();
	void enableLogging();
	void cacheLog(LogType logType, u16 tableId, const byte *data, size_t size);
	u64 writeLog(LogType logType, u16 tableId, const byte *data, size_t size);
	u64 writeCpstLog(LogType logType, u16 tableId, u64 cpstForLsn, const byte *data, size_t size);
	void flushLog(LsnType lsn, FlushSource fs);
	u64 getLastLsn() const;
	u64 getTxnStartLsn() const;
	u64 getTxnDurableLsn() const;
	void setTxnDurableLsn(u64 lsn);
	bool isTxnCommit() const;
	byte* constructPreUpdateLog(const TableDef *tableDef, const SubRecord *before, const SubRecord *update,
		bool updateLob, const SubRecord *indexPreImage, size_t *size) throw(NtseException);
	void writePreUpdateLog(const TableDef *tableDef, const byte *log, size_t size);
	PreUpdateLog* parsePreUpdateLog(const TableDef *tableDef, const LogEntry *log);
	static RowId getRidFromPreUpdateLog(const LogEntry *log);
	void writePreUpdateHeapLog(const TableDef *tableDef, const SubRecord *update);
	SubRecord* parsePreUpdateHeapLog(const TableDef *tableDef, const LogEntry *log);
	static TxnType parseTxnStartLog(const LogEntry *log);
	static bool parseTxnEndLog(const LogEntry *log);



private:	// 这些函数只被SessionManager调用
	Session(u16 id, Database *db);
	void reset();

private:
	BufferPageHandle* allocBufferPageHandle(BufferPageHdr *page, const char *path, u64 pageId, 
		LockMode lockMode, bool pinned, const char *file, uint line);
	void freeBufferPageHandle(BufferPageHandle *handle);
	RowLockHandle* allocRowLockHandle(u16 tableId, RowId rid, LockMode lockMode, const char *file, uint line);
	void freeRowLockHandle(RowLockHandle *handle);
	void checkBufferPages();
	void checkRowLocks();
	void checkIdxLocks();
	void checkLocks();
	inline u64 getGlobalRid(u16 tableId, RowId rid) const {
		return (((u64)tableId) << RID_BITS) | rid;
	}
	u64 writeTxnStartLog(u16 tableId, TxnType type);
	u64 writeTxnEndLog(bool commit);
	UKLockHandle* allocNewUKLockHandle();
	void freeUKLockHandle(UKLockHandle *ukLockHdl);

private:
	/** 一个会话最多可能同时持有的缓存页数 */
	static const uint MAX_PAGE_HANDLES = Limits::MAX_BTREE_LEVEL * 2 + 4;

	// Note: 增加属性时记得检查是否需要在Session::reset时重置状态
	BufferPageHandle	m_bpHandles[MAX_PAGE_HANDLES];	/** 缓存页句柄数组 */

	ObjectPool<RowLockHandleElem> m_rlHandlePool; /** 行锁句柄对象分配池 */
	DList<RowLockHandleElem*> m_rlHandleInUsed;   /** 当前持有的行锁列表 */
	
	vector<const Lock *>	m_locks;	/** 粗粒度锁对象 */
	Database	*m_db;					/** 数据库对象 */
	Connection	*m_conn;				/** 数据库连接 */
	const char	*m_name;				/** 会话名称 */
#ifdef NTSE_UNIT_TEST
	bool		m_bufferTest;			/** 仅用于单元测试所用的mock对象 */
#endif
	Buffer		*m_buffer;				/** 页面缓存 */
	LockManager	*m_rowLockMgr;			/** 行锁管理器 */
	Txnlog		*m_txnLog;				/** 事务日志 */
	TxnType		m_txnStatus;			/** 是否在事务中 */
	u16			m_id;					/** 会话ID，同时也充当事务ID */
	u16			m_tableId;				/** startTransaction时操作的tableId */
	u64			m_lastLsn;				/** 本会话写的最后一条日志的LSN */
	u64			m_txnStartLsn;			/** 事务起始LSN */
	u64			m_txnDurableLsn;		/** 若此日志被写出，即使事务日志不完整，
										 * 事务也会被REDO，其效果也会被持久化
										 */
	bool		m_isTxnCommit;			/** 刚刚结束的事务是否成功 */
	MemoryContext	*m_memoryContext;	/** 会话对应的内存分配上下文 */
	MemoryContext	*m_lobContext;		/** 专用于存储查询时所返回的大对象内容
										 * 的内存分配上下文，注意这一内存分配
										 * 上下文并不用于在IUD操作中的大对象
										 * 相关处理
										 */
	LockManager  *m_uniqueIdxLockMgr;
	ObjectPool<UKLockHandle> m_ukLockHandlePool;
	DList<UKLockHandle*> m_ukLockList;

	IndicesLockManager *m_idxLockMgr;	/** 索引加页面锁使用的锁表管理器 */
	map<u64, PageId> m_lockedIdxObjects;/** 用来保存该会话加过的所有索引锁对象信息 */
	u64			m_token;				/** 会话每加一个页面锁，对应一个不同的值，该值只增不减 */
	bool		m_inuse;				/** 是否在使用中 */
	u32			m_allocTime;			/** 会话分配时的时间 */
	bool		m_logging;				/** 是否记录日志 */
	bool		m_canceled;				/** 操作是否被取消 */

#ifdef TNT_ENGINE
	tnt::TNTTransaction	*m_trx;				/** 当前session所对应的事务 */
	tnt::TNTDatabase    *m_tntDb;
#endif

friend class SessionManager;
friend class BgTask;
};

#define NEW_PAGE(session, file, pageType, pageId, lockMode, dbObjStats)	(session)->newPage((file), (pageType), (pageId), (lockMode), __FILE__, __LINE__, (dbObjStats));
#define GET_PAGE(session, file, pageType, pageId, lockMode, dbObjStats, guess)	(session)->getPage((file), (pageType), (pageId), (lockMode), __FILE__, __LINE__, (dbObjStats), (guess))
#define TRY_GET_PAGE(session, file, pageType, pageId, lockMode, dbObjStats)	(session)->tryGetPage((file), (pageType), (pageId), (lockMode), __FILE__, __LINE__, (dbObjStats))
#define LOCK_PAGE_HANDLE(session, handle, lockMode)	(session)->lockPage((handle), (lockMode), __FILE__, __LINE__)
#define LOCK_PAGE(session, page, file, pageId, lockMode)	(session)->lockPage((page), (file), (pageId), (lockMode), __FILE__, __LINE__)
#define UPGRADE_PAGELOCK(session, pageHandle)	(session)->upgradePageLock((pageHandle), __FILE__, __LINE__)
#define LOCK_ROW(session, tableId, rid, mode)	(session)->lockRow((tableId), (rid), (mode), __FILE__, __LINE__)
#define TRY_LOCK_ROW(session, tableId, rid, mode)	(session)->tryLockRow((tableId), (rid), (mode), __FILE__, __LINE__)


/** 后台数据库任务线程。这一任务线程的主要功能是分配线程所用数据库连接和会话。
 * 若构造BgTask时指定了数据库，则会在BgTask的setUp和tearDown函数中分配与释放
 * 数据库连接。同时BgTask在每次调用runIt函数之前会准备好会话。如果构造时指定
 * alwaysHoldSession，则始终使用一个会话，在setUp与tearDown中分配与释放，这时
 * 每次调用完runIt函数后BgTask会重置会话。如果没有指定alwaysHoldSession，则
 * BgTask会在调用runIt之前分配一个会话，runIt返回后马上释放。如果allocSessionTimeoutMs
 * 不是-1，则在指定的时间内分配不到会话时，runIt函数不会被调用。
 */
class BgTask: public Task {
public:
	BgTask(Database *db, const char *name, uint interval, bool alwaysHoldSession = false, int allocSessionTimeoutMs = -1, bool runInRecover = false);
	virtual ~BgTask();
	void setUp();
	void tearDown();
	void run();
	bool shouldRunInRecover();
	bool setUpFinished();
	/** 重载这一函数实现每次运行时要作的操作 */
	virtual void runIt() = 0;

protected:
	Database	*m_db;		/** 数据库，可能为NULL */
	Connection	*m_conn;	/** 数据库连接，若m_db为NULL则为NULL */
	Session		*m_session;	/** 会话，若m_db为NULL则为NULL。每次doRun运行之前分配，运行之后释放 */
	bool		m_alwaysHoldSession;		/** 是否始终持有会话，若为false则每次运行时取得会话 */
	int			m_allocSessionTimeoutMs;	/** 分配会话超时时间 */
	bool		m_runInRecover;		/** 恢复过程中是否也需要运行 */
	bool		m_setUpFinished;	/** setUp是否已经完成 */
};
}

#endif
