/**
 * 会话管理
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include <algorithm>
#include "misc/Session.h"
#include "api/Database.h"
#include "misc/Buffer.h"
#include "misc/LockManager.h"
#include "misc/IndiceLockManager.h"
#include "misc/Syslog.h"
#include "util/File.h"
#include "api/Table.h"
#include "util/Stream.h"
#include "util/Thread.h"
#include "misc/Trace.h"
#include "misc/MemCtx.h"
#include "misc/Profile.h"

#ifdef TNT_ENGINE
#include "api/TNTDatabase.h"
#endif

namespace ntse {
///////////////////////////////////////////////////////////////////////////////
// SessionManager /////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/**
 * 创建一个会话管理器
 *
 * @param db 数据库
 * @param maxSessions 最大会话数
 * @param internalSessions 内部会话数
 */
SessionManager::SessionManager(Database *db, u16 maxSessions, u16 internalSessions): m_lock("SessionManager::lock", __FILE__, __LINE__) {
	assert(maxSessions >= internalSessions);
	m_maxSessions = maxSessions;
	m_internalSessions = internalSessions;
	m_sessions = new Session *[m_maxSessions];
	for (u16 i = 0; i < m_maxSessions; i++) {
		m_sessions[i] = new Session(i + 1, db);
	}
	m_activeConns = new DList<Connection *>();
	m_nextConnId = 1;
}

SessionManager::~SessionManager() {
	for (u16 i = 0; i < m_maxSessions; i++) 
		delete m_sessions[i];
	delete []m_sessions;
	DLink<Connection *>* dlink;
	while (NULL != (dlink = m_activeConns->removeLast()))
		delete dlink->get();
	delete m_activeConns;
}

#ifdef TNT_ENGINE
void SessionManager::setTNTDb(TNTDatabase *tntDb) {
	for (u16 i = 0; i < m_maxSessions; i++) {
		m_sessions[i]->m_tntDb = tntDb;
	}
}
#endif

/**
 * 分配一个会话
 * 
 * @param name 会话名称，不允许为NULL。直接引用不拷贝
 * @param conn 数据库连接，不允许为NULL
 * @param timeoutMs 超时时间，<0表示不超时，0表示马上超时，>0表示毫秒数指定的超时时间
 * @return 成功返回会话对象，若所有会话都在使用中返回NULL，如果retry为true则不可能返回NULL
 */
Session* SessionManager::allocSession(const char *name, Connection *conn, int timeoutMs) {
	assert(name && conn);

	int low, high;
	if (conn->isInternal()) {
		low = 0;
		high = m_internalSessions - 1;
	} else {
		low = m_internalSessions;
		high = m_maxSessions - 1;
	}
	Session *ret = tryAllocSession(name, conn, high, low);
	if (ret)
		return ret;
	u64 before = System::currentTimeMillis();
_retry:
	ret = tryAllocSession(name, conn, high, low);
	if (!ret) {
		if (timeoutMs == 0 || (timeoutMs > 0 && (System::currentTimeMillis() - before) > (u64)timeoutMs))
			return NULL;
		Thread::msleep(10);
		goto _retry;
	}
	return ret;
}

/** 尝试分配会话
 * @param name 会话名称
 * @param conn 连接
 * @param high 上界
 * @param low 下界
 * @return 分配到返回会话，分配不到返回NULL
 */
Session* SessionManager::tryAllocSession(const char *name, Connection *conn, int high, int low) {
	Session *ret = NULL;
	LOCK(&m_lock);
	for (int i = high; i >= low; i--) {
		if (!m_sessions[i]->m_inuse) {
			m_sessions[i]->m_inuse = true;
			ret = m_sessions[i];
			ret->m_conn = conn;
			ret->m_name = name;
			ret->m_allocTime = System::fastTime();
			break;
		}
	}
	UNLOCK(&m_lock);
	return ret;
}

/**
 * 直接获取指定ID的会话（在恢复时使用）
 * 
 * @param name 会话名称，不允许为NULL。直接引用不拷贝
 * @param id 会话ID
 * @param conn 数据库连接，不允许为NULL
 * @return 成功返回会话对象，若指定会话在使用中返回NULL
 */
Session* SessionManager::getSessionDirect(const char *name, u16 id, Connection *conn) {
	assert(name);
	assert(conn);
	assert(id > 1);

	if (id > m_maxSessions)
		return NULL;

	Session *ret = NULL;
	LOCK(&m_lock);
	if (!m_sessions[id - 1]->m_inuse) {
		m_sessions[id - 1]->m_inuse = true;
		ret = m_sessions[id - 1];
		ret->m_conn = conn;
		ret->m_name = name;
		ret->m_allocTime = System::fastTime();
	}
	UNLOCK(&m_lock);
	return ret;
}

/**
 * 释放一个会话
 *
 * @param 要释放的会话对象
 */
void SessionManager::freeSession(Session *session) {
	session->reset();

	LOCK(&m_lock);
	assert(session->m_inuse);
	session->m_inuse = false;
	UNLOCK(&m_lock);
}

/**
 * 获得最大会话数
 *
 * @return 最大会话数
 */
u16 SessionManager::getMaxSessions() {
	return m_maxSessions;
}

/**
 * 打印使用中的页面缓存句柄信息到标准输出
 * 注: 本函数在访问各会话信息时无法实现同步控制，只应在调试时使用
 */
void SessionManager::dumpBufferPageHandles() {
	for (int i = 0; i < m_maxSessions; i++) {
		if (m_sessions[i]->m_inuse)
			m_sessions[i]->dumpBufferPageHandles();
	}
}

/**
 * 打印使用中的行锁缓存句柄信息到标准输出
 * 注: 本函数在访问各会话信息时无法实现同步控制，只应在调试时使用
 */
void SessionManager::dumpRowLockHandles() {
	for (int i = 0; i < m_maxSessions; i++) {
		if (m_sessions[i]->m_inuse)
			m_sessions[i]->dumpRowLockHandles();
	}
}

/**
 * 获得所有活跃事务起始LSN的最小值
 *
 * @return 所有活跃事务起始LSN的最小值，若没有活跃事务则返回INVALID_LSN
 */
u64 SessionManager::getMinTxnStartLsn() {
	u64 ret = INVALID_LSN;

	LOCK(&m_lock);
	for (int i = m_maxSessions - 1; i >= 0; i--) {
		if (m_sessions[i]->m_inuse) {
			u64 lsn = m_sessions[i]->getTxnStartLsn();
			if (lsn != INVALID_LSN) {
				if (ret == INVALID_LSN)
					ret = lsn;
				else if (lsn < ret)
					ret = lsn;
			}
		}
	}
	UNLOCK(&m_lock);

	return ret;
}

/**
 * 得到目前使用中的会话个数
 *
 * @return 使用中的会话个数
 */
u16 SessionManager::getActiveSessions() {
	u16 n = 0;
	LOCK(&m_lock);
	for (u16 i = 0; i < m_maxSessions; i++) {
		if (m_sessions[i]->m_inuse)
			n++;
	}
	UNLOCK(&m_lock);
	return n;
}

/** 会话遍历扫描句柄 */
struct SesScanHandle {
	u16		m_curPos;	/** 当前位置 */

	SesScanHandle() {
		m_curPos = 0;
	}
};

/**
 * 遍历使用中的会话
 *
 * @return 扫描句柄
 */
SesScanHandle* SessionManager::scanSessions() {
	LOCK(&m_lock);
	return new SesScanHandle();
}

/**
 * 得到下一个使用中的会话
 * @post 会话在下一次getNext或endScan之前不会被释放
 *
 * @param h 扫描句柄
 * @return 下一个使用中的会话，没有则返回NULL
 */
const Session* SessionManager::getNext(SesScanHandle *h) {
	while (h->m_curPos < m_maxSessions) {
		if (m_sessions[h->m_curPos]->m_inuse)
			return m_sessions[h->m_curPos++];
		h->m_curPos++;
	}
	return NULL;
}

/**
 * 结束会话遍历扫描
 * @post 扫描句柄对象已经被销毁
 *
 * @param h 扫描句柄
 */
void SessionManager::endScan(SesScanHandle *h) {
	UNLOCK(&m_lock);
	delete h;
}

/**
 * 创建一个连接（从连接池中获取）
 *
 * @param internal 是否为内部连接，内部连接指的为了执行与用户请求没有
 *   直接关系的操作（如检查点、MMS刷更新缓存等）而获取的连接
 * @param config 连接私有状态
 * @param globalStat 全局操作统计
 * @param name 连接名称
 * @return 数据库连接
 */
Connection* SessionManager::getConnection(bool internal, const LocalConfig *config, OpStat *globalStat, const char *name) {
	Connection *conn = new Connection(config, globalStat, internal, name);
	LOCK(&m_lock);
	conn->m_id = m_nextConnId++;
	m_activeConns->addFirst(&conn->m_cpLink);
	UNLOCK(&m_lock);
	return conn;
}

/**
 * 释放一个连接（到连接池）
 * @pre 要释放的连接conn为使用中的连接
 *
 * @param 数据库连接
 */
void SessionManager::freeConnection(Connection *conn) {
	assert(conn);

	LOCK(&m_lock);
	conn->m_cpLink.unLink();
	UNLOCK(&m_lock);

	delete conn;
}

/** 连接遍历扫描句柄 */
struct ConnScanHandle {
	DLink<Connection *>	*m_curPos;	/** 当前连接 */
};

/**
 * 遍历使用中的连接对象
 * 注：遍历过程中系统将不能分配与释放连接
 *
 */
ConnScanHandle* SessionManager::scanConnections() {
	LOCK(&m_lock);
	ConnScanHandle *h = new ConnScanHandle();
	h->m_curPos = m_activeConns->getHeader()->getNext();
	return h;
}

/**
 * 得到下一个连接
 *
 * @param h 扫描句柄
 * @return 下一个连接，若没有则返回NULL
 */
const Connection* SessionManager::getNext(ConnScanHandle *h) {
	if (h->m_curPos == m_activeConns->getHeader())
		return NULL;
	Connection *conn = h->m_curPos->get();
	h->m_curPos = h->m_curPos->getNext();
	return conn;
}

/**
 * 结束连接遍历
 * @post 扫描句柄对象已经被销毁
 *
 * @param h 扫描句柄
 */
void SessionManager::endScan(ConnScanHandle *h) {
	delete h;
	UNLOCK(&m_lock);
}

///////////////////////////////////////////////////////////////////////////////
// Session ////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////


/**
 * 构造函数
 *
 * @param id 会话ID
 * @param db 数据库
 */
Session::Session(u16 id, Database *db) {
	m_id = id;
	m_db = db;
	m_buffer = db->getPageBuffer();
	m_rowLockMgr = db->getRowLockManager();
	m_idxLockMgr = db->getIndicesLockManager();
	m_token = 0;
	m_txnLog = db->getTxnlog();
	m_txnStatus = TXN_NONE;
	m_lastLsn = INVALID_LSN;
#ifdef TNT_ENGINE
	m_tntDb = NULL;
#endif
	m_txnStartLsn = INVALID_LSN;
	m_memoryContext = new MemoryContext(Limits::PAGE_SIZE, 4);
	m_lobContext = new MemoryContext(Limits::PAGE_SIZE, 1);
#ifdef NTSE_UNIT_TEST
	m_bufferTest = false;
#endif
	m_inuse = false;
	m_logging = true;
	m_canceled = false;
	m_trx = NULL;
}

#ifdef NTSE_UNIT_TEST
/**
 * 分配一个页面缓存测试时所用的虚拟会话。定义了NTSE_UNIT_TEST编译参数后才可用
 *
 * @param conn 连接
 * @param buffer 页面缓存
 */
Session::Session(Connection *conn, Buffer *buffer) {
	m_id = 1;
	m_buffer = buffer;
	m_conn = conn;
	m_bufferTest = true;
	m_memoryContext = NULL;
	m_lobContext = NULL;
	m_txnStatus = TXN_NONE;
	m_lastLsn = INVALID_LSN;
	m_txnStartLsn = INVALID_LSN;
	m_rowLockMgr = NULL;
	m_idxLockMgr = NULL;
	m_txnLog = NULL;
	m_inuse = false;
	m_logging = true;
	m_canceled = false;
}
#endif

Session::~Session() {
	delete m_memoryContext;
	delete m_lobContext;
}

/**
 * 分配一个新面，pin和锁定该页
 *
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 * @param dbObjStats 数据对象状态
 *
 * @return 缓存页句柄
 */
BufferPageHandle* Session::newPage( File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats ) {
	assert(m_inuse);
	BufferPageHdr *page = m_buffer->newPage(this, file, pageType, pageId, lockMode, dbObjStats);
	return allocBufferPageHandle(page, file->getPath(), pageId, lockMode, true, sourceFile, line);
}

/**
 * 获取一个页，pin和锁定该页
 *
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 * @param dbObjStats 数据对象状态
 * @guess 要读取的页很有可能是这个地址
 *
 * @return 缓存页句柄
 */
BufferPageHandle* Session::getPage( File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats, BufferPageHdr *guess /*= NULL*/ ) {
	assert(m_inuse);
	BufferPageHdr *page = m_buffer->getPage(this, file, pageType, pageId, lockMode, dbObjStats, guess);
	return allocBufferPageHandle(page, file->getPath(), pageId, lockMode, true, sourceFile, line);
}

/**
 * 试图获取一个页，pin并且锁定该页
 *
 * @param file 文件
 * @param pageType 页面类型
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 * @param dbObjStats 数据对象状态
 *
 * @return 缓存页句柄，若为NULL表示未加锁成功
 */
BufferPageHandle* Session::tryGetPage( File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats )
{
	assert(m_inuse);
	BufferPageHdr *page = m_buffer->tryGetPage(this, file, pageType, pageId, lockMode, dbObjStats);
	if (!page)
		return NULL;
	return allocBufferPageHandle(page, file->getPath(), pageId, lockMode, true, sourceFile, line);
}

/**
 * 释放一个页，包括释放该页的锁和pin
 *
 * @param handle 缓存页句柄
 */
void Session::releasePage(BufferPageHandle **handle) {
	assert(m_inuse);
	assert((*handle)->m_valid);
	assert((*handle)->m_pinned);

	m_buffer->releasePage(this, (*handle)->m_page, (*handle)->m_lockMode);
	freeBufferPageHandle(*handle);
	*handle = NULL;
}

/**
 * 锁定一个页
 *
 * @param page 缓存页
 * @param file 文件
 * @param pageId 页号
 * @param lockMode 锁模式
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 * @return 缓存页句柄
 */
BufferPageHandle* Session::lockPage(BufferPageHdr *page, File *file, u64 pageId, LockMode lockMode, const char *sourceFile, uint line) {
	assert(m_inuse);
	m_buffer->lockPage(m_id, page, lockMode, true);
	return allocBufferPageHandle(page, file->getPath(), pageId, lockMode, false, sourceFile, line);
}

/**
 * 已知一个缓存页句柄时，重新锁定一个页
 * @pre 调用者应该在当前会话中持有对该页的pin
 *
 * @param handle 缓存页句柄
 * @param lockMode 锁模式
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 */
void Session::lockPage(BufferPageHandle *handle, LockMode lockMode, const char *sourceFile, uint line) {
	assert(m_inuse);
	assert(handle->m_valid);
	assert(handle->m_lockMode == None);
	assert(handle->m_pinned);
	m_buffer->lockPage(m_id, handle->m_page, lockMode, false);
	handle->m_lockMode = lockMode;
	handle->m_file = sourceFile;
	handle->m_line = line;
}

/**
 * 释放一个页上的锁，但不释放pin
 * 若缓存页句柄是通过lockPage获得，只持有锁不持有pin则调用本函数之后
 * 缓存页句柄已经被释放。
 *
 * @param handle 缓存页句柄
 */
void Session::unlockPage(BufferPageHandle **handle) {
	assert(m_inuse);
	assert((*handle)->m_valid);
	assert((*handle)->m_lockMode != None);
	m_buffer->unlockPage(m_id, (*handle)->m_page, (*handle)->m_lockMode);
	if (!(*handle)->m_pinned) {
		freeBufferPageHandle(*handle);
		*handle = NULL;
	} else
		(*handle)->m_lockMode = None;
}

/**
 * 将一个页上的锁由共享锁升级为互斥锁
 * @pre 调用者一定要持有页面上的pin
 *
 * @param handle 缓存页句柄
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 */
void Session::upgradePageLock(BufferPageHandle *handle, const char *sourceFile, uint line) {
	assert(m_inuse);
	assert(handle->m_pinned);
	m_buffer->upgradePageLock(this, handle->m_page);
	handle->m_lockMode = Exclusived;
	handle->m_file = sourceFile;
	handle->m_line = line;
}

/**
 * 释放一个页上的pin
 * @pre 调用者不持有页上的锁，持有锁时释放pin没有任何意义
 *
 * @param handle 缓存页句柄
 */
void Session::unpinPage(BufferPageHandle **handle) {
	assert(m_inuse);
	assert((*handle)->m_valid);
	assert((*handle)->m_lockMode == None);
	assert((*handle)->m_pinned);
	m_buffer->unpinPage((*handle)->m_page);
	freeBufferPageHandle(*handle);
	*handle = NULL;
}

/**
 * 设置一个页为脏页
 *
 * @param handle 缓存页句柄
 */
void Session::markDirty(ntse::BufferPageHandle *handle) {
	assert(m_inuse);
	assert(handle->m_valid);
	m_buffer->markDirty(this, handle->m_page);
}

/**
 * 释放页面缓存中指定文件缓存的所有页
 *
 * @param file 文件
 * @param writeDirty 是否写出脏页
 */
void Session::freePages(File *file, bool writeDirty) {
	assert(m_inuse);
	return m_buffer->freePages(this, file, writeDirty);
}

/**
 * 释放页面缓存中为指定索引缓存的所有页
 *
 * @param file 文件
 * @param indexId 索引ID
 * @param callback 用于判断一个页是否是指定索引使用的页的回调函数
 *   若为NULL则释放为指定文件缓存的所有页
 */
void Session::freePages(File *file, uint indexId, bool (*callback)(BufferPageHdr *page, PageId pageId, uint indexId)) {
	assert(m_inuse);
	return m_buffer->freePages(this, file, indexId, callback);
}

/**
 * 开始事务。NTSE中的事务与普通数据库中的事务不同，
 * 仅用于保证对一条记录所做操作的原子性。
 *
 * @param type 事务类型
 * @param tableId 事务要修改的表的ID（不考虑小型大对象虚拟表的ID）
 * @param writeLog 是否写日志
 */
void Session::startTransaction(TxnType type, u16 tableId, bool writeLog) {
	assert(m_inuse);
	assert(m_txnStatus == TXN_NONE);
	m_txnStatus = type;
	m_txnDurableLsn = INVALID_LSN;
	m_tableId = tableId;
	if (writeLog && m_logging) {
		m_lastLsn = writeTxnStartLog(tableId, type);
		m_txnStartLsn = m_lastLsn;
	}
}

/**
 * 结束事务
 *
 * @param commit 是提交还是回滚
 * @param writeLog 是否写日志
 */
void Session::endTransaction(bool commit, bool writeLog) {
	assert(m_inuse);
	assert(m_txnStatus != TXN_NONE);
	if (writeLog && m_logging)
		m_lastLsn = writeTxnEndLog(commit);
	m_txnStatus = TXN_NONE;
	m_txnStartLsn = INVALID_LSN;
	m_isTxnCommit = commit;
}

/** 得到事务状态
 * @param return 当前会话在什么事务中，TXN_NONE表示不在事务中
 */
TxnType Session::getTxnStatus() const {
	return m_txnStatus;
}

/** 返回本会话所作操作是否会记录日志
 * @return 是否记录日志
 */
bool Session::isLogging() {
	assert(m_inuse);
	return m_logging;
}

/** 指定本会话操作所作操作不要写日志
 * @pre 不能在事务中
 */
void Session::disableLogging() {
	assert(m_inuse);
	assert(m_txnStatus == TXN_NONE);
	m_logging = false;
}

/** 指定本会话操作所作操作要写日志
 * @pre 不能在事务中
 */
void Session::enableLogging() {
	assert(m_inuse);
	assert(m_txnStatus == TXN_NONE);
	m_logging = true;
}

/**
 * 缓存一条日志
 *
 * @param logType 日志类型
 * @param tableId 表ID
 * @param data 日志内容
 * @param size 日志内容大小
 */
void Session::cacheLog(LogType logType, u16 tableId, const byte *data, size_t size) {
	assert(m_inuse);
	if (!m_logging)
		return;
	// 先简单实现，直接写到日志文件中，如果有性能问题再来优化
	m_lastLsn = writeLog(logType, tableId, data, size);
}

/**
 * 写日志。注: 写日志不会抛出异常
 *
 * @param logType 日志类型
 * @param tableId 表ID
 * @param data 日志内容
 * @param size 日志内容大小
 * @return 日志LSN，若会话设置为不写日志则返回INVALID_LSN
 */
u64 Session::writeLog(LogType logType, u16 tableId, const byte *data, size_t size) {
	assert(m_inuse);
	if (!m_logging)
		return 0;
	assert_always(!TableDef::tableIdIsTemp(tableId));
	u16 txnId = m_txnStatus != TXN_NONE ? m_id: 0;
	m_lastLsn = m_txnLog->log(txnId, logType, tableId, data, size);
	return m_lastLsn;
}

/**
 * 写补偿日志。
 *
 * @param logType 日志类型
 * @param tableId 表ID
 * @param cpstForLsn 补偿日志是为了补偿这条(REDO)日志
 * @param data 日志内容
 * @param size 日志内容大小
 * @return 日志LSN
 */
u64 Session::writeCpstLog(LogType logType, u16 tableId, u64 cpstForLsn, const byte *data, size_t size) {
	assert(m_inuse);
	assert(m_txnStatus != TXN_NONE);
	assert(m_logging);
	assert_always(!TableDef::tableIdIsTemp(tableId));
	m_lastLsn = m_txnLog->logCpst(m_id, logType, tableId, data, size, cpstForLsn);
	return m_lastLsn;
}

/**
 * flush事务日志。
 *
 * @param lsnType 日志LSN
 */
void Session::flushLog(LsnType lsn, FlushSource fs) {
	m_txnLog->flush(lsn, fs);
}

/** 构造预更新日志。预更新日志中包含所有被更新属性的值及被更新索引属性的值
 *
 * @param tableDef 表定义
 * @param before 更新前的记录内容，为REC_REDUNDANT格式，当被MMS间隔写日志调用时为NULL
 * @param update 更新后的属性值，为REC_MYSQL格式，当被MMS间隔写日志调用时为REC_REDUNDANT格式
 * @param updateLob 是否更新大对象数据
 * @param indexPreImage 若不为NULL则包含更新涉及索引的所有属性有前像，为NULL表示不更新索引
 * @param size OUT，预更新日志大小
 * @return 预更新日志内容
 * @throw NtseException 日志超长
 */
byte* Session::constructPreUpdateLog(const TableDef *tableDef, const SubRecord *before, const SubRecord *update,
	bool updateLob, const SubRecord *indexPreImage, size_t *size) throw(NtseException) {
	assert(!before || before->m_format == REC_REDUNDANT);
	assert((before && update->m_format == REC_MYSQL) || (!before && update->m_format == REC_REDUNDANT));
	assert(!indexPreImage || indexPreImage->m_format == REC_REDUNDANT);

	size_t maxLogSize = RID_BYTES;
	u16 numLobs = 0;
	maxLogSize += RecordOper::getSubRecordSerializeSize(tableDef, update, false, false);
	if (indexPreImage)
		maxLogSize += RecordOper::getSubRecordSerializeSize(tableDef, indexPreImage, false, false);
	if (updateLob) {
		for (u16 i = 0; i < update->m_numCols; i++) {
			u16 cno = update->m_columns[i];
			ColumnDef *columnDef = tableDef->m_columns[cno];
			if (columnDef->isLob() && (!RecordOper::isNullR(tableDef, before, cno) || !RecordOper::isNullR(tableDef, update, cno))) {
				// 不包含更新前后都是NULL的情况
				numLobs++;
				maxLogSize += sizeof(LobId) + sizeof(cno);
				if (!RecordOper::isNullR(tableDef, update, cno))
					maxLogSize += sizeof(uint) + RecordOper::readLobSize(update->m_data, columnDef);
			}
		}
	}
	maxLogSize += sizeof(bool) + sizeof(bool);
	maxLogSize += sizeof(u16);
	byte *log = (byte *)getMemoryContext()->alloc(maxLogSize);
	Stream s(log, maxLogSize);
	// 头信息
	s.writeRid(update->m_rowId);
	s.write(indexPreImage != NULL);
	s.write(numLobs);
	// 主记录
	SubRecord updateR(REC_REDUNDANT, update->m_numCols, update->m_columns, update->m_data, update->m_size);
	assert(RecordOper::getSubRecordSerializeSize(tableDef, &updateR, false, true) < maxLogSize - s.getSize());
	RecordOper::serializeSubRecordMNR(&s, tableDef, &updateR, false, false); 
	// 被更新索引前像
	if (indexPreImage) {
		assert(RecordOper::getSubRecordSerializeSize(tableDef, indexPreImage, false, true) < maxLogSize - s.getSize());
		RecordOper::serializeSubRecordMNR(&s, tableDef, indexPreImage, false, false);
	}	
	// 大对象
	if (updateLob) {
		for (u16 i = 0; i < update->m_numCols; i++) {
			u16 cno = update->m_columns[i];
			ColumnDef *columnDef = tableDef->m_columns[cno];
			if (columnDef->isLob() && (!RecordOper::isNullR(tableDef, before, cno) || !RecordOper::isNullR(tableDef, update, cno))) {
				s.write(cno);
				LobId lid;
				if (!RecordOper::isNullR(tableDef, before, cno))
					lid = RecordOper::readLobId(before->m_data, columnDef);
				else
					lid = INVALID_ROW_ID;
				s.write(lid);
				if (!RecordOper::isNullR(tableDef, update, update->m_columns[i])) {
					s.write(RecordOper::readLobSize(update->m_data, columnDef));
					s.write(RecordOper::readLob(update->m_data, columnDef), RecordOper::readLobSize(update->m_data, columnDef));
				}
				if (s.getSize() > LogConfig::MAX_LOG_RECORD_SIZE)
					NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Too big update, can not log update larger than %ld bytes", LogConfig::MAX_LOG_RECORD_SIZE);
			}
		}
	}
	*size = s.getSize();
	return log;
}

/** 写预更新日志
 * @param tableDef 表定义
 * @param log 日志内容
 * @param size 日志大小
 */
void Session::writePreUpdateLog(const TableDef *tableDef, const byte *log, size_t size) {
	cacheLog(LOG_PRE_UPDATE, tableDef->m_id, log, size);
}

/**
 * 解析预更新日志。所有内容从MemoryContext中分配
 *
 * @param tableDef 表定义
 * @param log 日志
 * @return 更新内容
 */
PreUpdateLog* Session::parsePreUpdateLog(const TableDef *tableDef, const LogEntry *log) {
	assert(log->m_logType == LOG_PRE_UPDATE && log->m_tableId == tableDef->m_id);

	PreUpdateLog *update = (PreUpdateLog *)getMemoryContext()->alloc(sizeof(PreUpdateLog));
	update->m_numLobs = 0;
	update->m_lobs = NULL;
	update->m_lobIds = NULL;
	update->m_lobSizes = NULL;
	update->m_updateIndex = false;
	update->m_indexPreImage = NULL;

	Stream s(log->m_data, log->m_size);
	// 头信息
	RowId rid;
	s.readRid(&rid);
	s.read(&update->m_updateIndex);
	s.read(&update->m_numLobs);
	// 主记录
	update->m_subRec = RecordOper::unserializeSubRecordMNR(&s, tableDef, getMemoryContext());
	update->m_subRec->m_rowId = rid;
	// 索引前像
	if (update->m_updateIndex) {
		update->m_indexPreImage = RecordOper::unserializeSubRecordMNR(&s, tableDef, getMemoryContext());
		update->m_indexPreImage->m_rowId = rid;
	}
	// 大对象
	if (update->m_numLobs > 0) {
		update->m_lobCnos = (u16 *)getMemoryContext()->alloc(sizeof(u16) * update->m_numLobs);
		update->m_lobIds = (LobId *)getMemoryContext()->alloc(sizeof(LobId) * update->m_numLobs);
		update->m_lobs = (byte **)getMemoryContext()->alloc(sizeof(byte *) * update->m_numLobs);
		update->m_lobSizes = (uint *)getMemoryContext()->alloc(sizeof(uint) * update->m_numLobs);

		for (u16 currLob = 0; currLob < update->m_numLobs; currLob++) {
			u16 cno;
			s.read(&cno);
			update->m_lobCnos[currLob] = cno;
			ColumnDef *columnDef = tableDef->m_columns[cno];
			assert(columnDef->isLob());
			s.read(update->m_lobIds + currLob);
			if (!RecordOper::isNullR(tableDef, update->m_subRec, cno)) {
				s.read(update->m_lobSizes + currLob);
				update->m_lobs[currLob] = (byte *)getMemoryContext()->alloc(update->m_lobSizes[currLob]);
				s.readBytes(update->m_lobs[currLob], update->m_lobSizes[currLob]);
			} else {
				update->m_lobs[currLob] = NULL;
				update->m_lobSizes[currLob] = 0;
			}
			RecordOper::writeLobId(update->m_subRec->m_data, columnDef, update->m_lobIds[currLob]);
			RecordOper::writeLobSize(update->m_subRec->m_data, columnDef, 0);
		}
	}
	assert(s.getSize() == log->m_size);
	
	return update;
}

/** 得到预更新日志所操作的记录RID
 * @param log 预更新日志
 * @return RID
 */
RowId Session::getRidFromPreUpdateLog(const LogEntry *log) {
	assert(log->m_logType == LOG_PRE_UPDATE);
	Stream s(log->m_data, log->m_size);
	RowId rid;
	s.readRid(&rid);
	return rid;
}

/**
 * MMS更新脏记录到堆时写预更新日志。日志格式:
 *   RID: 占用RID_BYTES
 *   被更新属性数: u16
 *   各被更新属性号: u16 * 被更新属性数
 *   被更新属性内容: 使用REC_VARLEN的部分记录格式表示
 *
 * @param tableDef 表模式定义
 * @param update 更新内容，为REC_REDUNDANT格式
 */
void Session::writePreUpdateHeapLog(const TableDef *tableDef, const SubRecord *update) {
	assert(m_inuse);
	assert(update->m_format == REC_REDUNDANT);
	McSavepoint memorySave(m_memoryContext);
	SubRecord updateV(REC_VARLEN, update->m_numCols, update->m_columns, (byte *)m_memoryContext->alloc(tableDef->m_maxRecSize), tableDef->m_maxRecSize, update->m_rowId);
	RecordOper::convertSubRecordRV(tableDef, update, &updateV);

	size_t bufSize = updateV.m_size + sizeof(u16) + sizeof(u16) * update->m_numCols + RID_BYTES;
	byte *logBuf = (byte *)m_memoryContext->alloc(bufSize);
	Stream s(logBuf, bufSize);
	s.writeRid(update->m_rowId);
	s.write(update->m_numCols);
	for (u16 i = 0; i < update->m_numCols; i++)
		s.write(update->m_columns[i]);
	s.write(updateV.m_data, updateV.m_size);
	cacheLog(LOG_PRE_UPDATE_HEAP, tableDef->m_id, logBuf, s.getSize());
}

/**
 * 解析LOG_PRE_UPDATE_HEAP日志
 *
 * @param tableDef 表模式定义
 * @param log 日志
 * @return 被更新属性内容，存储为REC_REDUNDANT格式。内存从MemoryContext分配
 */
SubRecord* Session::parsePreUpdateHeapLog(const TableDef *tableDef, const LogEntry *log) {
	assert(m_inuse);
	Stream s(log->m_data, log->m_size);
	
	SubRecord *ret = (SubRecord *)m_memoryContext->alloc(sizeof(SubRecord));
	ret->m_data = (byte *)m_memoryContext->alloc(tableDef->m_maxRecSize);
	ret->m_size = tableDef->m_maxRecSize;
	ret->m_format = REC_REDUNDANT;

	s.readRid(&ret->m_rowId);
	s.read(&ret->m_numCols);
	ret->m_columns = (u16 *)m_memoryContext->alloc(sizeof(u16) * ret->m_numCols);
	for (u16 i = 0; i < ret->m_numCols; i++)
		s.read(ret->m_columns + i);

	SubRecord updateV;
	updateV.m_data = log->m_data + s.getSize();
	updateV.m_size = (uint)(log->m_size - s.getSize());
	updateV.m_format = REC_VARLEN;
	updateV.m_numCols = ret->m_numCols;
	updateV.m_columns = ret->m_columns;
	updateV.m_rowId = ret->m_rowId;

	RecordOper::convertSubRecordVR(tableDef, &updateV, ret);

	return ret;
}

/**
 * 写事务开始日志
 *
 * @param tableId 事务操作的表的ID
 * @param type 事务类型
 * @return 日志LSN
 */
u64 Session::writeTxnStartLog(u16 tableId, TxnType type) {
	byte bType = (byte)type;
	return m_txnLog->log(m_id, LOG_TXN_START, tableId, &bType, sizeof(bType));
}

/**
 * 解析事务开始日志
 *
 * @param log 日志内容
 * @return 事务类型
 */
TxnType Session::parseTxnStartLog(const LogEntry *log) {
	assert(log->m_size == sizeof(byte));
	assert(log->m_logType == LOG_TXN_START);

	return (TxnType )(*(log->m_data));
}

/**
 * 写事务结束日志
 *
 * @param commit 事务是否成功
 * @return 日志LSN。若会话设置为不写日志则返回INVALID_LSN
 */
u64 Session::writeTxnEndLog(bool commit) {
	if (!m_logging)
		return INVALID_LSN;
	byte succ = (byte)commit;
	return m_txnLog->log(m_id, LOG_TXN_END, m_tableId, &succ, sizeof(succ));
}

/**
 * 解析事务结束日志
 *
 * @param log 日志内容
 * @return 事务是否提交
 */
bool Session::parseTxnEndLog(const LogEntry *log) {
	assert(log->m_size == sizeof(byte));
	assert(log->m_logType == LOG_TXN_END);

	return *(log->m_data) != 0;
}

/**
 * 最后一条日志的LSN
 *
 * @return 最后一条日志的LSN
 */
u64 Session::getLastLsn() const {
	assert(m_inuse);
	return m_lastLsn;
}

/**
 * 获得当前活跃事务的起始LSN
 *
 * @return 当前活跃事务的起始LSN，若没有活跃事务则返回INVALID_LSN
 */
u64 Session::getTxnStartLsn() const {
	assert(m_inuse);
	return m_txnStartLsn;
}

/**
 * 获得决定事务效果是否持久化的日志LSN。即若此日志被写出，则事务在恢复时会被REDO，
 * 其效果会持久化。否则，事务在恢复时会被回滚。
 * 本函数应该在事务结束之后调用，返回刚刚完成的事务中决定其效果是否持久化的日志LSN。
 * @pre 不在事务中
 *
 * @return 获得决定事务效果是否持久化的日志LSN
 */
u64 Session::getTxnDurableLsn() const {
	assert(m_inuse && m_txnStatus == TXN_NONE);
	return m_txnDurableLsn;
}

/**
 * 设置决定事务效果是否持久化的日志LSN。
 * @pre 在事务中
 *
 * @param lsn 日志LSN 
 */
void Session::setTxnDurableLsn(u64 lsn) {
	assert(m_inuse && m_txnStatus != TXN_NONE && m_txnDurableLsn == INVALID_LSN);
	m_txnDurableLsn = lsn;
}

/**
 * 获取最近一次事务是否提交。
 * @pre 事务已经结束，当前不在事务中
 *
 * @return 最近一次事务是否提交
 */
bool Session::isTxnCommit() const {
	assert(m_inuse && m_txnStatus == TXN_NONE);
	return m_isTxnCommit;
}

/**
 * 尝试锁定一行
 *
 * @param tableId 表ID
 * @param rid 要锁定行的RID
 * @param lockMode 锁模式
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 * @return 行锁句柄，若为NULL表示没有锁成功
 */
RowLockHandle* Session::tryLockRow(u16 tableId, RowId rowId, LockMode lockMode, const char *sourceFile, uint line) {
	assert(m_inuse);
	if (!m_rowLockMgr->tryLock(m_id, getGlobalRid(tableId, rowId), lockMode))
		return NULL;
	ftrace(ts.rl, tout << tableId << rid(rowId) << lockMode << sourceFile << line);
	return allocRowLockHandle(tableId, rowId, lockMode, sourceFile, line);
}

/**
 * 锁定一行
 *
 * @param tableId 表ID
 * @param rowId 要锁定行的RID
 * @param lockMode 锁模式
 * @param sourceFile 调用这一函数代码所在源文件
 * @param line 调用这一函数代码行号
 * @return 行锁句柄，一定可以成功加锁，不为NULL
 */
RowLockHandle* Session::lockRow(u16 tableId, RowId rowId, LockMode lockMode, const char *sourceFile, uint line) {
	assert(m_inuse);
	m_rowLockMgr->lock(m_id, getGlobalRid(tableId, rowId), lockMode, -1);
	ftrace(ts.rl, tout << tableId << rid(rowId) << lockMode << sourceFile << line);
	return allocRowLockHandle(tableId, rowId, lockMode, sourceFile, line);
}

/**
 * 判断指定的行是否被当前会话以指定的模式锁定
 *
 * @param tableId 表ID
 * @param rid 要锁定行的RID
 * @param lockMode 锁模式
 * @return 指定的行是否被当前会话以指定的模式锁定
 */
bool Session::isRowLocked(u16 tableId, RowId rid, LockMode lockMode) {
	assert(m_inuse);
	if (lockMode == Exclusived) {
		return m_rowLockMgr->isExclusivedLocked(m_id, getGlobalRid(tableId, rid));
	} else {
		bool isLocked = m_rowLockMgr->isSharedLocked(getGlobalRid(tableId, rid));
		if (isLocked) {
			DLink<RowLockHandleElem*> *head = m_rlHandleInUsed.getHeader();
			DLink<RowLockHandleElem*> *it = head->getNext();
			while (it != head) {
				const RowLockHandle *rlh = it->get();
				if (rlh->getRid() == rid && rlh->getTableId() == tableId
					&& rlh->getLockMode() == Shared)
					return true;
				it = it->getNext();
			}
			return false;
		}
		return false;
	}
}

/**
 * 解除对一行的锁
 *
 * @param handle 行锁句柄
 */
void Session::unlockRow(RowLockHandle **handle) {
	assert(m_inuse);
	assert((*handle)->m_valid);
	ftrace(ts.rl, tout << (*handle)->m_tableId << rid((*handle)->m_rid) << (*handle)->m_mode);
	m_rowLockMgr->unlock(getGlobalRid((*handle)->m_tableId, (*handle)->m_rid), (*handle)->m_mode);
	freeRowLockHandle(*handle);
	*handle = NULL;
}

/**
 * 打印使用中的页面缓存句柄信息到标准输出
 * 注: 本函数在访问各会话信息时无法实现同步控制，只应在调试时使用
 */
void Session::dumpBufferPageHandles() const {
	printf("== SESSION[%d]: %s\n", m_id, m_name);
	for (uint i = 0; i < MAX_PAGE_HANDLES; i++) {
		if (m_bpHandles[i].m_valid) {
			const BufferPageHandle *handle = m_bpHandles + i;
			printf("  BUFFER PAGE HANDLE\n");
			printf("    PATH: %s\n", handle->m_path);
			printf("    PAGE: "I64FORMAT"u\n", handle->m_pageId);
			printf("    MODE: %s\n", RWLock::getModeStr(handle->m_lockMode));
			printf("    PIN: %d\n", handle->m_pinned);
			printf("    CODE: %s:%u\n", handle->m_file, handle->m_line);
		}
	}
}

/**
 * 打印使用中的行锁句柄信息到标准输出
 * 注: 本函数在访问各会话信息时无法实现同步控制，只应在调试时使用
 */
void Session::dumpRowLockHandles() {
	printf("== SESSION[%d]: %s\n", m_id, m_name);

	DLink<ntse::RowLockHandleElem*> *head = m_rlHandleInUsed.getHeader();
	DLink<ntse::RowLockHandleElem*> *it = head->getNext();
	while (it != head) {		
		RowLockHandle *handle = it->get();
		if (handle->m_valid) {
			printf("  ROW LOCK HANDLE\n");
			printf("	TABLE: %d\n", handle->m_tableId);
			printf("	RID: "I64FORMAT"u\n", handle->m_rid);
			printf("	MODE: %s\n", RWLock::getModeStr(handle->m_mode));
			printf("	CODE: %s:%u\n", handle->m_file, handle->m_line);
		}
		it = it->getNext();
	}
}

/** 增加一个锁对象
 * @param lock 锁对象
 */
void Session::addLock(const Lock *lock) {
	assert(m_inuse);
	m_locks.push_back(lock);
}

/** 移除一个锁对象
 * @param lock 锁对象
 */
void Session::removeLock(const Lock *lock) {
	assert(m_inuse);
	vector<const Lock *>::iterator it = std::find(m_locks.begin(), m_locks.end(), lock);
	assert(it != m_locks.end());
	m_locks.erase(it);
}

/** 重置一个会话的状态，检查资源是否泄漏
 * 重置以下内容: memoryContext，lobContext, userData
 */
void Session::reset() {
	assert(m_inuse);
	assert(m_txnStatus == TXN_NONE);
	assert(m_rlHandleInUsed.getSize() == 0);
	assert(m_ukLockList.getSize() == 0);
	checkBufferPages();
	checkRowLocks();
	checkLocks();
	m_memoryContext->reset();
	m_lobContext->reset();
	m_logging = true;
	m_canceled = false;
	m_rlHandlePool.clear();
	m_ukLockHandlePool.clear(); 
#ifdef TNT_ENGINE
	m_trx = NULL;
#endif
}

/**
 * 检查缓存页资源是否泄漏，泄漏时自动释放并且打印错误信息
 */
void Session::checkBufferPages() {
	for (uint i = 0; i < MAX_PAGE_HANDLES; i++) {
		if (m_bpHandles[i].m_valid) {
			if (m_bpHandles[i].m_pinned) {
				if (m_bpHandles[i].m_lockMode != None)
					m_buffer->releasePage(this, m_bpHandles[i].m_page, m_bpHandles[i].m_lockMode);
				else
					m_buffer->unpinPage(m_bpHandles[i].m_page);
			} else
				m_buffer->unlockPage(m_id, m_bpHandles[i].m_page, m_bpHandles[i].m_lockMode);
			m_db->getSyslog()->log(EL_ERROR, "Buffer page leaked, file: %s, line: %d, mode: %s",
				m_bpHandles[i].m_file, m_bpHandles[i].m_line,
				RWLock::getModeStr(m_bpHandles[i].m_lockMode));
			// 需要将valid标志设为false,参见QA平台Bug27142
			m_bpHandles[i].m_valid = false;
		}
	}
}

/**
 * 检查行锁资源是否泄漏，泄漏时自动释放并且打印错误信息
 */
void Session::checkRowLocks() {
	while (m_rlHandleInUsed.getSize() > 0) {
		DLink<ntse::RowLockHandleElem*> *link = m_rlHandleInUsed.removeLast();
		RowLockHandle *rlHandle = link->get();
		if (rlHandle->m_valid) {
			m_rowLockMgr->unlock(getGlobalRid(rlHandle->m_tableId, rlHandle->m_rid), rlHandle->m_mode);
			m_db->getSyslog()->log(EL_ERROR, "Row lock leaked, file: %s, line: %d, mode: %s",
				rlHandle->m_file, rlHandle->m_line,
				(rlHandle->m_mode == Shared) ? "Shared" : "Exclusived");
			freeRowLockHandle(rlHandle);
			// 需要将valid标志设为false,参见QA平台Bug27142
			rlHandle->m_valid = false;
		}
	}
}

/**
 * 检查索引相关的锁是否都释放结束
 */
void Session::checkIdxLocks() {
	if (!m_lockedIdxObjects.empty()) {
		map<u64, PageId>::iterator iter = m_lockedIdxObjects.begin();
		while (iter != m_lockedIdxObjects.end()) {
			u64 objectId = (*iter).second;
			m_db->getSyslog()->log(EL_ERROR, "Index objects leaked, objectId: %ld", objectId);
		}
		m_lockedIdxObjects.clear();
		m_idxLockMgr->unlockAll(m_id);
	}
}

/** 检查各类粗粒度锁是否泄漏 */
void Session::checkLocks() {
	if (!m_locks.empty()) {
		m_db->getSyslog()->log(EL_PANIC, "Unfreed locks: %s[%s:%u]", m_locks[0]->m_name, m_locks[0]->m_file, m_locks[0]->m_line);
		assert(!"Unfreed locks");
	}
}

BufferPageHandle* Session::allocBufferPageHandle(BufferPageHdr *page, const char *path, u64 pageId, LockMode lockMode, bool pinned, const char *file, uint line) {
	for (uint i = 0; i < MAX_PAGE_HANDLES; i++) {
		if (!m_bpHandles[i].m_valid) {
			BufferPageHandle *handle = m_bpHandles + i;
			handle->m_valid = true;
			handle->m_page = page;
			handle->m_path = path;
			handle->m_pageId = pageId;
			handle->m_lockMode = lockMode;
			handle->m_pinned = pinned;
			handle->m_file = file;
			handle->m_line = line;
			return handle;
		}
	}
	assert(!"All page handles is in use");
	return NULL;
}

void Session::freeBufferPageHandle(BufferPageHandle *handle) {
	assert(handle >= m_bpHandles && handle < m_bpHandles + MAX_PAGE_HANDLES);
	handle->m_valid = false;
	handle->m_file = NULL;
	handle->m_line = 0;
	handle->m_page = NULL;
	handle->m_lockMode = None;
	handle->m_pinned = false;
}

RowLockHandle* Session::allocRowLockHandle(u16 tableId, RowId rid, LockMode lockMode, const char *file, uint line) {
	size_t poolId = m_rlHandlePool.alloc();
	RowLockHandleElem *handleElem = &m_rlHandlePool[poolId];
	handleElem->relatedWithList(&m_rlHandleInUsed);

	handleElem->m_valid = true;
	handleElem->m_tableId = tableId;
	handleElem->m_rid = rid;
	handleElem->m_mode = lockMode;
	handleElem->m_file = file;
	handleElem->m_line = line;
	handleElem->m_poolId = poolId;

	return handleElem;
}

void Session::freeRowLockHandle(RowLockHandle *handle) {
	if (handle->m_valid) {
		handle->m_valid = false;
		handle->m_file = NULL;
		handle->m_line = 0;
		handle->m_tableId = 0;
		handle->m_rid = 0;
		handle->m_mode = None;

		RowLockHandleElem *handleElem = (RowLockHandleElem*)handle;
		handleElem->excludeFromList();
		m_rlHandlePool.free(handleElem->m_poolId);
	}
}

/**
 * 对指定索引对象尝试加锁
 * @param objectId	索引对象ID
 * @return 加锁成功true，否则false
 */
bool Session::tryLockIdxObject(u64 objectId) {
	if (m_idxLockMgr->tryLock(m_id, objectId)) {
		m_lockedIdxObjects.insert(map<u64, PageId>::value_type(m_token++, objectId));
		nftrace(ts.irl, tout << m_id << " tl " << objectId;);
		return true;
	}
	return false;
}

/**
 * 对指定索引对象 加锁
 * @param objectId	索引对象ID
 * @return 加锁成功true，否则false
 */
bool Session::lockIdxObject(u64 objectId) {
	if (m_idxLockMgr->lock(m_id, objectId)) {
		m_lockedIdxObjects.insert(multimap<u64, PageId>::value_type(m_token++, objectId));
		nftrace(ts.irl, tout << m_id << " l " << objectId;);
		return true;
	}
	return false;
}

/**
 * 释放某个索引对象锁
 * @param objectId	索引对象ID
 * @param token		释放的锁记号必须大于等于token，默认值从锁表存在开始
 * @return 放锁成功true，否则false
 */
bool Session::unlockIdxObject(u64 objectId, u64 token) {
	bool unlocked = false;

	map<u64, PageId>::iterator iter = m_lockedIdxObjects.lower_bound(token);
	// 只要token使用恰当，while遍历的次数应该不多，除非释放某个对象的所有锁只能从头开始遍历
	while (iter != m_lockedIdxObjects.end()) {
		if ((*iter).second == objectId) {
			m_idxLockMgr->unlock(m_id, objectId);
			m_lockedIdxObjects.erase(iter++);
			unlocked = true;
			nftrace(ts.irl, tout << m_id << " ul " << objectId;);
		} else 
			++iter;
	}

	return unlocked;
}

/**
 * 释放当前会话加上的所有锁资源
 * @return 释放成功true，否则false
 */
bool Session::unlockIdxAllObjects() {
	nftrace(ts.irl, tout << m_id <<  " unlock all locks";);
	bool succ = m_idxLockMgr->unlockAll(m_id);
	if (succ)
		m_lockedIdxObjects.clear();
	return succ;
}

/**
 * 判断当前会话是否持有某个索引对象锁资源
 * @param objectId	索引对象ID
 * @return true表示持有，否则false
 */
bool Session::isLocked(u64 objectId) {
	bool locked = false;
	map<u64, PageId>::iterator iter = m_lockedIdxObjects.begin();
	while (iter != m_lockedIdxObjects.end()) {
		if ((*iter).second == objectId) {
			locked = true;
			break;
		}
		iter++;
	}
	assert(m_idxLockMgr->isLocked(m_id, objectId) == locked);
	return locked;
}


/**
 * 得到当前的加锁操作序列号
 * @return 加锁操作序列号
 */
u64 Session::getToken() {
	return m_token;
}


/**
 * 释放那些加锁操作序列号在指定值之后的锁
 * @param token	指定的加锁序列号
 * @return 返回放锁成功与否
 */
bool Session::unlockIdxObjects(u64 token) {
	map<u64, PageId>::iterator iter = m_lockedIdxObjects.lower_bound(token);
	while (iter != m_lockedIdxObjects.end()) {
		u64 objectId = (*iter).second;
		m_idxLockMgr->unlock(m_id, objectId);
		nftrace(ts.irl, tout << m_id << " ul " << objectId;);
		++iter;
	}
	m_lockedIdxObjects.erase(m_lockedIdxObjects.lower_bound(token), m_lockedIdxObjects.end());

	return true;
}

/**
 * 返回会话对象当前是否持有锁资源
 */
bool Session::hasLocks() {
	return !m_lockedIdxObjects.empty();
}


/**
 * 得到指定对象当前是由哪个会话持有，如果没有被任何会话持有，返回-1
 * @param objectId	对象号
 * @return 持有对象锁的会话编号，不被会话持有返回-1
 */
u64 Session::whoIsHolding(u64 objectId) {
	return m_idxLockMgr->whoIsHolding(objectId);
}

bool Session::tryLockUniqueKey(UKLockManager *uniqueKeyMgr, u64 keyChecksum, const char *file, uint line) {
	assert(NULL != uniqueKeyMgr);
	if (uniqueKeyMgr->tryLock(m_id, keyChecksum)) {
		UKLockHandle* ukLockHdl = allocNewUKLockHandle();
		assert(NULL != ukLockHdl);
		ukLockHdl->m_lockMgr = uniqueKeyMgr;
		ukLockHdl->m_key = keyChecksum;
		ukLockHdl->m_file = file;
		ukLockHdl->m_line = line;
		ukLockHdl->relatedWithList(&m_ukLockList);
		return true;
	}
	return false;
}

bool Session::lockUniqueKey(UKLockManager *uniqueKeyMgr, u64 keyChecksum, const char *file, uint line) {
	assert(NULL != uniqueKeyMgr);
	if (uniqueKeyMgr->lock(m_id, keyChecksum)) {
		UKLockHandle* ukLockHdl = allocNewUKLockHandle();
		assert(NULL != ukLockHdl);
		ukLockHdl->m_lockMgr = uniqueKeyMgr;
		ukLockHdl->m_key = keyChecksum;
		ukLockHdl->m_file = file;
		ukLockHdl->m_line = line;
		ukLockHdl->relatedWithList(&m_ukLockList);
		return true;
	}
	return false;
}

void Session::unlockUniqueKey(UKLockHandle* ukLockHdl) {
	assert(NULL != ukLockHdl);
	UKLockManager *uniqueKeyMgr = ukLockHdl->m_lockMgr;
	uniqueKeyMgr->unlock(ukLockHdl->m_key);
	ukLockHdl->excludeFromList();
	freeUKLockHandle(ukLockHdl);
}

void Session::unlockAllUniqueKey() {
	DLink<UKLockHandle*> *node = NULL;
	while ((node = m_ukLockList.removeLast()) != NULL) {
		UKLockHandle *lockHdl = node->get();
		UKLockManager *uniqueKeyMgr = lockHdl->m_lockMgr;
		uniqueKeyMgr->unlock(lockHdl->m_key);
		lockHdl->excludeFromList();
		freeUKLockHandle(lockHdl);
	};
}

UKLockHandle* Session::allocNewUKLockHandle() {
	size_t poolId = m_ukLockHandlePool.alloc();
	UKLockHandle *handle = &m_ukLockHandlePool[poolId];
	handle->m_poolId = poolId;

	return handle;
}

void Session::freeUKLockHandle(UKLockHandle *ukLockHdl) {
	size_t poolId = ukLockHdl->m_poolId;
	m_ukLockHandlePool.free(poolId);
}

///////////////////////////////////////////////////////////////////////////////
// 数据库连接 ///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
#ifdef NTSE_UNIT_TEST
/** 创建仅用于页面缓存测试所用的连接对象 */
Connection::Connection(): m_stat(NULL), m_lock("Connection::m_lock", __FILE__, __LINE__) {
	m_config.m_accurateTblScan = true;
	m_internal = false;
	m_userData = NULL;
	m_cpLink.set(this);
	m_durationBegin = System::fastTime();
	m_name = NULL;
	m_status = NULL;
	m_thdId = 0;
	m_trx = false;
}
#endif

Connection::Connection(const LocalConfig *globalConfig, OpStat *globalStat, bool internal, const char *name):
	m_config(*globalConfig), m_stat(globalStat), m_lock("Connection::m_lock", __FILE__, __LINE__) {
	m_internal = internal;
	m_cpLink.set(this);
	m_userData = NULL;
	m_durationBegin = System::fastTime();
	m_name = System::strdup(name);
	m_status = NULL;
	m_thdId = 0;
#ifdef NTSE_UNIT_TEST
	m_trx = false;
#else
	m_trx = true;
#endif
}

bool Connection::isInternal() const {
	return m_internal;
}

/**
 * 获取用户自定义数据
 *
 * @return 用户自定义数据
 */
void* Connection::getUserData() const {
	return m_userData;
}

/**
 * 设置用户自定义数据
 * 注: 用户自定义数据在连接释放的时候会被自动重置为NULL
 *
 * @param dat 用户自定义数据
 */
void Connection::setUserData(void *dat) {
	m_userData = dat;
}

/**
 * 得到连接ID
 *
 * @return 连接ID，一定>0
 */
uint Connection::getId() const {
	return m_id;
}

/**
 * 得到连接保持当前状态的时间
 *
 * @return 连接保持当前状态的时间，单位秒
 */
uint Connection::getDuration() const {
	return System::fastTime() - m_durationBegin;
}

/**
 * 设置状态信息
 * @param status 状态信息，内存使用约定: 直接引用
 */
void Connection::setStatus(const char *status) {
	LOCK(&m_lock);
	m_status = status;
	m_durationBegin = System::fastTime();
	UNLOCK(&m_lock);
}

/** 获得连接名称
 * @return 连接名称，可能是NULL
 */
const char* Connection::getName() const {
	return m_name;
}


///////////////////////////////////////////////////////////////////////////////
// BgTask /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** 构造函数
 * @param db 数据库，可能为NULL
 * @param name 任务线程名称
 * @param interval 任务执行周期，单位毫秒
 * @param alwaysHoldSession 是否始终持有会话，若为false则每次运行时取得会话
 * @param allocSessionTimeoutMs 分配会话超时时间，<0表示不超时，0表示马上超时，
 *   >0表示毫秒数指定的超时时间。仅在alwaysHoldSession为false时才有效
 * @param runInRecover 恢复过程中是否也需要运行，为保证可恢复性，只有不会产生日志的的后台
 *   线程才可以设为true
 */
BgTask::BgTask(Database *db, const char *name, uint interval, bool alwaysHoldSession, int allocSessionTimeoutMs, bool runInRecover): Task(name, interval) {
	m_db = db;
	m_conn = NULL;
	m_session = NULL;
	m_alwaysHoldSession = alwaysHoldSession;
	m_allocSessionTimeoutMs = allocSessionTimeoutMs;
	m_runInRecover = runInRecover;
	m_setUpFinished = false;
	if (db)
		db->registerBgTask(this);
}


/** 析构函数 */
BgTask::~BgTask() {
	if (m_db)
		m_db->unregisterBgTask(this);
	assert(!m_session && !m_conn);
}

/** @see Task::setUp */
void BgTask::setUp() {
#ifdef NTSE_PROFILE
	g_tlsProfileInfo.prepareProfile(getId(), BG_THREAD, g_profiler.getThreadProfileAutorun());
#endif
	if (m_db) {
		while (true) {
			if (m_db->getStat() == DB_CLOSING || m_stopped) {
				m_setUpFinished = true;
				return;
			}
			if (m_db->getStat() == DB_RUNNING)
				break;
			if (m_db->getStat() == DB_MOUNTED && m_runInRecover)
				break;
			msleep(100);
		}	
		// TODO 后台线程需要进行管理，数据库关闭之前需要停止各后台线程
		// 否则在数据库关闭过程中，某些对象已经被析构的情况下可能出错
		m_conn = m_db->getConnection(true, m_name);
		m_conn->setThdID(getId());
		m_conn->setStatus("Idle");
		if (m_alwaysHoldSession) {
			m_session = m_db->getSessionManager()->allocSession(m_name, m_conn);
		}
	}
	m_setUpFinished = true;
}

/** @see Task::tearDown */
void BgTask::tearDown() {
	if (m_conn) {
		if (m_session) {
			m_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
		}	
		m_db->freeConnection(m_conn);
		m_conn = NULL;
	}
#ifdef NTSE_PROFILE
	g_tlsProfileInfo.endProfile();
#endif
}

/** @see Thread::run */
void BgTask::run() {
	if (m_conn) {
		if (!m_alwaysHoldSession) {
			SYNCHERE(SP_BGTASK_GET_SESSION_START);
			m_session = m_db->getSessionManager()->allocSession(m_name, m_conn, m_allocSessionTimeoutMs);
			SYNCHERE(SP_BGTASK_GET_SESSION_END);
			if (!m_session)
				return;
		}
		m_conn->setStatus("Busy");
	}
	runIt();
	if (m_conn) {
		m_session->incOpStat(OPS_OPER);
		if (!m_alwaysHoldSession) {
			m_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
		} else
			m_session->reset();
		m_conn->setStatus("Idle");
	}
}

/** 检查setUp是否完成
 * @return setUp是否完成
 */
bool BgTask::setUpFinished() {
	return m_setUpFinished;
}

/** 检查 恢复过程中是否也需要运行
 * @return 恢复过程中是否也需要运行
 */
bool BgTask::shouldRunInRecover() {
	return m_runInRecover;
}
}
