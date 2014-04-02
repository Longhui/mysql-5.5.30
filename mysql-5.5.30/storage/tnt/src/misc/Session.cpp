/**
 * �Ự����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
 * ����һ���Ự������
 *
 * @param db ���ݿ�
 * @param maxSessions ���Ự��
 * @param internalSessions �ڲ��Ự��
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
 * ����һ���Ự
 * 
 * @param name �Ự���ƣ�������ΪNULL��ֱ�����ò�����
 * @param conn ���ݿ����ӣ�������ΪNULL
 * @param timeoutMs ��ʱʱ�䣬<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��>0��ʾ������ָ���ĳ�ʱʱ��
 * @return �ɹ����ػỰ���������лỰ����ʹ���з���NULL�����retryΪtrue�򲻿��ܷ���NULL
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

/** ���Է���Ự
 * @param name �Ự����
 * @param conn ����
 * @param high �Ͻ�
 * @param low �½�
 * @return ���䵽���ػỰ�����䲻������NULL
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
 * ֱ�ӻ�ȡָ��ID�ĻỰ���ڻָ�ʱʹ�ã�
 * 
 * @param name �Ự���ƣ�������ΪNULL��ֱ�����ò�����
 * @param id �ỰID
 * @param conn ���ݿ����ӣ�������ΪNULL
 * @return �ɹ����ػỰ������ָ���Ự��ʹ���з���NULL
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
 * �ͷ�һ���Ự
 *
 * @param Ҫ�ͷŵĻỰ����
 */
void SessionManager::freeSession(Session *session) {
	session->reset();

	LOCK(&m_lock);
	assert(session->m_inuse);
	session->m_inuse = false;
	UNLOCK(&m_lock);
}

/**
 * ������Ự��
 *
 * @return ���Ự��
 */
u16 SessionManager::getMaxSessions() {
	return m_maxSessions;
}

/**
 * ��ӡʹ���е�ҳ�滺������Ϣ����׼���
 * ע: �������ڷ��ʸ��Ự��Ϣʱ�޷�ʵ��ͬ�����ƣ�ֻӦ�ڵ���ʱʹ��
 */
void SessionManager::dumpBufferPageHandles() {
	for (int i = 0; i < m_maxSessions; i++) {
		if (m_sessions[i]->m_inuse)
			m_sessions[i]->dumpBufferPageHandles();
	}
}

/**
 * ��ӡʹ���е�������������Ϣ����׼���
 * ע: �������ڷ��ʸ��Ự��Ϣʱ�޷�ʵ��ͬ�����ƣ�ֻӦ�ڵ���ʱʹ��
 */
void SessionManager::dumpRowLockHandles() {
	for (int i = 0; i < m_maxSessions; i++) {
		if (m_sessions[i]->m_inuse)
			m_sessions[i]->dumpRowLockHandles();
	}
}

/**
 * ������л�Ծ������ʼLSN����Сֵ
 *
 * @return ���л�Ծ������ʼLSN����Сֵ����û�л�Ծ�����򷵻�INVALID_LSN
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
 * �õ�Ŀǰʹ���еĻỰ����
 *
 * @return ʹ���еĻỰ����
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

/** �Ự����ɨ���� */
struct SesScanHandle {
	u16		m_curPos;	/** ��ǰλ�� */

	SesScanHandle() {
		m_curPos = 0;
	}
};

/**
 * ����ʹ���еĻỰ
 *
 * @return ɨ����
 */
SesScanHandle* SessionManager::scanSessions() {
	LOCK(&m_lock);
	return new SesScanHandle();
}

/**
 * �õ���һ��ʹ���еĻỰ
 * @post �Ự����һ��getNext��endScan֮ǰ���ᱻ�ͷ�
 *
 * @param h ɨ����
 * @return ��һ��ʹ���еĻỰ��û���򷵻�NULL
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
 * �����Ự����ɨ��
 * @post ɨ���������Ѿ�������
 *
 * @param h ɨ����
 */
void SessionManager::endScan(SesScanHandle *h) {
	UNLOCK(&m_lock);
	delete h;
}

/**
 * ����һ�����ӣ������ӳ��л�ȡ��
 *
 * @param internal �Ƿ�Ϊ�ڲ����ӣ��ڲ�����ָ��Ϊ��ִ�����û�����û��
 *   ֱ�ӹ�ϵ�Ĳ���������㡢MMSˢ���»���ȣ�����ȡ������
 * @param config ����˽��״̬
 * @param globalStat ȫ�ֲ���ͳ��
 * @param name ��������
 * @return ���ݿ�����
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
 * �ͷ�һ�����ӣ������ӳأ�
 * @pre Ҫ�ͷŵ�����connΪʹ���е�����
 *
 * @param ���ݿ�����
 */
void SessionManager::freeConnection(Connection *conn) {
	assert(conn);

	LOCK(&m_lock);
	conn->m_cpLink.unLink();
	UNLOCK(&m_lock);

	delete conn;
}

/** ���ӱ���ɨ���� */
struct ConnScanHandle {
	DLink<Connection *>	*m_curPos;	/** ��ǰ���� */
};

/**
 * ����ʹ���е����Ӷ���
 * ע������������ϵͳ�����ܷ������ͷ�����
 *
 */
ConnScanHandle* SessionManager::scanConnections() {
	LOCK(&m_lock);
	ConnScanHandle *h = new ConnScanHandle();
	h->m_curPos = m_activeConns->getHeader()->getNext();
	return h;
}

/**
 * �õ���һ������
 *
 * @param h ɨ����
 * @return ��һ�����ӣ���û���򷵻�NULL
 */
const Connection* SessionManager::getNext(ConnScanHandle *h) {
	if (h->m_curPos == m_activeConns->getHeader())
		return NULL;
	Connection *conn = h->m_curPos->get();
	h->m_curPos = h->m_curPos->getNext();
	return conn;
}

/**
 * �������ӱ���
 * @post ɨ���������Ѿ�������
 *
 * @param h ɨ����
 */
void SessionManager::endScan(ConnScanHandle *h) {
	delete h;
	UNLOCK(&m_lock);
}

///////////////////////////////////////////////////////////////////////////////
// Session ////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////


/**
 * ���캯��
 *
 * @param id �ỰID
 * @param db ���ݿ�
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
 * ����һ��ҳ�滺�����ʱ���õ�����Ự��������NTSE_UNIT_TEST���������ſ���
 *
 * @param conn ����
 * @param buffer ҳ�滺��
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
 * ����һ�����棬pin��������ҳ
 *
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
 * @param dbObjStats ���ݶ���״̬
 *
 * @return ����ҳ���
 */
BufferPageHandle* Session::newPage( File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats ) {
	assert(m_inuse);
	BufferPageHdr *page = m_buffer->newPage(this, file, pageType, pageId, lockMode, dbObjStats);
	return allocBufferPageHandle(page, file->getPath(), pageId, lockMode, true, sourceFile, line);
}

/**
 * ��ȡһ��ҳ��pin��������ҳ
 *
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
 * @param dbObjStats ���ݶ���״̬
 * @guess Ҫ��ȡ��ҳ���п����������ַ
 *
 * @return ����ҳ���
 */
BufferPageHandle* Session::getPage( File *file, PageType pageType, u64 pageId, LockMode lockMode, const char *sourceFile, uint line, DBObjStats* dbObjStats, BufferPageHdr *guess /*= NULL*/ ) {
	assert(m_inuse);
	BufferPageHdr *page = m_buffer->getPage(this, file, pageType, pageId, lockMode, dbObjStats, guess);
	return allocBufferPageHandle(page, file->getPath(), pageId, lockMode, true, sourceFile, line);
}

/**
 * ��ͼ��ȡһ��ҳ��pin����������ҳ
 *
 * @param file �ļ�
 * @param pageType ҳ������
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
 * @param dbObjStats ���ݶ���״̬
 *
 * @return ����ҳ�������ΪNULL��ʾδ�����ɹ�
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
 * �ͷ�һ��ҳ�������ͷŸ�ҳ������pin
 *
 * @param handle ����ҳ���
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
 * ����һ��ҳ
 *
 * @param page ����ҳ
 * @param file �ļ�
 * @param pageId ҳ��
 * @param lockMode ��ģʽ
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
 * @return ����ҳ���
 */
BufferPageHandle* Session::lockPage(BufferPageHdr *page, File *file, u64 pageId, LockMode lockMode, const char *sourceFile, uint line) {
	assert(m_inuse);
	m_buffer->lockPage(m_id, page, lockMode, true);
	return allocBufferPageHandle(page, file->getPath(), pageId, lockMode, false, sourceFile, line);
}

/**
 * ��֪һ������ҳ���ʱ����������һ��ҳ
 * @pre ������Ӧ���ڵ�ǰ�Ự�г��жԸ�ҳ��pin
 *
 * @param handle ����ҳ���
 * @param lockMode ��ģʽ
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
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
 * �ͷ�һ��ҳ�ϵ����������ͷ�pin
 * ������ҳ�����ͨ��lockPage��ã�ֻ������������pin����ñ�����֮��
 * ����ҳ����Ѿ����ͷš�
 *
 * @param handle ����ҳ���
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
 * ��һ��ҳ�ϵ����ɹ���������Ϊ������
 * @pre ������һ��Ҫ����ҳ���ϵ�pin
 *
 * @param handle ����ҳ���
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
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
 * �ͷ�һ��ҳ�ϵ�pin
 * @pre �����߲�����ҳ�ϵ�����������ʱ�ͷ�pinû���κ�����
 *
 * @param handle ����ҳ���
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
 * ����һ��ҳΪ��ҳ
 *
 * @param handle ����ҳ���
 */
void Session::markDirty(ntse::BufferPageHandle *handle) {
	assert(m_inuse);
	assert(handle->m_valid);
	m_buffer->markDirty(this, handle->m_page);
}

/**
 * �ͷ�ҳ�滺����ָ���ļ����������ҳ
 *
 * @param file �ļ�
 * @param writeDirty �Ƿ�д����ҳ
 */
void Session::freePages(File *file, bool writeDirty) {
	assert(m_inuse);
	return m_buffer->freePages(this, file, writeDirty);
}

/**
 * �ͷ�ҳ�滺����Ϊָ���������������ҳ
 *
 * @param file �ļ�
 * @param indexId ����ID
 * @param callback �����ж�һ��ҳ�Ƿ���ָ������ʹ�õ�ҳ�Ļص�����
 *   ��ΪNULL���ͷ�Ϊָ���ļ����������ҳ
 */
void Session::freePages(File *file, uint indexId, bool (*callback)(BufferPageHdr *page, PageId pageId, uint indexId)) {
	assert(m_inuse);
	return m_buffer->freePages(this, file, indexId, callback);
}

/**
 * ��ʼ����NTSE�е���������ͨ���ݿ��е�����ͬ��
 * �����ڱ�֤��һ����¼����������ԭ���ԡ�
 *
 * @param type ��������
 * @param tableId ����Ҫ�޸ĵı��ID��������С�ʹ����������ID��
 * @param writeLog �Ƿ�д��־
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
 * ��������
 *
 * @param commit ���ύ���ǻع�
 * @param writeLog �Ƿ�д��־
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

/** �õ�����״̬
 * @param return ��ǰ�Ự��ʲô�����У�TXN_NONE��ʾ����������
 */
TxnType Session::getTxnStatus() const {
	return m_txnStatus;
}

/** ���ر��Ự���������Ƿ���¼��־
 * @return �Ƿ��¼��־
 */
bool Session::isLogging() {
	assert(m_inuse);
	return m_logging;
}

/** ָ�����Ự��������������Ҫд��־
 * @pre ������������
 */
void Session::disableLogging() {
	assert(m_inuse);
	assert(m_txnStatus == TXN_NONE);
	m_logging = false;
}

/** ָ�����Ự������������Ҫд��־
 * @pre ������������
 */
void Session::enableLogging() {
	assert(m_inuse);
	assert(m_txnStatus == TXN_NONE);
	m_logging = true;
}

/**
 * ����һ����־
 *
 * @param logType ��־����
 * @param tableId ��ID
 * @param data ��־����
 * @param size ��־���ݴ�С
 */
void Session::cacheLog(LogType logType, u16 tableId, const byte *data, size_t size) {
	assert(m_inuse);
	if (!m_logging)
		return;
	// �ȼ�ʵ�֣�ֱ��д����־�ļ��У�������������������Ż�
	m_lastLsn = writeLog(logType, tableId, data, size);
}

/**
 * д��־��ע: д��־�����׳��쳣
 *
 * @param logType ��־����
 * @param tableId ��ID
 * @param data ��־����
 * @param size ��־���ݴ�С
 * @return ��־LSN�����Ự����Ϊ��д��־�򷵻�INVALID_LSN
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
 * д������־��
 *
 * @param logType ��־����
 * @param tableId ��ID
 * @param cpstForLsn ������־��Ϊ�˲�������(REDO)��־
 * @param data ��־����
 * @param size ��־���ݴ�С
 * @return ��־LSN
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
 * flush������־��
 *
 * @param lsnType ��־LSN
 */
void Session::flushLog(LsnType lsn, FlushSource fs) {
	m_txnLog->flush(lsn, fs);
}

/** ����Ԥ������־��Ԥ������־�а������б��������Ե�ֵ���������������Ե�ֵ
 *
 * @param tableDef ����
 * @param before ����ǰ�ļ�¼���ݣ�ΪREC_REDUNDANT��ʽ������MMS���д��־����ʱΪNULL
 * @param update ���º������ֵ��ΪREC_MYSQL��ʽ������MMS���д��־����ʱΪREC_REDUNDANT��ʽ
 * @param updateLob �Ƿ���´��������
 * @param indexPreImage ����ΪNULL����������漰����������������ǰ��ΪNULL��ʾ����������
 * @param size OUT��Ԥ������־��С
 * @return Ԥ������־����
 * @throw NtseException ��־����
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
				// ����������ǰ����NULL�����
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
	// ͷ��Ϣ
	s.writeRid(update->m_rowId);
	s.write(indexPreImage != NULL);
	s.write(numLobs);
	// ����¼
	SubRecord updateR(REC_REDUNDANT, update->m_numCols, update->m_columns, update->m_data, update->m_size);
	assert(RecordOper::getSubRecordSerializeSize(tableDef, &updateR, false, true) < maxLogSize - s.getSize());
	RecordOper::serializeSubRecordMNR(&s, tableDef, &updateR, false, false); 
	// ����������ǰ��
	if (indexPreImage) {
		assert(RecordOper::getSubRecordSerializeSize(tableDef, indexPreImage, false, true) < maxLogSize - s.getSize());
		RecordOper::serializeSubRecordMNR(&s, tableDef, indexPreImage, false, false);
	}	
	// �����
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

/** дԤ������־
 * @param tableDef ����
 * @param log ��־����
 * @param size ��־��С
 */
void Session::writePreUpdateLog(const TableDef *tableDef, const byte *log, size_t size) {
	cacheLog(LOG_PRE_UPDATE, tableDef->m_id, log, size);
}

/**
 * ����Ԥ������־���������ݴ�MemoryContext�з���
 *
 * @param tableDef ����
 * @param log ��־
 * @return ��������
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
	// ͷ��Ϣ
	RowId rid;
	s.readRid(&rid);
	s.read(&update->m_updateIndex);
	s.read(&update->m_numLobs);
	// ����¼
	update->m_subRec = RecordOper::unserializeSubRecordMNR(&s, tableDef, getMemoryContext());
	update->m_subRec->m_rowId = rid;
	// ����ǰ��
	if (update->m_updateIndex) {
		update->m_indexPreImage = RecordOper::unserializeSubRecordMNR(&s, tableDef, getMemoryContext());
		update->m_indexPreImage->m_rowId = rid;
	}
	// �����
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

/** �õ�Ԥ������־�������ļ�¼RID
 * @param log Ԥ������־
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
 * MMS�������¼����ʱдԤ������־����־��ʽ:
 *   RID: ռ��RID_BYTES
 *   ������������: u16
 *   �����������Ժ�: u16 * ������������
 *   ��������������: ʹ��REC_VARLEN�Ĳ��ּ�¼��ʽ��ʾ
 *
 * @param tableDef ��ģʽ����
 * @param update �������ݣ�ΪREC_REDUNDANT��ʽ
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
 * ����LOG_PRE_UPDATE_HEAP��־
 *
 * @param tableDef ��ģʽ����
 * @param log ��־
 * @return �������������ݣ��洢ΪREC_REDUNDANT��ʽ���ڴ��MemoryContext����
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
 * д����ʼ��־
 *
 * @param tableId ��������ı��ID
 * @param type ��������
 * @return ��־LSN
 */
u64 Session::writeTxnStartLog(u16 tableId, TxnType type) {
	byte bType = (byte)type;
	return m_txnLog->log(m_id, LOG_TXN_START, tableId, &bType, sizeof(bType));
}

/**
 * ��������ʼ��־
 *
 * @param log ��־����
 * @return ��������
 */
TxnType Session::parseTxnStartLog(const LogEntry *log) {
	assert(log->m_size == sizeof(byte));
	assert(log->m_logType == LOG_TXN_START);

	return (TxnType )(*(log->m_data));
}

/**
 * д���������־
 *
 * @param commit �����Ƿ�ɹ�
 * @return ��־LSN�����Ự����Ϊ��д��־�򷵻�INVALID_LSN
 */
u64 Session::writeTxnEndLog(bool commit) {
	if (!m_logging)
		return INVALID_LSN;
	byte succ = (byte)commit;
	return m_txnLog->log(m_id, LOG_TXN_END, m_tableId, &succ, sizeof(succ));
}

/**
 * �������������־
 *
 * @param log ��־����
 * @return �����Ƿ��ύ
 */
bool Session::parseTxnEndLog(const LogEntry *log) {
	assert(log->m_size == sizeof(byte));
	assert(log->m_logType == LOG_TXN_END);

	return *(log->m_data) != 0;
}

/**
 * ���һ����־��LSN
 *
 * @return ���һ����־��LSN
 */
u64 Session::getLastLsn() const {
	assert(m_inuse);
	return m_lastLsn;
}

/**
 * ��õ�ǰ��Ծ�������ʼLSN
 *
 * @return ��ǰ��Ծ�������ʼLSN����û�л�Ծ�����򷵻�INVALID_LSN
 */
u64 Session::getTxnStartLsn() const {
	assert(m_inuse);
	return m_txnStartLsn;
}

/**
 * ��þ�������Ч���Ƿ�־û�����־LSN����������־��д�����������ڻָ�ʱ�ᱻREDO��
 * ��Ч����־û������������ڻָ�ʱ�ᱻ�ع���
 * ������Ӧ�����������֮����ã����ظո���ɵ������о�����Ч���Ƿ�־û�����־LSN��
 * @pre ����������
 *
 * @return ��þ�������Ч���Ƿ�־û�����־LSN
 */
u64 Session::getTxnDurableLsn() const {
	assert(m_inuse && m_txnStatus == TXN_NONE);
	return m_txnDurableLsn;
}

/**
 * ���þ�������Ч���Ƿ�־û�����־LSN��
 * @pre ��������
 *
 * @param lsn ��־LSN 
 */
void Session::setTxnDurableLsn(u64 lsn) {
	assert(m_inuse && m_txnStatus != TXN_NONE && m_txnDurableLsn == INVALID_LSN);
	m_txnDurableLsn = lsn;
}

/**
 * ��ȡ���һ�������Ƿ��ύ��
 * @pre �����Ѿ���������ǰ����������
 *
 * @return ���һ�������Ƿ��ύ
 */
bool Session::isTxnCommit() const {
	assert(m_inuse && m_txnStatus == TXN_NONE);
	return m_isTxnCommit;
}

/**
 * ��������һ��
 *
 * @param tableId ��ID
 * @param rid Ҫ�����е�RID
 * @param lockMode ��ģʽ
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
 * @return �����������ΪNULL��ʾû�����ɹ�
 */
RowLockHandle* Session::tryLockRow(u16 tableId, RowId rowId, LockMode lockMode, const char *sourceFile, uint line) {
	assert(m_inuse);
	if (!m_rowLockMgr->tryLock(m_id, getGlobalRid(tableId, rowId), lockMode))
		return NULL;
	ftrace(ts.rl, tout << tableId << rid(rowId) << lockMode << sourceFile << line);
	return allocRowLockHandle(tableId, rowId, lockMode, sourceFile, line);
}

/**
 * ����һ��
 *
 * @param tableId ��ID
 * @param rowId Ҫ�����е�RID
 * @param lockMode ��ģʽ
 * @param sourceFile ������һ������������Դ�ļ�
 * @param line ������һ���������к�
 * @return ���������һ�����Գɹ���������ΪNULL
 */
RowLockHandle* Session::lockRow(u16 tableId, RowId rowId, LockMode lockMode, const char *sourceFile, uint line) {
	assert(m_inuse);
	m_rowLockMgr->lock(m_id, getGlobalRid(tableId, rowId), lockMode, -1);
	ftrace(ts.rl, tout << tableId << rid(rowId) << lockMode << sourceFile << line);
	return allocRowLockHandle(tableId, rowId, lockMode, sourceFile, line);
}

/**
 * �ж�ָ�������Ƿ񱻵�ǰ�Ự��ָ����ģʽ����
 *
 * @param tableId ��ID
 * @param rid Ҫ�����е�RID
 * @param lockMode ��ģʽ
 * @return ָ�������Ƿ񱻵�ǰ�Ự��ָ����ģʽ����
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
 * �����һ�е���
 *
 * @param handle �������
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
 * ��ӡʹ���е�ҳ�滺������Ϣ����׼���
 * ע: �������ڷ��ʸ��Ự��Ϣʱ�޷�ʵ��ͬ�����ƣ�ֻӦ�ڵ���ʱʹ��
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
 * ��ӡʹ���е����������Ϣ����׼���
 * ע: �������ڷ��ʸ��Ự��Ϣʱ�޷�ʵ��ͬ�����ƣ�ֻӦ�ڵ���ʱʹ��
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

/** ����һ��������
 * @param lock ������
 */
void Session::addLock(const Lock *lock) {
	assert(m_inuse);
	m_locks.push_back(lock);
}

/** �Ƴ�һ��������
 * @param lock ������
 */
void Session::removeLock(const Lock *lock) {
	assert(m_inuse);
	vector<const Lock *>::iterator it = std::find(m_locks.begin(), m_locks.end(), lock);
	assert(it != m_locks.end());
	m_locks.erase(it);
}

/** ����һ���Ự��״̬�������Դ�Ƿ�й©
 * ������������: memoryContext��lobContext, userData
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
 * ��黺��ҳ��Դ�Ƿ�й©��й©ʱ�Զ��ͷŲ��Ҵ�ӡ������Ϣ
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
			// ��Ҫ��valid��־��Ϊfalse,�μ�QAƽ̨Bug27142
			m_bpHandles[i].m_valid = false;
		}
	}
}

/**
 * ���������Դ�Ƿ�й©��й©ʱ�Զ��ͷŲ��Ҵ�ӡ������Ϣ
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
			// ��Ҫ��valid��־��Ϊfalse,�μ�QAƽ̨Bug27142
			rlHandle->m_valid = false;
		}
	}
}

/**
 * ���������ص����Ƿ��ͷŽ���
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

/** ��������������Ƿ�й© */
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
 * ��ָ�����������Լ���
 * @param objectId	��������ID
 * @return �����ɹ�true������false
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
 * ��ָ���������� ����
 * @param objectId	��������ID
 * @return �����ɹ�true������false
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
 * �ͷ�ĳ������������
 * @param objectId	��������ID
 * @param token		�ͷŵ����Ǻű�����ڵ���token��Ĭ��ֵ��������ڿ�ʼ
 * @return �����ɹ�true������false
 */
bool Session::unlockIdxObject(u64 objectId, u64 token) {
	bool unlocked = false;

	map<u64, PageId>::iterator iter = m_lockedIdxObjects.lower_bound(token);
	// ֻҪtokenʹ��ǡ����while�����Ĵ���Ӧ�ò��࣬�����ͷ�ĳ�������������ֻ�ܴ�ͷ��ʼ����
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
 * �ͷŵ�ǰ�Ự���ϵ���������Դ
 * @return �ͷųɹ�true������false
 */
bool Session::unlockIdxAllObjects() {
	nftrace(ts.irl, tout << m_id <<  " unlock all locks";);
	bool succ = m_idxLockMgr->unlockAll(m_id);
	if (succ)
		m_lockedIdxObjects.clear();
	return succ;
}

/**
 * �жϵ�ǰ�Ự�Ƿ����ĳ��������������Դ
 * @param objectId	��������ID
 * @return true��ʾ���У�����false
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
 * �õ���ǰ�ļ����������к�
 * @return �����������к�
 */
u64 Session::getToken() {
	return m_token;
}


/**
 * �ͷ���Щ�����������к���ָ��ֵ֮�����
 * @param token	ָ���ļ������к�
 * @return ���ط����ɹ����
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
 * ���ػỰ����ǰ�Ƿ��������Դ
 */
bool Session::hasLocks() {
	return !m_lockedIdxObjects.empty();
}


/**
 * �õ�ָ������ǰ�����ĸ��Ự���У����û�б��κλỰ���У�����-1
 * @param objectId	�����
 * @return ���ж������ĻỰ��ţ������Ự���з���-1
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
// ���ݿ����� ///////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
#ifdef NTSE_UNIT_TEST
/** ����������ҳ�滺��������õ����Ӷ��� */
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
 * ��ȡ�û��Զ�������
 *
 * @return �û��Զ�������
 */
void* Connection::getUserData() const {
	return m_userData;
}

/**
 * �����û��Զ�������
 * ע: �û��Զ��������������ͷŵ�ʱ��ᱻ�Զ�����ΪNULL
 *
 * @param dat �û��Զ�������
 */
void Connection::setUserData(void *dat) {
	m_userData = dat;
}

/**
 * �õ�����ID
 *
 * @return ����ID��һ��>0
 */
uint Connection::getId() const {
	return m_id;
}

/**
 * �õ����ӱ��ֵ�ǰ״̬��ʱ��
 *
 * @return ���ӱ��ֵ�ǰ״̬��ʱ�䣬��λ��
 */
uint Connection::getDuration() const {
	return System::fastTime() - m_durationBegin;
}

/**
 * ����״̬��Ϣ
 * @param status ״̬��Ϣ���ڴ�ʹ��Լ��: ֱ������
 */
void Connection::setStatus(const char *status) {
	LOCK(&m_lock);
	m_status = status;
	m_durationBegin = System::fastTime();
	UNLOCK(&m_lock);
}

/** �����������
 * @return �������ƣ�������NULL
 */
const char* Connection::getName() const {
	return m_name;
}


///////////////////////////////////////////////////////////////////////////////
// BgTask /////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
/** ���캯��
 * @param db ���ݿ⣬����ΪNULL
 * @param name �����߳�����
 * @param interval ����ִ�����ڣ���λ����
 * @param alwaysHoldSession �Ƿ�ʼ�ճ��лỰ����Ϊfalse��ÿ������ʱȡ�ûỰ
 * @param allocSessionTimeoutMs ����Ự��ʱʱ�䣬<0��ʾ����ʱ��0��ʾ���ϳ�ʱ��
 *   >0��ʾ������ָ���ĳ�ʱʱ�䡣����alwaysHoldSessionΪfalseʱ����Ч
 * @param runInRecover �ָ��������Ƿ�Ҳ��Ҫ���У�Ϊ��֤�ɻָ��ԣ�ֻ�в��������־�ĵĺ�̨
 *   �̲߳ſ�����Ϊtrue
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


/** �������� */
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
		// TODO ��̨�߳���Ҫ���й������ݿ�ر�֮ǰ��Ҫֹͣ����̨�߳�
		// ���������ݿ�رչ����У�ĳЩ�����Ѿ�������������¿��ܳ���
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

/** ���setUp�Ƿ����
 * @return setUp�Ƿ����
 */
bool BgTask::setUpFinished() {
	return m_setUpFinished;
}

/** ��� �ָ��������Ƿ�Ҳ��Ҫ����
 * @return �ָ��������Ƿ�Ҳ��Ҫ����
 */
bool BgTask::shouldRunInRecover() {
	return m_runInRecover;
}
}
