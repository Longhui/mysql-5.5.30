/**
 * �Ự����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** ����ͳ������ */
enum StatType {
	OPS_LOG_READ = 0,	/** �߼������� */
	OPS_LOG_WRITE,		/** �߼�д���� */
	OPS_PHY_READ,		/** ��������� */
	OPS_PHY_WRITE,		/** ����д���� */
	OPS_ROW_READ,		/** ��ȡ�ļ�¼�� */
	OPS_ROW_INSERT,		/** ����ļ�¼�� */
	OPS_ROW_UPDATE,		/** ���µļ�¼�� */
	OPS_ROW_DELETE,		/** ɾ���ļ�¼�� */
	OPS_TBL_SCAN,		/** ��ɨ����� */
	OPS_TBL_SCAN_ROWS,	/** ��ɨ�辭�������� */
	OPS_IDX_SCAN,		/** ����ɨ����� */
	OPS_IDX_SCAN_ROWS,	/** ����ɨ�辭�������� */
	OPS_POS_SCAN,		/** ��λɨ����� */
	OPS_POS_SCAN_ROWS,	/** ��λɨ�辭�������� */
	OPS_OPER,			/** �������������������̨�̲߳������� */
	OPS_NUM,
};

/** ��������ͳ�� */
struct OpStat {
	u64		m_statArr[OPS_NUM];	/** �������ͳ����Ϣ */
	OpStat	*m_parent;			/** ��ͳ�ƶ�����ֵΪ��ͳ�ƶ���֮�� */

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

/** ���ݿ�������� */
enum DBObjType {
	DBO_Unknown,	/** δ֪���� */
	DBO_Heap,		/** �ѣ�������С�ʹ����ѣ� */
	DBO_TblDef,     /** tabledefԪ�����ļ�*/
	DBO_LlobDir,	/** ���ʹ����Ŀ¼ */
	DBO_LlobDat,	/** ���ʹ�������� */
	DBO_Slob,		/** С�ʹ���� */
	DBO_STblDef,
	DBO_Indice,		/** �����г���������֮������� */
	DBO_Index,		/** ����ĳ����������� */
};

/** ���ݿ�����߼������д״̬���� */
enum DBObjStatType {
    DBOBJ_LOG_READ = 0,		/** �߼��� */
	DBOBJ_LOG_WRITE,		/** �߼�д */
	DBOBJ_PHY_READ,			/** ����� */
	DBOBJ_PHY_WRITE,		/** ����д */
	DBOBJ_ITEM_READ,		/** ������� */
	DBOBJ_ITEM_INSERT,		/** ��������� */
	DBOBJ_ITEM_UPDATE,		/** ��������� */
	DBOBJ_ITEM_DELETE,		/** ������ɾ�� */
	DBOBJ_SCAN,				/** ɨ����� */
	DBOBJ_SCAN_ITEM,		/** ɨ�践�ص��������� */
	DBOBJ_MAX,
};

/** ���ݿ������ͳ�ƽṹ */
struct DBObjStats {
	DBObjType	m_type;					/** �������� */
	const char	*m_idxName;				/** ��������ΪDBO_IndexʱΪ������������Ϊ"" */
	u64			m_statArr[DBOBJ_MAX];	/** ���ݶ���״̬���� */
	bool		m_bufInternal;			/** �Ƿ���ҳ�滺��ģ��ʹ�õ��ڲ�ͳ�ƽṹ
										 * ���ݿ����ͳ�ƽṹ���ڲ�����������ⲿ����ģ�
										 * ֻ�иձ�Ԥ��������ҳ������������ΪԤ��������ҳ���ͳ�ƽṹ�ϲ�û�и�����
										 * ���ǲ�֪������һ������ǰֻ�������Ż����һ���ļ����ܶ�Ӧ���ͳ�ƽṹ
										 * ���������Ϊ���Ժ���չʱ�İ�ȫ�ԣ����������Ͷ�����Ԥ��������ҳ���ͳ��
										 * �ṹ�뵱ǰҳ����ͬ��һ���衣
										 * �ڲ������ͳ�ƽṹʹ��new/delete�����ͷţ�����Ԥ��������ҳ��ͨ���ᱻ
										 * ���Ϸ��ʵ�����ʱ�ڲ������ͳ�ƽṹ�ͻᱻ�ͷţ�����ڲ������ͳ�ƽṹ
										 * ����������̫�࣬���ᵼ�����ص��������⡣
										 */ 
	
	DBObjStats(DBObjType type, bool bufInternal = false) {
		memset(m_statArr, 0, sizeof(m_statArr));
		m_idxName = "";
		m_type = type;
		m_bufInternal = bufInternal;
	}

	/** ����ָ����ͳ����Ϣ��
	 * @param type ͳ����Ϣ��
	 * @param delta ��ֵ
	 */
	inline void countIt(DBObjStatType type, int delta = 1) {
		assert(type < DBOBJ_MAX);
		m_statArr[type] += delta;
	}

	/** ��another�е�ͳ�ƽ���ϲ�����
	 * @param another Ҫ�ϲ���ͳ�ƽ��
	 */
	void merge(const DBObjStats *another) {
		for (size_t i = 0; i < sizeof(m_statArr) / sizeof(m_statArr[0]); i++)
			m_statArr[i] += another->m_statArr[i];
	}
};

class Session;
/** ���ݿ����� */
class Connection {
public:
#ifdef NTSE_UNIT_TEST
	Connection();
#endif
	Connection(const LocalConfig *globalConfig, OpStat *globalStat, bool internal, const char *name = NULL);
	~Connection() {
		delete []m_name;
	}
	/** �õ�����˽������
	 * @param ����˽������
	 */
	LocalConfig* getLocalConfig() const {
		return (LocalConfig *)&m_config;
	}
	/** �õ�����˽��ͳ����Ϣ
	 * @param ����˽��ͳ����Ϣ
	 */
	OpStat* getLocalStatus() const {
		return (OpStat *)&m_stat;
	}
	bool isInternal() const;
	void* getUserData() const;
	void setUserData(void *dat);
	uint getId() const;
	uint getDuration() const;
	/** ��õ�ǰ״̬��Ϣ
	 * @return ��ǰ״̬��Ϣ��Ϊ�ڲ���Ϣ�Ŀ�������������Ҫ�ͷ�
	 */
	char* getStatus() {
		LOCK(&m_lock);
		char *copy = System::strdup(m_status);
		UNLOCK(&m_lock);
		return copy;
	}
	void setStatus(const char *status);
	const char* getName() const;
	/** ����ʹ�ø����ӵ��߳�ID����Ӧ���ڹ��캯���о�ָ���߳�ID��
	 * ��Ϊ�˷�ֹ�޸Ĵ�����ȡ���ӵĴ��룬ʹ�ñ�������ָ����һ��
	 * ����ֻ������һ�Ρ�
	 *
	 * @pre ��ǰû�����ù��߳�ID
	 *
	 * @param id �߳�ID������Ϊ������
	 */
	void setThdID(uint id) {
		assert(!m_thdId && id > 0);
		m_thdId = id;
	}
	/** ��ȡʹ�ø����ӵ��߳�ID
	 * @pre �߳�ID�Ѿ�������
	 *
	 * @return ʹ�����ӵ��߳�ID
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
	const char	*m_name;			/** �������� */
	LocalConfig	m_config;			/** ��ǰ����˽��������Ϣ */
	OpStat		m_stat;				/** ��ǰ����˽��ͳ����Ϣ */
	bool		m_internal;			/** �Ƿ�Ϊ�ڲ����� */
	DLink<Connection *>	m_cpLink;	/** �����ӳ��е����� */
	void		*m_userData;		/** �û��Զ�����Ϣ */
	uint		m_id;				/** ����ID */
	uint		m_thdId;			/** ʹ�ø����ӵ��߳�ID */
	const char	*m_status;			/** ״̬ */
	u32			m_durationBegin;	/** ��ǰ״̬��ʼʱ�� */
	bool        m_trx;              /** ��ʶ�������Ƿ�Ϊ�������� */
	Mutex		m_lock;				/** ����status���� */

friend class SessionManager;
};

struct BufferPageHdr;
class File;
/** ����ҳ��� */
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
	bool			m_valid;	/** �Ƿ���ʹ���� */
	BufferPageHdr	*m_page;	/** ��Ӧ����ҳ */
	const char		*m_path;	/** �ļ�·�� */
	u64				m_pageId;	/** ҳ�� */
	const char		*m_file;	/** ������һ������������Դ�ļ� */
	uint			m_line;		/** ������һ���������к� */
	LockMode		m_lockMode;	/** ���ӵ��� */
	bool			m_pinned;	/** �Ƿ����pin */

friend class Session;
};

/** ������� */
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
	bool		m_valid;	/** �Ƿ���ʹ���� */
	u16			m_tableId;	/** ����ס����������ID */
	RowId		m_rid;		/** ����ס���е�RID */
	LockMode	m_mode;		/** ��ģʽ */
	const char	*m_file;	/** ������һ������������Դ�ļ� */
	uint		m_line;		/** ������һ���������к� */

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
	size_t m_poolId;  /** ��С���ڴ���е�ID */

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
/** �Ự������ */
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

	/** �ڲ��Ự�������ƣ����������ò���������һ������Ҫϵͳ
	 * ʵ���йأ��븺�ع�ϵ����
	 */
	static const uint INTERNAL_SESSIONS = 8;

private:
	Session* tryAllocSession(const char *name, Connection *conn, int high, int low);

private:
	u16		m_maxSessions;		/** ���Ự�� */
	u16		m_internalSessions;	/** �ڲ��Ự�� */
	Session	**m_sessions;		/** ���Ự���� */
	Mutex	m_lock;				/** ������������ */
	DList<Connection *>	*m_activeConns;	/** ��Ծ���� */
	uint	m_nextConnId;		/** ��һ������ID */
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
/** �Ự������������ά��һ�����ִ�й�����NTSE�ײ�Ļ���ҳ��������Դ */
class Session {
public:
#ifdef NTSE_UNIT_TEST
	Session(Connection *conn, Buffer *buffer);
#endif
	virtual ~Session();
	// ��Ϣ��ȡ
	/** ��ȡ�ỰID
	 * @return �ỰID��һ��>0
	 */
	u16 getId() const {
		assert(m_id > 0);
		return m_id;
	}
	/** ���ػỰ���ڴ����������
	 * @return �Ự���ڴ����������
	 */
	MemoryContext* getMemoryContext() const {
		assert(m_inuse);
		return m_memoryContext;
	}
	/** ����ר���ڴ洢��ѯʱ�����صĴ�������ݵ��ڴ���������ģ�ע����һ
	 * �ڴ���������Ĳ���������IUD�����еĴ������ش���
	 * @return �Ự���ڴ����������
	 */
	MemoryContext* getLobContext() const {
		assert(m_inuse);
		return m_lobContext;
	}
	/** ���ػỰ���������ݿ�����
	 * @return �Ự���������ݿ�����
	 */
	Connection* getConnection() const {
		assert(m_inuse);
		return m_conn;
	}
	/** ��ȡ�Ự����ʱ��ʱ��
	 * @return �Ự����ʱ��ʱ�䣬��λ��
	 */
	u32 getAllocTime() const {
		assert(m_inuse);
		return m_allocTime;
	}
	/** ����ָ�������Ĵ���
	 * @param type ��������
	 */
	void incOpStat(StatType type) {
		m_conn->getLocalStatus()->countIt(type);
	}
	/** ���ûỰȡ��״̬
	 * @param canceled �Ƿ�ȡ��
	 */
	void setCanceled(bool canceled) {
		m_canceled = canceled;
	}
	/** ��ȡ�Ựȡ��״̬
	 * @return �Ƿ�ȡ��
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

	// ����ҳ����
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
	// ��������
	RowLockHandle* tryLockRow(u16 tableId, RowId rid, LockMode lockMode, const char *sourceFile, uint line);
	RowLockHandle* lockRow(u16 tableId, RowId rid, LockMode lockMode, const char *sourceFile, uint line);
	bool isRowLocked(u16 tableId, RowId rid, LockMode lockMode);
	void unlockRow(RowLockHandle **handle);
	void dumpRowLockHandles();
	//Ψһ�Լ�ֵ������
	bool tryLockUniqueKey(UKLockManager *uniqueKeyMgr, u64 keyChecksum, const char *file, uint line);
	bool lockUniqueKey(UKLockManager *uniqueKeyMgr, u64 keyChecksum, const char *file, uint line);
	void unlockUniqueKey(UKLockHandle* ukLockHdl);
	void unlockAllUniqueKey();
	// ����ҳ��������
	bool tryLockIdxObject(u64 objectId);
	bool lockIdxObject(u64 objectId);
	u64 getToken();
	bool unlockIdxObject(u64 objectId, u64 token = 0);
	bool unlockIdxAllObjects();
	bool unlockIdxObjects(u64 token = 0);
	bool isLocked(u64 objectId);
	bool hasLocks();
	u64 whoIsHolding(u64 objectId);
	// �������������
	void addLock(const Lock *lock);
	void removeLock(const Lock *lock);
	// ������־
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



private:	// ��Щ����ֻ��SessionManager����
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
	/** һ���Ự������ͬʱ���еĻ���ҳ�� */
	static const uint MAX_PAGE_HANDLES = Limits::MAX_BTREE_LEVEL * 2 + 4;

	// Note: ��������ʱ�ǵü���Ƿ���Ҫ��Session::resetʱ����״̬
	BufferPageHandle	m_bpHandles[MAX_PAGE_HANDLES];	/** ����ҳ������� */

	ObjectPool<RowLockHandleElem> m_rlHandlePool; /** ��������������� */
	DList<RowLockHandleElem*> m_rlHandleInUsed;   /** ��ǰ���е������б� */
	
	vector<const Lock *>	m_locks;	/** ������������ */
	Database	*m_db;					/** ���ݿ���� */
	Connection	*m_conn;				/** ���ݿ����� */
	const char	*m_name;				/** �Ự���� */
#ifdef NTSE_UNIT_TEST
	bool		m_bufferTest;			/** �����ڵ�Ԫ�������õ�mock���� */
#endif
	Buffer		*m_buffer;				/** ҳ�滺�� */
	LockManager	*m_rowLockMgr;			/** ���������� */
	Txnlog		*m_txnLog;				/** ������־ */
	TxnType		m_txnStatus;			/** �Ƿ��������� */
	u16			m_id;					/** �ỰID��ͬʱҲ�䵱����ID */
	u16			m_tableId;				/** startTransactionʱ������tableId */
	u64			m_lastLsn;				/** ���Ựд�����һ����־��LSN */
	u64			m_txnStartLsn;			/** ������ʼLSN */
	u64			m_txnDurableLsn;		/** ������־��д������ʹ������־��������
										 * ����Ҳ�ᱻREDO����Ч��Ҳ�ᱻ�־û�
										 */
	bool		m_isTxnCommit;			/** �ոս����������Ƿ�ɹ� */
	MemoryContext	*m_memoryContext;	/** �Ự��Ӧ���ڴ���������� */
	MemoryContext	*m_lobContext;		/** ר���ڴ洢��ѯʱ�����صĴ��������
										 * ���ڴ���������ģ�ע����һ�ڴ����
										 * �����Ĳ���������IUD�����еĴ����
										 * ��ش���
										 */
	LockManager  *m_uniqueIdxLockMgr;
	ObjectPool<UKLockHandle> m_ukLockHandlePool;
	DList<UKLockHandle*> m_ukLockList;

	IndicesLockManager *m_idxLockMgr;	/** ������ҳ����ʹ�õ���������� */
	map<u64, PageId> m_lockedIdxObjects;/** ��������ûỰ�ӹ�������������������Ϣ */
	u64			m_token;				/** �Ựÿ��һ��ҳ��������Ӧһ����ͬ��ֵ����ֵֻ������ */
	bool		m_inuse;				/** �Ƿ���ʹ���� */
	u32			m_allocTime;			/** �Ự����ʱ��ʱ�� */
	bool		m_logging;				/** �Ƿ��¼��־ */
	bool		m_canceled;				/** �����Ƿ�ȡ�� */

#ifdef TNT_ENGINE
	tnt::TNTTransaction	*m_trx;				/** ��ǰsession����Ӧ������ */
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


/** ��̨���ݿ������̡߳���һ�����̵߳���Ҫ�����Ƿ����߳��������ݿ����ӺͻỰ��
 * ������BgTaskʱָ�������ݿ⣬�����BgTask��setUp��tearDown�����з������ͷ�
 * ���ݿ����ӡ�ͬʱBgTask��ÿ�ε���runIt����֮ǰ��׼���ûỰ���������ʱָ��
 * alwaysHoldSession����ʼ��ʹ��һ���Ự����setUp��tearDown�з������ͷţ���ʱ
 * ÿ�ε�����runIt������BgTask�����ûỰ�����û��ָ��alwaysHoldSession����
 * BgTask���ڵ���runIt֮ǰ����һ���Ự��runIt���غ������ͷš����allocSessionTimeoutMs
 * ����-1������ָ����ʱ���ڷ��䲻���Ựʱ��runIt�������ᱻ���á�
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
	/** ������һ����ʵ��ÿ������ʱҪ���Ĳ��� */
	virtual void runIt() = 0;

protected:
	Database	*m_db;		/** ���ݿ⣬����ΪNULL */
	Connection	*m_conn;	/** ���ݿ����ӣ���m_dbΪNULL��ΪNULL */
	Session		*m_session;	/** �Ự����m_dbΪNULL��ΪNULL��ÿ��doRun����֮ǰ���䣬����֮���ͷ� */
	bool		m_alwaysHoldSession;		/** �Ƿ�ʼ�ճ��лỰ����Ϊfalse��ÿ������ʱȡ�ûỰ */
	int			m_allocSessionTimeoutMs;	/** ����Ự��ʱʱ�� */
	bool		m_runInRecover;		/** �ָ��������Ƿ�Ҳ��Ҫ���� */
	bool		m_setUpFinished;	/** setUp�Ƿ��Ѿ���� */
};
}

#endif
