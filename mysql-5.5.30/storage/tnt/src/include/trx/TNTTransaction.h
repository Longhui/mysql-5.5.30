/**
 * TNT�������ģ�顣
 * 
 * @author ��ΰ��(liweizhao@corp.netease.com)
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

/** ����״̬ */
enum TrxState {
	TRX_NOT_START, /** ����δ��ʼ */
	TRX_ACTIVE,    /** ��Ծ״̬ */
	TRX_PREPARED,  /** ׼��״̬ */
	TRX_COMMITTED_IN_MEMORY, /** �������ύ���ǻ�δ�־û� */
};

/** ������뼶�� */
enum TrxIsolationLevel {
	TRX_ISO_READ_UNCOMMITTED, /** ��� */
	TRX_ISO_READ_COMMITTED,   /** �ύ�� */
	TRX_ISO_REPEATABLE_READ,  /** ���ظ��� */
	TRX_ISO_SERIALIZABLE,     /** �ɴ��л� */
	TRX_ISO_UNDEFINED,        /** δ���� */
};

enum TrxFlushMode {
	TFM_NOFLUSH = 0, //�����ύʱ����buffer����־ˢ�����
	TFM_FLUSH_SYNC,  //�����ύʱ��buffer����־ˢ����棬��fsync
	TFM_FLUSH_NOSYNC //�����ύʱ��buffer����־ˢ����棬��������fsync
};

const static uint TRX_MAGIC_NUM = 91118598; /** ����У����������Ƿ�Ϸ���ħ�� */

class TNTTrxLog;
class TNTTrxSys;

/** �ɼ�����ͼ */
class ReadView {
public:
	ReadView(TrxId createTrxId, TrxId *trxIds, uint trxCnt);
	~ReadView() {}

	/**
	 * ��ͼ��ʼ��
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
	 * ������ͼ�еĻ�Ծ������
	 * @param trxCnt
	 */
	inline void setTrxCnt(uint trxCnt) {
		m_trxCnt = trxCnt;
	}

	/**
	 * ���õ�ǰ��ͼ�е�ĳһ����Ծ����Id
	 * @param n
	 * @param trxId
	 */
	inline void setNthTrxIdViewed(uint n, TrxId trxId) {
		assert(m_trxIds);
		assert(n < m_trxCnt);
		m_trxIds[n] = trxId;
	}

	/**
	 * ��ȡ��ǰ��ͼ�е�ĳһ����Ծ����ID
	 * @param n
	 * @return 
	 */
	inline TrxId getNthTrxIdViewed(uint n) {
		assert(m_trxIds);
		assert(n < m_trxCnt);
		return m_trxIds[n];
	}

	/**
	 * ���ò��ɼ�����ID������
	 * @param lowTrxId
	 */
	inline void setLowTrxId(TrxId lowTrxId) {
		m_lowTrxId = lowTrxId;
	}

	/**
	 * ���ÿɼ�����ID������
	 * @param upTrxId
	 */
	inline void setUpTrxId(TrxId upTrxId) {
		m_upTrxId = upTrxId;
	}

	/**
	 * ��ò��ɼ�����ID������
	 * @return 
	 */
	inline TrxId getLowTrxId() const {
		return m_lowTrxId;
	}

	/**
	 * ��ÿɼ�����ID������
	 * @return 
	 */
	inline TrxId getUpTrxId() const {
		return m_upTrxId;
	}

	/**
	 * ����ͼ����ȫ����ͼ�б���
	 * @param readViewList
	 * @return 
	 */
	inline void relatedWithList(DList<ReadView*> *readViewList) {
		readViewList->addFirst(&m_rvLink);
	}
	/**
	 * ����ͼ��ȫ����ͼ�б���ɾ��
	 */
	inline void excludeFromList() {
		assert(m_rvLink.getList());
		m_rvLink.unLink();
	}

	/**
	 * ����readView����СLSN
	 * @param upTrxId
	 */
	inline void setMinStartLsn(LsnType minStartLsn) {
		m_minStartLsn = minStartLsn;
	}


	/**
	 * ��ȡreadView����СLSN
	 * @param upTrxId
	 */
	inline LsnType getMinStartLsn() {
		return m_minStartLsn;
	}

	bool isVisible(TrxId trxId);

	void print();

private:
	TrxId   m_upTrxId;// ���� < ��ֵ������id�������޸ģ���ǰreadviewһ���ɼ�
	TrxId	m_lowTrxId;// ���� >= ��ֵ������id�������޸ģ���ǰreadview�����ɼ�
	TrxId	m_createTrxId;// ������readview������id
	/** 
	 * ��readview����ʱ�Ļ�Ծ��������б�(���������)��
	 * ���б��е������������޸ģ������ɼ�
	 * ����id�����б��а��մӴ�С��˳������
	 * m_trxCnt��ʶ�б��е������� */
	uint	m_trxCnt;
	TrxId	*m_trxIds;
	DLink<ReadView*> m_rvLink; // ��������ȫ�ֵ�������ͼ
	LsnType m_minStartLsn;
};

//��¼���մ����ʱ�Ѿ������յĴ����
struct TblLob {
	TblLob() {}

	TblLob(u16 tableId, u64 lobId) {
		m_tableId = tableId;
		m_lobId = lobId;
	}

	u16		m_tableId; //���մ��������Ӧ�ı�
	u64     m_lobId;   //���մ�����id
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
	CS_NONE,    //��ʼֵ
	CS_NORMAL,  //�����ύ����
	CS_INNER,   //�ڲ������ύ
	CS_RECOVER, //recover���ύ������
	CS_PURGE,   //Purge����
	CS_DUMP,	//dump����
	CS_RECLAIMLOB,  //reclaimLob����
	CS_DEFRAG_HASHINDEX, //defrag hashIndex����
	CS_BACKUP       //��������
};

enum RollBackStat {
	RBS_NONE,    //��ʼֵ
	RBS_NORMAL,  //����rollback������ָ���ϲ����rollback
	RBS_TIMEOUT, //��Ϊtimeout�����rollback
	RBS_DEADLOCK, //������ⱻѡΪ�����������rollback
	RBS_INNER,  //�ڲ����������rollback������ָddl
	RBS_RECOVER, //recover�����������rollback
	RBS_DUPLICATE_KEY, //Ψһ��������ͻ
	RBS_ROW_TOO_LONG,  //��¼̫��
	RBS_ABORT,         //���񱻷���
	RBS_OUT_OF_MEMORY,  //�ڴ治��������rollback
	RBS_ONLINE_DDL
};

/** TNT���� */
class TNTTransaction {
public:
	TNTTransaction();
	~TNTTransaction() {}

	void init(TNTTrxSys *trxSys, Txnlog *tntLog, MemoryContext *ctx, TrxId trxId, size_t poolId);
	void destory();
	void reset();

	//////////////////////////////////////////////////////////////////////////////////
	///	����ӿ�
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
	/// ��������ؽӿ�
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
	/// ��־��ؽӿ�
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

	//���ڻָ�
	inline void setReadOnly(bool readOnly) {
		m_readOnly = readOnly;
	}

	/**
	 * ������һ����־��LSN
	 * @param lsn
	 */
	inline void setTrxLastLsn(LsnType lsn) {
		m_lastLSN = lsn;
	}

	/**
	 * �������ID
	 * @return 
	 */
	inline TrxId getTrxId() const { 
		return m_trxId;
	}

	/**
	 * ������������
	 * @return 
	 */
	inline uint getErrState() const { 
		return m_errState;
	}

	/**
	 * ����������ڵȴ�����
	 * @return
	 */
	inline TLock* getWaitLock() const { 
		return m_lockOwner->getWaitLock();
	}

	/**
	 * ����ϲ�mysql������id
	 * @return 
	 */
	inline const XID* getTrxXID() const {
		return &m_xid;
	}

	/**
	 * �����������ʱ����־���к�
	 * @return 
	 */
	inline LsnType	getTrxRealStartLsn() const {
		return m_realStartLsn;
	}

	/**
	 * �������ʼ����־���к�
	 * @return 
	 */
	inline LsnType	getTrxBeginLsn() const {
		return m_beginLSN;
	}

	/**
	 * �����������һ����־���к�
	 * @return 
	 */
	inline LsnType getTrxLastLsn() const { 
		return m_lastLSN;
	}

	/**
	 * ����������伶��ͼ
	 * @return 
	 */
	inline ReadView* getReadView() const { 
		return m_readView;
	}

	/**
	 * �����������伶��ͼ
	 * @param view 
	 */
	inline void setReadView(ReadView *view) {
		m_readView = view;
	}

	/**
	 * ���������ͼ
	 * @return 
	 */
	inline ReadView* getGlobalReadView() const {
		return m_globalReadView;
	}

	/**
	 * ����������ͼ
	 * @param view
	 */
	inline void setGlobalReadView(ReadView *view) {
		m_globalReadView = view;
	}

	/**
	 * �������״̬
	 * @return 
	 */
	inline TrxState getTrxState() const {
		return m_trxState;
	}

	/**
	 * ��������״̬
	 * @param trxState
	 */
	inline void setTrxState(TrxState trxState) {
		m_trxState = trxState;
	}

	/**
	* ��ȡ��������ʹ���еı�����
	* @return
	*/
	inline uint getTableInUse() const {
		return m_tablesInUse;
	}

	/**
	* ��ȡ����ĸ��뼶��
	* @return
	*/
	inline TrxIsolationLevel getIsolationLevel() const {
		return m_isolationLevel;
	}

	/**
	* ���ص�ǰ�����Ƿ��Ѿ�ע�ᵽXA������
	* @return
	*/
	inline bool getTrxIsRegistered() const {
		return m_trxIsRegistered;
	}

	/**	
	* ��������ĸ��뼶��
	* @return
	*/
	inline void setIsolationLevel(TrxIsolationLevel isoLevel) {
		m_isolationLevel = isoLevel;
	}

	/**
	 * ��������ʼ����־���к�
	 * @param lsn
	 * @return 
	 */
	inline void setTrxBeginLsn(LsnType lsn) {
		m_beginLSN = lsn; 
	}
	
	/**
	* ���������XAע���ʶ
	* @param isRegistered
	*/
	inline void setTrxRegistered(bool isRegistered) {
		m_trxIsRegistered = isRegistered;
	}

	/**
	 * ����binlogλ����Ϣ
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
	 * ����������мӱ����ı���Ŀ
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
	 * ��������Ӧ�İ汾��ID
	 * @return 
	 */
	inline u8 getVersionPoolId() {
		return m_versionPoolId;
	}

	/**
	 * �������ʹ�õİ汾��ID
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
	 * ��ûָ��лع�����LobId hash
	 * @return 
	 */
	inline TblLobHashMap* getRollbackInsertLobHash() {
		return m_insertLobs;
	}

	/**
	 * �ͷŻָ��лع�����LobId hash
	 * @return 
	 */
	inline void releaseRollbackInsertLobHash() {
		m_insertLobs->~TblLobHashMap();
		m_insertLobs = NULL;
	}

	/**
	 * ��ʼ���ع�����LobId hash
	 * @return 
	 */
	inline void initRollbackInsertLobHash() {
		m_insertLobs = new (m_memctx->alloc(sizeof(TblLobHashMap))) TblLobHashMap();;
	}
	
	/**
	 * ��������ʼ�ع�����־��LSN
	 * @param lsn
	 */
	inline void setTrxBeginRollbackLsn(LsnType lsn) {
		m_beginRollbackLsn = lsn;
	}

	/**
	 * �������ʼ�ع���LSN
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

	/* ���ñ�ʶ��ǰ������Lock Tables�����֮�� */
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
	DLink<TNTTransaction*> 	m_trxLink;// ϵͳ�����е�����������ϵͳ�����֮��
	TNTTrxSys	*m_tranSys; // �������������������
	const char  *m_opInfo;  // ���������Ϣ
	TrxId	    m_trxId;	// ����id 
	TrxIsolationLevel m_isolationLevel;	// �����������뼶��
	TrxFlushMode m_flushMode; //����ˢ��־�ķ�ʽ
	
	/** XA֧�����裬�ϲ�mysql������id����prepareʱ��ã�����д��prepare��־�����ݴ�xid��ϵͳ����ʱ��
	 *  ���Իָ���binlog��innodb��־һ�µ�״̬; ͬʱ��Ҳ��*_by_xid����Ⱥ��һ���������; */
	XID		   m_xid;
	bool	   m_supportXA; // �����Ƿ�֧�ֶ��׶��ύ
	TrxState   m_trxState;  // ����״̬��not_active/active/prepare/commit
	void	   *m_thd;	    // ��������thread

	uint			m_threadId;// ����������mysql�߳�id
	uint			m_processId;// ���������Ĳ���ϵͳ����id

	MemoryContext   *m_memctx; // ���ڱ��������������ص��ڴ�ʹ��
	u64             m_sp;
	DldLockOwner    *m_lockOwner;// ��������������
	Connection		*m_conn;

	// ����ʱ����δ���duplicate��DUP_IGNORE��DUP_REPLACE
	// ��extra���������ã���write_row�Ⱥ����ж�ȡ
	uint			m_duplicates;
	// ��ʶ��Ծ�����״̬��innodb�У������Ƿ����prepare mutex
	uint			m_trxActiveStat;
	// ��ǰstatement���ж��ٱ���ʹ�ã����ٱ�����
	// external_lock�����ж�m_tablesInUse������ȷ��
	// ��ǰstatement�Ƿ�������Ƿ����autocommit
	uint			m_tablesInUse;
	uint			m_tablesLocked;

	// �жϵ�ǰ����������Lock Tables�����֮��
	// ���ǵĻ������м����Ͳ���Ҫ����
	// ע�⣺m_inLockTables��MySQL�ϲ��Lock Tables���һ��һ��
	// ��Ϊ��AutoCommit = 1ʱ��Lock Tables������Ч
	bool			m_inLockTables;

	// ��������Ӧ����䣬�Լ����ĳ���
	// innodb�ڴ�������ʱ����ָ��ָ��thd�ж�Ӧ������
	char			**m_queryStr;
	uint			*m_queryLen;
	
	ReadView        *m_globalReadView;// ������ͼ
	ReadView		*m_readView;      // ��伶��ͼ

	// autoinc fields
	// autoinc���ṹ���Լ�������Ҫ��autoincֵ������
	// TLock			*m_autoIncLock;
    // uint			m_autoIncCnt;

	// ����binlog״̬�£���¼binlog��
	// �ļ�����ǰд��λ�õ�trx_sys_header��
	// innodb��trx_commit_off_kernel������ʵ�ִ˹���
	// ����Ŀǰ���������¼����ֵ�Ĺ���
	const char		*m_binlogFileName;
	u64				m_binlogPosition;
	
	// log fields��ֻ��redo��־��TNT��undoͨ���汾��ʵ��
	// �����������������������һ����־���к�
	// ����ͬһ�����redo����������������һ������undo����
	// ����1����������undoʱ���������������ҵ�������־������undo
	// ����2��crash recoveryʱ��ͬ�������ش���������undo
	LsnType			m_lastLSN;
	LsnType         m_lastStmtLsn;

	LsnType			m_realStartLsn; // ��������ʱ��Ӧ����־tailLsn����������dumpLSN
	LsnType			m_beginLSN;// ����ʼ����־���к�
	LsnType         m_commitLsn;// �����ύ����־���к�
	LsnType			m_beginRollbackLsn; //����ʼ�ع�����־���к�

	u8				m_versionPoolId;// ���������İ汾�ر�ID
	Txnlog          *m_tntLog;// ������־ģ��
	bool            m_logging;// �Ƿ���Ҫд��־
	
	// ������
	// errState��������룻errInfo��duplicate����ʱ����¼�����index
	// errInfo����info�����е��ã�info�������ΪHA_STATUS_ERRKEY
	uint			m_errState;
	void			*m_errInfo;

	// ��ʶ��ǰ�����Ƿ�ע�ᵽMySQL�ڲ�XA�����У�true��ʾ��ע�᣻false��ʾδע��
	bool			m_trxIsRegistered;

	size_t          m_trxPoolId;// ���ڱ�ʾ���������������е�ID
	//bool            m_isPurge;  // �Ƿ���Purge����
	bool            m_valid;    // ��������Ƿ���Ч
	uint            m_magicNum; // ����У����������Ƿ�Ϸ���ħ��

	TblLobHashMap   *m_insertLobs;  //��¼������ع����մ��������б������lobId����

	bool            m_readOnly;
	DldLockResult   m_lastRowLockStat; //��¼�ϴμ��������ص�״̬

	u32             m_beginTime; //����ʼʱ��
	u32             m_redoCnt;   //����redo���ܸ���

	bool			m_waitingLock;		//�����Ƿ����ڵ���
	u32				m_waitStartTime;	//����ʼ������ʱ��

	bool            m_hangByRecover; //�Ƿ�Ϊrecover����������񣬼��ⲿxa parepre������recover�����ȱ���Ϊ����
	u32             m_hangTime;
	
	friend class TNTTrxSys;
};

struct TNTTrxSysStat {
	u64 m_commit_normal; //commit������ܸ���
	u64 m_commit_inner;  //�ڲ��ύ������ܸ���
	u64 m_rollback_normal; //�û�����rollback������ܸ���
	u64 m_rollback_timeout; //timeout�����rollback������ܸ���
	u64 m_rollback_deadlock;//���deadlock����rollback������ܸ���
	u64 m_rollback_duplicate_key;//duplicate key�����rollback������ܸ���
	u64 m_rollback_row_too_long; //��¼���������rollback������ܸ���
	u64 m_rollback_abort;  //���񱻷��������rollback������ܸ���
	u64 m_rollback_inner; //ͳ���ڲ�����rollback�ĸ���
	u64 m_rollback_recover;//ͳ��recover������rollback����ĸ���
	u64 m_rollback_out_of_mem; //ͳ�����ڴ治��������rollback����ĸ���
	u64 m_partial_rollback_normal; //�û�����rollback������ܸ���
	u64 m_partial_rollback_timeout; //timeout�����rollback������ܸ���
	u64 m_partial_rollback_deadlock;//���deadlock����rollback������ܸ���

	u32 m_maxTime;  //���������Ӧʱ��
	u32 m_avgLockCnt; //������ƽ������
	u32 m_maxLockCnt; //�������������
	u32 m_avgRedoCnt; //����ƽ��redo��¼����
	u32 m_maxRedoCnt; //�������redo��¼����
};

/** TNT������� */
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

	// ���ָ�����ʹ��
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
	 * ���������������
	 * @return 
	 */
	inline TLockSys* getLockSys() const {
		return m_lockSys;
	}

	/**
	 * ������������ʱʱ��
	 * @param lockTimeoutMs ��������ʱʱ�䣬��λ����
	 */
	inline void setLockTimeout(int lockTimeoutMs) {
		m_lockSys->setLockTimeout(lockTimeoutMs);
	}

	/**
	 * ���õ�ǰ����ʹ�õİ汾��ID
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
	// �������ײ�ʵ��
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
	Mutex					m_transMutex;/** ����ȫ�ֽṹ���� */
	uint                    m_maxTrxNum; /** ֧�ֵ�����Ծ������ */
	TrxFlushMode            m_trxFlushMode; /** ����ˢ��־�ķ�ʽ */
	TLockSys	            *m_lockSys;  /** ȫ�ֵ������������� */
	TrxId					m_maxTrxId;  /** ��ǰϵͳ��Сδʹ��ID���´οɷ��� */
	DList<TNTTransaction *>	m_activeTrxs;/** ��Ծ����������������id�������� */
	DList<TNTTransaction *> m_activeInnerTrxs; /** ��Ծ���ڲ�����������������id�������� */
	DLink<TNTTransaction *> *m_recoverLastTrxPos; /** ������getPreparedTrxForMysql�����ڱ�ʶ�´β��ҿ�ʼ��δ֪*/
	DList<ReadView*>		m_activeReadViews;/** ��Ծ�����read_view�б� */
	DList<MemoryContext*>   m_freeMemCtxList;/** ���е��ڴ�����������б� */
	TNTDatabase             *m_db;           /** ���ݿ���� */
	ObjectPool<TNTTransaction> m_freeTrxPool;/** ���ڷ�ֹ�ڴ���Ƭ������������Ч�� */
	//Connection              *m_dummyConn;    /** �ڲ����� */
	//Session                 *m_dummySession; /** �ڲ��Ự */
	uint                    m_activeVerPoolId;/** ��ǰ��Ծ�汾��ID */

	//�����ǹ��������ͳ����Ϣ
	TNTTrxSysStat           m_stat;
	u64                     m_trxCnt;   //rollback��commit������ܸ���
	u64                     m_totalLockCnt; //�������������ܺ�
	u64                     m_totalRedoCnt; //��������redo��¼���ܺ�

	bool                    m_hasHangTrx;
friend class TNTTransaction;
};

}
#endif