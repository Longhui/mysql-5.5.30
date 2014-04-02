/**
 * �����ӿ�
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TABLE_H_
#define _NTSE_TABLE_H_

#include "api/Database.h"
#include "misc/Global.h"
#include "misc/Record.h"
#include "util/Array.h"
#include "lob/Lob.h"
#include "mms/Mms.h"
#include "misc/TableDef.h"
#include "misc/Session.h"
#include "util/Thread.h"
#include "rec/Records.h"


#ifdef NTSE_UNIT_TEST
class TableTestCase;
#endif

#ifdef TNT_ENGINE
namespace tnt {
	class TNTTable;
}
#endif

namespace ntse {

/** �޸���Ϣ����ʾUPDATE/DELETE��乲ͬ�Ĳ������� */
struct TSModInfo {
public:
	TSModInfo(Session *session, const TableDef *tableDef, const ColList &updCols, const ColList &readCols);
	void prepareForUpdate(const SubRecord *redSr);
	
public:
	ColList			m_missCols;			/** ����֮ǰ��Ҫ���������(Ŀǰ�������漰��ɨ��ʱδ��ȡ����������) */
	bool			m_updIndex;			/** �Ƿ�������� */
	SubRecord		*m_indexPreImage;	/** UPDATE/DELETEʱ����������ǰ�񲿷ּ�¼�����ڴ洢��¼���ڴ����ⲿָ�� */
};

class IndexScanHandle;
class DrsHeapScanHandle;
class Session;
class SubRecord;
class RowLockHandle;
struct MmsRecord;
class DrsIndex;
class Table;
class TblScan {
private:
	TblScan();

public:
	ScanType getType() const {
		return m_type;
	}
	RowId getCurrentRid() const {
		return m_redRow->m_rowId;
	}
	SubToSubExtractor* getIdxExtractor() const {
		return m_idxExtractor;
	}
	bool getCoverageIndex() const {
		return m_coverageIndex;
	}
	SubRecord* getIdxKey() const {
		return m_idxKey;
	}
	SubRecord* getMysqlRow() const {
		return m_mysqlRow;
	}
	SubRecord* getRedRow() const {
		return m_redRow;
	}
	Records::BulkOperation* getRecInfo() const {
		return m_recInfo;
	}
	ColList getReadCols() const {
		return m_readCols;
	}

	IndexScanHandle* getDrsScanHandle() const {
		return m_indexScan;
	}

	void setCurrentData(byte *redData) {
		m_redRow->m_data = redData;
	}	
	void setUpdateColumns(u16 numCols, u16 *columns);
	void setPurgeUpdateColumns(u16 numCols, u16 *columns);
	bool isUpdateColumnsSet() const { return m_updInfo != NULL; }
	static StatType getTblStatTypeForScanType(ScanType scanType);
	static StatType getRowStatTypeForScanType(ScanType scanType);
	bool checkEofOnGetNext();

private:
	void prepareForUpdate(const byte *mysqlUpdate, const byte *oldRow, bool isUpdateEngineFormat) throw(NtseException);
	void prepareForDelete();
	void initDelInfo();
	void determineRowLockPolicy();

	Session		*m_session;			/** �Ự */
	ScanType	m_type;				/** ɨ������ */
	OpType		m_opType;			/** �������� */
	Table		*m_table;			/** �� */
	TableDef	*m_tableDef;		/** ���� */
	ColList		m_readCols;			/** �ϲ�ָ��Ҫ��ȡ������ */
	bool		m_bof;				/** �Ƿ�û��ʼɨ�� */
	bool		m_eof;				/** �Ƿ��Ѿ�ɨ�赽ĩβ */

	LockMode	m_rowLock;			/** ��¼��ģʽ */
	RowLockHandle	**m_pRlh;		/** ָ�����������ָ�� */
	RowLockHandle	*m_rlh;			/** ������� */
	
	bool		m_tblLock;			/** Ϊtrue��ʾ��ɨ������Զ��ӱ�����Ϊfalse��ʾ�����߸���ӱ��� */
	StatType	m_scanRowsType;		/** ��������ɨ������ */
	SubRecord	*m_mysqlRow;		/** ��ǰ�У�REC_MYSQL��ʽ */
	SubRecord	*m_redRow;			/** ��ǰ�У�REC_REDUNDANT��ʽ */
	TSModInfo	*m_delInfo;			/** DELETE���õĸ�����Ϣ */
	TSModInfo	*m_updInfo;			/** UPDATE���õĸ�����Ϣ */

	bool		m_singleFetch;		/** �Ƿ�Ϊָ��Ψһ��������ֵ�����Ļ�ȡ������¼���� */
	IndexDef	*m_pkey;			/** ���� */
	const SubRecord	*m_indexKey;	/** ������ */
	IndexScanHandle	*m_indexScan;	/** ����ɨ���� */
	IndexDef	*m_indexDef;		/** ����ɨ����������� */
	DrsIndex	*m_index;			/** ����ɨ������� */
	bool		m_coverageIndex;	/** �������Ƿ���������������� */
	SubRecord	*m_idxKey;			/** ���ڴ������ж�ȡ���� */
	SubToSubExtractor	*m_idxExtractor;	/** ����������ȡ�Ӽ�¼����ȡ�� */

	Records::BulkOperation	*m_recInfo;
	
friend class Table;
friend Tracer& operator << (Tracer& tracer, const TblScan *scan);
};


/** INSERT ... ON DUPLICATE KEY UPDATE��REPLACE�������� */
#ifdef TNT_ENGINE
template <typename T>
#endif
class IUSequence {
public:
	void getDupRow(byte *buf) {
		memcpy(buf, m_mysqlRow, m_tableDef->m_maxRecSize);
	}
#ifdef TNT_ENGINE
	T getScanHandle() {
#else
	TblScan *getScanHandle() {
#endif
		return m_scan;
	}

	uint getDupIndex() {
		return m_dupIndex;
	}

	byte *getMysqlRow() {
		return m_mysqlRow;
	}

private:
	TableDef	*m_tableDef;	/** ���� */
	byte		*m_mysqlRow;	/** ���³�ͻ�ļ�¼ */
	uint		m_dupIndex;		/** ���³�ͻ������ */
#ifdef TNT_ENGINE
	T           m_scan;
#else
	TblScan		*m_scan;		/** ɨ���� */
#endif
friend class Table;
#ifdef TNT_ENGINE
friend class tnt::TNTTable;
#endif
};

/** ����ɨ������ */
struct IndexScanCond {
	IndexScanCond(u16 idx, const SubRecord *key, bool forward, 
		bool includeKey, bool singleFetch) {
		m_idx = idx;
		m_key = key;
		m_forward = forward;
		m_includeKey = includeKey;
		m_singleFetch = singleFetch;
	}

	u16				m_idx;			/** ������� */
	const SubRecord *m_key;			/** ɨ����ʼ��ֵ */
	bool			m_forward;		/** �Ƿ�Ϊǰ��ɨ�� */
	bool			m_includeKey;	/** �Ƿ����=m_key�ļ�¼ */
	bool			m_singleFetch;	/** �Ƿ�ΪΨһ�Ե�ֵɨ�� */
};

class DrsHeap;
class Database;
class DrsIndice;
class MmsTable;
class Session;
class LobStorage;
class MemoryContext;
struct LogEntry;
struct PreUpdateLog;
struct Stream;


/** ����� */
class Table {
public:
	///////////////////////////////////////////////////////////////////////////
	// DDL�����ӿ� ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	static void create(Database *db, Session *session, const char *path, TableDef *tableDef) throw(NtseException);
	static void drop(Database *db, const char *path) throw(NtseException);
	static void drop(const char *baseDir, const char *path) throw(NtseException);
	static Table* open(Database *db, Session *session, const char *path, bool hasCprsDict = false) throw(NtseException);
	static void checkRename(Database *db, const char *oldPath, const char *newPath, bool hasLob, bool hasCprsDict = false) throw(NtseException);
	static void rename(Database *db, Session *session, const char *oldPath, const char *newPath, bool hasLob, bool redo, bool hasCprsDict = false);
	void close(Session *session, bool flushDirty, bool closeComponents = false);
	void truncate(Session *session, bool tblLock = true, bool *newHasDic = NULL, bool isTruncateOper = true) throw(NtseException);
	void addIndex(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException);

	// TNT�������Ҫ����NTSE Table��addIndex�������޸�Ϊ���׶�����
	void addIndexPhaseOne(Session *session, u16 numIndice, const IndexDef **indexDefs) throw (NtseException);
	void addIndexPhaseTwo(Session *session, u16 numIndice, const IndexDef **indexDefs) throw(NtseException);

	// TNT�������Ҫ����NTSE Table��dropIndex�������޸�Ϊ���׶�����
	void dropIndexPhaseOne(Session *session, uint numIndice, ILMode oldLock) throw(NtseException);
	void dropIndexPhaseTwo(Session *session, uint numIndice, ILMode oldLock) throw(NtseException);
	void dropIndexFlushPhaseOne(Session *session) throw(NtseException);
	void dropIndexFlushPhaseTwo(Session *session, ILMode oldLock) throw(NtseException);


	void dropIndex(Session *session, uint idx) throw(NtseException);
	void optimize(Session *session, bool keepDict, bool *newHasDic, bool *cancelFlag) throw(NtseException);
	void verify(Session *session) throw(NtseException);
	void flush(Session *session);
	void flushComponent(Session *session, bool flushHeap, bool flushIndice, bool flushMms, bool flushLob);
	static void getSchemaTableFromPath(const char *path, char **schemaName, char **tableName);
	void setOnlineAddingIndice(Session *session, u16 numIndice, const IndexDef **indice);
	void resetOnlineAddingIndice(Session *session);
	void alterTableArgument(Session *session, const char *name, const char *valueStr, int timeout, bool inLockTables = false) throw (NtseException);
	void defragLobs(Session *session) throw (NtseException);

	void alterUseMms(Session *session, bool useMms, int timeout, bool redo = false) throw(NtseException);
	void alterCacheUpdate(Session *session, bool cachUpdate, int timeout, bool redo = false) throw(NtseException);
	void doAlterCacheUpdate(Session *session, bool cachUpdate, int timeout, bool redo = false) throw(NtseException);
	void alterCacheUpdateTime(Session *session, uint interval, int timeout, bool redo = false) throw(NtseException);
	void alterCachedColumns(Session *session, const char *valueStr, int timeout) throw(NtseException);
	void alterCachedColumns(Session *session, u16 *cols, u16 numCols, bool enable, int timeout, bool redo = false) throw(NtseException);
	void alterCompressLobs(Session *session, bool compressLobs, int timeout, bool redo = false) throw(NtseException);
	void alterHeapPctFree(Session *session, u8 pctFree, int timeout, bool redo = false) throw(NtseException);
	void alterSplitFactors(Session *session, const char *valueStr, int timeout) throw(NtseException);
	void alterSplitFactors(Session *session, u8 indexNo, s8 splitFactor, int timeout, bool redo = false) throw(NtseException);
	void alterIncrSize(Session *session, u16 incrSize, int timeout, bool redo = false) throw(NtseException);
	void alterCompressRows(Session *session, bool compressRows, int timeout, bool redo = false) throw(NtseException);
	void alterHeapFixLen(Session *session, bool fixlen, int timeout) throw(NtseException);
	void alterColGrpDef(Session *session, const char *valueStr, int timeout) throw(NtseException);
	void alterColGrpDef(Session *session, Array<ColGroupDef *> *colGrpDef, int timeout, bool redo = false) throw(NtseException);
	void alterDictionaryArg(Session *session, int cmdType, uint newValue, int timeout, bool redo = false) throw(NtseException);
#ifdef TNT_ENGINE
	void alterTableStatus(Session *session, TableStatus tableStatus, int timeout, bool inLockTables, bool redo = false) throw(NtseException);
#endif
	void checkPermission(Session *session, bool redo) throw(NtseException);
	static void checkPermissionReal(bool trxConn, TableStatus tblStatus, const char *path) throw(NtseException);

	bool hasOnlineIndex(string *idxInfo);
	bool isDoneOnlineIndex(const IndexDef **indexDefs, u16 numIndice);
	bool isOnlineIndexOrder(const IndexDef **indexDefs, u16 numIndice);
	void dualFlushAndBump(Session *session, int timeout) throw (NtseException);
	///////////////////////////////////////////////////////////////////////////
	// DML�����ӿ� ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	TblScan* tableScan(Session *session, OpType opType,
		u16 numReadCols, u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	TblScan* indexScan(Session *session, OpType opType, const IndexScanCond *cond,
		u16 numReadCols, u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	TblScan* positionScan(Session *session, OpType opType,
		u16 numReadCols, u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	bool getNext(TblScan *scan, byte *mysqlRow, RowId rid = INVALID_ROW_ID, bool needConvertToUppMysqlFormat = false);
	bool updateCurrent(TblScan *scan, const byte *update, bool isUpdateEngineFormat = true, uint *dupIndex = NULL, const byte *oldRow = NULL, void *cbParam = NULL) throw(NtseException);
	void deleteCurrent(TblScan *scan, void *cbParam = NULL);
	void endScan(TblScan *scan);

	RowId insert(Session *session, const byte *record, bool isRecordEngineFormat, uint *dupIndex, bool tblLock = true, void *cbParam = NULL) throw(NtseException);
#ifdef TNT_ENGINE
	RowId insert(Session *session, const byte *record, uint *dupIndex, RowLockHandle **rlh) throw(NtseException);
	void deleteLob(Session* session,  LobId lobId);
	void deleteLobAllowDupDel(Session* session,  LobId lobId);
	void tntLockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void tntUnlockMeta(Session *session, ILMode mode);

	IUSequence<TblScan *>* insertForDupUpdate(Session *session, const byte *record, bool tblLock = true, void *cbParam = NULL) throw(NtseException);
	bool updateDuplicate(IUSequence<TblScan *> *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex = 0, void *cbParam = NULL) throw(NtseException);
	void deleteDuplicate(IUSequence<TblScan *> *iuSeq, void *cbParam = NULL);
	void freeIUSequenceDirect(IUSequence<TblScan *> *iduSeq);
#else
	IUSequence* insertForDupUpdate(Session *session, const byte *record, bool tblLock = true, void *cbParam = NULL) throw(NtseException);
	bool updateDuplicate(IUSequence *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex = 0, void *cbParam = NULL) throw(NtseException);
	void deleteDuplicate(IUSequence *iuSeq, void *cbParam = NULL);
	void freeIUSequenceDirect(IUSequence *iduSeq);
#endif

	///////////////////////////////////////////////////////////////////////////
	// �����ӿ� ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	void lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void unlockMeta(Session *session, ILMode mode);
	ILMode getMetaLock(Session *session) const;
	const IntentionLockUsage* getMetaLockUsage() const;

	void lock(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void upgradeLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void downgradeLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void unlock(Session *session, ILMode mode);
	ILMode getLock(Session *session) const;
	const IntentionLockUsage* getLockUsage() const;

	///////////////////////////////////////////////////////////////////////////
	// �ָ��ӿ� /////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	static void redoTrunate(Database *db, Session *session, const LogEntry *log, const char *path, bool *newHasDict)  throw(NtseException);
	void redoDropIndex(Session *session, u64 lsn, const LogEntry *log);
	PreDeleteLog* parsePreDeleteLog(Session *session, const LogEntry *log);
#ifdef TNT_ENGINE		//tnt�й���ɾ����������־
	LobId parsePreDeleteLobLog(Session *session, const LogEntry *log);
	void writePreDeleteLobLog(Session *session, LobId lobId);
#endif
	RowId getRidFromPreDeleteLog(const LogEntry *log);
	static char* parseCreateLog(const LogEntry *log, TableDef *tableDef);
	IndexDef* parseAddIndexLog(const LogEntry *log);
	void redoFlush(Session *session);
	void redoAlterTableArg(Session *session, const LogEntry *log) throw (NtseException);
	static void redoAlterIndice(Session *session, const LogEntry *log, Database *db, const char *tablepath) throw (NtseException);
	static void redoAlterColumn(Session *session, const LogEntry *log, Database *db, const char *tablepath, bool *newHasDict);
	void redoMoveLobs(Session *session, LsnType lsn, const LogEntry *log) throw (NtseException);
	static void redoCreateDictionary(Session *session, Database *db, const char *tablePath);

	///////////////////////////////////////////////////////////////////////////
	// ͳ����Ϣ ///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	void refreshRows(Session *session);
	u64 getRows();
	u64 getDataLength(Session *session);
	u64 getIndexLength(Session *session, bool includeMeta = true);
	Array<DBObjStats*>* getDBObjStats();
	u64 getNumRowLocks(LockMode mode) {
		return m_rlockCnts[mode];  
	}

	void writeTableDef();
	///////////////////////////////////////////////////////////////////////////
	// ��¼ѹ���ӿ� ///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	/**
	* �Ƿ��м�¼ѹ���ֵ�
	*/
	inline bool hasCompressDict() {
		return m_hasCprsDict;
	}
	inline const RCDictionary * getCompressDict() {
		assert(m_records);
		return hasCompressDict() ? m_records->getDictionary() : NULL; 
	}
	void createDictionary(Session *session, ILMode *metaLockMode, ILMode *dataLockMode) throw(NtseException);

	///////////////////////////////////////////////////////////////////////////
	// �õ���ɱ�ĸ������� ////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////	
	/** �õ�����
	 * @param assertSafe �Ƿ����ڼ��˱�Ԫ��������İ�ȫ����
	 * @param session ��assertSafeΪtrue������Ự�����������NULL
	 * @return ����
	 */
	TableDef* getTableDef(bool assertSafe = false, Session *session = NULL) const {
		if (assertSafe) {
			UNREFERENCED_PARAMETER(session);
			assert(getMetaLock(session) != IL_NO || m_db->getStat() == DB_RECOVERING);
		}

		return m_tableDef;
	}

#ifdef TNT_ENGINE
	TableDef **getTableDefAddr() {
		return &m_tableDef;
	}
#endif

	void setTableDef(TableDef *tableDef) {
		m_tableDef = tableDef;
		m_tableDefWithAddingIndice = tableDef;
		m_records->setTableDef(tableDef);
	}
	/** �õ���ļ�¼������
	 * @return ��ļ�¼������
	 */
	Records* getRecords() const {
		return m_records;
	}
	/** �õ����Ӧ��MMSϵͳ
	 * @return ���Ӧ��MMSϵͳ
	 */
	MmsTable* getMmsTable() const {
		return m_records->getMms();
	}
	/** �õ����ڴ洢���м�¼�ĶѶ���
	 * @return �Ѷ���
	 */
	DrsHeap* getHeap() const {
		return m_records->getHeap();
	}
	/** �õ����ڴ洢�����������ݵ���������
	 * @return ��������
	 */
	DrsIndice* getIndice() const {
		return m_indice;
	}
	/** �õ����ڴ洢���д�������ݵĴ����洢����
	 * @return �����洢����
	 */
	LobStorage* getLobStorage() const {
		return m_records->getLobStorage();
	}
	/** �õ����ļ�·��
	 * @return ���ļ�·��
	 */
	const char* getPath() const {
		return m_path;
	}
	
	/** �õ����Ԫ������
	*	@return ��Ԫ������
	*/
	IntentionLock* getMetaLockInst() {
		return &m_metaLock;
	}

	/** ���õ�ǰ�����ʱ������
	 * @param mysqlTmpTable	true��ʾ��mysql����ʱ��false��ʾ����
	 */
	void setMysqlTmpTable(bool mysqlTmpTable) {
		m_mysqlTmpTable = mysqlTmpTable;
	}
	
	/** �õ����openTableLink �����
	*	@return ���������
	 */
	DLink<Table *> &getOpenTablesLink()  {
		return m_openTablesLink;
	}

private:
	Table(Database *db, const char *path, Records *records, TableDef *tableDef, bool hasCprsDict);
	TblScan* beginScan(Session *session, ScanType type, OpType opType,
		u16 numReadCols, u16 *readCols, bool tblLock);
	bool isCoverageIndex(const TableDef *table, const IndexDef *index, const ColList &cols);

	/** �ͷ����һ����¼ռ�õ���Դ
	 *
	 * @param scan ɨ����
	 * @param retainLobs �Ƿ������������
	 */
	void releaseLastRow(TblScan *scan, bool retainLobs) {
		if (scan->m_rlh)
			scan->m_session->unlockRow(&scan->m_rlh);
		if (scan->m_recInfo)
			scan->m_recInfo->releaseLastRow(retainLobs);
	}
	TblScan* allocScan(Session *session, u16 numCols, u16 *columns, OpType opType, bool tblLock);
	void stopUnderlyingScan(TblScan *scan);
	void verifyRecord(Session *session, RowId rid, const SubRecord *expected) throw(NtseException);
	void verifyRecord(Session *session, const Record *expected);
	void verifyDeleted(Session *session, RowId rid, const SubRecord *indexAttrs);
	bool checkLock(Session *session, OpType opType);
	void lockTable(Session *session, OpType opType) throw(NtseException);
	void unlockTable(Session *session, OpType opType);
	
	// ��־���
	void writePreDeleteLog(Session *session, RowId rid, const byte *row, const SubRecord *indexPreImage);
	void writeAddIndexLog(Session *session, const IndexDef *indexDef);
	u64 writeTruncateLog(Session *session, const TableDef *tableDef, bool hasDict, bool isTruncateOper);
	static void parseTruncateLog(const LogEntry *log, TableDef** tableDefOut, bool *hasDict, bool *isTruncateOper);

	u64 writeAlterIndiceLog(Session *session, const TableDef *newTableDef, const char* relativeIdxPath);
	static void parseAlterIndiceLog(const LogEntry *log, TableDef **tableDef, char** relativeIdxPath);
	u64 writeAlterColumnLog(Session *session, const char* tmpTablePath, bool hasLob, bool hasDict);
	static void parseAlterColumnLog(const LogEntry *log, char** tmpTablePath, bool* hasLob, bool *hasDict);

	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, uint newValue);
	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, u16 *cols, u16 numCols, bool enable);
	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, u8 indexNo, s8 splitFactor);
	u64 writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, Array<ColGroupDef *> *colGrpDefArr);

	u64 writeCreateDictLog(Session *session);


	void replaceComponents(const Table *another);
	void setTableId(Session *session, u16 tableId);

	//�޸ı������
	void preAlter(Session *session, int cmdType, uint newValue, int timeout) throw (NtseException);
	void createTmpCompressDictFile(const char *dicFullPath, const RCDictionary *tmpDict) throw (NtseException);
	RCDictionary *extractCompressDict(Session *session) throw (NtseException); 

	void shareFlush(Session *session);
	void dualFlush(Session *session, int timeout) throw(NtseException) ;

	static void binlogMmsUpdate(Session *session, SubRecord *dirtyRecord, void *data);
private:
	Database	*m_db;			/** �������ݿ� */
	Records		*m_records;		/** ��¼������ */
	TableDef	*m_tableDef;	/** ���� */
	TableDef	*m_tableDefWithAddingIndice;	/** �������ڴ����������ı��壬����m_tableDef��ͬ���ʾ���ڴ������������� */
	DrsIndice	*m_indice;		/** DRS���� */
	const char	*m_path;		/** ���ļ�·�� */
	IntentionLock	m_metaLock;	/** Ԫ������ */
	IntentionLock	m_tblLock;	/** ���� */
	s64			m_estimateRows;	/** �����ж�������¼ */
	u64			m_rlockCnts[Exclusived + 1];	/** ���������ļ������� */
	struct MMSBinlogCallBack *m_mmsCallback;	/** MMS��¼binlog����Ҫ�Ļص����� */
	bool		m_mysqlTmpTable;				/** �Ƿ���Mysql�ϲ�ָ������ʱ�� */
	bool        m_hasCprsDict;                  /** �Ƿ���ȫ���ֵ��ļ� */
	DLink<Table *>	m_openTablesLink;		/** ��LRU�����е������ */

	friend class TblScan;
	friend class TableOnlineMaintain;
	friend class TblMntAlterIndex;
	friend class TblMntAlterColumn;
#ifdef NTSE_UNIT_TEST
public:
	friend class ::TableTestCase;
#endif
};

/** �����������Զ��ͷű��� */
struct TblLockGuard {
	TblLockGuard() {
		m_session = NULL;
	}
	~TblLockGuard() {
		if (!m_session)
			return;
		if (m_table->getLock(m_session) != IL_NO)
			m_table->unlock(m_session, m_table->getLock(m_session));
		if (m_table->getMetaLock(m_session) != IL_NO)
			m_table->unlockMeta(m_session, m_table->getMetaLock(m_session));
		m_session = NULL;
		m_table = NULL;
	}
	void attach(Session *session, Table *table) {
		m_session = session;
		m_table = table;
	}
	void detach() {
		m_session = NULL;
		m_table = NULL;
	}

private:
	Session	*m_session;
	Table	*m_table;
};

}

#endif

