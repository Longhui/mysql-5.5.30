/**
* TNT�����ģ�顣
*
* @author �εǳ�
*/
#ifndef _TNTTABLE_H_
#define _TNTTABLE_H_

#include "misc/TableDef.h"
#include "api/TNTTblScan.h"
#include "api/Table.h"
#include "btree/TNTIndex.h"
#include "rec/MRecords.h"

namespace tnt {

class MRecords;
class TNTDatabase;
class TNTTableStat;
class TNTTable;

enum PurgeStatus {
	PS_NOOP,
	PS_PURGEPHASE1,
	PS_PURGECOMPENSATE,
	PS_PURGEPHASE2
};

// TNT Table�Ļ�����Ϣ��������TNT Heap��Rowid Hash��Indice�����ڴ��е�λ����Ϣ
class TNTTableBase {
public:
	u16                     m_tableId;
	DLink<TNTTableBase *>	m_tableBaseLink;
	MRecords				*m_records;			// �ڴ�Heap
	TNTIndice				*m_indice;			// �ڴ�Indice
	bool					m_opened;			// TableBase�Ƿ��Ѿ���
	bool					m_closed;			// TableBase�Ƿ��Ѿ��ر�

public:
	TNTTableBase() {
		m_tableId = TableDef::INVALID_TABLEID;
		m_tableBaseLink.set(this);
		m_records	= NULL;
		m_indice	= NULL;
		m_opened	= false;
		m_closed	= false;
	}

	~TNTTableBase() {
		assert(m_records == NULL && m_indice == NULL);
	}

	// open/close TNTIM�ڴ�ṹ�������ڴ�ѣ��ڴ�����
	void open(Session *session, TNTDatabase *db, TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice)throw(NtseException);
	void close(Session *session, bool flush);
};

struct TNTTableInfo {
public:
	TNTTable	*m_table;		// TNT tableʵ��
	char		*m_path;		// TNT table��Ӧ��NTSE��·��
	uint		m_refCnt;		// open���ü���

public:
	TNTTableInfo(TNTTable *table, const char *path) {
		m_table = table;
		m_path = System::strdup(path);
		m_refCnt = 1;
	}

	~TNTTableInfo() {
		delete []m_path;
		m_table = NULL;
		m_path = NULL;
	}
};

class TNTTable {
public:
	~TNTTable();

	// get & set
	/** ���õ�ǰ�����ʱ������
	 * @param mysqlTmpTable	true��ʾ��mysql����ʱ��false��ʾ����
	 */
	void setMysqlTmpTable(bool mysqlTmpTable) {
		m_mysqlTmpTable = mysqlTmpTable;
	}
	void refreshRows(Session* session);

	// ddl operation
	static void create(TNTDatabase *db, Session *session, 
		const char *path, TableDef *tableDef) throw(NtseException);
	static void drop(TNTDatabase *db, const char *path) throw(NtseException);
	static TNTTable* open(TNTDatabase *db,Session *session, 
		Table *ntseTable, TNTTableBase *tableBase) throw(NtseException);
	static void rename(TNTDatabase *db, Session *session, const char *oldPath, 
		const char *newPath);
	void close(Session *session, bool flushDirty, bool closeComponents = false);	
	void truncate(Session *session);

	void addIndex(Session *session, const IndexDef **indexDefs, u16 numIndice) throw(NtseException);
	void dropIndex(Session *session, uint idx) throw(NtseException);

	void optimize(Session *session, bool keepDict, bool *newHasDic, bool *cancelFlag) throw(NtseException);
	void verify(Session *session) throw(NtseException);
	void flush(Session *session);
	// ���ߴ�����������Ҫ֪��TNT��TNT����purge�ڴ�+ntse���ߴ��������ķ�ʽ���
	void setOnlineAddingIndice(Session *session, u16 numIndice, const IndexDef **indice);
	void resetOnlineAddingIndice(Session *session);
	void alterTableArgument(Session *session, const char *name, const char *valueStr, 
		int timeout, bool inLockTables) throw(NtseException);
	void alterSupportTrxStatus(Session *session, TableStatus tableStatus, int timeout, bool inLockTables) throw(NtseException);
	void alterHeapFixLen(Session *session, bool fixlen, int timeout) throw(NtseException);
		
	// dml operation
	TNTTblScan* tableScan(Session *session, OpType opType, TNTOpInfo *opInfo, u16 numReadCols, 
		u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	TNTTblScan* indexScan(Session *session, OpType opType, TNTOpInfo *opInfo, const IndexScanCond *cond, 
		u16 numReadCols, u16 *readCols, bool unique = false, bool tblLock = true, 
		MemoryContext *lobCtx = NULL) throw(NtseException);
	TNTTblScan* positionScan(Session *session, OpType opType, TNTOpInfo *opInfo, u16 numReadCols, 
		u16 *readCols, bool tblLock = true, MemoryContext *lobCtx = NULL) throw(NtseException);
	bool getNext(TNTTblScan *scan, byte *mysqlRow, RowId rid = INVALID_ROW_ID, bool needConvertToMysqlUppFormat = false) throw(NtseException);
	bool updateCurrent(TNTTblScan *scan, const byte *update, 
		const byte *oldRow = NULL, uint *dupIndex = NULL, bool fastCheck = true) throw(NtseException);
	void deleteCurrent(TNTTblScan *scan) throw(NtseException);
	void endScan(TNTTblScan *scan);

	RowId insert(Session *session, const byte *record, uint *dupIndex, TNTOpInfo *opInfo, bool fastCheck = true) throw(NtseException);
	void buildMemIndexs(Session *session, MIndice *indice);
		
	// duplicate insert/update������
	bool checkDuplicate(Session *session, TableDef *m_tableDef, RowId rowId, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum, bool fastCheck = true) throw (NtseException);
	// bool checkDuplicateFast(Session *session, TableDef *m_tableDef, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw (NtseException);
	bool checkDuplicateSlow(Session *session, TableDef *m_tableDef, RowId rowId, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw (NtseException);
	void transIndexCols(u16 maxCols, IndexDef *index, u16 *cols);

	IUSequence<TNTTblScan *>* insertForDupUpdate(Session *session, const byte *record, TNTOpInfo *opInfo) throw(NtseException);
	bool updateDuplicate(IUSequence<TNTTblScan *> *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex = 0) throw(NtseException);
	void deleteDuplicate(IUSequence<TNTTblScan *> *iuSeq) throw(NtseException);
	void freeIUSequenceDirect(IUSequence<TNTTblScan *> *iduSeq);
	
	// �����ӿڣ�TNT��Lockģ����ɣ��˴�����Ҫ����
	// ������Ҫ����meta lock����
	void lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void unlockMeta(Session *session, ILMode mode);
	static void unlockMetaWithoutInst(Session *session, Table *table, ILMode mode);
	ILMode getMetaLock(Session *session) const;
	// const ILUsage* getMetaLockUsage() const;

	//purge
	void purgePhase1(Session *session, TNTTransaction *trx);
	u64 purgePhase2(Session *session, TNTTransaction *trx);

	void reclaimMemIndex(Session *session); 

	inline bool canCoverageIndexScan() {
		return m_purgeStatus < PS_PURGEPHASE2;
	}
	
	// �ָ��ӿڣ�DDL�ָ���������ȫ����NTSE��ɼ���
	
	// statistics
	
	// TNT�������в���
	// 1. autoinc
	u64 getAutoinc();
	void updateAutoincIfGreater(u64 value);
	void initAutoinc(u64 value);
	// �������ƣ�mutex���Ż�·����lock������·����
	// ֻ���ڼ򵥵�insert/replace�﷨ʱ���ܹ�ʹ��mutex�Ż�·��
	void enterAutoincMutex();
	void exitAutoincMutex();

	//redo
	void redoInsert(Session *session, const LogEntry *log, RedoType redoType);
	void redoFirUpdate(Session *session, const LogEntry *log, RedoType redoType);
	void redoSecUpdate(Session *session, const LogEntry *log, RedoType redoType);
	void redoFirRemove(Session *session, const LogEntry *log, RedoType redoType);
	void redoSecRemove(Session *session, const LogEntry *log, RedoType redoType);

	ColList getUpdateIndexAttrs(const ColList &updCols, MemoryContext *ctx);
	LsnType undoInsertLob(Session *session, const LogEntry *log);
	LsnType undoInsert(Session *session, const LogEntry *log, const bool crash);
	LsnType undoFirUpdate(Session *session, const LogEntry *log, const bool crash);
	LsnType undoSecUpdate(Session *session, const LogEntry *log, const bool crash);
	LsnType undoFirRemove(Session *session, const LogEntry *log, const bool crash);
	LsnType undoSecRemove(Session *session, const LogEntry *log, const bool crash);

	static void parseInsertLob(const LogEntry *log, TrxId *txnId, LsnType *preLsn, LobId *lobId);

	u64 getMRecSize();
public:
	// get set operations
	Table* getNtseTable() const {return m_tab;}
	void setNtseTable(Table *m_ntseTable) {m_tab = m_ntseTable;}
	Records* getRecords() const;
	MRecords* getMRecords() const {return m_mrecs;}
	TNTIndice* getIndice() const {return m_indice;}

	static void parsePurgePhase1(const LogEntry *log, TrxId *txnId, LsnType *preLsn, TrxId *minReadView);
	static void parsePurgePhase2(const LogEntry *log, TrxId *txnId, LsnType *preLsn);
	//static void parsePurgeTableEnd(const LogEntry *log, TrxId *txnId, LsnType *preLsn);

	//Purge
	LsnType writePurgePhase1(Session *session, TrxId txnId, LsnType preLsn, TrxId minReadView);
	LsnType writePurgePhase2(Session *session, TrxId txnId, LsnType preLsn);
	LsnType writePurgeTableEnd(Session *session, TrxId txnId, LsnType preLsn);
private:
	TNTTable();

	TNTTblScan* beginScan(Session *session, ScanType type, OpType opType, u16 numReadCols, u16 *readCols, MemoryContext *lobCtx);
	bool isCoverageIndex(const TableDef *table, const IndexDef *index, const ColList &cols);
	void releaseLastRow(TNTTblScan *scan, bool retainLobs);
	TNTTblScan* allocScan(Session *session, u16 numCols, u16 *columns, OpType opType);
	void stopUnderlyingScan(TNTTblScan *scan);
	TNTTransaction* getScanTrx();
	bool readRecByRowId(TNTTblScan *scan, RowId rid, bool* ntseReturned, bool isDeleted, bool isVisible, u16 rowVersion, bool isSnapshotRead);
	bool readNtseRec(TNTTblScan *scan);
	Record *readNtseRec(Session *session, RowId rid);
	bool doubleCheckRec(TNTTblScan *scan);
	void fillMysqlRow(TNTTblScan *scan, byte *mysqlRow);
	void lockTableOnStatStart(TNTTransaction *trx, TLockMode lockType) throw(NtseException);

	void addMemIndex(Session *session, u16 numIndice, const IndexDef **indexDefs);
	void dropMemIndex(Session *session, uint idx);

	//redo
	void redoUpdate(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *updateSub, RedoType redoType);
	void redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, RedoType redoType);

	//Log��־
	//Insert Lob
	LsnType writeInsertLob(Session *session, TrxId txnId, LsnType preLsn, LobId lobId);

private:
	TNTDatabase		*m_db;				// tnt database
	Table			*m_tab;				// ntse table
	MRecords		*m_mrecs;			// tntim mrecords
	TNTIndice		*m_indice;			// tntim indice
	s64				m_estimateRows;		// estimate rows
	TNTTableStat	*m_tabStat;			// table stat, normal & dump & purge
	bool			m_mysqlTmpTable;	/** �Ƿ���Mysql�ϲ�ָ������ʱ�� */
	
	// autoinc & mutex & lock
	// ���ϵ�autoinc��ǰֵ��insert��Ҫ���Ӵ�ֵ
	u64				m_autoinc;
	// mutex�ǲ����Ż���ʩ
	// ����֪Insert����autoinc������ǰ���£�����ʹ��mutex
	// δ֪����������ʹ��lock
	Mutex			m_autoincMutex;
	TLock			*m_autoincLock;
	uint			m_reqAutoincLocks;
	// ����autoinc lock������
	const TNTTransaction *m_autoincTrx;

	// TNT Table�ϵļ����������жԴ˱�ӵ����������������
	TLock			*m_tabLockHead;
	uint			m_lockCnt;

	// TNT Table��Ӧ��TableBase�������ڴ��������ڴ�ѵȵ�
	// TNT Table�رյ�ʱ���ڴ��������ڴ�Ѳ���ͬʱ�ر�
	TNTTableBase	*m_tabBase;

	PurgeStatus     m_purgeStatus;
	
	friend class TNTTblMntAlterIndex;
	friend class TNTTblMntAlterColumn;
};
}
#endif