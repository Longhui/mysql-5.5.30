/**
* TNT表管理模块。
*
* @author 何登成
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

// TNT Table的基本信息，包括：TNT Heap，Rowid Hash，Indice等在内存中的位置信息
class TNTTableBase {
public:
	u16                     m_tableId;
	DLink<TNTTableBase *>	m_tableBaseLink;
	MRecords				*m_records;			// 内存Heap
	TNTIndice				*m_indice;			// 内存Indice
	bool					m_opened;			// TableBase是否已经打开
	bool					m_closed;			// TableBase是否已经关闭

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

	// open/close TNTIM内存结构，包括内存堆，内存索引
	void open(Session *session, TNTDatabase *db, TableDef **tableDef, LobStorage *lobStorage, DrsIndice *drsIndice)throw(NtseException);
	void close(Session *session, bool flush);
};

struct TNTTableInfo {
public:
	TNTTable	*m_table;		// TNT table实例
	char		*m_path;		// TNT table对应的NTSE表路径
	uint		m_refCnt;		// open引用计数

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
	/** 设置当前表的临时表属性
	 * @param mysqlTmpTable	true表示是mysql的临时表，false表示不是
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
	// 在线创建索引，需要知会TNT，TNT采用purge内存+ntse在线创建索引的方式完成
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
		
	// duplicate insert/update处理函数
	bool checkDuplicate(Session *session, TableDef *m_tableDef, RowId rowId, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum, bool fastCheck = true) throw (NtseException);
	// bool checkDuplicateFast(Session *session, TableDef *m_tableDef, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw (NtseException);
	bool checkDuplicateSlow(Session *session, TableDef *m_tableDef, RowId rowId, const SubRecord *before, SubRecord *after, uint *dupIndex, u16 *updateIndexNum) throw (NtseException);
	void transIndexCols(u16 maxCols, IndexDef *index, u16 *cols);

	IUSequence<TNTTblScan *>* insertForDupUpdate(Session *session, const byte *record, TNTOpInfo *opInfo) throw(NtseException);
	bool updateDuplicate(IUSequence<TNTTblScan *> *iuSeq, byte *update, u16 numUpdateCols, u16 *updateCols, uint *dupIndex = 0) throw(NtseException);
	void deleteDuplicate(IUSequence<TNTTblScan *> *iuSeq) throw(NtseException);
	void freeIUSequenceDirect(IUSequence<TNTTblScan *> *iduSeq);
	
	// 表级锁接口，TNT由Lock模块完成，此处不需要处理
	// 但是需要处理meta lock方法
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
	
	// 恢复接口，DDL恢复操作，完全交由NTSE完成即可
	
	// statistics
	
	// TNT引擎特有操作
	// 1. autoinc
	u64 getAutoinc();
	void updateAutoincIfGreater(u64 value);
	void initAutoinc(u64 value);
	// 并发控制；mutex是优化路径；lock是正常路径；
	// 只有在简单的insert/replace语法时，能够使用mutex优化路径
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

	//Log日志
	//Insert Lob
	LsnType writeInsertLob(Session *session, TrxId txnId, LsnType preLsn, LobId lobId);

private:
	TNTDatabase		*m_db;				// tnt database
	Table			*m_tab;				// ntse table
	MRecords		*m_mrecs;			// tntim mrecords
	TNTIndice		*m_indice;			// tntim indice
	s64				m_estimateRows;		// estimate rows
	TNTTableStat	*m_tabStat;			// table stat, normal & dump & purge
	bool			m_mysqlTmpTable;	/** 是否是Mysql上层指定的临时表 */
	
	// autoinc & mutex & lock
	// 表上的autoinc当前值，insert需要增加此值
	u64				m_autoinc;
	// mutex是并发优化措施
	// 在已知Insert消耗autoinc数量的前提下，可以使用mutex
	// 未知数量，必须使用lock
	Mutex			m_autoincMutex;
	TLock			*m_autoincLock;
	uint			m_reqAutoincLocks;
	// 持有autoinc lock的事务
	const TNTTransaction *m_autoincTrx;

	// TNT Table上的加锁链表，所有对此表加的锁，都链入此链表
	TLock			*m_tabLockHead;
	uint			m_lockCnt;

	// TNT Table对应的TableBase，包括内存索引，内存堆等等
	// TNT Table关闭的时候，内存索引、内存堆并不同时关闭
	TNTTableBase	*m_tabBase;

	PurgeStatus     m_purgeStatus;
	
	friend class TNTTblMntAlterIndex;
	friend class TNTTblMntAlterColumn;
};
}
#endif