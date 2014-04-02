#ifndef _TNT_MRECORDS_H_
#define _TNT_MRECORDS_H_

#include "misc/Global.h"
#include "misc/TNTIMPageManager.h"
#include "misc/Session.h"

#include "api/TNTTblScan.h"

#include "heap/HashIndex.h"
#include "heap/VersionPool.h"
#include "heap/MHeap.h"

using namespace ntse;
namespace tnt {
class MRecords {
public:
	~MRecords(void);

	static MRecords *open(TableDef **tableDef, TNTIMPageManager *pageManager, VersionPool *versionPool);
	void close(Session *session, bool flush);
	void drop(Session *session);

	void dump(Session *session, ReadView *readView, File *file, u64 *offset) throw (NtseException);
	bool readDump(Session *session, File *file, u64 *offset);

	//内存堆记录操作
	//Record* getRecord(TNTTblScan *scan, u64 version, MemoryContext *ctx);
	bool getRecord(TNTTblScan *scan, RowIdVersion version, bool *ntseVisible);
	bool insert(Session *session, TrxId txnId, RowId rowId);
	bool prepareUD(TNTTblScan *scan);
	bool update(TNTTblScan *scan, SubRecord *updateSub);
	bool remove(TNTTblScan *scan);

	MHeapScanContext *beginPurgePhase1(Session *session) throw(NtseException);
	bool purgeNext(MHeapScanContext *ctx, ReadView *readView, HeapRecStat *stat);
	bool purgeCompensate(MHeapScanContext *ctx, ReadView *readView, HeapRecStat *stat);
	void endPurgePhase1(MHeapScanContext *ctx);
	u64 purgePhase2(Session *session, ReadView *readView);

	size_t getCompensateRowSize();

	MHeapScanContext *beginScan(Session *session) throw(NtseException);
	bool getNext(MHeapScanContext *ctx, ReadView *readView);
	void endScan(MHeapScanContext *ctx);

	//redo
	void redoInsert(Session *session, RowId rid, TrxId trxId, RedoType redoType);
	void redoUpdate(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, Record *rec);
	void redoUpdate(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *updateSub);
	void redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex, Record *delRec);
	void redoRemove(Session *session, TrxId txnId, RowId rid, RowId rollBackId, u8 tableIndex);

	//undo
	void parseFirUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx);
	void parseSecUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx);

	void parseFirRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex);
	void parseSecRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex);

	void undoInsert(Session *session, RowId rid);
	void undoFirUpdate(Session *session, RowId rid, MHeapRec *rollBackRec);
	void undoSecUpdate(Session *session, RowId rid, MHeapRec *rollBackRec);
	void undoFirRemove(Session *session, RowId rid, MHeapRec *rollBackRec);
	void undoSecRemove(Session *session, RowId rid, MHeapRec *rollBackRec);
	
	void replaceComponents(TableDef **tableDef);
	void replaceHashIndex(HashIndex *hashIdx);
	int freeSomePage(Session *session, int target);
	inline HashIndex *getHashIndex() {
		return m_hashIndex;
	}

	u32 defragHashIndex(const Session *session, const TrxId minReadView, u32 maxTraverseCnt) {
		return m_hashIndex->defrag(session, (*m_tableDef)->m_id, minReadView, maxTraverseCnt);
	}

	inline MHeapStat getMHeapStat() {
		return m_heap->getMHeapStat();
	}

	inline void resetMHeapStat() {
		m_heap->resetStatus();
	}

	RowIdVersion getVersion();

	MHeapRec *getBeforeAndAfterImageForRollback(Session *session, RowId rid, Record **beforeImg, Record *afterImg);

#ifndef NTSE_UNIT_TEST
private:
#endif

	MRecords(TableDef **tableDef, TNTIMPageManager *pageManager, HashIndex *hashIndex, MHeap *heap, VersionPool *versionPool);

	bool updateMem(TNTTblScan *scan, Record *rec, TrxId txnId, RowId rollBackId, u8 tableIndex, u8 delBit);

	void autoIncrementVersion();

	static SubRecord *createEmptySubRecord(TableDef *tblDef, RecFormat format, RowId rid, MemoryContext *ctx);
	Record *convertRVF(Record *redRec, MemoryContext *ctx);

	void rollBackRecord(Session *session, RowId rid, MHeapRec* rollBackRec);

	//DML LOG
	//Update
	LsnType writeUpdateLog(Session *session, LogType logType, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update);
	LsnType writeFirUpdateLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update);
	LsnType writeSecUpdateLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex, SubRecord *update);

	//Remove
	LsnType writeRemoveLog(Session *session, LogType logType, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex);
	LsnType writeFirRemoveLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex);
	LsnType writeSecRemoveLog(Session *session, TrxId txnId, LsnType preLsn, RowId rid, RowId rollBackId, u8 tableIndex);
	
	void parseUpdateLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex, SubRecord **update, MemoryContext *ctx);
	void parseRemoveLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid, RowId *rollBackId, u8 *tableIndex);

	TNTIMPageManager *m_pageManager;
	MHeap			 *m_heap;
	TableDef         **m_tableDef;
	HashIndex        *m_hashIndex;     //hash索引
	VersionPool      *m_versionPool;   //版本池

	Atomic<int>      m_version;

	friend class TNTTblMntAlterColumn;
};
}
#endif
