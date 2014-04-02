/**
 * 对给定的Table执行DML操作
 *
 * 苏斌(bsu@corp.netease.com, naturally@163.org)
 */

#ifndef _NTSETEST_TABLEDMLHELPER_H_
#define _NTSETEST_TABLEDMLHELPER_H_

#include "api/Table.h"
#include "misc/Global.h"
#include "misc/Record.h"

using namespace ntse;

enum TblOpResult {
	SUCCESS = 0,
	FAIL,
	OUT_OF_BOUND,
	CHECK_FAIL
};

class TableDMLHelper {
public:
	static TblOpResult insertRecord(Session *session, Table *table, u64 key, uint opid);
	
	static TblOpResult updateRecord(Session *session, Table *table, u64 *key, u64 *kplus, uint opid, bool check);
	
	static TblOpResult deleteRecord(Session *session, Table *table, u64 *key);

	static TblOpResult fetchRecord(Session *session, Table *table, u64 key, byte *record, RowId *rowId, bool precise);

	static u16* updateSomeColumnsOfRecord(MemoryContext *memotyContext, Table *table, Record *record, u64 key, uint opid, uint *updateCols);

	static void printStatus(Table *table);
};


class TableWarmUpHelper {
public:
	static void warmUpTableByIndice(Database *db, Table *table, bool allIndice, bool readTableRecord);
};

class ResultChecker {
public:
	static bool checkTableToIndice(Database *db, Table *table, bool checkTable, bool checkRecord);

	static bool checkIndiceToTable(Database *db, Table *table, bool checkIndex, bool checkKey);

	static bool checkIndexToTable(Database *db, Table *table, uint indexNo, bool checkKey);

	static bool checkRecord(Table *table, byte *record, RowId rowId, IndexDef *indexDef);
	
	static bool checkIndice(Database *db, Table *table);

	static bool checkHeap(Database *db, Table *table);

	static bool checkRecordToRecord(TableDef *tableDef, byte *rec1, byte *rec2);

private:
	static bool checkKeyToRecord(TableDef *tableDef, IndexDef *indexDef, byte *key, byte *record);

	static bool checkColumn(TableDef *tableDef, u16 colNo, byte *cols1, byte *cols2);
};


class RecordHelper {
public:
	static u16* getAllColumns(TableDef *tableDef);

	static bool isColPrimaryKey(uint colNo, const IndexDef *indexDef);

	static Record* formRecordFromData(TableDef *tableDef, RowId rowId, byte *record);

	static SubRecord* formIdxKeyFromData(TableDef *tableDef, IndexDef *indexDef, RowId rowId, byte *record);

	static char* getLongChar(MemoryContext *memoryContext, size_t size, const TableDef *tableDef, const ColumnDef *columnDef, uint opId);

	static bool checkLongChar(const TableDef *tableDef, const ColumnDef *columnDef, uint opid, size_t size, byte *buf);

	static s32 checkEqual(const TableDef *tableDef, const IndexDef *indexDef, const byte *record, const byte *key);
};

const static uint MAX_FILES = 5;
const static char *postfixes[MAX_FILES] = {".nsd", ".nsi", ".nsso", ".nsld", ".nsli"};

extern bool createSpecifiedFile(const char *filename);

extern void backupAFile(char *backupHeapFile, char *origFile);

extern void backupFiles(const char *path, const char *tableName, bool useMms, bool backup);


#endif

