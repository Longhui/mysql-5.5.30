#ifndef _TNTTEST_MRECORDS_H_
#define _TNTTEST_MRECORDS_H_

#include <cppunit/extensions/HelperMacros.h>
#include "rec/MRecords.h"
#include "api/TNTTable.h"

using namespace ntse;
using namespace tnt;

struct RecAndFieldVal {
	Record		*m_redRec;
	char        *m_name;
	u32			m_stuNo;
	u16			m_age;
	char		*m_sex;
	u32			m_clsNo;
	u64			m_grade;
	RowId		m_rid;
};

class GetRecordThread: public Thread {
public:
	GetRecordThread(TNTDatabase *db, TableDef *tableDef, TNTTable *table);
	void run();
private:
	TNTDatabase       *m_db;
	TableDef          *m_tableDef;
	TNTTable          *m_table;
};

class UpdateDefragThread: public Thread {
public :
	UpdateDefragThread(TNTDatabase *db, TableDef *tableDef, TNTTable *table, RecAndFieldVal *rec);
	void run();
private:
	TNTDatabase       *m_db;
	TableDef          *m_tableDef;
	TNTTable          *m_table;
	RecAndFieldVal    *m_rec;
};

class RemoveDefragThread: public Thread {
public :
	RemoveDefragThread(TNTDatabase *db, TableDef *tableDef, TNTTable *table, RecAndFieldVal *rec);
	void run();
private:
	TNTDatabase       *m_db;
	TableDef          *m_tableDef;
	TNTTable          *m_table;
	RecAndFieldVal    *m_rec;
};

class MRecordsTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MRecordsTestCase);
	CPPUNIT_TEST(testFixLenInsert);
	CPPUNIT_TEST(testVarLenInsert);
	CPPUNIT_TEST(testFixLenUpdate);
	CPPUNIT_TEST(testVarLenUpdate);
	CPPUNIT_TEST(testFixLenRemove);
	CPPUNIT_TEST(testVarLenRemove);
	CPPUNIT_TEST(testFixLenRollBackRecord);
	CPPUNIT_TEST(testVarLenRollBackRecord);
	CPPUNIT_TEST(testFixLenRedoInsert);
	CPPUNIT_TEST(testVarLenRedoInsert);
	CPPUNIT_TEST(testFixLenRedoFirUpdate);
	CPPUNIT_TEST(testVarLenRedoFirUpdate);
	CPPUNIT_TEST(testFixLenRedoSecUpdate);
	CPPUNIT_TEST(testVarLenRedoSecUpdate);
	CPPUNIT_TEST(testFixLenRedoFirRemove);
	CPPUNIT_TEST(testVarLenRedoFirRemove);
	CPPUNIT_TEST(testFixLenRedoSecRemove);
	CPPUNIT_TEST(testVarLenRedoSecRemove);
	CPPUNIT_TEST(testFixLenUndoInsert);
	CPPUNIT_TEST(testVarLenUndoInsert);
	CPPUNIT_TEST(testFixLenUndoUpdate);
	CPPUNIT_TEST(testVarLenUndoUpdate);
	CPPUNIT_TEST(testFixLenUndoFirRemove);
	CPPUNIT_TEST(testVarLenUndoFirRemove);
	CPPUNIT_TEST(testFixLenUndoSecRemove);
	CPPUNIT_TEST(testVarLenUndoSecRemove);
	CPPUNIT_TEST(testFixLenDump);
	CPPUNIT_TEST(testVarLenDump);
	CPPUNIT_TEST(testFixLenUpdateDefrag);
	CPPUNIT_TEST(testVarLenUpdateDefrag);
	CPPUNIT_TEST(testFixLenRemoveDefrag);
	CPPUNIT_TEST(testVarLenRemoveDefrag);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	void initTable(bool fix);
	void shutDownTable();

	static void confirmRedRecord(TableDef *tableDef, Record *rec, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade);
	static SubRecord* createRedSubRecord(TableDef *tableDef, const std::vector<u16> &columns, const std::vector<void *>&datas);
	static void freeSubRecord(SubRecord *sub);
	static void deepCopyRecord(Record *dest, Record *src);
	static TNTTransaction *startTrx(TNTTrxSys *trxSys, Connection *conn, TrxId trxId, TLockMode lockMode = TL_NO, bool log = false, TrxId* activeTrxIds = NULL, u16 activeCnt = 0);
	static void commitTrx(TNTTrxSys *trxSys,TNTTransaction *trx);

	static void initTNTOpInfo(TNTOpInfo &opInfo, TLockMode mode) {
		opInfo.m_selLockType = mode;
		opInfo.m_sqlStatStart = true;
		opInfo.m_mysqlHasLocked = false;
		opInfo.m_mysqlOper = true;
	}
protected:
	void testFixLenInsert();
	void testVarLenInsert();
	void testFixLenUpdate();
	void testVarLenUpdate();
	void testFixLenRemove();
	void testVarLenRemove();
	void testFixLenRollBackRecord();
	void testVarLenRollBackRecord();
	void testFixLenRedoInsert();
	void testVarLenRedoInsert();
	void testFixLenRedoFirUpdate();
	void testVarLenRedoFirUpdate();
	void testFixLenRedoSecUpdate();
	void testVarLenRedoSecUpdate();
	void testFixLenRedoFirRemove();
	void testVarLenRedoFirRemove();
	void testFixLenRedoSecRemove();
	void testVarLenRedoSecRemove();
	void testFixLenUndoInsert();
	void testVarLenUndoInsert();
	void testFixLenUndoUpdate();
	void testVarLenUndoUpdate();
	void testFixLenUndoFirRemove();
	void testVarLenUndoFirRemove();
	void testFixLenUndoSecRemove();
	void testVarLenUndoSecRemove();
	void testFixLenDump();
	void testVarLenDump();
	void testFixLenUpdateDefrag();
	void testVarLenUpdateDefrag();
	void testFixLenRemoveDefrag();
	void testVarLenRemoveDefrag();
private:
	void testInsert();
	void testUpdate();
	void testRemove();
	void testRollBackRecord();
	void testRedoInsert();
	void testRedoFirUpdate();
	void testRedoSecUpdate();
	void testRedoFirRemove();
	void testRedoSecRemove();
	void testUndoInsert();
	void testUndoUpdate();
	void testUndoFirRemove();
	void testUndoSecRemove();
	void testDump();
	void testUpdateDefrag();
	void testRemoveDefrag();

	TableDef *createStudentTableDef(bool fix);
	Record* createStudentRec(RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade);
	void reOpenAllTable();
	bool isFixTable() {
		return (m_tableDef->m_recFormat == REC_FIXLEN);
	};

	LogScanHandle *getNextTNTLog(Txnlog *txnLog, LogScanHandle *logHdl);

	MRecords          *m_records;

	Connection        *m_conn;
	Session           *m_session;

	TNTConfig		  m_config;
	TNTDatabase       *m_db;
	TableDef          *m_tableDef;
	TNTTable          *m_table;
};
#endif