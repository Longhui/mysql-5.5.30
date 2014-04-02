#ifndef _NTSETEST_TNTONLINEDDL_
#define _NTSETEST_TNTONLINEDDL_

#include <cppunit/extensions/HelperMacros.h>
#include "api/TNTDatabase.h"
#include "api/TNTTblScan.h"

using namespace tnt;
using namespace ntse;

struct RecordVal;

class TNTOnlineDDLTestCase: public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE(TNTOnlineDDLTestCase);
	CPPUNIT_TEST(testCreateOnlineIndex);
	CPPUNIT_TEST(testOnlineOptimize);
	CPPUNIT_TEST(testCreateOnlineIndexUpdate);
	CPPUNIT_TEST(testOnlineOptimizeUpdate);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	static SubRecord *createSubRecord(TableDef *tableDef, RecFormat format, const std::vector<u16> &columns, const std::vector<void *>&datas);

	static TNTTransaction *startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode = TL_NO, bool log = false);
	static void commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
	static void initTNTOpInfo(TNTOpInfo &opInfo, TLockMode mode) {
		opInfo.m_selLockType = mode;
		opInfo.m_sqlStatStart = true;
		opInfo.m_mysqlHasLocked = false;
		opInfo.m_mysqlOper = true;
	}

	static void update(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save);
	static void remove(Session *session, TNTTable *table, RecordVal *recs, const u32 begin, const u32 end, bool save);

protected:
	void testCreateOnlineIndex();
	void testOnlineOptimize();
	void testCreateOnlineIndexUpdate();
	void testOnlineOptimizeUpdate();

private:
	void openTable(bool idx);
	void closeTable();
	TableDef *createStudentTableDef(bool idx);
	void updateAll(RecordVal *recs, const u32 count, u8 factor, bool save, bool log);
	void insert(RecordVal *recs, const u32 count, bool log);
	void update(RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save, bool log);
	void remove(RecordVal *recs, const u32 begin, const u32 end, bool save, bool log);
	void insert(Session *session, RecordVal *recs, const u32 count);
	void update(Session *session, RecordVal *recs, const u32 begin, const u32 end, u8 factor, bool save);
	void remove(Session *session, RecordVal *recs, const u32 begin, const u32 end, bool save);
	Record* createStudentRec(u32 stuNo, const char *name, u16 age, u32 clsNo, float gpa);

	void compareAllRecordByIndexScan(Session *session, TNTTable *table, RecordVal *recs, u32 count);
	void compareRecordByIndexScan(Session *session, TNTTable *table, RecordVal *recVal);
	//void compareRecord(Session *session, TNTTable *table, RecordVal *recVal, RowId rid);
	SubRecord *createKey(TableDef *tblDef, int idxNo, Record *rec);

	void releaseAllRecordVal(RecordVal *recs, u32 count) ;

	TNTDatabase	*m_db;
	Session		*m_session;
	Connection	*m_conn;
	TNTTable	*m_table;
	TNTConfig   m_config;
};
#endif