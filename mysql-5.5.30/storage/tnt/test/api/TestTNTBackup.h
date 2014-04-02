/**
 * ²âÊÔTNTDatabase backup²Ù×÷
 *
 * @author xindingfeng
 */
#ifndef _NTSETEST_TNTBACKUP_
#define _NTSETEST_TNTBACKUP_

#include <cppunit/extensions/HelperMacros.h>
#include "api/TNTDatabase.h"
#include "api/TNTTblScan.h"

using namespace tnt;
using namespace ntse;

struct StuRec;

class TNTBackupTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TNTBackupTestCase);
	CPPUNIT_TEST(testBackupAndRestoreBeforeDump);
	CPPUNIT_TEST(testBackupAndRestoreAfterDump);
	CPPUNIT_TEST(testBackupAndRestoreMultiThr);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	static SubRecord *createSubRecord(TableDef *tableDef, const std::vector<u16> &columns, const std::vector<void *>&datas);
	static TNTTransaction *startTrx(TNTTrxSys *trxSys, Connection *conn, TLockMode lockMode = TL_NO, bool log = false);
	static void commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
	static void rollBackTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
	static void initTNTOpInfo(TNTOpInfo &opInfo, TLockMode mode) {
		opInfo.m_selLockType = mode;
		opInfo.m_sqlStatStart = true;
		opInfo.m_mysqlHasLocked = false;
		opInfo.m_mysqlOper = true;
	}

	static void update(Session *session, TNTTable *table, StuRec *recs, const u32 begin, const u32 end, u8 factor, bool save);
	static void remove(Session *session, TNTTable *table, StuRec *recs, const u32 begin, const u32 end, bool save);

protected:
	void testBackupAndRestoreBeforeDump();
	void testBackupAndRestoreAfterDump();
	void testBackupAndRestoreMultiThr();
private:
	void testBackupAndRestore(bool afterDump);
	TableDef *createStudentTableDef();
	Record* createStudentRec(u32 stuNo, const char *name, u16 age, u32 clsNo, float gpa);
	void releaseAllRecordVal(StuRec *recs, u32 count);
	void compareAllRecord(StuRec *recs, u32 count);
	void compareRecord(Session *session, TNTTable *table, StuRec *recVal);

	void updateAll(StuRec *recs, const u32 count, u8 factor, bool save, bool log);
	void insert(StuRec *recs, const u32 count, bool log);
	void update(StuRec *recs, const u32 begin, const u32 end, u8 factor, bool save, bool log);
	void remove(StuRec *recs, const u32 begin, const u32 end, bool save, bool log);
	void insert(Session *session, StuRec *recs, const u32 count);
	void update(Session *session, StuRec *recs, const u32 begin, const u32 end, u8 factor, bool save);
	void remove(Session *session, StuRec *recs, const u32 begin, const u32 end, bool save);

	void init(const char *base, const char *log, bool create);
	void shutDown(const char *base, const char *log);

	TNTDatabase	*m_db;
	Session		*m_session;
	Connection	*m_conn;
	TNTTable	*m_table;
	TNTConfig   m_config;
};
#endif
