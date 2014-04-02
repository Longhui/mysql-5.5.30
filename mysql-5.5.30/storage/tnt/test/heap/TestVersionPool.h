#ifndef _TNTTEST_VERSIONPOOL_H_
#define _TNTTEST_VERSIONPOOL_H_

#include <cppunit/extensions/HelperMacros.h>
#include "heap/VersionPool.h"

using namespace ntse;
using namespace tnt;

class VersionPoolTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(VersionPoolTestCase);
	CPPUNIT_TEST(testInsert);
	CPPUNIT_TEST(testGetVersionRecord);
	CPPUNIT_TEST(testGetVersionRecordBigDiff);
	CPPUNIT_TEST(testGetRollBackHeapRec);
	CPPUNIT_TEST(testGetRollBackHeapRecBigDiff);
//	CPPUNIT_TEST(testDefrag);
	CPPUNIT_TEST(testRollBack);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testInsert();
	void testGetVersionRecord();
	void testGetVersionRecordBigDiff();
	void testGetRollBackHeapRec();
	void testGetRollBackHeapRecBigDiff();
	void testDefrag();
	void testRollBack();
private:
	TableDef* createTableDef();
	SubRecord *createEmptySubRecord(TableDef *tblDef, RecFormat format, MemoryContext *ctx);
	SubRecord *createSubRecord(TableDef *tblDef, RecFormat format, const std::vector<u16> &columns, std::vector<void *> &datas, MemoryContext *ctx);
	Record* createStudentRec(RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade, const char* comment = NULL, LobId lobId = 0);
	ReadView* createReadView(MemoryContext *ctx, TrxId createTrxId, TrxId up, TrxId low, TrxId *activeTrxIds = NULL, u16 activeTrxCnt = 0);

	VersionPool *m_versionPool;
	char        *m_basePath;
	u8          m_count;
	u64         m_maxPageId;

	Connection       *m_conn;
	Session          *m_session;
	Database	     *m_db;
	Config		     m_config;

	TableDef         *m_tableDef;
};
#endif