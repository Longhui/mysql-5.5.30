/**
 * ²âÊÔLob²Ù×÷
 *
 * @author ºúì¿
 */
#ifndef _NTSETEST_TNTLOB_
#define _NTSETEST_TNTLOB_

#include <cppunit/extensions/HelperMacros.h>
#include "api/TNTDatabase.h"
#include "misc/TNTControlFile.h"
#include "util/PagePool.h"
#include "misc/TNTIMPageManager.h"
#include "misc/MemCtx.h"
#include "misc/TableDef.h"
#include "api/TNTTable.h"
#include "api/TNTTblScan.h"
#include "rec/Records.h"
#include "rec/MRecords.h"
#include "btree/TNTIndex.h"
#include "btree/MIndex.h"
#include "misc/RecordHelper.h"

using namespace tnt;
using namespace ntse;

class TNTLobTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TNTLobTestCase);
//	CPPUNIT_TEST(testLob);
	CPPUNIT_TEST(testLobInsert);
	CPPUNIT_TEST(testLobUpdate);
	CPPUNIT_TEST( testLobCRRead);
	CPPUNIT_TEST(testLobDelete);
	CPPUNIT_TEST(testLobPurge);
	CPPUNIT_TEST(testLobPurge2);
	CPPUNIT_TEST(testLobPurge3);
	CPPUNIT_TEST(testLobPurge4);
	CPPUNIT_TEST(testLobRollback);
	CPPUNIT_TEST(testLobPartialRollback);
	CPPUNIT_TEST(testLobRecovery);
	CPPUNIT_TEST(testLobIdReuseInRollBack);
	CPPUNIT_TEST(testReclaimLob);
	CPPUNIT_TEST(testLobIdReuseInReclaim);
	CPPUNIT_TEST(testLobIdReuseInPurge);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	static const uint SMALL_LOB_SIZE;
	static const uint LARGE_LOB_SIZE;

private:
	void init();
	void clear();
//	void testLob();
	void testLobInsert();
	void testLobUpdate();
	void testLobCRRead();
	void testLobDelete();
	void testLobPurge();
	void testLobPurge2();
	void testLobPurge3();
	void testLobPurge4();
	void testLobRollback();
	void testLobPartialRollback();
	void testLobRecovery();
	void testLobIdReuseInRollBack();
	void testReclaimLob();
	void testLobIdReuseInReclaim();
	void testLobIdReuseInPurge();


private:

	void initTNTOpInfo(TLockMode lockMode);
	TNTTransaction* beginTransaction(OpType intention, u64 *trxIds = NULL, uint activeTrans = 0);
	void commitTransaction(TNTTransaction *trx);
	void rollbackTransaction(TNTTransaction *trx);
	void printSubRecord(SubRecord *subRec);

	void populateBlog(TNTDatabase *db, TNTTable *table, uint numRows, Record **rows, bool lobNotNull, byte **contentArray, u64 idstart = 1);
	TableDef* getBlogDef(bool useMms = false, bool onlineIdx = false);

	uint updateBlog(TNTDatabase *db, TNTTable *table, Record **rows, uint numRows, byte** resContents, int cout);
	uint crRead(TNTDatabase *db, TNTTable *table, byte **resContents, int times);
	uint deleteBlog(TNTDatabase *db, TNTTable *table, uint numRows);
	void reOpenDb();
	uint currentRead(TNTDatabase *db, TNTTable *table, ScanType scanType, byte **contentArray, bool readIdxCol = false, uint recordNum = 0, Record **rows = NULL);

private:
	TNTDatabase	*m_db;
	TNTConfig	m_config;
	Session		*m_session;
	Connection	*m_conn;
	TableDef	*m_tableDef;
	TNTTable	*m_table;
	TNTTable	*m_lobTable;
	TNTOpInfo	m_opInfo;
};


class LobThread: public Thread {
public:
	LobThread(TNTDatabase *db, TNTTable *table, TNTOpInfo opInfo);
	virtual void run() = 0;

protected:
	void populateBlog(TNTDatabase *db, TNTTable *table, uint numRows, bool lobNotNull, u64 idstart = 1);
	TNTTransaction* beginTransaction(OpType intention, u64 *trxIds = NULL, uint activeTrans = 0);
	void commitTransaction(TNTTransaction *trx);

	TNTDatabase			*m_db;
	TNTTable			*m_table;
	TNTOpInfo			m_opInfo;
	Session				*m_session;
	Connection			*m_conn;

public:
	TNTTransaction		*m_trx;
};



class InsertLobThread: public LobThread {
public:
	InsertLobThread(TNTDatabase	*db, TNTTable *lobTable, TNTOpInfo opInfo):LobThread(db, lobTable, opInfo){}
	virtual void run();
};


class ReclaimLobThread: public LobThread {
public:
	ReclaimLobThread(TNTDatabase	*db, TNTTable *lobTable, TNTOpInfo opInfo):LobThread(db, lobTable, opInfo){}
	virtual void run();
};


class PurgeLobThread: public LobThread {
public:
	PurgeLobThread(TNTDatabase	*db, TNTTable *lobTable, TNTOpInfo opInfo):LobThread(db, lobTable, opInfo){}
	virtual void run();
};


class RealPurgeLobThread: public LobThread {
public:
	RealPurgeLobThread(TNTDatabase	*db, TNTTable *lobTable, TNTOpInfo opInfo):LobThread(db, lobTable, opInfo){}
	virtual void run();
};
#endif

