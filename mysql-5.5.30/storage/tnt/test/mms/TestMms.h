/**
* MMS���ܲ���
*
* @author �۷�(shaofeng@corp.netease.com, sf@163.org)
*/

#ifndef _NTSETEST_MMS_H_
#define _NTSETEST_MMS_H_

#include <cppunit/extensions/HelperMacros.h>
#include <heap/FixedLengthRecordHeap.h>
#include "api/Database.h"
#include "util/PagePool.h"
#include "util/Thread.h"
#include "mms/Mms.h"

#ifdef NTSE_UNIT_TEST

using namespace ntse;

class FMmsTable;
class VMmsTable;
class MmsTester;
class MmsTestHelper;

#define NR_RECORDS 100

enum MmsThreadTask {
	MTT_DOTOUCH = 0,								/** MmsTable::doTouch	*/
	MTT_DEL_DOTOUCH,
	MTT_GETRID_WHEN_LOCKROW_0,						/** MmsTable::getByRid	*/
	MTT_GETRID_WHEN_LOCKROW_1,
	MTT_GETRID_WHEN_LOCKROW_2,
	MTT_GETRID_WHEN_LOCKROW_3,
	MTT_GETRID_DISABLEPG,
	MTT_GETRID_TRYLOCK,
	MTT_PUT_DISABLEPG,
	MTT_GETSUBRECORD_DISABLEPG,
	MTT_GETSESSION_WHEN_FLUSHLOG_1,					/** MmsTable::flushLog	*/
	MTT_GETSESSION_WHEN_FLUSHLOG_2,
	MTT_FLUSHLOG_VICTIMPAGE,						/** Mms::flushLog		*/
	MTT_FLUSHLOG_RPCLASS,
	MTT_FLUSH_VICTIMPAGE,							/** Mms::flushLog		*/
	MTT_SORTFLUSH_RPCLASS,
	MTT_DIRTY_REC,									
	MTT_GETPIN_REPLACEMENT,							/** Mms::allocMmsPage	*/
	MTT_AMP_GET_TBL,
	MTT_AMP_GET_PG,
	MTT_AMP_PIN_PG,

	MTT_MAX
};

/** MMS���ܲ������� */
class MmsTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MmsTestCase);

	CPPUNIT_TEST(testDoTouch);
	CPPUNIT_TEST(testDelWhenDoTouch);
	CPPUNIT_TEST(testGetRidWhenLockRow);
	CPPUNIT_TEST(testCacheUpdateTime);
	CPPUNIT_TEST(testMmsStats);
	CPPUNIT_TEST(testReplacement);
	CPPUNIT_TEST(testPageTypeWhenFlush);
	CPPUNIT_TEST(testPageTypeWhenGetDirRec);
	CPPUNIT_TEST(testRecover);
	CPPUNIT_TEST(testReplaceWhenPinTopPage);
	CPPUNIT_TEST(testAllocMmsPage);
	CPPUNIT_TEST(testFlushLogRpClass);
	CPPUNIT_TEST(testSortFlushRpClass);
	CPPUNIT_TEST(testFreqHeap);
	CPPUNIT_TEST(testGetRecord);
	CPPUNIT_TEST(testRpClassRecSize);
	CPPUNIT_TEST(testRidHashConflict);
	CPPUNIT_TEST(testTryLockWhenGetByRid);
	CPPUNIT_TEST(testDisablePgWhenGetByRid);         
	CPPUNIT_TEST(testDisablePgWhenPutIfNotExist);
	CPPUNIT_TEST(testDisablePgWhenGetSubRecord);                   
	CPPUNIT_TEST(testFlushLogForce);          
	CPPUNIT_TEST(testHeapMove);                                  
	
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	// MmsTester��غ���
	void doTestTouch(MmsTester *tester);
	void doHelpTestTouch(MmsTester *tester);
	void doTestDelWhenTouch(MmsTester *tester);
	void doHelpTestDelWhenTouch(MmsTester *tester);
	void doGetRidWhenLockRow(MmsTester *tester);
	void doHelpGetRidWhenLockRow(MmsTester *tester, MmsThreadTask task);
	void doGetSessionWhenFlushLog(MmsTester *tester, MmsThreadTask task);
	void doHelpGetSessionWhenFlushLog(MmsTester *tester, MmsThreadTask task);
	void doTestGetSessionWhenReplacement(MmsTester *tester);
	void doHelpTestGetSessionWhenReplacement(MmsTester *tester);
	void doTestPageType(MmsTester *tester);
	void doHelpTestPageType(MmsTester *tester);
	void doTestPageTypeWhenFlush(MmsTester *tester);
	void doHelpTestPageTypeWhenFlush(MmsTester *tester);
	void doReplaceWhenPinTopPage(MmsTester *tester);
	void doHelpReplaceWhenPinTopPage(MmsTester *tester);
	void doTestAllocMmsPage(MmsTester *tester, MmsThreadTask task);
	void doHelpAllocMmsPage(MmsTester *tester, MmsThreadTask task);
	void doTestPageTypeWhenGetDirRec(MmsTester *tester);
	void doHelpTestPageTypeWhenGetDirRec(MmsTester *tester);
	void doTestFlushLogRpClass(MmsTester *tester);
	void doHelpTestFlushLogRpClass(MmsTester *tester);
	void doTestSortFlushRpClass(MmsTester *tester);
	void doHelpTestSortFlushRpClass(MmsTester *tester);
	void doTestDisablePgWhenGetByRid(MmsTester *tester, MmsThreadTask task);
	void doHelpDisablePgWhenGetByRid(MmsTester *tester, MmsThreadTask task);
	void doTestTryLockWhenGetByRid(MmsTester *tester, MmsThreadTask task);
	void doHelpTryLockWhenGetByRid(MmsTester *tester, MmsThreadTask task);
	void doTestDisablePgWhenPutIfNotExist(MmsTester *tester, MmsThreadTask task);
	void doHelpDisablePgWhenPutIfNotExist(MmsTester *tester, MmsThreadTask task);
	void doTestDisablePgWhenGetSubRecord(MmsTester *tester, MmsThreadTask task);
	void doHelpDisablePgWhenGetSubRecord(MmsTester *tester, MmsThreadTask task);

protected:
	// ���������
	void testCreateMms();
	void testCreateMmsTable();

	// ����ӿ������
	void testPutIfNotExist();
	void testGetByPrimaryKey();
	void testGetByRid();
	void testHeapMove();
	void testUpdate();
	void testUpdateWhenVariousSet();
	void doTestUpdate(bool insertHeap, int count = NR_RECORDS);
	void doTestUpdate_simple(bool insertHeap, int count = NR_RECORDS);
	void testDel();
	void testCheckPointFlush();
	void testCacheUpdateTime();
	void testFreeSomePages();
	void testRecover();
	void testMmsStats();
	void testDoTouch();
	void testAllocFail();
	void testDelWhenDoTouch();
	void testGetRidWhenLockRow();
	void testGetRidWhenLockRow(MmsThreadTask task);
	void testGetPKWhenLockRow(MmsThreadTask task);
	void testGetSessionWhenFlushLog();
	void testGetSessionWhenFlushLog(MmsThreadTask task);
	void testPageType();
	void testPageTypeWhenFlush();
	void testUpdateNonPK();
	void testUpdateWhenParitalCached();
	void testReplaceWhenPinTopPage();
	void doBackupFile(File *file, const char *backupName);
	void doRestoreFile(const char *backupFile, const char *origFile);
	void testAllocMmsPage();
	void testAllocMmsPage(MmsThreadTask task);
	void testDisablePgWhenGetByRid();
	void testTryLockWhenGetByRid();
	void testDisablePgWhenPutIfNotExist();
	void testDisablePgWhenGetSubRecord();
	void testRpClassRecSize();
	void testGetSubRecord();
	void testUpdateWhenAllocFail();
	void testGetDirtyCols();
	void testRidHashConflict();
	void testFlushLogForce();
	
	// �ڲ����ܲ���
	void testReplacement();
	void testReplaceThread();

	void testRedoUpdate(int cond);
	void testPageTypeWhenGetDirRec();
	void testMms();
	void testFlushLogRpClass();
	void testSortFlushRpClass();
	void testUpdateEx();
	void testFreqHeap();
	void testFPage();
	void testGetRecord();


private:
	Config		*m_config;				/** ���ö���		*/
	Database	*m_db;					/** ���ݿ����		*/
	Connection	*m_conn;				/** ���Ӷ���		*/
	Session     *m_session;				/** �Ự����		*/
	PagePool	*m_pagePool;			/** ҳ�ض���		*/
	DrsHeap		*m_flrHeap;				/** �����Ѷ���		*/
	DrsHeap		*m_vlrHeap;				/** �䳤�Ѷ���		*/
	Mms			*m_mms;					/** MMSȫ�ֶ���		*/
	FMmsTable	*m_fMmsTable;           /** ����MMS�����	*/
	VMmsTable	*m_vMmsTable;			/** �䳤MMS�����	*/
	VMmsTable	*m_vMmsTable2;			/** �䳤MMS�����2	*/
	bool		m_finish;

	RowId fixInsert(MmsTable *mmsTable, Session *session, u64 userId, bool insertHeap, DrsHeap *heap, const TableDef *tblDef);

	void init(uint targetSize, bool cacheUpdate, bool colUpdate, bool needRedo = false, bool delHeapFile = true, bool varcharPK = true, bool partialCached = false, int partitionNr = 31);
	void close(bool delHeapFile = true, bool flush = true);
	/** �䳤���ڲ����� */
	Record* createVRecord(u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance);
};

/** MMS������ 
*
* ����Ϊ�� Create Table fTable(X) ( userId	bigint primary key, 
*									  username	char(X), 
*									  bankcc	bigint,
*									  balance   int	    )
*/
class FMmsTable {
public:
	void create(Session *session, Database *db, Mms *mms, int tid, const char *schema, 
		const char *tablename, const char *namePrefix, int prefixSize, bool tableUpdate, bool colUpdate, bool delHeapFile, bool partialCached = false, int partitionNr = 31);
	void drop(Session *session, bool delHeapFile = true, bool flushDirty = true);
	RowId insert(Session *session, u64 userId, bool insertHeap);
	RowId insertEx(Session *session, u64 userId, bool insertHeap);
	RowId insertReal(Session *session, u64 userId, bool insertHeap);
	RowId insertDouble(Session *session, u64 userId, bool insertHeap);
	bool del(Session *session, u64 userId);
	void update(Session *session, u64 rowId, u64 updateId);
	void updateColumn1(Session *session, u64 rowId, u64 updateId);
	void updateReal(Session *session, u64 rowId, u64 updateId);
	void updateRealEx(Session *session, u64 rowId, u64 updateId);
	MmsRecord* selectByRid(Session *session, u64 userId, RowLockHandle **rlh, LockMode lockMode);
	// MmsRecord* selectByPK(Session *session, u64 userId, RowLockHandle **rlh, LockMode lockMode);
	// MmsRecord* selectByPKReal(Session *session, u64 userId, RowLockHandle **rlh, LockMode lockMode);
	void release(Session *session, MmsRecord *mmsRecord, RowLockHandle **rlh); 
	void getRecord(Session *session, u64 userId, bool insertHeap);
	MmsTable* getMmsTable();
	int getTid();
	DrsHeap* getHeap();
	TableDef* getTableDef();
private:
	Database	*m_db;					/** �������ݿ�	*/
	DrsHeap		*m_heap;				/** ������		*/
	TableDef    *m_tblDef;
	int			m_tid;					/** ��ID		*/
	MmsTable	*m_mmsTable;			/** ���MMS��	*/
	const char	*m_namePrefix;			/** �û���ǰ׺	*/
	int			m_prefixSize;			/** ǰ׺����	*/
	Mms			*m_mms;
};

/** MMS�䳤�� 
*
* ����Ϊ��Create Table vTable(X) ( userId   bigint, 
*                                    username varchar(X)  primary key, 
*                                    bankcc   bigint,
*                                    balance  int        )
*/
class VMmsTable {
public:
	void create(Session *session, Database *db, Mms *mms, int tid, const char *schema, const char *tablename, /*const char *heapfilename,*/
		int maxNameSize, bool tableUpdate, bool colUpdate, bool varcharPK, bool paritalUpdate, bool delHeapFile, int partitionNr);
	void drop(Session *session, bool delHeapFile = true, bool flushDirty = true);
	RowId insert(Session *session, u64 userId, const char *username, int nameSize, bool insertHeap);
	RowId insertReal(Session *session, u64 userId, const char *username, int nameSize, bool insertHeap);
	bool del(Session *session, u64 rowId);
	// bool del(Session *session, const char *username, int nameSize);
	void update(Session *session, u64 rowId, u64 updateId);
	void update(Session *session, u64 rowId, char *username);
	void update(Session *session, u64 rowId, char *username, bool touch);
	void updateReal(Session *session, u64 rowId, const char *username);
	void release(Session *session, MmsRecord *mmsRecord, RowLockHandle **rlh); 
	MmsRecord* selectByRid(Session *session, u64 rowId, RowLockHandle **rlh, LockMode lockMode);
	// MmsRecord* selectByPK(Session *session, const char* userName, int nameSize, RowLockHandle **rlh, LockMode lockMode);
	// MmsRecord* selectByPKReal(Session *session, const char* userName, int nameSize, RowLockHandle **rlh, LockMode lockMode);
	MmsTable* getMmsTable();
	DrsHeap* getHeap();
	TableDef* getTableDef();
	void updateColumn0(Session *session, u64 rowId, u64 userId);
	void updateColumn1(Session *session, u64 rowId, char *username);
	void updateColumn3(Session *session, u64 rowId, u64 userId);
	void updateColumn13(Session *session, u64 rowId, u64 userId, char *username);
	void updateColumn23(Session *session, u64 rowId, u64 userId);
	void updateColumn0123(Session *session, u64 rowId, u64 userId, char *username);
private:
	Database	*m_db;				/** �������ݿ�	*/
	DrsHeap		*m_heap;			/** ������		*/
	TableDef    *m_tblDef;
	const char	*m_heapName;		/** ���ļ���	*/
	int			m_tid;				/** ��ID		*/
	MmsTable	*m_mmsTable;		/** ���MMS��	*/
	int			m_maxNameSize;		/** ������󳤶�*/
	bool		m_varcharPK;		/** �䳤����	*/
	Mms			*m_mms;
};

/** �������� */
class MmsTester : public ntse::Thread {
public:
	MmsTester(MmsTestCase *testCase, MmsThreadTask task) : Thread("MmsTester") {
		m_testCase = testCase;
		m_task = task;
	}

private:
	MmsTestCase *m_testCase;
	MmsThreadTask m_task;
	virtual void run();
};

/** �������� */
class MmsTestHelper : public ntse::Thread {
public:
	MmsTestHelper(MmsTestCase *testCase, MmsTester *tester, MmsThreadTask task) : Thread("MmsTestHelper") { 
		m_testCase = testCase;
		m_tester = tester;
		m_task = task;
	}

private:
	MmsTestCase *m_testCase;
	MmsTester *m_tester;
	MmsThreadTask m_task;
	virtual void run();
};

#endif


#endif // _NTSETEST_MMS_H_
