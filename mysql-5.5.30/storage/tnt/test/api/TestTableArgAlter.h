/**
 * ���Կ����޸ı�������Թ���
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#ifndef _NTSETEST_TABLEALTER_H_
#define _NTSETEST_TABLEALTER_H_

#include <cppunit/extensions/HelperMacros.h>
#include <string>
#include <iostream>

#include "Test.h"
#include "api/TblArgAlter.h"
#include "api/Table.h"
#include "misc/Record.h"
#include "api/TestHelper.h"

using namespace std;
using namespace ntse;

class BlogCountTblBuilder {
public:
	BlogCountTblBuilder() {
		m_db = NULL;
		m_ti = NULL;
		Database::drop(".");
		File dir("testDB");
		dir.rmdir(true);
		dir.mkdir();
		EXCPT_OPER(m_db = Database::open(&m_config, true));
		EXCPT_OPER(Table::drop(m_db, "BlogCount"));
	}
	~BlogCountTblBuilder() {
		if (m_ti) {
			m_ti->close();
			delete m_ti;
			m_ti = NULL;
		}
		if (m_db) {
			EXCPT_OPER(Table::drop(m_db, "BlogCount"));
			m_db->close(false, false);
			delete m_db;
			m_db = NULL;
		}
		Database::drop(".");
		File dir("testDB");
		dir.rmdir(true);
	}
	void prepareEnv(uint numRow = 2) {
		TableDefBuilder tdb(TableDef::INVALID_TABLEID, "testDB", "BlogCount");
		tdb.addColumn("A", CT_INT, false);
		tdb.addColumn("B", CT_INT, true);
		tdb.addColumn("C", CT_INT, true);
		tdb.addIndex("A", true, true, true, "A", 0, NULL);
		tdb.addIndex("B", false, false, true, "B", 0, NULL);
		TableDef *tableDef = tdb.getTableDef();
		tableDef->m_useMms = true;

		m_ti = new TblInterface(m_db, string(string(m_config.m_basedir) + "/testDB/BlogCount").c_str());
		EXCPT_OPER(m_ti->create(tableDef));
		EXCPT_OPER(m_ti->open());
		try {
			for (uint i = 0; i < numRow; i++) {
				CPPUNIT_ASSERT(m_ti->insertRow(NULL, i+ 1, 1, 1));
			}
		} catch (NtseException &e) {
			cout << "Error: " << e.getMessage() << endl;	
			CPPUNIT_FAIL(e.getMessage());						
		}		
		delete tableDef;
		tableDef = NULL;
	}
	Table *getTable() const {
		return m_ti->getTable();
	}
	Table *reOpenTable() {
		m_ti->close();
		EXCPT_OPER(m_ti->open());
		return m_ti->getTable();
	}
	Database *getDb() const {
		return m_db;
	}
	const char *getSchemaName() {
		return m_ti->getTable()->getTableDef()->m_schemaName;
	}
	void checkTable() {
		assert(m_ti && m_ti->getTable());
		const uint numRows = 3;
		EXCPT_OPER(m_ti->insertRow(NULL, 1, 22, 333));
		EXCPT_OPER(m_ti->insertRow(NULL, 11, 22, 33));
		EXCPT_OPER(m_ti->insertRow(NULL, 111, 22, 3));
		m_ti->verify();
	}
	void checkTableDef() {
		Connection *conn = m_db->getConnection(false, __FUNCTION__);
		Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn, -1);
		Table *table = NULL;
		try {
			table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/testDB/BlogCount").c_str());
			table->lockMeta(session, IL_S, -1, __FUNCTION__, __LINE__);
			table->getTableDef()->check();
			table->unlockMeta(session, IL_S);
		} catch (NtseException &e) {
			CPPUNIT_FAIL(e.getMessage());
		}
		m_db->closeTable(session,table);
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

private:
	Database      *m_db;
	TblInterface  *m_ti;
	Config        m_config;
};

/**
 * LsnKeeper�࣬������־.
 */
class LsnKeeper {
public:
	/**
	 * ���캯��, ����<code>LsnKeeper</code>����. ִ�б����ӵ�ǰ֮���LSN��־.
	 * @param db <code>Database</code>ʵ������ΪNULL.
	 */
	LsnKeeper(const Database *db) {
		assert(NULL != db);
		m_db = db;
		m_keepLsn = m_db->getTxnlog()->tailLsn();
		m_token = keepLsn(m_keepLsn);
	}

	/**
	 * ��ȡԤ����ʼ��LSN.
	 * @return Ԥ����ʼ��LSN.
	 */
	int getKeepLsn() const {
		return (int)m_keepLsn;
	}

	/**
	 * ��������. ȡ����־LSN����.
	 */
	~LsnKeeper() {
		clearKeepLsn(m_token);
	}
private:
	/**
	 * ������ǰlsn֮�����־.
	 * @return ���³ɹ�����һ�����������ʧ�ܷ���ֵС��0.
	 */
	int keepLsn(LsnType keepLsn) {
		int token = m_db->getTxnlog()->setOnlineLsn(keepLsn);
		assert(token >= 0);
		return token;
	}
	/**
	 * ����lsn����.
	 * @param token TableAlterArgTestCase#keepLsn()���ص�token.
	 */
	void clearKeepLsn(int token) {
		m_db->getTxnlog()->clearOnlineLsn(token);
	}
	
private:
	const Database *m_db;
	int m_token;
	LsnType m_keepLsn;
};

class TableAlterArgTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TableAlterArgTestCase);
	CPPUNIT_TEST(testAlterUseMms);
	CPPUNIT_TEST(testRedoAlterUseMms);
// 	CPPUNIT_TEST(testAlterCacheUpdate);
// 	CPPUNIT_TEST(testRedoAlterCacheUpdate);
// 	CPPUNIT_TEST(testAlterCacheUpdateTime);
// 	CPPUNIT_TEST(testRedoAlterCacheUpdateTime);
// 	CPPUNIT_TEST(testAlterCachedColummns);
// 	CPPUNIT_TEST(testRedoAlterCachedColummns);
	CPPUNIT_TEST(testAlterCompressLobs);
	CPPUNIT_TEST(testRedoAlterCompressLobs);
	CPPUNIT_TEST(testAlterHeapPctFree);
	CPPUNIT_TEST(testRedoAlterHeapPctFree);
	CPPUNIT_TEST(testAlterSplitFactors);
	CPPUNIT_TEST(testRedoAlterSplitFactors);
	CPPUNIT_TEST(testAlterIncrSize);
	CPPUNIT_TEST(testRedoAlterIncrSize);
	CPPUNIT_TEST(testAlterCompressRows);
	CPPUNIT_TEST(testRedoAlterCompressRows);
	CPPUNIT_TEST(testAlterHeapFixLen);
	CPPUNIT_TEST(testAlterColGrpDef);
	CPPUNIT_TEST(testRedoAlterColGrpDef);
	CPPUNIT_TEST(testAlterDictionaryArg);
	CPPUNIT_TEST(testRedoAlterDictionaryArg);
	CPPUNIT_TEST(testWrongCommand);
	CPPUNIT_TEST(testLockUpgradeFail);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	static Database * initDb();
	static void destroyDb(Database *db);

	static TableDef* getBlogDef(bool useMms);
	static void createBlog(Database *db, bool useMms);
	static void insertBlog(Database *db, uint numRows, bool startFromOne = false);
	static Record** populateBlog(Database *db, Table *table, uint numRows, bool doubleInsert, bool lobNotNull,bool startFromOne = false);

	static const char *DB_NAME;					/* ���ݿ��� */
	static const char *TABLE_NAME;				/* ���� */
	static const char *TABLE_PATH;				/* ��·�� */

protected:
	void testAlterUseMms();
	void testRedoAlterUseMms();
	void testAlterCacheUpdate();
	void testRedoAlterCacheUpdate();
	void testAlterCacheUpdateTime();
	void testRedoAlterCacheUpdateTime();
	void testAlterCachedColummns();
	void testRedoAlterCachedColummns();
	void testAlterCompressLobs();
	void testRedoAlterCompressLobs();
	void testAlterHeapPctFree();
	void testRedoAlterHeapPctFree();
	void testAlterSplitFactors();
	void testRedoAlterSplitFactors();
	void testAlterIncrSize();
	void testRedoAlterIncrSize();

	void testAlterCompressRows();
	void testRedoAlterCompressRows();
	void testAlterHeapFixLen();
	void testAlterColGrpDef();
	void testRedoAlterColGrpDef();
	void testAlterDictionaryArg();
	void testRedoAlterDictionaryArg();

	void testWrongCommand();
	void testLockUpgradeFail();
private:
	
	void runCommand() throw (NtseException);
	void checkValue(TableArgAlterCmdType cmdType, bool expect);
	void checkValue(TableArgAlterCmdType cmdType, int expect);
	void checkValue(TableArgAlterCmdType cmdType, const char *name, int expect);
	void prepareDataMisc(uint insertRowsEachTime = 10);
	void checkTable();
	void checkTableDef();
	void checkTableColoumnGroups(bool hasColGrp);
	u8 getNumColumnGroup();
	const LogEntry * checkAndFetchLastTblArgAlterLog(LsnType startLsn);
	void redoAlterTableArg(const LogEntry *log);
	void checkRunCommandFail();

private:
	
	static const char *INVALID_COMMAND_STRING;	/* һ����Ч���޸����� */

	static const char *COL_NAME_ID;				/* ID���� */
	static const char *COL_NAME_USER_ID;		/* USER_ID���� */
	static const char *COL_NAME_PUBLISH_TIME;	/* PUBLISH_TIME���� */
	static const char *COL_NAME_TITLE;			/* TITLE���� */
	static const char *COL_NAME_TAGS;			/* TAGS���� */
	static const char *COL_NAME_ABSTRACT;		/* ABSTRACT���� */
	static const char *COL_NAME_CONTENT;		/* CONTENT���� */

	static const char *ALTER_TABLE_CMD_COMM;		/* ���޸����������*/
	static string TEST_CMD_MMS_ENABLE;				/* ����MMS�������� */
	static string TEST_CMD_MMS_DISABLE;				/* ����MMS�������� */
	static string TEST_CMD_CACHE_UPDATE_ENABLE;		/* ����MMS��������������� */
	static string TEST_CMD_CACHE_UPDATE_DISABLE;	/* ����MMS������½������� */
	static string TEST_CMD_CACHE_UPDATE_TIME;		/* ����MMS�������ʱ�������� */
	static string TEST_CMD_CACHE_UPDATE_TIME2;		/* ����MMS�������ʱ�������� */
	static string TEST_CMD_CACHED_COLUMNS_ENABLE;	/* ����MMS�������������� */
	static string TEST_CMD_CACHED_COLUMNS_DISABLE;	/* ����MMS�����н������� */
	static string TEST_CMD_COMPRESS_LOBS_ENABLE;	/* ���Կ��������ѹ������ */
	static string TEST_CMD_COMPRESS_LOBS_DISABLE;	/* ���Խ��ô����ѹ������ */
	static string TEST_CMD_SET_HEAP_PCT_FREE;		/* ���Կ��������ѹ������ */
	static string TEST_CMD_SET_HEAP_PCT_FREE2;		/* ���Կ��������ѹ������ */
	static string TEST_CMD_SET_SPLIT_FACTORS;		/* �����޸ķ���ϵ������ */
	static string TEST_CMD_SET_SPLIT_FACTORS2;		/* �����޸ķ���ϵ������ */
	static string TEST_CMD_SET_SPLIT_FACTOR_AUTO;	/* �����޸ķ���ϵ���Զ����� */
	static string TEST_CMD_INCREASE_SIZE;			/* �����޸���չ��С */
	static string TEST_CMD_INCREASE_SIZE2;			/* �����޸���չ��С */
	static string TEST_CMD_COMPRESS_ROWS;           /* �������ü�¼ѹ�� */
	static string TEST_CMD_COMPRESS_ROWS2;          /* ���Խ��ü�¼ѹ�� */
	static string TEST_CMD_HEAP_FIXLEN;             /* ���Ը��Ķ�����Ϊ������ */
	static string TEST_CMD_HEAP_FIXLEN2;            /* ���Ը��Ķ�����Ϊ�䳤�� */
	static string TEST_CMD_COL_GROUP_DEF;           /* ���Ի��������� */
	static string TEST_CMD_COMPRESS_DICT_SIZE;      /* �����޸�ѹ���ֵ��С */
	static string TEST_CMD_COMPRESS_DICT_MIN_LEN;   /* �����޸��ֵ���С���� */
	static string TEST_CMD_COMPRESS_DICT_MAX_LEN;   /* �����޸��ֵ���󳤶� */
	static string TEST_CMD_COMPRESS_THRESHOLD;      /* �����޸�ѹ���ȷ�ֵ */

	static Config	m_config;	/* ���ݿ����� */
	Database *m_db;		/* ���Ե����ݿ� */
	const char *m_cmd;	/* ���Ե����� */
	Parser *m_parser;	/* ���Ե���������� */
	int m_timeout;		/* ���Ի�ȡ����ʱʱ��, ��λ�� */

	friend class TableAlterArgBigTestCase;
	friend class TableInsertTestTask;
};

class TableAlterArgBigTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(TableAlterArgBigTestCase);
	CPPUNIT_TEST(testWithTableInsert);
	CPPUNIT_TEST(testRedoAlterTableArgs);

	CPPUNIT_TEST_SUITE_END();

public:		
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testWithTableInsert();
	void testRedoAlterTableArgs();
	
private:
	void alterTableArgs(uint runTimeSeconds);
private:
	TableAlterArgTestCase *m_instance;						/* ���޸Ĳ�������ʵ�� */
	static const uint RUN_TIME_SECONDS = 60 * 10;			/* ��������ʱ�䣬��λ�� */
	static const uint ALTER_INTERVAL_MS = 2000;				/* ���޸Ĳ�����ִ�м���¼�, ��λ���� */
	static const uint ALTER_INNER_TESTS_INTERVAL_MS = 100;	/* ���޸Ĳ����������ִ�м��ʱ�䣬��λ���� */	
};



#endif
