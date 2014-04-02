/**
 * 测试控制修改表参数测试功能
 *
 * @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
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
 * LsnKeeper类，保留日志.
 */
class LsnKeeper {
public:
	/**
	 * 构造函数, 构造<code>LsnKeeper</code>对象. 执行保留从当前之后的LSN日志.
	 * @param db <code>Database</code>实例，不为NULL.
	 */
	LsnKeeper(const Database *db) {
		assert(NULL != db);
		m_db = db;
		m_keepLsn = m_db->getTxnlog()->tailLsn();
		m_token = keepLsn(m_keepLsn);
	}

	/**
	 * 获取预留开始的LSN.
	 * @return 预留开始的LSN.
	 */
	int getKeepLsn() const {
		return (int)m_keepLsn;
	}

	/**
	 * 析构函数. 取消日志LSN保留.
	 */
	~LsnKeeper() {
		clearKeepLsn(m_token);
	}
private:
	/**
	 * 保留当前lsn之后的日志.
	 * @return 更新成功返回一个句柄，更新失败返回值小于0.
	 */
	int keepLsn(LsnType keepLsn) {
		int token = m_db->getTxnlog()->setOnlineLsn(keepLsn);
		assert(token >= 0);
		return token;
	}
	/**
	 * 清理lsn保留.
	 * @param token TableAlterArgTestCase#keepLsn()返回的token.
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

	static const char *DB_NAME;					/* 数据库名 */
	static const char *TABLE_NAME;				/* 表名 */
	static const char *TABLE_PATH;				/* 表路径 */

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
	
	static const char *INVALID_COMMAND_STRING;	/* 一个无效的修改命令 */

	static const char *COL_NAME_ID;				/* ID列名 */
	static const char *COL_NAME_USER_ID;		/* USER_ID列名 */
	static const char *COL_NAME_PUBLISH_TIME;	/* PUBLISH_TIME列名 */
	static const char *COL_NAME_TITLE;			/* TITLE列名 */
	static const char *COL_NAME_TAGS;			/* TAGS列名 */
	static const char *COL_NAME_ABSTRACT;		/* ABSTRACT列名 */
	static const char *COL_NAME_CONTENT;		/* CONTENT列名 */

	static const char *ALTER_TABLE_CMD_COMM;		/* 表修改命令公共部分*/
	static string TEST_CMD_MMS_ENABLE;				/* 测试MMS开启命令 */
	static string TEST_CMD_MMS_DISABLE;				/* 测试MMS禁用命令 */
	static string TEST_CMD_CACHE_UPDATE_ENABLE;		/* 测试MMS缓存更新启用命令 */
	static string TEST_CMD_CACHE_UPDATE_DISABLE;	/* 测试MMS缓存更新禁用命令 */
	static string TEST_CMD_CACHE_UPDATE_TIME;		/* 测试MMS缓存更新时间间隔命令 */
	static string TEST_CMD_CACHE_UPDATE_TIME2;		/* 测试MMS缓存更新时间间隔命令 */
	static string TEST_CMD_CACHED_COLUMNS_ENABLE;	/* 测试MMS缓存列启用命令 */
	static string TEST_CMD_CACHED_COLUMNS_DISABLE;	/* 测试MMS缓存列禁用命令 */
	static string TEST_CMD_COMPRESS_LOBS_ENABLE;	/* 测试开启大对象压缩命令 */
	static string TEST_CMD_COMPRESS_LOBS_DISABLE;	/* 测试禁用大对象压缩命令 */
	static string TEST_CMD_SET_HEAP_PCT_FREE;		/* 测试开启大对象压缩命令 */
	static string TEST_CMD_SET_HEAP_PCT_FREE2;		/* 测试开启大对象压缩命令 */
	static string TEST_CMD_SET_SPLIT_FACTORS;		/* 测试修改分裂系数命令 */
	static string TEST_CMD_SET_SPLIT_FACTORS2;		/* 测试修改分裂系数命令 */
	static string TEST_CMD_SET_SPLIT_FACTOR_AUTO;	/* 测试修改分裂系数自动设置 */
	static string TEST_CMD_INCREASE_SIZE;			/* 测试修改扩展大小 */
	static string TEST_CMD_INCREASE_SIZE2;			/* 测试修改扩展大小 */
	static string TEST_CMD_COMPRESS_ROWS;           /* 测试启用记录压缩 */
	static string TEST_CMD_COMPRESS_ROWS2;          /* 测试禁用记录压缩 */
	static string TEST_CMD_HEAP_FIXLEN;             /* 测试更改堆类型为定长堆 */
	static string TEST_CMD_HEAP_FIXLEN2;            /* 测试更改堆类型为变长堆 */
	static string TEST_CMD_COL_GROUP_DEF;           /* 测试划分属性组 */
	static string TEST_CMD_COMPRESS_DICT_SIZE;      /* 测试修改压缩字典大小 */
	static string TEST_CMD_COMPRESS_DICT_MIN_LEN;   /* 测试修改字典最小长度 */
	static string TEST_CMD_COMPRESS_DICT_MAX_LEN;   /* 测试修改字典最大长度 */
	static string TEST_CMD_COMPRESS_THRESHOLD;      /* 测试修改压缩比阀值 */

	static Config	m_config;	/* 数据库配置 */
	Database *m_db;		/* 测试的数据库 */
	const char *m_cmd;	/* 测试的命令 */
	Parser *m_parser;	/* 测试的命令解析器 */
	int m_timeout;		/* 测试获取锁超时时间, 单位秒 */

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
	TableAlterArgTestCase *m_instance;						/* 表修改测试用例实例 */
	static const uint RUN_TIME_SECONDS = 60 * 10;			/* 测试运行时间，单位秒 */
	static const uint ALTER_INTERVAL_MS = 2000;				/* 表修改参数的执行间隔事件, 单位毫秒 */
	static const uint ALTER_INNER_TESTS_INTERVAL_MS = 100;	/* 表修改参数函数间的执行间隔时间，单位毫秒 */	
};



#endif
