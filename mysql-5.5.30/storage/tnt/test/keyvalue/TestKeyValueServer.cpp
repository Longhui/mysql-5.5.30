/*
*	测试KeyValueServer的APIs
*
*	@author	廖定柏(liaodingbai@corp.netease.com)
*/
#ifdef NTSE_KEYVALUE_SERVER

#include "keyvalue/TestKeyValueServer.h"
#include "util/File.h"
#include "misc/TableDef.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "Test.h"

using namespace ntse;
using namespace std;

/**
*	设置ntse数据库的根目录路径
*/
const string KeyValueServerTestCase::baseDir = "kv-db";

const char* KeyValueServerTestCase::getName() {
	return "Key-value server test.";
}

const char* KeyValueServerTestCase::getDescription() {
	return "Test key-value server operations.";
}

bool KeyValueServerTestCase::isBig() {
	return false;
}

/**
*	Set up the test environment.
*/
void KeyValueServerTestCase::setUp()	{
	/** 数据库的准备 */
	m_db = NULL;
	m_table = NULL;
	File dir(baseDir.c_str());
	dir.rmdir(true);
	dir.mkdir();
	m_config.setBasedir(baseDir.c_str());
	m_config.m_logLevel = EL_WARN;
	m_config.m_pageBufSize = 500;
	m_config.m_mmsSize = 2000;
	m_config.m_logFileSize = 1024 * 128;
	EXCPT_OPER(m_db = Database::open(&m_config, true));

	/** 数据表的准备 */
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, baseDir.c_str(), "KeyValueTest");
	tdb.addColumn("id", CT_BIGINT, false);
	tdb.addColumnS("name", CT_VARCHAR, 20, false);
	tdb.addColumn("currency", CT_FLOAT, true);
	tdb.addColumn("balance", CT_DOUBLE, false);
	tdb.addColumn("age", CT_INT, false);
	tdb.addColumnS("address", CT_VARCHAR, 50, false);
	tdb.addIndex("PRIMARY", true, true, "id", "name", NULL);
	TableDef *tableDef = tdb.getTableDef();

	m_table = new TblInterface(m_db, "KeyValueTest");
	EXCPT_OPER(m_table->create(tableDef));
	m_table->open();

}

/**
*	Clear the test environment.
*/
void KeyValueServerTestCase::tearDown() {
	if (m_table) {
		if (m_table->getTable())
			m_table->close();
		delete m_table;
	}

	if (m_db) {
		m_db->close(false, false);
		delete m_db;
	}

	File dir(baseDir.c_str());
	dir.rmdir(true);
}

/*
 *	测试 simple server
 */
void KeyValueServerTestCase::testSimpleRun()	{
	ThriftConfig config;
	config.port = 9191;
	config.serverType = 0;

	KeyValueServer instance(&config, m_db);

	instance.start();

	instance.msleep(1000);

	CPPUNIT_ASSERT(instance.isOpen() == true);

	instance.close();
}

/*
*	测试 thread pool server
*/
void KeyValueServerTestCase::testThreadPoolRun()	{
	ThriftConfig config;
	config.port = 9191;
	config.serverType = 1;
	config.threadNum = 5;

	KeyValueServer instance(&config, m_db);

	instance.start();

	instance.msleep(1000);

	CPPUNIT_ASSERT(instance.isOpen() == true);

	instance.close();
	instance.msleep(1000);
}

/*
*	测试 threaded server
*/
void KeyValueServerTestCase::testThreadedRun()	{
	ThriftConfig config;
	config.port = 9292;
	config.serverType = 2;

	KeyValueServer instance(&config, m_db);

	instance.start();

	instance.msleep(1000);

	CPPUNIT_ASSERT(instance.isOpen() == true);

	instance.close();
	instance.msleep(1000);
}

/*
 *	测试KeyValueServer::close函数
 */
void KeyValueServerTestCase::testClose()	{
	ThriftConfig config;
	config.port = 9191;
	config.serverType = 2;

	KeyValueServer instance(&config, m_db);

	/*
	 *	如果服务已经打开，应run()直接返回
	 */
	instance.start();

	instance.msleep(1000);

	CPPUNIT_ASSERT(instance.isOpen() == true);
	
	instance.close();
	instance.msleep(1000);
	CPPUNIT_ASSERT(instance.isOpen() == false);
}

#endif