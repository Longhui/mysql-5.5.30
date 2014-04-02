/**
 * 测试在线修改索引
 *
 * @author 辛颖伟(xinyingwei@corp.netease.com, xinyingwei@163.org)
 */

#ifndef _NTSETEST_IDXPREALTER_H_
#define _NTSETEST_IDXPREALTER_H_

#include <cppunit/extensions/HelperMacros.h>
#include <string>

#include "api/TNTTable.h"
#include "api/TNTDatabase.h"
#include "api/TestHelper.h"
#include "api/IdxPreAlter.h"

using namespace ntse;

class IdxPreAlterTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IdxPreAlterTestCase);
	CPPUNIT_TEST(testParAddCmd);
	CPPUNIT_TEST(testParDropCmd);
	CPPUNIT_TEST(testCreateOnlineIndex);
	CPPUNIT_TEST(testDeleteOnlineIndex);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

	static TNTDatabase * initDb();
	static void destroyDb(TNTDatabase *db);

	static TableDef* getTestBlogDef();
	static void createTestBlog(TNTDatabase *db);

	static const char *DB_NAME;
	static const char *TABLE_NAME;
	//static const char *TABLE_PATH;

protected:
	void testParAddCmd();
	void testParDropCmd();
	void testCreateOnlineIndex();
	void testDeleteOnlineIndex();

private:
	void runParAddCmd(AddIndexInfo &add) throw (NtseException);
	void runParDropCmd(DropIndexInfo &drop) throw (NtseException);
	void runAddOnlineIndex() throw (NtseException);
	void runAddMysqlIndex(u16 numIndice) throw (NtseException);
	void runDropIndex() throw (NtseException);
	
	void checkTable();
	void checkTableDef();
	void checkOnlineIndexCnt(int expect);

	IndexDef **createMysqlIndexDefs(Session *session, TableDef *tableDef);

	static const char *COL_NAME_ID;				/* ID列名 */
	static const char *COL_NAME_USER_ID;		/* USER_ID列名 */
	static const char *COL_NAME_PUB_TIME;		/* PUBLISH_TIME列名 */
	static const char *COL_NAME_TITLE;			/* TITLE列名 */
	static const char *COL_NAME_TAGS;			/* TAGS列名 */
	static const char *COL_NAME_ABSTRACT;		/* ABSTRACT列名 */
	static const char *COL_NAME_CONTENT;		/* CONTENT列名 */
	static const char *COL_NAME_INVALID;		/* 无效的列名 */

	static const char *IDX_NAME_PUBTIME;		/* 索引名IDX_PUBTIME */
	static const char *IDX_NAME_TITLE_TAGS;		/* 索引名IDX_TITLE_TAGS */
	static const char *IDX_NAME_PREFIX_TITLE_TAGS; /* 索引名IDX_PREFIX_TITLE_TAGS */
	static const char *IDX_NAME_USERID;			/* 索引名IDX_USERID */
	static const char *IDX_NAME_ID_PUB;			/* 索引名IDX_ID_PUB */
	static const char *IDX_NAME_INVALID;		/* 无效的索引名 */

	static const char *ADD_INDEX_CMD_COMM;		/* 添加在线索引命令公共部分*/
	static const char *DROP_INDEX_CMD_COMM;		/* 删除在线索引命令公共部分*/

	static const char *INVALID_ADD_CMD_A;		/* 非法的添加在线索引命令A */
	static const char *INVALID_ADD_CMD_B;		/* 非法的添加在线索引命令B */
	static const char *INVALID_ADD_CMD_C;		/* 非法的添加在线索引命令C */
	static const char *INVALID_ADD_CMD_D;		/* 非法的添加在线索引命令D */
	static const char *INVALID_ADD_CMD_E;		/* 非法的添加在线索引命令E */
	static const char *INVALID_ADD_CMD_F;		/* 非法的添加在线索引命令E */
	static const char *INVALID_DROP_CMD_A;		/* 非法的删除在线索引命令A */
	static const char *INVALID_DROP_CMD_B;		/* 非法的删除在线索引命令B */
	static const char *INVALID_DROP_CMD_C;		/* 非法的删除在线索引命令C */
	static const char *INVALID_DROP_CMD_D;		/* 非法的删除在线索引命令D */

	static string ADD_INDEX_CASE_A;				/* 添加在线索引用例A*/
	static string ADD_INDEX_CASE_B;				/* 添加在线索引用例B*/
	static string ADD_INDEX_CASE_C;				/* 添加在线索引用例C*/
	static string ADD_INDEX_CASE_D;				/* 添加在线索引用例D*/
	static string ADD_INDEX_CASE_E;				/* 添加在线索引用例E*/

	static string DROP_INDEX_CASE_A;			/* 删除在线索引用例A*/
	static string DROP_INDEX_CASE_B;			/* 删除在线索引用例B*/
	static string DROP_INDEX_CASE_C;			/* 删除在线索引用例C*/
	static string DROP_INDEX_CASE_D;			/* 删除在线索引用例D*/
	static string DROP_INDEX_CASE_E;			/* 删除在线索引用例E*/


	static string TEST_CMD_ADD_A;				/* 测试添加在线索引命令A */
	static string TEST_CMD_ADD_B;				/* 测试添加在线索引命令B */
	static string TEST_CMD_ADD_C;				/* 测试添加在线索引命令C */
	static string TEST_CMD_ADD_D;				/* 测试添加在线索引命令D */
	static string TEST_CMD_ADD_E;				/* 测试添加在线索引命令D */
	static string TEST_CMD_DROP_A;				/* 测试删除在线索引命令A */
	static string TEST_CMD_DROP_B;				/* 测试删除在线索引命令B */
	static string TEST_CMD_DROP_C;				/* 测试删除在线索引命令C */
	static string TEST_CMD_DROP_D;				/* 测试删除在线索引命令D */
	static string TEST_CMD_DROP_E;				/* 测试删除在线索引命令D */

	static TNTConfig m_config;						/* 数据库配置 */

	TNTDatabase *m_db;								/* 测试的数据库 */
	Parser *m_parser;							/* 测试的命令解析器 */
	const char *m_cmd;							/* 测试的相关命令 */

};
#endif
