/**
 * ���������޸�����
 *
 * @author ��ӱΰ(xinyingwei@corp.netease.com, xinyingwei@163.org)
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

	static const char *COL_NAME_ID;				/* ID���� */
	static const char *COL_NAME_USER_ID;		/* USER_ID���� */
	static const char *COL_NAME_PUB_TIME;		/* PUBLISH_TIME���� */
	static const char *COL_NAME_TITLE;			/* TITLE���� */
	static const char *COL_NAME_TAGS;			/* TAGS���� */
	static const char *COL_NAME_ABSTRACT;		/* ABSTRACT���� */
	static const char *COL_NAME_CONTENT;		/* CONTENT���� */
	static const char *COL_NAME_INVALID;		/* ��Ч������ */

	static const char *IDX_NAME_PUBTIME;		/* ������IDX_PUBTIME */
	static const char *IDX_NAME_TITLE_TAGS;		/* ������IDX_TITLE_TAGS */
	static const char *IDX_NAME_PREFIX_TITLE_TAGS; /* ������IDX_PREFIX_TITLE_TAGS */
	static const char *IDX_NAME_USERID;			/* ������IDX_USERID */
	static const char *IDX_NAME_ID_PUB;			/* ������IDX_ID_PUB */
	static const char *IDX_NAME_INVALID;		/* ��Ч�������� */

	static const char *ADD_INDEX_CMD_COMM;		/* ��������������������*/
	static const char *DROP_INDEX_CMD_COMM;		/* ɾ�������������������*/

	static const char *INVALID_ADD_CMD_A;		/* �Ƿ������������������A */
	static const char *INVALID_ADD_CMD_B;		/* �Ƿ������������������B */
	static const char *INVALID_ADD_CMD_C;		/* �Ƿ������������������C */
	static const char *INVALID_ADD_CMD_D;		/* �Ƿ������������������D */
	static const char *INVALID_ADD_CMD_E;		/* �Ƿ������������������E */
	static const char *INVALID_ADD_CMD_F;		/* �Ƿ������������������E */
	static const char *INVALID_DROP_CMD_A;		/* �Ƿ���ɾ��������������A */
	static const char *INVALID_DROP_CMD_B;		/* �Ƿ���ɾ��������������B */
	static const char *INVALID_DROP_CMD_C;		/* �Ƿ���ɾ��������������C */
	static const char *INVALID_DROP_CMD_D;		/* �Ƿ���ɾ��������������D */

	static string ADD_INDEX_CASE_A;				/* ���������������A*/
	static string ADD_INDEX_CASE_B;				/* ���������������B*/
	static string ADD_INDEX_CASE_C;				/* ���������������C*/
	static string ADD_INDEX_CASE_D;				/* ���������������D*/
	static string ADD_INDEX_CASE_E;				/* ���������������E*/

	static string DROP_INDEX_CASE_A;			/* ɾ��������������A*/
	static string DROP_INDEX_CASE_B;			/* ɾ��������������B*/
	static string DROP_INDEX_CASE_C;			/* ɾ��������������C*/
	static string DROP_INDEX_CASE_D;			/* ɾ��������������D*/
	static string DROP_INDEX_CASE_E;			/* ɾ��������������E*/


	static string TEST_CMD_ADD_A;				/* �������������������A */
	static string TEST_CMD_ADD_B;				/* �������������������B */
	static string TEST_CMD_ADD_C;				/* �������������������C */
	static string TEST_CMD_ADD_D;				/* �������������������D */
	static string TEST_CMD_ADD_E;				/* �������������������D */
	static string TEST_CMD_DROP_A;				/* ����ɾ��������������A */
	static string TEST_CMD_DROP_B;				/* ����ɾ��������������B */
	static string TEST_CMD_DROP_C;				/* ����ɾ��������������C */
	static string TEST_CMD_DROP_D;				/* ����ɾ��������������D */
	static string TEST_CMD_DROP_E;				/* ����ɾ��������������D */

	static TNTConfig m_config;						/* ���ݿ����� */

	TNTDatabase *m_db;								/* ���Ե����ݿ� */
	Parser *m_parser;							/* ���Ե���������� */
	const char *m_cmd;							/* ���Ե�������� */

};
#endif
