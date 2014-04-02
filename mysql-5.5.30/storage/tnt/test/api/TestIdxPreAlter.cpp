/**
 * ���������޸�����
 *
 * @author ��ӱΰ(xinyingwei@corp.netease.com, xinyingwei@163.org)
 */

#include "misc/TableDef.h"
#include "misc/Parser.h"
#include "util/SmartPtr.h"
#include "api/TestIdxPreAlter.h"
#include "Test.h"

using namespace std;
using namespace ntse;

const char * IdxPreAlterTestCase::DB_NAME = "testntse";
const char * IdxPreAlterTestCase::TABLE_NAME = "testblog";
//const char * IdxPreAlterTestCase::TABLE_PATH = "./testntse/testblog";

const char * IdxPreAlterTestCase::COL_NAME_ID = "ID";
const char * IdxPreAlterTestCase::COL_NAME_USER_ID = "UserID";
const char * IdxPreAlterTestCase::COL_NAME_PUB_TIME = "PublishTime";
const char * IdxPreAlterTestCase::COL_NAME_TITLE = "Title";
const char * IdxPreAlterTestCase::COL_NAME_TAGS = "Tags";
const char * IdxPreAlterTestCase::COL_NAME_ABSTRACT = "Abstract";
const char * IdxPreAlterTestCase::COL_NAME_CONTENT = "Content";
const char * IdxPreAlterTestCase::COL_NAME_INVALID = "Invalid";

const char * IdxPreAlterTestCase::IDX_NAME_PUBTIME = "IDX_PUBTIME";
const char * IdxPreAlterTestCase::IDX_NAME_TITLE_TAGS = "IDX_TITLE_TAGS";
const char * IdxPreAlterTestCase::IDX_NAME_PREFIX_TITLE_TAGS = "IDX_PREFIX_TITLE_TAGS";
const char * IdxPreAlterTestCase::IDX_NAME_USERID = "IDX_USERID";
const char * IdxPreAlterTestCase::IDX_NAME_ID_PUB = "IDX_ID_PUB";
const char * IdxPreAlterTestCase::IDX_NAME_INVALID = "IDX_INVALID";

const char *IdxPreAlterTestCase::ADD_INDEX_CMD_COMM = "add index on ";
const char *IdxPreAlterTestCase::DROP_INDEX_CMD_COMM = "drop index on ";

const char *IdxPreAlterTestCase::INVALID_ADD_CMD_A = "add index on testntse.testblog IDX_PUBTIME(PublishTime, ), IDX_TITLE_TAGS(Title,Tags)";
const char *IdxPreAlterTestCase::INVALID_ADD_CMD_B = "add on testntse.testblog IDX_PUBTIME(PublishTime), IDX_TITLE_TAGS:Title,Tags";
const char *IdxPreAlterTestCase::INVALID_ADD_CMD_C = "add index testntse,testblog IDX_PUBTIME:PublishTime, IDX_TITLE_TAGS(Title,Tags";
const char *IdxPreAlterTestCase::INVALID_ADD_CMD_D = "add index on testntse,testblog IDX_PUBTIME(PublishTime), IDX_TITLE_TAGS (Title, Tags)";
const char *IdxPreAlterTestCase::INVALID_ADD_CMD_E = "add on testntse.testblog IDX_TITLE_TAGS(Title(,Tags))";
const char *IdxPreAlterTestCase::INVALID_ADD_CMD_F = "add on testntse.testblog IDX_TITLE_TAGS(Title(0),Tags)";

const char *IdxPreAlterTestCase::INVALID_DROP_CMD_A = "drop index on testntse.testblog IDX_PUBTIME, IDX_TITLE_TAGS)";
const char *IdxPreAlterTestCase::INVALID_DROP_CMD_B = "drop on testntse.testblog :IDX_PUBTIME, IDX_TITLE_TAGS";
const char *IdxPreAlterTestCase::INVALID_DROP_CMD_C = "drop index testntse,testblog IDX_PUBTIME, IDX_TITLE_TAGS";
const char *IdxPreAlterTestCase::INVALID_DROP_CMD_D = "drop index on testntse,testblog IDX_PUBTIME,IDX_TITLE_TAGS";

string IdxPreAlterTestCase::ADD_INDEX_CASE_A = string(IDX_NAME_PUBTIME) + "(" + string(COL_NAME_PUB_TIME) + ")";
string IdxPreAlterTestCase::ADD_INDEX_CASE_B = string(IDX_NAME_TITLE_TAGS) + "(" + string(COL_NAME_TITLE) + "," + string(COL_NAME_TAGS) + ")";
string IdxPreAlterTestCase::ADD_INDEX_CASE_C = string(IDX_NAME_USERID) + "(" + string(COL_NAME_USER_ID) + "), " + string(IDX_NAME_INVALID) + "(" + string(COL_NAME_USER_ID) + "," + string(COL_NAME_INVALID) + ")";
string IdxPreAlterTestCase::ADD_INDEX_CASE_D = string(IDX_NAME_USERID) + "(" + string(COL_NAME_USER_ID) + "), " + string(IDX_NAME_ID_PUB) + "(" + string(COL_NAME_USER_ID) + "," + string(COL_NAME_PUB_TIME) + ")";
string IdxPreAlterTestCase::DROP_INDEX_CASE_A = string(IDX_NAME_PUBTIME);
string IdxPreAlterTestCase::DROP_INDEX_CASE_B = string(IDX_NAME_TITLE_TAGS);
string IdxPreAlterTestCase::DROP_INDEX_CASE_C = string(IDX_NAME_USERID) + ", " + string(IDX_NAME_INVALID);
string IdxPreAlterTestCase::DROP_INDEX_CASE_D = string(IDX_NAME_USERID) + ", " + string(IDX_NAME_ID_PUB);


string IdxPreAlterTestCase::ADD_INDEX_CASE_E =  string(IDX_NAME_PREFIX_TITLE_TAGS) + "(" + string(COL_NAME_TITLE) + "(3)" + "," + string(COL_NAME_TAGS) + "(5)" + ")";
string IdxPreAlterTestCase::DROP_INDEX_CASE_E = string(IDX_NAME_PREFIX_TITLE_TAGS);

string IdxPreAlterTestCase::TEST_CMD_ADD_A = string(ADD_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(ADD_INDEX_CASE_A);
string IdxPreAlterTestCase::TEST_CMD_ADD_B = string(ADD_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(ADD_INDEX_CASE_B);
string IdxPreAlterTestCase::TEST_CMD_ADD_C = string(ADD_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(ADD_INDEX_CASE_C) ;
string IdxPreAlterTestCase::TEST_CMD_ADD_D = string(ADD_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(ADD_INDEX_CASE_D) ;
string IdxPreAlterTestCase::TEST_CMD_ADD_E = string(ADD_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(ADD_INDEX_CASE_E);

string IdxPreAlterTestCase::TEST_CMD_DROP_A = string(DROP_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(DROP_INDEX_CASE_A);
string IdxPreAlterTestCase::TEST_CMD_DROP_B = string(DROP_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(DROP_INDEX_CASE_B);
string IdxPreAlterTestCase::TEST_CMD_DROP_C = string(DROP_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(DROP_INDEX_CASE_C);
string IdxPreAlterTestCase::TEST_CMD_DROP_D = string(DROP_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(DROP_INDEX_CASE_D);
string IdxPreAlterTestCase::TEST_CMD_DROP_E = string(DROP_INDEX_CMD_COMM) + string(DB_NAME) + "." + string(TABLE_NAME) + " " + string(DROP_INDEX_CASE_E);

TNTConfig	IdxPreAlterTestCase::m_config;	/* ���ݿ����� */

/** 
 * ����������
 */
const char* IdxPreAlterTestCase::getName() {
	return "Add/Drop Online Index Test";
}

/**
 * ������������
 */
const char* IdxPreAlterTestCase::getDescription() {
	return "Test alter online index functions";
}

/** 
 * ��������
 * @return false С�͵�Ԫ����
 */
bool IdxPreAlterTestCase::isBig() {
	return false;
}

/** 
 * Set up context before start a test.
 * @see <code>TestFixture::setUp()</code>
 */
void IdxPreAlterTestCase::setUp() {
	//��ʼ�����ݿ�
	m_db = initDb();
	//����
	EXCPT_OPER(createTestBlog(m_db));
}

/** 
 * Clean up after the test run.
 * @see <code>TestFixture::tearDown()</code>
 */
void IdxPreAlterTestCase::tearDown() {
	destroyDb(m_db);
}

/** 
 * ��ʼ��DB������DB
 *
 * @return ���ݿ�ʵ��
 */
TNTDatabase * IdxPreAlterTestCase::initDb() {
	TNTDatabase *db = NULL;
	TNTDatabase::drop(".", ".");
	File dir(DB_NAME);
	dir.rmdir(true);
	dir.mkdir();
	EXCPT_OPER(db = TNTDatabase::open(&m_config, true, 0));
	Table::drop(db->getNtseDb(), string(string(DB_NAME) + "/" + TABLE_NAME).c_str());
	return db;
}

/** 
 * �������ݿ�
 */
void IdxPreAlterTestCase::destroyDb(TNTDatabase *db) {
	if (db) {
		//Table::drop(db->getNtseDb(), string(string(DB_NAME) + "/" + TABLE_NAME).c_str());
		db->close();
		delete db;
	}
	TNTDatabase::drop(".", ".");
	File dir(DB_NAME);
	dir.rmdir(true);
}

/**
 * ����testblog����
 *
 * @return testblog����
 */
TableDef* IdxPreAlterTestCase::getTestBlogDef() {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, DB_NAME, TABLE_NAME);

	builder->addColumn(COL_NAME_ID, CT_BIGINT, false)->addColumn(COL_NAME_USER_ID, CT_BIGINT, false);
	builder->addColumn(COL_NAME_PUB_TIME, CT_BIGINT);
	builder->addColumnS(COL_NAME_TITLE, CT_VARCHAR, 411)->addColumnS(COL_NAME_TAGS, CT_VARCHAR, 200);
	builder->addColumn(COL_NAME_ABSTRACT, CT_SMALLLOB)->addColumn(COL_NAME_CONTENT, CT_MEDIUMLOB);
	builder->addIndex("PRIMARY", true, true, false, COL_NAME_ID, 0, NULL);
	TableDef *tableDef = builder->getTableDef();
	delete builder;
	builder = NULL;
	return tableDef;
}

/** ����testblog����
CREATE TABLE testblog (
  ID bigint NOT NULL,
  UserID bigint NOT NULL,
  PublishTime bigint,
  Title varchar(511),
  Tags varchar(200),
  Abstract text,
  Content mediumtext,
  PRIMARY KEY (ID),
  //KEY IDX_PUBTIME(PublishTime),
  //KEY IDX_TITLE_TAGS(Title, Tags)
)ENGINE=NTSE;
 @param db		���ݿ�
 */
void IdxPreAlterTestCase::createTestBlog(TNTDatabase *db) {
	assert(NULL != db);
	TNTTransaction trx;
	Database *ntsedb = db->getNtseDb();
	TableDef *tableDef = getTestBlogDef();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("IdxPreAlterTestCase::createTestBlog", conn);
	session->setTrans(&trx);
	db->createTable(session, string(string(ntsedb->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TABLE_NAME).c_str(), tableDef);
	session->setTrans(NULL);
	delete tableDef;
	tableDef = NULL;
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}

/**
 * ���������������ntse_command���������
 *
 * @throw <code>NtseException</code> ������ʧ��
 */
void IdxPreAlterTestCase::runParAddCmd(AddIndexInfo &add) throw (NtseException) {
	AutoPtr<Parser> parser(new Parser(m_cmd));
	const char *token = parser->nextToken();
	CPPUNIT_ASSERT(!System::stricmp(token, "add"));
	m_parser = parser.detatch();

	Connection *conn = m_db->getNtseDb()->getConnection(false);
	IdxPreAlter idxPreAlter(m_db, conn, m_parser);
	idxPreAlter.parAddCmd(add);
}

/**
 * ����ɾ����������ntse_command���������
 *
 * @throw <code>NtseException</code> ������ʧ��
 */
void IdxPreAlterTestCase::runParDropCmd(DropIndexInfo &drop) throw (NtseException) {
	AutoPtr<Parser> parser(new Parser(m_cmd));
	const char *token = parser->nextToken();
	CPPUNIT_ASSERT(!System::stricmp(token, "drop"));
	m_parser = parser.detatch();

	Connection *conn = m_db->getNtseDb()->getConnection(false);
	IdxPreAlter idxPreAlter(m_db, conn, m_parser);
	idxPreAlter.parDropCmd(drop);
}

/**
 * �����������ntse_command����ִ�д�������
 *
 * @throw <code>NtseException</code> ������ʧ��
 */
void IdxPreAlterTestCase::runAddOnlineIndex() throw (NtseException) {
	CPPUNIT_ASSERT(NULL != m_cmd);
	AutoPtr<Parser> parser(new Parser(m_cmd));
	const char *token = parser->nextToken();
	CPPUNIT_ASSERT(!System::stricmp(token, "add"));
	m_parser = parser.detatch();
	Connection *conn = m_db->getNtseDb()->getConnection(false, __FUNCTION__);
	//���ߴ�������
	IdxPreAlter idxPreAlter(m_db, conn, m_parser);
	idxPreAlter.createOnlineIndex();
	m_db->getNtseDb()->freeConnection(conn);
}

/**
 * �������mysql����ִ�д�������
 *
 * @throw <code>NtseException</code> ������ʧ��
 */
void IdxPreAlterTestCase::runAddMysqlIndex(u16 numIndice) throw (NtseException) {
	CPPUNIT_ASSERT(NULL != m_cmd);
	uint i = 0;
	AutoPtr<Parser> parser(new Parser(m_cmd));
	const char *token = parser->nextToken();
	CPPUNIT_ASSERT(!System::stricmp(token, "add"));
	m_parser = parser.detatch();
	
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("IdxPreAlterTestCase::runAddMysqlIndex", conn);
	TNTTransaction trx;
	session->setTrans(&trx);
	TNTTable *table = m_db->openTable(session, string(string(m_db->getNtseDb()->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TABLE_NAME).c_str());

	const IndexDef **indexDefs = (const IndexDef **) createMysqlIndexDefs(session, table->getNtseTable()->getTableDef());

	//���mysql����
	try {
		m_db->addIndex(session, table, numIndice, indexDefs);
	} catch (NtseException &e) {
		for (i = 0; i < numIndice; i++) {
			CPPUNIT_ASSERT(indexDefs[i] != NULL);
			delete indexDefs[i];
		}
		m_db->closeTable(session,table);
		m_db->getNtseDb()->getSessionManager()->freeSession(session);
		m_db->getNtseDb()->freeConnection(conn);
		throw e;
	}

	for (i = 0; i < numIndice; i++) {
		CPPUNIT_ASSERT(indexDefs[i] != NULL);
		delete indexDefs[i];
	}
	m_db->closeTable(session,table);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
}

/**
 * ����mysql����õ���indexDefs
 *
 * @return indexDefs��������
 */
IndexDef **IdxPreAlterTestCase::createMysqlIndexDefs(Session *session, TableDef *tableDef) {
	//TNTTable *table = m_db->openTable(session, string(string(m_db->getNtseDb()->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TABLE_NAME).c_str());
	AddIndexInfo add;
	IdxPreAlter idxPreAlter(m_db, session->getConnection(), m_parser);
	idxPreAlter.parAddCmd(add);
	IndexDef **indexDefs = (IndexDef **)session->getMemoryContext()->alloc(sizeof(IndexDef *) * add.idxs.size());
	for (uint i = 0; i < add.idxs.size(); i++)
	{
		ColumnDef **columns = new (session->getMemoryContext()->alloc(sizeof(ColumnDef *) * add.idxs[i].attribute.size())) ColumnDef *[add.idxs[i].attribute.size()];
		u32 *prefixLenAttr = new (session->getMemoryContext()->alloc(sizeof(u32) * add.idxs[i].attribute.size())) u32[add.idxs[i].attribute.size()];
		for (uint j = 0; j < add.idxs[i].attribute.size(); j++)
		{
			columns[j] = tableDef->getColumnDef(add.idxs[i].attribute[j].c_str());
			prefixLenAttr[j]  = add.idxs[i].prefixLenArr[j];
			if (columns[j] == NULL) {
				CPPUNIT_FAIL("Invalid Column Name.");
			}
		}
		indexDefs[i] = new IndexDef(add.idxs[i].idxName.c_str(), (u16)add.idxs[i].attribute.size(), columns, prefixLenAttr);
	}
	return indexDefs;
}

/**
 * ɾ����������ntse_command����ִ�д�������
 *
 * @throw <code>NtseException</code> ������ʧ��
 */
void IdxPreAlterTestCase::runDropIndex() throw (NtseException) {
	CPPUNIT_ASSERT(NULL != m_cmd);
	AutoPtr<Parser> parser(new Parser(m_cmd));
	const char *token = parser->nextToken();
	CPPUNIT_ASSERT(!System::stricmp(token, "drop"));
	m_parser = parser.detatch();
	Connection *conn = m_db->getNtseDb()->getConnection(false, __FUNCTION__);
	//����ɾ������
	IdxPreAlter idxPreAlter(m_db, conn, m_parser);
	idxPreAlter.deleteOnlineIndex();
	m_db->getNtseDb()->freeConnection(conn);
}

/** 
 * ��������һ����
 *
 * @see <code>TblInterface::verify()</code> method.
 */
void IdxPreAlterTestCase::checkTable() {
	Database *ntsedb = m_db->getNtseDb();
	TblInterface tblInterface(ntsedb, string(string(ntsedb->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TABLE_NAME).c_str());
	tblInterface.open();
	tblInterface.verify();
	tblInterface.close();
}

/** 
 * ������һ����
 *
 * @see <code> TableDef::checkTableDef()</code> method.
 */
void IdxPreAlterTestCase::checkTableDef() {
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false, __FUNCTION__);
	Session *session = ntsedb->getSessionManager()->allocSession(__FUNCTION__, conn, -1);
	TNTTable *table = NULL;
	TNTTransaction trx;
	try {
		session->setTrans(&trx);
		table = m_db->openTable(session, string(string(ntsedb->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TABLE_NAME).c_str());
		table->lockMeta(session, IL_S, -1, __FUNCTION__, __LINE__);
		table->getNtseTable()->getTableDef()->check();
		table->unlockMeta(session, IL_S);
		session->setTrans(NULL);
	} catch (NtseException &e) {
		session->setTrans(NULL);
		CPPUNIT_FAIL(e.getMessage());
	}
	m_db->closeTable(session,table);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}

/** 
 * ���δ��ɵ�������������
 *
 * @param expect ��ʾ������ִ�н��ֵ
 */
void IdxPreAlterTestCase::checkOnlineIndexCnt(int expect) {
	CPPUNIT_ASSERT(NULL != m_db);
	Database *ntsedb = m_db->getNtseDb();
	Connection *conn = ntsedb->getConnection(false);
	Session *session = ntsedb->getSessionManager()->allocSession("IdxPreAlterTestCase::checkAddIndexCnt", conn);
	TNTTransaction trx;
	session->setTrans(&trx);
	TNTTable *table = m_db->openTable(session, string(string(ntsedb->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TABLE_NAME).c_str());
	CPPUNIT_ASSERT(NULL != table);
	session->setTrans(NULL);
	
	table->lockMeta(session, IL_S, ntsedb->getConfig()->m_tlTimeout, __FILE__, __LINE__);
	TableDef *tableDef = table->getNtseTable()->getTableDef();

	int cnt = 0;
	for (int i = 0; i < tableDef->m_numIndice; i++) {
		const IndexDef *indexDef = tableDef->getIndexDef(i);
		if(indexDef->m_online) cnt++;
	}
	CPPUNIT_ASSERT(expect == cnt);

	table->unlockMeta(session, IL_S);	
	m_db->closeTable(session, table);
	ntsedb->getSessionManager()->freeSession(session);
	ntsedb->freeConnection(conn);
}

/** 
 * ���Խ������������������
 */
void IdxPreAlterTestCase::testParAddCmd() {
	AddIndexInfo add;
	//�������������������TEST_CMD_ADD_A
	m_cmd = TEST_CMD_ADD_A.c_str();
	EXCPT_OPER(runParAddCmd(add));
	CPPUNIT_ASSERT(DB_NAME==add.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==add.tableName);
	CPPUNIT_ASSERT(IDX_NAME_PUBTIME==add.idxs[0].idxName);
	CPPUNIT_ASSERT(string(COL_NAME_PUB_TIME)==add.idxs[0].attribute[0]);
	//�������������������TEST_CMD_ADD_B
	m_cmd = TEST_CMD_ADD_B.c_str();
	add.idxs.clear();
	EXCPT_OPER(runParAddCmd(add));
	CPPUNIT_ASSERT(DB_NAME==add.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==add.tableName);
	CPPUNIT_ASSERT(IDX_NAME_TITLE_TAGS==add.idxs[0].idxName);
	CPPUNIT_ASSERT(string(COL_NAME_TITLE)==add.idxs[0].attribute[0]);
	CPPUNIT_ASSERT(string(COL_NAME_TAGS)==add.idxs[0].attribute[1]);
	//�������������������TEST_CMD_ADD_C
	m_cmd = TEST_CMD_ADD_C.c_str();
	add.idxs.clear();
	EXCPT_OPER(runParAddCmd(add));
	CPPUNIT_ASSERT(DB_NAME==add.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==add.tableName);
	CPPUNIT_ASSERT(IDX_NAME_USERID==add.idxs[0].idxName);
	CPPUNIT_ASSERT(string(COL_NAME_USER_ID)==add.idxs[0].attribute[0]);
	CPPUNIT_ASSERT(IDX_NAME_INVALID==add.idxs[1].idxName);
	CPPUNIT_ASSERT(string(COL_NAME_USER_ID)==add.idxs[1].attribute[0]);
	CPPUNIT_ASSERT(string(COL_NAME_INVALID)==add.idxs[1].attribute[1]);
	//�������������������TEST_CMD_ADD_D
	m_cmd = TEST_CMD_ADD_D.c_str();
	add.idxs.clear();
	EXCPT_OPER(runParAddCmd(add));
	CPPUNIT_ASSERT(DB_NAME==add.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==add.tableName);
	CPPUNIT_ASSERT(IDX_NAME_USERID==add.idxs[0].idxName);
	CPPUNIT_ASSERT(string(COL_NAME_USER_ID)==add.idxs[0].attribute[0]);
	CPPUNIT_ASSERT(IDX_NAME_ID_PUB==add.idxs[1].idxName);
	CPPUNIT_ASSERT(string(COL_NAME_USER_ID)==add.idxs[1].attribute[0]);
	CPPUNIT_ASSERT(string(COL_NAME_PUB_TIME)==add.idxs[1].attribute[1]);

	//�������������������TEST_CMD_ADD_E
	m_cmd = TEST_CMD_ADD_E.c_str();
	add.idxs.clear();
	EXCPT_OPER(runParAddCmd(add));
	CPPUNIT_ASSERT(DB_NAME==add.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==add.tableName);
	CPPUNIT_ASSERT(IDX_NAME_PREFIX_TITLE_TAGS==add.idxs[0].idxName);
	CPPUNIT_ASSERT(string(COL_NAME_TITLE)==add.idxs[0].attribute[0]);
	CPPUNIT_ASSERT(string(COL_NAME_TAGS)==add.idxs[0].attribute[1]);
	CPPUNIT_ASSERT(3 == add.idxs[0].prefixLenArr[0]);
	CPPUNIT_ASSERT(5 == add.idxs[0].prefixLenArr[1]);

	//�����Ƿ������������������INVALID_ADD_CMD_A��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_ADD_CMD_A;
	add.idxs.clear();
	try {
		runParAddCmd(add);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
	//�����Ƿ������������������INVALID_ADD_CMD_B��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_ADD_CMD_B;
	add.idxs.clear();
	try {
		runParAddCmd(add);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
	//�����Ƿ������������������INVALID_ADD_CMD_C��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_ADD_CMD_C;
	add.idxs.clear();
	try {
		runParAddCmd(add);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
	//�����Ƿ������������������INVALID_ADD_CMD_D��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_ADD_CMD_D;
	add.idxs.clear();
	try {
		runParAddCmd(add);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}

	//�����Ƿ������������������INVALID_ADD_CMD_E��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_ADD_CMD_E;
	add.idxs.clear();
	try {
		runParAddCmd(add);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}

	//�����Ƿ������������������INVALID_ADD_CMD_F��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_ADD_CMD_F;
	add.idxs.clear();
	try {
		runParAddCmd(add);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
}

/** 
 * ���Խ���ɾ��������������
 */
void IdxPreAlterTestCase::testParDropCmd() {
	DropIndexInfo drop;
	//����ɾ��������������TEST_CMD_DROP_A
	m_cmd = TEST_CMD_DROP_A.c_str();
	EXCPT_OPER(runParDropCmd(drop));
	CPPUNIT_ASSERT(DB_NAME==drop.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==drop.tableName);
	CPPUNIT_ASSERT(IDX_NAME_PUBTIME==drop.idxNames[0]);
	//����ɾ��������������TEST_CMD_DROP_B
	m_cmd = TEST_CMD_DROP_B.c_str();
	drop.idxNames.clear();
	EXCPT_OPER(runParDropCmd(drop));
	CPPUNIT_ASSERT(DB_NAME==drop.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==drop.tableName);
	CPPUNIT_ASSERT(IDX_NAME_TITLE_TAGS==drop.idxNames[0]);
	//����ɾ��������������TEST_CMD_DROP_C
	m_cmd = TEST_CMD_DROP_C.c_str();
	drop.idxNames.clear();
	EXCPT_OPER(runParDropCmd(drop));
	CPPUNIT_ASSERT(DB_NAME==drop.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==drop.tableName);
	CPPUNIT_ASSERT(IDX_NAME_USERID==drop.idxNames[0]);
	CPPUNIT_ASSERT(IDX_NAME_INVALID==drop.idxNames[1]);
	//����ɾ��������������TEST_CMD_DROP_D
	m_cmd = TEST_CMD_DROP_D.c_str();
	drop.idxNames.clear();
	EXCPT_OPER(runParDropCmd(drop));
	CPPUNIT_ASSERT(DB_NAME==drop.schemaName);
	CPPUNIT_ASSERT(TABLE_NAME==drop.tableName);
	CPPUNIT_ASSERT(IDX_NAME_USERID==drop.idxNames[0]);
	CPPUNIT_ASSERT(IDX_NAME_ID_PUB==drop.idxNames[1]);

	//�����Ƿ���ɾ��������������INVALID_DROP_CMD_A��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_DROP_CMD_A;
	drop.idxNames.clear();
	try {
		runParDropCmd(drop);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
	//�����Ƿ���ɾ��������������INVALID_DROP_CMD_B��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_DROP_CMD_B;
	drop.idxNames.clear();
	try {
		runParDropCmd(drop);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
	//�����Ƿ���ɾ��������������INVALID_DROP_CMD_C��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_DROP_CMD_C;
	drop.idxNames.clear();
	try {
		runParDropCmd(drop);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
	//�����Ƿ���ɾ��������������INVALID_DROP_CMD_D��Ԥ�ڻ��׳��쳣
	m_cmd = INVALID_DROP_CMD_D;
	drop.idxNames.clear();
	try {
		runParDropCmd(drop);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkTable();
		checkTableDef();
	}
}

/** 
 * ���������������
 */
void IdxPreAlterTestCase::testCreateOnlineIndex() {
	m_cmd = TEST_CMD_ADD_A.c_str();
	//ͨ��ntse_command�����������TEST_CMD_ADD_A����ɺ��������������Ϊ1
	EXCPT_OPER(runAddOnlineIndex());
	checkOnlineIndexCnt(1);
	checkTable();
	checkTableDef();

	//ͨ��ntse_command���������������TEST_CMD_ADD_A���������������Ѵ��ڣ����׳��쳣
	try {
		runAddOnlineIndex();
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkOnlineIndexCnt(1);
		checkTable();
		checkTableDef();
	}

	//ͨ��mysql���������������TEST_CMD_ADD_A����ɺ��������������Ϊ0
	EXCPT_OPER(runAddMysqlIndex(1));
	checkOnlineIndexCnt(0);
	checkTable();
	checkTableDef();

	//ͨ��ntse_command���������������TEST_CMD_ADD_A�����������������������Ȼ���׳��쳣
	try {
		runAddOnlineIndex();
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkOnlineIndexCnt(0);
		checkTable();
		checkTableDef();
	}

	m_cmd = TEST_CMD_ADD_B.c_str();
	//ͨ��ntse_command�����������TEST_CMD_ADD_B����ɺ��������������Ϊ1
	EXCPT_OPER(runAddOnlineIndex());
	checkOnlineIndexCnt(1);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_ADD_C.c_str();
	//ͨ��ntse_command�����������TEST_CMD_ADD_C�����ڰ����Ƿ�������Ԥ�ڻ��׳��쳣
	try {
		runAddOnlineIndex();
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkOnlineIndexCnt(1);
		checkTable();
		checkTableDef();
	}

	m_cmd = TEST_CMD_ADD_D.c_str();
	//ͨ��mysql���������TEST_CMD_ADD_D�����ڴ���δ��ɵ���������TEST_CMD_ADD_B��Ԥ�ڻ��׳��쳣
	try {
		runAddMysqlIndex(2);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkOnlineIndexCnt(1);
		checkTable();
		checkTableDef();
	}

	//ͨ��ntse_command�����������TEST_CMD_ADD_D����ɺ��������������Ϊ3
	EXCPT_OPER(runAddOnlineIndex());
	checkOnlineIndexCnt(3);
	checkTable();
	checkTableDef();

	//ͨ��mysql���������������TEST_CMD_ADD_D������˳�������׳��쳣
	try {
		runAddMysqlIndex(2);
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkOnlineIndexCnt(3);
		checkTable();
		checkTableDef();
	}

	m_cmd = TEST_CMD_ADD_B.c_str();
	//ͨ��mysql���������������TEST_CMD_ADD_B����ɺ��������������Ϊ2
	EXCPT_OPER(runAddMysqlIndex(1));
	checkOnlineIndexCnt(2);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_ADD_D.c_str();
	//ͨ��mysql���������������TEST_CMD_ADD_D����ɺ��������������Ϊ0
	EXCPT_OPER(runAddMysqlIndex(2));
	checkOnlineIndexCnt(0);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_ADD_E.c_str();
	//ͨ��ntse_command�����������TEST_CMD_ADD_E����ɺ��������������Ϊ1
	EXCPT_OPER(runAddOnlineIndex());
	checkOnlineIndexCnt(1);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_ADD_E.c_str();
	//ͨ��mysql���������������TEST_CMD_ADD_E����ɺ��������������Ϊ0
	EXCPT_OPER(runAddMysqlIndex(1));
	checkOnlineIndexCnt(0);
	checkTable();
	checkTableDef();

}

/** 
 * ����ɾ����������
 */
void IdxPreAlterTestCase::testDeleteOnlineIndex() {
	m_cmd = TEST_CMD_ADD_A.c_str();
	//ͨ��ntse_command�����������TEST_CMD_ADD_A����ɺ��������������Ϊ1
	EXCPT_OPER(runAddOnlineIndex());
	checkOnlineIndexCnt(1);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_DROP_A.c_str();
	//ͨ��ntse_commandɾ��������������TEST_CMD_DROP_A����ɺ��������������Ϊ0
	EXCPT_OPER(runDropIndex());
	checkOnlineIndexCnt(0);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_ADD_B.c_str();
	//ͨ��ntse_command�����������TEST_CMD_ADD_B����ɺ��������������Ϊ1
	EXCPT_OPER(runAddOnlineIndex());
	checkOnlineIndexCnt(1);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_ADD_D.c_str();
	//ͨ��ntse_command�����������TEST_CMD_ADD_D����ɺ��������������Ϊ3
	EXCPT_OPER(runAddOnlineIndex());
	checkOnlineIndexCnt(3);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_DROP_C.c_str();
	//ͨ��ntse_commandɾ��������������TEST_CMD_DROP_C�����ڰ����Ƿ������������׳��쳣
	try {
		runDropIndex();
		//���Ԥ�ڵ��쳣û�з���
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		cout << "NTSE_THROW: " << e.getMessage() << endl;
		checkOnlineIndexCnt(3);
		checkTable();
		checkTableDef();
	}

	m_cmd = TEST_CMD_DROP_D.c_str();
	//ͨ��ntse_commandɾ��������������TEST_CMD_DROP_D������ɾ��˳��һ�£����ᷢ���쳣����ɺ��������������Ϊ1
	EXCPT_OPER(runDropIndex());
	checkOnlineIndexCnt(1);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_DROP_B.c_str();
	//ͨ��ntse_commandɾ��������������TEST_CMD_DROP_B����ɺ��������������Ϊ0
	EXCPT_OPER(runDropIndex());
	checkOnlineIndexCnt(0);
	checkTable();
	checkTableDef();
}
