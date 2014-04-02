/**
 * ������޸���ز�������ʵ��
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#include <iostream>
#include <strstream>
#include "api/TestTableArgAlter.h"
#include "api/TblArgAlter.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "Test.h"
#include "util/Thread.h"

using namespace std;
using namespace ntse;

const char * TableAlterArgTestCase::DB_NAME = "testdb";
const char * TableAlterArgTestCase::TABLE_NAME = "blog";
const char * TableAlterArgTestCase::TABLE_PATH = "./testdb/blog";
const char * TableAlterArgTestCase::INVALID_COMMAND_STRING = "Alter table set I am an invalid command, bla, bla...";

const char * TableAlterArgTestCase::COL_NAME_ID = "ID";		
const char * TableAlterArgTestCase::COL_NAME_USER_ID= "UserID";		
const char * TableAlterArgTestCase::COL_NAME_PUBLISH_TIME= "PublishTime";	
const char * TableAlterArgTestCase::COL_NAME_TITLE= "Title";			
const char * TableAlterArgTestCase::COL_NAME_TAGS= "Tags";			
const char * TableAlterArgTestCase::COL_NAME_ABSTRACT= "Abstract";
const char * TableAlterArgTestCase::COL_NAME_CONTENT= "Content";

const char *TableAlterArgTestCase::ALTER_TABLE_CMD_COMM = "alter table set ";

string TableAlterArgTestCase::TEST_CMD_MMS_ENABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_USEMMS + " = true";

string TableAlterArgTestCase::TEST_CMD_MMS_DISABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_USEMMS + " = false";

string TableAlterArgTestCase::TEST_CMD_CACHE_UPDATE_ENABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_CACHE_UPDATE + " = true";

string TableAlterArgTestCase::TEST_CMD_CACHE_UPDATE_DISABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_CACHE_UPDATE + " = false";

string TableAlterArgTestCase::TEST_CMD_CACHE_UPDATE_TIME = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_UPDATE_CACHE_TIME + " = 120";
string TableAlterArgTestCase::TEST_CMD_CACHE_UPDATE_TIME2 = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_UPDATE_CACHE_TIME + " = 121";

string TableAlterArgTestCase::TEST_CMD_CACHED_COLUMNS_ENABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_CACHED_COLUMNS + " =  ENABLE " + COL_NAME_TITLE + ", " + COL_NAME_TAGS;
string TableAlterArgTestCase::TEST_CMD_CACHED_COLUMNS_DISABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_CACHED_COLUMNS + " =  DISABLE " + COL_NAME_TITLE + "," + COL_NAME_TAGS;
string TableAlterArgTestCase::TEST_CMD_COMPRESS_LOBS_ENABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_COMPRESS_LOBS +" = true";
string TableAlterArgTestCase::TEST_CMD_COMPRESS_LOBS_DISABLE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_COMPRESS_LOBS +" = false";	
string TableAlterArgTestCase::TEST_CMD_SET_HEAP_PCT_FREE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_HEAP_PCT_FREE +" = 30";
string TableAlterArgTestCase::TEST_CMD_SET_HEAP_PCT_FREE2 = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_HEAP_PCT_FREE +" = 29";	
string TableAlterArgTestCase::TEST_CMD_SET_SPLIT_FACTORS = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_SPLIT_FACTORS +" = IDX_BLOG_PUTTIME 6";
string TableAlterArgTestCase::TEST_CMD_SET_SPLIT_FACTOR_AUTO = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_SPLIT_FACTORS +" = IDX_BLOG_PUTTIME -1";
string TableAlterArgTestCase::TEST_CMD_SET_SPLIT_FACTORS2 = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_SPLIT_FACTORS +" = IDX_BLOG_PUTTIME 7";
string TableAlterArgTestCase::TEST_CMD_INCREASE_SIZE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_INCR_SIZE +" = 64";
string TableAlterArgTestCase::TEST_CMD_INCREASE_SIZE2 = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "."+ TableArgAlterHelper::CMD_INCR_SIZE +" = 128";
string TableAlterArgTestCase::TEST_CMD_COMPRESS_ROWS = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_COMPRESS_ROWS + " = true";
string TableAlterArgTestCase::TEST_CMD_COMPRESS_ROWS2 = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_COMPRESS_ROWS + " = false";
string TableAlterArgTestCase::TEST_CMD_HEAP_FIXLEN = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_FIX_LEN + " = true";
string TableAlterArgTestCase::TEST_CMD_HEAP_FIXLEN2 = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_FIX_LEN + " = false";
string TableAlterArgTestCase::TEST_CMD_COL_GROUP_DEF = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_SET_COLUMN_GROUPS + " = ";
string TableAlterArgTestCase::TEST_CMD_COMPRESS_DICT_SIZE = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_COMPRESS_DICT_SIZE + " = ";
string TableAlterArgTestCase::TEST_CMD_COMPRESS_DICT_MIN_LEN = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_COMPRESS_DICT_MIN_LEN + " = ";
string TableAlterArgTestCase::TEST_CMD_COMPRESS_DICT_MAX_LEN = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_COMPRESS_DICT_MAX_LEN + " = ";
string TableAlterArgTestCase::TEST_CMD_COMPRESS_THRESHOLD = string(ALTER_TABLE_CMD_COMM) + DB_NAME + "." + TABLE_NAME + "." + TableArgAlterHelper::CMD_COMPRESS_THRESHOLD + " = ";

Config	TableAlterArgTestCase::m_config;	/* ���ݿ����� */
/** 
 * ������������
 */
const char* TableAlterArgTestCase::getName() {
	return "Alter Table Parameters Test.";
}

/**
 * ��������������
 */
const char* TableAlterArgTestCase::getDescription() {
	return "Test alter table parameter functions.";
}

/** 
 * �������͡�
 * @return false, С�͵�Ԫ���ԡ�
 */
bool TableAlterArgTestCase::isBig() {
	return false;
}

/** 
 * Set up context before start a test.
 * @see <code>TestFixture::setUp()</code>
 */
void TableAlterArgTestCase::setUp() {
	//��ʼ�����ݿ⡣
	m_db = initDb();

	//����
	EXCPT_OPER(createBlog(m_db, true));

	prepareDataMisc();
}

/** 
 * ��ʼ��DB������DB��
 *
 * @return ���ݿ�ʵ��
 */
Database * TableAlterArgTestCase::initDb() {
	Database *db = NULL;
	Database::drop(".");
	File dir(DB_NAME);
	dir.rmdir(true);
	dir.mkdir();
	EXCPT_OPER(db = Database::open(&m_config, true));
	Table::drop(db, string(string(DB_NAME) + "/" + TABLE_NAME).c_str());
	return db;
}

/** 
 * �������ݿ⡣
 */
void TableAlterArgTestCase::destroyDb(Database *db) {
	if (db) {
		EXCPT_OPER(Table::drop(db, TABLE_NAME));
		db->close();
		delete db;
	}
	Database::drop(".");
	File dir(DB_NAME);
	dir.rmdir(true);
}

/** 
 * Clean up after the test run.
 * @see <code>TestFixture::tearDown()</code>
 */
void TableAlterArgTestCase::tearDown() {
	destroyDb(m_db);
	m_db = NULL;
}

/**
 * ����Blog����
 *
 * @param useMms �Ƿ�ʹ��MMS
 * @return Blog����
 */
TableDef* TableAlterArgTestCase::getBlogDef(bool useMms) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, DB_NAME, TABLE_NAME);

	builder->addColumn(COL_NAME_ID, CT_BIGINT, false)->addColumn(COL_NAME_USER_ID, CT_BIGINT, false);
	builder->addColumn(COL_NAME_PUBLISH_TIME, CT_BIGINT);
	builder->addColumnS(COL_NAME_TITLE, CT_VARCHAR, 411)->addColumnS(COL_NAME_TAGS, CT_VARCHAR, 200);
	builder->addColumn(COL_NAME_ABSTRACT, CT_SMALLLOB)->addColumn(COL_NAME_CONTENT, CT_MEDIUMLOB);
	builder->addColumn("c1", CT_INT, true);
	builder->addColumn("c2", CT_INT, true);
	builder->addColumn("c3", CT_INT, true);
	builder->addColumn("c4", CT_INT, true);
	builder->addColumn("c5", CT_INT, true);
	builder->addColumn("c6", CT_INT, true);
	builder->addColumn("c7", CT_INT, true);
	builder->addColumn("c8", CT_INT, true);
	builder->addColumn("c9", CT_INT, true);
	builder->addColumn("c10", CT_INT, true);
	builder->addColumn("c11", CT_INT, true);
	builder->addColumn("c12", CT_INT, true);
	builder->addColumn("c13", CT_INT, true);
	builder->addColumn("c14", CT_INT, true);
	builder->addColumn("c15", CT_INT, true);
	builder->addColumn("c16", CT_INT, true);
	builder->addColumn("c17", CT_INT, true);
	builder->addColumn("c18", CT_INT, true);
	builder->addColumn("c19", CT_INT, true);
	builder->addColumn("c20", CT_INT, true);
	builder->addColumn("c21", CT_INT, true);
	builder->addColumn("c22", CT_INT, true);
	builder->addColumn("c23", CT_INT, true);
	builder->addColumn("c24", CT_INT, true);
	builder->addColumn("c25", CT_INT, true);
	builder->addColumn("c26", CT_INT, true);
	builder->addColumn("c27", CT_INT, true);
	builder->addColumn("c28", CT_INT, true);
	builder->addColumn("c29", CT_INT, true);
	builder->addColumn("c30", CT_INT, true);
	builder->addColumn("c31", CT_INT, true);
	builder->addColumn("c32", CT_INT, true);
	builder->addColumn("c33", CT_INT, true);
	builder->addIndex("PRIMARY", true, true, false, COL_NAME_ID, 0, NULL);
	builder->addIndex("IDX_BLOG_PUTTIME", false, false, false, COL_NAME_PUBLISH_TIME, 0, NULL);
	builder->addIndex("IDX_BLOG_PUBTIME", false, false, false, COL_NAME_USER_ID, 0, COL_NAME_PUBLISH_TIME, 0, NULL);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;

	delete builder;
	builder = NULL;
	return tableDef;
}

/** ����BLOG��Ϊ���������ı䳤����������
CREATE TABLE blog (
  ID bigint NOT NULL,
  UserID bigint NOT NULL,
  PublishTime bigint,
  Title varchar(511),
  Tags varchar(200),
  Abstract text,
  Content mediumtext,
  PRIMARY KEY (ID),
  KEY IDX_BLOG_PUTTIME(PublishTime),
  KEY IDX_BLOG_PUBTIME(UserID, PublishTime)
)ENGINE=NTSE;
 @param db		���ݿ�
 @param useMms	�Ƿ�ʹ��MMS
 @note ���ǵ�TestTable��û��д�����ַ�������û��ֱ������Table�Ĳ�������, ͬʱ�����c1 ��c33������������mms�����й����case��
 */
void TableAlterArgTestCase::createBlog(Database *db, bool useMms) {
	assert(NULL != db);

	TableDef *tableDef = getBlogDef(useMms);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TableAlterTestCase::createBlog", conn);
	db->createTable(session, string(string(db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str(), tableDef);
	delete tableDef;
	tableDef = NULL;

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);	
}

/** 
 * ����BLOG���¼��
 *
 * @param db			���ݿ�
 * @param numRows		��������������
 * @param startFromOne	id�Ƿ��1��ʼ��
 */
void TableAlterArgTestCase::insertBlog(Database *db, uint numRows, bool startFromOne /* = false */) {
	assert(NULL != db);
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession(__FUNCTION__, conn);	

	Table *table = db->openTable(session, string(string(db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());

	Record **rows = populateBlog(db, table, numRows, false, true, startFromOne);
	for (uint i = 0; i < numRows; i++) {
		freeMysqlRecord(table->getTableDef(), rows[i]);
	}
	delete[] rows;
	
	db->closeTable(session, table);
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);	
}
/**
 * ����Blog��������ݣ����ɵ����ݵĹ���Ϊ����n(��0��ʼ)�еĸ����԰����¹�������
 *
 * - ID:  System::currentTimeMillis() + 1 �� start from 1++
 * - UserID: n / 5 + 1
 * - PublishTime: ��ǰʱ�������
 * - Title: ����Ϊ100������ַ���
 * - Tags: ����Ϊ100������ַ���
 * - Abstract: ����Ϊ100������ַ�������lobNotNullΪfalse��������ΪNULL
 * - Content: ������Ϊ���ʹ����ż����ΪС�ʹ����С�ʹ�����СΪSMALL_LOB_SIZE��
 *   ���ʹ�����СΪLARGE_LOB_SIZE_MIN��LARGE_LOB_SIZE_MAX֮�䣬��lobNotNullΪfalse��ż����ΪNULL
 *
 * @param db			���ݿ����
 * @param table			BlogCount��
 * @param numRows		Ҫ���������
 * @param doubleInsert	ÿ����¼�Ƿ��ظ���������
 * @param startFromOne	ID�Ƿ��1��ʼ
 * @return				����¼���ݣ�ΪMySQL��ʽ
 */
Record** TableAlterArgTestCase::populateBlog(Database *db, Table *table, uint numRows, bool doubleInsert, bool lobNotNull, bool startFromOne /*= false*/) {
	Record **rows = new Record *[numRows];
	const uint SMALL_LOB_SIZE = Limits::PAGE_SIZE / 4;
	const uint LARGE_LOB_SIZE_MIN = Limits::PAGE_SIZE ;
	const uint LARGE_LOB_SIZE_MAX = Limits::PAGE_SIZE * 4 ;
	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
	for (uint n = 0; n < numRows; n++) {
		
		u64 id = System::currentTimeMillis() + 1;
		if(startFromOne) {
			static int newID = 0;
			newID++;
			id = newID;
		}
		Thread::msleep(1);
		u64 userId = n / 5 + 1;
		u64 publishTime = System::currentTimeMillis()+1;
		char *title = randomStr(100);
		char *tags = randomStr(100);
		SubRecord *subRec = srb.createSubRecordByName("ID UserID PublishTime Title Tags", &id, &userId, &publishTime, title, tags);

		char *abs;
		if (!lobNotNull && (n % 2 == 0)) {
			abs = NULL;
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 0);
			RecordOper::setNullR(table->getTableDef(), subRec, 5, true);
		} else {
			abs = randomStr(100);
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 100);
			RecordOper::setNullR(table->getTableDef(), subRec, 5, false);
		}
		RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[5], (byte *)abs);


		size_t contentSize;
		if (n % 2) {
			if (lobNotNull)
				contentSize = SMALL_LOB_SIZE;
			else
				contentSize = 0;
		} else {				
			contentSize = (System::random()%(LARGE_LOB_SIZE_MAX-LARGE_LOB_SIZE_MIN)) + LARGE_LOB_SIZE_MIN;
		}
		if (contentSize) {
			char *content = randomStr(contentSize);
			RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], (byte *)content);
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], (uint)contentSize);
			RecordOper::setNullR(table->getTableDef(), subRec, 6, false);
		} else {
			RecordOper::setNullR(table->getTableDef(), subRec, 6, true);
			RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], NULL);
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], 0);
		}

		Record *rec = new Record(RID(0, 0), REC_MYSQL, subRec->m_data, table->getTableDef()->m_maxRecSize);

		Connection *conn = db->getConnection(false);
		Session *session = db->getSessionManager()->allocSession("TableTestCase::populateBlog", conn);
		uint dupIndex;
		rec->m_format = REC_MYSQL;

		CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, true, &dupIndex)) != INVALID_ROW_ID);
		if (doubleInsert)
			CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, true, &dupIndex)) == INVALID_ROW_ID);
		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);

		rows[n] = rec;
		delete []title;
		delete []tags;
		delete [] subRec->m_columns;
		delete subRec;
	}
	return rows;
}

/**
 * ������޸�����ִ�д������̡�
 *
 * @throw <code>NtseException</code> ���޸�ʧ�ܡ�
 */
void TableAlterArgTestCase::runCommand() throw (NtseException) {
	CPPUNIT_ASSERT(NULL != m_cmd);
	AutoPtr<Parser> parser(new Parser(m_cmd));
	const char *token = parser->nextToken();

	CPPUNIT_ASSERT(!System::stricmp(token, "alter"));
	m_parser = parser.detatch();
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
		//��̬�޸ı����
	{
		TableArgAlterHelper tblAltHelp(m_db, conn, m_parser, m_db->getConfig()->m_tlTimeout);
		tblAltHelp.alterTableArgument();
	}
	
	m_db->freeConnection(conn);
}

/** 
 * ������ֵ��
 *
 * @param cmdType	��ʾִ�����������
 * @param expect	��ʾ������ִ�н��ֵ��
 */
void TableAlterArgTestCase::checkValue(TableArgAlterCmdType cmdType, bool expect) {
	CPPUNIT_ASSERT(NULL != m_db);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableAlterTestCase::checkValue", conn);
	Table *table = m_db->openTable(session,  string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	CPPUNIT_ASSERT(NULL != table);
	table->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout, __FILE__, __LINE__);
	TableDef *tableDef = table->getTableDef();

	switch (cmdType) {
		case USEMMS:
			CPPUNIT_ASSERT(expect == tableDef->m_useMms);
			break;
		case CACHE_UPDATE:
			CPPUNIT_ASSERT(expect == tableDef->m_cacheUpdate);
			break;
		case COMPRESS_LOBS:
			CPPUNIT_ASSERT(expect == tableDef->m_compressLobs);
			break;
		case COMPRESS_ROWS:
			CPPUNIT_ASSERT(expect == tableDef->m_isCompressedTbl);
			CPPUNIT_ASSERT(!expect || tableDef->m_rowCompressCfg != NULL);
			break;
		default:
			CPPUNIT_FAIL("Unknown Command Type.");
			break;
	}
	table->unlockMeta(session, IL_S);	
	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
 * ������ֵ��
 *
 * @param cmdType	��ʾִ�����������
 * @param expect	��ʾ������ִ�н��ֵ��
 */
void TableAlterArgTestCase::checkValue(TableArgAlterCmdType cmdType, int expect) {
	CPPUNIT_ASSERT(NULL != m_db);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableAlterTestCase::checkValue", conn);
	Table *table = m_db->openTable(session,  string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());

	CPPUNIT_ASSERT(NULL != table);
	table->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout, __FILE__, __LINE__);
	TableDef *tableDef = table->getTableDef();
	switch (cmdType) {
		case UPDATE_CACHE_TIME:
			CPPUNIT_ASSERT(expect == tableDef->m_updateCacheTime);
			break;
		case HEAP_PCT_FREE:
			CPPUNIT_ASSERT(expect == tableDef->m_pctFree);
			break;
		case INCR_SIZE:
			CPPUNIT_ASSERT(expect == tableDef->m_incrSize);
			break;
		case COMPRESS_DICT_SIZE:
			CPPUNIT_ASSERT(tableDef->m_rowCompressCfg != NULL);
			CPPUNIT_ASSERT(expect == tableDef->m_rowCompressCfg->dicSize());
			break;
		case COMPRESS_DICT_MIN_LEN:
			CPPUNIT_ASSERT(tableDef->m_rowCompressCfg != NULL);
			CPPUNIT_ASSERT(expect == tableDef->m_rowCompressCfg->dicItemMinLen());
			break;
		case COMPRESS_DICT_MAX_LEN:
			CPPUNIT_ASSERT(tableDef->m_rowCompressCfg != NULL);
			CPPUNIT_ASSERT(expect == tableDef->m_rowCompressCfg->dicItemMaxLen());
			break;
		case COMPRESS_THRESHOLD:
			CPPUNIT_ASSERT(tableDef->m_rowCompressCfg != NULL);
			CPPUNIT_ASSERT(expect == tableDef->m_rowCompressCfg->compressThreshold());
			break;
		default:
			CPPUNIT_FAIL("Unknown Command Type.");
			break;
	}
	table->unlockMeta(session, IL_S);

	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
 * ������ֵ��
 *
 * @param cmdType	��ʾִ�����������
 * @param name		��ʾ����������������cmdType��CACHED_COLUMNSʱ����ʾ��������cmdType��SPLIT_FACTORSʱ����ʾ��������
 * @param expect	��ʾ������ִ�н��ֵ��
 */
void TableAlterArgTestCase::checkValue(TableArgAlterCmdType cmdType, const char *name, int expect) {
	CPPUNIT_ASSERT(NULL != m_db);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableAlterTestCase::checkValue", conn);
	Table *table = m_db->openTable(session,  string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());

	CPPUNIT_ASSERT(NULL != table);
	table->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout, __FILE__, __LINE__);
	TableDef *tableDef = table->getTableDef();
	int index = -1;
	IndexDef *indexDef = NULL;
	switch (cmdType) {
		case CACHED_COLUMNS:
			index = tableDef->getColumnNo(name);
			if (index < 0) {
				CPPUNIT_FAIL("Can find column name");
			}
			CPPUNIT_ASSERT(tableDef->m_columns[index]->m_cacheUpdate == (expect != 0));
			break;
		case SPLIT_FACTORS:
			indexDef = tableDef->getIndexDef(name);
			for (index = 0; index < tableDef->m_numIndice; index++) {
				if (tableDef->m_indice[index] == indexDef) {
					break;
				}
			}
			
			if (index == tableDef->m_numIndice) {
				CPPUNIT_FAIL("Can find index name");
			}
			CPPUNIT_ASSERT(tableDef->m_indice[index]->m_splitFactor == expect);
			break;
		default:
			CPPUNIT_FAIL("Unknown Command Type.");
			break;
	}
	table->unlockMeta(session, IL_S);

	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
 * ׼���������ݡ�
 */
void TableAlterArgTestCase::prepareDataMisc(uint insertRowsEachTime /*= 10*/) {
	TblInterface tblInterface(m_db, string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	//1. ��������
	insertBlog(m_db, insertRowsEachTime, true);
	//2. ������ѯ
	tblInterface.open();
	TableDef *tableDef = getBlogDef(true);
	u64 low = 2;
	u64 high = System::currentTimeMillis() + 1000000;
	IdxRange range(tableDef, 1, IdxKey(tableDef, 1, 1, low), IdxKey(tableDef, 1, 1, high), true, true);
	
	ResultSet *rs = tblInterface.selectRows(range, COL_NAME_PUBLISH_TIME);
	
	delete tableDef;
	delete rs;
	
	tblInterface.close();
	tblInterface.freeConnection();
	//3. ��������
	this->insertBlog(m_db, insertRowsEachTime, true);

	//4. ��������
	TblInterface tblInterface2(m_db, string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	tblInterface2.open();
	u64 updateRows = tblInterface2.updateRows(range, COL_NAME_PUBLISH_TIME, System::currentTimeMillis());
	this->insertBlog(m_db, insertRowsEachTime, true);
	tblInterface2.close();
	tblInterface2.freeConnection();
}

/** 
 * ��������һ���ԡ�
 *
 * @see <code>TblInterface::verify()</code> method.
 */
void TableAlterArgTestCase::checkTable() {
	TblInterface tblInterface(m_db, string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	tblInterface.open();
	tblInterface.verify();
	tblInterface.close();
}

/** 
 * ������һ���ԡ�
 *
 * @see <code> TableDef::checkTableDef()</code> method.
 */
void TableAlterArgTestCase::checkTableDef() {
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn, -1);
	Table *table = NULL;
	try {
		table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
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

/** 
* ���������鶨���Ƿ���ȷ��
* @param hasColGrp �Ƿ�����������
*/
void TableAlterArgTestCase::checkTableColoumnGroups(bool hasColGrp) {
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn, -1);
	Table *table = NULL;
	try {
		table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
		table->lockMeta(session, IL_S, -1, __FUNCTION__, __LINE__);
		TableDef *tableDef = table->getTableDef();
		if (hasColGrp) {
			CPPUNIT_ASSERT(tableDef->m_numColGrps > 0);
			CPPUNIT_ASSERT(tableDef->m_colGrps != NULL);
			for (u16 i = 0; i < tableDef->m_numColGrps; i++) {
				CPPUNIT_ASSERT(tableDef->m_colGrps[i]);
				for (uint j = 0; j < tableDef->m_colGrps[i]->m_numCols; j++) {
					CPPUNIT_ASSERT(tableDef->m_colGrps[i]->m_colGrpNo == i);
					u16 colNo = tableDef->m_colGrps[i]->m_colNos[j];
					CPPUNIT_ASSERT(tableDef->m_columns[colNo]->m_colGrpNo == tableDef->m_colGrps[i]->m_colGrpNo);
				}
			}
		} else {
			CPPUNIT_ASSERT(tableDef->m_numColGrps == 0);
			CPPUNIT_ASSERT(tableDef->m_colGrps == NULL);
		}
		table->unlockMeta(session, IL_S);
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());
	}
	m_db->closeTable(session,table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ��ñ������������
 */
u8 TableAlterArgTestCase::getNumColumnGroup() {
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn, -1);
	Table *table = NULL;
	u8 numColGrp = 0;
	try {
		table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
		table->lockMeta(session, IL_S, -1, __FUNCTION__, __LINE__);
		TableDef *tableDef = table->getTableDef();
		numColGrp = tableDef->m_numColGrps;
		table->unlockMeta(session, IL_S);
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());
	}
	m_db->closeTable(session,table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	return numColGrp;
}

/** 
 * �����޸�useMMS��
 *
 * ���壺step 1: �������ݣ�step 2:��������ѯ���� step 3: �������� 4���޸ı���� 5���������һ���ԡ�
 * @note ����������޸Ĳ��������������޸�use mms���ơ�
 */
void TableAlterArgTestCase::testAlterUseMms() {
	m_cmd = TEST_CMD_MMS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(USEMMS, false);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_MMS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(USEMMS, true);
	checkTable();
	checkTableDef();
}
/** 
 * �����ڲ���ȡ������޸���־��
 *
 * @param startLsn ɨ����ʼLSN.
 * @return	�������һ��������޸���־��
 * @note	���ñ����������ޱ�����޸���־����CPPUNIT_ASSERT fails��
 */
const LogEntry * TableAlterArgTestCase::checkAndFetchLastTblArgAlterLog(LsnType startLsn) {

	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(startLsn, Txnlog::MAX_LSN);
	
	int count = 0;
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	assert(NULL != conn);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);
	const LogEntry *log = NULL;
	const LogEntry *targetLog = NULL;

	LogEntry *retTargetLog = new LogEntry;
	byte tmpData[Limits::PAGE_SIZE];
	while (m_db->getTxnlog()->getNext(logHdl)) {
		log = logHdl->logEntry();
		CPPUNIT_ASSERT(NULL != log);

		if(LOG_ALTER_TABLE_ARG == log->m_logType) {
			targetLog = log;
			count++;

			memcpy((void *)retTargetLog, (void *)targetLog, sizeof(LogEntry));
			memcpy((void *)tmpData, (void *)targetLog->m_data, targetLog->m_size);			
		}
	}

	CPPUNIT_ASSERT(count > 0);
	byte *logData = new byte[targetLog->m_size];
	memcpy((void *)logData, (void *) tmpData, targetLog->m_size);
	retTargetLog->m_data = logData;	
	
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_db->getTxnlog()->endScan(logHdl);	
	
	return retTargetLog;
}

/** 
 * ����ִ�б��޸Ĳ�����־��
 *
 * @param log ��������־
 */
void TableAlterArgTestCase::redoAlterTableArg(const LogEntry *log) {
	CPPUNIT_ASSERT(NULL != log);

	CPPUNIT_ASSERT(NULL != m_db);
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	CPPUNIT_ASSERT(NULL != conn);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);

	Table *table = m_db->openTable(session, TABLE_PATH);
	try {
		table->redoAlterTableArg(session, log);
	} catch (NtseException &) {
		CPPUNIT_FAIL("����ִ�б��޸Ĳ�����־ʧ��");
	}

	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * ����redo�޸�useMMS��
 *
 * @note ����״̬һ�ºͷ�һ���µ�redo
 */
void TableAlterArgTestCase::testRedoAlterUseMms() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1)do mms enable & then disable
	m_cmd = TEST_CMD_MMS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	m_cmd = TEST_CMD_MMS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(USEMMS, false);
	checkTable();
	checkTableDef();

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(USEMMS, false);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do mms enable
	m_cmd = TEST_CMD_MMS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(USEMMS, true);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(USEMMS, false);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}

/** 
 * �����޸�CACHE_UPDATE
 */
void TableAlterArgTestCase::testAlterCacheUpdate() {
	//�ر�MMS
	m_cmd = (char *) TEST_CMD_MMS_DISABLE.c_str();
	runCommand();

	//����cache update enable
	m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	try {
		runCommand();
	} catch (NtseException e) {
		//expected.
	}

	// ȷ��MMS������
	m_cmd = TEST_CMD_MMS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(USEMMS, true);
	checkTable();
	checkTableDef();

	//����cache update disable
	m_cmd = TEST_CMD_CACHE_UPDATE_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(CACHE_UPDATE,false);
	checkTable();
	checkTableDef();
	
	//����cache update enable
	m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(CACHE_UPDATE,true);	
	checkTable();
	checkTableDef();

	//����ĳЩ��cache update
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	EXCPT_OPER(runCommand());

	//����cache update disable, ʹ��cache update��Ҳ��disable
	m_cmd = TEST_CMD_CACHE_UPDATE_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(CACHE_UPDATE,false);
	checkTable();
	checkTableDef();
}
/** 
 * ������������������á�
 */
void TableAlterArgTestCase::testRedoAlterCacheUpdate() {
	LsnKeeper lsnKeeper(m_db);

	//A: ״̬һ���µ�redo
	// (1)mms enable, cache update disable and then enable
	m_cmd = TEST_CMD_MMS_ENABLE.c_str();
	EXCPT_OPER(runCommand());

	m_cmd = TEST_CMD_CACHE_UPDATE_DISABLE.c_str();
	EXCPT_OPER(runCommand());	
	checkValue(CACHE_UPDATE, false);

	m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	EXCPT_OPER(runCommand());
	checkValue(CACHE_UPDATE, true);

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(CACHE_UPDATE, true);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do cache update disable
	m_cmd = TEST_CMD_CACHE_UPDATE_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(CACHE_UPDATE, false);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(CACHE_UPDATE, true);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}

/** 
 * �����޸Ļ������ʱ�����ڡ�
 */
void TableAlterArgTestCase::testAlterCacheUpdateTime() {
	// disable cache update
	m_cmd = TEST_CMD_CACHE_UPDATE_DISABLE.c_str();
	runCommand();
	
	//�޸�cache updateʱ��
	m_cmd = TEST_CMD_CACHE_UPDATE_TIME.c_str();
	try {
		runCommand();
	} catch (NtseException &) {
		//expected.
	}

	// enable cache update
	m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	runCommand();

	//�޸�cache updateʱ��
	m_cmd = TEST_CMD_CACHE_UPDATE_TIME.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	const int magic_num = 120;
	checkValue(UPDATE_CACHE_TIME, magic_num);	
	checkTable();
	checkTableDef();

	//�ر�MMS
	m_cmd = (char *) TEST_CMD_MMS_DISABLE.c_str();
	runCommand();

	//����cache update ʱ��
	m_cmd = TEST_CMD_CACHE_UPDATE_TIME.c_str();
	try {
		runCommand();
	} catch (NtseException &) {
		//expected.
	}
}

/** 
 * �������û���������ڡ�
 */
void TableAlterArgTestCase::testRedoAlterCacheUpdateTime() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1)mms enable, set cache update interval
	m_cmd = (char *) TEST_CMD_MMS_ENABLE.c_str();
	EXCPT_OPER(runCommand());
	m_cmd = (char *)TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	EXCPT_OPER(runCommand());

	//�޸�cache updateʱ��
	m_cmd = TEST_CMD_CACHE_UPDATE_TIME.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	const int magic_num = 120;
	checkValue(UPDATE_CACHE_TIME, magic_num);	
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_CACHE_UPDATE_TIME2.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	const int magic_num2 = 121;
	checkValue(UPDATE_CACHE_TIME, magic_num2);	
	checkTable();
	checkTableDef();

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(UPDATE_CACHE_TIME, magic_num2);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do set cache update time
	m_cmd = TEST_CMD_CACHE_UPDATE_TIME.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(UPDATE_CACHE_TIME, magic_num);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(UPDATE_CACHE_TIME, magic_num2);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}
/** 
 * �����޸Ļ����С�
 */
void TableAlterArgTestCase::testAlterCachedColummns() {
	//����MMS
	m_cmd = (char *) TEST_CMD_MMS_ENABLE.c_str();
	runCommand();

	//����cache update
	m_cmd = (char *) TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	runCommand();

	//�޸�����������
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, true);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, true);
	checkTable();
	checkTableDef();

	//�ٴ�����������
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, true);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, true);
	checkTable();
	checkTableDef();

	//�޸Ľ�ֹ������
	m_cmd = TEST_CMD_CACHED_COLUMNS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());		
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, false);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, false);
	checkTable();
	checkTableDef();

	//�ر�MMS
	m_cmd = (char *) TEST_CMD_MMS_DISABLE.c_str();
	runCommand();

	bool expected = false;

	//����������
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	try {
		runCommand();
	} catch (NtseException &) {
		//expected.
		expected = true;
	}
	CPPUNIT_ASSERT(expected);
	expected = false;
	

	//��ֹ������
	m_cmd = TEST_CMD_CACHED_COLUMNS_DISABLE.c_str();
	try {
		runCommand();
	} catch (NtseException &) {
		//expected.
		expected = true;
	}
	CPPUNIT_ASSERT(expected);
	expected = false;

	//��cacheupdateΪfalseʱ���Զ���ÿ���ֶε�cache update����Ϊfalse
	//׼������
	m_cmd = TEST_CMD_MMS_ENABLE.c_str();
	runCommand();
	m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	runCommand();
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	runCommand();
	//����
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	try {
		runCommand();
	} catch (NtseException &) {
		CPPUNIT_FAIL("��cacheupdateΪfalseʱ���Զ���ÿ���ֶε�cache update����Ϊfalseʧ��");
	}	
}


/** 
 * ���������޸Ļ����С�
 */
void TableAlterArgTestCase::testRedoAlterCachedColummns() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1)mms enable, disable cache columns and then enable
	m_cmd = (char *) TEST_CMD_MMS_ENABLE.c_str();
	EXCPT_OPER(runCommand());

	//����cache update
	m_cmd = (char *) TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
	runCommand();

	//disable cache columns
	m_cmd = TEST_CMD_CACHED_COLUMNS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, false);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, false);
	checkTable();
	checkTableDef();

	//enable
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, true);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, true);
	checkTable();
	checkTableDef();

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, true);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, true);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do set CACHED_COLUMNS_DISABLE
	m_cmd = TEST_CMD_CACHED_COLUMNS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, false);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, false);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(CACHED_COLUMNS, COL_NAME_TITLE, true);
	checkValue(CACHED_COLUMNS, COL_NAME_TAGS, true);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}
/** 
 * �����޸�ѹ�������
 */
void TableAlterArgTestCase::testAlterCompressLobs() {
	m_cmd = TEST_CMD_COMPRESS_LOBS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_LOBS, true);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_COMPRESS_LOBS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(COMPRESS_LOBS, false);
	checkTable();
	checkTableDef();
}

/** 
 * ���������޸�ѹ�������
 */
void TableAlterArgTestCase::testRedoAlterCompressLobs() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1)disable and then enable
	//disable
	m_cmd = TEST_CMD_COMPRESS_LOBS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(COMPRESS_LOBS, false);
	checkTable();
	checkTableDef();

	//enable
	m_cmd = TEST_CMD_COMPRESS_LOBS_ENABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_LOBS, true);
	checkTable();
	checkTableDef();

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(COMPRESS_LOBS, true);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do disable
	m_cmd = TEST_CMD_COMPRESS_LOBS_DISABLE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(COMPRESS_LOBS, false);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(COMPRESS_LOBS, true);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}
/** 
 * �����޸Ŀ��аٷֱȡ�
 */
void TableAlterArgTestCase::testAlterHeapPctFree() {
	m_cmd = TEST_CMD_SET_HEAP_PCT_FREE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	const int magic_pct_free_num = 30;
	checkValue(HEAP_PCT_FREE, magic_pct_free_num);
	checkTable();
	checkTableDef();

	//������һ�Σ�
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(HEAP_PCT_FREE, magic_pct_free_num);
	checkTable();
	checkTableDef();
}
/** 
 * ���������޸Ŀ��аٷֱȡ�
 */
void TableAlterArgTestCase::testRedoAlterHeapPctFree() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1) ���� and then �޸�
	//����
	m_cmd = TEST_CMD_SET_HEAP_PCT_FREE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	const int magic_pct_free_num = 30;
	checkValue(HEAP_PCT_FREE, magic_pct_free_num);

	//�޸�
	m_cmd = TEST_CMD_SET_HEAP_PCT_FREE2.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	const int magic_pct_free_num2 = 29;
	checkValue(HEAP_PCT_FREE, magic_pct_free_num2);

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(HEAP_PCT_FREE, magic_pct_free_num2);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do ��������
	m_cmd = TEST_CMD_SET_HEAP_PCT_FREE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(HEAP_PCT_FREE, magic_pct_free_num);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(HEAP_PCT_FREE, magic_pct_free_num2);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}
/** 
 * �����޸ķ���ϵ����
 */
void TableAlterArgTestCase::testAlterSplitFactors() {
	m_cmd = TEST_CMD_SET_SPLIT_FACTORS.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(SPLIT_FACTORS, "IDX_BLOG_PUTTIME", 6);
	checkTable();
	checkTableDef();

	//��������ֵ��ԭֵ��ͬ���ظ�����
	m_cmd = TEST_CMD_SET_SPLIT_FACTORS.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(SPLIT_FACTORS, "IDX_BLOG_PUTTIME", 6);
	checkTable();
	//��������ֵ-1
	m_cmd = TEST_CMD_SET_SPLIT_FACTOR_AUTO.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkTable();
	checkTableDef();
}

/** 
 * ���������޸ķ���ϵ����
 */
void TableAlterArgTestCase::testRedoAlterSplitFactors() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1) ���� and then �޸�
	//����
	m_cmd = TEST_CMD_SET_SPLIT_FACTORS.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	const int mage_factor_num = 6;
	checkValue(SPLIT_FACTORS, "IDX_BLOG_PUTTIME", mage_factor_num);
	checkTable();
	checkTableDef();

	//�޸�
	m_cmd = TEST_CMD_SET_SPLIT_FACTORS2.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	const int mage_factor_num2 = 7;
	checkValue(SPLIT_FACTORS, "IDX_BLOG_PUTTIME", mage_factor_num2);
	checkTable();
	checkTableDef();

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(SPLIT_FACTORS, "IDX_BLOG_PUTTIME", mage_factor_num2);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do ��������
	m_cmd = TEST_CMD_SET_SPLIT_FACTORS.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(SPLIT_FACTORS, "IDX_BLOG_PUTTIME", mage_factor_num);
	checkTable();
	checkTableDef();

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(SPLIT_FACTORS, "IDX_BLOG_PUTTIME", mage_factor_num2);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}
/** 
 * �����޸���չҳ������
 */
void TableAlterArgTestCase::testAlterIncrSize() {
	m_cmd = TEST_CMD_INCREASE_SIZE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	const int magic_inc_num = 64;
	checkValue(INCR_SIZE, magic_inc_num);
	checkTable();
	checkTableDef();

	//��������ֵ��ԭֵ��ͬ���ظ�����
	m_cmd = TEST_CMD_INCREASE_SIZE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(INCR_SIZE, magic_inc_num);
	checkTable();
	checkTableDef();
}

/** 
 * ���������޸���չҳ������
 */
void TableAlterArgTestCase::testRedoAlterIncrSize() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1) ���� and then �޸�
	//����
	m_cmd = TEST_CMD_INCREASE_SIZE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	const int magic_inc_num = 64;
	checkValue(INCR_SIZE, magic_inc_num);
	checkTable();
	checkTableDef();

	//�޸�
	m_cmd = TEST_CMD_INCREASE_SIZE2.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	const int magic_inc_num2 = 128;
	checkValue(INCR_SIZE, magic_inc_num2);
	checkTable();
	checkTableDef();

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(INCR_SIZE, magic_inc_num2);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do ��������
	m_cmd = TEST_CMD_INCREASE_SIZE.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(INCR_SIZE, magic_inc_num);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(INCR_SIZE, magic_inc_num2);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}

/**
* �����޸�ѹ������
*/
void TableAlterArgTestCase::testAlterCompressRows() {
	m_cmd = TEST_CMD_COMPRESS_ROWS.c_str();	

	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_ROWS, true);
	checkTable();
	checkTableDef();

	//��������ֵ��ԭֵ��ͬ���ظ�����
	m_cmd = TEST_CMD_COMPRESS_ROWS.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_ROWS, true);
	checkTable();
	checkTableDef();

	m_cmd = TEST_CMD_COMPRESS_ROWS2.c_str();	
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_ROWS, false);
	checkTable();
	checkTableDef();
}

/**
* ���������޸�ѹ������
*/
void TableAlterArgTestCase::testRedoAlterCompressRows() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1) ���� and then �޸�
	//����
	m_cmd = TEST_CMD_COMPRESS_ROWS.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_ROWS, true);
	checkTable();
	checkTableDef();

	//�޸�
	m_cmd = TEST_CMD_COMPRESS_ROWS2.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_ROWS, false);
	checkTable();
	checkTableDef();

	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkValue(COMPRESS_ROWS, false);
	checkTable();
	checkTableDef();

	//B��״̬��һ���µ�redo
	// (1)do ��������
	m_cmd = TEST_CMD_COMPRESS_ROWS.c_str();
	prepareDataMisc();
	EXCPT_OPER(runCommand());
	checkValue(COMPRESS_ROWS, true);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkValue(COMPRESS_ROWS, false);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}

/**
* �����޸Ķ�����
*/
void TableAlterArgTestCase::testAlterHeapFixLen() {
	{
		//���䳤�ѱ�ɶ�����
		m_cmd = TEST_CMD_HEAP_FIXLEN.c_str();	
		prepareDataMisc();
		NEED_EXCPT(runCommand());
		checkTable();
		checkTableDef();
		tearDown();
	}
	{
		setUp();

		//���䳤�Ѹ�Ϊ�䳤��
		m_cmd = TEST_CMD_HEAP_FIXLEN2.c_str();	
		prepareDataMisc();
		runCommand();
		checkTable();
		checkTableDef();

		tearDown();
	}
	{
		//��ʹ�ö����ѵı�����fix_lenΪtrue
		BlogCountTblBuilder blogCountTblBuilder;
		blogCountTblBuilder.prepareEnv();
		Table *table = blogCountTblBuilder.getTable();

		CPPUNIT_ASSERT(table->getTableDef()->m_fixLen);
		CPPUNIT_ASSERT(table->getTableDef()->m_recFormat == REC_FIXLEN && 
			table->getTableDef()->m_origRecFormat == REC_FIXLEN);
		CPPUNIT_ASSERT(HEAP_VERSION_FLR == DrsHeap::getVersionFromTableDef(table->getTableDef()));

		string str = string(ALTER_TABLE_CMD_COMM) + blogCountTblBuilder.getSchemaName() + "." 
			+ table->getTableDef()->m_name + "." + TableArgAlterHelper::CMD_FIX_LEN + " = true";
		m_cmd = str.c_str();	
		CPPUNIT_ASSERT(NULL != m_cmd);
		AutoPtr<Parser> parser(new Parser(m_cmd));
		const char *token;
		EXCPT_OPER(token = parser->nextToken());

		CPPUNIT_ASSERT(!System::stricmp(token, "alter"));
		m_parser = parser.detatch();
		Connection *conn = blogCountTblBuilder.getDb()->getConnection(false, __FUNCTION__);
		//��̬�޸ı����
		{
			TableArgAlterHelper tblAltHelp(blogCountTblBuilder.getDb(), conn, m_parser, blogCountTblBuilder.getDb()->getConfig()->m_tlTimeout);
			EXCPT_OPER(tblAltHelp.alterTableArgument());
		}
		CPPUNIT_ASSERT(table->getTableDef()->m_fixLen);
		CPPUNIT_ASSERT(table->getTableDef()->m_recFormat == REC_FIXLEN && 
			table->getTableDef()->m_origRecFormat == REC_FIXLEN);
		CPPUNIT_ASSERT(HEAP_VERSION_FLR == DrsHeap::getVersionFromTableDef(table->getTableDef()));

		blogCountTblBuilder.checkTable();
		blogCountTblBuilder.checkTableDef();

		blogCountTblBuilder.getDb()->freeConnection(conn);
	}
	{
		//�������ѱ�ɱ䳤��
		BlogCountTblBuilder blogCountTblBuilder;
		blogCountTblBuilder.prepareEnv();
		Table *table = blogCountTblBuilder.getTable();

		CPPUNIT_ASSERT(table->getTableDef()->m_fixLen);
		CPPUNIT_ASSERT(table->getTableDef()->m_recFormat == REC_FIXLEN && 
			table->getTableDef()->m_origRecFormat == REC_FIXLEN);
		CPPUNIT_ASSERT(HEAP_VERSION_FLR == DrsHeap::getVersionFromTableDef(table->getTableDef()));

		string str = string(ALTER_TABLE_CMD_COMM) + blogCountTblBuilder.getSchemaName() + "." 
			+ table->getTableDef()->m_name + "." + TableArgAlterHelper::CMD_FIX_LEN + " = false";
		m_cmd = str.c_str();	
		CPPUNIT_ASSERT(NULL != m_cmd);
		AutoPtr<Parser> parser(new Parser(m_cmd));
		const char *token;
		EXCPT_OPER(token = parser->nextToken());

		CPPUNIT_ASSERT(!System::stricmp(token, "alter"));
		m_parser = parser.detatch();
		Connection *conn = blogCountTblBuilder.getDb()->getConnection(false, __FUNCTION__);
		//��̬�޸ı����
		{
			TableArgAlterHelper tblAltHelp(blogCountTblBuilder.getDb(), conn, m_parser, blogCountTblBuilder.getDb()->getConfig()->m_tlTimeout);
			EXCPT_OPER(tblAltHelp.alterTableArgument());
		}
		table = blogCountTblBuilder.reOpenTable();
		TableDef *tblDef = table->getTableDef();
		CPPUNIT_ASSERT(!tblDef->m_fixLen);
		CPPUNIT_ASSERT(table->getTableDef()->m_recFormat == REC_VARLEN && 
			table->getTableDef()->m_origRecFormat == REC_FIXLEN);
		CPPUNIT_ASSERT(HEAP_VERSION_VLR == DrsHeap::getVersionFromTableDef(table->getTableDef()));
		
		blogCountTblBuilder.checkTable();
		blogCountTblBuilder.checkTableDef();
			
		blogCountTblBuilder.getDb()->freeConnection(conn);
	}
}

/**
* �����޸������鶨��
*/
void TableAlterArgTestCase::testAlterColGrpDef() {
	//��������
	string invalidCmd1 = TEST_CMD_COL_GROUP_DEF + "(ID, PublishTime, Abstract, Title), (c1, c2)";
	m_cmd = invalidCmd1.c_str();	
	prepareDataMisc();
	NEED_EXCPT(runCommand());
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(false);

	//���Ϸ�����
	string invalidCmd2 = TEST_CMD_COL_GROUP_DEF + "(ID, UserID, PublishTime, Abstract, Title, Content, Tags), (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9,"
		"c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33)";
	m_cmd = invalidCmd2.c_str();	
	prepareDataMisc();
	NEED_EXCPT(runCommand());	
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(false);

	//���Ϸ������鶨��
	string invalidCmd3 = TEST_CMD_COL_GROUP_DEF + "(ID, UserID, PublishTime, Abstract, Title, Content, Tags), (c1, c2, c3, c4, c5, c6, c7, c8, c9,"
		"c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33),()";
	m_cmd = invalidCmd3.c_str();	
	prepareDataMisc();
	NEED_EXCPT(runCommand());	
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(false);
	
	//��ʽ����
	string invalidCmd4 = TEST_CMD_COL_GROUP_DEF + "(ID, UserID, PublishTime, Abstract, Title, Content, Tags), i_nvalid (c1, c2, c3, c4, c5, c6, c7, c8, c9,"
		"c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33)";
	m_cmd = invalidCmd4.c_str();	
	prepareDataMisc();
	NEED_EXCPT(runCommand());	
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(false);

	//��ʽ����2
	string invalidCmd5 = TEST_CMD_COL_GROUP_DEF + "(ID, UserID, PublishTime, Abstract, Title, Content, Tags), , (c1, c2, c3, c4, c5, c6, c7, c8, c9,"
		"c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33)";
	m_cmd = invalidCmd5.c_str();	
	prepareDataMisc();
	NEED_EXCPT(runCommand());	
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(false);

	string goodCmd = TEST_CMD_COL_GROUP_DEF + "(ID, UserID, PublishTime, Abstract, Title, Content, Tags), (c1, c2, c3, c4, c5, c6, c7, c8, c9,"
		"c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20), (c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33)";
	m_cmd = goodCmd.c_str();	
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(true);
}

/**
* ���������޸������鶨��
*/
void TableAlterArgTestCase::testRedoAlterColGrpDef() {
	LsnKeeper lsnKeeper(m_db);
	//A: ״̬һ���µ�redo
	// (1) ���� and then �޸�
	//����
	string goodCmd = TEST_CMD_COL_GROUP_DEF + "(ID, UserID, PublishTime, Abstract, Title, Content, Tags), (c1, c2, c3, c4, c5, c6, c7, c8, c9,"
		"c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20), (c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33)";
	m_cmd = goodCmd.c_str();	
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(true);
	CPPUNIT_ASSERT(getNumColumnGroup() == 3);

	//�޸�
	string goodCmd2 = TEST_CMD_COL_GROUP_DEF + "(ID, UserID, PublishTime), (Abstract, Title, Content, Tags), (c1, c2, c3, c4, c5, c6, c7, c8, c9),"
		"(c10, c11, c12, c13, c14, c15, c16, c17, c18), (c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33)";
	m_cmd = goodCmd2.c_str();	
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(true);
	CPPUNIT_ASSERT(getNumColumnGroup() == 5);


	// (2)��ȡlog
	const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
	if (NULL == log) {
		CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
	}

	// (3) redo
	redoAlterTableArg(log);

	// (4) ���״̬
	checkTable();
	checkTableDef();
	checkTableColoumnGroups(true);
	CPPUNIT_ASSERT(getNumColumnGroup() == 5);

	//B��״̬��һ���µ�redo
	// (1)do ��������
	prepareDataMisc();
	m_cmd = goodCmd.c_str();
	EXCPT_OPER(runCommand());
	checkTableColoumnGroups(true);
	CPPUNIT_ASSERT(getNumColumnGroup() == 3);

	// (2)ʹ��֮ǰ��ȡ����־����redo
	redoAlterTableArg(log);

	// (3) ���״̬
	checkTableColoumnGroups(true);
	CPPUNIT_ASSERT(getNumColumnGroup() == 5);
	checkTable();
	checkTableDef();

	//�ͷ�log��Դ
	delete[] log->m_data;
	delete log;
	log = NULL;
}

/**
* �����޸�ȫ��ѹ���ֵ����
*/
void TableAlterArgTestCase::testAlterDictionaryArg() {	
	TableArgAlterCmdType cmdTypes[] = { COMPRESS_DICT_SIZE, COMPRESS_DICT_MIN_LEN, COMPRESS_DICT_MAX_LEN, COMPRESS_THRESHOLD };
	string cmds[] = { TEST_CMD_COMPRESS_DICT_SIZE, TEST_CMD_COMPRESS_DICT_MIN_LEN, TEST_CMD_COMPRESS_DICT_MAX_LEN, TEST_CMD_COMPRESS_THRESHOLD };
	enum DataIndex {
		VALID,
		INVALID,
		DEFAULT,
	};
	int testDatas[4][3] = {
		{ RowCompressCfg::DEFAULT_DICTIONARY_SIZE - 1, RowCompressCfg::DEFAULT_DICTIONARY_SIZE + 1, RowCompressCfg::DEFAULT_DICTIONARY_SIZE },
		{ RowCompressCfg::MIN_DIC_ITEM_MIN_LEN, RowCompressCfg::MAX_DIC_ITEM_MIN_LEN + 1, RowCompressCfg::DEFAULT_DIC_ITEM_MIN_LEN },
		{ RowCompressCfg::MIN_DIC_ITEM_MAX_LEN, RowCompressCfg::MAX_DIC_ITEM_MAX_LEN + 1, RowCompressCfg::DEFAULT_DIC_ITEM_MAX_LEN },
		{ 60, 101, RowCompressCfg::DEFAULT_ROW_COMPRESS_THRESHOLD }
	};


	//�޸ı���Ϊѹ����
	m_cmd = TEST_CMD_COMPRESS_ROWS.c_str();	
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_ROWS, true);
	checkTable();
	checkTableDef();

	u8 testCases = 4;
	for (u8 i = 0; i < testCases; i++) {
		m_cmd = cmds[i].c_str();	
		
		//���Բ���ȷ�Ĳ���
		prepareDataMisc(2);
		std::ostrstream oos;
		oos << cmds[i] << testDatas[i][INVALID] << '\0';
		m_cmd = oos.str();
		cout << "###############################################################" << endl;
		cout << "# CMD: " << m_cmd << endl;
		cout << "###############################################################" << endl;
		NEED_EXCPT(runCommand());		
		checkValue(cmdTypes[i], (int)testDatas[i][DEFAULT]);
		checkTable();
		checkTableDef();
		delete []m_cmd;

		//������ȷ�Ĳ���
		prepareDataMisc(2);
		std::ostrstream oos2;
		oos2 << cmds[i] << testDatas[i][VALID] << '\0';
		m_cmd = oos2.str();
		cout << "###############################################################" << endl;
		cout << "# CMD: " << m_cmd << endl;
		cout << "###############################################################" << endl;
		EXCPT_OPER(runCommand());	
		checkValue(cmdTypes[i], (int)testDatas[i][VALID]);
		checkTable();
		checkTableDef();

		//��������ֵ��ԭֵ��ͬ���ظ�����
		cout << "###############################################################" << endl;
		cout << "# CMD: " << m_cmd << endl;
		cout << "###############################################################" << endl;
		prepareDataMisc(2);
		EXCPT_OPER(runCommand());	
		checkValue(cmdTypes[i], (int)testDatas[i][VALID]);
		checkTable();
		checkTableDef();
		delete []m_cmd;
	}
}

/**
* ���������޸�ȫ��ѹ���ֵ����
*/
void TableAlterArgTestCase::testRedoAlterDictionaryArg() {
	TableArgAlterCmdType cmdTypes[] = { COMPRESS_DICT_SIZE, COMPRESS_DICT_MIN_LEN, COMPRESS_DICT_MAX_LEN, COMPRESS_THRESHOLD };
	string cmds[] = { TEST_CMD_COMPRESS_DICT_SIZE, TEST_CMD_COMPRESS_DICT_MIN_LEN, TEST_CMD_COMPRESS_DICT_MAX_LEN, TEST_CMD_COMPRESS_THRESHOLD };
	enum DataIndex {
		STAT1,
		STAT2,
	};
	int testDatas[4][2] = {
		{ 4096, 9600 },
		{ 3, 5 },
		{ 30, 40 },
		{ 50, 60 }
	};


	//�޸ı���Ϊѹ����
	m_cmd = TEST_CMD_COMPRESS_ROWS.c_str();	
	prepareDataMisc();
	EXCPT_OPER(runCommand());	
	checkValue(COMPRESS_ROWS, true);
	checkTable();
	checkTableDef();

	u8 testCases = 4;
	for (u8 i = 0; i < testCases; i++) {
		LsnKeeper lsnKeeper(m_db);

		//����
		prepareDataMisc(2);
		std::ostrstream oos;
		oos << cmds[i] << testDatas[i][STAT1] << '\0';
		m_cmd = oos.str();
		cout << "###############################################################" << endl;
		cout << "# CMD: " << m_cmd << endl;
		cout << "###############################################################" << endl;
		EXCPT_OPER(runCommand());		
		checkValue(cmdTypes[i], (int)testDatas[i][STAT1]);
		checkTable();
		checkTableDef();
		delete []m_cmd;
		m_cmd = NULL;

		//�޸�
		prepareDataMisc(2);
		std::ostrstream oos2;
		oos2 << cmds[i] << testDatas[i][STAT2] << '\0';
		m_cmd = oos2.str();
		cout << "###############################################################" << endl;
		cout << "# CMD: " << m_cmd << endl;
		cout << "###############################################################" << endl;
		EXCPT_OPER(runCommand());	
		checkValue(cmdTypes[i], (int)testDatas[i][STAT2]);
		checkTable();
		checkTableDef();
		delete []m_cmd;
		m_cmd = NULL;

		// (2)��ȡlog
		const LogEntry *log = checkAndFetchLastTblArgAlterLog(lsnKeeper.getKeepLsn());
		if (NULL == log) {
			CPPUNIT_FAIL("��ȡ��������־ʧ�ܡ�");
		}

		// (3) redo
		redoAlterTableArg(log);

		// (4) ���״̬
		checkTable();
		checkTableDef();
		checkValue(cmdTypes[i], (int)testDatas[i][STAT2]);

		//B��״̬��һ���µ�redo
		// (1)do ��������
		prepareDataMisc();
		std::ostrstream oos3;
		oos3 << cmds[i] << testDatas[i][STAT1] << '\0';
		m_cmd = oos3.str();
		cout << "###############################################################" << endl;
		cout << "# CMD: " << m_cmd << endl;
		cout << "###############################################################" << endl;
		EXCPT_OPER(runCommand());		
		checkValue(cmdTypes[i], (int)testDatas[i][STAT1]);
		delete []m_cmd;
		m_cmd = NULL;

		// (2)ʹ��֮ǰ��ȡ����־����redo
		redoAlterTableArg(log);

		// (3) ���״̬
		checkValue(cmdTypes[i], (int)testDatas[i][STAT2]);
		checkTable();
		checkTableDef();

		//�ͷ�log��Դ
		delete[] log->m_data;
		delete log;
		log = NULL;
	}
}

/** 
 *  ������Ч���
 */
void TableAlterArgTestCase::testWrongCommand() {
	int testCount = 23;
	int i;
	for (i = 0; i < testCount; i++) {
		switch (i) {
			case 0:
				m_cmd = INVALID_COMMAND_STRING;
				break;
			case 1:
				//����������ַ�
				m_cmd = "alter table set testdb.blog.usemms = false 1";
				break;
			case 2:
				//����ȷ��cache_update����ֵ
			//	m_cmd = "alter table set testdb.blog.cache_update = Invalid_Value";
				break;
			case 3:
				//����ȷ��update_cache_time����ֵ
			//	m_cmd = "alter table set testdb.blog.update_cache_time = abd";
				break;
			case 4:
				//����ȷ��cached_columns����
			//	m_cmd = "alter table set testdb.blog.cached_columns = enable Invalid_Table_Name";
				break;
			case 5:
				//����ȷ��compress_lobs����ֵ
				m_cmd = "alter table set testdb.blog.compress_lobs = Invalid_Value";
				break;
			case 6:
				//����ȷ��heap_pct_free����ֵ
				m_cmd = "alter table set testdb.blog.heap_pct_free = Invalid_Value";
				break;
			case 7:
				//����ȷ��split_factors����ֵ
				m_cmd = "alter table set testdb.blog.split_factors = Invalid_Value";
				break;
			case 8:
				//����ȷ��incr_size����ֵ
				m_cmd = "alter table set testdb.blog.incr_size = Invalid_Value";
				break;
			//Other specific invalid tests may well add below:
			case 9:
				//û����������
				m_cmd = "alter table set";
				break;
			case 10:
				//scheme.table.cmd ��ʽ����
				m_cmd = "alter table set testdb.";
				break;
			case 11:
				//scheme.table.cmd ��ʽ����
				m_cmd = "alter table set testdb.blog.";
				break;
			case 12:
				//scheme.table.cmd ��ʽ����
				m_cmd = "alter table set testdb.testdb.blog.incr_sizeXX";
				break;
			case 13:
				//����ϵ��Ϊֵ���ڹ涨��Χ
				m_cmd = "alter table set testdb.blog.split_factors = IDX_BLOG_PUTTIME -6";
				break;
			case 14:
				//����ϵ����������ַ�
				m_cmd = "alter table set testdb.blog.split_factors = IDX_BLOG_PUTTIME 6 additional remains";
				break;
			case 15:
				//cached_columns ��enable disable����
				m_cmd = "alter table set testdb.blog.cached_columns = xenable ID";
				break;
			case 16:
				//����ϵ�������������ڡ�
				m_cmd = "alter table set testdb.blog.split_factors = IDX_BLOG_PUTTIME_Invalid 6";
				break;
			case 17:
				//����cached columns ����32�������Ƶ����
				m_cmd = "alter table set testdb.blog.cached_columns = enable ID,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33";
				break;
			case 18:
				//����heap pct free > ����ֵ
				m_cmd = "alter table set testdb.blog.HEAP_PCT_FREE = 50";
				break;
			case 19:
				//����cached_columns����������
				m_cmd = "alter table set testdb.blog.cached_columns = enable";
				break;
			case 20:
				//����enable��cache column�Ѿ���������
				//ȷ��mms�ѿ�����cacheupdate����Ϊtrue
				m_cmd = TEST_CMD_MMS_ENABLE.c_str();
				runCommand();
				m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
				runCommand();
				m_cmd = "alter table set testdb.blog.cached_columns = enable ID";
				break;
			case 21:
				//cmd_type ����֧���б���
				m_cmd = "alter table set testdb.blog.wrong_cmd_type = enable ID";
				break;
			case 22:
				//��use mmsΪfalseʱ����cacheupdate
				m_cmd = TEST_CMD_MMS_DISABLE.c_str();
				runCommand();
				m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
				break;
			default:
				CPPUNIT_FAIL("testWrongCommand default case should not be called.");
				break;
		}
		try {
			if (m_cmd == INVALID_COMMAND_STRING)
				break;
			runCommand();
		} catch (NtseException &) {
			//expected.
			std::cout<<"TableAlterTestCase::testWrongCommand test:"<< i<<" passed."<<endl;
			continue;			
		}
		CPPUNIT_FAIL("Failed to throw NtseException.");
	}
}

/**
 * �����������֤��׽�����������г����쳣��
 *
 * @pre m_cmd �����á�
 */
void TableAlterArgTestCase::checkRunCommandFail() {
	CPPUNIT_ASSERT(NULL != m_cmd);
	try {
		runCommand();
	} catch (NtseException &) {
		//expected exception.
		return;
	}
	return;
}

/** 
 * A ����������ʧ��; B ���Ի�ȡmetaLock U ��ʧ��;
 * ���Է������Ƚ�����ģʽ����ΪԪ���ݹ�����/����X���� Ȼ�����ñ�����޸�
 *
 * @note ��������ϵͳ��̬������ء����޸�Ŀ��״̬�ͳ�ʼ״̬һ��ʱ���޸Ĳ���ֱ�ӷ��أ������쳣���ֽ�ʧ�ܡ�
 */
void TableAlterArgTestCase::testLockUpgradeFail() {
	const int magicSubTestNum = 12;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TableAlterTestCase::createBlog", conn);	

	Table *table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	assert(NULL != table);
	//A: ����������ʧ��;
	//[1] ����Ԫ���ݹ�����
	table->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	CPPUNIT_ASSERT(table->getMetaLock(session) == IL_S);
	//[2] �����޸ģ�Ԥ�ڲ�׽ʧ��
	cout<<"��������ʱ���ָ�:Ԥ����Ҫ"<< (magicSubTestNum * m_db->getConfig()->m_tlTimeout) <<"��"<<endl;
	//1. mms disable & enable
	m_cmd = TEST_CMD_MMS_DISABLE.c_str();
	checkRunCommandFail();
	cout<<magicSubTestNum<<"-1"<<endl;

	//2. cache update enable
// 	m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
// 	checkRunCommandFail();
// 	cout<<magicSubTestNum<<"-2"<<endl;

	//3. �޸�cache updateʱ��
// 	m_cmd = TEST_CMD_CACHE_UPDATE_TIME.c_str();
// 	checkRunCommandFail();
// 	cout<<magicSubTestNum<<"-3"<<endl;

	//4. �޸����û�����
// 	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
// 	checkRunCommandFail();
// 	cout<<magicSubTestNum<<"-4"<<endl;

	//5. COMPRESS_LOBS
	m_cmd = TEST_CMD_COMPRESS_LOBS_DISABLE.c_str();
	checkRunCommandFail();
	cout<<magicSubTestNum<<"-5"<<endl;

	//6. HEAP_PCT_FREE
	m_cmd = TEST_CMD_SET_HEAP_PCT_FREE.c_str();
	checkRunCommandFail();
	cout<<magicSubTestNum<<"-6"<<endl;

	//7.SPLIT_FACTORS
	m_cmd = TEST_CMD_SET_SPLIT_FACTORS.c_str();
	checkRunCommandFail();
	cout<<magicSubTestNum<<"-7"<<endl;

	//8.INCREASE_SIZE
	m_cmd = TEST_CMD_INCREASE_SIZE.c_str();
	checkRunCommandFail();
	cout<<magicSubTestNum<<"-8"<<endl;

	//[3] ������X��, �����޸Ĳ�����������������ʱ
	table->lock(session, IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	CPPUNIT_ASSERT(table->getLock(session) == IL_X);

	m_cmd = TEST_CMD_INCREASE_SIZE.c_str();
	checkRunCommandFail();
	cout<<magicSubTestNum<<"-9"<<endl;

	//[4] ������X��
	table->unlock(session, IL_X);

	//[5] �ͷ�Ԫ���ݹ�����
	table->unlockMeta(session, IL_S);	

	//[6] ����һ�޸ĳɹ���
	testAlterUseMms();
	cout<<magicSubTestNum<<"-10 testAlterUseMms()"<<endl;	

	//B ���Ի�ȡmetaLock U ��ʧ��; 
	//���Ӳ���alterColumn metaLockʧ�ܣ�

	m_cmd = TEST_CMD_MMS_ENABLE.c_str();
	runCommand();
// 	m_cmd = TEST_CMD_CACHE_UPDATE_ENABLE.c_str();
// 	runCommand();
// 	m_cmd = TEST_CMD_CACHED_COLUMNS_DISABLE.c_str();
// 	runCommand();
	//����
	table->lockMeta(session, IL_U, -1, __FILE__, __LINE__);
	
	m_cmd = TEST_CMD_CACHED_COLUMNS_ENABLE.c_str();
	checkRunCommandFail();
	table->unlockMeta(session, IL_U);
	cout<<magicSubTestNum<<"-11"<<endl;
	
	//�ر���Դ
	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	
}
/** 
 * ������������
 */
const char* TableAlterArgBigTestCase::getName() {
	return "Alter Table Parameters Test[Big].";
}

/**
 * ��������������
 */
const char* TableAlterArgBigTestCase::getDescription() {
	return "Test alter table parameter functions[Big].";
}

/** 
 * �������͡�
 * @return true, ��С�͵�Ԫ���ԡ�
 */
bool TableAlterArgBigTestCase::isBig() {
	return true;
}
/** 
 * Set up context before start a test.
 * @see <code>TestFixture::setUp()</code>
 */
void TableAlterArgBigTestCase::setUp() {
	m_instance = new TableAlterArgTestCase;
	m_instance->setUp();
}

/** 
 * Clean up after the test run.
 * @see <code>TestFixture::tearDown()</code>
 */
void TableAlterArgBigTestCase::tearDown() {
	m_instance->tearDown();
}

/** 
 * ���¼��������߳���
 */
class TableInsertTestTask : public Thread {
public:
	/** 
	 * ���캯��
	 */
	TableInsertTestTask(TableAlterArgTestCase *instance, uint seconds) : Thread("TableInsertTestTask") {
		m_instance = instance;
		m_stopTime = time(NULL) + seconds;
	}
	/** 
	 * @see <code>Thread::run()</code>
	 */
	virtual void run() {
		while (time(NULL) < m_stopTime) {
			Thread::msleep(INSERT_INTERVAL_MS);
			TableAlterArgTestCase::insertBlog(m_instance->m_db, NUM_ROWS, true);
			cout<<"inserted"<<endl;
		}		
	}
private:
	TableAlterArgTestCase *m_instance;					/* ������������ */
	time_t m_stopTime;									/* ֹͣ�ľ���ʱ�� */
	static const uint INSERT_INTERVAL_MS = 1000;		/* ������ʱ�䣬��λ���� */
	static const uint NUM_ROWS = 10;					/* ������������ */
};


/** 
 * �����б����ݲ�����޸�ģʽ�ͻָ���
 *
 * ������һ���̣߳�ÿ0.1�����100�����ݣ�����һ�̣߳�ÿ2��������ִ��һ�α��޸Ĳ�����
 */
void TableAlterArgBigTestCase::testWithTableInsert() {
	assert(NULL != m_instance);
	
	//���������߳�
	TableInsertTestTask insertTask(m_instance, RUN_TIME_SECONDS);

	insertTask.start();
	
	//�޸ı�������ԡ�
	alterTableArgs(RUN_TIME_SECONDS);

	insertTask.join();
}

/** 
 * �����޸ı������
 * 
 */
void TableAlterArgBigTestCase::testRedoAlterTableArgs() {
	cout<<"Test will take about " << RUN_TIME_SECONDS << " seconds."<<endl;
	assert(NULL != m_instance);

	time_t stopTime = time(NULL) + RUN_TIME_SECONDS;
	const int Magic_NUM_TESTS = 8;
	try {
		while (time(NULL) < stopTime) {
			Thread::msleep(ALTER_INTERVAL_MS);
			for (int i = 0; i < Magic_NUM_TESTS; i++) {
				Thread::msleep(ALTER_INNER_TESTS_INTERVAL_MS);
				switch (i) {
					case 0:
						m_instance->testRedoAlterUseMms();
						cout<<"m_instance->testRedoAlterUseMms();"<<endl;
						break;
					case 1:
						m_instance->testRedoAlterCacheUpdate();
						cout<<"m_instance->testRedoAlterCacheUpdate();"<<endl;
						break;
					case 2:
						m_instance->testRedoAlterCacheUpdateTime();
						cout<<"m_instance->testRedoAlterCacheUpdateTime();"<<endl;
						break;
					case 3:
						m_instance->testRedoAlterCachedColummns();
						cout<<"m_instance->testRedoAlterCachedColummns();"<<endl;
						break;
					case 4:
						m_instance->testRedoAlterCompressLobs();
						cout<<"m_instance->testRedoAlterCompressLobs();"<<endl;
						break;
					case 5:
						m_instance->testRedoAlterHeapPctFree();
						cout<<"m_instance->testRedoAlterHeapPctFree();"<<endl;
						break;
					case 6:
						m_instance->testRedoAlterIncrSize();
						cout<<"m_instance->testRedoAlterIncrSize();"<<endl;
						break;
					case 7:
						m_instance->testRedoAlterSplitFactors();
						cout<<"m_instance->testRedoAlterSplitFactors();"<<endl;
						break;
					default:
						break;
				}
			}
		}
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());	
	}
}
/** 
 * �޸ı������
 *
 * @param runTimeSeconds �޸ı����ִ�е���ʱ�䣬��λ�롣
 */
void TableAlterArgBigTestCase::alterTableArgs(uint runTimeSeconds) {
	cout<<"Test will take about " << runTimeSeconds << " seconds."<<endl;
	assert(NULL != m_instance);

	time_t stopTime = time(NULL) + runTimeSeconds;
	const int Magic_NUM_TESTS = 8;
	try {
		while (time(NULL) < stopTime) {
			Thread::msleep(ALTER_INTERVAL_MS);
			for (int i = 0; i < Magic_NUM_TESTS; i++) {
				Thread::msleep(ALTER_INNER_TESTS_INTERVAL_MS);
				switch (i) {
					case 0:
						m_instance->testAlterUseMms();
						cout<<"m_instance->testAlterUseMms();"<<endl;
						break;
					case 1:
						m_instance->testAlterCacheUpdate();
						cout<<"m_instance->testAlterCacheUpdate();"<<endl;
						break;
					case 2:
						m_instance->testAlterCacheUpdateTime();
						cout<<"m_instance->testAlterCacheUpdateTime();"<<endl;
						break;
					case 3:
						m_instance->testAlterCachedColummns();
						cout<<"m_instance->testAlterCachedColummns();"<<endl;
						break;
					case 4:
						m_instance->testAlterCompressLobs();
						cout<<"m_instance->testAlterCompressLobs();"<<endl;
						break;
					case 5:
						m_instance->testAlterHeapPctFree();
						cout<<"m_instance->testAlterHeapPctFree();"<<endl;
						break;
					case 6:
						m_instance->testAlterIncrSize();
						cout<<"m_instance->testAlterIncrSize();"<<endl;
						break;
					case 7:
						m_instance->testAlterSplitFactors();
						cout<<"m_instance->testAlterSplitFactors();"<<endl;
						break;
					default:
						break;
				}
			}
		}
	} catch (NtseException &e) {
		CPPUNIT_FAIL(e.getMessage());	
	}

}
