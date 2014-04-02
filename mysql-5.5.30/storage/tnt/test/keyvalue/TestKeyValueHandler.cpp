/*
 *	����KeyValueHandler����api��
 *
 *	@author	�ζ���(liaodingbai@corp.netease.com)
 */
#ifdef NTSE_KEYVALUE_SERVER

#include "TestKeyValueHandler.h"
#include "keyvalue/KeyValueHelper.h"
#include "util/File.h"
#include "misc/TableDef.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "Test.h"
#include <vector>
#include "util/Bitmap.h"
#include <sstream>

using namespace ntse;
using namespace std;

/**
 *	����ntse���ݿ�ĸ�Ŀ¼·��
 */
const string KeyValueHandlerTestCase::baseDir = "kv-db";

const char* KeyValueHandlerTestCase::getName() {
	return "Key-value handler test.";
}

const char* KeyValueHandlerTestCase::getDescription() {
	return "Test key-value handler operations.";
}

bool KeyValueHandlerTestCase::isBig() {
	return false;
}

/**
 *	Set up the test environment.
 */
void KeyValueHandlerTestCase::setUp()	{
	/** ���ݿ��׼�� */
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

	/** ���ݱ��׼�� */
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, baseDir.c_str(), "KeyValueTest");
	tdb.addColumn("id", CT_BIGINT, false);
	tdb.addColumnS("name", CT_VARCHAR, 20, false);
	tdb.addColumn("currency", CT_FLOAT);
	tdb.addColumn("balance", CT_DOUBLE, false);
	tdb.addColumn("age", CT_INT, false);
	tdb.addColumnS("address", CT_VARCHAR, 50, false);
	tdb.addColumn("tinyID", CT_TINYINT);
	tdb.addColumn("smallID", CT_SMALLINT);
	tdb.addColumn("mediumID", CT_MEDIUMINT);
	tdb.addColumn("bigID", CT_BIGINT);
	tdb.addColumnS("charName", CT_CHAR, 4);
	tdb.addColumnS("varbinary", CT_BINARY, 9);
	tdb.addIndex("PRIMARY", true, true, "id", "name", NULL);
	TableDef *tableDef = tdb.getTableDef();

	m_table = new TblInterface(m_db, "KeyValueTest");
	EXCPT_OPER(m_table->create(tableDef));
	m_table->open();

	m_instance = new KeyValueHandler(m_db);
}

/**
*	Clear the test environment.
*/
void KeyValueHandlerTestCase::tearDown() {
	m_instance->clearOpenTables();

	if (m_instance) {
		delete m_instance;
	}

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

/**
 *	KeyValueHandler::get�ľ�ȷ����һ
 *	
 *	���ĳһ��Ϊnullable�������д�������ʱ����
 */
void KeyValueHandlerTestCase::testGet_Accuracy1()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";
	s8 tinyID = 12;
	s16 smallID = 13;
	s32 mediumID = 234;
	s64 bigID = 23414141;
	string charName = "char";
	string binaryString = "012345678";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size())
		->writeNumber(6, tinyID)->writeNumber(7, smallID)->writeMediumInt(8, mediumID)->writeNumber(9, bigID)
		->writeChar(10, charName.c_str(), charName.size());
	RedRecord::writeRaw(m_table->getTableDef(), rec.getRecord()->m_data, 11, (byte*)binaryString.c_str(),
		binaryString.size());
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** IndexKey */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);
	attrs.push_back(6);
	attrs.push_back(7);
	attrs.push_back(8);
	attrs.push_back(9);
	attrs.push_back(10);
	attrs.push_back(11);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == attrs.size() + 1);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(!bm.isSet(4) && !bm.isSet(5) && !bm.isSet(6) && !bm.isSet(7));
	CPPUNIT_ASSERT(!bm.isSet(8));
	CPPUNIT_ASSERT(isEqual(currency, *(float*)retValue[1].c_str()));
	CPPUNIT_ASSERT(isEqual(balance, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(age == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT(address == retValue[4]);
	CPPUNIT_ASSERT(tinyID == *(s8*)retValue[5].c_str());
	CPPUNIT_ASSERT(smallID == *(s16*)retValue[6].c_str());
	CPPUNIT_ASSERT(mediumID == *(s32*)retValue[7].c_str());
	CPPUNIT_ASSERT(bigID == *(s64*)retValue[8].c_str());
	CPPUNIT_ASSERT(charName == retValue[9]);
	CPPUNIT_ASSERT(binaryString == retValue[10]);
}

/**
*	KeyValueHandler::get�ľ�ȷ���Զ�
*	
*	���ĳһ��Ϊnullable�������в���������ʱ���ö�Ӧ�Ŀ�ֵλͼλΪ1
*/
void KeyValueHandlerTestCase::testGet_Accuracy2()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);


	/** IndexKey */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);

	/**
	 *	��ʱcurrency��Ӧ��Ϊnull
	 */
	CPPUNIT_ASSERT(bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(balance, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(age == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT(address == retValue[4]);
}

/**
*	KeyValueHandler::get�ľ�ȷ������
*	
*	��key��Ӧ�ļ�¼������ʱ������empty��
*/
void KeyValueHandlerTestCase::testGet_Accuracy3()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** IndexKey */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, 23);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	vector<int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	/** �����ؽ�� */
	CPPUNIT_ASSERT(retValue.size() == 0);
}

/**
*	KeyValueHandler::put�ľ�ȷ����һ
*	
*	��key��Ӧ�ļ�¼������ʱ������ɹ�������1��
*/
void KeyValueHandlerTestCase::testPut_Accuracy1()	{
	/** ���ݼ�¼��׼�� */
	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";
	s8 tinyID = 12;
	s16 smallID = 13;
	s32 mediumID = 234;
	s64 bigID = 23414141;
	string charName = "char";
	string binaryString = "012345678";

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** Other Attributes */
	Attrs values;
	{
		Attr curr;
		curr.attrNo = 2;
		number2String(curr.value, currency);

		Attr bal;
		bal.attrNo = 3;
		number2String(bal.value, balance);

		Attr ageAttr;
		ageAttr.attrNo = 4;
		number2String(ageAttr.value, age);

		Attr addressAttr;
		addressAttr.attrNo = 5;
		addressAttr.value = address;

		Attr tiny;
		tiny.attrNo = 6;
		number2String(tiny.value, tinyID);

		Attr small;
		small.attrNo = 7;
		number2String(small.value, smallID);

		Attr medium;
		medium.attrNo = 8;
		number2String(medium.value, mediumID);

		Attr big;
		big.attrNo = 9;
		number2String(big.value, bigID);

		Attr charAttr;
		charAttr.attrNo = 10;
		charAttr.value = charName;

		Attr binaryAttr;
		binaryAttr.attrNo = 11;
		binaryAttr.value = binaryString;

		/** ���Կ�ֵλͼ */
		int byteCount = (10 + 7) / 8;
		byte *bmMem = new byte[byteCount];
		memset(bmMem, 0 ,byteCount);
		Bitmap bm(bmMem, byteCount * 8);
		bm.clearBit(0);
		bm.clearBit(1);
		bm.clearBit(2);
		bm.clearBit(3);
		bm.clearBit(4);
		bm.clearBit(5);
		bm.clearBit(6);
		bm.clearBit(7);
		bm.clearBit(8);
		bm.clearBit(9);

		bytes2String(values.bmp, bmMem, byteCount);
		values.attrlist.push_back(curr);
		values.attrlist.push_back(bal);
		values.attrlist.push_back(ageAttr);
		values.attrlist.push_back(addressAttr);
		values.attrlist.push_back(tiny);
		values.attrlist.push_back(small);
		values.attrlist.push_back(medium);
		values.attrlist.push_back(big);
		values.attrlist.push_back(charAttr);
		values.attrlist.push_back(binaryAttr);
	}

	::int8_t effectRows = m_instance->put(tableInfo, key, values, tableDef.version);
	CPPUNIT_ASSERT(effectRows==1); /** Ӱ���¼��ĿΪ1 */

	/**
	 *	������ļ�¼��ȷ��
	 */
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(currency, *(float*)retValue[1].c_str()));
	CPPUNIT_ASSERT(isEqual(balance, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(age == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT(address == retValue[4]);
}

/**
*	KeyValueHandler::put�ľ�ȷ���Զ�
*	
*	��key��Ӧ�ļ�¼����ʱ������ʧ�ܣ�����0��
*/
void KeyValueHandlerTestCase::testPut_Accuracy2()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** Other Attributes */
	Attrs values;
	{
		Attr curr;
		curr.attrNo = 2;
		number2String(curr.value, currency);

		Attr bal;
		bal.attrNo = 3;
		number2String(bal.value, balance);

		Attr ageAttr;
		ageAttr.attrNo = 4;
		number2String(ageAttr.value, age);

		Attr addressAttr;
		addressAttr.attrNo = 5;
		addressAttr.value = address;

		/** ���Կ�ֵλͼ */
		int byteCount = (4 + 7) / 8;
		byte *bmMem = new byte[byteCount];
		memset(bmMem, 0 ,byteCount);
		Bitmap bm(bmMem, byteCount * 8);
		bm.clearBit(0);
		bm.clearBit(1);
		bm.clearBit(2);
		bm.clearBit(3);

		bytes2String(values.bmp, bmMem, byteCount);
		values.attrlist.push_back(curr);
		values.attrlist.push_back(bal);
		values.attrlist.push_back(ageAttr);
		values.attrlist.push_back(addressAttr);
	}

	::int8_t effectRows = m_instance->put(tableInfo, key, values, tableDef.version);
	CPPUNIT_ASSERT(effectRows==0); /** ����¼�Ѿ����ڣ�Ӱ���¼��ĿΪ0 */
}

/**
*	KeyValueHandler::put�ľ�ȷ������
*	
*	�����������Ϊnullableʱ�����趨����ֵ��Ӧ����ɹ���
*/
void KeyValueHandlerTestCase::testPut_Accuracy3()	{
	/** ���ݼ�¼��׼�� */
	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** Other Attributes */
	Attrs values;
	{
		Attr curr;
		curr.attrNo = 2;
		number2String(curr.value, curr);
		
		Attr bal;
		bal.attrNo = 3;
		number2String(bal.value, balance);

		Attr ageAttr;
		ageAttr.attrNo = 4;
		number2String(ageAttr.value, age);

		Attr addressAttr;
		addressAttr.attrNo = 5;
		addressAttr.value = address;

		/** ���Կ�ֵλͼ */
		int byteCount = (4 + 7) / 8;
		byte *bmMem = new byte[byteCount];
		memset(bmMem, 0 ,byteCount);
		Bitmap bm(bmMem, byteCount * 8);
		bm.setBit(0);
		bm.clearBit(1);
		bm.clearBit(2);
		bm.clearBit(3);

		bytes2String(values.bmp, bmMem, byteCount);
		values.attrlist.push_back(curr);
		values.attrlist.push_back(bal);
		values.attrlist.push_back(ageAttr);
		values.attrlist.push_back(addressAttr);
	}

	::int8_t effectRows = m_instance->put(tableInfo, key, values, tableDef.version);
	CPPUNIT_ASSERT(effectRows==1); /** Ӱ���¼��ĿΪ1 */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);

	/**
	*	��ʱcurrency��Ӧ��Ϊnull
	*/
	CPPUNIT_ASSERT(bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(balance, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(age == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT(address == retValue[4]);
}

/**
 *	KeyValueHandlerTestCase::setrec��ȷ����һ
 *
 *	�����¼�����ڣ�������¼������1
 */
void KeyValueHandlerTestCase::testSetrec_Accuracy1()	{
	/** ���ݼ�¼��׼�� */
	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** Other Attributes */
	Attrs values;
	{
		Attr curr;
		curr.attrNo = 2;
		number2String(curr.value, currency);

		Attr bal;
		bal.attrNo = 3;
		number2String(bal.value, balance);

		Attr ageAttr;
		ageAttr.attrNo = 4;
		number2String(ageAttr.value, age);

		Attr addressAttr;
		addressAttr.attrNo = 5;
		addressAttr.value = address;

		/** ���Կ�ֵλͼ */
		int byteCount = (4 + 7) / 8;
		byte *bmMem = new byte[byteCount];
		memset(bmMem, 0 ,byteCount);
		Bitmap bm(bmMem, byteCount * 8);
		bm.clearBit(0);
		bm.clearBit(1);
		bm.clearBit(2);
		bm.clearBit(3);

		bytes2String(values.bmp, bmMem, byteCount);
		values.attrlist.push_back(curr);
		values.attrlist.push_back(bal);
		values.attrlist.push_back(ageAttr);
		values.attrlist.push_back(addressAttr);
	}

	::int8_t effectRows = m_instance->setrec(tableInfo, key, values, tableDef.version);
	CPPUNIT_ASSERT(effectRows==1); /** Ӱ���¼��ĿΪ1 */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(currency, *(float*)retValue[1].c_str()));
	CPPUNIT_ASSERT(isEqual(balance, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(age == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT(address == retValue[4]);
}

/**
*	KeyValueHandlerTestCase::setrec��ȷ����һ
*
*	�����¼���ڣ���Ըü�¼��ȡupdate����������1
*/
void KeyValueHandlerTestCase::testSetrec_Accuracy2()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);


	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** Other Attributes */
	Attrs values;
	{
		Attr curr;
		curr.attrNo = 2;
		number2String(curr.value, currency);

		Attr bal;
		bal.attrNo = 3;
		number2String(bal.value, 250.4);

		Attr ageAttr;
		ageAttr.attrNo = 4;
		number2String(ageAttr.value, 24);

		Attr addressAttr;
		addressAttr.attrNo = 5;
		addressAttr.value = "china";

		/** ���Կ�ֵλͼ */
		int byteCount = (4 + 7) / 8;
		byte *bmMem = new byte[byteCount];
		memset(bmMem, 0 ,byteCount);
		Bitmap bm(bmMem, byteCount * 8);
		bm.clearBit(0);
		bm.clearBit(1);
		bm.clearBit(2);
		bm.clearBit(3);

		bytes2String(values.bmp, bmMem, byteCount);
		values.attrlist.push_back(curr);
		values.attrlist.push_back(bal);
		values.attrlist.push_back(ageAttr);
		values.attrlist.push_back(addressAttr);
	}

	::int8_t effectRows = m_instance->setrec(tableInfo, key, values, tableDef.version);
	CPPUNIT_ASSERT(effectRows==1); /** Ӱ���¼��ĿΪ1 */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(currency, *(float*)retValue[1].c_str()));
	CPPUNIT_ASSERT(ntse::isEqual(250.4, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(24 == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT("china" == retValue[4]);
}

/**
*	KeyValueHandlerTestCase::replace��ȷ����һ
*
*	�����¼���ڣ���Ըü�¼��ȡupdate����������1
*/
void KeyValueHandlerTestCase::testReplace_Accuracy1()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);


	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);
	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** Other Attributes */
	Attrs values;
	{
		Attr curr;
		curr.attrNo = 2;
		number2String(curr.value, currency);

		Attr bal;
		bal.attrNo = 3;
		number2String(bal.value, 250.4);

		Attr ageAttr;
		ageAttr.attrNo = 4;
		number2String(ageAttr.value, 24);

		Attr addressAttr;
		addressAttr.attrNo = 5;
		addressAttr.value = "china";

		/** ���Կ�ֵλͼ */
		int byteCount = (4 + 7) / 8;
		byte *bmMem = new byte[byteCount];
		memset(bmMem, 0 ,byteCount);
		Bitmap bm(bmMem, byteCount * 8);
		bm.clearBit(0);
		bm.clearBit(1);
		bm.clearBit(2);
		bm.clearBit(3);

		bytes2String(values.bmp, bmMem, byteCount);
		values.attrlist.push_back(curr);
		values.attrlist.push_back(bal);
		values.attrlist.push_back(ageAttr);
		values.attrlist.push_back(addressAttr);
	}

	::int8_t effectRows = m_instance->replace(tableInfo, key, values, tableDef.version);
	CPPUNIT_ASSERT(effectRows==1); /** Ӱ���¼��ĿΪ1 */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(currency, *(float*)retValue[1].c_str()));
	CPPUNIT_ASSERT(ntse::isEqual(250.4, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(24 == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT("china" == retValue[4]);
}

/**
*	KeyValueHandlerTestCase::replace��ȷ����һ
*
*	�����¼�����ڣ��򲻲�ȡ�κβ���������0
*/
void KeyValueHandlerTestCase::testReplace_Accuracy2()	{
	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** Other Attributes */
	Attrs values;
	{
		Attr curr;
		curr.attrNo = 2;
		number2String(curr.value, currency);

		Attr bal;
		bal.attrNo = 3;
		number2String(bal.value, balance);

		Attr ageAttr;
		ageAttr.attrNo = 4;
		number2String(ageAttr.value, age);

		Attr addressAttr;
		addressAttr.attrNo = 5;
		addressAttr.value = address;

		/** ���Կ�ֵλͼ */
		int byteCount = (4 + 7) / 8;
		byte *bmMem = new byte[byteCount];
		memset(bmMem, 0 ,byteCount);
		Bitmap bm(bmMem, byteCount * 8);
		bm.clearBit(0);
		bm.clearBit(1);
		bm.clearBit(2);
		bm.clearBit(3);

		bytes2String(values.bmp, bmMem, byteCount);
		values.attrlist.push_back(curr);
		values.attrlist.push_back(bal);
		values.attrlist.push_back(ageAttr);
		values.attrlist.push_back(addressAttr);
	}

	::int8_t effectRows = m_instance->replace(tableInfo, key, values, tableDef.version);
	CPPUNIT_ASSERT(effectRows==0); /** Ӱ���¼��ĿΪ0 */
}

/**
*	KeyValueHandlerTestCase::remove��ȷ����һ
*
*	�����¼���ڣ����ȡɾ������������1
*/
void KeyValueHandlerTestCase::testRemove_Accuracy1()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	::int8_t effectRows = m_instance->remove(tableInfo, key, tableDef.version);
	CPPUNIT_ASSERT(effectRows==1); /** Ӱ���¼��ĿΪ1 */

	/** ɾ�����¼Ӧ�ò����ڵ� */
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	/** �����ؽ�� */
	CPPUNIT_ASSERT(retValue.size() == 0);
}

/**
*	KeyValueHandlerTestCase::remove��ȷ���Զ�
*
*	�����¼�����ڣ��򲻲�ȡ�κβ���������0
*/
void KeyValueHandlerTestCase::testRemove_Accuracy2()	{
	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, 12);

		Attr pName;
		pName.attrNo = 1;
		pName.value = "not-exist";

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	::int8_t effectRows = m_instance->remove(tableInfo, key, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** Ӱ���¼��ĿΪ1 */
}

/**
 *	KeyValueHandlerTestCase::update��ȷ����һ
 */
void KeyValueHandlerTestCase::testUpdate_Accuracy1()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";
	s8 tinyID = 12;
	s16 smallID = 13;
	s32 mediumID = 234;
	s64 bigID = 23414141;
	string charName = "char";
	string binaryString = "012345678";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size())
		->writeNumber(6, tinyID)->writeNumber(7, smallID)->writeMediumInt(8, mediumID)->writeNumber(9, bigID)
		->writeChar(10, charName.c_str(), charName.size());
	RedRecord::writeRaw(m_table->getTableDef(), rec.getRecord()->m_data, 11, (byte*)binaryString.c_str(),
		binaryString.size());
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 2;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::EQLESS;
		colNO = 3;
		c_one.valueTwo.dataType = DataType::KV_COL;
		number2String(c_one.valueTwo.dataValue, colNO);

		/** ��������������Ƚ�һ */
		Cond c_two;
		c_two.valueOne.dataType = DataType::KV_COL;
		number2String(c_two.valueOne.dataValue, colNO);
		c_two.op = Op::GRATER;
		double comBalance = 30.0;
		c_two.valueTwo.dataType = DataType::KV_DOUBLE;
		number2String(c_two.valueTwo.dataValue, comBalance);

		/** ��������������Ƚ϶� */
		Cond c_three;
		colNO = 4;
		c_three.valueOne.dataType = DataType::KV_COL;
		number2String(c_three.valueOne.dataValue, colNO);
		c_three.op = Op::EQ;
		int comAge = 25;
		c_three.valueTwo.dataType = DataType::KV_INT;
		number2String(c_three.valueTwo.dataValue, comAge);

		/** �������ַ����Ƚ� */
		Cond c_four;
		colNO = 5;
		c_four.valueOne.dataType = DataType::KV_COL;
		number2String(c_four.valueOne.dataValue, colNO);
		c_four.op = Op::LIKE;
		c_four.valueTwo.dataType = DataType::KV_VARCHAR;
		c_four.valueTwo.dataValue = "net%";

		Cond c_five;
		colNO = 6;
		c_five.valueOne.dataType = DataType::KV_COL;
		number2String(c_five.valueOne.dataValue, colNO);
		c_five.op = Op::EQ;
		s8 tiny = 12;
		c_five.valueTwo.dataType = DataType::KV_TINYINT;
		number2String(c_five.valueTwo.dataValue, tiny);

		Cond c_six;
		colNO = 7;
		c_six.valueOne.dataType = DataType::KV_COL;
		number2String(c_six.valueOne.dataValue, colNO);
		c_six.op = Op::EQ;
		s16 small = 13;
		c_six.valueTwo.dataType = DataType::KV_SMALLINT;
		number2String(c_six.valueTwo.dataValue, small);

		Cond c_seven;
		colNO = 8;
		c_seven.valueOne.dataType = DataType::KV_COL;
		number2String(c_seven.valueOne.dataValue, colNO);
		c_seven.op = Op::LESS;
		s32 medium = 4235;
		c_seven.valueTwo.dataType = DataType::KV_MEDIUMINT;
		number2String(c_seven.valueTwo.dataValue, medium);

		Cond c_eight;
		colNO = 9;
		c_eight.valueOne.dataType = DataType::KV_COL;
		number2String(c_eight.valueOne.dataValue, colNO);
		c_eight.op = Op::GRATER;
		s64 big = 24134;
		c_eight.valueTwo.dataType = DataType::KV_BIGINT;
		number2String(c_eight.valueTwo.dataValue, big);

		Cond c_night;
		colNO = 10;
		c_night.valueOne.dataType = DataType::KV_COL;
		number2String(c_night.valueOne.dataValue, colNO);
		c_night.op = Op::LESS;
		c_night.valueTwo.dataType = DataType::KV_CHAR;
		c_night.valueTwo.dataValue = "cida";

		Cond c_ten;
		colNO = 11;
		c_ten.valueOne.dataType = DataType::KV_COL;
		number2String(c_ten.valueOne.dataValue, colNO);
		c_ten.op = Op::EQ;
		c_ten.valueTwo.dataType = DataType::KV_BINARY;
		c_ten.valueTwo.dataValue = "012345678";

		conds.push_back(c_one);
		conds.push_back(c_two);
		conds.push_back(c_three);
		conds.push_back(c_four);
		conds.push_back(c_five);
		conds.push_back(c_six);
		conds.push_back(c_seven);
		conds.push_back(c_eight);
		conds.push_back(c_night);
		conds.push_back(c_ten);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SETNULL;

		DriverUpdateMode modeTwo;
		modeTwo.attrNo = 3;
		modeTwo.mod = Mode::SET;
		double upBalance = 2345.0;
		number2String(modeTwo.value, upBalance);

		DriverUpdateMode modeThree;
		modeThree.attrNo = 4;
		modeThree.mod = Mode::INCR;
		number2String(modeThree.value, 1);

		DriverUpdateMode modeFour;
		modeFour.attrNo = 5;
		modeFour.mod = Mode::APPEND;
		modeFour.value = "-163";

		DriverUpdateMode modeFive;
		modeFive.attrNo = 6;
		modeFive.mod = Mode::INCR;
		number2String(modeFive.value, (s8)1);

		DriverUpdateMode modeSix;
		modeSix.attrNo = 7;
		modeSix.mod = Mode::INCR;
		number2String(modeSix.value, (s16)1);

		DriverUpdateMode modeSeven;
		modeSeven.attrNo = 8;
		modeSeven.mod = Mode::INCR;
		number2String(modeSeven.value, (s32)1);

		DriverUpdateMode modeEight;
		modeEight.attrNo = 9;
		modeEight.mod = Mode::INCR;
		number2String(modeEight.value, (s64)1);

		DriverUpdateMode modeNight;
		modeNight.attrNo = 10;
		modeNight.mod = Mode::APPEND;
		modeNight.value = "";

		DriverUpdateMode modeTen;
		modeTen.attrNo = 11;
		modeTen.mod = Mode::SET;
		modeTen.value = "876543210";

		updatemodes.push_back(modeOne);
		updatemodes.push_back(modeTwo);
		updatemodes.push_back(modeThree);
		updatemodes.push_back(modeFour);
		updatemodes.push_back(modeFive);
		updatemodes.push_back(modeSix);
		updatemodes.push_back(modeSeven);
		updatemodes.push_back(modeEight);
		updatemodes.push_back(modeNight);
		updatemodes.push_back(modeTen);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 1); /** ����һ�м�¼ */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);
	attrs.push_back(6);
	attrs.push_back(7);
	attrs.push_back(8);
	attrs.push_back(9);
	attrs.push_back(10);
	attrs.push_back(11);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == attrs.size() + 1);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(!bm.isSet(4) && !bm.isSet(5) && !bm.isSet(6) && !bm.isSet(7));
	CPPUNIT_ASSERT(!bm.isSet(8));
	CPPUNIT_ASSERT(isEqual(2345.0, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(age + 1 == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT("netease-163" == retValue[4]);
	CPPUNIT_ASSERT(tinyID + 1 == *(s8*)retValue[5].c_str());
	CPPUNIT_ASSERT(smallID + 1== *(s16*)retValue[6].c_str());
	CPPUNIT_ASSERT(mediumID + 1 == *(s32*)retValue[7].c_str());
	CPPUNIT_ASSERT(bigID + 1== *(s64*)retValue[8].c_str());
	CPPUNIT_ASSERT(charName == retValue[9]);
	CPPUNIT_ASSERT("876543210" == retValue[10]);
}

/**
*	KeyValueHandlerTestCase::update��ȷ���Զ�
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy2()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 2;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::ISNULL;

		/** ��������������Ƚ�һ */
		Cond c_two;
		c_two.valueOne.dataType = DataType::KV_COL;
		number2String(c_two.valueOne.dataValue, colNO + 1);
		c_two.op = Op::EQGRATER;
		double comBalance = 234.3;
		c_two.valueTwo.dataType = DataType::KV_DOUBLE;
		number2String(c_two.valueTwo.dataValue, comBalance);

		/** ��������������Ƚ϶� */
		Cond c_three;
		colNO = 4;
		c_three.valueOne.dataType = DataType::KV_COL;
		number2String(c_three.valueOne.dataValue, colNO);
		c_three.op = Op::LESS;
		int comAge = 29;
		c_three.valueTwo.dataType = DataType::KV_INT;
		number2String(c_three.valueTwo.dataValue, comAge);

		/** �������ַ����Ƚ� */
		Cond c_four;
		colNO = 5;
		c_four.valueOne.dataType = DataType::KV_COL;
		number2String(c_four.valueOne.dataValue, colNO);
		c_four.op = Op::LIKE;
		c_four.valueTwo.dataType = DataType::KV_VARCHAR;
		c_four.valueTwo.dataValue = "%et%";

		conds.push_back(c_one);
		conds.push_back(c_two);
		conds.push_back(c_three);
		conds.push_back(c_four);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		DriverUpdateMode modeTwo;
		modeTwo.attrNo = 3;
		modeTwo.mod = Mode::SET;
		double upBalance = 2345.0;
		number2String(modeTwo.value, upBalance);

		DriverUpdateMode modeThree;
		modeThree.attrNo = 4;
		modeThree.mod = Mode::DECR;
		number2String(modeThree.value, (s32)1);

		DriverUpdateMode modeFour;
		modeFour.attrNo = 5;
		modeFour.mod = Mode::PREPEND;
		modeFour.value = "163-";

		updatemodes.push_back(modeOne);
		updatemodes.push_back(modeTwo);
		updatemodes.push_back(modeThree);
		updatemodes.push_back(modeFour);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 1); /** ����һ�м�¼ */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(currency, *(float*)retValue[1].c_str()));
	CPPUNIT_ASSERT(isEqual(2345.0, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(24 == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT("163-netease" == retValue[4]);
}

/**
*	KeyValueHandlerTestCase::update��ȷ������
*
*	��Ҫ����NULLSAFEEQ��������������
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy3()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 2;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::NULLSAFEEQ;
		c_one.valueTwo.dataType = DataType::KV_NULL;

		/** ��������������Ƚ�һ */
		Cond c_two;
		c_two.valueOne.dataType = DataType::KV_COL;
		number2String(c_two.valueOne.dataValue, colNO + 1);
		c_two.op = Op::LESS;
		double comBalance = 1000.0;
		c_two.valueTwo.dataType = DataType::KV_DOUBLE;
		number2String(c_two.valueTwo.dataValue, comBalance);

		/** ��������������Ƚ϶� */
		Cond c_three;
		colNO = 4;
		c_three.valueOne.dataType = DataType::KV_COL;
		number2String(c_three.valueOne.dataValue, colNO);
		c_three.op = Op::NULLSAFEEQ;
		int comAge = 25;
		c_three.valueTwo.dataType = DataType::KV_INT;
		number2String(c_three.valueTwo.dataValue, comAge);

		/** �������ַ����Ƚ� */
		Cond c_four;
		colNO = 5;
		c_four.valueOne.dataType = DataType::KV_COL;
		number2String(c_four.valueOne.dataValue, colNO);
		c_four.op = Op::LIKE;
		c_four.valueTwo.dataType = DataType::KV_VARCHAR;
		c_four.valueTwo.dataValue = "%ease";

		conds.push_back(c_one);
		conds.push_back(c_two);
		conds.push_back(c_three);
		conds.push_back(c_four);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency + 1);

		DriverUpdateMode modeTwo;
		modeTwo.attrNo = 3;
		modeTwo.mod = Mode::SET;
		double upBalance = 2345.0;
		number2String(modeTwo.value, upBalance);

		DriverUpdateMode modeThree;
		modeThree.attrNo = 4;
		modeThree.mod = Mode::DECR;
		number2String(modeThree.value, (s32)1);

		DriverUpdateMode modeFour;
		modeFour.attrNo = 5;
		modeFour.mod = Mode::PREPEND;
		modeFour.value = "163-";

		updatemodes.push_back(modeOne);
		updatemodes.push_back(modeTwo);
		updatemodes.push_back(modeThree);
		updatemodes.push_back(modeFour);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 1); /** ����һ�м�¼ */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(currency + 1, *(float*)retValue[1].c_str()));
	CPPUNIT_ASSERT(isEqual(2345.0, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(24 == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT("163-netease" == retValue[4]);
}

/**
*	KeyValueHandlerTestCase::update��ȷ������
*
*	����NULLSAFEEQ����������������һ
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy4()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 2;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::NULLSAFEEQ;
		c_one.valueTwo.dataType = DataType::KV_FLOAT;
		number2String(c_one.valueTwo.dataValue, currency);

		conds.push_back(c_one);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ������
*
*	�����ַ���like����������������
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy5()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		/** �������ַ����Ƚ� */
		Cond c_four;
		u16 colNO = 5;
		c_four.valueOne.dataType = DataType::KV_COL;
		number2String(c_four.valueOne.dataValue, colNO);
		c_four.op = Op::LIKE;
		c_four.valueTwo.dataType = DataType::KV_VARCHAR;
		c_four.valueTwo.dataValue = "%easee%";

		conds.push_back(c_four);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ������
*
*	������ֵ�Ƚϲ���������������
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy6()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		/** ��������������Ƚ�һ */
		Cond c_two;
		c_two.valueOne.dataType = DataType::KV_COL;
		number2String(c_two.valueOne.dataValue, 3);
		c_two.op = Op::GRATER;
		double comBalance = 1000.0;
		c_two.valueTwo.dataType = DataType::KV_DOUBLE;
		number2String(c_two.valueTwo.dataValue, comBalance);

		conds.push_back(c_two);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����0�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ������
*
*	����ISNULL����������������
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy7()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 2;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::ISNULL;

		conds.push_back(c_one);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ����8
*
*	����NULLSAFEEQ����(��ֵ�Ƚ�)������������2
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy8()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)
		->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 2;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::NULLSAFEEQ;

		currency = 3242.4f;
		c_one.valueTwo.dataType = DataType::KV_FLOAT;
		number2String(c_one.valueTwo.dataValue, currency);

		conds.push_back(c_one);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ����9
*
*	����NULLSAFEEQ����(�ַ����Ƚ�)������������3
*	�����Ƚ�������ͬʱΪNULL
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy9()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";
	string charName = "char";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)
		->writeVarchar(5, (byte*)address.c_str(), address.size())->setNull(10);
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;
	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 10;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::NULLSAFEEQ;
		c_one.valueTwo.dataType = DataType::KV_CHAR;
		c_one.valueTwo.dataValue = charName;

		conds.push_back(c_one);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 10;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ����10
*
*	����NULLSAFEEQ����(�ַ����Ƚ�)������������4
*	�����Ƚ���ͬʱ��ΪNULL�����ǲ����
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy10()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";
	string charName = "char";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)
		->writeVarchar(5, (byte*)address.c_str(), address.size())->writeChar(10, charName.c_str());
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 10;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::NULLSAFEEQ;

		charName = "3244";
		c_one.valueTwo.dataType = DataType::KV_CHAR;
		c_one.valueTwo.dataValue = charName;

		conds.push_back(c_one);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 10;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ����9
*
*	����NULLSAFEEQ����(�ڴ����Ƚ�)������������5
*	�����Ƚ�������ͬʱΪNULL
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy11()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";
	string binaryString = "012345678";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)
		->writeVarchar(5, (byte*)address.c_str(), address.size())->setNull(10);
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 11;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::NULLSAFEEQ;
		c_one.valueTwo.dataType = DataType::KV_BINARY;
		c_one.valueTwo.dataValue = binaryString;

		Cond c_two;
		c_two.valueOne.dataType = DataType::KV_COL;
		number2String(c_two.valueOne.dataValue, 10);
		c_two.op = Op::ISNULL;

		conds.push_back(c_two);
		conds.push_back(c_one);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 11;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
*	KeyValueHandlerTestCase::update��ȷ����12
*
*	����NULLSAFEEQ����(�ڴ����Ƚ�)������������6
*	�����Ƚ���ͬʱ��ΪNULL�����ǲ����
*/
void KeyValueHandlerTestCase::testUpdate_Accuracy12()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";
	string binaryString = "012345678";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)
		->writeNumber(3, balance)->writeNumber(4, age)
		->writeVarchar(5, (byte*)address.c_str(), address.size());
	RedRecord::writeRaw(m_table->getTableDef(), rec.getRecord()->m_data, 11, (byte*)binaryString.c_str(),
		binaryString.size());
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** �������� */
	vector<Cond> conds;
	{
		u16 colNO = 11;
		/** ���Ի���Ƚ� */
		Cond c_one;
		c_one.valueOne.dataType = DataType::KV_COL;
		number2String(c_one.valueOne.dataValue, colNO);
		c_one.op = Op::NULLSAFEEQ;

		binaryString = "034233454";
		c_one.valueTwo.dataType = DataType::KV_BINARY;
		c_one.valueTwo.dataValue = binaryString;

		conds.push_back(c_one);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 11;
		modeOne.mod = Mode::SET;
		number2String(modeOne.value, currency);

		updatemodes.push_back(modeOne);
	}

	::int8_t effectRows = m_instance->update(tableInfo, key, conds, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 0); /** ����һ�м�¼ */
}

/**
 *	KeyValueHandler::put_or_update�ľ�ȷ����һ
 *
 *	����¼������ʱ����ȡput����
 */
void KeyValueHandlerTestCase::testPutOrUpdate_Accuracy1()	{
	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeCurrency;
		modeCurrency.attrNo = 2;
		modeCurrency.mod = Mode::SETNULL;

		DriverUpdateMode modeBalance;
		modeBalance.attrNo = 3;
		modeBalance.mod = Mode::SET;
		number2String(modeBalance.value, balance);

		DriverUpdateMode modeAge;
		modeAge.attrNo = 4;
		number2String(modeAge.value, age);

		DriverUpdateMode modeAddress;
		modeAddress.attrNo = 5;
		modeAddress.value = address;

		updatemodes.push_back(modeCurrency);
		updatemodes.push_back(modeBalance);
		updatemodes.push_back(modeAge);
		updatemodes.push_back(modeAddress);
	}

	::int8_t effectRows = m_instance->put_or_update(tableInfo, key, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 1); /** ����һ�м�¼ */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(balance, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(age == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT(address == retValue[4]);
}

/**
*	KeyValueHandler::put_or_update�ľ�ȷ����һ
*
*	����¼����ʱ����ȡ������ģʽ���²���
*/
void KeyValueHandlerTestCase::testPutOrUpdate_Accuracy2()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);
	RedRecord rec(m_table->getTableDef());

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;	
	int age = 25;
	string address = "netease";

	rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())->writeNumber(2, currency)->writeNumber(3, balance)
		->writeNumber(4, age)->writeVarchar(5, (byte*)address.c_str(), address.size());;
	m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, id);

		Attr pName;
		pName.attrNo = 1;
		pName.value = name;

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	/** ������Ϊ */
	vector<DriverUpdateMode> updatemodes;
	{
		DriverUpdateMode modeOne;
		modeOne.attrNo = 2;
		modeOne.mod = Mode::SETNULL;

		DriverUpdateMode modeTwo;
		modeTwo.attrNo = 3;
		modeTwo.mod = Mode::SET;
		double upBalance = 2345.0;
		number2String(modeTwo.value, upBalance);

		DriverUpdateMode modeThree;
		modeThree.attrNo = 4;
		modeThree.mod = Mode::DECR;
		number2String(modeThree.value, (s32)1);

		DriverUpdateMode modeFour;
		modeFour.attrNo = 5;
		modeFour.mod = Mode::PREPEND;
		modeFour.value = "163-";

		updatemodes.push_back(modeOne);
		updatemodes.push_back(modeTwo);
		updatemodes.push_back(modeThree);
		updatemodes.push_back(modeFour);
	}

	::int8_t effectRows = m_instance->put_or_update(tableInfo, key, updatemodes, tableDef.version);
	CPPUNIT_ASSERT(effectRows == 1); /** ����һ�м�¼ */

	/**
	*	������ļ�¼��ȷ��
	*/
	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	vector<string> retValue;
	m_instance->get(retValue, tableInfo, key, attrs, tableDef.version);

	CPPUNIT_ASSERT(retValue.size() == 5);
	/** ����ֵλͼ */
	Bitmap bm((byte*)retValue[0].c_str(), retValue[0].size() * 8);
	CPPUNIT_ASSERT(bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
	CPPUNIT_ASSERT(isEqual(2345.0, *(double*)retValue[2].c_str()));
	CPPUNIT_ASSERT(24 == *(int*)retValue[3].c_str());
	CPPUNIT_ASSERT("163-netease" == retValue[4]);
}

/**
 *	KeyValueHandlerTestCase::multi_get��ȷ����һ
 *
 *	�����������keyʱ��Ӧ�ø��ݴ���key��index����һ�������
 *	�ò������������key������Ӧ�м�¼����
 */
void KeyValueHandlerTestCase::testMultiget_Accuracy1()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";

	s64 recordCount = 5;	/** �����¼����Ŀ */

	stringstream strStream;
	for (; id <= recordCount; ++id) {
		RedRecord rec(m_table->getTableDef());

		strStream.clear();
		strStream<<id;
		strStream>>name;
		rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())
			->writeNumber(3, balance + (double)id)->writeNumber(4, age + (int)id)
			->writeVarchar(5, (byte*)address.c_str(), address.size());
		if (0 == id % 2) {
			rec.writeNumber(2, currency + (float)id);
		}
		m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);
	}

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	vector<s64> keyIndices;

	/** IndexKeys */
	vector<Attrs> keys;
	{
		/**
		 *	�������е�key
		 */
		for (id = 1; id <= (recordCount - id) + 1; ++id) {
			Attrs key;
			{
				Attr pId;
				pId.attrNo = 0;
				number2String(pId.value, id);

				Attr pName;
				strStream.clear();
				strStream<<id;
				strStream>>name;
				pName.attrNo = 1;
				pName.value = name;

				key.attrlist.push_back(pId);
				key.attrlist.push_back(pName);

				keyIndices.push_back(id);
			}
			keys.push_back(key);
			if (id < (recordCount - id + 1)) {
				Attrs anotherKey;
				{
					Attr pId;
					pId.attrNo = 0;
					number2String(pId.value, recordCount - id + 1);

					Attr pName;
					strStream.clear();
					strStream<<(recordCount - id + 1);
					strStream>>name;
					pName.attrNo = 1;
					pName.value = name;

					anotherKey.attrlist.push_back(pId);
					anotherKey.attrlist.push_back(pName);

					keyIndices.push_back(recordCount - id + 1);
				}
				keys.push_back(anotherKey);
			}
		}
	}

	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	map<::int16_t, vector<string>> result;
	m_instance->multi_get(result, tableInfo, keys, attrs, tableDef.version);

	CPPUNIT_ASSERT(result.size() == recordCount);

	/**
	 *	��֤��������ص�map��key��Ӧ���������������кţ�value��Ӧ���
	 */
	typedef map<::int16_t, vector<string>>::iterator resItor;
	for (resItor it = result.begin(); it != result.end(); ++it) {
		id = keyIndices[it->first];

		vector<string> currentResult = it->second;

		CPPUNIT_ASSERT(currentResult.size() == attrs.size() + 1);

		/** ����ֵλͼ */
		Bitmap bm((byte*)currentResult[0].c_str(), currentResult[0].size() * 8);
		if (0 == id % 2) {
			CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
			float value = *(float*)currentResult[1].c_str();
			CPPUNIT_ASSERT(isEqual(currency + (float)id, *(float*)currentResult[1].c_str()));
		} else {
			CPPUNIT_ASSERT(bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
		}

		CPPUNIT_ASSERT(isEqual(balance + (double)id, *(double*)currentResult[2].c_str()));
		CPPUNIT_ASSERT(age + id == *(int*)currentResult[3].c_str());
		CPPUNIT_ASSERT(address == currentResult[4]);
	}
}

/**
*	KeyValueHandlerTestCase::multi_get��ȷ����һ
*
*	�����������keyʱ��Ӧ�ø��ݴ���key��index����һ�������
*	�ò������������key����Ӧ�ļ�¼����һ������
*/
void KeyValueHandlerTestCase::testMultiget_Accuracy2()	{
	/** ���ݼ�¼��׼�� */
	Connection *conn = m_db->getConnection(false, __FUNC__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNC__, conn);

	s64 id = 1;
	string name = "liaodingbai";
	float currency = 2.3f;
	double balance = 234.3;
	int age = 25;
	string address = "netease";

	s64 recordCount = 5;	/** �����¼����Ŀ */

	stringstream strStream;
	for (; id <= recordCount; ++id) {
		RedRecord rec(m_table->getTableDef());

		strStream.clear();
		strStream<<id;
		strStream>>name;
		rec.writeNumber(0, id)->writeVarchar(1, (byte*)name.c_str(), name.size())
			->writeNumber(3, balance + (double)id)->writeNumber(4, age + (int)id)
			->writeVarchar(5, (byte*)address.c_str(), address.size());
		if (0 == id % 2) {
			rec.writeNumber(2, currency + (float)id);
		}
		m_table->getTable()->insert(session, rec.getRecord()->m_data, NULL);
	}

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	KVTableDef tableDef;
	m_instance->getTableDef(tableDef, tableInfo);

	vector<s64> keyIndices;

	/** IndexKeys */
	vector<Attrs> keys;
	{
		/**
		*	�������е�key
		*/
		for (id = 1; id <= (recordCount * 2 - id) + 1; ++id) {
			Attrs key;
			{
				Attr pId;
				pId.attrNo = 0;
				number2String(pId.value, id);

				Attr pName;
				strStream.clear();
				strStream<<id;
				strStream>>name;
				pName.attrNo = 1;
				pName.value = name;

				key.attrlist.push_back(pId);
				key.attrlist.push_back(pName);

				keyIndices.push_back(id);
			}
			keys.push_back(key);
			if (id < (recordCount * 2 - id + 1)) {
				Attrs anotherKey;
				{
					Attr pId;
					pId.attrNo = 0;
					number2String(pId.value, recordCount * 2 - id + 1);

					Attr pName;
					strStream.clear();
					strStream<<(recordCount * 2 - id + 1);
					strStream>>name;
					pName.attrNo = 1;
					pName.value = name;

					anotherKey.attrlist.push_back(pId);
					anotherKey.attrlist.push_back(pName);

					keyIndices.push_back(recordCount * 2 - id + 1);
				}
				keys.push_back(anotherKey);
			}
		}
	}

	vector<::int16_t> attrs;
	attrs.push_back(2);
	attrs.push_back(3);
	attrs.push_back(4);
	attrs.push_back(5);

	map<::int16_t, vector<string>> result;
	m_instance->multi_get(result, tableInfo, keys, attrs, tableDef.version);

	CPPUNIT_ASSERT(result.size() == 5);

	/**
	*	��֤��������ص�map��key��Ӧ���������������кţ�value��Ӧ���
	*/
	typedef map<::int16_t, vector<string>>::iterator resItor;
	for (resItor it = result.begin(); it != result.end(); ++it) {
		id = keyIndices[it->first];

		vector<string> currentResult = it->second;

		CPPUNIT_ASSERT(currentResult.size() == attrs.size() + 1);

		/** ����ֵλͼ */
		Bitmap bm((byte*)currentResult[0].c_str(), currentResult[0].size() * 8);
		if (0 == id % 2) {
			CPPUNIT_ASSERT(!bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
			float value = *(float*)currentResult[1].c_str();
			CPPUNIT_ASSERT(isEqual(currency + (float)id, *(float*)currentResult[1].c_str()));
		} else {
			CPPUNIT_ASSERT(bm.isSet(0) && !bm.isSet(1) && !bm.isSet(2) && !bm.isSet(3));
		}

		CPPUNIT_ASSERT(isEqual(balance + (double)id, *(double*)currentResult[2].c_str()));
		CPPUNIT_ASSERT(age + id == *(int*)currentResult[3].c_str());
		CPPUNIT_ASSERT(address == currentResult[4]);
	}
}

/**
 *	KeyValueHandlerTestCase::getTableDef�ľ�ȷ����
 *
 *	��֤���ر���Ϣ����ȷ(�У�������������)
 */
void KeyValueHandlerTestCase::testGetTableDef()	{
	KVTableDef result;

	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	m_instance->getTableDef(result, tableInfo);

	CPPUNIT_ASSERT(result.m_name=="KeyValueTest");
	CPPUNIT_ASSERT(result.m_schemaName==baseDir);
	CPPUNIT_ASSERT(result.m_bmBytes = 1);

	CPPUNIT_ASSERT(result.m_pkey.m_name == "PRIMARY");
	CPPUNIT_ASSERT(result.m_pkey.m_columns.size()==2);
	CPPUNIT_ASSERT(result.m_pkey.m_columns[0]==0);
	CPPUNIT_ASSERT(result.m_pkey.m_columns[1]==1);

	/**
	 *	�Ƚϱ����е���Ϣ
	 */
	CPPUNIT_ASSERT(result.m_columns.size() == 12);
	/** ��һ */
	CPPUNIT_ASSERT(result.m_columns[0].m_name == "id");
	CPPUNIT_ASSERT(result.m_columns[0].m_no == 0);
	CPPUNIT_ASSERT(!result.m_columns[0].m_nullable);
	CPPUNIT_ASSERT(result.m_columns[0].m_type==DataType::KV_BIGINT);

	/** �ж� */
	CPPUNIT_ASSERT(result.m_columns[1].m_name == "name");
	CPPUNIT_ASSERT(result.m_columns[1].m_no == 1);
	CPPUNIT_ASSERT(!result.m_columns[1].m_nullable);
	CPPUNIT_ASSERT(result.m_columns[1].m_type==DataType::KV_VARCHAR);

	/** ���� */
	CPPUNIT_ASSERT(result.m_columns[2].m_name == "currency");
	CPPUNIT_ASSERT(result.m_columns[2].m_no == 2);
	CPPUNIT_ASSERT(result.m_columns[2].m_nullable);
	CPPUNIT_ASSERT(result.m_columns[2].m_type==DataType::KV_FLOAT);

	/** ���� */
	CPPUNIT_ASSERT(result.m_columns[3].m_name == "balance");
	CPPUNIT_ASSERT(result.m_columns[3].m_no == 3);
	CPPUNIT_ASSERT(!result.m_columns[3].m_nullable);
	CPPUNIT_ASSERT(result.m_columns[3].m_type==DataType::KV_DOUBLE);

	/** ���� */
	CPPUNIT_ASSERT(result.m_columns[4].m_name == "age");
	CPPUNIT_ASSERT(result.m_columns[4].m_no == 4);
	CPPUNIT_ASSERT(!result.m_columns[4].m_nullable);
	CPPUNIT_ASSERT(result.m_columns[4].m_type==DataType::KV_INT);

	/** ���� */
	CPPUNIT_ASSERT(result.m_columns[5].m_name == "address");
	CPPUNIT_ASSERT(result.m_columns[5].m_no == 5);
	CPPUNIT_ASSERT(!result.m_columns[5].m_nullable);
	CPPUNIT_ASSERT(result.m_columns[5].m_type==DataType::KV_VARCHAR);
}

/**
 *	KeyValueHandler::GetTableDef��Failure��������
 *	��ȡ�����ڵı�ʧ��
 */
void KeyValueHandlerTestCase::testGetTableDef_Failed()	{
	KVTableInfo tableInfo;
	tableInfo.m_name = "NotExist";
	tableInfo.m_schemaName = baseDir;

	KVTableDef result;
	try {
		m_instance->getTableDef(result, tableInfo);
		CPPUNIT_FAIL("Open the table that not exist should be failed!!!");
	} catch (ServerException &e) {
		CPPUNIT_ASSERT(e.errcode== KVErrorCode::KV_EC_FILE_NOT_EXIST);
	}
}

/**
 *	���ͻ��˱�汾������ʧ��ʱ���׳��쳣��ErrCodeΪKV_EC_TBLDEF_NOT_MATCH
 */
void KeyValueHandlerTestCase::testVersionMissMatched()	{
	KVTableInfo tableInfo;
	tableInfo.m_name = "KeyValueTest";
	tableInfo.m_schemaName = baseDir;

	/** Index Key */
	Attrs key;
	{
		Attr pId;
		pId.attrNo = 0;
		number2String(pId.value, 12);

		Attr pName;
		pName.attrNo = 1;
		pName.value = "not-exist";

		key.attrlist.push_back(pId);
		key.attrlist.push_back(pName);
	}

	try {
		::int8_t effectRows = m_instance->remove(tableInfo, key, 1);
		CPPUNIT_FAIL("Open the table that not exist should be failed!!!");
	} catch (ServerException &e) {
		CPPUNIT_ASSERT(e.errcode== KVErrorCode::KV_EC_TBLDEF_NOT_MATCH);
	}
}

#endif