#include "misc/TestRecord.h"

using namespace std;
using namespace ntse;

const char* RecordTestCase::getName() {
	return "Record operations test";
}

const char* RecordTestCase::getDescription() {
	return "Test record operations";
}

bool RecordTestCase::isBig() {
	return false;
}

/**
 * extractKeyRC
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录没有NULL列, 压缩格式Key的列顺序不同于记录列顺序
 *	case 3. 记录中包含NULL列，Key包含NULL列
 *	case 4. 记录中包含NULL列，Key不包含NULL列
 */
void RecordTestCase::testExtractKeyRC() {
	const char* sname = "beibei";
	const int sno = 1;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder compBuilder(tableDef, KEY_COMPRESS);
	{ // 记录没有NULL列
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[5];
		Record* record = studentTable.createRecord(REC_REDUNDANT, sname, sno, 16, "M", 1, 4.0f);
		SubRecord* key = compBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_SNO);
		RecordOper::extractKeyRC(tableDef, indexDef, record, NULL, key);
		SubRecord* trueKey = compBuilder.createSubRecordByName(STU_NAME" "STU_SNO, sname, &sno);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 记录没有NULL列, 子记录的列顺序颠倒
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[6];
		Record* record = studentTable.createRecord(REC_REDUNDANT, sname, sno, 16, "M", 1, 4.0f);
		SubRecord* key = compBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_NAME);
		RecordOper::extractKeyRC(tableDef, indexDef, record, NULL, key);
		SubRecord* trueKey = compBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &sno, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 记录中有NULL列
		RecordBuilder rb(tableDef, 0, REC_REDUNDANT);
		rb.appendVarchar(sname);
		rb.appendNull();
		rb.appendSmallInt(16);
		rb.appendChar("M");
		rb.appendNull();
		rb.appendFloat(4.0f);
		rb.appendBigInt(0);
		Record* record = rb.getRecord();
		// 1. 提取NULL列
		{
			const IndexDef* indexDef = studentTable.getTableDef()->m_indice[14];
			SubRecord* key = compBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO);
			RecordOper::extractKeyRC(tableDef, indexDef, record, NULL, key);
			SubRecord* trueKey = compBuilder.createSubRecordByName(STU_SNO, (int*)0);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		// 2. 提取一个NULL列和一个非NULL列
		{
			const IndexDef* indexDef = studentTable.getTableDef()->m_indice[5];
			SubRecord* key = compBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_SNO);
			RecordOper::extractKeyRC(tableDef, indexDef, record, NULL, key);
			SubRecord* trueKey = compBuilder.createSubRecordByName(STU_NAME" "STU_SNO, sname, (int*)0);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		freeRecord(record);

	}

	// 测试大对象列及前缀索引及超长字段
	const u64 id = 1;
	const char *name = "blog name";
	const char *author = "lostbag";
	const char *abs = "blog is exist";
	const char *content = "blog is not exist";
	const u64 pubtime = 5;
	UserBlogTable blogTable;
	tableDef = blogTable.getTableDef();
	const IndexDef* indexDef = blogTable.getTableDef()->m_indice[0];
	KeyBuilder compBuilder1(tableDef, indexDef, KEY_COMPRESS);
	{ // 记录没有NULL列
		Record* record = blogTable.createRecord(0,REC_REDUNDANT, id, name, author, abs, content, pubtime);
		SubRecord* key = compBuilder1.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		Array<LobPair *>lobArray;
		lobArray.push(new LobPair((byte *)abs, strlen(abs)));
		lobArray.push(new LobPair((byte *)content, strlen(content)));
		RecordOper::extractKeyRC(tableDef, indexDef, record, &lobArray, key);
		SubRecord* trueKey = compBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, name, abs, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		for (size_t i = 0; i <  lobArray.getSize(); i++) 
			delete lobArray[i];

		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 记录中有NULL列
		Record* record = blogTable.createRecord(0,REC_REDUNDANT, id, NULL, author, NULL, content, pubtime);
		SubRecord* key = compBuilder1.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		Array<LobPair *>lobArray;
		lobArray.push(new LobPair((byte *)abs, strlen(abs)));
		lobArray.push(new LobPair((byte *)content, strlen(content)));
		RecordOper::extractKeyRC(tableDef, indexDef, record, &lobArray, key);
		SubRecord* trueKey = compBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, NULL, NULL, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		for (size_t i = 0; i <  lobArray.getSize(); i++) 
			delete lobArray[i];
		freeSubRecord(trueKey);
		freeSubRecord(key);
		freeRecord(record);
	}

}
/**
 * convertKeyPC
 * 测试流程：
 *	case 1. key没有NULL列
 *	case 2. key没有NULL列, 压缩格式Key的列顺序不同于冗余格式Key
 *	case 3. key中包含NULL列
 *	case 4. key的列全NULL
 */
void RecordTestCase::testConvertKeyPC() {
	const char* sname = "feilong";
	const int age = 100;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	SubRecordBuilder pb(tableDef, KEY_PAD);
	SubRecordBuilder cb(tableDef, KEY_COMPRESS);
	{
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* padKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyPC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // 颠倒列顺序
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[2];
		SubRecord* padKey = pb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_AGE" "STU_NAME);
		RecordOper::convertKeyPC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* padKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyPC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL ALL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[3];
		SubRecord* padKey = pb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_AGE);
		RecordOper::convertKeyPC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
}
/**
 * convertKeyPN
 * 测试流程：
 *	case 1. key没有NULL列
 *	case 2. key没有NULL列, 输入key和输出key的列顺序不同
 *	case 3. key中包含NULL列
 *	case 4. key的列全NULL
 */
void RecordTestCase::testConvertKeyPN() {
	const char* sname = "Orange";
	const int age = -128;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	SubRecordBuilder pb(tableDef, KEY_PAD);
	SubRecordBuilder nb(tableDef, KEY_NATURAL);
	{
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* padKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		SubRecord* natKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyPN(tableDef, indexDef, padKey, natKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, natKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(natKey);
		freeSubRecord(trueKey);
	}
	{ // 颠倒列顺序
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[2];
		SubRecord* padKey = pb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		SubRecord* natKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_AGE" "STU_NAME);
		RecordOper::convertKeyPN(tableDef, indexDef, padKey, natKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,natKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(natKey);
		freeSubRecord(trueKey);
	}
	{ // NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* padKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		SubRecord* natKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyPN(tableDef, indexDef, padKey, natKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,natKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(natKey);
		freeSubRecord(trueKey);
	}
	{ // NULL ALL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[3];
		SubRecord* padKey = pb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* natKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_AGE);
		RecordOper::convertKeyPN(tableDef, indexDef, padKey, natKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,natKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(natKey);
		freeSubRecord(trueKey);
	}
}
/**
 * convertKeyCP
 * 测试流程：
 *	case 1. key没有NULL列
 *	case 2. key没有NULL列, 输入key和输出key的列顺序不同
 *	case 3. key中包含NULL列
 *	case 4. key的列全NULL
 */
void RecordTestCase::testConvertKeyCP() {
	const char* sname = "fengshao";
	const int age = 28;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	const IndexDef* indexDef = studentTable.getTableDef()->m_indice[0];
	SubRecordBuilder pb(tableDef, KEY_PAD);
	SubRecordBuilder cb(tableDef, KEY_COMPRESS);
	{
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* comKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyCP(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // 颠倒列顺序
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[2];
		SubRecord* comKey = cb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_AGE" "STU_NAME);
		RecordOper::convertKeyCP(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* comKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyCP(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL ALL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[3];
		SubRecord* comKey = cb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_AGE);
		RecordOper::convertKeyCP(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
}

/**
 * convertKeyNC
 * 测试流程：
 *	case 1. key没有NULL列
 *	case 2. key没有NULL列, 输入key和输出key的列顺序不同
 *	case 3. key中包含NULL列
 *	case 4. key的列全NULL
 */
void RecordTestCase::testConvertKeyNC() {
	const char* sname = "feilong";
	const int age = 100;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();

	SubRecordBuilder nb(tableDef, KEY_NATURAL);
	SubRecordBuilder cb(tableDef, KEY_COMPRESS);
	{
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* padKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyNC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // 颠倒列顺序
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[2];
		SubRecord* padKey = nb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_AGE" "STU_NAME);
		RecordOper::convertKeyNC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* padKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyNC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL ALL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[3];
		SubRecord* padKey = nb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* comKey = cb.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_AGE);
		RecordOper::convertKeyNC(tableDef, indexDef, padKey, comKey);
		SubRecord* trueKey = cb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, comKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
}

/**
 * convertKeyCN
 * 测试流程：
 *	case 1. key没有NULL列
 *	case 2. key没有NULL列, 输入key和输出key的列顺序不同
 *	case 3. key中包含NULL列
 *	case 4. key的列全NULL
 */
void RecordTestCase::testConvertKeyCN() {
	const char* sname = "fengshao";
	const int age = 28;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	const IndexDef* indexDef = studentTable.getTableDef()->m_indice[0];
	SubRecordBuilder nb(tableDef, KEY_NATURAL);
	SubRecordBuilder cb(tableDef, KEY_COMPRESS);
	{
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* comKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		SubRecord* padKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyCN(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // 颠倒列顺序
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[2];
		SubRecord* comKey = cb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		SubRecord* padKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_AGE" "STU_NAME);
		RecordOper::convertKeyCN(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* comKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		SubRecord* padKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyCN(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
	{ // NULL ALL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[3];
		SubRecord* comKey = cb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* padKey = nb.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_AGE);
		RecordOper::convertKeyCN(tableDef, indexDef, comKey, padKey);
		SubRecord* trueKey = nb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(comKey);
		freeSubRecord(trueKey);
	}
}
/**
 * convertKeyMP
 * 测试流程：
 *	case 1. key没有NULL列
 *	case 2. key没有NULL列, 输入key和输出key的列顺序不同
 *	case 3. key中包含NULL列
 *	case 4. key的列全NULL
 */
void RecordTestCase::testConvertKeyMP() {
	const char* sname = "Jim Starkey";
	const int age = 48;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	SubRecordBuilder pb(tableDef, KEY_PAD);
	SubRecordBuilder mb(tableDef, KEY_MYSQL);
	{
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* mysqlKey = mb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyMP(tableDef, indexDef, mysqlKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(mysqlKey);
		freeSubRecord(trueKey);
	}
	{ // 颠倒列顺序
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[2];
		SubRecord* mysqlKey = mb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_AGE" "STU_NAME);
		RecordOper::convertKeyMP(tableDef, indexDef, mysqlKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(mysqlKey);
		freeSubRecord(trueKey);
	}
	{ // 第二列为NULL
		TableDefBuilder tb(0, "test", "test");
		tb.addColumnS(STU_NAME, CT_VARCHAR, 12);
		tb.addColumn(STU_AGE, CT_INT);
		tb.addIndex("name_age_index", false, false, false, STU_NAME, 0, STU_AGE, 0, NULL);
		TableDef *tableDef = tb.getTableDef();
		const IndexDef* indexDef = tableDef->m_indice[0];
		SubRecordBuilder pb(tableDef, KEY_PAD);
		SubRecordBuilder mb(tableDef, KEY_MYSQL);
		SubRecord* mysqlKey = mb.createSubRecordByName(STU_NAME" "STU_AGE, NULL, &age);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyMP(tableDef, indexDef, mysqlKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, NULL, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(mysqlKey);
		freeSubRecord(trueKey);
		delete tableDef;
	}
	{ // 第一列为NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[1];
		SubRecord* mysqlKey = mb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertKeyMP(tableDef, indexDef, mysqlKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(mysqlKey);
		freeSubRecord(trueKey);
	}
	{ // NULL ALL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[3];
		SubRecord* mysqlKey = mb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* padKey = pb.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_AGE);
		RecordOper::convertKeyMP(tableDef, indexDef, mysqlKey, padKey);
		SubRecord* trueKey = pb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(mysqlKey);
		freeSubRecord(trueKey);
	}

	// 测试前缀索引和超长字段
	// 测试大对象列及前缀索引及超长字段
	const u64 id = 1;
	const char *name = "blog name";
	const char *author = "lostbag";
	const char *abs = "blog is exist";
	const char *content = "blog is not exist";
	const u64 pubtime = 5;
	UserBlogTable blogTable;
	tableDef = blogTable.getTableDef();
	const IndexDef* indexDef = blogTable.getTableDef()->m_indice[0];
	KeyBuilder padBuilder(tableDef, indexDef, KEY_PAD);
	KeyBuilder mysqlBuilder(tableDef, indexDef, KEY_MYSQL);
	{ // 记录没有NULL列
		SubRecord *mysqlKey = mysqlBuilder.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, name, abs, content);
		SubRecord *padKey = padBuilder.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		RecordOper::convertKeyMP(tableDef, indexDef, mysqlKey, padKey);
		SubRecord *trueKey = padBuilder.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, name, abs, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(mysqlKey);
		freeSubRecord(trueKey);  
	}
	{ // 第二列为NULL
		SubRecord* mysqlKey = mysqlBuilder.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, NULL, NULL, content);
		SubRecord* padKey = padBuilder.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		RecordOper::convertKeyMP(tableDef, indexDef, mysqlKey, padKey);
		SubRecord* trueKey = padBuilder.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, NULL, NULL, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,padKey, trueKey, indexDef));
		freeSubRecord(padKey);
		freeSubRecord(mysqlKey);
		freeSubRecord(trueKey);
	}
}

typedef void (*ExtractKeyNaturalFunc)(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);
/**
 * 测试自然格式key提取
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录没有NULL列, Key的列顺序不同于记录列顺序
 *	case 3. 记录中包含NULL列，Key包含NULL列
 *	case 4. 记录中包含NULL列，Key的列都是NULL
 */
void testExtractKeyNatural(RecFormat format) {
	ExtractKeyNaturalFunc doExact = (format == REC_REDUNDANT) ? RecordOper::extractKeyRN : RecordOper::extractKeyVN;
	const char *sname = "Spy007";
	const int sno = 100;
	const short age = 18;

	StudentTable studentTable;
	const TableDef *tableDef = studentTable.getTableDef();
	
	SubRecordBuilder naturalBuilder(tableDef, KEY_NATURAL);
	{ // 无NULL列
		Record *record = studentTable.createRecord(format, sname, sno, age, "F", 1, 4.0f);
		{
			const IndexDef *indexDef = studentTable.getTableDef()->m_indice[5];
			SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , STU_NAME" "STU_SNO);
			doExact(tableDef, indexDef, record, NULL, key);
			SubRecord *trueKey = naturalBuilder.createSubRecordByName(STU_NAME" "STU_SNO, sname, &sno);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		{ // 子记录的列顺序颠倒
			const IndexDef *indexDef = studentTable.getTableDef()->m_indice[6];
			SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , STU_SNO" "STU_NAME);
			doExact(tableDef, indexDef, record, NULL, key);
			SubRecord *trueKey = naturalBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &sno, sname);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		freeRecord(record);
	}
	{ // 提取一个NULL列
		RecordBuilder rb(tableDef, 0, format);
		rb.appendVarchar(sname);
		rb.appendNull();
		rb.appendSmallInt(age);
		rb.appendChar("M");
		rb.appendNull();
		rb.appendNull();
		rb.appendBigInt(0);
		Record *record = rb.getRecord();
		const IndexDef *indexDef = studentTable.getTableDef()->m_indice[5];
		SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , STU_NAME" "STU_SNO);
		doExact(tableDef, indexDef, record, NULL, key);
		SubRecord *trueKey = naturalBuilder.createSubRecordByName(STU_NAME" "STU_SNO, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
		freeSubRecord(trueKey);
		freeSubRecord(key);
		freeRecord(record);
	}
	{ // 提取的列都是NULL
		RecordBuilder rb(tableDef, 0, format);
		rb.appendVarchar(sname);
		rb.appendNull();
		rb.appendNull();
		rb.appendChar("M");
		rb.appendNull();
		rb.appendNull();
		rb.appendBigInt(0);
		Record *record = rb.getRecord();
		const IndexDef *indexDef = studentTable.getTableDef()->m_indice[4];
		SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , STU_AGE" "STU_SNO);
		doExact(tableDef, indexDef, record, NULL, key);
		SubRecord *trueKey = naturalBuilder.createSubRecordByName(STU_AGE" "STU_SNO, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
		freeSubRecord(trueKey);
		freeSubRecord(key);
		freeRecord(record);
	}
	// 测试大对象列及前缀索引及超长字段
	const u64 id = 1;
	const char *name = "blog name";
	const char *author = "lostbag";
	const char *abs = "blog is exist";
	const char *content = "blog is not exist";
	const u64 pubtime = 5;
	UserBlogTable blogTable;
	tableDef = blogTable.getTableDef();
	const IndexDef* indexDef = blogTable.getTableDef()->m_indice[0];
	KeyBuilder naturalBuilder1(tableDef, indexDef, KEY_NATURAL);
	{ // 记录没有NULL列
		Record* record = blogTable.createRecord(0, format, id, name, author, abs, content, pubtime);
		SubRecord* key = naturalBuilder1.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		Array<LobPair *>lobArray;
		lobArray.push(new LobPair((byte *)abs, strlen(abs)));
		lobArray.push(new LobPair((byte *)content, strlen(content)));
		doExact(tableDef, indexDef, record, &lobArray, key);
		SubRecord* trueKey = naturalBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, name, abs, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		for (size_t i = 0; i <  lobArray.getSize(); i++) 
			delete lobArray[i];

		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 记录中有NULL列
		Record* record = blogTable.createRecord(0, format, id, NULL, author, NULL, content, pubtime);
		SubRecord* key = naturalBuilder1.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		Array<LobPair *>lobArray;
		lobArray.push(new LobPair((byte *)abs, strlen(abs)));
		lobArray.push(new LobPair((byte *)content, strlen(content)));
		doExact(tableDef, indexDef, record, &lobArray, key);
		SubRecord* trueKey = naturalBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, NULL, NULL, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		for (size_t i = 0; i <  lobArray.getSize(); i++) 
			delete lobArray[i];
		freeSubRecord(trueKey);
		freeSubRecord(key);
		freeRecord(record);
	}
}
/** 参考testExtractKeyNatural */
void RecordTestCase::testExtractKeyRN() {
	return testExtractKeyNatural(REC_REDUNDANT);
}
/** 参考testExtractKeyNatural */
void RecordTestCase::testExtractKeyVN() {
	return testExtractKeyNatural(REC_VARLEN);
}
/**
 * ExtractKeyFN
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录没有NULL列, Key的列顺序不同于记录列顺序
 *	case 3. 记录中包含NULL列，Key包含NULL列
 *	case 4. 记录中包含NULL列，Key的列都是NULL
 */
void RecordTestCase::testExtractKeyFN() {
	const char *title = "hacker";
	const char *isbn = "0000001";
	const int pages = 338;
	const int price = 129;

	BookTable bookTable;
	const TableDef* tableDef = bookTable.getTableDef();
	
	SubRecordBuilder naturalBuilder(tableDef, KEY_NATURAL);
	{ // 无NULL列
		Record *record = bookTable.createRecord(title, isbn, pages, price, REC_FIXLEN);
		{
			const IndexDef* indexDef = bookTable.getTableDef()->m_indice[1];
			SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , BOOK_TITLE" "BOOK_ISBN);
			RecordOper::extractKeyFN(tableDef, indexDef, record, key);
			SubRecord *trueKey = naturalBuilder.createSubRecordByName(BOOK_TITLE" "BOOK_ISBN, title, isbn);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		{ // 子记录的列顺序颠倒
			const IndexDef* indexDef = bookTable.getTableDef()->m_indice[2];
			SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , BOOK_ISBN" "BOOK_TITLE);
			RecordOper::extractKeyFN(tableDef, indexDef, record, key);
			SubRecord *trueKey = naturalBuilder.createSubRecordByName(BOOK_ISBN" "BOOK_TITLE, isbn, title);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		freeRecord(record);
	}
	{ // 提取一个NULL列
		RecordBuilder rb(tableDef, 0, REC_FIXLEN);
		rb.appendChar(title);
		rb.appendNull();
		rb.appendInt(pages);
		rb.appendInt(price);
		Record *record = rb.getRecord();
		const IndexDef* indexDef = bookTable.getTableDef()->m_indice[1];
		SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , BOOK_TITLE" "BOOK_ISBN);
		RecordOper::extractKeyFN(tableDef, indexDef, record, key);
		SubRecord *trueKey = naturalBuilder.createSubRecordByName(BOOK_TITLE" "BOOK_ISBN, title, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
		freeSubRecord(trueKey);
		freeSubRecord(key);
		freeRecord(record);
	}
	{ // 提取的列都是NULL
		RecordBuilder rb(tableDef, 0, REC_FIXLEN);
		rb.appendChar(title);
		rb.appendNull();
		rb.appendNull();
		rb.appendInt(price);
		Record *record = rb.getRecord();
		const IndexDef* indexDef = bookTable.getTableDef()->m_indice[3];
		SubRecord *key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , BOOK_PAGES" "BOOK_ISBN);
		RecordOper::extractKeyFN(tableDef, indexDef, record, key);
		SubRecord *trueKey = naturalBuilder.createSubRecordByName(BOOK_PAGES" "BOOK_ISBN, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
		freeSubRecord(trueKey);
		freeSubRecord(key);
		freeRecord(record);
	}
}
/**
 * ExtractKeyRP
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录没有NULL列, Key的列顺序不同于记录列顺序
 */
void RecordTestCase::testExtractKeyRP() {
	const char* sname = "jingjing";
	const int sno = 100;

	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	Record* record = studentTable.createRecord(REC_REDUNDANT, sname, sno, 18, "F", 1, 4.0f);
	SubRecordBuilder naturalBuilder(tableDef, KEY_PAD);
	{
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[5];
		SubRecord* key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , STU_NAME" "STU_SNO);
		RecordOper::extractKeyRP(tableDef, indexDef, record, NULL, key);
		SubRecord* trueKey = naturalBuilder.createSubRecordByName(STU_NAME" "STU_SNO, sname, &sno);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 子记录的列顺序颠倒
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[6];
		SubRecord* key = naturalBuilder.createEmptySbByName(tableDef->m_maxRecSize , STU_SNO" "STU_NAME);
		RecordOper::extractKeyRP(tableDef, indexDef, record, NULL, key);
		SubRecord* trueKey = naturalBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &sno, sname);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey, indexDef));
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	freeRecord(record);

	// 测试大对象列及前缀索引及超长字段
	const u64 id = 1;
	const char *name = "blog name";
	const char *author = "lostbag";
	const char *abs = "blog is exist";
	const char *content = "blog is not exist";
	const u64 pubtime = 5;
	UserBlogTable blogTable;
	tableDef = blogTable.getTableDef();
	const IndexDef* indexDef = blogTable.getTableDef()->m_indice[0];
	KeyBuilder padBuilder1(tableDef, indexDef, KEY_PAD);
	{ // 记录没有NULL列
		Record* record = blogTable.createRecord(0, REC_REDUNDANT, id, name, author, abs, content, pubtime);
		SubRecord* key = padBuilder1.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		Array<LobPair *>lobArray;
		lobArray.push(new LobPair((byte *)abs, strlen(abs)));
		lobArray.push(new LobPair((byte *)content, strlen(content)));
		RecordOper::extractKeyRP(tableDef, indexDef, record, &lobArray, key);
		SubRecord* trueKey = padBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, name, abs, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		for (size_t i = 0; i <  lobArray.getSize(); i++) 
			delete lobArray[i];

		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 记录中有NULL列
		Record* record = blogTable.createRecord(0, REC_REDUNDANT, id, NULL, author, NULL, content, pubtime);
		SubRecord* key = padBuilder1.createEmptyKeyByName(indexDef->m_maxKeySize, BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT);
		Array<LobPair *>lobArray;
		lobArray.push(new LobPair((byte *)abs, strlen(abs)));
		lobArray.push(new LobPair((byte *)content, strlen(content)));
		RecordOper::extractKeyRP(tableDef, indexDef, record, &lobArray, key);
		SubRecord* trueKey = padBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME" "BLOG_ABS" "BLOG_CONTENT, author, NULL, NULL, content);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, key, trueKey, indexDef));

		for (size_t i = 0; i <  lobArray.getSize(); i++) 
			delete lobArray[i];
		freeSubRecord(trueKey);
		freeSubRecord(key);
		freeRecord(record);
	}
}

void doTestExtractSubRecordFR(void (*extractor)(const TableDef *, const Record *, SubRecord *)) {
	const char* title = "linux";
	const int pages = 338;

	BookTable bookTable;
	const TableDef* tableDef = bookTable.getTableDef();
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	{ // 没有NULL列
		Record* record = bookTable.createRecord(title, "007-008", pages, 98, REC_FIXLEN);
		SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, BOOK_TITLE" "BOOK_PAGES);
		extractor(tableDef, record, key);
		SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
		SubRecord* trueKey = sbb.createSubRecordByName(BOOK_TITLE" "BOOK_PAGES, title, &pages);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));

		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 有NULL列
		RecordBuilder rb(tableDef, 0, REC_FIXLEN);
		rb.appendChar(title)->appendNull()->appendInt(pages)->appendInt(98);
		Record* record = rb.getRecord();
		{ // 1. 提取非NULL列
			SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, BOOK_TITLE" "BOOK_PAGES);
			extractor(tableDef, record, key);
			SubRecord* trueKey = redundantBuilder.createSubRecordByName(BOOK_TITLE" "BOOK_PAGES, title, &pages);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		{ // 2. 提取NULL列
			SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, BOOK_ISBN" "BOOK_PAGES);
			extractor(tableDef, record, key);
			SubRecord* trueKey = redundantBuilder.createSubRecordByName(BOOK_ISBN" "BOOK_PAGES, (char*)0, &pages);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		freeRecord(record);
	}
	{
		TableDefBuilder tb(0, "noname", "noname");
		tb.addColumn("a", CT_BIGINT);
		tb.addColumn("b", CT_BIGINT);
		tb.addColumn("c", CT_BIGINT);
		tb.addColumn("d", CT_BIGINT);
		tb.addColumn("e", CT_BIGINT);
		tb.addColumnS("f", CT_CHAR, 16);
		tb.addColumn("g", CT_INT);
		tb.addColumn("h", CT_SMALLINT);
		tb.addColumn("i", CT_TINYINT);

		TableDef *tableDef = tb.getTableDef();
		RecordBuilder rb(tableDef, 0, REC_FIXLEN);

		for (char a = 'a'; a <='e'; a++) {
			if (a % 2 == 0)
				rb.appendNull();
			else
				rb.appendBigInt(a - 'a');
		}
		rb.appendChar("Hello God");
		s8 i = 102;
		s16 h = 101;
		int g = 100;
		s64 a = 0;
		rb.appendInt(g);
		rb.appendSmallInt(h);
		rb.appendTinyInt(i);

		Record* record = rb.getRecord();
		SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
		{
			SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "a b d i");
			extractor(tableDef, record, key);
			SubRecord* trueKey = redundantBuilder.createSubRecordByName("a b d i", &a, NULL, NULL, &i);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		{
			SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "a g h");
			extractor(tableDef, record, key);
			SubRecord* trueKey = redundantBuilder.createSubRecordByName("a g h", &a, &g, &h);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		freeRecord(record);
		delete tableDef;
	}
}

/**
 * ExtractSubRecordFR
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列，子记录不包含NULL列
 *	case 3. 记录中包含NULL列，子记录包含NULL
 */
void RecordTestCase::testExtractSubRecordFR() {
	doTestExtractSubRecordFR(RecordOper::extractSubRecordFR);
}
void doTestExtractSubRecordVR(void (*extractor)(const TableDef *, const Record *, SubRecord *)) {
	const char* sname = "huanhuan";
	const int age = 28;

	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	{ // 没有NULL列
		Record* record = studentTable.createRecord(REC_VARLEN, sname, 200, age, "F", 1, 4.0f);
		SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		extractor(tableDef, record, key);
		SubRecord* trueKey = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 有NULL列
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		rb.appendVarchar(sname)->appendInt(12)->appendNull()->appendChar("F");
		Record* record = rb.getRecord();
		SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		extractor(tableDef, record, key);
		SubRecord* trueKey = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{ // 三个NULL列
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		rb.appendVarchar(sname)->appendNull()->appendNull()->appendNull();
		Record* record = rb.getRecord();
		SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		extractor(tableDef, record, key);
		SubRecord* trueKey = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
		freeRecord(record);
		freeSubRecord(trueKey);
		freeSubRecord(key);
	}
	{
		TableDefBuilder tb(0, "noname", "noname");
		tb.addColumnS("name", CT_VARCHAR, 64, false, false);
		tb.addColumnS("password", CT_VARCHAR, 128);
		tb.addColumn("type", CT_INT);
		tb.addColumnS("grants", CT_VARCHAR, 1024, false);
		tb.addColumnS("client_hosts", CT_VARCHAR, 1024, false);
		tb.addColumnS("qs_hosts", CT_VARCHAR, 1024, false);
		tb.addColumnS("admin_hosts", CT_VARCHAR, 1024, false);
		tb.addColumn("quota", CT_BIGINT);
		TableDef *tableDef = tb.getTableDef();
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		s32 type = 0;
		s64 quota = 1;
		rb.appendVarchar("Cat");
		rb.appendVarchar("Dog");
		rb.appendInt(type);
		rb.appendVarchar("Bull");
		rb.appendVarchar("client_hosts");
		rb.appendVarchar("qs_hosts");
		rb.appendVarchar("admin_hosts");
		rb.appendBigInt(quota);
		Record *record = rb.getRecord();
		SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
		{
			SubRecord* key = redundantBuilder.createEmptySbById(tableDef->m_maxRecSize, "0 1 2 3 4 5 6 7");
			SubRecord* trueKey = redundantBuilder.createSubRecordById("0 1 2 3 4 5 6 7",
				"Cat", "Dog", &type, "Bull", "client_hosts", "qs_hosts", "admin_hosts", &quota);
			extractor(tableDef, record, key);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		freeRecord(record);
		delete tableDef;
	}
	{
		TableDefBuilder tb(0, "noname", "noname");
		tb.addColumn("A", CT_BIGINT);
		tb.addColumn("B", CT_BIGINT);
		tb.addColumnS("a", CT_VARCHAR, Limits::PAGE_SIZE / 3, false, false);
		tb.addColumn("b", CT_BIGINT);
		tb.addColumn("c", CT_BIGINT);
		tb.addColumn("d", CT_BIGINT);
		tb.addColumn("e", CT_BIGINT);
		tb.addColumn("f", CT_BIGINT);
		tb.addColumn("g", CT_BIGINT);
		tb.addColumn("h", CT_BIGINT);
		tb.addColumnS("i", CT_VARCHAR, 7);
		tb.addColumnS("j", CT_VARCHAR, 7);
		tb.addColumnS("k", CT_VARCHAR, 7);
		tb.addColumn("l", CT_BIGINT);
		TableDef *tableDef = tb.getTableDef();
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		rb.appendBigInt(200);
		rb.appendBigInt(201);
		rb.appendVarchar("a");
		for (char a = 'b'; a <='h'; a++) {
			rb.appendBigInt(a - 'b');
		}
		rb.appendVarchar("i");
		rb.appendVarchar("j");
		rb.appendVarchar("k");
		rb.appendBigInt(100);
		Record* record = rb.getRecord();
		SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
		{
			SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "b h");
			u64 b = 0, h = 6;
			SubRecord* trueKey = redundantBuilder.createSubRecordByName("b h", &b, &h);
			extractor(tableDef, record, key);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		{
			SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "c h");
			u64 c = 1, h = 6;
			SubRecord* trueKey = redundantBuilder.createSubRecordByName("c h", &c, &h);
			extractor(tableDef, record, key);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}
		{
			SubRecord* key = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "a b k");
			u64 b = 0;
			SubRecord* trueKey = redundantBuilder.createSubRecordByName("a b k", "a", &b, "k");
			extractor(tableDef, record, key);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key, trueKey));
			freeSubRecord(trueKey);
			freeSubRecord(key);
		}

		freeRecord(record);
		delete tableDef;
	}
}
/**
 * ExtractSubRecordVR
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列，子记录不包含NULL列
 *	case 3. 记录中包含NULL列，子记录包含NULL
 */
void RecordTestCase::testExtractSubRecordVR() {
	doTestExtractSubRecordVR(RecordOper::extractSubRecordVR);
}


/**
 * ExtractSubRecordCR
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列，子记录不包含NULL列
 *	case 3. 记录中包含NULL列，子记录包含NULL
 */
void RecordTestCase::testExtractSubRecordCR() {
	const char* sname = "huanhuan";
	const short age = 28;
	const int sno = 100;

	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	SubRecordBuilder keyBuilder(tableDef, KEY_COMPRESS);
	{ // 没有NULL列
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(12);
		SubRecord *key = keyBuilder.createSubRecordByName(STU_AGE" "STU_SNO" "STU_NAME" "STU_SEX
			, &age, &sno, sname, "M");
		SubRecord* subRecord = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize
			, STU_NAME" "STU_SNO" "STU_AGE);
		RecordOper::extractSubRecordCR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubrecord = redundantBuilder.createSubRecordByName(STU_NAME" "STU_SNO" "STU_AGE
			, sname, &sno, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRecord, trueSubrecord));
		freeSubRecord(subRecord);
		freeSubRecord(trueSubrecord);
		freeSubRecord(key);
	}
	{ // 有NULL列
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(13);
		SubRecord *key = keyBuilder.createSubRecordByName(STU_AGE" "STU_NAME" "STU_SEX, NULL, sname, "M");
		SubRecord* subRecord = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::extractSubRecordCR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubrecord = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRecord, trueSubrecord));
		freeSubRecord(subRecord);
		freeSubRecord(trueSubrecord);
		freeSubRecord(key);
	}
	{ // 三个NULL列
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(13);
		SubRecord *key = keyBuilder.createSubRecordByName(STU_AGE" "STU_NAME" "STU_SEX, NULL, sname, NULL);
		SubRecord* subRecord = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_SEX" "STU_AGE);
		RecordOper::extractSubRecordCR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubrecord = redundantBuilder.createSubRecordByName(STU_SEX" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRecord, trueSubrecord));
		freeSubRecord(subRecord);
		freeSubRecord(trueSubrecord);
		freeSubRecord(key);
	}

	// 测试普通前缀索引
	const u64 id = 1;
	const char *name = "blog name";
	const char *author = "lostbag";
	const char *abs = "blog is exist";
	const char *content = "blog is not exist";
	const u64 pubtime = 5;

	const char *name_prefix = "blog";
	const char *author_prefix = "los";
	UserBlogTable blogTable;
	tableDef = blogTable.getTableDef();
	const IndexDef* indexDef = blogTable.getTableDef()->m_indice[1];
	KeyBuilder compBuilder1(tableDef, indexDef, KEY_COMPRESS);
	SubRecordBuilder sbBuilder(tableDef, REC_REDUNDANT);
	{ // 记录没有NULL列
		SubRecord* key = compBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME, author, name);
		SubRecord* subRecord = sbBuilder.createEmptySbByName(tableDef->m_maxRecSize, BLOG_NAME" "BLOG_AUTHOR);
		RecordOper::extractSubRecordCR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubRecord = sbBuilder.createSubRecordByName(BLOG_NAME" "BLOG_AUTHOR, name_prefix, author_prefix);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, subRecord, trueSubRecord, indexDef));

		freeSubRecord(subRecord);
		freeSubRecord(trueSubRecord);
		freeSubRecord(key);
	}
	{ // 记录中有NULL列
		SubRecord* key = compBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME, NULL, name);
		SubRecord* subRecord = sbBuilder.createEmptySbByName(tableDef->m_maxRecSize, BLOG_NAME" "BLOG_AUTHOR);
		RecordOper::extractSubRecordCR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubRecord = sbBuilder.createSubRecordByName(BLOG_NAME" "BLOG_AUTHOR, name_prefix, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, subRecord, trueSubRecord, indexDef));

		freeSubRecord(subRecord);
		freeSubRecord(trueSubRecord);
		freeSubRecord(key);
	}
}


/**
 * ExtractSubRecordNR
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列，子记录不包含NULL列
 *	case 3. 记录中包含NULL列，子记录包含NULL
 */
void RecordTestCase::testExtractSubRecordNR() {
	const char* sname = "huanhuan";
	const short age = 28;
	const int sno = 100;

	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	SubRecordBuilder keyBuilder(tableDef, KEY_NATURAL);
	{ // 没有NULL列
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(12);
		SubRecord *key = keyBuilder.createSubRecordByName(STU_AGE" "STU_SNO" "STU_NAME" "STU_SEX
			, &age, &sno, sname, "M");
		SubRecord* subRecord = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize
			, STU_NAME" "STU_SNO" "STU_AGE);
		RecordOper::extractSubRecordNR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubrecord = redundantBuilder.createSubRecordByName(STU_NAME" "STU_SNO" "STU_AGE
			, sname, &sno, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRecord, trueSubrecord));
		freeSubRecord(subRecord);
		freeSubRecord(trueSubrecord);
		freeSubRecord(key);
	}
	{ // 有NULL列
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(13);
		SubRecord *key = keyBuilder.createSubRecordByName(STU_AGE" "STU_NAME" "STU_SEX, NULL, sname, "M");
		SubRecord* subRecord = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::extractSubRecordNR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubrecord = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRecord, trueSubrecord));
		freeSubRecord(subRecord);
		freeSubRecord(trueSubrecord);
		freeSubRecord(key);
	}
	{ // 三个NULL列
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(13);
		SubRecord *key = keyBuilder.createSubRecordByName(STU_AGE" "STU_NAME" "STU_SEX, NULL, sname, NULL);
		SubRecord* subRecord = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_SEX" "STU_AGE);
		RecordOper::extractSubRecordNR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubrecord = redundantBuilder.createSubRecordByName(STU_SEX" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRecord, trueSubrecord));
		freeSubRecord(subRecord);
		freeSubRecord(trueSubrecord);
		freeSubRecord(key);
	}

	// 测试普通前缀索引
	const u64 id = 1;
	const char *name = "blog name";
	const char *author = "lostbag";
	const char *abs = "blog is exist";
	const char *content = "blog is not exist";
	const u64 pubtime = 5;

	const char *name_prefix = "blog";
	const char *author_prefix = "los";
	UserBlogTable blogTable;
	tableDef = blogTable.getTableDef();
	const IndexDef* indexDef = blogTable.getTableDef()->m_indice[1];
	KeyBuilder compBuilder1(tableDef, indexDef, KEY_NATURAL);
	SubRecordBuilder sbBuilder(tableDef, REC_REDUNDANT);
	{ // 记录没有NULL列
		SubRecord* key = compBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME, author, name);
		SubRecord* subRecord = sbBuilder.createEmptySbByName(tableDef->m_maxRecSize, BLOG_NAME" "BLOG_AUTHOR);
		RecordOper::extractSubRecordNR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubRecord = sbBuilder.createSubRecordByName(BLOG_NAME" "BLOG_AUTHOR, name_prefix, author_prefix);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, subRecord, trueSubRecord, indexDef));

		freeSubRecord(subRecord);
		freeSubRecord(trueSubRecord);
		freeSubRecord(key);
	}
	{ // 记录中有NULL列
		SubRecord* key = compBuilder1.createKeyByName(BLOG_AUTHOR" "BLOG_NAME, NULL, name);
		SubRecord* subRecord = sbBuilder.createEmptySbByName(tableDef->m_maxRecSize, BLOG_NAME" "BLOG_AUTHOR);
		RecordOper::extractSubRecordNR(tableDef, indexDef, key, subRecord);
		SubRecord* trueSubRecord = sbBuilder.createSubRecordByName(BLOG_NAME" "BLOG_AUTHOR, name_prefix, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, subRecord, trueSubRecord, indexDef));

		freeSubRecord(subRecord);
		freeSubRecord(trueSubRecord);
		freeSubRecord(key);
	}
}

/**
 * ConvertRecordRV
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列
 */
void RecordTestCase::testConvertRecordRV() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	{
		Record* record = studentTable.createRecord(REC_REDUNDANT, "huanhuan", 300, 58, "F", 1, 4.0f);
		Record* convertedRecord = RecordBuilder::createEmptyRecord(
			record->m_rowId, REC_VARLEN, tableDef->m_maxRecSize);
		RecordOper::convertRecordRV(tableDef, record, convertedRecord);

		Record* trueRecord = studentTable.createRecord(REC_VARLEN, "huanhuan", 300, 58, "F", 1, 4.0f);

		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef,convertedRecord, trueRecord));

		freeRecord(record);
		freeRecord(trueRecord);
		freeRecord(convertedRecord);
	}
	{ // NULL
		RecordBuilder rb(tableDef, 0, REC_REDUNDANT);
		rb.appendVarchar("google")->appendInt(300)->appendNull()->appendChar("F")->appendNull()
			->appendFloat(4.0f)->appendBigInt(0);
		Record* record = rb.getRecord();
		Record* convertedRecord = RecordBuilder::createEmptyRecord(
			record->m_rowId, REC_VARLEN, tableDef->m_maxRecSize);
		RecordOper::convertRecordRV(tableDef, record, convertedRecord);
		RecordBuilder rb1(tableDef, 0, REC_VARLEN);
		rb1.appendVarchar("google")->appendInt(300)->appendNull()->appendChar("F")->appendNull()
			->appendFloat(4.0f)->appendBigInt(0);
		Record* trueRecord = rb1.getRecord();
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, convertedRecord, trueRecord));
		freeRecord(record);
		freeRecord(trueRecord);
		freeRecord(convertedRecord);
	}
}

void doTestConvertSubRecordRV(const TableDef *tableDef, const RedRecord *record) {
	u16 *columns = new u16[tableDef->m_numCols];
	AutoPtr<u16> guard(columns, true);
	for (u16 i = 0; i < tableDef->m_numCols; ++i)
		columns[i] = i;

	byte vbuf[Limits::PAGE_SIZE];
	byte rbuf[Limits::PAGE_SIZE];
	
	System::srandom(System::fastTime());
	for (int outloop = 0; outloop < 10; ++ outloop) {
		RedRecord newRecord(*record);
		int numNullCols = System::random() % tableDef->m_numCols;
		cout << endl << "null cols: ";
		for (int i = 0; i < numNullCols; ++i) {
			u16 cno = System::random() % tableDef->m_numCols;
			if (tableDef->m_columns[cno]->m_nullable) {
				newRecord.setNull(cno);
				cout << cno << ", ";
			}
		}
		
		for (u16 num = 1; num <= tableDef->m_numCols; ++num) {
			// 生成列
			int count = 0;
			while(count < num) {
				bool found = false;
				int cno = System::random() % tableDef->m_numCols;
				for (int i = 0; i < count; ++i) {
					if (columns[i] == cno)
						found = true;
				}
				if (!found) {
					++count;
					columns[count - 1] = cno;
				}
			}
			sort(columns, columns + num);
			cout << endl << "  subrecord columns: ";
			copy(columns, columns + num, ostream_iterator<u16>(cout, ","));
			// 验证正确性
			SubRecord srcsr(REC_REDUNDANT, num, columns, newRecord.getRecord()->m_data, newRecord.getRecord()->m_size);
			SubRecord destsr(REC_VARLEN, num, columns, vbuf, tableDef->m_maxRecSize);
			SubRecord recvSrc(REC_REDUNDANT, num, columns, rbuf, tableDef->m_maxRecSize);
			RecordOper::convertSubRecordRV(tableDef, &srcsr, &destsr);
			RecordOper::convertSubRecordVR(tableDef, &destsr, &recvSrc);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, &srcsr, &recvSrc));			
		}
	}
}

/**
 * ConvertSubRecordRV
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列
 */
void RecordTestCase::testConvertSubRecordRV() {
	const char *name = "King";
	const short age = 128;
	StudentTable studentTable;
	const TableDef *tableDef = studentTable.getTableDef();
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	SubRecordBuilder varlenBuilder(tableDef, REC_VARLEN);
	{
		SubRecord *sr = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, &age);
		SubRecord *varSr = varlenBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertSubRecordRV(tableDef, sr, varSr);
		SubRecord *trueSr = varlenBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,varSr, trueSr));
		freeSubRecord(trueSr);
		freeSubRecord(varSr);
		freeSubRecord(sr);
	}
	{ // NULL
		SubRecord *sr = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, NULL);
		SubRecord *varSr = varlenBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertSubRecordRV(tableDef, sr, varSr);
		SubRecord *trueSr = varlenBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,varSr, trueSr));
		freeSubRecord(trueSr);
		freeSubRecord(varSr);
		freeSubRecord(sr);
	}
	{
		Record *rec = studentTable.createRecord(REC_REDUNDANT, "name", 001, 20, "F", 8, 5.0, 3);
		RedRecord redRec(tableDef, rec);
		doTestConvertSubRecordRV(tableDef, &redRec);
		freeRecord(rec);
	}
	{
		BookTable bookTable;
		Record *rec = bookTable.createRecord("abook", "----", 100, 100, REC_REDUNDANT);
		RedRecord redRec(bookTable.getTableDef(), rec);
		doTestConvertSubRecordRV(bookTable.getTableDef(), &redRec);
		freeRecord(rec);
	}
}
/**
 * ConvertSubRecordVR
 * 测试流程：
 *	case 1. 子记录没有NULL列
 *	case 2. 子记录中包含NULL列
 */
void RecordTestCase::testConvertSubRecordVR() {
	const char *name = "King";
	const short age = 128;
	StudentTable studentTable;
	const TableDef *tableDef = studentTable.getTableDef();
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	SubRecordBuilder varlenBuilder(tableDef, REC_VARLEN);
	{
		SubRecord *sr = varlenBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, &age);
		SubRecord *reSr = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertSubRecordVR(tableDef, sr, reSr);
		SubRecord *trueSr = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,reSr, trueSr));
		freeSubRecord(trueSr);
		freeSubRecord(reSr);
		freeSubRecord(sr);
	}
	{ // NULL
		SubRecord *sr = varlenBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, NULL);
		SubRecord *reSr = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::convertSubRecordVR(tableDef, sr, reSr);
		SubRecord *trueSr = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, name, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,reSr, trueSr));
		freeSubRecord(trueSr);
		freeSubRecord(reSr);
		freeSubRecord(sr);
	}
}
/**
 * UpdateRecordRR
 * 测试流程：
 *	case 1. 记录变短
 *	case 2. 记录变长
 *	case 3. 记录长度不变
 *	case 4. 更新为NULL
 */
void RecordTestCase::testUpdateRecordRR() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	Record* record = studentTable.createRecord(REC_REDUNDANT, "beast", 127, 58, "F", 1, 4.0f);
	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	{ // 变短
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_SEX, "nini", "M");
		RecordOper::updateRecordRR(tableDef, record, update);
		Record* trueRecord = studentTable.createRecord(REC_REDUNDANT, "nini", 127, 58, "M", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}

	{ // 变长
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_SEX, "ultralisk", "M");
		RecordOper::updateRecordRR(tableDef, record, update);
		Record* trueRecord = studentTable.createRecord(REC_REDUNDANT, "ultralisk", 127, 58, "M", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}

	{ // 长度不变
		int sno = 99;
		SubRecord* update = sbb.createSubRecordByName(STU_SNO" "STU_SEX, &sno, "M");
		RecordOper::updateRecordRR(tableDef, record, update);
		Record* trueRecord = studentTable.createRecord(REC_REDUNDANT, "ultralisk", sno, 58, "M", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}
	{ // 更新为NULL
		SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
		SubRecord* update = sbb.createSubRecordByName(STU_SNO" "STU_SEX, NULL, NULL);
		RecordOper::updateRecordRR(tableDef, record, update);
		RecordBuilder rb(tableDef, 0, REC_REDUNDANT);
		rb.appendVarchar("ultralisk")->appendNull()->appendSmallInt(58)->appendNull()->appendMediumInt(1)
			->appendFloat(4.0f)->appendBigInt(0);
		Record* trueRecord = rb.getRecord();
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}

	freeRecord(record);
}
/**
 * UpdateRecordFR
 * 测试流程：
 *	case 1. 普通情况
 *	case 2. 更新为NULL
 */
void RecordTestCase::testUpdateRecordFR() {
	BookTable bookTable;
	const TableDef* tableDef = bookTable.getTableDef();
	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	{
		Record* record = bookTable.createRecord("C++", "007-008", 120, 18, REC_FIXLEN);
		int price = 99;
		SubRecord* update = sbb.createSubRecordByName(BOOK_ISBN" "BOOK_PRICE, "001-002", &price);
		RecordOper::updateRecordFR(tableDef, record, update);
		Record* trueRecord = bookTable.createRecord("C++", "001-002", 120, price, REC_FIXLEN);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(record);
		freeRecord(trueRecord);
		freeSubRecord(update);
	}
	{ // NULL
		Record* record = bookTable.createRecord("C++", "007-008", 120, 18, REC_FIXLEN);
		SubRecord* update = sbb.createSubRecordByName(BOOK_ISBN" "BOOK_PRICE, NULL, NULL);
		RecordOper::updateRecordFR(tableDef, record, update);
		RecordBuilder rb(tableDef, 0, REC_FIXLEN);
		rb.appendChar("C++")->appendNull()->appendInt(120)->appendNull();
		Record* trueRecord = rb.getRecord();
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(record);
		freeRecord(trueRecord);
		freeSubRecord(update);
	}
}

/**
 * GetUpdateSizeVR
 * 测试流程：
 *	case 1. 记录长度变短
 *	case 2. 记录长度不变
 *	case 3. 记录长度变长
 *	case 4. 记录更新为NULL
 */
void RecordTestCase::testGetUpdateSizeVR() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	Record* record = studentTable.createRecord(REC_VARLEN, "huanhuan", 127, 58, "F", 1, 4.0f);
	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	{ // 变短
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_SEX, "nini", "M");
		u16 size = RecordOper::getUpdateSizeVR(tableDef, record, update);
		Record* newRecord = studentTable.createRecord(REC_VARLEN, "nini", 127, 58, "M", 1, 4.0f);
		CPPUNIT_ASSERT((uint)size == newRecord->m_size);
		freeRecord(newRecord);
		freeSubRecord(update);
	}
	{ // 不变
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_SEX, "nauhnuah", "M");
		u16 size = RecordOper::getUpdateSizeVR(tableDef, record, update);
		Record* newRecord = studentTable.createRecord(REC_VARLEN,  "nauhnuah", 127, 58, "M", 1, 4.0f);
		CPPUNIT_ASSERT((uint)size == newRecord->m_size);
		freeRecord(newRecord);
		freeSubRecord(update);
	}
	{ // 变长
		int sno = 1;
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_SNO, "huanhuan1", &sno);
		u16 size = RecordOper::getUpdateSizeVR(tableDef, record, update);
		Record* newRecord = studentTable.createRecord(REC_VARLEN, "huanhuan1", sno, 58, "F", 1, 4.0f);
		CPPUNIT_ASSERT((uint)size == newRecord->m_size);
		freeRecord(newRecord);
		freeSubRecord(update);
	}
	{ // NULL
		SubRecord* update = sbb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		u16 size = RecordOper::getUpdateSizeVR(tableDef, record, update);
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		rb.appendVarchar("huanhuan")->appendNull()->appendNull()->appendChar("F")->appendMediumInt(1)
			->appendFloat(4.0f)->appendBigInt(0);
		Record* newRecord = rb.getRecord();
		CPPUNIT_ASSERT((uint)size == newRecord->m_size);
		freeRecord(newRecord);
		freeSubRecord(update);
	}
	freeRecord(record);
}


void RecordTestCase::testGetRecordSizeRV() {
	StudentTable student;
	const TableDef *tableDef = student.getTableDef();
	Record *redRec = student.createRecord(REC_REDUNDANT, "House", 1, 12, "M", 10, 4.0f);
	{

		Record *varRec = student.createRecord(REC_VARLEN, "House", 1, 12, "M", 10, 4.0f);
		CPPUNIT_ASSERT(varRec->m_size == RecordOper::getRecordSizeRV(student.getTableDef(), redRec));
		freeRecord(varRec);
	}
	{
		byte *data = new byte[tableDef->m_maxRecSize];
		RedRecord::setNull(student.getTableDef(), redRec->m_data, 0);
		RedRecord::setNull(student.getTableDef(), redRec->m_data, 1);
		Record tmpRec(INVALID_ROW_ID, REC_VARLEN, data, tableDef->m_maxRecSize);
		RecordOper::convertRecordRV(student.getTableDef(), redRec, &tmpRec);
		CPPUNIT_ASSERT(tmpRec.m_size == RecordOper::getRecordSizeRV(tableDef, redRec));
		delete [] data;
	}
	freeRecord(redRec);
}



/**
 * 冗余格式子子记录转化为变成格式之后的长度
 */
void RecordTestCase::testGetSubRecordSizeRV() {
	{ // 变长表
		StudentTable studentTable;
		const TableDef* tableDef = studentTable.getTableDef();
		Record* record = studentTable.createRecord(REC_REDUNDANT, "lily", 123, 50, "M", 9, 4.0, 2);
		RedRecord redRec(tableDef, record);
		byte buf[Limits::PAGE_SIZE];
		{ //  普通记录
			u16 columns[4] = {0, 1, 3, 5};
			SubRecord subrec(REC_REDUNDANT, sizeof(columns)/sizeof(columns[0]), columns, record->m_data, record->m_size);
			SubRecord dest(REC_VARLEN, sizeof(columns)/sizeof(columns[0]), columns, buf, sizeof(buf));
			RecordOper::convertSubRecordRV(tableDef, &subrec, &dest);
			CPPUNIT_ASSERT(dest.m_size == RecordOper::getSubRecordSizeRV(tableDef, &subrec));
		}
		{ // 包含null
			u16 columns[3] =  {1, 2, 3};
			redRec.setNull(2);
			SubRecord subrec(REC_REDUNDANT, sizeof(columns)/sizeof(columns[0]), columns, record->m_data, record->m_size);
			SubRecord dest(REC_VARLEN, sizeof(columns)/sizeof(columns[0]), columns, buf, sizeof(buf));
			RecordOper::convertSubRecordRV(tableDef, &subrec, &dest);
			subrec.m_format = REC_MYSQL; // 验证Mysql格式也可以
			CPPUNIT_ASSERT(dest.m_size == RecordOper::getSubRecordSizeRV(tableDef, &subrec));
			subrec.m_format = REC_REDUNDANT;
		}
		{ // 全null
			u16 columns[3] =  {1, 3, 4};
			redRec.setNull(1)->setNull(4)->setNull(3);
			SubRecord subrec(REC_REDUNDANT, sizeof(columns)/sizeof(columns[0]), columns, record->m_data, record->m_size);
			SubRecord dest(REC_VARLEN, sizeof(columns)/sizeof(columns[0]), columns, buf, sizeof(buf));
			RecordOper::convertSubRecordRV(tableDef, &subrec, &dest);
			CPPUNIT_ASSERT(dest.m_size == RecordOper::getSubRecordSizeRV(tableDef, &subrec));
		}
		freeRecord(record);
	}
	{ // 定长表
		BookTable bookTable;;
		const TableDef* tableDef = bookTable.getTableDef();
		Record* record = bookTable.createRecord("dojo", "001099911", 200, 100, REC_REDUNDANT);
		RedRecord redRec(tableDef, record);
		byte buf[Limits::PAGE_SIZE];
		{ //  普通记录
			u16 columns[3] = {0, 1, 3};
			SubRecord subrec(REC_REDUNDANT, sizeof(columns)/sizeof(columns[0]), columns, record->m_data, record->m_size);
			SubRecord dest(REC_VARLEN, sizeof(columns)/sizeof(columns[0]), columns, buf, sizeof(buf));
			RecordOper::convertSubRecordRV(tableDef, &subrec, &dest);
			CPPUNIT_ASSERT(dest.m_size == RecordOper::getSubRecordSizeRV(tableDef, &subrec));
		}
		{ // 包含NULL
			u16 columns[3] =  {1, 2, 3};
			redRec.setNull(2);
			SubRecord subrec(REC_REDUNDANT, sizeof(columns)/sizeof(columns[0]), columns, record->m_data, record->m_size);
			SubRecord dest(REC_VARLEN, sizeof(columns)/sizeof(columns[0]), columns, buf, sizeof(buf));
			RecordOper::convertSubRecordRV(tableDef, &subrec, &dest);
			CPPUNIT_ASSERT(dest.m_size == RecordOper::getSubRecordSizeRV(tableDef, &subrec));
		}
		freeRecord(record);
	}
}


/**
 * UpdateRecordVRInPlace
 * 测试流程：
 *	case 1. 记录长度变短
 *	case 2. 记录长度变长
 *	case 3. 记录长度不变
 *	case 4. 记录更新为NULL
 */
void RecordTestCase::testUpdateRecordVRInPlace() {
	short age = 58;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	Record* record = studentTable.createRecord(REC_VARLEN, "huanhuan", 127, age, "F", 1, 4.0f);
	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	{ // 变短
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_SEX, "nini", "M");
		RecordOper::updateRecordVRInPlace(tableDef, record, update, record->m_size);
		Record* trueRecord = studentTable.createRecord(REC_VARLEN, "nini", 127, age, "M", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}

	{ // 变长
		age = 99;
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_AGE, "huanhuan1", &age);
		RecordOper::updateRecordVRInPlace(tableDef, record, update, tableDef->m_maxRecSize);
		Record* trueRecord = studentTable.createRecord(REC_VARLEN, "huanhuan1", 127, 99, "M", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}

	{ // 长度不变
		int sno = 99;
		SubRecord* update = sbb.createSubRecordByName(STU_SNO" "STU_SEX, &sno, "M");
		RecordOper::updateRecordVRInPlace(tableDef, record, update, record->m_size);
		Record* trueRecord = studentTable.createRecord(REC_VARLEN, "huanhuan1", sno, age, "M", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}
	{ // 更新为NULL
		SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
		SubRecord* update = sbb.createSubRecordByName(STU_SNO" "STU_SEX, NULL, NULL);
		RecordOper::updateRecordVRInPlace(tableDef, record, update, record->m_size);
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		rb.appendVarchar("huanhuan1")->appendNull()->appendSmallInt(age)->appendNull()->appendMediumInt(1)
			->appendFloat(4.0f)->appendBigInt(0);
		Record* trueRecord = rb.getRecord();
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}

	freeRecord(record);
}

/**
 * UpdateRecordVR
 * 测试流程：
 *	case 1. 普通更新
 *	case 2. 记录更新为NULL
 */
void RecordTestCase::testUpdateRecordVR() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	Record* record = studentTable.createRecord(REC_VARLEN, "huanhuan", 127, 58, "F", 1, 4.0f);
	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	{
		short age = 200;
		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_AGE, "beijing2008", &age);
		u16 newSize = RecordOper::getUpdateSizeVR(tableDef, record, update);
		byte *newBuf = new byte[newSize];
		RecordOper::updateRecordVR(tableDef, record, update, newBuf);
		delete[] record->m_data;
		record->m_data = newBuf;
		record->m_size = newSize;
		Record* trueRecord = studentTable.createRecord(REC_VARLEN, "beijing2008", 127, age, "F", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}
	{ // 更新为NULL
		SubRecord* update = sbb.createSubRecordByName(STU_AGE" "STU_SEX, NULL, NULL);
		u16 newSize = RecordOper::getUpdateSizeVR(tableDef, record, update);
		byte *newBuf = new byte[newSize];
		RecordOper::updateRecordVR(tableDef, record, update, newBuf);
		delete[] record->m_data;
		record->m_data = newBuf;
		record->m_size = newSize;
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		rb.appendVarchar("beijing2008")->appendInt(127)->appendNull()->appendNull()->appendMediumInt(1)
			->appendFloat(4.0f)->appendBigInt(0);
		Record* trueRecord = rb.getRecord();
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, record, trueRecord));
		freeRecord(trueRecord);
		freeSubRecord(update);
	}
	freeRecord(record);
}


/**
 * CompareKeyCC
 * 测试流程：
 *	case 1. key1 == key2
 *	case 2. key1 > key2, 且key1、key2的列顺序不同于表定义列顺序
 *	case 3. key1 > key2
 *	case 4. key1 > key2, 验证0和负数比较的正确性
 *	case 5. key1 > key2, 验证负数比较的正确性
 *	case 6. key1 > key2, 验证NULL处理的正确性
 *	case 7. key1 == key2, key1,key2的列全NULL
 */
void RecordTestCase::testCompareKeyCC() {
	BookTable bookTable;
	const TableDef* tableDef = bookTable.getTableDef();

	SubRecordBuilder sbb(tableDef, KEY_COMPRESS);
	int pages, price;
	{
		const IndexDef *indexDef = tableDef->m_indice[4];
		SubRecord* sb1 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = 100), &(price = 99));
		SubRecord* sb2 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = 100), &(price = 99));
		CPPUNIT_ASSERT(RecordOper::compareKeyCC(tableDef, sb1, sb2, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{ // 颠倒列顺序
		const IndexDef *indexDef = tableDef->m_indice[5];
		SubRecord* sb1 = sbb.createSubRecordByName(BOOK_PRICE" "BOOK_PAGES, &(price = 99), &(pages = 101));
		SubRecord* sb2 = sbb.createSubRecordByName(BOOK_PRICE" "BOOK_PAGES, &(price = 99), &(pages = 100));
		CPPUNIT_ASSERT(RecordOper::compareKeyCC(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->m_indice[4];
		SubRecord* sb1 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = 101), &(price = 98));
		SubRecord* sb2 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = 100), &(price = 99));
		CPPUNIT_ASSERT(RecordOper::compareKeyCC(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->m_indice[4];
		SubRecord* sb1 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = 0), &(price = 98));
		SubRecord* sb2 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = -1), &(price = 98));
		CPPUNIT_ASSERT(RecordOper::compareKeyCC(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->m_indice[4];
		SubRecord* sb1 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = -12), &(price = 0));
		SubRecord* sb2 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = -5678), &(price = 0));
		CPPUNIT_ASSERT(RecordOper::compareKeyCC(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{ // NULL为最小
		const IndexDef *indexDef = tableDef->m_indice[4];
		SubRecord* sb1 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, NULL, &(price = 0));
		SubRecord* sb2 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, &(pages = 1), &(price = 0));
		CPPUNIT_ASSERT(RecordOper::compareKeyCC(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->m_indice[4];
		SubRecord* sb1 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, NULL, NULL);
		SubRecord* sb2 = sbb.createSubRecordByName(BOOK_PAGES" "BOOK_PRICE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::compareKeyCC(tableDef, sb1, sb2, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
}
/**
 * CompareKeyRC
 * 测试流程：
 *	case 1. key1 < key2, 127和128刚好处在1字节和2字节的边界处
 *	case 2. key1 > key2, 验证负数比较
 *	case 3. key1 > key2，key1和key2有不同的列顺序
 *	case 4. key1 < key2, 字符串列和整数列一起参与比较
 *	case 5. key1 < key2, 字符串长度决定大小
 *	case 6. key1 > key2, 验证NULL处理的正确性
 *	case 7. key1 == key2, key1,key2的列全NULL
 */
void RecordTestCase::testCompareKeyRC() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder compBuilder(tableDef, KEY_COMPRESS);
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	int sno;
	{ // 颠倒列顺序
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = redundantBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = compBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &(sno = -2), "liuxiang");
		CPPUNIT_ASSERT(RecordOper::compareKeyRC(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{ // 无符号整数
		s64 grade = -1;
		{
			const IndexDef *indexDef = tableDef->getIndexDef(7);
			SubRecord* sb1 = redundantBuilder.createSubRecordByName(STU_GRADE, &(grade = -1));
			SubRecord* sb2 = compBuilder.createSubRecordByName(STU_GRADE, &(grade = -2));
			CPPUNIT_ASSERT(RecordOper::compareKeyRC(tableDef, sb1, sb2, indexDef) > 0);
			freeSubRecord(sb1);
			freeSubRecord(sb2);
		}
		{
			const IndexDef *indexDef = tableDef->getIndexDef(7);
			SubRecord* sb1 = redundantBuilder.createSubRecordByName(STU_GRADE, &(grade = -100));
			SubRecord* sb2 = compBuilder.createSubRecordByName(STU_GRADE, &(grade = 100));
			CPPUNIT_ASSERT(RecordOper::compareKeyRC(tableDef, sb1, sb2, indexDef) > 0);
			freeSubRecord(sb1);
			freeSubRecord(sb2);
		}
	}
	{
		struct StuKey {
			bool m_nullArray[4];
			char *m_name;
			int	m_sno;
			float m_gpa;
			int m_class;
		};

		struct CompareTest {
			StuKey m_key1;
			StuKey m_key2;
			int m_result;
		};


		CompareTest tests[] = {
			{{{false, false, true, true}, "liuxiang", 127},	{{false, false, true, true}, "liuxiang", 128},	-1},
			{{{false, false, true, true}, "liuxiang", -1},	{{false, false, true, true}, "liuxiang", -2},	1},
			{{{false, false, true, true}, "liuxiang", -1},	{{false, false, true, true}, "liuyifei", -1},	-1},
			{{{false, false, true, true}, "liuxiang", -1},	{{false, false, true, true}, "liuxiang1", -1},	-1},
			{{{false, true, true, true}, "a"},	{{false, false, true, true}, "a", 1},	-1},
			{{{false, true, true, true}, "a"},	{{false, true, true, true}, "a"},	0},
			{{{false, false, false, false}, "liuxiang", -1, 4.0f, 70000},	{{false, false, false, false}, "liuxiang", -1, 4.0f, 70001},	-1},
			{{{false, false, false, false}, "liuxiang", -1, 4.0f, -1},	{{false, false, false, false}, "liuxiang", -1, 4.0f, 70001},	-1},
			{{{false, false, false, false}, "liuxiang", -1, 4.0f, -1},	{{false, false, false, false}, "liuxiang", -1, 4.0f, -2},	1},
			{{{false, false, false, false}, "liuxiang", -1, 4.0f, 70000},	{{false, false, false, false}, "liuxiang", -1, -3.9f, 70001},	1},
		};

		const IndexDef *indexDef = tableDef->getIndexDef(8);
		for (size_t i = 0; i < sizeof(tests) / sizeof(*tests); ++i) {
			char *name = tests[i].m_key1.m_nullArray[0] ? NULL : tests[i].m_key1.m_name;
			int *sno = tests[i].m_key1.m_nullArray[1] ? NULL : &tests[i].m_key1.m_sno;
			float *gpa = tests[i].m_key1.m_nullArray[2] ? NULL : &tests[i].m_key1.m_gpa;
			int *cls = tests[i].m_key1.m_nullArray[3] ? NULL : &tests[i].m_key1.m_class;
			SubRecord* sb1 = redundantBuilder.createSubRecordByName(STU_NAME" "STU_SNO" "STU_GPA" "STU_CLASS, name, sno, gpa, cls);

			name = tests[i].m_key2.m_nullArray[0] ? NULL : tests[i].m_key2.m_name;
			sno = tests[i].m_key2.m_nullArray[1] ? NULL : &tests[i].m_key2.m_sno;
			gpa = tests[i].m_key2.m_nullArray[2] ? NULL : &tests[i].m_key2.m_gpa;
			cls = tests[i].m_key2.m_nullArray[3] ? NULL : &tests[i].m_key2.m_class;
			SubRecord* sb2 = compBuilder.createSubRecordByName(STU_NAME" "STU_SNO" "STU_GPA" "STU_CLASS, name, sno, gpa, cls);

			if (tests[i].m_result == 0) {
				CPPUNIT_ASSERT(RecordOper::compareKeyRC(tableDef, sb1, sb2, indexDef) == 0);
			} else if (tests[i].m_result < 0) {
				CPPUNIT_ASSERT(RecordOper::compareKeyRC(tableDef, sb1, sb2, indexDef) < 0);
			} else {
				CPPUNIT_ASSERT(RecordOper::compareKeyRC(tableDef, sb1, sb2, indexDef) > 0);
			}
			freeSubRecord(sb1);
			freeSubRecord(sb2);
		}

	}
	{ // for coverage
		TableDefBuilder tb(1, "Olympic", "student");
		tb.addColumnS(STU_NAME, CT_VARCHAR, 11, false, false, COLL_LATIN1);
		tb.addColumn(STU_SNO, CT_BIGINT);
		tb.addColumn(STU_AGE, CT_TINYINT);
		tb.addColumn("money", CT_SMALLINT);
		tb.addIndex("name_sno_age_index", false, false, false, STU_NAME, 0, STU_SNO, 0, STU_AGE, 0, "money", 0, NULL);
		TableDef *tableDef = tb.getTableDef();
		SubRecordBuilder compBuilder(tableDef, KEY_COMPRESS);
		SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
		u64 sno = 99999;
		u8 age = 11;
		u16 money = 100;
		const IndexDef *indexDef = tableDef->getIndexDef(0);
		SubRecord  *sb1 = redundantBuilder.createSubRecordByName(
			STU_NAME" "STU_SNO" "STU_AGE" money", "mooncake", &sno, &age, &money);
		SubRecord  *sb2 = compBuilder.createSubRecordByName(
			STU_NAME" "STU_SNO" "STU_AGE" money", "mooncake", &sno, &age, &money);
		CPPUNIT_ASSERT(RecordOper::compareKeyRC(tableDef, sb1, sb2, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
		delete tableDef;
	}
}
/**
 * CompareKeyPC
 * 测试流程：
 *	case 1. key1 > key2, 验证负数比较
 *	case 2. key1 > key2, key1和key2有不同的列顺序
 *	case 3. key1 > key2，字符串列和整数列一起参与比较
 *	case 4. key1 < key2, 字符串长度决定大小
 *	case 5. key1 > key2, 验证NULL处理的正确性
 *	case 6. key1 == key2, key1,key2的列全NULL
 */
void RecordTestCase::testCompareKeyPC() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder compBuilder(tableDef, KEY_COMPRESS);
	SubRecordBuilder padBuilder(tableDef, KEY_PAD);
	int sno;
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = compBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -2));
		CPPUNIT_ASSERT(RecordOper::compareKeyPC(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{ // 颠倒列顺序
		const IndexDef *indexDef = tableDef->getIndexDef(6);
		SubRecord* sb1 = padBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &(sno = -1), "liuxiang");
		SubRecord* sb2 = compBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &(sno = -2), "liuxiang");
		CPPUNIT_ASSERT(RecordOper::compareKeyPC(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = compBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuyifei", &(sno = -1));
		CPPUNIT_ASSERT(RecordOper::compareKeyPC(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = compBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang1", &(sno = -1));
		CPPUNIT_ASSERT(RecordOper::compareKeyPC(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "a", NULL);
		SubRecord* sb2 = compBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "a", &(sno = 1));
		CPPUNIT_ASSERT(RecordOper::compareKeyPC(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(3);
		SubRecord* sb1 = padBuilder.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* sb2 = compBuilder.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::compareKeyPC(tableDef, sb1, sb2, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
}



/**
 * CompareKeyRR
 * 测试流程：
 *	case 1. key1 > key2, 验证负数比较
 *	case 2. key1 > key2, 验证NULL处理的正确性
 *	case 3. key1 == key2, key1,key2的列全NULL
 */
void RecordTestCase::testCompareKeyRR() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder redBuilder1(tableDef, REC_REDUNDANT);
	SubRecordBuilder redBuilder2(tableDef, REC_REDUNDANT);
	int sno;
	int age;
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = redBuilder1.createSubRecordByName(STU_NAME" "STU_SNO" "STU_AGE, "liuxiang", &(sno = -1), &(age = 1));
		SubRecord* sb2 = redBuilder2.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -2));
		CPPUNIT_ASSERT(RecordOper::compareKeyRR(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = redBuilder1.createSubRecordByName(STU_NAME" "STU_SNO" "STU_AGE, "a", NULL, NULL);
		SubRecord* sb2 = redBuilder2.createSubRecordByName(STU_NAME" "STU_SNO, "a", &(sno = 1));
		CPPUNIT_ASSERT(RecordOper::compareKeyRR(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(3);
		SubRecord* sb1 = redBuilder1.createSubRecordByName(STU_SNO" "STU_AGE" "STU_SEX, NULL, NULL, NULL);
		SubRecord* sb2 = redBuilder2.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::compareKeyRR(tableDef, sb1, sb2, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
}
/**
 * CompareKeyNN
 * 测试流程：
 *	case 1. key1 > key2, 验证负数比较
 *	case 2. key1 > key2, key1和key2有不同的列顺序
 *	case 3. key1 > key2，字符串列和整数列一起参与比较
 *	case 4. key1 < key2, 字符串长度决定大小
 *	case 5. key1 > key2, 验证NULL处理的正确性
 *	case 6. key1 == key2, key1,key2的列全NULL
 *	case 7. key1 == key2, 测试列末空格
 */
void RecordTestCase::testCompareKeyNN() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder srb(tableDef, KEY_NATURAL);
	int sno;
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -2));
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{ // 颠倒列顺序
		const IndexDef *indexDef = tableDef->getIndexDef(6);
		SubRecord* sb1 = srb.createSubRecordByName(STU_SNO" "STU_NAME, &(sno = -1), "liuxiang");
		SubRecord* sb2 = srb.createSubRecordByName(STU_SNO" "STU_NAME, &(sno = -2), "dongrina");
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "liuyifei", &(sno = -1));
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang1", &(sno = -1));
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "a", NULL);
		SubRecord* sb2 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "a", &(sno = 1));
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "a", NULL);
		SubRecord* sb2 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "a", NULL);
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb2, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{ // 测试列末空格
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "a ", NULL);
		SubRecord* sb2 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "a", NULL);
		SubRecord* sb3 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "a  ", NULL);
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb2, indexDef) == 0);
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb1, sb3, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
		freeSubRecord(sb3);

		SubRecord* sb4 = srb.createSubRecordByName(STU_NAME" "STU_SNO, "", NULL);
		SubRecord* sb5 = srb.createSubRecordByName(STU_NAME" "STU_SNO, " ", NULL);
		CPPUNIT_ASSERT(RecordOper::compareKeyNN(tableDef, sb4, sb5, indexDef) == 0);
		freeSubRecord(sb4);
		freeSubRecord(sb5);
	}
}

/**
 * CompareKeyNP
 * 测试流程：
 *	case 1. key1 > key2, 验证负数比较
 *	case 2. key1 > key2, key1和key2有不同的列顺序
 *	case 3. key1 > key2，字符串列和整数列一起参与比较
 *	case 4. key1 < key2, 字符串长度决定大小
 *	case 5. key1 > key2, 验证NULL处理的正确性
 *	case 6. key1 == key2, key1,key2的列全NULL
 */
void RecordTestCase::testCompareKeyNP() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder natBuilder(tableDef, KEY_NATURAL);
	SubRecordBuilder padBuilder(tableDef, KEY_PAD);
	int sno;
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = natBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -2));
		CPPUNIT_ASSERT(RecordOper::compareKeyNP(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{ // 颠倒列顺序
		const IndexDef *indexDef = tableDef->getIndexDef(6);
		SubRecord* sb1 = natBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &(sno = -1), "liuxiang");
		SubRecord* sb2 = padBuilder.createSubRecordByName(STU_SNO" "STU_NAME, &(sno = -2), "liuxiang");
		CPPUNIT_ASSERT(RecordOper::compareKeyNP(tableDef, sb1, sb2, indexDef) > 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = natBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuyifei", &(sno = -1));
		CPPUNIT_ASSERT(RecordOper::compareKeyNP(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = natBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang", &(sno = -1));
		SubRecord* sb2 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "liuxiang1", &(sno = -1));
		CPPUNIT_ASSERT(RecordOper::compareKeyNP(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(5);
		SubRecord* sb1 = natBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "a", NULL);
		SubRecord* sb2 = padBuilder.createSubRecordByName(STU_NAME" "STU_SNO, "a", &(sno = 1));
		CPPUNIT_ASSERT(RecordOper::compareKeyNP(tableDef, sb1, sb2, indexDef) < 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
	{
		const IndexDef *indexDef = tableDef->getIndexDef(3);
		SubRecord* sb1 = natBuilder.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* sb2 = padBuilder.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(RecordOper::compareKeyNP(tableDef, sb1, sb2, indexDef) == 0);
		freeSubRecord(sb1);
		freeSubRecord(sb2);
	}
}

/**
 * CompressKey
 * 测试流程：
 *	case 1. 普通key
 *	case 2. key的列顺序不同于表定义的列顺序
 *	case 3. 验证NULL处理的正确性
 */
void RecordTestCase::testCompressKey() {
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	SubRecordBuilder natureBuilder(tableDef, KEY_NATURAL);
	SubRecordBuilder compBuilder(tableDef, KEY_COMPRESS);
	{ // NOT NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[9];
		SubRecord* natureKey = natureBuilder.createSubRecordByName(STU_NAME" "STU_SEX, "hangzhou", "M");
		SubRecord* compKey = compBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_SEX);
		RecordOper::compressKey(tableDef, indexDef, natureKey, compKey);
		SubRecord* trueKey = compBuilder.createSubRecordByName(STU_NAME" "STU_SEX, "hangzhou", "M");
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,compKey, trueKey, indexDef));
		freeSubRecord(natureKey);
		freeSubRecord(compKey);
		freeSubRecord(trueKey);
	}
	{ // 颠倒顺序
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[10];
		SubRecord* natureKey = natureBuilder.createSubRecordByName(STU_SEX" "STU_NAME, "M", "hangzhou");
		SubRecord* compKey = compBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_SEX" "STU_NAME);
		RecordOper::compressKey(tableDef, indexDef, natureKey, compKey);
		SubRecord* trueKey = compBuilder.createSubRecordByName(STU_SEX" "STU_NAME, "M", "hangzhou");
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,compKey, trueKey, indexDef));
		freeSubRecord(natureKey);
		freeSubRecord(compKey);
		freeSubRecord(trueKey);
	}
	{ // NULL
		const IndexDef* indexDef = studentTable.getTableDef()->m_indice[11];
		SubRecord* natureKey = natureBuilder.createSubRecordByName(STU_SNO" "STU_SEX, NULL, "M");
		SubRecord* compKey = compBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_SNO" "STU_SEX);
		RecordOper::compressKey(tableDef, indexDef, natureKey, compKey);
		SubRecord* trueKey = compBuilder.createSubRecordByName(STU_SNO" "STU_SEX, NULL, "M");
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,compKey, trueKey, indexDef));
		freeSubRecord(natureKey);
		freeSubRecord(compKey);
		freeSubRecord(trueKey);
	}
}
/**
 * MergeSubRecordRR
 * 测试流程：
 *	case 1. 子记录变短
 *	case 2. 子记录变长
 *	case 3. 子记录长度不变
 *	case 4. 更新为NULL
 */
void RecordTestCase::testMergeSubRecordRR() {
	short age = 100;
	int sno = 250;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	SubRecord* oldSr = sbb.createSubRecordByName(STU_NAME" "STU_AGE" "STU_SEX, "J.Bush", &age, "M");
	{ // 变短
		SubRecord* newSr = sbb.createSubRecordByName(STU_NAME" "STU_SNO, "Obama", &sno);
		RecordOper::mergeSubRecordRR(tableDef, newSr, oldSr);
		SubRecord* trueSr = sbb.createSubRecordByName(STU_NAME" "STU_SNO" "STU_AGE" "STU_SEX
			, "Obama", &sno, &age, "M");
		CPPUNIT_ASSERT(trueSr->m_size == newSr->m_size);
		CPPUNIT_ASSERT(!memcmp(newSr->m_data, trueSr->m_data, newSr->m_size));
		freeSubRecord(trueSr);
		freeSubRecord(newSr);
	}
	{ // 变长
		SubRecord* newSr = sbb.createSubRecordByName(STU_NAME" "STU_SEX, "Android", "XY");
		RecordOper::mergeSubRecordRR(tableDef, newSr, oldSr);
		SubRecord* trueSr = sbb.createSubRecordByName(STU_NAME" "STU_AGE" "STU_SEX, "Android", &age, "XY");
		CPPUNIT_ASSERT(trueSr->m_size == newSr->m_size);
		CPPUNIT_ASSERT(!memcmp(newSr->m_data, trueSr->m_data, newSr->m_size));
		freeSubRecord(trueSr);
		freeSubRecord(newSr);
	}

	{ // 长度不变
		SubRecord* newSr = sbb.createSubRecordByName(STU_SEX, "F");
		RecordOper::mergeSubRecordRR(tableDef, newSr, oldSr);
		SubRecord* trueSr = sbb.createSubRecordByName(STU_NAME" "STU_AGE" "STU_SEX, "J.Bush", &age, "F");
		CPPUNIT_ASSERT(trueSr->m_size == newSr->m_size);
		CPPUNIT_ASSERT(!memcmp(newSr->m_data, trueSr->m_data, newSr->m_size));
		freeSubRecord(trueSr);
		freeSubRecord(newSr);
	}
	{ // 更新为NULL
		SubRecord* newSr = sbb.createSubRecordByName(STU_NAME" "STU_SEX, "McCain", NULL);
		RecordOper::mergeSubRecordRR(tableDef, newSr, oldSr);
		SubRecord* trueSr = sbb.createSubRecordByName(STU_NAME" "STU_AGE" "STU_SEX, "McCain", &age, NULL);
		CPPUNIT_ASSERT(trueSr->m_size == newSr->m_size);
		CPPUNIT_ASSERT(!memcmp(newSr->m_data, trueSr->m_data, newSr->m_size));
		freeSubRecord(trueSr);
		freeSubRecord(newSr);
	}

	freeSubRecord(oldSr);
}
/**
 * 测试小型大对象相关记录操作
 * 测试流程：
 *	case 1. 创建小型大对象记录
 *	case 2. 创建一个为NULL的大对象记录
 *	case 3. 创建小型大对象子记录
 *	case 4. 创建一个为NULL的大对象子记录
 *	case 5. 提取大对象数据
 */
void RecordTestCase::testSlobOpers() {
	Record record;
	byte data[Limits::PAGE_SIZE];
	record.m_data = data;
	record.m_size = Limits::PAGE_SIZE;

	TableDefBuilder tb(1, "blob", "virtualTable");
	tb.addColumn("length", CT_INT);
	tb.addColumnS("content", CT_VARCHAR, Limits::MAX_REC_SIZE - 100, false, true, COLL_LATIN1);
	TableDef* tableDef = tb.getTableDef();
	// create Record
	{
		const char *blob = "Sanlu powdered milk";
		RecordOper::createSlobRecord(tableDef, &record, (const byte *)blob, strlen(blob), 100);
		RecordBuilder rb(tableDef, INVALID_ROW_ID, REC_VARLEN);
		rb.appendInt(100);
		rb.appendVarchar(blob);
		Record* trueRecord = rb.getRecord();
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, &record, trueRecord));
		freeRecord(trueRecord);
	}
	{
		const char *blob = 0;
		RecordOper::createSlobRecord(tableDef, &record, (const byte *)blob, 0, 0);
		RecordBuilder rb(tableDef, INVALID_ROW_ID, REC_VARLEN);
		rb.appendNull();
		rb.appendNull();
		Record* trueRecord = rb.getRecord();
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, &record, trueRecord));
		freeRecord(trueRecord);
	}
	SubRecord sr;
	byte srData[Limits::PAGE_SIZE];
	sr.m_data = srData;
	sr.m_size = Limits::PAGE_SIZE;
	u16 columns[] = {0, 1};
	sr.m_numCols = sizeof(columns) / sizeof(columns[0]);
	sr.m_columns = columns;
	// createSubRecord
	{
		u32 orgLen = 100;
		const char *blob = "apple king";
		RecordOper::createSlobSubRecordR(tableDef, &sr, (const byte *)blob, strlen(blob), orgLen);
		SubRecordBuilder srb(tableDef, REC_REDUNDANT, INVALID_ROW_ID);
		SubRecord* trueSr = srb.createSubRecordById("0 1", &orgLen, blob);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, &sr, trueSr));
		freeSubRecord(trueSr);
	}
	{
		const char *blob = 0;
		RecordOper::createSlobSubRecordR(tableDef, &sr, (const byte *)blob, blob ? strlen(blob) : 0, 0);
		SubRecordBuilder srb(tableDef, REC_REDUNDANT, INVALID_ROW_ID);
		SubRecord* trueSr = srb.createSubRecordById("0 1", 0, blob);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, &sr, trueSr));
		freeSubRecord(trueSr);
	}
	// extract
	{
		size_t orgLen;
		const char *blob = "Sanlu powdered milk";
		record.m_size = Limits::PAGE_SIZE;
		RecordOper::createSlobRecord(tableDef, &record, (const byte *)blob, strlen(blob), 100);
		size_t size;
		byte *data = RecordOper::extractSlobData(tableDef, &record, &size, &orgLen);
		CPPUNIT_ASSERT(size == strlen(blob));
		CPPUNIT_ASSERT(orgLen == 100);
		CPPUNIT_ASSERT(!memcmp(data, blob, size));
	}
	delete tableDef;
}
/**
 * 判断冗余格式记录指定列是否为NULL
 * 测试流程：
 *	case 1. 创建一条记录，2列为NULL，2列不为NULL
 *	case 2. 判断isNullR的结果是否也是如此
 */
void RecordTestCase::testIsNullR() {
	const char* title = "Philosophy";
	const int pages = 338;
	BookTable bookTable;
	const TableDef* tableDef = bookTable.getTableDef();
	RecordBuilder redundantBuilder(tableDef, 0, REC_REDUNDANT);

	redundantBuilder.appendChar(title)->appendNull()->appendInt(pages)->appendNull();
	Record *record = redundantBuilder.getRecord();
	CPPUNIT_ASSERT(!RecordOper::isNullR(tableDef, record, 0));
	CPPUNIT_ASSERT(RecordOper::isNullR(tableDef, record, 1));
	CPPUNIT_ASSERT(!RecordOper::isNullR(tableDef, record, 2));
	CPPUNIT_ASSERT(RecordOper::isNullR(tableDef, record, 3));
	freeRecord(record);
}

static void extractSubrecordFR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	SubrecExtractor* extractor = SubrecExtractor::createInst(NULL, tableDef
		, subRecord->m_numCols, subRecord->m_columns, REC_FIXLEN, REC_REDUNDANT);
	extractor->extract(record, subRecord);
	delete extractor;
}
/**
 * 测试SubrecExtractor接口
 */
void RecordTestCase::testSubrecExtractorFR() {
	doTestExtractSubRecordFR(extractSubrecordFR);
}

/**
 * FastExtractSubRecordFR
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列，子记录不包含NULL列
 *	case 3. 记录中包含NULL列，子记录包含NULL
 */
void RecordTestCase::testFastExtractSubRecordFR() {
	doTestExtractSubRecordFR(RecordOper::fastExtractSubRecordFR);
}


static void extractSubrecordVR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	SubrecExtractor* extractor = SubrecExtractor::createInst(NULL, tableDef
		, subRecord->m_numCols, subRecord->m_columns, REC_VARLEN, REC_REDUNDANT);

	extractor->extract(record, subRecord);
	delete extractor;
}

/**
 * 测试SubrecExtractor接口
 */
void RecordTestCase::testSubrecExtractorVR() {
	doTestExtractSubRecordVR(extractSubrecordVR);
}

/**
 * FastExtractSubRecordVR
 * 测试流程：
 *	case 1. 记录没有NULL列
 *	case 2. 记录中包含NULL列，子记录不包含NULL列
 *	case 3. 记录中包含NULL列，子记录包含NULL
 */
void RecordTestCase::testFastExtractSubRecordVR() {
	doTestExtractSubRecordVR(RecordOper::fastExtractSubRecordVR);
}
/**
 * IsFastCCComparable
 * 测试流程
 *	case 1. varchar列不能快速比较
 *	case 2. int列能快速比较
 *	case 3. nullable列能不能快速比较
 *	case 4. varchar + int 列能不能快速比较
 *	case 5. int + nullable列能不能快速比较
 */
void RecordTestCase::testIsFastCCComparable() {
	TableDefBuilder tb(1, "Olympic", "student");
	tb.addColumnS(STU_NAME, CT_VARCHAR, 11, false, false, COLL_LATIN1);
	tb.addColumn(STU_SNO, CT_INT, false);
	tb.addColumn(STU_AGE, CT_SMALLINT);
	tb.addColumnS(STU_SEX, CT_CHAR, 2, false, true, COLL_LATIN1);
	tb.addColumn(STU_CLASS, CT_MEDIUMINT, false);
	tb.addIndex("IDX_NAME", false, false, false, STU_NAME, 0, NULL);
	tb.addIndex("IDX_SNO", false, false, false, STU_SNO, 0, NULL);
	tb.addIndex("IDX_AGE", false, false, false, STU_AGE, 0, NULL);
	tb.addIndex("IDX_NAME_SNO", false, false, false, STU_NAME, 0, STU_SNO, 0, NULL);
	tb.addIndex("IDX_SNO_AGE", false, false, false, STU_SNO, 0, STU_AGE, 0, NULL);
	TableDef *tableDef = tb.getTableDef();
	u16 keyCols[128];
	u16 numCols = 128;
	numCols = 1;
	keyCols[0] = 0;	// varchar是不行的
	CPPUNIT_ASSERT(!RecordOper::isFastCCComparable(tableDef, tableDef->getIndexDef("IDX_NAME"), numCols, keyCols));
	keyCols[0] = 1;	// int是行的
	CPPUNIT_ASSERT(RecordOper::isFastCCComparable(tableDef, tableDef->getIndexDef("IDX_SNO"), numCols, keyCols));
	keyCols[0] = 2; // nullable也行
	CPPUNIT_ASSERT(RecordOper::isFastCCComparable(tableDef, tableDef->getIndexDef("IDX_AGE"), numCols, keyCols));

	numCols = 2;
	keyCols[0] = 0;
	keyCols[1] = 1;
	CPPUNIT_ASSERT(!RecordOper::isFastCCComparable(tableDef, tableDef->getIndexDef("IDX_NAME_SNO"), numCols, keyCols));
	keyCols[0] = 1;
	keyCols[1] = 2;
	CPPUNIT_ASSERT(RecordOper::isFastCCComparable(tableDef, tableDef->getIndexDef("IDX_SNO_AGE"), numCols, keyCols));
	CPPUNIT_ASSERT(!RecordOper::isFastCCComparable(tableDef, tableDef->getIndexDef("IDX_SNO_AGE"), 1, keyCols));

	delete tableDef;
}

void isSubRecordEqStu() {
	const char* sname = "huanhuan";
	const int age = 28;

	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	vector<SubRecordBuilder *> builders;
	builders.push_back(new SubRecordBuilder(tableDef, REC_REDUNDANT));
	builders.push_back(new SubRecordBuilder(tableDef, REC_FIXLEN));
	builders.push_back(new SubRecordBuilder(tableDef, KEY_COMPRESS));
	builders.push_back(new SubRecordBuilder(tableDef, KEY_NATURAL));
	builders.push_back(new SubRecordBuilder(tableDef, KEY_PAD));
	for (uint i = 0; i < builders.size(); ++i) {
		// 冗余格式
		{ // 相同
			const IndexDef *indexDef = tableDef->getIndexDef(1);
			SubRecord* key1 = builders[i]->createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
			SubRecord* key2 = builders[i]->createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key1, key2, indexDef));
			freeSubRecord(key1);
			freeSubRecord(key2);
		}
		{ // 不相同
			const IndexDef *indexDef = tableDef->getIndexDef(1);
			SubRecord* key1 = builders[i]->createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
			int largerAge = age + 1;
			SubRecord* key2 = builders[i]->createSubRecordByName(STU_NAME" "STU_AGE, sname, &largerAge);
			CPPUNIT_ASSERT(!RecordOper::isSubRecordEq(tableDef,key1, key2, indexDef));
			freeSubRecord(key1);
			freeSubRecord(key2);
		}
		{ // 不相同 - NULL
			const IndexDef *indexDef = tableDef->getIndexDef(1);
			SubRecord* key1 = builders[i]->createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
			SubRecord* key2 = builders[i]->createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
			CPPUNIT_ASSERT(!RecordOper::isSubRecordEq(tableDef,key1, key2, indexDef));
			freeSubRecord(key1);
			freeSubRecord(key2);
		}
		delete builders[i];
	}
}

void isSubRecordEqBook() {
	const char* title = "linux";
	const int pages = 338;

	BookTable bookTable;
	const TableDef* tableDef = bookTable.getTableDef();
	vector<SubRecordBuilder *> builders;
	builders.push_back(new SubRecordBuilder(tableDef, REC_REDUNDANT));
	builders.push_back(new SubRecordBuilder(tableDef, REC_FIXLEN));
	builders.push_back(new SubRecordBuilder(tableDef, KEY_COMPRESS));
	builders.push_back(new SubRecordBuilder(tableDef, KEY_NATURAL));
	builders.push_back(new SubRecordBuilder(tableDef, KEY_PAD));
	for (uint i = 0; i < builders.size(); ++i) {
		// 冗余格式
		{ // 相同
			const IndexDef *indexDef = tableDef->getIndexDef(6);
			SubRecord* key1 = builders[i]->createSubRecordByName(BOOK_TITLE" "BOOK_PAGES, title, &pages);
			SubRecord* key2 = builders[i]->createSubRecordByName(BOOK_TITLE" "BOOK_PAGES, title, &pages);
			CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,key1, key2, indexDef));
			freeSubRecord(key1);
			freeSubRecord(key2);
		}
		{ // 不相同
			const IndexDef *indexDef = tableDef->getIndexDef(6);
			SubRecord* key1 = builders[i]->createSubRecordByName(BOOK_TITLE" "BOOK_PAGES, title, &pages);
			int largerPages = pages + 1;
			SubRecord* key2 = builders[i]->createSubRecordByName(BOOK_TITLE" "BOOK_PAGES, title, &largerPages);
			CPPUNIT_ASSERT(!RecordOper::isSubRecordEq(tableDef,key1, key2, indexDef));
			freeSubRecord(key1);
			freeSubRecord(key2);
		}
		{ // 不相同 - NULL
			const IndexDef *indexDef = tableDef->getIndexDef(7);
			SubRecord* key1 = builders[i]->createSubRecordByName(BOOK_ISBN" "BOOK_PAGES, "isbn-0x001", &pages);
			SubRecord* key2 = builders[i]->createSubRecordByName(BOOK_ISBN" "BOOK_PAGES, NULL, &pages);
			CPPUNIT_ASSERT(!RecordOper::isSubRecordEq(tableDef,key1, key2, indexDef));
			freeSubRecord(key1);
			freeSubRecord(key2);
		}
		delete builders[i];
	}
}
/**
 * 测试子记录比较函数
 *	测试定长表和变长表子记录
 *	测试各种格式子记录
 *	测试NULL和非NULL情况
 */
void RecordTestCase::testIsSubRecordEq() {
	isSubRecordEqStu();
	isSubRecordEqBook();
};


void isRecordEqStu() {
	const char* sname = "huanhuan";
	int age = 28;
	int sno = 100;

	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	RecFormat formats[] = {REC_REDUNDANT, REC_VARLEN, REC_MYSQL};


	for (uint i = 0; i < sizeof(formats) / sizeof(formats[0]); ++i) {
		// 完全一样
		Record *rec1 = studentTable.createRecord(formats[i], sname, sno, age, "F", 1, 4.0f);
		Record *rec2 = studentTable.createRecord(formats[i], sname, sno, age, "F", 1, 4.0f);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, rec1, rec2));
		freeRecord(rec1);
		freeRecord(rec2);
		// sno不一样
		rec1 = studentTable.createRecord(formats[i], sname, sno, age, "F", 1, 4.0f);
		rec2 = studentTable.createRecord(formats[i], sname, sno + 1, age, "F", 1, 4.0f);
		CPPUNIT_ASSERT(!RecordOper::isRecordEq(tableDef, rec1, rec2));
		freeRecord(rec1);
		freeRecord(rec2);

		// age不一样， rec2的age为NULL
		{
			rec1 = studentTable.createRecord(formats[i], sname, sno, age, "F", 1, 4.0f);
			RecordBuilder rb(tableDef, 0, formats[i]);
			rb.appendVarchar(sname);
			rb.appendInt(sno);
			rb.appendNull();
			rb.appendChar("F");
			rb.appendMediumInt(1);
			rb.appendFloat(4.0f);
			rb.appendBigInt(0);
			rec2 = rb.getRecord(tableDef->m_maxRecSize);
			CPPUNIT_ASSERT(!RecordOper::isRecordEq(tableDef, rec1, rec2));
			freeRecord(rec1);
			freeRecord(rec2);
		}

		// 有NULL列，但是相等
		{
			RecordBuilder rb(tableDef, 0, formats[i]);
			rb.appendVarchar(sname)->appendNull()->appendNull()->appendNull()->appendNull()
				->appendNull()->appendBigInt(0);
			rec1 = rb.getRecord(tableDef->m_maxRecSize);
			rec2 = rb.getRecord(tableDef->m_maxRecSize);
			CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, rec1, rec2));
			freeRecord(rec1);
			freeRecord(rec2);
		}
	}

}


void isRecordEqBook() {
	const char* title = "linuxapp";
	const char* isbn = "111-222";
	const int pages = 378;
	const int price = 333;

	BookTable bookTable;
	const TableDef* tableDef = bookTable.getTableDef();
	RecFormat formats[] = {REC_REDUNDANT, REC_FIXLEN, REC_MYSQL};


	for (uint i = 0; i < sizeof(formats) / sizeof(formats[0]); ++i) {
		// 完全一样
		Record *rec1 = bookTable.createRecord(title, isbn, pages, price, formats[i]);
		Record *rec2 = bookTable.createRecord(title, isbn, pages, price, formats[i]);
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, rec1, rec2));
		freeRecord(rec1);
		freeRecord(rec2);
		// sno不一样
		rec1 = bookTable.createRecord(title, isbn, pages, price, formats[i]);
		rec2 = bookTable.createRecord(title, isbn, pages, price + 1, formats[i]);
		CPPUNIT_ASSERT(!RecordOper::isRecordEq(tableDef, rec1, rec2));
		freeRecord(rec1);
		freeRecord(rec2);

		// age不一样， rec2的isbn为NULL
		{
			rec1 = bookTable.createRecord(title, isbn, pages, price, formats[i]);
			RecordBuilder rb(tableDef, 0, formats[i]);
			rb.appendChar(title);
			rb.appendNull();
			rb.appendInt(pages);
			rb.appendInt(price);
			rec2 = rb.getRecord(tableDef->m_maxRecSize);
			CPPUNIT_ASSERT(!RecordOper::isRecordEq(tableDef, rec1, rec2));
			freeRecord(rec1);
			freeRecord(rec2);
		}

		// 有NULL列，但是相等
		{
			RecordBuilder rb(tableDef, 0, formats[i]);
			rb.appendChar(title)->appendNull()->appendNull()->appendNull();
			rec1 = rb.getRecord(tableDef->m_maxRecSize);
			rec2 = rb.getRecord(tableDef->m_maxRecSize);
			CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, rec1, rec2));
			freeRecord(rec1);
			freeRecord(rec2);
		}
	}
	{ // 大对象内容比较
		TableDefBuilder tdb(0, "test", "test");
		tdb.addColumn("a", CT_MEDIUMLOB);
		TableDef *tableDef = tdb.getTableDef();
		RedRecord rec1(tableDef);
		RedRecord rec2(tableDef);
		{ // 内容不同
			const char *str1 = "hello world";
			const char *str2 = "hellO world";
			rec1.writeLob(0, (byte *)str1, strlen(str1));
			rec2.writeLob(0, (byte *)str2, strlen(str2));
			CPPUNIT_ASSERT(!RecordOper::isRecordEq(tableDef, rec1.getRecord(), rec2.getRecord()));
			rec1.setNull(0);
			rec2.setNull(0);
		}
		{ // 内容相同，但是地址不同
			const char *str1 = "hello world";
			char str2[] = "hello world";
			rec1.writeLob(0, (byte *)str1, strlen(str1));
			rec2.writeLob(0, (byte *)str2, strlen(str2));
			CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, rec1.getRecord(), rec2.getRecord()));
			rec1.setNull(0);
			rec2.setNull(0);
		}
		delete tableDef;
	}

}
/**
 * 测试记录比较函数
 *	测试定长表和变长表记录
 *	测试各种格式子记录
 *	测试NULL和非NULL情况
 */
void RecordTestCase::testIsRecordEq() {
	isRecordEqStu();
	isRecordEqBook();
}

/**
 * 测试流程：
 *	case 1. key没有NULL列
 *	case 2. key列顺序不同于记录列顺序
 *	case 3. key中包含NULL列
 *	case 4. key的列全NULL
 */
void RecordTestCase::testGetKeySizeCN() {
	const char* sname = "ntes' ntse";
	const int age = 28;
	StudentTable studentTable;
	const TableDef* tableDef = studentTable.getTableDef();
	
	SubRecordBuilder nb(tableDef, KEY_NATURAL);
	SubRecordBuilder cb(tableDef, KEY_COMPRESS);
	{
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(1);
		SubRecord* comKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		SubRecord* natKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(natKey->m_size == RecordOper::getKeySizeCN(tableDef, indexDef, comKey));
		freeSubRecord(natKey);
		freeSubRecord(comKey);
	}
	{ // 颠倒列顺序
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(2);
		SubRecord* comKey = cb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		SubRecord* natKey = nb.createSubRecordByName(STU_AGE" "STU_NAME, &age, sname);
		CPPUNIT_ASSERT(natKey->m_size == RecordOper::getKeySizeCN(tableDef, indexDef, comKey));
		freeSubRecord(natKey);
		freeSubRecord(comKey);
	}
	{ // NULL
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(1);
		SubRecord* comKey = cb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		SubRecord* natKey = nb.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(natKey->m_size == RecordOper::getKeySizeCN(tableDef, indexDef, comKey));
		freeSubRecord(natKey);
		freeSubRecord(comKey);
	}
	{ // NULL ALL
		const IndexDef *indexDef = studentTable.getTableDef()->getIndexDef(3);
		SubRecord* comKey = cb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		SubRecord* natKey = nb.createSubRecordByName(STU_SNO" "STU_AGE, NULL, NULL);
		CPPUNIT_ASSERT(natKey->m_size == RecordOper::getKeySizeCN(tableDef, indexDef, comKey));
		freeSubRecord(natKey);
		freeSubRecord(comKey);;
	}
}

/**
 * 回归测试，测试如下sql序列导致的coredump
 * create table t1 ( a1 int not null, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int ) engine=ntse;
 * insert into t1 values (1, 1, 1, 1, 1, 1, 1, 1, 1);
 * select a1,a2,a3,a4,a5,a6,a7,a8,a9 from t1;
 */
void RecordTestCase::testBug2580() {
	TableDefBuilder tb(1, "Olympic", "bug2580");
	tb.addColumn("a1", CT_INT, false);
	tb.addColumn("a2", CT_INT);
	tb.addColumn("a3", CT_INT);
	tb.addColumn("a4", CT_INT);
	tb.addColumn("a5", CT_INT);
	tb.addColumn("a6", CT_INT);
	tb.addColumn("a7", CT_INT);
	tb.addColumn("a8", CT_INT);
	tb.addColumn("a9", CT_INT);
	TableDef *tableDef = tb.getTableDef();

	RecordBuilder builder(tableDef, INVALID_ROW_ID, REC_FIXLEN);
	for (int i = 0; i < 9; ++i)
		builder.appendInt(1);

	Record *record = builder.getRecord();
	SubRecordBuilder sb(tableDef, REC_REDUNDANT, INVALID_ROW_ID);
	SubRecord *subRec = sb.createEmptySbById(tableDef->m_maxRecSize, "0 1 2 3 4 5 6 7 8");
	RecordOper::extractSubRecordFR(tableDef, record, subRec);
	int v = 1;
	SubRecord *trueSubRec = sb.createSubRecordById("0 1 2 3 4 5 6 7 8", &v, &v, &v, &v, &v, &v, &v, &v, &v);
	CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, subRec, trueSubRec));
	freeSubRecord(trueSubRec);
	freeSubRecord(subRec);
	freeRecord(record);
	delete tableDef;
}

void doTestInitEmptyRecord(const TableDef *tableDef) {
	byte buf[Limits::PAGE_SIZE];
	Record rec;
	rec.m_data = buf;
	rec.m_size = Limits::PAGE_SIZE;
	RowId rid = System::clockCycles();
	RecordOper::initEmptyRecord(&rec, tableDef, rid, tableDef->m_recFormat);
	CPPUNIT_ASSERT(rec.m_rowId == rid);
	CPPUNIT_ASSERT(rec.m_format == tableDef->m_recFormat);
	uint size = tableDef->m_bmBytes;
	for (u16 cno = 0; cno < tableDef->m_numCols; ++cno) {
		ColumnDef *columnDef = tableDef->m_columns[cno];
		if (columnDef->m_nullable) {
			CPPUNIT_ASSERT(BitmapOper::isSet(rec.m_data, tableDef->m_bmBytes << 3, columnDef->m_nullBitmapOffset));
		} else {
			/*for (int i = 0; i < columnDef->m_size; ++i)
				CPPUNIT_ASSERT(*(rec.m_data + size + i) == 0);*/
			if (columnDef->isFixlen()) {
				size += columnDef->m_size;
			} else {
				size += columnDef->m_lenBytes;
			}
		}
	}
	CPPUNIT_ASSERT(rec.m_size == (tableDef->m_recFormat == REC_FIXLEN ? tableDef->m_maxRecSize : size));
}
/**
 * RecordOper::initEmptyRecord功能
 */
void RecordTestCase::testInitEmptyRecord() {
	StudentTable stu;
	doTestInitEmptyRecord(stu.getTableDef());
	BookTable book;
	doTestInitEmptyRecord(book.getTableDef());
}

void RecordTestCase::testColList() {
	MemoryContext *mc = new MemoryContext(Limits::PAGE_SIZE, 1);
	// merge and hasIntersect
	{
		u16 cols1[3] = {1, 2, 4};
		u16 cols2[2] = {2, 5};
		u16 cols3[4] = {1, 2, 4, 5};
		ColList c1(sizeof(cols1) / sizeof(cols1[0]), cols1);
		ColList c2(sizeof(cols2) / sizeof(cols2[0]), cols2);
		ColList c3 = c1.merge(mc, c2);
		CPPUNIT_ASSERT(c3 == ColList(sizeof(cols3) / sizeof(cols3[0]), cols3));
		CPPUNIT_ASSERT(c1.hasIntersect(c2) && c2.hasIntersect(c1));
	}
	{
		u16 cols1[3] = {1, 2, 5};
		u16 cols2[2] = {2, 4};
		u16 cols3[4] = {1, 2, 4, 5};
		ColList c1(sizeof(cols1) / sizeof(cols1[0]), cols1);
		ColList c2(sizeof(cols2) / sizeof(cols2[0]), cols2);
		ColList c3 = c1.merge(mc, c2);
		CPPUNIT_ASSERT(c3 == ColList(sizeof(cols3) / sizeof(cols3[0]), cols3));
		CPPUNIT_ASSERT(c1.hasIntersect(c2) && c2.hasIntersect(c1));
	}
	{
		u16 cols1[3] = {1, 3, 5};
		u16 cols2[2] = {2, 4};
		u16 cols3[5] = {1, 2, 3, 4, 5};
		ColList c1(sizeof(cols1) / sizeof(cols1[0]), cols1);
		ColList c2(sizeof(cols2) / sizeof(cols2[0]), cols2);
		ColList c3 = c1.merge(mc, c2);
		CPPUNIT_ASSERT(c3 == ColList(sizeof(cols3) / sizeof(cols3[0]), cols3));
		CPPUNIT_ASSERT(!c1.hasIntersect(c2) && !c2.hasIntersect(c1));
	}
	{
		u16 cols1[2] = {1, 2};
		ColList c1(sizeof(cols1) / sizeof(cols1[0]), cols1);
		ColList c2(0, NULL);
		ColList c3 = c1.merge(mc, c2);
		CPPUNIT_ASSERT(c3 == c1);
		CPPUNIT_ASSERT(!c1.hasIntersect(c2) && !c2.hasIntersect(c1));
	}
	{
		u16 cols1[2] = {1, 2};
		ColList c1(sizeof(cols1) / sizeof(cols1[0]), cols1);
		ColList c2(0, NULL);
		ColList c3 = c2.merge(mc, c1);
		CPPUNIT_ASSERT(c3 == c1);
		CPPUNIT_ASSERT(!c1.hasIntersect(c2) && !c2.hasIntersect(c1));
	}
	{
		ColList c1(0, NULL);
		ColList c2(0, NULL);
		ColList c3 = c1.merge(mc, c2);
		CPPUNIT_ASSERT(c3 == c1);
		CPPUNIT_ASSERT(!c1.hasIntersect(c2) && !c2.hasIntersect(c1));
	}
	// except
	{
		u16 cols1[3] = {1, 2, 4};
		u16 cols2[2] = {2, 5};
		u16 cols3[2] = {1, 4};
		ColList c1(sizeof(cols1) / sizeof(cols1[0]), cols1);
		ColList c2(sizeof(cols2) / sizeof(cols2[0]), cols2);
		ColList c3 = c1.except(mc, c2);
		CPPUNIT_ASSERT(c3 == ColList(sizeof(cols3) / sizeof(cols3[0]), cols3));
	}
	{
		u16 cols1[3] = {1, 2, 4};
		u16 cols2[2] = {1, 2};
		u16 cols3[1] = {4};
		ColList c1(sizeof(cols1) / sizeof(cols1[0]), cols1);
		ColList c2(sizeof(cols2) / sizeof(cols2[0]), cols2);
		ColList c3 = c1.except(mc, c2);
		CPPUNIT_ASSERT(c3 == ColList(sizeof(cols3) / sizeof(cols3[0]), cols3));
	}
	mc->reset();
	delete mc;
}

void RecordTestCase::testSubRecordSerializationMNR() {
	PaperTable paperTable;
	const TableDef *tableDef = paperTable.getTableDef();

	// 创建一个变长格式的不含NULL大对象记录
	const Record *redrec1 = paperTable.createRecord(1, "C.Mohan", "LOGICAL REDO", "I've written a paper about logical redo in RMDB", 10000, 20, REC_REDUNDANT, INVALID_ROW_ID);

	// 创建一个变长格式的不含NULL大对象记录
	const Record *sqlrec1 = paperTable.createRecord(3, "C.Mohan", "KVL LOCK", "I've written a paper about KVL-LOCK about RMDB", 10002, 20, REC_REDUNDANT, INVALID_ROW_ID);
	// 创建一个变长格式的含NULL大对象记录
	const Record *sqlrec2 = paperTable.createRecord(4, "C.Mohan", NULL, NULL, 10003, 20, REC_REDUNDANT, INVALID_ROW_ID);

	MemoryContext *mcStream = new MemoryContext(Limits::PAGE_SIZE, 2);
	MemoryContext *mc = new MemoryContext(Limits::PAGE_SIZE, 2);

	// 针对上面的每一种记录，产生子记录进行序列化反序列化
	{	// 测试函大对象的REDUNDANT格式的序列化
		SubRecordBuilder srb(tableDef, REC_REDUNDANT, INVALID_ROW_ID);
		SubRecord *sb = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 2 3 5");
		byte *data = sb->m_data;
		sb->m_data = redrec1->m_data;
		size_t size = RecordOper::getSubRecordSerializeSize(tableDef, sb, false);

		u64 savePoint1 = mcStream->setSavepoint();
		byte *buf = (byte*)mcStream->alloc(Limits::PAGE_SIZE);
		Stream s(buf, size);
		Stream us(buf, Limits::PAGE_SIZE);
		RecordOper::serializeSubRecordMNR(&s, tableDef, sb, false);

		assert(s.getSize() == size);
		
		u64 savePoint2 = mc->setSavepoint();
		SubRecord *ret = RecordOper::unserializeSubRecordMNR(&us, tableDef, mc);

		// 对于REC_REDUNDANT类型，只需要验证记录本身读取正确
		assert(ret->m_format == REC_REDUNDANT);
		assert(sb->m_size = ret->m_size);
		assert(sb->m_numCols = ret->m_numCols);
		assert(memcmp(sb->m_columns, ret->m_columns, sizeof(sb->m_columns[0]) * sb->m_numCols) == 0);
		// 未读取的第4号属性不一样，其他都应该一样
		{
			u64 id1, id2;
			id1 = RedRecord::readBigInt(tableDef, sb->m_data, PAPER_ID_COLNO);
			id2 = RedRecord::readBigInt(tableDef, ret->m_data, PAPER_ID_COLNO);
			assert(id1 == id2);
			size_t size1, size2;
			byte *b1, *b2;
			RedRecord::readVarchar(tableDef, sb->m_data, PAPER_AUTHOR_COLNO, (void**)&b1, &size1);
			RedRecord::readVarchar(tableDef, ret->m_data, PAPER_AUTHOR_COLNO, (void**)&b2, &size2);
			assert(size1 == size2 && memcmp((void*)b1, (void*)b2, size1) == 0);

			RedRecord::readLob(tableDef, sb->m_data, PAPER_ABSTRACT_COLNO, (void**)&b1, &size1);
			RedRecord::readLob(tableDef, ret->m_data, PAPER_ABSTRACT_COLNO, (void**)&b2, &size2);
			assert(size1 == size2 && memcmp((void*)b1, (void*)b2, size1) == 0);

			RedRecord::readLob(tableDef, sb->m_data, PAPER_CONTENT_COLNO, (void**)&b1, &size1);
			RedRecord::readLob(tableDef, ret->m_data, PAPER_CONTENT_COLNO, (void**)&b2, &size2);
			assert(size1 == size2 && memcmp((void*)b1, (void*)b2, size1) == 0);

			int page1, page2;
			page1 = RedRecord::readInt(tableDef, sb->m_data, PAPER_PAGES_COLNO);
			page2 = RedRecord::readInt(tableDef, ret->m_data, PAPER_PAGES_COLNO);
			assert(page1 == page2);
		}

		// 清空使用的内存
		sb->m_data = data;
		freeSubRecord(sb);
		mc->resetToSavepoint(savePoint2);
		mcStream->resetToSavepoint(savePoint1);
	}

	{	// 测试函大对象的MYSQL格式的序列化
		SubRecordBuilder srb(tableDef, REC_REDUNDANT, INVALID_ROW_ID);
		SubRecord *sb = srb.createEmptySbById(tableDef->m_maxRecSize, "0 1 2 3 5");
		byte *data = sb->m_data;
		sb->m_data = redrec1->m_data;
		sb->m_format = REC_MYSQL;
		size_t size = RecordOper::getSubRecordSerializeSize(tableDef, sb, true);

		u64 savePoint1 = mcStream->setSavepoint();
		byte *buf = (byte*)mcStream->alloc(Limits::PAGE_SIZE);
		Stream s(buf, size);
		Stream us(buf, Limits::PAGE_SIZE);
		Stream us1(buf, Limits::PAGE_SIZE);
		RecordOper::serializeSubRecordMNR(&s, tableDef, sb, true);

		assert(s.getSize() == size);

		u64 savePoint2 = mc->setSavepoint();
		SubRecord *ret = RecordOper::unserializeSubRecordMNR(&us, tableDef, mc);

		assert(sb->m_size = ret->m_size);
		assert(sb->m_numCols = ret->m_numCols);
		assert(memcmp(sb->m_columns, ret->m_columns, sizeof(sb->m_columns[0]) * sb->m_numCols) == 0);
		// 逐一读取属性进行比较
		assert(RedRecord::readBigInt(tableDef, sb->m_data, PAPER_ID_COLNO) == RedRecord::readBigInt(tableDef, ret->m_data, PAPER_ID_COLNO));
		assert(RedRecord::readInt(tableDef, sb->m_data, PAPER_PAGES_COLNO) == RedRecord::readInt(tableDef, ret->m_data, PAPER_PAGES_COLNO));

		char *author1, *author2;
		size_t size1, s2;
		RedRecord::readVarchar(tableDef, sb->m_data, PAPER_AUTHOR_COLNO, (void**)&author1, &size1);
		RedRecord::readVarchar(tableDef, ret->m_data, PAPER_AUTHOR_COLNO, (void**)&author2, &s2);
		assert(size1 == s2 && memcmp(author1, author2, size1) == 0);

		char *abs1, *abs2;
		RedRecord::readLob(tableDef, sb->m_data, PAPER_ABSTRACT_COLNO, (void**)&abs1, &size1);
		RedRecord::readLob(tableDef, ret->m_data, PAPER_ABSTRACT_COLNO, (void**)&abs2, &s2);
		assert(size1 == s2 && memcmp(abs1, abs2, size1) == 0);

		char *cont1, *cont2;
		RedRecord::readLob(tableDef, sb->m_data, PAPER_CONTENT_COLNO, (void**)&cont1, &size1);
		RedRecord::readLob(tableDef, ret->m_data, PAPER_CONTENT_COLNO, (void**)&cont2, &s2);
		assert(size1 == s2 && memcmp(cont1, cont2, size1) == 0);
	
		//////////////////////////////////////////////////////////////////
		// 测试另一个接口
		size = RecordOper::getSubRecordSerializeSize(tableDef, sb, true, true);
		Stream s1(buf, size);
		RecordOper::serializeSubRecordMNR(&s1, tableDef, sb, true, true);
		assert(s1.getSize() == size);

		byte *out = (byte*)mc->alloc(tableDef->m_maxRecSize);
		RecordOper::unserializeSubRecordMNR(&us1, tableDef, sb->m_numCols, sb->m_columns, out);
		// 逐一读取属性进行比较
		assert(RedRecord::readBigInt(tableDef, sb->m_data, PAPER_ID_COLNO) == RedRecord::readBigInt(tableDef, out, PAPER_ID_COLNO));
		assert(RedRecord::readInt(tableDef, sb->m_data, PAPER_PAGES_COLNO) == RedRecord::readInt(tableDef, out, PAPER_PAGES_COLNO));

		RedRecord::readVarchar(tableDef, sb->m_data, PAPER_AUTHOR_COLNO, (void**)&author1, &size1);
		RedRecord::readVarchar(tableDef, out, PAPER_AUTHOR_COLNO, (void**)&author2, &s2);
		assert(size1 == s2 && memcmp(author1, author2, size1) == 0);

		RedRecord::readLob(tableDef, sb->m_data, PAPER_ABSTRACT_COLNO, (void**)&abs1, &size1);
		RedRecord::readLob(tableDef, out, PAPER_ABSTRACT_COLNO, (void**)&abs2, &s2);
		assert(size1 == s2 && memcmp(abs1, abs2, size1) == 0);

		RedRecord::readLob(tableDef, sb->m_data, PAPER_CONTENT_COLNO, (void**)&cont1, &size1);
		RedRecord::readLob(tableDef, out, PAPER_CONTENT_COLNO, (void**)&cont2, &s2);
		assert(size1 == s2 && memcmp(cont1, cont2, size1) == 0);


		// 清空使用的内存
		sb->m_data = data;
		freeSubRecord(sb);
		mc->resetToSavepoint(savePoint2);
		mcStream->resetToSavepoint(savePoint1);
	}

	freeRecord((Record*)redrec1);
	freeRecord((Record*)sqlrec1);
	freeRecord((Record*)sqlrec2);
	delete mc;
	delete mcStream;
}

void RecordTestCase::testUpperMysqlRow() {
	// 测试大对象列及前缀索引及超长字段
	const u64 id = 1;
	const char *name = "blog name";
	const char *author = "lostbag";
	const char *abs = "blog is exist";
	const char *content = "blog is not exist";
	const u64 pubtime = 5;
	UserBlogTable blogTable;
	const TableDef *tableDef = blogTable.getTableDef();
	
	
	{	// 测试上层格式转下层格式
		// 测试没有NULL

		// 构建MYSQL 上层格式记录
		RecordBuilder upRb(tableDef, 0, REC_UPPMYSQL);
		RecordBuilder engineRb(tableDef, 0, REC_MYSQL);
		upRb.appendBigInt(id);
		upRb.appendVarchar(name);
		upRb.appendChar(author);
		upRb.appendVarchar(abs);
		upRb.appendSmallLob((const byte*)content);
		upRb.appendBigInt(pubtime);
		Record *upRecord = upRb.getRecord();

		Record *engineRecord = engineRb.createEmptyRecord(0, REC_MYSQL, tableDef->m_maxRecSize);

		RecordOper::convertRecordMUpToEngine(tableDef, upRecord, engineRecord);

		engineRb.appendBigInt(id);
		engineRb.appendVarchar(name);
		engineRb.appendChar(author);
		engineRb.appendSmallLob((const byte*)abs);
		engineRb.appendSmallLob((const byte*)content);
		engineRb.appendBigInt(pubtime);
		Record *trueRecord = engineRb.getRecord();


		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, trueRecord, engineRecord));

		// 测试子记录的转换
		memset(engineRecord->m_data, 0, tableDef->m_maxRecSize);
 		u16 columns[6]  = {0, 1, 2, 3, 4, 5};
 		SubRecord upSubRecord(REC_UPPMYSQL, 6, columns, upRecord->m_data, tableDef->m_maxMysqlRecSize);
		SubRecord engineSubRecord(REC_MYSQL, 6, columns, engineRecord->m_data, tableDef->m_maxRecSize);

		RecordOper::convertSubRecordMUpToEngine(tableDef, &upSubRecord, &engineSubRecord);

		SubRecord trueSubRecord(REC_MYSQL, 6, columns, trueRecord->m_data, tableDef->m_maxRecSize);

		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, &trueSubRecord, &engineSubRecord));

		freeRecord(upRecord);
		freeRecord(engineRecord);
		freeRecord(trueRecord);
	}

	{	// 测试上层格式转下层格式
		// 测试有NULL

		// 构建MYSQL 上层格式记录
		RecordBuilder upRb(tableDef, 0, REC_UPPMYSQL);
		RecordBuilder engineRb(tableDef, 0, REC_MYSQL);
		upRb.appendBigInt(id);
		upRb.appendVarchar(name);
		upRb.appendChar(author);
		upRb.appendNull();
		upRb.appendSmallLob((const byte*)content);
		upRb.appendBigInt(pubtime);
		Record *upRecord = upRb.getRecord();

		Record *engineRecord = engineRb.createEmptyRecord(0, REC_MYSQL, tableDef->m_maxRecSize);

		RecordOper::convertRecordMUpToEngine(tableDef, upRecord, engineRecord);

		engineRb.appendBigInt(id);
		engineRb.appendVarchar(name);
		engineRb.appendChar(author);
		engineRb.appendNull();
		engineRb.appendSmallLob((const byte*)content);
		engineRb.appendBigInt(pubtime);
		Record *trueRecord = engineRb.getRecord();


		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, trueRecord, engineRecord));

		// 测试子记录的转换
		memset(engineRecord->m_data, 0, tableDef->m_maxRecSize);
		u16 columns[6]  = {0, 1, 2, 3, 4, 5};
		SubRecord upSubRecord(REC_UPPMYSQL, 6, columns, upRecord->m_data, tableDef->m_maxMysqlRecSize);
		SubRecord engineSubRecord(REC_MYSQL, 6, columns, engineRecord->m_data, tableDef->m_maxRecSize);

		RecordOper::convertSubRecordMUpToEngine(tableDef, &upSubRecord, &engineSubRecord);

		SubRecord trueSubRecord(REC_MYSQL, 6, columns, trueRecord->m_data, tableDef->m_maxRecSize);

		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, &trueSubRecord, &engineSubRecord));

		freeRecord(upRecord);
		freeRecord(engineRecord);
		freeRecord(trueRecord);
	}

	{	// 测试下层格式转上层格式
		// 测试没有NULL
		RecordBuilder upRb(tableDef, 0, REC_UPPMYSQL);
		RecordBuilder engineRb(tableDef, 0, REC_MYSQL);
		engineRb.appendBigInt(id);
		engineRb.appendVarchar(name);
		engineRb.appendChar(author);
		engineRb.appendSmallLob((const byte*)abs);
		engineRb.appendSmallLob((const byte*)content);
		engineRb.appendBigInt(pubtime);
		Record *engineRecord = engineRb.getRecord();

		Record *upRecord = engineRb.createEmptyRecord(0, REC_UPPMYSQL, tableDef->m_maxMysqlRecSize);

		RecordOper::convertRecordMEngineToUp(tableDef, engineRecord, upRecord);

		upRb.appendBigInt(id);
		upRb.appendVarchar(name);
		upRb.appendChar(author);
		upRb.appendVarchar(abs);
		upRb.appendSmallLob((const byte*)content);
		upRb.appendBigInt(pubtime);
		Record *trueRecord = upRb.getRecord();

		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, trueRecord, upRecord));

		// 测试子记录的转换
		memset(upRecord->m_data, 0, tableDef->m_maxMysqlRecSize);
		u16 columns[6]  = {0, 1, 2, 3, 4, 5};
		SubRecord upSubRecord(REC_UPPMYSQL, 6, columns, upRecord->m_data, tableDef->m_maxMysqlRecSize);
		SubRecord engineSubRecord(REC_MYSQL, 6, columns, engineRecord->m_data, tableDef->m_maxRecSize);

		RecordOper::convertSubRecordMEngineToUp(tableDef, &engineSubRecord, &upSubRecord);

		SubRecord trueSubRecord(REC_UPPMYSQL, 6, columns, trueRecord->m_data, tableDef->m_maxMysqlRecSize);

		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, &trueSubRecord, &upSubRecord));


		freeRecord(upRecord);
		freeRecord(engineRecord);
		freeRecord(trueRecord);			 
	}	

	{	// 测试下层格式转上层格式
		// 测试有NULL
		RecordBuilder upRb(tableDef, 0, REC_UPPMYSQL);
		RecordBuilder engineRb(tableDef, 0, REC_MYSQL);
		engineRb.appendBigInt(id);
		engineRb.appendVarchar(name);
		engineRb.appendChar(author);
		engineRb.appendNull();
		engineRb.appendSmallLob((const byte*)content);
		engineRb.appendBigInt(pubtime);
		Record *engineRecord = engineRb.getRecord();

		Record *upRecord = engineRb.createEmptyRecord(0, REC_UPPMYSQL, tableDef->m_maxMysqlRecSize);

		RecordOper::convertRecordMEngineToUp(tableDef, engineRecord, upRecord);

		upRb.appendBigInt(id);
		upRb.appendVarchar(name);
		upRb.appendChar(author);
		upRb.appendNull();
		upRb.appendSmallLob((const byte*)content);
		upRb.appendBigInt(pubtime);
		Record *trueRecord = upRb.getRecord();

		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, trueRecord, upRecord));

		// 测试子记录的转换
		memset(upRecord->m_data, 0, tableDef->m_maxMysqlRecSize);
		u16 columns[6]  = {0, 1, 2, 3, 4, 5};
		SubRecord upSubRecord(REC_UPPMYSQL, 6, columns, upRecord->m_data, tableDef->m_maxMysqlRecSize);
		SubRecord engineSubRecord(REC_MYSQL, 6, columns, engineRecord->m_data, tableDef->m_maxRecSize);

		RecordOper::convertSubRecordMEngineToUp(tableDef, &engineSubRecord, &upSubRecord);

		SubRecord trueSubRecord(REC_UPPMYSQL, 6, columns, trueRecord->m_data, tableDef->m_maxMysqlRecSize);

		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, &trueSubRecord, &upSubRecord));

		freeRecord(upRecord);
		freeRecord(engineRecord);
		freeRecord(trueRecord);			 
	}	


}

//////////////////////////////////////////////////////////////////////////


class DoubleInt {
public:
	DoubleInt() {
		TableDefBuilder tb(1, "Olympic", "DoubleInt");
		tb.addColumn("a", CT_INT);
		tb.addColumn("b", CT_INT);
		tb.addIndex("primarykey", true, true, false, "a", 0, NULL);
		m_tableDef = tb.getTableDef();
	}
	Record* createRecord(int a, int b, RecFormat format) const {
		RecordBuilder rb(m_tableDef, 0, format);
		rb.appendInt(a);
		rb.appendInt(b);
		return rb.getRecord(m_tableDef->m_maxRecSize);
	}

	~DoubleInt() {
		delete m_tableDef;
	}
	const TableDef* getTableDef() const {
		return m_tableDef;
	}
private:
	TableDef* m_tableDef;
};


class DoubleIntVarchar {
public:
	DoubleIntVarchar() {
		TableDefBuilder tb(1, "Olympic", "DoubleIntVarchar");
		tb.addColumn("a", CT_INT);
		tb.addColumn("b", CT_INT);
		tb.addColumnS("c", CT_VARCHAR, 200);
		tb.addIndex("primarykey", true, true, false, "a", 0, NULL);
		m_tableDef = tb.getTableDef();
	}
	Record* createRecord(int a, int b, const char *c, RecFormat format) const {
		RecordBuilder rb(m_tableDef, 0, format);
		rb.appendInt(a);
		rb.appendInt(b);
		if (!c)
			rb.appendNull();
		else
			rb.appendVarchar(c);
		return rb.getRecord(m_tableDef->m_maxRecSize);
	}

	~DoubleIntVarchar() {
		delete m_tableDef;
	}
	const TableDef* getTableDef() const {
		return m_tableDef;
	}
private:
	TableDef* m_tableDef;
};

const char* RecordBigTest::getName() {
	return "Record operations performance test";
}

const char* RecordBigTest::getDescription() {
	return "Test record performance ";
}

bool RecordBigTest::isBig() {
	return true;
}

/** 测试ExtractSubRecordVR性能 */
void RecordBigTest::testExtractSubRecordVR() {
	DoubleIntVarchar div;
	const TableDef *tabDef = div.getTableDef();
	Record *rec = div.createRecord(1, 100, "hello world", REC_VARLEN);
	SubRecordBuilder rb(tabDef, REC_REDUNDANT);
	SubRecord *subRec = rb.createEmptySbByName(tabDef->m_maxRecSize, "a b");

	const int repeat = 1000000;
	u64 before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::extractSubRecordVR(tabDef, rec, subRec);
	}
	u64 after = System::clockCycles();
	cout << endl << "extractSubRecordVR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;
	freeRecord(rec);
	freeSubRecord(subRec);
}

/** 测试ExtractSubRecordVR性能 */
void RecordBigTest::testExtractSubRecordFR() {
	DoubleInt di;
	const TableDef *tabDef  = di.getTableDef();
	Record *rec = di.createRecord(1, 100, REC_FIXLEN);
	SubRecordBuilder rb(tabDef, REC_REDUNDANT);
	SubRecord *subRec = rb.createEmptySbByName(tabDef->m_maxRecSize, "a b");
	const int repeat = 1000000;
	u64 before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::extractSubRecordFR(tabDef, rec, subRec);
	}
	u64 after = System::clockCycles();
	cout << endl << "extractSubRecordFR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;

	freeRecord(rec);
	freeSubRecord(subRec);
}

/** 测试FastExtractSubRecordFR性能 */
void RecordBigTest::testFastExtractSubRecordFR() {
	DoubleInt di;
	const TableDef *tabDef  = di.getTableDef();
	Record *rec = di.createRecord(1, 100, REC_FIXLEN);
	SubRecordBuilder rb(tabDef, REC_REDUNDANT);
	SubRecord *subRec = rb.createEmptySbByName(tabDef->m_maxRecSize, "a b");
	const int repeat = 1000000;
	u64 before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::fastExtractSubRecordFR(tabDef, rec, subRec);
	}
	u64 after = System::clockCycles();
	cout << endl << "fastExtractSubRecordFR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;

	freeRecord(rec);
	freeSubRecord(subRec);
}
/** 测试FastExtractSubRecordVR性能 */
void RecordBigTest::testFastExtractSubRecordVR() {
	DoubleIntVarchar div;
	const TableDef *tabDef = div.getTableDef();
	Record *rec = div.createRecord(1, 100, "hello world", REC_VARLEN);
	SubRecordBuilder rb(tabDef, REC_REDUNDANT);
	SubRecord *subRec = rb.createEmptySbByName(tabDef->m_maxRecSize, "a b");

	const int repeat = 1000000;
	u64 before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::fastExtractSubRecordVR(tabDef, rec, subRec);
	}
	u64 after = System::clockCycles();
	cout << endl << "fastExtractSubRecordVR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;
	freeRecord(rec);
	freeSubRecord(subRec);
}

void compareExtractorFR(const TableDef *tableDef, const Record *rec, SubRecord *subRec) {
	SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
		, subRec->m_numCols, subRec->m_columns, rec->m_format, subRec->m_format);

	const int repeat = 1000000;
	u64 before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		extrator->extract(rec, subRec);
	}
	u64 after = System::clockCycles();
	cout << endl << "SubrecExtrator(FR): "
		<< (after - before) / repeat
		<< "(cc)" << endl;
	delete extrator;

	{
		MemoryContext *memctx = new MemoryContext(Limits::PAGE_SIZE, 10);
		u64 before = System::clockCycles();
		for (uint i = 0; i < repeat; ++i) {
			SubrecExtractor *extrator = SubrecExtractor::createInst(memctx, tableDef
				, subRec->m_numCols, subRec->m_columns, rec->m_format, subRec->m_format);
			extrator->extract(rec, subRec);
			memctx->reset();
		}
		u64 after = System::clockCycles();
		cout << "SubrecExtrator(FR) with constructor: "
			<< (after - before) / repeat
			<< "(cc)" << endl;
		delete memctx;
	}

	{
		MemoryContext *memctx = new MemoryContext(Limits::PAGE_SIZE, 10);
		u64 before = System::clockCycles();
		for (uint i = 0; i < repeat; ++i) {
			SubrecExtractor *extrator = SubrecExtractor::createInst(memctx, tableDef
				, subRec->m_numCols, subRec->m_columns, rec->m_format, subRec->m_format, 1);
			extrator->extract(rec, subRec);
			memctx->reset();
		}
		u64 after = System::clockCycles();
		cout << "SubrecExtrator(FR) degradation: "
			<< (after - before) / repeat
			<< "(cc)" << endl;
		delete memctx;
	}
	before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::fastExtractSubRecordFR(tableDef, rec, subRec);
	}
	after = System::clockCycles();
	cout << "fastExtractSubRecordFR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;

	before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::extractSubRecordFR(tableDef, rec, subRec);
	}
	after = System::clockCycles();
	cout << "extractSubRecordFR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;
}

void RecordBigTest::testSubrecExtratorFR() {
	{
		DoubleInt di;
		const TableDef *tabDef = di.getTableDef();
		Record *rec = di.createRecord(1, 100, REC_FIXLEN);
		SubRecordBuilder rb(tabDef, REC_REDUNDANT);
		{
			cout << endl << "{a int, b int} -> {a, b}";
			SubRecord *subRec = rb.createEmptySbByName(tabDef->m_maxRecSize, "a b");
			compareExtractorFR(tabDef, rec, subRec);
			freeSubRecord(subRec);
		}
		{
			cout << endl << "{a int, b int} -> {b}";
			SubRecord *subRec = rb.createEmptySbByName(tabDef->m_maxRecSize, "b");
			compareExtractorFR(tabDef, rec, subRec);
			freeSubRecord(subRec);
		}
		freeRecord(rec);
	}
	{
		TableDefBuilder tb(0, "noname", "noname");
		tb.addColumn("a", CT_BIGINT);
		tb.addColumn("b", CT_BIGINT);
		tb.addColumn("c", CT_BIGINT);
		tb.addColumn("d", CT_BIGINT);
		tb.addColumn("e", CT_BIGINT);
		tb.addColumn("f", CT_BIGINT);
		tb.addColumn("g", CT_BIGINT);
		tb.addColumn("h", CT_BIGINT);
		tb.addColumn("i", CT_BIGINT);
		TableDef *tableDef = tb.getTableDef();
		RecordBuilder rb(tableDef, 0, REC_FIXLEN);
		for (char a = 'a'; a <='i'; a++)
			rb.appendBigInt(0);
		Record* rec = rb.getRecord();
		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		{
			cout << endl << "{a bigint, b bigint, ..., i bigint } -> {g}";
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "g");
			compareExtractorFR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		{
			cout << endl << "{a bigint, b bigint, ..., i bigint } -> {e}";
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "e");
			compareExtractorFR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		freeRecord(rec);
	}
}

void compareExtractorVR(const TableDef *tableDef, const Record *rec, SubRecord *subRec) {
	SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
		, subRec->m_numCols, subRec->m_columns, rec->m_format, subRec->m_format);

	const int repeat = 1000000;
	u64 before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		extrator->extract(rec, subRec);
	}
	u64 after = System::clockCycles();
	cout << endl << "SubrecExtrator(VR): "
		<< (after - before) / repeat
		<< "(cc)" << endl;
	delete extrator;

	{
		MemoryContext *memctx = new MemoryContext(Limits::PAGE_SIZE, 10);
		u64 before = System::clockCycles();
		for (uint i = 0; i < repeat; ++i) {
			SubrecExtractor *extrator = SubrecExtractor::createInst(memctx, tableDef
				, subRec->m_numCols, subRec->m_columns, rec->m_format, subRec->m_format);
			extrator->extract(rec, subRec);
			memctx->reset();
		}
		u64 after = System::clockCycles();
		cout << "SubrecExtrator(VR) with constructor: "
			<< (after - before) / repeat
			<< "(cc)" << endl;
		delete memctx;
	}

	{
		MemoryContext *memctx = new MemoryContext(Limits::PAGE_SIZE, 10);
		u64 before = System::clockCycles();
		for (uint i = 0; i < repeat; ++i) {
			SubrecExtractor *extrator = SubrecExtractor::createInst(memctx, tableDef
				, subRec->m_numCols, subRec->m_columns, rec->m_format, subRec->m_format, 1);
			extrator->extract(rec, subRec);
			memctx->reset();
		}
		u64 after = System::clockCycles();
		cout << "SubrecExtrator(VR) degradation: "
			<< (after - before) / repeat
			<< "(cc)" << endl;
		delete memctx;
	}

	before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::fastExtractSubRecordVR(tableDef, rec, subRec);
	}
	after = System::clockCycles();
	cout << "fastExtractSubRecordVR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;

	before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::extractSubRecordVR(tableDef, rec, subRec);
	}
	after = System::clockCycles();
	cout << "extractSubRecordVR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;
}


void RecordBigTest::testSubrecExtratorVR() {
	{ // 简单表
		DoubleIntVarchar div;
		const TableDef *tableDef = div.getTableDef();
		Record *rec = div.createRecord(1, 100, "hello world", REC_VARLEN);
		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		{
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "a b c");
			SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
				, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);

			cout << endl << "{ a int, b int, c varchar} -> {a, b, c}";
			compareExtractorVR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		{
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "a b");
			SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
				, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);

			cout  << endl << "{ a int, b int, c varchar} -> {a, b}";
			compareExtractorVR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		{
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "b");
			SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
				, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);

			cout << endl << "{ a int, b int, c varchar} -> {a}";
			compareExtractorVR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		{
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "b");
			SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
				, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);

			cout << endl << "{ a int, b int, c varchar} -> {b}";
			compareExtractorVR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		{
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "c");
			SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
				, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);

			cout << endl << "{ a int, b int, c varchar} -> {c}";
			compareExtractorVR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		freeRecord(rec);
	}
	{ // 退化情况，包含NULL
		DoubleIntVarchar div;
		const TableDef *tableDef = div.getTableDef();
		Record *rec = div.createRecord(1, 100, NULL, REC_VARLEN);
		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "a b");
		SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
			, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);
		cout << endl << "{ a int, b int, c varchar(NULL) } -> {a, b}";
		compareExtractorVR(tableDef, rec, subRec);
		freeSubRecord(subRec);
		freeRecord(rec);
	}
	{ // 所有列都是变长
		TableDefBuilder tb(0, "noname", "noname");
		tb.addColumnS("a", CT_VARCHAR, 20);
		tb.addColumnS("b", CT_VARCHAR, 20);
		tb.addColumnS("c", CT_VARCHAR, 20);
		tb.addColumnS("d", CT_VARCHAR, 20);

		TableDef *tableDef = tb.getTableDef();
		RecordBuilder rb(tableDef, 0, REC_VARLEN);
		for (char a = 'a'; a <='d'; a++)
			rb.appendVarchar("hello world");
		Record* rec = rb.getRecord();
		SubRecordBuilder srb(tableDef, REC_REDUNDANT);
		{
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "a b c d");
			SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
				, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);

			cout << endl << "{ a varchar, b varchar, c varchar, d varchar} -> {a, b, c, d}";
			compareExtractorVR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		{
			SubRecord *subRec = srb.createEmptySbByName(tableDef->m_maxRecSize, "d");
			SubrecExtractor *extrator = SubrecExtractor::createInst(0, tableDef
				, subRec->m_numCols, subRec->m_columns, REC_VARLEN, REC_REDUNDANT);

			cout << endl << "{ a varchar, b varchar, c varchar, d varchar} -> {d}";
			compareExtractorVR(tableDef, rec, subRec);
			freeSubRecord(subRec);
		}
		freeRecord(rec);
		delete tableDef;
	}
}

static void* memcpy8(byte *dst, byte *src, size_t size) {
	UNREFERENCED_PARAMETER(size);
	*(u64 *)dst = *(u64 *)src;
	return dst;
}

static void* memcpy4(byte *dst, byte *src, size_t size) {
	UNREFERENCED_PARAMETER(size);
	*(u32 *)dst = *(u32 *)src;
	return dst;
}



class MemoryCopyMethod {
public:
	virtual void* copy(byte *dst, byte *src, size_t size) = 0;
};

class MemoryCopyMethod4: public MemoryCopyMethod {
public:
	virtual void* copy(byte *dst, byte *src, size_t size) {
		UNREFERENCED_PARAMETER(size);
		*(u32 *)dst = *(u32 *)src;
		return dst;
	}
};

class MemoryCopyMethod8: public MemoryCopyMethod {
public:
	virtual void* copy(byte *dst, byte *src, size_t size) {
		UNREFERENCED_PARAMETER(size);
		*(u64 *)dst = *(u64 *)src;
		return dst;
	}
};

typedef void* (*copyFunc)(byte *, byte *, size_t);

/** 测试memcpy的性能 */
void RecordBigTest::testMemcpy() {
	const int repeat = 10000000;
	byte dst[1024];
	byte src[1024];
	for (size_t i = 0; i < sizeof(src); ++i)
		src[i] = (byte)i;

	int arr_size = 5;
	if (System::currentTimeMillis() > 1000)
		arr_size = 10;
	int *size_arr = new int[arr_size];
	int *off_arr = new int[arr_size];
	copyFunc *func_arr = new copyFunc[arr_size];
	MemoryCopyMethod** copyMethods = new MemoryCopyMethod*[arr_size];
	for (int i = 0; i < arr_size; i++) {
		off_arr[i] = 4 * i + (i / 2) * 4;
		if ( i % 2 == 0) {
			size_arr[i] = 4;
			func_arr[i] = memcpy4;
			copyMethods[i] = new MemoryCopyMethod4;
		} else {
			size_arr[i] = 8;
			func_arr[i] = memcpy8;
			copyMethods[i] = new MemoryCopyMethod8;
		}
	}

	{
		u64 before = System::clockCycles();
		for (int i = 0; i < repeat; ++i) {
			for (int n = 0; n < arr_size; n++)
				(*func_arr[n])(dst + off_arr[n], src + off_arr[n], size_arr[n]);
		}
		u64 after = System::clockCycles();
		cout << endl << "funcpointer: "
			<< (double) (after - before) / repeat / 10
			<< "(cc)" << endl;
	}
	{
		u64 before = System::clockCycles();
		for (int i = 0; i < repeat; ++i) {
			for (int n = 0; n < arr_size; n++)
				memcpy(dst + off_arr[n], src + off_arr[n], size_arr[n]);
		}
		u64 after = System::clockCycles();
		cout << endl << "memcpy: "
			<< (after - before) / repeat / 10
			<< "(cc)" << endl;
	}
	{ // memcpy vs length
		for (int len = 0; len < 32; ++len) {
			u64 before = System::clockCycles();
			for (int i = 0; i < repeat; ++i) {
				for (int n = 0; n < arr_size; n++)
					memcpy(dst + off_arr[n], src + off_arr[n], len);
			}
			u64 after = System::clockCycles();
			cout << endl << "memcpy(" << len << "): "
				<< (after - before) / repeat / 10
				<< "(cc)" << endl;
		}
	}
	{
		u64 before = System::clockCycles();
		for (int i = 0; i < repeat; ++i) {
			*(u32 *)(dst) =  *(u32 *)src;
			*(u32 *)(dst + 4) = *(u32 *)(src + 4);
			*(u32 *)(dst + 8) = *(u32 *)(src + 8);
			*(u32 *)(dst + 12) = *(u32 *)(src + 12);
			*(u32 *)(dst + 16) = *(u32 *)(src + 16);
			*(u32 *)(dst + 20) = *(u32 *)(src + 20);
			*(u32 *)(dst + 24) = *(u32 *)(src + 24);
			*(u32 *)(dst + 28) = *(u32 *)(src + 28);
			*(u32 *)(dst + 32) = *(u32 *)(src + 32);
			*(u32 *)(dst + 36) = *(u32 *)(src + 36);
		}
		u64 after = System::clockCycles();
		cout << endl << "assignment: "
			<< (after - before) / repeat / 10
			<< "(cc)" << endl;
	}
	{
		u64 before = System::clockCycles();


		for (int i = 0; i < repeat; ++i) {
			for (int n = 0; n < arr_size; n++)
				copyMethods[n]->copy(dst + off_arr[n], src + off_arr[n], size_arr[n]);
		}

		u64 after = System::clockCycles();
		cout << endl << "virtual method: "
			<< (after - before) / repeat / 10
			<< "(cc)" << endl;
	}

	for (int i = 0; i < arr_size; ++i) {
		delete copyMethods[i];
	}
	delete [] size_arr;
	delete [] off_arr;
	delete [] func_arr;
	delete [] copyMethods;
}

void compareExtractorCR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRec) {
	MemoryContext *memctx = new MemoryContext(Limits::PAGE_SIZE, 10);
	SubToSubExtractor *extrator = SubToSubExtractor::createInst(memctx, tableDef, indexDef
		, key->m_numCols, key->m_columns, subRec->m_numCols, subRec->m_columns,
		key->m_format, subRec->m_format);

	const int repeat = 1000000;
	u64 before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		extrator->extract(key, subRec);
	}
	u64 after = System::clockCycles();
	cout << endl << "SubToSubExtrator(CR): "
		<< (after - before) / repeat
		<< "(cc)" << endl;

	{
		u64 before = System::clockCycles();
		for (uint i = 0; i < repeat; ++i) {
			SubToSubExtractor *extrator = SubToSubExtractor::createInst(memctx, tableDef, indexDef
				, key->m_numCols, key->m_columns, subRec->m_numCols, subRec->m_columns,
				key->m_format, subRec->m_format);
			extrator->extract(key, subRec);
			memctx->reset();
		}
		u64 after = System::clockCycles();
		cout << "SubToSubExtrator(CR) with constructor: "
			<< (after - before) / repeat
			<< "(cc)" << endl;
	}

	{
		u64 before = System::clockCycles();
		for (uint i = 0; i < repeat; ++i) {
			SubToSubExtractor *extrator = SubToSubExtractor::createInst(memctx, tableDef, indexDef
				, key->m_numCols, key->m_columns, subRec->m_numCols, subRec->m_columns,
				key->m_format, subRec->m_format, 1);
			extrator->extract(key, subRec);
			memctx->reset();
		}
		u64 after = System::clockCycles();
		cout << "SubToSubExtrator(CR) degradation: "
			<< (after - before) / repeat
			<< "(cc)" << endl;
	}

	before = System::clockCycles();
	for (uint i = 0; i < repeat; ++i) {
		RecordOper::extractSubRecordCR(tableDef, indexDef, key, subRec);
	}
	after = System::clockCycles();
	cout << "extractSubRecordCR: "
		<< (after - before) / repeat
		<< "(cc)" << endl;

	delete memctx;
}

// 对比测试SubrecExtractorCR与extractSubRecordCR的性能
void RecordBigTest::testSubrecExtratorCR() {
	// 全整数属性
	{
		TableDefBuilder builder(0, "test", "test");
		builder.addColumn("a", CT_BIGINT)->addColumn("b", CT_INT)->addColumn("c", CT_SMALLINT);
		builder.addIndex("bac", false, false, false, "b", 0, "a", 0, "c", 0, NULL);
		const TableDef *tableDef = builder.getTableDef();
		const IndexDef *indexDef = tableDef->getIndexDef(0);
		
		SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
		SubRecordBuilder keyBuilder(tableDef, KEY_COMPRESS);
		u64 a = 100;
		u32 b = 20;
		u16 c = 58;
		SubRecord *srC = keyBuilder.createSubRecordByName("b a c", &b, &a, &c);
		{
			cout << endl << "{b int, a bigint, c smallint} -> {a, b, c}";
			SubRecord *srR = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "a b c");
			compareExtractorCR(tableDef, indexDef, srC, srR);
			freeSubRecord(srR);
		}
		{
			cout << endl << "{a bigint, b int, c smallint} -> {a, c}";
			SubRecord *srR = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "a c");
			compareExtractorCR(tableDef, indexDef, srC, srR);
			freeSubRecord(srR);
		}
		freeSubRecord(srC);
		delete tableDef;
	}
	// 整数与字符串混合
	{
		TableDefBuilder builder(0, "test", "test");
		builder.addColumn("a", CT_BIGINT)->addColumnS("b", CT_CHAR, 16)->addColumnS("c", CT_VARCHAR, 16);
		builder.addIndex("bac", false, false, false, "b", 0, "a", 0, "c", 0, NULL);
		const TableDef *tableDef = builder.getTableDef();
		const IndexDef *indexDef = tableDef->getIndexDef(0);
		
		SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
		SubRecordBuilder keyBuilder(tableDef, KEY_COMPRESS);
		u64 a = 100;
		SubRecord *srC = keyBuilder.createSubRecordByName("b a c", "bbbbbbbb", &a, "cccccccc");
		{
			cout << endl << "{b char(16), a bigint, c varchar(16)} -> {a, b, c}";
			SubRecord *srR = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "a b c");
			compareExtractorCR(tableDef, indexDef, srC, srR);
			freeSubRecord(srR);
		}
		{
			cout << endl << "{b char(16), a bigint, c varchar(16)} -> {a, c}";
			SubRecord *srR = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, "a c");
			compareExtractorCR(tableDef, indexDef, srC, srR);
			freeSubRecord(srR);
		}
		freeSubRecord(srC);
		delete tableDef;
	}
}

const char* RecordConvertTest::getName() {
	return "Record convert test";
}

const char* RecordConvertTest::getDescription() {
	return "Test record convert ";
}

void RecordConvertTest::testRecordConvert() {
	MemoryContext mc(1024 * 1024, 1);

	enum ColNum {
		ID = 0,
		Name,
		Balance,
		Comment,
		Date,
		Resume,
	};

	TableDefBuilder tb(1, "Schema Name", "TestConvert");
	tb.addColumn("ID", CT_BIGINT, false);
	tb.addColumnS("Name", CT_CHAR, 40);
	PrType prtype;
	prtype.m_deicmal = 2;
	prtype.m_precision = 10;
	tb.addColumnN("Balance", CT_BIGINT, prtype);
	tb.addColumnS("Comment", CT_VARCHAR, 250);
	tb.addColumn("Date", CT_DOUBLE);
	tb.addColumn("Resume", CT_MEDIUMLOB, true, COLL_GBK);
	tb.addIndex("iID", true, true, false, "ID", 0, NULL);
	tb.addIndex("iName", false, false, false, "Name", 0, NULL);
	tb.addIndex("iAccount", false, false, false, "Name", 0, "Balance", 0, NULL);
	tb.addIndex("iDate", false, false, false, "Date", 0, NULL);
	TableDef *origTD = tb.getTableDef();
	origTD->check();

	// converter
	RecordConvert *convert;
	TableDef *convTD;
	// 设定删除的列
	u16 delColNum;
	ColumnDef **delCol;



	// 删除一列测试
	delColNum = 1;
	delCol = new ColumnDef *[delColNum];
	delCol[0] = origTD->m_columns[Comment];
	assert(!strcmp(delCol[0]->m_name, "Comment"));
	try {
		convert = new RecordConvert(origTD, NULL, 0, (const ColumnDef **)delCol, delColNum);
	} catch (NtseException &) {
		CPPUNIT_ASSERT(false);
	}
	// 测试
	convTD = convert->getNewTableDef();
	CPPUNIT_ASSERT(convTD->m_numCols == 5);
	CPPUNIT_ASSERT(convTD->m_numIndice == origTD->m_numIndice);
	CPPUNIT_ASSERT(0 == strcmp(convTD->m_columns[3]->m_name, origTD->m_columns[4]->m_name));
	CPPUNIT_ASSERT(convTD->m_indice[3]->m_columns[0] == origTD->m_indice[3]->m_columns[0] - 1);
	
	/*
	RecordBuilder rb(origTD, INVALID_ROW_ID, REC_MYSQL);
	rb.appendBigInt(128)->appendChar("John Smith")->appendBigInt(780000);
	rb.appendVarchar("this is a test")->appendDouble(2009.0727)
	*/
	Record *orec = new Record(INVALID_ROW_ID, REC_MYSQL, new byte[origTD->m_maxRecSize], origTD->m_maxRecSize);
	// ID
	RedRecord::setNotNull(origTD, orec->m_data, ID);
	RedRecord::writeNumber(origTD, ID, orec->m_data, (u64)128);
	// Name
	RedRecord::setNotNull(origTD, orec->m_data, Name);
	RedRecord::writeChar(origTD, orec->m_data, Name, "John Smith");
	// Balance
	RedRecord::setNotNull(origTD, orec->m_data, Balance);
	RedRecord::writeNumber(origTD, Balance, orec->m_data, (u64)780000);
	// Comment
	RedRecord::setNull(origTD, orec->m_data, Comment);
	// Date
	RedRecord::setNotNull(origTD, orec->m_data, Date);
	RedRecord::writeNumber(origTD, Date, orec->m_data, (double)2009.0728);
	// Resume
	char *resume = randomStr(8193);
	RedRecord::setNotNull(origTD, orec->m_data, Resume);
	//RedRecord::writeLobSize()
	RedRecord::writeLob(origTD, orec->m_data, Resume, (byte *)resume, 8193+1);

	void * nNamedata;
	char * nName;
	size_t nNamesize;
	RedRecord::readChar(origTD, orec->m_data, Name, &nNamedata, &nNamesize);
	nName = (char *)nNamedata;



	// finish
	byte *nrec = convert->convertMysqlOrRedRec(orec, &mc);

	// 依照convTD验证每个数据都正确
	s64 oID = RedRecord::readBigInt(convTD, nrec, ID);
	CPPUNIT_ASSERT(oID == 128);
	void * oNamedata;
	char * oName;
	size_t oNamesize; 
	RedRecord::readChar(convTD, nrec, Name, &oNamedata, &oNamesize);
	oName = (char *)oNamedata;
	CPPUNIT_ASSERT(0 == memcmp(oName, nName, oNamesize));
	s64 oBalance = RedRecord::readBigInt(convTD, nrec, Balance);
	CPPUNIT_ASSERT(oBalance == 780000);
	void * oDatedata;
	size_t oDate;
	RedRecord::readRaw(convTD, nrec, Date-1, &oDatedata, &oDate);
	CPPUNIT_ASSERT(oDate == sizeof(double));
	double oDateDouble = *((double *)oDatedata);
	CPPUNIT_ASSERT(oDateDouble == (double)2009.0728);
	void *olob;
	size_t olobSize;
	RedRecord::readLob(convTD, nrec, Resume - 1, &olob, &olobSize);
	CPPUNIT_ASSERT(olobSize == 8194);
	CPPUNIT_ASSERT(olob == (void *)resume);


	delete [] delCol;
	delete convert;
	delete convTD;

	// 增加一列测试
	delColNum = 1;
	delCol = new ColumnDef *[delColNum];
	delCol[0] = origTD->m_columns[3];
	assert(!strcmp(delCol[0]->m_name, "Comment"));
	ColumnDef ageCol("Age", CT_TINYINT);
	AddColumnDef *addCol = new AddColumnDef[1];
	addCol[0].m_addColDef = &ageCol;
	addCol[0].m_position = 1; // ID之前Name之后
	u8 defaultAge = 20;
	addCol[0].m_defaultValue = (void *)&defaultAge;
	addCol[0].m_valueLength = sizeof(defaultAge);
	try {
		convert = new RecordConvert(origTD, addCol, 1, (const ColumnDef **)delCol, delColNum);
	} catch (NtseException &) {
		CPPUNIT_ASSERT(false);
	}
	convTD = convert->getNewTableDef();
	CPPUNIT_ASSERT(convTD->m_numCols == 6);
	CPPUNIT_ASSERT(convTD->m_indice[2]->m_columns[0] == 2);
	CPPUNIT_ASSERT(convTD->m_indice[2]->m_columns[1] == 3);

	// 验证
	nrec = convert->convertMysqlOrRedRec(orec, &mc);
	oID = RedRecord::readBigInt(convTD, nrec, ID);
	CPPUNIT_ASSERT(oID == 128);

	u8 oAge = RedRecord::readTinyInt(convTD, nrec, ID+1);
	CPPUNIT_ASSERT(oAge == 20);

	RedRecord::readChar(convTD, nrec, Name + 1, &oNamedata, &oNamesize);
	oName = (char *)oNamedata;
	CPPUNIT_ASSERT(0 == memcmp(oName, nName, oNamesize));
	oBalance = RedRecord::readBigInt(convTD, nrec, Balance + 1);
	CPPUNIT_ASSERT(oBalance == 780000);

	RedRecord::readRaw(convTD, nrec, Date, &oDatedata, &oDate);
	CPPUNIT_ASSERT(oDate == sizeof(double));
	oDateDouble = *((double *)oDatedata);
	CPPUNIT_ASSERT(oDateDouble == (double)2009.0728);
	RedRecord::readLob(convTD, nrec, Resume, &olob, &olobSize);
	CPPUNIT_ASSERT(olobSize == 8194);
	CPPUNIT_ASSERT(olob == (void *)resume);


	delete [] delCol;
	delete convert;
	delete convTD;
	delete [] addCol;


	freeMysqlRecord(origTD, orec);

	delete origTD;

}
