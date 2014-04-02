/**
 * Record操作测试
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#ifndef _NTSETEST_RECORD_H_
#define _NTSETEST_RECORD_H_

#include <cppunit/extensions/HelperMacros.h>
#include "Test.h"
#include "misc/RecordHelper.h"
#include "api/Table.h"
#include "util/System.h"
#include "heap/Heap.h"
#include "util/SmartPtr.h"
#include <iostream>
#include <iterator>

#define STU_NAME "name"
#define STU_SNO "sno"
#define STU_AGE "age"
#define STU_SEX "sex"
#define STU_CLASS "class"
#define STU_GPA "gpa"
#define STU_GRADE "grade"
/** 学生表 */
class StudentTable {
public:
	StudentTable(bool isCompressedTable = false) {
		TableDefBuilder tb(1, "Olympic", "student");
		tb.addColumnS(STU_NAME, CT_VARCHAR, 11, false, false, COLL_LATIN1);
		tb.addColumn(STU_SNO, CT_INT);
		tb.addColumn(STU_AGE, CT_SMALLINT);
		tb.addColumnS(STU_SEX, CT_CHAR, 2, false, true, COLL_LATIN1);
		tb.addColumn(STU_CLASS, CT_MEDIUMINT);
		tb.addColumn(STU_GPA, CT_FLOAT);
		PrType prtype;
		prtype.setUnsigned();
		tb.addColumnN(STU_GRADE, CT_BIGINT, prtype);
		tb.addIndex("name_index", false, false, false, STU_NAME, 0, NULL);
		tb.addIndex("name_age_index", false, false, false, STU_NAME, 0, STU_AGE, 0, NULL);
		tb.addIndex("age_name_index", false, false, false, STU_AGE, 0, STU_NAME, 0, NULL);
		tb.addIndex("sno_age_index", false, false, false, STU_SNO, 0, STU_AGE, 0, NULL);
		tb.addIndex("age_sno_index", false, false, false, STU_AGE, 0, STU_SNO, 0, NULL);
		tb.addIndex("name_sno_index", false, false, false, STU_NAME, 0, STU_SNO, 0, NULL);
		tb.addIndex("sno_name_index", false, false, false, STU_SNO, 0, STU_NAME, 0, NULL);
		tb.addIndex("grade_index", false, false, false, STU_GRADE, 0, NULL);
		tb.addIndex("name_sno_gpa_class_index", false, false, false, STU_NAME, 0, STU_SNO, 0, STU_GPA, 0, STU_CLASS, 0, NULL);
		tb.addIndex("name_sex_index", false, false, false, STU_NAME, 0, STU_SEX, 0, NULL);
		tb.addIndex("sex_name_index", false, false, false, STU_SEX, 0, STU_NAME, 0, NULL);
		tb.addIndex("sno_sex_index", false, false, false, STU_SNO, 0, STU_SEX, 0, NULL);
		tb.addIndex("age_sno_name_sex_index", false, false, false, STU_AGE, 0, STU_SNO, 0, STU_NAME, 0, STU_SEX, 0, NULL);
		tb.addIndex("age_name_sex_index", false, false, false, STU_AGE, 0, STU_NAME, 0, STU_SEX, 0, NULL);
		tb.addIndex("sno_index", false, false, false, STU_SNO, 0, NULL);

		if (isCompressedTable) {
			tb.setCompresssTbl(true);
			Array<u16> colgrp1, colgrp2;//分为两个属性组			
			colgrp1.push(3);//STU_SEX
			colgrp1.push(2);//STU_AGE
			colgrp1.push(1);//STU_SNO
			colgrp1.push(0);//STU_NAME
			colgrp2.push(6);//STU_GRADE
			colgrp2.push(5);//STU_GPA
			colgrp2.push(4);//STU_CLASS
			tb.addColGrp(0, colgrp1);
			tb.addColGrp(1, colgrp2);
		}
		m_tableDef = tb.getTableDef();
	}

	~StudentTable() {
		delete m_tableDef;
	}

	const TableDef* getTableDef() const {
		return m_tableDef;
	}

	Record* createRecord(RecFormat format, const char* name
		, int sno, short age, const char* sex, int cls, float gpa, u64 grade = 0) const {
			RecordBuilder rb(m_tableDef, 0, format);
			rb.appendVarchar(name);
			rb.appendInt(sno);
			rb.appendSmallInt(age);
			rb.appendChar(sex);
			rb.appendMediumInt(cls);
			rb.appendFloat(gpa);
			rb.appendBigInt(grade);
			return rb.getRecord(m_tableDef->m_maxRecSize);
	}

	CompressOrderRecord *createCompressOrderRecord(const char* name
		, int sno, short age, const char* sex, int cls, float gpa, u64 grade = 0) {
			RecordBuilder rb(m_tableDef, 0, REC_COMPRESSORDER);
			rb.appendChar(sex);
			rb.appendSmallInt(age);
			rb.appendInt(sno);
			rb.appendVarchar(name);	
			rb.appendBigInt(grade);
			rb.appendFloat(gpa);
			rb.appendMediumInt(cls);
			return rb.getCompressOrderRecord(m_tableDef->m_maxRecSize);
	}

private:
	TableDef* m_tableDef;
};


#define BOOK_TITLE "title"
#define BOOK_ISBN "isbn"
#define BOOK_PAGES "pages"
#define BOOK_PRICE "price"
/** 书本（定长）*/
class BookTable {
public:
	BookTable() {
		TableDefBuilder tb(1, "Olympic", "book");
		tb.addColumnS(BOOK_TITLE, CT_CHAR, 10, false, false, COLL_LATIN1);
		tb.addColumnS(BOOK_ISBN, CT_CHAR, 10, false, true, COLL_LATIN1);
		tb.addColumn(BOOK_PAGES, CT_INT);
		tb.addColumn(BOOK_PRICE, CT_INT);
		tb.addIndex("title_index", false, false, false, BOOK_TITLE, 0, NULL);
		tb.addIndex("title_isbn_index", false, false, false, BOOK_TITLE, 0, BOOK_ISBN, 0, NULL);
		tb.addIndex("isbn_title_index", false, false, false, BOOK_ISBN, 0, BOOK_TITLE, 0, NULL);
		tb.addIndex("pages_isbn_index", false, false, false, BOOK_PAGES, 0, BOOK_ISBN, 0, NULL);
		tb.addIndex("pages_price_index", false, false, false, BOOK_PAGES, 0, BOOK_PRICE, 0, NULL);
		tb.addIndex("price_pages_index", false, false, false, BOOK_PRICE, 0, BOOK_PAGES, 0, NULL);
		tb.addIndex("title_pages_index", false , false, false, BOOK_TITLE, 0, BOOK_PAGES, 0, NULL);
		tb.addIndex("isbn_pages_index", false, false, false, BOOK_ISBN, 0, BOOK_PAGES, 0, NULL);
		m_tableDef = tb.getTableDef();
	}
	~BookTable() {
		delete m_tableDef;
	}
	const TableDef* getTableDef() const {
		return m_tableDef;
	}
	Record* createRecord(const char* title, const char* isbn, int pages, int price,
		RecFormat format, RowId rowId = 0) const {
			RecordBuilder rb(m_tableDef, rowId, format);
			rb.appendChar(title);
			rb.appendChar(isbn);
			rb.appendInt(pages);
			rb.appendInt(price);
			return rb.getRecord(m_tableDef->m_maxRecSize);
	}
private:
	TableDef* m_tableDef;
};


#define PAPER_ID "id"
#define PAPER_AUTHOR "author"
#define PAPER_ABSTRACT "abstract"
#define PAPER_CONTENT "content"
#define PAPER_PUBLISHTIME "publishtime"
#define PAPER_PAGES "pages"

#define PAPER_ID_COLNO 0
#define PAPER_AUTHOR_COLNO 1
#define PAPER_ABSTRACT_COLNO 2
#define PAPER_CONTENT_COLNO 3
#define PAPER_PUBLISHTIME_COLNO 4
#define PAPER_PAGES_COLNO 5

class PaperTable {
public:
	PaperTable() {
		TableDefBuilder tb(2, "Olympic", "paper");
		tb.addColumn(PAPER_ID, CT_BIGINT, false);
		tb.addColumnS(PAPER_AUTHOR, CT_VARCHAR, 20, false, false, COLL_LATIN1);
		tb.addColumn(PAPER_ABSTRACT, CT_SMALLLOB);
		tb.addColumn(PAPER_CONTENT, CT_MEDIUMLOB);
		tb.addColumn(PAPER_PUBLISHTIME, CT_INT);
		tb.addColumn(PAPER_PAGES, CT_INT);
		tb.addIndex("paper_id_index", true, true, PAPER_ID, 0, NULL);
		tb.addIndex("paper_author_index", false, false, PAPER_AUTHOR, 0, NULL);
		m_tableDef = tb.getTableDef();
	}
	~PaperTable() {
		delete m_tableDef;
	}
	const TableDef* getTableDef() const {
		return m_tableDef;
	}
	const Record* createRecord(u64 id, const char *author, const char *abs, const char *content, u32 publishTime, u32 pages, 
		RecFormat format, RowId rowId = 0) {
			RecordBuilder rb(m_tableDef, rowId, format);
			RedRecord redRec(m_tableDef, rb.getRecord(m_tableDef->m_maxRecSize));
			memset(redRec.getRecord()->m_data, 0, m_tableDef->m_maxRecSize);
			redRec.writeNumber((u16)PAPER_ID_COLNO, id);
			redRec.writeVarchar((u16)PAPER_AUTHOR_COLNO, (const byte*)author, strlen(author));
			if (abs)
				redRec.writeLob((u16)PAPER_ABSTRACT_COLNO, (const byte*)abs, strlen(abs));
			else
				redRec.setNull((u16)PAPER_ABSTRACT_COLNO);
			if (content)
				redRec.writeLob((u16)PAPER_CONTENT_COLNO, (const byte*)content, strlen(content));
			else
				redRec.setNull((u16)PAPER_CONTENT_COLNO);
			redRec.writeNumber((u16)PAPER_PUBLISHTIME_COLNO, publishTime);
			redRec.writeNumber((u16)PAPER_PAGES_COLNO, pages);
			return redRec.getRecord();
	}

private:
	TableDef *m_tableDef;
};


#define BLOG_ID "id"
#define BLOG_NAME "name"
#define BLOG_AUTHOR "author"
#define BLOG_ABS "abstract"
#define BLOG_CONTENT "content"
#define BLOG_PUBTIME "pubtime"

#define BLOG_ID_COLNO 0
#define BLOG_NAME_COLNO 1
#define BLOG_AUTHOR_COLNO 2
#define BLOG_ABSTRACT_COLNO 3
#define BLOG_CONTENT_COLNO 4
#define BLOG_PUBLISHTIME_COLNO 5
class UserBlogTable {
public:
	UserBlogTable(bool isCompressedTable = false) {
		TableDefBuilder tb(1, "Olympic", "blog");
		tb.addColumn(BLOG_ID, CT_BIGINT);
		tb.addColumnS(BLOG_NAME, CT_VARCHAR, 10, false, true, COLL_LATIN1);
		tb.addColumnS(BLOG_AUTHOR, CT_CHAR, 10, false, true, COLL_LATIN1);
		tb.addColumnS(BLOG_ABS, CT_VARCHAR, 2000, true, true, COLL_BIN);
		tb.addColumn(BLOG_CONTENT, CT_SMALLLOB);
		tb.addColumn(BLOG_PUBTIME, CT_BIGINT);

		tb.addIndex("author_name_abs_content_comment_index", false, false, false, BLOG_AUTHOR, 3, BLOG_NAME, 4, BLOG_ABS, 10, BLOG_CONTENT, 256, NULL);
		tb.addIndex("author_name_index", false, false, false, BLOG_AUTHOR, 3, BLOG_NAME, 4, NULL);

		m_tableDef = tb.getTableDef();
	}

	~UserBlogTable() {
		delete m_tableDef;
	}

	const TableDef* getTableDef() const {
		return m_tableDef;
	}
// 	Record* createRecord(RowId rowId, RecFormat format, const u64 id, const char *name, 
// 		const char *author, const char *abs, const char *content, const u64 publishTime){
// 			RecordBuilder rb(m_tableDef, rowId, format);
// 			RedRecord redRec(m_tableDef, rb.getRecord(m_tableDef->m_maxRecSize));
// 			((Record*)redRec.getRecord())->m_size = m_tableDef->m_maxRecSize;
// 			memset(redRec.getRecord()->m_data, 0, m_tableDef->m_maxRecSize);
// 			redRec.writeNumber((u16)BLOG_ID_COLNO, id);
// 			if(name)
// 				redRec.writeVarchar((u16)BLOG_NAME_COLNO, (const byte*)name, strlen(name));
// 			else
// 				redRec.setNull((u16)BLOG_NAME_COLNO);
// 			if(author)
// 				redRec.writeChar((u16)BLOG_AUTHOR_COLNO, author, strlen(author));
// 			else
// 				redRec.setNull((u16)BLOG_AUTHOR_COLNO);
// 			if (abs)
// 				redRec.writeLob((u16)BLOG_ABSTRACT_COLNO, (const byte*)abs, strlen(abs));
// 			else
// 				redRec.setNull((u16)BLOG_ABSTRACT_COLNO);
// 			if (content)
// 				redRec.writeLob((u16)BLOG_CONTENT_COLNO, (const byte*)content, strlen(content));
// 			else
// 				redRec.setNull((u16)BLOG_CONTENT_COLNO);
// 
// 			redRec.writeNumber((u16)BLOG_PUBLISHTIME_COLNO, publishTime);
// 			return (Record*)redRec.getRecord();
// 	}

	Record* createRecord(RowId rowId, RecFormat format, const u64 id, const char* name,
		const char *author, const char *abs, const char *content, const u64 publishTime) {
			RecordBuilder rb(m_tableDef, rowId, format);
			rb.appendBigInt(id);
			if (name)
				rb.appendVarchar(name);
			else
				rb.appendNull();
			if (author)
				rb.appendChar(author);
			else 
				rb.appendNull();
			if (abs)
				rb.appendSmallLob((const byte*)abs);
			else 
				rb.appendNull();
			if (content)
				rb.appendSmallLob((const byte*)content);
			else
				rb.appendNull();
			rb.appendBigInt(publishTime);
			return rb.getRecord(m_tableDef->m_maxRecSize);
	}


private:
	TableDef* m_tableDef;
};

/** 记录操作类测试用例 */
class RecordTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RecordTestCase);
	CPPUNIT_TEST(testExtractKeyRC);
	CPPUNIT_TEST(testConvertKeyPC);
	CPPUNIT_TEST(testConvertKeyPN);
	CPPUNIT_TEST(testConvertKeyCP);
	CPPUNIT_TEST(testConvertKeyNC);
	CPPUNIT_TEST(testConvertKeyCN);
	CPPUNIT_TEST(testConvertKeyMP);
	CPPUNIT_TEST(testExtractKeyRN);
	CPPUNIT_TEST(testExtractKeyFN);
	CPPUNIT_TEST(testExtractKeyVN);
	CPPUNIT_TEST(testExtractKeyRP);
	CPPUNIT_TEST(testExtractSubRecordFR);
	CPPUNIT_TEST(testExtractSubRecordVR);
	CPPUNIT_TEST(testExtractSubRecordCR);
	CPPUNIT_TEST(testExtractSubRecordNR);
	CPPUNIT_TEST(testConvertRecordRV);
	CPPUNIT_TEST(testConvertSubRecordRV);
	CPPUNIT_TEST(testConvertSubRecordVR);
	CPPUNIT_TEST(testUpdateRecordRR);
	CPPUNIT_TEST(testUpdateRecordFR);
	CPPUNIT_TEST(testGetUpdateSizeVR);
	CPPUNIT_TEST(testUpdateRecordVRInPlace);
	CPPUNIT_TEST(testUpdateRecordVR);
	CPPUNIT_TEST(testCompareKeyCC);
	CPPUNIT_TEST(testCompareKeyRC);
	CPPUNIT_TEST(testCompareKeyPC);
	CPPUNIT_TEST(testCompareKeyRR);
	CPPUNIT_TEST(testCompareKeyNN);
	CPPUNIT_TEST(testCompareKeyNP);
	CPPUNIT_TEST(testCompressKey);
	CPPUNIT_TEST(testMergeSubRecordRR);
	CPPUNIT_TEST(testSlobOpers);
	CPPUNIT_TEST(testIsNullR);
	CPPUNIT_TEST(testIsFastCCComparable);
	CPPUNIT_TEST(testIsSubRecordEq);
	CPPUNIT_TEST(testIsRecordEq);
	CPPUNIT_TEST(testGetKeySizeCN);
	CPPUNIT_TEST(testGetRecordSizeRV);
	CPPUNIT_TEST(testGetSubRecordSizeRV);
	CPPUNIT_TEST(testBug2580);
	CPPUNIT_TEST(testFastExtractSubRecordFR);
	CPPUNIT_TEST(testSubrecExtractorFR);
	CPPUNIT_TEST(testSubrecExtractorVR);
	CPPUNIT_TEST(testFastExtractSubRecordVR);
	CPPUNIT_TEST(testInitEmptyRecord);
	CPPUNIT_TEST(testColList);
	CPPUNIT_TEST(testSubRecordSerializationMNR);
	CPPUNIT_TEST(testUpperMysqlRow);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testExtractKeyRC();
	void testConvertKeyPC();
	void testConvertKeyPN();
	void testConvertKeyCP();
	void testConvertKeyNC();
	void testConvertKeyCN();
	void testConvertKeyMP();
	void testExtractKeyRN();
	void testExtractKeyFN();
	void testExtractKeyVN();
	void testExtractKeyRP();
	void testExtractSubRecordFR();
	void testExtractSubRecordVR();
	void testExtractSubRecordCR();
	void testExtractSubRecordNR();
	void testConvertRecordRV();
	void testConvertSubRecordRV();
	void testConvertSubRecordVR();
	void testUpdateRecordRR();
	void testUpdateRecordFR();
	void testGetUpdateSizeVR();
	void testUpdateRecordVRInPlace();
	void testUpdateRecordVR();
	void testCompareKeyCC();
	void testCompareKeyRC();
	void testCompareKeyPC();
	void testCompareKeyRR();
	void testCompareKeyNN();
	void testCompareKeyNP();
	void testCompressKey();
	void testMergeSubRecordRR();
	void testSlobOpers();
	void testIsNullR();
	void testIsFastCCComparable();
	void testIsSubRecordEq();
	void testIsRecordEq();
	void testGetKeySizeCN();
	void testGetRecordSizeRV();
	void testGetSubRecordSizeRV();
	void testBug2580();


	void testFastExtractSubRecordFR();
	void testSubrecExtractorFR();
	void testSubrecExtractorVR();
	void testFastExtractSubRecordVR();
	void testInitEmptyRecord();
	void testColList();
	void testSubRecordSerializationMNR();
	void testUpperMysqlRow();
};

/** 记录操作类测试用例 */
class RecordBigTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RecordBigTest);
	CPPUNIT_TEST(testExtractSubRecordVR);
	CPPUNIT_TEST(testExtractSubRecordFR);
	CPPUNIT_TEST(testFastExtractSubRecordFR);
	CPPUNIT_TEST(testFastExtractSubRecordVR);
	CPPUNIT_TEST(testSubrecExtratorFR);
	CPPUNIT_TEST(testSubrecExtratorVR);
	CPPUNIT_TEST(testMemcpy);
	CPPUNIT_TEST(testSubrecExtratorCR);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testExtractSubRecordVR();
	void testExtractSubRecordFR();
	void testFastExtractSubRecordFR();
	void testFastExtractSubRecordVR();
	void testSubrecExtratorFR();
	void testSubrecExtratorVR();
	void testMemcpy();
	void testSubrecExtratorCR();
};

/** 记录操作类测试用例 */
class RecordConvertTest: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RecordConvertTest);
	CPPUNIT_TEST(testRecordConvert);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig() { return false; };

protected:
	void testRecordConvert();
};


#endif // _NTSETEST_RECORD_H_

