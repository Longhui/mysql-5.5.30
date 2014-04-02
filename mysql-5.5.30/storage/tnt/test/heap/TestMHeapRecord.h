#ifndef _TNTTEST_MHEAPRECORD_H_
#define _TNTTEST_MHEAPRECORD_H_

#include <cppunit/extensions/HelperMacros.h>
#include "heap/MHeapRecord.h"

using namespace ntse;
using namespace tnt;

class MHeapRecordTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MHeapRecordTestCase);
	CPPUNIT_TEST(testHeapRecSerialize);
	CPPUNIT_TEST(testGetFreeGrade);
	CPPUNIT_TEST(testAppendRecord);
	CPPUNIT_TEST(testUpdateRecord);
	CPPUNIT_TEST(testRemoveRecord);
	CPPUNIT_TEST(testDefrag);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testHeapRecSerialize();
	void testGetFreeGrade();
	void testAppendRecord();
	void testUpdateRecord();
	void testRemoveRecord();
	void testDefrag();
private:
	void testDefrag(u32 total);
	Record* createStudentRec(RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade);
	void compareHeapRec(MHeapRec *heapRec1, MHeapRec *heapRec2);
	void appendRecord(MRecordPage *page, u32 total);

	TableDef *m_tableDef;

	Record   *m_rec1;
	Record   *m_rec2;
	MHeapRec *m_heapRec1;
	MHeapRec *m_heapRec2;
};
#endif