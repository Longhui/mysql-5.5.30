/**
 * ²âÊÔË÷Òý»ùÀà
 *
 * @author ËÕ±ó(bsu@corp.netease.com, naturally@163.org)
 */

#ifndef _NTSETEST_INDEX_H_
#define _NTSETEST_INDEX_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/Database.h"
#include "api/Table.h"
using namespace ntse;
class IndexTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(IndexTestCase);
	CPPUNIT_TEST(testCreateAndOpenAndDrop);
	//CPPUNIT_TEST(testDrop);
	CPPUNIT_TEST(testRedoCreate);
	CPPUNIT_TEST(testCreateIndex);
	CPPUNIT_TEST(testDropIndex);
	CPPUNIT_TEST(testGetFiles);
	CPPUNIT_TEST(testIndexFileExtend);
	CPPUNIT_TEST(testReuseFreeByteMaps);
	CPPUNIT_TEST(testUseBytesOfByteMaps);
	CPPUNIT_TEST(testGetIndexNo);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testCreateAndOpenAndDrop();
	void testDrop();
	void testRedoCreate();
	void testCreateIndex();
	void testDropIndex();
	void testGetFiles();
	void testIndexFileExtend();
	void testReuseFreeByteMaps();
	void testUseBytesOfByteMaps();
	void testGetIndexNo();

private:
	void init();
	void clear();
	TableDef* getATableDef();
	void create(const char *path, const TableDef *tableDef) throw(NtseException);
	DrsIndice* open(const char *path, const TableDef *tableDef) throw(NtseException);
	void drop(const char *path) throw(NtseException);
	void redoCreate(const char *path, const TableDef *tableDef) throw(NtseException);

	TableDef* createBigTableDef();
	DrsHeap* createBigHeap(const TableDef *tableDef);
	uint buildBigHeap(DrsHeap *heap, const TableDef *tableDef, bool testPrefix, uint size, uint step);

	TableDef* createSmallTableDef();
	DrsHeap* createSmallHeap(const TableDef *tableDef);
	void closeSmallHeap(DrsHeap *heap);
	void buildHeap(DrsHeap *heap, const TableDef *tableDef, uint size, bool keepSequence, bool hasSame);
	Record* createRecord(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance);

	bool checkBMPageFree(Session *session, PageId pageId);

	void createIndex(char *path, DrsHeap *heap, const TableDef *tableDef) throw(NtseException);

private:
	Config m_cfg;
	Database *m_db;
	Session *m_session;
	Connection *m_conn;
	DrsIndice *m_index;
	TableDef *m_tblDef;
	DrsHeap *m_heap;
	char m_path[255];
};

#endif


