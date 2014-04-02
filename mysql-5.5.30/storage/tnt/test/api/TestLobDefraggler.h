/**
 * 在线大对象整理测试功能
 *
 * @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#ifndef _NTSETEST_LOBDEFRAGGLER_H_
#define _NTSETEST_LOBDEFRAGGLER_H_

#include <cppunit/extensions/HelperMacros.h>
#include "api/LobDefraggler.h"
#include "misc/TableDef.h"
#include <vector>

using namespace std;
using namespace ntse;


class LobDefragglerTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(LobDefragglerTestCase);
	CPPUNIT_TEST(testDefrag);
	CPPUNIT_TEST(testWrongDefragOperation);
	CPPUNIT_TEST(testRedoMove);

	CPPUNIT_TEST_SUITE_END();

public:		
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testDefrag();
	void testWrongDefragOperation();
	void testRedoMove();
private:
	void checkTable();
	TableDef* getAnotherTblDef(bool useMms);
	void createAnotherTbl(Database *db, bool useMms);
	void insertAndDeleteLobs();
	void copyFiles(bool revert = false);
	void saveLobs(vector< pair<byte *, uint> > *lobVector);
	void checkLobsSame(vector< pair<byte *, uint> > *lobVector);

private:
	static const int NUM_LOB_INSERT	= 20;		/* 插入大对象的个数 */

	Database *m_db;		/* 数据库 */
};

#endif
