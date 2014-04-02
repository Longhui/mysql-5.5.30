/**
 * ²âÊÔÄÚ´æË÷Òý²Ù×÷
 *
 * @author ÀîÎ°îÈ(liweizhao@corp.netease.com)
 */
#ifndef _NTSETEST_MEM_INDEX_OPERATION_
#define _NTSETEST_MEM_INDEX_OPERATION_

#include <cppunit/extensions/HelperMacros.h>
#include "btree/IndexPage.h"
#include "btree/MIndexKey.h"
#include "btree/MIndexPage.h"
#include "util/PagePool.h"
#include "misc/TNTIMPageManager.h"
#include "misc/MemCtx.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "btree/MIndexKeyHelper.h"

using namespace tnt;
using namespace ntse;

class MIndexPageTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MIndexPageTestCase);
	CPPUNIT_TEST(testCommon);
	CPPUNIT_TEST(testAddKeyAndFindKey);
	CPPUNIT_TEST(testAppendKeyAndDeleteKey);
	//CPPUNIT_TEST(testBulkPhyReclaim);
	CPPUNIT_TEST(testRedistribute);
	CPPUNIT_TEST(testSplitAndMerge);

	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

private:
	void testCommon();
	void testAddKeyAndFindKey();
	void testAppendKeyAndDeleteKey();
	//void testBulkPhyReclaim();
	void testRedistribute();
	void testSplitAndMerge();

private:
	MIndexPageHdl generatePageWithKeys(u16 reserveFreeSpace);

private:
	MemoryContext    *m_mtx;
	MIndexKeyHelper  *m_keyHelper;
	PagePool         *m_pool;
	TNTIMPageManager *m_pageManager;
	MIndexPageHdl    m_currentPage;
};


#endif