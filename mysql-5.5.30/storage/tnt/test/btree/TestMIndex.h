/**
 * �����ڴ������ӿ�
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#ifndef _NTSETEST_MEM_INDEX_H_

#include <cppunit/extensions/HelperMacros.h>
#include "btree/MIndex.h"
#include "btree/IndexBLinkTree.h"
#include "btree/IndexBLinkTreeManager.h"
#include "btree/MIndexKeyHelper.h"
#include "util/PagePool.h"

using namespace tnt;
using namespace ntse;

/**
 * �ڴ�������������
 */
class MIndexTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(MIndexTestCase);
 	CPPUNIT_TEST(testInsertAndUniqueScan);
	CPPUNIT_TEST(testRangeScan);
	CPPUNIT_TEST(testCheckDup);
	CPPUNIT_TEST(testMergeOrRedistribute);
	CPPUNIT_TEST(testRepairRightMost);
	CPPUNIT_TEST(testRepairRightMost2);
	CPPUNIT_TEST(testRepairNotRightMost);
	CPPUNIT_TEST(testRepairNotRightMost2);
	CPPUNIT_TEST(testChangeTreeHeight);
	CPPUNIT_TEST(testUpgradePageLatch);
	CPPUNIT_TEST(testTryTrxLockHoldingLatch);
	CPPUNIT_TEST(testShiftBackScan);
	CPPUNIT_TEST(testCheckHandlePage);
	CPPUNIT_TEST(testShiftForwardKey);
	CPPUNIT_TEST(testShiftBackwardKey);
	CPPUNIT_TEST(testInsertAndDelDifKeyByEndSpaces);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

private:
	void init(bool isUniqueIndex = false);
	void initVarcharIndex(bool isUniqueIndex);
	void clear();
 	void testInsertAndUniqueScan();
	void testRangeScan();
	void testCheckDup();
	void testMergeOrRedistribute();
	void testRepairRightMost();
	void testRepairRightMost2();
	void testRepairNotRightMost();
	void testRepairNotRightMost2();
	void testChangeTreeHeight();
	void testUpgradePageLatch();
	void testTryTrxLockHoldingLatch();
	void testShiftBackScan();
	void testCheckHandlePage();
	void testShiftForwardKey();
	void testShiftBackwardKey();
	void testInsertAndDelDifKeyByEndSpaces();
	
private:
	TNTConfig     m_cfg;
	TNTDatabase   *m_db;
	Connection *m_conn;
	Session    *m_session;
	TableDef   *m_tblDef;
	const IndexDef   *m_idxDef;
	BLinkTreeIndice  *m_memIndice;
	MIndex           *m_memIndex;	
	MIndexKeyHelper  *m_keyHelper;
};

//���̲߳�����������
class MIndexOpThread: public Thread {
public:
	MIndexOpThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : Thread(name) {
		m_name = name;
		m_db = db;
		m_tblDef = tblDef;
		m_index = index;
		m_keyHelper = keyHelper; 
	}
protected:
	uint m_recordid;
	const char *m_name;
	TNTDatabase *m_db;
	Connection *m_conn;
	DrsHeap *m_heap;
	const TableDef *m_tblDef;
	MIndex *m_index;
	RowId m_rowId;
	MIndexKeyHelper  *m_keyHelper;
};



//tryLock�еĶ��߳�
class MIndexTryLockThread : public MIndexOpThread {
public:
	MIndexTryLockThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}

private:
	virtual void run();
};

//repairRightMost�еĶ��߳�
class MIndexRepairRightMostThread: public MIndexOpThread {
public:
	MIndexRepairRightMostThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}

private:
	virtual void run();
};

//repairNotRightMost�еĶ��߳�
class MIndexRepairNotRightMostThread: public MIndexOpThread {
public:
	MIndexRepairNotRightMostThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}

private:
	virtual void run();
};

//����ɨ���ҳ�Ķ��߳�
class MIndexShiftBackScanThread: public MIndexOpThread {
public:
	MIndexShiftBackScanThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}
private:
	virtual void run();
};


//latch�����Ķ��߳�
class MIndexUpgradeLatchThread: public MIndexOpThread {
public:
	MIndexUpgradeLatchThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}
private:
	virtual void run();
};



class MIndexCheckPageLeafThread: public MIndexOpThread {
public:
	MIndexCheckPageLeafThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}
private :
	virtual void run();

};


class MIndexShiftForwardKeyThread: public MIndexOpThread {
public:
	MIndexShiftForwardKeyThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}
private :
	virtual void run();

};

class MIndexShiftBackwardKeyThread: public MIndexOpThread {
public:
	MIndexShiftBackwardKeyThread(const char *name, TNTDatabase *db, const TableDef *tblDef, MIndex *index, MIndexKeyHelper  *keyHelper) : MIndexOpThread(name, db, tblDef, index, keyHelper) {}
private :
	virtual void run();

};

#endif

