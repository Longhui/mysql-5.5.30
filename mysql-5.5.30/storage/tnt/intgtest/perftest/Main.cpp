#include "PerfTest.h"
#include "MemoryTableScan.h"
#include "DiskTableScan.h"
#include "MemoryIndexScan.h"
#include "DiskIndexScan.h"
#include "MemoryRandomRead.h"
#include "DiskRandomRead.h"
#include "Insert.h"
#include "Delete.h"
#include "Update.h"
#include "Synthesis.h"
#include "MmsSynthesis.h"
#include "MmsReplacePolicy.h"
#include "IndexCreation.h"
#include "Backup.h"
#include "Recovery.h"
#include "misc/Syslog.h"
#include "LobInsert.h"
#include "LobRead.h"
#include "LobDel.h"
#include "LobUpdate.h"
#include "LobDefrag.h"
#include "CountTable.h"
#include "LongCharTable.h"
#include "AccountTable.h"
#include "misc/Trace.h"
#include "misc/GlobalFactory.h"
#include "BufferReplacePolicy.h"
#include "DbConfigs.h"



#ifdef TNT_ENGINE
#include "TNTAccountTable.h"
#include "TNTBlogTable.h"
#include "TNTLongCharTable.h"
#include "TNTInsert.h"
#include "TNTUpdate.h"
#include "TNTTableScan.h"
#include "TNTIndexScan.h"
#include "TNTDelete.h"
#include "TNTIndexTest.h"
#include "TNTPurge.h"
#endif

#include <sstream>
#include <iostream>

using namespace std;
using namespace ntseperf;

#ifdef TNT_ENGINE
using namespace tntperf;
#endif


Syslog *logger = new Syslog("perftest.log", EL_DEBUG, true, true);
ofstream *spaceOs = 0; // 空间性能输出文件

/** 输出性能测试结果 */
void logResult(ostream &os, const list<PTResult *> &results) {
	list<PTResult *>::const_iterator iter = results.begin();
	for (; iter != results.end(); ++iter) {
		os	<< (*iter)->toString() << endl;
	}
}
/**
 * 输出用例信息
 * @param os 输出流
 * @param suite 测试用例集
 * @param prefix 编号前缀
 */
void printSuite(ostream &os, TestCaller *suite, const string &prefix = "", bool verbose = false) {
	if (!prefix.empty())
		os << prefix << ": ";
	if (!suite->getChildTestCount()) {
		os << suite->getName();
		if (verbose)
			os << ": " << suite->getDescription();
		os << endl;
	} else {
		os << suite->getName() << "[" << suite->getChildTestCount() << "]" << endl;
		stringstream ss;
		for (int i = 0; i < suite->getChildTestCount(); ++i) {
			ss.str("");
			ss << prefix;
			if (!prefix.empty())
				ss << ".";
			ss << (i + 1);
			printSuite(os, suite->getChildTestAt(i), ss.str());
		}
	}
}
/**
 * 查找测试集
 * @param suiteNo 测试集编号(例如1.2)
 * @param allTests 所有测试用例
 * @return 目标测试用例集，如果suiteNo错误则返回NULL
 */
TestCaller* findSuite(const string& suiteNo, TestCaller *allTests) {
	int dotPos = (int)suiteNo.find('.', 0);
	int index = atoi(suiteNo.substr(0, dotPos).c_str());
	--index;
	if (index <0 || index >= allTests->getChildTestCount()) {
		cerr << "Invalid test suite: " << suiteNo << endl;
	} else if (dotPos != (int)string::npos) {
		return findSuite(suiteNo.substr(dotPos + 1), allTests->getChildTestAt(index));
	} else {
		return allTests->getChildTestAt(index);
	}
	return 0;
}
/**
 * 获取已选的测试集
 * @param is 输入流
 * @param allTests 所有测试用例
 * @return 已选择的测试用例集，如果输入错误则返回NULL
 */
TestSuite* getSelectedSuites(istream &is, TestSuite *allTests) {
	bool selectAll = false;
	char buf[256];
	is.getline(buf, sizeof(buf));
	istringstream iss(buf);

	vector<string> suitesInput;
	while (!iss.eof()) {
		string suite;
		iss >> suite;
		if (suite == "0" || suite == "") {
			selectAll = true;
			break;
		} else
			suitesInput.push_back(suite);
	}

	TestSuite *suite = new TestSuite("Selected");
	if (selectAll) {
		suite->addTest(allTests);
	} else {
		for (size_t i = 0; i < suitesInput.size(); ++i) {
			TestCaller *cur = findSuite(suitesInput[i], allTests);
			if (!cur) {
				suite->clear();
				delete suite;
				return 0;
			}
			suite->addTest(cur);
		}
	}
	return suite;
}


void addTests(TestSuite& allTests) {
	TestSuite *basicSuite = new TestSuite("Basic");
	allTests.addTest(basicSuite);
	TestSuite *lobSuite = new TestSuite("Lob");
	allTests.addTest(lobSuite);
	TestSuite *miscSuite = new TestSuite("Misc");
	allTests.addTest(miscSuite);
	TestSuite *simulateSuite = new TestSuite("Simulation");
	allTests.addTest(simulateSuite);
	TestSuite *backupSuite = new TestSuite("Backup");
	allTests.addTest(backupSuite);
	TestSuite *recoverSuite = new TestSuite("Recovery");
	allTests.addTest(recoverSuite);
	TestSuite *testSuite = new TestSuite("Test");
	allTests.addTest(testSuite);

	testSuite->addTestCase(new EmptyTestCase());
	
	/* 基本操作测试 */
	// 表扫描用例
	TestSuite *mmTableScanSuite = new TestSuite("Memory Table Scan Suite");
	basicSuite->addTest(mmTableScanSuite);
	mmTableScanSuite->addTestCase(new MemoryTableScanTest(false, false, false));
	mmTableScanSuite->addTestCase(new MemoryTableScanTest(true, false, false));
	mmTableScanSuite->addTestCase(new MemoryTableScanTest(true, true, false));
	mmTableScanSuite->addTestCase(new MemoryTableScanTest(false, false, true));
	mmTableScanSuite->addTestCase(new MemoryTableScanTest(true, false, true));
	mmTableScanSuite->addTestCase(new MemoryTableScanTest(true, true, true));

#ifdef PERF_SMALL_CONFIG
	// 数据量相对buffer大小的倍数
	DiskTableScanTest::VOLUMN_RATIO = 1;
	DiskIndexScanTest::VOLUMN_RATIO = 1;
	DiskRandomReadTest::VOLUMN_RATIO = 1;
#endif

	TestSuite *dsTableScanSuite = new TestSuite("Disk Table Scan Suite");
	basicSuite->addTest(dsTableScanSuite);
	dsTableScanSuite->addTestCase(new DiskTableScanTest(false, false, false));
	dsTableScanSuite->addTestCase(new DiskTableScanTest(true, false, false));
	dsTableScanSuite->addTestCase(new DiskTableScanTest(true, true, false));
	dsTableScanSuite->addTestCase(new DiskTableScanTest(false, false, true));
	dsTableScanSuite->addTestCase(new DiskTableScanTest(true, false, true));
	dsTableScanSuite->addTestCase(new DiskTableScanTest(true, true, true));


	TestSuite *mmIndexScanSuite = new TestSuite("Memory Index Scan Suite");
	basicSuite->addTest(mmIndexScanSuite);
	mmIndexScanSuite->addTestCase(new MemoryIndexScanTest(false, false, false));
	mmIndexScanSuite->addTestCase(new MemoryIndexScanTest(true, false, false));
	mmIndexScanSuite->addTestCase(new MemoryIndexScanTest(true, true, false));
	mmIndexScanSuite->addTestCase(new MemoryIndexScanTest(false, false, true));
	mmIndexScanSuite->addTestCase(new MemoryIndexScanTest(true, false, true));
	mmIndexScanSuite->addTestCase(new MemoryIndexScanTest(true, true, true));

	TestSuite *dsIndexScanSuite = new TestSuite("Disk Index Scan Suite");
	basicSuite->addTest(dsIndexScanSuite);
	dsIndexScanSuite->addTestCase(new DiskIndexScanTest(false));
	dsIndexScanSuite->addTestCase(new DiskIndexScanTest(true));

	// 随机读用例
#ifdef PERF_SMALL_CONFIG
	int nrThreads = 1;
	int loopCount = 100000 / nrThreads;
#else
	int nrThreads = 100;
	int loopCount = 1000000 / nrThreads;
#endif
	TestSuite *mmRandomReadSuite = new TestSuite("Memory Random Read Suite");
	basicSuite->addTest(mmRandomReadSuite);

	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(false, false, false, nrThreads, loopCount));
	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(true, false, false, nrThreads, loopCount));
	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(false, true, false, nrThreads, loopCount));
	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(true, true, false, nrThreads, loopCount));

	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(false, false, true, nrThreads, loopCount));
	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(true, false, true, nrThreads, loopCount));
	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(false, true, true, nrThreads, loopCount));
	mmRandomReadSuite->addTestCase(new MemoryRandomReadTest(true, true, true, nrThreads, loopCount));


	TestSuite *dsRandomReadSuite = new TestSuite("Disk Random Read Suite");
	basicSuite->addTest(dsRandomReadSuite);

	dsRandomReadSuite->addTestCase(new DiskRandomReadTest(false, false, nrThreads, loopCount));
	dsRandomReadSuite->addTestCase(new DiskRandomReadTest(true, false, nrThreads, loopCount));
	dsRandomReadSuite->addTestCase(new DiskRandomReadTest(false, true, nrThreads, loopCount));
	dsRandomReadSuite->addTestCase(new DiskRandomReadTest(true, true, nrThreads, loopCount));


	TestSuite *updatingOpSuite = new TestSuite("INSERT, DELETE, UPDATE Test Suite");
	basicSuite->addTest(updatingOpSuite);
#ifdef PERF_SMALL_CONFIG
	double dataSizeRatio = 1.1;
	int thrLoopCnt = 1000;
#else
	double dataSizeRatio = 100.0; // 外存测试数据量和buffer大小的比例
	int thrLoopCnt = 10000;
#endif
	/* 4.1.4.1 */
	updatingOpSuite->addTestCase(new FLRInsertTest(TABLE_NAME_COUNT, SMALL, 1, ASCENDANT, 0.5));
	updatingOpSuite->addTestCase(new FLRInsertTest(TABLE_NAME_COUNT, SMALL, 1, RANDOM, 0.5));
	updatingOpSuite->addTestCase(new FLRInsertTest(TABLE_NAME_COUNT, SMALL, 500, RANDOM, 0.5));
	/* 4.1.4.2 */
	updatingOpSuite->addTestCase(new FLRInsertTest(TABLE_NAME_LONGCHAR, SMALL, 1, ASCENDANT, dataSizeRatio));
	updatingOpSuite->addTestCase(new FLRInsertTest(TABLE_NAME_LONGCHAR, SMALL, 1, RANDOM, dataSizeRatio));
	updatingOpSuite->addTestCase(new FLRInsertTest(TABLE_NAME_LONGCHAR, SMALL, 500, RANDOM, dataSizeRatio));
	/* 4.1.4.3 */
	updatingOpSuite->addTestCase(new VLRInsertTest(TABLE_NAME_ACCOUNT, SMALL, 1, ASCENDANT, dataSizeRatio));
	updatingOpSuite->addTestCase(new VLRInsertTest(TABLE_NAME_ACCOUNT, SMALL, 1, RANDOM, dataSizeRatio));
	updatingOpSuite->addTestCase(new VLRInsertTest(TABLE_NAME_ACCOUNT, SMALL, 500, RANDOM, dataSizeRatio));
	/* 4.1.4.4 */
	updatingOpSuite->addTestCase(new TNTFLRInsertTest(TABLE_NAME_COUNT, SMALL, 1, ASCENDANT, 0.5));
	updatingOpSuite->addTestCase(new TNTFLRInsertTest(TABLE_NAME_COUNT, SMALL, 1, RANDOM, 0.5));
	updatingOpSuite->addTestCase(new TNTFLRInsertTest(TABLE_NAME_COUNT, SMALL, 500, RANDOM, 0.5));
	/* 4.1.4.5 */
	updatingOpSuite->addTestCase(new TNTFLRInsertTest(TABLE_NAME_LONGCHAR, SMALL, 1, ASCENDANT, dataSizeRatio));
	updatingOpSuite->addTestCase(new TNTFLRInsertTest(TABLE_NAME_LONGCHAR, SMALL, 1, RANDOM, dataSizeRatio));
	updatingOpSuite->addTestCase(new TNTFLRInsertTest(TABLE_NAME_LONGCHAR, SMALL, 500, RANDOM, dataSizeRatio));
	/* 4.1.4.6 */
	updatingOpSuite->addTestCase(new TNTVLRInsertTest(TNTTABLE_NAME_ACCOUNT, SMALL, 1, ASCENDANT, dataSizeRatio));
	updatingOpSuite->addTestCase(new TNTVLRInsertTest(TNTTABLE_NAME_ACCOUNT, SMALL, 1, RANDOM, dataSizeRatio));
	updatingOpSuite->addTestCase(new TNTVLRInsertTest(TNTTABLE_NAME_ACCOUNT, SMALL, 500, RANDOM, dataSizeRatio));
	/* 4.1.5.1 */
	updatingOpSuite->addTestCase(new UpdatePerfTest_1(MEDIUM, true, false, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new UpdatePerfTest_1(MEDIUM, false, false, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new UpdatePerfTest_1(MEDIUM, true, true, 500, thrLoopCnt));
	/* 4.1.5.2 */
	updatingOpSuite->addTestCase(new UpdatePerfTest_2(MEDIUM, true, false, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new UpdatePerfTest_2(MEDIUM, false, false, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new UpdatePerfTest_2(MEDIUM, true, true, 500, thrLoopCnt));
	/* 4.1.5.3 */
	updatingOpSuite->addTestCase(new UpdatePerfTest_3(MEDIUM, true, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new UpdatePerfTest_3(MEDIUM, false, 500, thrLoopCnt));
	/* 4.1.5.4 */
	updatingOpSuite->addTestCase(new UpdatePerfTest_4(MEDIUM, true, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new UpdatePerfTest_4(MEDIUM, false, 500, thrLoopCnt));
	/* 4.1.5.5 */
	updatingOpSuite->addTestCase(new UpdatePerfTest_5(SMALL, 500, dataSizeRatio, thrLoopCnt));
	/* 4.1.5.6 */
	updatingOpSuite->addTestCase(new UpdatePerfTest_6(SMALL, 500, dataSizeRatio, thrLoopCnt));
	/* 4.1.5.7 */
	updatingOpSuite->addTestCase(new TNTFLRUpdatePerfTest(MEDIUM, true, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new TNTFLRUpdatePerfTest(MEDIUM, false, 500, thrLoopCnt));
	/* 4.1.5.8 */
	updatingOpSuite->addTestCase(new TNTVLRUpdatePerfTest(MEDIUM, true, 500, thrLoopCnt));
	updatingOpSuite->addTestCase(new TNTVLRUpdatePerfTest(MEDIUM, false, 500, thrLoopCnt));
	/* 4.1.6.1 */
	updatingOpSuite->addTestCase(new DeletePerfTest(MEDIUM, 0.5));
	/* 4.1.6.2 */
	updatingOpSuite->addTestCase(new DeletePerfTest(SMALL, dataSizeRatio));
	/* 4.1.6.3 */
	updatingOpSuite->addTestCase(new TNTDeleteTest(MEDIUM, 0.5));

    /** TNT table test case*/
	TestSuite *tntTableTestSuite = new TestSuite("TNT Table Test Suite");
	basicSuite->addTest(tntTableTestSuite);
	tntTableTestSuite->addTestCase(new TNTTableScanTest(true, true, false));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_COUNT, SMALL, true));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_COUNT, SMALL, false));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_COUNT, MEDIUM, true));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_COUNT, MEDIUM, false));

	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_LONGCHAR, SMALL, true));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_LONGCHAR, SMALL, false));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_LONGCHAR, MEDIUM, true));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_LONGCHAR, MEDIUM, false));

	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_ACCOUNT, SMALL, true));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_ACCOUNT, SMALL, false));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_ACCOUNT, MEDIUM, true));
	tntTableTestSuite->addTestCase(new TNTPurgePerfTest(TNTTABLE_NAME_ACCOUNT, MEDIUM, false));

	/**TNT Index performance test */
	TestSuite *tntIndexTestSuite = new TestSuite("TNT Index Test Suite");
	basicSuite->addTest(tntIndexTestSuite);
	tntIndexTestSuite->addTestCase(new TNTIndexScanTest(true, true, false));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, true, false, false, PURGE_TABLE));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, false, false, false, PURGE_TABLE));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, true, false, false, PURGE_TABLE));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, false, false, false, PURGE_TABLE));	

	tntIndexTestSuite->addTestCase(new TNTIndexScanTest(true, true, false));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, true, false, false, BEGIN_END_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, false, false, false, BEGIN_END_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, true, false, false, BEGIN_END_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, false, false, false, BEGIN_END_SCAN));	

	tntIndexTestSuite->addTestCase(new TNTIndexScanTest(true, true, false));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, true, false, false, UNIQUE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, false, false, false, UNIQUE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, true, false, false, UNIQUE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, false, false, false, UNIQUE_SCAN));	

	tntIndexTestSuite->addTestCase(new TNTIndexScanTest(true, true, false));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, true, false, false, RANGE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, false, false, false, RANGE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, true, false, false, RANGE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, false, false, false, RANGE_SCAN));

	tntIndexTestSuite->addTestCase(new TNTIndexScanTest(true, true, false));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, true, false, false, TABLE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, SMALL, false, false, false, TABLE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, true, false, false, TABLE_SCAN));
	tntIndexTestSuite->addTestCase(new TNTIndexTest(TNTTABLE_NAME_BLOG, MEDIUM, false, false, false, TABLE_SCAN));	
	/* 大对象测试 */
	u64 configSize = CommonDbConfig::getMedium()->m_pageBufSize * Limits::PAGE_SIZE;

	//1、大对象的insert
	TestSuite *lobInsertSuite = new TestSuite("Lob Insert Suite");
	lobSuite->addTest(lobInsertSuite);
	lobInsertSuite->addTestCase(new LobInsertTest(true, configSize / 2, 4, true));
	lobInsertSuite->addTestCase(new LobInsertTest(true, configSize * 5, 1024, false));
	lobInsertSuite->addTestCase(new LobInsertTest(true, configSize * 10, 64 * 1024, false));

	//2、大对象的read
	TestSuite *lobReadSuite = new TestSuite("Lob Read Suite");
	lobSuite->addTest(lobReadSuite);
	lobReadSuite->addTestCase(new LobReadTest(true, configSize / 2,  4, true, true));
	lobReadSuite->addTestCase(new LobReadTest(true, configSize * 5, 1024, false, true));
	lobReadSuite->addTestCase(new LobReadTest(true, configSize * 10, 64 * 1024, false, true));
	lobReadSuite->addTestCase(new LobReadTest(true, configSize * 10, 64 * 1024, false, false));

	//3、大对象的del
	TestSuite *lobDelSuite = new TestSuite("Lob Del Suite");
	lobSuite->addTest(lobDelSuite);
	lobDelSuite->addTestCase(new LobDelTest(true, configSize / 2, 4, true));
	lobDelSuite->addTestCase(new LobDelTest(true, configSize * 5, 1024, false));
	lobDelSuite->addTestCase(new LobDelTest(true, configSize * 5, 64 * 1024, false));

	//4、大对象的更新
	TestSuite *lobUpdateSuite = new TestSuite("Lob Update Suite");
	lobSuite->addTest(lobUpdateSuite);
	lobUpdateSuite->addTestCase(new LobUpdateTest(true, configSize / 2, 4, 4));
	lobUpdateSuite->addTestCase(new LobUpdateTest(true, configSize * 5, 1024, 1024));
	lobUpdateSuite->addTestCase(new LobUpdateTest(true, configSize * 5, 1024, 64 * 1024));
	lobUpdateSuite->addTestCase(new LobUpdateTest(true, configSize * 5, 64 * 1024, 64 * 1024));
	lobUpdateSuite->addTestCase(new LobUpdateTest(true, configSize * 5, 64 * 1024, (64 + 8) * 1024));

	//5、大对象的碎片整理
	TestSuite *lobDefragSuite = new TestSuite("Lob Defrag Suite");
	lobSuite->addTest(lobDefragSuite);
	lobDefragSuite->addTestCase(new LobDefragTest(true, configSize * 5, 64 * 1024));


	/* 多才多艺的测试 */
	TestSuite *indexCreationSuite = new TestSuite("Index Creation Suite");
	miscSuite->addTest(indexCreationSuite);

#ifdef PERF_SMALL_CONFIG
	indexCreationSuite->addTestCase(new IndexCreationTest(1 * 1024 * 1024));
#else
	indexCreationSuite->addTestCase(new IndexCreationTest(16 * 1024 * 1024));
	indexCreationSuite->addTestCase(new IndexCreationTest(32 * 1024 * 1024));
	indexCreationSuite->addTestCase(new IndexCreationTest(64 * 1024 * 1024));
	indexCreationSuite->addTestCase(new IndexCreationTest(128 * 1024 * 1024));
	indexCreationSuite->addTestCase(new IndexCreationTest(256 * 1024 * 1024));
	indexCreationSuite->addTestCase(new IndexCreationTest(512 * 1024 * 1024));
#endif

	TestSuite *dbBackupSuite = new TestSuite("Database Backup Suite");
	miscSuite->addTest(dbBackupSuite);

	dbBackupSuite->addTestCase(new DBBackupTest(false, 0, 0));
	dbBackupSuite->addTestCase(new DBBackupTest(true, 0, 0));
	dbBackupSuite->addTestCase(new DBBackupTest(true, 600, 100));
	dbBackupSuite->addTestCase(new DBBackupTest(true, 60, 10));

	TestSuite *dbRecoverySuite = new TestSuite("Database Recovery Suite");
	miscSuite->addTest(dbRecoverySuite);

	dbRecoverySuite->addTestCase(new DBRecoveryTest(200 * 1024 * 1024));

	miscSuite->addTestCase(new BufferReplacePolicyTest());

	TestSuite *synthesisSuite = new TestSuite("Synthesis Suite");
	basicSuite->addTest(synthesisSuite);

	synthesisSuite->addTestCase(new SynthesisTest(false, true, 1, 100));
	synthesisSuite->addTestCase(new SynthesisTest(false, false, nrThreads, 1000));
	synthesisSuite->addTestCase(new SynthesisTest(true, false, nrThreads, 1000));

	TestSuite *mmsSynthesisSuite = new TestSuite("Mms Synthesis Suite");
	basicSuite->addTest(mmsSynthesisSuite);

	mmsSynthesisSuite->addTestCase(new MmsPerfTestCase(true));
	mmsSynthesisSuite->addTestCase(new MmsPerfTestCase(true, true));

	TestSuite *mmsReplacePolicySuite = new TestSuite("Mms Replace Policy Suite");
	basicSuite->addTest(mmsReplacePolicySuite);

	mmsReplacePolicySuite->addTestCase(new MmsReplacePolicyTest());
};


int main(int argc, char* argv[]) {
	GlobalFactory::getInstance();
	Tracer::init();

	spaceOs = new ofstream("SpaceResults.txt");
	TestSuite allTests("All");
	addTests(allTests);
	TestSuite *selectedSuite = 0;
	if (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-h"))) {
		cout << argv[0] << "[-l loglevel] [suites]" << endl;
		cout << "  suites	A list of selected suites seperated with blank, such as '1 2.3'" << endl;
		cout << "  -l loglevel	debug|warn|log|error|panic" << endl;
		return 0;
	} else if (argc > 1) {
		stringstream ss;
		int i = 1;
		while (i < argc) {
			char *cur = argv[i++];
			if (strcmp(cur, "-l") == 0) { // 日志级别
				if (i >= argc) {
					cerr << "no log level provided" << endl;
					return 1;
				}
				cur = argv[i++];
				if (strcmp(cur, "debug") == 0) {
					logger->setErrLevel(EL_DEBUG);
				} else if (strcmp(cur, "warn") == 0) {
					logger->setErrLevel(EL_WARN);
				} else if (strcmp(cur, "log") == 0) {
					logger->setErrLevel(EL_LOG);
				} else if (strcmp(cur, "error") == 0) {
					logger->setErrLevel(EL_ERROR);
				} else if (strcmp(cur, "panic") == 0) {
					logger->setErrLevel(EL_PANIC);
				} else {
					cerr << "invalid log level" << endl;
					return 1;
				}
				continue;
			}
			ss << cur;
			if (i == argc) {
				ss << endl;
			} else {
				ss << " ";
			}
		}		
		selectedSuite = getSelectedSuites(ss, &allTests);
	} else {
		cout << "Select test suites to run, 0 for all test suites. "
			"You can select multiple suites, seperated with blank. " << endl;
		printSuite(cout, &allTests);
		selectedSuite = getSelectedSuites(cin, &allTests);
	}

	if (!selectedSuite) // 输入错误
		return 1;

	FileResultListener results("PerformanceResults.txt", true);
	u64 before = System::currentTimeMillis();
	selectedSuite->run(&results);
	u64 after = System::currentTimeMillis();
	logger->log(EL_LOG, "TestSuite run time %u(s)", (after - before) / 1000);

	Tracer::exit();
	GlobalFactory::freeInstance();

	selectedSuite->clear();
	delete selectedSuite;
	spaceOs->close();
	delete spaceOs;
	delete logger;
	return 0;
}


