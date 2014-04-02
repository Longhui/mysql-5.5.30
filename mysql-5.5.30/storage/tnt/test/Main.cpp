/**
 * NTSE单元测试
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/Test.h>
#include <cppunit/TestSuite.h>

#include <iostream>
#include <sstream>
#include <vector>
//#include <vld.h>

// 在这里包含各测试类的头文件, 注意按序排列
/*********************************************
 * NTSE相关
 *********************************************/
#ifdef NTSE_KEYVALUE_SERVER
#include "keyvalue/TestKeyValueHandler.h"
#include "keyvalue/TestKeyValueServer.h"
#endif
#include "misc/GlobalFactory.h"
#include "misc/Trace.h"
#include "Test.h"
//#include "TestRowCache.h"

#include "api/TestDatabase.h"
#include "api/TestRecover.h"
#include "api/TestBackup.h"
#include "api/TestIdxPreAlter.h"
#include "api/TestLobDefraggler.h"
#include "api/TestRecvDDL.h"
#include "api/TestRecvInsert.h"
#include "api/TestRecvUpdate.h"
#include "api/TestRecvDelete.h"
#include "api/TestTable.h"
#include "api/TestTableArgAlter.h"
#include "api/TestTblMnt.h"
#include "api/TestTNTRecover.h"
#include "api/TestTNTOnlineDDL.h"
#include "btree/TestIndex.h"
#include "btree/TestIndexOperation.h"
#include "compress/TestRCMSmplTbl.h"
#include "compress/TestRowCompress.h"
#include "compress/TestSDAT.h"
#include "compress/TestSmplTrie.h"
#include "misc/TestCompressRecord.h"
#include "misc/TestDldLock.h"
#include "heap/TestFLRHeap.h"
#include "heap/TestVLRHeap.h"
#include "heap/TestHashIndex.h"
#include "heap/TestMHeap.h"
#include "heap/TestMHeapRecord.h"
#include "heap/TestVersionPool.h"
#include "lob/TestLobDefrag.h"
#include "lob/TestLobOper.h"
#include "misc/TestLockManager.h"
#include "misc/TestBuffer.h"
#include "misc/TestControlFile.h"
#include "misc/TestProfile.h"
#include "misc/TestParser.h"
#include "misc/TestParFileParser.h"
#include "misc/TestRecord.h"
#include "misc/TestResourcePool.h"
#include "misc/TestMemoryContext.h"
#include "misc/TestTableDef.h"
#include "misc/TestTxnLog.h"
#include "mms/TestMms.h"
#include "rec/TestMRecords.h"
#include "util/TestBitmap.h"
#include "util/TestFile.h"
#include "util/TestHash.h"
#include "util/TestMutex.h"
#include "util/TestNumCom.h"
#include "util/TestPool.h"
#include "util/TestRWLock.h"
#include "util/TestSync.h"
#include "util/TestThread.h"
#include "util/TestSystem.h"
#include "util/TestUtil.h"

/*********************************************
 * TNT相关
 *********************************************/
#include "api/TestTNTDatabase.h"
#include "api/TestTNTTable.h"
#include "api/TestTNTLob.h"
#include "btree/TestMIndexOperation.h"
#include "btree/TestMIndex.h"
#include "btree/TestTNTIndex.h"
#include "trx/TestTNTTransaction.h"
#include "api/TestTNTLob.h"
#include "api/TestTNTBackup.h"
#include "misc/TestConfig.h"

using namespace std;
using namespace CPPUNIT_NS;

/** 表示一个测试类的信息 */
class TestInfo {
public:
	TestInfo(TestSuite *suite, const char *name, const char *desc, bool big) {
		m_suite = suite;
		m_name = name;
		m_desc = desc;
		m_big = big;
	}

	virtual ~TestInfo() {
		delete m_suite;
	}

	TestSuite* getSuite() {
		return m_suite;
	}

	const char* getName() {
		return m_name;
	}

	const char* getDescription() {
		return m_desc;
	}

	bool isBig() {
		return m_big;
	}

private:
	TestSuite	*m_suite;
	const char	*m_name;
	const char	*m_desc;
	bool		m_big;
};

/** 测试用例分类 */
class TestCategory {
public:
	TestCategory(const char *name) {
		m_name = name;
	}

	virtual ~TestCategory() {
		for (size_t i = 0; i < m_tests.size(); i++) {
			TestInfo *ti = m_tests[i];
			delete ti;
		}
	}

	const char* getName() {
		return m_name;
	}
	
	void addTest(TestInfo *test) {
		m_tests.push_back(test);
	}

	size_t getNumTests() {
		return m_tests.size();
	}

	TestInfo* getTest(size_t i) {
		return m_tests[i];
	}

private:
	vector<TestInfo *>	m_tests;
	const char	*m_name;
};

template<typename T>
static void addTest(TestCategory *testCategory, T *test) {
	testCategory->addTest(new TestInfo(T::suite(), T::getName(), T::getDescription(), T::isBig()));
}

struct SuiteRef {
	TestCategory	*category;
	TestInfo		*test;
	
	SuiteRef(TestCategory *category, TestInfo *test) {
		this->category = category;
		this->test = test;
	}
};

void reportResults(const char *reportName, TestResultCollector *result) {
	// 输出错误报表
	CPPUNIT_NS::OFileStream os(reportName);
	os << "<test>";
	os << "<total>" << result->runTests() << "</total>";
	os << "<success>" << result->runTests() - result->testFailuresTotal() << "</success>";
	os << "<failure>" << result->testFailuresTotal() << "</failure>";
	os << "<message><![CDATA[";
	CompilerOutputter reportOutputter(result, os);
	reportOutputter.printFailureReport();
	os << "]]></message>";
	os << "</test>";
}

int main(int argc, char* argv[]) {
	TestResult controller;
	TestResultCollector result;
	controller.addListener(&result);

	BriefTestProgressListener progress;
	controller.addListener(&progress);

	vector<TestCategory *>	allTests;
	TestCategory utilSuite("Utilities");
	TestCategory miscSuite("Misc facilities");
	TestCategory heapSuite("Heap");
	TestCategory indexSuite("Index");
	TestCategory mmsSuite("MMS");
	TestCategory recSuite("Records");
	TestCategory lobSuite("Large object");
	TestCategory apiSuite("API");
	TestCategory mysqlSuite("MySQL");
	TestCategory rowCompressSuite("Row Compress");
	TestCategory keyvalueSuite("KeyValue");
	TestCategory tntIndexSuite("TNT Index index");
	TestCategory tntDatabaseSuite("TNT Database");
	TestCategory tntTableSuite("TNT Table");
	TestCategory tntTransactionSuite("TNT Transaction");

	allTests.push_back(&utilSuite);
	allTests.push_back(&miscSuite);
	allTests.push_back(&heapSuite);
	allTests.push_back(&indexSuite);
	allTests.push_back(&mmsSuite);
	allTests.push_back(&recSuite);
	allTests.push_back(&lobSuite);
	allTests.push_back(&apiSuite);
	allTests.push_back(&mysqlSuite);
	allTests.push_back(&rowCompressSuite);
	allTests.push_back(&keyvalueSuite);
	allTests.push_back(&tntIndexSuite);
	allTests.push_back(&tntDatabaseSuite);
	allTests.push_back(&tntTableSuite);
	allTests.push_back(&tntTransactionSuite);

	// begin: 在这里将所有测试用例加到对应的测试集中
	addTest(&utilSuite, (UtilTestCase *)0);
	addTest(&utilSuite, (MutexTestCase *)0);
	addTest(&utilSuite, (MutexBigTestCase *)0);
	addTest(&utilSuite, (RWLockTestCase *)0);
	addTest(&utilSuite, (RWLockBigTestCase *)0);
	addTest(&utilSuite, (SyncTestCase *)0);
	addTest(&utilSuite, (SyncBigTest *)0);
	addTest(&utilSuite, (ThreadTestCase *)0);
	addTest(&utilSuite, (HashTestCase *)0);
	addTest(&utilSuite, (HashBigTest *)0);
	addTest(&utilSuite, (FileTestCase *)0);
	addTest(&utilSuite, (FileBigTest *)0);
	addTest(&utilSuite, (PoolTestCase *)0);
	addTest(&utilSuite, (NumberCompressTestCase *)0);
	addTest(&utilSuite, (NumberCompressBigTest *)0);
	addTest(&utilSuite, (BitmapTestCase *)0);
	addTest(&utilSuite, (SystemBigTest *)0);

	addTest(&miscSuite, (LockManagerTestCase *)0);
	addTest(&miscSuite, (LockManagerBigTest *)0);
	addTest(&miscSuite, (IndicesLockManagerTestCase *)0);
	addTest(&miscSuite, (IndicesLockManagerBigTestCase *)0);
	addTest(&miscSuite, (BufferTestCase *)0);
	addTest(&miscSuite, (BufferBigTest *)0);
	addTest(&miscSuite, (RecordTestCase *)0);
	addTest(&miscSuite, (RecordBigTest *)0);
	addTest(&miscSuite, (RecordConvertTest *)0);
	addTest(&miscSuite, (CtrlFileTestCase *)0);
	addTest(&miscSuite, (TxnLogTestCase *)0);
	addTest(&miscSuite, (TxnLogBigTest *)0);
	addTest(&miscSuite, (MemoryContextTestCase *)0);
	addTest(&miscSuite, (TableDefTestCase *)0);
#ifdef NTSE_PROFILE
	addTest(&miscSuite, (ProfileTestCase *)0);
#endif
	addTest(&miscSuite, (ParserTestCase *)0);
	addTest(&miscSuite, (ParFileParserTestCase *)0);
	addTest(&miscSuite, (RecordCompressTestCase *)0);
	addTest(&miscSuite, (ResourcePoolTestCase *)0);
	addTest(&miscSuite, (DldLockTestCase*)0);
	addTest(&miscSuite, (ConfigTestCase *)0);
	addTest(&miscSuite, (TNTConfigTestCase*)0);

	addTest(&heapSuite, (FLRHeapTestCase *)0);
	//addTest(&heapSuite, (FLRHeapPerformanceTestCase *)0);
	addTest(&heapSuite, (VLRHeapTestCase *)0);
	addTest(&heapSuite, (HashIndexTestCase *)0);
	addTest(&heapSuite, (MHeapTestCase *)0);
	addTest(&heapSuite, (MHeapRecordTestCase *)0);
	addTest(&heapSuite, (VersionPoolTestCase *)0);

	addTest(&indexSuite, (IndexTestCase *)0);
	addTest(&indexSuite, (IndexOperationTestCase *)0);
	addTest(&indexSuite, (IndexRecoveryTestCase *)0);
	addTest(&indexSuite, (IndexOPStabilityTestCase *)0);
	addTest(&indexSuite, (IndexSMOTestCase *)0);
	addTest(&indexSuite, (IndexBugsTestCase *)0);

	addTest(&mmsSuite, (MmsTestCase *)0);

	addTest(&recSuite, (MRecordsTestCase *)0);

	addTest(&lobSuite, (LobOperTestCase *)0);
	addTest(&lobSuite, (LobDefragTestCase *)0);

	addTest(&apiSuite, (TableTestCase *)0);
	addTest(&apiSuite, (TableBigTest *)0);
	addTest(&apiSuite, (DatabaseTestCase *)0);
	addTest(&apiSuite, (BackupTestCase *)0);
	addTest(&apiSuite, (RecoverTestCase *)0);
	addTest(&apiSuite, (RecvDDLTestCase *)0);
	addTest(&apiSuite, (RecvInsertTestCase *)0);
	addTest(&apiSuite, (RecvUpdateTestCase *)0);
	addTest(&apiSuite, (RecvDeleteTestCase *)0);	
	addTest(&apiSuite, (TableAlterArgTestCase *)0);
	addTest(&apiSuite, (TableAlterArgBigTestCase *)0);
	addTest(&apiSuite, (LobDefragglerTestCase *)0);
	addTest(&apiSuite, (TblMntTestCase *)0);
	addTest(&apiSuite, (IdxPreAlterTestCase *)0);

	//addTest(&mysqlSuite, (RowCacheTestCase *)0);

	//Row Compress
	addTest(&rowCompressSuite, (SmplTrieTestCase *)0);
	addTest(&rowCompressSuite, (SmplTrieNodeTestCase *)0);
	addTest(&rowCompressSuite, (SmplTrieDListTestCase *)0);
	addTest(&rowCompressSuite, (SmplTrieMaxHeapTestCase *)0);
	addTest(&rowCompressSuite, (RCDictionaryTestCase *)0);
	addTest(&rowCompressSuite, (TrieTestCase *)0);
	addTest(&rowCompressSuite, (RCMSmplTblTestCase *)0);
	addTest(&rowCompressSuite, (RowCompressTestCase *)0);
	addTest(&rowCompressSuite, (RecordCompressTestCase *)0);

#ifdef NTSE_KEYVALUE_SERVER
	addTest(&keyvalueSuite, (KeyValueHandlerTestCase *)0);
	addTest(&keyvalueSuite, (KeyValueServerTestCase *)0);
#endif

	addTest(&tntIndexSuite, (MIndexPageTestCase *)0);
	addTest(&tntIndexSuite, (MIndexTestCase *)0);
	addTest(&tntIndexSuite, (MIndexOPStabilityTestCase *)0);
	addTest(&tntIndexSuite, (TNTIndiceTestCase *)0);
	addTest(&tntIndexSuite, (TNTIndexTestCase *)0);

	addTest(&tntDatabaseSuite, (TNTDatabaseTestCase *)0);
	addTest(&tntDatabaseSuite, (TNTRecoverTestCase *)0);
	addTest(&tntDatabaseSuite, (TNTBackupTestCase *)0);
	addTest(&tntDatabaseSuite, (TNTOnlineDDLTestCase *)0);
	
	addTest(&tntTableSuite, (TNTTableTestCase *)0);
	addTest(&tntTableSuite, (TNTLobTestCase *)0);

	addTest(&tntTransactionSuite, (TNTTransactionTestCase *)0);

	// end: 在这里将所有测试用例加到对应的测试集中

	GlobalFactory::getInstance();
	Tracer::init();

	vector<string> suitesInput;
	bool selectAll = false;
	bool runBigTest = false;
	vector<SuiteRef> selectedSuites;
	bool selectIndividual = false;
	string reportName;
	if (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-h"))) {
		cout << argv[0] <<" [-f reportFileName][[-b] suites]" << endl;
		cout << "Arguments:" << endl;
		cout << "  suites	A list of selected suites seperated with blank, such as '1 2.3'" << endl; 
		cout << "  -b		Run big test" << endl;
		return 0;
	} else if (argc > 1) {
		int i = 1;
		if (!strcmp(argv[i], "-f")) {
			assert(argc > 2);
			reportName = argv[++i];
			i++;
		}
		if (!strcmp(argv[i], "-b")) {
			runBigTest = true;
			i++;
		}
		while (i < argc) {
			if (!strcmp(argv[i], "0")) {
				selectAll = true;
				break;
			} else
				suitesInput.push_back(argv[i++]);
		}
	} else {
		// 显示所有测试的列表，并要求用户选择要运行的测试和是否运行大型测试
		cout << "Select test suites to run, 0 for all test suites. You can select multiple suites, seperated with blank. You can select level 2 suites, such as 1.2[0]" << endl;
		for (size_t i = 0; i < allTests.size(); i++) {
			TestCategory *testCategory = allTests[i];
			cout << i + 1 << ": " << testCategory->getName() << "[" << testCategory->getNumTests() << "]:" << endl;
			for (size_t j = 0; j < testCategory->getNumTests(); j++) {
				TestInfo *testInfo = testCategory->getTest(j);
				cout << "  " << i + 1 << "." << j + 1 << ": " << testInfo->getName() << ": " << testInfo->getDescription() << "[big:" << testInfo->isBig() << "];" << endl;
			}
		}

		char buf[256];
		cin.getline(buf, sizeof(buf));
		istringstream iss(buf);
	
		while (!iss.eof()) {
			string suite;
			iss >> suite;
			if (suite == "0" || suite == "") {
				selectAll = true;
				break;
			} else
				suitesInput.push_back(suite);
		}
	}
		
	if (selectAll) {
		selectedSuites.clear();
		for (size_t i = 0; i < allTests.size(); i++)
			selectedSuites.push_back(SuiteRef(allTests[i], NULL));
	}
	else {
		for (size_t s = 0; s < suitesInput.size(); s++) {
			string suite = suitesInput[s];
			int dotPos = (int)suite.find('.', 0);
			int cate;
			if (dotPos > 0) {
				cate = atoi(suite.substr(0, dotPos).c_str());
				int test = atoi(suite.substr(dotPos + 1, suite.size() - dotPos - 1).c_str());
				if (cate <= 0 || (size_t)cate > allTests.size()) {
					cout << "Invalid test suite: " << cate << endl;
					return 1;
				}
				TestCategory *category = allTests[cate - 1];
				if (test < 0 || (size_t)test > category->getNumTests()) {
					cout << "Invalid test suite: " << cate << "." << test << endl;
					return 1;
				}
				selectedSuites.push_back(SuiteRef(category, category->getTest(test - 1)));
			} else {
				cate = atoi(suite.c_str());
				if (cate < 0 || (size_t)cate > allTests.size()) {
					cout << "Invalid test suite: " << cate << endl;
					return 1;
				}
				selectedSuites.push_back(SuiteRef(allTests[cate - 1], NULL));
			}
		}
	}
	
	if (argc == 1) {
		// 如果只选择了一个具体的测试类，允许进一步选择一个测试用例
		if (selectedSuites.size() == 1 && selectedSuites[0].test != NULL) {
			cout << "Select individual test case(y/n)[y]?" << endl;
			string s;
			cin >> s;
			if (s != "n" && s != "N") {
				selectIndividual = true;
				cout << "Select individual test case for test class " << selectedSuites[0].test->getName() 
					<< ", only one test case can be selected." << endl;
				
				TestSuite *suite = selectedSuites[0].test->getSuite();
				for (int i = 0; i < suite->getChildTestCount(); i++) {
					Test *test = suite->getChildTestAt(i);
					cout << "  " << (i + 1) << ": " << test->getName() << endl;
				}
				int i;
				cin >> i;
				if (i <= 0 || i > suite->getChildTestCount()) {
					cout << "Invalid test: " << i << endl;
					return 1;
				}

				cout << "Essential mode[y/n]?" << endl;
				cin >> s;
				if (s == "y" || s == "Y")
					setEssentialOnly(true);
				else
					setEssentialOnly(false);

				suite->getChildTestAt(i - 1)->run(&controller);
			}
		}
	}
	
	if (argc == 1) {
		string s;
		if (!selectIndividual && selectedSuites.size() > 1) {
			cout << "Run big tests(y/n)[n]?" << endl;
			cin >> s;
			if (s == "y" || s == "Y")
				runBigTest = true;
		}

		if (!selectIndividual) {
			cout << "Run non-essential tests(y/n)[y]?" << endl;
			cin >> s;
			if (s == "y" || s == "Y")
				setEssentialOnly(false);
			else
				setEssentialOnly(true);
		}
	}
	
	if (!selectIndividual) {
		for (size_t i = 0; i < selectedSuites.size(); i++) {
			TestCategory *testCategory = selectedSuites[i].category;
			if (selectedSuites[i].test == NULL) {
				for (size_t j = 0; j < testCategory->getNumTests(); j++) {
					TestInfo *testInfo = testCategory->getTest(j);
					if (!testInfo->isBig() || runBigTest) {
						TestSuite *suite = testInfo->getSuite();
						suite->run(&controller);
					}
				}
			} else {
				selectedSuites[i].test->getSuite()->run(&controller);
			}
		}
	}


	CompilerOutputter outputter(&result, CPPUNIT_NS::stdCOut());
	outputter.write();
	if (reportName != "")
		reportResults(reportName.c_str(), &result);

	Tracer::exit();
	GlobalFactory::freeInstance();

	return result.wasSuccessful() ? 0 : 1;
}

