#include "FuncTest.h"
#include "misc/Syslog.h"
#include "misc/Trace.h"
#include "misc/GlobalFactory.h"
#include "ConsistenceAndDurability.h"
#include "AtomicOne.h"
#include "AtomicTwo.h"
#include "Stability.h"
#include <sstream>
#include <iostream>

using namespace std;
using namespace ntse;
using namespace ntsefunc;

Syslog *logger = new Syslog("functest.log", EL_DEBUG, true, true);


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


int main (int argc, char* argv[]) {
	GlobalFactory::getInstance();
	Tracer::init();

	TestSuite allTests("All");

	// 添加具体测试用例
	TestSuite *validitySuite = new TestSuite("Validity");
	allTests.addTest(validitySuite);
	TestSuite *stabilitySuite = new TestSuite("Stability");
	allTests.addTest(stabilitySuite);

	TestSuite *dbAtomicTestSuite1 = new TestSuite("Database validity test(A Test)");
	validitySuite->addTest(dbAtomicTestSuite1);
	dbAtomicTestSuite1->addTestCase(new AtomicOne(100, 500));
	//dbAtomicTestSuite1->addTestCase(new AtomicTwo(100, 500));
	dbAtomicTestSuite1->addTestCase(new AtomicTwo(100, 100));

	TestSuite *dbCDTestSuite = new TestSuite("Database validity test(C/D Test)");
	validitySuite->addTest(dbCDTestSuite);
	dbCDTestSuite->addTestCase(new CDTest(Limits::PAGE_SIZE * 50, 500));

	stabilitySuite->addTestCase(new Stability(500, 100, 5 * 60 * 1000, 30 * 60 * 1000, 10 * 60 * 1000));

	TestSuite *selectedSuite = 0;
	if (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-h"))) {
		cout << argv[0] << " [suites]" << endl;
		cout << "  suites	A list of selected suites separated with blank, such as '1 2.3'" << endl;
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
			"You can select multiple suites, separated with blank. " << endl;
		printSuite(cout, &allTests);
		selectedSuite = getSelectedSuites(cin, &allTests);
	}

	if (!selectedSuite) // 输入错误
		return 1;

	u64 before = System::currentTimeMillis();
	selectedSuite->run();
	u64 after = System::currentTimeMillis();
	logger->log(EL_LOG, "TestSuite run time %u(s)", (after - before) / 1000);

	Tracer::exit();
	GlobalFactory::freeInstance();

	selectedSuite->clear();
	delete selectedSuite;
	delete logger;
	return 0;
}