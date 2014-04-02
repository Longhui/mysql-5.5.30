#include <cppunit/BriefTestProgressListener.h>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/TestRunner.h>
#include <cppunit/Test.h>
#include <cppunit/TestSuite.h>
#include "cppunit/ui/text/TestRunner.h"

#include "util/SmartPtr.h"
#include "misc/Trace.h"
#include "misc/GlobalFactory.h"

#include "MemHeapTest.h"
#include "AccountTableTest.h"
#include "UtilTest.h"
#include "TestSbHeap.h"
#include "TestSblMms.h"
#include "LobStabilityTest.h"
#include "IdxStabilityTest.h"

using namespace CPPUNIT_NS;



TestSuite* buildTests() {
	TestSuite *allTest = new TestSuite("all");
	
	// ��Ԫ�������� 
	TestSuite *unitTest = new TestSuite("unittest");
	allTest->addTest(unitTest);
	unitTest->addTest(MemHeapTestCase::suite());
	unitTest->addTest(AccountTableTestCase::suite());
	unitTest->addTest(UtilTestCase::suite());


	// ����������ģ���ȶ��Բ�������
	allTest->addTest(HeapStabilityTestCase::suite());
	allTest->addTest(MmsSblTestCase::suite());
	allTest->addTest(LobSblTestCase::suite());
	allTest->addTest(IndexStabilityTestCase::suite());
	return allTest;

	
}

/**
 * ���������Ϣ
 * @param os �����
 * @param suite ����������
 * @param prefix ���ǰ׺
 */
void printSuite(ostream &os, Test *suite, const string &prefix = "") {
	if (!prefix.empty())
		os << prefix << ": ";
	if (!suite->getChildTestCount()) {
		os << suite->getName() << endl;
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
 * ���Ҳ��Լ�
 * @param suiteNo ���Լ����(����1.2)
 * @param allTests ���в�������
 * @return Ŀ����������������suiteNo�����򷵻�NULL
 */
Test* findSuite(const string& suiteNo, Test *allTests) {
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
 * ��ȡ��ѡ�Ĳ��Լ�
 * @param is ������
 * @param allTests ���в�������
 * @return ��ѡ��Ĳ����������������������򷵻�NULL
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
		if (suite == "0") {
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
			Test *cur = findSuite(suitesInput[i], allTests);
			if (!cur) {
				return suite;
			}
			suite->addTest(cur);
		}
	}
	return suite;
}



TestSuite* getSelectedTests(TestSuite *allTest, int argc, char **argv) {


	if (argc == 1) { // û��ָ������
		printSuite(cout, allTest);
		return getSelectedSuites(cin, allTest);
	} else {
		stringstream ss;
		for (int i = 1; i < argc; ++i) {
			ss << argv[i];
			if (i != argc - 1)
				ss << " ";
		}
		return getSelectedSuites(ss, allTest);
	}

	return 0;
}


int main (int argc, char **argv) {
	TestSuite *allTest = buildTests();
	TestSuite *selected = getSelectedTests(allTest, argc, argv);
	if (!selected) {
		delete allTest;
		return 1;
	}
	GlobalFactory::getInstance();
	Tracer::init();

	TextUi::TestRunner runner;
	BriefTestProgressListener progress;
	runner.eventManager().addListener(&progress);
	runner.addTest(selected);
	runner.run("", true);
	
	int ret =  runner.result().wasSuccessful() ? 0 : 1;

	Tracer::exit();
	GlobalFactory::freeInstance();

	return ret;
}

