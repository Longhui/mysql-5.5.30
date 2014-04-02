#include "FuncTest.h"
#include "DbConfigs.h"
#include "util/File.h"
#include "util/System.h"
#include "misc/Syslog.h"
#include <sstream>
#include <list>

using namespace std;

namespace ntsefunc {

//////////////////////////////////////////////////////////////////////////
/////TestCaller
//////////////////////////////////////////////////////////////////////////
TestCaller::TestCaller(TestCase *testcase) 
	: m_testcase(testcase) {
}

TestCaller::~TestCaller() {
	delete m_testcase;
}

/** 子测试个数 */
int TestCaller::getChildTestCount() {
	return 0;
}

/** 获取子测试 */
TestCaller* TestCaller::getChildTestAt(int index) {
	UNREFERENCED_PARAMETER(index);
	return 0;
}

/** 获取本测试名称 */
string TestCaller::getName() const {
	return m_testcase->getName();
}

/** 获取本测试描述 */
string TestCaller::getDescription() const {
	return m_testcase->getDescription();
}

/** 测试用例测试主体 */
void TestCaller::run() {
	logger->log(EL_DEBUG, "Begin case %s", m_testcase->getName().c_str());
	// 初始化
	logger->log(EL_DEBUG, "setUp");
	m_testcase->setUp();
	// 装载数据
	u64 totalRecSize, recCnt;
	logger->log(EL_DEBUG, "loadData");
	m_testcase->loadData(&totalRecSize, &recCnt);
	logger->log(EL_DEBUG, "dataSize "I64FORMAT"u recCnt "I64FORMAT"u", totalRecSize, recCnt);
	// 预热
	logger->log(EL_DEBUG, "warmUp");
	m_testcase->warmUp();
	// 开始测试
	logger->log(EL_DEBUG, "run");
	m_testcase->run();
	// 验证正确性
	logger->log(EL_DEBUG, "verify");
	result(m_testcase->verify());
	// 清理环境
	logger->log(EL_DEBUG, "tearDown");
	m_testcase->tearDown();	
}


//////////////////////////////////////////////////////////////////////////
//// RepeatedTestCaller
//////////////////////////////////////////////////////////////////////////
string RepeatedTestCaller::getName() const {
	stringstream ss;
	ss << "[RPT:" << m_runCnt << "]" << m_caller->getName();
	return ss.str();
}

//////////////////////////////////////////////////////////////////////////
/////测试集
//////////////////////////////////////////////////////////////////////////
/**
 * 往测试集中增加一个用例
 * @param testcase 测试用例对象
 */
void TestSuite::addTestCase(TestCase *testcase) {
	m_cases.push_back(new TestCaller(testcase));
}
/**
 * 往测试集中增加一个测试运行对象
 * @param testcaller 测试运行对象
 */
void TestSuite::addTest(TestCaller *testcaller) {
	m_cases.push_back(testcaller);
}

int TestSuite::getChildTestCount() {
	return (int)m_cases.size();
}

TestCaller* TestSuite::getChildTestAt(int index) {
	return m_cases[index];
}

/** 获取本测试名称 */
string TestSuite::getName() const {
	return m_name;
}
/** 获取本测试描述 */
string TestSuite::getDescription() const {
	return m_desc;
}

/**
 * 运行一个测试集
 * @param results 结果统计对象
 */
void TestSuite::run() {
	File dbDir(CommonDbConfig::getBasedir());
	for (size_t i = 0; i < m_cases.size(); ++i) {
		// 清除上一次运行产生的数据
		dbDir.rmdir(true);
		dbDir.mkdir();
		// 运行当前用例
		m_cases[i]->run();
	}
}

TestSuite::~TestSuite() {
	for (size_t i = 0; i < m_cases.size(); ++i)
		delete m_cases[i];
}

void TestSuite::clear() {
	m_cases.clear();
}

}