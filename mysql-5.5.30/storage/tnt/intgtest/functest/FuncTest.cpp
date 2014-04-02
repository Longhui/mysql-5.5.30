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

/** �Ӳ��Ը��� */
int TestCaller::getChildTestCount() {
	return 0;
}

/** ��ȡ�Ӳ��� */
TestCaller* TestCaller::getChildTestAt(int index) {
	UNREFERENCED_PARAMETER(index);
	return 0;
}

/** ��ȡ���������� */
string TestCaller::getName() const {
	return m_testcase->getName();
}

/** ��ȡ���������� */
string TestCaller::getDescription() const {
	return m_testcase->getDescription();
}

/** ���������������� */
void TestCaller::run() {
	logger->log(EL_DEBUG, "Begin case %s", m_testcase->getName().c_str());
	// ��ʼ��
	logger->log(EL_DEBUG, "setUp");
	m_testcase->setUp();
	// װ������
	u64 totalRecSize, recCnt;
	logger->log(EL_DEBUG, "loadData");
	m_testcase->loadData(&totalRecSize, &recCnt);
	logger->log(EL_DEBUG, "dataSize "I64FORMAT"u recCnt "I64FORMAT"u", totalRecSize, recCnt);
	// Ԥ��
	logger->log(EL_DEBUG, "warmUp");
	m_testcase->warmUp();
	// ��ʼ����
	logger->log(EL_DEBUG, "run");
	m_testcase->run();
	// ��֤��ȷ��
	logger->log(EL_DEBUG, "verify");
	result(m_testcase->verify());
	// ������
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
/////���Լ�
//////////////////////////////////////////////////////////////////////////
/**
 * �����Լ�������һ������
 * @param testcase ������������
 */
void TestSuite::addTestCase(TestCase *testcase) {
	m_cases.push_back(new TestCaller(testcase));
}
/**
 * �����Լ�������һ���������ж���
 * @param testcaller �������ж���
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

/** ��ȡ���������� */
string TestSuite::getName() const {
	return m_name;
}
/** ��ȡ���������� */
string TestSuite::getDescription() const {
	return m_desc;
}

/**
 * ����һ�����Լ�
 * @param results ���ͳ�ƶ���
 */
void TestSuite::run() {
	File dbDir(CommonDbConfig::getBasedir());
	for (size_t i = 0; i < m_cases.size(); ++i) {
		// �����һ�����в���������
		dbDir.rmdir(true);
		dbDir.mkdir();
		// ���е�ǰ����
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