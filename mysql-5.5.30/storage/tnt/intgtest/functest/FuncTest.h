/**
 * ���ܲ��Կ��
 *
 * @author �ձ�(bsu@corp.netease.com, naturally@163.org)
 */

#ifndef _NTSETEST_FUNC_TEST_H_
#define _NTSETEST_FUNC_TEST_H_

#include "api/Database.h"
using namespace ntse;

#include <string>
#include <vector>
#include <list>
#include <fstream>
#include "util/Thread.h"
using namespace std;

namespace ntsefunc {

/**
 * ���ܲ�������
 *	һ��������Ӧһ��TestCase������
 */
class TestCase {
public:
	virtual ~TestCase() {}
	/** ��ʼ�� */
	virtual void setUp() {}
	/** ���� */
	virtual void tearDown() {}

	/**
	 * װ������, �����ڴ����ɲ�������
	 * @param totalRecSize [out] ����������
	 * @post �������ݿ�Ϊ�ر�״̬
	 */
	virtual void loadData(u64 *totalRecSize, u64 *recCnt) {
		*totalRecSize = 0;
		*recCnt = 0;
	}
	/**
	 * Ԥ�ȣ������ڴ�open���ݿ⣬����Ԥ��
	 * @post ���ݿ�״̬Ϊopen
	 */
	virtual void warmUp() {}
	/** �������� */
	virtual void run() = 0;
	/** ���������� */
	virtual string getName() const = 0;
	/** ��������������Ϣ */
	virtual string getDescription()const = 0;
	/** ��֤��ǰ���ݿ���Ϣ��ȷ�� */
	virtual bool verify() = 0;
	/** ��ӡ���Ե�ͳ����Ϣ */
	virtual void status() = 0;
	/** ����������Ҫ���̲߳����Ĳ������� */
	virtual void mtOperation(void *param) = 0;

	/** ������ʱ,����-1��ʾ���ÿ�ܼ�ʱ */
	virtual u64 getMillis() { return (u64)-1;}


	/** �Ƿ����������ݻ���, true:����, false������ */
	virtual bool cacheEnabled() { return false; }
	/** ������������Ĭ��Ϊ������; ����������Թ���������� */
	virtual string getCachedName() { return getName();}
};


class TestOperationThread : public Thread {
public:
	TestOperationThread(TestCase *testcase, u64 threadId) 
		: Thread("FuncTest"), m_testcase(testcase), m_threadId(threadId) {}
	virtual void run() { m_testcase->mtOperation(&m_threadId); }

protected:
	TestCase	*m_testcase;	/** �̹߳����Ĳ������� */
	u64			m_threadId;		/** �̺߳� */
};


/** �������ж��� */
class TestCaller {
public:
	TestCaller(TestCase *testcase);
	virtual ~TestCaller();
	virtual int getChildTestCount();
	virtual TestCaller* getChildTestAt(int index);
	virtual string getName() const;
	virtual string getDescription() const;
	virtual void run();
	virtual void result(bool successful) { cout << (successful ? "successful" : "fail") << endl; }
protected:
	TestCase *m_testcase;
};


/** �ظ�һ��������� */
class RepeatedTestCaller: public TestCaller {
public:
	RepeatedTestCaller(TestCase *testcase, int runCnt) 
		: TestCaller(0) {
			m_runCnt = runCnt;
			m_testcase = testcase;
			m_caller = new TestCaller(m_testcase);
	}

	~RepeatedTestCaller() {
		delete m_caller;
	}

	virtual void run() {
		while (m_runCnt-- > 0)
			m_caller->run();
	}

	virtual string getName() const;

private:
	TestCaller *m_caller;
	int m_runCnt;
};


/** ���Լ������������������ */
class TestSuite: public TestCaller {
public:
	TestSuite(const string &name = "", const string &desc = "")
		: TestCaller(0), m_name(name), m_desc(desc) {
	}
	~TestSuite();
	void clear();
	void addTestCase(TestCase *testcase);
	void addTest(TestCaller *testcaller);
	virtual int getChildTestCount();
	virtual TestCaller* getChildTestAt(int index);
	virtual void run();
	virtual string getName() const;
	virtual string getDescription() const;
private:
	string m_name;
	string m_desc;
	vector<TestCaller *> m_cases;
};

}

extern Syslog *logger;

#endif