/**
 * 功能测试框架
 *
 * @author 苏斌(bsu@corp.netease.com, naturally@163.org)
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
 * 功能测试用例
 *	一个用例对应一个TestCase子类型
 */
class TestCase {
public:
	virtual ~TestCase() {}
	/** 初始化 */
	virtual void setUp() {}
	/** 销毁 */
	virtual void tearDown() {}

	/**
	 * 装载数据, 用例在此生成测试数据
	 * @param totalRecSize [out] 测试数据量
	 * @post 保持数据库为关闭状态
	 */
	virtual void loadData(u64 *totalRecSize, u64 *recCnt) {
		*totalRecSize = 0;
		*recCnt = 0;
	}
	/**
	 * 预热，用例在此open数据库，进行预热
	 * @post 数据库状态为open
	 */
	virtual void warmUp() {}
	/** 运行用例 */
	virtual void run() = 0;
	/** 测试用例名 */
	virtual string getName() const = 0;
	/** 测试用例描述信息 */
	virtual string getDescription()const = 0;
	/** 验证当前数据库信息正确性 */
	virtual bool verify() = 0;
	/** 打印测试的统计信息 */
	virtual void status() = 0;
	/** 测试用例需要多线程操作的操作主体 */
	virtual void mtOperation(void *param) = 0;

	/** 用例耗时,返回-1表示利用框架计时 */
	virtual u64 getMillis() { return (u64)-1;}


	/** 是否开启测试数据缓存, true:缓存, false不缓存 */
	virtual bool cacheEnabled() { return false; }
	/** 缓存数据名，默认为用例名; 多个用例可以共享测试数据 */
	virtual string getCachedName() { return getName();}
};


class TestOperationThread : public Thread {
public:
	TestOperationThread(TestCase *testcase, u64 threadId) 
		: Thread("FuncTest"), m_testcase(testcase), m_threadId(threadId) {}
	virtual void run() { m_testcase->mtOperation(&m_threadId); }

protected:
	TestCase	*m_testcase;	/** 线程归属的测试用例 */
	u64			m_threadId;		/** 线程号 */
};


/** 测试运行对象 */
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


/** 重复一个用例多次 */
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


/** 测试集，包含多个测试用例 */
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