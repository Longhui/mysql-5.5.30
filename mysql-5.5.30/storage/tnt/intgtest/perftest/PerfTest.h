/**
 * 性能测试框架
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */
#ifndef _NTSETEST_PERF_TESTCASE_H_
#define _NTSETEST_PERF_TESTCASE_H_

#include "api/Database.h"
#include "misc/Buffer.h"
#include "misc/Txnlog.h"
#include "mms/Mms.h"
using namespace ntse;

#include <string>
#include <vector>
#include <list>
#include <fstream>
using namespace std;

namespace ntseperf {


/** 测试结果 */
class PTResult {
public:
	/**
	 * 构造一个测试结果
	 * @param casename 测试用例名
	 */
	PTResult(const string &casename)
		: m_name(casename) {
	}
	string toString() const;
	static string getHeader();

	string m_name;	/** 测试用例名	*/
	u64 m_opCnt;	/** 操作次数	*/
	u64 m_dataSize;	/** 数据量大小	*/
	u64	m_time;		/** 耗时,ms		*/
};
/** 结果统计 */
class ResultListener {
public:
	virtual void addResult(const PTResult *result) = 0;
};


/** 监听用例执行结果，并输入到文件中 */
class FileResultListener: public ResultListener {
public:
	FileResultListener(const string& filename, bool printToStdout = true);
	~FileResultListener();
	virtual void addResult(const PTResult *result);
private:
	bool m_printToStdout;
	ofstream m_os;
};

struct TestCaseStatus {
	BufferStatus	m_bufferStatus;
	LogStatus		m_logStatus;
	MmsStatus		m_mmsStatus;
};

/**
 * 性能测试用例
 *	一个用例对应一个TestCase子类型
 */
class TestCase {
public:
	virtual ~TestCase() {}
	/** 初始化 */
	virtual void setUp() {}
	/** 销毁 */
	virtual void tearDown() {}
	/** 返回所操作的数据库对象
	 * @return 所操作的数据库对象，可能为NULL
	 */
	virtual Database* getDatabase() {
		return NULL;
	}

	/**
	 * 装载数据, 用例在此生成测试数据
	 * @param totalRecSize [out] 测试数据量
	 * @param recCnt [out] 测试数据记录数
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
	/** 运行用例，测试框架对本函数计时 */
	virtual void run() = 0;
	/** 测试用例名 */
	virtual string getName() const = 0;
	/** 测试用例描述信息 */
	virtual string getDescription() const = 0;

	//////////////////////////////////////////////////////////////////////////
	//// 测试结果
	//////////////////////////////////////////////////////////////////////////
	
	/** 返回用例操作的总数据量 */
	virtual u64 getDataSize() { return 0;}
	/** 返回用例的操作次数 */
	virtual u64 getOpCnt() { return 0;}
	/** 用例耗时,返回-1表示利用框架计时 */
	virtual u64 getMillis() { return (u64)-1;}

	//////////////////////////////////////////////////////////////////////////
	//// 测试数据缓存
	//////////////////////////////////////////////////////////////////////////
	/** 是否开启测试数据缓存, true:缓存, false不缓存 */
	virtual bool cacheEnabled() { return false; }
	/** 缓存数据名，默认为用例名; 多个用例可以共享测试数据 */
	virtual string getCachedName() const { return getName();}
	/**
	 * 设置已加载数据信息
	 *	从测试数据缓存加载数据后，测试框架调用本函数设置已加载数据信息
	 * @param totalRecSize 总数据量
	 * @param recCnt 记录数
	 */
	virtual void setLoadedDataInfo(u64 totalRecSize, u64 recCnt) {
		UNREFERENCED_PARAMETER(totalRecSize);
		UNREFERENCED_PARAMETER(recCnt);
	}
	//////////////////////////////////////////////////////////////////////////
	//// 测试用例统计信息
	//////////////////////////////////////////////////////////////////////////
	/** 是否内存测试用例(内存用例不能有IO) */
	virtual bool isMemoryCase() { return false; }
	/**
	 * 获取统计信息
	 * @return 统计信息，返回0表示没有统计信息
	 */
	virtual const TestCaseStatus* getStatus() const { return 0; }
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
	virtual void run(ResultListener *results);
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

	virtual void run(ResultListener *results) {
		while (m_runCnt-- > 0)
			m_caller->run(results);
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
	virtual void run(ResultListener *results);
	virtual string getName() const;
	virtual string getDescription() const;
private:
	string m_name;
	string m_desc;
	vector<TestCaller *> m_cases;
};

} // namespace ntseperf

extern Syslog *logger;

#endif //_NTSETEST_PERF_TESTCASE_H_
