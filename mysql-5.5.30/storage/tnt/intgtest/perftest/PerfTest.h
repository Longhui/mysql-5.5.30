/**
 * ���ܲ��Կ��
 *
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
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


/** ���Խ�� */
class PTResult {
public:
	/**
	 * ����һ�����Խ��
	 * @param casename ����������
	 */
	PTResult(const string &casename)
		: m_name(casename) {
	}
	string toString() const;
	static string getHeader();

	string m_name;	/** ����������	*/
	u64 m_opCnt;	/** ��������	*/
	u64 m_dataSize;	/** ��������С	*/
	u64	m_time;		/** ��ʱ,ms		*/
};
/** ���ͳ�� */
class ResultListener {
public:
	virtual void addResult(const PTResult *result) = 0;
};


/** ��������ִ�н���������뵽�ļ��� */
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
	/** ���������������ݿ����
	 * @return �����������ݿ���󣬿���ΪNULL
	 */
	virtual Database* getDatabase() {
		return NULL;
	}

	/**
	 * װ������, �����ڴ����ɲ�������
	 * @param totalRecSize [out] ����������
	 * @param recCnt [out] �������ݼ�¼��
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
	/** �������������Կ�ܶԱ�������ʱ */
	virtual void run() = 0;
	/** ���������� */
	virtual string getName() const = 0;
	/** ��������������Ϣ */
	virtual string getDescription() const = 0;

	//////////////////////////////////////////////////////////////////////////
	//// ���Խ��
	//////////////////////////////////////////////////////////////////////////
	
	/** ���������������������� */
	virtual u64 getDataSize() { return 0;}
	/** ���������Ĳ������� */
	virtual u64 getOpCnt() { return 0;}
	/** ������ʱ,����-1��ʾ���ÿ�ܼ�ʱ */
	virtual u64 getMillis() { return (u64)-1;}

	//////////////////////////////////////////////////////////////////////////
	//// �������ݻ���
	//////////////////////////////////////////////////////////////////////////
	/** �Ƿ����������ݻ���, true:����, false������ */
	virtual bool cacheEnabled() { return false; }
	/** ������������Ĭ��Ϊ������; ����������Թ���������� */
	virtual string getCachedName() const { return getName();}
	/**
	 * �����Ѽ���������Ϣ
	 *	�Ӳ������ݻ���������ݺ󣬲��Կ�ܵ��ñ����������Ѽ���������Ϣ
	 * @param totalRecSize ��������
	 * @param recCnt ��¼��
	 */
	virtual void setLoadedDataInfo(u64 totalRecSize, u64 recCnt) {
		UNREFERENCED_PARAMETER(totalRecSize);
		UNREFERENCED_PARAMETER(recCnt);
	}
	//////////////////////////////////////////////////////////////////////////
	//// ��������ͳ����Ϣ
	//////////////////////////////////////////////////////////////////////////
	/** �Ƿ��ڴ��������(�ڴ�����������IO) */
	virtual bool isMemoryCase() { return false; }
	/**
	 * ��ȡͳ����Ϣ
	 * @return ͳ����Ϣ������0��ʾû��ͳ����Ϣ
	 */
	virtual const TestCaseStatus* getStatus() const { return 0; }
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
	virtual void run(ResultListener *results);
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

	virtual void run(ResultListener *results) {
		while (m_runCnt-- > 0)
			m_caller->run(results);
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
