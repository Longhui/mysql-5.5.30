#include "PerfTest.h"
#include "DbConfigs.h"
#include "util/File.h"
#include "util/System.h"
#include "misc/Syslog.h"
#include <sstream>
#include <list>


using namespace std;


namespace ntseperf {

	/** 获取文件名 */
string basename(const string& path) {
	size_t slashPos = path.rfind(NTSE_PATH_SEP_CHAR);
	return (slashPos == string::npos) ? path : path.substr(slashPos + 1);
}
/**
 * 拷贝目录
 * @param destPath 目的目录
 * @param srcPath 源目录
 * @param overrideExist 是否覆盖已有文件
 * @throws NtseException
 * @return 拷贝的文件个数
 */
int copyDir(const char *destPath, const char *srcPath, bool overrideExist) throw(NtseException) {
	File dest(destPath);
	dest.mkdir();

	File src(srcPath);
	bool isDir;
	src.isDirectory(&isDir);
	assert(isDir);

	int numCopied = 0;

	u64 errNo = 0;
	list<string> filenames;
	if ((errNo = src.listFiles(&filenames, true)) != File::E_NO_ERROR)
		NTSE_THROW(errNo, "listFile failed");

	list<string>::iterator iter = filenames.begin();
	for (; iter != filenames.end(); ++iter) {
		string filename = basename(*iter);
		string curSrcPath = string(srcPath) + NTSE_PATH_SEP + filename;
		string curDestPath = string(destPath) + NTSE_PATH_SEP + filename;
		File curFile(curSrcPath.c_str());
		bool isDir;
		curFile.isDirectory(&isDir);
		if (isDir) {
			File(curDestPath.c_str()).mkdir();
			numCopied += copyDir(curDestPath.c_str(), curSrcPath.c_str(), overrideExist);
		} else {
			errNo = File::copyFile(curDestPath.c_str(), curSrcPath.c_str(), overrideExist);
			if (File::E_NO_ERROR != errNo)
				NTSE_THROW(errNo, "copy file %s to %s failed", curSrcPath.c_str(),curDestPath.c_str());
			++numCopied;
		}
	}
	return numCopied;
}

//////////////////////////////////////////////////////////////////////////
////TestDataCache
//////////////////////////////////////////////////////////////////////////
/** 测试数据信息 */
struct TestDataInfo {
	TestDataInfo() {

	}
	TestDataInfo(u64 dataSize, u64 recCnt) {
		m_dataSize = dataSize;
		m_recCnt = recCnt;
	}

	void load(istream &is) {
		is >> m_dataSize >> m_recCnt;
	}

	void store(ostream &os) const {
		os << m_dataSize << endl << m_recCnt << endl;
	}
	u64 m_dataSize;
	u64 m_recCnt;
};
/** 测试数据缓存 */
class TestDataCache {
public:
	bool load(const string& dataname, TestDataInfo *info) {
		string path = m_cachePath + NTSE_PATH_SEP + dataname;
		File file(path.c_str());
		bool exists;
		file.isExist(&exists);
		if (!exists)
			return false;
		int numCopied = 0;
		try {
			string cachePath = m_cachePath + NTSE_PATH_SEP + dataname;
			numCopied = copyDir(CommonDbConfig::getBasedir(), cachePath.c_str(), true);
			ifstream of((cachePath + NTSE_PATH_SEP + "datainfo.txt").c_str());
			if (!of.is_open())
				return false;
			info->load(of);
			of.close();
		} catch(NtseException &e) {
			cerr << "Exception in TestDataCache::load" << e.getMessage() << endl;
		}
		return numCopied >= 2;
	}

	void store(const string& dataname, const TestDataInfo& info) {
		try {
			string cachePath = m_cachePath + NTSE_PATH_SEP + dataname;
			copyDir(cachePath.c_str(), CommonDbConfig::getBasedir(), true);
			ofstream of((cachePath + NTSE_PATH_SEP + "datainfo.txt").c_str());
			info.store(of);
			of.close();
		} catch(NtseException &e) {
			cerr << "Exception in TestDataCache::store" << e.getMessage() << endl;
		}
	}

	static TestDataCache* instance() {
		return &m_inst;
	}
private:
	TestDataCache(const string& path)
		: m_cachePath(path) {
		File(path.c_str()).mkdir();
	}

private:
	string m_cachePath;
	static TestDataCache m_inst;
};
TestDataCache TestDataCache::m_inst = TestDataCache("datacache");


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
void TestCaller::run(ResultListener *results) {
	logger->log(EL_DEBUG, "Begin case %s", m_testcase->getName().c_str());
	// 初始化
	logger->log(EL_DEBUG, "setUp");
	m_testcase->setUp();
	// 装载数据
	u64 dataSize, recCnt;
	if (m_testcase->cacheEnabled()) {
		TestDataInfo info;
		if (!TestDataCache::instance()->load(m_testcase->getCachedName(), &info)) { // 测试数据不在缓存中
			logger->log(EL_DEBUG, "loadData");
			m_testcase->loadData(&dataSize, &recCnt);
			logger->log(EL_DEBUG, "dataSize "I64FORMAT"u recCnt "I64FORMAT"u", dataSize, recCnt);
			logger->log(EL_DEBUG, "Save data to cache %s", m_testcase->getCachedName().c_str());
			TestDataCache::instance()->store(m_testcase->getCachedName(), TestDataInfo(dataSize, recCnt));
		} else {
			logger->log(EL_DEBUG, "Load data from cache %s", m_testcase->getCachedName().c_str());
			m_testcase->setLoadedDataInfo(info.m_dataSize, info.m_recCnt);
			logger->log(EL_DEBUG, "dataSize "I64FORMAT"u recCnt "I64FORMAT"u", info.m_dataSize, info.m_recCnt);
		}
	} else {
		logger->log(EL_DEBUG, "loadData");
		m_testcase->loadData(&dataSize, &recCnt);
		logger->log(EL_DEBUG, "dataSize "I64FORMAT"u recCnt "I64FORMAT"u", dataSize, recCnt);
	}
	
	// 预热
	logger->log(EL_DEBUG, "warmUp");
	m_testcase->warmUp();
	if (m_testcase->getDatabase()) {
		cout << "== Status After Warmup" << endl; 
		m_testcase->getDatabase()->printStatus(cout);
		cout << endl;
	}
	cout << "press any key to continue" << endl;
	char c;
	cin >> c;
	
	// 运行用例
	TestCaseStatus beforeStatus;
	const TestCaseStatus *status = m_testcase->getStatus();
	if (status) beforeStatus = *status;
	logger->log(EL_DEBUG, "run");
	u64 before = System::currentTimeMillis();
	m_testcase->run();
	u64 after = System::currentTimeMillis();
	const TestCaseStatus *afterStatus = m_testcase->getStatus();
	if (m_testcase->isMemoryCase()) { // 内存用例：检查IO是否为0
		if (!afterStatus) {
			logger->log(EL_WARN, "Memory case %s doesn't provide case status", m_testcase->getName().c_str());
		} else {
			u64 phyReads = afterStatus->m_bufferStatus.m_physicalReads - beforeStatus.m_bufferStatus.m_physicalReads;
			u64 phyWrites = afterStatus->m_bufferStatus.m_physicalWrites - beforeStatus.m_bufferStatus.m_physicalWrites;
			u64 logFlushes = afterStatus->m_logStatus.m_flushCnt - beforeStatus.m_logStatus.m_flushCnt;
			if (phyReads || phyWrites || logFlushes) {
				logger->log(EL_ERROR, "%s declared to be memory case, but buffer physicalReads "
					I64FORMAT"u, buffer physicalWrites "I64FORMAT"u, log flushs "I64FORMAT"u",
					m_testcase->getName().c_str(), phyReads, phyWrites, logFlushes);
				NTSE_ASSERT(false);
			}
		}
	}
	if (m_testcase->getDatabase()) {
		cout << "== Status After Run" << endl; 
		m_testcase->getDatabase()->printStatus(cout);
		cout << endl;
	}

	// 保存结果
	PTResult res(m_testcase->getName());
	res.m_opCnt = m_testcase->getOpCnt();
	res.m_dataSize = m_testcase->getDataSize();
	u64 userTime = m_testcase->getMillis();
	res.m_time = (userTime == (u64)-1 ? after - before : userTime);
	results->addResult(&res);
	logger->log(EL_DEBUG, "tearDown");
	// 清理
	m_testcase->tearDown();
}
//////////////////////////////////////////////////////////////////////////
//// RepeatedTestCaller
//////////////////////////////////////////////////////////////////////////
string RepeatedTestCaller::getName() const {
	stringstream ss;
	ss << "[RPT:" << m_runCnt << "]"<< m_caller->getName();
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
void TestSuite::run(ResultListener *results) {
	File dbDir(CommonDbConfig::getBasedir());
	for (size_t i = 0; i < m_cases.size(); ++i) {
		// 清除上一次运行产生的数据
		dbDir.rmdir(true);
		dbDir.mkdir();
		// 运行当前用例
		m_cases[i]->run(results);
	}
}

TestSuite::~TestSuite() {
	for (size_t i = 0; i < m_cases.size(); ++i)
		delete m_cases[i];
}

void TestSuite::clear() {
	m_cases.clear();
}
//////////////////////////////////////////////////////////////////////////
////FileResultLister
//////////////////////////////////////////////////////////////////////////
FileResultListener::FileResultListener(const string& filename,bool printToStdout)
	: m_os(filename.c_str()), m_printToStdout(printToStdout) {
	assert(m_os.is_open());
	m_os << PTResult::getHeader() << endl;
	m_os.flush();
	if (m_printToStdout)
		cout << PTResult::getHeader() << endl;
}

FileResultListener::~FileResultListener() {
	m_os.close();
}

void FileResultListener::addResult(const PTResult *result) {
	m_os << result->toString() << endl;
	m_os.flush();
	if (m_printToStdout)
		cout << result->toString() << endl;
}

//////////////////////////////////////////////////////////////////////////
/////测试结果
//////////////////////////////////////////////////////////////////////////
/** 测试结果转化为字符串 */
string PTResult::toString() const {
	stringstream ss;
	ss	<< m_name << '\t'
		<< m_time << '\t'
		<< m_dataSize << '\t'
		<< (double)m_dataSize / 1024 / 1024 * 1000 / m_time << '\t'
		<< m_opCnt << '\t'
		<< (double)m_opCnt * 1000 / m_time;
	return ss.str();
}

string PTResult::getHeader() {
	return "Name\tTime(ms)\tSize(B)\tThroughput(MB/s)\tops\top/s";
}

}

