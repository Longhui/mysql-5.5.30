/**
* 小型大对象的insert
*
* @author zx(zx@163.org)
*/


#ifndef _NTSETEST_MEMORY_LOBINSERT_H_
#define _NTSETEST_MEMORY_LOBINSERT_H_

#include "PerfTest.h"
#include "util/Thread.h"
#include "misc/Session.h"
#include "EmptyTestCase.h"

#define THREAD_COUNT 100


using namespace ntse;
using namespace ntseperf;


/** 进行数据插入的任务线程 */
template <typename T> 
class LobTestThread: public Thread {
private: 
	u16 m_id;			       /** 线程号，比如100线程这里就是 1 - 100 */
	T *m_tester;               /** 要测试的类 */   
public: 
	LobTestThread(const char *name, u16 threadId, T *tester): Thread(name) {
		m_id = threadId;
		m_tester = tester;
	}

	/**
	* 线程运行主体
	*/
	void run() {
		m_tester->test(m_id);
	}

};

class LobInsertTest: public EmptyTestCase {
public:
	LobInsertTest(bool useMms, u64 dataSize, u32 lobSize, bool inMemory);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();
	//测试的主方法，供线程调用
	void test(u16 tid );

private: 
	void insertRecord(u16 tid);
	void InsertOneTime();
	u64 getRecCount();

private:
    
	bool m_useMms;		        /** 表是否使用Mms */
	u64 m_dataSize;				/** 测试数据大小 */
	u32 m_lobSize;				/** 每个大对象长度 */
	Record **m_records;
	LobTestThread<LobInsertTest> **m_threads;  /** 测试的线程 */
};



#endif // _NTSETEST_MEMORY_TABLESCAN_H_
