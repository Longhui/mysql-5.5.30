/**
* 小型大对象的insert
*
* @author zx(zx@163.org)
*/


#ifndef _NTSETEST_MEMORY_SMALLLOBINSERT_H_
#define _NTSETEST_MEMORY_SMALLLOBINSERT_H_

#include "PerfTest.h"
#include "util/Thread.h"
#include "misc/Session.h"
#include "EmptyTestCase.h"

#define SMALL_LOB_INSERT_DATASIZE_SMALL_RATIO 2 
#define SMALL_LOB_INSERT_DATASIZE_BIG_RATIO 5
#define BIG_LOB_INSERT_DATASIZE_BIG_RATIO 10
#define SMALL_LOB_INSERT_SMALL_LEN 4
#define SMALL_LOB_INSERT_BIG_LEN 1024
#define BIG_LOB_INSERT_BIG_LEN 64 * 1024
#define THREAD_COUNT 10


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
	LobInsertTest(bool useMms, bool config, bool isSmall);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
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
	bool m_isSmall;             /** m_dataSize是否是小型配置*/
	bool m_isSmallLob;           /** 是否是小型大对象 */
	Record **m_records;
	LobTestThread<LobInsertTest> **m_threads;  /** 测试的线程 */
};



#endif // _NTSETEST_MEMORY_TABLESCAN_H_
