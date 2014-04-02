/**
* С�ʹ�����insert
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


/** �������ݲ���������߳� */
template <typename T> 
class LobTestThread: public Thread {
private: 
	u16 m_id;			       /** �̺߳ţ�����100�߳�������� 1 - 100 */
	T *m_tester;               /** Ҫ���Ե��� */   
public: 
	LobTestThread(const char *name, u16 threadId, T *tester): Thread(name) {
		m_id = threadId;
		m_tester = tester;
	}

	/**
	* �߳���������
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
	//���Ե������������̵߳���
	void test(u16 tid );

private: 
	void insertRecord(u16 tid);
	void InsertOneTime();
	u64 getRecCount();

private:
    
	bool m_useMms;		        /** ���Ƿ�ʹ��Mms */
	u64 m_dataSize;				/** �������ݴ�С */
	u32 m_lobSize;				/** ÿ������󳤶� */
	Record **m_records;
	LobTestThread<LobInsertTest> **m_threads;  /** ���Ե��߳� */
};



#endif // _NTSETEST_MEMORY_TABLESCAN_H_
