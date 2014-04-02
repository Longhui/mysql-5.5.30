/**
* С�ʹ�����insert
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
	LobInsertTest(bool useMms, bool config, bool isSmall);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual u64 getOpCnt();
	virtual u64 getDataSize();
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
	bool m_isSmall;             /** m_dataSize�Ƿ���С������*/
	bool m_isSmallLob;           /** �Ƿ���С�ʹ���� */
	Record **m_records;
	LobTestThread<LobInsertTest> **m_threads;  /** ���Ե��߳� */
};



#endif // _NTSETEST_MEMORY_TABLESCAN_H_
