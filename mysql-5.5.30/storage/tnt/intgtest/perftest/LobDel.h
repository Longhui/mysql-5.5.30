/**
* ������del
*
* @author zx(zx@163.org)
*/


#ifndef _NTSETEST_LOB_DEL_H_
#define _NTSETEST_LOB_DEL_H_

#include "PerfTest.h"
#include "util/Thread.h"
#include "misc/Session.h"
#include "LobInsert.h"
#include "EmptyTestCase.h"

#define DEL_THREAD_COUNT 100


using namespace ntse;
using namespace ntseperf;

class LobDelTest: public EmptyTestCase {
public:
	LobDelTest(bool useMms, u64 dataSize, u32 lobSize, bool inMemory);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();
	//���Ե������������̵߳���
	void test(u16 tid );

private: 
	void delRecord(u16 tid);
	void delOneTime();

private:

	bool m_useMms;		/** ���Ƿ�ʹ��Mms */
	u64 m_dataSize;		/** ������������ */
	u32 m_lobSize;		/** �����Ĵ�С*/
	bool m_inMemory;	/** �Ƿ��ڴ���� */
	
	LobTestThread<LobDelTest> **m_threads;  /** ���Ե��߳� */
};



#endif // _NTSETEST_MEMORY_TABLESCAN_H_
