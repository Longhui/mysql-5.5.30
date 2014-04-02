/**
* ������update
*
* @author zx(zx@163.org)
*/

#ifndef _NTSETEST_LOB_UPDATE_H_
#define _NTSETEST_LOB_UPDATE_H_

#include "PerfTest.h"
#include "util/Thread.h"
#include "misc/Session.h"
#include "LobInsert.h"
#include "EmptyTestCase.h"

using namespace ntse;
using namespace ntseperf;

#define UPDATE_THREAD_COUNT 100
/** ÿ��update�̲߳����Ĵ��� */
#define UPDATE_TIMES 1000

class LobUpdateTest: public EmptyTestCase {
public:
	LobUpdateTest(bool useMms, u64 dataSize, u32 lobSize, u32 upLobSize, bool inMemory = false);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();
	//���Ե������������̵߳���
	void test(u16 tid );

private: 
	void updateRecord(u16 tid);
	void updateOneTime();

private:

	bool m_useMms;		    /** ���Ƿ�ʹ��Mms */
	bool m_inMemory;        /** �Ƿ����ڴ���� */
	u64 m_dataSize;         /** ����������*/
	u32 m_lobSize;			/** �Ƿ���С�ʹ���� */
	LobTestThread<LobUpdateTest> **m_threads;  /** ���Ե��߳� */
	byte *m_lob;                /** Ҫ�޸ĵĴ�������� */
	uint m_newLobLen;           /** ���º�LOB���� */

};


#endif // _NTSETEST_MEMORY_TABLESCAN_H_
