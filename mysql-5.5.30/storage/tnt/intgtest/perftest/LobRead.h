/**
* ������read
*
* @author zx(zx@163.org)
*/


#ifndef _NTSETEST_LOB_READ_H_
#define _NTSETEST_LOB_READ_H_

#include "PerfTest.h"
#include "util/Thread.h"
#include "misc/Session.h"
#include "EmptyTestCase.h"

using namespace ntse;
using namespace ntseperf;

class LobReadTest: public EmptyTestCase {
public:
	LobReadTest(bool useMms, u64 dataSize, u32 lobSize, bool inMemory,bool isNewData);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();

	uint scanOneTime();

private:

	bool m_useMms;		        /** ���Ƿ�ʹ��Mms */
	u64 m_dataSize;             /** ������������*/
	u32 m_lobSize;				/** �����Ĵ�С */
	bool m_inMemory;            /** �Ƿ��ڴ���� */
	bool m_isNewData;           /** �����������ݻ��Ǿ�����*/
};



#endif // _NTSETEST_MEMORY_TABLESCAN_H_
