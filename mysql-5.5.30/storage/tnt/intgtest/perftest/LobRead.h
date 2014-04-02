/**
* 大对象的read
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

	bool m_useMms;		        /** 表是否使用Mms */
	u64 m_dataSize;             /** 操作的数据量*/
	u32 m_lobSize;				/** 大对象的大小 */
	bool m_inMemory;            /** 是否内存操作 */
	bool m_isNewData;           /** 测试是新数据还是旧数据*/
};



#endif // _NTSETEST_MEMORY_TABLESCAN_H_
