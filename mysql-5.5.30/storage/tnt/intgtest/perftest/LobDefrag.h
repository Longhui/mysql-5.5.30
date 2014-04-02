/**
* 大对象的defrag
*
* @author zx(zx@163.org)
*/


#ifndef _NTSETEST_LOB_DEFRAG_H_
#define _NTSETEST_LOB_DEFRAG_H_

#include "PerfTest.h"
#include "util/Thread.h"
#include "misc/Session.h"
#include "EmptyTestCase.h"

#define BIG_LOB_DEFRAG_BIG_LEN 64 * 1024
#define BIG_LOB_DEFRAG_RANDOM_LOW  (-16) *1024
#define BIG_LOB_DEFRAG_RANDOM_HIGH 16 * 1024

#define DEFRAG_DEL_THREAD_COUNT 5
#define DEFRAG_UPDATE_THREAD_COUNT 2
#define DEFRAG_READ_THREAD_COUNT 20
#define DEFRAG_INSERT_THREAD_COUNT 20

using namespace ntse;
using namespace ntseperf;

class LobDefragTest: public EmptyTestCase {
public:
	LobDefragTest(bool useMms, u64 dataSize, u32 lobSize);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();

private: 
	void defragOneTime();

private:

	bool m_useMms;		        /** 表是否使用Mms */
	u64 m_dataSize;             /** 操作的数据量*/
	u32 m_lobSize;              /** 大对象的长度*/
};



#endif 
