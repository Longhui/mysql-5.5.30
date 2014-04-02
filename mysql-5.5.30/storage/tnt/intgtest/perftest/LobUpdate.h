/**
* 大对象的update
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
/** 每个update线程操作的次数 */
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
	//测试的主方法，供线程调用
	void test(u16 tid );

private: 
	void updateRecord(u16 tid);
	void updateOneTime();

private:

	bool m_useMms;		    /** 表是否使用Mms */
	bool m_inMemory;        /** 是否是内存操作 */
	u64 m_dataSize;         /** 测试数据量*/
	u32 m_lobSize;			/** 是否是小型大对象 */
	LobTestThread<LobUpdateTest> **m_threads;  /** 测试的线程 */
	byte *m_lob;                /** 要修改的大对象内容 */
	uint m_newLobLen;           /** 更新后LOB长度 */

};


#endif // _NTSETEST_MEMORY_TABLESCAN_H_
