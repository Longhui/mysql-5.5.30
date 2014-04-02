/**
 * 测试页面缓存的替换策略效果
 *
 * @author 汪源(wangyuan@corp.netease.com wy@163.org)
 */

#ifndef _NTSETEST_BUFFER_REPLACE_POLICY_H_
#define _NTSETEST_BUFFER_REPLACE_POLICY_H_

#include "PerfTest.h"
#include "EmptyTestCase.h"

using namespace ntse;

namespace ntseperf {

class BufferReplacePolicyTest: public EmptyTestCase {
public:
	BufferReplacePolicyTest();
	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void run();

private:
	
};

}

#endif
