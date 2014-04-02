/**
* ≤‚ ‘◊ ‘¥≥ÿ
*
* @author ¿ÓŒ∞Ó»(liweizhao@corp.netease.com)
*/
#ifndef _NTSETEST_RESOURCE_POOL_H_
#define _NTSETEST_RESOURCE_POOL_H_

#include <cppunit/extensions/HelperMacros.h>
#include "misc/Global.h"
#include "misc/ResourcePool.h"
#include "util//Thread.h"

using namespace std;
using namespace ntse;

class PoolConsumer : public Thread {
public:
	PoolConsumer(Pool *testPool, uint getNum, bool needRegister) : Thread("ResourcePoolConsumer"), 
		m_testPool(testPool), m_getNum(getNum), m_needRegister(needRegister), m_user(NULL) {
			m_user = needRegister ? m_testPool->registerUser("PoolConsumer") : NULL;
	}
	virtual ~PoolConsumer() {
		if (m_user) {
			m_testPool->unRegisterUser(&m_user);
			NTSE_ASSERT(m_user);
		}
	}
	void run() {
		Resource **resArr = new Resource *[m_getNum];
		for (uint i  = 0; i < m_getNum; i++) {
			resArr[i] = m_testPool->getInst(1, m_user);
			CPPUNIT_ASSERT(resArr[i]);
		}
		SYNCHERE(SP_RESOURCE_POOL_AFTER_FATCH_DONE);
		for (uint i = 0; i < m_getNum; i++)
			m_testPool->reclaimInst(resArr[i]);
		delete []resArr;
		resArr = NULL;
	}
public:
	Pool *m_testPool;
	uint m_getNum;
	bool m_needRegister;
	ResourceUser *m_user;
};

class ResourcePoolTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(ResourcePoolTestCase);
	CPPUNIT_TEST(testBasic);
	CPPUNIT_TEST(testRegisterUser);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBasic();
	void testRegisterUser();

private:
	
	Pool *m_resourcePool;
};

#endif