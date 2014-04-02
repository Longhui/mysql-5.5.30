/**
* 测试资源池
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "misc/TestResourcePool.h"

const char* ResourcePoolTestCase::getName() {
	return "ResourcePool test";
}

const char* ResourcePoolTestCase::getDescription() {
	return "Test basic operations of resource pool.";
}

bool ResourcePoolTestCase::isBig() {
	return false;
}

void ResourcePoolTestCase::setUp() {
	m_resourcePool = NULL;
}

void ResourcePoolTestCase::tearDown() {
	if (m_resourcePool) {
		delete m_resourcePool;
		m_resourcePool = NULL;
	}
}

void ResourcePoolTestCase::testBasic() {
	assert(!m_resourcePool);

	//初始化资源池
	m_resourcePool = new Pool(-1, false);
	const uint resourceNum = 17;
	for (uint i = 0; i < resourceNum; i++) {
		Resource *r = new Resource();
		m_resourcePool->add(r);
		CPPUNIT_ASSERT(m_resourcePool->getSize() == (i + 1));
		CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == 0);
	}
	CPPUNIT_ASSERT(NULL == m_resourcePool->registerUser("Failed"));

	//另一个线程将资源池的资源取光
	PoolConsumer consumer(m_resourcePool, resourceNum, false);
	consumer.enableSyncPoint(SP_RESOURCE_POOL_AFTER_FATCH_DONE);
	consumer.start();
	Thread::msleep(500);

	//本线程从资源池取资源失败
	uint fatchFailedTimes = 10;
	for (uint i = 0; i < fatchFailedTimes; i++) {
		Resource *r = m_resourcePool->getInst(1);
		CPPUNIT_ASSERT(!r);
		CPPUNIT_ASSERT(m_resourcePool->getSize() == 0);
		CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == resourceNum);
	}

	//另一个线程将资源还给资源池
	consumer.notifySyncPoint(SP_RESOURCE_POOL_AFTER_FATCH_DONE);
	Thread::msleep(500);

	//本线程可以取资源池的任一资源
	Resource **resourceInPool = new Resource *[resourceNum];
	for (uint i = 0; i < resourceNum; i++) {
		resourceInPool[i] = m_resourcePool->getInst(1);
		CPPUNIT_ASSERT(resourceInPool[i]);
		CPPUNIT_ASSERT(m_resourcePool->getSize() == (resourceNum - i - 1));
		CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == (i + 1));
	}
	for (uint i = 0; i < resourceNum; i++) {
		m_resourcePool->reclaimInst(resourceInPool[i]);
	}
	CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == 0);
	CPPUNIT_ASSERT(m_resourcePool->getSize() == resourceNum);
	delete []resourceInPool;
	resourceInPool = NULL;
	
	consumer.disableSyncPoint(SP_RESOURCE_POOL_AFTER_FATCH_DONE);

	delete m_resourcePool;
	m_resourcePool = NULL;
}

void ResourcePoolTestCase::testRegisterUser() {
	assert(!m_resourcePool);

	//初始化资源池
	const uint maxUserNum = 4;
	m_resourcePool = new Pool(maxUserNum, true);

	//注册用户
	ResourceUser **userArr = new ResourceUser*[maxUserNum];
	string userName("PoolUser");
	for (uint i = 0; i < maxUserNum; i++) {
		userName += "+";
		userArr[i] = m_resourcePool->registerUser(userName.c_str());
		CPPUNIT_ASSERT((i + 1) == m_resourcePool->getRegisterUserNum());
	}

	//新用户注册失败
	uint registerFailedTimes = 5;
	for (uint i = 0; i < registerFailedTimes; i++) {
		CPPUNIT_ASSERT(NULL == m_resourcePool->registerUser("FailedUser"));
		CPPUNIT_ASSERT(maxUserNum == m_resourcePool->getRegisterUserNum());
	}

	//初始化资源池资源
 	const uint resourceNum = 17;
 	for (uint i = 0; i < resourceNum; i++) {
 		Resource *r = new Resource();
 		m_resourcePool->add(r);
 		CPPUNIT_ASSERT(m_resourcePool->getSize() == (i + 1));
 		CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == 0);
 	}
	CPPUNIT_ASSERT(m_resourcePool->getSize() == resourceNum);

	//从资源池获取资源，验证用户
	for (uint i = 0; i < resourceNum; i++) {
		ResourceUser *user = userArr[i % maxUserNum];
		Resource *r = m_resourcePool->getInst(1, user);
		CPPUNIT_ASSERT(r->getUser() == user);
		m_resourcePool->reclaimInst(r);
	}

	//注销所有用户
	for (uint i = 0; i < maxUserNum; i++) {
		m_resourcePool->unRegisterUser(&userArr[i]);
		CPPUNIT_ASSERT(NULL == userArr[i]);
	}
	CPPUNIT_ASSERT(0 == m_resourcePool->getRegisterUserNum());
	delete []userArr;
	userArr = NULL;

	delete m_resourcePool;
	m_resourcePool = NULL;
}