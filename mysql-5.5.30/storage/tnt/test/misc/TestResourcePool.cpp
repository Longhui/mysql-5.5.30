/**
* ������Դ��
*
* @author ��ΰ��(liweizhao@corp.netease.com)
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

	//��ʼ����Դ��
	m_resourcePool = new Pool(-1, false);
	const uint resourceNum = 17;
	for (uint i = 0; i < resourceNum; i++) {
		Resource *r = new Resource();
		m_resourcePool->add(r);
		CPPUNIT_ASSERT(m_resourcePool->getSize() == (i + 1));
		CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == 0);
	}
	CPPUNIT_ASSERT(NULL == m_resourcePool->registerUser("Failed"));

	//��һ���߳̽���Դ�ص���Դȡ��
	PoolConsumer consumer(m_resourcePool, resourceNum, false);
	consumer.enableSyncPoint(SP_RESOURCE_POOL_AFTER_FATCH_DONE);
	consumer.start();
	Thread::msleep(500);

	//���̴߳���Դ��ȡ��Դʧ��
	uint fatchFailedTimes = 10;
	for (uint i = 0; i < fatchFailedTimes; i++) {
		Resource *r = m_resourcePool->getInst(1);
		CPPUNIT_ASSERT(!r);
		CPPUNIT_ASSERT(m_resourcePool->getSize() == 0);
		CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == resourceNum);
	}

	//��һ���߳̽���Դ������Դ��
	consumer.notifySyncPoint(SP_RESOURCE_POOL_AFTER_FATCH_DONE);
	Thread::msleep(500);

	//���߳̿���ȡ��Դ�ص���һ��Դ
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

	//��ʼ����Դ��
	const uint maxUserNum = 4;
	m_resourcePool = new Pool(maxUserNum, true);

	//ע���û�
	ResourceUser **userArr = new ResourceUser*[maxUserNum];
	string userName("PoolUser");
	for (uint i = 0; i < maxUserNum; i++) {
		userName += "+";
		userArr[i] = m_resourcePool->registerUser(userName.c_str());
		CPPUNIT_ASSERT((i + 1) == m_resourcePool->getRegisterUserNum());
	}

	//���û�ע��ʧ��
	uint registerFailedTimes = 5;
	for (uint i = 0; i < registerFailedTimes; i++) {
		CPPUNIT_ASSERT(NULL == m_resourcePool->registerUser("FailedUser"));
		CPPUNIT_ASSERT(maxUserNum == m_resourcePool->getRegisterUserNum());
	}

	//��ʼ����Դ����Դ
 	const uint resourceNum = 17;
 	for (uint i = 0; i < resourceNum; i++) {
 		Resource *r = new Resource();
 		m_resourcePool->add(r);
 		CPPUNIT_ASSERT(m_resourcePool->getSize() == (i + 1));
 		CPPUNIT_ASSERT(m_resourcePool->getCurrentUsedNum() == 0);
 	}
	CPPUNIT_ASSERT(m_resourcePool->getSize() == resourceNum);

	//����Դ�ػ�ȡ��Դ����֤�û�
	for (uint i = 0; i < resourceNum; i++) {
		ResourceUser *user = userArr[i % maxUserNum];
		Resource *r = m_resourcePool->getInst(1, user);
		CPPUNIT_ASSERT(r->getUser() == user);
		m_resourcePool->reclaimInst(r);
	}

	//ע�������û�
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