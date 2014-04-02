/**
* 资源池相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/

#include "misc/ResourcePool.h"

namespace ntse {

/**
 * 资源池构造
 * @param maxUserNum   最大注册用户数
 * @param needResister 是否需要注册才能获得实例
 */
Pool::Pool(uint maxUserNum, bool needResister) : m_lock("ResourcePoolLock", __FILE__, __LINE__), 
m_resourceNum(0), m_currentUsedNum(0), m_maxUserNum(maxUserNum), m_userNum(0), m_needRegister(needResister) {
}

Pool::~Pool() {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	uint listSize = m_resourceList.getSize();
	if (listSize > 0) {
		for (uint i = 0; i < listSize; i++) {
			Resource *r = m_resourceList.removeLast()->get();
			NTSE_ASSERT(!r->getUser());
			delete r;
		}
	}
	NTSE_ASSERT(0 == m_userNum);
}

/**
 * 向资源池注册用户
 * @param userName
 * @return 
 */
ResourceUser* Pool::registerUser(const char *userName) {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	if (m_needRegister) {		
		if (m_userNum < m_maxUserNum) {
			m_userNum++;
			return new ResourceUser(userName, this);
		}
	}
	return NULL;
}

/**
 * 注销用户
 * @param user
 */
void Pool::unRegisterUser(ResourceUser **user) {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	assert(m_userNum <= m_maxUserNum);
	NTSE_ASSERT((*user)->m_pool == this);
	m_userNum--;
	delete *user;
	*user = NULL;
}

/**
 * 向资源池中添加资源
 * @param r
 */
void Pool::add(Resource *r) {
	bool lockByMe = false;
	if (!m_lock.isLocked()) {
		LOCK(&m_lock);
		lockByMe = true;
	}
	assert(!r->getUser());
	++m_resourceNum;
	m_resourceList.addLast(&r->m_listEntry);
	if (lockByMe)
		UNLOCK(&m_lock);
}

/**
 * 从资源池中获取资源实例
 * @param timeoutMs 等待资源可用超时时间, <=0表示不超时，>0表示超时毫秒数
 * @param user 获取实例的用户，如果不需要注册用户则为NULL
 * @return 
 */
Resource* Pool::getInst(int timeoutMs, ResourceUser *user) {
	assert(m_resourceNum > 0);
	bool timeout = false;
	do {
		LOCK(&m_lock);
		if (m_currentUsedNum >= m_resourceNum) {
			UNLOCK(&m_lock);
			timeout = !m_availaleEvent.wait(timeoutMs);
		} else {
			break;
		}
	} while (!timeout);
	if (!timeout) {
		assert(m_lock.isLocked());
		m_currentUsedNum++;
		Resource *r = m_resourceList.removeFirst()->get();
		if (m_needRegister) {
			NTSE_ASSERT(user);
			r->setUser(user);
		}
		UNLOCK(&m_lock);
		return r;
	}
	return NULL;
}

/**
 * 返回资源实例到资源池
 * @param r
 */
void Pool::reclaimInst(Resource *r) {
	LOCK(&m_lock);
	m_currentUsedNum--;
	if (m_needRegister)
		r->setUser(NULL);
	assert(NULL == r->getUser());
	m_resourceList.addLast(&r->m_listEntry);
	UNLOCK(&m_lock);
	m_availaleEvent.signal(false);
}

/**
 * 获得当前资源池的资源数目
 * @return
 */
uint Pool::getSize() {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	return m_resourceList.getSize();
}

/**
 * 获得当前被使用的资源数目
 * @return
 */
uint Pool::getCurrentUsedNum() {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	return m_currentUsedNum;
}

/**
 * 获得当前注册用户数
 * @return
 */
uint Pool::getRegisterUserNum() {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	return m_userNum;
}

}