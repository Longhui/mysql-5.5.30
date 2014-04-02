/**
* ��Դ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/

#include "misc/ResourcePool.h"

namespace ntse {

/**
 * ��Դ�ع���
 * @param maxUserNum   ���ע���û���
 * @param needResister �Ƿ���Ҫע����ܻ��ʵ��
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
 * ����Դ��ע���û�
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
 * ע���û�
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
 * ����Դ���������Դ
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
 * ����Դ���л�ȡ��Դʵ��
 * @param timeoutMs �ȴ���Դ���ó�ʱʱ��, <=0��ʾ����ʱ��>0��ʾ��ʱ������
 * @param user ��ȡʵ�����û����������Ҫע���û���ΪNULL
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
 * ������Դʵ������Դ��
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
 * ��õ�ǰ��Դ�ص���Դ��Ŀ
 * @return
 */
uint Pool::getSize() {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	return m_resourceList.getSize();
}

/**
 * ��õ�ǰ��ʹ�õ���Դ��Ŀ
 * @return
 */
uint Pool::getCurrentUsedNum() {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	return m_currentUsedNum;
}

/**
 * ��õ�ǰע���û���
 * @return
 */
uint Pool::getRegisterUserNum() {
	MutexGuard mutexGuard(&m_lock, __FILE__, __LINE__);
	return m_userNum;
}

}