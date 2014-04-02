/**
* 资源池相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _NTSE_RESOURCE_POOL_H_
#define  _NTSE_RESOURCE_POOL_H_

#include "misc/Global.h"
#include "util/DList.h"
#include "util/Sync.h"

namespace ntse {

class Pool;
/**
 * 资源用户
 */
class ResourceUser {
	friend class Pool;
public:
	ResourceUser(const char *user, Pool *p) : m_pool(p) {
		m_user = System::strdup(user);
	}
	virtual ~ResourceUser() {
		if (m_user)
			delete []m_user;
	}
	const char *getUserName() {
		return m_user;
	}
protected:
	const char *m_user; /** 用户名 */
	Pool       *m_pool; /** 所属资源池 */
};

/**
* 资源
*/
class Resource {
	friend class Pool;
public:
	Resource() : m_user(NULL) {
		m_listEntry.set(this);
	}
	virtual ~Resource() {}
	inline void setUser(ResourceUser *user) {
		m_user = user;
	}
	inline ResourceUser *getUser() {
		return m_user;
	}
	inline DLink<Resource *>* getListEntry() {
		return &m_listEntry;
	}
protected:
	DLink<Resource *> m_listEntry; /** 资源池双向链表入口 */
	ResourceUser      *m_user;     /** 当前使用该资源用户 */
};

/**
 * 资源池
 */
class Pool {
public:
	Pool(uint maxUserNum = (uint)-1, bool needRegister = true);
	virtual ~Pool();

	ResourceUser *registerUser(const char *userName);
	void unRegisterUser(ResourceUser **user);
	void add(Resource *r);
	Resource *getInst(int timeoutMs = -1, ResourceUser *user = NULL);
	void reclaimInst(Resource *r);
	uint getSize();
	uint getCurrentUsedNum();
	uint getRegisterUserNum();

protected:
	Mutex        m_lock;              /** 保证数据安全锁 */
	Event        m_availaleEvent;     /** 资源可用事件 */
	uint         m_resourceNum;       /** 资源数目 */
	uint         m_currentUsedNum;    /** 当前被使用的资源数目 */
	uint         m_maxUserNum;        /** 最大用户数 */
	uint         m_userNum;           /** 当前用户数 */
	bool         m_needRegister;      /** 是否需要注册用户才能获得资源 */
	DList<Resource *> m_resourceList; /** 资源实例链表 */
};

}

#endif