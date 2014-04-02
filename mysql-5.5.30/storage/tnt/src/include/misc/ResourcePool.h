/**
* ��Դ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _NTSE_RESOURCE_POOL_H_
#define  _NTSE_RESOURCE_POOL_H_

#include "misc/Global.h"
#include "util/DList.h"
#include "util/Sync.h"

namespace ntse {

class Pool;
/**
 * ��Դ�û�
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
	const char *m_user; /** �û��� */
	Pool       *m_pool; /** ������Դ�� */
};

/**
* ��Դ
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
	DLink<Resource *> m_listEntry; /** ��Դ��˫��������� */
	ResourceUser      *m_user;     /** ��ǰʹ�ø���Դ�û� */
};

/**
 * ��Դ��
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
	Mutex        m_lock;              /** ��֤���ݰ�ȫ�� */
	Event        m_availaleEvent;     /** ��Դ�����¼� */
	uint         m_resourceNum;       /** ��Դ��Ŀ */
	uint         m_currentUsedNum;    /** ��ǰ��ʹ�õ���Դ��Ŀ */
	uint         m_maxUserNum;        /** ����û��� */
	uint         m_userNum;           /** ��ǰ�û��� */
	bool         m_needRegister;      /** �Ƿ���Ҫע���û����ܻ����Դ */
	DList<Resource *> m_resourceList; /** ��Դʵ������ */
};

}

#endif