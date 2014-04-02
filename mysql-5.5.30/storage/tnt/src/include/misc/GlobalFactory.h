#ifndef _NTSE_GLOBAL_FACTORY_H_
#define _NTSE_GLOBAL_FACTORY_H_

#include "util/System.h"
#include "util/Sync.h"
#include "misc/EventMonitor.h"

namespace ntse {

template <class T> 
void* GlobalObjectCreator() {
	return new T();
}

template <class T>
void GlobalObjectDestructor(void *obj) {
	delete (T*)obj;
}

typedef void* (*creator)();
typedef void (*destructor)(void *obj);
struct GlobalObject {
	const char* m_objectName;
	creator m_creator;
	destructor m_destructor;
	void *m_object;
};

/**
 * ���ฺ�𴴽����в�����Ϊ��̬����ʹ�õĶ���,�Լ�����������Ҫ����ȫ�ֶ������,��Щ��������޲�������ģ��������ɵ����ж�����Ψһ��
 * ����System��EventMonitorHelper����,���ǵĹ��캯�������˴����̵߳Ĳ���,Ϊ�˱�֤�߳��ź��������ȷ��,����ʹ�þ�̬����
 * ����ĳ��������Ҫһ��ȫ�ֶ���,�������������ɺ���������û��������ϵ
 * GlobalObject���ᰴ������˳��һһ�������ж����ж���������ϵ�ģ��޸�Global.cpp���������ʼ������ʱ����Ҫע��˳������
 * ���̰߳�ȫ��
 *
 * ʹ�÷��������ֻ�ǳ�ʼ������Ψһ����ֻ��Ҫ����getInstance()�Լ�freeInstance()�ӿڼ���
 * �����Ҫ�õ�����Ķ��󣬿���ͨ��getObject������ͨ�����������õ�����ָ��
 */
class GlobalFactory {
public:
	static GlobalFactory* getInstance() {
		if (m_instance == NULL)
			m_instance = new GlobalFactory();
		return m_instance;
	}

	static void freeInstance() {
		if (m_instance != NULL) {
			delete m_instance;
			m_instance = NULL;
		}
	}

	void *getObject(const char *name);

private:
	GlobalFactory();
	~GlobalFactory();

private:
	static GlobalFactory	*m_instance;		/** Ψһʵ�� */
	static GlobalObject		m_globalObjects[];	/** ����ȫ�ֱ������� */
};

}

#endif