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
 * 该类负责创建所有不能作为静态变量使用的对象,以及其他所有需要生成全局对象的类,这些类必须是无参数构造的，该类生成的所有对象都是唯一的
 * 例如System和EventMonitorHelper对象,它们的构造函数包含了创建线程的操作,为了保证线程信号掩码的正确性,不能使用静态变量
 * 或者某个对象需要一个全局对象,而这个对象的生成和其他对象没有依赖关系
 * GlobalObject将会按照数组顺序一一生成所有对象，有对象依赖关系的，修改Global.cpp当中数组初始化内容时，需要注意顺序问题
 * 非线程安全类
 *
 * 使用方法：如果只是初始化所有唯一对象，只需要调用getInstance()以及freeInstance()接口即可
 * 如果需要得到具体的对象，可以通过getObject方法，通过对象类名得到对象指针
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
	static GlobalFactory	*m_instance;		/** 唯一实例 */
	static GlobalObject		m_globalObjects[];	/** 所有全局变量数组 */
};

}

#endif