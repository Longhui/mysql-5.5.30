/**
 * 一个简单的对象池
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_OBJECTPOOL_H_
#define _NTSE_OBJECTPOOL_H_

#include "Array.h"

namespace ntse {
/** 用于将对象池中的空闲元素链接成双向链表 */
struct OpeDoubleLink {
	int		m_next;		/** 前一空闲元素位置，没有则为-1 */
	int		m_prev;		/** 后一空闲元素位置，没有则为-1 */
};

/** 对象池中的元素 */
template<typename T>
struct ObjectPoolElem {
	bool			m_free;	/** 是否为空闲元素 */
	OpeDoubleLink	m_link;	/** 当是空闲元素时存储前后空闲元素 */
	T				m_obj;	/** 当不是空闲元素时存储对象的值 */

	ObjectPoolElem() {}
};

/**
 * 一个简单的对象池，用于小型对象的分配。
 * 本类设计的目的是防止直接调用malloc/free分配内存带来的碎片问题。
 * 
 * @param T 对象类型，必须实现不带参数的构造函数
 */
template<typename T>
class ObjectPool {
public:
	ObjectPool(bool needPop = false) {
		m_firstFree = -1;
		m_needPop = needPop;
	}

	ObjectPool(PagePoolUser *poolUser, PageType pageType, bool needPop = false) : m_data(poolUser, pageType) {
		m_firstFree = -1;
		m_needPop = needPop;
	}

	/**
	 * 分配一个对象
	 *
	 * @return 新分配的对象ID，调用operator[]得到对象的地址
	 */
	size_t alloc(bool forceAllocForPool = true) {
		if (m_firstFree >= 0) {
			ObjectPoolElem<T> *e = &m_data[m_firstFree];
			e->m_free = false;
			size_t ret = m_firstFree;
			m_firstFree = e->m_link.m_next;
			if (m_firstFree != -1) {
				m_data[m_firstFree].m_link.m_prev = -1;
			}
			return ret;
		} else {
			if(!m_data.expand(forceAllocForPool))
				return size_t(-1);	
			ObjectPoolElem<T> *e = new(&(m_data.last()))ObjectPoolElem<T>;
			e->m_free = false;
			return m_data.getSize() - 1;
		}
	}

	/**
	 * 得到指定对象ID对应的值
	 *
	 * @param i 对象ID
	 * @return 对象的值
	 */
	T& operator[](size_t id) {
		assert(!m_data[id].m_free);
		return m_data[id].m_obj;
	}

	/**
	 * 释放一个对象
	 *
	 * @param 对象ID
	 */
	void free(size_t id) {
		ObjectPoolElem<T> *e = &m_data[id];
		assert(!e->m_free);
		//如果free的是最后一个元素，可以将它pop掉
		if (m_needPop && id == m_data.getSize() - 1) {
			m_data.pop();
			if (m_data.getSize() == 0) {
				return;
			}
			//从尾部开始检查free元素，pop free元素一直到尾部非free元素
			ObjectPoolElem<T> lastElem = m_data.last();
			while (lastElem.m_free) {
				if (-1 == lastElem.m_link.m_prev && -1 != lastElem.m_link.m_next) {
					//开始项为free
					m_firstFree = lastElem.m_link.m_next;
					m_data[m_firstFree].m_link.m_prev = -1;
				} else if (-1 != lastElem.m_link.m_prev && -1 != lastElem.m_link.m_next) {
					//中间项为free
					m_data[lastElem.m_link.m_next].m_link.m_prev = lastElem.m_link.m_prev;
					m_data[lastElem.m_link.m_prev].m_link.m_next = lastElem.m_link.m_next;
				} else if (-1 != lastElem.m_link.m_prev && -1 == lastElem.m_link.m_next) {
					//最后项为free
					m_data[lastElem.m_link.m_prev].m_link.m_next = -1;
				} else {
					//只有一项free的情况下
					assert(-1 == lastElem.m_link.m_prev && -1 == lastElem.m_link.m_next);
					m_firstFree = -1;
				}
				m_data.pop();

				if (m_data.getSize() > 0) {
					lastElem = m_data.last();
				} else {
					break;
				}
			}
			return;
		}
		e->m_free = true;
		if (m_firstFree < 0) {
			e->m_link.m_next = -1;
			e->m_link.m_prev = -1;
		} else {
			ObjectPoolElem<T> *e2 = &m_data[m_firstFree];
			e->m_link.m_next = m_firstFree;
			e->m_link.m_prev = -1;
			e2->m_link.m_prev = (int)id;
		}
		m_firstFree = (int)id;
	}

	/** 清空所有数据 */
	void clear() {
		m_data.clear();
		m_firstFree = -1;
	}
#ifndef NTSE_UNIT_TEST
private:
#endif
	Array<ObjectPoolElem<T> >	m_data;	/** 存储元素的动态数组 */
	int			m_firstFree;			/** 第一个空闲元素位置，没有则为-1 */
	bool        m_needPop;
};

} // namespace ntse
#endif
