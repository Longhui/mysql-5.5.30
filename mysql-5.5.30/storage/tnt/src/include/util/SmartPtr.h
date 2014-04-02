/**
 * 智能指针
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_SMARTPTR_H_
#define _NTSE_SMARTPTR_H_

/**
 * 析构函数中自动释放内存的智能指针
 *
 * @param T 对象类型
 */
template<typename T>
class AutoPtr {
public:
	AutoPtr(T *p, bool isArray = false) {
		m_ptr = p;
		m_isArray = isArray;
	}

	~AutoPtr() {
		if (m_isArray)
			delete []m_ptr;
		else
			delete m_ptr;
	}

	T* operator->() {
		return m_ptr;
	}

	operator T*() {
		return m_ptr;
	}

	T* detatch() {
		T *p = m_ptr;
		m_ptr = NULL;
		return p;
	}
private:
	T	*m_ptr;		/** 指针 */
	bool	m_isArray;	/** 是否是数组 */
};

#endif

