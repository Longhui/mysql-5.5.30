/**
 * һ���򵥵Ķ����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_OBJECTPOOL_H_
#define _NTSE_OBJECTPOOL_H_

#include "Array.h"

namespace ntse {
/** ���ڽ�������еĿ���Ԫ�����ӳ�˫������ */
struct OpeDoubleLink {
	int		m_next;		/** ǰһ����Ԫ��λ�ã�û����Ϊ-1 */
	int		m_prev;		/** ��һ����Ԫ��λ�ã�û����Ϊ-1 */
};

/** ������е�Ԫ�� */
template<typename T>
struct ObjectPoolElem {
	bool			m_free;	/** �Ƿ�Ϊ����Ԫ�� */
	OpeDoubleLink	m_link;	/** ���ǿ���Ԫ��ʱ�洢ǰ�����Ԫ�� */
	T				m_obj;	/** �����ǿ���Ԫ��ʱ�洢�����ֵ */

	ObjectPoolElem() {}
};

/**
 * һ���򵥵Ķ���أ�����С�Ͷ���ķ��䡣
 * ������Ƶ�Ŀ���Ƿ�ֱֹ�ӵ���malloc/free�����ڴ��������Ƭ���⡣
 * 
 * @param T �������ͣ�����ʵ�ֲ��������Ĺ��캯��
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
	 * ����һ������
	 *
	 * @return �·���Ķ���ID������operator[]�õ�����ĵ�ַ
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
	 * �õ�ָ������ID��Ӧ��ֵ
	 *
	 * @param i ����ID
	 * @return �����ֵ
	 */
	T& operator[](size_t id) {
		assert(!m_data[id].m_free);
		return m_data[id].m_obj;
	}

	/**
	 * �ͷ�һ������
	 *
	 * @param ����ID
	 */
	void free(size_t id) {
		ObjectPoolElem<T> *e = &m_data[id];
		assert(!e->m_free);
		//���free�������һ��Ԫ�أ����Խ���pop��
		if (m_needPop && id == m_data.getSize() - 1) {
			m_data.pop();
			if (m_data.getSize() == 0) {
				return;
			}
			//��β����ʼ���freeԪ�أ�pop freeԪ��һֱ��β����freeԪ��
			ObjectPoolElem<T> lastElem = m_data.last();
			while (lastElem.m_free) {
				if (-1 == lastElem.m_link.m_prev && -1 != lastElem.m_link.m_next) {
					//��ʼ��Ϊfree
					m_firstFree = lastElem.m_link.m_next;
					m_data[m_firstFree].m_link.m_prev = -1;
				} else if (-1 != lastElem.m_link.m_prev && -1 != lastElem.m_link.m_next) {
					//�м���Ϊfree
					m_data[lastElem.m_link.m_next].m_link.m_prev = lastElem.m_link.m_prev;
					m_data[lastElem.m_link.m_prev].m_link.m_next = lastElem.m_link.m_next;
				} else if (-1 != lastElem.m_link.m_prev && -1 == lastElem.m_link.m_next) {
					//�����Ϊfree
					m_data[lastElem.m_link.m_prev].m_link.m_next = -1;
				} else {
					//ֻ��һ��free�������
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

	/** ����������� */
	void clear() {
		m_data.clear();
		m_firstFree = -1;
	}
#ifndef NTSE_UNIT_TEST
private:
#endif
	Array<ObjectPoolElem<T> >	m_data;	/** �洢Ԫ�صĶ�̬���� */
	int			m_firstFree;			/** ��һ������Ԫ��λ�ã�û����Ϊ-1 */
	bool        m_needPop;
};

} // namespace ntse
#endif
