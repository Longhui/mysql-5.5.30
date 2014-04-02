/**
 * ˫������
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_DLIST_H_
#define _NTSE_DLIST_H_

#include <assert.h>

namespace ntse {

/** ˫�������еĽڵ� */
template<typename E> class DList;
template<typename E>
class DLink {
public:
	/**
	 * ���캯������ʼ��Ϊ����������������ɽڵ�
	 */
	DLink() {
		m_prev = m_next = NULL;
		m_list = NULL;
	}

	/**
	 * ���캯������ʼ��Ϊ����������������ɽڵ�
	 *
	 * @param v �ڵ�����
	 */
	DLink(E v) {
		m_prev = m_next = NULL;
		m_list = NULL;
		m_v = v;
	} 

	/**
	 * �õ��ڵ��Ӧ��ֵ
	 *
	 * @return �ڵ��Ӧ��ֵ
	 */
	inline E get() {
		return m_v;
	}

	/**
	 * ���ýڵ��ֵ
	 *
	 * @param v �ڵ��ֵ
	 */
	inline void set(const E &v) {
		m_v = v;
	}

	/**
	 * �õ���һ���ڵ�
	 *
	 * @return ��һ���ڵ�
	 */
	inline DLink<E>* getNext() {
		return m_next;
	}

	/**
	 * �õ�ǰһ���ڵ�
	 *
	 * @return ǰһ���ڵ�
	 */
	inline DLink<E>* getPrev() {
		return m_prev;
	}

	/**
	 * �õ��ڵ�����������
	 *
	 * @return �ڵ�����������
	 */
	inline DList<E>* getList() {
		return m_list;
	}

	/**
	 * ����һ���ڵ㵽��ǰ�ڵ�֮ǰ
	 *
	 * @param e Ҫ����Ľڵ�
	 */
	void addBefore(DLink<E> *e) {
		assert(m_list);
		assert(!e->m_list);
		add(m_prev, e, this);
	}

	/**
	 * ����һ���ڵ㵽��ǰ�ڵ�֮��
	 *
	 * @param e Ҫ����Ľڵ�
	 */
	void addAfter(DLink<E> *e) {
		assert(m_list);
		assert(!e->m_list);
		add(this, e, m_next);
	}

	/**
	 * ���ڵ�������������жϿ�
	 */
	void unLink() {
		if (!m_list)
			return;
		m_prev->m_next = m_next;
		m_next->m_prev = m_prev;
		m_prev = m_next = NULL;
		m_list->m_size--;
		m_list = NULL;
	}

private:
	void add(DLink<E> *prev, DLink<E> *e, DLink<E> *next) {
		prev->m_next = e;
		e->m_prev = prev;
		next->m_prev = e;
		e->m_next = next;
		e->m_list = prev->m_list;
		e->m_list->m_size++;
	}

private:
	DList<E>	*m_list;
	DLink<E>	*m_prev;
	DLink<E>	*m_next;
	E		m_v;

friend class DList<E>;
};


/** ˫������ */
template<typename E>
class DList {
public:
	/**
	 * ����һ���յ�˫������
	 */
	DList() {
		m_header.m_next = m_header.m_prev = &m_header;
		m_header.m_list = this;
		m_size = 0;
	}

	/**
	 * �õ�����ͷ�ڵ�
	 *
	 * @return ����ͷ�ڵ�
	 */
	DLink<E>* getHeader() {
		return &m_header;
	}

	/**
	 * ����һ���ڵ㵽����β
	 *
	 * @param e Ҫ����Ľڵ�
	 */
	void addLast(DLink<E> *e) {
		m_header.addBefore(e);
	}

	/**
	 * ����һ���ڵ㵽����ͷ
	 *
	 * @param e Ҫ����Ľڵ�
	 */
	void addFirst(DLink<E> *e) {
		m_header.addAfter(e);
	}

	/**
	 * �������е�һ���ڵ��Ƶ�����ͷ
	 *
	 * @param e Ҫ�ƶ��Ľڵ�
	 */
	void moveToFirst(DLink<E> *e) {
		assert(e->m_list == this && e != &m_header);
		e->unLink();
		addFirst(e);
	}

	/**
	 * �������е�һ���ڵ��Ƶ�����β
	 *
	 * @param e Ҫ�ƶ��Ľڵ�
	 */
	void moveToLast(DLink<E> *e) {
		assert(e->m_list == this && e != &m_header);
		e->unLink();
		addLast(e);
	}

	/**
	 * �Ƴ������е����һ���ڵ�
	 *
	 * @return �����е����һ���ڵ㣬��Ϊ�������򷵻�NULL
	 */
	DLink<E>* removeLast() {
		return remove(m_header.m_prev);
	}

	/**
	 * �Ƴ������еĵ�һ���ڵ�
	 *
	 * @return �����еĵ�һ���ڵ㣬��Ϊ�������򷵻�NULL
	 */
	DLink<E>* removeFirst() {
		return remove(m_header.m_next);
	}

	/**
	 * ���������Ƿ�Ϊ������
	 *
	 * @return �����Ƿ�Ϊ������
	 */
	inline bool isEmpty() {
		return m_header.m_next == &m_header;
	}

	/**
	 * �õ������С�����ȣ�
	 *
	 * @return �����С
	 */
	inline uint getSize() const {
		return m_size;
	}

private:
	DLink<E>* remove(DLink<E> *e) {
		if (e == &m_header)
			return NULL;
		e->unLink();
		return e;
	}

private:
	DLink<E>	m_header;
	uint		m_size;

friend class DLink<E>;
};

}
#endif
