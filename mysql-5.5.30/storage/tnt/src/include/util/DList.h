/**
 * 双向链表
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_DLIST_H_
#define _NTSE_DLIST_H_

#include <assert.h>

namespace ntse {

/** 双向链表中的节点 */
template<typename E> class DList;
template<typename E>
class DLink {
public:
	/**
	 * 构造函数。初始化为不从属于链表的自由节点
	 */
	DLink() {
		m_prev = m_next = NULL;
		m_list = NULL;
	}

	/**
	 * 构造函数。初始化为不从属于链表的自由节点
	 *
	 * @param v 节点内容
	 */
	DLink(E v) {
		m_prev = m_next = NULL;
		m_list = NULL;
		m_v = v;
	} 

	/**
	 * 得到节点对应的值
	 *
	 * @return 节点对应的值
	 */
	inline E get() {
		return m_v;
	}

	/**
	 * 设置节点的值
	 *
	 * @param v 节点的值
	 */
	inline void set(const E &v) {
		m_v = v;
	}

	/**
	 * 得到下一个节点
	 *
	 * @return 下一个节点
	 */
	inline DLink<E>* getNext() {
		return m_next;
	}

	/**
	 * 得到前一个节点
	 *
	 * @return 前一个节点
	 */
	inline DLink<E>* getPrev() {
		return m_prev;
	}

	/**
	 * 得到节点所属的链表
	 *
	 * @return 节点所属的链表
	 */
	inline DList<E>* getList() {
		return m_list;
	}

	/**
	 * 插入一个节点到当前节点之前
	 *
	 * @param e 要插入的节点
	 */
	void addBefore(DLink<E> *e) {
		assert(m_list);
		assert(!e->m_list);
		add(m_prev, e, this);
	}

	/**
	 * 插入一个节点到当前节点之后
	 *
	 * @param e 要插入的节点
	 */
	void addAfter(DLink<E> *e) {
		assert(m_list);
		assert(!e->m_list);
		add(this, e, m_next);
	}

	/**
	 * 将节点从其所属链表中断开
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


/** 双向链表 */
template<typename E>
class DList {
public:
	/**
	 * 构造一个空的双向链表
	 */
	DList() {
		m_header.m_next = m_header.m_prev = &m_header;
		m_header.m_list = this;
		m_size = 0;
	}

	/**
	 * 得到链表头节点
	 *
	 * @return 链表头节点
	 */
	DLink<E>* getHeader() {
		return &m_header;
	}

	/**
	 * 插入一个节点到链表尾
	 *
	 * @param e 要插入的节点
	 */
	void addLast(DLink<E> *e) {
		m_header.addBefore(e);
	}

	/**
	 * 插入一个节点到链表头
	 *
	 * @param e 要插入的节点
	 */
	void addFirst(DLink<E> *e) {
		m_header.addAfter(e);
	}

	/**
	 * 将链表中的一个节点移到链表头
	 *
	 * @param e 要移动的节点
	 */
	void moveToFirst(DLink<E> *e) {
		assert(e->m_list == this && e != &m_header);
		e->unLink();
		addFirst(e);
	}

	/**
	 * 将链表中的一个节点移到链表尾
	 *
	 * @param e 要移动的节点
	 */
	void moveToLast(DLink<E> *e) {
		assert(e->m_list == this && e != &m_header);
		e->unLink();
		addLast(e);
	}

	/**
	 * 移除链表中的最后一个节点
	 *
	 * @return 链表中的最后一个节点，若为空链表则返回NULL
	 */
	DLink<E>* removeLast() {
		return remove(m_header.m_prev);
	}

	/**
	 * 移除链表中的第一个节点
	 *
	 * @return 链表中的第一个节点，若为空链表则返回NULL
	 */
	DLink<E>* removeFirst() {
		return remove(m_header.m_next);
	}

	/**
	 * 返回链表是否为空链表
	 *
	 * @return 链表是否为空链表
	 */
	inline bool isEmpty() {
		return m_header.m_next == &m_header;
	}

	/**
	 * 得到链表大小（长度）
	 *
	 * @return 链表大小
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
