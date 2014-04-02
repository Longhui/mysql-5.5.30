/**
 * 数组
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_ARRAY_H_
#define _NTSE_ARRAY_H_

#include <malloc.h>
#include "misc/Global.h"
#include "util/PagePool.h"

namespace ntse {

/** 
 * 动态数组。
 * 数组使用的内存可以使用标准的malloc/free分配，也可以从内存页池中分配。
 * 如果指定使用内存页池，则数组使用的大部分内存从内存页池中分配，但用于维护各内存页池指针的
 * 内存是用malloc分配的。
 * FIXME: 目前E不能是引用类型
 */
template<typename E>
class Array {
public:
	/**
	 * 创建一个不使用内存页池的数组
	 */
	Array() {
		init(NULL, PAGE_EMPTY);
	}

	/**
	 * 创建一个使用内存页池的数组
	 *
	 * @param poolUser 内存页池用户
	 * @param pageType 内存页池类型
	 */
	Array(PagePoolUser *poolUser, PageType pageType) {
		init(poolUser, pageType);
	}

	/**
	 * 析构函数，释放所有内存
	 */
	~Array() {
		clear();
		while (m_usedPages > 0)
			popPage();
		free(m_pages);
	}

	/**
	 * 得到指定位置的元素
	 * @pre i不能超过数组大小，否则会assert
	 *
	 * @param i 元素索引，从0开始
	 * @return 元素引用
	 */
	E& operator[](size_t i) const {
		assert(i < m_size);
		size_t pageNo, slot;
		getPageAndSlot(i, &pageNo, &slot);
		void *page = m_pages[pageNo];
		return *(((E *)page) + slot);
	}

	/**
	 * 加入一个元素到数组末尾
	 *
	 * @param e 要加入的元素
	 * @param forceAllocForPool 使用内存页池分配空间时是否用allocPageForce接口
	 * @return 是否成功
	 */
	bool push(const E& e, bool forceAllocForPool = false) {
		if (!expand(forceAllocForPool))
			return false;
		size_t pageNo, slot;
		getPageAndSlot(m_size - 1, &pageNo, &slot);
		void *page = m_pages[pageNo];
		E *ep = ((E *)page) + slot;
		new (ep)E;
		*ep = e;
		return true;
	}

	/**
	 * 增加数组的大小
	 * 
	 * @param forceAllocForPool 使用内存页池分配空间时是否用allocPageForce接口
	 * @return 是否成功
	 */
	bool expand(bool forceAllocForPool = false) {
		m_size++;
		m_tailSize++;
		if (m_size > m_capacity) {
			assert(m_size > m_reservedSize);
			if (!expandCapacity(m_size, forceAllocForPool)) {
				m_size--;
				m_tailSize--;
				return false;
			}
		}
		if (m_tailSize > m_elementsPerPage) {
			assert(m_tailSize == m_elementsPerPage + 1);
			m_tailSize = 1;
		}
		return true;
	}

	/**
	 * 获得数组预留空间大小
	 *
	 * @return 数组预留空间大小
	 */
	size_t getReservedSize() const {
		return m_reservedSize;
	}

	/**
	 * 设定预留空间
	 *
	 * @param size 预留空间
	 * @param forceAllocForPool 使用内存页池分配空间时是否用allocPageForce接口
	 * @return 是否成功
	 */
	bool setReservedSize(size_t size, bool forceAllocForPool = false) {
		if (size < m_reservedSize)
			shrinkCapacity(size >= m_size ? size: m_size);
		else if (size > m_capacity) {
			if (!expandCapacity(size, forceAllocForPool))
				return false;
		}
		m_reservedSize = size;
		return true;
	}

	/**
	 * 删除数组尾部的元素
	 */
	void pop() {
		assert(m_size > 0);
		assert(m_tailSize > 0);
		last().~E();
		m_size--;
		m_tailSize--;
		if (m_tailSize == 0) {	// 当前页已经空了，考虑将它释放
			m_tailSize = m_elementsPerPage;
			if (m_size > m_reservedSize) {
				popPage();
			}
		}
	}

	/**
	 * 得到数组最后一个元素
	 * @pre 数组非空
	 *
	 * @return 数组最后一个元素
	 */
	E& last() const {
		assert(m_size > 0);
		return (*this)[m_size - 1];
	}

	/**
	 * 得到数组大小
	 *
	 * @return 数组大小
	 */
	size_t getSize() const {
		return m_size;
	}

	/**
	 * 得到数组已经分配的空间大小
	 * 
	 * @return 数组已经分配的空间大小，单位为元素个数
	 */
	size_t getCapacity() const {
		return m_capacity;
	}

	/**
	 * 返回数组是否为空
	 *
	 * @return 数组是否为空
	 */
	bool isEmpty() const {
		return m_size == 0;
	}

	/**
	 * 清空数组中所有数据
	 */
	void clear() {
		while (m_size > 0)
			pop();
		m_size = 0;
		m_tailSize = 0;
		shrinkCapacity(m_reservedSize);
	}

	/** 返回数组占用的内存大小
	 * @return 占用内存大小
	 */
	size_t getMemUsage() const {
		return m_usedPages * getPageSize();
	}

private:
	size_t getPageSize() const {
		if (m_poolUser != NULL)
			return m_poolUser->getPool()->getPageSize();
		else
			return PAGE_SIZE;
	}

	/** 分配一个新页面
	 *
	 * @param forceAllocForPool 使用内存页池分配空间时是否用allocPageForce接口
	 * @return 成功返回新页面，返回NULL表示分配页面失败
	 */
	void* appendPage(bool forceAllocForPool) {
		void *p;
		if (m_poolUser != NULL) {
			if (forceAllocForPool)
				p = m_poolUser->allocPageForce(0, m_pageType, NULL);
			else
				p = m_poolUser->allocPage(0, m_pageType, NULL);
		} else
			p = malloc(PAGE_SIZE);
		if (p) {
			if (m_pagesSize == m_usedPages) {
				m_pagesSize *= 2;
				m_pages = (void **)realloc(m_pages, sizeof(void *) * m_pagesSize);
			}
			m_pages[m_usedPages] = p;
			m_usedPages++;
			m_capacity += m_elementsPerPage;
		}
		return p;
	}

	void popPage() {
		assert(m_usedPages > 0);
		void *lastPage = m_pages[m_usedPages - 1];
		if (m_poolUser != NULL)
			m_poolUser->freePage(0, lastPage);
		else
			free(lastPage);
		m_usedPages--;
		m_pages[m_usedPages] = NULL;
		m_capacity -= m_elementsPerPage;
		if (m_usedPages < m_pagesSize / 2) {
			m_pagesSize /= 2;
			m_pages = (void **)realloc(m_pages, sizeof(void *) * m_pagesSize);
		}
	}

	void init(PagePoolUser *poolUser, PageType pageType) {
		m_poolUser = poolUser;
		m_pageType = pageType;
		m_size = 0;
		m_elementsPerPage = getPageSize() / sizeof(E);
		m_capacity = 0;
		m_reservedSize = 0;
		m_tailSize = 0;
		m_pages = (void **)malloc(sizeof(void *));
		m_pagesSize = 1;
		m_usedPages = 0;
		
		size_t n = 2, level = 1;
		while (n < m_elementsPerPage) {
			n = n * 2;
			level++;
		}
		if (n == m_elementsPerPage)
			m_eppLevel = level;
		else
			m_eppLevel = 0;
	}

	inline void getPageAndSlot(size_t index, size_t *pageNo, size_t *slot) const {
		if (m_eppLevel > 0) {
			*pageNo = index >> m_eppLevel;
			*slot = index & (m_elementsPerPage - 1);
		} else {
			*pageNo = index / m_elementsPerPage;
			*slot = index % m_elementsPerPage;
		}
	}

	/**
	 * 调整数组的容量
	 *
	 * @param size 要保证数组能够存储这么多的元素
	 * @param forceAllocForPool 使用内存页池分配空间时是否用allocPageForce接口
	 * @return 成功与否
	 */
	bool expandCapacity(size_t size, bool forceAllocForPool) {
		assert(size > m_capacity);
		size_t targetPages = (size - 1) / m_elementsPerPage + 1;
		size_t oldPages = m_usedPages;
		for (size_t i = oldPages; i < targetPages; i++) {
			void *p = appendPage(forceAllocForPool);
			if (!p) {
				while (m_usedPages > oldPages)
					popPage();
				return false;
			}
		}
		return true;
	}

	void shrinkCapacity(size_t size) {
		size_t keepPages;
		if (size == 0)
			keepPages = 0;
		else
			keepPages = (size - 1) / m_elementsPerPage + 1;
		while (m_usedPages > keepPages) 
			popPage();
	}

public:
	static const size_t	PAGE_SIZE = 1024;	/** 不使用内存页池时的页大小 */
private:
	PagePoolUser	*m_poolUser;	/** 内存页池用户 */
	PageType	m_pageType;			/** 我所用的内存页类型 */
	void	**m_pages;				/** 内存页数组 */
	size_t	m_pagesSize;			/** m_pages数组大小 */
	size_t	m_usedPages;			/** 已经分配的页数 */
	size_t	m_size;					/** 数组大小 */
	size_t	m_capacity;				/** 已经分配的内存能够存放多少个元素 */
	size_t	m_tailSize;				/** 最后一页上已经用了多少个元素 */
	size_t	m_elementsPerPage;		/** 每页可存储多少个元素 */
	size_t	m_eppLevel;				/** 当每页存储的元素是2的整数次幂时，记录幂次，否则为0 */
	size_t	m_reservedSize;			/** 预留的空间大小 */
};

}
#endif
