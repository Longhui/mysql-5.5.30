/**
 * ����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_ARRAY_H_
#define _NTSE_ARRAY_H_

#include <malloc.h>
#include "misc/Global.h"
#include "util/PagePool.h"

namespace ntse {

/** 
 * ��̬���顣
 * ����ʹ�õ��ڴ����ʹ�ñ�׼��malloc/free���䣬Ҳ���Դ��ڴ�ҳ���з��䡣
 * ���ָ��ʹ���ڴ�ҳ�أ�������ʹ�õĴ󲿷��ڴ���ڴ�ҳ���з��䣬������ά�����ڴ�ҳ��ָ���
 * �ڴ�����malloc����ġ�
 * FIXME: ĿǰE��������������
 */
template<typename E>
class Array {
public:
	/**
	 * ����һ����ʹ���ڴ�ҳ�ص�����
	 */
	Array() {
		init(NULL, PAGE_EMPTY);
	}

	/**
	 * ����һ��ʹ���ڴ�ҳ�ص�����
	 *
	 * @param poolUser �ڴ�ҳ���û�
	 * @param pageType �ڴ�ҳ������
	 */
	Array(PagePoolUser *poolUser, PageType pageType) {
		init(poolUser, pageType);
	}

	/**
	 * �����������ͷ������ڴ�
	 */
	~Array() {
		clear();
		while (m_usedPages > 0)
			popPage();
		free(m_pages);
	}

	/**
	 * �õ�ָ��λ�õ�Ԫ��
	 * @pre i���ܳ��������С�������assert
	 *
	 * @param i Ԫ����������0��ʼ
	 * @return Ԫ������
	 */
	E& operator[](size_t i) const {
		assert(i < m_size);
		size_t pageNo, slot;
		getPageAndSlot(i, &pageNo, &slot);
		void *page = m_pages[pageNo];
		return *(((E *)page) + slot);
	}

	/**
	 * ����һ��Ԫ�ص�����ĩβ
	 *
	 * @param e Ҫ�����Ԫ��
	 * @param forceAllocForPool ʹ���ڴ�ҳ�ط���ռ�ʱ�Ƿ���allocPageForce�ӿ�
	 * @return �Ƿ�ɹ�
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
	 * ��������Ĵ�С
	 * 
	 * @param forceAllocForPool ʹ���ڴ�ҳ�ط���ռ�ʱ�Ƿ���allocPageForce�ӿ�
	 * @return �Ƿ�ɹ�
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
	 * �������Ԥ���ռ��С
	 *
	 * @return ����Ԥ���ռ��С
	 */
	size_t getReservedSize() const {
		return m_reservedSize;
	}

	/**
	 * �趨Ԥ���ռ�
	 *
	 * @param size Ԥ���ռ�
	 * @param forceAllocForPool ʹ���ڴ�ҳ�ط���ռ�ʱ�Ƿ���allocPageForce�ӿ�
	 * @return �Ƿ�ɹ�
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
	 * ɾ������β����Ԫ��
	 */
	void pop() {
		assert(m_size > 0);
		assert(m_tailSize > 0);
		last().~E();
		m_size--;
		m_tailSize--;
		if (m_tailSize == 0) {	// ��ǰҳ�Ѿ����ˣ����ǽ����ͷ�
			m_tailSize = m_elementsPerPage;
			if (m_size > m_reservedSize) {
				popPage();
			}
		}
	}

	/**
	 * �õ��������һ��Ԫ��
	 * @pre ����ǿ�
	 *
	 * @return �������һ��Ԫ��
	 */
	E& last() const {
		assert(m_size > 0);
		return (*this)[m_size - 1];
	}

	/**
	 * �õ������С
	 *
	 * @return �����С
	 */
	size_t getSize() const {
		return m_size;
	}

	/**
	 * �õ������Ѿ�����Ŀռ��С
	 * 
	 * @return �����Ѿ�����Ŀռ��С����λΪԪ�ظ���
	 */
	size_t getCapacity() const {
		return m_capacity;
	}

	/**
	 * ���������Ƿ�Ϊ��
	 *
	 * @return �����Ƿ�Ϊ��
	 */
	bool isEmpty() const {
		return m_size == 0;
	}

	/**
	 * �����������������
	 */
	void clear() {
		while (m_size > 0)
			pop();
		m_size = 0;
		m_tailSize = 0;
		shrinkCapacity(m_reservedSize);
	}

	/** ��������ռ�õ��ڴ��С
	 * @return ռ���ڴ��С
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

	/** ����һ����ҳ��
	 *
	 * @param forceAllocForPool ʹ���ڴ�ҳ�ط���ռ�ʱ�Ƿ���allocPageForce�ӿ�
	 * @return �ɹ�������ҳ�棬����NULL��ʾ����ҳ��ʧ��
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
	 * �������������
	 *
	 * @param size Ҫ��֤�����ܹ��洢��ô���Ԫ��
	 * @param forceAllocForPool ʹ���ڴ�ҳ�ط���ռ�ʱ�Ƿ���allocPageForce�ӿ�
	 * @return �ɹ����
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
	static const size_t	PAGE_SIZE = 1024;	/** ��ʹ���ڴ�ҳ��ʱ��ҳ��С */
private:
	PagePoolUser	*m_poolUser;	/** �ڴ�ҳ���û� */
	PageType	m_pageType;			/** �����õ��ڴ�ҳ���� */
	void	**m_pages;				/** �ڴ�ҳ���� */
	size_t	m_pagesSize;			/** m_pages�����С */
	size_t	m_usedPages;			/** �Ѿ������ҳ�� */
	size_t	m_size;					/** �����С */
	size_t	m_capacity;				/** �Ѿ�������ڴ��ܹ���Ŷ��ٸ�Ԫ�� */
	size_t	m_tailSize;				/** ���һҳ���Ѿ����˶��ٸ�Ԫ�� */
	size_t	m_elementsPerPage;		/** ÿҳ�ɴ洢���ٸ�Ԫ�� */
	size_t	m_eppLevel;				/** ��ÿҳ�洢��Ԫ����2����������ʱ����¼�ݴΣ�����Ϊ0 */
	size_t	m_reservedSize;			/** Ԥ���Ŀռ��С */
};

}
#endif
