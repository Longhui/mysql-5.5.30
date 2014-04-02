/**
 * 内存分配上下文实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include <new>
#include "misc/MemCtx.h"

namespace ntse {

#ifndef NTSE_MEM_CHECK
MemoryContext::MemoryContext(size_t pageSize, uint reservedPages) {
	m_memPool = NULL;
	init(pageSize, reservedPages);
}

MemoryContext::MemoryContext(CommonMemPool *memPool, uint reservedPages) {
	m_memPool = memPool;
	init(NTSE_PAGE_SIZE, reservedPages);
}

MemoryContext::~MemoryContext() {
	DLink<FMCPage *> *e, *next;
	for (e = m_pages.getHeader()->getNext(), next = e->getNext(); 
		e != m_pages.getHeader(); e = next, next = next->getNext()) {
		e->unLink();
		freePage(e->get());
	}
	m_currentPage = NULL;
	m_top = NULL;
}

void MemoryContext::init(size_t pageSize, uint reservedPages) {
	assert(reservedPages >= 1);
	m_pageSize = pageSize;
	m_reservedPages = reservedPages;
	// 预先分配要保留的页，为简化实现，第一页的大小为
	// m_pageSize * m_reservedPages
	m_memUsage = 0;
	uint totalSize = pageSize * reservedPages;
	if (m_memPool == NULL || totalSize <= NTSE_PAGE_SIZE) {
		// 仍然按保留一个页作简化处理
		m_currentPage = (char *)allocPage(0, totalSize);
		m_top = ((char *)m_currentPage) + sizeof(FMCPage);
	} else {
		// 因为使用了内存池，所以单次内存分配的大小不能超过MEM_CTX_MAX_ALLOC_IN_POOL
		assert(pageSize <= NTSE_PAGE_SIZE);	
		m_currentPage = (char *)allocPage(0, pageSize);
		m_top = ((char *)m_currentPage) + sizeof(FMCPage);
		for (uint i = 0; i < reservedPages - 1; i++) {
			if (!alloc(pageSize - sizeof(FMCPage)))
				NTSE_ASSERT(false);
		}
	}	
}

void* MemoryContext::alloc(size_t size) {
	size = ALIGN_SIZE(size);
	if ((m_top - m_currentPage) + size <= ((FMCPage *)m_currentPage)->m_size) {
		void *r = m_top;
		m_top += size;
		return r;
	}
	size_t allocSize = size + sizeof(FMCPage);
	if (allocSize < m_pageSize)
		allocSize = m_pageSize;
	void *page = allocPage(((FMCPage *)m_currentPage)->m_distance + ((FMCPage *)m_currentPage)->m_size, allocSize);
	if (!page)
		return NULL;
	m_currentPage = (char *)page;
	m_top = m_currentPage + sizeof(FMCPage) + size;
	return m_currentPage + sizeof(FMCPage);
}

void MemoryContext::realResetToSavepoint(u64 savepoint) {
	assert(setSavepoint() >= savepoint);
	if (savepoint == 0) {
		reset();
		return;
	}
	while (savepoint <= ((FMCPage *)m_currentPage)->m_distance) {
		m_pages.removeLast();
		freePage((FMCPage *)m_currentPage);
		m_currentPage = (char *)m_pages.getHeader()->getPrev()->get();
	}
	m_top = m_currentPage + (savepoint - ((FMCPage *)m_currentPage)->m_distance);
}

void MemoryContext::reset() {
	DLink<FMCPage *> *e, *next;
	for (e = m_pages.getHeader()->getNext()->getNext(), next = e->getNext(); 
		e != m_pages.getHeader(); e = next, next = next->getNext()) {
		e->unLink();
		freePage(e->get());
	}
	m_currentPage = (char *)m_pages.getHeader()->getNext()->get();
	m_top = m_currentPage + sizeof(FMCPage);
}

void* MemoryContext::allocPage(size_t distance, size_t size) {
	void *addr = NULL;

	if (m_memPool == NULL) {
		addr = malloc(size);
	} else {
		if (unlikely(size > m_pageSize))
			return NULL;
		addr = m_memPool->getPage();
	}

	if (!addr)
		return NULL;
	FMCPage *page = new (addr)FMCPage();
	page->m_distance = distance;
	page->m_size = size;
	page->m_link.set(page);
	m_pages.addLast(&page->m_link);
	m_memUsage += page->m_size;
	return page;
}

void MemoryContext::freePage(FMCPage *page) {
	m_memUsage -= page->m_size;
	if (m_memPool == NULL) {
		free(page);
	} else {
		m_memPool->releasePage(page);
	}
}

#else
/** 调试模式的内存分配上下文 */
MemoryContext::MemoryContext(size_t pageSize, uint reservedPages) {
	UNREFERENCED_PARAMETER(pageSize);
	UNREFERENCED_PARAMETER(reservedPages);
	m_memUsage = 0;
}

MemoryContext::MemoryContext(CommonMemPool *memPool, uint reservedPages) {
	UNREFERENCED_PARAMETER(memPool);
	UNREFERENCED_PARAMETER(reservedPages);
	m_memUsage = 0;
}

MemoryContext::~MemoryContext() {
	reset();
}

void* MemoryContext::alloc(size_t size) {
	void *chunk = allocChunk(size);
	if (!chunk)
		return NULL;
	return (char *)chunk + sizeof(DMCChunk);
}

void MemoryContext::realResetToSavepoint(u64 savepoint) {
	assert(m_chunks.getSize() >= savepoint);
	while (m_chunks.getSize() > savepoint) {
		DMCChunk *chunk = m_chunks.last();
		freeChunk(chunk);
		m_chunks.pop();
	}
}

void MemoryContext::reset() {
	resetToSavepoint(0);
}

DMCChunk* MemoryContext::allocChunk(size_t size) {
	DMCChunk *chunk = (DMCChunk *)malloc(size + sizeof(DMCChunk));
	if (!chunk)
		return NULL;
	chunk->m_size = size;
	m_chunks.push(chunk);
	m_memUsage += size;
	return chunk;
}

void MemoryContext::freeChunk(DMCChunk *chunk) {
	m_memUsage -= chunk->m_size;
	free(chunk);
}

#endif

}
