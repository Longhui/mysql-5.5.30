/** 内存分配上下文
内存分配上下文的概念由Stonebraker提出，目的是提高内存分配的效率和防止内存泄漏。
使用内存分配上下文时，程序不使用标准的new/delete(malloc/free)来分配内存，而是
通过内存分配上下文来分配内存。这些内存不需要显式的一个个去释放，只需要在合适的时候
重置内存分配上下文，则通过此上下文分配的所有内存都会被释放。

由于内存分配上下文不需要支持释放单块内存，使用内存分配上下文分配内存时就像在栈上分配
空间一下，可以简化到只是移动一个指针值，因此效率非常高。

Stonebraker提出的内存分配上下文只支持完全重置操作，即释放通过上下文分配的所有内存，
这一限制使得开发一些应用复杂化。比如在数据库中，处理一条语句是一个比较独立的工作单元，
在处理语句时，需要分配很多内存，如语法解析、查询优化时分配的语法树、执行计划等。这些
内存在语句执行结束后就没用了，因此一个合理的做法是在语句开始时创建一个内存分配上下文，
然后语句处理过程中的所有内存都由这一上下文来分配，不用担心内存的释放问题，最后语句结束
后重置上下文释放所有内存。语法树、执行计划等数据结构一条语句只会分配一次，大小是可控的，
这样做没有问题。但一条语句可能会处理很多记录，而在处理一条记录时，又可能会分配一些内存，
如果也用上述语句级的上下文来分配，则这个上下文占用的内存就可能无限制增长。

PostgreSQL中解决上述问题的方法是使用多级内存分配上下文，语句对应的一个语句级上下文，
处理每条记录时还有一个记录级上下文。记录级上下文在处理完每条记录后都重置。这一解法方案
虽然灵活，但使用起来比较麻烦。编程时经常需要关心分配的内存在哪个上下文中，如果一个内存
是通过记录级上下文分配的，但处理完这条记录后还要用，就需要显式的将这些内存拷贝到语句级
上下文中。

NTSE的解决方法是增强内存分配上下文的功能，允许程序设置和恢复到一个保存点。一个保存点
对应着内存分配上下文某一时刻的状态，恢复到一个保存点时，在此保存点之后分配的所有内存
都会被释放，而保存点之前分配的内存则不受影响。这样，NTSE中的内存分配上下文一定是语句
级的。NTSE中索引或堆等模块在处理一条记录时若需要分配一些临时内存，即在函数返回之后就
不再需要的内存时，可在函数入口处设置一个保存点，在函数退出时重围到保存点即可。如果分配
的内存在函数返回之后还要用，比如扫描句柄等，则不需要设置保存点。程序可以调用MemoryContext
的setSavepoint和resetToSavepoint方法来设置和重置到一个保存点，更安全的方法是在
函数一开始定义一个McSavepoint对象，这一对象的构造函数中会自动设置保存点，析构时会自动
重置回保存点，这样即使出现了异常也会自动进行正确的重置操作。

内存分配上下文有两个实现。产品环境中应该使用快速内存分配上下文，这一上下文实现内存分配时
只是移动一个指针位置。但这样分配内存无法使用Rational/Valgrind等工具检查内存越界读写
等错误，因此还实现一个调试模式的内存分配上下文，直接使用malloc分配内存。

 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_MEMCTX_H_
#define _NTSE_MEMCTX_H_

#include "misc/CommonMemPool.h"
#include "util/Portable.h"
#include "util/DList.h"
#include <stdlib.h>
#include "util/Array.h"

namespace ntse {

#ifdef NTSE_MEM_CHECK
/** DebugMemoryContext分配的内存块 */
struct DMCChunk {
	size_t	m_size;		/** 内存块大小 */
};
#endif

/** 快速内存分配上下文中的内存页 */
struct FMCPage {
	DLink<FMCPage *>	m_link;	/** 构成双向链表 */
	size_t		m_distance;		/** 我之前的页包含多少空间，包括页头 */
	size_t		m_size;			/** 页大小，包括页头 */
};

/** 内存分配上下文 */
class MemoryContext {
public:
	static const uint MEM_CTX_MAX_ALLOC_IN_POOL = NTSE_PAGE_SIZE - sizeof(FMCPage);

public:
	/** 构造函数
	 *
	 * @param pageSize 页大小，若定义了NTSE_MEM_CHECK则此参数无用
	 * @param reservedPages reset时保留多少个页，若定义了NTSE_MEM_CHECK则此参数无用
	 */
	MemoryContext(size_t pageSize, uint reservedPages);

	MemoryContext(CommonMemPool *memPool, uint reservedPages);

	/** 析构函数 */
	~MemoryContext();

	/** 分配一块内存
	 *
	 * @param size 内存块大小
	 * @return 新分配的内存块，若分配失败则返回NULL
	 */
	void* alloc(size_t size);

	/** 分配一块内存并清零
	 *
	 * @param size 内存块大小
	 * @return 新分配的内存块，若分配失败则返回NULL
	 */
	void* calloc(size_t size) {
		void *p = alloc(size);
		if (p)
			memset(p, 0, size);
		return p;
	}

	/** 拷贝一块内存
	 *
	 * @param p 要拷贝的内存地址
	 * @parma size 要拷贝的内存大小
	 * @return 拷贝的内存，若分配失败则返回NULL
	 */
	void* dup(const void *p, size_t size) {
		void *p2 = alloc(size);
		if (p2)
			memcpy(p2, p, size);
		return p2;
	}

	/**
	 * 设置一个保存点
	 *
	 * @return 保存点
	 */
	u64 setSavepoint() {
#ifdef NTSE_MEM_CHECK
		return m_chunks.getSize();
#else
		return ((FMCPage *)m_currentPage)->m_distance + (m_top - m_currentPage);
#endif
	}
	
	/**
	 * 重置内存分配上下文到先前设置的保存点，在设置保存点之后分配的内存
	 * 都被释放，之前分配的内存则不受影响。
	 *
	 * @param savepoint 保存点
	 */
	void resetToSavepoint(u64 savepoint) {
		if (setSavepoint() == savepoint)
			return;
		realResetToSavepoint(savepoint);
	}

	/**
	 * 完全重置内存分配上下文，释放用此上下文分配的所有内存
	 */
	void reset();

	/**
	 * 得到占用的内存量
	 *
	 * @return 占用的内存量，是内存分配上下文向操作系统申请的内存量，而不是
	 * 用户向内存分配上下文申请的内存量
	 */
	u64 getMemUsage() const {
		return m_memUsage;
	}

private:
	void realResetToSavepoint(u64 savepoint);
#ifdef NTSE_MEM_CHECK
	DMCChunk* allocChunk(size_t size);
	void freeChunk(DMCChunk *chunk);
#else
	void init(size_t pageSize, uint reservedPages);
	void* allocPage(size_t distance, size_t size);
	void freePage(FMCPage *page);
#endif

private:
#ifdef NTSE_MEM_CHECK
	Array<DMCChunk *>	m_chunks;	/** 各分配的内存块 */
#else
	size_t	m_pageSize;				/** 页大小 */
	uint	m_reservedPages;		/** 保留的页数 */
	DList<FMCPage *>	m_pages;	/** 页面链表 */
	char	*m_currentPage;			/** 当前页 */
	char	*m_top;					/** 已分配内存尾 */
	CommonMemPool	*m_memPool;		/** 内存页池 */
#endif
	size_t	m_memUsage;				/** 占用的内存大小 */
};

/** 内存分配上下文保存点。构造函数中自动设置保存点，析构函数中自动重置到保存点
 */
class McSavepoint {
public:
	McSavepoint(MemoryContext *mc) {
		m_memoryContext = mc;
		m_savepoint = mc->setSavepoint();
	}
	~McSavepoint() {
		m_memoryContext->resetToSavepoint(m_savepoint);
	}
private:
	MemoryContext	*m_memoryContext;
	u64	m_savepoint;
};

}

#endif
