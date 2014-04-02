/**
 * 通用内存池
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef _NTSE_COMMON_MEM_POOL_H_
#define _NTSE_COMMON_MEM_POOL_H_

#include "util/PagePool.h"
#include "misc/Global.h"
#include "util/Sync.h"

namespace ntse {

/**
 * 通用内存池类似于InnoDB中的common memory pool(additional memory pool)。InnoDB是通过Memory Heap
 * 的方式进行动态内存管理的，为一些普通小型数据对象分配内存时，需要从additional memory pool 
 * 中申请，不够时则会从buffer pool中申请(比如事务锁，自适应哈希索引等)。这样做的好处一个是可以控制内存
 * 的使用量，另一方面通过自己管理内存避免部分内存碎片。NTSE已经有Memory Context, 基本已经满足动态
 * 内存管理的需求，但Memory Context是直接向Running Enviroment申请内存，没法控制内存使用量。所以对
 * Memory Context进行改造，这样Memory Context就可以使用内存池申请内存。但这个只是通用内存池的功能
 * 之一，以后还可以基于通用内存池实现类似于InnoDB的动态内存管理算法，但现在这个需求还不是很迫切。目前是
 * 通过其他方式避免内存碎片。
 *
 * TODO：基于通用内存池实现伙伴算法
 */
class CommonMemPool : public PagePoolUser {
public:
	static const u16 VIRTUAL_USER_ID_COM_POOL = (u16)-1;
public:
	CommonMemPool(uint targetSize, PagePool *pool) : PagePoolUser(targetSize, pool), 
		m_lock("Common Pool Mutex", __FILE__, __LINE__) {}
	~CommonMemPool() {}

	uint freeSomePages(u16 userId, uint numPages);
	void* getPage(bool force = false);
	void releasePage(void *page);

public:
	Mutex        m_lock;         /* 保护全局结构的互斥锁 */
};

}

#endif