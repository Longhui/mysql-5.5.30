/** �ڴ����������
�ڴ���������ĵĸ�����Stonebraker�����Ŀ��������ڴ�����Ч�ʺͷ�ֹ�ڴ�й©��
ʹ���ڴ����������ʱ������ʹ�ñ�׼��new/delete(malloc/free)�������ڴ棬����
ͨ���ڴ�����������������ڴ档��Щ�ڴ治��Ҫ��ʽ��һ����ȥ�ͷţ�ֻ��Ҫ�ں��ʵ�ʱ��
�����ڴ���������ģ���ͨ���������ķ���������ڴ涼�ᱻ�ͷš�

�����ڴ���������Ĳ���Ҫ֧���ͷŵ����ڴ棬ʹ���ڴ���������ķ����ڴ�ʱ������ջ�Ϸ���
�ռ�һ�£����Լ򻯵�ֻ���ƶ�һ��ָ��ֵ�����Ч�ʷǳ��ߡ�

Stonebraker������ڴ����������ֻ֧����ȫ���ò��������ͷ�ͨ�������ķ���������ڴ棬
��һ����ʹ�ÿ���һЩӦ�ø��ӻ������������ݿ��У�����һ�������һ���Ƚ϶����Ĺ�����Ԫ��
�ڴ������ʱ����Ҫ����ܶ��ڴ棬���﷨��������ѯ�Ż�ʱ������﷨����ִ�мƻ��ȡ���Щ
�ڴ������ִ�н������û���ˣ����һ�����������������俪ʼʱ����һ���ڴ���������ģ�
Ȼ����䴦������е������ڴ涼����һ�����������䣬���õ����ڴ���ͷ����⣬���������
�������������ͷ������ڴ档�﷨����ִ�мƻ������ݽṹһ�����ֻ�����һ�Σ���С�ǿɿصģ�
������û�����⡣��һ�������ܻᴦ��ܶ��¼�����ڴ���һ����¼ʱ���ֿ��ܻ����һЩ�ڴ棬
���Ҳ��������伶�������������䣬�����������ռ�õ��ڴ�Ϳ���������������

PostgreSQL�н����������ķ�����ʹ�ö༶�ڴ���������ģ�����Ӧ��һ����伶�����ģ�
����ÿ����¼ʱ����һ����¼�������ġ���¼���������ڴ�����ÿ����¼�����á���һ�ⷨ����
��Ȼ����ʹ�������Ƚ��鷳�����ʱ������Ҫ���ķ�����ڴ����ĸ��������У����һ���ڴ�
��ͨ����¼�������ķ���ģ���������������¼��Ҫ�ã�����Ҫ��ʽ�Ľ���Щ�ڴ濽������伶
�������С�

NTSE�Ľ����������ǿ�ڴ���������ĵĹ��ܣ�����������úͻָ���һ������㡣һ�������
��Ӧ���ڴ����������ĳһʱ�̵�״̬���ָ���һ�������ʱ���ڴ˱����֮�����������ڴ�
���ᱻ�ͷţ��������֮ǰ������ڴ�����Ӱ�졣������NTSE�е��ڴ����������һ�������
���ġ�NTSE��������ѵ�ģ���ڴ���һ����¼ʱ����Ҫ����һЩ��ʱ�ڴ棬���ں�������֮���
������Ҫ���ڴ�ʱ�����ں�����ڴ�����һ������㣬�ں����˳�ʱ��Χ������㼴�ɡ��������
���ڴ��ں�������֮��Ҫ�ã�����ɨ�����ȣ�����Ҫ���ñ���㡣������Ե���MemoryContext
��setSavepoint��resetToSavepoint���������ú����õ�һ������㣬����ȫ�ķ�������
����һ��ʼ����һ��McSavepoint������һ����Ĺ��캯���л��Զ����ñ���㣬����ʱ���Զ�
���ûر���㣬������ʹ�������쳣Ҳ���Զ�������ȷ�����ò�����

�ڴ����������������ʵ�֡���Ʒ������Ӧ��ʹ�ÿ����ڴ���������ģ���һ������ʵ���ڴ����ʱ
ֻ���ƶ�һ��ָ��λ�á������������ڴ��޷�ʹ��Rational/Valgrind�ȹ��߼���ڴ�Խ���д
�ȴ�����˻�ʵ��һ������ģʽ���ڴ���������ģ�ֱ��ʹ��malloc�����ڴ档

 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
/** DebugMemoryContext������ڴ�� */
struct DMCChunk {
	size_t	m_size;		/** �ڴ���С */
};
#endif

/** �����ڴ�����������е��ڴ�ҳ */
struct FMCPage {
	DLink<FMCPage *>	m_link;	/** ����˫������ */
	size_t		m_distance;		/** ��֮ǰ��ҳ�������ٿռ䣬����ҳͷ */
	size_t		m_size;			/** ҳ��С������ҳͷ */
};

/** �ڴ���������� */
class MemoryContext {
public:
	static const uint MEM_CTX_MAX_ALLOC_IN_POOL = NTSE_PAGE_SIZE - sizeof(FMCPage);

public:
	/** ���캯��
	 *
	 * @param pageSize ҳ��С����������NTSE_MEM_CHECK��˲�������
	 * @param reservedPages resetʱ�������ٸ�ҳ����������NTSE_MEM_CHECK��˲�������
	 */
	MemoryContext(size_t pageSize, uint reservedPages);

	MemoryContext(CommonMemPool *memPool, uint reservedPages);

	/** �������� */
	~MemoryContext();

	/** ����һ���ڴ�
	 *
	 * @param size �ڴ���С
	 * @return �·�����ڴ�飬������ʧ���򷵻�NULL
	 */
	void* alloc(size_t size);

	/** ����һ���ڴ沢����
	 *
	 * @param size �ڴ���С
	 * @return �·�����ڴ�飬������ʧ���򷵻�NULL
	 */
	void* calloc(size_t size) {
		void *p = alloc(size);
		if (p)
			memset(p, 0, size);
		return p;
	}

	/** ����һ���ڴ�
	 *
	 * @param p Ҫ�������ڴ��ַ
	 * @parma size Ҫ�������ڴ��С
	 * @return �������ڴ棬������ʧ���򷵻�NULL
	 */
	void* dup(const void *p, size_t size) {
		void *p2 = alloc(size);
		if (p2)
			memcpy(p2, p, size);
		return p2;
	}

	/**
	 * ����һ�������
	 *
	 * @return �����
	 */
	u64 setSavepoint() {
#ifdef NTSE_MEM_CHECK
		return m_chunks.getSize();
#else
		return ((FMCPage *)m_currentPage)->m_distance + (m_top - m_currentPage);
#endif
	}
	
	/**
	 * �����ڴ���������ĵ���ǰ���õı���㣬�����ñ����֮�������ڴ�
	 * �����ͷţ�֮ǰ������ڴ�����Ӱ�졣
	 *
	 * @param savepoint �����
	 */
	void resetToSavepoint(u64 savepoint) {
		if (setSavepoint() == savepoint)
			return;
		realResetToSavepoint(savepoint);
	}

	/**
	 * ��ȫ�����ڴ���������ģ��ͷ��ô������ķ���������ڴ�
	 */
	void reset();

	/**
	 * �õ�ռ�õ��ڴ���
	 *
	 * @return ռ�õ��ڴ��������ڴ���������������ϵͳ������ڴ�����������
	 * �û����ڴ����������������ڴ���
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
	Array<DMCChunk *>	m_chunks;	/** ��������ڴ�� */
#else
	size_t	m_pageSize;				/** ҳ��С */
	uint	m_reservedPages;		/** ������ҳ�� */
	DList<FMCPage *>	m_pages;	/** ҳ������ */
	char	*m_currentPage;			/** ��ǰҳ */
	char	*m_top;					/** �ѷ����ڴ�β */
	CommonMemPool	*m_memPool;		/** �ڴ�ҳ�� */
#endif
	size_t	m_memUsage;				/** ռ�õ��ڴ��С */
};

/** �ڴ���������ı���㡣���캯�����Զ����ñ���㣬�����������Զ����õ������
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
