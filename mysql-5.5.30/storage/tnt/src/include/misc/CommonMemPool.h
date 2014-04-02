/**
 * ͨ���ڴ��
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */
#ifndef _NTSE_COMMON_MEM_POOL_H_
#define _NTSE_COMMON_MEM_POOL_H_

#include "util/PagePool.h"
#include "misc/Global.h"
#include "util/Sync.h"

namespace ntse {

/**
 * ͨ���ڴ��������InnoDB�е�common memory pool(additional memory pool)��InnoDB��ͨ��Memory Heap
 * �ķ�ʽ���ж�̬�ڴ����ģ�ΪһЩ��ͨС�����ݶ�������ڴ�ʱ����Ҫ��additional memory pool 
 * �����룬����ʱ����buffer pool������(����������������Ӧ��ϣ������)���������ĺô�һ���ǿ��Կ����ڴ�
 * ��ʹ��������һ����ͨ���Լ������ڴ���ⲿ���ڴ���Ƭ��NTSE�Ѿ���Memory Context, �����Ѿ����㶯̬
 * �ڴ��������󣬵�Memory Context��ֱ����Running Enviroment�����ڴ棬û�������ڴ�ʹ���������Զ�
 * Memory Context���и��죬����Memory Context�Ϳ���ʹ���ڴ�������ڴ档�����ֻ��ͨ���ڴ�صĹ���
 * ֮һ���Ժ󻹿��Ի���ͨ���ڴ��ʵ��������InnoDB�Ķ�̬�ڴ�����㷨��������������󻹲��Ǻ����С�Ŀǰ��
 * ͨ��������ʽ�����ڴ���Ƭ��
 *
 * TODO������ͨ���ڴ��ʵ�ֻ���㷨
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
	Mutex        m_lock;         /* ����ȫ�ֽṹ�Ļ����� */
};

}

#endif