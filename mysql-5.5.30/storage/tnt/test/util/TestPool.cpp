/**
 * �����ڴ�ҳ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include <cppunit/config/SourcePrefix.h>
#include "util/TestPool.h"
#include "util/PagePool.h"

using namespace ntse;

const char* PoolTestCase::getName() {
	return "Page pool test";
}

const char* PoolTestCase::getDescription() {
	return "Test function of page pool";
}

bool PoolTestCase::isBig() {
	return false;
}

class TestPoolUser: public PagePoolUser {
public:
	TestPoolUser(uint targetSize, PagePool *pool): PagePoolUser(targetSize, pool) {
	}

	~TestPoolUser() {
		while (!m_pages.isEmpty()) {
			DLink<void *> *e = m_pages.getHeader()->getNext();
			e->unLink();
			delete e;
		}
	}

	virtual uint freeSomePages(u16 userId, uint numPages) {
		uint count = PagePoolUser::freeSomeFreePages(userId, numPages);
		if (count >= numPages)
			return count;
		while (count < numPages && !m_pages.isEmpty()) {
			DLink<void *> *e = m_pages.removeFirst();
			freePage(userId, e->get());
			delete e;
			count++;
		}
		return count;
	}

	void addPage(void *page) {
		m_pages.addLast(new DLink<void *>(page));
	}

	void removePage(void *page) {
		for (DLink<void *> *e = m_pages.getHeader()->getNext(); e != m_pages.getHeader();
			e = e->getNext()) {
			if (e->get() == page) {
				e->unLink();
				delete e;
				return;
			}
		}
	}

private:
	DList<void *>	m_pages;
};

/** ���Ի�����ҳ������ͷŹ��� */
void PoolTestCase::testAlloc() {
	PagePool pool(0, 1024);
	uint pageCount = PagePoolUser::CACHE_SIZE * 2;
	TestPoolUser user(pageCount, &pool);
	pool.registerUser(&user);
	pool.init();

	CPPUNIT_ASSERT(pool.getPageSize() == 1024);
	CPPUNIT_ASSERT(pool.getSize() == pageCount);
	CPPUNIT_ASSERT(!pool.isFull());
	
	void **pages = (void **)calloc(sizeof(void *), pageCount);
	for (uint i = 0; i < pageCount; i++) {
		pages[i] = user.allocPage(0, PAGE_HEAP, NULL);
		CPPUNIT_ASSERT(pages[i]);
		CPPUNIT_ASSERT(user.getCurrentSize() == i + 1);
		CPPUNIT_ASSERT(pool.getType(pages[i]) == PAGE_HEAP);
		CPPUNIT_ASSERT(pool.getInfo(pages[i]) == NULL);
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Exclusived));
		if (i < pageCount - 1)
			CPPUNIT_ASSERT(!pool.isFull());
	}
	CPPUNIT_ASSERT(pool.isFull());
	CPPUNIT_ASSERT(user.allocPage(0, PAGE_HEAP, NULL) == NULL);
	CPPUNIT_ASSERT(user.getCurrentSize() == pageCount);

	for (uint i = 0; i < pageCount; i++) {
		user.freePage(0, pages[i]);

		CPPUNIT_ASSERT(pool.getType(pages[i]) == PAGE_EMPTY);
	}
	CPPUNIT_ASSERT(user.getCurrentSize() == PagePoolUser::CACHE_SIZE);
	CPPUNIT_ASSERT(!pool.isFull());
	free(pages);

	pool.preDelete();
}

/** ���Զ���û�ʱ��̬�������� */
void PoolTestCase::testDynamicAdjust() {
	PagePool pool(0, 1024);
	uint pageCount = PagePoolUser::CACHE_SIZE * 2;
	TestPoolUser user1(pageCount, &pool);
	TestPoolUser user2(pageCount, &pool);
	pool.registerUser(&user1);
	pool.registerUser(&user2);
	pool.init();
	pool.getRebalancer()->pause();

	CPPUNIT_ASSERT(pool.getPageSize() == 1024);
	CPPUNIT_ASSERT(pool.getSize() == pageCount * 2);
	CPPUNIT_ASSERT(!pool.isFull());

	// һ���û������䣬��һ���û�������Գ�����Ŀ���С
	void **pages = (void **)calloc(sizeof(void *), pageCount * 2);
	for (uint i = 0; i < pageCount * 2; i++) {
		pages[i] = user1.allocPage(0, PAGE_HEAP, NULL);
		CPPUNIT_ASSERT(pages[i]);
		user1.addPage(pages[i]);
		CPPUNIT_ASSERT(user1.getCurrentSize() == i + 1);
		CPPUNIT_ASSERT(pool.getType(pages[i]) == PAGE_HEAP);
		CPPUNIT_ASSERT(pool.getInfo(pages[i]) == NULL);
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Exclusived));
		if (i < pageCount - 1)
			CPPUNIT_ASSERT(!pool.isFull());
	}
	CPPUNIT_ASSERT(pool.isFull());
	CPPUNIT_ASSERT(user1.allocPage(0, PAGE_HEAP, NULL) == NULL);
	CPPUNIT_ASSERT(user1.getCurrentSize() == pageCount * 2);

	// ��һ���û�����ռ䣬ԭ�û��ͷſռ�ﵽƽ��
	for (uint i = 0; i < pageCount; i++) {
		user1.freePage(0, pages[i]);
		user1.removePage(pages[i]);
	}
	for (uint i = 0; i < pageCount; i++) {
		pages[i] = user2.allocPage(0, PAGE_HEAP, NULL);
		CPPUNIT_ASSERT(pages[i]);
		user2.addPage(pages[i]);
	}
	CPPUNIT_ASSERT(user1.getCurrentSize() == pageCount);
	CPPUNIT_ASSERT(user2.getCurrentSize() == pageCount);
	CPPUNIT_ASSERT(user2.allocPage(0, PAGE_HEAP, NULL) == NULL);
	CPPUNIT_ASSERT(user1.allocPage(0, PAGE_HEAP, NULL) == NULL);

	// ������С
	CPPUNIT_ASSERT(!user1.setTargetSize(pageCount + 20));
	CPPUNIT_ASSERT(user2.setTargetSize(pageCount - 20));
	CPPUNIT_ASSERT(user1.setTargetSize(pageCount + 20));
	// ��̨�����̱߳�ֹͣ��user2�����ͷ�ҳ�浼�·���ʧ��
	for (uint i = 0; i < 20; i++)
		CPPUNIT_ASSERT(!user1.allocPage(0, PAGE_HEAP, NULL));
	pool.getRebalancer()->resume();
	pool.getRebalancer()->signal();
	Thread::msleep(1000);
	// ��̨�����߳��Ѿ�֪ͨuser2�ͷ�ҳ�棬user1�����ܳɹ�
	for (uint i = 0; i < 20; i++)
		CPPUNIT_ASSERT(user1.allocPage(0, PAGE_HEAP, NULL));
	// ����Ŀ���С�󣬷���ʧ��
	CPPUNIT_ASSERT(!user1.allocPage(0, PAGE_HEAP, NULL));
	// ��ǿ�Ʒ����ɹ�
	CPPUNIT_ASSERT(user1.allocPageForce(0, PAGE_HEAP, NULL));
	free(pages);

	pool.preDelete();
}

/** ���Լӽ�����ع��� */
void PoolTestCase::testLock() {
	PagePool pool(0, 1024);
	uint pageCount = 20;
	TestPoolUser user(pageCount, &pool);
	pool.registerUser(&user);
	pool.init();

	// �����ͽ���
	void **pages = (void **)calloc(sizeof(void *), pageCount);
	for (uint i = 0; i < pageCount; i++) {
		pages[i] = user.allocPage(0, PAGE_HEAP, NULL);
		pool.unlockPage(0, pages[i], Exclusived);
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Exclusived));

		pool.lockPage(0, pages[i], Shared, __FILE__, __LINE__);
		CPPUNIT_ASSERT(pool.trylockPage(0, pages[i], Shared, __FILE__, __LINE__));
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Exclusived));

		CPPUNIT_ASSERT(!pool.trylockPage(0, pages[i], Exclusived, __FILE__, __LINE__));
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Exclusived));

		pool.unlockPage(0, pages[i], Shared);
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Shared));
		pool.unlockPage(0, pages[i], Shared);
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Exclusived));

		pool.lockPage(0, pages[i], Exclusived, __FILE__, __LINE__);
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Exclusived));

		pool.unlockPage(0, pages[i], Exclusived);
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Exclusived));

		pool.lockPage(0, pages[i], Exclusived, __FILE__, __LINE__);
	}
	for (uint i = 0; i < pageCount; i++)
		user.freePage(0, pages[i]);

	// lockPageIfType
	for (uint i = 0; i < pageCount; i++) {
		pages[i] = user.allocPage(0, PAGE_HEAP, NULL);
		pool.unlockPage(0, pages[i], Exclusived);
		// ����ȷ�����ͣ�����ʧ��
		CPPUNIT_ASSERT(!pool.lockPageIfType(0, pages[i], Shared, PAGE_INDEX, 0, __FILE__, __LINE__));
		// ��ȷ�����ͣ������ɹ�
		CPPUNIT_ASSERT(pool.lockPageIfType(0, pages[i], Shared, PAGE_HEAP, 0, __FILE__, __LINE__));
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Exclusived));
		// ��ʱ
		CPPUNIT_ASSERT(!pool.lockPageIfType(0, pages[i], Exclusived, PAGE_HEAP, 10, __FILE__, __LINE__));
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Exclusived));

		pool.unlockPage(0, pages[i], Shared);
		pool.lockPage(0, pages[i], Exclusived, __FILE__, __LINE__);
	}
	for (uint i = 0; i < pageCount; i++)
		user.freePage(0, pages[i]);

	// �������ͷ�ҳ��ʱ�Զ��ӽ���
	for (uint i = 0; i < pageCount; i++) {
		pages[i] = user.allocPage(0, PAGE_HEAP, NULL);

		CPPUNIT_ASSERT(!pool.isPageLocked(pages[i], Shared));
		CPPUNIT_ASSERT(pool.isPageLocked(pages[i], Exclusived));
	}
	for (uint i = 0; i < pageCount; i++)
		user.freePage(0, pages[i]);
	free(pages);

	pool.preDelete();
}

/** ���Ը����ڴ�ҳ�в���ַ�õ��ڴ�ҳ�׵�ַ���� */
void PoolTestCase::testAlign() {
	PagePool pool(0, 1024);
	uint pageCount = 10;
	TestPoolUser user(pageCount, &pool);
	pool.registerUser(&user);
	pool.init();

	void **pages = (void **)calloc(sizeof(void *), pageCount);
	for (uint i = 0; i < pageCount; i++) {
		pages[i] = user.allocPage(0, PAGE_HEAP, NULL);

		for (char *p = (char *)pages[i]; p < (char *)pages[i] + pool.getPageSize(); p++)
			CPPUNIT_ASSERT(pool.alignPage(p) == pages[i]);
	}
	free(pages);

	pool.preDelete();
}

/** �����ڴ�ҳ�������� */
void PoolTestCase::testScan() {
	PagePool pool(0, 1024);
	TestPoolUser user1(2, &pool);
	TestPoolUser user2(2, &pool);
	pool.registerUser(&user1);
	pool.registerUser(&user2);
	pool.init();

	void *p11 = user1.allocPage(0, PAGE_HEAP, NULL);
	pool.unlockPage(0, p11, Exclusived);
	void *p12 = user1.allocPage(0, PAGE_HEAP, NULL);
	pool.unlockPage(0, p12, Exclusived);
	void *p21 = user2.allocPage(0, PAGE_HEAP, NULL);
	pool.unlockPage(0, p21, Exclusived);
	void *p22 = user2.allocPage(0, PAGE_HEAP, NULL);
	pool.unlockPage(0, p22, Exclusived);

	PoolScanHandle *h = pool.beginScan(&user1, 0);
	CPPUNIT_ASSERT(p11 == pool.getNext(h));
	CPPUNIT_ASSERT(pool.isPageLocked(p11, Shared));
	CPPUNIT_ASSERT(p12 == pool.getNext(h));
	CPPUNIT_ASSERT(!pool.isPageLocked(p11, Shared));
	CPPUNIT_ASSERT(pool.isPageLocked(p12, Shared));
	CPPUNIT_ASSERT(!pool.getNext(h));
	CPPUNIT_ASSERT(!pool.isPageLocked(p12, Shared));
	pool.endScan(h);

	h = pool.beginScan(&user2, 0);
	CPPUNIT_ASSERT(p21 == pool.getNext(h));
	CPPUNIT_ASSERT(p22 == pool.getNext(h));
	CPPUNIT_ASSERT(!pool.getNext(h));
	pool.endScan(h);

	h = pool.beginScan(NULL, 0);
	CPPUNIT_ASSERT(p11 == pool.getNext(h));
	CPPUNIT_ASSERT(p12 == pool.getNext(h));
	CPPUNIT_ASSERT(p21 == pool.getNext(h));
	CPPUNIT_ASSERT(p22 == pool.getNext(h));
	CPPUNIT_ASSERT(!pool.getNext(h));
	pool.endScan(h);

	// ɨ����;����ʱҲ�����
	h = pool.beginScan(&user1, 0);
	CPPUNIT_ASSERT(p11 == pool.getNext(h));
	CPPUNIT_ASSERT(pool.isPageLocked(p11, Shared));
	pool.endScan(h);
	CPPUNIT_ASSERT(!pool.isPageLocked(p11, Shared));
}
