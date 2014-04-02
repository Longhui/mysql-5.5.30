#include "misc/TestBuffer.h"
#include "misc/Buffer.h"
#include "util/File.h"
#include "misc/Syslog.h"
#include "util/Thread.h"
#include <iostream>
#include "misc/Session.h"
#include "api/Database.h"

using namespace std;

const char* BufferTestCase::getName() {
	return "Buffer test";
}

const char* BufferTestCase::getDescription() {
	return "Functional test for page buffer.";
}

bool BufferTestCase::isBig() {
	return false;
}

void BufferTestCase::setUp() {
	File f("buftest");
	f.remove();
	File f2("test2");
	f2.remove();
}

void BufferTestCase::tearDown() {
	File f("buftest");
	f.remove();
	File f2("test2");
	f2.remove();
}

class BufferTestGetThread: public Thread {
public:
	BufferTestGetThread(File *file, Buffer *buffer, u64 pageId, LockMode lockMode, DBObjStats *dbObjStats): Thread("BufferTestGetThread") {
		m_file = file;
		m_buffer = buffer;
		m_pageId = pageId;
		m_lockMode = lockMode;
		m_dbObjStats = dbObjStats;
		m_got = false;
	}

	void run() {
		Connection conn;
		Session session(&conn, m_buffer);
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, m_pageId, m_lockMode, m_dbObjStats, NULL);
		m_got = true;
		m_buffer->releasePage(&session, page, m_lockMode);
	}

	bool isGot() {
		return m_got;
	}

private:
	File	*m_file;
	Buffer	*m_buffer;
	u64		m_pageId;
	LockMode	m_lockMode;
	bool	m_got;
	DBObjStats *m_dbObjStats;
};

void BufferTestCase::init(uint bufferSize, uint numPages) {
	m_pool = new PagePool(4, Limits::PAGE_SIZE);
	m_numPages = numPages;
	m_syslog = new Syslog(NULL, EL_LOG, true, true);
	m_buffer = new Buffer(NULL, bufferSize, m_pool, m_syslog, NULL);
	m_dbObjStats = new DBObjStats(DBO_Heap);
	m_pool->registerUser(m_buffer);
	m_pool->init();

	m_file = new File("buftest");
	m_file->create(true, true);
	m_file->setSize(numPages * Limits::PAGE_SIZE);
}

void BufferTestCase::cleanUp() {
	m_pool->preDelete();
	delete m_buffer;
	m_file->close();
	m_file->remove();
	delete m_file;
	delete m_pool;
	delete m_syslog;
	delete m_dbObjStats;
}

void BufferTestCase::testGet() {
	init(4, 8);
	m_buffer->disableScavenger();

	CPPUNIT_ASSERT(m_buffer->isChecksumed());
	m_buffer->setChecksumed(false);
	CPPUNIT_ASSERT(!m_buffer->isChecksumed());
	m_buffer->setChecksumed(true);
	CPPUNIT_ASSERT(m_buffer->isChecksumed());

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化各页面数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		CPPUNIT_ASSERT(page);
		CPPUNIT_ASSERT(m_pool->isPageLocked(page, Exclusived));
		CPPUNIT_ASSERT(m_buffer->getStatus().m_logicalReads == i + 1);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_globalLockUsage->m_rlockCnt == i + 1);
		memset(page, 0, m_pool->getPageSize());
		
		m_buffer->markDirty(&session, page);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_logicalWrites == i + 1);
		CPPUNIT_ASSERT(m_buffer->isDirty(page));
		m_buffer->writePage(&session, page);
		CPPUNIT_ASSERT(!m_buffer->isDirty(page));
		CPPUNIT_ASSERT(m_buffer->getStatus().m_physicalWrites == i + 1);
		m_buffer->releasePage(&session, page, Exclusived);
		CPPUNIT_ASSERT(!m_pool->isPageLocked(page, Exclusived));
	}

	// 读取各个页
	m_buffer->freePages(&session, m_file, false);
	u64 savedLogicalReads = m_buffer->getStatus().m_logicalReads;
	for (uint i = 0; i < m_numPages; i++) {
		// 物理IO
		BufferPageHdr *page1 = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats, NULL);
		CPPUNIT_ASSERT(page1);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_logicalReads == savedLogicalReads + 5 * i + 1);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_physicalReads == i + 1);
		CPPUNIT_ASSERT(m_pool->isPageLocked(page1, Shared));
		// 逻辑IO
		BufferPageHdr *page2 = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats, NULL);
		CPPUNIT_ASSERT(page2);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_logicalReads == savedLogicalReads + 5 * i + 2);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_physicalReads == i + 1);
		// 指定guess，逻辑IO
		BufferPageHdr *page3 = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats, page1);
		CPPUNIT_ASSERT(page3);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_logicalReads == savedLogicalReads + 5 * i + 3);
		// 不相容的锁模式，加不上锁
		BufferPageHdr *page4 = m_buffer->tryGetPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		CPPUNIT_ASSERT(m_buffer->getStatus().m_logicalReads == savedLogicalReads + 5 * i + 4);
		CPPUNIT_ASSERT(!page4);

		m_buffer->releasePage(&session, page1, Shared);
		m_buffer->releasePage(&session, page2, Shared);
		m_buffer->releasePage(&session, page3, Shared);
		CPPUNIT_ASSERT(!m_pool->isPageLocked(page1, Shared));

		page4 = m_buffer->tryGetPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		CPPUNIT_ASSERT(page4);
		CPPUNIT_ASSERT(m_pool->isPageLocked(page4, Exclusived));
		CPPUNIT_ASSERT(m_buffer->getStatus().m_logicalReads == savedLogicalReads + 5 * i + 5);
		m_buffer->releasePage(&session, page4, Exclusived);
		CPPUNIT_ASSERT(!m_pool->isPageLocked(page4, Exclusived));
	}

	// 两个线程锁定页面发生冲突时等待
	{
		BufferTestGetThread *thread = new BufferTestGetThread(m_file, m_buffer, 1, Shared, m_dbObjStats);
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 1, Exclusived, m_dbObjStats, NULL);
		CPPUNIT_ASSERT(m_pool->isPageLocked(page, Exclusived));
		thread->start();
		Thread::msleep(100);
		CPPUNIT_ASSERT(!thread->isGot());
		m_buffer->releasePage(&session, page, Exclusived);
		Thread::msleep(Buffer::GET_PAGE_SLEEP * 2);
		CPPUNIT_ASSERT(thread->isGot());
		delete thread;
	}

	// 缓存满并且找不到页面替换时等待
	{
		BufferTestGetThread *thread = new BufferTestGetThread(m_file, m_buffer, 4, Shared, m_dbObjStats);
		BufferPageHdr **pages = (BufferPageHdr **)malloc(sizeof(BufferPageHdr *) * 4);
		for (uint i = 0; i < 4; i++) 
			pages[i] = m_buffer->tryGetPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);

		thread->start();
		Thread::msleep(100);
		CPPUNIT_ASSERT(!thread->isGot());

		for (uint i = 0; i < 4; i++)
			m_buffer->releasePage(&session, pages[i], Exclusived);
		thread->join();
		CPPUNIT_ASSERT(thread->isGot());
		
		delete thread;
		free(pages);
	}

	// getPage时指定错误的guess
	{
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 0, Exclusived, m_dbObjStats, NULL);
		// guess指定的页面被锁定
		BufferPageHdr *page2 = m_buffer->getPage(&session, m_file, PAGE_HEAP, 1, Exclusived, m_dbObjStats, page);
		CPPUNIT_ASSERT(page2 && page2 != page);
		m_buffer->releasePage(&session, page, Exclusived);
		m_buffer->releasePage(&session, page2, Exclusived);

		// guess指定的页面没有被锁定
		page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 0, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
		page2 = m_buffer->getPage(&session, m_file, PAGE_HEAP, 1, Exclusived, m_dbObjStats, page);
		m_buffer->releasePage(&session, page2, Exclusived);
	}

	// tryGetPage由于缓存区满，找不到页面替换而失败
	{
		BufferPageHdr **pages = (BufferPageHdr **)malloc(sizeof(BufferPageHdr *) * 4);
		for (uint i = 0; i < 4; i++) 
			pages[i] = m_buffer->tryGetPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		BufferPageHdr *page = m_buffer->tryGetPage(&session, m_file, PAGE_HEAP, 4, Shared, m_dbObjStats);
		CPPUNIT_ASSERT(!page);
		for (uint i = 0; i < 4; i++)
			m_buffer->releasePage(&session, pages[i], Exclusived);
		free(pages);
	}

	// 验证发生替换时会修改页面类型
	m_buffer->freePages(&session, m_file, false);
	for (uint i = 0; i < m_buffer->getTargetSize(); i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats);
		m_buffer->releasePage(&session, page, Shared);
	}
	for (uint i = m_buffer->getTargetSize(); i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_INDEX, i, Shared, m_dbObjStats);
		CPPUNIT_ASSERT(m_pool->getType(page) == PAGE_INDEX);
		m_buffer->releasePage(&session, page, Shared);
	}

	cleanUp();
}

void BufferTestCase::testLock() {
	init(4, 8);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化各页面数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->writePage(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->unlockPage(session.getId(), page, Exclusived);
		m_buffer->lockPage(session.getId(), page, Exclusived, true);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->unlockPage(session.getId(), page, Exclusived);
		m_buffer->lockPage(session.getId(), page, Exclusived, true);
		m_buffer->unlockPage(session.getId(), page, Exclusived);
		m_buffer->unpinPage(page);
	}

	cleanUp();
}

void BufferTestCase::testScavenger() {
	init(4, 4);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化各页面数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	// 锁住一个页，验证Scavenger会跳过这个脏页
	BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 1, Exclusived, m_dbObjStats, NULL);

	Thread::msleep(Buffer::SCAVENGER_DEFAULT_INTERVAL + 1000);
	CPPUNIT_ASSERT(m_buffer->getStatus().m_physicalWrites > 0);
	CPPUNIT_ASSERT(m_buffer->getStatus().m_scavengerWrites > 0);
	m_buffer->releasePage(&session, page, Exclusived);

	// 调整Scavenger参数
	m_buffer->setScavengerInterval(200);
	m_buffer->setScavengerMaxPagesRatio(0.6);
	m_buffer->setScavengerMultiplier(1.0);
	CPPUNIT_ASSERT(m_buffer->getScavengerInterval() == 200);
	CPPUNIT_ASSERT(m_buffer->getScavengerMaxPagesRatio() == 0.6);
	CPPUNIT_ASSERT(m_buffer->getScavengerMultiplier() == 1.0);
	// 设置不合理的值，不会生效
	m_buffer->setScavengerMaxPagesRatio(2.0);
	CPPUNIT_ASSERT(m_buffer->getScavengerMaxPagesRatio() == 0.6);
	m_buffer->setScavengerMultiplier(-1);
	CPPUNIT_ASSERT(m_buffer->getScavengerMultiplier() == 1.0);
	// 睡眠足够的时间使interval设置生效
	Thread::msleep(Buffer::SCAVENGER_DEFAULT_INTERVAL + 1000);

	u64 savedScavengerWrites = m_buffer->getStatus().m_scavengerWrites;
	u64 savedPhysicalWrites = m_buffer->getStatus().m_physicalWrites;
	m_buffer->freePages(&session, m_file, false);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	Thread::msleep(350);
	CPPUNIT_ASSERT(savedPhysicalWrites < m_buffer->getStatus().m_physicalWrites);
	CPPUNIT_ASSERT(savedScavengerWrites < m_buffer->getStatus().m_scavengerWrites);

	cleanUp();
}

bool shouldFreePage(BufferPageHdr *page, PageId pageId, uint indexId) {
	return page->m_lsn == indexId;
}

void BufferTestCase::testFreePages() {
	init(4, 4);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化各页面数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	// 调用freePages的再读，由于数据不在缓存中会导致物理IO增长
	m_buffer->freePages(&session, m_file, false);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		page->m_lsn = i;
		CPPUNIT_ASSERT(m_buffer->getStatus().m_physicalReads == i + 1);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	// 只会释放一个页面
	u64 savedPhysicalReads = m_buffer->getStatus().m_physicalReads;
	m_buffer->freePages(&session, m_file, 1, shouldFreePage);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	CPPUNIT_ASSERT(m_buffer->getStatus().m_physicalReads == savedPhysicalReads + 1);

	cleanUp();
}

void BufferTestCase::testFlush() {
	init(4, 4);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化各页面数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	m_buffer->freePages(&session, m_file, false);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	m_buffer->flushAll(NULL);
	CPPUNIT_ASSERT(m_buffer->getStatus().m_flushWrites == m_numPages);

	cleanUp();
}

void BufferTestCase::testPrefetch() {
	init(512, 512);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化各页面数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	m_buffer->freePages(&session, m_file, false);

	CPPUNIT_ASSERT(m_buffer->getPrefetchSize() == Buffer::DEFAULT_PREFETCH_SIZE);
	CPPUNIT_ASSERT(m_buffer->getPrefetchRatio() == Buffer::DEFAULT_PREFETCH_RATIO);
	
	// 正向预读
	m_buffer->setPrefetchSize(256 * Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(m_buffer->getPrefetchSize() == 256 * Limits::PAGE_SIZE);
	m_buffer->setPrefetchSize(127 * Limits::PAGE_SIZE);		// 不是2的整数次幂
	CPPUNIT_ASSERT(m_buffer->getPrefetchSize() == 256 * Limits::PAGE_SIZE);
	m_buffer->setPrefetchRatio(0.6);
	CPPUNIT_ASSERT(m_buffer->getPrefetchSize() == 256 * Limits::PAGE_SIZE);
	for (uint i = 0; i < m_buffer->getPrefetchSize() / Limits::PAGE_SIZE; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	CPPUNIT_ASSERT(m_buffer->getStatus().m_prefetches == 1);

	// 再访问一半预读进来的内容，替换统计对象
	for (uint i = 0; i < m_buffer->getPrefetchSize() / Limits::PAGE_SIZE; i += 2) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	// 再访问一半，替换统计对象
	for (uint i = 0; i < m_buffer->getPrefetchSize() / Limits::PAGE_SIZE; i += 2) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 64 + i, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	// 释放页面，但内部统计对象是否被正确释放
	m_buffer->freePages(&session, m_file, false);

	// 取消正在进行中的预读
	

	cleanUp();
}

void BufferTestCase::testFreeSomePages() {
	m_pool = new PagePool(4, Limits::PAGE_SIZE);
	m_dbObjStats = new DBObjStats(DBO_Heap);
	m_numPages = 8;
	m_syslog = new Syslog(NULL, EL_LOG, true, true);
	m_buffer = new Buffer(NULL, m_numPages / 2, m_pool, m_syslog, NULL);
	Buffer *buffer2 = new Buffer(NULL, m_numPages / 2, m_pool, m_syslog, NULL);
	m_pool->registerUser(m_buffer);
	m_pool->registerUser(buffer2);
	m_pool->init();

	m_file = new File("buftest");
	m_file->create(true, true);
	m_file->setSize(m_numPages * Limits::PAGE_SIZE);

	File *file2 = new File("test2");
	file2->create(true, true);
	file2->setSize(m_numPages * Limits::PAGE_SIZE);

	Connection conn;
	Session session(&conn, m_buffer);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 0, Exclusived, m_dbObjStats, NULL);
	m_buffer->markDirty(&session, page);

	for (uint i = 0; i < m_numPages / 2; i++) {
		BufferPageHdr *page = buffer2->newPage(&session, file2, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	delete buffer2;
	file2->close();
	file2->remove();
	delete file2;
	cleanUp();
}

void BufferTestCase::testLru() {
	init(Buffer::OLD_GEN_SHIFT * 4, Buffer::OLD_GEN_SHIFT * 4);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->writePage(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	m_buffer->freePages(&session, m_file, false);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	
	// 进行一次非相关性访问，触发新老分代调整。此时老分代过大
	{
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 0, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	// 对所有页面进行非相关性访问，导致老分代过小调整
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	cleanUp();
}

void BufferTestCase::testScan() {
	m_pool = new PagePool(4, Limits::PAGE_SIZE);
	m_numPages = 4;
	m_dbObjStats = new DBObjStats(DBO_Heap);
	m_syslog = new Syslog(NULL, EL_LOG, true, true);
	m_buffer = new Buffer(NULL, m_numPages * 2, m_pool, m_syslog, NULL);
	m_pool->registerUser(m_buffer);
	m_pool->init();

	m_file = new File("buftest");
	m_file->create(true, true);
	m_file->setSize(m_numPages * Limits::PAGE_SIZE);

	File *file2 = new File("buftest2");
	file2->create(true, true);
	file2->setSize(m_numPages * Limits::PAGE_SIZE);

	Connection conn;
	Session session(&conn, m_buffer);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats);
		m_buffer->releasePage(&session, page, Shared);
	}
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, file2, PAGE_HEAP, i, Shared, m_dbObjStats);
		m_buffer->releasePage(&session, page, Shared);
	}

	uint n = 0;
	BufScanHandle *h = m_buffer->beginScan(session.getId(), NULL);
	while (m_buffer->getNext(h))
		n++;
	CPPUNIT_ASSERT(n == m_numPages * 2);
	m_buffer->endScan(h);

	n = 0;
	h = m_buffer->beginScan(session.getId(), file2);
	while (m_buffer->getNext(h))
		n++;
	CPPUNIT_ASSERT(n == m_numPages);
	m_buffer->endScan(h);

	cleanUp();
	file2->close();
	delete file2;
}

class BWTestThread: public Thread {
public:
	BWTestThread(Buffer *buffer, File *file, PageType pageType): Thread("BWTestThread") {
		m_buffer = buffer;
		m_file = file;
		m_pageType = pageType;
		m_beforeBatchWrite = false;
	}

	virtual void run() {
		Connection conn;
		Session session(&conn, m_buffer);
		DBObjStats * stats = new DBObjStats(DBO_Heap);
		byte validValue = 73;
		uint numPages = m_buffer->getTargetSize();

		for (uint i = 0; i < numPages; i++) {
			BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, stats);
			memset(page, validValue, Limits::PAGE_SIZE);
			m_buffer->markDirty(&session, page);
			m_buffer->releasePage(&session, page, Exclusived);
		}

		m_beforeBatchWrite = true;
		m_evt.wait(-1);
		m_buffer->batchWrite(&session, m_file, PAGE_HEAP, 0, m_buffer->getTargetSize() - 1);

		byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * numPages);
		u64 code = m_file->read(0, Limits::PAGE_SIZE * numPages, buf);
		CPPUNIT_ASSERT(code == File::E_NO_ERROR);
		for (uint i = 0; i < numPages; i++) {
			for (size_t j = sizeof(BufferPageHdr); j < Limits::PAGE_SIZE; j++)
				if ((buf + Limits::PAGE_SIZE * i)[j] != validValue)
					CPPUNIT_FAIL("Error");
		}

		System::virtualFree(buf);
		delete stats;
	}

public:
	Buffer	*m_buffer;
	File	*m_file;
	PageType	m_pageType;
	Event	m_evt;
	bool	m_beforeBatchWrite;
};

void BufferTestCase::testBatchWrite() {
	init(512, 512);

	byte validValue = 67;
	// 顺利的加上锁
	Connection conn;
	Session session(&conn, m_buffer);
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, validValue, m_pool->getPageSize());
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	m_buffer->batchWrite(&session, m_file, PAGE_HEAP, 0, m_numPages - 1);
	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * m_numPages);
	u64 code = m_file->read(0, Limits::PAGE_SIZE * m_numPages, buf);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);
	for (uint i = 0; i < m_numPages; i++) {
		for (size_t j = sizeof(BufferPageHdr); j < Limits::PAGE_SIZE; j++)
			if ((buf + Limits::PAGE_SIZE * i)[j] != validValue)
				CPPUNIT_FAIL("Error");
	}
	System::virtualFree(buf);

	// 加不上锁
	BWTestThread testThread(m_buffer, m_file, PAGE_HEAP);
	testThread.enableSyncPoint(SP_BUF_BATCH_WRITE_LOCK_FAIL);

	testThread.enableSyncPoint(SP_BUF_BATCH_WRITE_LOCK_FAIL);
	testThread.start();
	while (!testThread.m_beforeBatchWrite)
		Thread::msleep(10);

	BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, 23, Exclusived, m_dbObjStats);
	
	testThread.m_evt.signal();

	testThread.joinSyncPoint(SP_BUF_BATCH_WRITE_LOCK_FAIL);
	m_buffer->releasePage(&session, page, Exclusived);

	testThread.disableSyncPoint(SP_BUF_BATCH_WRITE_LOCK_FAIL);
	testThread.notifySyncPoint(SP_BUF_BATCH_WRITE_LOCK_FAIL);

	testThread.join();

	cleanUp();
}

const char* BufferBigTest::getName() {
	return "Buffer performance test";
}

const char* BufferBigTest::getDescription() {
	return "Performance test for page buffer.";
}

bool BufferBigTest::isBig() {
	return true;
}

void BufferBigTest::tearDown() {
	File f("buftest");
	f.remove();
	File f2("test2");
	f2.remove();
}

void BufferBigTest::setUp() {
	File f("buftest");
	f.remove();
	File f2("test2");
	f2.remove();
}

void BufferBigTest::init(uint bufferSize, uint numPages) {
	m_pool = new PagePool(4, Limits::PAGE_SIZE);
	m_bufferSize = bufferSize;
	m_numPages = numPages;
	m_syslog = new Syslog(NULL, EL_DEBUG, true, true);
	m_buffer = new Buffer(NULL, bufferSize, m_pool, m_syslog, NULL);
	m_dbObjStats = new DBObjStats(DBO_Heap);
	m_pool->registerUser(m_buffer);
	m_pool->init();

	m_file = new File("buftest");
	m_file->create(true, false);
	m_file->setSize(numPages * Limits::PAGE_SIZE);
}

void BufferBigTest::cleanUp() {
	m_file->close();
	m_file->remove();
	delete m_file;
	delete m_buffer;
	delete m_pool;
	delete m_syslog;
	delete m_dbObjStats;
}

/** 
 * 性能日志
 * 2008/09/12之前
 *   get/release: 256 cc
 *   get(guess)/release: 241 cc
 *   lock/unlock: 96 cc
 * 2008/09/12
 *   lockPage指定touch参数
 *   lock/unlock: 81 cc
 * 2008/09/12
 *   内联pageToDesc
 *   get/release: 266 cc
 *   get(guess)/release: 238 cc
 *   lock/unlock: 72 cc
 * 2008/09/12
 *   内联PagePool::lockPage/trylockPage/unlockPage/getInfo
 *   get/release: 236 cc
 *   get(guess)/release: 215 cc
 *   lock/unlock: 63 cc
 * 2008/09/16
 *   内联Buffer::lockPage/unlockPage
 *   get/release: 245 cc
 *   get(guess)/release: 205 cc
 *   lock/unlock: 51 cc
 */
void BufferBigTest::testBasicOperations() {
	init(32, 32);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->writePage(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}

	uint loopCount = 100000;
	// 测试get/release
	cout << "Test performance of getPage/releasePage" << endl;
	u64 before = System::clockCycles();
	for (uint loop = 0; loop < loopCount; loop++) {
		for (uint i = 0; i < m_numPages; i++) {
			BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats, NULL);
			m_buffer->releasePage(&session, page, Shared);
		}
	}
	u64 after = System::clockCycles();

	cout << "  Clock cycles per getPage/releasePage: " << (after - before) / loopCount / m_numPages << endl;

	// 测试指定guess的get/release
	cout << "Test performance of getPage(with guess)/releasePage" << endl;
	BufferPageHdr **pages = (BufferPageHdr **)malloc(sizeof(BufferPageHdr *) * m_numPages);
	for (uint i = 0; i < m_numPages; i++) {
		pages[i] = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats, NULL);
	}

	before = System::clockCycles();
	for (uint loop = 0; loop < loopCount; loop++) {
		for (uint i = 0; i < m_numPages; i++) {
			BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Shared, m_dbObjStats, pages[i]);
			m_buffer->releasePage(&session, page, Shared);
		}
	}
	after = System::clockCycles();

	cout << "  Clock cycles per getPage/releasePage: " << (after - before) / loopCount / m_numPages << endl;

	// 测试lock/unlock
	cout << "Test performance of lockPage(no touch)/unlockPage" << endl;
	before = System::clockCycles();
	for (uint loop = 0; loop < loopCount; loop++) {
		for (uint i = 0; i < m_numPages; i++) {
			m_buffer->lockPage(session.getId(), pages[i], Shared, false);
			m_buffer->unlockPage(session.getId(), pages[i], Shared);
		}
	}
	after = System::clockCycles();

	cout << "  Clock cycles per lockPage/unlockPage: " << (after - before) / loopCount / m_numPages << endl;

	cleanUp();
}

/** 概率分布 */
struct Distribution {
	uint	*m_source;		/** 数据来源所占比例，加起来等于100 */
	uint	*m_freq;		/** 访问量所占比例，加起来等于100
							 * 示例：若m_source为{10, 90}，m_freq为{80,20}
							 * 则表示10%的页面占了80%的访问，剩下90%的页面只占了20%的访问
							 */
	uint	m_size;			/** 上述两个数组的大小 */

	Distribution(uint size, uint *source, uint *freq) {
		uint sum = 0;
		for (uint i = 0; i < size; i++) {
			assert(source[i] > 0 && source[i] <= 100);
			sum += source[i];
		}
		assert(sum == 100);
		sum = 0;
		for (uint i = 0; i < size; i++) {
			assert(freq[i] > 0 && freq[i] <= 100);
			sum += freq[i];
		}
		assert(sum == 100);
		
		m_size = size;
		m_source = source;
		m_freq = freq;
	}
};

class BufferMTTest: public Thread {
public:
	BufferMTTest(File *file, Buffer *buffer, uint loopCount, PageId *pageIds, uint numPages, const Distribution *distr, uint writePecent): Thread("BufferMTTest") {
		m_file = file;
		m_buffer = buffer;
		m_loopCount = loopCount;
		m_distr = distr;
		m_writePecent = writePecent;
		m_pageIds = pageIds;
		m_numPages = numPages;
		m_dbObjStats = new DBObjStats(DBO_Heap);
	}

	void run() {
		System::srandom(getId());
		Connection conn;
		Session session(&conn, m_buffer);
		for (uint i = 0; i < m_loopCount; i++) {
			u64 pageId;
			uint v = ((uint)System::random()) % 100;
			uint sumFreq = 0;
			uint targetFreq = m_distr->m_size - 1;
			for (uint i = 0; i < m_distr->m_size - 1; i++) {
				if (sumFreq + m_distr->m_freq[i] > v) {
					targetFreq = i;
					break;
				}
				sumFreq += m_distr->m_freq[i];
			}
			uint startSource = 0, endSource = 0;
			for (uint i = 0; i < targetFreq; i++) {
				startSource += m_distr->m_source[i];
			}
			endSource = startSource + m_distr->m_source[targetFreq];

			size_t startIdx = (m_numPages * startSource) / 100;
			size_t endIdx = (m_numPages * endSource) / 100;
			size_t idx = startIdx + (((uint)System::random()) % (endIdx - startIdx));
			assert(idx >= startIdx && idx < endIdx);
			assert(idx < m_numPages);

			pageId = m_pageIds[idx];
			
			LockMode lockMode;
			if (((uint)System::random()) % 100 >= m_writePecent)
				lockMode = Shared;
			else
				lockMode = Exclusived;
			BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, pageId, lockMode, m_dbObjStats, NULL);
			// 模拟一些CPU负载
			uint n = 0;
			for (uint j = 0; j < 200; j++) {
				n += j;
				n = n % m_numPages;
			}
			if (n > m_numPages * 2)		// 不可能为真，目的是防止编译器将上述一段代码优化掉
				break;
			if (lockMode == Exclusived)
				m_buffer->markDirty(&session, page);
			m_buffer->releasePage(&session, page, lockMode);
		}
	}

private:
	File	*m_file;
	Buffer	*m_buffer;
	DBObjStats *m_dbObjStats;
	uint	m_loopCount;
	PageId	*m_pageIds;
	uint	m_numPages;
	const Distribution	*m_distr;
	uint	m_writePecent;
};

class CheckpointThread: public Task {
public:
	CheckpointThread(Buffer *buffer, uint interval): Task("CheckpointThread", interval) {
		m_buffer = buffer;
	}

	void run() {
		m_buffer->flushAll(NULL);
	}

private:
	Buffer	*m_buffer;
};

class ProgressReporter: public Task {
public:
	ProgressReporter(Buffer *buffer, uint interval): Task("ProgressReporter", interval) {
		m_buffer = buffer;
		m_logicalReads = m_buffer->getStatus().m_logicalReads;
		m_physicalReads = m_buffer->getStatus().m_physicalReads;
		m_flushWrites = m_buffer->getStatus().m_flushWrites;
		m_scavengerWrites = m_buffer->getStatus().m_scavengerWrites;
		m_reportTime = System::currentTimeMillis();
	}

	void run() {
		u64 logicalReads = m_buffer->getStatus().m_logicalReads;
		u64 physicalReads = m_buffer->getStatus().m_physicalReads;
		u64 flushWrites = m_buffer->getStatus().m_flushWrites;
		u64 scavengerWrites = m_buffer->getStatus().m_scavengerWrites;
		u64 now = System::currentTimeMillis();

		double readsPerSecond = (double)(physicalReads - m_physicalReads) * 1000 / (now - m_reportTime);
		double flushWritesPerSecond = (double)(flushWrites - m_flushWrites) * 1000 / (now - m_reportTime);
		double scavengerWritesPerSecond = (double)(scavengerWrites - m_scavengerWrites) * 1000 / (now - m_reportTime);
		cout << "  reads: " << readsPerSecond;
		cout << "/s, flush writes: " << flushWritesPerSecond;
		cout << "/s, scavenger writes: " << scavengerWritesPerSecond;
		cout << "/s, miss rate: " << (physicalReads - m_physicalReads) * 1000 / (logicalReads - m_logicalReads) << "/1000";
		cout << endl;
		m_buffer->updateExtendStatus();
		m_buffer->printStatus(cout);

		m_logicalReads = logicalReads;
		m_physicalReads = physicalReads;
		m_flushWrites = flushWrites;
		m_scavengerWrites = scavengerWrites;
		m_reportTime = now;
	}

private:
	Buffer	*m_buffer;
	u64		m_logicalReads;
	u64		m_physicalReads;
	u64		m_flushWrites;
	u64		m_scavengerWrites;
	u64		m_reportTime;
};

/**
 * 测试替换算法的效率。访问请求具有以下特点: 所有页面被分为三个等级，
 * 一等热门页占2%，二等普通页占8%，三等冷门页占90%。
 * 每次访问98%的情况下访问一等或二等页面，其中80%访问一等热门页，
 * 其余访问冷门页。
 * 一等与二等页面数之和恰好为缓存大小，因此理想的情况下缓存失配率应为2%。
 * 读写比例为9:1，与访问的页面类型无关。
 * 另外每分钟进行一次检查点。
 *
 * 性能日志:
 * 2008/07/23:
 *   老分代比例为30%，相关性访问为老分代50%个页面
 *   平稳运行后失配率为4.2%左右
 */
void BufferBigTest::testReplacePolicy() {
	cout << "Test replace policy" << endl;
	init(17000, 163840);
	m_syslog->setErrLevel(EL_LOG);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->writePage(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	u64 baseLogicalReads = m_buffer->getStatus().m_logicalReads;

	uint source[3] = {2, 8, 90};
	uint freq[3] = {80, 18, 2};
	Distribution distr(3, source, freq);
	uint loopCount = 1000000;
	PageId *pageIds = new PageId[m_numPages];
	for (uint i = 0; i < m_numPages; i++)
		pageIds[i] = i;
	BufferMTTest *testThread = new BufferMTTest(m_file, m_buffer, loopCount, pageIds, m_numPages, &distr, 0);
	CheckpointThread *checkpointer = new CheckpointThread(m_buffer, 600 * 1000);
	ProgressReporter *progressReporter = new ProgressReporter(m_buffer, 30000);

	u32 before = System::fastTime();
	checkpointer->start();
	progressReporter->start();
	testThread->start();

	testThread->join();
	checkpointer->stop();
	checkpointer->join();
	progressReporter->stop();
	progressReporter->join();
	u32 after = System::fastTime();
	
	cout << "  Time: " << (after - before) << " seconds" << endl;
	cout << "  Buffer miss rate: " << m_buffer->getStatus().m_physicalReads * 1000 / (m_buffer->getStatus().m_logicalReads - baseLogicalReads) << "/1000" << endl;
	cout << "  Throughput: " << loopCount / (after - before) << " ops/s" << endl;

	delete testThread;
	delete checkpointer;
	delete progressReporter;
	delete []pageIds;
	cleanUp();
}

/** 测试顺序扫描性能
 * 2008/07/28
 *   吞吐率: 110M/s
 */
void BufferBigTest::testScanST() {
	cout << "Test performance of buffer scan" << endl;
	init(1000, 500000);
	m_syslog->setErrLevel(EL_LOG);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化数据
	cout << " writing pages" << endl;
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		if ((i % 10000) == 0)
			cout << "  " << i << "/" << m_numPages << endl;
		m_buffer->writePage(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	cout << endl;
	m_buffer->freePages(&session, m_file, false);

	cout << "  reading pages" << endl;
	int sum = 0;
	u32 before = System::fastTime();
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->getPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats, NULL);
		for (int n = 0; n < 2000; n++) {
			sum += n;
			sum = sum % 10029;
		}
		m_buffer->releasePage(&session, page, Exclusived);
		if ((i % 10000) == 0)
			cout << "  " << i << "/" << m_numPages << endl;
	}
	u64 after = System::fastTime();

	cout << "  Sum: " << sum << endl;
	cout << "  Time: " << after - before << " seconds" << endl;
	cout << "  Throughput: " << m_numPages * Limits::PAGE_SIZE / 1024 / (after - before) << " KB/s" << endl;
	m_buffer->updateExtendStatus();
	m_buffer->printStatus(cout);

	cleanUp();
}

/**
 * 综合性多线程测试。
 */
void BufferBigTest::testMT() {
	cout << "Test buffer with multiple threads" << endl;
	init(2000, 20000);
	m_syslog->setErrLevel(EL_LOG);

	Connection conn;
	Session session(&conn, m_buffer);
	// 初始化数据
	for (uint i = 0; i < m_numPages; i++) {
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		memset(page, 0, m_pool->getPageSize());
		m_buffer->writePage(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
	}
	m_file->close();
	m_file->open(false);

	uint source[2] = {10, 90};
	uint freq[2] = {90, 10};
	Distribution distr(2, source, freq);
	PageId *pageIds = new PageId[m_numPages];
	for (uint i = 0; i < m_numPages; i++)
		pageIds[i] = i;
	// 把次序打乱，防止热门页面集中在一起使硬盘缓存带来干扰
	for (uint n = 0; n < m_numPages * 20; n++) {
		int i = System::random() % m_numPages;
		int j = System::random() % m_numPages;
		PageId id = pageIds[i];
		pageIds[i] = pageIds[j];
		pageIds[j] = id;
	}

	uint loopCount = 1000000;

	BufferMTTest *threads[100];
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i] = new BufferMTTest(m_file, m_buffer, loopCount, pageIds, m_numPages, &distr, 0);
	CheckpointThread *checkpointer = new CheckpointThread(m_buffer, 600 * 1000);
	ProgressReporter *progressReporter = new ProgressReporter(m_buffer, 30000);

	u64 before = System::currentTimeMillis();
	checkpointer->start();
	progressReporter->start();
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->start();

	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++)
		threads[i]->join();
	checkpointer->stop();
	checkpointer->join();
	progressReporter->stop();
	progressReporter->join();
	u64 after = System::currentTimeMillis();
	
	for (size_t i = 0; i < sizeof(threads) / sizeof(threads[0]); i++) 
		delete threads[i];
	
	m_buffer->updateExtendStatus();
	m_buffer->printStatus(cout);

	delete checkpointer;
	delete progressReporter;
	delete []pageIds;
	cleanUp();
}

/**
 * 模拟装载数据（大量调用newPage）时的性能
 */
void BufferBigTest::testLoadData() {
	uint numPages, bufferSize, fileSize;
	bufferSize = 32 * 1024 * 1024 / Limits::PAGE_SIZE;	// 32M
	numPages = 512 * 1024 * 1024 / Limits::PAGE_SIZE;	// 512M
	fileSize = 32;
	init(bufferSize, fileSize);
	m_syslog->setErrLevel(EL_LOG);

	Connection conn;
	Session session(&conn, m_buffer);
	u64 before = System::currentTimeMillis();
	for (uint i = 0; i < numPages; i++) {
		if (i == fileSize) {
			fileSize += 32;
			m_file->setSize(fileSize * Limits::PAGE_SIZE);
		}
		BufferPageHdr *page = m_buffer->newPage(&session, m_file, PAGE_HEAP, i, Exclusived, m_dbObjStats);
		m_buffer->markDirty(&session, page);
		m_buffer->releasePage(&session, page, Exclusived);
		if ((i % 10000) == 0) {
			u64 after = System::currentTimeMillis();
			double pagesPerSec = (10000.0 * 1000 / (after - before));
			double mbPerSec = (pagesPerSec * Limits::PAGE_SIZE) / 1000000;
			cout << pagesPerSec << " pages, " << mbPerSec << "mb per second" << endl;
			before = after;
		}
	}

	cout << m_buffer->m_prefetchSize;
	m_buffer->updateExtendStatus();
	m_buffer->printStatus(cout);
}
