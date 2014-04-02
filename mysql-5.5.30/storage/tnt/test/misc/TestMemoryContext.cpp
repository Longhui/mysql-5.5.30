#include "misc/TestMemoryContext.h"

const char* MemoryContextTestCase::getName() {
	return "MemoryContext Test";
}

const char* MemoryContextTestCase::getDescription() {
	return "Test fast memory context and debug memory context";
}

bool MemoryContextTestCase::isBig() {
	return false;
}

#ifndef NTSE_MEM_CHECK
void MemoryContextTestCase::testMemoryContext() {
	MemoryContext *mc = new MemoryContext(4096, 1);

	u64 firstSavepoint = mc->setSavepoint();

	int chunkSize = 700;
	void *first = mc->alloc(chunkSize);
	memset(first, 0, chunkSize);	// 便于valgrind检测有无越界写问题
	for (int i = 0; i < 12; i++) {
		void *chunk = mc->alloc(chunkSize);
		memset(chunk, 0, chunkSize);
	}
	u64 savepoint = mc->setSavepoint();
	void *firstAfterSavepoint = mc->alloc(chunkSize);
	for (int i = 0; i < 12; i++) {
		void *chunk = mc->alloc(chunkSize);
		memset(chunk, 0, chunkSize);
	}
	// 分配超出页面大小的块
	for (int i = 0; i < 3; i++)
		mc->alloc(8000);
	mc->resetToSavepoint(savepoint);
	CPPUNIT_ASSERT(mc->setSavepoint() == savepoint);
	CPPUNIT_ASSERT(mc->alloc(chunkSize) == firstAfterSavepoint);

	mc->reset();
	CPPUNIT_ASSERT(mc->setSavepoint() == firstSavepoint);
	CPPUNIT_ASSERT(mc->alloc(chunkSize) == first);	// 第一页始终保持因此地址不变
	mc->resetToSavepoint(0);
	CPPUNIT_ASSERT(mc->setSavepoint() == firstSavepoint);
	CPPUNIT_ASSERT(mc->alloc(chunkSize) == first);

	// 测试getMemUsage
	{
		mc->reset();
		for (int i = 0; i < 12; i++)
			mc->alloc(chunkSize);
		CPPUNIT_ASSERT(mc->getMemUsage() > chunkSize * 12);
		u64 muBak = mc->getMemUsage();
		u64 savepoint = mc->setSavepoint();
		for (int i = 0; i < 12; i++)
			mc->alloc(chunkSize);
		CPPUNIT_ASSERT(mc->getMemUsage() > chunkSize * 24);
		mc->resetToSavepoint(savepoint);
		CPPUNIT_ASSERT(mc->getMemUsage() == muBak);
		mc->reset();
		CPPUNIT_ASSERT(mc->getMemUsage() < muBak);
	}
	delete mc;
}

void MemoryContextTestCase::testMemoryContextUsePool() {
	uint poolSize = 4096;
	uint reservedPages = 1;
	PagePool pool(1, NTSE_PAGE_SIZE);
	CommonMemPool commonMemPool(poolSize, &pool);
	pool.registerUser(&commonMemPool);
	pool.init();

	MemoryContext *mc = new MemoryContext(&commonMemPool, reservedPages);

	u64 firstSavepoint = mc->setSavepoint();

	int chunkSize = 700;
	void *first = mc->alloc(chunkSize);
	memset(first, 0, chunkSize);	// 便于valgrind检测有无越界写问题
	for (int i = 0; i < 12; i++) {
		void *chunk = mc->alloc(chunkSize);
		memset(chunk, 0, chunkSize);
	}
	u64 savepoint = mc->setSavepoint();
	void *firstAfterSavepoint = mc->alloc(chunkSize);
	for (int i = 0; i < 12; i++) {
		void *chunk = mc->alloc(chunkSize);
		memset(chunk, 0, chunkSize);
	}
	// 分配超出页面大小的块
	for (int i = 0; i < 3; i++)
		NTSE_ASSERT(NULL == mc->alloc(NTSE_PAGE_SIZE + 1));
	mc->resetToSavepoint(savepoint);
	CPPUNIT_ASSERT(mc->setSavepoint() == savepoint);
	CPPUNIT_ASSERT(mc->alloc(chunkSize) == firstAfterSavepoint);

	// 测试内存池内存不足
	mc->reset();
	for (int i = 0; i < poolSize; i++) {
		int cs = NTSE_PAGE_SIZE / 2 + 100;
		void *data = mc->alloc(cs);
		NTSE_ASSERT(NULL != data);
		memset(data, 0, cs);
	}

	NTSE_ASSERT(poolSize == commonMemPool.getCurrentSize());
	for (int i = 0; i < 10; i++) {
		int cs = NTSE_PAGE_SIZE / 2 + 100;
		void *data = mc->alloc(cs);
		NTSE_ASSERT(NULL == data);
		NTSE_ASSERT(poolSize == commonMemPool.getCurrentSize());
	}

	mc->reset();
	CPPUNIT_ASSERT(mc->setSavepoint() == firstSavepoint);
	CPPUNIT_ASSERT(mc->alloc(chunkSize) == first);	// 第一页始终保持因此地址不变
	mc->resetToSavepoint(0);
	CPPUNIT_ASSERT(mc->setSavepoint() == firstSavepoint);
	CPPUNIT_ASSERT(mc->alloc(chunkSize) == first);

	// 测试getMemUsage
	{
		mc->reset();
		for (int i = 0; i < 12; i++)
			mc->alloc(chunkSize);
		CPPUNIT_ASSERT(mc->getMemUsage() > chunkSize * 12);
		u64 muBak = mc->getMemUsage();
		u64 savepoint = mc->setSavepoint();
		for (int i = 0; i < 12; i++)
			mc->alloc(chunkSize);
		CPPUNIT_ASSERT(mc->getMemUsage() > chunkSize * 24);
		mc->resetToSavepoint(savepoint);
		CPPUNIT_ASSERT(mc->getMemUsage() == muBak);
		mc->reset();
		CPPUNIT_ASSERT(mc->getMemUsage() < muBak);
	}
	delete mc;
	mc = NULL;

	// 测试预留页面数
	for (uint i = 1; i <= 10; i++) {	
		mc = new MemoryContext(&commonMemPool, i);

		uint lastUsedPages = commonMemPool.getCurrentSize();
		
		for (uint j = 0; j < i; j++) {
			int cs = NTSE_PAGE_SIZE / 2 + 100;
			void *data = mc->alloc(cs);
			memset(data, 0, cs);
		}

		NTSE_ASSERT(lastUsedPages == commonMemPool.getCurrentSize());

		mc->reset();
		delete mc;
		mc = NULL;
	}
}

#else
void MemoryContextTestCase::testMemoryContext() {
	MemoryContext *mc = new MemoryContext(4096, 1);

	CPPUNIT_ASSERT(mc->setSavepoint() == 0);
	int chunkSize = 700;
	for (int i = 0; i < 12; i++) {
		void *chunk = mc->alloc(chunkSize);
		memset(chunk, 0, chunkSize);
	}
	u64 savepoint = mc->setSavepoint();
	CPPUNIT_ASSERT(savepoint == 12);

	for (int i = 0; i < 12; i++) {
		void *chunk = mc->alloc(chunkSize);
		memset(chunk, 0, chunkSize);
	}

	mc->resetToSavepoint(savepoint);
	CPPUNIT_ASSERT(mc->setSavepoint() == savepoint);

	mc->reset();
	CPPUNIT_ASSERT(mc->setSavepoint() == 0);
	mc->alloc(chunkSize);
	mc->resetToSavepoint(0);
	CPPUNIT_ASSERT(mc->setSavepoint() == 0);

	// 测试getMemUsage
	{
		mc->reset();
		for (int i = 0; i < 12; i++)
			mc->alloc(chunkSize);
		CPPUNIT_ASSERT(mc->getMemUsage() == chunkSize * 12);
		u64 muBak = mc->getMemUsage();
		u64 savepoint = mc->setSavepoint();
		for (int i = 0; i < 12; i++)
			mc->alloc(chunkSize);
		CPPUNIT_ASSERT(mc->getMemUsage() == chunkSize * 24);
		mc->resetToSavepoint(savepoint);
		CPPUNIT_ASSERT(mc->getMemUsage() == muBak);
		mc->reset();
		CPPUNIT_ASSERT(mc->getMemUsage() == 0);
	}
	delete mc;
}

void MemoryContextTestCase::testMemoryContextUsePool() {
	// nothing
}
#endif

