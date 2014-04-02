/**
 * 测试内存索引操作
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#include "btree/TestMIndexOperation.h"
#include <vector>

const static uint DFL_TNTIM_POOL_SIZE = 16;
const static uint DFL_TNTIM_ADDITIONAL_SIZE = 24;
const static u16 DFL_USER_ID = 0;
const static u16 DFL_INDEX_ID = 0;

const char* MIndexPageTestCase::getName() {
	return "Memory Index Page Operation Test";
}
const char* MIndexPageTestCase::getDescription() {
	return "Test Memory index page operation";
}

bool MIndexPageTestCase::isBig() {
	return false;
}

void MIndexPageTestCase::setUp() {
	m_keyHelper = new MIndexKeyHelper();
	m_keyHelper->init();

	m_mtx = new MemoryContext(Limits::PAGE_SIZE, 1);

	m_pool = new PagePool(1, Limits::PAGE_SIZE);
	m_pageManager = new TNTIMPageManager(DFL_TNTIM_POOL_SIZE, DFL_TNTIM_ADDITIONAL_SIZE, m_pool);
	m_pool->registerUser(m_pageManager);
	m_pool->init();
	m_currentPage = (MIndexPageHdl)m_pageManager->getPage(DFL_USER_ID, PAGE_MEM_INDEX, false);
}

void MIndexPageTestCase::tearDown() {
	delete m_pageManager;
	m_pageManager = NULL;

	delete m_pool;
	m_pool = NULL;

	m_mtx->reset();
	delete m_mtx;
	m_mtx = NULL;

	delete m_keyHelper;
	m_keyHelper = NULL;
}

void MIndexPageTestCase::testCommon() {
	m_currentPage->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), ROOT_AND_LEAF);
	CPPUNIT_ASSERT(m_currentPage->isPageLeaf());
	CPPUNIT_ASSERT(m_currentPage->isPageRoot());
	CPPUNIT_ASSERT(0 == m_currentPage->getKeyCount());
	CPPUNIT_ASSERT(MIDX_PAGE_NONE == m_currentPage->getPrevPage());
	CPPUNIT_ASSERT(MIDX_PAGE_NONE == m_currentPage->getNextPage());
}

void MIndexPageTestCase::testAddKeyAndFindKey() {
	m_currentPage->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), LEAF_PAGE);

	MemoryContext mtx(Limits::PAGE_SIZE, 1);
	const IndexDef *idxDef = m_keyHelper->getIndexDef();
	KeyComparator comparator(m_keyHelper->getTableDef(), idxDef);
	comparator.setComparator(RecordOper::compareKeyNN);

	//插入一个无限大键值并设置为high-key
	SubRecord *infiniteKey = MIndexKeyOper::createInfiniteKey(&mtx, idxDef);
	m_currentPage->appendIndexKey(infiniteKey);
	m_currentPage->setLeafPageHighKey();

	SubRecord tmpSr(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	IndexInsertResult result = INSERT_SUCCESS;
	RowId rowId = 0;
	//u64 idValue = 0;
	std::vector<SubRecord*> keyCollection;

	u16 keyCount = 1;
	while (true) {
		CPPUNIT_ASSERT(INSERT_SUCCESS == result);
		CPPUNIT_ASSERT(keyCount == m_currentPage->getKeyCount());
		SubRecord * key = m_keyHelper->createKey(m_mtx, rowId++, (u64)System::random() % 65536);
		result = m_currentPage->addIndexKey(key, &tmpSr, &comparator, MIDX_PAGE_NONE);
		if (result == NEED_SPLIT) {
			break;
		}
		keyCollection.push_back(key);
		
		SubRecord keyInPage(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		MIndexSearchResult sr;
		m_currentPage->findKeyInLeafPage(key, &keyInPage, &SearchFlag::DEFAULT_FLAG, &comparator, &sr);
		NTSE_ASSERT(0 == sr.m_cmpResult);
		m_currentPage->getKeyByLocation(&sr.m_keyLocation, &keyInPage);

		CPPUNIT_ASSERT(key->m_size == keyInPage.m_size);
		CPPUNIT_ASSERT(0 == memcmp(key->m_data, keyInPage.m_data, keyInPage.m_size));
		CPPUNIT_ASSERT(key->m_rowId == keyInPage.m_rowId);
		CPPUNIT_ASSERT(key->m_rowId == MIndexKeyOper::readRowId(&keyInPage));

		++keyCount;
	}
	CPPUNIT_ASSERT(keyCount > 1);
	m_currentPage->verifyPage(m_keyHelper->getTableDef(), m_keyHelper->getIndexDef());

	//测试结点分裂
	MIndexPageHdl newPage = (MIndexPageHdl)m_pageManager->getPage(DFL_USER_ID, PAGE_MEM_INDEX, false);
	newPage->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), m_currentPage->getPageType(), m_currentPage->getPageLevel());
	m_currentPage->split(newPage);

	CPPUNIT_ASSERT(m_pageManager->isPageLatched(m_currentPage, Exclusived));
	CPPUNIT_ASSERT(m_pageManager->isPageLatched(newPage, Exclusived));
	
	m_currentPage->verifyPage(m_keyHelper->getTableDef(), m_keyHelper->getIndexDef());
	newPage->verifyPage(m_keyHelper->getTableDef(), m_keyHelper->getIndexDef());
	
	CPPUNIT_ASSERT(m_currentPage->getKeyCount() + newPage->getKeyCount() == keyCount);
	CPPUNIT_ASSERT(keyCollection.size() == keyCount - 1);

	for (std::vector<SubRecord *>::iterator it = keyCollection.begin(); it != keyCollection.end(); ++it) {
		MIndexSearchResult sr;
		SubRecord keyInPage(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		m_currentPage->findKeyInLeafPage(*it, &keyInPage, &SearchFlag::DEFAULT_FLAG, &comparator, &sr);
		if (0 != sr.m_cmpResult) {
			newPage->findKeyInLeafPage(*it, &keyInPage, &SearchFlag::DEFAULT_FLAG, 
				&comparator, &sr);
			NTSE_ASSERT(0 == sr.m_cmpResult);
			newPage->getKeyByLocation(&sr.m_keyLocation, &keyInPage);
		} else {
			m_currentPage->getKeyByLocation(&sr.m_keyLocation, &keyInPage);
		}
		
		CPPUNIT_ASSERT((*it)->m_size == keyInPage.m_size);
		CPPUNIT_ASSERT(0 == memcmp((*it)->m_data, keyInPage.m_data, keyInPage.m_size));
		CPPUNIT_ASSERT((*it)->m_rowId == keyInPage.m_rowId);
		CPPUNIT_ASSERT((*it)->m_rowId == MIndexKeyOper::readRowId(&keyInPage));
	}

	m_pageManager->unlatchPage(DFL_USER_ID, m_currentPage, Exclusived);
	m_pageManager->unlatchPage(DFL_USER_ID, newPage, Exclusived);
}

void MIndexPageTestCase::testAppendKeyAndDeleteKey() {
	MemoryContext mtx(Limits::PAGE_SIZE, 1);

	const IndexDef *idxDef = m_keyHelper->getIndexDef();
	m_currentPage->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), LEAF_PAGE);

	u16 keyCount = 0;
	IndexInsertResult result = INSERT_SUCCESS;
	std::vector<SubRecord*> keyCollection;
	while (m_currentPage->getFreeSpace() >= MIndexPage::MINDEX_PAGE_SIZE / 4) {
		CPPUNIT_ASSERT(INSERT_SUCCESS == result);
		CPPUNIT_ASSERT(keyCount == m_currentPage->getKeyCount());
		SubRecord * key = m_keyHelper->createNatualKeyByOrder(m_mtx);
		result = m_currentPage->appendIndexKey(key, MIDX_PAGE_NONE);
		assert(result == INSERT_SUCCESS);

		keyCollection.push_back(key);
		
		SubRecord keyInPage(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		m_currentPage->getKeyByNo(keyCount, &keyInPage);
		CPPUNIT_ASSERT(key->m_size == keyInPage.m_size);
		CPPUNIT_ASSERT(0 == memcmp(key->m_data, keyInPage.m_data, keyInPage.m_size));
		CPPUNIT_ASSERT(key->m_rowId == keyInPage.m_rowId);
		CPPUNIT_ASSERT(key->m_rowId == MIndexKeyOper::readRowId(&keyInPage));

		++keyCount;
	}	
	CPPUNIT_ASSERT(keyCount > 1);
	m_currentPage->setLeafPageHighKey();
	m_currentPage->verifyPage(m_keyHelper->getTableDef(), m_keyHelper->getIndexDef());

	KeyComparator comparator(m_keyHelper->getTableDef(), idxDef);
	comparator.setComparator(RecordOper::compareKeyNN);

	for (std::vector<SubRecord *>::iterator it = keyCollection.begin(); it != keyCollection.end(); ++it) {
		SubRecord *key = *it;

		SubRecord keyInPage(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		MIndexSearchResult sr;
		m_currentPage->findKeyInLeafPage(key, &keyInPage, &SearchFlag::DEFAULT_FLAG, &comparator, &sr);
		NTSE_ASSERT(0 == sr.m_cmpResult);
		m_currentPage->getKeyByLocation(&sr.m_keyLocation, &keyInPage);

		SubRecord tmpKey(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		NTSE_ASSERT(m_currentPage->deleteIndexKey(key, &tmpKey, &comparator));

		if (m_currentPage->getKeyCount() > 0) {
			m_currentPage->findKeyInLeafPage(key, &keyInPage, &SearchFlag::DEFAULT_FLAG, &comparator, &sr);
			NTSE_ASSERT(sr.m_cmpResult < 0);
		}
	}

	CPPUNIT_ASSERT(0 == m_currentPage->getKeyCount());
}

void MIndexPageTestCase::testRedistribute() {
	const TableDef *tableDef = m_keyHelper->getTableDef();
	const IndexDef *idxDef = m_keyHelper->getIndexDef();

	{
		//测试从右边页面移部分键值到左边页面
		MemoryContext mtx(Limits::PAGE_SIZE, 1);

		MIndexPageHdl page1 = generatePageWithKeys(MIndexPage::MINDEX_PAGE_SIZE / 3);
		MIndexPageHdl page2 = generatePageWithKeys(MIndexPage::MINDEX_PAGE_SIZE / 4);
		page1->setMaxTrxId(5);
		page2->setMaxTrxId(4);

		NTSE_ASSERT(page1->getKeyDataTotalLen() < page2->getKeyDataTotalLen());

		const u16 keyCount1 = page1->getKeyCount();
		const u16 keyCount2 = page2->getKeyCount();

		SubRecord tmpKey(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		SubRecord *highKey1 = MIndexKeyOper::allocSubRecord(&mtx, idxDef, KEY_NATURAL);
		SubRecord *highKey2 = MIndexKeyOper::allocSubRecord(&mtx, idxDef, KEY_NATURAL);

		page2->getHighKey(&tmpKey);
		MIndexKeyOper::copyKey(highKey2, &tmpKey);
		
		//测试重新分布两个页面的键值
		page1->redistribute(page2);

		CPPUNIT_ASSERT(page1->getKeyCount() + page2->getKeyCount() == keyCount1 + keyCount2);

		//右页面的high-key应该不变
		page2->getHighKey(&tmpKey);
		CPPUNIT_ASSERT(tmpKey.m_size == highKey2->m_size);
		CPPUNIT_ASSERT(0 == memcmp(tmpKey.m_data, highKey2->m_data, tmpKey.m_size));
		CPPUNIT_ASSERT(tmpKey.m_rowId == highKey2->m_rowId);

		//左页面的high-key等于页面的最大键值
		page1->getHighKey(&tmpKey);
		MIndexKeyOper::copyKey(highKey1, &tmpKey);

		page1->getLastKey(&tmpKey);
		CPPUNIT_ASSERT(tmpKey.m_size == highKey1->m_size);
		CPPUNIT_ASSERT(0 == memcmp(tmpKey.m_data, highKey1->m_data, tmpKey.m_size));
		CPPUNIT_ASSERT(tmpKey.m_rowId == highKey1->m_rowId);
	}
	{
		//测试从左边页面移部分键值到右边页面
		MemoryContext mtx(Limits::PAGE_SIZE, 1);

		MIndexPageHdl page1 = generatePageWithKeys(MIndexPage::MINDEX_PAGE_SIZE / 4);
		MIndexPageHdl page2 = generatePageWithKeys(MIndexPage::MINDEX_PAGE_SIZE / 3);
		page1->setMaxTrxId(4);
		page2->setMaxTrxId(5);

		NTSE_ASSERT(page1->getKeyDataTotalLen() > page2->getKeyDataTotalLen());

		const u16 keyCount1 = page1->getKeyCount();
		const u16 keyCount2 = page2->getKeyCount();

		SubRecord tmpKey(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		SubRecord *highKey1 = MIndexKeyOper::allocSubRecord(&mtx, idxDef, KEY_NATURAL);
		SubRecord *highKey2 = MIndexKeyOper::allocSubRecord(&mtx, idxDef, KEY_NATURAL);

		page2->getHighKey(&tmpKey);
		MIndexKeyOper::copyKey(highKey2, &tmpKey);
		
		//测试重新分布两个页面的键值
		page1->redistribute(page2);

		CPPUNIT_ASSERT(page1->getKeyCount() + page2->getKeyCount() == keyCount1 + keyCount2);
		
		//右页面的high-key应该不变
		page2->getHighKey(&tmpKey);
		CPPUNIT_ASSERT(tmpKey.m_size == highKey2->m_size);
		CPPUNIT_ASSERT(0 == memcmp(tmpKey.m_data, highKey2->m_data, tmpKey.m_size));
		CPPUNIT_ASSERT(tmpKey.m_rowId == highKey2->m_rowId);

		//左页面的high-key等于页面的最大键值
		page1->getHighKey(&tmpKey);
		MIndexKeyOper::copyKey(highKey1, &tmpKey);

		page1->getLastKey(&tmpKey);
		CPPUNIT_ASSERT(tmpKey.m_size == highKey1->m_size);
		CPPUNIT_ASSERT(0 == memcmp(tmpKey.m_data, highKey1->m_data, tmpKey.m_size));
		CPPUNIT_ASSERT(tmpKey.m_rowId == highKey1->m_rowId);
	}
}

MIndexPageHdl MIndexPageTestCase::generatePageWithKeys(u16 reserveFreeSpace) {
	const TableDef *tableDef = m_keyHelper->getTableDef();
	const IndexDef *idxDef = m_keyHelper->getIndexDef();
	KeyComparator comparator(m_keyHelper->getTableDef(), idxDef);
	comparator.setComparator(RecordOper::compareKeyNN);

	MIndexPageHdl page = (MIndexPageHdl)m_pageManager->getPage(DFL_USER_ID, PAGE_MEM_INDEX, false);
	page->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), LEAF_PAGE);
	SubRecord tmpSr(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	IndexInsertResult result = INSERT_SUCCESS;
	while (true) {
		CPPUNIT_ASSERT(INSERT_SUCCESS == result);
		SubRecord * key = m_keyHelper->createNatualKeyByOrder(m_mtx);
		result = page->addIndexKey(key, &tmpSr, &comparator, MIDX_PAGE_NONE);
		assert(result == INSERT_SUCCESS);
		if (page->getFreeSpace() < reserveFreeSpace) {
			break;
		}
	}
	page->setLeafPageHighKey();
	page->verifyPage(tableDef, idxDef);
	return page;
}

void MIndexPageTestCase::testSplitAndMerge() {
	m_currentPage->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), LEAF_PAGE);

	const TableDef *tableDef = m_keyHelper->getTableDef();
	const IndexDef *idxDef = m_keyHelper->getIndexDef();

	MemoryContext mtx(Limits::PAGE_SIZE, 1);

	//插入一个无限大键值并设置为high-key
	SubRecord *infiniteKey = MIndexKeyOper::createInfiniteKey(&mtx, idxDef);
	m_currentPage->appendIndexKey(infiniteKey);
	m_currentPage->setLeafPageHighKey();

	u16 keyCount = 1;	
	KeyComparator comparator(m_keyHelper->getTableDef(), idxDef);
	comparator.setComparator(RecordOper::compareKeyNN);

	SubRecord tmpSr(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	IndexInsertResult result = INSERT_SUCCESS;
	while (true) {
		CPPUNIT_ASSERT(INSERT_SUCCESS == result);
		CPPUNIT_ASSERT(keyCount == m_currentPage->getKeyCount());
		SubRecord * key = m_keyHelper->createNatualKeyByOrder(m_mtx);
		result = m_currentPage->addIndexKey(key, &tmpSr, &comparator, MIDX_PAGE_NONE);
		if (result == NEED_SPLIT) {
			break;
		}
		++keyCount;
	}
	m_currentPage->verifyPage(tableDef, idxDef);

	const u16 totalKeyCount = m_currentPage->getKeyCount();

	SubRecord *highKey = MIndexKeyOper::allocSubRecord(&mtx, idxDef, KEY_NATURAL);

	SubRecord tmpKey(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	MIndexPageHdl newPage = (MIndexPageHdl)m_pageManager->getPage(DFL_USER_ID, PAGE_MEM_INDEX, false);
	newPage->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), m_currentPage->getPageType(), 
		m_currentPage->getPageLevel());
	
	//原页面的high-key分裂之后变成新页面的high-key
	m_currentPage->getHighKey(&tmpKey);
	MIndexKeyOper::copyKey(highKey, &tmpKey);
	
	m_currentPage->split(newPage);

	m_currentPage->verifyPage(tableDef, idxDef);
	newPage->verifyPage(tableDef, idxDef);

	const u16 keyCount1 = m_currentPage->getKeyCount();
	const u16 keyCount2 = newPage->getKeyCount();
	CPPUNIT_ASSERT(keyCount1 + keyCount2 == totalKeyCount);

	newPage->getHighKey(&tmpKey);
	CPPUNIT_ASSERT(tmpKey.m_size == highKey->m_size);
	CPPUNIT_ASSERT(0 == memcmp(tmpKey.m_data, highKey->m_data, tmpKey.m_size));
	CPPUNIT_ASSERT(tmpKey.m_rowId == highKey->m_rowId);

	//原页面的最大键值成为high-key
	m_currentPage->getHighKey(&tmpKey);
	MIndexKeyOper::copyKey(highKey, &tmpKey);
	m_currentPage->getKeyByNo(m_currentPage->getKeyCount() - 1, &tmpKey);

	CPPUNIT_ASSERT(tmpKey.m_size == highKey->m_size);
	CPPUNIT_ASSERT(0 == memcmp(tmpKey.m_data, highKey->m_data, tmpKey.m_size));
	CPPUNIT_ASSERT(tmpKey.m_rowId == highKey->m_rowId);

	//合并两个页面，合并之后，被合并页面的high-key成为新的合并页面的high-key
	newPage->getHighKey(&tmpKey);
	MIndexKeyOper::copyKey(highKey, &tmpKey);

	m_currentPage->merge(newPage);
	m_currentPage->getHighKey(&tmpKey);
	
	CPPUNIT_ASSERT(tmpKey.m_size == highKey->m_size);
	CPPUNIT_ASSERT(0 == memcmp(tmpKey.m_data, highKey->m_data, tmpKey.m_size));
	CPPUNIT_ASSERT(tmpKey.m_rowId == highKey->m_rowId);

	CPPUNIT_ASSERT(m_currentPage->getKeyCount() == totalKeyCount);

	m_currentPage->verifyPage(tableDef, idxDef);
}

/*
void MIndexPageTestCase::testBulkPhyReclaim() {
	m_currentPage->initPage(MIndexPage::formPageMark(DFL_INDEX_ID), LEAF_PAGE);

	u16 keyCount = 0;
	const IndexDef *idxDef = m_keyHelper->getIndexDef();
	KeyComparator comparator(m_keyHelper->getTableDef(), idxDef);
	comparator.setComparator(RecordOper::compareKeyNN);
	SubRecord tmpSr(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
	IndexInsertResult result = INSERT_SUCCESS;
	RowId rowId = 0;
	//u64 idValue = 0;
	std::vector<SubRecord*> keyCollection;

	while (true) {
		CPPUNIT_ASSERT(INSERT_SUCCESS == result);
		CPPUNIT_ASSERT(keyCount == m_currentPage->getKeyCount());
		SubRecord * key = m_keyHelper->createKey(m_mtx, rowId++, (u64)System::random() % 65536);
		result = m_currentPage->addIndexKey(key, &tmpSr, &comparator, MIDX_PAGE_NONE);
		if (result == NEED_SPLIT) {
			break;
		}
		keyCollection.push_back(key);
		
		SubRecord keyInPage(KEY_NATURAL, idxDef->m_numCols, idxDef->m_columns, NULL, 0);
		MIndexSearchResult sr;
		m_currentPage->findKeyInLeafPage(key, &keyInPage, &SearchFlag::DEFAULT_FLAG, &comparator, &sr);
		NTSE_ASSERT(0 == sr.m_cmpResult);
		m_currentPage->getKeyByLocation(&sr.m_keyLocation, &keyInPage);

		CPPUNIT_ASSERT(key->m_size == keyInPage.m_size);
		CPPUNIT_ASSERT(0 == memcmp(key->m_data, keyInPage.m_data, keyInPage.m_size));
		CPPUNIT_ASSERT(key->m_rowId == keyInPage.m_rowId);
		CPPUNIT_ASSERT(key->m_rowId == MIndexKeyOper::readRowId(&keyInPage));

		++keyCount;
	}
	CPPUNIT_ASSERT(keyCount > 1);
	m_currentPage->verifyPage(m_keyHelper->getTableDef(), m_keyHelper->getIndexDef());

	u64 timeStamp = m_currentPage->getTimeStamp();
	//m_currentPage->bulkPhyReclaim((u64)-1, &tmpSr);

	CPPUNIT_ASSERT(timeStamp != m_currentPage->getTimeStamp());
	CPPUNIT_ASSERT(0 == m_currentPage->getKeyCount());
	m_currentPage->verifyPage(m_keyHelper->getTableDef(), m_keyHelper->getIndexDef());
}
*/
