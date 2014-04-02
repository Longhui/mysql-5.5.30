/**
 * 测试内存索引接口
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */

#include "btree/TestMIndex.h"
#include "Test.h"

const static u16 DFL_INDEX_NO = 0;
const static u16 DFL_TNTIM_POOL_SIZE = 10240;

const char* MIndexTestCase::getName() {
	return "Memory Index interface Test";
}
const char* MIndexTestCase::getDescription() {
	return "Test Memory index interface";
}

bool MIndexTestCase::isBig() {
	return false;
}

void MIndexTestCase::setUp() {
}

void MIndexTestCase::tearDown() {
}

void MIndexTestCase::init(bool isUniqueIndex/* = false*/) {
	m_keyHelper = new MIndexKeyHelper();
	m_keyHelper->init(isUniqueIndex);

	TNTDatabase::drop(".", ".");
 	m_cfg.setNtseBasedir("testdb");
	m_cfg.setTntBasedir("testdb");
	m_cfg.m_ntseConfig.m_logFileSize = 64 << 20;
	//m_cfg.m_tntLogfileSize = 1024 * 1024 * 64;

	File dir(m_cfg.m_ntseConfig.m_basedir);
	dir.rmdir(true);
	dir.mkdir();

 	EXCPT_OPER(m_db = TNTDatabase::open(&m_cfg, true, 0));
	
	Database *db = m_db->getNtseDb();

	m_conn = db->getConnection(false);
	m_session = db->getSessionManager()->allocSession(getName(), m_conn);

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	trx->trxAssignReadView();
	m_session->setTrans(trx);

	m_tblDef = m_keyHelper->getTableDef();
	m_idxDef = m_keyHelper->getIndexDef();
	
	m_memIndice = new BLinkTreeIndice(m_db, m_session, m_tblDef->m_numIndice, &m_tblDef, NULL, NULL);
	m_memIndice->init(m_session);
	m_memIndex = m_memIndice->getIndex(DFL_INDEX_NO);
}

void MIndexTestCase::initVarcharIndex(bool isUniqueIndex){
	m_keyHelper = new MIndexKeyHelper();
	m_keyHelper->initVarchar(isUniqueIndex);

	TNTDatabase::drop(".", ".");
	m_cfg.setNtseBasedir("testdb");
	m_cfg.setTntBasedir("testdb");
	m_cfg.m_ntseConfig.m_logFileSize = 64 << 20;
	//m_cfg.m_tntLogfileSize = 1024 * 1024 * 64;

	File dir(m_cfg.m_ntseConfig.m_basedir);
	dir.rmdir(true);
	dir.mkdir();

	EXCPT_OPER(m_db = TNTDatabase::open(&m_cfg, true, 0));

	Database *db = m_db->getNtseDb();

	m_conn = db->getConnection(false);
	m_session = db->getSessionManager()->allocSession(getName(), m_conn);

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	trx->trxAssignReadView();
	m_session->setTrans(trx);

	m_tblDef = m_keyHelper->getTableDef();
	m_idxDef = m_keyHelper->getIndexDef();

	m_memIndice = new BLinkTreeIndice(m_db, m_session, m_tblDef->m_numIndice, &m_tblDef, NULL, NULL);
	m_memIndice->init(m_session);
	m_memIndex = m_memIndice->getIndex(DFL_INDEX_NO);
}

void MIndexTestCase::clear() {
	m_memIndice->close(m_session);
	delete m_memIndice;
	m_memIndice = NULL;

	if (m_db != NULL) {
		TNTTransaction *trx = m_session->getTrans();
		m_db->getTransSys()->closeReadViewForMysql(trx);
		trx->commitTrx(CS_NORMAL);
		m_db->getTransSys()->freeTrx(trx);
		m_session->setTrans(NULL);

		Database *db = m_db->getNtseDb();
		db->getSessionManager()->freeSession(m_session);
		db->freeConnection(m_conn);

		m_db->close();
		delete m_db;
		m_db = NULL;

		TNTDatabase::drop(".", ".");
		File dir(m_cfg.m_ntseConfig.m_basedir);
		dir.rmdir(true);
	}
	delete m_keyHelper;
	m_keyHelper = NULL;
}

void MIndexTestCase::testInsertAndUniqueScan() {
	init(true);

	for (uint i = 0; i < 80000; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_PAD);

		m_memIndex->insert(m_session, key, 0);

		if(i % 2 == 0){
			RowIdVersion version = INVALID_VERSION;
			m_memIndex->delByMark(m_session, key, &version); //先删除
			m_memIndex->insert(m_session, key, 1);		//再插入。 这里用于测试插入的时候发现相同的删除项
		}


		//唯一性索引等值查询
		SubRecord *key2;
		if(i % 2 == 0)
			key2 = m_keyHelper->createKey(m_session->getMemoryContext(), INVALID_ROW_ID, i, KEY_PAD);
		else
			key2 =  m_keyHelper->createKey(m_session->getMemoryContext(), INVALID_ROW_ID, i, KEY_NATURAL);

// 		NTSE_ASSERT(m_memIndex->getByUniqueKey(m_session, key2, &id, NULL, NULL));
// 		NTSE_ASSERT(id == key->m_rowId);

		int count = 0;
		RowLockHandle *rlh = NULL;
		MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle*)m_memIndex->beginScan(
			m_session, key2, true, true, Shared, &rlh, TL_S);
		MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
		while (m_memIndex->getNext(scanHdl)) {
			CPPUNIT_ASSERT(scanHdl->getMVInfo().m_delBit == 0);
			scanInfo->moveCursor();
			++count;
		}
		m_memIndex->endScan(scanHdl);
		CPPUNIT_ASSERT(1 == count);
		CPPUNIT_ASSERT(key->m_rowId == scanHdl->getRowId());
	}

	clear();
}

void MIndexTestCase::testRangeScan() {
	init(false);

	MemoryContext *mtx = m_session->getMemoryContext();
	const uint testLoop = 1200;
	vector<RowId> rowIdSequence;
	for (uint i = 1; i <= testLoop; i++) {
		u64 savepoint = mtx->setSavepoint();
		
		SubRecord *key = m_keyHelper->createKey(mtx, i, i, KEY_PAD);
		m_memIndex->insert(m_session, key, 0);
		rowIdSequence.push_back(i);

		SubRecord *key2 = m_keyHelper->createKey(mtx, i + testLoop, i, KEY_PAD);
		m_memIndex->insert(m_session, key2, 0);
		rowIdSequence.push_back(i + testLoop);

		mtx->resetToSavepoint(savepoint);
	}

	//SubToSubExtractor *extracor = SubToSubExtractor::createInst(mtx, m_tblDef, m_idxDef->m_numCols, 
	//	m_idxDef->m_columns, m_idxDef->m_numCols, m_idxDef->m_columns, KEY_NATURAL, 1);
	//SubRecord *verifyKey = MIndexKeyOper::allocSubRecord(mtx, m_idxDef, REC_REDUNDANT);

	//正向包含键值扫描
	for (uint i = 1, j = 1; i <= testLoop;) {
		u64 savepoint = mtx->setSavepoint();

		SubRecord *searchKey = m_keyHelper->createKey(mtx, INVALID_ROW_ID, i, KEY_PAD);
 
		RowLockHandle *rlh = NULL;
		MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle *)m_memIndex->beginScan(
			m_session, searchKey, true, true, Shared, &rlh, TL_S);
		MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
		uint fetchCount = 0;

		while (m_memIndex->getNext(scanHdl)) {
			CPPUNIT_ASSERT_EQUAL(scanHdl->getRowId(), rowIdSequence[2 * (i - 1) + fetchCount]);

			fetchCount++;
			CPPUNIT_ASSERT(2 * (testLoop - i + 1) >= fetchCount);
			CPPUNIT_ASSERT_EQUAL(fetchCount, (uint)scanInfo->m_fetchCount);

 			if (i > testLoop / 10)
 				scanInfo->m_readPage->incrTimeStamp();

			scanInfo->moveCursor();
		}
		NTSE_ASSERT(2 * (testLoop - i + 1) == fetchCount);

		m_memIndex->endScan(scanHdl);

		mtx->resetToSavepoint(savepoint);

		i += ++j;
	}

	//正向不包含键值扫描
	for (uint i = 1, j = 1; i <= testLoop; ) {
		u64 savepoint = mtx->setSavepoint();

		SubRecord *searchKey = m_keyHelper->createKey(mtx, INVALID_ROW_ID, i, KEY_PAD);

		RowLockHandle *rlh = NULL;
		MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle*)m_memIndex->beginScan(
			m_session, searchKey, true, false, Shared, &rlh, TL_S);
		MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
		uint fetchCount = 0;
		while (m_memIndex->getNext(scanHdl)) {
			NTSE_ASSERT(scanHdl->getRowId() == rowIdSequence[2 * i + fetchCount]);

		 	if (i > testLoop / 10)
 				scanInfo->m_readPage->incrTimeStamp();

			fetchCount++;
			NTSE_ASSERT(2 * (testLoop - i) >= fetchCount);
			NTSE_ASSERT(scanInfo->m_fetchCount == fetchCount);

			scanInfo->moveCursor();
		}
		NTSE_ASSERT(2 * (testLoop - i) == fetchCount);

		m_memIndex->endScan(scanHdl);

		mtx->resetToSavepoint(savepoint);

		i += ++j;
	}

	//反向包含键值扫描
	for (int i = testLoop, j = 1; i >= 1; ) {
		u64 savepoint = mtx->setSavepoint();

		SubRecord *searchKey = m_keyHelper->createKey(mtx, INVALID_ROW_ID, i, KEY_PAD);

		RowLockHandle *rlh = NULL;
		MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle*)m_memIndex->beginScan(
			m_session, searchKey, false, true, Shared, &rlh, TL_S);
		MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
		uint fetchCount = 0;
		int checkIndex = 2 * i - 1;
		while (m_memIndex->getNext(scanHdl)) {
			NTSE_ASSERT(scanHdl->getRowId() == rowIdSequence[checkIndex - fetchCount]);

 			if (i > testLoop / 10)
 				scanInfo->m_readPage->incrTimeStamp();

			fetchCount++;
			NTSE_ASSERT(2 * i  >= (int)fetchCount);
			NTSE_ASSERT(scanInfo->m_fetchCount == fetchCount);

			scanInfo->moveCursor();
		}
		NTSE_ASSERT(2 * i == fetchCount);

		m_memIndex->endScan(scanHdl);

		mtx->resetToSavepoint(savepoint);

		i -= ++j;
	}

	//反向不包含键值扫描
	for (int i = testLoop, j = 1; i >= 1;) {
		u64 savepoint = mtx->setSavepoint();

		SubRecord *searchKey = m_keyHelper->createKey(mtx, INVALID_ROW_ID, i, KEY_PAD);

		RowLockHandle *rlh = NULL;
		MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle*)m_memIndex->beginScan(
			m_session, searchKey, false, false, Shared, &rlh, TL_S);
		MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
		uint fetchCount = 0;
		int checkIndex = 2 * (i- 1) - 1;
		while (m_memIndex->getNext(scanHdl)) {
			NTSE_ASSERT(scanHdl->getRowId() == rowIdSequence[checkIndex - fetchCount]);

 			if (i > testLoop / 10)
 				scanInfo->m_readPage->incrTimeStamp();

			fetchCount++;
			NTSE_ASSERT(2 * (i - 1) >= (int)fetchCount);
			NTSE_ASSERT(scanInfo->m_fetchCount == fetchCount);

			scanInfo->moveCursor();
		}
		NTSE_ASSERT(2 * (i - 1)== fetchCount);

		m_memIndex->endScan(scanHdl);

		mtx->resetToSavepoint(savepoint);

		i -= ++j;
	}

	clear();
}

void MIndexTestCase::testMergeOrRedistribute() {
	init(false);
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}
	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);


	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	bltree->unlatchPage(m_session, rootPage, Exclusived);


	//将左节点插分裂
	bltree->latchPage(m_session, leftPage, Exclusived);
	for(uint i = 0; i< 250; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+5000, i, KEY_NATURAL);
		if (NEED_SPLIT == leftPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			newPage = MIDX_PAGE_NONE;
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, leftPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}
	MIndexPageHdl middlePage = newPage;
	assert(middlePage != rightPage && middlePage == leftPage->getNextPage() && middlePage == rightPage->getPrevPage());

	bltree->unlatchPage(m_session, leftPage, Exclusived);
	bltree->unlatchPage(m_session, middlePage, Exclusived);

	//中节点插入部分数据
	bltree->latchPage(m_session, middlePage, Exclusived);
	for(uint i = 0; i< 30; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+5000, i, KEY_NATURAL);
		if (NEED_SPLIT == middlePage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			break;
		}
	}
	bltree->unlatchPage(m_session, middlePage, Exclusived);


	MIndexScanInfo *scanInfo1 = bltree->generateScanInfo(m_session, Exclusived);
	void *data1 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo1->m_searchKey = new (data1)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	bltree->latchPage(m_session, rootPage, Exclusived);
	bltree->latchPage(m_session, leftPage, Exclusived);
	bltree->latchPage(m_session, middlePage, Exclusived);
	bltree->mergeOrRedistribute(scanInfo1, &rootPage, leftPage, middlePage);
	bltree->unlatchPage(m_session, rootPage, Exclusived);
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	
	clear();
}

void MIndexTestCase::testRepairRightMost() {
	init(false);
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);
		
			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);

	
	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);
	
	bltree->unlatchPage(m_session, rootPage, Exclusived);


	bltree->latchPage(m_session, leftPage, Exclusived);
	//左节点删除部分键值变成下溢页面
	for(uint i = 0; i< 30; i++){
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i , i, KEY_NATURAL);
		leftPage->deleteIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	bltree->latchPage(m_session, rightPage, Exclusived);
	//右节点插入40条数据，使左右页面无法merge，引发redistribute
	for(uint i = 0; i<40; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+429, i+429, KEY_NATURAL);
		 rightPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, rightPage, Exclusived);

	//更新模式遍历，触发重分配
	MIndexScanInfo *scanInfo3 = bltree->generateScanInfo(m_session, Exclusived);
	void *data3 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
// 	scanInfo3->m_searchKey = new (data3)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
// 		NULL, 0, INVALID_ROW_ID);
	scanInfo3->m_searchKey = m_keyHelper->createKey(m_session->getMemoryContext(), 100, 100, KEY_NATURAL);
	bltree->updateModeLocateLeafPage(scanInfo3);
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	
	//右节点删除部分键值变成下溢页面
	bltree->latchPage(m_session, rightPage, Exclusived);
	for(uint i = 0; i< 70; i++){
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), 468 - i , 468 - i, KEY_NATURAL);
		rightPage->deleteIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	assert(rightPage->getKeyCount() < 200);
	bltree->unlatchPage(m_session, rightPage, Exclusived);

	
	//将左节点插分裂
	bltree->latchPage(m_session, leftPage, Exclusived);
	for(uint i = 0; i< 250; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+5000, i, KEY_NATURAL);
		if (NEED_SPLIT == leftPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			newPage = MIDX_PAGE_NONE;
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, leftPage, &newPage);
		
			bltree->latchPage(m_session, newPage, Exclusived);
		
			scanInfo->m_currentPage = insertPage;
			break;
		}
	}


	MIndexPageHdl middlePage = newPage;
	assert(middlePage != rightPage && middlePage == leftPage->getNextPage() && middlePage == rightPage->getPrevPage());

	bltree->unlatchPage(m_session, leftPage, Exclusived);
 	bltree->unlatchPage(m_session, middlePage, Exclusived);

	MIndexScanInfo *scanInfo2 = bltree->generateScanInfo(m_session, Exclusived);
	void *data2 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	// 	scanInfo2->m_searchKey = new (data2)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
	// 		NULL, 0, INVALID_ROW_ID);
	scanInfo2->m_searchKey = m_keyHelper->createKey(m_session->getMemoryContext(), 428+5000, 428, KEY_NATURAL);
	MIndexPageHdl page = bltree->updateModeLocateLeafPage(scanInfo2);
	bltree->unlatchPage(m_session, page, Exclusived);

	clear();
}

void MIndexTestCase::testRepairRightMost2() {
	init(false);
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);


	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	bltree->unlatchPage(m_session, rootPage, Exclusived);



	//右节点删除部分键值变成下溢页面
	bltree->latchPage(m_session, rightPage, Exclusived);
	for(uint i = 0; i< 70; i++){
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), 428 - i , 428 - i, KEY_NATURAL);
		rightPage->deleteIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	assert(rightPage->getKeyCount() < 200);
	bltree->unlatchPage(m_session, rightPage, Exclusived);


	//将左节点插分裂
	bltree->latchPage(m_session, leftPage, Exclusived);
	for(uint i = 0; i< 250; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+5000, i, KEY_NATURAL);
		if (NEED_SPLIT == leftPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			newPage = MIDX_PAGE_NONE;
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, leftPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}


	MIndexPageHdl middlePage = newPage;
	assert(middlePage != rightPage && middlePage == leftPage->getNextPage() && middlePage == rightPage->getPrevPage());

	bltree->unlatchPage(m_session, leftPage, Exclusived);
	bltree->unlatchPage(m_session, middlePage, Exclusived);

	MIndexScanInfo *scanInfo2 = bltree->generateScanInfo(m_session, Exclusived);
	void *data2 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	// 	scanInfo2->m_searchKey = new (data2)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
	// 		NULL, 0, INVALID_ROW_ID);
	scanInfo2->m_searchKey = m_keyHelper->createKey(m_session->getMemoryContext(), 428+5000, 428, KEY_NATURAL);
	MIndexPageHdl page = bltree->updateModeLocateLeafPage(scanInfo2);
	bltree->unlatchPage(m_session, page, Exclusived);

	clear();
}


void MIndexTestCase::testRepairNotRightMost() {
	init(false);
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);


	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	bltree->unlatchPage(m_session, rootPage, Exclusived);


	//将左节点插分裂
	bltree->latchPage(m_session, leftPage, Exclusived);
	for(uint i = 0; i< 250; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+5000, i, KEY_NATURAL);
		if (NEED_SPLIT == leftPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			newPage = MIDX_PAGE_NONE;
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, leftPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}
	MIndexPageHdl middlePage = newPage;
	assert(middlePage != rightPage && middlePage == leftPage->getNextPage() && middlePage == rightPage->getPrevPage());

	bltree->unlatchPage(m_session, leftPage, Exclusived);
	bltree->unlatchPage(m_session, middlePage, Exclusived);


	bltree->latchPage(m_session, leftPage, Exclusived);
	//左节点删除部分键值变成下溢页面
	for(uint i = 0; i< 30; i++){
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i , i, KEY_NATURAL);
		leftPage->deleteIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	//中节点中插入部分数据
	bltree->latchPage(m_session, middlePage, Exclusived);
	for(uint i = 0; i< 50; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+8000, i+200, KEY_NATURAL);
		middlePage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, middlePage, Exclusived);

	//更新模式遍历，触发重分配
	MIndexScanInfo *scanInfo3 = bltree->generateScanInfo(m_session, Exclusived);
	void *data3 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
// 	scanInfo3->m_searchKey = new (data3)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
// 		NULL, 0, INVALID_ROW_ID);
	scanInfo3->m_searchKey = m_keyHelper->createKey(m_session->getMemoryContext(), 50, 50, KEY_NATURAL);
	bltree->updateModeLocateLeafPage(scanInfo3);
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	//将右节点插分裂
	bltree->latchPage(m_session, rightPage, Exclusived);
	for(uint i = 0; i< 450; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+8000, i+8000, KEY_NATURAL);
		if (NEED_SPLIT == rightPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			newPage = MIDX_PAGE_NONE;
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rightPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}
	MIndexPageHdl rightRightPage = newPage;
	assert(rightRightPage != rightPage && rightRightPage == rightPage->getNextPage());

	bltree->unlatchPage(m_session, rightPage, Exclusived);
	bltree->unlatchPage(m_session, rightRightPage, Exclusived);

	bltree->latchPage(m_session, rightPage, Exclusived);
	//右节点删除部分键值变成下溢页面
	for(uint i = 0; i< 20; i++){
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+300 , i+300, KEY_NATURAL);
		rightPage->deleteIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, rightPage, Exclusived);

	//右右节点中插入部分数据
	bltree->latchPage(m_session, rightRightPage, Exclusived);
	for(uint i = 0; i< 30; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+10000, i+10000, KEY_NATURAL);
		rightRightPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, rightRightPage, Exclusived);


	//连接右右节点和父节点
	bltree->latchPage(m_session, rootPage, Exclusived);
	bltree->latchPage(m_session, rightPage, Exclusived);
	bltree->latchPage(m_session, rightRightPage, Exclusived);
	bltree->linkChild(scanInfo, rootPage, rightPage, rightRightPage);
	bltree->unlatchPage(m_session, rootPage, Exclusived);
	bltree->unlatchPage(m_session, rightPage, Exclusived);
	bltree->unlatchPage(m_session, rightRightPage, Exclusived);


	//更新模式遍历，触发重分配
	MIndexScanInfo *scanInfo4 = bltree->generateScanInfo(m_session, Exclusived);
	void *data4 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
// 	scanInfo4->m_searchKey = new (data4)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
// 		NULL, 0, INVALID_ROW_ID);
	scanInfo4->m_searchKey = m_keyHelper->createKey(m_session->getMemoryContext(), 400, 400, KEY_NATURAL);
	bltree->updateModeLocateLeafPage(scanInfo4);
	bltree->unlatchPage(m_session, rightPage, Exclusived);


	//右右节点中插分裂
	bltree->latchPage(m_session, rightRightPage, Exclusived);
	for(uint i = 0; i< 450; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+12000, i+12000, KEY_NATURAL);
		if (NEED_SPLIT == rightRightPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			newPage = MIDX_PAGE_NONE;
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rightRightPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}
	MIndexPageHdl right3Page = newPage;
	assert(right3Page != rightRightPage && right3Page == rightRightPage->getNextPage());

	bltree->unlatchPage(m_session, rightRightPage, Exclusived);
	bltree->unlatchPage(m_session, right3Page, Exclusived);

	//右节点删除部分键值变成下溢页面
	bltree->latchPage(m_session, rightPage, Exclusived);
	for(uint i = 0; i< 20; i++){
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+250 , i+250, KEY_NATURAL);
		rightPage->deleteIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, rightPage, Exclusived);

	//更新模式遍历，触发重分配
	MIndexScanInfo *scanInfo5 = bltree->generateScanInfo(m_session, Exclusived);
	void *data5 = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
// 	scanInfo5->m_searchKey = new (data5)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
// 		NULL, 0, INVALID_ROW_ID);
	scanInfo5->m_searchKey = m_keyHelper->createKey(m_session->getMemoryContext(), 249, 249, KEY_NATURAL);
	bltree->updateModeLocateLeafPage(scanInfo5);
	bltree->unlatchPage(m_session, rightPage, Exclusived);

	clear();
}


void MIndexTestCase::testRepairNotRightMost2() {
	init(false);
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);


	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	bltree->unlatchPage(m_session, rootPage, Exclusived);



	bltree->latchPage(m_session, leftPage, Exclusived);
	//左节点删除部分键值变成下溢页面
	for(uint i = 0; i< 30; i++){
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i , i, KEY_NATURAL);
		leftPage->deleteIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator);
	}
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	//将右节点插分裂
	bltree->latchPage(m_session, rightPage, Exclusived);
	for(uint i = 0; i< 450; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i+429, i+429, KEY_NATURAL);
		if (NEED_SPLIT == rightPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			newPage = MIDX_PAGE_NONE;
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rightPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}
	MIndexPageHdl rightRightPage = newPage;
	assert(rightRightPage != rightPage && rightRightPage == rightPage->getNextPage());

	bltree->unlatchPage(m_session, rightPage, Exclusived);
	bltree->unlatchPage(m_session, rightRightPage, Exclusived);

	//根节点插分裂
	for(uint i = 1000; i< 1406; i++){
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, 300, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator, NULL)) 
			break;
	}

	//开始repair线程
	MIndexRepairNotRightMostThread rplUndflwThread("RepairNotRightMost",m_db, m_tblDef, m_memIndex, m_keyHelper);

	rplUndflwThread.enableSyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST);
	rplUndflwThread.enableSyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST1);


	rplUndflwThread.start();
	rplUndflwThread.joinSyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST);


	rplUndflwThread.disableSyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST);
	rplUndflwThread.notifySyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST);

	rplUndflwThread.joinSyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST1);
	SubRecord highKey(KEY_NATURAL, scanInfo->m_searchKey->m_numCols, scanInfo->m_searchKey->m_columns, NULL, 0);
	rootPage->initPage(MIndexPage::formPageMark(0), rootPage->getPageType(), 
		rootPage->getPageLevel());
	leftPage->getHighKey(&highKey);
	NTSE_ASSERT(INSERT_SUCCESS == rootPage->addIndexKey(&highKey, scanInfo->m_assistKey, 
		scanInfo->m_comparator, leftPage));//一定成功

	rplUndflwThread.disableSyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST1);
	rplUndflwThread.notifySyncPoint(SP_MEM_INDEX_REPAIRNOTRIGMOST1);

	rplUndflwThread.join(-1);


	clear();
}






void MIndexTestCase::testCheckDup() {
	init(true);

	//插入测试键值
	uint range = 8000;
	for (uint i = 0; i < range; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_PAD);
		m_memIndex->insert(m_session, key, 0);
	}

	//测试已插入键值会发生键值重复（KEY_NATURAL）
	for (uint i = 0; i < range; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_NATURAL);

		CPPUNIT_ASSERT(m_memIndex->checkDuplicate(m_session, key));
	}

	//测试已插入键值会发生键值重复 （KEY_PAD）
	for (uint i = 0; i < range; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_PAD);

		CPPUNIT_ASSERT(m_memIndex->checkDuplicate(m_session, key));
	}

	//测试没插入过键值不会发生键值重复
	for (uint i = 0; i < range; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		uint testKeyValue = (uint)(range + (uint)System::random() % range);
		
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, testKeyValue, KEY_NATURAL);

		CPPUNIT_ASSERT(!m_memIndex->checkDuplicate(m_session, key));
	}

	clear();
}

void MIndexTestCase::testChangeTreeHeight() {
	init(true);

	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);
	
	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);

	for (uint i = 0; i < 100000; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);
			//此时在分裂后的页面上插入一定可以成功
			NTSE_ASSERT(INSERT_SUCCESS == rootPage->addIndexKey(key, scanInfo->m_assistKey, 
				scanInfo->m_comparator, MIDX_PAGE_NONE));
			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);

	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	//将树变成孤枝，然后降低树高度
	CPPUNIT_ASSERT(rootPage->deleteIndexKey(&tmpKey, scanInfo->m_assistKey, scanInfo->m_comparator));
	leftPage->setNextPage(MIDX_PAGE_NONE);

	bltree->latchPage(m_session, leftPage, Exclusived);
	coverPage = bltree->decreaseTreeHeight(scanInfo, leftPage);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	clear();
}

void MIndexTestCase::testUpgradePageLatch() {
	init(true);

	{
		const IndexDef *indexDef = m_keyHelper->getIndexDef();
		BLinkTree *bltree = (BLinkTree*)m_memIndex;
		MIndexPageHdl rootPage = bltree->getRootPage();

		bltree->latchPage(m_session, rootPage, Shared);
		CPPUNIT_ASSERT_EQUAL(Shared, bltree->getPageLatchMode(rootPage));
		CPPUNIT_ASSERT(bltree->upgradeLatch(m_session, rootPage));
		CPPUNIT_ASSERT_EQUAL(Exclusived, bltree->getPageLatchMode(rootPage));

		bltree->unlatchPage(m_session, rootPage, Exclusived);

		//第一次
		MIndexUpgradeLatchThread uplatchThread1("upgrade latch thread",m_db, m_tblDef, m_memIndex, m_keyHelper);
		uplatchThread1.enableSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		uplatchThread1.enableSyncPoint(SP_MEM_INDEX_UPGREADLATCH1);
		uplatchThread1.start();
		uplatchThread1.joinSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		bltree->latchPage(m_session, rootPage, Shared);
		uplatchThread1.disableSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		uplatchThread1.notifySyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		Thread::msleep(2000);
		bltree->unlatchPage(m_session, rootPage, Shared);
		

		uplatchThread1.joinSyncPoint(SP_MEM_INDEX_UPGREADLATCH1);
		bltree->setPageType(rootPage, PAGE_EMPTY);
		uplatchThread1.disableSyncPoint(SP_MEM_INDEX_UPGREADLATCH1);
		uplatchThread1.notifySyncPoint(SP_MEM_INDEX_UPGREADLATCH1);
		Thread::msleep(2000);
		bltree->setPageType(rootPage, PAGE_MEM_INDEX);
		uplatchThread1.join(-1);

		//第二次
		MIndexUpgradeLatchThread uplatchThread2("upgrade latch thread",m_db, m_tblDef, m_memIndex, m_keyHelper);
		uplatchThread2.enableSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		uplatchThread2.enableSyncPoint(SP_MEM_INDEX_UPGREADLATCH2);
		uplatchThread2.start();
		uplatchThread2.joinSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		bltree->latchPage(m_session, rootPage, Shared);
		uplatchThread2.disableSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		uplatchThread2.notifySyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		Thread::msleep(2000);
		bltree->unlatchPage(m_session, rootPage, Shared);

		uplatchThread2.joinSyncPoint(SP_MEM_INDEX_UPGREADLATCH2);
		rootPage->incrTimeStamp();
		uplatchThread2.disableSyncPoint(SP_MEM_INDEX_UPGREADLATCH2);
		uplatchThread2.notifySyncPoint(SP_MEM_INDEX_UPGREADLATCH2);
		uplatchThread2.join(-1);

		//第三次
		MIndexUpgradeLatchThread uplatchThread3("upgrade latch thread",m_db, m_tblDef, m_memIndex, m_keyHelper);
		uplatchThread3.enableSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		uplatchThread3.start();
		uplatchThread3.joinSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		bltree->latchPage(m_session, rootPage, Shared);
		uplatchThread3.disableSyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		uplatchThread3.notifySyncPoint(SP_MEM_INDEX_UPGREADLATCH);
		Thread::msleep(2000);
		bltree->unlatchPage(m_session, rootPage, Shared);
		uplatchThread3.join(-1);


	}

	clear();
}

void MIndexTestCase::testTryTrxLockHoldingLatch() {
	init(true);
	
	//插入测试键值
	uint range = 1;
	for (uint i = 0; i < range; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_PAD);
		m_memIndex->insert(m_session, key, 0);
	}
	//对一行加上X NTSE锁
	RowLockHandle* rowLockHdl = m_session->tryLockRow(m_tblDef->m_id, 0, Exclusived, __FILE__, __LINE__);

	assert(rowLockHdl != NULL);
	//新启一个线程，做加锁操作
	MIndexTryLockThread vlThread("LockRow",m_db, m_tblDef, m_memIndex, m_keyHelper);
	vlThread.enableSyncPoint(SP_MEM_INDEX_TRYLOCK);
	vlThread.start();
	vlThread.joinSyncPoint(SP_MEM_INDEX_TRYLOCK);
	vlThread.notifySyncPoint(SP_MEM_INDEX_TRYLOCK);

	//解锁
	SubRecord *key2 = m_keyHelper->createKey(m_session->getMemoryContext(), 1, 1, KEY_PAD);
	m_memIndex->insert(m_session, key2, 0);
	m_session->unlockRow(&rowLockHdl);

	vlThread.joinSyncPoint(SP_MEM_INDEX_TRYLOCK);
	//再加锁
	RowLockHandle* rowLockHdl1 = m_session->tryLockRow(m_tblDef->m_id, 0, Exclusived, __FILE__, __LINE__);
	vlThread.disableSyncPoint(SP_MEM_INDEX_TRYLOCK);
	vlThread.notifySyncPoint(SP_MEM_INDEX_TRYLOCK);

	Thread::msleep(2000);
	//再解锁
	m_session->unlockRow(&rowLockHdl1);
	vlThread.join(-1);
	clear();
}



void MIndexTestCase::testShiftBackScan() {
	init(false);
	//插入原始数据
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, 0, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);


	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	bltree->unlatchPage(m_session, rootPage, Exclusived);

	//启动扫描线程
	MIndexShiftBackScanThread shbackThread("shiftBackThread",m_db, m_tblDef, m_memIndex, m_keyHelper);
	shbackThread.enableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	shbackThread.enableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN1);
	shbackThread.enableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN2);
	shbackThread.enableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN3);
	shbackThread.start();
	
	//第一次， 加页面latch，改变prev页面的页面类型
	shbackThread.joinSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	bltree->latchPage(m_session, leftPage, Exclusived);
	shbackThread.notifySyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	Thread::msleep(2000);
	bltree->unlatchPage(m_session, leftPage, Exclusived);
	
	shbackThread.joinSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN1);
	bltree->setPageType(leftPage, PAGE_EMPTY);
	shbackThread.disableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN1);
	shbackThread.notifySyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN1);
	Thread::msleep(2000);
	bltree->setPageType(leftPage, PAGE_MEM_INDEX);

	//第二次， 加页面latch，改变current页面的页面类型
	shbackThread.joinSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	bltree->latchPage(m_session, leftPage, Exclusived);
	shbackThread.notifySyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	Thread::msleep(2000);
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	shbackThread.joinSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN2);
	bltree->setPageType(rightPage, PAGE_EMPTY);
	shbackThread.disableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN2);
	shbackThread.notifySyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN2);
	Thread::msleep(2000);
	bltree->setPageType(rightPage, PAGE_MEM_INDEX);

	//第三次， 加页面latch，改变current页面的时间戳
	shbackThread.joinSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	bltree->latchPage(m_session, leftPage, Exclusived);
	shbackThread.notifySyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	Thread::msleep(2000);
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	shbackThread.joinSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN3);
	rightPage->incrTimeStamp();
	shbackThread.disableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN3);
	shbackThread.notifySyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN3);


	//第四次，加上页面latch
	shbackThread.joinSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	bltree->latchPage(m_session, leftPage, Exclusived);
	shbackThread.disableSyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	shbackThread.notifySyncPoint(SP_MEM_INDEX_SHIFTBACKSCAN);
	Thread::msleep(2000);
	bltree->unlatchPage(m_session, leftPage, Exclusived);

	shbackThread.join(-1);

	clear();
}




void MIndexTestCase::testCheckHandlePage() {
	init(true);

	//插入测试键值
	uint range = 10;
	for (uint i = 0; i < range; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, i, KEY_PAD);
		m_memIndex->insert(m_session, key, 0);
	}

	//开启线程
	MIndexCheckPageLeafThread checkPageThread("checkPageThread",m_db, m_tblDef, m_memIndex, m_keyHelper);
	checkPageThread.enableSyncPoint(SP_MEM_INDEX_CHECKPAGE);
	checkPageThread.enableSyncPoint(SP_MEM_INDEX_CHECKPAGE1);
	checkPageThread.start();
	checkPageThread.joinSyncPoint(SP_MEM_INDEX_CHECKPAGE);

	BLinkTree *blTree = (BLinkTree*)m_memIndex;
	MIndexPageHdl rootPage = blTree->getRootPage();
	rootPage->setPageType(NON_LEAF_PAGE);
	rootPage->incrTimeStamp();

	checkPageThread.disableSyncPoint(SP_MEM_INDEX_CHECKPAGE);
	checkPageThread.notifySyncPoint(SP_MEM_INDEX_CHECKPAGE);

	checkPageThread.joinSyncPoint(SP_MEM_INDEX_CHECKPAGE1);
	rootPage->setPageType(ROOT_AND_LEAF);

	checkPageThread.disableSyncPoint(SP_MEM_INDEX_CHECKPAGE1);
	checkPageThread.notifySyncPoint(SP_MEM_INDEX_CHECKPAGE1);
	
	checkPageThread.join(-1);
	clear();
}

void MIndexTestCase::testShiftForwardKey() {
	init(false);
	//插入原始数据
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, 0, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);


	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	bltree->unlatchPage(m_session, rootPage, Exclusived);

	//启动扫描线程
	MIndexShiftForwardKeyThread shForwardThread("shiftForwardThread",m_db, m_tblDef, m_memIndex, m_keyHelper);
	shForwardThread.enableSyncPoint(SP_MEM_INDEX_SHIFT_FORWARD_KEY);
	shForwardThread.start();
	shForwardThread.joinSyncPoint(SP_MEM_INDEX_SHIFT_FORWARD_KEY);

	rightPage->initPage(MIndexPage::formPageMark(m_memIndex->getIndexId()), ROOT_AND_LEAF, 0);

	shForwardThread.disableSyncPoint(SP_MEM_INDEX_SHIFT_FORWARD_KEY);
	shForwardThread.notifySyncPoint(SP_MEM_INDEX_SHIFT_FORWARD_KEY);

	shForwardThread.join(-1);


	clear();
}

void MIndexTestCase::testShiftBackwardKey() {
	init(false);
	//插入原始数据
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	for (uint i = 0; i < 429; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createKey(m_session->getMemoryContext(), i, 0, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}

	//此时根页面有溢出页面，树只有一层
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)1);

	//测试增加树高度
	MIndexPageHdl coverPage = bltree->increaseTreeHeight(scanInfo);
	bltree->unlatchPage(m_session, coverPage, Exclusived);
	CPPUNIT_ASSERT_EQUAL(bltree->getHeight(), (u8)2);


	bltree->latchPage(m_session, rootPage, Exclusived);
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	rootPage->getKeyByNo(0, &tmpKey);
	MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&tmpKey);
	rootPage->getKeyByNo(1, &tmpKey);
	MIndexPageHdl rightPage = MIndexKeyOper::readPageHdl(&tmpKey);
	CPPUNIT_ASSERT_EQUAL(leftPage->getNextPage(), rightPage);

	bltree->unlatchPage(m_session, rootPage, Exclusived);

	//启动扫描线程
	MIndexShiftBackwardKeyThread shForwardThread("shiftBackwardThread",m_db, m_tblDef, m_memIndex, m_keyHelper);
	shForwardThread.enableSyncPoint(SP_MEM_INDEX_SHIFT_BACKWARD_KEY);
	shForwardThread.start();
	shForwardThread.joinSyncPoint(SP_MEM_INDEX_SHIFT_BACKWARD_KEY);

	leftPage->initPage(MIndexPage::formPageMark(m_memIndex->getIndexId()), ROOT_AND_LEAF, 0);

	shForwardThread.disableSyncPoint(SP_MEM_INDEX_SHIFT_BACKWARD_KEY);
	shForwardThread.notifySyncPoint(SP_MEM_INDEX_SHIFT_BACKWARD_KEY);

	shForwardThread.join(-1);


	clear();
}

void MIndexTestCase::testInsertAndDelDifKeyByEndSpaces() {
	initVarcharIndex(false);
	//插入原始数据
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_memIndex;
	MIndexScanInfo *scanInfo = bltree->generateScanInfo(m_session, Exclusived);
	void *data = m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
		NULL, 0, INVALID_ROW_ID);

	MIndexPageHdl rootPage = bltree->getRootPage();
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	bltree->latchPage(m_session, rootPage, Exclusived);
	char title[5] = "aaaa";
	for (uint i = 0; i < 5; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		SubRecord *key = m_keyHelper->createVarcharKey(m_session->getMemoryContext(), i, title, KEY_NATURAL);
		if (NEED_SPLIT == rootPage->addIndexKey(key, scanInfo->m_assistKey, scanInfo->m_comparator)) {
			MIndexPageHdl insertPage = bltree->splitPage(scanInfo, rootPage, &newPage);

			bltree->latchPage(m_session, newPage, Exclusived);

			scanInfo->m_currentPage = insertPage;
			break;
		}
	}
	assert(newPage == MIDX_PAGE_NONE);
	bltree->unlatchPage(m_session, rootPage, Exclusived);
   

	SubRecordBuilder srb(m_tblDef, KEY_PAD);
	
	// 尝试插入一条Rowid相同，varchar尾部空格数不同的记录
	char newTitle[8] = "aaaa   ";
	SubRecord *insertRec = srb.createSubRecordByName("Title", newTitle);
	m_memIndex->insert(m_session, insertRec, 1);


	char newTitle1[10] = "aaaa     ";
	SubRecord *delRec = srb.createSubRecordByName("Title", newTitle1);
	RowIdVersion delVersion = 2;
	m_memIndex->delByMark(m_session, delRec, &delVersion);

	assert(rootPage->getKeyCount() == 6);

	freeSubRecord(insertRec);
	freeSubRecord(delRec);
	clear();
}



void MIndexTryLockThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	RowLockHandle *rlh = NULL;
	MIndexRangeScanHandle *newScanHdl = (MIndexRangeScanHandle*)m_index->beginScan(session, NULL, true, true, Shared, &rlh, TL_X);
	MIndexScanInfoExt *newScanInfo = (MIndexScanInfoExt*)newScanHdl->getScanInfo();
	while (m_index->getNext(newScanHdl)) {
		newScanInfo->moveCursor();
	}
	m_index->endScan(newScanHdl);

	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
	
}


void MIndexRepairRightMostThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	//需要做的事情
	BLinkTree *bltree = (BLinkTree*)m_index;
	const IndexDef* indexDef = m_keyHelper->getIndexDef();
	MIndexScanInfo *scanInfo2 = bltree->generateScanInfo(session, Exclusived);
	void *data2 = session->getMemoryContext()->alloc(sizeof(SubRecord));
// 	scanInfo2->m_searchKey = new (data2)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
// 		NULL, 0, INVALID_ROW_ID);
	scanInfo2->m_searchKey = m_keyHelper->createKey(session->getMemoryContext(), 428+5000, 428, KEY_NATURAL);
	MIndexPageHdl page = bltree->updateModeLocateLeafPage(scanInfo2);
	bltree->unlatchPage(session, page, Exclusived);

	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
}

void MIndexShiftBackScanThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	//需要做的事情
	//反向跨页扫描
	
	u64 savepoint = memoryContext->setSavepoint();

	SubRecord *searchKey = m_keyHelper->createKey(memoryContext, INVALID_ROW_ID, 0, KEY_PAD);

	RowLockHandle *rlh = NULL;
	MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle*)m_index->beginScan(
		session, searchKey, false, true, Shared, &rlh, TL_S);
	MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
	uint fetchCount = 0;
	while (m_index->getNext(scanHdl)) {

		fetchCount++;
		scanInfo->moveCursor();
	}

	m_index->endScan(scanHdl);

	memoryContext->resetToSavepoint(savepoint);

	
	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
}


void MIndexUpgradeLatchThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	//需要做的事情
	const IndexDef *indexDef = m_keyHelper->getIndexDef();
	BLinkTree *bltree = (BLinkTree*)m_index;
	MIndexPageHdl rootPage = bltree->getRootPage();

	bltree->latchPage(session, rootPage, Shared);
	CPPUNIT_ASSERT_EQUAL(Shared, bltree->getPageLatchMode(rootPage));
	bool ret = bltree->upgradeLatch(session, rootPage);

	if(bltree->getPageLatchMode(rootPage) == Shared)
		bltree->unlatchPage(session, rootPage, Shared);
	else if (bltree->getPageLatchMode(rootPage) == Exclusived)
		bltree->unlatchPage(session, rootPage, Exclusived);


	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);

}


void MIndexCheckPageLeafThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();
	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	//需要做的事情
	RowLockHandle *rlh = NULL;
	MIndexRangeScanHandle *newScanHdl = (MIndexRangeScanHandle*)m_index->beginScan(session, NULL, true, true, Shared, &rlh, TL_X);
	MIndexScanInfoExt *newScanInfo = (MIndexScanInfoExt*)newScanHdl->getScanInfo();
	while (m_index->getNext(newScanHdl)) {
		newScanInfo->moveCursor();
	}
	m_index->endScan(newScanHdl);

	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
}

void MIndexRepairNotRightMostThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	//需要做的事情
	BLinkTree *bltree = (BLinkTree*)m_index;
	const IndexDef* indexDef = m_keyHelper->getIndexDef();
	MIndexScanInfo *scanInfo2 = bltree->generateScanInfo(session, Exclusived);
	void *data2 = session->getMemoryContext()->alloc(sizeof(SubRecord));
// 	scanInfo2->m_searchKey = new (data2)SubRecord(KEY_NATURAL, indexDef->m_numCols, indexDef->m_columns, 
// 		NULL, 0, INVALID_ROW_ID);
	scanInfo2->m_searchKey = m_keyHelper->createKey(session->getMemoryContext(), 0, 0, KEY_NATURAL);
	MIndexPageHdl page = bltree->updateModeLocateLeafPage(scanInfo2);
	bltree->unlatchPage(session, page, Exclusived);

	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
}

void MIndexShiftForwardKeyThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	//需要做的事情
	//正向跨页扫描

	u64 savepoint = memoryContext->setSavepoint();

	SubRecord *searchKey = m_keyHelper->createKey(memoryContext, INVALID_ROW_ID, 0, KEY_PAD);

	RowLockHandle *rlh = NULL;
	MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle*)m_index->beginScan(
		session, searchKey, true, true, Shared, &rlh, TL_S);
	MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
	uint fetchCount = 0;
	while (m_index->getNext(scanHdl)) {

		fetchCount++;
		scanInfo->moveCursor();
	}

	m_index->endScan(scanHdl);

	memoryContext->resetToSavepoint(savepoint);


	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
}

void MIndexShiftBackwardKeyThread::run() {
	Connection *conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("MIndexTestCase::testMulti", conn);
	MemoryContext *memoryContext = session->getMemoryContext();

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(conn);
	trx->trxAssignReadView();
	session->setTrans(trx);

	//需要做的事情
	//反向跨页扫描

	u64 savepoint = memoryContext->setSavepoint();

	SubRecord *searchKey = m_keyHelper->createKey(memoryContext, INVALID_ROW_ID, 0, KEY_PAD);

	RowLockHandle *rlh = NULL;
	MIndexRangeScanHandle *scanHdl = (MIndexRangeScanHandle*)m_index->beginScan(
		session, searchKey, false, true, Shared, &rlh, TL_S);
	MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHdl->getScanInfo();
	uint fetchCount = 0;
	while (m_index->getNext(scanHdl)) {

		fetchCount++;
		scanInfo->moveCursor();
	}

	m_index->endScan(scanHdl);

	memoryContext->resetToSavepoint(savepoint);


	session->getTrans()->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(conn);
}