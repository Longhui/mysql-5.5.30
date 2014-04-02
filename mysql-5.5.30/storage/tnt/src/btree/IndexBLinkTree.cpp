/**
* BLink树实现相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "btree/IndexBLinkTree.h"
#include "btree/MIndexPage.h"
#include <stack>

namespace tnt {

MIndexScanInfo::MIndexScanInfo(Session *session, TNTTransaction *trx, const TableDef *tableDef, 
	const IndexDef *indexDef, LockMode latchMode, CompareKey comparator) 
	: m_session(session), m_trx(trx), m_indexDef(indexDef), m_searchFlag(SearchFlag::DEFAULT_FLAG) {

		m_traversalMode = (Shared == latchMode) ? READMODE : UPDATEMODE;
		m_currentPage = MIDX_PAGE_NONE;
		m_pageTimeStamp = (u64)-1;
		
		MemoryContext* mtx = session->getMemoryContext();
		void * buf = mtx->alloc(sizeof(SubRecord));
		m_assistKey = new (buf)SubRecord(KEY_NATURAL, indexDef->m_numCols, 
			indexDef->m_columns, NULL, 0);
		buf = mtx->alloc(sizeof(SubRecord));

		m_latchMode = latchMode;

		buf = mtx->alloc(sizeof(KeyComparator));
		m_comparator = new (buf)KeyComparator(tableDef, indexDef);
		m_comparator->setComparator(comparator);
		
		m_searchKey = NULL;
		m_extractor = NULL;
		m_fetchCount = 0;
		m_rangeFirst = true;

		m_forward = true;
		m_uniqueScan = true;
		m_hasNext = true;
}

MIndexScanInfoExt::MIndexScanInfoExt(Session *session, TNTTransaction *trx, 
									 const TableDef *tableDef, const IndexDef *indexDef, 
									 LockMode latchMode, CompareKey comparator, 
									 LockMode ntseRlMode, TLockMode trxLockMode)
									 : MIndexScanInfo(session, trx, tableDef, indexDef, 
									 latchMode, comparator) {
	m_readKey = IndexKey::allocSubRecord(session->getMemoryContext(), indexDef, KEY_NATURAL);
	m_readPageTimeStamp = (u64)-1;
	m_readPage = MIDX_PAGE_NONE;
	m_searchKeyBufSize = 0;
	m_forceSearchPage = false;
	m_ntseRlMode = ntseRlMode;
	m_trxLockMode = trxLockMode;
}


/**
 * 检查查找键的格式是否是第一次查询的状态，不是则转化为全属性键格式
 */
void MIndexScanInfoExt::checkFormat() {
	if(unlikely(m_searchKey->m_numCols != m_indexDef->m_numCols)) {
		m_searchKey->m_columns = m_indexDef->m_columns;
		m_searchKey->m_numCols = m_indexDef->m_numCols;
		m_searchKey->m_size = m_indexDef->m_maxKeySize;
	}
}

/**
 * 移动内存索引游标到读到的下一条记录
 */
void MIndexScanInfoExt::moveCursor() {
	checkFormat();
	MIndexKeyOper::copyKeyAndExt(m_searchKey, m_readKey, true);
	m_currentPage = m_readPage;
	m_pageTimeStamp = m_readPageTimeStamp;
	m_keyLocation = m_readKeyLocation;
	m_tmpRangeFirst = m_rangeFirst;
}

/**
 * 移动内存索引游标到指定的记录
 * @param key 指定的记录
 */
void MIndexScanInfoExt::moveCursorTo(const SubRecord *key) {
	checkFormat();
	assert(KEY_PAD == key->m_format);
	m_searchKey->m_size = m_indexDef->m_maxKeySize;
	RecordOper::convertKeyPN(m_tableDef, m_indexDef, key, m_searchKey);
	m_includeKey = false;
	m_forceSearchPage = true;
}

/**
 * 构造函数
 * @param session 会话
 * @param mIndice 所属的内存索引管理
 * @param pageManager TNT内存页面管理
 * @param doubleChecker RowId校验
 * @param tableDef 所属的表定义
 * @param indexDef 索引定义
 * @param indexId  索引id
 */
BLinkTree::BLinkTree(Session *session, MIndice *mIndice, TNTIMPageManager *pageManager, 
					 const DoubleChecker *doubleChecker, TableDef **tableDef, 
					 const IndexDef *indexDef, u8 indexId) 
					 : m_mIndice(mIndice), m_pageManager(pageManager), m_tableDef(tableDef), 
					 m_indexDef(indexDef), m_indexId(indexId), m_doubleChecker(doubleChecker) {
	ftrace(ts.mIdx, tout << mIndice << indexDef << indexId);
	//初始化统计信息
	memset(&m_indexStatus, 0, sizeof(m_indexStatus));
}

BLinkTree::~BLinkTree() {
	assert(MIDX_PAGE_NONE == m_rootPage);
	m_pageManager = NULL;
}


bool BLinkTree::init(Session *session, bool waitSuccess) {
	//分配以及初始化根页面
	m_rootPage = getFreeNewPage(session, waitSuccess);
	if(MIDX_PAGE_NONE == m_rootPage)
		return false;
	initRootPage(session);
	unlatchPage(session, m_rootPage, Exclusived);
	return true;
}

/**
 * 关闭内存索引，回收内存
 * @param session 会话
 */
void BLinkTree::close(Session *session) {
	ftrace(ts.mIdx, tout << session);

	stack<MIndexPageHdl> path;
	MIndexPageHdl currentPage = m_rootPage;
	latchPage(session, currentPage, Exclusived, __FILE__, __LINE__);
	path.push(currentPage);
	
	SubRecord tmpKey(KEY_NATURAL, 0, NULL, NULL, 0);
	while (!currentPage->isPageLeaf()) {
		currentPage->getFirstKey(&tmpKey);
		MIndexPageHdl nextPage = MIndexKeyOper::readPageHdl(&tmpKey);
		currentPage = nextPage;
		latchPage(session, currentPage, Exclusived, __FILE__, __LINE__);
		path.push(currentPage);
	}
	assert((u8)path.size() == m_rootPage->getPageLevel() + 1);

	while (!path.empty()) {
		MIndexPageHdl firstPage = path.top();
		realeaseLevel(session, firstPage);
		path.pop();
	}
	m_rootPage = MIDX_PAGE_NONE;
}

/**
 * 回收一层的所有页面
 * @param session
 * @param firstPage
 */
void BLinkTree::realeaseLevel(Session *session, MIndexPageHdl firstPage) {
	assert(firstPage != MIDX_PAGE_NONE);

	MIndexPageHdl currentPage = firstPage;

	while (currentPage != MIDX_PAGE_NONE) {
		MIndexPageHdl nextPage = currentPage->getNextPage();
		if (nextPage!= MIDX_PAGE_NONE) {
			latchPage(session, nextPage, Exclusived, __FILE__, __LINE__);
		}
		releasePage(session, currentPage);
		currentPage = nextPage;
	}
}

/**
 * 回收一个页面
 * @param session
 * @param page
 * @param clean
 */
void BLinkTree::releasePage(Session *session, MIndexPageHdl page, bool isToFreeList, bool clean) {
	if (clean) {
		memset((void *)page, 0, MIndexPage::MINDEX_PAGE_SIZE);
	}
	if(!isToFreeList)
		m_pageManager->releasePage(session->getId(), page);
	else
		m_pageManager->putToFreeList(session->getId(), page);

	//统计信息
	m_indexStatus.m_numFreePage.increment();
}

/**
 * 初始化根页面
 * @param session 会话
 */
void BLinkTree::initRootPage(Session *session) {
	ftrace(ts.mIdx, tout << session);

	m_rootPage->initPage(MIndexPage::formPageMark(m_indexId), ROOT_AND_LEAF, 0);

	McSavepoint mcs(session->getMemoryContext());
	SubRecord *infiniteKey = MIndexKeyOper::createInfiniteKey(session->getMemoryContext(), m_indexDef);
	m_rootPage->appendIndexKey(infiniteKey);
	m_rootPage->setLeafPageHighKey();
}

/**
 * 在Blink树中插入键值
 * @param session 会话
 * @param key     要插入的键值, 冗余格式
 * @return 插入是否成功
 */
void BLinkTree::insert(Session *session, const SubRecord *key, RowIdVersion version) {
	ftrace(ts.mIdx, tout << session << key << rid(key->m_rowId););

	assert(key != NULL);
	assert(KEY_PAD == key->m_format);
	// 首先生成一个扫描信息句柄
	TNTTransaction *trx = session->getTrans();
	assert(trx != NULL);
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavePoint(mtx);
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);

	//设置查找键
	scanInfo->m_searchKey = MIndexKeyOper::convertKeyPN(mtx, key, *m_tableDef, m_indexDef);
	
	u64 sp = session->getMemoryContext()->setSavepoint();

__restart:
	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);
	
	MkeyLocation keyLocation;
	assert(scanInfo->m_currentPage);
	bool exist = scanInfo->m_currentPage->locateInsertKeyInLeafPage(scanInfo->m_searchKey, 
		scanInfo->m_assistKey, scanInfo->m_comparator, &keyLocation);

	if (exist) {
		assert(MIDX_PAGE_NONE != scanInfo->m_currentPage);
		assert(scanInfo->m_assistKey->m_rowId == scanInfo->m_searchKey->m_rowId);
		assert(m_pageManager->isPageLatched(scanInfo->m_currentPage, Exclusived));
		
		// varchar 结尾的空格，导致mysql认为相等，但是实际数据不同
		// TODO: 如果申请不到分裂页面，会暂时导致其他事务快照读遗漏记录
		if (RecordOper::compareKeyNNorPPColumnSize(*m_tableDef, scanInfo->m_searchKey, scanInfo->m_assistKey, m_indexDef)) {
			// 先删除原有项，再插入新项
			uint oldSize = scanInfo->m_assistKey->m_size;
			uint newSize = scanInfo->m_searchKey->m_size;
			if (newSize > oldSize && newSize - oldSize > scanInfo->m_currentPage->getFreeSpace()) {
				// 增长更新，需要分裂索引页面， 要保证删除和插入在同一次latch保护下完成，因此先申请新页面
				newPage = getFreeNewPage(session, false);
				if (newPage == MIDX_PAGE_NONE) {
					unlatchPage(session, scanInfo->m_currentPage, Exclusived);
					newPage = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, true);
					releasePage(session, newPage, true);
					session->getMemoryContext()->resetToSavepoint(sp);
					//统计信息
					m_indexStatus.m_numAllocPage.increment();
					m_indexStatus.m_numRestarts++;
					goto __restart ;
				}
			}
			scanInfo->m_currentPage->deleteIndexKey(scanInfo->m_searchKey, scanInfo->m_assistKey, scanInfo->m_comparator);
			goto __insertStep;
		}

		RowIdVersion oldVersion = MIndexKeyOper::readIdxVersion(scanInfo->m_assistKey);
		// 当索引中存在一项完全一样项（rowId、键值、delbit都相等），其version必须不一样
		if (MIndexKeyOper::readDelBit(scanInfo->m_assistKey) == 0) {
			assert(oldVersion != version);
		}

		MIndexKeyOper::writeDelBit(scanInfo->m_assistKey, 0);
		if(version != INVALID_VERSION)
			MIndexKeyOper::writeIdxVersion(scanInfo->m_assistKey, version);
	} else {
__insertStep:
		MIndexKeyOper::writeDelBit(scanInfo->m_searchKey, 0);
		if(version != INVALID_VERSION)
			MIndexKeyOper::writeIdxVersion(scanInfo->m_searchKey, version);
		//尝试插入，如果失败（内存不足拿不到分裂新页面，重启算法）
		if(addKeyOnLeafPage(scanInfo, scanInfo->m_searchKey, &keyLocation, newPage) == false) {
			assert(newPage == MIDX_PAGE_NONE);
			session->getMemoryContext()->resetToSavepoint(sp);	
			//统计信息
			m_indexStatus.m_numRestarts++;
			goto __restart ;
		}
	}

	//修改页面最大更新事务号
	scanInfo->m_currentPage->setMaxTrxId(trx->getTrxId());

	//释放页面x-latch
	unlatchPage(session, scanInfo->m_currentPage, Exclusived);

	//修改统计信息
	m_indexStatus.m_numInsert++;
}

/**
 * 查找索引项并标记为删除
 * @param session 会话
 * @param key 要标记删除的键值，冗余格式
 * @param version IN/OUT 传入为FirstUpdate传入的version值，传出为标记为删除项的version值
 * @return 是否找到指定项
 */
bool BLinkTree::delByMark(Session *session, const SubRecord *key, RowIdVersion *version) {

	ftrace(ts.mIdx, tout << session << key << rid(key->m_rowId););

	assert(key != NULL);
	assert(key->m_rowId != INVALID_ROW_ID);
	assert(KEY_PAD == key->m_format);

	TNTTransaction *trx = session->getTrans();
	assert(trx != NULL);
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavePoint(mtx);
	
	// 首先生成一个扫描信息句柄
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);
	
	//设置查找键
	scanInfo->m_searchKey = MIndexKeyOper::convertKeyPN(mtx, key, *m_tableDef, m_indexDef);
	
	u64 sp = session->getMemoryContext()->setSavepoint();

__restart:
	//在叶页面中定位查找键
	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);
	
	MkeyLocation keyLocation;
	assert(scanInfo->m_currentPage);
	bool exist = scanInfo->m_currentPage->locateInsertKeyInLeafPage(scanInfo->m_searchKey, 
		scanInfo->m_assistKey, scanInfo->m_comparator, &keyLocation);

	if (exist) {//旧版本存在, 修改delete bit
		assert(MIDX_PAGE_NONE != scanInfo->m_currentPage);
		assert(scanInfo->m_assistKey->m_rowId == scanInfo->m_searchKey->m_rowId);
		assert(m_pageManager->isPageLatched(scanInfo->m_currentPage, Exclusived));
		// varchar 结尾的空格，导致mysql认为相等，但是实际数据不同
		// TODO: 如果申请不到分裂页面，会暂时导致其他事务快照读遗漏记录
		if (RecordOper::compareKeyNNorPPColumnSize(*m_tableDef, scanInfo->m_searchKey, scanInfo->m_assistKey, m_indexDef)) {
			// 先删除原有项，再插入新项
			uint oldSize = scanInfo->m_assistKey->m_size;
			uint newSize = scanInfo->m_searchKey->m_size;
			if (newSize > oldSize && newSize - oldSize > scanInfo->m_currentPage->getFreeSpace()) {
				// 增长更新，需要分裂索引页面， 要保证删除和插入在同一次latch保护下完成，因此先申请新页面
				newPage = getFreeNewPage(session, false);
				if (newPage == MIDX_PAGE_NONE) {
					unlatchPage(session, scanInfo->m_currentPage, Exclusived);
					newPage = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, true);
					releasePage(session, newPage, true);
					session->getMemoryContext()->resetToSavepoint(sp);
					//统计信息
					m_indexStatus.m_numAllocPage.increment();
					m_indexStatus.m_numRestarts++;
					goto __restart ;
				}
			}
			scanInfo->m_currentPage->deleteIndexKey(scanInfo->m_searchKey, scanInfo->m_assistKey, scanInfo->m_comparator);
			goto __insertStep;
		}

		MIndexKeyOper::writeDelBit(scanInfo->m_assistKey, 1);
		*version = MIndexKeyOper::readIdxVersion(scanInfo->m_assistKey);
	} else {//旧版本不存在，插入delete bit为1的旧版本
__insertStep:
		MIndexKeyOper::writeDelBit(scanInfo->m_searchKey, 1);
		NTSE_ASSERT(*version != INVALID_VERSION);
		MIndexKeyOper::writeIdxVersion(scanInfo->m_searchKey, *version);
		//尝试插入，如果失败（内存不足拿不到分裂新页面，重启算法）
		if(addKeyOnLeafPage(scanInfo, scanInfo->m_searchKey, &keyLocation, newPage) == false) {
			assert(newPage == MIDX_PAGE_NONE);
			session->getMemoryContext()->resetToSavepoint(sp);
			//统计信息
			m_indexStatus.m_numRestarts++;
			goto __restart;
		}
	}

	//修改页面最大更新事务号
	scanInfo->m_currentPage->setMaxTrxId(trx->getTrxId());

	unlatchPage(session, scanInfo->m_currentPage, Exclusived);

	//修改统计信息
	m_indexStatus.m_numDelete++;
	return true;
}

/**
 * 摘除已被Purge掉的索引记录
 * @param session
 * @param readView
 * @return 
 */
u64 BLinkTree::purge(Session *session, const ReadView *readView) {
	ftrace(ts.mIdx, tout << session << readView);
	u32 beginTime = System::fastTime();
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);
	void *data = session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, m_indexDef->m_numCols, 
		m_indexDef->m_columns, NULL, 0, INVALID_ROW_ID);
	
	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);

	//第一次遍历叶页面，回收页面中的可回收项
	MIndexPageHdl &currentPage = scanInfo->m_currentPage;
	u64 purgeKeyCount = 0;
	do {
		assert(currentPage->isPageLeaf());
		assert(Exclusived == getPageLatchMode(currentPage));
		purgeKeyCount += currentPage->bulkPhyReclaim(readView, scanInfo->m_assistKey, m_doubleChecker);
	
		//修改统计信息
		m_indexStatus.m_numDelete += purgeKeyCount;

		VERIFY_TNT_MINDEX_PAGE(currentPage);

		MIndexPageHdl nextPage = currentPage->getNextPage();
		if (MIDX_PAGE_NONE != nextPage) {
			latchPage(session, nextPage, Exclusived, __FILE__, __LINE__);
			unlatchPage(session, currentPage, Exclusived);
			currentPage = nextPage;
		} else {
			unlatchPage(session, currentPage, Exclusived);
			break;
		}
	} while (MIDX_PAGE_NONE != currentPage);
	u32 endTime = System::fastTime();
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "purge Table: %s Index: %s  free %d records Time is %d",(*this->m_tableDef)->m_name, this->m_indexDef->m_name, purgeKeyCount, endTime - beginTime);
	//重整树，回收或合并下溢的页面
//	repairTree(scanInfo);

	return purgeKeyCount;
}

/**
 * 回收内存索引页面
 * @param session
 * @param hwm
 * @param lwm
 * @return 
 */
void BLinkTree::reclaimIndex(Session *session, u32 hwm, u32 lwm) {
	// 当前索引页面没有找过hwm则不做页面回收
	assert(m_indexStatus.m_numAllocPage.get() >= (u64)m_indexStatus.m_numFreePage.get());
	u32 currentPages = ((u64)m_indexStatus.m_numAllocPage.get() - (u64)m_indexStatus.m_numFreePage.get()); 
	if (currentPages <= hwm)
		return;
	u32 numReclaimPages = currentPages - lwm;
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);
	void *data = session->getMemoryContext()->alloc(sizeof(SubRecord));
	scanInfo->m_searchKey = new (data)SubRecord(KEY_NATURAL, m_indexDef->m_numCols, 
		m_indexDef->m_columns, NULL, 0, INVALID_ROW_ID);

	repairTree(scanInfo, numReclaimPages);
}

/**
 * 逐层重整整棵树
 * @param scanInfo 扫描句柄信息
 * @param numReclaimPages 本次回收操作至多需要回收的页面数
 */
void BLinkTree::repairTree(MIndexScanInfo *scanInfo, u32 numReclaimPages) {
	u8 currentLevel = 1;
	MIndexPageHdl currentPage = MIDX_PAGE_NONE;
	Session *session = scanInfo->m_session;
	uint reclaimLeafPageCnt = numReclaimPages;
	do {
		currentPage = updateModeTraverse(scanInfo, currentLevel);
		
		while (MIDX_PAGE_NONE != currentPage) {
			numReclaimPages -= repairLevel(scanInfo, currentPage, numReclaimPages);
			if (numReclaimPages == 0) {
				// 如果释放页面的数目达到目标，则结束整个回收操作
				unlatchPage(session, currentPage, Exclusived);
				break;
			}

			VERIFY_TNT_MINDEX_PAGE(currentPage);

			MIndexPageHdl nextPage = currentPage->getNextPage();
			if (nextPage != MIDX_PAGE_NONE) {
				latchPage(session, nextPage, Exclusived, __FILE__, __LINE__);
				unlatchPage(session, currentPage, Exclusived);
				currentPage = nextPage;
			} else {
				currentLevel = currentPage->getPageLevel() + 1;
				unlatchPage(session, currentPage, Exclusived);
				break;
			}
		}
	} while (currentPage != MIDX_PAGE_NONE && numReclaimPages > 0);

	//如果存在孤枝，降低树高度
	latchPage(session, m_rootPage, Exclusived, __FILE__, __LINE__);

	if (1 == m_rootPage->getKeyCount() && !m_rootPage->isPageLeaf()) {
		SubRecord tmpKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
		m_rootPage->getKeyByNo(0, &tmpKey);

		MIndexPageHdl childPage = MIndexKeyOper::readPageHdl(&tmpKey);
		latchPage(session, childPage, Exclusived, __FILE__, __LINE__);

		if (MIDX_PAGE_NONE == childPage->getNextPage()) {
			currentPage = decreaseTreeHeight(scanInfo, childPage);
			VERIFY_TNT_MINDEX_PAGE(m_rootPage);
			unlatchPage(session, currentPage, Exclusived);
		} else {
			unlatchPage(session, childPage, Exclusived);
			unlatchPage(session, m_rootPage, Exclusived);
		}
	} else {
		unlatchPage(session, m_rootPage, Exclusived);
	}
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "purge Table: %s Index: %s release %d pages", (*this->m_tableDef)->m_name, this->m_indexDef->m_name, reclaimLeafPageCnt - numReclaimPages);
}

/**
 * 重整树的一层
 * @param scanInfo   扫描句柄信息
 * @param parentPage 一层最左边的页面
 * @param numReclaimPages 至多能回收的页面数
 */
uint BLinkTree::repairLevel(MIndexScanInfo *scanInfo, MIndexPageHdl parentPage, u32 maxReclaimPages) {
	if (maxReclaimPages == 0)
		return 0;
	assert(getPageLatchMode(parentPage) == Exclusived);
	uint reclaimPageCnt = 0;
	u16 keyNo = 0;
	MIndexPageHdl currentPage = MIDX_PAGE_NONE;
	Session *session = scanInfo->m_session;

	if(parentPage->getKeyCount() > 0) {
		parentPage->getKeyByNo(0, scanInfo->m_assistKey);
		currentPage = MIndexKeyOper::readPageHdl(scanInfo->m_assistKey);

		//latch当前页面
		latchPage(session, currentPage, Exclusived, __FILE__, __LINE__);
		
		while(currentPage != MIDX_PAGE_NONE && keyNo<parentPage->getKeyCount()-1) {
			MIndexPageHdl rightPage = currentPage->getNextPage();
			latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);
			
			//能合并则合并
			if (currentPage->canMergeWith(rightPage)) {
				if (!currentPage->isOverflow()) {
					SubRecord rightPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
					NTSE_ASSERT(parentPage->findPageAssosiateKey(rightPage, &rightPageKey));//找到rightPage对应的索引键
					SubRecord leftPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
					NTSE_ASSERT(parentPage->findPageAssosiateKey(currentPage, &leftPageKey));//找到currentPage对应的索引键
					unlinkChild(scanInfo, parentPage, currentPage, rightPage, &leftPageKey, &rightPageKey);
				}
				mergePage(scanInfo, currentPage, rightPage);
				reclaimPageCnt++;
				// 当回收到足够页面时退出
				if(reclaimPageCnt == maxReclaimPages)
					break;
			} else {
				if(currentPage->isOverflow()) {
					assert(!rightPage->isOverflow());
					MIndexPageHdl rightRightPage = rightPage->getNextPage();
					unlatchPage(session, currentPage, Exclusived);
					latchPage(session, rightRightPage, Exclusived, __FILE__, __LINE__);
					currentPage = rightRightPage;
					unlatchPage(session, rightPage, Exclusived);
				} else {
					unlatchPage(session, currentPage, Exclusived);
					currentPage = rightPage;
				}
				keyNo++;
			}
		}	
		unlatchPage(session, currentPage, Exclusived);
	}
	return reclaimPageCnt;
}

/**
 * 根据键值可以唯一定位的取项操作
 * @param session   会话
 * @param key       查找键，KEY_PAD格式
 * @param rowId     OUT 索引键对应的rowId
 * @param subRecord OUT 输出要提取的键值，为NULL表示不需要提取
 * @param extractor 子记录提取器
 * @return 是否找到指定项
 */
/*
bool BLinkTree::getByUniqueKey(Session *session, const SubRecord *key, RowId *rowId, 
							   SubRecord *subRecord, SubToSubExtractor *extractor) {
	assert(KEY_PAD == key->m_format || KEY_NATURAL == key->m_format);

	MTransaction *trx = session->getTrans();
	McSavepoint msc(session->getMemoryContext());	
	MIndexScanInfo *scanInfo = generateScanInfo(session, Shared);

	if (fetchUnique(scanInfo, key)) {
		//保存查找到的索引键对应的RowId
		*rowId = scanInfo->m_assistKey->m_rowId;

// 		KeyMVInfo *keyMVInfo;
// 		keyMVInfo->m_visable = true;
// 		keyMVInfo->m_version = MIndexKeyOper::readIdxVersion(scanInfo->m_assistKey);
// 		keyMVInfo->m_delBit = MIndexKeyOper::readDelBit(scanInfo->m_assistKey);
// 		assert(0 == keyMVInfo->m_delBit);//delete bit为1的之前已经过滤掉
		
		//根据需要提取键值，释放叶页面latch
		if (NULL != subRecord) {
			assert(extractor != NULL);
			extractor->extract(scanInfo->m_assistKey, subRecord);
		}
		unlatchPage(session, scanInfo->m_currentPage, scanInfo->m_latchMode);

		return true;
	}
	return false;
}
*/

/**
 * 检查是否存在重复键，一定是当前读
 * @param session
 * @param key
 * @return 
 */
bool BLinkTree::checkDuplicate(Session *session, const SubRecord *key) {
	assert(KEY_NATURAL == key->m_format || KEY_PAD == key->m_format);
	assert(m_indexDef->m_unique);

	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint msc(mtx);
	MIndexScanInfo *scanInfo = generateScanInfo(session, Shared);

	/** 
	 * 虽然是唯一性索引，但是可能存在键值相等的多个索引项(RowId不同, 但其中最多只有一项delete bit为0，其余都为1)
	 * 所以需要设置是否允许相等标记为false
	 */
	scanInfo->m_searchFlag.setFlag(true, true, false);

	//如果外部传递进来的搜索键格式为KEY_PAD, 这里转化为内存索引的内部搜索键格式KEY_NATURAL
	scanInfo->m_searchKey = MIndexKeyOper::allocSubRecord(mtx, m_indexDef, KEY_NATURAL);
	if (KEY_NATURAL == key->m_format) {
		MIndexKeyOper::copyKey(scanInfo->m_searchKey, key);
	} else {
		RecordOper::convertKeyPN(*m_tableDef, m_indexDef, key, scanInfo->m_searchKey);
	}

	//首先定位叶页面
	scanInfo->m_currentPage = readModeLocateLeafPage(scanInfo);
	
	return findDupKey(scanInfo);
}

#ifdef TNT_INDEX_OPTIMIZE
/**
 * 查找唯一的索引项
 * @param scanInfo
 * @param key
 * @return 
 */
bool BLinkTree::fetchUnique(MIndexScanInfo *scanInfo, const SubRecord *key) {
	assert(KEY_PAD == key->m_format || KEY_NATURAL == key->m_format);

	Session *session = scanInfo->m_session;
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint msc(mtx);

	scanInfo->m_uniqueScan = true;

	/** 
	 * 虽然是唯一性索引，但是可能存在键值相等的多个索引项(RowId不同)，其中只有一项delete bit为0，其余都为1，
	 * 所以需要设置是否允许相等标记为false
	 */
	scanInfo->m_searchFlag.setFlag(true, true, false);

	//如果外部传递进来的搜索键格式为KEY_PAD, 这里转化为内存索引的内部搜索键格式KEY_NATURAL
	scanInfo->m_searchKey = MIndexKeyOper::allocSubRecord(mtx, m_indexDef, KEY_NATURAL);
	if (KEY_NATURAL == key->m_format) {
		MIndexKeyOper::copyKey(scanInfo->m_searchKey, key);
	} else {
		RecordOper::convertKeyPN(m_tableDef, key, scanInfo->m_searchKey);
	}

	//首先定位叶页面
	IDXResult result = IDX_FAIL;
	do {
		locateLeafPage(scanInfo);

		result = searchAtLeafLevel(scanInfo, &scanInfo->m_keyLocation, &scanInfo->m_searchFlag);
		if (IDX_FAIL == result) {
			unlatchPage(session, scanInfo->m_currentPage, scanInfo->m_latchMode);
			return false;
		}
	} while (IDX_RESTART == result);
	assert(IDX_SUCCESS == result);

	return true;
}

/**
 * 在叶页面中查找键值
 * @param scanInfo 扫描句柄信息
 * @param leafPageHdl 叶页面句柄
 * @param searchResult OUT 查找结果
 * @return 是否成功找到键值
 */
IDXResult BLinkTree::searchAtLeafLevel(MIndexScanInfo *scanInfo, MkeyLocation *keyLocation, 
									   const SearchFlag *searchFlag) {
	assert(MIDX_PAGE_NONE != scanInfo->m_currentPage);
	assert(scanInfo->m_currentPage->isPageLeaf());
	assert(None != getPageLatchMode(scanInfo->m_currentPage));

	MIndexSearchResult searchResult;
	bool needFetchNext = true;
	MIndexPageHdl leafPage = scanInfo->m_currentPage;

	if (likely(leafPage->getKeyCount() > 0)) {
		needFetchNext = leafPage->findKeyInLeafPage(scanInfo->m_searchKey, 
			scanInfo->m_assistKey, searchFlag, scanInfo->m_comparator, &searchResult);
		*keyLocation = searchResult.m_keyLocation;
		needFetchNext = needFetchNext && (!scanInfo->m_uniqueScan || searchResult.m_cmpResult < 0);
	}

	if (needFetchNext) {
		return shiftToNextKey(scanInfo, keyLocation, &scanInfo->m_searchFlag);
	} else {
		if (unlikely(MIndexKeyOper::isInfiniteKey(scanInfo->m_assistKey)) {
			unlatchPage(scanInfo->m_session, scanInfo->m_currentPage, scanInfo->m_latchMode);
			return IDX_FAIL;
		} else {
			return IDX_SUCCESS;
		}
	}
}

/**
 * 开始一个索引范围扫描
 * @param session 会话
 * @param key 查找键，KEY_PAD格式
 * @param forward 扫描方向
 * @param includeKey 是否是>=或<=
 * @param extractor 子记录提取器
 * @return 索引扫描句柄
 */
MIndexScanHandle* BLinkTree::beginScanFast(Session *session, const SubRecord *key, 
									  bool forward, bool includeKey) {
    assert(KEY_PAD == key->m_format || KEY_NATURAL == key->m_format);
	MIndexScanInfo *scanInfo = generateScanInfo(session, Shared);

	//外部传递进来的搜索键格式为KEY_PAD, 这里转化为内存索引的内部搜索键格式KEY_NATURAL

	if (MIndexKeyOper::isKeyValid(key)) {
		scanInfo->m_searchKey = MIndexKeyOper::allocSubRecord(session->getMemoryContext(), m_indexDef, 
			KEY_NATURAL);
		if (KEY_NATURAL == key->m_format) {
			MIndexKeyOper::copyKey(scanInfo->m_searchKey, key);
		} else {
			RecordOper::convertKeyPN(m_tableDef, key, scanInfo->m_searchKey);
		}
	} else {
		void *p = session->getMemoryContext()->alloc(sizeof(SubRecord));
		scanInfo->m_searchKey = new (p)SubRecord(KEY_NATURAL, m_indexDef->m_numCols, 
			m_indexDef->m_columns, NULL, 0, INVALID_ROW_ID);
	}

	scanInfo->m_forward = forward;
	scanInfo->m_includeKey = includeKey;
	scanInfo->m_uniqueScan = false;

	scanInfo->m_searchFlag.setFlag(forward, includeKey, true);

	byte *data = (byte*)session->getMemoryContext()->alloc(sizeof(MIndexRangeScanHandle));
	MIndexRangeScanHandle *mIdxScanHdl = new (data)MIndexRangeScanHandle();
	mIdxScanHdl->setScanInfo(scanInfo);

	return mIdxScanHdl;
}

/**
 * 获取扫描方向的下一个索引项
 * @param scanHandle 索引扫描句柄
 * @return 是否有下一项
 */
bool BLinkTree::getNextFast(MIndexScanHandle *handle) {
	assert(handle);

	MIndexRangeScanHandle *scanHandle = (MIndexRangeScanHandle*)handle;
	
	MIndexScanInfo *mIdxScanInfo = scanHandle->getScanInfo();
	mIdxScanInfo->m_searchFlag.setFlag(mIdxScanInfo->m_forward, mIdxScanInfo->m_includeKey, 
		mIdxScanInfo->m_fetchCount > 0);
	
	if (likely(!mIdxScanInfo->m_rangeFirst)) {//已经定位过叶页面
		//重新对扫描句柄中保存的当前页面加latch，判断页面类型以及更新时间戳是否发生变更，必要时重定位叶页面	
		if (latchPageIfType(mIdxScanInfo->m_session, mIdxScanInfo->m_currentPage, mIdxScanInfo->m_latchMode)) {

			if ((mIdxScanInfo->m_currentPage->getTimeStamp() == mIdxScanInfo->m_pageTimeStamp//页面没有发生变更
				|| !judgeIfNeedRestart(mIdxScanInfo))) {//查找键还在原页面中且在中部，则还是可以在原页面查找!
					IDXResult result = shiftToNextKey(mIdxScanInfo, &mIdxScanInfo->m_keyLocation, 
						&mIdxScanInfo->m_searchFlag);
							if (IDX_SUCCESS == result) {
								goto __succeed;
							} else if (IDX_FAIL == result) {
								goto __failed;
							}
							assert(IDX_RESTART == result);
			}
			unlatchPage(mIdxScanInfo->m_session, mIdxScanInfo->m_currentPage, mIdxScanInfo->m_latchMode);
		}
	}
	
__locateLeaf:
	//需要定位叶页面
	locateLeafPage(mIdxScanInfo);

	IDX_SWITCH_AND_GOTO(searchAtLeafLevel(mIdxScanInfo, &mIdxScanInfo->m_keyLocation, 
		&mIdxScanInfo->m_searchFlag), __locateLeaf, __failed);

__succeed:
	//if (mIdxScanInfo->m_trx->getLockType() != TL_NO) {
		//TODO: 加事务级行锁!
		///
	//}

	//保存查找结果
	MIndexKeyOper::copyKeyAndExt(mIdxScanInfo->m_searchKey, mIdxScanInfo->m_assistKey, true);
	scanHandle->saveKey(mIdxScanInfo->m_assistKey);

	//保存页面时间戳
	mIdxScanInfo->m_pageTimeStamp = mIdxScanInfo->m_currentPage->getTimeStamp();
	++mIdxScanInfo->m_fetchCount;
	mIdxScanInfo->m_rangeFirst = false;

	//释放页面latch
	unlatchPage(mIdxScanInfo->m_session, mIdxScanInfo->m_currentPage, mIdxScanInfo->m_latchMode);
	return true;

__failed:
	mIdxScanInfo->m_hasNext = false;
	return false;
}

/**
 * 结束索引范围扫描
 * @param scanHandle
 */
void BLinkTree::endScanFast(MIndexScanHandle *scanHandle) {
	assert(scanHandle);
	UNREFERENCED_PARAMETER(scanHandle);
}

#endif


///////////////////////////////////////////////////////////////

/**
 * 开启一个BLinkTree索引范围扫描
 * @param session 会话
 * @param key     查找键
 * @param forward 是否是前向查找
 * @param includeKey 是否包含键值(大于等于或小于等于)
 * @return 内存索引扫描句柄
 */
MIndexScanHandle* BLinkTree::beginScan(Session *session, const SubRecord *key, bool forward, 
									   bool includeKey, LockMode ntseRlMode, RowLockHandle **rowHdl, 
									   TLockMode trxLockMode) {
    assert(!key || KEY_PAD == key->m_format || KEY_NATURAL == key->m_format);
	assert(ntseRlMode == None || rowHdl != NULL);

	void *memory = session->getMemoryContext()->alloc(sizeof(MIndexScanInfoExt));
	MIndexScanInfoExt *scanInfo = new (memory) MIndexScanInfoExt(session, session->getTrans(),
		*m_tableDef, m_indexDef, Shared, RecordOper::compareKeyNN, ntseRlMode, trxLockMode);
	scanInfo->m_rowHandle = rowHdl;
	if (scanInfo->m_rowHandle)
		*(scanInfo->m_rowHandle) = NULL;

	//外部传递进来的搜索键格式为KEY_PAD, 这里转化为内存索引的内部搜索键格式KEY_NATURAL
	if (key != NULL) {
		assert(MIndexKeyOper::isKeyValid(key));
		scanInfo->m_searchKey = MIndexKeyOper::allocSubRecord(session->getMemoryContext(), 
			key->m_numCols, key->m_columns,  KEY_NATURAL, m_indexDef->m_maxKeySize);
		scanInfo->m_searchKeyBufSize = scanInfo->m_searchKey->m_size;

		if (KEY_NATURAL == key->m_format) {
			MIndexKeyOper::copyKey(scanInfo->m_searchKey, key);
		} else {
			RecordOper::convertKeyPN(*m_tableDef, m_indexDef, key, scanInfo->m_searchKey);
		}
	} else {
		scanInfo->m_searchKey = MIndexKeyOper::allocSubRecord(session->getMemoryContext(), 
			m_indexDef, KEY_NATURAL);
		scanInfo->m_searchKey->m_rowId = 0;
		scanInfo->m_searchKeyBufSize = scanInfo->m_searchKey->m_size;
		scanInfo->m_searchKey->m_size = 0;
	}
	assert(scanInfo->m_searchKeyBufSize > 0);

	scanInfo->m_forward = forward;
	scanInfo->m_includeKey = includeKey;
	scanInfo->m_uniqueScan = false;
	scanInfo->m_tableDef = *m_tableDef;
	scanInfo->m_tmpRangeFirst = true;

	scanInfo->m_searchFlag.setFlag(forward, includeKey, true);

	byte *data = (byte*)session->getMemoryContext()->alloc(sizeof(MIndexRangeScanHandle));
	MIndexRangeScanHandle *mIdxScanHdl = new (data)MIndexRangeScanHandle();
	mIdxScanHdl->setScanInfo(scanInfo);
	
	//统计信息
	m_indexStatus.m_numScans++;

	if(!forward)
		m_indexStatus.m_backwardScans++;


	return mIdxScanHdl;
}

/**
 * 获取范围扫描下一项
 * @param handle 内存范围扫描句柄
 * @return 是否存在下一项
 */
bool BLinkTree::getNext(MIndexScanHandle *handle) throw(NtseException) {
	assert(handle);

	MIndexRangeScanHandle *scanHandle = (MIndexRangeScanHandle*)handle;	
	MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHandle->getScanInfo();
	Session *session = scanInfo->m_session;

	//初始化当前读到的信息
	scanInfo->m_readPage = scanInfo->m_currentPage;
	scanInfo->m_readPageTimeStamp = scanInfo->m_pageTimeStamp;
	scanInfo->m_rangeFirst = scanInfo->m_tmpRangeFirst;
	scanInfo->m_readKeyLocation = scanInfo->m_keyLocation;

	assert(scanInfo->m_searchKey->m_format == KEY_NATURAL);

	scanHandle->unlatchLastRow();

	scanInfo->m_searchFlag.setFlag(scanInfo->m_forward, scanInfo->m_includeKey, 
		!scanInfo->m_rangeFirst);
	
	if (likely(!scanInfo->m_rangeFirst && scanInfo->m_readPage != MIDX_PAGE_NONE)) {//已经定位过叶页面
		//重新对扫描句柄中保存的当前页面加latch，判断页面类型以及更新时间戳是否发生变更，必要时重定位叶页面	
		if (latchPageIfType(session, scanInfo->m_readPage, scanInfo->m_latchMode)) {
			if (checkHandlePage(scanInfo, scanInfo->m_forceSearchPage)) {
					IDX_SWITCH_AND_GOTO(shiftToNextKey(scanInfo, &scanInfo->m_readKeyLocation, 
						&scanInfo->m_searchFlag, true), __locateLeaf, __failed);
					goto __succeed;
			}
			unlatchPage(session, scanInfo->m_readPage, scanInfo->m_latchMode);
		}
	}
	
__locateLeaf:
	//需要定位叶页面
	scanInfo->m_readPage = locateLeafPage(scanInfo);
	//如果内存不足
	//如果内存不足
	if(scanInfo->m_readPage == MIDX_PAGE_NONE)
		goto __locateLeaf;

	IDX_SWITCH_AND_GOTO(searchAtLeafLevelSecond(scanInfo, &scanInfo->m_readKeyLocation, 
		&scanInfo->m_searchFlag), __locateLeaf, __failed);

__succeed:
	// 需要的话，加NTSE底层表的行锁以及TNT事务锁
	IDX_SWITCH_AND_GOTO(tryTrxLockHoldingLatch(scanInfo, 
		scanInfo->m_readPage, scanInfo->m_assistKey), __locateLeaf, __failed);

	//保存页面时间戳
	//scanInfo->m_pageTimeStamp = scanInfo->m_currentPage->getTimeStamp();
	++scanInfo->m_fetchCount;
	scanInfo->m_rangeFirst = false;
	scanInfo->m_forceSearchPage = false;

	MIndexKeyOper::copyKeyAndExt(scanInfo->m_readKey, scanInfo->m_assistKey, true);
	scanHandle->saveKey(scanInfo->m_readKey);

	//释放页面latch
	unlatchPage(session, scanInfo->m_readPage, scanInfo->m_latchMode);

	//统计信息
	m_indexStatus.m_rowsScanned++;
	if(!scanInfo->m_forward)
		m_indexStatus.m_rowsBScanned++;

	return true;

__failed:
	scanInfo->m_hasNext = false;
	return false;
}

/**
 * 结束BLinkTree范围扫描
 * @param scanHandle
 * @return 
 */
void BLinkTree::endScan(MIndexScanHandle *handle) {
	assert(handle);
	MIndexRangeScanHandle *scanHandle = (MIndexRangeScanHandle*)handle;	
	scanHandle->unlatchLastRow();
}

/**
 * 在持有页面latch的情况下，尝试加事务级行锁
 * 
 * @pre 要加事务锁的记录所在的内存索引页面的latch已经加上
 * @param info 内存索引扫描句柄信息
 * @param pageHdl 要加事务锁的记录所在的内存索引页面
 * @param key 要加事务锁的记录
 * @throw NtseException 锁表空间不足等
 * @return 三阶段加锁成功返回IDX_SUCCESS，此时持有页面latch，NTSE行锁以及事务锁；
 *         加锁失败返回IDX_RESTART，由上层重启整个查找过程，此时仍持有页面latch
 */
IDXResult BLinkTree::tryTrxLockHoldingLatch(MIndexScanInfoExt *info, MIndexPageHdl pageHdl, 
											const SubRecord *key) throw(NtseException) {
	Session *session = info->m_session;
	RowId keyRowId = key->m_rowId;// 运行到这里，下一项的键值肯定保存在record指向的m_cKey1当中

	if (info->m_ntseRlMode != None) {
		// 检查NTSE行锁是否已经加上
		assert(info->m_ntseRlMode == Shared);
		if (!session->isRowLocked((*m_tableDef)->m_id, keyRowId, info->m_ntseRlMode)) {
			SYNCHERE(SP_MEM_INDEX_TRYLOCK);
			if ((*info->m_rowHandle = TRY_LOCK_ROW(session, (*m_tableDef)->m_id, keyRowId, 
				info->m_ntseRlMode)) == NULL) {
					u64 oldTs = pageHdl->getTimeStamp();
					unlatchPage(session, pageHdl, info->m_latchMode);

					*info->m_rowHandle = LOCK_ROW(session, (*m_tableDef)->m_id, keyRowId, info->m_ntseRlMode);

					if (latchPageIfType(session, pageHdl, info->m_latchMode)) {
						if (pageHdl->getTimeStamp() == oldTs) 
							return IDX_SUCCESS;
						unlatchPage(session, pageHdl, info->m_latchMode);
					}

					info->m_currentPage = MIDX_PAGE_NONE;
					session->unlockRow(info->m_rowHandle);
					return IDX_RESTART;
			}
		}
	}
	return IDX_SUCCESS;	
}

/**
 * 判断在扫描句柄中记录的叶页面信息是否有效
 * @param info
 * @param forceSearchPage
 * @return 
 */
bool BLinkTree::checkHandlePage(MIndexScanInfoExt *info, bool forceSearchPage) const {
	MIndexPageHdl leafPageHld = info->m_currentPage;
	assert(None != getPageLatchMode(leafPageHld));
	SYNCHERE(SP_MEM_INDEX_CHECKPAGE);
	if (leafPageHld->getTimeStamp() != info->m_pageTimeStamp || forceSearchPage) {
		if (unlikely(!leafPageHld->isPageLeaf())) {
			SYNCHERE(SP_MEM_INDEX_CHECKPAGE1);
			return false;
		}

		if (leafPageHld->getKeyCount() > 0 && leafPageHld->compareWithHighKey(
			info->m_searchKey, info->m_assistKey, info->m_comparator, &info->m_searchFlag) <= 0) {
				MIndexSearchResult searchResult;
				leafPageHld->findKeyInLeafPage(info->m_searchKey, info->m_assistKey, &SearchFlag::DEFAULT_FLAG, 
					info->m_comparator, &searchResult);
				
				//u16 keyNo = searchResult.m_keyLocation.m_keyNo;
				if (0 == searchResult.m_cmpResult //找到相等索引项
					/*&& keyNo < (leafPageHld->getKeyCount() - 1) && keyNo > 0*/) {//在页面中间
						info->m_pageTimeStamp = leafPageHld->getTimeStamp();
						info->m_keyLocation = searchResult.m_keyLocation;
						info->m_readKeyLocation = searchResult.m_keyLocation;
						return true;
				}
		}
		return false;
	}
	return true;
}

/**
 * 获得索引ID号
 * @return 
 */
u8 BLinkTree::getIndexId() const {
	return m_indexId;
}

/**
 * 获得树高度
 * @return 
 */
u8 BLinkTree::getHeight() const {
	return m_rootPage->getPageLevel() + 1;
}

/**
 * 获得根页面
 * @return 
 */
MIndexPageHdl BLinkTree::getRootPage() const {
	return m_rootPage;
}

/**
 * 生成扫描句柄信息
 * @param session 会话
 * @param traversalMode 遍历模式
 * @return 扫描句柄信息
 */
MIndexScanInfo* BLinkTree::generateScanInfo(Session *session, LockMode latchMode) {
	void *memory = session->getMemoryContext()->alloc(sizeof(MIndexScanInfo));
	MIndexScanInfo *scanInfo = new (memory) MIndexScanInfo(session, session->getTrans(),
		*m_tableDef, m_indexDef, latchMode, RecordOper::compareKeyNN);
	return scanInfo;
}


/**
 * 判断两个页面哪个包含查找键
 * @param scanInfo
 * @param leftPage
 * @param rightPage
 * @return 
 */
MIndexPageHdl BLinkTree::switchToCoverPage(MIndexScanInfo *scanInfo, MIndexPageHdl leftPage,
										 MIndexPageHdl rightPage) {
	assert(scanInfo->m_latchMode == getPageLatchMode(leftPage));
	assert(scanInfo->m_latchMode == getPageLatchMode(rightPage));

	Session *session = scanInfo->m_session;

	if (leftPage->compareWithHighKey(scanInfo->m_searchKey, scanInfo->m_assistKey, 
		scanInfo->m_comparator, &scanInfo->m_searchFlag) <= 0) {
		unlatchPage(session, rightPage, scanInfo->m_latchMode);
		return leftPage;
	} else {
		unlatchPage(session, leftPage, scanInfo->m_latchMode);
		return rightPage;
	}
}

/**
 * 在linkChild中判断两个页面哪个包含待link的子页面,由linkChild层去判断放latch
 * @param scanInfo
 * @param leftPage
 * @param rightPage
 * @return 
 */
MIndexPageHdl BLinkTree::switchToCoverPageForLinkChild(MIndexScanInfo *scanInfo, MIndexPageHdl leftPage,
										 MIndexPageHdl rightPage, const SubRecord *includeKey) {
	assert(scanInfo->m_latchMode == getPageLatchMode(leftPage));
	assert(scanInfo->m_latchMode == getPageLatchMode(rightPage));

	if (leftPage->compareWithHighKey(includeKey, scanInfo->m_assistKey, 
		scanInfo->m_comparator, &SearchFlag::DEFAULT_FLAG) <= 0) {
		return leftPage;
	} else {
		return rightPage;
	}
}


/**
 * 定位叶页面
 * @param scanInfo
 * @return 
 */
MIndexPageHdl BLinkTree::locateLeafPage(MIndexScanInfo *scanInfo) {
	return READMODE == scanInfo->m_traversalMode ? 
		readModeLocateLeafPage(scanInfo) : updateModeLocateLeafPage(scanInfo);
}

/**
 * 只读模式定位叶页面
 * @param scanInfo 索引扫描信息
 * @return 索引为空时返回MIDX_PAGE_NONE, 否则返回叶页面
 */
MIndexPageHdl BLinkTree::readModeLocateLeafPage(MIndexScanInfo *scanInfo) {
	const LockMode latchMode = Shared;
	MIndexPageHdl currentPage = m_rootPage;
	Session *session = scanInfo->m_session;

	latchPage(session, currentPage, latchMode, __FILE__, __LINE__);
	assert(!currentPage->isPageEmpty());

	do {
		assert(MIDX_PAGE_NONE != currentPage);
		if (currentPage->compareWithHighKey(scanInfo->m_searchKey, scanInfo->m_assistKey, 
			scanInfo->m_comparator, &scanInfo->m_searchFlag) > 0) {
				//如果查找键值大于high-key，转到右链接页面
				assert(currentPage->isOverflow());
				MIndexPageHdl rightPage = currentPage->getNextPage();
				assert(MIDX_PAGE_NONE != rightPage);
				latchPage(session, rightPage, latchMode, __FILE__, __LINE__);
				unlatchPage(session, currentPage, latchMode);
				assert(rightPage->compareWithHighKey(scanInfo->m_searchKey, scanInfo->m_assistKey, 
					scanInfo->m_comparator, &scanInfo->m_searchFlag) <= 0);

				currentPage = rightPage;
		}
		if (!currentPage->isPageLeaf()) {//非叶页面
			MIndexPageHdl childPage = currentPage->findChildPage(scanInfo->m_searchKey, 
				scanInfo->m_assistKey, &scanInfo->m_searchFlag, scanInfo->m_comparator);
			assert(MIDX_PAGE_NONE != childPage);

			latchPage(session, childPage, latchMode, __FILE__, __LINE__);
			unlatchPage(session, currentPage, latchMode);
			currentPage = childPage;
		} else {
			break;
		}
	} while (true);

	assert(m_pageManager->isPageLatched(currentPage, latchMode));

	return currentPage;
}

/**
 * 合并或重分布两个页面的键值
 * @param scanInfo
 * @param parentPage  IN/OUT
 * @param leftPage
 * @param rightPage
 * @param needUnlink
 * @param rightPageKey
 * @return 
 */
MIndexPageHdl BLinkTree::mergeOrRedistribute(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage,
											 MIndexPageHdl leftPage, MIndexPageHdl rightPage,
											 SubRecord *leftPageKey/*=NULL*/,
											 SubRecord *rightPageKey/*=NULL*/) {
    assert(Exclusived == getPageLatchMode(*parentPage));
	assert(Exclusived == getPageLatchMode(leftPage));
	assert(Exclusived == getPageLatchMode(rightPage));
	
	goto NORMAL;

	//fprintf(stderr, "id:%d, mergeOrRedistribute: %x, %x, %x\n", scanInfo->m_session->getId(), 
	//	parentPage, leftPage, rightPage);

	if (leftPage->canMergeWith(rightPage)) {
		if (!leftPage->isOverflow()) {
			unlinkChild(scanInfo, *parentPage, leftPage, rightPage, leftPageKey, rightPageKey);
		}
		return mergePage(scanInfo, leftPage, rightPage);
	} else {
		if (needRedistribute(leftPage, rightPage)) {
			if (!leftPage->isOverflow()) {
				unlinkChild(scanInfo, *parentPage, leftPage, rightPage, leftPageKey, rightPageKey);
			}
			return redistributeTwoPage(scanInfo, parentPage, leftPage, rightPage);
		} else {
NORMAL:
			if (leftPage->isOverflow()) {
				*parentPage = linkChild(scanInfo, *parentPage, leftPage, rightPage);
				//如果内存不够，分裂不出新页面, 释放所有latch 后返回NULL，重启算法
				if(*parentPage == MIDX_PAGE_NONE) {
					return MIDX_PAGE_NONE;
				}
			}
			return switchToCoverPage(scanInfo, leftPage, rightPage);
		}
	}
}

/**
 * 判断是否需要重新分布两个页面的键值
 * @param leftPage
 * @param rightPage
 * @return 
 */
bool BLinkTree::needRedistribute(MIndexPageHdl leftPage, MIndexPageHdl rightPage) const {
	int margin = (int)leftPage->getFreeSpace() - (int)rightPage->getFreeSpace();
	return abs(margin) > (MIN(leftPage->getFreeSpace(), rightPage->getFreeSpace()) / 4);
}

/**
 * 重新分布两个页面的键值
 * @param scanInfo
 * @param parentPage IN/OUT
 * @param leftPage
 * @param rightPage
 * @return 
 */
MIndexPageHdl BLinkTree::redistributeTwoPage(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
									MIndexPageHdl leftPage, MIndexPageHdl rightPage) {
	assert(Exclusived == getPageLatchMode(*parentPage));
	assert(Exclusived == getPageLatchMode(leftPage));
	assert(Exclusived == getPageLatchMode(rightPage));
	assert(leftPage->isOverflow());
	assert(!rightPage->isOverflow());
	//fprintf(stderr, "id:%d, redistributeTwoPage:%x, %x\n", scanInfo->m_session->getId(), leftPage, rightPage);

	leftPage->redistribute(rightPage);
	leftPage->incrTimeStamp();
	rightPage->incrTimeStamp();
	VERIFY_TNT_MINDEX_PAGE(leftPage);
	VERIFY_TNT_MINDEX_PAGE(rightPage);
	
	*parentPage = linkChild(scanInfo, *parentPage, leftPage, rightPage);
	if(*parentPage == MIDX_PAGE_NONE) {
		return MIDX_PAGE_NONE;
	}
	//更新统计信息	
	m_indexStatus.m_numRedistribute++;

	return switchToCoverPage(scanInfo, leftPage, rightPage);
}

/**
 * 消除溢出的链接页面
 * @param scanInfo
 * @param parentPage IN/OUT
 * @param overflowPage
 * @param needUnLatchParent 是否需要放parent节点的latch updateModeTraverse需要放，而purge时repairLevel时不需要放
 * @return 
 */
MIndexPageHdl BLinkTree::repairPageOverflow(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
								   MIndexPageHdl overflowPage) {
	assert(Exclusived == getPageLatchMode(*parentPage));
	assert(Exclusived == getPageLatchMode(overflowPage));

	//fprintf(stderr, "id:%d, repairPageOverflow: %x, %x\n", scanInfo->m_session->getId(), parentPage, overflowPage);

	Session *session = scanInfo->m_session;
	MIndexPageHdl currentPage = *parentPage;
	MIndexPageHdl rightPage = overflowPage->getNextPage();

	latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);

	//此时同时持有3个页面的latch, current-X, child-X, right-X
	*parentPage = linkChild(scanInfo, currentPage, overflowPage, rightPage);

	//如果内存不足,释放两个子页面latch
	if(*parentPage == MIDX_PAGE_NONE) {
		return MIDX_PAGE_NONE;
	}

	//统计信息
	m_indexStatus.m_numRepairOverflow++;
	return switchToCoverPage(scanInfo, overflowPage, rightPage);
}

/**
 *  修复下溢的页面(下溢页面是其父页面的最后一个子页面)
 * @param scanInfo
 * @param parentPage
 * @param childPage
 * @return 
 */
MIndexPageHdl BLinkTree::repairRightMostChild(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
											  MIndexPageHdl childPage) {
  	assert(Exclusived == getPageLatchMode(*parentPage));
	assert(Exclusived == getPageLatchMode(childPage));
	Session *session = scanInfo->m_session;

	//此时子页面不会有溢出页面
	assert(!childPage->isOverflow());
	//childPage没有溢出页面，将childPage和它左边的页面合并
	if ((*parentPage)->getKeyCount() > 1) {
		MkeyLocation location;
		MkeyLocation prevLocation;
		u16 keyNo = 0;
		//找到父节点中的左边指针指向的页面
		SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
		NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//找到childPage对应的索引键			
		NTSE_ASSERT(0 == (*parentPage)->binarySearch(&childPageKey, scanInfo->m_assistKey, 
			&keyNo, &SearchFlag::DEFAULT_FLAG, scanInfo->m_comparator));
		assert(keyNo > 0);
		(*parentPage)->getKeyLocationByNo(keyNo, &location);

		//leftPageKey存储父页面中指向childPage左页面的键值
		SubRecord leftPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
		NTSE_ASSERT((*parentPage)->getPrevKey(&location, &prevLocation, &leftPageKey));
		MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&leftPageKey);
		
		//释放原页面latch
		u64 oldStamp = childPage->getTimeStamp();
		unlatchPage(session, childPage, Exclusived);

		latchPage(session, leftPage, Exclusived, __FILE__, __LINE__);
		

		//从父节点定位到的左节点并不是真正的左节点（溢出）
		MIndexPageHdl middlePage = MIDX_PAGE_NONE;
		if(leftPage->isOverflow()) {
			middlePage = leftPage->getNextPage();
			latchPage(session, middlePage, Exclusived, __FILE__, __LINE__);
			//FIXME：此处要考虑父页面分裂的情况，但是由于目前索引大小为8K，leftPage, middlePage和childPage一定属于同一个父节点
			*parentPage = linkChild(scanInfo, *parentPage, leftPage, middlePage);
			//如果分裂失败
			if(*parentPage == NULL){
				return MIDX_PAGE_NONE;
			}

			unlatchPage(session, leftPage, Exclusived);
			assert(! middlePage->isOverflow());

			//重新获取真正的当前页面左页面在父节点对应的键值
			SubRecord middlePageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
			middlePage->getHighKey(&middlePageKey);

			assert(middlePage->getNextPage() == childPage);
			latchPage(session, childPage, Exclusived, __FILE__, __LINE__);

			if(childPage->isUnderFlow()) {
				SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
				NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//找到childPage对应的索引键			
				return mergeOrRedistribute(scanInfo, parentPage, middlePage, childPage, 
				&middlePageKey, &childPageKey);
			} else{
				unlatchPage(session, middlePage, Exclusived);
				return childPage;
			}
		} else {
			assert(leftPage->getNextPage() == childPage);
			latchPage(session, childPage, Exclusived, __FILE__, __LINE__);
			//如果加回latch后发现时间戳不同，重启算法
			if(childPage->getTimeStamp() != oldStamp) {
				unlatchPage(session, *parentPage, Exclusived);
				unlatchPage(session, leftPage, Exclusived);
				unlatchPage(session, childPage, Exclusived);
				return MIDX_PAGE_NONE;
			}
			
			if(childPage->isUnderFlow()) {
				SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
				NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//找到childPage对应的索引键			
				return mergeOrRedistribute(scanInfo, parentPage, leftPage, childPage, 
				&leftPageKey, &childPageKey);
			} else {
				unlatchPage(session, leftPage, Exclusived);
				return childPage;
			}
		}
	}
	return childPage;
	
}

/**
 * 修复下溢的页面(下溢页面不是其父页面的最后一个子页面)
 * @param scanInfo
 * @param parentPage
 * @param childPage
 * @return 
 */
MIndexPageHdl BLinkTree::repairNotRightMostChild(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
												  MIndexPageHdl childPage) {
	assert(Exclusived == getPageLatchMode(*parentPage));
	assert(Exclusived == getPageLatchMode(childPage));

	Session *session = scanInfo->m_session;
	MIndexPageHdl rightPage = childPage->getNextPage();
	assert(MIDX_PAGE_NONE != rightPage);

	if (childPage->isOverflow()) {//rightPage是childPage的溢出页面
		latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);
		return mergeOrRedistribute(scanInfo, parentPage, childPage, rightPage);
	} else {//rightPage不是溢出页面
		SubRecord rightPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);

		NTSE_ASSERT((*parentPage)->findPageAssosiateKey(rightPage, &rightPageKey));//找到rightPage对应的索引键			
		latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);

		if (!rightPage->isOverflow()) {	
			SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
			NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//找到childPage对应的索引键			
			return mergeOrRedistribute(scanInfo, parentPage, childPage, rightPage, 
				&childPageKey, &rightPageKey);
		} else {//rightPage有溢出页面
			MIndexPageHdl sPage = rightPage->getNextPage();
			assert(MIDX_PAGE_NONE != sPage);
			assert(!sPage->isOverflow());
	
			latchPage(session, sPage, Exclusived, __FILE__, __LINE__);
			
			MIndexPageHdl rightPageParentPage = linkChild(scanInfo, *parentPage, rightPage, sPage);
		//	*parentPage = linkChild(scanInfo, *parentPage, rightPage, sPage);
			//如果内存不足分裂失败
			if(rightPageParentPage == MIDX_PAGE_NONE) {
				unlatchPage(session, childPage, Exclusived);
				return MIDX_PAGE_NONE;
			}
			unlatchPage(session, sPage, Exclusived);
			
			SYNCHERE(SP_MEM_INDEX_REPAIRNOTRIGMOST);
			//判断分裂后，rightPage的父节点已经不是原来的parentPage，那么重启算法
			if (*parentPage != rightPageParentPage) {
					unlatchPage(session, rightPageParentPage, Exclusived);
					unlatchPage(session, childPage, Exclusived);
					unlatchPage(session, rightPage, Exclusived);
					SYNCHERE(SP_MEM_INDEX_REPAIRNOTRIGMOST1);
					return MIDX_PAGE_NONE;
			}
			
			rightPage->getHighKey(&rightPageKey);
			SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
			NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//找到childPage对应的索引键			

			return mergeOrRedistribute(scanInfo, parentPage, childPage, rightPage, 
				&childPageKey, &rightPageKey);
		}
	}
}

/**
 * 修复下溢的页面
 * @param scanInfo
 * @param parentPage
 * @param childPage
 * @return 
 */
MIndexPageHdl BLinkTree::repairPageUnderFlow(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
											 MIndexPageHdl childPage) {
	assert(Exclusived == getPageLatchMode(*parentPage));
	assert(Exclusived == getPageLatchMode(childPage));

	//统计信息
	m_indexStatus.m_numRepairUnderflow++;
	
	VERIFY_TNT_MINDEX_PAGE(*parentPage);
	VERIFY_TNT_MINDEX_PAGE(childPage);

	SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
	childPage->getHighKey(&childPageKey);
	if ((*parentPage)->compareWithHighKey(&childPageKey, scanInfo->m_assistKey, scanInfo->m_comparator, 
		&SearchFlag::DEFAULT_FLAG) < 0) {//childPage不是parentPage的最右子页面
			return repairNotRightMostChild(scanInfo, parentPage, childPage);
	} else {//childPage是parentPage的最右子页面
		NTSE_ASSERT((*parentPage)->compareWithHighKey(&childPageKey, scanInfo->m_assistKey, scanInfo->m_comparator, 
			&SearchFlag::DEFAULT_FLAG) == 0);
		return repairRightMostChild(scanInfo, parentPage, childPage);
	}
}

/**
 * 更新模式定位叶页面
 * @param scanInfo 扫描句柄信息
 * @return 索引为空时返回MIDX_PAGE_NONE, 否则返回叶页面
 */
MIndexPageHdl BLinkTree::updateModeLocateLeafPage(MIndexScanInfo *scanInfo) {
	return updateModeTraverse(scanInfo, 0);
}

/**
 * 更新模式定位到指定的层
 * @param scanInfo 扫描句柄信息
 * @param level 指定的层数
 * @return 索引为空或指定层数超过树高时返回MIDX_PAGE_NONE, 否则返回定位到的那一层的页面
 */
MIndexPageHdl BLinkTree::updateModeTraverse(MIndexScanInfo *scanInfo, u8 level) {
	MemoryContext *memCtx = scanInfo->m_session->getMemoryContext();
	u64 savePoint = memCtx->setSavepoint();
__restart:
	Session *session = scanInfo->m_session;
	MIndexPageHdl currentPage = m_rootPage;
	MIndexPageHdl childPage = MIDX_PAGE_NONE;

	latchPage(session, currentPage, Exclusived, __FILE__, __LINE__);
	assert(!currentPage->isPageEmpty());

	if (unlikely(currentPage->getPageLevel() < level)) {
		unlatchPage(session, currentPage, Exclusived);
		return MIDX_PAGE_NONE;
	}

	//判断根页面是否有溢出页面，以决定是否增加树高度
	if (unlikely(currentPage->getNextPage() != MIDX_PAGE_NONE)) {
		latchPage(session, currentPage->getNextPage(), Exclusived, __FILE__, __LINE__);
		currentPage = increaseTreeHeight(scanInfo);
		//内存不够，重置内存保存点重启算法
		if(currentPage == MIDX_PAGE_NONE) {
			memCtx->resetToSavepoint(savePoint);
			//统计信息
			m_indexStatus.m_numRestarts++;
			goto __restart;
		}
	}

	while (currentPage->getPageLevel() > level) {
		childPage = currentPage->findChildPage(scanInfo->m_searchKey, scanInfo->m_assistKey, 
			&scanInfo->m_searchFlag, scanInfo->m_comparator);
		assert(MIDX_PAGE_NONE != childPage);

		latchPage(session, childPage, Exclusived, __FILE__, __LINE__);

		if (unlikely(childPage->isUnderFlow())) {
			childPage = repairPageUnderFlow(scanInfo, &currentPage, childPage);
			//如果内存不足，或者需要重启算法时
			if (unlikely(MIDX_PAGE_NONE == childPage)) {
				memCtx->resetToSavepoint(savePoint);
				//统计信息
				m_indexStatus.m_numRestarts++;
				goto __restart; 
			}

#ifdef NTSE_UNIT_TEST
			assert(childPage->compareWithHighKey(scanInfo->m_searchKey, scanInfo->m_assistKey, 
				scanInfo->m_comparator, &scanInfo->m_searchFlag) <= 0);
#endif
			unlatchPage(session, currentPage, Exclusived);
			currentPage = childPage;
		} else {
			if (childPage->isOverflow()) {
				//此时持有溢出页面的父结点的x-latch, 消除溢出页面
				childPage = repairPageOverflow(scanInfo, &currentPage, childPage);
				//如果内存不足
				if(childPage == MIDX_PAGE_NONE) {
					memCtx->resetToSavepoint(savePoint);
					//统计信息
					m_indexStatus.m_numRestarts++;
					goto __restart;
				}

#ifdef NTSE_UNIT_TEST
				assert(currentPage->compareWithHighKey(scanInfo->m_searchKey, 
					scanInfo->m_assistKey, scanInfo->m_comparator, &scanInfo->m_searchFlag) <= 0);
#endif
			} 
			unlatchPage(session, currentPage, Exclusived);
			currentPage = childPage;
			
		}
	}

	assert(m_pageManager->isPageLatched(currentPage, Exclusived));
	return currentPage;
}

/**
 * 从定位到的叶页面开始往后查找重复键值
 * @param scanInfo
 * @return 
 */
bool BLinkTree::findDupKey(MIndexScanInfo *scanInfo) {
	assert(scanInfo->m_uniqueScan);
	assert(scanInfo->m_searchFlag.isForward());

	MIndexSearchResult searchResult;
	bool needFetchNext = true;
	MIndexPageHdl leafPage = scanInfo->m_currentPage;

	if (likely(leafPage->getKeyCount() > 0)) {
		needFetchNext = leafPage->findKeyInLeafPage(scanInfo->m_searchKey, 
			scanInfo->m_assistKey, &scanInfo->m_searchFlag, scanInfo->m_comparator, &searchResult);
		scanInfo->m_keyLocation = searchResult.m_keyLocation;
		needFetchNext = needFetchNext && (searchResult.m_cmpResult < 0);
	}

	return loopForDupKey(scanInfo, &scanInfo->m_keyLocation, needFetchNext);
}

/**
 * 从指定的位置开始逐项查找重复键值
 * @param scanInfo    扫描句柄信息
 * @param keyLocation INOUT 如果fetchNext为false，则从此位置开始查找；否则从此位置的下一个索引键开始查找
                      输出为下一个可见索引键的位置
 * @param fetchNext   是否从keyLocation的下一个索引键开始查找
 * @param isDuplicate OUT 是否找到重复项
 * @return 成功找到下一项返回IDX_SUCCESS，到达索引边界返回IDX_FAIL，需要重启返回IDX_RESTART
 */
bool BLinkTree::loopForDupKey(MIndexScanInfo *scanInfo, MkeyLocation *keyLocation, bool fetchNext) {
	bool needFetchNext = fetchNext;
	bool isDuplicate = false;

	for ( ; ; ) {
		if (needFetchNext) {
			IDXResult result = shiftToForwardKey(scanInfo, keyLocation);
			if (unlikely(IDX_SUCCESS != result)) {
				assert(IDX_FAIL == result);
				return false;
			}
		} else {
			//根据索引键位置获取键值
			scanInfo->m_currentPage->getKeyByLocation(keyLocation, scanInfo->m_assistKey);
			if (unlikely(MIndexKeyOper::isInfiniteKey(scanInfo->m_assistKey))) {
				//到达无限大键值，没有匹配的索引键
				break;
			}
		}

		int cmp = RecordOper::compareKeyNN(*m_tableDef, scanInfo->m_searchKey, 
			scanInfo->m_assistKey, m_indexDef);
		if (cmp == 0) {
			if (0 == MIndexKeyOper::readDelBit(scanInfo->m_assistKey)) {
				isDuplicate = true;
				break;
			}
		} else if (cmp > 0) 
			break;

		needFetchNext = true;
	}

	unlatchPage(scanInfo->m_session, scanInfo->m_currentPage, scanInfo->m_latchMode);

	return isDuplicate;
}

/**
 * 在叶页面查找索引键改进版本
 * @param scanInfo 内存索引扫描句柄信息
 * @param keyLocation OUT 定位到的键值位置
 * @param searchFlag 查找标志
 * @return IDX_FAIL表示不存在，IDX_RESTART表示需要重启，IDX_SUCCESS表示查找成功
 */
IDXResult BLinkTree::searchAtLeafLevelSecond(MIndexScanInfo *scanInfo, MkeyLocation *keyLocation, 
											 const SearchFlag *searchFlag) {
	MIndexSearchResult searchResult;
	bool needFetchNext = true;
	MIndexScanInfoExt *infoExt = (MIndexScanInfoExt*)scanInfo;
	MIndexPageHdl leafPage = infoExt->m_readPage;
	assert(MIDX_PAGE_NONE != leafPage);
	assert(leafPage->isPageLeaf());
	assert(None != getPageLatchMode(leafPage));

	if (likely(leafPage->getKeyCount() > 0)) {
		needFetchNext = leafPage->findKeyInLeafPage(scanInfo->m_searchKey, 
			scanInfo->m_assistKey, searchFlag, scanInfo->m_comparator, &searchResult);
		*keyLocation = searchResult.m_keyLocation;
		needFetchNext = needFetchNext && (!scanInfo->m_uniqueScan || searchResult.m_cmpResult < 0);
	}

	if (needFetchNext) {
		return shiftToNextKey(scanInfo, keyLocation, &scanInfo->m_searchFlag, true);
	} else {
		if (unlikely(MIndexKeyOper::isInfiniteKey(scanInfo->m_assistKey))) {
			unlatchPage(scanInfo->m_session,infoExt->m_readPage, infoExt->m_latchMode);
			return IDX_FAIL;
		}
		return IDX_SUCCESS;
	}	
}

/**
 * 移动到下一键值
 * @param info        扫描句柄信息
 * @param leafPageHdl 要查找的叶页面
 * @param keyLocation NOUT 当前键值位置
 * @param searchFlag  查找标志
 * @return 
 */
IDXResult BLinkTree::shiftToNextKey(MIndexScanInfo *info, MkeyLocation *keyLocation, 
									const SearchFlag *searchFlag, bool onlyRead) {
	
	assert(None != getPageLatchMode(((MIndexScanInfoExt*)info)->m_readPage));
	/** 
	 * 正向扫描和反向扫描如果分别往两个方向加页面latch，可能会造成死锁, 处理办法是正向扫描从左往右加latch，
	 * 反向扫描先尝试加左边页面的latch，如果不成功，放掉当前页面的latch，再从左往右加latch，成功之后判断
	 * 右边页面latch的更新时间戳，发生改变则重启从根结点的查找
	 */
	return searchFlag->isForward() ? shiftToForwardKey(info, keyLocation, onlyRead) 
		: shiftToBackwardKey(info, keyLocation, onlyRead);
}

/**
 * 移动到下一索引键
 * @param info        扫描句柄信息
 * @param keyLocation INOUT 当前键值位置
 * @return 存在下一项返回IDX_SUCCESS，不存在返回IDX_FAIL
 */
IDXResult BLinkTree::shiftToForwardKey(MIndexScanInfo *info, MkeyLocation *keyLocation, bool onlyRead) {
	MIndexScanInfoExt *infoImpr = (MIndexScanInfoExt *)info;
	MIndexPageHdl leafPageHdl = MIDX_PAGE_NONE;
	if(!onlyRead)
		leafPageHdl = info->m_currentPage;
	else
		leafPageHdl = infoImpr->m_readPage;
	assert(None != getPageLatchMode(leafPageHdl));
	MkeyLocation *keyLoc = keyLocation;

	if(leafPageHdl->getKeyCount() > 0) {
		bool hasNextInPage = leafPageHdl->getNextKey(keyLocation, keyLoc, info->m_assistKey);
		if (likely(hasNextInPage)) {
			info->m_hasNext = true;
			return IDX_SUCCESS;
		} 
	}
		
	MIndexPageHdl nextPageHdl = MIDX_PAGE_NONE;
	
	while (true) {
		IDXResult result = shiftToForwardPage(info, leafPageHdl, &nextPageHdl);
		if (likely(IDX_SUCCESS == result)) {
			assert(getPageLatchMode(nextPageHdl) == info->m_latchMode);
			if (!onlyRead) {
				info->m_currentPage = nextPageHdl;
				info->m_pageTimeStamp = nextPageHdl->getTimeStamp();
			} else {
				infoImpr->m_readPage = nextPageHdl;
				infoImpr->m_readPageTimeStamp = nextPageHdl->getTimeStamp();
			}
			SYNCHERE(SP_MEM_INDEX_SHIFT_FORWARD_KEY);
			if (nextPageHdl->getKeyCount() > 0) {
				nextPageHdl->getFirstKey(info->m_assistKey, keyLoc);
				if(unlikely(!MIndexKeyOper::isKeyValid(info->m_assistKey))) {
					unlatchPage(infoImpr->m_session, nextPageHdl, infoImpr->m_latchMode);
					return IDX_FAIL;
				}
				return IDX_SUCCESS;
			} else {
				leafPageHdl = nextPageHdl;
			}
		} else {
			return result;
		}
	}
	return IDX_FAIL;
}

/**
 * 移动到前一索引键
 * @param info        扫描句柄信息
 * @param keyLocation INOUT 当前键值位置
 * @return 存在下一项返回IDX_SUCCESS，不存在返回IDX_FAIL, 
 *         如果因预防死锁需要重启从根结点的查找过程时返回IDX_RESTART
 */
IDXResult BLinkTree::shiftToBackwardKey(MIndexScanInfo *info, MkeyLocation *keyLocation, bool onlyRead) {
	MIndexPageHdl leafPageHdl = ((MIndexScanInfoExt*)info)->m_readPage;
	assert(None != getPageLatchMode(leafPageHdl));
	MIndexScanInfoExt *infoImpr = (MIndexScanInfoExt *)info;

	MkeyLocation *keyLoc = keyLocation;

	if(leafPageHdl->getKeyCount() > 0) {
		bool hasNextInPage = leafPageHdl->getPrevKey(keyLocation, keyLoc, info->m_assistKey);
		if (likely(hasNextInPage)) {
			info->m_hasNext = true;
			return IDX_SUCCESS;
		}
	}

	MIndexPageHdl prevPageHdl = MIDX_PAGE_NONE;
	while (true) {
		IDXResult result = shiftToBackwardPage(info, leafPageHdl, &prevPageHdl);
		if (likely(IDX_SUCCESS == result)) {
			assert(getPageLatchMode(prevPageHdl) == info->m_latchMode);
			if (!onlyRead) {
				info->m_currentPage = prevPageHdl;
				info->m_pageTimeStamp = prevPageHdl->getTimeStamp();
			} else {
				infoImpr->m_readPage = prevPageHdl;
				infoImpr->m_readPageTimeStamp = prevPageHdl->getTimeStamp();
			}
			SYNCHERE(SP_MEM_INDEX_SHIFT_BACKWARD_KEY);
			if (prevPageHdl->getKeyCount() > 0) {
				prevPageHdl->getExtremeKey(info->m_forward, info->m_assistKey, keyLoc);
				return IDX_SUCCESS;
			} else {
				leafPageHdl = prevPageHdl;
			}
		} else {
			return result;
		}
	}
	return IDX_FAIL;
}


/**
 * 移动到下一页面
 * @param info
 * @param currentPage
 * @param nextPageHdl
 * @return 
 */
IDXResult BLinkTree::shiftToForwardPage(MIndexScanInfo *info, MIndexPageHdl currentPage, 
										MIndexPageHdl *nextPageHdl) {
	*nextPageHdl = currentPage->getNextPage();
	MIndexPageHdl nextPage = *nextPageHdl;
	Session *session = info->m_session;

	if (MIDX_PAGE_NONE != nextPage) {
		latchPage(session, nextPage, info->m_latchMode, __FILE__, __LINE__);
		unlatchPage(session, currentPage, info->m_latchMode);
		return IDX_SUCCESS;
	} else {
		unlatchPage(session, currentPage, info->m_latchMode);
		return IDX_FAIL;
	}
}


/**
 * 移动到前一页面
 * @param info
 * @param currentPage
 * @param prevPageHdl
 * @return 
 */
IDXResult BLinkTree::shiftToBackwardPage(MIndexScanInfo *info, MIndexPageHdl currentPage, 
										 MIndexPageHdl *prevPageHdl) {
	*prevPageHdl = currentPage->getPrevPage();
	MIndexPageHdl prevPage = *prevPageHdl;
	Session *session = info->m_session;

	if (MIDX_PAGE_NONE != prevPage) {
		SYNCHERE(SP_MEM_INDEX_SHIFTBACKSCAN);
		if (tryLatchPage(session, prevPage, info->m_latchMode)) {
			//统计信息
			m_indexStatus.m_numLatchesConflicts++;
			unlatchPage(session, currentPage, info->m_latchMode);
			return IDX_SUCCESS;
		} else {
			u64 timeStamp = currentPage->getTimeStamp();
			unlatchPage(session, currentPage, info->m_latchMode);

			//无条件申请左页面latch，后再申请右页面latch
			SYNCHERE(SP_MEM_INDEX_SHIFTBACKSCAN1);
			if (latchPageIfType(session, prevPage, info->m_latchMode)) {
				SYNCHERE(SP_MEM_INDEX_SHIFTBACKSCAN2);
				if (!latchPageIfType(session, currentPage, info->m_latchMode)) {
					unlatchPage(session, prevPage, info->m_latchMode);
					return IDX_RESTART;
				}
			} else {
				return IDX_RESTART;
			}

			//判断右页面更新时间戳
			SYNCHERE(SP_MEM_INDEX_SHIFTBACKSCAN3);
			if (currentPage->getTimeStamp() != timeStamp) {
				unlatchPage(session, currentPage, info->m_latchMode);
				unlatchPage(session, prevPage, info->m_latchMode);
				return IDX_RESTART;
			} else {
				unlatchPage(session, currentPage, info->m_latchMode);
				return IDX_SUCCESS;
			}
		}
	} else {
		info->m_hasNext = false;
		unlatchPage(session, currentPage, info->m_latchMode);
		return IDX_FAIL;
	}
}

/**
 * 在叶页面插入键值，如果空间不够，分裂叶页面
 * @param scanInfo 扫描句柄信息
 * @param addKey   要插入的键值
 * @param keyLocation 如果不为NULL表示已经定位过插入位置
 * @param splitNewpage 提前申请到的用于分裂的新页面，用于保证不会因内存不足重启算法
 */
bool BLinkTree::addKeyOnLeafPage(MIndexScanInfo *scanInfo, SubRecord *addKey, 
								 const MkeyLocation *keyLocation, MIndexPageHdl splitNewPage) {
	//在叶页面插入键值，如果空间不够，分裂叶页面
	if (NEED_SPLIT == scanInfo->m_currentPage->addIndexKey(addKey, scanInfo->m_assistKey, 
		scanInfo->m_comparator, MIDX_PAGE_NONE, keyLocation)) {
			MIndexPageHdl newPage = splitNewPage;
			MIndexPageHdl insertPage = splitPage(scanInfo, scanInfo->m_currentPage, &newPage, false);	
			//如果内存不足，释放latch
			if(insertPage == MIDX_PAGE_NONE) {
				//释放当前页面latch
				unlatchPage(scanInfo->m_session, scanInfo->m_currentPage, Exclusived);
				MIndexPageHdl newPage = (MIndexPageHdl)m_pageManager->getPage(scanInfo->m_session->getId(), PAGE_MEM_INDEX, true);
				//释放新页面的latch后重启算法
				releasePage(scanInfo->m_session, newPage, true);
				//统计信息
				m_indexStatus.m_numAllocPage.increment();
				return false;
			}
			//此时在分裂后的页面上插入一定可以成功
			NTSE_ASSERT(INSERT_SUCCESS == insertPage->addIndexKey(addKey, scanInfo->m_assistKey, 
				scanInfo->m_comparator, MIDX_PAGE_NONE));
			scanInfo->m_currentPage = insertPage;
	} else {
		assert(splitNewPage == MIDX_PAGE_NONE);
	}

	scanInfo->m_currentPage->incrTimeStamp();
	return true;
}

/**
 * 将溢出页面链接到父页面
 * @param scanInfo       扫描句柄信息
 * @param parentPage     溢出页面的父页面
 * @param leftChildPage  溢出页面的前驱页面
 * @param rightChildPage 溢出页面
 * @return 
 */
MIndexPageHdl BLinkTree::linkChild(MIndexScanInfo *scanInfo, MIndexPageHdl parentPage, 
								   MIndexPageHdl leftChildPage, MIndexPageHdl rightChildPage) {
	assert(Exclusived == getPageLatchMode(parentPage));
	assert(Exclusived == getPageLatchMode(leftChildPage));
	assert(Exclusived == getPageLatchMode(rightChildPage));
	assert(!parentPage->isPageLeaf());

	//fprintf(stderr, "id:%d, linkChild:%x, %x\n", scanInfo->m_session->getId(), leftChildPage, rightChildPage);

	assert(leftChildPage->isOverflow());
	leftChildPage->markOverflow(false);

	SubRecord leftChildHighKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
	SubRecord rightChildHighKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);

	//更新原指向左子结点的索引项为指向右子结点
	rightChildPage->getHighKey(&rightChildHighKey);

#ifdef NTSE_UNIT_TEST
	u16 keyNoInPage;
	if (parentPage->binarySearch(&rightChildHighKey, scanInfo->m_assistKey, &keyNoInPage, 
		&SearchFlag::DEFAULT_FLAG, scanInfo->m_comparator) != 0) {
		parentPage->verifyPage(*m_tableDef, m_indexDef, true);
		leftChildPage->verifyPage(*m_tableDef, m_indexDef, true);
		rightChildPage->verifyPage(*m_tableDef, m_indexDef, true);
	}
#endif

	parentPage->resetChildPage(&rightChildHighKey, scanInfo->m_assistKey, scanInfo->m_comparator, rightChildPage);
	
	MIndexPageHdl currentPage = parentPage;
	//插入对应左子结点的索引项
	leftChildPage->getHighKey(&leftChildHighKey);

	if (NEED_SPLIT == parentPage->addIndexKey(&leftChildHighKey, scanInfo->m_assistKey, scanInfo->m_comparator, 
		leftChildPage)) {
			//父页面空间不够，需要分裂父页面
			MIndexPageHdl newPage = MIDX_PAGE_NONE;

			//分裂页面， 此时父页面和新分裂的页面的latch都不放
			currentPage = splitPage(scanInfo, parentPage, &newPage, true, &leftChildHighKey);
			//如果页面分不出来，释放三个页面的latch，死等页面，重启算法
			if(currentPage == NULL) {
				//逻辑上退回原来父节点指向子节点的指针
				parentPage->resetChildPage(&rightChildHighKey, scanInfo->m_assistKey, scanInfo->m_comparator, leftChildPage);
				leftChildPage->markOverflow(true);
				unlatchPage(scanInfo->m_session, parentPage, Exclusived);
				unlatchPage(scanInfo->m_session, leftChildPage, Exclusived);
				unlatchPage(scanInfo->m_session, rightChildPage, Exclusived);
				
				MIndexPageHdl newPage = (MIndexPageHdl)m_pageManager->getPage(scanInfo->m_session->getId(), PAGE_MEM_INDEX, true);
				//释放新页面的latch后重启算法
				releasePage(scanInfo->m_session, newPage, true);

				//统计信息
				m_indexStatus.m_numAllocPage.increment();

				return MIDX_PAGE_NONE;
			}
			assert(Exclusived == getPageLatchMode(currentPage));

			NTSE_ASSERT(INSERT_SUCCESS == currentPage->addIndexKey(&leftChildHighKey, scanInfo->m_assistKey, 
				scanInfo->m_comparator, leftChildPage));


			//比较parentPage页面的highKey和左子页面的highKey，确认左子节点的父节点
			if (parentPage->compareWithHighKey(&leftChildHighKey, scanInfo->m_assistKey, 
				scanInfo->m_comparator, &SearchFlag::DEFAULT_FLAG) <= 0) {
					unlatchPage(scanInfo->m_session, newPage, Exclusived);
					return parentPage;
			} else {
				unlatchPage(scanInfo->m_session, parentPage, Exclusived);
				return newPage;
			}

	}

	VERIFY_TNT_MINDEX_PAGE(parentPage);
	
	return currentPage;
}

/**
 * 将子页面从父页面的链接中删除
 * @param scanInfo       扫描句柄信息
 * @param parentPage     父页面
 * @param leftChildPage  前驱页面
 * @param rightChildPage 要unlink的页面
 * @param leftPageKey    前驱页面对应的索引键
 * @param rightPageKey   要unlink的页面对应的索引键
 * @return 
 */
void BLinkTree::unlinkChild(MIndexScanInfo *scanInfo, MIndexPageHdl parentPage, 
							MIndexPageHdl leftPage, MIndexPageHdl rightPage, 
							const SubRecord *leftPageKey, const SubRecord *rightPageKey) {
	assert(Exclusived == getPageLatchMode(parentPage));
	assert(Exclusived == getPageLatchMode(rightPage));

	//fprintf(stderr, "id:%d, unlinkChild:%x(%"I64FORMAT"d), %x(%"I64FORMAT"d)\n", scanInfo->m_session->getId(), leftPage, 
	//	leftPageKey->m_rowId, rightPage, rightPageKey->m_rowId);

	VERIFY_TNT_MINDEX_PAGE(parentPage);

	assert(!leftPage->isOverflow());
	leftPage->markOverflow(true);
	leftPage->incrTimeStamp();

	parentPage->resetChildPage(rightPageKey, scanInfo->m_assistKey, scanInfo->m_comparator, leftPage);

	NTSE_ASSERT(parentPage->deleteIndexKey(leftPageKey, scanInfo->m_assistKey, scanInfo->m_comparator));
	
	parentPage->incrTimeStamp();

	VERIFY_TNT_MINDEX_PAGE(parentPage);
}

/**
 * 分裂页面
 * @post 返回时分裂页面和新页面都加上了x-latch
 * @param session 会话
 * @param pageToSplit 要分裂的页面
 * @param newPageHdl IN/OUT 传入为提前申请的用于分裂的新页面，传出为申请得到的新页面
 * @return 新分裂页面句柄
 */
MIndexPageHdl BLinkTree::splitPage(MIndexScanInfo *scanInfo, MIndexPageHdl pageToSplit, 
								   MIndexPageHdl *newPageHdl, bool isForLinkChild, SubRecord *highKeyOfChildPageToBeLinked) {
	assert(m_pageManager->isPageLatched(pageToSplit, Exclusived));
	assert(!pageToSplit->isOverflow());

	Session *session = scanInfo->m_session;
	MIndexPageHdl rightPage = pageToSplit->getNextPage();

	MIndexPageHdl newPage = *newPageHdl;
	if (newPage == MIDX_PAGE_NONE) {
		newPage = getFreeNewPage(session, false);
		//如果分配不出新页面,返回NULL，由上层去释放latch等页面
		if(newPage == MIDX_PAGE_NONE) {
			return MIDX_PAGE_NONE;
		}
	}
	newPage->initPage(MIndexPage::formPageMark(m_indexId), pageToSplit->getPageType(), 
		pageToSplit->getPageLevel());
	newPage->setPrevPage(pageToSplit);
			
	if (MIDX_PAGE_NONE != rightPage) {
		latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);
		rightPage->setPrevPage(newPage);
	}

	//fprintf(stderr, "id:%d, split: %x, %x\n", scanInfo->m_session->getId(), pageToSplit, newPage);

	pageToSplit->split(newPage);
	
	pageToSplit->markOverflow(true);
	pageToSplit->setNextPage(newPage);
	pageToSplit->incrTimeStamp();
	newPage->setNextPage(rightPage);

	if (MIDX_PAGE_NONE != rightPage) {
		rightPage->incrTimeStamp();
		unlatchPage(session, rightPage, Exclusived);
	}

	VERIFY_TNT_MINDEX_PAGE(pageToSplit);
	VERIFY_TNT_MINDEX_PAGE(newPage);

	*newPageHdl = newPage;

	//更新统计信息	
	m_indexStatus.m_numSplit++;
	if(!isForLinkChild)
		return switchToCoverPage(scanInfo, pageToSplit, newPage);
	else 
		return switchToCoverPageForLinkChild(scanInfo, pageToSplit, newPage, highKeyOfChildPageToBeLinked);
}

/**
 * 合并两个页面
 * @pre 两个页面的数据一定可以放进同一个页面
 * @param session
 * @param pageMergeTo
 * @param pageMergeFrom
 * @return 
 */
MIndexPageHdl BLinkTree::mergePage(MIndexScanInfo *info, MIndexPageHdl pageMergeTo, 
								   MIndexPageHdl pageMergeFrom) {
	assert(pageMergeTo->getFreeSpace() >= pageMergeFrom->getDataSpaceUsed());
	assert(pageMergeFrom == pageMergeTo->getNextPage());
	assert(Exclusived == getPageLatchMode(pageMergeTo));
	assert(Exclusived == getPageLatchMode(pageMergeFrom));

	//FIX：此处是为测试添加的Assert
	NTSE_ASSERT(pageMergeTo->getPageLevel() < 10 && pageMergeTo->getPageLevel() == pageMergeFrom->getPageLevel() );
	//fprintf(stderr, "id:%d, mergePage:%x, %x\n", info->m_session->getId(), pageMergeTo, pageMergeFrom);
//	assert(!pageMergeFrom->isOverflow());
	assert(pageMergeTo->isOverflow());
	pageMergeTo->markOverflow(pageMergeFrom->isOverflow());

	MIndexPageHdl rightPage = pageMergeFrom->getNextPage();
	if (MIDX_PAGE_NONE != rightPage) {
		latchPage(info->m_session, rightPage, Exclusived, __FILE__, __LINE__);

		NTSE_ASSERT(pageMergeTo->getPageLevel() == rightPage->getPageLevel()); 
		rightPage->setPrevPage(pageMergeTo);
		rightPage->incrTimeStamp();
	}

	VERIFY_TNT_MINDEX_PAGE(pageMergeTo);
	VERIFY_TNT_MINDEX_PAGE(pageMergeFrom);

	pageMergeTo->setNextPage(rightPage);
	pageMergeTo->merge(pageMergeFrom);

	VERIFY_TNT_MINDEX_PAGE(pageMergeTo);
	
	if (MIDX_PAGE_NONE != rightPage) {
		VERIFY_TNT_MINDEX_PAGE(rightPage);
		unlatchPage(info->m_session, rightPage, Exclusived);
	}

	releasePage(info->m_session, pageMergeFrom);

	pageMergeTo->incrTimeStamp();

	//更新统计信息	
	m_indexStatus.m_numMerge++;

	return pageMergeTo;
}

/**
 * 增加树高度
 * @param info 扫描信息
 * @return 返回增加树高度之后，扫描要移动到的页面，返回时一定已经加了latch
 */
MIndexPageHdl BLinkTree::increaseTreeHeight(MIndexScanInfo *info) {
	MIndexPageHdl rightPage = m_rootPage->getNextPage();
	assert(m_pageManager->isPageLatched(m_rootPage, Exclusived));
	assert(m_pageManager->isPageLatched(rightPage, Exclusived));

	//分配和初始化新页面
	MIndexPageHdl newPage = getFreeNewPage(info->m_session,false);
	if(newPage == MIDX_PAGE_NONE) {
		unlatchPage(info->m_session, m_rootPage, Exclusived);
		unlatchPage(info->m_session, rightPage, Exclusived);
		newPage = (MIndexPageHdl)m_pageManager->getPage(info->m_session->getId(), PAGE_MEM_INDEX, true);
		//释放新页面的latch后重启算法
		releasePage(info->m_session, newPage, true);

		//统计信息
		m_indexStatus.m_numAllocPage.increment();

		return MIDX_PAGE_NONE;
	}

	assert(MIDX_PAGE_NONE != newPage && m_pageManager->isPageLatched(newPage, Exclusived));
	newPage->initPage(MIndexPage::formPageMark(m_indexId), rightPage->getPageType(), 
		m_rootPage->getPageLevel());
	
	//此时根页面及其右兄弟页面都加了latch
	//将根页面键值移到newPage上，并且在根页面上插入两个索引项
	MIndexPage::movePageKeys(newPage, m_rootPage);
	
	m_rootPage->markOverflow(false);
	IndexPageType childPageType = (m_rootPage->getPageType() & ROOT_AND_LEAF) ? LEAF_PAGE : NON_LEAF_PAGE;
	newPage->setPageType(childPageType);
	newPage->setNextPage(m_rootPage->getNextPage());
	
	m_rootPage->setPageType(ROOT_PAGE);
	m_rootPage->setNextPage(MIDX_PAGE_NONE);
	m_rootPage->incrPageLevel();

	SubRecord highKey(KEY_NATURAL, info->m_searchKey->m_numCols, info->m_searchKey->m_columns, NULL, 0);
	newPage->getHighKey(&highKey);
	NTSE_ASSERT(INSERT_SUCCESS == m_rootPage->addIndexKey(&highKey, info->m_assistKey, 
		info->m_comparator, newPage));//一定成功

	rightPage->getHighKey(&highKey);
	NTSE_ASSERT(INSERT_SUCCESS == m_rootPage->addIndexKey(&highKey, info->m_assistKey, 
		info->m_comparator, rightPage));//一定成功

	m_rootPage->incrTimeStamp();
	unlatchPage(info->m_session, m_rootPage, Exclusived);

	rightPage->setPageType(childPageType);
	rightPage->setPrevPage(newPage);
	rightPage->incrTimeStamp();

	//统计信息
	++m_indexStatus.m_numIncreaseTreeHeight;

	return switchToCoverPage(info, newPage, rightPage);
}


/**
 * 降低树高度
 * @param info
 * @param parentPage
 * @param childPage
 * @return 
 */
MIndexPageHdl BLinkTree::decreaseTreeHeight(MIndexScanInfo *info, MIndexPageHdl childPage) {
	assert(Exclusived == getPageLatchMode(m_rootPage));
	assert(Exclusived == getPageLatchMode(childPage));
	assert(1 == m_rootPage->getKeyCount());
	assert(MIDX_PAGE_NONE == childPage->getNextPage());

	if (m_rootPage->isPageRoot() && childPage->isPageLeaf()) {
		assert(m_rootPage == m_rootPage);
		m_rootPage->setPageType(ROOT_AND_LEAF);
	}

	MIndexPage::movePageKeys(m_rootPage, childPage);

	releasePage(info->m_session, childPage);

	m_rootPage->decPageLevel();
	m_rootPage->incrTimeStamp();

	//统计信息
	m_indexStatus.m_numDecreaseTreeHeight++;

	return m_rootPage;
}

/**
 * 升级页面latch
 *
 * @pre  页面已经加了S-latch
 * @post 如果升级成功，返回时页面加了X-latch；否则不加任何latch
 * @param session     会话
 * @param pageToLatch 要进行锁升级的页面
 * @return 是否升级成功
 */
bool BLinkTree::upgradeLatch(Session *session, MIndexPageHdl pageToLatch) {
	assert(m_pageManager->isPageLatched(pageToLatch, Shared));
	SYNCHERE(SP_MEM_INDEX_UPGREADLATCH);
	if (tryUpgradePage(session, pageToLatch)) {//尝试latch升级
		return true;
	} else {
		//latch升级失败，此时放掉已经持有的S-latch，直接无条件申请X-latch
		u64 pageTimeStampBefore = pageToLatch->getTimeStamp();
		unlatchPage(session, pageToLatch, Shared);
		SYNCHERE(SP_MEM_INDEX_UPGREADLATCH1);
		if (latchPageIfType(session, pageToLatch, Exclusived)) {
			//根据时间戳判断页面是否发生变更
			SYNCHERE(SP_MEM_INDEX_UPGREADLATCH2);
			u64 pageTimeStampAfter = pageToLatch->getTimeStamp();
			if (pageTimeStampBefore == pageTimeStampAfter) {
				assert(m_pageManager->isPageLatched(pageToLatch, Exclusived));
				return true;
			} else {
				//页面被更改过
				unlatchPage(session, pageToLatch, Exclusived);
				return false;
			}
		} else {
			return false;
		}
	}
}

/**
 * 判断查找键是否还在页面中部
 * @param info
 * @return 
 */
// bool BLinkTree::judgeIfNeedRestart(MIndexScanInfo *info) const {
// 	MIndexPageHdl leafPageHld = info->m_currentPage;
// 	assert(None != getPageLatchMode(leafPageHld));
// 
// 	if (unlikely(!leafPageHld->isPageLeaf())) {
// 		return true;
// 	}
// 
// 	if (leafPageHld->getKeyCount() > 0 && leafPageHld->compareWithHighKey(
// 		info->m_searchKey, info->m_assistKey, info->m_comparator, &info->m_searchFlag) <= 0) {
// 			MIndexSearchResult searchResult;
// 			leafPageHld->findKeyInLeafPage(info->m_searchKey, info->m_assistKey, &SearchFlag::DEFAULT_FLAG, 
// 				info->m_comparator, &searchResult);
// 			
// 			u16 keyNo = searchResult.m_keyLocation.m_keyNo;
// 			if (0 == searchResult.m_cmpResult && //找到相等索引项
// 				keyNo < (leafPageHld->getKeyCount() - 1) && keyNo > 0) {//在页面中间
// 					info->m_pageTimeStamp = leafPageHld->getTimeStamp();
// 					info->m_keyLocation = searchResult.m_keyLocation;
// 					return false;
// 			} else {
// 				return true;
// 			}
// 	} else {
// 		return true;
// 	}
// }

u64 BLinkTree::recordsInRange(Session *session, const SubRecord *min, bool includeKeyMin, 
	const SubRecord *max, bool includeKeyMax) {
		//TODO: 添加实现
		UNREFERENCED_PARAMETER(session);
		UNREFERENCED_PARAMETER(min);
		UNREFERENCED_PARAMETER(includeKeyMin);
		UNREFERENCED_PARAMETER(max);
		UNREFERENCED_PARAMETER(includeKeyMax);
		return 0;
}

SampleHandle * BLinkTree::beginSample(Session *session, uint wantSampleNum, bool fastSample) {
	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(wantSampleNum);
	UNREFERENCED_PARAMETER(fastSample);
	return NULL;
}

Sample * BLinkTree::sampleNext(SampleHandle *handle) {
	UNREFERENCED_PARAMETER(handle);
	return NULL;
}

void BLinkTree::endSample(SampleHandle *handle) {
	UNREFERENCED_PARAMETER(handle);
}

DBObjStats* BLinkTree::getDBObjStats() {
	return NULL;
}



void BLinkTree::delRec(Session *session, const SubRecord *key, RowIdVersion *version){
	ftrace(ts.mIdx, tout << session << key << rid(key->m_rowId););

	assert(key != NULL);
	assert(KEY_PAD == key->m_format);
	// 首先生成一个扫描信息句柄
	TNTTransaction *trx = session->getTrans();
	assert(trx != NULL);
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavePoint(mtx);
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);

	//设置查找键
	scanInfo->m_searchKey = MIndexKeyOper::convertKeyPN(mtx, key, *m_tableDef, m_indexDef);

	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);

	MkeyLocation keyLocation;
	assert(scanInfo->m_currentPage);
	bool exist = scanInfo->m_currentPage->locateInsertKeyInLeafPage(scanInfo->m_searchKey, 
		scanInfo->m_assistKey, scanInfo->m_comparator, &keyLocation);
	
	//必定能找到记录
	assert(exist);
	*version = MIndexKeyOper::readIdxVersion(scanInfo->m_assistKey);
	scanInfo->m_currentPage->deleteIndexKey(scanInfo->m_searchKey, scanInfo->m_assistKey, scanInfo->m_comparator);

	//不需要修改页面最大更新事务号

	//释放页面x-latch
	unlatchPage(session, scanInfo->m_currentPage, Exclusived);
	
}

bool BLinkTree::undoDelByMark(Session *session, const SubRecord *key, RowIdVersion version) {
	ftrace(ts.mIdx, tout << session << key << rid(key->m_rowId););

	assert(key != NULL);
	assert(key->m_rowId != INVALID_ROW_ID);
	assert(KEY_PAD == key->m_format);

	TNTTransaction *trx = session->getTrans();
	assert(trx != NULL);
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavePoint(mtx);

	// 首先生成一个扫描信息句柄
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);

	//设置查找键
	scanInfo->m_searchKey = MIndexKeyOper::convertKeyPN(mtx, key, *m_tableDef, m_indexDef);

	//在叶页面中定位查找键
	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);

	MkeyLocation keyLocation;
	assert(scanInfo->m_currentPage);
	bool exist = scanInfo->m_currentPage->locateInsertKeyInLeafPage(scanInfo->m_searchKey, 
		scanInfo->m_assistKey, scanInfo->m_comparator, &keyLocation);

	// 正常流程中删除后的版本必须存在，此时不可能被purge出去
	// TODO 此处是暂时做容错，相关BUG见 JIRA NTSETNT-100
	if (likely(exist))	{
		//旧版本存在, 修改delete bit
		assert(MIDX_PAGE_NONE != scanInfo->m_currentPage);
		scanInfo->m_currentPage->getKeyByLocation(&keyLocation, scanInfo->m_assistKey);
		assert(scanInfo->m_assistKey->m_rowId == scanInfo->m_searchKey->m_rowId);
		assert(MIndexKeyOper::readDelBit(scanInfo->m_assistKey) == 1);
		MIndexKeyOper::writeDelBit(scanInfo->m_assistKey, 0);

		// 页面最大更新事务号不用修改
		unlatchPage(session, scanInfo->m_currentPage, Exclusived);
	} else {
		// 先释放页面latch
		unlatchPage(session, scanInfo->m_currentPage, Exclusived);
		// 旧版本不存在只可能是在恢复初期上层rollback,此时再插入一条旧版本
		session->getTNTDb()->getTNTSyslog()->log(EL_WARN, "Undo Second Update MIndex OP Can't Find Before Image");
		assert(version != INVALID_VERSION);
		insert(session, key, version);	
	}

	return true;
}



/**
 * 获取索引的基本统计信息
 * @return 索引基本统计信息
 */
const MIndexStatus& BLinkTree::getStatus() {
	return m_indexStatus;
}

}