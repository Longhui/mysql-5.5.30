/**
* BLink��ʵ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
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
 * �����Ҽ��ĸ�ʽ�Ƿ��ǵ�һ�β�ѯ��״̬��������ת��Ϊȫ���Լ���ʽ
 */
void MIndexScanInfoExt::checkFormat() {
	if(unlikely(m_searchKey->m_numCols != m_indexDef->m_numCols)) {
		m_searchKey->m_columns = m_indexDef->m_columns;
		m_searchKey->m_numCols = m_indexDef->m_numCols;
		m_searchKey->m_size = m_indexDef->m_maxKeySize;
	}
}

/**
 * �ƶ��ڴ������α굽��������һ����¼
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
 * �ƶ��ڴ������α굽ָ���ļ�¼
 * @param key ָ���ļ�¼
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
 * ���캯��
 * @param session �Ự
 * @param mIndice �������ڴ���������
 * @param pageManager TNT�ڴ�ҳ�����
 * @param doubleChecker RowIdУ��
 * @param tableDef �����ı���
 * @param indexDef ��������
 * @param indexId  ����id
 */
BLinkTree::BLinkTree(Session *session, MIndice *mIndice, TNTIMPageManager *pageManager, 
					 const DoubleChecker *doubleChecker, TableDef **tableDef, 
					 const IndexDef *indexDef, u8 indexId) 
					 : m_mIndice(mIndice), m_pageManager(pageManager), m_tableDef(tableDef), 
					 m_indexDef(indexDef), m_indexId(indexId), m_doubleChecker(doubleChecker) {
	ftrace(ts.mIdx, tout << mIndice << indexDef << indexId);
	//��ʼ��ͳ����Ϣ
	memset(&m_indexStatus, 0, sizeof(m_indexStatus));
}

BLinkTree::~BLinkTree() {
	assert(MIDX_PAGE_NONE == m_rootPage);
	m_pageManager = NULL;
}


bool BLinkTree::init(Session *session, bool waitSuccess) {
	//�����Լ���ʼ����ҳ��
	m_rootPage = getFreeNewPage(session, waitSuccess);
	if(MIDX_PAGE_NONE == m_rootPage)
		return false;
	initRootPage(session);
	unlatchPage(session, m_rootPage, Exclusived);
	return true;
}

/**
 * �ر��ڴ������������ڴ�
 * @param session �Ự
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
 * ����һ�������ҳ��
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
 * ����һ��ҳ��
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

	//ͳ����Ϣ
	m_indexStatus.m_numFreePage.increment();
}

/**
 * ��ʼ����ҳ��
 * @param session �Ự
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
 * ��Blink���в����ֵ
 * @param session �Ự
 * @param key     Ҫ����ļ�ֵ, �����ʽ
 * @return �����Ƿ�ɹ�
 */
void BLinkTree::insert(Session *session, const SubRecord *key, RowIdVersion version) {
	ftrace(ts.mIdx, tout << session << key << rid(key->m_rowId););

	assert(key != NULL);
	assert(KEY_PAD == key->m_format);
	// ��������һ��ɨ����Ϣ���
	TNTTransaction *trx = session->getTrans();
	assert(trx != NULL);
	MIndexPageHdl newPage = MIDX_PAGE_NONE;
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavePoint(mtx);
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);

	//���ò��Ҽ�
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
		
		// varchar ��β�Ŀո񣬵���mysql��Ϊ��ȣ�����ʵ�����ݲ�ͬ
		// TODO: ������벻������ҳ�棬����ʱ��������������ն���©��¼
		if (RecordOper::compareKeyNNorPPColumnSize(*m_tableDef, scanInfo->m_searchKey, scanInfo->m_assistKey, m_indexDef)) {
			// ��ɾ��ԭ����ٲ�������
			uint oldSize = scanInfo->m_assistKey->m_size;
			uint newSize = scanInfo->m_searchKey->m_size;
			if (newSize > oldSize && newSize - oldSize > scanInfo->m_currentPage->getFreeSpace()) {
				// �������£���Ҫ��������ҳ�棬 Ҫ��֤ɾ���Ͳ�����ͬһ��latch��������ɣ������������ҳ��
				newPage = getFreeNewPage(session, false);
				if (newPage == MIDX_PAGE_NONE) {
					unlatchPage(session, scanInfo->m_currentPage, Exclusived);
					newPage = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, true);
					releasePage(session, newPage, true);
					session->getMemoryContext()->resetToSavepoint(sp);
					//ͳ����Ϣ
					m_indexStatus.m_numAllocPage.increment();
					m_indexStatus.m_numRestarts++;
					goto __restart ;
				}
			}
			scanInfo->m_currentPage->deleteIndexKey(scanInfo->m_searchKey, scanInfo->m_assistKey, scanInfo->m_comparator);
			goto __insertStep;
		}

		RowIdVersion oldVersion = MIndexKeyOper::readIdxVersion(scanInfo->m_assistKey);
		// �������д���һ����ȫһ���rowId����ֵ��delbit����ȣ�����version���벻һ��
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
		//���Բ��룬���ʧ�ܣ��ڴ治���ò���������ҳ�棬�����㷨��
		if(addKeyOnLeafPage(scanInfo, scanInfo->m_searchKey, &keyLocation, newPage) == false) {
			assert(newPage == MIDX_PAGE_NONE);
			session->getMemoryContext()->resetToSavepoint(sp);	
			//ͳ����Ϣ
			m_indexStatus.m_numRestarts++;
			goto __restart ;
		}
	}

	//�޸�ҳ�������������
	scanInfo->m_currentPage->setMaxTrxId(trx->getTrxId());

	//�ͷ�ҳ��x-latch
	unlatchPage(session, scanInfo->m_currentPage, Exclusived);

	//�޸�ͳ����Ϣ
	m_indexStatus.m_numInsert++;
}

/**
 * ������������Ϊɾ��
 * @param session �Ự
 * @param key Ҫ���ɾ���ļ�ֵ�������ʽ
 * @param version IN/OUT ����ΪFirstUpdate�����versionֵ������Ϊ���Ϊɾ�����versionֵ
 * @return �Ƿ��ҵ�ָ����
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
	
	// ��������һ��ɨ����Ϣ���
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);
	
	//���ò��Ҽ�
	scanInfo->m_searchKey = MIndexKeyOper::convertKeyPN(mtx, key, *m_tableDef, m_indexDef);
	
	u64 sp = session->getMemoryContext()->setSavepoint();

__restart:
	//��Ҷҳ���ж�λ���Ҽ�
	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);
	
	MkeyLocation keyLocation;
	assert(scanInfo->m_currentPage);
	bool exist = scanInfo->m_currentPage->locateInsertKeyInLeafPage(scanInfo->m_searchKey, 
		scanInfo->m_assistKey, scanInfo->m_comparator, &keyLocation);

	if (exist) {//�ɰ汾����, �޸�delete bit
		assert(MIDX_PAGE_NONE != scanInfo->m_currentPage);
		assert(scanInfo->m_assistKey->m_rowId == scanInfo->m_searchKey->m_rowId);
		assert(m_pageManager->isPageLatched(scanInfo->m_currentPage, Exclusived));
		// varchar ��β�Ŀո񣬵���mysql��Ϊ��ȣ�����ʵ�����ݲ�ͬ
		// TODO: ������벻������ҳ�棬����ʱ��������������ն���©��¼
		if (RecordOper::compareKeyNNorPPColumnSize(*m_tableDef, scanInfo->m_searchKey, scanInfo->m_assistKey, m_indexDef)) {
			// ��ɾ��ԭ����ٲ�������
			uint oldSize = scanInfo->m_assistKey->m_size;
			uint newSize = scanInfo->m_searchKey->m_size;
			if (newSize > oldSize && newSize - oldSize > scanInfo->m_currentPage->getFreeSpace()) {
				// �������£���Ҫ��������ҳ�棬 Ҫ��֤ɾ���Ͳ�����ͬһ��latch��������ɣ������������ҳ��
				newPage = getFreeNewPage(session, false);
				if (newPage == MIDX_PAGE_NONE) {
					unlatchPage(session, scanInfo->m_currentPage, Exclusived);
					newPage = (MIndexPageHdl)m_pageManager->getPage(session->getId(), PAGE_MEM_INDEX, true);
					releasePage(session, newPage, true);
					session->getMemoryContext()->resetToSavepoint(sp);
					//ͳ����Ϣ
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
	} else {//�ɰ汾�����ڣ�����delete bitΪ1�ľɰ汾
__insertStep:
		MIndexKeyOper::writeDelBit(scanInfo->m_searchKey, 1);
		NTSE_ASSERT(*version != INVALID_VERSION);
		MIndexKeyOper::writeIdxVersion(scanInfo->m_searchKey, *version);
		//���Բ��룬���ʧ�ܣ��ڴ治���ò���������ҳ�棬�����㷨��
		if(addKeyOnLeafPage(scanInfo, scanInfo->m_searchKey, &keyLocation, newPage) == false) {
			assert(newPage == MIDX_PAGE_NONE);
			session->getMemoryContext()->resetToSavepoint(sp);
			//ͳ����Ϣ
			m_indexStatus.m_numRestarts++;
			goto __restart;
		}
	}

	//�޸�ҳ�������������
	scanInfo->m_currentPage->setMaxTrxId(trx->getTrxId());

	unlatchPage(session, scanInfo->m_currentPage, Exclusived);

	//�޸�ͳ����Ϣ
	m_indexStatus.m_numDelete++;
	return true;
}

/**
 * ժ���ѱ�Purge����������¼
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

	//��һ�α���Ҷҳ�棬����ҳ���еĿɻ�����
	MIndexPageHdl &currentPage = scanInfo->m_currentPage;
	u64 purgeKeyCount = 0;
	do {
		assert(currentPage->isPageLeaf());
		assert(Exclusived == getPageLatchMode(currentPage));
		purgeKeyCount += currentPage->bulkPhyReclaim(readView, scanInfo->m_assistKey, m_doubleChecker);
	
		//�޸�ͳ����Ϣ
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
	//�����������ջ�ϲ������ҳ��
//	repairTree(scanInfo);

	return purgeKeyCount;
}

/**
 * �����ڴ�����ҳ��
 * @param session
 * @param hwm
 * @param lwm
 * @return 
 */
void BLinkTree::reclaimIndex(Session *session, u32 hwm, u32 lwm) {
	// ��ǰ����ҳ��û���ҹ�hwm����ҳ�����
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
 * �������������
 * @param scanInfo ɨ������Ϣ
 * @param numReclaimPages ���λ��ղ���������Ҫ���յ�ҳ����
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
				// ����ͷ�ҳ�����Ŀ�ﵽĿ�꣬������������ղ���
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

	//������ڹ�֦���������߶�
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
 * ��������һ��
 * @param scanInfo   ɨ������Ϣ
 * @param parentPage һ������ߵ�ҳ��
 * @param numReclaimPages �����ܻ��յ�ҳ����
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

		//latch��ǰҳ��
		latchPage(session, currentPage, Exclusived, __FILE__, __LINE__);
		
		while(currentPage != MIDX_PAGE_NONE && keyNo<parentPage->getKeyCount()-1) {
			MIndexPageHdl rightPage = currentPage->getNextPage();
			latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);
			
			//�ܺϲ���ϲ�
			if (currentPage->canMergeWith(rightPage)) {
				if (!currentPage->isOverflow()) {
					SubRecord rightPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
					NTSE_ASSERT(parentPage->findPageAssosiateKey(rightPage, &rightPageKey));//�ҵ�rightPage��Ӧ��������
					SubRecord leftPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
					NTSE_ASSERT(parentPage->findPageAssosiateKey(currentPage, &leftPageKey));//�ҵ�currentPage��Ӧ��������
					unlinkChild(scanInfo, parentPage, currentPage, rightPage, &leftPageKey, &rightPageKey);
				}
				mergePage(scanInfo, currentPage, rightPage);
				reclaimPageCnt++;
				// �����յ��㹻ҳ��ʱ�˳�
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
 * ���ݼ�ֵ����Ψһ��λ��ȡ�����
 * @param session   �Ự
 * @param key       ���Ҽ���KEY_PAD��ʽ
 * @param rowId     OUT ��������Ӧ��rowId
 * @param subRecord OUT ���Ҫ��ȡ�ļ�ֵ��ΪNULL��ʾ����Ҫ��ȡ
 * @param extractor �Ӽ�¼��ȡ��
 * @return �Ƿ��ҵ�ָ����
 */
/*
bool BLinkTree::getByUniqueKey(Session *session, const SubRecord *key, RowId *rowId, 
							   SubRecord *subRecord, SubToSubExtractor *extractor) {
	assert(KEY_PAD == key->m_format || KEY_NATURAL == key->m_format);

	MTransaction *trx = session->getTrans();
	McSavepoint msc(session->getMemoryContext());	
	MIndexScanInfo *scanInfo = generateScanInfo(session, Shared);

	if (fetchUnique(scanInfo, key)) {
		//������ҵ�����������Ӧ��RowId
		*rowId = scanInfo->m_assistKey->m_rowId;

// 		KeyMVInfo *keyMVInfo;
// 		keyMVInfo->m_visable = true;
// 		keyMVInfo->m_version = MIndexKeyOper::readIdxVersion(scanInfo->m_assistKey);
// 		keyMVInfo->m_delBit = MIndexKeyOper::readDelBit(scanInfo->m_assistKey);
// 		assert(0 == keyMVInfo->m_delBit);//delete bitΪ1��֮ǰ�Ѿ����˵�
		
		//������Ҫ��ȡ��ֵ���ͷ�Ҷҳ��latch
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
 * ����Ƿ�����ظ�����һ���ǵ�ǰ��
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
	 * ��Ȼ��Ψһ�����������ǿ��ܴ��ڼ�ֵ��ȵĶ��������(RowId��ͬ, ���������ֻ��һ��delete bitΪ0�����඼Ϊ1)
	 * ������Ҫ�����Ƿ�������ȱ��Ϊfalse
	 */
	scanInfo->m_searchFlag.setFlag(true, true, false);

	//����ⲿ���ݽ�������������ʽΪKEY_PAD, ����ת��Ϊ�ڴ��������ڲ���������ʽKEY_NATURAL
	scanInfo->m_searchKey = MIndexKeyOper::allocSubRecord(mtx, m_indexDef, KEY_NATURAL);
	if (KEY_NATURAL == key->m_format) {
		MIndexKeyOper::copyKey(scanInfo->m_searchKey, key);
	} else {
		RecordOper::convertKeyPN(*m_tableDef, m_indexDef, key, scanInfo->m_searchKey);
	}

	//���ȶ�λҶҳ��
	scanInfo->m_currentPage = readModeLocateLeafPage(scanInfo);
	
	return findDupKey(scanInfo);
}

#ifdef TNT_INDEX_OPTIMIZE
/**
 * ����Ψһ��������
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
	 * ��Ȼ��Ψһ�����������ǿ��ܴ��ڼ�ֵ��ȵĶ��������(RowId��ͬ)������ֻ��һ��delete bitΪ0�����඼Ϊ1��
	 * ������Ҫ�����Ƿ�������ȱ��Ϊfalse
	 */
	scanInfo->m_searchFlag.setFlag(true, true, false);

	//����ⲿ���ݽ�������������ʽΪKEY_PAD, ����ת��Ϊ�ڴ��������ڲ���������ʽKEY_NATURAL
	scanInfo->m_searchKey = MIndexKeyOper::allocSubRecord(mtx, m_indexDef, KEY_NATURAL);
	if (KEY_NATURAL == key->m_format) {
		MIndexKeyOper::copyKey(scanInfo->m_searchKey, key);
	} else {
		RecordOper::convertKeyPN(m_tableDef, key, scanInfo->m_searchKey);
	}

	//���ȶ�λҶҳ��
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
 * ��Ҷҳ���в��Ҽ�ֵ
 * @param scanInfo ɨ������Ϣ
 * @param leafPageHdl Ҷҳ����
 * @param searchResult OUT ���ҽ��
 * @return �Ƿ�ɹ��ҵ���ֵ
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
 * ��ʼһ��������Χɨ��
 * @param session �Ự
 * @param key ���Ҽ���KEY_PAD��ʽ
 * @param forward ɨ�跽��
 * @param includeKey �Ƿ���>=��<=
 * @param extractor �Ӽ�¼��ȡ��
 * @return ����ɨ����
 */
MIndexScanHandle* BLinkTree::beginScanFast(Session *session, const SubRecord *key, 
									  bool forward, bool includeKey) {
    assert(KEY_PAD == key->m_format || KEY_NATURAL == key->m_format);
	MIndexScanInfo *scanInfo = generateScanInfo(session, Shared);

	//�ⲿ���ݽ�������������ʽΪKEY_PAD, ����ת��Ϊ�ڴ��������ڲ���������ʽKEY_NATURAL

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
 * ��ȡɨ�跽�����һ��������
 * @param scanHandle ����ɨ����
 * @return �Ƿ�����һ��
 */
bool BLinkTree::getNextFast(MIndexScanHandle *handle) {
	assert(handle);

	MIndexRangeScanHandle *scanHandle = (MIndexRangeScanHandle*)handle;
	
	MIndexScanInfo *mIdxScanInfo = scanHandle->getScanInfo();
	mIdxScanInfo->m_searchFlag.setFlag(mIdxScanInfo->m_forward, mIdxScanInfo->m_includeKey, 
		mIdxScanInfo->m_fetchCount > 0);
	
	if (likely(!mIdxScanInfo->m_rangeFirst)) {//�Ѿ���λ��Ҷҳ��
		//���¶�ɨ�����б���ĵ�ǰҳ���latch���ж�ҳ�������Լ�����ʱ����Ƿ����������Ҫʱ�ض�λҶҳ��	
		if (latchPageIfType(mIdxScanInfo->m_session, mIdxScanInfo->m_currentPage, mIdxScanInfo->m_latchMode)) {

			if ((mIdxScanInfo->m_currentPage->getTimeStamp() == mIdxScanInfo->m_pageTimeStamp//ҳ��û�з������
				|| !judgeIfNeedRestart(mIdxScanInfo))) {//���Ҽ�����ԭҳ���������в������ǿ�����ԭҳ�����!
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
	//��Ҫ��λҶҳ��
	locateLeafPage(mIdxScanInfo);

	IDX_SWITCH_AND_GOTO(searchAtLeafLevel(mIdxScanInfo, &mIdxScanInfo->m_keyLocation, 
		&mIdxScanInfo->m_searchFlag), __locateLeaf, __failed);

__succeed:
	//if (mIdxScanInfo->m_trx->getLockType() != TL_NO) {
		//TODO: ����������!
		///
	//}

	//������ҽ��
	MIndexKeyOper::copyKeyAndExt(mIdxScanInfo->m_searchKey, mIdxScanInfo->m_assistKey, true);
	scanHandle->saveKey(mIdxScanInfo->m_assistKey);

	//����ҳ��ʱ���
	mIdxScanInfo->m_pageTimeStamp = mIdxScanInfo->m_currentPage->getTimeStamp();
	++mIdxScanInfo->m_fetchCount;
	mIdxScanInfo->m_rangeFirst = false;

	//�ͷ�ҳ��latch
	unlatchPage(mIdxScanInfo->m_session, mIdxScanInfo->m_currentPage, mIdxScanInfo->m_latchMode);
	return true;

__failed:
	mIdxScanInfo->m_hasNext = false;
	return false;
}

/**
 * ����������Χɨ��
 * @param scanHandle
 */
void BLinkTree::endScanFast(MIndexScanHandle *scanHandle) {
	assert(scanHandle);
	UNREFERENCED_PARAMETER(scanHandle);
}

#endif


///////////////////////////////////////////////////////////////

/**
 * ����һ��BLinkTree������Χɨ��
 * @param session �Ự
 * @param key     ���Ҽ�
 * @param forward �Ƿ���ǰ�����
 * @param includeKey �Ƿ������ֵ(���ڵ��ڻ�С�ڵ���)
 * @return �ڴ�����ɨ����
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

	//�ⲿ���ݽ�������������ʽΪKEY_PAD, ����ת��Ϊ�ڴ��������ڲ���������ʽKEY_NATURAL
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
	
	//ͳ����Ϣ
	m_indexStatus.m_numScans++;

	if(!forward)
		m_indexStatus.m_backwardScans++;


	return mIdxScanHdl;
}

/**
 * ��ȡ��Χɨ����һ��
 * @param handle �ڴ淶Χɨ����
 * @return �Ƿ������һ��
 */
bool BLinkTree::getNext(MIndexScanHandle *handle) throw(NtseException) {
	assert(handle);

	MIndexRangeScanHandle *scanHandle = (MIndexRangeScanHandle*)handle;	
	MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHandle->getScanInfo();
	Session *session = scanInfo->m_session;

	//��ʼ����ǰ��������Ϣ
	scanInfo->m_readPage = scanInfo->m_currentPage;
	scanInfo->m_readPageTimeStamp = scanInfo->m_pageTimeStamp;
	scanInfo->m_rangeFirst = scanInfo->m_tmpRangeFirst;
	scanInfo->m_readKeyLocation = scanInfo->m_keyLocation;

	assert(scanInfo->m_searchKey->m_format == KEY_NATURAL);

	scanHandle->unlatchLastRow();

	scanInfo->m_searchFlag.setFlag(scanInfo->m_forward, scanInfo->m_includeKey, 
		!scanInfo->m_rangeFirst);
	
	if (likely(!scanInfo->m_rangeFirst && scanInfo->m_readPage != MIDX_PAGE_NONE)) {//�Ѿ���λ��Ҷҳ��
		//���¶�ɨ�����б���ĵ�ǰҳ���latch���ж�ҳ�������Լ�����ʱ����Ƿ����������Ҫʱ�ض�λҶҳ��	
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
	//��Ҫ��λҶҳ��
	scanInfo->m_readPage = locateLeafPage(scanInfo);
	//����ڴ治��
	//����ڴ治��
	if(scanInfo->m_readPage == MIDX_PAGE_NONE)
		goto __locateLeaf;

	IDX_SWITCH_AND_GOTO(searchAtLeafLevelSecond(scanInfo, &scanInfo->m_readKeyLocation, 
		&scanInfo->m_searchFlag), __locateLeaf, __failed);

__succeed:
	// ��Ҫ�Ļ�����NTSE�ײ��������Լ�TNT������
	IDX_SWITCH_AND_GOTO(tryTrxLockHoldingLatch(scanInfo, 
		scanInfo->m_readPage, scanInfo->m_assistKey), __locateLeaf, __failed);

	//����ҳ��ʱ���
	//scanInfo->m_pageTimeStamp = scanInfo->m_currentPage->getTimeStamp();
	++scanInfo->m_fetchCount;
	scanInfo->m_rangeFirst = false;
	scanInfo->m_forceSearchPage = false;

	MIndexKeyOper::copyKeyAndExt(scanInfo->m_readKey, scanInfo->m_assistKey, true);
	scanHandle->saveKey(scanInfo->m_readKey);

	//�ͷ�ҳ��latch
	unlatchPage(session, scanInfo->m_readPage, scanInfo->m_latchMode);

	//ͳ����Ϣ
	m_indexStatus.m_rowsScanned++;
	if(!scanInfo->m_forward)
		m_indexStatus.m_rowsBScanned++;

	return true;

__failed:
	scanInfo->m_hasNext = false;
	return false;
}

/**
 * ����BLinkTree��Χɨ��
 * @param scanHandle
 * @return 
 */
void BLinkTree::endScan(MIndexScanHandle *handle) {
	assert(handle);
	MIndexRangeScanHandle *scanHandle = (MIndexRangeScanHandle*)handle;	
	scanHandle->unlatchLastRow();
}

/**
 * �ڳ���ҳ��latch������£����Լ���������
 * 
 * @pre Ҫ���������ļ�¼���ڵ��ڴ�����ҳ���latch�Ѿ�����
 * @param info �ڴ�����ɨ������Ϣ
 * @param pageHdl Ҫ���������ļ�¼���ڵ��ڴ�����ҳ��
 * @param key Ҫ���������ļ�¼
 * @throw NtseException ����ռ䲻���
 * @return ���׶μ����ɹ�����IDX_SUCCESS����ʱ����ҳ��latch��NTSE�����Լ���������
 *         ����ʧ�ܷ���IDX_RESTART�����ϲ������������ҹ��̣���ʱ�Գ���ҳ��latch
 */
IDXResult BLinkTree::tryTrxLockHoldingLatch(MIndexScanInfoExt *info, MIndexPageHdl pageHdl, 
											const SubRecord *key) throw(NtseException) {
	Session *session = info->m_session;
	RowId keyRowId = key->m_rowId;// ���е������һ��ļ�ֵ�϶�������recordָ���m_cKey1����

	if (info->m_ntseRlMode != None) {
		// ���NTSE�����Ƿ��Ѿ�����
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
 * �ж���ɨ�����м�¼��Ҷҳ����Ϣ�Ƿ���Ч
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
				if (0 == searchResult.m_cmpResult //�ҵ����������
					/*&& keyNo < (leafPageHld->getKeyCount() - 1) && keyNo > 0*/) {//��ҳ���м�
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
 * �������ID��
 * @return 
 */
u8 BLinkTree::getIndexId() const {
	return m_indexId;
}

/**
 * ������߶�
 * @return 
 */
u8 BLinkTree::getHeight() const {
	return m_rootPage->getPageLevel() + 1;
}

/**
 * ��ø�ҳ��
 * @return 
 */
MIndexPageHdl BLinkTree::getRootPage() const {
	return m_rootPage;
}

/**
 * ����ɨ������Ϣ
 * @param session �Ự
 * @param traversalMode ����ģʽ
 * @return ɨ������Ϣ
 */
MIndexScanInfo* BLinkTree::generateScanInfo(Session *session, LockMode latchMode) {
	void *memory = session->getMemoryContext()->alloc(sizeof(MIndexScanInfo));
	MIndexScanInfo *scanInfo = new (memory) MIndexScanInfo(session, session->getTrans(),
		*m_tableDef, m_indexDef, latchMode, RecordOper::compareKeyNN);
	return scanInfo;
}


/**
 * �ж�����ҳ���ĸ��������Ҽ�
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
 * ��linkChild���ж�����ҳ���ĸ�������link����ҳ��,��linkChild��ȥ�жϷ�latch
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
 * ��λҶҳ��
 * @param scanInfo
 * @return 
 */
MIndexPageHdl BLinkTree::locateLeafPage(MIndexScanInfo *scanInfo) {
	return READMODE == scanInfo->m_traversalMode ? 
		readModeLocateLeafPage(scanInfo) : updateModeLocateLeafPage(scanInfo);
}

/**
 * ֻ��ģʽ��λҶҳ��
 * @param scanInfo ����ɨ����Ϣ
 * @return ����Ϊ��ʱ����MIDX_PAGE_NONE, ���򷵻�Ҷҳ��
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
				//������Ҽ�ֵ����high-key��ת��������ҳ��
				assert(currentPage->isOverflow());
				MIndexPageHdl rightPage = currentPage->getNextPage();
				assert(MIDX_PAGE_NONE != rightPage);
				latchPage(session, rightPage, latchMode, __FILE__, __LINE__);
				unlatchPage(session, currentPage, latchMode);
				assert(rightPage->compareWithHighKey(scanInfo->m_searchKey, scanInfo->m_assistKey, 
					scanInfo->m_comparator, &scanInfo->m_searchFlag) <= 0);

				currentPage = rightPage;
		}
		if (!currentPage->isPageLeaf()) {//��Ҷҳ��
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
 * �ϲ����طֲ�����ҳ��ļ�ֵ
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
				//����ڴ治�������Ѳ�����ҳ��, �ͷ�����latch �󷵻�NULL�������㷨
				if(*parentPage == MIDX_PAGE_NONE) {
					return MIDX_PAGE_NONE;
				}
			}
			return switchToCoverPage(scanInfo, leftPage, rightPage);
		}
	}
}

/**
 * �ж��Ƿ���Ҫ���·ֲ�����ҳ��ļ�ֵ
 * @param leftPage
 * @param rightPage
 * @return 
 */
bool BLinkTree::needRedistribute(MIndexPageHdl leftPage, MIndexPageHdl rightPage) const {
	int margin = (int)leftPage->getFreeSpace() - (int)rightPage->getFreeSpace();
	return abs(margin) > (MIN(leftPage->getFreeSpace(), rightPage->getFreeSpace()) / 4);
}

/**
 * ���·ֲ�����ҳ��ļ�ֵ
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
	//����ͳ����Ϣ	
	m_indexStatus.m_numRedistribute++;

	return switchToCoverPage(scanInfo, leftPage, rightPage);
}

/**
 * �������������ҳ��
 * @param scanInfo
 * @param parentPage IN/OUT
 * @param overflowPage
 * @param needUnLatchParent �Ƿ���Ҫ��parent�ڵ��latch updateModeTraverse��Ҫ�ţ���purgeʱrepairLevelʱ����Ҫ��
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

	//��ʱͬʱ����3��ҳ���latch, current-X, child-X, right-X
	*parentPage = linkChild(scanInfo, currentPage, overflowPage, rightPage);

	//����ڴ治��,�ͷ�������ҳ��latch
	if(*parentPage == MIDX_PAGE_NONE) {
		return MIDX_PAGE_NONE;
	}

	//ͳ����Ϣ
	m_indexStatus.m_numRepairOverflow++;
	return switchToCoverPage(scanInfo, overflowPage, rightPage);
}

/**
 *  �޸������ҳ��(����ҳ�����丸ҳ������һ����ҳ��)
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

	//��ʱ��ҳ�治�������ҳ��
	assert(!childPage->isOverflow());
	//childPageû�����ҳ�棬��childPage������ߵ�ҳ��ϲ�
	if ((*parentPage)->getKeyCount() > 1) {
		MkeyLocation location;
		MkeyLocation prevLocation;
		u16 keyNo = 0;
		//�ҵ����ڵ��е����ָ��ָ���ҳ��
		SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
		NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//�ҵ�childPage��Ӧ��������			
		NTSE_ASSERT(0 == (*parentPage)->binarySearch(&childPageKey, scanInfo->m_assistKey, 
			&keyNo, &SearchFlag::DEFAULT_FLAG, scanInfo->m_comparator));
		assert(keyNo > 0);
		(*parentPage)->getKeyLocationByNo(keyNo, &location);

		//leftPageKey�洢��ҳ����ָ��childPage��ҳ��ļ�ֵ
		SubRecord leftPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
		NTSE_ASSERT((*parentPage)->getPrevKey(&location, &prevLocation, &leftPageKey));
		MIndexPageHdl leftPage = MIndexKeyOper::readPageHdl(&leftPageKey);
		
		//�ͷ�ԭҳ��latch
		u64 oldStamp = childPage->getTimeStamp();
		unlatchPage(session, childPage, Exclusived);

		latchPage(session, leftPage, Exclusived, __FILE__, __LINE__);
		

		//�Ӹ��ڵ㶨λ������ڵ㲢������������ڵ㣨�����
		MIndexPageHdl middlePage = MIDX_PAGE_NONE;
		if(leftPage->isOverflow()) {
			middlePage = leftPage->getNextPage();
			latchPage(session, middlePage, Exclusived, __FILE__, __LINE__);
			//FIXME���˴�Ҫ���Ǹ�ҳ����ѵ��������������Ŀǰ������СΪ8K��leftPage, middlePage��childPageһ������ͬһ�����ڵ�
			*parentPage = linkChild(scanInfo, *parentPage, leftPage, middlePage);
			//�������ʧ��
			if(*parentPage == NULL){
				return MIDX_PAGE_NONE;
			}

			unlatchPage(session, leftPage, Exclusived);
			assert(! middlePage->isOverflow());

			//���»�ȡ�����ĵ�ǰҳ����ҳ���ڸ��ڵ��Ӧ�ļ�ֵ
			SubRecord middlePageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
			middlePage->getHighKey(&middlePageKey);

			assert(middlePage->getNextPage() == childPage);
			latchPage(session, childPage, Exclusived, __FILE__, __LINE__);

			if(childPage->isUnderFlow()) {
				SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
				NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//�ҵ�childPage��Ӧ��������			
				return mergeOrRedistribute(scanInfo, parentPage, middlePage, childPage, 
				&middlePageKey, &childPageKey);
			} else{
				unlatchPage(session, middlePage, Exclusived);
				return childPage;
			}
		} else {
			assert(leftPage->getNextPage() == childPage);
			latchPage(session, childPage, Exclusived, __FILE__, __LINE__);
			//����ӻ�latch����ʱ�����ͬ�������㷨
			if(childPage->getTimeStamp() != oldStamp) {
				unlatchPage(session, *parentPage, Exclusived);
				unlatchPage(session, leftPage, Exclusived);
				unlatchPage(session, childPage, Exclusived);
				return MIDX_PAGE_NONE;
			}
			
			if(childPage->isUnderFlow()) {
				SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
				NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//�ҵ�childPage��Ӧ��������			
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
 * �޸������ҳ��(����ҳ�治���丸ҳ������һ����ҳ��)
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

	if (childPage->isOverflow()) {//rightPage��childPage�����ҳ��
		latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);
		return mergeOrRedistribute(scanInfo, parentPage, childPage, rightPage);
	} else {//rightPage�������ҳ��
		SubRecord rightPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);

		NTSE_ASSERT((*parentPage)->findPageAssosiateKey(rightPage, &rightPageKey));//�ҵ�rightPage��Ӧ��������			
		latchPage(session, rightPage, Exclusived, __FILE__, __LINE__);

		if (!rightPage->isOverflow()) {	
			SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
			NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//�ҵ�childPage��Ӧ��������			
			return mergeOrRedistribute(scanInfo, parentPage, childPage, rightPage, 
				&childPageKey, &rightPageKey);
		} else {//rightPage�����ҳ��
			MIndexPageHdl sPage = rightPage->getNextPage();
			assert(MIDX_PAGE_NONE != sPage);
			assert(!sPage->isOverflow());
	
			latchPage(session, sPage, Exclusived, __FILE__, __LINE__);
			
			MIndexPageHdl rightPageParentPage = linkChild(scanInfo, *parentPage, rightPage, sPage);
		//	*parentPage = linkChild(scanInfo, *parentPage, rightPage, sPage);
			//����ڴ治�����ʧ��
			if(rightPageParentPage == MIDX_PAGE_NONE) {
				unlatchPage(session, childPage, Exclusived);
				return MIDX_PAGE_NONE;
			}
			unlatchPage(session, sPage, Exclusived);
			
			SYNCHERE(SP_MEM_INDEX_REPAIRNOTRIGMOST);
			//�жϷ��Ѻ�rightPage�ĸ��ڵ��Ѿ�����ԭ����parentPage����ô�����㷨
			if (*parentPage != rightPageParentPage) {
					unlatchPage(session, rightPageParentPage, Exclusived);
					unlatchPage(session, childPage, Exclusived);
					unlatchPage(session, rightPage, Exclusived);
					SYNCHERE(SP_MEM_INDEX_REPAIRNOTRIGMOST1);
					return MIDX_PAGE_NONE;
			}
			
			rightPage->getHighKey(&rightPageKey);
			SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
			NTSE_ASSERT((*parentPage)->findPageAssosiateKey(childPage, &childPageKey));//�ҵ�childPage��Ӧ��������			

			return mergeOrRedistribute(scanInfo, parentPage, childPage, rightPage, 
				&childPageKey, &rightPageKey);
		}
	}
}

/**
 * �޸������ҳ��
 * @param scanInfo
 * @param parentPage
 * @param childPage
 * @return 
 */
MIndexPageHdl BLinkTree::repairPageUnderFlow(MIndexScanInfo *scanInfo, MIndexPageHdl *parentPage, 
											 MIndexPageHdl childPage) {
	assert(Exclusived == getPageLatchMode(*parentPage));
	assert(Exclusived == getPageLatchMode(childPage));

	//ͳ����Ϣ
	m_indexStatus.m_numRepairUnderflow++;
	
	VERIFY_TNT_MINDEX_PAGE(*parentPage);
	VERIFY_TNT_MINDEX_PAGE(childPage);

	SubRecord childPageKey(KEY_NATURAL, m_indexDef->m_numCols, m_indexDef->m_columns, NULL, 0);
	childPage->getHighKey(&childPageKey);
	if ((*parentPage)->compareWithHighKey(&childPageKey, scanInfo->m_assistKey, scanInfo->m_comparator, 
		&SearchFlag::DEFAULT_FLAG) < 0) {//childPage����parentPage��������ҳ��
			return repairNotRightMostChild(scanInfo, parentPage, childPage);
	} else {//childPage��parentPage��������ҳ��
		NTSE_ASSERT((*parentPage)->compareWithHighKey(&childPageKey, scanInfo->m_assistKey, scanInfo->m_comparator, 
			&SearchFlag::DEFAULT_FLAG) == 0);
		return repairRightMostChild(scanInfo, parentPage, childPage);
	}
}

/**
 * ����ģʽ��λҶҳ��
 * @param scanInfo ɨ������Ϣ
 * @return ����Ϊ��ʱ����MIDX_PAGE_NONE, ���򷵻�Ҷҳ��
 */
MIndexPageHdl BLinkTree::updateModeLocateLeafPage(MIndexScanInfo *scanInfo) {
	return updateModeTraverse(scanInfo, 0);
}

/**
 * ����ģʽ��λ��ָ���Ĳ�
 * @param scanInfo ɨ������Ϣ
 * @param level ָ���Ĳ���
 * @return ����Ϊ�ջ�ָ��������������ʱ����MIDX_PAGE_NONE, ���򷵻ض�λ������һ���ҳ��
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

	//�жϸ�ҳ���Ƿ������ҳ�棬�Ծ����Ƿ��������߶�
	if (unlikely(currentPage->getNextPage() != MIDX_PAGE_NONE)) {
		latchPage(session, currentPage->getNextPage(), Exclusived, __FILE__, __LINE__);
		currentPage = increaseTreeHeight(scanInfo);
		//�ڴ治���������ڴ汣��������㷨
		if(currentPage == MIDX_PAGE_NONE) {
			memCtx->resetToSavepoint(savePoint);
			//ͳ����Ϣ
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
			//����ڴ治�㣬������Ҫ�����㷨ʱ
			if (unlikely(MIDX_PAGE_NONE == childPage)) {
				memCtx->resetToSavepoint(savePoint);
				//ͳ����Ϣ
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
				//��ʱ�������ҳ��ĸ�����x-latch, �������ҳ��
				childPage = repairPageOverflow(scanInfo, &currentPage, childPage);
				//����ڴ治��
				if(childPage == MIDX_PAGE_NONE) {
					memCtx->resetToSavepoint(savePoint);
					//ͳ����Ϣ
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
 * �Ӷ�λ����Ҷҳ�濪ʼ��������ظ���ֵ
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
 * ��ָ����λ�ÿ�ʼ��������ظ���ֵ
 * @param scanInfo    ɨ������Ϣ
 * @param keyLocation INOUT ���fetchNextΪfalse����Ӵ�λ�ÿ�ʼ���ң�����Ӵ�λ�õ���һ����������ʼ����
                      ���Ϊ��һ���ɼ���������λ��
 * @param fetchNext   �Ƿ��keyLocation����һ����������ʼ����
 * @param isDuplicate OUT �Ƿ��ҵ��ظ���
 * @return �ɹ��ҵ���һ���IDX_SUCCESS�����������߽緵��IDX_FAIL����Ҫ��������IDX_RESTART
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
			//����������λ�û�ȡ��ֵ
			scanInfo->m_currentPage->getKeyByLocation(keyLocation, scanInfo->m_assistKey);
			if (unlikely(MIndexKeyOper::isInfiniteKey(scanInfo->m_assistKey))) {
				//�������޴��ֵ��û��ƥ���������
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
 * ��Ҷҳ������������Ľ��汾
 * @param scanInfo �ڴ�����ɨ������Ϣ
 * @param keyLocation OUT ��λ���ļ�ֵλ��
 * @param searchFlag ���ұ�־
 * @return IDX_FAIL��ʾ�����ڣ�IDX_RESTART��ʾ��Ҫ������IDX_SUCCESS��ʾ���ҳɹ�
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
 * �ƶ�����һ��ֵ
 * @param info        ɨ������Ϣ
 * @param leafPageHdl Ҫ���ҵ�Ҷҳ��
 * @param keyLocation NOUT ��ǰ��ֵλ��
 * @param searchFlag  ���ұ�־
 * @return 
 */
IDXResult BLinkTree::shiftToNextKey(MIndexScanInfo *info, MkeyLocation *keyLocation, 
									const SearchFlag *searchFlag, bool onlyRead) {
	
	assert(None != getPageLatchMode(((MIndexScanInfoExt*)info)->m_readPage));
	/** 
	 * ����ɨ��ͷ���ɨ������ֱ������������ҳ��latch�����ܻ��������, ����취������ɨ��������Ҽ�latch��
	 * ����ɨ���ȳ��Լ����ҳ���latch��������ɹ����ŵ���ǰҳ���latch���ٴ������Ҽ�latch���ɹ�֮���ж�
	 * �ұ�ҳ��latch�ĸ���ʱ����������ı��������Ӹ����Ĳ���
	 */
	return searchFlag->isForward() ? shiftToForwardKey(info, keyLocation, onlyRead) 
		: shiftToBackwardKey(info, keyLocation, onlyRead);
}

/**
 * �ƶ�����һ������
 * @param info        ɨ������Ϣ
 * @param keyLocation INOUT ��ǰ��ֵλ��
 * @return ������һ���IDX_SUCCESS�������ڷ���IDX_FAIL
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
 * �ƶ���ǰһ������
 * @param info        ɨ������Ϣ
 * @param keyLocation INOUT ��ǰ��ֵλ��
 * @return ������һ���IDX_SUCCESS�������ڷ���IDX_FAIL, 
 *         �����Ԥ��������Ҫ�����Ӹ����Ĳ��ҹ���ʱ����IDX_RESTART
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
 * �ƶ�����һҳ��
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
 * �ƶ���ǰһҳ��
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
			//ͳ����Ϣ
			m_indexStatus.m_numLatchesConflicts++;
			unlatchPage(session, currentPage, info->m_latchMode);
			return IDX_SUCCESS;
		} else {
			u64 timeStamp = currentPage->getTimeStamp();
			unlatchPage(session, currentPage, info->m_latchMode);

			//������������ҳ��latch������������ҳ��latch
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

			//�ж���ҳ�����ʱ���
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
 * ��Ҷҳ������ֵ������ռ䲻��������Ҷҳ��
 * @param scanInfo ɨ������Ϣ
 * @param addKey   Ҫ����ļ�ֵ
 * @param keyLocation �����ΪNULL��ʾ�Ѿ���λ������λ��
 * @param splitNewpage ��ǰ���뵽�����ڷ��ѵ���ҳ�棬���ڱ�֤�������ڴ治�������㷨
 */
bool BLinkTree::addKeyOnLeafPage(MIndexScanInfo *scanInfo, SubRecord *addKey, 
								 const MkeyLocation *keyLocation, MIndexPageHdl splitNewPage) {
	//��Ҷҳ������ֵ������ռ䲻��������Ҷҳ��
	if (NEED_SPLIT == scanInfo->m_currentPage->addIndexKey(addKey, scanInfo->m_assistKey, 
		scanInfo->m_comparator, MIDX_PAGE_NONE, keyLocation)) {
			MIndexPageHdl newPage = splitNewPage;
			MIndexPageHdl insertPage = splitPage(scanInfo, scanInfo->m_currentPage, &newPage, false);	
			//����ڴ治�㣬�ͷ�latch
			if(insertPage == MIDX_PAGE_NONE) {
				//�ͷŵ�ǰҳ��latch
				unlatchPage(scanInfo->m_session, scanInfo->m_currentPage, Exclusived);
				MIndexPageHdl newPage = (MIndexPageHdl)m_pageManager->getPage(scanInfo->m_session->getId(), PAGE_MEM_INDEX, true);
				//�ͷ���ҳ���latch�������㷨
				releasePage(scanInfo->m_session, newPage, true);
				//ͳ����Ϣ
				m_indexStatus.m_numAllocPage.increment();
				return false;
			}
			//��ʱ�ڷ��Ѻ��ҳ���ϲ���һ�����Գɹ�
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
 * �����ҳ�����ӵ���ҳ��
 * @param scanInfo       ɨ������Ϣ
 * @param parentPage     ���ҳ��ĸ�ҳ��
 * @param leftChildPage  ���ҳ���ǰ��ҳ��
 * @param rightChildPage ���ҳ��
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

	//����ԭָ�����ӽ���������Ϊָ�����ӽ��
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
	//�����Ӧ���ӽ���������
	leftChildPage->getHighKey(&leftChildHighKey);

	if (NEED_SPLIT == parentPage->addIndexKey(&leftChildHighKey, scanInfo->m_assistKey, scanInfo->m_comparator, 
		leftChildPage)) {
			//��ҳ��ռ䲻������Ҫ���Ѹ�ҳ��
			MIndexPageHdl newPage = MIDX_PAGE_NONE;

			//����ҳ�棬 ��ʱ��ҳ����·��ѵ�ҳ���latch������
			currentPage = splitPage(scanInfo, parentPage, &newPage, true, &leftChildHighKey);
			//���ҳ��ֲ��������ͷ�����ҳ���latch������ҳ�棬�����㷨
			if(currentPage == NULL) {
				//�߼����˻�ԭ�����ڵ�ָ���ӽڵ��ָ��
				parentPage->resetChildPage(&rightChildHighKey, scanInfo->m_assistKey, scanInfo->m_comparator, leftChildPage);
				leftChildPage->markOverflow(true);
				unlatchPage(scanInfo->m_session, parentPage, Exclusived);
				unlatchPage(scanInfo->m_session, leftChildPage, Exclusived);
				unlatchPage(scanInfo->m_session, rightChildPage, Exclusived);
				
				MIndexPageHdl newPage = (MIndexPageHdl)m_pageManager->getPage(scanInfo->m_session->getId(), PAGE_MEM_INDEX, true);
				//�ͷ���ҳ���latch�������㷨
				releasePage(scanInfo->m_session, newPage, true);

				//ͳ����Ϣ
				m_indexStatus.m_numAllocPage.increment();

				return MIDX_PAGE_NONE;
			}
			assert(Exclusived == getPageLatchMode(currentPage));

			NTSE_ASSERT(INSERT_SUCCESS == currentPage->addIndexKey(&leftChildHighKey, scanInfo->m_assistKey, 
				scanInfo->m_comparator, leftChildPage));


			//�Ƚ�parentPageҳ���highKey������ҳ���highKey��ȷ�����ӽڵ�ĸ��ڵ�
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
 * ����ҳ��Ӹ�ҳ���������ɾ��
 * @param scanInfo       ɨ������Ϣ
 * @param parentPage     ��ҳ��
 * @param leftChildPage  ǰ��ҳ��
 * @param rightChildPage Ҫunlink��ҳ��
 * @param leftPageKey    ǰ��ҳ���Ӧ��������
 * @param rightPageKey   Ҫunlink��ҳ���Ӧ��������
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
 * ����ҳ��
 * @post ����ʱ����ҳ�����ҳ�涼������x-latch
 * @param session �Ự
 * @param pageToSplit Ҫ���ѵ�ҳ��
 * @param newPageHdl IN/OUT ����Ϊ��ǰ��������ڷ��ѵ���ҳ�棬����Ϊ����õ�����ҳ��
 * @return �·���ҳ����
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
		//������䲻����ҳ��,����NULL�����ϲ�ȥ�ͷ�latch��ҳ��
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

	//����ͳ����Ϣ	
	m_indexStatus.m_numSplit++;
	if(!isForLinkChild)
		return switchToCoverPage(scanInfo, pageToSplit, newPage);
	else 
		return switchToCoverPageForLinkChild(scanInfo, pageToSplit, newPage, highKeyOfChildPageToBeLinked);
}

/**
 * �ϲ�����ҳ��
 * @pre ����ҳ�������һ�����ԷŽ�ͬһ��ҳ��
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

	//FIX���˴���Ϊ������ӵ�Assert
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

	//����ͳ����Ϣ	
	m_indexStatus.m_numMerge++;

	return pageMergeTo;
}

/**
 * �������߶�
 * @param info ɨ����Ϣ
 * @return �����������߶�֮��ɨ��Ҫ�ƶ�����ҳ�棬����ʱһ���Ѿ�����latch
 */
MIndexPageHdl BLinkTree::increaseTreeHeight(MIndexScanInfo *info) {
	MIndexPageHdl rightPage = m_rootPage->getNextPage();
	assert(m_pageManager->isPageLatched(m_rootPage, Exclusived));
	assert(m_pageManager->isPageLatched(rightPage, Exclusived));

	//����ͳ�ʼ����ҳ��
	MIndexPageHdl newPage = getFreeNewPage(info->m_session,false);
	if(newPage == MIDX_PAGE_NONE) {
		unlatchPage(info->m_session, m_rootPage, Exclusived);
		unlatchPage(info->m_session, rightPage, Exclusived);
		newPage = (MIndexPageHdl)m_pageManager->getPage(info->m_session->getId(), PAGE_MEM_INDEX, true);
		//�ͷ���ҳ���latch�������㷨
		releasePage(info->m_session, newPage, true);

		//ͳ����Ϣ
		m_indexStatus.m_numAllocPage.increment();

		return MIDX_PAGE_NONE;
	}

	assert(MIDX_PAGE_NONE != newPage && m_pageManager->isPageLatched(newPage, Exclusived));
	newPage->initPage(MIndexPage::formPageMark(m_indexId), rightPage->getPageType(), 
		m_rootPage->getPageLevel());
	
	//��ʱ��ҳ�漰�����ֵ�ҳ�涼����latch
	//����ҳ���ֵ�Ƶ�newPage�ϣ������ڸ�ҳ���ϲ�������������
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
		info->m_comparator, newPage));//һ���ɹ�

	rightPage->getHighKey(&highKey);
	NTSE_ASSERT(INSERT_SUCCESS == m_rootPage->addIndexKey(&highKey, info->m_assistKey, 
		info->m_comparator, rightPage));//һ���ɹ�

	m_rootPage->incrTimeStamp();
	unlatchPage(info->m_session, m_rootPage, Exclusived);

	rightPage->setPageType(childPageType);
	rightPage->setPrevPage(newPage);
	rightPage->incrTimeStamp();

	//ͳ����Ϣ
	++m_indexStatus.m_numIncreaseTreeHeight;

	return switchToCoverPage(info, newPage, rightPage);
}


/**
 * �������߶�
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

	//ͳ����Ϣ
	m_indexStatus.m_numDecreaseTreeHeight++;

	return m_rootPage;
}

/**
 * ����ҳ��latch
 *
 * @pre  ҳ���Ѿ�����S-latch
 * @post ��������ɹ�������ʱҳ�����X-latch�����򲻼��κ�latch
 * @param session     �Ự
 * @param pageToLatch Ҫ������������ҳ��
 * @return �Ƿ������ɹ�
 */
bool BLinkTree::upgradeLatch(Session *session, MIndexPageHdl pageToLatch) {
	assert(m_pageManager->isPageLatched(pageToLatch, Shared));
	SYNCHERE(SP_MEM_INDEX_UPGREADLATCH);
	if (tryUpgradePage(session, pageToLatch)) {//����latch����
		return true;
	} else {
		//latch����ʧ�ܣ���ʱ�ŵ��Ѿ����е�S-latch��ֱ������������X-latch
		u64 pageTimeStampBefore = pageToLatch->getTimeStamp();
		unlatchPage(session, pageToLatch, Shared);
		SYNCHERE(SP_MEM_INDEX_UPGREADLATCH1);
		if (latchPageIfType(session, pageToLatch, Exclusived)) {
			//����ʱ����ж�ҳ���Ƿ������
			SYNCHERE(SP_MEM_INDEX_UPGREADLATCH2);
			u64 pageTimeStampAfter = pageToLatch->getTimeStamp();
			if (pageTimeStampBefore == pageTimeStampAfter) {
				assert(m_pageManager->isPageLatched(pageToLatch, Exclusived));
				return true;
			} else {
				//ҳ�汻���Ĺ�
				unlatchPage(session, pageToLatch, Exclusived);
				return false;
			}
		} else {
			return false;
		}
	}
}

/**
 * �жϲ��Ҽ��Ƿ���ҳ���в�
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
// 			if (0 == searchResult.m_cmpResult && //�ҵ����������
// 				keyNo < (leafPageHld->getKeyCount() - 1) && keyNo > 0) {//��ҳ���м�
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
		//TODO: ���ʵ��
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
	// ��������һ��ɨ����Ϣ���
	TNTTransaction *trx = session->getTrans();
	assert(trx != NULL);
	MemoryContext *mtx = session->getMemoryContext();
	McSavepoint mcSavePoint(mtx);
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);

	//���ò��Ҽ�
	scanInfo->m_searchKey = MIndexKeyOper::convertKeyPN(mtx, key, *m_tableDef, m_indexDef);

	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);

	MkeyLocation keyLocation;
	assert(scanInfo->m_currentPage);
	bool exist = scanInfo->m_currentPage->locateInsertKeyInLeafPage(scanInfo->m_searchKey, 
		scanInfo->m_assistKey, scanInfo->m_comparator, &keyLocation);
	
	//�ض����ҵ���¼
	assert(exist);
	*version = MIndexKeyOper::readIdxVersion(scanInfo->m_assistKey);
	scanInfo->m_currentPage->deleteIndexKey(scanInfo->m_searchKey, scanInfo->m_assistKey, scanInfo->m_comparator);

	//����Ҫ�޸�ҳ�������������

	//�ͷ�ҳ��x-latch
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

	// ��������һ��ɨ����Ϣ���
	MIndexScanInfo *scanInfo = generateScanInfo(session, Exclusived);

	//���ò��Ҽ�
	scanInfo->m_searchKey = MIndexKeyOper::convertKeyPN(mtx, key, *m_tableDef, m_indexDef);

	//��Ҷҳ���ж�λ���Ҽ�
	scanInfo->m_currentPage = updateModeLocateLeafPage(scanInfo);

	MkeyLocation keyLocation;
	assert(scanInfo->m_currentPage);
	bool exist = scanInfo->m_currentPage->locateInsertKeyInLeafPage(scanInfo->m_searchKey, 
		scanInfo->m_assistKey, scanInfo->m_comparator, &keyLocation);

	// ����������ɾ����İ汾������ڣ���ʱ�����ܱ�purge��ȥ
	// TODO �˴�����ʱ���ݴ����BUG�� JIRA NTSETNT-100
	if (likely(exist))	{
		//�ɰ汾����, �޸�delete bit
		assert(MIDX_PAGE_NONE != scanInfo->m_currentPage);
		scanInfo->m_currentPage->getKeyByLocation(&keyLocation, scanInfo->m_assistKey);
		assert(scanInfo->m_assistKey->m_rowId == scanInfo->m_searchKey->m_rowId);
		assert(MIndexKeyOper::readDelBit(scanInfo->m_assistKey) == 1);
		MIndexKeyOper::writeDelBit(scanInfo->m_assistKey, 0);

		// ҳ������������Ų����޸�
		unlatchPage(session, scanInfo->m_currentPage, Exclusived);
	} else {
		// ���ͷ�ҳ��latch
		unlatchPage(session, scanInfo->m_currentPage, Exclusived);
		// �ɰ汾������ֻ�������ڻָ������ϲ�rollback,��ʱ�ٲ���һ���ɰ汾
		session->getTNTDb()->getTNTSyslog()->log(EL_WARN, "Undo Second Update MIndex OP Can't Find Before Image");
		assert(version != INVALID_VERSION);
		insert(session, key, version);	
	}

	return true;
}



/**
 * ��ȡ�����Ļ���ͳ����Ϣ
 * @return ��������ͳ����Ϣ
 */
const MIndexStatus& BLinkTree::getStatus() {
	return m_indexStatus;
}

}