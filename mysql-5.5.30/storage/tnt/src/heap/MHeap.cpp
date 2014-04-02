//ע���:Ŀǰϵͳ���dump��purge��ҳ��defrag����ͬʱ����
#include "heap/MHeap.h"
#include "util/System.h"
#include "util/Array.h"
#include "util/File.h"
#include "util/SmartPtr.h"
#include "misc/RecordHelper.h"

namespace tnt {
MHeap::MHeap(TableDef **tableDef, TNTIMPageManager *pageManager, HashIndexOperPolicy *hashIndexOperPolicy): 
m_statusLock("TNTHeap Status Lock", __FILE__, __LINE__), m_compensateRows(pageManager, PAGE_DUMP)
{
	m_gradeNum = FREE_NODE_GRADE_NUM;
	m_freeList = (FreeNode **)malloc(m_gradeNum * sizeof(FreeNode *));
	for (int i = 0; i < m_gradeNum; i++) {
		m_freeList[i] = new FreeNode("MHeap FreeNode");
	}
	m_pageManager = pageManager;
	m_hashIndexOperPolicy = hashIndexOperPolicy;
	m_state = TNTIM_NOOP;
	m_tableDef = tableDef;
	m_tableId = (*tableDef)->m_id;
	memset(&m_stat, 0, sizeof(MHeapStat));
	m_totalAllocCnt = 0;
	m_totalSearchCnt = 0;
}

MHeap::~MHeap(void)
{
	for (int i = 0; i < m_gradeNum; i++) {
		delete m_freeList[i];
		m_freeList[i] = NULL;
	}

	free(m_freeList);
}

bool MHeap::verifyAllPage(u16 tableId) {
	bool ret = true;
	for (int i = 0; ret && i < m_gradeNum; i++) {
		m_freeList[i]->m_lock.lock(Exclusived, __FILE__, __LINE__);
		MRecordPage *page = m_freeList[i]->m_node;
		while (page != NULL) {
			assert(page->m_tableId == tableId);
			if (page->m_tableId != tableId) {
				ret = false;
				break;
			}
			page = page->m_next;
		}
		m_freeList[i]->m_lock.unlock(Exclusived);
	}
	return ret;
}

void MHeap::replaceComponents(TableDef **tableDef) {
	if (tableDef != NULL) {
		assert(m_tableId == (*tableDef)->m_id);
		verify_ex(vs.mheap, verifyAllPage((*tableDef)->m_id));
	} else {
		verify_ex(vs.mheap, verifyAllPage((*m_tableDef)->m_id));
	}
	m_tableDef = tableDef;
}

/** ����¼ҳ��ӵ�����������ײ�
 * @param page ��Ҫ��ӵļ�¼ҳ
 */
void MHeap::addToFreeList(MRecordPage* page) {
	assert(page->m_pageType == MPT_MHEAP);
	u8 grade = page->getFreeGrade(page->m_freeSize);
	FreeNode *free = m_freeList[grade];
	free->m_lock.lock(Exclusived, __FILE__, __LINE__);
	addToFreeListSafe(free, page);
	free->m_lock.unlock(Exclusived);
}

/** ����¼ҳ��ӵ�ָ������������ײ�
 * @param free ��Ҫ��ӵĿ�������
 * @param page ��Ҫ��ӵļ�¼ҳ
 */
void MHeap::addToFreeListSafe(FreeNode *free, MRecordPage* page) {
	assert(free->m_lock.getLockMode() == Exclusived);
	assert(page->m_pageType == MPT_MHEAP);
	assert(page->m_tableId == (*m_tableDef)->m_id);
	assert(page->m_tableId == m_tableId);
	page->m_next = free->m_node;
	page->m_prev = NULL;
	free->m_node = page;
	if (page->m_next != NULL) {
		page->m_next->m_prev = page;
	}
	free->m_size++;
}

/** ��һ��ҳ����Ӧ�Ŀ����������Ƴ�
 * @param free ָ���Ŀ�������
 * @param page ��Ҫ�Ƴ��ļ�¼ҳ
 */
void MHeap::removeFromFreeListSafe(FreeNode *free, MRecordPage *page) {
	assert(free->m_lock.getLockMode() == Exclusived);
	assert(page->m_pageType == MPT_MHEAP);
	assert(page->m_tableId == (*m_tableDef)->m_id);
	assert(page->m_tableId == m_tableId);
	if (free->m_node == page) {
		free->m_node = page->m_next;
		if (page->m_next != NULL) {
			page->m_next->m_prev = NULL;
		}
	} else {
		page->m_prev->m_next = page->m_next;
		if (page->m_next != NULL) {
			page->m_next->m_prev = page->m_prev;
		}
	}
	free->m_size--;

	assert(free->m_node == NULL || free->m_node->m_prev == NULL);
	free->m_mutex.lock(__FILE__, __LINE__);
	if (free->m_lastFind == page) {
		free->m_lastFind = page->m_prev;
	}
	free->m_mutex.unlock();
}

/** ������пռ����ָ����С��ҳ�棬��������ڿ��пռ����ָ����С��ҳ�棬��ô�����µ�ҳ��
 * @param session �Ự
 * @param size ��Ҫ����ļ�¼��С
 * @param max ��ÿ������������������max��
 * @param force �Ƿ���Ҫǿ�Ʒ���
 * @param isNew [out] ��ʶ��ҳ���Ƿ��·���ģ�������·����ҳ�棬��Ҫ����������ӵ�freeList��
 * return ���ز��ҵ���ҳ��
 */
MRecordPage* MHeap::allocRecordPage(Session *session, u16 size, u16 max, bool force, bool *isNew) {
	assert(max > 0);
	*isNew = false;
	m_totalAllocCnt++;
	u32 totalSearch = 0;
	u8 index = MRecordPage::getFreeGrade(size);

	MRecordPage *findPage = NULL;
	FreeNode *free = NULL;
	for (int i = index; i < m_gradeNum; i++) {
		u16 tryCount = 0;
		free = m_freeList[i];
_ReStart:
		//���������µ�������ǳ���latch��lock������free lock��latch��˳������lock��latch��
		//�������������µ�����³���ԭ��¼page��latch����ҪtryLock
		if (!free->m_lock.tryLock(Shared, __FILE__, __LINE__)) {
			if (tryCount < RETRY_COUNT) {
				tryCount++;
				Thread::delay(2);
				goto _ReStart;
			} else {
				continue;
			}
		}
		if (!free->m_node) {
			free->m_lock.unlock(Shared);
			continue;
		}

		u16 count = 0;
		MRecordPage *begin = NULL;
		MRecordPage *lastFind = free->getLastFind();

		if (!lastFind || !lastFind->m_next) {
			begin = free->m_node;
		} else {
			begin = lastFind;//lastFind->m_next;
		}
		MRecordPage *next = begin;
		MRecordPage *page = NULL;

		do {
			page = next;
			if (!page->m_next) {
				next = free->m_node;
			} else {
				next = page->m_next;
			}

			count++;
			totalSearch++;
			if (page->m_freeSize < size) {
				continue;
			}

			SYNCHERE(SP_MHEAP_ALLOC_PAGE_RACE);
			//�ڳ���һ��ҳ����������Ҫlatch����һ��ҳ�棬�ڴ�Ѳ������Ⱥ�˳��
			//����Ҳֻ�������������µ��������Ҫ�ڳ���latchһ��ҳ��������ȥlatch��һ��ҳ��
			//��ʱ������õ�ԭ���ǶԵڶ���ҳ�����tryLatch��Ϊ�˱�������
			if (!tryLatchPage(session, page, Exclusived, __FILE__, __LINE__)) {
				continue;
			}

			if (page->m_freeSize < size) {
				unLatchPage(session, page, Exclusived);
				continue;
			}

			findPage = page;
			break;
		} while (next != begin && count <= max);

		if (page != NULL) {
			assert(page->m_tableId == m_tableId);
			assert(page->m_tableId == (*m_tableDef)->m_id);
		}
		free->setLastFind(page);
		free->m_lock.unlock(Shared);
		if (findPage != NULL) {
			break;
		}
	}

	m_totalSearchCnt += totalSearch;
	if (totalSearch > m_stat.m_maxSearchSize) {
		m_stat.m_maxSearchSize = totalSearch;
	}

	if (!findPage) {
#ifndef NTSE_UNIT_TEST
		if (unlikely(force)) {
			// ���ҳ����ôﵽ������ôֱ�ӷ���ʧ��
			if (session->getTNTDb()->getTNTIMPageManager()->reachedUpperBound(session->getTrans()->isBigTrx()))
				return NULL;
			session->getTNTDb()->getTNTSyslog()->log(EL_LOG, "MHeap must alloc page force");
		}
#endif
		MRecordPage *newPage = (MRecordPage *)m_pageManager->getPage(session->getId(), PAGE_MEM_HEAP, force);
		if (!newPage) {
			return NULL;
		}
		newPage->init(MPT_MHEAP, (*m_tableDef)->m_id);
		//��ӵ���Ӧ���б���
		/*u8 grade = MRecordPage::getFreeGrade(newPage->m_freeSize - size);
		free = m_freeList[grade];
		while (!free->m_lock.tryLock(Exclusived, __FILE__, __LINE__)) {
			Thread::delay(2);
		}
		addToFreeList(free, newPage);
		free->m_lock.unlock(Exclusived);*/
		findPage = newPage;
		*isNew = true;
	}

	return findPage;
}

/** ��ĳ��ҳ�����Ӧ�������Ƴ������黹��ҳ���������
 * @param session �Ự
 * @param free ��¼ҳ���ڵ�����
 * @param page ��Ҫ�Ƴ��ļ�¼ҳ
 */
void MHeap::freeRecordPage(Session *session, FreeNode *free, MRecordPage* page) {
	RWLockGuard guard(&free->m_lock, Exclusived, __FILE__, __LINE__);
	latchPage(session, page, Exclusived, __FILE__, __LINE__);
	if (page->m_recordCnt == 0) {
		removeFromFreeListSafe(free, page);
		m_pageManager->releasePage(session->getId(), page);
		return;
	}
	unLatchPage(session, page, Exclusived);
}

/** �ڴ�Ѿ����ͷ�ָ����С��ҳ��ҳ������
 * @param session �Ự
 * @param target ��Ҫ�ͷ�target��ҳ���ҳ������
 * return �����ͷŵ�ҳ����
 */
int MHeap::freeSomePage(Session *session, int target) {
	int totalFreePage = 0;
	int count = 0;
	Array<MRecordPage *> emptyPageList;
	MRecordPage *p1, *p2;
	MRecordPage *page = NULL;
	RowId rid = INVALID_ROW_ID;

	u32 beginTime = System::fastTime();
	defragFreeList(session);
	u32 endTime = System::fastTime();

#ifndef NTSE_UNIT_TEST
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "defragFreeList time is %d", endTime - beginTime);
#endif

	beginTime = endTime;
	FreeNode *free = m_freeList[3];

	while (true) {
		p1 = NULL;
		p2 = NULL;
		count = 0;
		free->m_lock.lock(Exclusived, __FILE__, __LINE__);
		if (!page) {
			page = free->m_node;
		}
		emptyPageList.clear();
		for (; page != NULL && totalFreePage < target && count < FREE_BATCH_SIZE; page = page->m_next) {
			latchPage(session, page, Exclusived, __FILE__, __LINE__);
			if (page->m_recordCnt == 0) {
				emptyPageList.push(page);
				totalFreePage++;
				count++;
				continue;
			}
			if (p1 == NULL) {
				p1 = page;
				continue;
			}

			p2 = page;
			s16 *p2Slot = NULL;
			s16 *p1Slot = NULL;
			byte *rec = NULL;
			u16 size = 0;
			//P2��������Ч��¼&& P1���㹻�Ŀռ�����P2�����һ����¼
			while (p2->getLastRecord(&p2Slot, &rec, &size) && p1->m_freeSize >= size + sizeof(s16)) {
				//�ƶ�P2�����һ����¼��P1�����p1ҳ���м京invalid slot����freeSize�㹻����ôappendRecordForce�ᴥ������ҳ��
				p1Slot = (s16 *)appendRecordForce(p1, rec, size);

				//ɾ��P2��ҳ��Ӧ��¼
				p2->removeRecord(p2Slot);

				//����rowId�޸�hash������ptr
				Stream s(rec, size);
				rid = MHeapRec::getRowId(&s);
				assert(m_pageManager->isPageLatched((TNTIMPage *)GET_PAGE_HEADER(p1Slot), Exclusived));
				m_hashIndexOperPolicy->update(rid, (u64)p1Slot);
			}

			if (p2->m_recordCnt > 0) {
				unLatchPage(session, p1, Exclusived);
				p1 = p2;
			} else {
				emptyPageList.push(p2);
				totalFreePage++;
				count++;
			}
		}

		if (p1 != NULL) {
			unLatchPage(session, p1, Exclusived);
		}
	
		size_t emptyListSize = emptyPageList.getSize();
		if (emptyListSize == 0) {
			free->m_lock.unlock(Exclusived);
			break;
		}
#ifndef NTSE_UNIT_TEST
		session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "defrag free %d pages this time", emptyListSize);
#endif
		//emptyPageList�е�ҳ��ȫ��latchס��
		for (u32 i = 0; i < emptyListSize; i++) {
			if (likely(emptyPageList[i]->m_recordCnt == 0)) {
				removeFromFreeListSafe(free, emptyPageList[i]);
				m_pageManager->releasePage(session->getId(), emptyPageList[i]);
			} else {
				assert(false);
			}
		}
		free->m_lock.unlock(Exclusived);

		//ֻ���ͷ�ҳ�����Ŀ�ﵽtargetʱ�������Ѿ������������ʱ��Ž���
		if (totalFreePage == target || !page) {
			break;
		}
	}
	endTime = System::fastTime();
#ifndef NTSE_UNIT_TEST
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "defrag page time is %d and free %d pages", endTime - beginTime, totalFreePage);
#endif

	return totalFreePage;
}

/** �ͷ�����ҳ��
 * @param session �Ự
 */
void MHeap::freeAllPage(Session *session) {
	FreeNode *free = NULL;
	MRecordPage *page = NULL;
	for (int i = 0; i <  m_gradeNum; i++) {
		free = m_freeList[i];

		free->m_lock.lock(Exclusived, __FILE__, __LINE__);
		for (page = free->m_node; page != NULL; page = page->m_next) {
			latchPage(session, page, Exclusived, __FILE__, __LINE__);
			m_pageManager->releasePage(session->getId(), page);
		}
		free->setLastFind(NULL);
		free->m_node = NULL;
		free->m_lock.unlock(Exclusived);
	}
}

/** ��ʼscan�׶Ρ�scanʱ����Ҫ����defrag����Ϊdefrag���ƻ�����Ľṹ
 * @param session �Ự
 * return scan������
 */
MHeapScanContext* MHeap::beginScan(Session *session) {
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	MHeapScanContext *scanCtx = new (ctx->alloc(sizeof(MHeapScanContext))) MHeapScanContext(session);
	scanCtx->m_page = (MRecordPage *)(ctx->alloc(TNTIMPage::TNTIM_PAGE_SIZE));
	scanCtx->m_grade = 0;
	while (scanCtx->m_grade < m_gradeNum) {
		RWLockGuard guard(&m_freeList[scanCtx->m_grade]->m_lock, Shared, __FILE__, __LINE__);
		FreeNode *free = m_freeList[scanCtx->m_grade];
		//����õȼ�������ҳ��
		if (free->m_node == NULL) {
			scanCtx->m_grade++;
		} else {
			latchPage(session, free->m_node, Shared, __FILE__, __LINE__);
			memcpy(scanCtx->m_page, free->m_node, TNTIMPage::TNTIM_PAGE_SIZE);
			unLatchPage(session, free->m_node, Shared);
			scanCtx->m_traversePageCnt++;
			break;
		}
	}
	scanCtx->m_sp = sp;
	return scanCtx;
}

/** scan�ڴ�ѻ�ȡ��һ���ļ�¼��Ŀǰֻ�����ڿ��ն�������д����ĵ�ǰ����ʵ�����ǰ�page���տ�����ctx page��
 * @param ctx scan�����ĻỰ
 * @param readView scan��¼���յ�readView
 * return ���������һ����¼����true�����򷵻�false
 */
bool MHeap::getNext(MHeapScanContext *ctx, ReadView *readView, bool *visible) {
	u16 recOffset = 0;
	u16 recSize = 0;
	byte *buf = NULL;
	s16 *slot = NULL;
	bool find = false;
	Record *rec = NULL;
	MemoryContext *memCtx = ctx->m_session->getMemoryContext();
	u64 sp = 0;

	//purge��dump��defrag��ͬʱ���С�������purge��defrag����ͬʱ���У������ͱ�֤��page��purge�׶β��ᱻ�ƶ�
	while (!find && ctx->m_grade < m_gradeNum) {
		slot = (s16 *)((byte *)ctx->m_page + sizeof(MRecordPage));
		slot += ctx->m_slotNum;

		while ((byte *)slot < (byte *)ctx->m_page + ctx->m_page->m_lastSlotOffset) {
			ctx->m_slotNum++;
			if (*slot == INVALID_SLOT) {
				slot++;
				continue;
			}
			recOffset = *slot;
			buf = (byte *)ctx->m_page + recOffset;
			recSize = *(u16 *)buf;
			
			sp = memCtx->setSavepoint();
			Stream s(buf, recSize);
			ctx->m_heapRec = MHeapRec::unSerialize(&s, memCtx, INVALID_ROW_ID, false);
			assert(ctx->m_heapRec != NULL);
			//�����ǰ�汾���ɼ�����ô�Ըü�¼������purge
			if(visible != NULL) {
				*visible = !(readView != NULL && !readView->isVisible(ctx->m_heapRec->m_txnId));
			}
			rec = new (memCtx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)memCtx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
			RecordOper::convertRecordVFR(*m_tableDef, &ctx->m_heapRec->m_rec, rec);
			memcpy(&ctx->m_heapRec->m_rec, rec, sizeof(Record));

			find = true;
			break;
		}
		

		//����һ��ҳ����û���ҵ�purge�ļ�¼���Ǽ�������һҳ��
		if (!find) {
			MRecordPage *nextPage = NULL;
			ctx->m_slotNum = 0;
			m_freeList[ctx->m_grade]->m_lock.lock(Shared, __FILE__, __LINE__);
			nextPage = ctx->m_page->m_next;
			if (nextPage != NULL) {
				latchPage(ctx->m_session, nextPage, Shared, __FILE__, __LINE__);
				memcpy(ctx->m_page, nextPage, TNTIMPage::TNTIM_PAGE_SIZE);
				unLatchPage(ctx->m_session, nextPage, Shared);
				ctx->m_traversePageCnt++;
			}
			m_freeList[ctx->m_grade]->m_lock.unlock(Shared);

			//�����һҳΪ�գ�������һ��������������
			while (nextPage == NULL && ++ctx->m_grade < m_gradeNum) {
				RWLockGuard guard(&m_freeList[ctx->m_grade]->m_lock, Shared, __FILE__, __LINE__);
				nextPage = m_freeList[ctx->m_grade]->m_node;
				if (nextPage != NULL) {
					latchPage(ctx->m_session, nextPage, Shared, __FILE__, __LINE__);
					memcpy(ctx->m_page, nextPage, TNTIMPage::TNTIM_PAGE_SIZE);
					unLatchPage(ctx->m_session, nextPage, Shared);
					ctx->m_traversePageCnt++;
					break;
				}
			}
			SYNCHERE(SP_MHEAP_PURGENEXT_REC_MODIFY);
		}
	}

	return find;
}

/** ����scan
 * @param ctx scan�����ĻỰ
 */
void MHeap::endScan(MHeapScanContext *ctx) {
	ctx->m_session->getMemoryContext()->resetToSavepoint(ctx->m_sp);
}

/** purge�ĵڶ��׶�
 * @param session �Ự
 * @param readView �ɼ����ж�
 * return purge�ļ�¼��
 */
u64 MHeap::purgePhase2(Session *session, ReadView *readView) {
	u64 count = 0;
	u8 grade = 0;
	s16 *slot = NULL;
	MRecordPage *prevPage = NULL;
	MRecordPage *page = NULL;
	u16 recOffset = 0;
	u16 recSize = 0;
	byte *buf = NULL;
	MHeapRec *heapRec = NULL;
	u64 sp = 0;
	MemoryContext *ctx = session->getMemoryContext();
	FreeNode *free = NULL;
	u32 batchPageCnt;
	u32 freePageCnt = 0;
	while (grade < m_gradeNum) {
		free = m_freeList[grade];
		free->m_lock.lock(Exclusived, __FILE__, __LINE__);
		if (!free->m_node) {
			free->m_lock.unlock(Exclusived);
			grade++;
			continue;
		}

		batchPageCnt = 0;
		if (!page) {
			page = free->m_node;
		}

		while (page != NULL && batchPageCnt < PURGE2_BATCH_SIZE) {
			latchPage(session, page, Exclusived, __FILE__, __LINE__);
			slot = (s16 *)((byte *)page + sizeof(MRecordPage));
			while ((byte *)slot < (byte *)page + page->m_lastSlotOffset) {
				if (*slot == INVALID_SLOT) {
					slot++;
					continue;
				}
				recOffset = *slot;
				buf = (byte *)page + recOffset;
				recSize = *(u16 *)buf;
				sp = ctx->setSavepoint();
				Stream s(buf, recSize);
				heapRec = MHeapRec::unSerialize(&s, ctx);
				//pickRowLock��Ҫ���ڸü�¼scan�󱻼�������heapRec��δ�޸ģ�
				//��ô��update����deleteǰ���purgeժ������������
				if (!readView->isVisible(heapRec->m_txnId) ||
					(heapRec->m_del != FLAG_MHEAPREC_DEL && //NTSETNT-188
					session->getTrans()->pickRowLock(heapRec->m_rec.m_rowId, (*m_tableDef)->m_id, true))) {
					ctx->resetToSavepoint(sp);
					slot++;
					continue;
				}

				SYNCHERE(SP_MHEAP_PURGE_PHASE2_PICKROW_AFTER);
				//��ɾ����ϣ�������ɾ������
				m_hashIndexOperPolicy->remove(heapRec->m_rec.m_rowId, HIT_MHEAPREC, (u64)slot);
				page->removeRecord(slot++);
				ctx->resetToSavepoint(sp);
				count++;
			}

			prevPage = page;
			page = page->m_next;

			if (prevPage->m_recordCnt == 0) {
				removeFromFreeListSafe(free, prevPage);
				m_pageManager->releasePage(session->getId(), prevPage);
				freePageCnt++;
			} else {
				unLatchPage(session, prevPage, Exclusived);
			}
			batchPageCnt++;
		}
		free->m_lock.unlock(Exclusived);
		
		if (page == NULL) {
			grade++;
		}
	}
	
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "purge phase2 and release %d pages", freePageCnt);
	return count;
}

/** ��dump�ļ�����incrSize���ֽ�
 * @param file ��Ҫ�����dump�ļ����
 * @param incrSize ��Ҫ������ֽ���
 */
void MHeap::extendDumpFileSize(File *file, u64 incrSize) throw (NtseException) {
	u64 srcSize;
	u64 code = file->getSize(&srcSize);
	if (code != File::E_NO_ERROR) {
		NTSE_THROW(NTSE_EC_FILE_FAIL, "get dumpfile(path = %s) size error", file->getPath());
	}
	
	code = file->setSize(srcSize + incrSize);
	if (code != File::E_NO_ERROR) {
		NTSE_THROW(NTSE_EC_FILE_FAIL, "Extend head dumpfile(path = %s) error", file->getPath());
	}
}

/** ��dump�ļ�д������
 * @param file dump�ļ����
 * @param offset dump�ļ���ʼд��ƫ����
 * @param buf ��Ҫд�������
 * @param size ��Ҫд����ֽ���
 */
void MHeap::writeDumpFile(File *file, u64 offset, byte *buf, u32 size) throw (NtseException) {
	u64 code = File::E_NO_ERROR;
	do {
		if (code == File::E_EOF) {
			extendDumpFileSize(file, EXTEND_DUMPFILE_SIZE);
		}
		code = file->write(offset, size, buf);
	} while (code == File::E_EOF);

	if (code != File::E_NO_ERROR) {
		NTSE_THROW(NTSE_EC_WRITE_FAIL, "write head dumpfile(path = %s) error offset="I64FORMAT"u", file->getPath(), offset);
	}
}

/** дdump�ļ��Ľ���ҳ
 * @param session �Ự
 * @param file dump�ļ����
 * @param offset [in, out] in д�ļ���ƫ������out �´�д�ļ�ʱ��ƫ����
 */
void MHeap::writeDumpEndPage(Session *session, File *file, u64 *offset) throw (NtseException) {
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	MRecordPage *page = (MRecordPage *)m_pageManager->getPage(session->getId(), PAGE_DUMP, true);
	memset(page, 0, MRecordPage::TNTIM_PAGE_SIZE);
	page->init(MPT_DUMP_END, (*m_tableDef)->m_id);
	writeDumpFile(file, *offset, (byte *)page, MRecordPage::TNTIM_PAGE_SIZE);
	*offset += MRecordPage::TNTIM_PAGE_SIZE;
	m_pageManager->releasePage(session->getId(), page);
}

/** ��ʼִ��dump
 * @param file dump�ļ�
 * @param offset [in,out] in д�ļ���ƫ������out �´�д�ļ�ʱ��ƫ����
 * @param ctx �����ķ����ڴ�
 * return �ɹ�����true�����򷵻�false
 */
bool MHeap::beginDump(Session *session, File *file, u64 *offset) {
	m_statusLock.lock(__FILE__, __LINE__);
	assert(m_state == TNTIM_NOOP && m_compensateRows.getSize() == 0);
	m_state = TNTIM_DUMPING;
	m_statusLock.unlock();

	UNREFERENCED_PARAMETER(session);
	UNREFERENCED_PARAMETER(file);
	UNREFERENCED_PARAMETER(offset);
	return true;
}

/**
 * ��ʼִ��purge
 */
bool MHeap::beginPurge() {
	m_statusLock.lock(__FILE__, __LINE__);
	assert(m_state == TNTIM_NOOP && m_compensateRows.getSize() == 0);
	m_state = TNTIM_PURGEING;
	m_statusLock.unlock();

	return true;
}

//����dump����
void MHeap::finishDumpCompensate() {
	m_compensateRows.clear();
}

/** ����purge���� */
void MHeap::finishPurgeCompensate() {
	m_compensateRows.clear();
}

void MHeap::resetStatus() {
	//��Ϊ���е�ҳ���Ѿ���purge����������û�б�Ҫ�ټ�¼����rowId����
	m_statusLock.lock(__FILE__, __LINE__);
	assert(m_state == TNTIM_DUMPING || m_state == TNTIM_PURGEING);
	m_state = TNTIM_NOOP;
	m_statusLock.unlock();
}

/** ���ɼ��ڴ�Ѽ�¼dumpָ�����ļ���ȥ
 * @param session �Ự
 * @param readView �ɼ����ж�
 * @param file ���dump��¼�������ļ�
 * @param offset [in,out] in д�ļ���ƫ���� out��һ��д���ļ���ƫ����
 * @param versionPoolPolicy �汾�ز���
 * return �ɹ�����true�����򷵻�false
 */
bool MHeap::dumpRealWork(Session *session, ReadView *readView, File *file, u64 *offset, VersionPoolPolicy *versionPoolPolicy) throw (NtseException) {
	SYNCHERE(SP_MHEAP_DUMPREALWORK_BEGIN);
	u32 i = 0;
	u16 j = 0;
	MHeapRec *heapRec = NULL;
	MRecordPage *page = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	byte *buf = NULL;
	u16 bufSize = 0;
	s16 *slot = NULL;
	MRecordPage *bufPage = (MRecordPage *)m_pageManager->getPage(session->getId(), PAGE_DUMP);
	if (bufPage == NULL) {
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "dump real work can't get page");
	}
	memset(bufPage, 0, MRecordPage::TNTIM_PAGE_SIZE);
	bufPage->init(MPT_DUMP, (*m_tableDef)->m_id);
	Array<MHeapRec> heapRecs;

	u32 dumpPageCnt = 0;
	FreeNode *free = NULL;
	for (i = 0; i < m_gradeNum; i++) {
		free = m_freeList[i];
		free->m_lock.lock(Shared, __FILE__, __LINE__);
		if (!free->m_node) {
			free->m_lock.unlock(Shared);
			continue;
		}

		McSavepoint msp1(ctx);
		page = free->m_node;
		assert(page != NULL);
		latchPage(session, page, Shared, __FILE__, __LINE__);
		free->m_lock.unlock(Shared);

		while (page != NULL) {
			McSavepoint msp2(ctx);
			assert(m_pageManager->isPageLatched(page, Shared));
			slot = (s16 *)((byte *)page + sizeof(MRecordPage));
			byte *slotEnd = (byte *)page + page->m_lastSlotOffset;
			for (; (byte *)slot < slotEnd; slot++) {
				if (!page->getRecord(slot, &buf, &bufSize)) {
					continue;
				}
				Stream s(buf, bufSize);
				heapRec = MHeapRec::unSerialize(&s, ctx, INVALID_ROW_ID, false);
				if (!readView->isVisible(heapRec->m_txnId)) {
					Stream s1(buf, bufSize);
					heapRec = MHeapRec::unSerialize(&s1, ctx, INVALID_ROW_ID, true);
					heapRecs.push(*heapRec);
					continue;
				}

				//ÿ��dumpһ��ҳ�濪ʼʱ��bufPage�϶�Ϊ�գ�
				//�������������writeDumpPageRecord�϶�����дӲ��
				try {
					writeDumpPageRecord(bufPage, buf, bufSize, file, offset);
				} catch (NtseException &e) {
					unLatchPage(session, page, Shared);
					m_pageManager->releasePage(session->getId(), bufPage);
					throw e;
				}
			}
			unLatchPage(session, page, Shared);

			//dump���ɼ���¼�Ŀɼ��汾
			for (j = 0; j < heapRecs.getSize(); j++) {
				heapRec = &heapRecs[j];
				heapRec = getVersionHeapRecord(session, heapRec, readView, versionPoolPolicy);
				if (heapRec == NULL) {
					continue;
				}
				bufSize = heapRec->getSerializeSize();
				buf = (byte *)ctx->alloc(bufSize);
				Stream s(buf, bufSize);
				heapRec->serialize(&s);
				writeDumpPageRecord(bufPage, buf, bufSize, file, offset);
			}
			heapRecs.clear();

			//��bufPage��δд�벿��д���ļ���
			//�����д��ʣ�ಿ�֣���ô���ܵ�����һ��page�ڵ�һ��writeDumpPageRecordʱдӲ��
			if (bufPage->m_recordCnt > 0) {
				assert(bufPage->m_pageType == MPT_DUMP);
				writeDumpFile(file, *offset, (byte *)bufPage, MRecordPage::TNTIM_PAGE_SIZE);
				*offset += MRecordPage::TNTIM_PAGE_SIZE;
				memset(bufPage, 0, MRecordPage::TNTIM_PAGE_SIZE);
				bufPage->init(MPT_DUMP, (*m_tableDef)->m_id);
			}

			dumpPageCnt++;
			SYNCHERE(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
			//TODO ���Կ��ǲ���freeNode��S��
			free->m_lock.lock(Shared, __FILE__, __LINE__);
			page = page->m_next;
			if (page != NULL) {
				latchPage(session, page, Shared, __FILE__, __LINE__);
			}
			free->m_lock.unlock(Shared);
		}
	}
	m_pageManager->releasePage(session->getId(), bufPage);
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "dumpRealWork (path = %s) pageCnt is %d && file size is "I64FORMAT"u", file->getPath(), dumpPageCnt, *offset);

	return true;
}

/** ������rowid��Ӧ�ļ�¼dump��ָ���ļ���ȥ
 *  ��dumpҳ�У������������µ��¼�¼��ʧ����
 * @param session �Ự
 * @param readView �ɼ����ж�
 * @param file ���dump���ݵ��ļ�
 * @param offset [in,out] in д�ļ���ƫ���� out �´�дdump�ļ���ƫ����
 * @param versionPoolPolicy �汾�ز���
 * return �ɹ�����true��ʧ�ܷ���false
 */
bool MHeap::dumpCompensateRow(Session *session, ReadView *readView, File *file, u64 *offset, VersionPoolPolicy *versionPoolPolicy) throw (NtseException) {
	size_t size = m_compensateRows.getSize();
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "need dump Compensate Row size is %d", size);
	RowId rid = 0;
	MHeapRec *rec = NULL;
	HashIndexEntry *entry = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp1(ctx);
	byte *buf = NULL;
	u16 bufSize = 0;
	MRecordPage *page = (MRecordPage *)m_pageManager->getPage(session->getId(), PAGE_DUMP);
	if (page == NULL) {
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "dump compensate row can't get page");
	}
	memset(page, 0, MRecordPage::TNTIM_PAGE_SIZE);
	page->init(MPT_DUMP_COMPENSATE, (*m_tableDef)->m_id);
	for (u16 i = 0; i < size; i++) {
		u64 sp2 = 0;
		rid = m_compensateRows[i];
_ReStart:
		sp2 = ctx->setSavepoint();
		entry = m_hashIndexOperPolicy->get(rid, ctx);
		NTSE_ASSERT(entry != NULL && entry->m_type == HIT_MHEAPREC);
		SYNCHERE(SP_MHEAP_DUMPCOMPENSATE_REC_MODIFY);
		//�����ʱ��rid��Ӧ�ļ�¼�����������£���ô���ص�recΪNULL����Ҫ���¶�λ
		rec = getHeapRecord(session, (void *)entry->m_value, rid);
		if (rec == NULL) {
			ctx->resetToSavepoint(sp2);
			goto _ReStart;
		}

		/*if (!readView->isVisible(rec->m_txnId)) {
			rec = getVersionHeapRecord(session, rec, readView, versionPoolPolicy);
		} else if (rec->m_del == FLAG_MHEAPREC_DEL) {
			ctx->resetToSavepoint(sp2);
			continue;
		}*/
		//��Ϊ������������dumpʱ��Ծ������������������ģ����Ըü�¼���ڴ��еİ汾��dump��Ȼ���ɼ�
		assert(!readView->isVisible(rec->m_txnId));
		rec = getVersionHeapRecord(session, rec, readView, versionPoolPolicy);
		if (rec == NULL) {
			ctx->resetToSavepoint(sp2);
			continue;
		}

		bufSize = rec->getSerializeSize();
		buf = (byte *)ctx->alloc(bufSize);
		Stream s(buf, bufSize);
		rec->serialize(&s);

		writeDumpPageRecord(page, buf, bufSize, file, offset);
		ctx->resetToSavepoint(sp2);
	}

	if (page->m_recordCnt > 0) {
		assert(page->m_pageType == MPT_DUMP_COMPENSATE);
		writeDumpFile(file, *offset, (byte *)page, MRecordPage::TNTIM_PAGE_SIZE);
		*offset += MRecordPage::TNTIM_PAGE_SIZE;
	}
	m_pageManager->releasePage(session->getId(), page);
	session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "dumpCompensateRow (path = %s) file size is "I64FORMAT"u", file->getPath(), *offset);
	return true;
}

/** ����readView��ȡ�汾���пɼ�������Ѽ�¼�����record��¼ΪREC_REDUNT��ʽ
 * @param session �Ự
 * @param heapRec �ڴ��жѼ�¼
 * @param readView �жϰ汾�Ŀɼ���
 * @param versionPool �汾�ز���
 * return �ɼ����ڴ�Ѽ�¼������record��¼ΪREC_REDUNT��ʽ
 */
MHeapRec* MHeap::getVersionHeapRedRecord(Session *session, MHeapRec *heapRec, ReadView *readView, VersionPoolPolicy *versionPool) {
	assert(heapRec->m_rec.m_format == REC_VARLEN || heapRec->m_rec.m_format == REC_FIXLEN);
	MHeapRec *ret = NULL;
	HeapRecStat stat;
	MemoryContext *ctx = session->getMemoryContext();
	u64 sp = ctx->setSavepoint();
	Record *destRec = new (ctx->alloc(sizeof(Record))) Record(heapRec->m_rec.m_rowId, REC_REDUNDANT, (byte *)ctx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
	RecordOper::convertRecordVFR(*m_tableDef, &heapRec->m_rec, destRec);
	ret = versionPool->getVersionRecord(session, *m_tableDef, heapRec, readView, destRec, &stat);
	if (stat == NTSE_VISIBLE || stat == NTSE_UNVISIBLE) {
		ctx->resetToSavepoint(sp);
		return NULL;
	}
	return ret;
}

/** ����readView��ȡ�汾���пɼ��ĶѼ�¼��
 * @param session �Ự
 * @param heapRec �ڴ��жѼ�¼
 * @param readView �жϰ汾�Ŀɼ���
 * @param versionPool �汾�ز���
 * return �ɼ����ڴ�Ѽ�¼
 */
MHeapRec* MHeap::getVersionHeapRecord(Session *session, MHeapRec *heapRec, ReadView *readView, VersionPoolPolicy *versionPool) {
	Record *varRec = NULL;
	MHeapRec *ret = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	if (heapRec->m_rec.m_format == REC_VARLEN) {
		varRec = new (ctx->alloc(sizeof(Record))) Record(heapRec->m_rec.m_rowId, REC_VARLEN, (byte *)ctx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
		ret = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec();
	}
	u64 sp = ctx->setSavepoint();
	MHeapRec *versionHeapRec = getVersionHeapRedRecord(session, heapRec, readView, versionPool);
	if (versionHeapRec == NULL) {
		ctx->resetToSavepoint(sp);
		return NULL;
	}

	if (heapRec->m_rec.m_format == REC_VARLEN) {
		RecordOper::convertRecordRV(*m_tableDef, &versionHeapRec->m_rec, varRec);
		ret->m_txnId = versionHeapRec->m_txnId;
		ret->m_rollBackId = versionHeapRec->m_rollBackId;
		ret->m_vTableIndex = versionHeapRec->m_vTableIndex;
		memcpy(&ret->m_rec, varRec, sizeof(Record));
		ret->m_del = versionHeapRec->m_del;
		ctx->resetToSavepoint(sp);
	} else {
		assert(heapRec->m_rec.m_format == REC_FIXLEN);
		ret = versionHeapRec;
		ret->m_rec.m_format = REC_FIXLEN;
	}
	ret->m_size = ret->getSerializeSize();
	return ret;
}

/** ���ڴ��¼д��page�����pageҳ����pageд��dump�ļ�
 * @param page ҳ��
 * @param buf ��¼���л��ֽ�
 * @param size ��¼���л��ֽ���
 * @param file dump�ļ�
 * @param offset [in,out] in д�ļ���ƫ������out �´�д�ļ�ʱ��ƫ����
 * return �ɹ�����true�����򷵻�false
 */
bool MHeap::writeDumpPageRecord(MRecordPage *page, byte *buf, u16 size, File *file, u64* offset) throw(NtseException) {
	s16 *slot = page->appendRecord(buf, size);
	//���page����
	if (!slot) {
		assert(page->m_pageType == MPT_DUMP_COMPENSATE || page->m_pageType == MPT_DUMP);
		writeDumpFile(file, *offset, (byte *)page, MRecordPage::TNTIM_PAGE_SIZE);
		*offset += MRecordPage::TNTIM_PAGE_SIZE;
		MPageType pageType = page->m_pageType;
		memset(page, 0, MRecordPage::TNTIM_PAGE_SIZE);
		page->init(pageType, (*m_tableDef)->m_id);
		slot = page->appendRecord(buf, size);
		assert(slot != NULL);
	}
	return true;
}

/**dump�ָ�����Ϊdump�ָ����ڵ��̵߳Ļ�����ִ�У����Բ���Ҫ�����̰߳�ȫ
 * @param session �Ự
 * @param file dump�ļ�
 * @param offset [in,out] in ��ȡ�ļ���ƫ���� out ��ȡ����ʱ��ƫ����
 * return �ɹ�����true�����򷵻�false
 */
bool MHeap::readDump(Session *session, RowIdVersion version, File *file, u64 *offset, u64 fileSize) {
	MRecordPage *page = NULL;
	u64 code;

	while (*offset < fileSize) {
		page = (MRecordPage *)m_pageManager->getPage(session->getId(), PAGE_MEM_HEAP, true);
		assert(page != NULL);
		code = file->read(*offset, MRecordPage::TNTIM_PAGE_SIZE, page);
		if (code != File::E_NO_ERROR) {
			m_pageManager->releasePage(session->getId(), page);
			return false;
		}

		//����rowid
		if (page->m_pageType == MPT_DUMP_COMPENSATE) {
			buildHashIndex(session, page, version, true);
			m_pageManager->releasePage(session->getId(), page);
		} else if (page->m_pageType == MPT_DUMP) {
			buildHashIndex(session, page, version, false);
			page->m_prev = NULL;
			page->m_next = NULL;
			page->m_pageType = MPT_MHEAP;
			addToFreeList(page);
			unLatchPage(session, page, Exclusived);
		} else {
			assert(page->m_pageType == MPT_DUMP_END);
			unLatchPage(session, page, Exclusived);
			break;
		}

		*offset += MRecordPage::TNTIM_PAGE_SIZE;
		page = NULL;
	}

	return true;
}

/** ��ָ�����ڴ�Ѽ�¼ҳ�ϵļ�¼���н���������
 * @param session �Ự
 * @param page ָ���ڴ�Ѽ�¼ҳ
 * @param version �����汾��
 * @param compensate �Ƿ�Ϊ����ҳ��
 */
void MHeap::buildHashIndex(Session *session, MRecordPage *page, RowIdVersion version, bool compensate) {
	MemoryContext *ctx = session->getMemoryContext();
	byte *ptr = NULL;
	MHeapRec *rec = NULL;
	u32 bufSize = 0;
	McSavepoint msp(ctx);
	s16 *slot = (s16*)((byte *)page + sizeof(MRecordPage));
	s16 *slotEnd = (s16*)((byte *)page + page->m_lastSlotOffset);
	for (; slot < slotEnd; slot++) {
		assert(*slot != INVALID_SLOT);
		ptr = (byte *)page + *slot;
		bufSize = *(u16 *)ptr;
		Stream s(ptr, bufSize);
		rec = MHeapRec::unSerialize(&s, ctx, INVALID_ROW_ID, false);
		if (compensate) {
			HashIndexEntry *entry = m_hashIndexOperPolicy->get(rec->m_rec.m_rowId, ctx);
			if (entry == NULL) {
				insertHeapRecordAndHash(session, rec, version);
			} /*else {
				//���ڲ���ҳ�棬��Ϊ�Ǹ���readView������dump����������ü�¼�Ѿ���dump��
				//��ô����readView��ȡ���Ŀɼ���¼��һ�µ�,����û�б�Ҫ����update
				updateHeapRecordAndHash(session, entry->m_rowId, rec, version);
			}*/
		} else {
			void *srcPtr = (void *)m_hashIndexOperPolicy->insertOrUpdate(rec->m_rec.m_rowId, (u64)slot, version, HIT_MHEAPREC);
			if (srcPtr != NULL) {
				//��Ϊ�������ֻ��dump�ָ�ʱ���ã����ָ�Ŀǰ�ǵ��̲߳�����������ʱʡȥ��ҳ���latch
				MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(srcPtr);
				page->removeRecord((s16 *)srcPtr);
			}
		}
	}
}

/** ��������������ҳ�治ͣ������ռ��ҳ����еȼ����������
 *  ��ʱ���ǲ��õĲ���������������ҳ�浽��Ӧ�ĵȼ�������ȥ��
 *  ����ͨ����������ʹ���λ����Ӧ�Ŀ��еȼ�������ȥ
 * @param session �Ự
 */
void MHeap::defragFreeList(Session *session) {
	Array<MRecordPage *> *targetList = new Array<MRecordPage *>[m_gradeNum];
	u8 grade = 0;
	u8 targetGrade = 0;
	u32 i = 0;
	MRecordPage* page = NULL;
	MRecordPage* changePage = NULL;
	FreeNode *free = NULL;
	Array<MRecordPage *> changePages;
	while (grade < m_gradeNum) {
		free = m_freeList[grade];
		free->m_lock.lock(Exclusived, __FILE__, __LINE__);
		if (!page) {
			page = free->m_node;
		}
		for (; page != NULL && changePages.getSize() < DEFRAG_BATCH_SIZE; page = page->m_next) {
			if (MRecordPage::getFreeGrade(page->m_freeSize) != grade) {
				changePages.push(page);
			}
		}

		if (changePages.isEmpty()) {
			free->m_lock.unlock(Exclusived);
			grade++;
			continue;
		}

		SYNCHERE(SP_MHEAP_DEFRAG_WAIT);
		for (targetGrade = 0; targetGrade < m_gradeNum; targetGrade++) {
			targetList[targetGrade].clear();
		}

		for (i = 0; i < changePages.getSize(); i++) {
			changePage = changePages[i];
			latchPage(session, changePage, Exclusived, __FILE__, __LINE__);
			//�����ҳû���κμ�¼�ˣ�ֱ���ͷŸ�manager
			if (changePage->m_recordCnt == 0) {
				removeFromFreeListSafe(free, changePage);
				m_pageManager->releasePage(session->getId(), changePage);
#ifndef NTSE_UNIT_TEST
				session->getTNTDb()->getTNTSyslog()->log(EL_DEBUG, "release one page from memHeap this time");
#endif
				continue;
			}
			targetGrade = MRecordPage::getFreeGrade(changePage->m_freeSize);
			if (targetGrade == grade) {
				unLatchPage(session, changePage, Exclusived);
				continue;
			}
			targetList[targetGrade].push(changePage);
			removeFromFreeListSafe(free, changePage);
			unLatchPage(session, changePage, Exclusived);
		}

		if (free->m_node != NULL) {
			assert(free->m_node->m_tableId == m_tableId);
			assert(free->m_node->m_tableId == (*m_tableDef)->m_id);
		}
		free->setLastFind(free->m_node);
		free->m_lock.unlock(Exclusived);

		for (targetGrade = 0; targetGrade < m_gradeNum; targetGrade++) {
			if (targetList[targetGrade].isEmpty()) {
				continue;
			}
			assert(targetGrade != grade);
			m_freeList[targetGrade]->m_lock.lock(Exclusived, __FILE__, __LINE__);
			for (i = 0; i < targetList[targetGrade].getSize(); i++) {
				addToFreeListSafe(m_freeList[targetGrade], (targetList[targetGrade])[i]);
			}
			m_freeList[targetGrade]->m_lock.unlock(Exclusived);
		}

		changePages.clear();
		if (page == NULL) {
			grade++;
		}
	}

	delete[] targetList;
}

/** ����ptr��ȡ��ָ����ڴ�Ѽ�¼
 * @pre ptr��ָ���ҳ��û�б�latch
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param rid ����ڴ�Ѽ�¼��RowIdֵ�Ƿ����rid����������ڣ�˵��slot�����ã���Ҫ�ض�λ������NULL
 * return ����ptrָ���ڴ�Ѽ�¼�����ڴ�Ѽ�¼��RowId����rid�����򷵻�NULL
 */
MHeapRec* MHeap::getHeapRecord(Session *session, void *ptr, RowId rid) {
	MRecordPage *page = NULL;
	MHeapRec *heapRec = getHeapRecordAndLatchPage(session, ptr, rid, Shared, true, &page);
	if (heapRec != NULL) {
		unLatchPage(session, page, Shared);
	}
	
	return heapRec;
}

/** ����ptr��ȡ��¼���Ҹü�¼��RowIdֵ�������rid
 * @pre ptr��ָ���ҳ���Ѿ���latchס
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param rid ����ڴ�Ѽ�¼��RowIdֵ�Ƿ����rid
 * return ����ptrָ���ڴ�Ѽ�¼�����ڴ�Ѽ�¼��RowId�������rid
 */
MHeapRec* MHeap::getHeapRecordSafe(Session *session, void *ptr, RowId rid) {
	byte *buf = NULL;
	u16 size = 0;
	MemoryContext *ctx = session->getMemoryContext();
	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	assert(m_pageManager->getPageLatchMode(page) >= Shared);
	NTSE_ASSERT(page->getRecord((s16 *)ptr, &buf, &size));
	Stream s(buf, size);
	MHeapRec *heapRec = MHeapRec::unSerialize(&s, ctx, rid, false);
	assert(heapRec != NULL);
	return heapRec;
}

/** ����ptr��ȡRowIdֵΪrid�ļ�¼
 * @pre ptr��ָ���ҳ��û�б�latch
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param rid ����ڴ�Ѽ�¼��RowIdֵ�Ƿ����rid����������ڣ�˵��slot�����ã���Ҫ�ض�λ������NULL
 * @param rec out ���������Ӧ��¼������rec_redunt��ʽ�ļ�¼
 * return ����ptrָ���ڴ�Ѽ�¼�����ڴ�Ѽ�¼��RowId����rid�����򷵻�NULL
 */
MHeapRec* MHeap::getHeapRedRecord(Session *session, void *ptr, RowId rid, Record *rec) {
	MRecordPage *page = NULL;
	MHeapRec *heapRec = getHeapRecordAndLatchPage(session, ptr, rid, Shared, false, &page);
	if (heapRec == NULL) {
		return NULL;
	}

	RecordOper::convertRecordVFR(*m_tableDef, &heapRec->m_rec, rec);
	memcpy(&heapRec->m_rec, rec, sizeof(Record));
	unLatchPage(session, page, Shared);

	return heapRec;
}

/** ����ptr��ȡ��¼���Ҹü�¼��RowIdֵ�������rid
 * @pre ptr��ָ���ҳ���Ѿ���latchס
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param rid ����ڴ�Ѽ�¼��RowIdֵ�Ƿ����rid
 * @param rec out ���������Ӧ��¼������rec_redundant��ʽ�ļ�¼
 * return ����ptrָ���ڴ�Ѽ�¼�����ڴ�Ѽ�¼��RowId�������rid
 */
MHeapRec* MHeap::getHeapRedRecordSafe(Session *session, void *ptr, RowId rid, Record *rec) {
	byte *buf = NULL;
	u16 size = 0;
	MemoryContext *ctx = session->getMemoryContext();
	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	assert(m_pageManager->getPageLatchMode(page) >= Shared);
	NTSE_ASSERT(page->getRecord((s16 *)ptr, &buf, &size));
	Stream s(buf, size);
	MHeapRec *heapRec = MHeapRec::unSerialize(&s, ctx, rid, false);
	assert(heapRec != NULL);
	assert(rec->m_rowId == INVALID_ROW_ID);
	RecordOper::convertRecordVFR(*m_tableDef, &heapRec->m_rec, rec);
	memcpy(&heapRec->m_rec, rec, sizeof(Record));

	return heapRec;
}

/** ����ptr��rowId��ȡ��Ӧ���ڴ�Ѽ�¼��ͬʱ���ؼ�¼���ڵ�ҳ��
 * @post ���������Ӧ�ļ�¼��ͬʱ���ؼ�¼���ڵ�ҳ�棬���Ҹ�ҳ�汻latch
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param rid ����ڴ�Ѽ�¼��RowIdֵ�Ƿ����rid����������ڣ�˵��slot�����ã���Ҫ�ض�λ������NULL
 * @param copyData record��data�������ռ仹��ָ��ҳ��ռ�
 * @param page out ���������Ӧ��¼�����ظü�¼���ڵ�ҳ��
 * return ����ptrָ���ڴ�Ѽ�¼�����ڴ�Ѽ�¼��RowId����rid�����򷵻�NULL
 */
MHeapRec* MHeap::getHeapRecordAndLatchPage(Session *session, void *ptr, RowId rid, LockMode mode, bool copyData, MRecordPage **page) {
	byte *buf = NULL;
	u16 size = 0;
	MemoryContext *ctx = session->getMemoryContext();
	MRecordPage *page1 = (MRecordPage *)GET_PAGE_HEADER(ptr);
	if (!latchPageIfType(session, page1, mode, PAGE_MEM_HEAP, -1, __FILE__, __LINE__)) {
		return NULL;
	}
	if (page1->m_tableId != (*m_tableDef)->m_id || !page1->getRecord((s16 *)ptr, &buf, &size)) {
		unLatchPage(session, page1, mode);
		return NULL;
	}
	Stream s(buf, size);
	MHeapRec *heapRec = MHeapRec::unSerialize(&s, ctx, rid, copyData);
	if (!heapRec) {
		unLatchPage(session, page1, mode);
		return NULL;
	}
	*page = page1;
	return heapRec;
}

/** ����rowId��ȡ��Ӧ��slotָ������ڵ�pageҳ��
 * @post �����ҳ�淵�أ���ô��ҳ���Ѿ���latch
 * @param session �Ự
 * @param rid ����rowId��ȡ��Ӧ��ptrֵ�Ͷ�Ӧ��ҳ��
 * @param page out �������Ӧ��ҳ��
 * return ������ҵ�rid��Ӧ�ļ�¼������ptrֵ�ͼ�¼���ڵ�ҳ�棬���򷵻�NULL
 */
void* MHeap::getSlotAndLatchPage(Session *session, RowId rid, LockMode mode, MRecordPage **page) {
	MRecordPage *page1 = NULL;
	MemoryContext *ctx = session->getMemoryContext();
_ReStart:
	HashIndexEntry *entry = m_hashIndexOperPolicy->get(rid, ctx);
	if (entry == NULL) {
		return NULL;
	}
	assert(entry->m_type == HIT_MHEAPREC);
	SYNCHERE(SP_MHEAP_GETSLOTANDLATCHPAGE_REC_MODIFY);
	MHeapRec *heapRec = getHeapRecordAndLatchPage(session, (void *)entry->m_value, rid, mode, true, &page1);
	if (!heapRec) {
		goto _ReStart;
	}

	*page = page1;
	return (void *)entry->m_value;
}

/** ���ptr��ָ��ļ�¼rowId�Ƿ����rid������ǵĻ���latchס��ҳ�棬������true�����򷵻�false
 * @post ���ptr��ָ��ļ�¼��rowId����rid����ô��¼���ڵ�ҳ�潫��latch
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param rid ptr��ָ���¼������rowId
 * return ���ptr��ָ��ļ�¼rowId����rid������true�����򷵻�false
 */
bool MHeap::checkAndLatchPage(Session *session, void *ptr, RowId rid, LockMode mode) {
	bool ret = false;
	byte *buf = NULL;
	u16 size = 0;
	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	if (!latchPageIfType(session, page, mode, PAGE_MEM_HEAP, -1, __FILE__, __LINE__)) {
		return ret;
	}
	if (page->m_tableId != (*m_tableDef)->m_id || !page->getRecord((s16 *)ptr, &buf, &size)) {
		unLatchPage(session, page, mode);
		return ret;
	}
	
	Stream s(buf, size);
	if (rid != MHeapRec::getRowId(&s)) {
		unLatchPage(session, page, mode);
		ret = false;
	} else {
		ret = true;
	}
	return ret;
}

/** ��recSize��С�ļ�¼����pageҳ������һ����֤�ɹ�
 * @param page ��Ҫ�����¼��page
 * @param buf  ��¼���л�
 * @param recSize ��¼���л����С
 */
void *MHeap::appendRecordForce(MRecordPage *page, byte *buf, u16 recSize) {
	assert(m_pageManager->isPageLatched(page, Exclusived));
	assert(page->m_freeSize >= recSize + sizeof(s16));
	void *ptr = (void *)page->appendRecord(buf, recSize);
	if (!ptr) {//��ʱ���벻�ɹ�����Ҫҳ��������
		page->defrag(m_hashIndexOperPolicy);
		ptr = (void *)page->appendRecord(buf, recSize);
		assert(ptr != NULL);
	}
	return ptr;
}

/** ��ptrָ��ļ�¼�ڱ�ҳ�����ΪrecSize��С���¼�¼������һ����֤�ɹ�
 * @param page ��Ҫ�����¼��page
 * @param ptr  ��Ҫ���¼�¼��ptr
 * @param buf  ���º����¼�����л�
 * @param recSize ���º����¼���л��Ĵ�С
 */
void *MHeap::updateRecordForce(MRecordPage *page, void *ptr, byte *buf, u16 recSize) {
	assert(m_pageManager->isPageLatched(page, Exclusived));
	assert(page->m_freeSize >= recSize - page->getRecordSize((s16*)ptr));
	void *newPtr = page->updateRecord((s16 *)ptr, buf, recSize);
	if (!newPtr) {//˵����ʱremove�ɹ�����appendRecordʧ��
		page->defrag(m_hashIndexOperPolicy);
		newPtr = page->appendRecord(buf, recSize);
		assert(newPtr != NULL);
	}
	return newPtr;
}

/** ���ڴ���в����¼����hash����
 * @param session �Ự
 * @param txnId �������������id
 * @param rollBackId �ع���¼id
 * @param vTableIndex �ع���¼���ڰ汾�ر��е����
 * @param rec ��ǰ���¼�¼
 * @param delbit ɾ�����λ
 * @param version hash������version
 * return �ɹ�����true�����򷵻�false
 */
bool MHeap::insertHeapRecordAndHash(Session *session, TrxId txnId, RowId rollBackId, u8 vTableIndex, Record *rec, u8 delbit, RowIdVersion version) {
	MHeapRec heapRec(txnId, rollBackId, vTableIndex, rec, delbit);
	return insertHeapRecordAndHash(session, &heapRec, version);
}

/** ���ڴ���в����¼����hash����
 * @param session �Ự
 * @param heapRec ��Ҫ����ļ�¼
 * @param version hash�����汾��
 * return �ɹ�����true�����򷵻�false
 */
bool MHeap::insertHeapRecordAndHash(Session *session, MHeapRec *heapRec, RowIdVersion version) {
	bool ret = true;
	u16 recSize = heapRec->getSerializeSize();
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	byte *buf = (byte *)ctx->alloc(recSize);
	Stream s(buf, recSize);
	heapRec->serialize(&s);
	assert(s.getSize() == recSize);
	bool isNew = false;
	MRecordPage *newPage = NULL;
_ReStart:
	u64 sp1 = ctx->setSavepoint();
	if ((newPage = allocRecordPage(session, recSize + sizeof(s16), SEARCH_MAX, false, &isNew)) == NULL) {
		//û�г���latch������µ���freeMem
		session->getTNTDb()->freeMem(__FILE__, __LINE__);
		Thread::yield();
		//�������latchȥalloc Page
		newPage = allocRecordPage(session, recSize + sizeof(s16), SEARCH_MAX, true, &isNew);
		// ���ǿ��������poolUser��page�����ﵽ���ޣ�����ʧ��
		if(newPage == NULL)
			return false;
	}

	u64 ptr = (u64)appendRecordForce(newPage, buf, recSize);
	assert(newPage->m_tableId == (*m_tableDef)->m_id);
	//�п���һ��ʼhash������ָ�����TxnRec
	do {
		bool succ = true;
		assert(m_pageManager->isPageLatched((TNTIMPage *)GET_PAGE_HEADER(ptr), Exclusived));
		m_hashIndexOperPolicy->insertOrUpdate(heapRec->m_rec.m_rowId, ptr, version, HIT_MHEAPREC, &succ);
		if (unlikely(!succ)) {
			newPage->removeRecord((s16 *)ptr);
			if (unlikely(isNew)) {
				addToFreeList(newPage);
			}
			unLatchPage(session, newPage, Exclusived);
			session->getTNTDb()->freeMem(__FILE__, __LINE__);
			Thread::yield();
			ctx->resetToSavepoint(sp1);
			goto _ReStart;
		} else {
			break;
		}
	} while (true);

	if (unlikely(isNew)) {
		addToFreeList(newPage);
	}
	unLatchPage(session, newPage, Exclusived);

	return ret;
}

/** ����rid�����ڴ�Ѽ�¼
 * @param session �Ự
 * @param rid ����rowIdȥ���¼�¼
  * @param ptr ָ���¼slot��ָ��(����ģ���һ����ȷ����Ҫcheck)�����û�н���ֵ����ΪNULL
 * @param heapRec ���º���
 * @param version ������versionֵ
 * return ���³ɹ�����true�����򷵻�false
 */
bool MHeap::updateHeapRecordAndHash(Session *session, RowId rid, void *ptr, MHeapRec *heapRec, RowIdVersion version) {
	/*assert(rid == heapRec->m_rec.m_rowId);
	MRecordPage *page = NULL;
	void *ptr = getSlotAndLatchPage(session, rid, Exclusived, &page);
	if (ptr == NULL) {
		return false;
	}

	bool ret = updateHeapRecordAndHash(session, ptr, heapRec, version);
	unLatchPage(session, page, Exclusived);
	return ret;*/
	bool ret = true;
	HashIndexEntry *entry = NULL;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 recSize = heapRec->getSerializeSize();
	byte *buf = (byte *)ctx->alloc(recSize);
	Stream s(buf, recSize);
	heapRec->serialize(&s);
	SYNCHERE(SP_MHEAP_UPDATEHEAPRECORDANDHASH_REC_MODIFY);
_ReStart:
	u64 sp1 = ctx->setSavepoint();
	bool isNew = false;
	MRecordPage *newPage = NULL;
	while (ptr == NULL || !checkAndLatchPage(session, ptr, rid, Exclusived)) {
		u64 sp2 = ctx->setSavepoint();
		entry = m_hashIndexOperPolicy->get(rid, ctx);
		NTSE_ASSERT(entry != NULL);
		ptr = (void *)entry->m_value;
		ctx->resetToSavepoint(sp2);
	}

	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	if (page->m_freeSize >= recSize - page->getRecordSize((s16*)ptr)) {
		void *newPtr = updateRecordForce(page, ptr, buf, recSize);
		if (newPtr != ptr) {
			assert(m_pageManager->isPageLatched((TNTIMPage *)GET_PAGE_HEADER(newPtr), Exclusived));
			ret = m_hashIndexOperPolicy->update(heapRec->m_rec.m_rowId, (u64)newPtr, version, HIT_MHEAPREC);
			assert(ret == true);
		}
	} else {
		if ((newPage = allocRecordPage(session, recSize + sizeof(s16), SEARCH_MAX, false, &isNew)) == NULL) {
			unLatchPage(session, page, Exclusived);
			session->getTNTDb()->freeMem(__FILE__, __LINE__);
			Thread::yield();
			newPage = allocRecordPage(session, recSize + sizeof(s16), SEARCH_MAX, true, &isNew);
			// ���ǿ��������poolUser��page�����ﵽ���ޣ�����ʧ��
			if(newPage == NULL)
				return false;
			if (unlikely(isNew)) {
				NTSE_ASSERT(newPage != NULL);
				addToFreeList(newPage);
			}
			unLatchPage(session, newPage, Exclusived);
			ctx->resetToSavepoint(sp1);
			goto _ReStart;
		}

		bool dumpOrPurge = false;
		//���status�Ƚ����ж�
		//��latch����������status���������if���ǻ�lock����һ���жϣ���϶���û�����
		//�����ʱif�ж�ʧ�ܣ���Ϊ�������������漰��2��ҳ���latch����ʹ��ʱdump����purge�տ�ʼ��Ҳ�޷�©�����rowid�ļ�¼
		if (m_state == TNTIM_DUMPING || m_state == TNTIM_PURGEING) {
			SYNCHERE(SP_MHEAP_UPDATE_WAIT_DUMP_FINISH);
			m_statusLock.lock(__FILE__, __LINE__);
			if (unlikely(m_state != TNTIM_DUMPING && m_state != TNTIM_PURGEING)) {
				m_statusLock.unlock();
			} else {
				dumpOrPurge = true;
			}
		}
		void *newPtr = appendRecordForce(newPage, buf, recSize);
		assert(newPage->m_tableId == (*m_tableDef)->m_id);
		if (unlikely(dumpOrPurge)) {
			m_compensateRows.push(heapRec->m_rec.m_rowId);
			m_statusLock.unlock();
		}
		assert(m_pageManager->isPageLatched((TNTIMPage *)GET_PAGE_HEADER(newPtr), Exclusived));
		ret = m_hashIndexOperPolicy->update(heapRec->m_rec.m_rowId, (u64)newPtr, version, HIT_MHEAPREC);
		assert(ret == true);
		page->removeRecord((s16 *)ptr);
	}
	unLatchPage(session, page, Exclusived);

	if (unlikely(isNew)) {
		NTSE_ASSERT(newPage != NULL);
		addToFreeList(newPage);
	}

	if (newPage != NULL) {
		unLatchPage(session, newPage, Exclusived);
	}

	return ret;
}

/** ����ptrָ����ڴ�Ѽ�¼
 * @pre ptr��ָ���ҳ���Ѿ���latch��
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param heapRec ���º���
 * @param version ������versionֵ
 * return ���³ɹ�����true�����򷵻�false
 */
/*bool MHeap::updateHeapRecordAndHash(Session *session, void *ptr, MHeapRec *heapRec, RowIdVersion version) {
	bool ret = true;
	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	assert(m_pageManager->isPageLatched(page, Exclusived));
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	u16 recSize = heapRec->getSerializeSize();
	byte *buf = (byte *)ctx->alloc(recSize);
	Stream s(buf, recSize);
	heapRec->serialize(&s);

	if (page->m_freeSize >= recSize - page->getRecordSize((s16*)ptr)) {
		void *newPtr = updateRecordForce(page, ptr, buf, recSize);
		if (newPtr != ptr) {
			ret = m_hashIndexOperPolicy->update(heapRec->m_rec.m_rowId, (u64)newPtr, version, HIT_MHEAPREC);
			assert(ret == true);
		}
	} else {
		MRecordPage *page1 = NULL;
		if ((page1 = allocRecordPage(session, recSize + sizeof(s16), SEARCH_MAX, false)) == NULL) {
			session->getTNTDb()->freeMem();
			Thread::yield();
			page1 = allocRecordPage(session, recSize + sizeof(s16), SEARCH_MAX, true);
			assert(page1 != NULL);
		}

		bool dumpping = false;
		//���status�Ƚ����ж�
		if (unlikely(m_status == TNTIM_DUMPING)) {
			SYNCHERE(SP_MHEAP_UPDATE_WAIT_DUMP_FINISH);
			m_statusLock.lock(__FILE__, __LINE__);
			if (unlikely(m_status != TNTIM_DUMPING)) {
				m_statusLock.unlock();
			} else {
				dumpping = true;
			}
		}
		void *newPtr = appendRecordForce(page1, buf, recSize);
		if (unlikely(dumpping)) {
			m_dumpCR.push(heapRec->m_rec.m_rowId);
			m_statusLock.unlock();
		}
		ret = m_hashIndexOperPolicy->update(heapRec->m_rec.m_rowId, (u64)newPtr, version, HIT_MHEAPREC);
		assert(ret == true);
		unLatchPage(session, page1, Exclusived);

		page->removeRecord((s16 *)ptr);
	}

	return ret;
}*/

/** ɾ��ָ��rowIdֵ�ļ�¼
 * @param session �Ự
 * @param rowId ָ��rowIdֵ
 * return ɾ���ɹ�����true�����򷵻�false
 */
bool MHeap::removeHeapRecordAndHash(Session *session, RowId rowId) {
	bool ret;
	MRecordPage *page = NULL;
	void *ptr = getSlotAndLatchPage(session, rowId, Exclusived, &page);
	if (ptr == NULL) {
		return false;
	}
	ret = removeHeapRecordAndHash(ptr, rowId);
	unLatchPage(session, page, Exclusived);
	return ret;
}

/** ɾ��ptrָ��ļ�¼
 * @pre ptr��ָ���ҳ���Ѿ���latch
 * @param session �Ự
 * @param ptr ָ���¼slot��ָ��
 * @param rowId ��¼RowId
 * return ɾ���ɹ�����true�����򷵻�false
 */
bool MHeap::removeHeapRecordAndHash(void *ptr, RowId rowId) {
	bool ret;
	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	assert(m_pageManager->isPageLatched(page, Exclusived));
	page->removeRecord((s16 *)ptr);
	ret = m_hashIndexOperPolicy->remove(rowId);
	assert(ret == true);
	return true;
}

/** ��ptr��Ӧ��¼��rowid��ΪnewRid
 * @param session �Ự
 * @param ptr ��¼��¼��ָ��
 * @param oldRid ԭ�е�rowid
 * @param newRid �µ�rowId
 */
bool MHeap::remapHeapRecord(Session *session, void *ptr, RowId oldRid, RowId newRid) {
	byte *buf = NULL;
	u16 size = 0;
	MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
	if (!latchPageIfType(session, page, Exclusived, PAGE_MEM_HEAP, -1, __FILE__, __LINE__)) {
		return false;
	}
	if (page->m_tableId != (*m_tableDef)->m_id || !page->getRecord((s16 *)ptr, &buf, &size)) {
		unLatchPage(session, page, Exclusived);
		return false;
	}
	Stream s(buf, size);
	MHeapRec::writeRowId(&s, oldRid, newRid);
	unLatchPage(session, page, Exclusived);
	return true;
}

/** ����(�ع�)�ڴ�Ѽ�¼������������
 * @param session �Ự
 * @param rowId �ع���¼id
 * @param newPtr ������������ĵ�ַ
 * @param version hash����version��versionΪ0��ʾ������version
 * �ɹ�����true�����򷵻�false
 */
bool MHeap::updateHeapRec2TxnIDHash(Session *session, RowId rowId, TrxId trxId, RowIdVersion version /**=0*/) {
	MRecordPage *page = NULL;
	void *ptr = getSlotAndLatchPage(session, rowId, Exclusived, &page);
	assert(ptr != NULL);
	page->removeRecord((s16 *)ptr);
	m_hashIndexOperPolicy->update(rowId, trxId, version, HIT_TXNID);
	unLatchPage(session, page, Exclusived);
	return true;
}

MHeapStat MHeap::getMHeapStat() {
	m_stat.m_freeNode0Size = m_freeList[0]->m_size;
	m_stat.m_freeNode1Size = m_freeList[1]->m_size;
	m_stat.m_freeNode2Size = m_freeList[2]->m_size;
	m_stat.m_freeNode3Size = m_freeList[3]->m_size;
	m_stat.m_total = m_stat.m_freeNode0Size + m_stat.m_freeNode1Size
		+ m_stat.m_freeNode2Size + m_stat.m_freeNode3Size;

	if (m_totalAllocCnt == 0) {
		NTSE_ASSERT(m_totalSearchCnt == 0);
		m_stat.m_avgSearchSize = 0;
	} else {
		m_stat.m_avgSearchSize = (u32)(m_totalSearchCnt/m_totalAllocCnt);
	}
	return m_stat;
}

void MHeap::printFreeList(int index) {
	if (index >= m_gradeNum) {
		return;
	}

	printf("print grade %d:\n", index);
	FreeNode *free = m_freeList[index];
	RWLockGuard guard(&free->m_lock, Shared, __FILE__, __LINE__);
	MRecordPage *page = free->m_node;
	while (page != NULL) {
		printf("page("I64FORMAT"x) tableId = %d, pageType = %d, recordCnt = %d, freeSize = %d, lastSlotOffset= %d, lastRecordOffset = %d\n", 
			(u64)page, page->m_tableId, page->m_pageType, page->m_recordCnt, page->m_freeSize, page->m_lastSlotOffset, page->m_lastRecordOffset);
		page = page->m_next;
	}

	page = free->getLastFind();
	if (page == NULL) {
		return;
	}
	printf("LastFind("I64FORMAT"x) tableId = %d, pageType = %d, recordCnt = %d, freeSize = %d, lastSlotOffset= %d, lastRecordOffset = %d\n", 
		(u64)page, page->m_tableId, page->m_pageType, page->m_recordCnt, page->m_freeSize, page->m_lastSlotOffset, page->m_lastRecordOffset);
}
}