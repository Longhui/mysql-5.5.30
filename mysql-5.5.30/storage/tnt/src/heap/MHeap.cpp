//注意点:目前系统设计dump，purge，页面defrag不能同时进行
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

/** 将记录页添加到空闲链表的首部
 * @param page 需要添加的记录页
 */
void MHeap::addToFreeList(MRecordPage* page) {
	assert(page->m_pageType == MPT_MHEAP);
	u8 grade = page->getFreeGrade(page->m_freeSize);
	FreeNode *free = m_freeList[grade];
	free->m_lock.lock(Exclusived, __FILE__, __LINE__);
	addToFreeListSafe(free, page);
	free->m_lock.unlock(Exclusived);
}

/** 将记录页添加到指定空闲链表的首部
 * @param free 需要添加的空闲链表
 * @param page 需要添加的记录页
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

/** 将一个页从相应的空闲链表中移除
 * @param free 指定的空闲链表
 * @param page 需要移除的记录页
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

/** 申请空闲空间大于指定大小的页面，如果不存在空闲空间大于指定大小的页面，那么分配新的页面
 * @param session 会话
 * @param size 需要分配的记录大小
 * @param max 在每个空闲链表中最多查找max次
 * @param force 是否需要强制分配
 * @param isNew [out] 标识新页面是否新分配的，如果是新分配的页面，需要后续将其添加到freeList中
 * return 返回查找到的页面
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
		//在增长更新的情况下是持有latch加lock，现在free lock和latch的顺序是先lock在latch，
		//所以在增长更新的情况下持有原纪录page的latch，需要tryLock
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
			//在持有一个页面的情况下需要latch另外一个页面，内存堆不定义先后顺序，
			//而且也只有在做增长更新的情况下需要在持有latch一个页面的情况下去latch另一个页面
			//此时这里采用的原则是对第二个页面采用tryLatch是为了避免死锁
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
			// 如果页面借用达到上限那么直接返回失败
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
		//添加到相应的列表中
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

/** 将某个页面从相应链表中移除，并归还给页分配管理器
 * @param session 会话
 * @param free 记录页所在的链表
 * @param page 需要移除的记录页
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

/** 内存堆尽力释放指定大小的页给页管理器
 * @param session 会话
 * @param target 需要释放target个页面给页管理器
 * return 最终释放的页面数
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
			//P2还存在有效记录&& P1有足够的空间容纳P2的最后一条记录
			while (p2->getLastRecord(&p2Slot, &rec, &size) && p1->m_freeSize >= size + sizeof(s16)) {
				//移动P2的最后一条记录至P1，如果p1页面中间含invalid slot，但freeSize足够，那么appendRecordForce会触发整理页面
				p1Slot = (s16 *)appendRecordForce(p1, rec, size);

				//删除P2的页对应记录
				p2->removeRecord(p2Slot);

				//根据rowId修改hash索引的ptr
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
		//emptyPageList中的页面全被latch住了
		for (u32 i = 0; i < emptyListSize; i++) {
			if (likely(emptyPageList[i]->m_recordCnt == 0)) {
				removeFromFreeListSafe(free, emptyPageList[i]);
				m_pageManager->releasePage(session->getId(), emptyPageList[i]);
			} else {
				assert(false);
			}
		}
		free->m_lock.unlock(Exclusived);

		//只有释放页面的数目达到target时，或者已经遍历完链表的时候才结束
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

/** 释放所有页面
 * @param session 会话
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

/** 开始scan阶段。scan时，需要避免defrag，因为defrag会破坏链表的结构
 * @param session 会话
 * return scan上下文
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
		//如果该等级不存在页面
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

/** scan内存堆获取下一条的记录，目前只适用于快照读或者无写事务的当前读，实现中是把page快照拷贝到ctx page上
 * @param ctx scan上下文会话
 * @param readView scan记录参照的readView
 * return 如果存在下一个记录返回true，否则返回false
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

	//purge，dump，defrag能同时进行。尤其是purge和defrag不能同时进行，这样就保证了page在purge阶段不会被移动
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
			//如果当前版本不可见，那么对该记录不进行purge
			if(visible != NULL) {
				*visible = !(readView != NULL && !readView->isVisible(ctx->m_heapRec->m_txnId));
			}
			rec = new (memCtx->alloc(sizeof(Record))) Record(INVALID_ROW_ID, REC_REDUNDANT, (byte *)memCtx->alloc((*m_tableDef)->m_maxRecSize), (*m_tableDef)->m_maxRecSize);
			RecordOper::convertRecordVFR(*m_tableDef, &ctx->m_heapRec->m_rec, rec);
			memcpy(&ctx->m_heapRec->m_rec, rec, sizeof(Record));

			find = true;
			break;
		}
		

		//在上一个页面中没有找到purge的记录，那继续在下一页找
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

			//如果下一页为空，则在下一个空闲链表中找
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

/** 结束scan
 * @param ctx scan上下文会话
 */
void MHeap::endScan(MHeapScanContext *ctx) {
	ctx->m_session->getMemoryContext()->resetToSavepoint(ctx->m_sp);
}

/** purge的第二阶段
 * @param session 会话
 * @param readView 可见性判断
 * return purge的记录数
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
				//pickRowLock主要用于该记录scan后被加锁，但heapRec还未修改，
				//那么在update或者delete前如果purge摘除将发生错误
				if (!readView->isVisible(heapRec->m_txnId) ||
					(heapRec->m_del != FLAG_MHEAPREC_DEL && //NTSETNT-188
					session->getTrans()->pickRowLock(heapRec->m_rec.m_rowId, (*m_tableDef)->m_id, true))) {
					ctx->resetToSavepoint(sp);
					slot++;
					continue;
				}

				SYNCHERE(SP_MHEAP_PURGE_PHASE2_PICKROW_AFTER);
				//先删除哈希索引项，再删除数据
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

/** 将dump文件扩充incrSize个字节
 * @param file 需要扩充的dump文件句柄
 * @param incrSize 需要扩充的字节数
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

/** 向dump文件写入数据
 * @param file dump文件句柄
 * @param offset dump文件开始写的偏移量
 * @param buf 需要写入的内容
 * @param size 需要写入的字节数
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

/** 写dump文件的结束页
 * @param session 会话
 * @param file dump文件句柄
 * @param offset [in, out] in 写文件的偏移量，out 下次写文件时的偏移量
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

/** 开始执行dump
 * @param file dump文件
 * @param offset [in,out] in 写文件的偏移量，out 下次写文件时的偏移量
 * @param ctx 上下文分配内存
 * return 成功返回true，否则返回false
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
 * 开始执行purge
 */
bool MHeap::beginPurge() {
	m_statusLock.lock(__FILE__, __LINE__);
	assert(m_state == TNTIM_NOOP && m_compensateRows.getSize() == 0);
	m_state = TNTIM_PURGEING;
	m_statusLock.unlock();

	return true;
}

//结束dump补偿
void MHeap::finishDumpCompensate() {
	m_compensateRows.clear();
}

/** 结束purge补偿 */
void MHeap::finishPurgeCompensate() {
	m_compensateRows.clear();
}

void MHeap::resetStatus() {
	//因为所有的页面已经被purge，增长更新没有必要再记录补偿rowId集，
	m_statusLock.lock(__FILE__, __LINE__);
	assert(m_state == TNTIM_DUMPING || m_state == TNTIM_PURGEING);
	m_state = TNTIM_NOOP;
	m_statusLock.unlock();
}

/** 将可见内存堆记录dump指定的文件中去
 * @param session 会话
 * @param readView 可见性判断
 * @param file 存放dump记录的数据文件
 * @param offset [in,out] in 写文件的偏移量 out下一次写入文件的偏移量
 * @param versionPoolPolicy 版本池策略
 * return 成功返回true，否则返回false
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

				//每次dump一个页面开始时，bufPage肯定为空，
				//这样导致了这次writeDumpPageRecord肯定不会写硬盘
				try {
					writeDumpPageRecord(bufPage, buf, bufSize, file, offset);
				} catch (NtseException &e) {
					unLatchPage(session, page, Shared);
					m_pageManager->releasePage(session->getId(), bufPage);
					throw e;
				}
			}
			unLatchPage(session, page, Shared);

			//dump不可见记录的可见版本
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

			//将bufPage中未写入部分写入文件，
			//如果不写入剩余部分，那么可能导致下一个page在第一次writeDumpPageRecord时写硬盘
			if (bufPage->m_recordCnt > 0) {
				assert(bufPage->m_pageType == MPT_DUMP);
				writeDumpFile(file, *offset, (byte *)bufPage, MRecordPage::TNTIM_PAGE_SIZE);
				*offset += MRecordPage::TNTIM_PAGE_SIZE;
				memset(bufPage, 0, MRecordPage::TNTIM_PAGE_SIZE);
				bufPage->init(MPT_DUMP, (*m_tableDef)->m_id);
			}

			dumpPageCnt++;
			SYNCHERE(SP_MHEAP_DUMPREALWORK_REC_MODIFY);
			//TODO 可以考虑不对freeNode加S锁
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

/** 将补偿rowid对应的记录dump到指定文件中去
 *  在dump页中，存在增长更新导致记录丢失现象
 * @param session 会话
 * @param readView 可见性判断
 * @param file 存放dump数据的文件
 * @param offset [in,out] in 写文件的偏移量 out 下次写dump文件的偏移量
 * @param versionPoolPolicy 版本池策略
 * return 成功返回true，失败返回false
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
		//如果此时该rid对应的记录发生增长更新，那么返回的rec为NULL，需要重新定位
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
		//因为补偿更新是在dump时活跃事务做增长更新引起的，所以该记录在内存中的版本对dump必然不可见
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

/** 根据readView获取版本池中可见的冗余堆记录项，其中record记录为REC_REDUNT格式
 * @param session 会话
 * @param heapRec 内存中堆记录
 * @param readView 判断版本的可见性
 * @param versionPool 版本池策略
 * return 可见的内存堆记录，其中record记录为REC_REDUNT格式
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

/** 根据readView获取版本池中可见的堆记录项
 * @param session 会话
 * @param heapRec 内存中堆记录
 * @param readView 判断版本的可见性
 * @param versionPool 版本池策略
 * return 可见的内存堆记录
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

/** 将内存记录写入page，如果page页满则将page写入dump文件
 * @param page 页面
 * @param buf 记录序列化字节
 * @param size 记录序列化字节数
 * @param file dump文件
 * @param offset [in,out] in 写文件的偏移量，out 下次写文件时的偏移量
 * return 成功返回true，否则返回false
 */
bool MHeap::writeDumpPageRecord(MRecordPage *page, byte *buf, u16 size, File *file, u64* offset) throw(NtseException) {
	s16 *slot = page->appendRecord(buf, size);
	//如果page已满
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

/**dump恢复，因为dump恢复是在单线程的环境中执行，所以不需要考虑线程安全
 * @param session 会话
 * @param file dump文件
 * @param offset [in,out] in 读取文件的偏移量 out 读取结束时的偏移量
 * return 成功返回true，否则返回false
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

		//补偿rowid
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

/** 对指定的内存堆记录页上的记录进行建索引操作
 * @param session 会话
 * @param page 指定内存堆记录页
 * @param version 索引版本号
 * @param compensate 是否为补偿页面
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
				//对于补偿页面，因为是根据readView来进行dump，所以如果该记录已经被dump，
				//那么根据readView获取到的可见记录是一致的,所以没有必要进行update
				updateHeapRecordAndHash(session, entry->m_rowId, rec, version);
			}*/
		} else {
			void *srcPtr = (void *)m_hashIndexOperPolicy->insertOrUpdate(rec->m_rec.m_rowId, (u64)slot, version, HIT_MHEAPREC);
			if (srcPtr != NULL) {
				//因为这个函数只在dump恢复时采用，而恢复目前是单线程操作，所以暂时省去对页面的latch
				MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(srcPtr);
				page->removeRecord((s16 *)srcPtr);
			}
		}
	}
}

/** 空闲链表整理，在页面不停被分配空间后，页面空闲等级发生变更，
 *  这时我们采用的不是立即调整空闲页面到相应的等级链表上去，
 *  而是通过定义整理使其归位到相应的空闲等级链表上去
 * @param session 会话
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
			//如果该页没有任何记录了，直接释放给manager
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

/** 根据ptr获取其指向的内存堆记录
 * @pre ptr所指向的页面没有被latch
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param rid 检查内存堆记录的RowId值是否等于rid，如果不等于，说明slot被重用，需要重定位，返回NULL
 * return 返回ptr指向内存堆记录，且内存堆记录的RowId等于rid，否则返回NULL
 */
MHeapRec* MHeap::getHeapRecord(Session *session, void *ptr, RowId rid) {
	MRecordPage *page = NULL;
	MHeapRec *heapRec = getHeapRecordAndLatchPage(session, ptr, rid, Shared, true, &page);
	if (heapRec != NULL) {
		unLatchPage(session, page, Shared);
	}
	
	return heapRec;
}

/** 根据ptr获取记录，且该记录的RowId值必须等于rid
 * @pre ptr所指向的页面已经被latch住
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param rid 检查内存堆记录的RowId值是否等于rid
 * return 返回ptr指向内存堆记录，且内存堆记录的RowId必须等于rid
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

/** 根据ptr获取RowId值为rid的记录
 * @pre ptr所指向的页面没有被latch
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param rid 检查内存堆记录的RowId值是否等于rid，如果不等于，说明slot被重用，需要重定位，返回NULL
 * @param rec out 如果存在相应记录，返回rec_redunt格式的记录
 * return 返回ptr指向内存堆记录，且内存堆记录的RowId等于rid，否则返回NULL
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

/** 根据ptr获取记录，且该记录的RowId值必须等于rid
 * @pre ptr所指向的页面已经被latch住
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param rid 检查内存堆记录的RowId值是否等于rid
 * @param rec out 如果存在相应记录，返回rec_redundant格式的记录
 * return 返回ptr指向内存堆记录，且内存堆记录的RowId必须等于rid
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

/** 根据ptr和rowId获取相应的内存堆记录，同时返回记录所在的页面
 * @post 如果存在相应的记录，同时返回记录所在的页面，并且该页面被latch
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param rid 检查内存堆记录的RowId值是否等于rid，如果不等于，说明slot被重用，需要重定位，返回NULL
 * @param copyData record中data另外分配空间还是指向页面空间
 * @param page out 如果存在相应记录，返回该记录所在的页面
 * return 返回ptr指向内存堆记录，且内存堆记录的RowId等于rid，否则返回NULL
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

/** 根据rowId获取相应的slot指针和所在的page页面
 * @post 如果有页面返回，那么该页面已经被latch
 * @param session 会话
 * @param rid 根据rowId获取相应的ptr值和对应的页面
 * @param page out 返回相对应的页面
 * return 如果能找到rid对应的记录，返回ptr值和记录所在的页面，否则返回NULL
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

/** 检查ptr所指向的记录rowId是否等于rid，如果是的话，latch住该页面，并返回true，否则返回false
 * @post 如果ptr所指向的记录的rowId等于rid，那么记录所在的页面将被latch
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param rid ptr所指向记录的期望rowId
 * return 如果ptr所指向的记录rowId等于rid，返回true，否则返回false
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

/** 将recSize大小的记录插入page页，而且一定保证成功
 * @param page 需要插入记录的page
 * @param buf  记录序列化
 * @param recSize 记录序列化后大小
 */
void *MHeap::appendRecordForce(MRecordPage *page, byte *buf, u16 recSize) {
	assert(m_pageManager->isPageLatched(page, Exclusived));
	assert(page->m_freeSize >= recSize + sizeof(s16));
	void *ptr = (void *)page->appendRecord(buf, recSize);
	if (!ptr) {//此时插入不成功，需要页面内整理
		page->defrag(m_hashIndexOperPolicy);
		ptr = (void *)page->appendRecord(buf, recSize);
		assert(ptr != NULL);
	}
	return ptr;
}

/** 将ptr指向的记录在本页面更新为recSize大小的新记录，而且一定保证成功
 * @param page 需要插入记录的page
 * @param ptr  需要更新记录的ptr
 * @param buf  更新后像记录的序列化
 * @param recSize 更新后像记录序列化的大小
 */
void *MHeap::updateRecordForce(MRecordPage *page, void *ptr, byte *buf, u16 recSize) {
	assert(m_pageManager->isPageLatched(page, Exclusived));
	assert(page->m_freeSize >= recSize - page->getRecordSize((s16*)ptr));
	void *newPtr = page->updateRecord((s16 *)ptr, buf, recSize);
	if (!newPtr) {//说明此时remove成功，但appendRecord失败
		page->defrag(m_hashIndexOperPolicy);
		newPtr = page->appendRecord(buf, recSize);
		assert(newPtr != NULL);
	}
	return newPtr;
}

/** 向内存堆中插入记录及其hash索引
 * @param session 会话
 * @param txnId 插入操作的事务id
 * @param rollBackId 回滚记录id
 * @param vTableIndex 回滚记录所在版本池表中的序号
 * @param rec 当前最新记录
 * @param delbit 删除标记位
 * @param version hash索引的version
 * return 成功返回true，否则返回false
 */
bool MHeap::insertHeapRecordAndHash(Session *session, TrxId txnId, RowId rollBackId, u8 vTableIndex, Record *rec, u8 delbit, RowIdVersion version) {
	MHeapRec heapRec(txnId, rollBackId, vTableIndex, rec, delbit);
	return insertHeapRecordAndHash(session, &heapRec, version);
}

/** 向内存堆中插入记录及其hash索引
 * @param session 会话
 * @param heapRec 需要插入的记录
 * @param version hash索引版本号
 * return 成功返回true，否则返回false
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
		//没有持有latch的情况下调用freeMem
		session->getTNTDb()->freeMem(__FILE__, __LINE__);
		Thread::yield();
		//不会持有latch去alloc Page
		newPage = allocRecordPage(session, recSize + sizeof(s16), SEARCH_MAX, true, &isNew);
		// 如果强制向其他poolUser借page数量达到上限，返回失败
		if(newPage == NULL)
			return false;
	}

	u64 ptr = (u64)appendRecordForce(newPage, buf, recSize);
	assert(newPage->m_tableId == (*m_tableDef)->m_id);
	//有可能一开始hash索引项指向的是TxnRec
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

/** 根据rid更新内存堆记录
 * @param session 会话
 * @param rid 根据rowId去更新记录
  * @param ptr 指向记录slot的指针(建议的，不一定正确，需要check)，如果没有建议值，则为NULL
 * @param heapRec 更新后像
 * @param version 索引项version值
 * return 更新成功返回true，否则返回false
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
			// 如果强制向其他poolUser借page数量达到上限，返回失败
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
		//脏读status先进行判断
		//在latch的情况下脏读status，如果进入if，那会lock后会进一步判断，这肯定是没问题的
		//如果此时if判断失败，因为持有增长更新涉及的2个页面的latch，即使此时dump或者purge刚开始，也无法漏掉这个rowid的记录
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

/** 更新ptr指向的内存堆记录
 * @pre ptr所指向的页面已经被latch了
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param heapRec 更新后像
 * @param version 索引项version值
 * return 更新成功返回true，否则返回false
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
		//脏读status先进行判断
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

/** 删除指定rowId值的记录
 * @param session 会话
 * @param rowId 指定rowId值
 * return 删除成功返回true，否则返回false
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

/** 删除ptr指向的记录
 * @pre ptr所指向的页面已经被latch
 * @param session 会话
 * @param ptr 指向记录slot的指针
 * @param rowId 记录RowId
 * return 删除成功返回true，否则返回false
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

/** 将ptr对应记录的rowid改为newRid
 * @param session 会话
 * @param ptr 记录记录的指针
 * @param oldRid 原有的rowid
 * @param newRid 新的rowId
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

/** 更新(回滚)内存堆记录到事务数组中
 * @param session 会话
 * @param rowId 回滚记录id
 * @param newPtr 所在事务数组的地址
 * @param version hash索引version。version为0表示不更新version
 * 成功返回true，否则返回false
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