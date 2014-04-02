/**
 * MMS页
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#ifndef _NTSE_MMS_PAGE_H_
#define _NTSE_MMS_PAGE_H_

#include "util/DList.h"
#include "mms/Mms.h"

namespace ntse {

class MmsRPClass;

/** 记录缓存页头 */
struct MmsRecPage {
	Atomic<int>			m_numPins;				/** 被Pin的次数 */
	MmsRPClass			*m_rpClass;				/** 所属的记录缓存页级别 */			
	DLink<MmsRecPage *>	m_freeLink;				/** 自由页双向链表 */	
	u8					m_lruHead;				/** 页内LRU头 */				
	u8					m_lruTail;				/** 页内LRU尾 */	
	u8					m_numFreeSlots;			/** 空闲记录槽个数 */ 
	u8					m_freeList;				/** 空闲记录槽单项链表头 */
	u8					m_numDirtyRecs;			/** 脏记录项个数 */
	u16					m_slotSize;				/** 记录槽大小 */				
	u32					m_oldestHeapIdx;		/** 最老缓存页堆索引号 */
	u16					m_version;				/** 版本号。内部约定：记录项rid，则版本也更新 */
};

/** 记录项头，内部约定：页内LRU链表指针放前两项 */
#pragma pack(2)
struct MmsRecord {
	u8					m_lruNext;				/** 页内LRU链表后项指针 */
	u8					m_lruPrev;				/** 页内LRU链表前项指针 */
	u8					m_rid[RID_BYTES];		/** 记录RID */
	u16					m_size;					/** 记录长度 */
	u32					m_timestamp;			/** 更新时间戳 */
	u32					m_updateBitmap;			/** 更新位图 */
	u32					m_dirtyBitmap;			/** 脏位图 */
	u8					m_dirty: 1;				/** 脏记录标志 */
	u8					m_valid: 1;				/** 是否在使用中 */
	u8                  m_compressed: 1;        /** 是否是压缩的格式 */
	u8					m_padding: 5;			/** 预留字段 */
};
#pragma pack()

/** 包含记录RID和记录地址的二元组
*   在checkpoint刷新时使用 
*/
struct MmsRecPair {
public:
	u8					m_rid[RID_BYTES];		/** 记录RID */
	MmsRecord			*m_mmsRecord;			/** 记录项指针 */

	MmsRecPair(MmsRecord *mmsRecord) {
		for (int i = 0; i < RID_BYTES; i++)
			m_rid[i] = mmsRecord->m_rid[i];
		m_mmsRecord = mmsRecord;
	}

	bool operator < (const MmsRecPair &another) const {
		return RID_READ((byte *)m_rid) < RID_READ((byte *)(another.m_rid));
	}
};

// MMS页操作类
class MmsPageOper{
public:
	static bool clearRecordInPage(MmsRecPage *recPage, MmsRecord *mmsRecord);
	static MmsRecord* getFreeRecSlot(MmsRecPage *recPage);
	static void getRecPageTs(MmsRecPage *recPage, u32 *tsMin, u32 *tsMax, u8 *numRecs);
	static void getDirtyRecords(Session *session, Mms *mms, MmsRPClass *rpClass, MmsRecPage *recPage, std::vector<MmsRecPair> *tmpRecArray);
	static void initRecPage(MmsRPClass *rpClass, MmsRecPage *recPage);
	static void fillRecSlotEx(MmsRecPage *recPage, MmsRecord *mmsRecord, const Record *record);
	static bool touchRecord(MmsRecPage *recPage, MmsRecord *mmsRecord);
	//static float computeFPage(const u32 tsCurr, const u32 tsMin, const u32 tsMax, const u8 num);
	static float computeFPage(const u32 tsCurr, const u32 tsMin, const u32 tsMax);
	static bool isMinFPage(MmsRecPage *recPage, u32 *tsMin, u32 *tsMax, u8 *numRecs, bool assigned, MmsTable *victimTable, float pgRatio = -1, bool ext = false);

	/**
	* 对缓存记录页解锁
	*
	* @param sesson 会话
	* @param mms 全局MMS对象
	* @param recPage 缓存记录页指针
	*/
	static inline void unlockRecPage(Session *session, Mms *mms, MmsRecPage *recPage) {
		mms->m_pagePool->unlockPage(session->getId(), recPage, Exclusived);
	}

	/** 
	 * 对缓存记录页解锁
	 *
	 * @param session 会话
	 * @param mms 全局MMS对象
	 * @param recPage 记录页
	 * @param mode 加锁模式
	 */
	static inline void unlockRecPage(Session *session, Mms *mms, MmsRecPage *recPage, LockMode mode) {
		mms->m_pagePool->unlockPage(session->getId(), recPage, mode);
	}

	/** 
	 * 尝试加缓存记录页锁
	 *
	 * @param session 会话
	 * @param mms 全局MMS对象
	 * @param recPage 缓存记录页指针
	 * @param file 发出加锁操作的代码文件名
	 * @param line 发出加锁操作的代码行号
	 * @return 加锁是否成功
	 */
	static inline bool trylockRecPage(Session *session, Mms *mms, MmsRecPage *recPage, const char *file, uint line) {
		return mms->m_pagePool->trylockPage(session->getId(), recPage, Exclusived, file, line);
	}

	/** 
	 * 如果为记录页则加锁
	 * @pre 未加页锁
	 *
	 * @param session 会话
	 * @param mms 全局MMS对象
	 * @param page 页指针
	 * @param pageType 页类型
	 * @param file 发出加锁操作的代码文件名
	 * @param line 发出加锁操作的代码行号
	 * @return 是否已加锁
	 */
	static inline bool lockIfPageType(Session *session, Mms *mms, void *page, ntse::PageType pageType, const char *file, uint line) {
		return mms->m_pagePool->lockPageIfType(session->getId(), page, Exclusived, pageType, -1, file, line);
	}

	/** 
	 * 加读写锁
	 *
	 * @param userId 用户ID，0表示未知用户
	 * @param lock 读写锁
	 * @param lockMode 锁模式
	 * @param file 发出加锁操作的代码文件名
	 * @param line 发出加锁操作的代码行号
	 */
	static inline void mmsRWLock(u16 userId, LURWLock *lock, LockMode lockMode, const char *file, uint line) {
		lock->lock(userId, lockMode, file, line);
	}

	/** 
	 * 释放读写锁
	 *
	 * @param userId 用户ID，0表示未知用户
	 * @param lock 读写锁
	 * @param lockMode 锁模式
	 */
	static inline void mmsRWUNLock(u16 userId, LURWLock *lock, LockMode lockMode) {
		lock->unlock(userId, lockMode);
	}

	/** 
	 * 尝试加读写锁
	 *
	 * @param userId 用户ID，0表示未知用户
	 * @param lock 读写锁
	 * @param lockMode 锁模式
	 * @param file 发出加锁操作的代码文件名
	 * @param line 发出加锁操作的代码行号
	 */
	static inline bool mmsRWTryLock(u16 userId, LURWLock *lock, LockMode lockMode, const char *file, uint line) {
		return lock->tryLock(userId, lockMode, file, line);
	}

	/** 
	 * 加读写锁(带超时)
	 *
	 * @param userId 用户ID，0表示未知用户
	 * @param lock 读写锁
	 * @param lockMode 锁模式
	 * @param timeoutMms 超时
	 * @param file 发出加锁操作的代码文件名
	 * @param line 发出加锁操作的代码行号
	 */
	static inline bool mmsRWTimedLock(u16 userId, LURWLock *lock, LockMode lockMode, int timeoutMs, const char *file, uint line) {
		return lock->timedLock(userId, lockMode, timeoutMs, file, line);
	}

	/** 
	 * 设置脏记录标志
	 *
	 * @param recPage 记录页
	 * @param mmsRecord 记录项
	 * @param isDirty 是否标记为脏记录
	 * @param dirtyBitmap 脏位图, 默认为0
	 */
	static inline void setDirty(MmsRecPage *recPage, MmsRecord *mmsRecord, bool isDirty, u32 dirtyBitmap = 0) {
		if (isDirty) {
			if (!mmsRecord->m_dirty) {
				recPage->m_numDirtyRecs++;
				recPage->m_rpClass->m_mmsTable->m_numDirtyRecords.increment();
			}
			mmsRecord->m_dirty = 1;
			mmsRecord->m_dirtyBitmap = dirtyBitmap;
		} else {
			if (mmsRecord->m_dirty) {
				recPage->m_numDirtyRecs--;
				recPage->m_rpClass->m_mmsTable->m_numDirtyRecords.decrement();
			}
			mmsRecord->m_dirty = 0;
			// 清空更新位图和脏记录位图
			mmsRecord->m_dirtyBitmap = 0;
			mmsRecord->m_updateBitmap = 0;
		}
	}

	/**
	 * 偏移转换成地址
	 *
	 * @param recPage 缓存页地址
	 * @param offset 页内偏移，必定大于0
	 * @return 偏移对应的地址
	 */
	static inline byte* offset2pointer(MmsRecPage *recPage, u8 offset) {
		if (offset)
			return (byte*)recPage + MMS_REC_PAGE_HEAD_SIZE + (offset - 1) * recPage->m_slotSize;
		return &recPage->m_lruHead;
	}

	/**
	 * 地址转换成偏移
	 *
	 * @param recPage 缓存页地址
	 * @param addr 页内地址
	 * @return 地址对应的偏移
	 */
	static inline u8 pointer2offset(MmsRecPage *recPage, byte *addr) {
		assert (addr >= (byte *)recPage + MMS_REC_PAGE_HEAD_SIZE);
		
		return (u8)((addr - (byte *)recPage - MMS_REC_PAGE_HEAD_SIZE) / recPage->m_slotSize + 1);
	}

	/** 
	 * LRU链表是否为空
	 *
	 * @param recPage 记录页
	 * @return 是否为空
	 */
	static inline bool isEmptyLru(MmsRecPage *recPage) {
		return !recPage->m_lruHead; /** m_lruHead=0表示LRU为空 */
	}

	/**
	 * 获取最老记录项
	 *
	 * @param recPage 记录页
	 * @return 记录项
	 */
	static inline MmsRecord* getLruRecord(MmsRecPage *recPage) {
		return (MmsRecord *)offset2pointer(recPage, recPage->m_lruHead);
	}

	/**
	 * 获取缓存记录所在的记录页
	 *
	 * @param pagePool 页池 
	 * @param mmsRecord 缓存记录头指针
	 * @return 缓存记录页
	 */
	static inline MmsRecPage* getRecPage(PagePool *pagePool, MmsRecord *mmsRecord) {
		return (MmsRecPage *)pagePool->alignPage(mmsRecord);
	}

	/** 
	 * 添加到页内LRU链表末尾
	 * 
	 * @param recPage 记录页
	 * @param mmsRecord 记录项
	 */
	static inline void addToLruList(MmsRecPage *recPage, MmsRecord *mmsRecord) {
		u8 *tmpByte;

		// 添加到页内LRU链表末尾
		if (recPage->m_lruHead) {
			mmsRecord->m_lruPrev = recPage->m_lruTail;
			mmsRecord->m_lruNext = 0;
			tmpByte = offset2pointer(recPage, recPage->m_lruTail);
			((MmsRecord *)tmpByte)->m_lruNext = pointer2offset(recPage, &mmsRecord->m_lruNext);
			recPage->m_lruTail = pointer2offset(recPage, &mmsRecord->m_lruPrev);
		} else {
			recPage->m_lruTail = pointer2offset(recPage, &mmsRecord->m_lruPrev);
			recPage->m_lruHead = pointer2offset(recPage, &mmsRecord->m_lruNext);
			mmsRecord->m_lruPrev = mmsRecord->m_lruNext = 0;
		}
	}

	/** 
	 * 从页内LRU删除记录
	 *
	 * @param recPage 记录页
	 * @param mmsRecord 记录项
	 */
	static inline void delFromLruList(MmsRecPage *recPage, MmsRecord *mmsRecord) {
		((MmsRecord *)offset2pointer(recPage, mmsRecord->m_lruNext))->m_lruPrev = mmsRecord->m_lruPrev;
		((MmsRecord *)offset2pointer(recPage, mmsRecord->m_lruPrev))->m_lruNext = mmsRecord->m_lruNext;
	}

	/** 
	 * 设置位图信息
	 * 
	 * @param mmsRecord 记录项
	 * @param dirtyBitmap 脏位图信息
	 * @param updateBitmap 更新位图信息
	 */
	static inline void setUpdateInfo(MmsRecord *mmsRecord, u32 dirtyBitmap, u32 updateBitmap = 0) {
		mmsRecord->m_dirtyBitmap |= dirtyBitmap;
		mmsRecord->m_updateBitmap |= updateBitmap;
	}
};

#define MMS_LOCK_REC_PAGE(session, mms, recPage) (mms)->m_pagePool->lockPage((session)->getId(), (recPage), Exclusived, __FILE__, __LINE__)
#define MMS_LOCK_REC_PAGE_EX(session, mms, recPage, mode) (mms)->m_pagePool->lockPage((session)->getId(), (recPage), mode, __FILE__, __LINE__)
#define MMS_UNPIN_REC_PAGE(session, mms, recPage) MmsPageOper::unpinRecPage((session), (mms), (recPage), __FILE__, __LINE__)
#define MMS_TRYLOCK_REC_PAGE(session, mms, recPage) MmsPageOper::trylockRecPage((session), (mms), (recPage), __FILE__, __LINE__)
#define MMS_LOCK_IF_PAGE_TYPE(session, mms, page, pageType) MmsPageOper::lockIfPageType((session), (mms), (page), (pageType), __FILE__, __LINE__)
#define MMS_RWLOCK(userId, lock, lockMode) MmsPageOper::mmsRWLock((userId), (lock), (lockMode), __FILE__, __LINE__)
#define MMS_TRYRWLOCK(userId, lock, lockMode) MmsPageOper::mmsRWTryLock((userId), (lock), (lockMode), __FILE__, __LINE__)
#define MMS_RWTIMEDLOCK(userId, lock, lockMode, timeoutMs) MmsPageOper::mmsRWTimedLock((userId), (lock), (lockMode), (timeoutMs), __FILE__, __LINE__)

}

#endif
