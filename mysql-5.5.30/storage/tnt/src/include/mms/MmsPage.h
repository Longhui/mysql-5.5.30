/**
 * MMSҳ
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */

#ifndef _NTSE_MMS_PAGE_H_
#define _NTSE_MMS_PAGE_H_

#include "util/DList.h"
#include "mms/Mms.h"

namespace ntse {

class MmsRPClass;

/** ��¼����ҳͷ */
struct MmsRecPage {
	Atomic<int>			m_numPins;				/** ��Pin�Ĵ��� */
	MmsRPClass			*m_rpClass;				/** �����ļ�¼����ҳ���� */			
	DLink<MmsRecPage *>	m_freeLink;				/** ����ҳ˫������ */	
	u8					m_lruHead;				/** ҳ��LRUͷ */				
	u8					m_lruTail;				/** ҳ��LRUβ */	
	u8					m_numFreeSlots;			/** ���м�¼�۸��� */ 
	u8					m_freeList;				/** ���м�¼�۵�������ͷ */
	u8					m_numDirtyRecs;			/** ���¼����� */
	u16					m_slotSize;				/** ��¼�۴�С */				
	u32					m_oldestHeapIdx;		/** ���ϻ���ҳ�������� */
	u16					m_version;				/** �汾�š��ڲ�Լ������¼��rid����汾Ҳ���� */
};

/** ��¼��ͷ���ڲ�Լ����ҳ��LRU����ָ���ǰ���� */
#pragma pack(2)
struct MmsRecord {
	u8					m_lruNext;				/** ҳ��LRU�������ָ�� */
	u8					m_lruPrev;				/** ҳ��LRU����ǰ��ָ�� */
	u8					m_rid[RID_BYTES];		/** ��¼RID */
	u16					m_size;					/** ��¼���� */
	u32					m_timestamp;			/** ����ʱ��� */
	u32					m_updateBitmap;			/** ����λͼ */
	u32					m_dirtyBitmap;			/** ��λͼ */
	u8					m_dirty: 1;				/** ���¼��־ */
	u8					m_valid: 1;				/** �Ƿ���ʹ���� */
	u8                  m_compressed: 1;        /** �Ƿ���ѹ���ĸ�ʽ */
	u8					m_padding: 5;			/** Ԥ���ֶ� */
};
#pragma pack()

/** ������¼RID�ͼ�¼��ַ�Ķ�Ԫ��
*   ��checkpointˢ��ʱʹ�� 
*/
struct MmsRecPair {
public:
	u8					m_rid[RID_BYTES];		/** ��¼RID */
	MmsRecord			*m_mmsRecord;			/** ��¼��ָ�� */

	MmsRecPair(MmsRecord *mmsRecord) {
		for (int i = 0; i < RID_BYTES; i++)
			m_rid[i] = mmsRecord->m_rid[i];
		m_mmsRecord = mmsRecord;
	}

	bool operator < (const MmsRecPair &another) const {
		return RID_READ((byte *)m_rid) < RID_READ((byte *)(another.m_rid));
	}
};

// MMSҳ������
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
	* �Ի����¼ҳ����
	*
	* @param sesson �Ự
	* @param mms ȫ��MMS����
	* @param recPage �����¼ҳָ��
	*/
	static inline void unlockRecPage(Session *session, Mms *mms, MmsRecPage *recPage) {
		mms->m_pagePool->unlockPage(session->getId(), recPage, Exclusived);
	}

	/** 
	 * �Ի����¼ҳ����
	 *
	 * @param session �Ự
	 * @param mms ȫ��MMS����
	 * @param recPage ��¼ҳ
	 * @param mode ����ģʽ
	 */
	static inline void unlockRecPage(Session *session, Mms *mms, MmsRecPage *recPage, LockMode mode) {
		mms->m_pagePool->unlockPage(session->getId(), recPage, mode);
	}

	/** 
	 * ���Լӻ����¼ҳ��
	 *
	 * @param session �Ự
	 * @param mms ȫ��MMS����
	 * @param recPage �����¼ҳָ��
	 * @param file �������������Ĵ����ļ���
	 * @param line �������������Ĵ����к�
	 * @return �����Ƿ�ɹ�
	 */
	static inline bool trylockRecPage(Session *session, Mms *mms, MmsRecPage *recPage, const char *file, uint line) {
		return mms->m_pagePool->trylockPage(session->getId(), recPage, Exclusived, file, line);
	}

	/** 
	 * ���Ϊ��¼ҳ�����
	 * @pre δ��ҳ��
	 *
	 * @param session �Ự
	 * @param mms ȫ��MMS����
	 * @param page ҳָ��
	 * @param pageType ҳ����
	 * @param file �������������Ĵ����ļ���
	 * @param line �������������Ĵ����к�
	 * @return �Ƿ��Ѽ���
	 */
	static inline bool lockIfPageType(Session *session, Mms *mms, void *page, ntse::PageType pageType, const char *file, uint line) {
		return mms->m_pagePool->lockPageIfType(session->getId(), page, Exclusived, pageType, -1, file, line);
	}

	/** 
	 * �Ӷ�д��
	 *
	 * @param userId �û�ID��0��ʾδ֪�û�
	 * @param lock ��д��
	 * @param lockMode ��ģʽ
	 * @param file �������������Ĵ����ļ���
	 * @param line �������������Ĵ����к�
	 */
	static inline void mmsRWLock(u16 userId, LURWLock *lock, LockMode lockMode, const char *file, uint line) {
		lock->lock(userId, lockMode, file, line);
	}

	/** 
	 * �ͷŶ�д��
	 *
	 * @param userId �û�ID��0��ʾδ֪�û�
	 * @param lock ��д��
	 * @param lockMode ��ģʽ
	 */
	static inline void mmsRWUNLock(u16 userId, LURWLock *lock, LockMode lockMode) {
		lock->unlock(userId, lockMode);
	}

	/** 
	 * ���ԼӶ�д��
	 *
	 * @param userId �û�ID��0��ʾδ֪�û�
	 * @param lock ��д��
	 * @param lockMode ��ģʽ
	 * @param file �������������Ĵ����ļ���
	 * @param line �������������Ĵ����к�
	 */
	static inline bool mmsRWTryLock(u16 userId, LURWLock *lock, LockMode lockMode, const char *file, uint line) {
		return lock->tryLock(userId, lockMode, file, line);
	}

	/** 
	 * �Ӷ�д��(����ʱ)
	 *
	 * @param userId �û�ID��0��ʾδ֪�û�
	 * @param lock ��д��
	 * @param lockMode ��ģʽ
	 * @param timeoutMms ��ʱ
	 * @param file �������������Ĵ����ļ���
	 * @param line �������������Ĵ����к�
	 */
	static inline bool mmsRWTimedLock(u16 userId, LURWLock *lock, LockMode lockMode, int timeoutMs, const char *file, uint line) {
		return lock->timedLock(userId, lockMode, timeoutMs, file, line);
	}

	/** 
	 * �������¼��־
	 *
	 * @param recPage ��¼ҳ
	 * @param mmsRecord ��¼��
	 * @param isDirty �Ƿ���Ϊ���¼
	 * @param dirtyBitmap ��λͼ, Ĭ��Ϊ0
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
			// ��ո���λͼ�����¼λͼ
			mmsRecord->m_dirtyBitmap = 0;
			mmsRecord->m_updateBitmap = 0;
		}
	}

	/**
	 * ƫ��ת���ɵ�ַ
	 *
	 * @param recPage ����ҳ��ַ
	 * @param offset ҳ��ƫ�ƣ��ض�����0
	 * @return ƫ�ƶ�Ӧ�ĵ�ַ
	 */
	static inline byte* offset2pointer(MmsRecPage *recPage, u8 offset) {
		if (offset)
			return (byte*)recPage + MMS_REC_PAGE_HEAD_SIZE + (offset - 1) * recPage->m_slotSize;
		return &recPage->m_lruHead;
	}

	/**
	 * ��ַת����ƫ��
	 *
	 * @param recPage ����ҳ��ַ
	 * @param addr ҳ�ڵ�ַ
	 * @return ��ַ��Ӧ��ƫ��
	 */
	static inline u8 pointer2offset(MmsRecPage *recPage, byte *addr) {
		assert (addr >= (byte *)recPage + MMS_REC_PAGE_HEAD_SIZE);
		
		return (u8)((addr - (byte *)recPage - MMS_REC_PAGE_HEAD_SIZE) / recPage->m_slotSize + 1);
	}

	/** 
	 * LRU�����Ƿ�Ϊ��
	 *
	 * @param recPage ��¼ҳ
	 * @return �Ƿ�Ϊ��
	 */
	static inline bool isEmptyLru(MmsRecPage *recPage) {
		return !recPage->m_lruHead; /** m_lruHead=0��ʾLRUΪ�� */
	}

	/**
	 * ��ȡ���ϼ�¼��
	 *
	 * @param recPage ��¼ҳ
	 * @return ��¼��
	 */
	static inline MmsRecord* getLruRecord(MmsRecPage *recPage) {
		return (MmsRecord *)offset2pointer(recPage, recPage->m_lruHead);
	}

	/**
	 * ��ȡ�����¼���ڵļ�¼ҳ
	 *
	 * @param pagePool ҳ�� 
	 * @param mmsRecord �����¼ͷָ��
	 * @return �����¼ҳ
	 */
	static inline MmsRecPage* getRecPage(PagePool *pagePool, MmsRecord *mmsRecord) {
		return (MmsRecPage *)pagePool->alignPage(mmsRecord);
	}

	/** 
	 * ��ӵ�ҳ��LRU����ĩβ
	 * 
	 * @param recPage ��¼ҳ
	 * @param mmsRecord ��¼��
	 */
	static inline void addToLruList(MmsRecPage *recPage, MmsRecord *mmsRecord) {
		u8 *tmpByte;

		// ��ӵ�ҳ��LRU����ĩβ
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
	 * ��ҳ��LRUɾ����¼
	 *
	 * @param recPage ��¼ҳ
	 * @param mmsRecord ��¼��
	 */
	static inline void delFromLruList(MmsRecPage *recPage, MmsRecord *mmsRecord) {
		((MmsRecord *)offset2pointer(recPage, mmsRecord->m_lruNext))->m_lruPrev = mmsRecord->m_lruPrev;
		((MmsRecord *)offset2pointer(recPage, mmsRecord->m_lruPrev))->m_lruNext = mmsRecord->m_lruNext;
	}

	/** 
	 * ����λͼ��Ϣ
	 * 
	 * @param mmsRecord ��¼��
	 * @param dirtyBitmap ��λͼ��Ϣ
	 * @param updateBitmap ����λͼ��Ϣ
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
