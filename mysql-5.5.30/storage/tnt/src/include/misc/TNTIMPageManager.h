/**
* TNTIM�ڴ�ҳ�����
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _TNTIM_PAGE_MANAGER_H_ 
#define _TNTIM_PAGE_MANAGER_H_

#include "util/PagePool.h"
#include "misc/Global.h"

using namespace ntse;

namespace tnt {

/** TNTIM �ڴ�ҳ������������ʵ����! */
class TNTIMPage {
public:
	static const uint TNTIM_PAGE_SIZE = ntse::Limits::PAGE_SIZE; //TNT�ڴ�ҳ���С

protected:
	TNTIMPage() {}
	~TNTIMPage() {}
};

#define LATCH_TNTIM_PAGE_IF_TYPE(session, pageManager, page, lockMode, pageType, timeoutMs) \
	(pageManager)->latchPageIfType(session->getId(), page, lockMode, pageType, timeoutMs, __FILE__, __LINE__)
#define LATCH_TNTIM_PAGE(session, pageManager, page, lockMode) \
	(pageManager)->latchPage(session->getId(), page, lockMode, __FILE__, __LINE__)
#define TRY_LATCH_TNTIM_PAGE(session, pageManager, page, lockMode) \
	(pageManager)->tryLatchPage(session->getId(), page, lockMode, __FILE__, __LINE__)
#define UNLATCH_TNTIM_PAGE(session, pageManager, page, lockMode) \
	(pageManager)->unlatchPage(session->getId(), page, lockMode)
#define TRY_UPGRADE_LATCH_TNTIM_PAGE(session, pageManager, page) \
	(pageManager)->tryUpgradePageLatch(session->getId(), page, __FILE__, __LINE__)

/** TNTIM�ڴ�ҳ����� */
class TNTIMPageManager : public PagePoolUser {
private:
	//����������
	struct FreeListLink {
		TNTIMPage *m_next;
	};

public:
	TNTIMPageManager(uint targetSize, uint totalBorrowSize, PagePool *pool);
	virtual ~TNTIMPageManager();

	TNTIMPage* getPage(u16 userId, PageType pageType, bool force = false);
	void releasePage(u16 userId, TNTIMPage* page);
	virtual uint freeSomePages(u16 userId, uint numPages);
	
	/**
	* ���������ַΪָ�����͵��ڴ�ҳ��������ҳ
	*
	* @param userId �û�ID
	* @param page �ڴ�ҳ
	* @param lockMode ��ģʽ
	* @param pageType ҳ������
	* @param timeoutMs ��ʱʱ�䣬��Ϊ0�����ϳ�ʱ����Ϊ<0�򲻳�ʱ
	* @param file �������������Ĵ����ļ���
	* @param line �������������Ĵ����к�
	* @return ���������ַΪָ�����͵��ڴ�ҳ���򷵻�true�����򷵻�false
	*/
	inline bool latchPageIfType(u16 userId, TNTIMPage *page, LockMode lockMode, PageType pageType, 
		int timeoutMs, const char *file, uint line) {
			assert(PAGE_MEM_HEAP <= pageType && PAGE_MEM_INDEX >= pageType);
			return m_pool->lockPageIfType(userId, page, lockMode, pageType, timeoutMs, file, line);
	}

	/**
	 * �ж�һ��ҳ���Ƿ���ָ������ģʽ����
	 * @param page �ڴ�ҳ
	 * @param lockMode ��ģʽ
	 * @return 
	 */
	inline bool isPageLatched(TNTIMPage *page, LockMode lockMode) {
		return m_pool->isPageLocked(page, lockMode);
	}
	
	/**
	 * ��õ�ǰҳ���latch��ģʽ
	 * @param page �ڴ�ҳ
	 * @return ��ģʽ
	 */
	inline LockMode getPageLatchMode(TNTIMPage *page) const {
		return m_pool->getPageLockMode(page);
	}

	/**
	* ����һ���ڴ�ҳ
	*
	* @param userId �û�ID
	* @param page �ڴ�ҳ
	* @param lockMode ��ģʽ
	* @param file �������������Ĵ����ļ���
	* @param line �������������Ĵ����к�
	*/
	inline void latchPage(u16 userId, TNTIMPage *page, LockMode lockMode, const char *file, uint line) {
		m_pool->lockPage(userId, page, lockMode, file, line);
	}

	/**
	* ��������һ���ڴ�ҳ
	*
	* @param userId �û�ID
	* @param page �ڴ�ҳ
	* @param lockMode ��ģʽ
	* @param file �������������Ĵ����ļ���
	* @param line �������������Ĵ����к�
	* @return �ɹ����
	*/
	inline bool tryLatchPage(u16 userId, TNTIMPage *page, LockMode lockMode, const char *file, uint line) {
		return m_pool->trylockPage(userId, page, lockMode, file, line);
	}

	/**
	* �����һ���ڴ�ҳ������
	*
	* @param userId �û�ID
	* @param page �ڴ�ҳ
	* @param lockMode ��ģʽ
	*/
	inline void unlatchPage(u16 userId, TNTIMPage *page, LockMode lockMode) {
		m_pool->unlockPage(userId, page, lockMode);
	}

	/**
	* ���Խ���������
	* @pre �����߼ӵ��Ƕ���
	*
	* @param userId �û�ID
	* @param �ڴ�ҳ
	* @param file �������������Ĵ����ļ���
	* @param line �������������Ĵ����к�
	* @return �ɹ����
	*/
	inline bool tryUpgradePageLatch(u16 userId, TNTIMPage *page, const char *file, uint line) {
		return m_pool->tryUpgradePageLock(userId, page, file, line);
	}

	/**
	* ��ȡTNTIM��Դ�����������ҳ����
	*
	* @return ���ҳ����
	*/
	inline uint getMaxSizeForBigTrx() {
		return m_maxSizeForBigTrx;
	}
	
	/**
	* ��ȡTNTIM���С����������ҳ����
	*
	* @return ���ҳ����
	*/
	inline uint getMaxSizeForSmallTrx() {
		return m_maxSizeForSmallTrx;
	}


	/**
	* ���TNTIM��С�Ƿ�ﵽ��ҳ�������
	*
	* @param bigTrx �Ƿ���Դ�����
	* @return �ﵽ���޷���true�����򷵻�false
	*/
	inline bool reachedUpperBound(bool bigTrx) {
		u32 maxTntimSize = bigTrx? m_maxSizeForBigTrx: m_maxSizeForSmallTrx;
		return m_currentSize >= maxTntimSize;
	}
private:
	//void addToPageFreeList(TNTIMPage *page);
	//TNTIMPage* getPageFromFreeList(u16 userId, PageType pageType, bool needLock = true);

//private:
	//Mutex        m_lock;         /* ����ȫ�ֽṹ�Ļ����� */
	//TNTIMPage	 *m_freePageList;/* ����ҳ������ */
	uint			m_maxSizeForBigTrx;							 //��Դ�������ҳ��������
	uint			m_maxSizeForSmallTrx;						 //���С������ҳ��������

	static const uint TNTIM_UPPBOUND_BIGTRX = 10;				 //��Դ��������ܹ����õ�NTSE PAGE BUFFER��С����������
	static const uint TNTIM_UPPBOUND_SMALLTRX = 30;				 //���С�������ܹ����õ�NTSE PAGE BUFFER��С����������
};

}

#endif