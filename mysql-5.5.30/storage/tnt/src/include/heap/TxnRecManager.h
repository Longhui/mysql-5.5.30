/** 事务数组的管理类，主要是存放，回收事务id
 * author 忻丁峰 xindingfeng@corp.netease.com
 */
#ifndef _TNT_TXNRECMANAGER_H_
#define _TNT_TXNRECMANAGER_H_

#include "misc/Global.h"
#include "misc/TNTIMPageManager.h"
#include "misc/Session.h"
#include "heap/HashIndex.h"
#include "trx/TNTTransaction.h"

using namespace ntse;

namespace tnt {

#define FIRST_ALLOC_PAGE_SIZE     2
#define MAX_U64                   0xFFFFFFFF << 8 |  0xFFFFFFFF

struct TxnRec {
	TxnRec() {}

	TxnRec(TrxId txnId, RowId rowId) {
		m_txnId = txnId;
		m_rowId = rowId;
	}

	TrxId   m_txnId;
	RowId   m_rowId;
};

/**
 * 事务数组的页头信息
 */
struct TxnRecPage: public TNTIMPage {
	void init();

	TxnRecPage        *m_prev;
	TxnRecPage        *m_next;
	TrxId             m_minTxnId;
	TrxId             m_maxTxnId;
	u32               m_recCnt;	  //事务记录条数
};

class TxnRecManager
{
public:
	TxnRecManager(TNTIMPageManager *pageManager);
	void init(Session *session);
	TxnRec* push(Session *session, TxnRec *rec);
	void defrag(Session *session, TrxId minReadView, HashIndexOperPolicy *policy);
	u16 freeSomePage(Session *session, u16 size);
	~TxnRecManager(void);

#ifndef NTSE_UNIT_TEST
private:
#endif

	bool allocPage(Session *session, u16 incrSize);

	inline void latchPage(Session *session, TxnRecPage *page, LockMode mode) {
		LATCH_TNTIM_PAGE(session, m_pageManager, page, mode);
	}

	inline void unLatchPage(Session *session, TxnRecPage *page, LockMode mode) {
		UNLATCH_TNTIM_PAGE(session, m_pageManager, page, mode);
	}

	TNTIMPageManager  *m_pageManager; //页分配的管理类

	RWLock            m_lock;       //保护m_head,m_tail和m_curPage数据
	TxnRecPage        *m_head;		//分配给事务数组的头页指针
	TxnRecPage        *m_tail;		//分配给事务数组的尾页指针
	TxnRecPage        *m_curPage;	//当前正在被分配的页指针

	u16               m_pageSize;	//分配给事务数组的页数
	u32               m_recPerPage;	//一个页上所能容纳的记录数

	//static const u8   ALLOC_PAGE_MAX = 10; //每次最多分配的页数
};
}
#endif