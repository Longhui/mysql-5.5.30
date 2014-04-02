/** ��������Ĺ����࣬��Ҫ�Ǵ�ţ���������id
 * author �ö��� xindingfeng@corp.netease.com
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
 * ���������ҳͷ��Ϣ
 */
struct TxnRecPage: public TNTIMPage {
	void init();

	TxnRecPage        *m_prev;
	TxnRecPage        *m_next;
	TrxId             m_minTxnId;
	TrxId             m_maxTxnId;
	u32               m_recCnt;	  //�����¼����
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

	TNTIMPageManager  *m_pageManager; //ҳ����Ĺ�����

	RWLock            m_lock;       //����m_head,m_tail��m_curPage����
	TxnRecPage        *m_head;		//��������������ͷҳָ��
	TxnRecPage        *m_tail;		//��������������βҳָ��
	TxnRecPage        *m_curPage;	//��ǰ���ڱ������ҳָ��

	u16               m_pageSize;	//��������������ҳ��
	u32               m_recPerPage;	//һ��ҳ���������ɵļ�¼��

	//static const u8   ALLOC_PAGE_MAX = 10; //ÿ���������ҳ��
};
}
#endif