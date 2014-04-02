/**
 * TNT���ڴ�ѵ�ʵ��
 * author �ö��� xindingfeng@corp.netease.com
 */
#ifndef _TNT_MHEAP_H_
#define _TNT_MHEAP_H_

#include "misc/Global.h"
#include "misc/TNTIMPageManager.h"
#include "misc/Session.h"
#include "misc/Txnlog.h"

#include "api/TNTDatabase.h"

#include "heap/HashIndex.h"
#include "heap/TxnRecManager.h"
#include "heap/VersionPool.h"
#include "heap/MHeapRecord.h"

using namespace ntse;
namespace tnt {
//���нڵ�
struct FreeNode {
	FreeNode(const char *name): m_lock(name, __FILE__, __LINE__), 
		m_mutex(name, __FILE__, __LINE__) {
		m_node = NULL;
		m_lastFind = NULL;
		m_size = 0;
	}

	inline void setLastFind(MRecordPage *lastFind) {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		m_lastFind = lastFind;
	}

	inline MRecordPage *getLastFind() {
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		return m_lastFind;
	}

	MRecordPage    *m_node;
	MRecordPage    *m_lastFind; //�ϴβ���ҳ��ı��
	RWLock         m_lock; //��Ҫ��������ڵ����Ӻ�ɾ�����������
	Mutex          m_mutex;//�û�����lastFind
	u32            m_size; //��freeNode�д��ڵ�pageҳ��
};

struct MHeapScanContext {
	MHeapScanContext(Session *session) {
		m_grade = 0;
		m_page = NULL;
		m_slotNum = 0;
		m_session = session;
		m_heapRec = NULL;

		m_traversePageCnt = 0;
	}

	u8				m_grade;     //����purgeҳ�����ڵĵȼ�
	MRecordPage     *m_page;     //����purgeҳ��
	u16             m_slotNum;   //����purge��¼����ҳ���slot���
	Session         *m_session;  //purge�Ự
	MHeapRec        *m_heapRec;  //��Ҫ��purge���ڴ�Ѽ�¼
	u64             m_sp;        //purge��ʼʱ��memoryContext���ڵı����

	u32             m_traversePageCnt;
};

struct MHeapStat {
	u32   m_total;          //4���ȼ���pageҳ��
	u32   m_freeNode0Size;  //��0�ȼ���pageҳ��
	u32   m_freeNode1Size;  //��1�ȼ���pageҳ��
	u32   m_freeNode2Size;  //��2�ȼ���pageҳ��
	u32   m_freeNode3Size;  //��3�ȼ���pageҳ��
	u32   m_avgSearchSize;  //���ҿ���ҳ���ƽ��search����
	u32   m_maxSearchSize;  //���ҿ���ҳ������search����

	u64   m_insert;         //insertִ�еĴ���
	u64   m_update_first;   //first updateִ�еĴ���
	u64   m_update_second;  //second updateִ�еĴ���
	u64   m_delete_first;   //first deleteִ�еĴ���
	u64   m_delete_second;  //second deleteִ�еĴ���
};

class MHeap
{
public:
	MHeap(TableDef **tableDef, TNTIMPageManager *pageManager, HashIndexOperPolicy *hashIndexOperPolicy);
	~MHeap(void);

	void replaceComponents(TableDef **tableDef);
	bool verifyAllPage(u16 tableId);

	void close(bool flush);
	void drop();

	//�ڴ�Ѵ洢�ռ����
	MRecordPage* allocRecordPage(Session *session, u16 size, u16 max, bool force, bool *isNew);
	void freeRecordPage(Session *session, FreeNode *free, MRecordPage* page);
	int freeSomePage(Session *session, int target);
	void freeAllPage(Session *session);

	void defragFreeList(Session *session);

	MHeapScanContext* beginScan(Session *session);
	bool getNext(MHeapScanContext *ctx, ReadView *readView, bool *visible = NULL);
	void endScan(MHeapScanContext *ctx);
	u64 purgePhase2(Session *session, ReadView *readView);

	//static u16 parseDumpTableHeadPage(MRecordPage *page);
	bool beginDump(Session *session, File *file, u64 *offset);
	bool dumpRealWork(Session *session, ReadView *readView, File *file, u64 *offset, VersionPoolPolicy *versionPoolPolicy) throw (NtseException);
	bool dumpCompensateRow(Session *session, ReadView *readView, File *file, u64 *offset, VersionPoolPolicy *versionPoolPolicy) throw (NtseException);
	bool writeDumpPageRecord(MRecordPage *page, byte *buf, u16 size, File *file, u64 *offset) throw (NtseException);
	void writeDumpEndPage(Session *session, File *file, u64 *offset) throw (NtseException);
	void finishDumpCompensate();
	bool beginPurge();
	void finishPurgeCompensate();
	void resetStatus();
	Array<RowId> *getCompensateRows() {
		return &m_compensateRows;
	}

	static void extendDumpFileSize(File *file, u64 incrSize) throw (NtseException);
	void writeDumpFile(File *file, u64 offset, byte *buf, u32 size) throw (NtseException);

	bool readDump(Session *session, RowIdVersion version, File *file, u64 *offset, u64 fileSize);
	void buildHashIndex(Session *session, MRecordPage *page, RowIdVersion version, bool compensate);

	void addToFreeList(MRecordPage* page);

	MHeapRec* getHeapRecord(Session *session, void *ptr, RowId rid);
	MHeapRec* getHeapRecordSafe(Session *session, void *ptr, RowId rid);
	MHeapRec* getHeapRedRecord(Session *session, void *ptr, RowId rid, Record *rec);
	MHeapRec* getHeapRedRecordSafe(Session *session, void *ptr, RowId rid, Record *rec);

	bool checkAndLatchPage(Session *session, void *ptr, RowId rid, LockMode mode = Shared);

	inline void unLatchPageByPtr(Session *session, void *ptr, LockMode mode) {
		MRecordPage *page = (MRecordPage *)GET_PAGE_HEADER(ptr);
		//UNLATCH_TNTIM_PAGE(session, m_pageManager, page, mode);
		m_pageManager->unlatchPage(session->getId(), page, mode);
	}

	bool insertHeapRecordAndHash(Session *session, TrxId txnId, RowId rollBackId, u8 vTableIndex, Record *rec, u8 delbit, RowIdVersion version);
	bool insertHeapRecordAndHash(Session *session, MHeapRec *heapRec, RowIdVersion version);
	bool updateHeapRecordAndHash(Session *session, RowId rowId, void *ptr, MHeapRec *heapRec, RowIdVersion version = 0);
	bool removeHeapRecordAndHash(Session *session, RowId rowId);
	bool updateHeapRec2TxnIDHash(Session *session, RowId rowId, TrxId trxId, RowIdVersion version = 0);

	bool remapHeapRecord(Session *session, void *ptr, RowId oldRid, RowId newRid);

	MHeapRec* getVersionHeapRecord(Session *session, MHeapRec *heapRec, ReadView *readView, VersionPoolPolicy *versionPool);
	MHeapRec* getVersionHeapRedRecord(Session *session, MHeapRec *heapRec, ReadView *readView, VersionPoolPolicy *versionPool);
	//bool updateHeapRecordAndHash(Session *session, void *ptr, MHeapRec *heapRec, RowIdVersion version = 0);

	MHeapStat getMHeapStat();
	void printFreeList(int index);

#ifndef NTSE_UNIT_TEST
private:
#endif

	bool removeHeapRecordAndHash(void *ptr, RowId rowId);

	void* getSlotAndLatchPage(Session *session, RowId rid, LockMode mode, MRecordPage **page);
	MHeapRec* getHeapRecordAndLatchPage(Session *session, void *ptr, RowId rid, LockMode mode, bool copyData, MRecordPage **page);
	void addToFreeListSafe(FreeNode *free, MRecordPage* page);
	void removeFromFreeListSafe(FreeNode *free, MRecordPage *page);

	inline void latchPage(Session *session, MRecordPage *page, LockMode mode, const char *file, uint line) {
		//LATCH_TNTIM_PAGE(session, m_pageManager, page, mode);
		m_pageManager->latchPage(session->getId(), page, mode, file, line);
	}

	inline void unLatchPage(Session *session, MRecordPage *page, LockMode mode) {
		//UNLATCH_TNTIM_PAGE(session, m_pageManager, page, mode);
		m_pageManager->unlatchPage(session->getId(), page, mode);
	}

	inline bool latchPageIfType(Session *session, MRecordPage *page, LockMode mode, PageType type, int timeoutMs, const char *file, uint line) {
		//return LATCH_TNTIM_PAGE_IF_TYPE(session, m_pageManager, page, mode, type, timeoutMs);
		return m_pageManager->latchPageIfType(session->getId(), page, mode, type, timeoutMs, file, line);
	}

	inline bool tryLatchPage(Session *session, MRecordPage *page, LockMode mode, const char *file, uint line) {
		//return TRY_LATCH_TNTIM_PAGE(session, m_pageManager, page, mode);
		return m_pageManager->tryLatchPage(session->getId(), page, mode, file, line);
	}

	void *appendRecordForce(MRecordPage *page, byte *buf, u16 recSize);
	void *updateRecordForce(MRecordPage *page, void *ptr, byte *buf, u16 recSize);

	FreeNode		  **m_freeList;		//������������
	u8				  m_gradeNum;		//���м������

	TNTIMPageManager  *m_pageManager;
	TableDef          **m_tableDef;
	u16               m_tableId;

	HashIndexOperPolicy *m_hashIndexOperPolicy;     //hash����

	Mutex             m_statusLock;
	TNTStat        m_state;
	Array<RowId>      m_compensateRows;   //dump,purge�ڼ���Ҫ������rowid

	MHeapStat         m_stat;
	u64               m_totalAllocCnt; //�ܹ�alloc����
	u64               m_totalSearchCnt; //alloc search page�Ĵ���

	static const u16 SEARCH_MAX = 10;
	static const u8 FREE_NODE_GRADE_NUM = 4;

	static const u64 EXTEND_DUMPFILE_SIZE = MRecordPage::TNTIM_PAGE_SIZE << 2;

	//����2��������Ҫ�Ǳ��ⳤʱ��ռ��������
	static const u16 DEFRAG_BATCH_SIZE = 50;
	static const u16 FREE_BATCH_SIZE = 20;
	static const u16 PURGE2_BATCH_SIZE = 20;

	static const u16 RETRY_COUNT = 5;

friend class MRecords;
};
}

#endif
