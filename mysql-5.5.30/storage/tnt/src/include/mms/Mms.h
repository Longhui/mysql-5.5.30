/**
 * MMS��¼����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_MMS_H_
#define _NTSE_MMS_H_

#include <vector>
#include <iostream>
#include "misc/Global.h"
#include "misc/Session.h"
#include "util/Array.h"
#include "util/DList.h"
#include "util/PagePool.h"
#include "util/Portable.h"
#include "util/Thread.h"
#include "heap/Heap.h"

using namespace std;

namespace ntse {

class Table;
class SubRecord;
class Record;
struct ColList;

class Mms;
class MmsTable;
class MmsRPClass;
class MmsFreqHeap;
class MmsOldestHeap;
class MmsPageOper;
class MmsRidMap;
class MmsFlushTimer;
class MmsReplacer;

struct MmsRecord;
struct MmsRecPage;
struct MmsRecPair;

/** 8�ֽڶ��� */
#define MMS_ALIGN_8(size)	((size + 7) & (~7))
/** MMS�����¼ҳͷ���� */
#define MMS_REC_PAGE_HEAD_SIZE MMS_ALIGN_8(sizeof(MmsRecPage))
/** ��󻺴��¼���� */
#define MMS_MAX_RECORD_SIZE (Limits::PAGE_SIZE - MMS_REC_PAGE_HEAD_SIZE)
/** ҳ������������ */
#define MMS_RPC_GROWTH_FACTOR	1.25

/** ȫ��MMSʹ��״̬ */
struct MmsStatus {
	u64		m_recordQueries;	/** MMS��¼��ѯ����	*/			
	u64		m_recordQueryHits;	/** MMS��¼��ѯ���д��� */
	u64		m_recordInserts;	/** MMS��¼�������	*/			
	u64		m_recordDeletes;	/** MMS��¼��ɾ������ */	
	u64		m_recordUpdates;	/** MMS��¼�����´��� */
	u64		m_recordVictims;    /** MMS��¼���滻���� */
	u64		m_pageVictims;		/** MMS����ҳ���滻���� */	
	u64		m_occupiedPages;	/** MMSռ�õ��ڴ�ҳ������������ϣ��ռ�õ�ҳ��ͻ���Ŀ���ҳ */
	u64		m_recordPages;		/** ռ�õļ�¼ҳ�� */
};

/** MMS��ʹ��״̬ */
struct MmsTableStatus {
	u64		m_records;			/** MMS���л����¼����	*/
	u64		m_recordPages;		/** MMS��ռ�õļ�¼ҳ���� */
	u64		m_recordQueries;	/** MMS���¼��ѯ���� */
	u64		m_recordQueryHits;	/** MMS���¼��ѯ���д��� */
	u64		m_recordInserts;	/** MMS���¼������� */ 
	u64		m_recordDeletes;	/** MMS���¼ɾ������ */
	u64		m_recordUpdates;	/** MMS���¼���´��� */
	u64		m_recordVictims;	/** MMS���¼���滻���� */
	u64		m_pageVictims;		/** MMS����ҳ���滻���� */
	u64		m_replaceFailsWhenPut; /** MMS�����ʱ�滻ʧ�ܴ��� */
	u64		m_updateMerges;		/** ������ºϲ����� */
	u64		m_dirtyRecords;		/** MMS�������¼���� */
};

/** MMSҳ��ʹ��״̬ */
struct MmsRPClassStatus {
	u64		m_recordPages;		/** ��ҳ����ռ�õĻ���ҳ���� */
	u64		m_freePages;		/** ��ҳ�������п��в۵�ҳ���� */
	u64		m_records;			/**  ��ҳ�����л����¼���� */
	u64		m_recordInserts;	/** ��ҳ�����м�¼������� */
	u64		m_recordDeletes;	/** ��ҳ�����м�¼��ɾ������ */
	u64		m_recordUpdates;	/** ��ҳ�����м�¼�����´��� */
	u64		m_recordVictims;	/** ��ҳ�����м�¼���滻���� */
	u64		m_pageInserts;		/** ��ҳ�����л���ҳ������� */
	u64		m_pageDeletes;		/** ��ҳ�����л���ҳɾ������ */
	u64		m_pageVictims;		/** ��ҳ�����л���ҳ���滻���� */						
};

/** BINLOGʹ�� */
/**
 * mmsˢ�¸��»����ʱ����õ�дbinlog�ص�����
 * @param session		�Ự
 * @param dirtyRecord	mms���µļ�¼���ݣ������˼�¼�������Լ����µ��������ݣ�һ����REC_REDUNDANT��ʽ��
 * @param data			������Ϣ����
 */
typedef void (*MMSBinlogWriter)(Session *session, SubRecord *dirtyRecord, void *data);
struct MMSBinlogCallBack {
	MMSBinlogCallBack(MMSBinlogWriter writer, void *data) : m_writer(writer), m_data(data) {}
	MMSBinlogWriter m_writer;      /** �ص�����ָ�붨�� */
	void			*m_data;       /** �����ϲ㴫��һ�����������ָ����Ϊ�ص������Ĳ��� */
};



/** MMS��¼����ҳ�� */
class MmsRPClass {
public:
	const MmsRPClassStatus& getStatus();
	u16 getMinRecSize();
	u16 getMaxRecSize();

private:
	MmsRPClass(MmsTable *mmsTable, u16 slotSize);
	~MmsRPClass();
	MmsRecPage* getRecPageFromFreelist();
	void delRecPageFromFreelist(MmsRecPage *recPage);
	void addRecPageToFreeList(MmsRecPage *recPage);
	void freeAllPages(Session *session);

private:
	MmsTable			*m_mmsTable;	/** ������MMS�� */
	u16					m_slotSize;		/** ҳ���л���۴�С */
	u8					m_numSlots;		/** ÿҳ�м�¼�۸��� */
	DList<MmsRecPage *> m_freeList;		/** ���м�¼ҳ˫������ */
	MmsOldestHeap		*m_oldestHeap;	/** ���ϻ���ҳ�� */
	MmsRPClassStatus    m_status;		/** ʹ��״̬ͳ�� */

	friend class Mms;
	friend class MmsTable;
	friend class MmsPageOper;
	friend class MmsOldestHeap;
};

class Database;
/** MMSȫ���� */
class Mms : public PagePoolUser {
public:
	Mms(Database *db, uint targetSize, PagePool *pagePool, bool needRedo = true, float replaceRatio = 0.01, int intervalReplacer = 60, float minFPageRatio = 0.01, float maxFPageRatio = 100.0);	
	void close();
	void stopReplacer();
	void registerMmsTable(Session *session, MmsTable *mmsTable);
	void unregisterMmsTable(Session *session, MmsTable *mmsTable);
	void setMaxNrDirtyRecs(int threshold);
	int getMaxNrDirtyRecs();
	float getPageReplaceRatio();
	bool setPageReplaceRatio(float ratio);
	virtual uint freeSomePages(u16 userId, uint numPages);
	void endRedo();
	const MmsStatus& getStatus();
	void printStatus(ostream &out);
	void computeDeltaQueries(Session *session);

#ifdef NTSE_UNIT_TEST
	void lockMmsTable(u16 userId);
	void unlockMmsTable(u16 userId);
	void lockRecPage(Session *session);
	void unlockRecPage(Session *session);
	void pinRecPage();
	void unpinRecPage();
	void mmsTestGetPage(MmsRecPage *recPage);
	void mmsTestGetTable(MmsTable *mmsTable);
	void mmsTestSetTask(MmsRPClass *rpClass, int taskCount);
	void runReplacerForce();
#endif

private:
	void* allocMmsPage(Session *session, bool external);
	void freeMmsPage(Session *session, MmsTable *mmsTable, void *page);
	int getReplacedPageNum();

private:
	Database	*m_db;							/** ���ݿ���� */
	Connection	*m_fspConn;						/** ������freeSomePages�����ݿ����� */
	u8			m_nrClasses;					/** �༶����� */
	u16			*m_pageClassSize;				/** �༶��ҳ��С */
	u16			*m_pageClassNrSlots;			/** �༶��۸��� */
	u16			*m_size2class;					/** ����ӳ��� */
	MmsReplacer *m_replacer;					/** ҳ�滻�߳�ʵ�� */
	PagePool			*m_pagePool;			/** ����ҳ�� */
	bool				m_duringRedo;			/** �Ƿ�����REDO */
	LURWLock			m_mmsLock;				/** Mmsȫ���� */
	DList<MmsTable *>	m_mmsTableList;			/** MmsTable˫������ */
	Atomic<long>		m_numPagesInUse;		/** MMSʹ�õ��ڴ�ҳ���� */
	int					m_maxNrDirtyRecs;		/** ��ʱ��������¼�� */
	float				m_pgRpcRatio;			/** ҳ�滻���ʣ�[0, 1]֮�� */
	float				m_replaceRatio;			/** �滻�� */									
	MmsStatus			m_status;				/** ʹ��״̬ͳ�� */
	u64					m_preRecordQueries;		/** ��ǰ��¼��ѯ���� */
	float				m_minFPageRatio;		/** ��СFPage�������� */
	float				m_maxFPageRatio;		/** ���FPage�������� */

#ifdef NTSE_UNIT_TEST
	int				m_taskCount;				/** ����������� */
	int				m_taskNum;					/** ��ǰ���������� */
	MmsOldestHeap	*m_taskBottomHeap;			/** ���������漰�ĵ�ҳ�� */
	MmsFreqHeap		*m_taskTopHeap;				/** ���������漰�Ķ�ҳ�� */
	MmsTable		*m_taskTable;				/** ���������漰��MMS�� */
	MmsRecPage		*m_taskPage;				/** ���������漰�ļ�¼ҳ */
#endif
	
	friend class MmsTable;
	friend class MmsRPClass;
	friend class MmsPageOper;
	friend class MmsOldestHeap;
	friend class MmsFreqHeap;
	friend class MmsRidMap;
	friend class MmsReplacer;
};

/** һ�����Ӧ��MMSϵͳ */
class MmsTable {
public:
	MmsTable(Mms *mms, Database *db, DrsHeap *drsHeap, const TableDef *tableDef, bool cacheUpdate, uint updateCacheTime, int partitionNr = 31);
	void close(Session *session, bool flushDirty);
	uint getUpdateCacheTime();
	void setUpdateCacheTime(uint updateCacheTime);
	MmsRecord* getByRid(Session *session, RowId rid, bool touch, RowLockHandle **rlh, LockMode lockMode);
	MmsRecord* putIfNotExist(Session *session, const Record *record);
	void unpinRecord(Session *session, const MmsRecord *record);
	bool canUpdate(MmsRecord *record, const SubRecord *subRecord, u16 *recSize);
	bool canUpdate(MmsRecord *record, u16 newSize);
	void update(Session *session, MmsRecord *record, const SubRecord *subRecord, u16 recSize, Record *newCprsRcd = NULL);
	void flushAndDel(Session *session, MmsRecord *record);
	void del(Session *session, MmsRecord *record);
	void flush(Session *session, bool force, bool ignoreCancel = true) throw(NtseException);
	bool getSubRecord(Session *session, RowId rid, SubrecExtractor *extractor, SubRecord *subRec, bool touch, bool ifDirty, u32 readMask);
	void getSubRecord(const MmsRecord *mmsRecord, SubrecExtractor *extractor, SubRecord *subRecord);
	void getRecord(const MmsRecord *mmsRecord, Record *rec, bool copyIt = true);
	bool isDirty(const MmsRecord *mmsRecord);
	void redoUpdate(Session *session, const byte *log, uint size);
	MmsRPClass** getMmsRPClass(int *nrRPClasses);
	const MmsTableStatus& getStatus();
	void flushUpdateLog(Session *session);
	const LURWLock& getTableLock();
	void setMaxRecordCount(u64 recordSize = (u64)-1);
	void setMapPartitions(int ridNrZone);
	void setCacheUpdate(bool cacheUpdate);
	uint getPartitionNumber();
	void getRidHashConflictStatus(uint partition, double *avgConflictLen, size_t *maxConflictLen);
	void setBinlogCallback(MMSBinlogCallBack *mmsCallback);
	void setCprsRecordExtrator(CmprssRecordExtractor *cprsRcdExtractor);

// ��Ԫ����ʹ��
#ifdef NTSE_UNIT_TEST
	void lockMmsTable(u16 userId);
	void unlockMmsTable(u16 userId);
	void flushMmsLog(Session *session);
	void disableAutoFlushLog();
	void evictCurrPage(Session *session);
	void pinCurrPage();
	void unpinCurrPage();
	void disableCurrPage();
	void lockCurrPage(Session *session);
	void unlockCurrPage(Session *session);
	void delCurrRecord();
	void mmsTableTestGetPage(MmsRecPage *recPage);
	void setRpClass(MmsRPClass *rpClass);
	void setMmsTableInRpClass(MmsTable *mmsTable);
	void runFlushTimerForce();
#endif

public:
	static const u64 MAX_UPDATE_CACHE_COLUMNS = 32;
	
private:
	void getSubRecord(Session *session, const MmsRecord *mmsRecord, SubRecord *subRecord);
	MmsRecPage* allocMmsRecord(Session *session, MmsRPClass *rpClass, int ridNr, bool *locked);
	void delRecord(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord);
	void evictMmsPage(Session *session, MmsRecPage *victimPage);
	void sortAndFlush(Session *session, bool force, bool ignoreCancel, MmsTable *table, std::vector<MmsRecPair> *tmpRecArray) throw(NtseException);
	void writeDataToDrs(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord, bool force);
	MmsRecord* put(Session *session, const Record *record, u32 dirtyBitmap, int ridNr, bool *tryAgain);
	void doFlush(Session *session, bool force, bool ignoreCancel, bool tblLocked) throw(NtseException);
	bool doTouch(Session *session, MmsRecPage *recPage, MmsRecord *mmsRecord);
	void doMmsLog(Session *session, MemoryContext *mc, const SubRecord *subRecord);
	void writeMmsUpdateLog(Session *session, MmsRecord *mmsRecord, u32 updBitmap);
	void writeCachedBinlog(Session *session, const ColList &updCachedCols, MmsRecord *mmsRecord);
	void writeMmsUpdateLog(Session *session, const SubRecord *subRecord);
	void writeMmsUpdateLog(Session *session, MmsRecord *mmsRecord, const SubRecord *subRecord, u32 updBitmap);
	void flushLog(Session *session, bool force = false);

	int dirtyBm2cols(u32 dirtyBitmap, u16 *cols);
	int updBm2cols(u32 updBitmap, u16 *cols);
	bool cols2bitmap(const SubRecord *subRecord, u32 *updBitmap, u32 *dirtyBitmap);
	int mergeCols(const SubRecord *subRecord, u32 updBitmap, u16 *cols, int numCols);
	int mergeCols(u16 *src1Cols, int num1Cols, u16 *src2Cols, int num2Cols, u16 *dstCols);
	
	inline int getRidPartition(const RowId rid) {
		return (int)(rid % m_ridNrZone);
	}

	RecFormat getMmsRecFormat(const TableDef *tableDef, const MmsRecord *mmsRecord);

private:
	Mms					*m_mms;								/** �����ļ�¼��������� */
	Database			*m_db;								/** ���������ݿ� */
	DrsHeap				*m_drsHeap;							/** �����Ķ� */
	const TableDef            *m_tableDef;
	LURWLock			m_mmsTblLock;						/** MMS���д�� */
	LURWLock			**m_ridLocks;						/** RID������ */
	MmsFreqHeap			*m_freqHeap;						/** ���Ƶ������ҳ�� */
	MmsRPClass			**m_rpClasses;						/** ҳ��ָ������ */
	MmsRidMap			**m_ridMaps;						/** RID����ӳ��� */
	bool				m_cacheUpdate;						/** �Ƿ񻺴���� */
	MmsFlushTimer		*m_flushTimer;						/** ���¼��ˢ�¶�ʱ�� */
	MMSBinlogCallBack   *m_binlogCallback;					/** BINLOG�ص� */
	u8					m_updateCacheNumCols;				/** ���»����ֶ��� */				
	u16					*m_updateCacheCols;					/** ���»���֧���ֶ��б� */
	byte				*m_updateBitmapOffsets;				/** ���»���λͼƫ���б� */
	Array<MmsRecPage *> *m_recPageArray;					/** ��¼ҳָ������ */
	MmsTableStatus		m_status;							/** ʹ��״̬ͳ�� */
	Atomic<int>			m_inLogFlush;						/** �Ƿ���ˢд��־ */
	Atomic<long>		m_numDirtyRecords;					/** MMS�����¼���� */ 
	u64					m_maxRecordCount;					/** MMS������������ŵļ�¼���� */
	int					m_ridNrZone;						/** RIDӳ������� */
	Atomic<int>			m_existPin;							/** �����PIN */
	u64					m_preRecordQueries;					/** ��ǰ�ļ�¼��ѯ�� */
	float				m_deltaRecordQueries;				/** ��¼��ѯ����ֵ�� */

	CmprssRecordExtractor *m_cprsRcdExtractor;                 /** ѹ����¼��ȡ�� */

// ��Ԫ����ʹ��
#ifdef NTSE_UNIT_TEST
	bool				m_autoFlushLog;
	MmsRecPage			*m_testCurrPage;
	MmsRecord			*m_testCurrRecord;
	MmsRPClass			*m_testCurrRPClass;
#endif

	friend class Mms;	
	friend class MmsRPClass;
	friend class MmsFreqHeap;
	friend class MmsOldestHeap;
	friend class MmsRidMap;
	friend class MmsFlushTimer;
	friend class MmsPageOper;
};

/** ��������־ˢд��̨�̡߳�*/
class MmsFlushTimer : public BgTask {
public:
	MmsFlushTimer(Database *db, MmsTable *mmsTable, uint interval);
	void runIt();

private:
	MmsTable			*m_mmsTable;						/** ������MMS�� */
};

/** ��ҳ�滻��̨�̡߳�*/
class MmsReplacer : public BgTask {
public:
	MmsReplacer(Mms *mms, Database *db, uint interval);
	void runIt();

private:
	Mms					*m_mms;								/** ����MMS	*/
	Database			*m_db;		/** ���ݿ� */
};

#ifdef NTSE_UNIT_TEST

#define MMS_TEST_GET_PAGE(mms, page) (mms)->mmsTestGetPage((page))
#define MMS_TEST_GET_TABLE(mms, tbl) (mms)->mmsTestGetTable((tbl))
#define MMS_TEST_SET_TASK(mms, rpclass, cnt) (mms)->mmsTestSetTask((rpclass), (cnt))
#define MMSTABLE_TEST_GET_PAGE(mmsTable, page) (mmsTable)->mmsTableTestGetPage((page))

#else

#define MMS_TEST_GET_PAGE(mms, page)
#define MMS_TEST_GET_TABLE(mms, tbl)
#define MMS_TEST_SET_TASK(mms, rpclass, cnt)
#define MMSTABLE_TEST_GET_PAGE(mmsTable, page)

#endif

}
#endif


