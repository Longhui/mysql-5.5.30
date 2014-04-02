/**
 * ���ļ�����
 *
 * @author л��(xieke@corp.netease.com, ken@163.org)
 */

#ifndef _NTSE_HEAP_H_
#define _NTSE_HEAP_H_

#include "misc/Global.h"
#include "misc/Buffer.h"
#include "misc/Sample.h"
#include "misc/Record.h"
#ifdef TNT_ENGINE
#include "misc/Txnlog.h"
#endif

namespace ntse {


class FixedLengthRecordHeap;
class VariableLengthRecordHeap;
struct BufScanHandle;
struct LogEntry;

/** ���ֲ�ͬ�Ķ����� */
enum HeapVersion {
	HEAP_VERSION_FLR = 0,
	HEAP_VERSION_VLR = 1,
}; 

struct DBObjStats;
/** �ѵ�ͳ����Ϣ */
struct HeapStatus {
	DBObjStats	*m_dboStats;		/** �������ݿ�����е�ͳ����Ϣ */
	u64			m_dataLength;		/** ռ�õ����ݴ�С��ֻ������ʹ�õ�ҳ�棬��λ�ֽ��� */
	u64 		m_rowsReadSubRec;	/** ��¼��ȡSubRecord�Ĵ��� */
	u64 		m_rowsReadRecord;	/** ��¼��ȡRecord�Ĵ��� */
	u64 		m_rowsUpdateSubRec;	/** ��¼��SubRecord���µĴ��� */
	u64 		m_rowsUpdateRecord;	/** ��¼��Record���µĴ��� */
};

/** ��ģ����չͳ����Ϣ��ͨ��������ã�����ȷ�� */
struct HeapStatusEx {
	u64		m_numRows;		/** ���м�¼�� */
	u64		m_numCmprsRows; /** ����ѹ���ļ�¼�� */
	u64		m_numLinks;		/** ���Ӽ�¼����ֻ�Ա䳤�������� */
	double	m_pctUsed;		/** ҳ�������� */
	double  m_cmprsRatio;   /** ѹ���� */
};

/** ����ҳ������Ϣ�ṹ */
struct HeapHeaderPageInfo {
	BufferPageHdr	m_bph;		/** ����ҳ����ͷ�ṹ */
	u64				m_pageNum;	/** ���а�������ҳ������������ҳ */
	u64				m_maxUsed;	/** �������ʹ�õ���ҳ��� */     
	u8				m_version;	/** �����Ͱ汾 */
};

class Database;
class TableDef;
class Record;
class SubRecord;
class Session;
class BufferPageHandle;
struct BufferPageHdr;
class Buffer;
class File;
class DrsHeapScanHandle;
class RowLockHandle;
struct DBObjStats;
class SubrecExtractor;
/** ���־���Ķ��ļ�ʵ�ֵĻ��� */
class DrsHeap : public Analysable {
public:
	DrsHeap(Database *db, const TableDef *tableDef, File *heapFile, BufferPageHdr *headerPage, DBObjStats* dboStats);
	virtual ~DrsHeap();
	static void create(Database *db, const char *path, const TableDef *tableDef) throw(NtseException);
	static DrsHeap* open(Database *db, Session *session, const char *path, const TableDef *tableDef) throw(NtseException);
	static void drop(const char *path) throw(NtseException);
	virtual void close(Session *session, bool flushDirty);
	virtual void flush(Session *session);
	/**
	 * ���ü�¼ѹ��������
	 */
	inline void setCompressRcdExtrator(CmprssRecordExtractor* cprsRcdExtrator) {
		m_cprsRcdExtrator = cprsRcdExtrator;
	}

	inline CmprssRecordExtractor* getCompressRcdExtrator() const {
		return m_cprsRcdExtrator;
	}
#ifdef NTSE_UNIT_TEST
	/**
	 * ��ҳ���ڱ�Ҫ��Ϣˢ����Buffer��������
	 */
	virtual void syncMataPages(Session *session);
	/**
	 * �����ҳ
	 * @return ��ҳ�ľ��
	 */
	BufferPageHdr* getHeaderPage() { return m_headerPage; }
#endif

	/**
	 * ��ȡ����ص��ļ�
	 * @param files OUT     �ļ��б�
	 * @param pageTypes     ҳ������
	 * @param numFile       �ļ���Ŀ
	 * @return              �������ļ�����
	 */
	virtual int getFiles(File **files, PageType* pageTypes, int numFile);

	/**
	 * ��ȡһ����¼�еĲ�������
	 * 
	 * @param session �Ự����
	 * @param rowId ��¼ID
	 * @param extractor ������ȡ�Ӽ�¼����ȡ��
	 * @param subRecord IN/OUT������ָ��Ҫ��ȡ�����ԣ����Ϊ��ȡ���������ݣ�����ΪREC_REDUNDANT��ʽ
	 * @param lockMode �Է��صļ�¼Ҫ�ӵ���ģʽ������ΪNone��������
	 * @param rlh OUT���Է��ؼ�¼����ʱ���ڴ洢�������
	 * @return ��¼�����ڷ���false�����򷵻�true
	 */
	virtual bool getSubRecord(Session *session, RowId rowId, SubrecExtractor *extractor, SubRecord *subRecord, LockMode lockMode = None, RowLockHandle **rlh = NULL) = 0;

	/**
	 * ��ȡһ�������ļ�¼
	 *
	 * @param session �Ự����
	 * @param rowId ��¼ID
	 * @param record OUT����¼���ݣ�������ΪREC_FIXLEN��ʽ���䳤��ΪREC_VARLEN��ʽ
	 * @param lockMode �Է��صļ�¼Ҫ�ӵ���ģʽ������ΪNone��������
	 * @param rlh OUT���Է��ؼ�¼����ʱ���ڴ洢�������
	 * @param duringRedo �Ƿ���REDO������
	 * @return ��¼�����ڷ���false�����򷵻�true
	 */
	virtual bool getRecord(Session *session, RowId rowId, Record *record,
		LockMode lockMode = None, RowLockHandle **rlh = NULL,
		bool duringRedo = false) = 0;

	/**
	 * ����һ����¼
	 * 
	 * @param session �Ự����
	 * @param record Ҫ����ļ�¼���ݣ�������ΪREC_FIXLEN��ʽ���䳤��ΪREC_VARLEN��ʽ
	 * @param rlh OUT�����������ΪNULL��ʾ��������
	 * @return �¼�¼��ID
	 */
	virtual RowId insert(Session *session, const Record *record, RowLockHandle **rlh) = 0;

	/**
	 * ����һ����¼
	 *
	 * @param session �Ự����
	 * @param rowId ��¼ID
	 * @param subRecord ���µļ�¼�Ӽ���ΪREC_REDUNDANT��ʽ
	 * @return Ҫ���µļ�¼�����ڷ���false�����򷵻�true
	 */
	virtual bool update(Session *session, RowId rowId, const SubRecord *subRecord) = 0;

	/**
	 * ����һ����¼
	 *
	 * @param session �Ự����
	 * @param rowId ��¼ID
	 * @param record ���µļ�¼��������ΪREC_FIXLEN��ʽ���䳤��ΪREC_VARLEN��ʽ
	 * @return Ҫ���µļ�¼�����ڷ���false�����³ɹ�����true
	 */
	virtual bool update(Session *session, RowId rowId, const Record *record) = 0;

	/**
	 * ɾ��һ����¼
	 * 
	 * @param session �Ự����
	 * @param rowId ��¼ID
	 * @return Ҫɾ���ļ�¼�����ڷ���false��ɾ���ɹ�����true
	 */
	virtual bool del(Session *session, RowId rowId) = 0;

	/**
	 * ��ʼ��ɨ��
	 *
	 * @param session �Ự
	 * @param extractor �ں���getNext��������ȡ�Ӽ�¼����ȡ��
	 * @param lockMode getNext���صļ�¼Ҫ�ӵ�������ΪNone�򲻼���
	 * @param rlh IN/OUT��getNext���صļ�¼�������������ΪNULL�򲻼���
	 * @param returnLinkSrc ���Ա䳤�������壬�����Ӽ�¼�Ƿ�������Դʱ����
	 * @return ��ɨ����
	 */
	virtual DrsHeapScanHandle* beginScan(Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc) = 0;

	/**
	 * ��ȡ��һ����¼
	 * @post ��¼�Ѿ�����beginScanʱָ������
	 * 
	 * @param scanHandle ɨ����
	 * @param subRec IN/OUT  ��Ҫ���صļ�¼�Ӽ������Ҳ�洢�����ΪREC_REDUNDANT��ʽ
	 * @return �Ƿ�λ��һ����¼������Ѿ�û�м�¼�ˣ�����false
	 */
	virtual bool getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRec) = 0;

	/**
	 * ���µ�ǰɨ��ļ�¼
	 *
	 * @param scanHandle ɨ����
	 * @param subRecord  ������µļ�¼�����Ӽ���ΪREC_REDUNDANT��ʽ
	 */
	virtual void updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord) = 0;

	/**
	 * ���µ�ǰɨ��ļ�¼��ֱ�ӿ���
	 * 
	 * @param scanHandle
	 * @param rcdDirectCopy ����ֱ�ӿ����ĸ��¼�¼
	 */
	virtual void updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy) = 0;

	/**
	 * ɾ����ǰɨ��ļ�¼
	 *
	 * @param scanHandle ɨ����
	 */
	virtual void deleteCurrent(DrsHeapScanHandle *scanHandle) = 0;

	/**
	 * ����һ�α�ɨ��
	 *
	 * @param scanHandle ɨ����
	 */
	virtual void endScan(DrsHeapScanHandle *scanHandle) = 0;

	/**
	 * ������������־
	 * @param db          ���ݿ�
	 * @param session     �Ự����
	 * @param path        ��·��
	 * @param tableDef    ���ݴ�TableDef��������
	 */
	static void redoCreate(Database *db, Session *session, const char *path, const TableDef *tableDef) throw(NtseException);

	/**
	 * ���ϻָ�ʱREDO��¼�������
	 *
	 * @param session �Ự����
	 * @param lsn ��־LSN
	 * @param log ��¼���������־����
	 * @param size ��־��С
	 * @param record OUT  ����������洢�ղ���ļ�¼���ݣ�����m_data�ռ����ϲ���䣬������ΪREC_FIXLEN��ʽ���䳤��ΪREC_VARLEN��ʽ
	 */
	virtual RowId redoInsert(Session *session, u64 lsn, const byte *log, uint size, Record *record) = 0;

	/**
	 * ���ϻָ�ʱREDO��¼���²���
	 *
	 * @param session �Ự����
	 * @param lsn ��־LSN
	 * @param log ��¼���²�����־����
	 * @param size ��־��С
	 * @param update �������Լ���ΪREC_REDUNDANT��ʽ
	 */
	virtual void redoUpdate(Session *session, u64 lsn, const byte *log, uint size, const SubRecord *update) = 0;

	/**
	 * ���ϻָ�ʱREDO��¼ɾ������
	 *
	 * @param session �Ự����
	 * @param lsn ��־LSN
	 * @param log ��¼ɾ��������־����
	 * @param size ��־��С
	 */
	virtual void redoDelete(Session *session, u64 lsn, const byte *log, uint size) = 0;

	/**
	 * ����������β����
	 * @param session    �Ự
	 */
	virtual void redoFinish(Session *session);

	/**
	 * �޸�ҳ��Ԥ���ռ�ٷֱ�
	 *
	 * @param session �Ự����
	 * @param pctFree �µ�ҳ��Ԥ���ռ�ٷֱ�
	 * @throw NtseException ָ���İٷֱȳ�����Χ���޷���֧��
	 */
	virtual void setPctFree(Session *session, u8 pctFree) throw(NtseException) = 0;

	/*** �����ӿ� ***/
	virtual SampleHandle *beginSample(Session *session, uint maxSampleNum, bool fastSample);
	virtual Sample * sampleNext(SampleHandle *handle);
	virtual void endSample(SampleHandle *handle);

	DBObjStats* getDBObjStats();

	/**
	 * ��öѵ�ͳ����Ϣ
	 * @return ��ͳ����Ϣ
	 */
	const HeapStatus& getStatus() {
		getDBObjStats();
		m_status.m_dataLength = (m_maxUsedPageNum + 1) * Limits::PAGE_SIZE;
		return m_status;
	}

	/**
	 * ���¶ѵ���չͳ����Ϣ
	 *
	 * @param session �Ự
	 * @param maxSamplePages �������������
	 */
	virtual void updateExtendStatus(Session *session, uint maxSamplePages) = 0;
	
	/**
	 * ��ȡ�ѵ���չͳ����Ϣ��ֻ�Ƿ���updateExtendStatus����õ���Ϣ�������²���ͳ�ƣ�
	 * @return �ѵ���չͳ����Ϣ
	 */
	const HeapStatusEx& getStatusEx() {
		return m_statusEx;
	}

	/**
	* ��öѰ汾���ַ�����ʾ
	* @param heapVersion ö������
	* @return C����ַ�������
	*/
	static const char* getVersionStr(HeapVersion heapVersion) {
		return (HEAP_VERSION_FLR == heapVersion) ? "HEAP_VERSION_FLR" : "HEAP_VERSION_VLR";
	}

	static HeapVersion getVersionFromTableDef(const TableDef *tableDef);
	static void getRecordFromInsertlog(LogEntry *log, Record *outRec);
	static RowId getRowIdFromInsLog(const LogEntry *inslog);
	u64 getUsedSize();

	//Ŀǰ�������ܲ���
	inline const TableDef* getTableDef() {
		return m_tableDef;
	}

#ifdef NTSE_UNIT_TEST
	void printInfo();
	virtual void printOtherInfo() {};
	File* getHeapFile() { return m_heapFile; }
	Buffer* getBuffer() { return m_buffer; }
	u64 getPageLSN(Session *session, u64 pageNum, DBObjStats *dbObjStats);
#endif
	u64 getMaxPageNum() { return m_maxPageNum;}
	u64 getMaxUsedPageNum() { return m_maxUsedPageNum;}
	/**
	 * �ж�һ��ҳ���Ƿ�empty
	 *
	 * @param session  �Ự
	 * @param pageNum  ҳ���
	 * @return  �շ���true���ǿշ���false
	 */
	virtual bool isPageEmpty(Session *session, u64 pageNum) = 0;

#ifdef TNT_ENGINE
	//Log��־
	//Insert
	static LsnType writeInsertTNTLog(Session *session, u16 tableId, TrxId txnId, LsnType preLsn, RowId rid);
	static void parseInsertTNTLog(const LogEntry *log, TrxId *txnId, LsnType *preLsn, RowId *rid);
#endif

	virtual void storePosAndInfo(DrsHeapScanHandle *scanHandle) = 0;
	virtual void restorePosAndInfo(DrsHeapScanHandle *scanHandle) = 0;

protected:
	 
	BufferPageHandle* lockHeaderPage(Session *session, LockMode lockMode);
	void unlockHeaderPage(Session *session, BufferPageHandle **handle);
	u16 extendHeapFile(Session *session, HeapHeaderPageInfo *headerPage);
	/**
	 * ��ʼ����չ��ҳ��
	 *
	 * @param session	�Ự
	 * @param size		ҳ������
	 */
	virtual void initExtendedNewPages(Session *session, uint size) = 0;
	/**
	 * ��չ���ļ��󣬽��и��ֶ����еĳ�ʼ������
	 *
	 * @param extendSize ��չ��ҳ��
	 */
	virtual void afterExtendHeap(uint extendSize) = 0;

	Sample *sample(Session *session, u64 pageNum);
	/**
	 * �ж�ҳ���Ƿ�������ڲ���
	 * @param pageNum      ҳ���
	 * @return             ���Բ�������true
	 */
	virtual bool isSamplable(u64 pageNum) = 0;
	/**
	 * ��һ������ҳ����
	 * @param session             �Ự
	 * @param page                ҳ��
	 * @return                    ����
	 */
	virtual Sample *sampleBufferPage(Session *session, BufferPageHdr *page) = 0;
	/**
	 * ѡ��һЩ���ڲ�����ҳ��
	 * @param outPages OUT          ���ҳ�������
	 * @param wantNum               ��ͼ����ҳ����
	 * @param min                   ������������ҳ���
	 * @param regionSize            ��������Ĵ�С
	 */
	virtual void selectPage(u64 *outPages, int wantNum, u64 min, u64 regionSize) = 0;
public:
	/** ����Ԫ����ҳ������� */
	virtual u64 metaDataPgCnt() = 0;
#ifdef NTSE_UNIT_TEST
public:
#else
protected:
#endif
	/**
	 * ��ȡһ�����������в��ɲ�����ҳ����
	 * @param downPgn           �������������ҳ��
	 * @param regionSize        ���������С
	 * @return                  ���ɲ���ҳ����
	 */
	virtual uint unsamplablePagesBetween(u64 downPgn, u64 regionSize) { 
		UNREFERENCED_PARAMETER(downPgn);
		UNREFERENCED_PARAMETER(regionSize);
		return 0;
	}


	Database	*m_db;				/** ���ݿ� */
	Buffer		*m_buffer;			/** ҳ�滺������� */
	const TableDef	*m_tableDef;		/** ���� */
	File		*m_heapFile;		/** ���ļ� */
	BufferPageHdr *m_headerPage;	/** ��ͷҳ��ҳ��ʼ�ձ�ʼ��pin���ڴ��� */
	HeapVersion m_version;			/** �����Ͱ汾 */
	u64			m_maxPageNum;		/** �ѵ����ҳ */
	u64			m_maxUsedPageNum;	/** �������ı��õ���ҳ��� */
	int 		m_pctFree;			/** �����ռ�ٷֱ� */
	HeapStatus	m_status;			/** ��ͳ����Ϣ */
	HeapStatusEx	m_statusEx;		/** �ѵĶ���ͳ����Ϣ */
	DBObjStats	*m_dboStats;		/** ���ݶ���״̬ */
	CmprssRecordExtractor* m_cprsRcdExtrator;/** ��¼ѹ�������� */
};

class SubrecExtractor;

/** DRS��ɨ���� */
class DrsHeapScanHandle{
public:
	DrsHeapScanHandle(DrsHeap *heap, Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **pRowLockHdl, void *info = NULL);
	~DrsHeapScanHandle();
	
	/**
	 * ȡ����ģʽ
	 * @return ��ģʽ
	 */
    inline LockMode getLockMode() {
        return m_lockMode;
    }

	/**
	 * �õ���ǰsession
	 * @return session
	 */
	inline Session* getSession() {
		return m_session;
	}

	/**
	 * ����������
	 * @return   �������
	 */
	inline RowLockHandle* getRowLockHandle() {
		return *m_pRowLockHandle;
	}

	/** 
	 * ��ȡ�Ӽ�¼��ȡ����
	 * @return    ��ȡ�ּ�¼��ȡ��
	 */
	inline SubrecExtractor* getExtractor() {
		return m_extractor;
	}

	/**
	* ��õ�ǰɨ�����ҳ�����
	* @return
	*/
	inline u64 getScanPageCount() const {
		return m_scanPagesCount;
	}
#ifndef TNT_ENGINE
private:
#endif
	/**
	 * ��ȡ���һ��ɨ��λ��
	 * @return ���ɨ��λ��
	 */
	inline RowId getNextPos() { return m_nextPos; }

	/**
	 * �������һ��ɨ��λ��
	 * @param rid    ���ɨ��λ��
	 */
	inline void setNextPos(RowId rid) { m_nextPos = rid; }

	/**
	 * ��ø�����Ϣ
	 * 
	 * @return ����ָ��
	 */
	inline void *getOtherInfo() {
		return m_info;
	}

	/**
	 * �����������
	 *
	 * @param �������
	 */
	inline void setRowLockHandle(RowLockHandle *rowLockHandle) {
		*m_pRowLockHandle = rowLockHandle;
	}

	/**
	 * ��ȡ��ǰҳ
	 *
	 * @return ��ǰҳ
	 */
	inline BufferPageHandle* getPage() {
		return m_pageHdl;
	}

	/** 
	 * ���õ�ǰҳ
	 *
	 * @param page ��ǰҳ
	 */
	inline void setPage(BufferPageHandle *pageHdl) {
		m_pageHdl = pageHdl;
		m_scanPagesCount++;
	}

#ifdef TNT_ENGINE
private:
#endif
	RowId		      m_nextPos;		/** �ϴ�ɨ�赽��λ��
								         * �����¼����Ϊ�䳤����SubRecord��RowIdδ�������λ�á�
								         */
	LockMode	      m_lockMode;		/** ��ģʽ */
	Session		      *m_session;		/** �Ự���� */
	DrsHeap		      *m_heap;		    /** �Ѷ��� */
	BufferPageHandle  *m_pageHdl;       /** ��ǰҳ��� */
	RowLockHandle	  **m_pRowLockHandle;/** �������ָ�� */
	void		      *m_info;		    /** ����������Ϣ�������� */ 
	SubrecExtractor   *m_extractor;	    /** �Ӽ�¼��ȡ���� */
	u64               m_scanPagesCount;  /** ��ǰɨ�����ҳ����� */

	RowId			  m_prevNextPos;	/** ����֮ǰ��ɨ�赽��λ����������ɨ�� */
	u64				  m_prevNextBmpNumForVarHeap;	/** ����䳤��֮ǰ����һ��λͼҳ����Ϣ��������ɨ�� */
	friend class FixedLengthRecordHeap;
	friend class VariableLengthRecordHeap;
};


class HeapSampleHandle : public SampleHandle {
public:
	HeapSampleHandle(Session *session, uint maxSampleNum, bool fastSample)
		: SampleHandle(session, maxSampleNum, fastSample) , m_blockPages(NULL), m_bufScanHdl(NULL) {}
private:
	/* ���±�����Ϊ�˴��̲��� */
	u64 m_minPage, m_maxPage, m_regionSize;
	int m_blockNum, m_curBlock;
	int m_blockSize, m_curIdxInBlock;
	u64 *m_blockPages;
	/* ���±�����Ϊ�˻��������� */
	BufScanHandle *m_bufScanHdl;

	friend class DrsHeap;
};


}
#endif

