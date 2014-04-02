/**
 * 堆文件管理
 *
 * @author 谢可(xieke@corp.netease.com, ken@163.org)
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

/** 各种不同的堆类型 */
enum HeapVersion {
	HEAP_VERSION_FLR = 0,
	HEAP_VERSION_VLR = 1,
}; 

struct DBObjStats;
/** 堆的统计信息 */
struct HeapStatus {
	DBObjStats	*m_dboStats;		/** 所有数据库对象共有的统计信息 */
	u64			m_dataLength;		/** 占用的数据大小，只包含已使用的页面，单位字节数 */
	u64 		m_rowsReadSubRec;	/** 记录读取SubRecord的次数 */
	u64 		m_rowsReadRecord;	/** 记录读取Record的次数 */
	u64 		m_rowsUpdateSubRec;	/** 记录被SubRecord更新的次数 */
	u64 		m_rowsUpdateRecord;	/** 记录被Record更新的次数 */
};

/** 堆模块扩展统计信息（通过采样获得，不精确） */
struct HeapStatusEx {
	u64		m_numRows;		/** 堆中记录数 */
	u64		m_numCmprsRows; /** 堆中压缩的记录数 */
	u64		m_numLinks;		/** 链接记录数，只对变长堆有意义 */
	double	m_pctUsed;		/** 页面利用率 */
	double  m_cmprsRatio;   /** 压缩比 */
};

/** 堆首页公共信息结构 */
struct HeapHeaderPageInfo {
	BufferPageHdr	m_bph;		/** 缓存页共用头结构 */
	u64				m_pageNum;	/** 堆中包含数据页数，不包含首页 */
	u64				m_maxUsed;	/** 堆中最大使用到的页面号 */     
	u8				m_version;	/** 堆类型版本 */
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
/** 各种具体的堆文件实现的基类 */
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
	 * 设置记录压缩管理器
	 */
	inline void setCompressRcdExtrator(CmprssRecordExtractor* cprsRcdExtrator) {
		m_cprsRcdExtrator = cprsRcdExtrator;
	}

	inline CmprssRecordExtractor* getCompressRcdExtrator() const {
		return m_cprsRcdExtrator;
	}
#ifdef NTSE_UNIT_TEST
	/**
	 * 将页面内必要信息刷出到Buffer，测试用
	 */
	virtual void syncMataPages(Session *session);
	/**
	 * 获得首页
	 * @return 首页的句柄
	 */
	BufferPageHdr* getHeaderPage() { return m_headerPage; }
#endif

	/**
	 * 获取表相关的文件
	 * @param files OUT     文件列表
	 * @param pageTypes     页面类型
	 * @param numFile       文件数目
	 * @return              传出的文件个数
	 */
	virtual int getFiles(File **files, PageType* pageTypes, int numFile);

	/**
	 * 读取一条记录中的部分属性
	 * 
	 * @param session 会话对象
	 * @param rowId 记录ID
	 * @param extractor 用于提取子记录的提取器
	 * @param subRecord IN/OUT，输入指定要读取的属性，输出为读取的属性内容，必须为REC_REDUNDANT格式
	 * @param lockMode 对返回的记录要加的锁模式，可能为None即不加锁
	 * @param rlh OUT，对返回记录加锁时用于存储行锁句柄
	 * @return 记录不存在返回false，否则返回true
	 */
	virtual bool getSubRecord(Session *session, RowId rowId, SubrecExtractor *extractor, SubRecord *subRecord, LockMode lockMode = None, RowLockHandle **rlh = NULL) = 0;

	/**
	 * 读取一条完整的记录
	 *
	 * @param session 会话对象
	 * @param rowId 记录ID
	 * @param record OUT，记录内容，定长堆为REC_FIXLEN格式，变长堆为REC_VARLEN格式
	 * @param lockMode 对返回的记录要加的锁模式，可能为None即不加锁
	 * @param rlh OUT，对返回记录加锁时用于存储行锁句柄
	 * @param duringRedo 是否在REDO过程中
	 * @return 记录不存在返回false，否则返回true
	 */
	virtual bool getRecord(Session *session, RowId rowId, Record *record,
		LockMode lockMode = None, RowLockHandle **rlh = NULL,
		bool duringRedo = false) = 0;

	/**
	 * 插入一条记录
	 * 
	 * @param session 会话对象
	 * @param record 要插入的记录内容，定长堆为REC_FIXLEN格式，变长堆为REC_VARLEN格式
	 * @param rlh OUT，行锁句柄，为NULL表示不加行锁
	 * @return 新记录的ID
	 */
	virtual RowId insert(Session *session, const Record *record, RowLockHandle **rlh) = 0;

	/**
	 * 更新一条记录
	 *
	 * @param session 会话对象
	 * @param rowId 记录ID
	 * @param subRecord 更新的记录子集，为REC_REDUNDANT格式
	 * @return 要更新的记录不存在返回false，否则返回true
	 */
	virtual bool update(Session *session, RowId rowId, const SubRecord *subRecord) = 0;

	/**
	 * 更新一条记录
	 *
	 * @param session 会话对象
	 * @param rowId 记录ID
	 * @param record 更新的记录。定长堆为REC_FIXLEN格式，变长堆为REC_VARLEN格式
	 * @return 要更新的记录不存在返回false，更新成功返回true
	 */
	virtual bool update(Session *session, RowId rowId, const Record *record) = 0;

	/**
	 * 删除一条记录
	 * 
	 * @param session 会话对象
	 * @param rowId 记录ID
	 * @return 要删除的记录不存在返回false，删除成功返回true
	 */
	virtual bool del(Session *session, RowId rowId) = 0;

	/**
	 * 开始表扫描
	 *
	 * @param session 会话
	 * @param extractor 在后续getNext中用于提取子记录的提取器
	 * @param lockMode getNext返回的记录要加的锁，若为None则不加锁
	 * @param rlh IN/OUT，getNext返回的记录的行锁句柄，若为NULL则不加锁
	 * @param returnLinkSrc 仅对变长堆有意义，对链接记录是否在遇到源时返回
	 * @return 表扫描句柄
	 */
	virtual DrsHeapScanHandle* beginScan(Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **rlh, bool returnLinkSrc) = 0;

	/**
	 * 读取下一条记录
	 * @post 记录已经加上beginScan时指定的锁
	 * 
	 * @param scanHandle 扫描句柄
	 * @param subRec IN/OUT  需要返回的记录子集，结果也存储在这里，为REC_REDUNDANT格式
	 * @return 是否定位到一条记录，如果已经没有记录了，返回false
	 */
	virtual bool getNext(DrsHeapScanHandle *scanHandle, SubRecord *subRec) = 0;

	/**
	 * 更新当前扫描的记录
	 *
	 * @param scanHandle 扫描句柄
	 * @param subRecord  所需更新的记录属性子集，为REC_REDUNDANT格式
	 */
	virtual void updateCurrent(DrsHeapScanHandle *scanHandle, const SubRecord *subRecord) = 0;

	/**
	 * 更新当前扫描的记录，直接拷贝
	 * 
	 * @param scanHandle
	 * @param rcdDirectCopy 可以直接拷贝的更新记录
	 */
	virtual void updateCurrent(DrsHeapScanHandle *scanHandle, const Record *rcdDirectCopy) = 0;

	/**
	 * 删除当前扫描的记录
	 *
	 * @param scanHandle 扫描句柄
	 */
	virtual void deleteCurrent(DrsHeapScanHandle *scanHandle) = 0;

	/**
	 * 结束一次表扫描
	 *
	 * @param scanHandle 扫描句柄
	 */
	virtual void endScan(DrsHeapScanHandle *scanHandle) = 0;

	/**
	 * 重做创建堆日志
	 * @param db          数据库
	 * @param session     会话对象
	 * @param path        堆路径
	 * @param tableDef    依据此TableDef来创建堆
	 */
	static void redoCreate(Database *db, Session *session, const char *path, const TableDef *tableDef) throw(NtseException);

	/**
	 * 故障恢复时REDO记录插入操作
	 *
	 * @param session 会话对象
	 * @param lsn 日志LSN
	 * @param log 记录插入操作日志内容
	 * @param size 日志大小
	 * @param record OUT  输出参数，存储刚插入的记录内容，其中m_data空间由上层分配，定长堆为REC_FIXLEN格式，变长堆为REC_VARLEN格式
	 */
	virtual RowId redoInsert(Session *session, u64 lsn, const byte *log, uint size, Record *record) = 0;

	/**
	 * 故障恢复时REDO记录更新操作
	 *
	 * @param session 会话对象
	 * @param lsn 日志LSN
	 * @param log 记录更新操作日志内容
	 * @param size 日志大小
	 * @param update 更新属性集，为REC_REDUNDANT格式
	 */
	virtual void redoUpdate(Session *session, u64 lsn, const byte *log, uint size, const SubRecord *update) = 0;

	/**
	 * 故障恢复时REDO记录删除操作
	 *
	 * @param session 会话对象
	 * @param lsn 日志LSN
	 * @param log 记录删除操作日志内容
	 * @param size 日志大小
	 */
	virtual void redoDelete(Session *session, u64 lsn, const byte *log, uint size) = 0;

	/**
	 * 做重做的收尾工作
	 * @param session    会话
	 */
	virtual void redoFinish(Session *session);

	/**
	 * 修改页面预留空间百分比
	 *
	 * @param session 会话对象
	 * @param pctFree 新的页面预留空间百分比
	 * @throw NtseException 指定的百分比超出范围或无法被支持
	 */
	virtual void setPctFree(Session *session, u8 pctFree) throw(NtseException) = 0;

	/*** 采样接口 ***/
	virtual SampleHandle *beginSample(Session *session, uint maxSampleNum, bool fastSample);
	virtual Sample * sampleNext(SampleHandle *handle);
	virtual void endSample(SampleHandle *handle);

	DBObjStats* getDBObjStats();

	/**
	 * 获得堆的统计信息
	 * @return 堆统计信息
	 */
	const HeapStatus& getStatus() {
		getDBObjStats();
		m_status.m_dataLength = (m_maxUsedPageNum + 1) * Limits::PAGE_SIZE;
		return m_status;
	}

	/**
	 * 更新堆的扩展统计信息
	 *
	 * @param session 会话
	 * @param maxSamplePages 采样的最大数据
	 */
	virtual void updateExtendStatus(Session *session, uint maxSamplePages) = 0;
	
	/**
	 * 获取堆的扩展统计信息（只是返回updateExtendStatus计算好的信息，不重新采样统计）
	 * @return 堆的扩展统计信息
	 */
	const HeapStatusEx& getStatusEx() {
		return m_statusEx;
	}

	/**
	* 获得堆版本的字符串表示
	* @param heapVersion 枚举类型
	* @return C风格字符串类型
	*/
	static const char* getVersionStr(HeapVersion heapVersion) {
		return (HEAP_VERSION_FLR == heapVersion) ? "HEAP_VERSION_FLR" : "HEAP_VERSION_VLR";
	}

	static HeapVersion getVersionFromTableDef(const TableDef *tableDef);
	static void getRecordFromInsertlog(LogEntry *log, Record *outRec);
	static RowId getRowIdFromInsLog(const LogEntry *inslog);
	u64 getUsedSize();

	//目前用于性能测试
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
	 * 判断一个页面是否empty
	 *
	 * @param session  会话
	 * @param pageNum  页面号
	 * @return  空返回true，非空返回false
	 */
	virtual bool isPageEmpty(Session *session, u64 pageNum) = 0;

#ifdef TNT_ENGINE
	//Log日志
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
	 * 初始化扩展的页面
	 *
	 * @param session	会话
	 * @param size		页面数量
	 */
	virtual void initExtendedNewPages(Session *session, uint size) = 0;
	/**
	 * 扩展堆文件后，进行各种堆特有的初始化动作
	 *
	 * @param extendSize 扩展的页数
	 */
	virtual void afterExtendHeap(uint extendSize) = 0;

	Sample *sample(Session *session, u64 pageNum);
	/**
	 * 判断页面是否可以用于采样
	 * @param pageNum      页面号
	 * @return             可以采样返回true
	 */
	virtual bool isSamplable(u64 pageNum) = 0;
	/**
	 * 对一个缓存页采样
	 * @param session             会话
	 * @param page                页面
	 * @return                    样本
	 */
	virtual Sample *sampleBufferPage(Session *session, BufferPageHdr *page) = 0;
	/**
	 * 选择一些用于采样的页面
	 * @param outPages OUT          输出页面号数组
	 * @param wantNum               意图采样页面数
	 * @param min                   采样区域最下页面号
	 * @param regionSize            采样区域的大小
	 */
	virtual void selectPage(u64 *outPages, int wantNum, u64 min, u64 regionSize) = 0;
public:
	/** 返回元数据页面的数量 */
	virtual u64 metaDataPgCnt() = 0;
#ifdef NTSE_UNIT_TEST
public:
#else
protected:
#endif
	/**
	 * 获取一个采样区域中不可采样的页面数
	 * @param downPgn           采样区域的下限页面
	 * @param regionSize        采样区域大小
	 * @return                  不可采样页面数
	 */
	virtual uint unsamplablePagesBetween(u64 downPgn, u64 regionSize) { 
		UNREFERENCED_PARAMETER(downPgn);
		UNREFERENCED_PARAMETER(regionSize);
		return 0;
	}


	Database	*m_db;				/** 数据库 */
	Buffer		*m_buffer;			/** 页面缓存管理器 */
	const TableDef	*m_tableDef;		/** 表定义 */
	File		*m_heapFile;		/** 堆文件 */
	BufferPageHdr *m_headerPage;	/** 堆头页，页面始终被始终pin在内存中 */
	HeapVersion m_version;			/** 堆类型版本 */
	u64			m_maxPageNum;		/** 堆的最大页 */
	u64			m_maxUsedPageNum;	/** 堆中最大的被用到的页面号 */
	int 		m_pctFree;			/** 保留空间百分比 */
	HeapStatus	m_status;			/** 堆统计信息 */
	HeapStatusEx	m_statusEx;		/** 堆的额外统计信息 */
	DBObjStats	*m_dboStats;		/** 数据对象状态 */
	CmprssRecordExtractor* m_cprsRcdExtrator;/** 记录压缩管理器 */
};

class SubrecExtractor;

/** DRS堆扫描句柄 */
class DrsHeapScanHandle{
public:
	DrsHeapScanHandle(DrsHeap *heap, Session *session, SubrecExtractor *extractor, LockMode lockMode, RowLockHandle **pRowLockHdl, void *info = NULL);
	~DrsHeapScanHandle();
	
	/**
	 * 取得锁模式
	 * @return 锁模式
	 */
    inline LockMode getLockMode() {
        return m_lockMode;
    }

	/**
	 * 得到当前session
	 * @return session
	 */
	inline Session* getSession() {
		return m_session;
	}

	/**
	 * 获得行锁句柄
	 * @return   行锁句柄
	 */
	inline RowLockHandle* getRowLockHandle() {
		return *m_pRowLockHandle;
	}

	/** 
	 * 获取子记录提取策略
	 * @return    获取字记录提取器
	 */
	inline SubrecExtractor* getExtractor() {
		return m_extractor;
	}

	/**
	* 获得当前扫描过的页面计数
	* @return
	*/
	inline u64 getScanPageCount() const {
		return m_scanPagesCount;
	}
#ifndef TNT_ENGINE
private:
#endif
	/**
	 * 获取最后一次扫描位置
	 * @return 最后扫描位置
	 */
	inline RowId getNextPos() { return m_nextPos; }

	/**
	 * 设置最后一次扫描位置
	 * @param rid    最后扫描位置
	 */
	inline void setNextPos(RowId rid) { m_nextPos = rid; }

	/**
	 * 获得附加信息
	 * 
	 * @return 返回指针
	 */
	inline void *getOtherInfo() {
		return m_info;
	}

	/**
	 * 设置行锁句柄
	 *
	 * @param 行锁句柄
	 */
	inline void setRowLockHandle(RowLockHandle *rowLockHandle) {
		*m_pRowLockHandle = rowLockHandle;
	}

	/**
	 * 获取当前页
	 *
	 * @return 当前页
	 */
	inline BufferPageHandle* getPage() {
		return m_pageHdl;
	}

	/** 
	 * 设置当前页
	 *
	 * @param page 当前页
	 */
	inline void setPage(BufferPageHandle *pageHdl) {
		m_pageHdl = pageHdl;
		m_scanPagesCount++;
	}

#ifdef TNT_ENGINE
private:
#endif
	RowId		      m_nextPos;		/** 上次扫描到的位置
								         * 必须记录，因为变长堆中SubRecord的RowId未必是这个位置。
								         */
	LockMode	      m_lockMode;		/** 锁模式 */
	Session		      *m_session;		/** 会话对象 */
	DrsHeap		      *m_heap;		    /** 堆对象 */
	BufferPageHandle  *m_pageHdl;       /** 当前页句柄 */
	RowLockHandle	  **m_pRowLockHandle;/** 行锁句柄指针 */
	void		      *m_info;		    /** 其他补充信息，是引用 */ 
	SubrecExtractor   *m_extractor;	    /** 子记录提取策略 */
	u64               m_scanPagesCount;  /** 当前扫描过的页面计数 */

	RowId			  m_prevNextPos;	/** 保存之前的扫描到的位置用于重启扫描 */
	u64				  m_prevNextBmpNumForVarHeap;	/** 保存变长堆之前的下一个位图页号信息用于重启扫描 */
	friend class FixedLengthRecordHeap;
	friend class VariableLengthRecordHeap;
};


class HeapSampleHandle : public SampleHandle {
public:
	HeapSampleHandle(Session *session, uint maxSampleNum, bool fastSample)
		: SampleHandle(session, maxSampleNum, fastSample) , m_blockPages(NULL), m_bufScanHdl(NULL) {}
private:
	/* 以下变量是为了磁盘采样 */
	u64 m_minPage, m_maxPage, m_regionSize;
	int m_blockNum, m_curBlock;
	int m_blockSize, m_curIdxInBlock;
	u64 *m_blockPages;
	/* 以下变量是为了缓存区采样 */
	BufScanHandle *m_bufScanHdl;

	friend class DrsHeap;
};


}
#endif

