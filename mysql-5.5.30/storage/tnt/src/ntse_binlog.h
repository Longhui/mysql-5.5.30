/**
 * NTSE记录binlog相关的内容定义
 * @author 苏斌(naturally@163.org)
 */

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#ifndef _NTSE_BINLOG_H_
#define _NTSE_BINLOG_H_

#include "misc/Callbacks.h"
#include "misc/Global.h"
#include "misc/Session.h"
#include "misc/Record.h"
#include "util/Sync.h"
#include <map>
#include <vector>

using namespace ntse;

/**
 * 定义了插入、删除和更新三种类型的Binlog
 */
enum BinlogType {
	BINLOG_INSERT = 0,
	BINLOG_DELETE,
	BINLOG_UPDATE
};

/** 表示一条binlog相关的信息内容，所有记录的格式都需要使用REC_MYSQL格式
 *	序列化结构和内容参见.cpp类实现的说明
 */
class BinlogInfo {
public:
	/** 要写binlog的事务参数信息
	 */
	struct TxnInfo {
		u64	m_sqlMode;	  /** 当前事务的SQL_MODE信息 */
		u64 m_serverId;  /** 执行事务的服务器ID */  
	};
public:
	size_t getSerializeSize(const TableDef *tableDef) const;
	void serialize(const TableDef *tableDef, byte *buffer, u32 bufferSize);
	void unserialize(MemoryContext *mc, byte *buffer, u32 bufferSize);
	bool isBinlogValid() const;

	TableDef			*m_tableDef;		/** 日志所属表的ID */
	TxnInfo				m_txnInfo;			/** 日志所属事务的信息 */
	SubRecord			m_beforeSR;			/** 前项记录内容 */
	SubRecord			m_afterSR;			/** 后项记录内容 */
	BinlogType			m_type;				/** binlog类型 */
};


/** 用于读写binlog的缓存结构，该缓存会实际采用一个MemoryContext来存储binlog信息
 * 当空间不够，会自动向后扩展，为了方便空间管理，该缓存不支持单个binlog的删除，只支持全部删除
 * 由于需要支持将缓存的binlog按照其对应的表排序，因此在缓存内部会维护一个vector容器保存相关内容，便于排序
 * 该缓存不是线程安全的
 */
class BinlogBuffer {
private:
	struct ExtendBinlogInfo {
		ExtendBinlogInfo(u16 tableId, void *address, size_t size) : m_address(address), m_tableId(tableId), m_binlogInfoSize(size) {}
		void *m_address;		/** binlogInfo对应的起始地址 */
		u16 m_tableId;			/** binlogInfo所对应表的ID */
		size_t m_binlogInfoSize;/** binlogInfo信息长度 */

		bool operator < (const ExtendBinlogInfo &other) const {
			return m_tableId < other.m_tableId;
		}
	};

public:
	BinlogBuffer(size_t bufferSize = DEFAULT_BUFFER_SIZE);
	~BinlogBuffer();

	bool isBufferFull();
	void append(const TableDef *tableDef, BinlogInfo *binlogInfo);
	bool getAt(MemoryContext *mc, uint pos, BinlogInfo *binlogInfo);
	size_t getBinlogSize(uint pos);
	size_t getBinlogCount() const;
	size_t getBufferSize() const;
	bool isEmpty() const;
	void sortByTableId();
	bool containTable(u16 tableId);
	void clear();
	bool isNearlyFull();
	void waitForClean();

private:
	Event m_bufferCleanEvent;				/** 用来等待缓存为空的信号量 */
	MemoryContext *m_buffer;				/** 用内存分配上下文作为实际缓存 */
	u64 m_bufferInitSize;					/** 缓存的起始大小 */
	vector<ExtendBinlogInfo> m_extendInfo;	/** 用于保存各个Binlog内存地址和对应TableId对的容器 */
	size_t	m_size;							/** 缓存已经使用的大小 */

	static const double BUFFER_FULL_RATIO;							/** 当前缓存使用量和已申请的内存总量之比阈值，超过这个阈值表明缓存很快就会写满 */
	static const u32 DEFAULT_RESERVE_EXTENDBINLOGINFO_SIZE = 5000;	/** 默认预留的扩展binlog信息数量 */

public:
	static const u32 DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;			/** 默认缓存为8M */
};


/**
 * 统计binlog缓存管理器使用状况
 */
struct BinlogBufferStatus {
	u64 m_switchTimes;			/** 读写缓存切换次数 */
	u64 m_writeTimes;			/** 写入缓存的binlog数 */
	u64 m_readTimes;			/** 从读缓存读取的次数，理论上应该等同于m_switchTimes */
	u64 m_readBufferFullRatio;	/** 读缓存饱满率，等同于写缓存饱满率 */
	u64 m_pollWaitTime;			/** POLL过程中等待的时间总和，单位毫秒 */
	u64 m_writeBufferSize;		/** 当前写缓存包含的日志大小 */
	u64 m_writeBufferCount;		/** 当前写缓存包含的日志条数 */
	u64 m_readBufferSize;		/** 当前读缓存包含的日志大小 */
	u64 m_readBufferUnflushSize;/** 当前读缓存未刷新出去的日志大小 */
	u64 m_readBufferUnflushCount;/** 当前读缓存未刷出去日志的条数 */
};


/**
 * 该类的功能是管理两个具体的BinlogBuffer类，每次使用一个缓存进行读，一个缓存进行写
 * 双缓存主要是能够读写不冲突，同时管理器维护着在写线程写满某个缓存的时候，如果读缓存此时空闲，
 * 需要及时的将写缓存和读缓存对换角色，让读对象有数据可读，而写对象可以不需要经过缓存扩展继续写入
 *
 * 该类会需要同步多线程对缓存的写，但是不同步读
 * 缓存只能读一次，不支持多次读取
 * 要读取之前需要使用pollBinlogInfo函数轮询保证当前缓存有足够数据可读
 * 接着需要按照beginReadBinlogInfo－readNextBinlogInfo-endReadBinlogInfo的顺序执行
 */
class BinlogBufferManager {
public:
	BinlogBufferManager(size_t bufferSize = 2 * BinlogBuffer::DEFAULT_BUFFER_SIZE);
	~BinlogBufferManager();

	void writeBinlogInfo(const TableDef *tableDef, BinlogInfo *binlogInfo);
	bool pollBinlogInfo(int timeoutMs = -1);
	void beginReadBinlogInfo();
	bool readNextBinlogInfo(MemoryContext *mc, BinlogInfo *binlogInfo);
	void endReadBinlogInfo();
	size_t getBufferTotalSize();
	void flushAll();
	void flush(u16 tableId);
	struct BinlogBufferStatus getStatus() { return m_status; }
	bool containTable(u16 tableId);

private:
	void switchBuffersIfNecessary(bool force = false);
	
	ntse::Mutex m_mutex;						/** 用来保护写入缓存线程的互斥锁 */
	Event m_switchEvent;				/** 当读缓存为空或者写缓存不够多的时候，用来等待的信号量 */
	Event m_fullEvent;					/** 当写缓存满的时候，用来等待的信号量 */
	BinlogBuffer *m_readBuffer;			/** 该缓存用于读 */
	ntse::Mutex m_readMutex;					/** 用来保护读缓存 */
	BinlogBuffer *m_writeBuffer;		/** 该缓存用于写 */
	size_t m_pos;						/** 读操作读取信息的项数下标 */
	struct BinlogBufferStatus m_status;	/** 统计信息 */
};

class THD;
struct st_table;
struct st_bitmap;
class injector;

struct BinlogWriterStatus {
	u64 m_totalWrites;				/** 总共写出了多少binlog */
	u64 m_transNum;					/** 使用了多少个事务 */
	u64 m_maxTransLogs;				/** 一个事务当中最多写出了多少连续的binlog */
	u64 m_minTransLogs;				/** 一个事务当中最少写出了多少连续的binlog */
	u64 m_insertLogs;				/** 插入事务的日志数 */
	u64 m_deleteLogs;				/** 删除事务的日志数 */
	u64 m_updateLogs;				/** 更新事务的日志数 */

	void onTransCommit(u64 transLogs) {
		if (transLogs > m_maxTransLogs)
			m_maxTransLogs = transLogs;
		if (transLogs != 0)	{ // 日志数不为零表示有效提交
			if (transLogs < m_minTransLogs || m_minTransLogs == 0)
				m_minTransLogs = transLogs;
			m_transNum++;
		}
		m_totalWrites += transLogs;
	}

	void onWrittenBinlog(BinlogInfo *binlogInfo) {
		if (binlogInfo->m_type == BINLOG_INSERT)
			m_insertLogs++;
		else if (binlogInfo->m_type == BINLOG_DELETE)
			m_deleteLogs++;
		else
			m_updateLogs++;
	}
};

/** binlog真正写日志基类定义
 */
class BinlogWriter : public ntse::Thread {
public:
	BinlogWriter();
	virtual ~BinlogWriter();

	virtual void run() = 0;

	void setStop() { m_running = false; }
	struct BinlogWriterStatus getStatus() { return m_status; }

protected:
	bool createTHD();
	void destroyTHD();

	void startTransaction(void *trans, TABLE *table, BinlogInfo::TxnInfo *thdInfo);
	bool commitTransaction(void *trans);

	void initBITMAP(struct st_bitmap *bitmap, u16 numCols, u16 *cols);
	void writeBinlog(void *trans, TABLE *table, BinlogInfo *binlogInfo);

protected:
	THD *m_thd;			/** 所需要的线程对象 */
	injector *m_inj;	/** 写日志需要的injector对象 */
	bool m_running;		/** 标识binlog写线程是否需要继续 */

	struct BinlogWriterStatus m_status;	/** 状态统计信息 */
};

class ha_ntse;

typedef map<CallbackType, NTSECallbackFN*>::iterator cbItor;
/**
 * NTSE实现写Binlog日志类接口定义
 */
class NTSEBinlog {
public:
	virtual ~NTSEBinlog() {}

	virtual void registerNTSECallbacks(Database *ntsedb);
	virtual void unregisterNTSECallbacks(Database *ntsedb);
	
	/** 
	 * 刷写出到指定表的binlog信息
	 * @param tableId 表id
	 */
	virtual void flushBinlog(u16 tableId) = 0;

	/** 得到binlog刷写线程的统计信息
	 * @return 返回统计信息
	 */
	virtual struct BinlogWriterStatus getBinlogWriterStatus() = 0;

	/** 得到binlog缓存统计信息
	 * @return 返回统计信息
	 */
	virtual struct BinlogBufferStatus getBinlogBufferStatus() = 0;

	virtual const char* getBinlogMethod() const = 0;

protected:
	/** 初始化回调函数对象
	 */
	virtual void initCallbacks() = 0;

	/** 销毁回调函数对象
	 */
	virtual void destroyCallbacks();

	static bool needBinlog(ha_ntse *handler);

protected:
	map<CallbackType, NTSECallbackFN*>	m_callbacks;	/** 回调对象 */
};

/**
 * 使用持有锁直接写binlog的实现
 */
class NTSEDirectBinlog : public NTSEBinlog {
public:
	static NTSEDirectBinlog* getNTSEBinlog();
	static void freeNTSEBinlog();
	~NTSEDirectBinlog();
	void flushBinlog(u16 tableId);

	struct BinlogWriterStatus getBinlogWriterStatus();
	struct BinlogBufferStatus getBinlogBufferStatus();

	const char* getBinlogMethod() const;

private:
	NTSEDirectBinlog();
	void initCallbacks();

	// 该实现为不缓存直接写binlog,不需要处理onCloseTable和onAlterTable事件
	//static void onCloseTable(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	//static void onAlterTable(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowInsert(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowDelete(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowUpdate(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);

	static void writeBinlog(const TableDef *tableDef, BinlogType type, ha_ntse *handler, const SubRecord *brec, const SubRecord *arec);

private:

	/** 该类只能有一个实例，只支持同步访问
	 */
	class DirectBinlogWriter : public BinlogWriter {
	public:
		DirectBinlogWriter();
		~DirectBinlogWriter() { destroyTHD(); }
		void run() {}
		void syncWrite(BinlogInfo *binlogInfo, TABLE *table);

	private:
		void writeInTxn();

	private:
		Event m_waitEvent;				/** binlog等待写对象信号量 */
		Event m_writtenEvent;			/** binlog等待写操作结束信号量 */

		BinlogInfo *m_binlogInfo;		/** 要写的BinlogInfo对象 */
		TABLE *m_table;					/** 要写binlog的table对象 */

		static DirectBinlogWriter *m_instance;		/** THD管理对象单例 */
	};

private:
	NTSEDirectBinlog::DirectBinlogWriter *m_writer;	/** binlog写对象 */

	static ntse::Mutex *m_mutex;						/** 同步访问THDManager对象的互斥锁 */

	static NTSEDirectBinlog *m_instance;		/** 单件模式实例对象 */

	static struct BinlogBufferStatus m_bufferStatus;	/** 缓存状态统计信息 */
};

/**
 * 支持缓存的Binlog实现
 */
class NTSECachableBinlog : public NTSEBinlog {
public:
	static NTSECachableBinlog* getNTSEBinlog(size_t bufferSize);
	static void freeNTSEBinlog();
	~NTSECachableBinlog();
	void flushBinlog(u16 tableId);

	struct BinlogWriterStatus getBinlogWriterStatus();
	struct BinlogBufferStatus getBinlogBufferStatus();

	const char* getBinlogMethod() const;

private:

	/** 采用Cache方式写binlog的类
	 */
	class CachedBinlogWriter : public BinlogWriter {
	public:
		CachedBinlogWriter(BinlogBufferManager *blBuffer);
		~CachedBinlogWriter();
		void run();

	private:
		bool isTableSwitched(u16 curTblId);

		u16 m_lastTableId;					/** 上一次写binlog的表ID，一开始为-1 */

		BinlogBufferManager *m_blBuffer;	/** 读取线程使用的缓存管理 */

		static const u32 DEFAULT_MAX_UNCOMMITTED_BINLOGS = 1000;	/** 默认一个事务最大的未提交binlog数，超过这个阈值必须至少提交一次 */
	};

private:
	NTSECachableBinlog(size_t bufferSize);
	void initCallbacks();
	void init(size_t bufferSize);
	void destroy();

	static void onCloseTable(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void onAlterTable(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowInsert(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowDelete(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowUpdate(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void writeBuffer(BinlogType type, THD *thd, const TableDef *tableDef, u16 bNumCols, u16 *bColumns, u16 bSize, byte *bData, u16 aNumCols, u16 *aColumns, u16 aSize, byte *aData);

private:
	CachedBinlogWriter *m_writer;						/** 刷写binlog线程对象 */
	
	static NTSECachableBinlog *m_instance;		/** 单件模式实例对象 */
	static BinlogBufferManager *m_blBuffer;		/** 写binlog使用的缓存 */
};

/**
 * 负责分配释放Binlog对象的工厂类
 */
class NTSEBinlogFactory {
public:
	static NTSEBinlogFactory* getInstance();
	static void freeInstance();

	NTSEBinlog* getNTSEBinlog(const char* method, size_t binlogBufferSize = 0);
	void freeNTSEBinlog(NTSEBinlog **ntseBinlog);

private:
	NTSEBinlogFactory() {}

private:
	static NTSEBinlogFactory *m_instance;	/** 单件实例 */
};

#endif