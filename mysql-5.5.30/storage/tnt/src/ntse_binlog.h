/**
 * NTSE��¼binlog��ص����ݶ���
 * @author �ձ�(naturally@163.org)
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
 * �����˲��롢ɾ���͸����������͵�Binlog
 */
enum BinlogType {
	BINLOG_INSERT = 0,
	BINLOG_DELETE,
	BINLOG_UPDATE
};

/** ��ʾһ��binlog��ص���Ϣ���ݣ����м�¼�ĸ�ʽ����Ҫʹ��REC_MYSQL��ʽ
 *	���л��ṹ�����ݲμ�.cpp��ʵ�ֵ�˵��
 */
class BinlogInfo {
public:
	/** Ҫдbinlog�����������Ϣ
	 */
	struct TxnInfo {
		u64	m_sqlMode;	  /** ��ǰ�����SQL_MODE��Ϣ */
		u64 m_serverId;  /** ִ������ķ�����ID */  
	};
public:
	size_t getSerializeSize(const TableDef *tableDef) const;
	void serialize(const TableDef *tableDef, byte *buffer, u32 bufferSize);
	void unserialize(MemoryContext *mc, byte *buffer, u32 bufferSize);
	bool isBinlogValid() const;

	TableDef			*m_tableDef;		/** ��־�������ID */
	TxnInfo				m_txnInfo;			/** ��־�����������Ϣ */
	SubRecord			m_beforeSR;			/** ǰ���¼���� */
	SubRecord			m_afterSR;			/** �����¼���� */
	BinlogType			m_type;				/** binlog���� */
};


/** ���ڶ�дbinlog�Ļ���ṹ���û����ʵ�ʲ���һ��MemoryContext���洢binlog��Ϣ
 * ���ռ䲻�������Զ������չ��Ϊ�˷���ռ�����û��治֧�ֵ���binlog��ɾ����ֻ֧��ȫ��ɾ��
 * ������Ҫ֧�ֽ������binlog�������Ӧ�ı���������ڻ����ڲ���ά��һ��vector��������������ݣ���������
 * �û��治���̰߳�ȫ��
 */
class BinlogBuffer {
private:
	struct ExtendBinlogInfo {
		ExtendBinlogInfo(u16 tableId, void *address, size_t size) : m_address(address), m_tableId(tableId), m_binlogInfoSize(size) {}
		void *m_address;		/** binlogInfo��Ӧ����ʼ��ַ */
		u16 m_tableId;			/** binlogInfo����Ӧ���ID */
		size_t m_binlogInfoSize;/** binlogInfo��Ϣ���� */

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
	Event m_bufferCleanEvent;				/** �����ȴ�����Ϊ�յ��ź��� */
	MemoryContext *m_buffer;				/** ���ڴ������������Ϊʵ�ʻ��� */
	u64 m_bufferInitSize;					/** �������ʼ��С */
	vector<ExtendBinlogInfo> m_extendInfo;	/** ���ڱ������Binlog�ڴ��ַ�Ͷ�ӦTableId�Ե����� */
	size_t	m_size;							/** �����Ѿ�ʹ�õĴ�С */

	static const double BUFFER_FULL_RATIO;							/** ��ǰ����ʹ��������������ڴ�����֮����ֵ�����������ֵ��������ܿ�ͻ�д�� */
	static const u32 DEFAULT_RESERVE_EXTENDBINLOGINFO_SIZE = 5000;	/** Ĭ��Ԥ������չbinlog��Ϣ���� */

public:
	static const u32 DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;			/** Ĭ�ϻ���Ϊ8M */
};


/**
 * ͳ��binlog���������ʹ��״��
 */
struct BinlogBufferStatus {
	u64 m_switchTimes;			/** ��д�����л����� */
	u64 m_writeTimes;			/** д�뻺���binlog�� */
	u64 m_readTimes;			/** �Ӷ������ȡ�Ĵ�����������Ӧ�õ�ͬ��m_switchTimes */
	u64 m_readBufferFullRatio;	/** �����汥���ʣ���ͬ��д���汥���� */
	u64 m_pollWaitTime;			/** POLL�����еȴ���ʱ���ܺͣ���λ���� */
	u64 m_writeBufferSize;		/** ��ǰд�����������־��С */
	u64 m_writeBufferCount;		/** ��ǰд�����������־���� */
	u64 m_readBufferSize;		/** ��ǰ�������������־��С */
	u64 m_readBufferUnflushSize;/** ��ǰ������δˢ�³�ȥ����־��С */
	u64 m_readBufferUnflushCount;/** ��ǰ������δˢ��ȥ��־������ */
};


/**
 * ����Ĺ����ǹ������������BinlogBuffer�࣬ÿ��ʹ��һ��������ж���һ���������д
 * ˫������Ҫ���ܹ���д����ͻ��ͬʱ������ά������д�߳�д��ĳ�������ʱ������������ʱ���У�
 * ��Ҫ��ʱ�Ľ�д����Ͷ�����Ի���ɫ���ö����������ݿɶ�����д������Բ���Ҫ����������չ����д��
 *
 * �������Ҫͬ�����̶߳Ի����д�����ǲ�ͬ����
 * ����ֻ�ܶ�һ�Σ���֧�ֶ�ζ�ȡ
 * Ҫ��ȡ֮ǰ��Ҫʹ��pollBinlogInfo������ѯ��֤��ǰ�������㹻���ݿɶ�
 * ������Ҫ����beginReadBinlogInfo��readNextBinlogInfo-endReadBinlogInfo��˳��ִ��
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
	
	ntse::Mutex m_mutex;						/** ��������д�뻺���̵߳Ļ����� */
	Event m_switchEvent;				/** ��������Ϊ�ջ���д���治�����ʱ�������ȴ����ź��� */
	Event m_fullEvent;					/** ��д��������ʱ�������ȴ����ź��� */
	BinlogBuffer *m_readBuffer;			/** �û������ڶ� */
	ntse::Mutex m_readMutex;					/** �������������� */
	BinlogBuffer *m_writeBuffer;		/** �û�������д */
	size_t m_pos;						/** ��������ȡ��Ϣ�������±� */
	struct BinlogBufferStatus m_status;	/** ͳ����Ϣ */
};

class THD;
struct st_table;
struct st_bitmap;
class injector;

struct BinlogWriterStatus {
	u64 m_totalWrites;				/** �ܹ�д���˶���binlog */
	u64 m_transNum;					/** ʹ���˶��ٸ����� */
	u64 m_maxTransLogs;				/** һ�����������д���˶���������binlog */
	u64 m_minTransLogs;				/** һ������������д���˶���������binlog */
	u64 m_insertLogs;				/** �����������־�� */
	u64 m_deleteLogs;				/** ɾ���������־�� */
	u64 m_updateLogs;				/** �����������־�� */

	void onTransCommit(u64 transLogs) {
		if (transLogs > m_maxTransLogs)
			m_maxTransLogs = transLogs;
		if (transLogs != 0)	{ // ��־����Ϊ���ʾ��Ч�ύ
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

/** binlog����д��־���ඨ��
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
	THD *m_thd;			/** ����Ҫ���̶߳��� */
	injector *m_inj;	/** д��־��Ҫ��injector���� */
	bool m_running;		/** ��ʶbinlogд�߳��Ƿ���Ҫ���� */

	struct BinlogWriterStatus m_status;	/** ״̬ͳ����Ϣ */
};

class ha_ntse;

typedef map<CallbackType, NTSECallbackFN*>::iterator cbItor;
/**
 * NTSEʵ��дBinlog��־��ӿڶ���
 */
class NTSEBinlog {
public:
	virtual ~NTSEBinlog() {}

	virtual void registerNTSECallbacks(Database *ntsedb);
	virtual void unregisterNTSECallbacks(Database *ntsedb);
	
	/** 
	 * ˢд����ָ�����binlog��Ϣ
	 * @param tableId ��id
	 */
	virtual void flushBinlog(u16 tableId) = 0;

	/** �õ�binlogˢд�̵߳�ͳ����Ϣ
	 * @return ����ͳ����Ϣ
	 */
	virtual struct BinlogWriterStatus getBinlogWriterStatus() = 0;

	/** �õ�binlog����ͳ����Ϣ
	 * @return ����ͳ����Ϣ
	 */
	virtual struct BinlogBufferStatus getBinlogBufferStatus() = 0;

	virtual const char* getBinlogMethod() const = 0;

protected:
	/** ��ʼ���ص���������
	 */
	virtual void initCallbacks() = 0;

	/** ���ٻص���������
	 */
	virtual void destroyCallbacks();

	static bool needBinlog(ha_ntse *handler);

protected:
	map<CallbackType, NTSECallbackFN*>	m_callbacks;	/** �ص����� */
};

/**
 * ʹ�ó�����ֱ��дbinlog��ʵ��
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

	// ��ʵ��Ϊ������ֱ��дbinlog,����Ҫ����onCloseTable��onAlterTable�¼�
	//static void onCloseTable(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	//static void onAlterTable(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowInsert(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowDelete(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
	static void logRowUpdate(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);

	static void writeBinlog(const TableDef *tableDef, BinlogType type, ha_ntse *handler, const SubRecord *brec, const SubRecord *arec);

private:

	/** ����ֻ����һ��ʵ����ֻ֧��ͬ������
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
		Event m_waitEvent;				/** binlog�ȴ�д�����ź��� */
		Event m_writtenEvent;			/** binlog�ȴ�д���������ź��� */

		BinlogInfo *m_binlogInfo;		/** Ҫд��BinlogInfo���� */
		TABLE *m_table;					/** Ҫдbinlog��table���� */

		static DirectBinlogWriter *m_instance;		/** THD��������� */
	};

private:
	NTSEDirectBinlog::DirectBinlogWriter *m_writer;	/** binlogд���� */

	static ntse::Mutex *m_mutex;						/** ͬ������THDManager����Ļ����� */

	static NTSEDirectBinlog *m_instance;		/** ����ģʽʵ������ */

	static struct BinlogBufferStatus m_bufferStatus;	/** ����״̬ͳ����Ϣ */
};

/**
 * ֧�ֻ����Binlogʵ��
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

	/** ����Cache��ʽдbinlog����
	 */
	class CachedBinlogWriter : public BinlogWriter {
	public:
		CachedBinlogWriter(BinlogBufferManager *blBuffer);
		~CachedBinlogWriter();
		void run();

	private:
		bool isTableSwitched(u16 curTblId);

		u16 m_lastTableId;					/** ��һ��дbinlog�ı�ID��һ��ʼΪ-1 */

		BinlogBufferManager *m_blBuffer;	/** ��ȡ�߳�ʹ�õĻ������ */

		static const u32 DEFAULT_MAX_UNCOMMITTED_BINLOGS = 1000;	/** Ĭ��һ����������δ�ύbinlog�������������ֵ���������ύһ�� */
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
	CachedBinlogWriter *m_writer;						/** ˢдbinlog�̶߳��� */
	
	static NTSECachableBinlog *m_instance;		/** ����ģʽʵ������ */
	static BinlogBufferManager *m_blBuffer;		/** дbinlogʹ�õĻ��� */
};

/**
 * ��������ͷ�Binlog����Ĺ�����
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
	static NTSEBinlogFactory *m_instance;	/** ����ʵ�� */
};

#endif