/**
 * 控制文件
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_CONTROLFILE_H_
#define _NTSE_CONTROLFILE_H_

#include <iostream>
#include <set>
#include <string>
#include <vector>
#include "Global.h"
#include "util/Hash.h"

using namespace std;

namespace ntse {

/** 控制文件头 */
struct CtrlFileHeader {
	u64 	m_checkpointLsn;/** 检查点LSN */
	u16		m_nextTableId;	/** 下一个表ID */
	u16		m_numTables;	/** 表个数 */
	u16		m_numDeleted;	/** 被删除的表个数 */
	u32		m_cleanClosed;	/** 是否安全关闭 */
	u32		m_numTxnlogs;	/** 事务日志文件个数 */
	u64		m_numTempFiles;	/** 临时文件个数 */
	u64		m_tempFileSeq;	/** 临时文件序号 */		
};

/** 控制文件中维护的表信息 */
struct CFTblInfo {
	u16		m_id;			/** 表ID */
	u64		m_flushLsn;		/** 表flushLsn，LSN小于此的日志的修改已经全部持久化不需要REDO */
	char	*m_path;		/** 表路径 */
	bool	m_hasLob;		/** 表是否包含大对象 */
	bool    m_hasCprsDict;  /** 是否有记录压缩字典 */
	u64     m_tntFlushLsn;  /** tnt flushLsn, lsn小于此的日志不做tnt redo/undo*/

	CFTblInfo(u16 id, const char *path, bool hasLob, bool hasCprsDic);
	virtual ~CFTblInfo();
	void setPath(const char *newPath);
	void setHasCprsDic(bool hasCprsDict);
	static CFTblInfo* read(std::istream &s, bool tnt) throw(NtseException);
	void write(std::ostream &s) throw(NtseException);

private:
	CFTblInfo();
};

class Syslog;
class File;
/** 控制文件 */
class ControlFile {
public:
	static ControlFile* open(const char *path, Syslog *syslog, bool tnt = true) throw(NtseException);
#ifdef TNT_ENGINE
	static void create(const char *path, Syslog *syslog, uint numTxnLogs = 2) throw(NtseException);
#else
	static void create(const char *path, Syslog *syslog) throw(NtseException);
#endif
	void close(bool cleanUpTemps = false, bool clean = true);

	u16 allocTableId() throw(NtseException);
	void createTable(const char *path, u16 tableId, bool hasLob, bool hasCprsDict = false);
	void dropTable(u16 tableId);
	void renameTable(const char *oldPath, const char *newPath);
	void alterHasCprsDic(u16 tableId, bool hasCprsDict);
	void bumpFlushLsn(u16 tableId, u64 flushLsn);
	u64 getFlushLsn(u16 tableId);
	void bumpTntAndNtseFlushLsn(u16 tableId, u64 flushLsn, u64 tntFlushLsn);
	u64 getTntFlushLsn(u16 tableId);

	u16 getNumTables();
	u16 listAllTables(u16 *tableIds, u16 maxTabCnt);
	string getTablePath(u16 tableId);
	byte* dupContent(u32 *size);
	u16 getTableId(const char *path);
	bool isDeleted(u16 tableId);
	bool hasLob(u16 tableId);
	bool hasCprsDict(u16 tableId);

	u64 getCheckpointLSN();
	void setCheckpointLSN(u64 lsn);
	bool isCleanClosed();
	u32 getNumTxnlogs();
	void setNumTxnlogs(u32 n);

	char* allocTempFile(const char *schemaName, const char *tableName);
	void unregisterTempFile(const char *path);
	void cleanUpTempFiles();

	u16 allocTempTableId() throw(NtseException);
	void releaseTempTableId(u16 tableId);

#ifdef NTSE_UNIT_TEST
public:
#else
private:
#endif
	ControlFile(File *file);
	void init();
	static u64 checksum(const byte *buf, size_t size);
	void updateFile();
	void check() throw(NtseException);
	void freeMem();
	string serialize();
	static void getLineWithCheck(stringstream &ss, bool eol, char delim, char *buf, size_t bufSize, const char *expected) throw(NtseException);

	static const int MAX_PATH_LENGTH = 1024;	/** 文件路径最大长度 */
	static const int REC_STORAGE_VERSION = 2;	/** 记录数据存储格式版本号 */
	static const int IDX_STORAGE_VERSION = 1;	/** 索引数据存储格式版本号 */

	bool	m_closed;				/** 是否被关闭 */
	Hash<u16, CFTblInfo *>	m_tables;	/** 表ID到表信息的映射表 */
	Hash<char *, u16, StrNoCaseHasher, StrNoCaseEqualer>	m_pathToIds;	/** 表文件路径到表ID的映射表 */
	Hash<u16, CFTblInfo *>	m_deleted;	/** 已经删除的表ID到表信息的映射表 */
	set<string>	m_tempFiles;		/** 临时文件 */
	byte	*m_fileBuf;				/** 在内存中保存控制文件内容，方便计算检验和 */
	u32		m_bufSize;				/** m_fileBuf的大小 */
	CtrlFileHeader	m_header;		/** 基本信息 */
	File	*m_file;				/** 文件 */
	bool	m_cleanClosed;			/** 上次系统是否安全关闭 */
	vector<bool>	m_tempTableIdUsed;	/** 各临时表ID是否已经使用 */
	Mutex	m_lock;					/** 保护并发的锁 */
	Syslog	*m_syslog;				/** 系统日志 */
};
}
#endif
