/**
 * �����ļ�
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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

/** �����ļ�ͷ */
struct CtrlFileHeader {
	u64 	m_checkpointLsn;/** ����LSN */
	u16		m_nextTableId;	/** ��һ����ID */
	u16		m_numTables;	/** ����� */
	u16		m_numDeleted;	/** ��ɾ���ı���� */
	u32		m_cleanClosed;	/** �Ƿ�ȫ�ر� */
	u32		m_numTxnlogs;	/** ������־�ļ����� */
	u64		m_numTempFiles;	/** ��ʱ�ļ����� */
	u64		m_tempFileSeq;	/** ��ʱ�ļ���� */		
};

/** �����ļ���ά���ı���Ϣ */
struct CFTblInfo {
	u16		m_id;			/** ��ID */
	u64		m_flushLsn;		/** ��flushLsn��LSNС�ڴ˵���־���޸��Ѿ�ȫ���־û�����ҪREDO */
	char	*m_path;		/** ��·�� */
	bool	m_hasLob;		/** ���Ƿ��������� */
	bool    m_hasCprsDict;  /** �Ƿ��м�¼ѹ���ֵ� */
	u64     m_tntFlushLsn;  /** tnt flushLsn, lsnС�ڴ˵���־����tnt redo/undo*/

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
/** �����ļ� */
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

	static const int MAX_PATH_LENGTH = 1024;	/** �ļ�·����󳤶� */
	static const int REC_STORAGE_VERSION = 2;	/** ��¼���ݴ洢��ʽ�汾�� */
	static const int IDX_STORAGE_VERSION = 1;	/** �������ݴ洢��ʽ�汾�� */

	bool	m_closed;				/** �Ƿ񱻹ر� */
	Hash<u16, CFTblInfo *>	m_tables;	/** ��ID������Ϣ��ӳ��� */
	Hash<char *, u16, StrNoCaseHasher, StrNoCaseEqualer>	m_pathToIds;	/** ���ļ�·������ID��ӳ��� */
	Hash<u16, CFTblInfo *>	m_deleted;	/** �Ѿ�ɾ���ı�ID������Ϣ��ӳ��� */
	set<string>	m_tempFiles;		/** ��ʱ�ļ� */
	byte	*m_fileBuf;				/** ���ڴ��б�������ļ����ݣ������������ */
	u32		m_bufSize;				/** m_fileBuf�Ĵ�С */
	CtrlFileHeader	m_header;		/** ������Ϣ */
	File	*m_file;				/** �ļ� */
	bool	m_cleanClosed;			/** �ϴ�ϵͳ�Ƿ�ȫ�ر� */
	vector<bool>	m_tempTableIdUsed;	/** ����ʱ��ID�Ƿ��Ѿ�ʹ�� */
	Mutex	m_lock;					/** ������������ */
	Syslog	*m_syslog;				/** ϵͳ��־ */
};
}
#endif
