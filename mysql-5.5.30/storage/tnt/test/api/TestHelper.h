/**
 * ���ڼ򻯱������ݿ⼶����������д�ĸ�������
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TEST_HELPER_H_
#define _NTSE_TEST_HELPER_H_

#include <vector>
#include "misc/TableDef.h"
#include "api/Database.h"
#include "api/Table.h"

using namespace std;
using namespace ntse;

/** ������ */
class IdxKey {
public:
	IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, ...);
	IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, va_list attrs);
	IdxKey(const IdxKey &copy);
	virtual ~IdxKey();
private:
	void init(const TableDef *tableDef, u16 idx, u16 numAttrs, va_list attrs);

	SubRecord	*m_key;		/** ��ֵ������ΪNULL */
friend class TblInterface;
};

/** ����ɨ�跶Χ */
class IdxRange {
public:
	IdxRange(const TableDef *tableDef, u16 idx, const IdxKey low, const IdxKey high,
		bool includeLow = true, bool includeHigh = true);
	IdxRange(const TableDef *tableDef, u16 idx);
	virtual ~IdxRange();

private:
	u16		m_idx;			/** ����ĸ���������ɨ�� */
	IdxKey	m_low;			/** �½� */
	IdxKey	m_high;			/** �Ͻ� */
	bool	m_includeLow;	/** �Ƿ���������½�ļ�¼ */
	bool	m_includeHigh;	/** �Ƿ���������Ͻ�ļ�¼ */

friend class TblInterface;
};

/** ����� */
class ResultSet {
public:
	ResultSet(const TableDef *tableDef, u16 numCols, const u16 *cols);
	virtual ~ResultSet();
	bool operator == (const ResultSet &another);
	u64 getNumRows();
	void addRow(RowId rid, const byte *row);

public:
	TableDef		*m_tableDef;/** ���� */
	u16				m_numCols;	/** ������ */
	u16				*m_cols;	/** �����Ժ� */
	vector<byte *>	m_rows;		/** ��¼���� */
	vector<RowId>	m_rids;		/** ��¼RID */
};

/** NTSE������ӿ� */
class TblInterface {
public:
	TblInterface(Database *db, const char *path);
	virtual ~TblInterface();
	Table* getTable();
	TableDef* getTableDef();
	u16 findIndex(const char *name);
	IndexDef* getIndexDef(u16 idx);
	void create(TableDef *tableDef) throw(NtseException);
	void open() throw(NtseException);
	void close(bool realClose = false);
	void truncate() throw(NtseException);
	void drop() throw(NtseException);
	void addIndex(const IndexDef **indexDef, u16 numIndice = 1) throw(NtseException);
	void dropIndex(u16 idx) throw(NtseException);
	void rename(const char *newPath) throw(NtseException);
	void optimize() throw(NtseException);
	void flush();
	bool insertRow(const char *cols, ...) throw(NtseException);
	u64 updateRows(const IdxRange cond, const char *cols, ...) throw(NtseException);
	u64 updateRows(const IdxRange cond, int limit, uint offset, const char *cols, ...) throw(NtseException);
	u64 updateRows(int limit, uint offset, const char *cols, ...) throw(NtseException);
	u64 deleteRows(const IdxRange cond, int limit = -1, uint offset = 0);
	u64 deleteRows(int limit = -1, uint offset = 0);
	ResultSet* selectRows(const IdxRange cond, const char *cols, int limit = -1, uint offset = 0);
	ResultSet* selectRows(const char *cols, int limit = -1, uint offset = 0);
	IdxRange buildRange(u16 idx, u16 numAttrs, bool includeLow, bool includeHigh, ...);
	IdxRange buildLRange(u16 idx, u16 numAttrs, bool includeLow, ...);
	IdxRange buildHRange(u16 idx, u16 numAttrs, bool includeHigh, ...);
	string backup(bool hasLob, const char *dir = NULL);
	void restore(string bakPath, bool hasLob, const char *toPath = NULL);
	string backupTxnlogs(const char *fromDir, const char *toDir);
	void restoreTxnlogs(string bakPath, const char *toDir);
	void reconnect(Database *db);
	void freeConnection();
	void verify();
	void checkpoint() throw(NtseException);
	byte* buildMysqlRow(const char *cols, va_list values);
	byte* buildMysqlRow(const char *cols, ...);

private:
	void loadConnection();
	SubRecord* convertKeyPN(const IndexDef *indexDef, const SubRecord *keyPad);
	IndexScanCond getIscFromIdxRange(const IdxRange &cond);
	u16* parseCols(const char *cols, u16 *numCols);
	u64 doUpdate(const IdxRange *cond, int limit, uint offset, const char *cols, va_list values) throw(NtseException);
	u64 doDelete(TblScan *h, const IdxRange *cond, int limit, uint offset);
	ResultSet* doSelect(TblScan *h, u16 numCols, u16 *cols, const IdxRange *cond, int limit, uint offset);
	void copyTable(string &from, string &to, bool hasLob, bool overrideExist);
	bool recordInRange(const byte *buf, const IdxRange &cond);
	bool cnosSubsume(u16 subSize, u16 *subset, u16 superSize, u16 *superSet);
	int compareRecKey(const byte *buf, const SubRecord *key, const IndexDef *indexDef);

private:
	Database		*m_db;			/** ���������ݿ� */
	const char		*m_path;		/** Ҫ�����ı��·�� */
	Connection		*m_conn;		/** ���ڲ������ݿ�����ӣ�����ΪNULL */
	Session			*m_session;		/** ���ڲ������ݿ�ĻỰ������ΪNULL */
	Table			*m_table;		/** �������ı�����ΪNULL */
	TableDef		*m_tableDef;	/** �������ı�Ķ��壬����ΪNULL */
};
#endif
