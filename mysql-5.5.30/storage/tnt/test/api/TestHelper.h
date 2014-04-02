/**
 * 用于简化表级或数据库级测试用例编写的辅助功能
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TEST_HELPER_H_
#define _NTSE_TEST_HELPER_H_

#include <vector>
#include "misc/TableDef.h"
#include "api/Database.h"
#include "api/Table.h"

using namespace std;
using namespace ntse;

/** 索引键 */
class IdxKey {
public:
	IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, ...);
	IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, va_list attrs);
	IdxKey(const IdxKey &copy);
	virtual ~IdxKey();
private:
	void init(const TableDef *tableDef, u16 idx, u16 numAttrs, va_list attrs);

	SubRecord	*m_key;		/** 键值，可能为NULL */
friend class TblInterface;
};

/** 索引扫描范围 */
class IdxRange {
public:
	IdxRange(const TableDef *tableDef, u16 idx, const IdxKey low, const IdxKey high,
		bool includeLow = true, bool includeHigh = true);
	IdxRange(const TableDef *tableDef, u16 idx);
	virtual ~IdxRange();

private:
	u16		m_idx;			/** 针对哪个索引进行扫描 */
	IdxKey	m_low;			/** 下界 */
	IdxKey	m_high;			/** 上界 */
	bool	m_includeLow;	/** 是否包含等于下界的记录 */
	bool	m_includeHigh;	/** 是否包含等于上界的记录 */

friend class TblInterface;
};

/** 结果集 */
class ResultSet {
public:
	ResultSet(const TableDef *tableDef, u16 numCols, const u16 *cols);
	virtual ~ResultSet();
	bool operator == (const ResultSet &another);
	u64 getNumRows();
	void addRow(RowId rid, const byte *row);

public:
	TableDef		*m_tableDef;/** 表定义 */
	u16				m_numCols;	/** 属性数 */
	u16				*m_cols;	/** 各属性号 */
	vector<byte *>	m_rows;		/** 记录数据 */
	vector<RowId>	m_rids;		/** 记录RID */
};

/** NTSE表操作接口 */
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
	Database		*m_db;			/** 所属的数据库 */
	const char		*m_path;		/** 要操作的表的路径 */
	Connection		*m_conn;		/** 用于操作数据库的连接，可能为NULL */
	Session			*m_session;		/** 用于操作数据库的会话，可能为NULL */
	Table			*m_table;		/** 所操作的表，可能为NULL */
	TableDef		*m_tableDef;	/** 所操作的表的定义，可能为NULL */
};
#endif
