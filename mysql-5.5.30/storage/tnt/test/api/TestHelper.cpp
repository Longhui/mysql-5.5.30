/**
 * 用于简化表级或数据库级测试用例编写的辅助功能
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include "api/TestHelper.h"
#include "misc/RecordHelper.h"
#include "api/TestTable.h"
#include "util/File.h"

/** 构造函数
 * @param tableDef 表定义
 * @param idx 索引编号
 * @param numAttrs 包含属性个数
 * @param ... 各属性值
 */
IdxKey::IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, ...) {
	va_list args;
	va_start(args, numAttrs);
	init(tableDef, idx, numAttrs, args);
	va_end(args);
}

/** 构造函数
 * @param tableDef 表定义
 * @param idx 索引编号
 * @param numAttrs 包含属性个数
 * @param attrs 各属性值
 */
IdxKey::IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, va_list attrs) {
	init(tableDef, idx, numAttrs, attrs);
}

/** 初始化
 * @param tableDef 表定义
 * @param idx 索引编号
 * @param numAttrs 包含属性个数
 * @param attrs 各属性值
 */
void IdxKey::init(const TableDef *tableDef, u16 idx, u16 numAttrs, va_list attrs) {
	assert(idx < tableDef->m_numIndice);
	IndexDef *indexDef = tableDef->m_indice[idx];
	assert(numAttrs <= indexDef->m_numCols);
	if (numAttrs > 0) {
		SubRecordBuilder srb(tableDef, KEY_PAD);
		vector<u16> columns;
		vector<void *> data;
		for (u16 i = 0; i < numAttrs; i++) {
			u16 cno = indexDef->m_columns[i];
			columns.push_back(cno);
			ColumnType type = tableDef->m_columns[cno]->m_type;
			char* p;
			switch (type) {
			case CT_TINYINT:
				p = new char[1];
				*p = (char)va_arg(attrs, int);
				data.push_back(p);
				break;
			case CT_SMALLINT:
				p = new char[2];
				(*(short *)p) = (short)va_arg(attrs, int);
				data.push_back(p);
				break;
			case CT_INT:
				p = new char[4];
				(*(int *)p) = va_arg(attrs, int);
				data.push_back(p);
				break;
			case CT_BIGINT:
				p = new char[8];
				(*(u64 *)p) = va_arg(attrs, u64);
				data.push_back(p);
				break;
			case CT_CHAR:
			case CT_VARCHAR:
				p = System::strdup(va_arg(attrs, char *));
				data.push_back(p);
				break;
			default:
				assert(false);
			}
		}
		m_key = srb.createSubRecord(columns, data);
		m_key->m_rowId = INVALID_ROW_ID;
		for (size_t i = 0; i < data.size(); i++)
			delete [] ((char *)data[i]);
	} else
		m_key = NULL;
}

/** 拷贝构造函数
 * @param copy 从这个对象拷贝
 */
IdxKey::IdxKey(const IdxKey &copy) {
	SubRecord *key = copy.m_key;
	if (key) {
		byte *data = new byte[key->m_size];
		memcpy(data, key->m_data, key->m_size);
		u16 *cols = new u16[key->m_numCols];
		memcpy(cols, key->m_columns, sizeof(u16) * key->m_numCols);
		m_key = new SubRecord(key->m_format, key->m_numCols, cols, data, key->m_size, key->m_rowId);
	} else
		m_key = key;
}

/** 析构函数 */
IdxKey::~IdxKey() {
	if (m_key) {
		freeSubRecord(m_key);
		m_key = NULL;
	}
}

/** 构造函数
 * @param tableDef 表定义
 * @param idx 索引编号
 * @param low 起始范围
 * @param high 结束范围
 * @param includeLow 为true输出>=low的记录，为false只输出>low的记录
 * @param includeHigh 为true输出<=high的记录，为false只输出<high的记录
 */
IdxRange::IdxRange(const TableDef *tableDef, u16 idx, const IdxKey low, const IdxKey high,
				   bool includeLow, bool includeHigh): m_low(low), m_high(high) {
	assert(idx < tableDef->m_numIndice);
	m_includeLow = includeLow;
	m_includeHigh = includeHigh;
	m_idx = idx;
}

/** 构造一个对指定索引进行全扫描的搜索范围
 * @param tableDef 表定义
 * @param idx 索引编号
 */
IdxRange::IdxRange(const TableDef *tableDef, u16 idx): m_low(tableDef, idx, 0), m_high(tableDef, idx, 0) {
	assert(idx < tableDef->m_numIndice);
	m_includeLow = true;
	m_includeHigh = true;
	m_idx = idx;
}

/** 析构函数 */
IdxRange::~IdxRange() {

}

/** 构造函数
 * @param numCols 结果集中包含的属性数
 * @param cols 结果集中包含的各属性号，拷贝
 */
ResultSet::ResultSet(const TableDef *tableDef, u16 numCols, const u16 *cols) {
	m_tableDef = new TableDef(tableDef);
	m_numCols = numCols;
	m_cols = new u16[numCols];
	memcpy(m_cols, cols, sizeof(u16) * numCols);
}

/**
 * 析构函数
 */
ResultSet::~ResultSet() {
	for (size_t n = 0; n < m_rows.size(); n++) {
		byte *row = m_rows[n];
		for (u16 i = 0; i < m_numCols; i++) {
			u16 cno = m_cols[i];
			if (!RedRecord::isNull(m_tableDef, row, cno) && m_tableDef->m_columns[cno]->isLob()) {
				void *lob;
				size_t size;
				RedRecord::readLob(m_tableDef, row, cno, &lob, &size);
				delete []((byte *)lob);
			}
		}
		delete []row;
	}
	delete m_tableDef;
	delete []m_cols;
}

/** 获得结果集中的记录数
 * @return 结果集中的记录数
 */
u64 ResultSet::getNumRows() {
	return (u64)m_rows.size();
}

/** 比较两个结果集是否相等，记录顺序有关
 * @pre 两个结果集所属的表定义包含相同的属性列表，且结果集包含的属性列表也相同
 * @param another 参与比较的另一个结果集
 * @return 是否相等
 */
bool ResultSet::operator == (const ResultSet &another) {
	assert(m_tableDef->m_numCols == another.m_tableDef->m_numCols);
	assert(m_numCols == another.m_numCols);
	assert(!memcmp(m_cols, another.m_cols, sizeof(u16) * m_numCols));
	if (m_rows.size() != another.m_rows.size())
		return false;
	for (size_t i = 0; i < m_rows.size(); i++) {
		if (TableTestCase::compareRecord(m_tableDef, m_rows[i], another.m_rows[i], m_numCols, m_cols))
			return false;
	}
	return true;
}

/** 增加一条记录
 * @param rid 记录RID
 * @param row 记录内容，拷贝
 */
void ResultSet::addRow(RowId rid, const byte *row) {
	byte *rc = new byte[m_tableDef->m_maxRecSize];
	memcpy(rc, row, m_tableDef->m_maxRecSize);
	m_rids.push_back(rid);
	m_rows.push_back(rc);
	for (u16 i = 0; i < m_numCols; i++) {
		u16 cno = m_cols[i];
		if (!RedRecord::isNull(m_tableDef, row, cno) && m_tableDef->m_columns[cno]->isLob()) {
			void *lob;
			size_t size;
			RedRecord::readLob(m_tableDef, row, cno, &lob, &size);
			byte *lobCopy = new byte[size];
			memcpy(lobCopy, lob, size);
			RedRecord::writeLob(m_tableDef, rc, cno, lobCopy, size);
		}
	}
}

/** 构造函数
 * @param db 所属数据库
 * @param path 表路径，拷贝
 */
TblInterface::TblInterface(Database *db, const char *path) {
	m_db = db;
	m_path = System::strdup(path);
	m_table = NULL;
	m_tableDef = NULL;
	m_conn = NULL;
	m_session = NULL;
}

/** 析构函数。若表没有关闭则自动关闭 */
TblInterface::~TblInterface() {
	if (m_table)
		close();
	delete []m_path;
}

/** 得到操作的表
 * @return 操作的表，表未打开时返回NULL
 */
Table* TblInterface::getTable() {
	return m_table;
}

/** 得到操作的表的定义
 * @return 操作的表的定义，表未打开时返回NULL
 */
TableDef* TblInterface::getTableDef() {
	return m_tableDef;
}

/** 查找指定名称的索引，大小写敏感
 * @param name 索引名
 * @return 索引编号，找不到时返回-1
 */
u16 TblInterface::findIndex(const char *name) {
	assert(m_table);
	for (u16 i = 0; i < m_tableDef->m_numIndice; i++)
		if (!strcmp(m_tableDef->m_indice[i]->m_name, name))
			return i;
	return (u16)-1;
}

/** 返回指定的索引定义
 * @param idx 索引编号
 * @return 对应的索引定义
 */
IndexDef* TblInterface::getIndexDef(u16 idx) {
	assert(m_table);
	assert(idx < m_tableDef->m_numIndice);
	return m_tableDef->m_indice[idx];
}

/** 创建表
 * @param tableDef 表定义
 * @throw NtseException 操作失败
 */
void TblInterface::create(TableDef *tableDef) throw(NtseException) {
	assert(!m_table);
	loadConnection();
	m_db->createTable(m_session, m_path, tableDef);
}

/** 打开表
 * @throw NtseException 打开失败
 */
void TblInterface::open() throw(NtseException) {
	assert(!m_table);
	loadConnection();
	m_table = m_db->openTable(m_session, m_path);
	m_tableDef = m_table->getTableDef();
}

/** 关闭表 */
void TblInterface::close(bool realClose) {
	assert(m_table);
	loadConnection();
	m_db->closeTable(m_session, m_table);
	if (realClose) {
		m_db->realCloseTableIfNeed(m_session, m_table);
	}
	m_table = NULL;
	m_tableDef = NULL;
}

/** 截断表数据
 * @throw NtseException 操作失败
 */
void TblInterface::truncate() throw(NtseException) {
	assert(m_table);
	loadConnection();
	m_db->truncateTable(m_session, m_table);
	m_tableDef = m_table->getTableDef();
}

/** 删除表。表被打开时自动关闭
 * @throw NtseException
 */
void TblInterface::drop() throw(NtseException) {
	if (m_table)
		close();
	loadConnection();
	m_db->dropTable(m_session, m_path);
}

/** 创建索引
 * @param indexDef 新增加的索引定义
 * @param numIndice 增加的索引个数
 * @throw NtseException 操作失败
 */
void TblInterface::addIndex(const IndexDef **indexDef, u16 numIndice) throw(NtseException) {
	assert(m_table);
	loadConnection();
	m_db->addIndex(m_session, m_table, numIndice, indexDef);
	m_tableDef = m_table->getTableDef();
}

/** 删除索引
 * @param idx 要删除的索引编号
 * @throw NtseException 操作失败
 */
void TblInterface::dropIndex(u16 idx) throw(NtseException) {
	assert(m_table);
	assert(idx < m_tableDef->m_numIndice);
	loadConnection();
	m_db->dropIndex(m_session, m_table, idx);
	m_tableDef = m_table->getTableDef();
}

/** 重命名表
 * @param newPath 表的新路径
 * @throw NtseException 文件操作失败
 */
void TblInterface::rename(const char *newPath) throw(NtseException) {
	assert(!m_table);
	loadConnection();
	m_db->renameTable(m_session, m_path, newPath);
	delete []m_path;
	m_path = System::strdup(newPath);
}

/** OPTIMIZE表
 * @throw NtseException 操作失败
 */
void TblInterface::optimize() throw(NtseException) {
	assert(m_table);
	loadConnection();
	m_db->optimizeTable(m_session, m_table);
	m_tableDef = m_table->getTableDef();
}

/** 刷出表中脏数据 */
void TblInterface::flush() {
	assert(m_table);
	loadConnection();
	m_table->flush(m_session);
}

/** 插入记录
 * @param cols 属性列表，用空格分隔，若为NULL则表示包含所有属性
 * @param ... 各属性值
 * @throw NtseException 加表锁超时或记录超长
 * @return 是否成功，由于唯一性索引冲突可能会失败
 */
bool TblInterface::insertRow(const char *cols, ...) throw(NtseException) {
	assert(m_table);
	va_list args;
	va_start(args, cols);
	byte *r = buildMysqlRow(cols, args);
	va_end(args);
	loadConnection();
	uint dupIndex;
	try {
		bool succ = m_table->insert(m_session, r, true, &dupIndex) != INVALID_ROW_ID;
		freeMysqlRecord(m_tableDef, new Record(INVALID_ROW_ID, REC_MYSQL, r, m_tableDef->m_maxRecSize));
		return succ;
	} catch (NtseException &e) {
		freeMysqlRecord(m_tableDef, new Record(INVALID_ROW_ID, REC_MYSQL, r, m_tableDef->m_maxRecSize));
		throw e;
	}
}

/** 通过索引扫描更新记录
 * @param cond 指定要更新的记录范围
 * @param cols 要更新的属性名称列表，用空格分隔，若为NULL则表示包含所有属性
 * @param ... 要更新的属性的新值
 * @throw NtseException 记录超长
 * @return 成功更新的记录数，更新时若发生唯一性索引冲突，则马上中断，不更新后续记录
 */
u64 TblInterface::updateRows(const IdxRange cond, const char *cols, ...) throw(NtseException) {
	assert(m_table);
	va_list args;
	va_start(args, cols);
	u64 n = doUpdate(&cond, -1, 0, cols, args);
	va_end(args);
	return n;
}

/** 通过索引扫描更新记录
 * @param cond 指定要更新的记录范围
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @param cols 要更新的属性名称列表，用空格分隔，若为NULL则表示包含所有属性
 * @param ... 要更新的属性的新值
 * @throw NtseException 记录超长
 * @return 成功更新的记录数，更新时若发生唯一性索引冲突，则马上中断，不更新后续记录
 */
u64 TblInterface::updateRows(const IdxRange cond, int limit, uint offset, const char *cols, ...) throw(NtseException) {
	assert(m_table);
	va_list args;
	va_start(args, cols);
	u64 n = doUpdate(&cond, limit, offset, cols, args);
	va_end(args);
	return n;
}

/** 通过表扫描更新记录
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @param cols 要更新的属性名称列表，用空格分隔，若为NULL则表示包含所有属性
 * @param ... 要更新的属性的新值
 * @throw NtseException 记录超长
 * @return 成功更新的记录数，更新时若发生唯一性索引冲突，则马上中断，不更新后续记录
 */
u64 TblInterface::updateRows(int limit, uint offset, const char *cols, ...) throw(NtseException) {
	assert(m_table);
	va_list args;
	va_start(args, cols);
	u64 n = doUpdate(NULL, limit, offset, cols, args);
	va_end(args);
	return n;
}

/** 更新记录
 * @param cond 指定要更新的记录范围，若为NULL则进行表扫描，否则进行索引扫描
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @param cols 要更新的属性名称列表，用空格分隔，若为NULL则表示包含所有属性
 * @param ... 要更新的属性的新值
 * @throw NtseException 记录超长
 * @return 成功更新的记录数，更新时若发生唯一性索引冲突，则马上中断，不更新后续记录
 */
u64 TblInterface::doUpdate(const IdxRange *cond, int limit, uint offset, const char *cols, va_list values) throw(NtseException) {
	byte *r = buildMysqlRow(cols, values);

	loadConnection();

	u16 numUpdCols;
	u16 *updCols = parseCols(cols, &numUpdCols);

	TblScan *h = NULL;

	try {
		if (cond) {
			IndexScanCond isc = getIscFromIdxRange(*cond);
			IndexDef *searchIdx = m_tableDef->m_indice[cond->m_idx];
			h = m_table->indexScan(m_session, OP_UPDATE, &isc, searchIdx->m_numCols, searchIdx->m_columns);
		} else {
			h = m_table->tableScan(m_session, OP_UPDATE, numUpdCols, updCols);
		}
	} catch (NtseException &) {
		freeMysqlRecord(m_tableDef, new Record(INVALID_ROW_ID, REC_MYSQL, r, m_tableDef->m_maxRecSize));
		delete []updCols;
		return 0;
	}
	h->setUpdateColumns(numUpdCols, updCols);

	u64 pos = 0;
	u64 n = 0;
	byte *buf = new byte[m_tableDef->m_maxRecSize];
	while (m_table->getNext(h, buf)) {
		if (cond && !recordInRange(buf, *cond))
			break;
		pos++;
		if (pos <= offset)
			continue;
		if (limit >= 0 && pos > (u64)limit + offset)
			break;
		try {
			if (m_table->updateCurrent(h, r))
				n++;
		} catch (NtseException &e) {
			delete []buf;
			m_table->endScan(h);
			delete []updCols;
			freeMysqlRecord(m_tableDef, new Record(INVALID_ROW_ID, REC_MYSQL, r, m_tableDef->m_maxRecSize));
			throw e;
		}
	}
	delete []buf;
	m_table->endScan(h);
	int j = 0;
	for (int i = 0; i < m_tableDef->m_numCols; i++) {
		bool isUpd = false;
		for (int j = 0 ;  j < numUpdCols; j++) {
			if (i == updCols[j]) {
				isUpd = true;			
				break;	
			}
		}
		if (!isUpd && m_tableDef->m_columns[i]->isLob())
			RedRecord::setNull(m_tableDef, r, i);
	
	}
	delete []updCols;
	freeMysqlRecord(m_tableDef, new Record(INVALID_ROW_ID, REC_MYSQL, r, m_tableDef->m_maxRecSize));
	return n;
}

/** 通过索引扫描删除记录
 * @param cond 指定要删除的记录范围
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @return 删除的记录数
 */
u64 TblInterface::deleteRows(const IdxRange cond, int limit, uint offset) {
	assert(m_table);

	loadConnection();
	IndexScanCond isc = getIscFromIdxRange(cond);
	TblScan *h = NULL;
	try {
		IndexDef *searchIdx = m_tableDef->m_indice[cond.m_idx];
		h = m_table->indexScan(m_session, OP_DELETE, &isc, searchIdx->m_numCols, searchIdx->m_columns);
	} catch (NtseException &) {
		return 0;
	}
	u64 r = doDelete(h, NULL, limit, offset);
	m_table->endScan(h);
	return r;
}

/** 通过表扫描删除记录
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @return 删除的记录数
 */
u64 TblInterface::deleteRows(int limit, uint offset) {
	assert(m_table);

	loadConnection();
	TblScan *h = NULL;
	u16 numCols = 1;
	u16 cols[1] = {0};
	try {
		h = m_table->tableScan(m_session, OP_DELETE, numCols, cols);
	} catch (NtseException &) {
		return 0;
	}
	u64 r = doDelete(h, NULL, limit, offset);
	m_table->endScan(h);
	return r;
}

/** 执行删除操作
 * @param h 扫描句柄
 * @param cond 索引扫描条件，可能为NULL
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @return 删除的记录数
 */
u64 TblInterface::doDelete(TblScan *h, const IdxRange *cond, int limit, uint offset) {
	u64 pos = 0;
	u64 n = 0;
	byte *buf = new byte[m_tableDef->m_maxRecSize];
	while (m_table->getNext(h, buf)) {
		if (cond && !recordInRange(buf, *cond))
			break;
		pos++;
		if (pos <= offset)
			continue;
		if (limit >= 0 && pos > (u64)limit + offset)
			break;
		m_table->deleteCurrent(h);
		n++;
	}
	delete []buf;
	return n;
}

/** 通过索引扫描查询记录
 * @param cond 查询条件
 * @param cols 要读取的属性名称列表，用空格分隔，若为NULL则表示包含所有属性，必须包含cond中涉及的属性
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @return 结果集，若发生异常返回NULL
 */
ResultSet* TblInterface::selectRows(const IdxRange cond, const char *cols, int limit, uint offset) {
	assert(m_table);

	loadConnection();

	IndexScanCond isc = getIscFromIdxRange(cond);
	u16 numCols;
	u16 *cnos = parseCols(cols, &numCols);
	// 为方便实现，要求要读取的属性必须包含cond中涉及的属性
	if (cond.m_low.m_key) {
		NTSE_ASSERT(cnosSubsume(cond.m_low.m_key->m_numCols, cond.m_low.m_key->m_columns, numCols, cnos));
	}
	if (cond.m_high.m_key) {
		NTSE_ASSERT(cnosSubsume(cond.m_high.m_key->m_numCols, cond.m_high.m_key->m_columns, numCols, cnos));
	}
	TblScan *h;
	try {
		h = m_table->indexScan(m_session, OP_READ, &isc, numCols, cnos);
	} catch (NtseException &) {
		return NULL;
	}
	ResultSet *rs = doSelect(h, numCols, cnos, &cond, limit, offset);
	m_table->endScan(h);
	delete []cnos;
	return rs;
}

/** 通过表扫描查询记录
 * @param cols 要读取的属性名称列表，用空格分隔，若为NULL则表示包含所有属性
 * @param limit LIMIT子句，若不指定LIMIT则设置为<0
 * @param offset OFFSET子句
 * @return 结果集
 */
ResultSet* TblInterface::selectRows(const char *cols, int limit, uint offset) {
	assert(m_table);

	loadConnection();

	u16 numCols;
	u16 *cnos = parseCols(cols, &numCols);
	TblScan *h;
	try {
		h = m_table->tableScan(m_session, OP_READ, numCols, cnos);
	} catch (NtseException &) {
		return NULL;
	}
	ResultSet *rs = doSelect(h, numCols, cnos, NULL, limit, offset);
	m_table->endScan(h);
	delete []cnos;
	return rs;
}

/** 执行SELECT操作
 * @param h 扫描句柄，有可能是索引扫描或表扫描
 * @param numCols 要读取的属性数
 * @param cols 要读取的属性编号
 * @param cond 选择条件，可能为NULL
 * @param limit LIMIT子句，<0表示不指定LIMIT
 * @param offset OFFSET子句
 * @return 结果集
 */
ResultSet* TblInterface::doSelect(TblScan *h, u16 numCols, u16 *cols, const IdxRange *cond, int limit, uint offset) {
	u64 pos = 0;
	ResultSet *rs = new ResultSet(m_tableDef, numCols, cols);
	byte *buf = new byte[m_tableDef->m_maxRecSize];
	memset(buf, 0, m_tableDef->m_maxRecSize);
	while (m_table->getNext(h, buf)) {
		if (cond && !recordInRange(buf, *cond))
			break;
		pos++;
		if (pos <= offset)
			continue;
		if (limit >= 0 && pos > (u64)offset + limit)
			break;
		rs->addRow(h->getCurrentRid(), buf);
	}
	delete []buf;
	return rs;
}

/** 构造一个索引扫描范围的便捷方式
 * @param idx 索引编号
 * @param numAttrs 起始与结束键值包含的属性数，使用此接口构造的起始与结束键值要求
 *   包含相同的属性数
 * @param includeLow 为true输出>=low的记录，为false只输出>low的记录
 * @param includeHigh 为true输出<=high的记录，为false只输出<high的记录
 * @param ... 起始与结束键值各属性值，应该具有2*numAttrs个参数
 * @return 索引扫描范围
 */
IdxRange TblInterface::buildRange(u16 idx, u16 numAttrs, bool includeLow, bool includeHigh, ...) {
	va_list args;
	va_start(args, includeHigh);
	IdxKey low(m_tableDef, idx, numAttrs, args);
	IdxKey high(m_tableDef, idx, numAttrs, args);
	va_end(args);
	return IdxRange(m_tableDef, idx, low, high, includeLow, includeHigh);
}

/** 构造一个只指定起始范围的索引扫描范围的便捷方式
 * @param idx 索引编号
 * @param numAttrs 起始键值包含的属性数
 * @param includeLow 为true输出>=low的记录，为false只输出>low的记录
 * @param ... 起始键值各属性值，应该具有numAttrs个参数
 * @return 索引扫描范围
 */
IdxRange TblInterface::buildLRange(u16 idx, u16 numAttrs, bool includeLow, ...) {
	va_list args;
	va_start(args, includeLow);
	IdxKey low(m_tableDef, idx, numAttrs, args);
	IdxKey high(m_tableDef, idx, 0);
	va_end(args);
	return IdxRange(m_tableDef, idx, low, high, includeLow, true);
}

/** 构造一个只指定结束范围的索引扫描范围的便捷方式
 * @param idx 索引编号
 * @param numAttrs 结束键值包含的属性数
 * @param includeHigh 为true输出<=high的记录，为false只输出<high的记录
 * @param ... 结束键值各属性值，应该具有numAttrs个参数
 * @return 索引扫描范围
 */
IdxRange TblInterface::buildHRange(u16 idx, u16 numAttrs, bool includeHigh, ...) {
	va_list args;
	va_start(args, includeHigh);
	IdxKey low(m_tableDef, idx, 0);
	IdxKey high(m_tableDef, idx, numAttrs, args);
	va_end(args);
	return IdxRange(m_tableDef, idx, low, high, true, includeHigh);
}

/** 备份
 *
 * @param hasLob 表是否包含大对象
 * @param dir 备份到这个目录，若为NULL则备份到数据库的basedir下
 * @return 备份结果的文件名，不含后缀
 */
string TblInterface::backup(bool hasLob, const char *dir) {
	string fromBase;
	if (dir == NULL)
		fromBase = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_path;
	else
		fromBase = string(dir) + NTSE_PATH_SEP + m_path;
	u64 ms = System::currentTimeMillis();
	stringstream ss;
	ss << fromBase << "-bak-" << ms;
	string toBase = ss.str();
	
	copyTable(fromBase, toBase, hasLob, false);

	return toBase;
}

/** 恢复备份
 * @pre 表必须已经被关闭
 *
 * @param bakPath 备份结果的文件名
 * @param hasLob 表是否包含大对象
 * @param toPath 恢复到这个路径，若为NULL则根据数据库basedir计算
 */
void TblInterface::restore(string bakPath, bool hasLob, const char *toPath) {
	assert(!m_table);
	string to;
	if (toPath)
		to = string(toPath);
	else
		to = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_path;
	copyTable(bakPath, to, hasLob, true);
}

/** 拷贝表数据文件
 * @param from 源路径
 * @param to 目标路径
 * @param hasLob 表是否包含大对象
 * @param overrideExist 是否覆盖已经存在的文件
 */
void TblInterface::copyTable(string &from, string &to, bool hasLob, bool overrideExist) {
	string fromPath = from + Limits::NAME_HEAP_EXT;
	string toPath = to + Limits::NAME_HEAP_EXT;
	CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), fromPath.c_str(), overrideExist) == File::E_NO_ERROR);

	fromPath = from + Limits::NAME_TBLDEF_EXT;
	toPath = to + Limits::NAME_TBLDEF_EXT;
	CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), fromPath.c_str(), overrideExist) == File::E_NO_ERROR);

	fromPath = from + Limits::NAME_IDX_EXT;
	toPath = to + Limits::NAME_IDX_EXT;
	CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), fromPath.c_str(), overrideExist) == File::E_NO_ERROR);

	if (hasLob) {
		fromPath = from + Limits::NAME_LOBD_EXT;
		toPath = to + Limits::NAME_LOBD_EXT;
		CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), fromPath.c_str(), overrideExist) == File::E_NO_ERROR);

		fromPath = from + Limits::NAME_LOBI_EXT;
		toPath = to + Limits::NAME_LOBI_EXT;
		CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), fromPath.c_str(), overrideExist) == File::E_NO_ERROR);

		fromPath = from + Limits::NAME_SOBH_TBLDEF_EXT;
		toPath = to + Limits::NAME_SOBH_TBLDEF_EXT;
		CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), fromPath.c_str(), overrideExist) == File::E_NO_ERROR);

		fromPath = from + Limits::NAME_SOBH_EXT;
		toPath = to + Limits::NAME_SOBH_EXT;
		CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), fromPath.c_str(), overrideExist) == File::E_NO_ERROR);
	}
}

/** 备份事务日志。本函数会备份指定目录下所有名为ntse_log.xxx的文件
 *
 * @param fromDir 从这个目录备份
 * @param toDir 备份到这个目录下
 * @return 备份路径
 */
string TblInterface::backupTxnlogs(const char *fromDir, const char *toDir) {
	File fd(fromDir);
	list<string> files;
	u64 code = fd.listFiles(&files, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	u64 ms = System::currentTimeMillis();
	stringstream ss;
	ss << toDir << NTSE_PATH_SEP << "bak-txnlog-" << ms << "-";
	string toBase = ss.str();

	string prefix = string(fromDir) + NTSE_PATH_SEP + Limits::NAME_TXNLOG;
	size_t prefixSize = prefix.length();
	for (list<string>::iterator iter = files.begin(); iter != files.end(); iter++) {
		string path = *iter;
		string name = path.substr(path.find_last_of(NTSE_PATH_SEP_CHAR) + 1);
		if (path.length() > prefixSize && !memcmp(path.c_str(), prefix.c_str(), prefixSize)) {
			string toPath = toBase + name;
			CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), path.c_str(), true) == File::E_NO_ERROR);
		}
	}

	return toBase;
}

/** 恢复备份的事务日志
 *
 * @param bakPath backupTxnlogs函数返回的备份路径
 * @param toDir 恢复到这个目录
 */
void TblInterface::restoreTxnlogs(string bakPath, const char *toDir) {
	size_t pos = bakPath.find_last_of(NTSE_PATH_SEP_CHAR);
	string fromDir = bakPath.substr(0, pos);
	size_t prefixSize = bakPath.length();

	File fd(fromDir.c_str());
	list<string> files;
	u64 code = fd.listFiles(&files, false);
	CPPUNIT_ASSERT(code == File::E_NO_ERROR);

	for (list<string>::iterator iter = files.begin(); iter != files.end(); iter++) {
		string path = *iter;
		if (path.length() > prefixSize && !memcmp(path.c_str(), bakPath.c_str(), prefixSize)) {
			string suffix = path.substr(prefixSize);
			string toPath = string(toDir) + NTSE_PATH_SEP + suffix;
			CPPUNIT_ASSERT(File::copyFile(toPath.c_str(), path.c_str(), true) == File::E_NO_ERROR);
		}
	}
}

/** 在数据库被关闭重新打开之后重连
 * @param db 数据库
 */
void TblInterface::reconnect(Database *db) {
	m_db = db;
	m_table = NULL;
	m_tableDef = NULL;
	m_conn = NULL;
	m_session = NULL;
}

/** 验证表数据一致 */
void TblInterface::verify() {
	assert(m_table);
	loadConnection();
	m_table->verify(m_session);
}

/** 进行检查点
 * @throw NtseException 检查点不成功
 */
void TblInterface::checkpoint() throw(NtseException) {
	loadConnection();
	m_db->checkpoint(m_session);
}

/** 构造REC_MYSQL格式的记录
 * @param cols 包含的属性名称列表，空格分隔，NULL表示所有属性
 * @param ... 各属性的值
 */
byte* TblInterface::buildMysqlRow(const char *cols, ...) {
	va_list args;
	va_start(args, cols);
	byte *r = buildMysqlRow(cols, args);
	va_end(args);
	return r;
}

/** 构造REC_MYSQL格式的记录
 * @param cols 包含的属性名称列表，空格分隔，NULL表示所有属性
 * @param values 各属性的值
 */
byte* TblInterface::buildMysqlRow(const char *cols, va_list values) {
	RedRecord rec(m_tableDef);
	u16 numCols;
	u16 *cnos = parseCols(cols, &numCols);
	for (u16 i = 0; i < numCols; i++) {
		u16 cno = cnos[i];
		ColumnType type = m_tableDef->m_columns[cno]->m_type;
		char* p;
		switch (type) {
			case CT_TINYINT:
				rec.writeNumber(cno, (char)va_arg(values, int));
				break;
			case CT_SMALLINT:
				rec.writeNumber(cno, (short)va_arg(values, int));
				break;
			case CT_INT:
				rec.writeNumber(cno, va_arg(values, int));
				break;
			case CT_BIGINT:
				rec.writeNumber(cno, va_arg(values, u64));
				break;
			case CT_CHAR:
			case CT_VARCHAR:
				p = va_arg(values, char *);
				if (p) {
					if (type == CT_CHAR)
						rec.writeChar(cno, p);
					else
						rec.writeVarchar(cno, p);
				} else
					rec.setNull(cno);
				break;
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				p = va_arg(values, char *);
				if (p) {
					p = System::strdup(p);
					size_t len = strlen(p);
					rec.writeLob(cno, (byte *)p, len);
				} else
					rec.setNull(cno);
				break;
			default:
				assert(false);
		}
	}
	delete []cnos;

	byte *r = new byte[rec.getRecord()->m_size];
	memcpy(r, rec.getRecord()->m_data, rec.getRecord()->m_size);
	for (u16 i = 0; i < m_tableDef->m_numCols; i++) {
		if (m_tableDef->m_columns[i]->isLob())
			rec.setNull(i);
	}
	return r;
}

SubRecord* TblInterface::convertKeyPN(const IndexDef *indexDef, const SubRecord *keyPad) {
	byte *data = new byte[m_tableDef->m_maxRecSize];
	u16 *cols = new u16[keyPad->m_numCols];
	memcpy(cols, keyPad->m_columns, sizeof(u16) * keyPad->m_numCols);
	SubRecord *keyNatural = new SubRecord(KEY_NATURAL, keyPad->m_numCols, cols, data, m_tableDef->m_maxRecSize, keyPad->m_rowId);
	RecordOper::convertKeyPN(m_tableDef, indexDef, keyPad, keyNatural);
	return keyNatural;
}

IndexScanCond TblInterface::getIscFromIdxRange(const IdxRange &cond) {
	bool singleFetch;
	if (m_tableDef->m_indice[cond.m_idx]->m_unique
		&& cond.m_includeLow && cond.m_includeHigh && cond.m_low.m_key && cond.m_high.m_key 
		&& cond.m_low.m_key->m_numCols == m_tableDef->m_indice[cond.m_idx]->m_numCols
		&& cond.m_high.m_key->m_numCols == m_tableDef->m_indice[cond.m_idx]->m_numCols) {
		SubRecord *lowNatural = convertKeyPN(m_tableDef->m_indice[cond.m_idx], cond.m_low.m_key);
		SubRecord *highNatural = convertKeyPN(m_tableDef->m_indice[cond.m_idx], cond.m_high.m_key);
		singleFetch = RecordOper::compareKeyNN(m_tableDef, lowNatural, highNatural, m_tableDef->m_indice[cond.m_idx]) == 0;
		freeSubRecord(lowNatural);
		freeSubRecord(highNatural);
	} else
		singleFetch = false;
	return IndexScanCond(cond.m_idx, cond.m_low.m_key, true, cond.m_includeLow, singleFetch);
}

u16* TblInterface::parseCols(const char *cols, u16 *numCols) {
	if (cols) {
		istringstream ss(cols);
		u16 curCol;
		string curColName;
		vector<u16> cnos;
		while (ss >> curColName) {
			for (curCol = 0; curCol < m_tableDef->m_numCols; ++curCol) {
				if (curColName == m_tableDef->m_columns[curCol]->m_name)
					break;
			}
			assert(curCol < m_tableDef->m_numCols);
			cnos.push_back(curCol);
		}
		*numCols = (u16)cnos.size();
		u16 *r = new u16[*numCols];
		for (u16 i = 0; i < cnos.size(); i++)
			r[i] = cnos[i];
		return r;
	} else {
		*numCols = m_tableDef->m_numCols;
		u16 *r = new u16[m_tableDef->m_numCols];
		for (u16 i = 0; i < m_tableDef->m_numCols; i++)
			r[i] = i;
		return r;
	}
}

void TblInterface::loadConnection() {
	if (!m_conn) {
		m_conn = m_db->getConnection(false);
		m_session = m_db->getSessionManager()->allocSession(__FUNCTION__, m_conn);
	}
}

/** 释放数据库连接与会话 */
void TblInterface::freeConnection() {
	if (m_conn) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_conn = NULL;
		m_session = NULL;
	}
}

/** 判断记录是否在指定的范围之中
 * @param buf 记录内容
 * @param cond 条件
 * @return 记录是否在指定范围之中
 */
bool TblInterface::recordInRange(const byte *buf, const IdxRange &cond) {
	if (cond.m_low.m_key && cond.m_low.m_key->m_numCols > 0) {
		int r = compareRecKey(buf, cond.m_low.m_key, m_tableDef->getIndexDef(cond.m_idx));
		if (r < 0 || (r == 0 && !cond.m_includeLow))
			return false;
	}
	if (cond.m_high.m_key && cond.m_high.m_key->m_numCols > 0) {
		int r = compareRecKey(buf, cond.m_high.m_key, m_tableDef->getIndexDef(cond.m_idx));
		if (r > 0 || (r == 0 && !cond.m_includeHigh))
			return false;
	}
	return true;
}

/** 比较一条记录与一个键
 * @param buf 记录
 * @param key 键值，必须为KEY_PAD格式
 * @return buf > key返回>0，buf = key返回0，否则返回<0
 */
int TblInterface::compareRecKey(const byte *buf, const SubRecord *key, const IndexDef *indexDef) {
	assert(key->m_format == KEY_PAD);
	Record rec(INVALID_ROW_ID, REC_REDUNDANT, (byte *)buf, m_tableDef->m_maxRecSize);
	byte *data = new byte[m_tableDef->m_maxRecSize];
	SubRecord key2(KEY_COMPRESS, key->m_numCols, key->m_columns, data, m_tableDef->m_maxRecSize);
	RecordOper::extractKeyRC(m_tableDef, indexDef, &rec, NULL, &key2);
	int r = RecordOper::compareKeyPC(m_tableDef, key, &key2, indexDef);
	delete []data;
	return -r;
}

/** 判断subset是否为superSet的子集
 * @param subSize subset大小
 * @param subset subset内容
 * @param superSize superSet大小
 * @param superSet superSet内容
 * @return subset是否为superSet子集
 */
bool TblInterface::cnosSubsume(u16 subSize, u16 *subset, u16 superSize, u16 *superSet) {
	for (u16 i= 0; i < subSize; i++) {
		u16 cno = subset[i];
		bool found = false;
		for (u16 j = 0; j < superSize; j++) {
			if (superSet[j] == cno) {
				found = true;
				break;
			}
		}
		if (!found)
			return false;
	}
	return true;
}
