/**
 * ���ڼ򻯱������ݿ⼶����������д�ĸ�������
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "api/TestHelper.h"
#include "misc/RecordHelper.h"
#include "api/TestTable.h"
#include "util/File.h"

/** ���캯��
 * @param tableDef ����
 * @param idx �������
 * @param numAttrs �������Ը���
 * @param ... ������ֵ
 */
IdxKey::IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, ...) {
	va_list args;
	va_start(args, numAttrs);
	init(tableDef, idx, numAttrs, args);
	va_end(args);
}

/** ���캯��
 * @param tableDef ����
 * @param idx �������
 * @param numAttrs �������Ը���
 * @param attrs ������ֵ
 */
IdxKey::IdxKey(const TableDef *tableDef, u16 idx, u16 numAttrs, va_list attrs) {
	init(tableDef, idx, numAttrs, attrs);
}

/** ��ʼ��
 * @param tableDef ����
 * @param idx �������
 * @param numAttrs �������Ը���
 * @param attrs ������ֵ
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

/** �������캯��
 * @param copy ��������󿽱�
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

/** �������� */
IdxKey::~IdxKey() {
	if (m_key) {
		freeSubRecord(m_key);
		m_key = NULL;
	}
}

/** ���캯��
 * @param tableDef ����
 * @param idx �������
 * @param low ��ʼ��Χ
 * @param high ������Χ
 * @param includeLow Ϊtrue���>=low�ļ�¼��Ϊfalseֻ���>low�ļ�¼
 * @param includeHigh Ϊtrue���<=high�ļ�¼��Ϊfalseֻ���<high�ļ�¼
 */
IdxRange::IdxRange(const TableDef *tableDef, u16 idx, const IdxKey low, const IdxKey high,
				   bool includeLow, bool includeHigh): m_low(low), m_high(high) {
	assert(idx < tableDef->m_numIndice);
	m_includeLow = includeLow;
	m_includeHigh = includeHigh;
	m_idx = idx;
}

/** ����һ����ָ����������ȫɨ���������Χ
 * @param tableDef ����
 * @param idx �������
 */
IdxRange::IdxRange(const TableDef *tableDef, u16 idx): m_low(tableDef, idx, 0), m_high(tableDef, idx, 0) {
	assert(idx < tableDef->m_numIndice);
	m_includeLow = true;
	m_includeHigh = true;
	m_idx = idx;
}

/** �������� */
IdxRange::~IdxRange() {

}

/** ���캯��
 * @param numCols ������а�����������
 * @param cols ������а����ĸ����Ժţ�����
 */
ResultSet::ResultSet(const TableDef *tableDef, u16 numCols, const u16 *cols) {
	m_tableDef = new TableDef(tableDef);
	m_numCols = numCols;
	m_cols = new u16[numCols];
	memcpy(m_cols, cols, sizeof(u16) * numCols);
}

/**
 * ��������
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

/** ��ý�����еļ�¼��
 * @return ������еļ�¼��
 */
u64 ResultSet::getNumRows() {
	return (u64)m_rows.size();
}

/** �Ƚ�����������Ƿ���ȣ���¼˳���й�
 * @pre ��������������ı��������ͬ�������б��ҽ���������������б�Ҳ��ͬ
 * @param another ����Ƚϵ���һ�������
 * @return �Ƿ����
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

/** ����һ����¼
 * @param rid ��¼RID
 * @param row ��¼���ݣ�����
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

/** ���캯��
 * @param db �������ݿ�
 * @param path ��·��������
 */
TblInterface::TblInterface(Database *db, const char *path) {
	m_db = db;
	m_path = System::strdup(path);
	m_table = NULL;
	m_tableDef = NULL;
	m_conn = NULL;
	m_session = NULL;
}

/** ��������������û�йر����Զ��ر� */
TblInterface::~TblInterface() {
	if (m_table)
		close();
	delete []m_path;
}

/** �õ������ı�
 * @return �����ı���δ��ʱ����NULL
 */
Table* TblInterface::getTable() {
	return m_table;
}

/** �õ������ı�Ķ���
 * @return �����ı�Ķ��壬��δ��ʱ����NULL
 */
TableDef* TblInterface::getTableDef() {
	return m_tableDef;
}

/** ����ָ�����Ƶ���������Сд����
 * @param name ������
 * @return ������ţ��Ҳ���ʱ����-1
 */
u16 TblInterface::findIndex(const char *name) {
	assert(m_table);
	for (u16 i = 0; i < m_tableDef->m_numIndice; i++)
		if (!strcmp(m_tableDef->m_indice[i]->m_name, name))
			return i;
	return (u16)-1;
}

/** ����ָ������������
 * @param idx �������
 * @return ��Ӧ����������
 */
IndexDef* TblInterface::getIndexDef(u16 idx) {
	assert(m_table);
	assert(idx < m_tableDef->m_numIndice);
	return m_tableDef->m_indice[idx];
}

/** ������
 * @param tableDef ����
 * @throw NtseException ����ʧ��
 */
void TblInterface::create(TableDef *tableDef) throw(NtseException) {
	assert(!m_table);
	loadConnection();
	m_db->createTable(m_session, m_path, tableDef);
}

/** �򿪱�
 * @throw NtseException ��ʧ��
 */
void TblInterface::open() throw(NtseException) {
	assert(!m_table);
	loadConnection();
	m_table = m_db->openTable(m_session, m_path);
	m_tableDef = m_table->getTableDef();
}

/** �رձ� */
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

/** �ضϱ�����
 * @throw NtseException ����ʧ��
 */
void TblInterface::truncate() throw(NtseException) {
	assert(m_table);
	loadConnection();
	m_db->truncateTable(m_session, m_table);
	m_tableDef = m_table->getTableDef();
}

/** ɾ��������ʱ�Զ��ر�
 * @throw NtseException
 */
void TblInterface::drop() throw(NtseException) {
	if (m_table)
		close();
	loadConnection();
	m_db->dropTable(m_session, m_path);
}

/** ��������
 * @param indexDef �����ӵ���������
 * @param numIndice ���ӵ���������
 * @throw NtseException ����ʧ��
 */
void TblInterface::addIndex(const IndexDef **indexDef, u16 numIndice) throw(NtseException) {
	assert(m_table);
	loadConnection();
	m_db->addIndex(m_session, m_table, numIndice, indexDef);
	m_tableDef = m_table->getTableDef();
}

/** ɾ������
 * @param idx Ҫɾ�����������
 * @throw NtseException ����ʧ��
 */
void TblInterface::dropIndex(u16 idx) throw(NtseException) {
	assert(m_table);
	assert(idx < m_tableDef->m_numIndice);
	loadConnection();
	m_db->dropIndex(m_session, m_table, idx);
	m_tableDef = m_table->getTableDef();
}

/** ��������
 * @param newPath �����·��
 * @throw NtseException �ļ�����ʧ��
 */
void TblInterface::rename(const char *newPath) throw(NtseException) {
	assert(!m_table);
	loadConnection();
	m_db->renameTable(m_session, m_path, newPath);
	delete []m_path;
	m_path = System::strdup(newPath);
}

/** OPTIMIZE��
 * @throw NtseException ����ʧ��
 */
void TblInterface::optimize() throw(NtseException) {
	assert(m_table);
	loadConnection();
	m_db->optimizeTable(m_session, m_table);
	m_tableDef = m_table->getTableDef();
}

/** ˢ������������ */
void TblInterface::flush() {
	assert(m_table);
	loadConnection();
	m_table->flush(m_session);
}

/** �����¼
 * @param cols �����б��ÿո�ָ�����ΪNULL���ʾ������������
 * @param ... ������ֵ
 * @throw NtseException �ӱ�����ʱ���¼����
 * @return �Ƿ�ɹ�������Ψһ��������ͻ���ܻ�ʧ��
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

/** ͨ������ɨ����¼�¼
 * @param cond ָ��Ҫ���µļ�¼��Χ
 * @param cols Ҫ���µ����������б��ÿո�ָ�����ΪNULL���ʾ������������
 * @param ... Ҫ���µ����Ե���ֵ
 * @throw NtseException ��¼����
 * @return �ɹ����µļ�¼��������ʱ������Ψһ��������ͻ���������жϣ������º�����¼
 */
u64 TblInterface::updateRows(const IdxRange cond, const char *cols, ...) throw(NtseException) {
	assert(m_table);
	va_list args;
	va_start(args, cols);
	u64 n = doUpdate(&cond, -1, 0, cols, args);
	va_end(args);
	return n;
}

/** ͨ������ɨ����¼�¼
 * @param cond ָ��Ҫ���µļ�¼��Χ
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @param cols Ҫ���µ����������б��ÿո�ָ�����ΪNULL���ʾ������������
 * @param ... Ҫ���µ����Ե���ֵ
 * @throw NtseException ��¼����
 * @return �ɹ����µļ�¼��������ʱ������Ψһ��������ͻ���������жϣ������º�����¼
 */
u64 TblInterface::updateRows(const IdxRange cond, int limit, uint offset, const char *cols, ...) throw(NtseException) {
	assert(m_table);
	va_list args;
	va_start(args, cols);
	u64 n = doUpdate(&cond, limit, offset, cols, args);
	va_end(args);
	return n;
}

/** ͨ����ɨ����¼�¼
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @param cols Ҫ���µ����������б��ÿո�ָ�����ΪNULL���ʾ������������
 * @param ... Ҫ���µ����Ե���ֵ
 * @throw NtseException ��¼����
 * @return �ɹ����µļ�¼��������ʱ������Ψһ��������ͻ���������жϣ������º�����¼
 */
u64 TblInterface::updateRows(int limit, uint offset, const char *cols, ...) throw(NtseException) {
	assert(m_table);
	va_list args;
	va_start(args, cols);
	u64 n = doUpdate(NULL, limit, offset, cols, args);
	va_end(args);
	return n;
}

/** ���¼�¼
 * @param cond ָ��Ҫ���µļ�¼��Χ����ΪNULL����б�ɨ�裬�����������ɨ��
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @param cols Ҫ���µ����������б��ÿո�ָ�����ΪNULL���ʾ������������
 * @param ... Ҫ���µ����Ե���ֵ
 * @throw NtseException ��¼����
 * @return �ɹ����µļ�¼��������ʱ������Ψһ��������ͻ���������жϣ������º�����¼
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

/** ͨ������ɨ��ɾ����¼
 * @param cond ָ��Ҫɾ���ļ�¼��Χ
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @return ɾ���ļ�¼��
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

/** ͨ����ɨ��ɾ����¼
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @return ɾ���ļ�¼��
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

/** ִ��ɾ������
 * @param h ɨ����
 * @param cond ����ɨ������������ΪNULL
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @return ɾ���ļ�¼��
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

/** ͨ������ɨ���ѯ��¼
 * @param cond ��ѯ����
 * @param cols Ҫ��ȡ�����������б��ÿո�ָ�����ΪNULL���ʾ�����������ԣ��������cond���漰������
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @return ��������������쳣����NULL
 */
ResultSet* TblInterface::selectRows(const IdxRange cond, const char *cols, int limit, uint offset) {
	assert(m_table);

	loadConnection();

	IndexScanCond isc = getIscFromIdxRange(cond);
	u16 numCols;
	u16 *cnos = parseCols(cols, &numCols);
	// Ϊ����ʵ�֣�Ҫ��Ҫ��ȡ�����Ա������cond���漰������
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

/** ͨ����ɨ���ѯ��¼
 * @param cols Ҫ��ȡ�����������б��ÿո�ָ�����ΪNULL���ʾ������������
 * @param limit LIMIT�Ӿ䣬����ָ��LIMIT������Ϊ<0
 * @param offset OFFSET�Ӿ�
 * @return �����
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

/** ִ��SELECT����
 * @param h ɨ�������п���������ɨ����ɨ��
 * @param numCols Ҫ��ȡ��������
 * @param cols Ҫ��ȡ�����Ա��
 * @param cond ѡ������������ΪNULL
 * @param limit LIMIT�Ӿ䣬<0��ʾ��ָ��LIMIT
 * @param offset OFFSET�Ӿ�
 * @return �����
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

/** ����һ������ɨ�跶Χ�ı�ݷ�ʽ
 * @param idx �������
 * @param numAttrs ��ʼ�������ֵ��������������ʹ�ô˽ӿڹ������ʼ�������ֵҪ��
 *   ������ͬ��������
 * @param includeLow Ϊtrue���>=low�ļ�¼��Ϊfalseֻ���>low�ļ�¼
 * @param includeHigh Ϊtrue���<=high�ļ�¼��Ϊfalseֻ���<high�ļ�¼
 * @param ... ��ʼ�������ֵ������ֵ��Ӧ�þ���2*numAttrs������
 * @return ����ɨ�跶Χ
 */
IdxRange TblInterface::buildRange(u16 idx, u16 numAttrs, bool includeLow, bool includeHigh, ...) {
	va_list args;
	va_start(args, includeHigh);
	IdxKey low(m_tableDef, idx, numAttrs, args);
	IdxKey high(m_tableDef, idx, numAttrs, args);
	va_end(args);
	return IdxRange(m_tableDef, idx, low, high, includeLow, includeHigh);
}

/** ����һ��ָֻ����ʼ��Χ������ɨ�跶Χ�ı�ݷ�ʽ
 * @param idx �������
 * @param numAttrs ��ʼ��ֵ������������
 * @param includeLow Ϊtrue���>=low�ļ�¼��Ϊfalseֻ���>low�ļ�¼
 * @param ... ��ʼ��ֵ������ֵ��Ӧ�þ���numAttrs������
 * @return ����ɨ�跶Χ
 */
IdxRange TblInterface::buildLRange(u16 idx, u16 numAttrs, bool includeLow, ...) {
	va_list args;
	va_start(args, includeLow);
	IdxKey low(m_tableDef, idx, numAttrs, args);
	IdxKey high(m_tableDef, idx, 0);
	va_end(args);
	return IdxRange(m_tableDef, idx, low, high, includeLow, true);
}

/** ����һ��ָֻ��������Χ������ɨ�跶Χ�ı�ݷ�ʽ
 * @param idx �������
 * @param numAttrs ������ֵ������������
 * @param includeHigh Ϊtrue���<=high�ļ�¼��Ϊfalseֻ���<high�ļ�¼
 * @param ... ������ֵ������ֵ��Ӧ�þ���numAttrs������
 * @return ����ɨ�跶Χ
 */
IdxRange TblInterface::buildHRange(u16 idx, u16 numAttrs, bool includeHigh, ...) {
	va_list args;
	va_start(args, includeHigh);
	IdxKey low(m_tableDef, idx, 0);
	IdxKey high(m_tableDef, idx, numAttrs, args);
	va_end(args);
	return IdxRange(m_tableDef, idx, low, high, true, includeHigh);
}

/** ����
 *
 * @param hasLob ���Ƿ���������
 * @param dir ���ݵ����Ŀ¼����ΪNULL�򱸷ݵ����ݿ��basedir��
 * @return ���ݽ�����ļ�����������׺
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

/** �ָ�����
 * @pre ������Ѿ����ر�
 *
 * @param bakPath ���ݽ�����ļ���
 * @param hasLob ���Ƿ���������
 * @param toPath �ָ������·������ΪNULL��������ݿ�basedir����
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

/** �����������ļ�
 * @param from Դ·��
 * @param to Ŀ��·��
 * @param hasLob ���Ƿ���������
 * @param overrideExist �Ƿ񸲸��Ѿ����ڵ��ļ�
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

/** ����������־���������ᱸ��ָ��Ŀ¼��������Ϊntse_log.xxx���ļ�
 *
 * @param fromDir �����Ŀ¼����
 * @param toDir ���ݵ����Ŀ¼��
 * @return ����·��
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

/** �ָ����ݵ�������־
 *
 * @param bakPath backupTxnlogs�������صı���·��
 * @param toDir �ָ������Ŀ¼
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

/** �����ݿⱻ�ر����´�֮������
 * @param db ���ݿ�
 */
void TblInterface::reconnect(Database *db) {
	m_db = db;
	m_table = NULL;
	m_tableDef = NULL;
	m_conn = NULL;
	m_session = NULL;
}

/** ��֤������һ�� */
void TblInterface::verify() {
	assert(m_table);
	loadConnection();
	m_table->verify(m_session);
}

/** ���м���
 * @throw NtseException ���㲻�ɹ�
 */
void TblInterface::checkpoint() throw(NtseException) {
	loadConnection();
	m_db->checkpoint(m_session);
}

/** ����REC_MYSQL��ʽ�ļ�¼
 * @param cols ���������������б��ո�ָ���NULL��ʾ��������
 * @param ... �����Ե�ֵ
 */
byte* TblInterface::buildMysqlRow(const char *cols, ...) {
	va_list args;
	va_start(args, cols);
	byte *r = buildMysqlRow(cols, args);
	va_end(args);
	return r;
}

/** ����REC_MYSQL��ʽ�ļ�¼
 * @param cols ���������������б��ո�ָ���NULL��ʾ��������
 * @param values �����Ե�ֵ
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

/** �ͷ����ݿ�������Ự */
void TblInterface::freeConnection() {
	if (m_conn) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_conn = NULL;
		m_session = NULL;
	}
}

/** �жϼ�¼�Ƿ���ָ���ķ�Χ֮��
 * @param buf ��¼����
 * @param cond ����
 * @return ��¼�Ƿ���ָ����Χ֮��
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

/** �Ƚ�һ����¼��һ����
 * @param buf ��¼
 * @param key ��ֵ������ΪKEY_PAD��ʽ
 * @return buf > key����>0��buf = key����0�����򷵻�<0
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

/** �ж�subset�Ƿ�ΪsuperSet���Ӽ�
 * @param subSize subset��С
 * @param subset subset����
 * @param superSize superSet��С
 * @param superSet superSet����
 * @return subset�Ƿ�ΪsuperSet�Ӽ�
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
