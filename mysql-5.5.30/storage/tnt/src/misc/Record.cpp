/**
 * ��¼�ı�ʾ�Ͳ���
 *
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */

#include <assert.h>
#include <vector>
#include <limits>
#include <sstream>
#include <algorithm>
#include "misc/Record.h"
#include "misc/RecordHelper.h"
#include "misc/Trace.h"
#include "api/Table.h"
#include "util/Bitmap.h"
#include "util/NumberCompress.h"
#include "lob/Lob.h"

#ifdef TNT_ENGINE
#include "misc/Syslog.h"
#endif

using namespace std;

namespace ntse {

class Column;

static bool isUppMysqlFormat(RecFormat format);
static bool isRedundantFormat(RecFormat format);
static u16 calcNullableCols(const TableDef* tableDef, const SubRecord* sb);
static size_t getColSize(const ColumnDef *col, const byte *buf);
static size_t getKeyColSize(const ColumnDef *col, const byte *buf, u32 prefixLen);
static inline size_t getUppMysqlColSize(const ColumnDef* col, const byte* buf);
static int compareColumn(Column *col1, Column *col2, bool sndCompessed = false, bool cmpLob = false, bool cmpLobAsVarchar = false);
static bool isCompressableNumberType(ColumnType type);
static void doUpdateRecordVRInPlace(const TableDef *tableDef, Record *record,
							const SubRecord *update, size_t oldBufferSize);
static byte* parseLobColumn(const Column *column, size_t *lobSize);
inline size_t calcBitmapBytes(RecFormat format, u16 nullableCols) {
	return format == REC_FIXLEN  ? (nullableCols + 8) / 8 : (nullableCols + 7) / 8;
}
static bool isFixedLen(ColumnType type);
static bool isLob(ColumnType type);
static size_t getRealSizeForPrefixIndex(CollType collation, u32 prefixLen, u32 dataLen, const char *str);



/**
* ��С�˷�ʽ������д�뻺��
* @param value ��д������
* @param buf �������
* @param lenBytes
*/
inline  void writeU32Le(u32 value, byte *buf, size_t lenBytes) {
	switch(lenBytes) {
		case 1:
			buf[0] = (byte)value;
			break;
		case 2:
			write2BytesLittleEndian(buf, value);
			break;
		default:
			assert(false);
	}
}
/** ��С�˷�ʽ��ȡ���� */
inline u32 readU32Le(const byte* buf, size_t lenBytes) {
	switch(lenBytes) {
		case 1:
			return buf[0];
		case 2:
			return read2BytesLittleEndian(buf);
		default:
			assert(false);
			return 0;
	}
}


static bool isKeyFormat(RecFormat format);
static bool isRecFormat(RecFormat formart);


/**
 * ��:
 *	ά���е���Ϣ
 */
class Column {
	friend class Row;
	friend class RandAccessRow;
	friend class MysqlRow;
	friend class CompressedKeyRow;
	friend class ColGroupRow;

public:
	Column() {};
	Column(byte *data, size_t cap)
		: m_buf(data), m_capacity(cap) {

	}
	inline void setBuf(byte *buf, size_t cap) {
		m_buf = buf;
		m_capacity = cap;
	}
	inline byte*  data() const {
		return m_buf;
	}
	inline byte*  data() {
		return m_buf;
	}
	inline size_t size() const {
		return m_size;
	}
	inline size_t capacity() const {
		return m_capacity;
	}
	inline u16 colNo() const {
		return m_colNo;
	}
	inline bool nullable() const {
		return m_columnDef->m_nullable;
	}
	inline u16 bitmapOffset() const {
		return m_columnDef->m_nullBitmapOffset;
	}
	inline bool isNull() const {
		return m_isNull;
	}
	inline const ColumnDef* def() const {
		return m_columnDef;
	}
	inline u8 colGrpNo() const {
		return m_columnDef->m_colGrpNo;
	}
	inline u16 colGrpOffset() const {
		return m_columnDef->m_colGrpOffset;
	}
	inline byte lenBytes() const {
		return m_lenBytes;
	}

private:
	ColumnDef		*m_columnDef;	/** �ж���       */
	byte			*m_buf;			/** ���ݵ�ַ     */
	size_t			m_size;			/** �д�С       */
	size_t			m_capacity;		/** ���ݻ����С */
	u16				m_colNo;		/** �����       */
	bool			m_isNull;		/** �Ƿ�Ϊnull   */
	byte			m_lenBytes;		/** ��ʾvarchar/lob���ͳ�����Ҫ���ֽ��� */
};



class VSubRecordIterator;
/**
 * �У�
 *	�����¼���ڴ�ռ�
 *	�ṩ�б����Ͳ�������
 */
class Row {
	friend class VSubRecordIterator;
public:
	Row() {}
	virtual ~Row() {}
	/**
	 * ����һ��
	 *
	 * @param tableDef	����
	 * @param buf		��¼ռ���ڴ�
	 * @param capacity  buf����
	 * @param size		��¼ʵ��ռ���ڴ�
	 */
	Row(const TableDef *tableDef, byte *buf, size_t capacity, size_t size) {
		init(tableDef, buf, capacity, size, tableDef->m_nullableCols, tableDef->m_recFormat);
	}
	/**
	 * ����һ��
	 *
	 * @param tableDef	����
	 * @param buf		��¼ռ���ڴ�
	 * @param capacity  buf����
	 * @param size		��¼ʵ��ռ���ڴ�
	 * @param format	��¼��ʽ
	 */
	Row(const TableDef *tableDef, byte *buf, size_t capacity, size_t size, RecFormat format) {
		init(tableDef, buf, capacity, size, tableDef->m_nullableCols, format);
	}
	/**
	 * ���ݼ�¼����һ��
	 *
	 * @param tableDef	����
	 * @param record	��¼
	 */
	Row(const TableDef *tableDef, const Record *record) {
		init(tableDef, record->m_data, record->m_size, record->m_size
			, tableDef->m_nullableCols, record->m_format);
	}

	/** ��ȡ��һ�� */
	Column* firstColumn(Column *column) const {
		return firstColumn(column, 0);
	}
	/**
	 * ��ȡ��һ��
	 * @param cur ��ǰ��
	 * @return ����cur
	 */
	Column* nextColumn(Column* cur) const {
		++cur->m_colNo;
		return (cur->m_colNo >= m_tableDef->m_numCols) ? NULL : nextColumn(cur, cur->m_colNo);
	}

	/**
	 * ��ȡ�������������еĵ�һ��
	 * @param column ���ڴ洢�����
	 * @param return ����column
	 */
	Column* firstColAcdColGrp(Column *column) const {
		assert(column);
		assert(m_tableDef->m_colGrps);	
		return firstColumn(column, m_tableDef->m_colGrps[0]->m_colNos[0]);
	}

	/**
	 * ��ȡ�������������е���һ��
	 * @param cur ��ǰ��
	 * @return ����cur
	 */
	Column* nextColAcdColGrp(Column *cur) const {
		assert(cur);
		assert(m_tableDef->m_colGrps);
		u8 colGrpNo = cur->colGrpNo();
		u16 nextOffset = cur->colGrpOffset() + 1;
		if (nextOffset >= m_tableDef->m_colGrps[colGrpNo]->m_numCols) {
			colGrpNo++;
			if (colGrpNo >= m_tableDef->m_numColGrps)
				return NULL;
			nextOffset = 0;
		}
		u16 colNo = m_tableDef->m_colGrps[colGrpNo]->m_colNos[nextOffset];
		return nextColumn(cur, colNo);
	}

	/**
	 * ��һ�е���ĩβ
	 * @param column ��������
	 */
	virtual void appendColumn(const Column* column) {
		assert(column->def() == m_tableDef->m_columns[column->colNo()]);
		appendColumn(column->def()->m_nullBitmapOffset, column);
	}


	/** ��ȡ��¼��С */
	inline size_t size() const {
		return m_size;
	}
	/** null λͼ�ֽ��� */
	inline size_t bitmapBytes() const {
		return m_bmBytes;
	}

protected:
	/**
	 * ��ʼ������
	 *
	 * @param tableDef	����
	 * @param buf		��¼ռ���ڴ�
	 * @param capacity  buf����
	 * @param size		��¼ʵ��ռ���ڴ�
	 * @param nullableCols nullable����Ŀ
	 * @param format ��¼��ʽ
	 */
	inline void init(const TableDef *tableDef, byte* buf, size_t capacity,
		size_t size, u16 nullableColls, RecFormat format) {
		assert(size <= capacity);

		m_tableDef = tableDef;
		m_buf = buf;
		m_capacity = capacity;
		m_size = size;
		m_format = format;
		if (isRecFormat(format))
			m_bmBytes = tableDef->m_bmBytes;
		else
			m_bmBytes = calcBitmapBytes(format, nullableColls);
		if (0 == m_size) { // ��ʼ��Ϊ�ռ�¼
			// Ԥ��λͼ�洢�ռ�
			m_size += m_bmBytes;
			// ��ʼ��λͼ������valgrind����
			memset(m_buf, 0, m_bmBytes);
		} else { // �ǿռ�¼
			// ���ٱ�֤λͼ�Ĵ洢�ռ�
			assert(m_size >= m_bmBytes);
		}
	}

	/**
	 * ��ʼ����
	 * @param column ����ʼ���ж���
	 * @param columnDef �ж���
	 * @param colNo �к�
	 * @param bmOffset nullλͼƫ��
	 * @param buf ��������ʼ��ַ
	 * @param size �����ݳ���
	 * @param capacity ��ʵ��ռ���ڴ�ռ�
	 * @return ����column
	 */
	inline Column* initColumn(Column* column, ColumnDef *columnDef,
		u16 colNo, bool isNull, byte* buf, byte lenBytes, size_t size, size_t capacity) const {
		// REC_REDUNDANT��ʽ�Ӽ�¼�ͼ�¼ռ����ͬ��С�ڴ棬��m_sizeû��ʵ������
		assert((m_format != REC_REDUNDANT && buf <= m_buf + m_size)
			|| (m_format == REC_REDUNDANT && buf <= m_buf + m_capacity));
		assert(colNo < m_tableDef->m_numCols);
		assert(columnDef == m_tableDef->m_columns[colNo]);

		column->m_colNo = colNo;
		column->m_columnDef = columnDef;
		column->m_buf = buf;
		column->m_isNull = isNull;
		column->m_lenBytes = lenBytes;
		column->m_size = size;
		column->m_capacity = capacity;
		// REC_REDUNDANT��ʽ�Ӽ�¼�ͼ�¼ռ����ͬ��С�ڴ棬��m_sizeû��ʵ������
		assert(column->m_size + column->m_buf <= m_buf + (isRecFormat(m_format) || isUppMysqlFormat(m_format) ? m_capacity : m_size));
		return column;
	}

	/**
	 * ��ȡ��һ��,ָ����һ�е��к�
	 *	��ǰ����һ��SubRecordʱ��Ӧ�ñ�����
	 * @param column ����ʼ����
	 * @param firstColNo ��һ���к�
	 */
	Column* firstColumn(Column* column, u16 firstColNo) const {
		assert(firstColNo < m_tableDef->m_numCols);
		ColumnDef *columnDef = m_tableDef->m_columns[firstColNo];
		byte* data = m_buf + m_bmBytes;
		size_t size = getColSize(columnDef, columnDef->m_nullBitmapOffset, data);
		size_t capacity = 0;
		byte lenBytes = columnDef->m_lenBytes;
		if (isUppMysqlFormat(m_format)) {
			capacity = columnDef->m_mysqlSize;
			if (columnDef->isLongVar())
				lenBytes = 2;
		} else if (isRedundantFormat(m_format)) {
			capacity = columnDef->m_size;
		} else {
			capacity = size;
		}
		return initColumn(column, columnDef, firstColNo, size == 0, data, lenBytes, size, capacity);
	}
	/**
	 * ��ȡ��һ�У�ָ����һ�е��к�
	 *	��ǰ����һ��SubRecordʱ��Ӧ�ñ�����
	 * @param cur ��ǰ��
	 * @param nextColNo ��һ���к�
	 * @return ����cur
	 */
	Column* nextColumn(Column* cur, u16 nextColNo) const {
		assert(nextColNo < m_tableDef->m_numCols);
		bool isRedundat = isRedundantFormat(m_format);
		bool isUpperMysql = isUppMysqlFormat (m_format);
		byte *data = cur->data();
		data += isRedundat || isUpperMysql ? cur->capacity() : cur->size();
		ColumnDef *columnDef = m_tableDef->m_columns[nextColNo];
		if (data >= m_buf + m_capacity) { // �洢�ռ��ѵ�ĩβ��ֻ����NULL��
			if (nextColNo < m_tableDef->m_numCols) { // ʣ�µ��ж���NULL
				assert(isColNull(columnDef->m_nullBitmapOffset));
			} else {
				return 0;
			}
		}
		size_t size = getColSize(columnDef, columnDef->m_nullBitmapOffset, data);
		size_t capacity = 0;
		byte lenBytes = columnDef->m_lenBytes;
		if (isUppMysqlFormat(m_format)) {
			capacity = columnDef->m_mysqlSize;
			if (columnDef->isLongVar())
				lenBytes = 2;
		} else if (isRedundantFormat(m_format)) {
			capacity = columnDef->m_size;
		} else {
			capacity = size;
		}
		return initColumn(cur, columnDef, nextColNo, size == 0, data, lenBytes, size, capacity);
	}

	/**
	 * ��һ�е���ĩβ
	 * @param NULLλͼƫ��
	 * @param column ��������
	 */
	void appendColumn(u16 bmOffset, const Column* column) {
		assert(column->colNo() < m_tableDef->m_numCols);
		if (column->m_isNull) {
			assert(column->nullable());
			setColNull(bmOffset);
			if (isUppMysqlFormat(m_format))
				m_size += column->def()->m_mysqlSize;
			else if (isRedundantFormat(m_format))
				m_size += column->def()->m_size;
		} else if (m_capacity >= m_size + column->m_size) {
			if (column->nullable())
				setColNotNull(bmOffset);
			memcpy(m_buf + m_size, column->m_buf, column->m_size);
			if (isUppMysqlFormat(m_format))
				m_size += column->def()->m_mysqlSize;
			else if (isRedundantFormat(m_format))
				m_size += column->def()->m_size;
			else 
				m_size += column->m_size;
		} else { // �ڴ治��
			assert(false);
		}
	}

	/**
	 * ��ȡ�д�С
	 * @param columnDef �ж���
	 * @param bmOffset NULLλͼƫ��
	 * @param buf	��������ʼ��ַ
	 */
	inline size_t getColSize(ColumnDef *columnDef, u16 bmOffset, byte *buf) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		return ntse::getColSize(columnDef, buf);
	}
	/** �ж����Ƿ�Ϊnull */
	inline bool isColNull(u16 offset) const {
		return BitmapOper::isSet(m_buf, (uint)m_bmBytes << 3, offset);
	}
	/** ������Ϊnull */
	inline void setColNull(u16 offset) {
		BitmapOper::setBit(m_buf, (uint)m_bmBytes << 3, offset);
	}
	/** ������Ϊ��null */
	inline void setColNotNull(u16 offset) {
		BitmapOper::clearBit(m_buf, (uint)m_bmBytes << 3, offset);
	}

protected:
	const TableDef	*m_tableDef;	/** ����     */
	byte			*m_buf;			/** ���ݻ���   */
	size_t			m_capacity;		/** �����С   */
	size_t			m_size;			/** ʵ�ʴ�С   */
	size_t			m_bmBytes;		/** λͼ�ֽ��� */
	RecFormat		m_format;		/** �и�ʽ     */
};

class VSubRecordIterator {
public:
	VSubRecordIterator(Row *row, const SubRecord *sr)
		:  m_numCols(sr->m_numCols), m_colNos(sr->m_columns)
		, m_lastColNoIdx((u16)-1), m_row(row) {
			assert(sr->m_format == REC_VARLEN);
	}

	inline Column* column() {
		assert(m_lastColNoIdx != (u16)-1);
		assert(m_lastColNoIdx < m_numCols);
		return &m_column;
	}

	VSubRecordIterator*  first() {
		m_lastColNoIdx = 0;
		u16 colNo = m_colNos[m_lastColNoIdx];
		Column* col = m_row->firstColumn(&m_column, colNo);
		UNREFERENCED_PARAMETER(col);
		assert(col);
		return this;
	}


	VSubRecordIterator*  next() {
		++m_lastColNoIdx;
		if (m_lastColNoIdx < m_numCols) {
			Column* col = m_row->nextColumn(&m_column, m_colNos[m_lastColNoIdx]);
			UNREFERENCED_PARAMETER(col);
			assert(col);
			return this;
		} else if (m_lastColNoIdx == m_numCols) { // �Ѿ��������һ��
			return 0;
		} else {
			assert(false);
			return 0;
		}
	}

	inline bool end() const {
		return m_lastColNoIdx >= m_numCols;
	}

private:
	u16			m_numCols;
	const u16	*m_colNos;
	u16			m_lastColNoIdx;	/** Index into m_colNos */
	Column		m_column;
	const Row *m_row;
};

/** �������� */
class KeyRow : public Row {
public:
	/** ���ڱ���������(��¼����ΪKEY_PAD, KEY_COMPRESSED�� KEY_NATURAL)�е��� */
	class Iterator {
	public:
		Iterator(KeyRow *row, const SubRecord *key)
			: m_numCols(key->m_numCols), m_colNos(key->m_columns)
			, m_lastColNoIdx((u16)-1), m_bitmapOffset(0), m_row(row) {
			assert(isKeyFormat(key->m_format));
		}

		inline Column* column() {
			assert(m_lastColNoIdx != (u16)-1);
			assert(m_lastColNoIdx < m_numCols);
			return &m_column;
		}

		Iterator*  first() {
			m_lastColNoIdx = 0;
			u16 colNo = m_colNos[m_lastColNoIdx];
			Column* col = m_row->firstColumn(&m_column, colNo, m_bitmapOffset, m_row->m_indexDef->m_prefixLens[m_lastColNoIdx]);
			assert(col);
			if (col->def()->m_nullable)
				++m_bitmapOffset;
			return this;
		}


		Iterator*  next() {
			++m_lastColNoIdx;
			if (m_lastColNoIdx < m_numCols) {
				Column* col = m_row->nextColumn(&m_column, m_colNos[m_lastColNoIdx], m_bitmapOffset, m_row->m_indexDef->m_prefixLens[m_lastColNoIdx]);
				assert(col);
				if (col->def()->m_nullable)
					++m_bitmapOffset;
				return this;
			} else if (m_lastColNoIdx == m_numCols) { // �Ѿ��������һ��
				return 0;
			} else {
				assert(false);
				return 0;
			}
		}

		inline bool end() const {
			return m_lastColNoIdx >= m_numCols;
		}

	private:
		u16			m_numCols;
		const u16	*m_colNos;
		u16			m_lastColNoIdx;	/** Index into m_colNos */
		u16 		m_bitmapOffset;	/** Index into null bitmap */
		Column		m_column;
		const KeyRow *m_row;
	};

	KeyRow() {}
	KeyRow(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord* key, bool empty = false) {
		assert(isKeyFormat(key->m_format));
		m_nullableCols = calcNullableCols(tableDef, key);
		m_curNullable = 0;
		m_indexDef = indexDef;
		size_t size = empty ? 0 :  key->m_size;
		init(tableDef, key->m_data, key->m_size, size, m_nullableCols, key->m_format);
		m_format = key->m_format;
	}

	/**
	 * ��һ�е���ĩβ
	 * @param column ��������
	 * @param prefixLen ��ǰ׺����
	 * @param lobPair ����������Ҫ�Ĵ�������
	 */
	virtual void appendColumn(const Column* column, u32 prefixLen, LobPair *lobPair) {
		assert(!column->nullable() || m_curNullable < m_nullableCols);

		assert(column->colNo() < m_tableDef->m_numCols);
		u32 colDefSize = column->def()->m_size; // columnDef �������Ĵ�С�����������ƴװǰ׺�У��������ֽ�(1/2)����prefixLen
		u32 columnSize = column->size();	//column�����Ĵ�С������Ǳ䳤�ֶ�����Ҫ����column����ǰ׺��ȡ���������С,����Ƕ����ֶΣ����ǿ��ǵ�ǰ׺������󳤶� 

		size_t curSize = 0;
		size_t realSize = 0;
		size_t lenBytes = column->def()->m_lenBytes;
		if (prefixLen > 0) {
			assert(column->def()->m_type == CT_VARCHAR || column->def()->m_type == CT_VARBINARY ||
				column->def()->m_type == CT_CHAR || column->def()->m_type == CT_BINARY ||
				column->def()->m_type == CT_SMALLLOB || column->def()->m_type == CT_MEDIUMLOB);

			if (column->def()->isLob()) {
				lenBytes = prefixLen > 255 ? 2 : 1;
				if (lobPair) {
					// �����Redundant��ʽת��Key��ʽ����Ҫ�Ӵ����������ж�ȡ���������
					curSize = lobPair->m_size;
					realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(lobPair->m_lob));
					colDefSize = prefixLen + lenBytes;
					columnSize  = lenBytes + realSize;
				} else {
					// �����Key��ʽ֮���ת��������Ҫ�Ӵ���������
					curSize = readU32Le(column->data(), lenBytes);
					realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(column->data() + lenBytes));
					colDefSize = prefixLen + lenBytes;
					columnSize = realSize + lenBytes;
				}
			} else {
				curSize = column->def()->isFixlen()? column->def()->m_size: readU32Le(column->data(), lenBytes);
				realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(column->data() + lenBytes));
				colDefSize = prefixLen + lenBytes;
				columnSize = column->def()->isFixlen()? prefixLen: realSize + lenBytes;
			}
		}
		if (column->isNull()) {
			assert(column->nullable());
			setColNull(m_curNullable);
			if (isRedundantFormat(m_format))
				m_size += colDefSize;
		} else if (m_capacity >= m_size + columnSize) {
			if (column->nullable())
				setColNotNull(m_curNullable);

			if (column->def()->isLob() && lobPair) {
				memcpy(m_buf + m_size + lenBytes, lobPair->m_lob, realSize);
				writeU32Le(realSize, m_buf + m_size, lenBytes);
			} else {
				memcpy(m_buf + m_size, column->data(), columnSize);
				if (prefixLen > 0) {	
					if (column->def()->isFixlen()) {
						int value = 0x0;
						if (column->def()->m_type == CT_CHAR) {
							value = 0x20;
						} else {
							assert(column->def()->m_type == CT_BINARY);
							value = 0x0;
						}				
						memset(m_buf + m_size + realSize + lenBytes, value, columnSize - realSize - lenBytes);
					} else
						writeU32Le(realSize, m_buf + m_size, lenBytes);	
				}
			}
			m_size += isRedundantFormat(m_format) ? colDefSize : columnSize;
		} else { // �ڴ治��
			assert(false);
		}
		if (column->nullable())
			++m_curNullable;
	}
protected:
	/**
	 * ��ȡ��һ��,ָ��������Ϣ
	 *	��ǰ����һ��KeyRowʱ�����ñ�����
	 * @param column ����ʼ����
	 * @param firstColNo ��һ���к�
	 * @param bmOffset Nullλͼƫ��
	 * @param prefixLen ǰ׺����
	 */
	Column* firstColumn(Column* column, u16 firstColNo, u16 bmOffset, u32 prefixLen) const {
		assert(firstColNo < m_tableDef->m_numCols);
		ColumnDef *columnDef = m_tableDef->m_columns[firstColNo];
		byte* data = m_buf + m_bmBytes;
		size_t size = getKeyColSize(columnDef, bmOffset, data, prefixLen);
		size_t columnSize = columnDef->m_size;
		byte lenBytes = columnDef->m_lenBytes;
		if (columnDef->isLob()) {
			lenBytes = prefixLen > 255 ? 2 : 1;
		}
		if (prefixLen > 0)
			columnSize = prefixLen + lenBytes;
		size_t capacity = isRedundantFormat(m_format) ? columnSize : size;


		return initColumn(column, columnDef, firstColNo, size == 0, data, lenBytes, size, capacity);
	}
	/**
	 * ��ȡ��һ�У�ָ��������Ϣ
	 *	��ǰ����һ��KeyRowʱ��Ӧ�ñ�����
	 * @param cur ��ǰ��
	 * @param nextColNo ��һ���к�
	 * @param bmOffset nullλͼƫ��
	 * @param prefixLen ǰ׺����
	 * @return ����cur
	 */
	Column* nextColumn(Column* cur, u16 nextColNo, u16 bmOffset, u32 prefixLen) const {
		assert(nextColNo < m_tableDef->m_numCols);
		bool isRedundat = isRedundantFormat(m_format);
		byte *data = cur->data() + (isRedundat ?  cur->capacity() : cur->size());
		if (data >= m_buf + m_capacity) { // �洢�ռ��ѵ�ĩβ��ֻ����NULL��
			if (nextColNo < m_tableDef->m_numCols) { // ʣ�µ��ж���NULL
				assert(isColNull(bmOffset));
			} else {
				return 0;
			}
		}
		ColumnDef *columnDef = m_tableDef->m_columns[nextColNo];
		size_t size = getKeyColSize(columnDef, bmOffset, data, prefixLen);
		size_t columnSize = columnDef->m_size;
		byte lenBytes = columnDef->m_lenBytes;
		if (columnDef->isLob())
			lenBytes = prefixLen > 255 ? 2 : 1;
		if (prefixLen > 0)
			columnSize = prefixLen + lenBytes;

		size_t capacity = isRedundat ? columnSize : size;

		return initColumn(cur, columnDef, nextColNo, size == 0, data, lenBytes, size, capacity);
	}

	/**
	* ��ȡ�д�С
	* @param columnDef �ж���
	* @param bmOffset NULLλͼƫ��
	* @param buf	��������ʼ��ַ
	* @param prefixLen ǰ׺����
	*/
	virtual size_t getKeyColSize(ColumnDef *columnDef, u16 bmOffset, byte *buf, u32 prefixLen) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		return ntse::getKeyColSize(columnDef, buf, prefixLen);
	}

protected:
	const IndexDef	*m_indexDef;	/** �������� */
	u16				m_nullableCols;	/** nullable������   */
	u16				m_curNullable;	/** ��ǰnullable��Ŀ */
};




/**
 * ֧������ǰ׺ѹ�����У� ��������
 *	���ý���Nullλͼ
 */
class CompressedKeyRow: public KeyRow {

public:
	CompressedKeyRow() {}

	/**
	 * ����һ��ѹ����
	 * @param tableDef ����
	 * @param indexDef ��������
	 * @param key �����Ӽ�¼
	 */
	CompressedKeyRow(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord* key, bool empty = false)
		: KeyRow(tableDef, indexDef, key, empty) {
			assert(key->m_format == KEY_COMPRESS);
	}


	/**
	 * ��һ�е���ĩβ
	 * @param column ��������
	 * @param prefixLen ǰ׺����
	 * @param lobPair ������������Ҫ�Ĵ����
	 */
	virtual void appendColumn(const Column* column, u32 prefixLen, LobPair *lobPair) {
		assert(column->colNo() < m_tableDef->m_numCols);
		// �ռ�һ��Ҫ�㹻�����û��ǰ׺ m_capacity >= m_size + column->size()
		// ���� m_capacity >= m_size >= m_size + prefixLen + lenBytes
		if (column->isNull()) {
			setColNull(m_curNullable);
		} else if (!isCompressableNumberType(column->def()->m_type)) { // ��������������
			if (column->nullable())
				setColNotNull(m_curNullable);
			size_t copySize = column->size();
			if (prefixLen > 0) { // �����ǰ׺������
				assert(column->def()->m_type == CT_VARCHAR || column->def()->m_type == CT_VARBINARY || column->def()->m_type == CT_CHAR ||
					column->def()->m_type == CT_BINARY || column->def()->m_type == CT_SMALLLOB || column->def()->m_type == CT_MEDIUMLOB);

				size_t lenBytes = column->def()->m_lenBytes;
				size_t curSize = 0;
				size_t realSize = 0;

				if (column->def()->isLob()) {
					lenBytes = prefixLen > 255 ? 2 : 1;
					if (lobPair) {
						//�����Redundant��ʽת����KEY��ʽ����Ҫ�Ӵ����������ж�ȡ��������������
						curSize = lobPair->m_size;
						realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(lobPair->m_lob));
						copySize  = lenBytes + realSize;

						memcpy(m_buf + m_size + lenBytes, lobPair->m_lob, realSize);
						writeU32Le(realSize, m_buf + m_size, lenBytes);
					} else {
						//�����KEY��ʽ֮���ת��������Ҫ�Ӵ����������ж�ȡ���������
						curSize = readU32Le(column->data(), lenBytes);
						realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(column->data() + lenBytes));
						copySize = realSize + lenBytes;
						memcpy(m_buf + m_size, column->data(), copySize);
						writeU32Le(realSize, m_buf + m_size, lenBytes);
					}
				} else {
					// �Ǵ�����У������ͱ䳤
					// ���ڶ����ֶ���˵����ǰ���Ⱦ͵����ж���ĳ���
					// ���ڱ䳤�ֶ���˵����ǰ�����Ǽ��ڼ�¼ͷ���ĳ���
					curSize = column->def()->isFixlen()? column->def()->m_size: readU32Le(column->data(), lenBytes);
					realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(column->data() + lenBytes));
					copySize = column->def()->isFixlen()? prefixLen: realSize + lenBytes;
					u32 columnSize = column->def()->isFixlen()? prefixLen: realSize + lenBytes;
					memcpy(m_buf + m_size, column->data(), copySize);
					// �����ֶ���Ҫ�޸��ֶ�β��Ϊ 0x20������0x0, �䳤�ֶ���Ҫ�޸�ͷ���ĳ�����Ϣ
					if (column->def()->isFixlen()) {
						int value = 0x0;
						if (column->def()->m_type == CT_CHAR) {
							value = 0x20;
						} else {
							assert(column->def()->m_type == CT_BINARY);
							value = 0x0;
						}				
						memset(m_buf + m_size + realSize + lenBytes, value, columnSize - realSize - lenBytes);
					} else {
						writeU32Le(realSize, m_buf + m_size, lenBytes);
					}
				}
			} else
				memcpy(m_buf + m_size, column->data(), copySize);
			m_size += copySize;
		} else { // �������ͣ�ѹ����
			if (column->nullable())
				setColNotNull(m_curNullable);
			assert(column->size() <= 8); // ������󳤶�Ϊ8
			size_t compSize = 0; // ѹ�����ֽ���
			bool succ = NumberCompressor::compress(column->data(), column->size()
				, m_buf + m_size, m_capacity - m_size, &compSize);
			UNREFERENCED_PARAMETER(succ);
			assert(succ);
			m_size += compSize;
		}
		if (column->nullable())
			++ m_curNullable;
	}
	/**
	 * ��ѹ�У������߱�֤dest���ڴ�
	 * @return dest;
	 */
	static Column* decompressColumn(const Column *src, Column *dest) {
		dest->m_colNo = src->m_colNo;
		dest->m_columnDef = src->m_columnDef;
		dest->m_isNull = src->isNull();
		if (!src->isNull() && isCompressableNumberType(src->def()->m_type)) {
			dest->m_size = dest->def()->m_size;
			assert(dest->m_size <= dest->m_capacity);
			NumberCompressor::decompress(src->data(), src->size(), dest->data(), dest->size());
		} else { // �����ѹ��ֱ����src�ڴ�
			dest->m_buf = src->m_buf;
			dest->m_size = src->m_size;
		}
		return dest;
	}

protected:
	/**
	 * ��ȡ�д�С
	 * @param columnDef �ж���
	 * @param bmOffset NULLλͼƫ��
	 * @param buf	��������ʼ��ַ
	 * @param prefixLen ǰ׺����
	 */
	virtual size_t getKeyColSize(ColumnDef *columnDef, u16 bmOffset, byte *buf, u32 prefixLen) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		if (isCompressableNumberType(columnDef->m_type)) {
			return NumberCompressor::sizeOfCompressed(buf);
		} else {
			return ntse::getKeyColSize(columnDef, buf, prefixLen);
		}
	}
};

/**
 * ���������
 *	���ܹ������λ
 */
class RandAccessRow: public Row {
public:
	RandAccessRow(bool empty)
		: m_isNewRow(empty) {	}

	RandAccessRow(const TableDef *tableDef, const Record *record)
		: Row(tableDef, record), m_isNewRow(false) {
		assert(record->m_format == REC_FIXLEN || record->m_format == REC_REDUNDANT || record->m_format == REC_MYSQL);
		assert(record->m_size == tableDef->m_maxRecSize);
	}

	/**
	 * ����������ʵ��Ӽ�¼
	 */
	RandAccessRow(const TableDef *tableDef, const SubRecord *sr)
		:  m_isNewRow(false) {
		assert(sr->m_format == REC_FIXLEN || sr->m_format == REC_REDUNDANT || sr->m_format == REC_MYSQL);
		assert(sr->m_size == tableDef->m_maxRecSize);
		init(tableDef, sr->m_data, tableDef->m_maxRecSize, tableDef->m_maxRecSize, tableDef->m_nullableCols, sr->m_format);
	}

	/** �����ȡ�� */
	inline virtual Column* columnAt(u16 colNo, Column* column) const {
		assert(colNo <= m_tableDef->m_numCols);
		ColumnDef *def = m_tableDef->m_columns[colNo];
		byte *data = m_buf + def->m_offset;
		if (m_isNewRow) {
			initColumn(column, def, colNo, true, data, def->m_lenBytes, 0, def->m_size);
		} else {
			size_t size = getColSize(def, def->m_nullBitmapOffset, data);
			initColumn(column, def, colNo, size == 0, data, def->m_lenBytes, size, def->m_size);
		}
		return column;
	}

	/**
	 * ����һ��
	 * @param colNo	�������е��к�
	 * @param srcCol Դ��
	 * @return �����Ƿ�ɹ�
	 */
	inline virtual void writeColumn(u16 colNo, const Column* srcCol) {
		assert(colNo < m_tableDef->m_numCols);
		ColumnDef *colDef = m_tableDef->m_columns[colNo];
		assert(colDef->m_size >= srcCol->size());
		// ��������ʽδʹ�õ��ڴ棬���ڼ�¼�Ƚ�
		if (srcCol->m_isNull) {
			assert(colDef->m_nullable);
			setColNull(colDef->m_nullBitmapOffset);
		} else  {
			if (colDef->m_nullable)
				setColNotNull(colDef->m_nullBitmapOffset);
			memcpy(m_buf + colDef->m_offset, srcCol->m_buf, srcCol->size());

			// ���srcCol��ǰ׺��������ĳһ�У�������char/binary���ͣ���Ҫ����������Ϊ0x20(char)����0x0(binary)
			if (colDef->isFixlen() && colDef->m_size > srcCol->size()) {
				int value = 0x0;
				if (colDef->m_type == CT_CHAR) {
					value = 0x20;
				} else {
					assert(colDef->m_type == CT_BINARY);
					value = 0x0;
				}
				memset(m_buf + colDef->m_offset + srcCol->size(), value, colDef->m_size - srcCol->size());
			}
		}
	}


	/**
	 * ��һ�е���ĩβ
	 * @param column ��������
	 */
	virtual void appendColumn(const Column* column) {
		UNREFERENCED_PARAMETER(column);
		assert(false); // ��֧��append
	}
	/** 
	 * ���������鶨���ȡ��һ��������ĵ�һ��
	 * ��һ��������ĵ�һ�п϶���������ĵ�һ��(����˳��)
	 * @param column
	 * @return Column
	 */
	 Column* firstColumnAcdGrp(Column *column) const {
		assert(m_tableDef->m_colGrps[0] != NULL && m_tableDef->m_numColGrps > 0);
		ColGroupDef * firstColGrp = m_tableDef->m_colGrps[0];
		assert(firstColGrp->m_numCols > 0);
		assert(firstColGrp->m_colGrpNo == 0);

		ColumnDef *columnDef = m_tableDef->m_columns[firstColGrp->m_colNos[0]];//��һ��������ĵ�һ�еĶ���
		return columnAt(columnDef->m_no, column);
	 }

	/**
	 * ���������鶨���ȡ��һ��
	 * ���ʵ�˳��϶����ȷ��ʱ��С�������飬�����������������һ������֮��ŷ�����һ��������
	 * @param cur ��ǰ��
	 * @return ����cur
	 */
	Column* nextColumnAcdGrp(Column* cur) const {
		assert(cur != NULL);
		u8 curGrpNo = cur->def()->m_colGrpNo;//��ǰ����������������
		u16 nextColGrpOffset = cur->def()->m_colGrpOffset + 1;

		while (true) {
			if (curGrpNo >= m_tableDef->m_numColGrps)
				return NULL;
			else {
				if (nextColGrpOffset < m_tableDef->m_colGrps[curGrpNo]->m_numCols) {
					u16 nextColNo = m_tableDef->m_colGrps[curGrpNo]->m_colNos[nextColGrpOffset];
					assert(nextColNo < m_tableDef->m_numCols);
					//�Ѿ��������һ�е��кţ�ͨ��columnAt��
					return columnAt(nextColNo, cur);	
				} else {
					++curGrpNo;
					nextColGrpOffset = 0;
				}
			}
		}//end while
	}
protected:
	/**
	 * ����һ��
	 * @param dstCol �������У� ���������ڱ��е���
	 * @param srcCol Դ��
	 * @return �����Ƿ�ɹ�
	 */
	void writeColumn(const Column* dstCol, const Column* srcCol) {
		assert(dstCol->colNo() < m_tableDef->m_numCols);
		assert(dstCol->m_columnDef == m_tableDef->m_columns[dstCol->colNo()]); // �������ڱ���
		assert(dstCol->capacity() >= srcCol->capacity());
		// ��������ʽδʹ�õ��ڴ棬���ڼ�¼�Ƚ�
		if (srcCol->m_isNull) {
			assert(dstCol->nullable());
			setColNull(dstCol->bitmapOffset());
		} else  {
			if (dstCol->nullable())
				setColNotNull(dstCol->bitmapOffset());
			memcpy(dstCol->m_buf, srcCol->m_buf, srcCol->size());
		}
	}

protected:
	// �Ƿ����´����Ŀ��У����е�columnAt����ʵ��������ͬ
	bool m_isNewRow;

};

class RASubRecordIterator {
public:
	RASubRecordIterator(RandAccessRow *row, const SubRecord *sr)
		: m_numCols(sr->m_numCols), m_colNos(sr->m_columns)
		, m_lastColNoIdx((u16)-1), m_row(row) {
		assert(sr->m_format == REC_FIXLEN || sr->m_format == REC_REDUNDANT || sr->m_format == REC_MYSQL || sr->m_format == REC_UPPMYSQL);
	}

	inline Column* column() {
		assert(m_lastColNoIdx != (u16)-1);
		assert(m_lastColNoIdx < m_numCols);
		return &m_column;
	}

	RASubRecordIterator*  first() {
		assert(m_lastColNoIdx == (u16)-1);
		m_lastColNoIdx = 0;
		u16 colNo = m_colNos[m_lastColNoIdx];
		Column* col = m_row->columnAt(colNo, &m_column);
		UNREFERENCED_PARAMETER(col);
		assert(col);
		return this;
	}


	RASubRecordIterator*  next() {
		++m_lastColNoIdx;
		if (m_lastColNoIdx < m_numCols) {
			Column* col = m_row->columnAt(m_colNos[m_lastColNoIdx], &m_column);
			UNREFERENCED_PARAMETER(col);
			assert(col);
			return this;
		} else if (m_lastColNoIdx == m_numCols) { // �Ѿ��������һ��
			return 0;
		} else {
			assert(false);
			return 0;
		}
	}

	inline bool end() const {
		return m_lastColNoIdx >= m_numCols;
	}
private:
	u16			m_numCols;
	const u16	*m_colNos;
	u16			m_lastColNoIdx;	/** Index into m_colNos */
	Column		m_column;
	const RandAccessRow *m_row;
};

/** �����ʽ��¼��ʽ */
class RedRow: public RandAccessRow {
public:
	RedRow(const TableDef *tableDef, const Record *record)
		: RandAccessRow(tableDef, record) {
		m_format = REC_REDUNDANT;
	}

	RedRow(const TableDef *tableDef, const SubRecord *sr, bool empty = false)
		: RandAccessRow(empty) {
		assert(sr->m_format == REC_REDUNDANT);
		assert(empty ? sr->m_size >= tableDef->m_maxRecSize : sr->m_size == tableDef->m_maxRecSize);
		init(tableDef, sr->m_data, tableDef->m_maxRecSize, tableDef->m_maxRecSize
			, tableDef->m_nullableCols, REC_REDUNDANT);
		if (empty) // ��ʼ��λͼ������valgrind����
			memset(m_buf, 0, bitmapBytes());
	}

};

/** MYSQL��ʽ��¼��ʽ */
class MysqlRow: public RandAccessRow {
public:
	MysqlRow(const TableDef *tableDef, const Record *record)
		: RandAccessRow(false) {
			if (record->m_format == REC_MYSQL) {
				init(tableDef, record->m_data, tableDef->m_maxRecSize, tableDef->m_maxRecSize, tableDef->m_nullableCols, REC_MYSQL);
			} else {
				assert(record->m_format == REC_UPPMYSQL);
				init(tableDef, record->m_data, tableDef->m_maxMysqlRecSize, tableDef->m_maxMysqlRecSize, tableDef->m_nullableCols, REC_UPPMYSQL);
			}
	}

	MysqlRow(const TableDef *tableDef, const SubRecord *sr)
		: RandAccessRow(false) {
			if (sr->m_format == REC_MYSQL) {
				init(tableDef, sr->m_data, tableDef->m_maxRecSize, tableDef->m_maxRecSize, tableDef->m_nullableCols, REC_MYSQL);
			} else {
				assert(sr->m_format == REC_UPPMYSQL);
				init(tableDef, sr->m_data, tableDef->m_maxMysqlRecSize, tableDef->m_maxMysqlRecSize, tableDef->m_nullableCols, REC_UPPMYSQL);
			}
	}

	/**
	 * ��ȡ�д�С
	 * @param columnDef �ж���
	 * @param bmOffset NULLλͼƫ��
	 * @param buf	��������ʼ��ַ
	 */
	inline size_t getColSize(ColumnDef *columnDef, u16 bmOffset, byte *buf) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		if (m_format == REC_MYSQL) {
			return ntse::getColSize(columnDef, buf);
		} else {
			assert(m_format == REC_UPPMYSQL);
			return ntse::getUppMysqlColSize(columnDef, buf);
		}
	}

	/** �����ȡ�� */
	inline virtual Column* columnAt(u16 colNo, Column* column) const {
		assert(colNo <= m_tableDef->m_numCols);
		ColumnDef *def = m_tableDef->m_columns[colNo];
		u32 offset = 0;
		u32 capacity = 0;
		if (m_format == REC_MYSQL){
			offset = def->m_offset;
			capacity = def->m_size;
		} else {
			assert(m_format == REC_UPPMYSQL);
			offset = def->m_mysqlOffset;
			capacity = def->m_mysqlSize;
		}
		byte *data = m_buf + offset;
		byte lenBytes = def->m_lenBytes;
		// ������ϲ�MYSQL��ʽ�����ǳ����ֶΣ���¼�����ֽ����϶���2	
		if (m_format == REC_UPPMYSQL && def->isLongVar())
			lenBytes = 2;
		size_t size = getColSize(def, def->m_nullBitmapOffset, data);
		initColumn(column, def, colNo, size == 0, data, lenBytes, size, capacity);
		
		return column;
	}

	/**
	 * ����һ��,���ϲ���и�ʽת�����������и�ʽ�������ֶ��ϲ���Ϊ��VARCHAR�� �������Ϊ��LOB��
	 * @param colNo	�������е��к�
	 * @param srcCol Դ��
	 */
	inline void writeColumnToEngineLayer(u16 colNo, const Column* srcCol) {
		assert(colNo < m_tableDef->m_numCols);
		assert(m_format == REC_MYSQL);
		ColumnDef *colDef = m_tableDef->m_columns[colNo];
		assert(colDef->m_mysqlSize >= srcCol->size());
		// ��������ʽδʹ�õ��ڴ棬���ڼ�¼�Ƚ�
		if (srcCol->m_isNull) {
			assert(colDef->m_nullable);
			setColNull(colDef->m_nullBitmapOffset);
		} else  {
			if (colDef->m_nullable)
				setColNotNull(colDef->m_nullBitmapOffset);
			
			// ���srcCol�ǳ����䳤�ֶ�
			if (colDef->isLongVar()) {
				u32 len = read2BytesLittleEndian(srcCol->m_buf);
				write2BytesLittleEndian(m_buf + colDef->m_offset, len);
				*(byte **)(m_buf + colDef->m_offset + colDef->m_size - 8) = (srcCol->m_buf + 2);
			} else {
				memcpy(m_buf + colDef->m_offset, srcCol->m_buf, srcCol->size());
			}
		}
	}

		/**
	 * ����һ��,���������и�ʽת�����ϲ���и�ʽ�������ֶ��ϲ���Ϊ��VARCHAR�� �������Ϊ��LOB��
	 * @param colNo	�������е��к�
	 * @param srcCol Դ��
	 */
	inline void writeColumnToUpperLayer(u16 colNo, const Column* srcCol) {
		assert(colNo < m_tableDef->m_numCols);
		assert(m_format == REC_UPPMYSQL);
		ColumnDef *colDef = m_tableDef->m_columns[colNo];
		assert(colDef->m_size >= srcCol->size());
		// ��������ʽδʹ�õ��ڴ棬���ڼ�¼�Ƚ�
		if (srcCol->m_isNull) {
			assert(colDef->m_nullable);
			setColNull(colDef->m_nullBitmapOffset);
		} else  {
			if (colDef->m_nullable)
				setColNotNull(colDef->m_nullBitmapOffset);
			
			// ���srcCol�ǳ����䳤�ֶ�
			if (colDef->isLongVar()) {
				// �ȿ���ǰ׺
				u32 len = read2BytesLittleEndian(srcCol->m_buf);
				write2BytesLittleEndian(m_buf + colDef->m_mysqlOffset, len);
				byte *lob = *((byte **)(srcCol->m_buf + colDef->m_size - 8));
				memcpy(m_buf + colDef->m_mysqlOffset + 2, lob, len);
			} else {
				memcpy(m_buf + colDef->m_mysqlOffset, srcCol->m_buf, srcCol->size());
			}
		}
	}
};


typedef Row CompressOrderRow;/** ѹ�������ʽ�У����ڲ���ѹ�������ʽ��¼���� */
typedef Row	VarLenRow;
typedef RandAccessRow FixedLenRow;


/**
 * ѹ����ʽ������
 * ���ڲ���ѹ����ʽ���е�ѹ��������
 */
class CompressedColGroup {
	friend class ColGroupRow;
	friend class CompressedRow;
public:
	/**
	 * ����һ��Ĭ��ѹ��������
	 */
	CompressedColGroup() : m_colGrpNo(0), m_colGrpDef(NULL), m_buf(NULL), m_size(0), m_lenBytes(0) {
	}
	/**
	 * ����һ��ѹ��������
	 * @param colGrpDef   �����鶨��
	 * @param buf         ѹ�����������ݵ�ַ(����0~3�ֽڵ������鳤��)
	 * @param size        ѹ�����������ݳ���
	 * @param lenBytes    ���ڱ�ʾѹ�������鳤�ȵ��ֽ���
	 */
	CompressedColGroup(const ColGroupDef *colGrpDef, byte *buf, size_t size, u8 lenBytes = 0) : m_colGrpNo(colGrpDef->m_colGrpNo), 
		m_colGrpDef(colGrpDef), m_buf(buf), m_size(size), m_lenBytes(lenBytes) {
	}
	inline void setBuf(byte *buf, size_t size) {
		m_buf = buf;
		m_size = size;
	}
	inline byte * data() const {
		return m_buf;
	}
	inline size_t size() const {
		return m_size;
	}
	inline const ColGroupDef * def() const {
		return m_colGrpDef;
	}
	inline u8 colGrpNo() const {
		return m_colGrpNo;
	}
	inline u8 lenBytes() const {
		return m_lenBytes;
	}
	/**
	 * ���ѹ��������ʵ��������ʼ��ַ(����ʾ�����鳤�ȵ��ֽ�)
	 * @return 
	 */
	inline byte * getRealData() const {
		return m_buf + m_lenBytes;
	}
	/**
	 * ���ѹ��������ʵ�����ݳ���(����ʾ�����鳤�ȵ��ֽ�)
	 * @return 
	 */
	inline size_t getRealSize() const {
		return m_size - m_lenBytes;
	}

private:
	u8                m_colGrpNo;       /** ������� */
	const ColGroupDef *m_colGrpDef;     /** �����鶨�� */
	byte			  *m_buf;			/** ���ݻ��� */
	size_t			  m_size;			/** ���ݻ����С */
	u8                m_lenBytes;       /** ���ڱ�ʾѹ�����ݴ�С���ֽ��� */
};

/**
 * ��������
 * ���ڲ����������е���
 */
class ColGroupRow {
public:
	/**
	 * @param tableDef   ��������
	 * @param colGrpNo   �������
	 * @param buf        ���ݻ���
	 * @param capacity   ���ݻ�������
	 * @param bmData     ��ֵλͼ��ʼ��ַ
	 * @param empty      �Ƿ��ǿ���
	 */
	ColGroupRow(const TableDef *tableDef, u8 colGrpNo, byte *buf, size_t capacity, byte *bmData, bool empty = false):
	m_tableDef(tableDef), m_buf(buf), m_capacity(capacity), m_bmData(bmData), m_curColOffset(0) {
		assert(colGrpNo < tableDef->m_numColGrps);
		m_colGrpDef = tableDef->m_colGrps[colGrpNo];
		m_size = empty ? 0 : capacity;
	}

	/**
	 * ��ȡ��һ��
	 * @param cur ���ڴ洢�����ݵ���ָ��
	 * @return ����cur
	 */
	inline Column* firstColumn(Column* cur) const {
		assert(m_curColOffset == 0);
		ColumnDef *colDef = m_tableDef->m_columns[m_colGrpDef->m_colNos[m_curColOffset]];
		size_t size = getColSize(colDef, colDef->m_nullBitmapOffset, m_buf);
		return initColumn(cur, colDef, colDef->m_no, size == 0, m_buf, size, size);
	}

	/**
	* ��ȡ��һ��
	* @param cur ��ǰ��
	* @return    ����cur
	*/
	inline Column* nextColumn(Column* cur) {
		++m_curColOffset;
		return (m_curColOffset >= m_colGrpDef->m_numCols) ? NULL : nextColumn(cur, m_curColOffset);
	}

	/**
	 * ����һ�е���ĩβ
	 * @param column ��������
	 */
	inline void appendColumn(Column *column) {
		assert(column->colNo() < m_tableDef->m_numCols);
		u16 bmOffset = column->def()->m_nullBitmapOffset;
		if (column->m_isNull) {
			assert(column->nullable());
			setColNull(bmOffset);
		} else if (m_capacity >= m_size + column->m_size) {
			if (column->nullable())
				setColNotNull(bmOffset);
			memcpy(m_buf + m_size, column->m_buf, column->m_size);
			m_size += column->m_size;
		} else { // �ڴ治��
			assert(false);
		}
	}

	/**
	 * �������ʵ�ʴ�С
	 */
	inline size_t size() const {
		return m_size;
	}

protected:
	/**
	* ��ʼ����
	* @param column     ����ʼ���ж���
	* @param columnDef  �ж���
	* @param colNo      �к�
	* @param isNull     �Ƿ�Ϊ��
	* @param buf        ��������ʼ��ַ
	* @param size       �����ݳ���
	* @param capacity   ��ʵ��ռ���ڴ�ռ�
	* @return           ����column
	*/
	inline Column* initColumn(Column* column, ColumnDef *columnDef,
		u16 colNo, bool isNull, byte* buf, size_t size, size_t capacity) const {
			assert(colNo < m_tableDef->m_numCols);
			assert(columnDef == m_tableDef->m_columns[colNo]);

			column->m_colNo = colNo;
			column->m_columnDef = columnDef;
			column->m_buf = buf;
			column->m_isNull = isNull;
			column->m_size = size;
			column->m_capacity = capacity;
			assert(column->m_size + column->m_buf <= m_buf + m_size);
			return column;
	}

	/**
	 * ��ȡ��һ��
	 * @param column ��ǰ��
	 * @param nextOffset ��һ�����������е��±�
	 */
	inline Column* nextColumn(Column* column, u16 nextOffset) const {
		assert(nextOffset < m_colGrpDef->m_numCols);
		ColumnDef *colDef = m_tableDef->m_columns[m_colGrpDef->m_colNos[nextOffset]];
		byte *data = column->data() + column->size();
		if (data >= m_buf + m_size) {
			if (nextOffset < m_colGrpDef->m_numCols) { // ʣ�µ��ж���NULL
				assert(isColNull(colDef->m_nullBitmapOffset));
			} else {
				return NULL;
			}
		}
		size_t size = getColSize(colDef, colDef->m_nullBitmapOffset, data);
		return initColumn(column, colDef, colDef->m_no, size == 0, data, size, size);
	}

	/**
	* ��ȡ�д�С
	* @param columnDef �ж���
	* @param bmOffset NULLλͼƫ��
	* @param data	��������ʼ��ַ
	*/
	inline size_t getColSize(ColumnDef *columnDef, u16 bmOffset, byte *data) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		return ntse::getColSize(columnDef, data);
	}

	/** �ж����Ƿ�Ϊnull */
	inline bool isColNull(u16 offset) const {
		return BitmapOper::isSet(m_bmData, (uint)m_tableDef->m_bmBytes << 3, offset);
	}

	/** ������Ϊnull */
	inline void setColNull(u16 offset) {
		BitmapOper::setBit(m_bmData, (uint)m_tableDef->m_bmBytes << 3, offset);
	}
	/** ������Ϊ��null */
	inline void setColNotNull(u16 offset) {
		BitmapOper::clearBit(m_bmData, (uint)m_tableDef->m_bmBytes << 3, offset);
	}

private:
	const ColGroupDef *m_colGrpDef; /** �����鶨�� */
	const TableDef	*m_tableDef;	/** ����     */
	byte			*m_buf;			/** ���ݻ���   */
	size_t          m_capacity;     /** ���ݻ����С */
	size_t			m_size;			/** ʵ�ʴ�С   */
	byte            *m_bmData;      /** ��ֵλͼ��ʼ��ַ */
	u16             m_curColOffset; /** ��ǰ�����������������е��±꣬��0��ʼ */
};

/**
* ѹ���У�
*	����ѹ����¼���ڴ�ռ�
*	�ṩѹ����¼������������Ͳ�������
*/
class CompressedRow {
public:
	/** 
	 * ����һ��ѹ����
	 * @param tableDef ����
	 * @param buf      ���ݻ���
	 * @param capacity ���ݻ�������
	 * @param empty    �Ƿ��ǿ��У�����ǿ��У������ݻ�������δ�����
	 */
	CompressedRow(const TableDef *tableDef, byte *buf, size_t capacity, bool empty = false) : m_tableDef(tableDef),
	m_buf(buf), m_capacity(capacity), m_size(0) {
		if (empty) {
			memset(m_buf, 0, tableDef->m_bmBytes);
			m_size += tableDef->m_bmBytes;
		} else {
			m_size = m_capacity;
		}		
	}
	/**
	 * ����һ��ѹ����
	 * @param tableDef ��������
	 * @param record   ѹ����¼��ΪREC_COMPRESSED��ʽ
	 */
	CompressedRow(const TableDef *tableDef, const Record *record) : m_tableDef(tableDef), 
		m_buf(record->m_data), m_capacity(record->m_size), m_size(record->m_size) {
			assert(record->m_format == REC_COMPRESSED);
	}

	/**
	* ����Null Bitmap
	* @param buf			Null Bitmap��ʼ��ַ
	* @param nullBitmapSize	Null Bitmap����
	*/
	inline void setNullBitmap(byte *buf, size_t nullBitmapSize) {
		memcpy(m_buf, buf, nullBitmapSize);
	}

	/**
	 * ��ȡ��һ��������
	 * @param cur ���ڴ�ŵ�һ������������
	 * @return ����cur
	 */
	inline CompressedColGroup* firstColGrp(CompressedColGroup *cur) const {
		const ColGroupDef *def = m_tableDef->m_colGrps[0];
		byte *data = m_buf + m_tableDef->m_bmBytes;
		u8 lenBytes;
		size_t colGrpSize = getColGrpSize(data, &lenBytes, m_size - m_tableDef->m_bmBytes);
		return initColGrp(cur, def->m_colGrpNo, def, data, colGrpSize, lenBytes);
	}

	/** 
	 * ��ȡ��һ��������
	 * @param cur ��ǰ������
	 * @param ����cur
	 */
	inline CompressedColGroup* nextColGrp(CompressedColGroup *cur) const {
		if (cur->m_colGrpNo >= m_tableDef->m_numColGrps - 1)
			return NULL;
		byte *data = cur->data() + cur->m_size;
		const ColGroupDef *def = m_tableDef->m_colGrps[++cur->m_colGrpNo];
		u8 lenBytes;
		size_t colGrpSize = getColGrpSize(data, &lenBytes);
		assert(m_buf + m_size >= data + colGrpSize);
		return initColGrp(cur, def->m_colGrpNo, def, data, colGrpSize, lenBytes);
	}
	/**
	 * ����һ�������鵽��ǰ��ĩβ
	 * @param cur ������������
	 */
	inline void appendColGrp(CompressedColGroup *cur) {
		assert(m_size + cur->size() <= m_capacity);
		memcpy(m_buf + m_size, cur->data(), cur->size());
		m_size += cur->size();
	}
	/**
	* ����һ�������鵽��ǰ��ĩβ
	* @param realData ѹ��������������ʼ��ַ�������������鳤����Ϣ
	* @param size ���ݳ���
	*/
	inline void appendColGrp(byte *realData, size_t size) {
		if (m_tableDef->m_numColGrps != 1) {
			u8 lenBytes = RecordOper::writeCompressedColGrpSize(m_buf + m_size, (uint)size);
			m_size += lenBytes;
		}
		memcpy(m_buf + m_size, realData, size);
		m_size += size;
	}

	/**
	 * �����������ݽ���ѹ����׷�ӵ���ĩβ
	 * @param cprsRcdExtrator ѹ����¼��ȡ��
	 * @param src ���ݵ�ַ
	 * @param offset ƫ����
	 * @param len ���ݳ���
	 */
	inline void compressAndAppendColGrp(CmprssRecordExtractor *cprsRcdExtrator, byte *src, 
		const uint &offset, const uint &len) {
		uint reCompressedSize = 0;
		byte *dest = m_buf + m_size + (m_tableDef->m_numColGrps == 1 ? 0 : 1);
		cprsRcdExtrator->compressColGroup(src, offset, len, dest, &reCompressedSize);
		m_size += reCompressedSize;
		if (m_tableDef->m_numColGrps != 1) {
			if (reCompressedSize < ONE_BYTE_SEG_MAX_SIZE) {
				*(dest - 1) = (byte)reCompressedSize;
				++m_size;
			} else {
				assert(reCompressedSize < TWO_BYTE_SEG_MAX_SIZE);
				memmove(dest + 1, dest, reCompressedSize);
				*(dest - 1) = (byte )((reCompressedSize >> 8) | 0x80);
				*dest = (byte )reCompressedSize;
				m_size += 2;
			}
		}
	}

	/**
	 * ��õ�ǰ������ʵ�ʴ�С
	 */
	inline size_t size() const {
		return m_size;
	}

protected:
	/**
	 * ��ʼ��һ��������
	 * @param colGrp     �ⲿ��������ڴ�����ݵĿռ��ָ��
	 * @param colGrpNo   �������
	 * @param colGrpDef  �����鶨��
	 * @param data       ������������ʼ��ַ
	 * @param size       ���������ݳ���
	 * @param lenBytes   ���ڱ�ʾ��������ʵ���ݳ�����Ϣ���ֽ���
	 */
	inline CompressedColGroup* initColGrp(CompressedColGroup *colGrp, u8 colGrpNo, const ColGroupDef *colGrpDef, byte *data, size_t size, u8 lenBytes) const {
		assert(colGrpDef == m_tableDef->m_colGrps[colGrpNo]);
		colGrp->m_colGrpNo = colGrpNo;
		colGrp->m_colGrpDef = colGrpDef;
		colGrp->m_buf = data;
		colGrp->m_size = size;
		colGrp->m_lenBytes = lenBytes;
		return colGrp;
	}
	/**
	 * ��������鳤��
	 * @param data     ������������ʼ��ַ
	 * @param lenBytes OUT�����������ʾ�����鳤�ȵ��ֽ���
	 * @param size     �����鳤��
	 */
	inline size_t getColGrpSize(byte *data, u8 *lenBytes, size_t size = 0) const {
		if (m_tableDef->m_numColGrps == 1) {
			*lenBytes = 0;
			return size;
		} else {
			uint segSize;
			*lenBytes = RecordOper::readCompressedColGrpSize(data, &segSize);
			return segSize + (*lenBytes);
		}
	}
	
private:
	const TableDef	*m_tableDef;	/** ����     */
	byte			*m_buf;			/** ���ݻ���   */
	size_t          m_capacity;     /*  ���ݻ����С */
	size_t			m_size;			/** ʵ�ʴ�С   */
};

/**
 * ѹ������������ȡ��Ϣ
 * ���ڴ��ݺͼ�����Щ��������Ҫ��ѹ������Щ����Ҫ��ȡ
 */
class CmprssColGrpExtractInfo {
public:
	/**
	 * ����ѹ������������ȡ��Ϣ
	 * @param tableDef ����
	 * @param numCols Ҫ��ȡ������
	 * @param columns Ҫ��ȡ���е��±�
	 * @param mtx     �ڴ����������
	 */
	CmprssColGrpExtractInfo(const TableDef *tableDef, u16 numCols, const u16* columns, MemoryContext *mtx = NULL) :
	  m_mtx(mtx), m_tableDef(tableDef), m_decompressBufSize(tableDef->m_maxRecSize) {
		assert(m_tableDef->m_numCols > 0 && m_tableDef->m_colGrps != NULL);
		if (m_mtx) {
			m_decompressBuf = (byte*)m_mtx->alloc(m_tableDef->m_maxRecSize);
			m_colNumNeedReadInGrp = (u16 *)m_mtx->calloc(m_tableDef->m_numColGrps * sizeof(u16));
			m_colNeedRead = (u8 *)m_mtx->calloc(m_tableDef->m_numCols * sizeof(u8));
		} else {
			m_decompressBuf = new byte[m_tableDef->m_maxRecSize];
			m_colNumNeedReadInGrp = new u16[m_tableDef->m_numColGrps];
			memset(m_colNumNeedReadInGrp, 0, m_tableDef->m_numColGrps * sizeof(u16));
			m_colNeedRead = new u8[m_tableDef->m_numCols];
			memset(m_colNeedRead, 0, m_tableDef->m_numCols * sizeof(u8));
		} 

		for (u16 i = 0; i < numCols; i++) {
			u16 colNo = columns[i];
			m_colNeedRead[colNo]++;
			u8 colGrpNo = tableDef->m_columns[colNo]->m_colGrpNo;
			m_colNumNeedReadInGrp[colGrpNo]++;
		}
	}

	~CmprssColGrpExtractInfo() {
		if (!m_mtx) {
			delete []m_decompressBuf;
			delete []m_colNumNeedReadInGrp;
			delete []m_colNeedRead;
		}
	}
public:
	MemoryContext  *m_mtx;                 /* �ڴ���������� */
	const TableDef *m_tableDef;            /* �������� */
	byte           *m_decompressBuf;       /* ���ڻ����ѹ�����ݵĻ����� */
	size_t         m_decompressBufSize;    /* ����Ľ�ѹ�����ݻ�������С */
	u16            *m_colNumNeedReadInGrp; /* ����������Ҫ��ȡ������ͳ�ƣ��������Ԫ�ش���0��ʾ����������Ҫ��ѹ�� */
	u8             *m_colNeedRead;         /* ��Ҫ��ȡ���У��������Ԫ�ش���0��ʾ������Ҫ��ȡ */
};

/**
 * ���ݴ�����ַ����ж� ǰ׺N���ַ�����ռ�ö����ֽ�
 * ��;: ǰ׺����ʱ��ȡǰ׺��ֵʹ��
 *
 * @param collation �ַ���collation
 * @param prefixLen ǰ׺��������󳤶ȣ�n character * mbmaxlen��
 * @param dataLen ���ж��ַ����ֽ���
 * @param str ���ж��ַ���
 *
 * @return �����ַ���ǰ׺N���ַ�����ռ�õ��ֽ���
 */
static size_t getRealSizeForPrefixIndex(CollType collation, u32 prefixLen, u32 dataLen, const char *str) {
	size_t mbMinLen = 0, mbMaxLen = 0;
	Collation::getMinMaxLen(collation, &mbMinLen, &mbMaxLen);

	if (mbMinLen != mbMaxLen) {
		assert(!(prefixLen % mbMaxLen));
		u32 nChars = prefixLen / mbMaxLen;
		u32 charLength = 0;
		if (mbMaxLen > 1) {
			charLength = Collation::charpos(collation, str, str + dataLen, nChars);
			if (charLength > dataLen)
				charLength = dataLen;
		} else {
			if (dataLen < prefixLen)
				charLength = dataLen;
			else
				charLength = prefixLen;
		}
		return charLength;
	}
	
	if (prefixLen < dataLen)
		return prefixLen;

	return dataLen;

}

/**
 * ��һ�������ʽ�ļ�¼��ȡ����������
 * ��;: ��������ʱ����������
 *
 * @param tableDef ��¼�����ı���
 * @param record һ�������ļ�¼����m_formatһ��ΪREC_REDUNDANT
 * @param lobArray ��¼�еĴ��������
 * @param key ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_COMPRESS
 */
void RecordOper::extractKeyRC(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key) {
	assert(record->m_format == REC_REDUNDANT);
	assert(key->m_format == KEY_COMPRESS);

	RedRow mRow(tableDef, record);
	CompressedKeyRow cRow(tableDef, indexDef, key, true);
	Column column;
	size_t lobCount = 0;
	for (u16 i = 0; i < key->m_numCols; ++i) {
		u16 colNo = key->m_columns[i];
		LobPair *lobPair = NULL;
		if (tableDef->getColumnDef(colNo)->isLob()) {
			lobPair = (*lobArray)[lobCount];
			lobCount++;
		}
		cRow.appendColumn(mRow.columnAt(colNo, &column), indexDef->m_prefixLens[i], lobPair);
	}
	key->m_size = (uint)cRow.size();
	key->m_rowId = record->m_rowId;
}

/**
 * ������ʽ��������ת��Ϊѹ����ʽ
 * ��;: SELECT����ɨ�����ѹ����ֱ�ӱȽ�ʱ
 *
 * @param tableDef ��¼�����ı���
 * @param indexDef ��¼��������������
 * @param src ����ʽ������������m_formatһ��ΪKEY_PAD
 * @param dest ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_COMPRESS
 */
void RecordOper::convertKeyPC(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src,  SubRecord *dest) {
	assert(src->m_format == KEY_PAD);
	assert(dest->m_format == KEY_COMPRESS);

	KeyRow srcRow(tableDef, indexDef, src);
	KeyRow::Iterator ki(&srcRow, src);
	CompressedKeyRow dstRow(tableDef, indexDef, dest, true);
	size_t iterCount = 0;
	for (iterCount = 0, ki.first(); !ki.end(); ki.next(), iterCount++) 
		dstRow.appendColumn(ki.column(), indexDef->m_prefixLens[iterCount], NULL);

	dest->m_size = (uint)dstRow.size();
	dest->m_rowId = src->m_rowId;
}

void RecordOper::convertKeyCP(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_COMPRESS);
	assert(dest->m_format == KEY_PAD);

	CompressedKeyRow srcRow(tableDef, indexDef, src);
	KeyRow::Iterator ki(&srcRow, src);
	KeyRow dstRow(tableDef, indexDef, dest, true);

	byte tmp[8];
	Column raw;
	size_t iterCount = 0;
	for (iterCount = 0, ki.first(); !ki.end(); ki.next(), iterCount++) {
		raw.setBuf(tmp, sizeof(tmp));
		dstRow.appendColumn(CompressedKeyRow::decompressColumn(ki.column(), &raw), indexDef->m_prefixLens[iterCount], NULL);	
	}
	dest->m_size = (uint)dstRow.size();
	dest->m_rowId = src->m_rowId;
}

void RecordOper::convertKeyNP(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_NATURAL);
	assert(dest->m_format == KEY_PAD);

	KeyRow srcRow(tableDef, indexDef, src);
	KeyRow::Iterator ki(&srcRow, src);
	KeyRow dstRow(tableDef, indexDef, dest, true);
	size_t iterCount = 0;
	for (iterCount = 0, ki.first(); !ki.end(); ki.next(), iterCount++)
		dstRow.appendColumn(ki.column(), indexDef->m_prefixLens[iterCount], NULL);
	dest->m_size = (uint)dstRow.size();
	dest->m_rowId = src->m_rowId;
}

void RecordOper::convertKeyNC(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_NATURAL);
	assert(dest->m_format == KEY_COMPRESS);

	KeyRow srcRow(tableDef, indexDef, src);
	KeyRow::Iterator ki(&srcRow, src);
	CompressedKeyRow dstRow(tableDef, indexDef, dest, true);
	size_t iterCount = 0;
	for (iterCount = 0, ki.first(); !ki.end(); ki.next(), iterCount++) 
		dstRow.appendColumn(ki.column(), indexDef->m_prefixLens[iterCount], NULL);

	dest->m_size = (uint)dstRow.size();
	dest->m_rowId = src->m_rowId;
}

void RecordOper::convertKeyCN(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_COMPRESS);
	assert(dest->m_format == KEY_NATURAL);

	CompressedKeyRow srcRow(tableDef, indexDef, src);
	KeyRow::Iterator ki(&srcRow, src);
	KeyRow dstRow(tableDef, indexDef, dest, true);

	byte tmp[8];
	Column raw;
	size_t iterCount = 0;
	for (iterCount = 0, ki.first(); !ki.end(); ki.next(), iterCount++) {
		raw.setBuf(tmp, sizeof(tmp));
		dstRow.appendColumn(CompressedKeyRow::decompressColumn(ki.column(), &raw), indexDef->m_prefixLens[iterCount], NULL);
	}
	dest->m_size = (uint)dstRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
 * ������ʽ��������ת��Ϊ��Ȼ��ʽ
 *
 * @param tableDef ��¼�����ı���
 * @param indexDef ��¼��������������
 * @param src ����ʽ������������m_formatһ��ΪKEY_PAD
 * @param dest ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_NATURAL
 */
void RecordOper::convertKeyPN(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_PAD);
	assert(dest->m_format == KEY_NATURAL);

	KeyRow srcRow(tableDef, indexDef, src);
	KeyRow dstRow(tableDef, indexDef, dest, true);
	KeyRow::Iterator ki(&srcRow, src);
	size_t iterCount = 0;
	for (iterCount = 0, ki.first(); !ki.end(); ki.next(), iterCount++) 
		dstRow.appendColumn(ki.column(), indexDef->m_prefixLens[iterCount], NULL);

	dest->m_size = (uint)dstRow.size();
	dest->m_rowId = src->m_rowId;
}
/**
 * ��MYSQL��ʽ��������ת��Ϊ����ʽ
 *
 * @param tableDef ��¼�����ı���
 * @param indexDef ��¼��������������
 * @param src MYSQL��ʽ������������m_formatһ��ΪKEY_MYSQL
 * @param dest ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_PAD
 */
bool RecordOper::convertKeyMP(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_MYSQL);
	assert(dest->m_format == KEY_PAD);

	size_t bmBytes = calcBitmapBytes(KEY_PAD, calcNullableCols(tableDef, src));
	size_t bmSize = (bmBytes << 3);
	size_t bmOffset = 0;
	bool bNullIncluded = false;

	memset(dest->m_data, 0 ,bmBytes); // ��ʼ��λͼ
	// TODO: ΪKEY_MYSQL������
	byte *srcPtr = src->m_data;
	byte *dstPtr = dest->m_data + bmBytes;
	for (u16 i = 0; i < src->m_numCols; ++i) {
		ColumnDef *columnDef = tableDef->m_columns[src->m_columns[i]];
		bool isFixLen = columnDef->isFixlen(); //������ת��Index Key��ʽ��������Ǳ䳤��
		if (columnDef->m_nullable) {
			if (*srcPtr) { // NULL
				BitmapOper::setBit(dest->m_data, bmSize, bmOffset++);
				++srcPtr;

				size_t lenBytes = columnDef->m_lenBytes;
				if (isLob(columnDef->m_type)) {
					lenBytes = indexDef->m_prefixLens[i] > 255 ? 2 : 1;
				}
				size_t columnSize = columnDef->m_size;
				if (indexDef->m_prefixLens[i])
					columnSize = indexDef->m_prefixLens[i] + lenBytes;
				srcPtr += isFixLen ? columnSize : (columnSize - lenBytes + 2);
				dstPtr += columnSize;

				bNullIncluded = true;

				continue;
			} else {
				BitmapOper::clearBit(dest->m_data, bmSize, bmOffset++);
				++srcPtr;
			}
		}
		// KEY_MSQL��VARCHAR�ĳ��������������ֽڱ�ʾ, VARCHAR�ֶ���Ҫ���⴦��
		if (!isFixLen) {
			// ������
			u32 len = readU32Le(srcPtr, 2);
			size_t lenBytes = columnDef->m_lenBytes;
			size_t remainCap = (columnDef->m_size - lenBytes);

			if (isLob(columnDef->m_type)) {
				lenBytes = indexDef->m_prefixLens[i] > 255 ? 2 : 1;
			}


			if (indexDef->m_prefixLens[i]){
				len = min(len, indexDef->m_prefixLens[i]);
				remainCap = indexDef->m_prefixLens[i];
			}
			writeU32Le(len, dstPtr, lenBytes);
			dstPtr += lenBytes;
			srcPtr += 2;
			// ����ʵ������
			assert(len <= remainCap);
			memcpy(dstPtr, srcPtr, len);
			srcPtr += remainCap;
			dstPtr += remainCap;
		} else {
			u32 len = indexDef->m_prefixLens[i] > 0? indexDef->m_prefixLens[i]: columnDef->m_size;
			memcpy(dstPtr, srcPtr, len);
			srcPtr += len;
			dstPtr += len;
		}
		assert(srcPtr <= src->m_data + src->m_size);
		assert(dstPtr <= dest->m_data + dest->m_size);
	}
	dest->m_size = (uint)(dstPtr - dest->m_data);

	return bNullIncluded;
}


/**
 * ��һ�������ʽ�ļ�¼��ȡ����������
 * ��;: MMS����
 *
 * @param tableDef ��¼�����ı���
 * @param indexDef ��¼��������������
 * @param record һ�������ļ�¼����m_formatһ��ΪREC_REDUNDANT
 * @param key ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_NATURAL
 */
void RecordOper::extractKeyRN(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair *> *lobArray, SubRecord *key) {
	assert(record->m_format == REC_REDUNDANT);
	assert(record->m_size == tableDef->m_maxRecSize);
	assert(key->m_format == KEY_NATURAL);

	RedRow mRow(tableDef, record);
	KeyRow row(tableDef, indexDef, key, true);
	Column column;
	u16 lobCount = 0;
	for (u16 i = 0; i < key->m_numCols; ++i) {
		u16 colNo = key->m_columns[i];
		LobPair *lobPair = NULL;
		if (tableDef->getColumnDef(colNo)->isLob()) {
			lobPair = (*lobArray)[lobCount];
			lobCount++;
		}
		row.appendColumn(mRow.columnAt(colNo, &column), indexDef->m_prefixLens[i], lobPair);
	}
	key->m_size = (uint)row.size();
	key->m_rowId = record->m_rowId;
}
/**
 * ��һ��������ʽ�ļ�¼��ȡ����������
 * ��;:  ��MMSģ��redoUpdateʱ����Ҫ����Record��ȡ������Ϣ
 *
 * @param tableDef ��¼�����ı���
 * @param indexDef ��¼��������������
 * @param record һ�������ļ�¼����m_formatһ��ΪREC_FIXLEN
 * @param key ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_NATURAL
 */
void RecordOper::extractKeyFN(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, SubRecord *key) {
	assert(record->m_format == REC_FIXLEN);
	assert(record->m_size == tableDef->m_maxRecSize);
	assert(key->m_format == KEY_NATURAL);

	FixedLenRow fRow(tableDef, record);
	KeyRow row(tableDef, indexDef, key, true);
	Column column;
	for (u16 i = 0; i < key->m_numCols; ++i) {
		u16 colNo = key->m_columns[i];
		row.appendColumn(fRow.columnAt(colNo, &column), indexDef->m_prefixLens[i], NULL);
	}
	key->m_size = (uint)row.size();
	key->m_rowId = record->m_rowId;
}
/**
 * ��һ���䳤��ʽ�ļ�¼��ȡ����������
 * ��;:  ��MMSģ��redoUpdateʱ����Ҫ����Record��ȡ������Ϣ
 *
 * @param tableDef ��¼�����ı���
 * @param indexDef ��¼��������������
 * @param record һ�������ļ�¼����m_formatһ��ΪREC_VARLEN
 * @param lobArray ���������������������
 * @param key ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_NATURAL
 */
void RecordOper::extractKeyVN(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key) {
	assert(record->m_format == REC_VARLEN);
	assert(record->m_size <= tableDef->m_maxRecSize);
	assert(key->m_format == KEY_NATURAL);

	VarLenRow vRow(tableDef, record);
	KeyRow row(tableDef, indexDef, key, true);
	// TODO��������һ���ڴ����
	vector<Column> cols(key->m_numCols); // ����ȡ������
	Column column; // ��ǰ��
	// ��ȡ���д���ȡ����Ϣ
	for (Column* col = vRow.firstColumn(&column); col; col = vRow.nextColumn(col)) {
		for (uint i = 0; i < key->m_numCols; ++i) {
			if (col->colNo() == key->m_columns[i]) { // �ҵ�һ��ƥ����
				cols[i] = *col;
				break;
			}
		}
	}
	// ����KEY
	size_t lobCount = 0;
	for (uint i = 0; i < key->m_numCols; ++i) {
		LobPair *lobPair = NULL;
		if (cols[i].def()->isLob()) {
			lobPair = (*lobArray)[lobCount];
			lobCount++;
		}
		row.appendColumn(&cols[i], indexDef->m_prefixLens[i], lobPair);
	}
	key->m_size = (uint)row.size();
	key->m_rowId = record->m_rowId;
}

/**
 * ��һ�������ʽ�ļ�¼��ȡ��������������
 * ��;: ִ��INSERT...ON DUPLICATE KEY UPDATE/REPLACE���ʱ��Ҫ������������������
 *
 * @param tableDef ��¼�����ı���
 * @param indexDef ��¼��������������
 * @param record һ�������ļ�¼����m_formatһ��ΪREC_REDUNDANT
 * @param lobArray ���������������������
 * @param key ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������key->m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪKEY_PAD
 */
void RecordOper::extractKeyRP(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key) {
	assert(record->m_format == REC_REDUNDANT);
	assert(record->m_size == tableDef->m_maxRecSize);
	assert(key->m_format == KEY_PAD);

	RedRow mRow(tableDef, record);
	KeyRow row(tableDef, indexDef, key, true);
	Column column;
	size_t lobCount = 0;
	for (u16 i = 0; i < key->m_numCols; ++i) {
		u16 colNo = key->m_columns[i];
		LobPair *lobPair = NULL;
		if (tableDef->getColumnDef(colNo)->isLob()) {
			lobPair = (*lobArray)[lobCount];
			lobCount++;
		}
		row.appendColumn(mRow.columnAt(colNo, &column), indexDef->m_prefixLens[i], lobPair);
	}
	key->m_size = (uint)row.size();
	key->m_rowId = record->m_rowId;
}

/**
 * ������RecordOper::extractKeyRP��ȫһ�£�Ψһ���������ڣ�����ֵ�б�ʶ����ȡ�������������Ƿ����NULLֵ
 *
 * @return ����ȡ��������������NULLֵ���򷵻�true�����򷵻�false
 */
bool RecordOper::extractKeyRPWithRet(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key) {
	assert(record->m_format == REC_REDUNDANT);
	assert(record->m_size == tableDef->m_maxRecSize);
	assert(key->m_format == KEY_PAD);

	bool isNullIncluded = false;

	RedRow mRow(tableDef, record);
	KeyRow row(tableDef, indexDef, key, true);
	Column column;
	size_t lobCount = 0;
	for (u16 i = 0; i < key->m_numCols; ++i) {
		u16 colNo = key->m_columns[i];
		mRow.columnAt(colNo, &column);
		if (column.isNull() == true)
			isNullIncluded = true;
		LobPair *lobPair = NULL;
		if (tableDef->getColumnDef(colNo)->isLob()) {
			lobPair = (*lobArray)[lobCount];
			lobCount++;
		}
		row.appendColumn(&column, indexDef->m_prefixLens[i], lobPair);
	}
	key->m_size = (uint)row.size();
	key->m_rowId = record->m_rowId;

	return isNullIncluded;
}

/**
 * ��һ��������ʽ�ļ�¼��ȡ���������Ե�ֵ���洢Ϊ�����ʽ��
 * ��;: �Ӷ����ѻ򶨳���¼���MMS��¼��ȡ��ѯ�������Է��ظ�MySQL
 *
 * @param tableDef ��¼�����ı���
 * @param record һ�������ļ�¼����m_formatһ��ΪREC_FIXLEN
 * @param subRecord ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������subRecord.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_REDUNDANT
 */
void RecordOper::extractSubRecordFR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	assert(record->m_format == REC_FIXLEN);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert(tableDef->m_recFormat == REC_FIXLEN);
	assert(tableDef->m_maxRecSize == record->m_size);
	assert(tableDef->m_maxRecSize <= subRecord->m_size);

	FixedLenRow fRow(tableDef, record);
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);
	RedRow mRow(tableDef, subRecord, true);
	Column column;
	for (u16 i = 0; i < subRecord->m_numCols; ++i) {
		u16 colNo = subRecord->m_columns[i];
		mRow.writeColumn(colNo, fRow.columnAt(colNo, &column));
	}
	subRecord->m_rowId = record->m_rowId;
	assert(tableDef->m_maxRecSize == (int)mRow.size());
	subRecord->m_size = tableDef->m_maxRecSize;
}

/** Ϊ���������ӱ�ID�Լ�����ID���ݣ�����checkDuplicateKey */
void RecordOper::appendKeyTblIdAndIdxId(SubRecord *key, TableId tblId, u8 idxNo) {
	assert(key->m_format == KEY_NATURAL);
	uint size = key->m_size;
	*(u16*)(key->m_data + size) = tblId;
	size += sizeof(u16);
	*(u8*)(key->m_data + size) = idxNo;
	size += sizeof(u8);
	key->m_size = size;
}


/** ��Ч��extractSubRecordFR */
void RecordOper::fastExtractSubRecordFR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	assert(record->m_format == REC_FIXLEN);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert(tableDef->m_recFormat == REC_FIXLEN);
	assert(tableDef->m_maxRecSize == record->m_size);
	assert(tableDef->m_maxRecSize <= subRecord->m_size);

	size_t bmBytes = tableDef->m_bmBytes;
	memset(subRecord->m_data, 0, bmBytes); // ��ʼ��λͼ
	size_t bmSize = (bmBytes << 3);
	assert(subRecord->m_size > bmBytes);

	for (u16 i = 0; i < subRecord->m_numCols; ++i) {
		u16 colNo = subRecord->m_columns[i];
		ColumnDef *colDef = tableDef->m_columns[colNo];
		if (colDef->m_nullable) {
			if (BitmapOper::isSet(record->m_data, bmSize, colDef->m_nullBitmapOffset)) {
				BitmapOper::setBit(subRecord->m_data, bmSize, colDef->m_nullBitmapOffset);
				// ��������ʽδʹ�õ��ڴ棬���ڼ�¼�Ƚ�
			} else {
				BitmapOper::clearBit(subRecord->m_data, bmSize, colDef->m_nullBitmapOffset);
				memcpy(subRecord->m_data + colDef->m_offset, record->m_data + colDef->m_offset, colDef->m_size);
			}
		} else {
			memcpy(subRecord->m_data + colDef->m_offset, record->m_data + colDef->m_offset, colDef->m_size);
		}

	}
	subRecord->m_rowId = record->m_rowId;
	subRecord->m_size = tableDef->m_maxRecSize;
}


/**
 * ��һ���䳤��ʽ�ļ�¼��ȡ���������Ե�ֵ���洢Ϊ�����ʽ��
 * ��;: �ӱ䳤�ѻ�䳤��¼���MMS��¼��ȡ��ѯ�������Է��ظ�MySQL
 *
 * @param tableDef ��¼�����ı���
 * @param record һ�������ļ�¼����m_formatһ��ΪREC_VARLEN
 * @param subRecord ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
 *   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������subRecord.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_REDUNDANT
 */
void RecordOper::extractSubRecordVR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert(ColList(subRecord->m_numCols, subRecord->m_columns).isAsc());
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);
	RedRow mRow(tableDef, subRecord, true);
	VarLenRow vRow(tableDef, record);

	uint idx = 0;
	Column column;
	for (Column* col = vRow.firstColumn(&column); col ; col = vRow.nextColumn(col)) {
		if (col->colNo() == subRecord->m_columns[idx]) { // �ҵ�ƥ����
			mRow.writeColumn(col->colNo(), col);
			if (++idx >= subRecord->m_numCols)
				break;
		}
	}
	assert(tableDef->m_maxRecSize == (int)mRow.size());
	subRecord->m_size = (uint)mRow.size();
	subRecord->m_rowId = record->m_rowId;
}

/** ��Ч��extractSubRecordVR */
void RecordOper::fastExtractSubRecordVR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert(ColList(subRecord->m_numCols, subRecord->m_columns).isAsc());
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);

	size_t bmBytes = tableDef->m_bmBytes;
	memset(subRecord->m_data, 0, bmBytes); // ��ʼ��λͼ
	size_t bmSize = (bmBytes << 3);
	byte *srcPtr = record->m_data + bmBytes; // Դָ��
	byte *srcEnd = record->m_data + record->m_size; // βָ��
	u16 srcColNo = 0; // Դ�к�
	uint dstColIdx = 0; // Ŀ��������
	while (srcPtr < srcEnd) {
		ColumnDef *colDef = tableDef->m_columns[srcColNo];
		size_t srcColSize; // �����ݳ���

		if (srcColNo == subRecord->m_columns[dstColIdx]) { // ƥ����
			// ��������ʽδʹ�õ��ڴ棬���ڼ�¼�Ƚ�
			if (colDef->m_nullable) {
				if (BitmapOper::isSet(record->m_data, bmSize, colDef->m_nullBitmapOffset)) {
					BitmapOper::setBit(subRecord->m_data, bmSize, colDef->m_nullBitmapOffset);
					srcColSize = 0;
				} else {
					BitmapOper::clearBit(subRecord->m_data, bmSize, colDef->m_nullBitmapOffset);
					srcColSize = getColSize(colDef, srcPtr);
				}
			} else {
				srcColSize = getColSize(colDef, srcPtr);
			}
			assert(colDef->m_size >= srcColSize);
			memcpy(subRecord->m_data + colDef->m_offset, srcPtr, srcColSize);
			if (++dstColIdx >= subRecord->m_numCols)
				break; // �Ѿ�����������д���ȡ��
		} else { // ��ƥ����
			srcColSize = (colDef->m_nullable
				&& BitmapOper::isSet(record->m_data, bmSize, colDef->m_nullBitmapOffset))
				? 0 : getColSize(colDef, srcPtr);
		}
		srcPtr += srcColSize;
		++srcColNo;
	}
	// ����ʣ����, ʣ�µĶ���NULL��
	for (; dstColIdx < subRecord->m_numCols; ++dstColIdx) {
		ColumnDef *colDef = tableDef->m_columns[subRecord->m_columns[dstColIdx]];
		assert(colDef->m_nullable);
		BitmapOper::setBit(subRecord->m_data, bmSize, colDef->m_nullBitmapOffset);
	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = record->m_rowId;
}
/**
* ��һ��������������ֵ���ɲ���������ֵ���洢Ϊ�����ʽ��
* ��;: ������ɨ���ʱ��������Ҫ�����ϲ�ָ��ĳЩ���Եļ�¼�����Ƕ�Ӧ������������¼
*
* @param tableDef ��¼�����ı���
* @param indexDef ��¼��������������
* @param record һ�������ļ�¼����m_formatһ��ΪKEY_COMPRESS
* @param subRecord ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
*   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������subRecord.m_size��֪
*   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_REDUNDANT
*/
void RecordOper::extractSubRecordCR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord) {
	assert(key->m_format == KEY_COMPRESS);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);
	assert(!indexDef->hasLob()); //�����ﲻ�ܺ��д���󣬷���û��ת��

	RedRow dstRow(tableDef, subRecord, true);
	CompressedKeyRow cRow(tableDef, indexDef, key);
	KeyRow::Iterator ki(&cRow, key);
	byte tmpBuf[8];
	Column tmpCol;
	for (ki.first(); !ki.end(); ki.next()) {
		Column *column = ki.column();
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // ����ƥ����
			if (subRecord->m_columns[i] == column->colNo()) { // �ҵ�һ��ƥ����
				tmpCol.setBuf(tmpBuf, sizeof(tmpBuf));
				dstRow.writeColumn(column->colNo(), CompressedKeyRow::decompressColumn(column, &tmpCol));
				break;
			}
		}

	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = key->m_rowId;
}

void RecordOper::extractSubRecordNR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord) {
	assert(key->m_format == KEY_NATURAL);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);
	assert(!indexDef->hasLob()); //�����ﲻ�ܺ��д���󣬷���û��ת��


	RedRow dstRow(tableDef, subRecord, true);
	KeyRow row(tableDef, indexDef, key);
	KeyRow::Iterator ki(&row, key);
	for (ki.first(); !ki.end(); ki.next()) {
		Column *column = ki.column();
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // ����ƥ����
			if (subRecord->m_columns[i] == column->colNo()) { // �ҵ�һ��ƥ����
				dstRow.writeColumn(column->colNo(), column);
				break;
			}
		}

	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = key->m_rowId;
}

void RecordOper::extractSubRecordPR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord) {
	assert(key->m_format == KEY_PAD);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);
	assert(!indexDef->hasLob());

	RedRow dstRow(tableDef, subRecord, true);
	KeyRow row(tableDef, indexDef, key);
	KeyRow::Iterator ki(&row, key);
	for (ki.first(); !ki.end(); ki.next()) {
		Column *column = ki.column();
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // ����ƥ����
			if (subRecord->m_columns[i] == column->colNo()) { // �ҵ�һ��ƥ����
				dstRow.writeColumn(column->colNo(), column);
				break;
			}
		}

	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = key->m_rowId;
}

/**
* ��һ��������������ֵ���ɲ����Ӽ�¼��ֵ���洢Ϊ�����ʽ�����й��˵�������С�
* ��;: TNT��ʼ���趨Auto_increment��ʱ����ɨ�����������ȡ�����ֵ
*
* @param tableDef ��¼�����ı���
* @param indexDef ��¼��������������
* @param record һ�������ļ�¼����m_formatһ��ΪKEY_COMPRESS
* @param subRecord ������������������ʾҪȡ�������ԣ����Ϊ����ֵ��ռ�ÿռ��С
*   �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������subRecord.m_size��֪
*   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_REDUNDANT
*/
void RecordOper::extractSubRecordCRNoLobColumn(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord) {
	assert(key->m_format == KEY_COMPRESS);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);

	RedRow dstRow(tableDef, subRecord, true);
	CompressedKeyRow cRow(tableDef, indexDef, key);
	KeyRow::Iterator ki(&cRow, key);
	byte tmpBuf[8];
	Column tmpCol;
	for (ki.first(); !ki.end(); ki.next()) {
		Column *column = ki.column();
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // ����ƥ����
			if (subRecord->m_columns[i] == column->colNo() && !column->def()->isLob()) { // �ҵ�һ��ƥ��ķǴ������
				tmpCol.setBuf(tmpBuf, sizeof(tmpBuf));
				dstRow.writeColumn(column->colNo(), CompressedKeyRow::decompressColumn(column, &tmpCol));
				break;
			}
		}

	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = key->m_rowId;
}

void RecordOper::extractSubRecordPRNoLobColumn(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord) {
	assert(key->m_format == KEY_PAD);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);

	RedRow dstRow(tableDef, subRecord, true);
	KeyRow row(tableDef, indexDef, key);
	KeyRow::Iterator ki(&row, key);
	for (ki.first(); !ki.end(); ki.next()) {
		Column *column = ki.column();
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // ����ƥ����
			if (subRecord->m_columns[i] == column->colNo() && !column->def()->isLob() ) { // �ҵ�һ��ƥ����
				dstRow.writeColumn(column->colNo(), column);
				break;
			}
		}

	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = key->m_rowId;
}


void RecordOper::extractLobFromR(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
							   LobStorage *lobStorage, const Record *record, Array<LobPair *> *lobArray) {
   assert(record->m_format == REC_REDUNDANT);
   assert(indexDef->hasLob());

   for (u16 colNum = 0; colNum < indexDef->m_numCols; colNum++) {
	   u16 cno = indexDef->m_columns[colNum];
	   ColumnDef *colDef = tableDef->getColumnDef(cno);
	   if (!colDef->isLob())
		   continue;
	   uint lobSize = 0;
	   byte *lob = NULL;
	   if (! RecordOper::isNullR(tableDef, record, cno)) {
		   LobId lobId = RecordOper::readLobId(record->m_data, colDef);
		   assert(lobId != INVALID_LOB_ID);
		   lob = lobStorage->get(session, session->getLobContext(), lobId, &lobSize, false);
		   assert(lob);
	   }
	   lobArray->push(new (session->getMemoryContext()->alloc(sizeof (LobPair)))LobPair(lob, lobSize));
   }
}

void RecordOper::extractLobFromR(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
							   LobStorage *lobStorage, const SubRecord *subRec, Array<LobPair *> *lobArray) {
   assert(subRec->m_format == REC_REDUNDANT);
   assert(indexDef->hasLob());

   for (u16 colNum = 0; colNum < indexDef->m_numCols; colNum++) {
	   u16 cno = indexDef->m_columns[colNum];
	   ColumnDef *colDef = tableDef->getColumnDef(cno);
	   if (!colDef->isLob())
		   continue;
	   uint lobSize = 0;
	   byte *lob = NULL;
	   if (! RecordOper::isNullR(tableDef, subRec, cno)) {
		   LobId lobId = RecordOper::readLobId(subRec->m_data, colDef);
		   assert(lobId != INVALID_LOB_ID);
		   lob = lobStorage->get(session, session->getLobContext(), lobId, &lobSize, false);
		   assert(lob);
	   }
	   lobArray->push(new (session->getMemoryContext()->alloc(sizeof (LobPair)))LobPair(lob, lobSize));
   }
}

/**
* ��һ����REC_MYSQL��ʽ�洢�ļ�¼����ȡ����������ݡ�
* ��;: NTSE�ָ�ʱ������logic_redoʱʹ��
*
* @param tableDef ��¼�����ı���
* @param indexDef ��¼��������������
* @param subRec �Ӽ�¼����m_columns�еĴ������ΪREC_MYSQL�洢��ʽ
* @param lobArray ������������������ʾҪȡ���Ĵ��������
*  
*/
void RecordOper::extractLobFromM(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
							   const Record *record, Array<LobPair *> *lobArray) {
   assert(indexDef->hasLob());

   for (u16 colNum = 0; colNum < indexDef->m_numCols; colNum++) {
	   u16 cno = indexDef->m_columns[colNum];
	   ColumnDef *colDef = tableDef->getColumnDef(cno);
	   if (!colDef->isLob())
		   continue;
	   uint lobSize = 0;
	   byte *lob = NULL;
	   if (! RecordOper::isNullR(tableDef, record, cno)) {
		   lob = RecordOper::readLob(record->m_data, colDef);
		   assert(lob);
		   lobSize = RecordOper::readLobSize(record->m_data, colDef);
	   }
	   lobArray->push(new (session->getMemoryContext()->alloc(sizeof (LobPair)))LobPair(lob, lobSize));
   }
}

void RecordOper::extractLobFromMixedMR(Session *session, const TableDef *tableDef, const IndexDef *indexDef, LobStorage *lobStorage,
						   const Record *record, u16 numColumnsInMysqlFormat, u16 *columnsInMysqlFormat, Array<LobPair *> *lobArray) {
   assert(indexDef->hasLob());

   for (u16 colNum = 0; colNum < indexDef->m_numCols; colNum++) {
	   u16 cno = indexDef->m_columns[colNum];
	   ColumnDef *colDef = tableDef->getColumnDef(cno);
	   if (!colDef->isLob())
		   continue;
	   uint lobSize = 0;
	   byte *lob = NULL;
	   bool isColMysqlFormat = false;
	   for (u16 colNoInMysqlFormat = 0; colNoInMysqlFormat < numColumnsInMysqlFormat; colNoInMysqlFormat++) {
		   if (columnsInMysqlFormat[colNoInMysqlFormat] == cno){
			   isColMysqlFormat = true;
			   break;
		   }
	   }
	   if (! RecordOper::isNullR(tableDef, record, cno)) {
		   if (isColMysqlFormat) {
			   lob = RecordOper::readLob(record->m_data, colDef);
			  
			   lobSize = RecordOper::readLobSize(record->m_data, colDef);

		   } else {
			   LobId lobId = RecordOper::readLobId(record->m_data, colDef);
			   assert(lobId != INVALID_LOB_ID);
			   lob = lobStorage->get(session, session->getLobContext(), lobId, &lobSize, false);
		   }
		   assert(lob);
	   }
	   lobArray->push(new (session->getMemoryContext()->alloc(sizeof (LobPair)))LobPair(lob, lobSize));
   }
}


/**
 * ��MYSQL��ʽ�ļ�¼���ϲ��ʽת����������ʽ
 * ��;: ���г����ֶεļ�¼����
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_MYSQL
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_MYSQL
 */
void RecordOper::convertRecordMUpToEngine(const TableDef *tableDef, const Record *src, Record *dest) {
	assert(src->m_format == REC_UPPMYSQL);
	assert(dest->m_format == REC_MYSQL);
	assert(tableDef->hasLongVar());

	MysqlRow upRow(tableDef, src);
	MysqlRow engineRow(tableDef, dest);
	Column column;
	for (Column *col = upRow.firstColumn(&column); col; col = upRow.nextColumn(col)) {
		engineRow.writeColumnToEngineLayer(col->colNo(), col);
	}
	dest->m_size = tableDef->m_maxRecSize;
	dest->m_rowId = src->m_rowId;
}


/**
 * ��MYSQL��ʽ�ļ�¼ת��������ʽתΪ�ϲ��ʽ
 * ��;: ���г����ֶεļ�¼����
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_MYSQL
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_UPPMYSQL
 */
void RecordOper::convertRecordMEngineToUp(const TableDef *tableDef, const Record *src, Record *dest) {
	assert(src->m_format == REC_MYSQL);
	assert(dest->m_format == REC_UPPMYSQL);
	assert(tableDef->hasLongVar());

	MysqlRow engineRow(tableDef, src);
	MysqlRow upRow(tableDef, dest);
	Column column;
	for (Column *col = engineRow.firstColumn(&column); col; col = engineRow.nextColumn(col)) {
		upRow.writeColumnToUpperLayer(col->colNo(), col);
	}
	dest->m_size = tableDef->m_maxMysqlRecSize;
	dest->m_rowId = src->m_rowId;
}
/**
 * ��MYSQL��ʽ���Ӽ�¼���ϲ��ʽת����������ʽ
 * ��;: ���г����ֶεļ�¼����
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_MYSQL
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_MYSQL
 */
void RecordOper::convertSubRecordMUpToEngine(const TableDef *tableDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == REC_UPPMYSQL);
	assert(dest->m_format == REC_MYSQL);
	assert(tableDef->hasLongVar());
	assert(ColList(src->m_numCols, src->m_columns).isAsc());
	
	MysqlRow upRow(tableDef, src);
	MysqlRow engineRow(tableDef, dest);
	
	Column col;
	for (size_t i = 0; i < src->m_numCols; i++) {
		u16 colIdx = src->m_columns[i];
		engineRow.writeColumnToEngineLayer(colIdx, upRow.columnAt(colIdx, &col));
	}

	dest->m_size = tableDef->m_maxRecSize;
	dest->m_rowId = src->m_rowId;
}


/**
 * ��MYSQL��ʽ���Ӽ�¼ת��������ʽתΪ�ϲ��ʽ
 * ��;: ���г����ֶεļ�¼����
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_MYSQL
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_UPPMYSQL
 */
void RecordOper::convertSubRecordMEngineToUp(const TableDef *tableDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == REC_MYSQL);
	assert(dest->m_format == REC_UPPMYSQL);
	assert(tableDef->hasLongVar());
	assert(ColList(src->m_numCols, src->m_columns).isAsc());

	MysqlRow engineRow(tableDef, src);
	MysqlRow upRow(tableDef, dest);
	Column col;
	for (size_t i = 0; i < src->m_numCols; i++) {
		u16 colIdx = src->m_columns[i];
		upRow.writeColumnToUpperLayer(colIdx, engineRow.columnAt(colIdx, &col));
	}
	dest->m_size = tableDef->m_maxMysqlRecSize;
	dest->m_rowId = src->m_rowId;
}


/**
 * �������ʽ�ļ�¼ת��Ϊ�䳤��ʽ�ļ�¼
 * ��;: �����¼
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_REDUNDANT
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_VARLEN
 */
void RecordOper::convertRecordRV(const TableDef *tableDef, const Record *src, Record *dest) {
	assert(src->m_format == REC_REDUNDANT);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert(dest->m_format == REC_VARLEN);

	RedRow mRow(tableDef, src);
	VarLenRow vRow(tableDef, dest->m_data, dest->m_size, 0);
	Column column;
	for (Column* col = mRow.firstColumn(&column); col ; col = mRow.nextColumn(col)) {
		vRow.appendColumn(col);
	}
	dest->m_size = (uint)vRow.size();
	dest->m_rowId = src->m_rowId;
}
/**
 * ���䳤��ʽ�ļ�¼ת��Ϊ������ֵļ�¼
 * ��;: �ָ����������ݿ�ά������
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_VARLEN
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_REDUNDANT
 */
void RecordOper::convertRecordVR(const TableDef *tableDef, const Record *src, Record *dest) {
	assert(src->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert(dest->m_format == REC_REDUNDANT);

	VarLenRow vRow(tableDef, src->m_data, src->m_size, src->m_size);
	RedRow  mRow(tableDef, dest);
	Column column;
	for (Column* col = vRow.firstColumn(&column); col ; col = vRow.nextColumn(col)) {
		mRow.writeColumn(col->colNo(), col);
	}
	dest->m_size = (uint)mRow.size();
	dest->m_rowId = src->m_rowId;
}

/** ���䳤���߶�����¼ת��Ϊ�����ʽ
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_VARLEN����REC_FIXLEN
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_REDUNDANT
 */
void RecordOper::convertRecordVFR(TableDef *tableDef, Record *src, Record *dest) {
	NTSE_ASSERT(src->m_format == REC_FIXLEN || src->m_format == REC_VARLEN);
	NTSE_ASSERT(dest->m_format == REC_REDUNDANT);
	if (src->m_format == REC_VARLEN) {
		RecordOper::convertRecordVR(tableDef, src, dest);
	} else {
		dest->m_rowId = src->m_rowId;
		dest->m_size = src->m_size;
		memcpy(dest->m_data, src->m_data, src->m_size);
	}
}

/**
 * �������ʽ���Ӽ�¼ת��Ϊ�䳤��ʽ���Ӽ�¼
 * ��;: MMSд��־ʱ��Ҫת�������ʽΪ�䳤��ʽ�Ӽ�¼�Խ�ʡ�ռ�
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_REDUNDANT
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪREC_VARLEN
 */
void RecordOper::convertSubRecordRV(const TableDef *tableDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == REC_REDUNDANT);
	assert(src->m_size == tableDef->m_maxRecSize);
	assert(ColList(src->m_numCols, src->m_columns).isAsc());
	assert(dest->m_format == REC_VARLEN);

	RedRow mRow(tableDef, src);
	VarLenRow vRow(tableDef, dest->m_data, dest->m_size, 0, REC_VARLEN);
	Column column;
	for (u16 i = 0; i < src->m_numCols; ++i)
		vRow.appendColumn(mRow.columnAt(src->m_columns[i], &column));
	dest->m_size = (uint)vRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
 * ���䳤��ʽ���Ӽ�¼ת��Ϊ�����ʽ���Ӽ�¼
 * ��;: MMS������־ʱ��Ҫת���䳤��ʽ�Ӽ�¼Ϊ�����ʽ�Ӽ�¼
 *
 * @param tableDef ��¼�����ı���
 * @param src һ�������ļ�¼����m_formatһ��ΪREC_VARLEN
 * @param dest ��������������߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
 *   �Ѿ�������ڴ��С����ֹԽ�硣��m_formatһ��ΪRECREDUNDANT
 */
void RecordOper::convertSubRecordVR(const TableDef *tableDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == REC_VARLEN);
	assert(ColList(src->m_numCols, src->m_columns).isAsc());
	assert(dest->m_format == REC_REDUNDANT);
	assert(dest->m_size >= tableDef->m_maxRecSize);

	VarLenRow vRow(tableDef, src->m_data, src->m_size, src->m_size, REC_VARLEN);
	VSubRecordIterator iter(&vRow, src);
	RedRow  mRow(tableDef, dest, true);
	for (iter.first(); !iter.end(); iter.next())
		mRow.writeColumn(iter.column()->colNo(), iter.column());
	dest->m_size = (uint)mRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
 * ����һ�������ʽ�ļ�¼�����ǽ��б��ظ���
 *
 * @param tableDef ��¼�����ı���
 * @param record Ҫ���µļ�¼����m_formatһ��ΪREC_REDUNDANT
 * @param update Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
 */
void RecordOper::updateRecordRR(const TableDef *tableDef, Record *record, const SubRecord *update) {
	assert(record->m_format == REC_REDUNDANT);
	assert(update->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize == update->m_size);
	assert((uint)tableDef->m_maxRecSize == record->m_size);
	RedRow row(tableDef, record);
	RedRow upd(tableDef, update);
	Column column;
	for (u16 i = 0; i < update->m_numCols; ++i) {
		u16 colNo = update->m_columns[i];
		row.writeColumn(colNo, upd.columnAt(colNo, &column));
	}
}

/**
 * ����һ��������¼�����ǽ��б��ظ���
 *
 * @param tableDef ��¼�����ı���
 * @param record Ҫ���µļ�¼����m_formatһ��ΪREC_FIXLEN
 * @param update Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
 */
void RecordOper::updateRecordFR(const TableDef *tableDef, Record *record, const SubRecord *update) {
	assert(record->m_format == REC_FIXLEN);
	assert(tableDef->m_recFormat == REC_FIXLEN);
	assert(update->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize == update->m_size);
	FixedLenRow fRow(tableDef, record);
	RedRow mRow(tableDef, update);
	Column column;
	for (u16 i = 0; i < update->m_numCols; ++i) {
		u16 colNo = update->m_columns[i];
		fRow.writeColumn(colNo, mRow.columnAt(colNo, &column));
	}
}


/**
 * �õ���¼���º�Ĵ�С
 *
 * @param tableDef ��¼�����ı���
 * @param record Ҫ���µļ�¼����m_formatһ��ΪREC_VARLEN
 * @param update Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
 */
u16 RecordOper::getUpdateSizeVR(const TableDef *tableDef, const Record *record, const SubRecord *update) {
	assert(update->m_format == REC_REDUNDANT);
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert((uint)tableDef->m_maxRecSize == update->m_size);
	RedRow mRow(tableDef, update); // ������Ϣ
	VarLenRow oldRow(tableDef, record);

	int deltaSize = 0;
	uint idx = 0;
	Column column, column1;
	for (Column* col = oldRow.firstColumn(&column); col; col = oldRow.nextColumn(col)) {
		if (col->colNo() == update->m_columns[idx]) { // �ҵ�һ��ƥ����
			Column* updatedCol = mRow.columnAt(col->colNo(), &column1);
			deltaSize += ((int)updatedCol->size() - (int)col->size());
			if (++idx >= update->m_numCols)
				break;
		}
	}

	return (u16)((int)oldRow.size() + deltaSize);
}

/**
 * ��ü�¼���º�Ĵ�С(���º�ļ�¼������ѹ��)
 * @pre ����뺬���ֵ䣬����¼ǰ�������ѹ����ʽҲ�����Ǳ䳤��ʽ
 * @param mtx      �ڴ����������
 * @param tableDef ����
 * @param cprsRcdExtrator ѹ����¼��ȡ��
 * @param oldRcd ��¼ǰ�񣬿�����ѹ����ʽҲ�����Ǳ䳤��ʽ
 * @param update ���º��������ʽ
 * @return 
 */
u16 RecordOper::getUpdateSizeNoCompress(MemoryContext *mtx, const TableDef *tableDef, 
										CmprssRecordExtractor *cprsRcdExtrator, 
										const Record *oldRcd, const SubRecord *update) {
	assert(REC_COMPRESSED == oldRcd->m_format || REC_VARLEN == oldRcd->m_format);
	assert(REC_REDUNDANT == update->m_format);

	if (REC_COMPRESSED == oldRcd->m_format) {
		McSavepoint msp(mtx);
		CmprssColGrpExtractInfo cmpressExtractInfo(tableDef, update->m_numCols, update->m_columns, mtx);
		byte *tmpDecompressBuf = (byte *)mtx->alloc(tableDef->m_maxRecSize);//���ڻ����ѹ���������
		
		RedRow mRow(tableDef, update);
		CompressedRow oldRow(tableDef, oldRcd);
		CompressedColGroup cmprsColGrp;
		int oldRecUncompressSize = tableDef->m_bmBytes;//ԭ��¼��ѹ����Ĵ�С
		int deltaSize = 0;

		for (CompressedColGroup *colGrp = oldRow.firstColGrp(&cmprsColGrp); colGrp; 
			colGrp = oldRow.nextColGrp(colGrp)) {
				u8 colGrpNo = colGrp->colGrpNo();
				if (cmpressExtractInfo.m_colNumNeedReadInGrp[colGrpNo] > 0) {
					//������ѹ����������Ҫ����,�Ƚ�ѹ,���º���ѹ��
					uint decompressSize = 0;//�������ѹ��֮��ĳ���
					cprsRcdExtrator->decompressColGroup(colGrp->data(), (uint)colGrp->lenBytes(), 
						colGrp->getRealSize(), tmpDecompressBuf, &decompressSize);
					assert(decompressSize <= tableDef->m_maxRecSize);
					oldRecUncompressSize += (int)decompressSize;

					//����ǰ�������ĳ���delta
					ColGroupRow cgRow(tableDef, colGrpNo, tmpDecompressBuf, decompressSize, oldRcd->m_data);
					Column column1, column2;
					for (Column *col = cgRow.firstColumn(&column1); col; col = cgRow.nextColumn(col)) {
						u16 colNo = col->colNo();
						if (cmpressExtractInfo.m_colNeedRead[colNo] > 0) {
							mRow.columnAt(colNo, &column2);
							deltaSize += (int)column2.size() - (int)col->size();
						}
					}
				} else {
					oldRecUncompressSize += (int)cprsRcdExtrator->calcColGrpDecompressSize(
						colGrp->data(), (uint)colGrp->lenBytes(), colGrp->getRealSize());
				}
		}
		return (u16)(oldRecUncompressSize + deltaSize);
	} else {
		return getUpdateSizeVR(tableDef, oldRcd, update);
	}
}

/**
 * ���ظ��±䳤��¼
 *
 * @param tableDef ��¼�����ı���
 * @param record Ҫ���µļ�¼����m_formatһ��ΪREC_VARLEN
 * @param update Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
 * @param oldBufferSize record.m_data�ܹ�ʹ�õ�����ڴ��С
 */
void RecordOper::updateRecordVRInPlace(const TableDef *tableDef, Record *record,
									   const SubRecord *update, size_t oldBufferSize) {
	return doUpdateRecordVRInPlace(tableDef, record, update, oldBufferSize);
}


/**
 * �Ǳ��ظ��±䳤��¼
 *
 * @param tableDef ��¼�����ı���
 * @param record Ҫ���µļ�¼����m_formatһ��ΪREC_VARLEN
 * @param update Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
 * @param newBuf �洢�¼�¼���ڴ�
 * @return ����֮��ļ�¼��С
 */
uint RecordOper::updateRecordVR(const TableDef *tableDef, const Record *record, const SubRecord *update,
								byte *newBuf) {
	assert(update->m_format == REC_REDUNDANT);
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert((uint)tableDef->m_maxRecSize == update->m_size);
	RedRow mRow(tableDef, update); // ������Ϣ
	VarLenRow oldRow(tableDef, record);
	// ������Ϊ�´����о������޿ռ䣬�����߱��뱣֤newBuf�ռ��㹻
	VarLenRow newRow(tableDef, newBuf, (size_t)-1, 0);
	uint idx = 0;
	Column column, column1;
	for (Column* col = oldRow.firstColumn(&column);	col; col = oldRow.nextColumn(col)) {
		if (idx < update->m_numCols && col->colNo() == update->m_columns[idx]){ // �ҵ�һ��ƥ����
			newRow.appendColumn(mRow.columnAt(col->colNo(), &column1));
			++idx;
		} else {
			newRow.appendColumn(col);
		}
	}
	return newRow.size();
}


/**
 * �ж�ָ��������ѹ����ʽ�ļ�ֵ�ܷ���п��ٵĴ�С�Ƚ�
 *
 * @param tableDef ��ֵ�����ı�
 * @param indexDef ��������
 * @param numKeyCols ������ֵ�а�����������
 * @param keyCols ��ֵ��ÿ������Ϊ���еĵڼ�������
 * @return �ܽ��п��ٴ�С�Ƚ�ʱ����true�����򷵻�false
 */
bool RecordOper::isFastCCComparable(const TableDef *tableDef, const IndexDef *indexDef, u16 numKeyCols, const u16 *keyCols) {
	if (numKeyCols != indexDef->m_numCols)
		return false;
	if (indexDef->m_bmBytes > 1)	// Ϊ��߱Ƚ�ʱ�����ܣ�ֻ�������Ŀ�ΪNULL�����Բ�����8�������
		return false;

	for (u16 i = 0; i < numKeyCols; ++i) {
		assert(keyCols[i] < tableDef->m_numCols);
		ColumnDef* columnDef = tableDef->m_columns[keyCols[i]];
		if (columnDef->m_type != CT_SMALLINT
			&& columnDef->m_type != CT_MEDIUMINT
			&& columnDef->m_type != CT_INT
			&& columnDef->m_type != CT_BIGINT
			&& columnDef->m_type != CT_RID
			&& columnDef->m_type != CT_BINARY)	// CHAR��VARCHAR���ܿ��ٱȽϣ���ʹcollationΪCOLL_BIN)
			return false; 						// TINYINTû�б�ѹ������˲���memcmp
		if (columnDef->m_prtype.isUnsigned())
			return false;
	}
	return true;
}

/**
 * �Ƚ�����ѹ����ʽ��������
 * ��;: ����������������ֻ���������ȿ���ֱ��memcmp�Ƚϵ�����ʱ��ʹ����һ�����Ƚ�
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_COMPRESS
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_COMPRESS
 * @param indexDef �Ƚϼ�ֵ��������������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyCC(const TableDef* tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	UNREFERENCED_PARAMETER(tableDef);
	assert(key1->m_format == KEY_COMPRESS);
	assert(key2->m_format == KEY_COMPRESS);
	assert(key1->m_numCols == indexDef->m_numCols && key2->m_numCols == indexDef->m_numCols);

	if (indexDef->m_bmBytes == 0 || (indexDef->m_bmBytes == 1 && *key1->m_data == 0 && *key2->m_data == 0)) {
		int res = memcmp(key1->m_data, key2->m_data, min(key1->m_size, key2->m_size));
		return res ? res : ((int)key1->m_size - (int)key2->m_size);
	} else {
		byte *padBuf = (byte *)alloca(indexDef->m_maxKeySize);
		SubRecord padKey(KEY_PAD, key1->m_numCols, key1->m_columns, padBuf, indexDef->m_maxKeySize);
		convertKeyCP(tableDef, indexDef, key1, &padKey);
		return compareKeyPC(tableDef, &padKey, key2, indexDef);
	}
}

/**
 * �Ƚ�һ�������ʽ����һ��ѹ����ʽ��������
 * ��;: �������������ܽ���memcmp�Ƚ��ұ�Ϊ������¼ʱ����һ����
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪREC_REDUNDANT��Ϊ������
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_COMPRESS
 * @param indexDef �Ƚϼ�ֵ�������������壬Ϊ���ṩ��compareKeyCCһ�µĽӿڶ����ӣ���compareKeyRC��˵��һ��������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyRC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	UNREFERENCED_PARAMETER(indexDef);
	
	assert(key1->m_format == REC_REDUNDANT);
	assert(key2->m_format == KEY_COMPRESS);
	assert((uint)tableDef->m_maxRecSize <= key1->m_size);
	assert(!indexDef->hasLob());

	int result = 0;
	Column column1;
	RedRow mRow(tableDef, key1);
	CompressedKeyRow cRow(tableDef, indexDef, key2);
	KeyRow::Iterator cRowIter(&cRow, key2);
	u16 count = 0; // �ѱȽ�����
	for (cRowIter.first(); result == 0 && !cRowIter.end() && count < key1->m_numCols; cRowIter.next()) {
		Column* col2 = cRowIter.column();
		u16 colNo = col2->colNo();
		mRow.columnAt(colNo, &column1);
		result = compareColumn(&column1, col2, true);
		++ count;
	}
	return result;
}

/**	�Ƚ�������ֵ����������Ӧ�ĶѼ�¼
*	��;��TNT���棬��double check֮��
*	@tableDef	����
*	@key1		������ֵ
*	@key2		�Ѽ�¼
*	@indexDef	��������
*	@return ����-1/0/1(С�ڣ����ڣ�����)
*/
int RecordOper::compareKeyRR(const ntse::TableDef *tableDef, const ntse::SubRecord *key1, 
							 const ntse::SubRecord *key2, const ntse::IndexDef *indexDef) {
	assert(key1->m_format == REC_REDUNDANT);
	assert(key2->m_format == REC_REDUNDANT);
	assert(!indexDef->m_hasLob);
	
	int result = 0;
	Column column1, column2;
	RedRow rowSub(tableDef, key1);
	RedRow rowFull(tableDef, key2);

	// ֻ�Ƚ�������ֵ��
	for(u16 i = 0; i < indexDef->m_numCols; i++) {
		u16 cno = indexDef->m_columns[i];

		rowSub.columnAt(cno, &column1);
		rowFull.columnAt(cno, &column2);
		
		result = compareColumn(&column1, &column2);
		if (result != 0)
			return result;
	}

	return result;
}

/**	�Ƚ�KEY_PAD��ʽ�������������ʽ������
*	@tableDef	����
*	@key1		������ֵ
*	@key2		�Ѽ�¼
*	@indexDef	��������
*	@return ����-1/0/1(С�ڣ����ڣ�����)
*/
int RecordOper::compareKeyPR(const TableDef *tableDef, const SubRecord *key1, 
							 const SubRecord *key2, const IndexDef *indexDef) {
	assert(key1->m_format == KEY_PAD);	
	assert(key2->m_format == REC_REDUNDANT);
	assert(!indexDef->hasLob());
	
	KeyRow r1(tableDef, indexDef, key1);
	RedRow r2(tableDef, key2);

	KeyRow::Iterator it(&r1, key1);
	Column column2;
	int result = 0;
	for (it.first(); result == 0 && !it.end(); it.next()) {
		Column *col1 = it.column();
		r2.columnAt(col1->colNo(), &column2);
		result = compareColumn(col1, &column2);
	}

	return result;
}

/**
 * �Ƚ�һ������ʽ����һ��ѹ����ʽ��������
 * ��;: SELECT����ɨ�裬����ѹ����ֱ�ӱȽϵ�
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_PAD��Ϊ������
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_COMPRESS
 * @param indexDef �Ƚϼ�ֵ�������������壬Ϊ���ṩ��compareKeyCCһ�µĽӿڶ����ӣ��Ա���˵��һ��������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyPC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	assert(KEY_PAD == key1->m_format);
	assert(KEY_COMPRESS == key2->m_format);
	return compareKeyPCOrNC(tableDef, key1, key2, indexDef);
}

/**
 * �Ƚ���Ȼ��ʽ��ѹ����ʽ��������
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_NATURAL��Ϊ������
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_COMPRESS
 * @param indexDef �Ƚϼ�ֵ�������������壬Ϊ���ṩ��compareKeyCCһ�µĽӿڶ����ӣ��Ա��ӿ���˵��һ��������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyNC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	assert(KEY_NATURAL == key1->m_format);
	assert(KEY_COMPRESS == key2->m_format);
	return compareKeyPCOrNC(tableDef, key1, key2, indexDef);
}

/**
 * �Ƚ�PAD��ʽ(����Ȼ��ʽ)��ѹ����ʽ��������
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_PAD��KEY_NATURAL��Ϊ������
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_COMPRESS
 * @param indexDef �Ƚϼ�ֵ�������������壬Ϊ���ṩ��compareKeyCCһ�µĽӿڶ����ӣ��Ա��ӿ���˵��һ��������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyPCOrNC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	UNREFERENCED_PARAMETER(indexDef);

	assert(key1->m_format == KEY_PAD || key1->m_format == KEY_NATURAL);
	assert(key2->m_format == KEY_COMPRESS);

	KeyRow r1(tableDef, indexDef, key1);
	CompressedKeyRow r2(tableDef, indexDef, key2);
	KeyRow::Iterator iter1(&r1, key1);
	KeyRow::Iterator iter2(&r2, key2);
	int result = 0;
	for (iter1.first(), iter2.first();
		result ==0 && !iter1.end() && !iter2.end();
		iter1.next(), iter2.next()) {
			result = compareColumn(iter1.column(), iter2.column(), true, true, true);
	}
	return result;
}

/**
 * �Ƚ���Ȼ��ʽ��PAD��ʽ��������
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_NATURE��Ϊ������
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_PAD
 * @param indexDef �Ƚϼ�ֵ�������������壬Ϊ���ṩ��compareKeyCCһ�µĽӿڶ����ӣ��Ա��ӿ���˵��һ��������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyNP(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, 
							 const IndexDef *indexDef) {
	assert(key1->m_format == KEY_NATURAL);
	assert(key2->m_format == KEY_PAD);

	UNREFERENCED_PARAMETER(indexDef);
	int result = 0; 
	KeyRow r1(tableDef, indexDef, key1);
	KeyRow r2(tableDef, indexDef, key2);
	KeyRow::Iterator iter1(&r1, key1);
	KeyRow::Iterator iter2(&r2, key2);
	
	for (iter1.first(), iter2.first();
		result ==0 && !iter1.end() && !iter2.end();
		iter1.next(), iter2.next()) {
			result = compareColumn(iter1.column(), iter2.column(), false, true, true);
	}
	return result;
}

/**
 * �Ƚ�������Ȼ��ʽ����ѹ������������
 * ��;: ��������ʱ�Ƚ�
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪREC_NATURAL
 * @param key2 ����Ƚ���2����m_formatһ��ΪREC_NATURAL
 * @param indexDef �Ƚϼ�ֵ�������������壬Ϊ���ṩ��compareKeyCCһ�µĽӿڶ����ӣ���compareKeyNN��˵��һ��������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyNN(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	UNREFERENCED_PARAMETER(indexDef);
	
	assert(key1->m_format == KEY_NATURAL);
	assert(key2->m_format == KEY_NATURAL);

	return realCompareKeyNNorPP(tableDef, key1, key2, indexDef);
}


/**
 * �Ƚ�����PAD��ʽ����ѹ������������
 * ��;: Key-Value�ӿ���multi-get��������ֵ����ʱʹ��
 *
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_PAD
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_PAD
 * @param indexDef �Ƚϼ�ֵ�������������壬Ϊ���ṩ��compareKeyCCһ�µĽӿڶ����ӣ���compareKeyNN��˵��һ��������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::compareKeyPP( const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef /*= NULL*/ ) {
	UNREFERENCED_PARAMETER(indexDef);

	assert(key1->m_format == KEY_PAD);
	assert(key2->m_format == KEY_PAD);

	return realCompareKeyNNorPP(tableDef, key1, key2, indexDef);
}

/** NN��PP��ʽ������ֵ�Ƚϵ�����ʵ��
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_PAD
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_PAD
 * @param indexDef ������������
 * @return key1 = key2ʱ����0��key1 < key2ʱ����<0��key1 > key2ʱ����>0
 */
int RecordOper::realCompareKeyNNorPP( const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	assert(key1->m_format == key2->m_format);
	assert(key1->m_format == KEY_NATURAL || key1->m_format == KEY_PAD);
	//assert(key1->m_numCols == key2->m_numCols);

	int result = 0;
	KeyRow row1(tableDef, indexDef, key1);
	KeyRow row2(tableDef, indexDef, key2);
	KeyRow::Iterator iter1(&row1, key1);
	KeyRow::Iterator iter2(&row2, key2);

	for (iter1.first(), iter2.first(); result == 0 && !iter1.end() && !iter2.end();
		iter1.next(), iter2.next()) {
			Column *col1 = iter1.column();
			Column *col2 = iter2.column();
			assert(col1->colNo() == col2->colNo());
			assert(col1->colNo() < tableDef->m_numCols);
			result = compareColumn(col1, col2, false, true, true);
	}

	return result;
}

/** NN��PP��ʽ������ֵ�Ƚ�ÿ�����Եĳ���
 * @param tableDef ��������
 * @param key1 ����Ƚ���1����m_formatһ��ΪKEY_PAD ���� Key_NATURE
 * @param key2 ����Ƚ���2����m_formatһ��ΪKEY_PAD
 * @param indexDef ������������
 * @return �����ֶ�key1 key2 size���ʱ����0�����򷵻ط�0
 */
int RecordOper::compareKeyNNorPPColumnSize( const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	assert(key1->m_format == key2->m_format);
	assert(key1->m_format == KEY_NATURAL || key1->m_format == KEY_PAD);
	int result = 0;
	KeyRow row1(tableDef, indexDef, key1);
	KeyRow row2(tableDef, indexDef, key2);
	KeyRow::Iterator iter1(&row1, key1);
	KeyRow::Iterator iter2(&row2, key2);

	for (iter1.first(), iter2.first(); result == 0 && !iter1.end() && !iter2.end();
		iter1.next(), iter2.next()) {
			Column *col1 = iter1.column();
			Column *col2 = iter2.column();
			assert(col1->colNo() == col2->colNo());
			assert(col1->colNo() < tableDef->m_numCols);
			if(col1->size() > col2->size()) 
				return 1;
			else if(col1->size() < col2->size())
				return -1;
	}
	return 0;
}



template <typename IterType1, typename IterType2>
bool isSubRecordEq(IterType1 &iter1, IterType2 &iter2, bool cmpLob = false) {
	int result = 0;
	for (iter1.first(), iter2.first(); result == 0 && !iter1.end() && !iter2.end();
		iter1.next(), iter2.next()) {
			Column *col1 = iter1.column();
			Column *col2 = iter2.column();
			assert(col1->colNo() == col2->colNo());
			result = compareColumn(col1, col2, false, cmpLob);
	}
	return result == 0;
}

/**
 * �Ƚ�������ͬ��ʽ�Ӽ�¼�Ƿ����
 *
 * @param tableDef ��������
 * @param sb1 ����Ƚ���1
 * @param sb2 ����Ƚ���2
 * @param indexDef ������������
 * @return sb1 = sb2ʱ����true, ���򷵻�false
 */
bool RecordOper::isSubRecordEq(const TableDef* tableDef, const SubRecord* sb1,const SubRecord* sb2, const IndexDef *indexDef) {
	assert(sb1->m_format == sb2->m_format);

	if (sb1->m_format != sb2->m_format
		|| sb1->m_numCols != sb2->m_numCols
		|| sb1->m_rowId != sb2->m_rowId)
		return false;
	if (memcmp(sb1->m_columns, sb2->m_columns, sb1->m_numCols * sizeof(u16)))
		return false;
	if (sb1->m_format == REC_UPPMYSQL) {
		Record record1(INVALID_ROW_ID, REC_UPPMYSQL, sb1->m_data, sb1->m_size);
		MysqlRow row1(tableDef, &record1);
		RASubRecordIterator iter1(&row1, sb1);
		Record record2(INVALID_ROW_ID, REC_UPPMYSQL, sb2->m_data, sb2->m_size);
		MysqlRow row2(tableDef, &record2);
		RASubRecordIterator iter2(&row2, sb2);
		return ntse::isSubRecordEq(iter1, iter2, false);
	} else if ((sb1->m_format == REC_FIXLEN || sb1->m_format == REC_REDUNDANT
			|| sb1->m_format == REC_MYSQL)){
		Record record1(INVALID_ROW_ID, REC_REDUNDANT, sb1->m_data, sb1->m_size);
		RandAccessRow row1(tableDef, &record1);
		RASubRecordIterator iter1(&row1, sb1);
		Record record2(INVALID_ROW_ID, REC_REDUNDANT, sb2->m_data, sb2->m_size);
		RandAccessRow row2(tableDef, &record2);
		RASubRecordIterator iter2(&row2, sb2);
		return ntse::isSubRecordEq(iter1, iter2, sb1->m_format == REC_MYSQL);
	} else if (sb1->m_format == REC_VARLEN) {
		VarLenRow row1(tableDef, sb1->m_data, sb1->m_size, sb1->m_size);
		VSubRecordIterator iter1(&row1, sb1);
		VarLenRow row2(tableDef, sb2->m_data, sb2->m_size, sb2->m_size);
		VSubRecordIterator iter2(&row2, sb2);
		return ntse::isSubRecordEq(iter1, iter2);
	} else if (sb1->m_format == KEY_COMPRESS) {
		assert(indexDef != NULL);
		CompressedKeyRow key1(tableDef, indexDef, sb1);
		KeyRow::Iterator iter1(&key1, sb1);
		CompressedKeyRow key2(tableDef, indexDef, sb2);
		KeyRow::Iterator iter2(&key2, sb2);
		return ntse::isSubRecordEq(iter1, iter2);
	} else if (isKeyFormat(sb1->m_format)) {
		assert(indexDef != NULL);
		KeyRow key1(tableDef, indexDef, sb1);
		KeyRow::Iterator iter1(&key1, sb1);
		KeyRow key2(tableDef, indexDef, sb2);
		KeyRow::Iterator iter2(&key2, sb2);
		return ntse::isSubRecordEq(iter1, iter2);
	} else {
		assert(false);
		return false;
	}
}

/**
 * �Ƚ�������ͬ��ʽ��¼�Ƿ����
 *
 * @param tableDef ��������
 * @param sb1 ����Ƚ���1
 * @param sb2 ����Ƚ���2
 * @return sb1 = sb2ʱ����true, ���򷵻�false
 */
bool RecordOper::isRecordEq(const TableDef* tableDef, const Record* r1, const Record* r2) {
	if (!(r1->m_format == r2->m_format &&
		r1->m_rowId == r2->m_rowId &&
		r1->m_size == r2->m_size)) {
			return false;
	}

	if (r1->m_format == REC_COMPRESSED ) {
		return !memcmp(r1->m_data, r2->m_data, r1->m_size);
	} else if (r1->m_format == REC_COMPRESSORDER) {
		CompressOrderRow row1(tableDef, r1->m_data, r1->m_size, r1->m_size);
		CompressOrderRow row2(tableDef, r2->m_data, r2->m_size, r2->m_size);
		Column tmpCol1, tmpCol2;
		Column *c1 = &tmpCol1, *c2 = &tmpCol2;
		for (c1 = row1.firstColAcdColGrp(c1), c2 = row2.firstColAcdColGrp(c2); 
			c1 && c2 ; c1 = row1.nextColAcdColGrp(c1), c2 = row2.nextColAcdColGrp(c2)) {
			if (compareColumn(c1, c2, false, false))
				return false;
		}	
		return true;
	} else if (r1->m_format == REC_UPPMYSQL) {
		MysqlRow row1(tableDef, r1);
		MysqlRow row2(tableDef, r2);
		Column tmpCol1, tmpCol2;
		Column *c1 = &tmpCol1, *c2 = &tmpCol2;
		int result = 0;
		for (c1 = row1.firstColumn(c1), c2 = row2.firstColumn(c2)
			; !result && c1 && c2; c1 = row1.nextColumn(c1), c2 = row2.nextColumn(c2)) {
				result = compareColumn(c1, c2, false, true, c1->def()->isLongVar());
		}

		return result == 0;
	} else {
		Row row1(tableDef, r1);
		Row row2(tableDef, r2);
		Column tmpCol1, tmpCol2;
		Column *c1 = &tmpCol1, *c2 = &tmpCol2;
		int result = 0;
		for (c1 = row1.firstColumn(c1), c2 = row2.firstColumn(c2)
			; !result && c1 && c2; c1 = row1.nextColumn(c1), c2 = row2.nextColumn(c2)) {
				result = compareColumn(c1, c2, false, r1->m_format == REC_MYSQL);
		}

		return result == 0;
	}
}

/**
 * ѹ��һ��������
 * ��;: ��������ʱ����ԭ������Ȼ��ʽ�洢��������ѹ�����뵽������
 *
 * @param tableDef ��������
 * @param indexDef ������������
 * @param src ԭ����������m_formatһ��ΪKEY_NATURAL
 * @param dest ѹ���������������m_formatһ��ΪKEY_COMPRESS
 */
void RecordOper::compressKey(const TableDef *tableDef, const IndexDef* indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_NATURAL);
	assert(dest->m_format == KEY_COMPRESS);
	assert(src->m_numCols == dest->m_numCols);

	CompressedKeyRow destRow(tableDef, indexDef, dest, true);
	KeyRow srcRow(tableDef, indexDef, src);
	KeyRow::Iterator srcIter(&srcRow, src);
	size_t iterCount = 0;
	for (iterCount = 0, srcIter.first(); !srcIter.end(); srcIter.next(), iterCount++)
		destRow.appendColumn(srcIter.column(), indexDef->m_prefixLens[iterCount], NULL);
	dest->m_size = (uint)destRow.size();
}

/**
 * ��newSr�����ݺϲ���oldSr���У����ս��������newSr
 * @param tableDef ����
 * @param newSr ��Ҫ���µĸ����Ե���ֵ
 * @param oldSr ���о�����ֵ
 */
void RecordOper::mergeSubRecordRR(const TableDef *tableDef, SubRecord *newSr, const SubRecord *oldSr) {
	assert(newSr->m_format == REC_REDUNDANT);
	assert(oldSr->m_format == REC_REDUNDANT);
	assert(ColList(newSr->m_numCols, newSr->m_columns).isAsc());
	assert(ColList(oldSr->m_numCols, oldSr->m_columns).isAsc());
	assert(newSr->m_numCols);
	assert(oldSr->m_numCols);

	RedRow newRow(tableDef, newSr);
	RedRow oldRow(tableDef, oldSr);
	Column column;
	u16 j = 0;
	for (u16 i = 0; i < oldSr->m_numCols; ++i) {
		// �ҵ�newSr�п���ƥ�����
		while (j < newSr->m_numCols && newSr->m_columns[j] < oldSr->m_columns[i])
			++j;
		if (j >= newSr->m_numCols || oldSr->m_columns[i] != newSr->m_columns[j])
			// new��û�и��У����old��copy
			newRow.writeColumn(oldSr->m_columns[i], oldRow.columnAt(oldSr->m_columns[i], &column));
	}
}

/**
* �������ʽ�ļ�¼ת��Ϊѹ�������ʽ�ļ�¼
*
* ֻ�Ǹ������������˳�򼰴���ΪNULL�����ԣ�����ѹ��ǰԤ����
*
* @param tableDef ��¼�����ı���
* @param src Ҫת���ļ�¼��һ��ΪREC_REDUNDANT��ʽ
* @param dest OUT ����ת�����REC_COMPRESSORDER��ʽ�ļ�¼�����÷���֤���㹻�ڴ汣��������
*                 ���dest->m_segSizesΪNULL���򲻻���������������ʼ���ݵ�ƫ����
*/
void RecordOper::convRecordRedToCO(const TableDef *tableDef, const Record *src, CompressOrderRecord *dest) {
	assert(src->m_format == REC_REDUNDANT);
	assert(dest->m_format == REC_COMPRESSORDER);
	assert(dest->m_segSizes != NULL);

	if (dest->m_segSizes != NULL)
		memset(dest->m_segSizes, 0, dest->m_numSeg * sizeof(size_t));

	RedRow mRow(tableDef, src);
	CompressOrderRow rcRow(tableDef, dest->m_data, dest->m_size, 0);
	Column column;
	for (Column* col = mRow.firstColumnAcdGrp(&column); col ; col = mRow.nextColumnAcdGrp(col)) {
		rcRow.appendColumn(col);
		u8 colGrpNo = col->def()->m_colGrpNo;
		if (dest->m_segSizes != NULL)
			dest->m_segSizes[colGrpNo] += col->size();
	}
	dest->m_size = (uint)rcRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
* ��ѹ�������ʽ�ļ�¼ת��Ϊ�����ʽ�ļ�¼
* ���ڽ�ѹ�������������Ķ��廹ԭΪ�����ʽ�ļ�¼
* @param tableDef �����ı���
* @param src Ҫת����ѹ�������ʽ�ļ�¼��ΪREC_COMPRESSORDER��ʽ
* @param dest OUT������ת����������ʽ�ļ�¼��һ��ΪREC_REDUNDANT��ʽ�����÷���֤���㹻���ڴ�
*/
void RecordOper::convRecordCOToRed(const TableDef *tableDef, const CompressOrderRecord *src, Record *dest) {
	assert(src->m_format == REC_COMPRESSORDER);
	assert(dest->m_format == REC_REDUNDANT);

	CompressOrderRow rcRow(tableDef, src->m_data, src->m_size, src->m_size);
	RedRow mRow(tableDef, dest);

	Column column;
	for (Column* col = rcRow.firstColAcdColGrp(&column); col ; col = rcRow.nextColAcdColGrp(col)) {
		mRow.writeColumn(col->colNo(), col);
	}
	dest->m_size = (uint)mRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
 * ��ѹ�������ʽ�ļ�¼ת��Ϊ�䳤��ʽ�ļ�¼ 
 * @param ctx �ڴ����������
 * @param table �����ı���
 * @param src Ҫת����ѹ�������ʽ�ļ�¼��ΪREC_COMPRESSORDER��ʽ
 * @param dest INOUT ����ת����ı䳤��ʽ�ļ�¼��һ��ΪREC_VAR��ʽ�����÷���֤���㹻���ڴ沢ͨ��dest->m_size��֮
 */
void RecordOper::convRecordCOToVar(MemoryContext *ctx, const TableDef *tableDef, const CompressOrderRecord *src, Record *dest) {
	assert(src->m_format == REC_COMPRESSORDER);
	assert(dest->m_format == REC_VARLEN);
	assert(ctx);

	McSavepoint savePoint(ctx);
	CompressOrderRow rcRow(tableDef, src->m_data, src->m_size, src->m_size);

	Column *idIncCol = new (ctx->alloc(tableDef->m_numCols * sizeof(Column)))Column[tableDef->m_numCols];
	Column column;
	for (Column* col = rcRow.firstColAcdColGrp(&column); col ; col = rcRow.nextColAcdColGrp(col)) {
		idIncCol[col->colNo()] = *col;//ǳ����
	}

	VarLenRow vRow(tableDef, dest->m_data, dest->m_size, 0);
	for (u16 i = 0; i < tableDef->m_numCols; i++) {
		vRow.appendColumn(&idIncCol[i]);
	}
	dest->m_size = (uint)vRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
 * ���䳤��ʽ�ļ�¼ת��Ϊѹ�������ʽ�ļ�¼
 * @param ctx �ڴ����������
 * @param tableDef ����
 * @param src Ҫת���ı䳤��ʽ�ļ�¼��ΪREC_VARLEN��ʽ
 * @param dest INOUT ���ת�����ѹ�������ʽ��¼��ΪREC_COMPRESSORDER��ʽ�����÷���֤���㹻�ڴ棬��ͨ��dest->m_size��֮
 *                 ���dest->m_segSizesΪNULL���򲻻���������������ʼ���ݵ�ƫ����
 */
void RecordOper::convRecordVarToCO(MemoryContext *ctx, const TableDef *tableDef, const Record *src, CompressOrderRecord *dest) {
	assert(src->m_format == REC_VARLEN);
	assert(dest->m_format == REC_COMPRESSORDER);
	assert(ctx);

	McSavepoint savePoint(ctx);
	VarLenRow vRow(tableDef, src->m_data, src->m_size, src->m_size);
	Column *idIncCol = new (ctx->alloc(tableDef->m_numCols * sizeof(Column)))Column[tableDef->m_numCols];
	Column column;
	for (Column* col = vRow.firstColumn(&column); col ; col = vRow.nextColumn(col)) {
		idIncCol[col->colNo()] = *col;//ǳ����
	}

	CompressOrderRow rcRow(tableDef, dest->m_data, dest->m_size, 0);
	for (u8 colGrp = 0; colGrp < tableDef->m_numColGrps; colGrp++) {
		uint segSize = 0;
		ColGroupDef *colGrpDef = tableDef->m_colGrps[colGrp];
		for (u16 i = 0; i < colGrpDef->m_numCols; i++) {
			u16 no = colGrpDef->m_colNos[i];
			rcRow.appendColumn(&idIncCol[no]);
			segSize += idIncCol[no].size();
		}
		if (dest->m_segSizes)
			dest->m_segSizes[colGrp] = segSize;
	}
	dest->m_size = (uint)rcRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
* ��ѹ�������ʽ�ļ�¼ת��Ϊѹ����ʽ�ļ�¼
* @param cprsRcdExtrator ѹ����ѹ����ȡ�� 
* @param src Ҫת����ѹ�������ʽ��¼, ΪREC_COMPRESSORDER��ʽ
* @param dest INOUT�����ת�����ѹ����ʽ��¼��ΪREC_COMPRESSED��ʽ�����÷���֤�в������ڴ沢ͨ��dest->m_size��֮
* @return ���ؼ�¼��ѹ����
*/
double RecordOper::convRecordCOToComprssed(CmprssRecordExtractor *cprsRcdExtrator, const CompressOrderRecord *src, Record *dest) {
	assert(src->m_format == REC_COMPRESSORDER);
	assert(dest->m_format == REC_COMPRESSED);
	assert(cprsRcdExtrator != NULL);
	assert(src->m_segSizes != NULL);
	return cprsRcdExtrator->compressRecord(src, dest);
}

/**
* ��ѹ����ʽ�ļ�¼ת��Ϊѹ�������ʽ�ļ�¼
* @param cprsRcdExtrator ѹ����ѹ����ȡ��
* @param src Ҫת����ѹ����ʽ��¼, ΪREC_COMPRESSED��ʽ
* @param dest INTOUT, ���ת�����ѹ�������ʽ��¼��ΪREC_COMPRESSORDER��ʽ�����÷���֤�в������ڴ沢ͨ��dest->m_size��֮
*/
void RecordOper::convRecordCompressedToCO(CmprssRecordExtractor *cprsRcdExtrator, const Record *src, CompressOrderRecord *dest) {
	assert(src->m_format == REC_COMPRESSED);
	assert(dest->m_format == REC_COMPRESSORDER);
	assert(cprsRcdExtrator != NULL);
	cprsRcdExtrator->decompressRecord(src, dest);
}

/**
 * ��ѹ���ļ�¼ת��Ϊ�䳤��ʽ�ļ�¼
 * @param ctx �ڴ����������
 * @param tableDef ��������
 * @param cprsRcdExtrator ѹ����ѹ����ȡ��
 * @param src INOUT ����Ϊѹ����ʽ�ļ�¼�����destΪNULL�����Ϊ�䳤��ʽ�ļ�¼
 * @param dest OUT �����������ΪNULL��ת����ļ�¼�洢��src��, ֱ�Ӹ���ԭ��src������(Ϊ�˱����ڴ���䣬���÷�Ӧ��ȷ��ԭ��src�����ݲ�������)
 */
void RecordOper::convRecordCompressedToVar(MemoryContext *ctx, const TableDef *tableDef, 
	CmprssRecordExtractor *cprsRcdExtrator, Record *src, Record *dest /*= NULL*/) {
		assert(src->m_format == REC_COMPRESSED);
		assert(ctx);

		McSavepoint msp(ctx);
		CompressOrderRecord dummyCprsRcd;
		dummyCprsRcd.m_size = tableDef->m_maxRecSize;
		dummyCprsRcd.m_data = (byte *)ctx->alloc(dummyCprsRcd.m_size);

		RecordOper::convRecordCompressedToCO(cprsRcdExtrator, src, &dummyCprsRcd);
		if (dest == NULL) {
			src->m_format = REC_VARLEN;
			src->m_size = tableDef->m_maxRecSize;
			RecordOper::convRecordCOToVar(ctx, tableDef, &dummyCprsRcd, src);
		} else {
			assert(dest->m_format == REC_VARLEN);
			RecordOper::convRecordCOToVar(ctx, tableDef, &dummyCprsRcd, dest);
		}
}

/**
 * ��ѹ����ʽ��¼����ȡ�����ʽ�Ӽ�¼
 * @param mtx �ڴ���������ģ����ΪNULL�����ڲ�ʹ��new�����ڴ�
 * @param cprsRcdExtrator ѹ����ѹ����ȡ��
 * @param tableDef ��������
 * @param record ѹ����ʽ�ļ�¼
 * @param subRecord OUT ����Ӽ�¼���ݣ�����Ϊ�����ʽ�����÷���֤�������㹻���ڴ�
 * @param cmpressExtractInfo ѹ����������ȡ��Ϣ
 */
void RecordOper::extractSubRecordCompressedR(MemoryContext *mtx, CmprssRecordExtractor *cprsRcdExtrator, 
											 const TableDef *tableDef, const Record *record, SubRecord *subRecord, 
											 CmprssColGrpExtractInfo *cmpressExtractInfo /* = NULL */) {
	assert(mtx);
	assert(cprsRcdExtrator);
	assert(record->m_format == REC_COMPRESSED);
	assert(subRecord->m_format == REC_REDUNDANT);

	McSavepoint msp(mtx);
	if (!cmpressExtractInfo) {
		void *d = mtx->alloc(sizeof(CmprssColGrpExtractInfo));
		cmpressExtractInfo = new (d)CmprssColGrpExtractInfo(tableDef, subRecord->m_numCols, 
			subRecord->m_columns, mtx);
	}

	RedRow mRow(tableDef, subRecord, true);
	CompressedRow rcRow(tableDef, record);
	CompressedColGroup cmprsColGrp;
	for (CompressedColGroup *colGrp = rcRow.firstColGrp(&cmprsColGrp); colGrp; colGrp = rcRow.nextColGrp(colGrp)) {
		if (cmpressExtractInfo->m_colNumNeedReadInGrp[colGrp->colGrpNo()] > 0) {//������ѹ����������Ҫ��ѹ
			uint decompressSize = 0;//�������ѹ��֮��ĳ���
			cprsRcdExtrator->decompressColGroup(colGrp->data(), (uint)colGrp->lenBytes(), colGrp->getRealSize(), 
				cmpressExtractInfo->m_decompressBuf, &decompressSize);
			assert(decompressSize <= tableDef->m_maxRecSize);

			ColGroupRow curColGrpRow(tableDef, colGrp->colGrpNo(), cmpressExtractInfo->m_decompressBuf, 
				decompressSize, record->m_data);
			Column column;
			u16 colNumNeedRead = cmpressExtractInfo->m_colNumNeedReadInGrp[colGrp->colGrpNo()];
			for (Column *col = curColGrpRow.firstColumn(&column); col; col = curColGrpRow.nextColumn(col)) {
				if (cmpressExtractInfo->m_colNeedRead[col->colNo()] > 0) {//��һ����Ҫ��ȡ
					mRow.writeColumn(col->colNo(), col);
					colNumNeedRead--;
					if (colNumNeedRead == 0)//�������������Ҫ��ȡ���ֶ��Ѿ�ȫ������
						break;
				}
			}
		} 
	}
	subRecord->m_rowId = record->m_rowId;
	subRecord->m_size = mRow.size();

	cmpressExtractInfo = NULL;
}

/**
* �Ǳ��ظ��°���ȫ���ֵ�ı�ļ�¼
* ע��Ҫ�������ļ�¼������REC_VARLEN��ʽ��Ҳ������REC_COMPRESSED��ʽ��
* ��Ϊ����ȫ���ֵ�ı������ѹ����Ҳ���ܲ���ѹ����,
* �����ѹ��������º�ļ�¼�Ƿ����ѹ����ȡ���ڸ��º���ѹ����ѹ���ȣ�����ѹ����
* @pre ���÷���֤���ݽ�����ѹ����¼��ȡ���ǿգ��������ֵ�һ������
* @post: newRcd��m_size, m_data, m_format, m_isRealCompressed���ܱ��޸�
* @param ctx             �ڴ����������
* @param tableDef        �����ı���
* @param cprsRcdExtrator ѹ����¼��ȡ��
* @param oldRcd          Ҫ���µļ�¼�����ʽΪREC_COMPRESSED��REC_VARLEN,
* @param update          Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
* @param newRcd OUT      ������º�ļ�¼�����÷���֤���㹻�Ŀռ䣬��ͨ��m_size��֮
* @param lobUseOld       ������ֶ��Ƿ�ʹ��ǰ���ֵ
*/
void RecordOper::updateRcdWithDic(MemoryContext *ctx, const TableDef *tableDef, 
	CmprssRecordExtractor *cprsRcdExtrator, const Record *oldRcd, 
	const SubRecord *update, Record *newRcd) {
		assert(update->m_format == REC_REDUNDANT);
		assert((uint)tableDef->m_maxRecSize == update->m_size);
		assert(cprsRcdExtrator);
		assert(oldRcd->m_format == REC_COMPRESSED || oldRcd->m_format == REC_VARLEN);

		if (oldRcd->m_format == REC_COMPRESSED) {//ԭ��¼��ѹ����
			updateCompressedRcd(ctx, tableDef, cprsRcdExtrator, oldRcd, update, newRcd);
		} else {//ԭ��¼����ѹ����
			updateUncompressedRcd(ctx, tableDef, cprsRcdExtrator, oldRcd, update, newRcd);
		}
}

/**
* ����ѹ����ʽ�ļ�¼
*
* @post: newRcd��m_id, m_size, m_data, m_format, m_isRealCompressed���ܱ��޸�
* @param ctx             �ڴ����������
* @param tableDef        ��¼�����ı���
* @param cprsRcdExtrator ѹ����¼��ȡ��
* @param oldRcd          Ҫ���µļ�¼�����ʽһ��ΪREC_COMPRESSED
* @param update          Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
* @param newRcd          OUT ������º�ļ�¼��������ѹ���ĸ�ʽ��Ҳ�����Ǳ䳤��ʽ��ȡ����shouldCompress�����Լ�ѹ���ȣ�
*                        ���÷���֤���㹻�Ŀռ䣬��ͨ��m_size��֮
* @param lobUseOld       ������ֶ��Ƿ�ʹ��ǰ���ֵ
*/
void RecordOper::updateCompressedRcd(MemoryContext *mtx, const TableDef *tableDef, 
									 CmprssRecordExtractor *cprsRcdExtrator, const Record *oldRcd, 
									 const SubRecord *update, Record *newRcd) {
	 assert(mtx);
	 assert(oldRcd->m_format == REC_COMPRESSED);

	 McSavepoint msp(mtx);
	 CmprssColGrpExtractInfo cmpressExtractInfo(tableDef, update->m_numCols, update->m_columns, mtx);
	 byte *tmpDecompressBuf = (byte *)mtx->alloc(tableDef->m_maxRecSize);//���ڻ����ѹ���������
	 byte *tmpUpdateBuf = (byte *)mtx->alloc(Limits::PAGE_SIZE);//���ڻ�����º��ѹ��������

	 RedRow mRow(tableDef, update);
	 CompressedRow oldRow(tableDef, oldRcd);

	 // newRow.m_size���ȱ���ʼ��ΪtableDef->m_bmBytes
	 CompressedRow newRow(tableDef, newRcd->m_data, newRcd->m_size, true);
	 CompressedColGroup cmprsColGrp;
	 uint uncprsSize = 0;

	 // Fix Bug #113623
	 // ������ҪCopy Null Bitmap��Ȼ����ܴ���Column Groups
	 newRow.setNullBitmap(oldRcd->m_data, tableDef->m_bmBytes);

	 for (CompressedColGroup *colGrp = oldRow.firstColGrp(&cmprsColGrp); colGrp; colGrp = oldRow.nextColGrp(colGrp)) {
		 u8 colGrpNo = colGrp->colGrpNo();
		 if (cmpressExtractInfo.m_colNumNeedReadInGrp[colGrpNo] > 0) {//������ѹ����������Ҫ����,�Ƚ�ѹ,���º���ѹ��
			 uint decompressSize = 0;//�������ѹ��֮��ĳ���
			 cprsRcdExtrator->decompressColGroup(colGrp->data(), (uint)colGrp->lenBytes(), colGrp->getRealSize(), 
				 tmpDecompressBuf, &decompressSize);
			 assert(decompressSize <= tableDef->m_maxRecSize);

			 //��������
			 ColGroupRow cgRow(tableDef, colGrpNo, tmpDecompressBuf, decompressSize, oldRcd->m_data);
			 ColGroupRow newColGrpRow(tableDef, colGrpNo, tmpUpdateBuf, Limits::DEF_MAX_REC_SIZE,
				 newRcd->m_data, true);
			 Column column, column1;
			 for (Column *col = cgRow.firstColumn(&column); col; col = cgRow.nextColumn(col)) {
				 if (cmpressExtractInfo.m_colNeedRead[col->colNo()] > 0) {
					 Column *updateCol = mRow.columnAt(col->colNo(), &column1);//�������
					 newColGrpRow.appendColumn(updateCol);
				 } else {
					 newColGrpRow.appendColumn(col);
				 }
			 }
			 newRow.compressAndAppendColGrp(cprsRcdExtrator, tmpUpdateBuf, (uint)0, newColGrpRow.size());
			 uncprsSize += (uint)newColGrpRow.size();
		 } else {
			 newRow.appendColGrp(colGrp);
			 uncprsSize += (uint)cprsRcdExtrator->calcColGrpDecompressSize(colGrp->getRealData(), 0, colGrp->getRealSize());
		 }
	 }

	 newRcd->m_rowId = oldRcd->m_rowId;
	 newRcd->m_size = newRow.size();
	 newRcd->m_format = REC_COMPRESSED;
	 if (tableDef->m_isCompressedTbl) {
		 //������º�ļ�¼��ѹ���ȣ�����ﲻ��ѹ���ȷ�ֵ�����ѹ
		 double cprsRatio = newRcd->m_size * 100.0 / uncprsSize;
		 if (cprsRatio <= tableDef->m_rowCompressCfg->compressThreshold()) {		
			 return;
		 }
	 }
	 RecordOper::convRecordCompressedToVar(mtx, tableDef, cprsRcdExtrator, newRcd);
}

/**
 * ���±䳤��ʽ�ļ�¼������ѹ��
 * ���º�ļ�¼�Ƿ����ѹ����ȡ����shouldCompress�����Լ����º���ѹ����ѹ����
 *
 * @post: newRcd��m_id, m_size, m_data, m_format, m_isRealCompressed���ܱ��޸�
 * @param ctx �ڴ����������
 * @param tableDef ��¼�����ı���
 * @param cprsRcdExtrator ѹ����¼��ȡ��
 * @param oldRcd Ҫ���µļ�¼�����ʽΪREC_VARLEN,
 * @param update Ҫ���µ����Լ���Щ���Ե���ֵ����m_formatһ��ΪREC_REDUNDANT
 * @param newRcd OUT ������º�ļ�¼�����÷���֤���㹻�Ŀռ�
 */

void RecordOper::updateUncompressedRcd(MemoryContext *ctx, const TableDef *tableDef, 
									   CmprssRecordExtractor *cprsRcdExtrator, const Record *oldRcd, 
									   const SubRecord *update, Record *newRcd) {
	assert(cprsRcdExtrator);
	assert(REC_VARLEN == oldRcd->m_format);

	McSavepoint savePoint(ctx);
	RedRow mRow(tableDef, update); // ������Ϣ
	VarLenRow vRow(tableDef, oldRcd->m_data, oldRcd->m_size, oldRcd->m_size);//ԭ��

	if (tableDef->m_isCompressedTbl) {
		Column *mergeCols = new (ctx->alloc(tableDef->m_numCols * sizeof(Column)))Column[tableDef->m_numCols];
		Column column, updCol;
		int deltaSize = 0;
		uint idx = 0;
		for (Column* col = vRow.firstColumn(&column); col ; col = vRow.nextColumn(col)) {
			u16 no = col->colNo();
			if (idx < update->m_numCols && no == update->m_columns[idx]){ // �ҵ�һ��ƥ����
				mergeCols[no] = *(mRow.columnAt(no, &updCol));
				deltaSize += mergeCols[no].size() - col->size();
				++idx;
			} else {
				mergeCols[no] = *col;//ǳ����
			}
		}
		
		//�������֮��ĺ����С
		u16 updSize = (u16)((int)vRow.size() + deltaSize);

		CompressOrderRecord dummyCprsRcd(oldRcd->m_rowId, (byte *)ctx->alloc(updSize), updSize, 
			tableDef->m_numColGrps, (size_t *)ctx->calloc(sizeof(size_t) * tableDef->m_numColGrps));
		CompressOrderRow rcRow(tableDef, dummyCprsRcd.m_data, dummyCprsRcd.m_size, 0);
		for (u8 i = 0; i < tableDef->m_numColGrps; i++) {
			ColGroupDef *colGrpDef = tableDef->m_colGrps[i];
			size_t segSize = 0;
			for (u16 j = 0; j < colGrpDef->m_numCols; j++) {
				u16 no = colGrpDef->m_colNos[j];
				rcRow.appendColumn(&mergeCols[no]);
				segSize += mergeCols[no].size();
			}
			dummyCprsRcd.m_segSizes[i] = segSize;
		}

		//��������ѹ�������ܱ�ѹ��Ҳ���ܲ���ѹ����ȡ���ڳ���ѹ�����ѹ����
		uint newRcdBufSize = newRcd->m_size;
		newRcd->m_format = REC_COMPRESSED;
		double compressRatio = 100.0 * cprsRcdExtrator->compressRecord(&dummyCprsRcd, newRcd);				
		if (compressRatio > tableDef->m_rowCompressCfg->compressThreshold()) {
			newRcd->m_size = newRcdBufSize;
			newRcd->m_format = REC_VARLEN;
			RecordOper::convRecordCOToVar(ctx, tableDef, &dummyCprsRcd, newRcd);
		}
	} else {
		VarLenRow newRow(tableDef, newRcd->m_data, newRcd->m_size, 0);
		Column column, column1;
		uint idx = 0;
		for (Column* col = vRow.firstColumn(&column);	col; col = vRow.nextColumn(col)) {
			if (idx < update->m_numCols && col->colNo() == update->m_columns[idx]){ // �ҵ�һ��ƥ����
				newRow.appendColumn(mRow.columnAt(col->colNo(), &column1));
				++idx;
			} else {
				newRow.appendColumn(col);
			}
		}
		newRcd->m_format = REC_VARLEN;
		newRcd->m_size = newRow.size();
	}
}

/**
 * ��ȡѹ���γ���
 * @param src         ѹ������ʼλ��
 * @param segSize OUT ѹ���γ���
 * @return            Ϊ��ʾ�ó������õ��ֽ���
 */
u8 RecordOper::readCompressedColGrpSize(byte *src, uint *segSize) {
	assert(src != NULL);
	*segSize = src[0];
	if (*segSize < ONE_BYTE_SEG_MAX_SIZE) {
		return 1;
	} else {
		*segSize = (src[0] & 0x7F) << 8;
		*segSize |= src[1];
		return 2; 
	}
}

/**
 * д��һ��ѹ���εĳ���
 * @pre ���ݶγ��Ȳ��ܳ�TWO_BYTE_SEG_MAX_SIZE����󳤶�
 * @param dest  д���Ŀ�ʼλ��
 * @param size  ѹ���γ���
 * @return      Ϊ��ʾѹ�������õ��ֽ���
 */
u8 RecordOper::writeCompressedColGrpSize(byte *dest, const uint& size) {
	assert(size < TWO_BYTE_SEG_MAX_SIZE);
	if (size < ONE_BYTE_SEG_MAX_SIZE) {
		*dest = (byte)size;
		return 1;
	} else {
		assert(size < TWO_BYTE_SEG_MAX_SIZE);
		dest[0] = (byte )((size >> 8) | 0x80);
		dest[1] = (byte )size;
		return 2;
	}
}

 /* ����С�ʹ�����¼
  *	С�ʹ�������(INT, VARCHAR)
  *	INT: ѹ��֮ǰ����
  *	VARHCAR: ���������
  * @param tableDef С�ʹ�������
  * @param buf ��¼�ڴ�
  * @param bufSize ��¼�ڴ��С�������߱��뱣֤buf���㹻�Ŀռ�
  * @param data С�ʹ��������, data==0��ʾ�����ΪNULL
  * @param size С�ʹ���󳤶�
  * @param orgSize ѹ��֮ǰ�Ĵ���󳤶�
  * @return ����ֵ��ͬ�ڼ�¼����
  */
size_t createSlobRow(const TableDef *tableDef, byte *buf, size_t bufSize, const byte *data, size_t size, size_t orgSize) {
	assert(!((size && !data) || (orgSize && !data)));
	// ��ʱû��ʵ��
	// assert(orgSize >= size);
	assert(tableDef->m_numCols == 2);
	assert(tableDef->m_nullableCols == 2);
	size_t bmBytes = calcBitmapBytes(REC_VARLEN, tableDef->m_nullableCols);
	assert(bufSize >= size + bmBytes);
	UNREFERENCED_PARAMETER(bufSize);
	// ��ʼ��λͼ
	memset(buf, 0, bmBytes);


	if (!data) {
		BitmapOper::setBit(buf, bmBytes << 3, 0);
		BitmapOper::setBit(buf, bmBytes << 3, 1);
		return bmBytes;
	}

	BitmapOper::clearBit(buf, bmBytes << 3, 0);
	BitmapOper::clearBit(buf, bmBytes << 3, 1);
	byte *ptr = buf + bmBytes;
	// ��������
	assert(tableDef->m_columns[0]->m_type == CT_INT);
	*(u32 *)ptr = (u32)orgSize;
	ptr += tableDef->m_columns[0]->m_size;

	// ����VARCHAR
	size_t lenBytes = tableDef->m_columns[1]->m_lenBytes;
	// д���ַ�������
	writeU32Le((u32)size, ptr, lenBytes);
	// д���ַ�������
	ptr += lenBytes;
	memcpy(ptr, data, size);
	return (size_t)(ptr + size - buf);
}
/**
 * ����С�ʹ�����¼
 * @param tableDef С�ʹ�������
 * @param rec ��¼ָ�룬�����߱��뱣֤rec->m_data���㹻�Ŀռ�
 * @param data С�ʹ��������, data==0��ʾ�����ΪNULL
 * @param size С�ʹ���󳤶�
 * @param orgSize ѹ��֮ǰ�Ĵ���󳤶�
 * @return ����ֵ��ͬ�ڲ���rec
 */
Record* RecordOper::createSlobRecord(const TableDef *tableDef, Record *rec, const byte *data, size_t size, size_t orgSize) {
	assert(rec);
	rec->m_size = (uint)createSlobRow(tableDef, rec->m_data, rec->m_size, data, size, orgSize);
	rec->m_rowId = INVALID_ROW_ID;
	rec->m_format = REC_VARLEN;
	return rec;
}
/**
 * ����һ�������ʽ��С�ʹ�����Ӽ�¼
 * @param tableDef С�ʹ�������
 * @param sr �Ӽ�¼�������߸����ڴ����
 * @param data ���������
 * @param size ��������ݳ���
 * @param orgSize ѹ��֮ǰ�Ĵ���󳤶�
 * @return ��¼����
 */
SubRecord* RecordOper::createSlobSubRecordR(const TableDef *tableDef, SubRecord *sr
													, const byte *data, size_t size, size_t orgSize) {
	assert(sr->m_size >= tableDef->m_maxRecSize);
	size_t realSize = createSlobRow(tableDef, sr->m_data, sr->m_size, data, size, orgSize);
	sr->m_format = REC_REDUNDANT;
	sr->m_rowId = INVALID_ROW_ID;
	sr->m_size = tableDef->m_maxRecSize;
	// ��������ʽδʹ�õ��ڴ棬���ڼ�¼�Ƚ�
	UNREFERENCED_PARAMETER(realSize);

	return sr;
}


/**
 * ��ȡС�ʹ�����¼�а����Ĵ���������
 * @param tableDef С�ʹ�������
 * @param rec С�ʹ�����¼
 * @param size[out] ���ݳ���
 * @param orgSize[out] ѹ��֮ǰ�Ĵ���󳤶�
 * @return ����ָ��
 */
byte* RecordOper::extractSlobData(const TableDef *tableDef, const Record *rec, size_t *size, size_t *orgSize) {
	assert(rec);
	assert(rec->m_format == REC_VARLEN);
	size_t bmBytes = calcBitmapBytes(REC_VARLEN, tableDef->m_nullableCols);
	size_t lenBytes = tableDef->m_columns[1]->m_lenBytes;
	if (BitmapOper::isSet(rec->m_data, bmBytes << 3, 0)) {
		*size = 0;
		*orgSize = 0;
		return NULL;
	} else {
		assert(bmBytes + lenBytes + tableDef->m_columns[0]->m_size <= rec->m_size);
		byte *ptr = rec->m_data + bmBytes;
		*orgSize = (size_t)(*(u32 *)ptr);
		ptr += tableDef->m_columns[0]->m_size;
		*size = (size_t)readU32Le(ptr, lenBytes);
		assert(rec->m_size == bmBytes + tableDef->m_columns[0]->m_size + lenBytes + *size);
		return ptr + lenBytes;
	}
}

/**
 * �ж�һ�������Ƿ�ΪNULL
 *
 * @param tableDef ����
 * @param record ��¼��ΪREC_REDUNDANT��ʽ
 * @param cno ���Ժţ���0��ʼ���
 * @return �����Ƿ�ΪNULL
 */
bool RecordOper::isNullR(const TableDef *tableDef, const Record *record, u16 cno) {
	assert(record->m_format == REC_REDUNDANT || record->m_format == REC_MYSQL || record->m_format == REC_UPPMYSQL);
	assert(cno < tableDef->m_numCols);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	if (columnDef->m_nullable) {
		assert(8 * calcBitmapBytes(record->m_format, tableDef->m_nullableCols) > columnDef->m_nullBitmapOffset);
		return BitmapOper::isSet(record->m_data, (size_t)Limits::MAX_COL_NUM, columnDef->m_nullBitmapOffset);
	} else {
		return false;
	}
}

/**
 * �ж�һ�������Ƿ�ΪNULL
 *
 * @param tableDef ����
 * @param subRec ���ּ�¼��ΪREC_REDUNDANT��ʽ
 * @param cno ���Ժţ���0��ʼ���
 * @return �����Ƿ�ΪNULL
 */
bool RecordOper::isNullR(const TableDef *tableDef, const SubRecord *subRec, u16 cno) {
	Record rec(INVALID_ROW_ID, subRec->m_format, subRec->m_data, subRec->m_size);
	return isNullR(tableDef, &rec, cno);
}

/**
 * ����һ�������Ƿ�ΪNULL
 *
 * @param tableDef ����
 * @param record ��¼��ΪREC_REDUNDANT��ʽ
 * @param cno ���Ժţ���0��ʼ���
 * @param null ��ΪNULL���Ƿ�NULL
 */
void RecordOper::setNullR(const TableDef *tableDef, const Record *record, u16 cno, bool null) {
	assert(record->m_format == REC_REDUNDANT || record->m_format == REC_MYSQL);
	assert(cno < tableDef->m_numCols);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(!null || columnDef->m_nullable);
	assert(8 * calcBitmapBytes(record->m_format, tableDef->m_nullableCols) > columnDef->m_nullBitmapOffset);
	if (null)
		BitmapOper::setBit(record->m_data, (size_t)Limits::MAX_COL_NUM, columnDef->m_nullBitmapOffset);
	else
		BitmapOper::clearBit(record->m_data, (size_t)Limits::MAX_COL_NUM, columnDef->m_nullBitmapOffset);
}

/**
 * ����һ�������Ƿ�ΪNULL
 *
 * @param tableDef ����
 * @param subRec ���ּ�¼��ΪREC_REDUNDANT��ʽ
 * @param cno ���Ժţ���0��ʼ���
 * @param null ��ΪNULL���Ƿ�NULL
 */
void RecordOper::setNullR(const TableDef *tableDef, const SubRecord *subRec, u16 cno, bool null) {
	Record rec(INVALID_ROW_ID, subRec->m_format, subRec->m_data, subRec->m_size);
	setNullR(tableDef, &rec, cno, null);
}

/**
 * ��ȡѹ����ʽkey����Ȼ��ʽ����
 * @param tableDef ����
 * @param indexDef ��������
 * @param compressedKey ѹ����ʽ��key
 * @return ��Ӧ��Ȼ��ʽkey�ĳ���
 */
u16 RecordOper::getKeySizeCN(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *compressedKey) {
	assert(compressedKey->m_format == KEY_COMPRESS);

	CompressedKeyRow srcRow(tableDef, indexDef, compressedKey);
	KeyRow::Iterator ki(&srcRow, compressedKey);
	byte buf[Limits::PAGE_SIZE];
	SubRecord natKey(KEY_NATURAL, compressedKey->m_numCols, compressedKey->m_columns, buf, sizeof(buf));
	KeyRow dstRow(tableDef, indexDef, &natKey, true);
	byte tmp[8];
	Column raw;
	size_t iterCount = 0;
	for (iterCount = 0, ki.first(); !ki.end(); ki.next(), iterCount++) {
		raw.setBuf(tmp, sizeof(tmp));
		dstRow.appendColumn(CompressedKeyRow::decompressColumn(ki.column(), &raw), indexDef->m_prefixLens[iterCount], NULL);
	}
	return (u16)dstRow.size();
}

/**
 * ��ȡ��¼ת��Ϊ�䳤��ʽ��ĳ���
 *
 * @param tableDef ����
 * @param record REC_REDUNDANT��REC_MYSQL��ʽ��ʾ�ļ�¼����
 * @return ��¼ת��Ϊ�䳤��ʽ��ĳ���
 */
u16 RecordOper::getRecordSizeRV(const TableDef *tableDef, const Record *record) {
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert(record->m_format == REC_REDUNDANT || record->m_format == REC_MYSQL);

	Column column;
	RandAccessRow row(tableDef, record);
	size_t size = row.bitmapBytes();
	for (Column *col = row.firstColumn(&column); col;  col = row.nextColumn(col))
		size += col->size();
	return (u16)size;
}

/**
 * ��ȡ�������Mysql��ʽ�Ӽ�¼ת��Ϊ�䳤��ʽ��ĳ���
 *
 * @param tableDef ����
 * @param subRec REC_REDUNDANT��REC_MYSQL��ʽ��ʾ�Ӽ�¼����
 * @return �Ӽ�¼ת��Ϊ�䳤��ʽ��ĳ���
 */
u16 RecordOper::getSubRecordSizeRV(const TableDef *tableDef, const SubRecord *subRec) {
	assert(subRec->m_format == REC_REDUNDANT || subRec->m_format == REC_MYSQL);

	Column column;
	RandAccessRow row(tableDef, subRec);
	size_t size = row.bitmapBytes();
	for (u16 i = 0; i < subRec->m_numCols; ++i) {
		size += row.columnAt(subRec->m_columns[i], &column)->size();
	}
	return (u16)size;
}

/**
 * ��ʼ���ռ�¼
 *
 * @param record [in/out] ����ʼ����¼
 * @param tableDef ����
 * @param rowId ��¼RowId
 * @return ��ʼ����ļ�¼
 */
Record* RecordOper::initEmptyRecord(Record *record, const TableDef *tableDef, RowId rowId, RecFormat recFormat) {
	record->m_rowId = rowId;
	record->m_format = recFormat;
	assert(tableDef->m_bmBytes <= record->m_size);
	// NULL������ΪNULL
	memset(record->m_data, -1, tableDef->m_bmBytes);
	if (tableDef->m_recFormat == REC_FIXLEN) { // ����
		assert(record->m_size >= tableDef->m_maxRecSize); 
		record->m_size = tableDef->m_maxRecSize;
		memset(record->m_data + tableDef->m_bmBytes, 0, tableDef->m_maxRecSize - tableDef->m_bmBytes);
	} else { // �䳤
		assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
		byte *ptr = record->m_data + tableDef->m_bmBytes;
		for(u16 cno = 0; cno < tableDef->m_numCols; ++cno) {
			ColumnDef *columnDef = tableDef->m_columns[cno];
			if (!columnDef->m_nullable) { // ��NULL�У�����Ϊ0
				assert(ptr + columnDef->m_size <= record->m_data + record->m_size);
				memset(ptr, 0, columnDef->m_size);
				ptr += ntse::getColSize(columnDef, ptr);
			}
		}
		record->m_size = ptr - record->m_data;
	}
	return record;
}

/** ��ȡ�����ID
 * @param record REC_REDUNDANT��ʽ����������
 * @param columnDef ��������Զ���
 * @return �����ID
 */
LobId RecordOper::readLobId(const byte *record, ColumnDef *columnDef) {
	assert(columnDef->isLob());
	return *((LobId *)(record + columnDef->m_offset + columnDef->m_size - 8));
}

/** д������ID
 * @param record REC_REDUNDANT��ʽ����������
 * @param columnDef ��������Զ���
 * @param lobId �����ID
 */
void RecordOper::writeLobId(byte *record, ColumnDef *columnDef, LobId lobId) {
	assert(columnDef->isLob());
	*((LobId *)(record + columnDef->m_offset + columnDef->m_size - 8)) = lobId;
}

/** ��ȡ������С
 * @param record REC_MYSQL��ʽ����������
 * @param columnDef ��������Զ���
 * @return ������С
 */
uint RecordOper::readLobSize(const byte *record, ColumnDef *columnDef) {
	assert(columnDef->isLob());
	if (columnDef->m_type == CT_SMALLLOB)
		return read2BytesLittleEndian(record + columnDef->m_offset);
	else
		return read3BytesLittleEndian(record + columnDef->m_offset);
}

/** д�������С
 * @param record REC_MYSQL��ʽ����������
 * @param columnDef ��������Զ���
 * @param size ������С
 */
void RecordOper::writeLobSize(byte *record, ColumnDef *columnDef, uint size) {
	assert(columnDef->isLob());
	if (columnDef->m_type == CT_SMALLLOB)
		return write2BytesLittleEndian(record + columnDef->m_offset, size);
	else
		return write3BytesLittleEndian(record + columnDef->m_offset, size);
}

/** ��ȡ���������
 * @param record REC_MYSQL��ʽ����������
 * @param columnDef ��������Զ���
 * @return ���������
 */
byte* RecordOper::readLob(const byte *record, ColumnDef *columnDef) {
	assert(columnDef->isLob());
	return *((byte **)(record + columnDef->m_offset + columnDef->m_size - 8));
}

/** д����������
 * @param record REC_MYSQL��ʽ����������
 * @param columnDef ��������Զ���
 * @param lob ���������
 */
void RecordOper::writeLob(byte *record, ColumnDef *columnDef, byte *lob) {
	assert(columnDef->isLob());
	*((byte **)(record + columnDef->m_offset + columnDef->m_size - 8)) = lob;
}

/**
 * ���л��Ӽ�¼��ָ��������
 * ����REC_REDUNDANT��ʽ�ļ�¼�����洢�����������Ϣ��REC_MYSQL��ʽ�洢�������Ϣ
 * @param s			out �洢���л����������������������ĳ����㹻�洢��¼����
 * @param tableDef	��¼��Ӧ�ı���
 * @param subRecord	Ҫ���л����Ӽ�¼����ʽ����ΪREC_MYSQL����REC_REDUNDANT
 * @param isLobNeeded		�Ƿ����л������
 * @param isDataOnly	true��ʾֻ���л��Ӽ�¼�����ݲ��֣�false��Ҫ���л��Ӽ�¼��������Ϣ��Ĭ��false
 */
void RecordOper::serializeSubRecordMNR( Stream *s, const TableDef *tableDef, const SubRecord *subRecord, bool isLobNeeded, bool isDataOnly /*= false */ ) {
	assert(subRecord->m_format == REC_MYSQL || subRecord->m_format == REC_REDUNDANT);

	s->write(isLobNeeded);
	s->write(isDataOnly);

	RecFormat format = subRecord->m_format;
	s->write((u8)format);	// �����ʽ�������л���ʱ���ܹ�֪���費��Ҫ��ȡ�������Ϣ

	if (!isDataOnly) {
		s->write(subRecord->m_numCols);
		s->write((byte*)subRecord->m_columns, sizeof(subRecord->m_columns[0]) * subRecord->m_numCols);
	}

	// ���������mysql��ʽ���Ӽ�¼תΪ�䳤��ʽ����
	SubRecord subV(REC_VARLEN, subRecord->m_numCols, subRecord->m_columns, s->currPtr() + sizeof(u32), tableDef->m_maxRecSize, subRecord->m_rowId);
	SubRecord subR(REC_REDUNDANT, subRecord->m_numCols, subRecord->m_columns, subRecord->m_data, tableDef->m_maxRecSize, subRecord->m_rowId);
	RecordOper::convertSubRecordRV(tableDef, &subR, &subV);
	s->write((u32)subV.m_size);
	s->skip(subV.m_size);

	// ����REC_MYSQL��ʽ���Ӽ�¼����������ǿմ������Ϣ
	if (isLobNeeded) {
		byte *data = subRecord->m_data;
		for (uint i = 0; i < subRecord->m_numCols; i++) {
			ColumnDef *columnDef = tableDef->m_columns[subRecord->m_columns[i]];
			if (columnDef->isLob() && !RecordOper::isNullR(tableDef, subRecord, subRecord->m_columns[i])) {
				s->write(RecordOper::readLobSize(data, columnDef));
				s->write(RecordOper::readLob(data, columnDef), RecordOper::readLobSize(data, columnDef));
			}
		}
	}
}

/**
 * �����л�ָ�����е��Ӽ�¼��Ϣ���������л�ָ�������л��Ӽ�¼������Ϣ�������ʹ�ñ��������з����л�
 * @param s				�Ӽ�¼���л���������
 * @param tableDef		�Ӽ�¼��������
 * @param memoryContext	�ڴ������ģ��������䷴���л����Ӽ�¼����ռ�
 * @return �����л����Ӽ�¼����
 */
SubRecord* RecordOper::unserializeSubRecordMNR( Stream *s, const TableDef *tableDef, MemoryContext *memoryContext ) {
	bool isLobNeeded;
	s->read(&isLobNeeded);
	bool isDataOnly;
	s->read(&isDataOnly);
	NTSE_ASSERT(!isDataOnly);

	u8 format;
	s->read(&format);
	u16 numCols;
	s->read(&numCols);
	u16 *columns = (u16 *)memoryContext->alloc(sizeof(numCols) * numCols);
	s->readBytes((byte*)columns, sizeof(columns[0]) * numCols);
	u32 size;
	s->read(&size);
	SubRecord subV(REC_VARLEN, numCols, columns, s->currPtr(), size, INVALID_ROW_ID);
	SubRecord *ret = new (memoryContext->alloc(sizeof(SubRecord))) SubRecord(REC_REDUNDANT, numCols, columns,
		(byte *)memoryContext->alloc(tableDef->m_maxRecSize), tableDef->m_maxRecSize);
	RecordOper::convertSubRecordVR(tableDef, &subV, ret);
	ret->m_format = (RecFormat)format;
	s->skip(size);

	if (isLobNeeded) {	// ��ȡ�������Ϣ
		byte *data = ret->m_data;
		for (u16 i = 0; i < ret->m_numCols; i++) {
			ColumnDef *columnDef = tableDef->m_columns[ret->m_columns[i]];
			if (columnDef->isLob() && !RecordOper::isNullR(tableDef, ret, ret->m_columns[i])) {
				uint lobSize;
				s->read(&lobSize);
				RecordOper::writeLobSize(data, columnDef, lobSize);
				RecordOper::writeLob(data, columnDef, s->currPtr());
				s->skip(lobSize);
			}
		}
	}

	return ret;
}

/**
 * �����л��Ӽ�¼�ļ�¼������Ϣ��ָ�����棬����ֻ���л��Ӽ�¼���ݵ������ʹ�ñ����������л�
 * ֻ��ȡ�Ӽ�¼������Ϣ������Ϣ��ʽ��Ϣ����ȡ
 * @param s			���л���¼���ڵ�������
 * @param tableDef	�����л��Ӽ�¼��������
 * @param numCols	�Ӽ�¼������������Ϣ
 * @param columns	�Ӽ�¼�����е��к�
 * @param buf		out �����л��Ӽ�¼���
 */
void RecordOper::unserializeSubRecordMNR( Stream *s, const TableDef *tableDef, u16 numCols, u16 *columns, byte *buf ) {
	bool isLobNeeded;
	s->read(&isLobNeeded);
	bool isDataOnly;
	s->read(&isDataOnly);
	NTSE_ASSERT(isDataOnly);

	u8 format;
	s->read(&format);
	u32 size;
	s->read(&size);
	// �Ӽ�¼��ʽת��
	SubRecord subR(REC_REDUNDANT, numCols, columns, buf, tableDef->m_maxRecSize);
	SubRecord subV(REC_VARLEN, numCols, columns, s->currPtr(), size, INVALID_ROW_ID);
	RecordOper::convertSubRecordVR(tableDef, &subV, &subR);
	s->skip(size);

	if (isLobNeeded) {	// ��ȡ�������Ϣ
		for (u16 i = 0; i < subR.m_numCols; i++) {
			ColumnDef *columnDef = tableDef->m_columns[subR.m_columns[i]];
			if (columnDef->isLob() && !RecordOper::isNullR(tableDef, &subR, subR.m_columns[i])) {
				uint lobSize;
				s->read(&lobSize);
				RecordOper::writeLobSize(buf, columnDef, lobSize);
				RecordOper::writeLob(buf, columnDef, s->currPtr());
				s->skip(lobSize);
			}
		}
	}
}

/**
 * ����һ���Ӽ�¼���л�����Ҫռ�õĿռ�
 * ����REC_REDUNDANT��ʽ���Ӽ�¼������Ҫ��¼������������Ϣ������REC_MYSQL��ʽ���Ӽ�¼
 * @param tableDef	�Ӽ�¼������ı���
 * @param subRecord	Ҫ���л����Ӽ�¼
 * @param isLobNeeded		�Ƿ����л������
 * @param isDataOnly	true��ʾֻ���л��Ӽ�¼�����ݲ��֣�false��Ҫ���л��Ӽ�¼��������Ϣ��Ĭ��false
 * @return �����Ӽ�¼���л�֮����Ҫ�Ŀռ��С
 */
size_t RecordOper::getSubRecordSerializeSize( const TableDef *tableDef, const SubRecord *subRecord, bool isLobNeeded, bool isDataOnly /*= false */ ) {
	assert(subRecord->m_format == REC_MYSQL || subRecord->m_format == REC_REDUNDANT);

	size_t size = 0;
	// �Ƿ�ֻ�洢������Ϣ�����Ƿ����л���������ݣ�format��m_numCols��m_columns��m_size��Ϣ�洢��Ҫ�Ŀռ�
	size += sizeof(bool) * 2 + sizeof(u8);
	if (!isDataOnly)
		size += sizeof(u16) + sizeof(subRecord->m_columns[0]) * subRecord->m_numCols;
	// ���ϼ�¼����ѹ���ɱ䳤��ʽ��Ҫ�Ŀռ�
	size += getSubRecordSizeRV(tableDef, subRecord) + sizeof(u32);

	// ����REC_MYSQL��ʽ�ļ�������������Ҫ�Ŀռ�
	if (isLobNeeded) {	// ��ȡ�������Ϣ
		for (u16 i = 0; i < subRecord->m_numCols; i++) {
			ColumnDef *columnDef = tableDef->m_columns[subRecord->m_columns[i]];
			if (columnDef->isLob() && !RecordOper::isNullR(tableDef, subRecord, subRecord->m_columns[i])) {
				size_t lobSize;
				byte *lob;
				RedRecord::readLob(tableDef, subRecord->m_data, subRecord->m_columns[i], (void**)&lob, &lobSize);
				size += sizeof(u32) + lobSize;
			}
		}
	}

	return size;
}

#ifdef TNT_ENGINE
u16 RecordOper::extractDiffColumns(const TableDef *tableDef, const Record *rec1, const Record *rec2, u16 **cols, MemoryContext *ctx, bool onlyLob) {
	assert(rec1->m_format == REC_REDUNDANT && rec2->m_format == REC_REDUNDANT);
	u16 numCols = 0;
	u16 totalNumCols = tableDef->m_numCols;
	*cols = new (ctx->alloc(totalNumCols*sizeof(u16))) u16[totalNumCols];

	int result = 0;
	Column column1, column2;
	RedRow row1(tableDef, rec1);
	RedRow row2(tableDef, rec2);
	for (u16 cno = 0; cno < totalNumCols; cno++) {
		if (onlyLob && !tableDef->getColumnDef(cno)->isLob())
			continue;

		row1.columnAt(cno, &column1);
		row2.columnAt(cno, &column2);
		
		// Ŀǰֻ��purge����ô˷���������Ƚ��в���ʹ��mysql�ϲ�ķ�����������Ϊ��׺�ո�����ͬ��varchar���Բ���ͬ����JIRA��NTSETNT-308
		result = (column1.size() == column2.size())? memcmp(column1.data(), column2.data(), column1.size()): 1;
		if (result != 0) {
			(*cols)[numCols] = cno;
			numCols++;
		}
	}

	return numCols;
}

void RecordOper::parseAndPrintRedSubRecord(Syslog *syslog, const TableDef *tableDef, const SubRecord *subRec) {
	assert(subRec->m_format == REC_REDUNDANT);
	Column column;
	RedRow row1(tableDef, subRec);
	for(u16 i = 0; i < subRec->m_numCols; i++) {
		u16 cno = subRec->m_columns[i];
		//��ӡ����Ϣ
		const ColumnDef *columnDef = tableDef->getColumnDef(cno); 
		switch(columnDef->m_type) {
		case CT_TINYINT:
			syslog->log(EL_DEBUG, "---------ColNo: %d || %d", cno, RedRecord::readTinyInt(tableDef, subRec->m_data, cno));
			break;
		case CT_SMALLINT:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || %d", cno, RedRecord::readSmallInt(tableDef, subRec->m_data, cno));
			break;
		case CT_MEDIUMINT:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || %d", cno, RedRecord::readMediumInt(tableDef, subRec->m_data, cno));
			break;
		case CT_INT:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || %d", cno, RedRecord::readInt(tableDef, subRec->m_data, cno));
			break;
		case CT_BIGINT:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || %lld", cno, RedRecord::readBigInt(tableDef, subRec->m_data, cno));
			break;
		case CT_FLOAT:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || %f", cno, RedRecord::readFloat(tableDef, subRec->m_data, cno));
			break;	
		case CT_DOUBLE:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || %lf", cno, RedRecord::readDouble(tableDef, subRec->m_data, cno));
			break;
		case CT_VARCHAR:
		case CT_CHAR:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || Char/Varchar not display", cno);
			break;
		case CT_VARBINARY:
		case CT_BINARY:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || Binary/Varbinary not display", cno);
		case CT_DECIMAL:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || Decimal not display", cno);
			break;
		case CT_SMALLLOB:
		case CT_MEDIUMLOB:
			syslog->log(EL_DEBUG,  "---------ColNo: %d || LobId: %llu", cno, (*(LobId*)(subRec->m_data + columnDef->m_offset + columnDef->m_size - 8)));
			break;
		default:
			assert(columnDef->m_type == CT_RID);
			syslog->log(EL_DEBUG,  "---------Col Type is CT_RID which is not display");
		}
	}
}

#endif

/** ���²����ṹ */
struct UpdateOper {
	UpdateOper(byte* dst, byte* src, size_t size, bool copy)
		: m_src(src), m_dst(dst), m_size(size), m_copyOp(copy)
	{}
	byte* m_src;   /** Դ */
	byte* m_dst;   /** Ŀ�� */
	size_t m_size; /** �ڴ泤�� */
	bool m_copyOp; /** ���¶��� -- ����: true, �ƶ�:false */
};

static void doUpdateRecordVRInPlace(const TableDef *tableDef, Record *record, const SubRecord *update, size_t oldBufferSize) {
	UNREFERENCED_PARAMETER(oldBufferSize);
	assert(update->m_format == REC_REDUNDANT);
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert(ColList(update->m_numCols, update->m_columns).isAsc());
	assert((uint)tableDef->m_maxRecSize <= update->m_size);

	RedRow updRow(tableDef, update);
	VarLenRow oldRow(tableDef, record);

	Column oldColumn;
	Column newColumn;
	uint updColIdx = 0;
	byte* start = record->m_data + oldRow.bitmapBytes();
	vector<UpdateOper> opStack; // δ��ɸ��²���ջ
	opStack.reserve(tableDef->m_numCols); // TODO: ������һ���ڴ���䣬�д��Ż�

	for(Column* oc = oldRow.firstColumn(&oldColumn); oc; ) {
		if (oc->colNo() < update->m_columns[updColIdx]) { // ����Ǹ�����
			// �����ɶ���Ǹ�������ɵ�segment
			byte*  segment = oc->data();
			size_t length = 0;
			for (; oc->colNo() < update->m_columns[updColIdx]; oc = oldRow.nextColumn(oc))
				length += oc->size();
			assert(record->m_data + oldBufferSize >= start + length);

			if (start < segment) { // �Ǹ�����ǰ��
				memmove(start, segment, length);
			} else if (start > segment) { // �Ǹ����к���
				// Ϊ�˱��⸲���������ݣ�����ֱ��memmove����˼�¼�˸��²���
				opStack.push_back(UpdateOper(start, segment, length, false));
			}
			start += length;
		}
		if (oc) { // ���������
			assert(oc->colNo() == update->m_columns[updColIdx]);
			updRow.columnAt(oc->colNo(), &newColumn);
			assert(record->m_data + oldBufferSize >= start + newColumn.size());
			bool isNull = oc->isNull();
			u16 bmOffset = oc->bitmapOffset();
			byte *data = oc->data();
			size_t size = oc->size();
			oc = oldRow.nextColumn(oc); // ���ݸ���֮ǰ��oc����ƶ�
			if (data + size >= start + newColumn.size()) { // ��ʱ���Ḳ����һ������
				memcpy(start, newColumn.data(), newColumn.size());
			} else { // Ϊ�˱��⸲���������ݣ�����ֱ��memcpy����˼�¼�˸��²���
				opStack.push_back(UpdateOper(start, newColumn.data(), newColumn.size(), true));
			}
			start += newColumn.size();
			if (isNull != newColumn.isNull()) { // ����Nullλͼ
				newColumn.isNull() ?
					BitmapOper::setBit(record->m_data, oldRow.bitmapBytes() << 3, bmOffset) :
					BitmapOper::clearBit(record->m_data, oldRow.bitmapBytes() << 3, bmOffset);
			}
			if (++updColIdx >= update->m_numCols && oc) { // ��Ҳû�д�������
				// �ƶ�ʣ����
				byte*  segment = oc->data();
				size_t length = 0;
				for (; oc; oc = oldRow.nextColumn(oc))
					length += oc->size();
				assert(record->m_data + oldBufferSize >= start + length);
				memmove(start, segment, length);
				start += length;
				break;
			}
		}
	}
	assert(updColIdx == update->m_numCols);
	// �ط�δ��ɲ���
	for (vector<UpdateOper>::reverse_iterator iter = opStack.rbegin(); iter != opStack.rend() ; ++iter) {
		if (iter->m_copyOp)
			memcpy(iter->m_dst, iter->m_src, iter->m_size);
		else
			memmove(iter->m_dst, iter->m_src, iter->m_size);
	}
	record->m_size = (uint)(start - record->m_data);
}


/**
* �����¼�����г���
* @pre col������ѹ�����ݣ�����ΪNULL
* @param col �ж���
* @param buf ������
*/
static inline size_t getColSize(const ColumnDef* col, const byte* buf) {
	uint actualSize = 0;
	uint lenBytes = 0;
	
	// �˴�VAR/FIX��REDUNDANT ��ת
	if (!isFixedLen(col->m_type)) {
		lenBytes = col->m_lenBytes;
		actualSize = readU32Le(buf, lenBytes);
		assert(actualSize <= col->m_size);
		return actualSize + lenBytes;
	} else {
		return col->m_size;
	}
}

/**
* ����������¼���г���
* @pre col������ѹ�����ݣ�����ΪNULL
* @param col �ж���
* @param buf ������
* @param prefixLen ǰ׺���ȣ�����ǰ׺����
*/
static inline size_t getKeyColSize(const ColumnDef* col, const byte* buf, u32 prefixLen) {
	uint actualSize = 0;
	uint lenBytes = 0;

	if (!col->isFixlen()) {
		lenBytes = col->m_lenBytes;
		if (isLob(col->m_type))
			lenBytes = prefixLen > 255 ? 2 : 1;
		actualSize = readU32Le(buf, lenBytes);
		assert( (isLob(col->m_type) && actualSize <= prefixLen) || (!isLob(col->m_type) && actualSize <= col->m_size));
		return actualSize + lenBytes;
	} else {
		if (prefixLen > 0)
			return prefixLen;
		else
			return col->m_size;
	}
}


/**
* ����REC_UPPERMYSQL��¼�г���
* @pre col������ѹ�����ݣ�����ΪNULL
* @param col �ж���
* @param buf ������
*/
static inline size_t getUppMysqlColSize(const ColumnDef* col, const byte* buf) {
	uint actualSize = 0;
	uint lenBytes = 0;

	// ���ϲ�MYSQL��ʽ�У������ֶ������ڱ䳤����
	if (!isFixedLen(col->m_type) || col->isLongVar()) {
		lenBytes = col->m_lenBytes;
		if (col->isLongVar())
			lenBytes = 2;
		actualSize = readU32Le(buf, lenBytes);
		assert(actualSize <= col->m_mysqlSize);
		return actualSize + lenBytes;
	} else {		
		return col->m_mysqlSize;
	}
}


/** �Ƿ��ѹ���������� */
static bool isCompressableNumberType(ColumnType type) {
	switch (type) {
		case CT_SMALLINT:   // ���ֽ�����
		case CT_MEDIUMINT:	// ���ֽ�����
		case CT_INT:        // ���ֽ�����
		case CT_BIGINT:		// ���ֽ�����
		case CT_RID:        // ROWID
			return true;
		default:
			return false;
	}
}


static void removeLenbytes(byte lenBytes, byte** buf, size_t* size) {
	*size =  readU32Le(*buf, lenBytes);
	*buf = *buf + lenBytes;
}

/** �Ƚ��������� */
template<class T> inline int compareNumber(T l, T r) {
	return (l>r) - (l<r);
}
template<> inline int compareNumber(s8 l, s8 r) {
	return (int)l - (int)r;
}
template<> inline int compareNumber(s16 l, s16 r) {
	return (int)l - (int)r;
}

#ifdef LITTLE_ENDIAN
/** 3�ֽ�����ת���������ֽ������� */
inline int leS24ToHostInt(const s8 *src) {
	int v = (int)*(src + 2) << 16;
	s8 *p = (s8 *)&v;
	*p = *src;
	*(p + 1) = *(src + 1);
	return v;
}
/** 3�ֽ�����ת���������ֽ������� */
inline uint leU24ToHostUint(const u8 *src) {
	uint v = 0;
	u8 *p = (u8 *)&v;
	*p = *src;
	*(p + 1) = *(src + 1);
	*(p + 2) = *(src + 2);
	return v;
}
#endif
/**
 * �Ƚ�Little Endian�з�������
 * @param num1 ����1
 * @param num2 ����2
 * @param len ��������
 */
static int compareNumberLe(byte *num1, byte *num2, size_t len) {
	switch(len) {
		case 4:
			return compareNumber(littleEndianToHost(*(s32 *)num1), littleEndianToHost(*(s32 *)num2));
		case 8:
			return compareNumber(littleEndianToHost(*(s64 *)num1), littleEndianToHost(*(s64 *)num2));
		case 2:
			return compareNumber(littleEndianToHost(*(s16 *)num1), littleEndianToHost(*(s16 *)num2));
		case 1:
			return compareNumber(*(s8 *)num1, *(s8 *)num2);
		case 3:
			return compareNumber(leS24ToHostInt((s8 *)num1), leS24ToHostInt((s8 *)num2));
		default:
			assert(false);
			return 0;
	}
}

/**
 * �Ƚ�Little Endian�޷�������
 * @param num1 ����1
 * @param num2 ����2
 * @param len ��������
 */
static int compareUnsignedNumberLe(byte *num1, byte *num2, size_t len) {
	switch(len) {
		case 4:
			return compareNumber(littleEndianToHost(*(u32 *)num1), littleEndianToHost(*(u32 *)num2));
		case 8:
			return compareNumber(littleEndianToHost(*(u64 *)num1), littleEndianToHost(*(u64 *)num2));
		case 2:
			return compareNumber(littleEndianToHost(*(u16 *)num1), littleEndianToHost(*(u16 *)num2));
		case 1:
			return compareNumber(*(u8 *)num1, *(u8 *)num2);
		case 3:
			return compareNumber(leU24ToHostUint((u8 *)num1), leU24ToHostUint((u8 *)num2));
		default:
			assert(false);
			return 0;
	}
}

/**
 * �Ƚ������еĴ�С
 * @param col1	��һ��
 * @param col2	�ڶ���
 * @param sndCompressed  �ڶ����Ƿ�ѹ��
 * @param cmpLob �Ƿ�Ƚϴ��������
 * @return -1/0/1 for </=/>
 */
static int compareColumn(Column *col1, Column *col2, bool sndCompressed, bool cmpLob, bool cmpLobAsVarchar) {
	const ColumnDef* columnDef = col1->def();
	assert(columnDef->m_no == col2->def()->m_no);
	int result = 0;
	if (col2->isNull() || col1->isNull()) { // ����NULL��
		result = (int)col2->isNull() - (int)col1->isNull();
	} else { // ��NULL��
		byte* data1 = col1->data();
		size_t size1 = col1->size();
		byte* data2 = col2->data();
		size_t size2 = col2->size();
		byte decompressed[8];
		switch(columnDef->m_type) {
			case CT_SMALLINT:
			case CT_MEDIUMINT:
			case CT_INT:
			case CT_BIGINT:
			case CT_RID: { // ��ѹ���������� -- ��ѹ֮
				if (sndCompressed) {
					NumberCompressor::decompress(data2, size2, decompressed, columnDef->m_size);
					data2 = decompressed;
					size2 = columnDef->m_size;
				}
			}
			case CT_TINYINT:
				assert(size1 == size2);
				result = columnDef->m_prtype.isUnsigned() ?
							compareUnsignedNumberLe(data1, data2, size1) : compareNumberLe(data1, data2, size1);
				break;
			case CT_FLOAT:
				assert(sizeof(float) == 4);
				result = compareNumber(littleEndianToHost(*(float *)data1), littleEndianToHost(*(float *)data2));
				break;
			case CT_DOUBLE:
				assert(sizeof(double) == 8);
				result = compareNumber(littleEndianToHost(*(double *)data1), littleEndianToHost(*(double *)data2));
				break;
			case CT_VARCHAR: // VARCHAR��ȥ���ַ����ײ����Ƚ��бȽ�
				removeLenbytes(columnDef->m_lenBytes, &data1, &size1);
				removeLenbytes(columnDef->m_lenBytes, &data2, &size2);
			case CT_CHAR:
				// �ַ����Ƚ�ʱ����ĩβ�Ŀո񣬵�������\0��\t�ȣ�����˵������
				// 'a\0\0' < 'a\0' < 'a\t\t' < 'a\t' < 'a' = 'a ' = 'a  '
				result = Collation::strcoll(columnDef->m_collation, data1, size1, data2, size2);
				break;
			case CT_VARBINARY:
				removeLenbytes(columnDef->m_lenBytes, &data1, &size1);
				removeLenbytes(columnDef->m_lenBytes, &data2, &size2);
				result = memcmp(data1, data2, min((u32)size1, (u32)size2));
				if (result == 0) // ��ͬǰ׺����Ƚϳ���
					return compareNumber((u32)size1, (u32)size2);
				break;
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				if (cmpLob) { // �Ƚϴ��������
					if (!cmpLobAsVarchar) {
						data1 = parseLobColumn(col1, &size1);
						data2 = parseLobColumn(col2, &size2);
						result = memcmp(data1, data2, min((u32)size1, (u32)size2));
						if (result == 0) // ��ͬǰ׺����Ƚϳ���
							return compareNumber((u32)size1, (u32)size2);
						break;
					} else {
						removeLenbytes(col1->lenBytes(), &data1, &size1);
						removeLenbytes(col2->lenBytes(), &data2, &size2);
						// �ַ����Ƚ�ʱ����ĩβ�Ŀո񣬵�������\0��\t�ȣ�����˵������
						// 'a\0\0' < 'a\0' < 'a\t\t' < 'a\t' < 'a' = 'a ' = 'a  '
						result = Collation::strcoll(columnDef->m_collation, data1, size1, data2, size2);
						break;
					}
				}
			case CT_DECIMAL:
			case CT_BINARY:
				result = memcmp(data1, data2, min((u32)size1, (u32)size2));
				if (result == 0) // ��ͬǰ׺����Ƚϳ���
					return compareNumber((u32)size1, (u32)size2);
				break;
			default:
				assert(false);
				break;
		}
	}
	return result;
}

/**
 * ����REC_MYSQL��ʽ��¼�Ĵ�����ֶ�
 * @param columns �ֶ�
 * @param lobSize [out] ����󳤶�
 * @return ������ڴ��ַ
 */
byte* parseLobColumn(const Column *column, size_t *lobSize) {
	const ColumnDef *columnDef = column->def();
	assert(columnDef->m_type == CT_SMALLLOB || columnDef->m_type == CT_MEDIUMLOB);

	byte *lob;
	if (column->isNull()) {
		lob = 0;
		*lobSize = 0;
	} else {
		lob = *((byte **)(column->data() + columnDef->m_size - 8));
		if (columnDef->m_type == CT_SMALLLOB)
			*lobSize = (size_t)read2BytesLittleEndian(column->data());
		else
			*lobSize = (size_t)read3BytesLittleEndian(column->data());
	}
	return lob;
}

/** �Ӽ�¼��nullable�и��� */
u16 calcNullableCols(const TableDef* tableDef, const SubRecord* sb) {
	u16 cnt = 0;
	for (u16 i = 0; i < sb->m_numCols; ++i) {
		if (tableDef->m_columns[sb->m_columns[i]]->m_nullable)
			++cnt;
	}
	return cnt;
}
/** �ж��Ƿ�ΪMYSQL��ʽ�� */
static bool isUppMysqlFormat(RecFormat format) {
	return format == REC_UPPMYSQL;
}
/** �ж��Ƿ����࣬����ռ�õ��ڴ��С�Ƿ�̶�Ϊ����г��� */
static bool isRedundantFormat(RecFormat format) {
	return format == REC_REDUNDANT || format == KEY_PAD || format == REC_FIXLEN || 
		format == REC_MYSQL || format == KEY_MYSQL;
}
/** �ж��Ƿ�Key��ʽ */
static bool isKeyFormat(RecFormat format) {
	return (format == KEY_COMPRESS || format == KEY_NATURAL || format == KEY_PAD);
}
/** �ж��Ƿ��¼��ʽ */
static bool isRecFormat(RecFormat format) {
	return (format == REC_REDUNDANT || format == REC_MYSQL || format == REC_VARLEN || 
		format == REC_FIXLEN || format == REC_COMPRESSED || format == REC_COMPRESSORDER);
}

//////////////////////////////////////////////////////////////////////////
//// ColList
//////////////////////////////////////////////////////////////////////////

/** ����һ���������б� */
ColList::ColList() {
	m_size = 0;
	m_cols = NULL;
}

/** ���������б�
 *
 * @param size ���Ը���
 * @param cols �����Ժ�
 */
ColList::ColList(u16 size, u16 *cols) {
	m_size = size;
	m_cols = cols;
}

/** ����һ��ӵ��ָ���������������ԺŴ�startCno�����������б�
 * @param mc ���ڷ��䷵��ֵ�����б����ÿռ���ڴ����������
 * @param size ���Ը���
 * @return �����б�
 */
ColList ColList::generateAscColList(MemoryContext *mc, u16 startCno, u16 size) {
	u16 *cols = (u16 *)mc->alloc(sizeof(u16) * size);
	for (u16 i = 0; i < size; i++)
		cols[i] = startCno + i;
	return ColList(size, cols);
}

/** ���������б�
 * @param mc ���ڷ��䷵��ֵ�����б����ÿռ���ڴ����������
 * @return ��ǰ�����б�Ŀ���
 */
ColList ColList::copy(MemoryContext *mc) const {
	return ColList(m_size, (u16 *)mc->dup(m_cols, sizeof(u16) * m_size));
}

/** �ϲ����������б�(ȥ���ظ�)
 * @pre ���Ժű������
 *
 * @param a Ҫ�ϲ��ĵ�һ�������б�
 * @param b Ҫ�ϲ��ĵڶ��������б�
 */
void ColList::merge(const ColList &a, const ColList &b) {
	assert(a.isAsc() && b.isAsc());
	u16 i = 0, j = 0, k = 0;
	while (i < a.m_size && j < b.m_size) {
		if (a.m_cols[i] < b.m_cols[j])
			m_cols[k++] = a.m_cols[i++];
		else if (a.m_cols[i] > b.m_cols[j])
			m_cols[k++] = b.m_cols[j++];
		else {
			m_cols[k++] = a.m_cols[i++];
			j++;
		}
	}
	while (i < a.m_size)
		m_cols[k++] = a.m_cols[i++];
	while (j < b.m_size)
		m_cols[k++] = b.m_cols[j++];
	assert(k <= m_size);
	m_size = k;
}

/** �ϲ����������б�(ȥ���ظ�)
 * @pre ���Ժű������
 *
 * @param mc ���ڷ��䷵��ֵ���ÿռ���ڴ����������
 * @param another ����������б�ϲ�
 * @return �ϲ����
 */
ColList ColList::merge(MemoryContext *mc, const ColList &another) const {
	assert(isAsc() && another.isAsc());
	ColList r(m_size + another.m_size, (u16 *)mc->alloc(sizeof(u16) * (m_size + another.m_size)));
	r.merge(*this, another);
	return r;
}

/** ���㵱ǰ�����б��ȥָ�������б���ʣ�µ������б�
 * @pre ���Ժű������
 *
 * @param mc ���ڷ��䷵��ֵ���ÿռ���ڴ����������
 * @param another ��ȥ��������б��е�����
 * @return ʣ�µ������б�
 */
ColList ColList::except(MemoryContext *mc, const ColList &another) const {
	assert(isAsc() && another.isAsc());
	ColList r(0, (u16 *)mc->alloc(sizeof(u16) * m_size));
	u16 i = 0, j = 0;
	while (i < m_size && j < another.m_size) {
		if (m_cols[i] < another.m_cols[j])
			r.m_cols[r.m_size++] = m_cols[i++];
		else if (m_cols[i] > another.m_cols[j])
			j++;
		else {
			i++;
			j++;
		}
	}
	while (i < m_size)
		r.m_cols[r.m_size++] = m_cols[i++];
	return r;
}

/** �����ԺŴ�С��������
 *
 * @param mc ���ڷ��䷵��ֵ���ÿռ���ڴ����������
 * @return ���Ժ��ź���������б�
 */
ColList ColList::sort(MemoryContext *mc) const {
	u16 *colsCopy = (u16 *)mc->alloc(sizeof(u16) * m_size);
	memcpy(colsCopy, m_cols, sizeof(u16) * m_size);
	std::sort(colsCopy, colsCopy + m_size);
	return ColList(m_size, colsCopy);
}

/** �ж�ָ���������б��뵱ǰ�����б��Ƿ��н���
 * @pre ���Ժű������
 *
 * @param another �����б�
 * @return �Ƿ��н���
 */
bool ColList::hasIntersect(const ColList &another) const {
	assert(isAsc() && another.isAsc());
	u16 i = 0, j = 0;
	while (i < m_size && j < another.m_size) {
		if (m_cols[i] < another.m_cols[j])
			i++;
		else if (m_cols[i] > another.m_cols[j])
			j++;
		else
			return true;
	}
	return false;
}

/** �Ƚ�ָ���������б��뵱ǰ�����б��Ƿ����
 * @param another ���ڱȽϵ���һ�����б�
 * @return �Ƿ����
 */
bool ColList::operator == (const ColList &another) const {
	if (m_size != another.m_size)
		return false;
	return memcmp(m_cols, another.m_cols, sizeof(u16) * m_size) == 0;
}

/** �ж����Ժ��Ƿ����
 * @return ���Ժ��Ƿ����
 */
bool ColList::isAsc() const {
	for (u16 i = 1; i < m_size; i++)
		if (m_cols[i] <= m_cols[i - 1])
			return false;
	return true;
}

/** �жϵ�ǰ�������ǲ���ָ������һ�������е��Ӽ�
 * @pre	���������б�Ӧ���ǵ�����
 * @param another �����ж��ǲ��ǳ����������б�
 * @return true��ʾthis��another���Ӽ������򷵻�false
 */
bool ColList::isSubsetOf(const ColList &another) const {
	assert(isAsc() && another.isAsc());
	if (another.m_size < m_size)
		return false;

	u16 i = 0, j = 0;
	while (i < m_size && j < another.m_size) {
		if (another.m_cols[j] < m_cols[i])
			j++;
		else if (another.m_cols[j] == m_cols[i]) {
			i++;
			j++;
		} else
			return false;
	}

	return !(i < m_size);
}
//////////////////////////////////////////////////////////////////////////
//// SubrecExtractors
//////////////////////////////////////////////////////////////////////////


struct MemSegment {
	u16 m_offset;	// ��Լ�¼ͷ��ƫ��
	u16 m_size;		// �ڴ�γ���
};

// ����memcpy���С�ڱ�����ʱ�� �ɺϲ�Ϊһ��memcpy
const size_t MAX_MEMCPY_MERGE_SIZE = 32;

/**
 * ������¼�������Ӽ�¼��ȡ��
 */
class SubRecExtractorFR: public SubrecExtractor {
public:
	SubRecExtractorFR(MemoryContext *ctx, const TableDef *tableDef
		, u16 numCols, const u16* columns)
		:m_tableDef(tableDef), m_numSegment(0) {
		assert(numCols <= tableDef->m_numCols);

		UNREFERENCED_PARAMETER(ctx);

		// ����λͼ
		if (tableDef->m_bmBytes > 0) {
			m_numSegment = 1;
			m_segments[0].m_offset = 0;
			m_segments[0].m_size = tableDef->m_bmBytes;
		}

		for (u16 i = 0; i < numCols; ++i) {
			u16 cno = columns[i];
			assert(cno < m_tableDef->m_numCols);
			u16 offset =  m_tableDef->m_columns[cno]->m_offset;
			u16 size = m_tableDef->m_columns[cno]->m_size;
			if (m_numSegment >= 1) {
				if (m_segments[m_numSegment - 1].m_offset + m_segments[m_numSegment - 1].m_size +  MAX_MEMCPY_MERGE_SIZE >= offset) {
					// �ҵ����Ժϲ����ڴ��
					m_segments[m_numSegment - 1].m_size = offset + size - m_segments[m_numSegment - 1].m_offset;
					continue;
				}
			}
			++m_numSegment;
			m_segments[m_numSegment - 1].m_offset = offset;
			m_segments[m_numSegment - 1].m_size = size;
		}
	}

	/**
	 * ��ȡ�Ӽ�¼
	 * @param record �������ͼ�¼
	 * @param subRecord ���������Ӽ�¼
	 */
	virtual void extract(const Record *record, SubRecord *subRecord) {
		assert(record->m_format == REC_FIXLEN);
		assert(subRecord->m_format == REC_REDUNDANT);
		assert(m_tableDef->m_recFormat == REC_FIXLEN);
		assert(m_tableDef->m_maxRecSize == record->m_size);
		assert(m_tableDef->m_maxRecSize <= subRecord->m_size);

		for (uint i = 0; i < m_numSegment; ++i) {
			assert(m_segments[i].m_offset + m_segments[i].m_size <= (u16)subRecord->m_size);
			memcpy(subRecord->m_data + m_segments[i].m_offset
							, record->m_data + m_segments[i].m_offset, m_segments[i].m_size);
		}
		subRecord->m_rowId = record->m_rowId;
		subRecord->m_size = m_tableDef->m_maxRecSize;
	}
private:
	const TableDef *m_tableDef;	// ����
	MemSegment m_segments[Limits::MAX_COL_NUM + 1]; // �ڴ�����
	uint m_numSegment; // �ڴ�������Ŀ
};

/**
 * �ж������Ƿ񶨳�
 * @param type ����
 * @return true:������ false���䳤
 */
static bool isFixedLen(ColumnType type) {
	return (type != CT_VARCHAR && type != CT_VARBINARY);
}

/**
 * �ж������Ƿ��Ǵ�����ֶ�
 * @param type ����
 * @return true:�Ǵ���� false�����Ǵ����
 */
static bool isLob(ColumnType type) {
	return (type == CT_SMALLLOB) || (type == CT_MEDIUMLOB);
}

/**
 * �䳤��¼�������Ӽ�¼��ȡ��
 */
class SubRecExtractorVR: public SubrecExtractor {
	// ����Ƭ������������������к�һ���䳤��
	struct RowFragement {
		u16 m_size;			// �����еĳ���
		u16 m_varlenBytes;	// �䳤�У������ֽ���
		u16 m_srcStart;		// Դ��ʼƫ��(��Ա�fragment��ʼ��ַ), -1 ��ʾ���追����Ŀ��
		u16 m_dstStart;		// Ŀ����ʼƫ��, -1 ��ʾ���追����Ŀ��
		u16 m_copySize;		// �������ȣ��������䳤��
		bool m_copyVarCol;	// �Ƿ񿽱��䳤��
	};
public:
	SubRecExtractorVR(MemoryContext *ctx, const TableDef *tableDef
						, u16 numCols, const u16* columns);
	virtual ~SubRecExtractorVR();

	virtual void extract(const Record *record, SubRecord *subRecord);
	string toString();

private:
	const TableDef *m_tableDef;	// ����
	MemoryContext *m_ctx;		// �ڴ�������
	RowFragement *m_fragments;	// ��
	uint m_numFragments;		// ����Ŀ
};

/**
 * ����SubRecExtractorVR
 * @param ctx �ڴ�������
 * @param tableDef ����
 * @param numCols ����ȡ����
 * @param columns ����ȡ��
 */
SubRecExtractorVR::SubRecExtractorVR(MemoryContext *ctx, const TableDef *tableDef
									, u16 numCols, const u16* columns)
	: m_tableDef(tableDef), m_ctx(ctx) {

	if (ctx)
		m_fragments = (RowFragement *)ctx->alloc((1 + tableDef->m_numCols) * sizeof(RowFragement));
	else
		m_fragments = new RowFragement[1 + tableDef->m_numCols];

	// ����λͼ
	m_fragments[0].m_size = m_tableDef->m_bmBytes;
	m_fragments[0].m_varlenBytes = 0;
	m_fragments[0].m_srcStart = 0;
	m_fragments[0].m_dstStart = 0;
	m_fragments[0].m_copySize = m_tableDef->m_bmBytes;
	m_fragments[0].m_copyVarCol = false;
	m_numFragments = 1;

	u16 colIdx = 0;
	u16 segStartOffset = 0; // ��ǰ�εĿ�ʼƫ��
	for (u16 cno = 0; cno < tableDef->m_numCols; ++cno) {
		ColumnDef *colDef = m_tableDef->m_columns[cno];
		if (isFixedLen(colDef->m_type)) { // ��ǰ���Ƕ�����
			if (!m_fragments[m_numFragments - 1].m_varlenBytes) { // ��ǰ�β������䳤��
				if (cno == columns[colIdx]) { // ��ǰ���Ǵ���ȡ��
					if (m_fragments[m_numFragments - 1].m_srcStart == (u16)-1) { // ���ε�һ������ȡ��
						m_fragments[m_numFragments - 1].m_srcStart = colDef->m_offset - segStartOffset;
						m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
						m_fragments[m_numFragments - 1].m_copySize = colDef->m_size;
					} else { // ���Ǳ��εĵ�һ������ȡ��, ���Ժϲ�memcpy
						u16 gap = colDef->m_offset - segStartOffset
							- m_fragments[m_numFragments - 1].m_srcStart - m_fragments[m_numFragments - 1].m_copySize;
						if (gap <= MAX_MEMCPY_MERGE_SIZE) { // ���Ժϲ�����
							m_fragments[m_numFragments - 1].m_copySize
								= colDef->m_offset + colDef->m_size - m_fragments[m_numFragments - 1].m_dstStart;
						} else { // ���ɺϲ�,��ʼ�µ�һ��
							goto _new_segment;
						}
					}
				}
				m_fragments[m_numFragments - 1].m_size = m_fragments[m_numFragments - 1].m_size + colDef->m_size;
			} else { // ��ǰ���Ѿ������䳤�У���ô��ʼ�µ�һ��(�����д�ͷ)
_new_segment:
				segStartOffset = m_tableDef->m_columns[cno]->m_offset;
				++ m_numFragments;
				m_fragments[m_numFragments - 1].m_size = colDef->m_size;
				m_fragments[m_numFragments - 1].m_varlenBytes = 0;
				m_fragments[m_numFragments - 1].m_copyVarCol = false;
				if (cno == columns[colIdx]) { // ����ȡ��
					m_fragments[m_numFragments - 1].m_srcStart = 0;
					m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
					m_fragments[m_numFragments - 1].m_copySize = colDef->m_size;
				} else {
					m_fragments[m_numFragments - 1].m_srcStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_dstStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_copySize = 0;
				}
			}
		} else { // ��ǰ���Ǳ䳤��
			if (m_fragments[m_numFragments - 1].m_varlenBytes != 0) { // ��ǰ���Ѱ����䳤��
_new_segment_var: // ��ʼ�µ�һ��
				segStartOffset = m_tableDef->m_columns[cno]->m_offset;
				++ m_numFragments;
				m_fragments[m_numFragments - 1].m_size = 0;
				m_fragments[m_numFragments - 1].m_copySize = 0;
				m_fragments[m_numFragments - 1].m_varlenBytes = colDef->m_lenBytes;
				if (cno == columns[colIdx]) { // ����ȡ��
					m_fragments[m_numFragments - 1].m_srcStart = 0;
					m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
					m_fragments[m_numFragments - 1].m_copyVarCol = true;
				} else {
					m_fragments[m_numFragments - 1].m_srcStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_dstStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_copyVarCol = false;
				}
			} else { // ��ǰ�β������䳤��
				if (cno == columns[colIdx]) { // ����ȡ��
					if (m_fragments[m_numFragments - 1].m_srcStart == (u16)-1) { // û�д������Ķ�����(����ϲ�����)
						m_fragments[m_numFragments - 1].m_srcStart
							= colDef->m_offset - segStartOffset;
						m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
						m_fragments[m_numFragments - 1].m_copyVarCol = true;
					}  else { // ���Ժϲ������кͱ䳤��
						u16 gap = colDef->m_offset - segStartOffset
							- m_fragments[m_numFragments - 1].m_srcStart - m_fragments[m_numFragments - 1].m_copySize;
						if (gap <= MAX_MEMCPY_MERGE_SIZE) { // ���Ժϲ�����
							m_fragments[m_numFragments - 1].m_copyVarCol = true;
							m_fragments[m_numFragments - 1].m_copySize = colDef->m_offset
																			- m_fragments[m_numFragments - 1].m_dstStart;
						} else { // ���ɺϲ�,��ʼ�µ�һ��
							goto _new_segment_var;
						}
					}
				}
				// ���õ�ǰ�εı䳤�г����ֽ���
				m_fragments[m_numFragments - 1].m_varlenBytes = colDef->m_lenBytes;
			}
		}

		if (cno == columns[colIdx])
			if (++colIdx >= numCols)
				break;
	}
	// cout << toString() << endl;
}


SubRecExtractorVR::~SubRecExtractorVR() {
	if (!m_ctx)
		delete [] m_fragments;
}

/**
 * ��ȡ�Ӽ�¼
 * @param record �������ͼ�¼
 * @param subRecord ���������Ӽ�¼
 */
void SubRecExtractorVR::extract(const Record *record, SubRecord *subRecord) {
	assert(record->m_format == REC_VARLEN);
	assert(m_tableDef->m_recFormat == REC_VARLEN || m_tableDef->m_recFormat == REC_COMPRESSED);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert(ColList(subRecord->m_numCols, subRecord->m_columns).isAsc());
	assert((uint)m_tableDef->m_maxRecSize <= subRecord->m_size);

	size_t bmBytes = m_tableDef->m_bmBytes;
	if (!BitmapOper::isZero(record->m_data, bmBytes)) {
		return RecordOper::fastExtractSubRecordVR(m_tableDef, record, subRecord);
	} else {
		byte *base = record->m_data; // ����ַ
		for (uint i = 0; i < m_numFragments; ++i) {
			size_t size = m_fragments[i].m_size;
			if (m_fragments[i].m_varlenBytes) { // �����䳤��
				size_t varColSize = (size_t)readU32Le(base + size, m_fragments[i].m_varlenBytes);
				varColSize += m_fragments[i].m_varlenBytes;
				if (m_fragments[i].m_srcStart != (u16) -1) { // �追��
					if (m_fragments[i].m_copyVarCol) {// �追���䳤��
						assert((uint)(m_fragments[i].m_dstStart + m_fragments[i].m_copySize + varColSize) <= (uint)subRecord->m_size);
						memcpy(subRecord->m_data + m_fragments[i].m_dstStart
							, base + m_fragments[i].m_srcStart, m_fragments[i].m_copySize + varColSize);
					} else {
						assert(m_fragments[i].m_dstStart + m_fragments[i].m_copySize <= (int)subRecord->m_size);
						memcpy(subRecord->m_data + m_fragments[i].m_dstStart
							, base + m_fragments[i].m_srcStart , m_fragments[i].m_copySize);
					}
				}
				size += varColSize;
			} else { // �������䳤��
				if (m_fragments[i].m_srcStart != (u16) -1) { // �追��
					assert(m_fragments[i].m_dstStart + m_fragments[i].m_copySize <= (int)subRecord->m_size);
					memcpy(subRecord->m_data + m_fragments[i].m_dstStart
						, base + m_fragments[i].m_srcStart , m_fragments[i].m_copySize);
				}
			}
			base += size;
		}
		subRecord->m_size = m_tableDef->m_maxRecSize;
		subRecord->m_rowId = record->m_rowId;
	}
}


/** ת��Ϊ�ַ��� */
string SubRecExtractorVR::toString() {
	stringstream ss;
	ss << "Count: " << m_numFragments << endl;
	for (uint i = 0; i < m_numFragments; ++i) {
		ss << "-------------------" << endl;
		ss << "size: " << m_fragments[i].m_size << endl;
		ss << "varlenBytes: " << m_fragments[i].m_varlenBytes << endl;
		ss << "srcStart: " << m_fragments[i].m_srcStart << endl;
		ss << "dstStart: " << m_fragments[i].m_dstStart << endl;
		ss << "copySize: " << m_fragments[i].m_copySize << endl;
		ss << "copyVarCol: " << m_fragments[i].m_copyVarCol << endl;
	}

	ss << endl;
	return ss.str();
}

	/**
	* ��ѹ����ʽ��¼����ȡ�����ʽ�Ӽ�¼����ȡ��
	* 
	*/
	class SubRecExtractorCR : public SubrecExtractor {
	public:
		SubRecExtractorCR(MemoryContext *ctx, const TableDef *tableDef
			, u16 numCols, const u16* columns, CmprssRecordExtractor *cprsRcdExtrator);
		virtual ~SubRecExtractorCR();

		virtual void extract(const Record *record, SubRecord *subRecord);

	private:
		const TableDef      *m_tableDef;	     /** ���� */
		MemoryContext       *m_ctx;		         /** �ڴ������� */
		SubRecExtractorVR   *m_subRecExtorVR;    /** �䳤��ʽ��¼�������ʽ�Ӽ�¼��ȡ�� */
		CmprssRecordExtractor *m_cprsRcdExtrator;  /** ѹ����¼��ȡ�� */
		CmprssColGrpExtractInfo *m_cmprsExtractInfo;
	};

	SubRecExtractorCR::SubRecExtractorCR(MemoryContext *ctx, const TableDef *tableDef, u16 numCols, 
		const u16* columns, CmprssRecordExtractor *cprsRcdExtrator) {
			m_tableDef = tableDef;
			m_ctx = ctx;
			m_cprsRcdExtrator = cprsRcdExtrator;

			if (ctx) {
				void *data = ctx->alloc(sizeof(SubRecExtractorVR));
				m_subRecExtorVR = new (data)SubRecExtractorVR(ctx, tableDef, numCols, columns);
				void *data2 = ctx->alloc(sizeof(CmprssColGrpExtractInfo));
				m_cmprsExtractInfo = new (data2)CmprssColGrpExtractInfo(tableDef, numCols, columns, ctx);
			} else {
				m_subRecExtorVR = new SubRecExtractorVR(ctx, tableDef, numCols, columns);
				m_cmprsExtractInfo = new CmprssColGrpExtractInfo(tableDef, numCols, columns);
			}
	}

	SubRecExtractorCR::~SubRecExtractorCR() {
		if (!m_ctx) {
			delete m_subRecExtorVR;
			delete m_cmprsExtractInfo;
		}
	}

	/**
	* ��ȡ�Ӽ�¼
	* @record ����ȡ�ļ�¼������ΪREC_COMPRESSED��ʽ��Ҳ������REC_VARLEN��ʽ
	* @subRecord �����ʽ�Ӽ�¼
	*/
	void SubRecExtractorCR::extract(const Record *record, SubRecord *subRecord) {
		assert(subRecord->m_format == REC_REDUNDANT);
		assert(ColList(subRecord->m_numCols, subRecord->m_columns).isAsc());
		assert((uint)m_tableDef->m_maxRecSize <= subRecord->m_size);

		assert(record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
		if (record->m_format == REC_COMPRESSED) {
			//�����¼��ѹ����,����Ҫ�Ƚ�ѹ��, ����ȡ�����ʽ�Ӽ�¼
			assert(NULL != m_cprsRcdExtrator);
			RecordOper::extractSubRecordCompressedR(m_ctx, m_cprsRcdExtrator, m_tableDef, record, 
				subRecord, m_cmprsExtractInfo);
		} else {
			//������һ����REC_VARLEN��ʽ�ģ����䳤��ʽ��ȡ
			m_subRecExtorVR->extract(record, subRecord);
		}
	}
 /* 
  *�䳤��¼�������Ӽ�¼��ȡ�����˻��汾)
  */
class DegSubRecExtractorFR: public SubrecExtractor {
public:
	DegSubRecExtractorFR(const TableDef *tableDef)
		: m_tableDef(tableDef) {

	}
	virtual void extract(const Record *record, SubRecord *subRecord) {
		return RecordOper::fastExtractSubRecordFR(m_tableDef, record, subRecord);
	}
private:
	const TableDef *m_tableDef; // ����
};

/**
 * �䳤��¼�������Ӽ�¼��ȡ�����˻��汾)
 */
class DegSubRecExtractorVR: public SubrecExtractor {
public:
	DegSubRecExtractorVR(const TableDef *tableDef)
		: m_tableDef(tableDef) {

	}
	virtual void extract(const Record *record, SubRecord *subRecord) {
		return RecordOper::fastExtractSubRecordVR(m_tableDef, record, subRecord);
	}
private:
	const TableDef *m_tableDef; // ����
};

/**
 * �����������Ӽ�¼��ȡ�����졣
 * ע: ʹ�ñ�������ȡ�Ӽ�¼���ܻᵼ��Ŀ���Ӽ�¼�зǱ���ȡ��¼������
 *
 * Ŀǰֻ֧������srcFmt -> dstFmt���
 * - REC_FIXLEN -> REC_REDUNDANT
 * - REC_VARLEN -> REC_REDUNDANT
 * @param ctx �ڴ������ģ������ΪNULL���ڴ���������
 * @param tableDef ���壬�ڲ������ã�������extractor֮��delete
 * @param numCols ����ȡ����
 * @param columns ����ȡ��
 * @param srcFmt ��¼��ʽ
 * @param dstFmt �Ӽ�¼��ʽ
 * @param extractCount Ԥ����ȡ����
 * @return �Ӽ�¼��ȡ��
 */
SubrecExtractor* SubrecExtractor::createInst(MemoryContext *ctx, const TableDef *tableDef, u16 numCols,
		const u16* columns, RecFormat srcFmt, RecFormat dstFmt, uint extractCount) {
	assert(ColList(numCols, (u16 *)columns).isAsc());
	const uint minExtractCount = 5;
	if (srcFmt == REC_FIXLEN && dstFmt == REC_REDUNDANT) {
		if (ctx) {
			if (extractCount < minExtractCount) {
				void *data = ctx->alloc(sizeof(DegSubRecExtractorFR));
				return new(data) DegSubRecExtractorFR(tableDef);
			} else {
				void *data = ctx->alloc(sizeof(SubRecExtractorFR));
				return new(data) SubRecExtractorFR(ctx, tableDef, numCols, columns);
			}
		} else {
			if (extractCount < minExtractCount) {
				return new DegSubRecExtractorFR(tableDef);
			} else {
				return new SubRecExtractorFR(ctx, tableDef, numCols, columns);
			}
		}
	} else if (srcFmt == REC_VARLEN && dstFmt == REC_REDUNDANT) {
		if (ctx) {
			if (extractCount < minExtractCount) {
				void *data = ctx->alloc(sizeof(DegSubRecExtractorVR));
				return new(data) DegSubRecExtractorVR(tableDef);
			} else {
				void *data = ctx->alloc(sizeof(SubRecExtractorVR));
				return new(data) SubRecExtractorVR(ctx, tableDef, numCols, columns);
			}
		} else {
			if (extractCount < minExtractCount) {
				return new DegSubRecExtractorVR(tableDef);
			} else {
				return new SubRecExtractorVR(ctx, tableDef, numCols, columns);
			}
		}
	} 
	assert(false);
	return 0;
}

/** 
 * ���캬ѹ����¼�ı���Ӽ�¼��ȡ��
 * @param ctx �ڴ������ģ������ΪNULL���ڴ���������
 * @param tableDef ���壬�ڲ������ã�������extractor֮��delete
 * @param numCols ����ȡ����
 * @param columns ����ȡ��
 * @param cprsRcdExtrator ѹ����ѹ����ȡ��
 */
SubrecExtractor* SubrecExtractor::createInst(MemoryContext *ctx, const TableDef *tableDef, u16 numCols,
		const u16* columns, CmprssRecordExtractor *cprsRcdExtrator) {
	assert(tableDef->m_recFormat != REC_FIXLEN);
	assert(cprsRcdExtrator);
	if (ctx) {
		void *data = ctx->alloc(sizeof(SubRecExtractorCR));
		return new(data) SubRecExtractorCR(ctx, tableDef, numCols, columns, cprsRcdExtrator);
	} else {
		return new SubRecExtractorCR(ctx, tableDef, numCols, columns, cprsRcdExtrator);
	}
}

/**
 * �����Ӽ�¼��ȡ��
 *
 * @param session �Ự
 * @param tableDef ���壬�ڲ������ã�������extractor֮��delete
 * @param dstRec ���ڴ洢��ȡ���ݵĲ��ּ�¼��ΪREC_REDUNDANT��ʽ
 * @param extractCount Ԥ����ȡ����
 * @param cprsRcdExtrator ѹ����ѹ����ȡ��
 * @return �Ӽ�¼��ȡ��
 */
SubrecExtractor* SubrecExtractor::createInst(Session *session, const TableDef *tableDef, const SubRecord *dstRec,
 	uint extractCount, CmprssRecordExtractor *cprsRcdExtrator/* = NULL */) {
	if (NULL != cprsRcdExtrator) {
		return createInst(session->getMemoryContext(), tableDef, dstRec->m_numCols, dstRec->m_columns,
			cprsRcdExtrator);
	} else {
		return createInst(session->getMemoryContext(), tableDef, dstRec->m_numCols, dstRec->m_columns,
			tableDef->m_recFormat == REC_FIXLEN ? REC_FIXLEN : REC_VARLEN, REC_REDUNDANT, extractCount);
	}
}

/** ѹ����ֵ��PAD��ʽ��ȡ�� */
class SubToSubExtractorCP: public SubToSubExtractor {
public:
	SubToSubExtractorCP(const TableDef *tableDef, const IndexDef *indexDef) {
		m_tableDef = tableDef;
		m_indexDef = indexDef;
	}

protected:
	const TableDef	*m_tableDef;	/** ���� */
	const IndexDef	*m_indexDef;	/** �������� */
};

/** ѹ����ֵ��PAD��ʽ��¼��ȡ��: �˻��棬֧�ֻ������ */
class DegSubToSubExtractorCP: public SubToSubExtractorCP {
public:
	DegSubToSubExtractorCP(const TableDef *tableDef, const IndexDef *indexDef): SubToSubExtractorCP(tableDef, indexDef) {}
	virtual void extract(const SubRecord *srSrc, SubRecord *srDest) {
		RecordOper::convertKeyCP(m_tableDef, m_indexDef, srSrc, srDest);
	}
};

/** ѹ����ֵ�������Ӽ�¼��ȡ�� */
class SubToSubExtractorCR: public SubToSubExtractor {
public:
	SubToSubExtractorCR(const TableDef *tableDef, const IndexDef *indexDef) {
		m_tableDef = tableDef;
		m_indexDef = indexDef;
	}

protected:
	const TableDef	*m_tableDef;	/** ���� */
	const IndexDef	*m_indexDef;	/** �������� */
};

/** ѹ����ֵ�������Ӽ�¼��ȡ��: �Ż��� */
class FastSubToSubExtractorCR: public SubToSubExtractorCR {
	/** �������Ե�һЩ���� */
	struct ColInfo {
		bool	m_isNeeded;		/** �Ƿ���Ҫ */
		bool	m_isCompressed;	/** �Ƿ���ѹ���� */
		bool	m_isVarlen;		/** �Ƿ�Ϊ�䳤 */
		ColumnDef	*m_colDef;	/** ���Զ��� */
	};
public:
	/** �ж��Ƿ���Խ����Ż�
	 *
	 * @param tableDef ����
	 * @param numColsSrc Դ�Ӽ�¼������
	 */
	static bool isSupported(const TableDef *tableDef, u16 numColsSrc) {
		// Ϊ�˿����ж����������Ƿ��NULL�����ھ������������������������8����һ���ƺ���
		if (numColsSrc > 8)
			return false;
		// Ϊ�˿�������Ŀ��Ŀ�ֵλͼ�������������������<64����¼��С��С��8�ֽ�
		if (tableDef->m_bmBytes > 8 || tableDef->m_maxRecSize < 8)
			return false;
		return true;
	}
	
	/** ����һ���߶��Ż���ѹ����ֵ�������Ӽ�¼��ȡ��
	 * @pre ֻ��isSupported����trueʱ����֧����һ�Ż�
	 *
	 * @param ctx �ڴ�������
	 * @param tableDef ���壬�ڲ������ã�������extractor֮��delete
	 * @param indexDef ��������
	 * @param numColsSrc Դ�Ӽ�¼������
	 * @param columnsSrc Դ�Ӽ�¼�����Ժţ��������غ󲻻�����
	 * @param numColsDst Ŀ���Ӽ�¼������
	 * @param columnsDst Ŀ���Ӽ�¼�����Ժţ��������غ󲻻�����
	 */
	FastSubToSubExtractorCR(MemoryContext *ctx, const TableDef *tableDef, const IndexDef *indexDef, u16 numColsSrc, const u16 *columnsSrc,
		u16 numColsDst, const u16 *columnsDst): SubToSubExtractorCR(tableDef, indexDef) {
		m_hasNull = false;
		m_colInfos = (ColInfo *)ctx->alloc(sizeof(ColInfo) * numColsSrc);
		for (u16 i = 0; i < numColsSrc; i++) {
			u16 cno = columnsSrc[i];
			m_colInfos[i].m_colDef = tableDef->m_columns[cno];
			if (tableDef->m_columns[cno]->m_nullable)
				m_hasNull = true;
			ColumnType type = tableDef->m_columns[cno]->m_type;
			m_colInfos[i].m_isNeeded = false;
			for (u16 j = 0; j < numColsDst; j++) {
				if (columnsDst[j] == cno) {
					m_colInfos[i].m_isNeeded = true;
					break;
				}
			}
			m_colInfos[i].m_isCompressed = isCompressableNumberType(type);
			m_colInfos[i].m_isVarlen = type == CT_VARCHAR || type == CT_VARBINARY;
		}
		m_bmBytes = m_hasNull? 1 : 0;
	}
	
	virtual void extract(const SubRecord *srSrc, SubRecord *srDst) {
		assert(srSrc->m_numCols < 8 && srDst->m_size >= 8 && m_tableDef->m_bmBytes <= 8);
		if (m_hasNull && *srSrc->m_data != 0) {	// ��Щ������NULL���������Ż�
			RecordOper::extractSubRecordCR(m_tableDef, m_indexDef, srSrc, srDst);
			return;
		}
		*((u64 *)srDst->m_data) = 0;	// ����Ŀ���ֵλͼ�������ǵ�һЩ����
		const byte *p = srSrc->m_data + m_bmBytes;
		for (u16 i = 0; i < srSrc->m_numCols; i++) {
			if (m_colInfos[i].m_isNeeded) {
				ColumnDef *col = m_colInfos[i].m_colDef;
				byte *pDst = srDst->m_data + col->m_offset;
				if (m_colInfos[i].m_isCompressed) {
					size_t compressedSize = NumberCompressor::sizeOfCompressed(p);
					NumberCompressor::decompress(p, compressedSize, pDst, col->m_size);
					p += compressedSize;
				} else if (m_colInfos[i].m_isVarlen) {
					byte lenBytes = col->m_lenBytes;
					u32 size = readU32Le(p, lenBytes) + lenBytes;
					memcpy(pDst, p, size);
					p += size;
				} else {
					u16 size = col->m_size;
					memcpy(pDst, p, size);
					p += size;
				}
			} else {
				if (m_colInfos[i].m_isCompressed)
					p += NumberCompressor::sizeOfCompressed(p);
				else if (m_colInfos[i].m_isVarlen) {
					byte lenBytes = m_colInfos[i].m_colDef->m_lenBytes;
					u32 size = readU32Le(p, lenBytes) + lenBytes;
					p += size;
				} else
					p += m_colInfos[i].m_colDef->m_size;
			}
		}
		srDst->m_rowId = srSrc->m_rowId;
	}

private:
	bool			m_hasNull;		/** �Ƿ�����ɿ����� */
	u16				m_bmBytes;		/** ��ֵλͼռ���ֽ��� */
	ColInfo			*m_colInfos;	/** ������Ҫ��ô���� */
};

/** ѹ����ֵ�������Ӽ�¼��ȡ��: �˻��棬֧�ֻ������ */
class DegSubToSubExtractorCR: public SubToSubExtractorCR {
public:
	DegSubToSubExtractorCR(const TableDef *tableDef, const IndexDef *indexDef): SubToSubExtractorCR(tableDef, indexDef) {}
	virtual void extract(const SubRecord *srSrc, SubRecord *srDest) {
		RecordOper::extractSubRecordCR(m_tableDef, m_indexDef, srSrc, srDest);
	}
};

class SubToSubExtractorCROrNR : public SubToSubExtractor {
public:
	SubToSubExtractorCROrNR(const TableDef *tableDef, const IndexDef *indexDef, SubToSubExtractor *extractorCR) 
		: m_tableDef(tableDef), m_indexDef(indexDef), m_extractorCR(extractorCR) {		
	}
	virtual void extract(const SubRecord *srSrc, SubRecord *srDst) {
		assert(REC_REDUNDANT == srDst->m_format);
		assert(KEY_COMPRESS == srSrc->m_format || KEY_NATURAL == srSrc->m_format);
		if (KEY_COMPRESS == srSrc->m_format) {
			m_extractorCR->extract(srSrc, srDst);
		} else {
			RecordOper::extractSubRecordNR(m_tableDef, m_indexDef, srSrc, srDst);
		}
	}
protected:
	const TableDef    *m_tableDef;
	const IndexDef	  *m_indexDef;
	SubToSubExtractor *m_extractorCR;
};

/** ��������������һ�����ʵ��Ӽ�¼���Ӽ�¼��ȡ��
 * ע: ʹ�ñ�������ȡ�Ӽ�¼���ܻᵼ��Ŀ���Ӽ�¼�зǱ���ȡ��¼������
 *
 * Ŀǰֻ֧������srcFmt -> dstFmt���
 * - KEY_COMPRESSED -> REC_REDUNDANT
 *
 * @param ctx �ڴ�������
 * @param tableDef ���壬�ڲ������ã�������extractor֮��delete
 * @param numColsSrc Դ�Ӽ�¼������
 * @param columnsSrc Դ�Ӽ�¼�����Ժţ��������غ󲻻�����
 * @param numColsDst Ŀ���Ӽ�¼������
 * @param columnsDst Ŀ���Ӽ�¼�����Ժţ��������غ󲻻�����
 * @param srcFmt Դ�Ӽ�¼��ʽ
 * @param dstFmt Ŀ���Ӽ�¼��ʽ
 * @param extractCount Ԥ����ȡ����
 * @return �Ӽ�¼��ȡ��
 */
SubToSubExtractor* SubToSubExtractor::createInst(MemoryContext *ctx, const TableDef *tableDef,
		const IndexDef *indexDef, u16 numColsSrc, const u16 *columnsSrc, u16 numColsDst, 
		const u16 *columnsDst, RecFormat srcFmt, RecFormat dstFmt, uint extractCount) {
	assert(ColList(numColsDst, (u16 *)columnsDst).isAsc());
	const uint minExtractCount = 5;
	if (srcFmt == KEY_COMPRESS && dstFmt == KEY_PAD) {
		// TODO: ����ֻ�ṩ��һ��subToSubExtractor�������Բο�CR��ʵ�����Ӹ��ٰ汾
		void *data = ctx->alloc(sizeof(DegSubToSubExtractorCP));
		return new (data)DegSubToSubExtractorCP(tableDef, indexDef);
	}
	assert(false);
	return NULL;
}

SubToSubExtractor* SubToSubExtractor::createInst(MemoryContext *ctx, const TableDef *tableDef, const IndexDef *indexDef,
		u16 numColsSrc, const u16 *columnsSrc, u16 numColsDst, const u16 *columnsDst,
		RecFormat dstFmt, uint extractCount) {
	UNREFERENCED_PARAMETER(dstFmt);
	SubToSubExtractor *extractor = SubToSubExtractor::createInst(ctx, tableDef, indexDef, numColsSrc, columnsSrc, 
		numColsDst, columnsDst, KEY_COMPRESS, REC_REDUNDANT, extractCount);

	void *data = ctx->alloc(sizeof(SubToSubExtractorCROrNR));
	return new (data)SubToSubExtractorCROrNR(tableDef, indexDef, extractor);
}

extern const char* getRecFormatStr( RecFormat format ) {
	switch (format) {
		case REC_REDUNDANT:
			return "REC_REDUNDANT";
		case REC_FIXLEN:
			return "REC_FIXLEN";
		case REC_VARLEN:
			return "REC_VARLEN";
		case REC_MYSQL:
			return "REC_MYSQL";
		case KEY_NATURAL:
			return "KEY_NATURAL";
		case KEY_COMPRESS:
			return "KEY_COMPRESS";
		case KEY_MYSQL:
			return "KEY_MYSQL";
		default:
			assert(format == KEY_PAD);
			return "KEY_PAD";
	}
}

}

