/**
 * 记录的表示和操作
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 * @author 李伟钊(liweizhao@corp.netease.com)
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
* 以小端方式把整形写入缓存
* @param value 待写入整形
* @param buf 输出缓存
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
/** 以小端方式读取整数 */
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
 * 列:
 *	维护列的信息
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
	ColumnDef		*m_columnDef;	/** 列定义       */
	byte			*m_buf;			/** 数据地址     */
	size_t			m_size;			/** 列大小       */
	size_t			m_capacity;		/** 数据缓存大小 */
	u16				m_colNo;		/** 列序号       */
	bool			m_isNull;		/** 是否为null   */
	byte			m_lenBytes;		/** 表示varchar/lob类型长度需要的字节数 */
};



class VSubRecordIterator;
/**
 * 行：
 *	管理记录的内存空间
 *	提供列遍历和操作方法
 */
class Row {
	friend class VSubRecordIterator;
public:
	Row() {}
	virtual ~Row() {}
	/**
	 * 构造一行
	 *
	 * @param tableDef	表定义
	 * @param buf		记录占用内存
	 * @param capacity  buf长度
	 * @param size		记录实际占用内存
	 */
	Row(const TableDef *tableDef, byte *buf, size_t capacity, size_t size) {
		init(tableDef, buf, capacity, size, tableDef->m_nullableCols, tableDef->m_recFormat);
	}
	/**
	 * 构造一行
	 *
	 * @param tableDef	表定义
	 * @param buf		记录占用内存
	 * @param capacity  buf长度
	 * @param size		记录实际占用内存
	 * @param format	记录格式
	 */
	Row(const TableDef *tableDef, byte *buf, size_t capacity, size_t size, RecFormat format) {
		init(tableDef, buf, capacity, size, tableDef->m_nullableCols, format);
	}
	/**
	 * 根据记录构造一行
	 *
	 * @param tableDef	表定义
	 * @param record	记录
	 */
	Row(const TableDef *tableDef, const Record *record) {
		init(tableDef, record->m_data, record->m_size, record->m_size
			, tableDef->m_nullableCols, record->m_format);
	}

	/** 获取第一列 */
	Column* firstColumn(Column *column) const {
		return firstColumn(column, 0);
	}
	/**
	 * 获取下一列
	 * @param cur 当前列
	 * @return 参数cur
	 */
	Column* nextColumn(Column* cur) const {
		++cur->m_colNo;
		return (cur->m_colNo >= m_tableDef->m_numCols) ? NULL : nextColumn(cur, cur->m_colNo);
	}

	/**
	 * 获取根据属性组排列的第一列
	 * @param column 用于存储输出列
	 * @param return 参数column
	 */
	Column* firstColAcdColGrp(Column *column) const {
		assert(column);
		assert(m_tableDef->m_colGrps);	
		return firstColumn(column, m_tableDef->m_colGrps[0]->m_colNos[0]);
	}

	/**
	 * 获取根据属性组排列的下一列
	 * @param cur 当前列
	 * @return 参数cur
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
	 * 加一列到行末尾
	 * @param column 待加入列
	 */
	virtual void appendColumn(const Column* column) {
		assert(column->def() == m_tableDef->m_columns[column->colNo()]);
		appendColumn(column->def()->m_nullBitmapOffset, column);
	}


	/** 获取记录大小 */
	inline size_t size() const {
		return m_size;
	}
	/** null 位图字节数 */
	inline size_t bitmapBytes() const {
		return m_bmBytes;
	}

protected:
	/**
	 * 初始化函数
	 *
	 * @param tableDef	表定义
	 * @param buf		记录占用内存
	 * @param capacity  buf长度
	 * @param size		记录实际占用内存
	 * @param nullableCols nullable列数目
	 * @param format 记录格式
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
		if (0 == m_size) { // 初始化为空记录
			// 预留位图存储空间
			m_size += m_bmBytes;
			// 初始化位图，避免valgrind报错
			memset(m_buf, 0, m_bmBytes);
		} else { // 非空记录
			// 至少保证位图的存储空间
			assert(m_size >= m_bmBytes);
		}
	}

	/**
	 * 初始化列
	 * @param column 待初始化列对象
	 * @param columnDef 列定义
	 * @param colNo 列号
	 * @param bmOffset null位图偏移
	 * @param buf 列数据起始地址
	 * @param size 列数据长度
	 * @param capacity 列实际占用内存空间
	 * @return 参数column
	 */
	inline Column* initColumn(Column* column, ColumnDef *columnDef,
		u16 colNo, bool isNull, byte* buf, byte lenBytes, size_t size, size_t capacity) const {
		// REC_REDUNDANT格式子记录和记录占用相同大小内存，其m_size没有实际作用
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
		// REC_REDUNDANT格式子记录和记录占用相同大小内存，其m_size没有实际作用
		assert(column->m_size + column->m_buf <= m_buf + (isRecFormat(m_format) || isUppMysqlFormat(m_format) ? m_capacity : m_size));
		return column;
	}

	/**
	 * 获取第一列,指定第一列的列号
	 *	当前行是一个SubRecord时，应用本函数
	 * @param column 待初始化列
	 * @param firstColNo 第一列列号
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
	 * 获取下一列，指定下一列的列号
	 *	当前行是一个SubRecord时，应用本函数
	 * @param cur 当前列
	 * @param nextColNo 下一列列号
	 * @return 参数cur
	 */
	Column* nextColumn(Column* cur, u16 nextColNo) const {
		assert(nextColNo < m_tableDef->m_numCols);
		bool isRedundat = isRedundantFormat(m_format);
		bool isUpperMysql = isUppMysqlFormat (m_format);
		byte *data = cur->data();
		data += isRedundat || isUpperMysql ? cur->capacity() : cur->size();
		ColumnDef *columnDef = m_tableDef->m_columns[nextColNo];
		if (data >= m_buf + m_capacity) { // 存储空间已到末尾，只允许NULL列
			if (nextColNo < m_tableDef->m_numCols) { // 剩下的列都是NULL
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
	 * 加一列到行末尾
	 * @param NULL位图偏移
	 * @param column 待加入列
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
		} else { // 内存不足
			assert(false);
		}
	}

	/**
	 * 获取列大小
	 * @param columnDef 列定义
	 * @param bmOffset NULL位图偏移
	 * @param buf	列数据起始地址
	 */
	inline size_t getColSize(ColumnDef *columnDef, u16 bmOffset, byte *buf) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		return ntse::getColSize(columnDef, buf);
	}
	/** 判断列是否为null */
	inline bool isColNull(u16 offset) const {
		return BitmapOper::isSet(m_buf, (uint)m_bmBytes << 3, offset);
	}
	/** 设置列为null */
	inline void setColNull(u16 offset) {
		BitmapOper::setBit(m_buf, (uint)m_bmBytes << 3, offset);
	}
	/** 设置列为非null */
	inline void setColNotNull(u16 offset) {
		BitmapOper::clearBit(m_buf, (uint)m_bmBytes << 3, offset);
	}

protected:
	const TableDef	*m_tableDef;	/** 表定义     */
	byte			*m_buf;			/** 数据缓存   */
	size_t			m_capacity;		/** 缓存大小   */
	size_t			m_size;			/** 实际大小   */
	size_t			m_bmBytes;		/** 位图字节数 */
	RecFormat		m_format;		/** 行格式     */
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
		} else if (m_lastColNoIdx == m_numCols) { // 已经到达最后一列
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

/** 索引键行 */
class KeyRow : public Row {
public:
	/** 用于遍历索引键(记录类型为KEY_PAD, KEY_COMPRESSED， KEY_NATURAL)中的列 */
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
			} else if (m_lastColNoIdx == m_numCols) { // 已经到达最后一列
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
	 * 加一列到行末尾
	 * @param column 待加入列
	 * @param prefixLen 列前缀长度
	 * @param lobPair 构建索引需要的大对象队列
	 */
	virtual void appendColumn(const Column* column, u32 prefixLen, LobPair *lobPair) {
		assert(!column->nullable() || m_curNullable < m_nullableCols);

		assert(column->colNo() < m_tableDef->m_numCols);
		u32 colDefSize = column->def()->m_size; // columnDef 中真正的大小，如果是用于拼装前缀列，即长度字节(1/2)加上prefixLen
		u32 columnSize = column->size();	//column真正的大小，如果是变长字段那需要计算column经过前缀提取后的真正大小,如果是定长字段，就是考虑到前缀的列最大长度 

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
					// 如果是Redundant格式转成Key格式，需要从大对象管理器中读取大对象数据
					curSize = lobPair->m_size;
					realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(lobPair->m_lob));
					colDefSize = prefixLen + lenBytes;
					columnSize  = lenBytes + realSize;
				} else {
					// 如果是Key格式之间的转换，不需要从大对象管理器
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
		} else { // 内存不足
			assert(false);
		}
		if (column->nullable())
			++m_curNullable;
	}
protected:
	/**
	 * 获取第一列,指定更多信息
	 *	当前行是一个KeyRow时，适用本函数
	 * @param column 待初始化列
	 * @param firstColNo 第一列列号
	 * @param bmOffset Null位图偏移
	 * @param prefixLen 前缀长度
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
	 * 获取下一列，指定更多信息
	 *	当前行是一个KeyRow时，应用本函数
	 * @param cur 当前列
	 * @param nextColNo 下一列列号
	 * @param bmOffset null位图偏移
	 * @param prefixLen 前缀长度
	 * @return 参数cur
	 */
	Column* nextColumn(Column* cur, u16 nextColNo, u16 bmOffset, u32 prefixLen) const {
		assert(nextColNo < m_tableDef->m_numCols);
		bool isRedundat = isRedundantFormat(m_format);
		byte *data = cur->data() + (isRedundat ?  cur->capacity() : cur->size());
		if (data >= m_buf + m_capacity) { // 存储空间已到末尾，只允许NULL列
			if (nextColNo < m_tableDef->m_numCols) { // 剩下的列都是NULL
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
	* 获取列大小
	* @param columnDef 列定义
	* @param bmOffset NULL位图偏移
	* @param buf	列数据起始地址
	* @param prefixLen 前缀长度
	*/
	virtual size_t getKeyColSize(ColumnDef *columnDef, u16 bmOffset, byte *buf, u32 prefixLen) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		return ntse::getKeyColSize(columnDef, buf, prefixLen);
	}

protected:
	const IndexDef	*m_indexDef;	/** 索引定义 */
	u16				m_nullableCols;	/** nullable列总数   */
	u16				m_curNullable;	/** 当前nullable数目 */
};




/**
 * 支持整形前缀压缩的行， 用于索引
 *	采用紧凑Null位图
 */
class CompressedKeyRow: public KeyRow {

public:
	CompressedKeyRow() {}

	/**
	 * 构造一个压缩行
	 * @param tableDef 表定义
	 * @param indexDef 索引定义
	 * @param key 索引子记录
	 */
	CompressedKeyRow(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord* key, bool empty = false)
		: KeyRow(tableDef, indexDef, key, empty) {
			assert(key->m_format == KEY_COMPRESS);
	}


	/**
	 * 加一列到行末尾
	 * @param column 待加入列
	 * @param prefixLen 前缀长度
	 * @param lobPair 构建索引键需要的大对象
	 */
	virtual void appendColumn(const Column* column, u32 prefixLen, LobPair *lobPair) {
		assert(column->colNo() < m_tableDef->m_numCols);
		// 空间一定要足够，如果没有前缀 m_capacity >= m_size + column->size()
		// 否则 m_capacity >= m_size >= m_size + prefixLen + lenBytes
		if (column->isNull()) {
			setColNull(m_curNullable);
		} else if (!isCompressableNumberType(column->def()->m_type)) { // 其他非整数类型
			if (column->nullable())
				setColNotNull(m_curNullable);
			size_t copySize = column->size();
			if (prefixLen > 0) { // 如果是前缀索引列
				assert(column->def()->m_type == CT_VARCHAR || column->def()->m_type == CT_VARBINARY || column->def()->m_type == CT_CHAR ||
					column->def()->m_type == CT_BINARY || column->def()->m_type == CT_SMALLLOB || column->def()->m_type == CT_MEDIUMLOB);

				size_t lenBytes = column->def()->m_lenBytes;
				size_t curSize = 0;
				size_t realSize = 0;

				if (column->def()->isLob()) {
					lenBytes = prefixLen > 255 ? 2 : 1;
					if (lobPair) {
						//如果是Redundant格式转换成KEY格式，需要从大对象管理器中读取大对象数据来填充
						curSize = lobPair->m_size;
						realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(lobPair->m_lob));
						copySize  = lenBytes + realSize;

						memcpy(m_buf + m_size + lenBytes, lobPair->m_lob, realSize);
						writeU32Le(realSize, m_buf + m_size, lenBytes);
					} else {
						//如果是KEY格式之间的转换，不需要从大对象管理器中读取大对象数据
						curSize = readU32Le(column->data(), lenBytes);
						realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(column->data() + lenBytes));
						copySize = realSize + lenBytes;
						memcpy(m_buf + m_size, column->data(), copySize);
						writeU32Le(realSize, m_buf + m_size, lenBytes);
					}
				} else {
					// 非大对象列，定长和变长
					// 对于定长字段来说，当前长度就等于列定义的长度
					// 对于变长字段来说，当前长度是记在记录头部的长度
					curSize = column->def()->isFixlen()? column->def()->m_size: readU32Le(column->data(), lenBytes);
					realSize = getRealSizeForPrefixIndex(column->def()->m_collation, prefixLen, curSize, (char*)(column->data() + lenBytes));
					copySize = column->def()->isFixlen()? prefixLen: realSize + lenBytes;
					u32 columnSize = column->def()->isFixlen()? prefixLen: realSize + lenBytes;
					memcpy(m_buf + m_size, column->data(), copySize);
					// 定长字段需要修改字段尾部为 0x20或者是0x0, 变长字段需要修改头部的长度信息
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
		} else { // 整数类型，压缩！
			if (column->nullable())
				setColNotNull(m_curNullable);
			assert(column->size() <= 8); // 整形最大长度为8
			size_t compSize = 0; // 压缩后字节数
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
	 * 解压列，调用者保证dest的内存
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
		} else { // 无需解压，直接用src内存
			dest->m_buf = src->m_buf;
			dest->m_size = src->m_size;
		}
		return dest;
	}

protected:
	/**
	 * 获取列大小
	 * @param columnDef 列定义
	 * @param bmOffset NULL位图偏移
	 * @param buf	列数据起始地址
	 * @param prefixLen 前缀长度
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
 * 随机访问行
 *	列能够随机定位
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
	 * 构造随机访问的子记录
	 */
	RandAccessRow(const TableDef *tableDef, const SubRecord *sr)
		:  m_isNewRow(false) {
		assert(sr->m_format == REC_FIXLEN || sr->m_format == REC_REDUNDANT || sr->m_format == REC_MYSQL);
		assert(sr->m_size == tableDef->m_maxRecSize);
		init(tableDef, sr->m_data, tableDef->m_maxRecSize, tableDef->m_maxRecSize, tableDef->m_nullableCols, sr->m_format);
	}

	/** 随机获取列 */
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
	 * 更新一列
	 * @param colNo	待更新列的列号
	 * @param srcCol 源列
	 * @return 更新是否成功
	 */
	inline virtual void writeColumn(u16 colNo, const Column* srcCol) {
		assert(colNo < m_tableDef->m_numCols);
		ColumnDef *colDef = m_tableDef->m_columns[colNo];
		assert(colDef->m_size >= srcCol->size());
		// 填充冗余格式未使用的内存，便于记录比较
		if (srcCol->m_isNull) {
			assert(colDef->m_nullable);
			setColNull(colDef->m_nullBitmapOffset);
		} else  {
			if (colDef->m_nullable)
				setColNotNull(colDef->m_nullBitmapOffset);
			memcpy(m_buf + colDef->m_offset, srcCol->m_buf, srcCol->size());

			// 如果srcCol是前缀索引键的某一列，并且是char/binary类型，需要将后续补充为0x20(char)或者0x0(binary)
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
	 * 加一列到行末尾
	 * @param column 待加入列
	 */
	virtual void appendColumn(const Column* column) {
		UNREFERENCED_PARAMETER(column);
		assert(false); // 不支持append
	}
	/** 
	 * 根据属性组定义获取第一个属性组的第一列
	 * 第一个属性组的第一列肯定是整个表的第一列(递增顺序)
	 * @param column
	 * @return Column
	 */
	 Column* firstColumnAcdGrp(Column *column) const {
		assert(m_tableDef->m_colGrps[0] != NULL && m_tableDef->m_numColGrps > 0);
		ColGroupDef * firstColGrp = m_tableDef->m_colGrps[0];
		assert(firstColGrp->m_numCols > 0);
		assert(firstColGrp->m_colGrpNo == 0);

		ColumnDef *columnDef = m_tableDef->m_columns[firstColGrp->m_colNos[0]];//第一个属性组的第一列的定义
		return columnAt(columnDef->m_no, column);
	 }

	/**
	 * 根据属性组定义获取下一列
	 * 访问的顺序肯定是先访问编号小的属性组，访问完该属性组的最后一个属性之后才访问下一个属性组
	 * @param cur 当前列
	 * @return 参数cur
	 */
	Column* nextColumnAcdGrp(Column* cur) const {
		assert(cur != NULL);
		u8 curGrpNo = cur->def()->m_colGrpNo;//当前列所属的属性组编号
		u16 nextColGrpOffset = cur->def()->m_colGrpOffset + 1;

		while (true) {
			if (curGrpNo >= m_tableDef->m_numColGrps)
				return NULL;
			else {
				if (nextColGrpOffset < m_tableDef->m_colGrps[curGrpNo]->m_numCols) {
					u16 nextColNo = m_tableDef->m_colGrps[curGrpNo]->m_colNos[nextColGrpOffset];
					assert(nextColNo < m_tableDef->m_numCols);
					//已经获得了下一列的列号，通过columnAt可
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
	 * 更新一列
	 * @param dstCol 待更新列， 必须是属于本行的列
	 * @param srcCol 源列
	 * @return 更新是否成功
	 */
	void writeColumn(const Column* dstCol, const Column* srcCol) {
		assert(dstCol->colNo() < m_tableDef->m_numCols);
		assert(dstCol->m_columnDef == m_tableDef->m_columns[dstCol->colNo()]); // 该列属于本行
		assert(dstCol->capacity() >= srcCol->capacity());
		// 填充冗余格式未使用的内存，便于记录比较
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
	// 是否是新创建的空行，空行的columnAt方法实现有所不同
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
		} else if (m_lastColNoIdx == m_numCols) { // 已经到达最后一列
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

/** 冗余格式记录格式 */
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
		if (empty) // 初始化位图，避免valgrind报错
			memset(m_buf, 0, bitmapBytes());
	}

};

/** MYSQL格式记录格式 */
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
	 * 获取列大小
	 * @param columnDef 列定义
	 * @param bmOffset NULL位图偏移
	 * @param buf	列数据起始地址
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

	/** 随机获取列 */
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
		// 如果是上层MYSQL格式并且是超长字段，记录长度字节数肯定是2	
		if (m_format == REC_UPPMYSQL && def->isLongVar())
			lenBytes = 2;
		size_t size = getColSize(def, def->m_nullBitmapOffset, data);
		initColumn(column, def, colNo, size == 0, data, lenBytes, size, capacity);
		
		return column;
	}

	/**
	 * 更新一列,从上层的列格式转换成引擎层的列格式（超长字段上层认为是VARCHAR， 引擎层认为是LOB）
	 * @param colNo	待更新列的列号
	 * @param srcCol 源列
	 */
	inline void writeColumnToEngineLayer(u16 colNo, const Column* srcCol) {
		assert(colNo < m_tableDef->m_numCols);
		assert(m_format == REC_MYSQL);
		ColumnDef *colDef = m_tableDef->m_columns[colNo];
		assert(colDef->m_mysqlSize >= srcCol->size());
		// 填充冗余格式未使用的内存，便于记录比较
		if (srcCol->m_isNull) {
			assert(colDef->m_nullable);
			setColNull(colDef->m_nullBitmapOffset);
		} else  {
			if (colDef->m_nullable)
				setColNotNull(colDef->m_nullBitmapOffset);
			
			// 如果srcCol是超长变长字段
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
	 * 更新一列,从引擎层的列格式转换成上层的列格式（超长字段上层认为是VARCHAR， 引擎层认为是LOB）
	 * @param colNo	待更新列的列号
	 * @param srcCol 源列
	 */
	inline void writeColumnToUpperLayer(u16 colNo, const Column* srcCol) {
		assert(colNo < m_tableDef->m_numCols);
		assert(m_format == REC_UPPMYSQL);
		ColumnDef *colDef = m_tableDef->m_columns[colNo];
		assert(colDef->m_size >= srcCol->size());
		// 填充冗余格式未使用的内存，便于记录比较
		if (srcCol->m_isNull) {
			assert(colDef->m_nullable);
			setColNull(colDef->m_nullBitmapOffset);
		} else  {
			if (colDef->m_nullable)
				setColNotNull(colDef->m_nullBitmapOffset);
			
			// 如果srcCol是超长变长字段
			if (colDef->isLongVar()) {
				// 先拷贝前缀
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


typedef Row CompressOrderRow;/** 压缩排序格式行，用于操作压缩排序格式记录的列 */
typedef Row	VarLenRow;
typedef RandAccessRow FixedLenRow;


/**
 * 压缩格式属性组
 * 用于操作压缩格式行中的压缩属性组
 */
class CompressedColGroup {
	friend class ColGroupRow;
	friend class CompressedRow;
public:
	/**
	 * 构造一个默认压缩属性组
	 */
	CompressedColGroup() : m_colGrpNo(0), m_colGrpDef(NULL), m_buf(NULL), m_size(0), m_lenBytes(0) {
	}
	/**
	 * 构造一个压缩属性组
	 * @param colGrpDef   属性组定义
	 * @param buf         压缩属性组数据地址(包含0~3字节的属性组长度)
	 * @param size        压缩属性组数据长度
	 * @param lenBytes    用于表示压缩属性组长度的字节数
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
	 * 获得压缩属性组实际数据起始地址(除表示属性组长度的字节)
	 * @return 
	 */
	inline byte * getRealData() const {
		return m_buf + m_lenBytes;
	}
	/**
	 * 获得压缩属性组实际数据长度(除表示属性组长度的字节)
	 * @return 
	 */
	inline size_t getRealSize() const {
		return m_size - m_lenBytes;
	}

private:
	u8                m_colGrpNo;       /** 属性组号 */
	const ColGroupDef *m_colGrpDef;     /** 属性组定义 */
	byte			  *m_buf;			/** 数据缓存 */
	size_t			  m_size;			/** 数据缓存大小 */
	u8                m_lenBytes;       /** 用于表示压缩数据大小的字节数 */
};

/**
 * 属性组行
 * 用于操作属性组中的列
 */
class ColGroupRow {
public:
	/**
	 * @param tableDef   所属表定义
	 * @param colGrpNo   属性组号
	 * @param buf        数据缓存
	 * @param capacity   数据缓存容量
	 * @param bmData     空值位图起始地址
	 * @param empty      是否是空行
	 */
	ColGroupRow(const TableDef *tableDef, u8 colGrpNo, byte *buf, size_t capacity, byte *bmData, bool empty = false):
	m_tableDef(tableDef), m_buf(buf), m_capacity(capacity), m_bmData(bmData), m_curColOffset(0) {
		assert(colGrpNo < tableDef->m_numColGrps);
		m_colGrpDef = tableDef->m_colGrps[colGrpNo];
		m_size = empty ? 0 : capacity;
	}

	/**
	 * 获取第一行
	 * @param cur 用于存储列数据的列指针
	 * @return 参数cur
	 */
	inline Column* firstColumn(Column* cur) const {
		assert(m_curColOffset == 0);
		ColumnDef *colDef = m_tableDef->m_columns[m_colGrpDef->m_colNos[m_curColOffset]];
		size_t size = getColSize(colDef, colDef->m_nullBitmapOffset, m_buf);
		return initColumn(cur, colDef, colDef->m_no, size == 0, m_buf, size, size);
	}

	/**
	* 获取下一列
	* @param cur 当前列
	* @return    参数cur
	*/
	inline Column* nextColumn(Column* cur) {
		++m_curColOffset;
		return (m_curColOffset >= m_colGrpDef->m_numCols) ? NULL : nextColumn(cur, m_curColOffset);
	}

	/**
	 * 加入一列到行末尾
	 * @param column 待加入列
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
		} else { // 内存不足
			assert(false);
		}
	}

	/**
	 * 获得数据实际大小
	 */
	inline size_t size() const {
		return m_size;
	}

protected:
	/**
	* 初始化列
	* @param column     待初始化列对象
	* @param columnDef  列定义
	* @param colNo      列号
	* @param isNull     是否为空
	* @param buf        列数据起始地址
	* @param size       列数据长度
	* @param capacity   列实际占用内存空间
	* @return           参数column
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
	 * 获取下一列
	 * @param column 当前列
	 * @param nextOffset 下一列在属性组中的下标
	 */
	inline Column* nextColumn(Column* column, u16 nextOffset) const {
		assert(nextOffset < m_colGrpDef->m_numCols);
		ColumnDef *colDef = m_tableDef->m_columns[m_colGrpDef->m_colNos[nextOffset]];
		byte *data = column->data() + column->size();
		if (data >= m_buf + m_size) {
			if (nextOffset < m_colGrpDef->m_numCols) { // 剩下的列都是NULL
				assert(isColNull(colDef->m_nullBitmapOffset));
			} else {
				return NULL;
			}
		}
		size_t size = getColSize(colDef, colDef->m_nullBitmapOffset, data);
		return initColumn(column, colDef, colDef->m_no, size == 0, data, size, size);
	}

	/**
	* 获取列大小
	* @param columnDef 列定义
	* @param bmOffset NULL位图偏移
	* @param data	列数据起始地址
	*/
	inline size_t getColSize(ColumnDef *columnDef, u16 bmOffset, byte *data) const {
		if (columnDef->m_nullable && isColNull(bmOffset))
			return 0;
		return ntse::getColSize(columnDef, data);
	}

	/** 判断列是否为null */
	inline bool isColNull(u16 offset) const {
		return BitmapOper::isSet(m_bmData, (uint)m_tableDef->m_bmBytes << 3, offset);
	}

	/** 设置列为null */
	inline void setColNull(u16 offset) {
		BitmapOper::setBit(m_bmData, (uint)m_tableDef->m_bmBytes << 3, offset);
	}
	/** 设置列为非null */
	inline void setColNotNull(u16 offset) {
		BitmapOper::clearBit(m_bmData, (uint)m_tableDef->m_bmBytes << 3, offset);
	}

private:
	const ColGroupDef *m_colGrpDef; /** 属性组定义 */
	const TableDef	*m_tableDef;	/** 表定义     */
	byte			*m_buf;			/** 数据缓存   */
	size_t          m_capacity;     /** 数据缓存大小 */
	size_t			m_size;			/** 实际大小   */
	byte            *m_bmData;      /** 空值位图起始地址 */
	u16             m_curColOffset; /** 当前操作的列在属性组中的下标，从0开始 */
};

/**
* 压缩行：
*	管理压缩记录的内存空间
*	提供压缩记录的属性组遍历和操作方法
*/
class CompressedRow {
public:
	/** 
	 * 构造一个压缩行
	 * @param tableDef 表定义
	 * @param buf      数据缓存
	 * @param capacity 数据缓存容量
	 * @param empty    是否是空行，如果是空行，则数据缓存内容未被填充
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
	 * 构造一个压缩行
	 * @param tableDef 所属表定义
	 * @param record   压缩记录，为REC_COMPRESSED格式
	 */
	CompressedRow(const TableDef *tableDef, const Record *record) : m_tableDef(tableDef), 
		m_buf(record->m_data), m_capacity(record->m_size), m_size(record->m_size) {
			assert(record->m_format == REC_COMPRESSED);
	}

	/**
	* 设置Null Bitmap
	* @param buf			Null Bitmap起始地址
	* @param nullBitmapSize	Null Bitmap长度
	*/
	inline void setNullBitmap(byte *buf, size_t nullBitmapSize) {
		memcpy(m_buf, buf, nullBitmapSize);
	}

	/**
	 * 获取第一个属性组
	 * @param cur 用于存放第一个属性组数据
	 * @return 参数cur
	 */
	inline CompressedColGroup* firstColGrp(CompressedColGroup *cur) const {
		const ColGroupDef *def = m_tableDef->m_colGrps[0];
		byte *data = m_buf + m_tableDef->m_bmBytes;
		u8 lenBytes;
		size_t colGrpSize = getColGrpSize(data, &lenBytes, m_size - m_tableDef->m_bmBytes);
		return initColGrp(cur, def->m_colGrpNo, def, data, colGrpSize, lenBytes);
	}

	/** 
	 * 获取下一个属性组
	 * @param cur 当前属性组
	 * @param 参数cur
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
	 * 加入一个属性组到当前行末尾
	 * @param cur 待加入属性组
	 */
	inline void appendColGrp(CompressedColGroup *cur) {
		assert(m_size + cur->size() <= m_capacity);
		memcpy(m_buf + m_size, cur->data(), cur->size());
		m_size += cur->size();
	}
	/**
	* 加入一个属性组到当前行末尾
	* @param realData 压缩属性组数据起始地址，不包含属性组长度信息
	* @param size 数据长度
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
	 * 将属性组数据进行压缩并追加到行末尾
	 * @param cprsRcdExtrator 压缩记录提取器
	 * @param src 数据地址
	 * @param offset 偏移量
	 * @param len 数据长度
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
	 * 获得当前行数据实际大小
	 */
	inline size_t size() const {
		return m_size;
	}

protected:
	/**
	 * 初始化一个属性组
	 * @param colGrp     外部分配的用于存放数据的空间的指针
	 * @param colGrpNo   属性组号
	 * @param colGrpDef  属性组定义
	 * @param data       属性组数据起始地址
	 * @param size       属性组数据长度
	 * @param lenBytes   用于表示属性组真实数据长度信息的字节数
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
	 * 获得属性组长度
	 * @param data     属性组数据起始地址
	 * @param lenBytes OUT，用于输出表示属性组长度的字节数
	 * @param size     属性组长度
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
	const TableDef	*m_tableDef;	/** 表定义     */
	byte			*m_buf;			/** 数据缓存   */
	size_t          m_capacity;     /*  数据缓存大小 */
	size_t			m_size;			/** 实际大小   */
};

/**
 * 压缩属性组列提取信息
 * 用于传递和计算哪些属性组需要解压缩，哪些列需要提取
 */
class CmprssColGrpExtractInfo {
public:
	/**
	 * 构造压缩属性组列提取信息
	 * @param tableDef 表定义
	 * @param numCols 要提取的列数
	 * @param columns 要提取的列的下标
	 * @param mtx     内存分配上下文
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
	MemoryContext  *m_mtx;                 /* 内存分配上下文 */
	const TableDef *m_tableDef;            /* 所属表定义 */
	byte           *m_decompressBuf;       /* 用于缓存解压缩数据的缓冲区 */
	size_t         m_decompressBufSize;    /* 分配的解压缩数据缓冲区大小 */
	u16            *m_colNumNeedReadInGrp; /* 属性组中需要提取的列数统计，如果数组元素大于0表示该属性组需要解压缩 */
	u8             *m_colNeedRead;         /* 需要提取的列，如果数组元素大于0表示该列需要提取 */
};

/**
 * 根据传入的字符串判断 前缀N个字符究竟占用多少字节
 * 用途: 前缀索引时提取前缀键值使用
 *
 * @param collation 字符集collation
 * @param prefixLen 前缀索引的最大长度（n character * mbmaxlen）
 * @param dataLen 待判断字符串字节数
 * @param str 待判断字符串
 *
 * @return 返回字符串前缀N个字符真正占用的字节数
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
 * 从一条冗余格式的记录中取出索引键。
 * 用途: 操作索引时构建索引键
 *
 * @param tableDef 记录所属的表定义
 * @param record 一条完整的记录，其m_format一定为REC_REDUNDANT
 * @param lobArray 记录中的大对象数据
 * @param key 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_COMPRESS
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
 * 将填充格式的索引键转化为压缩格式
 * 用途: SELECT索引扫描可以压缩后直接比较时
 *
 * @param tableDef 记录所属的表定义
 * @param indexDef 记录所属的索引定义
 * @param src 填充格式的索引键，其m_format一定为KEY_PAD
 * @param dest 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_COMPRESS
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
 * 将填充格式的索引键转化为自然格式
 *
 * @param tableDef 记录所属的表定义
 * @param indexDef 记录所属的索引定义
 * @param src 填充格式的索引键，其m_format一定为KEY_PAD
 * @param dest 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_NATURAL
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
 * 将MYSQL格式的索引键转化为填充格式
 *
 * @param tableDef 记录所属的表定义
 * @param indexDef 记录所属的索引定义
 * @param src MYSQL格式的索引键，其m_format一定为KEY_MYSQL
 * @param dest 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_PAD
 */
bool RecordOper::convertKeyMP(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest) {
	assert(src->m_format == KEY_MYSQL);
	assert(dest->m_format == KEY_PAD);

	size_t bmBytes = calcBitmapBytes(KEY_PAD, calcNullableCols(tableDef, src));
	size_t bmSize = (bmBytes << 3);
	size_t bmOffset = 0;
	bool bNullIncluded = false;

	memset(dest->m_data, 0 ,bmBytes); // 初始化位图
	// TODO: 为KEY_MYSQL定义类
	byte *srcPtr = src->m_data;
	byte *dstPtr = dest->m_data + bmBytes;
	for (u16 i = 0; i < src->m_numCols; ++i) {
		ColumnDef *columnDef = tableDef->m_columns[src->m_columns[i]];
		bool isFixLen = columnDef->isFixlen(); //由于是转成Index Key格式，大对象是变长的
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
		// KEY_MSQL中VARCHAR的长度总是用两个字节表示, VARCHAR字段需要特殊处理
		if (!isFixLen) {
			// 处理长度
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
			// 拷贝实际数据
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
 * 从一条冗余格式的记录中取出索引键。
 * 用途: MMS主键
 *
 * @param tableDef 记录所属的表定义
 * @param indexDef 记录所属的索引定义
 * @param record 一条完整的记录，其m_format一定为REC_REDUNDANT
 * @param key 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_NATURAL
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
 * 从一条定长格式的记录中取出索引键。
 * 用途:  在MMS模块redoUpdate时，需要根据Record获取主键信息
 *
 * @param tableDef 记录所属的表定义
 * @param indexDef 记录所属的索引定义
 * @param record 一条完整的记录，其m_format一定为REC_FIXLEN
 * @param key 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_NATURAL
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
 * 从一条变长格式的记录中取出索引键。
 * 用途:  在MMS模块redoUpdate时，需要根据Record获取主键信息
 *
 * @param tableDef 记录所属的表定义
 * @param indexDef 记录所属的索引定义
 * @param record 一条完整的记录，其m_format一定为REC_VARLEN
 * @param lobArray 构建索引键所需大对象队列
 * @param key 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_NATURAL
 */
void RecordOper::extractKeyVN(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key) {
	assert(record->m_format == REC_VARLEN);
	assert(record->m_size <= tableDef->m_maxRecSize);
	assert(key->m_format == KEY_NATURAL);

	VarLenRow vRow(tableDef, record);
	KeyRow row(tableDef, indexDef, key, true);
	// TODO：这里有一次内存分配
	vector<Column> cols(key->m_numCols); // 待提取列数组
	Column column; // 当前列
	// 获取所有待提取列信息
	for (Column* col = vRow.firstColumn(&column); col; col = vRow.nextColumn(col)) {
		for (uint i = 0; i < key->m_numCols; ++i) {
			if (col->colNo() == key->m_columns[i]) { // 找到一个匹配列
				cols[i] = *col;
				break;
			}
		}
	}
	// 构造KEY
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
 * 从一条冗余格式的记录中取出索引搜索键。
 * 用途: 执行INSERT...ON DUPLICATE KEY UPDATE/REPLACE语句时需要构造主键索引搜索键
 *
 * @param tableDef 记录所属的表定义
 * @param indexDef 记录所属的索引定义
 * @param record 一条完整的记录，其m_format一定为REC_REDUNDANT
 * @param lobArray 构建索引键所需大对象队列
 * @param key 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置key->m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为KEY_PAD
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
 * 功能与RecordOper::extractKeyRP完全一致，唯一的区别在于，返回值中标识出提取的索引搜索键是否包含NULL值
 *
 * @return 若提取出的搜索键包含NULL值，则返回true；否则返回false
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
 * 从一条定长格式的记录中取出部分属性的值，存储为冗余格式。
 * 用途: 从定长堆或定长记录表的MMS记录中取查询所需属性返回给MySQL
 *
 * @param tableDef 记录所属的表定义
 * @param record 一条完整的记录，其m_format一定为REC_FIXLEN
 * @param subRecord 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置subRecord.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_REDUNDANT
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

/** 为索引键增加表ID以及索引ID内容，用于checkDuplicateKey */
void RecordOper::appendKeyTblIdAndIdxId(SubRecord *key, TableId tblId, u8 idxNo) {
	assert(key->m_format == KEY_NATURAL);
	uint size = key->m_size;
	*(u16*)(key->m_data + size) = tblId;
	size += sizeof(u16);
	*(u8*)(key->m_data + size) = idxNo;
	size += sizeof(u8);
	key->m_size = size;
}


/** 高效的extractSubRecordFR */
void RecordOper::fastExtractSubRecordFR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	assert(record->m_format == REC_FIXLEN);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert(tableDef->m_recFormat == REC_FIXLEN);
	assert(tableDef->m_maxRecSize == record->m_size);
	assert(tableDef->m_maxRecSize <= subRecord->m_size);

	size_t bmBytes = tableDef->m_bmBytes;
	memset(subRecord->m_data, 0, bmBytes); // 初始化位图
	size_t bmSize = (bmBytes << 3);
	assert(subRecord->m_size > bmBytes);

	for (u16 i = 0; i < subRecord->m_numCols; ++i) {
		u16 colNo = subRecord->m_columns[i];
		ColumnDef *colDef = tableDef->m_columns[colNo];
		if (colDef->m_nullable) {
			if (BitmapOper::isSet(record->m_data, bmSize, colDef->m_nullBitmapOffset)) {
				BitmapOper::setBit(subRecord->m_data, bmSize, colDef->m_nullBitmapOffset);
				// 填充冗余格式未使用的内存，便于记录比较
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
 * 从一条变长格式的记录中取出部分属性的值，存储为冗余格式。
 * 用途: 从变长堆或变长记录表的MMS记录中取查询所需属性返回给MySQL
 *
 * @param tableDef 记录所属的表定义
 * @param record 一条完整的记录，其m_format一定为REC_VARLEN
 * @param subRecord 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
 *   调用者必须为保存输出内容分配足够多的内存，并且通过设置subRecord.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_REDUNDANT
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
		if (col->colNo() == subRecord->m_columns[idx]) { // 找到匹配列
			mRow.writeColumn(col->colNo(), col);
			if (++idx >= subRecord->m_numCols)
				break;
		}
	}
	assert(tableDef->m_maxRecSize == (int)mRow.size());
	subRecord->m_size = (uint)mRow.size();
	subRecord->m_rowId = record->m_rowId;
}

/** 高效的extractSubRecordVR */
void RecordOper::fastExtractSubRecordVR(const TableDef *tableDef, const Record *record, SubRecord *subRecord) {
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert(ColList(subRecord->m_numCols, subRecord->m_columns).isAsc());
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);

	size_t bmBytes = tableDef->m_bmBytes;
	memset(subRecord->m_data, 0, bmBytes); // 初始化位图
	size_t bmSize = (bmBytes << 3);
	byte *srcPtr = record->m_data + bmBytes; // 源指针
	byte *srcEnd = record->m_data + record->m_size; // 尾指针
	u16 srcColNo = 0; // 源列号
	uint dstColIdx = 0; // 目的列索引
	while (srcPtr < srcEnd) {
		ColumnDef *colDef = tableDef->m_columns[srcColNo];
		size_t srcColSize; // 列数据长度

		if (srcColNo == subRecord->m_columns[dstColIdx]) { // 匹配列
			// 填充冗余格式未使用的内存，便于记录比较
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
				break; // 已经处理完成所有待提取列
		} else { // 非匹配列
			srcColSize = (colDef->m_nullable
				&& BitmapOper::isSet(record->m_data, bmSize, colDef->m_nullBitmapOffset))
				? 0 : getColSize(colDef, srcPtr);
		}
		srcPtr += srcColSize;
		++srcColNo;
	}
	// 处理剩余列, 剩下的都是NULL列
	for (; dstColIdx < subRecord->m_numCols; ++dstColIdx) {
		ColumnDef *colDef = tableDef->m_columns[subRecord->m_columns[dstColIdx]];
		assert(colDef->m_nullable);
		BitmapOper::setBit(subRecord->m_data, bmSize, colDef->m_nullBitmapOffset);
	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = record->m_rowId;
}
/**
* 从一个由完整索引键值生成部分索引键值，存储为冗余格式。
* 用途: 在索引扫描的时候，索引需要返回上层指定某些属性的记录而不是对应的整条索引记录
*
* @param tableDef 记录所属的表定义
* @param indexDef 记录所属的索引定义
* @param record 一条完整的记录，其m_format一定为KEY_COMPRESS
* @param subRecord 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
*   调用者必须为保存输出内容分配足够多的内存，并且通过设置subRecord.m_size告知
*   已经分配的内存大小，防止越界。其m_format一定为REC_REDUNDANT
*/
void RecordOper::extractSubRecordCR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord) {
	assert(key->m_format == KEY_COMPRESS);
	assert(subRecord->m_format == REC_REDUNDANT);
	assert((uint)tableDef->m_maxRecSize <= subRecord->m_size);
	assert(!indexDef->hasLob()); //索引里不能含有大对象，否则没法转换

	RedRow dstRow(tableDef, subRecord, true);
	CompressedKeyRow cRow(tableDef, indexDef, key);
	KeyRow::Iterator ki(&cRow, key);
	byte tmpBuf[8];
	Column tmpCol;
	for (ki.first(); !ki.end(); ki.next()) {
		Column *column = ki.column();
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // 查找匹配列
			if (subRecord->m_columns[i] == column->colNo()) { // 找到一个匹配列
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
	assert(!indexDef->hasLob()); //索引里不能含有大对象，否则没法转换


	RedRow dstRow(tableDef, subRecord, true);
	KeyRow row(tableDef, indexDef, key);
	KeyRow::Iterator ki(&row, key);
	for (ki.first(); !ki.end(); ki.next()) {
		Column *column = ki.column();
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // 查找匹配列
			if (subRecord->m_columns[i] == column->colNo()) { // 找到一个匹配列
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
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // 查找匹配列
			if (subRecord->m_columns[i] == column->colNo()) { // 找到一个匹配列
				dstRow.writeColumn(column->colNo(), column);
				break;
			}
		}

	}
	subRecord->m_size = tableDef->m_maxRecSize;
	subRecord->m_rowId = key->m_rowId;
}

/**
* 从一个由完整索引键值生成部分子记录键值，存储为冗余格式，其中过滤掉大对象列。
* 用途: TNT初始化设定Auto_increment列时用于扫描外存索引获取最大项值
*
* @param tableDef 记录所属的表定义
* @param indexDef 记录所属的索引定义
* @param record 一条完整的记录，其m_format一定为KEY_COMPRESS
* @param subRecord 输入输出参数。输入表示要取出的属性，输出为属性值及占用空间大小
*   调用者必须为保存输出内容分配足够多的内存，并且通过设置subRecord.m_size告知
*   已经分配的内存大小，防止越界。其m_format一定为REC_REDUNDANT
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
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // 查找匹配列
			if (subRecord->m_columns[i] == column->colNo() && !column->def()->isLob()) { // 找到一个匹配的非大对象列
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
		for (u16 i = 0; i < subRecord->m_numCols; ++i) { // 查找匹配列
			if (subRecord->m_columns[i] == column->colNo() && !column->def()->isLob() ) { // 找到一个匹配列
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
* 从一个以REC_MYSQL方式存储的记录中提取出大对象内容。
* 用途: NTSE恢复时索引做logic_redo时使用
*
* @param tableDef 记录所属的表定义
* @param indexDef 记录所属的索引定义
* @param subRec 子记录，其m_columns中的大对象列为REC_MYSQL存储方式
* @param lobArray 输入输出参数。输入表示要取出的大对象内容
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
 * 将MYSQL格式的记录从上层格式转化成引擎层格式
 * 用途: 含有超长字段的记录插入
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_MYSQL
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_MYSQL
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
 * 将MYSQL格式的记录转从引擎层格式转为上层格式
 * 用途: 含有超长字段的记录插入
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_MYSQL
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_UPPMYSQL
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
 * 将MYSQL格式的子记录从上层格式转化成引擎层格式
 * 用途: 含有超长字段的记录插入
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_MYSQL
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_MYSQL
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
 * 将MYSQL格式的子记录转从引擎层格式转为上层格式
 * 用途: 含有超长字段的记录插入
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_MYSQL
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_UPPMYSQL
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
 * 将冗余格式的记录转化为变长格式的记录
 * 用途: 插入记录
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_REDUNDANT
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_VARLEN
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
 * 将变长格式的记录转化为冗余格林的记录
 * 用途: 恢复及在线数据库维护操作
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_VARLEN
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_REDUNDANT
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

/** 将变长或者定长记录转化为冗余格式
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_VARLEN或者REC_FIXLEN
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_REDUNDANT
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
 * 将冗余格式的子记录转化为变长格式的子记录
 * 用途: MMS写日志时需要转化冗余格式为变长格式子记录以节省空间
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_REDUNDANT
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为REC_VARLEN
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
 * 将变长格式的子记录转化为冗余格式的子记录
 * 用途: MMS重做日志时需要转化变长格式子记录为冗余格式子记录
 *
 * @param tableDef 记录所属的表定义
 * @param src 一条完整的记录，其m_format一定为REC_VARLEN
 * @param dest 输出参数。调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
 *   已经分配的内存大小，防止越界。其m_format一定为RECREDUNDANT
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
 * 更新一条冗余格式的记录。总是进行本地更新
 *
 * @param tableDef 记录所属的表定义
 * @param record 要更新的记录，其m_format一定为REC_REDUNDANT
 * @param update 要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
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
 * 更新一条定长记录。总是进行本地更新
 *
 * @param tableDef 记录所属的表定义
 * @param record 要更新的记录，其m_format一定为REC_FIXLEN
 * @param update 要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
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
 * 得到记录更新后的大小
 *
 * @param tableDef 记录所属的表定义
 * @param record 要更新的记录，其m_format一定为REC_VARLEN
 * @param update 要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
 */
u16 RecordOper::getUpdateSizeVR(const TableDef *tableDef, const Record *record, const SubRecord *update) {
	assert(update->m_format == REC_REDUNDANT);
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert((uint)tableDef->m_maxRecSize == update->m_size);
	RedRow mRow(tableDef, update); // 更新信息
	VarLenRow oldRow(tableDef, record);

	int deltaSize = 0;
	uint idx = 0;
	Column column, column1;
	for (Column* col = oldRow.firstColumn(&column); col; col = oldRow.nextColumn(col)) {
		if (col->colNo() == update->m_columns[idx]) { // 找到一个匹配列
			Column* updatedCol = mRow.columnAt(col->colNo(), &column1);
			deltaSize += ((int)updatedCol->size() - (int)col->size());
			if (++idx >= update->m_numCols)
				break;
		}
	}

	return (u16)((int)oldRow.size() + deltaSize);
}

/**
 * 获得记录更新后的大小(更新后的记录不进行压缩)
 * @pre 表必须含有字典，即记录前像可能是压缩格式也可能是变长格式
 * @param mtx      内存分配上下文
 * @param tableDef 表定义
 * @param cprsRcdExtrator 压缩记录提取器
 * @param oldRcd 记录前像，可能是压缩格式也可能是变长格式
 * @param update 更新后像，冗余格式
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
		byte *tmpDecompressBuf = (byte *)mtx->alloc(tableDef->m_maxRecSize);//用于缓存解压缩后的数据
		
		RedRow mRow(tableDef, update);
		CompressedRow oldRow(tableDef, oldRcd);
		CompressedColGroup cmprsColGrp;
		int oldRecUncompressSize = tableDef->m_bmBytes;//原记录解压缩后的大小
		int deltaSize = 0;

		for (CompressedColGroup *colGrp = oldRow.firstColGrp(&cmprsColGrp); colGrp; 
			colGrp = oldRow.nextColGrp(colGrp)) {
				u8 colGrpNo = colGrp->colGrpNo();
				if (cmpressExtractInfo.m_colNumNeedReadInGrp[colGrpNo] > 0) {
					//如果这个压缩属性组需要更新,先解压,更新后再压缩
					uint decompressSize = 0;//属性组解压缩之后的长度
					cprsRcdExtrator->decompressColGroup(colGrp->data(), (uint)colGrp->lenBytes(), 
						colGrp->getRealSize(), tmpDecompressBuf, &decompressSize);
					assert(decompressSize <= tableDef->m_maxRecSize);
					oldRecUncompressSize += (int)decompressSize;

					//计算前像与后像的长度delta
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
 * 本地更新变长记录
 *
 * @param tableDef 记录所属的表定义
 * @param record 要更新的记录，其m_format一定为REC_VARLEN
 * @param update 要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
 * @param oldBufferSize record.m_data能够使用的最大内存大小
 */
void RecordOper::updateRecordVRInPlace(const TableDef *tableDef, Record *record,
									   const SubRecord *update, size_t oldBufferSize) {
	return doUpdateRecordVRInPlace(tableDef, record, update, oldBufferSize);
}


/**
 * 非本地更新变长记录
 *
 * @param tableDef 记录所属的表定义
 * @param record 要更新的记录，其m_format一定为REC_VARLEN
 * @param update 要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
 * @param newBuf 存储新记录的内存
 * @return 更新之后的记录大小
 */
uint RecordOper::updateRecordVR(const TableDef *tableDef, const Record *record, const SubRecord *update,
								byte *newBuf) {
	assert(update->m_format == REC_REDUNDANT);
	assert(record->m_format == REC_VARLEN);
	assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
	assert((uint)tableDef->m_maxRecSize == update->m_size);
	RedRow mRow(tableDef, update); // 更新信息
	VarLenRow oldRow(tableDef, record);
	// 我们认为新创建行具有无限空间，调用者必须保证newBuf空间足够
	VarLenRow newRow(tableDef, newBuf, (size_t)-1, 0);
	uint idx = 0;
	Column column, column1;
	for (Column* col = oldRow.firstColumn(&column);	col; col = oldRow.nextColumn(col)) {
		if (idx < update->m_numCols && col->colNo() == update->m_columns[idx]){ // 找到一个匹配列
			newRow.appendColumn(mRow.columnAt(col->colNo(), &column1));
			++idx;
		} else {
			newRow.appendColumn(col);
		}
	}
	return newRow.size();
}


/**
 * 判断指定的两个压缩格式的键值能否进行快速的大小比较
 *
 * @param tableDef 键值所属的表
 * @param indexDef 索引定义
 * @param numKeyCols 搜索键值中包含的属性数
 * @param keyCols 键值中每个属性为表中的第几个属性
 * @return 能进行快速大小比较时返回true，否则返回false
 */
bool RecordOper::isFastCCComparable(const TableDef *tableDef, const IndexDef *indexDef, u16 numKeyCols, const u16 *keyCols) {
	if (numKeyCols != indexDef->m_numCols)
		return false;
	if (indexDef->m_bmBytes > 1)	// 为提高比较时的性能，只处理常见的可为NULL的属性不超过8个的情况
		return false;

	for (u16 i = 0; i < numKeyCols; ++i) {
		assert(keyCols[i] < tableDef->m_numCols);
		ColumnDef* columnDef = tableDef->m_columns[keyCols[i]];
		if (columnDef->m_type != CT_SMALLINT
			&& columnDef->m_type != CT_MEDIUMINT
			&& columnDef->m_type != CT_INT
			&& columnDef->m_type != CT_BIGINT
			&& columnDef->m_type != CT_RID
			&& columnDef->m_type != CT_BINARY)	// CHAR和VARCHAR不能快速比较（即使collation为COLL_BIN)
			return false; 						// TINYINT没有被压缩，因此不能memcmp
		if (columnDef->m_prtype.isUnsigned())
			return false;
	}
	return true;
}

/**
 * 比较两个压缩格式的搜索键
 * 用途: 当索引搜索条件中只包含整数等可以直接memcmp比较的类型时，使用这一函数比较
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_COMPRESS
 * @param key2 参与比较者2，其m_format一定为KEY_COMPRESS
 * @param indexDef 比较键值所属的索引定义
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
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
 * 比较一个冗余格式与另一个压缩格式的搜索键
 * 用途: 索引搜索，不能进行memcmp比较且表为定长记录时用这一函数
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为REC_REDUNDANT，为搜索键
 * @param key2 参与比较者2，其m_format一定为KEY_COMPRESS
 * @param indexDef 比较键值所属的索引定义，为了提供与compareKeyCC一致的接口而增加，对compareKeyRC来说这一参数无用
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
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
	u16 count = 0; // 已比较列数
	for (cRowIter.first(); result == 0 && !cRowIter.end() && count < key1->m_numCols; cRowIter.next()) {
		Column* col2 = cRowIter.column();
		u16 colNo = col2->colNo();
		mRow.columnAt(colNo, &column1);
		result = compareColumn(&column1, col2, true);
		++ count;
	}
	return result;
}

/**	比较索引键值，与索引对应的堆记录
*	用途：TNT引擎，做double check之用
*	@tableDef	表定义
*	@key1		索引键值
*	@key2		堆记录
*	@indexDef	索引定义
*	@return 返回-1/0/1(小于，等于，大于)
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

	// 只比较索引键值列
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

/**	比较KEY_PAD格式索引键与冗余格式索引键
*	@tableDef	表定义
*	@key1		索引键值
*	@key2		堆记录
*	@indexDef	索引定义
*	@return 返回-1/0/1(小于，等于，大于)
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
 * 比较一个填充格式与另一个压缩格式的搜索键
 * 用途: SELECT索引扫描，不能压缩后直接比较的
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_PAD，为搜索键
 * @param key2 参与比较者2，其m_format一定为KEY_COMPRESS
 * @param indexDef 比较键值所属的索引定义，为了提供与compareKeyCC一致的接口而增加，对本来说这一参数无用
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
 */
int RecordOper::compareKeyPC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	assert(KEY_PAD == key1->m_format);
	assert(KEY_COMPRESS == key2->m_format);
	return compareKeyPCOrNC(tableDef, key1, key2, indexDef);
}

/**
 * 比较自然格式与压缩格式的搜索键
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_NATURAL，为搜索键
 * @param key2 参与比较者2，其m_format一定为KEY_COMPRESS
 * @param indexDef 比较键值所属的索引定义，为了提供与compareKeyCC一致的接口而增加，对本接口来说这一参数无用
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
 */
int RecordOper::compareKeyNC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	assert(KEY_NATURAL == key1->m_format);
	assert(KEY_COMPRESS == key2->m_format);
	return compareKeyPCOrNC(tableDef, key1, key2, indexDef);
}

/**
 * 比较PAD格式(或自然格式)与压缩格式的搜索键
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_PAD或KEY_NATURAL，为搜索键
 * @param key2 参与比较者2，其m_format一定为KEY_COMPRESS
 * @param indexDef 比较键值所属的索引定义，为了提供与compareKeyCC一致的接口而增加，对本接口来说这一参数无用
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
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
 * 比较自然格式与PAD格式的搜索键
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_NATURE，为搜索键
 * @param key2 参与比较者2，其m_format一定为KEY_PAD
 * @param indexDef 比较键值所属的索引定义，为了提供与compareKeyCC一致的接口而增加，对本接口来说这一参数无用
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
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
 * 比较两个自然格式（非压缩）的搜索键
 * 用途: 创建索引时比较
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为REC_NATURAL
 * @param key2 参与比较者2，其m_format一定为REC_NATURAL
 * @param indexDef 比较键值所属的索引定义，为了提供与compareKeyCC一致的接口而增加，对compareKeyNN来说这一参数无用
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
 */
int RecordOper::compareKeyNN(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef) {
	UNREFERENCED_PARAMETER(indexDef);
	
	assert(key1->m_format == KEY_NATURAL);
	assert(key2->m_format == KEY_NATURAL);

	return realCompareKeyNNorPP(tableDef, key1, key2, indexDef);
}


/**
 * 比较两个PAD格式（非压缩）的搜索键
 * 用途: Key-Value接口种multi-get对索引键值排序时使用
 *
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_PAD
 * @param key2 参与比较者2，其m_format一定为KEY_PAD
 * @param indexDef 比较键值所属的索引定义，为了提供与compareKeyCC一致的接口而增加，对compareKeyNN来说这一参数无用
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
 */
int RecordOper::compareKeyPP( const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef /*= NULL*/ ) {
	UNREFERENCED_PARAMETER(indexDef);

	assert(key1->m_format == KEY_PAD);
	assert(key2->m_format == KEY_PAD);

	return realCompareKeyNNorPP(tableDef, key1, key2, indexDef);
}

/** NN和PP格式索引键值比较的真正实现
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_PAD
 * @param key2 参与比较者2，其m_format一定为KEY_PAD
 * @param indexDef 所属索引定义
 * @return key1 = key2时返回0，key1 < key2时返回<0，key1 > key2时返回>0
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

/** NN和PP格式索引键值比较每个属性的长度
 * @param tableDef 所属表定义
 * @param key1 参与比较者1，其m_format一定为KEY_PAD 或者 Key_NATURE
 * @param key2 参与比较者2，其m_format一定为KEY_PAD
 * @param indexDef 所属索引定义
 * @return 所有字段key1 key2 size相等时返回0，否则返回非0
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
 * 比较两个相同格式子记录是否相等
 *
 * @param tableDef 所属表定义
 * @param sb1 参与比较者1
 * @param sb2 参与比较者2
 * @param indexDef 所属索引定义
 * @return sb1 = sb2时返回true, 否则返回false
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
 * 比较两个相同格式记录是否相等
 *
 * @param tableDef 所属表定义
 * @param sb1 参与比较者1
 * @param sb2 参与比较者2
 * @return sb1 = sb2时返回true, 否则返回false
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
 * 压缩一个搜索键
 * 用途: 创建索引时，将原来用自然格式存储的搜索键压缩插入到索引中
 *
 * @param tableDef 所属表定义
 * @param indexDef 所属索引定义
 * @param src 原搜索键，其m_format一定为KEY_NATURAL
 * @param dest 压缩后的搜索键，其m_format一定为KEY_COMPRESS
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
 * 将newSr的内容合并到oldSr当中，最终结果保存在newSr
 * @param tableDef 表定义
 * @param newSr 需要更新的各属性的新值
 * @param oldSr 所有旧属性值
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
		// 找到newSr中可能匹配的列
		while (j < newSr->m_numCols && newSr->m_columns[j] < oldSr->m_columns[i])
			++j;
		if (j >= newSr->m_numCols || oldSr->m_columns[i] != newSr->m_columns[j])
			// new中没有该列，则从old中copy
			newRow.writeColumn(oldSr->m_columns[i], oldRow.columnAt(oldSr->m_columns[i], &column));
	}
}

/**
* 将冗余格式的记录转化为压缩排序格式的记录
*
* 只是根据属性组调整顺序及处理为NULL的属性，用于压缩前预处理
*
* @param tableDef 记录所属的表定义
* @param src 要转化的记录，一定为REC_REDUNDANT格式
* @param dest OUT 保存转换后的REC_COMPRESSORDER格式的记录，调用方保证有足够内存保存输出结果
*                 如果dest->m_segSizes为NULL，则不会填充各个属性组起始数据的偏移量
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
* 将压缩排序格式的记录转化为冗余格式的记录
* 用于解压缩后根据属性组的定义还原为冗余格式的记录
* @param tableDef 所属的表定义
* @param src 要转换的压缩排序格式的记录，为REC_COMPRESSORDER格式
* @param dest OUT，保存转换后的冗余格式的记录，一定为REC_REDUNDANT格式，调用方保证有足够的内存
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
 * 将压缩排序格式的记录转化为变长格式的记录 
 * @param ctx 内存分配上下文
 * @param table 所属的表定义
 * @param src 要转换的压缩排序格式的记录，为REC_COMPRESSORDER格式
 * @param dest INOUT 保存转换后的变长格式的记录，一定为REC_VAR格式，调用方保证有足够的内存并通过dest->m_size告之
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
		idIncCol[col->colNo()] = *col;//浅拷贝
	}

	VarLenRow vRow(tableDef, dest->m_data, dest->m_size, 0);
	for (u16 i = 0; i < tableDef->m_numCols; i++) {
		vRow.appendColumn(&idIncCol[i]);
	}
	dest->m_size = (uint)vRow.size();
	dest->m_rowId = src->m_rowId;
}

/**
 * 将变长格式的记录转化为压缩排序格式的记录
 * @param ctx 内存分配上下文
 * @param tableDef 表定义
 * @param src 要转换的变长格式的记录，为REC_VARLEN格式
 * @param dest INOUT 输出转换后的压缩排序格式记录，为REC_COMPRESSORDER格式，调用方保证有足够内存，并通过dest->m_size告之
 *                 如果dest->m_segSizes为NULL，则不会填充各个属性组起始数据的偏移量
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
		idIncCol[col->colNo()] = *col;//浅拷贝
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
* 将压缩排序格式的记录转化为压缩格式的记录
* @param cprsRcdExtrator 压缩解压缩提取器 
* @param src 要转换的压缩排序格式记录, 为REC_COMPRESSORDER格式
* @param dest INOUT，输出转换后的压缩格式记录，为REC_COMPRESSED格式，调用方保证有不够的内存并通过dest->m_size告之
* @return 返回记录的压缩比
*/
double RecordOper::convRecordCOToComprssed(CmprssRecordExtractor *cprsRcdExtrator, const CompressOrderRecord *src, Record *dest) {
	assert(src->m_format == REC_COMPRESSORDER);
	assert(dest->m_format == REC_COMPRESSED);
	assert(cprsRcdExtrator != NULL);
	assert(src->m_segSizes != NULL);
	return cprsRcdExtrator->compressRecord(src, dest);
}

/**
* 将压缩格式的记录转化为压缩排序格式的记录
* @param cprsRcdExtrator 压缩解压缩提取器
* @param src 要转换的压缩格式记录, 为REC_COMPRESSED格式
* @param dest INTOUT, 输出转换后的压缩排序格式记录，为REC_COMPRESSORDER格式，调用方保证有不够的内存并通过dest->m_size告之
*/
void RecordOper::convRecordCompressedToCO(CmprssRecordExtractor *cprsRcdExtrator, const Record *src, CompressOrderRecord *dest) {
	assert(src->m_format == REC_COMPRESSED);
	assert(dest->m_format == REC_COMPRESSORDER);
	assert(cprsRcdExtrator != NULL);
	cprsRcdExtrator->decompressRecord(src, dest);
}

/**
 * 将压缩的记录转化为变长格式的记录
 * @param ctx 内存分配上下文
 * @param tableDef 所属表定义
 * @param cprsRcdExtrator 压缩解压缩提取器
 * @param src INOUT 输入为压缩格式的记录，如果dest为NULL则输出为变长格式的记录
 * @param dest OUT 如果传进来的为NULL则转化后的记录存储在src中, 直接覆盖原来src的内容(为了避免内存分配，调用方应该确保原来src的内容不再用了)
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
 * 从压缩格式记录中提取冗余格式子记录
 * @param mtx 内存分配上下文，如果为NULL，则内部使用new分配内存
 * @param cprsRcdExtrator 压缩解压缩提取器
 * @param tableDef 所属表定义
 * @param record 压缩格式的记录
 * @param subRecord OUT 输出子记录内容，必须为冗余格式，调用方保证分配了足够的内存
 * @param cmpressExtractInfo 压缩属性组提取信息
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
		if (cmpressExtractInfo->m_colNumNeedReadInGrp[colGrp->colGrpNo()] > 0) {//如果这个压缩属性组需要解压
			uint decompressSize = 0;//属性组解压缩之后的长度
			cprsRcdExtrator->decompressColGroup(colGrp->data(), (uint)colGrp->lenBytes(), colGrp->getRealSize(), 
				cmpressExtractInfo->m_decompressBuf, &decompressSize);
			assert(decompressSize <= tableDef->m_maxRecSize);

			ColGroupRow curColGrpRow(tableDef, colGrp->colGrpNo(), cmpressExtractInfo->m_decompressBuf, 
				decompressSize, record->m_data);
			Column column;
			u16 colNumNeedRead = cmpressExtractInfo->m_colNumNeedReadInGrp[colGrp->colGrpNo()];
			for (Column *col = curColGrpRow.firstColumn(&column); col; col = curColGrpRow.nextColumn(col)) {
				if (cmpressExtractInfo->m_colNeedRead[col->colNo()] > 0) {//这一列需要读取
					mRow.writeColumn(col->colNo(), col);
					colNumNeedRead--;
					if (colNumNeedRead == 0)//这个属性组中需要读取的字段已经全部读完
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
* 非本地更新包含全局字典的表的记录
* 注意要传进来的记录可能是REC_VARLEN格式，也可能是REC_COMPRESSED格式，
* 因为包含全局字典的表可能是压缩表也可能不是压缩表,
* 如果是压缩表，则更新后的记录是否进行压缩，取决于更新后再压缩的压缩比；否则不压缩；
* @pre 调用方保证传递进来的压缩记录提取器非空，并且其字典一定可用
* @post: newRcd的m_size, m_data, m_format, m_isRealCompressed可能被修改
* @param ctx             内存分配上下文
* @param tableDef        所属的表定义
* @param cprsRcdExtrator 压缩记录提取器
* @param oldRcd          要更新的记录，其格式为REC_COMPRESSED或REC_VARLEN,
* @param update          要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
* @param newRcd OUT      输出更新后的记录，调用方保证有足够的空间，并通过m_size告之
* @param lobUseOld       大对象字段是否使用前像的值
*/
void RecordOper::updateRcdWithDic(MemoryContext *ctx, const TableDef *tableDef, 
	CmprssRecordExtractor *cprsRcdExtrator, const Record *oldRcd, 
	const SubRecord *update, Record *newRcd) {
		assert(update->m_format == REC_REDUNDANT);
		assert((uint)tableDef->m_maxRecSize == update->m_size);
		assert(cprsRcdExtrator);
		assert(oldRcd->m_format == REC_COMPRESSED || oldRcd->m_format == REC_VARLEN);

		if (oldRcd->m_format == REC_COMPRESSED) {//原纪录是压缩的
			updateCompressedRcd(ctx, tableDef, cprsRcdExtrator, oldRcd, update, newRcd);
		} else {//原纪录不是压缩的
			updateUncompressedRcd(ctx, tableDef, cprsRcdExtrator, oldRcd, update, newRcd);
		}
}

/**
* 更新压缩格式的记录
*
* @post: newRcd的m_id, m_size, m_data, m_format, m_isRealCompressed可能被修改
* @param ctx             内存分配上下文
* @param tableDef        记录所属的表定义
* @param cprsRcdExtrator 压缩记录提取器
* @param oldRcd          要更新的记录，其格式一定为REC_COMPRESSED
* @param update          要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
* @param newRcd          OUT 输出更新后的记录，可能是压缩的格式，也可能是变长格式，取决于shouldCompress参数以及压缩比，
*                        调用方保证有足够的空间，并通过m_size告之
* @param lobUseOld       大对象字段是否使用前像的值
*/
void RecordOper::updateCompressedRcd(MemoryContext *mtx, const TableDef *tableDef, 
									 CmprssRecordExtractor *cprsRcdExtrator, const Record *oldRcd, 
									 const SubRecord *update, Record *newRcd) {
	 assert(mtx);
	 assert(oldRcd->m_format == REC_COMPRESSED);

	 McSavepoint msp(mtx);
	 CmprssColGrpExtractInfo cmpressExtractInfo(tableDef, update->m_numCols, update->m_columns, mtx);
	 byte *tmpDecompressBuf = (byte *)mtx->alloc(tableDef->m_maxRecSize);//用于缓存解压缩后的数据
	 byte *tmpUpdateBuf = (byte *)mtx->alloc(Limits::PAGE_SIZE);//用于缓存更新后待压缩的数据

	 RedRow mRow(tableDef, update);
	 CompressedRow oldRow(tableDef, oldRcd);

	 // newRow.m_size首先被初始化为tableDef->m_bmBytes
	 CompressedRow newRow(tableDef, newRcd->m_data, newRcd->m_size, true);
	 CompressedColGroup cmprsColGrp;
	 uint uncprsSize = 0;

	 // Fix Bug #113623
	 // 首先需要Copy Null Bitmap，然后才能处理Column Groups
	 newRow.setNullBitmap(oldRcd->m_data, tableDef->m_bmBytes);

	 for (CompressedColGroup *colGrp = oldRow.firstColGrp(&cmprsColGrp); colGrp; colGrp = oldRow.nextColGrp(colGrp)) {
		 u8 colGrpNo = colGrp->colGrpNo();
		 if (cmpressExtractInfo.m_colNumNeedReadInGrp[colGrpNo] > 0) {//如果这个压缩属性组需要更新,先解压,更新后再压缩
			 uint decompressSize = 0;//属性组解压缩之后的长度
			 cprsRcdExtrator->decompressColGroup(colGrp->data(), (uint)colGrp->lenBytes(), colGrp->getRealSize(), 
				 tmpDecompressBuf, &decompressSize);
			 assert(decompressSize <= tableDef->m_maxRecSize);

			 //更新数据
			 ColGroupRow cgRow(tableDef, colGrpNo, tmpDecompressBuf, decompressSize, oldRcd->m_data);
			 ColGroupRow newColGrpRow(tableDef, colGrpNo, tmpUpdateBuf, Limits::DEF_MAX_REC_SIZE,
				 newRcd->m_data, true);
			 Column column, column1;
			 for (Column *col = cgRow.firstColumn(&column); col; col = cgRow.nextColumn(col)) {
				 if (cmpressExtractInfo.m_colNeedRead[col->colNo()] > 0) {
					 Column *updateCol = mRow.columnAt(col->colNo(), &column1);//后像的列
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
		 //计算更新后的记录的压缩比，如果达不到压缩比阀值，则解压
		 double cprsRatio = newRcd->m_size * 100.0 / uncprsSize;
		 if (cprsRatio <= tableDef->m_rowCompressCfg->compressThreshold()) {		
			 return;
		 }
	 }
	 RecordOper::convRecordCompressedToVar(mtx, tableDef, cprsRcdExtrator, newRcd);
}

/**
 * 更新变长格式的记录并尝试压缩
 * 更新后的记录是否进行压缩，取决于shouldCompress参数以及更新后再压缩的压缩比
 *
 * @post: newRcd的m_id, m_size, m_data, m_format, m_isRealCompressed可能被修改
 * @param ctx 内存分配上下文
 * @param tableDef 记录所属的表定义
 * @param cprsRcdExtrator 压缩记录提取器
 * @param oldRcd 要更新的记录，其格式为REC_VARLEN,
 * @param update 要更新的属性及这些属性的新值，其m_format一定为REC_REDUNDANT
 * @param newRcd OUT 输出更新后的记录，调用方保证有足够的空间
 */

void RecordOper::updateUncompressedRcd(MemoryContext *ctx, const TableDef *tableDef, 
									   CmprssRecordExtractor *cprsRcdExtrator, const Record *oldRcd, 
									   const SubRecord *update, Record *newRcd) {
	assert(cprsRcdExtrator);
	assert(REC_VARLEN == oldRcd->m_format);

	McSavepoint savePoint(ctx);
	RedRow mRow(tableDef, update); // 更新信息
	VarLenRow vRow(tableDef, oldRcd->m_data, oldRcd->m_size, oldRcd->m_size);//原行

	if (tableDef->m_isCompressedTbl) {
		Column *mergeCols = new (ctx->alloc(tableDef->m_numCols * sizeof(Column)))Column[tableDef->m_numCols];
		Column column, updCol;
		int deltaSize = 0;
		uint idx = 0;
		for (Column* col = vRow.firstColumn(&column); col ; col = vRow.nextColumn(col)) {
			u16 no = col->colNo();
			if (idx < update->m_numCols && no == update->m_columns[idx]){ // 找到一个匹配列
				mergeCols[no] = *(mRow.columnAt(no, &updCol));
				deltaSize += mergeCols[no].size() - col->size();
				++idx;
			} else {
				mergeCols[no] = *col;//浅拷贝
			}
		}
		
		//计算更新之后的后像大小
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

		//尝试重新压缩，可能被压缩也可能不被压缩，取决于尝试压缩后的压缩比
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
			if (idx < update->m_numCols && col->colNo() == update->m_columns[idx]){ // 找到一个匹配列
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
 * 读取压缩段长度
 * @param src         压缩段起始位置
 * @param segSize OUT 压缩段长度
 * @return            为表示该长度所用的字节数
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
 * 写出一个压缩段的长度
 * @pre 数据段长度不能超TWO_BYTE_SEG_MAX_SIZE的最大长度
 * @param dest  写出的开始位置
 * @param size  压缩段长度
 * @return      为表示压缩段所用的字节数
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

 /* 创建小型大对象记录
  *	小型大对象表定义(INT, VARCHAR)
  *	INT: 压缩之前长度
  *	VARHCAR: 大对象数据
  * @param tableDef 小型大对象表定义
  * @param buf 记录内存
  * @param bufSize 记录内存大小，调用者必须保证buf有足够的空间
  * @param data 小型大对象数据, data==0表示大对象为NULL
  * @param size 小型大对象长度
  * @param orgSize 压缩之前的大对象长度
  * @return 返回值等同于记录长度
  */
size_t createSlobRow(const TableDef *tableDef, byte *buf, size_t bufSize, const byte *data, size_t size, size_t orgSize) {
	assert(!((size && !data) || (orgSize && !data)));
	// 暂时没有实现
	// assert(orgSize >= size);
	assert(tableDef->m_numCols == 2);
	assert(tableDef->m_nullableCols == 2);
	size_t bmBytes = calcBitmapBytes(REC_VARLEN, tableDef->m_nullableCols);
	assert(bufSize >= size + bmBytes);
	UNREFERENCED_PARAMETER(bufSize);
	// 初始化位图
	memset(buf, 0, bmBytes);


	if (!data) {
		BitmapOper::setBit(buf, bmBytes << 3, 0);
		BitmapOper::setBit(buf, bmBytes << 3, 1);
		return bmBytes;
	}

	BitmapOper::clearBit(buf, bmBytes << 3, 0);
	BitmapOper::clearBit(buf, bmBytes << 3, 1);
	byte *ptr = buf + bmBytes;
	// 处理长度列
	assert(tableDef->m_columns[0]->m_type == CT_INT);
	*(u32 *)ptr = (u32)orgSize;
	ptr += tableDef->m_columns[0]->m_size;

	// 处理VARCHAR
	size_t lenBytes = tableDef->m_columns[1]->m_lenBytes;
	// 写入字符串长度
	writeU32Le((u32)size, ptr, lenBytes);
	// 写入字符串内容
	ptr += lenBytes;
	memcpy(ptr, data, size);
	return (size_t)(ptr + size - buf);
}
/**
 * 创建小型大对象记录
 * @param tableDef 小型大对象表定义
 * @param rec 记录指针，调用者必须保证rec->m_data有足够的空间
 * @param data 小型大对象数据, data==0表示大对象为NULL
 * @param size 小型大对象长度
 * @param orgSize 压缩之前的大对象长度
 * @return 返回值等同于参数rec
 */
Record* RecordOper::createSlobRecord(const TableDef *tableDef, Record *rec, const byte *data, size_t size, size_t orgSize) {
	assert(rec);
	rec->m_size = (uint)createSlobRow(tableDef, rec->m_data, rec->m_size, data, size, orgSize);
	rec->m_rowId = INVALID_ROW_ID;
	rec->m_format = REC_VARLEN;
	return rec;
}
/**
 * 创建一个冗余格式的小型大对象子记录
 * @param tableDef 小型大对象表定义
 * @param sr 子记录，调用者负责内存分配
 * @param data 大对象数据
 * @param size 大对象数据长度
 * @param orgSize 压缩之前的大对象长度
 * @return 记录数据
 */
SubRecord* RecordOper::createSlobSubRecordR(const TableDef *tableDef, SubRecord *sr
													, const byte *data, size_t size, size_t orgSize) {
	assert(sr->m_size >= tableDef->m_maxRecSize);
	size_t realSize = createSlobRow(tableDef, sr->m_data, sr->m_size, data, size, orgSize);
	sr->m_format = REC_REDUNDANT;
	sr->m_rowId = INVALID_ROW_ID;
	sr->m_size = tableDef->m_maxRecSize;
	// 填充冗余格式未使用的内存，便于记录比较
	UNREFERENCED_PARAMETER(realSize);

	return sr;
}


/**
 * 提取小型大对象记录中包含的大对象的数据
 * @param tableDef 小型大对象表定义
 * @param rec 小型大对象记录
 * @param size[out] 数据长度
 * @param orgSize[out] 压缩之前的大对象长度
 * @return 数据指针
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
 * 判断一个属性是否为NULL
 *
 * @param tableDef 表定义
 * @param record 记录，为REC_REDUNDANT格式
 * @param cno 属性号，从0开始编号
 * @return 属性是否为NULL
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
 * 判断一个属性是否为NULL
 *
 * @param tableDef 表定义
 * @param subRec 部分记录，为REC_REDUNDANT格式
 * @param cno 属性号，从0开始编号
 * @return 属性是否为NULL
 */
bool RecordOper::isNullR(const TableDef *tableDef, const SubRecord *subRec, u16 cno) {
	Record rec(INVALID_ROW_ID, subRec->m_format, subRec->m_data, subRec->m_size);
	return isNullR(tableDef, &rec, cno);
}

/**
 * 设置一个属性是否为NULL
 *
 * @param tableDef 表定义
 * @param record 记录，为REC_REDUNDANT格式
 * @param cno 属性号，从0开始编号
 * @param null 是为NULL还是非NULL
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
 * 设置一个属性是否为NULL
 *
 * @param tableDef 表定义
 * @param subRec 部分记录，为REC_REDUNDANT格式
 * @param cno 属性号，从0开始编号
 * @param null 是为NULL还是非NULL
 */
void RecordOper::setNullR(const TableDef *tableDef, const SubRecord *subRec, u16 cno, bool null) {
	Record rec(INVALID_ROW_ID, subRec->m_format, subRec->m_data, subRec->m_size);
	setNullR(tableDef, &rec, cno, null);
}

/**
 * 获取压缩格式key的自然格式长度
 * @param tableDef 表定义
 * @param indexDef 索引定义
 * @param compressedKey 压缩格式的key
 * @return 对应自然格式key的长度
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
 * 获取记录转化为变长格式后的长度
 *
 * @param tableDef 表定义
 * @param record REC_REDUNDANT或REC_MYSQL格式表示的记录内容
 * @return 记录转化为变长格式后的长度
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
 * 获取冗余或者Mysql格式子记录转化为变长格式后的长度
 *
 * @param tableDef 表定义
 * @param subRec REC_REDUNDANT或REC_MYSQL格式表示子记录内容
 * @return 子记录转化为变长格式后的长度
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
 * 初始化空记录
 *
 * @param record [in/out] 待初始化记录
 * @param tableDef 表定义
 * @param rowId 记录RowId
 * @return 初始化后的记录
 */
Record* RecordOper::initEmptyRecord(Record *record, const TableDef *tableDef, RowId rowId, RecFormat recFormat) {
	record->m_rowId = rowId;
	record->m_format = recFormat;
	assert(tableDef->m_bmBytes <= record->m_size);
	// NULL列设置为NULL
	memset(record->m_data, -1, tableDef->m_bmBytes);
	if (tableDef->m_recFormat == REC_FIXLEN) { // 定长
		assert(record->m_size >= tableDef->m_maxRecSize); 
		record->m_size = tableDef->m_maxRecSize;
		memset(record->m_data + tableDef->m_bmBytes, 0, tableDef->m_maxRecSize - tableDef->m_bmBytes);
	} else { // 变长
		assert(tableDef->m_recFormat == REC_VARLEN || tableDef->m_recFormat == REC_COMPRESSED);
		byte *ptr = record->m_data + tableDef->m_bmBytes;
		for(u16 cno = 0; cno < tableDef->m_numCols; ++cno) {
			ColumnDef *columnDef = tableDef->m_columns[cno];
			if (!columnDef->m_nullable) { // 非NULL列，设置为0
				assert(ptr + columnDef->m_size <= record->m_data + record->m_size);
				memset(ptr, 0, columnDef->m_size);
				ptr += ntse::getColSize(columnDef, ptr);
			}
		}
		record->m_size = ptr - record->m_data;
	}
	return record;
}

/** 读取大对象ID
 * @param record REC_REDUNDANT格式的属性内容
 * @param columnDef 大对象属性定义
 * @return 大对象ID
 */
LobId RecordOper::readLobId(const byte *record, ColumnDef *columnDef) {
	assert(columnDef->isLob());
	return *((LobId *)(record + columnDef->m_offset + columnDef->m_size - 8));
}

/** 写入大对象ID
 * @param record REC_REDUNDANT格式的属性内容
 * @param columnDef 大对象属性定义
 * @param lobId 大对象ID
 */
void RecordOper::writeLobId(byte *record, ColumnDef *columnDef, LobId lobId) {
	assert(columnDef->isLob());
	*((LobId *)(record + columnDef->m_offset + columnDef->m_size - 8)) = lobId;
}

/** 读取大对象大小
 * @param record REC_MYSQL格式的属性内容
 * @param columnDef 大对象属性定义
 * @return 大对象大小
 */
uint RecordOper::readLobSize(const byte *record, ColumnDef *columnDef) {
	assert(columnDef->isLob());
	if (columnDef->m_type == CT_SMALLLOB)
		return read2BytesLittleEndian(record + columnDef->m_offset);
	else
		return read3BytesLittleEndian(record + columnDef->m_offset);
}

/** 写入大对象大小
 * @param record REC_MYSQL格式的属性内容
 * @param columnDef 大对象属性定义
 * @param size 大对象大小
 */
void RecordOper::writeLobSize(byte *record, ColumnDef *columnDef, uint size) {
	assert(columnDef->isLob());
	if (columnDef->m_type == CT_SMALLLOB)
		return write2BytesLittleEndian(record + columnDef->m_offset, size);
	else
		return write3BytesLittleEndian(record + columnDef->m_offset, size);
}

/** 读取大对象内容
 * @param record REC_MYSQL格式的属性内容
 * @param columnDef 大对象属性定义
 * @return 大对象内容
 */
byte* RecordOper::readLob(const byte *record, ColumnDef *columnDef) {
	assert(columnDef->isLob());
	return *((byte **)(record + columnDef->m_offset + columnDef->m_size - 8));
}

/** 写入大对象内容
 * @param record REC_MYSQL格式的属性内容
 * @param columnDef 大对象属性定义
 * @param lob 大对象内容
 */
void RecordOper::writeLob(byte *record, ColumnDef *columnDef, byte *lob) {
	assert(columnDef->isLob());
	*((byte **)(record + columnDef->m_offset + columnDef->m_size - 8)) = lob;
}

/**
 * 序列化子记录到指定流对象
 * 对于REC_REDUNDANT格式的记录，不存储大对象数据信息，REC_MYSQL格式存储大对象信息
 * @param s			out 存储序列化结果的流对象，这里假设流的长度足够存储记录本身
 * @param tableDef	记录对应的表定义
 * @param subRecord	要序列化的子记录，格式必须为REC_MYSQL或者REC_REDUNDANT
 * @param isLobNeeded		是否序列化大对象
 * @param isDataOnly	true表示只序列化子记录的数据部分，false需要序列化子记录的所有信息，默认false
 */
void RecordOper::serializeSubRecordMNR( Stream *s, const TableDef *tableDef, const SubRecord *subRecord, bool isLobNeeded, bool isDataOnly /*= false */ ) {
	assert(subRecord->m_format == REC_MYSQL || subRecord->m_format == REC_REDUNDANT);

	s->write(isLobNeeded);
	s->write(isDataOnly);

	RecFormat format = subRecord->m_format;
	s->write((u8)format);	// 保存格式，反序列化的时候能够知道需不需要读取大对象信息

	if (!isDataOnly) {
		s->write(subRecord->m_numCols);
		s->write((byte*)subRecord->m_columns, sizeof(subRecord->m_columns[0]) * subRecord->m_numCols);
	}

	// 将冗余或者mysql格式的子记录转为变长格式保存
	SubRecord subV(REC_VARLEN, subRecord->m_numCols, subRecord->m_columns, s->currPtr() + sizeof(u32), tableDef->m_maxRecSize, subRecord->m_rowId);
	SubRecord subR(REC_REDUNDANT, subRecord->m_numCols, subRecord->m_columns, subRecord->m_data, tableDef->m_maxRecSize, subRecord->m_rowId);
	RecordOper::convertSubRecordRV(tableDef, &subR, &subV);
	s->write((u32)subV.m_size);
	s->skip(subV.m_size);

	// 对于REC_MYSQL格式的子记录，遍历保存非空大对象信息
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
 * 反序列化指定流中的子记录信息，对于序列化指定了序列化子记录所有信息的情况，使用本函数进行反序列化
 * @param s				子记录序列化的流对象
 * @param tableDef		子记录所属表定义
 * @param memoryContext	内存上下文，用来分配反序列化的子记录所需空间
 * @return 反序列化的子记录对象
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

	if (isLobNeeded) {	// 读取大对象信息
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
 * 反序列化子记录的记录数据信息到指定缓存，对于只序列化子记录数据的情况，使用本函数反序列化
 * 只读取子记录数据信息，列信息格式信息不读取
 * @param s			序列化记录所在的流对象
 * @param tableDef	反序列化子记录所属表定义
 * @param numCols	子记录包含的列数信息
 * @param columns	子记录各个列的列号
 * @param buf		out 反序列化子记录输出
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
	// 子记录格式转换
	SubRecord subR(REC_REDUNDANT, numCols, columns, buf, tableDef->m_maxRecSize);
	SubRecord subV(REC_VARLEN, numCols, columns, s->currPtr(), size, INVALID_ROW_ID);
	RecordOper::convertSubRecordVR(tableDef, &subV, &subR);
	s->skip(size);

	if (isLobNeeded) {	// 读取大对象信息
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
 * 计算一个子记录序列化所需要占用的空间
 * 对于REC_REDUNDANT格式的子记录，不需要记录大对象的数据信息，对于REC_MYSQL格式的子记录
 * @param tableDef	子记录所属表的表定义
 * @param subRecord	要序列化的子记录
 * @param isLobNeeded		是否序列化大对象
 * @param isDataOnly	true表示只序列化子记录的数据部分，false需要序列化子记录的所有信息，默认false
 * @return 返回子记录序列化之后需要的空间大小
 */
size_t RecordOper::getSubRecordSerializeSize( const TableDef *tableDef, const SubRecord *subRecord, bool isLobNeeded, bool isDataOnly /*= false */ ) {
	assert(subRecord->m_format == REC_MYSQL || subRecord->m_format == REC_REDUNDANT);

	size_t size = 0;
	// 是否只存储数据信息，，是否序列化大对象数据，format，m_numCols，m_columns，m_size信息存储需要的空间
	size += sizeof(bool) * 2 + sizeof(u8);
	if (!isDataOnly)
		size += sizeof(u16) + sizeof(subRecord->m_columns[0]) * subRecord->m_numCols;
	// 加上记录内容压缩成变长格式需要的空间
	size += getSubRecordSizeRV(tableDef, subRecord) + sizeof(u32);

	// 对于REC_MYSQL格式的计算大对象数据需要的空间
	if (isLobNeeded) {	// 读取大对象信息
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
		
		// 目前只有purge会调用此方法，这里比较列不能使用mysql上层的方法，这里认为后缀空格数不同的varchar属性不相同，见JIRA：NTSETNT-308
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
		//打印列信息
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

/** 更新操作结构 */
struct UpdateOper {
	UpdateOper(byte* dst, byte* src, size_t size, bool copy)
		: m_src(src), m_dst(dst), m_size(size), m_copyOp(copy)
	{}
	byte* m_src;   /** 源 */
	byte* m_dst;   /** 目的 */
	size_t m_size; /** 内存长度 */
	bool m_copyOp; /** 更新动作 -- 拷贝: true, 移动:false */
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
	vector<UpdateOper> opStack; // 未完成更新操作栈
	opStack.reserve(tableDef->m_numCols); // TODO: 这里有一次内存分配，有待优化

	for(Column* oc = oldRow.firstColumn(&oldColumn); oc; ) {
		if (oc->colNo() < update->m_columns[updColIdx]) { // 处理非更新列
			// 计算由多个非更新列组成的segment
			byte*  segment = oc->data();
			size_t length = 0;
			for (; oc->colNo() < update->m_columns[updColIdx]; oc = oldRow.nextColumn(oc))
				length += oc->size();
			assert(record->m_data + oldBufferSize >= start + length);

			if (start < segment) { // 非更新列前移
				memmove(start, segment, length);
			} else if (start > segment) { // 非更新列后移
				// 为了避免覆盖已有数据，不能直接memmove，因此记录此更新操作
				opStack.push_back(UpdateOper(start, segment, length, false));
			}
			start += length;
		}
		if (oc) { // 处理更新列
			assert(oc->colNo() == update->m_columns[updColIdx]);
			updRow.columnAt(oc->colNo(), &newColumn);
			assert(record->m_data + oldBufferSize >= start + newColumn.size());
			bool isNull = oc->isNull();
			u16 bmOffset = oc->bitmapOffset();
			byte *data = oc->data();
			size_t size = oc->size();
			oc = oldRow.nextColumn(oc); // 数据覆盖之前，oc向后移动
			if (data + size >= start + newColumn.size()) { // 此时不会覆盖下一列数据
				memcpy(start, newColumn.data(), newColumn.size());
			} else { // 为了避免覆盖已有数据，不能直接memcpy，因此记录此更新操作
				opStack.push_back(UpdateOper(start, newColumn.data(), newColumn.size(), true));
			}
			start += newColumn.size();
			if (isNull != newColumn.isNull()) { // 处理Null位图
				newColumn.isNull() ?
					BitmapOper::setBit(record->m_data, oldRow.bitmapBytes() << 3, bmOffset) :
					BitmapOper::clearBit(record->m_data, oldRow.bitmapBytes() << 3, bmOffset);
			}
			if (++updColIdx >= update->m_numCols && oc) { // 再也没有待更新列
				// 移动剩余列
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
	// 回放未完成操作
	for (vector<UpdateOper>::reverse_iterator iter = opStack.rbegin(); iter != opStack.rend() ; ++iter) {
		if (iter->m_copyOp)
			memcpy(iter->m_dst, iter->m_src, iter->m_size);
		else
			memmove(iter->m_dst, iter->m_src, iter->m_size);
	}
	record->m_size = (uint)(start - record->m_data);
}


/**
* 计算记录类型列长度
* @pre col不能是压缩数据，不能为NULL
* @param col 列定义
* @param buf 列数据
*/
static inline size_t getColSize(const ColumnDef* col, const byte* buf) {
	uint actualSize = 0;
	uint lenBytes = 0;
	
	// 此处VAR/FIX与REDUNDANT 互转
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
* 计算索引记录中列长度
* @pre col不能是压缩数据，不能为NULL
* @param col 列定义
* @param buf 列数据
* @param prefixLen 前缀长度，用于前缀索引
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
* 计算REC_UPPERMYSQL记录列长度
* @pre col不能是压缩数据，不能为NULL
* @param col 列定义
* @param buf 列数据
*/
static inline size_t getUppMysqlColSize(const ColumnDef* col, const byte* buf) {
	uint actualSize = 0;
	uint lenBytes = 0;

	// 在上层MYSQL格式中，超长字段是属于变长类型
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


/** 是否可压缩整数类型 */
static bool isCompressableNumberType(ColumnType type) {
	switch (type) {
		case CT_SMALLINT:   // 二字节整数
		case CT_MEDIUMINT:	// 三字节整数
		case CT_INT:        // 四字节整数
		case CT_BIGINT:		// 八字节整数
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

/** 比较两个整数 */
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
/** 3字节整数转化成主机字节序整数 */
inline int leS24ToHostInt(const s8 *src) {
	int v = (int)*(src + 2) << 16;
	s8 *p = (s8 *)&v;
	*p = *src;
	*(p + 1) = *(src + 1);
	return v;
}
/** 3字节整数转化成主机字节序整数 */
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
 * 比较Little Endian有符号整数
 * @param num1 整数1
 * @param num2 整数2
 * @param len 整数长度
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
 * 比较Little Endian无符号整数
 * @param num1 整数1
 * @param num2 整数2
 * @param len 整数长度
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
 * 比较两个列的大小
 * @param col1	第一列
 * @param col2	第二列
 * @param sndCompressed  第二列是否压缩
 * @param cmpLob 是否比较大对象内容
 * @return -1/0/1 for </=/>
 */
static int compareColumn(Column *col1, Column *col2, bool sndCompressed, bool cmpLob, bool cmpLobAsVarchar) {
	const ColumnDef* columnDef = col1->def();
	assert(columnDef->m_no == col2->def()->m_no);
	int result = 0;
	if (col2->isNull() || col1->isNull()) { // 处理NULL列
		result = (int)col2->isNull() - (int)col1->isNull();
	} else { // 非NULL列
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
			case CT_RID: { // 可压缩整数类型 -- 解压之
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
			case CT_VARCHAR: // VARCHAR，去掉字符串首部长度进行比较
				removeLenbytes(columnDef->m_lenBytes, &data1, &size1);
				removeLenbytes(columnDef->m_lenBytes, &data2, &size2);
			case CT_CHAR:
				// 字符串比较时忽略末尾的空格，但不忽略\0、\t等，举例说明如下
				// 'a\0\0' < 'a\0' < 'a\t\t' < 'a\t' < 'a' = 'a ' = 'a  '
				result = Collation::strcoll(columnDef->m_collation, data1, size1, data2, size2);
				break;
			case CT_VARBINARY:
				removeLenbytes(columnDef->m_lenBytes, &data1, &size1);
				removeLenbytes(columnDef->m_lenBytes, &data2, &size2);
				result = memcmp(data1, data2, min((u32)size1, (u32)size2));
				if (result == 0) // 相同前缀，则比较长度
					return compareNumber((u32)size1, (u32)size2);
				break;
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				if (cmpLob) { // 比较大对象内容
					if (!cmpLobAsVarchar) {
						data1 = parseLobColumn(col1, &size1);
						data2 = parseLobColumn(col2, &size2);
						result = memcmp(data1, data2, min((u32)size1, (u32)size2));
						if (result == 0) // 相同前缀，则比较长度
							return compareNumber((u32)size1, (u32)size2);
						break;
					} else {
						removeLenbytes(col1->lenBytes(), &data1, &size1);
						removeLenbytes(col2->lenBytes(), &data2, &size2);
						// 字符串比较时忽略末尾的空格，但不忽略\0、\t等，举例说明如下
						// 'a\0\0' < 'a\0' < 'a\t\t' < 'a\t' < 'a' = 'a ' = 'a  '
						result = Collation::strcoll(columnDef->m_collation, data1, size1, data2, size2);
						break;
					}
				}
			case CT_DECIMAL:
			case CT_BINARY:
				result = memcmp(data1, data2, min((u32)size1, (u32)size2));
				if (result == 0) // 相同前缀，则比较长度
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
 * 解析REC_MYSQL格式记录的大对象字段
 * @param columns 字段
 * @param lobSize [out] 大对象长度
 * @return 大对象内存地址
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

/** 子记录中nullable列个数 */
u16 calcNullableCols(const TableDef* tableDef, const SubRecord* sb) {
	u16 cnt = 0;
	for (u16 i = 0; i < sb->m_numCols; ++i) {
		if (tableDef->m_columns[sb->m_columns[i]]->m_nullable)
			++cnt;
	}
	return cnt;
}
/** 判断是否为MYSQL格式列 */
static bool isUppMysqlFormat(RecFormat format) {
	return format == REC_UPPMYSQL;
}
/** 判断是否冗余，即列占用的内存大小是否固定为最大列长度 */
static bool isRedundantFormat(RecFormat format) {
	return format == REC_REDUNDANT || format == KEY_PAD || format == REC_FIXLEN || 
		format == REC_MYSQL || format == KEY_MYSQL;
}
/** 判断是否Key格式 */
static bool isKeyFormat(RecFormat format) {
	return (format == KEY_COMPRESS || format == KEY_NATURAL || format == KEY_PAD);
}
/** 判断是否记录格式 */
static bool isRecFormat(RecFormat format) {
	return (format == REC_REDUNDANT || format == REC_MYSQL || format == REC_VARLEN || 
		format == REC_FIXLEN || format == REC_COMPRESSED || format == REC_COMPRESSORDER);
}

//////////////////////////////////////////////////////////////////////////
//// ColList
//////////////////////////////////////////////////////////////////////////

/** 构造一个空属性列表 */
ColList::ColList() {
	m_size = 0;
	m_cols = NULL;
}

/** 构造属性列表
 *
 * @param size 属性个数
 * @param cols 各属性号
 */
ColList::ColList(u16 size, u16 *cols) {
	m_size = size;
	m_cols = cols;
}

/** 构造一个拥有指定的属性数，属性号从startCno递增的属性列表
 * @param mc 用于分配返回值属性列表所用空间的内存分配上下文
 * @param size 属性个数
 * @return 属性列表
 */
ColList ColList::generateAscColList(MemoryContext *mc, u16 startCno, u16 size) {
	u16 *cols = (u16 *)mc->alloc(sizeof(u16) * size);
	for (u16 i = 0; i < size; i++)
		cols[i] = startCno + i;
	return ColList(size, cols);
}

/** 拷贝属性列表
 * @param mc 用于分配返回值属性列表所用空间的内存分配上下文
 * @return 当前属性列表的拷贝
 */
ColList ColList::copy(MemoryContext *mc) const {
	return ColList(m_size, (u16 *)mc->dup(m_cols, sizeof(u16) * m_size));
}

/** 合并两个属性列表(去除重复)
 * @pre 属性号必须递增
 *
 * @param a 要合并的第一个属性列表
 * @param b 要合并的第二个属性列表
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

/** 合并两个属性列表(去除重复)
 * @pre 属性号必须递增
 *
 * @param mc 用于分配返回值所用空间的内存分配上下文
 * @param another 与这个属性列表合并
 * @return 合并结果
 */
ColList ColList::merge(MemoryContext *mc, const ColList &another) const {
	assert(isAsc() && another.isAsc());
	ColList r(m_size + another.m_size, (u16 *)mc->alloc(sizeof(u16) * (m_size + another.m_size)));
	r.merge(*this, another);
	return r;
}

/** 计算当前属性列表除去指定属性列表所剩下的属性列表
 * @pre 属性号必须递增
 *
 * @param mc 用于分配返回值所用空间的内存分配上下文
 * @param another 除去这个属性列表中的属性
 * @return 剩下的属性列表
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

/** 将属性号从小到大排序
 *
 * @param mc 用于分配返回值所用空间的内存分配上下文
 * @return 属性号排好序的属性列表
 */
ColList ColList::sort(MemoryContext *mc) const {
	u16 *colsCopy = (u16 *)mc->alloc(sizeof(u16) * m_size);
	memcpy(colsCopy, m_cols, sizeof(u16) * m_size);
	std::sort(colsCopy, colsCopy + m_size);
	return ColList(m_size, colsCopy);
}

/** 判断指定的属性列表与当前属性列表是否有交集
 * @pre 属性号必须递增
 *
 * @param another 属性列表
 * @return 是否有交集
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

/** 比较指定的属性列表与当前属性列表是否相等
 * @param another 用于比较的另一属性列表
 * @return 是否相等
 */
bool ColList::operator == (const ColList &another) const {
	if (m_size != another.m_size)
		return false;
	return memcmp(m_cols, another.m_cols, sizeof(u16) * m_size) == 0;
}

/** 判断属性号是否递增
 * @return 属性号是否递增
 */
bool ColList::isAsc() const {
	for (u16 i = 1; i < m_size; i++)
		if (m_cols[i] <= m_cols[i - 1])
			return false;
	return true;
}

/** 判断当前属性列是不是指定的另一个属性列的子集
 * @pre	两个属性列表都应该是递增的
 * @param another 用于判断是不是超集的属性列表
 * @return true表示this是another的子集，否则返回false
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
	u16 m_offset;	// 相对记录头的偏移
	u16 m_size;		// 内存段长度
};

// 两次memcpy间隔小于本常量时， 可合并为一次memcpy
const size_t MAX_MEMCPY_MERGE_SIZE = 32;

/**
 * 定长记录到冗余子记录提取器
 */
class SubRecExtractorFR: public SubrecExtractor {
public:
	SubRecExtractorFR(MemoryContext *ctx, const TableDef *tableDef
		, u16 numCols, const u16* columns)
		:m_tableDef(tableDef), m_numSegment(0) {
		assert(numCols <= tableDef->m_numCols);

		UNREFERENCED_PARAMETER(ctx);

		// 拷贝位图
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
					// 找到可以合并的内存段
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
	 * 提取子记录
	 * @param record 定长类型记录
	 * @param subRecord 冗余类型子记录
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
	const TableDef *m_tableDef;	// 表定义
	MemSegment m_segments[Limits::MAX_COL_NUM + 1]; // 内存区段
	uint m_numSegment; // 内存区段数目
};

/**
 * 判定类型是否定长
 * @param type 类型
 * @return true:定长； false：变长
 */
static bool isFixedLen(ColumnType type) {
	return (type != CT_VARCHAR && type != CT_VARBINARY);
}

/**
 * 判定类型是否是大对象字段
 * @param type 类型
 * @return true:是大对象； false：不是大对象
 */
static bool isLob(ColumnType type) {
	return (type == CT_SMALLLOB) || (type == CT_MEDIUMLOB);
}

/**
 * 变长记录到冗余子记录提取器
 */
class SubRecExtractorVR: public SubrecExtractor {
	// 行碎片：包含多个连续定长列和一个变长列
	struct RowFragement {
		u16 m_size;			// 定长列的长度
		u16 m_varlenBytes;	// 变长列，长度字节数
		u16 m_srcStart;		// 源起始偏移(相对本fragment起始地址), -1 表示无需拷贝到目的
		u16 m_dstStart;		// 目标起始偏移, -1 表示无需拷贝到目的
		u16 m_copySize;		// 拷贝长度，不包括变长列
		bool m_copyVarCol;	// 是否拷贝变长列
	};
public:
	SubRecExtractorVR(MemoryContext *ctx, const TableDef *tableDef
						, u16 numCols, const u16* columns);
	virtual ~SubRecExtractorVR();

	virtual void extract(const Record *record, SubRecord *subRecord);
	string toString();

private:
	const TableDef *m_tableDef;	// 表定义
	MemoryContext *m_ctx;		// 内存上下文
	RowFragement *m_fragments;	// 段
	uint m_numFragments;		// 段数目
};

/**
 * 构造SubRecExtractorVR
 * @param ctx 内存上下文
 * @param tableDef 表定义
 * @param numCols 待提取列数
 * @param columns 待提取列
 */
SubRecExtractorVR::SubRecExtractorVR(MemoryContext *ctx, const TableDef *tableDef
									, u16 numCols, const u16* columns)
	: m_tableDef(tableDef), m_ctx(ctx) {

	if (ctx)
		m_fragments = (RowFragement *)ctx->alloc((1 + tableDef->m_numCols) * sizeof(RowFragement));
	else
		m_fragments = new RowFragement[1 + tableDef->m_numCols];

	// 处理位图
	m_fragments[0].m_size = m_tableDef->m_bmBytes;
	m_fragments[0].m_varlenBytes = 0;
	m_fragments[0].m_srcStart = 0;
	m_fragments[0].m_dstStart = 0;
	m_fragments[0].m_copySize = m_tableDef->m_bmBytes;
	m_fragments[0].m_copyVarCol = false;
	m_numFragments = 1;

	u16 colIdx = 0;
	u16 segStartOffset = 0; // 当前段的开始偏移
	for (u16 cno = 0; cno < tableDef->m_numCols; ++cno) {
		ColumnDef *colDef = m_tableDef->m_columns[cno];
		if (isFixedLen(colDef->m_type)) { // 当前列是定长列
			if (!m_fragments[m_numFragments - 1].m_varlenBytes) { // 当前段不包含变长列
				if (cno == columns[colIdx]) { // 当前列是待提取列
					if (m_fragments[m_numFragments - 1].m_srcStart == (u16)-1) { // 本段第一个待提取列
						m_fragments[m_numFragments - 1].m_srcStart = colDef->m_offset - segStartOffset;
						m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
						m_fragments[m_numFragments - 1].m_copySize = colDef->m_size;
					} else { // 不是本段的第一个待提取列, 尝试合并memcpy
						u16 gap = colDef->m_offset - segStartOffset
							- m_fragments[m_numFragments - 1].m_srcStart - m_fragments[m_numFragments - 1].m_copySize;
						if (gap <= MAX_MEMCPY_MERGE_SIZE) { // 可以合并拷贝
							m_fragments[m_numFragments - 1].m_copySize
								= colDef->m_offset + colDef->m_size - m_fragments[m_numFragments - 1].m_dstStart;
						} else { // 不可合并,则开始新的一段
							goto _new_segment;
						}
					}
				}
				m_fragments[m_numFragments - 1].m_size = m_fragments[m_numFragments - 1].m_size + colDef->m_size;
			} else { // 当前段已经包含变长列，那么开始新的一段(定长列带头)
_new_segment:
				segStartOffset = m_tableDef->m_columns[cno]->m_offset;
				++ m_numFragments;
				m_fragments[m_numFragments - 1].m_size = colDef->m_size;
				m_fragments[m_numFragments - 1].m_varlenBytes = 0;
				m_fragments[m_numFragments - 1].m_copyVarCol = false;
				if (cno == columns[colIdx]) { // 待提取列
					m_fragments[m_numFragments - 1].m_srcStart = 0;
					m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
					m_fragments[m_numFragments - 1].m_copySize = colDef->m_size;
				} else {
					m_fragments[m_numFragments - 1].m_srcStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_dstStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_copySize = 0;
				}
			}
		} else { // 当前列是变长列
			if (m_fragments[m_numFragments - 1].m_varlenBytes != 0) { // 当前段已包含变长列
_new_segment_var: // 开始新的一段
				segStartOffset = m_tableDef->m_columns[cno]->m_offset;
				++ m_numFragments;
				m_fragments[m_numFragments - 1].m_size = 0;
				m_fragments[m_numFragments - 1].m_copySize = 0;
				m_fragments[m_numFragments - 1].m_varlenBytes = colDef->m_lenBytes;
				if (cno == columns[colIdx]) { // 待提取列
					m_fragments[m_numFragments - 1].m_srcStart = 0;
					m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
					m_fragments[m_numFragments - 1].m_copyVarCol = true;
				} else {
					m_fragments[m_numFragments - 1].m_srcStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_dstStart = (u16)-1;
					m_fragments[m_numFragments - 1].m_copyVarCol = false;
				}
			} else { // 当前段不包含变长列
				if (cno == columns[colIdx]) { // 待提取列
					if (m_fragments[m_numFragments - 1].m_srcStart == (u16)-1) { // 没有待拷贝的定长列(无需合并拷贝)
						m_fragments[m_numFragments - 1].m_srcStart
							= colDef->m_offset - segStartOffset;
						m_fragments[m_numFragments - 1].m_dstStart = colDef->m_offset;
						m_fragments[m_numFragments - 1].m_copyVarCol = true;
					}  else { // 尝试合并定长列和变长列
						u16 gap = colDef->m_offset - segStartOffset
							- m_fragments[m_numFragments - 1].m_srcStart - m_fragments[m_numFragments - 1].m_copySize;
						if (gap <= MAX_MEMCPY_MERGE_SIZE) { // 可以合并拷贝
							m_fragments[m_numFragments - 1].m_copyVarCol = true;
							m_fragments[m_numFragments - 1].m_copySize = colDef->m_offset
																			- m_fragments[m_numFragments - 1].m_dstStart;
						} else { // 不可合并,则开始新的一段
							goto _new_segment_var;
						}
					}
				}
				// 设置当前段的变长列长度字节数
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
 * 提取子记录
 * @param record 定长类型记录
 * @param subRecord 冗余类型子记录
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
		byte *base = record->m_data; // 基地址
		for (uint i = 0; i < m_numFragments; ++i) {
			size_t size = m_fragments[i].m_size;
			if (m_fragments[i].m_varlenBytes) { // 包含变长列
				size_t varColSize = (size_t)readU32Le(base + size, m_fragments[i].m_varlenBytes);
				varColSize += m_fragments[i].m_varlenBytes;
				if (m_fragments[i].m_srcStart != (u16) -1) { // 需拷贝
					if (m_fragments[i].m_copyVarCol) {// 需拷贝变长列
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
			} else { // 不包含变长列
				if (m_fragments[i].m_srcStart != (u16) -1) { // 需拷贝
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


/** 转化为字符串 */
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
	* 从压缩格式记录中提取冗余格式子记录的提取器
	* 
	*/
	class SubRecExtractorCR : public SubrecExtractor {
	public:
		SubRecExtractorCR(MemoryContext *ctx, const TableDef *tableDef
			, u16 numCols, const u16* columns, CmprssRecordExtractor *cprsRcdExtrator);
		virtual ~SubRecExtractorCR();

		virtual void extract(const Record *record, SubRecord *subRecord);

	private:
		const TableDef      *m_tableDef;	     /** 表定义 */
		MemoryContext       *m_ctx;		         /** 内存上下文 */
		SubRecExtractorVR   *m_subRecExtorVR;    /** 变长格式记录到冗余格式子记录提取器 */
		CmprssRecordExtractor *m_cprsRcdExtrator;  /** 压缩记录提取器 */
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
	* 提取子记录
	* @record 待提取的记录，可能为REC_COMPRESSED格式，也可能是REC_VARLEN格式
	* @subRecord 冗余格式子记录
	*/
	void SubRecExtractorCR::extract(const Record *record, SubRecord *subRecord) {
		assert(subRecord->m_format == REC_REDUNDANT);
		assert(ColList(subRecord->m_numCols, subRecord->m_columns).isAsc());
		assert((uint)m_tableDef->m_maxRecSize <= subRecord->m_size);

		assert(record->m_format == REC_VARLEN || record->m_format == REC_COMPRESSED);
		if (record->m_format == REC_COMPRESSED) {
			//如果记录是压缩的,则需要先解压缩, 再提取冗余格式子记录
			assert(NULL != m_cprsRcdExtrator);
			RecordOper::extractSubRecordCompressedR(m_ctx, m_cprsRcdExtrator, m_tableDef, record, 
				subRecord, m_cmprsExtractInfo);
		} else {
			//否则其一定是REC_VARLEN格式的，按变长格式提取
			m_subRecExtorVR->extract(record, subRecord);
		}
	}
 /* 
  *变长记录到冗余子记录提取器（退化版本)
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
	const TableDef *m_tableDef; // 表定义
};

/**
 * 变长记录到冗余子记录提取器（退化版本)
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
	const TableDef *m_tableDef; // 表定义
};

/**
 * 工厂方法：子记录提取器构造。
 * 注: 使用本函数提取子记录可能会导致目标子记录中非被提取记录被覆盖
 *
 * 目前只支持下列srcFmt -> dstFmt组合
 * - REC_FIXLEN -> REC_REDUNDANT
 * - REC_VARLEN -> REC_REDUNDANT
 * @param ctx 内存上下文，如果不为NULL，内存从这里分配
 * @param tableDef 表定义，内部会引用，必须在extractor之后delete
 * @param numCols 待提取列数
 * @param columns 待提取列
 * @param srcFmt 记录格式
 * @param dstFmt 子记录格式
 * @param extractCount 预计提取次数
 * @return 子记录提取器
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
 * 构造含压缩记录的表的子记录提取器
 * @param ctx 内存上下文，如果不为NULL，内存从这里分配
 * @param tableDef 表定义，内部会引用，必须在extractor之后delete
 * @param numCols 待提取列数
 * @param columns 待提取列
 * @param cprsRcdExtrator 压缩解压缩提取器
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
 * 构造子记录提取器
 *
 * @param session 会话
 * @param tableDef 表定义，内部会引用，必须在extractor之后delete
 * @param dstRec 用于存储提取内容的部分记录，为REC_REDUNDANT格式
 * @param extractCount 预计提取次数
 * @param cprsRcdExtrator 压缩解压缩提取器
 * @return 子记录提取器
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

/** 压缩键值到PAD格式提取器 */
class SubToSubExtractorCP: public SubToSubExtractor {
public:
	SubToSubExtractorCP(const TableDef *tableDef, const IndexDef *indexDef) {
		m_tableDef = tableDef;
		m_indexDef = indexDef;
	}

protected:
	const TableDef	*m_tableDef;	/** 表定义 */
	const IndexDef	*m_indexDef;	/** 索引定义 */
};

/** 压缩键值到PAD格式记录提取器: 退化版，支持混合属性 */
class DegSubToSubExtractorCP: public SubToSubExtractorCP {
public:
	DegSubToSubExtractorCP(const TableDef *tableDef, const IndexDef *indexDef): SubToSubExtractorCP(tableDef, indexDef) {}
	virtual void extract(const SubRecord *srSrc, SubRecord *srDest) {
		RecordOper::convertKeyCP(m_tableDef, m_indexDef, srSrc, srDest);
	}
};

/** 压缩键值到冗余子记录提取器 */
class SubToSubExtractorCR: public SubToSubExtractor {
public:
	SubToSubExtractorCR(const TableDef *tableDef, const IndexDef *indexDef) {
		m_tableDef = tableDef;
		m_indexDef = indexDef;
	}

protected:
	const TableDef	*m_tableDef;	/** 表定义 */
	const IndexDef	*m_indexDef;	/** 索引定义 */
};

/** 压缩键值到冗余子记录提取器: 优化版 */
class FastSubToSubExtractorCR: public SubToSubExtractorCR {
	/** 各个属性的一些性质 */
	struct ColInfo {
		bool	m_isNeeded;		/** 是否需要 */
		bool	m_isCompressed;	/** 是否是压缩的 */
		bool	m_isVarlen;		/** 是否为变长 */
		ColumnDef	*m_colDef;	/** 属性定义 */
	};
public:
	/** 判断是否可以进行优化
	 *
	 * @param tableDef 表定义
	 * @param numColsSrc 源子记录属性数
	 */
	static bool isSupported(const TableDef *tableDef, u16 numColsSrc) {
		// 为了快速判断所有属性是否非NULL，由于绝大多数索引中属性数不超过8，这一限制合理
		if (numColsSrc > 8)
			return false;
		// 为了快速设置目标的空值位图，绝大多数表中属性数<64，记录大小不小于8字节
		if (tableDef->m_bmBytes > 8 || tableDef->m_maxRecSize < 8)
			return false;
		return true;
	}
	
	/** 构造一个高度优化的压缩键值到冗余子记录提取器
	 * @pre 只有isSupported返回true时才能支持这一优化
	 *
	 * @param ctx 内存上下文
	 * @param tableDef 表定义，内部会引用，必须在extractor之后delete
	 * @param indexDef 索引定义
	 * @param numColsSrc 源子记录属性数
	 * @param columnsSrc 源子记录各属性号，函数返回后不会引用
	 * @param numColsDst 目标子记录属性数
	 * @param columnsDst 目标子记录各属性号，函数返回后不会引用
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
		if (m_hasNull && *srSrc->m_data != 0) {	// 有些属性是NULL，不进行优化
			RecordOper::extractSubRecordCR(m_tableDef, m_indexDef, srSrc, srDst);
			return;
		}
		*((u64 *)srDst->m_data) = 0;	// 设置目标空值位图，允许覆盖掉一些数据
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
	bool			m_hasNull;		/** 是否包含可空属性 */
	u16				m_bmBytes;		/** 空值位图占用字节数 */
	ColInfo			*m_colInfos;	/** 各属性要怎么处理 */
};

/** 压缩键值到冗余子记录提取器: 退化版，支持混合属性 */
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

/** 工厂方法，构造一个合适的子记录到子记录提取器
 * 注: 使用本函数提取子记录可能会导致目标子记录中非被提取记录被覆盖
 *
 * 目前只支持下列srcFmt -> dstFmt组合
 * - KEY_COMPRESSED -> REC_REDUNDANT
 *
 * @param ctx 内存上下文
 * @param tableDef 表定义，内部会引用，必须在extractor之后delete
 * @param numColsSrc 源子记录属性数
 * @param columnsSrc 源子记录各属性号，函数返回后不会引用
 * @param numColsDst 目标子记录属性数
 * @param columnsDst 目标子记录各属性号，函数返回后不会引用
 * @param srcFmt 源子记录格式
 * @param dstFmt 目标子记录格式
 * @param extractCount 预计提取次数
 * @return 子记录提取器
 */
SubToSubExtractor* SubToSubExtractor::createInst(MemoryContext *ctx, const TableDef *tableDef,
		const IndexDef *indexDef, u16 numColsSrc, const u16 *columnsSrc, u16 numColsDst, 
		const u16 *columnsDst, RecFormat srcFmt, RecFormat dstFmt, uint extractCount) {
	assert(ColList(numColsDst, (u16 *)columnsDst).isAsc());
	const uint minExtractCount = 5;
	if (srcFmt == KEY_COMPRESS && dstFmt == KEY_PAD) {
		// TODO: 这里只提供了一种subToSubExtractor，今后可以参考CR的实现增加高速版本
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

