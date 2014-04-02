#include <vector>
#include <sstream>
#include <algorithm>
#include "api/Table.h"
#include "util/NumberCompress.h"
#include "misc/RecordHelper.h"


using namespace ntse;
using namespace std;

namespace ntse {

static vector<u16> parseColNos(const char* cols) ;
static vector<u16> parseColNames(const TableDef* tableDef, const char* colNames);
static size_t getBitmapSize(RecFormat format, size_t numCols);
static bool isKeyFormat(RecFormat format);
static bool isRedundantFormat(RecFormat format);
static bool isUppMysqlFormat(RecFormat format);
const static byte REC_UNINIT_BYTE = 0xcd; // 未初始化字节的值

template<typename InputIterator> 
static size_t getNullableColCnt(const TableDef* tableDef, InputIterator first, InputIterator last) {
	size_t cnt = 0;
	for (; first != last; ++first) {
		if (tableDef->m_columns[*first]->m_nullable)
			++cnt;
	}
	return cnt;
}


/** 
 * 以大端方式把整形写入缓存 
 * @param value 待写入整形
 * @param buf 输出缓存
 * @param size 输出缓存大小,如果size < sizeof(T),那么写入T的低位
 * @return 写入字节数
 */
template <typename T> inline  size_t writeNumberBe(T value, byte* buf, size_t size) {
	assert(size);
	size_t len = min(sizeof(T), size);
	size_t remain = len;
	while (remain > 0)
		*buf++ = (byte)(value >> ((--remain)<<3));
	return len;
}
/** 
 * 以小端方式把整形写入缓存 
 * @param value 待写入整形
 * @param buf 输出缓存
 * @param size 输出缓存大小,如果size < sizeof(T),那么写入T的低位
 * @return 写入字节数
 */
 template <typename T> inline  size_t writeNumberLe(T value, byte* buf, size_t size) {
	assert(size);
	size_t len = min(sizeof(T), size);
	byte *data = (byte *)&value;
#ifdef LITTLE_ENDIAN
	// 直接拷贝
	for (size_t cur = 0; cur < len; ++cur)
		*buf++ = data[cur];
#else
	for (size_t cur = len; cur > 0; --cur)
		*buf++ = data[cur - 1];
#endif
	return len;
}

/** 构造Record和Subrecord的数据
 * 此类不操作位图，只操作数据 
 */
class RowBuilder {
public:
	/** 
	 * @param tableDef 表定义
	 * @param 行格式
	 * @param capacity 行内存空间
	 * @param bmBytes null位图字节数
	 */
	RowBuilder(const TableDef* tableDef, RecFormat format, size_t capacity, size_t bmBytes, bool isSb = false) 
		: m_tableDef(tableDef), m_format(format), m_capacity(capacity), m_size(bmBytes), m_isSubRecord(isSb) {
		if (format == REC_REDUNDANT) 
			assert(capacity >= tableDef->m_maxRecSize);
		m_buf = new byte[m_capacity];
		memset(m_buf, REC_UNINIT_BYTE, m_capacity);
		memset(m_buf, 0, bmBytes);
	}
	~RowBuilder() {
		delete[] m_buf;
	}
	/** 是否子记录 */
	void setSubRecord(bool isSb) {
		m_isSubRecord = isSb;
	}
	bool isSubRecord() const {
		return m_isSubRecord;
	}
	/** 插入一个Null列, size是列大小
	 * 只适用于Redundant和fixlen列，其它类型记录的null列不占用内存空间
	 */
	RowBuilder* appendNull(size_t size) {
		assert(m_size + size <= m_capacity);
		memset(m_buf + m_size, REC_UNINIT_BYTE, size);
		m_size += size;
		return this;
	}
	RowBuilder* appendTinyInt(u8 value) {
		return appendField(&value, sizeof(value));
	}
	RowBuilder* appendSmallInt(u16 value) {
		byte tmp[8];
		if (m_format == KEY_COMPRESS) {
			writeNumberLe(value, tmp, sizeof(value));
			appendCompressedField(&value, sizeof(value));
		} else {
			writeNumberLe(value, tmp, sizeof(value));
			appendField(tmp, sizeof(value));
		}
		return this;
	}
	RowBuilder* appendMediumInt(u32 value) {
		byte tmp[8];
		if (m_format == KEY_COMPRESS) {
			writeNumberLe(value, tmp, 3);
			appendCompressedField(tmp, 3);
		} else {
			writeNumberLe(value, tmp, 3);
			appendField(tmp, 3);
		}
		return this;
	}
	RowBuilder* appendInt(u32 value) {
		byte tmp[8];
		if (m_format == KEY_COMPRESS) {
			writeNumberLe(value, tmp, sizeof(value));
			appendCompressedField(tmp, sizeof(value));
		} else {
			writeNumberLe(value, tmp, sizeof(value));
			appendField(tmp, sizeof(value));
		}
		return this;
	}
	RowBuilder* appendFloat(float value) {
		byte tmp[8];
		writeNumberLe(value, tmp, sizeof(value));
		return appendField(tmp, sizeof(value));
	}
	RowBuilder* appendDouble(double value) {
		byte tmp[8];
		writeNumberLe(value, tmp, sizeof(value));
		appendField(tmp, sizeof(value));
		return appendField(&value, sizeof(value));
	}
	RowBuilder* appendBigInt(u64 value) {
		byte tmp[8];
		if (m_format == KEY_COMPRESS) {
			writeNumberLe(value, tmp, sizeof(value));
			appendCompressedField(tmp, sizeof(value));
		} else {
			writeNumberLe(value, tmp, sizeof(value));
			appendField(tmp, sizeof(value));
		}
		return this;
	}
	RowBuilder* appendChar(const char* str, size_t maxsize) {
		size_t slen = strlen(str);
		// 由于要考虑前缀，因此传入的maxsize可能小于slen

		char *tmp = new char[maxsize];
		memset(tmp, ' ', maxsize);
		memcpy(tmp, str, min(maxsize, slen));
		appendField(tmp, maxsize);
		delete[] tmp;
		return this;
	}
	
	RowBuilder* appendChar(const char* str, size_t size, size_t maxsize) {
		
		assert(size <= maxsize);

		char *tmp = new char[maxsize];
		memset(tmp, ' ', maxsize);
		memcpy(tmp, str, min(maxsize, size));
		appendField(tmp, maxsize);
		delete[] tmp;
		return this;
	}

	RowBuilder* appendVarchar(const char* str, size_t maxsize, size_t lenBytes) {
		size_t slen = strlen(str);
		// 记录以及索引键构造的方法都会调用此方法
		// 要考虑前缀索引，maxsize即为前缀加上lenBytes的长度
		slen = min(slen, maxsize - lenBytes);
		assert(m_size + lenBytes + slen <= m_capacity);
		assert(slen + lenBytes <= maxsize);
		byte* ptr = m_buf + m_size;
		size_t bytesWriten = writeNumberLe(static_cast<u32>(slen), ptr, lenBytes);
		assert(bytesWriten == lenBytes);
		ptr += bytesWriten;
		memcpy(ptr, str, slen);
		if (isUppMysqlFormat(m_format) || isRedundantFormat(m_format)) {
			m_size += maxsize;
		} else {
			m_size += slen + lenBytes;
		}
		return this;
	}

	RowBuilder* appendSmallLob(const byte* str, size_t maxsize) {
		size_t lenBytes = 2;
		assert(m_size + lenBytes + sizeof(LobId) <= m_capacity);
		byte *ptr = m_buf + m_size;
		size_t bytesWritten = writeNumberLe(maxsize, ptr, lenBytes);
		assert(bytesWritten == lenBytes);
		ptr += bytesWritten;
		*(byte**)ptr = (byte *)str;
		m_size += 10;
		return this;
	}

	RowBuilder* appendMediumLob(const byte* str, size_t maxsize) {
		size_t lenBytes = 3;
		assert(m_size + lenBytes + sizeof(LobId) <= m_capacity);
		byte *ptr = m_buf + m_size;
		size_t bytesWritten = writeNumberLe(maxsize, ptr, lenBytes);
		assert(bytesWritten == lenBytes);
		ptr += bytesWritten;
		*(byte**)ptr = (byte *)str;
		m_size += 11;
		return this;
	}

	/** 更新整数列,只用于构造Redundant子记录 */
	template<typename T> RowBuilder* writeNumber(u16 colNo, T value) {
		byte tmp[sizeof(value)];
		writeNumberLe(value, tmp, sizeof(value));
		return writeField(colNo, tmp, sizeof(value));
	}
	RowBuilder* writeMediumInt(u16 colNo, u32 value) {
		byte tmp[3];
		writeNumberLe(value, tmp, 3);
		return writeField(colNo, tmp, 3);
	}
	/** 更新Varchar列,只用于构造Redundant子记录 */
	RowBuilder* writeVarchar(u16 colNo, const char* str, size_t maxsize, size_t lenBytes) {
		UNREFERENCED_PARAMETER(maxsize);
		size_t slen = strlen(str);
		assert(m_size + lenBytes + slen <= m_capacity);
		assert(slen + lenBytes <= maxsize);
		assert(colNo < m_tableDef->m_numCols);
		ColumnDef* columnDef = m_tableDef->m_columns[colNo];
		byte* ptr = m_buf + columnDef->m_offset;
		size_t bytesWriten = writeNumberLe(static_cast<u32>(slen), ptr, lenBytes);
		assert(bytesWriten == lenBytes);
		ptr += bytesWriten;
		memcpy(ptr, str, slen);
		return this;
	}
	/** 更新Char列,只用于构造Redundant子记录 */
	RowBuilder* writeChar(u16 colNo, const char* str, size_t maxsize) {
		assert(colNo < m_tableDef->m_numCols);
		size_t slen = strlen(str);
		assert( slen <= maxsize);
		ColumnDef* columnDef = m_tableDef->m_columns[colNo];
		char *tmp = new char[maxsize];
		memset(tmp, ' ', maxsize);
		memcpy(tmp, str, min(maxsize, slen));
		memcpy(m_buf + columnDef->m_offset, tmp, maxsize);
		delete[] tmp;
		return this;
	}

	byte* data() const {
		return m_buf;
	}

	size_t size() const {
		/** REC_REDUNDANT/FIXLEN的记录和子记录长度是一样的，都是m_tableDef->m_maxRecSize */
		return ((m_format == REC_REDUNDANT || m_format == REC_FIXLEN) && m_isSubRecord) ? m_tableDef->m_maxRecSize : m_size;
	}

private:
	RowBuilder* appendField(const void* buf, size_t size) {
		assert(m_size + size <= m_capacity);
		memcpy(m_buf + m_size, buf, size);
		m_size += size;
		return this;
	}

	RowBuilder* appendCompressedField(const void* buf, size_t size) {
		assert(size <= 8);
		byte tmp[9];
		size_t outSize = 0;
		NumberCompressor::compress((byte*)buf, size, tmp, sizeof(tmp), &outSize);
		return appendField(tmp, outSize);
	}

	RowBuilder* writeField(u16 colNo, const void* buf, size_t size) {
		assert(m_format == REC_FIXLEN || m_format == REC_REDUNDANT);
		assert(colNo < m_tableDef->m_numCols);
		ColumnDef* columnDef = m_tableDef->m_columns[colNo];		
		assert(size <= columnDef->m_size);
		memcpy(m_buf + columnDef->m_offset, buf, size);
		return this;
	}

private:
	const TableDef* m_tableDef;
	RecFormat m_format;
	size_t m_capacity;
	size_t m_size;
	bool m_isSubRecord;
	byte *m_buf;
};
//////////////////////////////////////////////////////////////////////////
////RedRecord
//////////////////////////////////////////////////////////////////////////
/**
 * 创建一条冗余记录
 *	nullable的列都设置为NULL
 * @param tableDef 表定义
 * @param format 记录格式
 */
RedRecord::RedRecord(const TableDef* tableDef, RecFormat format)
	: m_tableDef(new TableDef(tableDef)), m_ownMemory(true) {
	m_record = new Record;
	m_record->m_data = new byte [tableDef->m_maxRecSize];
	memset(m_record->m_data, 0, tableDef->m_maxRecSize);
	for (u16 cno = 0; cno < m_tableDef->m_numCols; ++cno) {
		if (m_tableDef->m_columns[cno]->m_nullable)
			setNull(m_tableDef, m_record->m_data, cno);
	}
	m_record->m_size = tableDef->m_maxRecSize;
	m_record->m_rowId = INVALID_ROW_ID;
	m_record->m_format = format;
#ifdef TNT_ENGINE
	m_tblDefOwn = true;
#endif
}
/**
 * 根据已有一条冗余记录
 * @param tableDef 表定义
 * @param format 记录格式
 */
RedRecord::RedRecord(const TableDef *tableDef, Record *record)
	: m_tableDef(new TableDef(tableDef)), m_record(record), m_ownMemory(false) {
	assert(record->m_format == REC_REDUNDANT || record->m_format == REC_MYSQL);
#ifdef TNT_ENGINE
	m_tblDefOwn = true;
#endif
}

#ifdef TNT_ENGINE
RedRecord::RedRecord(TableDef* tableDef, Record *record, bool tblDefOwn)
	: m_record(record), m_ownMemory(false), m_tblDefOwn(tblDefOwn) {
	assert(record->m_format == REC_REDUNDANT || record->m_format == REC_MYSQL);
	if (likely(!m_tblDefOwn)) {
		m_tableDef = tableDef;
	} else {
		m_tableDef = new TableDef(tableDef);
	}
}
#endif

/** 拷贝构造函数 */
RedRecord::RedRecord(const RedRecord& record)
	: m_tableDef(new TableDef(record.m_tableDef)), m_ownMemory(true) {
	m_record = new Record;
	memcpy(m_record, record.getRecord(), sizeof(Record));
	m_record->m_data = new byte [m_tableDef->m_maxRecSize];
	memcpy(m_record->m_data, record.m_record->m_data, m_tableDef->m_maxRecSize);
#ifdef TNT_ENGINE
	m_tblDefOwn = true;
#endif
}

RedRecord::~RedRecord() {
	if (m_ownMemory) {
		if (m_record->m_format == REC_MYSQL)
			freeMysqlRecord(m_tableDef, m_record);
		else
			freeRecord(m_record);
	}
#ifdef TNT_ENGINE
	if (m_tblDefOwn) {
		assert(m_tableDef != NULL);
#endif
		delete m_tableDef;
#ifdef TNT_ENGINE
	}
#endif
}
/**
 * 改写一列
 * @param cno 列号
 * @param data 新数据
 * @param size 数据长度
 * @return this
 */
RedRecord* RedRecord::writeField(u16 cno, const void *data, size_t size) {
	writeField(m_tableDef, m_record->m_data, cno, data, size);
	return this;
}

/**
 * 改写一列
 * @param tableDef 表定义
 * @param cno 列号
 * @param data 新数据
 * @param size 数据长度
 */
void RedRecord::writeField(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(size <= columnDef->m_size);
	if (size == 0) {
		assert(columnDef->m_nullable);
		setNull(tableDef, rec, cno);
	} else {
		memcpy(rec + columnDef->m_offset, data, size);
		setNotNull(tableDef, rec, cno);
	}
}

/**
 * 改写大对象列
 * @param cno 列号
 * @param lob 大对象数据指针
 * @param size 大对象长度
 * @return this
 */
RedRecord* RedRecord::writeLob(u16 cno, const byte *lob, size_t size) {
	writeLob(m_tableDef, m_record->m_data, cno, lob, size);
	return this;
}
/**
 * 改写大对象列
 * @param cno 列号
 * @param lob 大对象数据指针
 * @param size 大对象长度
 * @return this
 */
void RedRecord::writeLob(const TableDef *tableDef, byte *rec, u16 cno, const byte *lob, size_t size) {
	assert(!(!lob && size));
	assert(tableDef->m_numCols > cno);

	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->isLob());
	setNotNull(tableDef, rec, cno);
	*((byte **)(rec + columnDef->m_offset + columnDef->m_size - 8)) = (byte *)lob;
	if (columnDef->m_type == CT_SMALLLOB)
		write2BytesLittleEndian(rec + columnDef->m_offset, (u32)size);
	else
		write3BytesLittleEndian(rec + columnDef->m_offset, (u32)size);
}
/**
 * 改写大对象长度
 * @param tableDef 表定义
 * @param rec 冗余记录
 * @param cno 列号
 * @param size 大对象长度
 * @return this
 */
RedRecord* RedRecord::writeLobSize(u16 cno, size_t size) {
	assert(m_tableDef->m_numCols > cno);
	assert(!isNull(m_tableDef, m_record->m_data, cno));
	ColumnDef *columnDef = m_tableDef->m_columns[cno];
	assert(columnDef->isLob());
	if (columnDef->m_type == CT_SMALLLOB)
		write2BytesLittleEndian(m_record->m_data + columnDef->m_offset, (u32)size);
	else
		write3BytesLittleEndian(m_record->m_data + columnDef->m_offset, (u32)size);
	return this;
}

RedRecord* RedRecord::writeChar(u16 cno, const char *str) {
	return writeChar(cno, str, strlen(str));
}
RedRecord* RedRecord::writeChar(u16 cno, const char *str, size_t size) {
	return writeField(cno, str, size);
}

RedRecord* RedRecord::writeBinary(u16 cno, const byte *str) {
	return writeBinary(cno, str, strlen((const char*)str));
}

RedRecord* RedRecord::writeBinary(u16 cno, const byte *str, size_t size) {
	return writeField(cno, str, size);
}

RedRecord* RedRecord::writeMediumInt(u16 cno, int v) {
	writeMediumInt(m_tableDef, m_record->m_data, cno, v);
	return this;
}

RedRecord* RedRecord::setRowId(RowId rid) {
	m_record->m_rowId = rid;
	return this;
}
RowId RedRecord::getRowId() const {
	return m_record->m_rowId;
}
/**
 * 改为为NULL
 * @param cno 列号
 * @return this
 */
RedRecord* RedRecord::setNull(u16 cno) {
	setNull(m_tableDef, m_record->m_data, cno);
	return this;
}
/**
 * 改为NULL
 * @param tableDef 表定义
 * @param rec 冗余记录
 * @param cno 列号
 */
void RedRecord::setNull(const TableDef *tableDef, byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	if (columnDef->m_nullable) // nullable列
		BitmapOper::setBit(rec, (tableDef->m_maxRecSize << 3), columnDef->m_nullBitmapOffset);
}
/**
 * 改为非NULL
 * @param tableDef 表定义
 * @param rec 冗余记录
 * @param cno 列号
 */
void RedRecord::setNotNull(const TableDef *tableDef, byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	if (columnDef->m_nullable) // nullable列
		BitmapOper::clearBit(rec, (tableDef->m_maxRecSize << 3), columnDef->m_nullBitmapOffset);
}
/**
 * 改写Varchar列
 * @param cno 列号
 * @param str 字符串
 * @return this
 */
RedRecord* RedRecord::writeVarchar(u16 cno, const char *str) {
	writeVarchar(m_tableDef, m_record->m_data, cno, str);
	return this;
}
/**
 * 改写Varchar列
 * @param cno 列号
 * @param str 字符串
 * @param size 字符串长度
 * @return this
 */
RedRecord* RedRecord::writeVarchar(u16 cno, const byte *str, size_t size) {
	writeVarchar(m_tableDef, m_record->m_data, cno, str, size);
	return this;
}
/**
 * 改写Varchar列(静态方法)
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param str 字符串
 */
void RedRecord::writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const char *str) {
	return writeVarchar(tableDef, rec, cno, (byte *)str, strlen(str));
}
/**
 * 改写Varchar列(静态方法)
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param str 字符串
 * @param size 字符串长度
 */
void RedRecord::writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size) {
	assert(str);
	
	u32 slen = (u32)size;
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_VARCHAR || columnDef->m_type == CT_VARBINARY);
	assert(slen + columnDef->m_lenBytes <= columnDef->m_size);

	byte* ptr = rec + columnDef->m_offset;
	// 写入列长度
	size_t bytesWriten = writeNumberLe(slen, ptr, columnDef->m_lenBytes);
	assert(bytesWriten == columnDef->m_lenBytes);
	ptr += bytesWriten;
	// 写入列数据
	memcpy(ptr, str, slen);
	setNotNull(tableDef, rec, cno);
}
/**
 * 改写Varchar列长度
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param size 新长度
 */
void RedRecord::writeVarcharLen(const TableDef *tableDef, byte *rec, u16 cno, size_t size) {
	assert(tableDef->m_numCols > cno);
	assert(!isNull(tableDef, rec, cno));
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_VARCHAR);
	assert(size && size + columnDef->m_lenBytes <= columnDef->m_size);
	byte* ptr = rec + columnDef->m_offset;
	// 写入列长度
	size_t bytesWriten = writeNumberLe((u32)size, ptr, columnDef->m_lenBytes);
	UNREFERENCED_PARAMETER(bytesWriten);
	assert(bytesWriten == columnDef->m_lenBytes);
	setNotNull(tableDef, rec, cno);
}
/**
 * 判断列是否为Null
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @return 是否为null
 */
bool RedRecord::isNull(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	if (!columnDef->m_nullable)
		return false;
	return BitmapOper::isSet(rec, (tableDef->m_maxRecSize << 3), columnDef->m_nullBitmapOffset);
}
/**
 * 转化成Record
 */
const Record* RedRecord::getRecord() const {
	return m_record;
}


/**
 * 读取varchar列的值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针，可以为NULL
 * @param size 输出参数，列长度
 * @return 列数据指针
 */
void* RedRecord::readVarchar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_VARCHAR || columnDef->m_type == CT_VARBINARY);
	void *ptr = 0;
	size_t len = 0;
	if (isNull(tableDef, rec, cno)) {
		ptr = 0;
		len = 0;
	} else {
		ptr = (void *)(rec + columnDef->m_offset + columnDef->m_lenBytes);
		if (columnDef->m_lenBytes == 1) {
			len = (size_t)*(byte *)(rec + columnDef->m_offset);
		} else {
			len = (size_t)read2BytesLittleEndian(rec + columnDef->m_offset);
		}
	}
	if (data)
		*data = ptr;
	if (size)
		*size = len;
	return ptr;
}

/**
 * 读取lob列的值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param lob 输出参数，列内存指针
 * @param size 输出参数，列长度
 */
void RedRecord::readLob(const TableDef *tableDef, const byte *rec, u16 cno, void **lob, size_t *size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_SMALLLOB || columnDef->m_type == CT_MEDIUMLOB);

	if (isNull(tableDef, rec, cno)) {
		*lob = 0;
		*size = 0;
	} else {
		*lob = *((void **)(rec + columnDef->m_offset + columnDef->m_size - 8));
		if (columnDef->m_type == CT_SMALLLOB)
			*size = (size_t)read2BytesLittleEndian(rec + columnDef->m_offset);
		else
			*size = (size_t)read3BytesLittleEndian(rec + columnDef->m_offset);
	}
}
/**
 * 读取Char列值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针
 * @param size 输出参数，列长度
 * @return 列数据指针
 */
void* RedRecord::readChar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	assert(tableDef->m_numCols > cno);
	assert(tableDef->m_columns[cno]->m_type == CT_CHAR);
	return readField(tableDef, rec, cno, data, size);
}
/**
 * 读取某列值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针，可以为NULL
 * @param size 输出参数，列长度，可以为NULL
 * @return 列数据指针
 */
void* RedRecord::readField(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	void *ptr;
	size_t len;
	if (isNull(tableDef, rec, cno)) {
		ptr = 0;
		len = 0;
	} else {
		ptr = (void *)(columnDef->m_offset + rec);
		len = columnDef->m_size;
	}
	if (data)
		*data = ptr;
	if (size)
		*size = len;
	return ptr;
}
/**
 * 读取Bigint列
 * @param cno 列号
 * @return 该列的值
 */
s64 RedRecord::readBigInt(u16 cno) {
	return readBigInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取Bigint列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s64 RedRecord::readBigInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_BIGINT);
	assert(!columnDef->m_nullable || !BitmapOper::isSet(rec, (tableDef->m_maxRecSize << 3), columnDef->m_nullBitmapOffset));
	return *(s64 *)(columnDef->m_offset + rec);
}
/**
 * 读取int列
 * @param cno 列号
 * @return 该列的值
 */
s32 RedRecord::readInt(u16 cno) {
	return readInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取Bigint列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s32 RedRecord::readInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_INT);
	assert(!columnDef->m_nullable || !BitmapOper::isSet(rec, (tableDef->m_maxRecSize << 3), columnDef->m_nullBitmapOffset));
	return *(s32 *)(columnDef->m_offset + rec);
}
/**
 * 读取small列
 * @param cno 列号
 * @return 该列的值
 */
s16 RedRecord::readSmallInt(u16 cno) {
	return readSmallInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取small列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s16 RedRecord::readSmallInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_SMALLINT);
	assert(!isNull(tableDef, rec, cno));
	return *(s16 *)(columnDef->m_offset + rec);
}

/**
 * 读取tiny列
 * @param cno 列号
 * @return 该列的值
 */
s8 RedRecord::readTinyInt(u16 cno) {
	return readTinyInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取tiny列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s8 RedRecord::readTinyInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_TINYINT);
	assert(!isNull(tableDef, rec, cno));
	return *(s8 *)(columnDef->m_offset + rec);
}
/** 读取MediumInt列
 * @param cno	列号
 * @return 该列的值，用32位表示
 */
s32 RedRecord::readMediumInt( u16 cno ) {
	return readMediumInt(m_tableDef, m_record->m_data, cno);
}

#ifdef LITTLE_ENDIAN
/** 3字节整数转化成主机字节序整数 */
static inline int leS24ToHostInt(const s8 *src) {
        int v = (int)*(src + 2) << 16;
        s8 *p = (s8 *)&v;
        *p = *src;
        *(p + 1) = *(src + 1);
        return v;
}
#endif
/** 读取MediumInt列
 * @param tableDef	表定义
 * @param rec		待读取记录
 * @param cno		列号
 * @return 该列的值，用32位表示
 */
s32 RedRecord::readMediumInt( const TableDef *tableDef, const byte *rec, u16 cno ) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_MEDIUMINT);
	assert(!isNull(tableDef, rec, cno));
	return leS24ToHostInt((s8 *)(columnDef->m_offset + rec));
}
/** 读取Float列
 * @param cno	列号
 * @return 该列的值
 */
float RedRecord::readFloat( u16 cno ) {
	return readFloat(m_tableDef, m_record->m_data, cno);
}
/** 读取Float列
 * @param tableDef	表定义
 * @param rec		待读取记录
 * @param cno		列号
 * @return 该列的值，用32位表示
 */
float RedRecord::readFloat(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_FLOAT);
	assert(!isNull(tableDef, rec, cno));
	return *(float*)(columnDef->m_offset + rec);
}
/** 读取Double列
 * @param cno	列号
 * @return 该列的值
 */
double RedRecord::readDouble( u16 cno ) {
	return readDouble(m_tableDef, m_record->m_data, cno);
}
/** 读取Double列
 * @param tableDef	表定义
 * @param rec		待读取记录
 * @param cno		列号
 * @return 该列的值，用32位表示
 */
double RedRecord::readDouble( const TableDef *tableDef, const byte *rec, u16 cno ) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_DOUBLE);
	assert(!isNull(tableDef, rec, cno));
	return *(double*)(columnDef->m_offset + rec);
}
/**
 * 改写Char列, 用空格补全
 * @param tableDef 表定义
 * @param rec 待修改记录
 * @param cno 列号
 * @param str 新数据,'\0'结尾
 */
void RedRecord::writeChar(const TableDef *tableDef, byte *rec, u16 cno, const char *str) {
	return writeChar(tableDef, rec, cno, (byte *)str, strlen(str));
}
/**
 * 改写Char列, 用空格补全
 * @param tableDef 表定义
 * @param rec 待修改记录
 * @param cno 列号
 * @param str 新数据
 * @param size 数据长度
 */
void RedRecord::writeChar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_CHAR);
	assert(columnDef->m_size >= size);
	void *data = alloca(columnDef->m_size);
	memcpy(data, str, size);
	memset((byte *)data + size, ' ', columnDef->m_size - size);
	return writeRaw(tableDef, rec, cno, data, columnDef->m_size);
}
/**
 * 改写MediumInt列
 * @param tableDef 表定义
 * @param rec 待修改记录
 * @param cno 列号
 * @param v 待写入数值
 */
void RedRecord::writeMediumInt(const TableDef *tableDef, byte *rec, u16 cno, int v) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_MEDIUMINT);
	byte tmp[8];
	writeNumberLe(v, tmp, 3);
	return writeRaw(tableDef, rec, cno, tmp, columnDef->m_size);
}


/**
 * 改写一列
 * @param tableDef 表定义
 * @param cno 列号
 * @param data 新数据
 * @param size 数据长度
 */
void RedRecord::writeRaw(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size) {
	return writeField(tableDef, rec, cno, data, size);
}
/**
 * 读取某列值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针，可以为NULL
 * @param size 输出参数，列长度，可以为NULL
 * @return 列数据指针
 */
void* RedRecord::readRaw(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	return readField(tableDef, rec, cno, data, size);
}

//////////////////////////////////////////////////////////////////////////
////UppMysqlRecord
//////////////////////////////////////////////////////////////////////////
/**
 * 创建一条上层Mysql记录
 *	nullable的列都设置为NULL
 * @param tableDef 表定义
 * @param format 记录格式
 */
UppMysqlRecord::UppMysqlRecord(const TableDef* tableDef, RecFormat format)
	: m_tableDef(new TableDef(tableDef)), m_ownMemory(true) {
	m_record = new Record;
	m_record->m_data = new byte [tableDef->m_maxMysqlRecSize];
	memset(m_record->m_data, 0, tableDef->m_maxMysqlRecSize);
	for (u16 cno = 0; cno < m_tableDef->m_numCols; ++cno) {
		if (m_tableDef->m_columns[cno]->m_nullable)
			setNull(m_tableDef, m_record->m_data, cno);
	}
	m_record->m_size = tableDef->m_maxMysqlRecSize;
	m_record->m_rowId = INVALID_ROW_ID;
	m_record->m_format = format;
#ifdef TNT_ENGINE
	m_tblDefOwn = true;
#endif
}
/**
 * 根据已有一条冗余记录
 * @param tableDef 表定义
 * @param format 记录格式
 */
UppMysqlRecord::UppMysqlRecord(const TableDef *tableDef, Record *record)
	: m_tableDef(new TableDef(tableDef)), m_record(record), m_ownMemory(false) {
	assert(record->m_format == REC_UPPMYSQL);
#ifdef TNT_ENGINE
	m_tblDefOwn = true;
#endif
}

#ifdef TNT_ENGINE
UppMysqlRecord::UppMysqlRecord(TableDef* tableDef, Record *record, bool tblDefOwn)
	: m_record(record), m_ownMemory(false), m_tblDefOwn(tblDefOwn) {
	assert(record->m_format == REC_REDUNDANT || record->m_format == REC_MYSQL);
	if (likely(!m_tblDefOwn)) {
		m_tableDef = tableDef;
	} else {
		m_tableDef = new TableDef(tableDef);
	}
}
#endif

/** 拷贝构造函数 */
UppMysqlRecord::UppMysqlRecord(const UppMysqlRecord& record)
	: m_tableDef(new TableDef(record.m_tableDef)), m_ownMemory(true) {
	m_record = new Record;
	memcpy(m_record, record.getRecord(), sizeof(Record));
	m_record->m_data = new byte [m_tableDef->m_maxMysqlRecSize];
	memcpy(m_record->m_data, record.m_record->m_data, m_tableDef->m_maxMysqlRecSize);
#ifdef TNT_ENGINE
	m_tblDefOwn = true;
#endif
}

UppMysqlRecord::~UppMysqlRecord() {
	if (m_ownMemory) {
		freeUppMysqlRecord(m_tableDef, m_record);
	}
#ifdef TNT_ENGINE
	if (m_tblDefOwn) {
		assert(m_tableDef != NULL);
#endif
		delete m_tableDef;
#ifdef TNT_ENGINE
	}
#endif
}
/**
 * 改写一列
 * @param cno 列号
 * @param data 新数据
 * @param size 数据长度
 * @return this
 */
UppMysqlRecord* UppMysqlRecord::writeField(u16 cno, const void *data, size_t size) {
	writeField(m_tableDef, m_record->m_data, cno, data, size);
	return this;
}

/**
 * 改写一列
 * @param tableDef 表定义
 * @param cno 列号
 * @param data 新数据
 * @param size 数据长度
 */
void UppMysqlRecord::writeField(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(size <= columnDef->m_mysqlSize);
	if (size == 0) {
		assert(columnDef->m_nullable);
		setNull(tableDef, rec, cno);
	} else {
		memcpy(rec + columnDef->m_mysqlOffset, data, size);
		setNotNull(tableDef, rec, cno);
	}
}

/**
 * 改写大对象列
 * @param cno 列号
 * @param lob 大对象数据指针
 * @param size 大对象长度
 * @return this
 */
UppMysqlRecord* UppMysqlRecord::writeLob(u16 cno, const byte *lob, size_t size) {
	writeLob(m_tableDef, m_record->m_data, cno, lob, size);
	return this;
}
/**
 * 改写大对象列
 * @param cno 列号
 * @param lob 大对象数据指针
 * @param size 大对象长度
 * @return this
 */
void UppMysqlRecord::writeLob(const TableDef *tableDef, byte *rec, u16 cno, const byte *lob, size_t size) {
	assert(!(!lob && size));
	assert(tableDef->m_numCols > cno);

	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->isLob());
	setNotNull(tableDef, rec, cno);
	*((byte **)(rec + columnDef->m_mysqlOffset + columnDef->m_mysqlSize - 8)) = (byte *)lob;
	if (columnDef->m_type == CT_SMALLLOB)
		write2BytesLittleEndian(rec + columnDef->m_mysqlOffset, (u32)size);
	else
		write3BytesLittleEndian(rec + columnDef->m_mysqlOffset, (u32)size);
}
/**
 * 改写大对象长度
 * @param tableDef 表定义
 * @param rec 冗余记录
 * @param cno 列号
 * @param size 大对象长度
 * @return this
 */
UppMysqlRecord* UppMysqlRecord::writeLobSize(u16 cno, size_t size) {
	assert(m_tableDef->m_numCols > cno);
	assert(!isNull(m_tableDef, m_record->m_data, cno));
	ColumnDef *columnDef = m_tableDef->m_columns[cno];
	assert(columnDef->isLob());
	if (columnDef->m_type == CT_SMALLLOB)
		write2BytesLittleEndian(m_record->m_data + columnDef->m_mysqlOffset, (u32)size);
	else
		write3BytesLittleEndian(m_record->m_data + columnDef->m_mysqlOffset, (u32)size);
	return this;
}

UppMysqlRecord* UppMysqlRecord::writeChar(u16 cno, const char *str) {
	return writeChar(cno, str, strlen(str));
}
UppMysqlRecord* UppMysqlRecord::writeChar(u16 cno, const char *str, size_t size) {
	return writeField(cno, str, size);
}

UppMysqlRecord* UppMysqlRecord::writeBinary(u16 cno, const byte *str) {
	return writeBinary(cno, str, strlen((const char*)str));
}

UppMysqlRecord* UppMysqlRecord::writeBinary(u16 cno, const byte *str, size_t size) {
	return writeField(cno, str, size);
}

UppMysqlRecord* UppMysqlRecord::writeMediumInt(u16 cno, int v) {
	writeMediumInt(m_tableDef, m_record->m_data, cno, v);
	return this;
}

UppMysqlRecord* UppMysqlRecord::setRowId(RowId rid) {
	m_record->m_rowId = rid;
	return this;
}
RowId UppMysqlRecord::getRowId() const {
	return m_record->m_rowId;
}
/**
 * 改为为NULL
 * @param cno 列号
 * @return this
 */
UppMysqlRecord* UppMysqlRecord::setNull(u16 cno) {
	setNull(m_tableDef, m_record->m_data, cno);
	return this;
}
/**
 * 改为NULL
 * @param tableDef 表定义
 * @param rec 冗余记录
 * @param cno 列号
 */
void UppMysqlRecord::setNull(const TableDef *tableDef, byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	if (columnDef->m_nullable) // nullable列
		BitmapOper::setBit(rec, (tableDef->m_maxMysqlRecSize << 3), columnDef->m_nullBitmapOffset);
}
/**
 * 改为非NULL
 * @param tableDef 表定义
 * @param rec 冗余记录
 * @param cno 列号
 */
void UppMysqlRecord::setNotNull(const TableDef *tableDef, byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	if (columnDef->m_nullable) // nullable列
		BitmapOper::clearBit(rec, (tableDef->m_maxMysqlRecSize << 3), columnDef->m_nullBitmapOffset);
}
/**
 * 改写Varchar列
 * @param cno 列号
 * @param str 字符串
 * @return this
 */
UppMysqlRecord* UppMysqlRecord::writeVarchar(u16 cno, const char *str) {
	writeVarchar(m_tableDef, m_record->m_data, cno, str);
	return this;
}
/**
 * 改写Varchar列
 * @param cno 列号
 * @param str 字符串
 * @param size 字符串长度
 * @return this
 */
UppMysqlRecord* UppMysqlRecord::writeVarchar(u16 cno, const byte *str, size_t size) {
	writeVarchar(m_tableDef, m_record->m_data, cno, str, size);
	return this;
}
/**
 * 改写Varchar列(静态方法)
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param str 字符串
 */
void UppMysqlRecord::writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const char *str) {
	return writeVarchar(tableDef, rec, cno, (byte *)str, strlen(str));
}
/**
 * 改写Varchar列(静态方法)
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param str 字符串
 * @param size 字符串长度
 */
void UppMysqlRecord::writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size) {
	assert(str);
	
	u32 slen = (u32)size;
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_VARCHAR || columnDef->m_type == CT_VARBINARY);
	assert(slen + columnDef->m_lenBytes <= columnDef->m_size);

	byte* ptr = rec + columnDef->m_mysqlOffset;
	// 写入列长度
	size_t bytesWriten = writeNumberLe(slen, ptr, columnDef->m_lenBytes);
	assert(bytesWriten == columnDef->m_lenBytes);
	ptr += bytesWriten;
	// 写入列数据
	memcpy(ptr, str, slen);
	setNotNull(tableDef, rec, cno);
}
/**
 * 改写Varchar列长度
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param size 新长度
 */
void UppMysqlRecord::writeVarcharLen(const TableDef *tableDef, byte *rec, u16 cno, size_t size) {
	assert(tableDef->m_numCols > cno);
	assert(!isNull(tableDef, rec, cno));
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_VARCHAR);
	assert(size && size + columnDef->m_lenBytes <= columnDef->m_mysqlSize);
	byte* ptr = rec + columnDef->m_mysqlOffset;
	// 写入列长度
	size_t bytesWriten = writeNumberLe((u32)size, ptr, columnDef->m_lenBytes);
	UNREFERENCED_PARAMETER(bytesWriten);
	assert(bytesWriten == columnDef->m_lenBytes);
	setNotNull(tableDef, rec, cno);
}
/**
 * 判断列是否为Null
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @return 是否为null
 */
bool UppMysqlRecord::isNull(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	if (!columnDef->m_nullable)
		return false;
	return BitmapOper::isSet(rec, (tableDef->m_maxMysqlRecSize << 3), columnDef->m_nullBitmapOffset);
}
/**
 * 转化成Record
 */
const Record* UppMysqlRecord::getRecord() const {
	return m_record;
}


/**
 * 读取varchar列的值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针，可以为NULL
 * @param size 输出参数，列长度
 * @return 列数据指针
 */
void* UppMysqlRecord::readVarchar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_VARCHAR || columnDef->m_type == CT_VARBINARY);
	void *ptr = 0;
	size_t len = 0;
	if (isNull(tableDef, rec, cno)) {
		ptr = 0;
		len = 0;
	} else {
		ptr = (void *)(rec + columnDef->m_mysqlOffset + columnDef->m_lenBytes);
		if (columnDef->m_lenBytes == 1) {
			len = (size_t)*(byte *)(rec + columnDef->m_mysqlOffset);
		} else {
			len = (size_t)read2BytesLittleEndian(rec + columnDef->m_mysqlOffset);
		}
	}
	if (data)
		*data = ptr;
	if (size)
		*size = len;
	return ptr;
}

/**
 * 读取lob列的值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param lob 输出参数，列内存指针
 * @param size 输出参数，列长度
 */
void UppMysqlRecord::readLob(const TableDef *tableDef, const byte *rec, u16 cno, void **lob, size_t *size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_SMALLLOB || columnDef->m_type == CT_MEDIUMLOB);

	if (isNull(tableDef, rec, cno)) {
		*lob = 0;
		*size = 0;
	} else {
		*lob = *((void **)(rec + columnDef->m_mysqlOffset + columnDef->m_mysqlSize - 8));
		if (columnDef->m_type == CT_SMALLLOB)
			*size = (size_t)read2BytesLittleEndian(rec + columnDef->m_mysqlOffset);
		else
			*size = (size_t)read3BytesLittleEndian(rec + columnDef->m_mysqlOffset);
	}
}
/**
 * 读取Char列值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针
 * @param size 输出参数，列长度
 * @return 列数据指针
 */
void* UppMysqlRecord::readChar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	assert(tableDef->m_numCols > cno);
	assert(tableDef->m_columns[cno]->m_type == CT_CHAR);
	return readField(tableDef, rec, cno, data, size);
}
/**
 * 读取某列值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针，可以为NULL
 * @param size 输出参数，列长度，可以为NULL
 * @return 列数据指针
 */
void* UppMysqlRecord::readField(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	void *ptr;
	size_t len;
	if (isNull(tableDef, rec, cno)) {
		ptr = 0;
		len = 0;
	} else {
		ptr = (void *)(columnDef->m_mysqlOffset + rec);
		len = columnDef->m_mysqlSize;
	}
	if (data)
		*data = ptr;
	if (size)
		*size = len;
	return ptr;
}
/**
 * 读取Bigint列
 * @param cno 列号
 * @return 该列的值
 */
s64 UppMysqlRecord::readBigInt(u16 cno) {
	return readBigInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取Bigint列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s64 UppMysqlRecord::readBigInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_BIGINT);
	assert(!columnDef->m_nullable || !BitmapOper::isSet(rec, (tableDef->m_maxMysqlRecSize << 3), columnDef->m_nullBitmapOffset));
	return *(s64 *)(columnDef->m_mysqlOffset + rec);
}
/**
 * 读取int列
 * @param cno 列号
 * @return 该列的值
 */
s32 UppMysqlRecord::readInt(u16 cno) {
	return readInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取Bigint列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s32 UppMysqlRecord::readInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_INT);
	assert(!columnDef->m_nullable || !BitmapOper::isSet(rec, (tableDef->m_maxMysqlRecSize << 3), columnDef->m_nullBitmapOffset));
	return *(s32 *)(columnDef->m_mysqlOffset + rec);
}
/**
 * 读取small列
 * @param cno 列号
 * @return 该列的值
 */
s16 UppMysqlRecord::readSmallInt(u16 cno) {
	return readSmallInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取small列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s16 UppMysqlRecord::readSmallInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_SMALLINT);
	assert(!isNull(tableDef, rec, cno));
	return *(s16 *)(columnDef->m_mysqlOffset + rec);
}

/**
 * 读取tiny列
 * @param cno 列号
 * @return 该列的值
 */
s8 UppMysqlRecord::readTinyInt(u16 cno) {
	return readTinyInt(m_tableDef, m_record->m_data, cno);
}
/**
 * 读取tiny列
 * @param tableDef 表定义
 * @param rec 待读取记录
 * @param cno 列号
 * @return 该列的值
 */
s8 UppMysqlRecord::readTinyInt(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_TINYINT);
	assert(!isNull(tableDef, rec, cno));
	return *(s8 *)(columnDef->m_mysqlOffset + rec);
}
/** 读取MediumInt列
 * @param cno	列号
 * @return 该列的值，用32位表示
 */
s32 UppMysqlRecord::readMediumInt( u16 cno ) {
	return readMediumInt(m_tableDef, m_record->m_data, cno);
}

/** 读取MediumInt列
 * @param tableDef	表定义
 * @param rec		待读取记录
 * @param cno		列号
 * @return 该列的值，用32位表示
 */
s32 UppMysqlRecord::readMediumInt( const TableDef *tableDef, const byte *rec, u16 cno ) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_MEDIUMINT);
	assert(!isNull(tableDef, rec, cno));
	return leS24ToHostInt((s8 *)(columnDef->m_mysqlOffset + rec));
}
/** 读取Float列
 * @param cno	列号
 * @return 该列的值
 */
float UppMysqlRecord::readFloat( u16 cno ) {
	return readFloat(m_tableDef, m_record->m_data, cno);
}
/** 读取Float列
 * @param tableDef	表定义
 * @param rec		待读取记录
 * @param cno		列号
 * @return 该列的值，用32位表示
 */
float UppMysqlRecord::readFloat(const TableDef *tableDef, const byte *rec, u16 cno) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_FLOAT);
	assert(!isNull(tableDef, rec, cno));
	return *(float*)(columnDef->m_mysqlOffset + rec);
}
/** 读取Double列
 * @param cno	列号
 * @return 该列的值
 */
double UppMysqlRecord::readDouble( u16 cno ) {
	return readDouble(m_tableDef, m_record->m_data, cno);
}
/** 读取Double列
 * @param tableDef	表定义
 * @param rec		待读取记录
 * @param cno		列号
 * @return 该列的值，用32位表示
 */
double UppMysqlRecord::readDouble( const TableDef *tableDef, const byte *rec, u16 cno ) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_DOUBLE);
	assert(!isNull(tableDef, rec, cno));
	return *(double*)(columnDef->m_mysqlOffset + rec);
}
/**
 * 改写Char列, 用空格补全
 * @param tableDef 表定义
 * @param rec 待修改记录
 * @param cno 列号
 * @param str 新数据,'\0'结尾
 */
void UppMysqlRecord::writeChar(const TableDef *tableDef, byte *rec, u16 cno, const char *str) {
	return writeChar(tableDef, rec, cno, (byte *)str, strlen(str));
}
/**
 * 改写Char列, 用空格补全
 * @param tableDef 表定义
 * @param rec 待修改记录
 * @param cno 列号
 * @param str 新数据
 * @param size 数据长度
 */
void UppMysqlRecord::writeChar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_CHAR);
	assert(columnDef->m_mysqlSize >= size);
	void *data = alloca(columnDef->m_mysqlSize);
	memcpy(data, str, size);
	memset((byte *)data + size, ' ', columnDef->m_mysqlSize - size);
	return writeRaw(tableDef, rec, cno, data, columnDef->m_mysqlSize);
}
/**
 * 改写MediumInt列
 * @param tableDef 表定义
 * @param rec 待修改记录
 * @param cno 列号
 * @param v 待写入数值
 */
void UppMysqlRecord::writeMediumInt(const TableDef *tableDef, byte *rec, u16 cno, int v) {
	assert(tableDef->m_numCols > cno);
	ColumnDef *columnDef = tableDef->m_columns[cno];
	assert(columnDef->m_type == CT_MEDIUMINT);
	byte tmp[8];
	writeNumberLe(v, tmp, 3);
	return writeRaw(tableDef, rec, cno, tmp, columnDef->m_mysqlSize);
}


/**
 * 改写一列
 * @param tableDef 表定义
 * @param cno 列号
 * @param data 新数据
 * @param size 数据长度
 */
void UppMysqlRecord::writeRaw(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size) {
	return writeField(tableDef, rec, cno, data, size);
}
/**
 * 读取某列值
 * @param tableDef 表定义
 * @param cno 列号
 * @param rec 冗余记录
 * @param data 输出参数，列内存指针，可以为NULL
 * @param size 输出参数，列长度，可以为NULL
 * @return 列数据指针
 */
void* UppMysqlRecord::readRaw(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size) {
	return readField(tableDef, rec, cno, data, size);
}

//////////////////////////////////////////////////////////////////////////
////RecordBuilder
//////////////////////////////////////////////////////////////////////////

RecordBuilder::RecordBuilder(const TableDef *tableDef, RowId rowId, RecFormat format) 
	: m_tableDef(tableDef), m_rowId(rowId), m_format(format), m_curColumnDef(NULL) {
		
	size_t bmBytes = getBitmapSize(tableDef->m_recFormat, tableDef->m_nullableCols);

	size_t capacity = isUppMysqlFormat(format) ? tableDef->m_maxMysqlRecSize : tableDef->m_maxRecSize;
	m_builder  = new RowBuilder(tableDef, format, capacity, bmBytes);

	m_bitmap = new Bitmap(m_builder->data(), bmBytes*8); 
	if (m_format == REC_COMPRESSORDER) {
		assert(tableDef->m_numColGrps > 0);
		m_colGrpSegmentSizes = new size_t[tableDef->m_numColGrps];
		memset(m_colGrpSegmentSizes, 0, tableDef->m_numColGrps * sizeof(size_t));
	} else {
		m_colGrpSegmentSizes = NULL;
	}
}


RecordBuilder::~RecordBuilder() {
	delete m_bitmap;
	delete m_builder;
	delete []m_colGrpSegmentSizes;
}

void RecordBuilder::moveToNextCol() {
	if (m_format == REC_COMPRESSORDER) {
		if (!m_curColumnDef) {
			u16 colNo = m_tableDef->m_colGrps[0]->m_colNos[0];
			m_curColumnDef = m_tableDef->m_columns[colNo];
		} else {
			u8 colGrpNo = m_curColumnDef->m_colGrpNo;
			u16  colGrpOffset = m_curColumnDef->m_colGrpOffset + 1;
			if (colGrpOffset >= m_tableDef->m_colGrps[colGrpNo]->m_numCols) {
				colGrpOffset = 0;
				colGrpNo++;
				assert(colGrpNo < m_tableDef->m_numColGrps);
			}
			u16 colNo = m_tableDef->m_colGrps[colGrpNo]->m_colNos[colGrpOffset];
			m_curColumnDef = m_tableDef->m_columns[colNo];
		}
	} else {
		u16 colNo = m_curColumnDef ? (m_curColumnDef->m_no + 1) : 0;
		assert(colNo < m_tableDef->m_numCols);
		m_curColumnDef = m_tableDef->m_columns[colNo];
	}
}

void RecordBuilder::updateBitmap(bool isNull) {
	if (m_curColumnDef->m_nullable) {
		u16 offset = m_curColumnDef->m_nullBitmapOffset;
		isNull ? m_bitmap->setBit(offset) : m_bitmap->clearBit(offset);	
	} else {
		assert(!isNull);
	}
}

RecordBuilder* RecordBuilder::appendNull() {
	moveToNextCol();
	if (isUppMysqlFormat(m_format)) {
		m_builder->appendNull(m_curColumnDef->m_mysqlSize);
	} else if (isRedundantFormat(m_format)) {
		// 这两种类型，NULL列也占用空间
		m_builder->appendNull(m_curColumnDef->m_size);
	}
	updateBitmap(true);
	return this;
}


RecordBuilder* RecordBuilder::appendTinyInt(u8 value) {
	moveToNextCol();
	assert(m_curColumnDef->m_type == CT_TINYINT);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	m_builder->appendTinyInt(value);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += sizeof(value);
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendSmallInt(u16 value) {
	moveToNextCol();
	assert(m_curColumnDef->m_type == CT_SMALLINT);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	m_builder->appendSmallInt(value);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += sizeof(value);
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendMediumInt(u32 value) {
	moveToNextCol();
	assert(m_curColumnDef->m_type == CT_MEDIUMINT);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	m_builder->appendMediumInt(value);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += 3;
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendInt(u32 value) {
	moveToNextCol();
	assert(m_curColumnDef->m_type == CT_INT);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	m_builder->appendInt(value);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += sizeof(value);
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendBigInt(u64 value) {
	moveToNextCol();
	assert(m_curColumnDef->m_type == CT_BIGINT);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	m_builder->appendBigInt(value);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += sizeof(value);
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendFloat(float value) {
	moveToNextCol();
	assert(m_curColumnDef->m_type == CT_FLOAT);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	m_builder->appendFloat(value);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += sizeof(value);
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendDouble(double value) {
	moveToNextCol();
	assert(m_curColumnDef->m_type == CT_DOUBLE);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	m_builder->appendDouble(value);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += sizeof(value);
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendChar(const char* str) {
	moveToNextCol();
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	assert(m_curColumnDef->m_type == CT_CHAR);
	m_builder->appendChar(str, m_curColumnDef->m_size);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += m_curColumnDef->m_size;
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendChar(const char* str, size_t size) {
	moveToNextCol();
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	assert(m_curColumnDef->m_type == CT_CHAR);
	m_builder->appendChar(str, size, m_curColumnDef->m_size);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += m_curColumnDef->m_size;
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendVarchar(const char* str) {
	moveToNextCol();
	// 如果上层是varchar格式，底层是lob格式，那么在构造上层记录时仍然会使用此方法来构造
	assert(m_curColumnDef ->m_type == CT_VARCHAR || m_curColumnDef->m_type == CT_SMALLLOB);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	size_t lenBytes = m_curColumnDef->m_lenBytes;
	if(m_curColumnDef->m_type == CT_SMALLLOB) {
		// 如果是超长字段，那么调用appendVarchar只可能是用于构建REC_UPPMYSQL格式的记录
		assert(m_format == REC_UPPMYSQL);
		lenBytes = 2;	
	}
	m_builder->appendVarchar(str, m_curColumnDef->m_mysqlSize, lenBytes);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += m_curColumnDef->m_lenBytes + strlen(str);
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendSmallLob(const byte* str) {
	moveToNextCol();
	assert(m_curColumnDef ->m_type == CT_SMALLLOB);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	size_t size = 0;
	if (m_format == REC_MYSQL || m_format == REC_UPPMYSQL)
		size = strlen((const char*)str);
	m_builder->appendSmallLob(str, size);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += 10;
	updateBitmap(false);
	return this;
}

RecordBuilder* RecordBuilder::appendMediumLob(const byte* str) {
	moveToNextCol();
	assert(m_curColumnDef ->m_type == CT_MEDIUMLOB);
	assert(m_format == REC_VARLEN || m_format == REC_COMPRESSORDER ||
		m_builder->size() == m_curColumnDef->m_offset || m_builder->size() == m_curColumnDef->m_mysqlOffset);
	size_t size = 0;
	if (m_format == REC_MYSQL || m_format == REC_UPPMYSQL)
		size = strlen((const char*)str);
	m_builder->appendMediumLob(str, size);
	if (m_colGrpSegmentSizes)
		m_colGrpSegmentSizes[m_curColumnDef->m_colGrpNo] += 11;
	updateBitmap(false);
	return this;
}

Record* RecordBuilder::getRecord(size_t size) {
	size_t capacity = size ? size : m_builder->size();
	assert(capacity >= m_builder->size());

	Record* r = new Record();
	r->m_format = m_format;
	r->m_size = (uint)m_builder->size();
	r->m_rowId = m_rowId;
	r->m_data = new byte[capacity];
	memcpy(r->m_data, m_builder->data(), r->m_size);

	return r;
}

CompressOrderRecord *RecordBuilder::getCompressOrderRecord(size_t size) {
	assert(m_format == REC_COMPRESSORDER);
	size_t capacity = size ? size : m_builder->size();
	assert(capacity >= m_builder->size());

	CompressOrderRecord* r = new CompressOrderRecord();
	if (m_colGrpSegmentSizes) {
		r->m_numSeg = m_tableDef->m_numColGrps;
		r->m_segSizes = new size_t[m_tableDef->m_numColGrps];
		memcpy(r->m_segSizes, m_colGrpSegmentSizes, sizeof(size_t) * m_tableDef->m_numColGrps);
	}
	r->m_format = m_format;
	r->m_size = (uint)m_builder->size();
	r->m_rowId = m_rowId;
	r->m_data = new byte[capacity];
	memcpy(r->m_data, m_builder->data(), r->m_size);

	return r;
}

Record* RecordBuilder::createEmptyRecord(RowId rowId, RecFormat format, size_t size) {
	Record* r = new Record();
	r->m_rowId = rowId;
	r->m_format = format;
	r->m_size = (uint)size;
	r->m_data = new byte[r->m_size];
	memset(r->m_data, 0, r->m_size);
	return r;
}

CompressOrderRecord * RecordBuilder::createEmptCompressOrderRcd(RowId rowId, size_t size, u8 colGrpSize) {
	CompressOrderRecord* r = new CompressOrderRecord();
	r->m_rowId = rowId;
	r->m_size = (uint)size;
	r->m_data = new byte[r->m_size];
	r->m_numSeg = colGrpSize;
	r->m_segSizes = new size_t[colGrpSize];
	memset(r->m_data, 0, r->m_size);
	memset(r->m_data, 0, colGrpSize * sizeof(size_t));
	return r;
}

/**
 * 释放记录空间
 * @param r 待释放记录
 */
void freeRecord(Record* r) {
	delete[] r->m_data;
	delete r;
}

/**
* 释放压缩格式记录空间
* @param r 待释放记录
*/
void freeCompressOrderRecord(CompressOrderRecord *r) {
	if (r->m_segSizes) {
		delete []r->m_segSizes;
		r->m_segSizes = NULL;
	}
	freeRecord(r);
}

/**
 * 深度释放Mysql格式记录空间
 * 释放记录中存储的大对象指针
 * @param tableDef 表定义
 * @param r 待释放记录
 */

void freeMysqlRecord(const TableDef *tableDef, Record *r) {
	assert(r->m_format == REC_MYSQL);

	for (u16 cno = 0; cno < tableDef->m_numCols; ++cno) {
		if (!RedRecord::isNull(tableDef, r->m_data, cno) && tableDef->m_columns[cno]->isLob()) {
			void *lob;
			size_t size;
			RedRecord::readLob(tableDef, r->m_data, cno, &lob, &size);
			delete [] (byte *)lob;
		}
	}
	freeRecord(r);
}

void freeEngineMysqlRecord(const TableDef *tableDef, Record *r) {
	assert(r->m_format == REC_MYSQL);
	
	for (u16 cno = 0; cno < tableDef->m_numCols; ++cno) {
		if (!RedRecord::isNull(tableDef, r->m_data, cno) && tableDef->m_columns[cno]->isLob() && !tableDef->m_columns[cno]->isLongVar()) {
			void *lob;
			size_t size;
			RedRecord::readLob(tableDef, r->m_data, cno, &lob, &size);
			delete [] (byte *)lob;
		}
	}
	freeRecord(r);
}

void freeUppMysqlRecord(const TableDef *tableDef, Record *r) {
	assert(r->m_format == REC_UPPMYSQL);
	for (u16 cno = 0; cno <  tableDef->m_numCols; ++cno) {
		if (!RedRecord::isNull(tableDef, r->m_data, cno) && tableDef->m_columns[cno]->isLob() && !tableDef->m_columns[cno]->isLongVar()) {
			void *lob;
			size_t size;
			UppMysqlRecord::readLob(tableDef, r->m_data, cno, &lob, &size);
			delete [] (byte *)lob;
		}
	}
	freeRecord(r);
}



void freeSubRecord(SubRecord* sb) {
	delete[] sb->m_columns;
	delete[] sb->m_data;
	delete sb;
}





SubRecordBuilder::SubRecordBuilder(const TableDef *tableDef, RecFormat format, RowId rowId) 
	: m_tableDef(tableDef), m_format(format), m_maxColNo(0), m_rowId(rowId) {
}

SubRecordBuilder::~SubRecordBuilder() {
}

SubRecord* SubRecordBuilder::createSubRecord(const vector<u16>& columns, const vector<void *>& data) {
	size_t bmBytes = 0;
	if (m_format != KEY_MYSQL) { // KEY_MYSQL格式没有位图
		bmBytes = isKeyFormat(m_format)
			? getBitmapSize(m_format, getNullableColCnt(m_tableDef, columns.begin(), columns.end()))
			: m_tableDef->m_bmBytes;
	}
	// KEY_MYSQL格式时，即使varchar列最大长度小于255, ColumnDef::lenBytes仍然为2
	// 因此内容空间比m_tableDef->m_maxRecSize要大一些
	size_t capacity = m_tableDef->m_maxRecSize + m_tableDef->m_numCols;
	RowBuilder builder(m_tableDef, m_format, capacity, bmBytes, true);
	Bitmap bitmap(builder.data(), bmBytes * 8);

	size_t bmOffset = 0; // Null位图索引

	for (size_t i = 0; i < columns.size(); ++i) {
		void* ptr = data[i]; 
		u16 colNo = columns[i];
		ColumnDef* columnDef = m_tableDef->m_columns[colNo];
		if (ptr) { // not NULL
			if (columnDef->m_nullable) {
				if (m_format != KEY_MYSQL) {
					// 更新位图
					if (isKeyFormat(m_format)) {
						bitmap.clearBit(bmOffset);
						++bmOffset;
					} else {
						bitmap.clearBit(columnDef->m_nullBitmapOffset);
					}
				} else {
					builder.appendTinyInt(0); // 0表示非null
				}
			}
			switch (columnDef->m_type) {
				case CT_TINYINT:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeNumber(colNo, *(u8*)ptr)
							: builder.appendTinyInt(*(u8*)ptr);
					break;
				case CT_SMALLINT:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeNumber(colNo, *(u16*)ptr)
							: builder.appendSmallInt(*(u16*)ptr);
					break;
				case CT_MEDIUMINT:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeMediumInt(colNo, *(u32*)ptr)
						: builder.appendMediumInt(*(u32*)ptr);
					break;
				case CT_INT:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeNumber(colNo, *(u32*)ptr)
							: builder.appendInt(*(u32*)ptr);
					break;
				case CT_BIGINT:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeNumber(colNo, *(u64*)ptr)
							: builder.appendBigInt(*(u64*)ptr);
					break;
				case CT_FLOAT:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeNumber(colNo, *(float *)ptr)
							: builder.appendFloat(*(float *)ptr);
					break;
				case CT_DOUBLE:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeNumber(colNo, *(double *)ptr)
						: builder.appendDouble(*(double *)ptr);
					break;
				case CT_CHAR:
					m_format == REC_REDUNDANT || m_format == REC_FIXLEN ? 
						builder.writeChar(colNo, (char*)ptr, columnDef->m_size)
							: builder.appendChar((char*)ptr, columnDef->m_size);
					break;
				case CT_VARCHAR:
					{
						byte lenBytes = columnDef->m_lenBytes;
						size_t maxsize = columnDef->m_mysqlSize;
						if (m_format == KEY_MYSQL) {
							lenBytes = 2; // VARCHAR的长度永远为2
							if (columnDef->m_lenBytes < lenBytes)
								++maxsize;
						}
						m_format == REC_REDUNDANT || m_format == REC_FIXLEN ?
							builder.writeVarchar(colNo, (char*)ptr, maxsize, lenBytes)
								: builder.appendVarchar((char*)ptr, maxsize, lenBytes);
					}
					break;
				default:
					assert(false);
			}
		} else { // NULL
			assert(columnDef->m_nullable);
			if (m_format == KEY_MYSQL) {
				builder.appendTinyInt(1);
				builder.appendNull(columnDef->m_size - columnDef->m_lenBytes + 2);
			} else {
				if (m_format == KEY_PAD) 
					builder.appendNull(columnDef->m_size);
				// 更新位图
				if (isKeyFormat(m_format)) {
					bitmap.setBit(bmOffset);
					++bmOffset;
				} else {
					bitmap.setBit(columnDef->m_nullBitmapOffset);
				}
			}
		}
	}

	// 构造一个SubRecord
	SubRecord* sb = new SubRecord();
	sb->m_format = m_format;
	sb->m_rowId = m_rowId;
	sb->m_size = (uint)builder.size();
	sb->m_data = new byte[sb->m_size];
	memcpy(sb->m_data, builder.data(), sb->m_size);
	sb->m_numCols = (u16)columns.size();
	sb->m_columns = new u16[sb->m_numCols];
	for (uint i = 0; i < sb->m_numCols; ++i) {
		sb->m_columns[i] = columns[i];
	}
	return sb;
}

SubRecord* SubRecordBuilder::createSubRecord(const vector<u16>& columns, va_list argp) {
	vector<void *> data;
	data.reserve(columns.size());
	for (size_t i = 0; i < columns.size(); ++i)
		data.push_back(va_arg(argp, void*));
	return createSubRecord(columns, data);
}

SubRecord* SubRecordBuilder::createSubRecordById(const char *cols, ...) {
	vector<u16> columns = parseColNos(cols);
	va_list argp; 
	va_start(argp, cols);
	SubRecord* sr = createSubRecord(columns, argp);
	va_end(argp);
	return sr;
}

SubRecord* SubRecordBuilder::createSubRecordByName(const char *colNames, ...) {
	vector<u16> columns = parseColNames(m_tableDef, colNames);
	va_list argp; 
	va_start(argp, colNames);
	SubRecord* sr = createSubRecord(columns, argp);
	va_end(argp);
	return sr;
}

SubRecord* SubRecordBuilder::createEmptySbById(uint size, const char* cols) {
	vector<u16> columns = parseColNos(cols);
	return createEmptySb(size, columns);
}

SubRecord* SubRecordBuilder::createEmptySbByName(uint size, const char* colNames) {
	vector<u16> columns = parseColNames(m_tableDef, colNames);
	return createEmptySb(size, columns);
}

SubRecord* SubRecordBuilder::createEmptySb(uint size, const vector<u16>& columns) {
	// 构造一个SubRecord
	SubRecord* sb = new SubRecord();
	sb->m_rowId = m_rowId;
	sb->m_format = m_format;
	sb->m_size = size;
	sb->m_data = new byte[sb->m_size];
	memset(sb->m_data, REC_UNINIT_BYTE, sb->m_size);
	sb->m_numCols = (u16)columns.size();
	sb->m_columns = new u16[sb->m_numCols];
	for (uint i = 0; i < sb->m_numCols; ++i) {
		sb->m_columns[i] = columns[i];
	}
	return sb;
}




KeyBuilder::KeyBuilder(const TableDef *tableDef, const IndexDef *indexDef, RecFormat format, RowId rowId) 
: m_tableDef(tableDef), m_indexDef(indexDef), m_format(format), m_maxColNo(0), m_rowId(rowId) {
}

KeyBuilder::~KeyBuilder() {
}

SubRecord* KeyBuilder::createKey(const vector<u16>& columns, const vector<void *>& data) {
	size_t bmBytes = 0;
	if (m_format != KEY_MYSQL) { // KEY_MYSQL格式没有位图
		bmBytes = isKeyFormat(m_format)
			? getBitmapSize(m_format, getNullableColCnt(m_tableDef, columns.begin(), columns.end()))
			: m_tableDef->m_bmBytes;
	}
	// KEY_MYSQL格式时，即使varchar列最大长度小于255, ColumnDef::lenBytes仍然为2
	// 因此内容空间比m_tableDef->m_maxRecSize要大一些
	size_t capacity = m_indexDef->m_maxKeySize + m_tableDef->m_numCols;
	RowBuilder builder(m_tableDef, m_format, capacity, bmBytes, true);
	Bitmap bitmap(builder.data(), bmBytes * 8);

	size_t bmOffset = 0; // Null位图索引

	for (size_t i = 0; i < columns.size(); ++i) {
		void* ptr = data[i]; 
		u16 colNo = columns[i];
		ColumnDef* columnDef = m_tableDef->m_columns[colNo];
		if (ptr) { // not NULL
			if (columnDef->m_nullable) {
				if (m_format != KEY_MYSQL) {
					// 更新位图
					if (isKeyFormat(m_format)) {
						bitmap.clearBit(bmOffset);
						++bmOffset;
					} else {
						bitmap.clearBit(columnDef->m_nullBitmapOffset);
					}
				} else {
					builder.appendTinyInt(0); // 0表示非null
				}
			}
			switch (columnDef->m_type) {
				case CT_TINYINT:
					builder.appendTinyInt(*(u8*)ptr);
					break;
				case CT_SMALLINT:
					builder.appendSmallInt(*(u16*)ptr);
					break;
				case CT_MEDIUMINT:
					builder.appendMediumInt(*(u32*)ptr);
					break;
				case CT_INT:
					builder.appendInt(*(u32*)ptr);
					break;
				case CT_BIGINT:
					builder.appendBigInt(*(u64*)ptr);
					break;
				case CT_FLOAT:
					builder.appendFloat(*(float *)ptr);
					break;
				case CT_DOUBLE:
					builder.appendDouble(*(double *)ptr);
					break;
				case CT_CHAR:
					{
						size_t maxsize = columnDef->m_size;
						if(m_indexDef->m_prefixLens[i] > 0)
							maxsize = m_indexDef->m_prefixLens[i];
						builder.appendChar((char*)ptr, maxsize);
					}
					break;
				case CT_VARCHAR:
					{
						byte lenBytes = columnDef->m_lenBytes;
						size_t maxsize = columnDef->m_mysqlSize;
						if(m_indexDef->m_prefixLens[i] > 0) {
							maxsize = m_indexDef->m_prefixLens[i] + lenBytes;
						}
						if (m_format == KEY_MYSQL) {
							lenBytes = 2; // VARCHAR的长度永远为2
							if (columnDef->m_lenBytes < lenBytes)
								++maxsize;
						}
						builder.appendVarchar((char*)ptr, maxsize,  lenBytes);
					}
					break;
				case CT_SMALLLOB:
					{
						assert(m_indexDef->m_prefixLens[i] > 0);
						byte lenBytes = m_indexDef->m_prefixLens[i] > 255 ? 2 : 1;
						size_t maxsize = m_indexDef->m_prefixLens[i] +lenBytes;
						if (m_format == KEY_MYSQL) {
							if (lenBytes == 1)
								++maxsize;
							lenBytes = 2; // VARCHAR的长度永远为2
						}
						builder.appendVarchar((char*)ptr, maxsize, lenBytes);
					}
					break;
				case CT_MEDIUMLOB:
					{
						assert(m_indexDef->m_prefixLens[i] > 0);
						byte lenBytes = m_indexDef->m_prefixLens[i] > 255 ? 2 : 1;
						size_t maxsize = m_indexDef->m_prefixLens[i] +lenBytes;
						if (m_format == KEY_MYSQL) {
							if (lenBytes == 1)
								++maxsize;
							lenBytes = 2; // VARCHAR的长度永远为2
						}
						builder.appendVarchar((char*)ptr, maxsize, lenBytes);
					}
					break;
				default:
					assert(false);
			}
		} else { // NULL
			assert(columnDef->m_nullable);
			if (m_format == KEY_MYSQL) {
				builder.appendTinyInt(1);
				size_t maxsize =  columnDef->m_size - columnDef->m_lenBytes + 2;
				if (m_indexDef->m_prefixLens[i])
					maxsize = maxsize = m_indexDef->m_prefixLens[i] + 2;
				builder.appendNull(maxsize);
			} else {
				if (m_format == KEY_PAD) {
					size_t maxsize = columnDef->m_size;
					byte lenBytes = columnDef->m_lenBytes;
					if (m_indexDef->m_prefixLens[i]) {
						if(columnDef->isLob())
							lenBytes = m_indexDef->m_prefixLens[i] > 255 ? 2 : 1;
						maxsize = m_indexDef->m_prefixLens[i] + lenBytes;
					}
					builder.appendNull(maxsize);
				}
				// 更新位图
				if (isKeyFormat(m_format)) {
					bitmap.setBit(bmOffset);
					++bmOffset;
				} else {
					bitmap.setBit(columnDef->m_nullBitmapOffset);
				}
			}
		}
	}

	// 构造一个SubRecord
	SubRecord* sb = new SubRecord();
	sb->m_format = m_format;
	sb->m_rowId = m_rowId;
	sb->m_size = (uint)builder.size();
	sb->m_data = new byte[sb->m_size];
	memcpy(sb->m_data, builder.data(), sb->m_size);
	sb->m_numCols = (u16)columns.size();
	sb->m_columns = new u16[sb->m_numCols];
	for (uint i = 0; i < sb->m_numCols; ++i) {
		sb->m_columns[i] = columns[i];
	}
	return sb;
}

SubRecord* KeyBuilder::createKey(const vector<u16>& columns, va_list argp) {
	vector<void *> data;
	data.reserve(columns.size());
	for (size_t i = 0; i < columns.size(); ++i)
		data.push_back(va_arg(argp, void*));
	return createKey(columns, data);
}

SubRecord* KeyBuilder::createKeyById(const char *cols, ...) {
	vector<u16> columns = parseColNos(cols);
	va_list argp; 
	va_start(argp, cols);
	SubRecord* sr = createKey(columns, argp);
	va_end(argp);
	return sr;
}

SubRecord* KeyBuilder::createKeyByName(const char *colNames, ...) {
	vector<u16> columns = parseColNames(m_tableDef, colNames);
	va_list argp; 
	va_start(argp, colNames);
	SubRecord* sr = createKey(columns, argp);
	va_end(argp);
	return sr;
}

SubRecord* KeyBuilder::createEmptyKeyById(uint size, const char* cols) {
	vector<u16> columns = parseColNos(cols);
	return createEmptyKey(size, columns);
}

SubRecord* KeyBuilder::createEmptyKeyByName(uint size, const char* colNames) {
	vector<u16> columns = parseColNames(m_tableDef, colNames);
	return createEmptyKey(size, columns);
}

SubRecord* KeyBuilder::createEmptyKey(uint size, const vector<u16>& columns) {
	// 构造一个SubRecord
	SubRecord* sb = new SubRecord();
	sb->m_rowId = m_rowId;
	sb->m_format = m_format;
	sb->m_size = size;
	sb->m_data = new byte[sb->m_size];
	memset(sb->m_data, REC_UNINIT_BYTE, sb->m_size);
	sb->m_numCols = (u16)columns.size();
	sb->m_columns = new u16[sb->m_numCols];
	for (uint i = 0; i < sb->m_numCols; ++i) {
		sb->m_columns[i] = columns[i];
	}
	return sb;
}


vector<u16> parseColNos(const char* cols) {
	istringstream ss(cols);
	u16 curCol;
	vector<u16> ret;
	while (ss >> curCol) {
		ret.push_back(curCol);
	}
	return ret;
}

vector<u16> parseColNames(const TableDef* tableDef, const char* colNames) {
	istringstream ss(colNames);
	u16 curCol;
	string curColName;
	vector<u16> ret;
	while (ss >> curColName) {
		for (curCol = 0; curCol < tableDef->m_numCols; ++curCol) {
			if (curColName == tableDef->m_columns[curCol]->m_name)
				break;
		}
		assert(curCol < tableDef->m_numCols);
		ret.push_back(curCol);
	}
	return ret;
}


static size_t getBitmapSize(RecFormat format, size_t nullableCols) {
	return (format == REC_FIXLEN) ? (nullableCols + 8) / 8 : (nullableCols + 7) / 8;
}

static bool isKeyFormat(RecFormat format) {
	return (format == KEY_COMPRESS || format == KEY_NATURAL || format == KEY_PAD);
}

static bool isRedundantFormat(RecFormat format) {
	return format == REC_FIXLEN || format == REC_REDUNDANT || format == KEY_PAD || format == REC_MYSQL || format == KEY_MYSQL;
}

static bool isUppMysqlFormat(RecFormat format) {
	return format == REC_UPPMYSQL;
}

/**
 * 构造一个RecordConvert对象
 * @param origTbdef			原表定义
 * @param addCol			增加列的定义数组
 * @param addColNum			增加列的数目
 * @param delCol			删除列的定义数组
 * @param delColNum			删除列的数目
 * @throw					列定义不合法则抛出异常
 * @pre				需要删除的列均存在于原表定义中，需要增加的列均不存在于原表定义和删除列的差集中。
 *					增加列的m_position从小到大排列，数值为原表中的对应列位置。
 */
RecordConvert::RecordConvert(TableDef *origTbdef, const AddColumnDef *addCol, u16 addColNum, const ColumnDef **delCol, 
							 u16 delColNum, RecFormat convFormat /*= REC_MYSQL*/) throw(NtseException){
	assert((addColNum && addCol) || (addColNum == 0 && addCol == NULL));
	assert((delColNum && delCol) || (delColNum == 0 && delCol == NULL));
	assert(REC_MYSQL == convFormat || REC_REDUNDANT == convFormat);

	m_convFormat = convFormat;
	m_origtbd = origTbdef;
	if (addColNum == 0 && delColNum == 0) {
		m_optimize = true;
		m_ColMapN2O = m_ColMapO2N = NULL;
		m_newtbd = NULL;
		m_convBufData = NULL;
		return;
	} 

	// else 完成其他非空设置
	m_optimize = false;
	// 验证参数合法性
	if (delColNum >= m_origtbd->m_numCols) { // 所有列都删？
		NTSE_THROW(NTSE_EC_COLDEF_ERROR, "Deleting too many columns.", delColNum);
	}
	for (u16 i = 0; i < addColNum - 1; ++i) {
		if (addCol[i].m_position > addCol[i + 1].m_position) // position必须是单调增加的
			NTSE_THROW(NTSE_EC_COLDEF_ERROR, "Adding column position not sorted.");
	}
	// 验证每一个需要删除的列都存在
	int *origColState = new int[m_origtbd->m_numCols];
	for (u16 i = 0; i < m_origtbd->m_numCols; ++i)
		origColState[i] = 1; // 1 means exist
	for (u16 i = 0; i < delColNum; ++i) {
		bool found = false;
		for (u16 j = 0; j < m_origtbd->m_numCols; ++j) {
			if (!System::stricmp(delCol[i]->m_name, m_origtbd->m_columns[j]->m_name)) {
				if (m_origtbd->m_columns[j]->m_inIndex) {
					delete [] origColState;
					NTSE_THROW(NTSE_EC_COLDEF_ERROR, "Can delete column while there is index on it.", m_origtbd->m_columns[j]->m_name);
				}
				found = true;
				origColState[j] = 0; // 0 means no longer exist
				break;
			}
		}
		if (!found) {
			delete [] origColState;
			NTSE_THROW(NTSE_EC_COLDEF_ERROR, "Column to delete not found in the original.", delCol[i]->m_name);
		}
	}
	// 检查要加入的列都不存在，且每一个列的默认值大小都是合适的
	for (u16 i = 0; i < addColNum; ++i) {
		ColumnDef *addDef = addCol[i].m_addColDef;
		for (u16 j = 0; j < m_origtbd->m_numCols; ++j) {
			if (origColState[j] == 0) // 这列已经被删除，不需要再检查，冲突也没关系
				continue;
			if (!System::stricmp(m_origtbd->m_columns[j]->m_name, addDef->m_name)) {
				// 冲突
				delete [] origColState;
				NTSE_THROW(NTSE_EC_COLDEF_ERROR, "Column to add conflict with existing.", addDef->m_name);
			}
		}
		//u16 colMaxSize;
		const char *errMsg = NULL;
		switch (addDef->m_type) {
			case CT_VARCHAR:
			case CT_VARBINARY:
			case CT_CHAR:
			case CT_BINARY:
				if (addCol[i].m_valueLength > addDef->m_size - addDef->m_lenBytes) {
					errMsg = "Column default value too long.";
					break;
				}
				break;
			case CT_DECIMAL:
				if (addCol[i].m_valueLength != addDef->m_size) {
					errMsg = "DECIMAL column value length wrong NULL.";
					break;
				}
				break;
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				if ((addCol[i].m_defaultValue && addCol[i].m_valueLength == 0)
					|| (!addCol[i].m_defaultValue && addCol[i].m_valueLength != 0)) {
						errMsg = "LOB column default value is wrong.";
						break;
				}
				// TODO: 检验大对象大小不会超出限制
				break;
			default:
				if (addDef->calcSize() != addCol[i].m_valueLength) {
					errMsg = "Fixlen column type's default value length must be fixed too.";
					break;
				}
				break;
		}
		if (addCol[i].m_valueLength && !addCol[i].m_defaultValue) {
			errMsg = "Must provide default value. ";
		}
		if (errMsg) {
			delete [] origColState;
			NTSE_THROW(NTSE_EC_COLDEF_ERROR, errMsg, addDef->m_name);
		}
	}
	// 检测通过

	// 计算新老列的映射
	int newColNum = origTbdef->m_numCols - delColNum + addColNum;
	m_ColMapO2N = new u16[origTbdef->m_numCols];
	m_ColMapN2O = new u16[newColNum];
	u16 oldidx = 0, addidx = 0, newidx = 0;
	while (newidx < newColNum) {
		assert(addidx < addColNum || oldidx < origTbdef->m_numCols);
		while (oldidx < origTbdef->m_numCols && origColState[oldidx] == 0) {
			m_ColMapO2N[oldidx] = (u16)-1; // 这个老列不用于新定义中
			++oldidx;
		}
		if (addidx < addColNum && oldidx < origTbdef->m_numCols) {
			// 两个列都没用完
			if (addCol[addidx].m_position <= oldidx) {
				// 新增加的列插在旧列前面
				m_ColMapN2O[newidx] = Limits::MAX_COL_NUM + addidx; // 增加一个Limits::MAX_COL_NUM大小，以和通常的列号区分开来
				++addidx;
			} else {
				m_ColMapN2O[newidx] = oldidx;
				m_ColMapO2N[oldidx] = newidx; // 互相指向
				++oldidx;
			}
		} else if(addidx < addColNum) {
			assert(oldidx >= origTbdef->m_numCols);
			assert(addCol[addidx].m_position == (u16)-1);
			m_ColMapN2O[newidx] = Limits::MAX_COL_NUM + addidx;
			++addidx;
		} else {
			assert(addidx >= addColNum);
			assert(oldidx < origTbdef->m_numCols);
			m_ColMapN2O[newidx] = oldidx;
			m_ColMapO2N[oldidx] = newidx;
			++oldidx;
		}
		++newidx;
	}
	// TODO: 如果oldidx还未完，加完之
	assert(addidx == addColNum);
	assert(newidx == newColNum);
	while (oldidx < origTbdef->m_numCols) {
		assert(origColState[oldidx] == 0);
		m_ColMapO2N[oldidx] = (u16)-1; // 这个老列不用于新定义中
		++oldidx;
	}
	assert(oldidx == origTbdef->m_numCols);

	delete [] origColState;

	// 生成新的TableDef
	TableDefBuilder tb(origTbdef->m_id, origTbdef->m_schemaName, origTbdef->m_name);

	Array<u16> **newColGrps = new Array<u16>*[origTbdef->m_numColGrps];//用于添加属性组
	for (u16 i = 0; i < origTbdef->m_numColGrps; i++) {
		newColGrps[i] = new Array<u16>();
	}
	// 首先添加列
	for (int i = 0; i < newColNum; ++i) {
		ColumnDef *colDef;
		colDef = (m_ColMapN2O[i] >= Limits::MAX_COL_NUM) ?
			addCol[m_ColMapN2O[i] - Limits::MAX_COL_NUM].m_addColDef
			: origTbdef->m_columns[m_ColMapN2O[i]];
		if (origTbdef->m_numColGrps > 0) {  
			newColGrps[colDef->m_colGrpNo]->push((u16)i);
		}
		switch (colDef->m_type) {
			// 需要指定最大长度的类型
			case CT_CHAR:
			case CT_BINARY:
				tb.addColumnS(colDef->m_name, colDef->m_type, colDef->m_size, true, colDef->m_nullable, colDef->m_collation);
				break;
			case CT_VARCHAR:
			case CT_VARBINARY:
				tb.addColumnS(colDef->m_name, colDef->m_type, colDef->m_size - colDef->m_lenBytes, true, colDef->m_nullable, colDef->m_collation);
				break;
			// 数字类型
			case CT_TINYINT:
			case CT_SMALLINT:
			case CT_MEDIUMINT:
			case CT_INT:
			case CT_BIGINT:
			case CT_FLOAT:
			case CT_DOUBLE:
			case CT_DECIMAL:
				tb.addColumnN(colDef->m_name, colDef->m_type, colDef->m_prtype, colDef->m_nullable);
				break;
			// 其他类型
			case CT_RID:
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				tb.addColumn(colDef->m_name, colDef->m_type, colDef->m_nullable, colDef->m_collation);
				break;
			default:
				assert(false); //应该没有其他类型
				break;
		}
	}
	// 其次添加索引
	for (u16 i = 0; i < origTbdef->m_numIndice; ++i) {
		IndexDef *oldIdxdef = origTbdef->m_indice[i];
		Array<u16> idxcolumns;
		Array<u32> prefixArr;
		for (u16 j = 0; j < oldIdxdef->m_numCols; ++j) {
			u16 newCol = m_ColMapO2N[oldIdxdef->m_columns[j]];
			assert(newCol != (u16)-1); // 所有索引的列都存在于新列定义中
			idxcolumns.push(newCol);
			prefixArr.push(oldIdxdef->m_prefixLens[j]);
		}
		tb.addIndex(oldIdxdef->m_name, oldIdxdef->m_primaryKey, oldIdxdef->m_unique, oldIdxdef->m_online, idxcolumns, prefixArr);
	}
	tb.setCompresssTbl(origTbdef->m_isCompressedTbl);
	for (u16 i = 0; i < origTbdef->m_numColGrps; i++) {
		if (origTbdef->m_isCompressedTbl)
			tb.addColGrp((u8)i, *newColGrps[i]);
		delete newColGrps[i];
		newColGrps[i] = NULL;
	}
	delete [] newColGrps;
	newColGrps = NULL;

	// 获得新的TableDef
	m_newtbd = tb.getTableDef();
#ifdef NTSE_UNIT_TEST
	m_newtbd->check();
#endif

	// 生成一个mysqlRecord，将新添加的默认值加入
	m_convBufData = new byte[m_newtbd->m_maxRecSize];
	memset(m_convBufData, 0, m_newtbd->m_maxRecSize);
	u16 position = 0; // addCol中的列在新TableDef中的位置
	for (u16 i = 0; i < addColNum; ++i) {
		// TODO: 写出新增列的默认值到m_mysqlData

		while (position < m_newtbd->m_numCols && strcmp(m_newtbd->m_columns[position]->m_name, addCol[i].m_addColDef->m_name)) {
			++position;
			assert(position < m_newtbd->m_numCols); //前提是addCol的列号从小到大排列
		}
		ColumnDef *addDef = m_newtbd->m_columns[position];
		assert(0 == strcmp(addDef->m_name, addCol[i].m_addColDef->m_name));
		// 处理空值情况
		if (addCol[i].m_valueLength == 0) { // 空列
			assert(!addCol[i].m_defaultValue);
			RedRecord::setNull(m_newtbd, m_convBufData, position);
			// 如果是大对象还需要置空
			if (addDef->m_type == CT_SMALLLOB || addDef->m_type == CT_MEDIUMLOB) {
				//Table::writeLobSize(m_convBufData, addDef, 0);
				//Table::writeLob(m_convBufData, addDef, NULL);
				RedRecord::writeLob(m_newtbd, m_convBufData, position, NULL, 0);
			}
			continue;
		}
		RedRecord::setNotNull(m_newtbd, m_convBufData, position);
		switch (addDef->m_type) {
			case CT_CHAR:
			case CT_BINARY:
				RedRecord::writeChar(m_newtbd, m_convBufData, position, (byte *)addCol[i].m_defaultValue, (size_t)addCol[i].m_valueLength);
				break;
			case CT_VARCHAR:
			case CT_VARBINARY:
				RedRecord::writeVarchar(m_newtbd, m_convBufData, position, (byte *)addCol[i].m_defaultValue, (size_t)addCol[i].m_valueLength);
				break;
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				if (convFormat == REC_MYSQL)
					RedRecord::writeLob(m_newtbd, m_convBufData, position, (byte *)addCol[i].m_defaultValue, (size_t)addCol[i].m_valueLength);
				else {
					assert(convFormat == REC_REDUNDANT);
					assert(!addCol);//只有不增加大对象列的情况下才能进行REC_REDUNDANT格式记录的转换
					RecordOper::writeLobId(m_convBufData, m_newtbd->m_columns[position], INVALID_LOB_ID);
				}
				break;
			default:
				assert(addDef->isFixlen());
				RedRecord::writeRaw(m_newtbd, m_convBufData, position, addCol[i].m_defaultValue, (size_t)addCol[i].m_valueLength);
				break;
		}
	}
}

/**
 * 析构函数
 */
RecordConvert::~RecordConvert() {
	if (!m_optimize) {
		delete [] m_ColMapN2O;
		delete [] m_ColMapO2N;
		delete m_newtbd;
		delete [] m_convBufData;
	}
}

/**
 * 获得一个转换后表定义的拷贝
 * @return 转换后的表定义
 */
TableDef *RecordConvert::getNewTableDef() {
	if (m_optimize) {
		return new TableDef(m_origtbd);
	} else {
		return new TableDef(m_newtbd);
	}
}

/**
 * 获得一个表所有列的列号数组
 * @param tbdef		需要获取列号数组的表定义
 * @param mc		申请存放列号数组的内存的MemoryContext
 * @return			列号数组
 */
u16 *RecordConvert::getAllColumnNum(TableDef *tbdef, MemoryContext *mc) {
	u16 *allCols = (u16 *)mc->alloc(sizeof(u16) * tbdef->m_numCols);
	for (u16 i = 0; i < tbdef->m_numCols; ++i) allCols[i] = i;
	return allCols;
}

/**
* 转换mysql格式记录
* @param inRec				需要转换的记录
* @param mc					内存分配上下文
* @param inNumCols			需要转换的列的数目，为0表示都转换
* @param inColumns			需要转换的列号，为NULL表示都转换
* @param outNumCols out		转换后的列数目
* @param outColumns out		转换后的列号
* @return			转换后的记录数据
*/
byte *RecordConvert::convertMysqlOrRedRec(ntse::Record *inRec, MemoryContext *mc, u16 inNumCols, u16 *inColumns, u16 *outNumCols, u16 **outColumns) {
	assert(inRec->m_format == REC_MYSQL || inRec->m_format == REC_REDUNDANT);
	assert((!inNumCols && !inColumns && !outNumCols && !outColumns)
		|| (inNumCols && inColumns && outNumCols && outColumns));
	if (m_optimize) {
		if (outNumCols) {
			*outNumCols = inNumCols;
			*outColumns = inColumns;
		}
		return inRec->m_data;
	}

	// TODO: 增删字段的TableDef在此处理
	u16 inCNum = inNumCols ? inNumCols : m_origtbd->m_numCols;
	u16 *inCols;
	if (inColumns) {
		inCols = inColumns;
	} else {
		inCols = getAllColumnNum(m_origtbd, mc);
	}
	u16 outCNum = 0; // 发现一个增加一个
	u16 *outCols = (u16 *)mc->alloc(sizeof(u16) * m_newtbd->m_numCols); // 最多会有这么多列
	for (u16 i = 0; i < inCNum; ++i) {
		u16 icn = inCols[i]; // in column number
		u16 ocn = m_ColMapO2N[icn];
		// 判断是否存在于转换后的列
		if (ocn == (u16)-1) {
			continue;
		}
		outCols[outCNum] = ocn;
		++outCNum;
		ColumnDef *icd = m_origtbd->m_columns[icn];
		ColumnDef *ocd = m_newtbd->m_columns[ocn];
		if (RedRecord::isNull(m_origtbd, inRec->m_data, icn)) {/*BitmapOper::isSet(inRec->m_data, (m_origtbd->m_maxRecSize << 3), icd->m_nullBitmapOffset)*/
			// 记录为空
			assert(icd->m_nullable && ocd->m_nullable);
			RedRecord::setNull(m_newtbd, m_convBufData, ocn);
		} else {
			RedRecord::setNotNull(m_newtbd, m_convBufData, ocn);
			//RedRecord::readRaw()
			assert(icd->m_size == ocd->m_size);
			memcpy(m_convBufData + ocd->m_offset, inRec->m_data + icd->m_offset, ocd->m_size); // 拷贝记录数据
		}
	}

	if (inNumCols) {
		*outNumCols = outCNum;
		*outColumns = outCols;
	}

	return m_convBufData;
}

/**
 * 转换IndexDef
 * @param inDef		需要转换的IndexDef
 * @param mc		内存分配上下文
 * @return			转换后的IndexDef，依据情况，可能是inDef的引用，也可能是通过mc分配的新内存
 */
IndexDef *RecordConvert::convertIndexDef(IndexDef *inDef, MemoryContext *mc) {
	if (m_optimize) {
		return inDef;
	} else {
		ColumnDef **columns = new ColumnDef *[inDef->m_numCols];
		for (u16 i = 0; i < inDef->m_numCols; ++i) {
			u16 ocn = m_ColMapO2N[inDef->m_columns[i]];
			assert(ocn != (u16)-1);
			columns[i] = m_newtbd->m_columns[ocn];
		}
		IndexDef *convertedDef = new IndexDef(inDef->m_name, inDef->m_numCols, columns, inDef->m_prefixLens, inDef->m_unique, inDef->m_primaryKey);
		delete [] columns;
		//return outDef;
		IndexDef *outDef = (IndexDef *)mc->alloc(sizeof(IndexDef));
		*outDef = *convertedDef;
		outDef->m_name = (char *)mc->alloc(strlen(convertedDef->m_name) + 1);
		strcpy(outDef->m_name, convertedDef->m_name);
		assert(outDef->m_numCols == convertedDef->m_numCols);
		outDef->m_columns = (u16*)mc->alloc(sizeof(u16) * outDef->m_numCols);
		memcpy(outDef->m_columns, convertedDef->m_columns, sizeof(u16) * outDef->m_numCols);
		outDef->m_offsets = (u16*)mc->alloc(sizeof(u16) * outDef->m_numCols);
		memcpy(outDef->m_offsets, convertedDef->m_offsets, sizeof(u16) * outDef->m_numCols);
		delete convertedDef;
		return outDef;
	}
}

}



