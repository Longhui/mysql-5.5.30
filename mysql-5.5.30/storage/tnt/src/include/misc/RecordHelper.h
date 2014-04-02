/**
 * Record测试辅助类
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#ifndef _NTSETEST_RECORDHELPER_H_
#define _NTSETEST_RECORDHELPER_H_

#include <algorithm>
#include <vector>
#include <stdarg.h>
#include "misc/Record.h"
#include "util/Bitmap.h"
#include "misc/TableDef.h"

namespace ntse {

class RowBuilder;

/**
 * RedRecord
 * 冗余记录(REC_MYSQL或者REC_REDUNDANT)操作类
 */
class RedRecord {
public:
	RedRecord(const TableDef* tableDef, RecFormat format = REC_MYSQL);
	RedRecord(const TableDef* tableDef, Record *record);
#ifdef TNT_ENGINE
	RedRecord(TableDef* tableDef, Record *record, bool tblDefOwn);
#endif
	RedRecord(const RedRecord& record);
	~RedRecord();
	RedRecord* writeVarchar(u16 cno, const char *str);
	RedRecord* writeVarchar(u16 cno, const byte *str, size_t size);
	RedRecord* writeLob(u16 cno, const byte *lob, size_t size);
	RedRecord* writeLobSize(u16 cno, size_t size);
	RedRecord* writeChar(u16 cno, const char *str);
	RedRecord* writeChar(u16 cno, const char *str, size_t size);
	RedRecord* writeBinary(u16 cno, const byte *str);
	RedRecord* writeBinary(u16 cno, const byte *str, size_t size);
	RedRecord* writeMediumInt(u16 cno, int v);
	RedRecord* setNull(u16 cno);
	RedRecord* setRowId(RowId rid);
	
	RowId getRowId() const;
	
	template<typename T> RedRecord* writeNumber(u16 cno, T v) {
		assert(m_tableDef->m_numCols > cno);
		ColumnDef *columnDef = m_tableDef->m_columns[cno];
		assert(sizeof(v) == columnDef->m_size);
		return writeField(cno, &v, sizeof(v));
	}


	template <typename T> void* readVarchar(u16 cno, T **data, size_t *size) {
		return readVarchar(m_tableDef, m_record->m_data, cno, (void **)data, size);
	}
	template <typename T> void readLob(u16 cno, T **lob, size_t *size) {
		readLob(m_tableDef, m_record->m_data, cno, (void **)lob, size);
	}
	template <typename T> void* readChar(u16 cno, T **data = 0, size_t *size = 0) {
		return readChar(m_tableDef, m_record->m_data, cno, (void **)data, size);
	}
	template <typename T> void* readBinary(u16 cno, T **data = 0, size_t *size = 0) {
		return readChar(m_tableDef, m_record->m_data, cno, (void **)data, size);
	}

	s64 readBigInt(u16 cno);
	s32 readInt(u16 cno);
	s32 readMediumInt(u16 cno);
	s16 readSmallInt(u16 cno);
	s8 readTinyInt(u16 cno);
	float readFloat(u16 cno);
	double readDouble(u16 cno);

	static void writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size);
	static void writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const char *str);
	static void writeVarcharLen(const TableDef *tableDef, byte *rec, u16 cno, size_t size);
	static void writeLob(const TableDef *tableDef, byte *rec, u16 cno, const byte *lob, size_t size);
	static void writeChar(const TableDef *tableDef, byte *rec, u16 cno, const char *str);
	static void writeChar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size);
	static void writeMediumInt(const TableDef *tableDef, byte *rec, u16 cno, int v);

	template<typename T> static void writeNumber(const TableDef *tableDef, u16 cno, byte *rec, T v) {
		assert(tableDef->m_numCols > cno);
		assert(sizeof(v) == tableDef->m_columns[cno]->m_size);
		return writeField(tableDef, rec, cno, &v, sizeof(v));
	}

	static void* readVarchar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);
	static void readLob(const TableDef *tableDef, const byte *rec, u16 cno, void **lob, size_t *size);
	static void* readChar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);
	static s64 readBigInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s32 readInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s32 readMediumInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s16 readSmallInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s8 readTinyInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static float readFloat(const TableDef *tableDef, const byte *rec, u16 cno);
	static double readDouble(const TableDef *tableDef, const byte *rec, u16 cno);

	static bool isNull(const TableDef *tableDef, const byte *rec, u16 cno);
	static void setNull(const TableDef *tableDef, byte *rec, u16 cno);
	static void setNotNull(const TableDef *tableDef, byte *rec, u16 cno);
	const Record* getRecord() const;

	static void writeRaw(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size);
	static void* readRaw(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);
private:
	RedRecord* writeField(u16 cno, const void *data, size_t size);
	static void writeField(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size);
	static void* readField(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);


private:
	TableDef	*m_tableDef;
	Record		*m_record;
	bool		m_ownMemory;
#ifdef TNT_ENGINE
	bool        m_tblDefOwn;
#endif
};


/**
 * RedRecord
 * 冗余记录(REC_MYSQL或者REC_REDUNDANT)操作类
 */
class UppMysqlRecord {
public:
	UppMysqlRecord(const TableDef* tableDef, RecFormat format = REC_UPPMYSQL);
	UppMysqlRecord(const TableDef* tableDef, Record *record);
#ifdef TNT_ENGINE
	UppMysqlRecord(TableDef* tableDef, Record *record, bool tblDefOwn);
#endif
	UppMysqlRecord(const UppMysqlRecord& record);
	~UppMysqlRecord();
	UppMysqlRecord* writeVarchar(u16 cno, const char *str);
	UppMysqlRecord* writeVarchar(u16 cno, const byte *str, size_t size);
	UppMysqlRecord* writeLob(u16 cno, const byte *lob, size_t size);
	UppMysqlRecord* writeLobSize(u16 cno, size_t size);
	UppMysqlRecord* writeChar(u16 cno, const char *str);
	UppMysqlRecord* writeChar(u16 cno, const char *str, size_t size);
	UppMysqlRecord* writeBinary(u16 cno, const byte *str);
	UppMysqlRecord* writeBinary(u16 cno, const byte *str, size_t size);
	UppMysqlRecord* writeMediumInt(u16 cno, int v);
	UppMysqlRecord* setNull(u16 cno);
	UppMysqlRecord* setRowId(RowId rid);
	
	RowId getRowId() const;
	
	template<typename T> RedRecord* writeNumber(u16 cno, T v) {
		assert(m_tableDef->m_numCols > cno);
		ColumnDef *columnDef = m_tableDef->m_columns[cno];
		assert(sizeof(v) == columnDef->m_size);
		return writeField(cno, &v, sizeof(v));
	}


	template <typename T> void* readVarchar(u16 cno, T **data, size_t *size) {
		return readVarchar(m_tableDef, m_record->m_data, cno, (void **)data, size);
	}
	template <typename T> void readLob(u16 cno, T **lob, size_t *size) {
		readLob(m_tableDef, m_record->m_data, cno, (void **)lob, size);
	}
	template <typename T> void* readChar(u16 cno, T **data = 0, size_t *size = 0) {
		return readChar(m_tableDef, m_record->m_data, cno, (void **)data, size);
	}
	template <typename T> void* readBinary(u16 cno, T **data = 0, size_t *size = 0) {
		return readChar(m_tableDef, m_record->m_data, cno, (void **)data, size);
	}

	s64 readBigInt(u16 cno);
	s32 readInt(u16 cno);
	s32 readMediumInt(u16 cno);
	s16 readSmallInt(u16 cno);
	s8 readTinyInt(u16 cno);
	float readFloat(u16 cno);
	double readDouble(u16 cno);

	static void writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size);
	static void writeVarchar(const TableDef *tableDef, byte *rec, u16 cno, const char *str);
	static void writeVarcharLen(const TableDef *tableDef, byte *rec, u16 cno, size_t size);
	static void writeLob(const TableDef *tableDef, byte *rec, u16 cno, const byte *lob, size_t size);
	static void writeChar(const TableDef *tableDef, byte *rec, u16 cno, const char *str);
	static void writeChar(const TableDef *tableDef, byte *rec, u16 cno, const byte *str, size_t size);
	static void writeMediumInt(const TableDef *tableDef, byte *rec, u16 cno, int v);

	template<typename T> static void writeNumber(const TableDef *tableDef, u16 cno, byte *rec, T v) {
		assert(tableDef->m_numCols > cno);
		assert(sizeof(v) == tableDef->m_columns[cno]->m_size);
		return writeField(tableDef, rec, cno, &v, sizeof(v));
	}

	static void* readVarchar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);
	static void readLob(const TableDef *tableDef, const byte *rec, u16 cno, void **lob, size_t *size);
	static void* readChar(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);
	static s64 readBigInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s32 readInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s32 readMediumInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s16 readSmallInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static s8 readTinyInt(const TableDef *tableDef, const byte *rec, u16 cno);
	static float readFloat(const TableDef *tableDef, const byte *rec, u16 cno);
	static double readDouble(const TableDef *tableDef, const byte *rec, u16 cno);

	static bool isNull(const TableDef *tableDef, const byte *rec, u16 cno);
	static void setNull(const TableDef *tableDef, byte *rec, u16 cno);
	static void setNotNull(const TableDef *tableDef, byte *rec, u16 cno);
	const Record* getRecord() const;

	static void writeRaw(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size);
	static void* readRaw(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);
private:
	UppMysqlRecord* writeField(u16 cno, const void *data, size_t size);
	static void writeField(const TableDef *tableDef, byte *rec, u16 cno, const void *data, size_t size);
	static void* readField(const TableDef *tableDef, const byte *rec, u16 cno, void **data, size_t *size);


private:
	TableDef	*m_tableDef;
	Record		*m_record;
	bool		m_ownMemory;
#ifdef TNT_ENGINE
	bool        m_tblDefOwn;
#endif
};


/** 方便生成Record的辅助类 */
class RecordBuilder {
public:
	/** 
	 * 构造一个RecordBuilder
	 * @param tableDef 表定义
	 * @param format 记录格式,可以为REC_REDUNDANT或者tableDef->m_recFormat
	 * @param rowId 记录ID
	 */
	RecordBuilder(const TableDef* tableDef, RowId rowId, RecFormat format);
	~RecordBuilder();

	RecordBuilder* appendNull();
	RecordBuilder* appendTinyInt(u8 value);
	RecordBuilder* appendSmallInt(u16 value);
	RecordBuilder* appendMediumInt(u32 value);
	RecordBuilder* appendInt(u32 value);
	RecordBuilder* appendFloat(float value);
	RecordBuilder* appendDouble(double value);
	RecordBuilder* appendBigInt(u64 value);
	RecordBuilder* appendChar(const char* str);
	RecordBuilder* appendChar(const char* str, size_t size);
	RecordBuilder* appendVarchar(const char* str);
	RecordBuilder* appendSmallLob(const byte* str);
	RecordBuilder* appendMediumLob(const byte* str);
	/** 
	 * 获取记录 
	 * @param size 记录占用内存空间
	 *			如果size==0，record->m_size = 记录数据实际占用空间大小
	 */
	Record* getRecord(size_t size = 0);

	/**
	* 获取压缩排序格式记录 
	* @pre 记录格式是压缩排序格式
	* @param size 记录占用内存空间
	*			如果size==0，record->m_size = 记录数据实际占用空间大小
	*/
	CompressOrderRecord *getCompressOrderRecord(size_t size = 0);

	/** 
	 * 创建一个空行 
	 * @param rowId 记录id
	 * @param format 记录格式
	 * @param size 记录大小
	 */
	static Record* createEmptyRecord(RowId rowId, RecFormat format, size_t size);

	/**
	 * 创建空的压缩排序格式行
	 * @param rowId 记录id
	 * @param size 记录大小
	 */
	static CompressOrderRecord* createEmptCompressOrderRcd(RowId rowId, size_t size, u8 colGrpSize = 1);

protected:
	void updateBitmap(bool isNull);
	void moveToNextCol();

private:
	RowBuilder*           m_builder;
	const TableDef *m_tableDef;
	RowId m_rowId;
	RecFormat m_format;
	ntse::Bitmap *m_bitmap;
	size_t       *m_colGrpSegmentSizes;
	ColumnDef*   m_curColumnDef;
};
/** 方便生成SubRecord的辅助类 */
class SubRecordBuilder {
public:
	/** 
	 * 构造一个SubRecordBuilder
	 * @param tableDef 表定义
	 * @param format 子记录格式，可以不同于tableDef::m_recFormat
	 * @param rowId 记录ID
	 */
	SubRecordBuilder(const TableDef* tableDef, RecFormat format, RowId rowId = 0);
	~SubRecordBuilder();
	/** 
	 * 创建一个子记录
	 * @param cols 指定子记录的列号，列号以空间间隔， 从0开始，如"0 1"
	 * @param ... 子记录各列的值内存指针
	 */
	SubRecord* createSubRecordById(const char* cols, ...);
	/** 
	 * 创建一个子记录
	 * @param colNames 指定子记录列名
	 * @param ... 子记录各列的内存指针
	 */
	SubRecord* createSubRecordByName(const char* colNames, ...);
	/** 
	 * 创建空的子记录 
	 * @param size 子记录内存空间
	 * @param cols cols 指定子记录的列号，列号以空间间隔， 从0开始，如"0 1"
	 */
	SubRecord* createEmptySbById(uint size, const char* cols);
	/** 
	 * 创建空的子记录 
	 * @param size 子记录内存空间
	 * @param colNames 指定子记录列名
	 */
	SubRecord* createEmptySbByName(uint size, const char* colNames);
	/**
	 * 创建子记录
	 * @param columns 子记录的列号
	 * @param data 子记录列对应的数据
	 * @return 创建好的子记录
	 */
	SubRecord* createSubRecord(const std::vector<u16>& columns, const std::vector<void *>& data);

private:
	SubRecord* createSubRecord(const std::vector<u16>& columns, va_list argp);
	SubRecord* createEmptySb(uint size, const std::vector<u16>& columns);

private:
	const TableDef *m_tableDef;
	RecFormat m_format;
	uint m_maxColNo;
	RowId m_rowId;
};

/** 增加列定义 */
struct AddColumnDef {
	ColumnDef	*m_addColDef;		/** 增加列的列定义 */
	u16			m_position;			/** 增加的列放在什么位置，这是相对于原来的列定义而言，AddColumnDef数组中，m_position必须是单调递增的*/
	void		*m_defaultValue;	/** 默认值 */
	u16			m_valueLength;		/** 默认值的数据长度，单位字节 */
};


/** 方便生成Key的辅助类 */
class KeyBuilder {
public:
	/** 
	 * 构造一个SubRecordBuilder
	 * @param tableDef 表定义
	 * @param format 子记录格式，可以不同于tableDef::m_recFormat
	 * @param rowId 记录ID
	 */
	KeyBuilder(const TableDef* tableDef, const IndexDef* indexDef, RecFormat format, RowId rowId = 0);
	~KeyBuilder();
	/** 
	 * 创建一个子记录
	 * @param cols 指定子记录的列号，列号以空间间隔， 从0开始，如"0 1"
	 * @param ... 子记录各列的值内存指针
	 */
	SubRecord* createKeyById(const char* cols, ...);
	/** 
	 * 创建一个子记录
	 * @param colNames 指定子记录列名
	 * @param ... 子记录各列的内存指针
	 */
	SubRecord* createKeyByName(const char* colNames, ...);
	/** 
	 * 创建空的子记录 
	 * @param size 子记录内存空间
	 * @param cols cols 指定子记录的列号，列号以空间间隔， 从0开始，如"0 1"
	 */
	SubRecord* createEmptyKeyById(uint size, const char* cols);
	/** 
	 * 创建空的子记录 
	 * @param size 子记录内存空间
	 * @param colNames 指定子记录列名
	 */
	SubRecord* createEmptyKeyByName(uint size, const char* colNames);
	/**
	 * 创建子记录
	 * @param columns 子记录的列号
	 * @param data 子记录列对应的数据
	 * @return 创建好的子记录
	 */
	SubRecord* createKey(const std::vector<u16>& columns, const std::vector<void *>& data);

private:
	SubRecord* createKey(const std::vector<u16>& columns, va_list argp);
	SubRecord* createEmptyKey(uint size, const std::vector<u16>& columns);

private:
	const TableDef *m_tableDef;
	const IndexDef *m_indexDef;
	RecFormat m_format;
	uint m_maxColNo;
	RowId m_rowId;
};


/** 记录变换类 */
class RecordConvert {
public:
	RecordConvert(TableDef *origTbdef, const AddColumnDef *addCol, u16 addColNum, const ColumnDef **delCol, u16 delColNum, 
		RecFormat convFormat = REC_MYSQL) throw(NtseException);
	~RecordConvert();
	TableDef *getNewTableDef();
	byte *convertMysqlOrRedRec(Record *inRec, MemoryContext *mc, u16 inNumCols = 0, u16 *inColumns = NULL, u16 *outNumCols = NULL, u16 **outColumns = NULL);
	IndexDef *convertIndexDef(IndexDef *inDef, MemoryContext *mc);

	u16 *getAllColumnNum(TableDef *tbdef, MemoryContext *mc);
private:
	TableDef	*m_origtbd;		/** 原表定义 */
	TableDef	*m_newtbd;		/** 新表定义 */
	bool		m_optimize;		/** 只是优化表，表定义未修改 */
	u16			*m_ColMapO2N;	/** 老的列到新的列的映射，转换后不存在则为(u16)-1 */
	u16			*m_ColMapN2O;	/** 新的列到老的列的映射，转换前不存在则为(u16)-1 */
	byte		*m_convBufData;	/** 存储填充好默认值的属性的数据 */
	RecFormat   m_convFormat;   /** 要转化的记录格式，只能为REC_MYSQL或REC_REDUNDANT */
};



extern void freeRecord(Record* r);
extern void freeCompressOrderRecord(CompressOrderRecord *r);
extern void freeMysqlRecord(const TableDef *tableDef, Record *r);
extern void freeEngineMysqlRecord(const TableDef *tableDef, Record *r);
extern void freeUppMysqlRecord(const TableDef *tableDef, Record *r);
/** 释放SubRecord内存  */
extern void freeSubRecord(SubRecord* sb);

}

#endif // _NTSETEST_RECORDHELPER_H_
