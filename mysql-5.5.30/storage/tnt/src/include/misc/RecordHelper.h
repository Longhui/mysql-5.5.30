/**
 * Record���Ը�����
 *
 * @author ������(yulihua@corp.netease.com, ylh@163.org)
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
 * �����¼(REC_MYSQL����REC_REDUNDANT)������
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
 * �����¼(REC_MYSQL����REC_REDUNDANT)������
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


/** ��������Record�ĸ����� */
class RecordBuilder {
public:
	/** 
	 * ����һ��RecordBuilder
	 * @param tableDef ����
	 * @param format ��¼��ʽ,����ΪREC_REDUNDANT����tableDef->m_recFormat
	 * @param rowId ��¼ID
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
	 * ��ȡ��¼ 
	 * @param size ��¼ռ���ڴ�ռ�
	 *			���size==0��record->m_size = ��¼����ʵ��ռ�ÿռ��С
	 */
	Record* getRecord(size_t size = 0);

	/**
	* ��ȡѹ�������ʽ��¼ 
	* @pre ��¼��ʽ��ѹ�������ʽ
	* @param size ��¼ռ���ڴ�ռ�
	*			���size==0��record->m_size = ��¼����ʵ��ռ�ÿռ��С
	*/
	CompressOrderRecord *getCompressOrderRecord(size_t size = 0);

	/** 
	 * ����һ������ 
	 * @param rowId ��¼id
	 * @param format ��¼��ʽ
	 * @param size ��¼��С
	 */
	static Record* createEmptyRecord(RowId rowId, RecFormat format, size_t size);

	/**
	 * �����յ�ѹ�������ʽ��
	 * @param rowId ��¼id
	 * @param size ��¼��С
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
/** ��������SubRecord�ĸ����� */
class SubRecordBuilder {
public:
	/** 
	 * ����һ��SubRecordBuilder
	 * @param tableDef ����
	 * @param format �Ӽ�¼��ʽ�����Բ�ͬ��tableDef::m_recFormat
	 * @param rowId ��¼ID
	 */
	SubRecordBuilder(const TableDef* tableDef, RecFormat format, RowId rowId = 0);
	~SubRecordBuilder();
	/** 
	 * ����һ���Ӽ�¼
	 * @param cols ָ���Ӽ�¼���кţ��к��Կռ����� ��0��ʼ����"0 1"
	 * @param ... �Ӽ�¼���е�ֵ�ڴ�ָ��
	 */
	SubRecord* createSubRecordById(const char* cols, ...);
	/** 
	 * ����һ���Ӽ�¼
	 * @param colNames ָ���Ӽ�¼����
	 * @param ... �Ӽ�¼���е��ڴ�ָ��
	 */
	SubRecord* createSubRecordByName(const char* colNames, ...);
	/** 
	 * �����յ��Ӽ�¼ 
	 * @param size �Ӽ�¼�ڴ�ռ�
	 * @param cols cols ָ���Ӽ�¼���кţ��к��Կռ����� ��0��ʼ����"0 1"
	 */
	SubRecord* createEmptySbById(uint size, const char* cols);
	/** 
	 * �����յ��Ӽ�¼ 
	 * @param size �Ӽ�¼�ڴ�ռ�
	 * @param colNames ָ���Ӽ�¼����
	 */
	SubRecord* createEmptySbByName(uint size, const char* colNames);
	/**
	 * �����Ӽ�¼
	 * @param columns �Ӽ�¼���к�
	 * @param data �Ӽ�¼�ж�Ӧ������
	 * @return �����õ��Ӽ�¼
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

/** �����ж��� */
struct AddColumnDef {
	ColumnDef	*m_addColDef;		/** �����е��ж��� */
	u16			m_position;			/** ���ӵ��з���ʲôλ�ã����������ԭ�����ж�����ԣ�AddColumnDef�����У�m_position�����ǵ���������*/
	void		*m_defaultValue;	/** Ĭ��ֵ */
	u16			m_valueLength;		/** Ĭ��ֵ�����ݳ��ȣ���λ�ֽ� */
};


/** ��������Key�ĸ����� */
class KeyBuilder {
public:
	/** 
	 * ����һ��SubRecordBuilder
	 * @param tableDef ����
	 * @param format �Ӽ�¼��ʽ�����Բ�ͬ��tableDef::m_recFormat
	 * @param rowId ��¼ID
	 */
	KeyBuilder(const TableDef* tableDef, const IndexDef* indexDef, RecFormat format, RowId rowId = 0);
	~KeyBuilder();
	/** 
	 * ����һ���Ӽ�¼
	 * @param cols ָ���Ӽ�¼���кţ��к��Կռ����� ��0��ʼ����"0 1"
	 * @param ... �Ӽ�¼���е�ֵ�ڴ�ָ��
	 */
	SubRecord* createKeyById(const char* cols, ...);
	/** 
	 * ����һ���Ӽ�¼
	 * @param colNames ָ���Ӽ�¼����
	 * @param ... �Ӽ�¼���е��ڴ�ָ��
	 */
	SubRecord* createKeyByName(const char* colNames, ...);
	/** 
	 * �����յ��Ӽ�¼ 
	 * @param size �Ӽ�¼�ڴ�ռ�
	 * @param cols cols ָ���Ӽ�¼���кţ��к��Կռ����� ��0��ʼ����"0 1"
	 */
	SubRecord* createEmptyKeyById(uint size, const char* cols);
	/** 
	 * �����յ��Ӽ�¼ 
	 * @param size �Ӽ�¼�ڴ�ռ�
	 * @param colNames ָ���Ӽ�¼����
	 */
	SubRecord* createEmptyKeyByName(uint size, const char* colNames);
	/**
	 * �����Ӽ�¼
	 * @param columns �Ӽ�¼���к�
	 * @param data �Ӽ�¼�ж�Ӧ������
	 * @return �����õ��Ӽ�¼
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


/** ��¼�任�� */
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
	TableDef	*m_origtbd;		/** ԭ���� */
	TableDef	*m_newtbd;		/** �±��� */
	bool		m_optimize;		/** ֻ���Ż�������δ�޸� */
	u16			*m_ColMapO2N;	/** �ϵ��е��µ��е�ӳ�䣬ת���󲻴�����Ϊ(u16)-1 */
	u16			*m_ColMapN2O;	/** �µ��е��ϵ��е�ӳ�䣬ת��ǰ��������Ϊ(u16)-1 */
	byte		*m_convBufData;	/** �洢����Ĭ��ֵ�����Ե����� */
	RecFormat   m_convFormat;   /** Ҫת���ļ�¼��ʽ��ֻ��ΪREC_MYSQL��REC_REDUNDANT */
};



extern void freeRecord(Record* r);
extern void freeCompressOrderRecord(CompressOrderRecord *r);
extern void freeMysqlRecord(const TableDef *tableDef, Record *r);
extern void freeEngineMysqlRecord(const TableDef *tableDef, Record *r);
extern void freeUppMysqlRecord(const TableDef *tableDef, Record *r);
/** �ͷ�SubRecord�ڴ�  */
extern void freeSubRecord(SubRecord* sb);

}

#endif // _NTSETEST_RECORDHELPER_H_
