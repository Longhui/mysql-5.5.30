/**
 * ������ز���ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */
#include "misc/TableDef.h"
#include "misc/Decimal.h"
#include "util/Stream.h"
#include "btree/Index.h"
#include "misc/KVParser.h"
#include "misc/Parser.h"
#include "util/SmartPtr.h"
#include "util/File.h"

namespace ntse {

///////////////////////////////////////////////////////////////////////////////
// TableDef ///////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
const char *ColumnDef::NAME = "name";
const char *ColumnDef::NAME_DEFAULT = "default";
const char *ColumnDef::TYPE = "type";
const char *ColumnDef::COLLATION = "collation";
const char *ColumnDef::SIZE = "size";
const char *ColumnDef::MYSQL_SIZE = "mysql_size";
const char *ColumnDef::NULLABLE = "null_able";
const char *ColumnDef::LONGVAR = "long_var";
const char *ColumnDef::CACHEUPDATE = "cache_update";
const char *ColumnDef::PRFLAGS = "pr_flags";
const char *ColumnDef::PRPRECISON = "pr_precision";
const char *ColumnDef::PRDECIMAL = "pr_decimal";
const char *ColumnDef::COLGRPNO = "col_group_no";
const char *ColumnDef::COLGRPOFFSET = "col_group_offset";

/** ����һ���ֶζ������ */
ColumnDef::ColumnDef() {
	m_name = NULL;
	m_type = ColumnDef::TYPE_DEFAULT;
	m_collation = ColumnDef::COLLATION_DEFAULT;
	m_size = ColumnDef::SIZE_DEFALUT;
	m_mysqlSize = ColumnDef::MYSQL_SIZE_DEFAULT;
	m_nullable = ColumnDef::NULLABLE_DEFAULT;
	m_cacheUpdate = ColumnDef::CACHEUPDATE_DEFAULT;
	m_prtype.m_flags = ColumnDef::PRFLAGS_DEFAULT;
	m_prtype.m_precision = ColumnDef::PRPRECISION_DEFAULT;
	m_prtype.m_deicmal = ColumnDef::PRDECIMAL_DEFAULT;
	m_colGrpNo = ColumnDef::COL_GRP_NO_DEFAULT;
	m_colGrpOffset = ColumnDef::COL_GRP_OFFSET_DEFAULT;
}

/**
 * ����һ���ֶζ������
 * 
 * @param name �ֶ���
 * @param type ����
 * @param maxSize ����CHAR/VARCHAR���ͣ�������󳤶ȣ���������Ϊ0
 */
ColumnDef::ColumnDef(const char *name, ColumnType type,  bool isLongVar, u16 maxSize, u8 colGrpNum, u16 colGrpOffset) {
	m_name = System::strdup(name);
	m_type = type;
	m_collation = COLL_BIN;
	m_lenBytes = 0;
	m_colGrpNo = colGrpNum;
	m_colGrpOffset = colGrpOffset;
	m_longVar = isLongVar;
	switch(m_type) {
	case CT_TINYINT:
		m_size = 1;
		break;
	case CT_SMALLINT:
		m_size = 2;
		break;
	case CT_MEDIUMINT:
		m_size = 3;
		break;
	case CT_INT:
	case CT_FLOAT:
		m_size = 4;
		break;
	case CT_BIGINT:
	case CT_DOUBLE:
		m_size = 8;
		break;
	case CT_CHAR:
	case CT_BINARY:
	case CT_DECIMAL:
		assert(maxSize > 0);
		m_size = maxSize;
		break;
	case CT_VARCHAR:
	case CT_VARBINARY:
		assert(maxSize > 0);
		m_lenBytes = (maxSize > 255)? 2 : 1;
		m_size = maxSize + m_lenBytes;
		break;
	case CT_SMALLLOB:
		m_size = 10;
		break;
	case CT_MEDIUMLOB:
		m_size = 11;
		break;
	case CT_RID:
		m_size = 6;
		break;
	}
	m_mysqlSize = isLongVar ? maxSize + 2 : m_size;
	m_nullable = true;
	m_inIndex = false;
	m_offset = 0;
	m_mysqlOffset = 0;
	m_nullBitmapOffset = 0;
	m_cacheUpdate = false;
	m_no = 0;
	calcCompressSize();

	m_prtype.m_flags = ColumnDef::PRFLAGS_DEFAULT;
	m_prtype.m_precision = ColumnDef::PRPRECISION_DEFAULT;
	m_prtype.m_deicmal = ColumnDef::PRDECIMAL_DEFAULT;
}

/**
 * ����һ���ֶζ������
 * 
 * @param copy ������һ�ֶζ���Ŀ���
 */
ColumnDef::ColumnDef(const ColumnDef *copy) {
	*this = *copy;
	m_name = System::strdup(copy->m_name);
}

ColumnDef::~ColumnDef() {
	delete []m_name;
}

/** ���������Ƿ�Ϊ��������
 * @return �����Ƿ�Ϊ��������
 */
bool ColumnDef::isFixlen() const {
	return m_type == CT_TINYINT || m_type == CT_SMALLINT || m_type == CT_INT 
		|| m_type == CT_BIGINT || m_type == CT_CHAR || m_type == CT_RID
		|| m_type == CT_FLOAT || m_type == CT_DOUBLE || m_type == CT_DECIMAL
		|| m_type == CT_MEDIUMINT || m_type == CT_BINARY;
}

/** ���������Ƿ�Ϊ���������
 * @return �����Ƿ�Ϊ���������
 */
bool ColumnDef::isLob() const {
	return m_type == CT_SMALLLOB || m_type == CT_MEDIUMLOB;
}

/** ���������Ƿ�Ϊ�ַ�������
 * @return �����Ƿ�Ϊ�ַ�������
 */
bool ColumnDef::isString() const {
	return m_type == CT_CHAR || m_type == CT_VARCHAR;
}

/** ���������Ƿ�ΪDECIMAL����
 * @return �����Ƿ�ΪDECIMAL����
 */
bool ColumnDef::isDecimal() const {
	return m_type == CT_DECIMAL;
}

/** �������Ե�m_size�Ƿ񲻹̶�
 * @return ���Ե�m_size�Ƿ񲻹̶�
 */
bool ColumnDef::varSized() const {
	return m_type == CT_VARCHAR || m_type == CT_VARBINARY || m_type == CT_CHAR || m_type == CT_BINARY
		|| m_type == CT_DECIMAL;
}


/** ���������Ƿ�Ϊ�����ֶ�
 * @return �����Ƿ�ΪDECIMAL����
 */
bool ColumnDef::isLongVar() const {
	return m_longVar;
}

/** ����̶���С���͵�m_size */
u16 ColumnDef::calcSize() {
	switch(m_type) {
	case CT_TINYINT:
		m_size = 1;
		break;
	case CT_SMALLINT:
		m_size = 2;
		break;
	case CT_MEDIUMINT:
		m_size = 3;
		break;
	case CT_INT:
	case CT_FLOAT:
		m_size = 4;
		break;
	case CT_BIGINT:
	case CT_DOUBLE:
		m_size = 8;
		break;
	case CT_SMALLLOB:
		m_size = 10;
		break;
	case CT_MEDIUMLOB:
		m_size = 11;
		break;
	case CT_RID:
		m_size = 6;
		break;
	default:
		assert(false);
	}
	return m_size;
}

/** �Ƚ��������Զ����Ƿ����
 * @another ����Ƚϵ���һ�����Զ���
 * @return �������Զ����Ƿ����
 */
bool ColumnDef::operator == (const ColumnDef &another) {
	if (strcmp(m_name, another.m_name))
		return false;
	if (m_type != another.m_type)
		return false;
	if (m_collation != another.m_collation)
		return false;
	if (m_longVar != another.m_longVar)
		return false;
	if (m_size != another.m_size)
		return false;
	if (m_mysqlSize != another.m_mysqlSize)
		return false;
	if (m_lenBytes != another.m_lenBytes)
		return false;
	if (m_nullable != another.m_nullable)
		return false;
	if (m_inIndex != another.m_inIndex)
		return false;
	if (m_offset != another.m_offset)
		return false;
	if (m_mysqlOffset != another.m_mysqlOffset)
		return false;
	if (m_nullBitmapOffset != another.m_nullBitmapOffset)
		return false;
	if (m_cacheUpdate != another.m_cacheUpdate)
		return false;
	if (m_no != another.m_no)
		return false;
	if (m_colGrpNo != another.m_colGrpNo)
		return false;
	if (m_colGrpOffset != another.m_colGrpOffset)
		return false;
	return true;
}

/** ������Զ����Ƿ�Ϸ�
 * @throw NtseException ���Զ��岻�Ϸ�
 */
void ColumnDef::check() throw(NtseException) {
	if (strlen(m_name) > Limits::MAX_NAME_LEN)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Length of column name can not exceed %d: %s.", Limits::MAX_NAME_LEN, m_name);
}

/** �ӻ������ж�ȡ���Զ���
 * @param buf �洢���Զ������л�����Ļ�����
 * @param size ��������С
 * @throw NtseException ���������
 * @return ���Զ������л���ռ�õĿռ��С
 */
void ColumnDef::readFromKV(byte* buf, u32 size) throw(NtseException) {
	u8 type, colltype;

	KVParser kvParser;
	kvParser.deserialize(buf, size);
	
	m_name = System::strdup(kvParser.getValue(string(NAME), string(NAME_DEFAULT)).c_str());
	type = (u8)kvParser.getValueI(string(TYPE), TYPE_DEFAULT);
	m_type = (ColumnType)type;
	
	if (isString() || isLob()) {
		colltype = (u8)kvParser.getValueI(string(COLLATION), COLLATION_DEFAULT);
		m_collation = (CollType)colltype;
	} else {
		m_collation = COLL_BIN;
	}

	if (varSized()) {
		m_size = (u16)kvParser.getValueI(string(SIZE), SIZE_DEFALUT);
		if (m_type == CT_VARCHAR || m_type == CT_VARBINARY)
			m_lenBytes = m_size > 256? 2: 1;
		else
			m_lenBytes = 0;
	} else {
		calcSize();
		m_lenBytes = 0;
	}
	m_mysqlSize = (u16)kvParser.getValueI(string(MYSQL_SIZE), MYSQL_SIZE_DEFAULT);

	m_nullable = kvParser.getValueB(string(NULLABLE), NULLABLE_DEFAULT);
	if (!m_nullable)
		m_nullBitmapOffset = 0;

	m_longVar = kvParser.getValueB(string(LONGVAR), LONGVAR_DEFAULT);

	m_inIndex = false;
	m_cacheUpdate = kvParser.getValueB(string(CACHEUPDATE), CACHEUPDATE_DEFAULT);;
	m_prtype.m_flags = (u16)kvParser.getValueI(string(PRFLAGS), PRFLAGS_DEFAULT);
	if (isDecimal()) {
		m_prtype.m_precision = (u8)kvParser.getValueI(string(PRPRECISON), PRPRECISION_DEFAULT);
		m_prtype.m_deicmal = (u8)kvParser.getValueI(string(PRDECIMAL), PRDECIMAL_DEFAULT);
	}

	m_colGrpNo = (u8)kvParser.getValueI(string(COLGRPNO), COL_GRP_NO_DEFAULT);
	m_colGrpOffset = (u16)kvParser.getValueI(string(COLGRPOFFSET), COL_GRP_OFFSET_DEFAULT);
	
	calcCompressSize();
}

/** �����Զ������л���ָ����������
 * @param buf �洢���л�����Ļ�����
 * @param size ��������С
 * @throw NtseException ���������
 * @return ���Զ������л���ռ�õĿռ��С
 */
void ColumnDef::writeToKV(byte **buf, u32 *size) const throw(NtseException) {
	KVParser kvParse;
	assert(m_name != NULL);
	kvParse.setValue(string(NAME), string(m_name));
	kvParse.setValueI(string(TYPE), m_type);
	if (isString() || isLob()) {
		kvParse.setValueI(string(COLLATION), m_collation);
	}

	if (varSized()) {
		kvParse.setValueI(string(SIZE), m_size);
	}
	kvParse.setValueI(string(MYSQL_SIZE), m_mysqlSize);
	
	// TODO���Ƿ���Բ��ﻯ������ļ�
	kvParse.setValueB(string(LONGVAR), m_longVar);
	kvParse.setValueB(string(NULLABLE), m_nullable);
	kvParse.setValueB(string(CACHEUPDATE), m_cacheUpdate);
	kvParse.setValueI(string(PRFLAGS), m_prtype.m_flags);
	if (isDecimal()) {
		kvParse.setValueI(string(PRPRECISON), m_prtype.m_precision);
		kvParse.setValueI(string(PRDECIMAL), m_prtype.m_deicmal);
	}
	kvParse.setValueI(string(COLGRPNO), m_colGrpNo);
	kvParse.setValueI(string(COLGRPOFFSET), m_colGrpOffset);
	kvParse.serialize(buf, size);
}

/** �����Ƿ����ѹ����ѹ����Ĵ�С */
void ColumnDef::calcCompressSize() {
	switch (m_type) {
	case CT_SMALLINT:
	case CT_MEDIUMINT:
	case CT_INT:
	case CT_BIGINT:
		m_compressable = true;
		m_maxCompressSize = m_size + 1;
		break;
	default:
		m_compressable = false;
		m_maxCompressSize = m_size;
	}
}

const char *IndexDef::NAME = "name";
const char *IndexDef::NAME_DEFALUT = "default";
const char *IndexDef::PRIMARYKEY = "primary_key";
const char *IndexDef::UNIQUE = "unique";
const char *IndexDef::ONLINE = "online";
const char *IndexDef::PREFIX = "prefix";
const char *IndexDef::HASLOB = "has_lob";
const char *IndexDef::MAXKEYSIZE = "max_key_size";
const char *IndexDef::NUMCOLS = "num_cols";
const char *IndexDef::BMBYTES = "bm_bytes";
const char *IndexDef::SPLITFACTOR = "split_factor";
const char *IndexDef::COLUMNS = "columns";
const char *IndexDef::COLUMNS_DEFAULT = "";
const char *IndexDef::OFFSET = "offsets";
const char *IndexDef::OFFSET_DEFAULT = "";
const char *IndexDef::PREFIXS = "prefixs";
const char *IndexDef::PREFIXS_DEFAULT = "";

/** ����һ������������� */
IndexDef::IndexDef() {
	m_name = NULL;
	m_primaryKey = IndexDef::PRIMARYKEY_DEFAULT;
	m_unique = IndexDef::UNIQUE_DEFAULT;
	m_online = IndexDef::ONLINE_DEFAULT;
	m_prefix = IndexDef::PREFIX_DEFAULT;
	m_hasLob = IndexDef::HASLOB_DEFAULT;
	m_maxKeySize = IndexDef::MAXKEYSIZE_DEFAULT;
	m_numCols = IndexDef::NUMCOLS_DEFAULT;
	m_bmBytes = IndexDef::BMBYTES_DEFAULT;
	m_splitFactor = IndexDef::SPLITFACTOR_DEFAULT;
	m_columns = NULL;
	m_offsets = NULL;
	m_prefixLens = NULL;
}

/**
 * ����һ�������������
 *
 * @param name ������
 * @param numCols �������Ը���
 * @param columns ���������ԣ�ע����m_inIndex���ܻᱻ�޸�
 * @param unique �Ƿ�ΪΨһ������
 * @param primaryKey �Ƿ�Ϊ��������
 */
IndexDef::IndexDef(const char *name, u16 numCols, ColumnDef **columns, u32 *prefixLens, bool unique, bool primaryKey, bool online) {
	assert(unique || !primaryKey);	// ����һ��Ψһ
	m_name = System::strdup(name);
	m_numCols = numCols;
	m_columns = new u16[numCols];
	m_prefixLens = new u32[numCols];
	m_primaryKey = primaryKey;
	m_unique = unique;
	m_online = online;
	m_prefix = false;
	m_hasLob = false;
	m_splitFactor = SMART_SPLIT_FACTOR;

	for (u16 i = 0; i < numCols; i++) {
		m_columns[i] = columns[i]->m_no;
		m_prefixLens[i] = prefixLens[i];
		if (prefixLens[i] > 0)
			m_prefix = true;
	}
	m_offsets = new u16[numCols];
	
	m_maxKeySize = 0;
	u16 nullableCols = 0;
	for (size_t i = 0; i < numCols; i++) {
		ColumnDef *colDef = columns[i];
		if (colDef->m_nullable)
			nullableCols++;		
		m_offsets[i] = m_maxKeySize;
		if (m_prefixLens[i] > 0) {
			if (colDef->m_type == CT_VARCHAR || colDef->m_type == CT_VARBINARY ||
				colDef->m_type == CT_CHAR || colDef->m_type == CT_BINARY) {
				m_maxKeySize = (u16)(m_maxKeySize + m_prefixLens[i] + colDef->m_lenBytes);
			} else {
				assert(colDef->isLob());
				m_maxKeySize = prefixLens[i] > 255 ? (m_maxKeySize + m_prefixLens[i] + 2) : (m_maxKeySize + m_prefixLens[i] + 1);
				m_hasLob = true;
			}
		} else 
			m_maxKeySize = (u16)(m_maxKeySize + colDef->m_maxCompressSize);
		colDef->m_inIndex = true;
	}
	m_bmBytes = (u8)((nullableCols + 7) / 8);
	m_maxKeySize = (u16)(m_maxKeySize + m_bmBytes);
	for (size_t i = 0; i < numCols; i++)
		m_offsets[i] = (u16)(m_offsets[i] + m_bmBytes);
}

/**
 * ����һ�������������
 *
 * @param copy ������һ��������Ŀ���
 */
IndexDef::IndexDef(const IndexDef *copy) {
	*this = *copy;
	m_name = System::strdup(copy->m_name);
	m_columns = new u16[m_numCols];
	memcpy(m_columns, copy->m_columns, sizeof(u16) * m_numCols);
	m_offsets = new u16[m_numCols];
	memcpy(m_offsets, copy->m_offsets, sizeof(u16) * m_numCols);
	m_prefixLens = new u32[m_numCols];
	memcpy(m_prefixLens, copy->m_prefixLens, sizeof(u32) * m_numCols);
}

IndexDef::~IndexDef() {
	delete []m_name;
	delete []m_columns;
	delete []m_offsets;
	delete []m_prefixLens;
}

/**
 * �Ƚϵ�ǰ����������ָ�����������Ƿ���ͬ
 *
 * @param another ����Ƚϵ���������
 * @return �Ƿ���ͬ
 */
bool IndexDef::operator == (const IndexDef &another) {
	if (strcmp(m_name, another.m_name))
		return false;
	if (m_primaryKey != another.m_primaryKey)
		return false;
	if (m_unique != another.m_unique)
		return false;
	if (m_online != another.m_online)
		return false;
	if (m_hasLob != another.m_hasLob)
		return false;
	if (m_prefix != another.m_prefix)
		return false;
	if (m_maxKeySize != another.m_maxKeySize)
		return false;
	if (m_numCols != another.m_numCols)
		return false;
	if (m_bmBytes != another.m_bmBytes)
		return false;
	if (m_splitFactor != another.m_splitFactor)
		return false;
	for (u16 i = 0; i < m_numCols; i++) {
		if (m_columns[i] != another.m_columns[i])
			return false;
		if (m_offsets[i] != another.m_offsets[i])
			return false;
		if (m_prefixLens[i] != another.m_prefixLens[i])
			return false;
	}
	return true;
}

/**
 * ����һ�����л�����������
 *
 * @param buf ���л�����
 * @param size buf�Ĵ�С
 * @return �������л���ռ�õĿռ��С
 * @throw NtseException ��ȡԽ��
 */
void IndexDef::readFromKV(byte* buf, u32 size) throw(NtseException) {
	KVParser kvParser;
	kvParser.deserialize(buf, size);
	m_name = System::strdup(kvParser.getValue(string(NAME), string(NAME_DEFALUT)).c_str());
	m_primaryKey = kvParser.getValueB(string(PRIMARYKEY), PRIMARYKEY_DEFAULT);
	m_unique = kvParser.getValueB(string(UNIQUE), UNIQUE_DEFAULT);
	m_online = kvParser.getValueB(string(ONLINE), ONLINE_DEFAULT);
	m_prefix = kvParser.getValueB(string(PREFIX), PREFIX_DEFAULT);
	m_hasLob = kvParser.getValueB(string(HASLOB), HASLOB_DEFAULT);

	m_maxKeySize = (u16)kvParser.getValueI(string(MAXKEYSIZE), MAXKEYSIZE_DEFAULT);
	m_numCols = (u16)kvParser.getValueI(string(NUMCOLS), NUMCOLS_DEFAULT);
	m_bmBytes = (u8)kvParser.getValueI(string(BMBYTES), BMBYTES_DEFAULT);
	m_splitFactor = (s8)kvParser.getValueI(string(SPLITFACTOR), SPLITFACTOR_DEFAULT);

	string columnStr = kvParser.getValue(string(COLUMNS), string(COLUMNS_DEFAULT));
	string offsetStr = kvParser.getValue(string(OFFSET), string(OFFSET_DEFAULT));
	string prefixStr = kvParser.getValue(string(PREFIXS), string(PREFIXS_DEFAULT));

	Array<string> *columns = kvParser.parseArray((byte*)columnStr.c_str(), columnStr.size());
	Array<string> *offsets = kvParser.parseArray((byte*)offsetStr.c_str(), offsetStr.size());
	Array<string> *prefixs = kvParser.parseArray((byte*)prefixStr.c_str(), prefixStr.size());

	assert(m_numCols == columns->getSize());
	m_columns = new u16[m_numCols];
	for (int j = 0; j < m_numCols; j++) {
		m_columns[j] = (u16)Parser::parseInt((*columns)[j].c_str());
	}
	delete columns;

	assert(m_numCols == offsets->getSize());
	m_offsets = new u16[m_numCols];
	for (int j = 0; j < m_numCols; j++) {
		m_offsets[j] = (u16)Parser::parseInt((*offsets)[j].c_str());
	}
	delete offsets;

	assert(m_numCols == prefixs->getSize());
	m_prefixLens = new u32[m_numCols];
	for (int j = 0; j < m_numCols; j++) {
		m_prefixLens[j] = (u16)Parser::parseInt((*prefixs)[j].c_str());
	}
	delete prefixs;
}

/**
 * ���л�һ����������
 *
 * @param buf �洢���л�����Ļ�����
 * @param size buf�Ĵ�С
 * @return �������л���ռ�õĿռ��С
 * @throw NtseException дԽ��
 */
void IndexDef::writeToKV(byte **buf, u32 *size) const throw(NtseException) {
	KVParser kvParser;
	assert(m_name != NULL);
	kvParser.setValue(string(NAME), string(m_name));
	kvParser.setValueB(string(PRIMARYKEY), m_primaryKey);
	kvParser.setValueB(string(UNIQUE), m_unique);
	kvParser.setValueB(string(ONLINE), m_online);
	kvParser.setValueB(string(PREFIX), m_prefix);
	kvParser.setValueB(string(HASLOB), m_hasLob);
	kvParser.setValueI(string(MAXKEYSIZE), m_maxKeySize);
	kvParser.setValueI(string(NUMCOLS), m_numCols);
	kvParser.setValueI(string(BMBYTES), m_bmBytes);
	kvParser.setValueI(string(SPLITFACTOR), m_splitFactor);
	
	byte *tmpBuffer = NULL;
	u32 tmpSize = 0;
	assert(m_numCols != 0 || m_columns != NULL);
	Array<int> columns;
	for (int j = 0; j < m_numCols; j++) {
		columns.push(m_columns[j]);
	}
	assert(columns.getSize() != 0);
	kvParser.serializeArray(columns, &tmpBuffer, &tmpSize);
	kvParser.setValue(string(COLUMNS), string((char *)tmpBuffer, tmpSize));
	delete[] tmpBuffer;

	assert(m_numCols != 0 || m_offsets != NULL);
	Array<int> offsets;
	for (int j = 0; j < m_numCols; j++) {
		offsets.push(m_offsets[j]);
	}
	kvParser.serializeArray(offsets, &tmpBuffer, &tmpSize);
	kvParser.setValue(string(OFFSET), string((char *)tmpBuffer, tmpSize));
	delete[] tmpBuffer;

	assert(m_numCols != 0 || m_prefixLens != NULL);
	Array<int> prefixs;
	for (int j = 0; j < m_numCols; j++) {
		prefixs.push(m_prefixLens[j]);
	}
	kvParser.serializeArray(prefixs, &tmpBuffer, &tmpSize);
	kvParser.setValue(string(PREFIXS), string((char *)tmpBuffer, tmpSize));
	delete[] tmpBuffer;
	

	kvParser.serialize(buf, size);
}

/**
 * ������������Ƿ�Ϸ�
 * @param tableDef ������Ķ���
 * @throw ���������Ƿ�Ϸ�
 */
void IndexDef::check(const TableDef *tableDef) const throw(NtseException) {
	if (strlen(m_name) > Limits::MAX_NAME_LEN)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Length of column name can not exceed %d: %s.", Limits::MAX_NAME_LEN, m_name);
	if (m_maxKeySize > DrsIndice::IDX_MAX_KEY_LEN)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Max length of index can not be greater than %d", DrsIndice::IDX_MAX_KEY_LEN);
	if (m_splitFactor != SMART_SPLIT_FACTOR && (m_splitFactor > MAX_SPLIT_FACTOR || m_splitFactor < MIN_SPLIT_FACTOR))
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Invalid SPLIT_FACTOR: %d, should be in [%d, %d] or %d.",
			m_splitFactor, MIN_SPLIT_FACTOR, MAX_SPLIT_FACTOR, SMART_SPLIT_FACTOR);
	/*if (isNew && tableDef->getIndexNo(m_name) != -1)
		NTSE_THROW(NTSE_EC_DUPINDEX, "Index %s already exists.", m_name);*/
	for (u16 i = 0; i < m_numCols; i++) {
		ColumnDef *col = tableDef->m_columns[m_columns[i]];
		if (col->m_cacheUpdate)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not build index on column '%s' who's update can be cached.", col->m_name);
		if (m_primaryKey && col->m_nullable)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not build primary key on nullable column '%s'.", col->m_name);
	}
}

/** �ж������Ƿ����Ϊ�����ȼ���
 * @param tableDef ������Ķ���
 * @return �����Ƿ����Ϊ�����ȼ���
 */
bool IndexDef::isPkeCandidate(const TableDef *tableDef) const {
	if (!m_unique)
		return false;
	if (m_primaryKey)
		return true;
	for (u16 i = 0; i < m_numCols; i++) {
		ColumnDef *col = tableDef->m_columns[m_columns[i]];
		if (col->m_nullable)
			return false;
	}
	return true;
}

bool IndexDef::hasLob() const {
	return m_hasLob;
}

/** �ж�ָ����������ϵ���Ƿ����
 * @param splitFactor	ָ����ϵ��
 * @return true��ʾ����false��ʾ������
 */
bool IndexDef::isSplitFactorValid( s8 splitFactor ) {
	return splitFactor == IndexDef::SMART_SPLIT_FACTOR || (splitFactor <= IndexDef::MAX_SPLIT_FACTOR && splitFactor >= IndexDef::MIN_SPLIT_FACTOR);
}
/************************************************************************/
/* ���������                                                                     */
/************************************************************************/

const char * ColGroupDef::COL_GRP_NO = "col_group_no";
const char * ColGroupDef::COL_NUM_COLS = "num_columns";
const char * ColGroupDef::COL_NOS = "col_nos";

/**
 * ����һ�������鶨��
 */
ColGroupDef::ColGroupDef() : m_colGrpNo(0), m_numCols(0), m_colNos(NULL) {
}

/**
 * ����һ�������鶨��
 * @param colGrpNo �������
 * @param numCols �������е�������
 * @param columns ���������Զ��壬��������ż����������е�λ�ÿ��ܱ��޸�
 */
ColGroupDef::ColGroupDef(u8 colGrpNo, u16 numCols, ColumnDef **columns) {
	assert(NULL != columns);
	m_colGrpNo = colGrpNo;
	m_numCols = numCols;
	m_colNos = new u16[numCols];
	for (u16 i = 0; i < numCols; i++) {
		m_colNos[i] = columns[i]->m_no;
		columns[i]->m_colGrpNo = m_colGrpNo;
		columns[i]->m_colGrpOffset = i;
	}	
}

/**
* ��������һ�������鶨��
* @param copy Ҫ�����������鶨��
*/
ColGroupDef::ColGroupDef(const ColGroupDef *copy) {
	*this = *copy;
	m_colNos = new u16[m_numCols];
	memcpy(m_colNos, copy->m_colNos, sizeof(u16) * m_numCols);
}

ColGroupDef::~ColGroupDef() {
	delete []m_colNos;
}

/**
 * �Ƚ����������鶨���Ƿ����
 * @another Ҫ�Ƚϵ�����һ�������鶨��
 */
bool ColGroupDef::operator == (const ColGroupDef& another) {
	if (m_numCols != another.m_numCols)
		return false;
	for (u16 i = 0; i < m_numCols; i++) {
		if (m_colNos[i] != another.m_colNos[i])
			return false;
	}
	return true;
}

/**
 * ��Key-Value��ʽ�������鶨�����л��������ع������鶨��
 * @param buf  ���л����ݵ�ַ
 * @param size ���л����ݳ���
 * @throw NtseException ��ȡ���л����ݳ���
 */
void ColGroupDef::readFromKV(byte *buf, u32 size) throw(NtseException) {
	KVParser kvparser;
	kvparser.deserialize(buf, size);
	m_colGrpNo = (u8)kvparser.getValueI(string(COL_GRP_NO), (u8)-1);
	m_numCols = (u16)kvparser.getValueI(string(COL_NUM_COLS), 0);
	string colNosValue = kvparser.getValue(string(COL_NOS), string(""));
	Array<string> *colNosArr = kvparser.parseArray((byte *)colNosValue.c_str(), colNosValue.size());
	m_colNos = new u16[m_numCols];
	for (uint i = 0; i < colNosArr->getSize(); i++) {
		m_colNos[i] = (u16)Parser::parseInt((*colNosArr)[i].c_str());
	}
	delete colNosArr;
}

/**
 * ��Key-Value��ʽ���л������鶨��
 * @param buf  ���л���������ַ
 * @param size ���л���������󳤶�
 * @throw NtseException ���л�����
 */
void ColGroupDef::writeToKV(byte **buf, u32 *size) const throw (NtseException) {
	KVParser kvparser;
	kvparser.setValueI(string(COL_GRP_NO), m_colGrpNo);
	kvparser.setValueI(string(COL_NUM_COLS), m_numCols);
	Array<int> colNosArr;
	for (uint i = 0; i < m_numCols; i++)
		colNosArr.push(m_colNos[i]);

	byte *tmpBuffer = NULL;
	u32 tmpSize = 0;
	kvparser.serializeArray(colNosArr, &tmpBuffer, &tmpSize);
	kvparser.setValue(string(COL_NOS), string((char *)tmpBuffer, (int)tmpSize));
	delete []tmpBuffer;

	kvparser.serialize(buf, size);
}

/**
* �������鶨��Ķ��������л��������ع������鶨��
* @param buf  ���л����ݵ�ַ
* @param size ���л����ݳ���
* @throw NtseException ��ȡ���л����ݳ���
*/
size_t ColGroupDef::read(byte *buf, size_t size) throw(NtseException) {
	Stream s(buf, size);

	s.read(&m_colGrpNo);
	s.read(&m_numCols);
	m_colNos = new u16[m_numCols];
	for (int j = 0; j < m_numCols; j++)
		s.read(&m_colNos[j]);

	return s.getSize();
}

/**
* �Զ��������л������鶨��
* @param buf  ���л���������ַ
* @param size ���л���������󳤶�
* @throw NtseException ���л�����
*/
size_t ColGroupDef::write(byte *buf, size_t size) const throw (NtseException) {
	Stream s(buf, size);

	s.write(m_colGrpNo);
	s.write(m_numCols);
	for (int j = 0; j < m_numCols; j++) {
		s.write(m_colNos[j]);
	}

	return s.getSize();
}

/**
 * ��������鶨�����л���Ĵ�С
 */
size_t ColGroupDef::getSerializeSize() const {
	size_t sum = sizeof(u8) + sizeof(u16) + m_numCols * sizeof(u16);
	return sum;
}

/** 
 * �����¾ɱ������¹��������鶨��
 * ע���������������ԵĹ���������append�����һ����������
 * @param newTableDef �±���
 * @param oldTableDef �ɱ���
 * @param oldColGrpDef �������鶨��
 * @return �������鶨��
 */
Array<ColGroupDef *> *ColGroupDef::buildNewColGrpDef(const TableDef *newTableDef, const TableDef *oldTableDef, Array<ColGroupDef *> *oldColGrpDef) {
	assert(newTableDef && newTableDef);
	assert(oldColGrpDef);
	Array<ColGroupDef *> *newColGrpArr = new Array<ColGroupDef *>();
	u8 newNumColGrps = 0;
	u8 *isInColGrp = new u8[newTableDef->m_numCols];
	memset(isInColGrp, 0, sizeof(u8) * newTableDef->m_numCols);
	for (uint i = 0; i < oldColGrpDef->getSize(); i++) {
		uint numCols = (*oldColGrpDef)[i]->m_numCols;
		ColumnDef **colDefs = new ColumnDef *[numCols];
		uint newNumCols = 0;
		for (uint j = 0; j < numCols; j++) {
			u16 no = (*oldColGrpDef)[i]->m_colNos[j];//�������鶨���л�ȡ�����ڱ��еı��
			const char *colName = oldTableDef->m_columns[no]->m_name;
			ColumnDef *col = newTableDef->getColumnDef(colName);
			if (col) {
				colDefs[newNumCols++] = col;
				isInColGrp[col->m_no] = 1;
			}
		}
		if (newNumCols > 0) {
			newColGrpArr->push(new ColGroupDef(newNumColGrps++, (u16)newNumCols, colDefs));
		}
		delete []colDefs;
	}
	Array<ColumnDef *> tmpArr;
	for (uint i  = 0; i < newTableDef->m_numCols; i++) {
		if (!isInColGrp[i]) {
			tmpArr.push(newTableDef->m_columns[i]);
		}
	}
	if (tmpArr.getSize() > 0) {
		if (newColGrpArr->getSize() == 0) {
			ColumnDef **colDefs = new ColumnDef *[tmpArr.getSize()];
			for (uint i = 0; i < tmpArr.getSize(); i++)
				colDefs[i] = tmpArr[i];
			newColGrpArr->push(new ColGroupDef(0, (u16)tmpArr.getSize(), colDefs));
			delete []colDefs;
		} else {				
			u8 appendcolGrpNo = (u8)(newColGrpArr->getSize() - 1);
			ColGroupDef *oldColGrpDef = (*newColGrpArr)[appendcolGrpNo];
			u16 oldNumCols = (*newColGrpArr)[appendcolGrpNo]->m_numCols;
			u16 numCols = oldNumCols + (u16)tmpArr.getSize();
			ColumnDef **colDefs = new ColumnDef *[numCols];

			for (uint i = 0; i < numCols; i++) {
				if (i < oldNumCols) 
					colDefs[i] = newTableDef->m_columns[oldColGrpDef->m_colNos[i]];
				else
					colDefs[i] = tmpArr[i - oldNumCols];
			}
			delete (*newColGrpArr)[appendcolGrpNo]; 
			(*newColGrpArr)[appendcolGrpNo] = new ColGroupDef(appendcolGrpNo, numCols, colDefs);
			delete []colDefs;
		}
	}
	delete []isInColGrp;
	for (uint i = 0; i < oldColGrpDef->getSize(); i++)
		delete (*oldColGrpDef)[i];
	delete oldColGrpDef;
	if (newColGrpArr->getSize() != 0) {
		return newColGrpArr;
	} else {
		delete newColGrpArr;
		return NULL;
	}
}

/************************************************************************/
/* �������                                                                     */
/************************************************************************/

const char *TableDef::ID = "id";
const char *TableDef::SCHEMANAME = "schema_name";
const char *TableDef::SCHEMANAME_DEFAULT = "default";
const char *TableDef::NAME = "name";
const char *TableDef::NAME_DEFAULT = "default";
const char *TableDef::NUMCOLS = "num_cols";
const char *TableDef::NULLABLECOLS = "nullable_cols";
const char *TableDef::NUMINDICE = "num_indice";
const char *TableDef::RECFORMAT = "rec_format";
const char *TableDef::ORIG_RECFORMAT = "orig_rec_format";
const char *TableDef::MAXRECSIZE = "max_recsize";
const char *TableDef::MAXMYSQLRECSIZE = "max_mysql_recsize";
const char *TableDef::USEMMS = "use_mms";
const char *TableDef::CACHEUPDATE = "cache_update";
const char *TableDef::UPDATECACHETIME = "update_cache_time";
const char *TableDef::COMPRESSLOBS = "compress_lobs";
const char *TableDef::BMBYTES = "bm_bytes";
const char *TableDef::PCTFREE = "pct_free";
const char *TableDef::INCRSIZE = "incr_size";
#ifdef TNT_ENGINE
//const char *TableDef::TNTTABLE = "tnt_table";
const char *TableDef::TABLESTATUS = "table_status";
#endif
const char *TableDef::INDEXONLY = "index_only";
const char *TableDef::COLUMN = "column";
const char *TableDef::COLUMN_DEFAULT = "";
const char *TableDef::INDEX = "index";
const char *TableDef::INDEX_DEFAULT = "";
const char *TableDef::FIXLEN = "fix_len";
const char *TableDef::COMPRESS_TABLE = "compress_rows";
const char *TableDef::COL_GROUPS_NUM = "num_col_groups";
const char *TableDef::COL_GROUPS = "column_groups";
const char *TableDef::COL_GROUPS_DEFAULT = "";
const char *TableDef::ROW_COMPRESS_CFG = "row_compress_cfg";
const char *TableDef::ROW_COMPRESS_CFG_DEFAULT = "";

TableDef::TableDef() {
	m_id = ID_DEFAULT;
	m_schemaName = m_name = NULL;
	m_numCols = m_numIndice = 0;
	m_columns = NULL;
	m_indice = NULL;
	m_pkey = NULL;
	m_pke = NULL;
	m_useMms = USEMMS_DEFAULT;
	m_cacheUpdate = CACHEUPDATE_DEFAULT;
	m_updateCacheTime = UPDATECACHETIME_DEFAULT;
	m_compressLobs = COMPRESSLOBS_DEFAULT;
	m_pctFree = PCTFREE_DEFAULT;
	m_incrSize = INCRSIZE_DEFAULT;
#ifdef TNT_ENGINE
	//m_tntTable = TNTTABLE_DEFAULT;
	m_tableStatus = TABLE_STATUS_DEFAULT;
#endif
	m_indexOnly = INDEXONLY_DEFAULT;
	m_isCompressedTbl = DEFAULT_COMPRESS_ROW;
	m_numColGrps = COL_GROUPS_NUM_DEFAULT;
	m_colGrps = NULL;
	m_rowCompressCfg = NULL;
	m_hasLob = false;
	m_hasLongVar = false;
	m_fixLen = FIXLEN_DEFAULT;
}

TableDef::TableDef(const TableDef *copy) {
	*this = *copy;
	m_schemaName = System::strdup(copy->m_schemaName);
	m_name = System::strdup(copy->m_name);
	m_columns = (m_numCols) ? (new ColumnDef *[m_numCols]): NULL;
	for (u16 i = 0; i < m_numCols; i++)
		m_columns[i] = new ColumnDef(copy->m_columns[i]);
	m_indice = (m_numIndice) ? (new IndexDef *[m_numIndice]): NULL;
	for (u16 i = 0; i < m_numIndice; i++) {
		m_indice[i] = new IndexDef(copy->m_indice[i]);
		if (m_indice[i]->m_primaryKey)
			m_pkey = m_indice[i];
	}
	m_colGrps = NULL;
	if (copy->m_colGrps) {
		m_colGrps = new ColGroupDef *[m_numColGrps];
		for (u8 i = 0; i < m_numColGrps; i++) {
			m_colGrps[i] = new ColGroupDef(copy->m_colGrps[i]);
		}
	}		
	m_rowCompressCfg = copy->m_rowCompressCfg ? new RowCompressCfg(*copy->m_rowCompressCfg) : NULL;
}

TableDef::~TableDef() {
	for (u16 i = 0; i < m_numIndice; i++)
		delete m_indice[i];
	delete []m_indice;
	for (u16 i = 0; i < m_numCols; i++)
		delete m_columns[i];
	delete []m_columns;
	delete []m_schemaName;
	delete []m_name;

	delete m_rowCompressCfg;
	for (u16 i = 0; i < m_numColGrps; i++) {
		delete m_colGrps[i];
	}
	delete []m_colGrps;
}

bool TableDef::operator == (const TableDef &another) {
	if (m_id != another.m_id)
		return false;
	if (strcmp(m_schemaName, another.m_schemaName))
		return false;
	if (strcmp(m_name, another.m_name))
		return false;
	if (m_numCols != another.m_numCols)
		return false;
	if (m_nullableCols != another.m_nullableCols)
		return false;
	if (m_numIndice != another.m_numIndice)
		return false;
	if (m_recFormat != another.m_recFormat)
		return false;
	if (m_origRecFormat != another.m_origRecFormat)
		return false;
	if (m_maxRecSize != another.m_maxRecSize)
		return false;
	if (m_useMms != another.m_useMms)
		return false;
	if (m_cacheUpdate != another.m_cacheUpdate)
		return false;
	if (m_updateCacheTime != another.m_updateCacheTime)
		return false;
	if (m_compressLobs != another.m_compressLobs)
		return false;
	for (u16 i = 0; i < m_numCols; i++)
		if (!(*m_columns[i] == *another.m_columns[i]))
			return false;
	for (u16 i = 0; i < m_numIndice; i++)
		if (!(*m_indice[i] == *another.m_indice[i]))
			return false;
	if (m_pctFree != another.m_pctFree)
		return false;
	if (m_incrSize != another.m_incrSize)
		return false;
	if (m_isCompressedTbl != another.m_isCompressedTbl)
		return false;
	if (m_numColGrps != another.m_numColGrps)
		return false;
	for (u8 i = 0; i < m_numColGrps; i++) {
		if (!(*m_colGrps[i] == *another.m_colGrps[i]))
			return false;
	}
	if (m_rowCompressCfg && another.m_rowCompressCfg) {
		if (!(*m_rowCompressCfg == *another.m_rowCompressCfg)) {
			return false;
		}
	} else {
		if (!(!m_rowCompressCfg && !another.m_rowCompressCfg)) {
			return false;
		}
	}
	if (m_indexOnly != another.m_indexOnly)
		return false;
	if (m_fixLen != another.m_fixLen)
		return false;
	assert(m_hasLob == another.m_hasLob);
	assert(m_hasLongVar == another.m_hasLongVar);
#ifdef TNT_ENGINE
	/*if (m_tntTable != another.m_tntTable) {
		return false;
	}*/
	if (m_tableStatus != another.m_tableStatus) {
		return false;
	}
#endif
	return true;
}

/**
 * ������ͨ��ID�����Ӧ�Ĵ���������ID
 *
 * @param normalTableId ��ͨ��ID
 * @return ����������ID
 */
u16 TableDef::getVirtualLobTableId(u16 normalTableId) {
	assert(normalTableId >= MIN_NORMAL_TABLEID && normalTableId <= MAX_TEMP_TABLEID);
	return normalTableId + VLOB_TABLEID_DIFF;
}

/**
 * ���ݴ���������ID�����Ӧ����ͨ��ID
 *
 * @param virtualLobTableId ����������ID
 * @return ��ͨ��ID
 */
u16 TableDef::getNormalTableId(u16 virtualLobTableId) {
	assert(tableIdIsVirtualLob(virtualLobTableId));
	return virtualLobTableId - VLOB_TABLEID_DIFF;
}

/** �ж�ָ���ı�ID�Ƿ�Ϊ��ʱ���ID
 * @param tableId ��ID
 */
bool TableDef::tableIdIsTemp(u16 tableId) {
	return tableId > MAX_NORMAL_TABLEID && tableId <= MAX_TEMP_TABLEID;
}

/** �ж�ָ���ı�ID�Ƿ�Ϊ�����������ID
 * @param tableId ��ID
 */
bool TableDef::tableIdIsVirtualLob(u16 tableId) {
	return tableId >= MIN_VLOB_TABLEID && tableId <= MAX_VLOB_TABLEID;
}

/** �ж�ָ���ı�ID�Ƿ�Ϊ��ͨ���ID
 * @param tableId ��ID
 */
bool TableDef::tableIdIsNormal(u16 tableId) {
	return tableId >= MIN_NORMAL_TABLEID && tableId <= MAX_NORMAL_TABLEID;
}

/** 
 * �����л�����
 *
 * @param buf �洢�������л������ڴ�
 * @param size buf�ڴ��С
 * @return �������л���ռ�õĿռ��С
 * @throw NtseException Խ���������Ƿ���
 */
void TableDef::read(byte *buf, u32 size) throw(NtseException) {
	KVParser kvParse;
	kvParse.deserialize(buf, size);

	m_id = (u16)kvParse.getValueI(string(ID), ID_DEFAULT);
	m_schemaName = System::strdup(kvParse.getValue(string(SCHEMANAME), string(SCHEMANAME_DEFAULT)).c_str());
	m_name = System::strdup(kvParse.getValue(string(NAME), string(NAME_DEFAULT)).c_str());
	m_numCols = (u16)kvParse.getValueI(string(NUMCOLS), NUMCOLS_DEFAULT);
	m_nullableCols = (u16)kvParse.getValueI(NULLABLECOLS, NULLABLECOLS_DEFAULT);
	m_numIndice = (u8)kvParse.getValueI(NUMINDICE, NUMINDICE_DEFAULT);
	m_recFormat = (RecFormat)(u8)kvParse.getValueI(RECFORMAT, RECFORMAT_DEFAULT);
	m_origRecFormat = (RecFormat)(u8)kvParse.getValueI(ORIG_RECFORMAT, RECFORMAT_DEFAULT);
	m_maxRecSize = (u16)kvParse.getValueI(MAXRECSIZE, MAXRECSIZE_DEFAULT);
	m_maxMysqlRecSize = (u16)kvParse.getValueI(MAXMYSQLRECSIZE, MAXMYSQLRECSIZE_DEFAULT);
	m_useMms = kvParse.getValueB(USEMMS, USEMMS_DEFAULT);
	m_cacheUpdate = kvParse.getValueB(CACHEUPDATE, CACHEUPDATE_DEFAULT);
	m_updateCacheTime = (u16)kvParse.getValueI(UPDATECACHETIME, UPDATECACHETIME_DEFAULT);
	m_compressLobs = kvParse.getValueB(COMPRESSLOBS, COMPRESSLOBS_DEFAULT);
	m_bmBytes = (u8)kvParse.getValueI(BMBYTES, BMBYTES_DEFAULT);
	m_pctFree = (u16)kvParse.getValueI(PCTFREE, PCTFREE_DEFAULT);
	m_incrSize = (u16)kvParse.getValueI(INCRSIZE, INCRSIZE_DEFAULT);
#ifdef TNT_ENGINE
	//m_tntTable = kvParse.getValueB(TNTTABLE, TNTTABLE_DEFAULT);
	m_tableStatus = (TableStatus)kvParse.getValueI(TABLESTATUS, TABLE_STATUS_DEFAULT);
#endif
	m_indexOnly = kvParse.getValueB(INDEXONLY, INDEXONLY_DEFAULT);

	m_fixLen = kvParse.getValueB(FIXLEN, FIXLEN_DEFAULT);

	// ColumnDefs
	kvParse.getValueO(string(COLUMN), (size_t)m_numCols, &m_columns, string(COLUMN_DEFAULT));
	
	u16 offset = m_bmBytes;
	u16 mysqlOffset = m_bmBytes;
	u16 nullBitmapOffset = (m_origRecFormat == REC_FIXLEN) ? 1 : 0;
	for (u16 i = 0; i < m_numCols; i++) {
		m_columns[i]->m_no = i;
		m_columns[i]->m_offset = offset;
		m_columns[i]->m_mysqlOffset = mysqlOffset;
		offset += m_columns[i]->m_size;
		mysqlOffset += m_columns[i]->m_mysqlSize;
		if (m_columns[i]->m_nullable) {
			m_columns[i]->m_nullBitmapOffset = nullBitmapOffset++;
		}
	}

	// IndexDefs
	m_pkey = NULL;
	if (m_numIndice > 0) {
		kvParse.getValueO(string(INDEX), (size_t)m_numIndice, &m_indice, string(INDEX_DEFAULT));
		for (int i = 0; i< m_numIndice; i++) {
			if (m_indice[i]->m_primaryKey)
				m_pkey = m_indice[i];
			for (u16 j = 0; j < m_indice[i]->m_numCols; j++)
				m_columns[m_indice[i]->m_columns[j]]->m_inIndex = true;
		}
	}

	calcPke();
	calcHasLob();
	calcHasLongVar();

	m_isCompressedTbl = kvParse.getValueB(COMPRESS_TABLE, COMPRESS_TABLE_DEFAULT);
	m_numColGrps = (u8)kvParse.getValueI(COL_GROUPS_NUM, COL_GROUPS_NUM_DEFAULT);

	if (m_numColGrps > 0) {
		// Column groups
		kvParse.getValueO(string(COL_GROUPS), (size_t)m_numColGrps, &m_colGrps, string(COL_GROUPS_DEFAULT));
		for (int i = 0; i< m_numColGrps; i++) {
			for (u8 j = 0; j < m_colGrps[i]->m_numCols; j++)
				m_columns[m_colGrps[i]->m_colNos[j]]->m_colGrpNo = m_colGrps[i]->m_colGrpNo;
		}
	}

 	m_rowCompressCfg = NULL;	
 	if (kvParse.isKeyExist(string(ROW_COMPRESS_CFG))) { 
 		kvParse.getValueO(string(ROW_COMPRESS_CFG), &m_rowCompressCfg, string(ROW_COMPRESS_CFG_DEFAULT));
 	}

	check();
}

/** ���������ȼ��� */
void TableDef::calcPke() {
	m_pke = NULL;
	for (int i = 0; i< m_numIndice; i++) {
		if (m_indice[i]->isPkeCandidate(this)) {
			m_pke = m_indice[i];
			return;
		}
	}
}

/** 
 * ���л�����
 *
 * @param buf �洢�������л������ڴ�
 * @param size buf�ڴ��С
 * @return �������л���ռ�õĿռ��С
 * @throw NtseException Խ��д
 */
void TableDef::write(byte **buf, u32 *size) const throw(NtseException) {
	KVParser kvParser;

	assert(m_id != INVALID_TABLEID);
	kvParser.setValueI(string(ID), m_id);
	assert(m_schemaName != NULL);
	kvParser.setValue(string(SCHEMANAME), string(m_schemaName));
	assert(m_name != NULL);
	kvParser.setValue(string(NAME), string(m_name));
	kvParser.setValueI(string(NUMCOLS), m_numCols);
	kvParser.setValueI(string(NULLABLECOLS), m_nullableCols);
	kvParser.setValueI(string(NUMINDICE), m_numIndice);
	kvParser.setValueI(string(RECFORMAT), m_recFormat);
	kvParser.setValueI(string(ORIG_RECFORMAT), m_origRecFormat);
	kvParser.setValueI(string(MAXRECSIZE), m_maxRecSize);
	kvParser.setValueI(string(MAXMYSQLRECSIZE), m_maxMysqlRecSize);
	kvParser.setValueB(string(USEMMS), m_useMms);
	kvParser.setValueB(string(CACHEUPDATE), m_cacheUpdate);
	kvParser.setValueI(string(UPDATECACHETIME), m_updateCacheTime);
	kvParser.setValueB(string(COMPRESSLOBS), m_compressLobs);
	kvParser.setValueI(string(BMBYTES), m_bmBytes);
	kvParser.setValueI(string(PCTFREE), m_pctFree);
	kvParser.setValueI(string(INCRSIZE), m_incrSize);
#ifdef TNT_ENGINE
	//kvParser.setValueB(string(TNTTABLE), m_tntTable);
	kvParser.setValueI(string(TABLESTATUS), m_tableStatus);
#endif
	kvParser.setValueB(string(INDEXONLY), m_indexOnly);
	kvParser.setValueB(string(FIXLEN), m_fixLen);

	// ColumnDefs
	assert(m_numCols != 0);
	kvParser.setValueO(string(COLUMN), m_columns, (size_t)m_numCols);

	// IndexDefs
	if (m_numIndice > 0) {
		kvParser.setValueO(string(INDEX), m_indice, (size_t)m_numIndice);
	}

	kvParser.setValueB(COMPRESS_TABLE, m_isCompressedTbl);
	kvParser.setValueI(COL_GROUPS_NUM, m_numColGrps);
	if (m_numColGrps > 0) {
		kvParser.setValueO(string(COL_GROUPS), m_colGrps, (size_t)m_numColGrps);
	}
	if (m_rowCompressCfg) {
		kvParser.setValueO(string(ROW_COMPRESS_CFG), m_rowCompressCfg);
	}

	kvParser.serialize(buf, size);
}

/** ��ָ���ļ��ж�ȡtabledef
 * @param path �ļ�·��
 * @throw NtseException ��ȡio�����쳣
 */
TableDef* TableDef::open(const char *path) throw (NtseException) {
	u64 errCode;
	AutoPtr<File> file(new File(path));
	if ((errCode = file->open(false)) != File::E_NO_ERROR) {
		NTSE_THROW(errCode, "Can not open file %s", path);
	}
	u64 fileSize;
	file->getSize(&fileSize);
	byte *fileBuf = new byte[(size_t)fileSize + 1];
	AutoPtr<byte> autoFileBuf(fileBuf, true);
	memset(fileBuf, '\0', (u32)fileSize);
	NTSE_ASSERT(file->read(0, (u32)fileSize, fileBuf) == File::E_NO_ERROR);
	file->close();

	TableDef *tblDef = new TableDef();
	tblDef->read(fileBuf, (u32)fileSize);

	return tblDef;
}

/** ��tabledefд��ָ���ļ�
 * @param path �ļ�·��
 * @throw NtseException IO�쳣
 */
void TableDef::writeFile(const char *path) throw (NtseException) {
	byte* fileBuf = NULL;
	u32 fileSize = 0;

	write(&fileBuf, &fileSize);
	AutoPtr<byte> autoFileBuf(fileBuf, true);

	// д����ʱ�ļ�
	string newPath = string(path) + ".tmp";		// �µ�Ԫ�����ļ�
	string bakPath = string(path) + ".tmpbak";		// ԭԪ�����ļ��ı���
	u64 code = File(newPath.c_str()).remove();
	if (!(code == File::E_NO_ERROR || File::getNtseError(code) == File::E_NOT_EXIST)) {
		NTSE_THROW(code, "Unable to remove old tmp file: %s", newPath.c_str());
	}
	code = File(bakPath.c_str()).remove();
	if (!(code == File::E_NO_ERROR || File::getNtseError(code) == File::E_NOT_EXIST)) {
		NTSE_THROW(code, "Unable to remove old tmp bak file: %s", bakPath.c_str());
	}

	File newFile(newPath.c_str());
	code = newFile.create(false, false);
	if (code != File::E_NO_ERROR) {
		NTSE_THROW(code, "Unable to create file: %s", newFile.getPath());
	}

	code = newFile.setSize(fileSize);
	if (code != File::E_NO_ERROR) {
		NTSE_THROW(code, "Unable to set size of file %s to %d", newFile.getPath(), fileSize);
	}

	code = newFile.write(0, fileSize, fileBuf);
	if (code != File::E_NO_ERROR) {
		NTSE_THROW(code, "Unable to write %d bytes to file", fileSize, newFile.getPath());
	}
	
	code = newFile.close();
	if (code != File::E_NO_ERROR) {
		NTSE_THROW(code, "Unable to close file: %s", newFile.getPath());
	}
	
	// �滻�����Է���ɾ��ԭ�ļ����������ļ��滻������ֱ�������ļ��滻ָ������ԭ�ļ�ʱ��
	// ��ĳЩ�����ϻ�Ī�������ʧ�ܡ���ʵ�齫ԭ�ļ���������Ȼ�����������ļ���ԭ�ļ���
	// ��ɾ��������֮���ԭ�ļ���û������
	bool isExist;

	isExist = File::isExist(path);
	if (isExist) {
		File srcFile(path);
		code = srcFile.move(bakPath.c_str());
		if (code != File::E_NO_ERROR) {
			NTSE_THROW(code, "Unable to move %s to %s", srcFile.getPath(), bakPath.c_str());
		}
	}

	code = File(newPath.c_str()).move(path);
	if (code != File::E_NO_ERROR) {
		NTSE_THROW(code, "Unable to move %s to %s", newPath.c_str(), path);
	}

	if (isExist) {
		code = File(bakPath.c_str()).remove();
		if (code != File::E_NO_ERROR) {
			NTSE_THROW(code, "Unable to remove file %s", bakPath.c_str());
		}
	}
}

/**
 * �����Ƿ����������ֶ�
 *
 * @return �����Ƿ����������ֶ�
 */
bool TableDef::hasLob() const {
	return m_hasLob;
}


/**
 * �����Ƿ���������䳤�ֶ�
 *
 * @return �����Ƿ���������䳤�ֶ�
 */
bool TableDef::hasLongVar() const {
	return m_hasLongVar;
}
/**
 * ָ�����ֶ��б����Ƿ����������ֶ�
 *
 * @param numCols ���Ը���
 * @param columns �����Ժ�
 * @return �Ƿ����������ֶ�
 */
bool TableDef::hasLob(u16 numCols, u16 *columns) const {
	assert(numCols <= m_numCols);
	for (u16 i = 0; i < numCols; i++) {
		u16 cno = columns[i];
		assert(cno < m_numCols);
		if (m_columns[cno]->isLob())
			return true;
	}
	return false;
}

/**
 * ��ñ��д�����ֶθ���
 *
 * @return ���д�����ֶθ���
 */
u16 TableDef::getNumLobColumns() const {
	u16 n = 0;
	for (u16 i = 0; i < m_numCols; i++) {
		if (m_columns[i]->isLob())
			n++;
	}
	return n;
}

/**
 * ����ָ�����Ƶ����ԵĶ��壬��Сд����
 *
 * @param columnName ������
 * @return ���Զ��壬��ָ�����Ƶ����Բ������򷵻�NULL
 */
ColumnDef* TableDef::getColumnDef(const char *columnName) const {
	ColumnDef *def = NULL;
	for (uint i = 0; i < m_numCols; ++i) {
		if (!strcmp(m_columns[i]->m_name, columnName)) {
			def = m_columns[i];
			break;
		}
	}
	return def;
}

/**
 * ����ָ�����Ժŵ����Զ���
 *
 * @param columnNo ���Ժ�
 * @return ���Զ��壬��ָ�����Ƶ����Բ������򷵻�NULL
 */
ColumnDef* TableDef::getColumnDef(const int columnNo) const {
	if ((columnNo>=0)&&(columnNo<m_numCols))
		return m_columns[columnNo];
	return NULL;
}

/**
 * ����ָ�����Ƶ����Ե����Ժţ���Сд����
 * @pre ָ�������Ա����Ǳ��е�����
 *
 * @param columnName ������
 * @return ���Ժţ�������ָ�����Ƶ�����ʱ����-1
 */
int TableDef::getColumnNo(const char *columnName) const {
	int cno = -1;
	for (u16 i = 0; i < m_numCols; ++i) {
		if (!strcmp(m_columns[i]->m_name, columnName)) {
			cno = i;
			break;
		}
	}
	return cno;
}

/**
 * ����һ������
 *
 * @param indexDef �������壬�������´��һ�ݣ������ɵ������ͷ�
 */
void TableDef::addIndex(const IndexDef *indexDef) {
	assert(!indexDef->m_primaryKey || !m_pkey);

	IndexDef **old = m_indice;
	m_indice = new IndexDef *[m_numIndice + 1];
	memcpy(m_indice, old, sizeof(IndexDef *) * m_numIndice);
	m_indice[m_numIndice] = new IndexDef(indexDef);
	delete []old;
	m_numIndice++;
	for (u16 i = 0; i < indexDef->m_numCols; i++) {
		u16 cno = indexDef->m_columns[i];
		m_columns[cno]->m_inIndex = true;
	}
	if (indexDef->m_primaryKey)
		m_pkey = m_indice[m_numIndice - 1];
	if (!m_pke && indexDef->isPkeCandidate(this))
		m_pke = m_indice[m_numIndice - 1];
}

/**
 * ɾ��ĳ������
 *
 * @param idx ������ţ���0��ʼ
 */
void TableDef::removeIndex(uint idx) {
	assert(idx < m_numIndice);

	if (m_indice[idx] == m_pkey)
		m_pkey = NULL;
	if (m_indice[idx] == m_pke)
		calcPke();

	IndexDef **old = m_indice;
	m_indice = new IndexDef *[m_numIndice - 1];
	memcpy(m_indice, old, sizeof(IndexDef *) * idx);
	memcpy(m_indice + idx, old + idx + 1, sizeof(IndexDef *) * (m_numIndice - idx - 1));
	delete old[idx];
	delete []old;
	m_numIndice--;

	for (u16 cno = 0; cno < m_numCols; cno++)
		m_columns[cno]->m_inIndex = false;
	for (u16 i = 0; i < m_numIndice; i++) {
		IndexDef *indexDef = m_indice[i];
		for (u16 ii = 0; ii < indexDef->m_numCols; ii++) {
			u16 cno = indexDef->m_columns[ii];
			m_columns[cno]->m_inIndex = true;
		}
	}
}

/** �õ����������������ĸ���
 * @param unique ��Ϊtrue��ֻͳ��Ψһ������(��������)������ֻͳ�Ʒ�Ψһ������
 * @return ���������������ĸ���
 */
u16 TableDef::getNumIndice(bool unique) const {
	u16 n = 0;
	for (u16 i = 0; i < m_numIndice; i++) {
		if (unique == m_indice[i]->m_unique)
			n++;
	}
	return n;
}

/**
 * �������Ƿ�Ϸ�
 *
 * @throw ���岻�Ϸ�
 */
void TableDef::check() const throw(NtseException) {
	if (strlen(m_name) > Limits::MAX_NAME_LEN)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Length of column name can not exceed %d: %s.", Limits::MAX_NAME_LEN, m_name);
	if (m_cacheUpdate && !m_useMms)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not cache update when MMS is not used");
	if (m_cacheUpdate && !m_pkey)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not cache update when table doesn't have a primary key");
	for (u16 i = 0; i < m_numCols; i++) {
		ColumnDef *col = m_columns[i];
		col->check();
		if (col->m_cacheUpdate && !m_cacheUpdate)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not cache update of column when table is set to not to cache update: %s", col->m_name);
		if (col->m_cacheUpdate && col->isLob())
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not cache update of LOBs: %s", col->m_name);
	}
	for (u16 i = 0; i < m_numIndice; i++)
		m_indice[i]->check(this);
	u16 minfree = (u16)MIN_PCT_FREE;
	if (m_pctFree > MAX_PCT_FREE || m_pctFree < minfree)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Invalid PCT_FREE: %d, should be in [%d, %d].", m_pctFree, MIN_PCT_FREE, MAX_PCT_FREE);
	if (m_incrSize > MAX_INCR_SIZE || m_incrSize < MIN_INCR_SIZE)
		NTSE_THROW(NTSE_EC_EXCEED_LIMIT, "Invalid INCR_SIZE: %d, should be in [%d, %d].", m_incrSize, MIN_INCR_SIZE, MAX_INCR_SIZE);
	if (m_recFormat == REC_FIXLEN && m_maxRecSize > Limits::MAX_REC_SIZE)
		NTSE_THROW(NTSE_EC_ROW_TOO_LONG, "Size of record can not be larger than %d.", Limits::MAX_REC_SIZE); 
	if (m_indexOnly) {
		if (m_numIndice == 0 || m_numIndice > 1)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index only table must have exactly one index");
		if (!m_pkey)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index only table must have primary key");
		if (m_indice[0]->m_numCols != m_numCols)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index of index only table must have all columns");
		if (m_useMms)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Index only table can not use mms");
	}
	if (m_numColGrps > 0)
		checkColumnGroups(m_colGrps, m_numColGrps);
}

/**
 * ָ�����ֶ��б����Ƿ�ֻ�����ɻ�����µ��ֶ�
 *
 * @param numCols ���Ը���
 * @param columns �����Ժ�
 * @return �Ƿ�ֻ�����ɻ�����µ��ֶ�
 */
bool TableDef::isUpdateCached(u16 numCols, u16 *columns) const {
	if (!m_useMms || !m_cacheUpdate)
		return false;
	for (u16 i = 0; i < numCols; i++) {
		u16 cno = columns[i];
		assert(cno < m_numCols);
		if (!m_columns[cno]->m_cacheUpdate)
			return false;
	}
	return true;
}

/** ����ָ�����Ƶ���������Сд����
 * @param indexName Ҫ���ҵ�������
 * @return ��Ӧ���������壬�Ҳ�������NULL
 */
IndexDef* TableDef::getIndexDef(const char *indexName) const {
	for (u16 i = 0; i < m_numIndice; i++)
		if (!strcmp(m_indice[i]->m_name, indexName))
			return m_indice[i];
	return NULL;
}

/** ����ָ��������ŵ���������
 * @param indexNo Ҫ���ҵ��������
 * @return ��Ӧ���������壬�Ҳ�������NULL
 */
IndexDef* TableDef::getIndexDef(const int indexNo) const {
	if((indexNo >= 0)&&(indexNo < m_numIndice))
		return m_indice[indexNo];
	return NULL;
}

/** ����ָ�����Ƶ���������Сд����
 * @param indexName Ҫ���ҵ�������
 * @return ��Ӧ��������ţ��Ҳ�������-1
 */
int TableDef::getIndexNo(const char *indexName) const {
	for (u16 i = 0; i < m_numIndice; i++)
		if (!strcmp(m_indice[i]->m_name, indexName))
			return i;
	return -1;
}

/** ���ñ��Ƿ�����MMS���Ƽ���������������ñ��Ƿ�����MMS����Ϊ���������Զ���������������Ա�֤����
 *  ֮����ͨ��TableDef::check
 * @param useMms �Ƿ�����MMS
 */
void TableDef::setUseMms(bool useMms) {
	m_useMms = useMms;
	if (!useMms)
		setCacheUpdate(false);
}

/** ���ñ��Ƿ����û�����¡��Ƽ���������������ñ��Ƿ����û�����£���Ϊ���������Զ������������
 *  ���Ա�֤����֮����ͨ��TableDef::check
 * @pre ��cacheUpdateΪtrue���һ��Ϊ����MMS
 * @param cacheUpdate �Ƿ����û������
 */
void TableDef::setCacheUpdate(bool cacheUpdate) {
	assert(!cacheUpdate || m_useMms);
	m_cacheUpdate = cacheUpdate;
	if (!cacheUpdate) {
		for (u16 i = 0; i < m_numCols; i++)
			m_columns[i]->m_cacheUpdate = false;
	}
}

/**
* ����ָ����������
* @param colGrpDef      �����鶨������
* @param copy           �Ƿ񿽱�ʹ�������鶨������
* @throws NtseException �����鶨�岻�Ϸ�
*/
void TableDef::setColGrps(Array<ColGroupDef *> *colGrpDefArr, bool copy) throw(NtseException) {
	assert(colGrpDefArr);
	u8 numColGrp = (u8)colGrpDefArr->getSize();
	ColGroupDef **colGrpDefs = new ColGroupDef*[numColGrp];
	memset(colGrpDefs, 0, numColGrp * sizeof(ColGroupDef *));
	for (uint i = 0; i < numColGrp; i++) {
		colGrpDefs[i] = (*colGrpDefArr)[i];
	}
	try {
		setColGrps(colGrpDefs, numColGrp, copy);
	} catch (NtseException &e) {
		delete []colGrpDefs;
		throw e;
	}
	if (copy)
		delete []colGrpDefs;
}

/**
 * ����ָ����������
 * @param colGrpDef      �����鶨������
 * @param numColGrps     ��������Ŀ
 * @param copy           �Ƿ񿽱�ʹ�������鶨������
 * @throws NtseException �����鶨�岻�Ϸ�
 */
void TableDef::setColGrps(ColGroupDef **colGrpDef, u8 numColGrps, bool copy /* = false*/) throw(NtseException) {
	assert(colGrpDef);
	assert(!m_colGrps);
	checkColumnGroups(colGrpDef, numColGrps);
	m_numColGrps = numColGrps;
	if (copy) {		
		m_colGrps = new ColGroupDef*[numColGrps];
		for (u16 i = 0; i < numColGrps; i++)
			m_colGrps[i] = new ColGroupDef(colGrpDef[i]);
	} else {
		m_colGrps = colGrpDef;
	}
	for (u16 i = 0; i < numColGrps; i++) {
		assert(colGrpDef[i]);
		for (uint j = 0; j < colGrpDef[i]->m_numCols; j++) {
			u16 colNo = colGrpDef[i]->m_colNos[j];
			m_columns[colNo]->m_colGrpNo = (u8)i;
			m_columns[colNo]->m_colGrpOffset = (u16)j;
		}
	}
	assert(m_numColGrps > 0 && m_colGrps);
}

/**
* ����Ĭ��������
*/
void TableDef::setDefaultColGrps() {
	assert(m_numColGrps == 0);
	ColGroupDefBuilder colGrpDefBuilder(0);
	for (u16 i = 0; i < m_numCols; i++) {
		colGrpDefBuilder.appendCol(i);
	}
	ColGroupDef ** colGrpDef = new ColGroupDef*[1];
	colGrpDef[0] = colGrpDefBuilder.getColGrpDef();
	try {
		setColGrps(colGrpDef, 1, false);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		assert(false);//�������쳣
	}
}

/**
 * ��������鶨���Ƿ�Ϸ�
 * @param colGrpDef      �����鶨������
 * @param numColGrps     ��������Ŀ
 * @throws NtseException �����鶨�岻�Ϸ�
 */
void TableDef::checkColumnGroups(ColGroupDef **colGrpDef, u8 numColGrps) const throw(NtseException) {
	assert(numColGrps > 0 && colGrpDef);
	bool *isColset = new bool[m_numCols];
	memset(isColset, 0, m_numCols * sizeof(bool));
	for (u16 i = 0; i < numColGrps; i++) {
		assert(colGrpDef[i]);
		for (uint j = 0; j < colGrpDef[i]->m_numCols; j++) {
			u16 colNo = colGrpDef[i]->m_colNos[j];
			if (isColset[colNo]) {//�Ƿ��ظ�����
				delete []isColset;
				NTSE_THROW(NTSE_EC_INVALID_COL_GRP, "Column \"%s\" is redefined in some column group.", m_columns[colNo]->m_name);
			}
			isColset[colNo] = true;
		}
	}
	//����Ƿ�ÿ�����Զ��Ѿ�����ȷ����
	for (uint i = 0; i < m_numCols; i++) {
		if (!isColset[i]) {
			delete []isColset;
			NTSE_THROW(NTSE_EC_INVALID_COL_GRP, "Column \"%s\" is not defined in any column group.", m_columns[i]->m_name);
		}
	}
	delete [] isColset;
}

/**
 * ���㵱ǰ�����Ƿ���������
 */
void TableDef::calcHasLob() {
	m_hasLob = false;
	for (u16 i = 0; i < m_numCols; i++) {
		if (m_columns[i]->isLob()) {
			m_hasLob = true;
			return;
		}
	}
}


/**
 * ���㵱ǰ�����Ƿ���������ֶ�
 */
void TableDef::calcHasLongVar() {
	m_hasLongVar = false;
	for (u16 i = 0; i < m_numCols; i++) {
		if (m_columns[i]->isLongVar()) {
			m_hasLongVar = true;
			return;
		}
	}
}


#ifdef TNT_ENGINE
/** �жϸñ��Ƿ�Ϊtnt��
 * return ����Ƿ���true�����򷵻�false
 */
bool TableDef::isTNTTable() const {
	return TS_TRX == m_tableStatus;
}

/** ���ñ����Ƿ�Ϊtnt��
 * @param ntseTable trueΪtnt������Ϊntse��
 */
/*void TableDef::setTNTTable(bool tntTable) {
	m_tntTable = tntTable;
}*/

/** ��ȡ��״̬��Ϣ
 * return ��״̬��Ϣ
 */
TableStatus TableDef::getTableStatus() const {
	return m_tableStatus;
}

/** ���ñ�״̬��Ϣ
 * @param tableStatus ��״̬��Ϣ
 */
void TableDef::setTableStatus(TableStatus tableStatus) {
	m_tableStatus = tableStatus;
}

/**��ȡ��״̬��Ϣ����
 *
 */
char *TableDef::getTableStatusDesc() const {
	return getTableStatusDesc(m_tableStatus);
}

/**��ȡ��״̬��Ϣ����
 *
 */
char *TableDef::getTableStatusDesc(TableStatus tableStatus) {
	char *desc = NULL;
	if (tableStatus == TS_CHANGING) {
		desc = "TS_CHANGING";
	} else if (tableStatus == TS_NON_TRX) {
		desc = "TS_NON_TRX";
	} else if (tableStatus == TS_TRX) {
		desc = "TS_TRX";
	} else if (tableStatus == TS_SYSTEM) {
		desc = "TS_SYSTEM";
	} else if (tableStatus == TS_ONLINE_DDL) {
		desc = "TS_ONLINE_DDL";
	}
	return desc;
}
#endif

/** �ж�ָ����pctFree�Ƿ���Ч�����ڶ����ı�ʼ������Ч��
 * @param pctFree	ָ����ֵ
 * @return true��ʾ��Ч��false��ʾ��Ч
 */
bool TableDef::isHeapPctFreeValid( u16 pctFree ) {
	u16 minfree = (u16)TableDef::MIN_PCT_FREE;
	return m_recFormat == REC_VARLEN && pctFree >= minfree && pctFree <= TableDef::MAX_PCT_FREE;
}

/**
 * �ж�incrSize�ĺϷ���
 * @param incrSize
 * @return �Ϸ��򷵻�true�����򷵻�false
 */
bool TableDef::isIncrSizeValid(int incrSize) {
	return incrSize >= TableDef::MIN_INCR_SIZE && incrSize <= TableDef::MAX_INCR_SIZE;
}

void TableDef::drop(const char* path) throw(NtseException) {
	u64 errCode;
	File file(path);
	errCode = file.remove();
	if (File::E_NOT_EXIST == File::getNtseError(errCode))
		return;
	if (File::E_NO_ERROR != File::getNtseError(errCode)) {
		NTSE_THROW(errCode, "Cannot drop table def file for path %s", path);
	}
}

///////////////////////////////////////////////////////////////////////////////
// TableDefBuilder ////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/**
 * ����һ�����幹����
 *
 * @param tableId ��ID
 * @param schemaName ������SCHEMA
 * @param tableName ����
 * @param colGrpNums ��������
 */
TableDefBuilder::TableDefBuilder(u16 tableId, const char *schemaName, const char *tableName) {
	m_tableId = tableId;
	m_schemaName = System::strdup(schemaName);
	m_tableName = System::strdup(tableName);
	m_isCompressTbl = false;
	m_hasLob = false;
	m_hasLongVar = false;
}

TableDefBuilder::~TableDefBuilder() {
	delete []m_schemaName;
	delete []m_tableName;
	for (size_t i = 0; i < m_columns.getSize(); i++)
		delete m_columns[i];
	for (size_t i = 0; i < m_indice.getSize(); i++)
		delete m_indice[i];
	for (size_t i = 0; i < m_colGrps.getSize(); i++)
		delete m_colGrps[i];
}

/**
 * ����һ������Ҫָ�����ȵ���
 *
 * @param columnName ����
 * @param columnType �����ͣ�ӦΪINT�Ȳ���Ҫָ����󳤶ȵ�����
 * @param nullable �Ƿ����ΪNULL
 * @param collation collation
 * @return ���幹����
 */
TableDefBuilder* TableDefBuilder::addColumn(const char *columnName, ColumnType columnType, 
											bool nullable, CollType collation) {
	assert(columnType != CT_CHAR && columnType != CT_VARCHAR
			&& columnType != CT_BINARY && columnType != CT_VARCHAR);
	ColumnDef *def = new ColumnDef(columnName, columnType);
	def->m_nullable = nullable;
	def->m_collation = collation;
	def->m_no = (u16)m_columns.getSize();
	m_columns.push(def);
	m_hasLob = (m_hasLob | def->isLob());
	m_hasLongVar = (m_hasLongVar | def->isLongVar());

	return this;
}

/**
 * ����һ����Ҫָ�����ȵ���
 *
 * @param columnName ����
 * @param columnType �����ͣ�ӦΪCHAR/VARCHAR����Ҫָ����󳤶ȵ�����
 * @param maxSize ����
 * @param nullable �Ƿ����ΪNULL
 * @param collation collation
 * @return ���幹����
 */
TableDefBuilder* TableDefBuilder::addColumnS(const char *columnName, ColumnType columnType, u16 maxSize, bool longVarConvertable,
											 bool nullable, CollType collation) {
	assert(columnType == CT_CHAR || columnType == CT_VARCHAR
			|| columnType == CT_BINARY || columnType == CT_VARBINARY);

	// ����ǳ����䳤�ֶΣ��ڲ�ת����LOB�洢
	bool isLongVar = false;
	if (longVarConvertable && maxSize > Limits::DEF_MAX_VAR_SIZE) {
		columnType = CT_SMALLLOB;
		isLongVar = true;
	}
	ColumnDef *def = new ColumnDef(columnName, columnType, isLongVar, maxSize);
	def->m_nullable = nullable;
	def->m_collation = collation;
	def->m_no = (u16)m_columns.getSize();
	// �ж��������ʶ�����䳤�ֶ�
	if (def->m_type == CT_SMALLLOB)
		def->m_longVar = true;
	m_columns.push(def);
	m_hasLob = (m_hasLob | def->isLob());
	m_hasLongVar = (m_hasLongVar | def->isLongVar());

	return this;
}


/**
 * ����һ����ֵ��
 *
 * @param columnName ����
 * @param columnType �����ͣ�ӦΪ�������ͣ���DECIMAL�������Ϊ����
 * @param prtype ��ȷ����
 * @param nullable �Ƿ����ΪNULL
 * @return ���幹����
 */
TableDefBuilder* TableDefBuilder::addColumnN(const char *columnName, ColumnType columnType, PrType prtype, bool nullable) {
	assert(columnType <= CT_DECIMAL);
	ColumnDef *def;
	if (columnType == CT_DECIMAL) {
		def = new ColumnDef(columnName, columnType, false
				, (u16)Decimal::getBinSize(prtype.m_precision, prtype.m_deicmal));
	} else {
		def = new ColumnDef(columnName, columnType);
	}
	def->m_nullable = nullable;
	def->m_collation = COLL_BIN;
	def->m_prtype = prtype;
	def->m_no = (u16)m_columns.getSize();
	m_columns.push(def);
	return this;
}

/**
 * ����һ������
 *
 * @param indexName ������
 * @param primaryKey �Ƿ�Ϊ����
 * @param unique �Ƿ�Ψһ
 * @param indexColumns ����������
 * @param prefixArr ��������ǰ׺��Ϣ
 * @return ���幹����
 */
TableDefBuilder* TableDefBuilder::addIndex(const char *indexName, bool primaryKey, bool unique, bool online, Array<u16> &indexColumns, Array<u32> &prefixLenArr) {
	ColumnDef **columns = new ColumnDef *[indexColumns.getSize()];
	u32 *prefixLens = new u32[prefixLenArr.getSize()];
	for (size_t i = 0; i < indexColumns.getSize(); i++) {
		columns[i] = m_columns[indexColumns[i]];
		prefixLens[i] = prefixLenArr[i];
	}
	IndexDef *def = new IndexDef(indexName, (u16)indexColumns.getSize(), columns, prefixLens, unique, primaryKey, online);
	m_indice.push(def);

	delete[] columns;
	delete[] prefixLens;
	return this;
}

/**
 * ����һ�����������ڵ�Ԫ���ԣ�
 *
 * @param indexName ������
 * @param primaryKey �Ƿ�Ϊ����
 * @param unique �Ƿ�Ψһ
 * @param ... �����������Լ�ǰ׺��Ϣ��������Գ��֣�һ����������һ��ǰ׺���ȣ�
 * @return ���幹����
 */
TableDefBuilder* TableDefBuilder::addIndex(const char *indexName, bool primaryKey, bool unique, bool online, ...) {
	va_list vl;
	va_start(vl, online);
	char *columnName;
	u32 prefixLen = 0;
	Array<u16> indexCols;
	Array<u32> prefixLenArr;
	for (columnName = va_arg(vl, char *), prefixLen = va_arg(vl, u32); columnName ; columnName = va_arg(vl, char *), prefixLen = va_arg(vl, u32)) {
		size_t i;
		for (i = 0; i < m_columns.getSize(); i++) {
			if (!System::stricmp(m_columns[i]->m_name, columnName))
				break;
		}
		assert(i < m_columns.getSize());
		indexCols.push((u16)i);
		prefixLenArr.push(prefixLen);
	}
	va_end(vl);
	return addIndex(indexName, primaryKey, unique, online, indexCols, prefixLenArr);
}

/**
 * ����һ��������
 * @param colGrpNo ��������,����
 * @param grpColumns ������ĸ�������
 * @TODO: �����������еĸ����������������������в�����,�������е����Զ���������ĳ��������
 */

TableDefBuilder* TableDefBuilder::addColGrp(const u8& colGrpNo, const Array<u16> &grpColumns) {
	assert(m_colGrps.getSize() == colGrpNo);
	assert(grpColumns.getSize() > 0);
	//У����ӵ��������и����ж��Ǵ��ڵ�
	for (u16 i = 0; i < grpColumns.getSize(); i++) {
		NTSE_ASSERT(grpColumns.getSize() <= m_columns.getSize());
		NTSE_ASSERT(m_columns[grpColumns[i]] != NULL);
		NTSE_ASSERT(m_columns[grpColumns[i]]->m_colGrpNo == 0);
	}
	ColGroupDefBuilder builder(colGrpNo);	
	for (u16 i = 0; i < grpColumns.getSize(); i++) {
		builder.appendCol(grpColumns[i]);
	}
	ColGroupDef * def = builder.getColGrpDef();
	m_colGrps.push(def);
	return this;
}

/**
 * �õ�����
 *
 * @return ����
 */
TableDef* TableDefBuilder::getTableDef() {
	TableDef *tableDef = new TableDef();
	tableDef->m_id = m_tableId;
	tableDef->m_schemaName = System::strdup(m_schemaName);
	tableDef->m_name = System::strdup(m_tableName);
	tableDef->m_numCols = (u16)m_columns.getSize();
	tableDef->m_nullableCols = 0;
	tableDef->m_maxRecSize = 0;
	tableDef->m_maxMysqlRecSize = 0;
	tableDef->m_origRecFormat = REC_FIXLEN;
	tableDef->m_numIndice = (u8)m_indice.getSize();
	tableDef->m_hasLob = m_hasLob;
	tableDef->m_hasLongVar = m_hasLongVar;

	tableDef->m_isCompressedTbl = m_isCompressTbl;
	tableDef->m_rowCompressCfg = tableDef->m_isCompressedTbl ? new RowCompressCfg() : NULL;

	tableDef->m_columns = new ColumnDef *[tableDef->m_numCols];
	for (u16 i = 0; i < tableDef->m_numCols; i++) {
		tableDef->m_columns[i] = new ColumnDef(m_columns[i]);
		if (m_columns[i]->m_nullable) {
			tableDef->m_columns[i]->m_nullBitmapOffset = tableDef->m_nullableCols++;
		} else {
			tableDef->m_columns[i]->m_nullBitmapOffset = 0;
		}
		tableDef->m_columns[i]->m_offset = tableDef->m_maxRecSize;
		tableDef->m_columns[i]->m_mysqlOffset = tableDef->m_maxMysqlRecSize;

		tableDef->m_maxRecSize = (u16)(tableDef->m_maxRecSize + m_columns[i]->m_size);
		tableDef->m_maxMysqlRecSize = (u16)(tableDef->m_maxMysqlRecSize + m_columns[i]->m_mysqlSize);
		if (!tableDef->m_columns[i]->isFixlen())
			tableDef->m_origRecFormat = REC_VARLEN;
	}

	// ���������ڼ�¼�е�ƫ�ƺͿ�ֵλͼλ���������ֵλͼ����һ��starting bit
	u8 nullBitmapSize;
	if (tableDef->m_origRecFormat != REC_FIXLEN)
		nullBitmapSize = (u8)((tableDef->m_nullableCols + 7) / 8);                          
	else
		nullBitmapSize = (u8)((tableDef->m_nullableCols + 8) / 8);
	tableDef->m_bmBytes = nullBitmapSize;
	for (u16 i = 0; i < tableDef->m_numCols; i++) {
		tableDef->m_columns[i]->m_offset = tableDef->m_columns[i]->m_offset + nullBitmapSize;
		tableDef->m_columns[i]->m_mysqlOffset = tableDef->m_columns[i]->m_mysqlOffset + nullBitmapSize;
	}
	if (tableDef->m_origRecFormat == REC_FIXLEN) {
		for (u16 i = 0; i < tableDef->m_numCols; i++) {
			if (tableDef->m_columns[i]->m_nullable)
				tableDef->m_columns[i]->m_nullBitmapOffset++;
		}
	}
	tableDef->m_recFormat = tableDef->m_isCompressedTbl ?
		REC_COMPRESSED : tableDef->m_origRecFormat;
	tableDef->m_maxRecSize = tableDef->m_maxRecSize + nullBitmapSize;
	tableDef->m_maxMysqlRecSize = tableDef->m_maxMysqlRecSize + nullBitmapSize;

	tableDef->m_indice = (tableDef->m_numIndice) ? (new IndexDef *[tableDef->m_numIndice]): NULL;
	for (u16 i = 0; i < tableDef->m_numIndice; i++) {
		tableDef->m_indice[i] = new IndexDef(m_indice[i]);
		if (tableDef->m_indice[i]->m_primaryKey)
			tableDef->m_pkey = tableDef->m_indice[i];
		if (!tableDef->m_pke && tableDef->m_indice[i]->isPkeCandidate(tableDef))
			tableDef->m_pke = tableDef->m_indice[i];
	}

	tableDef->m_numColGrps = (u8)m_colGrps.getSize();
	tableDef->m_colGrps = NULL;
	if (tableDef->m_numColGrps > 0) {
		try {
			tableDef->setColGrps(&m_colGrps, true);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			NTSE_ASSERT(false);
		}
	} 	
	return tableDef;
}

}

