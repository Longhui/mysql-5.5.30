/**
 * ����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TABLEDEF_H_
#define _NTSE_TABLEDEF_H_

#include "misc/Global.h"
#include "misc/Record.h"
#include "util/Array.h"
#include "compress/RowCompressCfg.h"

namespace ntse {

/** �������� */
/* ���������޸ĵĻ�����¼ת���ĵط���Ҫ����ѡ�� */
enum ColumnType {
	CT_TINYINT,		/** һ�ֽ����� */
	CT_SMALLINT,	/** ���ֽ����� */
	CT_MEDIUMINT,	/** ���ֽ��������� */
	CT_INT,			/** ���ֽ����� */
	CT_BIGINT,		/** ���ֽ����� */
	CT_FLOAT,		/** �������� */
	CT_DOUBLE,		/** Double/Real���� */
	CT_DECIMAL,		/** Decimal/Numeric���� */
	CT_RID,			/** �����RID���� */
	CT_CHAR,		/** �����ַ��� */
	CT_VARCHAR,		/** �䳤�ַ��� */
	CT_BINARY,		/** binary���� */
	CT_VARBINARY,	/** varbinary���� */
	CT_SMALLLOB,	/** С�ʹ��������С64K */
	CT_MEDIUMLOB,	/** ���ʹ��������С16M */
};

/** �����ַ�����collation���� */
enum CollType {
	COLL_BIN,		/** ���������ƴ����Ƚϴ�С����memcmp */
	COLL_LATIN1,    /** ��ŷ�ַ�(ISO-8859-1) */
	COLL_GBK,		/** GBK��������� */
	COLL_UTF8,		/** UTF8��������� */
	COLL_UTF8MB4,	/** UTF8MB4���������*/
};

enum TableStatus {
	TS_NON_TRX = 0,  //�������
	TS_TRX,          //�����
	TS_SYSTEM,       //ϵͳ��
	TS_CHANGING,     //������ת����
	TS_ONLINE_DDL,   //��������online ddl����
};

/** collation�����ڰ���ͬ�Ĺ���Ƚ��ַ�����С */
class Collation {
public:
	/**
	 * �Ƚ������ַ���
	 *
	 * @param coll Collation
	 * @param str1 �ַ���1
	 * @param len1 �ַ���1��С
	 * @param str2 �ַ���2
	 * @param len2 �ַ���2��С
	 * @return str1 < str2ʱ����<0��str1 = str2ʱ����0�����򷵻�>0
	 */
	static int strcoll(CollType coll, const byte *str1, size_t len1, const byte *str2, size_t len2);
	static size_t charpos(CollType coll, const char *pos, const char *end, size_t length);
	static void getMinMaxLen(CollType coll, size_t *mbMinLen, size_t *mbMaxLen);
};

// ��ȷ����
struct PrType {
	PrType(): m_flags(0), m_precision(0), m_deicmal(0) {}
	
	inline void setUnsigned() {
		m_flags |= UNSIGNED;
	}

	inline bool isUnsigned() const{
		return (m_flags & UNSIGNED) != 0;
	}
	
	u16 m_flags;	// �Ƿ��޷��ŵȱ�־λ
	u8 m_precision;	// ���λ��������С��
	u8 m_deicmal;	// С����λ��

	static const u16 UNSIGNED = 0x1; // �޷��ű�ʶλ
};

/** �ֶζ��� */
class ColumnDef {
public:
	ColumnDef();
	ColumnDef(const char *name, ColumnType type, bool isLongVar = false, u16 maxSize = 0, u8 colGrpNum = 0, u16 colGrpOffset = 0);
	ColumnDef(const ColumnDef *copy);
	~ColumnDef();
	void check() throw(NtseException);
	bool isFixlen() const;
	bool isString() const;
	bool isDecimal() const;
	bool varSized() const;
	u16 calcSize();
	bool operator == (const ColumnDef &another);
	bool isLob() const;
	bool isLongVar() const;
	void readFromKV(byte* buf, u32 size) throw(NtseException);
	void writeToKV(byte **buf, u32 *size) const throw(NtseException);
	void calcCompressSize();

	static const u16 RID_COLUMN_NO = 65534;	/** �����RowId���Ե����Ժ� */

	u16		m_no;				/** ���Ա�ţ���0��ʼ */
	char	*m_name;			/** ������ */
	ColumnType	m_type;			/** ���� */
	CollType	m_collation;	/** �ַ�����collation */
	u16		m_offset;			/** �����������¼��ʽ���ֶδ洢λ��ƫ�ƣ�������ֵλͼ */
	u16		m_size;				/** ��󳤶ȣ�����VARCHAR�����Ѿ�������һ�������ֽڵĳ��� */
	u32		m_mysqlOffset;		/** MYSQL��ʽ���ֶδ洢λ��ƫ�ƣ�������ֵλͼ */
	u16		m_mysqlSize;		/** MYSQL��ʽ��󳤶ȣ�����VARCHAR�����Ѿ�������һ�������ֽڵĳ��� */
	bool	m_nullable;			/** �ֶ��Ƿ����Ϊ�� */
	u16		m_nullBitmapOffset; /** ��ֵλͼ��Ӧ��λ */
	byte	m_lenBytes;			/** ��ʾVARCHAR���ͳ�����Ҫ���ֽ��� */
	bool	m_cacheUpdate;		/** �ڱ�����MMS���»���ʱ��������ֶεĸ����Ƿ񻺴� */
	PrType	m_prtype;			/** ��ȷ���ͣ�����������Ϣ��������Ϣ�� */
	bool	m_compressable;		/** �Ƿ���Ա�ѹ�� */
	u16		m_maxCompressSize;	/** ���Ա�ѹ��������ѹ�������󳤶ȣ����ڲ���ѹ�������Լ�Ϊm_size */
	bool	m_inIndex;			/** �ֶ��Ƿ�Ϊĳ��������һ���� */
	bool	m_longVar;			/** �ֶ��Ƿ��ǳ���varchar/varbinary���� */
	/** ��¼ѹ����� */
	u8      m_colGrpNo;      /** �ֶ�������������� */
	u16     m_colGrpOffset;  /** �ֶ����������е�λ�� */

#ifndef NTSE_UNIT_TEST
private:
#endif
	static const char		*NAME;
	static const char		*NAME_DEFAULT;
	static const char		*TYPE;
	static const ColumnType TYPE_DEFAULT = CT_INT;
	static const char		*COLLATION;
	static const CollType	COLLATION_DEFAULT = COLL_BIN;
	static const char		*SIZE;
	static const u16		SIZE_DEFALUT = 4;
	static const char		*MYSQL_SIZE;
	static const u16		MYSQL_SIZE_DEFAULT = 4;
	static const char		*NULLABLE;
	static const bool		NULLABLE_DEFAULT = false;
	static const char		*LONGVAR;
	static const bool		LONGVAR_DEFAULT = false;
	static const char		*CACHEUPDATE;
	static const bool		CACHEUPDATE_DEFAULT = true;
	static const char		*PRFLAGS;
	static const u16		PRFLAGS_DEFAULT = 0;
	static const char		*PRPRECISON;
	static const u8			PRPRECISION_DEFAULT = 0;
	static const char		*PRDECIMAL;
	static const u8			PRDECIMAL_DEFAULT = 0;
	static const char       *COLGRPNO;
	static const u8         COL_GRP_NO_DEFAULT = (u8)-1;
	static const char       *COLGRPOFFSET;
	static const u16        COL_GRP_OFFSET_DEFAULT = (u16)-1;
};

class TableDef;
/** �������� */
class IndexDef {
public:
	IndexDef();
	IndexDef(const char *name, u16 numCols, ColumnDef **columns, u32* prefixLens, bool unique = false, bool primaryKey = false, bool online = false);
	IndexDef(const IndexDef *copy);
	~IndexDef();
	bool operator == (const IndexDef &another);
	void readFromKV(byte* buf, u32 size) throw(NtseException);
	void writeToKV(byte **buf, u32 *size) const throw(NtseException);
	void check(const TableDef *tableDef) const throw(NtseException);
	bool isPkeCandidate(const TableDef *tableDef) const;
	bool hasLob() const;
	static bool isSplitFactorValid(s8 splitFactor);

	static const s8 SMART_SPLIT_FACTOR = -1;		/** ���ܼ����������ʱ����ҳ�����ݰٷֱ� */
	static const s8 MIN_SPLIT_FACTOR = 5;			/** ����ϵ����Сֵ */
	static const s8 MAX_SPLIT_FACTOR = 95;			/** ����ϵ�����ֵ */

	char	*m_name;		/** ������ */
	bool	m_primaryKey;	/** �Ƿ�Ϊ���� */
	bool	m_unique;		/** �Ƿ�ΪΨһ������ */
	bool	m_online;		/** �Ƿ�Ϊ�������� */
	bool	m_prefix;		/** �Ƿ��Ǻ�ǰ׺������ */
	bool	m_hasLob;		/** �Ƿ����������� */
	u16		m_maxKeySize;	/** ��������󳤶ȣ�������ֵλͼ */
	u16		m_numCols;		/** �漰������ */
	u16		*m_columns;		/** �����������ڱ��ֶ��б��е�λ�ã���0��ʼ��� */
	u16		*m_offsets;		/** ������������KEY_PAD��ʽ�д洢��ʼλ�� */
	u32		*m_prefixLens;  /** ����������ǰ׺���� */
	u8		m_bmBytes;		/** ��������NULLλͼռ�õ��ֽ��� */
	s8		m_splitFactor;	/** ��������ʱ���ҳ�������ݰٷֱȣ�Ĭ��ΪSMART_SPLIT_FACTOR */

#ifndef NTSE_UNIT_TEST
private:
#endif
	static const char *NAME;
	static const char *NAME_DEFALUT;
	static const char *PRIMARYKEY;
	static const bool PRIMARYKEY_DEFAULT = false;
	static const char *UNIQUE;
	static const bool UNIQUE_DEFAULT = false;
	static const char *ONLINE;
	static const bool ONLINE_DEFAULT = false;
	static const char *PREFIX;
	static const bool PREFIX_DEFAULT = false;
	static const char *HASLOB;
	static const bool HASLOB_DEFAULT = false;
	static const char *MAXKEYSIZE;
	static const u16  MAXKEYSIZE_DEFAULT = 0;
	static const char *NUMCOLS;
	static const u16  NUMCOLS_DEFAULT = 0;
	static const char *BMBYTES;
	static const u8   BMBYTES_DEFAULT = 0;
	static const char *SPLITFACTOR;
	static const s8   SPLITFACTOR_DEFAULT = 0;
	static const char *COLUMNS;
	static const char *COLUMNS_DEFAULT;
	static const char *OFFSET;
	static const char *OFFSET_DEFAULT;
	static const char *PREFIXS;
	static const char *PREFIXS_DEFAULT;
};

/** 
 * �����鶨��
 *
 * �����鶨��Ϊһ�����Ե���ϣ����������������������ڱ��ֶ��б��е�λ�á�һ�����п����ж�������顣
 */
class ColGroupDef {
public:
	static const uint MAX_NUM_COL_GROUPS = 256; /* ������������ */
	static const char *COL_GRP_NO;
	static const char *COL_NUM_COLS;
	static const char *COL_NOS;

public:
	ColGroupDef();
	ColGroupDef(u8 colGrpNo, u16 numCols, ColumnDef **columns);
	ColGroupDef(const ColGroupDef *copy);
	~ColGroupDef();
	bool operator == (const ColGroupDef& another);

	void readFromKV(byte *buf, u32 size) throw(NtseException);
	void writeToKV(byte **buf, u32 *size) const throw(NtseException);
	
	size_t read(byte *buf, size_t size) throw(NtseException);
	size_t write(byte *buf, size_t size) const throw(NtseException);
	size_t getSerializeSize() const;
	static Array<ColGroupDef *>* buildNewColGrpDef(const TableDef *newtblDef, const TableDef *oldTblDef, Array<ColGroupDef *> *oldColGrpDef);

	u8      m_colGrpNo;     /** �������ڱ�������������е����, ��0��ʼ��� */
	u16		m_numCols;		/** ���������� */
	u16		*m_colNos;      /** �������ڱ��ֶ��б��еı�� */
};

/**
 * �����鶨�幹����
 */
class ColGroupDefBuilder {
public:
	ColGroupDefBuilder(u8 colGrpNo) : m_colGrpNo(colGrpNo) {
	}
	~ColGroupDefBuilder() {}
	inline void appendCol(u16 colNo) {
		m_colNos.push(colNo);
	}
	ColGroupDef* getColGrpDef() {
		assert(m_colNos.getSize() > 0);
		ColGroupDef *colGrpDef = new ColGroupDef();
		colGrpDef->m_colGrpNo = m_colGrpNo;
		colGrpDef->m_numCols = (u16)m_colNos.getSize();
		u16 *colNos = new u16[m_colNos.getSize()];
		for (u16 i = 0; i < colGrpDef->m_numCols; i++) {
			colNos[i] = m_colNos[i];
		}
		colGrpDef->m_colNos = colNos;
		return colGrpDef;
	}
private:
	u8         m_colGrpNo;  /* �������*/
	Array<u16> m_colNos;    /* �������и������Ե����Ժ� */
};

#pragma pack(4)
/** ���� */
class TableDef {
public:
	TableDef();
	TableDef(const TableDef *copy);
	~TableDef();
	bool operator == (const TableDef &another);
	static u16 getVirtualLobTableId(u16 normalTableId);
	static u16 getNormalTableId(u16 virtualLobTableId);
	static bool tableIdIsTemp(u16 tableId);
	static bool tableIdIsVirtualLob(u16 tableId);
	static bool tableIdIsNormal(u16 tableId);

#ifdef TNT_ENGINE
	bool isTNTTable() const;
	//void setTNTTable(bool tntTable);

	TableStatus getTableStatus() const;
	void setTableStatus(TableStatus tableStatus);

	char *getTableStatusDesc() const;
	static char *getTableStatusDesc(TableStatus tableStatus);
#endif

	bool isHeapPctFreeValid(u16 pctFree);
	static bool isIncrSizeValid(int incrSize);
	void check() const throw(NtseException);
	bool hasLob() const;
	bool hasLob(u16 numCols, u16 *columns) const;
	bool hasLongVar() const;
	u16 getNumLobColumns() const;
	ColumnDef* getColumnDef(const char *columnName) const;
	ColumnDef* getColumnDef(const int columnNo) const;
	int getColumnNo(const char *columnName) const;
	IndexDef* getIndexDef(const char *indexName) const;
	IndexDef* getIndexDef(const int indexNo) const;
	int getIndexNo(const char *indexName) const;
	void addIndex(const IndexDef *indexDef);
	void removeIndex(uint idx);
	u16 getNumIndice(bool unique) const;
	bool isUpdateCached(u16 numCols, u16 *columns) const;
	void setUseMms(bool useMms);
	void setCacheUpdate(bool cacheUpdate);
	
    /** �����鶨����� */
	void checkColumnGroups(ColGroupDef **colGrpDef, u8 numColGrps) const throw(NtseException);
	void setColGrps(Array<ColGroupDef *> *colGrpDef, bool copy = false) throw(NtseException);
	void setColGrps(ColGroupDef **colGrpDef, u8 numColGrps, bool copy = false) throw(NtseException);
	void setDefaultColGrps();
	static TableDef* open(const char *path) throw (NtseException);
	void writeFile(const char *path) throw (NtseException);

	void read(byte *buf, u32 size) throw(NtseException);
	void write(byte **buf, u32 *size) const throw(NtseException);

	static void drop(const char* path) throw(NtseException);
private:
	void calcPke();
	void calcHasLob();
	void calcHasLongVar();
public:
	static const bool DEFAULT_USE_MMS = true;		/** Ĭ���Ƿ�ʹ��MMS */
	static const bool DEFAULT_CACHE_UPDATE = false;	/** Ĭ���Ƿ񻺴���� */
	static const bool DEFAULT_COMPRESS_LOBS = true;	/** Ĭ���Ƿ�ѹ������� */
	static const bool DEFAULT_COMPRESS_ROW = false; /** Ĭ���Ƿ����ü�¼ѹ��*/
	static const bool DEFAULT_FIX_LEN = true;      /** Ĭ���Ƿ�ʹ�ö����� */
	static const u8 DEFAULT_COLUMN_GROUPS = 1;      /** Ĭ�ϵ���������*/
	static const bool DEFAULT_INDEX_ONLY = false;	/** Ĭ���Ƿ�ֻ������ */

	static const u32 DEFAULT_UPDATE_CACHE_TIME = 300;	/** Ĭ�ϸ��»���ʱ�� */
	static const u32 MIN_UPDATE_CACHE_TIME = 1;		/** ���»���ʱ����Сֵ */
	static const u32 MAX_UPDATE_CACHE_TIME = 7200;	/** ���»���ʱ�����ֵ */

	static const u8 DEFAULT_PCT_FREE = 5;			/** ����Ԥ���ռ�ٷֱ�Ĭ��ֵ */
	static const u8 MIN_PCT_FREE = 0;				/** ����Ԥ���ռ�ٷֱ���Сֵ */
	static const u8 MAX_PCT_FREE = 30;				/** ����Ԥ���ռ�ٷֱ����ֵ */

	static const u16 DEFAULT_INCR_SIZE = 32;		/** �ļ���չ��СĬ��ֵ */
	static const u16 MIN_INCR_SIZE = 8;				/** �ļ���չ��С��Сֵ */
	static const u16 MAX_INCR_SIZE = 32768;			/** �ļ���չ��С���ֵ */
#ifdef TNT_ENGINE
#ifdef NTSE_UNIT_TEST
	static const TableStatus DEFAULT_TABLE_STATUS = TS_NON_TRX;
#else
	static const TableStatus DEFAULT_TABLE_STATUS = TS_TRX;
#endif
#endif

	/** [MIN_NORMAL_TABLEID, MAX_NORMAL_TABLEID]Ϊ��ͨ��ID��[MAX_NORMAL_TABLEID + 1,MAX_TEMP_TABLEID]Ϊ��ʱ��ID
	 *  ��ͨ�����ʱ��ID + VLOB_TABLEID_DIFFΪ��Ӧ�Ĵ���������ID
	 *  ��ID�Ͷ�Ӧ�Ĵ�����ID��
	 *    static u16 getVirtualLobTableId(u16 normalTableId);
	 *    static u16 getNormalTableId(u16 virtualLobTableId);
	 *  ����ת��
	 */
	static const u16 MIN_NORMAL_TABLEID = 1;		/** ��ͨ��ID����Сֵ */
	static const u16 MAX_NORMAL_TABLEID = 30000;	/** ��ͨ��ID�����ֵ */
	/** ��ʱ��ID����Сֵ */
	static const u16 MIN_TEMP_TABLEID = MAX_NORMAL_TABLEID + 1;
	static const u16 MAX_TEMP_TABLEID = 32000;		/** ��ʱ��ID�����ֵ */
	/** ��ʱ�����ĸ��� */
	static const u16 MAX_TEMP_TABLE_NUM = MAX_TEMP_TABLEID - MAX_NORMAL_TABLEID + 1;
	static const u16 VLOB_TABLEID_DIFF = 32768;		/** ����������ID���Ӧ��ͨ�����ʱ��ID֮��Ĳ� */
	/** ����������ID��С��С */
	static const u16 MIN_VLOB_TABLEID = VLOB_TABLEID_DIFF + MIN_NORMAL_TABLEID;
	/** ����������ID����С */
	static const u16 MAX_VLOB_TABLEID = VLOB_TABLEID_DIFF + MAX_TEMP_TABLEID;
	static const u16 INVALID_TABLEID = 0;			/** ����ȷ�ı�ID */
	
	char	*m_schemaName;	/** schema�� */
	char	*m_name;		/** ���� */
	u16		m_id;			/** ȫ��Ψһ�ı�ID */
	u16		m_numCols;		/** ������ */
	u16		m_nullableCols;	/** ��ΪNULL�������� */
	u8		m_bmBytes;		/** ��¼��NULLλͼռ�õ��ֽ��� */
	u8		m_numIndice;	/** ������ */
	u16 	m_maxRecSize;	/** ��¼����󳤶ȣ����������ʱ���������������ݣ�������ֵλͼ */
	u32		m_maxMysqlRecSize; /** �ϲ�MYSQLROW ��ʽ������¼���ȣ���û�г����䳤�ֶ�ʱ����m_maxRecSize */
	u16		m_incrSize;		/** �ѡ����������������ļ���չ��С����λҳ���� */
	RecFormat	m_recFormat;/** ��¼���ͣ��ײ�洢ʹ�õĸ�ʽ */
	RecFormat   m_origRecFormat;/** ��¼ԭʼ���ͣ��ϲ㶨��ĸ�ʽ */
	ColumnDef	**m_columns;/** �����ֶ� */
	IndexDef	**m_indice;	/** �������� */
	bool	m_useMms;		/** �Ƿ�ʹ��MMS */
	bool	m_cacheUpdate;	/** �Ƿ�����MMS������Ϣ���� */
	bool	m_compressLobs;	/** �Ƿ�ѹ������� */
	bool	m_indexOnly;	/** �Ƿ�ֻ������ */
	bool	m_hasLob;		/** �Ƿ��������󣬸����Բ������л� */
	bool	m_hasLongVar;	/** �Ƿ���������䳤�ֶΣ������Բ������л� */
	u16		m_updateCacheTime;	/** ���»�����ˢ��ʱ�䣬��λ�� */
	u16		m_pctFree;		/** �����¼ʱҳ��Ԥ�����пռ�İٷֱ� */
	bool    m_fixLen;       /** Ϊfalse��ǿ��ʹ�ñ䳤�� */
	IndexDef	*m_pkey;	/** �������� */
	IndexDef	*m_pke;		/** �����ȼ������������Ϊ����������Ϊ��һ�����Զ�Ϊ��NULL��Ψһ������ */
	u64		m_version;		/** ����ϵͳʱ�䶨���汾 */
	/** ��¼ѹ����� */
	bool			m_isCompressedTbl;       /** �Ƿ���һ��ѹ���� */
	u8				m_numColGrps;            /** �������� */
	ColGroupDef       **m_colGrps;             /** ��ĸ��������� */
	RowCompressCfg  *m_rowCompressCfg;       /** ���¼ѹ������ */

#ifdef TNT_ENGINE
	//bool             m_tntTable;   /** �ñ����Ƿ�Ϊtnt��falseΪntse�� */
	TableStatus     m_tableStatus; //�ñ�״̬��Ϣ
#endif

#ifndef NTSE_UNIT_TEST
private:
#endif
	static const char *ID;
	static const u16  ID_DEFAULT = INVALID_TABLEID;
	static const char *SCHEMANAME;
	static const char *SCHEMANAME_DEFAULT;
	static const char *NAME;
	static const char *NAME_DEFAULT;
	static const char *NUMCOLS;
	static const u16  NUMCOLS_DEFAULT = 0;
	static const char *NULLABLECOLS;
	static const u16  NULLABLECOLS_DEFAULT = 0;
	static const char *NUMINDICE;
	static const u8   NUMINDICE_DEFAULT = 0;
	static const char *RECFORMAT;
	static const char *ORIG_RECFORMAT;
	static const RecFormat RECFORMAT_DEFAULT = REC_FIXLEN;
	static const char *MAXRECSIZE;
	static const u16  MAXRECSIZE_DEFAULT = 0;
	static const char *MAXMYSQLRECSIZE;
	static const u16  MAXMYSQLRECSIZE_DEFAULT = 0;
	static const char *USEMMS;
	static const bool USEMMS_DEFAULT = DEFAULT_USE_MMS;
	static const char *CACHEUPDATE;
	static const bool CACHEUPDATE_DEFAULT = DEFAULT_CACHE_UPDATE;
	static const char *UPDATECACHETIME;
	static const u16  UPDATECACHETIME_DEFAULT = DEFAULT_UPDATE_CACHE_TIME;
	static const char *COMPRESSLOBS;
	static const bool COMPRESSLOBS_DEFAULT = DEFAULT_COMPRESS_LOBS;
	static const char *BMBYTES;
	static const u8   BMBYTES_DEFAULT = 0;
	static const char *PCTFREE;
	static const u16  PCTFREE_DEFAULT = DEFAULT_PCT_FREE;
	static const char *INCRSIZE;
	static const u16  INCRSIZE_DEFAULT = DEFAULT_INCR_SIZE;
#ifdef TNT_ENGINE
	//static const char *TNTTABLE;
	//static const bool TNTTABLE_DEFAULT = DEFAULT_TNT_TABLE;
	static const char *TABLESTATUS;
	static const TableStatus TABLE_STATUS_DEFAULT = DEFAULT_TABLE_STATUS;
#endif
	static const char *INDEXONLY;
	static const bool INDEXONLY_DEFAULT = DEFAULT_INDEX_ONLY;
	static const char *COLUMN;
	static const char *COLUMN_DEFAULT;
	static const char *INDEX;
	static const char *INDEX_DEFAULT;
	static const char *FIXLEN;
	static const bool FIXLEN_DEFAULT = true;
	static const char *COMPRESS_TABLE;
	static const bool COMPRESS_TABLE_DEFAULT = false;
	static const char *COL_GROUPS_NUM;
	static const u8   COL_GROUPS_NUM_DEFAULT = 0;
	static const char *COL_GROUPS;
	static const char *COL_GROUPS_DEFAULT;
	static const char *ROW_COMPRESS_CFG;
	static const char *ROW_COMPRESS_CFG_DEFAULT;
};

#pragma pack()

/** ���ڷ�������TableDef����ṹ�ĸ����� */
class TableDefBuilder {
public:
	TableDefBuilder(u16 tableId, const char *schemaName, const char *tableName);
	~TableDefBuilder();
	TableDefBuilder* addColumn(const char *columnName, ColumnType columnType, bool nullable = true, CollType collation = COLL_BIN);
	TableDefBuilder* addColumnN(const char *columnName, ColumnType columnType, PrType prtype, bool nullable = true);
	TableDefBuilder* addColumnS(const char *columnName, ColumnType columnType, u16 maxSize, bool longVarConvertable = true, bool nullable = true, CollType collation = COLL_BIN);
	TableDefBuilder* addIndex(const char *indexName, bool primaryKey, bool unique, bool online, ...);
	TableDefBuilder* addIndex(const char *indexName, bool primaryKey, bool unique, bool online, Array<u16> &indexColumns, Array<u32> &prefixLenArr);
	TableDefBuilder* addColGrp(const u8& colGrpNo, const Array<u16> &grpColumns);
	TableDef* getTableDef();
	inline void setCompresssTbl(bool isCprsTbl) {
		m_isCompressTbl = isCprsTbl;
	}

private:
	u16		m_tableId;
	char	*m_schemaName;
	char	*m_tableName;
	bool	m_isCompressTbl;           /** �Ƿ���ѹ���� */
	Array<ColumnDef *>	m_columns;
	Array<IndexDef *>	m_indice;
	Array<ColGroupDef *>  m_colGrps;     /** ���������鶨�� */
	bool	m_hasLob;
	bool	m_hasLongVar;
};
}

#endif

