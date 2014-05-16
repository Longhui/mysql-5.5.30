/**
 * 表定义
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TABLEDEF_H_
#define _NTSE_TABLEDEF_H_

#include "misc/Global.h"
#include "misc/Record.h"
#include "util/Array.h"
#include "compress/RowCompressCfg.h"

namespace ntse {

/** 属性类型 */
/* 属性类型修改的话，记录转换的地方需要增加选项 */
enum ColumnType {
	CT_TINYINT,		/** 一字节整数 */
	CT_SMALLINT,	/** 二字节整数 */
	CT_MEDIUMINT,	/** 三字节整数类型 */
	CT_INT,			/** 四字节整数 */
	CT_BIGINT,		/** 八字节整数 */
	CT_FLOAT,		/** 浮点类型 */
	CT_DOUBLE,		/** Double/Real类型 */
	CT_DECIMAL,		/** Decimal/Numeric类型 */
	CT_RID,			/** 特殊的RID类型 */
	CT_CHAR,		/** 定长字符串 */
	CT_VARCHAR,		/** 变长字符串 */
	CT_BINARY,		/** binary类型 */
	CT_VARBINARY,	/** varbinary类型 */
	CT_SMALLLOB,	/** 小型大对象，最大大小64K */
	CT_MEDIUMLOB,	/** 中型大对象，最大大小16M */
};

/** 属性字符集与collation类型 */
enum CollType {
	COLL_BIN,		/** 当作二进制处理，比较大小可用memcmp */
	COLL_LATIN1,    /** 西欧字符(ISO-8859-1) */
	COLL_GBK,		/** GBK编码的中文 */
	COLL_UTF8,		/** UTF8编码的中文 */
	COLL_UTF8MB4,	/** UTF8MB4编码的中文*/
};

enum TableStatus {
	TS_NON_TRX = 0,  //非事务表
	TS_TRX,          //事务表
	TS_SYSTEM,       //系统表
	TS_CHANGING,     //表类型转化中
	TS_ONLINE_DDL,   //表正在做online ddl操作
};

/** collation，用于按不同的规则比较字符串大小 */
class Collation {
public:
	/**
	 * 比较两个字符串
	 *
	 * @param coll Collation
	 * @param str1 字符串1
	 * @param len1 字符串1大小
	 * @param str2 字符串2
	 * @param len2 字符串2大小
	 * @return str1 < str2时返回<0，str1 = str2时返回0，否则返回>0
	 */
	static int strcoll(CollType coll, const byte *str1, size_t len1, const byte *str2, size_t len2);
	static size_t charpos(CollType coll, const char *pos, const char *end, size_t length);
	static void getMinMaxLen(CollType coll, size_t *mbMinLen, size_t *mbMaxLen);
};

// 精确类型
struct PrType {
	PrType(): m_flags(0), m_precision(0), m_deicmal(0) {}
	
	inline void setUnsigned() {
		m_flags |= UNSIGNED;
	}

	inline bool isUnsigned() const{
		return (m_flags & UNSIGNED) != 0;
	}
	
	u16 m_flags;	// 是否无符号等标志位
	u8 m_precision;	// 最大位数，包括小数
	u8 m_deicmal;	// 小数点位数

	static const u16 UNSIGNED = 0x1; // 无符号标识位
};

/** 字段定义 */
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

	static const u16 RID_COLUMN_NO = 65534;	/** 特殊的RowId属性的属性号 */

	u16		m_no;				/** 属性编号，从0开始 */
	char	*m_name;			/** 属性名 */
	ColumnType	m_type;			/** 类型 */
	CollType	m_collation;	/** 字符集与collation */
	u16		m_offset;			/** 定长或冗余记录格式中字段存储位置偏移，包含空值位图 */
	u16		m_size;				/** 最大长度，对于VARCHAR类型已经包含了一至两个字节的长度 */
	u32		m_mysqlOffset;		/** MYSQL格式中字段存储位置偏移，包含空值位图 */
	u16		m_mysqlSize;		/** MYSQL格式最大长度，对于VARCHAR类型已经包含了一至两个字节的长度 */
	bool	m_nullable;			/** 字段是否可以为空 */
	u16		m_nullBitmapOffset; /** 空值位图对应的位 */
	byte	m_lenBytes;			/** 表示VARCHAR类型长度需要的字节数 */
	bool	m_cacheUpdate;		/** 在表启用MMS更新缓存时，对这个字段的更新是否缓存 */
	PrType	m_prtype;			/** 精确类型，包含符号信息，精度信息等 */
	bool	m_compressable;		/** 是否可以被压缩 */
	u16		m_maxCompressSize;	/** 可以被压缩的属性压缩后的最大长度，对于不可压缩的属性即为m_size */
	bool	m_inIndex;			/** 字段是否为某个索引的一部分 */
	bool	m_longVar;			/** 字段是否是超长varchar/varbinary类型 */
	/** 记录压缩相关 */
	u8      m_colGrpNo;      /** 字段所属的属性组号 */
	u16     m_colGrpOffset;  /** 字段在属性组中的位置 */

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
/** 索引定义 */
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

	static const s8 SMART_SPLIT_FACTOR = -1;		/** 智能检测索引分裂时左右页面数据百分比 */
	static const s8 MIN_SPLIT_FACTOR = 5;			/** 分裂系数最小值 */
	static const s8 MAX_SPLIT_FACTOR = 95;			/** 分裂系数最大值 */

	char	*m_name;		/** 索引名 */
	bool	m_primaryKey;	/** 是否为主键 */
	bool	m_unique;		/** 是否为唯一性索引 */
	bool	m_online;		/** 是否为在线索引 */
	bool	m_prefix;		/** 是否是含前缀的索引 */
	bool	m_hasLob;		/** 是否包含大对象列 */
	u16		m_maxKeySize;	/** 索引键最大长度，包含空值位图 */
	u16		m_numCols;		/** 涉及的列数 */
	u16		*m_columns;		/** 各个索引键在表字段列表中的位置，从0开始编号 */
	u16		*m_offsets;		/** 各索引属性在KEY_PAD格式中存储起始位置 */
	u32		*m_prefixLens;  /** 各索引属性前缀长度 */
	u8		m_bmBytes;		/** 索引键中NULL位图占用的字节数 */
	s8		m_splitFactor;	/** 索引分裂时左边页面存放数据百分比，默认为SMART_SPLIT_FACTOR */

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
 * 属性组定义
 *
 * 属性组定义为一组属性的组合，保存各个属于属性组的列在表字段列表中的位置。一个表中可能有多个属性组。
 */
class ColGroupDef {
public:
	static const uint MAX_NUM_COL_GROUPS = 256; /* 属性组最多个数 */
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

	u8      m_colGrpNo;     /** 属性组在表的所有属性组中的序号, 从0开始编号 */
	u16		m_numCols;		/** 包含的列数 */
	u16		*m_colNos;      /** 各个列在表字段列表中的编号 */
};

/**
 * 属性组定义构建器
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
	u8         m_colGrpNo;  /* 属性组号*/
	Array<u16> m_colNos;    /* 属性组中各个属性的属性号 */
};

#pragma pack(4)
/** 表定义 */
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
	
    /** 属性组定义相关 */
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
	static const bool DEFAULT_USE_MMS = true;		/** 默认是否使用MMS */
	static const bool DEFAULT_CACHE_UPDATE = false;	/** 默认是否缓存更新 */
	static const bool DEFAULT_COMPRESS_LOBS = true;	/** 默认是否压缩大对象 */
	static const bool DEFAULT_COMPRESS_ROW = false; /** 默认是否启用记录压缩*/
	static const bool DEFAULT_FIX_LEN = true;      /** 默认是否使用定长堆 */
	static const u8 DEFAULT_COLUMN_GROUPS = 1;      /** 默认的属性组数*/
	static const bool DEFAULT_INDEX_ONLY = false;	/** 默认是否只有索引 */

	static const u32 DEFAULT_UPDATE_CACHE_TIME = 300;	/** 默认更新缓存时间 */
	static const u32 MIN_UPDATE_CACHE_TIME = 1;		/** 更新缓存时间最小值 */
	static const u32 MAX_UPDATE_CACHE_TIME = 7200;	/** 更新缓存时间最大值 */

	static const u8 DEFAULT_PCT_FREE = 5;			/** 堆中预留空间百分比默认值 */
	static const u8 MIN_PCT_FREE = 0;				/** 堆中预留空间百分比最小值 */
	static const u8 MAX_PCT_FREE = 30;				/** 堆中预留空间百分比最大值 */

	static const u16 DEFAULT_INCR_SIZE = 32;		/** 文件扩展大小默认值 */
	static const u16 MIN_INCR_SIZE = 8;				/** 文件扩展大小最小值 */
	static const u16 MAX_INCR_SIZE = 32768;			/** 文件扩展大小最大值 */
#ifdef TNT_ENGINE
#ifdef NTSE_UNIT_TEST
	static const TableStatus DEFAULT_TABLE_STATUS = TS_NON_TRX;
#else
	static const TableStatus DEFAULT_TABLE_STATUS = TS_TRX;
#endif
#endif

	/** [MIN_NORMAL_TABLEID, MAX_NORMAL_TABLEID]为普通表ID，[MAX_NORMAL_TABLEID + 1,MAX_TEMP_TABLEID]为临时表ID
	 *  普通表或临时表ID + VLOB_TABLEID_DIFF为对应的大对象虚拟表ID
	 *  表ID和对应的大对象表ID用
	 *    static u16 getVirtualLobTableId(u16 normalTableId);
	 *    static u16 getNormalTableId(u16 virtualLobTableId);
	 *  进行转换
	 */
	static const u16 MIN_NORMAL_TABLEID = 1;		/** 普通表ID的最小值 */
	static const u16 MAX_NORMAL_TABLEID = 30000;	/** 普通表ID的最大值 */
	/** 临时表ID的最小值 */
	static const u16 MIN_TEMP_TABLEID = MAX_NORMAL_TABLEID + 1;
	static const u16 MAX_TEMP_TABLEID = 32000;		/** 临时表ID的最大值 */
	/** 临时表最多的个数 */
	static const u16 MAX_TEMP_TABLE_NUM = MAX_TEMP_TABLEID - MAX_NORMAL_TABLEID + 1;
	static const u16 VLOB_TABLEID_DIFF = 32768;		/** 大对象虚拟表ID与对应普通表或临时表ID之间的差 */
	/** 大对象虚拟表ID最小大小 */
	static const u16 MIN_VLOB_TABLEID = VLOB_TABLEID_DIFF + MIN_NORMAL_TABLEID;
	/** 大对象虚拟表ID最大大小 */
	static const u16 MAX_VLOB_TABLEID = VLOB_TABLEID_DIFF + MAX_TEMP_TABLEID;
	static const u16 INVALID_TABLEID = 0;			/** 不正确的表ID */
	
	char	*m_schemaName;	/** schema名 */
	char	*m_name;		/** 表名 */
	u16		m_id;			/** 全局唯一的表ID */
	u16		m_numCols;		/** 属性数 */
	u16		m_nullableCols;	/** 可为NULL的属性数 */
	u8		m_bmBytes;		/** 记录中NULL位图占用的字节数 */
	u8		m_numIndice;	/** 索引数 */
	u16 	m_maxRecSize;	/** 记录的最大长度，包含大对象时不包括大对象的内容，包含空值位图 */
	u32		m_maxMysqlRecSize; /** 上层MYSQLROW 格式的最大记录长度，在没有超长变长字段时等于m_maxRecSize */
	u16		m_incrSize;		/** 堆、索引或大对象数据文件扩展大小，单位页面数 */
	RecFormat	m_recFormat;/** 记录类型，底层存储使用的格式 */
	RecFormat   m_origRecFormat;/** 记录原始类型，上层定义的格式 */
	ColumnDef	**m_columns;/** 各个字段 */
	IndexDef	**m_indice;	/** 各个索引 */
	bool	m_useMms;		/** 是否使用MMS */
	bool	m_cacheUpdate;	/** 是否启用MMS更新信息缓存 */
	bool	m_compressLobs;	/** 是否压缩大对象 */
	bool	m_indexOnly;	/** 是否只有索引 */
	bool	m_hasLob;		/** 是否包含大对象，该属性不被序列化 */
	bool	m_hasLongVar;	/** 是否包含超长变长字段，该属性不被序列化 */
	u16		m_updateCacheTime;	/** 更新缓存间隔刷新时间，单位秒 */
	u16		m_pctFree;		/** 插入记录时页面预留空闲空间的百分比 */
	bool    m_fixLen;       /** 为false则强制使用变长堆 */
	IndexDef	*m_pkey;	/** 主键索引 */
	IndexDef	*m_pke;		/** 主键等价物，若有主键则为主键，否则为第一个属性都为非NULL的唯一性索引 */
	u64		m_version;		/** 利用系统时间定义表版本 */
	/** 记录压缩相关 */
	bool			m_isCompressedTbl;       /** 是否是一个压缩表 */
	u8				m_numColGrps;            /** 属性组数 */
	ColGroupDef       **m_colGrps;             /** 表的各个属性组 */
	RowCompressCfg  *m_rowCompressCfg;       /** 表记录压缩配置 */

#ifdef TNT_ENGINE
	//bool             m_tntTable;   /** 该表定义是否为tnt表，false为ntse表 */
	TableStatus     m_tableStatus; //该表状态信息
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

/** 用于方便生成TableDef表定义结构的辅助类 */
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
	bool	m_isCompressTbl;           /** 是否是压缩表 */
	Array<ColumnDef *>	m_columns;
	Array<IndexDef *>	m_indice;
	Array<ColGroupDef *>  m_colGrps;     /** 所有属性组定义 */
	bool	m_hasLob;
	bool	m_hasLongVar;
};
}

#endif

