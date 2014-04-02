/**
 * 记录的表示
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 * @author 李伟钊(liweizhao@corp.netease.com)
 */

#ifndef _NTSE_RECORD_H_
#define _NTSE_RECORD_H_

#include <stddef.h>
#include "Global.h"
#include "util/Array.h"


namespace ntse {

class CmprssColGrpExtractInfo;
class ColumnDef;
class TableDef;
class MemoryContext;
class IndexDef;
struct Stream;
class LobStorage;

#ifdef TNT_ENGINE
class Syslog;
#endif 
/*
记录、部分记录及不同格式记录之间的转换规则:
1.REC_REDUNDANT格式的记录与部分记录都为所有属性分配最大存储空间和空值位图，
  因此具有相同的格式，不需要转换
2.REC_MYSQL格式的记录与部分记录都为所有属性分配最大存储空间和空值位图，
  因此具有相同的格式，不需要转换
3.REC_FIXLEN和REC_REDUNDANT的记录格式相同，不需要转换，但部分记录之间不可相互转换
4.表中不包含大对象时，REC_REDUNDANT与REC_MYSQL的记录与部分记录格式相同，不需要转换
  其它情况下，需要调用RecordOper类的对应函数转换
5.REC_COMPRESSORDER格式的记录跟变长格式类似，但是字段的顺序是按照属性组定义排列的，
  跟REC_VARLEN格式的记录类似, 实际上是一种压缩解压缩的中间状态的格式；
6.REC_COMPRESSED格式的记录解压缩之后即为REC_COMPRESSORDER格式，如果要转换成REC_REDUNDANT
  或者REC_VARLEN还需要调用RecordOper类的对应函数转换。
*/

/** 记录数据格式 */
enum RecFormat {
	REC_FIXLEN,		  /** 定长记录，与冗余格式一致 */
	REC_VARLEN,		  /** 变长记录, 变长记录的SubRecord格式同Record格式 */
	REC_REDUNDANT,	  /** 冗余格式，每个字段都分配最大的空间，并且SubRecord中也为表中所有属性分配空间，
					      不包含大对象大小信息，统一设为0 */
	REC_MYSQL,		  /** 引擎层MySQL格式，没有大对象时与冗余格式一致，包含大对象大小信息, 超长字段按照大对象方式存储，与引擎层表定义保持一致 */
	REC_UPPMYSQL,	  /** 上层MYSQL格式，没有大对象时与REC_MYSQL格式一致，超长字段按照varchar/varbinary存储，与上层表定义一致 */
	KEY_MYSQL,		  /** MYSQL搜索键格式，每个可空属性前有一个字节表示是否为空，VARCHAR一定用两字节表示长度 */
	KEY_PAD,		  /** 索引键格式，其属性号顺序按索引定义表示，不进行压缩，且为每个属性分配最大存储空间 */
	KEY_NATURAL,	  /** 索引键格式，其属性号顺序按索引定义表示，不进行压缩 */
	KEY_COMPRESS,	  /** 索引键格式，其属性号顺序按索引定义表示，进行压缩 */
	REC_COMPRESSED,   /** 压缩格式记录 */
	REC_COMPRESSORDER,/** 压缩排序格式记录, 跟变长格式类似，但是字段的顺序是按照属性组定义排列的 */
};

extern const char* getRecFormatStr(RecFormat format); 

// 大对象队列元素
class LobPair {
public:
	LobPair(byte *lob, u32 lobSize)
		: m_lob(lob), m_size(lobSize){
	}
	byte		*m_lob;     /** 大对象内容 */
	u32			m_size;		/** 大对象大小*/
};

/** 记录 */
class Record {
public:
	Record() {}
	Record(RowId rowId, RecFormat format, byte *data, uint size)
		: m_rowId(rowId), m_format(format), m_size(size), m_data(data) {
	}
	RowId		m_rowId;	/** 记录ID */
	RecFormat	m_format;	/** 记录数据格式 */
	uint		m_size;		/** 数据大小 */
	byte		*m_data;	/** 数据 */
};

/**
 * 根据属性组定义调整列与列顺序后的压缩排序格式的记录
 *
 */
class CompressOrderRecord : public Record {
public:
	CompressOrderRecord() : Record(INVALID_ROW_ID, REC_COMPRESSORDER, NULL, 0), m_numSeg(0), m_segSizes(NULL) {}
	CompressOrderRecord(RowId rowId, byte *data, uint size, u8 numSeg, size_t *segSizes)
		: Record(rowId, REC_COMPRESSORDER, data, size), m_numSeg(numSeg), m_segSizes(segSizes) {
	}
	u8     m_numSeg;      /** 属性组段数目 */
	size_t *m_segSizes;   /** 各个属性组段的长度 */
};

/** 属性列表 */
struct ColList {
public:
	ColList();
	ColList(u16 size, u16 *cols);
	static ColList generateAscColList(MemoryContext *mc, u16 startCno, u16 size); 
	ColList copy(MemoryContext *mc) const;
	ColList merge(MemoryContext *mc, const ColList &another) const;
	void merge(const ColList &a, const ColList &b);
	ColList except(MemoryContext *mc, const ColList &another) const;
	ColList sort(MemoryContext *mc) const;
	bool isSubsetOf(const ColList &another) const;
	bool hasIntersect(const ColList &another) const;
	bool operator == (const ColList &another) const;
	bool isAsc() const;
	
public:
	u16			m_size;		/** 属性数 */
	u16			*m_cols;	/** 属性号列表 */
};

/** 记录的一部分 */
class SubRecord {
public:
	SubRecord() {}
	SubRecord(RecFormat format, u16 numCols, u16 *columns, byte *data, uint size, RowId rowId = INVALID_ROW_ID) {
		m_format = format;
		m_rowId = rowId;
		m_numCols = numCols;
		m_columns = columns;
		m_data = data;
		m_size = size;
	}

	void resetColumns(u16 numCols, u16 *columns) {
		m_numCols = numCols;
		m_columns = columns;
	}

	RowId		m_rowId;	/** 记录ID */
	RecFormat	m_format;	/** 记录数据格式 */
	u16			m_numCols;	/** 属性数 */
	/** 各属性在表字段列表中的位置，从0开始编号，若格式不为KEY_XXX
	 * 一定是递增的，为KEY_XXX时由索引定义决定。
	 */
	u16			*m_columns;
	uint		m_size;		/** 数据大小 */
	byte		*m_data;	/** 数据 */
};

/** 压缩记录提取器 */
class CmprssRecordExtractor {
public:
	virtual ~CmprssRecordExtractor() {}
	/**
	* 压缩一条记录
	* @pre 记录必须为REC_COMPRESSORDER格式
	* @post dest的m_id, m_data, m_size可能被修改
	* @param src 待压缩的记录
	* @param dest OUT 压缩后的记录, 调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
	*   已经分配的内存大小，防止越界
	* @return 压缩比
	*/
	virtual double compressRecord(const CompressOrderRecord *src, Record *dest) = 0;

	/**
	* 解压缩一条记录
	* @pre 记录必须为REC_COMPRESSED格式
	* @post dest的m_id, m_data, m_size可能被修改
	* @param src 待解压的记录
	* @param dest OUT 解压缩后的记录, 调用者必须为保存输出内容分配足够多的内存，并且通过设置dest.m_size告知
	*   已经分配的内存大小
	*/
	virtual void decompressRecord(const Record *src, CompressOrderRecord *dest) = 0;

	/** 
	* 压缩一个属性组
	* @param src 待压缩段的地址
	* @param offset 待压缩段的偏移量
	* @param len 待压缩段的长度
	* @param dest OUT 输出压缩后的段，调用方保证有足够内存
	* @param destSize OUT 输出压缩后段的长度
	*/
	virtual void compressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) = 0;

	/** 
	* 解压缩一个属性组
	* @param src 待解压缩段的地址
	* @param offset 待解压缩段的偏移量
	* @param len 待解压缩段的长度
	* @param dest OUT 输出解压缩后的段，调用方保证有足够内存
	* @param destSize OUT 输出解压缩后段的长度
	*/
	virtual void decompressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) = 0;
	
	/**
	 * 计算压缩记录解压缩后的大小
	 * @pre 记录是压缩格式的
	 * @param cprsRcd 要计算的压缩记录
	 */
	virtual u64 calcRcdDecompressSize(const Record *cprsRcd) const = 0;

	/**
	* 计算压缩属性组解压缩后的大小
	* @param src 属性组数据
	* @param offset 数据开始偏移量
	* @param len 数据长度
	*/
	virtual u64 calcColGrpDecompressSize(const byte *src, const uint& offset, const uint& len) const = 0;
};

class TableDef;
class MemoryContext;
class Session;
/** 从记录中提取子记录的提取器 */
class SubrecExtractor {
public:
	static SubrecExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef, u16 numCols, const u16 *columns, 
		RecFormat srcFmt, RecFormat dstFmt, uint extractCount = (uint) -1);
	static SubrecExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef, u16 numCols, const u16* columns, 
		CmprssRecordExtractor *cprsRcdExtrator);
	static SubrecExtractor* createInst(Session *session, const TableDef *tableDef, const SubRecord *dstRec, 
		uint extractCount = (uint) -1, CmprssRecordExtractor *cprsRcdExtrator = NULL); 
	/** 从记录中提取子记录
	 * 注: 使用本函数提取子记录可能会导致目标子记录中非被提取记录被覆盖
	 *
	 * @param record 记录
	 * @param subRecord 子记录
	 */
	virtual void extract(const Record *record, SubRecord *subRecord) = 0;
	virtual ~SubrecExtractor() {}
};

/** 从子记录中提取子记录的提取器 */
class SubToSubExtractor {
public:
	static SubToSubExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef,
		const IndexDef *indexDef, u16 numColsSrc, const u16 *columnsSrc, u16 numColsDst, 
		const u16 *columnsDst, RecFormat srcFmt, RecFormat dstFmt, uint extractCount = (uint) -1);
	static SubToSubExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef, 
		const IndexDef *indexDef, u16 numColsSrc, const u16 *columnsSrc, u16 numColsDst,
		const u16 *columnsDst, RecFormat dstFmt, uint extractCount);
	/** 从子记录中提取子记录
	 * 注: 使用本函数提取子记录可能会导致目标子记录中非被提取记录被覆盖
	 *
	 * @param srSrc 源子记录
	 * @param srDst 目标子记录
	 */
	virtual void extract(const SubRecord *srSrc, SubRecord *srDst) = 0;
	virtual ~SubToSubExtractor() {}
};

/** 对记录进行的各种操作 */
class RecordOper {
public:
	static void extractKeyRC(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);
	static void convertKeyPC(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);
	static void convertKeyPN(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);
	static void convertKeyCP(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);
	static bool convertKeyMP(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);

	static void convertKeyNP(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);
	static void convertKeyNC(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);
	static void convertKeyCN(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);

	static void extractKeyRN(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);
	static void extractKeyFN(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, SubRecord *key);
	static void extractKeyVN(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);
	static void extractKeyRP(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);
	static bool extractKeyRPWithRet(const TableDef *tableDef, const IndexDef *indexDef, const Record *record, Array<LobPair*> *lobArray, SubRecord *key);

	static void extractSubRecordFR(const TableDef *tableDef, const Record *record, SubRecord *subRecord);
	static void extractSubRecordVR(const TableDef *tableDef, const Record *record, SubRecord *subRecord);
	static void extractSubRecordCR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord);
	static void extractSubRecordNR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord);
	static void extractSubRecordPR(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord);
	static void extractSubRecordCRNoLobColumn(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord);
	static void extractSubRecordPRNoLobColumn(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *key, SubRecord *subRecord);

	static void extractLobFromR(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
		LobStorage *lobStorage, const Record *record, Array<LobPair *> *lobArray);
	static void extractLobFromR(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
		LobStorage *lobStorage, const SubRecord *record, Array<LobPair *> *lobArray);
	static void extractLobFromM(Session *session, const TableDef *tableDef, const IndexDef *indexDef, 
		const Record *record, Array<LobPair *> *lobArray);
	static void extractLobFromMixedMR(Session *session, const TableDef *tableDef, const IndexDef *indexDef, LobStorage *lobStorage,
		const Record *record, u16 numColumnsInMysqlFormat, u16 *columnsInMysqlFormat, Array<LobPair *> *lobArray);

	static void convertRecordMUpToEngine(const TableDef *tableDef, const Record *src, Record *dest);
	static void convertSubRecordMUpToEngine(const TableDef *tableDef, const SubRecord *src, SubRecord *dest);
	static void convertRecordMEngineToUp(const TableDef *tableDef, const Record *src, Record *dest);
	static void convertSubRecordMEngineToUp(const TableDef *tableDef, const SubRecord *src, SubRecord *dest);
	static void convertRecordVFR(TableDef *tableDef, Record *src, Record *dest);
	static void convertRecordRV(const TableDef *tableDef, const Record *src, Record *dest);
	static void convertRecordVR(const TableDef *tableDef, const Record *src, Record *dest);
	static void convertSubRecordRV(const TableDef *tableDef, const SubRecord *src, SubRecord *dest);
	static void convertSubRecordVR(const TableDef *tableDef, const SubRecord *src, SubRecord *dest);
	static void updateRecordRR(const TableDef *tableDef, Record *record, const SubRecord *update);
	static void updateRecordFR(const TableDef *tableDef, Record *record, const SubRecord *update);
	static u16 getUpdateSizeVR(const TableDef *tableDef, const Record *record, const SubRecord *update);
	static u16 getUpdateSizeNoCompress(MemoryContext *mtx, const TableDef *tableDef, 
		CmprssRecordExtractor *cprsRcdExtrator, const Record *oldRcd, const SubRecord *update);
	static void updateRecordVRInPlace(const TableDef *tableDef, Record *record, const SubRecord *update, size_t oldBufferSize);
	static uint updateRecordVR(const TableDef *tableDef, const Record *record, const SubRecord *update, byte *newBuf);
	static void compressKey(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest);
	static void mergeSubRecordRR(const TableDef *tableDef, SubRecord *newSr, const SubRecord *oldSr);


	//根据记录压缩模块引入的记录格式转换接口
	static void convRecordRedToCO(const TableDef *tableDef, const Record *src, CompressOrderRecord *dest);
	static void convRecordCOToRed(const TableDef *tableDef, const CompressOrderRecord *src, Record *dest);
	static void convRecordCOToVar(MemoryContext *ctx, const TableDef *tableDef, const CompressOrderRecord *src, Record *dest);
	static void convRecordVarToCO(MemoryContext *ctx, const TableDef *tableDef, const Record *src, CompressOrderRecord *dest);
	static double convRecordCOToComprssed(CmprssRecordExtractor *cprsRcdExtrator, 
		const CompressOrderRecord *src, Record *dest);
	static void convRecordCompressedToCO(CmprssRecordExtractor *cprsRcdExtrator, const Record *src, CompressOrderRecord *dest);
	static void convRecordCompressedToVar(MemoryContext *ctx, const TableDef *tableDef, CmprssRecordExtractor *cprsRcdExtrator, 
		Record *src, Record *dest = NULL);
	static void extractSubRecordCompressedR(MemoryContext *mtx, CmprssRecordExtractor *cprsRcdExtrator, const TableDef *tableDef, 
		const Record *record, SubRecord *subRecord, CmprssColGrpExtractInfo *cmpressExtractInfo = NULL);
	static void updateRcdWithDic(MemoryContext *ctx, const TableDef *tableDef, CmprssRecordExtractor *cprsRcdExtrator, 
		const Record *oldRcd, const SubRecord *update, Record *newRcd);
	static void updateCompressedRcd(MemoryContext *ctx, const TableDef *tableDef, CmprssRecordExtractor *cprsRcdExtrator, 
		const Record *oldRcd, const SubRecord *update, Record *newRcd);
	static void updateUncompressedRcd(MemoryContext *ctx, const TableDef *tableDef, CmprssRecordExtractor *cprsRcdExtrator, 
		const Record *oldRcd, const SubRecord *update, Record *newRcd);
	static u8 readCompressedColGrpSize(byte *src, uint *bytes);
	static u8 writeCompressedColGrpSize(byte *dest, const uint& size);

	static bool isFastCCComparable(const TableDef *tableDef, const IndexDef *indexDef, u16 numKeyCols, const u16 *keyCols);
	static int compareKeyCC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);
	static int compareKeyRC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);
	static int compareKeyPC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);
	static int compareKeyNC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);

	static int compareKeyPCOrNC(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);
	static int compareKeyNP(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);
	static int compareKeyNN(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);
	static int compareKeyPP(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);

	static int compareKeyNNorPPColumnSize( const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);

	static int compareKeyRR(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef = NULL);
	static int compareKeyPR(const TableDef *tableDef, const SubRecord *key1, 
		const SubRecord *key2, const IndexDef *indexDef = NULL);
	static bool isSubRecordEq(const TableDef* tableDef, const SubRecord* sb1,const SubRecord* sb2, const IndexDef *indexDef = NULL);
	static bool isRecordEq(const TableDef* tableDef, const Record* r1, const Record* r2);


	static SubRecord* createSlobSubRecordR(const TableDef *tableDef, SubRecord *sr, const byte *data, size_t size, size_t orgSize);
	static Record* createSlobRecord(const TableDef *tableDef, Record *rec, const byte *data, size_t size, size_t orgSize);
	static byte* extractSlobData(const TableDef *tableDef, const Record *rec, size_t *size, size_t *orgSize);

	static void fastExtractSubRecordFR(const TableDef *tableDef, const Record *record, SubRecord *subRecord);
	static void fastExtractSubRecordVR(const TableDef *tableDef, const Record *record, SubRecord *subRecord);

	static bool isNullR(const TableDef *tableDef, const Record *record, u16 cno);
	static bool isNullR(const TableDef *tableDef, const SubRecord *subRec, u16 cno);
	static void setNullR(const TableDef *tableDef, const Record *record, u16 cno, bool null);
	static void setNullR(const TableDef *tableDef, const SubRecord *subRec, u16 cno, bool null);

	static u16 getKeySizeCN(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *compressedKey);
	static u16 getRecordSizeRV(const TableDef *tableDef, const Record *record);
	static u16 getSubRecordSizeRV(const TableDef *tableDef, const SubRecord *subRec);

	static Record* initEmptyRecord(Record *record, const TableDef *tableDef, RowId rowId, RecFormat recFormat);

	static LobId readLobId(const byte *record, ColumnDef *columnDef);
	static void writeLobId(byte *record, ColumnDef *columnDef, LobId lobId);
	static uint readLobSize(const byte *record, ColumnDef *columnDef);
	static void writeLobSize(byte *record, ColumnDef *columnDef, uint size);
	static byte* readLob(const byte *record, ColumnDef *columnDef);
	static void writeLob(byte *record, ColumnDef *columnDef, byte *lob);

	static size_t getSubRecordSerializeSize(const TableDef *tableDef, const SubRecord *subRecord, bool isLobNeeded, bool isDataOnly = false);
	static void serializeSubRecordMNR(Stream *s, const TableDef *tableDef, const SubRecord *subRecord, bool isLobNeeded, bool isDataOnly = false);
	static SubRecord* unserializeSubRecordMNR(Stream *s, const TableDef *tableDef, MemoryContext *memoryContext);
	static void unserializeSubRecordMNR(Stream *s, const TableDef *tableDef, u16 numCols, u16 *columns, byte *buf);

#ifdef TNT_ENGINE
	static void appendKeyTblIdAndIdxId(SubRecord *key, TableId tblId, u8 idxNo);
	static u16 extractDiffColumns(const TableDef *tableDef, const Record *rec1, const Record *rec2, u16 **cols, MemoryContext *ctx, bool onlyLob);
	static void parseAndPrintRedSubRecord(Syslog *syslog, const TableDef *tableDef, const SubRecord *subRec);
#endif
private:
	static int realCompareKeyNNorPP(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, const IndexDef *indexDef);
};

struct Tracer;
Tracer& operator << (Tracer& tracer, RecFormat format);

}

#endif
