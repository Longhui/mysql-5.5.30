/**
 * ��¼�ı�ʾ
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 * @author ��ΰ��(liweizhao@corp.netease.com)
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
��¼�����ּ�¼����ͬ��ʽ��¼֮���ת������:
1.REC_REDUNDANT��ʽ�ļ�¼�벿�ּ�¼��Ϊ�������Է������洢�ռ�Ϳ�ֵλͼ��
  ��˾�����ͬ�ĸ�ʽ������Ҫת��
2.REC_MYSQL��ʽ�ļ�¼�벿�ּ�¼��Ϊ�������Է������洢�ռ�Ϳ�ֵλͼ��
  ��˾�����ͬ�ĸ�ʽ������Ҫת��
3.REC_FIXLEN��REC_REDUNDANT�ļ�¼��ʽ��ͬ������Ҫת���������ּ�¼֮�䲻���໥ת��
4.���в����������ʱ��REC_REDUNDANT��REC_MYSQL�ļ�¼�벿�ּ�¼��ʽ��ͬ������Ҫת��
  ��������£���Ҫ����RecordOper��Ķ�Ӧ����ת��
5.REC_COMPRESSORDER��ʽ�ļ�¼���䳤��ʽ���ƣ������ֶε�˳���ǰ��������鶨�����еģ�
  ��REC_VARLEN��ʽ�ļ�¼����, ʵ������һ��ѹ����ѹ�����м�״̬�ĸ�ʽ��
6.REC_COMPRESSED��ʽ�ļ�¼��ѹ��֮��ΪREC_COMPRESSORDER��ʽ�����Ҫת����REC_REDUNDANT
  ����REC_VARLEN����Ҫ����RecordOper��Ķ�Ӧ����ת����
*/

/** ��¼���ݸ�ʽ */
enum RecFormat {
	REC_FIXLEN,		  /** ������¼���������ʽһ�� */
	REC_VARLEN,		  /** �䳤��¼, �䳤��¼��SubRecord��ʽͬRecord��ʽ */
	REC_REDUNDANT,	  /** �����ʽ��ÿ���ֶζ��������Ŀռ䣬����SubRecord��ҲΪ�����������Է���ռ䣬
					      ������������С��Ϣ��ͳһ��Ϊ0 */
	REC_MYSQL,		  /** �����MySQL��ʽ��û�д����ʱ�������ʽһ�£�����������С��Ϣ, �����ֶΰ��մ����ʽ�洢�����������屣��һ�� */
	REC_UPPMYSQL,	  /** �ϲ�MYSQL��ʽ��û�д����ʱ��REC_MYSQL��ʽһ�£������ֶΰ���varchar/varbinary�洢�����ϲ����һ�� */
	KEY_MYSQL,		  /** MYSQL��������ʽ��ÿ���ɿ�����ǰ��һ���ֽڱ�ʾ�Ƿ�Ϊ�գ�VARCHARһ�������ֽڱ�ʾ���� */
	KEY_PAD,		  /** ��������ʽ�������Ժ�˳�����������ʾ��������ѹ������Ϊÿ�����Է������洢�ռ� */
	KEY_NATURAL,	  /** ��������ʽ�������Ժ�˳�����������ʾ��������ѹ�� */
	KEY_COMPRESS,	  /** ��������ʽ�������Ժ�˳�����������ʾ������ѹ�� */
	REC_COMPRESSED,   /** ѹ����ʽ��¼ */
	REC_COMPRESSORDER,/** ѹ�������ʽ��¼, ���䳤��ʽ���ƣ������ֶε�˳���ǰ��������鶨�����е� */
};

extern const char* getRecFormatStr(RecFormat format); 

// ��������Ԫ��
class LobPair {
public:
	LobPair(byte *lob, u32 lobSize)
		: m_lob(lob), m_size(lobSize){
	}
	byte		*m_lob;     /** ��������� */
	u32			m_size;		/** ������С*/
};

/** ��¼ */
class Record {
public:
	Record() {}
	Record(RowId rowId, RecFormat format, byte *data, uint size)
		: m_rowId(rowId), m_format(format), m_size(size), m_data(data) {
	}
	RowId		m_rowId;	/** ��¼ID */
	RecFormat	m_format;	/** ��¼���ݸ�ʽ */
	uint		m_size;		/** ���ݴ�С */
	byte		*m_data;	/** ���� */
};

/**
 * ���������鶨�����������˳����ѹ�������ʽ�ļ�¼
 *
 */
class CompressOrderRecord : public Record {
public:
	CompressOrderRecord() : Record(INVALID_ROW_ID, REC_COMPRESSORDER, NULL, 0), m_numSeg(0), m_segSizes(NULL) {}
	CompressOrderRecord(RowId rowId, byte *data, uint size, u8 numSeg, size_t *segSizes)
		: Record(rowId, REC_COMPRESSORDER, data, size), m_numSeg(numSeg), m_segSizes(segSizes) {
	}
	u8     m_numSeg;      /** ���������Ŀ */
	size_t *m_segSizes;   /** ����������εĳ��� */
};

/** �����б� */
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
	u16			m_size;		/** ������ */
	u16			*m_cols;	/** ���Ժ��б� */
};

/** ��¼��һ���� */
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

	RowId		m_rowId;	/** ��¼ID */
	RecFormat	m_format;	/** ��¼���ݸ�ʽ */
	u16			m_numCols;	/** ������ */
	/** �������ڱ��ֶ��б��е�λ�ã���0��ʼ��ţ�����ʽ��ΪKEY_XXX
	 * һ���ǵ����ģ�ΪKEY_XXXʱ���������������
	 */
	u16			*m_columns;
	uint		m_size;		/** ���ݴ�С */
	byte		*m_data;	/** ���� */
};

/** ѹ����¼��ȡ�� */
class CmprssRecordExtractor {
public:
	virtual ~CmprssRecordExtractor() {}
	/**
	* ѹ��һ����¼
	* @pre ��¼����ΪREC_COMPRESSORDER��ʽ
	* @post dest��m_id, m_data, m_size���ܱ��޸�
	* @param src ��ѹ���ļ�¼
	* @param dest OUT ѹ����ļ�¼, �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
	*   �Ѿ�������ڴ��С����ֹԽ��
	* @return ѹ����
	*/
	virtual double compressRecord(const CompressOrderRecord *src, Record *dest) = 0;

	/**
	* ��ѹ��һ����¼
	* @pre ��¼����ΪREC_COMPRESSED��ʽ
	* @post dest��m_id, m_data, m_size���ܱ��޸�
	* @param src ����ѹ�ļ�¼
	* @param dest OUT ��ѹ����ļ�¼, �����߱���Ϊ����������ݷ����㹻����ڴ棬����ͨ������dest.m_size��֪
	*   �Ѿ�������ڴ��С
	*/
	virtual void decompressRecord(const Record *src, CompressOrderRecord *dest) = 0;

	/** 
	* ѹ��һ��������
	* @param src ��ѹ���εĵ�ַ
	* @param offset ��ѹ���ε�ƫ����
	* @param len ��ѹ���εĳ���
	* @param dest OUT ���ѹ����ĶΣ����÷���֤���㹻�ڴ�
	* @param destSize OUT ���ѹ����εĳ���
	*/
	virtual void compressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) = 0;

	/** 
	* ��ѹ��һ��������
	* @param src ����ѹ���εĵ�ַ
	* @param offset ����ѹ���ε�ƫ����
	* @param len ����ѹ���εĳ���
	* @param dest OUT �����ѹ����ĶΣ����÷���֤���㹻�ڴ�
	* @param destSize OUT �����ѹ����εĳ���
	*/
	virtual void decompressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) = 0;
	
	/**
	 * ����ѹ����¼��ѹ����Ĵ�С
	 * @pre ��¼��ѹ����ʽ��
	 * @param cprsRcd Ҫ�����ѹ����¼
	 */
	virtual u64 calcRcdDecompressSize(const Record *cprsRcd) const = 0;

	/**
	* ����ѹ���������ѹ����Ĵ�С
	* @param src ����������
	* @param offset ���ݿ�ʼƫ����
	* @param len ���ݳ���
	*/
	virtual u64 calcColGrpDecompressSize(const byte *src, const uint& offset, const uint& len) const = 0;
};

class TableDef;
class MemoryContext;
class Session;
/** �Ӽ�¼����ȡ�Ӽ�¼����ȡ�� */
class SubrecExtractor {
public:
	static SubrecExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef, u16 numCols, const u16 *columns, 
		RecFormat srcFmt, RecFormat dstFmt, uint extractCount = (uint) -1);
	static SubrecExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef, u16 numCols, const u16* columns, 
		CmprssRecordExtractor *cprsRcdExtrator);
	static SubrecExtractor* createInst(Session *session, const TableDef *tableDef, const SubRecord *dstRec, 
		uint extractCount = (uint) -1, CmprssRecordExtractor *cprsRcdExtrator = NULL); 
	/** �Ӽ�¼����ȡ�Ӽ�¼
	 * ע: ʹ�ñ�������ȡ�Ӽ�¼���ܻᵼ��Ŀ���Ӽ�¼�зǱ���ȡ��¼������
	 *
	 * @param record ��¼
	 * @param subRecord �Ӽ�¼
	 */
	virtual void extract(const Record *record, SubRecord *subRecord) = 0;
	virtual ~SubrecExtractor() {}
};

/** ���Ӽ�¼����ȡ�Ӽ�¼����ȡ�� */
class SubToSubExtractor {
public:
	static SubToSubExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef,
		const IndexDef *indexDef, u16 numColsSrc, const u16 *columnsSrc, u16 numColsDst, 
		const u16 *columnsDst, RecFormat srcFmt, RecFormat dstFmt, uint extractCount = (uint) -1);
	static SubToSubExtractor* createInst(MemoryContext *ctx, const TableDef *tableDef, 
		const IndexDef *indexDef, u16 numColsSrc, const u16 *columnsSrc, u16 numColsDst,
		const u16 *columnsDst, RecFormat dstFmt, uint extractCount);
	/** ���Ӽ�¼����ȡ�Ӽ�¼
	 * ע: ʹ�ñ�������ȡ�Ӽ�¼���ܻᵼ��Ŀ���Ӽ�¼�зǱ���ȡ��¼������
	 *
	 * @param srSrc Դ�Ӽ�¼
	 * @param srDst Ŀ���Ӽ�¼
	 */
	virtual void extract(const SubRecord *srSrc, SubRecord *srDst) = 0;
	virtual ~SubToSubExtractor() {}
};

/** �Լ�¼���еĸ��ֲ��� */
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


	//���ݼ�¼ѹ��ģ������ļ�¼��ʽת���ӿ�
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
