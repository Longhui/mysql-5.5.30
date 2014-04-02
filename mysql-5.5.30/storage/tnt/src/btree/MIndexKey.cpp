/**
* �ڴ�������ֵ���
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#include "btree/MIndexKey.h"
#include <algorithm>

namespace tnt {

const u16 LeafPageKeyExt::PAGE_KEY_EXT_LEN = sizeof(LeafPageKeyExt);
const u16 InnerPageKeyExt::PAGE_KEY_EXT_LEN = sizeof(InnerPageKeyExt);


/**
 * �������ʽ�Ӽ�¼ת��ΪPAD��ʽ����������ʽ
 * @param memoryContext
 * @param key
 * @param tableDef
 * @param indexDef
 * @return 
 */
SubRecord* MIndexKeyOper::convertKeyRP(MemoryContext *memoryContext, const SubRecord *key, Array<LobPair*> *lobArray,
							   const TableDef *tableDef, const IndexDef *indexDef) {
	assert(key->m_format == REC_REDUNDANT);
	SubRecord *padKey = MIndexKeyOper::allocSubRecord(memoryContext, indexDef, KEY_PAD);
	Record record(key->m_rowId, REC_REDUNDANT, key->m_data, key->m_size);
	RecordOper::extractKeyRP(tableDef, indexDef, &record, lobArray, padKey);
	MIndexKeyOper::writeRowId(padKey, record.m_rowId);
	
	return padKey;
}


/**
 * �������ʽ�Ӽ�¼ת��Ϊ��Ȼ��ʽ����������ʽ
 * @param memoryContext
 * @param key
 * @param lobArray
 * @param tableDef
 * @param indexDef
 * @return 
 */
SubRecord* MIndexKeyOper::convertKeyRN(MemoryContext *memoryContext, const SubRecord *key, Array<LobPair*> *lobArray,
							   const TableDef *tableDef, const IndexDef *indexDef) {
	assert(key->m_format == REC_REDUNDANT);
	SubRecord *naturalKey = MIndexKeyOper::allocSubRecord(memoryContext, indexDef, KEY_NATURAL);
	Record record(key->m_rowId, REC_REDUNDANT, key->m_data, key->m_size);
	RecordOper::extractKeyRN(tableDef, indexDef, &record, lobArray, naturalKey);
	MIndexKeyOper::writeRowId(naturalKey, record.m_rowId);
	
	return naturalKey;
}

/**
 * ��PADʽ�Ӽ�¼ת��Ϊ��Ȼ��ʽ����������ʽ
 * @param memoryContext
 * @param key
 * @param tableDef
 * @param indexDef
 * @return 
 */
SubRecord* MIndexKeyOper::convertKeyPN(MemoryContext *memoryContext, const SubRecord *key,
							   const TableDef *tableDef, const IndexDef *indexDef) {
	assert(key->m_format == KEY_PAD);
	SubRecord *naturalKey = MIndexKeyOper::allocSubRecord(memoryContext, indexDef, KEY_NATURAL);
	RecordOper::convertKeyPN(tableDef, indexDef, key, naturalKey);
	MIndexKeyOper::writeRowId(naturalKey, key->m_rowId);
	
	return naturalKey;
}

/**
 * ������ֵ�Լ���չ��Ϣ
 * @pre ���÷���Ҫ��֤Ŀ�껺�����㹻�Ŀռ�
 * @param dst
 * @param src
 * @param isLeafPageKey
 * @return 
 */
void MIndexKeyOper::copyKeyAndExt(SubRecord *dst, const SubRecord *src, bool isLeafPageKey) {
	assert(KEY_NATURAL == dst->m_format);
	assert(KEY_NATURAL == src->m_format);
	dst->m_rowId = src->m_rowId;
	dst->m_size = src->m_size;
	dst->m_columns = src->m_columns;
	dst->m_numCols = src->m_numCols;
	u16 copyLen = calcKeyTotalLen((u16)src->m_size, isLeafPageKey);
	memcpy(dst->m_data, src->m_data, copyLen);
}

/**
 * ������ֵ
 * @pre ���÷���Ҫ��֤Ŀ�껺�����㹻�Ŀռ�
 * @param dst
 * @param src
 * @return 
 */
void MIndexKeyOper::copyKey(SubRecord *dst, const SubRecord *src) {
	assert(KEY_NATURAL == dst->m_format);
	assert(KEY_NATURAL == src->m_format);
	dst->m_rowId = src->m_rowId;
	dst->m_size = src->m_size;
	dst->m_columns = src->m_columns;
	dst->m_numCols = src->m_numCols;
	memcpy(dst->m_data, src->m_data, src->m_size);
}

/**
 * ���ݲ��ҷ���������Ƚ�������ֵ�Ĵ�С
 * @param key			Ҫ���ҵļ�ֵ
 * @param indexKey		�����϶�ȡ�ļ�ֵ
 * @param flag			�Ƚϱ��
 * @param comparator	������ֵ�Ƚ���
 * @return ����key��indexKey�Ĺ�ϵ��key���ڵ���С��indexKey���ֱ���1/0/-1��ʾ
 */
s32 MIndexKeyOper::compareKey(const SubRecord *key, const SubRecord *indexKey, const SearchFlag *flag, 
						   KeyComparator *comparator) {
	bool includeKey = flag->isIncludingKey();
	bool forward = flag->isForward();
	bool equalAllowable = flag->isEqualAllowed();

	if (unlikely(MIndexKeyOper::isInfiniteKey(key))) {
		return indexKey->m_rowId == key->m_rowId  ? 0 : 1;
	} else if (unlikely(MIndexKeyOper::isInfiniteKey(indexKey))) {
		return -1;
	}

	s32 result = comparator->compareKey(key, indexKey);

	/**************************************************************************************/
	/* ������		includeKey	forward		compare result: 1	0/0	(equalAllowable)	-1
		===================================================================================
		>=				1			1					|	1	-1/0					-1
		=				1			1					|	1	-1/0					-1
		>				0			1					|	1	1/0						-1
		<				0			0					|	1	-1/0					-1
		<=				1			0					|	1	1/0						-1
		===================================================================================
		���������ȣ����ڱȽϽ��Ϊ0��ֱ�ӷ���0
	*/
	/**************************************************************************************/
	return result != 0 ? result : equalAllowable ? 0 : (includeKey ^ forward) ? 1 : -1;
}

/**
 * ����ָ������ֵ��С�������ʽ�ڴ�������¼
 * @param memoryContext
 * @param maxKeySize
 * @return 
 */
SubRecord* MIndexKeyOper::allocSubRecordRED(MemoryContext *memoryContext, uint maxKeySize) {
	SubRecord *record = (SubRecord*)memoryContext->alloc(sizeof(SubRecord));
	record->m_data = (byte*)memoryContext->alloc(maxKeySize);
	record->m_size = maxKeySize;
	record->m_format = REC_REDUNDANT;
	memset(record->m_data, 0, maxKeySize);

	return record;
}

/**
 * ����ָ����ʽ���ڴ�������¼
 * @param memoryContext
 * @param indexDef
 * @param format
 * @return 
 */
SubRecord* MIndexKeyOper::allocSubRecord(MemoryContext *memoryContext, const IndexDef *indexDef, 
										 RecFormat format) {
	u16 maxSize = indexDef->m_maxKeySize + max(sizeof(InnerPageKeyExt), sizeof(LeafPageKeyExt));
	SubRecord *record = allocSubRecordRED(memoryContext, maxSize);
	record->m_format = format;
	record->m_numCols = indexDef->m_numCols;
	record->m_columns = indexDef->m_columns;

	return record;
}


/**
 * �����Ӽ�¼�����ڴ�������¼
 * @param memoryContext
 * @param key
 * @param indexDef
 * @return 
 */
SubRecord* MIndexKeyOper::allocSubRecord(MemoryContext *memoryContext, const SubRecord *key, 
										 const IndexDef *indexDef) {
	assert(key != NULL);
	u16 dataSize = max((u16)key->m_size, indexDef->m_maxKeySize);
	dataSize = calcKeyTotalLen(dataSize, false);
	SubRecord *newRecord = allocSubRecordRED(memoryContext, dataSize);
	newRecord->m_format = key->m_format;
	newRecord->m_numCols = key->m_numCols;
	newRecord->m_columns = key->m_columns;
	newRecord->m_size = key->m_size;
	newRecord->m_rowId = key->m_rowId;
	memcpy(newRecord->m_data, key->m_data, key->m_size);

	writeRowId(newRecord, newRecord->m_rowId);

	return newRecord;
}

/**
 * ����һ����¼�����ڴ�������¼
 * @param memoryContext
 * @param record
 * @param tableDef
 * @param indexDef
 * @return 
 */
SubRecord* MIndexKeyOper::allocSubRecord(MemoryContext *memoryContext, const Record *record, 
										 const TableDef *tableDef, const IndexDef *indexDef) {
	SubRecord *key = allocSubRecordRED(memoryContext, calcKeyTotalLen(
		tableDef->m_maxRecSize, false));
	key->m_columns = indexDef->m_columns;
	key->m_numCols = indexDef->m_numCols;
	key->m_rowId = record->m_rowId;

	// ���������ʽ��Ϣ
	key->m_size = record->m_size;
	memcpy(key->m_data, record->m_data, record->m_size);

	// ����rowId��data
	writeRowId(key, key->m_rowId);

	return key;
}

SubRecord* MIndexKeyOper::allocSubRecord(MemoryContext *memoryContext, u16 numCols, u16 *columns, 
										 RecFormat format, u16 maxKeySize) {
	SubRecord *record = (SubRecord*)memoryContext->alloc(sizeof(SubRecord));
	record->m_data = (byte*)memoryContext->calloc(calcKeyTotalLen(maxKeySize, false));
	record->m_columns = (u16*)memoryContext->alloc(numCols * sizeof(u16));
	record->m_size = maxKeySize;
	record->m_format = format;
	record->m_numCols = numCols;
	memcpy(record->m_columns, columns, numCols * sizeof(u16));

	return record;
}

/**
 * �������޴�������
 * @param memoryContext
 * @param indexDef
 * @return 
 */
SubRecord *MIndexKeyOper::createInfiniteKey(MemoryContext *memoryContext, const IndexDef *indexDef) {
	SubRecord *key = allocSubRecordRED(memoryContext, calcKeyTotalLen(0, false));
	key->m_format = KEY_NATURAL;
	key->m_columns = indexDef->m_columns;
	key->m_numCols = indexDef->m_numCols;

	key->m_rowId = INVALID_ROW_ID;
	key->m_size = 0;
	writeRowId(key, key->m_rowId);

	return key;
}

}