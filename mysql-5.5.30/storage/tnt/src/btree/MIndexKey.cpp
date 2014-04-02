/**
* 内存索引键值相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "btree/MIndexKey.h"
#include <algorithm>

namespace tnt {

const u16 LeafPageKeyExt::PAGE_KEY_EXT_LEN = sizeof(LeafPageKeyExt);
const u16 InnerPageKeyExt::PAGE_KEY_EXT_LEN = sizeof(InnerPageKeyExt);


/**
 * 将冗余格式子记录转化为PAD格式的索引键格式
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
 * 将冗余格式子记录转化为自然格式的索引键格式
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
 * 将PAD式子记录转化为自然格式的索引键格式
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
 * 拷贝键值以及扩展信息
 * @pre 调用方需要保证目标缓存有足够的空间
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
 * 拷贝键值
 * @pre 调用方需要保证目标缓存有足够的空间
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
 * 根据查找方向和条件比较两个键值的大小
 * @param key			要查找的键值
 * @param indexKey		索引上读取的键值
 * @param flag			比较标记
 * @param comparator	索引键值比较器
 * @return 返回key和indexKey的关系，key大于等于小于indexKey，分别用1/0/-1表示
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
	/* 操作符		includeKey	forward		compare result: 1	0/0	(equalAllowable)	-1
		===================================================================================
		>=				1			1					|	1	-1/0					-1
		=				1			1					|	1	-1/0					-1
		>				0			1					|	1	1/0						-1
		<				0			0					|	1	-1/0					-1
		<=				1			0					|	1	1/0						-1
		===================================================================================
		如果允许相等，对于比较结果为0，直接返回0
	*/
	/**************************************************************************************/
	return result != 0 ? result : equalAllowable ? 0 : (includeKey ^ forward) ? 1 : -1;
}

/**
 * 生成指定最大键值大小的冗余格式内存索引记录
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
 * 生成指定格式的内存索引记录
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
 * 根据子记录生成内存索引记录
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
 * 根据一条记录生成内存索引记录
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

	// 保留冗余格式信息
	key->m_size = record->m_size;
	memcpy(key->m_data, record->m_data, record->m_size);

	// 拷贝rowId到data
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
 * 生成无限大索引键
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