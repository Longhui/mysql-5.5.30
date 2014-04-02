/**
 * NTSE B+树索引键值管理类
 *
 * author: naturally (naturally@163.org)
 */

#include "btree/IndexKey.h"
#include "misc/Record.h"
#include "assert.h"
#include "misc/Session.h"
#include "api/Table.h"

namespace ntse {


/**
 * 该类主要负责封装索引键值的压缩解压细节，键值的存储格式等相关内容都被隐藏在该类当中
 *
 * 键值结构：prefixLen(1~2bytes) + postfixLen(1~2bytes) + postfix(postfixLen bytes) + RowId(RID_BYTES) + PageId(PID_BYTES,可能不存在)
 *
 * 在压缩键值到索引页面的时候，采用的格式是：键值前缀长度、键值后缀长度(包括键值数据的长度以及RowId和PageId——如果有的总长度)
 * 其中前后缀的长度按照类似UTF-8编码的格式，如果长度小于128，那么可以用一个字节表示，如果长度大于128，那么用两个字节表示，
 * 第一个字节的最高位需要标志为1，表示该长度需要两个字节。最后总长度最大值应该是127 * 256 + 255，足够表示索引键长度
 *
 * 在解压的时候，根据现有SubRecord的格式，将所有键值信息(索引键值，RowId和PageId——如果有)按顺序都保存在m_data，最后以'\0'结尾
 * rowId还需要另外保存到m_rowId属性中，m_size的值表示的是索引键值的真实长度，不包括rowId和pageId的内容
 *
 */

/**
 * 从指定页面偏移处获得一条记录，无论记录是否压缩
 *
 * @param lastKey		记录的起始地址
 * @param key			out	用来保存获得的记录
 * @param isPageIdExist	数据是否包含PageId
 * @return 当前取得记录所占用的总长度
 * @attention 实际上lastKey和record内的m_data可能是同一个指针，这个时候也能正确处理
 */
u16 IndexKey::extractKey(const byte *lastKey, SubRecord *key, bool isPageIdExist) {
	u16 preLength = 0;
	u16 postLength = 0;
	u16 preLengthSize = 1;
	u16 postLengthSize = 1;
	byte *data = (byte*)this;

	if ((preLength = *data++) >= 128) {
		preLength = ((preLength & 0x7f) << 8) | *data++;
		preLengthSize++;
	}

	if ((postLength = *data++) >= 128) {
		postLength = ((postLength & 0x7f) << 8) | *data++;
		postLengthSize++;
	}

	if (key->m_data != lastKey && preLength != 0 && lastKey != NULL) {	// 有可能lastKey使用record表示，不需要拷贝前缀
		memcpy(key->m_data, lastKey, preLength);
	}
	memcpy(key->m_data + preLength, data, postLength);
	// 保证这里的长度是纯数据的长度，data结尾以'\0'标记真正结束，rowId必须已经读取
	key->m_size = preLength + postLength - getIDsLen(isPageIdExist);
	setKeyDataEndZero(key, isPageIdExist);
	key->m_rowId = RID_READ(getRowIdStart(key));

	return preLengthSize + postLengthSize + postLength;
}


/**
 * 专门用于在解压MiniPage第一项的时候，这时候因为记录没有前缀压缩，实际上不需要memcpy拷贝出来
 * @pre 外部必须保存saveKey的m_data指针做备份
 * @post saveKey的m_data指针被指向IndexKey的数据起始位置
 * @param saveKey		保存读取键值的key
 * @param isPageIdExist	当前页面保存是否包含pageId信息
 * @return 当前取得记录所占用的总长度
 */
u16 IndexKey::fakeExtractMPFirstKey(SubRecord *saveKey, bool isPageIdExist) {
	u16 preLength = 0;
	u16 postLength = 0;
	u16 preLengthSize = 1;
	u16 postLengthSize = 1;
	byte *key = (byte*)this;

	if ((preLength = *key++) >= 128) {
		preLength = ((preLength & 0x7f) << 8) | *key++;
		preLengthSize++;
	}

	if ((postLength = *key++) >= 128) {
		postLength = ((postLength & 0x7f) << 8) | *key++;
		postLengthSize++;
	}

	saveKey->m_data = key;
	// 保证这里的长度是纯数据的长度，data结尾以'\0'标记真正结束，rowId必须已经读取
	saveKey->m_size = preLength + postLength - getIDsLen(isPageIdExist);
	saveKey->m_rowId = RID_READ(getRowIdStart(saveKey));

	return preLengthSize + postLengthSize + postLength;
}


/**
 * 获得当前位置记录包含的PageId信息
 *
 * @pre	调用者应该能够保证该页面是索引的非叶页面
 * @param pageId	IN/OUT 返回获得PageId值
 * @return 下一个键值的相对偏移
 */
u16 IndexKey::extractNextPageId(PageId *pageId) {
	byte *key = (byte*)this;
	u16 offset = 0;
	u16 size;

	// 跳过前缀长度
	if (*key >= 128) {
		key++;
		offset++;
	}
	key++;
	offset += 2;
	// 根据后缀长度，读取具体数值
	if ((size = *key++) >= 128) {
		size = ((size & 0x7f) << 8) | *key++;
		offset++;
	}

	*pageId = PID_READ(key + size - PID_BYTES);

	return offset + size;
}


/**
 * 遍历忽略当前键值
 * @return 当前键值相对于上一个键值的偏移
 */
u16 IndexKey::skipAKey() {
	byte *key = (byte*)this;
	u16 offset = 0;
	u16 size;

	// 跳过前缀长度
	if (*key >= 128) {
		key++;
		offset++;
	}
	key++;
	offset += 2;
	// 根据后缀长度，忽略整个键值
	if ((size = *key++) >= 128) {
		size = ((size & 0x7f) << 8) | *key++;
		offset++;
	}

	return offset + size;
}



/**
 * 根据指定的前一键值和当前键值，保存当前键值在当前位置
 *
 * @param prevKey		前一键值，若size为0或者prevKey为NULL表示不存在前项
 * @param key			要插入的当前键值
 * @param isPageIdExist	key的data里面是否包含pageId信息
 * @return 返回键值插入结束位置
 */
byte* IndexKey::compressAndSaveKey(const SubRecord *prevKey, const SubRecord *key, bool isPageIdExist) {
	assert(key != NULL);
	assert(key->m_format == KEY_COMPRESS && (prevKey == NULL || prevKey->m_format == KEY_COMPRESS));
	u16 prefixLen = prevKey != NULL ? computePrefix(prevKey, key) : 0;
	// 计算后缀长度的时候要包含RowId和PageId的信息
	return computePostfixAndSave(key, prefixLen, isPageIdExist);
}



/**
 * 根据指定的前一键值和当前键值，保存当前键值在当前位置
 *
 * @param key			要插入的当前键值
 * @param prefixLen		已经得知的该键值压缩的前缀长度
 * @param isPageIdExist	key的data里面是否包含pageId信息
 * @return 返回键值插入结束位置
 */
byte* IndexKey::compressAndSaveKey(const SubRecord *key, u16 prefixLen, bool isPageIdExist) {
	return computePostfixAndSave(key, prefixLen, isPageIdExist);
}


/**
 * 计算要插入某个键值需要多少空间
 *
 * @param prevKey		前一个键值，NULL或者size为0表示前项不存在
 * @param key			要插入的键值
 * @param isPageIdExist	键值是否包含PageId内容
 * @return	需要的空间
 */
u16 IndexKey::computeSpace(const SubRecord *prevKey, const SubRecord *key, bool isPageIdExist) {
	assert(key != NULL);
	assert(key->m_format == KEY_COMPRESS && (prevKey == NULL || prevKey->m_format == KEY_COMPRESS));
	u16 prefixLen;
	if (prevKey == NULL || !IndexKey::isKeyValid(prevKey))
		prefixLen = 0;
	else
		prefixLen = computePrefix(prevKey, key);

	return computeSpace(key, prefixLen, isPageIdExist);
}



/**
 * 根据指定键值，获得键值内的PageId信息
 *
 * @param key	键值
 * @return 包含的PageId信息
 */
PageId IndexKey::getPageId(const SubRecord *key) {
	assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, true)) == '\0');
	return PID_READ(getPageIdStart(key));
}



/**
 * 将指定PageId信息添加到key的键值表示当中，可能原来的键值已经有PageId信息，新的信息会覆盖原来的
 *
 * @param key		要表示的key
 * @param pageId	要添加的pageId
 */
void IndexKey::appendPageId(const SubRecord *key, PageId pageId) {
	byte *start = getPageIdStart(key);
	assert(*start == '\0' || *(start + PID_BYTES) == '\0');
	PID_WRITE(pageId, start);
	setKeyDataEndZero(key, true);
	assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, true)) == '\0');
}



/**
 * 拷贝一个键值到另一个键值
 *
 * @param dest			保存的位置
 * @param src			要拷贝的键值
 * @param isPageIdExist	src是否包含PageId
 */
void IndexKey::copyKey(SubRecord *dest, const SubRecord *src, bool isPageIdExist) {
	assert(src->m_numCols == dest->m_numCols);
	assert(memcmp(src->m_columns, dest->m_columns, sizeof(u16) * src->m_numCols) == 0);
	assert(src->m_format == dest->m_format);

	dest->m_rowId = src->m_rowId;
	dest->m_size = src->m_size;
	byte *start = dest->m_data;
	u16 copySize = getKeyTotalLength((u16)src->m_size, isPageIdExist);

	memcpy(start, src->m_data, copySize);
	setKeyDataEndZero(dest, isPageIdExist);
}



/**
 * 交换两个索引键值指针
 *
 * @param key1	IN/OUT	键值1
 * @param key2	IN/OUT	键值2
 * @return
 */
void IndexKey::swapKey(SubRecord **key1, SubRecord **key2) {
	SubRecord *temp = *key1;
	*key1 = *key2;
	*key2 = temp;
}




/**
 * 比较两个键值是否相等，不比较rowId和pageId
 *
 * @param key1	键值1
 * @param key2	键值2
 * @return 相等TRUE，不相等FALSE
 */
bool IndexKey::isKeyValueEqual(const SubRecord *key1, const SubRecord *key2) {
	if (key1->m_size != key2->m_size)
		return false;
	return !memcmp(key1->m_data, key2->m_data, key1->m_size);
}

/**
 * 根据指定内容初始化一个新的SubRecord，同时根据指定大小分配data使用的内存
 *
 * @param key		IN/OUT	需要初始化的SubRecord
 * @param format	SubRecord格式
 * @param numCols	SubRecord涉及的列数
 * @param columns	SubRecord各个列的序号
 * @param size		SubRecord最大数据长度
 * @param rowId		SubReocrd的rowId
 */
void IndexKey::initialKey(SubRecord *key, RecFormat format, u16 numCols, u16 *columns, uint size, RowId rowId) {
	key->m_format = format;
	key->m_numCols = numCols;
	key->m_columns = columns;
	key->m_rowId = rowId;
	key->m_size = size;
	key->m_data = new byte[size];
	memset(key->m_data, 0, size);
}



/**
 * 构造一个REC_REDUNDANT格式的Record对象返回，data有独立空间
 *
 * @Param memoryContext	内存分配上下文
 * @param size			数据总长度(包括RowId)
 * @return record		构造的record对象
 */
Record* IndexKey::allocRecord(MemoryContext *memoryContext, uint size) {
	Record *record = (Record*)memoryContext->alloc(sizeof(Record));

	record->m_format = REC_REDUNDANT;
	record->m_data = (byte*)memoryContext->alloc(size);

	return record;
}




/**
 * 在指定内存分配上下文中分配一个冗余结构的SubRecord，用于存储数据内容
 * m_data都需要独立分配空间且按照最大可能值分配
 *
 * @param memoryContext	内存分配上下文
 * @param maxKeySize	记录数据长度
 * @return 构造好的SubRecord
 */
SubRecord* IndexKey::allocSubRecordRED(MemoryContext *memoryContext, uint maxKeySize) {
	SubRecord *record = (SubRecord*)memoryContext->alloc(sizeof(SubRecord));
	record->m_data = (byte*)memoryContext->alloc(maxKeySize);
	record->m_size = maxKeySize;
	record->m_format = REC_REDUNDANT;
	memset(record->m_data, 0, maxKeySize);

	return record;
}



/**
 * 根据索引定义分配一个SubRecord供索引使用
 *
 * @param memoryContext	内存分配上下文
 * @param indexDef		索引定义
 * @param format		SubRecord的格式
 * @return	返回分配出来的SubRecord
 */
SubRecord* IndexKey::allocSubRecord(MemoryContext *memoryContext, const IndexDef *indexDef, RecFormat format) {
	u16 size = getKeyTotalLength(indexDef->m_maxKeySize, true) + 1;
	SubRecord *record = allocSubRecordRED(memoryContext, size);
	record->m_format = format;
	record->m_numCols = indexDef->m_numCols;
	record->m_columns = indexDef->m_columns;

	return record;
}


/**
 * 根据指定SubRecord键值分配一个SubRecord供索引使用，无论rowId是否存在，都在data中保存
 * 返回子记录的columns的值是借用指定的子记录键值的columns指针
 *
 * @param memoryContext	内存分配上下文
 * @param key			指定的SubRecord
 * @param indexDef		索引定义
 * @return	返回分配出来的SubRecord
 */
SubRecord* IndexKey::allocSubRecord(MemoryContext *memoryContext, const SubRecord *key, const IndexDef *indexDef) {
	assert(key != NULL);
	u16 dataSize = (u16)key->m_size > indexDef->m_maxKeySize ?
		(u16)key->m_size : indexDef->m_maxKeySize;
	dataSize = getKeyTotalLength(dataSize, true) + 1;
	SubRecord *newRecord = allocSubRecordRED(memoryContext, dataSize);
	newRecord->m_format = key->m_format;
	newRecord->m_numCols = key->m_numCols;
	newRecord->m_columns = key->m_columns;
	newRecord->m_size = key->m_size;
	newRecord->m_rowId = key->m_rowId;
	memcpy(newRecord->m_data, key->m_data, key->m_size);

	RID_WRITE(key->m_rowId, getRowIdStart(newRecord));
	setKeyDataEndZero(newRecord, false);

	return newRecord;
}




/**
 * 根据指定的record构造一个SubRecord
 *
 * @param memoryContext		内存上下文
 * @param keyNeedCompress	键值是否需要压缩
 * @param record				指定的record
 * @param lobArray			Record中提取出构造索引键所需的大对象队列
 * @param tableDef			索引对应表定义
 * @param indexDef			索引定义
 * @return 构造好的SubRecord
 */
SubRecord* IndexKey::allocSubRecord(MemoryContext *memoryContext, bool keyNeedCompress, const Record *record, 
									Array<LobPair*> *lobArray, const TableDef *tableDef, const IndexDef *indexDef) {
	u16 maxKeySize = indexDef->m_maxKeySize;
	SubRecord *key = allocSubRecordRED(memoryContext, getKeyTotalLength(maxKeySize, true) + 1);
	key->m_columns = indexDef->m_columns;
	key->m_numCols = indexDef->m_numCols;
	key->m_rowId = record->m_rowId;

	if (keyNeedCompress) {
		key->m_format = KEY_COMPRESS;
		RecordOper::extractKeyRC(tableDef, indexDef, record, lobArray, key);
		assert(key->m_size <= indexDef->m_maxKeySize);
	} else {	// 保留冗余格式信息
		key->m_format = KEY_PAD;
		RecordOper::extractKeyRP(tableDef, indexDef, record, lobArray, key);
		assert(key->m_size <= indexDef->m_maxKeySize);
	}

	// 拷贝rowId到data
	RID_WRITE(key->m_rowId, getRowIdStart(key));
	setKeyDataEndZero(key, false);

	return key;
}


/**
 * 根据指定的格式创建一个SubRecord
 * @param memoryContext		内存分配上下文
 * @param numCols			subrecord的列数
 * @param columns			subrecord的列号数组
 * @param format			subrecord的格式
 * @param maxKeySize		subrecord的最大键值长度
 * @return 构造好的SubRecord
 */
SubRecord* IndexKey::allocSubRecord(MemoryContext *memoryContext, u16 numCols, u16 *columns, RecFormat format, u16 maxKeySize) {
	SubRecord *record = (SubRecord*)memoryContext->alloc(sizeof(SubRecord));
	record->m_data = (byte*)memoryContext->alloc(getKeyTotalLength(maxKeySize, true) + 1);
	record->m_columns = (u16*)memoryContext->alloc(numCols * sizeof(u16));
	record->m_size = maxKeySize;
	record->m_format = format;
	record->m_numCols = numCols;
	memset(record->m_data, 0, maxKeySize);
	memcpy(record->m_columns, columns, numCols * sizeof(u16));

	return record;
}


/**
 * 封装Record对索引键值压缩的处理，主要附加对rowId和pageId的处理
 * @param tableDef			表定义
 * @param src				要压缩的键值
 * @param dest				压缩过键值的保存在这里
 * @param isPageIdExist		pageId是否存在
 */
void IndexKey::compressKey(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest, bool isPageIdExist) {
	RecordOper::compressKey(tableDef, indexDef, src, dest);

	// 此时dest->m_size只是数据长度，要处理rowId和pageId
	dest->m_rowId = src->m_rowId;
	memcpy(dest->m_data + dest->m_size, src->m_data + src->m_size, getIDsLen(isPageIdExist));
	setKeyDataEndZero(dest, isPageIdExist);
}


/**
 * 将冗余格式键值转换为压缩格式
 *
 * @param memoryContext	内存上下文
 * @param key			要转换的键值
 * @param lobArray		构造索引键所需大对象队列
 * @param tableDef		表定义
 * @param indexDef		索引定义
 * @return 压缩好的键值
 */
SubRecord* IndexKey::convertKeyRC(MemoryContext *memoryContext, const SubRecord *key, Array<LobPair*> *lobArray, const TableDef *tableDef, const IndexDef *indexDef) {
	assert(key->m_format == REC_REDUNDANT);
	SubRecord *compressKey = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_COMPRESS);
	Record record;
	record.m_format = REC_REDUNDANT;
	record.m_rowId = key->m_rowId;
	record.m_size = key->m_size;
	record.m_data = key->m_data;
	RecordOper::extractKeyRC(tableDef, indexDef, &record, lobArray, compressKey);
	RID_WRITE(key->m_rowId, getRowIdStart(compressKey));
	setKeyDataEndZero(compressKey, false);

	return compressKey;
}

/**
 * 将PAD格式键值转换为压缩格式
 *
 * @param memoryContext	内存上下文
 * @param key			要转换的键值
 * @param tableDef		表定义
 * @param indexDef		索引定义
 * @return 压缩好的键值
 */
SubRecord* IndexKey::convertKeyPC(MemoryContext *memoryContext, const SubRecord *key, const TableDef *tableDef, const IndexDef *indexDef) {
	assert(key->m_format == KEY_PAD);
	SubRecord *compressKey = IndexKey::allocSubRecord(memoryContext, indexDef, KEY_COMPRESS);
	RecordOper::convertKeyPC(tableDef, indexDef, key, compressKey);
	RID_WRITE(key->m_rowId, getRowIdStart(compressKey));
	setKeyDataEndZero(compressKey, false);

	return compressKey;
}


/**
 * 将指定压缩格式键值转换成Pad格式
 *
 * @param key			要转换的键值
 * @param padKey		pad格式键值，可能为空，本函数分配赋值返回
 * @param tableDef		表定义
 * @param indexDef		索引定义
 * @param isPageIdExist	键值是否包含PageId信息
 * @return	构造好的PAD格式键值
 */
SubRecord* IndexKey::convertKeyCP(const SubRecord *key, SubRecord *padKey, const TableDef *tableDef, const IndexDef *indexDef, bool isPageIdExist) {
	assert(padKey != NULL);
	RecordOper::convertKeyCP(tableDef, indexDef, key, padKey);
	assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, isPageIdExist)) == '\0');
	memcpy(getRowIdStart(padKey), getRowIdStart(key), RID_BYTES);
	if (isPageIdExist)	// 需要拷贝PageId信息
		memcpy(getPageIdStart(padKey), getPageIdStart(key), PID_BYTES);
	setKeyDataEndZero(padKey, isPageIdExist);
	padKey->m_rowId = key->m_rowId;
	return padKey;
}

///**
// * 更新当前键值的pageId信息
// * @param keyLen	该键值的长度信息
// * @param newPageId	新的pageId信息
// * @return 更新pageId的起始偏移
// */
//u16 IndexKey::updatePageId(u16 keyLen, PageId newPageId) {
//	byte *start = (byte*)this + keyLen - PID_BYTES;
//	PID_WRITE(newPageId, start);
//	return keyLen - PID_BYTES;
//}
//
///**
// * 读取当前键值的pageId信息
// * @param keyLen	当前键值的长度
// * @return pageId
// */
//PageId IndexKey::getCurPageId(u16 keyLen) {
//	byte *start = (byte*)this + keyLen - PID_BYTES;
//	return PID_READ(start);
//}


}

