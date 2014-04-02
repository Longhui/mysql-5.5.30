/**
 * NTSE B+��������ֵ������
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
 * ������Ҫ�����װ������ֵ��ѹ����ѹϸ�ڣ���ֵ�Ĵ洢��ʽ��������ݶ��������ڸ��൱��
 *
 * ��ֵ�ṹ��prefixLen(1~2bytes) + postfixLen(1~2bytes) + postfix(postfixLen bytes) + RowId(RID_BYTES) + PageId(PID_BYTES,���ܲ�����)
 *
 * ��ѹ����ֵ������ҳ���ʱ�򣬲��õĸ�ʽ�ǣ���ֵǰ׺���ȡ���ֵ��׺����(������ֵ���ݵĳ����Լ�RowId��PageId��������е��ܳ���)
 * ����ǰ��׺�ĳ��Ȱ�������UTF-8����ĸ�ʽ���������С��128����ô������һ���ֽڱ�ʾ��������ȴ���128����ô�������ֽڱ�ʾ��
 * ��һ���ֽڵ����λ��Ҫ��־Ϊ1����ʾ�ó�����Ҫ�����ֽڡ�����ܳ������ֵӦ����127 * 256 + 255���㹻��ʾ����������
 *
 * �ڽ�ѹ��ʱ�򣬸�������SubRecord�ĸ�ʽ�������м�ֵ��Ϣ(������ֵ��RowId��PageId���������)��˳�򶼱�����m_data�������'\0'��β
 * rowId����Ҫ���Ᵽ�浽m_rowId�����У�m_size��ֵ��ʾ����������ֵ����ʵ���ȣ�������rowId��pageId������
 *
 */

/**
 * ��ָ��ҳ��ƫ�ƴ����һ����¼�����ۼ�¼�Ƿ�ѹ��
 *
 * @param lastKey		��¼����ʼ��ַ
 * @param key			out	���������õļ�¼
 * @param isPageIdExist	�����Ƿ����PageId
 * @return ��ǰȡ�ü�¼��ռ�õ��ܳ���
 * @attention ʵ����lastKey��record�ڵ�m_data������ͬһ��ָ�룬���ʱ��Ҳ����ȷ����
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

	if (key->m_data != lastKey && preLength != 0 && lastKey != NULL) {	// �п���lastKeyʹ��record��ʾ������Ҫ����ǰ׺
		memcpy(key->m_data, lastKey, preLength);
	}
	memcpy(key->m_data + preLength, data, postLength);
	// ��֤����ĳ����Ǵ����ݵĳ��ȣ�data��β��'\0'�������������rowId�����Ѿ���ȡ
	key->m_size = preLength + postLength - getIDsLen(isPageIdExist);
	setKeyDataEndZero(key, isPageIdExist);
	key->m_rowId = RID_READ(getRowIdStart(key));

	return preLengthSize + postLengthSize + postLength;
}


/**
 * ר�������ڽ�ѹMiniPage��һ���ʱ����ʱ����Ϊ��¼û��ǰ׺ѹ����ʵ���ϲ���Ҫmemcpy��������
 * @pre �ⲿ���뱣��saveKey��m_dataָ��������
 * @post saveKey��m_dataָ�뱻ָ��IndexKey��������ʼλ��
 * @param saveKey		�����ȡ��ֵ��key
 * @param isPageIdExist	��ǰҳ�汣���Ƿ����pageId��Ϣ
 * @return ��ǰȡ�ü�¼��ռ�õ��ܳ���
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
	// ��֤����ĳ����Ǵ����ݵĳ��ȣ�data��β��'\0'�������������rowId�����Ѿ���ȡ
	saveKey->m_size = preLength + postLength - getIDsLen(isPageIdExist);
	saveKey->m_rowId = RID_READ(getRowIdStart(saveKey));

	return preLengthSize + postLengthSize + postLength;
}


/**
 * ��õ�ǰλ�ü�¼������PageId��Ϣ
 *
 * @pre	������Ӧ���ܹ���֤��ҳ���������ķ�Ҷҳ��
 * @param pageId	IN/OUT ���ػ��PageIdֵ
 * @return ��һ����ֵ�����ƫ��
 */
u16 IndexKey::extractNextPageId(PageId *pageId) {
	byte *key = (byte*)this;
	u16 offset = 0;
	u16 size;

	// ����ǰ׺����
	if (*key >= 128) {
		key++;
		offset++;
	}
	key++;
	offset += 2;
	// ���ݺ�׺���ȣ���ȡ������ֵ
	if ((size = *key++) >= 128) {
		size = ((size & 0x7f) << 8) | *key++;
		offset++;
	}

	*pageId = PID_READ(key + size - PID_BYTES);

	return offset + size;
}


/**
 * �������Ե�ǰ��ֵ
 * @return ��ǰ��ֵ�������һ����ֵ��ƫ��
 */
u16 IndexKey::skipAKey() {
	byte *key = (byte*)this;
	u16 offset = 0;
	u16 size;

	// ����ǰ׺����
	if (*key >= 128) {
		key++;
		offset++;
	}
	key++;
	offset += 2;
	// ���ݺ�׺���ȣ�����������ֵ
	if ((size = *key++) >= 128) {
		size = ((size & 0x7f) << 8) | *key++;
		offset++;
	}

	return offset + size;
}



/**
 * ����ָ����ǰһ��ֵ�͵�ǰ��ֵ�����浱ǰ��ֵ�ڵ�ǰλ��
 *
 * @param prevKey		ǰһ��ֵ����sizeΪ0����prevKeyΪNULL��ʾ������ǰ��
 * @param key			Ҫ����ĵ�ǰ��ֵ
 * @param isPageIdExist	key��data�����Ƿ����pageId��Ϣ
 * @return ���ؼ�ֵ�������λ��
 */
byte* IndexKey::compressAndSaveKey(const SubRecord *prevKey, const SubRecord *key, bool isPageIdExist) {
	assert(key != NULL);
	assert(key->m_format == KEY_COMPRESS && (prevKey == NULL || prevKey->m_format == KEY_COMPRESS));
	u16 prefixLen = prevKey != NULL ? computePrefix(prevKey, key) : 0;
	// �����׺���ȵ�ʱ��Ҫ����RowId��PageId����Ϣ
	return computePostfixAndSave(key, prefixLen, isPageIdExist);
}



/**
 * ����ָ����ǰһ��ֵ�͵�ǰ��ֵ�����浱ǰ��ֵ�ڵ�ǰλ��
 *
 * @param key			Ҫ����ĵ�ǰ��ֵ
 * @param prefixLen		�Ѿ���֪�ĸü�ֵѹ����ǰ׺����
 * @param isPageIdExist	key��data�����Ƿ����pageId��Ϣ
 * @return ���ؼ�ֵ�������λ��
 */
byte* IndexKey::compressAndSaveKey(const SubRecord *key, u16 prefixLen, bool isPageIdExist) {
	return computePostfixAndSave(key, prefixLen, isPageIdExist);
}


/**
 * ����Ҫ����ĳ����ֵ��Ҫ���ٿռ�
 *
 * @param prevKey		ǰһ����ֵ��NULL����sizeΪ0��ʾǰ�����
 * @param key			Ҫ����ļ�ֵ
 * @param isPageIdExist	��ֵ�Ƿ����PageId����
 * @return	��Ҫ�Ŀռ�
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
 * ����ָ����ֵ����ü�ֵ�ڵ�PageId��Ϣ
 *
 * @param key	��ֵ
 * @return ������PageId��Ϣ
 */
PageId IndexKey::getPageId(const SubRecord *key) {
	assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, true)) == '\0');
	return PID_READ(getPageIdStart(key));
}



/**
 * ��ָ��PageId��Ϣ��ӵ�key�ļ�ֵ��ʾ���У�����ԭ���ļ�ֵ�Ѿ���PageId��Ϣ���µ���Ϣ�Ḳ��ԭ����
 *
 * @param key		Ҫ��ʾ��key
 * @param pageId	Ҫ��ӵ�pageId
 */
void IndexKey::appendPageId(const SubRecord *key, PageId pageId) {
	byte *start = getPageIdStart(key);
	assert(*start == '\0' || *(start + PID_BYTES) == '\0');
	PID_WRITE(pageId, start);
	setKeyDataEndZero(key, true);
	assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, true)) == '\0');
}



/**
 * ����һ����ֵ����һ����ֵ
 *
 * @param dest			�����λ��
 * @param src			Ҫ�����ļ�ֵ
 * @param isPageIdExist	src�Ƿ����PageId
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
 * ��������������ֵָ��
 *
 * @param key1	IN/OUT	��ֵ1
 * @param key2	IN/OUT	��ֵ2
 * @return
 */
void IndexKey::swapKey(SubRecord **key1, SubRecord **key2) {
	SubRecord *temp = *key1;
	*key1 = *key2;
	*key2 = temp;
}




/**
 * �Ƚ�������ֵ�Ƿ���ȣ����Ƚ�rowId��pageId
 *
 * @param key1	��ֵ1
 * @param key2	��ֵ2
 * @return ���TRUE�������FALSE
 */
bool IndexKey::isKeyValueEqual(const SubRecord *key1, const SubRecord *key2) {
	if (key1->m_size != key2->m_size)
		return false;
	return !memcmp(key1->m_data, key2->m_data, key1->m_size);
}

/**
 * ����ָ�����ݳ�ʼ��һ���µ�SubRecord��ͬʱ����ָ����С����dataʹ�õ��ڴ�
 *
 * @param key		IN/OUT	��Ҫ��ʼ����SubRecord
 * @param format	SubRecord��ʽ
 * @param numCols	SubRecord�漰������
 * @param columns	SubRecord�����е����
 * @param size		SubRecord������ݳ���
 * @param rowId		SubReocrd��rowId
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
 * ����һ��REC_REDUNDANT��ʽ��Record���󷵻أ�data�ж����ռ�
 *
 * @Param memoryContext	�ڴ����������
 * @param size			�����ܳ���(����RowId)
 * @return record		�����record����
 */
Record* IndexKey::allocRecord(MemoryContext *memoryContext, uint size) {
	Record *record = (Record*)memoryContext->alloc(sizeof(Record));

	record->m_format = REC_REDUNDANT;
	record->m_data = (byte*)memoryContext->alloc(size);

	return record;
}




/**
 * ��ָ���ڴ�����������з���һ������ṹ��SubRecord�����ڴ洢��������
 * m_data����Ҫ��������ռ��Ұ���������ֵ����
 *
 * @param memoryContext	�ڴ����������
 * @param maxKeySize	��¼���ݳ���
 * @return ����õ�SubRecord
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
 * ���������������һ��SubRecord������ʹ��
 *
 * @param memoryContext	�ڴ����������
 * @param indexDef		��������
 * @param format		SubRecord�ĸ�ʽ
 * @return	���ط��������SubRecord
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
 * ����ָ��SubRecord��ֵ����һ��SubRecord������ʹ�ã�����rowId�Ƿ���ڣ�����data�б���
 * �����Ӽ�¼��columns��ֵ�ǽ���ָ�����Ӽ�¼��ֵ��columnsָ��
 *
 * @param memoryContext	�ڴ����������
 * @param key			ָ����SubRecord
 * @param indexDef		��������
 * @return	���ط��������SubRecord
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
 * ����ָ����record����һ��SubRecord
 *
 * @param memoryContext		�ڴ�������
 * @param keyNeedCompress	��ֵ�Ƿ���Ҫѹ��
 * @param record				ָ����record
 * @param lobArray			Record����ȡ����������������Ĵ�������
 * @param tableDef			������Ӧ����
 * @param indexDef			��������
 * @return ����õ�SubRecord
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
	} else {	// ���������ʽ��Ϣ
		key->m_format = KEY_PAD;
		RecordOper::extractKeyRP(tableDef, indexDef, record, lobArray, key);
		assert(key->m_size <= indexDef->m_maxKeySize);
	}

	// ����rowId��data
	RID_WRITE(key->m_rowId, getRowIdStart(key));
	setKeyDataEndZero(key, false);

	return key;
}


/**
 * ����ָ���ĸ�ʽ����һ��SubRecord
 * @param memoryContext		�ڴ����������
 * @param numCols			subrecord������
 * @param columns			subrecord���к�����
 * @param format			subrecord�ĸ�ʽ
 * @param maxKeySize		subrecord������ֵ����
 * @return ����õ�SubRecord
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
 * ��װRecord��������ֵѹ���Ĵ�����Ҫ���Ӷ�rowId��pageId�Ĵ���
 * @param tableDef			����
 * @param src				Ҫѹ���ļ�ֵ
 * @param dest				ѹ������ֵ�ı���������
 * @param isPageIdExist		pageId�Ƿ����
 */
void IndexKey::compressKey(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest, bool isPageIdExist) {
	RecordOper::compressKey(tableDef, indexDef, src, dest);

	// ��ʱdest->m_sizeֻ�����ݳ��ȣ�Ҫ����rowId��pageId
	dest->m_rowId = src->m_rowId;
	memcpy(dest->m_data + dest->m_size, src->m_data + src->m_size, getIDsLen(isPageIdExist));
	setKeyDataEndZero(dest, isPageIdExist);
}


/**
 * �������ʽ��ֵת��Ϊѹ����ʽ
 *
 * @param memoryContext	�ڴ�������
 * @param key			Ҫת���ļ�ֵ
 * @param lobArray		���������������������
 * @param tableDef		����
 * @param indexDef		��������
 * @return ѹ���õļ�ֵ
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
 * ��PAD��ʽ��ֵת��Ϊѹ����ʽ
 *
 * @param memoryContext	�ڴ�������
 * @param key			Ҫת���ļ�ֵ
 * @param tableDef		����
 * @param indexDef		��������
 * @return ѹ���õļ�ֵ
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
 * ��ָ��ѹ����ʽ��ֵת����Pad��ʽ
 *
 * @param key			Ҫת���ļ�ֵ
 * @param padKey		pad��ʽ��ֵ������Ϊ�գ����������丳ֵ����
 * @param tableDef		����
 * @param indexDef		��������
 * @param isPageIdExist	��ֵ�Ƿ����PageId��Ϣ
 * @return	����õ�PAD��ʽ��ֵ
 */
SubRecord* IndexKey::convertKeyCP(const SubRecord *key, SubRecord *padKey, const TableDef *tableDef, const IndexDef *indexDef, bool isPageIdExist) {
	assert(padKey != NULL);
	RecordOper::convertKeyCP(tableDef, indexDef, key, padKey);
	assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, isPageIdExist)) == '\0');
	memcpy(getRowIdStart(padKey), getRowIdStart(key), RID_BYTES);
	if (isPageIdExist)	// ��Ҫ����PageId��Ϣ
		memcpy(getPageIdStart(padKey), getPageIdStart(key), PID_BYTES);
	setKeyDataEndZero(padKey, isPageIdExist);
	padKey->m_rowId = key->m_rowId;
	return padKey;
}

///**
// * ���µ�ǰ��ֵ��pageId��Ϣ
// * @param keyLen	�ü�ֵ�ĳ�����Ϣ
// * @param newPageId	�µ�pageId��Ϣ
// * @return ����pageId����ʼƫ��
// */
//u16 IndexKey::updatePageId(u16 keyLen, PageId newPageId) {
//	byte *start = (byte*)this + keyLen - PID_BYTES;
//	PID_WRITE(newPageId, start);
//	return keyLen - PID_BYTES;
//}
//
///**
// * ��ȡ��ǰ��ֵ��pageId��Ϣ
// * @param keyLen	��ǰ��ֵ�ĳ���
// * @return pageId
// */
//PageId IndexKey::getCurPageId(u16 keyLen) {
//	byte *start = (byte*)this + keyLen - PID_BYTES;
//	return PID_READ(start);
//}


}

