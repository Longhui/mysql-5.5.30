/**
 * NTSE B+树索引键值管理类
 * 
 * author: naturally (naturally@163.org)
 */

#ifndef _NTSE_INDEX_KEY_H_
#define _NTSE_INDEX_KEY_H_

#include "misc/Global.h"
#include "string.h"
#include "misc/Record.h"
#include <assert.h>

namespace ntse {

class SubRecord;
class MemoryContext;
class Record;
class TableDef;
class IndexDef;
class LobStorage;

#define PID_BYTES RID_BYTES

/** 读取一个PAGEID，RID占用PID_BYTES个字节 */
inline RowId PID_READ(byte *buf) {
	return RID_READ(buf);
}

/** 写入一个PAGE，PAGEID占用PID_BYTES个字节 */
inline void PID_WRITE(PageId rid, byte *buf) {
	RID_WRITE(rid, buf);
}


/**
 * 处理索引键的压缩存储与读取，具体约束条件见各函数说明
 * 类本身指向索引键的起始位置，类所用空间必须使用者已经分配调整好
 */
class IndexKey {
public:
	// 读取/保存键值内容
	u16 extractKey(const byte *lastKey, SubRecord *key, bool isPageIdExist);
	u16 fakeExtractMPFirstKey(SubRecord *saveKey, bool isPageIdExist);
	u16 extractNextPageId(PageId *pageId);
	byte* compressAndSaveKey(const SubRecord *prevKey, const SubRecord *nextKey, bool isPageIdExist);
	byte* compressAndSaveKey(const SubRecord *key, u16 prefixLen, bool isPageIdExist);
	u16 skipAKey();
	//u16 updatePageId(u16 keyLen, PageId newPageId);
	//PageId getCurPageId(u16 keyLen);

	static u16 computeSpace(const SubRecord *prevKey, const SubRecord *key, bool isPageIdExist);
	static PageId getPageId(const SubRecord *key);
	static void appendPageId(const SubRecord *key, PageId pageId);
	static void copyKey(SubRecord *dest, const SubRecord *src, bool isPageIdExist);
	static void swapKey(SubRecord **key1, SubRecord **key2);
	static bool isKeyValueEqual(const SubRecord *key1, const SubRecord *key2);
	static void initialKey(SubRecord *key, RecFormat format, u16 numCols, u16 *columns, uint size, RowId rowId);
	static void compressKey(const TableDef *tableDef, const IndexDef *indexDef, const SubRecord *src, SubRecord *dest, bool isPageIdExist);
	static SubRecord* convertKeyRC(MemoryContext *memoryContext, const SubRecord *key, Array<LobPair*> *lobArray, const TableDef *tableDef, const IndexDef *indexDef);
	static SubRecord* convertKeyPC(MemoryContext *memoryContext, const SubRecord *key, const TableDef *tableDef, const IndexDef *indexDef);
	static SubRecord* convertKeyCP(const SubRecord *key, SubRecord *padKey, const TableDef *tableDef, const IndexDef *indexDef, bool isPageIdExist);

	// 根据SubReocrd保存索引键值使用情况，分配SubRecord
	static Record* allocRecord(MemoryContext *memoryContext, uint size);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, const IndexDef *indexDef, RecFormat format);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, const SubRecord *key, const IndexDef *indexDef);
	static SubRecord* allocSubRecordRED(MemoryContext *memoryContext, uint maxKeySize);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, bool keyNeedCompress, const Record *record, Array<LobPair*> *lobArray, 
		const TableDef *tableDef, const IndexDef *indexDef);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, u16 numCols, u16 *columns, RecFormat format, u16 maxKeySize);

	/**
	 * 计算两个键值的前缀长度
	 *
	 * @param prevKey	键值1
	 * @param nextKey	键值2
	 * @return 共同前缀长度
	 */
	static u16 computePrefix(const SubRecord *prevKey, const SubRecord *nextKey) {
		uint size = MIN(prevKey->m_size, nextKey->m_size);
		byte *data1 = prevKey->m_data;
		byte *data2 = nextKey->m_data;
		u16 i = 0;

		for (; i < size; i++)
			if (*data1++ != *data2++)
				return i;

		return i;
	}

	/**
	 * 计算插入某个键值所需空间
	 *
	 * @param key	要插入的键值
	 * @param prefixLen	插入键值的前缀可压缩长度
	 * @isPageIdExist	键值是否包含PageId信息
	 * @return 键值需要空间大小
	 */
	static u16 computeSpace(const SubRecord *key, u16 prefixLen, bool isPageIdExist) {
		u16 postfixLen = getKeyTotalLength((u16)key->m_size, isPageIdExist) - prefixLen;
		return (prefixLen >= 128 ? 2 : 1) + (postfixLen >= 128 ? 2 : 1) + postfixLen;
	}

	/**
	 * 判断一个键值是否有效，主要看键值是不是为空
	 * @param key 要判断的键值
	 * @return 键值不为NULL且长度不为0表示有效返回true，否则false
	 */
	static bool isKeyValid(const SubRecord *key) {
		return key != NULL && key->m_size != 0;
	}

private:
	/**
	 * 将某条记录的前后缀、数据信息保存到当前地址
	 *
	 * @param prefixLen		前缀长度所占字节
	 * @param postfixLen	后缀长度所占字节
	 * @param data		数据信息
	 * @return 保存信息之后的地址
	 */
	inline byte* saveKeyInfo(u16 prefixLen, u16 postfixLen, const byte *data) {
		assert(postfixLen >= RID_BYTES);
		//assert(*(data + prefixLen + postfixLen) == '\0');
		byte *start = (byte*)this;

		if (prefixLen >= 128)
			*start++ = (u8)((prefixLen >> 8) | 0x80);
		*start++ = (u8)prefixLen;
		if (postfixLen >= 128)
			*start++ = (u8)((postfixLen >> 8) | 0x80);
		*start++ = (u8)postfixLen;

		memcpy(start, data + prefixLen, postfixLen);

		return start + postfixLen;
	}


	/**
	 * 根据指定的前一键值和当前键值，保存当前键值在当前位置
	 *
	 * @param key		要插入的当前键值
	 * @param prefixLen	已经得知的该键值压缩的前缀长度
	 * @param isPageIdExist	key的data里面是否包含pageId信息
	 * @return 返回插入结束位置
	 */
	inline byte* computePostfixAndSave(const SubRecord *key, u16 prefixLen, bool isPageIdExist) {
		assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, isPageIdExist)) == '\0');
		u16 postfixLen = getKeyTotalLength((u16)key->m_size, isPageIdExist) - prefixLen;
		// 压缩保存后项
		return saveKeyInfo(prefixLen, postfixLen, key->m_data);
	}

	/** 返回键值长度加上ID信息之后的最大长度，这个值保证该键值可以存储所有ID信息，以及最后的\0 */
	inline static u16 getKeyTotalLength(u16 keyLength, bool isPageIdExist) {
		return keyLength + getIDsLen(isPageIdExist);
	}

	/** 根据键值是否需要包含PageId信息，返回键值保存所需要ID信息的准确空间长度 */
	inline static u16 getIDsLen(bool isPageIdExist) {
		return isPageIdExist ? RID_BYTES + PID_BYTES : RID_BYTES;
	}

	/** 返回某个键值的PageId存储的起始地址，该键值必须有保存PageId */
	inline static byte* getPageIdStart(const SubRecord *key) {
		return key->m_data + key->m_size + RID_BYTES;
	}

	/** 返回某个键值的RowId存储的起始地址 */
	inline static byte* getRowIdStart(const SubRecord *key) {
		return key->m_data + key->m_size;
	}

	/** 将键值数据的结束位下一位设置为0 */
	inline static void setKeyDataEndZero(const SubRecord *key, bool isPageIdExist) {
		*(key->m_data + getKeyTotalLength((u16)key->m_size, isPageIdExist)) = '\0';
	}
};
}


#endif