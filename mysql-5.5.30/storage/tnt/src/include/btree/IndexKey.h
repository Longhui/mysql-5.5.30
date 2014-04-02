/**
 * NTSE B+��������ֵ������
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

/** ��ȡһ��PAGEID��RIDռ��PID_BYTES���ֽ� */
inline RowId PID_READ(byte *buf) {
	return RID_READ(buf);
}

/** д��һ��PAGE��PAGEIDռ��PID_BYTES���ֽ� */
inline void PID_WRITE(PageId rid, byte *buf) {
	RID_WRITE(rid, buf);
}


/**
 * ������������ѹ���洢���ȡ������Լ��������������˵��
 * �౾��ָ������������ʼλ�ã������ÿռ����ʹ�����Ѿ����������
 */
class IndexKey {
public:
	// ��ȡ/�����ֵ����
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

	// ����SubReocrd����������ֵʹ�����������SubRecord
	static Record* allocRecord(MemoryContext *memoryContext, uint size);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, const IndexDef *indexDef, RecFormat format);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, const SubRecord *key, const IndexDef *indexDef);
	static SubRecord* allocSubRecordRED(MemoryContext *memoryContext, uint maxKeySize);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, bool keyNeedCompress, const Record *record, Array<LobPair*> *lobArray, 
		const TableDef *tableDef, const IndexDef *indexDef);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, u16 numCols, u16 *columns, RecFormat format, u16 maxKeySize);

	/**
	 * ����������ֵ��ǰ׺����
	 *
	 * @param prevKey	��ֵ1
	 * @param nextKey	��ֵ2
	 * @return ��ͬǰ׺����
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
	 * �������ĳ����ֵ����ռ�
	 *
	 * @param key	Ҫ����ļ�ֵ
	 * @param prefixLen	�����ֵ��ǰ׺��ѹ������
	 * @isPageIdExist	��ֵ�Ƿ����PageId��Ϣ
	 * @return ��ֵ��Ҫ�ռ��С
	 */
	static u16 computeSpace(const SubRecord *key, u16 prefixLen, bool isPageIdExist) {
		u16 postfixLen = getKeyTotalLength((u16)key->m_size, isPageIdExist) - prefixLen;
		return (prefixLen >= 128 ? 2 : 1) + (postfixLen >= 128 ? 2 : 1) + postfixLen;
	}

	/**
	 * �ж�һ����ֵ�Ƿ���Ч����Ҫ����ֵ�ǲ���Ϊ��
	 * @param key Ҫ�жϵļ�ֵ
	 * @return ��ֵ��ΪNULL�ҳ��Ȳ�Ϊ0��ʾ��Ч����true������false
	 */
	static bool isKeyValid(const SubRecord *key) {
		return key != NULL && key->m_size != 0;
	}

private:
	/**
	 * ��ĳ����¼��ǰ��׺��������Ϣ���浽��ǰ��ַ
	 *
	 * @param prefixLen		ǰ׺������ռ�ֽ�
	 * @param postfixLen	��׺������ռ�ֽ�
	 * @param data		������Ϣ
	 * @return ������Ϣ֮��ĵ�ַ
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
	 * ����ָ����ǰһ��ֵ�͵�ǰ��ֵ�����浱ǰ��ֵ�ڵ�ǰλ��
	 *
	 * @param key		Ҫ����ĵ�ǰ��ֵ
	 * @param prefixLen	�Ѿ���֪�ĸü�ֵѹ����ǰ׺����
	 * @param isPageIdExist	key��data�����Ƿ����pageId��Ϣ
	 * @return ���ز������λ��
	 */
	inline byte* computePostfixAndSave(const SubRecord *key, u16 prefixLen, bool isPageIdExist) {
		assert(*(key->m_data + getKeyTotalLength((u16)key->m_size, isPageIdExist)) == '\0');
		u16 postfixLen = getKeyTotalLength((u16)key->m_size, isPageIdExist) - prefixLen;
		// ѹ���������
		return saveKeyInfo(prefixLen, postfixLen, key->m_data);
	}

	/** ���ؼ�ֵ���ȼ���ID��Ϣ֮�����󳤶ȣ����ֵ��֤�ü�ֵ���Դ洢����ID��Ϣ���Լ�����\0 */
	inline static u16 getKeyTotalLength(u16 keyLength, bool isPageIdExist) {
		return keyLength + getIDsLen(isPageIdExist);
	}

	/** ���ݼ�ֵ�Ƿ���Ҫ����PageId��Ϣ�����ؼ�ֵ��������ҪID��Ϣ��׼ȷ�ռ䳤�� */
	inline static u16 getIDsLen(bool isPageIdExist) {
		return isPageIdExist ? RID_BYTES + PID_BYTES : RID_BYTES;
	}

	/** ����ĳ����ֵ��PageId�洢����ʼ��ַ���ü�ֵ�����б���PageId */
	inline static byte* getPageIdStart(const SubRecord *key) {
		return key->m_data + key->m_size + RID_BYTES;
	}

	/** ����ĳ����ֵ��RowId�洢����ʼ��ַ */
	inline static byte* getRowIdStart(const SubRecord *key) {
		return key->m_data + key->m_size;
	}

	/** ����ֵ���ݵĽ���λ��һλ����Ϊ0 */
	inline static void setKeyDataEndZero(const SubRecord *key, bool isPageIdExist) {
		*(key->m_data + getKeyTotalLength((u16)key->m_size, isPageIdExist)) = '\0';
	}
};
}


#endif