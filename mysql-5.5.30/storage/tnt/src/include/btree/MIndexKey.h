/**
* �ڴ�������ֵ���
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _TNT_MINDEX_KEY_H_
#define _TNT_MINDEX_KEY_H_

#include "misc/Record.h"
#include "misc/Session.h"
#include "misc/TableDef.h"
#include "btree/IndexCommon.h"

using namespace ntse;

namespace tnt {

class MIndexPage;

typedef u16 MIdxVersion; /** �ڴ�������汾�ţ���ֹRowId������ */

typedef MIndexPage* MIndexPageHdl;    /** �ڴ�����ҳ��� */
#define MIDX_PAGE_NONE NULL           /** ���ڴ�����ҳ��� */
static const uint MINDEX_PAGE_HDL_SIZE = sizeof(MIndexPageHdl);/** �ڴ�����ҳ�����С */

#pragma pack(1)
/** �ڴ�����Ҷҳ���ֵ��չ��Ϣ */
struct LeafPageKeyExt {
public:
	static const u16 PAGE_KEY_EXT_LEN;//��չ��Ϣ����
public:
	byte      m_rowId[RID_BYTES];//�洢��ֵ��Ӧ��RowId
	MIdxVersion m_version;       //�汾��
	u8        m_deleteBit:1;     //delete bit����
	u8        m_extraData:7;     //�����ֶ�
};

/** �ڴ������ڲ�ҳ���ֵ��չ��Ϣ */
struct InnerPageKeyExt {
public:
	static const u16 PAGE_KEY_EXT_LEN;//��չ��Ϣ����
public:
	byte          m_rowId[RID_BYTES];//�洢��ֵ��Ӧ��RowId
	MIndexPageHdl m_pageHdl;         //��Ӧ����ҳ����
};
#pragma pack()

/** �ڴ������� */
class MIndexKeyOper
{
public:
	static SubRecord* convertKeyRP(MemoryContext *memoryContext, const SubRecord *key, Array<LobPair*> *lobArray,
		const TableDef *tableDef, const IndexDef *indexDef);
	static SubRecord* convertKeyRN(MemoryContext *memoryContext, const SubRecord *key, Array<LobPair*> *lobArray,
		const TableDef *tableDef, const IndexDef *indexDef);
	static SubRecord* convertKeyPN(MemoryContext *memoryContext, const SubRecord *key, const TableDef *tableDef, const IndexDef *indexDef);
	static void copyKeyAndExt(SubRecord *dst, const SubRecord *src, bool isLeafPageKey);
	static void copyKey(SubRecord *dst, const SubRecord *src);
	static s32 compareKey(const SubRecord *key, const SubRecord *indexKey, const SearchFlag *flag, 
		KeyComparator *comparator);

	static SubRecord* allocSubRecordRED(MemoryContext *memoryContext, uint maxKeySize);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, const IndexDef *indexDef, 
		RecFormat format);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, const SubRecord *key, 
		const IndexDef *indexDef);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, const Record *record,
		const TableDef *tableDef, const IndexDef *indexDef);
	static SubRecord* allocSubRecord(MemoryContext *memoryContext, u16 numCols, u16 *columns, 
		RecFormat format, u16 maxKeySize);
	static SubRecord* createInfiniteKey(MemoryContext *memoryContext, const IndexDef *indexDef);

	/**
	 * �ж��Ƿ������޴�������
	 * @param key
	 * @return 
	 */
	inline static bool isInfiniteKey(const SubRecord * key) {
		return 0 == key->m_size && !(key->m_rowId ^ 0xFFFFFFFFFFFF);   //0xFFFFFFFFFFFF ��ΪInvalidRowId ��8�ֽ�ת����6�ֽڵĽ������ΪRowIdֻ��6���ֽ�������
	}

	/**
	 * �ж��������Ƿ���Ч
	 * @param key ������
	 * @return 
	 */
	inline static bool isKeyValid(const SubRecord *key) {
		return NULL != key->m_data && 0 != key->m_size;
	}

	/**
	 * ��ü�ֵ��չ��Ϣ��ʼ��ַ
	 * @param key ������
	 * @return ��չ��Ϣ��ʼ��ַ
	 */
	inline static byte* getKeyExtStart(const SubRecord *key) {
		return key->m_data + key->m_size;
	}

	/**
	 * ��ü�ֵ��չ��Ϣ����
	 * @param isLeafKey �Ƿ���Ҷҳ��ļ�ֵ
	 * @return ��չ��Ϣ����
	 */
	inline static u16 getKeyExtLength(bool isLeafKey) {
		return isLeafKey ? LeafPageKeyExt::PAGE_KEY_EXT_LEN : InnerPageKeyExt::PAGE_KEY_EXT_LEN;
	}

	/**
	 * ����洢������ֵ��Ϣ����Ҫ�Ŀռ��С������rowid����ҳ������
	 * @param keyDataLen ����ֵ���ݳ���
	 * @param isLeafKey �Ƿ���Ҷҳ���������
	 * @return 
	 */
	inline static u16 calcKeyTotalLen(u16 keyDataLen, bool isLeafKey) {
		return keyDataLen + (isLeafKey ? LeafPageKeyExt::PAGE_KEY_EXT_LEN : InnerPageKeyExt::PAGE_KEY_EXT_LEN);
	}

	/**
	 * ��ȡ��ҳ����
	 * @param key
	 * @return 
	 */
	inline static MIndexPageHdl readPageHdl(const SubRecord *key) {
		InnerPageKeyExt *keyExt = (InnerPageKeyExt*)getKeyExtStart(key);
		return keyExt->m_pageHdl;
	}

	/**
	 * д����ҳ����
	 * @param key
	 * @param pageHdl
	 * @return 
	 */
	inline static void writePageHdl(SubRecord *key, MIndexPageHdl pageHdl) {
		InnerPageKeyExt *keyExt = (InnerPageKeyExt*)getKeyExtStart(key);
		keyExt->m_pageHdl = pageHdl;
	}

	/**
	 * ��ȡRowId
	 * @param key
	 * @return 
	 */
	inline static RowId readRowId(const SubRecord *key) {
		byte *ext = getKeyExtStart(key);
		return RID_READ(ext);
	}

	/**
	 * д��RowId
	 * @param key
	 * @param rowId
	 * @return 
	 */
	inline static void writeRowId(SubRecord *key, RowId rowId) {
		byte *ext = getKeyExtStart(key);
		RID_WRITE(rowId, ext);
	}

	/**
	 * ��ȡdelete bit
	 * @param key
	 * @return 
	 */
	inline static u8 readDelBit(const SubRecord *key) {
		LeafPageKeyExt *keyExt = (LeafPageKeyExt *)getKeyExtStart(key);
		return keyExt->m_deleteBit;
	}

	/**
	 * д��delete bit
	 * @param key
	 * @param delBit
	 * @return 
	 */
	inline static void writeDelBit(SubRecord *key, u8 delBit) {
		LeafPageKeyExt *keyExt = (LeafPageKeyExt *)getKeyExtStart(key);
		keyExt->m_deleteBit = delBit;
	}

	/**
	 * ��ȡ������汾
	 * @param key
	 * @return 
	 */
	inline static MIdxVersion readIdxVersion (const SubRecord *key) {
		LeafPageKeyExt *keyExt = (LeafPageKeyExt *)getKeyExtStart(key);
		return keyExt->m_version;
	}

	/**
	 * д��������汾
	 * @param key
	 * @param version
	 * @return 
	 */
	inline static void writeIdxVersion(SubRecord *key, MIdxVersion version) {
		LeafPageKeyExt *keyExt = (LeafPageKeyExt *)getKeyExtStart(key);
		keyExt->m_version = version;
	}

private:
	MIndexKeyOper() {}
	~MIndexKeyOper() {}
};

}

#endif
