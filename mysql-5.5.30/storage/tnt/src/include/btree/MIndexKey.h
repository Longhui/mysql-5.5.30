/**
* 内存索引键值相关
*
* @author 李伟钊(liweizhao@corp.netease.com)
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

typedef u16 MIdxVersion; /** 内存索引项版本号，防止RowId被重用 */

typedef MIndexPage* MIndexPageHdl;    /** 内存索引页句柄 */
#define MIDX_PAGE_NONE NULL           /** 空内存索引页句柄 */
static const uint MINDEX_PAGE_HDL_SIZE = sizeof(MIndexPageHdl);/** 内存索引页句柄大小 */

#pragma pack(1)
/** 内存索引叶页面键值扩展信息 */
struct LeafPageKeyExt {
public:
	static const u16 PAGE_KEY_EXT_LEN;//扩展信息长度
public:
	byte      m_rowId[RID_BYTES];//存储键值对应的RowId
	MIdxVersion m_version;       //版本号
	u8        m_deleteBit:1;     //delete bit属性
	u8        m_extraData:7;     //保留字段
};

/** 内存索引内部页面键值扩展信息 */
struct InnerPageKeyExt {
public:
	static const u16 PAGE_KEY_EXT_LEN;//扩展信息长度
public:
	byte          m_rowId[RID_BYTES];//存储键值对应的RowId
	MIndexPageHdl m_pageHdl;         //对应的子页面句柄
};
#pragma pack()

/** 内存索引键 */
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
	 * 判断是否是无限大索引键
	 * @param key
	 * @return 
	 */
	inline static bool isInfiniteKey(const SubRecord * key) {
		return 0 == key->m_size && !(key->m_rowId ^ 0xFFFFFFFFFFFF);   //0xFFFFFFFFFFFF 即为InvalidRowId 从8字节转化成6字节的结果，因为RowId只有6个字节有意义
	}

	/**
	 * 判断索引键是否有效
	 * @param key 索引键
	 * @return 
	 */
	inline static bool isKeyValid(const SubRecord *key) {
		return NULL != key->m_data && 0 != key->m_size;
	}

	/**
	 * 获得键值扩展信息起始地址
	 * @param key 索引键
	 * @return 扩展信息起始地址
	 */
	inline static byte* getKeyExtStart(const SubRecord *key) {
		return key->m_data + key->m_size;
	}

	/**
	 * 获得键值扩展信息长度
	 * @param isLeafKey 是否是叶页面的键值
	 * @return 扩展信息长度
	 */
	inline static u16 getKeyExtLength(bool isLeafKey) {
		return isLeafKey ? LeafPageKeyExt::PAGE_KEY_EXT_LEN : InnerPageKeyExt::PAGE_KEY_EXT_LEN;
	}

	/**
	 * 计算存储完整键值信息所需要的空间大小，包括rowid，子页面句柄等
	 * @param keyDataLen 纯键值数据长度
	 * @param isLeafKey 是否是叶页面的索引项
	 * @return 
	 */
	inline static u16 calcKeyTotalLen(u16 keyDataLen, bool isLeafKey) {
		return keyDataLen + (isLeafKey ? LeafPageKeyExt::PAGE_KEY_EXT_LEN : InnerPageKeyExt::PAGE_KEY_EXT_LEN);
	}

	/**
	 * 读取子页面句柄
	 * @param key
	 * @return 
	 */
	inline static MIndexPageHdl readPageHdl(const SubRecord *key) {
		InnerPageKeyExt *keyExt = (InnerPageKeyExt*)getKeyExtStart(key);
		return keyExt->m_pageHdl;
	}

	/**
	 * 写入子页面句柄
	 * @param key
	 * @param pageHdl
	 * @return 
	 */
	inline static void writePageHdl(SubRecord *key, MIndexPageHdl pageHdl) {
		InnerPageKeyExt *keyExt = (InnerPageKeyExt*)getKeyExtStart(key);
		keyExt->m_pageHdl = pageHdl;
	}

	/**
	 * 读取RowId
	 * @param key
	 * @return 
	 */
	inline static RowId readRowId(const SubRecord *key) {
		byte *ext = getKeyExtStart(key);
		return RID_READ(ext);
	}

	/**
	 * 写入RowId
	 * @param key
	 * @param rowId
	 * @return 
	 */
	inline static void writeRowId(SubRecord *key, RowId rowId) {
		byte *ext = getKeyExtStart(key);
		RID_WRITE(rowId, ext);
	}

	/**
	 * 读取delete bit
	 * @param key
	 * @return 
	 */
	inline static u8 readDelBit(const SubRecord *key) {
		LeafPageKeyExt *keyExt = (LeafPageKeyExt *)getKeyExtStart(key);
		return keyExt->m_deleteBit;
	}

	/**
	 * 写入delete bit
	 * @param key
	 * @param delBit
	 * @return 
	 */
	inline static void writeDelBit(SubRecord *key, u8 delBit) {
		LeafPageKeyExt *keyExt = (LeafPageKeyExt *)getKeyExtStart(key);
		keyExt->m_deleteBit = delBit;
	}

	/**
	 * 读取索引项版本
	 * @param key
	 * @return 
	 */
	inline static MIdxVersion readIdxVersion (const SubRecord *key) {
		LeafPageKeyExt *keyExt = (LeafPageKeyExt *)getKeyExtStart(key);
		return keyExt->m_version;
	}

	/**
	 * 写入索引项版本
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
