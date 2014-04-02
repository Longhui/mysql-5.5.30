/**
 * NTSE B+树索引日志类
 * 
 * author: naturally (naturally@163.org)
 */

#ifndef _NTSE_INDEX_LOG_H_
#define _NTSE_INDEX_LOG_H_

#include "misc/Global.h"
#include "stddef.h"
#include "misc/TableDef.h"

namespace ntse {

class Session;
class Record;
class SubRecord;
class IndexDef;

#define myoffsetof(TYPE, MEMBER) ((size_t)((char *)&(((TYPE *)0x10)->MEMBER) - (char*)0x10))
#define OFFSET(structure, member) ((u16)myoffsetof(structure, member))
#define OFFSETOFARRAY(structure, member, arrayTypeSize, seqNo) ((u16)(myoffsetof(structure, member) + (arrayTypeSize) * (seqNo)))

#define IDX_LOG_TYPE	u8
enum IDXLogType {		// 代表具体的日志类型，占用日志1字节
	IDX_LOG_UPDATE_PAGE = 0,		/** 对页面的更新操作 */
	IDX_LOG_ADD_MP,					/** 对页面增加一个MiniPage操作 */
	IDX_LOG_DELETE_MP,				/** 删除页面的一个MiniPage操作 */
	IDX_LOG_SPLIT_MP,				/** 页面MiniPage分裂操作 */
	IDX_LOG_MERGE_MP,				/** 页面MiniPage合并操作 */
	IDX_LOG_SMO_MERGE,				/** SMO的页面合并操作 */
	IDX_LOG_SMO_SPLIT,				/** SMO的页面分裂操作 */
	IDX_LOG_INSERT,					/** 索引插入操作 */
	IDX_LOG_DELETE,					/** 索引删除操作 */
	IDX_LOG_APPEND					/** 索引添加数据到页尾操作 */
};

/**
 * 负责编排格式记录索引操作日志以及解析对应的日志
 * 一个类对应一个Indice，因此对应的表是唯一的
 */
class IndexLog {
public:
	IndexLog(u16 tableId) {
		m_tableId = tableId;
		m_available = true;
	}

	void enable() {
		m_available = true;
	}

	void disable() {
		m_available = false;
	}
	
	// 正常顺序日志
	u64 logCreateIndexBegin(Session *session, u8 indexId);
	u64 logCreateIndexEnd(Session *session, const IndexDef *indexDef, u64 beginLSN, u8 indexId, bool successful);
	u64 logDMLUpdateBegin(Session *session);
	u64 logDMLDoneUpdateIdxNo(Session *session, u8 indexNo);
	u64 logDMLDeleteInUpdateDone(Session *session);
	u64 logDMLUpdateEnd(Session *session, u64 beginLSN, bool succ);
	u64 logDropIndex(Session *session, u8 indexId, s32 idxNo);

	u64 logDMLUpdate(Session *session, IDXLogType type, PageId pageId, u16 offset, u16 miniPageNo, byte *oldValue, u16 oldSize, byte *newValue, u16 newSize, u64 origLSN);

	u64 logSMOMerge(Session *session, IDXLogType type, PageId pageId, PageId mergePageId, PageId prevPageId, const byte *moveData, u16 dataSize, const byte *moveDir, u16 dirSize, u64 origLSN1, u64 origLSN2);
	u64 logSMOSplit(Session *session, IDXLogType type, PageId pageId, PageId newPageId, PageId nextPageId, const byte *moveData, u16 dataSize, const byte *oldSplitKey, u16 oldSKLen, const byte *newSplitKey, u16 newSKLen, const byte *moveDir, u16 dirSize, u8 mpLeftCount, u8 mpMoveCount, u64 origLSN1, u64 origLSN2);

	u64 logPageUpdate(Session *session, IDXLogType type, PageId pageId, u16 offset, const byte *newValue, const byte *oldValue, u16 size, u64 origLSN, bool clearPageFirst = false);
	u64 logPageAddMP(Session *session, IDXLogType type, PageId pageId, const byte *keyValue, u16 dataSize, u16 miniPageNo, u64 origLSN);
	u64 logPageDeleteMP(Session *session, IDXLogType type, PageId pageId, const byte *keyValue, u16 size, u16 miniPageNo, u64 origLSN);
	u64 logSplitMP(Session *session, IDXLogType type, PageId pageId, u16 offset, byte *compressValue, u16 compressSize, byte *extractedValue, u16 extractedSize, u16 leftItems, u16 miniPageNo, u64 origLSN);
	u64 logMergeMP(Session *session, IDXLogType type, PageId pageId, u16 offset, byte *compressValue, u16 compressSize, byte *originalValue, u16 originalSize, u16 miniPageNo, u16 originalMPKeyCounts, u64 origLSN);

	void decodeDMLUpdate(const byte *log, uint size, IDXLogType *type, PageId *pageId, u16 *offset, u16 *miniPageNo, byte **oldValue, u16 *oldSize, byte **newValue, u16 *newSize, u64 *origLSN);
	void decodeSMOMerge(const byte *log, uint size, PageId *pageId, PageId *mergePageId, PageId *prevPageId, byte **moveData, u16 *dataSize, byte **moveDir, u16 *dirSize, u64 *origLSN1, u64 *origLSN2);
	void decodeSMOSplit(const byte *log, uint size, PageId *pageId, PageId *newPageId, PageId *nextPageId, byte **moveData, u16 *dataSize, byte **oldSplitKey, u16 *oldSKLen, byte **newSplitKey, u16 *newSKLen, byte **moveDir, u16 *dirSize, u8 *mpLeftCount, u8 *mpMoveCount, u64 *origLSN1, u64 *origLSN2);

	void decodePageUpdate(const byte *log, uint size, PageId *pageId, u16 *offset, byte **newValue, byte **oldValue, u16 *valueLen, u64 *origLSN, bool *clearPageFirst);
	void decodePageAddMP(const byte *log, uint size, PageId *pageId, byte **keyValue, u16 *dataSize, u16 *miniPageNo, u64 *origLSN);
	void decodePageDeleteMP(const byte *log, uint size, PageId *pageId, byte **keyValue, u16 *dataSize, u16 *miniPageNo, u64 *origLSN);
	void decodePageSplitMP(const byte *log, uint size, PageId *pageId, u16 *offset, byte **compressValue, u16 *compressSize, byte **extractedValue, u16 *extractedSize, u16 *leftItems, u16 *miniPageNo, u64 *origLSN);
	void decodePageMergeMP(const byte *log, uint size, PageId *pageId, u16 *offset, byte **compressValue, u16 *compressSize, byte **originalValue, u16 *originalSize, u16 *originalMPKeyCounts, u16 *miniPageNo, u64 *origLSN);

	void decodeDropIndex(const byte *log, uint size, u8 *indexId, s32 *idxNo);

	void decodeCreateIndex(const byte *log, uint size, u8 *indexId);
	void decodeCreateIndexEnd(const byte *log, uint size, IndexDef *indexDef, u8 *indexId, bool *successful);

	void decodeDMLDoneUpdateIdxNo(const byte *log, uint size, u8 *indexNo);
	void decodeDMLUpdateEnd(const byte *log, uint size, bool *succ);

	// 补偿日志
	u64 logDMLUpdateCPST(Session *session, u64 prevLsn, const byte *log, uint size);
	u64 logSMOCPST(Session *session, u64 prevLsn, const byte *log, uint size);
	u64 logPageSetCPST(Session *session, u64 prevLsn, const byte *log, uint size);
	u64 logCreateIndexCPST(Session *session, u64 prevLsn, const byte *log, uint size);

	IDXLogType getType(const byte *log, uint size);

private:
	u16 getCommonPostfix(const byte *data1, u16 size1, const byte *data2, u16 size2);

private:
	u16		m_tableId;		/** 对应的表ID */
	bool	m_available;	/** 标志当前日志器是否可用，不可用不记录日志 */
};

}

#endif

