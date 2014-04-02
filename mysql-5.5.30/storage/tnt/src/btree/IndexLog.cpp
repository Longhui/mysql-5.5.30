/**
 * NTSE B+树索引日志类实现
 *
 * author: naturally (naturally@163.org)
 */

#include <iostream>
#include "btree/IndexLog.h"
#include "api/Table.h"
#include "util/Stream.h"
#include "misc/Record.h"
#include "misc/Session.h"
#include "util/SmartPtr.h"

namespace ntse {


/**
 * 记录索引创建开始日志
 * @param session	会话句柄
 * @param indexId	创建的索引ID
 * @return 日志LSN
 */
u64 IndexLog::logCreateIndexBegin(Session *session, u8 indexId) {
	return session->writeLog(LOG_IDX_CREATE_BEGIN, m_tableId, (byte*)&indexId, sizeof(u8));
}


/**
 * 记录索引创建结束日志
 * @param session		会话句柄
 * @param indexDef		索引定义
 * @param beginLSN		创建开始日志LSN
 * @param indexId		索引ID
 * @param successful	索引创建是否成功
 * @return 日志LSN
 */
u64 IndexLog::logCreateIndexEnd(Session *session, const IndexDef *indexDef, u64 beginLSN, u8 indexId, bool successful) {
	u64 lsn = 0;
	byte *indexBuf = NULL;
	u32 indexSize = 0;
	byte *buffer = NULL;

	indexDef->writeToKV(&indexBuf, &indexSize);
	AutoPtr<byte> autoIndexBuf(indexBuf, true);

	McSavepoint msp(session->getMemoryContext());
	buffer = (byte*)session->getMemoryContext()->alloc(indexSize + Limits::PAGE_SIZE);
	Stream s(buffer, indexSize + Limits::PAGE_SIZE);
	try {
		s.write(indexId)->write(beginLSN);
		s.write(successful);
		s.write(indexSize);
		s.write(indexBuf, indexSize);
	} catch (NtseException) { NTSE_ASSERT(false); }

	lsn = session->writeLog(LOG_IDX_CREATE_END, m_tableId, buffer, s.getSize());
	session->flushLog(lsn, FS_NTSE_CREATE_INDEX);

	return lsn;
}



/**
 * 记录索引修改开始日志
 * @param session	会话句柄
 * @return 日志LSN
 */
u64 IndexLog::logDMLUpdateBegin(Session *session) {
	return session->writeLog(LOG_IDX_DML_BEGIN, m_tableId, NULL, 0);
}


/**
 * 记录索引修改结束日志
 * @param session	会话句柄
 * @param beginLSN	索引修改开始日志LSN
 * @param succ		索引修改是否成功
 * @return 日志LSN
 */
u64 IndexLog::logDMLUpdateEnd(Session *session, u64 beginLSN, bool succ) {
	byte buf[sizeof(beginLSN) + sizeof(succ)];
	Stream s(buf, sizeof(buf));
	try {
		s.write(beginLSN)->write(succ);
	} catch (NtseException) { NTSE_ASSERT(false); }
	return session->writeLog(LOG_IDX_DML_END, m_tableId, buf, s.getSize());
}



/**
 * 在DML过程当中记录某个索引的相关更新操作完整结束
 * @param session	会话句柄
 * @param indexNo	索引编号
 * @return 日志LSN
 */
u64 IndexLog::logDMLDoneUpdateIdxNo(Session *session, u8 indexNo) {
	byte buf[sizeof(indexNo)];
	Stream s(buf, sizeof(buf));
	try {
		s.write(indexNo);
	} catch (NtseException) { NTSE_ASSERT(false); }
	return session->writeLog(LOG_IDX_DMLDONE_IDXNO, m_tableId, buf, s.getSize());
}



/**
 * 记录丢弃索引日志
 * @param session	会话句柄
 * @param indexId	丢弃的索引ID
 * @param idxNo		索引序列号
 * @return 日志LSN
 */
u64 IndexLog::logDropIndex(Session *session, u8 indexId, s32 idxNo) {
	byte buf[sizeof(indexId) + sizeof(idxNo)];
	Stream s(buf, sizeof(buf));
	try {
		s.write(indexId)->write(idxNo);
	} catch (NtseException) { NTSE_ASSERT(false); }
	return session->writeLog(LOG_IDX_DROP_INDEX, m_tableId, buf, s.getSize());
}


/**
 * 记录索引具体修改操作日志
 * @param session		会话句柄
 * @param type			修改类型：INSERT/DELETE/APPEND
 * @param pageId		修改页面的ID
 * @param offset		修改的页内起始偏移量
 * @param miniPageNo	修改涉及的MiniPage号
 * @param oldValue		修改之前的页内数据
 * @param oldSize		修改之前页内数据的长度
 * @param newValue		修改之后的页内数据
 * @param newSize		修改之后也那数据的长度
 * @param origLSN		记录日志页面的lsn
 * @return 日志LSN
 */
u64 IndexLog::logDMLUpdate(Session *session, IDXLogType type, PageId pageId, u16 offset, u16 miniPageNo, byte *oldValue, u16 oldSize, byte *newValue, u16 newSize, u64 origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	//ftrace(ts.recv, tout << type << pageId << origLSN);

	byte buffer[Limits::PAGE_SIZE];
	Stream s(buffer, Limits::PAGE_SIZE);

	u16 commonPostfix = getCommonPostfix(oldValue, oldSize, newValue, newSize);
	assert(oldSize >= commonPostfix);
	assert(newSize >= commonPostfix);
	assert(!(oldSize - commonPostfix == 0 && newSize - commonPostfix == 0));
	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(offset);
		s.write(miniPageNo);
		s.write(u16(oldSize - commonPostfix))->write(oldValue, oldSize - commonPostfix);
		s.write(u16(newSize - commonPostfix))->write(newValue, newSize - commonPostfix);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_DML, m_tableId, buffer, s.getSize());
}


/**
 * 记录索引页面SMO操作日志
 * @param session		会话句柄
 * @param type			具体SMO类型：SMOMerge，用于解析日志
 * @param pageId		引发SMO的原始页面ID
 * @param mergePageId	要合并的页面ID，原页面的左页面
 * @param prevPageId	合并阿面的前一个页面ID
 * @param moveData		合并过程移动的数据
 * @param dataSize		数据长度
 * @param moveDir		合并过程移动的项目录
 * @Param dirSize		项目录长度
 * @param origLSN1		pageId对应页面的原始LSN
 * @param origLSN2		mergePageId对应页面原始LSN
 * @return 日志LSN
 */
u64 IndexLog::logSMOMerge(Session *session, IDXLogType type, PageId pageId, PageId mergePageId, PageId prevPageId, const byte *moveData, u16 dataSize, const byte *moveDir, u16 dirSize, u64 origLSN1, u64 origLSN2) {
	UNREFERENCED_PARAMETER(origLSN1);
	UNREFERENCED_PARAMETER(origLSN2);
	//ftrace(ts.recv, tout << type << pageId << origLSN1 << mergePageId << origLSN2;);
	
	byte buffer[Limits::PAGE_SIZE];
	Stream s(buffer, Limits::PAGE_SIZE);

	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(mergePageId);
		s.write(prevPageId);
		s.write(dataSize)->write(moveData, dataSize);
		s.write(dirSize)->write(moveDir, dirSize);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN1);
		s.write((u64)origLSN2);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_SMO, m_tableId, buffer, s.getSize());
}


/**
 * 记录索引SMO分裂日志
 * @param session		会话句柄
 * @param type			具体SMO类型：SMOSplit，用于解析日志
 * @param pageId		引发SMO的原始页面ID
 * @param newPageId		使用的新页面ID
 * @param nextPageId	原始页面分裂前的后继页面
 * @param moveData		分裂过程移动的数据
 * @param dataSize		数据长度
 * @param oldSplitKey	分裂键值原始内容
 * @param oldSKLen		分裂键值原始长度
 * @param newSplitKey	分裂键值新内容
 * @param newSKLen		分裂键值新长度
 * @param moveDir		分裂过程移动的项目录
 * @param dirSize		项目录长度
 * @param mpLeftCount	分裂MP留在原始页面的项数
 * @param mpMoveCount	分裂MP移到新页面的项数
 * @param origLSN1		pageId对应页面的原始LSN
 * @param origLSN2		newPageId对应页面原始LSN
 * @return 日志LSN
 */
u64 IndexLog::logSMOSplit(Session *session, IDXLogType type, PageId pageId, PageId newPageId, PageId nextPageId, const byte *moveData, u16 dataSize, const byte *oldSplitKey, u16 oldSKLen, const byte *newSplitKey, u16 newSKLen, const byte *moveDir, u16 dirSize, u8 mpLeftCount, u8 mpMoveCount, u64 origLSN1, u64 origLSN2) {
	UNREFERENCED_PARAMETER(origLSN1);
	UNREFERENCED_PARAMETER(origLSN2);
	//ftrace(ts.recv, tout << type << pageId << origLSN1 << newPageId << origLSN2;);

	byte buffer[Limits::PAGE_SIZE];
	Stream s(buffer, Limits::PAGE_SIZE);

	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(newPageId);
		s.write(nextPageId);
		s.write(dataSize)->write(moveData, dataSize);
		s.write(oldSKLen)->write(oldSplitKey, oldSKLen);
		s.write(newSKLen)->write(newSplitKey, newSKLen);
		s.write(dirSize)->write(moveDir, dirSize);
		s.write(mpLeftCount);
		s.write(mpMoveCount);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN1);
		s.write((u64)origLSN2);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_SMO, m_tableId, buffer, s.getSize());
}


/**
 * 记录索引页面更新日志
 * @param session			会话句柄
 * @param type				日之具体类型
 * @param pageId			修改的页面ID
 * @param offset			修改起始偏移
 * @param newValue			修改后的页面内容
 * @param oldVlaue			修改前的页面内容
 * @param size				修改内容的长度
 * @param origLSN			pageId页面原始LSN
 * @param clearPageFirst	恢复之前首先将页面清零
 * @return 日志LSN
 */
u64 IndexLog::logPageUpdate(Session *session, IDXLogType type, PageId pageId, u16 offset, const byte *newValue, const byte *oldValue, u16 size, u64 origLSN, bool clearPageFirst) {
	UNREFERENCED_PARAMETER(origLSN);
	//ftrace(ts.recv, tout << type << pageId << origLSN1 << newPageId << origLSN2;);

	byte buffer[Limits::PAGE_SIZE * 3];
	Stream s(buffer, Limits::PAGE_SIZE * 3);

	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(offset);
		s.write(size);
		s.write(newValue, size);
		s.write(oldValue, size);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN);
#endif
		s.write(clearPageFirst);
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_SET_PAGE, m_tableId, buffer, s.getSize());
}


/**
 * 记录索引页面添加一个MiniPage并插入键值操作
 * @param session		会话句柄
 * @param type			日志具体类型
 * @param pageId		修改的页面ID
 * @param keyValue		插入后的键值内容
 * @param dataSize		插入后键值长度
 * @param miniPageNo	新创建的MiniPage号
 * @param origLSN		pageId页面原始LSN
 * @return 日志LSN
 */
u64 IndexLog::logPageAddMP(Session *session, IDXLogType type, PageId pageId, const byte *keyValue, u16 dataSize, u16 miniPageNo, u64 origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	byte buffer[Limits::PAGE_SIZE];
	Stream s(buffer, Limits::PAGE_SIZE);

	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(dataSize)->write(keyValue, dataSize);
		s.write(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_SET_PAGE, m_tableId, buffer, s.getSize());
}


/**
 * 记录索引页面删除一个MiniPage操作
 * @param session		会话句柄
 * @param type			日志具体类型
 * @param pageId		修改的页面ID
 * @param keyValue		删除前的键值内容
 * @param size			删除前键值长度
 * @param miniPageNo	新创建的MiniPage号
 * @param origLSN		pageId页面原始LSN
 * @return 日志LSN
 */
u64 IndexLog::logPageDeleteMP(Session *session, IDXLogType type, PageId pageId, const byte *keyValue, u16 size, u16 miniPageNo, u64 origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	//ftrace(ts.recv, tout << type << pageId << origLSN;);

	byte buffer[Limits::PAGE_SIZE];
	Stream s(buffer, Limits::PAGE_SIZE);

	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(size)->write(keyValue, size);
		s.write(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_SET_PAGE, m_tableId, buffer, s.getSize());
}


/**
 * 记录索引页面分裂某个MiniPage操作
 * @param session		会话句柄
 * @param type			日志具体类型
 * @param pageId		修改的页面ID
 * @param offset		分裂点的页内偏移
 * @param compressValue	分裂点键值的压缩格式
 * @param compressSize	压缩格式键值长度
 * @param extractedValue分裂后前述键值的格式
 * @param extractedSize	分裂后键值的长度
 * @param leftItems		分裂后在原Minipage剩余的项数
 * @param miniPageNo	要分裂的Minipage号
 * @param origLSN		pageId页面原始LSN
 * @return 日志LSN
 */
u64 IndexLog::logSplitMP(Session *session, IDXLogType type, PageId pageId, u16 offset, byte *compressValue, u16 compressSize, byte *extractedValue, u16 extractedSize, u16 leftItems, u16 miniPageNo, u64 origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	//ftrace(ts.recv, tout << type << pageId << origLSN;);

	byte buffer[Limits::PAGE_SIZE];
	Stream s(buffer, Limits::PAGE_SIZE);

	u16 commonPostfix = getCommonPostfix(compressValue, compressSize, extractedValue, extractedSize);
	assert(compressSize >= commonPostfix);
	assert(extractedSize >= commonPostfix);

	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(offset);
		s.write(u16(compressSize - commonPostfix))->write(compressValue, compressSize - commonPostfix);
		s.write(u16(extractedSize - commonPostfix))->write(extractedValue, extractedSize - commonPostfix);
		s.write(leftItems);
		s.write(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_SET_PAGE, m_tableId, buffer, s.getSize());
}

/** 记录索引页面合并某个MP操作的日志
 * @param session		会话
 * @param type			日志具体类型
 * @param pageId		索引页面ID信息
 * @param offset		合并点的页内偏移
 * @param compressValue	合并之后压缩的键值内容
 * @param compressSize	合并之后压缩的键值长度
 * @param originalValue	合并前未压缩键值内容
 * @param originalSize	合并前为压缩键值长度
 * @param miniPageNo	合并之后的MP号
 * @param originalMPKeyCounts	合并之前左边MP包含的键值个数
 * @param origLSN		页面原来的LSN信息
 * @return 返回记录日志的LSN
 */
u64 IndexLog::logMergeMP( Session *session, IDXLogType type, PageId pageId, u16 offset, byte *compressValue, u16 compressSize, byte *originalValue, u16 originalSize, u16 miniPageNo, u16 originalMPKeyCounts, u64 origLSN ) {
	UNREFERENCED_PARAMETER(origLSN);
	//ftrace(ts.recv, tout << type << pageId << origLSN;);

	byte buffer[Limits::PAGE_SIZE];
	Stream s(buffer, Limits::PAGE_SIZE);

	u16 commonPostfix = getCommonPostfix(compressValue, compressSize, originalValue, originalSize);
	assert(compressSize >= commonPostfix);
	assert(originalSize >= commonPostfix);

	try {
		s.write((IDX_LOG_TYPE)type);
		s.write(pageId);
		s.write(offset);
		s.write(u16(compressSize - commonPostfix))->write(compressValue, compressSize - commonPostfix);
		s.write(u16(originalSize - commonPostfix))->write(originalValue, originalSize - commonPostfix);
		s.write(originalMPKeyCounts);
		s.write(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.write((u64)origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	return session->writeLog(LOG_IDX_SET_PAGE, m_tableId, buffer, s.getSize());
}


/**
 * 解析丢弃索引日志
 * @param log		日志
 * @param size		日志长度
 * @param indexId	OUT	丢弃的索引ID
 * @param idxNo		OUT	丢弃的索引的序列号
 */
void IndexLog::decodeDropIndex(const byte *log, uint size, u8 *indexId, s32 *idxNo) {
	Stream s((byte*)log, size);

	try {
		s.read(indexId);
		s.read(idxNo);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析索引修改操作日志
 * @param log			日志
 * @param size			日志长度
 * @param type			OUT	修改类型：INSERT/DELETE/APPEND
 * @param pageId		OUT	修改页面的ID
 * @param offset		OUT	修改的页内起始偏移量
 * @param miniPageNo	OUT	修改涉及的MiniPage号
 * @param oldValue		OUT	修改之前的页内数据
 * @param oldSize		OUT	修改之前页内数据的长度
 * @param newValue		OUT	修改之后的页内数据
 * @param newSize		OUT	修改之后页内数据的长度
 * @param origLSN		OUT	修改之前页面LSN
 */
void IndexLog::decodeDMLUpdate(const byte *log, uint size, IDXLogType *type, PageId *pageId, u16 *offset, u16 *miniPageNo, byte **oldValue, u16 *oldSize, byte **newValue, u16 *newSize, u64 *origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	Stream s((byte*)log, size);

	try {
		IDX_LOG_TYPE mode;
		s.read(&mode);
		*type = (IDXLogType)mode;

		s.read(pageId);
		s.read(offset);
		s.read(miniPageNo);
		s.read(oldSize);
		*oldValue = (byte*)log + s.getSize();
		s.skip(*oldSize);
		s.read(newSize);
		*newValue = (byte*)log + s.getSize();
		s.skip(*newSize);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN);
#endif
		assert(!(*oldSize == 0 && *newSize == 0));
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析索引SMO合并日志
 * @param log			日志
 * @param size			日志长度
 * @param pageId		OUT	引发SMO的原始页面ID
 * @param mergePageId	OUT	要合并的页面ID，原页面的左页面
 * @param prevPageId	OUT	合并阿面的前一个页面ID
 * @param moveData		OUT	合并过程移动的数据
 * @param dataSize		OUT	数据长度
 * @param moveDir		OUT	合并过程移动的项目录
 * @param dirSize		OUT	项目录长度
 * @param origLSN1		OUT pageId页面原始LSN
 * @param origLSN2		OUT mergePageId页面原始LSN
 */
void IndexLog::decodeSMOMerge(const byte *log, uint size, PageId *pageId, PageId *mergePageId, PageId *prevPageId, byte **moveData, u16 *dataSize, byte **moveDir, u16 *dirSize, u64 *origLSN1, u64 *origLSN2) {
	UNREFERENCED_PARAMETER(origLSN1);
	UNREFERENCED_PARAMETER(origLSN2);
	Stream s((byte*)log, size);

	try {
		s.skip(sizeof(IDX_LOG_TYPE));

		s.read(pageId);
		s.read(mergePageId);
		s.read(prevPageId);
		s.read(dataSize);
		*moveData = (byte*)log + s.getSize();
		s.skip(*dataSize);
		s.read(dirSize);
		*moveDir = (byte*)log + s.getSize();
		s.skip(*dirSize);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN1);
		s.read(origLSN2);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析SMO分裂日志
 * @param log			日志
 * @param size			日志长度
 * @param pageId		OUT	引发SMO的原始页面ID
 * @param newPageId		OUT	使用的新页面ID
 * @param nextPageId	OUT	原始页面分裂前的后继页面
 * @param moveData		OUT	分裂过程移动的数据
 * @param dataSize		OUT	数据长度
 * @param oldSplitKey	OUT	分裂点下一个键值原始内容
 * @param oldSKLen		OUT	分裂点下一个键值原始长度
 * @param newSplitKey	OUT	分裂点下一个键值新内容
 * @param newSKLen		OUT	分裂点下一个键值新长度
 * @param moveDir		OUT	分裂过程移动的项目录
 * @param dirSize		OUT	项目录长度
 * @param mpLeftCount	OUT	分裂MP剩下在原始页面的项数
 * @param mpMoveCount	OUT 分裂MP移动到新页面的项数
 * @param origLSN1		OUT pageId页面原始LSN
 * @param origLSN2		OUT newPageId页面原始LSN
 */
void IndexLog::decodeSMOSplit(const byte *log, uint size, PageId *pageId, PageId *newPageId, PageId *nextPageId,
							  byte **moveData, u16 *dataSize, byte **oldSplitKey, u16 *oldSKLen,
							  byte **newSplitKey, u16 *newSKLen, byte **moveDir, u16 *dirSize,
							  u8 *mpLeftCount, u8 *mpMoveCount, u64 *origLSN1, u64 *origLSN2) {
								  UNREFERENCED_PARAMETER(origLSN1);
								  UNREFERENCED_PARAMETER(origLSN2);
	Stream s((byte*)log, size);

	try {
		s.skip(sizeof(IDX_LOG_TYPE));

		s.read(pageId);
		s.read(newPageId);
		s.read(nextPageId);
		s.read(dataSize);
		*moveData = (byte*)log + s.getSize();
		s.skip(*dataSize);
		s.read(oldSKLen);
		*oldSplitKey = (byte*)log + s.getSize();
		s.skip(*oldSKLen);
		s.read(newSKLen);
		*newSplitKey = (byte*)log + s.getSize();
		s.skip(*newSKLen);
		s.read(dirSize);
		*moveDir = (byte*)log + s.getSize();
		s.skip(*dirSize);
		s.read(mpLeftCount);
		s.read(mpMoveCount);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN1);
		s.read(origLSN2);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}



/**
 * 解析索引页面更新日志
 * @param log				日志
 * @param size				日志长度
 * @param pageId			OUT	引发SMO的原始页面ID
 * @param offset			OUT	修改起始偏移
 * @param newValue			OUT	修改后的页面内容
 * @param oldVlaue			OUT	修改前的页面内容
 * @param size				OUT	修改内容的长度
 * @param origLSN			OUT	修改之前页面LSN
 * @param clearPageFirst	OUT 是否先清零页面
 */
void IndexLog::decodePageUpdate(const byte *log, uint size, PageId *pageId, u16 *offset, byte **newValue, byte **oldValue, u16 *valueLen, u64 *origLSN, bool *clearPageFirst) {
	UNREFERENCED_PARAMETER(origLSN);
	Stream s((byte*)log, size);

	try {
		s.skip(sizeof(IDX_LOG_TYPE));

		s.read(pageId);
		s.read(offset);
		s.read(valueLen);
		*newValue = (byte*)log + s.getSize();
		s.skip(*valueLen);
		*oldValue = (byte*)log + s.getSize();
		s.skip(*valueLen);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN);
#endif
		s.read(clearPageFirst);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析索引页面增加MiniPage日志
 * @param log			日志
 * @param size			日志长度
 * @param pageId		OUT	引发SMO的原始页面ID
 * @param keyValue		OUT	插入后的键值内容
 * @param dataSize		OUT	插入后键值长度
 * @param miniPageNo	OUT	新创建的MiniPage号
 * @param origLSN		OUT	修改之前页面LSN
 */
void IndexLog::decodePageAddMP(const byte *log, uint size, PageId *pageId, byte **keyValue, u16 *dataSize, u16 *miniPageNo, u64 *origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	Stream s((byte*)log, size);
	try {
		s.skip(sizeof(IDX_LOG_TYPE));

		s.read(pageId);
		s.read(dataSize);
		*keyValue = (byte*)log + s.getSize();
		s.skip(*dataSize);
		s.read(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析索引页面删除MiniPage日志
 * @param log			日志
 * @param size			日志长度
 * @param pageId		OUT	引发SMO的原始页面ID
 * @param keyValue		OUT	插入后的键值内容
 * @param dataSize		OUT	插入后键值长度
 * @param miniPageNo	OUT	新创建的MiniPage号
 * @param origLSN		OUT	修改之前页面LSN
 */
void IndexLog::decodePageDeleteMP(const byte *log, uint size, PageId *pageId, byte **keyValue, u16 *dataSize, u16 *miniPageNo, u64 *origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	Stream s((byte*)log, size);

	try {
		s.skip(sizeof(IDX_LOG_TYPE));

		s.read(pageId);
		s.read(dataSize);
		*keyValue = (byte*)log + s.getSize();
		s.skip(*dataSize);
		s.read(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析分裂页面MiniPage日志
 * @param log			日志
 * @param size			日志长度
 * @param pageId		OUT	引发SMO的原始页面ID
 * @param offset		OUT	分裂点的页内偏移
 * @param compressValue	OUT	分裂点键值的压缩格式
 * @param compressSize	OUT	压缩格式键值长度
 * @param extractedValueOUT	分裂后前述键值的格式
 * @param extractedSize	OUT	分裂后键值的长度
 * @param leftItems		OUT	分裂后在原Minipage÷
 * @param miniPageNo	OUT	要分裂的Minipage号
 * @param origLSN		OUT	修改之前页面LSN
 */
void IndexLog::decodePageSplitMP(const byte *log, uint size, PageId *pageId, u16 *offset, byte **compressValue, u16 *compressSize, byte **extractedValue, u16 *extractedSize, u16 *leftItems, u16 *miniPageNo, u64 *origLSN) {
	UNREFERENCED_PARAMETER(origLSN);
	Stream s((byte*)log, size);

	try {
		s.skip(sizeof(IDX_LOG_TYPE));

		s.read(pageId);
		s.read(offset);
		s.read(compressSize);
		*compressValue = (byte*)log + s.getSize();
		s.skip(*compressSize);
		s.read(extractedSize);
		*extractedValue = (byte*)log + s.getSize();
		s.skip(*extractedSize);
		s.read(leftItems);
		s.read(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}

/** 解析索引页面合并某个MP操作的日志
 * @param log			日志内容
 * @param size			日志长度
 * @param pageId		out 索引页面ID信息
 * @param offset		out 合并点的页内偏移
 * @param compressValue	out 合并之后压缩的键值内容
 * @param compressSize	out 合并之后压缩的键值长度
 * @param originalValue	out 合并前未压缩键值内容
 * @param originalSize	out 合并前为压缩键值长度
 * @param originalMPKeyCounts	out 合并之前左边MP包含的键值个数
 * @param miniPageNo	out 合并之后的MP号
 * @param origLSN		out 页面原来的LSN信息
 */
void IndexLog::decodePageMergeMP( const byte *log, uint size, PageId *pageId, u16 *offset, byte **compressValue, u16 *compressSize, byte **originalValue, u16 *originalSize, u16 *originalMPKeyCounts, u16 *miniPageNo, u64 *origLSN ) {
	UNREFERENCED_PARAMETER(origLSN);
	Stream s((byte*)log, size);

	try {
		s.skip(sizeof(IDX_LOG_TYPE));

		s.read(pageId);
		s.read(offset);
		s.read(compressSize);
		*compressValue = (byte*)log + s.getSize();
		s.skip(*compressSize);
		s.read(originalSize);
		*originalValue = (byte*)log + s.getSize();
		s.skip(*originalSize);
		s.read(originalMPKeyCounts);
		s.read(miniPageNo);
#ifdef NTSE_VERIFY_EX
		s.read(origLSN);
#endif
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析DML操作结束日志
 * @param log	日志
 * @param size	日志长度
 * @param succ	OUT	日志内容包括的DML操作成功与否信息
 * @return
 */
void IndexLog::decodeDMLUpdateEnd(const byte *log, uint size, bool *succ) {
	Stream s((byte*)log, size);
	try {
		s.skip(sizeof(u64));
		s.read(succ);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}


/**
 * 解析创建索引日志
 * @param log			日志
 * @param size			日志长度
 * @param indexId		OUT	索引ID
 */
void IndexLog::decodeCreateIndex(const byte *log, uint size, u8 *indexId) {
	Stream s((byte*)log, size);

	try {
		s.read(indexId);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}



/**
 * 解析创建索引结束日志
 * @param log			日志
 * @param size			日志长度
 * @param indexDef		索引定义
 * @param indexId		OUT	索引ID
 * @param successful	OUT 创建索引是否成功
 */
void IndexLog::decodeCreateIndexEnd(const byte *log, uint size, IndexDef *indexDef, u8 *indexId, bool *successful) {
	Stream s((byte*)log, size);

	try {
		s.read(indexId);
		s.skip(sizeof(u64));
		s.read(successful);
		u32 realSize = 0;
		s.read(&realSize);
		indexDef->readFromKV(s.currPtr(), realSize);
	} catch (NtseException) { NTSE_ASSERT(false); }
}


/**
 * 解析DML操作过程中，当前第几个索引被完整修改过
 * @param log		日志内容
 * @param size		日志长度
 * @param indexNo	out 当前日志记录的索引编号
 */
void IndexLog::decodeDMLDoneUpdateIdxNo(const byte *log, uint size, u8 *indexNo) {
	Stream s((byte*)log, size);
	try {
		s.read(indexNo);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}



/**
 * 记录索引DML操作补偿日志
 * @param session	会话句柄
 * @param prevLsn	对应undo日志LSN
 * @param log		对应undo日志内容
 * @param size		日志长度
 * @return 日志LSN
 */
u64 IndexLog::logDMLUpdateCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_DML_CPST, m_tableId, prevLsn, log, size);
}


/**
 * 记录索引SMO补偿日志
 * @param session	会话句柄
 * @param prevLsn	对应undo日志LSN
 * @param log		对应undo日志内容
 * @param size		日志长度
 * @return 日志LSN
 */
u64 IndexLog::logSMOCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_SMO_CPST, m_tableId, prevLsn, log, size);
}


/**
 * 记录索引页面修改补偿日志
 * @param session	会话句柄
 * @param prevLsn	对应undo日志LSN
 * @param log		对应undo日志内容
 * @param size		日志长度
 * @return 日志LSN
 */
u64 IndexLog::logPageSetCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_SET_PAGE_CPST, m_tableId, prevLsn, log, size);
}


/**
 * 记录创建索引补偿日志
 * @param session	会话句柄
 * @param prevLsn	对应undo日志LSN
 * @param log		对应undo日志内容
 * @param size		日志长度
 * @return 日志LSN
 */
u64 IndexLog::logCreateIndexCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_ADD_INDEX_CPST, m_tableId, prevLsn, log, size);
}


/**
 * 得到日志的具体类型
 * @pre	上层需保证该日志确实记录了索引模块记录的类型，否则会取得错误的类型
 * @param log	日志
 * @Param size	日之长度
 * @return 日志类型
 */
IDXLogType IndexLog::getType(const byte *log, uint size) {
	assert(size > sizeof(IDX_LOG_TYPE));
	Stream s((byte*)log, size);

	IDX_LOG_TYPE type;
	s.read(&type);

	return (IDXLogType)type;
}


/**
 * 记录索引创建开始日志
 * @param session	会话句柄
 * @param indexId	创建的索引ID
 * @return 日志LSN
 */
u16 IndexLog::getCommonPostfix(const byte *data1, u16 size1, const byte *data2, u16 size2) {
	u16 mostCommon = (size1 > size2) ? size2 : size1;
	if (mostCommon == 0)
		return 0;

	u16 commonPostfix = 0;
	byte *end1 = (byte*)data1 + size1 - 1;
	byte *end2 = (byte*)data2 + size2 - 1;
	while (*end1-- == *end2-- && commonPostfix < mostCommon)
		++commonPostfix;

	return commonPostfix;
}

/**
 * 在更新操作当中，如果更新非为一性索引，在删除操作执行完之后，需要记录该日志
 * @param session 会话句柄
 * @return 日志LSN
 */
u64 IndexLog::logDMLDeleteInUpdateDone(Session *session) {
	return session->writeLog(LOG_IDX_DIU_DONE, m_tableId, NULL, 0);
}

}


