/**
 * NTSE B+��������־��ʵ��
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
 * ��¼����������ʼ��־
 * @param session	�Ự���
 * @param indexId	����������ID
 * @return ��־LSN
 */
u64 IndexLog::logCreateIndexBegin(Session *session, u8 indexId) {
	return session->writeLog(LOG_IDX_CREATE_BEGIN, m_tableId, (byte*)&indexId, sizeof(u8));
}


/**
 * ��¼��������������־
 * @param session		�Ự���
 * @param indexDef		��������
 * @param beginLSN		������ʼ��־LSN
 * @param indexId		����ID
 * @param successful	���������Ƿ�ɹ�
 * @return ��־LSN
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
 * ��¼�����޸Ŀ�ʼ��־
 * @param session	�Ự���
 * @return ��־LSN
 */
u64 IndexLog::logDMLUpdateBegin(Session *session) {
	return session->writeLog(LOG_IDX_DML_BEGIN, m_tableId, NULL, 0);
}


/**
 * ��¼�����޸Ľ�����־
 * @param session	�Ự���
 * @param beginLSN	�����޸Ŀ�ʼ��־LSN
 * @param succ		�����޸��Ƿ�ɹ�
 * @return ��־LSN
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
 * ��DML���̵��м�¼ĳ����������ظ��²�����������
 * @param session	�Ự���
 * @param indexNo	�������
 * @return ��־LSN
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
 * ��¼����������־
 * @param session	�Ự���
 * @param indexId	����������ID
 * @param idxNo		�������к�
 * @return ��־LSN
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
 * ��¼���������޸Ĳ�����־
 * @param session		�Ự���
 * @param type			�޸����ͣ�INSERT/DELETE/APPEND
 * @param pageId		�޸�ҳ���ID
 * @param offset		�޸ĵ�ҳ����ʼƫ����
 * @param miniPageNo	�޸��漰��MiniPage��
 * @param oldValue		�޸�֮ǰ��ҳ������
 * @param oldSize		�޸�֮ǰҳ�����ݵĳ���
 * @param newValue		�޸�֮���ҳ������
 * @param newSize		�޸�֮��Ҳ�����ݵĳ���
 * @param origLSN		��¼��־ҳ���lsn
 * @return ��־LSN
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
 * ��¼����ҳ��SMO������־
 * @param session		�Ự���
 * @param type			����SMO���ͣ�SMOMerge�����ڽ�����־
 * @param pageId		����SMO��ԭʼҳ��ID
 * @param mergePageId	Ҫ�ϲ���ҳ��ID��ԭҳ�����ҳ��
 * @param prevPageId	�ϲ������ǰһ��ҳ��ID
 * @param moveData		�ϲ������ƶ�������
 * @param dataSize		���ݳ���
 * @param moveDir		�ϲ������ƶ�����Ŀ¼
 * @Param dirSize		��Ŀ¼����
 * @param origLSN1		pageId��Ӧҳ���ԭʼLSN
 * @param origLSN2		mergePageId��Ӧҳ��ԭʼLSN
 * @return ��־LSN
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
 * ��¼����SMO������־
 * @param session		�Ự���
 * @param type			����SMO���ͣ�SMOSplit�����ڽ�����־
 * @param pageId		����SMO��ԭʼҳ��ID
 * @param newPageId		ʹ�õ���ҳ��ID
 * @param nextPageId	ԭʼҳ�����ǰ�ĺ��ҳ��
 * @param moveData		���ѹ����ƶ�������
 * @param dataSize		���ݳ���
 * @param oldSplitKey	���Ѽ�ֵԭʼ����
 * @param oldSKLen		���Ѽ�ֵԭʼ����
 * @param newSplitKey	���Ѽ�ֵ������
 * @param newSKLen		���Ѽ�ֵ�³���
 * @param moveDir		���ѹ����ƶ�����Ŀ¼
 * @param dirSize		��Ŀ¼����
 * @param mpLeftCount	����MP����ԭʼҳ�������
 * @param mpMoveCount	����MP�Ƶ���ҳ�������
 * @param origLSN1		pageId��Ӧҳ���ԭʼLSN
 * @param origLSN2		newPageId��Ӧҳ��ԭʼLSN
 * @return ��־LSN
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
 * ��¼����ҳ�������־
 * @param session			�Ự���
 * @param type				��֮��������
 * @param pageId			�޸ĵ�ҳ��ID
 * @param offset			�޸���ʼƫ��
 * @param newValue			�޸ĺ��ҳ������
 * @param oldVlaue			�޸�ǰ��ҳ������
 * @param size				�޸����ݵĳ���
 * @param origLSN			pageIdҳ��ԭʼLSN
 * @param clearPageFirst	�ָ�֮ǰ���Ƚ�ҳ������
 * @return ��־LSN
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
 * ��¼����ҳ�����һ��MiniPage�������ֵ����
 * @param session		�Ự���
 * @param type			��־��������
 * @param pageId		�޸ĵ�ҳ��ID
 * @param keyValue		�����ļ�ֵ����
 * @param dataSize		������ֵ����
 * @param miniPageNo	�´�����MiniPage��
 * @param origLSN		pageIdҳ��ԭʼLSN
 * @return ��־LSN
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
 * ��¼����ҳ��ɾ��һ��MiniPage����
 * @param session		�Ự���
 * @param type			��־��������
 * @param pageId		�޸ĵ�ҳ��ID
 * @param keyValue		ɾ��ǰ�ļ�ֵ����
 * @param size			ɾ��ǰ��ֵ����
 * @param miniPageNo	�´�����MiniPage��
 * @param origLSN		pageIdҳ��ԭʼLSN
 * @return ��־LSN
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
 * ��¼����ҳ�����ĳ��MiniPage����
 * @param session		�Ự���
 * @param type			��־��������
 * @param pageId		�޸ĵ�ҳ��ID
 * @param offset		���ѵ��ҳ��ƫ��
 * @param compressValue	���ѵ��ֵ��ѹ����ʽ
 * @param compressSize	ѹ����ʽ��ֵ����
 * @param extractedValue���Ѻ�ǰ����ֵ�ĸ�ʽ
 * @param extractedSize	���Ѻ��ֵ�ĳ���
 * @param leftItems		���Ѻ���ԭMinipageʣ�������
 * @param miniPageNo	Ҫ���ѵ�Minipage��
 * @param origLSN		pageIdҳ��ԭʼLSN
 * @return ��־LSN
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

/** ��¼����ҳ��ϲ�ĳ��MP��������־
 * @param session		�Ự
 * @param type			��־��������
 * @param pageId		����ҳ��ID��Ϣ
 * @param offset		�ϲ����ҳ��ƫ��
 * @param compressValue	�ϲ�֮��ѹ���ļ�ֵ����
 * @param compressSize	�ϲ�֮��ѹ���ļ�ֵ����
 * @param originalValue	�ϲ�ǰδѹ����ֵ����
 * @param originalSize	�ϲ�ǰΪѹ����ֵ����
 * @param miniPageNo	�ϲ�֮���MP��
 * @param originalMPKeyCounts	�ϲ�֮ǰ���MP�����ļ�ֵ����
 * @param origLSN		ҳ��ԭ����LSN��Ϣ
 * @return ���ؼ�¼��־��LSN
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
 * ��������������־
 * @param log		��־
 * @param size		��־����
 * @param indexId	OUT	����������ID
 * @param idxNo		OUT	���������������к�
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
 * ���������޸Ĳ�����־
 * @param log			��־
 * @param size			��־����
 * @param type			OUT	�޸����ͣ�INSERT/DELETE/APPEND
 * @param pageId		OUT	�޸�ҳ���ID
 * @param offset		OUT	�޸ĵ�ҳ����ʼƫ����
 * @param miniPageNo	OUT	�޸��漰��MiniPage��
 * @param oldValue		OUT	�޸�֮ǰ��ҳ������
 * @param oldSize		OUT	�޸�֮ǰҳ�����ݵĳ���
 * @param newValue		OUT	�޸�֮���ҳ������
 * @param newSize		OUT	�޸�֮��ҳ�����ݵĳ���
 * @param origLSN		OUT	�޸�֮ǰҳ��LSN
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
 * ��������SMO�ϲ���־
 * @param log			��־
 * @param size			��־����
 * @param pageId		OUT	����SMO��ԭʼҳ��ID
 * @param mergePageId	OUT	Ҫ�ϲ���ҳ��ID��ԭҳ�����ҳ��
 * @param prevPageId	OUT	�ϲ������ǰһ��ҳ��ID
 * @param moveData		OUT	�ϲ������ƶ�������
 * @param dataSize		OUT	���ݳ���
 * @param moveDir		OUT	�ϲ������ƶ�����Ŀ¼
 * @param dirSize		OUT	��Ŀ¼����
 * @param origLSN1		OUT pageIdҳ��ԭʼLSN
 * @param origLSN2		OUT mergePageIdҳ��ԭʼLSN
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
 * ����SMO������־
 * @param log			��־
 * @param size			��־����
 * @param pageId		OUT	����SMO��ԭʼҳ��ID
 * @param newPageId		OUT	ʹ�õ���ҳ��ID
 * @param nextPageId	OUT	ԭʼҳ�����ǰ�ĺ��ҳ��
 * @param moveData		OUT	���ѹ����ƶ�������
 * @param dataSize		OUT	���ݳ���
 * @param oldSplitKey	OUT	���ѵ���һ����ֵԭʼ����
 * @param oldSKLen		OUT	���ѵ���һ����ֵԭʼ����
 * @param newSplitKey	OUT	���ѵ���һ����ֵ������
 * @param newSKLen		OUT	���ѵ���һ����ֵ�³���
 * @param moveDir		OUT	���ѹ����ƶ�����Ŀ¼
 * @param dirSize		OUT	��Ŀ¼����
 * @param mpLeftCount	OUT	����MPʣ����ԭʼҳ�������
 * @param mpMoveCount	OUT ����MP�ƶ�����ҳ�������
 * @param origLSN1		OUT pageIdҳ��ԭʼLSN
 * @param origLSN2		OUT newPageIdҳ��ԭʼLSN
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
 * ��������ҳ�������־
 * @param log				��־
 * @param size				��־����
 * @param pageId			OUT	����SMO��ԭʼҳ��ID
 * @param offset			OUT	�޸���ʼƫ��
 * @param newValue			OUT	�޸ĺ��ҳ������
 * @param oldVlaue			OUT	�޸�ǰ��ҳ������
 * @param size				OUT	�޸����ݵĳ���
 * @param origLSN			OUT	�޸�֮ǰҳ��LSN
 * @param clearPageFirst	OUT �Ƿ�������ҳ��
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
 * ��������ҳ������MiniPage��־
 * @param log			��־
 * @param size			��־����
 * @param pageId		OUT	����SMO��ԭʼҳ��ID
 * @param keyValue		OUT	�����ļ�ֵ����
 * @param dataSize		OUT	������ֵ����
 * @param miniPageNo	OUT	�´�����MiniPage��
 * @param origLSN		OUT	�޸�֮ǰҳ��LSN
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
 * ��������ҳ��ɾ��MiniPage��־
 * @param log			��־
 * @param size			��־����
 * @param pageId		OUT	����SMO��ԭʼҳ��ID
 * @param keyValue		OUT	�����ļ�ֵ����
 * @param dataSize		OUT	������ֵ����
 * @param miniPageNo	OUT	�´�����MiniPage��
 * @param origLSN		OUT	�޸�֮ǰҳ��LSN
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
 * ��������ҳ��MiniPage��־
 * @param log			��־
 * @param size			��־����
 * @param pageId		OUT	����SMO��ԭʼҳ��ID
 * @param offset		OUT	���ѵ��ҳ��ƫ��
 * @param compressValue	OUT	���ѵ��ֵ��ѹ����ʽ
 * @param compressSize	OUT	ѹ����ʽ��ֵ����
 * @param extractedValueOUT	���Ѻ�ǰ����ֵ�ĸ�ʽ
 * @param extractedSize	OUT	���Ѻ��ֵ�ĳ���
 * @param leftItems		OUT	���Ѻ���ԭMinipage��
 * @param miniPageNo	OUT	Ҫ���ѵ�Minipage��
 * @param origLSN		OUT	�޸�֮ǰҳ��LSN
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

/** ��������ҳ��ϲ�ĳ��MP��������־
 * @param log			��־����
 * @param size			��־����
 * @param pageId		out ����ҳ��ID��Ϣ
 * @param offset		out �ϲ����ҳ��ƫ��
 * @param compressValue	out �ϲ�֮��ѹ���ļ�ֵ����
 * @param compressSize	out �ϲ�֮��ѹ���ļ�ֵ����
 * @param originalValue	out �ϲ�ǰδѹ����ֵ����
 * @param originalSize	out �ϲ�ǰΪѹ����ֵ����
 * @param originalMPKeyCounts	out �ϲ�֮ǰ���MP�����ļ�ֵ����
 * @param miniPageNo	out �ϲ�֮���MP��
 * @param origLSN		out ҳ��ԭ����LSN��Ϣ
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
 * ����DML����������־
 * @param log	��־
 * @param size	��־����
 * @param succ	OUT	��־���ݰ�����DML�����ɹ������Ϣ
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
 * ��������������־
 * @param log			��־
 * @param size			��־����
 * @param indexId		OUT	����ID
 */
void IndexLog::decodeCreateIndex(const byte *log, uint size, u8 *indexId) {
	Stream s((byte*)log, size);

	try {
		s.read(indexId);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}



/**
 * ������������������־
 * @param log			��־
 * @param size			��־����
 * @param indexDef		��������
 * @param indexId		OUT	����ID
 * @param successful	OUT ���������Ƿ�ɹ�
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
 * ����DML���������У���ǰ�ڼ��������������޸Ĺ�
 * @param log		��־����
 * @param size		��־����
 * @param indexNo	out ��ǰ��־��¼���������
 */
void IndexLog::decodeDMLDoneUpdateIdxNo(const byte *log, uint size, u8 *indexNo) {
	Stream s((byte*)log, size);
	try {
		s.read(indexNo);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == size);
}



/**
 * ��¼����DML����������־
 * @param session	�Ự���
 * @param prevLsn	��Ӧundo��־LSN
 * @param log		��Ӧundo��־����
 * @param size		��־����
 * @return ��־LSN
 */
u64 IndexLog::logDMLUpdateCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_DML_CPST, m_tableId, prevLsn, log, size);
}


/**
 * ��¼����SMO������־
 * @param session	�Ự���
 * @param prevLsn	��Ӧundo��־LSN
 * @param log		��Ӧundo��־����
 * @param size		��־����
 * @return ��־LSN
 */
u64 IndexLog::logSMOCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_SMO_CPST, m_tableId, prevLsn, log, size);
}


/**
 * ��¼����ҳ���޸Ĳ�����־
 * @param session	�Ự���
 * @param prevLsn	��Ӧundo��־LSN
 * @param log		��Ӧundo��־����
 * @param size		��־����
 * @return ��־LSN
 */
u64 IndexLog::logPageSetCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_SET_PAGE_CPST, m_tableId, prevLsn, log, size);
}


/**
 * ��¼��������������־
 * @param session	�Ự���
 * @param prevLsn	��Ӧundo��־LSN
 * @param log		��Ӧundo��־����
 * @param size		��־����
 * @return ��־LSN
 */
u64 IndexLog::logCreateIndexCPST(Session *session, u64 prevLsn, const byte *log, uint size) {
	return session->writeCpstLog(LOG_IDX_ADD_INDEX_CPST, m_tableId, prevLsn, log, size);
}


/**
 * �õ���־�ľ�������
 * @pre	�ϲ��豣֤����־ȷʵ��¼������ģ���¼�����ͣ������ȡ�ô��������
 * @param log	��־
 * @Param size	��֮����
 * @return ��־����
 */
IDXLogType IndexLog::getType(const byte *log, uint size) {
	assert(size > sizeof(IDX_LOG_TYPE));
	Stream s((byte*)log, size);

	IDX_LOG_TYPE type;
	s.read(&type);

	return (IDXLogType)type;
}


/**
 * ��¼����������ʼ��־
 * @param session	�Ự���
 * @param indexId	����������ID
 * @return ��־LSN
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
 * �ڸ��²������У�������·�Ϊһ����������ɾ������ִ����֮����Ҫ��¼����־
 * @param session �Ự���
 * @return ��־LSN
 */
u64 IndexLog::logDMLDeleteInUpdateDone(Session *session) {
	return session->writeLog(LOG_IDX_DIU_DONE, m_tableId, NULL, 0);
}

}


