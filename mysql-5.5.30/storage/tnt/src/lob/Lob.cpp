/**
 * ��������
 *
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */
#include "lob/Lob.h"
#include "lob/BigLob.h"
#include "lob/SmallLob.h"
#include "heap/Heap.h"
#include "api/Database.h"
#include "misc/Buffer.h"
#include "util/File.h"
#include "util/Stream.h"
#include "misc/Session.h"
#include "misc/Syslog.h"
#include "misc/LockManager.h"
#include "api/Table.h"
#include <cassert>
#include <iostream>
#include <string>
#include "misc/Profile.h"
#include "heap/VariableLengthRecordHeap.h"


#ifdef TNT_ENGINE
#include "trx/TNTTransaction.h"
#endif

class TableDef;
namespace ntse {

#ifdef NTSE_UNIT_TEST
File* LobStorage::getIndexFile(){
	return m_blob->getIndexFile();

}
File* LobStorage::getBlobFile() {
	return m_blob->m_file;

}
#endif

/**
 * �������ڴ洢С�ʹ����ĶѶ���
 *
 * @return ���ڴ洢С�ʹ����ĶѶ���
 */
DrsHeap* LobStorage::getSLHeap() const {
	return m_slob->getHeap();
}

/**
 * �������ڻ���С�ʹ�����MMS�����
 *
 * @return ���ڻ���С�ʹ�����MMS�����
 */
MmsTable* LobStorage::getSLMmsTable() const {
	return m_slob->getMmsTable();
}

/** 
 * �����洢�๹�캯��
 *
 * @param db �������ݿ�
 * @param tableDef ����
 * @param slob С�ʹ����洢
 * @param blob ���ʹ����洢
 * @param lzoOper lzoѹ��
 */
LobStorage::LobStorage(Database *db, const TableDef *tableDef, SmallLobStorage *slob, BigLobStorage *blob, LzoOper *lzoOper) {
	m_blob = blob;
	m_db = db;
	m_tableDef = tableDef;
	m_slob = slob;
	m_maxSmallLobLen = Limits::MAX_REC_SIZE - m_slob->getLenColumnBytes();
	m_lzo = lzoOper;
	memset(&m_status, 0, sizeof(LobStatus));
	memset(&m_statusEx, 0, sizeof(LobStatusEx));
}

/** 
 * �����洢����������
 */
LobStorage::~LobStorage () {
	delete m_slob;
	delete m_blob;
	delete m_lzo;
}

/**
 * ����һ�������洢���������ڴ������д�����ֶεı�ʱ����,��Ҫ
 * ���ɴ��ʹ�����ļ���Ŀ¼�ļ����Լ�С�ʹ�����ļ�
 *
 * @param db ���ݿ�
 * @param tableDef ��������
 * @param path �ļ�·������������׺��
 * @throw NtseException �ļ����������
 */
void LobStorage::create(Database *db, const TableDef *tableDef, const char *path) throw(NtseException) {
	ftrace(ts.lob, tout << db << tableDef << path);
	// ����С�ʹ�����ļ�
	SmallLobStorage::create(db, tableDef, tableDef->m_id, path);

	// �������ʹ����
	try {
		BigLobStorage::create(db, tableDef, path);
	} catch (NtseException &e) {
		// ������Ҫɾ��ǰ���Ѿ��ɹ�create��С�ʹ�����ļ�
		SmallLobStorage::drop(path);
		throw e;
	}
}

/**
 * ��һ�������洢���ڱ��һ�α�ʹ��ʱ����
 *
 * @param db ���ݿ�
 * @param session �Ự����
 * @param tableDef ����
 * @param path �ļ�·������������׺��
 * @param useMms ��С�ʹ�����Ƿ�ʹ��mms
 * @return �����洢
 * @throw NtseException �ļ����������
 */
LobStorage* LobStorage::open(Database *db, Session *session, const TableDef *tableDef, const char *path, bool useMms) throw(NtseException) {
	ftrace(ts.lob, tout << db << session << tableDef << path << useMms);
	// ��������SmallLob����
	SmallLobStorage *slob = NULL;

	slob = SmallLobStorage::open(db, session, path, useMms);

	// ����BigLobStorage����
	BigLobStorage *blob = NULL;
	try {
		blob = BigLobStorage::open(db, session, path);
	} catch (NtseException &e) {
		slob->close(session, false);
		delete slob;
		slob = NULL;
		throw e;
	}

	LzoOper *lzo_Oper = new LzoOper();
	return new LobStorage(db, (TableDef *)tableDef, slob, blob, lzo_Oper);
}

/**
 * ����һ�������
 *
 * @param session �Ự����
 * @param lob ���������
 * @param size ������С
 * @param compress �Ƿ�Ӧ�洢Ϊѹ����ʽ
 * @return �����ID
 */
LobId LobStorage::insert(Session *session, const byte *lob, u32 size, bool compress) {
	ftrace(ts.lob, tout << session << lob << size << compress);
	assert(lob != NULL);

	PROFILE(PI_LobStorage_insert);

	LobId lid = INVALID_LOB_ID;
	bool needCompress = NEED_COMPRESS(compress, size);
	u32 realSize = size;
	byte *realData = (byte *)lob;

	if (needCompress) {
		MemoryContext *mc = session->getMemoryContext();
		uint outLen = MAX_COMPRESSED_SIZE(size);
		byte *out = (byte *)mc->alloc(outLen);
		uint compressRet = m_lzo->compress(lob, size, out, &outLen);
		if (compressRet == 0) {
			realSize = outLen;
			realData = out;
			m_status.m_usefulCompress += 1;
		} else {
			m_status.m_uselessCompress += 1;
		}
	}
	m_status.m_preCompressSize += size;
	m_status.m_postCompressSize += realSize;
		
	if (realSize <= m_maxSmallLobLen) {
		lid = m_slob->insert(session, realData, realSize, size);
	} else {
		lid = m_blob->insert(session, m_tableDef, realData, realSize, size);
	}
	nftrace(ts.lob, tout << lid);
	m_status.m_lobInsert++;
	return lid;
}


/**
 * ��ȡһ������������
 *
 * @param session �Ự����
 * @param mc ���ڷ��䷵��ֵ�洢�ռ���ڴ����������
 * @param lobId Ҫ��ȡ�Ĵ�����ID
 * @param size �����������ȡ�Ĵ����Ĵ�С
 * @param intoMms ����С�ʹ�����Ƿ����mms��
 * @return �ɹ����ش�������ݣ�ָ���Ĵ���󲻴��ڷ���NULL
 */
byte* LobStorage::get(Session *session, MemoryContext *mc, LobId lobId, uint *size, bool intoMms) {
	ftrace(ts.lob, tout << session << mc << lobId << intoMms);

	PROFILE(PI_LobStorage_get);

	uint orgSize;
	byte *data;
	if (!IS_BIG_LOB(lobId)) {
		data = m_slob->read(session, mc, lobId, size, intoMms, &orgSize);
		if (data == NULL) {
			assert(*size == 0);
			return NULL;
		}
	} else {
		data = m_blob->read(session, mc, lobId, size, &orgSize);
	}

	nftrace(ts.lob, tout << lobId << size << orgSize);
	if (*size != orgSize) {
		assert (*size < orgSize);
		uint outSize;
		byte *out = (byte *)mc->alloc(orgSize);
		bool isSuccess = m_lzo->decompress(data, *size, out, &outSize);
		assert_always(isSuccess && outSize == orgSize);
		*size = outSize;
		return out;
	}
	return data;
}

/**
 * ɾ��һ�������
 *
 * @param session �Ự����
 * @param lobId Ҫɾ���Ĵ����ID
 */
void LobStorage::del(Session *session, LobId lobId) {
	ftrace(ts.lob, tout << session << lobId);

	PROFILE(PI_LobStorage_del);

	if (!IS_BIG_LOB(lobId)) {
		m_slob->del(session, lobId);
	} else {
		m_blob->del(session, lobId);
	}
	m_status.m_lobDelete++;
}

#ifdef TNT_ENGINE
/**
 * ɾ��һ�������
 *
 * @param session �Ự����
 * @param lobId Ҫɾ���Ĵ����ID
 */
void LobStorage::delAtCrash(Session *session, LobId lobId) {
	ftrace(ts.lob, tout << session << lobId);

	PROFILE(PI_LobStorage_del);

	if (!IS_BIG_LOB(lobId)) {
		m_slob->delAtCrash(session, lobId);
	} else {
		m_blob->delAtCrash(session, lobId);
	}
	m_status.m_lobDelete++;
}

#endif
/**
 * ����һ�������
 *
 * @param session �Ự����
 * @param lobId Ҫ���µĴ�����ID
 * @param lob �´��������
 * @param size �´�������ݴ�С
 * @param compress �Ƿ�Ӧ�洢Ϊѹ����ʽ
 * @return �µĴ����ID��С�ʹ������±�ɴ��ʹ������Ƿ�ѹ�������仯ʱ�������ID�ᷢ���仯��
 */
LobId LobStorage::update(Session *session, LobId lobId, const byte *lob, uint size, bool compress) {
	ftrace(ts.lob, tout << session << lobId << lob << size);
	PROFILE(PI_LobStorage_update);

	bool needCompress = NEED_COMPRESS(compress, size);
	u32  realSize = size;
	byte *realData = (byte *)lob;
	LobId newLobId = lobId;

	if (needCompress) {
		MemoryContext *mc = session->getMemoryContext();
		uint outLen = MAX_COMPRESSED_SIZE(size);
		byte *out = (byte *)mc->alloc(outLen);
		uint compressRet = m_lzo->compress(lob, size, out, &outLen);
		if (compressRet == 0) { // ѹ���ɹ�
			realSize = outLen;
			realData = out;
			m_status.m_usefulCompress += 1;
		} else
			m_status.m_uselessCompress += 1;
		m_status.m_preCompressSize += size;
		m_status.m_postCompressSize += realSize;
	}

	if (IS_BIG_LOB(lobId)) {
		m_blob->update(session, m_tableDef, lobId, realData, realSize, size);
	} else {
		if (realSize > m_maxSmallLobLen) {
			m_slob->del(session, lobId);
			newLobId = m_blob->insert(session, m_tableDef, realData, realSize, size);
		} else {
			m_slob->update(session, lobId, realData, realSize, size);
		}
	}
	nftrace(ts.lob, tout << "update finish, the new ID is " << newLobId);
	m_status.m_lobUpdate++;
	return newLobId;
}

/**
 * ɾ�������洢���ڱ�ɾ��ʱ����
 *
 * @param path �ļ�·������������׺��
 * @throw NtseException �ļ����������
 */
void LobStorage::drop(const char *path) throw(NtseException) {
	ftrace(ts.lob, tout << path);

	SmallLobStorage::drop(path);

	// ɾ�����ʹ�����ļ�
	BigLobStorage::drop(path);
}

/**
 * �رմ����洢
 *
 * @param session �Ự����
 * @param flushDirty �Ƿ�д��������
 */
void LobStorage::close(Session *session, bool flushDirty) {
	ftrace(ts.lob, tout << session << flushDirty);

	if (m_slob)
		m_slob->close(session, flushDirty);
	if (m_blob)
		m_blob->close(session, flushDirty);
	delete m_slob;
	delete m_blob;
	m_slob = NULL;
	m_blob = NULL;
	delete m_lzo;
	m_lzo = NULL;
}

/**
 * ������Ƭ����
 *
 * @param session �Ự
 */
void LobStorage::defrag(Session *session) {
	ftrace(ts.lob, tout << session);

	m_blob->defrag(session, m_tableDef);
}

/** ���ñ�ID
 * @param session �Ự
 * @param tableId �µı�ID
 */
void LobStorage::setTableId(Session *session, u16 tableId) {
	m_slob->setTableId(session, tableId);
	m_blob->setTableId(session, tableId);
}

/**
 * ���ϻָ�ʱREDO���������
 *
 * @param db ���ݿ����
 * @param session �Ự
 * @param tableDef ����
 * @param path �ļ�·����������׺��
 * @param tableid ��ID
 * @throw NtseException �ļ����������
 */
void LobStorage::redoCreate(Database *db, Session *session, const TableDef *tableDef, const char *path, u16 tableId) throw(NtseException) {
	ftrace(ts.lob | ts.recv, tout << db << session << tableDef << path << tableId);

	SmallLobStorage::redoCreate(db, session, path, tableId);

	try {
		BigLobStorage::redoCreate(db, tableDef, path, tableId);
	} catch (NtseException &e) {
		// ���粻�ɹ�����Ҫ����С�ʹ�����ļ�
		SmallLobStorage::drop(path);
		throw e;
	}
}

/**
 * ���ϻָ�ʱREDOС�ʹ����������
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param logSize ��־��С
 * @return �����ID
 */
LobId LobStorage::redoSLInsert(Session *session, u64 lsn, const byte *log, uint logSize) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << logSize);

	return m_slob->redoInsert(session, lsn, log, logSize);
}

/**
 * ���ϻָ�ʱREDO���ʹ����������
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param logSize ��־��С
 * @return �����ID
 */
LobId LobStorage::redoBLInsert(Session *session, u64 lsn, const byte *log, uint logSize) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << logSize);

	return m_blob->redoInsert(session, m_tableDef, lsn, log, logSize);
}

/**
 * ���ϻָ�ʱREDOС�ʹ����ɾ������
 *
 * @param session �Ự����
 * @param lobId ��ɾ�������ID
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 */
void LobStorage::redoSLDelete(Session *session, LobId lobId, u64 lsn, const byte *log, uint size) {
	ftrace(ts.lob | ts.recv, tout << session << lobId << lsn << log << size);

	m_slob->redoDelete(session, lobId, lsn, log, size);
}

/**
 * ���ϻָ�ʱREDO���ʹ����ɾ������
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 */
void LobStorage::redoBLDelete(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << size);

	m_blob->redoDelete(session, lsn, log, size);
}

/**
 * ���ϻָ�ʱREDOС�ʹ��������MMS�ĸ��²���
 *
 * @param session �Ự����
 * @param lobId �����´����ID
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 * @param lob Ԥ���µ�����(���������)
 * @param lobSize ����󳤶�
 * @param compress �Ƿ�Ӧ�洢Ϊѹ����ʽ
 * @return �����ID
 */
LobId LobStorage::redoSLUpdateHeap(Session *session, LobId lobId, u64 lsn, const byte *log, uint size,
							  const byte *lob, uint lobSize, bool compress) {
	ftrace(ts.lob | ts.recv, tout << session << lobId << lsn << log << size << lob << lobSize);

	bool needCompress = NEED_COMPRESS(compress, lobSize);
	u32 realSize = lobSize;
	byte *realData = (byte *)lob;

	if (needCompress) {
		uint outLen = MAX_COMPRESSED_SIZE(lobSize);
		byte *out = (byte *)session->getMemoryContext()->alloc(outLen);
		// ����ѹ����һ����ɹ�����Ϊ������redo
		if (!m_lzo->compress(lob, lobSize, out, &outLen)) {
			realSize = outLen;
			realData = out;
		}
		m_status.m_preCompressSize += lobSize;
		m_status.m_postCompressSize += realSize;
	}
	m_slob->redoUpdateHeap(session, lobId, lsn, log, size, realData, realSize, lobSize);
	return lobId;
}

/**
 * ���ϻָ�ʱREDOС�ʹ��������MMSʱ�ĸ��²���
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 */
void LobStorage::redoSLUpdateMms(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << size);

	m_slob->redoUpdateMms(session, lsn, log, size);
}

/**
 * ���ϻָ�ʱREDO���ʹ������²���
 *
 * @param session �Ự����
 * @param lobId �ɵĴ����ID
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param logSize ��־��С
 * @param lob Ԥ���µ�����(���������)
 * @param lobSize ����󳤶�
 * @param compress �Ƿ�ѹ��
 */
void LobStorage::redoBLUpdate(Session *session, LobId lobId, u64 lsn, const byte *log, uint logSize, const byte *lob, uint lobSize, bool compress) {
	ftrace(ts.lob | ts.recv, tout << session << lobId << lsn << log << logSize << lob << lobSize);

	bool needCompress = NEED_COMPRESS(compress, lobSize);
	u32 realSize = lobSize;
	byte *realData = (byte *)lob;

	if (needCompress) {
		uint outLen = MAX_COMPRESSED_SIZE(lobSize);
		byte *out = (byte *)session->getMemoryContext()->alloc(outLen);
		// ����ѹ����һ����ɹ�����Ϊ������redo
		if (!m_lzo->compress(lob, lobSize, out, &outLen)) {
			realSize = outLen;
			realData = out;
		}
		m_status.m_preCompressSize += lobSize;
		m_status.m_postCompressSize += realSize;
	}
	m_blob->redoUpdate(session, m_tableDef, lobId, lsn, log, logSize, realData, realSize);
}

/**
 * ���ϻָ�ʱREDO���ʹ�����ƶ�����
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param logSize ��־��С
 */
void LobStorage::redoMove(Session *session, u64 lsn, const byte *log, uint logSize) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << logSize);

	m_blob->redoMove(session, m_tableDef, lsn, log, logSize);
}

/**
 * �õ������ģ�����õ��ļ�
 *
 * @param files in/out ��ģ�������Fileָ�����飬�ռ�����߷���
 * @param pageTypes in/out File��Ӧ��ҳ����
 * @param numFile files�����pageTypes���鳤��
 * @return File�������
 */
int LobStorage::getFiles(File** files, PageType* pageTypes, int numFile) {
	assert(numFile >= 3);
	int bigFiles = m_blob->getFiles(files, pageTypes, numFile);
	int smallFiles = m_slob->getFiles(files + bigFiles, pageTypes + bigFiles, numFile - bigFiles);
	return bigFiles + smallFiles;
}

/**
 * ˢ��������
 *
 * @param session �Ự
 */
void LobStorage::flush(Session *session) {
	ftrace(ts.lob, tout << session);

	m_slob->flush(session);
	m_blob->flush(session);
}

/**
 * ��ȡ�����Ļ���ͳ����Ϣ
 *
 * @return ��������ͳ����Ϣ
 */
const LobStatus& LobStorage::getStatus(){
	m_status.m_slobStatus = m_slob->getStatus();
	m_status.m_blobStatus = m_blob->getStatus();
	return m_status;
}

/**
 * ��ȡ��������չͳ����Ϣ
 *
 * @return �������չͳ����Ϣ
 */
const LobStatusEx& LobStorage::getStatusEx() {
	m_statusEx.m_slobStatus = m_slob->getStatusEx();
	m_statusEx.m_blobStatus = m_blob->getStatusEx();
	return m_statusEx;
}

/**
 * ���´�������չͳ����Ϣ
 *
 * @param session �Ự
 * @param maxSamplePages ��������ô���ҳ��
 * @return ���´������չͳ����Ϣ
 */
void LobStorage::updateExtendStatus(Session *session, uint maxSamplePages) {
	ftrace(ts.lob, tout << session << maxSamplePages);

	m_slob->updateExtendStatus(session, maxSamplePages);
	m_blob->updateExtendStatus(session, maxSamplePages);
}

/** 
 * ���ش��ʹ����Ŀ¼�ļ���ͳ����Ϣ
 *
 * @return ���ʹ����Ŀ¼�ļ���ͳ����Ϣ
 */
DBObjStats* LobStorage::getLLobDirStats() {
	return m_blob->getDirStats();
}

/** 
 * ���ش��ʹ���������ļ���ͳ����Ϣ
 *
 * @return ���ʹ���������ļ���ͳ����Ϣ
 */
DBObjStats* LobStorage::getLLobDatStats() {
	return m_blob->getDatStats();
}

/** 
 * ���ñ���
 *
 * @param tableDef ����
 */
void LobStorage::setTableDef(const TableDef *tableDef) {
	m_tableDef = tableDef;
}

const TableDef* LobStorage::getSLVTableDef() {
	return m_slob->getVTableDef();
}

/**
 * ����������־
 *
 * @param log ��־��
 * @param lid ������LobId
 * @param origLen out δѹ�������ݴ�С
 * @param mc ����������ݻ������õ�MemoryContext
 * @return  ���������ѹ����ʱ�򣬷���ָ����־�����еĴ�������ݵ�ָ��
 *          ���������ѹ����ʱ�򣬽�ѹ��������ݷŵ�buf�У�����buf
 */
byte * LobStorage::parseInsertLog(const LogEntry *log, LobId lid, size_t *origLen, MemoryContext *mc) {
	bool biglob = IS_BIG_LOB(lid);

	byte *cdata; // compressed data
	u32 clen; // compressed data length

	if (biglob) {
		assert(log->m_logType == LOG_LOB_INSERT);
		LobId lobid;
		u32 pageId, newListHeader, oLen;
		bool flags;

		Stream s(log->m_data, log->m_size);

		s.read(&lobid)->read(&pageId)->read(&flags);
		assert(lobid == lid);
		if (flags) 
			s.read(&newListHeader);
		s.read(&oLen); // ����ԭʼ��С
		s.read(&clen);
		*origLen = oLen;
		cdata = (byte *)(log->m_data + s.getSize());
	} else {
		assert(log->m_logType == LOG_HEAP_INSERT); // ����Ѳ���
		Record outRec;
		outRec.m_format = REC_VARLEN;
		VariableLengthRecordHeap::getRecordFromInsertlog((LogEntry *)log, &outRec);
		size_t compressedLen;
		cdata = RecordOper::extractSlobData(m_slob->getVTableDef(), &outRec, &compressedLen, origLen); // �������origLen
		clen = (u32)compressedLen;
		assert(lid == outRec.m_rowId);
	}

	if (cdata && clen != *origLen) {
		uint outSize;
		byte *buf = (byte *)mc->alloc(*origLen);
		bool success = m_lzo->decompress(cdata, (uint)clen, buf, &outSize);
		assert_always(success && outSize == *origLen);
		return buf;
	}
	return cdata;
}

/** 
* ����MMS��
*
* @param session �Ự
* @param useMms ʹ��MMS
* @param flushDirty �Ƿ�ˢд���¼
*/
void LobStorage::setMmsTable(Session *session, bool useMms, bool flushDirty) {
	if (m_slob)
		m_slob->setMmsTable(session, useMms, flushDirty);
}


#ifdef TNT_ENGINE
/** insert Lob����־
 * @param session �����Ự
 * @param txnId purge��������������id
 * @param preLsn ͬһ����ǰһ����־��lsn
 * @param lobId ���������id
 * return ��־�����к�
 */
u64 LobStorage::writeTNTInsertLob(Session *session, TrxId txnId,  u16 tableId, u64 preLsn, LobId lobId) {
	u64 lsn = 0;
	MemoryContext *ctx = session->getMemoryContext();
	McSavepoint msp(ctx);
	size_t size = sizeof(txnId) + sizeof(preLsn) + sizeof(lobId);
	byte *buf = (byte *)ctx->alloc(size);
	Stream s(buf, size);
	s.write(txnId);
	s.write(preLsn);
	s.write(lobId);
	lsn = session->getTrans()->writeTNTLog(TNT_UNDO_LOB_LOG, tableId, buf, s.getSize());
	return lsn;
}

/** ���������������־
 * @param log ��������־����
 * @param txnId out ���������������������id
 * @param preLsn out ͬһ����ǰһ����־��lsn
 * @param lobId out ���������id
 */
void LobStorage::parseTNTInsertLob(const LogEntry *log, TrxId *txnId, u64 *preLsn, LobId *lobId) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.read(lobId);
}
#endif

}