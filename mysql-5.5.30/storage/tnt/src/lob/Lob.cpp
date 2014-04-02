/**
 * 大对象管理
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
 * 返回用于存储小型大对象的堆对象
 *
 * @return 用于存储小型大对象的堆对象
 */
DrsHeap* LobStorage::getSLHeap() const {
	return m_slob->getHeap();
}

/**
 * 返回用于缓存小型大对象的MMS表对象
 *
 * @return 用于缓存小型大对象的MMS表对象
 */
MmsTable* LobStorage::getSLMmsTable() const {
	return m_slob->getMmsTable();
}

/** 
 * 大对象存储类构造函数
 *
 * @param db 所属数据库
 * @param tableDef 表定义
 * @param slob 小型大对象存储
 * @param blob 大型大对象存储
 * @param lzoOper lzo压缩
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
 * 大对象存储类析构函数
 */
LobStorage::~LobStorage () {
	delete m_slob;
	delete m_blob;
	delete m_lzo;
}

/**
 * 创建一个大对象存储。本函数在创建含有大对象字段的表时调用,需要
 * 生成大型大对象文件和目录文件，以及小型大对象文件
 *
 * @param db 数据库
 * @param tableDef 所属表定义
 * @param path 文件路径，不包含后缀名
 * @throw NtseException 文件操作出错等
 */
void LobStorage::create(Database *db, const TableDef *tableDef, const char *path) throw(NtseException) {
	ftrace(ts.lob, tout << db << tableDef << path);
	// 创建小型大对象文件
	SmallLobStorage::create(db, tableDef, tableDef->m_id, path);

	// 创建大型大对象
	try {
		BigLobStorage::create(db, tableDef, path);
	} catch (NtseException &e) {
		// 这里需要删除前面已经成功create的小型大对象文件
		SmallLobStorage::drop(path);
		throw e;
	}
}

/**
 * 打开一个大对象存储。在表第一次被使用时调用
 *
 * @param db 数据库
 * @param session 会话对象
 * @param tableDef 表定义
 * @param path 文件路径，不包含后缀名
 * @param useMms 对小型大对象是否使用mms
 * @return 大对象存储
 * @throw NtseException 文件操作出错等
 */
LobStorage* LobStorage::open(Database *db, Session *session, const TableDef *tableDef, const char *path, bool useMms) throw(NtseException) {
	ftrace(ts.lob, tout << db << session << tableDef << path << useMms);
	// 首先生成SmallLob对象
	SmallLobStorage *slob = NULL;

	slob = SmallLobStorage::open(db, session, path, useMms);

	// 生成BigLobStorage对象
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
 * 插入一个大对象
 *
 * @param session 会话对象
 * @param lob 大对象内容
 * @param size 大对象大小
 * @param compress 是否应存储为压缩格式
 * @return 大对象ID
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
 * 读取一个大对象的内容
 *
 * @param session 会话对象
 * @param mc 用于分配返回值存储空间的内存分配上下文
 * @param lobId 要读取的大对象的ID
 * @param size 输出参数，读取的大对象的大小
 * @param intoMms 对于小型大对象是否放入mms中
 * @return 成功返回大对象内容，指定的大对象不存在返回NULL
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
 * 删除一个大对象
 *
 * @param session 会话对象
 * @param lobId 要删除的大对象ID
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
 * 删除一个大对象
 *
 * @param session 会话对象
 * @param lobId 要删除的大对象ID
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
 * 更新一个大对象
 *
 * @param session 会话对象
 * @param lobId 要更新的大对象的ID
 * @param lob 新大对象内容
 * @param size 新大对象内容大小
 * @param compress 是否应存储为压缩格式
 * @return 新的大对象ID（小型大对象更新变成大型大对象或是否压缩发生变化时，大对象ID会发生变化）
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
		if (compressRet == 0) { // 压缩成功
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
 * 删除大对象存储。在表被删除时调用
 *
 * @param path 文件路径，不包含后缀名
 * @throw NtseException 文件操作出错等
 */
void LobStorage::drop(const char *path) throw(NtseException) {
	ftrace(ts.lob, tout << path);

	SmallLobStorage::drop(path);

	// 删除大型大对象文件
	BigLobStorage::drop(path);
}

/**
 * 关闭大对象存储
 *
 * @param session 会话对象
 * @param flushDirty 是否写出脏数据
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
 * 在线碎片整理
 *
 * @param session 会话
 */
void LobStorage::defrag(Session *session) {
	ftrace(ts.lob, tout << session);

	m_blob->defrag(session, m_tableDef);
}

/** 设置表ID
 * @param session 会话
 * @param tableId 新的表ID
 */
void LobStorage::setTableId(Session *session, u16 tableId) {
	m_slob->setTableId(session, tableId);
	m_blob->setTableId(session, tableId);
}

/**
 * 故障恢复时REDO创建表操作
 *
 * @param db 数据库对象
 * @param session 会话
 * @param tableDef 表定义
 * @param path 文件路径，不带后缀名
 * @param tableid 表ID
 * @throw NtseException 文件操作出错等
 */
void LobStorage::redoCreate(Database *db, Session *session, const TableDef *tableDef, const char *path, u16 tableId) throw(NtseException) {
	ftrace(ts.lob | ts.recv, tout << db << session << tableDef << path << tableId);

	SmallLobStorage::redoCreate(db, session, path, tableId);

	try {
		BigLobStorage::redoCreate(db, tableDef, path, tableId);
	} catch (NtseException &e) {
		// 假如不成功，需要处理小型大对象文件
		SmallLobStorage::drop(path);
		throw e;
	}
}

/**
 * 故障恢复时REDO小型大对象插入操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param logSize 日志大小
 * @return 大对象ID
 */
LobId LobStorage::redoSLInsert(Session *session, u64 lsn, const byte *log, uint logSize) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << logSize);

	return m_slob->redoInsert(session, lsn, log, logSize);
}

/**
 * 故障恢复时REDO大型大对象插入操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param logSize 日志大小
 * @return 大对象ID
 */
LobId LobStorage::redoBLInsert(Session *session, u64 lsn, const byte *log, uint logSize) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << logSize);

	return m_blob->redoInsert(session, m_tableDef, lsn, log, logSize);
}

/**
 * 故障恢复时REDO小型大对象删除操作
 *
 * @param session 会话对象
 * @param lobId 被删除大对象ID
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 */
void LobStorage::redoSLDelete(Session *session, LobId lobId, u64 lsn, const byte *log, uint size) {
	ftrace(ts.lob | ts.recv, tout << session << lobId << lsn << log << size);

	m_slob->redoDelete(session, lobId, lsn, log, size);
}

/**
 * 故障恢复时REDO大型大对象删除操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 */
void LobStorage::redoBLDelete(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << size);

	m_blob->redoDelete(session, lsn, log, size);
}

/**
 * 故障恢复时REDO小型大对象不命中MMS的更新操作
 *
 * @param session 会话对象
 * @param lobId 被更新大对象ID
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 * @param lob 预更新的内容(大对象内容)
 * @param lobSize 大对象长度
 * @param compress 是否应存储为压缩格式
 * @return 大对象ID
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
		// 这里压缩不一定会成功，因为现在是redo
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
 * 故障恢复时REDO小型大对象命中MMS时的更新操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 */
void LobStorage::redoSLUpdateMms(Session *session, u64 lsn, const byte *log, uint size) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << size);

	m_slob->redoUpdateMms(session, lsn, log, size);
}

/**
 * 故障恢复时REDO大型大对象更新操作
 *
 * @param session 会话对象
 * @param lobId 旧的大对象ID
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param logSize 日志大小
 * @param lob 预更新的内容(大对象内容)
 * @param lobSize 大对象长度
 * @param compress 是否压缩
 */
void LobStorage::redoBLUpdate(Session *session, LobId lobId, u64 lsn, const byte *log, uint logSize, const byte *lob, uint lobSize, bool compress) {
	ftrace(ts.lob | ts.recv, tout << session << lobId << lsn << log << logSize << lob << lobSize);

	bool needCompress = NEED_COMPRESS(compress, lobSize);
	u32 realSize = lobSize;
	byte *realData = (byte *)lob;

	if (needCompress) {
		uint outLen = MAX_COMPRESSED_SIZE(lobSize);
		byte *out = (byte *)session->getMemoryContext()->alloc(outLen);
		// 这里压缩不一定会成功，因为现在是redo
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
 * 故障恢复时REDO大型大对象移动操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param logSize 日志大小
 */
void LobStorage::redoMove(Session *session, u64 lsn, const byte *log, uint logSize) {
	ftrace(ts.lob | ts.recv, tout << session << lsn << log << logSize);

	m_blob->redoMove(session, m_tableDef, lsn, log, logSize);
}

/**
 * 得到大对象模块所用的文件
 *
 * @param files in/out 该模块的所有File指针数组，空间调用者分配
 * @param pageTypes in/out File对应的页类型
 * @param numFile files数组和pageTypes数组长度
 * @return File对象个数
 */
int LobStorage::getFiles(File** files, PageType* pageTypes, int numFile) {
	assert(numFile >= 3);
	int bigFiles = m_blob->getFiles(files, pageTypes, numFile);
	int smallFiles = m_slob->getFiles(files + bigFiles, pageTypes + bigFiles, numFile - bigFiles);
	return bigFiles + smallFiles;
}

/**
 * 刷出脏数据
 *
 * @param session 会话
 */
void LobStorage::flush(Session *session) {
	ftrace(ts.lob, tout << session);

	m_slob->flush(session);
	m_blob->flush(session);
}

/**
 * 获取大对象的基本统计信息
 *
 * @return 大对象基本统计信息
 */
const LobStatus& LobStorage::getStatus(){
	m_status.m_slobStatus = m_slob->getStatus();
	m_status.m_blobStatus = m_blob->getStatus();
	return m_status;
}

/**
 * 获取大对象的扩展统计信息
 *
 * @return 大对象扩展统计信息
 */
const LobStatusEx& LobStorage::getStatusEx() {
	m_statusEx.m_slobStatus = m_slob->getStatusEx();
	m_statusEx.m_blobStatus = m_blob->getStatusEx();
	return m_statusEx;
}

/**
 * 更新大对象的扩展统计信息
 *
 * @param session 会话
 * @param maxSamplePages 最多采样这么多个页面
 * @return 更新大对象扩展统计信息
 */
void LobStorage::updateExtendStatus(Session *session, uint maxSamplePages) {
	ftrace(ts.lob, tout << session << maxSamplePages);

	m_slob->updateExtendStatus(session, maxSamplePages);
	m_blob->updateExtendStatus(session, maxSamplePages);
}

/** 
 * 返回大型大对象目录文件的统计信息
 *
 * @return 大型大对象目录文件的统计信息
 */
DBObjStats* LobStorage::getLLobDirStats() {
	return m_blob->getDirStats();
}

/** 
 * 返回大型大对象数据文件的统计信息
 *
 * @return 大型大对象数据文件的统计信息
 */
DBObjStats* LobStorage::getLLobDatStats() {
	return m_blob->getDatStats();
}

/** 
 * 设置表定义
 *
 * @param tableDef 表定义
 */
void LobStorage::setTableDef(const TableDef *tableDef) {
	m_tableDef = tableDef;
}

const TableDef* LobStorage::getSLVTableDef() {
	return m_slob->getVTableDef();
}

/**
 * 解析插入日志
 *
 * @param log 日志项
 * @param lid 大对象的LobId
 * @param origLen out 未压缩的数据大小
 * @param mc 分配输出数据缓存区用的MemoryContext
 * @return  当大对象不是压缩的时候，返回指向日志数据中的大对象数据的指针
 *          当大对象是压缩的时候，解压缩后的数据放到buf中，返回buf
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
		s.read(&oLen); // 读出原始大小
		s.read(&clen);
		*origLen = oLen;
		cdata = (byte *)(log->m_data + s.getSize());
	} else {
		assert(log->m_logType == LOG_HEAP_INSERT); // 虚拟堆插入
		Record outRec;
		outRec.m_format = REC_VARLEN;
		VariableLengthRecordHeap::getRecordFromInsertlog((LogEntry *)log, &outRec);
		size_t compressedLen;
		cdata = RecordOper::extractSlobData(m_slob->getVTableDef(), &outRec, &compressedLen, origLen); // 这里会获得origLen
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
* 设置MMS表
*
* @param session 会话
* @param useMms 使用MMS
* @param flushDirty 是否刷写脏记录
*/
void LobStorage::setMmsTable(Session *session, bool useMms, bool flushDirty) {
	if (m_slob)
		m_slob->setMmsTable(session, useMms, flushDirty);
}


#ifdef TNT_ENGINE
/** insert Lob的日志
 * @param session 操作会话
 * @param txnId purge操作所属的事务id
 * @param preLsn 同一事务前一个日志的lsn
 * @param lobId 插入大对象的id
 * return 日志的序列号
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

/** 解析插入大对象的日志
 * @param log 待解析日志对象
 * @param txnId out 插入大对象操作所属的事务id
 * @param preLsn out 同一事务前一个日志的lsn
 * @param lobId out 插入大对象的id
 */
void LobStorage::parseTNTInsertLob(const LogEntry *log, TrxId *txnId, u64 *preLsn, LobId *lobId) {
	Stream s(log->m_data, log->m_size);
	s.read(txnId);
	s.read(preLsn);
	s.read(lobId);
}
#endif

}