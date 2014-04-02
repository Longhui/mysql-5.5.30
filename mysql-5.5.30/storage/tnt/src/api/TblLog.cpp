/**
 * 表管理模块日志相关功能实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include "api/Table.h"
#include "util/Stream.h"

using namespace ntse;

namespace ntse {

/**
 * 写预删除日志
 *
 * @param session 会话
 * @param rid 要删除的记录ID
 * @param row 要删除的记录内容，为REC_REDUNDANT格式
 * @param indexPreImage 表拥有索引时给出所有索引属性的前像，否则无意义 
 */
void Table::writePreDeleteLog(Session *session, RowId rid, const byte *row, const SubRecord *indexPreImage) {
	// 计算要删除的大对象个数
	Record rec(0, REC_REDUNDANT, (byte *)row, m_tableDef->m_maxRecSize);
	u16 numLobs = 0;
	for (u16 i = 0; i < m_tableDef->m_numCols; i++) {
		if (m_tableDef->m_columns[i]->isLob()) {
			if (!RecordOper::isNullR(m_tableDef, &rec, i))
				numLobs++;
		}
	}
#ifdef TNT_ENGINE   //如果是TNT引擎，那么这里不需要任何大对象相关的记录
	if(session->getTrans() != NULL)
		numLobs = 0;
#endif

	size_t maxLogSize = RID_BYTES + sizeof(numLobs) + sizeof(LobId) * numLobs;
	if (m_tableDefWithAddingIndice->m_numIndice > 0)
		maxLogSize += RecordOper::getSubRecordSerializeSize(m_tableDef, indexPreImage, false);
	byte *buf = (byte *)session->getMemoryContext()->alloc(maxLogSize);
	Stream s(buf, maxLogSize);
	s.writeRid(rid);
	if (m_tableDefWithAddingIndice->m_numIndice > 0) {
		assert(maxLogSize - s.getSize() >= RecordOper::getSubRecordSerializeSize(m_tableDef, indexPreImage, false));
		RecordOper::serializeSubRecordMNR(&s, m_tableDef, indexPreImage, false);
	}
	s.write(numLobs);

#ifdef TNT_ENGINE    //如果是TNT引擎，那么这里不需要任何大对象相关的记录
	if(session->getTrans() == NULL) {
		for (int i = m_tableDef->m_numCols - 1; i >= 0; i--) {
			if (m_tableDef->m_columns[i]->isLob()) {
				if (!RecordOper::isNullR(m_tableDef, &rec, (u16)i))
					s.write(RecordOper::readLobId(row, m_tableDef->m_columns[i]));
			}
		}
	}
#endif
	session->cacheLog(LOG_PRE_DELETE, m_tableDef->m_id, buf, s.getSize());
}

/**
 * 写预删除大对象日志
 *
 * @pre	  调用之前必须已经判断出是否是NULL
 * @param session 会话
 * @param lobId 要删除的大对象ID
 */
void Table::writePreDeleteLobLog(Session *session, LobId lobId) {
	size_t maxLogSize =  sizeof(LobId);
	byte *buf = (byte *)session->getMemoryContext()->alloc(maxLogSize);
	Stream s(buf, maxLogSize);
	s.write(lobId);	
	session->cacheLog(LOG_PRE_LOB_DELETE, m_tableDef->m_id, buf, s.getSize());
}

/**
 * 解析预删除大对象日志。所有内存从会话的MemoryContext分配
 * 
 * @param session 会话
 * @param log 日志内容
 * @return 预删除的大对象ID
 */
LobId Table::parsePreDeleteLobLog(Session *session, const LogEntry *log) {
	assert(log->m_logType == LOG_PRE_LOB_DELETE && log->m_tableId == m_tableDef->m_id);
	LobId *lobId = (LobId *)session->getMemoryContext()->alloc(sizeof(LobId));
	Stream s(log->m_data, log->m_size);
	s.read(lobId);
	assert(s.getSize() == log->m_size);
	return *lobId;
}
/**
 * 解析预删除日志。所有内存从会话的MemoryContext分配
 *
 * @param session 会话
 * @param log 日志内容
 * @return 预删除日志
 */
PreDeleteLog* Table::parsePreDeleteLog(Session *session, const LogEntry *log) {
	assert(log->m_logType == LOG_PRE_DELETE && log->m_tableId == m_tableDef->m_id);
	PreDeleteLog *ret = (PreDeleteLog *)session->getMemoryContext()->alloc(sizeof(PreDeleteLog));
	Stream s(log->m_data, log->m_size);
	s.readRid(&ret->m_rid);
	if (m_tableDefWithAddingIndice->m_numIndice > 0)
		ret->m_indexPreImage = RecordOper::unserializeSubRecordMNR(&s, m_tableDef, session->getMemoryContext());
	else
		ret->m_indexPreImage = NULL;
	s.read(&ret->m_numLobs);
	if (ret->m_numLobs > 0) {
		ret->m_lobIds = (LobId *)session->getMemoryContext()->alloc(sizeof(LobId) * (ret->m_numLobs));
		for (u16 i = 0; i < ret->m_numLobs; i++)
			s.read(ret->m_lobIds + i);
	} else
		ret->m_lobIds = NULL;
	assert(s.getSize() == log->m_size);
	return ret;
}

/** 得到预删除日志所操作的记录RID
 * @param log 预删除日志
 * @return RID
 */
RowId Table::getRidFromPreDeleteLog(const LogEntry *log) {
	assert(log->m_logType == LOG_PRE_DELETE && log->m_tableId == m_tableDef->m_id);
	Stream s(log->m_data, log->m_size);
	RowId rid;
	s.readRid(&rid);
	return rid;
}

/**
 * 写创建索引日志
 *
 * @param session 会话
 * @param indexDef 索引定义
 */
void Table::writeAddIndexLog(Session *session, const IndexDef *indexDef) {
	byte *tmpBuffer = NULL;
	u32 size;
	byte *buf = NULL;

	indexDef->writeToKV(&tmpBuffer, &size);
	McSavepoint msp(session->getMemoryContext());
	buf = (byte*)session->getMemoryContext()->alloc(size + Limits::MAX_FREE_MALLOC);
	Stream s(buf, size + Limits::MAX_FREE_MALLOC);
	s.write(size);
	s.write(tmpBuffer, size);

	session->writeLog(LOG_ADD_INDEX, m_tableDef->m_id, buf, s.getSize());

	if (tmpBuffer != NULL) {
		delete[] tmpBuffer;
		tmpBuffer = NULL;
	}
}

/**
 * 解析创建索引日志
 *
 * @param log LOG_ADD_INDEX日志
 * @return 索引定义
 */
IndexDef* Table::parseAddIndexLog(const LogEntry *log) {
	assert(log->m_logType == LOG_ADD_INDEX);
	Stream s(log->m_data, log->m_size);

	u32 size = 0;
	s.read(&size);
	IndexDef *indexDef = new IndexDef();
	indexDef->readFromKV(s.currPtr(), size);
	assert(s.currPtr() - log->m_data + size == (int)log->m_size);

	return indexDef;
}

/** 写TRUNCATE日志
 * @param session        会话
 * @param tableDef       表定义
 * @param hasDict        原表是否有字典
 * @param isTruncateOper 是否是truncate操作
 */
u64 Table::writeTruncateLog(Session *session, const TableDef *tableDef, bool hasDict, bool isTruncateOper) {
	u64 lsn = 0;
	byte *tmpBuffer = NULL;
	u32 size;
	byte *buf = NULL;
	tableDef->write(&tmpBuffer, &size);

	McSavepoint msp(session->getMemoryContext());
	buf = (byte*)session->getMemoryContext()->alloc(size + Limits::MAX_FREE_MALLOC);
	Stream s(buf, size + Limits::MAX_FREE_MALLOC);
	s.write(size);
	s.write(tmpBuffer, size);
	s.write((u8)hasDict);
	s.write((u8)isTruncateOper);
	lsn = session->writeLog(LOG_TRUNCATE, tableDef->m_id, buf, s.getSize());

	if (tmpBuffer != NULL) {
		delete[] tmpBuffer;
		tmpBuffer = NULL;
	}

	return lsn;
}

/** 解析TRUNCATE日志
 * @param log            日志内容
 * @param tableDefOut    OUT 表定义
 * @param hasDict        OUT 原表是否含有字典文件
 * @param isTruncateOper OUT 是否是truncate操作
 */
void Table::parseTruncateLog(const LogEntry *log, TableDef** tableDefOut, bool *hasDict, bool *isTruncateOper) {
	assert(log->m_logType == LOG_TRUNCATE);
	Stream s(log->m_data, log->m_size);
	TableDef *tableDef = new TableDef();
	u32 size = 0;
	s.read(&size);
	tableDef->read(s.currPtr(), size);
	s.skip(size);
	u8 d = 0;
	s.read(&d);
	*hasDict = (d > 0);	
	s.read(&d);
	*isTruncateOper = (d > 0);
	assert(s.currPtr() - log->m_data == (int)log->m_size);
	*tableDefOut = tableDef;
}

/**
 * 记录维护索引日志
 * 
 * @param session  会话
 * @param newTableDef  新的TableDef
 * @return 日志的LSN
 */
u64 Table::writeAlterIndiceLog(Session *session, const TableDef *newTableDef, const char* relativeIdxPath) {
	u64 lsn = 0;
	byte *tmpBuffer = NULL;
	u32 size;
	byte *buf = NULL;
	newTableDef->write(&tmpBuffer, &size);

	McSavepoint msp(session->getMemoryContext());
	buf = (byte*)session->getMemoryContext()->alloc(size + Limits::PAGE_SIZE);
	Stream s(buf, size + Limits::PAGE_SIZE);

	s.write(size);
	s.write(tmpBuffer, size);
	s.write(relativeIdxPath);
	lsn = session->writeLog(LOG_ALTER_INDICE, newTableDef->m_id, buf, s.getSize());

	if (tmpBuffer != NULL) {
		delete[] tmpBuffer;
		tmpBuffer = NULL;
	}

	return lsn;
}

/**
 * 解析索引维护日志
 * @param log 日志项
 * @return  解析出来的TableDef
 */
void Table::parseAlterIndiceLog(const LogEntry *log, TableDef** tableDef, char** relativeIdxPath) {
	assert(log->m_logType == LOG_ALTER_INDICE);
	Stream s(log->m_data, log->m_size);

	u32 size = 0;
	s.read(&size);
	*tableDef = new TableDef();
	(*tableDef)->read(s.currPtr(), size);
	s.skip(size);

	s.readString(relativeIdxPath);
	assert(s.currPtr() - log->m_data == (int)log->m_size);
	return;
}

/**
* 记录列改变日志
* 
* @param session  会话
* @param hasLob  更新后的表是否有lob文件。
* @param hasDict 更新后的表是否含有压缩字典文件
* @return  日志的LSN
*/
u64 Table::writeAlterColumnLog(ntse::Session *session, const char* tmpTablePath, bool hasLob, bool hasDict) {
	McSavepoint savePoint(session->getMemoryContext());
	const uint bufSize = Limits::PAGE_SIZE;
	byte *buf = (byte *)session->getMemoryContext()->alloc(bufSize);	
	Stream s(buf, bufSize);
	s.write(tmpTablePath);
	s.write(hasLob);
	s.write(hasDict);
	return session->writeLog(LOG_ALTER_COLUMN, m_tableDef->m_id, buf, s.getSize());
}

/** 解析列改变日志
 *
 * @param log 日志内容
 * @return 新表是否包含大对象
 */
void Table::parseAlterColumnLog(const LogEntry *log, char** tmpTablePath, bool *hasLob, bool *hasDict) {
	Stream s(log->m_data, log->m_size);
	s.readString(tmpTablePath);
	s.read(hasLob);
	s.read(hasDict);

	return;
}

/** 简单地写创建字典日志
 * @param session 会话
 */
u64 Table::writeCreateDictLog(Session *session) {
	const uint size = sizeof(u8);
	byte buf[size];
	*((u8*) buf) = 1;
	return session->writeLog(LOG_CREATE_DICTIONARY, m_tableDef->m_id, buf, size);
}
}

