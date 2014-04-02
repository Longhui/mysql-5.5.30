/**
 * �����ģ����־��ع���ʵ��
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "api/Table.h"
#include "util/Stream.h"

using namespace ntse;

namespace ntse {

/**
 * дԤɾ����־
 *
 * @param session �Ự
 * @param rid Ҫɾ���ļ�¼ID
 * @param row Ҫɾ���ļ�¼���ݣ�ΪREC_REDUNDANT��ʽ
 * @param indexPreImage ��ӵ������ʱ���������������Ե�ǰ�񣬷��������� 
 */
void Table::writePreDeleteLog(Session *session, RowId rid, const byte *row, const SubRecord *indexPreImage) {
	// ����Ҫɾ���Ĵ�������
	Record rec(0, REC_REDUNDANT, (byte *)row, m_tableDef->m_maxRecSize);
	u16 numLobs = 0;
	for (u16 i = 0; i < m_tableDef->m_numCols; i++) {
		if (m_tableDef->m_columns[i]->isLob()) {
			if (!RecordOper::isNullR(m_tableDef, &rec, i))
				numLobs++;
		}
	}
#ifdef TNT_ENGINE   //�����TNT���棬��ô���ﲻ��Ҫ�κδ������صļ�¼
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

#ifdef TNT_ENGINE    //�����TNT���棬��ô���ﲻ��Ҫ�κδ������صļ�¼
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
 * дԤɾ���������־
 *
 * @pre	  ����֮ǰ�����Ѿ��жϳ��Ƿ���NULL
 * @param session �Ự
 * @param lobId Ҫɾ���Ĵ����ID
 */
void Table::writePreDeleteLobLog(Session *session, LobId lobId) {
	size_t maxLogSize =  sizeof(LobId);
	byte *buf = (byte *)session->getMemoryContext()->alloc(maxLogSize);
	Stream s(buf, maxLogSize);
	s.write(lobId);	
	session->cacheLog(LOG_PRE_LOB_DELETE, m_tableDef->m_id, buf, s.getSize());
}

/**
 * ����Ԥɾ���������־�������ڴ�ӻỰ��MemoryContext����
 * 
 * @param session �Ự
 * @param log ��־����
 * @return Ԥɾ���Ĵ����ID
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
 * ����Ԥɾ����־�������ڴ�ӻỰ��MemoryContext����
 *
 * @param session �Ự
 * @param log ��־����
 * @return Ԥɾ����־
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

/** �õ�Ԥɾ����־�������ļ�¼RID
 * @param log Ԥɾ����־
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
 * д����������־
 *
 * @param session �Ự
 * @param indexDef ��������
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
 * ��������������־
 *
 * @param log LOG_ADD_INDEX��־
 * @return ��������
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

/** дTRUNCATE��־
 * @param session        �Ự
 * @param tableDef       ����
 * @param hasDict        ԭ���Ƿ����ֵ�
 * @param isTruncateOper �Ƿ���truncate����
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

/** ����TRUNCATE��־
 * @param log            ��־����
 * @param tableDefOut    OUT ����
 * @param hasDict        OUT ԭ���Ƿ����ֵ��ļ�
 * @param isTruncateOper OUT �Ƿ���truncate����
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
 * ��¼ά��������־
 * 
 * @param session  �Ự
 * @param newTableDef  �µ�TableDef
 * @return ��־��LSN
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
 * ��������ά����־
 * @param log ��־��
 * @return  ����������TableDef
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
* ��¼�иı���־
* 
* @param session  �Ự
* @param hasLob  ���º�ı��Ƿ���lob�ļ���
* @param hasDict ���º�ı��Ƿ���ѹ���ֵ��ļ�
* @return  ��־��LSN
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

/** �����иı���־
 *
 * @param log ��־����
 * @return �±��Ƿ���������
 */
void Table::parseAlterColumnLog(const LogEntry *log, char** tmpTablePath, bool *hasLob, bool *hasDict) {
	Stream s(log->m_data, log->m_size);
	s.readString(tmpTablePath);
	s.read(hasLob);
	s.read(hasDict);

	return;
}

/** �򵥵�д�����ֵ���־
 * @param session �Ự
 */
u64 Table::writeCreateDictLog(Session *session) {
	const uint size = sizeof(u8);
	byte buf[size];
	*((u8*) buf) = 1;
	return session->writeLog(LOG_CREATE_DICTIONARY, m_tableDef->m_id, buf, size);
}
}

