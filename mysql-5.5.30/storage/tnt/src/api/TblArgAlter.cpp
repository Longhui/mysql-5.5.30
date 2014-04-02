/**
 * ������޸���ز���ʵ��
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
 */
#include <string>
#include "api/TblArgAlter.h"
#include "api/Table.h"
#include "misc/ControlFile.h"
#include "api/Database.h"
#include "util/Portable.h"
#include "util/Stream.h"
#include "btree/Index.h"
#include "misc/TableDef.h"
#include "misc/ColumnGroupParser.h"
#include "api/TblMnt.h"
#include "misc/ParFileParser.h"

using namespace std;

namespace ntse {
/**
 * ���캯����
 * ��ʼ������ӳ���
 */
TableArgAlterCommandMapTool::TableArgAlterCommandMapTool() : m_mp(TableArgAlterHelper::NUM_CMDS) {
	initCommandMap();
}

/**
 * ��ȡ����ӳ���
 *
 * @return ������޸�����ӳ���
 */
Hash<const char *, TableArgAlterCmdType, Hasher<const char *>, Equaler<const char *, const char *> > *TableArgAlterCommandMapTool::getCmdMap() {
	return &m_mp;
}

/**
 * ��ʼ�����޸Ĳ�������ӳ�䡣
 */
void TableArgAlterCommandMapTool::initCommandMap() {
	m_mp.put(TableArgAlterHelper::CMD_USEMMS, USEMMS);
	//m_mp.put(TableArgAlterHelper::CMD_CACHE_UPDATE, CACHE_UPDATE);
	//m_mp.put(TableArgAlterHelper::CMD_UPDATE_CACHE_TIME, UPDATE_CACHE_TIME);
	//m_mp.put(TableArgAlterHelper::CMD_CACHED_COLUMNS, CACHED_COLUMNS);

	m_mp.put(TableArgAlterHelper::CMD_COMPRESS_LOBS, COMPRESS_LOBS);
	m_mp.put(TableArgAlterHelper::CMD_HEAP_PCT_FREE, HEAP_PCT_FREE);
	m_mp.put(TableArgAlterHelper::CMD_SPLIT_FACTORS, SPLIT_FACTORS);
	m_mp.put(TableArgAlterHelper::CMD_INCR_SIZE, INCR_SIZE);
	m_mp.put(TableArgAlterHelper::CMD_COMPRESS_ROWS, COMPRESS_ROWS);
	m_mp.put(TableArgAlterHelper::CMD_FIX_LEN, FIX_LEN);
	m_mp.put(TableArgAlterHelper::CMD_SET_COLUMN_GROUPS, COLUMN_GROUPS);
	m_mp.put(TableArgAlterHelper::CMD_COMPRESS_DICT_SIZE, COMPRESS_DICT_SIZE);
	m_mp.put(TableArgAlterHelper::CMD_COMPRESS_DICT_MIN_LEN, COMPRESS_DICT_MIN_LEN);
	m_mp.put(TableArgAlterHelper::CMD_COMPRESS_DICT_MAX_LEN, COMPRESS_DICT_MAX_LEN);
	m_mp.put(TableArgAlterHelper::CMD_COMPRESS_THRESHOLD, COMPRESS_THRESHOLD);
	m_mp.put(TableArgAlterHelper::CMD_SUPPORT_TRX, SUPPORT_TRX);

	assert(m_mp.getSize() == (size_t)TableArgAlterHelper::NUM_CMDS);
}

const char * TableArgAlterHelper::CMD_USEMMS = "USEMMS";
const char * TableArgAlterHelper::CMD_CACHE_UPDATE = "CACHE_UPDATE";
const char * TableArgAlterHelper::CMD_UPDATE_CACHE_TIME = "UPDATE_CACHE_TIME";
const char * TableArgAlterHelper::CMD_CACHED_COLUMNS = "CACHED_COLUMNS";

const char * TableArgAlterHelper::CMD_COMPRESS_LOBS = "COMPRESS_LOBS";
const char * TableArgAlterHelper::CMD_HEAP_PCT_FREE = "HEAP_PCT_FREE";
const char * TableArgAlterHelper::CMD_SPLIT_FACTORS = "SPLIT_FACTORS";
const char * TableArgAlterHelper::CMD_INCR_SIZE = "INCR_SIZE";
const char * TableArgAlterHelper::CMD_COMPRESS_ROWS = "COMPRESS_ROWS";
const char * TableArgAlterHelper::CMD_FIX_LEN = "FIX_LEN";
const char * TableArgAlterHelper::CMD_SET_COLUMN_GROUPS = "COLUMN_GROUPS";
const char * TableArgAlterHelper::CMD_COMPRESS_DICT_SIZE = "DICTIONARY_SIZE";
const char * TableArgAlterHelper::CMD_COMPRESS_DICT_MIN_LEN = "DICTIONARY_MIN_LEN";
const char * TableArgAlterHelper::CMD_COMPRESS_DICT_MAX_LEN = "DICTIONARY_MAX_LEN";
const char * TableArgAlterHelper::CMD_COMPRESS_THRESHOLD = "COMPRESS_THRESHOLD";
const char * TableArgAlterHelper::CMD_SUPPORT_TRX = "SUPPORT_TRX";

const char * TableArgAlterHelper::ENABLE = "ENABLE";
const char * TableArgAlterHelper::DISABLE = "DISABLE";

const char * TableArgAlterHelper::STR_TABLE = "table";
const char * TableArgAlterHelper::STR_SET = "set";
const char * TableArgAlterHelper::STR_EQUAL = "=";
const char * TableArgAlterHelper::STR_DOT = ".";
const char TableArgAlterHelper::COL_COMMA_DELIMITER = ',';

TableArgAlterCommandMapTool TableArgAlterHelper::cmdMapTool;

const char * TableArgAlterHelper::ALLOC_SESSION_NAME = "Alter table session";

const char * TableArgAlterHelper::MSG_EXCEPTION_MMS_NOT_ENABLED = "MMS is not enabled, operation failed.";
const char * TableArgAlterHelper::MSG_EXCEPTION_CACHE_UPDATE_NOT_ENABLED = "Cache update is not enabled, operation failed.";
const char * TableArgAlterHelper::MSG_EXCEPTION_WRONG_COMMAND_TYPE = "Wrong command type for scheme.table.cmdType";
const char * TableArgAlterHelper::MSG_EXCEPTION_WRONG_COMMAND_ARGUMENTS = "Wrong command argument: %s";
const char * TableArgAlterHelper::MSG_EXCEPTION_TOO_MANY_MMS_COLUMNS = "Too many MMS cached columns: system supports %d cached columns, %d columns are already cache enabled previously, but %d columns provided in this command with %d columns need to enable.";
const char * TableArgAlterHelper::MSG_EXCEPTION_NON_EXISTED_COL_NAME = "Column name '%s' provided does NOT exist.";
const char * TableArgAlterHelper::MSG_EXCEPTION_NON_EXISTED_INDEX_NAME = "Index name '%s' provided does NOT exist.";
const char * TableArgAlterHelper::MSG_EXCEPTION_WRONG_CMD_FORMAT_SCHEME_TBL_CMD = "Command format does NOT match scheme.table.cmd";
const char * TableArgAlterHelper::MSG_EXCEPTION_INVALID_INDEX_SPLIT_FACTOR = "Invalid SPLIT_FACTOR: %d, should be in [%d, %d] or %d.";
const char * TableArgAlterHelper::MSG_EXCEPTION_INVALID_HEAP_PCT_FREE = "Invalid HEAP_PCT_FREE: %d, should be in [%d, %d], or make sure the records are variable length.";
const char * TableArgAlterHelper::MSG_EXCEPTION_INVALID_INCR_SIZE = "Invalid INCR_SIZE: %d, should be in [%d, %d].";

const char * TableArgAlterHelper::MSG_EXCEPTION_COLUMNGROUP_HAVE_DICTIONARY = "Failed to set column groups when table has compression dictionary.";
const char * TableArgAlterHelper::MSG_EXCEPTION_DICT_ARG_HAVE_DICTIONARY = "Failed to set dictionary arguments when table has compression dictionary.";
const char * TableArgAlterHelper::MSG_EXCEPTION_EMPTY_COL_GRP = "Could not define empty column group.";
/**
 * ���캯����
 *
 * @param db		���ݿ�
 * @param conn		���ݿ�����
 * @param parser	�������
 * @param timeout	����ʱʱ�䣬��λ�롣
 * @param inLockTables  �����Ƿ���Lock Tables ���֮��
 */
TableArgAlterHelper::TableArgAlterHelper(Database *db, Connection *conn, Parser *parser, int timeout, bool inLockTables) {
	assert(NULL != db);
	assert(NULL != conn);
	assert(NULL != parser);
	m_db = db;
	m_conn = conn;
	m_parser = parser;
	m_timeout = timeout;
	m_inLockTables = inLockTables;
}

/**
 * ����������
 *
 * �رձ��ͷŻỰ�����ݿ����ӣ��ͷŽ�������
 */
TableArgAlterHelper::~TableArgAlterHelper() {
	if (NULL != m_parser) {
		delete m_parser;
		m_parser = NULL;
	}
}

/** 
 * ���ַ���ת���ɴ�д��
 *
 * @param str	ת�����ַ���
 * @param len	�ַ����ĳ���
 *
 */
void TableArgAlterHelper::toUpper(char *str, size_t len) {
	assert(NULL != str);
	for (size_t i = 0; i < len; i++) {
		str[i] = (char) toupper(str[i]);
	}
}

/** 
 * ���Cached Columns�Ƿ񳬳�������ơ�
 * 
 * @pre tableDef��Ӧ�ı��Ѿ�������
 *
 * @param session		�Ự��
 * @param tableDef		���塣
 * @param cols			��enable�����±����顣
 * @param numClos		��enable���������С��
 * @param numEnabled	out ֮ǰ�Ѿ�enble��������
 * @prarm numToEnable	������enable��������
 *
 * @return true ������û�г������ƣ�false���enable������������������ơ�
 */
bool TableArgAlterHelper::checkCachedColumns(Session *session, TableDef *tableDef, u16 *cols, u16 numCols, int *numEnabled, int *numToEnable) {
	assert(NULL != session);
	assert(NULL != tableDef);
	assert(NULL != cols);
	assert(NULL != numEnabled);
	assert(NULL != numToEnable);
	UNREFERENCED_PARAMETER(session);
	bool flagArr[Limits::MAX_COL_NUM];
	for (int i = 0; i < Limits::MAX_COL_NUM; i++) {
		flagArr[i] = false;
	}

	*numEnabled = 0;
	*numToEnable = 0;
	for (u16 i = 0; i < tableDef->m_numCols; i++) {
		if (tableDef->m_columns[i]->m_cacheUpdate) {
			++(*numEnabled);
			flagArr[i] = true;
		}
	}

	for (u16 i = 0; i < numCols; i++) {
		int index = cols[i];
		if ((!tableDef->m_columns[index]->m_cacheUpdate) && (!flagArr[i])) {
			++(*numToEnable);
			flagArr[i] = true;
		}
	}
	return (!((u64)(*numEnabled + *numToEnable) > MmsTable::MAX_UPDATE_CACHE_COLUMNS));
}

/**
 * �޸ı������
 *
 * @throw NtseException ���޸Ĳ���ʧ�ܡ�
 */
void TableArgAlterHelper::alterTableArgument() throw(NtseException) {
	assert((NULL != m_db) && (NULL != m_parser));
	//����ĸ�ʽ��alter table set scheme.table_name.name = value
	m_parser->match(STR_TABLE);
	m_parser->match(STR_SET);

	//����scheme.table_name.name

	char *schemeName = NULL;
	char *tableName = NULL;
	char *name = NULL;

	try {
		schemeName = System::strdup(m_parser->nextToken());
		m_parser->match(STR_DOT);
		tableName = System::strdup(m_parser->nextToken());
		m_parser->match(STR_DOT);
		name = System::strdup(m_parser->nextToken());
	} catch (NtseException &) {
		delete []schemeName;
		delete []tableName;
		delete []name;
		NTSE_THROW(NTSE_EC_GENERIC, MSG_EXCEPTION_WRONG_CMD_FORMAT_SCHEME_TBL_CMD);
	}
	
	//��������ת�ɴ�д
	toUpper(name, strlen(name));
	const char *pValue = NULL;

	try {
		m_parser->match(STR_EQUAL);
		pValue = m_parser->remainingString();
	} catch (NtseException &e) {
		delete []schemeName;
		delete []tableName;
		delete []name;
		throw e;
	}

	//��ȡ�Ự�ͱ�ʵ��
	Session *session = m_db->getSessionManager()->allocSession(ALLOC_SESSION_NAME, m_conn);
	string path = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + schemeName + NTSE_PATH_SEP + tableName;
	std::vector<string> parFiles;
	try {		
		ParFileParser parser(path.c_str());
		parser.parseParFile(parFiles);
	} catch (NtseException &e) {
		delete []schemeName;
		delete []tableName;
		delete []name;
		if (session)
			m_db->getSessionManager()->freeSession(session);
		m_db->getSyslog()->log(EL_PANIC, "Parse the partition table .par file error: %s", e.getMessage());
		throw e;
	}

	try {
		alterTableArgumentReal(session, &parFiles, name, pValue);
		m_db->getSessionManager()->freeSession(session);
		session = NULL;
	} catch (NtseException &e) {
		delete []schemeName;
		delete []tableName;
		delete []name;
		if (session)
			m_db->getSessionManager()->freeSession(session);
		throw e;
	}
	assert(!session);
	delete []schemeName;
	delete []tableName;
	delete []name;
}

/** ����ִ���޸ı������������
 * @param session �Ự
 * @param parFiles ������·��
 * @param name  �޸Ĳ���������
 * @param value �޸Ĳ�����ֵ
 */
void TableArgAlterHelper::alterTableArgumentReal(Session *session, vector<string> *parFiles, const char *name, const char *value) throw(NtseException) {
	Table *table = NULL;

	try {
		for (uint i = 0; i < parFiles->size(); i++) {
			table = m_db->openTable(session, parFiles->at(i).c_str());
			//�Ƿ�����ִ���޸ı�
			m_db->alterTableArgument(session, table, name, value, m_timeout, m_inLockTables);
			m_db->closeTable(session, table);
			table = NULL;
		}
	} catch (NtseException &e) {
		if (table)
			m_db->closeTable(session, table);
		throw e;
	}
}

/**
 * �޸ı������
 *
 * @param session		�Ự��
 * @param name			�޸ĵĶ�������
 * @param valueStr		�޸ĵ�ֵ��
 * @param timeout		������ʱʱ�䣬��λ��
 * @param inLockTables  �����Ƿ���Lock Tables ���֮��
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 */
void Table::alterTableArgument(Session *session, const char *name, const char *valueStr, int timeout, bool inLockTables) throw (NtseException) {
	assert(NULL != session);
	assert(NULL != name);
	assert(NULL != valueStr);
	assert(!session->getConnection()->isTrx());
	ftrace(ts.ddl, tout << session << name << valueStr << timeout);

	TableArgAlterCmdType cmdType = TableArgAlterHelper::cmdMapTool.getCmdMap()->get(name);
	switch (cmdType) {
		case USEMMS:
			alterUseMms(session, Parser::parseBool(valueStr), timeout);
			break;
		case CACHE_UPDATE:
			alterCacheUpdate(session, Parser::parseBool(valueStr), timeout);
			break;
		case UPDATE_CACHE_TIME:
			alterCacheUpdateTime(session, Parser::parseInt(valueStr), timeout);
			break;
		case CACHED_COLUMNS:
			alterCachedColumns(session, valueStr, timeout);
			break;
		case COMPRESS_LOBS:
			alterCompressLobs(session, Parser::parseBool(valueStr), timeout);
			break;
		case HEAP_PCT_FREE:
			alterHeapPctFree(session, (u8)Parser::parseInt(valueStr), timeout);
			break;
		case SPLIT_FACTORS:
			alterSplitFactors(session, valueStr, timeout);
			break;
		case INCR_SIZE:
			alterIncrSize(session, (u16)Parser::parseInt(valueStr), timeout);
			break;
		case COMPRESS_ROWS:
			alterCompressRows(session, Parser::parseBool(valueStr), timeout);
			break;
		case FIX_LEN:
			alterHeapFixLen(session, Parser::parseBool(valueStr), timeout);
			break;
		case COLUMN_GROUPS:
			alterColGrpDef(session, valueStr, timeout);
			break;
		case COMPRESS_DICT_SIZE:
		case COMPRESS_DICT_MIN_LEN:
		case COMPRESS_DICT_MAX_LEN:
		case COMPRESS_THRESHOLD:
			alterDictionaryArg(session, cmdType, (uint)Parser::parseInt(valueStr), timeout);
			break;
		case SUPPORT_TRX:
			alterTableStatus(session, Parser::parseBool(valueStr)? TS_TRX: TS_NON_TRX, timeout, inLockTables);
			break;
		default:
			//Hash����ƥ��ʱ���� 0 ���� MIN_TBL_ARG_CMD_TYPE_VALUE
			assert(cmdType == MIN_TBL_ARG_CMD_TYPE_VALUE);
			NTSE_THROW(NTSE_EC_GENERIC, "Wrong Command type.");
			break;
	}
	try {
		m_tableDef->check();
	} catch (NtseException &e) {
		m_db->getSyslog()->log(EL_PANIC, "Invalid table definition: %s", e.getMessage());
		assert(false);
	}
}

/** ׼���޸ı����
 * @pre ����Ԫ����U��
 * @post Ԫ����������ΪX
 *
 * @param session �Ự
 * @param cmdType ��������
 * @param newValue ��ֵ
 * @param timeout ������ʱʱ�䣬��λ��
 * @throw NtseException ������ʱ
 */
void Table::preAlter(Session *session, int cmdType, uint newValue, int timeout) throw (NtseException) {
#ifdef TNT_ENGINE
	if(session->getTrans() == NULL)
		assert(getMetaLock(session) == IL_U);
#endif
	dualFlushAndBump(session, timeout);
	assert(getMetaLock(session) == IL_X);
	LsnType lsn = writeAlterTableArgLog(session, m_tableDef->m_id, cmdType, newValue);
	m_db->getTxnlog()->flush(lsn);
}

/** ˢ�����������ݲ����޸ı��flushLsn
 * @pre ����Ԫ����U��
 * @post Ԫ����������ΪX
 *
 * @param session �Ự
 * @param timeout ������ʱʱ�䣬��λ��
 * @throw NtseException ������ʱ
 */
void Table::dualFlushAndBump(Session *session, int timeout) throw (NtseException) {
#ifdef TNT_ENGINE
	if(session->getTrans() == NULL)
		assert(getMetaLock(session) == IL_U);
#endif
	dualFlush(session, timeout);
	assert(getMetaLock(session) == IL_X);
	m_db->bumpFlushLsn(session, m_tableDef->m_id);
}

/**
 * �޸�ʹ��MMS��
 *
 * @param session		�Ự��
 * @param useMms		�Ƿ�ʹ��MMS�� true ��ʾʹ��MMS��false��ʾ��ʹ��MMS��
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterUseMms(Session *session, bool useMms, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	checkPermission(session, redo);
	if (useMms == m_tableDef->m_useMms)
		return;

	if (!redo)
		preAlter(session, (int)USEMMS, useMms, timeout);
	m_records->alterUseMms(session, useMms);
	m_records->setMmsCallback(m_mmsCallback);
	writeTableDef();
}


/**
 * �޸�ʹ��MMS������¡�
 *
 * @param session		�Ự��
 * @param cacheUpdate	�Ƿ�ʹ��MMS�� true ��ʾʹ�û�����£�false��ʾ��ʹ�û�����¡�
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterCacheUpdate(Session *session, bool cacheUpdate, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	checkPermission(session, redo);
	if (cacheUpdate && !m_tableDef->m_useMms)
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_MMS_NOT_ENABLED);
	if (cacheUpdate && !m_tableDef->m_pkey)
		NTSE_THROW(NTSE_EC_GENERIC, "Can not cache update when table doesn't have a primary key");
	if (cacheUpdate && !m_db->getConfig()->m_enableMmsCacheUpdate)
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Mms cache update feature has been disabled "
		"(Notice if ntse_binlog_method is \"direct\" or log_slave_updtes is false, this feature is automatically disabled.)");

	if (cacheUpdate == m_tableDef->m_cacheUpdate)
		return;

	doAlterCacheUpdate(session, cacheUpdate, timeout, redo);
}

/** ִ���������޸��Ƿ�����MMS������¹���
 * @pre ����Ԫ����U������Ҫ�������޸��Ƿ�����MMS������¹���
 * @post ��redoΪfalseʱԪ�������Ѿ�������X�������������Ѿ�ˢ��
 *
 * @param session �Ự
 * @param cacheUpdate �Ƿ�����MMS������¹���
 * @param lockTimeout ������ʱʱ�䣬��λ��
 * @throw NtseException ������ʱ
 */
void Table::doAlterCacheUpdate(Session *session, bool cacheUpdate, int lockTimeout, bool redo) throw(NtseException) {
	assert(m_tableDef->m_cacheUpdate != cacheUpdate);
#ifdef TNT_ENGINE
	if(session->getTrans() == NULL)
		assert(redo || getMetaLock(session) == IL_U);
#endif
	if (!redo)
		preAlter(session, (int)CACHE_UPDATE, cacheUpdate, lockTimeout);
	m_records->alterMmsCacheUpdate(cacheUpdate);
	writeTableDef();
}

/**
 * �޸�ʹ��MMS����������ڡ�
 *
 * @param session		�Ự��
 * @param interval		MMS����������ڣ���λ�롣
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterCacheUpdateTime(Session *session, uint interval, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	checkPermission(session, redo);
	if (!m_tableDef->m_useMms)
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_MMS_NOT_ENABLED);

	if (!m_tableDef->m_cacheUpdate)
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_CACHE_UPDATE_NOT_ENABLED);

	if (interval == m_tableDef->m_updateCacheTime)
		return;
	
	if (!redo)
		preAlter(session, (int)UPDATE_CACHE_TIME, interval, timeout);
	m_records->alterMmsUpdateCacheTime((u16)interval);
	writeTableDef();
}

/**
 * �޸�ʹ��MMS��������С�
 *
 * @param session		�Ự��
 * @param valueStr		��������Ϣ����ʽΪ 'enable|disable ����[������] '��
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterCachedColumns(Session *session, const char *valueStr, int timeout) throw(NtseException) {
	assert(NULL != session);
	assert(NULL != valueStr);
	
	AutoPtr<Parser> parser(new Parser(valueStr));
	const char *token = parser->nextToken();

	bool enable;
	if(!System::stricmp(TableArgAlterHelper::ENABLE, token)) {
		enable = true;
	} else if (!System::stricmp(TableArgAlterHelper::DISABLE, token)){
		enable = false;
	} else {
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_WRONG_COMMAND_ARGUMENTS, "argument shall be 'enable or disable");
	}

	Array<char *> *colNameArray = NULL;
	try {
		colNameArray = Parser::parseList(parser->remainingString(), TableArgAlterHelper::COL_COMMA_DELIMITER);

		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		if (NULL != colNameArray) {
			for (size_t i = 0; i < colNameArray->getSize(); i++) {
				delete []((*colNameArray)[i]);
			}
		}
		delete colNameArray;
		throw e;
	}
	
	try {
		checkPermission(session, false);
	} catch (NtseException &e) {
		unlockMeta(session, IL_U);
		throw e;
	}
	u16 numCols =  (u16)colNameArray->getSize();
	u16 *cols= new u16[numCols];
	//����ṩ�������Ƿ���Ч
	for (size_t i = 0; i <numCols; i++) {
		char *str = (*colNameArray)[i];
		int colNum = -1;
		colNum = m_tableDef->getColumnNo(str);
		if (colNum < 0) {
			string nameCopy(str);
			delete[] cols;
			for (size_t i = 0; i < colNameArray->getSize(); i++) {
				delete []((*colNameArray)[i]);
			}
			delete colNameArray;
			unlockMeta(session, IL_U);
			NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_NON_EXISTED_COL_NAME, nameCopy.c_str());
		}
		cols[i] = (u16)colNum;
	}
	//���enable���������Ƿ񳬳��������
	if (enable) {
		int numEnabled = 0;
		int numToEnable = 0;
		if (!TableArgAlterHelper::checkCachedColumns(session, m_tableDef, cols, numCols, &numEnabled, &numToEnable)) {	
			delete[] cols;
			for (size_t i = 0; i < colNameArray->getSize(); i++) {
				delete []((*colNameArray)[i]);
			}
			delete colNameArray;
			unlockMeta(session, IL_U);
			NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_TOO_MANY_MMS_COLUMNS, MmsTable::MAX_UPDATE_CACHE_COLUMNS, numEnabled, numCols, numToEnable);		
		}
	}	

	//ִ���޸�
	try {
		alterCachedColumns(session, cols, numCols, enable, timeout);
	} catch (NtseException &e) {
		delete[] cols;
		for (size_t i = 0; i < colNameArray->getSize(); i++) {
			delete []((*colNameArray)[i]);
		}
		delete colNameArray;
		throw e;
	}

	//�ͷ���Դ
	delete[] cols;
	for (size_t i = 0; i < colNameArray->getSize(); i++) {
		delete []((*colNameArray)[i]);
	}
	delete colNameArray;
}

/**
 * �޸�ʹ��MMS���������
 *
 * @pre		�ڷ�redoģʽ�£�m_metaLock �Ѽ�IL_U;
 * @post	�ͷ�Դ��������������
 *
 * @param session		�Ự
 * @param cols			MMS�������±����顣
 * @param numCols		MMS�����������С��
 * @param enable		�Ƿ����û�����¡�true��ʾ���ã�false��ʾ���á�
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 * @see <code> TableDef::check </code> �޸ı�����������顣
 */
void Table::alterCachedColumns(Session *session, u16 *cols, u16 numCols, bool enable, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	assert(NULL != cols);
	assert((numCols > 0) && (numCols < MmsTable::MAX_UPDATE_CACHE_COLUMNS));
	assert(redo || m_metaLock.isLocked(session->getId(), IL_U));
	//��MMSδ���û�cache updateδ����ʱ��ֹ�޸��ֶε�cache update
	if ((!m_tableDef->m_useMms) || !m_tableDef->m_cacheUpdate) {
		if (!redo) {
			unlockMeta(session, IL_U);
		}
		const char * strMsg;
		(!m_tableDef->m_useMms) ? strMsg = TableArgAlterHelper::MSG_EXCEPTION_MMS_NOT_ENABLED : strMsg = TableArgAlterHelper::MSG_EXCEPTION_CACHE_UPDATE_NOT_ENABLED;
		NTSE_THROW(NTSE_EC_GENERIC, strMsg);
	}
	
	bool isIdentical = true;
	for (u16 i = 0; i < numCols; ++i) {
		int index = cols[i];
		//�ڽ������������ϣ���ֹ����Ϊcached column.
		if (enable) {
			for (u16 j = 0; j < m_tableDef->m_numIndice; ++j) {
				for (u16 k = 0; k < m_tableDef->m_indice[j]->m_numCols; ++k) {
					if (index == m_tableDef->m_indice[j]->m_columns[k]) {
						unlockMeta(session, IL_U);
						NTSE_THROW(NTSE_EC_GENERIC, "Column '%s' is an indexed column, can not set to be cached column.", m_tableDef->m_columns[index]->m_name);
					}
				}
			}
		}

		if (enable != m_tableDef->m_columns[index]->m_cacheUpdate) {
			isIdentical = false;
		}		
	}
	//��״̬һ�£��˳�
	if (isIdentical) {
		if (!redo) {
			unlockMeta(session, IL_U);
		}
		return;
	}

	if (!redo) {
		dualFlushAndBump(session, timeout);
		LsnType lsn = writeAlterTableArgLog(session, m_tableDef->m_id, CACHED_COLUMNS, cols, numCols, enable);
		m_db->getTxnlog()->flush(lsn);
	}

	m_records->alterMmsCachedColumns(session, numCols, cols, enable);

	writeTableDef();

	if (!redo)
		unlockMeta(session, IL_X);
}

/**
 * �޸�ʹ�ô����ѹ����
 *
 * @param session		�Ự��
 * @param compressLobs	�Ƿ���ʵʹ�ô����ѹ����true����ʹ�ã�false����ʹ�á�
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterCompressLobs(Session *session, bool compressLobs, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	checkPermission(session, redo);
	if (compressLobs == m_tableDef->m_compressLobs)
		return;
	if (!redo)
		preAlter(session, (int)COMPRESS_LOBS, compressLobs, timeout);
	m_tableDef->m_compressLobs = compressLobs;
	writeTableDef();
}

/**
 * �޸Ĳ����¼ʱҳ��Ԥ�����пռ�İٷֱȡ�
 *
 * @param session		�Ự��
 * @param pctFree		�����¼ʱҳ��Ԥ�����пռ�İٷֱȡ�
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterHeapPctFree(Session *session, u8 pctFree, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	checkPermission(session, redo);
	if (!m_tableDef->isHeapPctFreeValid(pctFree))
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_INVALID_HEAP_PCT_FREE,
		pctFree, TableDef::MIN_PCT_FREE, TableDef::MAX_PCT_FREE);
	if (pctFree == m_tableDef->m_pctFree)
		return;
	if (!redo)
		preAlter(session, (int)HEAP_PCT_FREE, pctFree, timeout);

	m_tableDef->m_pctFree = pctFree;
	m_records->alterPctFree(session, pctFree);
	writeTableDef();
}

/**
 * �޸���������ϵ����
 *
 * @param session		�Ự��
 * @param valueStr		����ϵ����Ϣ����ʽΪ '������ ����ϵ��'��
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterSplitFactors(Session *session, const char *valueStr, int timeout) throw(NtseException){
	assert(session);
	assert(valueStr);

	AutoPtr<Parser> parser(new Parser(valueStr));
	string indexName;
	s8 splitFactor;
	try {
		indexName = parser->nextToken();
		splitFactor = (s8)parser->nextInt();
		parser->checkNoRemain();
	} catch (NtseException &) {
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_WRONG_COMMAND_ARGUMENTS, "cmd argument should be 'index_name_str split_factor_uint'.");
	}

	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);

	try {
		checkPermission(session, false);
	} catch (NtseException &e) {
		unlockMeta(session, IL_U);
		throw e;
	}

	IndexDef * indexDef = m_tableDef->getIndexDef(indexName.c_str());
	if (NULL == indexDef) {
		unlockMeta(session, IL_U);
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_NON_EXISTED_INDEX_NAME, indexName.c_str());
	}
	u8 indexNo;
	for (indexNo = 0; indexNo < m_tableDef->m_numIndice; indexNo++) {
		if (m_tableDef->m_indice[indexNo] == indexDef) {
			break;
		}
	}
	alterSplitFactors(session, indexNo, splitFactor, timeout);
}

/**
 * �޸���������ϵ����
 *
 * @pre		�ڷ�redoģʽ�£�m_metaLock �Ѽ�IL_U;
 * @post	�ͷ�Դ��������������
 *
 * @param session		�Ự��
 * @param indexNo		�����±ꡣ
 * @param splitFactor	��������ʱ���ҳ�������ݰٷֱȡ�
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterSplitFactors(Session *session, u8 indexNo, s8 splitFactor, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);

	if (!redo) {
		assert(m_metaLock.isLocked(session->getId(), IL_U));
		assert(indexNo < m_tableDef->m_numIndice);
		if (!IndexDef::isSplitFactorValid(splitFactor)) {
			unlockMeta(session, IL_U);
			NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_INVALID_INDEX_SPLIT_FACTOR,
			splitFactor, IndexDef::MIN_SPLIT_FACTOR, IndexDef::MAX_SPLIT_FACTOR, IndexDef::SMART_SPLIT_FACTOR);
		}

		//��״̬һ�£��˳�
		if (splitFactor == m_tableDef->m_indice[indexNo]->m_splitFactor) {
			unlockMeta(session, IL_U);
			return;
		}

		dualFlushAndBump(session, timeout);
		//д��־
		LsnType lsn = writeAlterTableArgLog(session, m_tableDef->m_id , (int)SPLIT_FACTORS, indexNo, splitFactor);
		m_db->getTxnlog()->flush(lsn);

	} else {
		assert(indexNo < m_tableDef->m_numIndice);
		if (splitFactor == m_tableDef->m_indice[indexNo]->m_splitFactor) {
			return;
		}
	}

	//�޸�����
	m_tableDef->m_indice[indexNo]->m_splitFactor = splitFactor;
	m_indice->getIndex(indexNo)->setSplitFactor(splitFactor);

	writeTableDef();

	if (!redo) {
		unlockMeta(session, IL_X);
	}
}

/**
 * �޸Ķѡ����������������ļ���չ��С����λҳ������
 *
 * @param session		�Ự��
 * @param incrSize		�ѡ����������������ļ���չ��С����λҳ������
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterIncrSize(Session *session, u16 incrSize, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	
	if (!TableDef::isIncrSizeValid(incrSize)) {
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_INVALID_INCR_SIZE,
			incrSize, TableDef::MIN_INCR_SIZE, TableDef::MAX_INCR_SIZE);
	}

	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	checkPermission(session, redo);
	if (incrSize == m_tableDef->m_incrSize)
		return;
	if (!redo)
		preAlter(session, (int)INCR_SIZE, incrSize, timeout);
	m_tableDef->m_incrSize = incrSize;
	writeTableDef();
}

/**
 * �޸�ѹ������
 * @param session       �Ự
 * @param compressRows  �Ƿ����ü�¼ѹ��
 * @param timeout       ������ʱʱ�䣬��λ�롣
 * @param path          �����·��
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 */
void Table::alterCompressRows(Session *session, bool compressRows, int timeout, bool redo) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;

	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}
	
	checkPermission(session, redo);
	if (compressRows == m_tableDef->m_isCompressedTbl)
		return;

	if (compressRows && m_tableDef->m_recFormat == REC_FIXLEN) {
		//��ʱ��֧�ֽ�������ʽ���Ϊѹ����
		NTSE_THROW(EL_ERROR, "Currently NTSE doesn't support define fix length table to compressed table! " \
			"You should set fixlen attribute to false."); 
	}

	if (!redo)
		preAlter(session, (int)COMPRESS_ROWS, compressRows, timeout);

	m_tableDef->m_isCompressedTbl = compressRows;
	if (compressRows) {
		/**
		 * ����Ǵӷ�ѹ�������Ϊѹ��������֮ǰ������û�л��������飬
		 * ����Ҫ����Ĭ��ѹ�����ã��Լ������������
		 */
		assert(m_tableDef->m_recFormat == REC_VARLEN);
		m_tableDef->m_recFormat = REC_COMPRESSED;
		if (m_tableDef->m_rowCompressCfg == NULL)
			m_tableDef->m_rowCompressCfg = new RowCompressCfg();
		assert(m_tableDef->m_rowCompressCfg != NULL);
		//FIXME: ����ֻ��Ĭ�Ϸ�Ϊ1��������
		if(m_tableDef->m_numColGrps == 0) {
			m_tableDef->setDefaultColGrps();	
		} else {
			assert(m_tableDef->m_colGrps);
		}
	} else {
		/**
		 * ����Ǵ�ѹ�������Ϊ��ѹ�����ɽ����¼��ʽ��ΪREC_VALLEN, 
		 * ����ʵ���ϱ��¼�����Ƕ�����ʽ�ģ���Ϊ֮ǰѹ������Ѿ�ͳһ�ñ䳤�����洢�ˣ����Կ�������䶯
		 * ����������鶨����ܻ����ã����Բ���ɾ��
		 */
		m_tableDef->m_recFormat = REC_VARLEN;
		assert(m_tableDef->m_numColGrps > 0);
		assert(m_tableDef->m_colGrps != NULL);
		assert(m_tableDef->m_rowCompressCfg != NULL);
	}
	
	writeTableDef();
}

/**
 * �޸Ķ�����
 * 
 * ֻ֧�ִӶ����ѵ��䳤�ѵĵ����޸ģ�������ͨ������ȥ�������ܺ�ʱ�ϳ�
 * �������������޸Ĳ�����ͬ���ǣ��޸Ķ����Ͳ������preAlter��ˢ�����ݺ�д��־����
 * ֻ����TblMntAlterColumn::alterTable����д��־�������TblMntAlterColumn::alterTable
 * ��д��־֮ǰϵͳ�������������redo�޸Ķ����͵Ĳ���(ע��TblMntAlterColumn::alterTable
 * ֻ�����滻���ļ�֮ǰ��д��־)
 *
 * @param session       �Ự
 * @param fixlen        �¶������Ƿ��Ƕ�������
 * @param timeout       ������ʱʱ�䣬��λ��
 * @throw NtseException ���䳤�Ѹ�Ϊ�����ѣ������ڿ�����ʱ����
 */
void Table::alterHeapFixLen(Session *session, bool fixlen, int timeout) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;
	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	guard.attach(session, this);

	checkPermission(session, false);
	if (fixlen) {
		if (m_tableDef->m_recFormat == REC_FIXLEN) {//�Ѿ��Ƕ�����
			assert(m_tableDef->m_fixLen);
			assert(m_tableDef->m_origRecFormat == m_tableDef->m_recFormat);
			return;
		} else {//��֧�ֽ��䳤�Ѹ�Ϊ������
			NTSE_THROW(NTSE_EC_GENERIC, "Can't change variable length heap to fixed length !"); 
		}
	} else {
		if (m_tableDef->m_recFormat != REC_FIXLEN)//�Ѿ����Ƕ�����
			return;
	}

	//�Ӷ�����ͨ������ɾ�еı������޸�Ϊ�䳤��
	assert(m_tableDef->m_recFormat == REC_FIXLEN && m_tableDef->m_origRecFormat == REC_FIXLEN);
	assert(!fixlen);
	assert(getMetaLock(session) == IL_U);
	try {
		TblMntAlterHeapType tableAlterHeapType(this, session->getConnection());
		tableAlterHeapType.alterTable(session);
	} catch (NtseException &e) {
		assert(getMetaLock(session) == IL_U);
		throw e;
	}
	assert(getMetaLock(session) == IL_U);
}

/**
 * �����޸������鶨��
 *
 * ���޸������鶨��Ĺ����в��ӱ�������Ӧ����û�����
 * @param session  �Ự
 * @param valueStr ����ֵ
 * @param timeout  ������ʱʱ�䣬��λ��
 * @throw NtseException �޸Ĳ���ʧ�ܡ�
 */
void Table::alterColGrpDef(Session *session, const char *valueStr, int timeout) throw(NtseException) {
	assert(NULL != session);
	assert(NULL != valueStr);

	MemoryContext *mc = session->getMemoryContext();
	McSavepoint mcs(mc);
	TblLockGuard guard;

	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	guard.attach(session, this);

	checkPermission(session, false);
	if (m_hasCprsDict)//������к���ѹ���ֵ䣬������������������ã���Ϊ��ʱ������ѹ����¼����ԭ�������ʽ�洢
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_COLUMNGROUP_HAVE_DICTIONARY);
	
	Array<ColGroupDef *> *colGrpDefArr = NULL;
	try {
		colGrpDefArr = ColumnGroupParser::parse(m_tableDef, valueStr);
		if (0 == colGrpDefArr->getSize()) 
			NTSE_THROW(NTSE_EC_INVALID_COL_GRP, TableArgAlterHelper::MSG_EXCEPTION_EMPTY_COL_GRP);

		alterColGrpDef(session, colGrpDefArr, timeout, false);
		delete colGrpDefArr;
		colGrpDefArr = NULL;
	} catch (NtseException &e) {
		if (colGrpDefArr) {
			for (uint i = 0; i < colGrpDefArr->getSize(); i++)
				delete (*colGrpDefArr)[i];
			delete colGrpDefArr;
			colGrpDefArr = NULL;
		}
		throw e;
	}
}

/**
* �����޸������鶨��
* @param session   �Ự
* @param numColGrp �µ����������
* @param colGrpDef �µ������鶨��
* @param timeout   ������ʱʱ�䣬��λ��
* @param redo      �Ƿ�����redo�׶�
* @throw NtseException �޸Ĳ���ʧ��
*/
void Table::alterColGrpDef(Session *session, Array<ColGroupDef *> *colGrpDef, int timeout, bool redo /*=false*/) throw(NtseException) {
	assert(redo || getMetaLock(session) == IL_U);
	assert(colGrpDef);

	u8 numOldColGrps = m_tableDef->m_numColGrps;
	m_tableDef->m_numColGrps = 0;
	ColGroupDef **oldColGrpDef = m_tableDef->m_colGrps;
	m_tableDef->m_colGrps = NULL;

	if (!redo) {
		dualFlushAndBump(session, timeout);
		LsnType lsn = writeAlterTableArgLog(session, m_tableDef->m_id, COLUMN_GROUPS, colGrpDef);
		m_db->getTxnlog()->flush(lsn);
	}

	m_tableDef->setColGrps(colGrpDef, false);

	//�־û�����
	writeTableDef();

	//�ͷž������鶨��ռ�õ��ڴ�
	if (oldColGrpDef) {		
		for (u16 i = 0; i < numOldColGrps; i++) {
			delete oldColGrpDef[i];
		}
		delete [] oldColGrpDef;
		oldColGrpDef = NULL;
	}
}

/**
 * �����޸�ȫ��ѹ���ֵ����
 * @param session       �Ự
 * @param cmdType       ��������
 * @param newvalue      �ֵ������ֵ
 * @param timeout       ������ʱʱ��
 * @param redo          �Ƿ�����redo�׶�
 * @throw NtseException �޸Ĺ��̳���
 */
void Table::alterDictionaryArg(Session *session, int cmdType, uint newValue, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(session);
	assert(cmdType == COMPRESS_DICT_SIZE || cmdType == COMPRESS_DICT_MIN_LEN || cmdType == COMPRESS_DICT_MAX_LEN || cmdType == COMPRESS_THRESHOLD);

	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	checkPermission(session, redo);
	if (!m_tableDef->m_isCompressedTbl)//����ѹ����
		NTSE_THROW(NTSE_EC_GENERIC, "The table isn\'t set as a compression table, please alter \'compress_rows\' argument first.");
	//������õ����ֵ��С���ֵ���ȣ� �����Ѿ����ֵ������²������������
	if (m_hasCprsDict && cmdType != COMPRESS_THRESHOLD)
		NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_DICT_ARG_HAVE_DICTIONARY);

	assert(m_tableDef->m_rowCompressCfg);

	RowCompressCfg newCompressCfg(*m_tableDef->m_rowCompressCfg);
	switch(cmdType) {
		case COMPRESS_DICT_SIZE:
			if (!RowCompressCfg::validateDictSize(newValue)) 
				NTSE_THROW(NTSE_EC_GENERIC, INVALID_DIC_SIZE_INFO, newValue, RowCompressCfg::MIN_DICTIONARY_SIZE, RowCompressCfg::MAX_DICTIONARY_SIZE);
			if (newCompressCfg.dicSize() == newValue)
				return;
			newCompressCfg.setDicSize(newValue);
			break;
		case COMPRESS_DICT_MIN_LEN:
			if (!RowCompressCfg::validateDictMinLen(newValue))
				NTSE_THROW(NTSE_EC_GENERIC, INVALID_DIC_MIN_LEN_INFO, newValue, RowCompressCfg::MIN_DIC_ITEM_MIN_LEN, RowCompressCfg::MAX_DIC_ITEM_MIN_LEN);
			if (newCompressCfg.dicItemMinLen() == newValue)
				return;
			newCompressCfg.setDicItemMinLen((u16)newValue);
			break;
		case COMPRESS_DICT_MAX_LEN:
			if (!RowCompressCfg::validateDictMaxLen(newValue)) 		
				NTSE_THROW(NTSE_EC_GENERIC, INVALID_DIC_MAX_LEN_INFO, newValue, RowCompressCfg::MIN_DIC_ITEM_MAX_LEN, RowCompressCfg::MAX_DIC_ITEM_MAX_LEN);
			if (newCompressCfg.dicItemMaxLen() == newValue)
				return;
			newCompressCfg.setDicItemMaxLen((u16)newValue);
			break;
		case COMPRESS_THRESHOLD:
			if (!RowCompressCfg::validateCompressThreshold(newValue))
				NTSE_THROW(NTSE_EC_GENERIC, INVALID_COMPRESS_THRESHOLD_INFO, newValue);
			if (newCompressCfg.compressThreshold() == newValue)
				return;
			newCompressCfg.setCompressThreshold((u8)newValue);
			break;
		default:
			assert(false);
			return;
	}

	if (!redo) {
		preAlter(session, (int)cmdType, newValue, timeout);
	}
	delete m_tableDef->m_rowCompressCfg;
	m_tableDef->m_rowCompressCfg = new RowCompressCfg(newCompressCfg);
	writeTableDef();
}

#ifdef TNT_ENGINE
/**
 * �޸�table�Ƿ�֧���������
 *
 * @param session		�Ự��
 * @param supportTrx	�Ƿ�֧����������� true ��ʾ֧�����������false��ʾ��֧�����������
 * @param timeout		������ʱʱ�䣬��λ�롣
 * @param inLockTables  �����Ƿ���Lock Tables ���֮��
 * @param redo			�Ƿ�����ִ��redo������
 * @throw NtseException	�޸Ĳ���ʧ�ܡ�
 *
 * @note ����ʼ״̬��Ŀ��״̬һ�£��򲻲�����
 */
void Table::alterTableStatus(Session *session, TableStatus tableStatus, int timeout, bool inLockTables, bool redo) throw(NtseException) {
	assert(NULL != session);

	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	if (session->getConnection()->isTrx()) {
		//������������ӣ���Table::alterTableStatus�ض���TNTTable�������ģ���ʱ��״̬�Ѿ�Ϊchanging
		//���ڴ�trx�ı�Ϊchanging״̬�ǳ���meta lock X������½��У�
		//ϵͳȨ�޼�鱣֤����changing״̬�²��������alter table argument���������Դ�ʱ����Ҫ�ٽ���Ȩ�޼��
		assert(redo || TS_CHANGING == m_tableDef->m_tableStatus);
	} else if (inLockTables){
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "This Connection is in lock tables, you must unlock tables");
	} else if (TS_NON_TRX != m_tableDef->getTableStatus()) {//����Ƿ���������ֻ�ܲ����������
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Non-Transaction Connection can operate Non-Transaction Table(%s) Only, Now Table Status is %s", 
			m_tableDef->m_name, m_tableDef->getTableStatusDesc());
	}
	if (tableStatus == m_tableDef->m_tableStatus)
		return;
	if (!redo)
		preAlter(session, (int)SUPPORT_TRX, tableStatus, timeout);
	m_tableDef->m_tableStatus = tableStatus;
	writeTableDef();
}
#endif

/** ���������Ȩ��
 * @param session �Ự
 * ���û����Ӧ��Ȩ��throw exception
 */
void Table::checkPermission(Session *session, bool redo) throw(NtseException) {
	//redo����£�connectionϵͳĬ��ֵ(true)������û����Ȩ�޼��
	//������alter table argument���ȼ��Ȩ�ޣ���дredo��־���ָ�����ͨ��redo�������ʾ���û������
	if (redo) {
		return;
	}
	assert(getMetaLock(session) != IL_NO);
	TableStatus tableStatus = m_tableDef->getTableStatus();
	bool trxConn = session->getConnection()->isTrx();
	checkPermissionReal(trxConn, tableStatus, m_path);
	/*if (trxConn && tableStatus != TS_TRX) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Transactional Connection can operate Transaction Table Only, Now Table Status is %s",
			m_tableDef->getTableStatusDesc());
	} else if (!trxConn && tableStatus != TS_NON_TRX) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Non-Transactional Connection can operate Non-Transaction Table Only, Now Table Status is %s",
			m_tableDef->getTableStatusDesc());
	}*/
	//ֻ���������Ӳ�����������������Ӳ������������ܲ�throw exception
}

/** ���������Ȩ��
 * @param trxConn �Ƿ�Ϊ��������
 * @param tblStatus ���״̬��Ϣ
 * @param path ��·��
 */
void Table::checkPermissionReal(bool trxConn, TableStatus tblStatus, const char *path) throw(NtseException) {
	if (tblStatus == TS_CHANGING) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "The table(%s) status is changing", path);
	} else if (tblStatus == TS_ONLINE_DDL) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "The table(%s) is online ddl operation", path);
	} else if (!trxConn && tblStatus == TS_TRX) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Non-Transactional Connection can't operate Transaction Table(%s)", path);
	} else if (!trxConn && tblStatus == TS_SYSTEM) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Non-Transactional Connection can't operate System Table(%s)", path);
	} else if (trxConn && tblStatus == TS_NON_TRX) {
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Transactional Connection can't operate Non-Transaction Table(%s)", path);
	} 
}

/**
 * ˢ�����������ݡ�
 *
 * @pre				�Ѿ�����Ԫ����IL_U��������IS��
 * @param session	�Ự��
 */
void Table::shareFlush(Session *session) {
	assert(NULL != session);
#ifdef TNT_ENGINE
	if(session->getTrans() == NULL)
		assert(m_metaLock.isLocked(session->getId(), IL_U) &&  m_tblLock.isLocked(session->getId(), IL_IS));	
#endif

	flush(session);
}

/**
 * ����ˢ�����������ݡ���һ���ڹ����ģʽˢ�������ݣ��ڶ��μ�IL_X����ˢ�����ݡ�
 *
 * @pre					�Ѿ�����Ԫ����IL_U����
 * @post				��ִ�����쳣���������ݱ�����������Ԫ����IL_X���������ͷų��е�����
 *
 * @param session		�Ự��
 * @param timeout		����ʱʱ�䣬��λ�롣
 * @throw NtseException	����ʧ�ܡ�
 */
void Table::dualFlush(Session *session, int timeout) throw(NtseException) {
	assert(NULL != session);
#ifdef TNT_ENGINE
	if(session->getTrans() == NULL)
		assert(m_metaLock.isLocked(session->getId(), IL_U));
#endif
	try {
		if (getLock(session) != IL_IS)
			lock(session, IL_IS, timeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		unlockMeta(session, IL_U);
		throw e;
	}
	shareFlush(session);;
	unlock(session, IL_IS);

	try {
		upgradeMetaLock(session, IL_U, IL_X, timeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		unlockMeta(session, IL_U);
		throw e;
	}
	flush(session);
	assert(m_metaLock.isLocked(session->getId(), IL_X));
}

/**
 * �ָ�������޸ġ�
 *
 * @param session		�Ự��
 * @param log			��־��
 * @throw NtseException ��������ʧ�ܡ�
 */
void Table::redoAlterTableArg(Session *session, const LogEntry *log) throw (NtseException) {
	assert((NULL != m_db) && (NULL != session) && (NULL != log));

	Stream s(log->m_data, log->m_size);
	int cmdType;
	try {
		s.read(&cmdType);
	} catch (NtseException &) {
		assert(false);			//no exception expected.
	}

	int timeout =  m_db->getConfig()->m_tlTimeout;
	switch (cmdType) {
		case USEMMS:
			{
				uint enable = 0;
				s.read(&enable);
				alterUseMms(session, (enable != 0), timeout, true);
			}
			break;
		case CACHE_UPDATE:
			{
				uint enable = 0;
				s.read(&enable);
				alterCacheUpdate(session, (enable != 0), timeout, true);
			}
			break;
		case UPDATE_CACHE_TIME:
			{
				uint interval;
				s.read(&interval);
				alterCacheUpdateTime(session, interval, timeout, true);
			}
			break;
		case CACHED_COLUMNS:
			{
				u16 cols[Limits::MAX_COL_NUM];
				u16 numCols;
				bool enable = false;
				s.read(&numCols);
				for (u16 i = 0; i < numCols; i++) {
					s.read(&(cols[i]));
				}
				s.read(&enable);
				alterCachedColumns(session, cols, numCols, enable, timeout, true);
			}
			break;
		case COMPRESS_LOBS:
			{
				uint enable = 0;
				s.read(&enable);
				alterCompressLobs(session, (enable != 0), timeout, true);
			}
			break;
		case HEAP_PCT_FREE:
			{
				uint pctFree;
				s.read(&pctFree);
				alterHeapPctFree(session, (u8)pctFree, timeout, true);
			}
			break;
		case SPLIT_FACTORS:
			{
				u8 indexNo;
				s8 splitFactor;
				s.read(&indexNo);
				s.read(&splitFactor);
				alterSplitFactors(session, indexNo, splitFactor, timeout, true);
			}
			break;
		case INCR_SIZE:
			{
				uint incrSize;
				s.read(&incrSize);
				alterIncrSize(session, (u16)incrSize, timeout, true);
			}
			break;
		case COMPRESS_ROWS:
			{
				uint compress_rows = false;
				s.read(&compress_rows);
				alterCompressRows(session, compress_rows != 0, timeout, true);
			}
			break;
		case COLUMN_GROUPS:
			{
				u8 numColGrps = 0;
				s.read(&numColGrps);
				Array<ColGroupDef *> colGrpDefArr;
				for (u16 i = 0; i < numColGrps; i ++) {
					ColGroupDef *colGrpDef = new ColGroupDef();
					assert(colGrpDef);
					s.skip(colGrpDef->read(log->m_data + s.getSize(), log->m_size - s.getSize()));
					colGrpDefArr.push(colGrpDef);
				}
				try {
					alterColGrpDef(session, &colGrpDefArr, timeout, true);
				} catch (NtseException &e) {
					for (size_t i = 0; i < colGrpDefArr.getSize(); i++)
						delete colGrpDefArr[i];
					throw e;
				}
			}
			break;
		case COMPRESS_DICT_SIZE:
		case COMPRESS_DICT_MIN_LEN:
		case COMPRESS_DICT_MAX_LEN:
		case COMPRESS_THRESHOLD:
			{
				uint newValue;
				s.read(&newValue);
				alterDictionaryArg(session, cmdType, newValue, timeout, true);
			}
			break;
		case SUPPORT_TRX:
			{
				uint tableStatus = 0;
				s.read(&tableStatus);
				assert(tableStatus == TS_TRX || tableStatus == TS_NON_TRX);
				//������������κ�ʱ��ת��Ϊ����
				//������ת��Ϊ��������redo insert�ǲ���hash������redo purge phase2�ܽ��ڴ�Ѽ�¼purge��
				alterTableStatus(session, (TableStatus)tableStatus, timeout, false, true);
			}
			break;
		default:
			NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_WRONG_COMMAND_TYPE);
			break;
	}
}


/**
 * д������޸���־��
 *
 * @param session	�Ự��
 * @param tableId	��ID��
 * @param cmdType	�޸��������͡�
 * @param newValue	�޸������ֵ��
 * @return			��־LSN�����Ự����Ϊ��д��־�򷵻�INVALID_LSN��
 */
u64 Table::writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, uint newValue) {
	assert(NULL != session);
	assert((cmdType > MIN_TBL_ARG_CMD_TYPE_VALUE) && (cmdType < MAX_TBL_ARG_CMD_TYPE_VALUE));

	const size_t bytes = sizeof(u16) + 2 * sizeof(int);
	byte logData[bytes];
	Stream s(logData, sizeof(logData));
	try {
		s.write(cmdType);
		s.write(newValue);
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}

/**
* д������޸���־��
*
* @param session	�Ự��
* @param tableId	��ID��
* @param cmdType	�޸��������͡�
* @param numColGrps	�޸ĵ����������
* @param colGrpDef  �µ������鶨��
* @return			��־LSN�����Ự����Ϊ��д��־�򷵻�INVALID_LSN��
*/
u64 Table::writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, Array<ColGroupDef *> *colGrpDefArr) {
	assert(NULL != session);
	assert(NULL != colGrpDefArr);
	assert(cmdType == COLUMN_GROUPS);

	size_t bytes = sizeof(u16) + sizeof(u8) * 2 + sizeof(int);
	for (u16 i = 0; i < colGrpDefArr->getSize(); i++) {
		bytes += (*colGrpDefArr)[i]->getSerializeSize();
	}
	McSavepoint mcs(session->getMemoryContext());
	byte *logData = (byte *)session->getMemoryContext()->alloc(bytes);
	Stream s(logData, bytes);
	try {
		s.write(cmdType);
		u8 numColGrps = (u8)colGrpDefArr->getSize();
		s.write(numColGrps);
		for (u16 i = 0; i < numColGrps; i++) {
			s.skip((*colGrpDefArr)[i]->write(logData + s.getSize(), bytes - s.getSize()));
		}
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}

/**
 * д������޸���־��
 *
 * @param session	�Ự��
 * @param tableId	��ID��
 * @param cmdType	�޸��������͡�
 * @param cols		�޸ĵ��ж���
 * @param numCols	�޸��еĸ�����
 * @param enable	�޸ĵ�Ŀ��ֵ��
 * @return			��־LSN�����Ự����Ϊ��д��־�򷵻�INVALID_LSN��
 */
u64 Table::writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, u16 *cols, u16 numCols, bool enable) {
	assert(NULL != session);
	assert((cmdType > MIN_TBL_ARG_CMD_TYPE_VALUE) && (cmdType < MAX_TBL_ARG_CMD_TYPE_VALUE));
	assert(NULL != cols);

	const int bytes = sizeof(int) + sizeof(u16) + Limits::MAX_COL_NUM * sizeof(u16) + sizeof(bool);
	byte logData[bytes];
	Stream s(logData, sizeof(logData));
	try {
		s.write(cmdType);
		s.write(numCols);
		for (size_t i = 0; i < numCols; i++) {
			s.write(cols[i]);
		}
		s.write(enable);
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}

/**
 * д������޸���־��
 *
 * @param session		�Ự��
 * @param tableId		��ID��
 * @param cmdType		�޸��������͡�
 * @param indexNo		�޸ĵ�index No��
 * @param splitFactor	�޸ĵķ���ϵ����
 * @return				��־LSN�����Ự����Ϊ��д��־�򷵻�INVALID_LSN��
 */
u64 Table::writeAlterTableArgLog(Session *session, u16 tableId, int cmdType, u8 indexNo, s8 splitFactor) {
	assert(NULL != session);
	assert((cmdType > MIN_TBL_ARG_CMD_TYPE_VALUE) && (cmdType < MAX_TBL_ARG_CMD_TYPE_VALUE));

	const size_t bytes = sizeof(int) + sizeof(u8) + sizeof(s8);
	byte logData[bytes];
	Stream s(logData, sizeof(logData));
	try {
		s.write(cmdType);
		s.write(indexNo);
		s.write(splitFactor);
	} catch (NtseException &) {
		assert(false);	// ���ﲻ���ܳ����쳣
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}
}

