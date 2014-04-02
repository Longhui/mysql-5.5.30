/**
 * 表参数修改相关操作实现
 *
 * @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
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
 * 构造函数。
 * 初始化命令映射表
 */
TableArgAlterCommandMapTool::TableArgAlterCommandMapTool() : m_mp(TableArgAlterHelper::NUM_CMDS) {
	initCommandMap();
}

/**
 * 获取命令映射表
 *
 * @return 表参数修改命令映射表
 */
Hash<const char *, TableArgAlterCmdType, Hasher<const char *>, Equaler<const char *, const char *> > *TableArgAlterCommandMapTool::getCmdMap() {
	return &m_mp;
}

/**
 * 初始化表修改参数命令映射。
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
 * 构造函数。
 *
 * @param db		数据库
 * @param conn		数据库连接
 * @param parser	命令解析
 * @param timeout	锁超时时间，单位秒。
 * @param inLockTables  操作是否处于Lock Tables 语句之中
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
 * 析构函数。
 *
 * 关闭表，释放会话和数据库连接，释放解析器。
 */
TableArgAlterHelper::~TableArgAlterHelper() {
	if (NULL != m_parser) {
		delete m_parser;
		m_parser = NULL;
	}
}

/** 
 * 将字符串转换成大写。
 *
 * @param str	转换的字符串
 * @param len	字符串的长度
 *
 */
void TableArgAlterHelper::toUpper(char *str, size_t len) {
	assert(NULL != str);
	for (size_t i = 0; i < len; i++) {
		str[i] = (char) toupper(str[i]);
	}
}

/** 
 * 检查Cached Columns是否超出最大限制。
 * 
 * @pre tableDef对应的表已经加锁。
 *
 * @param session		会话。
 * @param tableDef		表定义。
 * @param cols			将enable的列下标数组。
 * @param numClos		将enable的列数组大小。
 * @param numEnabled	out 之前已经enble的列数。
 * @prarm numToEnable	将新增enable的列数。
 *
 * @return true 如果检查没有超出限制；false如果enable的总列数超出最大限制。
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
 * 修改表参数。
 *
 * @throw NtseException 若修改操作失败。
 */
void TableArgAlterHelper::alterTableArgument() throw(NtseException) {
	assert((NULL != m_db) && (NULL != m_parser));
	//命令的格式：alter table set scheme.table_name.name = value
	m_parser->match(STR_TABLE);
	m_parser->match(STR_SET);

	//解析scheme.table_name.name

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
	
	//将命令名转成大写
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

	//获取会话和表实例
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

/** 真正执行修改表定义参数的命令
 * @param session 会话
 * @param parFiles 分区表路径
 * @param name  修改参数的名称
 * @param value 修改参数的值
 */
void TableArgAlterHelper::alterTableArgumentReal(Session *session, vector<string> *parFiles, const char *name, const char *value) throw(NtseException) {
	Table *table = NULL;

	try {
		for (uint i = 0; i < parFiles->size(); i++) {
			table = m_db->openTable(session, parFiles->at(i).c_str());
			//非分区表执行修改表
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
 * 修改表参数。
 *
 * @param session		会话。
 * @param name			修改的对象名。
 * @param valueStr		修改的值。
 * @param timeout		表锁超时时间，单位秒
 * @param inLockTables  操作是否处于Lock Tables 语句之中
 * @throw NtseException	修改操作失败。
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
			//Hash表不能匹配时返回 0 即： MIN_TBL_ARG_CMD_TYPE_VALUE
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

/** 准备修改表参数
 * @pre 加了元数据U锁
 * @post 元数据锁升级为X
 *
 * @param session 会话
 * @param cmdType 参数类型
 * @param newValue 新值
 * @param timeout 加锁超时时间，单位秒
 * @throw NtseException 加锁超时
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

/** 刷出表中脏数据并且修改表的flushLsn
 * @pre 加了元数据U锁
 * @post 元数据锁升级为X
 *
 * @param session 会话
 * @param timeout 加锁超时时间，单位秒
 * @throw NtseException 加锁超时
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
 * 修改使用MMS。
 *
 * @param session		会话。
 * @param useMms		是否使用MMS。 true 表示使用MMS，false表示不使用MMS。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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
 * 修改使用MMS缓存更新。
 *
 * @param session		会话。
 * @param cacheUpdate	是否使用MMS。 true 表示使用缓存更新，false表示不使用缓存更新。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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

/** 执行真正的修改是否启用MMS缓存更新功能
 * @pre 加了元数据U锁；需要真正的修改是否启用MMS缓存更新功能
 * @post 当redo为false时元数据锁已经升级到X锁，表脏数据已经刷出
 *
 * @param session 会话
 * @param cacheUpdate 是否启用MMS缓存更新功能
 * @param lockTimeout 加锁超时时间，单位秒
 * @throw NtseException 加锁超时
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
 * 修改使用MMS缓存更新周期。
 *
 * @param session		会话。
 * @param interval		MMS缓存更新周期，单位秒。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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
 * 修改使用MMS缓存更新列。
 *
 * @param session		会话。
 * @param valueStr		缓存列信息。格式为 'enable|disable 列名[，列名] '。
 * @param timeout		表锁超时时间，单位秒。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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
	//检查提供的列名是否有效
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
	//检查enable的总列数是否超出最大限制
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

	//执行修改
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

	//释放资源
	delete[] cols;
	for (size_t i = 0; i < colNameArray->getSize(); i++) {
		delete []((*colNameArray)[i]);
	}
	delete colNameArray;
}

/**
 * 修改使用MMS缓存更新列
 *
 * @pre		在非redo模式下，m_metaLock 已加IL_U;
 * @post	释放源数据锁和数据锁
 *
 * @param session		会话
 * @param cols			MMS缓存列下标数组。
 * @param numCols		MMS缓存列数组大小。
 * @param enable		是否启用缓存更新。true表示启用；false表示禁用。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
 * @see <code> TableDef::check </code> 修改必须满足表定义检查。
 */
void Table::alterCachedColumns(Session *session, u16 *cols, u16 numCols, bool enable, int timeout, bool redo/* = false*/) throw(NtseException) {
	assert(NULL != session);
	assert(NULL != cols);
	assert((numCols > 0) && (numCols < MmsTable::MAX_UPDATE_CACHE_COLUMNS));
	assert(redo || m_metaLock.isLocked(session->getId(), IL_U));
	//在MMS未启用或cache update未启用时禁止修改字段的cache update
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
		//在建立索引的列上，禁止设置为cached column.
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
	//若状态一致，退出
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
 * 修改使用大对象压缩。
 *
 * @param session		会话。
 * @param compressLobs	是否其实使用大对象压缩。true代表使用，false代表不使用。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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
 * 修改插入记录时页面预留空闲空间的百分比。
 *
 * @param session		会话。
 * @param pctFree		插入记录时页面预留空闲空间的百分比。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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
 * 修改索引分裂系数。
 *
 * @param session		会话。
 * @param valueStr		分裂系数信息。格式为 '索引名 分裂系数'。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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
 * 修改索引分裂系数。
 *
 * @pre		在非redo模式下，m_metaLock 已加IL_U;
 * @post	释放源数据锁和数据锁
 *
 * @param session		会话。
 * @param indexNo		索引下标。
 * @param splitFactor	索引分裂时左边页面存放数据百分比。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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

		//若状态一致，退出
		if (splitFactor == m_tableDef->m_indice[indexNo]->m_splitFactor) {
			unlockMeta(session, IL_U);
			return;
		}

		dualFlushAndBump(session, timeout);
		//写日志
		LsnType lsn = writeAlterTableArgLog(session, m_tableDef->m_id , (int)SPLIT_FACTORS, indexNo, splitFactor);
		m_db->getTxnlog()->flush(lsn);

	} else {
		assert(indexNo < m_tableDef->m_numIndice);
		if (splitFactor == m_tableDef->m_indice[indexNo]->m_splitFactor) {
			return;
		}
	}

	//修改配置
	m_tableDef->m_indice[indexNo]->m_splitFactor = splitFactor;
	m_indice->getIndex(indexNo)->setSplitFactor(splitFactor);

	writeTableDef();

	if (!redo) {
		unlockMeta(session, IL_X);
	}
}

/**
 * 修改堆、索引或大对象数据文件扩展大小，单位页面数。
 *
 * @param session		会话。
 * @param incrSize		堆、索引或大对象数据文件扩展大小，单位页面数。
 * @param timeout		表锁超时时间，单位秒。
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
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
 * 修改压缩表定义
 * @param session       会话
 * @param compressRows  是否启用记录压缩
 * @param timeout       表锁超时时间，单位秒。
 * @param path          表相对路径
 * @throw NtseException	修改操作失败。
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
		//暂时不支持将定长格式表改为压缩表
		NTSE_THROW(EL_ERROR, "Currently NTSE doesn't support define fix length table to compressed table! " \
			"You should set fixlen attribute to false."); 
	}

	if (!redo)
		preAlter(session, (int)COMPRESS_ROWS, compressRows, timeout);

	m_tableDef->m_isCompressedTbl = compressRows;
	if (compressRows) {
		/**
		 * 如果是从非压缩表更改为压缩表，并且之前表定义中没有划分属性组，
		 * 则需要分配默认压缩配置，以及给属性组分组
		 */
		assert(m_tableDef->m_recFormat == REC_VARLEN);
		m_tableDef->m_recFormat = REC_COMPRESSED;
		if (m_tableDef->m_rowCompressCfg == NULL)
			m_tableDef->m_rowCompressCfg = new RowCompressCfg();
		assert(m_tableDef->m_rowCompressCfg != NULL);
		//FIXME: 这里只是默认分为1个属性组
		if(m_tableDef->m_numColGrps == 0) {
			m_tableDef->setDefaultColGrps();	
		} else {
			assert(m_tableDef->m_colGrps);
		}
	} else {
		/**
		 * 如果是从压缩表更改为非压缩表，可将表记录格式改为REC_VALLEN, 
		 * 尽管实际上表记录可能是定长格式的，因为之前压缩表就已经统一用变长堆来存储了，所以可以无需变动
		 * 而表的属性组定义可能还有用，所以不能删除
		 */
		m_tableDef->m_recFormat = REC_VARLEN;
		assert(m_tableDef->m_numColGrps > 0);
		assert(m_tableDef->m_colGrps != NULL);
		assert(m_tableDef->m_rowCompressCfg != NULL);
	}
	
	writeTableDef();
}

/**
 * 修改堆类型
 * 
 * 只支持从定长堆到变长堆的单向修改，由于是通过表拷贝去做，可能耗时较长
 * 跟其他表在线修改操作不同的是，修改堆类型不会调用preAlter先刷脏数据和写日志，而
 * 只是在TblMntAlterColumn::alterTable里面写日志，如果在TblMntAlterColumn::alterTable
 * 中写日志之前系统崩溃，不会进行redo修改堆类型的操作(注：TblMntAlterColumn::alterTable
 * 只是在替换表文件之前才写日志)
 *
 * @param session       会话
 * @param fixlen        新堆类型是否是定长类型
 * @param timeout       表锁超时时间，单位秒
 * @throw NtseException 将变长堆改为定长堆，或者在拷贝表时出错
 */
void Table::alterHeapFixLen(Session *session, bool fixlen, int timeout) throw(NtseException) {
	assert(NULL != session);
	TblLockGuard guard;
	lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
	guard.attach(session, this);

	checkPermission(session, false);
	if (fixlen) {
		if (m_tableDef->m_recFormat == REC_FIXLEN) {//已经是定长堆
			assert(m_tableDef->m_fixLen);
			assert(m_tableDef->m_origRecFormat == m_tableDef->m_recFormat);
			return;
		} else {//不支持将变长堆改为定长堆
			NTSE_THROW(NTSE_EC_GENERIC, "Can't change variable length heap to fixed length !"); 
		}
	} else {
		if (m_tableDef->m_recFormat != REC_FIXLEN)//已经不是定长堆
			return;
	}

	//从定长表通过不增删列的表在线修改为变长表
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
 * 在线修改属性组定义
 *
 * 在修改属性组定义的过程中不加表数据锁应该是没问题的
 * @param session  会话
 * @param valueStr 参数值
 * @param timeout  表锁超时时间，单位秒
 * @throw NtseException 修改操作失败。
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
	if (m_hasCprsDict)//如果表中含有压缩字典，则不允许进行属性组设置，因为此时可能有压缩记录按照原属性组格式存储
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
* 在线修改属性组定义
* @param session   会话
* @param numColGrp 新的属性组个数
* @param colGrpDef 新的属性组定义
* @param timeout   表锁超时时间，单位秒
* @param redo      是否发生在redo阶段
* @throw NtseException 修改操作失败
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

	//持久化到堆
	writeTableDef();

	//释放旧属性组定义占用的内存
	if (oldColGrpDef) {		
		for (u16 i = 0; i < numOldColGrps; i++) {
			delete oldColGrpDef[i];
		}
		delete [] oldColGrpDef;
		oldColGrpDef = NULL;
	}
}

/**
 * 在线修改全局压缩字典参数
 * @param session       会话
 * @param cmdType       命令类型
 * @param newvalue      字典参数新值
 * @param timeout       表锁超时时间
 * @param redo          是否发生在redo阶段
 * @throw NtseException 修改过程出错
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
	if (!m_tableDef->m_isCompressedTbl)//不是压缩表
		NTSE_THROW(NTSE_EC_GENERIC, "The table isn\'t set as a compression table, please alter \'compress_rows\' argument first.");
	//如果设置的是字典大小，字典项长度， 则在已经有字典的情况下不允许进行设置
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
 * 修改table是否支持事务操作
 *
 * @param session		会话。
 * @param supportTrx	是否支持事务操作。 true 表示支持事务操作，false表示不支持事务操作。
 * @param timeout		表锁超时时间，单位秒。
 * @param inLockTables  操作是否处于Lock Tables 语句之中
 * @param redo			是否正在执行redo操作。
 * @throw NtseException	修改操作失败。
 *
 * @note 若初始状态与目标状态一致，则不操作。
 */
void Table::alterTableStatus(Session *session, TableStatus tableStatus, int timeout, bool inLockTables, bool redo) throw(NtseException) {
	assert(NULL != session);

	TblLockGuard guard;
	if (!redo) {
		lockMeta(session, IL_U, timeout * 1000, __FILE__, __LINE__);
		guard.attach(session, this);
	}

	if (session->getConnection()->isTrx()) {
		//如果是事务连接，则Table::alterTableStatus必定有TNTTable调下来的，那时表状态已经为changing
		//由于从trx改变为changing状态是持有meta lock X的情况下进行，
		//系统权限检查保证在在changing状态下不允许进行alter table argument操作，所以此时不需要再进行权限检查
		assert(redo || TS_CHANGING == m_tableDef->m_tableStatus);
	} else if (inLockTables){
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "This Connection is in lock tables, you must unlock tables");
	} else if (TS_NON_TRX != m_tableDef->getTableStatus()) {//如果是非事务连接只能操作非事务表
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

/** 检查操作表的权限
 * @param session 会话
 * 如果没有响应的权限throw exception
 */
void Table::checkPermission(Session *session, bool redo) throw(NtseException) {
	//redo情况下，connection系统默认值(true)，所以没法做权限检查
	//但鉴于alter table argument是先检查权限，再写redo日志，恢复又是通过redo来做，故觉得没有问题
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
	//只有事务连接操作事务表，非事务连接操作非事务表才能不throw exception
}

/** 检查操作表的权限
 * @param trxConn 是否为事务连接
 * @param tblStatus 表的状态信息
 * @param path 表路径
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
 * 刷出表中脏数据。
 *
 * @pre				已经加了元数据IL_U锁和数据IS。
 * @param session	会话。
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
 * 两次刷出表中脏数据。第一次在共享的模式刷出脏数据，第二次加IL_X锁，刷出数据。
 *
 * @pre					已经加了元数据IL_U锁。
 * @post				若执行无异常，则脏数据被出，并加上元数据IL_X锁；否则释放持有的锁。
 *
 * @param session		会话。
 * @param timeout		锁超时时间，单位秒。
 * @throw NtseException	操作失败。
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
 * 恢复表参数修改。
 *
 * @param session		会话。
 * @param log			日志。
 * @throw NtseException 重做操作失败。
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
				//非事务可以在任何时候转化为事务
				//非事务转化为事务，由于redo insert是不插hash索引，redo purge phase2能将内存堆记录purge空
				alterTableStatus(session, (TableStatus)tableStatus, timeout, false, true);
			}
			break;
		default:
			NTSE_THROW(NTSE_EC_GENERIC, TableArgAlterHelper::MSG_EXCEPTION_WRONG_COMMAND_TYPE);
			break;
	}
}


/**
 * 写表参数修改日志。
 *
 * @param session	会话。
 * @param tableId	表ID。
 * @param cmdType	修改命令类型。
 * @param newValue	修改命令的值。
 * @return			日志LSN，若会话设置为不写日志则返回INVALID_LSN。
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
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}

/**
* 写表参数修改日志。
*
* @param session	会话。
* @param tableId	表ID。
* @param cmdType	修改命令类型。
* @param numColGrps	修改的属性组个数
* @param colGrpDef  新的属性组定义
* @return			日志LSN，若会话设置为不写日志则返回INVALID_LSN。
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
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}

/**
 * 写表参数修改日志。
 *
 * @param session	会话。
 * @param tableId	表ID。
 * @param cmdType	修改命令类型。
 * @param cols		修改的列对象。
 * @param numCols	修改列的个数。
 * @param enable	修改的目标值。
 * @return			日志LSN，若会话设置为不写日志则返回INVALID_LSN。
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
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}

/**
 * 写表参数修改日志。
 *
 * @param session		会话。
 * @param tableId		表ID。
 * @param cmdType		修改命令类型。
 * @param indexNo		修改的index No。
 * @param splitFactor	修改的分裂系数。
 * @return				日志LSN，若会话设置为不写日志则返回INVALID_LSN。
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
		assert(false);	// 这里不可能出现异常
	}
	return session->writeLog(LOG_ALTER_TABLE_ARG, tableId, logData, s.getSize());
}
}

