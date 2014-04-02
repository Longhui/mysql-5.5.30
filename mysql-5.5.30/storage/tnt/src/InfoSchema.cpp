#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#ifdef NTSE_KEYVALUE_SERVER
#include "keyvalue/KeyValueServer.h"
#endif

#define MYSQL_SERVER 1
#include <vector>
#include <string>
#include <list>
#include <sstream>
#ifdef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <sql_time.h>
#include <sql_show.h>
#include "ha_tnt.h"
#include <mysql/plugin.h>
#endif
#include "api/Database.h"
#ifndef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <sql_time.h>
#include <sql_show.h>
#include "ha_tnt.h"
#include <mysql/plugin.h>
#endif
#include "mysys_err.h"
#include "misc/ControlFile.h"
#include "mms/Mms.h"
#include "lob/Lob.h"
#include "btree/Index.h"

#ifdef NTSE_PROFILE
#include "misc/Profile.h"
#endif

using namespace tnt;

/* 获得BOOL值的字符串形式 */
static const char *getBoolStr(bool v) {
	return v ? "True" : "False";
};

/* 将time_t 格式转化成MYSQL_TIME 格式 */
static void convertTimeToMysqlTime(time_t time, MYSQL_TIME* mysqlTime) {
	struct tm tmTime;
	localtime_r(&time, &tmTime);
	localtime_to_TIME(mysqlTime, &tmTime);
	mysqlTime->time_type = MYSQL_TIMESTAMP_DATETIME;
}

/** NTSE_MUTEX_STATS表定义 */
ST_FIELD_INFO mutexStatsFieldInfo[] = {
	{"FILE",		256, MYSQL_TYPE_STRING,	0, 0, "File", SKIP_OPEN_TABLE},
	{"LINE",		4, MYSQL_TYPE_LONG,		0, 0, "Line", SKIP_OPEN_TABLE},
	{"NAME",		100, MYSQL_TYPE_STRING,	0, 0, "Name", SKIP_OPEN_TABLE},
	{"INSTANCES",	4, MYSQL_TYPE_LONG,		0, 0, "Number of Instances", SKIP_OPEN_TABLE},
	{"LOCKS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock Operations", SKIP_OPEN_TABLE},
	{"SPINS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Spins", SKIP_OPEN_TABLE},
	{"WAITS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Waits", SKIP_OPEN_TABLE},
	{"WAIT_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in Milisecons of Waits", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_MUTEX_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISMutexStats(void *p) {
	DBUG_ENTER("ha_tnt::initISMutexStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = mutexStatsFieldInfo;
	schema->fill_table = ha_tnt::fillISMutexStats;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_MUTEX_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISMutexStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISMutexStats");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_MUTEX_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISMutexStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISMutexStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	MuScanHandle scanHandle;
	const MutexUsage *usage;
	MutexUsage::beginScan(&scanHandle);
	while ((usage = MutexUsage::getNext(&scanHandle)) != NULL) {
		int col = 0;
		table->field[col++]->store(usage->m_allocFile, strlen(usage->m_allocFile), cs);
		table->field[col++]->store(usage->m_allocLine, true);
		table->field[col++]->store(usage->m_name, strlen(usage->m_name), cs);
		table->field[col++]->store(usage->m_instanceCnt, true);
		table->field[col++]->store(usage->m_lockCnt, true);
		table->field[col++]->store(usage->m_spinCnt, true);
		table->field[col++]->store(usage->m_waitCnt, true);
		table->field[col++]->store(usage->m_waitTime, true);
		if (schema_table_store_record(thd, table))
			break;
	}
	MutexUsage::endScan(&scanHandle);
	DBUG_RETURN(0);
}

/** NTSE_INTENTION_LOCK_STATS表定义 */
ST_FIELD_INFO intentionlockStatsFieldInfo[] = {
	{"FILE",		256, MYSQL_TYPE_STRING,	0, 0, "File", SKIP_OPEN_TABLE},
	{"LINE",		4, MYSQL_TYPE_LONG,		0, 0, "Line", SKIP_OPEN_TABLE},
	{"NAME",		100, MYSQL_TYPE_STRING,	0, 0, "Name", SKIP_OPEN_TABLE},
	{"INSTANCES",	4, MYSQL_TYPE_LONG,		0, 0, "Number of Instances", SKIP_OPEN_TABLE},
	{"LOCKS_NO",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock NO Operations", SKIP_OPEN_TABLE},
	{"LOCKS_IS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock IS Operations", SKIP_OPEN_TABLE},
	{"LOCKS_IX",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock IX Operations", SKIP_OPEN_TABLE},
	{"LOCKS_S",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock S  Operations", SKIP_OPEN_TABLE},
	{"LOCKS_SIX",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock SIX Operations", SKIP_OPEN_TABLE},
	{"LOCKS_U",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock U  Operations", SKIP_OPEN_TABLE},
	{"LOCKS_X",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock X  Operations", SKIP_OPEN_TABLE},
	{"SPINS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Spins", SKIP_OPEN_TABLE},
	{"WAITS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Waits", SKIP_OPEN_TABLE},
	{"WAIT_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in Milisecons of Waits", SKIP_OPEN_TABLE},
	{"FAILS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Lock Timeout", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_INTENTION_LOCK_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISIntentionLockStats(void *p) {
	DBUG_ENTER("ha_tnt::initISIntentionLockStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = intentionlockStatsFieldInfo;
	schema->fill_table = ha_tnt::fillISIntentionLockStats;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_INTENTION_LOCK_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISIntentionLockStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISIntentionLockStats");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_INTENTION_LOCK_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISIntentionLockStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISIntentionLockStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	IntentionLockUsageScanHandle scanHandle;
	const IntentionLockUsage *usage;
	IntentionLockUsage::beginScan(&scanHandle);
	while ((usage = IntentionLockUsage::getNext(&scanHandle)) != NULL) {
		int col = 0;
		table->field[col++]->store(usage->m_allocFile, strlen(usage->m_allocFile), cs);
		table->field[col++]->store(usage->m_allocLine, true);
		table->field[col++]->store(usage->m_name, strlen(usage->m_name), cs);
		table->field[col++]->store(usage->m_instanceCnt, true);
		table->field[col++]->store(usage->m_lockCnt[0], true);
		table->field[col++]->store(usage->m_lockCnt[1], true);
		table->field[col++]->store(usage->m_lockCnt[2], true);
		table->field[col++]->store(usage->m_lockCnt[3], true);
		table->field[col++]->store(usage->m_lockCnt[4], true);
		table->field[col++]->store(usage->m_lockCnt[5], true);
		table->field[col++]->store(usage->m_lockCnt[6], true);
		table->field[col++]->store(usage->m_spinCnt, true);
		table->field[col++]->store(usage->m_waitCnt, true);
		table->field[col++]->store(usage->m_waitTime, true);
		table->field[col++]->store(usage->m_failCnt, true);
		if (schema_table_store_record(thd, table))
			break;
	}
	IntentionLockUsage::endScan(&scanHandle);
	DBUG_RETURN(0);
}

/** NTSE_RWLOCK_STATS表定义 */
ST_FIELD_INFO rwlockStatsFieldInfo[] = {
	{"FILE",		256, MYSQL_TYPE_STRING,	0, 0, "File", SKIP_OPEN_TABLE},
	{"LINE",		4, MYSQL_TYPE_LONG,		0, 0, "Line", SKIP_OPEN_TABLE},
	{"NAME",		100, MYSQL_TYPE_STRING,	0, 0, "Name", SKIP_OPEN_TABLE},
	{"INSTANCES",	4, MYSQL_TYPE_LONG,		0, 0, "Number of Instances", SKIP_OPEN_TABLE},
	{"READ_LOCKS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Read Lock Operations", SKIP_OPEN_TABLE},
	{"WRITE_LOCKS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Write Lock Operations", SKIP_OPEN_TABLE},
	{"SPINS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Spins", SKIP_OPEN_TABLE},
	{"WAITS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Waits", SKIP_OPEN_TABLE},
	{"WAIT_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in Milisecons of Waits", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_RWLOCK_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISRWLockStats(void *p) {
	DBUG_ENTER("ha_tnt::initISRWLockStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = rwlockStatsFieldInfo;
	schema->fill_table = ha_tnt::fillISRWLockStats;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_RWLOCK_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISRWLockStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISRWLockStats");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_RWLOCK_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISRWLockStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISRWLockStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	RwluScanHandle scanHandle;
	const RWLockUsage *usage;
	RWLockUsage::beginScan(&scanHandle);
	while ((usage = RWLockUsage::getNext(&scanHandle)) != NULL) {
		int col = 0;
		table->field[col++]->store(usage->m_allocFile, strlen(usage->m_allocFile), cs);
		table->field[col++]->store(usage->m_allocLine, true);
		table->field[col++]->store(usage->m_name, strlen(usage->m_name), cs);
		table->field[col++]->store(usage->m_instanceCnt, true);
		table->field[col++]->store(usage->m_rlockCnt, true);
		table->field[col++]->store(usage->m_wlockCnt, true);
		table->field[col++]->store(usage->m_spinCnt, true);
		table->field[col++]->store(usage->m_waitCnt, true);
		table->field[col++]->store(usage->m_waitTime, true);
		if (schema_table_store_record(thd, table))
			break;
	}
	RWLockUsage::endScan(&scanHandle);
	DBUG_RETURN(0);
}

/** NTSE_BUF_DISTRIBUTION表定义 */
ST_FIELD_INFO bufDistrFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"TYPE",		10, MYSQL_TYPE_STRING,	0, 0, "Type of Data", SKIP_OPEN_TABLE},
	{"INDEX",		Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Name of Index if Type is Index", SKIP_OPEN_TABLE},
	{"BUF_PAGES",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Buffer Pages Used by This Type of Data", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_BUF_DISTRIBUTION表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISBufDistr(void *p) {
	DBUG_ENTER("ha_tnt::initISBufDistr");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = bufDistrFieldInfo;
	schema->fill_table = ha_tnt::fillISBufDistr;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_BUF_DISTRIBUTION表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISBufDistr(void *p) {
	DBUG_ENTER("ha_tnt::deinitISBufDistr");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_BUF_DISTRIBUTION表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISBufDistr(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISBufDistr");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISBufDistr", conn);

	Array<BufUsage *> *usageArr = ntse_db->getBufferDistr(session);
	for (size_t i = 0; i < usageArr->getSize(); i++) {
		BufUsage *usage = (*usageArr)[i];
		int col = 0;
		table->field[col++]->store(usage->m_tblId, true);
		table->field[col++]->store(usage->m_tblSchema, strlen(usage->m_tblSchema), cs);
		table->field[col++]->store(usage->m_tblName, strlen(usage->m_tblName), cs);
		table->field[col++]->store(BufUsage::getTypeStr(usage->m_type), strlen(BufUsage::getTypeStr(usage->m_type)), cs);
		table->field[col++]->store(usage->m_idxName, strlen(usage->m_idxName), cs);
		table->field[col++]->store(usage->m_numPages, true);
		if (schema_table_store_record(thd, table))
			break;
	}
	for (size_t i = 0; i < usageArr->getSize(); i++)
		delete (*usageArr)[i];
	delete usageArr;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

#define NTSE_CONNS_NAME_WIDTH	255
#define NTSE_CONNS_INFO_WIDTH	255

/** NTSE_CONNECTIONS表定义 */
ST_FIELD_INFO connsFieldInfo[] = {
	{"ID",			4, MYSQL_TYPE_LONG,		0, 0, "ID of Connection", SKIP_OPEN_TABLE},
	{"TYPE",		10, MYSQL_TYPE_STRING,	0, 0, "Type of Connection(MySQL/Internal)", SKIP_OPEN_TABLE},
	{"THD",			4, MYSQL_TYPE_LONG,		0, 0, "ID of MySQL Connection of Background Thread", SKIP_OPEN_TABLE},
	{"NAME",		NTSE_CONNS_NAME_WIDTH, MYSQL_TYPE_STRING,	0, 0, "Name of Connection", SKIP_OPEN_TABLE},
	{"DURATION",	4, MYSQL_TYPE_LONG,		0, 0, "Duration (in seconds) of Current Status", SKIP_OPEN_TABLE},
	{"INFO",		NTSE_CONNS_INFO_WIDTH, MYSQL_TYPE_STRING,	0, 0, "Info of Current Operation", SKIP_OPEN_TABLE},
	{"OPER",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Operations", SKIP_OPEN_TABLE},
	{"LOG_READ",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Logical Reads", SKIP_OPEN_TABLE},
	{"PHY_READ",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Physical Reads", SKIP_OPEN_TABLE},
	{"LOG_WRITE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Logical Writes", SKIP_OPEN_TABLE},
	{"PHY_WRITE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Physical Writes", SKIP_OPEN_TABLE},
	{"ROW_READ",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Reads", SKIP_OPEN_TABLE},
	{"ROW_INSERT",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Inserts", SKIP_OPEN_TABLE},
	{"ROW_UPDATE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Updates", SKIP_OPEN_TABLE},
	{"ROW_DELETE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Deletes", SKIP_OPEN_TABLE},
	{"TBL_SCAN",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Table Scans", SKIP_OPEN_TABLE},
	{"TBL_SCAN_ROW",8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Rows Return by Table Scan", SKIP_OPEN_TABLE},
	{"IDX_SCAN",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Index Scans", SKIP_OPEN_TABLE},
	{"IDX_SCAN_ROW",8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Rows Return by Index Scan", SKIP_OPEN_TABLE},
	{"POS_SCAN",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Positional Scans", SKIP_OPEN_TABLE},
	{"POS_SCAN_ROW",8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Rows Return by Positional Scan", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_CONNECTIONS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISConns(void *p) {
	DBUG_ENTER("ha_tnt::initISConns");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = connsFieldInfo;
	schema->fill_table = ha_tnt::fillISConns;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_CONNECTIONS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISConns(void *p) {
	DBUG_ENTER("ha_tnt::deinitISConns");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_CONNECTIONS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISConns(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISConns");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database * ntse_db = tnt_db->getNtseDb();

	ConnScanHandle *scanHandle = ntse_db->getSessionManager()->scanConnections();
	const Connection *conn;
	while ((conn = ntse_db->getSessionManager()->getNext(scanHandle)) != NULL) {
		int col = 0;
		table->field[col++]->store(conn->getId(), true);	// ID
		const char *typeStr;
		if (conn->isInternal())
			typeStr = "Internal";
		else
			typeStr = "MySQL";
		table->field[col++]->store(typeStr, strlen(typeStr), cs);	//TYPE
		table->field[col++]->store(conn->getThdID(), true);	// THD
		const char *name = conn->getName();
		if (name) {		// NAME
			table->field[col++]->store(name, min(strlen(name), NTSE_CONNS_NAME_WIDTH), cs);
		} else {
			table->field[col++]->store("", strlen(""), cs);
		}
		table->field[col++]->store(conn->getDuration(), true);	// DURATION
		char *info = ((Connection *)conn)->getStatus();
		if (info)
			table->field[col++]->store(info, min(strlen(info), NTSE_CONNS_INFO_WIDTH), cs);
		else
			table->field[col++]->store("", strlen(""), cs);
		delete []info;
		OpStat *status = conn->getLocalStatus();
		table->field[col++]->store(status->m_statArr[OPS_OPER], true);	// STMT
		table->field[col++]->store(status->m_statArr[OPS_LOG_READ], true);
		table->field[col++]->store(status->m_statArr[OPS_PHY_READ], true);
		table->field[col++]->store(status->m_statArr[OPS_LOG_WRITE], true);
		table->field[col++]->store(status->m_statArr[OPS_PHY_WRITE], true);
		table->field[col++]->store(status->m_statArr[OPS_ROW_READ], true);
		table->field[col++]->store(status->m_statArr[OPS_ROW_INSERT], true);
		table->field[col++]->store(status->m_statArr[OPS_ROW_UPDATE], true);
		table->field[col++]->store(status->m_statArr[OPS_ROW_DELETE], true);
		table->field[col++]->store(status->m_statArr[OPS_TBL_SCAN], true);
		table->field[col++]->store(status->m_statArr[OPS_TBL_SCAN_ROWS], true);
		table->field[col++]->store(status->m_statArr[OPS_IDX_SCAN], true);
		table->field[col++]->store(status->m_statArr[OPS_IDX_SCAN_ROWS], true);
		table->field[col++]->store(status->m_statArr[OPS_POS_SCAN], true);
		table->field[col++]->store(status->m_statArr[OPS_POS_SCAN_ROWS], true);
		if (schema_table_store_record(thd, table))
			break;
	}
	ntse_db->getSessionManager()->endScan(scanHandle);
	DBUG_RETURN(0);
}

/** NTSE_TABLE_MMS_STATS表定义 */
ST_FIELD_INFO tblMmsFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"ISLOB",		4, MYSQL_TYPE_LONG,	0, 0, "Is MMS for Small Lob?", SKIP_OPEN_TABLE},
	{"RECORD",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records", SKIP_OPEN_TABLE},
	{"PAGE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Pages", SKIP_OPEN_TABLE},
	{"DIRTY_RECORD",8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Dirty Records", SKIP_OPEN_TABLE},
	{"QUERY",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Queries", SKIP_OPEN_TABLE},
	{"QUERY_HIT",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Queries witch Matched", SKIP_OPEN_TABLE},
	{"INSERT",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records Inserted", SKIP_OPEN_TABLE},
	{"INSERT_FAIL",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records Insertion Fail", SKIP_OPEN_TABLE},
	{"UPDATE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records Updated", SKIP_OPEN_TABLE},
	{"UPDATE_MERGE",8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Merged Updates", SKIP_OPEN_TABLE},
	{"DELETE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records Deleted", SKIP_OPEN_TABLE},
	{"RECORD_REPLACE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records witch Were Replaced", SKIP_OPEN_TABLE},
	{"PAGE_REPLACE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Pages witch Were Replaced", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_MMS_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISMms(void *p) {
	DBUG_ENTER("ha_tnt::initISMms");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = tblMmsFieldInfo;
	schema->fill_table = ha_tnt::fillISMms;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_MMS_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISMms(void *p) {
	DBUG_ENTER("ha_tnt::deinitISMms");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_MMS_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISMms(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISMms");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;
	Database *ntse_db = tnt_db->getNtseDb();
	TNTTransaction trx;

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISMms", conn);

	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		int col = 0;
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		if (!tableInfo->getNtseTable()->getTableDef(true, session)->m_useMms) {
			tableInfo->unlockMeta(session, IL_S);
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		table->field[col++]->store(tableIds[i], true);
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
		table->field[col++]->store(0, true);
		MmsTableStatus mmsStat = tableInfo->getNtseTable()->getMmsTable()->getStatus();
		table->field[col++]->store(mmsStat.m_records, true);
		table->field[col++]->store(mmsStat.m_dirtyRecords, true);
		table->field[col++]->store(mmsStat.m_recordPages, true);
		table->field[col++]->store(mmsStat.m_recordQueries, true);
		table->field[col++]->store(mmsStat.m_recordQueryHits, true);
		table->field[col++]->store(mmsStat.m_recordInserts, true);
		table->field[col++]->store(mmsStat.m_replaceFailsWhenPut, true);
		table->field[col++]->store(mmsStat.m_recordUpdates, true);
		table->field[col++]->store(mmsStat.m_updateMerges, true);
		table->field[col++]->store(mmsStat.m_recordDeletes, true);
		table->field[col++]->store(mmsStat.m_recordVictims, true);
		table->field[col++]->store(mmsStat.m_pageVictims, true);

		bool fail = schema_table_store_record(thd, table) != 0;
		
		if (tableDef->hasLob()) {
			col = 0;
			table->field[col++]->store(tableIds[i], true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(1, true);
			MmsTableStatus mmsStat = tableInfo->getNtseTable()->getLobStorage()->getSLMmsTable()->getStatus();
			table->field[col++]->store(mmsStat.m_records, true);
			table->field[col++]->store(mmsStat.m_dirtyRecords, true);
			table->field[col++]->store(mmsStat.m_recordPages, true);
			table->field[col++]->store(mmsStat.m_recordQueries, true);
			table->field[col++]->store(mmsStat.m_recordQueryHits, true);
			table->field[col++]->store(mmsStat.m_recordInserts, true);
			table->field[col++]->store(mmsStat.m_replaceFailsWhenPut, true);
			table->field[col++]->store(mmsStat.m_recordUpdates, true);
			table->field[col++]->store(mmsStat.m_updateMerges, true);
			table->field[col++]->store(mmsStat.m_recordDeletes, true);
			table->field[col++]->store(mmsStat.m_recordVictims, true);
			table->field[col++]->store(mmsStat.m_pageVictims, true);

			if (schema_table_store_record(thd, table))
				fail = true;
		}

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);

		if (fail)
			break;
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_MMS_RPCLS_STATS表定义 */
ST_FIELD_INFO mmsRPClsFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"ISLOB",		4, MYSQL_TYPE_LONG,	0, 0, "Is MMS for Small Lob?", SKIP_OPEN_TABLE},
	{"MIN_SIZE",	4, MYSQL_TYPE_LONG,		0, 0, "Minimal Size of Records Managed by This Class", SKIP_OPEN_TABLE},
	{"MAX_SIZE",	4, MYSQL_TYPE_LONG,		0, 0, "Maximal Size of Records Managed by This Class", SKIP_OPEN_TABLE},
	{"PAGE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Pages", SKIP_OPEN_TABLE},
	{"NONFULL_PAGE",8, MYSQL_TYPE_LONGLONG,0, 0, "Number of Non-Full Pages", SKIP_OPEN_TABLE},
	{"INSERT",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records Inserted", SKIP_OPEN_TABLE},
	{"UPDATE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records Updated", SKIP_OPEN_TABLE},
	{"DELETE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records Deleted", SKIP_OPEN_TABLE},
	{"RECORD_REPLACE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Records witch Were Replaced", SKIP_OPEN_TABLE},
	{"PAGE_REPLACE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Pages witch Were Replaced", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_MMS_RPCLS_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISMmsRPCls(void *p) {
	DBUG_ENTER("ha_tnt::initISMmsRPCls");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = mmsRPClsFieldInfo;
	schema->fill_table = ha_tnt::fillISMmsRPCls;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_MMS_RPCLS_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISMmsRPCls(void *p) {
	DBUG_ENTER("ha_tnt::deinitISMmsRPCls");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_MMS_RPCLS_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISMmsRPCls(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISMmsRPCls");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;
	Database *ntse_db = tnt_db->getNtseDb();
	TNTTransaction trx;

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISMmsRPCls", conn);

	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		if (!tableInfo->getNtseTable()->getTableDef(true, session)->m_useMms) {
			tableInfo->unlockMeta(session, IL_S);
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int numRPCls;
		MmsRPClass **clsArr = tableInfo->getNtseTable()->getMmsTable()->getMmsRPClass(&numRPCls);
		for (int cls = 0; cls < numRPCls; cls++) {
			if (!clsArr[cls])
				continue;
			int col = 0;
			table->field[col++]->store(tableIds[i], true);
			TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(0, true);
			MmsRPClassStatus status = clsArr[cls]->getStatus();
			table->field[col++]->store(clsArr[cls]->getMinRecSize(), true);
			table->field[col++]->store(clsArr[cls]->getMaxRecSize(), true);
			table->field[col++]->store(status.m_recordPages, true);
			table->field[col++]->store(status.m_freePages, true);
			table->field[col++]->store(status.m_recordInserts, true);
			table->field[col++]->store(status.m_recordUpdates, true);
			table->field[col++]->store(status.m_recordDeletes, true);
			table->field[col++]->store(status.m_recordVictims, true);
			table->field[col++]->store(status.m_pageVictims, true);
			if (schema_table_store_record(thd, table))
				break;
		}
		if (tableInfo->getNtseTable()->getTableDef(true, session)->hasLob()) {
			clsArr = tableInfo->getNtseTable()->getLobStorage()->getSLMmsTable()->getMmsRPClass(&numRPCls);
			for (int cls = 0; cls < numRPCls; cls++) {
				if (!clsArr[cls])
					continue;
				int col = 0;
				table->field[col++]->store(tableIds[i], true);
				TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
				table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
				table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
				table->field[col++]->store(1, true);
				MmsRPClassStatus status = clsArr[cls]->getStatus();
				table->field[col++]->store(clsArr[cls]->getMinRecSize(), true);
				table->field[col++]->store(clsArr[cls]->getMaxRecSize(), true);
				table->field[col++]->store(status.m_recordPages, true);
				table->field[col++]->store(status.m_freePages, true);
				table->field[col++]->store(status.m_recordInserts, true);
				table->field[col++]->store(status.m_recordUpdates, true);
				table->field[col++]->store(status.m_recordDeletes, true);
				table->field[col++]->store(status.m_recordVictims, true);
				table->field[col++]->store(status.m_pageVictims, true);
				if (schema_table_store_record(thd, table))
					break;
			}
		}
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_TABLE_STATS表定义 */
ST_FIELD_INFO tableFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", OPEN_FULL_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", OPEN_FULL_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", OPEN_FULL_TABLE},
	{"HEAP_SIZE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Size of Heap", OPEN_FULL_TABLE},
	{"INDEX_SIZE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Size of Index", OPEN_FULL_TABLE},
	{"SLOB_SIZE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Size of Small LOB", OPEN_FULL_TABLE},
	{"BLOB_DIR_SIZE",8, MYSQL_TYPE_LONGLONG,0, 0, "Size of Directory of Big LOB", OPEN_FULL_TABLE},
	{"BLOB_DAT_SIZE",8, MYSQL_TYPE_LONGLONG,0, 0, "Size of Data of Big LOB", OPEN_FULL_TABLE},
	{"TBL_IS_LOCKS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of IS Locks of Table", SKIP_OPEN_TABLE},
	{"TBL_IX_LOCKS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of IX Locks of Table", SKIP_OPEN_TABLE},
	{"TBL_S_LOCKS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of S Locks of Table", SKIP_OPEN_TABLE},
	{"TBL_X_LOCKS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of X Locks of Table", SKIP_OPEN_TABLE},
	{"TBL_SIX_LOCKS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of SIX Locks of Table", SKIP_OPEN_TABLE},
	{"TBL_LOCK_SPINS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Spins in Acquiring Locks of Table", SKIP_OPEN_TABLE},
	{"TBL_LOCK_WAITS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Waits in Acquiring Locks of Table", SKIP_OPEN_TABLE},
	{"TBL_LOCK_WAIT_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time of Waits in Miliseconds in Acquiring Locks of Table", SKIP_OPEN_TABLE},
	{"RLOCK_NONE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Read Without Locking", SKIP_OPEN_TABLE},
	{"RLOCK_READ",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Read Locks of Row", SKIP_OPEN_TABLE},
	{"RLOCK_WRITE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Write Locks of Row", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_TABLE_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISTable(void *p) {
	DBUG_ENTER("ha_tnt::initISTable");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = tableFieldInfo;
	schema->fill_table = ha_tnt::fillISTable;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_TABLE_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISTable(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTable");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_TABLE_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISTable(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTable");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISMmsRPCls", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int col = 0;
		table->field[col++]->store(tableIds[i], true);
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
		HeapStatus heapStatus = tableInfo->getNtseTable()->getHeap()->getStatus();
		table->field[col++]->store(heapStatus.m_dataLength, true);
		table->field[col++]->store(tableInfo->getNtseTable()->getIndice()->getDataLength(), true);
		if (tableDef->hasLob()) {
			LobStatus lobStatus = tableInfo->getNtseTable()->getLobStorage()->getStatus();
			table->field[col++]->store(lobStatus.m_slobStatus.m_dataLength, true);
			table->field[col++]->store(lobStatus.m_blobStatus.m_idxLength, true);
			table->field[col++]->store(lobStatus.m_blobStatus.m_datLength, true);
		} else {
			table->field[col++]->store(0, true);
			table->field[col++]->store(0, true);
			table->field[col++]->store(0, true);
		}
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_lockCnt[IL_IS], true);
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_lockCnt[IL_IX], true);
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_lockCnt[IL_S], true);
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_lockCnt[IL_X], true);
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_lockCnt[IL_SIX], true);
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_spinCnt, true);
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_waitCnt, true);
		table->field[col++]->store(tableInfo->getNtseTable()->getLockUsage()->m_waitTime, true);
		table->field[col++]->store(tableInfo->getNtseTable()->getNumRowLocks(None), true);
		table->field[col++]->store(tableInfo->getNtseTable()->getNumRowLocks(Shared), true);
		table->field[col++]->store(tableInfo->getNtseTable()->getNumRowLocks(Exclusived), true);

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);

		if (schema_table_store_record(thd, table))
			break;
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_HEAP_STATS表定义 */
ST_FIELD_INFO heapFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"HEAP_VERSION",  Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,  0, 0, "Type of Heap Version", SKIP_OPEN_TABLE},
	{"RR_SUBREC",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Reads Returning Part of Record", SKIP_OPEN_TABLE},
	{"RR_REC",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Reads Returning Whole Record", SKIP_OPEN_TABLE},
	{"RU_SUBREC",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Updates Updating Part of Record", SKIP_OPEN_TABLE},
	{"RU_REC",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Dpdates Updating Whole Record", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_HEAP_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISHeap(void *p) {
	DBUG_ENTER("ha_tnt::initISHeap");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = heapFieldInfo;
	schema->fill_table = ha_tnt::fillISHeap;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_HEAP_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISHeap(void *p) {
	DBUG_ENTER("ha_tnt::deinitISHeap");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_HEAP_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISHeap(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISHeap");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;
	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISMmsRPCls", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int col = 0;
		table->field[col++]->store(tableIds[i], true);
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
		HeapVersion heapVersion = DrsHeap::getVersionFromTableDef(tableDef);
		const char * heapVersionStr = DrsHeap::getVersionStr(heapVersion);
		table->field[col++]->store(heapVersionStr, strlen(heapVersionStr), cs);
		HeapStatus heapStatus = tableInfo->getNtseTable()->getHeap()->getStatus();
		table->field[col++]->store(heapStatus.m_rowsReadSubRec, true);
		table->field[col++]->store(heapStatus.m_rowsReadRecord, true);
		table->field[col++]->store(heapStatus.m_rowsUpdateSubRec, true);
		table->field[col++]->store(heapStatus.m_rowsUpdateRecord, true);

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);

		if (schema_table_store_record(thd, table))
			break;
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_HEAP_STATS_EX表定义 */
ST_FIELD_INFO heapExFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"ROW",			8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Rows", SKIP_OPEN_TABLE},
	{"CMPRS_ROW",	8, MYSQL_TYPE_LONGLONG, 0, 0, "Number of Compress Rows", SKIP_OPEN_TABLE},
	{"LINK_ROW",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Links", SKIP_OPEN_TABLE},
	{"PCT_USED",	8, MYSQL_TYPE_DOUBLE,	0, 0, "Percentage of Used Space in Pages", SKIP_OPEN_TABLE},
	{"CMPRS_RATIO", 8, MYSQL_TYPE_DOUBLE,	0, 0, "Compress Ratio of compressed Records", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_HEAP_STATS_EX表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISHeapEx(void *p) {
	DBUG_ENTER("ha_tnt::initISHeapEx");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = heapExFieldInfo;
	schema->fill_table = ha_tnt::fillISHeapEx;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_HEAP_STATS_EX表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISHeapEx(void *p) {
	DBUG_ENTER("ha_tnt::deinitISHeapEx");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_HEAP_STATS_EX表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISHeapEx(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISHeapEx");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISHeapEx", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int col = 0;
		table->field[col++]->store(tableIds[i], true);
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
		tableInfo->getNtseTable()->getHeap()->updateExtendStatus(session, ntse_sample_max_pages);
		HeapStatusEx status = tableInfo->getNtseTable()->getHeap()->getStatusEx();
		table->field[col++]->store(status.m_numRows, true);
		table->field[col++]->store(status.m_numCmprsRows, true);
		table->field[col++]->store(status.m_numLinks, true);
		table->field[col++]->store(status.m_pctUsed * 100);
		table->field[col++]->store(status.m_cmprsRatio * 100);

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);

		if (schema_table_store_record(thd, table))
			break;
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_INDEX_STATS表定义 */
ST_FIELD_INFO indexFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"INDEX_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Index Name", SKIP_OPEN_TABLE},
	{"ALLOC_SIZE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Size of Space Used by This Index(Including Free Space)", SKIP_OPEN_TABLE},
	{"USED_SIZE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Size of Space Used by This Index", SKIP_OPEN_TABLE},
	{"BACK_SCAN",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Backward Index Scans", SKIP_OPEN_TABLE},
	{"BSCAN_ROW",	8, MYSQL_TYPE_LONGLONG,0, 0, "Number of Rows Returned by Backward Index Scans", SKIP_OPEN_TABLE},
	{"SPLIT",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Splits", SKIP_OPEN_TABLE},
	{"MERGE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Merges", SKIP_OPEN_TABLE},
	{"RL_RESTART",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Restarts After Acquiring Row Locks", SKIP_OPEN_TABLE},
	{"INSERT_PL_RESTART",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Restarts in Insert After Acquiring Page Locks", SKIP_OPEN_TABLE},
	{"DELETE_PL_RESTART",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Restarts in Delete After Acquiring Page Locks", SKIP_OPEN_TABLE},
	{"DEADLOCK",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Deadlocks", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_INDEX_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISIndex(void *p) {
	DBUG_ENTER("ha_tnt::initISIndex");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = indexFieldInfo;
	schema->fill_table = ha_tnt::fillISIndex;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_INDEX_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISIndex(void *p) {
	DBUG_ENTER("ha_tnt::deinitISIndex");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_INDEX_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISIndex(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISIndex");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISMmsRPCls", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int numIndice = tableInfo->getIndice()->getIndexNum();
		for (int idx = 0; idx < numIndice; idx++) {
			DrsIndex *index = tableInfo->getNtseTable()->getIndice()->getIndex(idx);
			IndexStatus status = index->getStatus();
			int col = 0;
			table->field[col++]->store(tableIds[i], true);
			TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(tableDef->m_indice[idx]->m_name, strlen(tableDef->m_indice[idx]->m_name), cs);
			table->field[col++]->store(status.m_dataLength, true);
			table->field[col++]->store(status.m_dataLength - status.m_freeLength, true);
			table->field[col++]->store(status.m_backwardScans, true);
			table->field[col++]->store(status.m_rowsBScanned, true);
			table->field[col++]->store(status.m_numSplit, true);
			table->field[col++]->store(status.m_numMerge, true);
			table->field[col++]->store(status.m_numRLRestarts, true);
			table->field[col++]->store(status.m_numILRestartsForI, true);
			table->field[col++]->store(status.m_numILRestartsForD, true);
			table->field[col++]->store(status.m_numDeadLockRestarts.get(), true);
			if (schema_table_store_record(thd, table))
				break;
		}
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_INDEX_STATS_EX表定义 */
ST_FIELD_INFO indexExFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"INDEX_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Index Name", SKIP_OPEN_TABLE},
	{"PCT_USED",	8, MYSQL_TYPE_DOUBLE,	0, 0, "Percentage of Used Space in Pages", SKIP_OPEN_TABLE},
	{"COMPRESS_RATIO",	8, MYSQL_TYPE_DOUBLE,	0, 0, "Size after Compress/Size before Compress", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_INDEX_STATS_EX表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISIndexEx(void *p) {
	DBUG_ENTER("ha_tnt::initISIndexEx");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = indexExFieldInfo;
	schema->fill_table = ha_tnt::fillISIndexEx;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_INDEX_STATS_EX表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISIndexEx(void *p) {
	DBUG_ENTER("ha_tnt::deinitISIndexEx");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_INDEX_STATS_EX表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISIndexEx(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISIndexEx");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISIndexEx", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int numIndice = tableInfo->getIndice()->getIndexNum();
		for (int idx = 0; idx < numIndice; idx++) {
			DrsIndex *index = tableInfo->getNtseTable()->getIndice()->getIndex(idx);
			index->updateExtendStatus(session, ntse_sample_max_pages);
			IndexStatusEx status = index->getStatusEx();
			int col = 0;
			table->field[col++]->store(tableIds[i], true);
			TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(tableDef->m_indice[idx]->m_name, strlen(tableDef->m_indice[idx]->m_name), cs);
			table->field[col++]->store(status.m_pctUsed * 100);
			table->field[col++]->store(status.m_compressRatio);
			if (schema_table_store_record(thd, table))
				break;
		}
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_LOB_STATS表定义 */
ST_FIELD_INFO lobFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"READ",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Reads", SKIP_OPEN_TABLE},
	{"INSERT",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Inserts", SKIP_OPEN_TABLE},
	{"UPDATE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Updates", SKIP_OPEN_TABLE},
	{"DELETE",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Deletes", SKIP_OPEN_TABLE},
	{"MOVE_UPDATE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Moving Updates of Big LOB", SKIP_OPEN_TABLE},
	{"USEFUL_COMPRESS",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Useful Compresses", SKIP_OPEN_TABLE},
	{"USELESS_COMPRESS",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Useless Compresses", SKIP_OPEN_TABLE},
	{"PRE_COMPRESS_SIZE",	8, MYSQL_TYPE_LONGLONG,0, 0, "Size before Compress", SKIP_OPEN_TABLE},
	{"POST_COMPRESS_SIZE",	8, MYSQL_TYPE_LONGLONG,0, 0, "Size after Compress", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_LOB_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISLob(void *p) {
	DBUG_ENTER("ha_tnt::initISLob");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = lobFieldInfo;
	schema->fill_table = ha_tnt::fillISLob;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_LOB_STATS表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISLob(void *p) {
	DBUG_ENTER("ha_tnt::deinitISLob");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_LOB_STATS表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISLob(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISLob");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISLob", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		if (!tableInfo->getNtseTable()->getTableDef(true, session)->hasLob()) {
			tableInfo->unlockMeta(session, IL_S);
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int col = 0;
		table->field[col++]->store(tableIds[i], true);
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
		LobStatus status = tableInfo->getNtseTable()->getLobStorage()->getStatus();
		DBObjStats *slobStats = status.m_slobStatus.m_dboStats;
		DBObjStats *blobStats = status.m_blobStatus.m_dboStats;
		table->field[col++]->store(slobStats->m_statArr[DBOBJ_ITEM_READ] + blobStats->m_statArr[DBOBJ_ITEM_READ], true);
		table->field[col++]->store(status.m_lobInsert, true);
		table->field[col++]->store(status.m_lobUpdate, true);
		table->field[col++]->store(status.m_lobDelete, true);
		table->field[col++]->store(status.m_blobStatus.m_moveUpdate, true);
		table->field[col++]->store(status.m_usefulCompress, true);
		table->field[col++]->store(status.m_uselessCompress, true);
		table->field[col++]->store(status.m_preCompressSize, true);
		table->field[col++]->store(status.m_postCompressSize, true);

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);

		if (schema_table_store_record(thd, table))
			break;
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_LOB_STATS_EX表定义 */
ST_FIELD_INFO lobExFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"SLOB_COUNT",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Small LOBs", SKIP_OPEN_TABLE},
	{"SLOB_LINK",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Row Links of Small LOB", SKIP_OPEN_TABLE},
	{"SLOB_PCT_USED",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Percentage of Used Space of Pages of Small LOB", SKIP_OPEN_TABLE},
	{"BLOB_COUNT",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Big LOBs", SKIP_OPEN_TABLE},
	{"BLOB_FREE_PAGE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Free Pages in Big LOB File", SKIP_OPEN_TABLE},
	{"BLOB_PCT_USED",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Percentage of Used Space of Non-Free Pages of Big LOB", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_LOB_STATS_EX表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISLobEx(void *p) {
	DBUG_ENTER("ha_tnt::initISLobEx");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = lobExFieldInfo;
	schema->fill_table = ha_tnt::fillISLobEx;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_LOB_STATS_EX表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISLobEx(void *p) {
	DBUG_ENTER("ha_tnt::deinitISLobEx");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_LOB_STATS_EX表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISLobEx(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISLobEx");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISLobEx", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		if (!tableInfo->getNtseTable()->getTableDef(true, session)->hasLob()) {
			tableInfo->unlockMeta(session, IL_S);
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int col = 0;
		table->field[col++]->store(tableIds[i], true);
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
		tableInfo->getNtseTable()->getLobStorage()->updateExtendStatus(session, ntse_sample_max_pages);
		LobStatusEx status = tableInfo->getNtseTable()->getLobStorage()->getStatusEx();
		table->field[col++]->store(status.m_slobStatus.m_numRows, true);
		table->field[col++]->store(status.m_slobStatus.m_numLinks, true);
		table->field[col++]->store(status.m_slobStatus.m_pctUsed * 100);
		table->field[col++]->store(status.m_blobStatus.m_numLobs, true);
		table->field[col++]->store(status.m_blobStatus.m_freePages, true);
		table->field[col++]->store(status.m_blobStatus.m_pctUsed * 100);

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);

		if (schema_table_store_record(thd, table))
			break;
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_COMMAND_RETURN表定义 */
ST_FIELD_INFO cmdReturnFieldInfo[] = {
	{"COMMAND",		255, MYSQL_TYPE_STRING,	0, 0, "Latest Command", SKIP_OPEN_TABLE},
	{"STATUS",		16, MYSQL_TYPE_STRING,	0, 0, "Status", SKIP_OPEN_TABLE},
	{"INFO",		255, MYSQL_TYPE_STRING,	0, 0, "Return Value/Error Message/Progress Information, etc", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_COMMAND_RETURN表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISCmdReturn(void *p) {
	DBUG_ENTER("ha_tnt::initISCmdReturn");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = cmdReturnFieldInfo;
	schema->fill_table = ha_tnt::fillISCmdReturn;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_COMMAND_RETURN表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISCmdReturn(void *p) {
	DBUG_ENTER("ha_tnt::deinitISCmdReturn");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_COMMAND_RETURN表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISCmdReturn(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISCmdReturn");

	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	THDInfo *thdInfo = checkTntTHDInfo(thd);
	assert(thdInfo != NULL);
	CmdInfo *cmdInfo = thdInfo->m_cmdInfo;
	table->field[0]->store(cmdInfo->getCommand(), strlen(cmdInfo->getCommand()), cs);
	table->field[1]->store(CmdInfo::getStatusStr(cmdInfo->getStatus()), strlen(CmdInfo::getStatusStr(cmdInfo->getStatus())), cs);
	table->field[2]->store(cmdInfo->getInfo(), strlen(cmdInfo->getInfo()), cs);
	schema_table_store_record(thd, table);
	
	DBUG_RETURN(0);
}

#define TBL_DEF_EX_CACHED_COLS_LEN		1024
#define TBL_DEF_EX_CREATE_ARGS_LEN		2000

ST_FIELD_INFO tblDefExFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", OPEN_FULL_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", OPEN_FULL_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", OPEN_FULL_TABLE},
	{"TABLE_PART_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Partition Name", OPEN_FULL_TABLE},
	{"TABLE_PART_SUB_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table SubPartition Name", OPEN_FULL_TABLE},
	{"SUPPORT_TRX",	4, MYSQL_TYPE_LONG,		0, 0, "Does This Table Support Transaction?", OPEN_FULL_TABLE},
	{"USE_MMS",		4, MYSQL_TYPE_LONG,		0, 0, "Does This Table Use MMS?", OPEN_FULL_TABLE},
	{"CACHE_UPDATE",4, MYSQL_TYPE_LONG,		0, 0, "Does This Table Cache Update?", OPEN_FULL_TABLE},
	{"UPDATE_CACHE_TIME",	4, MYSQL_TYPE_LONG,	0, 0, "Flush Interval in Seconds of Update Cache", OPEN_FULL_TABLE},
	{"COMPRESS_LOBS",	4, MYSQL_TYPE_LONG,	0, 0, "Does LOB of This Table Be Compressed?", OPEN_FULL_TABLE},
	{"PCT_FREE",	4, MYSQL_TYPE_LONG,		0, 0, "Percentage of Free Space in Variable Length Heap", OPEN_FULL_TABLE},
	{"INCR_SIZE",	4, MYSQL_TYPE_LONG,		0, 0, "Number of Pages When Heap/Index/Lob File Increase", OPEN_FULL_TABLE},
	{"INDEX_ONLY",	4, MYSQL_TYPE_LONG,		0, 0, "Does This Table is Index Only?", OPEN_FULL_TABLE},
	{"CREATE_ARGS",	TBL_DEF_EX_CREATE_ARGS_LEN, MYSQL_TYPE_STRING,	0, 0, "Create Arguments", OPEN_FULL_TABLE},
	{"ROW_COMPRESS", 4, MYSQL_TYPE_LONG, 0, 0, "Is This Table Defined As Compress Table?", OPEN_FULL_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_TBL_DEF_EX表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISTblDefEx(void *p) {
	DBUG_ENTER("ha_tnt::initISTblDefEx");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = tblDefExFieldInfo;
	schema->fill_table = ha_tnt::fillISTblDefEx;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_TBL_DEF_EX表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISTblDefEx(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTblDefEx");
	DBUG_RETURN(0);
}

string getCreateArgs(const TableDef *tableDef) {
	stringstream ss;
	ss << "usemms:" << tableDef->m_useMms << ";";
	if (tableDef->m_useMms) {
		ss << "cache_update:" << tableDef->m_cacheUpdate << ";";
		if (tableDef->m_cacheUpdate) {
			ss << "update_cache_time:" << tableDef->m_updateCacheTime << ";";
			ss << "cached_columns:";
			bool hasCachedColumns = false;
			for (u16 i = 0; i < tableDef->m_numCols; i++) {
				ColumnDef *c = tableDef->m_columns[i];
				if (c->m_cacheUpdate) {
					if (hasCachedColumns)
						ss << ",";
					ss << c->m_name;
					hasCachedColumns = true;
				}
			}
			ss << ";";
		}
	}

	ss << "compress_rows:" << tableDef->m_isCompressedTbl << ";";
	ss << "fix_len:" << (tableDef->m_fixLen ? "1" : "0") << ";";

	if (tableDef->m_rowCompressCfg) {
		ss << "compress_threshold:" << (uint)tableDef->m_rowCompressCfg->compressThreshold() << ";";
		ss << "dictionary_size:" << tableDef->m_rowCompressCfg->dicSize() << ";";
		ss << "dictionary_min_len:" << tableDef->m_rowCompressCfg->dicItemMinLen() << ";";
		ss << "dictionary_max_len:" << tableDef->m_rowCompressCfg->dicItemMaxLen() << ";";
	}
	if (tableDef->m_colGrps) {
		assert(tableDef->m_numColGrps > 0);
		ss << "column_groups:";
		for (u16 i = 0; i < tableDef->m_numColGrps; i++) {
			ss << ((i == 0) ? "(" : ", (");
			for (uint j = 0; j < tableDef->m_colGrps[i]->m_numCols; j++) {
				u16 colNo = tableDef->m_colGrps[i]->m_colNos[j];
				if (j != 0)
					ss << ", ";
				ss <<  tableDef->m_columns[colNo]->m_name;
			}
			ss << ")";
		}
		ss << ";";
	}

	if (tableDef->m_indexOnly)
		ss << "index_only:true" << ";";
	else {
		ss << "compress_lobs:" << tableDef->m_compressLobs << ";";
		ss << "heap_pct_free:" << tableDef->m_pctFree << ";";
	}
	ss << "incr_size:" << tableDef->m_incrSize << ";";
	if (tableDef->m_numIndice) {
		ss << "split_factors:";
		for (u16 i = 0; i < tableDef->m_numIndice; i++) {
			IndexDef *idx = tableDef->m_indice[i];
			ss << idx->m_name << ":" << (int)idx->m_splitFactor << ",";
		}
	}
	string str = ss.str();
	return str.substr(0, str.size() - 1);
}

/**
 * 填充NTSE_TBL_DEF_EX表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISTblDefEx(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTblDefEx");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISLobEx", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		int col = 0;
		table->field[col++]->store(tableIds[i], true);
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		char *parptr = NULL;
		char *subptr = NULL;
		if ((parptr = strstr(tableDef->m_name, "#P#")) != NULL) {
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name) - strlen(parptr), cs);
			if ((subptr = strstr(tableDef->m_name, "#SP#")) != NULL) {
				table->field[col++]->store(parptr + 3, strlen(parptr) - strlen(subptr) - 3, cs);
				table->field[col++]->store(subptr + 4, strlen(subptr) - 4, cs);
			} else {
				table->field[col++]->store(parptr + 3, strlen(parptr) - 3, cs);
				table->field[col++]->store("0", 1, cs);
			}			
		} else {
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store("0", 1, cs);
			table->field[col++]->store("0", 1, cs);
		}
		table->field[col++]->store(tableDef->m_tableStatus, true);
		table->field[col++]->store(tableDef->m_useMms, true);
		table->field[col++]->store(tableDef->m_cacheUpdate, true);
		table->field[col++]->store(tableDef->m_updateCacheTime, true);
		table->field[col++]->store(tableDef->m_compressLobs, true);
		table->field[col++]->store(tableDef->m_pctFree, true);
		table->field[col++]->store(tableDef->m_incrSize, true);
		table->field[col++]->store(tableDef->m_indexOnly, true);
		string args = getCreateArgs(tableDef);
		table->field[col++]->store(args.c_str(), strlen(args.c_str()), cs); 
		table->field[col++]->store(tableDef->m_isCompressedTbl, true);

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);

		if (schema_table_store_record(thd, table))
			break;
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

ST_FIELD_INFO colDefExFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", OPEN_FULL_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", OPEN_FULL_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", OPEN_FULL_TABLE},
	{"NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Column Name", OPEN_FULL_TABLE},
	{"CACHE_UPDATE",4, MYSQL_TYPE_LONG,		0, 0, "Does This Column Cache Update?", OPEN_FULL_TABLE},
	{"COL_GROUP_NO",4, MYSQL_TYPE_LONG,		0, 0, "Column Group No.", OPEN_FULL_TABLE},
	{"COL_GROUP_OFFSET",4, MYSQL_TYPE_LONG, 0, 0, "The Offset In Column Group", OPEN_FULL_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_COLUMN_DEF_EX表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISColDefEx(void *p) {
	DBUG_ENTER("ha_tnt::initISColDefEx");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = colDefExFieldInfo;
	schema->fill_table = ha_tnt::fillISColDefEx;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_COLUMN_DEF_EX表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISColDefEx(void *p) {
	DBUG_ENTER("ha_tnt::deinitISColDefEx");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_COLUMN_DEF_EX表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISColDefEx(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISColDefEx");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISLobEx", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		for (u16 j = 0; j < tableDef->m_numCols; j++) {
			int col = 0;
			table->field[col++]->store(tableIds[i], true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(tableDef->m_columns[j]->m_name, strlen(tableDef->m_columns[j]->m_name), cs);
			table->field[col++]->store(tableDef->m_columns[j]->m_cacheUpdate, true);
			table->field[col++]->store(tableDef->m_columns[j]->m_colGrpNo);
			table->field[col++]->store(tableDef->m_columns[j]->m_colGrpOffset);
			if (schema_table_store_record(thd, table))
				break;
		}

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

ST_FIELD_INFO idxDefExFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", OPEN_FULL_TABLE},
	{"TABLE_SCHEMA",Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", OPEN_FULL_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", OPEN_FULL_TABLE},
	{"NAME",		Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Index Name", OPEN_FULL_TABLE},
	{"SPLIT_FACTOR",4, MYSQL_TYPE_LONG,		0, 0, "Percentage of Data Left in Left Page After Index Splits", OPEN_FULL_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_INDEX_DEF_EX表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISIdxDefEx(void *p) {
	DBUG_ENTER("ha_tnt::initISIdxDefEx");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = idxDefExFieldInfo;
	schema->fill_table = ha_tnt::fillISIdxDefEx;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_INDEX_DEF_EX表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISIdxDefEx(void *p) {
	DBUG_ENTER("ha_tnt::deinitISIdxDefEx");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_INDEX_DEF_EX表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISIdxDefEx(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISIdxDefEx");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISLobEx", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		for (u16 j = 0; j < tableDef->m_numIndice; j++) {
			int col = 0;
			table->field[col++]->store(tableIds[i], true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(tableDef->m_indice[j]->m_name, strlen(tableDef->m_indice[j]->m_name), cs);
			table->field[col++]->store(tableDef->m_indice[j]->m_splitFactor, false);
			if (schema_table_store_record(thd, table))
				break;
		}

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_MMS_RIDHASH_CONFLICTS表定义 */
ST_FIELD_INFO mmsRidHashConflictsFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,	0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"TABLE_SCHEMA", Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema", OPEN_FULL_TABLE},
	{"TABLE_NAME", Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", OPEN_FULL_TABLE},
	{"PARTITION", 4, MYSQL_TYPE_LONG,	0, 0, "RID Hash Partition", SKIP_OPEN_TABLE},
	{"MAX_CONFLICT_LENGTH", 8, MYSQL_TYPE_LONGLONG,	0, 0, "Max Length of Conflict Lists", SKIP_OPEN_TABLE},
	{"AVG_CONFLICT_LENGTH", 8, MYSQL_TYPE_DOUBLE,	0, 0, "Avarage Length of Conflict Lists", SKIP_OPEN_TABLE},
	{0, 0, MYSQL_TYPE_STRING, 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISMmsRidHashConflicts(void *p) {
	DBUG_ENTER("ha_tnt::initISMmsRidHashConflicts");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = mmsRidHashConflictsFieldInfo;
	schema->fill_table = ha_tnt::fillISMmsRidHashConflicts;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISMmsRidHashConflicts(void *p) {
	DBUG_ENTER("ha_tnt::deinitISMmsRidHashConflicts");
	DBUG_RETURN(0);
}

int ha_tnt::fillISMmsRidHashConflicts(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISMmsRidConflict");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISMmsRidHashConflicts", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			tnt_db->closeTable(session, tableInfo);
			continue;
		}
		if (!tableInfo->getNtseTable()->getTableDef(true, session)->m_useMms) {
			tableInfo->unlockMeta(session, IL_S);
			tnt_db->closeTable(session, tableInfo);
			continue;
		}

		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		uint numParts = tableInfo->getNtseTable()->getMmsTable()->getPartitionNumber();
		for (uint part = 0; part < numParts; part++) {
			int col = 0;
			table->field[col++]->store(tableIds[i], true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(part, true);
			double avg = 0;
			size_t max = 0;
			tableInfo->getNtseTable()->getMmsTable()->getRidHashConflictStatus(part, &avg, &max);
			table->field[col++]->store(max, true);
			table->field[col++]->store(avg);

			if (schema_table_store_record(thd, table))
				break;
		}
		
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_DBOBJ_STATS表定义 */
ST_FIELD_INFO dbObjFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"SCHEMA_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema Name", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"TYPE", 		10, MYSQL_TYPE_STRING, 0, 0, "Object Type", SKIP_OPEN_TABLE},
	{"INDEX_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING, 0, 0, "Index Name If Type Is Index", SKIP_OPEN_TABLE},
	{"LOG_READ",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Logical Reads", SKIP_OPEN_TABLE},
	{"PHY_READ",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Physical Reads", SKIP_OPEN_TABLE},
	{"LOG_WRITE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Logical Writes", SKIP_OPEN_TABLE},
	{"PHY_WRITE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Physical Writes", SKIP_OPEN_TABLE},
	{"ITEM_READ",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Item Reads", SKIP_OPEN_TABLE},
	{"ITEM_INSERT",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Item Inserts", SKIP_OPEN_TABLE},
	{"ITEM_UPDATE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Item Updates", SKIP_OPEN_TABLE},
	{"ITEM_DELETE",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Item Deletes", SKIP_OPEN_TABLE},
	{"SCAN",		8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Scans", SKIP_OPEN_TABLE},
	{"SCAN_ITEM",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Number of Items Return by Scans", SKIP_OPEN_TABLE},
	{0, 0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISDBObjStats(void *p) {
	DBUG_ENTER("ha_tnt::initISDBObjStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = dbObjFieldInfo;
	schema->fill_table = ha_tnt::fillISDBObjStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISDBObjStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISDBObjStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISDBObjStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISDBObjStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISDBObjStats", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			tnt_db->closeTable(session, tableInfo);
			continue;
		}

		/** 获得每个表中的数据库对象进行统计 */
		Array<DBObjStats*> *dbObjsArr = tableInfo->getNtseTable()->getDBObjStats();
		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef(true, session);
		for (uint j = 0; j < dbObjsArr->getSize(); j++) {
			DBObjStats* dbObjStats = dbObjsArr->operator[](j);
			int col = 0;
			table->field[col++]->store(tableIds[i], true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);
			table->field[col++]->store(BufUsage::getTypeStr(dbObjStats->m_type), strlen(BufUsage::getTypeStr(dbObjStats->m_type)), cs);
			table->field[col++]->store(dbObjStats->m_idxName, strlen(dbObjStats->m_idxName), cs);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_LOG_READ], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_PHY_READ], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_LOG_WRITE], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_PHY_WRITE], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_ITEM_READ], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_ITEM_INSERT], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_ITEM_UPDATE], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_ITEM_DELETE], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_SCAN], true);
			table->field[col++]->store(dbObjStats->m_statArr[DBOBJ_SCAN_ITEM], true);
			if (schema_table_store_record(thd, table))
				break;
		}
		/** end */

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** NTSE_COMPRESS_STATS表定义 */
ST_FIELD_INFO compressFieldInfo[] = {
	{"TABLE_ID",	4, MYSQL_TYPE_LONG,		0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"SCHEMA_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema Name", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"COMPRESS_ROWS",     6,  MYSQL_TYPE_STRING, 0, 0, "Is Table defined As Compressed Table", OPEN_FULL_TABLE},
	{"DICT_CREATED",      5,  MYSQL_TYPE_STRING, 0, 0, "Has Compressed Dictionary Been Created", OPEN_FULL_TABLE}, 
	{"DICT_MAX_SIZE",	  5,  MYSQL_TYPE_STRING,  0, 0, "Global Dictionary Size Configure", OPEN_FULL_TABLE},
	{"DICT_ACTUAL_SIZE",  4,  MYSQL_TYPE_LONG,   0, 0, "Global Dictionary Actual Size", OPEN_FULL_TABLE},
	{"DICT_MEM_USAGE",    4,  MYSQL_TYPE_LONG,   0, 0, "Global Dictionary Memory Usage", OPEN_FULL_TABLE},
	{"DICT_MIN_LEN",	  6,  MYSQL_TYPE_STRING,  0, 0, "Dictionary Item Min Size", OPEN_FULL_TABLE},
	{"DICT_MAX_LEN",	  6,  MYSQL_TYPE_STRING,  0, 0, "Dictionary Item Max Size", OPEN_FULL_TABLE},
	{"DICT_AVG_LEN",	  8,  MYSQL_TYPE_DOUBLE, 0, 0, "Dictionary Item Average Size", OPEN_FULL_TABLE},
	{"COMPRESS_THRESHOLD", 6, MYSQL_TYPE_STRING,  0, 0, "Compress Threshold(%)", OPEN_FULL_TABLE},
	{0,				       0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISCompressStats(void *p) {
	DBUG_ENTER("ha_tnt::initISCompressStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = compressFieldInfo;
	schema->fill_table = ha_tnt::fillISCompressStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISCompressStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISCompressStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISCompressStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISDBObjStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISDBObjStats", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);

	const u8 dicCfgNum = 4;
	char tmp[dicCfgNum][10];
	const char *dicCfgArr[dicCfgNum];//字典配置的字符串形式

	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = NULL;
		try {
			tableInfo = tnt_db->openTable(session, path.c_str());
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}

		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef();
		bool hasDict = tableInfo->getNtseTable()->hasCompressDict();
		int col = 0;
		const char *isCprsTblStr = getBoolStr(tableDef->m_isCompressedTbl);
		const char *hasDictStr = getBoolStr(hasDict);

		if (!tableDef->m_rowCompressCfg) {
			for (u8 j = 0; j < dicCfgNum; j++)
				dicCfgArr[j] = "Null";
		} else {
			System::snprintf_mine(tmp[0], sizeof(tmp[0]), "%d\0", tableDef->m_rowCompressCfg->dicSize());
			dicCfgArr[0] = tmp[0];
			System::snprintf_mine(tmp[1], sizeof(tmp[1]), "%d\0", tableDef->m_rowCompressCfg->dicItemMinLen());
			dicCfgArr[1] = tmp[1];
			System::snprintf_mine(tmp[2], sizeof(tmp[2]), "%d\0", tableDef->m_rowCompressCfg->dicItemMaxLen());
			dicCfgArr[2] = tmp[2];
			System::snprintf_mine(tmp[3], sizeof(tmp[3]), "%d\0", tableDef->m_rowCompressCfg->compressThreshold());
			dicCfgArr[3] = tmp[3];
		}

		table->field[col++]->store(tableDef->m_id, true);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);		
		table->field[col++]->store(isCprsTblStr, strlen(isCprsTblStr), cs);		
		table->field[col++]->store(hasDictStr, strlen(hasDictStr), cs);		
		table->field[col++]->store(dicCfgArr[0], strlen(dicCfgArr[0]), cs);
		table->field[col++]->store(hasDict ? tableInfo->getNtseTable()->getCompressDict()->size() : 0, true);
		table->field[col++]->store(hasDict ? tableInfo->getNtseTable()->getCompressDict()->getMemUsage() : 0, true);
		table->field[col++]->store(dicCfgArr[1], strlen(dicCfgArr[1]), cs);
		table->field[col++]->store(dicCfgArr[2], strlen(dicCfgArr[2]), cs);
		table->field[col++]->store(hasDict ? tableInfo->getNtseTable()->getCompressDict()->getDictItemAvgLen() : 0);
		table->field[col++]->store(dicCfgArr[3], strlen(dicCfgArr[3]), cs);

		if (schema_table_store_record(thd, table)) {
			tableInfo->unlockMeta(session, IL_S);
			tnt_db->closeTable(session, tableInfo);
			break;
		}

		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}//for
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** TNT_MHEAP_STATS表定义 */
ST_FIELD_INFO mheapFieldInfo[] = {
	{"TABLE_ID",	4,  MYSQL_TYPE_LONG,     0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"SCHEMA_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema Name", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"TOTAL",       4,  MYSQL_TYPE_LONG, 0, 0, "Total Page Count in mheap", OPEN_FULL_TABLE},
	{"FREENODE0_SIZE",  4,  MYSQL_TYPE_LONG, 0, 0, "Total Page Count in first freenode of mheap", OPEN_FULL_TABLE}, 
	{"FREENODE1_SIZE",	4,  MYSQL_TYPE_LONG, 0, 0, "Total Page Count in second freenode of mheap", OPEN_FULL_TABLE},
	{"FREENODE2_SIZE",  4,  MYSQL_TYPE_LONG, 0, 0, "Total Page Count in third freenode of mheap", OPEN_FULL_TABLE},
	{"FREENODE3_SIZE",  4,  MYSQL_TYPE_LONG, 0, 0, "Total Page Count in forth freenode of mheap", OPEN_FULL_TABLE},
	{"AVG_SEARCH_SIZE",	4,  MYSQL_TYPE_LONG, 0, 0, "The Average Search Count", OPEN_FULL_TABLE},
	{"MAX_SEARCH_SIZE",	4,  MYSQL_TYPE_LONG, 0, 0, "The Max Search Count", OPEN_FULL_TABLE},
	{"INSERT",			8,  MYSQL_TYPE_LONGLONG, 0, 0, "The Insert Count", OPEN_FULL_TABLE},
	{"UPDATE_FIRST",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "The Update First Count",  OPEN_FULL_TABLE},
	{"UPDATE_SECORND",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "The Update Second Count", OPEN_FULL_TABLE},
	{"DELETE_FIRST",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "The Delete First Count",  OPEN_FULL_TABLE},
	{"DELETE_SECOND",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "The Delete Second Count", OPEN_FULL_TABLE},
	{0,				0,  MYSQL_TYPE_STRING,	 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISTNTMHeapStats(void *p) {
	DBUG_ENTER("ha_tnt::initISTNTMHeapStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = mheapFieldInfo;
	schema->fill_table = ha_tnt::fillISTNTMHeapStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISTNTMHeapStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTNTMHeapStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISTNTMHeapStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTNTMHeapStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISTNTMHeapStats", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);

	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}

		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef();
		MHeapStat stat = tableInfo->getMRecords()->getMHeapStat();
		int col = 0;
		table->field[col++]->store(tableDef->m_id, true);
		table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
		table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);		
		table->field[col++]->store(stat.m_total, true);		
		table->field[col++]->store(stat.m_freeNode0Size, true);		
		table->field[col++]->store(stat.m_freeNode1Size, true);
		table->field[col++]->store(stat.m_freeNode2Size, true);		
		table->field[col++]->store(stat.m_freeNode3Size, true);
		table->field[col++]->store(stat.m_avgSearchSize, true);		
		table->field[col++]->store(stat.m_maxSearchSize, true);
		table->field[col++]->store(stat.m_insert, true);
		table->field[col++]->store(stat.m_update_first, true);
		table->field[col++]->store(stat.m_update_second, true);
		table->field[col++]->store(stat.m_delete_first, true);
		table->field[col++]->store(stat.m_delete_second, true);
		if (schema_table_store_record(thd, table)) {
			tableInfo->unlockMeta(session, IL_S);
			tnt_db->closeTable(session, tableInfo);
			break;
		}
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}//for
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** TNT_HASHINDEX_STATS表定义 */
ST_FIELD_INFO hashIndexFieldInfo[] = {
	{"TABLE_ID",	4,  MYSQL_TYPE_LONG,     0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"SCHEMA_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema Name", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"HASHINDEX_NO",4,  MYSQL_TYPE_LONG,	 0, 0, "HashIndex Map Number", SKIP_OPEN_TABLE},
	{"TOTAL",       8,  MYSQL_TYPE_LONGLONG, 0, 0, "HashIndex Map Total Count", OPEN_FULL_TABLE},
	{"TRXID_CNT",   8,  MYSQL_TYPE_LONGLONG, 0, 0, "HashIndex Map TrxId Count", OPEN_FULL_TABLE},
	{"READ_CNT",    8,  MYSQL_TYPE_LONGLONG, 0, 0, "HashIndex Map Read Count",  OPEN_FULL_TABLE}, 
	{"INSERT_CNT",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "HashIndex Map Insert Count", OPEN_FULL_TABLE},
	{"UPDATE_CNT",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "HashIndex Map Update Count", OPEN_FULL_TABLE},
	{"REMOVE_CNT",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "HashIndex Map Remove Count", OPEN_FULL_TABLE},
	{"DEFRAG_CNT",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "HashIndex Map Defrag Count", OPEN_FULL_TABLE},
	{0,				0,  MYSQL_TYPE_STRING,	 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISTNTHashIndexStats(void *p) {
	DBUG_ENTER("ha_tnt::initISTNTHashIndexStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = hashIndexFieldInfo;
	schema->fill_table = ha_tnt::fillISTNTHashIndexStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISTNTHashIndexStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTNTHashIndexStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISTNTHashIndexStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTNTHashIndexStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISTNTHashIndexStats", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);

	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}

		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef();
		Array<HashIndexStat> stats;
		tableInfo->getMRecords()->getHashIndex()->getHashIndexStats(&stats);
		u16 arraySize = stats.getSize();
		for (u16 j = 0; j < arraySize; j++) {
			int col = 0;
			HashIndexStat stat = stats[j];
			table->field[col++]->store(tableDef->m_id, true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);		
			table->field[col++]->store(j, true);		
			table->field[col++]->store(stat.m_count, true);
			table->field[col++]->store(stat.m_trxIdCnt, true);
			table->field[col++]->store(stat.m_readCnt, true);
			table->field[col++]->store(stat.m_insertCnt, true);
			table->field[col++]->store(stat.m_updateCnt, true);
			table->field[col++]->store(stat.m_removeCnt, true);
			table->field[col++]->store(stat.m_defragCnt, true);
			if (schema_table_store_record(thd, table)) {
				tableInfo->unlockMeta(session, IL_S);
				tnt_db->closeTable(session, tableInfo);
				break;
			}
		}
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}//for
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}

/** TNT_TNT_TRXSYS_STATS表定义 */
ST_FIELD_INFO tntTrxSysFieldInfo[] = {
	{"COMMIT_NORMAL",      8,  MYSQL_TYPE_LONGLONG, 0, 0, "Commint TNTTransaction Normal Count", SKIP_OPEN_TABLE},
	{"COMMIT_INNER",       8,  MYSQL_TYPE_LONGLONG, 0, 0, "Commint TNTTransaction Inner Count", SKIP_OPEN_TABLE},
	{"ROLLBACK_NORMAL",    8,  MYSQL_TYPE_LONGLONG, 0, 0, "RollBack Normal TNTTransaction Count", SKIP_OPEN_TABLE}, 
	{"ROLLBACK_TIMEOUT",   8,  MYSQL_TYPE_LONGLONG, 0, 0, "RollBack Timeout TNTTransaction Count", SKIP_OPEN_TABLE},
	{"ROLLBACK_DEADLOCK",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "RollBack DeadLock TNTTransaction Count", SKIP_OPEN_TABLE},
	{"ROLLBACK_DUPLICATE_KEY",     8,  MYSQL_TYPE_LONGLONG, 0, 0, "RollBack Duplicate Key TNTTransaction Count", SKIP_OPEN_TABLE}, 
	{"ROLLBACK_ROW_TOO_LONG",      8,  MYSQL_TYPE_LONGLONG, 0, 0, "RollBack Row Too Long TNTTransaction Count", SKIP_OPEN_TABLE},
	{"ROLLBACK_ABORT",             8,  MYSQL_TYPE_LONGLONG, 0, 0, "RollBack Abort TNTTransaction Count", SKIP_OPEN_TABLE},
	{"ROLLBACK_NO_MEMORY",         8,  MYSQL_TYPE_LONGLONG, 0, 0, "RollBack No Memory Count", SKIP_OPEN_TABLE},
	{"PARTIAL_ROLLBACK_NORMAL",    8,  MYSQL_TYPE_LONGLONG, 0, 0, "Partial RollBack Normal TNTTransaction Count", SKIP_OPEN_TABLE},
	{"PARTIAL_ROLLBACK_TIMEOUT",   8,  MYSQL_TYPE_LONGLONG, 0, 0, "Partial RollBack Timeout TNTTransaction Count", SKIP_OPEN_TABLE},
	{"PARTIAL_ROLLBACK_DEADLOCK",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "Partial RollBack DeadLock TNTTransaction Count", SKIP_OPEN_TABLE},
	{"TRX_TIME_MAX",               4,  MYSQL_TYPE_LONG, 0, 0, "TNTTransaction Max Execute Time", SKIP_OPEN_TABLE},
	{"TRX_LOCKCNT_AVG",            4,  MYSQL_TYPE_LONG, 0, 0, "TNTTransaction Average Lock Count", SKIP_OPEN_TABLE},
	{"TRX_LOCKCNT_MAX",            4,  MYSQL_TYPE_LONG, 0, 0, "TNTTransaction Max Lock Count", SKIP_OPEN_TABLE},
	{"TRX_REDOCNT_AVG",            4,  MYSQL_TYPE_LONG, 0, 0, "TNTTransaction Average Redo Count", SKIP_OPEN_TABLE},
	{"TRX_REDOCNT_MAX",            4,  MYSQL_TYPE_LONG, 0, 0, "TNTTransaction Max Redo Count", SKIP_OPEN_TABLE},
	{0,				0,  MYSQL_TYPE_STRING,	 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISTNTTrxSysStats(void *p) {
	DBUG_ENTER("ha_tnt::initISTNTTrxSysStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = tntTrxSysFieldInfo;
	schema->fill_table = ha_tnt::fillISTNTTrxSysStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISTNTTrxSysStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTNTTrxSysStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISTNTTrxSysStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTNTTrxSysStats");
	TABLE *table = tables->table;

	TNTTrxSysStat stat = tnt_db->getTransSys()->getTNTTrxSysStat();
	int col = 0;	
	table->field[col++]->store(stat.m_commit_normal, true);
	table->field[col++]->store(stat.m_commit_inner, true);
	table->field[col++]->store(stat.m_rollback_normal, true);
	table->field[col++]->store(stat.m_rollback_timeout, true);
	table->field[col++]->store(stat.m_rollback_deadlock, true);
	table->field[col++]->store(stat.m_rollback_duplicate_key, true);
	table->field[col++]->store(stat.m_rollback_row_too_long, true);
	table->field[col++]->store(stat.m_rollback_abort, true);
	table->field[col++]->store(stat.m_rollback_out_of_mem, true);
	table->field[col++]->store(stat.m_partial_rollback_normal, true);
	table->field[col++]->store(stat.m_partial_rollback_timeout, true);
	table->field[col++]->store(stat.m_partial_rollback_deadlock, true);
	table->field[col++]->store(stat.m_maxTime, true);
	table->field[col++]->store(stat.m_avgLockCnt, true);
	table->field[col++]->store(stat.m_maxLockCnt, true);
	table->field[col++]->store(stat.m_avgRedoCnt, true);
	table->field[col++]->store(stat.m_maxRedoCnt, true);
	schema_table_store_record(thd, table);
	DBUG_RETURN(0);
}

/** TNT_TNT_TRANSACTION_STATS表定义 */
ST_FIELD_INFO tntTrxFieldInfo[] = {
	{"TRXID",               8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Id",           SKIP_OPEN_TABLE},
	{"TRX_STATUS",         50,  MYSQL_TYPE_STRING,   0, 0, "TNTTransaction Status",       SKIP_OPEN_TABLE},
	{"TRX_START_TIME",     50,  MYSQL_TYPE_DATETIME,   0, 0, "TNTTransaction Begin Time",   SKIP_OPEN_TABLE},
	{"TRX_EXCUTE_TIME",     4,  MYSQL_TYPE_LONG,     0, 0, "TNTTransaction Excute Time",  SKIP_OPEN_TABLE}, 
	{"TRX_WAIT_START_TIME", 50,  MYSQL_TYPE_DATETIME,   0, MY_I_S_MAYBE_NULL, "TNTTransaction Wait Start Time",   SKIP_OPEN_TABLE},
	{"TRX_LOCK_CNT",        4,  MYSQL_TYPE_LONG,     0, 0, "TNTTransaction Lock Count",   SKIP_OPEN_TABLE},
	{"TRX_IS_READ_ONLY",    2,  MYSQL_TYPE_SHORT,    0, 0, "TNTTransaction Is Read Only", SKIP_OPEN_TABLE},
	{"TRX_REAL_START_LSN",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Real Start Lsn",    SKIP_OPEN_TABLE},
	{"TRX_BEGIN_LSN",		8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Begin Lsn",    SKIP_OPEN_TABLE},
	{"TRX_LAST_LSN",		8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Last Lsn",     SKIP_OPEN_TABLE},
	{"TRX_VERSION_POOL_ID", 2,  MYSQL_TYPE_SHORT,    0, 0, "TNTTransaction Version Pool Id", SKIP_OPEN_TABLE},
	{"TRX_REDO_CNT",        4,  MYSQL_TYPE_LONG,     0, 0, "TNTTransaction Redo Count",      SKIP_OPEN_TABLE},
	{"TRX_IS_RECOVER_HANG", 2,  MYSQL_TYPE_SHORT,    0, 0, "TNTTransaction Is Hang Recover", SKIP_OPEN_TABLE},
	{"TRX_LOCK_MEMORY_BYTES", 8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Lock Memory Usage", SKIP_OPEN_TABLE},
	{"TRX_SQL_STATEMENT",1024,  MYSQL_TYPE_STRING,   0, MY_I_S_MAYBE_NULL, "TNTTransaction Sql Statement",   SKIP_OPEN_TABLE},
	{0,				        0,  MYSQL_TYPE_STRING,	 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISTNTTrxStats(void *p) {
	DBUG_ENTER("ha_tnt::initISTNTTrxStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = tntTrxFieldInfo;
	schema->fill_table = ha_tnt::fillISTNTTrxStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISTNTTrxStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTNTTrxStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISTNTTrxStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTNTTrxStats");
	u32 now = System::fastTime();
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	TNTTrxSys *trxSys = tnt_db->getTransSys();
	trxSys->lockTrxSysMutex(__FILE__, __LINE__);
	TNTTrxSys::Iterator<TNTTransaction> iter(trxSys->getActiveTrxs());
	while (iter.hasNext()) {
		TNTTransaction *trx = iter.next();
		TrxState trxState = trx->getTrxState();
		char *trxStatusDesc = TNTTransaction::getTrxStateDesc(trxState);
		
		// 格式化事务的开始时间
		MYSQL_TIME trxBeginTime;
		convertTimeToMysqlTime((time_t)trx->getBeginTime(), &trxBeginTime);

		// 格式化事务等待开始的时间
		MYSQL_TIME trxWaitStartTime;
		convertTimeToMysqlTime((time_t)trx->getWaitStartTime(), &trxWaitStartTime);
	
		// 截取sql语句的前1024个字节
		char trxStatementStr[1024 + 1];
		u32 trxStatementLength = 0;
		char *trxSql = NULL;
		THD *trxThd = (THD*)trx->getThd();
		if (trxThd) {
			LEX_STRING *lexString = thd_query_string((THD*) trxThd);
			trxSql = lexString->str;
			trxStatementLength = lexString->length;
			trxStatementLength = trxStatementLength > 1024? 1024: trxStatementLength;
			memcpy(trxStatementStr, trxSql, trxStatementLength);
		}
		int col = 0;	
		table->field[col++]->store(trx->getTrxId(), true);
		table->field[col++]->store(trxStatusDesc, strlen(trxStatusDesc), cs);
		table->field[col++]->store_time(&trxBeginTime, MYSQL_TIMESTAMP_DATETIME);
		table->field[col++]->store(now - trx->getBeginTime(), true);
		if (trx->getWaitLock()) {
			table->field[col]->set_notnull();
			table->field[col++]->store_time(&trxWaitStartTime, MYSQL_TIMESTAMP_DATETIME);
		} else {
			table->field[col++]->set_null();
		}
		table->field[col++]->store(trx->getHoldingLockCnt(), true);
		table->field[col++]->store(trx->isReadOnly(), true);
		table->field[col++]->store(trx->getTrxRealStartLsn(), true);
		table->field[col++]->store(trx->getTrxBeginLsn(), true);
		table->field[col++]->store(trx->getTrxLastLsn(), true);
		table->field[col++]->store(trx->getVersionPoolId(), true);
		table->field[col++]->store(trx->getRedoCnt(), true);
		table->field[col++]->store(trx->isHangByRecover(), true);
		table->field[col++]->store(trx->getMemoryContext()->getMemUsage(), true);
		if(trxThd && trxSql) {
			table->field[col]->set_notnull();
			table->field[col++]->store(trxStatementStr, trxStatementLength, cs);
		} else {
			table->field[col++]->set_null();
		}
		
		schema_table_store_record(thd, table);
	}
	trxSys->unlockTrxSysMutex();
	DBUG_RETURN(0);
}


/** TNT_TNT_INNER_TRANSACTION_STATS表定义 */
ST_FIELD_INFO tntInnerTrxFieldInfo[] = {
	{"TRXID",               8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Id",           SKIP_OPEN_TABLE},
	{"TRX STATUS",         50,  MYSQL_TYPE_STRING,   0, 0, "TNTTransaction Status",       SKIP_OPEN_TABLE},
	{"TRX_START_TIME",     50,  MYSQL_TYPE_DATETIME,   0, 0, "TNTTransaction Begin Time",   SKIP_OPEN_TABLE},
	{"TRX_EXCUTE_TIME",     4,  MYSQL_TYPE_LONG,     0, 0, "TNTTransaction Excute Time",  SKIP_OPEN_TABLE}, 
	{"TRX_LOCK_CNT",        4,  MYSQL_TYPE_LONG,     0, 0, "TNTTransaction Lock Count",   SKIP_OPEN_TABLE},
	{"TRX_IS_READ_ONLY",    2,  MYSQL_TYPE_SHORT,    0, 0, "TNTTransaction Is Read Only", SKIP_OPEN_TABLE},
	{"TRX_REAL_START_LSN",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Real Start Lsn",    SKIP_OPEN_TABLE},
	{"TRX_BEGIN_LSN",		8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Begin Lsn",    SKIP_OPEN_TABLE},
	{"TRX_LAST_LSN",		8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTTransaction Last Lsn",     SKIP_OPEN_TABLE},
	{"TRX_VERSION_POOL_ID", 2,  MYSQL_TYPE_SHORT,    0, 0, "TNTTransaction Version Pool Id", SKIP_OPEN_TABLE},
	{"TRX_REDO_CNT",        4,  MYSQL_TYPE_LONG,     0, 0, "TNTTransaction Redo Count",      SKIP_OPEN_TABLE},
	{"TRX_IS_RECOVER_HANG", 2,  MYSQL_TYPE_SHORT,    0, 0, "TNTTransaction Is Hang Recover", SKIP_OPEN_TABLE},
	{0,				        0,  MYSQL_TYPE_STRING,	 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISTNTInnerTrxStats(void *p) {
	DBUG_ENTER("ha_tnt::initISTNTInnerTrxStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = tntInnerTrxFieldInfo;
	schema->fill_table = ha_tnt::fillISTNTInnerTrxStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISTNTInnerTrxStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTNTInnerTrxStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISTNTInnerTrxStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTNTInnerTrxStats");
	u32 now = System::fastTime();
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	TNTTrxSys *trxSys = tnt_db->getTransSys();
	trxSys->lockTrxSysMutex(__FILE__, __LINE__);
	TNTTrxSys::Iterator<TNTTransaction> iter(trxSys->getActiveInnerTrxs());
	while (iter.hasNext()) {
		TNTTransaction *trx = iter.next();
		char *trxStatusDesc = TNTTransaction::getTrxStateDesc(trx->getTrxState());

		// 格式化事务的开始时间
		MYSQL_TIME trxBeginTime;
		convertTimeToMysqlTime((time_t)trx->getBeginTime(), &trxBeginTime);

		int col = 0;	
		table->field[col++]->store(trx->getTrxId(), true);
		table->field[col++]->store(trxStatusDesc, strlen(trxStatusDesc), cs);
		table->field[col++]->store_time(&trxBeginTime, MYSQL_TIMESTAMP_DATETIME);
		table->field[col++]->store(now - trx->getBeginTime(), true);
		table->field[col++]->store(trx->getHoldingLockCnt(), true);
		table->field[col++]->store(trx->isReadOnly(), true);
		table->field[col++]->store(trx->getTrxRealStartLsn(), true);
		table->field[col++]->store(trx->getTrxBeginLsn(), true);
		table->field[col++]->store(trx->getTrxLastLsn(), true);
		table->field[col++]->store(trx->getVersionPoolId(), true);
		table->field[col++]->store(trx->getRedoCnt(), true);
		table->field[col++]->store(trx->isHangByRecover(), true);
		schema_table_store_record(thd, table);
	}
	trxSys->unlockTrxSysMutex();
	DBUG_RETURN(0);
}


/** TNT_MEMORYINDEX_STATS表定义 */
ST_FIELD_INFO memoryIndexFieldInfo[] = {
	{"TABLE_ID",	4,  MYSQL_TYPE_LONG,     0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"SCHEMA_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema Name", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"INDEX_NAME",  Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Index Name", SKIP_OPEN_TABLE},
	{"INSERT_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Insert Count", OPEN_FULL_TABLE},
	{"DELETE_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Delete Count", OPEN_FULL_TABLE},
	{"SCAN_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Scan Count", OPEN_FULL_TABLE},
	{"SCANROWS_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Scan Row Count", OPEN_FULL_TABLE},
	{"BACKSCAN_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Backward Scan Count", OPEN_FULL_TABLE},
	{"BACKSCANROWS_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Backward Scan Row Count", OPEN_FULL_TABLE},
	{"SPLIT_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Split Count", OPEN_FULL_TABLE},
	{"MERGE_CNT",       8,  MYSQL_TYPE_LONGLONG, 0, 0, "MemoryIndex Merge Count", OPEN_FULL_TABLE},
	{"REDISTRIBUTE_CNT",    8,  MYSQL_TYPE_LONGLONG, 0, 0, "MemoryIndex Redistribute Count", OPEN_FULL_TABLE},
	{"RESTART_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Restart Count", OPEN_FULL_TABLE},
	{"LATCH_CONFLICT_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex Latch Conflict Count", OPEN_FULL_TABLE},
	{"REPAIR_UNDERFLOW_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex RepairUnderFlow Count", OPEN_FULL_TABLE},
	{"REPAIR_OVERFLOW_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "MemoryIndex RepairOverFlow Count", OPEN_FULL_TABLE},
	{"INCREASE_HEIGHT_CNT",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "MemoryIndex Increase Tree Height Count", OPEN_FULL_TABLE},
	{"DECREASE_HEIGHT_CNT",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "MemoryIndex Decrease Count", OPEN_FULL_TABLE},
	{"ALLOC_PAGE_CNT",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "MemoryIndex Alloc Page Count", OPEN_FULL_TABLE},
	{"FREE_PAGE_CNT",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "MemoryIndex Free Page Count", OPEN_FULL_TABLE},
	{0,				0,  MYSQL_TYPE_STRING,	 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISTNTMemoryIndexStats(void *p) {
 	DBUG_ENTER("ha_tnt::initISTNTMemoryIndexStats");
 	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
 	schema->fields_info = memoryIndexFieldInfo;
 	schema->fill_table = ha_tnt::fillISTNTMemoryIndexStats;
 	DBUG_RETURN(0);
}

int ha_tnt::deinitISTNTMemoryIndexStats(void *p) {
 	DBUG_ENTER("ha_tnt::deinitISTNTMemoryIndexStats");
 	DBUG_RETURN(0);
}

int ha_tnt::fillISTNTMemoryIndexStats(THD *thd, TABLE_LIST *tables, COND *cond) {
 	DBUG_ENTER("ha_tnt::fillISTNTMemoryIndexStats");
 	TABLE *table = tables->table;
 	charset_info_st *cs = system_charset_info;
 
 	Database *ntse_db = tnt_db->getNtseDb();
 
 	Connection *conn = checkTntTHDInfo(thd)->m_conn;
 	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISTNTMemoryIndexStats", conn);
 
 	TNTTransaction trx;
 	session->setTrans(&trx);
 
 	u16 numTables = ntse_db->getControlFile()->getNumTables();
 	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);
 
 	for (u16 i = 0; i < numTables; i++) {
 		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
 		if (path.empty())
 			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
 		try {
 			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
 		} catch (NtseException &) {
 			if (tableInfo)
 				tnt_db->closeTable(session, tableInfo);
 			continue;
 		}
 
 		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef();
		u16 idxNum = tableInfo->getNtseTable()->getIndice()->getIndexNum();
		for (u16 j = 0; j < idxNum; j++) {
			int col = 0;
			MIndexStatus stat = tableInfo->getIndice()->getMemIndice()->getIndex(j)->getStatus();
			table->field[col++]->store(tableDef->m_id, true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);		
			table->field[col++]->store(tableDef->getIndexDef(j)->m_name, strlen(tableDef->getIndexDef(j)->m_name), cs);		
			table->field[col++]->store(stat.m_numInsert, true);
			table->field[col++]->store(stat.m_numDelete, true);
			table->field[col++]->store(stat.m_numScans, true);
			table->field[col++]->store(stat.m_rowsScanned, true);
			table->field[col++]->store(stat.m_backwardScans, true);
			table->field[col++]->store(stat.m_rowsBScanned, true);
			table->field[col++]->store(stat.m_numSplit, true);		
			table->field[col++]->store(stat.m_numMerge, true);
			table->field[col++]->store(stat.m_numRedistribute, true);
			table->field[col++]->store(stat.m_numRestarts, true);
			table->field[col++]->store(stat.m_numLatchesConflicts, true);
			table->field[col++]->store(stat.m_numRepairUnderflow, true);
			table->field[col++]->store(stat.m_numRepairOverflow, true);
			table->field[col++]->store(stat.m_numIncreaseTreeHeight, true);
			table->field[col++]->store(stat.m_numDecreaseTreeHeight, true);		
			table->field[col++]->store((u64)stat.m_numAllocPage.get(), true);
			table->field[col++]->store((u64)stat.m_numFreePage.get(), true);
			if (schema_table_store_record(thd, table)) {
				tableInfo->unlockMeta(session, IL_S);
				tnt_db->closeTable(session, tableInfo);
				break;
			}
		}
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}//for
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
 	DBUG_RETURN(0);
}


/** TNT_INDEX_STATS表定义 */
ST_FIELD_INFO tntIndexFieldInfo[] = {
	{"TABLE_ID",	4,  MYSQL_TYPE_LONG,     0, 0, "Table ID", SKIP_OPEN_TABLE},
	{"SCHEMA_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Schema Name", SKIP_OPEN_TABLE},
	{"TABLE_NAME",	Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Table Name", SKIP_OPEN_TABLE},
	{"INDEX_NAME",  Limits::MAX_NAME_LEN + 1, MYSQL_TYPE_STRING,	0, 0, "Index Name", SKIP_OPEN_TABLE},
	{"SCAN_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "TNTIndex Scan Count", OPEN_FULL_TABLE},
	{"SCANROWS_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "TNTIndex Scan Row Count", OPEN_FULL_TABLE},
	{"BACKSCAN_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "TNTIndex Backward Scan Count", OPEN_FULL_TABLE},
	{"BACKSCANROWS_CNT",	8,  MYSQL_TYPE_LONGLONG,	 0, 0, "TNTIndex Backward Scan Row Count", OPEN_FULL_TABLE},
	{"DRS_RETURN_CNT",	8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTIndex DrsIndex Return Count", OPEN_FULL_TABLE},
	{"MIDX_RETURN_CNT",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTIndex MIndex Return Count", OPEN_FULL_TABLE},
	{"ROWLOCK_CONFLICT_CNT",  8,  MYSQL_TYPE_LONGLONG, 0, 0, "TNTIndex Alloc Page Count", OPEN_FULL_TABLE},
	{0,				0,  MYSQL_TYPE_STRING,	 0, 0, 0, SKIP_OPEN_TABLE}
};

int ha_tnt::initISTNTIndexStats(void *p) {
	DBUG_ENTER("ha_tnt::initISTNTIndexStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = tntIndexFieldInfo;
	schema->fill_table = ha_tnt::fillISTNTIndexStats;
	DBUG_RETURN(0);
}

int ha_tnt::deinitISTNTIndexStats(void *p) {
	DBUG_ENTER("ha_tnt::deinitISTNTIndexStats");
	DBUG_RETURN(0);
}

int ha_tnt::fillISTNTIndexStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISTNTIndexStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Database *ntse_db = tnt_db->getNtseDb();

	Connection *conn = checkTntTHDInfo(thd)->m_conn;
	Session *session = ntse_db->getSessionManager()->allocSession("ha_tnt::fillISTNTMemoryIndexStats", conn);

	TNTTransaction trx;
	session->setTrans(&trx);

	u16 numTables = ntse_db->getControlFile()->getNumTables();
	u16 *tableIds = new u16[numTables];
	numTables = ntse_db->getControlFile()->listAllTables(tableIds, numTables);

	for (u16 i = 0; i < numTables; i++) {
		string path = ntse_db->getControlFile()->getTablePath(tableIds[i]);
		if (path.empty())
			continue;
		TNTTable *tableInfo = tnt_db->pinTableIfOpened(session, path.c_str(), 100);
		if (!tableInfo)
			continue;
		try {
			tableInfo->lockMeta(session, IL_S, 0, __FILE__, __LINE__);
		} catch (NtseException &) {
			if (tableInfo)
				tnt_db->closeTable(session, tableInfo);
			continue;
		}

		TableDef *tableDef = tableInfo->getNtseTable()->getTableDef();
		u16 idxNum = tableInfo->getNtseTable()->getIndice()->getIndexNum();
		for (u8 j = 0; j < idxNum; j++) {
			int col = 0;
			TNTIndexStatus stat = tableInfo->getIndice()->getTntIndex(j)->getStatus();
			table->field[col++]->store(tableDef->m_id, true);
			table->field[col++]->store(tableDef->m_schemaName, strlen(tableDef->m_schemaName), cs);
			table->field[col++]->store(tableDef->m_name, strlen(tableDef->m_name), cs);		
			table->field[col++]->store(tableDef->getIndexDef(j)->m_name, strlen(tableDef->getIndexDef(j)->m_name), cs);		
			table->field[col++]->store(stat.m_numScans, true);
			table->field[col++]->store(stat.m_rowsScanned, true);
			table->field[col++]->store(stat.m_backwardScans, true);
			table->field[col++]->store(stat.m_rowsBScanned, true);
			table->field[col++]->store(stat.m_numDrsReturn, true);		
			table->field[col++]->store(stat.m_numMIdxReturn, true);
			table->field[col++]->store(stat.m_numRLRestarts, true);
			if (schema_table_store_record(thd, table)) {
				tableInfo->unlockMeta(session, IL_S);
				tnt_db->closeTable(session, tableInfo);
				break;
			}
		}
		tableInfo->unlockMeta(session, IL_S);
		tnt_db->closeTable(session, tableInfo);
	}//for
	delete tableIds;
	ntse_db->getSessionManager()->freeSession(session);
	DBUG_RETURN(0);
}


#ifdef NTSE_PROFILE
/** NTSE_PROFILE_SUMMARY表定义 */
ST_FIELD_INFO profileSummFieldInfo[] = {
	{"CALLER",		100, MYSQL_TYPE_STRING,	0, 0, "Caller Name", SKIP_OPEN_TABLE},
	{"FUNCNAME",	100, MYSQL_TYPE_STRING,	0, 0, "Function Name", SKIP_OPEN_TABLE},
	{"COUNT",		4, MYSQL_TYPE_LONG,		0, 0, "Called Times", SKIP_OPEN_TABLE},
	{"SUMMARY_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Running", SKIP_OPEN_TABLE},
	{"AVERAGE_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Average Running Cycles", SKIP_OPEN_TABLE},
	{"MAX_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of MAX Running Cycles", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
 * 初始化NTSE_PROFILE_SUMMARY表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISProfSumm(void *p) {
	DBUG_ENTER("ha_tnt::initISProfSumm");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = profileSummFieldInfo;
	schema->fill_table = ha_tnt::fillISProfSumm;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_PROFILE_SUMMARY表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISProfSumm(void *p) {
	DBUG_ENTER("ha_tnt::deinitISProfSumm");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_PROFILE_SUMMARY表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISProfSumm(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISProfSumm");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Array<ProfileInfoRecord *> *profileInfoArray = g_profiler.getGlobalProfileInfos();
	for (size_t i = 0; i < profileInfoArray->getSize(); i++) {
		ProfileInfoRecord *profInfo = profileInfoArray->operator[](i);
		table->field[0]->store(profInfo->getCaller(), strlen(profInfo->getCaller()), cs);
		table->field[1]->store(profInfo->getFuncName(), strlen(profInfo->getFuncName()), cs);
		table->field[2]->store(profInfo->getCount(), true);
		table->field[3]->store(profInfo->getSumT(), true);
		table->field[4]->store(profInfo->getAvgT(), true);
		table->field[5]->store(profInfo->getMaxT(), true);
		if (schema_table_store_record(thd, table))
			break;
	}

	for (size_t i = 0; i < profileInfoArray->getSize(); i++)
		delete profileInfoArray->operator[](i);
	delete profileInfoArray;

	DBUG_RETURN(0);
}


/** NTSE_BGTHREAD_PROFILES表定义 */
ST_FIELD_INFO bgThdProfileFieldInfo[] = {
	{"THD_ID",		4, MYSQL_TYPE_LONG,		0, 0, "ID of MySQL Thread", SKIP_OPEN_TABLE},
	{"CALLER",		100, MYSQL_TYPE_STRING,	0, 0, "Caller Function Name", SKIP_OPEN_TABLE},
	{"FUNCNAME",	100, MYSQL_TYPE_STRING,	0, 0, "Function Name", SKIP_OPEN_TABLE},
	{"COUNT",		4, MYSQL_TYPE_LONG,		0, 0, "Called Times", SKIP_OPEN_TABLE},
	{"SUMMARY_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Running", SKIP_OPEN_TABLE},
	{"AVERAGE_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Average Running Cycles", SKIP_OPEN_TABLE},
	{"MAX_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of MAX Running Cycles", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};


/**
 * 初始化NTSE_BGTHREAD_PROFILES表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISBGThdProfile(void *p) {
	DBUG_ENTER("ha_tnt::initISBGThdProfile");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = bgThdProfileFieldInfo;
	schema->fill_table = ha_tnt::fillISBGThdProfile;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_BGTHREAD_PROFILES表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISBGThdProfile(void *p) {
	DBUG_ENTER("ha_tnt::deinitISBGThdProfile");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_BGTHREAD_PROFILES表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISBGThdProfile(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISBGThdProfile");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(BG_THREAD);
	for (size_t i = 0; i < thdProfInfoArray->getSize(); i++) {
		ThdProfileInfoRecord *profInfo = thdProfInfoArray->operator[](i);
		table->field[0]->store(profInfo->getId(), true);
		table->field[1]->store(profInfo->getCaller(), strlen(profInfo->getCaller()), cs);
		table->field[2]->store(profInfo->getFuncName(), strlen(profInfo->getFuncName()), cs);
		table->field[3]->store(profInfo->getCount(), true);
		table->field[4]->store(profInfo->getSumT(), true);
		table->field[5]->store(profInfo->getAvgT(), true);
		table->field[6]->store(profInfo->getMaxT(), true);
		if (schema_table_store_record(thd, table))
			break;
	}

	for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
		delete thdProfInfoArray->operator[](i);
	delete thdProfInfoArray;

	DBUG_RETURN(0);
}

/** NTSE_CONN_PROFILES表定义 */
ST_FIELD_INFO connProfileFieldInfo[] = {
	{"CONN_ID",		4, MYSQL_TYPE_LONG,		0, 0, "ID of MySQL Connection", SKIP_OPEN_TABLE},
	{"CALLER",		100, MYSQL_TYPE_STRING,	0, 0, "Caller Function Name", SKIP_OPEN_TABLE},
	{"FUNCNAME",	100, MYSQL_TYPE_STRING,	0, 0, "Function Name", SKIP_OPEN_TABLE},
	{"COUNT",		4, MYSQL_TYPE_LONG,		0, 0, "Called Times", SKIP_OPEN_TABLE},
	{"SUMMARY_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Running", SKIP_OPEN_TABLE},
	{"AVERAGE_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Average Running Cycles", SKIP_OPEN_TABLE},
	{"MAX_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of MAX Running Cycles", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};


/**
 * 初始化NTSE_CONN_PROFILES表
 *
 * @param p ST_SCHEMA_TABLE实例
 */
int ha_tnt::initISConnThdProfile(void *p) {
	DBUG_ENTER("ha_tnt::initISConnThdProfile");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = connProfileFieldInfo;
	schema->fill_table = ha_tnt::fillISConnThdProfile;
	DBUG_RETURN(0);
}

/**
 * 清理NTSE_CONN_PROFILES表
 *
 * @param p ST_SCHEMA_TABLE实例，不用
 */
int ha_tnt::deinitISConnThdProfile(void *p) {
	DBUG_ENTER("ha_tnt::deinitISConnThdProfile");
	DBUG_RETURN(0);
}

/**
 * 填充NTSE_CONN_PROFILES表内容
 *
 * @param thd 不用
 * @param tables 不用
 * @param cond 不用
 */
int ha_tnt::fillISConnThdProfile(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_tnt::fillISConnThdProfile");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(CONN_THREAD);
	for (size_t i = 0; i < thdProfInfoArray->getSize(); i++) {
		ThdProfileInfoRecord *profInfo = thdProfInfoArray->operator[](i);
		table->field[0]->store(profInfo->getId(), true);
		table->field[1]->store(profInfo->getCaller(), strlen(profInfo->getCaller()), cs);
		table->field[2]->store(profInfo->getFuncName(), strlen(profInfo->getFuncName()), cs);
		table->field[3]->store(profInfo->getCount(), true);
		table->field[4]->store(profInfo->getSumT(), true);
		table->field[5]->store(profInfo->getAvgT(), true);
		table->field[6]->store(profInfo->getMaxT(), true);
		if (schema_table_store_record(thd, table))
			break;
	}

	for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
		delete thdProfInfoArray->operator[](i);
	delete thdProfInfoArray;

	DBUG_RETURN(0);
}

#ifdef NTSE_KEYVALUE_SERVER
/** NTSE_KEYVALUE_PROFILES表定义 */
ST_FIELD_INFO kvProfileSummFieldInfo[] = {
	{"FUNCNAME",	100, MYSQL_TYPE_STRING,	0, 0, "Function Name", SKIP_OPEN_TABLE},
	{"COUNT",		4, MYSQL_TYPE_LONG,		0, 0, "Called Times", SKIP_OPEN_TABLE},
	{"SUMMARY_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Running", SKIP_OPEN_TABLE},
	{"AVERAGE_TIME",8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of Average Running Cycles", SKIP_OPEN_TABLE},
	{"MAX_TIME",	8, MYSQL_TYPE_LONGLONG,	0, 0, "Time in KCC of MAX Running Cycles", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
* 初始化NTSE_KEYVALUE_PROFILES表
*
* @param p ST_SCHEMA_TABLE实例
*/
int ha_ntse::initKVProfSumm(void *p) {
	DBUG_ENTER("ha_ntse::initKVProfSumm");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = kvProfileSummFieldInfo;
	schema->fill_table = ha_ntse::fillKVProfSumm;
	DBUG_RETURN(0);
}

/**
* 清理NTSE_KEYVALUE_PROFILES表
*
* @param p ST_SCHEMA_TABLE实例，不用
*/
int ha_ntse::deinitKVProfSumm(void *p) {
	DBUG_ENTER("ha_ntse::deinitKVProfSumm");
	DBUG_RETURN(0);
}

/**
* 填充NTSE_KEYVALUE_PROFILES表内容
*
* @param thd 不用
* @param tables 不用
* @param cond 不用
*/
int ha_ntse::fillKVProfSumm(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_ntse::fillKVProfSumm");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	Array<ProfileInfoRecord *> *kvProfileInfoArray = g_profiler.getGlobalProfileInfos();
	for (size_t i = 0; i < kvProfileInfoArray->getSize(); i++) {
		ProfileInfoRecord *profInfo = (*kvProfileInfoArray)[i];
		string callerName(profInfo->getCaller());
		string funcName(profInfo->getFuncName());
		if (callerName=="ROOT" && funcName.find("KeyValueHandler::") != string::npos) {
			table->field[0]->store(profInfo->getFuncName(), strlen(profInfo->getFuncName()), cs);
			table->field[1]->store(profInfo->getCount(), true);
			table->field[2]->store(profInfo->getSumT(), true);
			table->field[3]->store(profInfo->getAvgT(), true);
			table->field[4]->store(profInfo->getMaxT(), true);
			if (schema_table_store_record(thd, table))
				break;
		}
	}

	for (size_t i = 0; i < kvProfileInfoArray->getSize(); i++)
		delete kvProfileInfoArray->operator[](i);
	delete kvProfileInfoArray;

	DBUG_RETURN(0);
}
#endif // NTSE_KEYVALUE_SERVER
#endif // NTSE_PROFILE

#ifdef NTSE_KEYVALUE_SERVER
/** NTSE_KEYVALUE_STATISTICS表定义 */
ST_FIELD_INFO kvStatsFieldInfo[] = {
	{"GET_HIT",		8, MYSQL_TYPE_LONG,	0, 0, "Get operation hit times", SKIP_OPEN_TABLE},
	{"GET_MISS",	8, MYSQL_TYPE_LONG,	0, 0, "Get operation miss times", SKIP_OPEN_TABLE},
	{"GET_FAIL",	8, MYSQL_TYPE_LONG,	0, 0, "Get operation fail times", SKIP_OPEN_TABLE},
	{"PUT_SUCCESS",	8, MYSQL_TYPE_LONG,	0, 0, "Put operation success times", SKIP_OPEN_TABLE},
	{"PUT_CONFLICT",8, MYSQL_TYPE_LONG,	0, 0, "Put operation conflict times", SKIP_OPEN_TABLE},
	{"PUT_FAIL",	8, MYSQL_TYPE_LONG,	0, 0, "Put operation fail times", SKIP_OPEN_TABLE},
	{"UPDATE_SUCCESS",8, MYSQL_TYPE_LONG,0, 0, "Update operation success times", SKIP_OPEN_TABLE},
	{"UPDATE_FAIL",	8, MYSQL_TYPE_LONG,	0, 0, "Update operation fail times", SKIP_OPEN_TABLE},
	{"UPDATE_BY_COND",	8, MYSQL_TYPE_LONG,	0, 0, "Update operation by condition times", SKIP_OPEN_TABLE},
	{"DELETE_FAIL",	8, MYSQL_TYPE_LONG,	0, 0, "Delete operation fail times", SKIP_OPEN_TABLE},
	{"DELETE_SUCCESS",8, MYSQL_TYPE_LONG,0, 0, "Delete operation success times", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
* 初始化NTSE_KEYVALUE_STATISTICS表
*
* @param p ST_SCHEMA_TABLE实例
*/
int ha_ntse::initKVStats(void *p) {
	DBUG_ENTER("ha_ntse::initKVStats");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = kvStatsFieldInfo;
	schema->fill_table = ha_ntse::fillKVStats;
	DBUG_RETURN(0);
}

/**
* 清理NTSE_KEYVALUE_STATISTICS表
*
* @param p ST_SCHEMA_TABLE实例，不用
*/
int ha_ntse::deinitKVStats(void *p) {
	DBUG_ENTER("ha_ntse::deinitKVStats");
	DBUG_RETURN(0);
}

/**
* 填充NTSE_KEYVALUE_STATISTICS表内容
*
* @param thd 不用
* @param tables 不用
* @param cond 不用
*/
int ha_ntse::fillKVStats(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_ntse::fillKVStats");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	table->field[0]->store(KeyValueServer::m_serverStats.getHits, true);
	table->field[1]->store(KeyValueServer::m_serverStats.getMisses, true);
	table->field[2]->store(KeyValueServer::m_serverStats.getFailed, true);
	table->field[3]->store(KeyValueServer::m_serverStats.putSuccess, true);
	table->field[4]->store(KeyValueServer::m_serverStats.putConflict, true);
	table->field[5]->store(KeyValueServer::m_serverStats.putFailed, true);
	table->field[6]->store(KeyValueServer::m_serverStats.updateSuccess, true);
	table->field[7]->store(KeyValueServer::m_serverStats.updateFailed, true);
	table->field[8]->store(KeyValueServer::m_serverStats.updateConfirmedByCond, true);
	table->field[9]->store(KeyValueServer::m_serverStats.deleteFailed, true);
	table->field[10]->store(KeyValueServer::m_serverStats.deleteSuccess, true);

	DBUG_RETURN(schema_table_store_record(thd, table));
}

/** NTSE_KEYVALUE_THREADINFO表定义 */
ST_FIELD_INFO kvThreadInfoFieldInfo[] = {
	{"CLIENT_IP",		50, MYSQL_TYPE_STRING,	0, 0, "The connected client ip", SKIP_OPEN_TABLE},
	{"PORT",			8,	MYSQL_TYPE_LONG,	0, 0, "The connected client port", SKIP_OPEN_TABLE},
	{"DB.TABLE",		50, MYSQL_TYPE_STRING,	0, 0, "The db and table to be operated", SKIP_OPEN_TABLE},
	{"API",				20, MYSQL_TYPE_STRING,	0, 0, "The api invoked by client", SKIP_OPEN_TABLE},
	{"DURATION_TIME/us",8,	MYSQL_TYPE_LONG,	0, 0, "The time spent by api", SKIP_OPEN_TABLE},
	{"STATUS",			20, MYSQL_TYPE_STRING,	0, 0, "The processing status", SKIP_OPEN_TABLE},
	{"INFORMATION",		1000, MYSQL_TYPE_STRING,	0, 0, "The api information", SKIP_OPEN_TABLE},
	{0,				0, MYSQL_TYPE_STRING,	0, 0, 0, SKIP_OPEN_TABLE}
};

/**
* 初始化NTSE_KEYVALUE_THREADINFO表
*
* @param p ST_SCHEMA_TABLE实例
*/
int ha_ntse::initKVThreadInfo(void *p) {
	DBUG_ENTER("ha_ntse::initKVThreadInfo");
	ST_SCHEMA_TABLE *schema = (ST_SCHEMA_TABLE*) p;
	schema->fields_info = kvThreadInfoFieldInfo;
	schema->fill_table = ha_ntse::fillKVThreadInfo;
	DBUG_RETURN(0);
}

/**
* 清理NTSE_KEYVALUE_THREADINFO表
*
* @param p ST_SCHEMA_TABLE实例，不用
*/
int ha_ntse::deinitKVThreadInfo(void *p) {
	DBUG_ENTER("ha_ntse::deinitKVThreadInfo");
	DBUG_RETURN(0);
}

/**
* 填充NTSE_KEYVALUE_THREADINFO表内容
*
* @param thd 不用
* @param tables 不用
* @param cond 不用
*/
int ha_ntse::fillKVThreadInfo(THD *thd, TABLE_LIST *tables, COND *cond) {
	DBUG_ENTER("ha_ntse::fillKVThreadInfo");
	TABLE *table = tables->table;
	charset_info_st *cs = system_charset_info;

	list<ThreadLocalInfo *> *threadInfos = &KeyValueServer::m_threadInfos;
	typedef list<ThreadLocalInfo *>::iterator it;

	LOCK(&KeyValueServer::m_threadInfoLock);
	for (it itor= threadInfos->begin(); itor !=threadInfos->end(); ++itor) {
		/*
		 *	在CREATE_CONTEXT接口调用时, 并没有客户端连接信息
		 */
		if ((*itor)->status == CREATE_CONTEXT) {
			table->field[0]->store("", strlen(""), cs);
			table->field[1]->store(0, true);
		} else {
			table->field[0]->store((*itor)->clientIP.c_str(), (*itor)->clientIP.size(), cs);
			table->field[1]->store((*itor)->port, true);
		}
		
		table->field[2]->store((*itor)->tablePath.c_str(), (*itor)->tablePath.size(), cs);
		table->field[3]->store(invokedMethodString[(*itor)->api], strlen(invokedMethodString[(*itor)->api]), cs);

		/*
		 *	不是在processor处理中，不用计算时间
		 */
		if ((*itor)->apiStartTime != -1) {
			table->field[4]->store(System::microTime() - (*itor)->apiStartTime, true);
		} else {
			table->field[4]->store(0, true);
		}
		table->field[5]->store(processingStatusString[(*itor)->status], strlen(processingStatusString[(*itor)->status]), cs);

		/**
		 *	只有当处理状态为PROCESSING，print解析的参数
		 */
		if ((*itor)->status >= PROCESSING && ((*itor)->status <= PROCESS_END)) {
			table->field[6]->store((*itor)->information.str().c_str(), (*itor)->information.str().size(), cs);
		} else {
			table->field[6]->store("", strlen(""), cs);
		}

		if (schema_table_store_record(thd, table))
			break;
	}
	UNLOCK(&KeyValueServer::m_threadInfoLock);

	DBUG_RETURN(0);
}
#endif

