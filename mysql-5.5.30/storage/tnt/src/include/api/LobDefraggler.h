/**
 * 大对象碎片整理
 *
 * @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#ifndef _NTSE_LOBDEFRAGGLER_H_
#define _NTSE_LOBDEFRAGGLER_H_
#include "misc/Global.h"
namespace ntse {
class NtseException;
class Database;
class Connection;
class LobStorage;

/** 
 * 在线大对象整理工具类
 */
class LobDefraggler {
public:
	LobDefraggler(Database *db, Connection *conn, const char *tableName);
	~LobDefraggler();
	void startDefrag() throw(NtseException);
private:
	Database *m_db;					/* 数据库。 */
	Connection *m_conn;				/* 数据库连接 */
	char *m_schemeTableStr;			/* 数据库表名，格式scheme.table_name。 */
};


}

#endif
