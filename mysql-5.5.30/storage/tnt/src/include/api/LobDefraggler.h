/**
 * �������Ƭ����
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
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
 * ���ߴ������������
 */
class LobDefraggler {
public:
	LobDefraggler(Database *db, Connection *conn, const char *tableName);
	~LobDefraggler();
	void startDefrag() throw(NtseException);
private:
	Database *m_db;					/* ���ݿ⡣ */
	Connection *m_conn;				/* ���ݿ����� */
	char *m_schemeTableStr;			/* ���ݿ��������ʽscheme.table_name�� */
};


}

#endif
