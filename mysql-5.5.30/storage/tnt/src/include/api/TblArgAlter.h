/**
 * ������޸Ĺ���
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
 */
#ifndef _NTSE_TABLEALTER_H_
#define _NTSE_TABLEALTER_H_
#include "util/Hash.h"
#include "misc/Parser.h"
#include "util/SmartPtr.h"

using namespace std;

namespace ntse {

class Database;
class Connection;
class Session;
class Table;
class TableDef;
/**
 * ��̬�ı����������
 */
enum TableArgAlterCmdType {
	MIN_TBL_ARG_CMD_TYPE_VALUE = 0,		/* ������޸���������ֵ */

	USEMMS,								/* ʹ��MMS�����ֵ����>0��������Ϊ��ϣ������������ʱ�᷵��0 */
	CACHE_UPDATE,						/* ʹ�û������ */
	UPDATE_CACHE_TIME,					/* ���û���������� */
	CACHED_COLUMNS,						/* ���û����� */

	COMPRESS_LOBS,						/* ���ô����ѹ�� */
	HEAP_PCT_FREE,						/* ����ҳ��Ԥ���ռ�ٷֱ� */
	SPLIT_FACTORS,						/* ���÷���ϵ�� */
	INCR_SIZE,							/* ����ҳ��������С */
	COMPRESS_ROWS,                      /* ��¼ѹ�� */
	FIX_LEN,                            /* �Ƿ�ʹ�ö����� */
	COLUMN_GROUPS,						/* �����鶨�� */
	COMPRESS_DICT_SIZE,                 /* ѹ��ȫ���ֵ��С */
	COMPRESS_DICT_MIN_LEN,              /* ѹ��ȫ���ֵ�����С���� */     
	COMPRESS_DICT_MAX_LEN,              /* ѹ��ȫ���ֵ�����󳤶� */
	COMPRESS_THRESHOLD,                 /* ��¼ѹ���ȷ�ֵ */
	SUPPORT_TRX,                        /* �����Ƿ�֧������ */

	MAX_TBL_ARG_CMD_TYPE_VALUE			/* ������޸���������ֵ */
};

/**
 * ������޸�����ӳ�丨����.
 */
class TableArgAlterCommandMapTool {
public:
	TableArgAlterCommandMapTool();
	Hash<const char *, TableArgAlterCmdType, Hasher<const char *>, Equaler<const char *, const char *> >* getCmdMap();

private:
	Hash<const char *, TableArgAlterCmdType, Hasher<const char *>, Equaler<const char *, const char *> > m_mp; /* Alter Table����ӳ��� */
	void initCommandMap();
};

/**
 * ������޸ĸ����ࡣ
 */
class TableArgAlterHelper {
public:
	static TableArgAlterCommandMapTool cmdMapTool; /* Alter Table����߶��� */

	static const char *CMD_USEMMS;					/* ʹ��MMS */
	static const char *CMD_CACHE_UPDATE;			/* ʹ��MMS���� */
	static const char *CMD_UPDATE_CACHE_TIME;		/* ���ø������� */
	static const char *CMD_CACHED_COLUMNS;			/* ���û����� */

	static const char *CMD_COMPRESS_LOBS;			/* ����LOBѹ�� */
	static const char *CMD_HEAP_PCT_FREE;			/* ���ÿ��аٷֱ� */
	static const char *CMD_SPLIT_FACTORS;			/* ���÷���ϵ�� */
	static const char *CMD_INCR_SIZE;				/* ������չҳ��С */
	static const char *CMD_COMPRESS_ROWS;           /* �����Ƿ���ѹ���� */
	static const char *CMD_FIX_LEN;                 /* �����Ƿ�ʹ�ö����� */
	static const char *CMD_SET_COLUMN_GROUPS;       /* ���������� */
	static const char *CMD_COMPRESS_DICT_SIZE;      /* ����ѹ��ȫ���ֵ��С */
	static const char *CMD_COMPRESS_DICT_MIN_LEN;   /* ����ѹ��ȫ���ֵ�����С���� */
	static const char *CMD_COMPRESS_DICT_MAX_LEN;   /* ����ѹ��ȫ���ֵ���󳤶� */
	static const char *CMD_COMPRESS_THRESHOLD;      /* ����ѹ���ȷ�ֵ */
	static const char *CMD_SUPPORT_TRX;				/* �����Ƿ�֧���������*/   

	static const char *ENABLE;						/* Enable ���� */
	static const char *DISABLE;						/* Disable ���� */
	static const int NUM_CMDS = 13;					/* �������Ŀ */
	static const char *ALLOC_SESSION_NAME;			/* ����Ļ����� */

	static const char *STR_TABLE;					/* �����ַ���"table" */
	static const char *STR_SET;						/* �����ַ���"set" */
	static const char *STR_EQUAL;					/* �����ַ���"set" */
	static const char *STR_DOT;						/* �����ַ���"." */
	static const char COL_COMMA_DELIMITER;          /* �м䶺�ŷָ���� ',' */

	static const char *MSG_EXCEPTION_MMS_NOT_ENABLED;					/* MMSδ������ʾ��Ϣ */
	static const char *MSG_EXCEPTION_CACHE_UPDATE_NOT_ENABLED;			/* CacheUpdateδ������ʾ��Ϣ */
	static const char *MSG_EXCEPTION_WRONG_COMMAND_TYPE;				/* ������޸�����������ʾ��Ϣ */
	static const char *MSG_EXCEPTION_WRONG_COMMAND_ARGUMENTS;			/* ��������������ʾ��Ϣ */
	static const char *MSG_EXCEPTION_TOO_MANY_MMS_COLUMNS;				/* ̫������Ŀ */	
	static const char *MSG_EXCEPTION_NON_EXISTED_COL_NAME;				/* �����ڵ�������ʾ��Ϣ */
	static const char *MSG_EXCEPTION_NON_EXISTED_INDEX_NAME;			/* �����ڵ���������ʾ��Ϣ */
	static const char *MSG_EXCEPTION_WRONG_CMD_FORMAT_SCHEME_TBL_CMD;	/* �����ʽscheme.table.cmd����ȷ��ʾ��Ϣ */
	static const char *MSG_EXCEPTION_INVALID_INDEX_SPLIT_FACTOR;		/* ����ϵ����Ч��ʾ��Ϣ */
	static const char *MSG_EXCEPTION_INVALID_HEAP_PCT_FREE;				/* HEAP_PCT_FREE������Ч��ʾ��Ϣ */	
	static const char *MSG_EXCEPTION_COLUMNGROUP_HAVE_DICTIONARY;       /* ���Ѿ���ȫ���ֵ䣬������������Ч */
	static const char *MSG_EXCEPTION_DICT_ARG_HAVE_DICTIONARY;          /* ���Ѿ���ȫ���ֵ�, �����ֵ������Ч */
	static const char *MSG_EXCEPTION_EMPTY_COL_GRP;                     /* �������鶨�� */
	static const char *MSG_EXCEPTION_INVALID_INCR_SIZE;                 /* INCR_SIZE������Ч��ʾ��Ϣ */

	TableArgAlterHelper(Database *db, Connection *conn, Parser *parser, int timeout, bool inLockTables = false);
	virtual ~TableArgAlterHelper();
	void alterTableArgument() throw(NtseException);
	static void toUpper(char *str, size_t len);
	static bool checkCachedColumns(Session *session, TableDef *tableDef, u16 *cols, u16 numCols, int *numEnabled, int *numToEnable);
protected:
	virtual void alterTableArgumentReal(Session *session, vector<string> *parFiles, const char *name, const char *value) throw(NtseException);
	Database	*m_db;		/* ���ݿ� */
	Connection	*m_conn;	/* ʹ�õ����� */
	Parser		*m_parser;	/* ���������������� */
	int 		m_timeout;	/* ��ȡ����ʱʱ�� */
	bool		m_inLockTables; /* ��Ӧ��mysql thd �Ƿ���lock tables��*/
};
}

#endif	
