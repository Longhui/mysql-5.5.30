/**
 * 表参数修改功能
 *
 * @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
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
 * 动态改变表命令类型
 */
enum TableArgAlterCmdType {
	MIN_TBL_ARG_CMD_TYPE_VALUE = 0,		/* 表参数修改类型下限值 */

	USEMMS,								/* 使用MMS，这个值必须>0，这是因为哈希表在搜索不到时会返回0 */
	CACHE_UPDATE,						/* 使用缓存更新 */
	UPDATE_CACHE_TIME,					/* 设置缓存更新周期 */
	CACHED_COLUMNS,						/* 设置缓存列 */

	COMPRESS_LOBS,						/* 启用大对象压缩 */
	HEAP_PCT_FREE,						/* 设置页面预留空间百分比 */
	SPLIT_FACTORS,						/* 设置分裂系数 */
	INCR_SIZE,							/* 设置页面增长大小 */
	COMPRESS_ROWS,                      /* 记录压缩 */
	FIX_LEN,                            /* 是否使用定长堆 */
	COLUMN_GROUPS,						/* 属性组定义 */
	COMPRESS_DICT_SIZE,                 /* 压缩全局字典大小 */
	COMPRESS_DICT_MIN_LEN,              /* 压缩全局字典项最小长度 */     
	COMPRESS_DICT_MAX_LEN,              /* 压缩全局字典项最大长度 */
	COMPRESS_THRESHOLD,                 /* 记录压缩比阀值 */
	SUPPORT_TRX,                        /* 设置是否支持事务 */

	MAX_TBL_ARG_CMD_TYPE_VALUE			/* 表参数修改类型上限值 */
};

/**
 * 表参数修改命令映射辅助类.
 */
class TableArgAlterCommandMapTool {
public:
	TableArgAlterCommandMapTool();
	Hash<const char *, TableArgAlterCmdType, Hasher<const char *>, Equaler<const char *, const char *> >* getCmdMap();

private:
	Hash<const char *, TableArgAlterCmdType, Hasher<const char *>, Equaler<const char *, const char *> > m_mp; /* Alter Table命令映射表 */
	void initCommandMap();
};

/**
 * 表参数修改辅助类。
 */
class TableArgAlterHelper {
public:
	static TableArgAlterCommandMapTool cmdMapTool; /* Alter Table命令工具对象 */

	static const char *CMD_USEMMS;					/* 使用MMS */
	static const char *CMD_CACHE_UPDATE;			/* 使用MMS更新 */
	static const char *CMD_UPDATE_CACHE_TIME;		/* 设置更新周期 */
	static const char *CMD_CACHED_COLUMNS;			/* 设置缓存列 */

	static const char *CMD_COMPRESS_LOBS;			/* 设置LOB压缩 */
	static const char *CMD_HEAP_PCT_FREE;			/* 设置空闲百分比 */
	static const char *CMD_SPLIT_FACTORS;			/* 设置分裂系数 */
	static const char *CMD_INCR_SIZE;				/* 设置扩展页大小 */
	static const char *CMD_COMPRESS_ROWS;           /* 设置是否是压缩表 */
	static const char *CMD_FIX_LEN;                 /* 设置是否使用定长堆 */
	static const char *CMD_SET_COLUMN_GROUPS;       /* 定义属性组 */
	static const char *CMD_COMPRESS_DICT_SIZE;      /* 设置压缩全局字典大小 */
	static const char *CMD_COMPRESS_DICT_MIN_LEN;   /* 设置压缩全局字典项最小长度 */
	static const char *CMD_COMPRESS_DICT_MAX_LEN;   /* 设置压缩全局字典最大长度 */
	static const char *CMD_COMPRESS_THRESHOLD;      /* 设置压缩比阀值 */
	static const char *CMD_SUPPORT_TRX;				/* 设置是否支持事务操作*/   

	static const char *ENABLE;						/* Enable 命令 */
	static const char *DISABLE;						/* Disable 命令 */
	static const int NUM_CMDS = 13;					/* 命令的数目 */
	static const char *ALLOC_SESSION_NAME;			/* 分配的话会名 */

	static const char *STR_TABLE;					/* 常量字符串"table" */
	static const char *STR_SET;						/* 常量字符串"set" */
	static const char *STR_EQUAL;					/* 常量字符串"set" */
	static const char *STR_DOT;						/* 常量字符串"." */
	static const char COL_COMMA_DELIMITER;          /* 列间逗号分割符号 ',' */

	static const char *MSG_EXCEPTION_MMS_NOT_ENABLED;					/* MMS未启动提示信息 */
	static const char *MSG_EXCEPTION_CACHE_UPDATE_NOT_ENABLED;			/* CacheUpdate未启用提示信息 */
	static const char *MSG_EXCEPTION_WRONG_COMMAND_TYPE;				/* 错误的修改命令类型提示信息 */
	static const char *MSG_EXCEPTION_WRONG_COMMAND_ARGUMENTS;			/* 错误的命令参数提示信息 */
	static const char *MSG_EXCEPTION_TOO_MANY_MMS_COLUMNS;				/* 太多列数目 */	
	static const char *MSG_EXCEPTION_NON_EXISTED_COL_NAME;				/* 不存在的列名提示信息 */
	static const char *MSG_EXCEPTION_NON_EXISTED_INDEX_NAME;			/* 不存在的索引名提示信息 */
	static const char *MSG_EXCEPTION_WRONG_CMD_FORMAT_SCHEME_TBL_CMD;	/* 命令格式scheme.table.cmd不正确提示信息 */
	static const char *MSG_EXCEPTION_INVALID_INDEX_SPLIT_FACTOR;		/* 分裂系数无效提示信息 */
	static const char *MSG_EXCEPTION_INVALID_HEAP_PCT_FREE;				/* HEAP_PCT_FREE参数无效提示信息 */	
	static const char *MSG_EXCEPTION_COLUMNGROUP_HAVE_DICTIONARY;       /* 表已经有全局字典，设置属性组无效 */
	static const char *MSG_EXCEPTION_DICT_ARG_HAVE_DICTIONARY;          /* 表已经有全局字典, 设置字典参数无效 */
	static const char *MSG_EXCEPTION_EMPTY_COL_GRP;                     /* 空属性组定义 */
	static const char *MSG_EXCEPTION_INVALID_INCR_SIZE;                 /* INCR_SIZE参数无效提示信息 */

	TableArgAlterHelper(Database *db, Connection *conn, Parser *parser, int timeout, bool inLockTables = false);
	virtual ~TableArgAlterHelper();
	void alterTableArgument() throw(NtseException);
	static void toUpper(char *str, size_t len);
	static bool checkCachedColumns(Session *session, TableDef *tableDef, u16 *cols, u16 numCols, int *numEnabled, int *numToEnable);
protected:
	virtual void alterTableArgumentReal(Session *session, vector<string> *parFiles, const char *name, const char *value) throw(NtseException);
	Database	*m_db;		/* 数据库 */
	Connection	*m_conn;	/* 使用的链接 */
	Parser		*m_parser;	/* 传入的命令解析参数 */
	int 		m_timeout;	/* 获取锁超时时间 */
	bool		m_inLockTables; /* 对应的mysql thd 是否处于lock tables中*/
};
}

#endif	
