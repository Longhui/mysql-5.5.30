/**
 * 在线修改索引
 *
 * @author 辛颖伟(xinyingwei@corp.netease.com, xinyingwei@163.org)
 */
#ifndef _TNT_IDXPREALTER_H_
#define _TNT_IDXPREALTER_H_

#include "misc/Parser.h"
#include "misc/Session.h"
#include <string>
#include <vector>

using namespace std;
using namespace ntse;

namespace tnt {

class TNTDatabase;

struct Idx
{
	string idxName;
	vector<string> attribute;
	vector<u32> prefixLenArr;
};

struct AddIndexInfo
{
	string schemaName;
	string tableName;
	vector<Idx> idxs;
};

struct DropIndexInfo
{
	string schemaName;
	string tableName;
	vector<string> idxNames;
};

/**
 * 在线修改索引
 */
class IdxPreAlter {
public:
	IdxPreAlter(TNTDatabase *db, Connection *conn, Parser *parser);
	~IdxPreAlter();

	static const char *STR_INDEX;								/** 常量字符串"index" */
	static const char *STR_ON;									/** 常量字符串"on" */
	static const char *STR_DOT;									/** 常量字符串"." */
	static const char *STR_COMMA;								/** 常量字符串"," */
	static const char *STR_LEFT_BRACKET;						/** 常量字符串"(" */
	static const char *STR_RIGHT_BRACKET;						/** 常量字符串")" */
	static const char *STR_COL_DELIMITER;						/** 列间分隔符")," */
	static const char *STR_COL_DELIMITER_WITH_PREFIX;			/** 列间分隔符"))," */
	static const char *STR_DOUBLE_RIGHT_BRACKEY;				/** 常量字符串"))" */

	static const char *ALLOC_ADD_SESSION_NAME;					/** 分配的在线创建索引会话名称 */
	static const char *ALLOC_DROP_SESSION_NAME;					/** 分配的在线删除索引会话名称 */

	static const char *MSG_EXCEPTION_FORMAT_ADD_ERROR;			/** 添加索引命令格式不正确提示信息 */
	static const char *MSG_EXCEPTION_FORMAT_DROP_ERROR;			/** 删除索引命令格式不正确提示信息 */
	static const char *MSG_EXCEPTION_INVALID_COLUMN_NAME;		/** 非法的属性名称或该属性名不存在 */
	static const char *MSG_EXCEPTION_INVALID_ONLINE_INDEX;		/** 非法的在线索引名称或该索引还未被添加 */
	static const char *MSG_EXCEPTION_INVALID_PREFIX;			/** 非法的在线索引前缀 */
	static const char *MSG_EXCEPTION_INVALID_LOB_COLUMN;		/** 在线添加的索引包含大对象列，暂时不支持 */

	void createOnlineIndex() throw(NtseException);				/** 在线创建索引 */
	void deleteOnlineIndex() throw(NtseException);				/** 在线删除索引 */

	void parAddCmd(AddIndexInfo &add) throw(NtseException);		/** 解析创建索引命令 */
	void parDropCmd(DropIndexInfo &drop) throw(NtseException);	/** 解析删除索引命令 */

private:
	TNTDatabase		*m_db;										/** 数据库 */
	Connection		*m_conn;									/** 使用的链接 */
	Parser			*m_parser;									/** 传入的命令解析参数 */
};


}

#endif
