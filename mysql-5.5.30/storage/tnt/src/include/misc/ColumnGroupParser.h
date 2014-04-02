/**
* 属性组解析器
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _NTSE_COLUMN_GROUP_PARSER_H_
#define _NTSE_COLUMN_GROUP_PARSER_H_

#include "misc/Global.h"
#include "misc/TableDef.h"

namespace ntse {

class ColumnGroupParser {
public:
	static const char COL_COMMA_DELIMITER;                              /* 列间逗号分割符号 ',' */
	static const char *MSG_EXCEPTION_NON_EXISTED_COL_NAME;				/* 不存在的列名提示信息 */
	static const char *MSG_EXCEPTION_COLUMNGROUP_REDIFINED;             /* 划分属性组时有属性在多个属性组定义 */
	static const char *MSG_EXCEPTION_COLUMNGROUP_NOT_DEFINED;           /* 划分属性组时有属性未定义为任何一组 */
	static const char *MSG_EXCEPTION_TOO_MANY_COL_GRPS;                 /* 超过最大属性组个数限制 */
	static const char *MSG_EXCEPTION_EMPTY_COL_GRP;                     /* 空属性组定义 */

public:
	ColumnGroupParser() {}
	~ColumnGroupParser() {}
	static Array<ColGroupDef *> * parse(const TableDef *tableDef, const char *valueStr) throw(NtseException);
};

}

#endif