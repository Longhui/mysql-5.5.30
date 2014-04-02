/**
* �����������
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _NTSE_COLUMN_GROUP_PARSER_H_
#define _NTSE_COLUMN_GROUP_PARSER_H_

#include "misc/Global.h"
#include "misc/TableDef.h"

namespace ntse {

class ColumnGroupParser {
public:
	static const char COL_COMMA_DELIMITER;                              /* �м䶺�ŷָ���� ',' */
	static const char *MSG_EXCEPTION_NON_EXISTED_COL_NAME;				/* �����ڵ�������ʾ��Ϣ */
	static const char *MSG_EXCEPTION_COLUMNGROUP_REDIFINED;             /* ����������ʱ�������ڶ�������鶨�� */
	static const char *MSG_EXCEPTION_COLUMNGROUP_NOT_DEFINED;           /* ����������ʱ������δ����Ϊ�κ�һ�� */
	static const char *MSG_EXCEPTION_TOO_MANY_COL_GRPS;                 /* �������������������� */
	static const char *MSG_EXCEPTION_EMPTY_COL_GRP;                     /* �������鶨�� */

public:
	ColumnGroupParser() {}
	~ColumnGroupParser() {}
	static Array<ColGroupDef *> * parse(const TableDef *tableDef, const char *valueStr) throw(NtseException);
};

}

#endif