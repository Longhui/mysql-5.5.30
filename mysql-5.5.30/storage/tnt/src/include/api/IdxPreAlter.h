/**
 * �����޸�����
 *
 * @author ��ӱΰ(xinyingwei@corp.netease.com, xinyingwei@163.org)
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
 * �����޸�����
 */
class IdxPreAlter {
public:
	IdxPreAlter(TNTDatabase *db, Connection *conn, Parser *parser);
	~IdxPreAlter();

	static const char *STR_INDEX;								/** �����ַ���"index" */
	static const char *STR_ON;									/** �����ַ���"on" */
	static const char *STR_DOT;									/** �����ַ���"." */
	static const char *STR_COMMA;								/** �����ַ���"," */
	static const char *STR_LEFT_BRACKET;						/** �����ַ���"(" */
	static const char *STR_RIGHT_BRACKET;						/** �����ַ���")" */
	static const char *STR_COL_DELIMITER;						/** �м�ָ���")," */
	static const char *STR_COL_DELIMITER_WITH_PREFIX;			/** �м�ָ���"))," */
	static const char *STR_DOUBLE_RIGHT_BRACKEY;				/** �����ַ���"))" */

	static const char *ALLOC_ADD_SESSION_NAME;					/** ��������ߴ��������Ự���� */
	static const char *ALLOC_DROP_SESSION_NAME;					/** ���������ɾ�������Ự���� */

	static const char *MSG_EXCEPTION_FORMAT_ADD_ERROR;			/** ������������ʽ����ȷ��ʾ��Ϣ */
	static const char *MSG_EXCEPTION_FORMAT_DROP_ERROR;			/** ɾ�����������ʽ����ȷ��ʾ��Ϣ */
	static const char *MSG_EXCEPTION_INVALID_COLUMN_NAME;		/** �Ƿ����������ƻ�������������� */
	static const char *MSG_EXCEPTION_INVALID_ONLINE_INDEX;		/** �Ƿ��������������ƻ��������δ����� */
	static const char *MSG_EXCEPTION_INVALID_PREFIX;			/** �Ƿ�����������ǰ׺ */
	static const char *MSG_EXCEPTION_INVALID_LOB_COLUMN;		/** ������ӵ���������������У���ʱ��֧�� */

	void createOnlineIndex() throw(NtseException);				/** ���ߴ������� */
	void deleteOnlineIndex() throw(NtseException);				/** ����ɾ������ */

	void parAddCmd(AddIndexInfo &add) throw(NtseException);		/** ���������������� */
	void parDropCmd(DropIndexInfo &drop) throw(NtseException);	/** ����ɾ���������� */

private:
	TNTDatabase		*m_db;										/** ���ݿ� */
	Connection		*m_conn;									/** ʹ�õ����� */
	Parser			*m_parser;									/** ���������������� */
};


}

#endif
