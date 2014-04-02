/**
 * ������
 *
 * @author ��Դ(wy@163.org, wangyuan@corp.netease.com)
 */

#ifndef _NTSE_PARSER_H_
#define _NTSE_PARSER_H_

#include <limits.h>

#include "misc/Global.h"
#include "util/Array.h"

namespace ntse {
/** һ���ǳ��򵥵Ľ�����
 * ���ţ���������ĸ�����ֻ��»��߹���һ�����ţ��������ݣ�һ���ֽ���Ϊһ�����ţ�
 *   �������ַ���������'��"����ʱ��������Ϊһ������
 */
class Parser {
public:
	Parser(const char *str);
	~Parser();
	bool hasNextToken() const;
	const char* nextToken(const char *delim = NULL) throw(NtseException);
	void match(const char *expected, bool matchCase = false) throw(NtseException);
	const char* nextString() throw(NtseException);
	const char* remainingString() throw(NtseException);
	int nextInt(int min = INT_MIN, int max = INT_MAX) throw(NtseException);
	void checkNoRemain() throw(NtseException);
	static Array<char *>* parseList(const char *str, char delimiter) throw(NtseException);
	static void parsePair(const char *str, char delimiter, char **first, char **second) throw(NtseException);
	static Array<char *>* parseBracketsGroups(const char *str) throw(NtseException);
	static bool parseBool(const char *str) throw(NtseException);
	static int parseInt(const char *str, int min = INT_MIN, int max = INT_MAX) throw(NtseException);
	static u64 parseU64(const char *str, u64 min = 0, u64 max = (u64)-1) throw(NtseException);
	static const char* trimWhitespace(const char *str);
	static bool isCommand(const char *cmd, ...);

private:	
	void skipWhitespace();
	static bool isDelimiter(char c, const char *delim);
	static bool isWord(char c);
	const char* setToken(const char *token);
	static char* str2int(const char *src, int radix, long lower, long upper, long *val);

private:
	const char	*m_str;			/** ���������ַ��� */
	char		*m_token;		/** ��ǰ���� */
	const char	*m_endPos;		/** �������ַ�������λ�ã�ָ��\0 */
	const char	*m_tokenStart;	/** ��ǰ������ʼλ�� */
	const char	*m_currPos;		/** �������д�������λ�� */

	static const char *MSG_NEXT_TOKEN_NOT_FOUND;		/** �޺���ַ�����Ϣ */
	static const char *MSG_UNEXPECTED_TOKEN;			/** �������ַ�����Ϣ */
	static const char *MSG_INCOMPLETE_ESCAPE_SEQUENCE;	/** ת����Ų�������Ϣ */
	static const char *MSG_INVALID_ESCAPE_SEQUENCE;		/** �Ƿ�ת�������Ϣ */
	static const char *MSG_STRING_NO_ENDING_QUOTATION;	/** �ַ����������Ų�ƥ����Ϣ */
	static const char *MSG_NO_REMAINING_STRING;			/** û��ʣ���ַ�����Ϣ */
	static const char *MSG_NEXT_INT_NOT_FOUND;			/** û���ҵ���һ��������Ϣ */
	static const char *MSG_STRING_IS_NOT_VALID_INT;		/** �Ƿ�������Ϣ */
	static const char *MSG_NUM_OUT_OF_GIVEN_RANGE;		/** ���ֳ�����Χ��Ϣ */
	static const char *MSG_UNPARSED_CONTENT_REMAINS;	/** ��δ�����ַ���Ϣ */
	static const char *MSG_INVALID_VALUE_TO_PARSE_PAIR;	/** ���ܽ�����ֵ����Ϣ */
	static const char *MSG_INVALID_BOOL_STR;			/** ��Ч�����ַ�����Ϣ */
	static const char *MSG_INVALID_DIGIT;				/** ��������Ϣ */
	static const char *MSG_INVALID_INT_OR_OUT_OF_RANGE; /** ������������������Χ��Ϣ */
	static const char *STR_TRUE;						/** true�ַ��� */
	static const char *STR_FALSE;						/** false�ַ��� */
};

}

#endif
