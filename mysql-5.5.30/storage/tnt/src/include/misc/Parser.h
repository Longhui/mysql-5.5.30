/**
 * 解析器
 *
 * @author 汪源(wy@163.org, wangyuan@corp.netease.com)
 */

#ifndef _NTSE_PARSER_H_
#define _NTSE_PARSER_H_

#include <limits.h>

#include "misc/Global.h"
#include "util/Array.h"

namespace ntse {
/** 一个非常简单的解析器
 * 符号：连续的字母、数字或下划线构成一个符号，其它内容，一个字节作为一个符号，
 *   但对于字符串内容用'或"括起时，整个作为一个符号
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
	const char	*m_str;			/** 待解析的字符串 */
	char		*m_token;		/** 当前符号 */
	const char	*m_endPos;		/** 待解析字符串结束位置，指向\0 */
	const char	*m_tokenStart;	/** 当前符号起始位置 */
	const char	*m_currPos;		/** 接下来有待分析的位置 */

	static const char *MSG_NEXT_TOKEN_NOT_FOUND;		/** 无后继字符串消息 */
	static const char *MSG_UNEXPECTED_TOKEN;			/** 非期望字符串消息 */
	static const char *MSG_INCOMPLETE_ESCAPE_SEQUENCE;	/** 转义符号不完整消息 */
	static const char *MSG_INVALID_ESCAPE_SEQUENCE;		/** 非法转义符号消息 */
	static const char *MSG_STRING_NO_ENDING_QUOTATION;	/** 字符串结束引号不匹配消息 */
	static const char *MSG_NO_REMAINING_STRING;			/** 没有剩余字符串消息 */
	static const char *MSG_NEXT_INT_NOT_FOUND;			/** 没有找到下一个整数消息 */
	static const char *MSG_STRING_IS_NOT_VALID_INT;		/** 非法整数消息 */
	static const char *MSG_NUM_OUT_OF_GIVEN_RANGE;		/** 数字超出范围消息 */
	static const char *MSG_UNPARSED_CONTENT_REMAINS;	/** 有未解析字符消息 */
	static const char *MSG_INVALID_VALUE_TO_PARSE_PAIR;	/** 不能解析成值对消息 */
	static const char *MSG_INVALID_BOOL_STR;			/** 无效布尔字符串消息 */
	static const char *MSG_INVALID_DIGIT;				/** 非数字消息 */
	static const char *MSG_INVALID_INT_OR_OUT_OF_RANGE; /** 非整数或整数超出范围消息 */
	static const char *STR_TRUE;						/** true字符串 */
	static const char *STR_FALSE;						/** false字符串 */
};

}

#endif
