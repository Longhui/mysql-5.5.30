/**
 * 一个功能简单的解析器
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 * @author 聂明军(neimingjun@corp.netease.com, niemingjun@163.org)
 * @author 李伟钊(liweizhao@corp.netease.com)
 */

#include "misc/Parser.h"
#include <string>
#include "util/SmartPtr.h"

using namespace ntse;

namespace ntse {
const char * Parser::MSG_NEXT_TOKEN_NOT_FOUND = "Next token not found.";
const char * Parser::MSG_UNEXPECTED_TOKEN = "Unexpected token, expect %s but is %s";
const char * Parser::MSG_INCOMPLETE_ESCAPE_SEQUENCE = "Incomplete escape sequence";
const char * Parser::MSG_INVALID_ESCAPE_SEQUENCE = "Invalid escape sequence: \\%c";
const char * Parser::MSG_STRING_NO_ENDING_QUOTATION = "Invalid string! %s has no ending quotation.";
const char * Parser::MSG_NO_REMAINING_STRING = "No remaining string.";
const char * Parser::MSG_NEXT_INT_NOT_FOUND = "Next int not found.";
const char * Parser::MSG_STRING_IS_NOT_VALID_INT = "%s is not a valid int.";
const char * Parser::MSG_NUM_OUT_OF_GIVEN_RANGE = "ld is out of the given range [%d, %d].";
const char * Parser::MSG_UNPARSED_CONTENT_REMAINS = "Unparsed content remains";
const char * Parser::MSG_INVALID_VALUE_TO_PARSE_PAIR = "%s is not a valid value to parse pair with delimiter %c.";
const char * Parser::MSG_INVALID_BOOL_STR = "%s is not a valid value for bool.";
const char * Parser::MSG_INVALID_DIGIT = "%c is not a digit.";
const char * Parser::MSG_INVALID_INT_OR_OUT_OF_RANGE = "%s is not a valid int or out of range [%d, %d].";
const char * Parser::STR_TRUE = "true";
const char * Parser::STR_FALSE = "false";

/** 创建一个用于解析指定字符串的解析器对象
 * @param str 待解析的字符串，直接引用
 */
Parser::Parser(const char *str) {
	m_str = str;
	m_token = NULL;
	m_currPos = m_str;
	m_tokenStart = m_str;
	m_endPos = m_str + strlen(str);
}

/** 析构函数 */
Parser::~Parser() {
	delete []m_token;
	m_str = NULL;
	m_currPos = NULL;
	m_tokenStart = NULL;
	m_endPos = NULL;
}

/** 
 *	检查是否有下一个符号。
 *
 * @return 是否有下一个符号。
 */
bool Parser::hasNextToken() const {
	const char *r = m_currPos;
	while (r < m_endPos) {
		if (!isspace(*r)) {
			break;
		}
		++r;
	}
	return r < m_endPos;
}

/** 
 * 获取下一个符号（去掉两端空白，但不进行任何其它处理）。
 *
 * @param delim 定界符, 默认NULL代表英文字母、数字、下划线为一个token，其他字符为一个token；若非NULL，则以delim中字符进行分割。
 *
 * @return 下一个符号，指向内部类型，外界请不要修改与释放，下一次调用nextToken后会自动释放
 * @throw NtseException 没有下一个符号
 */
const char* Parser::nextToken(const char *delim/* = NULL*/) throw(NtseException) {
	m_tokenStart = m_currPos;

	bool wantRegWord = true; //下一个token是否是字母、数字、下划线组成的token
	skipWhitespace();
		
	if (m_currPos >= m_endPos)
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_NEXT_TOKEN_NOT_FOUND);
	
	if (NULL == delim && !isWord(*m_currPos)){
		wantRegWord = false;
	}
	std::string r;
	int numEndSpace = 0;
	while (m_currPos < m_endPos) {
		char c = *m_currPos;
		if (NULL == delim) {
			if ( !isWord(c) && wantRegWord) {				
				break;
			} else if (!isWord(c) && !wantRegWord) {
				while (m_currPos < m_endPos) {
					if(!isWord(c) && !isspace(c)) {
						r += c;
						m_currPos++;
						c = *m_currPos;
					} else {
						break;
					}
				}
				break;
			}
		} else {
			if (isDelimiter(c, delim)) {
				++m_currPos;
				break;
			} 
		}
		r = r + c;
		m_currPos++;
		if (isspace(c)) {
			++numEndSpace;
		} else {
			numEndSpace = 0;
		}
	}
	r = r.substr(0, r.length() - numEndSpace);
	return setToken(r.c_str());
}

/** 
 * 匹配下一个符号。
 *
 * @param expected 下一个符号应该是这个
 * @param matchCase 是否匹配大小写
 *
 * @throw NtseException 下一个符号不是expected
 */
void Parser::match(const char *expected, bool matchCase) throw(NtseException) {
	const char *token = nextToken();
	if ((matchCase && strcmp(token, expected)) || (!matchCase && System::stricmp(token, expected)))
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_UNEXPECTED_TOKEN, expected, token);
}

/** 
 * 获取下一个字符串类型的符号（去掉两端空白，同时去掉用于括起字符串的单引号(')对或者双引号(”)对，并进行反转义）
 * 支持使用\x形式的转义，其中x可以是n、r、t、"、‘、\等字符
 * 
 * @return 下一个符号，指向内部类型，外界请不要修改与释放
 * @throw NtseException 没有下一个符号，或格式有问题
 */
const char* Parser::nextString() throw(NtseException) {
	m_tokenStart = m_currPos;
	skipWhitespace();
	if (m_currPos >= m_endPos)
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_NEXT_TOKEN_NOT_FOUND);
	bool doubleQuoted = false;
	bool singleQuoted = false; 
	bool hasEndQuotation = false;
	doubleQuoted = ('\"' == *m_currPos);
	singleQuoted = ('\''== *m_currPos);
	if (doubleQuoted || singleQuoted) {
		m_currPos++;
		m_tokenStart++;
	}
	std::string r;
	int numEndSpace = 0;
	while (m_currPos < m_endPos) {
		char c = *m_currPos;
		if (c == '\\') {
			m_currPos++;
			if (m_currPos == m_endPos)
				NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_INCOMPLETE_ESCAPE_SEQUENCE);
			c = *m_currPos;
			switch (c) {
				case 'n':
					r += '\n';
					++numEndSpace;
					break;
				case 'r':
					r += '\r';
					++numEndSpace;
					break;
				case 't':
					r += '\t';
					++numEndSpace;
					break;
				case '\\':
					r += '\\';
					numEndSpace = 0;
					break;
				case '\"':
					r += '\"';
					numEndSpace = 0;
					break;
				case '\'':
					r += '\'';
					numEndSpace = 0;
					break;
				default:
					NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_INVALID_ESCAPE_SEQUENCE, c);
					break;
			}
		} else if (c == '\"') {			
			if (doubleQuoted) {
				++m_currPos;
				hasEndQuotation = true;
				break;
			}
			r += '\"';
		} else if (c == '\'') {			
			if (singleQuoted) {
				++m_currPos;
				hasEndQuotation = true;
				break;
			}
			r += '\'';
		} else {
			if (isspace(c)) {
				if ((!doubleQuoted) && (!singleQuoted)) {
					break;
				}
				++numEndSpace;				
			} else {
				numEndSpace = 0;
			}
			r = r + c;
		}
		++m_currPos;
	}
	if ((doubleQuoted || singleQuoted) && !hasEndQuotation){
		NTSE_THROW(NTSE_EC_GENERIC, MSG_STRING_NO_ENDING_QUOTATION, m_tokenStart);
	}
	r = r.substr(0, (r.length() - numEndSpace));
	return setToken(r.c_str());
}

/** 
 * 获取剩余的未被解析字符串(去掉前导空白)。
 *
 * @throw NtseException 没有剩余未被解析的字符。
 */
const char* Parser::remainingString() throw(NtseException) {
	const char *r = m_currPos;
	while ((r < m_endPos) && isspace(*r)) {
		++r;
	}
	if (r >= m_endPos) {
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_NO_REMAINING_STRING);
	}
	return r;
}
/** 获取下一个内容为非负整数的符号
 * @param min 最小允许的值
 * @param max 最大允许的值
 * @return 整数值
 * @throw NtseException 没有下一个符号，若下一个符号不是整数类型，或是负数
 */
int Parser::nextInt(int min, int max) throw(NtseException) {
	m_tokenStart = m_currPos;
	if (m_currPos >= m_endPos)
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_NEXT_INT_NOT_FOUND);
	skipWhitespace();
	bool isNegative = false;
	long long result = 0L;
	const char *token = NULL;
	switch (*m_currPos) {
		case '-':
			isNegative = true;
			//no break here;
		case '+':
			++m_currPos;
			++m_tokenStart;
			//no break here;
		default:
			if ((m_currPos >= m_endPos) || (!isdigit(*m_currPos)))
				NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_NEXT_INT_NOT_FOUND);
			token = nextToken();
			break;
	}
	try {
		result = (long long)parseU64(token);
		(isNegative) ? result = -result : NULL;
	} catch (NtseException &) {
		NTSE_THROW(NTSE_EC_GENERIC, MSG_STRING_IS_NOT_VALID_INT, token);
	}
	if (result < min || result > max) {
		NTSE_THROW(NTSE_EC_GENERIC, MSG_NUM_OUT_OF_GIVEN_RANGE, result, min, max);
	}
	
	return int(result);
}

/** 检查是否所有内容已经分析完毕
 * @throw NtseException 还有非空白内容没有分析完毕
 */
void Parser::checkNoRemain() throw(NtseException) {
	m_tokenStart = m_currPos;
	skipWhitespace();
	if (m_currPos < m_endPos)
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_UNPARSED_CONTENT_REMAINS, m_currPos);
}

/** 跳过空白内容 */
void Parser::skipWhitespace() {
	assert(m_tokenStart == m_currPos);
	while (m_currPos < m_endPos && isspace(*m_currPos)) {
		m_currPos++;
		m_tokenStart++;
	}
}

/**
* 返回字符串的副本， 去掉中间出现的空白字符
*/
const char* Parser::trimWhitespace(const char *str) {
	const size_t len = strlen(str);
	char *buf = new char[len + 1];
	char *dest = buf;
	for (size_t i = 0; i < len; i++) {
		if (!isspace(*(str + i)))
			(*dest++) = *(str + i);
	}
	(*dest) = '\0';
	const char *r = System::strdup(buf);
	delete []buf;
	return r;
}

/** 
 * 检查是否字母数字或者下划线。
 *
 * @param c 字符。
 * @return 是否字母、数字或者下滑线
 */
bool Parser::isWord(char c){
	return (isalnum(c) || ('_' == c) || ('#' == c));
}

/** 
 * 检查是否是分隔符。
 * 
 * @param c		检查的字符
 * @param delim	分隔符
 *
 * @return 是否分隔符号。true表示是分隔符；false表示非分隔符。
 */
bool Parser::isDelimiter(char c, const char *delim) {
	assert(NULL != delim);
	bool match = false;
	while ('\0' != *delim) {
		if (*delim == c) {
			match = true;
			break;
		}
		++delim;
	}
	return match;
}

/** 设置当前符号
 * @param token 要设置的值
 * @return 当前符号
 */
const char* Parser::setToken(const char *token) {
	delete []m_token;
	m_token = System::strdup(token);
	return m_token;
}

/** 解析一个字符串列表
 * @param str 待解析的字符串列表
 * @param delimiter 分隔符
 * @return 解析结果
 * throw NtseException 格式错误
 */
Array<char *>* Parser::parseList(const char *str, char delimiter) throw(NtseException) {
	assert(NULL != str);
	Array<char *> *r = new Array<char *>();
	char delim[2] = {delimiter, '\0'};
	AutoPtr<Parser> parser(new Parser(str));
	const char *token = NULL;
	
	while (parser->hasNextToken()) {
		try {
			token = parser->nextToken(delim);
		} catch (NtseException &) {
			//no exception expected.
			assert(false);
		}		
		r->push(System::strdup(token));
	}
	return r;
}

/** 解析一个形如"first delimiter second"的字符串对
 * @param str 待解析的字符串
 * @param delimiter 分隔符
 * @param first OUT，第一项
 * @param second OUT，第二项
 * @throw  NtseException 格式错误
 */
void Parser::parsePair(const char *str, char delimiter, char **first, char **second) throw(NtseException) {
	assert(NULL != str);
	char delim[2] = {delimiter, '\0'};
	AutoPtr<Parser> parser(new Parser(str));
	*first = *second = NULL;
	try {
		*first = System::strdup(parser->nextToken(delim));
		*second = System::strdup(parser->remainingString());
	} catch (NtseException &) {
		delete[] *first;
		delete[] *second;
		NTSE_THROW(NTSE_EC_GENERIC, MSG_INVALID_VALUE_TO_PARSE_PAIR, str, delimiter);
	}	
}

/**
 * 将括号包围的字符串解析成列表
 * 待解析的为形如"(first,second,third),(four,five)..."的字符串, 输出已经去除括号
 * @param str 要解析的字符串
 * @throw  NtseException 格式错误
 */
Array<char *>* Parser::parseBracketsGroups(const char *str) throw(NtseException) {
	assert(NULL != str);
	Array<char *> *r = new Array<char *>();
	char leftDelim[2] = {'(', '\0'};
	char rightDelim[2] = {')', '\0'};

	try {
		AutoPtr<Parser> parser(new Parser(str));
		parser->skipWhitespace();	
		while (parser->hasNextToken()) {
			const char *nextToken = parser->nextToken(leftDelim);//跳过左括号之前的字符
			const char *cur = nextToken;
			const char *end = nextToken + strlen(nextToken);
			bool hitComma = false;
			while (cur != end) {
				if (*cur == ',') {
					if (hitComma)
						NTSE_THROW(NTSE_EC_GENERIC, "There is syntax error near \"%s\".", nextToken);
					hitComma = true;
				} else {
					if (*cur != ' ')
						NTSE_THROW(NTSE_EC_GENERIC, "There is syntax error near \"%s\".", nextToken);
				} 
				cur++;
			}
			nextToken = parser->nextToken(rightDelim);
			r->push(System::strdup(nextToken));
		}
	} catch (NtseException &e) {
		for (size_t i = 0; i < r->getSize(); i++)
			delete [] (*r)[i];
		delete r;
		throw e;
	}

	return r;
}

/** 解析一个字符串表示的布尔型。正确的格式为大小写无关的true或false
 * @param str 待解析的字符串
 * @return 解析结果
 * @throw  NtseException 格式错误
 */
bool Parser::parseBool(const char *str) throw(NtseException) {
	if (!System::stricmp(str, STR_TRUE)) {
		return true;
	} else if (!System::stricmp(str, STR_FALSE)) {
		return false;
	} else {
		NTSE_THROW(NTSE_EC_GENERIC, MSG_INVALID_BOOL_STR, str);
	}
}

/** 解析一个字符串表示的整数
 * @param str 待解析的字符串
 * @param min 最小允许的值
 * @param max 最大允许的值
 * @return 解析结果
 * @throw  NtseException 格式错误，越界等
 */
int Parser::parseInt(const char *str, int min, int max) throw(NtseException) {
	long v;
	const char *remain = str2int(str, 10, min, max, &v);
	if (!remain || *remain != '\0') {
		NTSE_THROW(NTSE_EC_GENERIC, MSG_INVALID_INT_OR_OUT_OF_RANGE, str, min, max);
	}
	return (u32)v;
}

/** 解析一个字符串表示的64位整数
 * @param str 待解析的字符串
 * @param min 最小允许的值
 * @param max 最大允许的值
 * @return 解析结果
 * @throw  NtseException 格式错误
 */
u64 Parser::parseU64(const char *str, u64 min, u64 max) throw(NtseException) {
	u64 n = 0;
	const char *p = str;
	while (*p) {
		char ch = *p;
		if (ch < '0' || ch > '9')
			NTSE_THROW(NTSE_EC_GENERIC, MSG_INVALID_DIGIT, ch);
		n = n * 10 + (ch - '0');
		p++;
	}
	if (n < min || n > max)
		NTSE_THROW(NTSE_EC_GENERIC, I64FORMAT"u is out of the given range ["I64FORMAT"u, "I64FORMAT"u].", n, min, max);
	return n;
}

/** 判断cmd是否为指定的命令
 * @param cmd 待判断的命令字符串
 * @... 标识命令的前n个单词，以NULL结束
 */
bool Parser::isCommand(const char *cmd, ...) {
	AutoPtr<Parser> parser(new Parser(cmd));
	va_list args;
	va_start(args, cmd);
	while (true) {
		char *expected = va_arg(args, char *);
		if (!expected)
			break;
		try {
			parser->match(expected);
		} catch (NtseException &) {
			return false;
		}
	}
	va_end(args);
	return true;
}

#define char_val(X) (X >= '0' && X <= '9' ? X-'0' :\
	X >= 'A' && X <= 'Z' ? X-'A'+10 :\
	X >= 'a' && X <= 'z' ? X-'a'+10 :\
	'\177')

/** 将字符串转化为整数。实现参考MySQL str2int.c代码。
 *
 * @param src 字符串
 * @param radix 进制，必须在[2..36]之间
 * @param lower 下界，包含
 * @param upper 上界，包含
 * @param val OUT，整数值
 * @return 成功返回输入字符串中未解析内容的起始位置，失败返回NULL
 */
char* Parser::str2int(const char *src, int radix, long lower, long upper, long *val) {
	assert(radix >= 2 && radix <= 36);
	int sign;			// is number negative (+1) or positive (-1)
	int n;				// number of digits yet to be converted
	long limit;			// "largest" possible valid input
	long scale;			// the amount to multiply next digit by
	long sofar;			// the running value
	register int d;		// (negative of) next digit
	char *start;
	int digits[32];		// Room for numbers

	//  Make sure *val is sensible in case of error
	*val = 0;

	/*  The basic problem is: how do we handle the conversion of
	a number without resorting to machine-specific code to
	check for overflow?  Obviously, we have to ensure that
	no calculation can overflow.  We are guaranteed that the
	"lower" and "upper" arguments are valid machine integers.
	On sign-and-magnitude, twos-complement, and ones-complement
	machines all, if +|n| is representable, so is -|n|, but on
	twos complement machines the converse is not true.  So the
	"maximum" representable number has a negative representative.
	Limit is set to min(-|lower|,-|upper|); this is the "largest"
	number we are concerned with.	*/

	/*  Calculate Limit using Scale as a scratch variable  */

	if ((limit = lower) > 0)
		limit = -limit;
	if ((scale = upper) > 0)
		scale = -scale;
	if (scale < limit)
		limit = scale;

	/*  Skip leading spaces and check for a sign.
	Note: because on a 2s complement machine MinLong is a valid
	integer but |MinLong| is not, we have to keep the current
	converted value (and the scale!) as *negative* numbers,
	so the sign is the opposite of what you might expect.
	*/
	while (isspace(*src))
		src++;
	sign = -1;
	if (*src == '+')
		src++; 
	else if (*src == '-') {
		src++;
		sign = 1;
	}

	/*  Skip leading zeros so that we never compute a power of radix
	in scale that we won't have a need for.  Otherwise sticking
	enough 0s in front of a number could cause the multiplication
	to overflow when it neededn't.
	*/
	start = (char *)src;
	while (*src == '0')
		src++;

	/*  Move over the remaining digits.  We have to convert from left
	to left in order to avoid overflow.  Answer is after last digit.
	*/

	for (n = 0; (digits[n] = char_val(*src)) < radix && n < 20; n++, src++)
		;

	/*  Check that there is at least one digit  */

	if (start == src) {
		return NULL;
	}

	/*  The invariant we want to maintain is that src is just
	to the right of n digits, we've converted k digits to
	sofar, scale = -radix**k, and scale < sofar < 0.	Now
	if the final number is to be within the original
	Limit, we must have (to the left)*scale+sofar >= Limit,
	or (to the left)*scale >= Limit-sofar, i.e. the digits
	to the left of src must form an integer <= (Limit-sofar)/(scale).
	In particular, this is true of the next digit.  In our
	incremental calculation of Limit,

	IT IS VITAL that (-|N|)/(-|D|) = |N|/|D|
	*/

	for (sofar = 0, scale = -1; --n >= 1;) {
		if ((long) - (d = digits[n]) < limit) {
			return NULL;
		}
		limit = (limit + d) / radix;
		sofar += d * scale;
		scale *= radix;
	}
	if (n == 0) {
		if ((long) -(d = digits[n]) < limit) {		/* get last digit */
			return NULL;
		}
		sofar += d * scale;
	}

	/*  Now it might still happen that sofar = -32768 or its equivalent,
	so we can't just multiply by the sign and check that the result
	is in the range lower..upper.  All of this caution is a right
	pain in the neck.  If only there were a standard routine which
	says generate thus and such a signal on integer overflow...
	But not enough machines can do it *SIGH*.
	*/
	if (sign < 0) {
		if (sofar < -LONG_MAX) { 
			return NULL;
		}
		sofar = -sofar;
		if (sofar < lower || sofar > upper) {
			return NULL;
		}
	} else if (sofar < lower || sofar > upper) {
		return NULL;
	}
	*val = sofar;
	return (char*) src;
}

}

