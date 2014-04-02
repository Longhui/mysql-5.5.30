/**
 * һ�����ܼ򵥵Ľ�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 * @author ������(neimingjun@corp.netease.com, niemingjun@163.org)
 * @author ��ΰ��(liweizhao@corp.netease.com)
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

/** ����һ�����ڽ���ָ���ַ����Ľ���������
 * @param str ���������ַ�����ֱ������
 */
Parser::Parser(const char *str) {
	m_str = str;
	m_token = NULL;
	m_currPos = m_str;
	m_tokenStart = m_str;
	m_endPos = m_str + strlen(str);
}

/** �������� */
Parser::~Parser() {
	delete []m_token;
	m_str = NULL;
	m_currPos = NULL;
	m_tokenStart = NULL;
	m_endPos = NULL;
}

/** 
 *	����Ƿ�����һ�����š�
 *
 * @return �Ƿ�����һ�����š�
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
 * ��ȡ��һ�����ţ�ȥ�����˿հף����������κ�����������
 *
 * @param delim �����, Ĭ��NULL����Ӣ����ĸ�����֡��»���Ϊһ��token�������ַ�Ϊһ��token������NULL������delim���ַ����зָ
 *
 * @return ��һ�����ţ�ָ���ڲ����ͣ�����벻Ҫ�޸����ͷţ���һ�ε���nextToken����Զ��ͷ�
 * @throw NtseException û����һ������
 */
const char* Parser::nextToken(const char *delim/* = NULL*/) throw(NtseException) {
	m_tokenStart = m_currPos;

	bool wantRegWord = true; //��һ��token�Ƿ�����ĸ�����֡��»�����ɵ�token
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
 * ƥ����һ�����š�
 *
 * @param expected ��һ������Ӧ�������
 * @param matchCase �Ƿ�ƥ���Сд
 *
 * @throw NtseException ��һ�����Ų���expected
 */
void Parser::match(const char *expected, bool matchCase) throw(NtseException) {
	const char *token = nextToken();
	if ((matchCase && strcmp(token, expected)) || (!matchCase && System::stricmp(token, expected)))
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_UNEXPECTED_TOKEN, expected, token);
}

/** 
 * ��ȡ��һ���ַ������͵ķ��ţ�ȥ�����˿հף�ͬʱȥ�����������ַ����ĵ�����(')�Ի���˫����(��)�ԣ������з�ת�壩
 * ֧��ʹ��\x��ʽ��ת�壬����x������n��r��t��"������\���ַ�
 * 
 * @return ��һ�����ţ�ָ���ڲ����ͣ�����벻Ҫ�޸����ͷ�
 * @throw NtseException û����һ�����ţ����ʽ������
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
 * ��ȡʣ���δ�������ַ���(ȥ��ǰ���հ�)��
 *
 * @throw NtseException û��ʣ��δ���������ַ���
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
/** ��ȡ��һ������Ϊ�Ǹ������ķ���
 * @param min ��С�����ֵ
 * @param max ��������ֵ
 * @return ����ֵ
 * @throw NtseException û����һ�����ţ�����һ�����Ų����������ͣ����Ǹ���
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

/** ����Ƿ����������Ѿ��������
 * @throw NtseException ���зǿհ�����û�з������
 */
void Parser::checkNoRemain() throw(NtseException) {
	m_tokenStart = m_currPos;
	skipWhitespace();
	if (m_currPos < m_endPos)
		NTSE_THROW(NTSE_EC_SYNTAX_ERROR, MSG_UNPARSED_CONTENT_REMAINS, m_currPos);
}

/** �����հ����� */
void Parser::skipWhitespace() {
	assert(m_tokenStart == m_currPos);
	while (m_currPos < m_endPos && isspace(*m_currPos)) {
		m_currPos++;
		m_tokenStart++;
	}
}

/**
* �����ַ����ĸ����� ȥ���м���ֵĿհ��ַ�
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
 * ����Ƿ���ĸ���ֻ����»��ߡ�
 *
 * @param c �ַ���
 * @return �Ƿ���ĸ�����ֻ����»���
 */
bool Parser::isWord(char c){
	return (isalnum(c) || ('_' == c) || ('#' == c));
}

/** 
 * ����Ƿ��Ƿָ�����
 * 
 * @param c		�����ַ�
 * @param delim	�ָ���
 *
 * @return �Ƿ�ָ����š�true��ʾ�Ƿָ�����false��ʾ�Ƿָ�����
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

/** ���õ�ǰ����
 * @param token Ҫ���õ�ֵ
 * @return ��ǰ����
 */
const char* Parser::setToken(const char *token) {
	delete []m_token;
	m_token = System::strdup(token);
	return m_token;
}

/** ����һ���ַ����б�
 * @param str ���������ַ����б�
 * @param delimiter �ָ���
 * @return �������
 * throw NtseException ��ʽ����
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

/** ����һ������"first delimiter second"���ַ�����
 * @param str ���������ַ���
 * @param delimiter �ָ���
 * @param first OUT����һ��
 * @param second OUT���ڶ���
 * @throw  NtseException ��ʽ����
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
 * �����Ű�Χ���ַ����������б�
 * ��������Ϊ����"(first,second,third),(four,five)..."���ַ���, ����Ѿ�ȥ������
 * @param str Ҫ�������ַ���
 * @throw  NtseException ��ʽ����
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
			const char *nextToken = parser->nextToken(leftDelim);//����������֮ǰ���ַ�
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

/** ����һ���ַ�����ʾ�Ĳ����͡���ȷ�ĸ�ʽΪ��Сд�޹ص�true��false
 * @param str ���������ַ���
 * @return �������
 * @throw  NtseException ��ʽ����
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

/** ����һ���ַ�����ʾ������
 * @param str ���������ַ���
 * @param min ��С�����ֵ
 * @param max ��������ֵ
 * @return �������
 * @throw  NtseException ��ʽ����Խ���
 */
int Parser::parseInt(const char *str, int min, int max) throw(NtseException) {
	long v;
	const char *remain = str2int(str, 10, min, max, &v);
	if (!remain || *remain != '\0') {
		NTSE_THROW(NTSE_EC_GENERIC, MSG_INVALID_INT_OR_OUT_OF_RANGE, str, min, max);
	}
	return (u32)v;
}

/** ����һ���ַ�����ʾ��64λ����
 * @param str ���������ַ���
 * @param min ��С�����ֵ
 * @param max ��������ֵ
 * @return �������
 * @throw  NtseException ��ʽ����
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

/** �ж�cmd�Ƿ�Ϊָ��������
 * @param cmd ���жϵ������ַ���
 * @... ��ʶ�����ǰn�����ʣ���NULL����
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

/** ���ַ���ת��Ϊ������ʵ�ֲο�MySQL str2int.c���롣
 *
 * @param src �ַ���
 * @param radix ���ƣ�������[2..36]֮��
 * @param lower �½磬����
 * @param upper �Ͻ磬����
 * @param val OUT������ֵ
 * @return �ɹ����������ַ�����δ�������ݵ���ʼλ�ã�ʧ�ܷ���NULL
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

