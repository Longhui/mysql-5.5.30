#include <assert.h>
#include "api/Table.h"
#include "strings/CtypeCommon.h"


namespace ntse {

/**
 * �Ƚ������ַ���
 *
 * @param coll Collation
 * @param str1 �ַ���1
 * @param len1 �ַ���1��С
 * @param str2 �ַ���2
 * @param len2 �ַ���2��С
 * @return str1 < str2ʱ����<0��str1 = str2ʱ����0�����򷵻�>0
 */
int Collation::strcoll(CollType coll, const byte *str1, size_t len1, const byte *str2, size_t len2) {
	switch (coll) {
		case COLL_BIN:
			return my_strnncoll_bin(str1, len1, str2, len2);
		case COLL_GBK:
			return my_strnncollsp_gbk(str1, len1, str2, len2);
		case COLL_LATIN1:
			return my_strnncollsp_latin1(str1, len1, str2, len2);
		case COLL_UTF8:
			return my_strnncollsp_utf8(str1, len1, str2, len2);
		default:
			assert(false);
	}
	return 0;
}

/**
 * ��ȡĳ�ַ���ǰN���ַ����ֽ���
 *
 * @param coll Collation
 * @param pos �ַ�����ʼ��ַ
 * @param end �ַ�����β��ַ
 * @param length �ַ���
 *
 * @return ǰlength���ַ����ֽ���
 */
size_t Collation::charpos(CollType coll, const char *pos, const char *end, size_t length) {
	const char *start = pos;
	while (length && pos < end) {
		uint mb_len;
		switch (coll) {
			case COLL_GBK:
				pos += (mb_len = ismbchar_gbk(pos, end))? mb_len: 1;
				break;
			case COLL_UTF8:
				pos += (mb_len = ismbchar_utf8(pos, end))? mb_len: 1;
				break;
			default:
				assert(false);
		}
		length--;
	}
	return (size_t)(length? end + 2 - start: pos - start);
}


/**
 * ��ȡ��ͬ�ַ��������ַ�����󳤶Ⱥ���С����
 *
 * @param coll Collation
 * @param mbMinLen OUT���ַ���С����
 * @param mbMaxLen OUT, �ַ���󳤶�
 *
 */
void Collation::getMinMaxLen(CollType coll, size_t *mbMinLen, size_t *mbMaxLen) {
	switch(coll) {
		case COLL_BIN:
			*mbMinLen = 1;
			*mbMaxLen = 1;
			break;
		case COLL_GBK:
			*mbMinLen = 1;
			*mbMaxLen = 2;
			break;
		case COLL_UTF8:
			*mbMinLen = 1;
			*mbMaxLen = 3;
			break;
		case COLL_LATIN1:
			*mbMinLen = 1;
			*mbMaxLen = 1;
	}
}


} // namespace ntse

