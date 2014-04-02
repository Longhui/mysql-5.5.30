/**
 * 二进制字符集处理
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#include "strings/CtypeCommon.h"
#include <string.h>
#include <algorithm>

namespace ntse {


int my_strnncoll_bin(const uchar *a, size_t a_length,
							const uchar *b, size_t b_length) {
	int res = memcmp(a, b, std::min(a_length, b_length));
	return res ? res : ((int)a_length - (int)b_length);
}

}
