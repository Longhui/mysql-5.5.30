/**
 * latin1字符集处理
 *
 *	基于Mysql的Ctype-latin1.c实现
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */

#include "strings/CtypeCommon.h"
#include <algorithm>

namespace ntse {
/**  
 * 简单比较函数(基于排序映射表)
 *
 * 基于Ctype-simple.c改写 
 * @param map 排序映射表 
 * @param s 待比较串1
 * @param slen 串1长度
 * @param t 待比较串2
 * @param tlen 串2长度
 * @return str1 < str2时返回<0，str1 = str2时返回0，否则返回>0
 */
	int my_strnncollsp_simple(uchar *map,  const uchar *a, size_t a_length, 
		const uchar *b, size_t b_length) {
	const uchar* end;
	size_t length;
	int res;
	
	end= a + (length= std::min(a_length, b_length));
	while (a < end)
	{
		if (map[*a++] != map[*b++])
			return ((int) map[a[-1]] - (int) map[b[-1]]);
	}
	res= 0;
	if (a_length != b_length)
	{
		int swap= 1;

		/*
			Check the next not space character of the longer key. If it's < ' ',
			 then it's smaller than the other key.
		*/
		if (a_length < b_length)
		{
			 /* put shorter key in s */
			a_length= b_length;
			a= b;
			swap= -1;                                 /* swap sign of result */
			res= -res;
		}
		for (end= a + a_length-length; a < end ; a++)
		{
			if (map[*a] != map[' '])
			return (map[*a] < map[' ']) ? -swap : swap;
		}
  }
  return res;
}


static uchar sort_order_latin1[] = {
    0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
   16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
   32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
   48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
   64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
   80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95,
   96, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
   80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90,123,124,125,126,127,
  128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,
  144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,
  160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,
  176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,
   65, 65, 65, 65, 92, 91, 92, 67, 69, 69, 69, 69, 73, 73, 73, 73,
   68, 78, 79, 79, 79, 79, 93,215,216, 85, 85, 85, 89, 89,222,223,
   65, 65, 65, 65, 92, 91, 92, 67, 69, 69, 69, 69, 73, 73, 73, 73,
   68, 78, 79, 79, 79, 79, 93,247,216, 85, 85, 85, 89, 89,222,255
};

/** 比较latin1字符 */
int my_strnncollsp_latin1(const byte *s, size_t slen,
							   const byte *t, size_t tlen) {
	return my_strnncollsp_simple(sort_order_latin1, s, slen, t, tlen);
}


}
