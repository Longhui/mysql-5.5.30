/**
 * Decimal类型相关函数实现
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */
#include "misc/Decimal.h"
#include "util/Portable.h"
#include <assert.h>

using namespace ntse;


#define DIG_PER_DEC1 9

typedef s32 dec1;

static const int dig2bytes[DIG_PER_DEC1+1]={0, 1, 1, 2, 2, 3, 3, 4, 4, 4};


/*
Returns the size of array to hold a binary representation of a decimal

RETURN VALUE
size in bytes
*/

int decimal_bin_size(int precision, int scale)
{
	int intg=precision-scale,
		intg0=intg/DIG_PER_DEC1, frac0=scale/DIG_PER_DEC1,
		intg0x=intg-intg0*DIG_PER_DEC1, frac0x=scale-frac0*DIG_PER_DEC1;

	assert(scale >= 0 && precision > 0 && scale <= precision);
	return intg0*sizeof(dec1)+dig2bytes[intg0x]+
		frac0*sizeof(dec1)+dig2bytes[frac0x];
}




/**
 * 得到Decimal的占用的空间长度
 * @param precision decimal的总位数
 * @param scale decimal的小数点位数
 * @return Decimal占用的内存长度
 */
int Decimal::getBinSize(int precision, int scale) {
	return decimal_bin_size(precision, scale);
}


