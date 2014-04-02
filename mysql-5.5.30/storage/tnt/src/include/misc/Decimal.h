/**
 * Decimal类型相关函数
 *
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */


#ifndef _NTSE_DECIMAL_H_
#define _NTSE_DECIMAL_H_

#include "util/Portable.h"

namespace ntse {

//////////////////////////////////////////////////////////////////////////
//// Types from MYSQL Source Code
//////////////////////////////////////////////////////////////////////////
	
typedef s32 decimal_digit_t;
typedef bool my_bool;

typedef struct st_decimal_t {
	int    intg, frac, len;
	my_bool sign;
	decimal_digit_t buf[1];
} decimal_t;

class MemoryContext;

/**
 * Decimal类型工具类
 */
class Decimal {
public:
	Decimal* makeDecimal(unsigned int len);
	static int getBinSize(int precision, int scale);
	static Decimal* double2Decimal(double from);
	double toDouble();

private:
	decimal_t m_dec;
};

}

#endif // _NTSE_DECIMAL_H_
