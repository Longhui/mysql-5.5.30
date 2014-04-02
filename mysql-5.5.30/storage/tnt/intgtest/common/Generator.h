/**
 * 规律数据生成
 *
 * @author 谢可(xieke@corp.netease.com, ken@163.org)
 */
#ifndef _NTSETEST_GENERATOR_H_
#define _NTSETEST_GENERATOR_H_




/** 生成数据的工具函数 */
#include "util/System.h"
#include <algorithm>
#include <assert.h>

namespace ntseperf {
enum Scale {
	SMALL,
	MEDIUM,
	BIG
};

enum TestTable {
	COUNT_TABLE,
	LONGCHAR_TABLE,
	ACCOUNT_TABLE,
	BLOG_TABLE
};

/**	顺序定义 **/
enum Order {
	ASCENDANT,
	DESCENDANT,
	RANDOM,

	MAX // 无用，只是用来标识最大值
};

/** 常用数据生成 **/
class Generator {
public:

	/**
	 * 生成数组序列
	 * 
	 * @param array OUT  生成的数组序列
	 *
	 */
	template <class T>
	static void generateArray(T array[], int count, T base, T step, Order order) {
		assert(order == ASCENDANT || order == DESCENDANT || order == RANDOM);
		T posiStep = (step >= 0) ? step : 0 - step;
		T baseMin = (step >= 0) ? base : (base + step * (count - 1));
		T bottom = (order == DESCENDANT) ? baseMin + posiStep * (count - 1) : baseMin;
		T incStep = (order == DESCENDANT) ? 0 - posiStep : posiStep;
		for (int i = 0; i < count; ++i) {
			array[i] = bottom + i * incStep;
		}
		if (order == RANDOM)
			random_shuffle(&array[0], &array[count - 1]);
	}

};


}



#endif //_NTSETEST_GENERATOR_H_

