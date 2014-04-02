/**
 * ������������
 *
 * @author л��(xieke@corp.netease.com, ken@163.org)
 */
#ifndef _NTSETEST_GENERATOR_H_
#define _NTSETEST_GENERATOR_H_




/** �������ݵĹ��ߺ��� */
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

/**	˳���� **/
enum Order {
	ASCENDANT,
	DESCENDANT,
	RANDOM,

	MAX // ���ã�ֻ��������ʶ���ֵ
};

/** ������������ **/
class Generator {
public:

	/**
	 * ������������
	 * 
	 * @param array OUT  ���ɵ���������
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

