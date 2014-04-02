#include "Random.h"
#include "util/System.h"
#include <stdlib.h>
#include <algorithm>
#include <assert.h>
#include <sstream>
#include <iostream>
#include <math.h>

using namespace ntse;
using namespace std;


#ifdef WIN32
const int RandomGen::m_randMax = RAND_MAX * (RAND_MAX + 1);
#else
const int RandomGen::m_randMax = RAND_MAX;
#endif

void RandomGen::setSeed(unsigned int seed) {
	System::srandom(seed);
}

/**
 * 生成[minValue, maxValue)之间的随机数
 */
int RandomGen::nextInt(int minValue, int maxValue) {
	assert(minValue <= maxValue);
	return minValue + (int) ((maxValue - minValue) * ((double)nextInt() / (m_randMax + 1.0)));

}

int RandomGen::nextInt() {
#ifdef WIN32
	return System::random() * RAND_MAX + System::random();
#else
	return System::random();
#endif
}

/** [0, 1)之间的随机数 */
double RandomGen:: nextDoubleExc() {
	return (double)nextInt() / (m_randMax + 1.0);
}

/**
 * 正态分布
 * @param mean 平均值
 * @param variance 方差
 */
double RandomGen::randNorm(double mean, double variance) {
	// Return a real number from a normal (Gaussian) distribution with given
	// mean and variance by Box-Muller method
	
	// (0, 1)之间的随机数
	double r1 = ((double)nextInt() + 0.5) / (m_randMax + 1.0);
	// [0, 1)之间的随机数
	double r2 = nextDoubleExc();
	double r = sqrt( -2.0 * log( 1.0-r1) ) * variance;
	double phi = 2.0 * 3.14159265358979323846264338328 * r2;
	return mean + r * cos(phi);
}

/**
 * 正态分布整数
 * @param mean 平均值
 * @param minValue 最小值，产生的随机数在[minValue, 2 * mean - minValue]之间
 */
int RandomGen::randNorm(int mean, int minValue) {
	assert(mean > minValue);
	// 标准差设置为(mean - minValue) / 3
	// 随机值超出[minValue, 2 * minValue - mean]的概率低于1%
	int v = (int)randNorm((double)mean, (double)(mean - minValue) / 3);
	v = max(minValue, v);
	int maxValue = (mean << 1) - minValue;
	v = min(maxValue, v);
	return v;
}
/**
 * 正态分布整数
 * @param mean 平均值
 * @param minValue 最小值
 * @param maxValue 最大值
 */
int RandomGen::randNorm(int mean, int minValue, int maxValue) {
	assert(minValue < mean);
	assert(mean < maxValue);
	double variance = (double)min(mean - minValue , maxValue - mean) / 2;

	int v = (int)randNorm((double)mean, variance);
	if (v >= minValue && v <= maxValue)
		return v;
	v = (int)randNorm((double)mean, variance);
	if (v < minValue)
		return minValue;
	if (v > maxValue)
		return maxValue;
	return v;
}
/**
 * 抛硬币
 * 正面为true，反面为false
 * @param percent 返回true的百分比, [0, 100]
 * @return 以percent / 100的概率返回true
 */
bool RandomGen::tossCoin(unsigned int percent) {
	assert(percent >= 0 && percent <= 100);
	return  nextInt((int)0, (int)1600) < ((int)percent << 4);
}
//////////////////////////////////////////////////////////////////////////
//// RandDist
//////////////////////////////////////////////////////////////////////////
/**
 * 构造一个离散随机分布
 * @param dist 随机分布数组，元素代表百分比, 和为100
 * @param cnt 数组元素个数
 */
RandDist::RandDist(unsigned dist[], unsigned cnt) {
	m_dist = new unsigned[cnt];
	m_count = cnt;
	unsigned total = 0;
	for (unsigned i = 0; i < m_count; ++i) {
		total += dist[i];
		m_dist[i] = total;
	}
	assert(total == 100);
}

RandDist::~RandDist() {
	delete [] m_dist;
}
/**
 * 根据概率分布随机选择
 * @return 已选择的元素下标
 */
unsigned RandDist::select() {
	unsigned v = RandomGen::nextInt((int)0, (int)100);
	unsigned i = 0;
	while(v >= m_dist[i++])
		;
	assert(i - 1 < m_count);
	return i - 1;
}

//////////////////////////////////////////////////////////////////////////
//// Zip Random Number Generator
//////////////////////////////////////////////////////////////////////////
/**
 * @param count 随机数个数，产生的随机数范围在[0, count -1)之间
 * @param screw zipf指数
 * @param randShuffle randShuffle == false时， 从0到count - 1 随机概率递减；
 */
ZipfRandom::ZipfRandom(unsigned int count, double screw, bool randShuffle)
	: m_count(count), m_probArray(count), m_screw(screw) {
	
	double sum = 0;
	unsigned n = 1;

	
	/* 计算概率密度 */
	for (n = 0; n <= m_count - 1; n++) {
		m_probArray[n] = 1.0 / pow(n + 1, screw);
		sum += m_probArray[n];
	}
	for (n = 0; n <= m_count - 1; n++)
		m_probArray[n] /= sum;

	m_topProb = m_probArray[0];
	/* 计算前10%记录的访问概率 */
	m_top10CP = 0.0;
	for (n = 0; n <= m_count / 10; n++)
		m_top10CP += m_probArray[n];

	/* 随机交换 */
	if (randShuffle)
		random_shuffle(m_probArray.begin(), m_probArray.end());

	/** 计算累积概率分布 */
	sum = 0;
	for (n = 0; n <= m_count - 1; n++) {
		sum += m_probArray[n];
		m_probArray[n] = sum;
	}

	assert(m_probArray[m_count - 1] + 0.00001 > 1.0);
}

/**
 * 通过调整随机数个数， 创建一个Zipf随机数实例, 最频繁的percent * 100 %个随机数累积概率为prob。
 * @param screw zipf指数
 * @param randShuffle randShuffle == false时， 从0到count - 1 随机概率递减；
 * @param minCount 最小Count
 * @param maxCount 最大Count
 * @param percent 指定百分比
 * @param prob  最频繁的percent * 100 %个随机数累积概率
 * @return Zipf随机数发生器
 */
ZipfRandom* ZipfRandom::createInstVaryCount(double screw, bool randShuffle, unsigned minCount
						, unsigned maxCount, double percent, double prob) {
	assert(percent <= 0.5 && percent >= 0.01);
	assert(prob >= 0.5 && prob <= 1.0);

	double eps = 0.005;
	while(minCount <= maxCount) {
		unsigned count = (minCount + maxCount) / 2;
		ZipfRandom zipf(count, screw, false);
		double v = zipf.cumulativeProbability((unsigned)(count * percent));
		// cout << endl << zipf.toString();
		if (v > prob + eps) {
			maxCount = count - 1;
		} else if (v < prob - eps) {
			minCount = count + 1;
		} else {
			return new ZipfRandom(count, screw, randShuffle);
		}
	}

	return 0;
}

/**
 * 通过调整指数，创建一个Zipf随机数实例, 最频繁的percent * 100 %个随机数累积概率为prob。
 * @param count 随机数个数
 * @param randShuffle randShuffle == false时， 从0到count - 1 随机概率递减；
 * @param minScrew 最小指数
 * @param maxScrew 最大指数
 * @param percent 指定百分比
 * @param prob  最频繁的percent * 100 %个随机数累积概率
 * @return Zipf随机数发生器
 */
ZipfRandom* ZipfRandom::createInstVaryScrew(unsigned count, bool randShuffle
							  , double minScrew, double maxScrew
							  , double percent, double prob) {
	assert(percent <= 0.5 && percent >= 0.01);
	assert(prob >= 0.5 && prob <= 1.0);

	double eps = 0.005;
	while(minScrew <= maxScrew) {
		double screw = (minScrew + maxScrew) / 2;
		ZipfRandom zipf(count, screw, false);
		double v = zipf.cumulativeProbability((unsigned)(count * percent));
		// cout << endl << zipf.toString();
		if (v > prob + eps) {
			maxScrew = screw - 0.01;
		} else if ( v < prob - eps) {
			minScrew = screw + 0.01;
		} else {
			return new ZipfRandom(count, screw, randShuffle);
		}
	}

	return 0;
}

/**
 * 产生随机数
 */
unsigned ZipfRandom::rand() {
	// [0, 1)之间的随机数
	double v = RandomGen::nextDoubleExc();

	int low = 0;
	int high = (int)(m_count - 1);
	while(low <= high) {
		int n = (high + low) / 2;
		if (m_probArray[n] < v) {
			low = n + 1;
		} else if (m_probArray[n] > v) {
			high = n - 1;
		} else {
			return n;
		}
	}
	if (high < 0) {
		assert(high == -1);
		return 0;
	} else {
		assert(m_probArray[high] <= v);
		assert(m_probArray[high + 1] > v);
		return high;
	}
}


double ZipfRandom::getScrew() {
	return m_screw;
}

unsigned ZipfRandom::getNumberOfElements() {
	return m_count;
}

double ZipfRandom::cumulativeProbability(unsigned x) {
	assert(x < m_count - 1);
	return m_probArray[x];
}

double ZipfRandom::top10Probability() {
	return m_top10CP;
}

double ZipfRandom::topProbability() {
	return m_topProb;
}

string ZipfRandom::toString() {
	stringstream ss;
	ss << "Count " << getNumberOfElements() << " screw " << getScrew()
		<< " top prob " << topProbability() << " top10 percent prob "<< top10Probability();
	return ss.str();
}

