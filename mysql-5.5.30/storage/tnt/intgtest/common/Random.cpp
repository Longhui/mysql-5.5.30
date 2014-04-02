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
 * ����[minValue, maxValue)֮��������
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

/** [0, 1)֮�������� */
double RandomGen:: nextDoubleExc() {
	return (double)nextInt() / (m_randMax + 1.0);
}

/**
 * ��̬�ֲ�
 * @param mean ƽ��ֵ
 * @param variance ����
 */
double RandomGen::randNorm(double mean, double variance) {
	// Return a real number from a normal (Gaussian) distribution with given
	// mean and variance by Box-Muller method
	
	// (0, 1)֮��������
	double r1 = ((double)nextInt() + 0.5) / (m_randMax + 1.0);
	// [0, 1)֮��������
	double r2 = nextDoubleExc();
	double r = sqrt( -2.0 * log( 1.0-r1) ) * variance;
	double phi = 2.0 * 3.14159265358979323846264338328 * r2;
	return mean + r * cos(phi);
}

/**
 * ��̬�ֲ�����
 * @param mean ƽ��ֵ
 * @param minValue ��Сֵ���������������[minValue, 2 * mean - minValue]֮��
 */
int RandomGen::randNorm(int mean, int minValue) {
	assert(mean > minValue);
	// ��׼������Ϊ(mean - minValue) / 3
	// ���ֵ����[minValue, 2 * minValue - mean]�ĸ��ʵ���1%
	int v = (int)randNorm((double)mean, (double)(mean - minValue) / 3);
	v = max(minValue, v);
	int maxValue = (mean << 1) - minValue;
	v = min(maxValue, v);
	return v;
}
/**
 * ��̬�ֲ�����
 * @param mean ƽ��ֵ
 * @param minValue ��Сֵ
 * @param maxValue ���ֵ
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
 * ��Ӳ��
 * ����Ϊtrue������Ϊfalse
 * @param percent ����true�İٷֱ�, [0, 100]
 * @return ��percent / 100�ĸ��ʷ���true
 */
bool RandomGen::tossCoin(unsigned int percent) {
	assert(percent >= 0 && percent <= 100);
	return  nextInt((int)0, (int)1600) < ((int)percent << 4);
}
//////////////////////////////////////////////////////////////////////////
//// RandDist
//////////////////////////////////////////////////////////////////////////
/**
 * ����һ����ɢ����ֲ�
 * @param dist ����ֲ����飬Ԫ�ش���ٷֱ�, ��Ϊ100
 * @param cnt ����Ԫ�ظ���
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
 * ���ݸ��ʷֲ����ѡ��
 * @return ��ѡ���Ԫ���±�
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
 * @param count ������������������������Χ��[0, count -1)֮��
 * @param screw zipfָ��
 * @param randShuffle randShuffle == falseʱ�� ��0��count - 1 ������ʵݼ���
 */
ZipfRandom::ZipfRandom(unsigned int count, double screw, bool randShuffle)
	: m_count(count), m_probArray(count), m_screw(screw) {
	
	double sum = 0;
	unsigned n = 1;

	
	/* ��������ܶ� */
	for (n = 0; n <= m_count - 1; n++) {
		m_probArray[n] = 1.0 / pow(n + 1, screw);
		sum += m_probArray[n];
	}
	for (n = 0; n <= m_count - 1; n++)
		m_probArray[n] /= sum;

	m_topProb = m_probArray[0];
	/* ����ǰ10%��¼�ķ��ʸ��� */
	m_top10CP = 0.0;
	for (n = 0; n <= m_count / 10; n++)
		m_top10CP += m_probArray[n];

	/* ������� */
	if (randShuffle)
		random_shuffle(m_probArray.begin(), m_probArray.end());

	/** �����ۻ����ʷֲ� */
	sum = 0;
	for (n = 0; n <= m_count - 1; n++) {
		sum += m_probArray[n];
		m_probArray[n] = sum;
	}

	assert(m_probArray[m_count - 1] + 0.00001 > 1.0);
}

/**
 * ͨ����������������� ����һ��Zipf�����ʵ��, ��Ƶ����percent * 100 %��������ۻ�����Ϊprob��
 * @param screw zipfָ��
 * @param randShuffle randShuffle == falseʱ�� ��0��count - 1 ������ʵݼ���
 * @param minCount ��СCount
 * @param maxCount ���Count
 * @param percent ָ���ٷֱ�
 * @param prob  ��Ƶ����percent * 100 %��������ۻ�����
 * @return Zipf�����������
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
 * ͨ������ָ��������һ��Zipf�����ʵ��, ��Ƶ����percent * 100 %��������ۻ�����Ϊprob��
 * @param count ���������
 * @param randShuffle randShuffle == falseʱ�� ��0��count - 1 ������ʵݼ���
 * @param minScrew ��Сָ��
 * @param maxScrew ���ָ��
 * @param percent ָ���ٷֱ�
 * @param prob  ��Ƶ����percent * 100 %��������ۻ�����
 * @return Zipf�����������
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
 * ���������
 */
unsigned ZipfRandom::rand() {
	// [0, 1)֮��������
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

