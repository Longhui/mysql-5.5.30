#ifndef _NTSETEST_RANDOM_H_
#define _NTSETEST_RANDOM_H_


#include <vector>
#include <string>
using namespace std;
/** 随机数发生器 */
class RandomGen {
public:
	static void setSeed(unsigned int  seed);
	static int nextInt(int minValue, int maxValue);
	static int nextInt();
	static double nextDoubleExc();
	static double randNorm(double mean, double variance = 1.0);
	static int randNorm(int mean, int minValue);
	static int randNorm(int mean, int minValue, int maxValue);
	static bool tossCoin(unsigned int percent);
private:
	const static int m_randMax;
};

/**
 * 离散随机分布类
 */
class RandDist {
public:

	RandDist(unsigned dist[], unsigned cnt);
	~RandDist();
	unsigned select();

private:
	unsigned *m_dist;
	unsigned m_count;
};

/**
 * 产生[0, count) 满足Zipf分布的随机整数
 */
class ZipfRandom {
public:
	ZipfRandom(unsigned count, double screw = 1.2f, bool randShuffle = true);
	static ZipfRandom* createInstVaryCount(double screw, bool randShuffle = true
		, unsigned minCount = 16 * 1024, unsigned maxCount = 8 * 1024 * 1024
		, double percent = 0.1, double prob = 0.9);
	static ZipfRandom* createInstVaryScrew(unsigned count, bool randShuffle = true
		, double minScrew = 0.5, double maxScrew = 2.0
		, double percent = 0.1, double prob = 0.9);
	unsigned getNumberOfElements();
	double getScrew();
	unsigned rand();
	double topProbability();
	double top10Probability();
	double probability(unsigned x);
	double cumulativeProbability(unsigned x);
	string toString();

private:
	unsigned int m_count;
	double m_screw;
	vector<double> m_probArray;/** 累积概率数组 */
	double m_topProb;	/** 最频繁随机数的概率 */
	double m_top10CP;	/** 最频繁的10%个随机数的累积概率 */
};

#endif // _NTSETEST_RANDOM_H_
