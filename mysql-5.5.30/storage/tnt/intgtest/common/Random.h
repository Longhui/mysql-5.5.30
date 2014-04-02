#ifndef _NTSETEST_RANDOM_H_
#define _NTSETEST_RANDOM_H_


#include <vector>
#include <string>
using namespace std;
/** ����������� */
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
 * ��ɢ����ֲ���
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
 * ����[0, count) ����Zipf�ֲ����������
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
	vector<double> m_probArray;/** �ۻ��������� */
	double m_topProb;	/** ��Ƶ��������ĸ��� */
	double m_top10CP;	/** ��Ƶ����10%����������ۻ����� */
};

#endif // _NTSETEST_RANDOM_H_
