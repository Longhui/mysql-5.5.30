#include "UtilTest.h"
#include "Random.h"
#include <iostream>
#include <assert.h>
#include "util/System.h"

using namespace ntse;
using namespace std;

void UtilTestCase::testRandDist() {
	{
		unsigned prob[] = { 0, 0, 100 };
		RandDist dist(prob, sizeof(prob) / sizeof(prob[0]));
		for (unsigned i = 0; i < 1000; ++i)
			CPPUNIT_ASSERT(2 == dist.select());
	}

	{
		unsigned prob[] = { 100, 0, 0 };
		RandDist dist(prob, sizeof(prob) / sizeof(prob[0]));
		for (unsigned i = 0; i < 1000; ++i)
			CPPUNIT_ASSERT(0 == dist.select());
	}

	{
		unsigned prob[] = { 0, 100, 0 };
		RandDist dist(prob, sizeof(prob) / sizeof(prob[0]));
		for (unsigned i = 0; i < 1000; ++i)
			CPPUNIT_ASSERT(1 == dist.select());
	}

	{
		unsigned loop = 10000;
		unsigned count[] = {0, 0, 0};
		unsigned prob[] = { 25, 25, 50};
		RandDist dist(prob, sizeof(prob) / sizeof(prob[0]));
		for (unsigned i = 0; i < loop; ++i) {
			count[dist.select()] ++;
		}
		cout << endl;
		for (unsigned i = 0; i < sizeof(count) / sizeof(count[0]); ++i)
			cout << (double)prob[i] / 100 << "/" << (double)count[i] / loop << ", ";
		cout << endl;
	}
}

void UtilTestCase::testZipfRand() {
	unsigned count = 4000000;
	ZipfRandom zipf(4000000);
	int loop = 1000000;
	cout << endl << "Count " << zipf.getNumberOfElements() << " screw " << zipf.getScrew()
		<< " top prob " << zipf.topProbability() << " top10percent prob "<< zipf.top10Probability();
	u64 before = System::microTime();
	for (int i = 0; i < loop; ++i) {
		unsigned v = zipf.rand();
		assert( v >=0 && v < count);
	}
	u64 after = System::microTime();
	cout << endl << "rand func cost(us) " << (after - before) / loop << endl;
}


void UtilTestCase::testZipfParams() {
	
	cout << endl << "---- screw -----";
	for (double screw = 0.9; screw < 2; screw += 0.1) {
		ZipfRandom zipf(100000, screw);
		cout << endl << zipf.toString();
	}
	cout << endl << "---- count -----";
	for (unsigned count = 128; count < 1024 * 1024 * 32; count *= 2) {
		ZipfRandom zipf(count);
		cout << endl << zipf.toString();
	}
}


void UtilTestCase::testZipfCreate() {
	ZipfRandom* zipf = ZipfRandom::createInstVaryCount(1.1);
	if (zipf) {
		cout << endl << zipf->toString();
	} else {
		CPPUNIT_ASSERT(false);
	}

	zipf = ZipfRandom::createInstVaryScrew(100000);
	if (zipf) {
		cout << endl << zipf->toString();
	} else {
		CPPUNIT_ASSERT(false);
	}
}

