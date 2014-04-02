#include "util/TestSystem.h"
#include <iostream>
#include "util/System.h"

using namespace ntse;
using namespace std;

const char* SystemTestCase::getName() {
	return "System facilities test";
}

const char* SystemTestCase::getDescription() {
	return "Function test of various system spesific facilities";
}

bool SystemTestCase::isBig() {
	return false;
}

const char* SystemBigTest::getName() {
	return "System facilities performance test";
}

const char* SystemBigTest::getDescription() {
	return "Performance test of various system spesific facilities";
}

bool SystemBigTest::isBig() {
	return false;
}

/** 测试各种计时机制性能
 */
void SystemBigTest::testTiming() {
	int repeats = 1000000;

	u64 sum = 0;
	u64 before = System::microTime();
	for (int i = 0; i < repeats; i++) {
		sum += System::fastTime();
	}
	u64 after = System::microTime();
	cout << "fastTime(): " << (after - before) * 1000 / repeats << " ns" << endl;

	before = System::microTime();
	for (int i = 0; i < repeats; i++) {
		sum += System::microTime();
	}
	after = System::microTime();
	cout << "microTime(): " << (after - before) * 1000 / repeats << " ns" << endl;

	before = System::microTime();
	for (int i = 0; i < repeats; i++) {
		sum += System::currentTimeMillis();
	}
	after = System::microTime();
	cout << "currentTimeMillis(): " << (after - before) * 1000 / repeats << " ns" << endl;

	before = System::microTime();
	for (int i = 0; i < repeats; i++) {
		sum += System::clockCycles();
	}
	after = System::microTime();
	cout << "clockCycles(): " << (after - before) * 1000 / repeats << " ns" << endl;

	cout << "the garbage sum is: " << sum << endl; 
}
