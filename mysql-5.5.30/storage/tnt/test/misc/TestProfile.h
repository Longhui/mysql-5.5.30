/**
* ≤‚ ‘Profile–‘ƒ‹
* 
* @author ≈Àƒ˛(panning@corp.netease.com, panning@163.org)
*/

#ifdef NTSE_PROFILE

#ifndef _NTSETEST_PROFILE_H_
#define _NTSETEST_PROFILE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "util/Thread.h"

using namespace ntse;

class TestedClass {
public:
	TestedClass(){}
	~TestedClass(){}
	void haveProfilePoint();

	void doA();
	void doB();
	void doC();
	void doD();
};

class ThreadTestedClassA : public ntse::Thread{
public:
	ThreadTestedClassA();
	~ThreadTestedClassA();
	void openProfile();
	void run() {
		openProfile();
		tested.doA();
		tested.doB();
	}
	TestedClass tested;
};
class ThreadTestedClassB : public ntse::Thread{
public:
	ThreadTestedClassB();
	~ThreadTestedClassB();
	void openProfile();
	void run() {
		openProfile();
		tested.doC();
		tested.doD();
	}
	TestedClass tested;
};
class ConnTestedClassC : public ntse::Thread{
public:
	ConnTestedClassC();
	~ConnTestedClassC();
	void openProfile();
	void run() {
		openProfile();
		tested.doA();
		tested.doB();
	}
	TestedClass tested;
};
class ConnTestedClassD : public ntse::Thread{
public:
	ConnTestedClassD();
	~ConnTestedClassD();
	void openProfile();
	void run() {
		openProfile();
		tested.doC();
		tested.doD();
	}
	TestedClass tested;
};

class ProfileControlTestThread : public ntse::Thread{
public:
	ProfileControlTestThread();
	~ProfileControlTestThread();
	void doSomething();
	void run();
	TestedClass tested;
};

class ThreadRunner{
public:
	ThreadRunner();
	~ThreadRunner();
private:
	ThreadTestedClassA *m_threadA;
	ThreadTestedClassB *m_threadB;
	ConnTestedClassC *m_threadC;
	ConnTestedClassD *m_threadD;
};

class ProfileTestCase : public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(ProfileTestCase);
	CPPUNIT_TEST(testProfile);
	CPPUNIT_TEST(testProfileConcurrency);
	CPPUNIT_TEST(testProfileControl);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();

protected:
	void testProfile();
	void testProfileConcurrency();
	void testProfileControl();
};

#endif

#endif