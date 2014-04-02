/**
* ²âÊÔProfileÐÔÄÜËðºÄ
*
* @author ÅËÄþ(panning@corp.netease.com, panning@163.org)
*/
#ifdef NTSE_PROFILE

#include "misc/Profile.h"
#include "misc/TestProfile.h"
#include "util/System.h"

using namespace std;
using namespace ntse;

const char* ProfileTestCase::getName() {
	return "Profile test";
}

const char* ProfileTestCase::getDescription() {
	return "Test Profile performance.";
}

bool ProfileTestCase::isBig() {
	return false;
}


void TestedClass::doA() {
	PROFILE(TestedClass_doA);
	for (int i = 0; i < 6; i++) {
		doD();
	}
}
void TestedClass::doB() {
	PROFILE(TestedClass_doB);
	int j = 0;
	for (int i = 0; i < 3; i++) {
		j++;
		doA();
	}
}
void TestedClass::doC() {
	PROFILE(TestedClass_doC);
	for (int i = 0; i < 5; i++) {
		doD();
	}
}
void TestedClass::doD() {
	PROFILE(TestedClass_doD);
	haveProfilePoint();
}

ThreadTestedClassA::ThreadTestedClassA() : Thread("TreadTestedClassA"){}
ThreadTestedClassA::~ThreadTestedClassA(){
	g_tlsProfileInfo.endProfile();
}
void ThreadTestedClassA::openProfile() {
	g_tlsProfileInfo.prepareProfile(1000001, BG_THREAD, true);
}
	
ThreadTestedClassB::ThreadTestedClassB() : Thread("ThreadTestedClassB"){}
ThreadTestedClassB::~ThreadTestedClassB() {
	g_tlsProfileInfo.endProfile();
}
void ThreadTestedClassB::openProfile() {
	g_tlsProfileInfo.prepareProfile(1000002, BG_THREAD, true);
}
ConnTestedClassC::ConnTestedClassC() : Thread("ConnTestedClassC"){}
ConnTestedClassC::~ConnTestedClassC() {
	g_tlsProfileInfo.endProfile();
}
void ConnTestedClassC::openProfile() {
	g_tlsProfileInfo.prepareProfile(1000003, CONN_THREAD, true);
}
ConnTestedClassD::ConnTestedClassD() : Thread("ConnTestedClassD"){}
ConnTestedClassD::~ConnTestedClassD() {
	g_tlsProfileInfo.endProfile();
}
void ConnTestedClassD::openProfile() {
	g_tlsProfileInfo.prepareProfile(1000004, CONN_THREAD, true);
}

ThreadRunner::ThreadRunner() {
	m_threadA = new ThreadTestedClassA();
	m_threadB = new ThreadTestedClassB();
	m_threadC = new ConnTestedClassC();
	m_threadD = new ConnTestedClassD();
	m_threadA->start();
	m_threadB->start();
	m_threadD->start();
	m_threadC->start();
}
ThreadRunner::~ThreadRunner() {
	m_threadA->join();
	m_threadB->join();
	m_threadC->join();
	m_threadD->join();
	delete m_threadA;
	delete m_threadB;
	delete m_threadC;
	delete m_threadD;
}

void TestedClass::haveProfilePoint() {
	PROFILE(TestedClass_haveProfilePoint);
}

void ProfileTestCase::testProfile() {
	cout << endl;
	TestedClass tested;
	
	{
		u64 before = System::clockCycles();
		for (int i = 0; i < 70000; ++i) {
			tested.haveProfilePoint();
		}
		u64 after = System::clockCycles();
		cout << "Has profile point but no opened use time: " << (after - before) / 70000 << endl;
		cout<<"-----------------------"<< endl;
	}
	
	g_profiler.openGlobalProfile();
	{
		u64 before = System::clockCycles();
		for (int i = 0; i < 70000; ++i){
			tested.haveProfilePoint();
		}
		u64 after = System::clockCycles();
		cout << "Has profile point and just open global profile use time: " << (after - before) / 70000 << endl;
	}
	g_profiler.shutdownGlobalClean();
	
	g_tlsProfileInfo.prepareProfile(1000001, BG_THREAD, true);
	{
		u64 before = System::clockCycles();
		for (int i = 0; i < 70000; ++i) {
			tested.haveProfilePoint();
		}
		u64 after = System::clockCycles();

		cout << "Has profile point and  just open thread profile use time: " << (after - before) / 70000 << endl;
	}

	g_profiler.openGlobalProfile();
	{
		u64 before = System::clockCycles();
		for (int i = 0; i < 70000; ++i) {
			tested.haveProfilePoint();
		}
		u64 after = System::clockCycles();

		cout << "Has profile point and open global and tread profile use time: " << (after - before) / 70000 << endl;
	}
}

void ProfileTestCase::testProfileConcurrency() {
	ThreadRunner runer;
	cout << "Thread Profile:" << endl;

	Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(BG_THREAD);
	for (size_t i = 0; i < thdProfInfoArray->getSize(); i++) {
		ThdProfileInfoRecord *profInfo = thdProfInfoArray->operator[](i);
		cout << "THD_ID:" << profInfo->getId() << endl;
		cout << "Caller:" << profInfo->getCaller() << endl;
		cout << "FuncName:" << profInfo->getFuncName() << endl;
		cout << "Count:"<<profInfo->getCount() << endl;
		cout << "Sum:"<<profInfo->getSumT() << endl;
		cout << "Average:"<<profInfo->getAvgT() << endl;
		cout << "Max:"<<profInfo->getMaxT() << endl;
		cout << "--------------------" << endl;
	}
	for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
		delete thdProfInfoArray->operator[](i);
	delete thdProfInfoArray;

	cout << "Connection Profile:" << endl;

	Array<ThdProfileInfoRecord *> *connProfInfoArray = g_profiler.getThdsProfileInfos(CONN_THREAD);
	for (size_t i = 0; i < connProfInfoArray->getSize(); i++) {
		ThdProfileInfoRecord *profInfo = connProfInfoArray->operator[](i);
		cout << "CONN_ID:"<< profInfo->getId() << endl;
		cout << "Caller:" << profInfo->getCaller() << endl;
		cout << "FuncName:" << profInfo->getFuncName() << endl;
		cout << "Count:"<< profInfo->getCount() << endl;
		cout << "Sum:"<< profInfo->getSumT() << endl;
		cout << "Average:"<< profInfo->getAvgT() << endl;
		cout << "Max:"<< profInfo->getMaxT() << endl;
		cout << "--------------------"<< endl;
	}

	for (size_t i = 0; i < connProfInfoArray->getSize(); i++)
		delete connProfInfoArray->operator[](i);
	delete connProfInfoArray;
}


ProfileControlTestThread::ProfileControlTestThread() : Thread("ProfileControlTestThread") {}
ProfileControlTestThread::~ProfileControlTestThread() {
	g_tlsProfileInfo.endProfile();
}

void ProfileControlTestThread::run() {
	g_tlsProfileInfo.prepareProfile(9,BG_THREAD,false);
	doSomething();
}
void ProfileControlTestThread::doSomething() {
	for ( int i = 0; i < 8; i++) {
		tested.doC();
		tested.doD();
	}
}

void ProfileTestCase::testProfileControl() {
	ProfileControlTestThread *threadForControl = new ProfileControlTestThread();
	threadForControl->run();
	{
		cout << endl << "just prepare profile for ProfileControlTestThread, profile no opened" << endl;
		Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(BG_THREAD);
		cout << "get profile records number£º" << thdProfInfoArray->getSize() << endl;
		cout << "--------------------" << endl;
		for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
			delete thdProfInfoArray->operator[](i);
		delete thdProfInfoArray;
	}

	{
		g_profiler.control(9,BG_THREAD,OpenIt);
		threadForControl->run();
		cout << "open profile for ProfileControlTestThread" << endl;
		Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(BG_THREAD);
		cout << "get profile records number£º" << thdProfInfoArray->getSize() << endl;
		cout << "--------------------" << endl;
		for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
			delete thdProfInfoArray->operator[](i);
		delete thdProfInfoArray;
	}

	{
		g_profiler.control(9,BG_THREAD,Shutdown_Keep);
		threadForControl->run();
		cout << "shutdown  profile but keep profile info record for ProfileControlTestThread" << endl;
		Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(BG_THREAD);
		cout << "get profile records number£º" << thdProfInfoArray->getSize() << endl;
		cout << "--------------------" << endl;
		for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
			delete thdProfInfoArray->operator[](i);
		delete thdProfInfoArray;
	}

	{
		g_profiler.control(9,BG_THREAD,Shutdown_Clean);
		threadForControl->run();
		cout << "shutdown  profile also clean profile info record for ProfileControlTestThread" << endl;
		Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(BG_THREAD);
		cout << "get profile records number£º" << thdProfInfoArray->getSize() << endl;
		cout << "--------------------" << endl;
		for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
			delete thdProfInfoArray->operator[](i);
		delete thdProfInfoArray;
	}
	
	{
		g_profiler.control(9,BG_THREAD,OpenIt);
		threadForControl->run();
		cout << "open  profile again  for ProfileControlTestThread" << endl;
		Array<ThdProfileInfoRecord *> *thdProfInfoArray = g_profiler.getThdsProfileInfos(BG_THREAD);
		cout << "get profile records number£º" << thdProfInfoArray->getSize() << endl;
		cout << "--------------------" << endl;
		for (size_t i = 0; i < thdProfInfoArray->getSize(); i++)
			delete thdProfInfoArray->operator[](i);
		delete thdProfInfoArray;
	}

	threadForControl->join();
	delete threadForControl;
}

#endif
