/**
 * ���Կ����ļ�������
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSETEST_CTRLFILE_H_
#define _NTSETEST_CTRLFILE_H_

#include <cppunit/extensions/HelperMacros.h>
#include "misc/ControlFile.h"

using namespace ntse;

class CtrlFileTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(CtrlFileTestCase);
	CPPUNIT_TEST(testBasic);
	CPPUNIT_TEST(testCreateTable);
	//CPPUNIT_TEST(testCrazyUpdate);	// ������Ժ�����ֻ�ڻ��ɿ����ļ�����������ʱ���ڲ���
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();

protected:
	void testBasic();
	void testCreateTable();
	void testCrazyUpdate();

private:
	ControlFile	*m_ctrlFile;
};

#endif
