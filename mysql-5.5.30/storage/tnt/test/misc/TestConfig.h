/**
 * ²âÊÔTNTDatabase backup²Ù×÷
 *
 * @author xindingfeng
 */
#ifndef _NTSETEST_CONFIG_TNTCONFIG_
#define _NTSETEST_CONFIG_TNTCONFIG_

#include <cppunit/extensions/HelperMacros.h>
#include "misc/Config.h"
#include "misc/TNTControlFile.h"

using namespace tnt;
using namespace ntse;

class ConfigTestCase : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE(ConfigTestCase);
	CPPUNIT_TEST(testWriteAndRead);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testWriteAndRead();
};

class TNTConfigTestCase : public CPPUNIT_NS::TestFixture
{
	CPPUNIT_TEST_SUITE(TNTConfigTestCase);
	CPPUNIT_TEST(testWriteAndRead);
	CPPUNIT_TEST_SUITE_END();
public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig();
	void setUp();
	void tearDown();
protected:
	void testWriteAndRead();
};
#endif
