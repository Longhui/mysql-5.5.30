/**
* 记录压缩操作相关测试
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/

#ifndef _NTSETEST_COMPRESS_RECORD_H_
#define _NTSETEST_COMPRESS_RECORD_H_

#include <cppunit/extensions/HelperMacros.h>
#include "Test.h"
#include "misc/Config.h"
#include "api/Database.h"
#include "misc/TableDef.h"
#include "misc/TestRecord.h"

using namespace ntse;

/**
 * 只用于测试的压缩解压缩提取器
 */
class CompressExtractorForTest : public RowCompressMng {
public:
	CompressExtractorForTest(const TableDef *tableDef) : RowCompressMng(NULL, tableDef, NULL) {
		m_dicFile = new File("./testDict.ndic");
		m_dicFile->remove();
		u64 errorCode = m_dicFile->create(false, false);
		if (File::getNtseError(errorCode) != File::E_NO_ERROR) {
			cout << endl << "Failed to create file " << m_dicFile->getPath() << 
				", Error message: " << File::explainErrno(errorCode) << endl;
			NTSE_ASSERT(false);
		}
		m_dictionary = new RCDictionary(tableDef->m_id, m_dicFile, 32);

		const char *item1 = "beijing2008";
		m_dictionary->setDicItem(0, (byte *)item1, strlen(item1));
		const char *item2 = "google";
		m_dictionary->setDicItem(1, (byte *)item2, strlen(item2));
		const char *item3 = "huanhuan";
		m_dictionary->setDicItem(2, (byte *)item3, strlen(item3));
		//const char *item4 = "";
		//m_dictionary->setDicItem(3, item4, strlen(item4));
		try {
			m_dictionary->buildDat();
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
			NTSE_ASSERT(false);
		}
	}
	~CompressExtractorForTest() {
		m_dictionary->close();
		delete m_dictionary;
		File file("./testDict.ndic");
		NTSE_ASSERT(File::E_NO_ERROR == File::getNtseError(file.remove()));
	}
	/*
	void compressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) {
		memcpy(dest, src + offset, len);
		*destSize = len;
	}
	void decompressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) {
		memcpy(dest, src + offset, len);
		*destSize = len;
	}
	*/
private:
	File *m_dicFile;
};


/** 记录压缩操作类测试用例 */
class RecordCompressTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RecordCompressTestCase);
	CPPUNIT_TEST(testConvRecordVarToCO);
	CPPUNIT_TEST(testConvRecordRedToCO);
	CPPUNIT_TEST(testConvRcdCOToReal);
	CPPUNIT_TEST(testExtractSubCompressR);
	CPPUNIT_TEST(testUpdateRecordWithDict);
	CPPUNIT_TEST(testUpdateCompressRecord);
	CPPUNIT_TEST(testUpdateUncompressRecord);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig() { return false; }
	void setUp();
	void tearDown();

protected:
	void testConvRecordVarToCO();
	void testConvRecordRedToCO();
	void testConvRcdCOToReal();
	void testExtractSubCompressR();
	void testUpdateRecordWithDict();
	void testUpdateCompressRecord();
	void testUpdateUncompressRecord();

private:
	void doUpdateCompressedRecord(bool needCompress, bool compressAble);
	void doUpdateUncompressedRecord(bool needCompress, bool compressAble);

private:
	StudentTable          *m_studentTable;
	CompressExtractorForTest *m_compressExtractor;
};

#endif