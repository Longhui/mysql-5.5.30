/**
* 压缩表采样
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/
#include <cppunit/config/SourcePrefix.h>
#include "compress/TestRCMSmplTbl.h"

const char* RCMSmplTblTestCase::getName() {
	return "Sample table test for row compress";
}

const char* RCMSmplTblTestCase::getDescription() {
	return "Functional test for sample table.";
}

void RCMSmplTblTestCase::setUp() {
}

void RCMSmplTblTestCase::tearDown() {
}

void RCMSmplTblTestCase::init() {
	//create db
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));

	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession(
		"RCMSmplTblTestCase::init", m_conn);
}

void RCMSmplTblTestCase::cleanUp() {
	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		Database::drop(".");
		m_db = NULL;
	}
}

void RCMSmplTblTestCase::createTestTable(u64 tableSize) {
	assert(m_db != NULL);
	m_cprsTblBuilder = new CompressTableBuilder(m_db);
	m_cprsTblBuilder->createTable(tableSize);
}

void RCMSmplTblTestCase::dropTable() {
	m_cprsTblBuilder->dropTable();
	delete m_cprsTblBuilder;
	m_cprsTblBuilder = NULL;
}

void RCMSmplTblTestCase::testRCMSearchResultBuf() {
	init();

	RCMSearchResultBuf *result = new RCMSearchResultBuf(40, 256);
	CPPUNIT_ASSERT(result->empty());
	CPPUNIT_ASSERT(result->getMatchResultSize() == 0);
	CPPUNIT_ASSERT(result->getSkipLenBufSize() == 0);

	RCMSearchBuffer *searchBuf = new RCMSearchBuffer(256, 1, 40);
	RCMSearchBufUnit *bufData = new RCMSearchBufUnit[256];
	for (uint i = 0; i < 256; i++) {
		bufData[i].m_data = i;
		bufData[i].m_colGrpBoundary = false;
		bufData[i].m_neekSeek = false;
	}
	searchBuf->init(bufData, 256);

	for (uint i = 1; i <= 40; i++) {
		result->addMatch(searchBuf, 0, i);
		CPPUNIT_ASSERT(!result->empty());
		CPPUNIT_ASSERT(result->getMatchResultSize() == i);
		CPPUNIT_ASSERT(result->getSkipLenBufSize() == i);
		const byte *matchResultBuf = result->getMatchResult();
		for (uint j = 0; j < i; j++) {
			CPPUNIT_ASSERT(matchResultBuf[j] == bufData[j].m_data);
		}
	}
	uint index = 1;
	uint skipListSize = result->getSkipLenBufSize();
	for (uint i = 0; i < skipListSize; i++) {
		CPPUNIT_ASSERT(result->getSkipLen(i) == index);
		index++;
	}
	result->reset();
	CPPUNIT_ASSERT(result->empty());
	CPPUNIT_ASSERT(result->getMatchResultSize() == 0);
	CPPUNIT_ASSERT(result->getSkipLenBufSize() == 0);

	delete []bufData;
	delete searchBuf;
	delete result;

	cleanUp();
}

void RCMSmplTblTestCase::testRCMSearchBuf() {
	init();

	uint bufSize = (uint)System::random() % 102400;
	u16 min = System::random() % 50;
	u16 max = min + System::random() % 50;
	RCMSearchBuffer *searchBuf = new RCMSearchBuffer(bufSize, min, max);
	CPPUNIT_ASSERT(bufSize == searchBuf->size());
	CPPUNIT_ASSERT(searchBuf->minMatchLen() == min);
	CPPUNIT_ASSERT(searchBuf->maxMatchLen() == max);
	delete searchBuf;

	bufSize = 1 << 20;
	searchBuf = new RCMSearchBuffer(bufSize, 1, 40);
	RCMSearchBufUnit *bufData = new RCMSearchBufUnit[bufSize];
	for (uint i = 0; i < bufSize; i++) {
		bufData[i].m_data = i;
		bufData[i].m_colGrpBoundary = false;//System::random() & 0x01;
		bufData[i].m_neekSeek = false;//System::random() & 0x01;
	}

	searchBuf->init(bufData, bufSize);

	searchBuf->createWindow(256 * 1024, m_db->getConfig()->m_localConfigs.m_tblSmplWinDetectTimes,
		m_db->getConfig()->m_localConfigs.m_tblSmplWinMemLevel);

	for (uint i = 0; i < bufSize; i++) {
		CPPUNIT_ASSERT(bufData[i].m_data == searchBuf->getUnit(i).m_data);
		CPPUNIT_ASSERT(bufData[i].m_colGrpBoundary == searchBuf->getUnit(i).m_colGrpBoundary);
		CPPUNIT_ASSERT(bufData[i].m_neekSeek == searchBuf->getUnit(i).m_neekSeek);
	}

	delete [] bufData;
	delete searchBuf;

	cleanUp();
}

void RCMSmplTblTestCase::testSlidingWinHashTbl() {
	init();
	SlidingWinHashTbl *hashTbl = new SlidingWinHashTbl(1, 256);
	CPPUNIT_ASSERT(hashTbl->hashSize() == 1 << 8);
	for (uint i = 0; i < 1024; i++) {
		uint val = (uint)System::random();
		hashTbl->put(i, val);
		DList<uint> *slot = hashTbl->getSlot(i);
		CPPUNIT_ASSERT(slot != NULL);
		DLink<uint> *it = NULL;
		DLink<uint> *head = slot->getHeader();
		for (it = head; it->getNext() != head; it = it->getNext());
		CPPUNIT_ASSERT(it->get() == val);
		hashTbl->removeSlotHead(i, val);
	}
	delete hashTbl;
	cleanUp();
}

void RCMSmplTblTestCase::testSeqSmplTbl() {
	{
		//1、没有足够的采样记录
		init();
		createTestTable(1);

		m_conn->getLocalConfig()->setCompressSampleStrategy("sequence");

		RCMSampleTbl *sampleTableHdl = new RCMSampleTbl(m_session, m_db, getTableDef(), getRecords());
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		CPPUNIT_ASSERT(errCode == SMP_NO_ENOUGH_ROWS);

		delete sampleTableHdl;
		sampleTableHdl = NULL;
		dropTable();
		cleanUp();
	}
	{
		//2、有足够的采样记录
		init();
		createTestTable(10000);

		m_conn->getLocalConfig()->setCompressSampleStrategy("sequence");
		m_conn->getLocalConfig()->m_tblSmplPct = 80;

		RCMSampleTbl * sampleTableHdl = new RCMSampleTbl(m_session, m_db, getTableDef(), getRecords());
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		if (errCode == SMP_NO_ERR) {
			RCDictionary *newDic = NULL;
			EXCPT_OPER(newDic = sampleTableHdl->createDictionary(m_session));
			CPPUNIT_ASSERT(newDic != NULL);
			newDic->close();
			delete newDic;
			newDic = NULL;
		} else {
			char errMsg[256];
			sprintf(errMsg, "Error occured when sampled table! error code: %d", errCode);
			CPPUNIT_FAIL(errMsg);
		}

		delete sampleTableHdl;
		sampleTableHdl = NULL;

		dropTable();
		cleanUp();
	}
}

void RCMSmplTblTestCase::testPartedSmplTbl() {
	{
		//1、没有足够的采样记录
		init();
		createTestTable(1);

		m_conn->getLocalConfig()->setCompressSampleStrategy("parted");

		RCMSampleTbl *sampleTableHdl = new RCMSampleTbl(m_session, m_db, getTableDef(), getRecords(), NULL);
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		CPPUNIT_ASSERT(errCode == SMP_NO_ENOUGH_ROWS);

		delete sampleTableHdl;
		sampleTableHdl = NULL;
		dropTable();
		cleanUp();
	}
	{
		//2、有足够的采样记录
		init();
		createTestTable(10000);

		m_conn->getLocalConfig()->setCompressSampleStrategy("parted");
		m_conn->getLocalConfig()->m_tblSmplPct = 30;

		RCMSampleTbl * sampleTableHdl = new RCMSampleTbl(m_session, m_db, getTableDef(), getRecords(), NULL);
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		if (errCode == SMP_NO_ERR) {
			RCDictionary *newDic = NULL;
			EXCPT_OPER(newDic = sampleTableHdl->createDictionary(m_session));
			CPPUNIT_ASSERT(newDic != NULL);
			newDic->close();
			delete newDic;
			newDic = NULL;
		} else {
			char errMsg[256];
			sprintf(errMsg, "Error occured when sampled table! error code: %d", errCode);
			CPPUNIT_FAIL(errMsg);
		}

		delete sampleTableHdl;
		sampleTableHdl = NULL;

		dropTable();
		cleanUp();
	}
}

void RCMSmplTblTestCase::testDiscreteSmplTbl() {
	{
		//1、没有足够的采样记录
		init();
		createTestTable(1);

		m_conn->getLocalConfig()->setCompressSampleStrategy("discrete");

		RCMSampleTbl *sampleTableHdl = new RCMSampleTbl(m_session, m_db, getTableDef(), getRecords(), NULL);
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		CPPUNIT_ASSERT(errCode == SMP_NO_ENOUGH_ROWS);

		delete sampleTableHdl;
		sampleTableHdl = NULL;
		dropTable();
		cleanUp();
	}
	{
		//2、有足够的采样记录
		init();
		createTestTable(10000);

		m_conn->getLocalConfig()->setCompressSampleStrategy("discrete");
		m_conn->getLocalConfig()->m_tblSmplPct = 30;

		RCMSampleTbl * sampleTableHdl = new RCMSampleTbl(m_session, m_db, getTableDef(), getRecords(), NULL);
		SmpTblErrCode errCode = sampleTableHdl->beginSampleTbl();
		sampleTableHdl->endSampleTbl();
		if (errCode == SMP_NO_ERR) {
			RCDictionary *newDic = NULL;
			EXCPT_OPER(newDic = sampleTableHdl->createDictionary(m_session));
			CPPUNIT_ASSERT(newDic != NULL);
			newDic->close();
			delete newDic;
			newDic = NULL;
		} else {
			char errMsg[256];
			sprintf(errMsg, "Error occured when sampled table! error code: %d", errCode);
			CPPUNIT_FAIL(errMsg);
		}

		delete sampleTableHdl;
		sampleTableHdl = NULL;

		dropTable();
		cleanUp();
	}
}