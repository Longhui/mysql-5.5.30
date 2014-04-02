/**
 * 记录压缩操作相关测试
 *
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
 */

#include "misc/TestCompressRecord.h"

using namespace std;
using namespace ntse;

const char* RecordCompressTestCase::getName() {
	return "Compress record test";
}

const char* RecordCompressTestCase::getDescription() {
	return "Functional test for compress record operation.";
}

void RecordCompressTestCase::setUp() {
	m_studentTable = new StudentTable(true);
	m_compressExtractor = new CompressExtractorForTest(m_studentTable->getTableDef());
}

void RecordCompressTestCase::tearDown() {
	if (m_studentTable) {
		delete m_studentTable;
		m_studentTable = NULL;
	}
	if (m_compressExtractor) {
		delete m_compressExtractor;
		m_compressExtractor = NULL;
	}
}

void RecordCompressTestCase::testConvRecordVarToCO() {
	const TableDef* tableDef = m_studentTable->getTableDef();
	MemoryContext *mtx = new MemoryContext(NTSE_PAGE_SIZE, 1);
	{
		//one column group
		CompressOrderRecord *convertedRecord = RecordBuilder::createEmptCompressOrderRcd(
			INVALID_ROW_ID, tableDef->m_maxRecSize, tableDef->m_numColGrps);
		Record* varRecord = m_studentTable->createRecord(REC_VARLEN, "beijing2008", 127, 58, "F", 1, 4.0f, 0);
		RecordOper::convRecordVarToCO(mtx, tableDef, varRecord, convertedRecord);

		RecordBuilder rb(tableDef, 0, REC_COMPRESSORDER);
		rb.appendChar("F")->appendSmallInt(58)->appendInt(127)->appendVarchar("beijing2008") \
			->appendBigInt(0)->appendFloat(4.0f)->appendMediumInt(1);
		CompressOrderRecord* trueRecord = rb.getCompressOrderRecord();

		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef,convertedRecord, trueRecord));

		freeRecord(varRecord);
		freeCompressOrderRecord(trueRecord);
		freeCompressOrderRecord(convertedRecord);
	}
	mtx->reset();
	delete mtx;
}

void RecordCompressTestCase::testConvRecordRedToCO() {
	const TableDef* tableDef = m_studentTable->getTableDef();
	{
		Record* record = m_studentTable->createRecord(REC_REDUNDANT, "huanhuan", 300, 58, "F", 1, 4.0f);
		CompressOrderRecord *convertedRecord = RecordBuilder::createEmptCompressOrderRcd(
			record->m_rowId, tableDef->m_maxRecSize, tableDef->m_numColGrps);

		RecordOper::convRecordRedToCO(tableDef, record, convertedRecord);

		Record *revertRecord = RecordBuilder::createEmptyRecord(
			record->m_rowId, REC_REDUNDANT, tableDef->m_maxRecSize);
		RecordOper::convRecordCOToRed(tableDef, convertedRecord, revertRecord);

		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef,record, revertRecord));
		
		freeRecord(record);
		freeRecord(revertRecord);
		freeCompressOrderRecord(convertedRecord);
	}
	{ // NULL
		
		RecordBuilder rb(tableDef, 0, REC_REDUNDANT);
		rb.appendVarchar("google")->appendInt(300)->appendNull()->appendChar("F")->appendNull()
			->appendFloat(4.0f)->appendBigInt(0);
		Record* record = rb.getRecord();

		CompressOrderRecord *convertedRecord = RecordBuilder::createEmptCompressOrderRcd(
			record->m_rowId, tableDef->m_maxRecSize, tableDef->m_numColGrps);

		RecordOper::convRecordRedToCO(tableDef, record, convertedRecord);

		Record *revertRecord = RecordBuilder::createEmptyRecord(
			record->m_rowId, REC_REDUNDANT, tableDef->m_maxRecSize);
		RecordOper::convRecordCOToRed(tableDef, convertedRecord, revertRecord);

		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef,record, revertRecord));

		freeRecord(record);
		freeRecord(revertRecord);
		freeCompressOrderRecord(convertedRecord);
	}
}
void RecordCompressTestCase::testConvRcdCOToReal() {
	const TableDef* tableDef = m_studentTable->getTableDef();
	{
		CompressOrderRecord* record = m_studentTable->createCompressOrderRecord("huanhuan", 300, 58, "F", 1, 4.0f);
	
		Record *compressedRcd = RecordBuilder::createEmptyRecord(record->m_rowId, REC_COMPRESSED, tableDef->m_maxRecSize);

		RecordOper::convRecordCOToComprssed(m_compressExtractor, record, compressedRcd);
		
		CompressOrderRecord *revertRecord = RecordBuilder::createEmptCompressOrderRcd(record->m_rowId, tableDef->m_maxRecSize, tableDef->m_numColGrps);

		RecordOper::convRecordCompressedToCO(m_compressExtractor, compressedRcd, revertRecord);

		CPPUNIT_ASSERT(!memcmp(record->m_data, revertRecord->m_data, record->m_size));
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef,record, revertRecord));

		freeCompressOrderRecord(record);
		freeCompressOrderRecord(revertRecord);
		freeRecord(compressedRcd);
	}
	{ // NULL
		RecordBuilder rb(tableDef, 0, REC_COMPRESSORDER);
		rb.appendChar("F")->appendNull()->appendInt(300)->appendVarchar("google")
			->appendBigInt(0)->appendFloat(4.0f)->appendNull();
		CompressOrderRecord* record = rb.getCompressOrderRecord();

		Record *convertedRecord = RecordBuilder::createEmptyRecord(record->m_rowId, REC_COMPRESSED, tableDef->m_maxRecSize);

		RecordOper::convRecordCOToComprssed(m_compressExtractor, record, convertedRecord);

		CompressOrderRecord *revertRecord = RecordBuilder::createEmptCompressOrderRcd(record->m_rowId, tableDef->m_maxRecSize, tableDef->m_numColGrps);

		RecordOper::convRecordCompressedToCO(m_compressExtractor, convertedRecord, revertRecord);

		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef,record, revertRecord));

		freeCompressOrderRecord(record);
		freeCompressOrderRecord(revertRecord);
		freeRecord(convertedRecord);
	}
}
void RecordCompressTestCase::testExtractSubCompressR() {
	const TableDef *tableDef = m_studentTable->getTableDef();
	MemoryContext *mtx = new MemoryContext(NTSE_PAGE_SIZE, 1);
	SubRecordBuilder redundantBuilder(tableDef, REC_REDUNDANT);
	const char *sname = "huanhuan";
	short age = 58;
	{ // 没有NULL列
		CompressOrderRecord* record = m_studentTable->createCompressOrderRecord(sname, 300, age, "F", 1, 4.0f);
		Record *compressedRecord = RecordBuilder::createEmptyRecord(record->m_rowId, REC_COMPRESSED, tableDef->m_maxRecSize);
		RecordOper::convRecordCOToComprssed(m_compressExtractor, record, compressedRecord);

		SubRecord* subRcd = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::extractSubRecordCompressedR(mtx, m_compressExtractor, tableDef, compressedRecord, subRcd);
		SubRecord* trueKey = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef, subRcd, trueKey));
		freeCompressOrderRecord(record);
		freeRecord(compressedRecord);
		freeSubRecord(trueKey);
		freeSubRecord(subRcd);
	}
	{ // 有NULL列
		RecordBuilder rb(tableDef, 0, REC_COMPRESSORDER);
		rb.appendChar("F")->appendNull()->appendInt(12)->appendVarchar(sname)->appendBigInt(0)->appendFloat(4.0f)->appendMediumInt(1);
		CompressOrderRecord *record = rb.getCompressOrderRecord();
		Record *compressedRecord = RecordBuilder::createEmptyRecord(record->m_rowId, REC_COMPRESSED, tableDef->m_maxRecSize);
		RecordOper::convRecordCOToComprssed(m_compressExtractor, record, compressedRecord);

		SubRecord* subRcd = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::extractSubRecordCompressedR(mtx, m_compressExtractor, tableDef, compressedRecord, subRcd);
		SubRecord* trueSubRcd = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRcd, trueSubRcd));
		freeRecord(compressedRecord);
		freeCompressOrderRecord(record);
		freeSubRecord(trueSubRcd);
		freeSubRecord(subRcd);
	}
	
	{ // 三个NULL列
		RecordBuilder rb(tableDef, 0, REC_COMPRESSORDER);
		rb.appendNull()->appendNull()->appendNull()->appendVarchar(sname) ->appendBigInt(0)->appendFloat(4.0f)->appendMediumInt(1);
		CompressOrderRecord *record = rb.getCompressOrderRecord();
		Record *compressedRecord = RecordBuilder::createEmptyRecord(record->m_rowId, REC_COMPRESSED, tableDef->m_maxRecSize);
		RecordOper::convRecordCOToComprssed(m_compressExtractor, record, compressedRecord);
		
		SubRecord* subRcd = redundantBuilder.createEmptySbByName(tableDef->m_maxRecSize, STU_NAME" "STU_AGE);
		RecordOper::extractSubRecordCompressedR(mtx, m_compressExtractor, tableDef, compressedRecord, subRcd);
		SubRecord* trueSubRcd = redundantBuilder.createSubRecordByName(STU_NAME" "STU_AGE, sname, NULL);
		CPPUNIT_ASSERT(RecordOper::isSubRecordEq(tableDef,subRcd, trueSubRcd));

		freeCompressOrderRecord(record);
		freeRecord(compressedRecord);
		freeSubRecord(trueSubRcd);
		freeSubRecord(subRcd);
	}
	
	mtx->reset();
	delete mtx;
	mtx = NULL;
}
void RecordCompressTestCase::testUpdateRecordWithDict() {

}
void RecordCompressTestCase::testUpdateCompressRecord() {	
	{
		//更新压缩格式记录， 但是更新之后不压缩
		doUpdateCompressedRecord(false, false);
		//更新压缩格式记录， 但是更新之后尝试压缩失败
		doUpdateCompressedRecord(true, false);
		//更新压缩格式记录， 但是更新之后尝试压缩成功
		doUpdateCompressedRecord(true, false);
	}
}

void RecordCompressTestCase::testUpdateUncompressRecord() {	
	//更新非压缩格式记录， 但是更新之后不压缩
	{
		doUpdateUncompressedRecord(false, false);	
	}
	//更新非压缩格式记录， 但是更新之后尝试压缩失败
	{
		doUpdateUncompressedRecord(true, false);;
	}
	//更新非压缩格式记录， 但是更新之后尝试压缩成功
	{
		doUpdateUncompressedRecord(true, false);
	}
}

/*
 * 更新压缩格式记录
 * @param lobUseOld     大对象是否使用原相
 * @param needCompress  是否需要进行压缩
 * @param compressAble  更新之后的记录是否应该为压缩格式
 */
void RecordCompressTestCase::doUpdateCompressedRecord(bool needCompress, bool compressAble) {
	TableDef* tableDef = const_cast<TableDef*>(m_studentTable->getTableDef());
	tableDef->m_isCompressedTbl = needCompress;
	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	MemoryContext *mtx = new MemoryContext(NTSE_PAGE_SIZE, 1);
	
	//非NULL
	{	
		CompressOrderRecord* compressOrderRcd = NULL;
		compressOrderRcd = m_studentTable->createCompressOrderRecord("beijing2008", 127, 58, "F", 1, 4.0f);

		//先转化为压缩格式记录
		Record *compressedRecord = RecordBuilder::createEmptyRecord(compressOrderRcd->m_rowId, 
			REC_COMPRESSED, tableDef->m_maxRecSize << 1);
		RecordOper::convRecordCOToComprssed(m_compressExtractor, compressOrderRcd, compressedRecord);

		//更新
		short age = 200;
		const char *sname = compressAble ? "huanhuan" : "h2u21a8hua" ;

		SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
		Record *newRecord = RecordBuilder::createEmptyRecord(compressOrderRcd->m_rowId, REC_VARLEN, 
			tableDef->m_maxRecSize << 1);
		RecordOper::updateCompressedRcd(mtx, tableDef, m_compressExtractor, compressedRecord, update, 
			newRecord);
		Record* trueRecord = NULL;

		if (needCompress && compressAble) {
			CPPUNIT_ASSERT(newRecord->m_format == REC_COMPRESSED);
			CompressOrderRecord* tmpRcd = m_studentTable->createCompressOrderRecord(sname, 127, age, "F", 1, 4.0f);
			trueRecord = RecordBuilder::createEmptyRecord(compressOrderRcd->m_rowId, REC_VARLEN, tableDef->m_maxRecSize << 1);		
			RecordOper::convRecordCOToComprssed(m_compressExtractor, tmpRcd, trueRecord);
			freeCompressOrderRecord(tmpRcd);
		} else {
			CPPUNIT_ASSERT(newRecord->m_format == REC_VARLEN);
			trueRecord = m_studentTable->createRecord(REC_VARLEN, sname, 127, age, "F", 1, 4.0f);
		}
		CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, newRecord, trueRecord));

		freeCompressOrderRecord(compressOrderRcd);
		freeRecord(trueRecord);
		freeRecord(newRecord);
		freeRecord(compressedRecord);
		freeSubRecord(update);
	}	
	mtx->reset();
	delete mtx;
}

/*
* 更新非压缩格式记录
* @param lobUseOld     大对象是否使用原相
* @param needCompress  是否需要进行压缩
* @param compressAble  更新之后的记录是否应该为压缩格式
*/
void RecordCompressTestCase::doUpdateUncompressedRecord(bool needCompress, bool compressAble) {
	TableDef* tableDef = const_cast<TableDef*>(m_studentTable->getTableDef());
	tableDef->m_isCompressedTbl = needCompress;

	SubRecordBuilder sbb(tableDef, REC_REDUNDANT);
	MemoryContext *mtx = new MemoryContext(NTSE_PAGE_SIZE, 1);

	Record* varRecord =m_studentTable->createRecord(REC_VARLEN, "beijing2008", 127, 58, "F", 1, 4.0f);

	//更新
	short age = 200;
	const char *sname = compressAble ? "huanhuan" : "h2u21a8hua";
	SubRecord* update = sbb.createSubRecordByName(STU_NAME" "STU_AGE, sname, &age);
	Record *newRecord = RecordBuilder::createEmptyRecord(varRecord->m_rowId, REC_VARLEN, 
		tableDef->m_maxRecSize << 1);
	RecordOper::updateUncompressedRcd(mtx, tableDef, m_compressExtractor, varRecord, update, newRecord);
	
	Record* trueRecord = NULL;
	if (needCompress && compressAble) {
		CPPUNIT_ASSERT(newRecord->m_format == REC_COMPRESSED);
		CompressOrderRecord* tmpRcd = m_studentTable->createCompressOrderRecord(sname, 127, age, "F", 1, 4.0f);
		trueRecord = RecordBuilder::createEmptyRecord(varRecord->m_rowId, REC_VARLEN, tableDef->m_maxRecSize << 1);		
		RecordOper::convRecordCOToComprssed(m_compressExtractor, tmpRcd, trueRecord);
		freeCompressOrderRecord(tmpRcd);
	} else {
		CPPUNIT_ASSERT(newRecord->m_format == REC_VARLEN);
		trueRecord = m_studentTable->createRecord(REC_VARLEN, sname, 127, age, "F", 1, 4.0f);
	}
	CPPUNIT_ASSERT(RecordOper::isRecordEq(tableDef, newRecord, trueRecord));

	freeRecord(varRecord);
	freeRecord(trueRecord);
	freeRecord(newRecord);
	freeSubRecord(update);

	mtx->reset();
	delete mtx;
}

