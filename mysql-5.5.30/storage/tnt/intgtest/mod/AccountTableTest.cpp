#include "AccountTableTest.h"
#include "AccountTable.h"
#include "misc/Record.h"
#include "misc/RecordHelper.h"

using namespace ntse;


void AccountTableTestCase::testCreateRecord() {
	for (uint i = 0; i < 10; ++i){
		int recSize;
		Record *record = AccountTable::createRecord(0, &recSize, 8, 9);
		CPPUNIT_ASSERT(record->m_size >= (uint)recSize);
		CPPUNIT_ASSERT(recSize >= 8);
		CPPUNIT_ASSERT(recSize <= 16);
		CPPUNIT_ASSERT(0 == RedRecord::readBigInt(AccountTable::getTableDef(true), record->m_data, ACCOUNT_ID_CNO));
		freeRecord(record);
	}

	u64 total = 0;
	uint count = 1000;
	uint minSize = Limits::PAGE_SIZE / 6;
	uint meanSize = Limits::PAGE_SIZE / 3;
	for (uint i = 0; i < 1000; ++i){
		int recSize;
		Record *record = AccountTable::createRecord(0, &recSize, minSize, meanSize);
		freeRecord(record);
		total += recSize;
	}
	cout << "expected avg " << meanSize << " avg " << total / count << endl;
}
