#include "TestRowCache.h"
#include "misc/RecordHelper.h"
#include "api/TestTable.h"
#include "Test.h"

const char* RowCacheTestCase::getName() {
	return "Row cache test";
}

const char* RowCacheTestCase::getDescription() {
	return "Test row cache operations.";
}

bool RowCacheTestCase::isBig() {
	return false;
}

void RowCacheTestCase::testBasic() {
	TableDefBuilder tdb(1, "schema", "test");
	tdb.addColumn("A", CT_BIGINT)->addColumnS("B", CT_CHAR, 16);
	TableDef *tableDef = tdb.getTableDef();

	u16 cols[1] = {1};
	RowCache cache(1000000, tableDef, 1, cols);

	RedRecord r1(tableDef);
	r1.writeChar(1, "test1");
	RedRecord r2(tableDef);
	r2.setNull(1);
	
	CPPUNIT_ASSERT(cache.put(1, r1.getRecord()->m_data) == 0);
	CPPUNIT_ASSERT(cache.put(2, r2.getRecord()->m_data) == 1);

	RedRecord r1o(tableDef);
	RedRecord r2o(tableDef);

	cache.get(0, r1o.getRecord()->m_data);
	cache.get(1, r2o.getRecord()->m_data);

	CPPUNIT_ASSERT(!TableTestCase::compareRecord(tableDef, r1.getRecord()->m_data, r1o.getRecord()->m_data, 1, cols));
	CPPUNIT_ASSERT(!TableTestCase::compareRecord(tableDef, r2.getRecord()->m_data, r2o.getRecord()->m_data, 1, cols));

	delete tableDef;
}

void RowCacheTestCase::testExceedMemLimit() {
	TableDefBuilder tdb(1, "schema", "test");
	tdb.addColumn("A", CT_BIGINT)->addColumnS("B", CT_CHAR, 16);
	TableDef *tableDef = tdb.getTableDef();

	u16 cols[1] = {1};
	RowCache cache(100000, tableDef, 1, cols);

	vector<RedRecord *> recs;
	long n = 0;
	while (true) {
		RedRecord *r = new RedRecord(tableDef);
		char *s = randomStr(10);
		r->writeChar(1, s);
		delete []s;
		if (cache.put(n, r->getRecord()->m_data) >= 0)
			recs.push_back(r);
		else {
			delete r;
			break;
		}
		n++;
	}
	CPPUNIT_ASSERT(cache.m_isFull);
	CPPUNIT_ASSERT(cache.m_rows.getMemUsage() + cache.m_ctx->getMemUsage() <= 1000000);
	RedRecord *r = new RedRecord(tableDef);
	char *s = randomStr(10);
	r->writeChar(1, s);
	delete []s;
	CPPUNIT_ASSERT(cache.put(1, r->getRecord()->m_data) < 0);
	delete r;

	for (long i = 0; i < n; i++) {
		RedRecord ro(tableDef);
		cache.get(i, ro.getRecord()->m_data);
		CPPUNIT_ASSERT(!TableTestCase::compareRecord(tableDef, recs[i]->getRecord()->m_data, ro.getRecord()->m_data, 1, cols));
	}

	while (!recs.empty()) {
		r = recs.back();
		recs.pop_back();
		delete r;
	}
	delete tableDef;
}

void RowCacheTestCase::testLob() {
	TableDefBuilder tdb(1, "schema", "test");
	tdb.addColumn("A", CT_BIGINT)->addColumnS("B", CT_CHAR, 16);
	tdb.addColumn("C", CT_SMALLLOB);
	TableDef *tableDef = tdb.getTableDef();

	u16 cols[2] = {1, 2};
	RowCache cache(1000000, tableDef, 2, cols);

	RedRecord r1(tableDef);
	r1.writeChar(1, "test1");
	r1.setNull(2);
	RedRecord r2(tableDef);
	r2.setNull(1);
	char *s = randomStr(100);
	r2.writeLob(2, (byte *)s, strlen(s));

	CPPUNIT_ASSERT(cache.put(1, r1.getRecord()->m_data) == 0);
	CPPUNIT_ASSERT(cache.put(2, r2.getRecord()->m_data) == 1);

	RedRecord r1o(tableDef);
	RedRecord r2o(tableDef);

	cache.get(0, r1o.getRecord()->m_data);
	cache.get(1, r2o.getRecord()->m_data);

	CPPUNIT_ASSERT(!TableTestCase::compareRecord(tableDef, r1.getRecord()->m_data, r1o.getRecord()->m_data, 2, cols));
	CPPUNIT_ASSERT(!TableTestCase::compareRecord(tableDef, r2.getRecord()->m_data, r2o.getRecord()->m_data, 2, cols));

	// r2o中大对象数据不是new出来的，设为NULL防止析构时delete
	r2o.setNull(2);

	delete tableDef;
}
