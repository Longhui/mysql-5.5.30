#include "TestVersionPool.h"
#include "util/File.h"
#include "misc/RecordHelper.h"
#include "api/Table.h"
#include "Test.h"

#define TNT_STU_NAME "name"
#define TNT_STU_NAME_IDX 0
#define TNT_STU_SNO "sno"
#define TNT_STU_SNO_IDX 1
#define TNT_STU_AGE "age"
#define TNT_STU_AGE_IDX 2
#define TNT_STU_SEX "sex"
#define TNT_STU_SEX_IDX 3
#define TNT_STU_CLASS "class"
#define TNT_STU_CLASS_IDX 4
#define TNT_STU_GPA "gpa"
#define TNT_STU_GPA_IDX 5
#define TNT_STU_GRADE "grade"
#define TNT_STU_GRADE_IDX 6
#define TNT_STU_COMMENT "comment"
#define TNT_STU_COMMENT_IDX 7
#define TNT_STU_CTX   "ctx"
#define TNT_STU_CTX_IDX 8

const char* VersionPoolTestCase::getName() {
	return "Version pool test";
}

const char* VersionPoolTestCase::getDescription() {
	return "Test various operations of version pool which can snapshot read";
}

bool VersionPoolTestCase::isBig() {
	return false;
}

TableDef* VersionPoolTestCase::createTableDef() {
	TableDefBuilder tb(TableDef::INVALID_TABLEID, "Olympic", "student");
	tb.addColumnS(TNT_STU_NAME, CT_VARCHAR, 30, false, false, COLL_LATIN1);
	tb.addColumn(TNT_STU_SNO, CT_INT);
	tb.addColumn(TNT_STU_AGE, CT_SMALLINT);
	tb.addColumnS(TNT_STU_SEX, CT_CHAR, 1, false, true, COLL_LATIN1);
	tb.addColumn(TNT_STU_CLASS, CT_MEDIUMINT);
	tb.addColumn(TNT_STU_GPA, CT_FLOAT);
	PrType prtype;
	prtype.setUnsigned();
	tb.addColumnN(TNT_STU_GRADE, CT_BIGINT, prtype);
	tb.addColumnS(TNT_STU_COMMENT, CT_VARCHAR, VersionPool::VERSIONPOOL_SMALLDIFF_MAX, false, true, COLL_LATIN1);
	tb.addColumn(TNT_STU_CTX, CT_SMALLLOB);
	return tb.getTableDef();
}

Record* VersionPoolTestCase::createStudentRec(RowId rid, const char *name, u32 stuNo, u16 age, const char *sex, u32 clsNo, u64 grade, const char* comment, LobId lobId) {
	RecordBuilder rb(m_tableDef, rid, REC_REDUNDANT);
	rb.appendVarchar(name);
	rb.appendInt(stuNo);
	rb.appendSmallInt(age);
	rb.appendChar(sex);
	rb.appendMediumInt(clsNo);
	rb.appendNull();
	rb.appendBigInt(grade);
	if (comment == NULL) {
		rb.appendNull();
	} else {
		rb.appendVarchar(comment);
	}
	Record *rec = rb.getRecord(m_tableDef->m_maxRecSize);

	size_t bmBytes = (m_tableDef->m_nullableCols + 7) / 8;//(format == REC_FIXLEN) ? (nullableCols + 8) / 8 : (nullableCols + 7) / 8;

	Bitmap bp(rec->m_data, bmBytes*8); 
	ColumnDef *columnDef = m_tableDef->m_columns[TNT_STU_CTX_IDX];
	u16 offset = columnDef->m_nullBitmapOffset;
	bp.clearBit(offset);
	RecordOper::writeLobId(rec->m_data, columnDef, lobId);
	rec->m_size += columnDef->calcSize();
	
	return rec;
}

SubRecord *VersionPoolTestCase::createEmptySubRecord(TableDef *tblDef, RecFormat format, MemoryContext *ctx) {
	vector<u16> columns;
	vector<void *> datas;
	return createSubRecord(tblDef, format, columns, datas, ctx);
}

SubRecord *VersionPoolTestCase::createSubRecord(TableDef *tblDef, RecFormat format, const std::vector<u16> &columns, std::vector<void *> &datas, MemoryContext *ctx) {
	NTSE_ASSERT(columns.size() == datas.size());
	u16 i = 0;
	ColumnDef * columnDef = NULL;
	std::vector<void *> lobIds;
	std::vector<u16> lobPos;
	for (i = 0; i < columns.size(); i++) {
		columnDef = tblDef->getColumnDef(columns.at(i));
		if (columnDef->isLob()) {
			lobPos.push_back(i);
			lobIds.push_back(datas.at(i));
			datas[i] = NULL;
		}
	}
	SubRecord *subRec = NULL;
	u64 sp = ctx->setSavepoint();
	void *p = ctx->alloc(sizeof(SubRecordBuilder));
	SubRecordBuilder* sb = new (p) SubRecordBuilder(tblDef, format);
	subRec = sb->createSubRecord(columns, datas);

	LobId *lobId = NULL;
	for (i = 0; i < lobPos.size(); i++) {
		lobId = (LobId *)lobIds[i];
		NTSE_ASSERT(datas[lobPos[i]] == NULL);
		datas[lobPos[i]] = lobIds[i];

		size_t bmBytes = (m_tableDef->m_nullableCols + 7) / 8;//(format == REC_FIXLEN) ? (nullableCols + 8) / 8 : (nullableCols + 7) / 8;

		Bitmap bp(subRec->m_data, bmBytes*8); 
		columnDef = tblDef->m_columns[columns[lobPos[i]]];
		u16 offset = columnDef->m_nullBitmapOffset;
		bp.clearBit(offset);
		RecordOper::writeLobId(subRec->m_data, columnDef, *lobId);

		if (subRec->m_size != tblDef->m_maxRecSize) {
			subRec->m_size += columnDef->calcSize();
		}
	}

	ctx->resetToSavepoint(sp);

	return subRec;
}

ReadView* VersionPoolTestCase::createReadView(MemoryContext *ctx, TrxId createTrxId, TrxId up, TrxId low, TrxId *activeTrxIds/*= NULL*/, u16 activeTrxCnt/*= 0*/) {
	ReadView *readView = new (ctx->alloc(sizeof(ReadView))) ReadView(createTrxId, activeTrxIds, activeTrxCnt);
	readView->setUpTrxId(up);
	readView->setLowTrxId(low);
	return readView;
}

void VersionPoolTestCase::setUp() {
	File dir("testdb");
	dir.rmdir(true);
	dir.mkdir();
	m_config.setBasedir("testdb");
	m_config.m_logLevel = EL_WARN;
	m_config.m_pageBufSize = 100;
	m_config.m_mmsSize = 20;
	m_config.m_logFileSize = 1024 * 1024 * 128;
	EXCPT_OPER(m_db = Database::open(&m_config, true));

	m_basePath = (char *)"./";
	m_count = 8;

	//变长堆，必须包括堆首页，中央位图，页簇位图，记录页。最后一个页面只插一条记录
	m_maxPageId = 4;

	m_conn = m_db->getConnection(false);
	m_conn->setTrx(true);
	m_session = m_db->getSessionManager()->allocSession("VersionPoolTestCase", m_conn);

	VersionPool::create(m_db, m_session, m_basePath, m_count);
	m_versionPool = VersionPool::open(m_db, m_session, m_basePath, m_count);
	CPPUNIT_ASSERT(m_versionPool != NULL);

	m_tableDef = createTableDef();
}

void VersionPoolTestCase::tearDown() {
	m_versionPool->close(m_session);
	delete m_versionPool;

	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
	m_db->close(false, false);

	delete m_db;
	m_db = NULL;

	delete m_tableDef;

	File dir("testdb");
	dir.rmdir(true);
}

void VersionPoolTestCase::testInsert() {
	u32 pageNum = 0;
	MemoryContext *ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
	u64 sp = ctx->setSavepoint();
	SubRecord *subRec = createEmptySubRecord(m_tableDef, REC_REDUNDANT, ctx);
	RowId rid = 0;
	u64 rollBackId = 0;
	u8 vTableIndex = 0;
	u64 txnId = 120;
	u8 tblIndex = 0;
	m_tableDef->m_id = 15;
	rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
	CPPUNIT_ASSERT(rid > 0);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;
	//创建第二条记录进行插入
	int sno = 1000;
	short age = 28;
	std::vector<u16> columns;
	std::vector<void *> datas;
	columns.push_back(TNT_STU_SNO_IDX);
	datas.push_back(&sno);
	columns.push_back(TNT_STU_AGE_IDX);
	datas.push_back(&age);
	subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
	rollBackId = rid;
	vTableIndex = tblIndex;
	txnId++;
	rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
	CPPUNIT_ASSERT(rid > 0);
	CPPUNIT_ASSERT(rid != rollBackId);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	int count = 1;
	u64 pageId1 = RID_GET_PAGE(rid);
	while (true) {
		columns.clear();
		datas.clear();
		columns.push_back(TNT_STU_SNO_IDX);
		sno++;
		datas.push_back(&sno);
		columns.push_back(TNT_STU_AGE_IDX);
		age = age + count % 10;
		datas.push_back(&age);
		subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
		rollBackId = rid;
		vTableIndex = tblIndex;
		txnId++;
		rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
		CPPUNIT_ASSERT(rid > 0);
		count++;
		delete[] subRec->m_columns;
		delete[] subRec->m_data;
		delete subRec;
		subRec = NULL;
		if (pageId1 == RID_GET_PAGE(rid)) {
			CPPUNIT_ASSERT(rid == rollBackId + 1);
		} else if (pageNum >= m_maxPageId) {
			break;
		} else {
			pageNum++;
			pageId1 = RID_GET_PAGE(rid);
		}
	}

	//切换至版本池的另外一个表
	columns.clear();
	datas.clear();
	columns.push_back(TNT_STU_SNO_IDX);
	sno++;
	datas.push_back(&sno);
	columns.push_back(TNT_STU_AGE_IDX);
	age--;
	datas.push_back(&age);
	subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
	rollBackId = rid;
	vTableIndex = tblIndex;
	tblIndex++;
	txnId++;
	rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
	CPPUNIT_ASSERT(rid > 0);

	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	ctx->resetToSavepoint(sp);
	delete ctx;
}

void VersionPoolTestCase::testGetVersionRecord() {
	TrxId txnId = 0;
	MemoryContext *ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
	u64 sp = ctx->setSavepoint();

	SubRecord *subRec = createEmptySubRecord(m_tableDef, REC_REDUNDANT, ctx);
	RowId rid1 = 0;
	u64 rollBackId1 = INVALID_ROW_ID;
	u8 vTableIndex1 = 0;
	u64 txnId1 = 120;
 	u8 tblIndex1 = 0;

	u64 txnId2 = txnId1 + 20;
	m_tableDef->m_id = 15;
	rid1 = m_versionPool->insert(m_session, tblIndex1, m_tableDef, rollBackId1, vTableIndex1, txnId1, subRec, 0, txnId2);
	CPPUNIT_ASSERT(rid1 > 0);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	//创建第二条记录进行插入
	RowId rid2 = 0;
	u64 rollBackId2 = rid1;
	u8 vTableIndex2 = tblIndex1;
	u64 txnId3 = txnId2 + 20;
 	u8 tblIndex2 = 0;

	int stuNo2 = 1000;
	short age2 = 28;
	std::vector<u16> columns;
	std::vector<void *> datas;
	columns.push_back(TNT_STU_SNO_IDX);
	datas.push_back(&stuNo2);
	columns.push_back(TNT_STU_AGE_IDX);
	datas.push_back(&age2);
	subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
	rid2 = m_versionPool->insert(m_session, tblIndex2, m_tableDef, rollBackId2, vTableIndex2, txnId2, subRec, 0, txnId3);
	CPPUNIT_ASSERT(rid2 > 0);
	CPPUNIT_ASSERT(rid2 != rollBackId2);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	//创建第三条记录进行插入
	RowId rid3 = 0;
	u64 rollBackId3 = rid2;
	u8 vTableIndex3 = tblIndex2;
	u64 txnId4 = txnId3 + 20;
 	u8 tblIndex3 = 1;

	int stuNo3 = 1002;
	u32 clsNo3 = 3;
	columns.clear();
	datas.clear();
	columns.push_back(TNT_STU_SNO_IDX);
	datas.push_back(&stuNo3);
	columns.push_back(TNT_STU_CLASS_IDX);
	datas.push_back(&clsNo3);
	subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
	rid3 = m_versionPool->insert(m_session, tblIndex3, m_tableDef, rollBackId3, vTableIndex3, txnId3, subRec, 0, txnId4);
	CPPUNIT_ASSERT(rid3 > 0);
	CPPUNIT_ASSERT(rid3 != rollBackId3 || tblIndex3 != vTableIndex3);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	HeapRecStat stat;
	const char *name = "netease";
	const char *sex = "F";
	u32 clsNo = 6;
	u64 grade = 59;
	u16 age = 29;
	Record *redRec = createStudentRec(202/*rowId*/, name, 1511 /*stuNo*/, age, sex, clsNo, grade);
	u64 sp1 = 0;

	size_t size = 0;
	char *pName = NULL;
	char *pSex = NULL;

	//构建合适的ReadView，最近第一个版本可见
	sp1 = ctx->setSavepoint();
	txnId = (txnId3 + txnId4)/2;
	ReadView *readView1 = createReadView(ctx, txnId, txnId - 1, txnId + 1);
	MHeapRec *heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(txnId4, rid3, tblIndex3, redRec, 0/**del*/);
	heapRec = m_versionPool->getVersionRecord(m_session, m_tableDef, heapRec, readView1, redRec, &stat);
	CPPUNIT_ASSERT(stat == VALID);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId3);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex3);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId3);

	RedRecord redRec1(m_tableDef, redRec);
	redRec1.readVarchar(TNT_STU_NAME_IDX, &pName, &size);
	CPPUNIT_ASSERT(strlen(name) == size);
	CPPUNIT_ASSERT(memcmp(name, pName, size) == 0);

	CPPUNIT_ASSERT(redRec1.readInt(TNT_STU_SNO_IDX) == stuNo3);
	CPPUNIT_ASSERT(redRec1.readSmallInt(TNT_STU_AGE_IDX) == age);

	
	redRec1.readChar(TNT_STU_SEX_IDX, &pSex, &size);
	CPPUNIT_ASSERT(strlen(sex) == size);
	CPPUNIT_ASSERT(memcmp(pSex, sex, size) == 0);

	CPPUNIT_ASSERT(redRec1.readMediumInt(TNT_STU_CLASS_IDX) == clsNo3);
	CPPUNIT_ASSERT(redRec1.readBigInt(TNT_STU_GRADE_IDX) == grade);
	
	ctx->resetToSavepoint(sp1);

	//构建合适的ReadView，最近第二个版本可见
	sp1 = ctx->setSavepoint();
	txnId = (txnId2 + txnId3)/2;
	ReadView *readView2 = createReadView(ctx, txnId, txnId - 1, txnId + 1);
	heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(txnId4, rid3, tblIndex3, redRec, 0);
	heapRec = m_versionPool->getVersionRecord(m_session, m_tableDef, heapRec, readView2, redRec, &stat);
	CPPUNIT_ASSERT(stat == VALID);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId2);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex2);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId2);

	RedRecord redRec2(m_tableDef, redRec);
	redRec1.readVarchar(TNT_STU_NAME_IDX, &pName, &size);
	CPPUNIT_ASSERT(strlen(name) == size);
	CPPUNIT_ASSERT(memcmp(name, pName, size) == 0);

	CPPUNIT_ASSERT(redRec2.readInt(TNT_STU_SNO_IDX) == stuNo2);
	CPPUNIT_ASSERT(redRec2.readSmallInt(TNT_STU_AGE_IDX) == age2);

	
	redRec2.readChar(TNT_STU_SEX_IDX, &pSex, &size);
	CPPUNIT_ASSERT(strlen(sex) == size);
	CPPUNIT_ASSERT(memcmp(pSex, sex, size) == 0);

	CPPUNIT_ASSERT(redRec2.readMediumInt(TNT_STU_CLASS_IDX) == clsNo3);
	CPPUNIT_ASSERT(redRec2.readBigInt(TNT_STU_GRADE_IDX) == grade);
	
	ctx->resetToSavepoint(sp1);

	//构建合适的ReadView，NTSE版本可见
	sp1 = ctx->setSavepoint();
	txnId = (txnId1 + txnId2)/2;
	ReadView *readView3 = createReadView(ctx, txnId, txnId - 1, txnId + 1);
	heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(txnId4, rid3, tblIndex3, redRec, 0);
	heapRec = m_versionPool->getVersionRecord(m_session, m_tableDef, heapRec, readView3, redRec, &stat);
	CPPUNIT_ASSERT(heapRec != NULL);
	CPPUNIT_ASSERT(stat == NTSE_VISIBLE);
	ctx->resetToSavepoint(sp1);

	//构建合适的ReadView，NTSE版本不可见
	sp1 = ctx->setSavepoint();
	txnId = txnId1/2;
	ReadView *readView4 = createReadView(ctx, txnId, txnId - 1, txnId + 1);
	heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(txnId4, rid3, tblIndex3, redRec, 0);
	heapRec = m_versionPool->getVersionRecord(m_session, m_tableDef, heapRec, readView4, redRec, &stat);
	CPPUNIT_ASSERT(heapRec == NULL);
	CPPUNIT_ASSERT(stat == NTSE_UNVISIBLE);
	ctx->resetToSavepoint(sp1);

	ctx->resetToSavepoint(sp);

	delete ctx;

	delete [] redRec->m_data;
	delete redRec;
}

void VersionPoolTestCase::testGetVersionRecordBigDiff() {
	MemoryContext *ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
	u64 sp = ctx->setSavepoint();

	SubRecord *subRec = createEmptySubRecord(m_tableDef, REC_REDUNDANT, ctx);
	RowId rid1 = 0;
	RowId rollBackId1 = INVALID_ROW_ID;
	u8 vTableIndex1 = 0;
	TrxId txnId1 = 120;
	u8 tblIndex1 = 0;

	TrxId txnId2 = txnId1 + 20;
	m_tableDef->m_id = 15;
	rid1 = m_versionPool->insert(m_session, tblIndex1, m_tableDef, rollBackId1, vTableIndex1, txnId1, subRec, 0, txnId2);
	CPPUNIT_ASSERT(rid1 > 0);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	//创建第二条记录进行插入
	RowId rid2 = 0;
	RowId rollBackId2 = rid1;
	u8 vTableIndex2 = tblIndex1;
	TrxId txnId3 = txnId2 + 20;
	u8 tblIndex2 = 1;

	int stuNo2 = 1000;
	short age2 = 28;
	char *comment2 = (char *)ctx->calloc((VersionPool::VERSIONPOOL_SMALLDIFF_MAX + 1)*sizeof(char));
	memset(comment2, 'a', VersionPool::VERSIONPOOL_SMALLDIFF_MAX);
	std::vector<u16> columns;
	std::vector<void *> datas;
	columns.push_back(TNT_STU_SNO_IDX);
	datas.push_back(&stuNo2);
	columns.push_back(TNT_STU_AGE_IDX);
	datas.push_back(&age2);
	columns.push_back(TNT_STU_COMMENT_IDX);
	datas.push_back(comment2);
	subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
	rid2 = m_versionPool->insert(m_session, tblIndex2, m_tableDef, rollBackId2, vTableIndex2, txnId2, subRec, 0, txnId3);
	CPPUNIT_ASSERT(rid2 > 0);
	CPPUNIT_ASSERT(rid2 != rollBackId2 || vTableIndex2 != tblIndex2);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	HeapRecStat stat; 
	TrxId txnId = INVALID_TRX_ID;
	const char *name = "netease";
	const char *sex = "F";
	u32 clsNo = 6;
	u64 grade = 59;
	u16 age = 29;
	size_t size = 0;
	char *pName = NULL;
	char *pSex = NULL;
	char *pComment = NULL;
	Record *redRec = createStudentRec(202/*rowId*/, name, 1511 /*stuNo*/, age, sex, clsNo, grade);
	u64 sp1 = 0;

	//构建合适的ReadView，最近第一个版本可见
	sp1 = ctx->setSavepoint();
	txnId = txnId3 + 20;
	ReadView *readView1 = createReadView(ctx, txnId, txnId - 1, txnId + 1);
	MHeapRec *heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(INVALID_TRX_ID, rid2, tblIndex2, redRec, 0/**del*/);
	heapRec = m_versionPool->getVersionRecord(m_session, m_tableDef, heapRec, readView1, redRec, &stat);
	CPPUNIT_ASSERT(stat == VALID);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId2);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex2);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId2);

	RedRecord redRec1(m_tableDef, redRec);
	redRec1.readVarchar(TNT_STU_NAME_IDX, &pName, &size);
	CPPUNIT_ASSERT(strlen(name) == size);
	CPPUNIT_ASSERT(memcmp(name, pName, size) == 0);

	CPPUNIT_ASSERT(redRec1.readInt(TNT_STU_SNO_IDX) == stuNo2);
	CPPUNIT_ASSERT(redRec1.readSmallInt(TNT_STU_AGE_IDX) == age2);

	redRec1.readChar(TNT_STU_SEX_IDX, &pSex, &size);
	CPPUNIT_ASSERT(strlen(sex) == size);
	CPPUNIT_ASSERT(memcmp(pSex, sex, size) == 0);

	CPPUNIT_ASSERT(redRec1.readMediumInt(TNT_STU_CLASS_IDX) == clsNo);
	CPPUNIT_ASSERT(redRec1.readBigInt(TNT_STU_GRADE_IDX) == grade);

	redRec1.readVarchar(TNT_STU_COMMENT_IDX, &pComment, &size);
	CPPUNIT_ASSERT(strlen(comment2) == size);
	CPPUNIT_ASSERT(memcmp(comment2, pComment, size) == 0);

	ctx->resetToSavepoint(sp1);

	//构建合适的ReadView，NTSE版本可见
	sp1 = ctx->setSavepoint();
	txnId = (txnId1 + txnId2)/2;
	ReadView *readView2 = createReadView(ctx, txnId, txnId - 1, txnId + 1);
	heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(INVALID_TRX_ID, rid2, tblIndex2, redRec, 0);
	heapRec = m_versionPool->getVersionRecord(m_session, m_tableDef, heapRec, readView2, redRec, &stat);
	CPPUNIT_ASSERT(heapRec != NULL);
	CPPUNIT_ASSERT(stat == NTSE_VISIBLE);
	ctx->resetToSavepoint(sp1);

	//构建合适的ReadView，NTSE版本不可见
	sp1 = ctx->setSavepoint();
	txnId = txnId1/2;
	ReadView *readView3 = createReadView(ctx, txnId, txnId - 1, txnId + 1);
	heapRec = new (ctx->alloc(sizeof(MHeapRec))) MHeapRec(INVALID_TRX_ID, rid2, tblIndex2, redRec, 0);
	heapRec = m_versionPool->getVersionRecord(m_session, m_tableDef, heapRec, readView3, redRec, &stat);
	CPPUNIT_ASSERT(heapRec == NULL);
	CPPUNIT_ASSERT(stat == NTSE_UNVISIBLE);
	ctx->resetToSavepoint(sp1);

	ctx->resetToSavepoint(sp);

	delete ctx;

	delete [] redRec->m_data;
	delete redRec;
}

void VersionPoolTestCase::testGetRollBackHeapRec() {
	MemoryContext *ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
	u64 sp = ctx->setSavepoint();
	SubRecord *subRec = createEmptySubRecord(m_tableDef, REC_REDUNDANT, ctx);
	RowId rid = 0;
	u64 rollBackId = 0;
	u8 vTableIndex = 0;
	u64 txnId = 120;
 	u8 tblIndex = 0;
	m_tableDef->m_id = 15;
	rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
	CPPUNIT_ASSERT(rid > 0);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;
	//创建第二条记录进行插入
	int stuNo = 1000;
	short age = 28;
	std::vector<u16> columns;
	std::vector<void *> datas;
	columns.push_back(TNT_STU_SNO_IDX);
	datas.push_back(&stuNo);
	columns.push_back(TNT_STU_AGE_IDX);
	datas.push_back(&age);
	subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
	rollBackId = rid;
	vTableIndex = tblIndex;
	txnId++;
	rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
	CPPUNIT_ASSERT(rid > 0);
	CPPUNIT_ASSERT(rid != rollBackId);

	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	const char *name = "netease";
	const char *sex = "F";
	u32 clsNo = 6;
	u64 grade = 59;
	Record *rec = createStudentRec(202/*rowId*/, name, 1511 /*stuNo*/, 19 /*age*/, sex, clsNo, grade);
	//根据更新前像拼凑出完整记录
	MHeapRec *heapRec = m_versionPool->getRollBackHeapRec(m_session, m_tableDef, tblIndex, rec, rid);
	CPPUNIT_ASSERT(heapRec->m_del == 0);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId);

	RedRecord redRec(m_tableDef, &heapRec->m_rec);

	size_t size = 0;
	char *pName = NULL;
	redRec.readVarchar(TNT_STU_NAME_IDX, &pName, &size);
	CPPUNIT_ASSERT(strlen(name) == size);
	CPPUNIT_ASSERT(memcmp(name, pName, size) == 0);

	CPPUNIT_ASSERT(redRec.readInt(TNT_STU_SNO_IDX) == stuNo);
	CPPUNIT_ASSERT(redRec.readSmallInt(TNT_STU_AGE_IDX) == age);

	char *pSex = NULL;
	redRec.readChar(TNT_STU_SEX_IDX, &pSex, &size);
	CPPUNIT_ASSERT(strlen(sex) == size);
	CPPUNIT_ASSERT(memcmp(pSex, sex, size) == 0);

	CPPUNIT_ASSERT(redRec.readMediumInt(TNT_STU_CLASS_IDX) == clsNo);
	CPPUNIT_ASSERT(redRec.readBigInt(TNT_STU_GRADE_IDX) == grade);

	ctx->resetToSavepoint(sp);

	delete [] rec->m_data;
	delete rec;
	delete ctx;
}

void VersionPoolTestCase::testGetRollBackHeapRecBigDiff() {
	MemoryContext *ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
	u64 sp = ctx->setSavepoint();
	SubRecord *subRec = createEmptySubRecord(m_tableDef, REC_REDUNDANT, ctx);
	RowId rid = 0;
	u64 rollBackId = 0;
	u8 vTableIndex = 0;
	u64 txnId = 120;
	u8 tblIndex = 0;
	m_tableDef->m_id = 15;
	rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
	CPPUNIT_ASSERT(rid > 0);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;
	//创建第二条记录进行插入
	int stuNo = 1000;
	short age = 28;
	char *comment = (char *)ctx->calloc((VersionPool::VERSIONPOOL_SMALLDIFF_MAX + 1)*sizeof(char));
	memset(comment, 'a', VersionPool::VERSIONPOOL_SMALLDIFF_MAX);
	std::vector<u16> columns;
	std::vector<void *> datas;
	columns.push_back(TNT_STU_SNO_IDX);
	datas.push_back(&stuNo);
	columns.push_back(TNT_STU_AGE_IDX);
	datas.push_back(&age);
	columns.push_back(TNT_STU_COMMENT_IDX);
	datas.push_back(comment);
	subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
	rollBackId = rid;
	vTableIndex = tblIndex;
	txnId++;
	rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 120);
	CPPUNIT_ASSERT(rid > 0);
	CPPUNIT_ASSERT(rid != rollBackId);

	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	subRec = NULL;

	const char *name = "netease";
	const char *sex = "F";
	u32 clsNo = 6;
	u64 grade = 59;
	Record *rec = createStudentRec(202/*rowId*/, name, 1511 /*stuNo*/, 19 /*age*/, sex, clsNo, grade);
	//根据更新前像拼凑出完整记录
	MHeapRec *heapRec = m_versionPool->getRollBackHeapRec(m_session, m_tableDef, tblIndex, rec, rid);
	CPPUNIT_ASSERT(heapRec->m_del == 0);
	CPPUNIT_ASSERT(heapRec->m_vTableIndex == vTableIndex);
	CPPUNIT_ASSERT(heapRec->m_rollBackId == rollBackId);
	CPPUNIT_ASSERT(heapRec->m_txnId == txnId);

	RedRecord redRec(m_tableDef, &heapRec->m_rec);

	size_t size = 0;
	char *pName = NULL;
	redRec.readVarchar(TNT_STU_NAME_IDX, &pName, &size);
	CPPUNIT_ASSERT(strlen(name) == size);
	CPPUNIT_ASSERT(memcmp(name, pName, size) == 0);

	CPPUNIT_ASSERT(redRec.readInt(TNT_STU_SNO_IDX) == stuNo);
	CPPUNIT_ASSERT(redRec.readSmallInt(TNT_STU_AGE_IDX) == age);

	char *pSex = NULL;
	redRec.readChar(TNT_STU_SEX_IDX, &pSex, &size);
	CPPUNIT_ASSERT(strlen(sex) == size);
	CPPUNIT_ASSERT(memcmp(pSex, sex, size) == 0);

	CPPUNIT_ASSERT(redRec.readMediumInt(TNT_STU_CLASS_IDX) == clsNo);
	CPPUNIT_ASSERT(redRec.readBigInt(TNT_STU_GRADE_IDX) == grade);

	size = 0;
	char *pComment = NULL;
	redRec.readVarchar(TNT_STU_COMMENT_IDX, &pComment, &size);
	CPPUNIT_ASSERT(strlen(comment) == size);
	CPPUNIT_ASSERT(memcmp(comment, pComment, size) == 0);

	ctx->resetToSavepoint(sp);

	delete [] rec->m_data;
	delete rec;
	delete ctx;
}

void VersionPoolTestCase::testDefrag() {
	Table *table = NULL;
	try {
		m_db->createTable(m_session, "Blog", m_tableDef);
		table = m_db->openTable(m_session, "Blog");
		CPPUNIT_ASSERT(table != NULL);
	} catch (NtseException &) {
		CPPUNIT_FAIL("can't create table Blog");
		return;
	}

	MemoryContext *ctx = new MemoryContext(Limits::PAGE_SIZE, 20);
	u64 sp = ctx->setSavepoint();
	//总共分配4K给lob使用
	u32 lobSize = Limits::PAGE_SIZE >> 1;
	byte *lob = (byte *)ctx->alloc(lobSize);
	SubRecord *emptySub = createEmptySubRecord(m_tableDef, REC_REDUNDANT, ctx);
	SubRecord *subRec = NULL;
	RowId rid = 0;
	u64 rollBackId = 0;
	u8 vTableIndex = 0;
	u64 txnId = 0;
	u8 tblIndex = 0;

	uint size = 0;
	int i = 0;
	int sno = 0;
	short age = 0;
	char *comment = NULL;
	LobId lobIds[10];
	std::vector<u16> columns;
	std::vector<void *> datas;
	u64 sp1;
	for (i = 1; i <= 10; i++) {
		sp1 = ctx->setSavepoint();
		txnId = i*100; 
		rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId - 1, emptySub, 0, txnId);
		CPPUNIT_ASSERT(rid > 0);

		//创建第二条记录进行插入
		memset(lob, i, lobSize);
		lobIds[i - 1] = table->getRecords()->getLobStorage()->insert(m_session, lob, lobSize, false);
		sno = 1000 + i;
		age = 28 + i;
		size = VersionPool::VERSIONPOOL_SMALLDIFF_MAX - 5*(i - 1);
		comment = (char *)ctx->calloc((size + 1)*sizeof(char));
		memset(comment, 'a', size);

		columns.clear();
		datas.clear();
		columns.push_back(TNT_STU_SNO_IDX);
		datas.push_back(&sno);
		columns.push_back(TNT_STU_AGE_IDX);
		datas.push_back(&age);
		columns.push_back(TNT_STU_COMMENT_IDX);
		datas.push_back(comment);
		columns.push_back(TNT_STU_CTX_IDX);
		datas.push_back(lobIds + i - 1);
		subRec = createSubRecord(m_tableDef, REC_REDUNDANT, columns, datas, ctx);
		rollBackId = rid;
		vTableIndex = tblIndex;
		rid = m_versionPool->insert(m_session, tblIndex, m_tableDef, rollBackId, vTableIndex, txnId, subRec, 0, txnId + 1);
		CPPUNIT_ASSERT(rid > 0);
		CPPUNIT_ASSERT(rid != rollBackId);

		delete[] subRec->m_columns;
		delete[] subRec->m_data;
		delete subRec;
		subRec = NULL;
		ctx->resetToSavepoint(sp1);
	}
	delete[] emptySub->m_columns;
	delete[] emptySub->m_data;
	delete emptySub;

	for (i = 1; i <= 10; i++) {
		if (i % 2 == 1) {
			continue;
		}
		txnId = i*100 + 1;
		m_versionPool->rollBack(m_session, tblIndex, txnId);
		//回滚第1，3，5，7，9项
		table->getRecords()->getLobStorage()->del(m_session, lobIds[i - 1]);
	}

	uint targetSize = 50;
	uint additionalSize = 60;
	PagePool *pool = new PagePool(4, Limits::PAGE_SIZE);
	TNTIMPageManager *pageManager = new TNTIMPageManager(targetSize, additionalSize, pool);
	pool->registerUser(pageManager);
	pool->init();

//	DynHash<TblLob*, TblLob*, TblLobHasher, TblLobHasher, TblLobEqualer<TblLob*, TblLob*> > *exceptLobIds = 
//		new DynHash<TblLob*, TblLob*, TblLobHasher, TblLobHasher, TblLobEqualer<TblLob*, TblLob*> >(pageManager, PAGE_HASH_INDEX);
//	//第2，8项除外
// 	TblLob tblLob2(m_tableDef->m_id, lobIds[2]);
// 	TblLob tblLob8(m_tableDef->m_id, lobIds[8]);
// 	exceptLobIds->put(&tblLob2);
// 	exceptLobIds->put(&tblLob8);

	//另一个会话占用table
	/*Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("VersionPoolTestCase::testDefrag", conn);
	table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	table->lock(session, IL_X, -1, __FILE__, __LINE__);
	bool catchException = false;
	try {
		m_versionPool->defrag(m_session, vTableIndex, exceptLobIds);
		CPPUNIT_FAIL("table is locked, so it must throw exception");
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_LOCK_TIMEOUT);
		catchException = true;
	}
	CPPUNIT_ASSERT(catchException == true);
	table->unlock(session, IL_X);
	table->unlockMeta(session, IL_S);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);*/

	m_versionPool->defrag(m_session, vTableIndex);

	size = 0;
	byte *buf = NULL;
	u64 sp2 = 0;
	for (i = 0; i < 9; i++) {
		sp2 = ctx->setSavepoint();
		//注，lobSize必须保证lob插入的是小型大对象，如果插入的大型大对象，由于对应lobId的lob不存在，会报assert错误
		//所以这里测试的回收大对象只测试了小型大对象，但这并不影响正确性。
		//因为我们调的是抽象的大对象接口，大对象接口函数保证了无论对于大型大对象还是小型大对象操作的正确性
		buf = table->getRecords()->getLobStorage()->get(m_session, ctx, lobIds[i], &size);
	
		NTSE_ASSERT(0 == size);
	
		ctx->resetToSavepoint(sp2);
	}

	m_db->closeTable(m_session, table);
	ctx->resetToSavepoint(sp);

//	delete exceptLobIds;
	delete pool;
	delete pageManager;
	delete ctx;
}

void VersionPoolTestCase::testRollBack() {
	u8 tblIndex = 0;
	u64 txnId = 0;
	u64 beginTxnId = 1001;
	u64 endTxnId = 2000;
	for (txnId = beginTxnId; txnId <= endTxnId; txnId++) {
		if (txnId % 2 == 1) {
			m_versionPool->rollBack(m_session, tblIndex, txnId);
		}
	}

	TrxIdHashMap rollBackIds;
	m_versionPool->readAllRollBackTxnId(m_session, tblIndex, &rollBackIds);
	CPPUNIT_ASSERT(rollBackIds.getSize() == (endTxnId - beginTxnId + 1)/2);
	for (txnId = beginTxnId; txnId <= endTxnId; txnId++) {
		if (txnId % 2 == 1) {
			CPPUNIT_ASSERT(rollBackIds.get(txnId) != 0);
		} else {
			CPPUNIT_ASSERT(rollBackIds.get(txnId) == 0);
		}
	}
}