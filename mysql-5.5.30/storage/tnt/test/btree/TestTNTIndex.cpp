/**
 * 测试TNT索引统一接口
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#include "btree/TestTNTIndex.h"
#include "util/File.h"
#include "heap/Heap.h"
#include "misc/RecordHelper.h"
#include "Test.h"

#define DATA_SCALE	5000				/** 堆和索引数据规模 */

TNTIndiceTestBuilder::TNTIndiceTestBuilder() {
	m_tableDef = NULL;
	m_db = NULL;
	m_conn = NULL;
	m_session = NULL;
	m_tntIndice = NULL;
}

void TNTIndiceTestBuilder::init(uint tntBufSize) {
	m_config.setNtseBasedir("testdb");
	m_config.setTntBasedir("testdb");
	File dir(m_config.m_ntseConfig.m_basedir);
	dir.rmdir(true);
	dir.mkdir();
	
	//m_config.m_tntLogfileSize = 1024 * 1024 * 64;
	m_config.m_ntseConfig.m_logFileSize = 64 << 20;
	m_config.m_tntBufSize = tntBufSize;
	EXCPT_OPER(m_db = TNTDatabase::open(&m_config, true, false));

	m_conn = m_db->getNtseDb()->getConnection(false);
	m_session =  m_db->getNtseDb()->getSessionManager()->allocSession("TNTIndiceTestCase::setUp", m_conn);

	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	trx->trxAssignReadView();
	m_session->setTrans(trx);

	assert(!m_tableDef);
	m_tableDef = generateTableDef();
}

void TNTIndiceTestBuilder::clear() {
	TNTTransaction *trx = m_session->getTrans();
	m_db->getTransSys()->closeReadViewForMysql(trx);
	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	m_session->setTrans(NULL);

	m_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_session = NULL;
	m_db->getNtseDb()->freeConnection(m_conn);
	m_conn = NULL;

	m_db->close(true, true);
	delete m_db;
	m_db = NULL;

	delete m_tableDef;
	m_tableDef = NULL;

	File dir(m_config.m_ntseConfig.m_basedir);
	dir.rmdir(true);
}

TableDef* TNTIndiceTestBuilder::getTableDef() const {
	return  m_tableDef;
}

TableDef* TNTIndiceTestBuilder::generateTableDef() {
	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50, false, false);
	builder->addColumn("BankAccount", CT_BIGINT, false)->addColumn("Balance", CT_INT, false);

	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	builder->addIndex("NORMAL_INDEX", false, false, false, "UserId", 0, "BankAccount", 0, NULL);

	tableDef = builder->getTableDef();
	string tableDefFilePath(string(m_db->getNtseDb()->getConfig()->m_basedir) + "/" + 
		tableDef->m_name + Limits::NAME_TBLDEF_EXT);
	EXCPT_OPER(tableDef->writeFile(tableDefFilePath.c_str()));

	delete builder;

	return tableDef;
}

DrsHeap* TNTIndiceTestBuilder::createSmallHeap() throw(NtseException) {
	DrsHeap *heap;
	File oldsmallfile(m_tableDef->m_name);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db->getNtseDb(), m_tableDef->m_name, m_tableDef));
	EXCPT_OPER(heap = DrsHeap::open(m_db->getNtseDb(), m_session, m_tableDef->m_name, m_tableDef));

	return heap;
}

void TNTIndiceTestBuilder::closeSmallHeap(DrsHeap *heap) {
	if (heap) {
		EXCPT_OPER(heap->close(m_session, true));
		DrsHeap::drop(m_tableDef->m_name);
		delete heap;
	}
}

void TNTIndiceTestBuilder::buildHeap(DrsHeap *heap, uint size, bool keepSequence, bool hasSame) {
	Record *record;

	if (hasSame) {
		// 简单构造若干个相同数据的表
		for (uint i = 0; i < size; i++) {
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", i);
			//Record *rec = createRecord(0, number, name, number + ((u64)number << 32), (u32)(-1) - number);
			record = createSmallHeapRecord(0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			//m_smallHeap->insert(NULL, rec, NULL);
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
			freeRecord(record);
		}
		// 插入一条相同记录
		int i = size - 1;
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createSmallHeapRecord(0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
		return;
	}

	if (keepSequence) {
		// 顺序创建
		for (uint i = 0; i < size; i++) {
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", i);
			//Record *rec = createRecord(0, number, name, number + ((u64)number << 32), (u32)(-1) - number);
			record = createSmallHeapRecord(0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			//m_smallHeap->insert(NULL, rec, NULL);
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
			freeRecord(record);
		}
		return;
	}

	// 随机创建
	char *mark = (char *)alloca(size);
	memset(mark, 0, sizeof(char) * size);
	for (uint i = 0; i < size; i++) {
		char name[50];
		RowId rid;

		// 随机产生一个数字
		int num = rand() % size;
		int dir = rand() % 2;
		uint loopCount = 0;
		while (true) {
			assert(loopCount <= 2 * size);
			assert(num < (int)size);
			if (mark[num] == 0) {
				mark[num] = 1;
				break;
			}

			if (dir == 0) {	// 正向
				++num;
				if (num >= (int)size)
					num = 0;
			} else {	// 反向
				--num;
				if (num < 0)
					num = size - 1;
			}

			loopCount++;
		}

		System::snprintf_mine(name, 50, "Kenneth Tse Jr. %d \0", num);
		//Record *rec = createRecord(0, number, name, number + ((u64)number << 32), (u32)(-1) - number);
		record = createSmallHeapRecord(INVALID_ROW_ID, num, name, num + ((u64)i << 32), 
			(u32)(-1) - num);
		//m_smallHeap->insert(NULL, rec, NULL);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
	}
}

Record* TNTIndiceTestBuilder::createSmallHeapRecord(u64 rowid, u64 userid, const char *username, 
													u64 bankacc, u32 balance, RecFormat format) {
	RecordBuilder rb(m_tableDef, rowid, format);
	rb.appendBigInt(userid);
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	return rb.getRecord(m_tableDef->m_maxRecSize);
}

void TNTIndiceTestBuilder::createIndice() {
	string indexFileName(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_IDX_EXT);
	TNTIndice::drop(indexFileName.c_str());
	EXCPT_OPER(TNTIndice::create(indexFileName.c_str(), m_tableDef));
	CPPUNIT_ASSERT(File::isExist(indexFileName.c_str()));

	EXCPT_OPER(m_drsIndice = DrsIndice::open(m_db->getNtseDb(), m_session, 
		indexFileName.c_str(), m_tableDef, NULL));

	EXCPT_OPER(m_tntIndice = TNTIndice::open(m_db, m_session, &m_tableDef, NULL, m_drsIndice, NULL));
	NTSE_ASSERT(NULL != m_tntIndice);
}

void TNTIndiceTestBuilder::dropIndice() {
	string indexFileName(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_IDX_EXT);
	m_drsIndice->close(m_session, true);
	m_tntIndice->close(m_session);
	EXCPT_OPER(TNTIndice::drop(indexFileName.c_str()));
	CPPUNIT_ASSERT(!File::isExist(indexFileName.c_str()));
	delete m_tntIndice;
	delete m_drsIndice;
}

void TNTIndiceTestBuilder::createAllTwoIndex(DrsHeap *heap) {
	DrsIndice *indice = m_tntIndice->getDrsIndice();
	uint idxNo = 0;

	//创建唯一性索引
	idxNo = 0;
	IndexDef *indexDef1 = m_tableDef->getIndexDef(idxNo);
	indice->createIndexPhaseOne(m_session, indexDef1, m_tableDef, heap);
	EXCPT_OPER(m_tntIndice->createIndexPhaseOne(m_session, indexDef1, m_tableDef, heap));
	indice->createIndexPhaseTwo(indexDef1);
	m_tntIndice->createIndexPhaseTwo(m_session, indexDef1, idxNo);

	//创建非唯一性索引
	idxNo = 1;
	IndexDef *indexDef2 = m_tableDef->getIndexDef(idxNo);
	indice->createIndexPhaseOne(m_session, indexDef2, m_tableDef, heap);
	EXCPT_OPER(m_tntIndice->createIndexPhaseOne(m_session, indexDef2, m_tableDef, heap));
	indice->createIndexPhaseTwo(indexDef2);
	m_tntIndice->createIndexPhaseTwo(m_session, indexDef2, idxNo);

	CPPUNIT_ASSERT(2 == m_tntIndice->getIndexNum());
	CPPUNIT_ASSERT(2 == m_tntIndice->getMemIndice()->getIndexNum());
	CPPUNIT_ASSERT(2 == m_tntIndice->getDrsIndice()->getIndexNum());
}

//////////////////////////////////////////////////////////////////////////////

UniqueKeyLockTester::UniqueKeyLockTester(TNTDatabase *db, TNTIndice *indice, const char *name) 
	: Thread(name), m_db(db), m_indice(indice) {
		Database *ntseDb = m_db->getNtseDb();
		m_conn = ntseDb->getConnection(false);
		m_session = ntseDb->getSessionManager()->allocSession(name, m_conn);
}

UniqueKeyLockTester::~UniqueKeyLockTester() {
	Database *db = m_db->getNtseDb();
	db->getSessionManager()->freeSession(m_session);
	db->freeConnection(m_conn);
	m_session = NULL;
	m_conn = NULL;
}

void UniqueKeyLockTester::setLockInfo(vector<Record*> *rec, bool expectSucc, uint expectDupIndex) {
	m_recArr = rec;
	m_expectSucc = expectSucc;
	m_expectDupIndex = expectDupIndex;
}

void UniqueKeyLockTester::run() {
	uint dupIndex = (uint)-1;
	bool succeed = true;
	for (vector<Record*>::iterator it = m_recArr->begin(); succeed && it != m_recArr->end(); it++) {
		Record *rec = *it;
		succeed = m_indice->lockAllUniqueKey(m_session, rec, &dupIndex);
		NTSE_ASSERT(succeed == m_expectSucc);
	}
	
	if (!m_expectSucc) {
		NTSE_ASSERT(m_expectDupIndex == dupIndex);
	}
}

void UniqueKeyLockTester::unlockAll() {
	m_session->unlockAllUniqueKey();
}

///////////////////////////////////////////////////////////////////////////////////

const char* TNTIndiceTestCase::getName() {
	return "TNT Indice interface Test";
}
const char* TNTIndiceTestCase::getDescription() {
	return "Test TNT Indice interface";
}

bool TNTIndiceTestCase::isBig() {
	return false;
}

void TNTIndiceTestCase::setUp() {
	init();
}

void TNTIndiceTestCase::tearDown() {
	clear();
}

void TNTIndiceTestCase::testCreateDropOpenClose() {
	createIndice();

	CPPUNIT_ASSERT(NULL != m_tntIndice);
	CPPUNIT_ASSERT(NULL != m_tntIndice->getDrsIndice());
	CPPUNIT_ASSERT(NULL != m_tntIndice->getMemIndice());
	CPPUNIT_ASSERT(0 == m_tntIndice->getIndexNum());
	CPPUNIT_ASSERT(0 == m_tntIndice->getDrsIndice()->getIndexNum());
	CPPUNIT_ASSERT(0 == m_tntIndice->getMemIndice()->getIndexNum());
	
	dropIndice();
}

void TNTIndiceTestCase::testCreateAndDropIndex() {
	createIndice();

	DrsHeap *heap = createSmallHeap();
	buildHeap(heap, DATA_SCALE, false, false);
	
	DrsIndice *indice = m_tntIndice->getDrsIndice();
	//创建唯一性索引
	EXCPT_OPER(indice->createIndexPhaseOne(m_session, m_tableDef->getIndexDef(0), m_tableDef, heap));
	EXCPT_OPER(m_tntIndice->createIndexPhaseOne(m_session, m_tableDef->getIndexDef(0), m_tableDef, heap));
	indice->createIndexPhaseTwo(m_tableDef->getIndexDef(0));
	m_tntIndice->createIndexPhaseTwo(m_session, m_tableDef->getIndexDef(0), 0);

	CPPUNIT_ASSERT(1 == m_tntIndice->getIndexNum());
	CPPUNIT_ASSERT(1 == m_tntIndice->getMemIndice()->getIndexNum());
	CPPUNIT_ASSERT(1 == m_tntIndice->getDrsIndice()->getIndexNum());
	CPPUNIT_ASSERT(NULL != m_tntIndice->getTntIndex(0));

	//创建非唯一性索引
	EXCPT_OPER(indice->createIndexPhaseOne(m_session, m_tableDef->getIndexDef(1), m_tableDef, heap));
	EXCPT_OPER(m_tntIndice->createIndexPhaseOne(m_session, m_tableDef->getIndexDef(1), m_tableDef, heap));
	indice->createIndexPhaseTwo(m_tableDef->getIndexDef(1));
	m_tntIndice->createIndexPhaseTwo(m_session, m_tableDef->getIndexDef(1), 1);

	CPPUNIT_ASSERT(2 == m_tntIndice->getIndexNum());
	CPPUNIT_ASSERT(2 == m_tntIndice->getMemIndice()->getIndexNum());
	CPPUNIT_ASSERT(2 == m_tntIndice->getDrsIndice()->getIndexNum());
	CPPUNIT_ASSERT(NULL != m_tntIndice->getTntIndex(1));

	try
	{
		TNTIndex *tntIndex = m_tntIndice->getTntIndex(0);
		IndexDef *indexDef = m_tableDef->getIndexDef(0);

		SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
			m_tableDef->getIndexDef(0), KEY_PAD);
		TNTIdxScanHandle *scanHdl = tntIndex->beginScan(m_session, NULL, redKey, 
			false, true, true, TL_S, NULL);
		uint fetchCount = 0;

		while ((tntIndex->getNext(scanHdl))) {
			scanHdl->unlatchNtseRowBoth();
			++fetchCount;
		}
		scanHdl->unlatchNtseRowBoth();
		tntIndex->endScan(scanHdl);
		NTSE_ASSERT(fetchCount == DATA_SCALE);
	} catch (NtseException &e) {
		printf("Error: %s\n", e.getMessage());
		NTSE_ASSERT(false);
	}
	//删除索引
	indice->dropPhaseOne(m_session, 1);
	m_tntIndice->dropPhaseOne(m_session, 1);
	m_tntIndice->dropPhaseTwo(m_session, 1);
	indice->dropPhaseTwo(m_session, 1);

	CPPUNIT_ASSERT(1 == m_tntIndice->getIndexNum());
	CPPUNIT_ASSERT(1 == m_tntIndice->getMemIndice()->getIndexNum());
	CPPUNIT_ASSERT(1 == m_tntIndice->getDrsIndice()->getIndexNum());
	CPPUNIT_ASSERT(NULL != m_tntIndice->getTntIndex(0));

	indice->dropPhaseOne(m_session, 0);
	m_tntIndice->dropPhaseOne(m_session, 0);
	m_tntIndice->dropPhaseTwo(m_session, 0);
	indice->dropPhaseTwo(m_session, 0);

	CPPUNIT_ASSERT(0 == m_tntIndice->getIndexNum());
	CPPUNIT_ASSERT(0 == m_tntIndice->getMemIndice()->getIndexNum());
	CPPUNIT_ASSERT(0 == m_tntIndice->getDrsIndice()->getIndexNum());

	string heapPath(heap->getHeapFile()->getPath());
	closeSmallHeap(heap);
	DrsHeap::drop(heapPath.c_str());

	dropIndice();
}

void TNTIndiceTestCase::testInsert() {
	//准备堆数据和索引数据
	createIndice();
	DrsHeap *heap = createSmallHeap();
	buildHeap(heap, DATA_SCALE, true, false);
	createAllTwoIndex(heap);

	//插入测试键值
	RowId rid = DATA_SCALE * 2;
	u64 userId = DATA_SCALE * 2;
	const char *userName = "tntTestUser";
	u64 account = DATA_SCALE * 2;
	u32 balance = DATA_SCALE * 2;
	Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

	m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
	m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
	m_session->endTransaction(true, true);

	{
		//验证数据, 检查唯一性索引上能找到记录
		TNTIndex *index1 = m_tntIndice->getTntIndex(0);
		SubRecordBuilder sb(m_tableDef, KEY_PAD);
 		SubRecord *primarykey = sb.createSubRecordByName("UserId UserName BankAccount", 
			&userId, userName, &account);
		SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
			m_tableDef->getIndexDef(0), KEY_PAD);

		TNTIdxScanHandle *scanHandle1 = index1->beginScan(m_session, primarykey, redKey, 
			true, true, true, TL_S);
		uint fetchCount1 = 0;
		while (index1->getNext(scanHandle1)) {
			CPPUNIT_ASSERT(scanHandle1->getRowId() == rid);
			scanHandle1->unlatchNtseRowBoth();
			fetchCount1++;
		}
		scanHandle1->unlatchNtseRowBoth();
		CPPUNIT_ASSERT(fetchCount1 == 1);
		index1->endScan(scanHandle1);
		freeSubRecord(primarykey);
	}
	{
		//非唯一性索引上进行扫描只能获取到一条记录(插入的键值为最大键值)
		TNTIndex *index2 = m_tntIndice->getTntIndex(1);
		SubRecordBuilder sb2(m_tableDef, KEY_PAD, INVALID_ROW_ID);
		SubRecord *normalkey = sb2.createSubRecordByName("UserId BankAccount", &userId, &account);
		SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
			m_tableDef->getIndexDef(1), KEY_PAD);
		
		TNTIdxScanHandle *scanHandle2 = index2->beginScan(m_session, normalkey, redKey, 
			false, true, true, TL_S);
		uint fetchCount2 = 0;
		while (index2->getNext(scanHandle2)) {
			CPPUNIT_ASSERT(scanHandle2->getRowId() == rid);
			scanHandle2->unlatchNtseRowBoth();
			fetchCount2++;
		}
		scanHandle2->unlatchNtseRowBoth();
		CPPUNIT_ASSERT(fetchCount2 == 1);
		index2->endScan(scanHandle2);
		freeSubRecord(normalkey);
	}

	string heapPath(heap->getHeapFile()->getPath());
	closeSmallHeap(heap);
	DrsHeap::drop(heapPath.c_str());
	freeRecord(rec);

	dropIndice();
}

void TNTIndiceTestCase::testUpdate() {
	//准备堆数据和索引数据
	createIndice();
	DrsHeap *heap = createSmallHeap();
	buildHeap(heap, DATA_SCALE, true, false);
	createAllTwoIndex(heap);

	//插入测试键值
	RowId rid = DATA_SCALE * 2;
	u64 userId = DATA_SCALE * 2;
	const char *userName = "tntTestUser";
	u64 account = DATA_SCALE * 2;
	u32 balance = DATA_SCALE * 2;
	Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

	m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
	m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
	m_session->endTransaction(true, true);

	///////////////////////////////////////////////////////////////////////////////////
	//更新记录，使之进入TNT
	///////////////////////////////////////////////////////////////////////////////////
	SubRecordBuilder sbuilder(m_tableDef, REC_REDUNDANT, rid);
	u64 newUserId = DATA_SCALE * 3;
	//更新前像
	SubRecord *before = sbuilder.createSubRecordByName("UserId UserName BankAccount Balance",
		&userId, userName, &account, &balance);
 	//更新后像, 键值比前像要大，完整填充， 相等于前后像合并的结果
	SubRecord *after = sbuilder.createSubRecordByName("UserId UserName BankAccount Balance",
		&newUserId, userName, &account, &balance);
 	m_tntIndice->updateIndexEntries(m_session, before, after, true, 0);

	///////////////////////////////////////////////////////////////////////////////////
	///验证数据	
	///////////////////////////////////////////////////////////////////////////////////
	{
		//检查唯一性索引上能找到记录
		TNTIndex *index1 = m_tntIndice->getTntIndex(0);
		SubRecordBuilder sb(m_tableDef, KEY_PAD, INVALID_ROW_ID);		
		{
			//前像能扫描到， 但是被标记为删除
			IndexDef *indexDef = m_tableDef->getIndexDef(0);
			SubRecord *primarykey = sb.createSubRecordByName("UserId UserName BankAccount", 
				&userId, userName, &account);
			SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
				indexDef->m_numCols, indexDef->m_columns, KEY_PAD, indexDef->m_maxKeySize);

			TNTIdxScanHandle *scanHandle1 = index1->beginScan(m_session, primarykey, redKey,
				true, true, true, TL_S);			
			
			CPPUNIT_ASSERT(index1->getNext(scanHandle1));
			CPPUNIT_ASSERT(scanHandle1->getRowId() == rid);

			SubRecord beforeKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->calloc(indexDef->m_maxKeySize),
				indexDef->m_maxKeySize, rid);
			Record beforeRec(rid, REC_REDUNDANT, before->m_data, m_tableDef->m_maxRecSize);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(m_session, m_tableDef, indexDef, m_drsIndice->getLobStorage(), &beforeRec, &lobArray);
			}
			RecordOper::extractKeyRP(m_tableDef, indexDef, &beforeRec, &lobArray, &beforeKey);
			CPPUNIT_ASSERT(0 == RecordOper::compareKeyPP(m_tableDef, redKey, &beforeKey, indexDef));
			
			scanHandle1->unlatchNtseRowBoth();

			CPPUNIT_ASSERT(!index1->getNext(scanHandle1));
			scanHandle1->unlatchNtseRowBoth();
			index1->endScan(scanHandle1);
			freeSubRecord(primarykey);
		}
		{
			//后像也能扫描到， 但是标记不删除
			IndexDef *indexDef = m_tableDef->getIndexDef(0);
			SubRecord *primarykey = sb.createSubRecordByName("UserId UserName BankAccount", 
				&newUserId, userName, &account);
			SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
				indexDef->m_numCols, indexDef->m_columns, KEY_PAD, indexDef->m_maxKeySize);

			TNTIdxScanHandle *scanHandle1 = index1->beginScan(m_session, primarykey, redKey, 
				true, true, true, TL_S);			
			CPPUNIT_ASSERT(index1->getNext(scanHandle1));
			CPPUNIT_ASSERT(scanHandle1->getMVInfo().m_delBit == 0);
			CPPUNIT_ASSERT(scanHandle1->getRowId() == rid);

			SubRecord afterKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->calloc(indexDef->m_maxKeySize),
				indexDef->m_maxKeySize, rid);
			Record afterRec(rid, REC_REDUNDANT, after->m_data, m_tableDef->m_maxRecSize);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(m_session, m_tableDef, indexDef, m_drsIndice->getLobStorage(), &afterRec, &lobArray);
			}
			RecordOper::extractKeyRP(m_tableDef, indexDef, &afterRec, &lobArray, &afterKey);
			CPPUNIT_ASSERT(0 == RecordOper::compareKeyPP(m_tableDef, redKey, &afterKey, indexDef));
	
			scanHandle1->unlatchNtseRowBoth();

			CPPUNIT_ASSERT(!index1->getNext(scanHandle1));
			scanHandle1->unlatchNtseRowBoth();
			index1->endScan(scanHandle1);
			freeSubRecord(primarykey);
		}
	}
	{
		//非唯一性索引上进行范围扫描能获取到两条记录(前像小于后像，并且是最大的两项)
		{
			//使用前像测试正向扫描
			TNTIndex *index2 = m_tntIndice->getTntIndex(1);
			IndexDef *indexDef = m_tableDef->getIndexDef(1);
			SubRecordBuilder sb2(m_tableDef, KEY_PAD, INVALID_ROW_ID);
			SubRecord *normalkey = sb2.createSubRecordByName("UserId BankAccount", &userId, &account);
			SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
				indexDef->m_numCols, indexDef->m_columns, KEY_PAD, indexDef->m_maxKeySize);
			TNTIdxScanHandle *scanHandle2 = index2->beginScan(m_session, normalkey, redKey, 
				false, true, true, TL_S);

			//前像能扫描到， 但是被标记为删除
			CPPUNIT_ASSERT(index2->getNext(scanHandle2));
			CPPUNIT_ASSERT(scanHandle2->getRowId() == rid);

			SubRecord beforeKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->calloc(indexDef->m_maxKeySize),
				indexDef->m_maxKeySize, rid);
			Record beforeRec(rid, REC_REDUNDANT, before->m_data, m_tableDef->m_maxRecSize);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(m_session, m_tableDef, indexDef, m_drsIndice->getLobStorage(), &beforeRec, &lobArray);
			}
			RecordOper::extractKeyRP(m_tableDef, indexDef, &beforeRec, &lobArray, &beforeKey);
			CPPUNIT_ASSERT(0 == RecordOper::compareKeyPP(m_tableDef, redKey, &beforeKey, indexDef));

			scanHandle2->unlatchNtseRowBoth();

			//后像也能扫描到， 但是标记不删除
			CPPUNIT_ASSERT(index2->getNext(scanHandle2));
			CPPUNIT_ASSERT(scanHandle2->getMVInfo().m_delBit == 0);
			CPPUNIT_ASSERT(scanHandle2->getRowId() == rid);

			SubRecord afterKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->calloc(indexDef->m_maxKeySize),
				indexDef->m_maxKeySize, rid);
			Record afterRec(rid, REC_REDUNDANT, after->m_data, m_tableDef->m_maxRecSize);
			lobArray.clear();
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(m_session, m_tableDef, indexDef, m_drsIndice->getLobStorage(), &afterRec, &lobArray);
			}
			RecordOper::extractKeyRP(m_tableDef, indexDef, &afterRec, &lobArray, &afterKey);
			CPPUNIT_ASSERT(0 == RecordOper::compareKeyPP(m_tableDef, redKey, &afterKey, indexDef));
		
			scanHandle2->unlatchNtseRowBoth();

			CPPUNIT_ASSERT(!index2->getNext(scanHandle2));
			scanHandle2->unlatchNtseRowBoth();
			index2->endScan(scanHandle2);
			freeSubRecord(normalkey);
		}
		{
			//使用后像测试反向扫描
			TNTIndex *index2 = m_tntIndice->getTntIndex(1);
			IndexDef *indexDef = m_tableDef->getIndexDef(1);
			SubRecordBuilder sb2(m_tableDef, KEY_PAD, INVALID_ROW_ID);
			SubRecord *normalkey = sb2.createSubRecordByName("UserId BankAccount", &newUserId, &account);
			SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
				indexDef->m_numCols, indexDef->m_columns, KEY_PAD, indexDef->m_maxKeySize);
			TNTIdxScanHandle *scanHandle2 = index2->beginScan(m_session, normalkey, redKey, 
				false, false, true, TL_S);

			//后像能扫描到，但是标记不删除
			CPPUNIT_ASSERT(index2->getNext(scanHandle2));
			CPPUNIT_ASSERT(scanHandle2->getMVInfo().m_delBit == 0);
			CPPUNIT_ASSERT(scanHandle2->getRowId() == rid);

			SubRecord afterKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->calloc(indexDef->m_maxKeySize),
				indexDef->m_maxKeySize, rid);
			Record afterRec(rid, REC_REDUNDANT, after->m_data, m_tableDef->m_maxRecSize);
			Array<LobPair*> lobArray;
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(m_session, m_tableDef, indexDef, m_drsIndice->getLobStorage(), &afterRec, &lobArray);
			}
			RecordOper::extractKeyRP(m_tableDef, indexDef, &afterRec, &lobArray, &afterKey);
			CPPUNIT_ASSERT(0 == RecordOper::compareKeyPP(m_tableDef, redKey, &afterKey, indexDef));
			
			scanHandle2->unlatchNtseRowBoth();

			//前像能扫描到， 但是被标记为删除
			CPPUNIT_ASSERT(index2->getNext(scanHandle2));
			CPPUNIT_ASSERT(scanHandle2->getRowId() == rid);

			SubRecord beforeKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, (byte *)m_session->getMemoryContext()->calloc(indexDef->m_maxKeySize),
				indexDef->m_maxKeySize, rid);
			Record beforeRec(rid, REC_REDUNDANT, before->m_data, m_tableDef->m_maxRecSize);
			lobArray.clear();
			if (indexDef->hasLob()) {
				RecordOper::extractLobFromR(m_session, m_tableDef, indexDef, m_drsIndice->getLobStorage(), &beforeRec, &lobArray);
			}
			RecordOper::extractKeyRP(m_tableDef, indexDef, &beforeRec, &lobArray, &beforeKey);
			CPPUNIT_ASSERT(0 == RecordOper::compareKeyPP(m_tableDef, redKey, &beforeKey, indexDef));
	
			scanHandle2->unlatchNtseRowBoth();

			index2->endScan(scanHandle2);
			freeSubRecord(normalkey);
		}
	}

	freeSubRecord(before);
	freeSubRecord(after);

	string heapPath(heap->getHeapFile()->getPath());
	closeSmallHeap(heap);
	DrsHeap::drop(heapPath.c_str());
	freeRecord(rec);

	dropIndice();
}

void TNTIndiceTestCase::testDelete() {
	//准备堆数据和索引数据
	createIndice();
	DrsHeap *heap = createSmallHeap();
	buildHeap(heap, DATA_SCALE, true, false);
	createAllTwoIndex(heap);

	//插入测试键值
	RowId rid = DATA_SCALE * 2;
	u64 userId = DATA_SCALE * 2;
	const char *userName = "tntTestUser";
	u64 account = DATA_SCALE * 2;
	u32 balance = DATA_SCALE * 2;
	Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
	uint dupIndex = (uint)-1;
	
	m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
	CPPUNIT_ASSERT(m_tntIndice->getDrsIndice()->insertIndexEntries(m_session, rec, &dupIndex));
	m_session->endTransaction(true);

	//删除记录
	m_tntIndice->deleteIndexEntries(m_session, rec, 1);

	//验证数据
	TNTIndex *index1 = m_tntIndice->getTntIndex(0);
	
	{
		//外存索引的记录还在(没做purge)
		DrsIndex *drsIndex1 = m_tntIndice->getDrsIndex(0);
		RowId outRowId = INVALID_ROW_ID;
		//RowLockHandle *rlHandle;

		SubRecordBuilder sb(m_tableDef, KEY_PAD, INVALID_ROW_ID);
		SubRecord *primarykey = sb.createSubRecordByName("UserId UserName BankAccount", &userId, userName, &account);
		CPPUNIT_ASSERT(drsIndex1->getByUniqueKey(m_session, primarykey, None, &outRowId, NULL, NULL, NULL));
		//m_session->unlockRow(&rlHandle);
		freeSubRecord(primarykey);
	}
	{
		//在内存索引中记录已经被标	记为删除
		{
			//检查唯一性索引
			TNTIndex *index1 = m_tntIndice->getTntIndex(0);

			SubRecordBuilder sb(m_tableDef, KEY_PAD, INVALID_ROW_ID);
 			SubRecord *primarykey = sb.createSubRecordByName("UserId UserName BankAccount", &userId, userName, &account);
			SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
				m_tableDef->getIndexDef(0), KEY_PAD);
			TNTIdxScanHandle *scanHandle1 = index1->beginScan(m_session, primarykey, redKey, 
				true, true, true, TL_S);
			
			CPPUNIT_ASSERT(index1->getNext(scanHandle1));
			CPPUNIT_ASSERT(scanHandle1->getMVInfo().m_delBit == 1);
			CPPUNIT_ASSERT(scanHandle1->getRowId() == rid);
			scanHandle1->unlatchNtseRowBoth();

			CPPUNIT_ASSERT(!index1->getNext(scanHandle1));
			scanHandle1->unlatchNtseRowBoth();
			index1->endScan(scanHandle1);
			freeSubRecord(primarykey);
		}
		{
			//检查非唯一性索引
			TNTIndex *index2 = m_tntIndice->getTntIndex(1);
			SubRecordBuilder sb2(m_tableDef, KEY_PAD, INVALID_ROW_ID);
			SubRecord *normalkey = sb2.createSubRecordByName("UserId BankAccount", &userId, &account);
			SubRecord *redKey = IndexKey::allocSubRecord(m_session->getMemoryContext(), 
				m_tableDef->getIndexDef(0), KEY_PAD);
			TNTIdxScanHandle *scanHandle2 = index2->beginScan(m_session, normalkey, redKey, 
				false, true, true, TL_S);

			CPPUNIT_ASSERT(index2->getNext(scanHandle2));
			CPPUNIT_ASSERT(scanHandle2->getMVInfo().m_delBit == 1);
			CPPUNIT_ASSERT(scanHandle2->getRowId() == rid);
			scanHandle2->unlatchNtseRowBoth();

			CPPUNIT_ASSERT(!index2->getNext(scanHandle2));
			scanHandle2->unlatchNtseRowBoth();
			index2->endScan(scanHandle2);
			freeSubRecord(normalkey);
		}
	}

	string heapPath(heap->getHeapFile()->getPath());
	closeSmallHeap(heap);
	DrsHeap::drop(heapPath.c_str());
	freeRecord(rec);

	dropIndice();
}

void TNTIndiceTestCase::testCheckDup() {
	//准备堆数据和索引数据
	createIndice();
	DrsHeap *heap = createSmallHeap();
	buildHeap(heap, DATA_SCALE, true, false);
	createAllTwoIndex(heap);

	vector<Record*> recArr;
	vector<Record*> upRecArr;
	//插入测试键值
	uint range = 800;
	for (uint i = 1; i <= range; i++) {
		RowId rid = DATA_SCALE + i;
		u64 userId = DATA_SCALE + i;
		const char *userName = "tntTestUser";
		u64 account = DATA_SCALE + i;
		u32 balance = DATA_SCALE + i;
			
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		m_session->endTransaction(true, true);

		recArr.push_back(rec);
	}

	//更新部分值到内存索引中
	for(uint i = 1; i <= range; i++) {
		RowId rid = DATA_SCALE + i;
		u64 userId = DATA_SCALE*3 + i;
		const char *userName = "tntTestUser";
		u64 account = DATA_SCALE + i;
		u32 balance = DATA_SCALE + i;

		u16 upCols[3] = {0,1,2};
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		SubRecord newSubRec(REC_REDUNDANT, 3, upCols, rec->m_data, m_tableDef->m_maxRecSize);
		newSubRec.m_rowId = rec->m_rowId;
		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getMemIndice()->insertIndexEntries(m_session, &newSubRec, 0);
		m_session->endTransaction(true, true);

		upRecArr.push_back(rec);
	}

	u16 uniqueNo[] = { 0 };
	//测试已插入键值会发生键值重复
	for (uint i = 0; i < range; i++) {
		uint dupIndex = (uint)-1;
		Record *rec = recArr[i];
		Record *upRec = upRecArr[i];
		CPPUNIT_ASSERT(m_tntIndice->checkDuplicate(m_session, rec, 1, uniqueNo, &dupIndex, NULL));
		CPPUNIT_ASSERT(m_tntIndice->checkDuplicate(m_session, upRec, 1, uniqueNo, &dupIndex, NULL));
		CPPUNIT_ASSERT_EQUAL(dupIndex, (uint)0);
	}

	//测试没插入过键值不会发生键值重复
	for (uint i = 1; i < range; i++) {
		RowId rid = DATA_SCALE + range + i;
		u64 userId = DATA_SCALE + range + i;
		const char *userName = "tntTestUser";
		u64 account = DATA_SCALE + range + i;
		u32 balance = DATA_SCALE + range + i;
		uint dupIndex = (uint)-1;
		
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		CPPUNIT_ASSERT(!m_tntIndice->checkDuplicate(m_session, rec, 1, uniqueNo, &dupIndex, NULL));
		freeRecord(rec);
	}

	for (vector<Record*>::iterator it = recArr.begin(); it != recArr.end(); it++) {
		freeRecord(*it);
	}
	for (vector<Record*>::iterator it = upRecArr.begin(); it != upRecArr.end(); it++) {
		freeRecord(*it);
	}

	string heapPath(heap->getHeapFile()->getPath());
	closeSmallHeap(heap);
	DrsHeap::drop(heapPath.c_str());
	dropIndice();
}

void TNTIndiceTestCase::testUniqueKeyLock() {
	//准备堆数据和索引数据
	createIndice();
	DrsHeap *heap = createSmallHeap();
	buildHeap(heap, DATA_SCALE, true, false);
	createAllTwoIndex(heap);

	for (uint i = 0; i < 10; i++) {
		uint testValue = (uint)System::random() % DATA_SCALE;

		UniqueKeyLockTester tester1(m_db, m_tntIndice, "Tester1");
		UniqueKeyLockTester tester2(m_db, m_tntIndice, "Tester2");
		UniqueKeyLockTester tester3(m_db, m_tntIndice, "Tester3");

		//插入测试键值
		RowId rid = testValue;
		u64 userId = testValue;
		const char *userName = "tntTestUser";
		u64 account = testValue;
		u32 balance = testValue;
		vector<Record*> recArr;
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		recArr.push_back(rec);
		Record *rec2 = createSmallHeapRecord(rid + DATA_SCALE, userId + DATA_SCALE, userName, 
			account + DATA_SCALE, balance + DATA_SCALE, REC_REDUNDANT);
		recArr.push_back(rec2);

		//测试者1加锁
		tester1.setLockInfo(&recArr, true);
		tester1.start();
		Thread::msleep(1000);

		//测试者2加锁失败
		tester2.setLockInfo(&recArr, false, 0);
		tester2.start();
		Thread::msleep(1000);

		//测试者1放锁
		tester1.unlockAll();

		//测试者3加锁成功
		tester3.setLockInfo(&recArr, true);
		tester3.start();
		Thread::msleep(1000);
		tester3.unlockAll();

		tester1.join(60000);
		tester2.join(60000);
		tester3.join(60000);

		for (vector<Record*>::iterator it = recArr.begin(); it != recArr.end(); it++) {
			freeRecord(*it);
		}
	} 

	string heapPath(heap->getHeapFile()->getPath());
	closeSmallHeap(heap);
	DrsHeap::drop(heapPath.c_str());
	dropIndice();
}


/////////////////////////////////////////////////////////

const char* TNTIndexTestCase::getName() {
	return "TNT Index Test";
}
const char* TNTIndexTestCase::getDescription() {
	return "Test TNT Index interface";
}

bool TNTIndexTestCase::isBig() {
	return false;
}

void TNTIndexTestCase::setUp() {
	init(6400);

	m_heap = NULL;
	string indexFileName(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_IDX_EXT);
	TNTIndice::drop(indexFileName.c_str());
	EXCPT_OPER(TNTIndice::create(indexFileName.c_str(), m_tableDef));
	CPPUNIT_ASSERT(File::isExist(indexFileName.c_str()));

	EXCPT_OPER(m_drsIndice = DrsIndice::open(m_db->getNtseDb(), m_session, 
		indexFileName.c_str(), m_tableDef, NULL));

	//double checker何时释放？？？
	m_hashIndex = new HashIndex(m_db->getTNTIMPageManager());
	EXCPT_OPER(m_tntIndice = TNTIndice::open(m_db, m_session, &m_tableDef, NULL, m_drsIndice, m_hashIndex));
	NTSE_ASSERT(NULL != m_tntIndice);


	string heapPath(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_HEAP_EXT);
	DrsHeap::drop(heapPath.c_str());
	m_heap = createSmallHeap();
	
	//buildHeap(m_heap, DATA_SCALE, false, false);

	createAllTwoIndex(m_heap);
}

void TNTIndexTestCase::tearDown() {
	string heapPath(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_HEAP_EXT);
	closeSmallHeap(m_heap);
	m_heap = NULL;
	DrsHeap::drop(heapPath.c_str());

	string indexFileName(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_IDX_EXT);
	m_drsIndice->close(m_session, true);
	m_tntIndice->close(m_session);
	delete m_tntIndice;
	delete m_drsIndice;
	EXCPT_OPER(TNTIndice::drop(indexFileName.c_str()));
	CPPUNIT_ASSERT(!File::isExist(indexFileName.c_str()));
	delete m_hashIndex;
	clear();
}

void TNTIndexTestCase::testUniqueFetch() {
	const uint size = 3;
	vector<Record*> oldRows;
	vector<Record*> newRows;

	//插入数据
	for(uint i = 0; i < size ; i++) {
		McSavepoint msc(m_session->getMemoryContext());

		RowId rid = i;
		u64 userId = i;
		const char * userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		oldRows.push_back(rec);
		if(i == 1)
			continue;
		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		m_session->endTransaction(true, true);
	}

	//更新记录，进入TNT
	for(uint i = 2; i < size; i++) {
		RowId rid = i;
		u64 userId = i + 10;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *newRec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		Record *oldRec = oldRows[i];

		SubRecord before(REC_REDUNDANT, 0, NULL, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, 0, NULL, newRec->m_data, newRec->m_size, newRec->m_rowId);
		m_tntIndice->updateIndexEntries(m_session, &before, &after, true, 0);

		newRows.push_back(newRec);
	}

	//再将记录更新回原来的值
	for(uint i = 2; i < size; i++) {
		RowId rid = i;
		u64 userId = i;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *newRec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		Record *oldRec = newRows[0];

		u16 colNum = 1;
		u16 cols[1] = {0};
		SubRecord before(REC_REDUNDANT, colNum, cols, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, colNum, cols, newRec->m_data, newRec->m_size, newRec->m_rowId);
		m_tntIndice->updateIndexEntries(m_session, &before, &after, false, 0);

		freeRecord(newRec);
	}


	
	TLockMode testLockModes[] = { TL_S };
	const u8 numTestModes = sizeof(testLockModes) / sizeof(TLockMode);
	//开始扫描
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNT Index Scan", m_conn);
	TNTTransaction *trx = m_db->getTransSys()->allocTrx(testLockModes[0]);
	trx->startTrxIfNotStarted(m_conn);
	session->setTrans(trx);

	IndexDef *indexDef = m_tableDef->getIndexDef(0);
	SubRecord outKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, 
		(byte*)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);
	//构建searchKey
	RowId rid = INVALID_ROW_ID;
	u64 userId = 1;
	const char *userName = "tntTestUser";
	u64 account = 1;
	u32 balance = 1;	

	SubRecordBuilder srBuilder(m_tableDef, KEY_PAD, rid);
	SubRecord *key = srBuilder.createSubRecordByName("UserId UserName BankAccount", &userId, userName, &account );
	SubRecord *searchKey = MIndexKeyOper::allocSubRecord(session->getMemoryContext(), key, indexDef);

	searchKey->m_numCols = indexDef->m_numCols;
	searchKey->m_columns = indexDef->m_columns;
	freeSubRecord(key);

	TLockMode lockMode = testLockModes[0];
	TNTIndex *index = m_tntIndice->getTntIndex(0);
	TNTIdxScanHandle *scan = index->beginScan(session, searchKey, &outKey, 
		true, true, true, lockMode);
 
	//一直读到取不到数据为止
	uint gotRows = 0;
	while(index->getNext(scan)){
		scan->unlatchNtseRowBoth();
		gotRows++;
	}
	scan->unlatchNtseRowBoth();
	assert(gotRows == 0);
	index->endScan(scan);

	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);

	session->setTrans(NULL);

	m_db->getNtseDb()->getSessionManager()->freeSession(session);


	//清理
	for(uint i = 0; i < size ; i++ )
		freeRecord(oldRows[i]);

	freeRecord(newRows[0]);

}

void TNTIndexTestCase::testRangeScan() {
	const uint size = 800;
	vector<Record*> oldRows;
	//vector<Record*> newRows;
	//插入测试数据
	for (uint i = 0; i < size; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		RowId rid = i;
		u64 userId = i;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		oldRows.push_back(rec);

		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		m_session->endTransaction(true, true);
	}

	//更新使记录全部进入TNT
 	for (uint i = 0; i < size; i++) {
		RowId rid = i;
		u64 userId = i + size;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *newRec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		Record *oldRec = oldRows[i];

		SubRecord before(REC_REDUNDANT, 0, NULL, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, 0, NULL, newRec->m_data, newRec->m_size, newRec->m_rowId);
 		m_tntIndice->updateIndexEntries(m_session, &before, &after, true, 0);
		
		//newRows.push_back(newRec);
		freeRecord(newRec);
 	}

	TLockMode testLockModes[] = { TL_X, TL_S };
	const u8 numTestModes = sizeof(testLockModes) / sizeof(TLockMode);

	//正向扫
	for (u8 id = 0; id < numTestModes; id++) {
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNT Index Scan", m_conn);
		TNTTransaction *trx = m_db->getTransSys()->allocTrx(testLockModes[id]);
		trx->startTrxIfNotStarted(m_conn);
		session->setTrans(trx);
		
		IndexDef *indexDef = m_tableDef->getIndexDef(0);
		SubRecord outKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, 
			(byte*)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);

		TLockMode lockMode = testLockModes[id];
		TNTIndex *index = m_tntIndice->getTntIndex(0);
		TNTIdxScanHandle *scan = index->beginScan(session, NULL, &outKey, 
			false, true, true, lockMode);
 		for (uint i = 0; i < size; i++) {
			NTSE_ASSERT(!trx->isRowLocked(oldRows[i]->m_rowId, m_tableDef->m_id, lockMode));
			NTSE_ASSERT(index->getNext(scan));
			NTSE_ASSERT(oldRows[i]->m_rowId == scan->getRowId());
			NTSE_ASSERT(trx->isRowLocked(oldRows[i]->m_rowId, m_tableDef->m_id, lockMode));
			scan->unlatchNtseRowBoth();
		}

		index->endScan(scan);

		trx->commitTrx(CS_NORMAL);
		m_db->getTransSys()->freeTrx(trx);

		session->setTrans(NULL);

		m_db->getNtseDb()->getSessionManager()->freeSession(session);
	}
	
	//反向扫
	for (u8 id = 0; id < numTestModes; id++) {
		Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNT Index Scan", m_conn);
		TNTTransaction *trx = m_db->getTransSys()->allocTrx(testLockModes[id]);
		trx->startTrxIfNotStarted(m_conn);
		session->setTrans(trx);

		IndexDef *indexDef = m_tableDef->getIndexDef(0);
		SubRecord outKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, 
			(byte*)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);

		TLockMode lockMode = testLockModes[id];
		TNTIndex *index = m_tntIndice->getTntIndex(0);
		TNTIdxScanHandle *scan = index->beginScan(session, NULL, &outKey, 
			false, false, true, lockMode);
		//扫内存项
		for (uint i = 0; i < size; i++) {
			NTSE_ASSERT(!trx->isRowLocked(oldRows[size - 1 - i]->m_rowId, m_tableDef->m_id, lockMode));
			NTSE_ASSERT(index->getNext(scan));
			NTSE_ASSERT(oldRows[size - 1 - i]->m_rowId == scan->getRowId());
			NTSE_ASSERT(trx->isRowLocked(oldRows[size - 1 - i]->m_rowId, m_tableDef->m_id, lockMode));
			scan->unlatchNtseRowBoth();
		}
		//扫外存项
		for (uint i = 0; i < size; i++) {
//			NTSE_ASSERT(!trx->isRowLocked(oldRows[size - 1 - i]->m_rowId, m_tableDef->m_id, lockMode));
			NTSE_ASSERT(index->getNext(scan));
			NTSE_ASSERT(oldRows[size - 1 - i]->m_rowId == scan->getRowId());
			NTSE_ASSERT(trx->isRowLocked(oldRows[size - 1 - i]->m_rowId, m_tableDef->m_id, lockMode));
			scan->unlatchNtseRowBoth();
		}
		index->endScan(scan);

		trx->commitTrx(CS_NORMAL);
		m_db->getTransSys()->freeTrx(trx);

		session->setTrans(NULL);

		m_db->getNtseDb()->getSessionManager()->freeSession(session);
	}

	for(uint i = 0; i < size ; i++ )
		freeRecord(oldRows[i]);
}

void TNTIndexTestCase::testPurge() {
	const uint size = 4000;
	vector<Record*> oldRows;
	vector<Record*> newRows;
	//插入测试数据
	for (uint i = 0; i < size; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		RowId rid = i;
		u64 userId = i+1;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		oldRows.push_back(rec);

		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		RowIdVersion version = 100;
		m_hashIndex->insert(rid, m_session->getTrans()->getTrxId(), version, HIT_TXNID);
		m_session->endTransaction(true, true);
	}
	//更新使记录全部进入TNT
 	for (uint i = 0; i < size; i++) {
		RowId rid = i;
		u64 userId = i+1;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *newRec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		Record *oldRec = oldRows[i];

		SubRecord before(REC_REDUNDANT, 0, NULL, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, 0, NULL, newRec->m_data, newRec->m_size, newRec->m_rowId);
 		m_tntIndice->updateIndexEntries(m_session, &before, &after, true, 0);
		
		newRows.push_back(newRec);
 	}

	TNTIndex *primaryIndex = m_tntIndice->getTntIndex(0);
	u64 keyCount = primaryIndex->purge(m_session, NULL);
	
	assert(keyCount == size);

	for (uint i = 0; i < size; i++) {
		McSavepoint mcs(m_session->getMemoryContext());
		
		RowId rid = DATA_SCALE + i;
		u64 userId = DATA_SCALE + i;
		const char *userName = "tntTestUser";
		u64 account = DATA_SCALE + i;
		u32 balance = DATA_SCALE + i;
		
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		m_session->endTransaction(true, true);

		freeRecord(rec);
	}

	for (vector<Record*>::iterator it = oldRows.begin(); it != oldRows.end(); it++) {
		Record *rec = *it;
		freeRecord(rec);
	}
	for (vector<Record*>::iterator it = newRows.begin(); it != newRows.end(); it++) {
		Record *rec = *it;
		freeRecord(rec);
	}
}



/////////////////////////////////////////
MIndexWorker::MIndexWorker(const char *name, u16 jobNo, u8 indexNo, const TableDef *tableDef,  
						   TNTDatabase *db, TNTIndice *tntIndice, TNTIndiceTestBuilder *testBuilder)
						   : Thread(name) {
	m_jobNo = jobNo;
	m_indexNo = indexNo;
	m_tableDef = tableDef;
	m_indexDef = tableDef->getIndexDef(indexNo);
	m_db = db;
	m_tntIndice = tntIndice;
	m_testBuilder = testBuilder;
	m_conn = m_db->getNtseDb()->getConnection(false, name);
	m_session = m_db->getNtseDb()->getSessionManager()->allocSession(name, m_conn);
	
	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	m_session->setTrans(trx);
}

MIndexWorker::~MIndexWorker() {
	TNTTransaction *trx = m_session->getTrans();
	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);

	m_db->getNtseDb()->getSessionManager()->freeSession(m_session);
	m_db->getNtseDb()->freeConnection(m_conn);
	m_session = NULL;
	m_conn = NULL;
}

void MIndexWorker::run() {
	TNTIndex *index = m_tntIndice->getTntIndex(m_indexNo);
	BLinkTree *bltree = (BLinkTree*)index->getMIndex();

	Thread::msleep(2000);

	uint testTimes = 50000;
	for (uint i = 0; i < testTimes; i++) {
		McSavepoint sp(m_session->getMemoryContext());

		//插入键值
		uint kv = m_jobNo * testTimes + i;
		SubRecord *redKey = createTestKey(kv);
		bltree->insert(m_session, redKey, 0);

		//构造查找键
		SubRecord *padKey = MIndexKeyOper::allocSubRecord(m_session->getMemoryContext(), m_indexDef, KEY_PAD);
		Record rec(redKey->m_rowId, REC_REDUNDANT, redKey->m_data, redKey->m_size);
		RecordOper::extractKeyRP(m_tableDef, m_indexDef, &rec, NULL, padKey);
		
		//验证数据
		checkKeyInTree(bltree, padKey, 0);
		
		//标记删除
		RowIdVersion version = INVALID_VERSION;
		NTSE_ASSERT(bltree->delByMark(m_session, redKey, &version));
		
		//验证数据
		checkKeyInTree(bltree, padKey, 1);

		freeSubRecord(redKey);

		Thread::msleep(2);
	}
}

void MIndexWorker::checkKeyInTree(BLinkTree *bltree, SubRecord *padKey, u8 delBit) {
	//查询
	bool forward = ((uint)System::random() % 2 == 0);
	MIndexRangeScanHandle* scanHandle = (MIndexRangeScanHandle*)bltree->beginScan(m_session, 
		padKey, forward, true, None, NULL, TL_S);
	uint count = 0;
	while (bltree->getNext(scanHandle)) {
		MIndexScanInfoExt *scanInfo = (MIndexScanInfoExt*)scanHandle->getScanInfo();
		int cmp = RecordOper::compareKeyNP(m_tableDef, scanInfo->m_readKey, padKey, m_indexDef);
		if (cmp != 0 && (forward ^ (cmp < 0))) {
			break;
		}
		const KeyMVInfo &keyMVInfo = scanHandle->getMVInfo();
		NTSE_ASSERT(scanHandle->getRowId() == padKey->m_rowId);
		NTSE_ASSERT(keyMVInfo.m_delBit == (u8)delBit);
		scanHandle->unlatchLastRow();
		count++;
		scanInfo->moveCursor();
	}
	scanHandle->unlatchLastRow();
	NTSE_ASSERT(count == 1);
	
	bltree->endScan(scanHandle);
	scanHandle = NULL;
}

SubRecord* MIndexWorker::createTestKey(uint kv) {
	SubRecordBuilder sbBuilder(m_tableDef, REC_REDUNDANT, kv);
	return sbBuilder.createSubRecordByName("UserId UserName BankAccount", &kv, "testKey", &kv);
}

const char* MIndexOPStabilityTestCase::getName() {
	return "Memory Index Stability Test";
}

const char* MIndexOPStabilityTestCase::getDescription(){
	return "Multi thread DML test of memory index stability";
}

bool MIndexOPStabilityTestCase::isBig() {
	return true;
}

void MIndexOPStabilityTestCase::setUp() {
	init(sizeof(int) == 4 ? 12800 : 64000);

	m_heap = NULL;
	string indexFileName(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_IDX_EXT);
	TNTIndice::drop(indexFileName.c_str());
	EXCPT_OPER(TNTIndice::create(indexFileName.c_str(), m_tableDef));
	CPPUNIT_ASSERT(File::isExist(indexFileName.c_str()));

	EXCPT_OPER(m_drsIndice = DrsIndice::open(m_db->getNtseDb(), m_session, 
		indexFileName.c_str(), m_tableDef, NULL));

	EXCPT_OPER(m_tntIndice = TNTIndice::open(m_db, m_session, &m_tableDef, NULL, m_drsIndice, NULL));
	NTSE_ASSERT(NULL != m_tntIndice);

	string heapPath(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_HEAP_EXT);
	DrsHeap::drop(heapPath.c_str());
	m_heap = createSmallHeap();

	createAllTwoIndex(m_heap);
}
void MIndexOPStabilityTestCase::tearDown() {
	string heapPath(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_HEAP_EXT);
	closeSmallHeap(m_heap);
	m_heap = NULL;
	DrsHeap::drop(heapPath.c_str());

	string indexFileName(string(m_db->getConfig()->m_ntseConfig.m_basedir) + 
		"/" + m_tableDef->m_name + Limits::NAME_IDX_EXT);
	m_drsIndice->close(m_session, true);
	m_tntIndice->close(m_session);
	EXCPT_OPER(TNTIndice::drop(indexFileName.c_str()));
	CPPUNIT_ASSERT(!File::isExist(indexFileName.c_str()));
	
	clear();
}

void MIndexOPStabilityTestCase::testMultiThreadsDML() {
	const uint concurrence = MIndexWorker::THREAD_CONCURRENCE;
	MIndexWorker *workers[concurrence];
	u8 indexNo = 0;

	cout << "BLink tree multi-thread update and query test may waste some time, please wait..." << endl;

	for (uint i = 0; i < concurrence; i++) {
		workers[i] = new MIndexWorker("", i, indexNo, m_tableDef, m_db, m_tntIndice, this);
		workers[i]->start();
	}

	for (uint i = 0; i < concurrence; i++) {
		workers[i]->join(-1);
		delete workers[i];
		workers[i] = NULL;
	}
}


void TNTIndexTestCase::testRollback() {
	const uint size = 1;
	vector<Record*> oldRows;
	vector<Record*> newRows1;
	vector<Record*> newRows2;
	vector<Record*> upRows;
	//插入测试数据
	for (uint i = 0; i < size; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		RowId rid = i;
		u64 userId = i;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		oldRows.push_back(rec);

		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		m_session->endTransaction(true, true);
	}

	//删除使记录进入TNT
	for (uint i = 0; i < size ; i++) {
		m_tntIndice->deleteIndexEntries(m_session,oldRows[i], 2);
	}

	//回滚第一次删除
	for (uint i = 0; i < size; i++) {
		m_tntIndice->getMemIndice()->undoFirstUpdateOrDeleteIndexEntries(m_session, oldRows[i]);
	}

	
	//更新使记录全部进入TNT
	for (uint i = 0; i < size; i++) {
		RowId rid = i;
		u64 userId = i + size;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *newRec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		Record *oldRec = oldRows[i];
		
		u16 columns[1];
		columns[0] = 0;
		SubRecord before(REC_REDUNDANT, 1, columns, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, 1, columns, newRec->m_data, newRec->m_size, newRec->m_rowId);
		m_tntIndice->updateIndexEntries(m_session, &before, &after, true, 0);
		newRows1.push_back(newRec);

	}

	for(uint i = 0; i < size; i++) {
		//回退记录
		m_tntIndice->getMemIndice()->undoFirstUpdateOrDeleteIndexEntries(m_session, newRows1[i]);
	}


	//更新使记录全部进入TNT
	for (uint i = 0; i < size; i++) {
		RowId rid = i;
		u64 userId = i + size;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *newRec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		Record *oldRec = oldRows[i];
		
		u16 columns[1];
		columns[0] = 0;
		SubRecord before(REC_REDUNDANT, 1, columns, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, 1, columns, newRec->m_data, newRec->m_size, newRec->m_rowId);
		m_tntIndice->updateIndexEntries(m_session, &before, &after, true, 0);
		newRows2.push_back(newRec);

	}

	//在TNT中更新记录
	for (uint i = 0; i < size; i++) {
		RowId rid = i;
		u64 userId = i + size +100;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *newRec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);
		Record *oldRec = newRows2[i];

		u16 columns[1];
		columns[0] = 0;
		SubRecord before(REC_REDUNDANT, 1, columns, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, 1, columns, newRec->m_data, newRec->m_size, newRec->m_rowId);
		m_tntIndice->updateIndexEntries(m_session, &before, &after, false, 0);
		upRows.push_back(newRec);

	}

	//回退更新
	for(uint i = 0; i < size; i++) {
		Record *newRec = upRows[i];
		Record *oldRec = newRows2[i];
		
		u16 columns[1];
		columns[0] = 0;
		SubRecord before(REC_REDUNDANT, 1, columns, oldRec->m_data, oldRec->m_size, oldRec->m_rowId);
		SubRecord after(REC_REDUNDANT, 1, columns, newRec->m_data, newRec->m_size, newRec->m_rowId);
		m_tntIndice->getMemIndice()->undoSecondUpdateIndexEntries(m_session, &before, &after);
	}



	//第二次删除记录
	for (uint i = 0; i < size ; i++) {
		m_tntIndice->deleteIndexEntries(m_session,oldRows[i], 3);
	}

	//回滚第二次删除
	for (uint i = 0; i < size; i++) {
		m_tntIndice->getMemIndice()->undoSecondDeleteIndexEntries(m_session, oldRows[i]);
	}

	for(uint i = 0; i<size; i++) {
		freeRecord(newRows1[i]);
		freeRecord(newRows2[i]);
		freeRecord(oldRows[i]);
		freeRecord(upRows[i]);
	}

	
}

void TNTIndexTestCase::testFetchMaxKey() {
	const uint size = 4000;
	vector<Record*> oldRows;
	//插入数据
	for (uint i = 0; i < size; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		RowId rid = i;
		u64 userId = i;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		oldRows.push_back(rec);

		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		m_session->endTransaction(true, true);
	}
	void *p	= m_session->getMemoryContext()->alloc(sizeof(SubRecord));
	byte *buf = (byte*)m_session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize);
	SubRecord *foundKey	= new (p)SubRecord(REC_REDUNDANT, m_tableDef->getIndexDef(0)->m_numCols, m_tableDef->getIndexDef(0)->m_columns, buf, m_tableDef->m_maxRecSize);
	bool ret = m_tntIndice->getDrsIndex(0)->locateLastLeafPageAndFindMaxKey(m_session, foundKey);
	for(uint i = 0 ; i < size; i++){
		freeRecord(oldRows[i]);
	}
}


void TNTIndexTestCase::testScanLock() {
	const uint size = 800;
	vector<Record*> oldRows;
	//vector<Record*> newRows;
	//插入测试数据
	for (uint i = 0; i < size; i++) {
		McSavepoint mcs(m_session->getMemoryContext());

		RowId rid = i;
		u64 userId = i;
		const char *userName = "tntTestUser";
		u64 account = i;
		u32 balance = i;	
		Record *rec = createSmallHeapRecord(rid, userId, userName, account, balance, REC_REDUNDANT);

		oldRows.push_back(rec);

		m_session->startTransaction(TXN_INSERT, m_tableDef->m_id);
		m_tntIndice->getDrsIndice()->insertIndexNoDupEntries(m_session, rec);
		m_session->endTransaction(true, true);
	}


	TLockMode testLockModes[] = { TL_X };
	const u8 numTestModes = sizeof(testLockModes) / sizeof(TLockMode);

	//正向扫
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNT Index Scan", m_conn);
	TNTTransaction *trx = m_db->getTransSys()->allocTrx(testLockModes[0]);
	trx->startTrxIfNotStarted(m_conn);
	session->setTrans(trx);

	IndexDef *indexDef = m_tableDef->getIndexDef(0);
	SubRecord outKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, 
		(byte*)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);

	TLockMode lockMode = testLockModes[0];
	TNTIndex *index = m_tntIndice->getTntIndex(0);
	TNTIdxScanHandle *scan = index->beginScan(session, NULL, &outKey, 
		false, true, true, lockMode);
	for (uint i = 0; i < size; i++) {
		NTSE_ASSERT(!trx->isRowLocked(oldRows[i]->m_rowId, m_tableDef->m_id, lockMode));
		NTSE_ASSERT(index->getNext(scan));
		NTSE_ASSERT(oldRows[i]->m_rowId == scan->getRowId());
		NTSE_ASSERT(trx->isRowLocked(oldRows[i]->m_rowId, m_tableDef->m_id, lockMode));
		scan->unlatchNtseRowBoth();
	}

	index->endScan(scan);
	
	//开始另一个进程
	TNTIndexTestScanLockThread scanLockThread("scanLockThread", m_tableDef, m_db, m_tntIndice);
	scanLockThread.enableSyncPoint(SP_TNT_INDEX_LOCK);
	scanLockThread.enableSyncPoint(SP_TNT_INDEX_LOCK1);
	scanLockThread.enableSyncPoint(SP_TNT_INDEX_LOCK2);
	scanLockThread.start();
	scanLockThread.joinSyncPoint(SP_TNT_INDEX_LOCK);

	//主线程事务提交
	trx->commitTrx(CS_NORMAL);

	scanLockThread.disableSyncPoint(SP_TNT_INDEX_LOCK);
	scanLockThread.notifySyncPoint(SP_TNT_INDEX_LOCK);

	
	m_db->getTransSys()->freeTrx(trx);

	scanLockThread.joinSyncPoint(SP_TNT_INDEX_LOCK2);

	TNTTransaction *trx2 = m_db->getTransSys()->allocTrx(testLockModes[0]);
	trx2->startTrxIfNotStarted(m_conn);
	session->setTrans(trx2);
	TNTIdxScanHandle *scan2 = index->beginScan(session, NULL, &outKey, 
		false, true, true, lockMode);
	for (uint i = 0; i < size; i++) {
		NTSE_ASSERT(!trx2->isRowLocked(oldRows[i]->m_rowId, m_tableDef->m_id, lockMode));
		NTSE_ASSERT(index->getNext(scan2));
		NTSE_ASSERT(oldRows[i]->m_rowId == scan2->getRowId());
		NTSE_ASSERT(trx2->isRowLocked(oldRows[i]->m_rowId, m_tableDef->m_id, lockMode));
		scan2->unlatchNtseRowBoth();
	}

	index->endScan(scan2);
	
	scanLockThread.disableSyncPoint(SP_TNT_INDEX_LOCK2);
	scanLockThread.notifySyncPoint(SP_TNT_INDEX_LOCK2);

	
	scanLockThread.joinSyncPoint(SP_TNT_INDEX_LOCK1);
	trx2->commitTrx(CS_NORMAL);

	scanLockThread.disableSyncPoint(SP_TNT_INDEX_LOCK1);
	scanLockThread.notifySyncPoint(SP_TNT_INDEX_LOCK1);
	scanLockThread.join(-1);

	m_db->getTransSys()->freeTrx(trx2);

	session->setTrans(NULL);

	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	
	for(uint i = 0; i < size ; i++ )
		freeRecord(oldRows[i]);

}

TNTIndexOPThread::TNTIndexOPThread(const char *name, TableDef *tableDef, 
				 TNTDatabase *db, TNTIndice *tntIndice):Thread(name) {
	
	m_tableDef = tableDef;
	m_db = db;
	m_tntIndice = tntIndice;
}

void TNTIndexTestScanLockThread::run() {
	//正向扫
	m_conn = m_db->getNtseDb()->getConnection(false);
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNT Index Scan", m_conn);
	
	TLockMode testLockModes[] = { TL_X };
	const u8 numTestModes = sizeof(testLockModes) / sizeof(TLockMode);

	TNTTransaction *trx = m_db->getTransSys()->allocTrx(testLockModes[0]);
	trx->startTrxIfNotStarted(m_conn);
	session->setTrans(trx);

	IndexDef *indexDef = m_tableDef->getIndexDef(0);
	SubRecord outKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, 
		(byte*)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);

	TLockMode lockMode = testLockModes[0];
	TNTIndex *index = m_tntIndice->getTntIndex(0);
	TNTIdxScanHandle *scan = index->beginScan(session, NULL, &outKey, 
		false, true, true, lockMode);

	index->getNext(scan);
	scan->unlatchNtseRowBoth();

	index->endScan(scan);
	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);
	session->setTrans(NULL);
	

	SYNCHERE(SP_TNT_INDEX_LOCK2);
	
	TNTTransaction *trx2 = m_db->getTransSys()->allocTrx(testLockModes[0]);
	trx2->startTrxIfNotStarted(m_conn);
	session->setTrans(trx2);
	TNTIdxScanHandle *scan2 = index->beginScan(session, NULL, &outKey, 
		false, true, true, lockMode);

	try {
		index->getNext(scan2);
		scan2->unlatchNtseRowBoth();
	}
	catch(NtseException)
	{
		printf("Row lock Timesout");
	}

	index->endScan(scan2);
	trx2->commitTrx(CS_NORMAL);

	m_db->getTransSys()->freeTrx(trx2);

	session->setTrans(NULL);

	m_db->getNtseDb()->getSessionManager()->freeSession(session);
	m_db->getNtseDb()->freeConnection(m_conn);

}



void TNTIndexTestScanThread::run() {
	m_conn = m_db->getNtseDb()->getConnection(false);
	//扫描索引
	Session *session = m_db->getNtseDb()->getSessionManager()->allocSession("TNT Index Scan", m_conn);
	TNTTransaction *trx = m_db->getTransSys()->allocTrx();
	trx->startTrxIfNotStarted(m_conn);
	session->setTrans(trx);

	IndexDef *indexDef = m_tableDef->getIndexDef(1);
	SubRecord outKey(KEY_PAD, indexDef->m_numCols, indexDef->m_columns, 
		(byte*)session->getMemoryContext()->alloc(m_tableDef->m_maxRecSize), m_tableDef->m_maxRecSize);

	TLockMode lockMode = TL_NO;
	TNTIndex *index = m_tntIndice->getTntIndex(1);
	TNTIdxScanHandle *scan = index->beginScan(session, NULL, &outKey, 
		false, true, true, lockMode);
	while (index->getNext(scan)) {
		scan->unlatchNtseRowBoth();
	}

	index->endScan(scan);

	trx->commitTrx(CS_NORMAL);
	m_db->getTransSys()->freeTrx(trx);

	session->setTrans(NULL);

	m_db->getNtseDb()->getSessionManager()->freeSession(session);
}