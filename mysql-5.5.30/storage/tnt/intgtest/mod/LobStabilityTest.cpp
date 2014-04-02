/**
* LOB稳定性测试
*
* @author zx(zx@163.org)
*/

#include <iostream>
#include "LobStabilityTest.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "Random.h"
#include "BlogTable.h"
#include "util/File.h"
#include "btree/IndexBPTree.h"
#include "heap/Heap.h"
#include "lob/BigLob.h"
#include "lob/LobIndex.h"



const char* LobSblTestCase::getName() {
	return "Lob Stability Test";
}

const char* LobSblTestCase::getDescription() {
	return "Stability Test for Lob Module";
}

bool LobSblTestCase::isBig() {
	return false;
}

void LobSblTestCase::setUp() {
	Database::drop(m_cfg.m_basedir);
	File dir(m_cfg.m_basedir);
	NTSE_ASSERT(m_db = Database::open(&m_cfg, true));
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::setUp", conn);
	
	//需要先清理以前文件
	string tname ("Blog");
	string basePath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + tname ;
	try {
		string fullPath = basePath + Limits::NAME_HEAP_EXT ;
		DrsHeap::drop(fullPath.c_str());
		fullPath = basePath + Limits::NAME_IDX_EXT;
		DrsIndice::drop(fullPath.c_str());
		LobStorage::drop(basePath.c_str());
	} catch (NtseException ex) {

	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_resLogger = new ResLogger(m_db, 1800, "[Lob]DbResource.txt");
}

/** 因为setUp不能带参数，所以这里加一个init函数，可以设置大对象平均长度等*/
void LobSblTestCase::init(uint lobLen, u64 dataSize, bool useMms) {
	m_dataSize = dataSize;
	m_avgLobSize = lobLen;
	m_useMms = useMms;
	u64 maxRecCnt = m_dataSize / m_avgLobSize;

	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::init", conn);
	
	//建立表等操作
	m_db->createTable(session, "Blog", (TableDef *)BlogTable::getTableDef(m_useMms));
	m_table = m_db->openTable(session, "Blog");
	m_lobS = m_table->getLobStorage();
	createVitualTable();
	m_memHeap = new MemHeap((uint)maxRecCnt, m_vTableDef);
	
	m_verifierThread = new LobSblVerifier(this, VERIFY_INTERNAL);
	m_defragThread = new LobDefragWorker(this, DEFRAG_INTERNAL);
	m_threadCount = THREAD_COUNT;
	m_workerThreads = new LobSblWorker *[m_threadCount];
	for (uint i = 0; i < m_threadCount; i++)
		m_workerThreads[i] = new LobSblWorker(this);

	//生成随机类
	unsigned dist[4] = {SELECT_WEIGHT, INSERT_WEIGHT, DELETE_WEIGHT, UPDATE_WEIGHT};
	m_randDist = new RandDist(dist, 4);
	m_flag = false;
	m_stop = false;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/** 根据一个均值得到正态分布一个值 */
uint LobSblTestCase::getLenByNDist() {
	return RandomGen::randNorm(m_avgLobSize, MIN_LOB_LEN, MAX_LOB_LEN);
}

/** 随机生成一个大对象 */
byte* LobSblTestCase::createLob(uint len){
	char *abs = new char[len];
	for (uint i = 0; i < len; i++ ) {
		abs[i] = (char )('A' + System::random() % 10);
	}
	return (byte *)abs;
}

/** 进行 defrag */
void LobSblTestCase::doDefrag() {
	cout << " start defrag " << endl;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::doDefrag", conn);
	m_lobS->defrag(session);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	cout << "stop defrag" <<endl;
}

/** 构造一条记录，格式是一个LobId + 大对象指针*/
Record* LobSblTestCase::createRecord(LobId lid, byte *lob, uint len) {
	RecordBuilder rb(m_vTableDef, RID(0, 0),  REC_REDUNDANT);
	rb.appendBigInt(lid);
	Record *rec = rb.getRecord();
	RecordOper::writeLob(rec->m_data, m_vTableDef->m_columns[1], lob);
	RecordOper::writeLobSize(rec->m_data, m_vTableDef->m_columns[1], len);
	return rec;
}

/** 生成一个SubRecord*/
SubRecord* LobSblTestCase::createSubRec(byte *lob, uint len) {
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecord *updateRec = srb.createSubRecordById("");
	RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[1], lob);
	RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[1], len);
	return updateRec;
}

/** 建立一个虚拟表：表结构就LobId(u64) + 大对象*/
void LobSblTestCase::createVitualTable() {
	TableDefBuilder *tbuilder = new TableDefBuilder(0, "testBlob", "testBlob");
	tbuilder->addColumn("lobId", CT_BIGINT, false);
	tbuilder->addColumn("lobContent", CT_MEDIUMLOB, true);
	m_vTableDef = tbuilder->getTableDef();
	delete tbuilder;
}

/** 进行insert */
void LobSblTestCase::doInsert() {
	MemHeapRid mhRid = m_memHeap->reserveRecord();
	if (mhRid == MemHeapRecord::INVALID_ID) return;
	//因为在内存堆中RowId不能变，所以这里要生成一个随机的RowId,便于加锁等。
	RowId rid = ((RowId)mhRid) << 32;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::doInsert", conn);
	uint len = getLenByNDist();
	byte *lob = createLob(len);
	LobId lid = m_lobS->insert(session, lob, len);
	//加行锁
	RowLockHandle *rlh = NULL;
	rlh = LOCK_ROW(session, m_vTableDef->m_id, rid, Exclusived);
	RedRecord redRec(m_vTableDef);
	redRec.writeNumber(0, lid);
	redRec.writeLob(1, lob, (size_t)len);
	NTSE_ASSERT(mhRid = m_memHeap->insertAt(session, mhRid, rid, redRec.getRecord()->m_data));
	//Record *rec = createRecord(lid, lob, len);
	//NTSE_ASSERT(MemHeapRid mhRid = m_memHeap->insertAt(session, mhRid, lid, rec->m_data));
	NTSE_ASSERT(mhRid != MemHeapRecord::INVALID_ID);
	session->unlockRow(&rlh);
	//清理工作 
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/** 进行update*/
void LobSblTestCase::doUpdate() {
	MemHeapRecord *mhRecord;
	RowLockHandle *rlh = NULL;
	LobId lid;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::doUpdate", conn);
	mhRecord = m_memHeap->getRandRecord(session, &rlh, Exclusived);
	if (mhRecord == NULL) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		return;
	}
	
	byte rec[Limits::PAGE_SIZE];
	mhRecord->toRecord(session, rec);
	lid = (LobId) RedRecord::readBigInt(m_vTableDef, rec, 0);
	uint newLen = getLenByNDist();
	byte *lob = createLob(newLen);
	LobId newLid = m_lobS->update(session, lid, lob, newLen, m_table->getTableDef()->m_compressLobs);
	RedRecord redRec(m_vTableDef);
	redRec.writeNumber(0, newLid);
	redRec.writeLob(1, lob, (size_t)newLen);
	u16 columns[2] = {0, 1}; 
	mhRecord->update(session, 2, columns, redRec.getRecord()->m_data);
	session->unlockRow(&rlh);
	
	//清理工作 
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

}

/** 进行del */
void LobSblTestCase::doDel() {
	MemHeapRecord *mhRecord;
	RowLockHandle *rlh = NULL;
	LobId lid;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::doDel", conn);
	mhRecord = m_memHeap->getRandRecord(session, &rlh, Exclusived);
	if (mhRecord == NULL) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		return;
	}

	byte rec[Limits::PAGE_SIZE];
	mhRecord->toRecord(session, rec);
	lid = (LobId) RedRecord::readBigInt(m_vTableDef, rec, 0);
	m_lobS->del(session, lid);

	bool re = m_memHeap->deleteRecord(session, mhRecord->getId());
	assert(re);
	session->unlockRow(&rlh);
	
	//清理工作 
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

}

/** 进行read */
void LobSblTestCase::doRead() {
	MemHeapRecord *mhRecord;
	RowLockHandle *rlh = NULL;
	LobId lid;
	uint size;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::doRead", conn);
	MemoryContext *mc = session->getMemoryContext();
	mhRecord = m_memHeap->getRandRecord(session, &rlh, Shared);
	if (mhRecord == NULL) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		return;
	}
	byte rec[Limits::PAGE_SIZE];
	mhRecord->toRecord(session, rec);
	lid = (LobId) RedRecord::readBigInt(m_vTableDef, rec, 0);
	byte *lob = m_lobS->get(session, mc, lid, &size);
	
	//read时候需要验证
	RedRecord redRec(m_vTableDef);
	redRec.writeNumber(0, lid);
	redRec.writeLob(1, lob, (size_t)size);
	bool compareRe = mhRecord->compare(session, redRec.getRecord());
	assert(compareRe);
	redRec.setNull(1);
	session->unlockRow(&rlh);
	
	//清理工作 
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 工作线程的入口函数 */
void LobSblTestCase::doWork() {
	while (true) {
		switch(getTaskType()) {
			case WORK_TASK_TYPE_INSERT:
				doInsert();
				break;
			case WORK_TASK_TYPE_DELETE:
				doDel();
				break;
			case WORK_TASK_TYPE_SELECT:
				doRead();
				break;
			case WORK_TASK_TYPE_UPDATE:
				doUpdate();
				break;
			default:
				assert(false); 
		}
		if (m_flag)
			break;
	}
}

/** 开始工作线程 */
void LobSblTestCase::startThreads() {
	for (uint i = 0; i < m_threadCount; i++)
		m_workerThreads[i]->start();
}

/** 得到需要的操作类型 */
uint LobSblTestCase::getTaskType() {
	uint i = m_randDist->select();
	switch (i) {
		case 0: 
			return WORK_TASK_TYPE_SELECT;
			break;
		case 1: 
			return WORK_TASK_TYPE_INSERT;
			break;
		case 2: 
			return WORK_TASK_TYPE_UPDATE;
			break;
		case 3: 
			return WORK_TASK_TYPE_DELETE;
			break;
		default:
			assert(false);
			return 4;
	}
}

/** 把工作线程暂停 */
void LobSblTestCase::stopThreads() {
	m_flag = true;
	for (uint i = 0; i < m_threadCount; i++) {
		m_workerThreads[i]->join();
		delete m_workerThreads[i];
		m_workerThreads[i] = NULL;
	}
	delete [] m_workerThreads;
	m_workerThreads = NULL;
}

/** 做线程验证 */
void LobSblTestCase::doVerify() {

	
	cout << "start verify" << endl;
	//先停止所有线程
	stopThreads();
	LobId lid;
	
	//然后做验证
	RowLockHandle *rlh = NULL;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::doVerify", conn);
	MemoryContext *mc = session->getMemoryContext();
	MemHeapRid mhRid;
	MemHeapRecord *mhRecord;
	uint recCount = m_memHeap->getUsedRecCount();
	uint maxCount = m_memHeap->getMaxRecCount();
	uint count = 0;
	u64 totalDataSize = 0;
	uint size = 0;
	for(uint i = 0; i < maxCount; i++) {
		if (count == recCount) break;
		mhRid = (MemHeapRid)i;
		mhRecord = m_memHeap->recordAt(session, mhRid, &rlh, Shared);
		if (mhRecord == NULL) {
			continue;
		} else {
			u64 savePoint = mc->setSavepoint();
			count++;
			byte rec[Limits::PAGE_SIZE];
			mhRecord->toRecord(session, rec);
			lid = (LobId) RedRecord::readBigInt(m_vTableDef, rec, 0);
			byte *lob = m_lobS->get(session, mc, lid, &size);
			totalDataSize += size;
			RedRecord redRec(m_vTableDef);
			redRec.writeNumber(0, lid);
			redRec.writeLob(1, lob, (size_t)size);
			bool compareRe = mhRecord->compare(session, redRec.getRecord());
			assert(compareRe);
			redRec.setNull(1);
			session->unlockRow(&rlh);
			mc->resetToSavepoint(savePoint);
		}
	}
	
	//清理工作 
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	
	//判断是否需要调整操作比率
	//需要调高insert
	if (totalDataSize <= m_dataSize / 2 ) {
		unsigned dist[4] = {SELECT_WEIGHT, NEW_INSERT_WEIGHT, UPDATE_WEIGHT, 10};
		delete m_randDist; 
		m_randDist = new RandDist(dist, 4);
	}
	
	//需要调高del
	if (totalDataSize >=  m_dataSize * 4 / 5 ) {
		unsigned dist[4] = {SELECT_WEIGHT, 20, UPDATE_WEIGHT, NEW_DELETE_WEIGHT};
		delete m_randDist; 
		m_randDist = new RandDist(dist, 4);	
	}
	m_flag = false;

	cout << "stop verify" << endl;
	//if (m_stop) {
	//	return;
	//}

	//开动工作线程
	startNewThreads();
	cout << "resume threads" << endl;
}

/** 重新生成新的工作线程序*/
void LobSblTestCase::startNewThreads() {
	m_workerThreads = new LobSblWorker *[m_threadCount];
	for (uint i = 0; i < m_threadCount; i++) {
		m_workerThreads[i] = new LobSblWorker(this);
		m_workerThreads[i]->start();
	}
}

/** 测试的入口函数 */
void LobSblTestCase::testLobStability(){
	
	u64 dataSize = 128 * 1024 * 1024;
	uint avgLobSize = AVG_LOB_LEN_2;
	init(avgLobSize, dataSize, true);
	
	runTest();
	
	// 不停止， 这里设置为了测试
	//因为msleep不精确，所以用while
	u64 runTime = 1000 * 60 * 60 * 5;
	u64 now ;
	u64 next;
	u64 timeOffset;
	while(true) {
		now = System::currentTimeMillis();
		Thread::msleep(1000 * 60 * 10);
		next = System::currentTimeMillis();
		timeOffset =  next - now;
		runTime -= timeOffset;
		if (runTime <= 0)
			break;
	}
	
	//cout << timeOffset << endl;
	//m_stop = true;
	cout << "stop test"<< endl; 
	m_verifierThread->stop();

	cout << "success finish the test"<< endl; 
}

void LobSblTestCase::tearDown() {
	delete m_resLogger;
	m_verifierThread->join();
	delete m_verifierThread;
	m_verifierThread = NULL;

	//处理线程
	stopThreads();
	//for(uint i = 0; i < m_threadCount; i++) {
	//	delete m_workerThreads[i];
	//	m_workerThreads[i] = NULL;
	//}
	//delete [] m_workerThreads;
	//m_workerThreads = NULL;
	m_defragThread->stop();
	m_defragThread->join();
	delete m_defragThread;
	m_defragThread = NULL;

	//处理其他资源
	delete m_randDist;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::tearDown", conn);

	//m_table->close(session, false);
	//delete m_table;
	//delete m_lobS;
	delete m_vTableDef;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	//这里需要做双向验证扫描两次
	bool  re = lastVerify();
	assert(re);
	//close db等操作
	delete m_memHeap;
	m_db->close(false, false);
	delete m_db;
	m_db = NULL;

}

/** 测试的函数 */
void LobSblTestCase::runTest() {
	// 线程运行
	startThreads();
	m_verifierThread->start();
	m_defragThread->start();
}

/** 最后的verify，需要scan */
bool LobSblTestCase::lastVerify() {
	
	File **files = new File *[3];
	PageType pageTypes[3]; 
	m_lobS->getFiles(files, pageTypes, 3);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSblTestCase::lastVerify", conn);
	uint recCount = m_memHeap->getUsedRecCount();
	//做双向认证,这里因为ID是唯一的，所以只验证个数
	Buffer *buffer = m_db->getPageBuffer();
	//得到首页
	BufferPageHandle *headerPageHdl = GET_PAGE(session, files[0], PAGE_LOB_INDEX, 0, Shared, m_lobS->getLLobDirStats(), NULL);
	u32 pageId = 0;
	uint blobNum = 0;
	uint indexNum = 0;
	uint slobNum = 0;
	LIFileHeaderPageInfo *headerPage = (LIFileHeaderPageInfo *)headerPageHdl->getPage();
	u32 blobFileTail = headerPage->m_blobFileTail;
	u32 indexFileLen = headerPage->m_fileLen;
	session->releasePage(&headerPageHdl);
	
	//验证目录文件
	pageId = 1;
	while (pageId < indexFileLen) {
		BufferPageHandle *pageHdl = GET_PAGE(session, files[0], PAGE_LOB_INDEX, pageId, Shared, m_lobS->getLLobDirStats(), NULL);
		LIFilePageInfo *page = (LIFilePageInfo *) pageHdl->getPage();
		u32 maxSlotNumPerPage = (Limits::PAGE_SIZE - sizeof(LIFilePageInfo)) / sizeof(LiFileSlotInfo);
		indexNum += (maxSlotNumPerPage - page->m_freeSlotNum);
		pageId++;
		session->releasePage(&pageHdl);
	}
	
	//再验证大对象文件
	pageId = 0;
	while (pageId < blobFileTail) {
		BufferPageHandle *pageHdl = GET_PAGE(session, files[1], PAGE_LOB_HEAP, pageId, Shared, m_lobS->getLLobDatStats(), NULL);
		LobBlockFirstPage *page = (LobBlockFirstPage *) pageHdl->getPage();
		if (!page->m_isFree)
			blobNum++;
		pageId += (page->m_len + Limits::PAGE_SIZE - 1)/ Limits::PAGE_SIZE;
		session->releasePage(&pageHdl);
	}
	assert(blobNum == indexNum);

	//最后scan小型大对象文件，得到个数
	//这里通过表扫描
	DrsHeap *heap = m_lobS->getSLHeap();
	RowLockHandle *rlh;
	u16 numCols = 1;
	u16 columns[1] = {0};
	byte data[Limits::PAGE_SIZE];
	SubRecord *subRec = (SubRecord *)data;
	subRec->m_format = REC_REDUNDANT;
	subRec->m_numCols = 1;
	u16 columns2[2] = {0};
	subRec->m_columns = columns2;
	subRec->m_size = Limits::PAGE_SIZE;
	subRec->m_data = data;

	SubrecExtractor *extractor = SubrecExtractor::createInst(session, heap->getTableDef(), subRec);
	DrsHeapScanHandle *scan = heap->beginScan(session, extractor, Shared, &rlh, false);
	while (heap->getNext(scan, subRec)){
		slobNum++;
		session->unlockRow(&rlh);
	}
	heap->endScan(scan);
	delete scan;
	assert(indexNum + slobNum == recCount);

	//资源清理
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	//下面的是在m_db->close()会做
	//for (uint i =0; i < 3; i++){

		//files[i]->close();
		//delete files[i];
	//}
	delete [] files;
	files = NULL;
	return true;
}

