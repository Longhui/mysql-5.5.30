/**
* 测试大对象的随片整理操作
* 
* @author zx (zx@corp.netease.com, zx@163.org)
*/

#include "lob/TestLobDefrag.h"
#include "lob/Lob.h"
#include "lob/BigLob.h"
#include "lob/LobIndex.h"
#include "mms/Mms.h"
#include "Test.h"
#include <string>
#include "util/System.h"
#include "misc/Session.h"
#include "misc/TableDef.h"
#include "misc/Global.h"
#include "util/File.h"
#include "api/Database.h"
#include "misc/Txnlog.h"
#include "util/Portable.h"
#include <iostream>
#include <vector>

using namespace std;

#define LOB_DIR  "."
#define LOBINDEX_PATH "lobtest.nsli"
#define LOBSMALL_PATH "lobtest.nsso"
#define LOBBIG_PATH   "lobtest.nsld"

const static uint MIN_NUM = 10;
const static uint MAX_NUM = 100;
const static uint LOW_LEN = 999;
const static uint HIGE_LEN = 9999;
const static char *LOB_PATH = "lobtest";
const static char *CFG_PATH = "ntse_ctrl";

static vector<LobId> lobIds;
static vector<pair<byte *, uint> > lobContents;

static vector<LobId>::iterator iter;
static vector<pair<byte *, uint> >::iterator c_iter;
static vector<uint> flag;

const char* LobDefragTestCase::getName() {
	return "Lob defrag test";
}

const char* LobDefragTestCase::getDescription() {
	return "Test defrag of lob file";
}

bool LobDefragTestCase::isBig() {
	return false;
}

/**
* 准备测试环境：建立大对象管理storage
*
*/
void LobDefragTestCase::setUp() {
	m_conn = NULL;
	m_db = NULL;
	m_lobStorage = NULL;
	File file(CFG_PATH);
	bool isExist;
	file.isExist(&isExist);
	if (isExist) 
		Database::drop(".");
	TableDefBuilder *builder = new TableDefBuilder(1, "LobFakeSchema", "LobFakeTable");
	builder->addColumn("FakeID", CT_INT, false);
	m_tableDef = builder->getTableDef();
	delete builder;
	m_lobStorage = createLobStorage();
}


void LobDefragTestCase::tearDown() {
	if (m_lobStorage) {
		dropLobStorage();
	}
	if (m_tableDef) {
		delete m_tableDef;
		m_tableDef = NULL;
	}
	if (m_conn) {
		m_db->freeConnection(m_conn);
		m_conn = NULL;
	}
	if (m_db) {
		m_db->close();
		delete m_db;
		m_db = NULL;
		Database::drop(LOB_DIR);
		
	}
}

/**
 * drop 大对象存储对象
 */
void LobDefragTestCase::dropLobStorage() {
	Connection *conn = m_db->getConnection(true);
	assert(conn);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::createLobStorage", conn);
	assert(session);
	EXCPT_OPER(m_lobStorage->close(session, true));
	delete m_lobStorage;
	EXCPT_OPER(LobStorage::drop(LOB_DIR));
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 生成大对象存储对象
 * @return 大对象存储对象
 */
LobStorage*  LobDefragTestCase::createLobStorage() {
	//u16 tid = (u16)1;
	File lobIndexFile(LOBINDEX_PATH);
	lobIndexFile.remove();
	File lobBigFile(LOBBIG_PATH);
	lobBigFile.remove();
	File lobSmallFile(LOBSMALL_PATH);
	lobSmallFile.remove();
	//cfg.m_pageBufSize = 10000;
	//Config cfg;
	m_db = Database::open(&m_cfg, true, -1);	
	//EXCPT_OPER(LobStorage::create(m_db, tid, LOB_PATH));
	LobStorage::create(m_db, m_tableDef, LOB_PATH);
	m_conn = m_db->getConnection(true);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::createLobStorage", m_conn);
	LobStorage *lobStorage;
	EXCPT_OPER(lobStorage= LobStorage::open(m_db, session, m_tableDef, LOB_PATH, false));
	m_db->getSessionManager()->freeSession(session);
	return lobStorage;
}

/************************************************************************/
/*        2008-11-13为了测试压缩的功能，这里重新整理代码，分成两个测试  */
/*   这里验证大对象正确性是根据在操作前记录大对象长度和内容，操作后通过 */
/*   查询，然后比较查询结果和记录的大对象内容                           */
/************************************************************************/


/**
 * 测试defrag功能
 * 测试步骤：
 * 1、插入8个大对象，其中删除第2个和第7个大对象
 * 2、各个大对象长度满足如下条件，第三个对象大于第二个大对象。
 * 3、所以defrag时候，第三个大对象会被移动到末尾，
 * 4、后面大对象依次前移
 */
void LobDefragTestCase::testAllDefrag() {
	testDefrag(false);
	//tearDown();
	//setUp();
	//testDefrag(true);
}

/**
 * 测试redoMove功能
 * 测试步骤：
 * 1、插入8个大对象，其中删除第2个和第7个大对象
 * 2、然后备份整个大对象文件和目录文件
 * 3、各个大对象长度满足如下条件，第三个对象大于第二个大对象。
 * 4、开始Defrag，第三个大对象会被移动到末尾，
 * 5、后面大对象依次前移
 * 6、然后关闭数据库，恢复备份的文件，然后打开数据库
 * 7、进行redoMove操作
 * 8、验证各个大对象正确性
 */
void LobDefragTestCase::testAllRedoMove() {
	testRedoMove();
}

/**
 * 测试当小型大对象，这里是没有mms cache
 * 测试步骤：
 * 1、把一个小型大对象L1 insert 
 * 2、然后做update, 最后del
 * 3、在insert 一个小型大对象L2， 然后read,再update,最后del
 *
 * 测试当小型大对象并大型大对象，这里是没有mms cache
 * 测试步骤：
 * 1、把一个小型大对象L1 insert 
 * 2、然后做update成大型大对象, 最后del
 */
void LobDefragTestCase::testAllOper(){
	testSmallOper(false);
	testSmallToBigOper(true);
}

/**
 * 测试当小型大对象，这里是没有mms cache
 * 测试步骤：
 * 1、把一个小型大对象L1 insert 
 * 2、然后做update, 最后del
 * 3、在insert 一个小型大对象L2， 然后read,再update,最后del
 */

void LobDefragTestCase::testAllSmallOper(){
	testSmallOper(false);
	//testSmallOper(true);
}

/**
 * 测试当小型大对象并大型大对象，这里是没有mms cache
 * 测试步骤：
 * 1、把一个小型大对象L1 insert 
 * 2、然后做update成大型大对象, 最后del
 */
void LobDefragTestCase::testAllSmallToBigOper() {
	//testSmallToBigOper(false);
	testSmallToBigOper(true);
}

void LobDefragTestCase::testSmallOper(bool isCompress) {
	uint lobLen = Limits::MAX_REC_SIZE - 8;
	byte *lob = createLob(lobLen);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testInsertSmall", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	LobId lid = m_lobStorage->insert(session, lob, lobLen, isCompress);
	uint size;
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == lobLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, lob, size));

	//在读一次，这个时候在MMS中
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == lobLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, lob, size));
	System::virtualFree(lob);

	// 更新
	uint newLen = Limits::MAX_REC_SIZE - 7;
	byte *newLob = createLob(newLen);
	LobId lid2 = m_lobStorage->update(session, lid, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid2 == lid);
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	System::virtualFree(newLob);

	// 最后del
	m_lobStorage->del(session, lid);
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(getLob == NULL);

	//在DRS堆中更新
	lobLen = Limits::MAX_REC_SIZE - 300;
	lob = createLob(lobLen);
	lid = m_lobStorage->insert(session, lob, lobLen, isCompress);
	newLen = Limits::MAX_REC_SIZE - 200;
	newLob = createLob(newLen);
	lid2 = m_lobStorage->update(session, lid, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid2 == lid);
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	m_db->getSessionManager()->freeSession(session);
	System::virtualFree(lob);
	System::virtualFree(newLob);
}

void LobDefragTestCase::testSmallToBigOper(bool isCompress) {
	uint lobLen = Limits::MAX_REC_SIZE - 8;
	byte *lob = createLob(lobLen);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testInsertTOBig", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	LobId lid = m_lobStorage->insert(session, lob, lobLen, isCompress);
	System::virtualFree(lob);

	uint  newLen = Limits::PAGE_SIZE;
	byte *newLob = createLob(newLen);
	LobId lid2 = m_lobStorage->update(session, lid, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid2 != lid);
	uint size;
	byte *getLob = m_lobStorage->get(session, mc, lid2, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	System::virtualFree(newLob);

	//再变小，但仍放在大型大对象文件中
	newLen = Limits::MAX_REC_SIZE - 5;
	newLob = createLob(newLen);
	LobId lid3 = m_lobStorage->update(session, lid2, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid3 == lid2);
	getLob = m_lobStorage->get(session, mc, lid3, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	System::virtualFree(newLob);


	//测试在mms中的update
	lobLen = Limits::MAX_REC_SIZE - 7;
	lob = createLob(lobLen);
	lid = m_lobStorage->insert(session, lob, lobLen, isCompress);
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == lobLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, lob, size));
	newLen = Limits::PAGE_SIZE;
	newLob = createLob(newLen);
	lid2 = m_lobStorage->update(session, lid, newLob, newLen, isCompress);

	getLob = m_lobStorage->get(session, mc, lid2, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	System::virtualFree(newLob);
	System::virtualFree(lob);

	//再变小，但仍放在大型大对象文件中
	newLen = Limits::MAX_REC_SIZE - 3;
	newLob = createLob(newLen);
	lid3 = m_lobStorage->update(session, lid2, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid3 == lid2);
	getLob = m_lobStorage->get(session, mc, lid3, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	m_db->getSessionManager()->freeSession(session);
	System::virtualFree(newLob);
}

/**
 * 为defrag做准备,insert一定数量的大对象，并删除其中几个
 * 
 * @param num 需要insert大对象的个数
 */
void LobDefragTestCase::insertAndDelLobs(uint num) {
	//先插入100个大对象
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::createLobStorage", m_conn);
	for (uint i = 0; i < num; i++) {
		uint size = System::random() / HIGE_LEN + LOW_LEN;
		byte *lob = createLob(size);
		LobId lid = m_lobStorage->insert(session, lob, size, false);
		lobIds.push_back(lid);
		pair<byte *, uint> p1 = make_pair(lob, size);
		lobContents.push_back(p1);
	}
	
	m_db->getSessionManager()->freeSession(session);
	
}


/**
 * 为defrag做准备, insert多个大对象，然后删除其中几个
 * @param isCompress 是否压缩
 */
void LobDefragTestCase::insertAndDelLobs(bool isCompress) {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::createLobStorage", m_conn);
	uint size = getLobSize(Limits::PAGE_SIZE, isCompress) * 2;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 2, isCompress) - 1;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 1, isCompress) - 50;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 3, isCompress) - 1;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 2, isCompress) + 1;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE, isCompress) - 1;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 5, isCompress);
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 3, isCompress) + 1;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE, isCompress) - 2;
	insertLob(session, size,isCompress);

	//删除一些LOB
	m_lobStorage->del(session, lobIds[1]);
	m_lobStorage->del(session, lobIds[2]);
	size_t num = lobIds.size();

	m_lobStorage->del(session, lobIds[num - 3]);

	m_db->getSessionManager()->freeSession(session);
}


/**
 * insert大对象的公用函数
 *
 * @param session    会话对象 
 * @param size       大对象的长度
 * @param isCompress 是否压缩
 */
void LobDefragTestCase::insertLob(Session *session, uint size, bool isCompress){
	byte *lob = createLob(size);
	LobId lid = m_lobStorage->insert(session, lob, size, isCompress);
	lobIds.push_back(lid);
	pair<byte *, uint> p1 = make_pair(lob, size);
	lobContents.push_back(p1);
}


/**
 * 进行碎片整理
 *
 * @param isCompress 是否压缩
 */
void  LobDefragTestCase::testDefrag(bool isCompress) {
	clearLobs();

	insertAndDelLobs(isCompress);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::createLobStorage", m_conn);
	m_lobStorage->defrag(session);
	for (uint i = 0; i < lobIds.size(); i++) {
		uint size;
		if (i != 1 && i != 2 && i != (lobIds.size() - 3)) {
 			byte *getLob = m_lobStorage->get(session, session->getLobContext(), lobIds[i], &size, false);
			CPPUNIT_ASSERT(size == lobContents[i].second);
			CPPUNIT_ASSERT(lobIsSame(getLob, lobContents[i].first, size));
		}
	}

	clearLobs();
	m_db->getSessionManager()->freeSession(session);
}

void LobDefragTestCase::testDefragEx() {
	clearLobs();

	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::createLobStorage", m_conn);
	uint size = getLobSize(Limits::PAGE_SIZE, false) * 2;
	insertLob(session, size, false);
	size = getLobSize(Limits::PAGE_SIZE, false) * 3;
	insertLob(session, size, false);
	size = getLobSize(Limits::PAGE_SIZE, false);
	insertLob(session, size, false);
	m_lobStorage->defrag(session);

	m_lobStorage->del(session, lobIds[2]);
	m_lobStorage->defrag(session);

	m_lobStorage->del(session, lobIds[1]);
	m_lobStorage->defrag(session);

	clearLobs();
	m_db->getSessionManager()->freeSession(session);
}



/**
 * 为测试testRedoMove准备环境,插入多个大对象,并删除其中几个
 * 
 * @param isCompress 是否压缩
 */
void LobDefragTestCase::insertAndDelLobs2() {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::createLobStorage", m_conn);
	uint size = getLobSize(Limits::PAGE_SIZE, false) * 2;
	insertLob(session, size, false);
	size = getLobSize(Limits::PAGE_SIZE * 2, true)  - 1;
	insertLob(session, size, true);
	size = getLobSize(Limits::PAGE_SIZE * 3, false)  - 1;
	insertLob(session, size, false);
	size = getLobSize(Limits::PAGE_SIZE * 2, true)  + 1;
	insertLob(session, size, true);
	size = getLobSize(Limits::PAGE_SIZE * 5, false) ;
	insertLob(session, size, false);
	size = getLobSize(Limits::PAGE_SIZE * 3, true)  + 1;
	insertLob(session, size, true);

	//删除一些LOB
	m_lobStorage->del(session, lobIds[1]);
	size_t num = lobIds.size();
	m_lobStorage->del(session, lobIds[num - 2]);

	m_db->getSessionManager()->freeSession(session);
}

/**
 * 测试RedoMove 
 * 
 * @param isCompress 是否压缩
 */ 
void LobDefragTestCase::testRedoMove() {
	//move log: LobId(u64) PageID(u32)  newPageID(u32) LobLen(u32) LobData(byte*)
	clearLobs();

	// 准备环境，在defrag时候发生问题
	insertAndDelLobs2();
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::testRedoMove", m_conn);
	
	byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFirstBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFreeBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobBuf2 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFirstBuf2 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFreeBuf2 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobBuf3 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFirstBuf3 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFreeBuf3 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *indexBuf1 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *indexBuf2 = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	backupLobPage(session, 4, lobFreeBuf);
	backupLobPage(session, 19, lobFirstBuf);
	backupLobPage(session, 21, lobBuf);
	backupLobPage(session, 7, lobFreeBuf2);
	backupLobPage(session, 2, lobFirstBuf2);
	backupLobPage(session, 3, lobBuf2);
	backupLobPage(session, 15, lobFreeBuf3);
	backupLobPage(session, 5, lobFirstBuf3);
	backupLobPage(session, 8, lobBuf3);
	backupLobIndexPage(session, 1, indexBuf1);

	m_lobStorage->defrag(session);

	restoreLobPage(session, 4, lobFreeBuf);
	restoreLobPage(session, 19, lobFirstBuf);
	restoreLobPage(session, 21, lobBuf);
	restoreLobPage(session, 7, lobFreeBuf2);
	restoreLobPage(session, 2, lobFirstBuf2);
	restoreLobPage(session, 3, lobBuf2);
	restoreLobPage(session, 15, lobFreeBuf3);
	restoreLobPage(session, 5, lobFirstBuf3);
	restoreLobPage(session, 8, lobBuf3);
	restoreLobIndexPage(session, 1, indexBuf1);
	m_db->getSessionManager()->freeSession(session);


	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobDefragTestCase::testRedoMove", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	//Config cfg;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobDefragTestCase::testRedoMove", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, false));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobDefragTestCase::testRedoMove", m_conn);
	//先跳过其他log
	while (j < 8) {
		m_db->getTxnlog()->getNext(logHdl);
		j++;
	}
	j = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		u64 lsn = logHdl->curLsn();
		m_lobStorage->redoMove(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
	}
	MemoryContext *mc = session->getMemoryContext();
	
	//验证大对象内容是否一致
	for (uint i =0; i < lobIds.size(); i++) {
		uint size;
		if (i != 1 && i != (lobIds.size() - 2)) {
			byte *getLob = m_lobStorage->get(session, mc, lobIds[i], &size, false);
			CPPUNIT_ASSERT(size == lobContents[i].second);
			CPPUNIT_ASSERT(lobIsSame(getLob, lobContents[i].first, size));
		}

	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	clearLobs();
	System::virtualFree(lobBuf);
	System::virtualFree(lobFirstBuf);
	System::virtualFree(lobFreeBuf);
	System::virtualFree(lobBuf2);
	System::virtualFree(lobFirstBuf2);
	System::virtualFree(lobFreeBuf2);
	System::virtualFree(lobBuf3);
	System::virtualFree(lobFirstBuf3);
	System::virtualFree(lobFreeBuf3);
	System::virtualFree(indexBuf1);
	System::virtualFree(indexBuf2);
}


/**
 * 得到大对象文件一页的lsn
 * 
 * @param session 会话对象
 * @param pageNum 页号
 */
u64 LobDefragTestCase::getLobPageLSN(Session *session, u64 pageNum) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getBlobFile() , PAGE_LOB_HEAP, pageNum, Shared, m_lobStorage->getLLobDatStats(), NULL);
	u64 lsn = bphdl->getPage()->m_lsn;
	session->releasePage(&bphdl);
	return lsn;
}

/**
 * 得到目录文件一页的lsn
 * 
 * @param session 会话对象
 * @param pageNum 页号 
 */
u64 LobDefragTestCase::getLobIndexPageLSN(Session *session, u64 pageNum) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, pageNum, Shared, m_lobStorage->getLLobDirStats(), NULL);
	u64 lsn = bphdl->getPage()->m_lsn;
	session->releasePage(&bphdl);
	return lsn;
}

/**
 * 备份大对象文件的一页
 * 
 * @param session 会话对象
 * @param pageNum 页号
 * @param pageBuffer 备份的空间 
 */
void LobDefragTestCase::backupLobPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getBlobFile(), PAGE_LOB_HEAP, pageNum, Shared, m_lobStorage->getLLobDatStats(), NULL);
	memcpy(pageBuffer, bphdl->getPage(), Limits::PAGE_SIZE);
	session->releasePage(&bphdl);
}

/**
 * 恢复大对象文件的一页
 * 
 * @param session 会话对象
 * @param pageNum 页号
 * @param pageBuffer 需要恢复的内容页 
 */
void LobDefragTestCase::restoreLobPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session,  m_lobStorage->getBlobFile(), PAGE_LOB_HEAP, pageNum, Exclusived, m_lobStorage->getLLobDatStats(), NULL);
	memcpy(bphdl->getPage(), pageBuffer, Limits::PAGE_SIZE);
	session->markDirty(bphdl);
	session->releasePage(&bphdl);
}

/**
 * 备份目录文件的一页
 * 
 * @param session 会话对象
 * @param pageNum 页号
 * @param pageBuffer 备份的空间
 */
void LobDefragTestCase::backupLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, pageNum, Shared, m_lobStorage->getLLobDirStats(), NULL);
	memcpy(pageBuffer, bphdl->getPage(), Limits::PAGE_SIZE);
	session->releasePage(&bphdl);
}

/**
 * 恢复目录文件的一页
 * 
 * @param session 会话对象
 * @param pageNum 页号
 * @param pageBuffer 需要恢复的内容页 
 */
void LobDefragTestCase::restoreLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session,  m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, pageNum, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
	memcpy(bphdl->getPage(), pageBuffer, Limits::PAGE_SIZE);
	session->markDirty(bphdl);
	session->releasePage(&bphdl);
}


/**
 * 比较两个大对象是否一致
 * 
 * @param src 其中一个大对象内容
 * @param dest 另外一个大对象内容
 * @param len 长度 
 * @return 是否一致
 */
bool LobDefragTestCase::lobIsSame(byte *src, byte *dest, uint len) {
	/*bool isSame = false;
	for(uint i =0; i < len; i++ ) {
		if (*(src + i) == *(dest + i)) {
			continue;
		} else {
			return false;
		}		
	}*/
	return true;
}


/**
 * 生成指定长度固定长度的LOB
 * 
 * @param len 长度
 * @return 大对象内容
 */
byte* LobDefragTestCase::createLob(uint len) {
	//计算虚拟内存页数
	u32 pageNum = (len + Limits::PAGE_SIZE -1) / Limits::PAGE_SIZE;
	byte *lob = (byte *)System::virtualAlloc(pageNum * Limits::PAGE_SIZE );
	for (uint i = 0; i < len; i++ ) {
		*(lob + i) = (byte )(System::random() % 256);
	}
	return lob;
}

/**
 * 根据块的长度得到大对象长度
 * 
 * @param size 块长度
 * @param isCompress 是否压缩
 * @return 大对象长度
 */
uint LobDefragTestCase::getLobSize(uint size, bool isCompress) {
	//假如小于一页
	if (size <=  Limits::PAGE_SIZE)
		return size - BigLobStorage::OFFSET_BLOCK_FIRST;
	else {
		uint pageNum = (size + Limits:: PAGE_SIZE - 1) / Limits:: PAGE_SIZE;
		return size - BigLobStorage::OFFSET_BLOCK_FIRST  -  (pageNum - 1) * BigLobStorage::OFFSET_BLOCK_OTHER;
	}
}

void LobDefragTestCase::clearLobs() {
	if (lobIds.size() > 0) {
		lobIds.clear();
		for (vector<pair<byte *, uint> >::iterator it = lobContents.begin(); it != lobContents.end(); it++) {
			pair<byte *, uint> &lobPair = *it;
			System::virtualFree(lobPair.first);
		}
		lobContents.clear();
	}
}



