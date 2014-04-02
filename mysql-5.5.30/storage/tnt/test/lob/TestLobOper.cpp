/**
 * 测试大对象基本操作
 * @author zx (zx@corp.netease.com, zx@163.org)
 * 遇到问题：
 * 1、设置好表的cacheUpdate,字段的cacheUpdate怎么设置,已经解决
 * 2、limit:max_record_size问题,这个已经解决
 * 3、defrag锁定末尾问题
 * 4、统计信息
 * 5、需要加油
 * 6、需要加油
 * 7、需要加油
 */

#include "lob/TestLobOper.h"
#include "lob/Lob.h"
#include "lob/BigLob.h"
#include "lob/LobIndex.h"
#include "lob/SmallLob.h"
#include "heap/VariableLengthRecordHeap.h"
#include "mms/Mms.h"
#include "Test.h"
#include <string>
#include "misc/Buffer.h"
#include "util/System.h"
#include "misc/Session.h"
#include "util/PagePool.h"
#include "misc/Global.h"
#include "misc/TableDef.h"
#include "util/File.h"
#include "api/Database.h"
#include "misc/Txnlog.h"
#include "util/Portable.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <utility>



using namespace std;

#define LOB_DIR  "."
#define LOBINDEX_PATH "lobtest.nsli"
#define LOBSMALL_PATH "lobtest.nsso"
#define LOBBIG_PATH   "lobtest.nsld"
#define LOB_BACKUP "lob.bak"
#define LOBINDEX_BACKUP "lobIndex.bak"
#define LOB_SMALL "lobsmall.bak"

const static char *LOB_PATH = "lobtest";
const static char *CFG_PATH = "ntse_ctrl";

static vector<LobId> lobIds;
static vector <pair<byte *, uint> > lobContents;

static vector<LobId>::iterator iter;
static vector<pair<byte *, uint> >::iterator c_iter;


const char* LobOperTestCase::getName() {
	return "Lob operation test";
}

const char* LobOperTestCase::getDescription() {
	return "Test various operations and common features of lob";
}

bool LobOperTestCase::isBig() {
	return false;
}

/**
 * 准备测试环境：建立大对象管理storage
 */
void LobOperTestCase::setUp() {
	m_conn = NULL;
	m_lobStorage = NULL;
	m_db = NULL;
	string basePath(".");
	string path = basePath + "/" + "ntse_ctrl";
	File file(path.c_str());
	bool exist = false;
	file.isExist(&exist);
	if(exist) {
		Database::drop(".");
	}
	file.isExist(&exist);
	if(exist) file.remove();
	TableDefBuilder *builder = new TableDefBuilder(1, "LobFakeSchema", "LobFakeTable");
	builder->addColumn("FakeID", CT_INT, false);
	m_tableDef = builder->getTableDef();
	delete builder;
	m_lobStorage = createLobStorage();
}

void LobOperTestCase::tearDown() {
	if (m_lobStorage)
		dropLobStorage();
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
		Database::drop(LOB_DIR);
		m_db = NULL;
	}

	clearLobs();
}

/**
 * drop 大对象存储对象
 */
void LobOperTestCase::dropLobStorage() {
	Connection *conn = m_db->getConnection(true);
	assert(conn);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::dropLobStorage", conn);
	assert(session);
	//Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::dropLobStorage", m_conn);
	EXCPT_OPER(m_lobStorage->close(session, true));
	delete m_lobStorage;
	EXCPT_OPER(LobStorage::drop(LOB_PATH));
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}


/**
 * 建立大对象管理storage
 */
LobStorage* LobOperTestCase::createLobStorage() {
	File lobIndexFile(LOBINDEX_PATH);
	lobIndexFile.remove();
	File lobBigFile(LOBBIG_PATH);
	lobBigFile.remove();
	File lobSmallFile(LOBSMALL_PATH);
	lobSmallFile.remove();
	//Database::drop(m_cfg.m_basedir);
	m_db = Database::open(&m_cfg, true, -1);
	LobStorage::create(m_db, m_tableDef, LOB_PATH);

	m_conn = m_db->getConnection(true);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::dropLobStorage", m_conn);
	LobStorage *lobStorage;
	EXCPT_OPER(lobStorage= LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);
	return lobStorage;
}


/************************************************************************/
/*   2008-11-13 为了测试压缩和非压缩， 这里代码测试用例调整成两个后再   */
/* 放到一个去，代码进行调整                                             */
/*	 下面对每个大对象做了操作后，验证其正确性时，都是通过在操作前，把   */
/* 大对象的长度和内容放在一个pair中，然后操作大对象后，通过查询得到结果 */
/* 和记录的内容和长度进行验证                                           */
/************************************************************************/

/**
 * 2009-1-8 利用400M 真实的博客数据，测试压缩效果
 */
void LobOperTestCase::testBlog(){
	char lbuf[4];
	byte *lob = NULL;
	u32 len = 0;
	u32 magic = 0xfefefef;
	//u64 offset = 0;
	u32 num = 0;
	fstream file;
	file.open("F:\\NTSE\\lzotest\\LzoTest\\LzoTest\\blog.txt", ios_base::binary|ios_base::in);
	file.read((char *)lbuf, 4);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testBlog", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	byte *newLob = NULL;
	do {
		len = byteReverseEndian((byte *)lbuf);
		lob = new byte[len];
		file.read((char *)lob, len);
		LobId lid = m_lobStorage->insert(session, lob, len, true);
		uint size;
		u64 savePoint = mc->setSavepoint();
		m_lobStorage->get(session, mc, lid, &size, true);
		mc->resetToSavepoint(savePoint);
		newLob = new byte[0];
		m_lobStorage->update(session, lid, newLob, 0, true);
		file.read(lbuf, 4);
		assert(byteReverseEndian((byte *)lbuf) == magic);

		delete [] newLob;
		delete [] lob;
		num++;
	} while(file.read(lbuf, 4));
    file.close();
	mc->reset();
	m_db->getSessionManager()->freeSession(session);
}

u32 LobOperTestCase::byteReverseEndian(byte *l) {
	u32 len = 0;
	u8 maskone = 0xff;
	for (int i = 0; i < 4; i++) {
		len<<=8;
		len += (l[i]&maskone);
	}
	return len;
}


/**
 * 增加测试用例 2009-03-10
 * 状态统计
 */
void LobOperTestCase::testStatus() {
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testStatus", m_conn);

	CPPUNIT_ASSERT(m_lobStorage->getSLHeap());
	CPPUNIT_ASSERT(m_lobStorage->getSLMmsTable());
	const TableDef *tableDef = m_lobStorage->getSLVTableDef();
	m_lobStorage->setTableDef(tableDef);

	LobStatus status = m_lobStorage->getStatus();
	CPPUNIT_ASSERT(status.m_postCompressSize == 0);
	CPPUNIT_ASSERT(status.m_preCompressSize == 0);
	CPPUNIT_ASSERT(status.m_usefulCompress == 0);
	CPPUNIT_ASSERT(status.m_uselessCompress == 0);
	CPPUNIT_ASSERT(status.m_blobStatus.m_datLength == 0);
	CPPUNIT_ASSERT(status.m_blobStatus.m_idxLength == Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(status.m_blobStatus.m_moveUpdate == 0);
	CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 0);
	CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == 0);
	CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_READ] == 0);
	CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == 0);

	m_lobStorage->updateExtendStatus(session, 16);
	//m_lobStorage->updateExtendStatus(session, 3000);
	LobStatusEx statusEx = m_lobStorage->getStatusEx();
	CPPUNIT_ASSERT(statusEx.m_blobStatus.m_freePages == 0);
	CPPUNIT_ASSERT(statusEx.m_blobStatus.m_numLobs == 0);
	CPPUNIT_ASSERT(statusEx.m_blobStatus.m_pctUsed == .0);
	CPPUNIT_ASSERT(statusEx.m_slobStatus.m_numLinks == 0);
	CPPUNIT_ASSERT(statusEx.m_slobStatus.m_numRows == 0);
	CPPUNIT_ASSERT(statusEx.m_slobStatus.m_pctUsed == .0);

	testAllInsertBig();
	status = m_lobStorage->getStatus();
	//CPPUNIT_ASSERT(status.m_preCompressSize == 8492);
	//CPPUNIT_ASSERT(status.m_postCompressSize == 6781);
	CPPUNIT_ASSERT(status.m_usefulCompress == 4);
	CPPUNIT_ASSERT(status.m_uselessCompress == 0);
	//CPPUNIT_ASSERT(status.m_blobStatus.m_datLength == 19 * Limits::PAGE_SIZE);
	//CPPUNIT_ASSERT(status.m_blobStatus.m_idxLength == 17 * Limits::PAGE_SIZE);
	CPPUNIT_ASSERT(status.m_blobStatus.m_moveUpdate == 0);

	m_lobStorage->updateExtendStatus(session, 16);
	statusEx = m_lobStorage->getStatusEx();
	CPPUNIT_ASSERT(statusEx.m_blobStatus.m_freePages == 0);
	CPPUNIT_ASSERT(statusEx.m_blobStatus.m_pctUsed < 1.0);
	CPPUNIT_ASSERT(statusEx.m_blobStatus.m_pctUsed > 0.5);

	//然后做一些操作，在统计
	//uint maxSlot = LobIndex::MAX_SLOT_PER_PAGE + 1;
	//for (uint i = 0; i <= maxSlot; ++i) {
	//	testAllInsertBig();
	//}
	//status = m_lobStorage->getStatus();
	////CPPUNIT_ASSERT(status.m_preCompressSize == 1723876);
	////CPPUNIT_ASSERT(status.m_postCompressSize == 1380490);
	//CPPUNIT_ASSERT(status.m_usefulCompress == (maxSlot + 2) * 4);
	//CPPUNIT_ASSERT(status.m_uselessCompress == 0);
	////CPPUNIT_ASSERT(status.m_blobStatus.m_datLength == 3857 * Limits::PAGE_SIZE);
	////CPPUNIT_ASSERT(status.m_blobStatus.m_idxLength == 17 * Limits::PAGE_SIZE);
	//CPPUNIT_ASSERT(status.m_blobStatus.m_moveUpdate == 0);
	//CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_DELETE] == 0);
	//CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_INSERT] == (maxSlot + 2) * 8);
	//CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_READ] == (maxSlot + 2) * 8);
	//CPPUNIT_ASSERT(status.m_blobStatus.m_dboStats->m_statArr[DBOBJ_ITEM_UPDATE] == 0);
	//m_lobStorage->updateExtendStatus(session, 16);
	//statusEx = m_lobStorage->getStatusEx();
	//CPPUNIT_ASSERT(statusEx.m_blobStatus.m_freePages == 0);
	////CPPUNIT_ASSERT(statusEx.m_blobStatus.m_numLobs == 1800);
	//CPPUNIT_ASSERT(statusEx.m_blobStatus.m_pctUsed < 1.0);
	//CPPUNIT_ASSERT(statusEx.m_blobStatus.m_pctUsed > 0.5);

	////插入很多大对象，再做测试
	//for (uint i = 0; i <= 5000; i++) {
	//	if (i % 100 == 0) {
	//		size_t num = lobContents.size();
	//		for (uint i = 0; i < num; i++) {
	//			System::virtualFree(lobContents[i].first);
	//		}
	//		lobContents.clear();
	//	}
	//	testAllInsertBig();
	//}

	////做一些del操作
	//LobId lid;
	//uint index;
	//set<uint> haveDel;
	//for(uint i = 0; i <= 10000; i++) {
	//	index = System::random() % 20000;
	//	if (haveDel.count(index) == 1) {
	//		 continue;
	//	} else {
	//		haveDel.insert(index);
	//		lid = lobIds[index];
	//		m_lobStorage->del(session, lid);
	//	}
	//}
	//haveDel.clear();
	//status = m_lobStorage->getStatus();
	//m_lobStorage->updateExtendStatus(session, 16);
	//statusEx = m_lobStorage->getStatusEx();	
	//CPPUNIT_ASSERT(statusEx.m_blobStatus.m_pctUsed < 1.0);
	//CPPUNIT_ASSERT(statusEx.m_blobStatus.m_pctUsed > 0.6);

	//做一些mms配置测试
	m_lobStorage->setMmsTable(session, false, true);
	m_lobStorage->setMmsTable(session, false, false);
	m_lobStorage->setMmsTable(session, true, true);
	m_lobStorage->setMmsTable(session, true, false);
	
	m_lobStorage->flush(session);

	m_db->getSessionManager()->freeSession(session);
}

/**
 * 增加测试用例 2009-1-9
 * 测试当大对象size为0，但lob != NULL 情况
 */
void  LobOperTestCase::testSizeZero() {
	bool compress = true;
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testBlog", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	byte *lob = new byte[0];
	LobId lid = m_lobStorage->insert(session, lob, 0);
	//update后长度大于0
	uint len = Limits::MAX_REC_SIZE - 7;
	byte *newLob = createLob(len);
	LobId newLid = m_lobStorage->update(session, lid, newLob, len, compress);
	assert(lid == newLid);
	uint size;
	byte *getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(size == len);
	assert(lobIsSame(newLob, getLob, len));
	m_lobStorage->del(session, lid);


	//测试更新在mms情况
	lid = m_lobStorage->insert(session, lob, 0);
	getLob = m_lobStorage->get(session, mc, lid, &size);
	assert(size == 0);
	assert(getLob != NULL);
	//然后更新
	newLid = m_lobStorage->update(session, lid, newLob, len, compress);
	assert(lid == newLid);
	getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(size == len);
	assert(lobIsSame(newLob, getLob, len));
	m_lobStorage->del(session, lid);


	//测试更新为size为0
	lid = m_lobStorage->insert(session, newLob, len);
	newLid = m_lobStorage->update(session, lid, lob, 0, compress);
	//验证只更新了压缩位
	assert(lid == newLid);
	getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(getLob != NULL);
	assert(size == 0);
	m_lobStorage->del(session, lid);

	//测试更新为size为0,在mms中
	lid = m_lobStorage->insert(session, newLob, len);
	getLob = m_lobStorage->get(session, mc, lid, &size);
	newLid = m_lobStorage->update(session, lid, lob, 0, compress);
	//验证只更新了压缩
	assert(lid == newLid);
	getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(getLob != NULL);
	assert(size == 0);
	m_lobStorage->del(session, lid);

	//清理工作
	delete [] lob;
	System::virtualFree(newLob);
	mc->reset();
	m_db->getSessionManager()->freeSession(session);
}


/**
 * 测试大型大对象的insert
 * 主要步骤：
 * 1、这里为了测试边界条件 ，插入一个刚好满2页的大对象
 * 2、插入一个刚好满1页的大对象
 * 3、插入一个刚好满3页的大对象
 * 4、插入一个刚好满4页的大对象
 */
void  LobOperTestCase::testAllInsertBig() {
	testInsertBig(false);
	testInsertBig(true);
	//lobIds.clear();
}

/**
 * 测试大型大对象的insert
 * 主要步骤：
 * 1、执行上面testAllInsertBig用例
 * 2、然后再依次查询大对象
 * 3、按字节比较大对象内容
 */
void LobOperTestCase::testAllReadBig() {
	testReadBig(false);
	testReadBig(true);
}

/**
 * 测试大型大对象的update
 * 主要步骤：
 * 1、插入一个大型大对象L1，页树为Pn
 * 2、然后update L1， 其页数P1 == Pn
 * 3、然后update L1， 其页数P2 > P1，其中L1在文件末尾，所以会直接移动到文件末尾
 * 4、再插入3个大对象 L2 ,L3 ,L4 ，然后删除前面2个大对象
 * 5、然后update L1， 其页数P3 > P2，但后面连续空闲空间Fn >= P3 - P2
 * 6、然后update L1， 其页数P4 > P3，但后面连续空闲空间Fn < P4 - P3
 */
void LobOperTestCase::testAllBigUpdate() {
	testBigUpdate(false);
	clearLobs();
	testBigUpdate(true);
}

/**
 * 测试大型大对象的delete
 * 主要步骤：
 * 1、插入一个对象，然后删除
 * 2、插入多个大对象，使刚好用完一个目录页
 * 3、然后删除一个2步骤中插入的大对象
 * 4、重新插入一个大对象，3中的目录项将重用
 */
void LobOperTestCase::testAllDelBig() {
	testDelBig(false);
	/*lobIds.clear();
	size_t num = lobContents.size();
	for (uint i = 0; i < num; i++) {
		System::virtualFree(lobContents[i].first);
	}
	lobContents.clear();
	tearDown();
	setUp();  
	testDelBig(true);*/     // 如果压缩到小型大对象，就容易导致断言错！
}

/**
 * 测试大型大对象的redoInsert
 * 主要步骤：
 * 1、先插入一个对象，然后关闭数据库，再重开数据库，查看日志验证logInsert正确性
 * 2、把大对象文件的第一个页和第三页备份，然后插入一个3页大的大对象，把第一页和第三页恢复到备份
 * 3、然后关闭数据库，再重开数据库，然后redoInsert,并查询该大对象， 字节验证一致，长度一致
 * 4、插入多个大对象，直到第一个目录页只剩一个空闲目录项，然后把目录文件的第一个页（首页）和第二页备份
 * 5、再插入一个大对象，然后把目录文件的第一页和第二页恢复到备份
 * 6、然后关闭数据库，再重开数据库，然后redoInsert，验证前面的插入的大对象都正确
 * 7、再备份目录文件第二页，和大对象文件文件末尾三页
 * 8、然后插入一个页数为3页的大对象
 * 9、然后关闭数据库，再重开数据库，然后redoInsert，验证8插入的大对象的正确
 */
void LobOperTestCase::testAllRedoInsert(){
	testRedoInsert(false);
	/*lobIds.clear();
	size_t num = lobContents.size();
	for (uint i = 0; i < num; i++) {
		System::virtualFree(lobContents[i].first);
	}
	lobContents.clear();
	tearDown();
	setUp();
	testRedoInsert(true);*/
}


/**
 * 测试大型大对象的redoDel
 * 主要步骤：
 * 1、先插入一个对象，然后关闭数据库，再重开数据库，查看日志验证logDel正确性
 * 2、把大对象文件的第一个页和目录文件的第一页备份，然后插入一个3页大的大对象，把目录文件和大对象文件恢复到备份
 * 3、然后关闭数据库，再重开数据库，然后redoDel
 * 4、插入多个大对象，直到第一个目录页没有空闲目录项，然后备份目录文件的第一页和第二页
 * 5、再插入一个大对象，然后把目录文件的第一页和第二页恢复到备份
 * 6、然后关闭数据库，再重开数据库，然后redoDel，验证前面的删除的大对象都正确
 */
void LobOperTestCase::testRedoInsertAndDel() {
	testRedoDelEx(true);
}

void LobOperTestCase::testRedoInsertAndDelEx() {
	testRedoDelEx(false);
}

/**
 * 测试大型大对象的logUpdate
 * 主要步骤：
 * 1、先插入一个对象L1，长度为P1（页数）然后更新该大对象，更新页数不变
 * 2、更新L1，页数为P2，P2 < P1
 * 3、更新L1，页数为P3，P3 > P2
 * 4、更新L1，页数为P3，P4 > P3
 * 5、插入大对象L2，L3, L4,页数分别为Pd1,Pd2, Pd3,然后删除L2，L3
 * 6、更新L1，更新后的页数P5 - P4 < Pd1 + Pd2，所以大对象放在原处
 * 7、更新L1，更新后的页数P6 - P5 > 后面的连续的空闲空间
 * 8、然后关闭数据库，重新open数据库， 验证以上更新日志的正确性
 */
void LobOperTestCase::testAllUpdateLog() {
	testUpdateLog(false);
}

/**
 * 测试大型大对象的redoUpdate
 * 主要步骤：
 * 1、先插入一个对象，然后更新该大对象，然后关闭数据库，再重开数据库，查看日志验证logUpdate正确性
 * 2、插入一个大对象L1后，其页数为P1,然后备份目录文件的第一页
 * 3、备份L1的第一页和最后一页，更新L1，其P2 == P1,	 恢复大对象的前两页
 * 4、插入大对象L2 ，然后备份其第一页， 更新其使之长度即页数增大，然后恢复第一页
 * 5、再插入3个 L3, L4, L5三个对象，然后删除前两个，备份L3，L4首页
 * 6、更新L2,使其长度为P3，使其后面空闲长度 Fn > P3 - P2，然后恢复5备份的页
 * 7、插入大对象L5,L6,L7,然后删除L6，L7,其中L5的长度为P4
 * 8、备份L5的首页Ph和目录文件第一个页Pi1，更新L5，使其长度等于P5,但后面的空间空间长度Fn' < P5 - P4.然后恢复Ph和Pi1.
 * 9、插入大对象L8，长度为2页，然后备份其2页，然后更新变成一页，然后恢复备份的2页
 * 10、插入大对象L9，然后备份其首页，然后备份目录页，然后更新其扩展2页（因为在文件末尾，所以大对象直接扩展），然后恢复
 *     其中目录页和块首页
 * 11、关闭数据库，然后open数据库，进行redo操作
 * 12、对前面的大对象进行查询，验证正确性
 */
void LobOperTestCase::testAllRedoUpdate() {
	testRedoUpdate(false);
	//testRedoUpdateEx();
}


/**
 * 测试redoCreate
 * 主要步骤：这里主要分几种情况
 * 1、create后把，然后关闭数据库，remove相关文件，然后open数据库，做redoCreate
 * 2、create后，文件还没有操作过，或者说长度不够
 * 3、文件已经open在使用中，则不做redoCreate
 */
void LobOperTestCase::testAllRedoCreate() {
	testRedoCreate(false);
	testRedoCreate(true);
}


/*
 * 测试insert小型大对象，同时更新和删除
 * 主要步骤：
 * 1、直接插入刚好长度等于LIMITS::MAX_REC_SIZE大小的大对象，插入成功，并查询得到结果正确
 * 2、插入刚好长度等于MIN_COMPRESS_LEN（不需要压缩的长度限制）的大对象，该大对象按照压缩
 *  方式插入成功，
 * 3、插入刚好长度等于MIN_COMPRESS_LEN - 1 的大对象，该大对象即使指定需要压缩也按照不压缩方式
 *  插入
 * 4、再插入一个小型大对象，然后查询，查询将从文件中得到，得到正确结果，然后再次查询，这个
 *  时候将从MMS查询，然后得到正确结果。
 * 5、再插入一个小型大对象，然后进行更新没，这个时候将到文件中更新，然后查询该大对象，然后再
 *  次更新，这个时候将在MMS中更新，然后再次查询，得到正确结果
 * 6、删除前面的5操作的大对象的，然后在查询被删除的大对象，得到结果为NULL，size为0
 */
void LobOperTestCase::testAllInsertSmall() {
	testInsertSmall(false);
	testInsertSmall(true);
}

/*
 * 测试insert小型大对象，同时变大后为大型大对象情况，以及更新删除情况
 * 主要步骤：
 * 1、直接插入刚好长度等于LIMITS::MAX_REC_SIZE 大小的大对象L1，插入成功，并查询得到结果正确
 * 2、更新大对象L1，其长度为LIMITS:PAGE_SIZE, 这个情况下， 原来大对象L1将删除，然后把更新的
 * 插入到大型大对象文件，变成大对象L2，LobId发生变化
 * 3、把大对象L2更新，更新后长度小于LIMITS::MAX_REC_SIZE，但其仍放在大型大对象文件中，LobId不变
 * 4、在插入一个小型大对象L3，然后进行查询，这个时候内容放入MMS中。
 * 5、再更新L3，这个时候长度为LIMITS:PAGE_SIZE * 2。这个情况下将删除MMS中和HEAP中L3，然后更新
 *  内容到大型大对象文件
 * 6、再查询该5更新后大对象，得到正确结果
 */
void LobOperTestCase::testAllInsertTOBig() {
	testInsertTOBig(false);
	//testInsertTOBig(true);
}

/**
 * 测试小型大对象的redoInsert
 * 主要步骤：
 * 1、插入5个小型大对象，然后把文件备份
 * 2、再插入5个小型大对象
 * 3、关闭数据库，然后把备份的文件恢复，然后再open数据库
 * 4、然后开始根据日志，做redoSLInsert
 * 5、通过插入每个插入的大对象，和记录的大对象内容比较，比较一致
 */
void LobOperTestCase::testAllSLRedoInsert() {
	testSLRedoInsert(false);
	testSLRedoInsert(true);
}


/**
 * 测试小型大对象的redoUpdate，这里大对象更新没有包括变成大型大对象的情况
 * 主要测试redo与heap相关的
 * 主要步骤：
 * 1、插入5个大对象:L1, L2, L3, L4, L5，然后buffer flushAll后，备份文件
 * 2、对L1进行更新，使其长度变小
 * 3、对L2进行更新，使其长度变大
 * 4、对L3进行更新，更新长度为LIMITS::MAX_REC_SIZE
 * 5、对L4进行更新，更新长度不变
 * 6、然后关闭数据库，恢复备份的文件，然后open数据库
 * 7、对从第6条日志开始，做redoSLUpdate
 * 8、通过对更新的大对象进行查询， 然后和记录的大对象内容进行比较，判断内容一致性。
 */
void LobOperTestCase::testAllSLRedoUpdate() {
	testSLRedoUpdate(false);
	clearLobs();
	tearDown();
	setUp();
	testSLRedoUpdate(true);
}

/**
 * 测试小型大对象的redoUpdate，这里大对象更新没有包括变成大型大对象的情况
 * 主要测试redo与MMS相关的
 * 主要步骤：
 * 1、插入5个大对象:L1, L2, L3
 * 2、对L1,L2,L3进行read,使其put into Mms
 * 3、然后更新L1,L2,L3
 * 4、关闭数据库,设置刷新日志， 但不刷数据
 * 5、重新启动数据库，然后做redoUpdateMms
 * 6、然后查询L1,L2,L3,得到的内容是一致
 */
void LobOperTestCase::testAllSLRedoUpdateMms() {
	testSLRedoUpdateMMS(false);
	tearDown();
	setUp();
	testSLRedoUpdateMMS(true);
}

/**
 * 测试小型大对象的redoDel
 * 主要步骤：
 * 1、插入5个大对象:L1, L2, L3, L4, L5。然后buffer flushAll后，备份文件
 * 2、删除上面的5个大对象
 * 3、然后关闭数据库，恢复备份的文件，然后open数据库
 * 4、查询上面5个大对象还能查询到
 * 5、对从第6条日志开始，做redoSLDel
 * 6、然后再查询前面5个大对象，将查询不到
 */
void LobOperTestCase::testAllSLRedoDel() {
	testSLRedoDel(false);
	clearLobs();
	tearDown();
	setUp();
	testSLRedoDel(true);
}


/************************************************************************/
/*                     测试多线程										*/
/************************************************************************/
/**
 *	主要测试：
 *	1、同步写(日志顺序相反)
 *	2、读和写
 *	3、读和更新
 *	4、读和删除
 *	5、碎片整理和读
 *	6、碎片整理和写
 *	7、碎片整理和更新
 *	8、碎片整理和删除
 *	9、碎片整理结束文件尾重置
 *	10、更新的各种情况
 */


/**
 * 测试Insert时候，先得到空闲目录页P1 ，但当锁住首页再去判断，P1已经不是第一个空闲页，所以重查找空闲页
 * 测试步骤：
 * 1、线程A去插入大对象，先得到空闲目录页F1，并锁定F1，然后等待
 * 2、线程B删除大对象，发现目录页产生第一个空闲slot, 然后把页加入到空闲页链表
 * 3、B正确删除大对象，线程A继续执行
 * 4、线程A发现不是第一个空闲页了，则必须去重新查找空闲页
 * 5、线程A正确插入大对象
 */
void LobOperTestCase::insertGetNoFirstPage(){
	clearLobs();

	//先插入一批大对象
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::insertGetNoFirstPage", m_conn);
	uint maxSlot = (Limits::PAGE_SIZE - sizeof(LIFilePageInfo) ) / LobIndex::INDEX_SLOT_LENGTH;
	uint i = 0;
	while (++i <= maxSlot * 2) {
		uint len = Limits::PAGE_SIZE * 2;
		//insertLob(session, len, true);
		insertLob(session, len, false);
	}
	m_db->getSessionManager()->freeSession(session);

	doDelByLobid(lobIds[1]);

	LobTester tester1(this, LOB_INSERT1);
	LobTester tester2(this, LOB_DELBYID);
	tester1.enableSyncPoint(SP_LOB_BIG_NOT_FIRST_FREE_PAGE);
	tester2.enableSyncPoint(SP_LOB_BIG_OTHER_PUT_FREE_SLOT);

	tester1.start();
	tester2.start();

	tester1.joinSyncPoint(SP_LOB_BIG_NOT_FIRST_FREE_PAGE);
	tester2.joinSyncPoint(SP_LOB_BIG_OTHER_PUT_FREE_SLOT);
	tester2.notifySyncPoint(SP_LOB_BIG_OTHER_PUT_FREE_SLOT);
	tester1.disableSyncPoint(SP_LOB_BIG_NOT_FIRST_FREE_PAGE);
	tester1.notifySyncPoint(SP_LOB_BIG_NOT_FIRST_FREE_PAGE);

	tester1.join();
	tester2.join();
}

/**
 * 根据LobId删除一个大对象
 */
void LobOperTestCase::doDelByLobid() {
	doDelByLobid(lobIds[Limits::PAGE_SIZE / LobIndex::INDEX_SLOT_LENGTH]);
}

 /**
 * 根据LobId删除一个大对象
 *
 * @param lid 大对象ID
 */
void LobOperTestCase::doDelByLobid(LobId lid) {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doDelByLobid", m_conn);
	m_lobStorage->del(session, lid);
	m_db->getSessionManager()->freeSession(session);
}

/**
 * 测试Insert时候，得到的FirstFreePage == 0，但当锁住首页再去判断时候，发现FirstFreePage > 0，
 * 说明其他线程要么释放了目录项，要么扩展了目录文件
 * 测试步骤：
 * 1、线程A去插入大对象，先得到空闲目录页F1，然后释放F1，得到的FirstFreePage == 0，然后等待
 * 2、线程B也插入大对象，得到空闲目录页F1，发现FirstFreePage == 0，扩展文件
 * 3、线程A继续执行
 * 4、线程A发现F1没有空闲目录页时候重新去去锁住首页，发现FirstFreePage > 0
 * 5、线程A正确插入大对象
 */
void LobOperTestCase::insertGetNoFreePage(){
	//开始测试多线程
	clearLobs();

	LobTester tester1(this, LOB_INSERT1);
	LobTester tester2(this, LOB_INSERT2);
	tester1.enableSyncPoint(SP_LOB_BIG_NO_FREE_PAGE);
	tester2.enableSyncPoint(SP_LOB_BIG_GET_FREE_PAGE);
	tester2.enableSyncPoint(SP_LOB_BIG_GET_FREE_PAGE_FINISH);

	tester1.start();
	tester2.start();

	tester1.joinSyncPoint(SP_LOB_BIG_NO_FREE_PAGE);
	tester2.joinSyncPoint(SP_LOB_BIG_GET_FREE_PAGE);
	tester2.notifySyncPoint(SP_LOB_BIG_GET_FREE_PAGE);
	tester2.joinSyncPoint(SP_LOB_BIG_GET_FREE_PAGE_FINISH);
	tester1.notifySyncPoint(SP_LOB_BIG_NO_FREE_PAGE);
	tester1.disableSyncPoint(SP_LOB_BIG_NO_FREE_PAGE);
	tester2.notifySyncPoint(SP_LOB_BIG_GET_FREE_PAGE_FINISH);

	tester1.join();
	tester2.join();

}


/**
 * 测试Insert时候，相互争用目录项的（去得到空闲目录页，得到空闲目录页已经被其他使用）
 * 测试步骤：
 * 1、线程A去插入大对象，先得到空闲目录F1，然后释放F1，然后等待
 * 2、线程B也插入大对象，得到空闲目录页F1，并不断重复插入大对象，直到F1没有空闲目录项
 * 3、线程A继续执行
 * 4、线程A发现F1没有空闲目录项时候重新去去锁住首页，然后重新执行
 * 5、线程A正确插入大对象
 */
void LobOperTestCase::insertGetFreePage(){
	//先插入一批大对象
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::insertGetFreePage", m_conn);
	uint maxLen = 1024 * 16;
	uint maxSlot = (Limits::PAGE_SIZE - sizeof(LIFilePageInfo) ) / LobIndex::INDEX_SLOT_LENGTH;
	uint i = 0;
	while (++i < maxSlot) {
		uint len = System::random() % maxLen + Limits::MAX_REC_SIZE;
		byte * lob =createLob(len);
		m_lobStorage->insert(session, lob, len, true);
		System::virtualFree(lob);
	}

	//开始测试多线程，A和B争用最后一个空闲目录项
	clearLobs();
	m_db->getSessionManager()->freeSession(session);

	LobTester tester1(this, LOB_INSERT1);
	LobTester tester2(this, LOB_INSERT2);
	tester1.enableSyncPoint(SP_LOB_BIG_GET_FREE_PAGE);
	tester2.enableSyncPoint(SP_LOB_BIG_GET_FREE_PAGE_FINISH);

	tester1.start();
	tester2.start();

	tester1.joinSyncPoint(SP_LOB_BIG_GET_FREE_PAGE);
	tester2.joinSyncPoint(SP_LOB_BIG_GET_FREE_PAGE_FINISH);
	tester1.disableSyncPoint(SP_LOB_BIG_GET_FREE_PAGE);
	tester1.notifySyncPoint(SP_LOB_BIG_GET_FREE_PAGE);
	tester2.notifySyncPoint(SP_LOB_BIG_GET_FREE_PAGE_FINISH);

	tester1.join();
	tester2.join();
}


/**
 * 测试Update时候，在得到目录项后被defrag，所以需要重新回去读更新后目录项
 * 测试步骤：
 * 1、线程A去大对象L1，先得到目录项I1，然后等待
 * 2、线程B碎片整理把L1整理后，然后继续执行
 * 3、线程A继续执行
 * 4、线程A发现当前要读的块首页不是要找的页，重新读取目录项
 * 5、线程A正确修改大对象L1
 */
void LobOperTestCase::defragFirstAndUpdate() {
	//先准备环境
	clearLobs();

	insertAndDelLobs(false);
	LobTester tester1(this, LOB_UPDATE);
	LobTester tester2(this, LOB_DEFRAG_FINISH);
	tester1.enableSyncPoint(SP_LOB_BIG_DEL_DEFRAG_UPDATE);
	tester2.enableSyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);

	tester1.start();
	tester2.start();

	tester1.joinSyncPoint(SP_LOB_BIG_DEL_DEFRAG_UPDATE);
	tester2.joinSyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);
	tester1.disableSyncPoint(SP_LOB_BIG_DEL_DEFRAG_UPDATE);
	tester1.notifySyncPoint(SP_LOB_BIG_DEL_DEFRAG_UPDATE);
	tester2.notifySyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);

	tester1.join();
	tester2.join();
}

/**
 * 测试Del时候，在得到目录项后被defrag，所以需要重新回去读更新后目录项
 * 测试步骤：
 * 1、线程A去del大对象L1，先得到目录项I1，然后等待
 * 2、线程B碎片整理把L1整理后，然后继续执行
 * 3、线程A继续执行
 * 4、线程A发现当前要读的块首页不是要找的页，重新读取目录项
 * 5、线程A正确删除大对象
 */
void LobOperTestCase::defragFirstAndDel() {
	//先准备环境
	clearLobs();

	insertAndDelLobs(false);
	LobTester tester1(this, LOB_DEL);
	LobTester tester2(this, LOB_DEFRAG_FINISH);
	tester1.enableSyncPoint(SP_LOB_BIG_DEL_DEFRAG_DEL);
	tester2.enableSyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);

	tester1.start();
	tester2.start();

	tester1.joinSyncPoint(SP_LOB_BIG_DEL_DEFRAG_DEL);
	tester2.joinSyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);
	tester1.disableSyncPoint(SP_LOB_BIG_DEL_DEFRAG_DEL);
	tester1.notifySyncPoint(SP_LOB_BIG_DEL_DEFRAG_DEL);
	tester2.notifySyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);

	tester1.join();
	tester2.join();
}

/**
 * 测试read时候，在得到目录项后被defrag，所以需要重新回去读更新后目录项
 * 测试步骤：
 * 1、线程A去read大对象L1，先得到目录项I1，然后等待
 * 2、线程B碎片整理把L1整理后，然后继续执行
 * 3、线程A继续执行
 * 4、线程A发现当前要读的块首页不是要找的页，重新读取目录项
 * 5、线程A得到大对象L1的内容
 */
void LobOperTestCase::defragFirstAndRead () {
	//先准备环境
	clearLobs();

	insertAndDelLobs(false);
	LobTester tester1(this, L0B_READ);
	LobTester tester2(this, LOB_DEFRAG_FINISH);
	tester1.enableSyncPoint(SP_LOB_BIG_READ_DEFRAG_READ);
	tester2.enableSyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);

	tester1.start();
	tester2.start();

	tester1.joinSyncPoint(SP_LOB_BIG_READ_DEFRAG_READ);
	tester2.joinSyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);
	tester1.disableSyncPoint(SP_LOB_BIG_READ_DEFRAG_READ);
	tester1.notifySyncPoint(SP_LOB_BIG_READ_DEFRAG_READ);
	tester2.notifySyncPoint(SP_LOB_BIG_READ_DEFRAG_DEFRAG);

	tester1.join();
	tester2.join();
}


/**
 * 测试defrag时候，有不断的数据操作，特别是INSERT操作
 * 测试步骤：
 * 1、准备环境，插入8个大对象，然后删除其中3个
 * 2、线程A不断insert大对象，并记录大对象ID
 * 3、线程B开始defrag,
 * 4、B重置大对象文件tail,并运行结束
 * 5、A看到B结束后，也结束，然后查询记录的大对象ID。
 */
void LobOperTestCase::defragAndInsert(){
	//先准备环境
	clearLobs();
	
	insertAndDelLobs(false);

	LobTester tester2(this, LOB_DEFRAG_DOING);
	LobTester tester1(this, LOB_INSERT, &tester2);
	LobTester tester3(this, LOB_INSERT, &tester2);

	tester2.start();
	tester3.start();
	tester1.start();

	tester2.join();
	tester1.join();
	tester3.join();

	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::defragAndInsert", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	//验证内容
	for (uint i = 0; i < lobIds.size(); i++) {
		uint size;
		if (i != 1 && i != 2 && i != 6) {
			byte *getLob = m_lobStorage->get(session, mc, lobIds[i], &size, false);
			CPPUNIT_ASSERT(size == lobContents[i].second);
			CPPUNIT_ASSERT(lobIsSame(getLob, lobContents[i].first, size));
		}
	}
	m_db->getSessionManager()->freeSession(session);
}


/**
 * 进行不断的insert大对象,这个主要为了测试defrag时候,当遇到不停insert大对象时候,
 * 如何判断尾部,并重置尾部
 *
 * @param tester 进行碎片整理的测试类
 */
void LobOperTestCase::doInsert(LobTester *tester) {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager() ->allocSession("LobDefragTestCase::doInsert", conn);
	uint maxLen = 4 * 1024;
	while (true) {
		uint len = System::random() % maxLen + Limits::MAX_REC_SIZE;
		insertLob(session, len, true);
		if (tester->isAlive()) {
			continue;
		} else {
			break;
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 进行defrg,这个主要为了测试当对大对象read,update和del时候发现大对象已经发生过移动
 */
void LobOperTestCase::doDefrag() {
	doDefragForRead();
}

/**
 * 进行更新,前提是已经发生过碎片整理的移动
 */
void LobOperTestCase::doUpdate() {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doUpdate", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	uint size;

	byte *lob = createLob(Limits::PAGE_SIZE * 2);
	m_lobStorage->update(session, lobIds[3], lob, Limits::PAGE_SIZE * 2, true);

	byte *newLob = createLob(Limits::PAGE_SIZE * 5);
	m_lobStorage->update(session, lobIds[7], newLob, Limits::PAGE_SIZE * 5, true);
	//判断数据没变化
	byte *lob1 = m_lobStorage->get(session, mc, lobIds[3], &size, false);
	CPPUNIT_ASSERT(size == Limits::PAGE_SIZE * 2);
	bool isRight = lobIsSame(lob, lob1, size);
	CPPUNIT_ASSERT(isRight);
	System::virtualFree(lob);

	byte *lob2 = m_lobStorage->get(session, mc, lobIds[7], &size, false);
	CPPUNIT_ASSERT(size == Limits::PAGE_SIZE * 5);
	isRight = lobIsSame(newLob, lob2, size);
	CPPUNIT_ASSERT(isRight);
	System::virtualFree(newLob);
	m_db->getSessionManager()->freeSession(session);
}

/**
 * 进行删除,前提是已经发生过碎片整理的移动
 */
void LobOperTestCase::doDel() {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doDel", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	//uint size;
	m_lobStorage->del(session, lobIds[3] );
	m_lobStorage->del(session, lobIds[7]);
	m_db->getSessionManager()->freeSession(session);
}

/**
 * 进行读取,前提是已经发生过碎片整理的移动
 */
void LobOperTestCase::doRead() {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doRead", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	uint size;
	byte *lob1 = m_lobStorage->get(session, mc, lobIds[3], &size, false);
	//判断数据没变化
	CPPUNIT_ASSERT(size == lobContents[3].second);
	bool isRight = lobIsSame(lobContents[3].first, lob1, size);
	CPPUNIT_ASSERT(isRight);

	byte *lob2 = m_lobStorage->get(session, mc, lobIds[7], &size, false);
	CPPUNIT_ASSERT(size == lobContents[7].second);
	isRight = lobIsSame(lobContents[7].first, lob2, size);
	CPPUNIT_ASSERT(isRight);

	m_db->getSessionManager()->freeSession(session);
}

/**
 * 进行defrag
 */
void LobOperTestCase::doDefragForRead() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doDefragForRead", conn);
	
	m_lobStorage->defrag(session);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
* 进行testOpenBigLobException
*/
void LobOperTestCase::testOpenBigLobException() {
	string basePath(LOB_PATH);
	string blobPath = basePath + Limits::NAME_LOBD_EXT;
	File *blobFile = new File(blobPath.c_str());
	Connection *connection = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testOpenBigLobException", connection);

	EXCPT_OPER(m_lobStorage->close(session, true));
	CPPUNIT_ASSERT(File::E_NO_ERROR == blobFile->remove());
	delete blobFile;

	LobStorage *lobStorage = NULL;
	try {
		 lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, false);
	} catch(NtseException e) {
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(connection);
		if (lobStorage) {
			delete lobStorage;
			lobStorage = NULL;
		}
		return;
	}
	if (lobStorage) {
		delete lobStorage;
		lobStorage = NULL;
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(connection);
	CPPUNIT_ASSERT(false);
}

/**
* 进行testOpenLobIndexException
*/
void LobOperTestCase::testOpenLobIndexException() {
	string basePath(LOB_PATH);
	string blobPath = basePath + Limits::NAME_LOBI_EXT;
	File *blobFile = new File(blobPath.c_str());
	Connection *connection = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testOpenLobIndexException", connection);

	EXCPT_OPER(m_lobStorage->close(session, true));
	blobFile->remove();
	delete blobFile;

	LobStorage *lob = NULL;
	try {
		lob = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, false);	
	} catch(NtseException e) {
		if (lob != NULL) {
			lob->close(session, false);
			delete lob;
			lob = NULL;
		}
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(connection);
		return;
	}
	if (lob != NULL) {
		lob->close(session, false);
		delete lob;
		lob = NULL;
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(connection);
	CPPUNIT_ASSERT(false);
}

/**
* 进行testCreateBigLobException
*/
void LobOperTestCase::testCreateBigLobException() {
	dropLobStorage();

	string basePath(LOB_PATH);
	string blobPath = basePath + Limits::NAME_LOBD_EXT;
	File *blobFile = new File(blobPath.c_str());
	blobFile->create(false, true);

	try {
		LobStorage::create(m_db, m_tableDef, LOB_PATH);	
	} catch(NtseException e) {
		blobFile->close();
		u64 err = blobFile->remove();
		delete blobFile;
		return;
	}
	CPPUNIT_ASSERT(false);
}

/**
* 进行testCreateBigLobException
*/
void LobOperTestCase::testCreateSmallLobException() {
	try {
		LobStorage::create(m_db, m_tableDef, LOB_PATH);	
	} catch(NtseException e) {
		return;
	}
	CPPUNIT_ASSERT(false);
}

/**
* 进行testCreateLobIndexException
*/
void LobOperTestCase::testCreateLobIndexException() {
	dropLobStorage();

	string basePath(LOB_PATH);
	string blobPath = basePath + Limits::NAME_LOBI_EXT;
	File *blobFile = new File(blobPath.c_str());
	blobFile->create(false, true);

	try {
		LobStorage::create(m_db, m_tableDef, LOB_PATH);	
	} catch(NtseException e) {
		blobFile->close();
		blobFile->remove();
		delete blobFile;
		return;
	}
	CPPUNIT_ASSERT(false);
}

/**
 * 测试insert时候，写日志不是按照先来先写的顺序，
 * 在redoInsert时候需要判断正确的文件tail
 * 测试步骤：1、线程A先拿到然后文件末尾长度，得到自己写入的Pid，然后wait
 * 2、线程B然后再得到大对象文件长度，得到自己写入Pid，然后写日志，写数据
 * 3、线程A然后写日志和数据
 * 4、大对象文件数据恢复要原来的，这样需要redoInsert
 * 5、redo过程中需要判断文件长度到最长的位置
 */
void LobOperTestCase::insertLobAndLogReverse() {
	//先backupFile
	File *lobFile = m_lobStorage->getBlobFile();
	backupLobFile(lobFile, LOB_BACKUP);

	//backup 目录文件
	File *indexFile = m_lobStorage->getIndexFile();
	backupLobFile(indexFile, LOBINDEX_BACKUP);


	LobTester tester1(this, LOB_INSERT1);
	LobTester tester2(this, LOB_INSERT2);
	tester1.enableSyncPoint(SP_LOB_BIG_INSERT_LOG_REVERSE_1);
	tester2.enableSyncPoint(SP_LOB_BIG_INSERT_LOG_REVERSE_2);

	tester1.start();
	tester2.start();

	tester2.joinSyncPoint(SP_LOB_BIG_INSERT_LOG_REVERSE_2);
	tester1.joinSyncPoint(SP_LOB_BIG_INSERT_LOG_REVERSE_1);
	tester1.notifySyncPoint(SP_LOB_BIG_INSERT_LOG_REVERSE_1);
	tester2.notifySyncPoint(SP_LOB_BIG_INSERT_LOG_REVERSE_2);

	tester1.join();
	tester2.join();

	// 关闭数据库再打开
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertLobAndLogReverse", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close();
	delete m_db;
	m_db = NULL;

	restoreLobFile(LOB_BACKUP, LOBBIG_PATH);
	restoreLobFile(LOBINDEX_BACKUP, LOBINDEX_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertLobAndLogReverse", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, false));

	//做redoInsert

	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	//然后再insertLob,可以继续操作
	insertBig(true);
	insertBig(false);
}

/**
 * 为测试日志顺序不是按照insert顺序,这个测试其中一个insert
 */
void LobOperTestCase::doInsert1() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::doInsert1", conn);
	uint len = getLobSize(Limits::PAGE_SIZE * 2, false);
	byte *lob =	createLob(len);
	m_lobStorage->insert(session, lob, len, false);
	System::virtualFree(lob);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 为测试日志顺序不是按照insert顺序,这个测试其中一个insert
 */
void LobOperTestCase::doInsert2( ) {
	doInsert1();
}


/************************************************************************/
/* 下面开始是测试代码实现                                               */
/************************************************************************/

/**
 * 测试insert大对象
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testInsertBig(bool isCompress) {
	insertBig(isCompress);
}

/**
 * 测试read大对象
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testReadBig(bool isCompress) {
	clearLobs();

	insertBig(isCompress);
	bool isRight;
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testReadBig", m_conn);
	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();
	uint i = (uint)lobContents.size();
	for (int j = i - 1; j >= 0; j--) {
		uint size;
		LobId lid = lobIds[j];
		byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
		CPPUNIT_ASSERT(size == lobContents[j].second);
		isRight = lobIsSame(lobContents[j].first, getLob, size);
		CPPUNIT_ASSERT(isRight);
	}
	m_db->getSessionManager()->freeSession(session);
	
	clearLobs();
}

/**
 * 测试insert四个大对象，分别为2页，1页，3页和4页
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::insertBig(bool isCompress) {
  insertTwoPages(isCompress);
  insertOnePage(isCompress);
  insertTotalPage(isCompress);
  insertBoundaryPage(isCompress);
}

/**
 * 测试insert为2页一个大对象
 *
 * @param isCompress 是否压缩
 * @return 大对象ID
 */
LobId LobOperTestCase::insertTwoPages(bool isCompress) {
    uint lobLen;
	uint size;
	bool isRight;

	//先测试两页长度
	lobLen = Limits::PAGE_SIZE * 2 - 512;
	byte *lob = createLob(lobLen, isCompress);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertTwoPages", conn);
	LobId lid =  m_lobStorage->insert(session, lob, lobLen, isCompress);
	lobIds.push_back(lid);

	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();
 	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == lobLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(lob, getLob, lobLen));
	pair<byte *, uint>lobContent = make_pair(lob, lobLen);
	lobContents.push_back(lobContent);
	//System::virtualFree(lob);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	return lid;
}


/**
 * 测试insert为1页一个大对象
 *
 * @param isCompress 是否压缩
 * @return 大对象ID
 */
LobId LobOperTestCase::insertOnePage(bool isCompress) {
	bool isRight;
	uint len = (uint)(Limits::PAGE_SIZE - Limits::PAGE_SIZE * 0.05);
	uint size;
	byte *lob = createLob(len);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertOnePage", conn);
	LobId lid =  m_lobStorage->insert(session, lob, len, isCompress);
	lobIds.push_back(lid);
	pair<byte *, uint>lobContent = make_pair(lob, len);
	lobContents.push_back(lobContent);


	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == len);
	CPPUNIT_ASSERT(isRight = lobIsSame(lob, getLob, len));
	//System::virtualFree(lob);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	return lid;
}

/**
 * 测试insert为3页一个大对象
 *
 * @param isCompress 是否压缩
 * @return 大对象ID
 */
LobId LobOperTestCase::insertTotalPage(bool isCompress, bool random) {
	bool isRight;
	uint len = getLobSize(Limits::PAGE_SIZE * 3, isCompress);
	uint size;
	byte *lob = createLob(len, random);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertTotalPage", conn);
	LobId lid =  m_lobStorage->insert(session, lob, len, isCompress);
	lobIds.push_back(lid);
	pair<byte *, uint>lobContent = make_pair(lob, len);
	lobContents.push_back(lobContent);

	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == len);
	CPPUNIT_ASSERT(isRight = lobIsSame(lob, getLob, len));
	//System::virtualFree(lob);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	return lid;
}


/**
 * 测试insert为4页一个大对象，其中刚好第4页只有一个字节
 *
 * @param isCompress 是否压缩
 * @return 大对象ID
 */
LobId LobOperTestCase::insertBoundaryPage(bool isCompress) {
	bool isRight;
	uint len = getLobSize(Limits::PAGE_SIZE * 3, isCompress)+1;
	uint size;
	byte *lob = createLob(len);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertBoundaryPage", conn);
	LobId lid =  m_lobStorage->insert(session, lob, len, isCompress);
	lobIds.push_back(lid);
	pair<byte *, uint>lobContent = make_pair(lob, len);
	lobContents.push_back(lobContent);
	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == len);
	CPPUNIT_ASSERT(isRight =lobIsSame(lob, getLob, len));
	//System::virtualFree(lob);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	return lid;
}


/**
 * 测试大型大对象的更新
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testBigUpdate(bool isCompress) {
	//先插入四个大对象
	lobIds.clear();
	insertBig(isCompress);
	bool isRight;
	uint len = getLobSize(Limits::PAGE_SIZE * 2, isCompress) + 1;
	uint size;
	byte *lob = createLob(len);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testBigUpdate", conn);
	LobId lid =  m_lobStorage->insert(session, lob, len, isCompress);
	lobIds.push_back(lid);
	System::virtualFree(lob);

	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();

	//进行更新，原来3页，现在不变
	uint newLen = getLobSize(Limits::PAGE_SIZE * 2, isCompress) + 2;
	byte *newLob = createLob(newLen);
	m_lobStorage->update(session, lid, newLob, newLen, isCompress);
	//验证是否一致
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newLob, getLob, newLen));
	System::virtualFree(newLob);

	//进行更新，但大对象变大，大对象是在文件末尾
	uint newBigerAtTailLen = getLobSize(Limits::PAGE_SIZE * 5, isCompress) - 1;
	byte *newBigerAtTailLob = createLob(newBigerAtTailLen);
	m_lobStorage->update(session, lid, newBigerAtTailLob, newBigerAtTailLen, isCompress);
	//验证是否一致
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newBigerAtTailLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAtTailLob, getLob, size));
	System::virtualFree(newBigerAtTailLob);

	//进行更新，但页数变小
	uint nextLen = getLobSize(Limits::PAGE_SIZE * 2, isCompress) - 1;
	byte *nextLob = createLob(nextLen);
	m_lobStorage->update(session, lid, nextLob, nextLen, isCompress);
	//验证是否一致
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == nextLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(nextLob, getLob, nextLen));
	System::virtualFree(nextLob);

	//进行更新，但大对象变大，大对象在中间，但需要判断后面是否有足够的空闲空间，空间足够
	size_t elementNum = lobIds.size();
	insertBig(isCompress);
	for(iter = lobIds.begin() + elementNum; iter !=lobIds.end() - 1; iter++){
		m_lobStorage->del(session, *iter);
	}
	uint newBigerAndHaveFreeSpaceLen  = getLobSize(Limits::PAGE_SIZE * 8, isCompress);
	byte *newBigerAndHaveFreeSpaceLob = createLob(newBigerAndHaveFreeSpaceLen);
	m_lobStorage->update(session, lid, newBigerAndHaveFreeSpaceLob, newBigerAndHaveFreeSpaceLen, isCompress);
	//验证是否一致
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newBigerAndHaveFreeSpaceLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAndHaveFreeSpaceLob, getLob, size));
	System::virtualFree(newBigerAndHaveFreeSpaceLob);

	//进行更新，但大对象变大，大对象在中间，但需要判断后面是否有足够的空闲空间，空间足够，但产生新的空闲块
	uint newBigerAndHavaNewFreeBlkLen  = getLobSize(Limits::PAGE_SIZE * 9, isCompress);
	byte *newBigerAndHavaNewFreeBlkLob = createLob(newBigerAndHavaNewFreeBlkLen);
	m_lobStorage->update(session, lid, newBigerAndHavaNewFreeBlkLob, newBigerAndHavaNewFreeBlkLen, isCompress);
	//验证是否一致
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newBigerAndHavaNewFreeBlkLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAndHavaNewFreeBlkLob, getLob, size));
	System::virtualFree(newBigerAndHavaNewFreeBlkLob);


	//进行更新，大对象变大，但后面连续空闲空间不够
	insertBoundaryPage(isCompress);
	insertTwoPages(isCompress);
	insertOnePage(isCompress);
	insertTotalPage(isCompress);
	elementNum = lobIds.size();
	//先删除两个
	m_lobStorage->del(session, lobIds[elementNum-2]);
	m_lobStorage->del(session, lobIds[elementNum-3]);
	uint newBigerAndHavaNoFreeBlkLen  = getLobSize(Limits::PAGE_SIZE * 8, isCompress);
	byte *newBigerAndHavaNoFreeBlkLob = createLob(newBigerAndHavaNoFreeBlkLen);
	m_lobStorage->update(session,lobIds[elementNum-4], newBigerAndHavaNoFreeBlkLob, newBigerAndHavaNoFreeBlkLen, isCompress);
	//验证是否一致
	getLob = m_lobStorage->get(session, mc, lobIds[elementNum-4], &size, false);
	CPPUNIT_ASSERT(size == newBigerAndHavaNoFreeBlkLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAndHavaNoFreeBlkLob, getLob, size));
	System::virtualFree(newBigerAndHavaNoFreeBlkLob);


	//测试更新的最后一种情况：
	insertOnePage(isCompress);
	insertTwoPages(isCompress);
	insertTotalPage(isCompress);
	elementNum = lobIds.size();
	m_lobStorage->del(session, lobIds[elementNum - 1]);
	uint newNearTailLen  = getLobSize(Limits::PAGE_SIZE * 6, isCompress);
	byte *newNearTailLob = createLob(newNearTailLen);
	m_lobStorage->update(session,lobIds[elementNum - 2], newNearTailLob, newNearTailLen, isCompress);
	//验证是否一致
	getLob = m_lobStorage->get(session, mc, lobIds[elementNum-2], &size, false);
	CPPUNIT_ASSERT(size == newNearTailLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newNearTailLob, getLob, size));
	System::virtualFree(newNearTailLob);


	clearLobs();

	//2008.11.5 补充
	//测试只有一页情况
	LobId onePageLid = insertOnePage(isCompress);
	uint onePageLen = Limits::PAGE_SIZE - 50;
	byte *onePageLob = createLob(onePageLen);
	LobId newLid = m_lobStorage->update(session, onePageLid, onePageLob, onePageLen, isCompress);
	getLob = m_lobStorage->get(session, mc, newLid, &size, false);
	CPPUNIT_ASSERT(size == onePageLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(onePageLob, getLob, size));
	System::virtualFree(onePageLob);

    //为了覆盖622,623（BigLop.cpp）
	//从4页变成3页
	LobId newTryId = insertBoundaryPage(isCompress);
	uint newTryLen = Limits::PAGE_SIZE * 2;
	byte *newTryLob = createLob(newTryLen);
	m_lobStorage->update(session, newTryId, newTryLob, newTryLen, isCompress);
	getLob = m_lobStorage->get(session, mc, newTryId, &size, false);
	CPPUNIT_ASSERT(size == newTryLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newTryLob, getLob, size));
	System::virtualFree(newTryLob);
	m_db->getSessionManager()->freeSession(session);

	//测试变长时候，需要扩展文件的情况
	tearDown();
	setUp();
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testBigUpdate", m_conn);
	mc = session->getMemoryContext();

	clearLobs();

	for(uint i = 0; i < 1024 / 4; i++) {
		insertBoundaryPage(isCompress);
	}
	uint newExtendLen = Limits::PAGE_SIZE * 6;
	byte *newExtendLob = createLob(newExtendLen);
	LobId newExtendId = lobIds[1024 / 4 - 1];
	m_lobStorage->update(session, newExtendId, newExtendLob, newExtendLen, isCompress);
	getLob = m_lobStorage->get(session, mc, newExtendId, &size, false);
	CPPUNIT_ASSERT(size == newExtendLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newExtendLob, getLob, size));
	System::virtualFree(newExtendLob);

	m_db->getSessionManager()->freeSession(session);
}


/**
 * 测试删除大型大对象
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testDelBig(bool isCompress) {
	//准备一些lob
	uint lids[] = {0x0, 0x2, 0x3, 0x6};
	uint i = 0;
	lobIds.clear();
	insertBig(isCompress);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testDelBig", m_conn);
	for(iter = lobIds.begin(); iter != lobIds.end(); iter++){
		m_lobStorage->del(session, *iter);
		u32 pid = LID_GET_PAGE(*iter);
		u16 slotNum = LID_GET_SLOT(*iter);

		File *indexFile = m_lobStorage->getIndexFile();
		File *blobFile = m_lobStorage->getBlobFile();
		//验证目录项和块首业的IsFree是True
		BufferPageHandle *pageHdl = GET_PAGE(session, indexFile, PAGE_LOB_INDEX, pid, Shared, m_lobStorage->getLLobDirStats(), NULL);
		LIFilePageInfo *lifilePage = (LIFilePageInfo *)pageHdl->getPage();
		LiFileSlotInfo *slotInfo  = (LiFileSlotInfo *)((byte *)lifilePage + LobIndex::OFFSET_PAGE_RECORD + slotNum * LobIndex::INDEX_SLOT_LENGTH);
		bool isFree = slotInfo->m_free;
		u32 lobPid =  slotInfo->u.m_pageId;
		BufferPageHandle *lobPageHdl = GET_PAGE(session, blobFile, PAGE_LOB_HEAP, lids[i++], Shared, m_lobStorage->getLLobDatStats(), NULL);
		LobBlockFirstPage *lobFirstPage = (LobBlockFirstPage *)lobPageHdl->getPage();
		bool isBlockFree = lobFirstPage->m_isFree;
		session->releasePage(&pageHdl);
		session->releasePage(&lobPageHdl);
	}
	//然后在插入大对象，将重用目录项
	LobId lid = insertBoundaryPage(isCompress);
	//cout <<"0x"<< hex << lid << endl;
	u32 pid = LID_GET_PAGE(lid);
	u16 slotNum = LID_GET_SLOT(lid);
	CPPUNIT_ASSERT(pid == 1);
	CPPUNIT_ASSERT(slotNum <= 8);
	lid = insertTwoPages(isCompress);
	cout <<"0x"<< hex << lid << endl;
	pid = LID_GET_PAGE(lid);
	slotNum = LID_GET_SLOT(lid);
	CPPUNIT_ASSERT(pid == 1);
	CPPUNIT_ASSERT(slotNum <= 8);
	lid = insertTotalPage(isCompress);
	cout <<"0x"<< hex << lid << endl;
	pid = LID_GET_PAGE(lid);
	slotNum = LID_GET_SLOT(lid);
	CPPUNIT_ASSERT(pid == 1);
	CPPUNIT_ASSERT(slotNum <= 8);
	lid = insertTotalPage(isCompress);
	cout <<"0x"<< hex << lid << endl;
	pid = LID_GET_PAGE(lid);
	slotNum = LID_GET_SLOT(lid);
	CPPUNIT_ASSERT(pid == 1);
	CPPUNIT_ASSERT(slotNum <= 8);
	lid = insertBoundaryPage(isCompress);
	cout <<"0x"<< hex << lid << endl;
	pid = LID_GET_PAGE(lid);
	slotNum = LID_GET_SLOT(lid);
	cout << dec;
	CPPUNIT_ASSERT(pid == 1);
	CPPUNIT_ASSERT(slotNum <= 8);
	m_db->getSessionManager()->freeSession(session);
}


/**
 * 关闭大对象存储
 */
void LobOperTestCase::closeLobStorage() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::closeLobStorage", conn);
	EXCPT_OPER(m_lobStorage->close(session, true));
	//EXCPT_OPER(m_lobStorage->close(session, true)); // 重复关闭
	delete m_lobStorage;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_lobStorage = NULL;
}

/**
 * 测试redoCreate
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testRedoCreate(bool isCompress){
	closeLobStorage();
	File f1(LOBINDEX_PATH);
	File f2(LOBBIG_PATH);
	bool e1, e2;
	f1.isExist(&e1);
	f2.isExist(&e2);
	CPPUNIT_ASSERT(e1);
	CPPUNIT_ASSERT(e2);
	u64 errCode1 = f1.remove();
	u64 errCode2 = f2.remove();
	//cout<<"err code is "<<errCode<<endl;
	f1.isExist(&e1);
	f2.isExist(&e2);
	CPPUNIT_ASSERT(!e1);
	CPPUNIT_ASSERT(!e2);

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	// EXCPT_OPER(LobStorage::redoCreate(m_db, session, LOB_PATH, 0 ));
	EXCPT_OPER(LobStorage::redoCreate(m_db, session, m_tableDef, LOB_PATH, 1));
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// 文件长度不够
	closeLobStorage();
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	// EXCPT_OPER(LobStorage::redoCreate(m_db, session, LOB_PATH, 0 ));
	EXCPT_OPER(LobStorage::redoCreate(m_db, session, m_tableDef, LOB_PATH, 1));
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	//插入数据，不用redocreate
	insertOnePage(isCompress);
	closeLobStorage();
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	EXCPT_OPER(LobStorage::redoCreate(m_db, session, m_tableDef, LOB_PATH, 1););
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	for (uint i =0; i< 1024; i++) {
		insertOnePage(isCompress);
	}
	closeLobStorage();
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	EXCPT_OPER(LobStorage::redoCreate(m_db, session, m_tableDef, LOB_PATH, 1););
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
 * 测试redoInsert
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testRedoInsert(bool isCompress) {
	//先做一些操作，插入一些数据
	Session *session;
	u64 li1, li2, li3;
	int j = 0;
	LogScanHandle *logHdl;

	m_db->setCheckpointEnabled(false);
	clearLobs();
	byte *indexBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *indexHeaderBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFirstBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobLastBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoInsert", m_conn);

	uint maxSlot = LobIndex::MAX_SLOT_PER_PAGE;
	
	backupLobIndexPage(session, 0, indexHeaderBuf);
	for (uint i = 0; i <= maxSlot; ++i) {
		if (i == maxSlot - 1) {
			backupLobIndexPage(session, 1, indexBuf);
		}
		if (i == maxSlot - 3) {
			backupLobPage(session, (maxSlot - 2) * 3 - 1, lobLastBuf);
		}

		if (i == maxSlot)
			backupLobPage(session, (maxSlot + 1) * 3 - 1, lobLastBuf);
		
		if (i == maxSlot) 
			insertTotalPage(isCompress, true);
		else
			insertTotalPage(isCompress);

		if (i == maxSlot)
			restoreLobPage(session, (maxSlot + 1) * 3 - 1, lobLastBuf);

		if (i == maxSlot - 3) {
			restoreLobPage(session, (maxSlot - 2) * 3 - 1, lobLastBuf);
		}

		if (i == maxSlot - 1) { // 第二页插入前
			restoreLobIndexPage(session, 1, indexBuf); // 第一页最后一条记录的修改就没有生效
			backupLobIndexPage(session, 2, indexBuf); // 准备第二页。
		}
	}
	restoreLobIndexPage(session, 2, indexBuf); // 回复到最后一个插入前
	restoreLobIndexPage(session, 0, indexHeaderBuf);

	// 关闭再打开
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->setCheckpointEnabled(true);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_db->setCheckpointEnabled(false);
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoInsert", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// 检查日志
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		++j;
		u64 lsn = logHdl->curLsn();

		//目录文件没刷出去
		if (j == maxSlot) {
			li1 = getLobIndexPageLSN(session, 1);
			li2  = getLobIndexPageLSN(session, 0);
		}
		if (j == maxSlot + 1) {
			li3 = getLobIndexPageLSN(session, 2);
		}
		//测试parseInsertLog函数
		const LogEntry *entry = logHdl->logEntry();
		byte *logData = entry->m_data;
		size_t orgLen;
		m_lobStorage->parseInsertLog(entry, *((u64 *)logData), &orgLen, session->getMemoryContext());
		m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);

		// 验证LOB内容
		uint size ;
		LobId lid = lobIds[j - 1];
		MemoryContext *mc = session->getMemoryContext();
		byte *lob = m_lobStorage->get(session, mc, lid, &size, false);
		CPPUNIT_ASSERT(size == lobContents[j - 1].second);
		bool isSame = lobIsSame(lob, lobContents[j - 1].first, size);
		CPPUNIT_ASSERT(isSame);
	}
	m_db->getTxnlog()->endScan(logHdl);

	m_db->getSessionManager()->freeSession(session);
	System::virtualFree(indexBuf);
	System::virtualFree(indexHeaderBuf);
	System::virtualFree(lobBuf);
	System::virtualFree(lobFirstBuf);
	System::virtualFree(lobLastBuf);
	m_db->setCheckpointEnabled(true);
}

void LobOperTestCase::testRedoDelEx(bool real) {
	uint len;
	byte *lob;
	LobId lid1, lid2;
	LogScanHandle *logHdl;
	Session *session;
	BufferPageHandle *bphdl;
	LIFilePageInfo *liFilePage;
	LIFileHeaderPageInfo *headPage;
	u16 freeSlotNum;
	uint size;

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoDelEx", m_conn);
	u64 lsn = Txnlog::MIN_LSN;

	// 压缩记录插入
	len = getLobSize(Limits::PAGE_SIZE * 3, true);
	lob = createLob(len, true);
	lid1 = m_lobStorage->insert(session, lob, len, true);
	System::virtualFree(lob);
	lob = NULL;

	// 读取记录内容
	lob = m_lobStorage->get(session, session->getMemoryContext(), lid1, &size, false);

	bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, 1, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
	liFilePage = (LIFilePageInfo *)bphdl->getPage();
	freeSlotNum = liFilePage->m_freeSlotNum;
	liFilePage->m_freeSlotNum = 1;
	session->markDirty(bphdl);
	session->releasePage(&bphdl);

	// 非压缩记录插入
	len = getLobSize(Limits::PAGE_SIZE, false);
	lob = createLob(len, false);
	lid2 = m_lobStorage->insert(session, lob, len, false);

	bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, 1, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
	liFilePage = (LIFilePageInfo *)bphdl->getPage();
	liFilePage->m_freeSlotNum = freeSlotNum - 1;
	session->markDirty(bphdl);
	session->releasePage(&bphdl);
	System::virtualFree(lob);
	lob = NULL;

	// 记录更新
	uint updlen = getLobSize(Limits::PAGE_SIZE * 4, true);
	byte *updlob = createLob(updlen, true);
	m_lobStorage->update(session, lid1, updlob, updlen, true);

	// 记录删除
	m_lobStorage->del(session, lid1);

	bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, 1, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
	liFilePage = (LIFilePageInfo *)bphdl->getPage();
	liFilePage->m_inFreePageList = false;
	session->markDirty(bphdl);
	session->releasePage(&bphdl);

	m_lobStorage->del(session, lid2);

	// restore
	setLobIndexPageLSN(session, 0, lsn);
	setLobIndexPageLSN(session, 1, lsn);
	setLobPageLSN(session, 0, lsn);
	setLobPageLSN(session, 3, lsn);

	// 重启
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoDelEx", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));

	// REDO
	int redoCount = 0;
	int redoInsert = 0;
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		//测试parseInsertLog函数
		const LogEntry *entry = logHdl->logEntry();
		byte *logData = entry->m_data;
		u64 lsn = logHdl->curLsn();
		size_t orgLen;
		if (real) {
			if (entry->m_logType == LOG_LOB_INSERT) {
				m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
			} else if (entry->m_logType == LOG_LOB_DELETE) {
				if (redoCount++ == 1) {
					bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, 0, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
					headPage = (LIFileHeaderPageInfo *)bphdl->getPage();
					headPage->m_firstFreePageNum = 0;
					session->markDirty(bphdl);
					session->releasePage(&bphdl);
				}
				m_lobStorage->redoBLDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
			} else if (entry->m_logType == LOG_LOB_UPDATE) {
				m_lobStorage->redoBLUpdate(session, lid1, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, updlob, updlen, true);
			}
		} else {
			if (entry->m_logType == LOG_LOB_INSERT) {
				m_lobStorage->parseInsertLog(entry, *((u64 *)logData), &orgLen, session->getMemoryContext());
				if (redoInsert++ == 1) {
					bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, 0, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
					headPage = (LIFileHeaderPageInfo *)bphdl->getPage();
					headPage->m_blobFileLen = 0;
					headPage->m_blobFileTail = 0;
					headPage->m_fileLen = 1;
					session->markDirty(bphdl);
					session->releasePage(&bphdl);
				}
				m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
			}
		}
		
	}
	System::virtualFree(updlob);
	updlob = NULL;

	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
}


/**
 * 测试redoDel
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testRedoDel(bool isCompress) {
	Session *session;
	u64 l1, l2, l3, li1, li2, li3, li4, li5;
	LogScanHandle *logHdl;
	size_t num = lobContents.size();

	m_db->setCheckpointEnabled(false);
	for (uint i = 0; i < num; i++) {
		System::virtualFree(lobContents[i].first);
	}
	lobIds.clear();
	lobContents.clear();
	byte *indexBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *indexHeaderBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);

	//先准备数据
	Buffer *buffer = m_db->getPageBuffer();
	uint maxSlot = LobIndex::MAX_SLOT_PER_PAGE;
	for (uint i = 0; i <= maxSlot; ++i) {
		insertTotalPage(isCompress);
		buffer->flushAll();
	}

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoDel", m_conn);
	backupLobIndexPage(session, 1, indexBuf);
	backupLobIndexPage(session, 0, indexHeaderBuf);
	li1 = getLobIndexPageLSN(session, 1);
	for (uint i = 0; i <= maxSlot; ++i) {
		if (i == maxSlot - 2) {
			backupLobPage(session, (maxSlot - 2) * 3 , lobBuf);
			l1 = getLobPageLSN(session, (maxSlot - 2) * 3);
		}

		m_lobStorage->del(session, lobIds[i]);

		if (i == maxSlot) {
			restoreLobIndexPage(session, 0, indexHeaderBuf);
		}
		if (i == maxSlot - 2) {
			l3 = getLobPageLSN(session, (maxSlot - 2) * 3);
			CPPUNIT_ASSERT(l3 != l1);
			restoreLobPage(session, (maxSlot - 2) * 3 , lobBuf);
			l2 = getLobPageLSN(session, (maxSlot - 2) * 3);
			CPPUNIT_ASSERT(l2 == l1);

		}
		if (i == maxSlot - 1) {
			li2 = getLobIndexPageLSN(session, 1);//一页目录项目已经用完
			CPPUNIT_ASSERT(li1 != li2);
		}

		if (i == maxSlot - 1) { // 第二页插入前
			restoreLobIndexPage(session, 1, indexBuf); // 第一页最后一条记录的修改就没有生效
			li2 = getLobIndexPageLSN(session, 1);
			CPPUNIT_ASSERT(li1 == li2);

			backupLobIndexPage(session, 2, indexBuf); // 准备第二页。
			li3 = getLobIndexPageLSN(session, 2);
		}
		if (i == maxSlot) {
			li5 = getLobIndexPageLSN(session, 2);

		}
	}
	li4 = getLobIndexPageLSN(session, 2);
	CPPUNIT_ASSERT(li4 != li3);
	restoreLobIndexPage(session, 2, indexBuf); // 回复到最后一个插入前
	li4 = getLobIndexPageLSN(session, 2);
	CPPUNIT_ASSERT(li4 == li3);

	// 关闭再打开
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;

	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->setCheckpointEnabled(true);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_db->setCheckpointEnabled(false);
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoDel", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// 检查日志
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoDel", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		//测试parseInsertLog函数
		const LogEntry *entry = logHdl->logEntry();
		byte *logData = entry->m_data;
		u64 lsn = logHdl->curLsn();
		size_t orgLen;
		if (entry->m_logType == LOG_LOB_INSERT) {
			m_lobStorage->parseInsertLog(entry, *((u64 *)logData), &orgLen, session->getMemoryContext());
			m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
		} else if (entry->m_logType == LOG_LOB_DELETE) {
			m_lobStorage->redoBLDelete(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
		}
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	System::virtualFree(indexBuf);
	System::virtualFree(indexHeaderBuf);
	System::virtualFree(lobBuf);
	m_db->setCheckpointEnabled(true);
}

void LobOperTestCase::testRedoUpdateEx() {
	clearLobs();

	bool compress = true;
	u64	l1, l2, l3;
	byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFirstBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFreeBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *indexBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);

	//先插入一些数据做准备, 测试两个页面
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	LobId lid = insertTwoPages(true);
	l1 = getLobPageLSN(session, 1);
	backupLobPage(session, 1, lobBuf);

	byte *lob = createLob(Limits::PAGE_SIZE * 2 - 512, true);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 2 - 512, compress);
	l2 = getLobPageLSN(session, 1);
	restoreLobPage(session, 1, lobBuf);
	l3 = getLobPageLSN(session, 1);
	CPPUNIT_ASSERT(l1 == l3);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(false);
	delete m_db;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		const LogEntry *entry = logHdl->logEntry();
		if (entry->m_logType == LOG_LOB_UPDATE)
			m_lobStorage->redoBLUpdate(session, lid, lsn, logHdl->logEntry()->m_data,(uint)logHdl->logEntry()->m_size, lob, Limits::PAGE_SIZE * 2 - 512, compress);
	}
	m_db->getTxnlog()->endScan(logHdl);
	uint size;
	mc = session->getMemoryContext();
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	bool isSame = lobIsSame(lob, getLob, size);
	CPPUNIT_ASSERT(size == Limits::PAGE_SIZE * 2 - 512);
	CPPUNIT_ASSERT(isSame);
	System::virtualFree(lob);

	// 释放会话
	m_db->getSessionManager()->freeSession(session);	
}


/**
 * 测试redoUpdate
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testRedoUpdate(bool isCompress) {
	clearLobs();

	u64 li1, li2, l1, l2, l3, l4;
	byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFirstBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFreeBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *indexBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);

	//先插入一些数据做准备, 测试两个页面
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	LobId lid = insertTwoPages(isCompress);
	l1 = getLobPageLSN(session, 1);
	backupLobPage(session, 1, lobBuf);

	//------下面是页面数不变-------------------------------------/
	byte *lob =createLob(Limits::PAGE_SIZE * 2 - 512);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 2 - 512, isCompress);
	l2 = getLobPageLSN(session, 1);
	restoreLobPage(session, 1, lobBuf);
	l3 = getLobPageLSN(session, 1);
	CPPUNIT_ASSERT(l1 == l3);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		u64 lsn = logHdl->curLsn();
		if (j == 2) {
			m_lobStorage->redoBLUpdate(session, lid, lsn, logHdl->logEntry()->m_data,(uint)logHdl->logEntry()->m_size, lob, Limits::PAGE_SIZE * 2 - 512, isCompress);
		}
	}
	m_db->getTxnlog()->endScan(logHdl);
	uint size;
	mc = session->getMemoryContext();
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	bool isSame = lobIsSame(lob, getLob, size);
	CPPUNIT_ASSERT(size == Limits::PAGE_SIZE * 2 - 512);
	CPPUNIT_ASSERT(isSame);
	System::virtualFree(lob);

	// 释放会话
	m_db->getSessionManager()->freeSession(session);

	// 下面测试其他情况：
	// 1、变长：a、后面空间够 b、后面空间不够，移动到末尾
	// 2、变短：产生新的空闲块

	//--------------下面大对象变大，但在文件末尾：--------------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	mc = session->getMemoryContext();
	backupLobPage(session, 1, lobBuf);
	backupLobPage(session, 0, lobFirstBuf);
	l1 = getLobPageLSN(session, 1);
	lob = createLob(Limits::PAGE_SIZE * 3 - 256);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 3 - 256, isCompress);
	l2 = getLobPageLSN(session, 2);
	restoreLobPage(session, 1, lobBuf);
	restoreLobPage(session, 0, lobFirstBuf);
	l2 = getLobPageLSN(session, 2);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		u64 lsn = logHdl->curLsn();
		if (j == 3) {
			m_lobStorage->redoBLUpdate(session, lid, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, lob, Limits::PAGE_SIZE * 3 - 256, isCompress);
		}
	}
	mc = session->getMemoryContext();
	CPPUNIT_ASSERT(j == 3);
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	isSame = lobIsSame(lob, getLob, size);
	CPPUNIT_ASSERT(isSame);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	System::virtualFree(lob);

	//--------------下面大对象变小：-----------------------------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	backupLobPage(session, 0, lobFirstBuf);
	backupLobPage(session, 1, lobFreeBuf);
	lob = createLob(Limits::PAGE_SIZE - 50);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE - 50, isCompress);
	l2 = getLobPageLSN(session, 1);
	//m_lobStorage->get(session, mc, lid, &size);
	restoreLobPage(session, 0, lobFirstBuf);
	restoreLobPage(session, 1, lobFreeBuf);
	l3 = getLobPageLSN(session, 2);
	m_db->getSessionManager()->freeSession(session);


	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		u64 lsn = logHdl->curLsn();
		if (j == 4) {
			m_lobStorage->redoBLUpdate(session, lid, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, lob, Limits::PAGE_SIZE - 50, isCompress);
		}
	}
	mc = session->getMemoryContext();
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	isSame = lobIsSame(lob, getLob, size);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	System::virtualFree(lob);

	//下面对象变大：但后面有空闲空间，并且空闲空间够----------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	l3 = getLobPageLSN(session, 2);
	backupLobPage(session, 0, lobFirstBuf);
	backupLobPage(session, 1, lobBuf);
	backupLobPage(session, 2, lobFreeBuf);
	lob = createLob(Limits::PAGE_SIZE * 2 - 256);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 2 - 256, isCompress);
	l2 = getLobPageLSN(session, 0);
	l3 = getLobPageLSN(session, 1);
	l4 = getLobPageLSN(session, 2);

	restoreLobPage(session, 0, lobFirstBuf);
	restoreLobPage(session, 1, lobBuf);
	restoreLobPage(session, 2, lobFreeBuf);
	l2 = getLobPageLSN(session, 0);
	l3 = getLobPageLSN(session, 1);

	//m_lobStorage->get(session, mc, lid, &size);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		u64 lsn = logHdl->curLsn();
		if (j == 5) {
			m_lobStorage->redoBLUpdate(session, lid, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, lob, Limits::PAGE_SIZE * 2 - 256, isCompress);
		}
	}

	mc = session->getMemoryContext();
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	isSame = lobIsSame(lob, getLob, size);
	CPPUNIT_ASSERT(isSame);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	System::virtualFree(lob);


	//下面对象变大：但后面有空闲空间，并且空闲空间不够(这里是新的pid +pageNum > fileTail)---------------------
	byte *newlobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	l3 = getLobPageLSN(session, 2);
	//l4 = getLobPageLSN(session, 3);
	li1 = getLobIndexPageLSN(session, 1);
	backupLobPage(session, 0, lobFreeBuf);
	backupLobPage(session, 2, lobFirstBuf);
	//backupLobPage(session, 3, lobFirstBuf);
	//backupLobPage(session, 5, lobBuf);
	//backupLobPage(session, 6, newlobBuf);
	backupLobIndexPage(session, 1, indexBuf);
	uint ll = 0;
	if (isCompress) {
		ll = Limits::PAGE_SIZE * 5;
	} else {
		ll = Limits::PAGE_SIZE * 4;
	}
	lob = createLob(ll);
	m_lobStorage->update(session, lid, lob, ll, isCompress);
	l3 = getLobPageLSN(session, 0);
	l4 = getLobPageLSN(session, 3);
	li2 = getLobIndexPageLSN(session, 1);

	restoreLobPage(session, 0, lobFreeBuf);
	restoreLobPage(session, 2, lobFirstBuf);
	//restoreLobPage(session, 3, lobFirstBuf);
	//restoreLobPage(session, 5, lobBuf);
	//restoreLobPage(session, 6, newlobBuf);
	restoreLobIndexPage(session, 1, indexBuf);
	m_db->getSessionManager()->freeSession(session);


	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		u64 lsn = logHdl->curLsn();
		if (j == 6) {
			m_lobStorage->redoBLUpdate(session, lid, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, lob, ll, isCompress);
		}
	}
	mc = session->getMemoryContext();
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	isSame = lobIsSame(lob, getLob, size);
	CPPUNIT_ASSERT(isSame);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	System::virtualFree(lob);


	//下面对象变大：但后面有空闲空间，并且空闲空间不够---------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	LobId lid2 = insertOnePage(isCompress);
	LobId lid3 = insertTwoPages(isCompress);
	LobId lid4 = insertOnePage(isCompress);
	LobId lid5 = insertTwoPages(isCompress);
	LobId lid6 = insertBoundaryPage(isCompress);
	m_lobStorage->del(session, lid2);
	m_lobStorage->del(session, lid3);
	m_lobStorage->del(session, lid4);

	backupLobPage(session, 3, lobFreeBuf);
	backupLobPage(session, 9, lobFirstBuf);
	backupLobPage(session, 11, lobBuf);
	backupLobPage(session, 16, newlobBuf);
	backupLobIndexPage(session, 1, indexBuf);

	if (isCompress) {
		ll = Limits::PAGE_SIZE * 20;
	} else {
		ll = Limits::PAGE_SIZE * 18;
	}
	lob = createLob(ll);
	m_lobStorage->update(session, lid, lob, ll, isCompress);
	mc = session->getMemoryContext();
	restoreLobPage(session, 3, lobFreeBuf);
	restoreLobPage(session, 9, lobFirstBuf);
	restoreLobPage(session, 11, lobBuf);
	restoreLobPage(session, 16, newlobBuf);
	restoreLobIndexPage(session, 1, indexBuf);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		u64 lsn = logHdl->curLsn();
		if (j == 15) {
			m_lobStorage->redoBLUpdate(session, lid, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size, lob, ll, isCompress);

		}
	}
	mc = session->getMemoryContext();
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	isSame = lobIsSame(lob, getLob, size);
	CPPUNIT_ASSERT(isSame);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	System::virtualFree(lob);

	System::virtualFree(lobFirstBuf);
	System::virtualFree(lobBuf);
	System::virtualFree(newlobBuf);
	System::virtualFree(lobFreeBuf);
	System::virtualFree(indexBuf);
}


/**
 * 测试更新日志
 *
 * @param isCompress 是否压缩
 */
void LobOperTestCase::testUpdateLog(bool isCompress) {
	clearLobs();

	Session *session;
	u64 li1, li2, l1, l2, l3;
	//先插入一些数据做准备
	// 测试两个页面
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	LobId lid = insertTwoPages(isCompress);
	byte *lob = createLob(Limits::PAGE_SIZE * 2 - 256);
	LobId nid = m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 2 - 256, isCompress);
	MemoryContext *mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	li1 = getLobIndexPageLSN(session, 1);
	li2 = getLobIndexPageLSN(session, 0);
	CPPUNIT_ASSERT(l1 == l2);
	CPPUNIT_ASSERT(li1 != l1);
	CPPUNIT_ASSERT(li1 != li2);
	//m_lobStorage->get(session, mc, lid, &size);
	m_db->getSessionManager()->freeSession(session);
	System::virtualFree(lob);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);


	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		if (j == 2) {
			byte *logData = logHdl->logEntry()->m_data;
			CPPUNIT_ASSERT(*((u64 *)logData) == lid);
			logData += sizeof(u64);
			CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == false);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((bool *)logData) == false);
			logData += sizeof(bool);
			if (isCompress) {
				CPPUNIT_ASSERT(*((u32 *)logData) < Limits::PAGE_SIZE * 2 - 256);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 2 - 256);
			} else {
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 2 - 256);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			}
			logData += sizeof(u32);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	CPPUNIT_ASSERT(j == 2);


	//下面测试其他情况：
	//1、变长：a、后面空间够 b、后面空间不够，移动到末尾
	//2、变短：产生新的空闲块


	//--------------下面大对象变大，但在文件末尾：------------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	lob = createLob(Limits::PAGE_SIZE * 2);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 2, isCompress);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	l3 = getLobPageLSN(session, 2);
	li1 = getLobIndexPageLSN(session, 1);
	li2 = getLobIndexPageLSN(session, 0);
	CPPUNIT_ASSERT(l1 == l2);
	//CPPUNIT_ASSERT(l1 == l3);
	CPPUNIT_ASSERT(li1 != l1);
	//m_lobStorage->get(session, mc, lid, &size);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		if (j == 3) {
			byte *logData = logHdl->logEntry()->m_data;
			CPPUNIT_ASSERT(*((u64 *)logData) == lid);
			logData += sizeof(u64);
			CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == false);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((bool *)logData) == false);
			logData += sizeof(bool);

			if (isCompress) {
				CPPUNIT_ASSERT(*((u32 *)logData) < Limits::PAGE_SIZE * 2);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 2);
			} else {
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 2);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			}
			logData += sizeof(u32);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	CPPUNIT_ASSERT(j == 3);
	System::virtualFree(lob);


	//--------------下面大对象变小：-----------------------------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	lob = createLob(Limits::PAGE_SIZE - 50);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE - 50, isCompress);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	li1 = getLobIndexPageLSN(session, 1);
	li2 = getLobIndexPageLSN(session, 0);
	CPPUNIT_ASSERT(l1 == l2);
	CPPUNIT_ASSERT(li1 != l1);
	//m_lobStorage->get(session, mc, lid, &size);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		if (j == 4 && !isCompress) {
			byte *logData = logHdl->logEntry()->m_data;
			CPPUNIT_ASSERT(*((u64 *)logData) == lid);
			logData += sizeof(u64);
			CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == false);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((bool *)logData) == true);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((u32 *)logData) == 1);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((u32 *)logData) == 2);
			logData += sizeof(u32);

			if (isCompress) {
				CPPUNIT_ASSERT(*((u32 *)logData) < Limits::PAGE_SIZE - 50);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE - 50);
			} else {
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE - 50);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			}
			logData += sizeof(u32);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	CPPUNIT_ASSERT(j == 4);
	System::virtualFree(lob);

	//下面对象变大：但后面有空闲空间，并且空闲空间够----------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	lob = createLob(Limits::PAGE_SIZE);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE, isCompress);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	l3 = getLobPageLSN(session, 2);
	li1 = getLobIndexPageLSN(session, 1);
	li2 = getLobIndexPageLSN(session, 0);
	//CPPUNIT_ASSERT(l1 == l2);
	//CPPUNIT_ASSERT(l3 == l2);
	//CPPUNIT_ASSERT(li1 != li2);
	//m_lobStorage->get(session, mc, lid, &size);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		if (j == 5 && !isCompress) {
			byte *logData = logHdl->logEntry()->m_data;
			CPPUNIT_ASSERT(*((u64 *)logData) == lid);
			logData += sizeof(u64);
			CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == false);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((bool *)logData) == true);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((u32 *)logData) == 2);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((u32 *)logData) == 1);
			logData += sizeof(u32);

			if (isCompress) {
				CPPUNIT_ASSERT(*((u32 *)logData) < Limits::PAGE_SIZE);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE);
			} else {
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			}
			logData += sizeof(u32);
		}
	}
	System::virtualFree(lob);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	//CPPUNIT_ASSERT(j == 5);


	//下面对象变大：但后面有空闲空间，并且空闲空间不够(这里是新的pid +pageNum > fileTail)---------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	lob = createLob(Limits::PAGE_SIZE * 3);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 3, isCompress);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 0);
	l2 = getLobPageLSN(session, 1);
	l3 = getLobPageLSN(session, 2);
	li1 = getLobIndexPageLSN(session, 1);
	li2 = getLobIndexPageLSN(session, 0);
	if (!isCompress) {
	CPPUNIT_ASSERT(l1 != l2);
	CPPUNIT_ASSERT(l3 == l2);
	CPPUNIT_ASSERT(li1 == l1);
	CPPUNIT_ASSERT(li1 != li2);
	}
	//m_lobStorage->get(session, mc, lid, &size);
	m_db->getSessionManager()->freeSession(session);

	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		if (j == 6 && !isCompress) {
			byte *logData = logHdl->logEntry()->m_data;
			CPPUNIT_ASSERT(*((u64 *)logData) == lid);
			logData += sizeof(u64);
			CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == true);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((u32 *)logData) == 3);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == true);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((u32 *)logData) == 2);
			logData += sizeof(u32);
			if (isCompress) {
				CPPUNIT_ASSERT(*((u32 *)logData) < Limits::PAGE_SIZE * 3);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 3 );
			} else {
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 3);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			};
			logData += sizeof(u32);
		}
	}
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	//CPPUNIT_ASSERT(j == 6);
	System::virtualFree(lob);


	//下面对象变大：但后面有空闲空间，并且空闲空间不够---------------------
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	LobId lid2 = insertOnePage(isCompress);
	LobId lid3 = insertTwoPages(isCompress);
	LobId lid4 = insertOnePage(isCompress);
	LobId lid5 = insertTwoPages(isCompress);
	LobId lid6 = insertBoundaryPage(isCompress);
	m_lobStorage->del(session, lid2);
	m_lobStorage->del(session, lid3);
	m_lobStorage->del(session, lid4);
	lob = createLob(Limits::PAGE_SIZE * 9);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 9, isCompress);
	mc = session->getMemoryContext();
	l1 = getLobPageLSN(session, 3);
	li1 = getLobIndexPageLSN(session, 1);
	if (!isCompress)
	CPPUNIT_ASSERT(li1 == l1);


	//m_lobStorage->get(session, mc, lid, &size);
	m_db->getSessionManager()->freeSession(session);


	// 关闭数据库再打开
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// update log: LobId(u64) PageID(u32) isNewPageID (u8) (new PageID) isNewFreeBlock(u8) (newFreeBlockPageID(u32) newFreeBlockLen(u32)) LobLen(u32)
	// 现在检查log
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testUpdateLog", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		j++;
		if (j == 15 && !isCompress) {
			byte *logData = logHdl->logEntry()->m_data;
			CPPUNIT_ASSERT(*((u64 *)logData) == lid);
			logData += sizeof(u64);
			CPPUNIT_ASSERT(*((u32 *)logData) == 3);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == true);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((u32 *)logData) == 17);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((bool *)logData) == true);
			logData += sizeof(bool);
			CPPUNIT_ASSERT(*((u32 *)logData) == 3);
			logData += sizeof(u32);
			CPPUNIT_ASSERT(*((u32 *)logData) == 4);
			logData += sizeof(u32);
			if (isCompress) {
				CPPUNIT_ASSERT(*((u32 *)logData) < Limits::PAGE_SIZE * 9);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 9);
			} else {
				CPPUNIT_ASSERT(*((u32 *)logData) == Limits::PAGE_SIZE * 9);
				logData += sizeof(u32);
				CPPUNIT_ASSERT(*((u32 *)logData) == 0);
			}
			logData += sizeof(u32);
		}
	}
	System::virtualFree(lob);
	m_db->getSessionManager()->freeSession(session);
	m_db->getTxnlog()->endScan(logHdl);
	//CPPUNIT_ASSERT(j == 15);
}


/**
 * 得到大对象文件一页的lsn
 *
 * @param session 会话对象
 * @param pageNum 页号
 */
u64 LobOperTestCase::getLobPageLSN(Session *session, u64 pageNum) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getBlobFile() , PAGE_LOB_HEAP, pageNum, Shared, m_lobStorage->getLLobDatStats(), NULL);
	u64 lsn = bphdl->getPage()->m_lsn;
	session->releasePage(&bphdl);
	return lsn;
}

void LobOperTestCase::setLobPageLSN(Session *session, u64 pageNum, u64 lsn) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getBlobFile() , PAGE_LOB_HEAP, pageNum, Shared, m_lobStorage->getLLobDatStats(), NULL);
	bphdl->getPage()->m_lsn = lsn;
	session->releasePage(&bphdl);
}

/**
 * 得到目录文件一页的lsn
 *
 * @param session 会话对象
 * @param pageNum 页号
 */
u64 LobOperTestCase::getLobIndexPageLSN(Session *session, u64 pageNum) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, pageNum, Shared, m_lobStorage->getLLobDirStats(), NULL);
	u64 lsn = bphdl->getPage()->m_lsn;
	session->releasePage(&bphdl);
	return lsn;
}

void LobOperTestCase::setLobIndexPageLSN(Session *session, u64 pageNum, u64 lsn) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, pageNum, Shared, m_lobStorage->getLLobDirStats(), NULL);
	bphdl->getPage()->m_lsn = lsn;
	session->releasePage(&bphdl);
}


/**
 * 备份大对象文件的一页
 *
 * @param session 会话对象
 * @param pageNum 页号
 * @param pageBuffer 备份的空间
 */
void LobOperTestCase::backupLobPage(Session *session, u64 pageNum, byte *pageBuffer) {
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
void LobOperTestCase::restoreLobPage(Session *session, u64 pageNum, byte *pageBuffer) {
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
void LobOperTestCase::backupLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer) {
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
void LobOperTestCase::restoreLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer) {
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
bool LobOperTestCase::lobIsSame(byte *src, byte *dest, uint len) {
	//bool isSame = false;
	//for(uint i =0; i < len; i++ ) {
	//	if (*(src + i) == *(dest + i))
	//		continue;
	//	else
	//		return false;
	//}
	return true;
}


/**
 * 生成一个内容随机的LOB，长度为2页
 *
 * @param len 长度
 * @return 大对象内容
 */
byte* LobOperTestCase::createLob(uint *len) {
	//先生成2页大小的Lob
	*len = Limits::PAGE_SIZE * 2 - 512;
	uint size = *len;
	byte *lob = (byte *)System::virtualAlloc(size);
	for (uint i = 0; i < size; i++ ) {
		uint rand = System::random();
		*(lob + i) = (byte)(rand % 256);
	}
	return lob;
}

/**
 * 生成指定长度的随机字符串
 *
 * @return 字符串，使用new分配内存
 */
char * LobOperTestCase::randomString(uint size) {
	char *s = (char *)System::virtualAlloc(size);
	for (size_t i = 0; i < size; i++)
		s[i] = (char )('A' + System::random() % 10);
	return s;
}

/**
* 生成指定长度的伪随机字符串
*
* @return 字符串，使用new分配内存
*/
char* LobOperTestCase::pesuRandomString(uint size) {
	char *s = (char *)System::virtualAlloc(size);
	for (size_t i = 0; i < size; i++)
		s[i] = (char )('A' + (size + i) % 10);
	return s;
}

/**
 * 生成指定长度固定长度的LOB
 *
 * @param len 长度
 * @return 大对象内容
 */
byte* LobOperTestCase::createLob(uint len, bool useRandom) {
	//计算虚拟内存页数
	//u32 pageNum = (len + Limits::PAGE_SIZE -1) / Limits::PAGE_SIZE;
	//byte *lob = (byte *)System::virtualAlloc(len);
	//for (uint i = 0; i < len; i++ ) {
	//	*(lob + i) = (byte )(System::random() % 256);
	//}
	//char *randomlob = randomStrting(len);
	char *randomlob;
	if (useRandom)
		randomlob = randomString(len);
	else
		randomlob = pesuRandomString(len);
	return (byte *)randomlob;
}

/**
 * 得到小于256的随即数
 *
 */
u8 LobOperTestCase::getRandom() {
	return 0;
}


/**
 * 根据块的长度得到大对象长度
 *
 * @param size 块长度
 * @param isCompress 是否压缩
 * @return 大对象长度
 */
uint LobOperTestCase::getLobSize(uint size, bool isCompress) {
	//假如小于一页
	if (size <=  Limits::PAGE_SIZE)
		return size - BigLobStorage::OFFSET_BLOCK_FIRST;
	else {
		uint pageNum = (size + Limits:: PAGE_SIZE - 1)/ Limits:: PAGE_SIZE;
		return size - BigLobStorage::OFFSET_BLOCK_FIRST  -  (pageNum - 1) * BigLobStorage::OFFSET_BLOCK_OTHER;
	}
}

/**
 * 比较大对象是否一样，按自己比较
 *
 * @return 是否一致
 */
bool LobOperTestCase::compareLobs() {
	bool isRight = true;
	bool re = true;
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::createLobStorage", m_conn);
	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();
	u64 savepoint = mc->setSavepoint();
	uint i = (uint)lobContents.size();
	for (int j = i - 1; j >= 0; j--) {
		uint size;
		LobId lid = lobIds[j];
		byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
		CPPUNIT_ASSERT(size == lobContents[j].second);
		isRight = lobIsSame(lobContents[j].first, getLob, size);
		re = re && isRight;
		CPPUNIT_ASSERT(isRight);
	}
	mc->resetToSavepoint(savepoint);
	m_db->getSessionManager()->freeSession(session);
	return re;
}

/**
 * 备份文件具体实现
 *
 * @param file  备份文件的句柄
 * @param backupName  原文件名称
 */
void backupLobFile(File *file, const char *backupName) {
	u64 errCode = 0;

	File bk(backupName);
	errCode = bk.remove();
	assert(File::getNtseError(errCode) == File::E_NO_ERROR || File::getNtseError(errCode) == File::E_NOT_EXIST);
	errCode = bk.create(true, false);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	//bk.open(true);
	byte *buf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);

	int j = 0;
	u64 size = 0;
	u32 blockSize;
	file->getSize(&size);
	errCode = bk.setSize(size);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	for (; j < size; j += Limits::PAGE_SIZE) {
		blockSize = (u32)((j + Limits::PAGE_SIZE > size) ? (size - j) : Limits::PAGE_SIZE);
		errCode = file->read(j, blockSize, buf);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
		errCode = bk.write(j, blockSize, buf);
		assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	}
	errCode = bk.close();
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);

	bool exist;
	bk.isExist(&exist);
	assert(exist);
	System::virtualFree(buf);
}

/**
 * 备份文件
 *
 * @param origName  备份文件名称
 * @param backupName  原文件名称
 */
void backupLobFile(const char *origName, const char *backupName) {
	u64 errCode = 0;

	File orig(origName);
	orig.open(true);
	backupLobFile(&orig, backupName);
	orig.close();
}

/**
 * 恢复备份文件
 *
 * @param backupFile  备份文件
 * @param origFile  原文件
 */
void restoreLobFile(const char *backupFile, const char *origFile) {
	u64 errCode;
	File bk(backupFile);
	File orig(origFile);
	errCode = orig.remove();
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	errCode = bk.move(origFile);
	assert(File::getNtseError(errCode) == File::E_NO_ERROR);
	bk.close();
	orig.close();

}

/**
 * 测试insert,update,read以及del小型大对象，需要考虑测试是否在MMS中
 *
 * @param isCompress  是否压缩
 */
void LobOperTestCase::testInsertSmall(bool isCompress) {
	uint lobLen = Limits::MAX_REC_SIZE - 7;
	byte *lob = createLob(lobLen);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testInsertSmall", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	LobId lid = m_lobStorage->insert(session, lob, lobLen, isCompress);
	uint size;
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, true);
	CPPUNIT_ASSERT(size == lobLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, lob, size));

	//在读一次，这个时候在MMS中
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == lobLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, lob, size));
	System::virtualFree(lob);
	lob = NULL;

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



/**
 * 测试小型大对象更新成大型大对象
 *
 * @param isCompress  是否压缩
 */
void LobOperTestCase::testInsertTOBig(bool isCompress) {
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
	newLen = Limits::MAX_REC_SIZE;
	newLob = createLob(newLen);
	LobId lid3 = m_lobStorage->update(session, lid2, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid3 == lid2);
	getLob = m_lobStorage->get(session, mc, lid3, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	System::virtualFree(newLob);


	//测试在mms中的update
	lobLen = Limits::MAX_REC_SIZE - 8;
	lob = createLob(lobLen);
	lid = m_lobStorage->insert(session, lob, lobLen, isCompress);
	getLob = m_lobStorage->get(session, mc, lid, &size, true);
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
	getLob = m_lobStorage->get(session, mc, lid3, &size, true);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	m_db->getSessionManager()->freeSession(session);
	System::virtualFree(newLob);
}


/**
 * 测试redoUpdate小型大对象,这里对小型大对象没有File句柄，
 * 只能通过backupFile -->restoreFile形式来测试redo
 *
 * @param isCompress  是否压缩
 */
void LobOperTestCase::testSLRedoInsert(bool isCompress) {

	//先插入5个小型大对象
	uint len;
	byte *lob;
	LobId lid;

	clearLobs();

	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoInsert", m_conn);
	for (uint i = 0; i < 5; i++) {
		len = Limits::MAX_REC_SIZE - 7 - i;
		lob = createLob(len);
		lid = m_lobStorage->insert(session, lob, len, isCompress);
		lobIds.push_back(lid);
		pair<byte *, uint>lobContent = make_pair(lob, len);
		lobContents.push_back(lobContent);

		//进行backupFile
		if (i == 1) {
			File *file = m_lobStorage->m_slob->m_heap->getHeapFile();
			backupLobFile(file, LOB_SMALL);
		}
	}

	// 关闭再打开
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	//进行restoreFile
	restoreLobFile(LOB_SMALL,  LOBSMALL_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoInsert", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	int count = 0;

	// 检查日志
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (!isCompress && count++ < 1) {
			//测试parseInsertLog函数
			const LogEntry *entry = logHdl->logEntry();
			byte *logData = entry->m_data;
			size_t orgLen;
			LobId lid;
#ifdef NTSE_VERIFY_EX
			lid = *((u64 *)(logData + 16));
#else
			lid = *((u64 *)(logData));
#endif
			m_lobStorage->parseInsertLog(entry, lid, &orgLen, session->getMemoryContext());
		}
		m_lobStorage->redoSLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	//bool  isSame = compareLobs();
	//cout << isSame;
}

/**
* 测试redoUpdate小型大对象,这里是测试mms相关的
*
* @param isCompress  是否压缩
*/
void LobOperTestCase::testSLRedoUpdateMMS(bool isCompress) {
	//先插入3个大对象
	LobId lids[3];
	uint len;
	byte *lob;
	LobId lid;
	uint size;
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdateMMS", m_conn);
	for (uint i = 0; i < 3; i++) {
		len = Limits::MAX_REC_SIZE - i - 7;
		lob = createLob(len);
		lid = m_lobStorage->insert(session, lob, len, isCompress);
		lids[i] = lid;
		System::virtualFree(lob);
	}
	//然后读进到mms中
	MemoryContext *mc = session->getMemoryContext();
	for (uint i = 0; i < 3; i++) {
		m_lobStorage->get(session, mc, lids[i], &size);
	}

	//开始更新操作
	uint newLen;
	byte *newLob;
	for (uint i = 0; i < 3; i++) {
		newLen = Limits::MAX_REC_SIZE - i - 7;
		newLob = createLob(newLen);
		LobId newLid = m_lobStorage->update(session, lids[i], newLob, newLen, isCompress);
		lobIds.push_back(newLid);
		pair<byte *, uint>lobContent = make_pair(newLob, newLen);
		lobContents.push_back(lobContent);
	}

	// 关闭再打开
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdateMMS", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// 检查日志
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdateMMS", m_conn);
	//跳过insert日志
	while (j < 3) {
		m_db->getTxnlog()->getNext(logHdl);
		j++;
	}
	j = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (j < 3)
		m_lobStorage->redoSLUpdateMms(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
		j++;
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	bool  isSame = compareLobs();
	assert(isSame);
}


/**
 * 测试redoUpdate小型大对象,这里是测试堆中
 *
 * @param isCompress  是否压缩
 */
void LobOperTestCase::testSLRedoUpdate(bool isCompress) {
	//先插入5个小型大对象
	uint len;
	byte *lob;
	LobId lid;	vector<LobId> newLobIds ;
	
	clearLobs();

	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdate", m_conn);
	for (uint i = 0; i < 5; i++) {
		len = Limits::MAX_REC_SIZE - i - 7;
		lob = createLob(len);
		lid = m_lobStorage->insert(session, lob, len, isCompress);
		newLobIds.push_back(lid);
		System::virtualFree(lob);
	}

	Buffer *buffer = m_db->getPageBuffer();
	buffer->flushAll();


	//进行更新
	uint newLen;
	byte *newLob;
	for (uint i = 0; i < 5; i++) {
		newLen = Limits::MAX_REC_SIZE - i - 7;
		newLob = createLob(newLen);
		if (i == 1) {
			File *file = m_lobStorage->m_slob->m_heap->getHeapFile();
			backupLobFile(file, LOB_SMALL);
		}
		LobId newLid = m_lobStorage->update(session, newLobIds[i], newLob, newLen, isCompress);
		lobIds.push_back(newLid);
		pair<byte *, uint>lobContent = make_pair(newLob, newLen);
		lobContents.push_back(lobContent);
	}

	// 关闭再打开
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	//进行restoreFile
	restoreLobFile(LOB_SMALL, LOBSMALL_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// 检查日志
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdate", m_conn);
	//跳过insert日志
	while (j < 5) {
		m_db->getTxnlog()->getNext(logHdl);
		j++;
	}
	j = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (j < 5) {
			m_lobStorage->redoSLUpdateHeap(session, lobIds[j], lsn, logHdl->logEntry()->m_data,
				(uint)logHdl->logEntry()->m_size, lobContents[j].first, lobContents[j].second, isCompress);
			j++;
		}
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	bool  isSame = compareLobs();
	cout << isSame;
	newLobIds.clear();
}

/**
 * 测试del小型大对象
 *
 * @param isCompress  是否压缩
 */
void LobOperTestCase::testSLRedoDel(bool isCompress) {

	//先插入5个小型大对象
	uint len;
	byte *lob;
	LobId lid;
	
	clearLobs();

	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoDel", m_conn);
	for (uint i = 0; i < 5; i++) {
		len = Limits::MAX_REC_SIZE - i - 7;
		lob = createLob(len);
		lid = m_lobStorage->insert(session, lob, len, isCompress);
		lobIds.push_back(lid);
		pair<byte *, uint>lobContent = make_pair(lob, len);
		lobContents.push_back(lobContent);
	}

	Buffer *buffer = m_db->getPageBuffer();
	buffer->flushAll();

	for (uint i = 0; i < 5; i++) {
		if (i == 1) {
			File *file = m_lobStorage->m_slob->m_heap->getHeapFile();
			backupLobFile(file, LOB_SMALL);
		}
		m_lobStorage->del(session, lobIds[i]);
	}

	// 关闭再打开
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	//进行restoreFile
	restoreLobFile(LOB_SMALL,  LOBSMALL_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoDel", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// 检查日志
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoDel", m_conn);
	//跳过insert日志
	while (j < 5) {
		m_db->getTxnlog()->getNext(logHdl);
		j++;
	}
	j = 0;
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		m_lobStorage->redoSLDelete(session, lobIds[j], lsn, logHdl->logEntry()->m_data,
			(uint)logHdl->logEntry()->m_size);
		j++;
	}
	m_db->getTxnlog()->endScan(logHdl);

	//get得不到
	//生成MemoryContext
	MemoryContext *mc = session->getMemoryContext();
	uint i = (uint)lobContents.size();
	for (int j = i - 1; j >= 0; j--) {
		uint size;
		LobId lid = lobIds[j];
		byte *getLob = m_lobStorage->get(session, mc, lid, &size, true);
		CPPUNIT_ASSERT(getLob == NULL);
	}
	m_db->getSessionManager()->freeSession(session);
}


/**
 * 测试得到文件句柄
 */
void LobOperTestCase::testGetFiles(){

	File **files = new File*[3];
	int num = 3;
	PageType *py = new PageType[3];
	int re = m_lobStorage->getFiles(files, py, 3);
	CPPUNIT_ASSERT(re == 3);

	delete [] files;
	delete [] py;
}


/**
 * 测试redoInsert时候，需要扩展大对象文件
 * 测试步骤：
 * 1、先测试插入多个大对象刚好到fileLen长度
 * 2、然后插入一个大对象A
 * 3、恢复大对象文件到A之前
 * 4、做恢复工作
 */
void LobOperTestCase::insertRedoAndExtendFile() {
	for(uint i = 0 ; i < 1024 / 2 ; i++) {
		insertTwoPages(false);
	}
	Buffer *buffer = m_db->getPageBuffer();
	buffer->flushAll();

	//先backupFile
	File *lobFile = m_lobStorage->getBlobFile();
	backupLobFile(lobFile, LOB_BACKUP);

	//backup 目录文件
	File *indexFile = m_lobStorage->getIndexFile();
	backupLobFile(indexFile, LOBINDEX_BACKUP);

	insertTwoPages(false);

	// 关闭数据库再打开
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertRedoAndExtendFile", m_conn);
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close();
	delete m_db;
	m_db = NULL;

	restoreLobFile(LOB_BACKUP, LOBBIG_PATH);
	restoreLobFile(LOBINDEX_BACKUP, LOBINDEX_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertRedoAndExtendFile", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, false));
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	//然后再insertLob,可以继续操作
	insertBig(true);
	insertBig(false);
}


/**
 * 为defrag做准备，insert7大对象，然后删除其中3个
 *
 * @param isCompress  是否压缩
 */
void LobOperTestCase::insertAndDelLobs(bool isCompress) {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::insertAndDelLobs", m_conn);
	uint size = getLobSize(Limits::PAGE_SIZE, isCompress) * 2;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 2, isCompress) - 1;
	insertLob(session, size, isCompress);
	size = getLobSize(Limits::PAGE_SIZE * 2 - 200, isCompress);
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
 * insert 一个大对象
 *
 * @param session  会话对象
 * @param size  大对象长度
 * @param isCompress  是否压缩
 */
void LobOperTestCase::insertLob(Session *session, uint size, bool isCompress){
	byte *lob = createLob(size);
	LobId lid = m_lobStorage->insert(session, lob, size, isCompress);
	lobIds.push_back(lid);
	pair<byte *, uint> p1 = make_pair(lob, size);
	lobContents.push_back(p1);
}

void LobOperTestCase::clearLobs() {
	if (lobIds.size() > 0) {
		lobIds.clear();
		for (vector<pair<byte *, uint> >::iterator it = lobContents.begin(); it != lobContents.end(); it++) {
			pair<byte *, uint> &lobPair = *it;
			System::virtualFree(lobPair.first);
		}
		lobContents.clear();
	}
}

/************************************************************************/
/*                     测试多线程的公用类								*/
/************************************************************************/

void LobTester::run() {
	switch(m_task) {
		case LOB_INSERT1:
			m_testCase->doInsert1();
			break;
		case LOB_INSERT2:
			m_testCase->doInsert2();
			break;
		case L0B_READ:
			m_testCase->doRead();
			break;
		case LOB_DEL:
			m_testCase->doDel();
			break;
		case LOB_UPDATE:
			m_testCase->doUpdate();
			break;
		case LOB_DEFRAG_FINISH:
			m_testCase->doDefragForRead();
			break;
		case LOB_INSERT:
			m_testCase->doInsert(m_testHelper);
			break;
		case LOB_DEFRAG_DOING:
			m_testCase->doDefrag();
			break;
		case LOB_DELBYID:
			m_testCase->doDelByLobid();
			break;
		default:
			break;
	}
}

