/**
 * ���Դ�����������
 * @author zx (zx@corp.netease.com, zx@163.org)
 * �������⣺
 * 1�����úñ��cacheUpdate,�ֶε�cacheUpdate��ô����,�Ѿ����
 * 2��limit:max_record_size����,����Ѿ����
 * 3��defrag����ĩβ����
 * 4��ͳ����Ϣ
 * 5����Ҫ����
 * 6����Ҫ����
 * 7����Ҫ����
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
 * ׼�����Ի�����������������storage
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
 * drop �����洢����
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
 * ������������storage
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
/*   2008-11-13 Ϊ�˲���ѹ���ͷ�ѹ���� ����������������������������   */
/* �ŵ�һ��ȥ��������е���                                             */
/*	 �����ÿ����������˲�������֤����ȷ��ʱ������ͨ���ڲ���ǰ����   */
/* �����ĳ��Ⱥ����ݷ���һ��pair�У�Ȼ�����������ͨ����ѯ�õ���� */
/* �ͼ�¼�����ݺͳ��Ƚ�����֤                                           */
/************************************************************************/

/**
 * 2009-1-8 ����400M ��ʵ�Ĳ������ݣ�����ѹ��Ч��
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
 * ���Ӳ������� 2009-03-10
 * ״̬ͳ��
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

	//Ȼ����һЩ��������ͳ��
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

	////����ܶ�������������
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

	////��һЩdel����
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

	//��һЩmms���ò���
	m_lobStorage->setMmsTable(session, false, true);
	m_lobStorage->setMmsTable(session, false, false);
	m_lobStorage->setMmsTable(session, true, true);
	m_lobStorage->setMmsTable(session, true, false);
	
	m_lobStorage->flush(session);

	m_db->getSessionManager()->freeSession(session);
}

/**
 * ���Ӳ������� 2009-1-9
 * ���Ե������sizeΪ0����lob != NULL ���
 */
void  LobOperTestCase::testSizeZero() {
	bool compress = true;
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testBlog", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	byte *lob = new byte[0];
	LobId lid = m_lobStorage->insert(session, lob, 0);
	//update�󳤶ȴ���0
	uint len = Limits::MAX_REC_SIZE - 7;
	byte *newLob = createLob(len);
	LobId newLid = m_lobStorage->update(session, lid, newLob, len, compress);
	assert(lid == newLid);
	uint size;
	byte *getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(size == len);
	assert(lobIsSame(newLob, getLob, len));
	m_lobStorage->del(session, lid);


	//���Ը�����mms���
	lid = m_lobStorage->insert(session, lob, 0);
	getLob = m_lobStorage->get(session, mc, lid, &size);
	assert(size == 0);
	assert(getLob != NULL);
	//Ȼ�����
	newLid = m_lobStorage->update(session, lid, newLob, len, compress);
	assert(lid == newLid);
	getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(size == len);
	assert(lobIsSame(newLob, getLob, len));
	m_lobStorage->del(session, lid);


	//���Ը���ΪsizeΪ0
	lid = m_lobStorage->insert(session, newLob, len);
	newLid = m_lobStorage->update(session, lid, lob, 0, compress);
	//��ֻ֤������ѹ��λ
	assert(lid == newLid);
	getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(getLob != NULL);
	assert(size == 0);
	m_lobStorage->del(session, lid);

	//���Ը���ΪsizeΪ0,��mms��
	lid = m_lobStorage->insert(session, newLob, len);
	getLob = m_lobStorage->get(session, mc, lid, &size);
	newLid = m_lobStorage->update(session, lid, lob, 0, compress);
	//��ֻ֤������ѹ��
	assert(lid == newLid);
	getLob = m_lobStorage->get(session, mc, newLid, &size);
	assert(getLob != NULL);
	assert(size == 0);
	m_lobStorage->del(session, lid);

	//������
	delete [] lob;
	System::virtualFree(newLob);
	mc->reset();
	m_db->getSessionManager()->freeSession(session);
}


/**
 * ���Դ��ʹ�����insert
 * ��Ҫ���裺
 * 1������Ϊ�˲��Ա߽����� ������һ���պ���2ҳ�Ĵ����
 * 2������һ���պ���1ҳ�Ĵ����
 * 3������һ���պ���3ҳ�Ĵ����
 * 4������һ���պ���4ҳ�Ĵ����
 */
void  LobOperTestCase::testAllInsertBig() {
	testInsertBig(false);
	testInsertBig(true);
	//lobIds.clear();
}

/**
 * ���Դ��ʹ�����insert
 * ��Ҫ���裺
 * 1��ִ������testAllInsertBig����
 * 2��Ȼ�������β�ѯ�����
 * 3�����ֽڱȽϴ��������
 */
void LobOperTestCase::testAllReadBig() {
	testReadBig(false);
	testReadBig(true);
}

/**
 * ���Դ��ʹ�����update
 * ��Ҫ���裺
 * 1������һ�����ʹ����L1��ҳ��ΪPn
 * 2��Ȼ��update L1�� ��ҳ��P1 == Pn
 * 3��Ȼ��update L1�� ��ҳ��P2 > P1������L1���ļ�ĩβ�����Ի�ֱ���ƶ����ļ�ĩβ
 * 4���ٲ���3������� L2 ,L3 ,L4 ��Ȼ��ɾ��ǰ��2�������
 * 5��Ȼ��update L1�� ��ҳ��P3 > P2���������������пռ�Fn >= P3 - P2
 * 6��Ȼ��update L1�� ��ҳ��P4 > P3���������������пռ�Fn < P4 - P3
 */
void LobOperTestCase::testAllBigUpdate() {
	testBigUpdate(false);
	clearLobs();
	testBigUpdate(true);
}

/**
 * ���Դ��ʹ�����delete
 * ��Ҫ���裺
 * 1������һ������Ȼ��ɾ��
 * 2�������������ʹ�պ�����һ��Ŀ¼ҳ
 * 3��Ȼ��ɾ��һ��2�����в���Ĵ����
 * 4�����²���һ�������3�е�Ŀ¼�����
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
	testDelBig(true);*/     // ���ѹ����С�ʹ���󣬾����׵��¶��Դ�
}

/**
 * ���Դ��ʹ�����redoInsert
 * ��Ҫ���裺
 * 1���Ȳ���һ������Ȼ��ر����ݿ⣬���ؿ����ݿ⣬�鿴��־��֤logInsert��ȷ��
 * 2���Ѵ�����ļ��ĵ�һ��ҳ�͵���ҳ���ݣ�Ȼ�����һ��3ҳ��Ĵ���󣬰ѵ�һҳ�͵���ҳ�ָ�������
 * 3��Ȼ��ر����ݿ⣬���ؿ����ݿ⣬Ȼ��redoInsert,����ѯ�ô���� �ֽ���֤һ�£�����һ��
 * 4�������������ֱ����һ��Ŀ¼ҳֻʣһ������Ŀ¼�Ȼ���Ŀ¼�ļ��ĵ�һ��ҳ����ҳ���͵ڶ�ҳ����
 * 5���ٲ���һ�������Ȼ���Ŀ¼�ļ��ĵ�һҳ�͵ڶ�ҳ�ָ�������
 * 6��Ȼ��ر����ݿ⣬���ؿ����ݿ⣬Ȼ��redoInsert����֤ǰ��Ĳ���Ĵ������ȷ
 * 7���ٱ���Ŀ¼�ļ��ڶ�ҳ���ʹ�����ļ��ļ�ĩβ��ҳ
 * 8��Ȼ�����һ��ҳ��Ϊ3ҳ�Ĵ����
 * 9��Ȼ��ر����ݿ⣬���ؿ����ݿ⣬Ȼ��redoInsert����֤8����Ĵ�������ȷ
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
 * ���Դ��ʹ�����redoDel
 * ��Ҫ���裺
 * 1���Ȳ���һ������Ȼ��ر����ݿ⣬���ؿ����ݿ⣬�鿴��־��֤logDel��ȷ��
 * 2���Ѵ�����ļ��ĵ�һ��ҳ��Ŀ¼�ļ��ĵ�һҳ���ݣ�Ȼ�����һ��3ҳ��Ĵ���󣬰�Ŀ¼�ļ��ʹ�����ļ��ָ�������
 * 3��Ȼ��ر����ݿ⣬���ؿ����ݿ⣬Ȼ��redoDel
 * 4�������������ֱ����һ��Ŀ¼ҳû�п���Ŀ¼�Ȼ�󱸷�Ŀ¼�ļ��ĵ�һҳ�͵ڶ�ҳ
 * 5���ٲ���һ�������Ȼ���Ŀ¼�ļ��ĵ�һҳ�͵ڶ�ҳ�ָ�������
 * 6��Ȼ��ر����ݿ⣬���ؿ����ݿ⣬Ȼ��redoDel����֤ǰ���ɾ���Ĵ������ȷ
 */
void LobOperTestCase::testRedoInsertAndDel() {
	testRedoDelEx(true);
}

void LobOperTestCase::testRedoInsertAndDelEx() {
	testRedoDelEx(false);
}

/**
 * ���Դ��ʹ�����logUpdate
 * ��Ҫ���裺
 * 1���Ȳ���һ������L1������ΪP1��ҳ����Ȼ����¸ô���󣬸���ҳ������
 * 2������L1��ҳ��ΪP2��P2 < P1
 * 3������L1��ҳ��ΪP3��P3 > P2
 * 4������L1��ҳ��ΪP3��P4 > P3
 * 5����������L2��L3, L4,ҳ���ֱ�ΪPd1,Pd2, Pd3,Ȼ��ɾ��L2��L3
 * 6������L1�����º��ҳ��P5 - P4 < Pd1 + Pd2�����Դ�������ԭ��
 * 7������L1�����º��ҳ��P6 - P5 > ����������Ŀ��пռ�
 * 8��Ȼ��ر����ݿ⣬����open���ݿ⣬ ��֤���ϸ�����־����ȷ��
 */
void LobOperTestCase::testAllUpdateLog() {
	testUpdateLog(false);
}

/**
 * ���Դ��ʹ�����redoUpdate
 * ��Ҫ���裺
 * 1���Ȳ���һ������Ȼ����¸ô����Ȼ��ر����ݿ⣬���ؿ����ݿ⣬�鿴��־��֤logUpdate��ȷ��
 * 2������һ�������L1����ҳ��ΪP1,Ȼ�󱸷�Ŀ¼�ļ��ĵ�һҳ
 * 3������L1�ĵ�һҳ�����һҳ������L1����P2 == P1,	 �ָ�������ǰ��ҳ
 * 4����������L2 ��Ȼ�󱸷����һҳ�� ������ʹ֮���ȼ�ҳ������Ȼ��ָ���һҳ
 * 5���ٲ���3�� L3, L4, L5��������Ȼ��ɾ��ǰ����������L3��L4��ҳ
 * 6������L2,ʹ�䳤��ΪP3��ʹ�������г��� Fn > P3 - P2��Ȼ��ָ�5���ݵ�ҳ
 * 7����������L5,L6,L7,Ȼ��ɾ��L6��L7,����L5�ĳ���ΪP4
 * 8������L5����ҳPh��Ŀ¼�ļ���һ��ҳPi1������L5��ʹ�䳤�ȵ���P5,������Ŀռ�ռ䳤��Fn' < P5 - P4.Ȼ��ָ�Ph��Pi1.
 * 9����������L8������Ϊ2ҳ��Ȼ�󱸷���2ҳ��Ȼ����±��һҳ��Ȼ��ָ����ݵ�2ҳ
 * 10����������L9��Ȼ�󱸷�����ҳ��Ȼ�󱸷�Ŀ¼ҳ��Ȼ���������չ2ҳ����Ϊ���ļ�ĩβ�����Դ����ֱ����չ����Ȼ��ָ�
 *     ����Ŀ¼ҳ�Ϳ���ҳ
 * 11���ر����ݿ⣬Ȼ��open���ݿ⣬����redo����
 * 12����ǰ��Ĵ������в�ѯ����֤��ȷ��
 */
void LobOperTestCase::testAllRedoUpdate() {
	testRedoUpdate(false);
	//testRedoUpdateEx();
}


/**
 * ����redoCreate
 * ��Ҫ���裺������Ҫ�ּ������
 * 1��create��ѣ�Ȼ��ر����ݿ⣬remove����ļ���Ȼ��open���ݿ⣬��redoCreate
 * 2��create���ļ���û�в�����������˵���Ȳ���
 * 3���ļ��Ѿ�open��ʹ���У�����redoCreate
 */
void LobOperTestCase::testAllRedoCreate() {
	testRedoCreate(false);
	testRedoCreate(true);
}


/*
 * ����insertС�ʹ����ͬʱ���º�ɾ��
 * ��Ҫ���裺
 * 1��ֱ�Ӳ���պó��ȵ���LIMITS::MAX_REC_SIZE��С�Ĵ���󣬲���ɹ�������ѯ�õ������ȷ
 * 2������պó��ȵ���MIN_COMPRESS_LEN������Ҫѹ���ĳ������ƣ��Ĵ���󣬸ô������ѹ��
 *  ��ʽ����ɹ���
 * 3������պó��ȵ���MIN_COMPRESS_LEN - 1 �Ĵ���󣬸ô����ʹָ����Ҫѹ��Ҳ���ղ�ѹ����ʽ
 *  ����
 * 4���ٲ���һ��С�ʹ����Ȼ���ѯ����ѯ�����ļ��еõ����õ���ȷ�����Ȼ���ٴβ�ѯ�����
 *  ʱ�򽫴�MMS��ѯ��Ȼ��õ���ȷ�����
 * 5���ٲ���һ��С�ʹ����Ȼ����и���û�����ʱ�򽫵��ļ��и��£�Ȼ���ѯ�ô����Ȼ����
 *  �θ��£����ʱ����MMS�и��£�Ȼ���ٴβ�ѯ���õ���ȷ���
 * 6��ɾ��ǰ���5�����Ĵ����ģ�Ȼ���ڲ�ѯ��ɾ���Ĵ���󣬵õ����ΪNULL��sizeΪ0
 */
void LobOperTestCase::testAllInsertSmall() {
	testInsertSmall(false);
	testInsertSmall(true);
}

/*
 * ����insertС�ʹ����ͬʱ����Ϊ���ʹ����������Լ�����ɾ�����
 * ��Ҫ���裺
 * 1��ֱ�Ӳ���պó��ȵ���LIMITS::MAX_REC_SIZE ��С�Ĵ����L1������ɹ�������ѯ�õ������ȷ
 * 2�����´����L1���䳤��ΪLIMITS:PAGE_SIZE, �������£� ԭ�������L1��ɾ����Ȼ��Ѹ��µ�
 * ���뵽���ʹ�����ļ�����ɴ����L2��LobId�����仯
 * 3���Ѵ����L2���£����º󳤶�С��LIMITS::MAX_REC_SIZE�������Է��ڴ��ʹ�����ļ��У�LobId����
 * 4���ڲ���һ��С�ʹ����L3��Ȼ����в�ѯ�����ʱ�����ݷ���MMS�С�
 * 5���ٸ���L3�����ʱ�򳤶�ΪLIMITS:PAGE_SIZE * 2���������½�ɾ��MMS�к�HEAP��L3��Ȼ�����
 *  ���ݵ����ʹ�����ļ�
 * 6���ٲ�ѯ��5���º����󣬵õ���ȷ���
 */
void LobOperTestCase::testAllInsertTOBig() {
	testInsertTOBig(false);
	//testInsertTOBig(true);
}

/**
 * ����С�ʹ�����redoInsert
 * ��Ҫ���裺
 * 1������5��С�ʹ����Ȼ����ļ�����
 * 2���ٲ���5��С�ʹ����
 * 3���ر����ݿ⣬Ȼ��ѱ��ݵ��ļ��ָ���Ȼ����open���ݿ�
 * 4��Ȼ��ʼ������־����redoSLInsert
 * 5��ͨ������ÿ������Ĵ���󣬺ͼ�¼�Ĵ�������ݱȽϣ��Ƚ�һ��
 */
void LobOperTestCase::testAllSLRedoInsert() {
	testSLRedoInsert(false);
	testSLRedoInsert(true);
}


/**
 * ����С�ʹ�����redoUpdate�������������û�а�����ɴ��ʹ��������
 * ��Ҫ����redo��heap��ص�
 * ��Ҫ���裺
 * 1������5�������:L1, L2, L3, L4, L5��Ȼ��buffer flushAll�󣬱����ļ�
 * 2����L1���и��£�ʹ�䳤�ȱ�С
 * 3����L2���и��£�ʹ�䳤�ȱ��
 * 4����L3���и��£����³���ΪLIMITS::MAX_REC_SIZE
 * 5����L4���и��£����³��Ȳ���
 * 6��Ȼ��ر����ݿ⣬�ָ����ݵ��ļ���Ȼ��open���ݿ�
 * 7���Դӵ�6����־��ʼ����redoSLUpdate
 * 8��ͨ���Ը��µĴ������в�ѯ�� Ȼ��ͼ�¼�Ĵ�������ݽ��бȽϣ��ж�����һ���ԡ�
 */
void LobOperTestCase::testAllSLRedoUpdate() {
	testSLRedoUpdate(false);
	clearLobs();
	tearDown();
	setUp();
	testSLRedoUpdate(true);
}

/**
 * ����С�ʹ�����redoUpdate�������������û�а�����ɴ��ʹ��������
 * ��Ҫ����redo��MMS��ص�
 * ��Ҫ���裺
 * 1������5�������:L1, L2, L3
 * 2����L1,L2,L3����read,ʹ��put into Mms
 * 3��Ȼ�����L1,L2,L3
 * 4���ر����ݿ�,����ˢ����־�� ����ˢ����
 * 5�������������ݿ⣬Ȼ����redoUpdateMms
 * 6��Ȼ���ѯL1,L2,L3,�õ���������һ��
 */
void LobOperTestCase::testAllSLRedoUpdateMms() {
	testSLRedoUpdateMMS(false);
	tearDown();
	setUp();
	testSLRedoUpdateMMS(true);
}

/**
 * ����С�ʹ�����redoDel
 * ��Ҫ���裺
 * 1������5�������:L1, L2, L3, L4, L5��Ȼ��buffer flushAll�󣬱����ļ�
 * 2��ɾ�������5�������
 * 3��Ȼ��ر����ݿ⣬�ָ����ݵ��ļ���Ȼ��open���ݿ�
 * 4����ѯ����5��������ܲ�ѯ��
 * 5���Դӵ�6����־��ʼ����redoSLDel
 * 6��Ȼ���ٲ�ѯǰ��5������󣬽���ѯ����
 */
void LobOperTestCase::testAllSLRedoDel() {
	testSLRedoDel(false);
	clearLobs();
	tearDown();
	setUp();
	testSLRedoDel(true);
}


/************************************************************************/
/*                     ���Զ��߳�										*/
/************************************************************************/
/**
 *	��Ҫ���ԣ�
 *	1��ͬ��д(��־˳���෴)
 *	2������д
 *	3�����͸���
 *	4������ɾ��
 *	5����Ƭ����Ͷ�
 *	6����Ƭ�����д
 *	7����Ƭ����͸���
 *	8����Ƭ�����ɾ��
 *	9����Ƭ��������ļ�β����
 *	10�����µĸ������
 */


/**
 * ����Insertʱ���ȵõ�����Ŀ¼ҳP1 ��������ס��ҳ��ȥ�жϣ�P1�Ѿ����ǵ�һ������ҳ�������ز��ҿ���ҳ
 * ���Բ��裺
 * 1���߳�Aȥ���������ȵõ�����Ŀ¼ҳF1��������F1��Ȼ��ȴ�
 * 2���߳�Bɾ������󣬷���Ŀ¼ҳ������һ������slot, Ȼ���ҳ���뵽����ҳ����
 * 3��B��ȷɾ��������߳�A����ִ��
 * 4���߳�A���ֲ��ǵ�һ������ҳ�ˣ������ȥ���²��ҿ���ҳ
 * 5���߳�A��ȷ��������
 */
void LobOperTestCase::insertGetNoFirstPage(){
	clearLobs();

	//�Ȳ���һ�������
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
 * ����LobIdɾ��һ�������
 */
void LobOperTestCase::doDelByLobid() {
	doDelByLobid(lobIds[Limits::PAGE_SIZE / LobIndex::INDEX_SLOT_LENGTH]);
}

 /**
 * ����LobIdɾ��һ�������
 *
 * @param lid �����ID
 */
void LobOperTestCase::doDelByLobid(LobId lid) {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doDelByLobid", m_conn);
	m_lobStorage->del(session, lid);
	m_db->getSessionManager()->freeSession(session);
}

/**
 * ����Insertʱ�򣬵õ���FirstFreePage == 0��������ס��ҳ��ȥ�ж�ʱ�򣬷���FirstFreePage > 0��
 * ˵�������߳�Ҫô�ͷ���Ŀ¼�Ҫô��չ��Ŀ¼�ļ�
 * ���Բ��裺
 * 1���߳�Aȥ���������ȵõ�����Ŀ¼ҳF1��Ȼ���ͷ�F1���õ���FirstFreePage == 0��Ȼ��ȴ�
 * 2���߳�BҲ�������󣬵õ�����Ŀ¼ҳF1������FirstFreePage == 0����չ�ļ�
 * 3���߳�A����ִ��
 * 4���߳�A����F1û�п���Ŀ¼ҳʱ������ȥȥ��ס��ҳ������FirstFreePage > 0
 * 5���߳�A��ȷ��������
 */
void LobOperTestCase::insertGetNoFreePage(){
	//��ʼ���Զ��߳�
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
 * ����Insertʱ���໥����Ŀ¼��ģ�ȥ�õ�����Ŀ¼ҳ���õ�����Ŀ¼ҳ�Ѿ�������ʹ�ã�
 * ���Բ��裺
 * 1���߳�Aȥ���������ȵõ�����Ŀ¼F1��Ȼ���ͷ�F1��Ȼ��ȴ�
 * 2���߳�BҲ�������󣬵õ�����Ŀ¼ҳF1���������ظ���������ֱ��F1û�п���Ŀ¼��
 * 3���߳�A����ִ��
 * 4���߳�A����F1û�п���Ŀ¼��ʱ������ȥȥ��ס��ҳ��Ȼ������ִ��
 * 5���߳�A��ȷ��������
 */
void LobOperTestCase::insertGetFreePage(){
	//�Ȳ���һ�������
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

	//��ʼ���Զ��̣߳�A��B�������һ������Ŀ¼��
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
 * ����Updateʱ���ڵõ�Ŀ¼���defrag��������Ҫ���»�ȥ�����º�Ŀ¼��
 * ���Բ��裺
 * 1���߳�Aȥ�����L1���ȵõ�Ŀ¼��I1��Ȼ��ȴ�
 * 2���߳�B��Ƭ�����L1�����Ȼ�����ִ��
 * 3���߳�A����ִ��
 * 4���߳�A���ֵ�ǰҪ���Ŀ���ҳ����Ҫ�ҵ�ҳ�����¶�ȡĿ¼��
 * 5���߳�A��ȷ�޸Ĵ����L1
 */
void LobOperTestCase::defragFirstAndUpdate() {
	//��׼������
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
 * ����Delʱ���ڵõ�Ŀ¼���defrag��������Ҫ���»�ȥ�����º�Ŀ¼��
 * ���Բ��裺
 * 1���߳�Aȥdel�����L1���ȵõ�Ŀ¼��I1��Ȼ��ȴ�
 * 2���߳�B��Ƭ�����L1�����Ȼ�����ִ��
 * 3���߳�A����ִ��
 * 4���߳�A���ֵ�ǰҪ���Ŀ���ҳ����Ҫ�ҵ�ҳ�����¶�ȡĿ¼��
 * 5���߳�A��ȷɾ�������
 */
void LobOperTestCase::defragFirstAndDel() {
	//��׼������
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
 * ����readʱ���ڵõ�Ŀ¼���defrag��������Ҫ���»�ȥ�����º�Ŀ¼��
 * ���Բ��裺
 * 1���߳�Aȥread�����L1���ȵõ�Ŀ¼��I1��Ȼ��ȴ�
 * 2���߳�B��Ƭ�����L1�����Ȼ�����ִ��
 * 3���߳�A����ִ��
 * 4���߳�A���ֵ�ǰҪ���Ŀ���ҳ����Ҫ�ҵ�ҳ�����¶�ȡĿ¼��
 * 5���߳�A�õ������L1������
 */
void LobOperTestCase::defragFirstAndRead () {
	//��׼������
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
 * ����defragʱ���в��ϵ����ݲ������ر���INSERT����
 * ���Բ��裺
 * 1��׼������������8�������Ȼ��ɾ������3��
 * 2���߳�A����insert����󣬲���¼�����ID
 * 3���߳�B��ʼdefrag,
 * 4��B���ô�����ļ�tail,�����н���
 * 5��A����B������Ҳ������Ȼ���ѯ��¼�Ĵ����ID��
 */
void LobOperTestCase::defragAndInsert(){
	//��׼������
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
	//��֤����
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
 * ���в��ϵ�insert�����,�����ҪΪ�˲���defragʱ��,��������ͣinsert�����ʱ��,
 * ����ж�β��,������β��
 *
 * @param tester ������Ƭ����Ĳ�����
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
 * ����defrg,�����ҪΪ�˲��Ե��Դ����read,update��delʱ���ִ�����Ѿ��������ƶ�
 */
void LobOperTestCase::doDefrag() {
	doDefragForRead();
}

/**
 * ���и���,ǰ�����Ѿ���������Ƭ������ƶ�
 */
void LobOperTestCase::doUpdate() {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doUpdate", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	uint size;

	byte *lob = createLob(Limits::PAGE_SIZE * 2);
	m_lobStorage->update(session, lobIds[3], lob, Limits::PAGE_SIZE * 2, true);

	byte *newLob = createLob(Limits::PAGE_SIZE * 5);
	m_lobStorage->update(session, lobIds[7], newLob, Limits::PAGE_SIZE * 5, true);
	//�ж�����û�仯
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
 * ����ɾ��,ǰ�����Ѿ���������Ƭ������ƶ�
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
 * ���ж�ȡ,ǰ�����Ѿ���������Ƭ������ƶ�
 */
void LobOperTestCase::doRead() {
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doRead", m_conn);
	MemoryContext *mc = session->getMemoryContext();
	uint size;
	byte *lob1 = m_lobStorage->get(session, mc, lobIds[3], &size, false);
	//�ж�����û�仯
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
 * ����defrag
 */
void LobOperTestCase::doDefragForRead() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTestCase::doDefragForRead", conn);
	
	m_lobStorage->defrag(session);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/**
* ����testOpenBigLobException
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
* ����testOpenLobIndexException
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
* ����testCreateBigLobException
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
* ����testCreateBigLobException
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
* ����testCreateLobIndexException
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
 * ����insertʱ��д��־���ǰ���������д��˳��
 * ��redoInsertʱ����Ҫ�ж���ȷ���ļ�tail
 * ���Բ��裺1���߳�A���õ�Ȼ���ļ�ĩβ���ȣ��õ��Լ�д���Pid��Ȼ��wait
 * 2���߳�BȻ���ٵõ�������ļ����ȣ��õ��Լ�д��Pid��Ȼ��д��־��д����
 * 3���߳�AȻ��д��־������
 * 4��������ļ����ݻָ�Ҫԭ���ģ�������ҪredoInsert
 * 5��redo��������Ҫ�ж��ļ����ȵ����λ��
 */
void LobOperTestCase::insertLobAndLogReverse() {
	//��backupFile
	File *lobFile = m_lobStorage->getBlobFile();
	backupLobFile(lobFile, LOB_BACKUP);

	//backup Ŀ¼�ļ�
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

	// �ر����ݿ��ٴ�
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

	//��redoInsert

	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);
	}
	m_db->getTxnlog()->endScan(logHdl);
	m_db->getSessionManager()->freeSession(session);
	//Ȼ����insertLob,���Լ�������
	insertBig(true);
	insertBig(false);
}

/**
 * Ϊ������־˳���ǰ���insert˳��,�����������һ��insert
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
 * Ϊ������־˳���ǰ���insert˳��,�����������һ��insert
 */
void LobOperTestCase::doInsert2( ) {
	doInsert1();
}


/************************************************************************/
/* ���濪ʼ�ǲ��Դ���ʵ��                                               */
/************************************************************************/

/**
 * ����insert�����
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::testInsertBig(bool isCompress) {
	insertBig(isCompress);
}

/**
 * ����read�����
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::testReadBig(bool isCompress) {
	clearLobs();

	insertBig(isCompress);
	bool isRight;
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testReadBig", m_conn);
	//����MemoryContext
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
 * ����insert�ĸ�����󣬷ֱ�Ϊ2ҳ��1ҳ��3ҳ��4ҳ
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::insertBig(bool isCompress) {
  insertTwoPages(isCompress);
  insertOnePage(isCompress);
  insertTotalPage(isCompress);
  insertBoundaryPage(isCompress);
}

/**
 * ����insertΪ2ҳһ�������
 *
 * @param isCompress �Ƿ�ѹ��
 * @return �����ID
 */
LobId LobOperTestCase::insertTwoPages(bool isCompress) {
    uint lobLen;
	uint size;
	bool isRight;

	//�Ȳ�����ҳ����
	lobLen = Limits::PAGE_SIZE * 2 - 512;
	byte *lob = createLob(lobLen, isCompress);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::insertTwoPages", conn);
	LobId lid =  m_lobStorage->insert(session, lob, lobLen, isCompress);
	lobIds.push_back(lid);

	//����MemoryContext
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
 * ����insertΪ1ҳһ�������
 *
 * @param isCompress �Ƿ�ѹ��
 * @return �����ID
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


	//����MemoryContext
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
 * ����insertΪ3ҳһ�������
 *
 * @param isCompress �Ƿ�ѹ��
 * @return �����ID
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

	//����MemoryContext
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
 * ����insertΪ4ҳһ����������иպõ�4ҳֻ��һ���ֽ�
 *
 * @param isCompress �Ƿ�ѹ��
 * @return �����ID
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
	//����MemoryContext
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
 * ���Դ��ʹ����ĸ���
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::testBigUpdate(bool isCompress) {
	//�Ȳ����ĸ������
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

	//����MemoryContext
	MemoryContext *mc = session->getMemoryContext();

	//���и��£�ԭ��3ҳ�����ڲ���
	uint newLen = getLobSize(Limits::PAGE_SIZE * 2, isCompress) + 2;
	byte *newLob = createLob(newLen);
	m_lobStorage->update(session, lid, newLob, newLen, isCompress);
	//��֤�Ƿ�һ��
	byte *getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newLob, getLob, newLen));
	System::virtualFree(newLob);

	//���и��£���������󣬴���������ļ�ĩβ
	uint newBigerAtTailLen = getLobSize(Limits::PAGE_SIZE * 5, isCompress) - 1;
	byte *newBigerAtTailLob = createLob(newBigerAtTailLen);
	m_lobStorage->update(session, lid, newBigerAtTailLob, newBigerAtTailLen, isCompress);
	//��֤�Ƿ�һ��
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newBigerAtTailLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAtTailLob, getLob, size));
	System::virtualFree(newBigerAtTailLob);

	//���и��£���ҳ����С
	uint nextLen = getLobSize(Limits::PAGE_SIZE * 2, isCompress) - 1;
	byte *nextLob = createLob(nextLen);
	m_lobStorage->update(session, lid, nextLob, nextLen, isCompress);
	//��֤�Ƿ�һ��
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == nextLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(nextLob, getLob, nextLen));
	System::virtualFree(nextLob);

	//���и��£���������󣬴�������м䣬����Ҫ�жϺ����Ƿ����㹻�Ŀ��пռ䣬�ռ��㹻
	size_t elementNum = lobIds.size();
	insertBig(isCompress);
	for(iter = lobIds.begin() + elementNum; iter !=lobIds.end() - 1; iter++){
		m_lobStorage->del(session, *iter);
	}
	uint newBigerAndHaveFreeSpaceLen  = getLobSize(Limits::PAGE_SIZE * 8, isCompress);
	byte *newBigerAndHaveFreeSpaceLob = createLob(newBigerAndHaveFreeSpaceLen);
	m_lobStorage->update(session, lid, newBigerAndHaveFreeSpaceLob, newBigerAndHaveFreeSpaceLen, isCompress);
	//��֤�Ƿ�һ��
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newBigerAndHaveFreeSpaceLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAndHaveFreeSpaceLob, getLob, size));
	System::virtualFree(newBigerAndHaveFreeSpaceLob);

	//���и��£���������󣬴�������м䣬����Ҫ�жϺ����Ƿ����㹻�Ŀ��пռ䣬�ռ��㹻���������µĿ��п�
	uint newBigerAndHavaNewFreeBlkLen  = getLobSize(Limits::PAGE_SIZE * 9, isCompress);
	byte *newBigerAndHavaNewFreeBlkLob = createLob(newBigerAndHavaNewFreeBlkLen);
	m_lobStorage->update(session, lid, newBigerAndHavaNewFreeBlkLob, newBigerAndHavaNewFreeBlkLen, isCompress);
	//��֤�Ƿ�һ��
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newBigerAndHavaNewFreeBlkLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAndHavaNewFreeBlkLob, getLob, size));
	System::virtualFree(newBigerAndHavaNewFreeBlkLob);


	//���и��£�������󣬵������������пռ䲻��
	insertBoundaryPage(isCompress);
	insertTwoPages(isCompress);
	insertOnePage(isCompress);
	insertTotalPage(isCompress);
	elementNum = lobIds.size();
	//��ɾ������
	m_lobStorage->del(session, lobIds[elementNum-2]);
	m_lobStorage->del(session, lobIds[elementNum-3]);
	uint newBigerAndHavaNoFreeBlkLen  = getLobSize(Limits::PAGE_SIZE * 8, isCompress);
	byte *newBigerAndHavaNoFreeBlkLob = createLob(newBigerAndHavaNoFreeBlkLen);
	m_lobStorage->update(session,lobIds[elementNum-4], newBigerAndHavaNoFreeBlkLob, newBigerAndHavaNoFreeBlkLen, isCompress);
	//��֤�Ƿ�һ��
	getLob = m_lobStorage->get(session, mc, lobIds[elementNum-4], &size, false);
	CPPUNIT_ASSERT(size == newBigerAndHavaNoFreeBlkLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newBigerAndHavaNoFreeBlkLob, getLob, size));
	System::virtualFree(newBigerAndHavaNoFreeBlkLob);


	//���Ը��µ����һ�������
	insertOnePage(isCompress);
	insertTwoPages(isCompress);
	insertTotalPage(isCompress);
	elementNum = lobIds.size();
	m_lobStorage->del(session, lobIds[elementNum - 1]);
	uint newNearTailLen  = getLobSize(Limits::PAGE_SIZE * 6, isCompress);
	byte *newNearTailLob = createLob(newNearTailLen);
	m_lobStorage->update(session,lobIds[elementNum - 2], newNearTailLob, newNearTailLen, isCompress);
	//��֤�Ƿ�һ��
	getLob = m_lobStorage->get(session, mc, lobIds[elementNum-2], &size, false);
	CPPUNIT_ASSERT(size == newNearTailLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newNearTailLob, getLob, size));
	System::virtualFree(newNearTailLob);


	clearLobs();

	//2008.11.5 ����
	//����ֻ��һҳ���
	LobId onePageLid = insertOnePage(isCompress);
	uint onePageLen = Limits::PAGE_SIZE - 50;
	byte *onePageLob = createLob(onePageLen);
	LobId newLid = m_lobStorage->update(session, onePageLid, onePageLob, onePageLen, isCompress);
	getLob = m_lobStorage->get(session, mc, newLid, &size, false);
	CPPUNIT_ASSERT(size == onePageLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(onePageLob, getLob, size));
	System::virtualFree(onePageLob);

    //Ϊ�˸���622,623��BigLop.cpp��
	//��4ҳ���3ҳ
	LobId newTryId = insertBoundaryPage(isCompress);
	uint newTryLen = Limits::PAGE_SIZE * 2;
	byte *newTryLob = createLob(newTryLen);
	m_lobStorage->update(session, newTryId, newTryLob, newTryLen, isCompress);
	getLob = m_lobStorage->get(session, mc, newTryId, &size, false);
	CPPUNIT_ASSERT(size == newTryLen);
	CPPUNIT_ASSERT(isRight = lobIsSame(newTryLob, getLob, size));
	System::virtualFree(newTryLob);
	m_db->getSessionManager()->freeSession(session);

	//���Ա䳤ʱ����Ҫ��չ�ļ������
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
 * ����ɾ�����ʹ����
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::testDelBig(bool isCompress) {
	//׼��һЩlob
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
		//��֤Ŀ¼��Ϳ���ҵ��IsFree��True
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
	//Ȼ���ڲ������󣬽�����Ŀ¼��
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
 * �رմ����洢
 */
void LobOperTestCase::closeLobStorage() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::closeLobStorage", conn);
	EXCPT_OPER(m_lobStorage->close(session, true));
	//EXCPT_OPER(m_lobStorage->close(session, true)); // �ظ��ر�
	delete m_lobStorage;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	m_lobStorage = NULL;
}

/**
 * ����redoCreate
 *
 * @param isCompress �Ƿ�ѹ��
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

	// �ļ����Ȳ���
	closeLobStorage();
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	// EXCPT_OPER(LobStorage::redoCreate(m_db, session, LOB_PATH, 0 ));
	EXCPT_OPER(LobStorage::redoCreate(m_db, session, m_tableDef, LOB_PATH, 1));
	m_db->getSessionManager()->freeSession(session);

	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoCreate", conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	//�������ݣ�����redocreate
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
 * ����redoInsert
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::testRedoInsert(bool isCompress) {
	//����һЩ����������һЩ����
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

		if (i == maxSlot - 1) { // �ڶ�ҳ����ǰ
			restoreLobIndexPage(session, 1, indexBuf); // ��һҳ���һ����¼���޸ľ�û����Ч
			backupLobIndexPage(session, 2, indexBuf); // ׼���ڶ�ҳ��
		}
	}
	restoreLobIndexPage(session, 2, indexBuf); // �ظ������һ������ǰ
	restoreLobIndexPage(session, 0, indexHeaderBuf);

	// �ر��ٴ�
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

	// �����־
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		++j;
		u64 lsn = logHdl->curLsn();

		//Ŀ¼�ļ�ûˢ��ȥ
		if (j == maxSlot) {
			li1 = getLobIndexPageLSN(session, 1);
			li2  = getLobIndexPageLSN(session, 0);
		}
		if (j == maxSlot + 1) {
			li3 = getLobIndexPageLSN(session, 2);
		}
		//����parseInsertLog����
		const LogEntry *entry = logHdl->logEntry();
		byte *logData = entry->m_data;
		size_t orgLen;
		m_lobStorage->parseInsertLog(entry, *((u64 *)logData), &orgLen, session->getMemoryContext());
		m_lobStorage->redoBLInsert(session, lsn, logHdl->logEntry()->m_data, (uint)logHdl->logEntry()->m_size);

		// ��֤LOB����
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

	// ѹ����¼����
	len = getLobSize(Limits::PAGE_SIZE * 3, true);
	lob = createLob(len, true);
	lid1 = m_lobStorage->insert(session, lob, len, true);
	System::virtualFree(lob);
	lob = NULL;

	// ��ȡ��¼����
	lob = m_lobStorage->get(session, session->getMemoryContext(), lid1, &size, false);

	bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, 1, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
	liFilePage = (LIFilePageInfo *)bphdl->getPage();
	freeSlotNum = liFilePage->m_freeSlotNum;
	liFilePage->m_freeSlotNum = 1;
	session->markDirty(bphdl);
	session->releasePage(&bphdl);

	// ��ѹ����¼����
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

	// ��¼����
	uint updlen = getLobSize(Limits::PAGE_SIZE * 4, true);
	byte *updlob = createLob(updlen, true);
	m_lobStorage->update(session, lid1, updlob, updlen, true);

	// ��¼ɾ��
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

	// ����
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
		//����parseInsertLog����
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
 * ����redoDel
 *
 * @param isCompress �Ƿ�ѹ��
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

	//��׼������
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
			li2 = getLobIndexPageLSN(session, 1);//һҳĿ¼��Ŀ�Ѿ�����
			CPPUNIT_ASSERT(li1 != li2);
		}

		if (i == maxSlot - 1) { // �ڶ�ҳ����ǰ
			restoreLobIndexPage(session, 1, indexBuf); // ��һҳ���һ����¼���޸ľ�û����Ч
			li2 = getLobIndexPageLSN(session, 1);
			CPPUNIT_ASSERT(li1 == li2);

			backupLobIndexPage(session, 2, indexBuf); // ׼���ڶ�ҳ��
			li3 = getLobIndexPageLSN(session, 2);
		}
		if (i == maxSlot) {
			li5 = getLobIndexPageLSN(session, 2);

		}
	}
	li4 = getLobIndexPageLSN(session, 2);
	CPPUNIT_ASSERT(li4 != li3);
	restoreLobIndexPage(session, 2, indexBuf); // �ظ������һ������ǰ
	li4 = getLobIndexPageLSN(session, 2);
	CPPUNIT_ASSERT(li4 == li3);

	// �ر��ٴ�
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

	// �����־
	logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoDel", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		//����parseInsertLog����
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

	//�Ȳ���һЩ������׼��, ��������ҳ��
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

	// �ر����ݿ��ٴ�
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

	// �ͷŻỰ
	m_db->getSessionManager()->freeSession(session);	
}


/**
 * ����redoUpdate
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::testRedoUpdate(bool isCompress) {
	clearLobs();

	u64 li1, li2, l1, l2, l3, l4;
	byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFirstBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *lobFreeBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);
	byte *indexBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE);

	//�Ȳ���һЩ������׼��, ��������ҳ��
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::testRedoUpdate", m_conn);
	LobId lid = insertTwoPages(isCompress);
	l1 = getLobPageLSN(session, 1);
	backupLobPage(session, 1, lobBuf);

	//------������ҳ��������-------------------------------------/
	byte *lob =createLob(Limits::PAGE_SIZE * 2 - 512);
	m_lobStorage->update(session, lid, lob, Limits::PAGE_SIZE * 2 - 512, isCompress);
	l2 = getLobPageLSN(session, 1);
	restoreLobPage(session, 1, lobBuf);
	l3 = getLobPageLSN(session, 1);
	CPPUNIT_ASSERT(l1 == l3);
	m_db->getSessionManager()->freeSession(session);

	// �ر����ݿ��ٴ�
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

	// �ͷŻỰ
	m_db->getSessionManager()->freeSession(session);

	// ����������������
	// 1���䳤��a������ռ乻 b������ռ䲻�����ƶ���ĩβ
	// 2����̣������µĿ��п�

	//--------------���������󣬵����ļ�ĩβ��--------------------------
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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

	//--------------���������С��-----------------------------------------
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


	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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

	//��������󣺵������п��пռ䣬���ҿ��пռ乻----------------------
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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


	//��������󣺵������п��пռ䣬���ҿ��пռ䲻��(�������µ�pid +pageNum > fileTail)---------------------
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


	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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


	//��������󣺵������п��пռ䣬���ҿ��пռ䲻��---------------------
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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
 * ���Ը�����־
 *
 * @param isCompress �Ƿ�ѹ��
 */
void LobOperTestCase::testUpdateLog(bool isCompress) {
	clearLobs();

	Session *session;
	u64 li1, li2, l1, l2, l3;
	//�Ȳ���һЩ������׼��
	// ��������ҳ��
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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


	//����������������
	//1���䳤��a������ռ乻 b������ռ䲻�����ƶ���ĩβ
	//2����̣������µĿ��п�


	//--------------���������󣬵����ļ�ĩβ��------------------------
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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


	//--------------���������С��-----------------------------------------
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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

	//��������󣺵������п��пռ䣬���ҿ��пռ乻----------------------
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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


	//��������󣺵������п��пռ䣬���ҿ��пռ䲻��(�������µ�pid +pageNum > fileTail)---------------------
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

	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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


	//��������󣺵������п��пռ䣬���ҿ��пռ䲻��---------------------
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


	// �ر����ݿ��ٴ�
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
	// ���ڼ��log
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
 * �õ�������ļ�һҳ��lsn
 *
 * @param session �Ự����
 * @param pageNum ҳ��
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
 * �õ�Ŀ¼�ļ�һҳ��lsn
 *
 * @param session �Ự����
 * @param pageNum ҳ��
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
 * ���ݴ�����ļ���һҳ
 *
 * @param session �Ự����
 * @param pageNum ҳ��
 * @param pageBuffer ���ݵĿռ�
 */
void LobOperTestCase::backupLobPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getBlobFile(), PAGE_LOB_HEAP, pageNum, Shared, m_lobStorage->getLLobDatStats(), NULL);
	memcpy(pageBuffer, bphdl->getPage(), Limits::PAGE_SIZE);
	session->releasePage(&bphdl);
}

/**
 * �ָ�������ļ���һҳ
 *
 * @param session �Ự����
 * @param pageNum ҳ��
 * @param pageBuffer ��Ҫ�ָ�������ҳ
 */
void LobOperTestCase::restoreLobPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session,  m_lobStorage->getBlobFile(), PAGE_LOB_HEAP, pageNum, Exclusived, m_lobStorage->getLLobDatStats(), NULL);
	memcpy(bphdl->getPage(), pageBuffer, Limits::PAGE_SIZE);
	session->markDirty(bphdl);
	session->releasePage(&bphdl);
}

/**
 * ����Ŀ¼�ļ���һҳ
 *
 * @param session �Ự����
 * @param pageNum ҳ��
 * @param pageBuffer ���ݵĿռ�
 */
void LobOperTestCase::backupLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session, m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, pageNum, Shared, m_lobStorage->getLLobDirStats(), NULL);
	memcpy(pageBuffer, bphdl->getPage(), Limits::PAGE_SIZE);
	session->releasePage(&bphdl);
}


/**
 * �ָ�Ŀ¼�ļ���һҳ
 *
 * @param session �Ự����
 * @param pageNum ҳ��
 * @param pageBuffer ��Ҫ�ָ�������ҳ
 */
void LobOperTestCase::restoreLobIndexPage(Session *session, u64 pageNum, byte *pageBuffer) {
	BufferPageHandle *bphdl = GET_PAGE(session,  m_lobStorage->getIndexFile(), PAGE_LOB_INDEX, pageNum, Exclusived, m_lobStorage->getLLobDirStats(), NULL);
	memcpy(bphdl->getPage(), pageBuffer, Limits::PAGE_SIZE);
	session->markDirty(bphdl);
	session->releasePage(&bphdl);
}

/**
 * �Ƚ�����������Ƿ�һ��
 *
 * @param src ����һ�����������
 * @param dest ����һ�����������
 * @param len ����
 * @return �Ƿ�һ��
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
 * ����һ�����������LOB������Ϊ2ҳ
 *
 * @param len ����
 * @return ���������
 */
byte* LobOperTestCase::createLob(uint *len) {
	//������2ҳ��С��Lob
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
 * ����ָ�����ȵ�����ַ���
 *
 * @return �ַ�����ʹ��new�����ڴ�
 */
char * LobOperTestCase::randomString(uint size) {
	char *s = (char *)System::virtualAlloc(size);
	for (size_t i = 0; i < size; i++)
		s[i] = (char )('A' + System::random() % 10);
	return s;
}

/**
* ����ָ�����ȵ�α����ַ���
*
* @return �ַ�����ʹ��new�����ڴ�
*/
char* LobOperTestCase::pesuRandomString(uint size) {
	char *s = (char *)System::virtualAlloc(size);
	for (size_t i = 0; i < size; i++)
		s[i] = (char )('A' + (size + i) % 10);
	return s;
}

/**
 * ����ָ�����ȹ̶����ȵ�LOB
 *
 * @param len ����
 * @return ���������
 */
byte* LobOperTestCase::createLob(uint len, bool useRandom) {
	//���������ڴ�ҳ��
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
 * �õ�С��256���漴��
 *
 */
u8 LobOperTestCase::getRandom() {
	return 0;
}


/**
 * ���ݿ�ĳ��ȵõ�����󳤶�
 *
 * @param size �鳤��
 * @param isCompress �Ƿ�ѹ��
 * @return ����󳤶�
 */
uint LobOperTestCase::getLobSize(uint size, bool isCompress) {
	//����С��һҳ
	if (size <=  Limits::PAGE_SIZE)
		return size - BigLobStorage::OFFSET_BLOCK_FIRST;
	else {
		uint pageNum = (size + Limits:: PAGE_SIZE - 1)/ Limits:: PAGE_SIZE;
		return size - BigLobStorage::OFFSET_BLOCK_FIRST  -  (pageNum - 1) * BigLobStorage::OFFSET_BLOCK_OTHER;
	}
}

/**
 * �Ƚϴ�����Ƿ�һ�������Լ��Ƚ�
 *
 * @return �Ƿ�һ��
 */
bool LobOperTestCase::compareLobs() {
	bool isRight = true;
	bool re = true;
	Session *session = m_db->getSessionManager()->allocSession("LobOperTestCase::createLobStorage", m_conn);
	//����MemoryContext
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
 * �����ļ�����ʵ��
 *
 * @param file  �����ļ��ľ��
 * @param backupName  ԭ�ļ�����
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
 * �����ļ�
 *
 * @param origName  �����ļ�����
 * @param backupName  ԭ�ļ�����
 */
void backupLobFile(const char *origName, const char *backupName) {
	u64 errCode = 0;

	File orig(origName);
	orig.open(true);
	backupLobFile(&orig, backupName);
	orig.close();
}

/**
 * �ָ������ļ�
 *
 * @param backupFile  �����ļ�
 * @param origFile  ԭ�ļ�
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
 * ����insert,update,read�Լ�delС�ʹ������Ҫ���ǲ����Ƿ���MMS��
 *
 * @param isCompress  �Ƿ�ѹ��
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

	//�ڶ�һ�Σ����ʱ����MMS��
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == lobLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, lob, size));
	System::virtualFree(lob);
	lob = NULL;

	// ����
	uint newLen = Limits::MAX_REC_SIZE - 7;
	byte *newLob = createLob(newLen);
	LobId lid2 = m_lobStorage->update(session, lid, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid2 == lid);
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	System::virtualFree(newLob);

	// ���del
	m_lobStorage->del(session, lid);
	getLob = m_lobStorage->get(session, mc, lid, &size, false);
	CPPUNIT_ASSERT(getLob == NULL);

	//��DRS���и���
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
 * ����С�ʹ������³ɴ��ʹ����
 *
 * @param isCompress  �Ƿ�ѹ��
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

	//�ٱ�С�����Է��ڴ��ʹ�����ļ���
	newLen = Limits::MAX_REC_SIZE;
	newLob = createLob(newLen);
	LobId lid3 = m_lobStorage->update(session, lid2, newLob, newLen, isCompress);
	CPPUNIT_ASSERT(lid3 == lid2);
	getLob = m_lobStorage->get(session, mc, lid3, &size, false);
	CPPUNIT_ASSERT(size == newLen);
	CPPUNIT_ASSERT(lobIsSame(getLob, newLob, size));
	System::virtualFree(newLob);


	//������mms�е�update
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

	//�ٱ�С�����Է��ڴ��ʹ�����ļ���
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
 * ����redoUpdateС�ʹ����,�����С�ʹ����û��File�����
 * ֻ��ͨ��backupFile -->restoreFile��ʽ������redo
 *
 * @param isCompress  �Ƿ�ѹ��
 */
void LobOperTestCase::testSLRedoInsert(bool isCompress) {

	//�Ȳ���5��С�ʹ����
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

		//����backupFile
		if (i == 1) {
			File *file = m_lobStorage->m_slob->m_heap->getHeapFile();
			backupLobFile(file, LOB_SMALL);
		}
	}

	// �ر��ٴ�
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	//����restoreFile
	restoreLobFile(LOB_SMALL,  LOBSMALL_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoInsert", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	int count = 0;

	// �����־
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoInsert", m_conn);
	while (m_db->getTxnlog()->getNext(logHdl)) {
		u64 lsn = logHdl->curLsn();
		if (!isCompress && count++ < 1) {
			//����parseInsertLog����
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
* ����redoUpdateС�ʹ����,�����ǲ���mms��ص�
*
* @param isCompress  �Ƿ�ѹ��
*/
void LobOperTestCase::testSLRedoUpdateMMS(bool isCompress) {
	//�Ȳ���3�������
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
	//Ȼ�������mms��
	MemoryContext *mc = session->getMemoryContext();
	for (uint i = 0; i < 3; i++) {
		m_lobStorage->get(session, mc, lids[i], &size);
	}

	//��ʼ���²���
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

	// �ر��ٴ�
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

	// �����־
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdateMMS", m_conn);
	//����insert��־
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
 * ����redoUpdateС�ʹ����,�����ǲ��Զ���
 *
 * @param isCompress  �Ƿ�ѹ��
 */
void LobOperTestCase::testSLRedoUpdate(bool isCompress) {
	//�Ȳ���5��С�ʹ����
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


	//���и���
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

	// �ر��ٴ�
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	//����restoreFile
	restoreLobFile(LOB_SMALL, LOBSMALL_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdate", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// �����־
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoUpdate", m_conn);
	//����insert��־
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
 * ����delС�ʹ����
 *
 * @param isCompress  �Ƿ�ѹ��
 */
void LobOperTestCase::testSLRedoDel(bool isCompress) {

	//�Ȳ���5��С�ʹ����
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

	// �ر��ٴ�
	m_lobStorage->close(session, true);
	delete m_lobStorage;
	m_lobStorage = NULL;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(m_conn);
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;
	//����restoreFile
	restoreLobFile(LOB_SMALL,  LOBSMALL_PATH);

	EXCPT_OPER(m_db = Database::open(&m_cfg, 1, -1));
	m_conn = m_db->getConnection(true);
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoDel", m_conn);
	EXCPT_OPER(m_lobStorage = LobStorage::open(m_db, session, m_tableDef, LOB_PATH, true));
	m_db->getSessionManager()->freeSession(session);

	// �����־
	LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
	uint j = 0;
	session = m_db->getSessionManager()->allocSession("LobOperTestCase::testSLRedoDel", m_conn);
	//����insert��־
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

	//get�ò���
	//����MemoryContext
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
 * ���Եõ��ļ����
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
 * ����redoInsertʱ����Ҫ��չ������ļ�
 * ���Բ��裺
 * 1���Ȳ��Բ����������պõ�fileLen����
 * 2��Ȼ�����һ�������A
 * 3���ָ�������ļ���A֮ǰ
 * 4�����ָ�����
 */
void LobOperTestCase::insertRedoAndExtendFile() {
	for(uint i = 0 ; i < 1024 / 2 ; i++) {
		insertTwoPages(false);
	}
	Buffer *buffer = m_db->getPageBuffer();
	buffer->flushAll();

	//��backupFile
	File *lobFile = m_lobStorage->getBlobFile();
	backupLobFile(lobFile, LOB_BACKUP);

	//backup Ŀ¼�ļ�
	File *indexFile = m_lobStorage->getIndexFile();
	backupLobFile(indexFile, LOBINDEX_BACKUP);

	insertTwoPages(false);

	// �ر����ݿ��ٴ�
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
	//Ȼ����insertLob,���Լ�������
	insertBig(true);
	insertBig(false);
}


/**
 * Ϊdefrag��׼����insert7�����Ȼ��ɾ������3��
 *
 * @param isCompress  �Ƿ�ѹ��
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

	//ɾ��һЩLOB
	m_lobStorage->del(session, lobIds[1]);
	m_lobStorage->del(session, lobIds[2]);
	size_t num = lobIds.size();

	m_lobStorage->del(session, lobIds[num - 3]);

	m_db->getSessionManager()->freeSession(session);
}

/**
 * insert һ�������
 *
 * @param session  �Ự����
 * @param size  ����󳤶�
 * @param isCompress  �Ƿ�ѹ��
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
/*                     ���Զ��̵߳Ĺ�����								*/
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

