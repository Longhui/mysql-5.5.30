/**
* 测试索引基类
*
* @author 苏斌(bsu@corp.netease.com, naturally@163.org)
*/

#include <cppunit/config/SourcePrefix.h>
#include "btree/TestIndex.h"
#include "Test.h"
#include "btree/Index.h"
#include "btree/IndexBPTree.h"
#include "btree/IndexBPTreesManager.h"
#include "btree/IndexKey.h"
#include "api/Table.h"
#include "misc/Buffer.h"
#include "misc/Global.h"
#include "api/Database.h"
#include "heap/Heap.h"
#include "util/File.h"
#include "misc/RecordHelper.h"
#include "misc/Session.h"
#include "misc/Trace.h"
#include <stdlib.h>
#include <iostream>

using namespace std;
using namespace ntse;
#define HEAP_NAME	"testIndex.ntd"		/** 堆名字 */
#define TBLDEF_NAME "testIndex.nttd"    /** tabledef的定义文件*/
#define DATA_SCALE	5000				/** 堆和索引数据规模 */
#define DROP_DATA_SCALE 100				/** 删除索引用例的数据规模 */

#define FAKE_LONG_CHAR234 "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" \
	"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
#define LONG_KEY_LENGTH	250					/** 长键值属性的最大长度 */

/**
 * 用于在初始化测试环境之前清理所有RSx类型的临时文件，保证测试程序可以完全自动运行
 */
void clearRSFile(Config *cfg) {
	for (int i = 0; i < 1000; i++) {
		char rspath[255];
		sprintf(rspath, "%s/RS%d", cfg->m_tmpdir, i);
		File file(rspath);
		bool isExist;
		file.isExist(&isExist);
		if (isExist)
			file.remove();
	}
}


const char* IndexTestCase::getName() {
	return "Index test";
}

const char* IndexTestCase::getDescription() {
	return "Test Index operations(Create/Open/Drop/RedoCreate/CreateIndex/DropIndex/truncateIndex).";
}

bool IndexTestCase::isBig() {
	return false;
}

void IndexTestCase::setUp() {
	m_tblDef = NULL;
	vs.idx = true;
}

void IndexTestCase::tearDown() {
	vs.idx = false;
}


/**
 * 测试创建索引文件和打开索引文件接口和丢弃索引接口
 * 用例1：创建一个索引文件，验证文件内容的正确性，然后删除索引文件
 * 用例2：创建相同的索引文件保证会抛异常不会有重复
 * 用例3：创建多个索引文件，打开文件关闭文件最后删除
 */
void IndexTestCase::testCreateAndOpenAndDrop() {
	init();
	// 创建一个索引文件，验证索引文件内容正确
	try {
		DBObjStats objstats(DBO_Indice);
		create(m_path, m_tblDef);
		if (!isEssentialOnly()) {
			// 读取索引文件页面内容判断内容正确
			File file(m_path);
			file.open(false);
			PageHandle *handle = GET_PAGE(m_session, &file, PAGE_INDEX, 0, Exclusived, &objstats, NULL);
			IndexHeaderPage *headerPage = (IndexHeaderPage*)handle->getPage();;
			CPPUNIT_ASSERT(headerPage->m_curFreeBitMap == IndicePageManager::HEADER_PAGE_NUM * INDEX_PAGE_SIZE + sizeof(Page));
			CPPUNIT_ASSERT(headerPage->m_minFreeBitMap == IndicePageManager::HEADER_PAGE_NUM * INDEX_PAGE_SIZE + sizeof(Page));
			CPPUNIT_ASSERT(headerPage->m_indexNum == 0);
			CPPUNIT_ASSERT(headerPage->m_indexUniqueId == 1);
			CPPUNIT_ASSERT(headerPage->m_dataOffset == INDEX_PAGE_SIZE * (IndicePageManager::BITMAP_PAGE_NUM + IndicePageManager::HEADER_PAGE_NUM));
			m_session->releasePage(&handle);
			handle = (GET_PAGE(m_session, &file, PAGE_INDEX, 1, Exclusived, &objstats, NULL));
			IndexFileBitMapPage *bmPage = (IndexFileBitMapPage*)handle->getPage();
			CPPUNIT_ASSERT(*((byte*)bmPage + sizeof(IndexFileBitMapPage)) == 0x00);
			m_session->releasePage(&handle);
			file.close();
		}
		m_index = open(m_path, m_tblDef);
		CPPUNIT_ASSERT(m_index->getDataLength() == IndicePageManager::NON_DATA_PAGE_NUM * INDEX_PAGE_SIZE);
		m_index->close(m_session, false);
		delete m_index;
		drop(m_path);
		m_index = NULL;
	} catch (NtseException) {
		CPPUNIT_ASSERT(false);
	}

	// 丢弃不存在的文件
	try {
		drop("nonexist.nti");
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	// 创建相同的文件
	try {
		create(m_path, m_tblDef);
		create(m_path, m_tblDef);
		CPPUNIT_ASSERT(false);
	} catch (NtseException &e) {
		CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_FILE_EXIST);
		EXCPT_OPER(drop(m_path));
		m_index = NULL;
	}

	if (!isEssentialOnly()) {
		// 随机创建打开多个索引文件，最后统一删除
		try {
			create("test1.nti", m_tblDef);
			DrsIndice *index1 = open("test1.nti", m_tblDef);

			create("test2.nti", m_tblDef);
			create("test3.nti", m_tblDef);
			create("test4.nti", m_tblDef);

			DrsIndice *index2 = open("test2.nti", m_tblDef);
			index1->close(m_session, false);
			DrsIndice *index3 = open("test3.nti", m_tblDef);
			DrsIndice *index4 = open("test4.nti", m_tblDef);
			index2->close(m_session, false);
			index3->close(m_session, false);
			index4->close(m_session, false);

			EXCPT_OPER(drop("test1.nti"));
			EXCPT_OPER(drop("test2.nti"));
			EXCPT_OPER(drop("test3.nti"));
			EXCPT_OPER(drop("test4.nti"));

			delete index1;
			delete index2;
			delete index3;
			delete index4;
		} catch (NtseException &e) {
			cout << e.getMessage() << endl;
			CPPUNIT_ASSERT(false);
		}
	}

	clear();
}



/**
 * 测试丢弃索引文件操作
 * case1: 丢弃不存在的索引文件
 * case2: 丢弃创建好但是未打开始用的索引文件
 * case3: 丢弃创建好并且打开使用但是没有执行关闭的索引文件
 */
void IndexTestCase::testDrop() {
	init();

	// 丢弃不存在的文件
	try {
		drop("nonexist.nti");
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	// 丢弃创建好未使用的文件
	try {
		create(m_path, m_tblDef);
		drop(m_path);
		m_index = NULL;
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	// 丢弃创建好使用并关闭的文件
	try {
		create(m_path, m_tblDef);
		m_index = open(m_path, m_tblDef);
		m_index->close(m_session, false);
		delete m_index;
		drop(m_path);
		m_index = NULL;
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	clear();
}


/**
 * 测试重做创建索引文件操作
 * @case1: 创建索引文件成功之后，执行重新创建操作
 * @case2: 创建索引文件失败之后，执行重新创建操作
 */
void IndexTestCase::testRedoCreate() {
	init();
	
	// 创建成功的情况下，执行redo
	try {
		create(m_path, m_tblDef);
		redoCreate(m_path, m_tblDef);
		EXCPT_OPER(drop(m_path));
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	// 创建不成功的情况下执行redo
	try {
		redoCreate(m_path, m_tblDef);
		EXCPT_OPER(drop(m_path));
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	clear();
}


/**
 * 测试创建索引操作
 * @case1: 测试创建两个小索引操作，索引不存在唯一性冲突
 * @case2: 测试创建一个大堆的两个索引，不存在唯一性冲突，为提高覆盖率
 * @case2: 测试创建一个大堆的两个索引，不存在唯一性冲突，为提高覆盖率
 * @case3: 测试创建两个小索引，索引都存在唯一性冲突，保证索引创建不成功
 * @case4: 测试创建空索引操作，分别传入NULL的堆指针以及传入一个空的堆，保证索引创建成功，但是索引是空的
 *			!!同时case3还测试flush接口
 */
void IndexTestCase::testCreateIndex() {
	init();

	DrsHeap *heap;
	TableDef *tableDef;

	// 创建有数据的两个小索引，不测试唯一性
	try {
		tableDef = createSmallTableDef();
		heap = createSmallHeap(tableDef);
		buildHeap(heap, tableDef, DATA_SCALE, false, false);
		CPPUNIT_ASSERT(heap != NULL);
		CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
		createIndex(m_path, heap, tableDef);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 2);
		CPPUNIT_ASSERT(m_index->getDataLength() > IndicePageManager::NON_DATA_PAGE_NUM * INDEX_PAGE_SIZE);
		m_index->close(m_session, true);
		delete m_index;
		m_index = NULL;
		drop(m_path);
		closeSmallHeap(heap);
		DrsHeap::drop(HEAP_NAME);

		delete tableDef;
		TableDef::drop(TBLDEF_NAME);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	// 创建大堆的两个大索引，提高覆盖率
	try {
		uint testScale = 757;
		tableDef = createBigTableDef();
		heap = createBigHeap(tableDef);
		buildBigHeap(heap, tableDef, false, testScale, 1);
		CPPUNIT_ASSERT(heap != NULL);
		CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
		createIndex(m_path, heap, tableDef);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		CPPUNIT_ASSERT(m_index->getDataLength() > IndicePageManager::NON_DATA_PAGE_NUM * INDEX_PAGE_SIZE);
		m_index->close(m_session, true);
		delete m_index;
		m_index = NULL;
		drop(m_path);
		closeSmallHeap(heap);
		DrsHeap::drop(HEAP_NAME);
		delete tableDef;
		TableDef::drop(TBLDEF_NAME);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	// 创建大堆的两个大索引，提高覆盖率
	try {
		uint testScale = 2000;
		tableDef = createBigTableDef();
		heap = createBigHeap(tableDef);
		buildBigHeap(heap, tableDef, false, testScale, 1);
		CPPUNIT_ASSERT(heap != NULL);
		CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
		createIndex(m_path, heap, tableDef);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		CPPUNIT_ASSERT(m_index->getDataLength() > IndicePageManager::NON_DATA_PAGE_NUM * INDEX_PAGE_SIZE);
		m_index->close(m_session, true);
		delete m_index;
		m_index = NULL;
		drop(m_path);
		closeSmallHeap(heap);
		DrsHeap::drop(HEAP_NAME);
		delete tableDef;
		TableDef::drop(TBLDEF_NAME);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	if (!isEssentialOnly()) {
		// 创建有数据的小索引，测试唯一性约束
		try {
			tableDef = createSmallTableDef();
			heap = createSmallHeap(tableDef);
			buildHeap(heap, tableDef, DATA_SCALE, false, true);
			CPPUNIT_ASSERT(heap != NULL);
			CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
			createIndex(m_path, heap, tableDef);
			CPPUNIT_ASSERT(m_index->getIndexNum() == 2);
			CPPUNIT_ASSERT(false);
		} catch (NtseException &e) {
			CPPUNIT_ASSERT(e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION);
			CPPUNIT_ASSERT(m_index->getIndexNum() == 0);
			m_index->close(m_session, true);
			delete m_index;
			m_index = NULL;
			drop(m_path);
			closeSmallHeap(heap);
			DrsHeap::drop(HEAP_NAME);
			delete tableDef;
			TableDef::drop(TBLDEF_NAME);
		}
	}

	// 创建一个空索引
	try {
		tableDef = createSmallTableDef();
		heap = createSmallHeap(tableDef);
		CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
		create(m_path, tableDef);
		m_index = open(m_path, tableDef);
		m_index->createIndexPhaseOne(m_session, tableDef->m_indice[0], tableDef, heap);
		m_index->createIndexPhaseTwo(tableDef->m_indice[0]);
		m_index->createIndexPhaseOne(m_session, tableDef->m_indice[1], tableDef, NULL);
		m_index->createIndexPhaseTwo(tableDef->m_indice[1]);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 2);

		File *file = ((DrsBPTreeIndice*)m_index)->getFileDesc();
		PageHandle *handle = GET_PAGE(m_session, file, PAGE_INDEX, IndicePageManager::NON_DATA_PAGE_NUM, Exclusived, m_index->getDBObjStats(), NULL);
		IndexPage *rootPage = (IndexPage*)handle->getPage();;
		CPPUNIT_ASSERT(rootPage->m_pageCount == 0);
		m_session->releasePage(&handle);

		m_index->flush(m_session);
		m_index->close(m_session, false);
		delete m_index;
		m_index = NULL;
		drop(m_path);
		closeSmallHeap(heap);
		DrsHeap::drop(HEAP_NAME);
		delete tableDef;
		TableDef::drop(TBLDEF_NAME);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	clear();
}


/**
 * 测试丢弃索引
 * @case1: 创建两个无数据的索引，执行丢弃操作
 * @case2: 创建两个有数据的索引，执行丢弃操作
 */
void IndexTestCase::testDropIndex() {
	init();

	DrsHeap *heap;
	TableDef *tableDef;
	// 创建两个无数据索引，执行丢弃
	try {
		tableDef = createSmallTableDef();
		heap = createSmallHeap(tableDef);
		CPPUNIT_ASSERT(tableDef->m_numIndice != 0);

		create(m_path, tableDef);
		m_index = open(m_path, tableDef);

		EXCPT_OPER(m_index->createIndexPhaseOne(m_session, tableDef->m_indice[0], tableDef, NULL));
		m_index->createIndexPhaseTwo(tableDef->m_indice[0]);

		EXCPT_OPER(m_index->createIndexPhaseOne(m_session, tableDef->m_indice[1], tableDef, NULL));
		m_index->createIndexPhaseTwo(tableDef->m_indice[1]);

		DrsIndex *index = m_index->getIndex(0);
		CPPUNIT_ASSERT(index != NULL);
		index = NULL;

		CPPUNIT_ASSERT(m_index->getIndexNum() == 2);
		m_index->dropPhaseOne(m_session, 0);
		m_index->dropPhaseTwo(m_session, 0);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);

		m_index->close(m_session, false);
		delete m_index;
		m_index = NULL;
		drop(m_path);
		closeSmallHeap(heap);
		DrsHeap::drop(HEAP_NAME);
		delete tableDef;
		TableDef::drop(TBLDEF_NAME);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	// 创建有数据的两个小索引，逐个丢弃
	try {
		tableDef = createSmallTableDef();
		heap = createSmallHeap(tableDef);
		buildHeap(heap, tableDef, DROP_DATA_SCALE, true, false);
		CPPUNIT_ASSERT(heap != NULL);
		CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
		createIndex(m_path, heap, tableDef);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 2);

		DrsIndex *index = NULL;
		index = m_index->getIndex(0);
		CPPUNIT_ASSERT(index != NULL);
		index = m_index->getIndex(1);
		CPPUNIT_ASSERT(index != NULL);
		index = NULL;

		m_index->dropPhaseOne(m_session, 0);
		m_index->dropPhaseTwo(m_session, 0);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 1);
		index = m_index->getIndex(0);
		CPPUNIT_ASSERT(index != NULL);

		m_index->dropPhaseOne(m_session, 0);
		m_index->dropPhaseTwo(m_session, 0);
		CPPUNIT_ASSERT(m_index->getIndexNum() == 0);

		m_index->close(m_session, true);
		delete m_index;
		m_index = NULL;

		CPPUNIT_ASSERT(checkBMPageFree(m_session, 1));

		drop(m_path);
		closeSmallHeap(heap);
		DrsHeap::drop(HEAP_NAME);
		delete tableDef;
		TableDef::drop(TBLDEF_NAME);
	} catch (NtseException &e) {
		cout << e.getMessage() << endl;
		CPPUNIT_ASSERT(false);
	}

	clear();
}


/**
 * 测试索引的getFiles接口
 * @case1: 创建两个索引之后，直接调用接口
 */
void IndexTestCase::testGetFiles() {
	init();

	TableDef *tableDef = createSmallTableDef();
	DrsHeap *heap = createSmallHeap(tableDef);
	buildHeap(heap, tableDef, 10, true, false);
	
	CPPUNIT_ASSERT(heap != NULL);
	CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
	createIndex(m_path, heap, tableDef);
	CPPUNIT_ASSERT(m_index->getIndexNum() == 2);

	File **files = new File*[5];
	for (uint i = 0; i < 5; i++)
		files[i] = NULL;
	PageType pageType;
	uint gotFiles = m_index->getFiles(files, &pageType, 5);
	CPPUNIT_ASSERT(gotFiles == 1);
	CPPUNIT_ASSERT(files[0] != NULL);
	CPPUNIT_ASSERT(pageType == PAGE_INDEX);

	m_index->close(m_session, true);
	delete m_index;
	m_index = NULL;
	drop(m_path);
	closeSmallHeap(heap);
	DrsHeap::drop(HEAP_NAME);
	delete tableDef;
	TableDef::drop(TBLDEF_NAME);

	delete[] files;

	clear();
}



void IndexTestCase::createIndex(char *path, DrsHeap *heap, const TableDef *tblDef) throw(NtseException) {
	create(path, tblDef);
	CPPUNIT_ASSERT(m_index == NULL);
	m_index = open(path, tblDef);
	CPPUNIT_ASSERT(m_index->getIndexNum() == 0);
	MemoryContext *memoryContext = m_session->getMemoryContext();
	try {
		SubRecord *key1 = IndexKey::allocSubRecord(memoryContext, tblDef->m_indice[0], KEY_COMPRESS);
		SubRecord *key2 = IndexKey::allocSubRecord(memoryContext, tblDef->m_indice[0], KEY_COMPRESS);
		SubRecord *pkey0 = IndexKey::allocSubRecord(memoryContext, tblDef->m_indice[0], KEY_PAD);
		DrsIndex *index;

		m_index->createIndexPhaseOne(m_session, tblDef->m_indice[0], tblDef, heap);
		m_index->createIndexPhaseTwo(tblDef->m_indice[0]);
		index = m_index->getIndex(0);
		if (!isEssentialOnly())
			CPPUNIT_ASSERT(((DrsBPTreeIndex*)index)->verify(m_session, key1, key2, pkey0, true));

		if (tblDef->m_numIndice > 1) {
			SubRecord *pkey1 = IndexKey::allocSubRecord(memoryContext, tblDef->m_indice[1], KEY_PAD);
			SubRecord *key3 = IndexKey::allocSubRecord(memoryContext, tblDef->m_indice[1], KEY_COMPRESS);
			SubRecord *key4 = IndexKey::allocSubRecord(memoryContext, tblDef->m_indice[1], KEY_COMPRESS);
			m_index->createIndexPhaseOne(m_session, tblDef->m_indice[1], tblDef, heap);
			m_index->createIndexPhaseTwo(tblDef->m_indice[1]);
			index = m_index->getIndex(1);
			if (!isEssentialOnly())
				CPPUNIT_ASSERT(((DrsBPTreeIndex*)index)->verify(m_session, key3, key4, pkey1, true));
		}

		CPPUNIT_ASSERT(m_index->getIndex(0)->getDBObjStats() != NULL);
	} catch (NtseException &e) {
		throw e;
	}
}


bool IndexTestCase::checkBMPageFree(Session *session, PageId pageId) {
	File file(m_path);
	file.open(false);
	DBObjStats objstats(DBO_Indice);
	PageHandle *handle = GET_PAGE(session, &file, PAGE_INDEX, pageId, Exclusived, &objstats, NULL);
	byte *page = (byte*)handle->getPage();
	byte *pageEnd = page + Limits::PAGE_SIZE;

	for (byte *start = page + sizeof(BufferPageHdr); start < pageEnd; start++)
		CPPUNIT_ASSERT(*start == 0x00);

	session->releasePage(&handle);
	m_db->getPageBuffer()->freePages(session, &file, true);
	file.close();

	return true;
}

TableDef* IndexTestCase::createSmallTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 50, false, false);
	builder->addColumn("BankAccount", CT_BIGINT, false)->addColumn("Balance", CT_INT, false);
	//builder->addIndex("PRIMARY", true, true, "UserId", NULL);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	builder->addIndex("PRIMARY", true, true, false, "UserId", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));

	delete builder;

	return tableDef;
}

DrsHeap* IndexTestCase::createSmallHeap(const TableDef *tableDef) {
	DrsHeap *heap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(heap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return heap;
}

void IndexTestCase::closeSmallHeap(DrsHeap *heap) {
	//NTSEOPER(m_smallHeap->close(true));
	EXCPT_OPER(heap->close(m_session, true));
	delete heap;
}


void IndexTestCase::buildHeap(DrsHeap *heap, const TableDef *tblDef, uint size, bool keepSequence, bool hasSame) {
	Record *record;

	if (hasSame) {
		// 简单构造若干个相同数据的表
		for (int i = 0; i < size; i++) {
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", i);
			//Record *rec = createRecord(0, number, name, number + ((u64)number << 32), (u32)(-1) - number);
			record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			//m_smallHeap->insert(NULL, rec, NULL);
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
			freeRecord(record);
		}
		// 插入一条相同记录
		int i = DATA_SCALE - 1;
		char name[50];
		RowId rid;
		sprintf(name, "Kenneth Tse Jr. %d \0", i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
		return;
	}

	if (keepSequence) {
		// 顺序创建
		for (int i = 0; i < size; i++) {
			char name[50];
			RowId rid;
			sprintf(name, "Kenneth Tse Jr. %d \0", i);
			//Record *rec = createRecord(0, number, name, number + ((u64)number << 32), (u32)(-1) - number);
			record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
			//m_smallHeap->insert(NULL, rec, NULL);
			EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
			freeRecord(record);
		}
		return;
	}

	// 随机创建
	char mark[DATA_SCALE];
	memset(mark, 0, size);
	for (int i = 0; i < size; i++) {
		char name[50];
		RowId rid;

		// 随机产生一个数字
		int num = rand() % size;
		int dir = rand() % 2;
		int loopCount = 0;
		while (true) {
			assert(loopCount <= 2 * size);
			if (mark[num] == 0) {
				mark[num] = 1;
				break;
			}

			if (dir == 0) {	// 正向
				++num;
				if (num >= size)
					num = 0;
			} else {	// 反向
				--num;
				if (num < 0)
					num = size - 1;
			}

			loopCount++;
		}

		sprintf(name, "Kenneth Tse Jr. %d \0", num);
		//Record *rec = createRecord(0, number, name, number + ((u64)number << 32), (u32)(-1) - number);
		record = createRecord(tblDef, 0, num, name, num + ((u64)i << 32), (u32)(-1) - num);
		//m_smallHeap->insert(NULL, rec, NULL);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		freeRecord(record);
	}
}


Record* IndexTestCase::createRecord(const TableDef *tableDef, u64 rowid, u64 userid, const char *username, u64 bankacc, u32 balance) {
	RecordBuilder rb(tableDef, rowid, REC_FIXLEN);
	rb.appendBigInt(userid);
	rb.appendChar(username);
	rb.appendBigInt(bankacc);
	rb.appendInt(balance);
	return rb.getRecord(tableDef->m_maxRecSize);
}



TableDef* IndexTestCase::getATableDef() {
	TableDefBuilder *builder = new TableDefBuilder(1, "space", "BlogCount");
	builder->addColumn("BlogID", CT_BIGINT, false)->addColumn("CommentCount", CT_INT);
	builder->addColumn("TrackbackCount", CT_SMALLINT)->addColumn("AccessCount", CT_INT);
	builder->addColumnS("Title", CT_CHAR, 50);
	builder->addColumn("UserID", CT_BIGINT);
	TableDef *tableDef = builder->getTableDef();
	delete builder;

	return tableDef;
}


void IndexTestCase::create(const char *path, const TableDef *tableDef) throw(NtseException) {
	DrsIndice::create(path, tableDef);
}

DrsIndice* IndexTestCase::open(const char *path, const TableDef *tableDef) throw(NtseException) {
	return DrsIndice::open(m_db, m_session, path, tableDef, NULL);
}


void IndexTestCase::drop(const char *path) throw(NtseException) {
	DrsIndice::drop(path);
}

void IndexTestCase::redoCreate(const char *path, const TableDef *tableDef) throw(NtseException) {
	DrsIndice::redoCreate(path, tableDef);
}

/**
 * 测试索引文件扩展功能正确性
 */
void IndexTestCase::testIndexFileExtend() {
	if (isEssentialOnly())
		return;

	init();

	const uint createPageTimes = 10;
	// 创建基本索引文件
	TableDef *tableDef = createSmallTableDef();
	DrsHeap *heap = createSmallHeap(tableDef);
	buildHeap(heap, tableDef, 10, true, false);
	
	tableDef->m_incrSize = 10;
	CPPUNIT_ASSERT(heap != NULL);
	CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
	createIndex(m_path, heap, tableDef);
	CPPUNIT_ASSERT(m_index->getIndexNum() == 2);

	u64 startlen;
	File *file = ((DrsBPTreeIndice*)m_index)->getFileDesc();
	file->getSize(&startlen);

	const uint pageSize = 1024;
	byte *buf = (byte*)System::virtualAlloc(pageSize);
	// 连续扩展索引文件到指定大小
	u32 extrapages = 0;
	u64 curLen = 0;
	for (uint i = 0; i < createPageTimes; i++, tableDef->m_incrSize++) {
		// 首先打开文件将各个索引的空闲链表置空
		PageHandle *handle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
		IndexHeaderPage *header = (IndexHeaderPage*)handle->getPage();
		header->m_freeListPageIds[0] = INVALID_PAGE_ID;
		header->m_freeListPageIds[1] = INVALID_PAGE_ID;
		CPPUNIT_ASSERT(header->m_indexNum == 2);
		m_session->markDirty(handle);
		m_session->releasePage(&handle);

		IndicePageManager *manager = ((DrsBPTreeIndice*)m_index)->getPagesManager();
		IndexLog *logger = ((DrsBPTreeIndice*)m_index)->getLogger();
		u64 pageId;
		PageHandle *newHandle = manager->allocPage(logger, m_session, 1, &pageId);
		m_session->releasePage(&newHandle);
		CPPUNIT_ASSERT(pageId < createPageTimes * IndicePageManager::PAGE_BLOCK_NUM + 1000);

		// 验证当前大小和预期的相等
		bool extended = extrapages < IndicePageManager::PAGE_BLOCK_NUM;
		u16 incrSize = Database::getBestIncrSize(tableDef, curLen);
		if (incrSize > IndicePageManager::PAGE_BLOCK_NUM && extrapages < IndicePageManager::PAGE_BLOCK_NUM)
			extrapages += incrSize > IndicePageManager::PAGE_BLOCK_NUM ? incrSize - IndicePageManager::PAGE_BLOCK_NUM : 0;
		else if (extrapages >= IndicePageManager::PAGE_BLOCK_NUM)
			extrapages -= IndicePageManager::PAGE_BLOCK_NUM;
		file->getSize(&curLen);
		assert(curLen == startlen + ((!extended) ? 0 : ((tableDef->m_incrSize > IndicePageManager::PAGE_BLOCK_NUM ? incrSize : IndicePageManager::PAGE_BLOCK_NUM) * INDEX_PAGE_SIZE)));
		startlen = curLen;
	}

	System::virtualFree(buf);

	m_index->close(m_session, true);
	delete m_index;
	m_index = NULL;
	drop(m_path);
	closeSmallHeap(heap);
	DrsHeap::drop(HEAP_NAME);
	delete tableDef;
	TableDef::drop(TBLDEF_NAME);

	clear();
}

/**
 * 测试对空索引位图页位的重用正确性
 */
void IndexTestCase::testReuseFreeByteMaps() {
	init();
	// 创建基本索引文件
	TableDef *tableDef = createSmallTableDef();
	DrsHeap *heap = createSmallHeap(tableDef);
	buildHeap(heap, tableDef, 10, true, false);
	
	CPPUNIT_ASSERT(heap != NULL);
	CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
	createIndex(m_path, heap, tableDef);
	CPPUNIT_ASSERT(m_index->getIndexNum() == 2);
	m_index->dropPhaseOne(m_session, 0);
	m_index->dropPhaseTwo(m_session, 0);
	CPPUNIT_ASSERT(m_index->getIndexNum() == 1);

	// 将空闲链表置空
	PageHandle *handle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
	IndexHeaderPage *header = (IndexHeaderPage*)handle->getPage();
	header->m_freeListPageIds[0] = INVALID_PAGE_ID;
	header->m_freeListPageIds[1] = INVALID_PAGE_ID;
	m_session->markDirty(handle);
	m_session->releasePage(&handle);
	// 在丢弃了第一个索引之后，再重新申请页面，必然会使用第一个索引释放的第一个页面
	IndicePageManager *manager = ((DrsBPTreeIndice*)m_index)->getPagesManager();
	IndexLog *logger = ((DrsBPTreeIndice*)m_index)->getLogger();
	u64 pageId;
	PageHandle *newHandle = manager->allocPage(logger, m_session, 2, &pageId);
	m_session->releasePage(&newHandle);

	CPPUNIT_ASSERT(pageId == IndicePageManager::NON_DATA_PAGE_NUM);

	m_index->close(m_session, true);
	delete m_index;
	m_index = NULL;
	drop(m_path);
	closeSmallHeap(heap);
	DrsHeap::drop(HEAP_NAME);
	delete tableDef;
	TableDef::drop(TBLDEF_NAME);

	clear();
}


/**
 * 测试根据某个指定页面指针和ID获得页面所属索引编号功能
 */
void IndexTestCase::testGetIndexNo() {
	init();

	// 创建基本索引文件
	TableDef *tableDef = createSmallTableDef();
	DrsHeap *heap = createSmallHeap(tableDef);
	buildHeap(heap, tableDef, 10, true, false);
	
	CPPUNIT_ASSERT(heap != NULL);
	CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
	createIndex(m_path, heap, tableDef);
	CPPUNIT_ASSERT(m_index->getIndexNum() == 2);

	int indexNo;
	File *file = ((DrsBPTreeIndice*)m_index)->getFileDesc();
	// 验证第0页和位图页不属于任何索引
	PageHandle *handle = GET_PAGE(m_session, file, PAGE_INDEX, 0, Shared, m_index->getDBObjStats(), NULL);
	BufferPageHdr *page = handle->getPage();
	indexNo = m_index->getIndexNo(page, 0);
	CPPUNIT_ASSERT(indexNo == -1);
	m_session->releasePage(&handle);

	handle = GET_PAGE(m_session, file, PAGE_INDEX, 1, Shared, m_index->getDBObjStats(), NULL);
	page = handle->getPage();
	indexNo = m_index->getIndexNo(page, 1);
	CPPUNIT_ASSERT(indexNo == -1);
	m_session->releasePage(&handle);

	handle = GET_PAGE(m_session, file, PAGE_INDEX, 128, Shared, m_index->getDBObjStats(), NULL);
	page = handle->getPage();
	indexNo = m_index->getIndexNo(page, 128);
	CPPUNIT_ASSERT(indexNo == -1);
	m_session->releasePage(&handle);

	// 验证129页面属于第一个索引
	handle = GET_PAGE(m_session, file, PAGE_INDEX, 129, Shared, m_index->getDBObjStats(), NULL);
	page = handle->getPage();
	indexNo = m_index->getIndexNo(page, 129);
	CPPUNIT_ASSERT(indexNo == 0);
	m_session->releasePage(&handle);

	// 验证倒数两个页面属于第二个索引
	u64 size;
	file->getSize(&size);
	PageId maxPageId = size / Limits::PAGE_SIZE - 1;
	handle = GET_PAGE(m_session, file, PAGE_INDEX, maxPageId, Shared, m_index->getDBObjStats(), NULL);
	page = handle->getPage();
	indexNo = m_index->getIndexNo(page, maxPageId);
	CPPUNIT_ASSERT(indexNo == 1);
	m_session->releasePage(&handle);

	handle = GET_PAGE(m_session, file, PAGE_INDEX, maxPageId - 1, Shared, m_index->getDBObjStats(), NULL);
	page = handle->getPage();
	indexNo = m_index->getIndexNo(page, maxPageId - 1);
	CPPUNIT_ASSERT(indexNo == 1);
	m_session->releasePage(&handle);

	m_index->close(m_session, true);
	delete m_index;
	m_index = NULL;
	drop(m_path);
	closeSmallHeap(heap);
	DrsHeap::drop(HEAP_NAME);
	delete tableDef;
	TableDef::drop(TBLDEF_NAME);

	clear();
}

void IndexTestCase::init() {
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_cfg, true));
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("IndexTestCase::setUp", m_conn);
	m_tblDef = getATableDef();
	char name[255] = "testIndex.nti";
	memcpy(m_path, name, sizeof(name));
	m_index = NULL;
	File oldindexfile(name);
	oldindexfile.remove();
	srand((unsigned)time(NULL));
	ts.idx = true;
}

void IndexTestCase::clear() {
	if (m_index != NULL) {
		m_index->close(m_session, true);
		delete m_index;
		drop(m_path);
	}
	if (m_db != NULL) {
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);
		m_db->close();
		delete m_db;
		m_db = NULL;
		Database::drop(".");
	}
	delete m_tblDef;
	m_tblDef = NULL;
}

/**
 * 测试位图页已经被使用了很多的情况下分配页面需要读多个页面的情况
 */
void IndexTestCase::testUseBytesOfByteMaps() {
	if (isEssentialOnly())
		return;

	init();
	// 创建基本索引文件
	TableDef *tableDef = createSmallTableDef();
	DrsHeap *heap = createSmallHeap(tableDef);
	buildHeap(heap, tableDef, 10, true, false);
	
	CPPUNIT_ASSERT(heap != NULL);
	CPPUNIT_ASSERT(tableDef->m_numIndice != 0);
	createIndex(m_path, heap, tableDef);

	// 首先标志所有索引都没有预留的空闲页
	PageHandle *handle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 0, Exclusived, m_index->getDBObjStats(), NULL);
	IndexHeaderPage *header = (IndexHeaderPage*)handle->getPage();
	header->m_freeListPageIds[0] = INVALID_PAGE_ID;
	header->m_freeListPageIds[1] = INVALID_PAGE_ID;
	m_session->markDirty(handle);
	m_session->releasePage(&handle);

	// 将位图页全部置为非零，表示都已经被使用
	handle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 1, Exclusived, m_index->getDBObjStats(), NULL);
	IndexFileBitMapPage *bmpage = (IndexFileBitMapPage*)handle->getPage();
	for (uint i = sizeof(IndicePage) + 2; i < INDEX_PAGE_SIZE; i++)
		*((byte*)bmpage + i) = 0xFF;
	m_session->markDirty(handle);
	m_session->releasePage(&handle);

	// 申请两个新的索引页面，第二次申请会需要遍历第二个位图页
	IndicePageManager *manager = ((DrsBPTreeIndice*)m_index)->getPagesManager();
	IndexLog *logger = ((DrsBPTreeIndice*)m_index)->getLogger();
	u64 pageId;
	PageHandle *newHandle = manager->allocPage(logger, m_session, 2, &pageId);
	m_session->releasePage(&newHandle);
	//newHandle = manager->allocPage(logger, m_session, 2, &pageId, tableDef->m_incrSize);
	//m_session->releasePage(&newHandle);

	handle = GET_PAGE(m_session, ((DrsBPTreeIndice*)m_index)->getFileDesc(), PAGE_INDEX, 2, Exclusived, m_index->getDBObjStats(), NULL);
	bmpage = (IndexFileBitMapPage*)handle->getPage();
	CPPUNIT_ASSERT(*((byte*)bmpage + sizeof(IndicePage)) != 0x00);
	m_session->releasePage(&handle);

	m_index->close(m_session, true);
	delete m_index;
	m_index = NULL;
	drop(m_path);
	closeSmallHeap(heap);
	DrsHeap::drop(HEAP_NAME);
	delete tableDef;
	TableDef::drop(TBLDEF_NAME);

	clear();
}

TableDef* IndexTestCase::createBigTableDef() {
	File oldsmallfile(TBLDEF_NAME);
	oldsmallfile.remove();

	TableDefBuilder *builder;
	TableDef *tableDef;

	// 创建小堆
	builder = new TableDefBuilder(99, "inventory", "User");
	builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, LONG_KEY_LENGTH);
	builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
	builder->addIndex("RANGE", false, false, false, "UserId", 0, "UserName", 0, "BankAccount", 0, NULL);
	tableDef = builder->getTableDef();
	EXCPT_OPER(tableDef->writeFile(TBLDEF_NAME));
	delete builder;

	return tableDef;
}

/**
 * 创建大堆，记录长度超过100多
 * 其中只包含一个索引，并且是非唯一性的索引
 */
DrsHeap* IndexTestCase::createBigHeap(const TableDef *tableDef) {
	DrsHeap *heap;
	char tableName[255] = HEAP_NAME;
	File oldsmallfile(tableName);
	oldsmallfile.remove();

	EXCPT_OPER(DrsHeap::create(m_db, tableName, tableDef));
	EXCPT_OPER(heap = DrsHeap::open(m_db, m_session, tableName, tableDef));

	return heap;
}

/**
 * 创建大表数据，该表为了测试索引键值超过128而建立
 * 因此表中设计索引的字符串长度必须达到甚至超过128
 */
uint IndexTestCase::buildBigHeap(DrsHeap *heap, const TableDef *tblDef, bool testPrefix, uint size, uint step) {
	Record *record;
	uint count = 0;
	for (uint i = 0; i < size; i += step) {
		char name[LONG_KEY_LENGTH];
		RowId rid;
		if (testPrefix)
			sprintf(name, FAKE_LONG_CHAR234 "%d", i);
		else
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
		record = createRecord(tblDef, 0, i, name, i + ((u64)i << 32), (u32)(-1) - i);
		EXCPT_OPER(rid = heap->insert(m_session, record, NULL));
		count++;
		freeRecord(record);
	}

	return count;
}