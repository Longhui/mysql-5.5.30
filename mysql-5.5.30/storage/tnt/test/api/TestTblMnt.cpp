#include "api/TblMnt.h"
#include "TestTblMnt.h"
#include "Test.h"
#include "api/TestTable.h"
#include "misc/RecordHelper.h"
#include "misc/Global.h"
#include "util/Thread.h"
//#include "api/Database.h"
#include "misc/ControlFile.h"
#include <cstdlib>

using namespace std;
using namespace ntse;

static const char* TEMP_IDX_EXT = ".tmpnsi";
static const char* TEMP_HEAP_EXT = ".tmpnsd";
static const char* TEMP_PATH_SEMIEXT = "_oltmp_";

/**
 * 比较两个索引定义是否相等
 * @param left           比较索引之一
 * @param right          比较索引之二
 * @return               两索引相同返回true
 */
bool defEq (const IndexDef *left, const IndexDef *right) {
	bool eq = true;
	if (left->m_numCols == right->m_numCols) {
		for (u16 col = 0; col < left->m_numCols; ++col) {
			if (left->m_columns[col] != right->m_columns[col]) {
				eq = false;
				break;
			}
		}
	} else {
		eq = false;
	}
	return eq;
}

bool nameEq(const IndexDef *left, const IndexDef *right) {
	return 0 == strcmp(left->m_name, right->m_name);
}

/*** Tasks ***/

class AlterIndice : public Thread {
public:
	AlterIndice(Database *db, Table *tb, u16 numAddIndice, const IndexDef **addIndice, u16 numDelIndice, const IndexDef **delIndice) : Thread("Alter Indice") {
		m_db = db;
		m_table = tb;
		m_numAdd = numAddIndice;
		m_numDel = numDelIndice;
		m_addIndice = addIndice;
		m_delIndice = delIndice;
	}
private:
	Database *m_db;
	Table *m_table;
	u16 m_numAdd, m_numDel;
	const IndexDef **m_addIndice, **m_delIndice;

	void run() {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("AlterIndice::run", conn);

		TblMntAlterIndex altIdx(m_table, m_numAdd, m_addIndice, m_numDel, m_delIndice);
		try {
			altIdx.alterTable(session);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
};


class AlterColumn : public Thread {
public:
	AlterColumn(Database *db, Table *tb, u16 addColNum = 0, const AddColumnDef *addCols = NULL, u16 delColNum = 0, const ColumnDef **delCols = NULL) : Thread("Alter Column") {
		m_db = db;
		m_table = tb;
		m_numAdd = addColNum;
		m_numDel = delColNum;
		m_addCols = addCols;
		m_delCols = delCols;
	}
private:
	Database *m_db;
	Table *m_table;
	u16 m_numAdd, m_numDel;
	const AddColumnDef *m_addCols;
	const ColumnDef **m_delCols; 


	void run() {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("AlterIndice::run", conn);

		//TblMntAlterColumn colAlt(m_table, conn, 0, NULL, 0, NULL);
		TblMntAlterColumn colAlt(m_table, conn, m_numAdd, m_addCols, m_numDel, m_delCols);
		try {
			colAlt.alterTable(session);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}
};

class InsertBlog : public Task {
public:
	InsertBlog(Database *db, Table *blogtb, uint interval, uint onceCnt, u64 startID, u64 step) : Task("Insert into table Blog", interval) {
		m_blogtb = blogtb;
		m_db = db;
		m_startID = startID;
		m_step = step;
		m_onceCnt = onceCnt;
	}

	void run() {
		TblMntTestCase::massInsertBlog(m_db, m_blogtb, m_onceCnt, m_startID, m_step);
		m_startID += m_onceCnt * m_step;
	}
private:
	u64 m_startID;
	u64 m_step;
	uint m_onceCnt;
	Database *m_db;
	Table *m_blogtb;
};


class InsertBlogCount : public Task {
public:
	InsertBlogCount(Database *db, Table *blogcnttb, uint interval, uint onceCnt, u64 startID, u64 step) : Task("Insert into table BlogCount", interval) {
		m_blogcnttb = blogcnttb;
		m_db = db;
		m_startID = startID;
		m_step = step;
		m_onceCnt = onceCnt;
	}

	void run() {
		TblMntTestCase::massInsertBlogCount(m_db, m_blogcnttb, m_onceCnt, m_startID, m_step);
		m_startID += m_onceCnt * m_step;
	}
private:
	u64 m_startID;
	u64 m_step;
	uint m_onceCnt;
	Database *m_db;
	Table *m_blogcnttb;
};

class DeleteEach : public Task {
public:
	DeleteEach(Database *db, Table *table, uint interval, uint percent) : Task("Delete from table using table scan.", interval) {
		m_percent = percent;
		m_db = db;
		m_tb = table;
	}
	void run() {
		TblMntTestCase::scanDelete(m_db, m_tb, m_percent);
	}
private:
	uint m_percent;
	Database *m_db;
	Table *m_tb;
};

class UpdateBlogTable : public Task {
public:
	UpdateBlogTable(Database *db, Table *table, uint interval, uint percent, uint priupdratio) : Task("Update from table Blog using table scan.", interval) {
		m_percent = percent;
		m_priupdratio = priupdratio;
		m_db = db;
		m_tb = table;
	}
	void run() {
		TblMntTestCase::scanUpdateBlog(m_db, m_tb, m_percent, m_priupdratio);
	}
private:
	uint m_percent;
	uint m_priupdratio;
	Database *m_db;
	Table *m_tb;
};
/*** end Tasks ***/
/*
static getIndexCols(IndexDef *idx) {
	u16 *columns = new u16[idx->m_numCols];
	for (u16 i= 0; i < idx->m_numCols; ++i) {
		columns[i] = idx->m_columns
	}
}
*/

const char * TblMntTestCase::getName() {
	return "Online table maintain test.";
}

const char * TblMntTestCase::getDescription() {
	return "Online table maintain operations.";
}

void TblMntTestCase::setUp() {
	m_db = NULL;
	m_blogTable = NULL;
	m_blogCntTable = NULL;
	Database::drop(".");
	EXCPT_OPER(m_db = Database::open(&m_config, true));
	Table::drop(m_db, "BlogCount");
	Table::drop(m_db, "Blog");
	File blogtmpidx("Blog.tmpnsi");
	blogtmpidx.remove();
	File blogcnttmpidx("BlogCount.tmpnsi");
	blogcnttmpidx.remove();

}

void TblMntTestCase::tearDown() {
	if (m_db) {
		//Table::drop(m_db, 0, "BlogCount", false);
		if (m_blogCntTable) {
			closeTable(m_blogCntTable, true);
			m_blogCntTable = NULL;
		}
		if (m_blogTable) {
			closeTable(m_blogTable, true);
			m_blogTable = NULL;
		}
		//Table::drop(m_db, 0, "BlogCount", false);
		//Table::drop(m_db, 0, "Blog", false);
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::tearDown", conn);
		m_db->dropTable(session, "BlogCount");
		m_db->dropTable(session, "Blog");
		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->close();
		delete m_db;
	}
	Database::drop(".");
}

bool TblMntTestCase::isBig() {
	return false;
}


void TblMntTestCase::createBlogTable(bool usemms, bool cacheUpdate, Database *db) {
	if (!db) db = m_db;
	TableDef *tbdef = TableTestCase::getBlogDef(usemms, true);
	// TODO: 修改cache update
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::createBlogTable", conn);
	db->createTable(session, "Blog", tbdef);
	//Table::create(db, session, "Blog", tbdef);
	delete tbdef;
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
}

void TblMntTestCase::createBlogCountTable(bool usemms, bool cacheUpdate, Database *db) {
	if (!db) db = m_db;
	TableDef *tbdef = TableTestCase::getBlogCountDef(usemms, true);
	// TODO: 修改cache update
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::createBlogCountTable", conn);
	//Table::create(db, session, "BlogCount", tbdef);
	db->createTable(session, "BlogCount", tbdef);
	delete tbdef;
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
}

Table* TblMntTestCase::openTable(const char *path, ntse::Database *db) {
	if (!db) db = m_db;
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::openTable", conn);
	try {
		//Table *table = Table::open(db, session, path);
		Table *table = db->openTable(session, path);
		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);
		return table;
	} catch (NtseException &e) {
		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);
		throw e;
	}
}

void TblMntTestCase::closeTable(ntse::Table *table, bool flushDirty, ntse::Database *db) {
	if (!db) db = m_db;
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::closeTable", conn);
	//table->close(session, flushDirty);
	m_db->closeTable(session, table);
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	//delete table;
}

/**
* 返回因主键冲突insert失败的数目
*/
uint TblMntTestCase::massInsertBlogCount(ntse::Database *db, ntse::Table *table, uint numRows, u64 startBlogID, u64 stepBlogID) {
	uint failed = 0;
	for (uint n = 0; n < numRows; ++n) {
		u64 BlogID = startBlogID + n * stepBlogID;

		Connection *conn = db->getConnection(false);
		Session *session = db->getSessionManager()->allocSession("TblMntTestCase::massInsertBlogCount", conn);


massInsertBlogCount_lockTable:
		SYNCHERE(SP_TBL_MASSINSERTBLOGCOUNT_BEFORE_LOCKMETA);
		do {
			try {
				table->lockMeta(session, IL_S, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
				break;
			} catch (NtseException &e) {
				UNREFERENCED_PARAMETER(e);
			}
		} while (true);
		SYNCHERE(SP_TBL_MASSINSERTBLOGCOUNT_BEFORE_LOCK);
		try {
			table->lock(session, IL_IX, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			table->unlockMeta(session, IL_S);
			goto massInsertBlogCount_lockTable;
		}

		u16 numcols = table->getTableDef()->m_numCols;

		RecordBuilder rb(table->getTableDef(), RID(0, 0), REC_REDUNDANT);
		rb.appendBigInt(BlogID);
		rb.appendInt(10);
		if (numcols == 6)
			rb.appendSmallInt(2);
		rb.appendInt(abs(System::random()));
		char title[25];
		for (size_t l = 0; l < sizeof(title) - 1; l++)
			title[l] = (char )('A' + System::random() % 26);
		title[sizeof(title) - 1] = '\0';
		rb.appendChar(title);
		rb.appendBigInt(((u64)abs(System::random()) << 32) + abs(System::random()));

		Record *rec = rb.getRecord(table->getTableDef()->m_maxRecSize);
		uint dupIndex;
		rec->m_format = REC_MYSQL;

		rec->m_rowId = table->insert(session, rec->m_data, true, &dupIndex, false);
		if (rec->m_rowId == INVALID_ROW_ID) ++failed;


		table->unlock(session, IL_IX);
		table->unlockMeta(session, IL_S);
		SYNCHERE(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);

		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);

		freeRecord(rec);
	}

	return failed;
}

uint TblMntTestCase::massInsertBlog(Database *db, Table* table, uint numRows, u64 startID, u64 stepID) {
	uint failed = 0;


	for (uint n = 0; n < numRows; ++n) {

		Connection *conn = db->getConnection(false);
		Session *session = db->getSessionManager()->allocSession("TableTestCase::populateBlog", conn);

massInsertBlog_lockTable:
		SYNCHERE(SP_TBL_MASSINSERTBLOG_BEFORE_LOCKMETA);
		do {
			try {
				table->lockMeta(session, IL_S, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
				break;
			} catch (NtseException &e) {
				UNREFERENCED_PARAMETER(e);
			}
		} while (true);
		SYNCHERE(SP_TBL_MASSINSERTBLOG_BEFORE_LOCK);
		try {
			table->lock(session, IL_IX, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			table->unlockMeta(session, IL_S);
			goto massInsertBlog_lockTable;
		}


		SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
		u16 colnum = table->getTableDef()->m_numCols;
		u64 ID = startID + n * stepID;

		//u64 id = n + 1;
		u64 userId = System::random() % 1000 + 1;
		u64 publishTime = System::currentTimeMillis();
		char *title = NULL;
		if (colnum == 7)
			title = randomStr(100);
		char *tags = NULL;
		if (colnum == 7)
			tags = randomStr(100);
		SubRecord *subRec;
		if (colnum == 7)
			subRec = srb.createSubRecordByName("ID UserID PublishTime Title", &ID, &userId, &publishTime, title);
		else {
			u16 ranking = (System::random() % 9) + 1;
			subRec = srb.createSubRecordByName("ID Ranking UserID PublishTime", &ID, &ranking, &userId, &publishTime);
		}

		if (tags) {
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[4], 100);
			RecordOper::setNullR(table->getTableDef(), subRec, 4, false);
			RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[4], (byte *)tags);
		} else {
			RecordOper::setNullR(table->getTableDef(),subRec, 4, true);
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[4], 0);
			RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[4], NULL);
		}
		char *abs;
		bool lobNotNull = (System::random() % 10 == 0); // 1/10的概率有lob
		if (!lobNotNull) {
			abs = NULL;
			if (colnum == 7) {
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 0);
				RecordOper::setNullR(table->getTableDef(), subRec, 5, true);
			}
			else {
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 0);
				RecordOper::setNullR(table->getTableDef(), subRec, 5, true);
			}
		} else {
			abs = randomStr(100);
			if (colnum == 7) {
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 100);
				RecordOper::setNullR(table->getTableDef(), subRec, 5, false);
			}
			else {
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 100);
				RecordOper::setNullR(table->getTableDef(), subRec, 5, false);
			}
		}
		if (colnum == 7)
			RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[5], (byte *)abs);
		else
			RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[5], (byte *)abs);
		//if (abs) delete [] abs;

		if (colnum == 7) {
			size_t contentSize;
			if (n % 20) {
				if (lobNotNull)
					contentSize = TableTestCase::SMALL_LOB_SIZE;
				else
					contentSize = 0;
			} else
				contentSize = TableTestCase::LARGE_LOB_SIZE;
			if (contentSize) {
				char *content = randomStr(contentSize);
				RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], (byte *)content);
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], (uint)contentSize);
				RecordOper::setNullR(table->getTableDef(), subRec, 6, false);
			} else {
				RecordOper::setNullR(table->getTableDef(), subRec, 6, true);
				RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], NULL);
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], 0);
			}
		} else {
			size_t contentSize;
			if (n % 20) {
				if (lobNotNull)
					contentSize = (System::random() % 400) + 100;
				else
					contentSize = 0;
			} else
				contentSize = 0;
			if (contentSize) {
				char *content = randomStr(contentSize);
				RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], (byte *)content);
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], (uint)contentSize);
				RecordOper::setNullR(table->getTableDef(), subRec, 6, false);
			} else {
				RecordOper::setNullR(table->getTableDef(), subRec, 6, true);
				RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], NULL);
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], 0);
			}
		}

		if (colnum == 8) {
			RecordOper::setNullR(table->getTableDef(), subRec, 7, true);
			RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[7], NULL);
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[7], 0);
		}

		Record *rec = new Record(RID(0, 0), REC_MYSQL, subRec->m_data, table->getTableDef()->m_maxRecSize);

		uint dupIndex;
		rec->m_format = REC_MYSQL;
		try {
			rec->m_rowId = table->insert(session, rec->m_data, true, &dupIndex, false);
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			assert(false);
		}
		if (rec->m_rowId == INVALID_ROW_ID) ++failed;
		//if (doubleInsert)
		//	CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, &dupIndex)) == INVALID_ROW_ID);

		freeMysqlRecord(table->getTableDef(), rec);

		SYNCHERE(SP_TBL_MASSINSERTBLOG_BEFORE_UNLOCK);
		table->unlock(session, IL_IX);
		SYNCHERE(SP_TBL_MASSINSERTBLOG_BEFORE_UNLOCKMETA);
		table->unlockMeta(session, IL_S);
		SYNCHERE(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);

		db->getSessionManager()->freeSession(session);
		db->freeConnection(conn);

		//rows[n] = rec;
		if (title)
			delete []title;
		
		delete [] subRec->m_columns;
		delete subRec;
		//freeRecord(rec);

		//freeSubRecord(subRec);
	}

	return failed;
}


void TblMntTestCase::scanDelete(Database *db, Table* table, uint percent) {
	assert(percent <= 100);
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::scanDelete", conn);
	byte buf[Limits::PAGE_SIZE];

scanDelete_lockTable:
	SYNCHERE(SP_TBL_SCANDELETE_BEFORE_LOCKMETA);
	do {
		try {
			table->lockMeta(session, IL_S, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			break;
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
		}
	} while (true);
	SYNCHERE(SP_TBL_SCANDELETE_BEFORE_LOCK);
	try {
		table->lock(session, IL_IX, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		table->unlockMeta(session, IL_S);
		goto scanDelete_lockTable;
	}


	u16 *columns = new u16[table->getTableDef()->m_numCols];
	for (int i = 0; i < table->getTableDef()->m_numCols; ++i) {
		columns[i] = i;
	}

	TblScan *hdl;
	//scanDelete_start_scan:
	try {
		hdl = table->tableScan(session, OP_DELETE, table->getTableDef()->m_numCols, columns, false);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		//goto scanDelete_start_scan;
		assert(false);
	}
	while (table->getNext(hdl, buf)) {
		bool todel = ((System::random() % 100) < (int)percent);
		if (todel) {
			table->deleteCurrent(hdl);
			SYNCHERE(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		}
	}
	table->endScan(hdl);

	SYNCHERE(SP_TBL_SCANDELETE_BEFORE_UNLOCK);
	table->unlock(session, IL_IX);
	SYNCHERE(SP_TBL_SCANDELETE_BEFORE_UNLOCKMETA);
	table->unlockMeta(session, IL_S);
	SYNCHERE(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);


	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	delete [] columns;
}

void TblMntTestCase::scanUpdateBlog(Database *db, Table *table, uint percent, uint priupdratio) {
	assert(percent <= 100);
	assert(priupdratio <= 100);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::scanUpdateBlog", conn);

	byte *buf = (byte *)session->getMemoryContext()->alloc(Limits::DEF_MAX_REC_SIZE);
	TblScan *hdl;

scanUpdateBlog_lockTable:
	SYNCHERE(SP_TBL_SCANUPDATE_BEFORE_LOCKMETA);
	try {
		table->lockMeta(session, IL_S, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		goto scanUpdateBlog_lockTable;
	}
	SYNCHERE(SP_TBL_SCANUPDATE_BEFORE_LOCK);
	try {
		table->lock(session, IL_IX, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		table->unlockMeta(session, IL_S);
		goto scanUpdateBlog_lockTable;
	}
	u16 colnum = table->getTableDef()->m_numCols;

	u16 *columns = new u16[table->getTableDef()->m_numCols];
	for (int i = 0; i < table->getTableDef()->m_numCols; ++i) {
		columns[i] = i;
	}

	try {
		hdl = table->tableScan(session, OP_UPDATE, table->getTableDef()->m_numCols, columns, false);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		assert(false);
	}
	bool updColset = false;
	u16 *updCols;
	u16 updNumCols;
	while (table->getNext(hdl, buf)) {
		bool toUpd= ((System::random() % 100) < (int)percent);
		if (toUpd) {
			u64 oldID = RedRecord::readBigInt(table->getTableDef(), buf, 0);
			u64 newID = oldID;
			if ((System::random() % 100) < (int)priupdratio) {
				newID = (System::random() % 10000000);
			}
			u64 userId = System::random() % 1000 + 1;
			u64 publishTime = System::currentTimeMillis();
			char *title = NULL;
			if (colnum == 7)
				title = randomStr(100);
			char *tags = NULL;
			if (colnum == 7)
				tags = randomStr(100);
			SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
			SubRecord *subRec;
			if (colnum == 7)
				subRec = srb.createSubRecordByName("ID UserID PublishTime Title", &newID, &userId, &publishTime, title);
			else {
				u16 ranking = (System::random() % 10) + 10;
				subRec = srb.createSubRecordByName("ID Ranking UserID PublishTime", &newID, &ranking, &userId, &publishTime);
			}

			//byte *recdata = new byte[table->getTableDef()->m_maxRecSize];
			//memcpy(recdata, subRec->m_data, table->getTableDef()->m_maxRecSize);
			Record *rec = new Record(RID(0, 0), REC_MYSQL, subRec->m_data, table->getTableDef()->m_maxRecSize);

			if (colnum == 7) {
				RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[4], 100);
				RecordOper::setNullR(table->getTableDef(), subRec, 4, false);
				RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[4], (byte *)tags);
			}

			char *abs;
			bool lobNotNull = (System::random() % 10 == 0); // 1/4的概率有lob
			if (!lobNotNull) {
				abs = NULL;
				if (colnum == 7) {
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 0);
					RecordOper::setNullR(table->getTableDef(), subRec, 5, true);
				}
				else {
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[4], 0);
					RecordOper::setNullR(table->getTableDef(), subRec, 4, true);
				}
			} else {
				abs = randomStr(100);
				if (colnum == 7) {
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 100);
					RecordOper::setNullR(table->getTableDef(), subRec, 5, false);
				} else {
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[4], 100);
					RecordOper::setNullR(table->getTableDef(), subRec, 4, false);
				}
			}
			if (colnum == 7)
				RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[5], (byte *)abs);
			else
				RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[4], (byte *)abs);


			bool hasbiglob = (System::random() % 20 == 0);
			bool biglobbig = (System::random() % 4 == 0);

			char *content = NULL;

			if (colnum == 7) {
				size_t contentSize;
				//char *content;
				if (!biglobbig) {
					if (hasbiglob)
						contentSize = TableTestCase::SMALL_LOB_SIZE;
					else
						contentSize = 0;
				} else
					contentSize = TableTestCase::LARGE_LOB_SIZE;
				if (contentSize) {
					content = randomStr(contentSize);
					RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], (byte *)content);
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], (uint)contentSize);
					RecordOper::setNullR(table->getTableDef(), subRec, 6, false);
					//delete [] content;
				} else {
					content = NULL;
					RecordOper::setNullR(table->getTableDef(), subRec, 6, true);
					RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], NULL);
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], 0);
				}
			} else {
				size_t contentSize;
				//char *content;
				if (!biglobbig) {
					if (hasbiglob)
						contentSize = (System::random() % 300) + 150;
					else
						contentSize = 0;
				} else
					contentSize = 0;
				if (contentSize) {
					content = randomStr(contentSize);
					RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[5], (byte *)content);
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], (uint)contentSize);
					RecordOper::setNullR(table->getTableDef(), subRec, 5, false);
					//delete [] content;
				} else {
					content = NULL;
					RecordOper::setNullR(table->getTableDef(), subRec, 5, true);
					RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[5], NULL);
					RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 0);
				}
			}



			uint dupIndex;
			rec->m_format = REC_MYSQL;

#if 0
			char *abs;
			bool lobNotNull = (System::random() % 4 == 0); // 1/4的概率有lob
			if (!lobNotNull) {
				abs = NULL;
				table->writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 0);
				//table->writeLob(subRec->m_data, table->getTableDef().m_columns[5], NULL);
				RecordOper::setNullR(table->getTableDef(), subRec, 5, true);
			} else {
				abs = randomStr(100);
				table->writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 100);
				RecordOper::setNullR(table->getTableDef(), subRec, 5, false);
			}
			table->writeLob(subRec->m_data, table->getTableDef()->m_columns[5], (byte *)abs);
#endif

			if (!updColset) {
				updNumCols = table->getTableDef()->m_numCols;
				updCols = new u16[updNumCols];
				for (u16 i = 0; i < updNumCols; ++i)
					updCols[i] = i;

				hdl->setUpdateColumns(updNumCols, updCols);
				updColset = true;
			}

			try {
				table->updateCurrent(hdl, rec->m_data, true, &dupIndex);
			} catch (NtseException &e) {
				UNREFERENCED_PARAMETER(e);
				// nothing
			}

			if (abs) delete [] abs;
			if (content) delete [] content;


			if (title)
				delete []title;

			if (tags)
				delete []tags;
			
			delete [] subRec->m_columns;
			delete subRec;
			freeRecord(rec);
			//freeSubRecord(subRec);

			SYNCHERE(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		}
	}
	if (updColset)
		delete [] updCols;
	table->endScan(hdl);

	SYNCHERE(SP_TBL_SCANUPDATE_BEFORE_UNLOCK);
	table->unlock(session, IL_IX);
	SYNCHERE(SP_TBL_SCANUPDATE_BEFORE_UNLOCKMETA);
	table->unlockMeta(session, IL_S);
	SYNCHERE(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	delete [] columns;

}


uint TblMntTestCase::recCountBlog(ntse::Database *db, ntse::Table *table) {
	uint recCnt = 0;
	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
	u64 ID;
	//SubRecord *subRec = srb.createSubRecordByName("ID UserID PublishTime Title Tags", &ID, &userId, &publishTime, title, tags);
	//SubRecord *subRec = srb.createEmptySbByName("ID", &ID);
	SubRecord *subRec = srb.createSubRecordByName("ID", &ID);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::recCountBlog", conn);

	TblScan *hdl = table->tableScan(session, OP_READ, subRec->m_numCols,subRec->m_columns);
	while (table->getNext(hdl, subRec->m_data))
		++recCnt;
	table->endScan(hdl);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);

	return recCnt;
}

uint TblMntTestCase::recCountBlogCount(ntse::Database *db, ntse::Table *table) {
	uint recCnt = 0;
	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
	u64 BlogID;
	SubRecord *subRec = srb.createSubRecordById("0", &BlogID);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TblMntTestCase::recCountBlogCount", conn);

	TblScan *hdl = table->tableScan(session, OP_READ, subRec->m_numCols,subRec->m_columns);
	while (table->getNext(hdl, subRec->m_data))
		++recCnt;
	table->endScan(hdl);

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);

	return recCnt;
}

void TblMntTestCase::testLogReplay() {
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));

	EXCPT_OPER(createBlogCountTable(true, true));
	EXCPT_OPER(m_blogCntTable = openTable("BlogCount"));

	int olLSN;

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);

	m_blogTable->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	m_blogTable->lock(session, IL_S, -1, __FILE__, __LINE__);
	u64 lsnstart = m_db->getTxnlog()->tailLsn();
	while ((olLSN = m_db->getTxnlog()->setOnlineLsn(lsnstart)) < 0) {
		lsnstart = m_db->getTxnlog()->tailLsn();
	}	
	m_blogTable->unlock(session, IL_S);
	m_blogTable->unlockMeta(session, IL_S);

	InsertBlog insblog1(m_db, m_blogTable, 300, 1600, 0, 2);
	InsertBlogCount insblogcnt1(m_db, m_blogCntTable, 1000, 51, 0, 7);
	InsertBlog insblog2(m_db, m_blogTable, 400, 7, 0, 3);
	InsertBlogCount insblogcnt2(m_db, m_blogCntTable, 500, 37, 0, 5);
	DeleteEach delblog1(m_db, m_blogTable, 20000, 5);
	DeleteEach delblogcnt1(m_db, m_blogCntTable, 17000, 7);

	Thread::msleep(1000);

	insblog1.stop();

	m_blogTable->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	m_blogTable->lock(session, IL_S, -1, __FILE__, __LINE__);
	u64 lsnend = m_db->getTxnlog()->tailLsn();
	m_blogTable->unlock(session, IL_S);
	m_blogTable->unlockMeta(session, IL_S);


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	cout <<"LSN start is " << lsnstart << endl;
	cout <<"LSN end is   " << lsnend << endl;

	LogReplay replay(m_blogTable, m_db, lsnstart, lsnend);
	replay.start();

	int txncount = 0;
	TxnLogList *txnlogs;
	while (txnlogs = replay.getNextTxn(session)) {
		if (txncount < 10)
			cout<<RID_GET_PAGE(txnlogs->m_rowId) << "\t" << RID_GET_SLOT(txnlogs->m_rowId) << endl;
		++txncount;
	}
	replay.end();

	m_db->getTxnlog()->clearOnlineLsn(olLSN);

	cout << "TXN count is " << txncount <<endl;

}

void TblMntTestCase::testAlterIndexException() {
	EXCPT_OPER(createBlogCountTable(true, true));
	EXCPT_OPER(m_blogCntTable = openTable("BlogCount"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testAlterIndexFLR", conn);

	InsertBlogCount insbcnt1(m_db, m_blogCntTable, 1000, 50, 0, 2);

	insbcnt1.enableSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	insbcnt1.start();

	//Thread::msleep(15000);
	int loop = 300;
	while (loop--) {
		insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
		insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	}

	insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);

	IndexDef **delindice = new IndexDef *[1];
	u16 num2del = m_blogCntTable->getTableDef()->m_numIndice - 1;
	IndexDef *index2del = m_blogCntTable->getTableDef()->m_indice[num2del];
	delindice[0] = new IndexDef(index2del);	
	
	IndexDef **faildelindice = new IndexDef *[1];
	faildelindice[0] = new IndexDef(index2del);
	delete [] faildelindice[0]->m_name;
	faildelindice[0]->m_name = new char[10];
	strcpy(faildelindice[0]->m_name, "Shit");

	try {
		TblMntAlterIndex fail1(m_blogCntTable, 0, NULL, 1, (const IndexDef **)faildelindice);
		fail1.alterTable(session);
		CPPUNIT_ASSERT(false);
	} catch (NtseException &) {
		CPPUNIT_ASSERT(true);
	}

	delete faildelindice[0];
	delete [] faildelindice;


	try {
		//delindice[0]->m_online = true;
		TblMntAlterIndex fail1(m_blogCntTable, 1, (const IndexDef **)delindice, 0, NULL);
		TableDef *tmptbdef = fail1.tempCopyTableDef(m_blogCntTable->getTableDef()->m_numIndice, m_blogCntTable->getTableDef()->m_indice);
		tmptbdef->m_numIndice = 0;
		tmptbdef->m_indice = NULL;
		delete tmptbdef;
		fail1.alterTable(session);
		CPPUNIT_ASSERT(false);
	} catch (NtseException &) {
		CPPUNIT_ASSERT(true);
	}


	string tablePath = string(m_blogCntTable->getPath());
	string fullPath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_blogCntTable->getPath();
	string fullIdxPath = fullPath + Limits::NAME_TMP_IDX_EXT;

	AlterIndice delIdx(m_db, m_blogCntTable, 0, NULL, 1, (const IndexDef **)delindice);

	//delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);
	delIdx.start();
	//delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	loop = 100;
	while (loop--) {
		insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
		insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	}
	CPPUNIT_ASSERT(insbcnt1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA));

	delIdx.enableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND); // 在获得lsn前设置
	delIdx.notifySyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	delIdx.joinSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);

	loop = 100;
	while (loop--) {
		insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
		insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	}

	delIdx.enableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	delIdx.notifySyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	delIdx.joinSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);

	m_blogCntTable->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	delIdx.disableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	//delIdx.enableSyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);
	delIdx.notifySyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	//delIdx.joinSyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);

	m_blogCntTable->unlockMeta(session, IL_S);

	//delIdx.disableSyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);
	//addIdx.enableSyncPoint();
	//delIdx.notifySyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);





	delIdx.join();

	/*
	stringstream ss;
	ss << schemaName << "_" << tableName << "_" << m_header.m_tempFileSeq++;
	string path = ss.str();
	*/

	/*stringstream ss;
	ss << m_blogCntTable->getTableDef()->m_schemaName
		<< "_" << m_blogCntTable->getTableDef()->m_name 
		<< "_" << m_db->getControlFile()->m_header.m_tempFileSeq ;  //m_header.m_tempFileSeq++;
	string tmppath = ss.str();

	File tmpfile(tmppath.c_str());
	tmpfile.create(true, false);*/
	/* 尝试增加索引 */
	//delindice[0]->m_online = true;
	AlterIndice addIdx(m_db, m_blogCntTable, 1, (const IndexDef **)delindice, 0, NULL);
	addIdx.enableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_ADDIND_ONLINE_INDICE);
	addIdx.start();

	addIdx.joinSyncPoint(SP_MNT_ALTERINDICE_BEFORE_ADDIND_ONLINE_INDICE);

	m_blogCntTable->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	addIdx.disableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_ADDIND_ONLINE_INDICE);
	addIdx.enableSyncPoint(SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL);
	addIdx.notifySyncPoint(SP_MNT_ALTERINDICE_BEFORE_ADDIND_ONLINE_INDICE);
	addIdx.joinSyncPoint(SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL);

	m_blogCntTable->unlockMeta(session, IL_S);

	addIdx.disableSyncPoint(SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL);
	addIdx.notifySyncPoint(SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL);
	/*
	addIdx.enableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	addIdx.notifySyncPoint(SP_MNT_ALTERINDICE_ADDIND_ONLINE_INDICE_UPDMETA_FAIL);
	addIdx.joinSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);

	m_blogCntTable->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	addIdx.disableSyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	addIdx.enableSyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);
	addIdx.notifySyncPoint(SP_MNT_ALTERINDICE_BEFORE_UPD_METALOCK);
	addIdx.joinSyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);

	m_blogCntTable->unlockMeta(session, IL_S);

	addIdx.disableSyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);
	//addIdx.enableSyncPoint();
	addIdx.notifySyncPoint(SP_MNT_ALTERINDICE_UPD_METALOCK_FAIL);
	*/




	addIdx.join();	

	IndexDef *tempidxdf = new IndexDef(delindice[0]);
	tempidxdf->m_columns[0]++;
	CPPUNIT_ASSERT(!defEq(tempidxdf, delindice[0]));
	delete tempidxdf;
	delete delindice[0];
	delete [] delindice;


	insbcnt1.disableSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	insbcnt1.stop();



	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TblMntTestCase::testAlterIndexFLR() {
	EXCPT_OPER(createBlogCountTable(true, true));
	EXCPT_OPER(m_blogCntTable = openTable("BlogCount"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testAlterIndexFLR", conn);

	InsertBlogCount insbcnt1(m_db, m_blogCntTable, 1000, 50, 0, 2);

	insbcnt1.enableSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	insbcnt1.start();

	//Thread::msleep(15000);
	int loop = 300;
	while (loop--) {
		insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
		insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	}

	insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);

	IndexDef **delindice = new IndexDef *[1];
	u16 num2del = m_blogCntTable->getTableDef()->m_numIndice - 1;
	IndexDef *index2del = m_blogCntTable->getTableDef()->m_indice[num2del];
	delindice[0] = new IndexDef(index2del);



	string tablePath = string(m_blogCntTable->getPath());
	string fullPath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_blogCntTable->getPath();
	string fullIdxPath = fullPath + Limits::NAME_TMP_IDX_EXT;

	File idxfile(fullIdxPath.c_str());
	idxfile.remove();

	AlterIndice delIdx(m_db, m_blogCntTable, 0, NULL, 1, (const IndexDef **)delindice);

	//delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);
	delIdx.start();
	//delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	loop = 100;
	while (loop--) {
		insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
		insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	}
	CPPUNIT_ASSERT(insbcnt1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA));

	delIdx.enableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND); // 在获得lsn前设置
	delIdx.notifySyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	delIdx.joinSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);

	loop = 100;
	while (loop--) {
		insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
		insbcnt1.joinSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	}

	delIdx.notifySyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	delIdx.join();

	delete delindice[0];
	delete [] delindice;


	insbcnt1.disableSyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	insbcnt1.notifySyncPoint(SP_TBL_MASSINSERTBLOGCOUNT_AFTER_UNLOCKMETA);
	insbcnt1.stop();



	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TblMntTestCase::testAlterIndexCompressTbl() {
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);

	//先插入一批数据
	uint numRows = 5000;
	Record ** rows = TableTestCase::populateBlog(m_db, m_blogTable, numRows, false, true, 1);

	Record **redRows = new Record* [numRows];
	for (int i = 0; i < numRows; i++) {
		byte *data = new byte[m_blogTable->getTableDef()->m_maxRecSize];
		redRows[i] = new Record(INVALID_ROW_ID, REC_MYSQL, data, m_blogTable->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_blogTable->getTableDef(), rows[i], redRows[i]);
	}

	//修改表为压缩表
	m_blogTable->lockMeta(session, IL_U, 2000, __FILE__, __LINE__);
	TableDef *tblDef = m_blogTable->getTableDef(true, session);
	tblDef->m_isCompressedTbl = true;
	tblDef->m_rowCompressCfg = new RowCompressCfg();
	tblDef->setDefaultColGrps();
	m_blogTable->writeTableDef();

	//创建字典
	ILMode metaLockMode, dataLockMode;
	EXCPT_OPER(m_blogTable->createDictionary(session, &metaLockMode, &dataLockMode));
	m_blogTable->unlock(session, dataLockMode);
	m_blogTable->unlockMeta(session, metaLockMode);	

	//optimize
	bool newHadDict;
	bool cancel = false;
	m_blogTable->optimize(session, true, &newHadDict, &cancel);

	InsertBlog insblog1(m_db, m_blogTable, 1000, 50, 5000, 2);
	DeleteEach delblog1(m_db, m_blogTable, 8000, 20);
	UpdateBlogTable updBlog1(m_db, m_blogTable, 7000, 20, 30);

	insblog1.enableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.start();

	int loop = 300;
	while (loop--) {
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}

	insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));


	IndexDef **delindice = new IndexDef *[1];
	u16 num2del = m_blogTable->getTableDef()->m_numIndice - 1;
	IndexDef *index2del = m_blogTable->getTableDef()->m_indice[num2del];
	delindice[0] = new IndexDef(index2del);

	AlterIndice delIdx(m_db, m_blogTable, 0, NULL, 1, (const IndexDef **)delindice);

	//delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);
	delIdx.start();
	//delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);
	// 在获取lsn后进行一些插入操作
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));


	// 删三行。
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.start();
	loop = 3;
	while (loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 更新三行。
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.start();
	loop = 3;
	while (loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);



	//delIdx.notifySyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.enableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND); // 在获得lsn前设置
	delIdx.notifySyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	delIdx.joinSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);

	// 在索引构建完成，获得lsnend之前再插入几条记录
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}

	// 在索引构建完成，获得lsnend之前再删除几条记录
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 在索引构建完成，获得lsnend之前再更新几条记录
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);


	delIdx.notifySyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	delIdx.join();


	assert(m_blogTable->getLock(session) == IL_NO);
	assert(m_blogTable->getMetaLock(session) == IL_NO);


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	//Thread::msleep(15000);



	// 再次增加索引
	Connection *conn2 = m_db->getConnection(false);
	Session *session2 = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn2);

	assert(m_blogTable->getLock(session2) == IL_NO);
	assert(m_blogTable->getMetaLock(session2) == IL_NO);

	AlterIndice addIdx(m_db, m_blogTable, 1, (const IndexDef **)delindice, 0, NULL);
	addIdx.start();
	addIdx.join();

	m_db->getSessionManager()->freeSession(session2);
	m_db->freeConnection(conn2);
	//Thread::msleep(15000);


	delete delindice[0];
	delete [] delindice;

	for (uint i = 0; i < numRows; i++) {
		byte *abs = RecordOper::readLob(redRows[i]->m_data, m_blogTable->getTableDef()->m_columns[5]);
		delete [] abs;
		byte *content = RecordOper::readLob(redRows[i]->m_data, m_blogTable->getTableDef()->m_columns[6]);
		delete [] content;
		freeRecord(redRows[i]);
		freeRecord(rows[i]);
	}

	delete []rows;
	delete []redRows;
	rows = NULL;

	insblog1.disableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.stop();

	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.stop();

	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.stop();
}

void TblMntTestCase::testAlterIndex() {
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);

	InsertBlog insblog1(m_db, m_blogTable, 1000, 50, 0, 2);
	DeleteEach delblog1(m_db, m_blogTable, 8000, 20);
	UpdateBlogTable updBlog1(m_db, m_blogTable, 7000, 20, 30);

	insblog1.enableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.start();

	//delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	//delblog1.start();

	int loop = 300;
	while (loop--) {
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}

	insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));



	//insblog1.stop();
	//delblog1.start();
	//updBlog1.start();


	//Thread::msleep(30000);

	IndexDef **delindice = new IndexDef *[1];
	u16 num2del = m_blogTable->getTableDef()->m_numIndice - 1;
	IndexDef *index2del = m_blogTable->getTableDef()->m_indice[num2del];
	delindice[0] = new IndexDef(index2del);
	//index2del;
	

	AlterIndice delIdx(m_db, m_blogTable, 0, NULL, 1, (const IndexDef **)delindice);

	//delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.enableSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);
	delIdx.start();
	//delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.joinSyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);
	// 在获取lsn后进行一些插入操作
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));


	// 删三行。
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.start();
	loop = 3;
	while (loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 更新三行。
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.start();
	loop = 3;
	while (loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);


	
	//delIdx.notifySyncPoint(SP_TBL_ALTIDX_AFTER_U_METALOCK);
	delIdx.enableSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND); // 在获得lsn前设置
	delIdx.notifySyncPoint(SP_TBL_ALTIDX_AFTER_GET_LSNSTART);

	delIdx.joinSyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);

	// 在索引构建完成，获得lsnend之前再插入几条记录
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}

	// 在索引构建完成，获得lsnend之前再删除几条记录
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 在索引构建完成，获得lsnend之前再更新几条记录
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);


	delIdx.notifySyncPoint(SP_TBL_ALTIDX_BEFORE_GET_LSNEND);
	delIdx.join();


	assert(m_blogTable->getLock(session) == IL_NO);
	assert(m_blogTable->getMetaLock(session) == IL_NO);


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	//Thread::msleep(15000);



	// 再次增加索引
	Connection *conn2 = m_db->getConnection(false);
	Session *session2 = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn2);

	assert(m_blogTable->getLock(session2) == IL_NO);
	assert(m_blogTable->getMetaLock(session2) == IL_NO);

	//delindice[0]->m_online = true;
	AlterIndice addIdx(m_db, m_blogTable, 1, (const IndexDef **)delindice, 0, NULL);
	addIdx.start();
	addIdx.join();

	m_db->getSessionManager()->freeSession(session2);
	m_db->freeConnection(conn2);
	//Thread::msleep(15000);


	delete delindice[0];
	delete [] delindice;


	insblog1.disableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.stop();

	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.stop();

	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.stop();
}

void TblMntTestCase::testOptimizeTable() {
#if 0
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);

	InsertBlog insblog1(m_db, m_blogTable, 1000, 50, 0, 2);
	DeleteEach delblog1(m_db, m_blogTable, 8000, 15);
	UpdateBlogTable updBlog1(m_db, m_blogTable, 7000, 20, 30);


	insblog1.start();

	Thread::msleep(30000);


	//insblog1.stop();
	delblog1.start();
	updBlog1.start();

	Thread::msleep(5000);

	/*
	TblMntOptimize optimize(m_blogTable, conn);
	try {
	optimize.alterTable(session);
	} catch (NtseException &e) {
	assert(false);
	}
	*/
	TblMntAlterColumn optimize(m_blogTable, conn, 0, NULL, 0, NULL);
	try {
		optimize.alterTable(session);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		assert(false);
	}

	/* 临时代码 */
	assert(m_blogTable->getLock(session) == IL_NO);
	assert(m_blogTable->getMetaLock(session) == IL_NO);
	//m_blogTable->unlockMeta(session, IL_X);
	//m_blogTable->unlock(session, IL_X);


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	Thread::msleep(20000);

	delblog1.stop();
	updBlog1.stop();
	insblog1.stop();
#endif
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);

	InsertBlog insblog1(m_db, m_blogTable, 1000, 50, 0, 2);
	DeleteEach delblog1(m_db, m_blogTable, 8000, 15);
	UpdateBlogTable updBlog1(m_db, m_blogTable, 7000, 20, 30);


	insblog1.enableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.start();

	// 插1000条记录先
	int loop = 300;
	while (loop--) {
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}
	insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));




	//insblog1.stop();
	//delblog1.start();
	//updBlog1.start();

	//Thread::msleep(10000);

	/*
	ColumnDef **delCols = new ColumnDef *[3];
	delCols[0] = m_blogTable->getTableDef()->m_columns[3];
	delCols[1] = m_blogTable->getTableDef()->m_columns[4];
	delCols[2] = m_blogTable->getTableDef()->m_columns[6];

	AddColumnDef *addCols = new AddColumnDef[2];
	// Ranking
	addCols[0].m_addColDef = new ColumnDef("Ranking", CT_SMALLINT);
	addCols[0].m_position = 0 + 1; // 加在ID之后
	u16 defaultRank = 1;
	addCols[0].m_defaultValue = &defaultRank;
	addCols[0].m_valueLength = sizeof(u16);
	// Footprint
	addCols[1].m_addColDef = new ColumnDef("Footprint", CT_SMALLLOB);
	addCols[1].m_position = (u16)-1; // 放最后
	char *footprint = "no comment here";
	addCols[1].m_defaultValue = (void *)footprint;
	addCols[1].m_valueLength = strlen(footprint) + 1;
	*/

	/*
	TblMntAlterColumn alterCol(m_blogTable, conn, 2, addCols, 3, (const ColumnDef **)delCols);
	try {
	alterCol.alterTable(session);
	} catch (NtseException &e) {
	UNREFERENCED_PARAMETER(e);
	assert(false);
	}
	*/
	AlterColumn altCol(m_db, m_blogTable, 0, NULL, 0, NULL);
	altCol.enableSyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);
	altCol.start();
	altCol.joinSyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);

	// 在获取lsn后进行一些插入操作
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));

	// 删三行。
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.start();
	loop = 3;
	while (loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 更新三行。
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.start();
	loop = 3;
	while (loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);

	// 获得lsnend前停下
	altCol.enableSyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNEND);
	altCol.notifySyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);
	altCol.joinSyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNEND);


	// 在表复制构建完成，获得lsnend之前再插入几条记录
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}

	// 在索引构建完成，获得lsnend之前再删除几条记录
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 在索引构建完成，获得lsnend之前再更新几条记录
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);


	// 等待维护线程结束
	altCol.notifySyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNEND);
	altCol.join();

	/* 临时代码 */
	assert(m_blogTable->getLock(session) == IL_NO);
	assert(m_blogTable->getMetaLock(session) == IL_NO);


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	//delete [] delCols;
	Thread::msleep(3000);

	insblog1.disableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.stop();

	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.stop();

	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.stop();


	//delete addCols[0].m_addColDef;
	//delete addCols[1].m_addColDef;
	//delete [] addCols;
}

/**
* 备份一个文件
* @param backupHeapFile	备份到的文件
* @param origFile			源文件
*/
static void backupAFile(char *backupHeapFile, char *origFile) {
	u64 errCode;
	errCode = File::copyFile(backupHeapFile, origFile, true);
	if (File::getNtseError(errCode) != File::E_NO_ERROR) {
		cout << File::explainErrno(errCode) << endl;
		return;
	}
}

/**
* 备份一组NTSE数据库文件
* @param path	备份文件路径
* @param tableName	表名
* @param useMms		是否使用mms
* @param backup true表示备份过程，false表示恢复备份文件
*/
static void backupFiles(const char *path, const char *tableName, bool backup, const char **postfix, uint postnum) {
	for (uint i = 0; i < postnum; i++) {
		char origFileName[255];
		char bkFileName[255];

		sprintf(origFileName, "%s/%s%s", path, tableName, postfix[i]);
		sprintf(bkFileName, "%s/%s%s.bak", path, tableName, postfix[i]);

		if (backup)
			backupAFile(bkFileName, origFileName);
		else
			backupAFile(origFileName, bkFileName);
	}
}

static void deleteBackupFiles(const char *path, const char *tableName, const char **postfix, uint postnum) {
	for (uint i = 0; i < postnum; i++) {
		char bkFileName[255];
		sprintf(bkFileName, "%s/%s%s.bak", path, tableName, postfix[i]);

		File bkFile(bkFileName);
		bkFile.remove();
	}
}

void TblMntTestCase::testRedoAlterIndex() {
	const char *postfix[] = {".nsd", ".nsi", ".nsso", ".nsld", ".nsli", ".tmpnsi"};
	int postfixnum = sizeof(postfix) / sizeof(postfix[0]);


	SyncPoint sp4Test[] =
	{
		SP_MNT_ALTERINDICE_JUST_WRITE_LOG,
		SP_MNT_ALTERINDICE_JUST_WRITE_TABLEDEF,
		SP_MNT_ALTERINDICE_INDICE_REPLACED,
		//SP_MAX
	};
	tearDown();

	u16 blogTid = 0;

	for (int i = 0; i < sizeof(sp4Test) / sizeof(sp4Test[0]); ++i) {
		setUp();

		m_db->setCheckpointEnabled(false);

		EXCPT_OPER(createBlogTable(true, true));
		EXCPT_OPER(m_blogTable = openTable("Blog"));

		blogTid = m_blogTable->getTableDef()->m_id;

		string tablename(m_blogTable->getTableDef()->m_name);

		cout << m_db->getConfig()->m_basedir + (NTSE_PATH_SEP + tablename) << endl;
		cout << m_db->getControlFile()->getTablePath(blogTid) << endl;


		deleteBackupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), postfix, postfixnum);

		InsertBlog insblog1(m_db, m_blogTable, 1000, 200, 0, 2);
		DeleteEach delblog1(m_db, m_blogTable, 5000, 10);
		UpdateBlogTable updBlog1(m_db, m_blogTable, 10000, 30, 50);

		insblog1.start();

		Thread::msleep(1000);
		insblog1.stop();
		//delblog1.start();
		//updBlog1.start();

		//Thread::msleep(5000);

		IndexDef **delindice = new IndexDef *[1];
		u16 num2del = m_blogTable->getTableDef()->m_numIndice - 1;
		IndexDef *index2del = m_blogTable->getTableDef()->m_indice[num2del];
		delindice[0] = new IndexDef(index2del);
		//index2del;
		AlterIndice delIndice(m_db, m_blogTable, 0, NULL, 1, (const IndexDef **)delindice);
		delIndice.enableSyncPoint(sp4Test[i]);

		delIndice.start();

		delIndice.joinSyncPoint(sp4Test[i]);


		// 拷贝数据库
		//string basePath(string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_blogTable->getTableDef()->m_name);

		backupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), true, postfix, postfixnum);



		/** 关闭表 */
		delIndice.disableSyncPoint(sp4Test[i]);
		delIndice.notifySyncPoint(sp4Test[i]);
		delIndice.join();

		//Thread::msleep(5000);

		delete delindice[0];
		delete [] delindice;

		//insblog1.stop();
		//delblog1.stop();
		//updBlog1.stop();

		closeTable(m_blogTable, true);
		m_blogTable = NULL;

		// restore
		backupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), false, postfix, postfixnum);

		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("testRedoAlterIndex", conn);

		while (m_db->getTxnlog()->getNext(logHdl)) {
			if (logHdl->logEntry()->m_logType == LOG_ALTER_INDICE) {

				cout << "redo Log alter indice" << endl;
				//string tablepath(m_db->getConfig()->m_basedir + (NTSE_PATH_SEP + tablename));
				//m_db->redoTblMntAlterIndice(session, logHdl->logEntry(), tablepath.c_str());
				//cout << m_db->getConfig()->m_basedir + (NTSE_PATH_SEP + tablename) << endl;
				//cout << m_db->getControlFile()->getTablePath(blogTid) << endl;
				try {
					Table::redoAlterIndice(session, logHdl->logEntry(), m_db, tablename.c_str());
				} catch (NtseException &e) {
					UNREFERENCED_PARAMETER(e);
				}
			}
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->getTxnlog()->endScan(logHdl);





		deleteBackupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), postfix, postfixnum);

		m_db->setCheckpointEnabled(true);
		tearDown();
	}

	setUp();
}


void TblMntTestCase::testRedoAlterColumn() {
	const char *postfix[] = {
		".nsd", ".nsi", ".nsso", ".nsld", ".nsli",
		".mapping.nsd", ".mapping.nsi",
		"_oltmp_.nsd", "_oltmp_.nsi", "_oltmp_.nsso", "_oltmp_.nsld", "_oltmp_.nsli"
	};

	int postfixnum = sizeof(postfix) / sizeof(postfix[0]);

	SyncPoint sp4Test[] =
	{
		SP_MNT_ALTERCOLUMN_JUST_WRITE_LOG,
		SP_MNT_ALTERCOLUMN_TABLE_REPLACED,
		//SP_MAX
	};
	tearDown();

	for (int i = 0; i < sizeof(sp4Test) / sizeof(sp4Test[0]); ++i) {
		setUp();

		m_db->setCheckpointEnabled(false);

		EXCPT_OPER(createBlogTable(true, true));
		EXCPT_OPER(m_blogTable = openTable("Blog"));

		string tablename(m_blogTable->getTableDef()->m_name);
		deleteBackupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), postfix, postfixnum);

		InsertBlog insblog1(m_db, m_blogTable, 1000, 200, 0, 2);
		DeleteEach delblog1(m_db, m_blogTable, 8000, 15);
		UpdateBlogTable updBlog1(m_db, m_blogTable, 7000, 30, 30);

		insblog1.start();

		Thread::msleep(1000);
		insblog1.stop();
		//delblog1.start();
		//updBlog1.start();

		//Thread::msleep(5000);


		// TODO
		//AlterIndice delIndice(m_db, m_blogTable, 0, NULL, 1, (const IndexDef **)delindice);
		AlterColumn optimize(m_db, m_blogTable);

		optimize.enableSyncPoint(sp4Test[i]);

		optimize.start();

		optimize.joinSyncPoint(sp4Test[i]);


		// 拷贝数据库
		//string basePath(string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_blogTable->getTableDef()->m_name);

		backupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), true, postfix, postfixnum);



		/** 关闭表 */
		optimize.disableSyncPoint(sp4Test[i]);
		optimize.notifySyncPoint(sp4Test[i]);
		optimize.join();

		//Thread::msleep(5000);


		//insblog1.stop();
		//delblog1.stop();
		//updBlog1.stop();

		closeTable(m_blogTable, true);
		m_blogTable = NULL;

		// restore
		backupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), false, postfix, postfixnum);

		LogScanHandle *logHdl = m_db->getTxnlog()->beginScan(Txnlog::MIN_LSN, Txnlog::MAX_LSN);
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("testRedoAlterColumn", conn);

		while (m_db->getTxnlog()->getNext(logHdl)) {
			if (logHdl->logEntry()->m_logType == LOG_ALTER_COLUMN) {

				cout << "redo Log alter column" << endl;
				try {
					bool newHasDict = false;
					Table::redoAlterColumn(session, logHdl->logEntry(), m_db, tablename.c_str(), &newHasDict);
				} catch (NtseException &e) {
					UNREFERENCED_PARAMETER(e);
				}
			}
		}

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
		m_db->getTxnlog()->endScan(logHdl);





		deleteBackupFiles(m_db->getConfig()->m_basedir, tablename.c_str(), postfix, postfixnum);

		m_db->setCheckpointEnabled(true);
		tearDown();
	}

	setUp();
}

void TblMntTestCase::testNtseRidMapping() {

	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);
	m_blogTable->lockMeta(session, IL_S, -1, __FILE__, __LINE__);

	NtseRidMapping *mapping = new NtseRidMapping(m_db, m_blogTable);
	try {
		mapping->init(session);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		delete mapping;
		assert(false);
	}

	u64 diff = 100000000;

	mapping->beginBatchLoad();
	for (u64 rid = 1; rid <= 1000; ++rid) {
		mapping->insertMapping(rid, rid + diff);
	}
	mapping->endBatchLoad();

	assert(mapping->getMapLockMode() == IL_NO);
	assert(mapping->getMapMetaLockMode() == IL_NO);

	// 测试成功查询
	for (u64 rid = 1; rid <= 100; ++rid) {
		u64 mapped = mapping->getMapping(rid);
		CPPUNIT_ASSERT(mapped == rid + diff);
	}

	assert(mapping->getMapLockMode() == IL_NO);
	assert(mapping->getMapMetaLockMode() == IL_NO);


	// 测试失败查询
	for (u64 rid = INVALID_ROW_ID / 2; rid <= INVALID_ROW_ID / 2 + 100; ++rid) {
		u64 mapped = mapping->getMapping(rid);
		CPPUNIT_ASSERT(mapped == INVALID_ROW_ID);
	}

	assert(mapping->getMapLockMode() == IL_NO);
	assert(mapping->getMapMetaLockMode() == IL_NO);


	// 删除测试
	for (u64 rid = 400; rid < 500; ++rid) {
		mapping->deleteMapping(rid);
	}
	for (u64 rid = 400; rid < 500; ++rid) {
		u64 mapped = mapping->getMapping(rid);
		CPPUNIT_ASSERT(mapped == INVALID_ROW_ID);
		mapped = mapping->getMapping(rid + 100);
		CPPUNIT_ASSERT(mapped == rid + 100 + diff);
	}

	assert(mapping->getMapLockMode() == IL_NO);
	assert(mapping->getMapMetaLockMode() == IL_NO);


	// 遍历测试
	/*
	mapping->startIter();
	const int n = 3;
	int query = n;
	u64 o[n], m[n];

	u64 orig, mapped;
	while (orig = mapping->getNextOrig(&mapped)) {
		if (query) {
			//CPPUNIT_ASSERT(mapping->getOrig(mapped) == orig);
			--query;
			o[query] = orig;
			m[query] = mapped;
		}
	}
	mapping->endIter();

	for (int i = 0; i < n; ++i) {
		CPPUNIT_ASSERT(mapping->getOrig(m[i]) == o[i]);
	}
	*/

	for (int i = 0; i < 10; ++i) {
		u64 mapped = diff + 100 + i;
		u64 orig = mapping->getOrig(mapped);
		CPPUNIT_ASSERT(orig == 100 + i);
	} 


	u64 total = mapping->getCount();
	CPPUNIT_ASSERT(total > 0);

#ifdef WIN32
	NtseRidMapping *newmapping = new NtseRidMapping(m_db, m_blogTable);
	try {
		newmapping->init(session);
		CPPUNIT_ASSERT(false);
	} catch (NtseException &) {
		delete newmapping;
		CPPUNIT_ASSERT(true);
	}
#endif

	delete mapping;

	m_blogTable->unlockMeta(session, IL_S);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void TblMntTestCase::testRedoAlterColumnDelLob() {
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);


	ColumnDef **delCols = new ColumnDef *[1];
	delCols[0] = m_blogTable->getTableDef()->m_columns[5];
/*	delCols[1] = m_blogTable->getTableDef()->m_columns[6];*/


	AlterColumn altCol1(m_db, m_blogTable, 0, NULL, 1, (const ColumnDef **)delCols);
	altCol1.enableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_SCAN_IS_LOCK);
	altCol1.start();

	altCol1.joinSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_SCAN_IS_LOCK);


	// 写锁表
	m_blogTable->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	m_blogTable->lock(session, IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	altCol1.disableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_SCAN_IS_LOCK);
	altCol1.enableSyncPoint(SP_MNT_ALTERCOLUMN_SCAN_IS_LOCK_FAIL);
	altCol1.notifySyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_SCAN_IS_LOCK);
	altCol1.joinSyncPoint(SP_MNT_ALTERCOLUMN_SCAN_IS_LOCK_FAIL);

	m_blogTable->unlock(session, IL_X);
	m_blogTable->unlockMeta(session, IL_S);

	altCol1.disableSyncPoint(SP_MNT_ALTERCOLUMN_SCAN_IS_LOCK_FAIL);
	altCol1.enableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);
	altCol1.notifySyncPoint(SP_MNT_ALTERCOLUMN_SCAN_IS_LOCK_FAIL);
	altCol1.joinSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);

	// 写锁表
	m_blogTable->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	m_blogTable->lock(session, IL_X, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	altCol1.disableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);
	//altCol1.enableSyncPoint(SP_MNT_ALTERCOLUMN_S_LOCKTABLE_FAIL);
	altCol1.notifySyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_S_LOCKTABLE);
	//altCol1.joinSyncPoint(SP_MNT_ALTERCOLUMN_S_LOCKTABLE_FAIL);

	m_blogTable->unlock(session, IL_X);
	m_blogTable->unlockMeta(session, IL_S);

	//altCol1.disableSyncPoint(SP_MNT_ALTERCOLUMN_S_LOCKTABLE_FAIL);
	//altCol1.enableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_X_METALOCK);
	//altCol1.notifySyncPoint(SP_MNT_ALTERCOLUMN_S_LOCKTABLE_FAIL);
	//altCol1.joinSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_X_METALOCK);

	m_blogTable->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);

	//altCol1.disableSyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_X_METALOCK);
	altCol1.enableSyncPoint(SP_MNT_ALTERCOLUMN_X_METALOCK_FAIL);
	//altCol1.notifySyncPoint(SP_MNT_ALTERCOLUMN_BEFORE_X_METALOCK);
	altCol1.joinSyncPoint(SP_MNT_ALTERCOLUMN_X_METALOCK_FAIL);

	m_blogTable->unlockMeta(session, IL_S);

	altCol1.disableSyncPoint(SP_MNT_ALTERCOLUMN_X_METALOCK_FAIL);
	//altCol1.enableSyncPoint();
	altCol1.notifySyncPoint(SP_MNT_ALTERCOLUMN_X_METALOCK_FAIL);




	altCol1.join();

	//m_db->getSessionManager()->freeSession(session);
	//m_db->freeConnection(conn);

	delete [] delCols;
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

}

void TblMntTestCase::testAlterColumnFLR() {

}



void TblMntTestCase::testAlterColumnException() {
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);

	InsertBlog insblog1(m_db, m_blogTable, 1000, 50, 0, 2);
	insblog1.enableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.start();

	// 插1000条记录先
	int loop = 300;
	while (loop--) {
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}
	insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));


	ColumnDef **delCols = new ColumnDef *[1];
	delCols[0] = m_blogTable->getTableDef()->m_columns[3];
// 	delCols[1] = m_blogTable->getTableDef()->m_columns[4];
// 	delCols[2] = m_blogTable->getTableDef()->m_columns[6];

	//AddColumnDef *addCols = new AddColumnDef[2];
	AddColumnDef *addCols = new AddColumnDef[3];
	// Ranking
	addCols[0].m_addColDef = new ColumnDef("Ranking", CT_SMALLINT);
	addCols[0].m_position = 0 + 1; // 加在ID之后
	u16 defaultRank = 1;
	addCols[0].m_defaultValue = &defaultRank;
	addCols[0].m_valueLength = sizeof(u16);
	// Footprint
	addCols[1].m_addColDef = new ColumnDef("Footprint", CT_SMALLLOB);
	addCols[1].m_position = (u16)-1; // 放最后
	char *footprint = "no comment here";
	addCols[1].m_defaultValue = (void *)footprint;
	addCols[1].m_valueLength = strlen(footprint) + 1;

	// nothing
	addCols[2].m_position = 0;

	AlterColumn altCol1(m_db, m_blogTable, 2, addCols, 1, (const ColumnDef **)delCols);
	altCol1.enableSyncPoint(SP_MNT_ALTERCOLUMN_TMPTABLE_CREATED);
	altCol1.start();
	altCol1.joinSyncPoint(SP_MNT_ALTERCOLUMN_TMPTABLE_CREATED);

	string tablePath = string(m_blogTable->getPath());
	string basePath = string(m_db->getConfig()->m_basedir);
	string tableFullPath = basePath + NTSE_PATH_SEP + tablePath;
	// 得到临时表路径
	string tmpTablePath = tablePath + TEMP_PATH_SEMIEXT; // 相对于baseDir的路径
	Table::drop(m_db, tmpTablePath.c_str());

	altCol1.notifySyncPoint(SP_MNT_ALTERCOLUMN_TMPTABLE_CREATED);
	altCol1.join();


	AlterColumn altCol2(m_db, m_blogTable, 8, addCols, 3, (const ColumnDef **)delCols);
	altCol2.start();
	altCol2.join();


	insblog1.disableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.stop();


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	
	delete [] delCols;
	delete addCols[0].m_addColDef;
	delete addCols[1].m_addColDef;
	delete [] addCols;
}

void TblMntTestCase::testAlterColumn() {
	EXCPT_OPER(createBlogTable(true, true));
	EXCPT_OPER(m_blogTable = openTable("Blog"));

	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("TblMntTestCase::testLogReplay", conn);

	InsertBlog insblog1(m_db, m_blogTable, 1000, 50, 0, 2);
	DeleteEach delblog1(m_db, m_blogTable, 8000, 15);
	UpdateBlogTable updBlog1(m_db, m_blogTable, 7000, 20, 30);


	insblog1.enableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.start();

	// 插1000条记录先
	int loop = 300;
	while (loop--) {
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}
	insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));




	//insblog1.stop();
	//delblog1.start();
	//updBlog1.start();

	//Thread::msleep(10000);

	ColumnDef **delCols = new ColumnDef *[1];
	delCols[0] = m_blogTable->getTableDef()->m_columns[3];
//  	delCols[1] = m_blogTable->getTableDef()->m_columns[4];
//  	delCols[2] = m_blogTable->getTableDef()->m_columns[6];

	AddColumnDef *addCols = new AddColumnDef[2];
	// Ranking
	addCols[0].m_addColDef = new ColumnDef("Ranking", CT_SMALLINT);
	addCols[0].m_position = 0 + 1; // 加在ID之后
	u16 defaultRank = 1;
	addCols[0].m_defaultValue = &defaultRank;
	addCols[0].m_valueLength = sizeof(u16);
	// Footprint
	addCols[1].m_addColDef = new ColumnDef("Footprint", CT_SMALLLOB);
	addCols[1].m_position = (u16)-1; // 放最后
	char *footprint = "no comment here";
	addCols[1].m_defaultValue = (void *)footprint;
	addCols[1].m_valueLength = strlen(footprint) + 1;


	/*
	TblMntAlterColumn alterCol(m_blogTable, conn, 2, addCols, 3, (const ColumnDef **)delCols);
	try {
		alterCol.alterTable(session);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		assert(false);
	}
	*/
	AlterColumn altCol(m_db, m_blogTable, 2, addCols, 1, (const ColumnDef **)delCols);
	altCol.enableSyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);
	altCol.start();
	altCol.joinSyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);

	// 在获取lsn后进行一些插入操作
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}
	CPPUNIT_ASSERT(insblog1.isWaitingSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA));

	// 删三行。
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.start();
	loop = 3;
	while (loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 更新三行。
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.start();
	loop = 3;
	while (loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);

	// 获得lsnend前停下
	altCol.enableSyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNEND);
	altCol.notifySyncPoint(SP_TBL_ALTCOL_AFTER_GET_LSNSTART);
	altCol.joinSyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNEND);


	// 在表复制构建完成，获得lsnend之前再插入几条记录
	loop = 100;
	while (loop--) {
		insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
		insblog1.joinSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	}

	// 在索引构建完成，获得lsnend之前再删除几条记录
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
		delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	}
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.enableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_DELETEROW);
	delblog1.joinSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);

	// 在索引构建完成，获得lsnend之前再更新几条记录
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	loop = 3;
	while(loop--) {
		updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
		updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	}
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.enableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UPDATEROW);
	updBlog1.joinSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);


	// 等待维护线程结束
	altCol.notifySyncPoint(SP_TBL_ALTCOL_BEFORE_GET_LSNEND);
	altCol.join();

	/* 临时代码 */
	assert(m_blogTable->getLock(session) == IL_NO);
	assert(m_blogTable->getMetaLock(session) == IL_NO);


	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	delete [] delCols;
	Thread::msleep(3000);

	insblog1.disableSyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.notifySyncPoint(SP_TBL_MASSINSERTBLOG_AFTER_UNLOCKMETA);
	insblog1.stop();

	delblog1.disableSyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.notifySyncPoint(SP_TBL_SCANDELETE_AFTER_UNLOCKMETA);
	delblog1.stop();

	updBlog1.disableSyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.notifySyncPoint(SP_TBL_SCANUPDATE_AFTER_UNLOCKMETA);
	updBlog1.stop();


	delete addCols[0].m_addColDef;
	delete addCols[1].m_addColDef;
	delete [] addCols;
}

bool DataChecker::checkIndiceWithHeap(Session *sessionhp, Database *db, DrsHeap *heap, DrsIndice *indice, TableDef *tblDef) {
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("DataChecker::checkIndexWithHeap", conn);
	u16 *columns = DataChecker::getAllColumns(tblDef);
	SubRecord *hpSb = new SubRecord(REC_REDUNDANT, tblDef->m_numCols, DataChecker::getAllColumns(tblDef), new byte[tblDef->m_maxRecSize], tblDef->m_maxRecSize);

	// check index to heap;
	SubRecord *idxSb = new SubRecord(REC_REDUNDANT, tblDef->m_numCols, columns, new byte[tblDef->m_maxRecSize], tblDef->m_maxRecSize);
	for (u16 idxno = 0; idxno < tblDef->m_numIndice; ++idxno) {
		DrsIndex *index = indice->getIndex(idxno);
		IndexDef *indexDef = tblDef->m_indice[idxno];
		RowLockHandle *rlh;
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tblDef, indexDef, indexDef->m_numCols, indexDef->m_columns,
			idxSb->m_numCols, idxSb->m_columns, KEY_COMPRESS, REC_REDUNDANT, 1000);
		IndexScanHandle *idxHandle = index->beginScan(session, NULL, true, true, Shared, &rlh, extractor);
		assert(idxHandle);
		SubrecExtractor *srExtractor = SubrecExtractor::createInst(session, tblDef, hpSb);
		while (index->getNext(idxHandle, idxSb)) {
			RowId rowId = idxHandle->getRowId();
			bool exist = heap->getSubRecord(sessionhp, rowId, srExtractor, hpSb);
			/*
			if (exist) {
			if (tableDef->m_recFormat == REC_FIXLEN) {
			redRec.m_rowId = rowId;
			redRec.m_data = 
			}
			}*/
			if (!exist || !DataChecker::checkKeyToRecord(tblDef, indexDef, idxSb->m_data, hpSb->m_data)) {
				session->unlockRow(&rlh);
				index->endScan(idxHandle);
				cout << "Check fail: " << indexDef->m_name << " has a key rowid " << rowId << ", which can't be found in heap." << endl;
				freeSubRecord(idxSb);
				freeSubRecord(hpSb);
				db->getSessionManager()->freeSession(session);
				db->freeConnection(conn);
				return false;
			}
			session->unlockRow(&rlh);
		}
		index->endScan(idxHandle);
	}

	// TODO: check heap to indice;
	DrsHeapScanHandle *hpHandle = heap->beginScan(session, SubrecExtractor::createInst(session, tblDef, hpSb), None, NULL, false);
	hpSb->m_rowId = INVALID_ROW_ID;
	while (heap->getNext(hpHandle, hpSb)) {
		assert(hpSb->m_rowId != INVALID_ROW_ID);
		for (u16 idxno = 0; idxno < indice->getIndexNum(); ++idxno) {
			IndexDef *indexDef = tblDef->m_indice[idxno];
			SubRecord *findKey = DataChecker::formIdxKeyFromData(tblDef, indexDef, hpSb->m_rowId, hpSb->m_data);
			findKey->m_rowId = hpSb->m_rowId;
			DrsIndex *index = indice->getIndex(idxno);
			RowId idxRid;
			bool got = index->getByUniqueKey(session, findKey, None, &idxRid, NULL, NULL, NULL);
			if (!got) {
				//table->endScan(tblhandle);
				heap->endScan(hpHandle);
				//db->getSessionManager()->freeSession(sessiontbl);
				//db->freeConnection(tblConn);
				db->getSessionManager()->freeSession(session);
				db->freeConnection(conn);
				cout << "Check fail: " << indexDef->m_name << " has a record rowid " << findKey->m_rowId << ", which can't be found in Index." << endl;
				freeSubRecord(findKey);
				freeSubRecord(idxSb);
				freeSubRecord(hpSb);
				return false;
			}
			assert(idxRid == findKey->m_rowId);
			freeSubRecord(findKey);
		}
	}
	heap->endScan(hpHandle);

	freeSubRecord(idxSb);
	freeSubRecord(hpSb);
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
	return true;

}

bool DataChecker::checkIndiceWithTable(Session *sessiontbl, Database *db, Table *table, DrsIndice *indice, TableDef *indiceTbdef) {
	//TableDef *tableDef = table->getTableDef();
	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("DataChecker::checkIndexWithTable", conn);
	byte tblbuf[Limits::PAGE_SIZE];
	memset(tblbuf, 0, Limits::PAGE_SIZE);
	if (NULL == indiceTbdef)
		indiceTbdef = table->getTableDef();
	u16 *columns = DataChecker::getAllColumns(indiceTbdef);

	// check index to table;
	SubRecord *idxSb = new SubRecord(REC_REDUNDANT, indiceTbdef->m_numCols, columns, new byte[indiceTbdef->m_maxRecSize], indiceTbdef->m_maxRecSize);
	for (u16 idxno = 0; idxno < indiceTbdef->m_numIndice; ++idxno) {
		//Connection *tblConn = db->getConnection(false);
		//DrsIndex *index = table->getIndice()->getIndex(idxno);
		DrsIndex *index = indice->getIndex(idxno);
		IndexDef *indexDef = indiceTbdef->m_indice[idxno];
		RowLockHandle *rlh;
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), indiceTbdef, indexDef, indexDef->m_numCols, indexDef->m_columns,
			idxSb->m_numCols, idxSb->m_columns, KEY_COMPRESS, REC_REDUNDANT, 1000);
		IndexScanHandle *idxHandle = index->beginScan(session, NULL, true, true, Shared, &rlh, extractor);
		assert(idxHandle);
		while (index->getNext(idxHandle, idxSb)) {
			RowId rowId = idxHandle->getRowId();
			// 得到table中记录
			//Session *sessiontbl = db->getSessionManager()->allocSession("DataChecker::checkIndexWithTable", tblConn);
			TblScan *tblHandle = NULL;
			while (tblHandle == NULL) {
				try {
					tblHandle = table->positionScan(sessiontbl, OP_READ, indiceTbdef->m_numCols, columns, false);
				} catch (NtseException &e) {
					UNREFERENCED_PARAMETER(e);
					tblHandle = NULL;
				}
			}
			if (!table->getNext(tblHandle, tblbuf, rowId) || !DataChecker::checkKeyToRecord(indiceTbdef, indexDef, idxSb->m_data, tblbuf)) {
				table->endScan(tblHandle);
				//db->getSessionManager()->freeSession(sessiontbl);
				//db->freeConnection(tblConn);
				session->unlockRow(&rlh);
				index->endScan(idxHandle);
				cout << "Check fail: " << indexDef->m_name << " has a key rowid " << rowId << ", which can't be found in table." << endl;
				freeSubRecord(idxSb);
				db->getSessionManager()->freeSession(session);
				db->freeConnection(conn);
				return false;
			}
			// TODO:
			table->endScan(tblHandle);
			//db->getSessionManager()->freeSession(sessiontbl);
			session->unlockRow(&rlh);
		}
		//db->freeConnection(tblConn);
		index->endScan(idxHandle);
	}
	freeSubRecord(idxSb);
	//delete [] columns;

	// check table to index
	columns = DataChecker::getAllColumns(indiceTbdef);
	//Connection *tblConn = db->getConnection(false);
	//Session *sessiontbl = db->getSessionManager()->allocSession("DataChecker::checkIndiceWithTable", tblConn);
	TblScan *tblhandle = table->tableScan(sessiontbl, OP_READ, indiceTbdef->m_numCols, columns, false);
	while (table->getNext(tblhandle, tblbuf)) {
		for (u16 idxno = 0; idxno < indiceTbdef->m_numIndice; ++idxno) {
			IndexDef *indexDef = indiceTbdef->m_indice[idxno];
			SubRecord *findKey = DataChecker::formIdxKeyFromData(indiceTbdef, indexDef, tblhandle->getCurrentRid(), tblbuf);
			findKey->m_rowId = tblhandle->getCurrentRid();
			DrsIndex *index = indice->getIndex(idxno);
			RowId idxRid;
			bool got = index->getByUniqueKey(session, findKey, None, &idxRid, NULL, NULL, NULL);
			if (!got) {
				table->endScan(tblhandle);
				//db->getSessionManager()->freeSession(sessiontbl);
				//db->freeConnection(tblConn);
				db->getSessionManager()->freeSession(session);
				db->freeConnection(conn);
				cout << "Check fail: " << indexDef->m_name << " has a record rowid " << findKey->m_rowId << ", which can't be found in Index." << endl;
				freeSubRecord(findKey);
				return false;
			}
			assert(idxRid == findKey->m_rowId);
			freeSubRecord(findKey);
		}
	}
	table->endScan(tblhandle);
	//db->getSessionManager()->freeSession(sessiontbl);
	//db->freeConnection(tblConn);
	delete [] columns;
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);


	return true;
}

bool DataChecker::checkTableWithTable(Database *db, Connection *conn, Table *orig, Table *conv, NtseRidMapping *ridmap, Session *origSess /* = NULL */, Session *convSess /* = NULL */) {
	if (origSess) {
		bool convUseMms = conv->getTableDef()->m_useMms;
		conv->getTableDef()->m_useMms = false;
		byte *buf = new byte[orig->getTableDef()->m_maxRecSize];
		byte *convBuf = new byte[conv->getTableDef()->m_maxRecSize];
		Record oRec, cRec;
		oRec.m_format = cRec.m_format = REC_MYSQL;
		oRec.m_rowId = cRec.m_rowId = INVALID_ROW_ID;
		oRec.m_data = buf;
		oRec.m_size = orig->getTableDef()->m_maxRecSize;
		cRec.m_data = convBuf;
		cRec.m_size = conv->getTableDef()->m_maxRecSize;

		assert(orig->getLock(origSess) == IL_X);
		assert(orig->getMetaLock(origSess) == IL_X);
		// 首先验证记录数目一样多
		u16 co0 = 0;
		uint origRecNum = 0;
		TblScan *origHdl = orig->tableScan(origSess, OP_READ, 1, &co0, false);
		while (orig->getNext(origHdl, buf))
			++origRecNum;
		orig->endScan(origHdl);
		uint convRecNum = 0;
		bool createConvSess = false;
		if (!convSess)
			createConvSess = true;
		if (createConvSess)
			convSess = db->getSessionManager()->allocSession("convSess", conn);
		//m_table->lock(session, IL_S, db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		conv->lockMeta(convSess, IL_S, -1, __FILE__, __LINE__);
		conv->lock(convSess, IL_S, -1, __FILE__, __LINE__);
		TblScan *convHdl = conv->tableScan(convSess, OP_READ, 1, &co0, false, false);
		while(conv->getNext(convHdl, buf))
			++convRecNum;
		conv->endScan(convHdl);
		conv->unlock(convSess, IL_S);
		conv->unlockMeta(convSess, IL_S);
		if (createConvSess)
			db->getSessionManager()->freeSession(convSess);
		assert(origRecNum == convRecNum);

		struct SameCol {
			u16 m_origCol;
			u16 m_convCol;
		};
		SameCol *sameCol = new SameCol[(orig->getTableDef()->m_numCols > conv->getTableDef()->m_numCols) ? conv->getTableDef()->m_numCols : orig->getTableDef()->m_numCols];
		int same = 0;
		for (u16 i = 0; i < orig->getTableDef()->m_numCols; ++i) {
			ColumnDef *origCol = orig->getTableDef()->m_columns[i];
			for (u16 j = 0; j < conv->getTableDef()->m_numCols; ++j) {
				ColumnDef *convCol = conv->getTableDef()->m_columns[j];
				if (strcmp(origCol->m_name, convCol->m_name) == 0) {
					sameCol[same].m_origCol = i;
					sameCol[same].m_convCol = j;
					++same;
					break;
				}
			}
		}
		assert(same <= orig->getTableDef()->m_numCols && same <= conv->getTableDef()->m_numCols);
		u16* origCols = new u16[same];
		u16* convCols = new u16[same];
		for (int i = 0; i < same; ++i) {
			origCols[i] = sameCol[i].m_origCol;
			convCols[i] = sameCol[i].m_convCol;
		}
		// 遍历ridmap
		ridmap->startIter();
		RowId origRid, mappedRid;
		uint ridmapCnt = 0;
		origHdl = orig->positionScan(origSess, OP_READ, (u16)same, origCols, false);
		if (createConvSess)
			convSess = db->getSessionManager()->allocSession("convSess", conn);
		conv->lockMeta(convSess, IL_S, -1, __FILE__, __LINE__);
		conv->lock(convSess, IL_S, -1, __FILE__, __LINE__);
		convHdl = conv->positionScan(convSess, OP_READ, (u16)same, convCols, false);
		while ((origRid = ridmap->getNextOrig(&mappedRid)) != INVALID_ROW_ID) {
			//
			++ridmapCnt;
			orig->getNext(origHdl, buf, origRid);
			conv->getNext(convHdl, convBuf, mappedRid);
			for (int i = 0; i < same; ++i) {
				ColumnDef *odef = orig->getTableDef()->m_columns[origCols[i]];
				ColumnDef *cdef = conv->getTableDef()->m_columns[convCols[i]];
				assert(odef->m_type == cdef->m_type);
				assert(odef->m_size == cdef->m_size);
				assert(RecordOper::isNullR(orig->getTableDef(), &oRec, origCols[i]) == RecordOper::isNullR(conv->getTableDef(), &cRec, convCols[i]));
				if (!RecordOper::isNullR(orig->getTableDef(), &oRec, origCols[i])) {
					switch (odef->m_type) {
						case CT_SMALLLOB:
						case CT_MEDIUMLOB:
							{
								uint lobsize = RecordOper::readLobSize(buf, odef);
								assert(lobsize == RecordOper::readLobSize(convBuf, cdef));
								byte *olob = RecordOper::readLob(buf, odef);
								byte *clob = RecordOper::readLob(convBuf, cdef);
								assert(0 == memcmp(olob, clob, lobsize));
							}
							break;
						case CT_VARCHAR:
						case CT_VARBINARY:
							{
								assert(odef->m_lenBytes == cdef->m_lenBytes);
								assert(odef->m_lenBytes == 2 || odef->m_lenBytes == 1);
								uint olen = (odef->m_lenBytes == 2) ? *(u16*)(buf + odef->m_offset) : *(u8*)(buf + odef->m_offset);
								uint clen = (cdef->m_lenBytes == 2) ? *(u16*)(convBuf + cdef->m_offset): *(u8*)(convBuf + cdef->m_offset);
								assert(olen == clen);
								assert(0 == memcmp(buf + odef->m_offset + odef->m_lenBytes, convBuf + cdef->m_offset + cdef->m_lenBytes, olen));
							}
							break;
						default:
							assert(0 == memcmp(buf + odef->m_offset, convBuf + cdef->m_offset, odef->m_size));
							break;
					}
				}
			}
		}
		ridmap->endIter();
		assert(ridmapCnt == origRecNum);
		conv->unlock(convSess, IL_S);
		conv->unlockMeta(convSess, IL_S);
		if (createConvSess)
			db->getSessionManager()->freeSession(convSess);

		conv->getTableDef()->m_useMms = convUseMms;

		delete [] sameCol;
		delete [] buf;
		delete [] convBuf;
		delete [] origCols;
		delete [] convCols;
	} else {
		assert(false);
	}
	return true;
}


u16* DataChecker::getAllColumns(TableDef *tableDef) {
	u16 *columns = new u16[tableDef->m_numCols];
	for (u16 i = 0; i < tableDef->m_numCols; i++)
		columns[i] = i;

	return columns;
}


bool DataChecker::checkKeyToRecord(TableDef *tableDef, IndexDef *indexDef, byte *key, byte *record) {
	assert(key != NULL && record != NULL);
	u16 numIdxCols = indexDef->m_numCols;
	for (uint i = 0; i < numIdxCols; i++) {
		u16 colNo = indexDef->m_columns[i];
		if (!checkColumn(tableDef, colNo, key, record))
			return false;
	}

	return true;
}

bool DataChecker::checkColumn(TableDef *tableDef, u16 colNo, byte *cols1, byte *cols2) {
	if (RedRecord::isNull(tableDef, cols1, colNo)) {
		if (!RedRecord::isNull(tableDef, cols2, colNo))
			return false;
		return true;
	} else {
		if (RedRecord::isNull(tableDef, cols2, colNo))
			return false;
	}

	ColumnDef *columnDef = tableDef->m_columns[colNo];
	switch (columnDef->m_type) {
		case CT_BIGINT:
			if (RedRecord::readBigInt(tableDef, cols1, colNo) != RedRecord::readBigInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_INT:
			if (RedRecord::readInt(tableDef, cols1, colNo) != RedRecord::readInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_SMALLINT:
			if (RedRecord::readSmallInt(tableDef, cols1, colNo) != RedRecord::readSmallInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_TINYINT:
			if (RedRecord::readTinyInt(tableDef, cols1, colNo) != RedRecord::readTinyInt(tableDef, cols2, colNo))
				return false;
			break;
		case CT_VARCHAR:
			{
				byte *keyVC = NULL, *recVC = NULL;
				size_t keySize, recSize;
				RedRecord::readVarchar(tableDef, cols1, colNo, (void**)&keyVC, &keySize);
				RedRecord::readVarchar(tableDef, cols2, colNo, (void**)&recVC, &recSize);
				if (keySize != recSize || memcmp((void*)keyVC, (void*)recVC, keySize) != 0) {
					cout << keySize << "	" << recSize << endl;
					for (uint i = 0; i < keySize; i++)
						cout << keyVC[i];
					cout << endl;
					for (uint i = 0; i < recSize; i++)
						cout << recVC[i];
					cout << endl;

					return false;
				}
			}
			break;
		case CT_CHAR:
			{
				byte *keyC, *recC;
				size_t keySize, recSize;
				RedRecord::readChar(tableDef, cols1, colNo, (void**)&keyC, &keySize);
				RedRecord::readChar(tableDef, cols2, colNo, (void**)&recC, &recSize);
				if (keySize != recSize || memcmp((void*)keyC, (void*)recC, keySize) != 0)
					return false;
			}
			break;
		default:
			return true;
	}

	return true;
}

SubRecord* DataChecker::formIdxKeyFromData(TableDef *tableDef, IndexDef *indexDef, RowId rowId, byte *record) {
	Record *newRecord = formRecordFromData(tableDef, rowId, record);
	SubRecord *subRecord = new SubRecord();
	subRecord->m_columns = new u16[indexDef->m_numCols];
	memcpy(subRecord->m_columns, indexDef->m_columns, indexDef->m_numCols * sizeof(u16));
	subRecord->m_data = new byte[indexDef->m_maxKeySize];
	subRecord->m_size = indexDef->m_maxKeySize;
	subRecord->m_numCols = indexDef->m_numCols;
	subRecord->m_format = KEY_PAD;
	subRecord->m_rowId = rowId;
	memset(subRecord->m_data, 0, indexDef->m_maxKeySize);

	RecordOper::extractKeyRP(tableDef, indexDef, newRecord, NULL, subRecord);
	freeRecord(newRecord);
	return subRecord;
}

Record* DataChecker::formRecordFromData(TableDef *tableDef, RowId rowId, byte *record) {
	Record *newRecord = RecordBuilder::createEmptyRecord(rowId, REC_REDUNDANT, tableDef->m_maxRecSize);
	memcpy(newRecord->m_data, record, tableDef->m_maxRecSize);
	return newRecord;
}


int DataChecker::searchIndex(Session *session, DrsIndex *index, const IndexDef *indexDef, SubRecord *key, TableDef *tbdef, u64 *out) {
	assert(key->m_format == KEY_PAD);
	u64 sp = session->getMemoryContext()->setSavepoint();
	SubRecord subRec;
	subRec.m_format = REC_REDUNDANT;
	subRec.m_numCols = key->m_numCols;
	subRec.m_columns = key->m_columns;
	subRec.m_data = (byte *)session->getMemoryContext()->alloc(Limits::DEF_MAX_REC_SIZE);
	subRec.m_rowId = INVALID_ROW_ID;
	subRec.m_size = Limits::DEF_MAX_REC_SIZE;
	SubRecord ckey;
	ckey.m_format = KEY_COMPRESS;
	ckey.m_numCols = key->m_numCols;
	ckey.m_columns = key->m_columns;
	ckey.m_data = (byte *)session->getMemoryContext()->alloc(Limits::DEF_MAX_REC_SIZE);
	ckey.m_rowId = INVALID_ROW_ID;
	ckey.m_size = Limits::DEF_MAX_REC_SIZE;
	RecordOper::convertKeyPC(tbdef, indexDef, key, &ckey);

	SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tbdef, indexDef, indexDef->m_numCols, indexDef->m_columns,
		subRec.m_numCols, subRec.m_columns, KEY_COMPRESS, REC_REDUNDANT, 1000);
	IndexScanHandle *hdl = index->beginScan(session, key, true, true, None, NULL, extractor);
	int count = 0;
	while (index->getNext(hdl, &subRec)) {
		assert(hdl->getRowId() != INVALID_ROW_ID);
		if (0 == RecordOper::compareKeyRC(tbdef, &subRec, &ckey, indexDef)) {
			++count;
			if (out)
				*out = hdl->getRowId();
		}
		else
			break;
	}
	index->endScan(hdl);
	session->getMemoryContext()->resetToSavepoint(sp);
	return count;
}

bool DataChecker::checkIndiceUnique(Session *session, const TableDef *tableDef, DrsIndice *indice, IndexDef **indiceDef) {
	u64 savepoint = session->getMemoryContext()->setSavepoint();
	uint idxnum = indice->getIndexNum();
	SubRecord preSub, curSub;
	preSub.m_format = curSub.m_format = REC_REDUNDANT;
	preSub.m_data = (byte *)session->getMemoryContext()->alloc(Limits::DEF_MAX_REC_SIZE);
	curSub.m_data = (byte *)session->getMemoryContext()->alloc(Limits::DEF_MAX_REC_SIZE);
	preSub.m_size = curSub.m_size = Limits::DEF_MAX_REC_SIZE;
	//bool hasPre;

	for (uint i = 0; i < idxnum; ++i) {
		if (!indiceDef[i]->m_unique) continue;

		DrsIndex *idx = indice->getIndex(i);
		bool hasPre = false;

		preSub.m_numCols = curSub.m_numCols = indiceDef[i]->m_numCols;
		preSub.m_columns = curSub.m_columns = indiceDef[i]->m_columns;
		memset(preSub.m_data, 0, Limits::DEF_MAX_REC_SIZE);
		memset(curSub.m_data, 0, Limits::DEF_MAX_REC_SIZE);
		IndexDef *indexDef = indiceDef[i];
		SubToSubExtractor *extractor = SubToSubExtractor::createInst(session->getMemoryContext(), tableDef, indexDef, indexDef->m_numCols, indexDef->m_columns,
			curSub.m_numCols, curSub.m_columns, KEY_COMPRESS, REC_REDUNDANT, 1000);
		IndexScanHandle *hdl = idx->beginScan(session, NULL, true, true, None, NULL, extractor);
		while (idx->getNext(hdl, &curSub)) {
			curSub.m_rowId = hdl->getRowId();
			if (hasPre) {
				assert(0 != memcmp(curSub.m_data, preSub.m_data, Limits::DEF_MAX_REC_SIZE));
				assert(curSub.m_rowId != preSub.m_rowId);
			} else 
				hasPre = true;
			// 换
			byte *tmp = curSub.m_data;
			curSub.m_data = preSub.m_data;
			preSub.m_data = tmp;
			preSub.m_rowId = curSub.m_rowId;
		}
		idx->endScan(hdl);
	}

	session->getMemoryContext()->resetToSavepoint(savepoint);
	return true;
}