/**
 * ���ߴ����������Թ���
 *
 * @author ������(niemingjun@corp.netease.com, niemingjun@163.org)
 */

#include "api/TestLobDefraggler.h"
#include "api/TestTableArgAlter.h"

#include "Test.h"
#include "misc/RecordHelper.h"
#include <vector>
using namespace std;

using namespace ntse;

/** 
 * ������������
 */
const char* LobDefragglerTestCase::getName() {
	return "Table lob defrag Test.";
}

/**
 * ��������������
 */
const char* LobDefragglerTestCase::getDescription() {
	return "Test defrag table lobs.";
}

/** 
 * �������͡�
 *
 * @return false, С�͵�Ԫ���ԡ�
 */
bool LobDefragglerTestCase::isBig() {
	return false;
}
/** 
 * Set up context before start a test.
 *
 * @see <code>TestFixture::setUp()</code>
 */
void LobDefragglerTestCase::setUp() {
	m_db = TableAlterArgTestCase::initDb();
	TableAlterArgTestCase::createBlog(m_db, true);
}

/** 
 * Clean up after the test run.
 *
 * @see <code>TestFixture::tearDown()</code>
 */
void LobDefragglerTestCase::tearDown() {
	TableAlterArgTestCase::destroyDb(m_db);
}

/** 
 * ���Դ��������
 * ������Է������ֱ��Ǵ����ѹ���ͷ�ѹ���������
 * 1. ����NUM_LOB_INSERT ��������СΪ LOB_SIZE_MIN �� LOB_SIZE_MAX �Ĵ����
 * 2. ɾ����¼��
 * 3. ִ�д��������
 * 4. �Ա�����һ���ԡ�
 *  
 */
void LobDefragglerTestCase::testDefrag() {
	assert(NULL != m_db);

	//����defrag֮ǰ������
	vector< pair<byte *, uint> > lobVector;

	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);	
	Table *table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + TableAlterArgTestCase::DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	assert(NULL != table);
	for (int i = 0; i < 2; i++)	{
		lobVector.clear();
		const char *boolStr = "FALSE";
		if (i > 0) {
			boolStr = "TRUE";
		}
		EXCPT_OPER(table->alterTableArgument(session, "COMPRESS_LOBS", boolStr, m_db->getConfig()->m_tlTimeout * 1000));

		table->lockMeta(session, IL_S, m_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		if ( i > 0) {
			CPPUNIT_ASSERT(table->getTableDef()->m_compressLobs);
		} else {
			CPPUNIT_ASSERT(!table->getTableDef()->m_compressLobs);
		}
		
		
		table->unlockMeta(session, IL_S);

		SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT, RID(0, 0));
		SubRecord *subRec = srb.createEmptySbById(table->getTableDef()->m_maxRecSize, "0 6");
		SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD);
		u16 columns[2] = {0, 6};
		TblScan *scanHandle = table->tableScan(session, OP_DELETE, 2, columns);
		uint gotRows = 0;

		//ɾ������ȫ������		
		gotRows = 0;
		while (table->getNext(scanHandle, subRec->m_data)) {
			table->deleteCurrent(scanHandle);
		}
		table->endScan(scanHandle);	

			//1. ��������
		TableAlterArgTestCase::insertBlog(m_db, NUM_LOB_INSERT, true);
		//2. ɨ��ɾ����������
		scanHandle = table->tableScan(session, OP_DELETE, 2, columns);
		while (table->getNext(scanHandle, subRec->m_data)) {
			if ((gotRows % 4) == 0) {
				table->deleteCurrent(scanHandle);
			}
			gotRows++;
		}
		CPPUNIT_ASSERT(gotRows == NUM_LOB_INSERT);
		table->endScan(scanHandle);	

		//�������һ����
		checkTable();

		//����һ��ɨ���ȡ�����	������index scan��
		IndexScanCond cond(0, NULL, true, true, false);
		//scanHandle = table->tableScan(session, OP_READ, 2, columns);
		scanHandle = table->indexScan(session, OP_READ, &cond, 2, columns);
		gotRows = 0;
		while (table->getNext(scanHandle, subRec->m_data)) {			
			uint lobSize = RecordOper::readLobSize(subRec->m_data, table->getTableDef()->m_columns[6]);
			byte *lob = RecordOper::readLob(subRec->m_data, table->getTableDef()->m_columns[6]);
			byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * 4 );
			memcpy(lobBuf, lob, lobSize);
			lobVector.push_back(make_pair(lobBuf, lobSize));
			gotRows++;
		}

		CPPUNIT_ASSERT(gotRows == 3 * NUM_LOB_INSERT / 4);
		table->endScan(scanHandle);
		string path = string(TableAlterArgTestCase::DB_NAME) + "." + TableAlterArgTestCase::TABLE_NAME;
		//3. ��������
		LobDefraggler lobDeffraggler(m_db, conn, path.c_str());
		EXCPT_OPER(lobDeffraggler.startDefrag());

		//4. �������һ���ԣ�����index scan��
		//IndexScanCond cond(0, NULL, true, true, false);
		scanHandle = table->indexScan(session, OP_READ, &cond, 2, columns);
		gotRows = 0;
		while (table->getNext(scanHandle, subRec->m_data)) {			
			uint lobSize = RecordOper::readLobSize(subRec->m_data, table->getTableDef()->m_columns[6]);
			byte *lob = RecordOper::readLob(subRec->m_data, table->getTableDef()->m_columns[6]);
			
			CPPUNIT_ASSERT(lobVector[gotRows].second == lobSize);
			CPPUNIT_ASSERT(!memcmp(lobVector[gotRows].first, lob, lobSize));

			System::virtualFree(lobVector[gotRows].first);
			gotRows++;	
		}
		CPPUNIT_ASSERT(gotRows == 3 * NUM_LOB_INSERT / 4);
		table->endScan(scanHandle);
		//��������
		checkTable();

		//ɾ������ȫ������
		scanHandle = table->tableScan(session, OP_DELETE, 2, columns);
		gotRows = 0;
		while (table->getNext(scanHandle, subRec->m_data)) {
			table->deleteCurrent(scanHandle);
			gotRows++;
		}
		CPPUNIT_ASSERT(gotRows == 3 * NUM_LOB_INSERT / 4);
		table->endScan(scanHandle);			
		delete[] subRec->m_columns;
		delete[] subRec->m_data;
		delete subRec;
	}	
	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);	
}


/** 
 * ���Դ���defrag�����
 *
 */
void LobDefragglerTestCase::testWrongDefragOperation() {
	//1. �����scheme.table_name ��ʽ
	string path = string(TableAlterArgTestCase::DB_NAME) + "o" + TableAlterArgTestCase::TABLE_NAME;
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	LobDefraggler lobDeffraggler(m_db, conn, path.c_str());
	bool resolved = false;
	try {
		lobDeffraggler.startDefrag();
	} catch (NtseException &) {
		//expected.
		resolved = true;
	}
	if (!resolved) {
		CPPUNIT_FAIL("Failed to catch expected exception.");
	}
	cout<<"case 1 passed"<<endl;

	//2. �����ڵ�table
	path = string(TableAlterArgTestCase::DB_NAME) + "." + TableAlterArgTestCase::TABLE_NAME + "_Wrong";
	LobDefraggler lobDeffraggler2(m_db, conn, path.c_str());
	resolved = false;
	try {
		lobDeffraggler2.startDefrag();
	} catch (NtseException &) {
		//expected.
		resolved = true;
	}
	if (!resolved) {
		CPPUNIT_FAIL("Failed to catch expected exception.");
	}
	cout<<"case 2 passed"<<endl;

	//3. ����һ�Ų�����lob�ı�another_table
	createAnotherTbl(m_db, false);
	path = string(TableAlterArgTestCase::DB_NAME) + "." + "another_table";
	LobDefraggler lobDeffraggler3(m_db, conn, path.c_str());
	resolved = false;
	try {
		lobDeffraggler3.startDefrag();
	} catch (NtseException &) {
		//expected.
		resolved = true;
	}
	if (!resolved) {
		CPPUNIT_FAIL("Failed to catch expected exception.");
	}
	cout<<"case 3 passed"<<endl;
	m_db->freeConnection(conn);
}

/** 
 * ����redoMove��
 * (1) ���� size �ֱ�Ϊ 2, 1, 2, 3, 5, 4 pages�����������ɾ���±�1��4�Ĵ����
 * (2) ���־���
 * (3) ������Ƭ����
 * (4) ������������
 * (5) �ָ�����2ʱ�ľ���
 * (6) ����move��־������Ƭ����
 * (7) �Ա�move����벽��(4) ���ֵĽ����
 * @note �ò�������������Ƭ��ǰ�ƶ�������ƶ�2�������
 */
void LobDefragglerTestCase::testRedoMove() {
	//(1) ����
	insertAndDeleteLobs();
	//(2) ���־���
	copyFiles();
	//(3) ��������
	string path = string(TableAlterArgTestCase::DB_NAME) + "." + TableAlterArgTestCase::TABLE_NAME;
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	LobDefraggler lobDeffraggler(m_db, conn, path.c_str());
	EXCPT_OPER(lobDeffraggler.startDefrag());
	m_db->freeConnection(conn);
	//(4) ����������
	//����defrag֮�������
	vector< pair<byte *, uint> > lobVector;
	saveLobs(&lobVector);	
	
	//�ر����ݿ�
	m_db->close(true, false);
	delete m_db;
	
	//(5)�ָ�����2ʱ�ľ���
	copyFiles(true);

	//(6)�����ݿ⣬��������
	static Config config;
	m_db = Database::open(&config, false, 1);
	
	//(7) �Ա�move����벽��(4) ���ֵĽ����
	checkLobsSame(&lobVector);	
}

/** 
 * ���Blog���е�log������֮ǰlobVector���ֵ���Ϣ��һ�µġ�
 * @param lobVector ֮ǰ����Ĵ������Ϣ��
 */
void LobDefragglerTestCase::checkLobsSame(vector< pair<byte *, uint> > *lobVector) {
	assert(NULL != lobVector);
	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);	
	Table *table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + TableAlterArgTestCase::DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	assert(NULL != table);

	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT, RID(0, 0));
	SubRecord *subRec = srb.createEmptySbById(table->getTableDef()->m_maxRecSize, "0 6");
	SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD);
	u16 columns[2] = {0, 6};

	TblScan *scanHandle = table->tableScan(session, OP_READ, 2, columns);
	uint gotRows = 0;
	while (table->getNext(scanHandle, subRec->m_data)) {			
		uint lobSize = RecordOper::readLobSize(subRec->m_data, table->getTableDef()->m_columns[6]);
		byte *lob = RecordOper::readLob(subRec->m_data, table->getTableDef()->m_columns[6]);
		
		CPPUNIT_ASSERT((*lobVector)[gotRows].second == lobSize);
		CPPUNIT_ASSERT(!memcmp((*lobVector)[gotRows].first, lob, lobSize));

		System::virtualFree((*lobVector)[gotRows].first);
		gotRows++;
	}
	CPPUNIT_ASSERT(4 == gotRows);

	table->endScan(scanHandle);
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	
	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}
/** 
 * ����������Ϣ
 * 
 * @param lobVector �������Ϣ�б�
 */
void LobDefragglerTestCase::saveLobs(vector< pair<byte *, uint> > *lobVector) {
	assert(NULL != lobVector);
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);	
	Table *table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + TableAlterArgTestCase::DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	assert(NULL != table);

	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT, RID(0, 0));
	SubRecord *subRec = srb.createEmptySbById(table->getTableDef()->m_maxRecSize, "0 6");
	SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD);
	u16 columns[2] = {0, 6};

	TblScan *scanHandle = table->tableScan(session, OP_READ, 2, columns);
	uint gotRows = 0;
	while (table->getNext(scanHandle, subRec->m_data)) {			
		uint lobSize = RecordOper::readLobSize(subRec->m_data, table->getTableDef()->m_columns[6]);
		byte *lob = RecordOper::readLob(subRec->m_data, table->getTableDef()->m_columns[6]);
		byte *lobBuf = (byte *)System::virtualAlloc(Limits::PAGE_SIZE * 4 );
		memcpy(lobBuf, lob, lobSize);
		lobVector->push_back(make_pair(lobBuf, lobSize));
		gotRows++;
	}
	CPPUNIT_ASSERT(4 == gotRows);

	table->endScan(scanHandle);
	
	delete [] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	
	m_db->closeTable(session, table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

/** 
 * �������ݿ��ļ��� log�ļ�����
 * @param revert �ļ����Ʒ���false��ʾ�����������ݿ��ļ���true��ʾ�ñ��ݵ��ļ����������ļ���
 */
void LobDefragglerTestCase::copyFiles(bool revert/* = false */) {
	//copy file
	string bak = ".bak";

	string frm = "./testdb/blog.frm";
	string frmBak = frm + bak;
	string nsd = "./testdb/blog.nsd";
	string nsdBak = nsd + bak;
	string nsi = "./testdb/blog.nsi";
	string nsiBak = nsi + bak;
	string nsld = "./testdb/blog.nsld";
	string nsldBak = nsld + bak;
	string nsli = "./testdb/blog.nsli";
	string nsliBak = nsli + bak;
	string nsso = "./testdb/blog.nsso";
	string nssoBak = nsso + bak;
	string ctrl = "./ntse_ctrl";
	string ctrlBak = ctrl + bak;
	
	if (!revert) {
		File::copyFile(frmBak.c_str() , frm.c_str(), true);
		File::copyFile(nsdBak.c_str() , nsd.c_str(), true);
		File::copyFile(nsiBak.c_str() , nsi.c_str(), true);
		File::copyFile(nsldBak.c_str() , nsld.c_str(), true);
		File::copyFile(nsliBak.c_str() , nsli.c_str(), true);
		File::copyFile(nssoBak.c_str() , nsso.c_str(), true);
		File::copyFile(ctrlBak.c_str() , ctrl.c_str(), true);
	} else {
		File::copyFile(frm.c_str(), frmBak.c_str() , true);
		File::copyFile(nsd.c_str(), nsdBak.c_str() , true);
		File::copyFile(nsi.c_str(),nsiBak.c_str() ,  true);
		File::copyFile(nsld.c_str(),nsldBak.c_str() ,  true);
		File::copyFile(nsli.c_str(), nsliBak.c_str() , true);
		File::copyFile(nsso.c_str(), nssoBak.c_str() , true);
		File::copyFile(ctrl.c_str(), ctrlBak.c_str() , true);
	}	
}

/** 
 * ���벢ɾ������lob��
 *  ���� size �ֱ�Ϊ 2, 1, 2, 3, 5, 4 pages�����������ɾ���±�1��4�Ĵ����
 */
void LobDefragglerTestCase::insertAndDeleteLobs() {
	Database *db = m_db;
	uint numRows = 6;
	bool doubleInsert = false;
	bool lobNotNull = true; 
	bool startFromOne = true;

	Connection *conn = m_db->getConnection(false, __FUNCTION__);
	Session *session = m_db->getSessionManager()->allocSession(__FUNCTION__, conn);	
	Table *table = m_db->openTable(session, string(string(m_db->getConfig()->m_basedir) + "/" + TableAlterArgTestCase::DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	assert(NULL != table);
	//��������
	Record **rows = new Record *[numRows];
	SubRecordBuilder srb(table->getTableDef(), REC_REDUNDANT);
	for (uint n = 0; n < numRows; n++) {
		
		u64 id = System::currentTimeMillis() + 1;
		if(startFromOne) {
			static int newID = 0;
			newID++;
			id = newID;
		}
		Thread::msleep(1);
		u64 userId = n / 5 + 1;
		u64 publishTime = System::currentTimeMillis()+1;
		char *title = randomStr(100);
		char *tags = randomStr(100);
		SubRecord *subRec = srb.createSubRecordByName("ID UserID PublishTime Title Tags", &id, &userId, &publishTime, title, tags);

		char *abs;
		if (!lobNotNull && (n % 2 == 0)) {
			abs = NULL;
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 0);
			RecordOper::setNullR(table->getTableDef(), subRec, 5, true);
		} else {
			abs = randomStr(100);
			RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[5], 100);
			RecordOper::setNullR(table->getTableDef(), subRec, 5, false);
		}
		RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[5], (byte *)abs);


		size_t contentSize;
		//2, 1, 2, 3, 5, 4
		switch (n) {
			case 0:
				contentSize = 2 * Limits::PAGE_SIZE;
				break;
			case 1:
				contentSize = 1 * Limits::PAGE_SIZE;
				break;
			case 2:
				contentSize = 2 * Limits::PAGE_SIZE;
				break;
			case 3:
				contentSize = 3 * Limits::PAGE_SIZE;
				break;
			case 4:
				contentSize = 5 * Limits::PAGE_SIZE;
				break;
			case 5:
				contentSize = 4 * Limits::PAGE_SIZE;
				break;
		}

		char *content = randomStr(contentSize);
		RecordOper::writeLob(subRec->m_data, table->getTableDef()->m_columns[6], (byte *)content);
		RecordOper::writeLobSize(subRec->m_data, table->getTableDef()->m_columns[6], (uint)contentSize);
		RecordOper::setNullR(table->getTableDef(), subRec, 6, false);
		

		Record *rec = new Record(RID(0, 0), REC_MYSQL, subRec->m_data, table->getTableDef()->m_maxRecSize);

		
		uint dupIndex;
		rec->m_format = REC_MYSQL;
		CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, true, &dupIndex)) != INVALID_ROW_ID);
		if (doubleInsert)
			CPPUNIT_ASSERT((rec->m_rowId = table->insert(session, rec->m_data, true, &dupIndex)) == INVALID_ROW_ID);
		rows[n] = rec;
		delete []title;
		delete []tags;
		delete[] subRec->m_columns;
		delete subRec;
	}
	for (uint i = 0; i < numRows; i++) {
		freeMysqlRecord(table->getTableDef(), rows[i]);
	}
	delete[] rows;
	rows = NULL;
	
	//ɾ�����ݣ�����index scan��
	SubRecord *subRec = srb.createEmptySbById(table->getTableDef()->m_maxRecSize, "0 6");
	SubRecordBuilder keyBuilder(table->getTableDef(), KEY_PAD);
	u16 columns[2] = {0, 6};
	IndexScanCond cond(0, NULL, true, true, false);

	TblScan *scanHandle = table->indexScan(session, OP_DELETE, &cond, 2, columns);
	uint index = 0;
	while (table->getNext(scanHandle, subRec->m_data)) {			
		if ((1 == index) || (4 == index)) {
			table->deleteCurrent(scanHandle);
		}		
		index++;	
	}
	table->endScan(scanHandle);		
	delete[] subRec->m_columns;
	delete[] subRec->m_data;
	delete subRec;
	db->closeTable(session, table);
	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);
}
/** 
 * ��������һ���ԡ�
 *
 * @see <code>TblInterface::verify()</code> method.
 */
void LobDefragglerTestCase::checkTable() {
	assert(NULL != m_db);
	TblInterface tblInterface(m_db, string(string(m_db->getConfig()->m_basedir) + "/" + TableAlterArgTestCase::DB_NAME + "/" + TableAlterArgTestCase::TABLE_NAME).c_str());
	tblInterface.open();
	tblInterface.verify();
	tblInterface.close();
}


/**
 * ����another_table����
 *
 * @param useMms �Ƿ�ʹ��MMS
 * @return another_table����
 */
TableDef* LobDefragglerTestCase::getAnotherTblDef(bool useMms) {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, TableAlterArgTestCase::DB_NAME, "another_table");

	builder->addColumn("ID", CT_BIGINT, false);

	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = useMms;

	delete builder;
	builder = NULL;
	return tableDef;
}

/** 
 * ����another_table��Ϊ�����������ı�
 *
 * @param db		���ݿ�
 * @param useMms	�Ƿ�ʹ��MMS
 */
void LobDefragglerTestCase::createAnotherTbl(Database *db, bool useMms) {
	assert(NULL != db);

	TableDef *tableDef = getAnotherTblDef(useMms);

	Connection *conn = db->getConnection(false);
	Session *session = db->getSessionManager()->allocSession("TableAlterTestCase::createBlog", conn);
	db->createTable(session, string(string(db->getConfig()->m_basedir) + "/" + TableAlterArgTestCase::DB_NAME + "/" + "another_table").c_str(), tableDef);
	delete tableDef;
	tableDef = NULL;

	db->getSessionManager()->freeSession(session);
	db->freeConnection(conn);	
}
