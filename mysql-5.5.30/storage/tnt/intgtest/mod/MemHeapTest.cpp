#include "MemHeapTest.h"
#include "MemHeap.h"
#include "misc/RecordHelper.h"
#include "BlogTable.h"


/**
 * �������Blog���¼����¼��RowId��0��ʼ����
 * BLOG_ID_CNO�е�ֵ���ڼ�¼��ID
 * ��֤�����¼����ȷ��
 */
void MemHeapTestCase::populateMemHeap(MemHeap *heap, unsigned numRecs, vector<MemHeapRid> *idvec, vector<RedRecord *> *recordVec) {
	
	for (unsigned i = 0; i < numRecs; ++i) {
		RedRecord *redRecord = new RedRecord(m_tableDef);
		RowId rid = (RowId)i;
		redRecord->setRowId(rid);

		redRecord->writeNumber(BLOG_USERID_CNO, System::random());
		redRecord->setNull(BLOG_PUBLISHTIME_CNO);
		byte title[128];
		memset(title, 'A' + i % 26, sizeof(title));
		redRecord->writeVarchar(BLOG_TITLE_CNO, title, sizeof(title));
		byte* abs = new byte [256];
		memset(abs, '0' + i % 10, 256);
		redRecord->writeLob(BLOG_ABSTRACT_CNO, abs, 256);
		redRecord->setNull(BLOG_CONTENT_CNO);
		redRecord->setNull(BLOG_PERMALINK_CNO);

		MemHeapRid id = heap->reserveRecord();
		RowLockHandle *rlh = LOCK_ROW(m_session, m_tableDef->m_id, rid, Exclusived);
		redRecord->writeNumber(BLOG_ID_CNO, (u64)id);
		CPPUNIT_ASSERT(id != MemHeapRecord::INVALID_ID && id < heap->getMaxRecCount());
		CPPUNIT_ASSERT(heap->insertAt(m_session, id, rid, redRecord->getRecord()->m_data));
		MemHeapRecord *memRec = heap->recordAt(m_session, id);
		CPPUNIT_ASSERT(memRec);
		CPPUNIT_ASSERT(memRec->compare(m_session, redRecord->getRecord()->m_data));
		for (u16 cno = 0; cno < m_tableDef->m_numCols; ++cno)
			CPPUNIT_ASSERT(memRec->compareColumn(m_session, cno, redRecord->getRecord()->m_data));
		CPPUNIT_ASSERT(memRec->getId() == id);
		CPPUNIT_ASSERT(memRec->getRowId() == rid);
		m_session->unlockRow(&rlh);
		idvec->push_back(id);
		recordVec->push_back(redRecord);
	}
}

void MemHeapTestCase::setUp() {
	Database::drop(m_config.m_basedir);
	m_db = Database::open(&m_config, true);
	m_conn = m_db->getConnection(false);
	m_session = m_db->getSessionManager()->allocSession("MemHeapTestCase", m_conn);
	m_tableDef = new TableDef(BlogTable::getTableDef(false));
}

void MemHeapTestCase::tearDown() {
	m_db->getSessionManager()->freeSession(m_session);
	m_db->freeConnection(m_conn);
	string basedir = m_db->getConfig()->m_basedir;
	m_db->close();
	delete m_db;
	Database::drop(basedir.c_str());
	delete m_tableDef;
}


/**
 * ����insertAt�ӿ�
 * ���������¼������ȡ��Щ��¼����֤��¼���
 */
void MemHeapTestCase::testInsertAt() {
	unsigned numRecs = 100;
	MemHeap heap(numRecs, m_tableDef);
	// ���Թ��캯����ͳ����Ϣ
	CPPUNIT_ASSERT(heap.getMaxRecCount() == numRecs);
	CPPUNIT_ASSERT(heap.getUsedRecCount() == 0);

	// �����¼ֱ������
	vector<MemHeapRid> idVec;
	vector<RedRecord *> recordVec;
	populateMemHeap(&heap, numRecs, &idVec, &recordVec);
	CPPUNIT_ASSERT(idVec.size() == numRecs);
	CPPUNIT_ASSERT(heap.getUsedRecCount() == numRecs);
	CPPUNIT_ASSERT(heap.reserveRecord() == MemHeapRecord::INVALID_ID);
	RowLockHandle *rlh = LOCK_ROW(m_session, m_tableDef->m_id, (RowId)numRecs + 1, Exclusived);
	CPPUNIT_ASSERT(MemHeapRecord::INVALID_ID == heap.insert(m_session, (RowId)numRecs + 1, NULL));
	m_session->unlockRow(&rlh);
	// ɾ�ն�
	for (unsigned i = 0; i < numRecs; ++i) {
		MemHeapRid id = i;
		RowLockHandle *rlh;
		MemHeapRecord *record = heap.recordAt(m_session, id, &rlh, Exclusived);
		CPPUNIT_ASSERT(record);
		CPPUNIT_ASSERT(record->getId() == id);
		CPPUNIT_ASSERT(heap.deleteRecord(m_session, (MemHeapRid)i));
		m_session->unlockRow(&rlh);
		CPPUNIT_ASSERT(heap.getUsedRecCount() == numRecs - 1 - i);
		CPPUNIT_ASSERT(heap.getMaxRecCount() == numRecs);
		delete recordVec[i];
	}

	idVec.clear();
	recordVec.clear();
	// ����һ������
	unsigned recCnt = numRecs / 2;
	populateMemHeap(&heap,recCnt, &idVec, &recordVec);
	CPPUNIT_ASSERT(idVec.size() == recCnt);
	CPPUNIT_ASSERT(heap.getUsedRecCount() == recCnt);
	
	for (unsigned i = 0; i < recCnt; ++i)
		delete recordVec[i];

}
/**
* ����update
* ���������¼������ȡ��Щ��¼��������Щ��¼����֤��¼���
*/
void MemHeapTestCase::testUpdate() {
	unsigned numRecs = 100;
	MemHeap heap(numRecs, m_tableDef);

	vector<MemHeapRid> idVec;
	vector<RedRecord *> recordVec;
	populateMemHeap(&heap, numRecs, &idVec, &recordVec);
	CPPUNIT_ASSERT(idVec.size() == numRecs);
	for (unsigned i = 0; i < numRecs; ++i) {
		MemHeapRid id = idVec[i];
		RowLockHandle *rlh;
		MemHeapRecord *memRec = heap.recordAt(m_session, id, &rlh, Exclusived);
		CPPUNIT_ASSERT(memRec);
		RedRecord *record = recordVec[i];
		// ���¼�¼��BLOG_USERID_CNO��
		record->writeNumber(BLOG_USERID_CNO, record->readInt(BLOG_USERID_CNO) + 1);
		CPPUNIT_ASSERT(!memRec->compare(m_session, record->getRecord()->m_data));
		CPPUNIT_ASSERT(!memRec->compareColumn(m_session, BLOG_USERID_CNO, record->getRecord()->m_data));
		{
			u16 upCols[] = {BLOG_USERID_CNO, BLOG_ABSTRACT_CNO};
			CPPUNIT_ASSERT(!memRec->compare(m_session, sizeof(upCols) / sizeof(upCols[0])
								, upCols, record->getRecord()->m_data));
		}
		{
			u16 upCols[] = {BLOG_USERID_CNO};
			CPPUNIT_ASSERT(!memRec->compare(m_session, sizeof(upCols) / sizeof(upCols[0])
				, upCols, record->getRecord()->m_data));
		}
		for (u16 cno = 0; cno < m_tableDef->m_numCols; ++cno) {
			if (cno != BLOG_USERID_CNO)
				CPPUNIT_ASSERT(memRec->compareColumn(m_session, cno, record->getRecord()->m_data));
			else
				CPPUNIT_ASSERT(!memRec->compareColumn(m_session, BLOG_USERID_CNO, record->getRecord()->m_data));
		}
		
		// ���¼�¼��BLOG_ABSTRACT_CNO��
		byte *lob;
		size_t size;
		record->readLob(BLOG_ABSTRACT_CNO, &lob, &size);
		if (size != 0)
			lob[0] ++;
		record->writeLob(BLOG_ABSTRACT_CNO, lob, size);
		CPPUNIT_ASSERT(!memRec->compare(m_session, record->getRecord()->m_data));
		CPPUNIT_ASSERT(!memRec->compareColumn(m_session, BLOG_ABSTRACT_CNO, record->getRecord()->m_data));
		for (u16 cno = 0; cno < m_tableDef->m_numCols; ++cno) {
			if (cno != BLOG_USERID_CNO && cno != BLOG_ABSTRACT_CNO)
				CPPUNIT_ASSERT(memRec->compareColumn(m_session, cno, record->getRecord()->m_data));
		}
		// �����ڴ�Ѽ�¼
		switch (i % 3) { 
			case 0: // ����������¼
				memRec->update(m_session, record->getRecord()->m_data);
				break;
			case 1: // �����Ӽ�¼
			{
				u16 updCols[] = {BLOG_USERID_CNO, BLOG_ABSTRACT_CNO};
				memRec->update(m_session, 2, updCols, record->getRecord()->m_data);
				break;
			}
				
			case 2:
			{
				u16 updCols[] = {BLOG_ABSTRACT_CNO, BLOG_USERID_CNO};
				memRec->update(m_session, 2, updCols, record->getRecord()->m_data);
				break;
			}
			default:
				assert(false);
		}

		CPPUNIT_ASSERT(memRec->compare(m_session, record->getRecord()->m_data));
		{
			u16 upCols[] = {BLOG_USERID_CNO, BLOG_ABSTRACT_CNO};
			CPPUNIT_ASSERT(memRec->compare(m_session, sizeof(upCols) / sizeof(upCols[0])
							, upCols, record->getRecord()->m_data));
		}
		for (u16 cno = 0; cno < m_tableDef->m_numCols; ++cno)
			CPPUNIT_ASSERT(memRec->compareColumn(m_session, cno, record->getRecord()->m_data));
		m_session->unlockRow(&rlh);
	}
	for (unsigned i = 0; i < numRecs; ++i)
		delete recordVec[i];
}

bool findRowId(const vector<RedRecord *> &recordVec, RowId rid) {
	for (unsigned i = 0; i < recordVec.size(); ++i) {
		if (recordVec[i]->getRowId() == rid)
			return true;
	}
	return false;
}
/**
 * ���������ȡ��¼
 */
void MemHeapTestCase::testGet() {
	{
		unsigned numRecs = 100;
		MemHeap heap(numRecs, m_tableDef);

		// �����¼ֱ������
		vector<MemHeapRid> idVec;
		vector<RedRecord *> recordVec;
		// ����һ�����Ķ�
		populateMemHeap(&heap, numRecs, &idVec, &recordVec);
		for (int i = 0; i < 1000; ++i) {
			RowId rid = heap.getRandRowId();
			CPPUNIT_ASSERT(rid != INVALID_ROW_ID);
			CPPUNIT_ASSERT(findRowId(recordVec, rid));
			RowLockHandle *rlh;
			MemHeapRecord *memRec = heap.getRandRecord(m_session, &rlh, Shared);
			CPPUNIT_ASSERT(memRec);
			CPPUNIT_ASSERT(memRec == heap.recordAt(m_session, memRec->getId()));
			m_session->unlockRow(&rlh);
		}
		for (size_t i = 0; i < recordVec.size(); ++i)
			delete recordVec[i];
	}
	{
		unsigned numRecs = 2000;
		MemHeap heap(numRecs, m_tableDef);
		
		unsigned notFoundRowId = 0;
		unsigned notFoundRecord = 0;
		// �����¼ֱ������
		vector<MemHeapRid> idVec;
		vector<RedRecord *> recordVec;
		unsigned recCnt =  numRecs / 20;
		// ����һ�����Ķ�
		populateMemHeap(&heap, recCnt, &idVec, &recordVec);
		for (int i = 0; i < 2000; ++i) {
			RowId rid = heap.getRandRowId();
			if (rid == INVALID_ROW_ID) {
				++ notFoundRowId;
			} else {
				CPPUNIT_ASSERT(findRowId(recordVec, rid));
			}
			RowLockHandle *rlh;
			MemHeapRecord *memRec = heap.getRandRecord(m_session, &rlh, Shared);
			if (!memRec) {
				notFoundRecord ++;
			} else {
				CPPUNIT_ASSERT(memRec == heap.recordAt(m_session, memRec->getId()));
				m_session->unlockRow(&rlh);
			}
		}
		for (size_t i = 0; i < recordVec.size(); ++i)
			delete recordVec[i];
		cout << "Total " << 1000 << endl;
		cout << "getRandRowId failed " << notFoundRowId << "\t";
		cout << "getRandRecord failed " << notFoundRecord << endl;
	}
}

//////////////////////////////////////////////////////////////////////////
//// MemHeapRecord
//////////////////////////////////////////////////////////////////////////
void MemHeapTestCase::testRecordCompare() {
	unsigned numRecs = 100;
	MemHeap heap(numRecs, m_tableDef);
	{
		RedRecord *redRecord = new RedRecord(m_tableDef, REC_REDUNDANT);
		redRecord->writeNumber(BLOG_ID_CNO, (u64)1023);
		MemHeapRid id = heap.reserveRecord();
		CPPUNIT_ASSERT(id != MemHeapRecord::INVALID_ID && id < numRecs);
		RowId rid = ((RowId)id) << 32;
		RowLockHandle *rlh = LOCK_ROW(m_session, m_tableDef->m_id, rid, Exclusived);
		CPPUNIT_ASSERT(heap.insertAt(m_session, id, rid, redRecord->getRecord()->m_data));
		MemHeapRecord *memRec = heap.recordAt(m_session, id);
		CPPUNIT_ASSERT(memRec);
		CPPUNIT_ASSERT(memRec->compare(m_session, redRecord->getRecord()->m_data));
		byte buf[Limits::PAGE_SIZE];
		Record varRec(INVALID_ROW_ID, REC_VARLEN, buf, Limits::PAGE_SIZE);
		RecordOper::convertRecordRV(m_tableDef, redRecord->getRecord(), &varRec);
		CPPUNIT_ASSERT(memRec->compare(m_session, &varRec));
		// ����������֮�󣬲������
		redRecord->writeNumber(BLOG_ID_CNO, (u64)1);
		CPPUNIT_ASSERT(!memRec->compare(m_session, redRecord->getRecord()->m_data));
		RecordOper::convertRecordRV(m_tableDef, redRecord->getRecord(), &varRec);
		CPPUNIT_ASSERT(!memRec->compare(m_session, &varRec));
		m_session->unlockRow(&rlh);
		delete redRecord;
	}

	{ 
		RedRecord *redRecord = new RedRecord(m_tableDef);
		byte oldlob[10];
		memset(oldlob, '1', sizeof(oldlob));
		redRecord->writeLob(BLOG_ABSTRACT_CNO, oldlob, sizeof(oldlob));

		MemHeapRid id = heap.reserveRecord();
		CPPUNIT_ASSERT(id != MemHeapRecord::INVALID_ID && id < numRecs);
		RowId rid = ((RowId)id) << 32;
		RowLockHandle *rlh = LOCK_ROW(m_session, m_tableDef->m_id, rid, Exclusived);
		CPPUNIT_ASSERT(heap.insertAt(m_session, id, rid, redRecord->getRecord()->m_data));
		MemHeapRecord *memRec = heap.recordAt(m_session, id);
		CPPUNIT_ASSERT(memRec);
		CPPUNIT_ASSERT(memRec->compare(m_session, redRecord->getRecord()->m_data));
		// ����lob��֮�󣬲������
		byte lob[100];
		memset(lob, 'X', sizeof(lob));
		redRecord->writeLob(BLOG_ABSTRACT_CNO, lob, sizeof(lob));
		CPPUNIT_ASSERT(!memRec->compare(m_session, redRecord->getRecord()->m_data));
		// ����ΪNULL֮�󣬲������
		redRecord->setNull(BLOG_ABSTRACT_CNO);
		CPPUNIT_ASSERT(!memRec->compare(m_session, redRecord->getRecord()->m_data));
		m_session->unlockRow(&rlh);
		delete redRecord;
	}
}
