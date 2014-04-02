#include "api/TestRecvUpdate.h"
#include "api/TestTable.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "misc/Buffer.h"
#include "misc/ControlFile.h"
#include "misc/TableDef.h"
#include "misc/ControlFile.h"
#include "util/File.h"
#include "util/Thread.h"
#include "util/SmartPtr.h"
#include "heap/Heap.h"
#include "lob/Lob.h"
#include "btree/Index.h"
#include <string>
#include "Test.h"
#include "api/TestHelper.h"

using namespace std;
using namespace ntse;

const char* RecvUpdateTestCase::getName() {
	return "Update recovery test.";
}

const char* RecvUpdateTestCase::getDescription() {
	return "Test recovery of update operations.";
}

bool RecvUpdateTestCase::isBig() {
	return false;
}

/**
 * ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ������������ɣ����¶��м�¼
 */
void RecvUpdateTestCase::testSuccHeap() {
	doUpdateRecvTest(LOG_MAX, 0, 2, 2, false, false);	
}

/**
 * ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ������������ɣ����¶��м�¼
 */
void RecvUpdateTestCase::testSuccMms() {
	doUpdateRecvTest(LOG_MAX, 0, 2, 2, false, true);
}

/**
 * ����ѹ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ������������ɣ����¶��м�¼
 */
void RecvUpdateTestCase::testSuccHeapHasDictionary() {
	doUpdateRecvTest(LOG_MAX, 0, 5000, 5000, false, false, true);
}

/**
 * ����ѹ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ������������ɣ����¶��м�¼
 */
void RecvUpdateTestCase::testSuccMmsHasDictionary() {
	doUpdateRecvTest(LOG_MAX, 0, 5000, 5000, false, true, true);
}

/**
 * ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ������ʼ�����ϱ���
 */
void RecvUpdateTestCase::testCrashAfterBegin() {
	doUpdateRecvTest(LOG_PRE_UPDATE, 1, 2, 1, true, true);
}

/**
 * ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ��д��Ԥ������־�����
 */
void RecvUpdateTestCase::testCrashAfterPreUpdate() {
	doUpdateRecvTest(LOG_MMS_UPDATE, 1, 2, 2, true, true);
}

/**
 * ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ���������ǰ����
 */
void RecvUpdateTestCase::testCrashBeforeEnd() {
	doUpdateRecvTest(LOG_TXN_END, 1, 2, 2, true, true);
}

/**
* ����UPDATE�����Ļָ����̣��������������Ҳ����´����ʱ�����¶��м�¼ǰ����
*/
void RecvUpdateTestCase::testCrashBeforeHeap() {
	doUpdateRecvTest(LOG_HEAP_UPDATE, 1, 2, 2, true, false);
}

/**
 * ִ�в������������Ҳ����´����ʱ��UPDATE�ָ�����
 *
 * @param crashOnThisLog ģ����д������־ʱ�������������ΪLOG_MAX�򲻽ض���־
 * @param nthLog �ڼ��γ���ָ����־ʱ��������0��ʼ���
 * @param insertRows Ҫ����ļ�¼��
 * @param succRows ���³ɹ��ļ�¼��
 * @param recoverTwice �Ƿ�ָ�����
 * @param hitMms �Ƿ����MMS�м�¼
 */
void RecvUpdateTestCase::doUpdateRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
	uint succRows, bool recoverTwice, bool hitMms, bool createDict) {

	prepareBlog(true);

	// ׼����������
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "Blog"));
	Record **rows = TableTestCase::populateBlog(m_db, m_table, insertRows, false, true);

	Record **redRows = new Record* [insertRows];
	for (int i = 0; i < insertRows; i++) {
		byte *data = new byte[m_table->getTableDef()->m_maxRecSize];
		redRows[i] = new Record(INVALID_ROW_ID, REC_MYSQL, data, m_table->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_table->getTableDef(), rows[i], redRows[i]);
	}

	if (createDict) {
		EXCPT_OPER(m_db->alterTableArgument(m_session, m_table, "COMPRESS_ROWS", "TRUE", 2000));
		EXCPT_OPER(m_db->createCompressDic(m_session, m_table));
		EXCPT_OPER(m_db->optimizeTable(m_session, m_table, true));
	}

	EXCPT_OPER(m_db->checkpoint(m_session));
	m_db->setCheckpointEnabled(false);
	backupTable("Blog", true);

	// ����UPDATE����
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	TblScan *scanHandle;
	SubRecord *key = NULL;
	SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "Title");
	if (hitMms) {
		u64 id = 0;
		key = keyBuilder.createSubRecordByName("ID", &id);
		key->m_rowId = INVALID_ROW_ID;
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, OP_UPDATE, &cond, 
			subRec->m_numCols, subRec->m_columns);
	} else
		scanHandle = m_table->tableScan(m_session, OP_UPDATE, subRec->m_numCols, 
			subRec->m_columns);
	scanHandle->setUpdateColumns(subRec->m_numCols, subRec->m_columns);
	for (uint i = 0; i < insertRows; i++) {
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!TableTestCase::compareRecord(m_table, subRec->m_data, redRows[i]->m_data,
			subRec->m_numCols, subRec->m_columns));
		
		char *title = randomStr(100);
		SubRecord *updateRec = srb.createSubRecordByName("Title", title);
		CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec->m_data));

		if (succRows > i) {
			redRows[i]->m_format = REC_REDUNDANT;
			RecordOper::updateRecordRR(m_table->getTableDef(), redRows[i], updateRec);
			redRows[i]->m_format = REC_MYSQL;
		}
		
		delete []title;
		freeSubRecord(updateRec);
	}

	m_table->endScan(scanHandle);
	if (key)
		freeSubRecord(key);
	freeSubRecord(subRec);
	
	closeDatabase();

	// ���лָ�
	truncateLog(crashOnThisLog, nthLog);
	restoreTable("Blog", true);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	
	// ��֤�ָ����
	EXCPT_OPER(openTable("Blog"));
	checkRecords(m_table, redRows, insertRows);

	// �ٻָ�һ��
	if (recoverTwice) {
		closeDatabase();
		
		restoreTable("Blog", false);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		EXCPT_OPER(openTable("Blog"));
		checkRecords(m_table, redRows, insertRows);
	}
	
	freeUppMySQLRecords(m_table->getTableDef(), rows, insertRows);

	for(int i = 0; i < insertRows; i++) {
		freeRecord(redRows[i]);
	}
	delete []redRows;
	if (m_db) {
		closeDatabase();
	}
}

/**
 * ������MMS�еļ�¼���Σ��ڶ��θ���ǰ���������
 */
void RecvUpdateTestCase::testDoubleUpdateMmsCrash() {
	prepareBlog(true);

	// ׼����������
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "Blog"));
	Record **rows = TableTestCase::populateBlog(m_db, m_table, 1, false, true);

	Record **redRows = new Record* [1];
	for (int i = 0; i < 1; i++) {
		byte *data = new byte[m_table->getTableDef()->m_maxRecSize];
		redRows[i] = new Record(INVALID_ROW_ID, REC_MYSQL, data, m_table->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_table->getTableDef(), rows[i], redRows[i]);
	}

	m_db->checkpoint(m_session);

	backupTable("Blog", true);

	// ����UPDATE����
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	TblScan *scanHandle;
	SubRecord *key = NULL;

	u64 id = 0;
	key = keyBuilder.createSubRecordByName("ID", &id);
	key->m_rowId = INVALID_ROW_ID;
	
	SubRecord *updateRec[2];
	u64 publishTime = 1;
	updateRec[0] = srb.createSubRecordByName("PublishTime Title", &publishTime,  "title version 1");
	updateRec[1] = srb.createSubRecordByName("Title", "title version 2");
	SubRecord *scanSubRec[2];
	scanSubRec[0] = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "PublishTime Title");
	scanSubRec[1] = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "Title");
	// ��������
	for (int repeat = 0; repeat < sizeof(updateRec) / sizeof(SubRecord *); repeat++) {
		SubRecord *subRec = scanSubRec[repeat];
		IndexScanCond cond(0, key, true, true, false);
		scanHandle = m_table->indexScan(m_session, OP_UPDATE, &cond, 
			subRec->m_numCols, subRec->m_columns);
		scanHandle->setUpdateColumns(subRec->m_numCols, subRec->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!TableTestCase::compareRecord(m_table, subRec->m_data, redRows[0]->m_data,
			subRec->m_numCols, subRec->m_columns));

		CPPUNIT_ASSERT(m_table->updateCurrent(scanHandle, updateRec[repeat]->m_data));

		redRows[0]->m_format = REC_REDUNDANT;
		RecordOper::updateRecordRR(m_table->getTableDef(), redRows[0], updateRec[repeat]);
		redRows[0]->m_format = REC_MYSQL;

		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
		m_table->endScan(scanHandle);
	}


	for (int i = 0; i < sizeof(updateRec) / sizeof(SubRecord *); ++i) {
		freeSubRecord(updateRec[i]);
		freeSubRecord(scanSubRec[i]);
	}

	freeSubRecord(key);
	
	closeDatabase();

	// ���лָ�
	truncateLog(LOG_MMS_UPDATE, 1);
	restoreTable("Blog", false);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	// ��֤�ָ����
	openTable("Blog");
	checkRecords(m_table, redRows, 1);

	// �ٻָ�һ��
	{
		closeDatabase();

		restoreTable("Blog", false);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("Blog");
		checkRecords(m_table, redRows, 1);
	}

	freeUppMySQLRecords(m_table->getTableDef(), rows, 1);
	for(int i = 0; i < 1; i++) {
		freeRecord(redRows[i]);
	}
	delete []redRows;
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ����С�ʹ����Ϊ���ʹ���󣬸��´��ʹ����ΪС�ʹ���󣬲����������
 */
void RecvUpdateTestCase::testLobReverseSizeSucc() {
	doUpdateLobRecvTest(LOG_MAX, 0, 2, 2, false, UPDATE_REVERSE_SMALL_LARGE);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ����С��ΪС�ͣ�����Ϊ���ͣ������������
 */
void RecvUpdateTestCase::testLobKeepSizeSucc() {
	doUpdateLobRecvTest(LOG_MAX, 0, 2, 2, false, UPDATE_KEEP_SMALL_LARGE);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ����С�ʹ����Ϊ���ʹ���󣬸��´��ʹ����ΪС�ʹ����
 *     ����Ψһ��������ͻʧ�ܻع� 
 */
void RecvUpdateTestCase::testLobDup() {
	doUpdateLobRecvTest(LOG_MAX, 0, 2, 1, false, UPDATE_DUP);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ����С�ʹ����Ϊ���ʹ���󣬸��´��ʹ����ΪС�ʹ����
 *     ���´�����ڼ����
 */
void RecvUpdateTestCase::testLobCrashDuringLob() {
	doUpdateLobRecvTest(LOG_HEAP_DELETE, 0, 2, 2, true, UPDATE_REVERSE_SMALL_LARGE);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ����С�ʹ����Ϊ���ʹ���󣬸��´��ʹ����ΪС�ʹ����
 *     С�ʹ�������Ϊ���ʹ����ʱ��С�Ͷ���ɾ���󣬴��ʹ�������ǰ����
 */
void RecvUpdateTestCase::testLobCrashBeforeInsertLarge() {
	doUpdateLobRecvTest(LOG_LOB_INSERT, 0, 2, 2, true, UPDATE_REVERSE_SMALL_LARGE);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ����С�ʹ����Ϊ���ʹ���󣬸��´��ʹ����ΪС�ʹ����
 *     ���������ڼ����
 */
void RecvUpdateTestCase::testLobCrashDuringIndex() {
	doUpdateLobRecvTest(LOG_IDX_DML_END, 1, 2, 2, true, UPDATE_REVERSE_SMALL_LARGE);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ��������ΪNULL�������������
 */
void RecvUpdateTestCase::testLobToNull() {
	doUpdateLobRecvTest(LOG_MAX, 0, 2, 2, false, UPDATE_TO_NULL);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ��������ΪNULL��ɾ��С�ʹ����֮ǰʧ��
 */
void RecvUpdateTestCase::testLobToNullCrash() {
	doUpdateLobRecvTest(LOG_HEAP_DELETE, 0, 2, 2, true, UPDATE_TO_NULL);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   �������NULL����Ϊ��NULL�������������
 */
void RecvUpdateTestCase::testLobFromNull() {
	doUpdateLobRecvTest(LOG_MAX, 0, 2, 2, false, UPDATE_FROM_NULL);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   �������NULL����Ϊ��NULL�����´���������ʧ��
 */
void RecvUpdateTestCase::testLobFromNullCrash() {
	doUpdateLobRecvTest(LOG_LOB_INSERT, 0, 2, 2, true, UPDATE_FROM_NULL);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ֻ�������������´���󣬲����������
 */
void RecvUpdateTestCase::testIdxOnly() {
	doUpdateLobRecvTest(LOG_MAX, 0, 2, 2, false, UPDATE_IDX_ONLY);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ֻ�������������´���󣬸��������ڼ����
 */
void RecvUpdateTestCase::testIdxOnlyCrash() {
	doUpdateLobRecvTest(LOG_IDX_DML_END, 1, 2, 2, true, UPDATE_IDX_ONLY);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ֻ���´���󲻸��������������������
 */
void RecvUpdateTestCase::testLobOnly() {
	doUpdateLobRecvTest(LOG_MAX, 0, 2, 2, false, UPDATE_LOB_ONLY);
}

/**
 * ִ�и����������´�����UPDATE�����ָ�����
 *   ֻ���´���󲻸������������´�����ڼ����
 */
void RecvUpdateTestCase::testLobOnlyCrash() {
	doUpdateLobRecvTest(LOG_HEAP_UPDATE, 1, 2, 2, true, UPDATE_LOB_ONLY);
}

/**
 * ִ�и����������Ҹ��´����ʱ��UPDATE�ָ�����
 *
 * @param crashOnThisLog ģ����д������־ʱ�������������ΪLOG_MAX�򲻽ض���־
 * @param nthLog �ڼ��γ���ָ����־ʱ��������0��ʼ���
 * @param insertRows Ҫ����ļ�¼��
 * @param succRows ���³ɹ��ļ�¼��
 * @param recoverTwice �Ƿ�ָ�����
 * @param howToUpdate ��ô����
 */
void RecvUpdateTestCase::doUpdateLobRecvTest(LogType crashOnThisLog, int nthLog, uint insertRows, 
	uint succRows, bool recoverTwice, int howToUpdate) {
	prepareBlog(true);

	// ׼����������
	loadConnectionAndSession();

	EXCPT_OPER(m_table = m_db->openTable(m_session, "Blog"));
	bool lobNotNull = true;
	if (howToUpdate == UPDATE_FROM_NULL)
		lobNotNull = false;
	Record **rows = TableTestCase::populateBlog(m_db, m_table, insertRows, false, lobNotNull);

	Record **redRows = new Record* [insertRows];
	for (int i = 0; i < insertRows; i++) {
		byte *data = new byte[m_table->getTableDef()->m_maxRecSize];
		redRows[i] = new Record(INVALID_ROW_ID, REC_MYSQL, data, m_table->getTableDef()->m_maxRecSize);
		RecordOper::convertRecordMUpToEngine(m_table->getTableDef(), rows[i], redRows[i]);
	}

	m_db->checkpoint(m_session);

	backupTable("Blog", true);

	// ����UPDATE����
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT);
	SubRecordBuilder keyBuilder(m_table->getTableDef(), KEY_PAD);
	TblScan *scanHandle;
	SubRecord *key = NULL;
	SubRecord *subRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "ID PublishTime Content");
	u64 id = 0;
	key = keyBuilder.createSubRecordByName("ID", &id);
	key->m_rowId = INVALID_ROW_ID;
	IndexScanCond cond(0, key, true, true, false);
	scanHandle = m_table->indexScan(m_session, OP_UPDATE, &cond, 
		subRec->m_numCols, subRec->m_columns);
	SubRecord *updateCols;
	for (uint i = 0; i < insertRows; i++) {
		u64 publishTime = System::currentTimeMillis() + 100;
		char *content = NULL, *abs = NULL;
		uint contentSize, absSize;
		SubRecord *updateRec;
		if (howToUpdate == UPDATE_REVERSE_SMALL_LARGE) {
			// ����С�ʹ����Ϊ���ʹ���󣬸��´��ʹ����ΪС�ʹ����
			updateRec = srb.createSubRecordByName("PublishTime", &publishTime);
			contentSize = RecordOper::readLobSize(redRows[i]->m_data, m_table->getTableDef()->m_columns[6]);
			if (contentSize > Limits::PAGE_SIZE)
				contentSize = TableTestCase::SMALL_LOB_SIZE;
			else
				contentSize = TableTestCase::LARGE_LOB_SIZE;
			content = randomStr(contentSize);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[6], contentSize);
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[6], (byte *)content);
			RecordOper::setNullR(m_table->getTableDef(), updateRec, 6, false);
			if (i == 0)
				updateCols = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "PublishTime Content");
		} else if (howToUpdate == UPDATE_KEEP_SMALL_LARGE) {
			// ����С��ΪС�ͣ�����Ϊ����
			updateRec = srb.createSubRecordByName("PublishTime", &publishTime);
			contentSize = RecordOper::readLobSize(redRows[i]->m_data, m_table->getTableDef()->m_columns[6]);
			content = randomStr(contentSize);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[6], contentSize);
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[6], (byte *)content);
			RecordOper::setNullR(m_table->getTableDef(), updateRec, 6, false);
			if (i == 0)
				updateCols = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "PublishTime Content");
		} else if (howToUpdate == UPDATE_TO_NULL) {
			// ����ΪNULL
			updateRec = srb.createSubRecordByName("PublishTime", &publishTime);
			contentSize = 0;
			content = NULL;
			RecordOper::setNullR(m_table->getTableDef(), updateRec, 6, true);
			if (i == 0)
				updateCols = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "PublishTime Content");
		} else if (howToUpdate == UPDATE_FROM_NULL) {
			// NULL����Ϊ��NULL
			updateRec = srb.createSubRecordByName("PublishTime", &publishTime);
			contentSize = TableTestCase::LARGE_LOB_SIZE;
			content = randomStr(contentSize);
			absSize = TableTestCase::SMALL_LOB_SIZE;
			abs = randomStr(absSize);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[6], contentSize);
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[6], (byte *)content);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[5], absSize);
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[5], (byte *)abs);
			RecordOper::setNullR(m_table->getTableDef(), updateRec, 5, false);
			RecordOper::setNullR(m_table->getTableDef(), updateRec, 6, false);
			if (i == 0)
				updateCols = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "PublishTime Abstract Content");
		} else if (howToUpdate == UPDATE_IDX_ONLY) {
			// ֻ��������
			updateRec = srb.createSubRecordByName("PublishTime", &publishTime);
			if (i == 0)
				updateCols = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "PublishTime");
		} else if (howToUpdate == UPDATE_LOB_ONLY) {
			// ֻ���´����
			updateRec = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize,
				"Abstract");
			absSize = TableTestCase::SMALL_LOB_SIZE;
			abs = randomStr(absSize);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[5], absSize);
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[5], (byte *)abs);
			RecordOper::setNullR(m_table->getTableDef(), updateRec, 5, false);
			if (i == 0)
				updateCols = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "Abstract");
		} else {
			// ���µ���������ͻ
			assert(howToUpdate == UPDATE_DUP);
			u64 id = insertRows + 1;
			updateRec = srb.createSubRecordByName("ID PublishTime", &id, &publishTime);
			contentSize = RecordOper::readLobSize(redRows[i]->m_data, m_table->getTableDef()->m_columns[6]);
			if (contentSize > Limits::PAGE_SIZE)
				contentSize = TableTestCase::SMALL_LOB_SIZE;
			else
				contentSize = TableTestCase::LARGE_LOB_SIZE;
			content = randomStr(contentSize);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[6], contentSize);
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[6], (byte *)content);
			RecordOper::setNullR(m_table->getTableDef(), updateRec, 6, false);
			if (i == 0)
				updateCols = srb.createEmptySbByName(m_table->getTableDef()->m_maxRecSize, "ID PublishTime Content");
		}
		if (i == 0)
			scanHandle->setUpdateColumns(updateCols->m_numCols, updateCols->m_columns);
		CPPUNIT_ASSERT(m_table->getNext(scanHandle, subRec->m_data));
		CPPUNIT_ASSERT(!TableTestCase::compareRecord(m_table, subRec->m_data, redRows[i]->m_data,
			subRec->m_numCols, subRec->m_columns));
		m_table->updateCurrent(scanHandle, updateRec->m_data);

		if (succRows > i) {
			for (u16 c = 0; c < updateCols->m_numCols; c++) {
				u16 cno = updateCols->m_columns[c];
				if (m_table->getTableDef()->m_columns[cno]->isLob() && !m_table->getTableDef()->m_columns[cno]->isLongVar()) {
					if (!RecordOper::isNullR(m_table->getTableDef(), redRows[i], cno))
						delete []RecordOper::readLob(redRows[i]->m_data, m_table->getTableDef()->m_columns[cno]);
				}
			}
			redRows[i]->m_format = REC_REDUNDANT;
			if (howToUpdate != UPDATE_LOB_ONLY)
				RecordOper::updateRecordRR(m_table->getTableDef(), redRows[i], updateRec);
			if (howToUpdate == UPDATE_REVERSE_SMALL_LARGE
				|| howToUpdate == UPDATE_KEEP_SMALL_LARGE
				|| howToUpdate == UPDATE_DUP
				|| howToUpdate == UPDATE_FROM_NULL) {
				RecordOper::writeLob(redRows[i]->m_data, m_table->getTableDef()->m_columns[6], (byte *)content);
				RecordOper::writeLobSize(redRows[i]->m_data, m_table->getTableDef()->m_columns[6], contentSize);
			}
			if (howToUpdate == UPDATE_TO_NULL) {
				RecordOper::setNullR(m_table->getTableDef(), redRows[i], 6, true);
			}
			if (howToUpdate == UPDATE_FROM_NULL) {
				RecordOper::setNullR(m_table->getTableDef(), redRows[i], 5, false);
				RecordOper::setNullR(m_table->getTableDef(), redRows[i], 6, false);
				RecordOper::writeLob(redRows[i]->m_data, m_table->getTableDef()->m_columns[5], (byte *)abs);
				RecordOper::writeLobSize(redRows[i]->m_data, m_table->getTableDef()->m_columns[5], absSize);
			}
			if (howToUpdate == UPDATE_LOB_ONLY) {
				RecordOper::writeLob(redRows[i]->m_data, m_table->getTableDef()->m_columns[5], (byte *)abs);
				RecordOper::writeLobSize(redRows[i]->m_data, m_table->getTableDef()->m_columns[5], absSize);
			}
			redRows[i]->m_format = REC_MYSQL;
		} else {
			for (u16 c = 0; c < updateCols->m_numCols; c++) {
				u16 cno = updateCols->m_columns[c];
				if (m_table->getTableDef()->m_columns[cno]->isLob()) {
					if (!RecordOper::isNullR(m_table->getTableDef(), updateRec, cno))
						delete []RecordOper::readLob(updateRec->m_data, m_table->getTableDef()->m_columns[cno]);
				}
			}
		}

		freeSubRecord(updateRec);
	}
	if (howToUpdate != UPDATE_DUP)	// UPDATE_DUPʱ������1�ĳ�3�����ظ�����
		CPPUNIT_ASSERT(!m_table->getNext(scanHandle, subRec->m_data));
	m_table->endScan(scanHandle);
	if (key)
		freeSubRecord(key);
	freeSubRecord(subRec);
	freeSubRecord(updateCols);

	closeDatabase();

	// ���лָ�
	truncateLog(crashOnThisLog, nthLog);
	restoreTable("Blog", true);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	// ��֤�ָ����
	openTable("Blog");
	checkRecords(m_table, redRows, insertRows);

	// �ٻָ�һ��
	if (recoverTwice) {
		closeDatabase();

		restoreTable("Blog", true);
		EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
		openTable("Blog");
		checkRecords(m_table, redRows, insertRows);
	}

	freeEngineMySQLRecords(m_table->getTableDef(), redRows, insertRows);
	for(int i = 0; i < insertRows; i++) {
		freeRecord(rows[i]);
	}
	delete []rows;
	if (m_db) {
		closeDatabase();
	}
}

/**
 * ���Ա���ѹ���Ĵ��ʹ������º��ɲ�ѹ���Ĵ��ʹ����
 */
void RecvUpdateTestCase::testLargeToUncompress() {
	doReverseCompressRecvTest(true, true, Limits::MAX_REC_SIZE * 2);
}

/**
 * ���Ա���ѹ����С�ʹ������º��ɲ�ѹ����С�ʹ����
 */
void RecvUpdateTestCase::testSmallToUncompress() {
	doReverseCompressRecvTest(false, true, Limits::MAX_REC_SIZE / 2);
}

/**
 * ���Ա���ѹ����С�ʹ������º��ɲ�ѹ����С�ʹ�������д����MMS
 */
void RecvUpdateTestCase::testMmsToUncompress() {
	doReverseCompressRecvTest(true, true, Limits::MAX_REC_SIZE / 2);
}

/**
 * ִ�и��µ��´�����Ƿ�ѹ�������仯�Ļָ�����
 *
 * @param useMms ���Ƿ�ʹ��MMS
 * @param oldIsCompressed true��ʾ����֮ǰ��ѹ��������֮�󲻿�ѹ��������֮
 * @param size ������С
 */
void RecvUpdateTestCase::doReverseCompressRecvTest(bool useMms, bool oldIsCompressed, uint size) {
	char *canNotCompress1 = randomStr(size / 2);
	char *canNotCompress2 = randomStr(size / 2);
	char *canCompress = new char[size / 2 + 1];
	memset(canCompress, 'a', size / 2);
	canCompress[size / 2] = '\0';
	string oldLob, newLob;
	if (oldIsCompressed) {
		oldLob = oldLob + canNotCompress1 + canCompress;
		newLob = newLob + canNotCompress1 + canNotCompress2;
	} else {
		oldLob = oldLob + canNotCompress1 + canNotCompress2;
		newLob = newLob + canNotCompress1 + canCompress;
	}
	delete []canNotCompress1;
	delete []canNotCompress2;
	delete []canCompress;

	prepareBlog(useMms);
	EXCPT_OPER(openTable("Blog"));

	// ׼����������
	loadConnectionAndSession();

	TableDef *tableDef = m_table->getTableDef();
	RedRecord rec(tableDef);
	rec.writeNumber(tableDef->getColumnNo("ID"), (u64)1);
	rec.writeNumber(tableDef->getColumnNo("UserID"), (u64)1);
	rec.writeNumber(tableDef->getColumnNo("PublishTime"), System::currentTimeMillis());
	rec.setNull(tableDef->getColumnNo("Title"))->setNull(tableDef->getColumnNo("Tags"));
	rec.setNull(tableDef->getColumnNo("Abstract"));
	rec.writeLob(tableDef->getColumnNo("Content"), (const byte *)oldLob.c_str(), oldLob.size());

	uint dupIndex;
	CPPUNIT_ASSERT(m_table->insert(m_session, rec.getRecord()->m_data, true, &dupIndex) != INVALID_ROW_ID);

	// ����û�и���ǰ������
	m_db->checkpoint(m_session);
	backupTable("Blog", true);

	// ���´����
	u16 columns[1] = {tableDef->getColumnNo("Content")};
	TblScan *scanHandle = m_table->tableScan(m_session, OP_UPDATE, 1, columns);
	scanHandle->setUpdateColumns(1, columns);
	byte *buf = new byte[tableDef->m_maxRecSize];
	CPPUNIT_ASSERT(m_table->getNext(scanHandle, buf));
	rec.writeLob(tableDef->getColumnNo("Content"), (const byte *)newLob.c_str(), newLob.size());
	m_table->updateCurrent(scanHandle, rec.getRecord()->m_data);
	m_table->endScan(scanHandle);
	delete [] buf;
	buf = NULL;

	closeDatabase();

	// �ָ����ݵ����ݣ����лָ�
	restoreTable("Blog", true);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));

	// ��֤���
	openTable("Blog");
	Record *r = (Record *)rec.getRecord();
	checkRecords(m_table, &r, 1);

	// ��ֹRedRecord����ʱ�ͷŴ����ռ�
	rec.setNull(m_table->getTableDef()->getColumnNo("Content"));
	rec.writeLob(m_table->getTableDef()->getColumnNo("Content"), NULL, 0);
}

/** ���Ա���û������ʱ����� */
void RecvUpdateTestCase::testNoIdx() {
	TableDef *tableDef = TableTestCase::getBlogCountDef(false);
	while (tableDef->m_numIndice > 0)
		tableDef->removeIndex(0);

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();
	string bakPath = ti.backup(tableDef->hasLob());

	EXCPT_OPER(ti.open());
	CPPUNIT_ASSERT(ti.insertRow(NULL, (s64)1, 3, (s16)6, 23, "title 1", (s64)1));
	CPPUNIT_ASSERT(ti.updateRows(-1, 0, "AccessCount", 30) == 1);
	ResultSet *rs1 = ti.selectRows(NULL);

	ti.close(true);
	ti.restore(bakPath, tableDef->hasLob());
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);
	ti.close(true);

	delete tableDef;
	delete rs1;
	delete rs2;
}

/** ���Ը���Ψһ���������֮ǰʧ��ʱ�Ļָ� */
void RecvUpdateTestCase::testCrashBeforeUniqueIndex() {
	doUniqueIndexTest(true, false, false, false);
}

/** ���Ը���Ψһ���������֮��ʧ��ʱ�Ļָ� */
void RecvUpdateTestCase::testCrashAfterUniqueIndex() {
	doUniqueIndexTest(false, false, true, false);
}

/** ���Ը��·�Ψһ������ʱ�����ɾ���׶�֮ǰʧ��ʱ�Ļָ� */
void RecvUpdateTestCase::testCrashBeforeDelInUpdate() {
	doUniqueIndexTest(false, true, true, false);
}

void RecvUpdateTestCase::doUniqueIndexTest(bool beforeUnique, bool beforeDiu, bool doubleNonUnique, bool noUnique) {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("a", CT_INT, false);
	tdb.addColumn("b", CT_INT);
	tdb.addColumn("c", CT_INT);
	tdb.addIndex("a", false, false, false, "a", 0, NULL);
	tdb.addIndex("b", false, true, false, "b", 0, NULL);
	tdb.addIndex("c", false, false, false, "c", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));

	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	
	EXCPT_OPER(ti.open());
	CPPUNIT_ASSERT(ti.insertRow(NULL, 1, 1, 1));
	CPPUNIT_ASSERT(ti.insertRow(NULL, 2, 2, 2));
	ti.checkpoint();
	string bakPath = ti.backup(tableDef->hasLob());

	ResultSet *rs1 = ti.selectRows(NULL);
	if (doubleNonUnique) {
		if (noUnique)
			CPPUNIT_ASSERT(ti.updateRows(1, 0, "a c", 3, 3) == 1);
		else
			CPPUNIT_ASSERT(ti.updateRows(1, 0, "a b c", 3, 3, 3) == 1);
	} else {
		if (noUnique)
			CPPUNIT_ASSERT(ti.updateRows(1, 0, "c", 3) == 1);
		else
			CPPUNIT_ASSERT(ti.updateRows(1, 0, "a b", 3, 3) == 1);
	}
	ResultSet *rs2 = ti.selectRows(NULL);

	// ��һ�λָ�
	ti.close(true);
	ti.restore(bakPath, tableDef->hasLob());
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	if (beforeUnique)
		truncateLog(LOG_IDX_DMLDONE_IDXNO, 0, false);
	else if (beforeDiu)
		truncateLog(LOG_IDX_DIU_DONE, 0, false);
	else
		truncateLog(LOG_IDX_DMLDONE_IDXNO, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs3 = ti.selectRows(NULL);
	if (beforeUnique)
		CPPUNIT_ASSERT(*rs1 == *rs3);
	else
		CPPUNIT_ASSERT(*rs2 == *rs3);

	// �ڶ��λָ�
	ti.close(true);
	ti.restore(bakPath, tableDef->hasLob());
	m_db->close(true, false);
	delete m_db;
	m_db = NULL;

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs4 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs4 == *rs3);
	
	ti.close(true);

	delete tableDef;
	delete rs1;
	delete rs2;
	delete rs3;
	delete rs4;
}

/** ���Ը���ʱĳЩ������ֶθ���ǰ����NULL */
void RecvUpdateTestCase::testUpdateLobKeepNull() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("id", CT_INT, false);
	tdb.addColumn("value", CT_INT);
	tdb.addColumn("lob1", CT_SMALLLOB);
	tdb.addColumn("lob2", CT_SMALLLOB);
	tdb.addIndex("pkey", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());

	ti.insertRow(NULL, 1, 1, NULL, NULL);
	ti.checkpoint();
	string bakPath = ti.backup(tableDef->hasLob());

	ti.updateRows(1, 0, "value lob1 lob2", 2, NULL, "new lob2");
	ResultSet *rs1 = ti.selectRows(NULL);

	ti.close(true);
	string logBak = ti.backupTxnlogs(m_config.m_basedir, m_config.m_basedir);
	ti.restore(bakPath, tableDef->hasLob());
	closeDatabase(true, false);

	// �����ɹ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs2);

	ti.close(true);
	closeDatabase(true, false);

	// ���´�����ʧ��
	ti.restore(bakPath, tableDef->hasLob(), "dbtestdir/BlogCount");
	ti.restoreTxnlogs(logBak, m_config.m_basedir);
	truncateLog(LOG_HEAP_UPDATE, 0);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs3 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs1 == *rs3);

	ti.close(true);
	closeDatabase(true, false);

	delete tableDef;
	delete rs1;
	delete rs2;
	delete rs3;
}

/** ����ˢMMS���¼���ѵĻָ� */
void RecvUpdateTestCase::testMmsUpdateHeap() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("id", CT_INT, false);
	tdb.addColumn("a", CT_INT);
	tdb.addColumn("b", CT_MEDIUMLOB);
	tdb.addIndex("pkey", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());

	ti.insertRow(NULL, 1, 1, "old lob");
	// Load record into MMS
	ResultSet *rs = ti.selectRows(ti.buildLRange(0, 1, true, 1), NULL);
	delete rs;
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);
	CPPUNIT_ASSERT(ti.getTable()->getLobStorage()->getSLMmsTable()->getStatus().m_records == 1);

	CPPUNIT_ASSERT(ti.updateRows(ti.buildLRange(0, 1, true, 1), "a", 2) == 1);
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_dirtyRecords == 1);
	ResultSet *rs1 = ti.selectRows(NULL);

	CPPUNIT_ASSERT(ti.updateRows(ti.buildLRange(0, 1, true, 1), "b", "new lob") == 1);
	CPPUNIT_ASSERT(ti.getTable()->getLobStorage()->getSLMmsTable()->getStatus().m_dirtyRecords == 1);
	ResultSet *rs2 = ti.selectRows(NULL);
	
	string bakPath = ti.backup(tableDef->hasLob());
	
	ti.close(true);
	string logBak = ti.backupTxnlogs(m_config.m_basedir, m_config.m_basedir);
	ti.restore(bakPath, tableDef->hasLob());
	closeDatabase(true, false);

	// �����ɹ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs3 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs3 == *rs2);

	ti.close(true);
	closeDatabase(true, false);

	// ���´����ǰ������ע��ˢС�Ͷ����MMS��־��ǰ
	ti.restore(bakPath, tableDef->hasLob(), "dbtestdir/BlogCount");
	ti.restoreTxnlogs(logBak, m_config.m_basedir);
	truncateLog(LOG_HEAP_UPDATE, 0);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs4 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs4 == *rs2);

	ti.close(true);
	closeDatabase(true, false);

	// ���¶�ǰ����
	ti.restore(bakPath, tableDef->hasLob(), "dbtestdir/BlogCount");
	ti.restoreTxnlogs(logBak, m_config.m_basedir);
	truncateLog(LOG_HEAP_UPDATE, 1);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs5 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs5 == *rs2);

	ti.close(true);
	closeDatabase(true, false);

	// ���¶�ʱдLOG_PRE_UPDATE_HEAP��־�����
	ti.restore(bakPath, tableDef->hasLob(), "dbtestdir/BlogCount");
	ti.restoreTxnlogs(logBak, m_config.m_basedir);
	truncateLog(LOG_PRE_UPDATE_HEAP, 1);
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs6 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs6 == *rs2);

	ti.close(true);
	closeDatabase(true, false);

	delete tableDef;
	delete rs1;
	delete rs2;
	delete rs3;
	delete rs4;
	delete rs5;
	delete rs6;
}

/** ����MMS����ָ��Ļָ� */
void RecvUpdateTestCase::testMmsFlushLog() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("id", CT_INT, false);
	tdb.addColumn("a", CT_INT);
	tdb.addIndex("pkey", true, true, false, "id", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_cacheUpdate = true;
	tableDef->m_updateCacheTime = 2;
	tableDef->getColumnDef("a")->m_cacheUpdate = true;

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	EXCPT_OPER(ti.open());

	ti.insertRow(NULL, 1, 1);
	// Load record into MMS
	ResultSet *rs = ti.selectRows(ti.buildLRange(0, 1, true, 1), NULL);
	delete rs;
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_records == 1);

	string bakPath = ti.backup(tableDef->hasLob());

	CPPUNIT_ASSERT(ti.updateRows(ti.buildLRange(0, 1, true, 1), "a", 2) == 1);
	u64 oldLsn = m_db->getTxnlog()->lastLsn();
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_dirtyRecords == 1);
	CPPUNIT_ASSERT(ti.getTable()->getMmsTable()->getStatus().m_updateMerges == 1);
	CPPUNIT_ASSERT(m_db->getTxnlog()->lastLsn() == oldLsn);
	ResultSet *rs1 = ti.selectRows(NULL);

	do {
		Thread::msleep(tableDef->m_updateCacheTime * 2000);
	} while (m_db->getTxnlog()->lastLsn() == oldLsn);

	ti.close(true);
	closeDatabase(true, false);

	ti.restore(bakPath, tableDef->hasLob(), "dbtestdir/BlogCount");
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	EXCPT_OPER(ti.open());

	ResultSet *rs2 = ti.selectRows(NULL);
	CPPUNIT_ASSERT(*rs2 == *rs1);

	ti.close(true);
	closeDatabase(true, false);

	delete tableDef;
	delete rs1;
	delete rs2;
}


void RecvUpdateTestCase::testIOT() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("a", CT_INT, false);
	tdb.addColumn("b", CT_INT, false);
	tdb.addIndex("pkey", true, true, false, "a", 0, "b", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_indexOnly = true;
	tableDef->m_useMms = false;

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();

	ti.open();

	string datBak = ti.backup(tableDef->hasLob());
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	ti.insertRow(NULL, 1, 1);
	ResultSet *rs1 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs1->getNumRows() == 1);
	ti.updateRows(ti.buildLRange(0, 1, true, 0), "b", 2);
	ResultSet *rs2 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs2->getNumRows() == 1);
	m_db->getTxnlog()->flush(m_db->getTxnlog()->tailLsn());

	ti.close(true);
	closeDatabase(true, false);

	// �ɹ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs3 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs3 == *rs2);
	delete rs3;

	ti.close(true);
	closeDatabase(true, false);

	// UPDATE����������ɣ�δдEND��־ʱʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DML_END, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs4 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs4 == *rs2);
	delete rs4;

	ti.close(true);
	closeDatabase(true, false);

	// UPDATE��������������ʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DMLDONE_IDXNO, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs5 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs5 == *rs1);
	delete rs5;

	ti.close(true);
	closeDatabase(true, false);

	delete rs1;
	delete rs2;
	delete tableDef;
}


void RecvUpdateTestCase::testIOTVarlen() {
	TableDefBuilder tdb(TableDef::INVALID_TABLEID, "space", "BlogCount");
	tdb.addColumn("a", CT_INT, false);
	tdb.addColumnS("b", CT_VARCHAR, 16, false, false);
	tdb.addIndex("pkey", true, true, false, "a", 0, "b", 0, NULL);
	TableDef *tableDef = tdb.getTableDef();
	tableDef->m_indexOnly = true;
	tableDef->m_useMms = false;

	Database::create(&m_config);
	EXCPT_OPER(m_db = Database::open(&m_config, false));
	TblInterface ti(m_db, "BlogCount");
	EXCPT_OPER(ti.create(tableDef));
	ti.checkpoint();

	ti.open();

	string datBak = ti.backup(tableDef->hasLob());
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl.bak", "dbtestdir/ntse_ctrl", true) == File::E_NO_ERROR);

	ti.insertRow(NULL, 1, "aaabbb");
	ResultSet *rs1 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs1->getNumRows() == 1);
	ti.updateRows(ti.buildLRange(0, 1, true, 0), "b", "bbbccc");
	ResultSet *rs2 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(rs2->getNumRows() == 1);
	m_db->getTxnlog()->flush(m_db->getTxnlog()->tailLsn());

	ti.close(true);
	closeDatabase(true, false);

	// �ɹ�
	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs3 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs3 == *rs2);
	delete rs3;

	ti.close(true);
	closeDatabase(true, false);

	// UPDATE����������ɣ�δдEND��־ʱʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DML_END, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs4 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs4 == *rs2);
	delete rs4;

	ti.close(true);
	closeDatabase(true, false);

	// UPDATE��������������ʧ��
	ti.restore(datBak, tableDef->hasLob(), "dbtestdir/BlogCount");
	CPPUNIT_ASSERT(File::copyFile("dbtestdir/ntse_ctrl", "dbtestdir/ntse_ctrl.bak", true) == File::E_NO_ERROR);
	truncateLog(LOG_IDX_DMLDONE_IDXNO, 1, false);

	EXCPT_OPER(m_db = Database::open(&m_config, false, 1));
	ti.reconnect(m_db);
	ti.open();
	ResultSet *rs5 = ti.selectRows(ti.buildLRange(0, 1, true, 0), NULL);
	CPPUNIT_ASSERT(*rs5 == *rs1);
	delete rs5;

	ti.close(true);
	closeDatabase(true, false);

	delete rs1;
	delete rs2;
	delete tableDef;
}
