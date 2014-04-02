/**
* 压缩表采样
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/

#ifndef _NTSETEST_RCM_SMPL_TBL_H_
#define _NTSETEST_RCM_SMPL_TBL_H_

#include <cppunit/extensions/HelperMacros.h>
#include "Test.h"
#include "misc/RecordHelper.h"
#include "api/Database.h"
#include "compress/RCMSampleTbl.h"

#define FAKE_LONG_CHAR234 "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" \
	"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
#define LONG_KEY_LENGTH 250

class CompressTableBuilder {
public:
	CompressTableBuilder(Database *db, bool onlyOneColGrp = true, bool largeRecord = false) : m_db(db), m_largeRecord(largeRecord) {
		TableDefBuilder *builder;

		// 创建小堆
		builder = new TableDefBuilder(99, "inventory", "User");
		builder->addColumn("UserId", CT_BIGINT, false)->addColumnS("UserName", CT_CHAR, 250);
		builder->addColumn("BankAccount", CT_BIGINT)->addColumn("Balance", CT_INT);
		if (largeRecord)
			builder->addColumnS("UserDetail", CT_CHAR, 250);
		builder->setCompresssTbl(true);

		if (onlyOneColGrp) {
			Array<u16> colNumArr;
			colNumArr.push(0);
			colNumArr.push(1);
			colNumArr.push(2);
			colNumArr.push(3);
			builder->addColGrp(0, colNumArr);
		} else {
			Array<u16> colNumArr;
			colNumArr.push(0);
			colNumArr.push(1);
			builder->addColGrp(0, colNumArr);
			Array<u16> colNumArr2;
			colNumArr2.push(2);
			colNumArr2.push(3);
			if (largeRecord)
				colNumArr2.push(4);
			builder->addColGrp(1, colNumArr2);
		}

		m_tableDef = builder->getTableDef();
		delete builder;
		builder = NULL;

		m_conn = m_db->getConnection(false);
		m_session = m_db->getSessionManager()->allocSession(
			"RCMSmplTblTestCase::init", m_conn);
		m_records = NULL;
	}
	~CompressTableBuilder() {
		if (m_records) {
			m_records->close(m_session, false);
			delete m_records;
			m_records = NULL;
		}
		m_db->getSessionManager()->freeSession(m_session);
		m_db->freeConnection(m_conn);

		delete m_tableDef;
		m_tableDef = NULL;
	}
	void createTable(u64 tableSize) {
		//clean garbage file
		File("User.ndic").remove();
		File("User.nsd").remove();

		string basePath = string(m_db->getConfig()->m_basedir) + NTSE_PATH_SEP + m_tableDef->m_name;
		EXCPT_OPER(Records::create(m_db, basePath.c_str(), m_tableDef));

		EXCPT_OPER(m_records = Records::open(m_db, m_session, basePath.c_str(), m_tableDef, false));
		CPPUNIT_ASSERT(m_records != NULL);

		//插入测试记录
		Record *record;
		RowLockHandle *rowlh;
		u64 count = 0;
		for (u64 i = 0; i < tableSize; i++) {
			char name[LONG_KEY_LENGTH];
			sprintf(name, "%d" FAKE_LONG_CHAR234, i);
			record = createRecord(m_tableDef, i, 0, name, i + ((u64)i << 32), (u32)((-1) - i), m_largeRecord ? name : NULL);
			m_records->insert(m_session, record, &rowlh);
			count++;
			m_session->unlockRow(&rowlh);
			freeRecord(record);
		}
		CPPUNIT_ASSERT(count == tableSize);
	}
	void dropTable() {
		string basePath = string(m_db->getConfig()->m_basedir) 
			+ NTSE_PATH_SEP + m_tableDef->m_name;
		m_records->close(m_session, false);
		EXCPT_OPER(m_records->drop(basePath.c_str()));
	}

	const TableDef* getTableDef() const {
		return m_tableDef;
	}

	Records* getRecords() const {
		return m_records;
	}

	Record* createRecord(u64 rowid, RecFormat format) {
		char name[LONG_KEY_LENGTH];
		sprintf(name, "%d" FAKE_LONG_CHAR234, rowid);
		return createRecord(m_tableDef, rowid, 0, name, rowid + ((u64)rowid << 32), (u32)((-1) - rowid), m_largeRecord ? name : NULL, format);
	}

private:
	Record* createRecord(const TableDef *tableDef, u64 rowid, 
		u64 userid, const char *username, 
		u64 bankacc, u32 balance, const char *userDetail, RecFormat format = REC_MYSQL) {
			RecordBuilder rb(tableDef, rowid, format);
			rb.appendBigInt(userid);
			rb.appendChar(username);
			rb.appendBigInt(bankacc);
			rb.appendInt(balance);
			if (userDetail != NULL)
				rb.appendChar(userDetail);
			return rb.getRecord(tableDef->m_maxRecSize);
	}

private:
	Database     *m_db;
	TableDef     *m_tableDef;
	Connection   *m_conn;
	Session      *m_session;
	Records      *m_records;
	bool         m_largeRecord;
};

class RCMSmplTblTestCase: public CPPUNIT_NS::TestFixture {
	CPPUNIT_TEST_SUITE(RCMSmplTblTestCase);
	CPPUNIT_TEST(testRCMSearchResultBuf);
	CPPUNIT_TEST(testRCMSearchBuf);
	CPPUNIT_TEST(testSlidingWinHashTbl);
	CPPUNIT_TEST(testSeqSmplTbl);
	CPPUNIT_TEST(testPartedSmplTbl);
	CPPUNIT_TEST(testDiscreteSmplTbl);
	CPPUNIT_TEST_SUITE_END();

public:
	static const char* getName();
	static const char* getDescription();
	static bool isBig(){
		return false;
	}
	void setUp();
	void tearDown();

protected:
	void testRCMSearchResultBuf();
	void testRCMSearchBuf();
	void testSlidingWinHashTbl();
	void testSeqSmplTbl();
	void testPartedSmplTbl();
	void testDiscreteSmplTbl();

private:
	void init();
	void cleanUp();
	void createTestTable(u64 tableSize);
	Record* createRecord(const TableDef *tableDef, u64 rowid, u64 userid, 
		const char *username, u64 bankacc, u32 balance);
	void dropTable();
	const TableDef *getTableDef() const {
		assert(m_cprsTblBuilder);
		return m_cprsTblBuilder->getTableDef();
	}
	Records* getRecords() const {
		assert(m_cprsTblBuilder);
		return m_cprsTblBuilder->getRecords();
	}

private:
	Config m_cfg;
	Database *m_db;
	Connection *m_conn;
	Session *m_session;
	CompressTableBuilder *m_cprsTblBuilder;
};

#endif