#include "MemoryTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LobRead.h"
#include "PaperTable.h"
#include <sstream>
#include <string>
#include "Random.h"
#include "set"

using namespace ntseperf;
using namespace std;

extern byte* creatLob (uint);


LobReadTest::LobReadTest(bool useMms, u64 dataSize, u32 lobSize, bool inMemory, bool isNewData ) {
	m_enableCache = true;
	m_useMms = useMms;
	m_dataSize = dataSize;
	m_lobSize = lobSize;
	m_inMemory = inMemory;
	m_isNewData = isNewData;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobReadTest::getName() const {
	stringstream ss;
	ss << "LobReadTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_inMemory)
		ss << "m_inMemory,";
	if (m_isNewData)
		ss << "m_isNewData,";
	ss << m_dataSize;
	ss << ", ";
	ss << m_lobSize;
	ss << ")";
	return ss.str();
}

string LobReadTest::getDescription() const {
	return "Lob Read Performance";
}

void LobReadTest::run() {
	if (m_isNewData) {
		scanOneTime();
		return;
	} else {
		m_opDataSize = scanOneTime();
		return;
	}
}

/** 
 * 插入记录
 * @post 数据库状态为关闭
 */
void LobReadTest::loadData(u64 *totalRecSize, u64 *recCnt){
	// 创建数据库和表
	openTable(true);

	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobReadTest::loadData", conn);
	m_totalRecSizeLoaded = m_dataSize;
	m_recCntLoaded = PaperTable::populate(session, m_table, &m_totalRecSizeLoaded, m_lobSize, false);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
	m_opCnt = m_recCntLoaded;
	m_opDataSize = m_totalRecSizeLoaded;
}

/** 
 * 假如需要预读的要进行先读
 */
void LobReadTest::warmUp() {

	openTable(false);
	if (m_inMemory) {
		scanOneTime();
	}

	//假如要进行旧数据操作，所以要进行一定操作
	if (!m_isNewData) {
		// 创建Session
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("LobReadTest::warmUp", conn);
		set<int> Ids;
		while(true) {
				int delId= RandomGen::nextInt(0, int(m_recCntLoaded));
				if (Ids.find(delId) != Ids.end()) 
					continue;
				Ids.insert(delId);
				if (Ids.size() == m_recCntLoaded / 3)
					break;
		}
		//删除数据
		u16 columns[1] = {0};
		SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT, RID(0, 0));
		SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0");
		TblScan *scanHandle = m_table->tableScan(session, OP_DELETE, 1, columns);

		while(m_table->getNext(scanHandle, subRec->m_data)) {
			ColumnDef *columnDef = m_table->getTableDef()->m_columns[0];
			u64 oneid = *(u64 *)(subRec->m_data + columnDef->m_offset);
			if (Ids.count(oneid) >0) {
				m_table->deleteCurrent(scanHandle);
			}
		}
		m_table->endScan(scanHandle);
		//插入数据
		set <int>::iterator si;
		uint dupIdx;
		for (si=Ids.begin(); si!=Ids.end(); si++) {
			Record *newRec = PaperTable::createRecord(*si, (uint)RandomGen::nextInt(8 * 1024,  72 * 1024), m_table);
			m_table->insert(session, newRec->m_data, &dupIdx);
		}
		
		//更新数据
		SubRecordBuilder srb2(m_table->getTableDef(), REC_REDUNDANT, RID(0, 0));
		SubRecord *subRec2 = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0");
		TblScan *scanHandle2 = m_table->tableScan(session, OP_UPDATE, 1, columns);
		u16 columns2[1] = {1}; 
		scanHandle2->setUpdateColumns(1, columns2);
		byte *content =NULL;
		while(m_table->getNext(scanHandle2, subRec2->m_data)) {
			ColumnDef *columnDef = m_table->getTableDef()->m_columns[0];
			u64 oneid = *(u64 *)(subRec2->m_data + columnDef->m_offset);
			if (Ids.count(oneid) >0) {
				SubRecord *updateRec = srb.createSubRecordById("");
				uint newLen = (uint)RandomGen::nextInt(8 * 1024,  72 * 1024);
				content = creatLob(newLen);
				RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[1], (byte *)content);
				RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[1], (uint)newLen);
				m_table->updateCurrent(scanHandle2, updateRec->m_data);
				delete content;
				content = NULL;
			}
		}
		m_table->endScan(scanHandle2);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

}

/**
 * 进行一遍scan
 * @return 读取数据的总量
 */
uint  LobReadTest::scanOneTime() {
	uint totalSize = 0;
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT, 0);
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0 1");
	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobReadTest::scanOneTime", conn);

	TblScan *scanHandle = m_table->tableScan(session, OP_READ, subRec->m_numCols, subRec->m_columns);
	
	m_opCnt = 0;
	while(m_table->getNext(scanHandle, subRec->m_data)) {
		uint lobsize = RecordOper::readLobSize(subRec->m_data, m_table->getTableDef()->m_columns[1]);
		totalSize += lobsize;
		RecordOper::readLob(subRec->m_data, m_table->getTableDef()->m_columns[1]);
		m_opCnt++;
	}

	m_table->endScan(scanHandle);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	return totalSize;
}

void LobReadTest::tearDown() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobReadTest::tearDown", conn);
	m_db->closeTable(session, m_table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
}

