#include "MemoryTableScan.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LobDefrag.h"
#include "PaperTable.h"
#include <sstream>
#include <string>
#include "Random.h"
#include "lob/Lob.h"

using namespace ntseperf;
using namespace std;

extern byte* creatLob (uint);

LobDefragTest::LobDefragTest(bool useMms, u64 dataSize, u32 lobSize) {
	UNREFERENCED_PARAMETER(useMms);
	m_enableCache = true;
	m_dataSize = dataSize;
	m_lobSize = lobSize;
	setConfig(CommonDbConfig::getMedium());
	setTableDef(PaperTable::getTableDef());
}

string LobDefragTest::getName() const {
	stringstream ss;
	ss << "LobDefragTest(";
	if (m_useMms)
		ss << "useMMs,";
	ss << m_dataSize;
	ss << ",";
	ss << m_lobSize;
	ss << ")";
	return ss.str();
}
string LobDefragTest::getDescription() const {
	return "Lob Defrag Performance";
}

void LobDefragTest::run() {
	defragOneTime();
}

/** 
 * 插入记录
 * @post 数据库状态为关闭
 */
void LobDefragTest::loadData(u64 *totalRecSize, u64 *recCnt){
	// 创建数据库和表
	openTable(true);

	// 创建Session
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTest::loadData", conn);

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
 * 打开表
 */
void LobDefragTest::warmUp() {
	openTable(false);
	//LobStatus status = m_table->getLobStorage()->getStatus();
	
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTest::warmUp", conn);
	//insert数据
	u64 dataSize = m_dataSize / 2;
	u32 lobSize = m_lobSize / 4;
	m_recCntLoaded = PaperTable::populate(session, m_table, &dataSize, lobSize, false);

	dataSize = m_dataSize / 2;
	lobSize = 1024 * 10;
	m_recCntLoaded = PaperTable::populate(session, m_table, &dataSize, lobSize, false);

	//进行一些必要数据操作
	//先删除一些数据
	u16 columns[1] = {0};
	uint index = 0;
	SubRecordBuilder srb(m_table->getTableDef(), REC_REDUNDANT, RID(0, 0));
	SubRecord *subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0");
	TblScan *scanHandle = m_table->tableScan(session, OP_DELETE, 1, columns);
	ColumnDef *columnDef = m_table->getTableDef()->m_columns[0];
	while(m_table->getNext(scanHandle, subRec->m_data)) {
		u64 oneid = *(u64 *)(subRec->m_data + columnDef->m_offset);
		if (oneid == index)  {
			m_table->deleteCurrent(scanHandle);
			index +=  5;
		}
	}
	m_table->endScan(scanHandle);
	freeSubRecord(subRec);

	//在更新数据
	subRec = srb.createEmptySbById(m_table->getTableDef()->m_maxRecSize, "0");
	scanHandle = m_table->tableScan(session, OP_UPDATE, 1, columns);
	u16 columns2[1] = {1}; 
	scanHandle->setUpdateColumns(1, columns2);
	byte *lob; 
	while(m_table->getNext(scanHandle, subRec->m_data)) {
		u64 oneid = *(u64 *)(subRec->m_data + columnDef->m_offset);
		if (oneid % 5 != 0)  {
			SubRecord *updateRec = srb.createSubRecordById("");
			uint len = (uint)m_dataSize + RandomGen::nextInt(BIG_LOB_DEFRAG_RANDOM_LOW, BIG_LOB_DEFRAG_RANDOM_HIGH);
			lob = creatLob(len); 
			RecordOper::writeLob(updateRec->m_data, m_table->getTableDef()->m_columns[1], lob);
			RecordOper::writeLobSize(updateRec->m_data, m_table->getTableDef()->m_columns[1], len);
			m_table->updateCurrent(scanHandle, updateRec->m_data);
		}
	}
	m_table->endScan(scanHandle);
	freeSubRecord(subRec);

}

/**
 * 进行一遍defrag
 * @return 读取数据的总量
 */
void LobDefragTest::defragOneTime() {
	// 创建Session和MemoryContext
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTest::defragOneTime", conn);
	m_table->getLobStorage()->defrag(session);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
}

void LobDefragTest::tearDown() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobDefragTest::tearDown", conn);
	m_db->closeTable(session, m_table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
}

