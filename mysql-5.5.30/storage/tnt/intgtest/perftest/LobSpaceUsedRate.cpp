#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"
#include "mms/Mms.h"
#include "LobSpaceUsedRate.h"
#include "PaperTable.h"
#include "BlogTable.h"
#include "util/File.h"
#include <sstream>
#include <string>
#include "Random.h"

using namespace ntseperf;
using namespace std;


LobSpaceTest::LobSpaceTest(bool useMms, u64 dataSize, bool isCompress, ostream *os): m_os(os) {
	m_enableCache = true;
	m_useMms = useMms;
	m_dataSize = dataSize;
	m_isCompress = isCompress;
	setConfig(CommonDbConfig::getSmall());
	setTableDef(BlogTable::getTableDef(m_useMms));
}

string LobSpaceTest::getName() const {
	stringstream ss;
	ss << "LobSpaceTest(";
	if (m_useMms)
		ss << "useMMs,";
	if (m_isCompress)
		ss << "m_isCompress,";
	ss << m_dataSize;
	ss << ")";
	return ss.str();
}
string LobSpaceTest::getDescription() const {
	return "Lob Space Performance";
}

void LobSpaceTest::run() {
	getFilesSpace();
}


/** 
 * 插入记录，建立在真实的blog数据上
 * @post 数据库状态为关闭
 */
void LobSpaceTest::loadData(u64 *totalRecSize, u64 *recCnt){
	// 创建数据库和表
	openTable(true);
	
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobReadTest::loadData", conn);
	m_totalRecSizeLoaded = m_dataSize;
	uint lobSize = 1;
	m_recCntLoaded = PaperTable::populate(session, m_table, &m_totalRecSizeLoaded, lobSize, false);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	closeDatabase();
	*totalRecSize = m_totalRecSizeLoaded;
	*recCnt = m_recCntLoaded;
	m_opCnt = 0;
	m_opDataSize = 0;
}

/** 
 * 打开表
 */
void LobSpaceTest::warmUp() {
	openTable(false);
}

/**
 * 得到文件的大小
 * @return 读取数据的总量
 */
void LobSpaceTest::getFilesSpace() {
	string tablePath(m_table->getPath()) ;
	string slobFileName = tablePath + Limits::NAME_SOBH_EXT; 
	string blobFileName = tablePath + Limits::NAME_LOBD_EXT;
	File slobFile(slobFileName.c_str());
	File blobFile(blobFileName.c_str());
	u64 slobSize;
	slobFile.getSize(&slobSize);
	u64 blobSize;
	blobFile.getSize(&blobSize);
	slobFile.close();
	blobFile.close();
}

void LobSpaceTest::tearDown() {
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("LobSpaceTest::tearDown", conn);
	m_db->closeTable(session, m_table);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);
	// delete m_countTab;
	m_table = NULL;
	m_db->close();
	delete m_db;
	m_db = NULL;
}

