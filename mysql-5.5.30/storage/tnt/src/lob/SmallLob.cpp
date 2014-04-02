/**
 * 小型大对象管理
 *
 * @author zx(zhangxiao@corp.netease.com, zx@163.org)
 */
#include "lob/SmallLob.h"
#include "mms/Mms.h"
#include "misc/Record.h"
#include "api/Database.h"
#include "api/Table.h"
#include "util/File.h"

namespace ntse {

/** 虚拟表的名称 */
static const char *VIRTUAL_TABLE_NAME = "smallLob";
static const char *VIRTUAL_TABLE_LEN_NAME = "lobLen";
static const char *VIRTUAL_TABLE_CONTENT_NAME = "lobContent";

/** 
 * 构造函数
 *
 * @param db 所属数据库
 * @param heap 所属堆
 * @param mmstable 所属MMS表
 * @param vtableDef 虚拟表定义 
 * @param useMms 是否启用MMS
 */
SmallLobStorage::SmallLobStorage(Database *db, DrsHeap *heap, const char *path, MmsTable *mmstable, TableDef *vtableDef, bool useMms) {
	m_db = db;
	m_heap =  heap;
	m_path = path;
	m_mtable = mmstable;
	m_vtableDef = vtableDef;
	m_useMms = useMms;
}

/** 
 * 析构函数
 */
SmallLobStorage::~SmallLobStorage(){
	delete m_heap;
	delete m_mtable;
	delete m_vtableDef;
	delete []m_path;
}

/**
 * 创建一个小型大对象存储，生成小型大对象文件
 *
 * @param db 数据库
 * @param tableDef 所属表的定义
 * @param tableId 所属表的ID
 * @param path 文件路径，不包含后缀名
 * @throw NtseException 文件操作出错等
 */
void SmallLobStorage::create(Database *db, const TableDef *tableDef, u16 tableId, const char *basePath) throw(NtseException) {
	// 创建小型大对象文件
	string heapPath = string(basePath) +  Limits::NAME_SOBH_EXT;
	string tblDefPath = string(basePath) + Limits::NAME_SOBH_TBLDEF_EXT;
	u16 vTableId = TableDef::getVirtualLobTableId(tableId);
	TableDef *vtableDef = createTableDef(vTableId, VIRTUAL_TABLE_NAME);
	vtableDef->m_tableStatus = tableDef->m_tableStatus;
	vtableDef->m_pctFree = tableDef->m_pctFree;
	vtableDef->m_incrSize = tableDef->m_incrSize;
	vtableDef->m_cacheUpdate = false;
	bool createVTbl = false;
	
	try {
		vtableDef->writeFile(tblDefPath.c_str());
		createVTbl = true;
		DrsHeap::create(db, heapPath.c_str(), vtableDef);
	} catch (NtseException &e) {
		if (createVTbl) {
			TableDef::drop(tblDefPath.c_str());
		}

		delete vtableDef;
		throw e;
	}
	delete vtableDef;
}

/**
 * 生成虚拟表定义
 *
 * @param tableId 所属表的ID
 * @param tableName 表的名称
 * @return 表定义
 */
TableDef* SmallLobStorage::createTableDef(u16 tableId, const char *tableName) {
	TableDef *virtualTable;
	TableDefBuilder *tbuilder = new TableDefBuilder(tableId, tableName, tableName);
	tbuilder->addColumn(VIRTUAL_TABLE_LEN_NAME, CT_INT, true);
	tbuilder->addColumnS(VIRTUAL_TABLE_CONTENT_NAME, CT_VARCHAR, MAX_LOB_DATA_SIZE, false, true);
	virtualTable = tbuilder->getTableDef();
	NTSE_ASSERT(virtualTable->m_maxRecSize == Limits::MAX_REC_SIZE);
	delete tbuilder;
	return virtualTable;
}


/**
 * 得到虚拟表定义的除了大对象后，其他数据需要长度
 *
 * @return 字节数
 */
u16 SmallLobStorage::getLenColumnBytes() {
	// 位图长度
	u16 nullBytes = m_vtableDef->m_columns[0]->m_offset;
	// 第一个字段长度
	u16 oneColumnLen = m_vtableDef->m_columns[0]->m_size;
	// 第二个字段的表示长度的字节数
	u16 lenBytes = m_vtableDef->m_columns[1]->m_lenBytes;
	return nullBytes + oneColumnLen + lenBytes;
}

/**
 * 打开大对象存储
 *
 * @param db 数据库对象
 * @param session 会话对象
 * @param path 路径
 * @param useMms 是否启用MMS
 * @throw NtseException 文件操作出错等
 * @return 小型大对象存储对象
 */
SmallLobStorage* SmallLobStorage::open(Database *db, Session *session, const char *basePath, bool useMms) throw(NtseException) {
	ftrace(ts.lob, tout << basePath);
	DrsHeap *heap = NULL;

	string heapPath = string(basePath) +  Limits::NAME_SOBH_EXT;
	string vtblDefPath = string(basePath) + Limits::NAME_SOBH_TBLDEF_EXT;
	// 读取tableDef
	TableDef *vtableDef = TableDef::open(vtblDefPath.c_str());

	heap = DrsHeap::open(db, session, heapPath.c_str(), vtableDef);
	heap->getDBObjStats()->m_type = DBO_Slob;

	// 建立MMSTable
	MmsTable *lobtable = NULL;
	if (useMms) {
		Mms *lobMms = db->getMms();
		lobtable = new MmsTable(lobMms, db, heap, vtableDef, false, 0);
		lobMms->registerMmsTable(session, lobtable);
	}
	nftrace(ts.lob, tout << "open finish");
	return new SmallLobStorage(db, heap, System::strdup(basePath), lobtable, vtableDef, useMms);
}
/**
 * 插入小型大对象
 *
 * @param session 会话对象
 * @param slob 对应的大对象字节流
 * @param size 大对象长度
 * @param orgLen 原始长度
 * @return 大对象ID
 */
LobId SmallLobStorage::insert(Session *session, const byte *slob, uint size, u32 orgLen) {
	ftrace(ts.lob, tout << session << size << orgLen);
	Record tempRec;
	byte temp[Limits::MAX_REC_SIZE] ;
	tempRec.m_data = temp;
	tempRec.m_size = Limits::MAX_REC_SIZE;
	Record *rec = RecordOper::createSlobRecord(m_vtableDef, &tempRec, slob, (size_t)size, (size_t)orgLen);
	try {
		rec->m_rowId = m_heap->insert(session, rec, NULL);
	} catch (NtseException &){
		// 大对象插入不需要加TNT行锁，因此不可能抛错
		assert(false);
	}
	nftrace(ts.lob, tout << "finish lob insert small lob" << rec->m_rowId);
	return rec->m_rowId;
}

/**
 * 读取一个小型大对象
 *
 * @param session 会话
 * @param mc 内存上下文
 * @param lid 大对象ID
 * @param size out 大对象的长度
 * @param intoMms 是否写入MMS
 * @param orgLen 原有长度
 * @return 大对象的字节流
 */
byte* SmallLobStorage::read(Session *session, MemoryContext *mc, LobId lid, u32 *size, bool intoMms, u32 *orgLen)  {
	ftrace(ts.lob, tout << session << lid);

	MmsRecord *mRecord = NULL;
	if (m_useMms)
		mRecord = m_mtable->getByRid(session, lid, true, NULL, None);
	// 首先判断是否在MMS中
	if (mRecord) {
		Record record;
		// 上层保证，这里可以不加行锁
		// 进行内存分配
		byte *data = (byte *)mc->alloc(Limits::MAX_REC_SIZE);
		record.m_data = data;
		record.m_size = Limits::MAX_REC_SIZE;
		m_mtable->getRecord(mRecord, &record);
		// 释放pin和行锁
		m_mtable->unpinRecord(session, mRecord);
		size_t len;
		size_t orgSize;
		byte *retData = RecordOper::extractSlobData(m_vtableDef, &record, &len, &orgSize);
		*size = (u32)len;
		*orgLen = (u32)orgSize;
		return retData;
	}
	// 假如不在则到Drs中找
	Record  record;
	// 分配内存
	byte *data = (byte *)mc->alloc(Limits::MAX_REC_SIZE);
	record.m_data = data;
	record.m_format = REC_VARLEN;
	bool isExist = m_heap->getRecord(session, lid, &record);
	// 假如记录不存在
	if (!isExist) {
		*size = 0;
		return NULL;
	}
	if (intoMms) {
		assert(m_useMms);
		// 同时插入MMS中
		mRecord =  m_mtable->putIfNotExist(session, &record);
		// 释放PIN
		if (mRecord)
			m_mtable->unpinRecord(session, mRecord);
	}

	size_t len;
	size_t orgSize;
	byte *retData = RecordOper::extractSlobData(m_vtableDef, &record, &len, &orgSize);
	*size = (u32)len;
	*orgLen = (u32)orgSize;
	return retData;
}

/**
 * 删除一个小型大对象
 *
 * @param session 会话
 * @param lid 大对象ID
 * @return 是否成功删除
 * @throw NtseException 文件操作出错等
 */
bool SmallLobStorage::del(Session *session, LobId lid) throw(NtseException) {
	ftrace(ts.lob, tout << session << lid);
	delInMms(session, lid);
	//然后到DRS中寻找
	bool isSucuss = m_heap->del(session, lid);
	NTSE_ASSERT(isSucuss);
	return isSucuss;
}

#ifdef TNT_ENGINE
/**
 * 删除一个小型大对象，用于crash恢复
 *
 * @param session 会话
 * @param lid 大对象ID
 * @return 是否成功删除
 * @throw NtseException 文件操作出错等
 */
bool SmallLobStorage::delAtCrash(Session *session, LobId lid) throw(NtseException) {
	ftrace(ts.lob, tout << session << lid);
	delInMms(session, lid);
	//然后到DRS中寻找
	bool isSucuss = m_heap->del(session, lid);
	return isSucuss;
}
#endif
/**
 * 更新小型大对象
 *
 * @param session 会话对象
 * @param lobId 大对象ID
 * @param lob 对应的大对象字节流
 * @param size 大对象长度
 * @param orgLen 原有长度
 */
void SmallLobStorage::update(Session *session, LobId lobId, const byte *lob, uint size, u32 orgLen) {
	ftrace(ts.lob, tout << session << lobId << lob << size << orgLen);
	// 先设置第二位的是否压缩的为0
	SubRecord subRec;
	byte data[Limits::MAX_REC_SIZE] ;
	u16 cols[2] = {0, 1};
	subRec.m_columns = cols;
	subRec.m_numCols = 2;
	subRec.m_data = data;
	subRec.m_size = Limits::MAX_REC_SIZE;
	// 设置压缩位为0
	SubRecord *reTSubRec = RecordOper::createSlobSubRecordR(m_vtableDef, &subRec, lob, size, (size_t)orgLen);
	reTSubRec->m_rowId = lobId;
	reTSubRec->m_format = REC_REDUNDANT;
	// 先更新MMS
	MmsRecord *mRecord = NULL;
	if (m_useMms)
		mRecord = m_mtable->getByRid(session, lobId, true, NULL, None);
	if (mRecord != NULL) {
		u16 recSize;
		if (m_mtable->canUpdate(mRecord, reTSubRec, &recSize))
			m_mtable->update(session, mRecord, reTSubRec, recSize);
		else {
			Session *sessionNew = m_db->getSessionManager()->allocSession("SmallLobStorage::update", session->getConnection());
			m_mtable->flushAndDel(sessionNew, mRecord);
			m_db->getSessionManager()->freeSession(sessionNew);
			NTSE_ASSERT(m_heap->update(session, lobId, reTSubRec));
		}
	} else {
		// 如果不在MMS，则更新DRS
		NTSE_ASSERT(m_heap->update(session, lobId, reTSubRec));
	}
}

/**
 * 删除小型大对象
 *
 * @param path 文件的完整路径
 * @throw NtseException 文件操作出错等
 */
void SmallLobStorage::drop(const char *basePath) throw(NtseException) {
	string heapPath = string(basePath) + Limits::NAME_SOBH_EXT;
	string vtblDefPath = string(basePath) + Limits::NAME_SOBH_TBLDEF_EXT;
	DrsHeap::drop(heapPath.c_str());
	TableDef::drop(vtblDefPath.c_str());
}

/**
 * 关闭大对象存储
 *
 * @param session 会话
 * @param flushDirty 是否写出脏数据
 */
void SmallLobStorage::close(Session *session, bool flushDirty) {
	// 先close mmsTable
	if (m_useMms) {
		Mms *lobMms =m_db->getMms();
		lobMms->unregisterMmsTable(session, m_mtable);
		m_mtable->close(session, flushDirty);
		delete m_mtable;
		m_mtable = NULL;
	}
	delete m_vtableDef;
	m_vtableDef = NULL;
	m_heap->close(session, flushDirty);
	delete m_heap;
	m_heap = NULL;
}

/** 
 * 设置MMS表
 *
 * @param session 会话
 * @param useMms 使用MMS
 * @param flushDirty 是否刷写脏记录
 */
void SmallLobStorage::setMmsTable(Session *session, bool useMms, bool flushDirty) {
	if (useMms && !m_useMms) {
		assert(!m_mtable);
		Mms *mms = m_db->getMms();
		m_mtable = new MmsTable(mms, m_db, m_heap, m_vtableDef, false, 0);
		mms->registerMmsTable(session, m_mtable);
	} 
	if (!useMms && m_useMms) {
		Mms *lobMms =m_db->getMms();
		lobMms->unregisterMmsTable(session, m_mtable);
		m_mtable->close(session, flushDirty);
		delete m_mtable;
		m_mtable = NULL;	
	}
	m_useMms = useMms;
}

/**
 * 故障恢复时REDO创建表操作
 *
 * @param db 数据库对象
 * @param session 会话
 * @param path 小型大对象文件路径
 * @param tableId 虚拟表ID
 * @throw NtseException 文件操作出错等
 */
void SmallLobStorage::redoCreate(Database *db, Session *session, const char *basePath, u16 tableId) throw(NtseException) {
	string heapPath = string(basePath) + Limits::NAME_SOBH_EXT;
	string tblDefPath = string(basePath) + Limits::NAME_SOBH_TBLDEF_EXT;
	u16 vTableId = TableDef::getVirtualLobTableId(tableId);;
	TableDef *vtableDef = createTableDef(vTableId, VIRTUAL_TABLE_NAME);
	vtableDef->m_cacheUpdate = false;
	bool createVTbl = false;
	try {
		vtableDef->writeFile(tblDefPath.c_str());
		createVTbl = true;
		DrsHeap::redoCreate(db, session, heapPath.c_str(), vtableDef);
	} catch (NtseException &e) {
		if (createVTbl) {
			TableDef::drop(tblDefPath.c_str());
		}
		delete vtableDef;
		throw e;
	}
	delete vtableDef;
}


/**
 * 故障恢复时REDO小型大对象插入操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param logSize 日志大小
 * @return 大对象ID
 */
LobId SmallLobStorage::redoInsert(Session *session, u64 lsn, const byte *log, uint logSize) {
	// 构造一个Record
	Record record;
	byte data [Limits::MAX_REC_SIZE];
	record.m_format = REC_VARLEN;
	record.m_data = data;
	record.m_size = Limits::MAX_REC_SIZE;
	return m_heap->redoInsert(session, lsn, log, logSize, &record);
}

/**
 * 在MMS查找相应lobId的大对象记录，如果存在就删除
 *
 * @param session 会话对象
 * @param lobId 大对象ID
 */
void SmallLobStorage::delInMms(Session *session, LobId lobId) {
	ftrace(ts.lob, tout << lobId);
	// 先设置第二位的是否压缩的为0
	MmsRecord *mRecord = NULL;
	if (m_useMms)
		mRecord = m_mtable->getByRid(session, lobId, false, NULL, None);
	if (mRecord != NULL) {
		m_mtable->del(session, mRecord);
	}
}
/**
 * 故障恢复时REDO小型大对象插入操作
 *
 * @param session 会话对象
 * @param lobId 大对象ID
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 */
void SmallLobStorage::redoDelete(Session *session, LobId lobId, u64 lsn, const byte *log, uint size) {
	// 先到mms中查看是否存在改大对象，如果在要删除
	delInMms(session, lobId);
	m_heap->redoDelete(session, lsn, log, size);
}

/**
 * 故障恢复时REDO小型大对象不命中堆时的更新操作,这里是不需要压缩的
 *
 * @param session 会话对象
 * @param lobId 大对象ID
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 * @param lob 大对象内容
 * @param lobSize 大对象的大小(可能是压缩前，有可能压缩后)
 * @param org_size 大对象压缩前大小
 */
void SmallLobStorage::redoUpdateHeap(Session *session, LobId lobId, u64 lsn, const byte *log, uint size,
	const byte *lob, uint lobSize, uint org_size) {
	// 先删除MMS中
	delInMms(session, lobId);
	SubRecord subRec;
	byte data[Limits::MAX_REC_SIZE];
	u16 cols[2] = {0, 1};
	subRec.m_columns = cols;
	subRec.m_numCols = 2;
	subRec.m_data = data;
	subRec.m_size = m_vtableDef->m_maxRecSize;
	SubRecord  *reSubRec = RecordOper::createSlobSubRecordR(m_vtableDef, &subRec, lob, lobSize, org_size);
	reSubRec->m_format = REC_REDUNDANT;
	m_heap->redoUpdate(session, lsn, log, size, reSubRec);
}

/**
 * 故障恢复时REDO小型大对象命中堆时的更新操作
 *
 * @param session 会话对象
 * @param lsn 日志LSN
 * @param log 记录插入操作日志内容
 * @param size 日志大小
 */
void SmallLobStorage::redoUpdateMms(Session *session, u64 lsn, const byte *log, uint size) {
	UNREFERENCED_PARAMETER(lsn);
	m_mtable->redoUpdate(session, log, size);
}

/**
 * 得到小型大对象文件句柄
 *
 * @param files in/out 该模块的所有File指针数组， 空间调用者分配
 * @param pageTypes in/out File对应的页类型
 * @param numFile files数组和pageTypes数组长度
 * @return File对象个数
 */
int SmallLobStorage::getFiles(File** files, PageType* pageTypes, int numFile) {
	return m_heap->getFiles(files, pageTypes, numFile);
}

/**
 * 刷出脏数据
 *
 * @param session 会话
 */
void SmallLobStorage::flush(Session *session) {
	if (m_useMms)
		m_mtable->flush(session, true);
	m_heap->flush(session);
}

/** 修改表ID
 * @param session 会话
 * @param tableId 新的表ID，为主表ID不是大对象虚拟表
 */
void SmallLobStorage::setTableId(Session *session, u16 tableId) {
	UNREFERENCED_PARAMETER(session);
	string tblDefPath = string(m_path) + Limits::NAME_SOBH_TBLDEF_EXT;
	m_vtableDef->m_id = TableDef::getVirtualLobTableId(tableId);
	m_vtableDef->writeFile(tblDefPath.c_str());
}


}
