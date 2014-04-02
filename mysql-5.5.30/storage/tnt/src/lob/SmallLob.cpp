/**
 * С�ʹ�������
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

/** ���������� */
static const char *VIRTUAL_TABLE_NAME = "smallLob";
static const char *VIRTUAL_TABLE_LEN_NAME = "lobLen";
static const char *VIRTUAL_TABLE_CONTENT_NAME = "lobContent";

/** 
 * ���캯��
 *
 * @param db �������ݿ�
 * @param heap ������
 * @param mmstable ����MMS��
 * @param vtableDef ������� 
 * @param useMms �Ƿ�����MMS
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
 * ��������
 */
SmallLobStorage::~SmallLobStorage(){
	delete m_heap;
	delete m_mtable;
	delete m_vtableDef;
	delete []m_path;
}

/**
 * ����һ��С�ʹ����洢������С�ʹ�����ļ�
 *
 * @param db ���ݿ�
 * @param tableDef ������Ķ���
 * @param tableId �������ID
 * @param path �ļ�·������������׺��
 * @throw NtseException �ļ����������
 */
void SmallLobStorage::create(Database *db, const TableDef *tableDef, u16 tableId, const char *basePath) throw(NtseException) {
	// ����С�ʹ�����ļ�
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
 * �����������
 *
 * @param tableId �������ID
 * @param tableName �������
 * @return ����
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
 * �õ��������ĳ��˴���������������Ҫ����
 *
 * @return �ֽ���
 */
u16 SmallLobStorage::getLenColumnBytes() {
	// λͼ����
	u16 nullBytes = m_vtableDef->m_columns[0]->m_offset;
	// ��һ���ֶγ���
	u16 oneColumnLen = m_vtableDef->m_columns[0]->m_size;
	// �ڶ����ֶεı�ʾ���ȵ��ֽ���
	u16 lenBytes = m_vtableDef->m_columns[1]->m_lenBytes;
	return nullBytes + oneColumnLen + lenBytes;
}

/**
 * �򿪴����洢
 *
 * @param db ���ݿ����
 * @param session �Ự����
 * @param path ·��
 * @param useMms �Ƿ�����MMS
 * @throw NtseException �ļ����������
 * @return С�ʹ����洢����
 */
SmallLobStorage* SmallLobStorage::open(Database *db, Session *session, const char *basePath, bool useMms) throw(NtseException) {
	ftrace(ts.lob, tout << basePath);
	DrsHeap *heap = NULL;

	string heapPath = string(basePath) +  Limits::NAME_SOBH_EXT;
	string vtblDefPath = string(basePath) + Limits::NAME_SOBH_TBLDEF_EXT;
	// ��ȡtableDef
	TableDef *vtableDef = TableDef::open(vtblDefPath.c_str());

	heap = DrsHeap::open(db, session, heapPath.c_str(), vtableDef);
	heap->getDBObjStats()->m_type = DBO_Slob;

	// ����MMSTable
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
 * ����С�ʹ����
 *
 * @param session �Ự����
 * @param slob ��Ӧ�Ĵ�����ֽ���
 * @param size ����󳤶�
 * @param orgLen ԭʼ����
 * @return �����ID
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
		// �������벻��Ҫ��TNT��������˲������״�
		assert(false);
	}
	nftrace(ts.lob, tout << "finish lob insert small lob" << rec->m_rowId);
	return rec->m_rowId;
}

/**
 * ��ȡһ��С�ʹ����
 *
 * @param session �Ự
 * @param mc �ڴ�������
 * @param lid �����ID
 * @param size out �����ĳ���
 * @param intoMms �Ƿ�д��MMS
 * @param orgLen ԭ�г���
 * @return �������ֽ���
 */
byte* SmallLobStorage::read(Session *session, MemoryContext *mc, LobId lid, u32 *size, bool intoMms, u32 *orgLen)  {
	ftrace(ts.lob, tout << session << lid);

	MmsRecord *mRecord = NULL;
	if (m_useMms)
		mRecord = m_mtable->getByRid(session, lid, true, NULL, None);
	// �����ж��Ƿ���MMS��
	if (mRecord) {
		Record record;
		// �ϲ㱣֤��������Բ�������
		// �����ڴ����
		byte *data = (byte *)mc->alloc(Limits::MAX_REC_SIZE);
		record.m_data = data;
		record.m_size = Limits::MAX_REC_SIZE;
		m_mtable->getRecord(mRecord, &record);
		// �ͷ�pin������
		m_mtable->unpinRecord(session, mRecord);
		size_t len;
		size_t orgSize;
		byte *retData = RecordOper::extractSlobData(m_vtableDef, &record, &len, &orgSize);
		*size = (u32)len;
		*orgLen = (u32)orgSize;
		return retData;
	}
	// ���粻����Drs����
	Record  record;
	// �����ڴ�
	byte *data = (byte *)mc->alloc(Limits::MAX_REC_SIZE);
	record.m_data = data;
	record.m_format = REC_VARLEN;
	bool isExist = m_heap->getRecord(session, lid, &record);
	// �����¼������
	if (!isExist) {
		*size = 0;
		return NULL;
	}
	if (intoMms) {
		assert(m_useMms);
		// ͬʱ����MMS��
		mRecord =  m_mtable->putIfNotExist(session, &record);
		// �ͷ�PIN
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
 * ɾ��һ��С�ʹ����
 *
 * @param session �Ự
 * @param lid �����ID
 * @return �Ƿ�ɹ�ɾ��
 * @throw NtseException �ļ����������
 */
bool SmallLobStorage::del(Session *session, LobId lid) throw(NtseException) {
	ftrace(ts.lob, tout << session << lid);
	delInMms(session, lid);
	//Ȼ��DRS��Ѱ��
	bool isSucuss = m_heap->del(session, lid);
	NTSE_ASSERT(isSucuss);
	return isSucuss;
}

#ifdef TNT_ENGINE
/**
 * ɾ��һ��С�ʹ��������crash�ָ�
 *
 * @param session �Ự
 * @param lid �����ID
 * @return �Ƿ�ɹ�ɾ��
 * @throw NtseException �ļ����������
 */
bool SmallLobStorage::delAtCrash(Session *session, LobId lid) throw(NtseException) {
	ftrace(ts.lob, tout << session << lid);
	delInMms(session, lid);
	//Ȼ��DRS��Ѱ��
	bool isSucuss = m_heap->del(session, lid);
	return isSucuss;
}
#endif
/**
 * ����С�ʹ����
 *
 * @param session �Ự����
 * @param lobId �����ID
 * @param lob ��Ӧ�Ĵ�����ֽ���
 * @param size ����󳤶�
 * @param orgLen ԭ�г���
 */
void SmallLobStorage::update(Session *session, LobId lobId, const byte *lob, uint size, u32 orgLen) {
	ftrace(ts.lob, tout << session << lobId << lob << size << orgLen);
	// �����õڶ�λ���Ƿ�ѹ����Ϊ0
	SubRecord subRec;
	byte data[Limits::MAX_REC_SIZE] ;
	u16 cols[2] = {0, 1};
	subRec.m_columns = cols;
	subRec.m_numCols = 2;
	subRec.m_data = data;
	subRec.m_size = Limits::MAX_REC_SIZE;
	// ����ѹ��λΪ0
	SubRecord *reTSubRec = RecordOper::createSlobSubRecordR(m_vtableDef, &subRec, lob, size, (size_t)orgLen);
	reTSubRec->m_rowId = lobId;
	reTSubRec->m_format = REC_REDUNDANT;
	// �ȸ���MMS
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
		// �������MMS�������DRS
		NTSE_ASSERT(m_heap->update(session, lobId, reTSubRec));
	}
}

/**
 * ɾ��С�ʹ����
 *
 * @param path �ļ�������·��
 * @throw NtseException �ļ����������
 */
void SmallLobStorage::drop(const char *basePath) throw(NtseException) {
	string heapPath = string(basePath) + Limits::NAME_SOBH_EXT;
	string vtblDefPath = string(basePath) + Limits::NAME_SOBH_TBLDEF_EXT;
	DrsHeap::drop(heapPath.c_str());
	TableDef::drop(vtblDefPath.c_str());
}

/**
 * �رմ����洢
 *
 * @param session �Ự
 * @param flushDirty �Ƿ�д��������
 */
void SmallLobStorage::close(Session *session, bool flushDirty) {
	// ��close mmsTable
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
 * ����MMS��
 *
 * @param session �Ự
 * @param useMms ʹ��MMS
 * @param flushDirty �Ƿ�ˢд���¼
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
 * ���ϻָ�ʱREDO���������
 *
 * @param db ���ݿ����
 * @param session �Ự
 * @param path С�ʹ�����ļ�·��
 * @param tableId �����ID
 * @throw NtseException �ļ����������
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
 * ���ϻָ�ʱREDOС�ʹ����������
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param logSize ��־��С
 * @return �����ID
 */
LobId SmallLobStorage::redoInsert(Session *session, u64 lsn, const byte *log, uint logSize) {
	// ����һ��Record
	Record record;
	byte data [Limits::MAX_REC_SIZE];
	record.m_format = REC_VARLEN;
	record.m_data = data;
	record.m_size = Limits::MAX_REC_SIZE;
	return m_heap->redoInsert(session, lsn, log, logSize, &record);
}

/**
 * ��MMS������ӦlobId�Ĵ�����¼��������ھ�ɾ��
 *
 * @param session �Ự����
 * @param lobId �����ID
 */
void SmallLobStorage::delInMms(Session *session, LobId lobId) {
	ftrace(ts.lob, tout << lobId);
	// �����õڶ�λ���Ƿ�ѹ����Ϊ0
	MmsRecord *mRecord = NULL;
	if (m_useMms)
		mRecord = m_mtable->getByRid(session, lobId, false, NULL, None);
	if (mRecord != NULL) {
		m_mtable->del(session, mRecord);
	}
}
/**
 * ���ϻָ�ʱREDOС�ʹ����������
 *
 * @param session �Ự����
 * @param lobId �����ID
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 */
void SmallLobStorage::redoDelete(Session *session, LobId lobId, u64 lsn, const byte *log, uint size) {
	// �ȵ�mms�в鿴�Ƿ���ڸĴ���������Ҫɾ��
	delInMms(session, lobId);
	m_heap->redoDelete(session, lsn, log, size);
}

/**
 * ���ϻָ�ʱREDOС�ʹ�������ж�ʱ�ĸ��²���,�����ǲ���Ҫѹ����
 *
 * @param session �Ự����
 * @param lobId �����ID
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 * @param lob ���������
 * @param lobSize �����Ĵ�С(������ѹ��ǰ���п���ѹ����)
 * @param org_size �����ѹ��ǰ��С
 */
void SmallLobStorage::redoUpdateHeap(Session *session, LobId lobId, u64 lsn, const byte *log, uint size,
	const byte *lob, uint lobSize, uint org_size) {
	// ��ɾ��MMS��
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
 * ���ϻָ�ʱREDOС�ʹ�������ж�ʱ�ĸ��²���
 *
 * @param session �Ự����
 * @param lsn ��־LSN
 * @param log ��¼���������־����
 * @param size ��־��С
 */
void SmallLobStorage::redoUpdateMms(Session *session, u64 lsn, const byte *log, uint size) {
	UNREFERENCED_PARAMETER(lsn);
	m_mtable->redoUpdate(session, log, size);
}

/**
 * �õ�С�ʹ�����ļ����
 *
 * @param files in/out ��ģ�������Fileָ�����飬 �ռ�����߷���
 * @param pageTypes in/out File��Ӧ��ҳ����
 * @param numFile files�����pageTypes���鳤��
 * @return File�������
 */
int SmallLobStorage::getFiles(File** files, PageType* pageTypes, int numFile) {
	return m_heap->getFiles(files, pageTypes, numFile);
}

/**
 * ˢ��������
 *
 * @param session �Ự
 */
void SmallLobStorage::flush(Session *session) {
	if (m_useMms)
		m_mtable->flush(session, true);
	m_heap->flush(session);
}

/** �޸ı�ID
 * @param session �Ự
 * @param tableId �µı�ID��Ϊ����ID���Ǵ���������
 */
void SmallLobStorage::setTableId(Session *session, u16 tableId) {
	UNREFERENCED_PARAMETER(session);
	string tblDefPath = string(m_path) + Limits::NAME_SOBH_TBLDEF_EXT;
	m_vtableDef->m_id = TableDef::getVirtualLobTableId(tableId);
	m_vtableDef->writeFile(tblDefPath.c_str());
}


}
