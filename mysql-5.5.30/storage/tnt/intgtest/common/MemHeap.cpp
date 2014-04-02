#include "Random.h"
#include "MemHeap.h"
#include "util/DList.h"
#include "misc/RecordHelper.h"


#include <vector>
#include <algorithm>
using namespace std;
using namespace ntse;


/**
 * �����ڴ��
 * @param maxRecCnt ����ܴ�ŵļ�¼��
 * @param tableDef ����
 * @param numCksumCols ��Ҫ��checksum������
 * @param cksumCols ��Ҫ��checksum����
 */
MemHeap::MemHeap(unsigned int maxRecCnt, const TableDef *tableDef, u16 numCksumCols, u16 *cksumCols)
	: m_tableDef(new TableDef(tableDef)), m_maxRecCnt(maxRecCnt)
	, m_numCols(tableDef->m_numCols), m_logger(NULL), m_mutex("MemHeap::mutex", __FILE__, __LINE__) {
	
	// ��������������
	m_colDescs = new ColumnDesc[m_numCols];
	memset(m_colDescs, 0, sizeof(ColumnDesc) * m_numCols);
	if (cksumCols) { // �Զ���checksum��
		for (u16 i = 0; i < numCksumCols; ++i) {
			assert(cksumCols[i] < m_numCols);
			m_colDescs[cksumCols[i]].m_needCksum = true;
			assert(!m_tableDef->m_columns[cksumCols[i]]->isLob());
		}
	} else { // �䳤��ȫ������Ϊchecksum
		for (u16 i = 0; i < tableDef->m_numCols; ++i) {
			if (m_tableDef->m_columns[i]->isLob()
				|| m_tableDef->m_columns[i]->m_type == CT_VARCHAR
				|| m_tableDef->m_columns[i]->m_type == CT_CHAR)
				m_colDescs[i].m_needCksum = true;
		}
	}
	m_recSize = m_tableDef->m_columns[0]->m_offset;
	for (u16 i = 0; i < m_tableDef->m_numCols; ++i) {
		m_colDescs[i].m_offset = m_recSize;
		m_colDescs[i].m_size = m_colDescs[i].m_needCksum ? CKSUM_SIZE : m_tableDef->m_columns[i]->m_size;
		m_recSize = m_recSize + m_colDescs[i].m_size;
	}
	

	// ��ʼ���ۺͼ�¼
	m_slots = new Slot[m_maxRecCnt];
	m_data = new byte[m_maxRecCnt * m_recSize];
	for (unsigned i = 0; i < m_maxRecCnt; ++i) {
		m_slots[i].m_isFree = true;
		m_slots[i].m_record = new MemHeapRecord(m_tableDef, m_colDescs, i, m_data + m_recSize * i);
		m_slots[i].m_dlink.set(&m_slots[i]);
	}
	// ��ʼ����������
	vector<unsigned> slotIds;	// �������
	slotIds.reserve(m_maxRecCnt);
	for (unsigned i = 0; i < m_maxRecCnt; ++i)
		slotIds.push_back(i);
	random_shuffle(slotIds.begin(), slotIds.end());
	for (unsigned i = 0; i < m_maxRecCnt; ++i) {
		m_freeList.addLast(&m_slots[slotIds[i]].m_dlink);
	}
}

MemHeap::~MemHeap() {
	for (unsigned i = 0; i < m_maxRecCnt; ++i)
		delete m_slots[i].m_record;
	delete m_tableDef;
	delete [] m_data;
	delete [] m_slots;
	delete [] m_colDescs;
}

void MemHeap::setLogger(Syslog *logger) {
	m_logger = logger;
}

MemHeapRid MemHeap::slotToId(const Slot * slot) {
	return (MemHeapRid)((slot - m_slots));
}

/** ����ܹ���ŵļ�¼�� */
unsigned MemHeap::getMaxRecCount() const {
	return m_maxRecCnt;
}

/** �Ѿ���ŵļ�¼�� */
unsigned MemHeap::getUsedRecCount() {
	return m_maxRecCnt - m_freeList.getSize();
}
/**
 * ɾ����¼
 * @pre ��¼�Ѿ���X��
 * @param session �Ự
 * @param id ��¼Ψһ��ʶ
 * @return true�������¼���ڣ����򷵻�false
 */
bool MemHeap::deleteRecord(Session *session, MemHeapRid id) {
	assert(id < m_maxRecCnt);
	
	LOCK(&m_slots[id].m_mutex);
	if (m_slots[id].m_isFree) {
		assert(false);
		UNLOCK(&m_slots[id].m_mutex);
		return false;
	}
	assert(session->isRowLocked(m_tableDef->m_id, m_slots[id].m_record->getRowId(), Exclusived));
	m_slots[id].m_isFree = true;
	if (m_logger)
		m_logger->log(EL_DEBUG, "deleteRecord: id %u, rowid "I64FORMAT"u "
				, id, m_slots[id].m_record->getRowId());
	m_slots[id].m_record->setRowId(INVALID_ROW_ID);
	UNLOCK(&m_slots[id].m_mutex);
	LOCK(&m_mutex);
	m_freeList.addLast(&m_slots[id].m_dlink);
	UNLOCK(&m_mutex);
	return true;
}
/**
 * ��ȡ��¼
 * @pre ��¼�Ѿ�����
 * @param session �Ự
 * @param id ��¼Ψһ��ʶ
 * @param rlh [out] �������, ��ΪNULL��ʾҪ������
 * @param mode ��ģʽ
 * @return ��¼ָ�룬�����¼�����ڣ�����NULL
 */
MemHeapRecord* MemHeap::recordAt(Session *session, MemHeapRid id, RowLockHandle **rlh, LockMode mode) {
	assert(id < m_maxRecCnt);
	MutexGuard guard(&m_slots[id].m_mutex, __FILE__, __LINE__);
	if (m_slots[id].m_isFree)
		return NULL;
	MemHeapRecord *record = m_slots[id].m_record;
	if (rlh) {
		assert(mode != None);
		RowId rid =  m_slots[id].m_record->getRowId();
		assert(rid != INVALID_ROW_ID);
		*rlh = TRY_LOCK_ROW(session, m_tableDef->m_id, rid, mode);
		if (!(*rlh)) { // �������ɹ�
			// �ߵ�����˳���Է�����
			guard.unlock();
			*rlh = LOCK_ROW(session, m_tableDef->m_id, rid, mode);
			guard.lock(__FILE__, __LINE__);
			// ����Ƿ�ԭ��������¼
			if (!m_slots[id].m_isFree && m_slots[id].m_record->getRowId() == rid)
				return record;
			else {
				session->unlockRow(rlh);
				return NULL;
			}
		}
	}
	assert(record->getRowId() != INVALID_ROW_ID);

	assert(session->isRowLocked(m_tableDef->m_id, record->getRowId(), Shared)
		||session->isRowLocked(m_tableDef->m_id, record->getRowId(), Exclusived));

	return record;
}


/**
 * �����ȡһ����¼������������
 * @param session �Ự
 * @param rlh [out] ����
 * @param lockMode ��ģʽ
 * @return �ڴ��¼ָ�룻�Ҳ�����¼����NULL
 * @post ��¼����������
 */
MemHeapRecord* MemHeap::getRandRecord(Session *session, RowLockHandle **rlh, LockMode mode) {
	assert(rlh);
	// ��һ������±꿪ʼɨ��scanLen��Ԫ��
	unsigned start = (unsigned)RandomGen::nextInt(0, m_maxRecCnt);
	for (unsigned i = 0; i < SCAN_LEN; ++i) {
		unsigned idx = (i + start) % m_maxRecCnt;
		if (!m_slots[idx].m_isFree) {
			MemHeapRecord *record = recordAt(session, idx, rlh, mode);
			if (record)
				return record;
		}
	}
	return false;
}

/**
 * ����õ�һ��RowId
 */
RowId MemHeap::getRandRowId() {
	RowId rid;
	getRandRecord(&rid, 0);
	return rid;
}

/**
 * ����õ�һ��MemHeapRid
 */
MemHeapRid MemHeap::getRandId() {
	MemHeapRid id;
	getRandRecord(0, &id);
	return id;
}

/**
 * ����õ�һ����¼
 * @param rid [out] ��¼RowId
 * @param id [out] ��¼id
 * @return bool �ҵ���¼����true�����򷵻�false
 */
bool MemHeap::getRandRecord(RowId *rid, MemHeapRid *id) {
	// ��һ������±꿪ʼɨ��scanLen��Ԫ��
	unsigned start = (unsigned)RandomGen::nextInt(0, m_maxRecCnt);
	for (unsigned i = 0; i < SCAN_LEN; ++i) {
		unsigned idx = (i + start) % m_maxRecCnt;
		if (!m_slots[idx].m_isFree) {
			MutexGuard guard(&m_slots[idx].m_mutex, __FILE__, __LINE__);
			if (!m_slots[idx].m_isFree) {
				if (rid)
					*rid = m_slots[idx].m_record->getRowId();
				if (id)
					*id = m_slots[idx].m_record->getId();
				return true;
			}
		}
	}
	if (rid)
		*rid = INVALID_ROW_ID;
	if (id)
		*id = MemHeapRecord::INVALID_ID;
	return false;
}
/**
 * ����һ����¼
 * @pre �Ѿ���������
 * @param session �Ự
 * @param rid ��¼Rowid
 * @param rec ��¼����
 * @return ��¼Ψһ��ʾ
 */
MemHeapRid MemHeap::insert(Session *session, RowId rid, const byte *rec) {
	assert(session->isRowLocked(m_tableDef->m_id, rid, Exclusived));
	MemHeapRid id = reserveRecord();
	if (id != MemHeapRecord::INVALID_ID) {
		MutexGuard guard(&m_slots[id].m_mutex, __FILE__, __LINE__); // ��ֹ���������ҵ�����
		m_slots[id].m_isFree = false;
		m_slots[id].m_record->setRowId(rid);
		m_slots[id].m_record->update(session, rec);
		if (m_logger)
			m_logger->log(EL_DEBUG, "insert: id %u, rowid "I64FORMAT"u "
			, id, m_slots[id].m_record->getRowId());
	}
	return id;
}
/**
 * Ԥ����¼�ռ�
 *	reserve֮�󣬱������insertAt�������¼;����ʹ��unreserve�黹��¼
 * @return ��¼ID, ���û�п��ü�¼�ռ䣬����INVALID_ID
 */
MemHeapRid MemHeap::reserveRecord() {
	MutexGuard guard(&m_mutex, __FILE__, __LINE__);
	DLink<Slot *> *dlink = m_freeList.getHeader()->getNext();
	if (dlink != m_freeList.getHeader()) {
		dlink->unLink();
		MemHeapRid id = slotToId(dlink->get());
		if (m_logger)
			m_logger->log(EL_DEBUG, "reserveRecord: id %u", id);
		return id;
	} else {
		return MemHeapRecord::INVALID_ID;
	}
}
/**
 * �黹Ԥ����¼
 * @param id ��¼��ʶ
 */
void MemHeap::unreserve(MemHeapRid id) {
	assert(id < m_maxRecCnt);
	assert(m_slots[id].m_isFree);
	MutexGuard guard(&m_mutex, __FILE__, __LINE__);
	m_freeList.addFirst(&m_slots[id].m_dlink);
}
/**
 * ��ָ��λ�ò���һ����¼
 * @pre �Ѿ���������
 * @param session �Ự
 * @param id ��¼��ʶ
 * @param rid ��¼Rowid
 * @param rec ��¼����
 * @return true ����ɹ�������false
 */
bool MemHeap::insertAt(Session *session, MemHeapRid id, RowId rid, const byte *rec) {
	assert(id < m_maxRecCnt);
	assert(session->isRowLocked(m_tableDef->m_id, rid, Exclusived));

	LOCK(&m_mutex);
	if (!m_slots[id].m_isFree) {
		UNLOCK(&m_mutex);
		assert(false);
		return false;
	} else {
		UNLOCK(&m_mutex);
		MutexGuard guard(&m_slots[id].m_mutex, __FILE__, __LINE__);
		m_slots[id].m_isFree = false;
		m_slots[id].m_dlink.unLink();
		m_slots[id].m_record->setRowId(rid);
		m_slots[id].m_record->update(session, rec);
		if (m_logger)
			m_logger->log(EL_DEBUG, "insertAt: id %u, rowid "I64FORMAT"u "
				, id, m_slots[id].m_record->getRowId());
		return true;
	}
}
//////////////////////////////////////////////////////////////////////////
//// MemHeapRecord
//////////////////////////////////////////////////////////////////////////


/**
 * �����ڴ�Ѽ�¼
 * @param tableDef ����
 * @param colDescs ������
 * @param data �ڴ�ռ�
 */
MemHeapRecord::MemHeapRecord(const TableDef *tableDef, const ColumnDesc *colDesc, MemHeapRid id, byte *data)
	: m_tableDef(tableDef), m_colDescs(colDesc)
	, m_numCols(tableDef->m_numCols), m_id(id)
	, m_data(data), m_rowId(INVALID_ROW_ID) {
}
/**
 * ����rowid
 * @param rid ��¼�к�
 */
void MemHeapRecord::setRowId(RowId rid) {
	m_rowId = rid;
}

/**
 * ��ȡRowid
 */
RowId MemHeapRecord::getRowId() const {
	return m_rowId;
}
/** ��ȡid */
MemHeapRid MemHeapRecord::getId() const {
	return m_id;
}

/**
 * ����У��
 * @pre ���в�ΪNULL�� ����Ҫ��checksum
 * @param cno �к�
 * @param record ��¼�ڴ�
 * @return У��
 */
u64 MemHeapRecord::checksumColumn(u16 cno, const byte *record) const {
	assert(cno < m_tableDef->m_numCols);
	assert(m_colDescs[cno].m_needCksum);
	assert(!RedRecord::isNull(m_tableDef, record, cno));
	byte *data;
	size_t size;
	if (m_tableDef->m_columns[cno]->isLob()) {
		RedRecord::readLob(m_tableDef, record, cno, (void **)&data, &size);
	} else if (m_tableDef->m_columns[cno]->m_type == CT_VARCHAR) {
		RedRecord::readVarchar(m_tableDef, record, cno, (void **)&data, &size);
	} else {
		data = (byte *)record + m_tableDef->m_columns[cno]->m_offset;
		size = m_tableDef->m_columns[cno]->m_size;
	}
	return checksum64(data, size);
}

/**
 * ����һ��
 * @param cno �к�
 * @param record ��¼ָ��
 */
void MemHeapRecord::updateColumn(u16 cno, const byte *record) {
	assert(cno < m_tableDef->m_numCols);
	if (RedRecord::isNull(m_tableDef, record, cno)) {
		RedRecord::setNull(m_tableDef, m_data, cno);
	} else {
		if (m_tableDef->m_columns[cno]->m_nullable)
			BitmapOper::clearBit(m_data, m_colDescs[0].m_offset << 3
					, m_tableDef->m_columns[cno]->m_nullBitmapOffset);
		if (m_colDescs[cno].m_needCksum) {
			assert(sizeof(u64) == m_colDescs[cno].m_size);
			*(u64 *)(m_data + m_colDescs[cno].m_offset) = checksumColumn(cno, record);
		} else {
			assert(m_tableDef->m_columns[cno]->m_size == m_colDescs[cno].m_size);
			memcpy(m_data + m_colDescs[cno].m_offset, record + m_tableDef->m_columns[cno]->m_offset, m_colDescs[cno].m_size);
		}
	}
}
/**
 * ���¼�¼
 * @pre ����X
 * @param session �Ự
 * @param record MYSQL��ʽ��¼
 */
void MemHeapRecord::update(Session *session, const byte *record) {
	assert(session->isRowLocked(m_tableDef->m_id, m_rowId, Exclusived));
	// ����ͷ��
	memcpy(m_data, record, m_colDescs[0].m_offset);
	
	// ��������
	for (u16 i = 0; i < m_numCols; ++i)
		updateColumn(i, record);
}


/**
 * ���¼�¼
 * @pre ����X
 * @param session �Ự
 * @param numCols ����
 * @param columns �к�����
 * @param subRecord MYSQL��ʽ�Ӽ�¼
 */
void MemHeapRecord::update(Session *session, u16 numCols, u16 *columns, const byte *subRecord) {
	assert(numCols <= m_numCols);
	assert(session->isRowLocked(m_tableDef->m_id, m_rowId, Exclusived));

	for (u16 i = 0; i < numCols; ++i) {
		updateColumn(columns[i], subRecord);
	}
}

/**
 * �Ƚ���
 * @param cno �к�
 * @param record ��¼����
 * @return ��ͬ����true�����򷵻�false
 */
bool MemHeapRecord::columnEq(u16 cno, const byte *record) const {
	assert(cno < m_tableDef->m_numCols);
	if (RedRecord::isNull(m_tableDef, record, cno))
		return RedRecord::isNull(m_tableDef, m_data, cno);

	if (m_colDescs[cno].m_needCksum) {
		return *(u64 *)(m_data + m_colDescs[cno].m_offset) == checksumColumn(cno, record);
	} else {
		assert(m_colDescs[cno].m_size ==  m_tableDef->m_columns[cno]->m_size);
		return !memcmp(m_data + m_colDescs[cno].m_offset, record + m_tableDef->m_columns[cno]->m_offset, m_colDescs[cno].m_size);
	}
}

/**
 * ��¼�Ƚ�
 * @pre ����
 * @param session �Ự
 * @param record mysql��ʽ��¼
 * @return �����ȷ���true�����򷵻�false
 */
bool MemHeapRecord::compare(Session *session, const byte *record) const {
	assert(session->isRowLocked(m_tableDef->m_id, m_rowId, Shared)
		|| session->isRowLocked(m_tableDef->m_id, m_rowId, Exclusived));

	for (u16 i = 0; i < m_numCols; ++i)
		if (!columnEq(i, record))
			return false;
	return true;
}

/**
 * ���Ӽ�¼�Ƚ�
 * @pre ����
 * @param session �Ự
 * @param numCols ����
 * @param columns �к�����
 * @param subRecord mysql��ʽ�Ӽ�¼
 * @return �����ȷ���true�����򷵻�false
 */
bool MemHeapRecord::compare(Session *session,  u16 numCols, u16 *columns, const byte *subRecord) const {
	assert(session->isRowLocked(m_tableDef->m_id, m_rowId, Shared)
		|| session->isRowLocked(m_tableDef->m_id, m_rowId, Exclusived));
	
	assert(numCols <= m_tableDef->m_numCols);
	for (u16 i = 0; i < numCols; ++i) {
		assert(columns[i] < m_tableDef->m_numCols);
		if (!columnEq(columns[i], subRecord))
			return false;
	}
	return true;
}
/**
 * �Ƚϼ�¼
 * @pre ����
 * @param session �Ự
 * @param rec ���Ƚϼ�¼
 * @return �����ȷ���true�����򷵻�false
 */
bool MemHeapRecord::compare(Session *session, const Record *rec) const {
	if (rec->m_format == REC_MYSQL
		|| rec->m_format == REC_FIXLEN) {
		return compare(session, rec->m_data);
	} else if (rec->m_format == REC_VARLEN) {
		u16 cols [] = {0, 1, 2, 3, 4, 5 ,6 ,7 ,8 ,9, 10};
		assert(m_tableDef->m_numCols < sizeof(cols) / sizeof(cols[0]));
		byte buf[Limits::PAGE_SIZE];
		SubRecord subRec;
		subRec.m_format = REC_REDUNDANT;
		subRec.m_data = buf;
		subRec.m_size = Limits::PAGE_SIZE;
		subRec.m_rowId = INVALID_ID;
		subRec.m_numCols = m_tableDef->m_numCols;
		subRec.m_columns = cols;
		RecordOper::extractSubRecordVR(m_tableDef, rec, &subRec);
		return  compare(session, subRec.m_data);
	} else {
		assert(false);
	}
	return false;
}
/**
 * ��¼�Ƚ�һ��
 * @pre ����
 * @param session �Ự
 * @param record mysql��ʽ��¼
 * @return �����ȷ���true�����򷵻�false
 */
bool MemHeapRecord::compareColumn(Session *session, u16 cno, const byte *rec) const {
	assert(session->isRowLocked(m_tableDef->m_id, m_rowId, Shared)
		|| session->isRowLocked(m_tableDef->m_id, m_rowId, Exclusived));
	return columnEq(cno, rec);
}
/**
 * ת���������ʽRecord
 *	����checksum������ΪNULL
 * @param session �Ự
 * @param record ��¼�ڴ�
 */
void MemHeapRecord::toRecord(Session *session, byte *record) {
	assert(session->isRowLocked(m_tableDef->m_id, m_rowId, Shared)
		|| session->isRowLocked(m_tableDef->m_id, m_rowId, Exclusived));
	// ����ͷ��
	memcpy(record, m_data, m_colDescs[0].m_offset);
	// ��������
	for (u16 i = 0; i < m_numCols; ++i) {
		if (!m_colDescs[i].m_needCksum) {
			assert(m_colDescs[i].m_size ==  m_tableDef->m_columns[i]->m_size);
			memcpy(record + m_tableDef->m_columns[i]->m_offset
				, m_data + m_colDescs[i].m_offset, m_colDescs[i].m_size);
		} else {
			RedRecord::setNull(m_tableDef, record, i);
		}
	}
}
