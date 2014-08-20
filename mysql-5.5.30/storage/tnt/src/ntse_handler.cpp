#ifdef NTSE_KEYVALUE_SERVER
#include "keyvalue/KeyValueServer.h"
#endif

#define MYSQL_SERVER 1
#include <vector>
#include <sstream>
#ifdef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include "ntse_handler.h"
#endif
//#include "misc/Parser.h"
//#include "misc/Session.h"
//#include "misc/Syslog.h"
//#include "misc/RecordHelper.h"
//#include "misc/Record.h"
//#include "misc/ColumnGroupParser.h"
//#include "util/SmartPtr.h"
//#include "util/File.h"
//#include "misc/GlobalFactory.h"
//#include "misc/Global.h"
#include "misc/ParFileParser.h"

#ifndef WIN32
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>
#include "ntse_handler.h"
#endif
#include "api/Database.h"
#include "btree/Index.h"
//#include "ntse_binlog.h"
//#include "mysys_err.h"
#include "RowCache.h"
//#include "ntse_version.h"

#ifdef NTSE_PROFILE
#include "misc/Profile.h"
#endif

using namespace ntse;

//handlerton		*ntse_hton;			/** Handler singleton */
Config			*ntse_config = NULL;		/** ���ݿ����� */
Database		*ntse_db = NULL;			/** ȫ��Ψһ�����ݿ� */
//NTSEBinlog		*ntse_binlog = NULL;		/** ȫ��Ψһ��binlog��¼�� */
//CmdExecutor		*cmd_executor = NULL;		/** ȫ��Ψһ������ִ���� */
//typedef DynHash<const char *, TableInfoEx *, InfoExHasher, Hasher<const char *>, InfoExEqualer> TblHash;
//static TblHash	openTables;			/** �Ѿ��򿪵ı� */
//static ntse::Mutex	openLock("openLock", __FILE__, __LINE__);			/** �����Ѿ��򿪵ı���� */

//HandlerInfo *HandlerInfo::m_instance = NULL;
//HandlerInfo *handlerInfo = NULL;
//static bool isNtseLogBin = false;


///////////////////////////////////////////////////////////////////////////////
// ���ò�����״̬���� //////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
// ������ȫ�����ã��μ�Config��˵��
//static char	*ntse_tmpdir = (char *)".";
//static char	*ntse_version = NULL;
//static ulong	ntse_log_file_size = 0;
//static ulong	ntse_log_buf_size = 0;
//static unsigned long long ntse_page_buf_size = 0;
//static unsigned long long ntse_rec_buf_size = 0;
//static ulong	ntse_max_sessions = 0;
//static ulong	ntse_table_lock_timeout = 0;
//static ulong	ntse_checkpoint_interval = 0;
//ulong			ntse_sample_max_pages = 1000;
//static ulong	ntse_incr_size;	/** �ѡ������������ļ���չҳ���� */
//static ulong	ntse_binlog_buffer_size = 10 * 1024 * 1024;	/** binlog����ʹ�û��������С */
//static char 	*ntse_binlog_method = (char *)"direct";	/** ����ntseдbinlog�ķ�ʽ,����"mysql"��ʾby_mysql/"direct"��ʾdirect/"cached"��ʾcached���� */
#ifdef NTSE_PROFILE
//static int 		ntse_profile_summary = 0; /** ͨ������-1/0/1 ������ȫ��Profile */
//static int		ntse_thread_profile_autorun = 0; /** ͨ������0/1 �������߳�Profile�Ƿ��Զ�����*/
#endif
//static char		ntse_backup_before_recover = 0;
//static char		ntse_verify_after_recover = 0;
//static char		ntse_enable_mms_cache_update = 0;
//static char		ntse_enable_syslog = 1;	/** NTSE��log�Ƿ������mysqld.log */
//static char		ntse_directio = 1; /** �Ƿ�ʹ��directio */

/*static void set_buf_page_from_para(uint *pages, const unsigned long long *para){
	unsigned long long bufSize = *para;
	if (sizeof(unsigned long int) == 4) {
		if (bufSize > (0xFFFFFFFFL)) {
			*pages = 1024;
		} else {
			*pages = (u32) bufSize / NTSE_PAGE_SIZE;
		}	
	} else {
		*pages = (u32)(bufSize / NTSE_PAGE_SIZE);
	}
}*/

static u64	ntse_handler_use_count = 0;
static u64	ntse_handler_use_time = 0;
/*static u64	ntse_binlog_num_trans = 0;
static u64	ntse_binlog_num_logs = 0;
static u64	ntse_binlog_num_insert_logs = 0;
static u64	ntse_binlog_num_update_logs = 0;
static u64	ntse_binlog_num_delete_logs = 0;
static u64	ntse_binlog_cache_size = 0;
static u64	ntse_binlog_cached_logs = 0;*/
static Status	ntse_status;	/** NTSEȫ��ͳ����Ϣ */

/**
 * ��ȡ������Ϣ
 *
 * @return ������Ϣ
 */
THDInfo* getTHDInfo(THD *thd) {
	return *((THDInfo **)thd_ha_data(thd, tnt_hton));
}

/**
 * ����������Ϣ
 *
 * @param THD ����
 * @param info ������Ϣ
 */
/*void setTHDInfo(THD *thd, THDInfo *info) {
	*((THDInfo **)thd_ha_data(thd, ntse_hton)) = info;
}*/

/**
 * ���������Ϣ�Ƿ����ã���û���򴴽�
 *
 * @param thd THD����
 */
/*THDInfo* checkTHDInfo(THD *thd) {
	THDInfo *info = getTHDInfo(thd);
	assert(info != NULL);
	return info;
}*/

/**
 * ����һ��NTSE handler
 *
 * @param hton NTSE�洢����ʵ��
 * @param table handlerҪ�����ı�
 * @param mem_root �ڴ˴���
 * @return �´�����handler
 */
/*static handler* ntse_create_handler(handlerton *hton,
	TABLE_SHARE *table,
	MEM_ROOT *mem_root) {
	ftrace(ts.mysql, tout << hton << table << mem_root);
	return new (mem_root) ntse_handler(hton, table);
}*/

/**
 * ��ʼ��NTSE�洢���棬�����ݿ�
 *
 * @param p handlerton
 * @return �ɹ�����0��ʧ�ܷ���1
 */
int ntse_handler::init(Database *db, Config *config) {
	ntse_db = db;
	ntse_config = config;
	return 0;
}

/**
 * ж��NTSE�洢���棬�ͷ�������Դ
 *
 * @param p handlerton
 * @return ���ǳɹ�������0
 */
int ntse_handler::exit(void *p) {
	ntse_db = NULL;
	ntse_config = NULL;
	return 0;
}

ntse_handler::ntse_handler(handlerton *hton, TABLE_SHARE *table_arg): handler(hton, table_arg) {
	ftrace(ts.mysql, tout << this << hton << table_arg);
	m_session = NULL;
	m_ntseTable = NULL;
	m_ntseScan = NULL;
	m_session = NULL;
	m_errno = 0;
	m_errmsg[0] = '\0';
	ref_length = RID_BYTES + 1;
	m_wantLock = m_gotLock = IL_NO;
	m_replace = false;
	m_ntseIuSeq = NULL;
	m_lastRow = INVALID_ROW_ID;
	m_beginTime = 0;
	m_thd = NULL;
	m_conn = NULL;
	m_isRndScan = false;
	m_rowCache = NULL;
	m_lobCtx = new MemoryContext(Limits::PAGE_SIZE, 1);
	m_isReadAll = false;
	m_increment = 1;
	m_offset = 0;
	m_ignoreDup	= false;
	m_checkSameKeyAfterIndexScan = false;
	m_deferred_read_cache_size = 0;
}


/**
 * ���ر�ɨ����ۡ�Ŀǰʹ�ô洢����ʾ���е�Ĭ��ʵ��
 *
 * @return ��ɨ�����
 */
/*double ntse_handler::scan_time() {
	ftrace(ts.mysql, tout << this);
	return (double) (stats.records + stats.deleted) / 20.0+10;
}*/

/**
 * ���ض�ȡָ��������¼�Ĵ��ۡ�Ŀǰʹ�ô洢����ʾ���е�Ĭ��ʵ��
 *
 * @return ��ȡ����
 */
/*double ntse_handler::read_time(ha_rows rows) {
	ftrace(ts.mysql, tout << this << rows);
	return (double) rows /  20.0 + 1;
}*/

/**
 * ����handler״̬
 *
 * @return ���Ƿ���0
 */
int ntse_handler::reset() {
	ftrace(ts.mysql, tout << this);
	assert(!m_ntseScan);
	m_errno = 0;
	m_errmsg[0] = '\0';
	m_ignoreDup = false;
	m_replace = false;
	m_ntseIuSeq = NULL;
	m_lastRow = INVALID_ROW_ID;
	m_isRndScan = false;
	m_lobCtx->reset();
	bitmap_clear_all(&m_readSet);
	m_deferred_read_cache_size = 0;
	return 0;
}

/**
 * �ر�handler��ǰ�����õı�
 *
 * @return �ɹ�����0��ʧ�ܷ���1������NTSE���Ƿ���0
 */
int ntse_handler::close(void) {
	ftrace(ts.mysql, tout << this);
	m_ntseTable = NULL;
	return 0;
}

/**
 * ����һ��
 *
 * @param buf �����е�����
 * @return �ɹ�����0������Ψһ�Գ�ͻʧ�ܷ���HA_ERR_FOUND_DUPP_KEY��
 *   ���ڼ�¼����ʧ�ܷ�����Ӧ������
 */
int ntse_handler::write_row(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);

	int code = 0;//checkSQLLogBin();
	assert(m_thd->lex->sql_command != SQLCOM_SELECT);
	/*if (code) {
		return code;
	}*/

	if (table->next_number_field && buf == table->record[0]) {
		// ��������
		if ((code = update_auto_increment()))
			// ���ڷ������֧�֣�ֱ�ӷ��ش�����
			return code;
	}

	try {
		if (!m_replace) {
			// ��ʹ��INSERT IGNORE��Ψһ�Գ�ͻʱҲ��Ҫ����HA_ERR_FOUND_DUPP_KEY��
			// �����ϲ�ͳ�Ƴɹ�����ͳ�ͻ�ĸ��ж�������¼
			m_dupIdx = -1;
			if (m_ntseTable->insert(m_session, buf, false, &m_dupIdx, false, (void*)this) == INVALID_ROW_ID)
				code = HA_ERR_FOUND_DUPP_KEY;
		} else {
			assert(m_ignoreDup);
			if (m_ntseIuSeq) {
				// INSERT ... ON DUPLICATE KEY UPDATE�ڷ�����ͻ��������ҪUPDATE����ֵ��ԭ��¼���
				// �򲻻����update_row��������INSERT ... SELECT ... ON DUPLICATE ...ʱ�����ӵ���
				// write_row
				m_ntseTable->freeIUSequenceDirect(m_ntseIuSeq);
				m_ntseIuSeq = NULL;
			}
			m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
			m_ntseIuSeq = m_ntseTable->insertForDupUpdate(m_session, buf, false, (void*)this);
			if (m_ntseIuSeq)
				code = HA_ERR_FOUND_DUPP_KEY;
		}
	} catch (NtseException &e) {
		assert(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		code = reportError(e.getErrorCode(), e.getMessage(), false);
	}

	// �����µ�����ֵ
	setAutoIncrIfNecessary(buf);
	// TODO: ֧�������ʱ�򣬿�����Ҫ�ο�Innodb���������ֶ�

	return code;
}

/**
 * ����һ����¼��new_dataΪ���º�����ԣ���������auto_increment���ͻ��Զ����µ�timestamp
 * ���͵�����ֵ
 * �ڵ���rnd_next/rnd_pos/index_next֮��MySQL������������һ�����������������ļ�¼��
 * ���NTSE���Ǹ��µ�ǰɨ��ļ�¼������Ҫold_data������
 *
 * @param old_data ԭ��¼���ݣ�NTSE����
 * @param new_data �¼�¼����
 * @return �ɹ�����0��Ψһ��������ͻʧ�ܷ���HA_ERR_FOUND_DUPP_KEY����¼������
 * ����ԭ����ʧ��ʱ������Ӧ������
 */
int ntse_handler::update_row(const uchar *old_data, uchar *new_data) {
	ftrace(ts.mysql, tout << this << old_data << new_data);
	assert(m_session && m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);

	try {
		if (m_ntseScan) {
			if (!m_ntseScan->isUpdateColumnsSet()) {
				u16 numUpdateCols;
				u16 *updateCols = transCols(table->write_set, &numUpdateCols);
				m_ntseScan->setUpdateColumns(numUpdateCols, updateCols);
			}
			if (!m_ntseTable->updateCurrent(m_ntseScan, new_data, false, &m_dupIdx, old_data, (void*)this))
				return HA_ERR_FOUND_DUPP_KEY;
		} else if (m_ntseIuSeq) {
			u16 numUpdateCols;
			u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			bool succ = m_ntseTable->updateDuplicate(m_ntseIuSeq, new_data, numUpdateCols, updateCols, &m_dupIdx, (void*)this);
			m_ntseIuSeq = NULL;
			m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
			if (!succ)
				return HA_ERR_FOUND_DUPP_KEY;
		} else {
			// 1. �����������л��ڽ���ɨ��֮���ٵ���update_row
			// create table t1 (a int not null, b int, primary key (a));
			// create table t2 (a int not null, b int, primary key (a));
			// insert into t1 values (10, 20);
			// insert into t2 values (10, 20);
			// update t1, t2 set t1.b = 150, t2.b = t1.b where t2.a = t1.a and t1.a = 10;
			// 2. �м�������ΪSLAVEִ��UPDATE�¼�ʱ
			if (!IntentionLock::isConflict(m_gotLock, IL_IX)) {
				int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not update row after scan ended if table is not locked.", false);
				return code;
			}
			assert(m_lastRow != INVALID_ROW_ID);
			McSavepoint mcSave(m_session->getMemoryContext());

			u16 numReadCols;
			u16 *readCols = transCols(table->read_set, &numReadCols);
			m_ntseScan = m_ntseTable->positionScan(m_session, OP_UPDATE, numReadCols, readCols, false);	// ���ӱ���ʱ�����ܳ��쳣
			byte *buf = (byte *)alloca(m_ntseTable->getTableDef(true, m_session)->m_maxRecSize);
			NTSE_ASSERT(m_ntseTable->getNext(m_ntseScan, buf, m_lastRow, true));

			u16 numUpdateCols;
			u16 *updateCols = transCols(table->write_set, &numUpdateCols);
			m_ntseScan->setUpdateColumns(numUpdateCols, updateCols);
			bool succ;
			try {
				succ = m_ntseTable->updateCurrent(m_ntseScan, new_data, false, &m_dupIdx, old_data, (void*)this);
			} catch (NtseException &e) {
				endScan();
				throw e;
			}

			endScan();
			if (!succ)
				return HA_ERR_FOUND_DUPP_KEY;
		}
	} catch (NtseException &e) {
		assert(e.getErrorCode() == NTSE_EC_ROW_TOO_LONG);
		return reportError(e.getErrorCode(), e.getMessage(), false);
	}

	// ����ĸ�������ֵ�����Բο�InnoDB��ʵ��ע�ͣ�Ч��һ��
	setAutoIncrIfNecessary(new_data);

	return 0;
}

/**
 * ɾ��һ����¼��
 * �ڵ���rnd_next/rnd_pos/index_next֮��MySQL������������һ����ɾ�����������ļ�¼��
 * ���NTSE���Ǹ��µ�ǰɨ��ļ�¼������Ҫbuf������
 * ������insert_idʱ��REPLACEʱ������ͻҲ�����delete_row
 *
 * @param buf ��ǰ��¼���ݣ�NTSE����
 * @return ���Ƿ���0
 */
int ntse_handler::delete_row(const uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);

	if (m_ntseScan) {
		/*
		 *	������������ڴ����洢��m_scanɨ��ļ�¼�ռ�ָ��buf���������ڲ�������ڴ档
		 *  ����������¼�¼ʱ���Ὣĳһ�����¼�¼��д�뵽��һ��������Ȼ����ԭ��������ɾ
		 *	�����������m_scanɨ���¼�Ŀռ�ָ��buf��m_scanɨ���¼�ᱻ���µ��¼�¼���ݣ���
		 *	������������֮���ٻص�ԭ��������m_scanɨ��ʱ�ᷢ����¼�����ڵĴ���
		 */
		if (!m_ntseTable->getTableDef()->hasLob()) {
			m_ntseScan->setCurrentData((byte*)buf);
		}
		
		m_ntseTable->deleteCurrent(m_ntseScan, (void*)this);
	}
	else if (m_ntseIuSeq) {
		m_ntseTable->deleteDuplicate(m_ntseIuSeq, (void*)this);
		m_ntseIuSeq = NULL;
		m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
	} else {
		assert(IntentionLock::isConflict(m_gotLock, IL_IX) && m_lastRow != INVALID_ROW_ID);
		McSavepoint mcSave(m_session->getMemoryContext());

		u16 numReadCols;
		u16 *readCols = transCols(table->read_set, &numReadCols);
		m_ntseScan = m_ntseTable->positionScan(m_session, OP_DELETE, numReadCols, readCols, false);	// ���ӱ���ʱ�����ܳ��쳣
		byte *buf = (byte *)alloca(m_ntseTable->getTableDef(true, m_session)->m_maxRecSize);
		NTSE_ASSERT(m_ntseTable->getNext(m_ntseScan, buf, m_lastRow, true));
		m_ntseTable->deleteCurrent(m_ntseScan, (void*)this);
		endScan();
	}

	return 0;
}

/**
 * ����ɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @param key ����������
 * @param keypart_map �������а�����Щ��������
 * @param find_flag �����������ͣ��緽���Ƿ������ʼ������
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
	enum ha_rkey_function find_flag) {
	ftrace(ts.mysql, tout << this << buf << key << (u64)keypart_map << find_flag);
	assert(m_session);

	if (m_ntseIuSeq) { // ������INSERT FOR DUPLICATE UPDATEʱ��ʹ��index_read_map������rnd_posȡ��¼
		NTSE_ASSERT(find_flag == HA_READ_KEY_EXACT);
		NTSE_ASSERT(active_index == m_ntseIuSeq->getDupIndex());
		transKey(active_index, key, keypart_map, &m_indexKey);
		NTSE_ASSERT(recordHasSameKey( m_ntseTable->getTableDef(true, m_session)->m_indice[active_index], m_ntseIuSeq->getScanHandle()->getIdxKey(), &m_indexKey));
		m_ntseIuSeq->getDupRow(buf);
		return 0;
	}

	bool forward, includeKey;
	switch (find_flag) {
	case HA_READ_KEY_EXACT:
		forward = true;
		includeKey = true;
		break;
	case HA_READ_KEY_OR_NEXT:
		forward = true;
		includeKey = true;
		break;
	case HA_READ_KEY_OR_PREV:
		forward = false;
		includeKey = true;
		break;
	case HA_READ_AFTER_KEY:
		forward = true;
		includeKey = false;
		break;
	case HA_READ_BEFORE_KEY:
		forward = false;
		includeKey = false;
		break;
	/** TODO: �������������NTSE��������֧�֣�NTSE���ᶪʧ����������ܻ᷵��
	 * ����Ľ���������Է���MySQL�ϲ���NTSE���صĴ��������˵�
	 */
	case HA_READ_PREFIX:
		forward = true;
		includeKey = true;
		break;
	case HA_READ_PREFIX_LAST:
		forward = false;
		includeKey = true;
		break;
	default:
		assert(find_flag == HA_READ_PREFIX_LAST_OR_PREV);
		forward = false;
		includeKey = true;
		break;
	}

	return indexRead(buf, key, keypart_map, forward, includeKey, find_flag == HA_READ_KEY_EXACT);
}

/**
 * ��������ɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @param key ����������
 * @param keypart_map �������а�����Щ��������
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_read_last_map(uchar *buf, const uchar *key, key_part_map keypart_map) {
	ftrace(ts.mysql, tout << this << buf << key << (u64)keypart_map);
	return indexRead(buf, key, keypart_map, false, true, false);
}

/**
 * ��������ȫɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_first(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	return indexRead(buf, NULL, 0, true, true, false);
}

/**
 * ��������ȫɨ��
 *
 * @param buf ���������¼�Ļ�����
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_last(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	return indexRead(buf, NULL, 0, false, true, false);
}

/**
 * ��ȡ��һ����¼
 *
 * @param buf ���ڴ洢�����¼����
 * @return �ɹ�����0��û�м�¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_next(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseScan && m_ntseScan->getType() == ST_IDX_SCAN);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	return fetchNext(buf);
}

/**
 * ��ȡǰһ����¼
 *
 * @param buf ���ڴ洢�����¼����
 * @return �ɹ�����0��û�м�¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::index_prev(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session && m_ntseScan && m_ntseScan->getType() == ST_IDX_SCAN);
	DBUG_ENTER("ntse_handler::index_prev");
	ha_statistic_increment(&SSV::ha_read_prev_count);
	assert(bitmap_is_subset(table->read_set, &m_readSet));
	DBUG_RETURN(fetchNext(buf));
}

/**
 * ��������ɨ�裬�ͷ�ɨ��������Դ
 *
 * @return ���Ƿ���0
 */
int ntse_handler::index_end() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ntse_handler::index_end");
	if (m_session)
		endScan();
	active_index = MAX_KEY;
	DBUG_RETURN(0);
}

/**
 * ��ʼ����ɨ��
 *
 * @param scan Ϊtrueʱ����Ҫ����
 * @return һ����ɹ�����0
 */
int ntse_handler::rnd_init(bool scan) {
	ftrace(ts.mysql, tout << this << scan);
	assert(m_session);
	DBUG_ENTER("ntse_handler::rnd_init");

	endScan();	// ��Ϊ�Ӳ�ѯʱ���ܻ�ɨ����
	//fix bug#23575��ɾ������	if (scan && m_rowCache) {delete m_rowCache;	m_rowCache = NULL;}��
	//��m_rowCache�����������ntse_handler::external_lock������sql����ڲ�����m_rowCache������bug��
	//position->rnd_init->rnd_pos����˳��ʱ�����ʱ������m_rowCache.

	int code = 0;//checkSQLLogBin();
	/*if (code) {
		DBUG_RETURN(code);
	}*/

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();
	m_isRndScan = scan;
	bitmap_copy(&m_readSet, table->read_set);

	DBUG_RETURN(code);
}

/**
 * ������ɨ��
 *
 * @return ����0
 */
int ntse_handler::rnd_end() {
	ftrace(ts.mysql, tout << this);
	DBUG_ENTER("ntse_handler::rnd_end");
	if (m_session)
		endScan();
	DBUG_RETURN(0);
}

/**
 * ���ر�����һ����¼
 *
 * @param buf �洢��¼�������
 * @return �ɹ�����0��û����һ�μ�¼����HA_ERR_END_OF_FILE
 */
int ntse_handler::rnd_next(uchar *buf) {
	ftrace(ts.mysql, tout << this << buf);
	assert(m_session);
	DBUG_ENTER("ntse_handler::rnd_next");
	ha_statistic_increment(&SSV::ha_read_rnd_next_count);
	if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do table scan on index only table", false);
		DBUG_RETURN(code);
	}

	// Ϊ������ȷ�ĵõ�read_set����Ϣ,������ų�ʼ��ɨ����
	if (!m_ntseScan) {
		initReadSet(false);
		beginTblScan();
	} else {
		assert(bitmap_is_subset(table->read_set, &m_readSet));
	}
	DBUG_RETURN(fetchNext(buf));
}


/**
 * ���Mysql������FileSort�㷨�� ����ȡ������������ԣ���ȡ�����������������
 * ������������г���֮�ʹ���max_length_for_sort_data�� ���߽�������������ʱ��Mysqlʹ�������㷨���μ�QA89930��
 * 
 * @return true��ʾMySQLʹ��������FileSort�㷨
 */
bool ntse_handler::is_twophase_filesort() {
	bool twophase = true;
	if (m_thd != NULL && m_thd->lex != NULL) {
		MY_BITMAP tmp_set;
		uint32 tmp_set_buf[Limits::MAX_COL_NUM / 8 + 1];	/** λͼ����Ҫ�Ļ��� */
		bitmap_init(&tmp_set, tmp_set_buf, m_ntseTable->getTableDef()->m_numCols, FALSE);
		// ��ס�ɵ�read_set
		MY_BITMAP *save_read_set= table->read_set;
		bitmap_clear_all(&tmp_set);
		table->read_set= &tmp_set;

		List_iterator<Item> list_iter(m_thd->lex->select_lex.item_list);
		Item *curr;
		while ((curr = list_iter++) != NULL) {
			curr->walk(&Item::register_field_in_read_map, 1, (uchar*) table);
		}
		
		// ���ѻ�������ɨ��readSet���ܰ�����ѯ������
		// TODO: ��ξ�ȷ�жϲ�ѯ������м��������ѯ��Mysql����
		if (bitmap_is_subset(table->read_set, &m_readSet))
			twophase = false;
		// �ָ�read_set
		table->read_set = save_read_set;
	}
	return twophase;
}

/**
 * ��¼��ǰ��¼��λ�ã�����NTSE��˵��ΪRID���������ڷ���ˮ������ʱ����
 * �Լ�¼����Ҫ���µļ�¼��λ�ã�����Filesortʱ����
 *
 * @param record ûʲô��
 */
void ntse_handler::position(const uchar *record) {
	ftrace(ts.mysql, tout << this << record);
	assert(m_session && (m_ntseScan || m_gotLock == IL_SIX || m_gotLock == IL_X));
	assert(m_lastRow != INVALID_ROW_ID);

	// �����ʹ�ü�¼�������Լӵ���¼�����У����ɹ����ý���������
	// ������Ҫ�������������������Ѿ��������򲻳��Լӵ���¼��������
	if (m_gotLock == IL_IS) {
		if (!m_rowCache) { // ���Գ�ʼ��rowCache
			if (m_isRndScan || !is_twophase_filesort()) { 
				// ���ɨ���Լ�����filesortʱ������rowCache
				// TODO: ��Ԫfilesortʱ�� ��Ȼmysql������position�����ǲ������rnd_pos��
				//	��ʱ����rowcacheû�����壬����Ϊ�˰�ȫ�������ʱ����rowCache
				u16 numReadCols;
				u16 *readCols =	transCols(&m_readSet, &numReadCols);
				m_rowCache = new RowCache(m_deferred_read_cache_size, m_ntseTable->getTableDef(true, m_session),
					numReadCols, readCols, &m_readSet);
			}
		}
		if (m_rowCache) {
			assert(m_rowCache->hasAttrs(&m_readSet));
			long id = m_rowCache->put(m_lastRow, record);
			if (id >= 0) {
				ref[0] = REF_CACHED_ROW_ID;
				RID_WRITE((RowId)id, ref + 1);
				return;
			}
		}
	}
	ref[0] = REF_ROWID;
	RID_WRITE(m_lastRow, ref + 1);
	if (m_gotLock == IL_IS)
		m_wantLock = IL_S;
	else if (m_gotLock == IL_IX)
		m_wantLock = IL_SIX;
	return;
}

/**
 * ���ݼ�¼λ�ö�ȡ��¼
 *
 * @param buf �洢��¼�������
 * @param pos ��¼λ��
 * @return ���Ƿ���0
 */
int ntse_handler::rnd_pos(uchar *buf, uchar *pos) {
	ftrace(ts.mysql, tout << this << buf << pos);
	assert(m_session);
	DBUG_ENTER("ntse_handler::rnd_pos");
	ha_statistic_increment(&SSV::ha_read_rnd_count);

	if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly) {
		int code = reportError(NTSE_EC_NOT_SUPPORT, "Can not do positional scan on index only table", false);
		DBUG_RETURN(code);
	}

	if (!m_ntseIuSeq) {
		// Ϊ������ȷ�ĵõ�read_set����Ϣ,������ų�ʼ��ɨ����
		if (!m_ntseScan) {
			initReadSet(false, true);
			beginTblScan();
		} else {
			assert(bitmap_is_subset(table->read_set, &m_readSet));
		}

		if (pos[0] == REF_CACHED_ROW_ID) {
			assert(m_rowCache);
			if (!m_rowCache->hasAttrs(&m_readSet)) {
				ntse_db->getSyslog()->log(EL_LOG, "Can not get request attributes from cache. SQL:\n%s", currentSQL());
				int code = reportError(NTSE_EC_GENERIC, "Can not get request attributes from cache", false);
				DBUG_RETURN(code);
			}
			long id = (long)RID_READ(pos + 1);
			m_rowCache->get(id, buf);
		} else {
			RowId rid = RID_READ(pos + 1);
			NTSE_ASSERT(m_ntseTable->getNext(m_ntseScan, buf, rid, true));
		}
	} else {
		assert(pos[0] == REF_ROWID && RID_READ(pos + 1) == m_ntseIuSeq->getScanHandle()->getCurrentRid());
		m_ntseIuSeq->getDupRow(buf);
	}
	table->status = 0;

	DBUG_RETURN(0);
}

/**
 * ɾ�����м�¼
 * ����ʵ��truncate��delete from TABLE_NAME����Ҳ���ܵ��ô˽ӿ�
 *
 * @return �ɹ�����0��ʧ�ܷ��ش�����
 */
int ntse_handler::truncate() {
	ftrace(ts.mysql, tout << this);
	assert(m_ntseTable->getTableDef()->getTableStatus() == TS_NON_TRX);
	assert(m_ntseTable->getMetaLock(m_session) == IL_S);
	int code = 0;
	try {
		m_ntseTable->upgradeMetaLock(m_session, IL_S, IL_X, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
	} catch (NtseException &e) {
		reportError(e.getErrorCode(), e.getMessage(), false);
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	}
	assert(m_gotLock == IL_IX || m_gotLock == IL_X);
	if (m_gotLock == IL_IX) {
		m_wantLock = IL_X;
		try {
			m_ntseTable->upgradeLock(m_session, IL_IX, IL_X, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
		} catch (NtseException &e) {
			reportError(e.getErrorCode(), e.getMessage(), false);
			return HA_ERR_LOCK_WAIT_TIMEOUT;
		}
		m_gotLock = IL_X;
	}
	try {
		deleteAllRowsReal();
		//truncate��delete from TABLE_NAME���п��ܵ��ñ�������
		//��delete from TABLE_NAME����Ҫɾ�����ڵ�ѹ���ֵ�
		/*bool isTruncateOper = (m_thd->lex->sql_command == SQLCOM_TRUNCATE);
		ntse_db->truncateTable(m_session, m_ntseTable, false, isTruncateOper);*/
	} catch (NtseException &e) {
		code = reportError(e.getErrorCode(), e.getMessage(), false);
		return code;
	}
	// ����������ֶε����������ֵ��Ϊ1
	reset_auto_increment_if_nozero(1);

	return code;
}

/**
 * MySQL�ϲ������һ�����ɴ洢��������Խ�Ҫִ�е������Ҫ��ʲô���ı�����
 * ����֧�������Ĵ洢�������ѡ�񽫱��ϵ�д����Ϊ�����ȣ�������ȫ��Ҫ
 * MySQL�ı������Լ�ʵ�ֱ��������߼Ӷ�������������MERGE�洢���棩��
 *
 * �ڲ�ѯ������;��ǰ����ʱMySQLҲ�������һ������������SELECTʱ������Щ
 * ֻ����һ����¼��const���������������ͷ�������ʱ������ñ�������
 *
 * ע: table->in_use����ָ����һ���̣߳���mysql_lock_abort_for_thread()�е���ʱ��
 *
 * ��ش���: thr_lock.c, get_lock_data()
 *
 * @param thd ��ǰ����
 * @param to ������������ڴ洢������
 * @param lock_type ��ģʽ
 * @return ����������βָ��
 */
THR_LOCK_DATA** ntse_handler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) {
	ftrace(ts.mysql, tout << this << thd << to << lock_type);
	if (lock_type != TL_IGNORE && m_mysqlLock.type == TL_UNLOCK) {
		enum_sql_command sql_command = thd->lex->sql_command;
		if (lock_type >= TL_WRITE_ALLOW_WRITE) {
			// ������õ�INSERT/UPDATE/DELETE��䲢��ִ�У������ĸ������
			// �����������Ȳ�һ��Ҳû���⣬��Ϊ�����ã����Ǳ�֤��ȫ�ĺ�
			if (sql_command == SQLCOM_LOCK_TABLES || (thd_in_lock_tables(thd) && !thd->lex->requires_prelocking())) {
				// �����LOCK TABLES������X������Щ����²���LOCk TABLES��䣬���������
				// LOCK TABLES���֮�ڻ�Ҫ������һ����Ҫ�ָ�����LOCK TABLES���Ҫ�ӵ�����
				// ���Ҳ����LOCK TABLES���һ������
				// ������һ��������:
				// lock table t1 write, t3 read;
				// alter table t1 modify i int default 1;
				//�޸�bug#28175��#28180
				//1.����trigger����ġ��١���lock tables��thd->lex->requires_prelocking()Ϊtrue��
				//�������lock tables��Ϊfalse
                //2.���ڴ洢����ʱ������thd_in_lock_tables(thd)Ϊtrue������ʱ
				//thd->lex->requires_prelocking()Ҳ��Ϊtrue��ʶ����Ǽٵ�lock tables��
                //�޸�bug#28380��ǿ���ԣ��Ự������low_priority_updates=1,LOCK TABLE WRITE��ʱ��ᵼ��lock_type=TL_WRITE_LOW_PRIORITY
				assert(lock_type == TL_WRITE || lock_type == TL_WRITE_LOW_PRIORITY);
				m_wantLock = IL_X;
			} else if (sql_command == SQLCOM_UPDATE
				|| sql_command == SQLCOM_DELETE
				|| sql_command == SQLCOM_INSERT || sql_command == SQLCOM_INSERT_SELECT) {
				m_wantLock = IL_IX;
				lock_type = TL_WRITE_ALLOW_WRITE;
			} else if (sql_command == SQLCOM_REPLACE || sql_command == SQLCOM_REPLACE_SELECT) {
				char *dbName = thd->lex->query_tables->get_db_name();				
				char *tableName = thd->lex->query_tables->get_table_name();				
				string path = string(ntse_db->getConfig()->m_basedir) + NTSE_PATH_SEP + dbName + NTSE_PATH_SEP + tableName;
				ParFileParser parser(path.c_str());
				m_wantLock = parser.isPartitionByLogicTabPath() ? IL_X : IL_IX;										
				lock_type = TL_WRITE_ALLOW_WRITE;
			} else if (sql_command == SQLCOM_TRUNCATE)
				m_wantLock = IL_X;
			else if (sql_command == SQLCOM_DELETE_MULTI || sql_command == SQLCOM_UPDATE_MULTI) {
				// ���UPDATE/DELETE��SIX����ԭ��������
				// DELETE t1 FROM t1 WHERE...
				// ֮��Ķ��DELETE�����ڽ���ɨ��֮����������positionȡ��ǰ��¼RID��
				// ����ʱ�����Ѿ��ͷţ����ֻ���ñ�������֤
				// ע������ܵ������������֮��������������������ĳ���Ϊ
				// ��Ҫ����RID��SELECT�������ڳ���S�������������������IS->S
				// ���DELETE����ڳ���SIX����������¼�X����
				// ��һ����ͨ�����������ĳ�ʱ���
				m_wantLock = IL_SIX;
				lock_type = TL_WRITE;
			} else if (sql_command == SQLCOM_ALTER_TABLE || sql_command == SQLCOM_CREATE_INDEX
				|| sql_command == SQLCOM_DROP_INDEX)
				m_wantLock = IL_IS;
			else {
				// ����ǰ������Slave����������ʱ��Ҫ�Ա��SIX��
				// ��Ҫ�Ǵ���Slave�ڸ���update�������ܻ���ֵ���scan���ٽ���scan
				// �õ�rowId֮���ֱ����update�����ʱ��Ϊ���ܶ�λ����ֱ��ʹ��m_lastRow����Ϣ
				// Ϊ�˱�֤�����Ϣ�ɿ�����Ҫ�ӱ�������ֹ����д����
				// ���ʱ��sql_command�������SQLCOM_END���������ֵ���жϷ�֧���ߵ�����
				// ����Slave�Ķ���������Ӧ���ߵ�����
				// ����m_thdδ��ֵ����Ҫʹ�ò���thd�ж��Ƿ�slave
				if (isSlaveThread(thd))
					m_wantLock = IL_SIX;
				else
					m_wantLock = IL_X;
			}
		} else {
			if (sql_command == SQLCOM_LOCK_TABLES || (thd_in_lock_tables(thd) && !thd->lex->requires_prelocking())) {
				m_wantLock = IL_S;
			} else
				m_wantLock = IL_IS;
		}
		m_mysqlLock.type = lock_type;
	}
	*to++ = &m_mysqlLock;
	m_beginTime = System::microTime();
	return to;
}

/**
 * һ������£��κ���俪ʼ����һ�ű�֮ǰ���������һ�����ô洢���������
 * ��������������һ�����ô洢���������
 * ����������LOCK TABLES/UNLOCK TABLES��������LOCK TABLESʱ����store_lock��external_lock������
 * UNLOCK TABLESʱ�������ڼ䲻�ٻ����store_lock��external_lock������
 *
 * @param thd ��ǰ����
 * @param lock_type ΪF_UNLCK��ʾ�������������Ϊ��д��
 * @return �ɹ�����0��������ʱ����HA_ERR_LOCK_WAIT_TIMEOUT��ȡ�����Ự����HA_ERR_NO_CONNECTION
 */
int ntse_handler::external_lock(THD *thd, int lock_type) {
	ftrace(ts.mysql, tout << this << thd << lock_type);
	DBUG_ENTER("ntse_handler::external_lock");
	int code = 0;
	if (lock_type == F_UNLCK) {
		if (m_ntseIuSeq) {
			m_ntseTable->freeIUSequenceDirect(m_ntseIuSeq);
			m_ntseIuSeq = NULL;
		}

		if (!m_thd)	// mysql_admin_table��û�м������Ƿ�ɹ�
			DBUG_RETURN(0);
		assert(m_session);

		endScan();

		// �ڽ���OPTIMIZE�Ȳ���ʱ�Ѿ��ͷ��˱���
		if (m_gotLock != IL_NO) {
			m_ntseTable->unlock(m_session, m_gotLock);
			m_gotLock = IL_NO;
		}
		if (m_ntseTable->getMetaLock(m_session) != IL_NO)
			m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;

		assert(m_beginTime);
		ntse_handler_use_count++;
		ntse_handler_use_time += System::microTime() - m_beginTime;
		m_beginTime = 0;

		/*if (getTHDInfo(m_thd)->m_handlers.getSize() == 1) {
			m_conn->setStatus("Idle");
			getTHDInfo(m_thd)->resetNextCreateArgs();
		}*/

		if (m_rowCache) {
			delete m_rowCache;
			m_rowCache = NULL;
		}

		detachFromTHD();
	} else {
		assert(m_gotLock == IL_NO);
		assert(!m_rowCache);

		// ALTER TABLEʱ����ʱ�������store_lock�������������¼�������
		if (m_wantLock == IL_NO) {
			if (lock_type == F_RDLCK)
				m_wantLock = IL_S;
			else
				m_wantLock = IL_X;
		}

		//attachToTHD(thd, "ntse_handler::external_lock");

		// ����Ự
		/*assert(!m_session);
		m_session = ntse_db->getSessionManager()->allocSession("ntse_handler::external_lock", m_conn, 100);
		if (!m_session) {
			detachFromTHD();
			DBUG_RETURN(HA_ERR_NO_CONNECTION);
		}*/

		// �ӱ���
		assert(m_ntseTable->getMetaLock(m_session) != IL_NO);
		//m_ntseTable->lockMeta(m_session, IL_S, -1, __FILE__, __LINE__);
		//��ʱ�ض��Ƿ��������ӣ���������������ֻ�ܼ�S����IS����
		if (m_ntseTable->getTableDef()->getTableStatus() == TS_TRX && 
			((thd->lex->sql_command != SQLCOM_SELECT && thd->lex->sql_command != SQLCOM_INSERT_SELECT) || m_wantLock != IL_IS)) {
			char msg[256];
			System::snprintf_mine(msg, 256, "Non-Transactional connection only support select transaction table(%s) on IL_IS mode", 
				m_ntseTable->getTableDef()->m_name);
			code = reportError(NTSE_EC_NOT_SUPPORT, msg, false);
			m_ntseTable->unlockMeta(m_session, IL_S);
			ntse_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			detachFromTHD();
			DBUG_RETURN(code);
		}

		try {
			if (m_wantLock == IL_X)
				ntse_db->getSyslog()->log(EL_LOG, "acquire IL_X on table %s. SQL:\n%s", m_ntseTable->getTableDef()->m_name, currentSQL());
			m_ntseTable->lock(m_session, m_wantLock, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			m_gotLock = m_wantLock;
		} catch (NtseException &e) {
			code = reportError(e.getErrorCode(), e.getMessage(), false);
			m_ntseTable->unlockMeta(m_session, IL_S);
			ntse_db->getSessionManager()->freeSession(m_session);
			m_session = NULL;
			detachFromTHD();
			DBUG_RETURN(code);
		}
		/*if (getTHDInfo(m_thd)->m_handlers.getSize() == 1) {
			m_conn->setStatus("Busy");
			m_conn->getLocalStatus()->countIt(OPS_OPER);
		}*/

		if (!m_beginTime)
			m_beginTime = System::microTime();
	}
	DBUG_RETURN(code);
}

/** ��LOCK TABLES����������MySQL����俪ʼʱ����ô˺�����������ʱ�������
 *
 * @param thd ����
 * @param lock_type �Ա�ӵ���
 * @return ���Ƿ���0
 */
int ntse_handler::start_stmt(THD *thd, thr_lock_type lock_type) {
	ftrace(ts.mysql, tout << this << thd << lock_type);
	DBUG_ENTER("ntse_handler::start_stmt");
	if (thd != m_thd)
		DBUG_RETURN(reportError(NTSE_EC_NOT_SUPPORT, "THD changed during sql processing", false));
	//THDInfo *info = getTHDInfo(m_thd);
	/*if (info->m_handlers.getHeader()->getNext()->get() == this) {
		m_conn->setStatus("Busy");
		m_conn->getLocalStatus()->countIt(OPS_OPER);
	}*/
	DBUG_RETURN(0);
}

/**
 * ɾ����
 * ��ɾ����֮ǰ�����жԱ�����ö��Ѿ����ر�
 *
 * @param name ���ļ�·����������׺��
 * @return �ɹ�����0�������ڷ���HA_ERR_NO_SUCH_TABLE���������󷵻�HA_ERR_GENERIC
 */
int ntse_handler::delete_table(const char *name) {
	ftrace(ts.mysql, tout << this << name);

	attachToTHD(ha_thd(), "ntse_handler::delete_table");
	int ret = 0;
	m_session = ntse_db->getSessionManager()->allocSession("ntse_handler::delete_table", m_conn, 100);
	if (!m_session) {
		ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
	} else {
		try {
			dropTableReal(name);
		} catch (NtseException &e) {
			ret = reportError(e.getErrorCode(), e.getMessage(), false);
		}
	}
	if (m_session) {
		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}
	detachFromTHD();

	return ret;
}

/**
 * �����������ô˺���ʱhandler����Ϊ�ոմ���û��open�Ķ���
 *
 * @param from ���ļ�ԭ·��
 * @param to ���ļ���·��
 */
int ntse_handler::rename_table(const char *from, const char *to) {
	ftrace(ts.mysql, tout << this << from << to);
	assert(!m_session);

	attachToTHD(ha_thd(), "ntse_handler::rename_table");
	int ret = 0;
	m_session = ntse_db->getSessionManager()->allocSession("ntse_handler::rename_table", m_conn, 100);
	if (!m_session) {
		ret = reportError(NTSE_EC_TOO_MANY_SESSION, "Too many sessions", false);
	} else {
		char *from_copy = System::strdup(from);
		char *to_copy = System::strdup(to);
		normalizePathSeperator(from_copy);
		normalizePathSeperator(to_copy);
		try {
			renameTableReal(from_copy, to_copy);
		} catch (NtseException &e) {
			ret = reportError(e.getErrorCode(), e.getMessage(), false);
		}
		delete []from_copy;
		delete []to_copy;
	}
	if (m_session) {
		ntse_db->getSessionManager()->freeSession(m_session);
		m_session = NULL;
	}
	detachFromTHD();

	return ret;
}

/**
 * ����ָ����������[min_key, max_key]֮��ļ�¼��
 *
 * @param inx �ڼ�������
 * @param min_key ���ޣ�����ΪNULL
 * @param max_key ���ޣ�����ΪNULL
 */
ha_rows ntse_handler::records_in_range(uint inx, key_range *min_key, key_range *max_key) {
	ftrace(ts.mysql, tout << this << inx << min_key << max_key);

	SubRecord minSr, maxSr;
	SubRecord *pMinSr = NULL, *pMaxSr = NULL;
	bool includeMin = false, includeMax = false;
	if (min_key) {
		transKey(inx, min_key->key, min_key->keypart_map, &minSr);
		pMinSr = &minSr;
		includeMin = isRkeyFuncInclusived(min_key->flag, true);
	}
	if (max_key) {
		transKey(inx, max_key->key, max_key->keypart_map, &maxSr);
		pMaxSr = &maxSr;
		includeMax = isRkeyFuncInclusived(max_key->flag, false);
	}
	u64 rows = m_ntseTable->getIndice()->getIndex(inx)->recordsInRangeSecond(m_session,
		pMinSr, includeMin, pMaxSr, includeMax);
	
	return rows;
}

/** NTSE�Ǳ�׼����������Ϣ */
struct IdxDefEx {
	const char	*m_name;		/** ������ */
	s8			m_splitFactor;	/** ����ϵ�� */

	IdxDefEx(const char *name, s8 splitFactor) {
		m_name = System::strdup(name);
		m_splitFactor = splitFactor;
	}
};

/** �ж�һ�����������Ƿ�Ϊǰ׺
 * @param table_arg ����
 * @param key_part ��������
 * @return �Ƿ�Ϊǰ׺
 */
bool ntse_handler::isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part) {
	// ����Ӧ��ͨ��HA_PART_KEY_SEG��־���ж��Ƿ���Ϊǰ׺����MySQL��û����ȷ���������ֵ��
	// ����ο�InnoDB�е��ж��߼�
	Field *field = table_arg->field[key_part->field->field_index];
	assert(!strcmp(field->field_name, key_part->field->field_name));
	if (field->type() == MYSQL_TYPE_BLOB || field->type() == MYSQL_TYPE_MEDIUM_BLOB 
		|| (key_part->length < field->pack_length() && field->type() != MYSQL_TYPE_VARCHAR)
		|| (field->type() == MYSQL_TYPE_VARCHAR && key_part->length < field->pack_length() - ((Field_varstring*)field)->length_bytes))
		return true;
	return false;
}

void ntse_handler::prepareAddIndexReal() throw(NtseException) {
	// �ͷű���������NTSE�ײ�Ĵ���
	m_ntseTable->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
}

// TODO:
// ɾ������ʧ�ܽ�����NTSE�ڲ�������MySQL��һ�£�����һ����ǳ��ѽ���������ԣ�InnoDB plugin
// �ڴ�����ɾ�����������б���Ҳ�ᵼ�¸ñ��ָܻ����������ڴ�����ɾ�������漰��MySQL�ϲ�ά���ı���
// ��洢�����ڲ�ά���ı���������Դ��ͬ���޸ģ�û�зֲ�ʽ������ƣ��������޷���֤ͬ���޸ģ�������
// ��һ�½����±��ܲ�������������ܵ��·���������

/**
 * ���߽�����
 *
 * @param table_arg ����
 * @param key_info ������������
 * @param num_of_keys ��������������
 * @return �ɹ�����0��ʧ�ܷ��ش�����
 */
int ntse_handler::add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys,
							handler_add_index **add) {
	ftrace(ts.mysql, tout << this << table_arg << key_info << num_of_keys);

	assert(m_conn && m_session);

	//int i = 0, j = 0;
	//bool online = false;

	//McSavepoint mc(m_session->getMemoryContext());
	//IndexDef **indexDefs = (IndexDef **)m_session->getMemoryContext()->alloc(sizeof(IndexDef *) * num_of_keys);
	int ret = 0;
	try {
		/*if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly) {
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not add index to index only table.");
		}

		if (num_of_keys > 1 && isSlaveThread()) {
			// ����Slave�������ֻҪMasterִ�гɹ�������ҲҪ���ִ�гɹ�����Ϊntse_index_build_algorithmֻ�ǻỰ˽�б���
			// ���ƹ����в��ᱻ���ƹ��������QA38567
			online = true;
			sql_print_warning("The create multiple index is from Master to Slave, temporary reset it to online to build multiple indices.");
		}

		for (i = 0; i < (int)num_of_keys; i++) {
			KEY *key = key_info + i;
			bool unique = (key->flags & HA_NOSAME) != 0;

			// �����������Ƿ���������̫�����ˣ�������InnoDB plugin������ôʵ�ֵ�
			bool primaryKey = !my_strcasecmp(system_charset_info, key->name, "PRIMARY");
			// ����MySQL��Ϊ�������ǵ�һ����������NTSEĿǰû����ô�������Ŀǰǿ��Ҫ������ֻ����Ϊ
			// ��һ������
			if (primaryKey && m_ntseTable->getTableDef(true, m_session)->m_numIndice)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Primary key must be the first index.");

			// ����MySQL��Ϊ��Ψһ������������Ψһ������֮��NTSEĿǰû����ô����Ϊ��֤��ȷ�ԣ�Ҫ��Ψһ������
			// �����ڷ�Ψһ������֮ǰ����
			if (unique && m_ntseTable->getTableDef(true, m_session)->getNumIndice(false) > 0)
				NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unique index must be created before non-unique index.");

			ColumnDef **columns = new (m_session->getMemoryContext()->alloc(sizeof(ColumnDef *) * key->key_parts)) ColumnDef *[key->key_parts];
			for (uint j = 0; j < key->key_parts; j++) {
				if (isPrefix(table_arg, key->key_part + j))
					NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Prefix key is not supported: %s", key->name);
				columns[j] = m_ntseTable->getTableDef(true, m_session)->m_columns[key->key_part[j].field->field_index];
			}

			indexDefs[i] = new IndexDef(key->name, key->key_parts, columns, unique, primaryKey, online);
		}

		// �ͷű���������NTSE�ײ�Ĵ���
		m_ntseTable->unlock(m_session, m_gotLock);
		m_gotLock = IL_NO;
		m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

		ntse_db->addIndex(m_session, m_ntseTable, (u16)num_of_keys, (const IndexDef **)indexDefs);
		if (online) {
			for (j = 0; j < (int)num_of_keys; j++) {
				indexDefs[j]->m_online = false;
			}
			ntse_db->addIndex(m_session, m_ntseTable, (u16)num_of_keys, (const IndexDef **)indexDefs);
		}*/
		addIndexReal(table_arg, key_info, num_of_keys, add);
	} catch (NtseException &e) {
		if (e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION)
			m_dupIdx = MAX_KEY;
		ret = reportError(e.getErrorCode(), e.getMessage(), false, e.getErrorCode() == NTSE_EC_INDEX_UNQIUE_VIOLATION);
	}

	return ret;
}

/**
 * ��ɴ��������������裬���ύ��ɻ�ع�֮ǰ�Ѵ���������
 *
 * @param  add ��add_index�����������½����������Ķ�����Ҫ���ڻع�
 * @param  commit true����commit, false������Ҫrollback
 * @return �ɹ�����0�����󷵻ش�����
 */
int ntse_handler::final_add_index(handler_add_index *add, bool commit) {
	int ret = 0;
	try {
		finalAddIndexReal(add, commit);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}
	delete add;
	return ret;
}



/**
 * ׼��ɾ����������NTSE����ʵ�Ѿ���������ɾ��
 *
 * @param table_arg ����
 * @param key_num Ҫɾ��������������飬����֤һ����˳���ź����
 * @param num_of_keys Ҫɾ������������
 */
int ntse_handler::prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys) {
	ftrace(ts.mysql, tout << this << table_arg << key_num << num_of_keys);
	assert(m_conn && m_session);

	int ret = 0;
	try {
		/*if (num_of_keys > 1)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can't drop multiple indice in one statement.");
		if (m_ntseTable->getTableDef(true, m_session)->m_indexOnly)
			NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Can not drop index of index only table.");
		ntse_db->dropIndex(m_session, m_ntseTable, key_num[0]);*/
		dropIndexReal(table_arg, key_num, num_of_keys);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}

	return ret;
}

/**
 * ���ɾ��������������NTSE��ʲôҲ����
 *
 * @param table_arg ����
 * @return ���Ƿ���0
 */
int ntse_handler::final_drop_index(TABLE *table_arg) {
	ftrace(ts.mysql, tout << this << table_arg);
	return 0;
}

/** ����ͳ����Ϣ
 *
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ���ʱ����HA_ADMIN_FAILED
 */
/*int ntse_handler::analyze(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::analyze");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	m_ntseTable->getHeap()->updateExtendStatus(m_session, ntse_sample_max_pages);
	for (u16 i = 0; i < m_ntseTable->getTableDef(true, m_session)->m_numIndice; i++) {
		m_ntseTable->getIndice()->getIndex(i)->updateExtendStatus(m_session, ntse_sample_max_pages);
	}
	if (m_ntseTable->getLobStorage())
		m_ntseTable->getLobStorage()->updateExtendStatus(m_session, ntse_sample_max_pages);

	DBUG_RETURN(HA_ADMIN_OK);
}*/

/** �Ż�������
 *
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ�����ʧ��ʱ����HA_ADMIN_FAILED
 */
int ntse_handler::optimize(THD *thd, HA_CHECK_OPT *check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::optimize");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	//�������˽������ntse_keep_compress_dictionary_on_optimize
	//bool keepDict = THDVAR(m_thd, keep_dict_on_optimize);
	//TODO
	bool keepDict = false;//����TNT�ݲ�֧��optimizeѹ��

	// �ͷű���������NTSE�ײ�Ĵ���
	m_ntseTable->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
	m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

	int ret = 0;
	try {
		optimizeTableReal(keepDict, false);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}
	DBUG_RETURN(!ret ? HA_ADMIN_OK: HA_ADMIN_FAILED);
}

/** ��������һ����
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ�����ʧ��ʱ����HA_ADMIN_FAILED�����ݲ�һ�·���HA_ADMIN_CORRUPT
 */
int ntse_handler::check(THD* thd, HA_CHECK_OPT* check_opt) {
	UNREFERENCED_PARAMETER(check_opt);
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::check");

	if (!m_thd)
		DBUG_RETURN(HA_ADMIN_FAILED);

	// �ͷű���������NTSE�ײ�Ĵ���
	m_ntseTable->unlock(m_session, m_gotLock);
	m_gotLock = IL_NO;
	m_ntseTable->unlockMeta(m_session, m_ntseTable->getMetaLock(m_session));

	int ret = 0;
	try {
		m_ntseTable->verify(m_session);
	} catch (NtseException &e) {
		ret = reportError(e.getErrorCode(), e.getMessage(), false);
	}
	DBUG_RETURN(!ret ? HA_ADMIN_OK: HA_ADMIN_CORRUPT);
}

/** �����������еĲ�һ��
 * @param thd ʹ�ñ�handler������
 * @param check_opt ����
 * @return �ɹ�����HA_ADMIN_OK��û�гɹ��ӱ�����ʧ��ʱ����HA_ADMIN_FAILED
 */
int ntse_handler::repair(THD* thd, HA_CHECK_OPT* check_opt) {
	ftrace(ts.mysql, tout << this << thd << check_opt);
	DBUG_ENTER("ntse_handler::repair");

	// OPTIMIZE��������ؽ��������������������Ĳ�һ��
	int ret = optimize(thd, check_opt);

	DBUG_RETURN(ret);
}

/**
 * �õ�������Ϣ
 *
 * @error ��һ�β���ʱ���صĴ����
 * @param buf OUT�����������Ϣ��MySQL
 * @return �Ƿ�����ʱ����
 */
/*bool ntse_handler::get_error_message(int error, String* buf) {
	ftrace(ts.mysql, tout << this << error << buf);
	if (error != m_errno)
		return false;
	buf->append(m_errmsg);
	m_errno = 0;
	return true;
}*/

/** ��·���е�Ŀ¼�ָ���ͳһת����/ */
void ntse_handler::normalizePathSeperator(char *path) {
	for (char *p = path; *p; ) {
		char *slash = strchr(p, '\\');
		if (slash) {
			*slash = '/';
			p = slash + 1;
		} else
			break;
	}
}

/**
 * ����MySQL���ַ����õ�NTSEʹ�õ�Collation
 *
 * @param charset �ַ���
 * @return Collation
 * @throw NtseException ��֧�ֵ��ַ���
 */
CollType ntse_handler::getCollation(CHARSET_INFO *charset) throw(NtseException) {
	if (!charset->sort_order)
		return COLL_BIN;
	else if (charset == &my_charset_gbk_chinese_ci)
		return COLL_GBK;
	else if (charset == &my_charset_utf8_general_ci)
		return COLL_UTF8;
	else if (charset == &my_charset_utf8mb4_general_ci)
		return COLL_UTF8MB4;
	else if (charset == &my_charset_latin1)
		return COLL_LATIN1;
	else
		NTSE_THROW(NTSE_EC_NOT_SUPPORT, "Unsupported charset %s", charset->name);
}

/**
 * �������ȷʵ���ȼ�¼�����ȴ�get_error_messageʱ�ŷ���
 *
 * @param errCode �쳣��
 * @param msg ������Ϣ
 * @param fatal �Ƿ�Ϊ���ش������ش��󽫵���ϵͳ�˳�
 * @param warnOrErr ��Ϊtrue�������push_warning_printf���澯�棬��Ϊfalse�����my_printf_error�������
 * @return ���ظ�MySQL�Ĵ�����
 */
int ntse_handler::reportError(ErrorCode errCode, const char *msg, bool fatal, bool warnOrErr) {
	if (ntse_db)
		ntse_db->getSyslog()->log(fatal? EL_PANIC: EL_ERROR, "%s", msg);

	m_errno = excptToMySqlError(errCode);
	int textNo = getMysqlErrCodeTextNo(m_errno);
	if(textNo != 0) {
		if (warnOrErr)
			push_warning_printf(ha_thd(), MYSQL_ERROR::WARN_LEVEL_WARN, textNo, "%s", msg);
		else
			my_printf_error(textNo, "%s", MYF(0), msg);
	}
	strncpy(m_errmsg, msg, sizeof(m_errmsg) - 1);
	m_errmsg[sizeof(m_errmsg) - 1] = '\0';
	if (fatal)
		::exit(-1);

	return m_errno;
}

/**	��NTSE��Error Codeת��ΪMySQL�ϲ��Error Code�����ұ�ʶ����״̬
* @param code	TNT�����Error Code
* @param thd	���������Ӧ��MySQL�߳�
* @return		����MySQL��Error Code
*/
int ntse_handler::excptToMySqlError(ErrorCode code) {
	if (code == NTSE_EC_INDEX_UNQIUE_VIOLATION)
		return HA_ERR_FOUND_DUPP_KEY;
	else if (code == NTSE_EC_ROW_TOO_LONG)
		return HA_ERR_TO_BIG_ROW;
	else if (code == NTSE_EC_LOCK_TIMEOUT)
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	else if (code == NTSE_EC_DEADLOCK)
		return HA_ERR_LOCK_DEADLOCK;
	else if (code == NTSE_EC_NOT_SUPPORT) 
		return HA_ERR_UNSUPPORTED;
	else if (code == NTSE_EC_FILE_NOT_EXIST)
		return HA_ERR_NO_SUCH_TABLE;
	else
		return HA_ERR_LAST + (int)code + 1;
}

/**	��MySQL�ϲ��HA������ת����������ERR������
* @param code	mysql�ϲ��HA������Error Code
* @param thd	���������Ӧ��MySQL�߳�
* @return		����MySQL��Error Code
*/
int ntse_handler::getMysqlErrCodeTextNo(int code) {
	switch (code) {
		case HA_ERR_FOUND_DUPP_KEY:
			return ER_DUP_ENTRY;
		case HA_ERR_LOCK_WAIT_TIMEOUT:
			return ER_LOCK_WAIT_TIMEOUT;
		case HA_ERR_LOCK_DEADLOCK:
			return ER_LOCK_DEADLOCK;
		case HA_ERR_UNSUPPORTED:
			return ER_UNSUPPORTED_EXTENSION;
		case HA_ERR_NO_SUCH_TABLE:
			return ER_NO_SUCH_TABLE;
		default:
			return 0;	
	}
}


/** 
 * ��ȡ��ǰSQL
 * @return ��ǰSQL
 */
const char* ntse_handler::currentSQL() {
	return (m_thd == NULL) ? "NULL" : (m_thd->query() == NULL ? "NULL" : m_thd->query());
}
/**
 * ����Ƿ���Ҫ������������Ҫʱ����
 *
 * @return �Ƿ�ɹ�
 */
bool ntse_handler::checkTableLock() {
	if (m_wantLock != m_gotLock) {
		assert(m_wantLock > m_gotLock);
		ntse_db->getSyslog()->log(EL_LOG, "Upgrade lock %s to %s on table %s. SQL:\n%s",
			IntentionLock::getLockStr(m_gotLock), IntentionLock::getLockStr(m_wantLock),
			m_ntseTable->getTableDef()->m_name, currentSQL());

		try {
			if (m_gotLock == IL_IS && m_wantLock == IL_S) {
				m_ntseTable->upgradeLock(m_session, m_gotLock, m_wantLock, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			} else if (m_gotLock == IL_IX && m_wantLock == IL_SIX) {
				m_ntseTable->downgradeLock(m_session, IL_IX, IL_IS, __FILE__, __LINE__);
				m_ntseTable->upgradeLock(m_session, IL_IS, IL_SIX, ntse_db->getConfig()->m_tlTimeout * 1000, __FILE__, __LINE__);
			} else {
				char buf[100];
				System::snprintf_mine(buf, sizeof(buf), "Upgrade table lock from %s to %s is unexpected.",
						IntentionLock::getLockStr(m_gotLock), IntentionLock::getLockStr(m_wantLock));
				reportError(NTSE_EC_NOT_SUPPORT, buf, false);
				return false;
			}
		} catch (NtseException &e) {
			reportError(e.getErrorCode(), e.getMessage(), false);
			return false;
		}
		m_gotLock = m_wantLock;
	}
	return true;
}

/**
 * ת��MySQL��λͼ��ʾ�Ĳ����漰���Լ�ΪNTSE��ʽ
 *
 * @param bitmap MySQL���Բ���λͼ
 * @param numCols OUT�����Ը���
 * @param idxScan �Ƿ�Ϊ����ɨ�裬Ĭ��Ϊfalse
 * @param idx ��Ϊ����ɨ�裬��ʹ�õ�����num
 * @return �����Ժţ��ӻỰ��MemoryContext�з���
 */
u16* ntse_handler::transCols(MY_BITMAP *bitmap, u16 *numCols, bool idxScan, uint idx) {
	
	int columns = bitmap_bits_set(bitmap);

	// TODO ���õĴ�����ȡ�κ����Ե����
	if (columns == 0) {
		*numCols = 1;
		u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16));

		if (idxScan) {
			// ����ɨ�裬��ȡ�����ĵ�һ��
			assert(idx < m_ntseTable->getTableDef(true, m_session)->m_numIndice);
			IndexDef *indexDef = m_ntseTable->getTableDef(true, m_session)->m_indice[idx];

			r[0] = indexDef->m_columns[0];
		} else {
			// ȫ��ɨ�裬��ȡ��ĵ�һ��
			r[0] = 0;
		}
		return r;
	}
	*numCols = (u16)columns;
	u16 *r = (u16 *)m_session->getMemoryContext()->alloc(sizeof(u16) * columns);
	u16 readCols = 0, i = 0;
	Field **field;
	for (field = table->field; *field; field++, i++) {
		if (bitmap_is_set(bitmap, (*field)->field_index)) {
			r[readCols++] = i;
		}
	}
	assert(readCols == columns);
	return r;
}

/** �ж�ָ����������ʽ�Ƿ������ֵ����
 * @param flag ������ʽ
 * @param lowerBound true��ʾ��ɨ�跶Χ���½磬false��ʾ���Ͻ�
 * @return �Ƿ������ֵ����
 */
bool ntse_handler::isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound) {
	switch (flag) {
		/*case HA_READ_KEY_EXACT:
		case HA_READ_KEY_OR_NEXT:
		case HA_READ_KEY_OR_PREV:
		case HA_READ_PREFIX:
		case HA_READ_PREFIX_LAST:
		case HA_READ_PREFIX_LAST_OR_PREV:
			return true;
		case HA_READ_AFTER_KEY:
			return !lowerBound;
		default:
			return false;*/

		// ��Щ�������������Equal
		case HA_READ_KEY_EXACT:
		case HA_READ_KEY_OR_NEXT:
		case HA_READ_KEY_OR_PREV:
		case HA_READ_PREFIX:
		case HA_READ_PREFIX_LAST:
		case HA_READ_PREFIX_LAST_OR_PREV:
			return true;
			// ��������������������Equal
		case HA_READ_AFTER_KEY:
			return !lowerBound;
		case HA_READ_BEFORE_KEY:
			return lowerBound;
		default:
			return false;
	};
}

/**
 * ת��MySQL����������(KEY_MYSQL)ΪNTSE�ڲ���ʽ(KEY_PAD)��ʽ���洢��out��
 * @pre �Ѿ����˱�Ԫ������
 *
 * @param idx �ڼ�������
 * @param key MySQL�ϲ��������������ΪKEY_MYSQL��ʽ
 * @param keypart_map �������а�����Щ��������
 * @param out �洢ת����Ľ�����ڴ��m_session���ڴ��������з���
 */
bool ntse_handler::transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out) {
	assert(idx < m_ntseTable->getTableDef(true, m_session)->m_numIndice);
	IndexDef *indexDef = m_ntseTable->getTableDef(true, m_session)->m_indice[idx];

	SubRecord mysqlKey;
	mysqlKey.m_format = KEY_MYSQL;
	mysqlKey.m_data = (byte *)key;
	mysqlKey.m_columns = indexDef->m_columns;
	parseKeyparMap(idx, keypart_map, &mysqlKey.m_size, &mysqlKey.m_numCols);

	out->m_columns = indexDef->m_columns;
	out->m_data = (byte *)m_session->getMemoryContext()->alloc(indexDef->m_maxKeySize);
	out->m_numCols = mysqlKey.m_numCols;
	out->m_format = KEY_PAD;
	out->m_size = indexDef->m_maxKeySize;
	out->m_rowId = INVALID_ROW_ID;

	return RecordOper::convertKeyMP(m_ntseTable->getTableDef(true, m_session), indexDef, &mysqlKey, out);
}

/**
 * ������key_part_map�ṹ��ʾ���������а�����Щ����������Ϣ
 *
 * @param idx �ڼ�������
 * @param keypart_map �������а�����Щ����������Ϣ
 * @param key_len OUT������������
 * @param num_cols OUT���������а���������������
 */
void ntse_handler::parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols) {
	DBUG_ASSERT(((keypart_map + 1) & keypart_map) == 0);	// ֻ֧��ǰn������

	KEY *key_info = table->s->key_info + idx;
	KEY_PART_INFO *key_part = key_info->key_part;
	KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;
	uint length = 0, cnt = 0;

	while (key_part < end_key_part && keypart_map) {
		length += key_part->store_length;
		keypart_map >>= 1;
		key_part++;
		cnt++;
	}

	*key_len = length;
	*num_cols = (u16)cnt;
}

/** �õ���������
 * @param return ��������
 */
OpType ntse_handler::getOpType() {
	// �޷����ݵ�ǰ��������жϲ������ͣ�������Ϊ�������������������
	// 1. ������UPDATE���ܵ���DELETE
	// 2. ��ΪSLAVEʱ����ǰ�����SQLCOM_END
	if (m_gotLock == IL_IS || m_gotLock == IL_S)
		return OP_READ;
	else
		return OP_WRITE;
}

/** ��ɨ�������ɨ��ʱ��ȡ��һ����¼����������ɨ��ʱʵ������ǰһ����¼��
 * @post m_lastRow�м�¼�˸ջ�ȡ�ɹ��ļ�¼��RID
 *
 * @param buf OUT���洢��ȡ�ļ�¼����
 * @return �ɹ�����0��ʧ�ܷ��ش�����
 */
int ntse_handler::fetchNext(uchar *buf) {
	table->status = STATUS_NOT_FOUND;
	if (!checkTableLock())
		return HA_ERR_LOCK_WAIT_TIMEOUT;

	m_lobCtx->reset();
	if (!m_ntseTable->getNext(m_ntseScan, buf, INVALID_ROW_ID, true))
		return HA_ERR_END_OF_FILE;
	if (m_ntseScan->getType() == ST_IDX_SCAN
		&& m_checkSameKeyAfterIndexScan
		&& !recordHasSameKey(m_ntseTable->getTableDef(true, m_session)->m_indice[active_index], m_ntseScan->getIdxKey(), &m_indexKey))
		return HA_ERR_END_OF_FILE;
	m_lastRow = m_ntseScan->getCurrentRid();
	table->status = 0;
	return 0;
}

/** �����ڽ���ɨ�������֮
 * @post �����ڽ���ɨ�裬��ײ��ɨ���Ѿ��������Ự��MemoryContext������
 *   ����ʼɨ��ǰ��״̬
 */
void ntse_handler::endScan() {
	assert(m_session);
	m_isRndScan = false;
	if (m_ntseScan) {
		m_ntseTable->endScan(m_ntseScan);
		m_ntseScan = NULL;
		m_session->getMemoryContext()->resetToSavepoint(m_mcSaveBeforeScan);
	}
	bitmap_clear_all(&m_readSet);
}

/** ��ʼ����ɨ�貢���ص�һ����¼
 *
 * @param buf ���������¼�Ļ�����
 * @param key ����������
 * @param keypart_map �������а�����Щ��������
 * @param forward ��������
 * @param includeKey �Ƿ��������key�ļ�¼
 * @param exact �Ƿ�ֻ���ص���key�ļ�¼
 * @return �ɹ�����0���Ҳ�����¼����HA_ERR_END_OF_FILE���ȱ�����ʱ����HA_ERR_LOCK_WAIT_TIMEOUT
 */
int ntse_handler::indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact) {
	m_lobCtx->reset();
	endScan();	// ������Χ��ѯʱ���ܻ�ɨ���Σ�������index_end

	int code;/* = checkSQLLogBin();
	if (code) {
		return code;
	}*/

	m_mcSaveBeforeScan = m_session->getMemoryContext()->setSavepoint();

	initReadSet(true);

	bool singleFetch;
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols, true, active_index);
	transKey(active_index, key, keypart_map, &m_indexKey);
	if (exact
		&& m_ntseTable->getTableDef(true, m_session)->m_indice[active_index]->m_unique
		&& m_indexKey.m_numCols == m_ntseTable->getTableDef(true, m_session)->m_indice[active_index]->m_numCols)
		singleFetch = true;
	else
		singleFetch = false;

	OpType opType = getOpType();
	try {
		IndexScanCond cond(active_index, m_indexKey.m_numCols? &m_indexKey: NULL, forward, includeKey, singleFetch);
		m_ntseScan = m_ntseTable->indexScan(m_session, opType, &cond, numReadCols, readCols, false, m_lobCtx);
	} catch(NtseException &e) {
		NTSE_ASSERT(e.getErrorCode() == NTSE_EC_LOCK_TIMEOUT);
		return HA_ERR_LOCK_WAIT_TIMEOUT;
	}
	// ����NTSE�����ݲ�֧�ַ���ֻ����ָ���������ļ�¼������ϲ�������ʽ��HA_READ_KEY_EXACT��
	// ���ֲ���singleFetch����NTSE�᷵��ʵ���ϴ���ָ���������ļ�¼�������ϲ�ָ����
	// HA_READ_KEY_EXACT������ϲ���Ϊ�ײ�ֻ�᷵�ص���ָ���������ļ�¼��������ȥ��飬
	// ���������Ҫ���˵���Щ�����������ļ�¼
	m_checkSameKeyAfterIndexScan = exact && !singleFetch;

	code = fetchNext(buf);
	return code;
}

/** ����HA_CREATE_INFO��Ϣ
 * @param create_info	��Ϣ����
 */
/*void ntse_handler::update_create_info( HA_CREATE_INFO *create_info ) {
	if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
		ntse_handler::info(HA_STATUS_AUTO);
		create_info->auto_increment_value = stats.auto_increment_value;
	}
}*/

/** �Ƚϼ�¼�ļ�ֵ��ָ����ֵ�Ƿ����
 *
 * @param buf ��¼
 * @param indexDef ��������
 * @param key1 KEY_PAD��ʽ�ļ�ֵ
 * @param key2 KEY_PAD��ʽ�ļ�ֵ
 * @return �Ƿ����
 */
bool ntse_handler::recordHasSameKey(const IndexDef *indexDef, const SubRecord *key1, const SubRecord *key2) {
	assert(key1->m_format == KEY_PAD);
	assert(key2->m_format == KEY_PAD);

	TableDef *tableDef = m_ntseTable->getTableDef(true, m_session);
	return !RecordOper::compareKeyPP(tableDef, key1, key2, indexDef);
}

/**
 * ��ʼ����ɨ����
 * @pre m_readSetӦ��Ҫ������ȷ
 */
void ntse_handler::beginTblScan() {
	u16 numReadCols;
	u16 *readCols = transCols(&m_readSet, &numReadCols);

	OpType opType = getOpType();
	try {
		if (m_isRndScan) {
			m_ntseScan = m_ntseTable->tableScan(m_session, opType, numReadCols, readCols, false, m_lobCtx);
		} else {
			m_ntseScan = m_ntseTable->positionScan(m_session, opType, numReadCols, readCols, false, m_lobCtx);
		}
	} catch(NtseException &) {
		NTSE_ASSERT(false);		// ���ӱ���ʱ�����ܻ�ʧ��
	}
}

/** ���ڸ���ɨ��֮ǰ��ʼ��m_readSet����Ϣ
 * @param idxScan	true��ʾ����ɨ�裬false��ʾ��ɨ��
 * @param posScan	true��ʾ��rnd_posɨ�裬false������������ɨ���������ɨ��
 */
void ntse_handler::initReadSet( bool idxScan, bool posScan/* = false */) {
	assert(m_thd);
	assert(!(idxScan && posScan));	// ���벻���ܼ�������ɨ������rnd_posɨ��

	bool isReadAll = false;
	//if ((opt_bin_log && !isBinlogSupportable()) || posScan) {
	// ����������¼binlog��rnd_posɨ���Լ�����mysql���м�binlog�Ĳ�����������ͬ���ж����̣�slave���Ƶ��߼�Ҳ����һ��
	// ����rnd_pos�Ĵ������QA70175������Ĵ���֤���ڸ��²������ܶ�ȡ�������ԣ����ܶ��������©��
	enum_sql_command sql_command = m_thd->lex->sql_command;
	bool isRead = (getOpType() == OP_READ);
	if (isSlaveThread()) {
		// Slaveִ��binlogʱ�����������SQLCOM_END���޷��ж��Ƿ�ΪUPDATE�����ֻҪ�Ǹ��²���������ȡ��������
		// TODO�����Slaveдbinlog�����õ���Master��binlog�����ݣ�Ӧ�ÿ��Բ���ȡ��������
		isReadAll = !isRead;
	} else {
		// Master������update��Ҫ��ȡ��������
		isReadAll = ((sql_command == SQLCOM_UPDATE) || (sql_command == SQLCOM_UPDATE_MULTI && !isRead));
	}
	//}

	if (isReadAll)
		bitmap_set_all(&m_readSet);
	else {
		if (idxScan)
			bitmap_copy(&m_readSet, table->read_set);
		else {
			// ��ɨ��ʱrnd_init�͵�һ��rnd_nextʱ������read_set��һ�£�Ϊ��ȫ���
			// ʹ��rnd_init�͵�һ��rnd_next��read_set֮�ϼ�
			bitmap_union(&m_readSet, table->read_set);
		}
	}
	m_isReadAll = bitmap_is_set_all(&m_readSet);
}



/** �õ���ǰ�����THD����
* @return ����m_thd����
*/
THD* ntse_handler::getTHD() {
	return m_thd;
}

/** ���ڸ��²�����insert/delete/update�����SQL_LOG_BIN�����Ƿ��б����ã���������ø�������
 * @return 0��ʾ����δ�����ã��޾��棻��0��ʾ���������ã�������Ӧ�ľ�����Ϣ
 */
/*int ntse_handler::checkSQLLogBin() {
	int code = 0;
	if (m_thd->lex->sql_command != SQLCOM_SELECT && !m_thd->sql_log_bin_toplevel)
		code = reportError(NTSE_EC_NOT_SUPPORT, "SQL_LOG_BIN variable is not supported by NTSE, please set to 1.", false);
	
	return code;
}*/

void ntse_handler::ntse_drop_database(handlerton *hton, char* path) {
	return;
}
#ifdef EXTENDED_FOR_COMMIT_ORDERED
void ntse_handler::ntse_commit_ordered(handlerton *hton, THD *thd, bool all) {
	return;
}
#endif
int ntse_handler::ntse_commit_trx(handlerton *hton, THD* thd, bool all) {
	return 0;
}

int ntse_handler::ntse_rollback_trx(handlerton *hton, THD *thd, bool all) {
	return 0;
}

int ntse_handler::ntse_xa_prepare(handlerton *hton, THD* thd, bool all) {
	return 0;
}
