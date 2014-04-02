/**
 * NTSE��¼binlog��ص�����ʵ��
 * @author �ձ�(naturally@163.org)
 */

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#ifdef NTSE_KEYVALUE_SERVER
#include "keyvalue/KeyValueServer.h"
#endif

#define MYSQL_SERVER 1
#ifdef WIN32
#include "mysql_priv.h"
#include "ntse_binlog.h"
#include "ha_ntse.h"
#include <mysql/plugin.h>
#endif

#include "api/Database.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"
#include "misc/ParFileParser.h"

#ifndef WIN32
#include "mysql_priv.h"
#include "ntse_binlog.h"
#include "ha_ntse.h"
#include <mysql/plugin.h>
#endif

#include "rpl_injector.h"
#include <algorithm>
#include <utility>

using namespace std;
using namespace ntse;

NTSECachableBinlog *NTSECachableBinlog::m_instance = NULL;		/** ����ģʽʵ������ */
BinlogBufferManager *NTSECachableBinlog::m_blBuffer = NULL;		/** дbinlogʹ�õĻ�������� */

NTSEDirectBinlog *NTSEDirectBinlog::m_instance = NULL;			/** ����ģʽʵ������ */
NTSEDirectBinlog::DirectBinlogWriter * NTSEDirectBinlog::DirectBinlogWriter::m_instance = NULL;	/** ����ʵ�� */
ntse::Mutex *NTSEDirectBinlog::m_mutex = NULL;						/** ������ */

BinlogBufferStatus NTSEDirectBinlog::m_bufferStatus;

NTSEBinlogFactory *NTSEBinlogFactory::m_instance = NULL;		/** ����ģʽʵ������ */

/************************************************************************/
/* BinlogInfo                                                           */
/************************************************************************/

/**
 * ����binlog��Ϣ�ڻ��浱�д洢��Ҫ���ٿռ�
 * @param tableDef	binlog�漰��¼��������
 * @return ������Ϣ�洢ռ��ʵ�ʳ���
 */
size_t BinlogInfo::getSerializeSize( const TableDef *tableDef ) const {
	size_t size = sizeof(void*) + sizeof(u8) + sizeof(TxnInfo);
	if (m_type != BINLOG_INSERT)
		size += RecordOper::getSubRecordSerializeSize(tableDef, &m_beforeSR, true, false);
	if (m_type != BINLOG_DELETE)
		size += RecordOper::getSubRecordSerializeSize(tableDef, &m_afterSR, true, false);
	return size;
}


/**
 * ����ǰbinlogInfoд��ָ���Ļ����ַ
 * @param tableDef	binlog�漰��¼��������
 * @param buffer		������ʼ��ַ
 * @param bufferSize	������Ч����
 */
void BinlogInfo::serialize( const TableDef *tableDef, byte *buffer, u32 bufferSize ) {
	nftrace(ts.mysql, tout << m_type << " " << tableDef);
	Stream s(buffer, bufferSize);
	try {
		s.write((byte*)&tableDef, sizeof(tableDef));
		s.write((u8)m_type);
		if (m_type != BINLOG_INSERT) {
			nftrace(ts.mysql, tout << m_beforeSR.m_numCols << " " << m_beforeSR.m_size);
			RecordOper::serializeSubRecordMNR(&s, tableDef, &m_beforeSR, true);
		}
		if (m_type != BINLOG_DELETE) {
			nftrace(ts.mysql, tout << m_afterSR.m_numCols << " " << m_afterSR.m_size);
			RecordOper::serializeSubRecordMNR(&s, tableDef, &m_afterSR, true);
		}
		s.write(m_txnInfo.m_sqlMode);
		s.write(m_txnInfo.m_serverId);
	} catch (NtseException) { NTSE_ASSERT(false); }

	assert(s.getSize() == bufferSize);
}

/**
 * ��ָ�������ж�ȡ����һ��binlogInfo
 * @param mc		�ڴ�������
 * @param buffer	�������ʼ��ַ
 * @param bufferSize�������Ч����
 */
void BinlogInfo::unserialize( MemoryContext *mc, byte *buffer, u32 bufferSize ) {
	memset(&m_beforeSR, 0, sizeof(m_beforeSR));
	memset(&m_afterSR, 0, sizeof(m_afterSR));

	Stream s(buffer, bufferSize);
	try {
		// ��ȡ������Ϣ����
		s.readBytes((byte*)&m_tableDef, sizeof(TableDef*));
		u8 type;
		s.read(&type);
		m_type = (BinlogType)type;
		nftrace(ts.mysql, tout << m_type << " " << m_tableDef);
		if (m_type != BINLOG_INSERT) {
			SubRecord *beforeSR = RecordOper::unserializeSubRecordMNR(&s, m_tableDef, mc);
			m_beforeSR.m_columns = beforeSR->m_columns;
			m_beforeSR.m_numCols = beforeSR->m_numCols;
			m_beforeSR.m_size = beforeSR->m_size;
			m_beforeSR.m_data = beforeSR->m_data;
			nftrace(ts.mysql, tout << m_beforeSR.m_numCols << " " << m_beforeSR.m_size);
		}
		if (m_type != BINLOG_DELETE) {
			SubRecord *afterSR = RecordOper::unserializeSubRecordMNR(&s, m_tableDef, mc);
			m_afterSR.m_numCols = afterSR->m_numCols;
			m_afterSR.m_columns = afterSR->m_columns;
			m_afterSR.m_size = afterSR->m_size;
			m_afterSR.m_data = afterSR->m_data;
			nftrace(ts.mysql, tout << m_afterSR.m_numCols << " " << m_afterSR.m_size);
		}
		s.read(&m_txnInfo.m_sqlMode);
		s.read(&m_txnInfo.m_serverId);
		assert(s.getSize() == bufferSize);
	} catch (NtseException) { NTSE_ASSERT(false); }
}



/************************************************************************/
/* BinlogWriter��ʵ��                                                   */
/************************************************************************/

/** ���캯��
 * @param blBuffer д�߳�ʹ�õ�binlog�������
 */
NTSECachableBinlog::CachedBinlogWriter::CachedBinlogWriter( BinlogBufferManager *blBuffer ) : BinlogWriter(), m_lastTableId((u16)-1), m_blBuffer(blBuffer) { 
	m_inj = injector::instance();
	memset(&m_status, 0, sizeof(m_status));
}

/** ��������
 */
NTSECachableBinlog::CachedBinlogWriter::~CachedBinlogWriter() {
	injector::free_instance();
}

/** ˢдbinlog�߳�������
 */
void NTSECachableBinlog::CachedBinlogWriter::run() {
	if (!createTHD()) {
		nftrace(ts.mysql, tout << "Binlog writer thread create thd fail and exit!");
		NTSE_ASSERT(false);
	}

	MemoryContext mc(Limits::PAGE_SIZE, 2);
	u64 savePoint = mc.setSavepoint();

	while (true) {
		while (!m_blBuffer->pollBinlogInfo(500))
			if (!m_running) goto finish;

		injector::transaction trans;
		BinlogInfo binlogInfo;
		u64 transLogs = 0;
		u64 writtenRBLs = 0;
		u64 lastSqlMode = 0;
		ha_ntse *handler = NULL;
		struct st_table *table = NULL;

		m_blBuffer->beginReadBinlogInfo();
		while (m_blBuffer->readNextBinlogInfo(&mc, &binlogInfo)) {
			u16 tableId = binlogInfo.m_tableDef->m_id;
			if (isTableSwitched(tableId) || writtenRBLs++ >= DEFAULT_MAX_UNCOMMITTED_BINLOGS || binlogInfo.m_txnInfo.m_sqlMode != lastSqlMode) {
				if (m_lastTableId != (u16)-1 && !commitTransaction(&trans)) {
					nftrace(ts.mysql, tout << "Binlog writer thread commit txn fail and exit!");
					NTSE_ASSERT(false);
				}
				writtenRBLs = 0;

				// ͳ����Ϣ��¼
				m_status.onTransCommit(transLogs);
				transLogs = 0;				

				if (handler != NULL)
					handlerInfo->returnHandler(handler);
				handler = handlerInfo->reserveSpecifiedHandler(tableId);
				if (handler != NULL)
					table = handler->getMysqlTable();
				else {
					
					/**
					 *	�����Ҫ��keyvalue��������Ҫͨ�����ַ�ʽ�򿪱�ṹ
					 */			
#ifdef NTSE_KEYVALUE_SERVER
					if (ParFileParser::isPartitionByPhyicTabName(binlogInfo.m_tableDef->m_name)) {
						char logicTabName[Limits::MAX_NAME_LEN + 1] = {0};
						bool ret = ParFileParser::parseLogicTabName(binlogInfo.m_tableDef->m_name, logicTabName, Limits::MAX_NAME_LEN + 1);
						assert(ret);
						UNREFERENCED_PARAMETER(ret);
						table = KeyValueServer::openMysqlTable(m_thd, tableId, binlogInfo.m_tableDef->m_schemaName, logicTabName);
					} else {
						table = KeyValueServer::openMysqlTable(m_thd, tableId, binlogInfo.m_tableDef->m_schemaName, binlogInfo.m_tableDef->m_name);
					}
#endif
				}

				assert(table != NULL);
				
				startTransaction(&trans, table, &binlogInfo.m_txnInfo);
				m_lastTableId = binlogInfo.m_tableDef->m_id;
				lastSqlMode = binlogInfo.m_txnInfo.m_sqlMode;
			}

			writeBinlog(&trans, table, &binlogInfo);
			m_status.onWrittenBinlog(&binlogInfo);
			transLogs++;
			mc.resetToSavepoint(savePoint);
		}

		if (!commitTransaction(&trans)) {
			sql_print_error("Commit ntse binlog failed, may be caused by binlog_cache_size or max_binlog_cache_size is not big enough.");
		}
		m_lastTableId = (u16)-1;

		writtenRBLs = 0;
		m_status.onTransCommit(transLogs);
		m_blBuffer->endReadBinlogInfo();

		if (handler != NULL) {
			handlerInfo->returnHandler(handler);
			handler = NULL;
		}

		mc.resetToSavepoint(savePoint);
	}

finish:
	destroyTHD();
}

/** �жϵ�ǰдbinlog�ı��ǲ��Ǻ�֮ǰ�ı�һ��
 * @param curTblId	��ǰ�����ID
 * @return true��ʾ���Ѿ��л�����false��ʾһ��
 */
bool NTSECachableBinlog::CachedBinlogWriter::isTableSwitched( u16 curTblId ) {
	return m_lastTableId == (u16)-1 || m_lastTableId != curTblId;
}


/************************************************************************/
/* BinlogWriter��ʵ��                                                   */
/************************************************************************/

/**
 * ��ʼһ������
 * @param trans		�������
 * @param table		����ʹ�õ�TABLE����
 * @param txnInfo	Ҫдbinlog���������Ϣ
 */
void BinlogWriter::startTransaction( void *trans, TABLE *table, BinlogInfo::TxnInfo *txnInfo ) {
	assert(table != NULL && trans != NULL);
	m_thd->variables.sql_mode = (ulong)txnInfo->m_sqlMode;
	m_thd->server_id = (ulong)txnInfo->m_serverId;
	m_thd->set_time();
	injector::transaction *realTrans = (injector::transaction*)trans;
	m_inj->new_trans(m_thd, realTrans);
	injector::transaction::table tbl(table, false);
	realTrans->use_table(m_thd->server_id, tbl);
}


/**
 * ����һ������
 * @param trans	�������
 * @return true��ʾ�ύ�ɹ�,false��ʾ�ύʧ��
 */
bool BinlogWriter::commitTransaction( void *trans ) {
	injector::transaction *realTrans = (injector::transaction*)trans;
	if (!realTrans->good())
		return false;

	if (realTrans->commit()) {
		// TODO: ����ʧ�ܵ����
		return false;
	}

	m_thd->variables.sql_mode = 0;
	return true;
}



/** ����THD�̶߳���
 * ģ��NDB��ndb_binlog_thread_func�������д���THD��ʵ��
 * @return true��ʾ�����ɹ�������false��ʾ����ʧ��
 */
bool BinlogWriter::createTHD() {
	my_thread_init();

	m_thd = new THD; /* note that contructor of THD uses DBUG_ */
	THD_CHECK_SENTRY(m_thd);

	/* We need to set thd->thread_id before thd->store_globals, or it will
	set an invalid value for thd->variables.pseudo_thread_id.
	*/
	pthread_mutex_lock(&LOCK_thread_count);
	m_thd->thread_id= thread_id++;
	pthread_mutex_unlock(&LOCK_thread_count);

	m_thd->thread_stack= (char*) &m_thd; /* remember where our stack is */
	if (m_thd->store_globals())
	{
		m_thd->cleanup();
		delete m_thd;
		m_thd = NULL;
		return false;
	}
	lex_start(m_thd);

	m_thd->init_for_queries();
	m_thd->command= COM_DAEMON;
	m_thd->system_thread= SYSTEM_THREAD_NDBCLUSTER_BINLOG;
	m_thd->version= refresh_version;
	m_thd->main_security_ctx.host_or_ip= "";
	m_thd->client_capabilities= 0;
	my_net_init(&m_thd->net, 0);
	m_thd->main_security_ctx.master_access= ~0;
	m_thd->main_security_ctx.priv_user= 0;

	pthread_detach_this_thread();
	m_thd->real_id= pthread_self();
	pthread_mutex_lock(&LOCK_thread_count);
	threads.append(m_thd);
	pthread_mutex_unlock(&LOCK_thread_count);
	m_thd->lex->start_transaction_opt= 0;

	// TODO�����鿴�ǲ��ǲ���Ҫ����صı�
	return true;
}

/** ����THD����
 */
void BinlogWriter::destroyTHD() {
	close_thread_tables(m_thd);	// TODO: ���ܲ���Ҫ����
	net_end(&m_thd->net);
	m_thd->cleanup();
	delete m_thd;
	my_thread_end();
}

/**
 * ����ĳ����¼�����ݳ�ʼ��ָ����λͼ
 * �ο�NDB��ndb_unpack_record��������������Ҫ���������У�
 * ����record�����ֶε����ݣ���bitmap����Ϊֻ������Щ�ֶε�λͼ��Ϣ
 *
 * @param bitmap	Ҫ�޸����õ�λͼ����
 * @param numCols	Ҫ����λͼ��record����������
 * @param cols		Ҫ����λͼ��record�����е��к�
 */
void BinlogWriter::initBITMAP( struct st_bitmap *bitmap, u16 numCols, u16 *cols ) {
	assert(numCols != 0 && cols != NULL);
	//// TODO:ȷ�������û���ã�NDBʵ�ֵ��а�����
	//if (table->s->null_bytes > 0)
	//	buf[table->s->null_bytes - 1]|= 256U - (1U << table->s->last_null_bit_pos);

	bitmap_clear_all(bitmap);
	for (uint i = 0; i < numCols; i++) {
		bitmap_set_bit(bitmap, cols[i]);
	}
}

/** дbinlog��־
 * @param trans			�������
 * @param table			�������mysql�ϲ�����
 * @param binlogInfo	Ҫд��binlog��־��Ϣ
 */
void BinlogWriter::writeBinlog( void *trans, TABLE *table, BinlogInfo *binlogInfo ) {
	injector::transaction *realTrans = (injector::transaction*)trans;
	u32 numFields = table->s->fields;

	MY_BITMAP bitmap;
	uint32 bitBuf[128 / (sizeof(uint32) * 8)];	// bitmap��Ҫ�Ļ���
	bitmap_init(&bitmap, numFields <= sizeof(bitBuf) * 8 ? bitBuf : NULL, numFields, FALSE);

	switch (binlogInfo->m_type) {
		case BINLOG_UPDATE:
			{
				assert(binlogInfo->m_beforeSR.m_numCols != 0 && binlogInfo->m_beforeSR.m_size != 0 && binlogInfo->m_beforeSR.m_data != NULL &&
					binlogInfo->m_afterSR.m_numCols == binlogInfo->m_beforeSR.m_numCols && binlogInfo->m_afterSR.m_size == binlogInfo->m_beforeSR.m_size && binlogInfo->m_afterSR.m_data != NULL);

				initBITMAP(&bitmap, binlogInfo->m_beforeSR.m_numCols, binlogInfo->m_beforeSR.m_columns);

				if (realTrans->update_row((uint32)binlogInfo->m_txnInfo.m_serverId, injector::transaction::table(table, false), 
					&bitmap, numFields, (uchar*)binlogInfo->m_beforeSR.m_data, (uchar*)binlogInfo->m_afterSR.m_data) != 0) {
					sql_print_error("Write ntse update_row binlog failed, make sure binlog_cache_size and max_binlog_cache_size is big enough or find other errors");
				}
			}

			break;
		case BINLOG_INSERT:
			assert(binlogInfo->m_afterSR.m_numCols != 0 && binlogInfo->m_afterSR.m_size != 0 && binlogInfo->m_afterSR.m_data != NULL &&
				binlogInfo->m_beforeSR.m_numCols == 0);

			initBITMAP(&bitmap, binlogInfo->m_afterSR.m_numCols, binlogInfo->m_afterSR.m_columns);

			if (realTrans->write_row((uint32)binlogInfo->m_txnInfo.m_serverId, injector::transaction::table(table, false), 
				&bitmap, numFields, (uchar*)binlogInfo->m_afterSR.m_data) != 0) {
				sql_print_error("Write ntse write_row binlog failed, make sure binlog_cache_size and max_binlog_cache_size is big enough or find other errors");
			}

			break;
		case BINLOG_DELETE:
			assert(binlogInfo->m_beforeSR.m_numCols != 0 && binlogInfo->m_beforeSR.m_size != 0 && binlogInfo->m_beforeSR.m_data != NULL &&
				binlogInfo->m_afterSR.m_numCols == 0);

			initBITMAP(&bitmap, binlogInfo->m_beforeSR.m_numCols, binlogInfo->m_beforeSR.m_columns);

			if (realTrans->delete_row((uint32)binlogInfo->m_txnInfo.m_serverId, injector::transaction::table(table, false), 
				&bitmap, numFields, (uchar*)binlogInfo->m_beforeSR.m_data) != 0) {
				sql_print_error("Write ntse delete_row binlog failed, make sure binlog_cache_size and max_binlog_cache_size is big enough or find other errors");
			}

			break;
		default:
			NTSE_ASSERT(false);
	}
}



BinlogWriter::BinlogWriter() : Thread("Binlog Writer thread") {
	m_running = true;
	m_inj = injector::instance();
	memset(&m_status, 0, sizeof(m_status));
}

BinlogWriter::~BinlogWriter() {
	injector::free_instance();
	m_inj = NULL;
}


/************************************************************************/
/* NTSEBinlog��ʵ��                                                     */
/************************************************************************/
/** ע��NTSE�Ļص�����
 * @param ntsedb	ntseʹ��db����
 */
void NTSEBinlog::registerNTSECallbacks( Database *ntsedb ) {
	for (cbItor itor = m_callbacks.begin(); itor != m_callbacks.end(); itor++)
		ntsedb->registerNTSECallback((CallbackType)itor->first, itor->second);
}

/** ע��NTSE�Ļص�����
 * @param ntsedb	ntseʹ��db����
 */
void NTSEBinlog::unregisterNTSECallbacks( Database *ntsedb ) {
	for (cbItor itor = m_callbacks.begin(); itor != m_callbacks.end(); itor++)
		ntsedb->unregisterNTSECallback((CallbackType)itor->first, itor->second);
}

void NTSEBinlog::destroyCallbacks() {
	if (!m_callbacks.empty()) {
		for (cbItor itor = m_callbacks.begin(); itor != m_callbacks.end(); itor++) {
			delete itor->second;
		}
		m_callbacks.clear();
	}
}

/** ����handler�����е�thd����Ϣ���ж��Ƿ���Ҫд����binlog
 * �����ǰ��slave,����ָ����opt_log_slave_updates=false,Ҳ���ü�¼binlog
 */
bool NTSEBinlog::needBinlog( ha_ntse *handler ) {
	return handler == NULL || (!handler->isSlaveThread() || opt_log_slave_updates);
}

/************************************************************************/
/* NTSECachableBinlog��ʵ��                                             */
/************************************************************************/

/** �õ�NTSEд��־����Ψһʵ��
 * @param bufferSize	binlog����ʹ�õĻ����С
 * @return NTSEBinlogΨһ����
 */
NTSECachableBinlog* NTSECachableBinlog::getNTSEBinlog(size_t bufferSize) {
	if (m_instance == NULL)
		m_instance = new NTSECachableBinlog(bufferSize);
	return m_instance;
}

/** ���캯��
 * @param bufferSize	binlog����ʹ�õĻ����С
 */
NTSECachableBinlog::NTSECachableBinlog(size_t bufferSize) {
	m_blBuffer = NULL;
	init(bufferSize);
}

/** ��������
 */
NTSECachableBinlog::~NTSECachableBinlog() {
	destroy();
	m_instance = NULL;
}

typedef pair<CallbackType, NTSECallbackFN*> cbPair;

/** @see NTSEBinlog::initCallbacks
 */
void NTSECachableBinlog::initCallbacks() {
	NTSECallbackFN *rowInsertCB = new NTSECallbackFN();
	rowInsertCB->callback = NTSECachableBinlog::logRowInsert;
	m_callbacks.insert(cbPair(ROW_INSERT, rowInsertCB));
	NTSECallbackFN *rowDeleteCB = new NTSECallbackFN();
	rowDeleteCB->callback = NTSECachableBinlog::logRowDelete;
	m_callbacks.insert(cbPair(ROW_DELETE, rowDeleteCB));
	NTSECallbackFN *rowUpdateCB = new NTSECallbackFN();
	rowUpdateCB->callback = NTSECachableBinlog::logRowUpdate;
	m_callbacks.insert(cbPair(ROW_UPDATE, rowUpdateCB));
	NTSECallbackFN *tableCloseCB = new NTSECallbackFN();
	tableCloseCB->callback = NTSECachableBinlog::onCloseTable;
	m_callbacks.insert(cbPair(TABLE_CLOSE, tableCloseCB));
	NTSECallbackFN *tableAlterCB = new NTSECallbackFN();
	tableAlterCB->callback = NTSECachableBinlog::onAlterTable;
	m_callbacks.insert(cbPair(TABLE_ALTER, tableAlterCB));
}

/** ��ʼ������
 * @param bufferSize	binlog����ʹ�õĻ����С
 */
void NTSECachableBinlog::init(size_t bufferSize) {
	initCallbacks();

	m_blBuffer = new BinlogBufferManager(bufferSize);

	m_writer = new CachedBinlogWriter(m_blBuffer);
	m_writer->start();
}

/** ���ٴ�����
 */
void NTSECachableBinlog::destroy() {
	if (m_writer != NULL) {
		m_writer->setStop();
		m_writer->join();
		delete m_writer;
		m_writer = NULL;
	}

	if (m_blBuffer != NULL) {
		delete m_blBuffer;
		m_blBuffer = NULL;
	}

	if (!m_callbacks.empty()) {
		for (cbItor itor = m_callbacks.begin(); itor != m_callbacks.end(); itor++) {
			delete itor->second;
		}
		m_callbacks.clear();
	}
}

/** �����ڹرձ��ʱ���binlog����
 * ����Լ������NTSE�ı�ر�֮ǰ����Ҫ��������ص�������ǰbinlog���浱�е���Ϣˢд��ȥ
 * @param tableDef	������������
 * @param brec		�м�¼��ǰ��������ΪNULL������Ҫʹ��
 * @param arec		�м�¼�ĺ�������ΪNULL������Ҫʹ��
 * @param param		�ص�����
 */
void NTSECachableBinlog::onCloseTable( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
	UNREFERENCED_PARAMETER(brec);
	UNREFERENCED_PARAMETER(arec);
	UNREFERENCED_PARAMETER(param);
	m_blBuffer->flush(tableDef->m_id);
}

/** �ڱ�ṹ���޸�ʱˢ��binlog
 * ����Լ������NTSE�ı�ر�֮ǰ����Ҫ��������ص�������ǰbinlog���浱�е���Ϣˢд��ȥ
 * @param tableDef	�����������壬����ΪNULL������Ҫʹ��
 * @param brec		�м�¼��ǰ��������ΪNULL������Ҫʹ��
 * @param arec		�м�¼�ĺ�������ΪNULL������Ҫʹ��
 * @param param		�ص�����
 */
void NTSECachableBinlog::onAlterTable( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
	UNREFERENCED_PARAMETER(brec);
	UNREFERENCED_PARAMETER(arec);
	UNREFERENCED_PARAMETER(param);
	if (tableDef) {
		m_blBuffer->flush(tableDef->m_id);
	} else {
		m_blBuffer->flushAll();
	}
}

/**
 * ��¼�в����binlog��־
 * @param tableDef	������������
 * @param brec		�е�ǰ�������ݣ�һ����MySQL��ʽ��
 * @param arec		�еĺ������ݣ�һ����MySQL��ʽ��
 * @param param		�ص�����
 */
void NTSECachableBinlog::logRowInsert( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
	assert(arec != NULL);
	ha_ntse *handler = (ha_ntse*)param;

	if (!needBinlog(handler))
		return;

	writeBuffer(BINLOG_INSERT, (handler != NULL ? handler->getTHD() : NULL), tableDef, 0, NULL, 0, NULL, 
		arec->m_numCols, arec->m_columns, arec->m_size, arec->m_data);
}

/**
 * ��¼��ɾ����binlog��־
 * @param tableDef	������������
 * @param brec		�е�ǰ�������ݣ�һ����MySQL��ʽ��
 * @param arec		�еĺ������ݣ�һ����MySQL��ʽ��
 * @param param		�ص�����
 */
void NTSECachableBinlog::logRowDelete( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
	assert(brec != NULL);
	ha_ntse *handler = (ha_ntse*)param;

	if (!needBinlog(handler))
		return;

	writeBuffer(BINLOG_DELETE, (handler != NULL ? handler->getTHD() : NULL), tableDef, brec->m_numCols, brec->m_columns, brec->m_size, brec->m_data, 
		0, NULL, 0, NULL);
}

/**
 * ��¼�и��µ�binlog��־
 * @param tableDef	������������
 * @param brec		�е�ǰ�������ݣ�һ����MySQL��ʽ��
 * @param arec		�еĺ������ݣ�һ����MySQL��ʽ��
 * @param param		�ص�����
 */
void NTSECachableBinlog::logRowUpdate( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
	assert(arec != NULL && brec != NULL);
	ha_ntse *handler = (ha_ntse*)param;
	if (!needBinlog(handler))
		return;

	ColList col1(brec->m_numCols, brec->m_columns);
	ColList col2(arec->m_numCols, arec->m_columns);
	u16 maxCols = brec->m_numCols + arec->m_numCols;
	ColList mergeCol(maxCols, (u16*)alloca(2 * sizeof(u16) * maxCols));
	mergeCol.merge(col1, col2);
	writeBuffer(BINLOG_UPDATE, (handler != NULL ? handler->getTHD() : NULL), tableDef, mergeCol.m_size, mergeCol.m_cols, brec->m_size, brec->m_data,
		mergeCol.m_size, mergeCol.m_cols, arec->m_size, arec->m_data);
}


/**
 * ��binlogд�뵽���浱��
 * @param type		дbinlog������
 * @param thd		дbinlog�Ự��thd����
 * @param tableDef	��������
 * @param bNumCols	��¼ǰ��������
 * @param bColumns	��¼ǰ������
 * @param bSize		��¼ǰ�����ݳ���
 * @param bData		��¼ǰ������
 * @param aNumCols	��¼����������
 * @param aColumns	��¼��������
 * @param aSize		��¼�������ݳ���
 * @param aData		��¼��������
 */
void NTSECachableBinlog::writeBuffer( BinlogType type, THD *thd, const TableDef *tableDef, u16 bNumCols, u16 *bColumns, u16 bSize, byte *bData, 
							 u16 aNumCols, u16 *aColumns, u16 aSize, byte *aData ) {
	BinlogInfo binlogInfo;

	binlogInfo.m_type = type;
	binlogInfo.m_beforeSR.m_format = binlogInfo.m_afterSR.m_format = REC_MYSQL;
	binlogInfo.m_beforeSR.m_numCols = bNumCols;
	binlogInfo.m_beforeSR.m_columns = bColumns;
	binlogInfo.m_beforeSR.m_size = bSize;
	binlogInfo.m_beforeSR.m_data = bData;
	binlogInfo.m_afterSR.m_numCols = aNumCols;
	binlogInfo.m_afterSR.m_columns = aColumns;
	binlogInfo.m_afterSR.m_size = aSize;
	binlogInfo.m_afterSR.m_data = aData;
	if (thd != NULL) {
		binlogInfo.m_txnInfo.m_sqlMode = thd->variables.sql_mode;
		binlogInfo.m_txnInfo.m_serverId = thd->server_id;
	} else {
		binlogInfo.m_txnInfo.m_sqlMode = 0;
		binlogInfo.m_txnInfo.m_serverId = ::server_id;
	}

	m_blBuffer->writeBinlogInfo(tableDef, &binlogInfo);
}

/**
 * �ͷ�����binlog��¼ʵ��
 */
void NTSECachableBinlog::freeNTSEBinlog() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}

/** @see NTSEBinlog::flushBinlog
 */
void NTSECachableBinlog::flushBinlog(u16 tableId) {
	m_blBuffer->flush(tableId);
}

/** @see NTSEBinlog::getBinlogWriterStatus
 */
struct BinlogWriterStatus NTSECachableBinlog::getBinlogWriterStatus() {
	return m_writer->getStatus();
}

/** @see NTSEBinlog::getBinlogBufferStatus
 */
struct BinlogBufferStatus NTSECachableBinlog::getBinlogBufferStatus() {
	return m_blBuffer->getStatus();
}

/** �õ�binlog����������
 * @return ����"cached"
 */
const char* NTSECachableBinlog::getBinlogMethod() const {
	return "cached";
}
/************************************************************************/
/* Binlog��־������                                                     */
/************************************************************************/

const double BinlogBuffer::BUFFER_FULL_RATIO = 0.95;	/** ����Ԥ��������ֵ */

/** ���캯��
 * @param bufferSize	��ʼ�����С
 */
BinlogBuffer::BinlogBuffer( size_t bufferSize /*= DEFAULT_BUFFER_SIZE*/ ) : m_bufferCleanEvent(false) {
	m_buffer = new MemoryContext(Limits::PAGE_SIZE, bufferSize / Limits::PAGE_SIZE);
	m_extendInfo.reserve(DEFAULT_RESERVE_EXTENDBINLOGINFO_SIZE);
	m_size = 0;
	m_bufferInitSize = bufferSize;
}

/** ��������
 */
BinlogBuffer::~BinlogBuffer() {
	m_extendInfo.clear();
	delete m_buffer;
}

/**
 * ���ָ��binlog��Ϣ������
 * @param tableDef		binlog��¼��������
 * @param binlogInfo	binlog��Ϣ
 */
void BinlogBuffer::append( const TableDef *tableDef, BinlogInfo *binlogInfo ) {
	size_t logSize = binlogInfo->getSerializeSize(tableDef);
	byte *buffer = (byte*)m_buffer->alloc(logSize);
	binlogInfo->serialize(tableDef, buffer, logSize);

	ExtendBinlogInfo eblInfo(tableDef->m_id, buffer, logSize);
	m_extendInfo.push_back(eblInfo);

	m_size += logSize;
}

/**
 * �õ�ָ��λ�õ�binlog��Ϣ
 * @param mc			�ڴ�������
 * @param pos			Ҫ��ȡ�ڼ���
 * @param binlogInfo	out �õ�binlog��Ϣ֮�󷵻�
 * @return true��ʾpos���ȡ�ɹ���false��ʾpos������ǰ��Ч��Ϣ��С
 */
bool BinlogBuffer::getAt( MemoryContext *mc, uint pos, BinlogInfo *binlogInfo ) {
	if (pos >= m_extendInfo.size())
		return false;

	void *blAddress = m_extendInfo[pos].m_address;
	size_t size = m_extendInfo[pos].m_binlogInfoSize;
	assert(size != 0);
	binlogInfo->unserialize(mc, (byte*)blAddress, size);

	return true;
}

/**
 * �õ�����binlog��Ϣ����
 * @return ������Ϣ����
 */
size_t BinlogBuffer::getBinlogCount() const {
	return m_extendInfo.size();
}

/**
 * �����Ƿ񻺴�Ϊ��
 */
bool BinlogBuffer::isEmpty() const {
	return m_size == 0;
}

/** ���������Ϣ����tableId��������
 * ���������������ȶ�������Ϊͬһ��table��binlog��Ϣ���ܴ���˳����ִ�У����ܻ����
 */
void BinlogBuffer::sortByTableId() {
	std::stable_sort(m_extendInfo.begin(), m_extendInfo.end());
}

/**
 * BinlogBuffer ���Ƿ����ָ�������־
 * @param tableId ��id
 * @return true��ʶ����ָ������־
 */
bool BinlogBuffer::containTable(u16 tableId) {
	for (size_t i = 0; i < m_extendInfo.size(); ++i)
		if (m_extendInfo[i].m_tableId == tableId)
			return true;
	return false;
}

/**
 * ��ջ���
 */
void BinlogBuffer::clear() {
	m_extendInfo.clear();
	m_buffer->reset();
	m_size = 0;
	m_bufferCleanEvent.signal(true);
}

/** �жϵ�ǰ�����ǲ��ǻ����Ѿ�ʹ����
 * @return true��ʾ����������false������
 */
bool BinlogBuffer::isNearlyFull() {
	return m_size >= (size_t)(m_bufferInitSize * BUFFER_FULL_RATIO);
}


/**
 * �ȴ����汻���
 */
void BinlogBuffer::waitForClean() {
	long sigToken = m_bufferCleanEvent.reset();
	while (true) {
		/* �����������ȴ���ԭ���ǣ�������ж�isEmpty�����ܳ��ֵ�ǰ��д����Ϊ�գ����Ƕ���������ˢ���ݵ����
		 * ��ʱ���ǲ���ֱ�ӷ��صģ�����ȵ������������ˢд�ɾ���
		 * ���ڶ�д����һֱ�ڲ�ͣ���滻��ֱ�ӵȴ����϶����ᵼ��˯���ѵ����
		 */
		m_bufferCleanEvent.wait(0, sigToken);
		sigToken = m_bufferCleanEvent.reset();
		if (isEmpty())
			break;
	}
}

/**
 * �õ�������־�Ĵ�С
 * @return ���ػ�����־���ܴ�С
 */
size_t BinlogBuffer::getBufferSize() const {
	return m_size;
}

/**
 * �õ�ָ��binlog���л�֮��ռ�û���Ĵ�С
 * @param pos	ָ����binlogλ��
 * @return ռ�û����С
 */
size_t BinlogBuffer::getBinlogSize( uint pos ) {
	if (pos >= m_extendInfo.size())
		return 0;

	return m_extendInfo[pos].m_binlogInfoSize;
}

/** ���ػ����ܷ��ٴ洢ָ����С�����ݶ����ᳬ������
 * @pre ��Ҫ�������Ʋ��ܱ�֤���׼ȷ
 * @return true	�㹻����size��С�Ļ��棬false����������ô�໺��
 */
bool BinlogBuffer::isBufferFull() {
	return m_size > m_bufferInitSize;
}
/************************************************************************/
/* ���������ʵ��                                                       */
/************************************************************************/

/** ���캯��
 * @param bufferSize	��ʼ�����С
 */
BinlogBufferManager::BinlogBufferManager( size_t bufferSize /*= 2 * BinlogBuffer::DEFAULT_BUFFER_SIZE*/ ) : 
	m_mutex("BinlogBufferManager::m_mutex", __FILE__, __LINE__), m_switchEvent(false),
		m_fullEvent(false), m_readMutex("BinlogBufferManager::m_readMutex", __FILE__, __LINE__) {
	m_readBuffer = new BinlogBuffer(bufferSize / 2);
	m_writeBuffer = new BinlogBuffer(bufferSize / 2);
	m_pos = 0;
	memset(&m_status, 0, sizeof(m_status));
}

/** ��������
 */
BinlogBufferManager::~BinlogBufferManager() {
	assert(m_readBuffer->isEmpty() && m_writeBuffer->isEmpty());
	delete m_readBuffer;
	delete m_writeBuffer;
}

/** дָ��binlog��Ϣ
 * �����л��ж��ǲ�����Ҫ������д����
 * @param tableDef		binlog��¼��������
 * @param binlogInfo	binlog��Ϣ����
 */
void BinlogBufferManager::writeBinlogInfo( const TableDef *tableDef, BinlogInfo *binlogInfo ) {
	MutexGuard guard(&m_mutex, __FILE__, __LINE__);

	// ѭ���ȴ���֤�������㹻�Ŀռ�д��
	long sigToken;
	while (true) {
		sigToken = m_fullEvent.reset();
		if (m_writeBuffer->isBufferFull()) {
			guard.unlock();
			m_fullEvent.wait(-1, sigToken);
			guard.lock(__FILE__, __LINE__);
		} else {
			m_fullEvent.signal();
			break;
		}
	}	// while����֮��guard�����Ծɳ���
	
	m_writeBuffer->append(tableDef, binlogInfo);
	
	m_status.m_writeTimes++;
	m_status.m_writeBufferSize = m_writeBuffer->getBufferSize(); 
	m_status.m_writeBufferCount++;

	switchBuffersIfNecessary();
}

/** ��ѯ�жϵ�ǰ�����Ƿ�Ϊ�գ�Ϊ�����Ч�ʣ�����ѯ���������ص㣺
 * @param timeoutMs	��ѯ��ʱʱ�䣬��λ����Ϊ-1��ʾ����ʱ
 * @return true��ʾ�ɹ���ѯ��binlog��Ϣ��false��ʾʧ��
 */
bool BinlogBufferManager::pollBinlogInfo( int timeoutMs/* = -1*/ ) {
	long sigToken;
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		if (!m_readBuffer->isEmpty())
			return true;
		sigToken = m_switchEvent.reset();
	}

	while (m_readBuffer->isEmpty()) {	// ��������������Ϊ�����Լ�֮��ֻ����һ��д�̻߳��л����棬���Ҳ�����������л�
		u64 start = System::currentTimeMillis();
		m_switchEvent.wait(timeoutMs, sigToken);
		u64 end = System::currentTimeMillis();
		m_status.m_pollWaitTime += end - start;
		sigToken = m_switchEvent.reset();

		// ����ǳ�ʱ���ѣ�������Ч���ѣ���Ҫ�жϵ�ǰ�ǲ�����Ҫ�����л�����
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		if (m_readBuffer->isEmpty()) {
			switchBuffersIfNecessary(true);
			if (timeoutMs != -1)
				return !m_readBuffer->isEmpty();
		}
	}

	return true;
}

/** �жϵ�ǰ�Ƿ���Ҫ�滻��д���棬�����Ҫֱ���滻
 * ��Ҫ������ǰ���Ƕ������Ѿ�Ϊ����д�����Ҫ���˻��ߵ�����ǿ��Ҫ���滻����
 * @pre ��Ҫ���л���Ļ�����
 * @param force	�Ƿ�ǿ��Ҫ�󽻻����棬Ĭ��Ϊfalse
 */
void BinlogBufferManager::switchBuffersIfNecessary(bool force/* = false*/) {
	if (force || (m_readBuffer->isEmpty() && m_writeBuffer->isNearlyFull())) {
		BinlogBuffer *tmp = m_writeBuffer;
		m_writeBuffer = m_readBuffer;
		m_readBuffer = tmp;
		m_switchEvent.signal();
		m_fullEvent.signal();
		assert(force || !m_readBuffer->isEmpty());
		m_status.m_switchTimes++;
		m_status.m_readBufferSize = m_status.m_writeBufferSize;
		m_status.m_readBufferUnflushSize = m_readBuffer->getBufferSize();
		m_status.m_readBufferUnflushCount = m_readBuffer->getBinlogCount();
		m_status.m_writeBufferSize = 0;
		m_status.m_writeBufferCount = 0;
		m_writeBuffer->clear();
	}
}

/** ��ʼ׼����ȡ�����浱�е����ݣ������͵Ķ�ȡ�ǲ����ظ���
 * @pre ���뱣֤�����治Ϊ�ա��������ܣ������������ǻ���Խ��Ӧ��Խ��
 */
void BinlogBufferManager::beginReadBinlogInfo() {
	// Ϊ����m_readBuffer->containTable()�Ĳ����� �˴���Ҫ����
	MutexGuard guard(&m_readMutex, __FILE__, __LINE__);
	m_readBuffer->sortByTableId();
	m_pos = 0;
	m_status.m_readTimes++;
}

/** ��ȡ�����浱�е���һ��binlog��Ϣ
 * @param mc			out �ڴ������ģ�binlogInfo�����ݲ��ֱ���������
 * @param binlogInfo	out ���ض�ȡ��binlog��Ϣ
 * @return ��ǰ��û�гɹ�ȡ��binlog��Ϣ��true��ʾ��
 */
bool BinlogBufferManager::readNextBinlogInfo( MemoryContext *mc, BinlogInfo *binlogInfo ) {
	if (m_pos >= m_readBuffer->getBinlogCount())
		return false;
	
	bool got = m_readBuffer->getAt(mc, m_pos, binlogInfo);
	size_t size = m_readBuffer->getBinlogSize(m_pos++);
	m_status.m_readBufferUnflushCount--;
	m_status.m_readBufferUnflushSize -= size;

	return got;
}

/**
 * ����������Ķ�ȡ����
 * ����֮����Ҫ��Ҫ��ն���������ݣ��Դ�����д����ʹ��
 */
void BinlogBufferManager::endReadBinlogInfo() {
	m_pos = 0;
	m_readBuffer->clear();
	m_status.m_readBufferSize = 0;
}

/** �õ���ǰ������ܴ�С�������ѷ�����ܿռ�
 * @return �����ܴ�С
 */
size_t BinlogBufferManager::getBufferTotalSize() {
	return m_readBuffer->getBufferSize() + m_writeBuffer->getBufferSize();
}


/** 
 * ˢ��ָ�������־
 * ��ǰ�߳���Ҫ�ȴ�ˢд�������
 * @post ִ��ʱ�̵ĸñ�binlog�������ݶ���ˢ��
 */
void BinlogBufferManager::flush(u16 tableId) {
	if (containTable(tableId))
		flushAll();
}


/** ˢд����ǰΪֹ�����л��棬ʵ��Ч���Ǳ�֤��ǰд��������ݶ���д��
 * ��ǰ�߳���Ҫ�ȴ�ˢд�������
 * @post ִ��ʱ�̵�binlog�������ݶ���ˢ��
 */
void BinlogBufferManager::flushAll() {
	BinlogBuffer *writeBuffer, *readBuffer;
	// Ϊ��������ܣ�����ÿ�ιرձ�Ĺ����ж�Ҫ�ȴ�������գ����ܳ�һ��ʱ��û��DML������ʱ��
	// �������涼Ϊ�գ�Ӧ�ÿ��Ժܿ�ķ���
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		writeBuffer = m_writeBuffer;
		readBuffer = m_readBuffer;
		if (writeBuffer->isEmpty() && readBuffer->isEmpty())
			return;
	}
	// ���ﲻ��Ҫͬ������ʹ���л���һ�ζ�д���棬�ȴ��ڵ�ǰ�����Ҳ�Ǻ����
	writeBuffer->waitForClean();
	// ����֮��ִ��flushAll��һʱ��֮ǰ�Ļ������ݿ϶��Ѿ�ˢд������
}


/**
 * BinlogBufferManager ���Ƿ����ָ�������־
 * @param tableId ��id
 * @return true��ʶ����ָ������־
 */
bool BinlogBufferManager::containTable(u16 tableId) {
	MutexGuard guard(&m_mutex, __FILE__, __LINE__);
	if (m_writeBuffer->containTable(tableId))
		return true;
	MutexGuard readGuard(&m_readMutex, __FILE__, __LINE__);
	return m_readBuffer->containTable(tableId);
}



/************************************************************************/
/* ֱ��дbinlog��ʵ�ַ�ʽ                                               */
/************************************************************************/
/** �õ�ֱ��дbinlog�Ķ���Ψһʵ��
 * @return ntseдbinlog����ʵ��
 */
NTSEDirectBinlog* NTSEDirectBinlog::getNTSEBinlog() {
	if (m_instance == NULL)
		m_instance = new NTSEDirectBinlog();
	return m_instance;
}

/** �ͷ�ntseдbinlog����
 */
void NTSEDirectBinlog::freeNTSEBinlog() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}


/** ���캯��
 */
NTSEDirectBinlog::NTSEDirectBinlog() {
	initCallbacks();
	memset(&m_bufferStatus, 0, sizeof(m_bufferStatus));
	m_mutex = new ntse::Mutex("NTSEDirectBinlog mutex", __FILE__, __LINE__);
	m_writer = new NTSEDirectBinlog::DirectBinlogWriter();
}

/** ��������
 */
NTSEDirectBinlog::~NTSEDirectBinlog() {
	destroyCallbacks();
	m_instance = NULL;
	injector::free_instance();
	delete m_mutex;

	if (m_writer != NULL) {
		m_writer->setStop();
		delete m_writer;
		m_writer = NULL;
	}
}

/** @see NTSEBinlog::flushBinlog
 */
void NTSEDirectBinlog::flushBinlog(u16 tableId) {
	UNREFERENCED_PARAMETER(tableId);
	return;
}

/** @see NTSEBinlog::getBinlogWriterStatus
 */
struct BinlogWriterStatus NTSEDirectBinlog::getBinlogWriterStatus() {
	MutexGuard guard(m_mutex, __FILE__, __LINE__);
	return m_writer->getStatus();
}

/** @see NTSEBinlog::getBinlogBufferStatus
 */
struct BinlogBufferStatus NTSEDirectBinlog::getBinlogBufferStatus() {
	return m_bufferStatus;
}


/** @see NTSEBinlog::initCallbacks
 */
void NTSEDirectBinlog::initCallbacks() {
	NTSECallbackFN *rowInsertCB = new NTSECallbackFN();
	rowInsertCB->callback = NTSEDirectBinlog::logRowInsert;
	m_callbacks.insert(cbPair(ROW_INSERT, rowInsertCB));
	NTSECallbackFN *rowDeleteCB = new NTSECallbackFN();
	rowDeleteCB->callback = NTSEDirectBinlog::logRowDelete;
	m_callbacks.insert(cbPair(ROW_DELETE, rowDeleteCB));
	NTSECallbackFN *rowUpdateCB = new NTSECallbackFN();
	rowUpdateCB->callback = NTSEDirectBinlog::logRowUpdate;
	m_callbacks.insert(cbPair(ROW_UPDATE, rowUpdateCB));
}

/**
 * ��¼�в����binlog��־
 * @param tableDef	������������
 * @param brec		�е�ǰ�������ݣ�һ����MySQL��ʽ��
 * @param arec		�еĺ������ݣ�һ����MySQL��ʽ��
 * @param param		�ص�����
 */
void NTSEDirectBinlog::logRowInsert( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
#ifndef NTSE_KEYVALUE_SERVER
	assert(param != NULL && arec != NULL);
#else
	assert(arec != NULL);
#endif
	ha_ntse *handler = (ha_ntse*)param;

	if (!needBinlog(handler))
		return;

	writeBinlog(tableDef, BINLOG_INSERT, handler, brec, arec);
}

/**
 * ��¼��ɾ����binlog��־
 * @param tableDef	������������
 * @param brec		�е�ǰ�������ݣ�һ����MySQL��ʽ��
 * @param arec		�еĺ������ݣ�һ����MySQL��ʽ��
 * @param param		�ص�����
 */
void NTSEDirectBinlog::logRowDelete( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
#ifndef NTSE_KEYVALUE_SERVER
	assert(param != NULL);
#endif
	ha_ntse *handler = (ha_ntse*)param;

	if (!needBinlog(handler))
		return;

	writeBinlog(tableDef, BINLOG_DELETE, handler, brec, arec);
}

/**
 * ��¼�и��µ�binlog��־
 * @param tableDef	������������
 * @param brec		�е�ǰ�������ݣ�һ����MySQL��ʽ��
 * @param arec		�еĺ������ݣ�һ����MySQL��ʽ��
 * @param param		�ص�����
 */
void NTSEDirectBinlog::logRowUpdate( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
#ifndef NTSE_KEYVALUE_SERVER	
	assert(param != NULL);
#endif
	ha_ntse *handler = (ha_ntse*)param;
	if (!needBinlog(handler))
		return;

	ColList col1(brec->m_numCols, brec->m_columns);
	ColList col2(arec->m_numCols, arec->m_columns);
	u16 maxCols = brec->m_numCols + arec->m_numCols;
	ColList mergeCol(maxCols, (u16*)alloca(2 * sizeof(u16) * maxCols));
	mergeCol.merge(col1, col2);
	SubRecord beforeSR(brec->m_format, mergeCol.m_size, mergeCol.m_cols, brec->m_data, brec->m_size);
	SubRecord afterSR(arec->m_format, mergeCol.m_size, mergeCol.m_cols, arec->m_data, arec->m_size);
	writeBinlog(tableDef, BINLOG_UPDATE, handler, &beforeSR, &afterSR);
}

/**
 * ��������,����һ�������¼binlog
 * @param tableDef	����ṹ
 * @param type		binlog������
 * @param handler	ntse���
 * @param brec		Ҫд��־�ļ�¼ǰ��
 * @param arec		Ҫд��־�ļ�¼����
 */
void NTSEDirectBinlog::writeBinlog( const TableDef *tableDef, BinlogType type, ha_ntse *handler, const SubRecord *brec, const SubRecord *arec ) {
	st_table *table = NULL;
	THD *thd = NULL;

	if (handler != NULL) {
		table = handler->getMysqlTable();
		thd = handler->getTHD();
		assert(thd != NULL);
	}

	BinlogInfo binlogInfo;
	memset(&binlogInfo, 0, sizeof(binlogInfo));

	binlogInfo.m_type = type;
	binlogInfo.m_tableDef = const_cast<TableDef*>(tableDef);
	if (thd != NULL) {
		binlogInfo.m_txnInfo.m_sqlMode = thd->variables.sql_mode;
		binlogInfo.m_txnInfo.m_serverId = thd->server_id;
	}
	else {
		binlogInfo.m_txnInfo.m_sqlMode = 0;
		binlogInfo.m_txnInfo.m_serverId = ::server_id;
	}	
	
	if (brec != NULL) {
		binlogInfo.m_beforeSR.m_format = REC_MYSQL;
		binlogInfo.m_beforeSR.m_numCols = brec->m_numCols;
		binlogInfo.m_beforeSR.m_columns = brec->m_columns;
		binlogInfo.m_beforeSR.m_size = brec->m_size;
		binlogInfo.m_beforeSR.m_data = brec->m_data;
	}

	if (arec != NULL) {
		binlogInfo.m_afterSR.m_format = REC_MYSQL;
		binlogInfo.m_afterSR.m_numCols = arec->m_numCols;
		binlogInfo.m_afterSR.m_columns = arec->m_columns;
		binlogInfo.m_afterSR.m_size = arec->m_size;
		binlogInfo.m_afterSR.m_data = arec->m_data;
	}

	MutexGuard guard(m_mutex, __FILE__, __LINE__);
	m_instance->m_writer->syncWrite(&binlogInfo, table);
}

/** �õ���¼binlog��������
 * @return ����"direct"
 */
const char* NTSEDirectBinlog::getBinlogMethod() const {
	return "direct";
}

/************************************************************************/
/* NTSEBinlog�Ĺ�������ʵ��                                           */
/************************************************************************/

/** �õ�NTSEBinlogFactory�ĵ���ʵ��
 * @return ���ص���ʵ��
 */
NTSEBinlogFactory* NTSEBinlogFactory::getInstance() {
	if (m_instance == NULL)
		m_instance = new NTSEBinlogFactory();
	return m_instance;
}

/** ���ٵ���ʵ��
 */
void NTSEBinlogFactory::freeInstance() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}

/** �õ�ָ����ʽ��NTSEBinlog����
 * @param method			ָ��NTSEBinlog��ʵ�ַ�ʽ��ֻ��Ϊ"mysql"/"direct"/"cached"
 * @param binlogBufferSize	����binlogʵ��ʹ�õĻ����С����,ֻ��BINLOG_METHOD_CACHED����,������Ĭ��ֵ0
 * @return ָ����ʽ��ʵ�ֶ���
 */
NTSEBinlog* NTSEBinlogFactory::getNTSEBinlog( const char* method, size_t binlogBufferSize/* = 0*/ ) {
	if (!System::stricmp(method, "direct"))
		return NTSEDirectBinlog::getNTSEBinlog();
	else if (!System::stricmp(method, "cached"))
		return NTSECachableBinlog::getNTSEBinlog(binlogBufferSize);
	else
		return NULL;
}

/** �ͷ�ָ����NTSEBinlogʵ��
 * @param ntseBinlog Ҫ�ͷŵ�NTSEBinlogʵ��,���غ��ָ����NULL
 */
void NTSEBinlogFactory::freeNTSEBinlog( NTSEBinlog **ntseBinlog ) {
	const char* method = (*ntseBinlog)->getBinlogMethod();
	if (!System::stricmp(method, "direct"))
		NTSEDirectBinlog::freeNTSEBinlog();
	else if (!System::stricmp(method, "cached"))
		NTSECachableBinlog::freeNTSEBinlog();
	else
		NTSE_ASSERT(false);

	*ntseBinlog = NULL;
}

/************************************************************************/
/* NTSEDirectBinlog::DirectBinlogWriterʵ��                                                                     */
/************************************************************************/


/** DirectBinlogWriter���캯��
 */
NTSEDirectBinlog::DirectBinlogWriter::DirectBinlogWriter() : BinlogWriter(), m_waitEvent(false), m_writtenEvent(false) {
	m_binlogInfo = NULL;
	m_table = NULL;
	m_status.m_maxTransLogs = m_status.m_minTransLogs = 1;
	if (!createTHD()) {
		nftrace(ts.mysql, tout << "Binlog writer thread create thd fail and exit!");
		NTSE_ASSERT(false);
	}
}

/** ͬ��дָ����binlogInfo
 * @param binlogInfo	Ҫд��binlogInfo
 * @param table			binlog��¼����TABLE����
 */
void NTSEDirectBinlog::DirectBinlogWriter::syncWrite( BinlogInfo *binlogInfo, TABLE *table ) {
	assert(m_binlogInfo == NULL);
	m_binlogInfo = binlogInfo;
	m_table = table;
	writeInTxn();
	m_binlogInfo = NULL;
	m_table = NULL;
}

/** ����injector������дһ��binlog��־
 */
void NTSEDirectBinlog::DirectBinlogWriter::writeInTxn() {
	assert(m_binlogInfo != NULL);

	/**
	*	�����Ҫ��keyvalue��������Ҫͨ�����ַ�ʽ�򿪱�ṹ
	*/
	if (m_table == NULL) {		
#ifdef NTSE_KEYVALUE_SERVER
		if (ParFileParser::isPartitionByPhyicTabName(m_binlogInfo->m_tableDef->m_name)) {
			char logicTabName[Limits::MAX_NAME_LEN + 1] = {0};
			bool ret = ParFileParser::parseLogicTabName(m_binlogInfo->m_tableDef->m_name, logicTabName, Limits::MAX_NAME_LEN + 1);
			assert(ret);
			UNREFERENCED_PARAMETER(ret);
			m_table = KeyValueServer::openMysqlTable(m_thd, m_binlogInfo->m_tableDef->m_id, m_binlogInfo->m_tableDef->m_schemaName, logicTabName);
		} else {
			m_table = KeyValueServer::openMysqlTable(m_thd, m_binlogInfo->m_tableDef->m_id, m_binlogInfo->m_tableDef->m_schemaName, m_binlogInfo->m_tableDef->m_name);
		}
#endif
	}
	assert(m_table != NULL);

	injector::transaction trans;
	startTransaction(&trans, m_table, &m_binlogInfo->m_txnInfo);

	writeBinlog((void*)&trans, m_table, m_binlogInfo);

	if (!commitTransaction(&trans)) {
		sql_print_error("Commit ntse binlog failed, may be caused by binlog_cache_size or max_binlog_cache_size is not big enough.");
	}

	m_status.onWrittenBinlog(m_binlogInfo);
	m_status.onTransCommit(1);
}
