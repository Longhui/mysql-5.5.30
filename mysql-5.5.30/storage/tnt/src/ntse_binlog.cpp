/**
 * NTSE记录binlog相关的内容实现
 * @author 苏斌(naturally@163.org)
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

NTSECachableBinlog *NTSECachableBinlog::m_instance = NULL;		/** 单件模式实例对象 */
BinlogBufferManager *NTSECachableBinlog::m_blBuffer = NULL;		/** 写binlog使用的缓存管理器 */

NTSEDirectBinlog *NTSEDirectBinlog::m_instance = NULL;			/** 单件模式实例对象 */
NTSEDirectBinlog::DirectBinlogWriter * NTSEDirectBinlog::DirectBinlogWriter::m_instance = NULL;	/** 单件实例 */
ntse::Mutex *NTSEDirectBinlog::m_mutex = NULL;						/** 互斥锁 */

BinlogBufferStatus NTSEDirectBinlog::m_bufferStatus;

NTSEBinlogFactory *NTSEBinlogFactory::m_instance = NULL;		/** 单件模式实例对象 */

/************************************************************************/
/* BinlogInfo                                                           */
/************************************************************************/

/**
 * 估算binlog信息在缓存当中存储需要多少空间
 * @param tableDef	binlog涉及记录所属表定义
 * @return 返回信息存储占用实际长度
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
 * 将当前binlogInfo写入指定的缓存地址
 * @param tableDef	binlog涉及记录所属表定义
 * @param buffer		缓存起始地址
 * @param bufferSize	缓存有效长度
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
 * 从指定缓存中读取构造一条binlogInfo
 * @param mc		内存上下文
 * @param buffer	缓存的起始地址
 * @param bufferSize缓存的有效长度
 */
void BinlogInfo::unserialize( MemoryContext *mc, byte *buffer, u32 bufferSize ) {
	memset(&m_beforeSR, 0, sizeof(m_beforeSR));
	memset(&m_afterSR, 0, sizeof(m_afterSR));

	Stream s(buffer, bufferSize);
	try {
		// 读取构造信息内容
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
/* BinlogWriter类实现                                                   */
/************************************************************************/

/** 构造函数
 * @param blBuffer 写线程使用的binlog缓存对象
 */
NTSECachableBinlog::CachedBinlogWriter::CachedBinlogWriter( BinlogBufferManager *blBuffer ) : BinlogWriter(), m_lastTableId((u16)-1), m_blBuffer(blBuffer) { 
	m_inj = injector::instance();
	memset(&m_status, 0, sizeof(m_status));
}

/** 析构函数
 */
NTSECachableBinlog::CachedBinlogWriter::~CachedBinlogWriter() {
	injector::free_instance();
}

/** 刷写binlog线程主函数
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

				// 统计信息记录
				m_status.onTransCommit(transLogs);
				transLogs = 0;				

				if (handler != NULL)
					handlerInfo->returnHandler(handler);
				handler = handlerInfo->reserveSpecifiedHandler(tableId);
				if (handler != NULL)
					table = handler->getMysqlTable();
				else {
					
					/**
					 *	如果需要打开keyvalue服务，则需要通过此种方式打开表结构
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

/** 判断当前写binlog的表是不是和之前的表不一样
 * @param curTblId	当前表对象ID
 * @return true表示表已经切换过，false表示一样
 */
bool NTSECachableBinlog::CachedBinlogWriter::isTableSwitched( u16 curTblId ) {
	return m_lastTableId == (u16)-1 || m_lastTableId != curTblId;
}


/************************************************************************/
/* BinlogWriter类实现                                                   */
/************************************************************************/

/**
 * 开始一个事务
 * @param trans		事务对象
 * @param table		事务使用的TABLE对象
 * @param txnInfo	要写binlog的事务的信息
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
 * 结束一个事务
 * @param trans	事务对象
 * @return true表示提交成功,false表示提交失败
 */
bool BinlogWriter::commitTransaction( void *trans ) {
	injector::transaction *realTrans = (injector::transaction*)trans;
	if (!realTrans->good())
		return false;

	if (realTrans->commit()) {
		// TODO: 处理失败的情况
		return false;
	}

	m_thd->variables.sql_mode = 0;
	return true;
}



/** 创建THD线程对象
 * 模拟NDB的ndb_binlog_thread_func函数当中创建THD的实现
 * @return true表示创建成功，返回false表示创建失败
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

	// TODO：试验看是不是不需要打开相关的表
	return true;
}

/** 销毁THD对象
 */
void BinlogWriter::destroyTHD() {
	close_thread_tables(m_thd);	// TODO: 可能不需要调用
	net_end(&m_thd->net);
	m_thd->cleanup();
	delete m_thd;
	my_thread_end();
}

/**
 * 根据某条记录的内容初始化指定的位图
 * 参考NDB的ndb_unpack_record函数，本函数需要做的事情有：
 * 根据record各个字段的内容，将bitmap设置为只包含这些字段的位图信息
 *
 * @param bitmap	要修改设置的位图对象
 * @param numCols	要计算位图的record包含的列数
 * @param cols		要计算位图的record各个列的列号
 */
void BinlogWriter::initBITMAP( struct st_bitmap *bitmap, u16 numCols, u16 *cols ) {
	assert(numCols != 0 && cols != NULL);
	//// TODO:确定这个有没有用，NDB实现当中包含的
	//if (table->s->null_bytes > 0)
	//	buf[table->s->null_bytes - 1]|= 256U - (1U << table->s->last_null_bit_pos);

	bitmap_clear_all(bitmap);
	for (uint i = 0; i < numCols; i++) {
		bitmap_set_bit(bitmap, cols[i]);
	}
}

/** 写binlog日志
 * @param trans			事务对象
 * @param table			所属表的mysql上层表对象
 * @param binlogInfo	要写的binlog日志信息
 */
void BinlogWriter::writeBinlog( void *trans, TABLE *table, BinlogInfo *binlogInfo ) {
	injector::transaction *realTrans = (injector::transaction*)trans;
	u32 numFields = table->s->fields;

	MY_BITMAP bitmap;
	uint32 bitBuf[128 / (sizeof(uint32) * 8)];	// bitmap需要的缓存
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
/* NTSEBinlog类实现                                                     */
/************************************************************************/
/** 注册NTSE的回调函数
 * @param ntsedb	ntse使用db对象
 */
void NTSEBinlog::registerNTSECallbacks( Database *ntsedb ) {
	for (cbItor itor = m_callbacks.begin(); itor != m_callbacks.end(); itor++)
		ntsedb->registerNTSECallback((CallbackType)itor->first, itor->second);
}

/** 注销NTSE的回调函数
 * @param ntsedb	ntse使用db对象
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

/** 根据handler对象中的thd的信息，判断是否需要写更新binlog
 * 如果当前是slave,并且指定了opt_log_slave_updates=false,也不用记录binlog
 */
bool NTSEBinlog::needBinlog( ha_ntse *handler ) {
	return handler == NULL || (!handler->isSlaveThread() || opt_log_slave_updates);
}

/************************************************************************/
/* NTSECachableBinlog类实现                                             */
/************************************************************************/

/** 得到NTSE写日志对象唯一实例
 * @param bufferSize	binlog所能使用的缓存大小
 * @return NTSEBinlog唯一对象
 */
NTSECachableBinlog* NTSECachableBinlog::getNTSEBinlog(size_t bufferSize) {
	if (m_instance == NULL)
		m_instance = new NTSECachableBinlog(bufferSize);
	return m_instance;
}

/** 构造函数
 * @param bufferSize	binlog所能使用的缓存大小
 */
NTSECachableBinlog::NTSECachableBinlog(size_t bufferSize) {
	m_blBuffer = NULL;
	init(bufferSize);
}

/** 析构函数
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

/** 初始化函数
 * @param bufferSize	binlog所能使用的缓存大小
 */
void NTSECachableBinlog::init(size_t bufferSize) {
	initCallbacks();

	m_blBuffer = new BinlogBufferManager(bufferSize);

	m_writer = new CachedBinlogWriter(m_blBuffer);
	m_writer->start();
}

/** 销毁处理函数
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

/** 处理在关闭表的时候的binlog操作
 * 按照约定，在NTSE的表关闭之前，需要调用这个回调，将当前binlog缓存当中的信息刷写出去
 * @param tableDef	操作所属表定义
 * @param brec		行记录的前镜像，这里为NULL，不需要使用
 * @param arec		行记录的后镜像，这里为NULL，不需要使用
 * @param param		回调参数
 */
void NTSECachableBinlog::onCloseTable( const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param ) {
	UNREFERENCED_PARAMETER(brec);
	UNREFERENCED_PARAMETER(arec);
	UNREFERENCED_PARAMETER(param);
	m_blBuffer->flush(tableDef->m_id);
}

/** 在表结构被修改时刷出binlog
 * 按照约定，在NTSE的表关闭之前，需要调用这个回调，将当前binlog缓存当中的信息刷写出去
 * @param tableDef	操作所属表定义，这里为NULL，不需要使用
 * @param brec		行记录的前镜像，这里为NULL，不需要使用
 * @param arec		行记录的后镜像，这里为NULL，不需要使用
 * @param param		回调参数
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
 * 记录行插入的binlog日志
 * @param tableDef	操作所属表定义
 * @param brec		行的前镜像内容，一定是MySQL格式的
 * @param arec		行的后镜像内容，一定是MySQL格式的
 * @param param		回调参数
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
 * 记录行删除的binlog日志
 * @param tableDef	操作所属表定义
 * @param brec		行的前镜像内容，一定是MySQL格式的
 * @param arec		行的后镜像内容，一定是MySQL格式的
 * @param param		回调参数
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
 * 记录行更新的binlog日志
 * @param tableDef	操作所属表定义
 * @param brec		行的前镜像内容，一定是MySQL格式的
 * @param arec		行的后镜像内容，一定是MySQL格式的
 * @param param		回调参数
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
 * 将binlog写入到缓存当中
 * @param type		写binlog的类型
 * @param thd		写binlog会话的thd对象
 * @param tableDef	所属表定义
 * @param bNumCols	记录前项属性数
 * @param bColumns	记录前项属性
 * @param bSize		记录前项数据长度
 * @param bData		记录前项数据
 * @param aNumCols	记录后项属性数
 * @param aColumns	记录后项属性
 * @param aSize		记录后项数据长度
 * @param aData		记录后项数据
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
 * 释放销毁binlog记录实例
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

/** 得到binlog操作的类型
 * @return 返回"cached"
 */
const char* NTSECachableBinlog::getBinlogMethod() const {
	return "cached";
}
/************************************************************************/
/* Binlog日志缓存类                                                     */
/************************************************************************/

const double BinlogBuffer::BUFFER_FULL_RATIO = 0.95;	/** 缓存预满警戒阈值 */

/** 构造函数
 * @param bufferSize	初始缓存大小
 */
BinlogBuffer::BinlogBuffer( size_t bufferSize /*= DEFAULT_BUFFER_SIZE*/ ) : m_bufferCleanEvent(false) {
	m_buffer = new MemoryContext(Limits::PAGE_SIZE, bufferSize / Limits::PAGE_SIZE);
	m_extendInfo.reserve(DEFAULT_RESERVE_EXTENDBINLOGINFO_SIZE);
	m_size = 0;
	m_bufferInitSize = bufferSize;
}

/** 析构函数
 */
BinlogBuffer::~BinlogBuffer() {
	m_extendInfo.clear();
	delete m_buffer;
}

/**
 * 添加指定binlog信息到缓存
 * @param tableDef		binlog记录所属表定义
 * @param binlogInfo	binlog信息
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
 * 得到指定位置的binlog信息
 * @param mc			内存上下文
 * @param pos			要获取第几项
 * @param binlogInfo	out 得到binlog信息之后返回
 * @return true表示pos项获取成功，false表示pos超过当前有效信息大小
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
 * 得到缓存binlog信息个数
 * @return 缓存信息个数
 */
size_t BinlogBuffer::getBinlogCount() const {
	return m_extendInfo.size();
}

/**
 * 返回是否缓存为空
 */
bool BinlogBuffer::isEmpty() const {
	return m_size == 0;
}

/** 将缓存的信息按照tableId进行排序
 * 这里的排序必须是稳定排序，因为同一个table的binlog信息不能打乱顺序再执行，可能会出错
 */
void BinlogBuffer::sortByTableId() {
	std::stable_sort(m_extendInfo.begin(), m_extendInfo.end());
}

/**
 * BinlogBuffer 中是否包含指定表的日志
 * @param tableId 表id
 * @return true标识包含指定表日志
 */
bool BinlogBuffer::containTable(u16 tableId) {
	for (size_t i = 0; i < m_extendInfo.size(); ++i)
		if (m_extendInfo[i].m_tableId == tableId)
			return true;
	return false;
}

/**
 * 清空缓存
 */
void BinlogBuffer::clear() {
	m_extendInfo.clear();
	m_buffer->reset();
	m_size = 0;
	m_bufferCleanEvent.signal(true);
}

/** 判断当前缓存是不是基本已经使用满
 * @return true表示即将用满，false不够满
 */
bool BinlogBuffer::isNearlyFull() {
	return m_size >= (size_t)(m_bufferInitSize * BUFFER_FULL_RATIO);
}


/**
 * 等待缓存被清空
 */
void BinlogBuffer::waitForClean() {
	long sigToken = m_bufferCleanEvent.reset();
	while (true) {
		/* 这里无条件等待的原因是，如果先判断isEmpty，可能出现当前的写缓存为空，但是读缓存正在刷数据的情况
		 * 这时候是不能直接返回的，必须等到读缓存的数据刷写干净，
		 * 由于读写缓存一直在不停的替换，直接等待，肯定不会导致睡不醒的情况
		 */
		m_bufferCleanEvent.wait(0, sigToken);
		sigToken = m_bufferCleanEvent.reset();
		if (isEmpty())
			break;
	}
}

/**
 * 得到缓存日志的大小
 * @return 返回缓存日志的总大小
 */
size_t BinlogBuffer::getBufferSize() const {
	return m_size;
}

/**
 * 得到指定binlog序列化之后占用缓存的大小
 * @param pos	指定的binlog位置
 * @return 占用缓存大小
 */
size_t BinlogBuffer::getBinlogSize( uint pos ) {
	if (pos >= m_extendInfo.size())
		return 0;

	return m_extendInfo[pos].m_binlogInfoSize;
}

/** 返回缓存能否再存储指定大小的内容而不会超过限制
 * @pre 需要并发控制才能保证结果准确
 * @return true	足够容纳size大小的缓存，false不够容纳这么多缓存
 */
bool BinlogBuffer::isBufferFull() {
	return m_size > m_bufferInitSize;
}
/************************************************************************/
/* 缓存管理类实现                                                       */
/************************************************************************/

/** 构造函数
 * @param bufferSize	初始缓存大小
 */
BinlogBufferManager::BinlogBufferManager( size_t bufferSize /*= 2 * BinlogBuffer::DEFAULT_BUFFER_SIZE*/ ) : 
	m_mutex("BinlogBufferManager::m_mutex", __FILE__, __LINE__), m_switchEvent(false),
		m_fullEvent(false), m_readMutex("BinlogBufferManager::m_readMutex", __FILE__, __LINE__) {
	m_readBuffer = new BinlogBuffer(bufferSize / 2);
	m_writeBuffer = new BinlogBuffer(bufferSize / 2);
	m_pos = 0;
	memset(&m_status, 0, sizeof(m_status));
}

/** 析构函数
 */
BinlogBufferManager::~BinlogBufferManager() {
	assert(m_readBuffer->isEmpty() && m_writeBuffer->isEmpty());
	delete m_readBuffer;
	delete m_writeBuffer;
}

/** 写指定binlog信息
 * 过程中会判断是不是需要调换读写缓存
 * @param tableDef		binlog记录所属表定义
 * @param binlogInfo	binlog信息对象
 */
void BinlogBufferManager::writeBinlogInfo( const TableDef *tableDef, BinlogInfo *binlogInfo ) {
	MutexGuard guard(&m_mutex, __FILE__, __LINE__);

	// 循环等待保证缓存有足够的空间写入
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
	}	// while结束之后guard的锁仍旧持有
	
	m_writeBuffer->append(tableDef, binlogInfo);
	
	m_status.m_writeTimes++;
	m_status.m_writeBufferSize = m_writeBuffer->getBufferSize(); 
	m_status.m_writeBufferCount++;

	switchBuffersIfNecessary();
}

/** 轮询判断当前缓存是否为空，为了提高效率，该轮询具有如下特点：
 * @param timeoutMs	轮询超时时间，单位毫秒为-1表示不超时
 * @return true表示成功轮询到binlog信息，false表示失败
 */
bool BinlogBufferManager::pollBinlogInfo( int timeoutMs/* = -1*/ ) {
	long sigToken;
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		if (!m_readBuffer->isEmpty())
			return true;
		sigToken = m_switchEvent.reset();
	}

	while (m_readBuffer->isEmpty()) {	// 这里可以脏读，因为除了自己之外只会有一个写线程会切换缓存，而且不会出现连续切换
		u64 start = System::currentTimeMillis();
		m_switchEvent.wait(timeoutMs, sigToken);
		u64 end = System::currentTimeMillis();
		m_status.m_pollWaitTime += end - start;
		sigToken = m_switchEvent.reset();

		// 如果是超时唤醒，或者无效唤醒，需要判断当前是不是需要主动切换缓存
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		if (m_readBuffer->isEmpty()) {
			switchBuffersIfNecessary(true);
			if (timeoutMs != -1)
				return !m_readBuffer->isEmpty();
		}
	}

	return true;
}

/** 判断当前是否需要替换读写缓存，如果需要直接替换
 * 需要交换的前提是读缓存已经为空且写缓存快要满了或者调用者强行要求替换缓存
 * @pre 需要加有缓存的互斥锁
 * @param force	是否强制要求交换缓存，默认为false
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

/** 开始准备读取读缓存当中的数据，该类型的读取是不可重复的
 * @pre 必须保证读缓存不为空。对于性能，大多数情况下是缓存越满应该越好
 */
void BinlogBufferManager::beginReadBinlogInfo() {
	// 为处理m_readBuffer->containTable()的并发， 此处需要加锁
	MutexGuard guard(&m_readMutex, __FILE__, __LINE__);
	m_readBuffer->sortByTableId();
	m_pos = 0;
	m_status.m_readTimes++;
}

/** 读取读缓存当中的下一条binlog信息
 * @param mc			out 内存上下文，binlogInfo的数据部分保存在这里
 * @param binlogInfo	out 返回读取的binlog信息
 * @return 当前有没有成功取到binlog信息，true表示有
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
 * 结束读缓存的读取操作
 * 结束之后主要需要清空读缓存的内容，以待后续写缓存使用
 */
void BinlogBufferManager::endReadBinlogInfo() {
	m_pos = 0;
	m_readBuffer->clear();
	m_status.m_readBufferSize = 0;
}

/** 得到当前缓存的总大小，计算已分配的总空间
 * @return 缓存总大小
 */
size_t BinlogBufferManager::getBufferTotalSize() {
	return m_readBuffer->getBufferSize() + m_writeBuffer->getBufferSize();
}


/** 
 * 刷出指定表的日志
 * 当前线程需要等待刷写动作完成
 * @post 执行时刻的该表binlog缓存数据都被刷出
 */
void BinlogBufferManager::flush(u16 tableId) {
	if (containTable(tableId))
		flushAll();
}


/** 刷写到当前为止的所有缓存，实际效果是保证当前写缓存的数据都被写出
 * 当前线程需要等待刷写动作完成
 * @post 执行时刻的binlog缓存数据都被刷出
 */
void BinlogBufferManager::flushAll() {
	BinlogBuffer *writeBuffer, *readBuffer;
	// 为了提高性能，避免每次关闭表的过程中都要等待缓存清空，当很长一段时间没有DML操作的时候，
	// 两个缓存都为空，应该可以很快的返回
	{
		MutexGuard guard(&m_mutex, __FILE__, __LINE__);
		writeBuffer = m_writeBuffer;
		readBuffer = m_readBuffer;
		if (writeBuffer->isEmpty() && readBuffer->isEmpty())
			return;
	}
	// 这里不需要同步，即使多切换了一次读写缓存，等待在当前缓存块也是合理的
	writeBuffer->waitForClean();
	// 唤醒之后，执行flushAll那一时刻之前的缓存数据肯定已经刷写出缓存
}


/**
 * BinlogBufferManager 中是否包含指定表的日志
 * @param tableId 表id
 * @return true标识包含指定表日志
 */
bool BinlogBufferManager::containTable(u16 tableId) {
	MutexGuard guard(&m_mutex, __FILE__, __LINE__);
	if (m_writeBuffer->containTable(tableId))
		return true;
	MutexGuard readGuard(&m_readMutex, __FILE__, __LINE__);
	return m_readBuffer->containTable(tableId);
}



/************************************************************************/
/* 直接写binlog的实现方式                                               */
/************************************************************************/
/** 得到直接写binlog的对象唯一实体
 * @return ntse写binlog对象实体
 */
NTSEDirectBinlog* NTSEDirectBinlog::getNTSEBinlog() {
	if (m_instance == NULL)
		m_instance = new NTSEDirectBinlog();
	return m_instance;
}

/** 释放ntse写binlog对象
 */
void NTSEDirectBinlog::freeNTSEBinlog() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}


/** 构造函数
 */
NTSEDirectBinlog::NTSEDirectBinlog() {
	initCallbacks();
	memset(&m_bufferStatus, 0, sizeof(m_bufferStatus));
	m_mutex = new ntse::Mutex("NTSEDirectBinlog mutex", __FILE__, __LINE__);
	m_writer = new NTSEDirectBinlog::DirectBinlogWriter();
}

/** 析构函数
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
 * 记录行插入的binlog日志
 * @param tableDef	操作所属表定义
 * @param brec		行的前镜像内容，一定是MySQL格式的
 * @param arec		行的后镜像内容，一定是MySQL格式的
 * @param param		回调参数
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
 * 记录行删除的binlog日志
 * @param tableDef	操作所属表定义
 * @param brec		行的前镜像内容，一定是MySQL格式的
 * @param arec		行的后镜像内容，一定是MySQL格式的
 * @param param		回调参数
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
 * 记录行更新的binlog日志
 * @param tableDef	操作所属表定义
 * @param brec		行的前镜像内容，一定是MySQL格式的
 * @param arec		行的后镜像内容，一定是MySQL格式的
 * @param param		回调参数
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
 * 根据类型,开启一个事务记录binlog
 * @param tableDef	表定义结构
 * @param type		binlog的类型
 * @param handler	ntse句柄
 * @param brec		要写日志的记录前项
 * @param arec		要写日志的记录后项
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

/** 得到记录binlog方法类型
 * @return 返回"direct"
 */
const char* NTSEDirectBinlog::getBinlogMethod() const {
	return "direct";
}

/************************************************************************/
/* NTSEBinlog的管理工厂类实现                                           */
/************************************************************************/

/** 得到NTSEBinlogFactory的单件实例
 * @return 返回单件实例
 */
NTSEBinlogFactory* NTSEBinlogFactory::getInstance() {
	if (m_instance == NULL)
		m_instance = new NTSEBinlogFactory();
	return m_instance;
}

/** 销毁单件实例
 */
void NTSEBinlogFactory::freeInstance() {
	if (m_instance != NULL) {
		delete m_instance;
		m_instance = NULL;
	}
}

/** 得到指定方式的NTSEBinlog对象
 * @param method			指定NTSEBinlog的实现方式，只能为"mysql"/"direct"/"cached"
 * @param binlogBufferSize	允许binlog实现使用的缓存大小限制,只在BINLOG_METHOD_CACHED有用,否则用默认值0
 * @return 指定方式的实现对象
 */
NTSEBinlog* NTSEBinlogFactory::getNTSEBinlog( const char* method, size_t binlogBufferSize/* = 0*/ ) {
	if (!System::stricmp(method, "direct"))
		return NTSEDirectBinlog::getNTSEBinlog();
	else if (!System::stricmp(method, "cached"))
		return NTSECachableBinlog::getNTSEBinlog(binlogBufferSize);
	else
		return NULL;
}

/** 释放指定的NTSEBinlog实例
 * @param ntseBinlog 要释放的NTSEBinlog实例,返回后该指针置NULL
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
/* NTSEDirectBinlog::DirectBinlogWriter实现                                                                     */
/************************************************************************/


/** DirectBinlogWriter构造函数
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

/** 同步写指定的binlogInfo
 * @param binlogInfo	要写的binlogInfo
 * @param table			binlog记录所属TABLE对象
 */
void NTSEDirectBinlog::DirectBinlogWriter::syncWrite( BinlogInfo *binlogInfo, TABLE *table ) {
	assert(m_binlogInfo == NULL);
	m_binlogInfo = binlogInfo;
	m_table = table;
	writeInTxn();
	m_binlogInfo = NULL;
	m_table = NULL;
}

/** 开启injector的事务写一条binlog日志
 */
void NTSEDirectBinlog::DirectBinlogWriter::writeInTxn() {
	assert(m_binlogInfo != NULL);

	/**
	*	如果需要打开keyvalue服务，则需要通过此种方式打开表结构
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
