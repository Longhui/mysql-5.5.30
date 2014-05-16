/**
 * NTSE�洢����ӿڶ���
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */
#ifndef _NTSE_HANDLER_H_
#define _NTSE_HANDLER_H_

#include "api/Table.h"
#include "misc/Session.h"
#include "THDInfoBase.h"

using namespace ntse;	// Make source insight happy

/** MySQL���Ӹ�����Ϣ */
struct ConnUserData {
	u32			m_thdId;	/** THD��ID */

	ConnUserData(u32 thdId) {
		m_thdId = thdId;
	}
};

//struct CmdInfo;
//class  ntse_handler;

extern handlerton *tnt_hton;

//struct TableInfoEx;
class RowCache;
//class NTSEBinlog;
/** NTSE�洢��������ӿ� */
class ntse_handler: public handler {
public:
	static int init(Database *db, Config *config);
	static int exit(void *p);

	static int ntse_close_connection(handlerton *hton, THD* thd);
	static void ntse_drop_database(handlerton *hton, char* path);
#ifdef EXTENDED_FOR_COMMIT_ORDERED
	static void ntse_commit_ordered(handlerton *hton, THD* thd, bool all);
#endif
	static int ntse_commit_trx(handlerton *hton, THD* thd, bool all);
	static int ntse_rollback_trx(handlerton *hton, THD *thd, bool all);
	static int ntse_xa_prepare(handlerton *hton, THD* thd, bool all);
	//static int ntseBinlogFunc(handlerton *hton, THD *thd, enum_binlog_func fn, void *arg);

	ntse_handler(handlerton *hton, TABLE_SHARE *table_arg);
	~ntse_handler() {
		delete m_lobCtx;
	}
	//bool check_if_incompatible_data(HA_CREATE_INFO *info, uint table_changes);
	//uint alter_table_flags(uint flags);
	//double scan_time();
	double read_time(ha_rows rows);
	//int open(const char *name, int mode, uint test_if_locked);
	int reset();
	int close(void);
	int write_row(uchar *buf);
	int update_row(const uchar *old_data, uchar *new_data);
	int delete_row(const uchar *buf);
	int index_read_map(uchar * buf, const uchar *key, key_part_map keypart_map,
		enum ha_rkey_function find_flag);
	int index_read_last_map(uchar * buf, const uchar *key, key_part_map keypart_map);
	int index_first(uchar *buf);
	int index_last(uchar *buf);
	int index_next(uchar *buf);
	int index_prev(uchar *buf);
	int index_end();
	int rnd_init(bool scan);
	int rnd_end();
	int rnd_next(uchar *buf);
	int rnd_pos(uchar *buf, uchar *pos);
	void position(const uchar *record);
	
	//int info(uint);
	//int extra(enum ha_extra_function operation);
	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);
	int external_lock(THD *thd, int lock_type);
	int start_stmt(THD *thd, thr_lock_type lock_type);
	int truncate(void);
	ha_rows records_in_range(uint inx, key_range *min_key,
		key_range *max_key);
	int delete_table(const char *from);
	int rename_table(const char * from, const char * to);
	//bool get_error_message(int error, String* buf);
	int add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add);
	int final_add_index(handler_add_index *add, bool commit);
	int prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys);
	int final_drop_index(TABLE *table_arg);
	//int analyze(THD *thd, HA_CHECK_OPT *check_opt);
	int optimize(THD *thd, HA_CHECK_OPT *check_opt);
	int check(THD* thd, HA_CHECK_OPT* check_opt);
	int repair(THD* thd, HA_CHECK_OPT* check_opt);

	/*void get_auto_increment(ulonglong offset, ulonglong increment,
		ulonglong nb_desired_values,
		ulonglong *first_value,
		ulonglong *nb_reserved_values);*/
	//void update_create_info(HA_CREATE_INFO *create_info);
	virtual int reset_auto_increment_if_nozero(ulonglong value) = 0;

	//u16 getNtseTableId();
	//st_table* getMysqlTable();
	THD* getTHD();
	virtual bool isSlaveThread() const = 0;
	virtual bool isSlaveThread(THD *thd) const = 0;

	void setNtseTable(Table *ntseTable) {
		m_ntseTable = ntseTable;
	}

protected:
	int excptToMySqlError(ErrorCode code);
	int getMysqlErrCodeTextNo(int code);
	bool transKey(uint idx, const uchar *key, key_part_map keypart_map, SubRecord *out);
	void initReadSet(bool idxScan, bool posScan = false);
	u16* transCols(MY_BITMAP *bitmap, u16 *numCols, bool idxScan = false, uint idx = 0);
	void parseKeyparMap(uint idx, key_part_map keypart_map, uint *key_len, u16 *num_cols);
	void normalizePathSeperator(char *path);
	CollType getCollation(CHARSET_INFO *charset) throw(NtseException);
	bool isPrefix(TABLE *table_arg, KEY_PART_INFO *key_part);
	bool recordHasSameKey(const IndexDef *indexDef, const SubRecord *key1, const SubRecord *key2);

	virtual void attachToTHD(THD *thd, const char *name) = 0;
	virtual void detachFromTHD() = 0;
	virtual void setAutoIncrIfNecessary(uchar * buf) = 0;
	virtual OpType getOpType();

	void prepareAddIndexReal() throw(NtseException);
	virtual void addIndexReal(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add) throw(NtseException) = 0;
	virtual void finalAddIndexReal(handler_add_index *add, bool commit) throw(NtseException) = 0;
	virtual void dropIndexReal(TABLE *table_arg, uint *key_num, uint num_of_keys) throw(NtseException) = 0;
	virtual void dropTableReal(const char *name) throw(NtseException) = 0;
	virtual void deleteAllRowsReal() throw(NtseException) = 0;

	virtual void optimizeTableReal(bool keepDict, bool waitForFinish) throw(NtseException) = 0;
	virtual void renameTableReal(const char *oldPath, const char *newPath) throw(NtseException) = 0;

private:
	const char* currentSQL();
	bool is_twophase_filesort();
	int reportError(ErrorCode errCode, const char *msg, bool fatal, bool warnOrErr = false);
	//bool openTable(const char *table_name, int *eno);
	//int closeTable(TableInfoEx *share);
	bool checkTableLock();
	bool isRkeyFuncInclusived(enum ha_rkey_function flag, bool lowerBound);
	int fetchNext(uchar *buf);
	void endScan();
	void beginTblScan();
	u64 calcNextAutoIncr(u64 current, u64 increment, u64 offset, u64 maxValue);
	int indexRead(uchar *buf, const uchar *key, key_part_map keypart_map, bool forward, bool includeKey, bool exact);
	//int checkSQLLogBin();

protected:
	static const byte REF_CACHED_ROW_ID = 0;	/** ����ļ�¼��ʶ�Ǳ������¼��ID */
	static const byte REF_ROWID = 1;			/** ����ļ�¼��ʶ��RID */

	THR_LOCK_DATA	m_mysqlLock;	/** MySQL�õı��� */
	//TableInfoEx		*m_tblInfo;		/** ����ͬһ�ű��handler������һ��Ϣ */
	Session			*m_session;		/** ��ǰ�Ự */
	MemoryContext	*m_lobCtx;		/** ���ڴ洢��ѯ���ش������ڴ���������� */
	ILMode			m_wantLock;		/** Ҫ�ӵı���ģʽ */
	ILMode			m_gotLock;		/** �Ѿ��ӵı���ģʽ */
	SubRecord		m_indexKey;		/** ���������� */
	bool			m_checkSameKeyAfterIndexScan;	/** �Ƿ�Ҫ�������ɨ��ļ�¼�Ƿ������������ */
	int				m_errno;		/** ���һ�δ���� */
	char			m_errmsg[1024];	/** ���һ�δ�����Ϣ */
	bool			m_ignoreDup;	/** �Ƿ����Ψһ�Գ�ͻ
									 * ��m_ignoreDupΪtrue��m_replaceΪtrue�����ʾΪREPLACE/ON DUPLICATE KEY UPDATE��
									 * ����ΪINSERT IGNORE��m_replaceΪtrueʱ��m_ignoreDupһ��ҲΪtrue
									 */
	bool			m_replace;		/** �Ƿ�ΪREPLACE��ON DUPLICATE KEY UPDATE */
	uint			m_dupIdx;		/** INSERT/UPDATE����Ψһ�Գ�ͻ�������� */
	u64				m_mcSaveBeforeScan;	/** ��ʼɨ��֮ǰ�ڴ����������λ�� */
	RowId			m_lastRow;		/** ���һ����¼��RID */
	u64				m_beginTime;	/** ��ʼʹ�ñ�handler��ʱ�䣬��λ΢�� */

	TblScan			        *m_ntseScan;	/** ɨ���� */
	IUSequence<TblScan *>	*m_ntseIuSeq;	/** REPLACE��ͻʱ��IDU���� */
	bool			m_isRndScan;	/** �Ƿ����ڽ��б�ɨ�裬������rnd_initʱscanΪtrue */
	RowCache		*m_rowCache;	/** ��¼���� */

	THD				*m_thd;			/** ����ʹ�ñ�handler��THD */
	const char		*m_name;		/**	��ʶ��ǰTHD�����ĸ���������attach�ϵ� */
	//DLink<ha_ntse *>	m_thdInfoLnk;	/** ��THDInfo handler�����е����� */
	//DLink<ha_ntse *>	m_hdlInfoLnk;	/** ��HandlerInfo handler�����е����� */
	Connection		*m_conn;		/** NTSE���ݿ����� */

	MY_BITMAP		m_readSet;		/** �������ɨ��read_set��λͼ */
	uint32			m_readSetBuf[Limits::MAX_COL_NUM / 8 + 1];	/** λͼ����Ҫ�Ļ��� */
	bool			m_isReadAll;	/** ����ͬ����ǰɨ��m_readSet�Ƿ���Ϊ��ȡȫ������ */

	u64				m_increment;	/** �ϲ��趨�ı���sql�������������� */
	u64				m_offset;		/** �ϲ��趨�ı���sql����������ƫ�� */
	u32             m_deferred_read_cache_size; /**rowcache�Ĵ�С*/
private:
	Table			*m_ntseTable;	/** ��ǰ���ڲ����ı� */
};

//extern CmdExecutor *cmd_executor;
extern Database	*ntse_db;
//extern ulong ntse_sample_max_pages;
//extern NTSEBinlog *ntse_binlog;
//extern HandlerInfo *handlerInfo;

extern THDInfo* getTHDInfo(THD *thd);
extern void setTHDInfo(THD *thd, THDInfo *info);
//extern THDInfo* checkTHDInfo(THD *thd);
#endif


