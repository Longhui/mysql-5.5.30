/**
 * ����
 * ������NTSE��ָ�Ե�����¼����ԭ������Ϊ��ֻ�ڹ��ϻָ�ʱʹ�á�
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_TRANSACTION_H_
#define _NTSE_TRANSACTION_H_

#include "misc/Global.h"
#include "misc/Txnlog.h"
#include "lob/Lob.h"
#include "util/DList.h"

namespace ntse {

/** �������� */
enum TxnType {
	TXN_NONE,			/** �������� */
	TXN_INSERT,			/** �����¼���� */
	TXN_UPDATE,			/** ���¼�¼���� */
	TXN_DELETE,			/** ɾ����¼���� */
	TXN_UPDATE_HEAP,	/** MMS���¼�¼����ʱ�������� */
	TXN_MMS_FLUSH_DIRTY,/** MMSд�����ݵ���־���� */
	TXN_ADD_INDEX,		/** ������������ */


#ifdef  TNT_ENGINE		
	TXN_LOB_INSERT,		/** ������������*/
	TXN_LOB_DELETE,		/** ɾ�����������*/
#endif
};

class SubRecord;
class TableDef;
/**
 * Ԥ������־����
 */
struct PreUpdateLog {
	SubRecord	*m_subRec;	/** ��ʾ�������ݵĲ��ּ�¼��ΪREC_REDUNDANT��ʽ */
	SubRecord	*m_indexPreImage;	/** �漰����������ʱ���������漰�������������Ե�ǰ�� */
	u16		m_numLobs;		/** �����漰�Ĵ�������Ը�������������Щ����ǰ��ΪNULL����� */
	u16		*m_lobCnos;		/** �����´��������Ժ� */
	LobId	*m_lobIds;		/** �����´����ID */
	uint	*m_lobSizes;	/** �����´�����С */
	byte	**m_lobs;		/** �����´�������ݣ�NULL��ʾ����ΪNULL */
	bool	m_updateIndex;	/** �Ƿ�������� */

	u16 getNthUpdateLobsCno(u16 nthUpdateLobs);
};

/** Ԥɾ����־���� */
struct PreDeleteLog {
	RowId		m_rid;				/** ��ɾ����¼��RID */
	SubRecord	*m_indexPreImage;	/** ���������ʱ�������Ե�ǰ�� */
	u16			m_numLobs;			/** ��ɾ���������� */
	LobId		*m_lobIds;			/** �����ɾ���ļ�¼���������Ϊ�������ID���Ӻ���ǰ���У�������ΪNULL */
};

/** һ����־ */
struct LogRecord {
	LsnType		m_lsn;	/** LSN */
	LogEntry	m_log;	/** ��־���� */
};

class Database;
class Table;
class Session;
class Connection;
/** ���� */
class Transaction {
public:
	static Transaction* createTransaction(TxnType type, Database *db, u16 id, u16 tableId, Table *table) throw(NtseException);
	Transaction(Database *db, u16 id, Table *table);
	virtual ~Transaction();

	/**
	 * ������־
	 *
	 * @param lsn ��־���к�
	 * @param log ��־
	 * @throw NtseException �ļ�����ʧ��
	 */
	virtual void redo(LsnType lsn, const LogEntry *log) throw(NtseException) = 0;

	/**
	 * ������񣬵�һ�׶Σ���DML�������������UNDO
	 *
	 * @param logComplete ������־�Ƿ�����
	 * @param commit ��������־����ʱ�������Ƿ��ύ
	 * @return �Ƿ���Ҫ����completePhase2
	 */
	virtual bool completePhase1(bool logComplete, bool commit) = 0;

	/** ������񣬵ڶ��׶Σ���DML���������߼���REDO */
	virtual void completePhase2() {}

	u16 getId();
	/**
	 * ����������ûỰ
	 *
	 * @return �������ûỰ
	 */
	Session* getSession() {
		return m_session;
	}

protected:
	Database	*m_db;			/** �����������ݿ� */
	Connection	*m_conn;		/** ���ݿ����� */
	Session		*m_session;		/** �Ự */
	u16			m_tableId;		/** �����ı�ID */
	Table		*m_table;		/** �����ı� */
};

}

#endif
