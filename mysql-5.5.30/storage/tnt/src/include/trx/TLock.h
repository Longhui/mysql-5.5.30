/**
 * TNT��ģ�����
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
 */

#ifndef _TNT_TLOCK_H_
#define _TNT_TLOCK_H_

#include "util/Hash.h"
#include "util/DList.h"
#include "util/Sync.h"
#include "misc/Global.h"
#include "misc/MemCtx.h"
#include "misc/DLDLockTable.h"

using namespace ntse;

namespace tnt {

typedef DldLockResult TLockResult;
typedef DldLockMode TLockMode;
typedef DldLock TLock;

/**
 * ����������
 */
enum TLockType {
	TL_TABLE, /** ���񼶱��� */
	TL_ROW,   /** �������� */
	TL_AUTOINCR,/** ���������� */
};

static const TableId TABLE_LOCK_VTBL_ID = (u16)-1; /** �������񼶱�����������ID */
static const TableId AUTOINCR_LOCK_VTBL_ID = (u16)-2;/** ��������������������������ID */

class TNTTransaction;
class TNTTable;

/**
 * �������������
 */
class TLockSys {
public:
	TLockSys(uint maxLocks, int timeoutMs);
	~TLockSys();

	bool pickRowLock(RowId rowId, TableId tabId, bool bePrecise = false);
	bool isRowLocked(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);
	bool isTableLocked(DldLockOwner *owner, TLockMode lockMode, TableId tabId);
	
	// ����
	DldLockResult tryLockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);
	DldLockResult lockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);
	bool unlockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);

	// ����
	bool tryLockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId);
	DldLockResult lockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId);
	bool unlockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId);

	// ����������
	bool tryLockAutoIncr(DldLockOwner *owner, TLockMode lockMode, u64 autoIncr);
	void lockAutoIncr(DldLockOwner *owner, TLockMode lockMode, 
		u64 autoIncr) throw(NtseException);
	bool unlockAutoIncr(DldLockOwner *owner, TLockMode lockMode, u64 autoIncr);
	
	// �ͷų��е�����������
	void releaseLockList(DldLockOwner *owner);

	/**
	 * ����Id��RowIdƴ�������ֵ
	 * @param tabId
	 * @param rowId
	 * @return 
	 */
	inline static u64 convertIdsToKey(TableId tabId, RowId rowId) {
		return ((u64)tabId << ((sizeof(u64) - sizeof(TableId)) * 8)) | (rowId & 0xFFFFFF);
	}

	/**
	 * �������ֵ��ԭΪ��Id��RowId
	 * @param key
	 * @param tableId
	 * @param rowId
	 * @return 
	 */
	inline static void convertKeyToIds(u64 key, TableId *tableId, RowId *rowId) {
		*tableId = (TableId)(key >> ((sizeof(u64) - sizeof(TableId)) * 8) & 0xFF);
		*rowId = key & 0xFFFFFF;
	}

	/**
	 * ���������
	 * @return 
	 */
	inline static TLockType getLockType(TLock *lock) {
		u64 key = lock->getId();
		TableId tableId = (key >> (sizeof(u64) - sizeof(TableId))) & 0xFF;
		return tableId == TABLE_LOCK_VTBL_ID ? TL_TABLE : 
			(tableId == AUTOINCR_LOCK_VTBL_ID ? TL_AUTOINCR : TL_ROW);
	}

	/**
	 * ������������ʱʱ��
	 * @param lockTimeoutMs ��������ʱʱ�䣬��λ����
	 */
	inline void setLockTimeout(int lockTimeoutMs) {
		m_lockTable->setLockTimeout(lockTimeoutMs);
	}

	bool checkResult(DldLockResult result) throw(NtseException);
	void translateErr(DldLockResult result) throw(NtseException);
	
private:
	DldLockTable *m_lockTable; /** ���� */
};

}
#endif