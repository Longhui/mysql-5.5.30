/**
 * TNT锁模块管理。
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
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
 * 事务锁类型
 */
enum TLockType {
	TL_TABLE, /** 事务级表锁 */
	TL_ROW,   /** 事务级行锁 */
	TL_AUTOINCR,/** 自增序列锁 */
};

static const TableId TABLE_LOCK_VTBL_ID = (u16)-1; /** 用于事务级表锁的虚拟表的ID */
static const TableId AUTOINCR_LOCK_VTBL_ID = (u16)-2;/** 用于自增序列事务锁的虚拟表的ID */

class TNTTransaction;
class TNTTable;

/**
 * 事务锁锁表管理
 */
class TLockSys {
public:
	TLockSys(uint maxLocks, int timeoutMs);
	~TLockSys();

	bool pickRowLock(RowId rowId, TableId tabId, bool bePrecise = false);
	bool isRowLocked(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);
	bool isTableLocked(DldLockOwner *owner, TLockMode lockMode, TableId tabId);
	
	// 行锁
	DldLockResult tryLockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);
	DldLockResult lockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);
	bool unlockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId);

	// 表锁
	bool tryLockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId);
	DldLockResult lockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId);
	bool unlockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId);

	// 自增序列锁
	bool tryLockAutoIncr(DldLockOwner *owner, TLockMode lockMode, u64 autoIncr);
	void lockAutoIncr(DldLockOwner *owner, TLockMode lockMode, 
		u64 autoIncr) throw(NtseException);
	bool unlockAutoIncr(DldLockOwner *owner, TLockMode lockMode, u64 autoIncr);
	
	// 释放持有的所有事务锁
	void releaseLockList(DldLockOwner *owner);

	/**
	 * 将表Id和RowId拼成锁表键值
	 * @param tabId
	 * @param rowId
	 * @return 
	 */
	inline static u64 convertIdsToKey(TableId tabId, RowId rowId) {
		return ((u64)tabId << ((sizeof(u64) - sizeof(TableId)) * 8)) | (rowId & 0xFFFFFF);
	}

	/**
	 * 将锁表键值还原为表Id和RowId
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
	 * 获得锁类型
	 * @return 
	 */
	inline static TLockType getLockType(TLock *lock) {
		u64 key = lock->getId();
		TableId tableId = (key >> (sizeof(u64) - sizeof(TableId))) & 0xFF;
		return tableId == TABLE_LOCK_VTBL_ID ? TL_TABLE : 
			(tableId == AUTOINCR_LOCK_VTBL_ID ? TL_AUTOINCR : TL_ROW);
	}

	/**
	 * 设置事务锁超时时间
	 * @param lockTimeoutMs 事务锁超时时间，单位毫秒
	 */
	inline void setLockTimeout(int lockTimeoutMs) {
		m_lockTable->setLockTimeout(lockTimeoutMs);
	}

	bool checkResult(DldLockResult result) throw(NtseException);
	void translateErr(DldLockResult result) throw(NtseException);
	
private:
	DldLockTable *m_lockTable; /** 锁表 */
};

}
#endif