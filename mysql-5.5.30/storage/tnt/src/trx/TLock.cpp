/**
 * TNT锁管理模块实现。
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */

#include "api/TNTTable.h"
#include "api/TNTDatabase.h"
#include "api/TNTTblScan.h"
#include "trx/TLock.h"
#include "trx/TNTTransaction.h"

namespace tnt {

/**
 * 事务锁表构造
 * @param maxLocks  最多支持的锁个数
 * @param timeoutMs 加锁超时时间
 */
TLockSys::TLockSys(uint maxLocks, int timeoutMs) {
	m_lockTable = new DldLockTable(maxLocks, timeoutMs);
}

TLockSys::~TLockSys() {
	delete m_lockTable;
	m_lockTable = NULL;
}

bool TLockSys::checkResult(DldLockResult result) throw(NtseException) {
	if (result == DLD_LOCK_SUCCESS || result == DLD_LOCK_IMPLICITY)
		return true;
	else if (result == DLD_LOCK_FAIL)
		return false;
	else
		translateErr(result);
	return false;
}

void TLockSys::translateErr(DldLockResult result) throw(NtseException) {
	if (result == DLD_LOCK_TIMEOUT) {// 锁超时
		NTSE_THROW(NTSE_EC_LOCK_TIMEOUT, "Require transaction lock timeout!");
	} else if (result == DLD_LOCK_DEAD_LOCK) {// 检测到死锁
		NTSE_THROW(NTSE_EC_DEADLOCK, "Dead lock is detected when requiring transaction lock!");
	} else if (result == DLD_LOCK_LACK_OF_MEM) {// 内存不足
		NTSE_THROW(NTSE_EC_OUT_OF_MEM, "There is not enough memory left for creating transaction lock!");
	}
	assert(result == DLD_LOCK_SUCCESS || result == DLD_LOCK_IMPLICITY || result == DLD_LOCK_FAIL);
}

/**
 * 判断指定的行是否被加了行锁
 * @param rowId
 * @param tabId
 * @param bePrecise
 * @return 
 */
bool TLockSys::pickRowLock(RowId rowId, TableId tabId, bool bePrecise) {
	u64 key = convertIdsToKey(tabId, rowId);
	return m_lockTable->pickLock(key, bePrecise);
}

/**
 * 尝试加行锁
 * @param owner
 * @param lockMode
 * @param rowId
 * @param tabId
 * @return 
 */
DldLockResult TLockSys::tryLockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, 
						  TableId tabId) {
	u64 key = convertIdsToKey(tabId, rowId);
	return m_lockTable->tryLock(owner, key, lockMode);
}

/**
 * 加事务级行锁
 * @param trx
 * @param lockMode
 * @param rowId
 * @param tabId
 * @throw 
 */
DldLockResult TLockSys::lockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, 
					   TableId tabId) {
	u64 key = convertIdsToKey(tabId, rowId);

	return m_lockTable->lock(owner, key, lockMode);
}

/**
 * 尝试加表锁
 * @param owner
 * @param lockMode
 * @param tabId
 * @return 
 */
bool TLockSys::tryLockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId) {
	u64 key = convertIdsToKey(TABLE_LOCK_VTBL_ID, (RowId)tabId);

	DldLockResult result = m_lockTable->tryLock(owner, key, lockMode);
	
	// try lock 不抛错，只返回成功或者失败
	try {
		return checkResult(result);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		return false;
	}
}

/**
 * 加事务级表锁
 * @param trx
 * @param lockMode
 * @param tabId
 * @param table
 * @param timeoutMs
 */
DldLockResult TLockSys::lockTable(DldLockOwner *owner, TLockMode lockMode, 
								TableId tabId) {
	u64 key = convertIdsToKey(TABLE_LOCK_VTBL_ID, (RowId)tabId);

	return m_lockTable->lock(owner, key, lockMode);
}

/**
 * 释放事务级行锁
 * @param trx
 * @param lockMode
 * @param rowId
 * @param tabId
 * @return 
 */
bool TLockSys::unlockRow(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId) {
	u64 key = convertIdsToKey(tabId, rowId);
	return m_lockTable->unlock(owner, key, lockMode);
}

/**
 * 释放事务级表锁
 * @param trx
 * @param lockMode
 * @param tabId
 * @return 
 */
bool TLockSys::unlockTable(DldLockOwner *owner, TLockMode lockMode, TableId tabId) {
	u64 key = convertIdsToKey(TABLE_LOCK_VTBL_ID, (RowId)tabId);
	return m_lockTable->unlock(owner, key, lockMode);
}

/**
 * 尝试加自增序列锁
 * @param owner
 * @param lockMode
 * @param autoIncr
 * @return 
 */
bool TLockSys::tryLockAutoIncr(DldLockOwner *owner, TLockMode lockMode, u64 autoIncr) {
	u64 key = convertIdsToKey(AUTOINCR_LOCK_VTBL_ID, autoIncr);

	DldLockResult result = m_lockTable->tryLock(owner, key, lockMode);

	// try lock 不抛错，只返回成功或者失败
	try {
		return checkResult(result);
	} catch (NtseException &e) {
		UNREFERENCED_PARAMETER(e);
		return false;
	}
}

/**
 * 对自增序列加锁
 * @param trx
 * @param lockMode
 * @param autoIncr
 * @return 
 */
void TLockSys::lockAutoIncr(DldLockOwner *owner, TLockMode lockMode, 
								   u64 autoIncr) throw(NtseException) {
	u64 key = convertIdsToKey(AUTOINCR_LOCK_VTBL_ID, autoIncr);

	DldLockResult result = m_lockTable->lock(owner, key, lockMode);

	return translateErr(result);
}

/**
 * 对自增序列解锁
 * @param trx
 * @param lockMode
 * @param autoIncr
 * @return 
 */
bool TLockSys::unlockAutoIncr(DldLockOwner *owner, TLockMode lockMode, u64 autoIncr) {
	u64 key = convertIdsToKey(AUTOINCR_LOCK_VTBL_ID, autoIncr);
	return m_lockTable->unlock(owner, key, lockMode);
}

/**
 * 事务commit/rollback，释放属于同一事务的所有Lock
 * @param trxLockHead
 * @param trxLockCnt
 */
void TLockSys::releaseLockList(DldLockOwner *owner) {
	m_lockTable->releaseAllLocks(owner);
}

/**
 * 判断某个事务是否已经持有了指定模式的行锁
 * @param owner
 * @param lockMode
 * @param rowId
 * @param tabId
 * @return 
 */
bool TLockSys::isRowLocked(DldLockOwner *owner, TLockMode lockMode, RowId rowId, TableId tabId) {
	u64 key = convertIdsToKey(tabId, rowId);
	return m_lockTable->isLocked(owner, key, lockMode);
}

/**
 * 判断某个事务是否已经持有了指定模式的表锁
 * @param owner
 * @param lockMode
 * @param tabId
 * @return 
 */
bool TLockSys::isTableLocked(DldLockOwner *owner, TLockMode lockMode, TableId tabId) {
	u64 key = convertIdsToKey(TABLE_LOCK_VTBL_ID, (RowId)tabId);
	return m_lockTable->isLocked(owner, key, lockMode);
}

}