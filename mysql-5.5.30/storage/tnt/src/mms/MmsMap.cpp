/**
 * MMS映射表管理
 *
 * 实现MmsRidMap类和MmsPKeyMap类
 * MmsRidMap类维护RowID到MMS缓存记录项的映射
 * MmsPKeyMap类维护主键到MMS缓存记录项的映射
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/MmsMap.h"
#include "mms/MmsPage.h"

namespace ntse {

/************************************************************************/
/*                 MmsRidMap类实现										*/
/************************************************************************/
/** 
 * MmsRidMap构造函数
 *
 * @param mms MMS全局对象
 * @param mmsTable MMS表对象
 */
MmsRidMap::MmsRidMap(Mms *mms, MmsTable *mmsTable) {
	m_mmsTable = mmsTable;
	m_ridHash = new DynHash<RowId, MmsRecord*, RidMmsHasher, 
		RidHasher, RidEqualer<RowId, MmsRecord*> >(mms, PAGE_MMS_MISC);
}

/** 
 * MmsRidMap析构函数
 */
MmsRidMap::~MmsRidMap() {
	delete m_ridHash;
}

/** 
 * 向RID映射表添加一条记录
 * @pre 已加MMS互斥锁
 *
 * @param mmsRecord 记录项
 */
void MmsRidMap::put(MmsRecord *mmsRecord) {
	m_ridHash->put(mmsRecord);
}

/**
 * 多预留空间
 * @pre 已加MMS表互斥锁
 *
 * @param num 多预留的个数
 * @param force 强制预留
 * @return 是否成功
 */
bool MmsRidMap::reserve(int num, bool force) {
	return m_ridHash->reserveSize(m_ridHash->getSize() + num, force);
}

/** 
 * 取消预留
 * @pre 已加MMS表互斥锁
 */
void MmsRidMap::unreserve() {
	m_ridHash->reserveSize(0);
}

/** 
 * 在RID映射表中删除一条记录
 * @pre 已加MMS表互斥锁
 * 
 * @param mmsRecord 记录项
 */
void MmsRidMap::del(MmsRecord *mmsRecord) {
	MmsRecord *ret = m_ridHash->remove(RID_READ(mmsRecord->m_rid));
	assert(ret);
	UNREFERENCED_PARAMETER(ret);
}

/** 
 * 在RID映射表中查询一条记录
 * @pre 已加MMS表共享锁(REDO阶段无此要求)
 *
 * @param rowId 记录的RowID
 * @return 记录项
 */
MmsRecord* MmsRidMap::get(RowId rowId) {
	return m_ridHash->get(rowId);
}

/** 
 * 获取冲突信息
 *
 * @param avgConflictLen OUT 冲突链表平均长度
 * @param maxConflictLen OUT 冲突链表最大长度
 */
void MmsRidMap::getConflictStatus(double *avgConflictLen, size_t *maxConflictLen) {
	m_ridHash->getConflictStatus(avgConflictLen, maxConflictLen);
}

}

