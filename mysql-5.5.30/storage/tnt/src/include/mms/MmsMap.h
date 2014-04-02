/**
 * MMS映射表
 *
 * @author 邵峰(shaofeng@corp.netease.com, sf@163.org)
 */

#ifndef _NTSE_MMS_MAP_H_
#define _NTSE_MMS_MAP_H_

#include "util/Portable.h"
#include "heap/Heap.h"
#include "mms/MmsPage.h"
#include "util/Hash.h"

namespace ntse {

class Mms;
class MmsTable;


/** RID映射哈希实现类 */
class RidMmsHasher {
public:
	inline unsigned int operator()(const MmsRecord* v) const {
		return RidHasher::hashCode(RID_READ((byte *)v->m_rid));
	}
};

/** RID映射等值比较实现类 */
template<typename T1, typename T2>
class RidEqualer {
public:
	inline bool operator()(const T1 &v1, const T2 &v2) const {
		return equals(v1, v2);
	}

private:
	static bool equals(const RowId &v1, const MmsRecord * v2) {
		return v1 == RID_READ((byte *)v2->m_rid);
	}
};

/** RID映射表　*/
class MmsRidMap {
public:
	MmsRidMap(Mms *mms, MmsTable *mmsTable);
	~MmsRidMap();
	void put(MmsRecord *mmsRecord);
	bool reserve(int num, bool force = false);
	void unreserve();
	void del(MmsRecord *mmsRecord);
	MmsRecord* get(RowId rowId);
	void getConflictStatus(double *avgConflictLen, size_t *maxConflictLen);

private:
	/** 所属MMS表 */
	MmsTable *m_mmsTable;
	/** 动态哈希 */
	DynHash<RowId, MmsRecord*, RidMmsHasher, RidHasher, RidEqualer<RowId, MmsRecord *> > *m_ridHash;	
	friend class MmsTable;
};

}
#endif
