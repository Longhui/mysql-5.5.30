/**
* 内外存索引公共组件与接口
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _NTSE_INDEX_COMMON_H_
#define _NTSE_INDEX_COMMON_H_

#include "misc/Session.h"
#include "misc/Record.h"
#include "misc/Sample.h"
#include "misc/Global.h"

namespace ntse {

enum IndexPageType {
	ROOT_PAGE = (1 << 1),			/** 根页面标志 */
	LEAF_PAGE = (1 << 2),			/** 叶页面标志 */
	ROOT_AND_LEAF = (1 << 3),		/** 既是根页面又是叶页面标志 */
	NON_LEAF_PAGE = (1 << 4),		/** 非叶页面标志 */
	BEING_SMO = (1 << 5),			/** 页面正处于SMO状态 */
	FREE = (1 << 6),				/** 页面处于空闲状态 */
	OVERFLOW_PAGE = (1 << 7)        /** 溢出页面标志(只用于BLink树) */
};

enum IDXResult {
	IDX_SUCCESS = 0,			/** 索引操作成功 */
	IDX_FAIL,					/** 索引操作失败 */
	IDX_RESTART					/** 索引操作遇到LSN改变(外存索引)/页面时间戳改变(内存索引)的情况，需要进行某些重定位 */
};

enum FindType {
	IDX_SEARCH = 0,				/** 当前是查找过程的标志 */
	IDX_ESTIMATE				/** 当前是预估算代价过程的标志 */
};

enum IDXOperation {
	IDX_INSERT = 0,				/** 标识插入操作 */
	IDX_DELETE,					/** 标识删除操作 */
	IDX_UPDATE					/** 标识更新操作 */
};

// 定义对需要进行IDX_RESULT判断的switch语句宏，对于IDX_RESTART和IDX_FAIL结果要进行跳转
#define IDX_SWITCH_AND_GOTO(switchStmt, restartMark, failMark) 				\
	do {                                \
		switch (switchStmt) {			\
			case IDX_SUCCESS:			\
				break;						\
			case IDX_RESTART:			\
				goto restartMark;			\
			case IDX_FAIL:				\
				goto failMark;				\
			default:					\
				assert(0);					\
		};                               \
	} while (0)

/** 查找标记 */
class SearchFlag {
public:
	SearchFlag(bool forward = true, bool includeKey = true, bool equalAllowed = true)
		: m_forward(forward), m_includeKey(includeKey),  m_equalAllowed(equalAllowed) {
	}

	SearchFlag(const SearchFlag& another) {
		m_forward = another.m_forward;
		m_includeKey = another.m_includeKey;
		m_equalAllowed = another.m_equalAllowed;
	}

	void setFlag(bool forward, bool includeKey, bool equalAllowed) {
		m_forward = forward;
		m_includeKey = includeKey;
		m_equalAllowed = equalAllowed;
	}

	inline bool isForward() const {	
		return m_forward;
	}	

	inline bool isIncludingKey() const {	
		return m_includeKey;
	}	

	inline bool isEqualAllowed() const {	
		return m_equalAllowed;
	}
private:
	bool m_forward;     /** 前向搜索标记 */
	bool m_includeKey;  /** 查找是否包含=标记 */
	bool m_equalAllowed;/** 允许相等标记 */		

public:
	static SearchFlag DEFAULT_FLAG;			/** 默认搜索标记对象 */
};

typedef int (*CompareKey)(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, 
						  const IndexDef *indexDef);	/** 比较函数指针定义 */

/**
 *	索引键值比较器
 */
class KeyComparator {
public:
	KeyComparator(const TableDef *tableDef, const IndexDef *indexDef) {
		m_tableDef = tableDef;
		m_indexDef = indexDef;
		m_comparator = NULL;
	}

	/**
	* 设置比较函数
	* @param comparator	比较器
	*/
	void setComparator(CompareKey comparator) {
		m_comparator = comparator;
	}

	/**
	* 调用真正的比较函数进行比较
	*/
	inline int compareKey(const SubRecord *key1, const SubRecord *key2) {
		assert(m_comparator != NULL);
		assert(key2->m_format == KEY_COMPRESS || key2->m_format == KEY_NATURAL);
		assert((key1->m_format == REC_REDUNDANT && m_comparator == RecordOper::compareKeyRC) ||
			(key1->m_format == KEY_COMPRESS && m_comparator == RecordOper::compareKeyCC) ||
			(key1->m_format == KEY_PAD && m_comparator == RecordOper::compareKeyPC) ||
			(key1->m_format == KEY_NATURAL && m_comparator == RecordOper::compareKeyNN));

		int result = m_comparator(m_tableDef, key1, key2, m_indexDef);
		if (result == 0) {
			if (key1->m_rowId == INVALID_ROW_ID || key2->m_rowId == INVALID_ROW_ID)
				return 0;
			return key1->m_rowId > key2->m_rowId ? 1 : key1->m_rowId == key2->m_rowId ? 0 : -1;
		} else
			return result;
	}

private:
	const TableDef *m_tableDef;		/** 比较函数使用索引对应的表定义 */
	const IndexDef *m_indexDef;		/** 比较键值所属索引的定义 */
	CompareKey m_comparator;		/** 比较函数指针 */
};

class IndexBase {
public:
	virtual ~IndexBase() {}
	// 采样接口
	virtual SampleHandle *beginSample(Session *session, uint wantSampleNum, bool fastSample) = 0;
	virtual Sample * sampleNext(SampleHandle *handle) = 0;
	virtual void endSample(SampleHandle *handle) = 0;
	virtual DBObjStats* getDBObjStats() = 0;
};

}

#endif