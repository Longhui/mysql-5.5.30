/**
* ������������������ӿ�
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _NTSE_INDEX_COMMON_H_
#define _NTSE_INDEX_COMMON_H_

#include "misc/Session.h"
#include "misc/Record.h"
#include "misc/Sample.h"
#include "misc/Global.h"

namespace ntse {

enum IndexPageType {
	ROOT_PAGE = (1 << 1),			/** ��ҳ���־ */
	LEAF_PAGE = (1 << 2),			/** Ҷҳ���־ */
	ROOT_AND_LEAF = (1 << 3),		/** ���Ǹ�ҳ������Ҷҳ���־ */
	NON_LEAF_PAGE = (1 << 4),		/** ��Ҷҳ���־ */
	BEING_SMO = (1 << 5),			/** ҳ��������SMO״̬ */
	FREE = (1 << 6),				/** ҳ�洦�ڿ���״̬ */
	OVERFLOW_PAGE = (1 << 7)        /** ���ҳ���־(ֻ����BLink��) */
};

enum IDXResult {
	IDX_SUCCESS = 0,			/** ���������ɹ� */
	IDX_FAIL,					/** ��������ʧ�� */
	IDX_RESTART					/** ������������LSN�ı�(�������)/ҳ��ʱ����ı�(�ڴ�����)���������Ҫ����ĳЩ�ض�λ */
};

enum FindType {
	IDX_SEARCH = 0,				/** ��ǰ�ǲ��ҹ��̵ı�־ */
	IDX_ESTIMATE				/** ��ǰ��Ԥ������۹��̵ı�־ */
};

enum IDXOperation {
	IDX_INSERT = 0,				/** ��ʶ������� */
	IDX_DELETE,					/** ��ʶɾ������ */
	IDX_UPDATE					/** ��ʶ���²��� */
};

// �������Ҫ����IDX_RESULT�жϵ�switch���꣬����IDX_RESTART��IDX_FAIL���Ҫ������ת
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

/** ���ұ�� */
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
	bool m_forward;     /** ǰ��������� */
	bool m_includeKey;  /** �����Ƿ����=��� */
	bool m_equalAllowed;/** ������ȱ�� */		

public:
	static SearchFlag DEFAULT_FLAG;			/** Ĭ��������Ƕ��� */
};

typedef int (*CompareKey)(const TableDef *tableDef, const SubRecord *key1, const SubRecord *key2, 
						  const IndexDef *indexDef);	/** �ȽϺ���ָ�붨�� */

/**
 *	������ֵ�Ƚ���
 */
class KeyComparator {
public:
	KeyComparator(const TableDef *tableDef, const IndexDef *indexDef) {
		m_tableDef = tableDef;
		m_indexDef = indexDef;
		m_comparator = NULL;
	}

	/**
	* ���ñȽϺ���
	* @param comparator	�Ƚ���
	*/
	void setComparator(CompareKey comparator) {
		m_comparator = comparator;
	}

	/**
	* ���������ıȽϺ������бȽ�
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
	const TableDef *m_tableDef;		/** �ȽϺ���ʹ��������Ӧ�ı��� */
	const IndexDef *m_indexDef;		/** �Ƚϼ�ֵ���������Ķ��� */
	CompareKey m_comparator;		/** �ȽϺ���ָ�� */
};

class IndexBase {
public:
	virtual ~IndexBase() {}
	// �����ӿ�
	virtual SampleHandle *beginSample(Session *session, uint wantSampleNum, bool fastSample) = 0;
	virtual Sample * sampleNext(SampleHandle *handle) = 0;
	virtual void endSample(SampleHandle *handle) = 0;
	virtual DBObjStats* getDBObjStats() = 0;
};

}

#endif