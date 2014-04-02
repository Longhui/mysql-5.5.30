/**
 * 定义所有NTSE支持的回调函数类型
 * @author	苏斌(naturally@163.org)
 */

#ifndef _NTSE_CALLBACKS_H_
#define _NTSE_CALLBACKS_H_

#include <vector>

using namespace std;

namespace ntse {

class TableDef;

enum CallbackType {
	ROW_INSERT = 0,		/** 行插入操作回调 */
	ROW_DELETE,			/** 行删除操作回调 */
	ROW_UPDATE,			/** 行更新操作回调 */
	TABLE_CLOSE,		/** 关闭表操作回调 */
	TABLE_ALTER,		/** 修改表结构操作回调 */
	CB_NUMBERS
};

class SubRecord;
struct NTSECallbackFN {
	/** 回调函数定义
	 * @param tableDef	操作所属表定义
	 * @param brec		如果是行操作，需要设置行操作的前镜像，没有前镜像或者不是行操作传入NULL
	 * @param arec		如果是行操作，需要设置行操作的后镜像，没有后镜像或者不是行操作传入NULL
	 * @param param		回调函数固定参数
	 */
	void (*callback)(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
};

/**
 * 回调函数管理器
 */

class NTSECallbackManager {
public:
	NTSECallbackManager() {}

	/** 注册一个指定类型调用的回调函数
	 * @param type	类型
	 * @param cbfn	回调函数
	 */
	void registerCallback(CallbackType type, NTSECallbackFN *cbfn) {
		m_cbFNs[type].push_back(cbfn);
	}

	/** 注销一个指定类型的回调函数
	 * @param type	类型
	 * @param cbfn	回调函数
	 * @return true表明有注销该函数，false为找不到
	 */
	bool unregisterCallback(CallbackType type, NTSECallbackFN *cbfn) {
		vector<NTSECallbackFN*> *callbacks = &m_cbFNs[type];
		vector<NTSECallbackFN*>::iterator itor = callbacks->begin();
		while (itor != callbacks->end()) {
			if ((*itor) == cbfn) {
				callbacks->erase(itor);
				return true;
			}
			++itor;
		}
		return false;
	}

	/** 调用执行所有指定类型的回调函数，如果函数为空，不调用
	 * @param type		类型
	 * @param tableDef	操作所属表定义
	 * @param brec		如果是行操作回调，需要传入行操作的前镜像，没有或者非行操作为NULL
	 * @param arec		如果是行操作回调，需要传入行操作的后镜像，没有或者非行操作为NULL
	 * @param param		回调函数固定参数
	 */
	void callback(CallbackType type, const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param) {
		for (uint i = 0; i < m_cbFNs[type].size(); i++)
			m_cbFNs[type][i]->callback(tableDef, brec, arec, param);
	}

private:
	vector<NTSECallbackFN*> m_cbFNs[CB_NUMBERS];	/** 各类回调函数的函数集合 */
};

}

#endif

