/**
* DoubleCheck�ӿ�
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#ifndef _TNT_DOUBLE_CHECKER_H_
#define _TNT_DOUBLE_CHECKER_H_

#include "misc/Global.h"

using namespace ntse;

namespace tnt {
/** double check �ӿ� */
class DoubleChecker {
public:
	virtual ~DoubleChecker() {}
	/**
	 * �ж�һ���������RowId�汾�Ƿ��Ѿ�����
	 * @param rowId
	 * @param rowIdVersion
	 * @return 
	 */
	virtual bool isRowIdValid(RowId rowId, RowIdVersion rowIdVersion) const = 0;
};

}

#endif