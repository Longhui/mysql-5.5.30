/**
* DoubleCheck接口
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#ifndef _TNT_DOUBLE_CHECKER_H_
#define _TNT_DOUBLE_CHECKER_H_

#include "misc/Global.h"

using namespace ntse;

namespace tnt {
/** double check 接口 */
class DoubleChecker {
public:
	virtual ~DoubleChecker() {}
	/**
	 * 判断一个索引项的RowId版本是否已经过期
	 * @param rowId
	 * @param rowIdVersion
	 * @return 
	 */
	virtual bool isRowIdValid(RowId rowId, RowIdVersion rowIdVersion) const = 0;
};

}

#endif