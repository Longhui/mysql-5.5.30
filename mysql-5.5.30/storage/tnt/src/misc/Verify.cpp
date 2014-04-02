/**
 * 扩展验证配置
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#include "misc/Verify.h"

namespace ntse {

/** 全局唯一的扩展验证配置 */
VerifySetting vs;

VerifySetting::VerifySetting() {
	memset(this, 0, sizeof(VerifySetting));
	//buf = true;
	//hp = true;
	log = true;
	//mheap = true;
}

}
