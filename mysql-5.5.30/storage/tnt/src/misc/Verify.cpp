/**
 * ��չ��֤����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#include "misc/Verify.h"

namespace ntse {

/** ȫ��Ψһ����չ��֤���� */
VerifySetting vs;

VerifySetting::VerifySetting() {
	memset(this, 0, sizeof(VerifySetting));
	//buf = true;
	//hp = true;
	log = true;
	//mheap = true;
}

}
