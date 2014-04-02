/**
 * ��չ��֤����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_VERIFY_H_
#define _NTSE_VERIFY_H_

#include "util/Portable.h"
#include "misc/Global.h"
#include <memory.h>

namespace ntse {

/** ������չ��֤�����Ƿ��������� */
struct VerifySetting {
public:
	VerifySetting();

public:
	bool	buf;	  /** �Ƿ�����ҳ�滺��ģ�����չ��֤���� */
	bool	hp;		  /** �Ƿ����ö�ģ�����չ��֤���� */
	bool	hpheavy;  /** �Ƿ����ö�ģ�����ر��ʱ����֤���� */
	bool	idx;	  /** �Ƿ���������ģ�����չ��֤���� */
	bool	tbl;	  /** �Ƿ����ñ�ģ�����չ��֤���� */
	bool	lob;	  /** �Ƿ����ô����ģ�����չ��֤���� */
	bool	mms;	  /** �Ƿ�����MMSģ�����չ��֤���� */
	bool	pool;	  /** �Ƿ������ڴ�ҳ��ģ�����չ��֤���� */
	bool	lsn;	  /** �Ƿ�����redoʱҳ��lsn��� */
	bool    compress; /** �Ƿ�������¼ѹ��ģ�����չ��֤���� */
	bool	log;	/** �Ƿ���־ģ�����չ��֤���� */
	bool    mheap;    /** �Ƿ������ڴ�Ѽ�¼��֤����*/
};

extern VerifySetting vs;

#ifdef NTSE_VERIFY_EX
/**
 * ������չ��֤�ĺꡣ��aspectΪtrueʱ��֤exprΪ��
 *
 * @param aspect ����vs.member��ָ����֤�������������
 * @param expr ��aspectΪtrueʱҪ��֤�ı��ʽ
 */
#define verify_ex(aspect, expr)		\
	do {							\
		if (aspect) {				\
			NTSE_ASSERT(expr);		\
		}							\
	} while(0)

/**
 * ֻ�ڿ�������չ��֤ʱִ�еĴ��롣
 *
 * @param aspect ����vs.member��ָ���������������
 * @param block ��aspectΪtrueʱҪִ�е����
 */
#define vecode(aspect, block)		\
	do {							\
		if (aspect) {				\
			block;					\
		}							\
	} while(0)
#else
#define verify_ex(aspect, expr)
#define vecode(aspect, block)
#endif
}

#endif
