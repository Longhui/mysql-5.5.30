/**
* ������space used rate
*
* @author zx(zx@163.org)
*/


#ifndef _NTSETEST_LOB_SPACE_USE_H_
#define _NTSETEST_LOB_SPACE_USE_H_

#include "iostream"
#include "PerfTest.h"
#include "util/Thread.h"
#include "misc/Session.h"
#include "EmptyTestCase.h"

using namespace ntse;
using namespace ntseperf;

class LobSpaceTest: public EmptyTestCase {
public:
	LobSpaceTest(bool useMms, u64 dataSize, bool isCompress, ostream *os);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();
	virtual void tearDown();

private: 
	void getFilesSpace();

private:

	bool m_useMms;				/** ���Ƿ�ʹ��Mms */
	u64 m_dataSize;				/** m_dataSize�Ƿ���С������*/
	bool m_isCompress;			/** �Ƿ�ѹ�� */
	ostream  *m_os;             /** ��������ռ������ʵ� */
};



#endif 
