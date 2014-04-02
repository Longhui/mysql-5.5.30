/**
 * �����������ܲ���
 *
 * @author �ձ�(bsu@corp.netease.com naturally@163.org)
 */

#ifndef _NTSETEST_INDEX_CREATION_H_
#define _NTSETEST_INDEX_CREATION_H_

#include "PerfTest.h"
#include "EmptyTestCase.h"
#include "misc/TableDef.h"
#include "btree/Index.h"

using namespace ntse;

namespace ntseperf {

class IndexCreationTest : public EmptyTestCase {
public:
	IndexCreationTest(u64 dataSize);

	string getName() const;
	string getDescription() const;

	virtual void loadData(u64 *totalRecSize, u64 *recCnt);
	virtual void warmUp();
	virtual void run();

private:
	IndexDef *newCountColIndexDefOfCountTable();

private:
	DrsIndice *m_indice;		/** ���Ӧ���������� */
	u64 m_dataSize;				/** �ⲿָ������������С */
};


}

#endif