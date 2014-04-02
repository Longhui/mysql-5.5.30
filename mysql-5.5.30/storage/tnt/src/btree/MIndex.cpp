/**
* �ڴ������ӿ�
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/
#include "btree/MIndex.h"
#include "btree/IndexBLinkTree.h"
#include "btree/IndexBLinkTreeManager.h"

namespace tnt {

/**
 * ���ڴ���������
 * @param db        �������ݿ�
 * @param session   �Ự
 * @param indexNum  �����������Ŀ
 * @param tableDef  ����
 * @param lobStorage ����������
 * @param doubleChecker RowIdУ����
 * @return 
 */
MIndice* MIndice::open(TNTDatabase *db, Session *session, uint indexNum, TableDef **tableDef, LobStorage *lobStorage, 
					   const DoubleChecker *doubleChecker) {
	//FIXME: BLinkTreeIndice���캯����������
	MIndice *mIndice = new BLinkTreeIndice(db, session, indexNum, tableDef, lobStorage, doubleChecker);
	if(!mIndice->init(session)) {
		delete mIndice;
		mIndice = NULL;
	}
	return mIndice;
}

/**
 * ���ڴ�����
 * @param db       �������ݿ�
 * @param session  �Ự
 * @param mIndice  �����ڴ���������
 * @param tableDef ����
 * @param indexDef ��������
 * @param indexId  ����ID
 * @param doubleChecker RowIdУ����
 * @return 
 */
MIndex* MIndex::open(TNTDatabase *db, Session *session, MIndice *mIndice, TableDef **tableDef, 
					 const IndexDef *indexDef, u8 indexId, const DoubleChecker *doubleChecker) {
	//FIXME: BLinkTree���캯����������
	MIndex *mIndex = new BLinkTree(session, mIndice, db->getTNTIMPageManager(), doubleChecker, tableDef, 
							 indexDef, indexId);
	if(!mIndex->init(session, true)) {
		delete mIndex;
		mIndex = NULL;
	}
	return mIndex;
}

}
