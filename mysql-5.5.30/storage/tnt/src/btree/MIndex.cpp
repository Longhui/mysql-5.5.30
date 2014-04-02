/**
* 内存索引接口
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "btree/MIndex.h"
#include "btree/IndexBLinkTree.h"
#include "btree/IndexBLinkTreeManager.h"

namespace tnt {

/**
 * 打开内存索引管理
 * @param db        所属数据库
 * @param session   会话
 * @param indexNum  表的总索引数目
 * @param tableDef  表定义
 * @param lobStorage 大对象管理器
 * @param doubleChecker RowId校验器
 * @return 
 */
MIndice* MIndice::open(TNTDatabase *db, Session *session, uint indexNum, TableDef **tableDef, LobStorage *lobStorage, 
					   const DoubleChecker *doubleChecker) {
	//FIXME: BLinkTreeIndice构造函数参数修正
	MIndice *mIndice = new BLinkTreeIndice(db, session, indexNum, tableDef, lobStorage, doubleChecker);
	if(!mIndice->init(session)) {
		delete mIndice;
		mIndice = NULL;
	}
	return mIndice;
}

/**
 * 打开内存索引
 * @param db       所属数据库
 * @param session  会话
 * @param mIndice  所属内存索引管理
 * @param tableDef 表定义
 * @param indexDef 索引定义
 * @param indexId  索引ID
 * @param doubleChecker RowId校验器
 * @return 
 */
MIndex* MIndex::open(TNTDatabase *db, Session *session, MIndice *mIndice, TableDef **tableDef, 
					 const IndexDef *indexDef, u8 indexId, const DoubleChecker *doubleChecker) {
	//FIXME: BLinkTree构造函数参数修正
	MIndex *mIndex = new BLinkTree(session, mIndice, db->getTNTIMPageManager(), doubleChecker, tableDef, 
							 indexDef, indexId);
	if(!mIndex->init(session, true)) {
		delete mIndex;
		mIndex = NULL;
	}
	return mIndex;
}

}
