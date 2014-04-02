/**
* 公共测试表:Paper
*
* @author zx(zx@163.org)
*/
#include "PaperTable.h"
#include "misc/RecordHelper.h"
#include <iostream>
#include <set>
#include <Random.h>

using namespace std;
using namespace ntse;


/**
 * 获取Paper表定义
 *
 * CREATE TABLE Paper (
 *   ID bigint PRIMARY KEY,
 *   Content mediumtext
 * );
 */

/**
 * 生成长度为len的大对象
 *
 * @param len
 */
byte* creatLob (uint len){
	char *abs = new char[len];
	for (uint i = 0; i < len; i++ ) {
		abs[i] = (char )('A' + System::random() % 10);
	}
	return (byte *)abs;
}

PaperTable PaperTable::m_inst;

PaperTable::PaperTable() {
	m_tableDef = makeTableDef();
}

PaperTable::~PaperTable() {
	if (m_tableDef)
		delete m_tableDef;
}

/**
 * 获取CountTable表定义
 *
 * @param useMms 不是使用MMS
 * @return 表定义
 */
const TableDef* PaperTable::getTableDef() {
	return m_inst.m_tableDef;
}


/**
 * 创建一条记录
 * @param id ID列
 * @param len content这个列（lob）的长度
 * @param table 表对象，因为大对象写需要表对象
 */
Record* PaperTable::createRecord(u64 id, uint len, Table *table) {
	RecordBuilder rb(PaperTable::getTableDef(), RID(0, 0), REC_REDUNDANT);
	rb.appendBigInt(id);
	Record *rec = rb.getRecord(PaperTable::getTableDef()->m_maxRecSize);
	rec->m_format = REC_MYSQL;
	RecordOper::writeLob(rec->m_data, table->getTableDef()->m_columns[1], creatLob(len));
	RecordOper::writeLobSize(rec->m_data, table->getTableDef()->m_columns[1], len);
	return rec;
}

/**
 * 随机构造总数为sizeCnt大小记录
 * @param session 会话对象
 * @param table 数据表
 * @param sizeCnt INOUT 输入为希望创建的记录总大小，输出为实际创建的记录总大小
 * @param len lob列的长度
 * @param recCount 记录数
 * @return 记录数组
 */
Record** PaperTable::populate(Session *session, Table *table, u64 *sizeCnt, uint len, u64 recCount) {
	UNREFERENCED_PARAMETER(session);
	u64 volumnCnt = *sizeCnt;
	Record **records = new Record *[(size_t)recCount];
	uint recCnt = 0;
	u64 recNum = recCount;
	while (recNum > 0) {
		// 创建记录
		records[recCnt] = createRecord((u64)recCnt, len, table);
		recCnt++;
		volumnCnt -= (sizeof (u64) + len);
		recNum--;
	}

	*sizeCnt -= volumnCnt;
	return records;
}

/**
 * 随机构造总数为sizeCnt大小的记录，这里主要为了每个测试线程准备一条记录
 * @param session 会话对象
 * @param table 表对象
 * @param len lob列的长度
 * @param recCount 记录数
 * @return 记录数组
 */
Record** PaperTable::populate(Session *session, Table *table, uint len, u64 recCount){
	UNREFERENCED_PARAMETER(session);
	Record **records = new Record *[(size_t)recCount];
	uint recCnt = 0;
	u64 recNum = recCount;
	while (recNum > 0) {
		// 创建记录
		records[recCnt] = createRecord((u64)recCnt, len, table);
		recCnt++;
		recNum--;
	}
	return records;
}

/**
 * 随机构造总数为dataSize大小的记录，并插入数据表中
 * @param session 会话对象
 * @param table 数据表
 * @param dataSize INOUT 输入为希望创建的记录总大小，输出为实际创建的记录总大小
 * @param len lob列的长度
 * @param isRand ID是否随机生成还是顺序生成
 * @return 记录数
 */
uint PaperTable::populate(Session *session, Table *table, u64 *dataSize, uint len, bool isRand) {
	Record *rec;
	u64 volumnCnt = *dataSize;
	uint recCnt = 0;
	uint dupIdx;
	RowId rid;
	uint outRecSize = len + sizeof(u64);
	if (!isRand) {
		while (true) {
			// 创建记录
			rec = createRecord((u64)recCnt, len, table);
			if (volumnCnt < outRecSize)
				break;
			// 插入记录
			rid = table->insert(session, rec->m_data, &dupIdx);
			if (rid != INVALID_ROW_ID) {
				recCnt++;
				volumnCnt -= outRecSize;
			}
			freeRecord(rec);
		}
	} 
	/*else {
		set<u64> idSet = new set<u64>;
		u64 recCount =  *dataSize / outRecSize;
		while (recCount > 0) {
			u64 newId = (u64)RandomGen::nextInt();
			while (idSet.end() == idSet.find(newId)) {
				newId = RandomGen::nextInt();
			}
			idSet.insert(newId);
			rec = createRecord((u64)newId, len, table);
			if (volumnCnt < outRecSize)
				break;
			 插入记录
			rid = table->insert(session, rec->m_data, &dupIdx);
			if (rid != INVALID_ROW_ID) {
				recCnt++;
				volumnCnt -= outRecSize;
				recCount--;
			}
			freeRecord(rec);
		 
	}*/
	*dataSize -= volumnCnt;
	return recCnt;

}
/**
* 更新记录的ID列
* @param rec 待更新记录
* @param id 新id
*/
void PaperTable::updateId(Record *rec, u64 newid) {
	//assert(rec->m_format == REC_REDUNDANT);
	const TableDef *tableDef = getTableDef();
	ColumnDef *columnDef = tableDef->m_columns[0];
	*(u64 *)(rec->m_data + columnDef->m_offset) = newid;
}

/**
 *  生成表定义
 */
TableDef* PaperTable::makeTableDef() {
	TableDefBuilder *builder = new TableDefBuilder(TableDef::INVALID_TABLEID, "perfTest", "PaperUseMms");
	builder->addColumn(PAPER_ID, CT_BIGINT, false)->addColumn(PAPER_CONTENT, CT_MEDIUMLOB);
	builder->addIndex("PRIMARY", true, true, PAPER_ID, NULL);
	TableDef *tableDef = builder->getTableDef();
	tableDef->m_useMms = true;
	delete builder;
	return tableDef;
}

