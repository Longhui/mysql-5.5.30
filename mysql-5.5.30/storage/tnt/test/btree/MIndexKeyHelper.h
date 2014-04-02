#ifndef _NTSETEST_MINDEX_KEY_HELPER_H_
#define _NTSETEST_MINDEX_KEY_HELPER_H_

#include "btree/IndexPage.h"
#include "btree/MIndexKey.h"
#include "btree/MIndexPage.h"
#include "util/PagePool.h"
#include "misc/TNTIMPageManager.h"
#include "misc/MemCtx.h"
#include "misc/TableDef.h"
#include "misc/RecordHelper.h"

using namespace tnt;
using namespace ntse;

class MIndexKeyHelper {
public:
	MIndexKeyHelper() : m_schemaName("TestDB"), m_tableName("SimpleTable"), m_currentId(0), 
		m_currentRowId(1), m_tableDef(NULL), m_indexDef(NULL) {
	}
	~MIndexKeyHelper() {
		if (m_tableDef)
			delete m_tableDef;
	}

	SubRecord* createKey(MemoryContext *mtx, RowId rowId, u64 idValue, RecFormat format = KEY_NATURAL) {
		SubRecordBuilder srBuilder(m_tableDef, format, rowId);
		SubRecord *key = srBuilder.createSubRecordByName("C1", &idValue);
		SubRecord *rtn = MIndexKeyOper::allocSubRecord(mtx, key, m_indexDef);
		rtn->m_numCols = m_indexDef->m_numCols;
		rtn->m_columns = m_indexDef->m_columns;
		freeSubRecord(key);
		return rtn;
	}

	SubRecord* createVarcharKey(MemoryContext *mtx, RowId rowId, char* value, RecFormat format = KEY_NATURAL) {
		SubRecordBuilder srBuilder(m_tableDef, format, rowId);
		SubRecord *key = srBuilder.createSubRecordByName("Title", value);
		SubRecord *rtn = MIndexKeyOper::allocSubRecord(mtx, key, m_indexDef);
		rtn->m_numCols = m_indexDef->m_numCols;
		rtn->m_columns = m_indexDef->m_columns;
		freeSubRecord(key);
		return rtn;
	}

	SubRecord* createNatualKeyByOrder(MemoryContext *mtx) {
		return createKey(mtx, m_currentRowId++, m_currentId++);
	}

	void init(bool isUnique = false) {
		assert(!m_tableDef);
		assert(!m_indexDef);
		TableDefBuilder builder(1, m_schemaName, m_tableName);
		
		builder.addColumn("C1", CT_BIGINT, false);
		builder.addIndex("INDEX1", false, isUnique, false, "C1", 0, NULL);

		m_tableDef = builder.getTableDef();
		m_indexDef = m_tableDef->m_indice[0];
	}

	void initVarchar(bool isUnique = false) {
		assert(!m_tableDef);
		assert(!m_indexDef);
		TableDefBuilder builder(1, m_schemaName, m_tableName);
		builder.addColumnS("Title", CT_VARCHAR, 20, true, false, COLL_UTF8);
		builder.addIndex("INDEX1", false, isUnique, false, "Title", 0, NULL);
		m_tableDef = builder.getTableDef();
		m_indexDef = m_tableDef->m_indice[0];
	}

	TableDef* getTableDef() const {
		return m_tableDef;
	}

	const IndexDef* getIndexDef() const {
		return m_indexDef;
	}

	const char *getSchemaName() const {
		return m_schemaName;
	}

	const char *getTableName() const {
		return m_tableName;
	}

private:
	const char *m_schemaName;
	const char *m_tableName;
	u64      m_currentId;
	RowId    m_currentRowId;
	TableDef *m_tableDef;
	IndexDef *m_indexDef;
};

#endif