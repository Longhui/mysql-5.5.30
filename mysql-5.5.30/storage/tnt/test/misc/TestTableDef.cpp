/**
 * 测试表定义
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#include "misc/TestTableDef.h"

const char* TableDefTestCase::getName() {
	return "Table define test";
}

const char* TableDefTestCase::getDescription() {
	return "Functional test for table define.";
}

bool TableDefTestCase::isBig() {
	return false;
}

void TableDefTestCase::setUp() {
	m_tableDef = TableTestCase::getBlogDef(true);
	m_tableDef->m_id = TableDef::MIN_NORMAL_TABLEID;
}

void TableDefTestCase::tearDown() {
	if (m_tableDef) {
		delete m_tableDef;
		m_tableDef = NULL;
	}
}

void TableDefTestCase::testEqual() {
	TableDef *ano = new TableDef(m_tableDef);
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	char * oldName = ano->m_schemaName;
	ano->m_schemaName = "test modified schema name";
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_schemaName = oldName;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	oldName = ano->m_name;
	ano->m_name = "test modified name";
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_name = oldName;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u16 oldId = ano->m_id;
	ano->m_id += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_id = oldId;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u16 oldNumCols = ano->m_numCols;
	ano->m_numCols += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_numCols = oldNumCols;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u16 oldNullCols = ano->m_nullableCols;
	ano->m_nullableCols += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_nullableCols = oldNullCols;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u8 oldNumIndice = ano->m_numIndice;
	ano->m_numIndice += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_numIndice = oldNumIndice;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	//test index
	exchangeIndex(ano, 0, 1);
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	exchangeIndex(ano, 1, 0);
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	//test column
	ColumnDef *col = ano->m_columns[0];
	char *oldColName = col->m_name;
	col->m_name = "modifiedColumnName";
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	col->m_name = oldColName;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u16 oldMaxRecSize = ano->m_maxRecSize;
	ano->m_maxRecSize += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_maxRecSize = oldMaxRecSize;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u16 oldIncrSize = ano->m_incrSize;
	ano->m_incrSize += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_incrSize = oldIncrSize;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	CPPUNIT_ASSERT(ano->m_recFormat == REC_VARLEN);
	RecFormat oldRecFormat = ano->m_recFormat;
	ano->m_recFormat = REC_FIXLEN;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_recFormat = oldRecFormat;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	bool oldUseMms = ano->m_useMms;
	ano->m_useMms = !ano->m_useMms;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_useMms = oldUseMms;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	bool oldCacheUpd = ano->m_cacheUpdate;
	ano->m_cacheUpdate = !ano->m_cacheUpdate;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_cacheUpdate = oldCacheUpd;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	bool oldCprsLobs = ano->m_compressLobs;
	ano->m_compressLobs = !ano->m_compressLobs;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_compressLobs = oldCprsLobs;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	bool oldIndexOnly = ano->m_indexOnly;
	ano->m_indexOnly = !ano->m_indexOnly;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_indexOnly = oldIndexOnly;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u16 oldUpdateCacheTime = ano->m_updateCacheTime;
	ano->m_updateCacheTime += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_updateCacheTime = oldUpdateCacheTime;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	u16 pctFree = ano->m_pctFree;
	ano->m_pctFree += 1;
	CPPUNIT_ASSERT(!(*ano == *m_tableDef));
	ano->m_pctFree = pctFree;
	CPPUNIT_ASSERT(*ano == *m_tableDef);

	delete ano;
	ano = NULL;
}

void TableDefTestCase::compareColumn(ColumnDef *columnDef1, ColumnDef *columnDef2) {
	CPPUNIT_ASSERT(strcmp(columnDef1->m_name, columnDef2->m_name) == 0);
	CPPUNIT_ASSERT(columnDef1->m_type == columnDef2->m_type);
	CPPUNIT_ASSERT(columnDef1->m_collation == columnDef2->m_collation);
	CPPUNIT_ASSERT(columnDef1->m_size == columnDef2->m_size);
	CPPUNIT_ASSERT(columnDef1->m_nullable == columnDef2->m_nullable);
	CPPUNIT_ASSERT(columnDef1->m_cacheUpdate == columnDef2->m_cacheUpdate);
	CPPUNIT_ASSERT(columnDef1->m_prtype.m_flags == columnDef2->m_prtype.m_flags);
	CPPUNIT_ASSERT(columnDef1->m_prtype.m_deicmal == columnDef2->m_prtype.m_deicmal);
	CPPUNIT_ASSERT(columnDef1->m_prtype.m_precision == columnDef2->m_prtype.m_precision);
}

void TableDefTestCase::testColumnWrite() {
	byte *buf = NULL;
	u32 size = 0;
	ColumnDef *tempColumnDef = NULL;

	ColumnDef *columnDef = m_tableDef->getColumnDef("ID");
	CPPUNIT_ASSERT(columnDef != NULL);
	columnDef->writeToKV(&buf, &size);
	tempColumnDef = new ColumnDef();
	tempColumnDef->readFromKV(buf, size);
	compareColumn(tempColumnDef, columnDef);
	delete[] buf;
	delete tempColumnDef;

	columnDef = m_tableDef->getColumnDef("UserID");
	CPPUNIT_ASSERT(columnDef != NULL);
	columnDef->writeToKV(&buf, &size);
	tempColumnDef = new ColumnDef();
	tempColumnDef->readFromKV(buf, size);
	compareColumn(tempColumnDef, columnDef);
	delete[] buf;
	delete tempColumnDef;

	columnDef = m_tableDef->getColumnDef("PublishTime");
	CPPUNIT_ASSERT(columnDef != NULL);
	columnDef->writeToKV(&buf, &size);
	tempColumnDef = new ColumnDef();
	tempColumnDef->readFromKV(buf, size);
	compareColumn(tempColumnDef, columnDef);
	delete[] buf;
	delete tempColumnDef;

	columnDef = m_tableDef->getColumnDef("Title");
	CPPUNIT_ASSERT(columnDef != NULL);
	columnDef->writeToKV(&buf, &size);
	tempColumnDef = new ColumnDef();
	tempColumnDef->readFromKV(buf, size);
	compareColumn(tempColumnDef, columnDef);
	delete[] buf;
	delete tempColumnDef;

	columnDef = m_tableDef->getColumnDef("Tags");
	CPPUNIT_ASSERT(columnDef != NULL);
	columnDef->writeToKV(&buf, &size);
	tempColumnDef = new ColumnDef();
	tempColumnDef->readFromKV(buf, size);
	compareColumn(tempColumnDef, columnDef);
	delete[] buf;
	delete tempColumnDef;

	columnDef = m_tableDef->getColumnDef("Abstract");
	CPPUNIT_ASSERT(columnDef != NULL);
	columnDef->writeToKV(&buf, &size);
	tempColumnDef = new ColumnDef();
	tempColumnDef->readFromKV(buf, size);
	compareColumn(tempColumnDef, columnDef);
	delete[] buf;
	delete tempColumnDef;

	columnDef = m_tableDef->getColumnDef("Content");
	CPPUNIT_ASSERT(columnDef != NULL);
	columnDef->writeToKV(&buf, &size);
	tempColumnDef = new ColumnDef();
	tempColumnDef->readFromKV(buf, size);
	compareColumn(tempColumnDef, columnDef);
	delete[] buf;
	delete tempColumnDef;
}

void TableDefTestCase::testColumnRead() {
	char *def = "name:name;type:10;collation:3;size:20;null_able:true;cache_update:true;pr_flags:1";
	u32 size = strlen(def);
	ColumnDef *columnDef = new ColumnDef();
	columnDef->readFromKV((byte *)def, size);
	CPPUNIT_ASSERT(strcmp(columnDef->m_name, "name") == 0);
	CPPUNIT_ASSERT(columnDef->m_type == CT_VARCHAR);
	CPPUNIT_ASSERT(columnDef->m_collation == COLL_UTF8);
	CPPUNIT_ASSERT(columnDef->m_size == 20);
	CPPUNIT_ASSERT(columnDef->m_nullable);
	CPPUNIT_ASSERT(columnDef->m_cacheUpdate);
	CPPUNIT_ASSERT(columnDef->m_prtype.m_flags == 1);
	CPPUNIT_ASSERT(columnDef->m_prtype.m_precision == ColumnDef::PRPRECISION_DEFAULT);
	CPPUNIT_ASSERT(columnDef->m_prtype.m_deicmal == ColumnDef::PRDECIMAL_DEFAULT);
	delete columnDef;

	def = "name:id;type:4;size:8;null_able:false;cache_update:true;pr_flags:1;pr_precision:20";
	size = strlen(def);
	columnDef = new ColumnDef();
	columnDef->readFromKV((byte *)def, size);
	CPPUNIT_ASSERT(strcmp(columnDef->m_name, "id") == 0);
	CPPUNIT_ASSERT(columnDef->m_type == CT_BIGINT);
	CPPUNIT_ASSERT(columnDef->m_collation == COLL_BIN);
	CPPUNIT_ASSERT(columnDef->m_size == 8);
	CPPUNIT_ASSERT(!columnDef->m_nullable);
	CPPUNIT_ASSERT(columnDef->m_cacheUpdate);
	CPPUNIT_ASSERT(columnDef->m_prtype.m_flags == 1);
	CPPUNIT_ASSERT(columnDef->m_prtype.m_precision == ColumnDef::PRPRECISION_DEFAULT);
	CPPUNIT_ASSERT(columnDef->m_prtype.m_deicmal == ColumnDef::PRDECIMAL_DEFAULT);
	delete columnDef;
}

void TableDefTestCase::testIndexWrite() {
	byte *buf = NULL;
	u32 size = 0;
	IndexDef *tempIndexDef = NULL;

	IndexDef *indexDef = m_tableDef->getIndexDef("PRIMARY");
	CPPUNIT_ASSERT(indexDef != NULL);
	indexDef->writeToKV(&buf, &size);
	tempIndexDef = new IndexDef();
	tempIndexDef->readFromKV(buf, size);
	CPPUNIT_ASSERT(*tempIndexDef == *indexDef);
	delete[] buf;
	delete tempIndexDef;
	
	indexDef = m_tableDef->getIndexDef("IDX_BLOG_PUTTIME");
	CPPUNIT_ASSERT(indexDef != NULL);
	indexDef->writeToKV(&buf, &size);
	tempIndexDef = new IndexDef();
	tempIndexDef->readFromKV(buf, size);
	CPPUNIT_ASSERT(*tempIndexDef == *indexDef);
	delete[] buf;
	delete tempIndexDef;

	indexDef = m_tableDef->getIndexDef("IDX_BLOG_PUBTIME");
	CPPUNIT_ASSERT(indexDef != NULL);
	indexDef->writeToKV(&buf, &size);
	tempIndexDef = new IndexDef();
	tempIndexDef->readFromKV(buf, size);
	CPPUNIT_ASSERT(*tempIndexDef == *indexDef);
	delete[] buf;
	delete tempIndexDef;
}

void TableDefTestCase::testIndexRead() {
	char *def = "name:idx_name_grade;primary_key:false;unique:false;max_key_size:10;num_cols:2;bm_bytes:2;split_factor:20;columns:1,2;offsets:0,10;prefixs:0,0";
	u32 size = strlen(def);
	IndexDef *indexDef = new IndexDef();
	indexDef->readFromKV((byte *)def, size);
	CPPUNIT_ASSERT(strcmp(indexDef->m_name, "idx_name_grade") == 0);
	CPPUNIT_ASSERT(!indexDef->m_primaryKey);
	CPPUNIT_ASSERT(!indexDef->m_unique);
	CPPUNIT_ASSERT(indexDef->m_maxKeySize == 10);
	CPPUNIT_ASSERT(indexDef->m_numCols == 2);
	CPPUNIT_ASSERT(indexDef->m_bmBytes == 2);
	CPPUNIT_ASSERT(indexDef->m_splitFactor == 20);
	CPPUNIT_ASSERT(indexDef->m_columns[0] == 1);
	CPPUNIT_ASSERT(indexDef->m_columns[1] == 2);
	CPPUNIT_ASSERT(indexDef->m_offsets[0] == 0);
	CPPUNIT_ASSERT(indexDef->m_offsets[1] == 10);
	delete indexDef;

	def = "name:idx_name_sex;primary_key:true;max_key_size:14;num_cols:2;bm_bytes:2;columns:1,5;offsets:0,10;prefixs:0,0";
	size = strlen(def);
	indexDef = new IndexDef();
	indexDef->readFromKV((byte *)def, size);
	CPPUNIT_ASSERT(strcmp(indexDef->m_name, "idx_name_sex") == 0);
	CPPUNIT_ASSERT(indexDef->m_primaryKey);
	CPPUNIT_ASSERT(indexDef->m_unique == IndexDef::UNIQUE_DEFAULT);
	CPPUNIT_ASSERT(indexDef->m_maxKeySize == 14);
	CPPUNIT_ASSERT(indexDef->m_numCols == 2);
	CPPUNIT_ASSERT(indexDef->m_bmBytes == 2);
	CPPUNIT_ASSERT(indexDef->m_splitFactor == IndexDef::SPLITFACTOR_DEFAULT);
	CPPUNIT_ASSERT(indexDef->m_columns[0] == 1);
	CPPUNIT_ASSERT(indexDef->m_columns[1] == 5);
	CPPUNIT_ASSERT(indexDef->m_offsets[0] == 0);
	CPPUNIT_ASSERT(indexDef->m_offsets[1] == 10);
	delete indexDef;
}

void TableDefTestCase::testTableDefWrite() {
	byte *buf = NULL;
	u32 size = 0;
	TableDef *tblDef = new TableDef();

	m_tableDef->write(&buf, &size);
	tblDef->read(buf, size);
	CPPUNIT_ASSERT(*m_tableDef == *tblDef);
	delete[] buf;
	delete tblDef;
}

void TableDefTestCase::testTableDefRead() {
	char *def = "id:1001;schema_name:user_schema;name:user;num_cols:5;nullable_cols:2;num_indice:2;rec_format:1;max_recsize:60;use_mms:true;"\
				"cache_update:true;update_cache_time:2000;compress_lobs:true;bm_bytes:1;pct_free:20;incr_size:512;index_only:false;"\
				"column:{name:id;type:4;size:8;null_able:false;cache_update:false;pr_flags:1;pr_precision:20},"\
				"{name:name;type:10;collation:3;size:20;null_able:false;cache_update:false;pr_flags:1},"\
				"{name:sex;type:1;size:1;null_able:true;cache_update:false;pr_flags:1},"\
				"{name:grade;type:7;size:5;null_able:false;cache_update:false;pr_flags:1;pr_precision:3;pr_decimal:2},"\
				"{name:description;type:10;collation:3;size:100;null_able:true;cache_update:true};"\
				"index:{name:idx_name_grade;primary_key:true;unique:false;max_key_size:10;num_cols:2;bm_bytes:2;split_factor:20;columns:1,3;offsets:0,10;prefixs:0,0},"\
				"{name:idx_name_sex;primary_key:false;unique:false;max_key_size:25;num_cols:2;bm_bytes:2;split_factor:30;columns:1,2;offsets:0,15;prefixs:0,0};";
	u32 size = strlen(def);
	TableDef *tableDef = new TableDef();
	tableDef->read((byte *)def, size);
	CPPUNIT_ASSERT(tableDef->m_id == 1001);
	CPPUNIT_ASSERT(strcmp(tableDef->m_schemaName, "user_schema") == 0);
	CPPUNIT_ASSERT(strcmp(tableDef->m_name, "user") == 0);
	CPPUNIT_ASSERT(tableDef->m_numCols == 5);
	CPPUNIT_ASSERT(tableDef->m_nullableCols == 2);
	CPPUNIT_ASSERT(tableDef->m_numIndice == 2);
	CPPUNIT_ASSERT(tableDef->m_recFormat == REC_VARLEN);
	CPPUNIT_ASSERT(tableDef->m_maxRecSize == 60);
	CPPUNIT_ASSERT(tableDef->m_useMms);
	CPPUNIT_ASSERT(tableDef->m_cacheUpdate);
	CPPUNIT_ASSERT(tableDef->m_updateCacheTime == 2000);
	CPPUNIT_ASSERT(tableDef->m_compressLobs);
	CPPUNIT_ASSERT(tableDef->m_bmBytes == 1);
	CPPUNIT_ASSERT(tableDef->m_pctFree == 20);
	CPPUNIT_ASSERT(tableDef->m_incrSize == 512);
	CPPUNIT_ASSERT(!tableDef->m_indexOnly);

	ColumnDef **columnDef = tableDef->m_columns;
	CPPUNIT_ASSERT(strcmp("id", columnDef[0]->m_name) == 0);
	CPPUNIT_ASSERT(columnDef[0]->m_type == CT_BIGINT);
	CPPUNIT_ASSERT(columnDef[0]->m_collation == COLL_BIN);
	CPPUNIT_ASSERT(columnDef[0]->m_size == 8);
	CPPUNIT_ASSERT(!columnDef[0]->m_nullable);
	CPPUNIT_ASSERT(!columnDef[0]->m_cacheUpdate);
	CPPUNIT_ASSERT(columnDef[0]->m_prtype.m_flags == 1);
	CPPUNIT_ASSERT(columnDef[0]->m_prtype.m_precision == ColumnDef::PRPRECISION_DEFAULT);
	CPPUNIT_ASSERT(columnDef[0]->m_prtype.m_deicmal == ColumnDef::PRDECIMAL_DEFAULT);

	CPPUNIT_ASSERT(strcmp("grade", columnDef[3]->m_name) == 0);
	CPPUNIT_ASSERT(columnDef[3]->m_type == CT_DECIMAL);
	CPPUNIT_ASSERT(columnDef[3]->m_collation == COLL_BIN);
	CPPUNIT_ASSERT(columnDef[3]->m_size == 5);
	CPPUNIT_ASSERT(!columnDef[3]->m_nullable);
	CPPUNIT_ASSERT(!columnDef[3]->m_cacheUpdate);
	CPPUNIT_ASSERT(columnDef[3]->m_prtype.m_flags == 1);
	CPPUNIT_ASSERT(columnDef[3]->m_prtype.m_precision == 3);
	CPPUNIT_ASSERT(columnDef[3]->m_prtype.m_deicmal == 2);

	CPPUNIT_ASSERT(strcmp("description", columnDef[4]->m_name) == 0);
	CPPUNIT_ASSERT(columnDef[4]->m_type == CT_VARCHAR);
	CPPUNIT_ASSERT(columnDef[4]->m_collation == COLL_UTF8);
	CPPUNIT_ASSERT(columnDef[4]->m_size == 100);
	CPPUNIT_ASSERT(columnDef[4]->m_nullable);
	CPPUNIT_ASSERT(columnDef[4]->m_cacheUpdate);
	CPPUNIT_ASSERT(columnDef[4]->m_prtype.m_flags == ColumnDef::PRFLAGS_DEFAULT);

	IndexDef **indexDef = tableDef->m_indice;
	CPPUNIT_ASSERT(strcmp(indexDef[0]->m_name, "idx_name_grade") == 0);
	CPPUNIT_ASSERT(indexDef[0]->m_primaryKey);
	CPPUNIT_ASSERT(!indexDef[0]->m_unique);
	CPPUNIT_ASSERT(indexDef[0]->m_maxKeySize == 10);
	CPPUNIT_ASSERT(indexDef[0]->m_numCols == 2);
	CPPUNIT_ASSERT(indexDef[0]->m_bmBytes == 2);
	CPPUNIT_ASSERT(indexDef[0]->m_splitFactor == 20);
	CPPUNIT_ASSERT(indexDef[0]->m_columns[0] == 1);
	CPPUNIT_ASSERT(indexDef[0]->m_columns[1] == 3);
	CPPUNIT_ASSERT(indexDef[0]->m_offsets[0] == 0);
	CPPUNIT_ASSERT(indexDef[0]->m_offsets[1] == 10);

	CPPUNIT_ASSERT(strcmp(indexDef[1]->m_name, "idx_name_sex") == 0);
	CPPUNIT_ASSERT(!indexDef[1]->m_primaryKey);
	CPPUNIT_ASSERT(!indexDef[1]->m_unique);
	CPPUNIT_ASSERT(indexDef[1]->m_maxKeySize == 25);
	CPPUNIT_ASSERT(indexDef[1]->m_numCols == 2);
	CPPUNIT_ASSERT(indexDef[1]->m_bmBytes == 2);
	CPPUNIT_ASSERT(indexDef[1]->m_splitFactor == 30);
	CPPUNIT_ASSERT(indexDef[1]->m_columns[0] == 1);
	CPPUNIT_ASSERT(indexDef[1]->m_columns[1] == 2);
	CPPUNIT_ASSERT(indexDef[1]->m_offsets[0] == 0);
	CPPUNIT_ASSERT(indexDef[1]->m_offsets[1] == 15);

	delete tableDef;
}

void TableDefTestCase::exchangeIndex(TableDef *tblDef, u16 first, u16 second) {
	IndexDef *tmp = tblDef->m_indice[first];
	tblDef->m_indice[first] = tblDef->m_indice[second];
	tblDef->m_indice[second] = tmp;
}