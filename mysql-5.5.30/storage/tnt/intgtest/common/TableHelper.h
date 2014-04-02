/**
 * 表相关的辅助功能
 * 
 * @author 余利华(yulihua@corp.netease.com, ylh@163.org)
 */
#ifndef _NTSETEST_TABLE_HELPER_H_
#define _NTSETEST_TABLE_HELPER_H_

#include "api/Database.h"
#include "api/Table.h"

using namespace ntse;

extern u64 tableScan(Database *db, Table *table, u16 numCols, u16 *columns);
extern u64 index0Scan(Database *db, Table *table, u16 numCols, u16 *columns, u64 maxRec = (u64)-1);
extern void indexFullScan(Database *db, Table *table);

#endif // _NTSETEST_TABLE_HELPER_H_
