#ifndef _TNTTEST_TABLE_HELPER_H_
#define _TNTTEST_TABLE_HELPER_H_

#include "api/TNTDatabase.h"
#include "api/TNTTable.h"

using namespace tnt;

extern u64 TNTTableScan(TNTDatabase *db, TNTTable *table, u16 numCols, u16 *columns);
extern u64 TNTIndex0Scan(TNTDatabase *db, TNTTable *table, u16 numCols, u16 *columns, u64 maxRec = (u64)-1);
extern void TNTIndexFullScan(TNTDatabase *db, TNTTable *table, TNTOpInfo *opInfo);
extern TNTTransaction *startTrx(TNTTrxSys *trxSys, Session *session, bool log = false);
extern void commitTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
extern void rollBackTrx(TNTTrxSys *trxSys, TNTTransaction *trx);
extern void initTNTOpInfo(TNTOpInfo *opInfo, TLockMode mode);

#endif
