#ifndef _TNT_TABLE_MAINTAIN
#define _TNT_TABLE_MAINTAIN

#include "api/TblMnt.h"
#include "api/TNTTable.h"

namespace tnt {
class TNTTblMntAlterIndex: public ntse::TblMntAlterIndex {
public:
	TNTTblMntAlterIndex(TNTTable *table, const u16 numAddIdx, const IndexDef **addIndice, 
		const u16 numDelIdx, const IndexDef **delIndice, bool *cancelFlag = NULL);
protected:
	void additionalAlterIndex(Session *session, TableDef *oldTblDef, TableDef **newTblDef, DrsIndice *drsIndice,
		const IndexDef **addIndice, u16 numAddIdx, bool *idxDeleted);
	void reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict = false);

	void lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void unlockMeta(Session *session, ILMode mode);

	void enableLogging(Session *session);
	void disableLogging(Session *session);
private:
	TNTTable *m_tntTable;
};

class TNTTblMntAlterColumn: public ntse::TblMntAlterColumn {
public:
	TNTTblMntAlterColumn(TNTTable *table, Connection *conn, u16 addColNum, const AddColumnDef *addCol, 
		u16 delColNum, const ColumnDef **delCol, bool *cancelFlag = NULL, bool keepOldDict = false);
protected:
	void preLockTable();
	void additionalAlterColumn(Session *session, NtseRidMapping *ridmap);
	void reopenTblAndReplaceComponent(Session *session, const char *origTablePath, bool hasCprsDict = false);

	void lockMeta(Session *session, ILMode mode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void upgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, int timeoutMs, const char *file, uint line) throw(NtseException);
	void downgradeMetaLock(Session *session, ILMode oldMode, ILMode newMode, const char *file, uint line);
	void unlockMeta(Session *session, ILMode mode);

	void enableLogging(Session *session);
	void disableLogging(Session *session);
private:
	TNTTable	*m_tntTable;
	TNTDatabase *m_tntDb;
};

/** 在线修改堆类型操作类 */
class TNTTblMntAlterHeapType : public TNTTblMntAlterColumn {
public:
	TNTTblMntAlterHeapType(TNTTable *table, Connection *conn, bool *cancelFlag = NULL);
	~TNTTblMntAlterHeapType();

protected:
	TableDef* preAlterTblDef(Session *session, TableDef *newTbdef, 
		TempTableDefInfo *tempTableDefInfo);
};

/** 在线Optimize操作类 */
class TNTTblMntOptimizer : public TNTTblMntAlterColumn {
public:
	TNTTblMntOptimizer(TNTTable *table, Connection *conn, bool *cancelFlag, bool keepOldDict);
	~TNTTblMntOptimizer();
};
}// namespace tnt

#endif