#ifndef _TNT_TABLE_ARG_ALTER_H_
#define _TNT_TABLE_ARG_ALTER_H_

#include "api/TblArgAlter.h"

using namespace ntse;
using namespace std;

namespace tnt {
class TNTDatabase;
class TNTTable;

class TNTTableArgAlterHelper: public TableArgAlterHelper {
public:
	TNTTableArgAlterHelper(TNTDatabase *tntDb, Connection *conn, Parser *parser, int timeout, bool inLockTables);
protected:
	void alterTableArgumentReal(Session *session, vector<string> *parFiles, const char *name, const char *value) throw(NtseException);
	TNTDatabase *m_tntDb;		/* Êý¾Ý¿â */
};
}
#endif
