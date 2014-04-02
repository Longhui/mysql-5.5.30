/**
 * NTSE B+树索引对外接口实现
 *
 * author: naturally (naturally@163.org)
 */

// Index.cpp: implementation of the DrsIndice and DrsIndex class.
//
//////////////////////////////////////////////////////////////////////

#include "api/Database.h"
#include "btree/Index.h"
#include "btree/IndexBPTreesManager.h"
#include "btree/IndexPage.h"
#include "api/Table.h"
#include "misc/Session.h"
#include "util/Sync.h"
#include "util/File.h"

// Implementation of DrsIndice class
//
//////////////////////////////////////////////////////////////////////


namespace ntse {

#define PageHandle BufferPageHandle

/**
 * 创建一个表的索引文件并初始化
 *
 * @param session	会话句柄
 * @param db		数据库
 * @param path		索引文件路径，含后缀
 * @param tableDef	表定义
 * @throw NtseException 文件无法创建，IO错误等等
 */
void DrsIndice::create(const char *path, const TableDef *tableDef) throw(NtseException) {
	ftrace(ts.idx, tout << path);

	// 创建索引相关文件
	File file(path);
	u64 errNo = file.create(false, false);

	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		NTSE_THROW(errNo, "Cannot create index file for table %s", tableDef->m_name);
	}

	// 初始化索引文件
	IndicePageManager::initIndexFileHeaderAndBitmap(&file);

	// 结束初始化
	errNo = file.close();
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		NTSE_THROW(errNo, "Cannot write index file for table %s", tableDef->m_name);
	}
}

/**
 * 打开一个表对应的索引，进行必要的初始化。
 * @post 索引文件头页面被pin在内存
 *
 * @param db		数据库对象
 * @param session	会话
 * @param path		索引文件所在目录
 * @param tableDef	表定义
 * @param lobStorage 大对象管理器
 * @return 索引
 * @throw NtseException IO异常，格式错误等
 */
DrsIndice* DrsIndice::open(Database *db, Session *session, const char *path, const TableDef *tableDef, LobStorage *lobStorage) throw(NtseException) {
	ftrace(ts.idx, tout << path);

	File *file = new File(path);
	u64 errNo = file->open(db->getConfig()->m_directIo);
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		delete file;
		NTSE_THROW(errNo, "Cannot open index file for table %s.", tableDef->m_name);
	}

	Buffer *buffer = db->getPageBuffer();
	
	DBObjStats *dbObjStats = new DBObjStats(DBO_Indice);
	Page *headerPage = buffer->getPage(session, file, PAGE_INDEX, 0, Shared, dbObjStats, NULL);

	DrsIndice *indice = new DrsBPTreeIndice(db, tableDef, file, lobStorage, headerPage, dbObjStats);

	buffer->unlockPage(session->getId(), headerPage, Shared);	// 这里将索引文件头pin在内存，等索引文件使用完毕再close释放

	return indice;
}


/**
 * 删除一个表对应的索引文件，只是简单删除该文件
 *
 * @param path	要删除的文件
 * @throw	抛出文件操作异常，文件已经不存在除外
 */
void DrsIndice::drop(const char *path) throw(NtseException) {
	ftrace(ts.idx, tout << path);

	u64 errCode;
	File file(path);
	errCode = file.remove();
	if (File::E_NOT_EXIST == File::getNtseError(errCode))
		return;

	if (File::E_NO_ERROR != File::getNtseError(errCode))
		NTSE_THROW(errCode, "Cannot drop index file %s", path);
}


/**
 * 重做创建索引的操作
 *
 * @param path		索引文件路径
 * @param tableDef	对应表定义
 * @return
 * @throw NtseException 文件无法创建，IO错误等等
 */
void DrsIndice::redoCreate(const char *path, const TableDef *tableDef) throw(NtseException) {
	ftrace(ts.idx, tout << path);

	File file(path);
	bool isExist;
	u64 errNo = file.isExist(&isExist);

	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		NTSE_THROW(errNo, "Cannot get file status %s.", path);
	}

	if (isExist) {
		u64 size;
		if (File::getNtseError(file.open(true)) != File::E_NO_ERROR) {
			NTSE_THROW(errNo, "Cannot open file %s.", path);
		}
		if (File::getNtseError(file.getSize(&size)) != File::E_NO_ERROR) {
			NTSE_THROW(errNo, "Cannot get file size %s.", path);
		}

		if (size >= IndicePageManager::NON_DATA_PAGE_NUM * Limits::PAGE_SIZE)
			return;	// 索引文件内容长度达到初始化标准，说明初始化完毕，可以返回
	}

	create(path, tableDef);
}


}

