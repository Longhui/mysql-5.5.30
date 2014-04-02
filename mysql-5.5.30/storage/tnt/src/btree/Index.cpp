/**
 * NTSE B+����������ӿ�ʵ��
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
 * ����һ����������ļ�����ʼ��
 *
 * @param session	�Ự���
 * @param db		���ݿ�
 * @param path		�����ļ�·��������׺
 * @param tableDef	����
 * @throw NtseException �ļ��޷�������IO����ȵ�
 */
void DrsIndice::create(const char *path, const TableDef *tableDef) throw(NtseException) {
	ftrace(ts.idx, tout << path);

	// ������������ļ�
	File file(path);
	u64 errNo = file.create(false, false);

	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		NTSE_THROW(errNo, "Cannot create index file for table %s", tableDef->m_name);
	}

	// ��ʼ�������ļ�
	IndicePageManager::initIndexFileHeaderAndBitmap(&file);

	// ������ʼ��
	errNo = file.close();
	if (File::getNtseError(errNo) != File::E_NO_ERROR) {
		NTSE_THROW(errNo, "Cannot write index file for table %s", tableDef->m_name);
	}
}

/**
 * ��һ�����Ӧ�����������б�Ҫ�ĳ�ʼ����
 * @post �����ļ�ͷҳ�汻pin���ڴ�
 *
 * @param db		���ݿ����
 * @param session	�Ự
 * @param path		�����ļ�����Ŀ¼
 * @param tableDef	����
 * @param lobStorage ����������
 * @return ����
 * @throw NtseException IO�쳣����ʽ�����
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

	buffer->unlockPage(session->getId(), headerPage, Shared);	// ���ｫ�����ļ�ͷpin���ڴ棬�������ļ�ʹ�������close�ͷ�

	return indice;
}


/**
 * ɾ��һ�����Ӧ�������ļ���ֻ�Ǽ�ɾ�����ļ�
 *
 * @param path	Ҫɾ�����ļ�
 * @throw	�׳��ļ������쳣���ļ��Ѿ������ڳ���
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
 * �������������Ĳ���
 *
 * @param path		�����ļ�·��
 * @param tableDef	��Ӧ����
 * @return
 * @throw NtseException �ļ��޷�������IO����ȵ�
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
			return;	// �����ļ����ݳ��ȴﵽ��ʼ����׼��˵����ʼ����ϣ����Է���
	}

	create(path, tableDef);
}


}

