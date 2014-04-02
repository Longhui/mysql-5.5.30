/**
 * MMSӳ������
 *
 * ʵ��MmsRidMap���MmsPKeyMap��
 * MmsRidMap��ά��RowID��MMS�����¼���ӳ��
 * MmsPKeyMap��ά��������MMS�����¼���ӳ��
 *
 * @author �۷�(shaofeng@corp.netease.com, sf@163.org)
 */

#include "mms/MmsMap.h"
#include "mms/MmsPage.h"

namespace ntse {

/************************************************************************/
/*                 MmsRidMap��ʵ��										*/
/************************************************************************/
/** 
 * MmsRidMap���캯��
 *
 * @param mms MMSȫ�ֶ���
 * @param mmsTable MMS�����
 */
MmsRidMap::MmsRidMap(Mms *mms, MmsTable *mmsTable) {
	m_mmsTable = mmsTable;
	m_ridHash = new DynHash<RowId, MmsRecord*, RidMmsHasher, 
		RidHasher, RidEqualer<RowId, MmsRecord*> >(mms, PAGE_MMS_MISC);
}

/** 
 * MmsRidMap��������
 */
MmsRidMap::~MmsRidMap() {
	delete m_ridHash;
}

/** 
 * ��RIDӳ������һ����¼
 * @pre �Ѽ�MMS������
 *
 * @param mmsRecord ��¼��
 */
void MmsRidMap::put(MmsRecord *mmsRecord) {
	m_ridHash->put(mmsRecord);
}

/**
 * ��Ԥ���ռ�
 * @pre �Ѽ�MMS������
 *
 * @param num ��Ԥ���ĸ���
 * @param force ǿ��Ԥ��
 * @return �Ƿ�ɹ�
 */
bool MmsRidMap::reserve(int num, bool force) {
	return m_ridHash->reserveSize(m_ridHash->getSize() + num, force);
}

/** 
 * ȡ��Ԥ��
 * @pre �Ѽ�MMS������
 */
void MmsRidMap::unreserve() {
	m_ridHash->reserveSize(0);
}

/** 
 * ��RIDӳ�����ɾ��һ����¼
 * @pre �Ѽ�MMS������
 * 
 * @param mmsRecord ��¼��
 */
void MmsRidMap::del(MmsRecord *mmsRecord) {
	MmsRecord *ret = m_ridHash->remove(RID_READ(mmsRecord->m_rid));
	assert(ret);
	UNREFERENCED_PARAMETER(ret);
}

/** 
 * ��RIDӳ����в�ѯһ����¼
 * @pre �Ѽ�MMS������(REDO�׶��޴�Ҫ��)
 *
 * @param rowId ��¼��RowID
 * @return ��¼��
 */
MmsRecord* MmsRidMap::get(RowId rowId) {
	return m_ridHash->get(rowId);
}

/** 
 * ��ȡ��ͻ��Ϣ
 *
 * @param avgConflictLen OUT ��ͻ����ƽ������
 * @param maxConflictLen OUT ��ͻ������󳤶�
 */
void MmsRidMap::getConflictStatus(double *avgConflictLen, size_t *maxConflictLen) {
	m_ridHash->getConflictStatus(avgConflictLen, maxConflictLen);
}

}

