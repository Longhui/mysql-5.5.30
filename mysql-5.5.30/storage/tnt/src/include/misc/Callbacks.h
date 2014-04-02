/**
 * ��������NTSE֧�ֵĻص���������
 * @author	�ձ�(naturally@163.org)
 */

#ifndef _NTSE_CALLBACKS_H_
#define _NTSE_CALLBACKS_H_

#include <vector>

using namespace std;

namespace ntse {

class TableDef;

enum CallbackType {
	ROW_INSERT = 0,		/** �в�������ص� */
	ROW_DELETE,			/** ��ɾ�������ص� */
	ROW_UPDATE,			/** �и��²����ص� */
	TABLE_CLOSE,		/** �رձ�����ص� */
	TABLE_ALTER,		/** �޸ı�ṹ�����ص� */
	CB_NUMBERS
};

class SubRecord;
struct NTSECallbackFN {
	/** �ص���������
	 * @param tableDef	������������
	 * @param brec		������в�������Ҫ�����в�����ǰ����û��ǰ������߲����в�������NULL
	 * @param arec		������в�������Ҫ�����в����ĺ���û�к�����߲����в�������NULL
	 * @param param		�ص������̶�����
	 */
	void (*callback)(const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param);
};

/**
 * �ص�����������
 */

class NTSECallbackManager {
public:
	NTSECallbackManager() {}

	/** ע��һ��ָ�����͵��õĻص�����
	 * @param type	����
	 * @param cbfn	�ص�����
	 */
	void registerCallback(CallbackType type, NTSECallbackFN *cbfn) {
		m_cbFNs[type].push_back(cbfn);
	}

	/** ע��һ��ָ�����͵Ļص�����
	 * @param type	����
	 * @param cbfn	�ص�����
	 * @return true������ע���ú�����falseΪ�Ҳ���
	 */
	bool unregisterCallback(CallbackType type, NTSECallbackFN *cbfn) {
		vector<NTSECallbackFN*> *callbacks = &m_cbFNs[type];
		vector<NTSECallbackFN*>::iterator itor = callbacks->begin();
		while (itor != callbacks->end()) {
			if ((*itor) == cbfn) {
				callbacks->erase(itor);
				return true;
			}
			++itor;
		}
		return false;
	}

	/** ����ִ������ָ�����͵Ļص��������������Ϊ�գ�������
	 * @param type		����
	 * @param tableDef	������������
	 * @param brec		������в����ص�����Ҫ�����в�����ǰ����û�л��߷��в���ΪNULL
	 * @param arec		������в����ص�����Ҫ�����в����ĺ���û�л��߷��в���ΪNULL
	 * @param param		�ص������̶�����
	 */
	void callback(CallbackType type, const TableDef *tableDef, const SubRecord *brec, const SubRecord *arec, void *param) {
		for (uint i = 0; i < m_cbFNs[type].size(); i++)
			m_cbFNs[type][i]->callback(tableDef, brec, arec, param);
	}

private:
	vector<NTSECallbackFN*> m_cbFNs[CB_NUMBERS];	/** ����ص������ĺ������� */
};

}

#endif

