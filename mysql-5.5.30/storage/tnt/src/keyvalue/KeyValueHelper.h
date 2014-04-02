/**
* KeyValue�������ݽṹ���������Ͷ���
*
* @author �ζ���(liaodingbai@corp.netease.com)
*/

#ifndef _KEYVALUE_HELPER_H_
#define _KEYVALUE_HELPER_H_

#include <map>
#include <set>
#include <vector>
#include "gen-cpp/KV.h"
#include "gen-cpp/KV_types.h"
#include "util/Portable.h"
#include "mysql_priv.h"
#include "misc/TableDef.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"

using namespace std;
using namespace ntse;
using namespace keyvalue;
using keyvalue::Op;

#ifndef WIN32
#define DBL_EPSILON     2.2204460492503131e-016 /* smallest such that 1.0+DBL_EPSILON != 1.0 */
#endif

/** ��NTSE�쳣ת���ɷ�����쳣 */
#define SERVER_EXCEPTION_THROW(ERROR_CODE, ERROR_MSG)	\
	do {										\
		ServerException serverException;		\
		serverException.errcode = ERROR_CODE;	\
		serverException.message = ERROR_MSG;	\
		throw serverException;					\
	} while(0);

namespace ntse	{

	/** Key�ͽ�����Ķ���*/
	typedef unsigned int IndexOfKey;

	/** ���������ͻ�����Ͷ��� */
	typedef keyvalue::TableInfo KVTableInfo;
	typedef keyvalue::ErrCode  KVErrorCode;

	/**
	*	����thrift��binary������CPP�ж�Ӧ����std::string���ͣ��ڴ��ؽ��֮ǰ��Ҫ��byte*ת����std::string
	*
	*	@param	result	�洢�ֽ������ַ���
	*	@param	data	�ֽ������׵�ַ
	*	@param	length	�ֽ�������
	*
	*	@return	��װ�ֽ�����
	*/
	inline void bytes2String(string &result, byte *data, int length)	{
		if (NULL != data) {
			result.resize(length);
			result.assign(data, data +length);
		}
	}

	/**
	 *	���ݸ������������ColList
	 *
	 *	@param	attrs	ColList��Ҫ������������
	 *
	 *	@return	ColList���
	 */
	template <class T> const ColList extractColList(const vector<T> &attrs, set<u16> &sets, Session *session) {
		for (u32 i = 0; i < attrs.size(); ++i) {
			sets.insert(attrs[i].attrNo);
		}

		u16 *cols = (u16*)session->getMemoryContext()->alloc(sets.size() * sizeof(u16));
		copy(sets.begin(), sets.end(), cols);
		return ColList(sets.size(), cols);
	}

	/**
	*	�Ƚ�����double����
	*
	*	@param	double��һ
	*	@param	double����
	*	@return	��ȷ���true������false
	*/
	static bool isEqual(double a, double b)	{
		double x = a - b;

		if ((x >= -DBL_EPSILON) && (x <= DBL_EPSILON))
			return true;
		return false;
	}

	/**
	 *	����ƥ���滻�ַ���
	 *	Note: ����"\"�ĵط���ֱ��ȥ��"\"���������滻
	 *	Example��	"%liaodingbai"	----->	"(.)*liaodingbai"
	 *				"\%liaodingbai"	----->	"%liaodingbai"
	 *
	 *	@param	toReplaced	��Ҫ���滻���ַ���
	 *	@param	matchStr	ƥ��ԭ��
	 *	@param	replaceStr	�滻�ַ���
	 */
	static void stringReplace(string& toReplaced, const string& matchStr, const string& replaceStr)	{
		string::size_type pos = 0;
		string::size_type a = matchStr.size();
		string::size_type b = replaceStr.size();
		while((pos = toReplaced.find(matchStr, pos)) != string::npos)	{
			if (pos != 0 && toReplaced[pos - 1] == '\\') {
				toReplaced.erase(pos - 1, 1);
				pos += a;
			} else {
				toReplaced.replace(pos,a,replaceStr);
				pos += b;
			}
		}
	}

	/**
	 *	�Ƚ�����double��ֵ(���е���ֵ���Ͷ�ת����double���бȽϣ�decimal����)
	 *
	 *	@param	l	�Ƚ�ֵһ
	 *	@param	r	�Ƚ�ֵ��
	 *	@param	compareType	�Ƚ�����(EQ,GRATER,LESS,EQLESS,EQGRATER)
	 *
	 *	@return	�ȽϽ��
	 */
	static bool compareNumber(double l, double r, Op::type compareType) {
		switch (compareType)	{
			case Op::EQ:
				return isEqual(l ,r);
			case Op::NOTEQ:
				return !isEqual(l ,r);
			case Op::GRATER:
				return l > r;
			case Op::LESS:
				return l < r;
			case Op::EQLESS:
				return l <= r;
			case Op::EQGRATER:
				return l >= r;
		}
		return false;
	}

	/**
	*	�Ƚ������ַ���
	*
	*	@param	one	�ַ���һ
	*	@param	two	�ַ�����
	*	@param	compareType	�Ƚ�����(EQ,GRATER,LESS,EQLESS,EQGRATER)
	*
	*	@return	�ȽϽ��
	*/
	static bool compareString(const string &one, const string &two, Op::type compareType)	{
		int compareResult;
		compareResult = System::stricmp(one.c_str(), two.c_str());
		 
		switch (compareType)	{
			case Op::EQ:
				return compareResult == 0;
			case Op::NOTEQ:
				return compareResult != 0;
			case Op::GRATER:
				return compareResult > 0;
			case Op::LESS:
				return compareResult < 0;
			case Op::EQLESS:
				return compareResult <= 0;
			case Op::EQGRATER:
				return compareResult >= 0;
		}
		return false;
	}

	/**
	*	��NTSE�ڲ��쳣��ת����KeyValue������
	*
	*	@param	code NTSE�ڲ��쳣��
	*	@return KeyValue������
	*/
	inline static KVErrorCode::type exceptToKVErrorCode(ErrorCode code)	{
		switch(code) {
			case NTSE_EC_FILE_NOT_EXIST:
				return KVErrorCode::KV_EC_TABLE_NOT_EXIST;
			default:
				return (KVErrorCode::type)code;
		}
	}

	/**
	 *	��NTSE��������ת���ɿͻ��˶����������
	 *
	 *	@param ntseColType NTSE��������
	 *	@return �ͻ�����Ҫ��������
	 */
	static DataType::type convertNtseColType(ColumnType ntseColType)	{
		switch (ntseColType)	{
			case CT_TINYINT:
			case CT_SMALLINT:
			case CT_MEDIUMINT:
			case CT_INT:
			case CT_BIGINT:
			case CT_FLOAT:
			case CT_DOUBLE:
			case CT_DECIMAL:
			case CT_RID:
			case CT_CHAR:
			case CT_VARCHAR:
			case CT_BINARY:
			case CT_VARBINARY:
				return (DataType::type)ntseColType;
			case CT_SMALLLOB:
			case CT_MEDIUMLOB:
				return DataType::KV_BLOB;
		}
	}
}
#endif
