/**
* KeyValue公共数据结构和数据类型定义
*
* @author 廖定柏(liaodingbai@corp.netease.com)
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

/** 将NTSE异常转化成服务端异常 */
#define SERVER_EXCEPTION_THROW(ERROR_CODE, ERROR_MSG)	\
	do {										\
		ServerException serverException;		\
		serverException.errcode = ERROR_CODE;	\
		serverException.message = ERROR_MSG;	\
		throw serverException;					\
	} while(0);

namespace ntse	{

	/** Key和结果集的定义*/
	typedef unsigned int IndexOfKey;

	/** 解决命名冲突的类型定义 */
	typedef keyvalue::TableInfo KVTableInfo;
	typedef keyvalue::ErrCode  KVErrorCode;

	/**
	*	由于thrift的binary类型在CPP中对应的是std::string类型，在传回结果之前需要将byte*转化成std::string
	*
	*	@param	result	存储字节流的字符串
	*	@param	data	字节流的首地址
	*	@param	length	字节流长度
	*
	*	@return	封装字节流的
	*/
	inline void bytes2String(string &result, byte *data, int length)	{
		if (NULL != data) {
			result.resize(length);
			result.assign(data, data +length);
		}
	}

	/**
	 *	根据给定的属性项构造ColList
	 *
	 *	@param	attrs	ColList中要包含的属性列
	 *
	 *	@return	ColList结果
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
	*	比较两个double型数
	*
	*	@param	double数一
	*	@param	double数二
	*	@return	相等返回true，否则false
	*/
	static bool isEqual(double a, double b)	{
		double x = a - b;

		if ((x >= -DBL_EPSILON) && (x <= DBL_EPSILON))
			return true;
		return false;
	}

	/**
	 *	根据匹配替换字符串
	 *	Note: 出现"\"的地方，直接去掉"\"，不发生替换
	 *	Example：	"%liaodingbai"	----->	"(.)*liaodingbai"
	 *				"\%liaodingbai"	----->	"%liaodingbai"
	 *
	 *	@param	toReplaced	将要被替换的字符串
	 *	@param	matchStr	匹配原则
	 *	@param	replaceStr	替换字符串
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
	 *	比较两个double数值(所有的数值类型都转化成double进行比较，decimal除外)
	 *
	 *	@param	l	比较值一
	 *	@param	r	比较值二
	 *	@param	compareType	比较类型(EQ,GRATER,LESS,EQLESS,EQGRATER)
	 *
	 *	@return	比较结果
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
	*	比较两个字符串
	*
	*	@param	one	字符串一
	*	@param	two	字符串二
	*	@param	compareType	比较类型(EQ,GRATER,LESS,EQLESS,EQGRATER)
	*
	*	@return	比较结果
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
	*	将NTSE内部异常码转化成KeyValue错误码
	*
	*	@param	code NTSE内部异常码
	*	@return KeyValue错误码
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
	 *	将NTSE的列类型转化成客户端定义的列类型
	 *
	 *	@param ntseColType NTSE的列类型
	 *	@return 客户端需要的列类型
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
