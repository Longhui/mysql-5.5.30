/**
 * 一个功能简单的key-value解析器
 *
 * @author 忻丁峰(xindingfeng@corp.netease.com)
 */
#ifndef _NTSE_KVPARSER_H_
#define _NTSE_KVPARSER_H_
#include <map>
#include <string>
#include "util/Array.h"
#include "misc/Global.h"

using namespace std;

/**该类主要负责对固定格式的key-value格式进行解析，取值，序列化。
 * 约定的格式为：
 * key与value之间用":"(即"key:value")，2个key-value之间用";"(即"k1:v1;k2:v2")，数组之间用","，如果value是一个对象，用"{}"括起来
 * k1:v1;k2:v21,v22;k3:{k31:v31;k32:v32};k4:{k41:v41;k42:v42},{k43:v43;k44:v44}
 */
namespace ntse {
	class KVParser
	{
	public:
		KVParser(void);
		~KVParser(void);
		void serialize(byte **buffer, u32 *size);
		void deserialize(byte *buffer, u32 size) throw (NtseException);

		int getValueI(const string &key, int defaultValue = 0);
		string getValue(const string &key, const string &defaultValue = "");
		bool getValueB(const string &key, bool defaultValue = true);
		void setValueI(const string &key, int value);
		void setValue(const string &key, const string &value);
		void setValueB(const string &key, bool value);
		bool isKeyExist(const string &key);
		Array<string>* getAllKey();

		/** 设置key-value, 其中value为数组对象
		* @param key 键值
		* @param t   对应key待序列化的数组对象value
		* @param size 数组大小
		*/
		template <class T>
		void setValueO(const string &key, const T * const *t, const size_t& size) {
			string value;
			value.push_back(OBJ_BEGIN);
			byte *tmpBuf;
			u32 tmpBufSize;
			for (size_t i = 0; i < size; i++) {
				t[i]->writeToKV(&tmpBuf, &tmpBufSize);
				value.append((char *)tmpBuf, (int)tmpBufSize);
				if (i != size - 1) {
					value.push_back(KVParser::OBJ_END);
					value.push_back(KVParser::ARRAY_SEP);
					value.push_back(KVParser::OBJ_BEGIN);
				}
				delete []tmpBuf;
				tmpBuf = NULL;
			}
			value.push_back(OBJ_END);
			setValue(key, value);
		}

		template <class T>
		void setValueO(const string &key, const T *t) {
			string value;
			value.push_back(OBJ_BEGIN);

			byte *tmpBuf;
			u32 tmpBufSize;
			t->writeToKV(&tmpBuf, &tmpBufSize);
			value.append((char *)tmpBuf, (int)tmpBufSize);
			delete [] tmpBuf;

			value.push_back(OBJ_END);

			setValue(key, value);
		}

		template <class T>
		void getValueO(const string &k, const size_t& size, T ***t, const string &defaultValue = "") {
			string value = getValue(k, defaultValue);
			assert(value.size() >= 2);
			assert(value.at(0) == KVParser::OBJ_BEGIN);
			assert(value.at(value.size() - 1) == KVParser::OBJ_END);
			Array<string> *objArr = parseArray((byte *)value.c_str(), value.size());
			assert(objArr->getSize() == size);
			*t = new T*[size];
			for (size_t i = 0; i < size; i++) {
				(*t)[i] = new T();
				string &obj = (*objArr)[i];
				assert(obj[0] == KVParser::OBJ_BEGIN);
				assert(obj[obj.size() - 1] == KVParser::OBJ_END);
				(*t)[i]->readFromKV((byte *)obj.c_str() + 1, obj.size() - 2); 
			}
			delete objArr;
		}

		template <class T>
		void getValueO(const string &k, T **t, const string &defaultValue = "") {
			string value = getValue(k, defaultValue);
			assert(value.size() >= 2);
			assert(value.at(0) == KVParser::OBJ_BEGIN);
			assert(value.at(value.size() - 1) == KVParser::OBJ_END);
			*t = new T();
			assert(value[0] == KVParser::OBJ_BEGIN);
			assert(value[value.size() - 1] == KVParser::OBJ_END);
			(*t)->readFromKV((byte *)value.c_str() + 1, value.size() - 2); 
		}

		Array<string>* parseArray(byte* buffer, u32 size);
		void serializeArray(const Array<string> &strs, byte **buffer, u32 *size);
		void serializeArray(const Array<int> &src, byte **buffer, u32 *size);
	private:
		bool check(byte *buffer, u32 size);
		int getDigit(int i);

		map<string, string> m_kvMap;

	public:
		static const char KV_SEP = ':';
		static const char OBJ_BEGIN = '{';
		static const char OBJ_END = '}';
		static const char ATTR_SEP = ';';
		static const char ARRAY_SEP = ',';

		static const char *STR_TRUE;
		static const char *STR_FALSE;
	};
}
#endif