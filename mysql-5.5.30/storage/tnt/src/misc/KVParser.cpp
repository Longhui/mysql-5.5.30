/**
 * 一个功能简单的key-value解析器
 *
 * @author 忻丁峰(xindingfeng@corp.netease.com)
 */
#include <assert.h>
#include <stack>
#include <algorithm>
#include "misc/KVParser.h"
#include "misc/Parser.h"

using namespace std;

namespace ntse {

const char *KVParser::STR_TRUE = "true";
const char *KVParser::STR_FALSE = "false";

KVParser::KVParser(void)
{
}

KVParser::~KVParser(void)
{
}

bool KVParser::check(byte *buffer, u32 size) {
	UNREFERENCED_PARAMETER(size);
	char *p = (char *)buffer;
	if (*p == KV_SEP || *p == OBJ_BEGIN || *p == OBJ_END || *p == ATTR_SEP || *p == ARRAY_SEP) {
		return false;
	}

	return true;
}

/**将所有的key-value序列化为byte数组输出
 * @param buffer out 序列化后的byte数组
 * @param size out 序列化后的byte数组长度
 */
void KVParser::serialize(byte **buffer, u32 *size) {
	map<string, string>::iterator iter = m_kvMap.begin();
	vector<string> keyArr;
	*size = 0;
	while (iter != m_kvMap.end()) {
		*size += iter->first.size() + 1 + iter->second.size() + 1;//key与value之间有1个分隔符，key-value对之间也有1个分隔符
		keyArr.push_back(iter->first);
		iter++;
	}

	sort(keyArr.begin(), keyArr.end());

	*buffer = new byte[*size];
	uint offset = 0;
	for (vector<string>::iterator it = keyArr.begin(); it != keyArr.end(); ++it) {
		iter = m_kvMap.find(*it);
		assert(iter != m_kvMap.end());
		memcpy(*buffer + offset, iter->first.c_str(), iter->first.size());
		offset += iter->first.size();
		(*buffer)[offset++] = KV_SEP;
		memcpy(*buffer + offset, iter->second.c_str(), iter->second.size());
		offset += iter->second.size();
		(*buffer)[offset++] = ATTR_SEP;
	}
	assert(*size == offset);
	(*buffer)[*size - 1] = 0;
	*size -= 1;
}

/** 将buffer中数据反序列化
 * @param buffer byte数组，反序列化内容
 * @param size buffer的长度
 */
void KVParser::deserialize(byte *buffer, u32 size) throw (NtseException) {
	assert(buffer != NULL);

	uint i = 0;
	char *p = NULL;
	stack<char> markStack;
	bool isKey = true;

	if (!check(buffer, size)) {
		NTSE_THROW(NTSE_EC_TABLEDEF_ERROR, "can't begin with %c", buffer[0]);
	}

	string key, value;
	for (i = 0; i < size; i++) {
		p = (char *)(buffer + i);

		if (*p == KV_SEP && markStack.empty()) {
			isKey = false;
			continue;
		} else if (*p == ATTR_SEP && markStack.empty()) {
			m_kvMap.insert(map<string, string>::value_type(key, value));
			key.clear();
			value.clear();
			isKey = true;
			continue;
		} else if (*p == OBJ_BEGIN) {
			markStack.push(*p);
		} else if (*p == OBJ_END) {
			assert(!markStack.empty());
			markStack.pop();
		}

		if (isKey) {
			key.push_back(*p);
		} else {
			value.push_back(*p);
		}
	}

	if (key.size() > 0 && value.size() > 0) {
		m_kvMap.insert(map<string, string>::value_type(key, value));
	}

	assert(markStack.empty());
}

/** 根据key获取相应的value，并将value转化为int，如果找不到相应的值，返回默认值
 * @param key 键值
 * @param defaultValue 默认值
 * @return 返回key对应的value，不存在返回默认值
 */
int KVParser::getValueI(const string &key, int defaultValue) {
	int ret = defaultValue;
	map<string, string>::iterator iter = m_kvMap.find(key);
	if (iter != m_kvMap.end()) {
		ret = Parser::parseInt(iter->second.c_str());
	}
	return ret;
}

/** 根据key获取相应的value，并将value转化为string，如果找不到相应的值，返回默认值
 * @param key 键值
 * @param defaultValue 默认值
 * @return 返回key对应的value，不存在返回默认值
 */
string KVParser::getValue(const string &key, const string &defaultValue) {
	string ret = defaultValue;
	map<string, string>::iterator iter = m_kvMap.find(key);
	if (iter != m_kvMap.end()) {
		ret = iter->second.c_str();
	}
	return ret;
}

/**
 * 判断指定的key是否存在
 * @param key 键值
 * @return key存在返回true，否则返回false
 */
bool KVParser::isKeyExist(const string &key) {
	map<string, string>::iterator iter = m_kvMap.find(key);
	return iter != m_kvMap.end();
}

/** 根据key获取相应的value，并将value转化为bool，如果找不到相应的值，返回默认值
 * @param key 键值
 * @param defaultValue 默认值
 * @return 返回key对应的value，不存在返回默认值
 */
bool KVParser::getValueB(const string &key, bool defaultValue) {
	bool ret = defaultValue;
	map<string, string>::iterator iter = m_kvMap.find(key);
	if (iter != m_kvMap.end()) {
		ret = Parser::parseBool(iter->second.c_str());
	}
	return ret;
}

/** 设置key-value
 * @param key 键值
 * @param value 对应于key的value值
 */
void KVParser::setValueI(const string &key, int value) {
	char* strValue = new char[Limits::MAX_FREE_MALLOC + 1];
	memset(strValue, 0, Limits::MAX_FREE_MALLOC + 1);
	sprintf(strValue, "%d", value);
	setValue(key, string(strValue));
	delete[] strValue;
}

/** 设置key-value
 * @param key 键值
 * @param value 对应于key的value值
 */
void KVParser::setValue(const string &key, const string &value) {
	m_kvMap.insert(map<string, string>::value_type(key, value));
}

/** 设置key-value
 * @param key 键值
 * @param value 对应于key的value值
 */
void KVParser::setValueB(const string &key, bool value) {
	string strValue;
	if (value) {
		strValue.append(STR_TRUE);
	} else {
		strValue.append(STR_FALSE);
	}

	setValue(key, strValue);
}

Array<string>* KVParser::getAllKey() {
	Array<string> *keys = new Array<string>();
	map<string, string>::iterator iter = m_kvMap.begin();
	while (iter != m_kvMap.end()) {
		keys->push(iter->first);
		iter++;
	}

	return keys;
}

/** 从buffer中按照数组的格式提取出其中的所有元素
 * @param buffer byte数组
 * @param size buffer的长度
 * @return 返回其中所有的字符串
 */
Array<string>* KVParser::parseArray(byte* buffer, u32 size) {
	assert(buffer != NULL);
	assert(buffer[0] != ARRAY_SEP);
	Array<string> *strs = new Array<string>();

	uint i = 0;
	char *p = NULL;
	stack<char> markStack;

	string str;
	for (i = 0; i < size; i++) {
		p = (char *)(buffer + i);

		if (*p == ARRAY_SEP && markStack.empty()) {
			if (!str.empty()) {
				strs->push(str);
			}
			str.clear();
			continue;
		} else if (*p == OBJ_BEGIN) {
			markStack.push(*p);
		} else if (*p == OBJ_END) {
			assert(!markStack.empty());
			markStack.pop();
		}

		str.push_back(*p);
	}

	if (str.size() > 0) {
		strs->push(str);
	}

	assert(markStack.empty());

	return strs;
}

/** 将字符串数组按照一定的规则进行序列化输出
 * @param buffer 输出的序列化内容
 * @param size 序列化后内容的长度
 */
void KVParser::serializeArray(const Array<string> &strs, byte **buffer, u32 *size) {
	*size = 0;
	unsigned int i = 0, offset = 0;
	for (i = 0; i < strs.getSize(); i++) {
		size += strs[i].size() + 1;
	}
	
	*buffer = new byte[*size];
	for (i = 0; i < strs.getSize(); i++) {
		memcpy(*buffer + offset, strs[i].c_str(), strs[i].size());
		offset += strs[i].size();
		*buffer[offset++] = ARRAY_SEP;
	}

	assert(*size == offset);
	(*buffer)[*size - 1] = 0;
	*size -= 1;
}

/**计算相应int为几位数
 * @param int 需要计算的数值
 */
int KVParser::getDigit(int i) {
	int digit = 0;
	if (i == 0) {
		return 1;
	}

	while (i > 0) {
		i /= 10;
		digit++;
	}
	return digit;
}

/** 将int数组按照一定的规则进行序列化输出
 * @param buffer 输出的序列化内容
 * @param size 序列化后内容的长度
 */
void KVParser::serializeArray(const Array<int> &srcs, byte **buffer, u32 *size) {
	uint offset = 0;
	uint i = 0;
	*size = 0;
	for (i = 0; i < srcs.getSize(); i++) {
		*size += getDigit(srcs[i]) + 1;
	}

	*buffer = new byte[*size];
	for (i = 0; i < srcs.getSize(); i++) {
		sprintf((char*)(*buffer + offset), "%d", srcs[i]);
		offset += getDigit(srcs[i]);
		(*buffer)[offset++] = ARRAY_SEP;
	}

	assert(*size == offset);
	(*buffer)[*size - 1] = 0;
	*size -= 1;
}
}