/**
 * һ�����ܼ򵥵�key-value������
 *
 * @author �ö���(xindingfeng@corp.netease.com)
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

/**�����е�key-value���л�Ϊbyte�������
 * @param buffer out ���л����byte����
 * @param size out ���л����byte���鳤��
 */
void KVParser::serialize(byte **buffer, u32 *size) {
	map<string, string>::iterator iter = m_kvMap.begin();
	vector<string> keyArr;
	*size = 0;
	while (iter != m_kvMap.end()) {
		*size += iter->first.size() + 1 + iter->second.size() + 1;//key��value֮����1���ָ�����key-value��֮��Ҳ��1���ָ���
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

/** ��buffer�����ݷ����л�
 * @param buffer byte���飬�����л�����
 * @param size buffer�ĳ���
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

/** ����key��ȡ��Ӧ��value������valueת��Ϊint������Ҳ�����Ӧ��ֵ������Ĭ��ֵ
 * @param key ��ֵ
 * @param defaultValue Ĭ��ֵ
 * @return ����key��Ӧ��value�������ڷ���Ĭ��ֵ
 */
int KVParser::getValueI(const string &key, int defaultValue) {
	int ret = defaultValue;
	map<string, string>::iterator iter = m_kvMap.find(key);
	if (iter != m_kvMap.end()) {
		ret = Parser::parseInt(iter->second.c_str());
	}
	return ret;
}

/** ����key��ȡ��Ӧ��value������valueת��Ϊstring������Ҳ�����Ӧ��ֵ������Ĭ��ֵ
 * @param key ��ֵ
 * @param defaultValue Ĭ��ֵ
 * @return ����key��Ӧ��value�������ڷ���Ĭ��ֵ
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
 * �ж�ָ����key�Ƿ����
 * @param key ��ֵ
 * @return key���ڷ���true�����򷵻�false
 */
bool KVParser::isKeyExist(const string &key) {
	map<string, string>::iterator iter = m_kvMap.find(key);
	return iter != m_kvMap.end();
}

/** ����key��ȡ��Ӧ��value������valueת��Ϊbool������Ҳ�����Ӧ��ֵ������Ĭ��ֵ
 * @param key ��ֵ
 * @param defaultValue Ĭ��ֵ
 * @return ����key��Ӧ��value�������ڷ���Ĭ��ֵ
 */
bool KVParser::getValueB(const string &key, bool defaultValue) {
	bool ret = defaultValue;
	map<string, string>::iterator iter = m_kvMap.find(key);
	if (iter != m_kvMap.end()) {
		ret = Parser::parseBool(iter->second.c_str());
	}
	return ret;
}

/** ����key-value
 * @param key ��ֵ
 * @param value ��Ӧ��key��valueֵ
 */
void KVParser::setValueI(const string &key, int value) {
	char* strValue = new char[Limits::MAX_FREE_MALLOC + 1];
	memset(strValue, 0, Limits::MAX_FREE_MALLOC + 1);
	sprintf(strValue, "%d", value);
	setValue(key, string(strValue));
	delete[] strValue;
}

/** ����key-value
 * @param key ��ֵ
 * @param value ��Ӧ��key��valueֵ
 */
void KVParser::setValue(const string &key, const string &value) {
	m_kvMap.insert(map<string, string>::value_type(key, value));
}

/** ����key-value
 * @param key ��ֵ
 * @param value ��Ӧ��key��valueֵ
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

/** ��buffer�а�������ĸ�ʽ��ȡ�����е�����Ԫ��
 * @param buffer byte����
 * @param size buffer�ĳ���
 * @return �����������е��ַ���
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

/** ���ַ������鰴��һ���Ĺ���������л����
 * @param buffer ��������л�����
 * @param size ���л������ݵĳ���
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

/**������ӦintΪ��λ��
 * @param int ��Ҫ�������ֵ
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

/** ��int���鰴��һ���Ĺ���������л����
 * @param buffer ��������л�����
 * @param size ���л������ݵĳ���
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