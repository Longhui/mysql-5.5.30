/*
* ¼ÇÂ¼Ñ¹ËõÅäÖÃ
*
* @author ÀîÎ°îÈ(liweizhao@corp.netease.com, liweizhao@163.org)
*/
#include "compress/RowCompressCfg.h"

namespace ntse {

const char * RowCompressCfg::DICT_SIZE_KEY = "dict_size";
const char * RowCompressCfg::DICT_MIN_LEN_KEY = "dict_min_len";
const char * RowCompressCfg::DICT_MAX_LEN_KEY = "dict_max_len";
const char * RowCompressCfg::DICT_COMPRESS_THRESHOLD_KEY = "dict_compress_threshold";

RowCompressCfg::RowCompressCfg() 
: m_dictionarySize(DEFAULT_DICTIONARY_SIZE), m_dicItemMinLen(DEFAULT_DIC_ITEM_MIN_LEN), 
m_dicItemMaxLen(DEFAULT_DIC_ITEM_MAX_LEN), m_cprsThreshold(DEFAULT_ROW_COMPRESS_THRESHOLD) {
}

RowCompressCfg::RowCompressCfg(uint dicSize, u16 dicItemMinLen, u16 dicItemMaxLen, u8 cprsThresholdPct)
: m_dictionarySize(dicSize), m_dicItemMinLen(dicItemMinLen), m_dicItemMaxLen(dicItemMaxLen) {
	assert(cprsThresholdPct > 0 && cprsThresholdPct < 100);
	m_cprsThreshold = cprsThresholdPct;
}

RowCompressCfg::RowCompressCfg(const RowCompressCfg &another) 
: m_dictionarySize(another.m_dictionarySize), m_dicItemMinLen(another.m_dicItemMinLen), 
m_dicItemMaxLen(another.m_dicItemMaxLen), m_cprsThreshold(another.m_cprsThreshold) {
}

bool RowCompressCfg::operator == (const RowCompressCfg& another) {
	if (m_dictionarySize != another.m_dictionarySize)
		return false;
	if (m_dicItemMinLen != another.m_dicItemMinLen)
		return false;
	if (m_dicItemMaxLen != another.m_dicItemMaxLen)
		return false;
	if (m_cprsThreshold != another.m_cprsThreshold)
		return false;
	return true;
}

void RowCompressCfg::readFromKV(byte *buf, u32 size) throw(NtseException) {
	assert(size > 0);
	KVParser kvparser;
	kvparser.deserialize(buf, size);
	m_dictionarySize = (uint)kvparser.getValueI(string(DICT_SIZE_KEY), DEFAULT_DICTIONARY_SIZE);
	m_dicItemMinLen = (u16)kvparser.getValueI(string(DICT_MIN_LEN_KEY), DEFAULT_DIC_ITEM_MIN_LEN);
	m_dicItemMaxLen = (u16)kvparser.getValueI(string(DICT_MAX_LEN_KEY), DEFAULT_DIC_ITEM_MAX_LEN);
	m_cprsThreshold = (u8)kvparser.getValueI(string(DICT_COMPRESS_THRESHOLD_KEY), DEFAULT_ROW_COMPRESS_THRESHOLD);
}

void RowCompressCfg::writeToKV(byte **buf, u32 *size) const throw(NtseException) {
	KVParser kvparser;
	kvparser.setValueI(string(DICT_SIZE_KEY), m_dictionarySize);
	kvparser.setValueI(string(DICT_MIN_LEN_KEY), m_dicItemMinLen);
	kvparser.setValueI(string(DICT_MAX_LEN_KEY), m_dicItemMaxLen);
	kvparser.setValueI(string(DICT_COMPRESS_THRESHOLD_KEY), m_cprsThreshold);
	kvparser.serialize(buf, size);
}

}