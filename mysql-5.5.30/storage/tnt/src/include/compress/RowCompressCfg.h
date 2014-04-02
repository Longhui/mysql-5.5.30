/*
* 记录压缩配置
*
* @author 李伟钊(liweizhao@corp.netease.com, liweizhao@163.org)
*/
#ifndef _NTSE_ROW_COMPRESS_CFG_H_
#define _NTSE_ROW_COMPRESS_CFG_H_

#include "misc/KVParser.h"

namespace ntse {

#define INVALID_DIC_SIZE_INFO "Invalid dictionary size: %d, valid range is %d ~ %d."
#define INVALID_DIC_MIN_LEN_INFO "Invalid dictionary minimum length: %d, valid range is %d ~ %d."
#define INVALID_DIC_MAX_LEN_INFO "Invalid dictionary maximum length: %d, valid range is %d ~ %d."
#define INVALID_COMPRESS_THRESHOLD_INFO "Invalid compress threshold: %d, valid range is 1 ~ 100."

/** 记录压缩配置 */
class RowCompressCfg {
public:
	static const uint MIN_DICTIONARY_SIZE = 4096;                      /** 最小字典项数 */
	static const uint MAX_DICTIONARY_SIZE = 65536;                     /** 最大字典项数 */
	static const uint DEFAULT_DICTIONARY_SIZE = 65536;                 /** 默认字典项数 */
	static const u16  DEFAULT_DIC_ITEM_MIN_LEN = 3;                    /** 默认字典项最小长度 */
	static const u16  DEFAULT_DIC_ITEM_MAX_LEN = 40;                   /** 默认字典项最大长度 */
	static const u16  MIN_DIC_ITEM_MIN_LEN = 3;                        /** 最小字典项最小长度 */
	static const u16  MAX_DIC_ITEM_MIN_LEN = 8;                        /** 最大字典项最小长度 */
	static const u16  MIN_DIC_ITEM_MAX_LEN = 8;                        /** 最小字典项最大长度 */
	static const u16  MAX_DIC_ITEM_MAX_LEN = 256;                      /** 最大字典项最大长度 */
	static const u8   DEFAULT_ROW_COMPRESS_THRESHOLD = 80;             /** 默认记录启用压缩百分比阀值, 如果压缩比低于这个百分比则不进行压缩 */
	static const char *DICT_SIZE_KEY;
	static const char *DICT_MIN_LEN_KEY;
	static const char *DICT_MAX_LEN_KEY;
	static const char *DICT_COMPRESS_THRESHOLD_KEY;

public:
	RowCompressCfg();
	RowCompressCfg(uint dicSize, u16 dicItemMinLen, u16 dicItemMaxLen, u8 cprsThresholdPct);
	RowCompressCfg(const RowCompressCfg &another);
	virtual ~RowCompressCfg() {}

	inline uint dicSize() const { 
		return m_dictionarySize; 
	}
	inline void setDicSize(uint dicSize) { 
		m_dictionarySize = dicSize; 
	}
	inline u16 dicItemMinLen() const { 
		return m_dicItemMinLen; 
	}
	inline void setDicItemMinLen(u16 min) {
		m_dicItemMinLen = min;
	}
	inline u16 dicItemMaxLen() const { 
		return m_dicItemMaxLen; 
	}
	inline void setDicItemMaxLen(u16 max) {
		m_dicItemMaxLen = max;
	}
	inline u8 compressThreshold() const { 
		return m_cprsThreshold; 
	}
	inline void setCompressThreshold(u8 threshold) {
		m_cprsThreshold = threshold;
	}

	bool operator == (const RowCompressCfg& another);
	void readFromKV(byte *buf, u32 size) throw(NtseException);
	void writeToKV(byte **buf, u32 *size) const throw(NtseException);

	static inline bool validateDictSize(uint size) {
		return size >= MIN_DICTIONARY_SIZE && size <= MAX_DICTIONARY_SIZE;
	}
	static inline bool validateDictMinLen(uint minLen) {
		return minLen >= MIN_DIC_ITEM_MIN_LEN && minLen <= MAX_DIC_ITEM_MIN_LEN;
	}
	static inline bool validateDictMaxLen(uint maxLen) {
		return maxLen >= MIN_DIC_ITEM_MAX_LEN && maxLen <= MAX_DIC_ITEM_MAX_LEN;
	}
	static inline bool validateCompressThreshold(uint threshold) {
		return threshold >= 1 && threshold <= 100;
	}
protected:
	uint              m_dictionarySize; /** 全局字典大小 */
	u16               m_dicItemMinLen;  /** 字典项最小长度 */
	u16               m_dicItemMaxLen;  /** 字典项最大长度 */
	u8                m_cprsThreshold;  /** 记录启用压缩阀值，如果大于这个阀值将对记录进行压缩，否则放弃对记录的压缩 */
};
}

#endif