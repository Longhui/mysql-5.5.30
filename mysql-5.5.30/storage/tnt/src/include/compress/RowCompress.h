/*
 * 记录压缩
 *
 * @author 李伟钊(liweizhao@corp.netease.com)
 */
#ifndef  _NTSE_ROW_COMPRESS_H_
#define  _NTSE_ROW_COMPRESS_H_

#include "compress/RowCompressCfg.h"
#include "compress/RowCompressCoder.h"
#include "compress/dastrie.h"
#include "misc/Global.h"
#include "util/File.h"
#include "misc/Session.h"
#include "api/Database.h"
#include "misc/TableDef.h"
#include "misc/Verify.h"

using namespace std;
using namespace dastrie;

namespace ntse {

/** 判断文件操作返回码是否有错误 */
#define CHECK_FILE_ERR(errCode) (File::getNtseError(errCode) != File::E_NO_ERROR)

/** 用于跟踪调试记录压缩解压缩过程编解码情况的宏 */
#ifdef NTSE_TRACE_COMPRESS
#define traceCoded(a,b) fprintf(stderr, "C(%d, %d)", a, b)
#define traceUncoded(a) fprintf(stderr, "U[%d]", a)
#define traceCompressBegin() fprintf(stderr, "COMPRESS<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
#define traceCompressEnd() fprintf(stderr, "\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
#define traceDecompressBegin() fprintf(stderr, "DECOMPRESS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n")
#define traceDecompressEnd() fprintf(stderr, "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n")
#else
#define traceCoded(a, b) 
#define traceUncoded(a)
#define traceCompressBegin()
#define traceCompressEnd() 
#define traceDecompressBegin()
#define traceDecompressEnd()
#endif

	/**
	 * 用不同字节表示的属性组压缩段最大长度
	 */
	enum CodedSegSize {
		ONE_BYTE_SEG_MAX_SIZE = 1 << 7, /** 1字节表示的段最大长度 */
		TWO_BYTE_SEG_MAX_SIZE = 1 << 15,/** 2字节表示的段最大长度 */
	};

	/**
	* 全局字典
	* 
	* 全局字典保存为一个数组，字典索引即是数组下标。另外全局字典还被
	* 另外组织成一种静态双数组结构（SDAT），可以减少Trie树的空间开销
	* 并且不影响查找效率。
	*/
	class RCDictionary {
	public:
		const static uint DICTIONARY_PAGE_SIZE = 1024;   /** 用于字典数据的内存分配上下文页面大小 */
		const static uint DEFAULT_RESERVE_PAGES = 4;     /** 用于字典数据的内存分配上下文的预留页面数 */

	public:
		typedef byte KeyType;
		typedef byte ValueType;
		//FIXME:字典项的最大长度不能超过255个字节，否则这里DicItemLenType的定义要改
		typedef u8 DicItemLenType;
		typedef dastrie::builder<KeyType *, ValueType *, DicItemLenType, doublearray5_traits> BuilderType;
		typedef dastrie::trie<ValueType, DicItemLenType, doublearray5_traits> TrieType;
		typedef BuilderType::record_type DicItemType;

	public:
		RCDictionary();
		RCDictionary(u16 tblId, File *dictFile, uint capacity = RowCompressCfg::DEFAULT_DICTIONARY_SIZE);
		~RCDictionary();
		void setDicItem(const uint& index, const byte* itemData, const DicItemLenType& itemDataLen);
		/**
		 * 根据字典索引获得字典项的值
		 * 内存使用约定：直接使用原内存，不做拷贝
		 * @param index    字典索引
		 * @param itemAddr OUT，保存字典项的地址
		 * @return         字典项长度
		 */
		inline uint getDicItem(const uint& index, const byte** itemAddr) const {
			assert(m_size > 0);
			assert(index < m_capacity);
			assert(m_mapTbl != NULL);
			assert(m_mapTbl[index].key != NULL);
			assert(m_mapTbl[index].keyLen > 0);

			*itemAddr = m_mapTbl[index].key;
			return m_mapTbl[index].keyLen;
		}
	       /**
		* 查找一个指定前缀的串是否在字典中存在
		* @param prefixKey  查找的前缀串数据
		* @param offset     查找的前缀串起始位置
		* @param lengthLeft 可匹配的最大长度
		* @param result     OUT 查找字典项的结果
		* @return           是否能找到匹配的字典项
		*/
	       inline bool findKey(const byte* prefixKey, const uint& offset, const byte * end, MatchResult *result) const {
		       assert(m_compressionTrie != NULL);
		       assert(prefixKey != NULL);
		       return m_compressionTrie->searchDicItem(prefixKey + offset, end, result);
	       }

#ifdef NTSE_READABLE_DICTIONARY
		void buildDat(const char* path) throw(NtseException);
#else 
		void buildDat() throw(NtseException);
#endif
		/**
		 * 获得字典所属的表ID
		 */
		inline u16 getTblId() const { 
			return m_id; 
		}
		/**
		 * 获得字典实际字典项数
		 */
		inline uint size() const { 
			return m_size; 
		}
		/**
		 * 获得当前字典占用的内存大小
		 */
		inline u64 getMemUsage() const {
			return m_mtx->getMemUsage() + (u64)m_compressionTrie->getSerialSize();
		}
		/**
		 * 获得字典项平均长度
		 */
		inline double getDictItemAvgLen() const {
			if (m_size > 0) {
				double totalLen = 0;
				for (uint i = 0; i < m_size; i++)
					totalLen += m_mapTbl[i].keyLen;
				return totalLen / m_size;
			} else {
				return 0;
			}
		}
		/**
		 * 获得字典的容量
		 */
		inline uint capacity() const {
			return m_capacity;
		}
		/**
		 * 获得字典的双数组Trie树
		 */
		inline const TrieType * getCompressionTrie() const {
			return m_compressionTrie;
		}

		void setTableId(u16 tableId);

		static void create(const char *path, RCDictionary *dictionary) throw(NtseException);
		void close();
		static RCDictionary* open(const char* dicFilePath) throw(NtseException);
		static void drop(const char* dicFilepath) throw(NtseException);
		static RCDictionary* copy(RCDictionary *another, u16 newTblId);
		static bool compareDicItem(const DicItemType &x, const DicItemType &y);

		size_t culcMaxSerialSize() const;
		size_t write(byte *buf, size_t bufSize) throw(NtseException);
		static RCDictionary* read(const byte *buf, size_t size) throw(NtseException);

	private:
#ifdef NTSE_READABLE_DICTIONARY
		char getHexChar(int v) const;
		char* getHexString(const byte *ba, size_t size, char *str) const;
		void exportDictoFile(const char * path) const throw(NtseException);
#endif
		bool readDAT(const byte *trieData, const size_t& dataLen, const size_t& offset = 0);
		void verify() const;	
		static void codeInex(MemoryContext *mtx, uint index, byte** output, u8 *outputLen);

	private:
		u16		      m_id;			     /** 全局唯一的表ID */
		uint          m_capacity;        /** 字典容量 */
		uint          m_size;            /** 字典大小 */
		DicItemType   *m_mapTbl;         /** 字典索引-值映射表 */
		TrieType      *m_compressionTrie;/** 压缩用双数组Trie树 */
		MemoryContext *m_mtx;            /** 专用于本字典的内存分配上下文，主要是为了避免内存碎片 */
		File          *m_dictFile;       /** 字典文件 */
	};

	/**
	 * 用于帮助压缩的缓冲区
	 */
	class CompressBuf {
	public:
		/**
		 * 构造用于帮助压缩的缓冲区
		 */
		CompressBuf(byte *destData) : m_buildSize(1), m_destData(destData), m_flagData(destData), 
			m_flagCursor(0), m_2bitsUncodedLenData(NULL), m_4bitsUncodedLenData(NULL), m_2bitsCur(0), m_4bitsCur(0) {
				*m_flagData = 0;
		}

		/*
		 * 获得当前压缩数据长度
		 * @return 
		 */
		inline uint getBuildSize() const {
			return m_buildSize;
		}

		/**
		 * 写入未编码数据
		 * @param 未编码数据起始地址
		 * @param 未编码数据长度
		 */
		inline void writeUncodedData(const byte *uncodedData, const uint& unCodedLen) {
			assert(unCodedLen > 0);
			checkFlagData();
			if (unCodedLen <= UNCODED_LEN_2BITS_MAXNUM) {
				if (unlikely(m_2bitsCur > 3 || m_2bitsCur == 0)) {
					m_2bitsUncodedLenData = m_destData + m_buildSize;
					*m_2bitsUncodedLenData = 0;
					m_buildSize++;
					m_2bitsCur = 0;
				}
				*m_2bitsUncodedLenData |= (((unCodedLen - 1) & 0x03) << (m_2bitsCur << 1));
				m_2bitsCur++;
				writeFlag(m_flagData, m_flagCursor++, UNCODE_2BITS_FLAG);
			} else {
				assert(unCodedLen <= UNCODED_LEN_4BITS_MAXNUM);
				if (unlikely(m_4bitsCur > 1 || m_4bitsCur == 0)) {
					m_4bitsUncodedLenData = m_destData + m_buildSize;
					*m_4bitsUncodedLenData = 0;
					m_buildSize++;
					m_4bitsCur = 0;
				}
				*m_4bitsUncodedLenData |= (((unCodedLen - 5) & 0x0F) << (m_4bitsCur << 2));
				m_4bitsCur++;
				writeFlag(m_flagData, m_flagCursor++, UNCODE_4BITS_FLAG);
			}
			memcpy(m_destData + m_buildSize, uncodedData, unCodedLen);
			m_buildSize += unCodedLen;
		}

		/**
		 * 写入编码数据
		 * @param codedData 编码数据起始地址
		 * @param len       编码数据长度
		 */
		inline void writeCodedData(const byte *codedData, const uint& len) {
			assert(len == ONE_BYTE || len == TWO_BYTE);
			checkFlagData();
			if (len == ONE_BYTE) {
				writeFlag(m_flagData, m_flagCursor++, ONE_BYTE_FLAG);
				*(m_destData + m_buildSize++) = *codedData;				
			} else {
				assert(len == TWO_BYTE);
				writeFlag(m_flagData, m_flagCursor++, TWO_BYTE_FLAG);
				*(u16 *)(m_destData + m_buildSize) = *(u16 *)codedData;
				m_buildSize += len;
			}
		}

	private:
		/**
		 * 分配新的标志位字节
		 */
		inline void allocNewFlagData() {
			m_flagData = m_destData + m_buildSize;
			*m_flagData = 0;
			m_buildSize++;
			m_flagCursor = 0;
		}
		/** 
		 * 检查标志位字节游标，必要时分配新的标志位字节
		 */
		inline void checkFlagData() {
			if (unlikely(m_flagCursor >= 4))
				allocNewFlagData();
		}

		/** 
		 * 写入编码标志
		 * @param flagData   标志字节地址
		 * @param flagOffset 要写入的标志在标志字节中的下标
		 * @param flag       写入标志
		 */
		inline void writeFlag(byte *flagData, u8 flagOffset, CodedFlag flag) {
			*flagData |= ((u8)flag & 0x03) << (flagOffset << 1);
		}

	private:
		uint   m_buildSize;             /** 当前压缩数据长度 */
		byte   *m_destData;             /** 压缩数据输出起始地址 */
		byte   *m_flagData;             /** 当前标志位字节 */
		u8     m_flagCursor;            /** 当前标志位字节游标 */
		byte   *m_2bitsUncodedLenData;  /** 当前2bit未编码字节 */
		byte   *m_4bitsUncodedLenData;  /** 当前4bit未编码字节 */
		u8     m_2bitsCur;              /** 当前2bit未编码字节游标 */
		u8     m_4bitsCur;              /** 当前4bit未编码字节游标 */
	};

	/**
	 * 用于帮助解压缩的缓冲区
	 */
	class DecompressBuf {
	public:
		/**
		 * 构造一个解压缩缓冲区
		 * @param srcData 压缩数据地址
		 */
		DecompressBuf(const byte *srcData) : m_decodeSize(1), m_srcData(srcData), m_flagData(*srcData),  
			m_flagCursor(0), m_2bitsUncodedLenData(0), m_4bitsUncodedLenData(0), m_2bitsCur(0), m_4bitsCur(0) {
		}

		/** 
		 * 读取下一个编码标志
		 * @return 下一个编码标志
		 */
		inline CodedFlag readNextFlag() {
			checkFlagData();
			return readFlag(m_flagCursor++);
		}

		/**
		 * 获得当前解压缩数据长度
		 * @return 
		 */
		inline uint getDecodeSize() const {
			return m_decodeSize;		
		}

		/**
		 * 跳过指定长度压缩数据
		 * @param skipSize 跳过长度
		 */
		inline void skip(const uint& skipSize) {
			m_decodeSize += skipSize;
		}

		/** 
		 * 读取未编码数据长度
		 * @param  编码标志(UNCODE_2BITS_FLAG或UNCODE_4BITS_FLAG)
		 * @return 未编码数据长度
		 */
		inline uint readUncodedLen(CodedFlag codeFlag) {
			uint unCodedLen = 0;
			if (codeFlag == UNCODE_2BITS_FLAG) {
				if (unlikely(m_2bitsCur > 3 || m_2bitsCur == 0)) {
					m_2bitsUncodedLenData = *(m_srcData + m_decodeSize);
					m_decodeSize++;
					m_2bitsCur = 0;
				}
				unCodedLen = ((m_2bitsUncodedLenData >> (m_2bitsCur << 1)) & 0x03) + 1;
				m_2bitsCur++;
			} else {
				assert(codeFlag == UNCODE_4BITS_FLAG);
				if (unlikely(m_4bitsCur > 1 || m_4bitsCur == 0)) {
					m_4bitsUncodedLenData = *(m_srcData + m_decodeSize);
					m_decodeSize++;
					m_4bitsCur = 0;
				}
				unCodedLen = ((m_4bitsUncodedLenData >> (m_4bitsCur << 2)) & 0x0F) + 5;
				m_4bitsCur++;
			}			
			return unCodedLen;
		}

	private:
		/**
		 * 检查并移动当前标志字节
		 */
		inline void checkFlagData() {
			if (unlikely(m_flagCursor >= 4)) {
				m_flagCursor = 0;
				m_flagData = *(m_srcData + m_decodeSize);
				m_decodeSize++;
			}
		}

        /**
		 * 读取编码标志
		 * @param flagData   标志字节
		 * @param flagOffset 在标志字节中的下标
		 * @return           编码标志
		 */
		inline CodedFlag readFlag(u8 flagOffset) const {
			return (CodedFlag)((m_flagData >> (flagOffset << 1)) & 0x03);
		}

	private:
		uint        m_decodeSize;           /** 当前解压缩出来的数据长度 */
		const byte  *m_srcData;             /** 源压缩数据 */
		byte        m_flagData;            /** 当前标志位字节 */
		u8          m_flagCursor;           /** 当前标志位字节游标 */
		byte        m_2bitsUncodedLenData;  /** 当前2bit未编码长度字节 */
		byte        m_4bitsUncodedLenData;  /** 当前4bit未编码长度字节 */
		u8          m_2bitsCur;             /** 当前2bit未编码长度字节游标 */
		u8          m_4bitsCur;             /** 当前4bit未编码长度字节游标 */
	};

	/**
	* 记录压缩管理
	* 
	*/
	class RowCompressMng : public CmprssRecordExtractor {
	public:
		RowCompressMng(Database *db, const TableDef *tableDef, RCDictionary* dictionary);
		virtual ~RowCompressMng() {}

		/** 1、记录压缩解压缩接口 */
		double compressRecord(const CompressOrderRecord *src, Record* dest);
		void decompressRecord(const Record *src, CompressOrderRecord* dest);
		void compressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize);
		void decompressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize);
		u64 calcRcdDecompressSize(const Record *cprsRcd) const;
		u64 calcColGrpDecompressSize(const byte *src, const uint& offset, const uint& len) const;

		/** 2、字典管理接口 */
		void setTableId(u16 tableId) {
			assert(m_dictionary);
			m_dictionary->setTableId(tableId);
		}
		inline RCDictionary* getDictionary() const {
			return m_dictionary;
		}
		static void drop(const char* dicFilepath) throw(NtseException);
		static void create(const char *path, const RCDictionary *dictionary) throw(NtseException);
		static RowCompressMng* open(Database *db, const TableDef *tableDef, const char *path) throw(NtseException);
		void close();

	protected:
		void writeDicItem(const uint& index, byte *dst , uint* writeSize) const;
		void verifyCompress(const byte *src, const uint& srcLen, const byte *dest, const uint &destLen);
		void verifyRecord(const CompressOrderRecord *src, Record* dest);
	
	protected:
		Database			   *m_db;				/** 所属的数据库 */
		const TableDef 		   *m_tableDef;		    /** 所属的表定义 */
		RCDictionary           *m_dictionary;       /** 全局字典 */
	};
}

#endif