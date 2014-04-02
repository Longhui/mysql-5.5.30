/*
 * ��¼ѹ��
 *
 * @author ��ΰ��(liweizhao@corp.netease.com)
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

/** �ж��ļ������������Ƿ��д��� */
#define CHECK_FILE_ERR(errCode) (File::getNtseError(errCode) != File::E_NO_ERROR)

/** ���ڸ��ٵ��Լ�¼ѹ����ѹ�����̱��������ĺ� */
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
	 * �ò�ͬ�ֽڱ�ʾ��������ѹ������󳤶�
	 */
	enum CodedSegSize {
		ONE_BYTE_SEG_MAX_SIZE = 1 << 7, /** 1�ֽڱ�ʾ�Ķ���󳤶� */
		TWO_BYTE_SEG_MAX_SIZE = 1 << 15,/** 2�ֽڱ�ʾ�Ķ���󳤶� */
	};

	/**
	* ȫ���ֵ�
	* 
	* ȫ���ֵ䱣��Ϊһ�����飬�ֵ��������������±ꡣ����ȫ���ֵ仹��
	* ������֯��һ�־�̬˫����ṹ��SDAT�������Լ���Trie���Ŀռ俪��
	* ���Ҳ�Ӱ�����Ч�ʡ�
	*/
	class RCDictionary {
	public:
		const static uint DICTIONARY_PAGE_SIZE = 1024;   /** �����ֵ����ݵ��ڴ����������ҳ���С */
		const static uint DEFAULT_RESERVE_PAGES = 4;     /** �����ֵ����ݵ��ڴ���������ĵ�Ԥ��ҳ���� */

	public:
		typedef byte KeyType;
		typedef byte ValueType;
		//FIXME:�ֵ������󳤶Ȳ��ܳ���255���ֽڣ���������DicItemLenType�Ķ���Ҫ��
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
		 * �����ֵ���������ֵ����ֵ
		 * �ڴ�ʹ��Լ����ֱ��ʹ��ԭ�ڴ棬��������
		 * @param index    �ֵ�����
		 * @param itemAddr OUT�������ֵ���ĵ�ַ
		 * @return         �ֵ����
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
		* ����һ��ָ��ǰ׺�Ĵ��Ƿ����ֵ��д���
		* @param prefixKey  ���ҵ�ǰ׺������
		* @param offset     ���ҵ�ǰ׺����ʼλ��
		* @param lengthLeft ��ƥ�����󳤶�
		* @param result     OUT �����ֵ���Ľ��
		* @return           �Ƿ����ҵ�ƥ����ֵ���
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
		 * ����ֵ������ı�ID
		 */
		inline u16 getTblId() const { 
			return m_id; 
		}
		/**
		 * ����ֵ�ʵ���ֵ�����
		 */
		inline uint size() const { 
			return m_size; 
		}
		/**
		 * ��õ�ǰ�ֵ�ռ�õ��ڴ��С
		 */
		inline u64 getMemUsage() const {
			return m_mtx->getMemUsage() + (u64)m_compressionTrie->getSerialSize();
		}
		/**
		 * ����ֵ���ƽ������
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
		 * ����ֵ������
		 */
		inline uint capacity() const {
			return m_capacity;
		}
		/**
		 * ����ֵ��˫����Trie��
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
		u16		      m_id;			     /** ȫ��Ψһ�ı�ID */
		uint          m_capacity;        /** �ֵ����� */
		uint          m_size;            /** �ֵ��С */
		DicItemType   *m_mapTbl;         /** �ֵ�����-ֵӳ��� */
		TrieType      *m_compressionTrie;/** ѹ����˫����Trie�� */
		MemoryContext *m_mtx;            /** ר���ڱ��ֵ���ڴ���������ģ���Ҫ��Ϊ�˱����ڴ���Ƭ */
		File          *m_dictFile;       /** �ֵ��ļ� */
	};

	/**
	 * ���ڰ���ѹ���Ļ�����
	 */
	class CompressBuf {
	public:
		/**
		 * �������ڰ���ѹ���Ļ�����
		 */
		CompressBuf(byte *destData) : m_buildSize(1), m_destData(destData), m_flagData(destData), 
			m_flagCursor(0), m_2bitsUncodedLenData(NULL), m_4bitsUncodedLenData(NULL), m_2bitsCur(0), m_4bitsCur(0) {
				*m_flagData = 0;
		}

		/*
		 * ��õ�ǰѹ�����ݳ���
		 * @return 
		 */
		inline uint getBuildSize() const {
			return m_buildSize;
		}

		/**
		 * д��δ��������
		 * @param δ����������ʼ��ַ
		 * @param δ�������ݳ���
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
		 * д���������
		 * @param codedData ����������ʼ��ַ
		 * @param len       �������ݳ���
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
		 * �����µı�־λ�ֽ�
		 */
		inline void allocNewFlagData() {
			m_flagData = m_destData + m_buildSize;
			*m_flagData = 0;
			m_buildSize++;
			m_flagCursor = 0;
		}
		/** 
		 * ����־λ�ֽ��α꣬��Ҫʱ�����µı�־λ�ֽ�
		 */
		inline void checkFlagData() {
			if (unlikely(m_flagCursor >= 4))
				allocNewFlagData();
		}

		/** 
		 * д������־
		 * @param flagData   ��־�ֽڵ�ַ
		 * @param flagOffset Ҫд��ı�־�ڱ�־�ֽ��е��±�
		 * @param flag       д���־
		 */
		inline void writeFlag(byte *flagData, u8 flagOffset, CodedFlag flag) {
			*flagData |= ((u8)flag & 0x03) << (flagOffset << 1);
		}

	private:
		uint   m_buildSize;             /** ��ǰѹ�����ݳ��� */
		byte   *m_destData;             /** ѹ�����������ʼ��ַ */
		byte   *m_flagData;             /** ��ǰ��־λ�ֽ� */
		u8     m_flagCursor;            /** ��ǰ��־λ�ֽ��α� */
		byte   *m_2bitsUncodedLenData;  /** ��ǰ2bitδ�����ֽ� */
		byte   *m_4bitsUncodedLenData;  /** ��ǰ4bitδ�����ֽ� */
		u8     m_2bitsCur;              /** ��ǰ2bitδ�����ֽ��α� */
		u8     m_4bitsCur;              /** ��ǰ4bitδ�����ֽ��α� */
	};

	/**
	 * ���ڰ�����ѹ���Ļ�����
	 */
	class DecompressBuf {
	public:
		/**
		 * ����һ����ѹ��������
		 * @param srcData ѹ�����ݵ�ַ
		 */
		DecompressBuf(const byte *srcData) : m_decodeSize(1), m_srcData(srcData), m_flagData(*srcData),  
			m_flagCursor(0), m_2bitsUncodedLenData(0), m_4bitsUncodedLenData(0), m_2bitsCur(0), m_4bitsCur(0) {
		}

		/** 
		 * ��ȡ��һ�������־
		 * @return ��һ�������־
		 */
		inline CodedFlag readNextFlag() {
			checkFlagData();
			return readFlag(m_flagCursor++);
		}

		/**
		 * ��õ�ǰ��ѹ�����ݳ���
		 * @return 
		 */
		inline uint getDecodeSize() const {
			return m_decodeSize;		
		}

		/**
		 * ����ָ������ѹ������
		 * @param skipSize ��������
		 */
		inline void skip(const uint& skipSize) {
			m_decodeSize += skipSize;
		}

		/** 
		 * ��ȡδ�������ݳ���
		 * @param  �����־(UNCODE_2BITS_FLAG��UNCODE_4BITS_FLAG)
		 * @return δ�������ݳ���
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
		 * ��鲢�ƶ���ǰ��־�ֽ�
		 */
		inline void checkFlagData() {
			if (unlikely(m_flagCursor >= 4)) {
				m_flagCursor = 0;
				m_flagData = *(m_srcData + m_decodeSize);
				m_decodeSize++;
			}
		}

        /**
		 * ��ȡ�����־
		 * @param flagData   ��־�ֽ�
		 * @param flagOffset �ڱ�־�ֽ��е��±�
		 * @return           �����־
		 */
		inline CodedFlag readFlag(u8 flagOffset) const {
			return (CodedFlag)((m_flagData >> (flagOffset << 1)) & 0x03);
		}

	private:
		uint        m_decodeSize;           /** ��ǰ��ѹ�����������ݳ��� */
		const byte  *m_srcData;             /** Դѹ������ */
		byte        m_flagData;            /** ��ǰ��־λ�ֽ� */
		u8          m_flagCursor;           /** ��ǰ��־λ�ֽ��α� */
		byte        m_2bitsUncodedLenData;  /** ��ǰ2bitδ���볤���ֽ� */
		byte        m_4bitsUncodedLenData;  /** ��ǰ4bitδ���볤���ֽ� */
		u8          m_2bitsCur;             /** ��ǰ2bitδ���볤���ֽ��α� */
		u8          m_4bitsCur;             /** ��ǰ4bitδ���볤���ֽ��α� */
	};

	/**
	* ��¼ѹ������
	* 
	*/
	class RowCompressMng : public CmprssRecordExtractor {
	public:
		RowCompressMng(Database *db, const TableDef *tableDef, RCDictionary* dictionary);
		virtual ~RowCompressMng() {}

		/** 1����¼ѹ����ѹ���ӿ� */
		double compressRecord(const CompressOrderRecord *src, Record* dest);
		void decompressRecord(const Record *src, CompressOrderRecord* dest);
		void compressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize);
		void decompressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize);
		u64 calcRcdDecompressSize(const Record *cprsRcd) const;
		u64 calcColGrpDecompressSize(const byte *src, const uint& offset, const uint& len) const;

		/** 2���ֵ����ӿ� */
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
		Database			   *m_db;				/** ���������ݿ� */
		const TableDef 		   *m_tableDef;		    /** �����ı��� */
		RCDictionary           *m_dictionary;       /** ȫ���ֵ� */
	};
}

#endif