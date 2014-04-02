/**
* ��¼ѹ��
*
* @author ��ΰ��(liweizhao@corp.netease.com)
*/

#include <string>
#include <algorithm>
#include "compress/RowCompress.h"
#include "compress/RowCompressCoder.h"
#include "misc/TableDef.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "misc/RecordHelper.h"

namespace ntse {
	/************************************************************************/
	/* ��¼ѹ��ȫ���ֵ����                                                 */
	/************************************************************************/

	/**
	* ����һ���ֵ�
	*/
	RCDictionary::RCDictionary() {
		m_id = 0;
		m_capacity = 0;
		m_size = 0;
		m_mapTbl = NULL;
		m_compressionTrie = NULL;
		m_mtx = new MemoryContext(DICTIONARY_PAGE_SIZE, DEFAULT_RESERVE_PAGES);
		m_dictFile = NULL;
	}

	/**
	* ����һ���ֵ�
	* @param tblId    ������id
	* @param File     �ֵ��ļ�
	* @param capacity �ֵ������
	*/
	RCDictionary::RCDictionary(u16 tblId, File *dictFile, uint capacity): 
	m_id(tblId), m_capacity(capacity), m_dictFile(dictFile) {
		m_size = 0;
		m_compressionTrie = NULL;

		m_mtx = new MemoryContext(DICTIONARY_PAGE_SIZE, DEFAULT_RESERVE_PAGES);

		m_mapTbl = (DicItemType *)m_mtx->alloc(m_capacity * sizeof(DicItemType));
		for (uint i = 0; i < m_capacity; i++) {
			m_mapTbl[i].key = NULL;
			m_mapTbl[i].keyLen = 0;
			m_mapTbl[i].value = NULL;
			m_mapTbl[i].valueLen = 0;
		}
	}
	RCDictionary::~RCDictionary() {
		m_mtx->reset();
		delete m_mtx;
		m_mtx = NULL;

		assert(NULL == m_compressionTrie);
		assert(NULL == m_mapTbl);
		assert(NULL == m_dictFile);
	}

	/**
	* ����һ���ֵ�����-ֵӳ��
	* @param index       �ֵ�������
	* @param itemData    �ֵ����ֵ
	* @param itemDataLen �ֵ���ĳ���
	*/
	void RCDictionary::setDicItem(const uint& index, const byte* itemData, const DicItemLenType& itemDataLen) {
		assert(index < m_capacity);
		assert(itemData != NULL);
		assert(itemDataLen > 0);
		assert(m_mapTbl[index].key == NULL && m_mapTbl[index].keyLen == 0);
		assert(m_mapTbl[index].value == NULL && m_mapTbl[index].valueLen == 0);

		m_mapTbl[index].key = (KeyType *)m_mtx->alloc(itemDataLen * sizeof(KeyType));
		memcpy(m_mapTbl[index].key, itemData, itemDataLen * sizeof(KeyType));
		m_mapTbl[index].keyLen = itemDataLen;
		RCDictionary::codeInex(m_mtx, index, &m_mapTbl[index].value, &m_mapTbl[index].valueLen);
		assert(m_mapTbl[index].value != NULL);
		m_size++;
	}

	/**
	 * ��֤�����ֵ����ȷ��
	 */
	void RCDictionary::verify() const {
		assert(m_compressionTrie != NULL);

		try {
			for (uint i = 0; i < m_size; i++) {
				MatchResult result;
				if (m_compressionTrie->searchDicItem(m_mapTbl[i].key, m_mapTbl[i].key + m_mapTbl[i].keyLen, &result)) {
					uint index = 0;//�ֵ�������
					RCMIntegerConverter::decodeBytesToInt(result.value, 0, (CodedBytes)result.valueLen, &index);
					NTSE_ASSERT(index == i);
					NTSE_ASSERT(result.matchLen == m_mapTbl[i].keyLen);
					NTSE_ASSERT(result.valueLen == m_mapTbl[i].valueLen);
					NTSE_ASSERT(0 == memcmp(result.value, m_mapTbl[i].value, result.valueLen));
				} else {
					NTSE_ASSERT(false);
				}
			}
		} catch (exception &e) {
			fprintf(stderr, "%s", e.what());
			NTSE_ASSERT(false);
		}
	}

#ifdef NTSE_READABLE_DICTIONARY
	class OutFileCache {
	public:
		const static size_t OUT_FILE_MAX_CACHE_SIZE = 1 << 20;
		OutFileCache(File *file, size_t maxSize = OUT_FILE_MAX_CACHE_SIZE) : m_file(file), m_maxSize(maxSize), m_size(0) {
			m_cache = new char[m_maxSize];
			m_writeOffset = 0;
		}
		~OutFileCache() {
			delete [] m_cache;
			m_cache = NULL;
		}
		inline void write(char ch) throw(NtseException) {
			if (m_size >= m_maxSize)
				flush();
			*(m_cache + m_size) = ch;
			m_size++;
		}
		inline void write(uint i) throw(NtseException) {
			if (m_size + sizeof(uint) >= m_maxSize)
				flush();
			int r = System::snprintf(m_cache + m_size, m_maxSize - m_size, "%d", i);
			assert(r >= 0);
			m_size += r;
		}
		inline void write(char *out, size_t outSize) throw(NtseException) {
			assert(outSize <= m_maxSize);
			if (m_size + outSize >= m_maxSize)
				flush();	
			memcpy(m_cache + m_size, out, outSize);
			m_size += outSize;
		}

		inline void flush() throw(NtseException) {
			if (m_size > 0) {
				u64 errCode = File::E_NO_ERROR;
				errCode = m_file->getSize(&m_writeOffset);
				if (File::E_NO_ERROR != File::getNtseError(errCode))
					NTSE_THROW(errCode, "Failed to get size of file %s!", m_file->getPath());
				errCode = m_file->setSize(m_writeOffset + m_size);
				if (File::E_NO_ERROR != File::getNtseError(errCode))
					NTSE_THROW(errCode, "Failed to set size of file %s!", m_file->getPath());
				errCode = m_file->write(m_writeOffset, m_size, m_cache);
				if (File::E_NO_ERROR != File::getNtseError(errCode))
					NTSE_THROW(errCode, "Failed to write into file %s!", m_file->getPath());
				m_size = 0;
			}
		}

	private:
		const size_t   m_maxSize;
		File           *m_file;
		u64            m_writeOffset;
		char           *m_cache;
		size_t         m_size;
	};

	char RCDictionary::getHexChar(int v) const {
		assert(v >= 0 && v <= 15);
		if (v <= 9) {
			return (char)(v + '0');
		} else {
			return (char)(v + 'A' - 10);
		}
	}

	char* RCDictionary::getHexString(const byte *ba, size_t size, char *str) const {
		char *ptr = str;
		for (size_t i = 0; i < size; i++) {
			byte b = ba[i];
			if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9'))
				*ptr++ = (char)b;
			else if (b == '\n') {
				*ptr++ = '\\';
				*ptr++ = 'n';
			} else if (b == '\r') {
				*ptr++ = '\\';
				*ptr++ = 'r';
			} else if (b == '\t') {
				*ptr++ = '\\';
				*ptr++ = 't';
			} else if (b == '\b') {
				*ptr++ = '\\';
				*ptr++ = 'b';
			} else {
				*ptr++ = '\\';
				*ptr++ = getHexChar(b >> 4);
				*ptr++ = getHexChar(b & 0x0F);
			}
		}
		return ptr;
	}

	void RCDictionary::exportDictoFile(const char * path) const throw(NtseException) {
		char rowBuf[1024];
		string fullPath = string(path) + string("-readable.ndic");
		u64 errCode = File::E_NO_ERROR;
		File readableDicFile(fullPath.c_str());
		if (File::isExist(fullPath.c_str())) {
			errCode = readableDicFile.remove();
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				NTSE_THROW(errCode, "Failed to remove old readable dictionary file %s", readableDicFile.getPath());
			}
		}

		errCode = readableDicFile.create(false, false);
		if (File::E_NO_ERROR != File::getNtseError(errCode)) {
			NTSE_THROW(errCode, "Failed to create readable dictionary file %s!", readableDicFile.getPath());
		}
		OutFileCache outFileCache(&readableDicFile);

		for (uint i = 1; i < m_size; i++) {
			outFileCache.write('{');
			outFileCache.write(i);
			outFileCache.write(',');
			char *ptr = getHexString(m_mapTbl[i].key, m_mapTbl[i].keyLen, rowBuf);
			*ptr = '\0';
			outFileCache.write(rowBuf, strlen(rowBuf));
			outFileCache.write('}');
			outFileCache.write('\n');
		}
		outFileCache.flush();

		errCode = readableDicFile.close();
		if (File::E_NO_ERROR != File::getNtseError(errCode)) {
			NTSE_THROW(errCode, "Failed to close readable dictionary file %s!", readableDicFile.getPath());
		}
	}
#endif

	/** 
	* ������̬˫����Trie��
	* @pre ���е��ֵ���Ѿ������
	*/
#ifdef NTSE_READABLE_DICTIONARY
	void RCDictionary::buildDat(const char *path) throw(NtseException) {
#else
	void RCDictionary::buildDat() throw(NtseException) {
#endif	
		assert(m_size > 0);
		assert(m_mapTbl != NULL);
		assert(m_compressionTrie == NULL);
		//�����ֵ���������������
		for (uint i = 0; i < m_size; i++) {
			if (m_mapTbl[i].key == NULL || m_mapTbl[i].keyLen == 0)
				NTSE_THROW(EL_ERROR, "Build global dictionary failed. The dictionary item of index %d hasn't been set.", i);
		}

		//���ֵ�������ļ��У�ֻ������
#ifdef NTSE_READABLE_DICTIONARY
		exportDictoFile(path);
#endif

		//������Ҫ���ֵ�������������Բ���ԭ����������ֱ�Ӳ�������Ӧ�����¿���һ���µ�
		McSavepoint savePoint(m_mtx);
		DicItemType *tmpMapTbl = (DicItemType *)m_mtx->alloc(m_size * sizeof(DicItemType));
		assert(tmpMapTbl != NULL);
		for (uint i = 0; i < m_size; i++) {
			tmpMapTbl[i] = m_mapTbl[i];
		}

		//SDAT�Ĺ���Ҫ���Ƚ��ֵ����ź���
		sort(&tmpMapTbl[0], &tmpMapTbl[m_size], compareDicItem);

		//����Ƿ����ֵ����ź���
		DicItemType *last = &tmpMapTbl[0];
		for (uint i = 1; i < m_size; i++) {
			assert(last != &tmpMapTbl[i]);
			assert(!compareDicItem(tmpMapTbl[i], *last));
			last = &tmpMapTbl[i];
		}

		//������̬˫����Trie��
		BuilderType builder;
		try {
			builder.build(tmpMapTbl, tmpMapTbl + m_size);
			assert(builder.getLeavesStat() == m_size);
		} catch (exception &e) {
			NTSE_THROW(EL_ERROR, "Build global dictionary failed for reason: %s", e.what());
		}

		//�������л�
		uint trieSerialSize;
		byte* trieSerialData = NULL;
		builder.write(&trieSerialData, &trieSerialSize);
		assert(trieSerialData);

		m_compressionTrie = new TrieType();
		assert(m_compressionTrie != NULL);
		uint actualRead = m_compressionTrie->read(trieSerialData, 0, trieSerialSize);
		if (actualRead == 0) {
			delete []trieSerialData;
			trieSerialData = NULL;
			NTSE_THROW(EL_ERROR, "Build global dictionary failed because can't read DAT from serialized data.");
		}

		if (trieSerialData != NULL) {
			delete []trieSerialData;
			trieSerialData = NULL;
		}

		//��֤Trie������ȷ��
		verify();
	}

	/** 
	* �ر��ֵ�
	*/
	void RCDictionary::close() {
		m_mtx->reset();
		if (m_compressionTrie != NULL) {
			delete m_compressionTrie;
			m_compressionTrie = NULL;
		}
		m_mapTbl = NULL;
		if (m_dictFile) {
			u64 errCode = m_dictFile->close();
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				fprintf(stderr, "Closing compress dictionary file %s failed.", m_dictFile->getPath());
			}
			delete m_dictFile;
			m_dictFile = NULL;
		}
	}

	/**
	 * �����ֵ������ı�Id
	 * @pre �Ѿ��Ա��Ԫ����X���ͱ�����X��
	 * @param session
	 * @param tableId �µı�Id
	 */
	void RCDictionary::setTableId(u16 tableId) {
		m_id = tableId;
		assert(m_dictFile);
		try {
			u64 errCode;
			errCode = m_dictFile->write(0, sizeof(u16), &m_id);
			if (CHECK_FILE_ERR(errCode)){
				NTSE_THROW(errCode, "Failed to write into global dictionary file %s.", m_dictFile->getPath());
			} 
			errCode = m_dictFile->sync();
			if (CHECK_FILE_ERR(errCode)) {
				NTSE_THROW(errCode, "Failed to sync global dictionary file %s into disk.", m_dictFile->getPath());
			}
		} catch (NtseException &e) {
			UNREFERENCED_PARAMETER(e);
			NTSE_ASSERT(false);
		}
	}


	/**
	* ���ֵ�־û���һ��ȫ���ֵ��ļ�
	*
	* @pre                 �ϲ㱣֤�Ѿ��Ա��д��
	* @param path		   ȫ���ֵ��ļ�·��������׺
	* @param dictionary    Ҫ�־û����ֵ�
	* @throw NtseException �ļ��޷�������IO����ȵ�
	*/
	void RCDictionary::create(const char *path, RCDictionary *dictionary) throw(NtseException) {
		assert(NULL == dictionary->m_dictFile);
		File *dicFile = new File(path);

		byte *serialData = NULL;
		bool fileCreated = false;
		try {
			u64 errCode = dicFile->create(false, false);
			if (File::E_NO_ERROR != File::getNtseError(errCode)) {
				NTSE_THROW(errCode, "Cannot create global dictionary file %s.", path);
			}
			fileCreated= true;

			//�������л���Ҫ�Ŀռ�
			size_t maxSize = dictionary->culcMaxSerialSize();

			serialData = (byte *)System::virtualAlloc(maxSize);
			if (!serialData) {
				NTSE_THROW(NTSE_EC_WRITE_FAIL, "Fail to alloc %d bytes buffer for writing " \
					"dictionary file %s.", maxSize, path);
			}
			//���л�
			u64 serialSize = dictionary->write(serialData, maxSize);

			errCode = dicFile->setSize((u64)serialSize);
			if (CHECK_FILE_ERR(errCode)) {
				NTSE_THROW(errCode, "Failed to extend global dictionary file size of %s.", path);
			}
			//д���ļ�
			errCode = dicFile->write(0, (u32)serialSize, serialData);
			if (CHECK_FILE_ERR(errCode)){
				NTSE_THROW(errCode, "Failed to write into global dictionary file %s.", path);
			} 
			//ȷ��ˢд������
			errCode = dicFile->sync();
			if (CHECK_FILE_ERR(errCode)) {
				NTSE_THROW(errCode, "Failed to sync global dictionary file %s into disk.", path);
			}	
		} catch (NtseException &e) {
			if (serialData != NULL) {
				System::virtualFree(serialData);
				serialData = NULL;
			}
			if (dicFile) {
				dicFile->close();
				if (fileCreated)
					dicFile->remove();
				delete dicFile;
				dicFile = NULL;
			}
			throw e;
		}
		if (serialData != NULL) {
			System::virtualFree(serialData);
			serialData = NULL;
		}
		assert(dicFile);
		dictionary->m_dictFile = dicFile;
	}

	/** 
	* ���ֵ��ļ�������ʼ��ȫ���ֵ�
	* @param dicFilePath   ȫ���ֵ�����·��, ����׺
	* @thros NtseException �ļ���������
	*/
	RCDictionary* RCDictionary::open(const char* dicFilePath)  throw(NtseException) {
		File *dicFile = new File(dicFilePath);

		RCDictionary *dicCreate = NULL;
		u64 fileSize = 0;
		byte *readBuf = NULL;
		try {
			u64 errCode = dicFile->open(false);
			if (CHECK_FILE_ERR(errCode)) {
				delete dicFile;
				dicFile = NULL;
				NTSE_THROW(NTSE_EC_NO_DICTIONARY, "Cannot open table global dictionary file %s", dicFilePath);
			}
			/** ȫ���ֵ��ļ�ͨ���������Խ���һ��ȫ�������ڴ�Ҳ�޷� */
			dicFile->getSize(&fileSize);
			readBuf = (byte *)System::virtualAlloc((size_t)fileSize);
			if (!readBuf) {
				NTSE_THROW(NTSE_EC_READ_FAIL, "Fail to alloc %d bytes buffer for reading dictionary file %s.", 
					fileSize, dicFilePath);
			}
			errCode = dicFile->read(0, (u32)fileSize, readBuf);
			if (CHECK_FILE_ERR(errCode)) {
				NTSE_THROW(errCode, "Failed to read dictionary file %s", dicFilePath);
			}
			dicCreate = RCDictionary::read(readBuf, (size_t)fileSize);
			dicCreate->m_dictFile = dicFile;
		} catch(NtseException &e) {
			if (readBuf != NULL) {
				System::virtualFree(readBuf);
				readBuf = NULL;
			}
			if (dicFile) {
				dicFile->close();
				delete dicFile;
				dicFile = NULL;
			}
			if (dicCreate) {
				dicCreate->close();
				delete dicCreate;
				dicCreate = NULL;
			}
			throw e;
		}
		if (readBuf != NULL) {
			System::virtualFree(readBuf);
			readBuf = NULL;
		}
		assert(dicCreate);
		return dicCreate;
	}

	/**
	* ɾ��ȫ���ֵ��ļ�
	* @post ����ֵ��ļ���������ļ�ϵͳ��ɾ��������������򷵻أ����ᱨ�쳣
	* @param dicFilePath �ֵ��ļ�����·��,����׺
	* @throw NtseException ɾ���ֵ��ļ�����
	*/
	void RCDictionary::drop(const char* dicFilePath) throw(NtseException) {
		u64 errCode;
		File file(dicFilePath);
		errCode = file.remove();
		if (File::E_NOT_EXIST == File::getNtseError(errCode))
			return;
		if (File::E_NO_ERROR != File::getNtseError(errCode))
			NTSE_THROW(NTSE_EC_FILE_FAIL, "Cannot drop table global dictionary file %s", dicFilePath);
	}

	/**
	 * �����ֵ�
	 * @param another  Ҫ�������ֵ�
	 * @param newTblId ���ֵ��Ӧ�ı��Id
	 */
	RCDictionary* RCDictionary::copy(RCDictionary *another, u16 newTblId) {
		assert(another);
		RCDictionary *newDict = new RCDictionary(newTblId, NULL, another->capacity());
		MemoryContext *mtx = newDict->m_mtx;

		uint len = 0;
		uint size = another->size();
		newDict->m_mapTbl = (DicItemType *)mtx->alloc(size * sizeof(DicItemType));
		for (uint i = 0; i < size; i++) {
			newDict->m_mapTbl[i].keyLen = another->m_mapTbl[i].keyLen;
			len = another->m_mapTbl[i].keyLen * sizeof(KeyType);
			newDict->m_mapTbl[i].key = (KeyType *)mtx->alloc(len);
			memcpy(newDict->m_mapTbl[i].key, another->m_mapTbl[i].key, len);
			newDict->m_mapTbl[i].valueLen = another->m_mapTbl[i].valueLen;
			len = another->m_mapTbl[i].valueLen * sizeof(ValueType);
			newDict->m_mapTbl[i].value = (ValueType *)mtx->alloc(len);
			memcpy(newDict->m_mapTbl[i].value, another->m_mapTbl[i].value, len);
		}
		newDict->m_size = size;
		uint trieSerialDataLen;
		byte* trieSerialData = NULL;
		another->m_compressionTrie->getTrieData(&trieSerialData, &trieSerialDataLen);
		newDict->m_compressionTrie = new TrieType();
		uint rtn = newDict->m_compressionTrie->read(trieSerialData, 0, trieSerialDataLen);
		UNREFERENCED_PARAMETER(rtn);
		assert(rtn != 0);

		return newDict;
	}

	/**
	* ���ֽڱȽ������ֵ���key�Ĵ�С
	* ��;������˫����Trie��ʱ��Ҫ�Ȱ��ֵ����ź���
	* @param x �Ƚ���1
	* @param y �Ƚ���2
	* @param ���x��key����y��key������false�����򷵻�true
	*/
	bool RCDictionary::compareDicItem(const DicItemType &x, const DicItemType &y) {
		int minLen = min(x.keyLen, y.keyLen);
		for (int i = 0; i < minLen; i++) {
			if ((u8)x.key[i] > (u8)y.key[i]) {
				return false;
			} else if ((u8)x.key[i] < (u8)y.key[i]) {
				return true;
			}
		}
		return x.keyLen < y.keyLen;
	}

	/**
	* ���ֵ��������б���
	* @param mtx        �ֵ���ڴ����������
	* @param index	    �ֵ�����
	* @param output     OUT ��������ַָ�룬�ɺ����ڲ�����ռ�
	* @param outputLen  OUT ����������
	*/
	void RCDictionary::codeInex(MemoryContext *mtx, uint index, byte** output, u8 *outputLen){
		assert(index <= CODED_TWO_BYTE_MAXNUM);
		if (index <= CODED_ONE_BYTE_MAXNUM) {//0~255
			*output = (byte *)mtx->alloc(ONE_BYTE);
			RCMIntegerConverter::codeIntToCodedBytes(index, ONE_BYTE_FLAG, *output);
			*outputLen = ONE_BYTE;
		} else {//>=255
			*output = (byte *)mtx->alloc(TWO_BYTE);
			RCMIntegerConverter::codeIntToCodedBytes(index, TWO_BYTE_FLAG, *output);
			*outputLen = TWO_BYTE;
		} 
	}

	/** 
	* �����л�˫����Trie��
	* @param trieData ˫����Trie�������л�����
	* @param dataLen  ���л����ݳ���
	* @return bool    �Ƿ����л��ɹ�
	*/
	bool RCDictionary::readDAT(const byte *trieData, const size_t& dataLen, const size_t& offset) {
		assert(trieData != NULL);
		assert(m_compressionTrie == NULL);
		m_compressionTrie = new TrieType();
		if (m_compressionTrie->read(trieData, offset, dataLen) == 0) {
			return false;
		} else {
			return true;
		}
	}

	/** 
	*  �����˫����Trie��֮������Ҫ�����ռ䣬
	*  �������л�ʱ�����ڴ�ռ�
	*/
	size_t RCDictionary::culcMaxSerialSize() const {
		size_t maxSize = sizeof(*this);
		for (uint i = 0; i < m_size;i++) {
			maxSize += (m_mapTbl[i].keyLen + m_mapTbl[i].valueLen + 2 * sizeof(u8) + sizeof(u32));
		}
		maxSize += m_compressionTrie->getSerialSize();
		return maxSize;
	}

	/** ���л�ȫ���ֵ�
	* @param buf     ���л����ݻ����������÷�����
	* @param bufSize ���л����ݻ�������С
	* @return        ���л��������
	* @throw NtseException ���л�����
	*/
	size_t RCDictionary::write(byte *buf, size_t bufSize) throw(NtseException) {
		Stream s(buf, bufSize);
		/** ���л�˳��Ϊ����ID(2bytes) + ȫ���ֵ�����(4bytes) + ȫ���ֵ��С(4bytes) + 
		 * DAT����(4bytes) + DAT����(?bytes) + �ֵ�����(4bytes) + �ֵ����(1bytes) + �ֵ���(?bytes) + 
		 * ���볤��(1bytes) + ��������(?bytes) */
		s.write(m_id);
		s.write((u32)m_capacity);
		s.write((u32)m_size);

		uint trieSerialDataLen;
		byte* trieSerialData = NULL;
		m_compressionTrie->getTrieData(&trieSerialData, &trieSerialDataLen);
		assert(trieSerialData);
		s.write((u32)trieSerialDataLen);
		s.write(trieSerialData, trieSerialDataLen);
		for (uint i = 0; i < m_size; i++) {
			s.write((u32)i);
			s.write((u8)m_mapTbl[i].keyLen);
			s.write(m_mapTbl[i].key, m_mapTbl[i].keyLen);
			s.write((u8)m_mapTbl[i].valueLen);
			s.write(m_mapTbl[i].value, m_mapTbl[i].valueLen);
		}
		return s.getSize();
	}

	/** �����л�ȫ���ֵ�����
	* @param buf ȫ���ֵ����л����
	* @param size ȫ���ֵ����л������С
	* @return ȫ���ֵ䣬ʹ��new����
	*/
	RCDictionary* RCDictionary::read(const byte *buf, size_t size)  throw(NtseException) {
		if (size == 0) {
			NTSE_THROW(NTSE_EC_READ_FAIL, "Invalid dictionary file size : %d!", size);
		}

		Stream s((byte *)buf, size);
		u16 tblId;
		s.read(&tblId);
		u32 capacity;
		s.read(&capacity);
		u32 dicSize;
		s.read(&dicSize);

		RCDictionary *dictionary = new RCDictionary(tblId, NULL, capacity);
		dictionary->m_size = dicSize;

		uint trieDataLen;
		s.read((u32*)&trieDataLen);

		MemoryContext *mtx = dictionary->m_mtx;
		u64 savePoint = mtx->setSavepoint();

		byte *trieSerialData = (byte *)mtx->alloc(trieDataLen);
		s.readBytes(trieSerialData, trieDataLen);
		bool initTrieRtn = dictionary->readDAT(trieSerialData, trieDataLen);//�����л�˫����Trie��
		if (!initTrieRtn) {
			mtx->resetToSavepoint(savePoint);
			NTSE_THROW(NTSE_EC_READ_FAIL, "Failed to reserialize double array trie!");
		}
		mtx->resetToSavepoint(savePoint);

		for (uint i = 0; i < dicSize; i++) {
			u32 dicIndex;
			s.read(&dicIndex);
			assert(i == dicIndex);
			u8 dicItemKeyLen;
			s.read(&dicItemKeyLen);
			dictionary->m_mapTbl[dicIndex].key = (byte *)mtx->alloc(dicItemKeyLen);
			s.readBytes(dictionary->m_mapTbl[dicIndex].key, dicItemKeyLen);
			dictionary->m_mapTbl[dicIndex].keyLen = dicItemKeyLen;

			u8 dicItemValueLen;
			s.read(&dicItemValueLen);
			dictionary->m_mapTbl[dicIndex].value = (byte *)mtx->alloc(dicItemValueLen);
			s.readBytes(dictionary->m_mapTbl[dicIndex].value, dicItemValueLen);
			dictionary->m_mapTbl[dicIndex].valueLen = dicItemValueLen;
		}
		return dictionary;
	}


/************************************************************************/
/* ��¼ѹ���������                                                     */
/************************************************************************/
	
	RowCompressMng::RowCompressMng(Database *db, const TableDef *tableDef, RCDictionary* dictionary): 
	m_db(db), m_tableDef(tableDef), m_dictionary(dictionary) {		
		assert(tableDef->m_rowCompressCfg);
	}

	/**
	 * ������¼ѹ������
	 * @param dicFullPath   �ֵ��ļ�����·��������׺
	 * @param dictionary    ѹ���ֵ�
	 * @throw NtseException �ļ���������
	 */
	void RowCompressMng::create(const char *dicFullPath, const RCDictionary *dictionary) throw(NtseException) {
		assert(NULL != dictionary);

		bool dicFileCreated = false;
		try {
			RCDictionary *dic = const_cast<RCDictionary *>(dictionary);
			RCDictionary::create(dicFullPath, dic);
			dicFileCreated = true;
		} catch (NtseException &e) {
			if (dicFileCreated) {
				RCDictionary::drop(dicFullPath);
			}
			throw e;
		}
	}

	/**
	 * �򿪼�¼ѹ������
	 * @param db             �������ݿ�
	 * @param session		 �Ự
	 * @param tableDef       ����
	 * @param rowCompressCfg ѹ������
	 * @param path           �ֵ�·���� ������׺
	 */
	RowCompressMng* RowCompressMng::open(Database *db, const TableDef *tableDef, const char *path) throw(NtseException) {
		assert(db && path && tableDef);

		string basePath(path);
		string dicFilePath = basePath + Limits::NAME_GLBL_DIC_EXT;

		//���ֵ��ļ�������ʼ��ȫ���ֵ�
		RCDictionary *dictionary = RCDictionary::open(dicFilePath.c_str());	
		RowCompressMng *rowCompressMng = new RowCompressMng(db, tableDef, dictionary);
		assert(rowCompressMng);
		return rowCompressMng;
	}

	/**
	 * �رռ�¼ѹ������
	 *
	 */
	void RowCompressMng::close() {
		if (m_dictionary != NULL) {
			m_dictionary->close();
			delete m_dictionary;
			m_dictionary = NULL;
		}
	}

	/**
	 * ɾ��ȫ���ֵ��ļ�
	 * @param dicFilepath ȫ���ֵ��ļ�·��������׺
	 */
	void RowCompressMng::drop(const char* dicFilepath) throw(NtseException) {
		assert(dicFilepath);
		RCDictionary::drop(dicFilepath);
	}

	/**
	* @see CprsRecordExtrator
	*/
	double RowCompressMng::compressRecord(const CompressOrderRecord *src, Record* dest) {
		assert(src && src->m_format == REC_COMPRESSORDER);
		assert(dest && dest->m_format == REC_COMPRESSED);
		assert(src->m_segSizes && src->m_numSeg >= 1);

		uint buildSize = 0;
		dest->m_rowId = src->m_rowId;

		//λͼ��ѹ����ֱ�ӿ���
		memcpy(dest->m_data, src->m_data, m_tableDef->m_bmBytes);
		buildSize += m_tableDef->m_bmBytes;

		/* �ֳɸ����ζԼ�¼����ѹ����ѹ���������ǰ�����1~2�ֽڼ�¼�εĳ��ȣ�
		   ���ѹ��������ݳ��ȴ���127�����������ֽڱ�ʾ��������һ���ֽڱ�ʾ */
		uint offset = m_tableDef->m_bmBytes;

		for (u16 i = 0; i < src->m_numSeg; i++) {
			uint compressedSegSize = 0;
			if (1 == src->m_numSeg) {
				//���������Ż�����ֻ��һ���������ʱ��Ͳ���¼ѹ�������ݳ�����
				assert(src->m_numSeg == m_tableDef->m_numColGrps);
				compressColGroup(src->m_data, offset, src->m_segSizes[i], dest->m_data + buildSize, &compressedSegSize);
				buildSize += compressedSegSize;
			} else {
				byte *lengthData = dest->m_data + buildSize;//ѹ���γ�����Ϣ��ʼ��ַ
				byte *copyDest = lengthData + 1;//1�ֽ��ܱ�ʾ����ʱѹ�������ݵ�ַ
				compressColGroup(src->m_data, offset, src->m_segSizes[i], copyDest, &compressedSegSize);
				if (compressedSegSize < ONE_BYTE_SEG_MAX_SIZE) {
					//д��һ�ֽڳ���
					*lengthData = (byte)compressedSegSize;
					buildSize += compressedSegSize + 1;
				} else {
					assert(compressedSegSize < TWO_BYTE_SEG_MAX_SIZE);
					//��ѹ���������Ųһ���ֽ�
					memmove(copyDest + 1, copyDest, compressedSegSize);
					//д�����ֽڳ���
					lengthData[0] = (byte)((compressedSegSize >> 8) | 0x80);
					lengthData[1] = (byte)compressedSegSize;
					buildSize += compressedSegSize + 2;
				}
			}
			offset += src->m_segSizes[i];
		}
		dest->m_size = buildSize;
		assert(offset == src->m_size);

#ifdef NTSE_VERIFY_EX
		verifyRecord(src, dest);
#endif

		return ((double)dest->m_size) / src->m_size;
	}

	/**
	* @see CprsRecordExtrator
	*/
	void RowCompressMng::decompressRecord(const Record *src, CompressOrderRecord* dest) {
		assert(src && src->m_format == REC_COMPRESSED);
		assert(dest && dest->m_format == REC_COMPRESSORDER);

		dest->m_rowId = src->m_rowId;
		uint buildSize = 0;
		//������ֵλͼ
		memcpy(dest->m_data, src->m_data, m_tableDef->m_bmBytes);
		buildSize += m_tableDef->m_bmBytes;

		//�ֱ��ѹ������������
		uint offset = m_tableDef->m_bmBytes;
		for (u16 i = 0; i < m_tableDef->m_numColGrps; i++) {
			uint decompressSegSize = 0;
			uint length = 0;
			if (m_tableDef->m_numColGrps > 1) {//���������
				u8 lenBytes = RecordOper::readCompressedColGrpSize(src->m_data + offset, &length);
				offset += lenBytes;
			} else {//ֻ��һ��������
				length = src->m_size - m_tableDef->m_bmBytes;
			}
			decompressColGroup(src->m_data, offset, length, dest->m_data + buildSize, &decompressSegSize);
			offset += length;
			buildSize += decompressSegSize;
		}
		assert(offset == src->m_size);
		dest->m_size = buildSize;
	}

	/**
	* @see CprsRecordExtrator
	*/
	void RowCompressMng::compressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) {
		assert(src && dest);
		uint unCodedLen = 0;   //���ܱ�������ݳ���
		uint unCodedOffset = 0;//���ܱ������ݵ�ƫ����
		const byte *seekStart = src + offset;
		const byte *end = seekStart + len;
		uint i = 0;
		CompressBuf compressHelper(dest);
		MatchResult *rp;
		MatchResult result;
		MatchResult result2;
		
		traceCompressBegin();

		while (i < len) {
			/**
			 * ���ھ����Ľ���Trie�����������еĽڵ㶼���ֵ�����Խ����ɱ���ص��»���
			 */
			if (m_dictionary->findKey(seekStart, i, end, &result)) {
				rp = &result;
				if (unCodedLen > 0) {
					if (m_dictionary->findKey(seekStart, i + 1, end, &result2) && result2.matchLen > result.matchLen) {
						++unCodedLen;
						++i;
						rp = &result2;
					}
					traceUncoded(unCodedLen);
					compressHelper.writeUncodedData(seekStart + unCodedOffset, unCodedLen);
					unCodedLen = 0;
				}
				traceCoded(rp->matchLen, rp->valueLen);
				compressHelper.writeCodedData(rp->value, rp->valueLen);				
				i += rp->matchLen;
			} else {
				if (unCodedLen == 0)
					unCodedOffset = i;
				++unCodedLen;
				++i;
				if (unlikely(unCodedLen == UNCODED_LEN_4BITS_MAXNUM)) {
					traceUncoded(unCodedLen);
					compressHelper.writeUncodedData(seekStart + unCodedOffset, unCodedLen);
					unCodedLen = 0;
				}
			}

		}//end while
		if (unCodedLen > 0) {
			traceUncoded(unCodedLen);
			compressHelper.writeUncodedData(seekStart + unCodedOffset, unCodedLen);
			unCodedLen = 0;
		}
		
		traceCompressEnd();

		assert(i == len);
		*destSize = compressHelper.getBuildSize();
#ifdef NTSE_VERIFY_EX
		verifyCompress(src + offset, len, dest, *destSize);
#endif
	}

	/**
	 * @see CprsRecordExtrator
	 */
	void RowCompressMng::decompressColGroup(const byte *src, const uint& offset, const uint& len, byte *dest, uint *destSize) {
		assert(src && dest);
		uint skip = 0;
		uint buildSize = 0;
		uint writeSize = 0;
		uint unCodedLen = 0;
		const byte *segStart = src + offset;
		DecompressBuf decompressBuf(segStart);

		traceDecompressBegin();

		while (decompressBuf.getDecodeSize() < len) {
			uint index;//�ֵ�������
			CodedFlag flag = decompressBuf.readNextFlag();
			switch(flag) {
				case ONE_BYTE_FLAG:
					RCMIntegerConverter::decodeOneByteToInt(segStart, decompressBuf.getDecodeSize(), &index);
					writeDicItem(index, dest + buildSize, &writeSize);
					buildSize += writeSize;
					skip = ONE_BYTE;
					traceCoded(writeSize, 1);
					break;
				case TWO_BYTE_FLAG:
					RCMIntegerConverter::decodeTwoBytesToInt(segStart, decompressBuf.getDecodeSize(), &index);
					writeDicItem(index, dest + buildSize, &writeSize);
					buildSize += writeSize;
					skip = TWO_BYTE;
					traceCoded(writeSize, 2);
					break;
				case UNCODE_2BITS_FLAG:
				case UNCODE_4BITS_FLAG:
					unCodedLen = decompressBuf.readUncodedLen(flag);		
					assert(unCodedLen <= len - decompressBuf.getDecodeSize());
					memcpy(dest + buildSize, segStart + decompressBuf.getDecodeSize(), unCodedLen);
					buildSize += unCodedLen;
					skip = unCodedLen;
					traceUncoded(unCodedLen);
					break;
				default:
					assert(false);
					break;
			}//end switch
			decompressBuf.skip(skip);
		}//end while
		traceDecompressEnd();
		assert(decompressBuf.getDecodeSize() == len);
		(*destSize) += buildSize;
	}

	/**
	 * @see CprsRecordExtrator
	 */
	u64 RowCompressMng::calcColGrpDecompressSize(const byte *src, const uint& offset, const uint& len) const {
		uint skipLen = 0;
		uint buildSize = 0;
		uint unCodedLen = 0;

		const byte *dicValue;
		const byte *segStart = src + offset;
		DecompressBuf decompressBuf(segStart);

		while (decompressBuf.getDecodeSize() < len) {
			uint index;//�ֵ�������
			CodedFlag flag = decompressBuf.readNextFlag();
			switch(flag) {
				case ONE_BYTE_FLAG:
					RCMIntegerConverter::decodeOneByteToInt(segStart, decompressBuf.getDecodeSize(), &index);
					buildSize += m_dictionary->getDicItem(index, &dicValue);
					skipLen = ONE_BYTE;
					break;
				case TWO_BYTE_FLAG:
					RCMIntegerConverter::decodeTwoBytesToInt(segStart, decompressBuf.getDecodeSize(), &index);
					buildSize += m_dictionary->getDicItem(index, &dicValue);
					skipLen = TWO_BYTE;
					break;
				case UNCODE_2BITS_FLAG:
				case UNCODE_4BITS_FLAG:
					unCodedLen = decompressBuf.readUncodedLen(flag);
					buildSize += unCodedLen;
					skipLen = unCodedLen;
					break;
				default:
					assert(false);
					break;
			}//end switch
			decompressBuf.skip(skipLen);
		}//end while
		assert(decompressBuf.getDecodeSize() == len);
		return buildSize;
	}

	u64 RowCompressMng::calcRcdDecompressSize(const Record *cprsRcd) const {
		assert(cprsRcd->m_format == REC_COMPRESSED);

		u64 rcdSize = m_tableDef->m_bmBytes;

		//�ֱ��ѹ������������
		uint offset = m_tableDef->m_bmBytes;
		for (u8 i = 0; i < m_tableDef->m_numColGrps; i++) {
			uint length = 0;
			if (m_tableDef->m_numColGrps > 1) {//���������
				u8 lenBytes = RecordOper::readCompressedColGrpSize(cprsRcd->m_data + offset, &length);
				offset += lenBytes;
			} else {//ֻ��һ��������
				length = cprsRcd->m_size - m_tableDef->m_bmBytes;
			}
			rcdSize += calcColGrpDecompressSize(cprsRcd->m_data, offset, length);
			offset += length;
		}
		assert(offset == cprsRcd->m_size);
		return rcdSize;
	}

	/**
	 * д���ֵ��������
	 * @param index �ֵ�����
	 * @param dst �������������ַ
	 * @param buildSize �����������������Ϊ�ѽ���������ݳ��ȣ����Ϊ���ν������ܵ�������ݳ���
	 */
	inline void RowCompressMng::writeDicItem(const uint& index, byte *dst , uint* writeSize) const {
		const byte *dicValue;//�ֵ����ַ
		uint dicValueLen = m_dictionary->getDicItem(index, &dicValue);
		memcpy(dst, dicValue, dicValueLen);
		(*writeSize) = dicValueLen;
	}

	/**
	 * ��֤ѹ�������������ȷ��ѹ��
	 * @param src      δѹ�����ݵ���ʼ��ַ
	 * @param srcLen   δѹ�����ݵĳ���
	 * @param dest     Ҫ��֤��ѹ�����ݵ���ʼ��ַ
	 * @param destLen  Ҫ��֤��ѹ�����ݵĳ���
	 */
	void RowCompressMng::verifyCompress(const byte *src, const uint& srcLen, const byte *dest, const uint &destLen) {
#ifdef NTSE_VERIFY_EX
		if (vs.compress) {
			byte verifyBuf[Limits::MAX_REC_SIZE];
			uint verifyLen = 0;
			decompressColGroup(dest, 0, destLen, verifyBuf, &verifyLen);
			NTSE_ASSERT(verifyLen == srcLen);
			NTSE_ASSERT(memcmp(verifyBuf, src, verifyLen) == 0);
		} 
#else
		UNREFERENCED_PARAMETER(src);
		UNREFERENCED_PARAMETER(srcLen);
		UNREFERENCED_PARAMETER(dest);
		UNREFERENCED_PARAMETER(destLen);
#endif
	}

	void RowCompressMng::verifyRecord(const CompressOrderRecord *src, Record* dest) {
#ifdef NTSE_VERIFY_EX
		if (vs.compress) {
			CompressOrderRecord *verifyRcd = RecordBuilder::createEmptCompressOrderRcd(src->m_rowId, m_tableDef->m_maxRecSize, m_tableDef->m_numColGrps);
			decompressRecord(dest, verifyRcd);
			NTSE_ASSERT(RecordOper::isRecordEq(m_tableDef, src, verifyRcd));
			freeCompressOrderRecord(verifyRcd);
		} 
#else
		UNREFERENCED_PARAMETER(src);
		UNREFERENCED_PARAMETER(dest);
#endif
	}
}