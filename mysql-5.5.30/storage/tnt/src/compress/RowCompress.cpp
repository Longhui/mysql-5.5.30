/**
* 记录压缩
*
* @author 李伟钊(liweizhao@corp.netease.com)
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
	/* 记录压缩全局字典相关                                                 */
	/************************************************************************/

	/**
	* 构造一个字典
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
	* 构造一个字典
	* @param tblId    所属表id
	* @param File     字典文件
	* @param capacity 字典的容量
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
	* 设置一对字典索引-值映射
	* @param index       字典项索引
	* @param itemData    字典项的值
	* @param itemDataLen 字典项的长度
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
	 * 验证生成字典的正确性
	 */
	void RCDictionary::verify() const {
		assert(m_compressionTrie != NULL);

		try {
			for (uint i = 0; i < m_size; i++) {
				MatchResult result;
				if (m_compressionTrie->searchDicItem(m_mapTbl[i].key, m_mapTbl[i].key + m_mapTbl[i].keyLen, &result)) {
					uint index = 0;//字典项索引
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
	* 构建静态双数组Trie树
	* @pre 所有的字典项都已经被添加
	*/
#ifdef NTSE_READABLE_DICTIONARY
	void RCDictionary::buildDat(const char *path) throw(NtseException) {
#else
	void RCDictionary::buildDat() throw(NtseException) {
#endif	
		assert(m_size > 0);
		assert(m_mapTbl != NULL);
		assert(m_compressionTrie == NULL);
		//检验字典项索引是连续的
		for (uint i = 0; i < m_size; i++) {
			if (m_mapTbl[i].key == NULL || m_mapTbl[i].keyLen == 0)
				NTSE_THROW(EL_ERROR, "Build global dictionary failed. The dictionary item of index %d hasn't been set.", i);
		}

		//将字典项存入文件中，只供调试
#ifdef NTSE_READABLE_DICTIONARY
		exportDictoFile(path);
#endif

		//由于需要对字典项进行排序，所以不能原来的数组上直接操作，而应该重新拷贝一份新的
		McSavepoint savePoint(m_mtx);
		DicItemType *tmpMapTbl = (DicItemType *)m_mtx->alloc(m_size * sizeof(DicItemType));
		assert(tmpMapTbl != NULL);
		for (uint i = 0; i < m_size; i++) {
			tmpMapTbl[i] = m_mapTbl[i];
		}

		//SDAT的构造要求先将字典项排好序
		sort(&tmpMapTbl[0], &tmpMapTbl[m_size], compareDicItem);

		//检查是否按照字典项排好序
		DicItemType *last = &tmpMapTbl[0];
		for (uint i = 1; i < m_size; i++) {
			assert(last != &tmpMapTbl[i]);
			assert(!compareDicItem(tmpMapTbl[i], *last));
			last = &tmpMapTbl[i];
		}

		//构建静态双数组Trie树
		BuilderType builder;
		try {
			builder.build(tmpMapTbl, tmpMapTbl + m_size);
			assert(builder.getLeavesStat() == m_size);
		} catch (exception &e) {
			NTSE_THROW(EL_ERROR, "Build global dictionary failed for reason: %s", e.what());
		}

		//进行序列化
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

		//验证Trie树的正确性
		verify();
	}

	/** 
	* 关闭字典
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
	 * 设置字典所属的表Id
	 * @pre 已经对表加元数据X锁和表数据X锁
	 * @param session
	 * @param tableId 新的表Id
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
	* 将字典持久化到一个全局字典文件
	*
	* @pre                 上层保证已经对表加写锁
	* @param path		   全局字典文件路径，含后缀
	* @param dictionary    要持久化的字典
	* @throw NtseException 文件无法创建，IO错误等等
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

			//计算序列化需要的空间
			size_t maxSize = dictionary->culcMaxSerialSize();

			serialData = (byte *)System::virtualAlloc(maxSize);
			if (!serialData) {
				NTSE_THROW(NTSE_EC_WRITE_FAIL, "Fail to alloc %d bytes buffer for writing " \
					"dictionary file %s.", maxSize, path);
			}
			//序列化
			u64 serialSize = dictionary->write(serialData, maxSize);

			errCode = dicFile->setSize((u64)serialSize);
			if (CHECK_FILE_ERR(errCode)) {
				NTSE_THROW(errCode, "Failed to extend global dictionary file size of %s.", path);
			}
			//写入文件
			errCode = dicFile->write(0, (u32)serialSize, serialData);
			if (CHECK_FILE_ERR(errCode)){
				NTSE_THROW(errCode, "Failed to write into global dictionary file %s.", path);
			} 
			//确保刷写到磁盘
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
	* 打开字典文件，并初始化全局字典
	* @param dicFilePath   全局字典完整路径, 含后缀
	* @thros NtseException 文件操作出错
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
			/** 全局字典文件通常不大，所以将其一次全部读入内存也无妨 */
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
	* 删除全局字典文件
	* @post 如果字典文件存在则从文件系统中删除，如果不存在则返回，不会报异常
	* @param dicFilePath 字典文件完整路径,含后缀
	* @throw NtseException 删除字典文件出错
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
	 * 拷贝字典
	 * @param another  要拷贝的字典
	 * @param newTblId 新字典对应的表的Id
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
	* 按字节比较两个字典项key的大小
	* 用途：构建双数组Trie树时需要先把字典项排好序
	* @param x 比较者1
	* @param y 比较者2
	* @param 如果x的key大于y的key，返回false，否则返回true
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
	* 对字典索引进行编码
	* @param mtx        字典的内存分配上下文
	* @param index	    字典索引
	* @param output     OUT 编码结果地址指针，由函数内部分配空间
	* @param outputLen  OUT 编码结果长度
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
	* 反序列化双数组Trie树
	* @param trieData 双数组Trie树的序列化数据
	* @param dataLen  序列化数据长度
	* @return bool    是否反序列化成功
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
	*  计算除双数组Trie树之外所需要的最大空间，
	*  用于序列化时分配内存空间
	*/
	size_t RCDictionary::culcMaxSerialSize() const {
		size_t maxSize = sizeof(*this);
		for (uint i = 0; i < m_size;i++) {
			maxSize += (m_mapTbl[i].keyLen + m_mapTbl[i].valueLen + 2 * sizeof(u8) + sizeof(u32));
		}
		maxSize += m_compressionTrie->getSerialSize();
		return maxSize;
	}

	/** 序列化全局字典
	* @param buf     序列化数据缓冲区，调用方分配
	* @param bufSize 序列化数据缓冲区大小
	* @return        序列化结果长度
	* @throw NtseException 序列化出错
	*/
	size_t RCDictionary::write(byte *buf, size_t bufSize) throw(NtseException) {
		Stream s(buf, bufSize);
		/** 序列化顺序为：表ID(2bytes) + 全局字典容量(4bytes) + 全局字典大小(4bytes) + 
		 * DAT长度(4bytes) + DAT数据(?bytes) + 字典索引(4bytes) + 字典项长度(1bytes) + 字典项(?bytes) + 
		 * 编码长度(1bytes) + 编码数据(?bytes) */
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

	/** 反序列化全局字典内容
	* @param buf 全局字典序列化结果
	* @param size 全局字典序列化结果大小
	* @return 全局字典，使用new分配
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
		bool initTrieRtn = dictionary->readDAT(trieSerialData, trieDataLen);//反序列化双数组Trie树
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
/* 记录压缩管理相关                                                     */
/************************************************************************/
	
	RowCompressMng::RowCompressMng(Database *db, const TableDef *tableDef, RCDictionary* dictionary): 
	m_db(db), m_tableDef(tableDef), m_dictionary(dictionary) {		
		assert(tableDef->m_rowCompressCfg);
	}

	/**
	 * 创建记录压缩管理
	 * @param dicFullPath   字典文件完整路径，含后缀
	 * @param dictionary    压缩字典
	 * @throw NtseException 文件操作出错
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
	 * 打开记录压缩管理
	 * @param db             所属数据库
	 * @param session		 会话
	 * @param tableDef       表定义
	 * @param rowCompressCfg 压缩配置
	 * @param path           字典路径， 不含后缀
	 */
	RowCompressMng* RowCompressMng::open(Database *db, const TableDef *tableDef, const char *path) throw(NtseException) {
		assert(db && path && tableDef);

		string basePath(path);
		string dicFilePath = basePath + Limits::NAME_GLBL_DIC_EXT;

		//打开字典文件，并初始化全局字典
		RCDictionary *dictionary = RCDictionary::open(dicFilePath.c_str());	
		RowCompressMng *rowCompressMng = new RowCompressMng(db, tableDef, dictionary);
		assert(rowCompressMng);
		return rowCompressMng;
	}

	/**
	 * 关闭记录压缩管理
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
	 * 删除全局字典文件
	 * @param dicFilepath 全局字典文件路径，含后缀
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

		//位图不压缩，直接拷贝
		memcpy(dest->m_data, src->m_data, m_tableDef->m_bmBytes);
		buildSize += m_tableDef->m_bmBytes;

		/* 分成各个段对记录进行压缩，压缩后各个段前面会有1~2字节记录段的长度，
		   如果压缩后的数据长度大于127，则用两个字节表示，否则用一个字节表示 */
		uint offset = m_tableDef->m_bmBytes;

		for (u16 i = 0; i < src->m_numSeg; i++) {
			uint compressedSegSize = 0;
			if (1 == src->m_numSeg) {
				//这里做了优化，在只有一个属性组的时候就不记录压缩后数据长度了
				assert(src->m_numSeg == m_tableDef->m_numColGrps);
				compressColGroup(src->m_data, offset, src->m_segSizes[i], dest->m_data + buildSize, &compressedSegSize);
				buildSize += compressedSegSize;
			} else {
				byte *lengthData = dest->m_data + buildSize;//压缩段长度信息起始地址
				byte *copyDest = lengthData + 1;//1字节能表示长度时压缩段数据地址
				compressColGroup(src->m_data, offset, src->m_segSizes[i], copyDest, &compressedSegSize);
				if (compressedSegSize < ONE_BYTE_SEG_MAX_SIZE) {
					//写入一字节长度
					*lengthData = (byte)compressedSegSize;
					buildSize += compressedSegSize + 1;
				} else {
					assert(compressedSegSize < TWO_BYTE_SEG_MAX_SIZE);
					//将压缩数据向后挪一个字节
					memmove(copyDest + 1, copyDest, compressedSegSize);
					//写入两字节长度
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
		//拷贝空值位图
		memcpy(dest->m_data, src->m_data, m_tableDef->m_bmBytes);
		buildSize += m_tableDef->m_bmBytes;

		//分别解压缩各个属性组
		uint offset = m_tableDef->m_bmBytes;
		for (u16 i = 0; i < m_tableDef->m_numColGrps; i++) {
			uint decompressSegSize = 0;
			uint length = 0;
			if (m_tableDef->m_numColGrps > 1) {//多个属性组
				u8 lenBytes = RecordOper::readCompressedColGrpSize(src->m_data + offset, &length);
				offset += lenBytes;
			} else {//只有一个属性组
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
		uint unCodedLen = 0;   //不能编码的数据长度
		uint unCodedOffset = 0;//不能编码数据的偏移量
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
			 * 由于经过改进的Trie树并不是所有的节点都是字典项，所以将不可避免地导致回溯
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
			uint index;//字典项索引
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
			uint index;//字典项索引
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

		//分别解压缩各个属性组
		uint offset = m_tableDef->m_bmBytes;
		for (u8 i = 0; i < m_tableDef->m_numColGrps; i++) {
			uint length = 0;
			if (m_tableDef->m_numColGrps > 1) {//多个属性组
				u8 lenBytes = RecordOper::readCompressedColGrpSize(cprsRcd->m_data + offset, &length);
				offset += lenBytes;
			} else {//只有一个属性组
				length = cprsRcd->m_size - m_tableDef->m_bmBytes;
			}
			rcdSize += calcColGrpDecompressSize(cprsRcd->m_data, offset, length);
			offset += length;
		}
		assert(offset == cprsRcd->m_size);
		return rcdSize;
	}

	/**
	 * 写出字典解码数据
	 * @param index 字典索引
	 * @param dst 解码数据输出地址
	 * @param buildSize 输入输出参数，输入为已解码输出数据长度，输出为本次解码后的总的输出数据长度
	 */
	inline void RowCompressMng::writeDicItem(const uint& index, byte *dst , uint* writeSize) const {
		const byte *dicValue;//字典项地址
		uint dicValueLen = m_dictionary->getDicItem(index, &dicValue);
		memcpy(dst, dicValue, dicValueLen);
		(*writeSize) = dicValueLen;
	}

	/**
	 * 验证压缩后的数据能正确解压缩
	 * @param src      未压缩数据的起始地址
	 * @param srcLen   未压缩数据的长度
	 * @param dest     要验证的压缩数据的起始地址
	 * @param destLen  要验证的压缩数据的长度
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