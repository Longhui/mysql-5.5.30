/*
*      Static Double-Array Trie (DASTrie)
*
* Copyright (c) 2008, Naoaki Okazaki
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of the Northwestern University, University of Tokyo,
*       nor the names of its contributors may be used to endorse or promote
*       products derived from this software without specific prior written
*       permission.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
* A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
* OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
* EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
* PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
* PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
* LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
* NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/* $Id: dastrie.h 9 2008-11-10 03:40:33Z naoaki $ */

/*
This code assumes that elements of a vector are in contiguous memory,
based on the following STL defect report:
http://www.open-std.org/jtc1/sc22/wg21/docs/lwg-defects.html#69
*/

#ifndef __DASTRIE_H__
#define __DASTRIE_H__

#include <cstring>
#include <map>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>
#include "misc/Global.h"

#define DASTRIE_MAJOR_VERSION   1
#define DASTRIE_MINOR_VERSION   0
#define DASTRIE_COPYRIGHT       "Copyright (c) 2008 Naoaki Okazaki"

using namespace std;

namespace dastrie {
	/** 
	* \addtogroup dastrie_api DASTrie API
	* @{
	*
	*	The DASTrie API.
	*/

#define DFL_DOUBLE_ARRAY_TRAITS doublearray5_traits

	//字符表中的字符类型，使用两个字节表示
	typedef u16 char_type;

	/**
	* Global constants.
	*/
	enum {
		/// Invalid index number for a double array.
		INVALID_INDEX = 0,
		/// Initial index for a double array.
		INITIAL_INDEX = 1,
		/// Number of characters.
		NUM_CHARS = 257,
		/// The size of a chunk name
		CHUNK_NAME_SIZE = 4,
		/// The size of a chunk header.
		CHUNK_SIZE = 8,
		/// The size of a "SDAT" chunk.
		SDAT_CHUNKSIZE = 16,
		/// The terminate char of a dictionary item
		KEY_TERM = NUM_CHARS - 1,
	};

	class MatchResult {
	public:
		MatchResult() : matchLen(0), value(NULL), valueLen(0) { }
	public:
		uint       matchLen;         /** 字典项匹配长度 */
		const byte *value;           /** 字典编码值的地址 */
		u8         valueLen;         /** 字典编码值长度 */
	};  

	/**
	* Attributes and operations for a double array (5 bytes/element).
	*/
	struct doublearray5_traits
	{
		/// A type that represents an element of a base array.
		typedef s32 base_type;
		/// A type that represents an element of a check array.
		typedef char_type check_type;
		/// A type that represents an element of a double array.
		struct element_type
		{
			// BASE: v[0:4], CHECK: v[4]
			u8 v[6];
		};

		/// The chunk ID.
		inline static const char *chunk_id()
		{
			static const char *id = "SDA5";
			return id;
		}

		/// Gets the minimum number of BASE values.
		inline static base_type min_base()
		{
			return 1;
		}

		/// Gets the maximum number of BASE values.
		inline static base_type max_base()
		{
			return 0x7FFFFFFF;
		}

		/// The default value of an element.
		inline static element_type default_value()
		{
			static const element_type def = {{0, 0, 0, 0, 0, 0}};
			return def;
		}

		/// Gets the BASE value of an element.
		inline static base_type get_base(const element_type& elem)
		{
			return *(base_type *)(elem.v);
		}

		/// Gets the CHECK value of an element.
		inline static check_type get_check(const element_type& elem)
		{
			return *(check_type *)(elem.v + 4);
		}

		/// Sets the BASE value of an element.
		inline static void set_base(element_type& elem, base_type v)
		{
			*(base_type *)(elem.v) = v;
		}

		/// Sets the CHECK value of an element.
		inline static void set_check(element_type& elem, check_type v)
		{
			*(check_type *)(elem.v + 4) = v;
		}
	};

	/**
	* An unextendable array.
	*  @param  value_tmpl  The element type to be stored in the array.
	*/
	template <class value_tmpl>
	class array
	{
	public:
		/// The type that represents elements of the array.
		typedef value_tmpl value_type;
		/// The type that represents the size of the array.

	protected:
		value_type* m_block;
		uint		m_size;
		bool        m_own;

	public:
		/// Constructs an array.
		array()
			: m_block(NULL), m_size(0), m_own(false)
		{
		}

		/// Constructs an array from an existing memory block.
		array(value_type* block, uint size, bool own = false)
			: m_block(NULL), m_size(0), m_own(false)
		{
			assign(block, size, own);
		}

		/// Constructs an array from another array instance.
		array(const array& rho)
			: m_block(NULL), m_size(0), m_own(false)
		{
			assign(rho.m_block, rho.m_size, rho.m_own);
		}

		/// Destructs an array.
		virtual ~array()
		{
			free();
		}

		/// Assigns the new array to this instance.
		array& operator=(const array& rho)
		{
			assign(rho.m_block, rho.m_size, rho.m_own);
			return *this;
		}

		bool operator==(const array& another) const {
			if (m_own != another.m_own)
				return false;
			if (m_size != another.m_size)
				return false;
			if (0 != memcmp(m_block, another.m_block, sizeof(value_type) * m_size))
				return false;
			return true;
		}

		/// Obtains a read/write access to an element in the array.
		inline value_type& operator[](uint i)
		{
			return m_block[i];
		}

		/// Obtains a read-only access to an element in the array.
		inline const value_type& operator[](uint i) const
		{
			return m_block[i];
		}

		/// Checks whether an array is allocated.
		inline operator bool() const
		{
			return (m_block != NULL);
		}

		/// Reports the size of the array.
		inline uint size() const
		{
			return m_size;
		}

		/// Assigns a new array from an existing memory block.
		inline void assign(value_type* block, uint size, bool own = false)
		{
			free();

			if (own) {
				// Allocate a memory block and copy the source array to the block.
				m_block = new value_type[size];
				std::memcpy(m_block, block, sizeof(value_type) * size);
			} else {
				// Just store the pointer to the source array.
				m_block = block;
			}
			m_size = size;
			m_own = own;
		}

		/// Destroy the array.
		inline void free()
		{
			// Free the memory block only when it was allocated by this class.
			if (m_own) {
				delete[] m_block;
			}
			m_block = NULL;
			m_size = 0;
			m_own = false;
		}
	};

	/**
	* A writer class for a tail array.
	*/
	class otail
	{
	public:
		/// The type that represents an element of a tail array.
		typedef u8 element_type;
		/// The container for the tail array.
		typedef std::vector<element_type> container_type;

	protected:
		/// The tail array.
		container_type m_cont;

	public:
		/**
		* Constructs an instance.
		*/
		otail()
		{
		}

		/**
		* Destructs an instance.
		*/
		virtual ~otail()
		{
		}

		/**
		* Obtains a read-only access to the pointer of the tail array.
		*  @return const element_type* The pointer to the tail array.
		*/
		inline const element_type* block() const
		{
			return &m_cont[0];
		}

		/**
		* Reports the size of the tail array.
		*  @return uint   The size, in bytes, of the tail array.
		*/
		inline uint bytes() const
		{
			return sizeof(element_type) * m_cont.size();
		}

		/**
		* Reports the offset position to which a next data is written.
		*  @return uint   The current position.
		*/
		inline uint tellp() const
		{
			return this->bytes();
		}

		/**
		* Removes all of the contents in the tail array.
		*/
		inline void clear()
		{
			m_cont.clear();
		}

		/**
		* Puts a byte stream to the tail array.
		*  @param  data        The pointer to the byte stream.
		*  @param  size        The size, in bytes, of the byte stream.
		*  @return otail&      The reference to this object.
		*/
		inline otail& write(const void *data, size_t size)
		{
			uint offset = this->bytes();
			if (0 < size) {
				m_cont.resize(offset + size);
				std::memcpy(&m_cont[offset], data, size);
			}
			return *this;
		}

		/**
		* Puts a value of a basic type to the tail array.
		*  @param  value       The reference to the value.
		*  @return otail&      The reference to this object.
		*/
		template <typename value_type>
		inline otail& write(const value_type& value)
		{
			return write(&value, sizeof(value));
		}

		template <typename value_type>
		inline otail& write_string(const byte* str, value_type len, value_type offset = 0) 
		{
			return write(str + offset, len);
		}

		inline otail& operator<<(char v)            { return write(v); }
		inline otail& operator<<(bool v)            { return write(v); }
		inline otail& operator<<(short v)           { return write(v); }
		inline otail& operator<<(unsigned short v)  { return write(v); }
		inline otail& operator<<(int v)             { return write(v); }
		inline otail& operator<<(unsigned int v)    { return write(v); }
		inline otail& operator<<(long v)            { return write(v); }
		inline otail& operator<<(unsigned long v)   { return write(v); }
		inline otail& operator<<(float v)           { return write(v); }
		inline otail& operator<<(double v)          { return write(v); }
		inline otail& operator<<(long double v)     { return write(v); }
	};

	/**
	* A reader class for a tail array.
	*/
	class itail
	{
	public:
		/// The type that represents an element of a tail array.
		typedef u8 element_type;
		/// The container for the tail array.
		typedef array<element_type> container_type;

	protected:
		/// The tail array.
		container_type m_cont;

	public:
		/**
		* Constructs an instance.
		*/
		itail()
		{
		}

		/**
		* Destructs an instance.
		*/
		virtual ~itail()
		{
		}

		/**
		* Checks whether a tail array is allocated.
		*  @return bool        \c true if allocated, \c false otherwise.
		*/
		inline operator bool() const
		{
			return m_cont;
		}

		inline bool operator == (const itail &another) const {
			return m_cont == another.m_cont;
		}

		/**
		* Initializes the tail array from an existing memory block.
		*  @param  ptr         The pointer to the memory block of the source.
		*  @param  size        The size of the memory block of the source.
		*  @param  own         \c true to copy the content of the source to a
		*                      new memory block managed by this instance.
		*/
		void assign(const element_type* ptr, uint size, bool own = false)
		{
			m_cont.assign(const_cast<element_type*>(ptr), size, own);
		}

		/**
		* Exact match for the string from the current position.
		*  @param  str         The pointer to the string to be compared.
		*  @return bool        \c true if the string starting from the current
		*                      position is identical to the give string str;
		*                      \c false otherwise.
		*/
		inline bool match_string(const uint& offset, const byte *str, const uint& len)
		{
			if (offset + len <= m_cont.size()) {
				if (std::memcmp(&m_cont[offset], str, len) == 0) {
					return true;
				}
			}
			return false;
		}

		/**
		* Prefix match for the string from the current position.
		*  @param  str         The pointer to the string to be compared.
		*  @return bool        \c true if the give string str begins with the
		*                      substring starting from the current position;
		*                      \c false otherwise.
		*/
		inline bool match_string_partial(const uint& offset, const uint& lengthLeftInTail, const byte *str)
		{
			if (unlikely(offset + lengthLeftInTail > m_cont.size())) 
				return false;
			return std::memcmp(&m_cont[offset], str, lengthLeftInTail) == 0;
		}

		/**
		* Gets a byte stream to the tail array.
		*  @param[out] data    The pointer to the byte stream to receive.
		*  @param  size        The size to read.
		*  @return itail&      The reference to this object.
		*/
		inline itail& read(const uint& offset, void *data, size_t size)
		{
			assert(offset + size <= m_cont.size());
			std::memcpy(data, &m_cont[offset], size);
			return *this;
		}

		/**
		* Gets a value of a basic type from the tail array.
		*  @param[out] value   The reference to the value.
		*  @return itail&      The reference to this object.
		*/
		template <typename value_type>
		inline itail& read(const uint& offset, value_type* value) 
		{
			return read(offset, value, sizeof(value_type));
		}

		inline u8 readLength(const uint& offset)
		{
			return (u8)m_cont[offset];
		}

		inline const byte* readValue(const uint& offset)  
		{
			return (byte*)&m_cont[offset];
		}

		/*
		inline itail& operator>>(bool& v)           { return read(v); }
		inline itail& operator>>(short& v)          { return read(v); }
		inline itail& operator>>(unsigned short& v) { return read(v); }
		inline itail& operator>>(int& v)            { return read(v); }
		inline itail& operator>>(unsigned int& v)   { return read(v); }
		inline itail& operator>>(long& v)           { return read(v); }
		inline itail& operator>>(unsigned long& v)  { return read(v); }
		inline itail& operator>>(float& v)          { return read(v); }
		inline itail& operator>>(double& v)         { return read(v); }
		inline itail& operator>>(long double& v)    { return read(v); }
		inline itail& operator>>(byte *& str)
		{
		str = (byte*)&m_cont[m_offset];
		return *this;
		}
		*/
	};



	/**
	* Double Array Trie (read-only).
	*
	*  @param  value_tmpl          A type that represents a record value.
	*  @param  length_impl		   Type of the value length and the key length
	*  @param  doublearray_traits  A class in which various properties of
	*                              double-array elements are described.
	*/
	template <class value_tmpl, 
	class length_impl = u8, 
	class doublearray_traits = DFL_DOUBLE_ARRAY_TRAITS>
	class trie
	{
	public:
		/// A type that represents a record value.
		typedef value_tmpl value_type;
		/// A type that represents an element of a double array.
		typedef typename doublearray_traits::element_type element_type;
		/// A type that represents a base value in a double array.
		typedef typename doublearray_traits::base_type base_type;
		/// A type that represents a check value in a double array.
		typedef typename doublearray_traits::check_type check_type;

		/// A type that implements a container of double-array elements.
		typedef array<element_type> doublearray_type;
		/// A type that represents a size.
		typedef length_impl length_type;

		/**
		* Exception class.
		*/
		class exception : public std::runtime_error
		{
		public:
			/**
			* Constructs an instance.
			*  @param  msg     The error message.
			*/
			explicit exception(const std::string& msg)
				: std::runtime_error(msg)
			{
			}
		};

	protected:
		byte* m_block;
		uint m_blockSize;
		char_type m_table[NUM_CHARS];
		doublearray_type m_da;
		itail m_tail;
		uint m_n;
		base_type m_startBase;

	public:
		/**
		* Constructs an instance.
		*/
		trie()
		{
			m_block = NULL;
			m_blockSize = 0;

			// Initialize the character table.
			for (int i = 0;i < NUM_CHARS;++i) {
				m_table[i] = (char_type)i;
			}
		}

		/**
		* Destructs an instance.
		*/
		virtual ~trie()
		{
			if (m_block != NULL) {
				delete[] m_block;
				m_block = NULL;
			}
		}

		/**
		* Gets the number of records in the trie.
		*  @return uint   The number of records.
		*/
		inline uint size() const
		{
			return m_n;
		}

		/**
		* 获得序列化后的大小
		*
		*/
		inline uint getSerialSize() const
		{
			return m_blockSize;
		}

		/**
		* Assigns a double-array trie from a builder.
		*  @param  da              The vector of double-array elements.
		*  @param  tail            The tail array.
		*  @param  table           The character-mapping table.
		*/
		void assign(
			const std::vector<element_type>& da,
			const otail& tail,
			const u8* table
			)
		{
			m_da.assign(const_cast<element_type*>(&da[0]), da.size(), true);
			m_tail.assign(tail.block(), tail.bytes(), true);
			for (int i = 0;i < NUM_CHARS;++i) {
				m_table[i] = table[i];
			}
		}

		/**
		* 从某个位置开始查找字典项
		* @param start 查找开始位置
		* @param maxLen 查找串的最大长度
		* @param result OUT 查找结果，包含匹配长度，字典项的值，字典项值的长度
		* @param 是否能找到匹配的字典项
		*/
		inline bool searchDicItem(const byte *start, const byte *end, MatchResult *result) {
			assert(start != NULL);
			/**
			* 在沿着到子节点的路径往下查找时，可能某个子孙节点就是字典项，
			* 但是可能会存在更长的字典项(在Trie树中的层次更深), 所以找到字典项时不是立即返回，
			* 而是继续往下查找，除非已经不存在匹配的子节点，最后获得的字典项
			* 将是匹配的路径上层次最深的节点，字典项的值保存在result的value属性中，字典项的
			* 值长度保存在result的valueLen属性中
			*/
			result->matchLen = 0;
			uint valueOffset = 0;

			base_type curBase = m_startBase;
			const byte *cursor = start;
			do {
				curBase = descend(curBase, *cursor++);
				if (likely(curBase > 0)) {
					base_type term = descend(curBase, KEY_TERM);
					if (term < 0) {
						valueOffset = -term + sizeof(length_type);
						result->matchLen = cursor - start;
					}
				} else if (curBase < 0) {
					uint offset = -curBase;
					length_type lt = m_tail.readLength(offset);
					if (lt <= end - cursor) {
						offset += sizeof(length_type);
						if (0 == lt || m_tail.match_string_partial(offset, lt, cursor)) {
							offset += lt;
							valueOffset = offset;
							result->matchLen = cursor - start + lt;
						}
					}
					break;
				} else
					break;
			} while (cursor != end);

			if (valueOffset > 0) {
				assert(valueOffset > 0);
				result->valueLen = m_tail.readLength(valueOffset);
				assert(result->valueLen > 0);
				valueOffset += sizeof(length_type);
				result->value = m_tail.readValue(valueOffset);
				return true;
			} else
				return false;
		}

		bool operator ==(const trie &another) const {
			if (m_blockSize != another.m_blockSize)
				return false;
			if (0 != memcmp(m_block, another.m_block, m_blockSize))
				return false;
			if (0 != memcmp(m_table, another.m_table, NUM_CHARS * sizeof(char_type)))
				return false;
			if (!(m_da == another.m_da))
				return false;
			if (!(m_tail == another.m_tail))
				return false;
			if (m_n != another.m_n)
				return false;
			return true;
		}

		inline bool operator !=(const trie &another) const {
			return ! (*this == another);
		}

	public:
		/**
		* 移动到子节点
		* @pre    当前节点不是叶子节点
		* @param  curBase 当前节点的base值
		* @param  c 子节点的键值
		* @return 子节点的base值
		*/
		inline base_type descend(const base_type &curBase, const char_type &c) const
		{
			assert(curBase > 0);
			uint next = curBase + m_table[c] + 1;
			if (likely(m_da.size() <= next || // Outside of the double array.
				get_check(next) != m_table[c])) {
					return 0;
			}
			return get_base(next);
		}

		inline base_type get_base(uint i) const
		{
			return doublearray_traits::get_base(m_da[i]);
		}

		inline check_type get_check(uint i) const
		{
			return doublearray_traits::get_check(m_da[i]);
		}

	public:
		uint read(const byte *buf, const uint& offset, const uint &size)
		{
			char chunk[CHUNK_NAME_SIZE];
			uint total_size;
			u8 data[CHUNK_SIZE];

			// Read CHUNKSIZE bytes.
			std::memcpy(data, buf + offset, CHUNK_SIZE);

			// Parse the data as a chunk.
			read_chunk(data, chunk, total_size);

			if(size < total_size)
				return 0;

			// Make sure that the data is a "SDAT" chunk.
			if (std::strncmp(chunk, "SDAT", CHUNK_NAME_SIZE) != 0) {
				return 0;
			}

			// Allocate a new memory block and copy the data.
			m_blockSize = total_size;
			m_block = new byte[total_size];
			std::memcpy(m_block, data, CHUNK_SIZE);

			// Read the actual data.
			std::memcpy(m_block + CHUNK_SIZE, buf + offset + CHUNK_SIZE,total_size - CHUNK_SIZE);

			// Allocate the trie.
			uint used_size = assign(m_block, total_size);
			if (used_size != total_size) {
				return 0;
			}

			m_startBase = get_base(INITIAL_INDEX);
			assert(m_startBase > 0);

			return used_size;
		}

		/** 获得双数组Trie树数据地址
		*  @param size 双数组Trie树的数据长度
		*  @return     双数组Trie树的数据起始地址
		*/
		void getTrieData(byte ** data, uint *size) {
			assert(m_block != NULL);
			assert(m_blockSize > 0);
			*data = m_block;
			*size = m_blockSize;
		}

	protected:
		/**
		* Assigns a double-array trie from a memory image.
		*  @param  block           The pointer to the memory block.
		*  @param  size            The size, in bytes, of the memory block.
		*  @return uint       If successful, the size, in bytes, of the
		*                          memory block used to read a double-array trie;
		*                          otherwise zero.
		*/
		uint assign(const byte *block, uint size)
		{
			char chunk[CHUNK_NAME_SIZE];
			uint value, sdat_size, total_size;
			const u8* p = reinterpret_cast<const u8*>(block);

			// The size of the memory block must not be smaller than SDAT_CHUNKSIZE.
			if (size < SDAT_CHUNKSIZE) {
				return 0;
			}

			// Read the "SDAT" chunk.
			p += read_chunk(p, chunk, total_size);
			if (std::strncmp(chunk, "SDAT", CHUNK_NAME_SIZE) != 0) {
				return 0;
			}

			// Check the size of the "SDAT" chunk.
			p += read_length(p, sdat_size);
			if (sdat_size != SDAT_CHUNKSIZE) {
				return 0;
			}

			// Read the number of records in the trie.
			p += read_length(p, value);
			m_n = value;

			// Loop for child chunks.
			const byte* last = reinterpret_cast<const byte*>(block) + total_size;
			while (p < last) {
				uint size;
				const byte* q = p;
				q += read_chunk(q, chunk, size);
				uint datasize = size - CHUNK_SIZE;

				if (strncmp(chunk, "TBLU", CHUNK_NAME_SIZE) == 0) {
					// "TBLU" chunk.
					uint tblSize = sizeof(char_type) * NUM_CHARS;
					if (datasize == tblSize) {
						char_type *arr = (char_type *)q;
						for (int i = 0;i < NUM_CHARS;++i) {
							m_table[i] = arr[i];
						}
					}
				} else if (strncmp(chunk, doublearray_traits::chunk_id(), CHUNK_NAME_SIZE) == 0) {
					// "SDA4" or "SDA5" chunk.
					m_da.assign((element_type*)q, datasize / sizeof(element_type));

				} else if (strncmp(chunk, "TAIL", CHUNK_NAME_SIZE) == 0) {
					// "TAIL" chunk.
					m_tail.assign(q, datasize);
				}

				p += size;
			}

			// Make sure that arrays are allocated successfully.
			if (!m_da || !m_tail) {
				return 0;
			}

			return total_size;
		}
		uint read_length(const u8* block, uint& value)
		{
			return read_data(block, &value, sizeof(value));
		}

		uint read_data(const u8* block, void *data, uint size)
		{
			std::memcpy(data, block, size);
			return size;
		}

		uint read_chunk(const u8* block, char *chunk, uint& size)
		{
			std::memcpy(chunk, block, CHUNK_NAME_SIZE);
			read_length(block + CHUNK_NAME_SIZE, size);
			return CHUNK_SIZE;
		}
	};

	/**
	* A builder of a double-array trie.
	*
	*  This class builds a double-array trie from records sorted in dictionary
	*  order of keys.
	*
	*  @param  key_tmpl            A type that represents a record key. This type
	*                              must be either \c char* or \c std::string .
	*  @param  value_tmpl          A type that represents a record value.
	*  @param  doublearray_traits  A class in which various properties of
	*                              double-array elements are described.
	*/
	template <
		class key_tmpl,
		class value_tmpl,
		class length_impl = u8,
		class doublearray_traits = DFL_DOUBLE_ARRAY_TRAITS
	>
	class builder
	{
	public:
		/// A type that represents a record.
		typedef key_tmpl key_type;
		/// A type that represents a record value.
		typedef value_tmpl value_type;
		/// A type that represents an element of a double array.
		typedef typename doublearray_traits::element_type element_type;
		/// A type that represents a base value in a double array.
		typedef typename doublearray_traits::base_type base_type;
		/// A type that represents a check value in a double array.
		typedef typename doublearray_traits::check_type check_type;
		/// A type that implements a double array.
		typedef std::vector<element_type> doublearray_type;

		typedef length_impl length_type;

		/**
		* A type that represents a record (a pair of key and value).
		*/
		struct record_type
		{
			key_type key;       ///< The key of the record.
			length_type keyLen;
			value_type value;   ///< The value of the record.
			length_type valueLen;
		};

		/**
		* Exception class.
		*/
		class exception : public std::runtime_error
		{
		public:
			/**
			* Constructs an instance.
			*  @param  msg     The error message.
			*/
			explicit exception(const std::string& msg)
				: std::runtime_error(msg)
			{
			}
		};

		/**
		* Statistics of the double array trie.
		*/
		struct stat_type
		{
			/// The size, in bytes, of the double array.
			uint   da_size;
			/// The number of elements in the double array.
			uint   da_num_total;
			/// The number of elements used actually in the double array.
			uint   da_num_used;
			/// The number of nodes (excluding leaves).
			uint   da_num_nodes;
			/// The number of leaves.
			uint   da_num_leaves;
			/// The utilization ratio of the double array.
			double      da_usage;
			/// The size, in bytes, of the tail array.
			uint   tail_size;
			/// The sum of the number of trials for finding bases.
			uint   bt_sum_base_trials;
			/// The average number of trials for finding bases.
			double      bt_avg_base_trials;
		};

		/**
		* The type of a progress callback function.
		*  @param  instance    The pointer to a user-defined instance.
		*  @param  i           The number of records that have already been
		*                      stored in the trie.
		*  @param  n           The total number of records to be stored.
		*/
		typedef void (*callback_type)(void *instance, uint i, uint n);

	protected:
		struct dlink_element_type
		{
			uint prev;
			uint next;
			dlink_element_type() : prev(0), next(0)
			{
			}
		};
		struct child_t {
			char_type           c;
			uint                offset;
			const record_type*  first;
			const record_type*  last;
		};

		typedef std::vector<dlink_element_type> dlink_type;

		typedef std::vector<bool> baseusage_type;

		void* m_instance;
		callback_type m_callback;

		uint m_i;
		uint m_n;

		doublearray_type m_da;
		otail m_tail;
		char_type m_table[NUM_CHARS];

		baseusage_type m_used_bases;
		dlink_type m_elink;

		stat_type m_stat;

	public:
		/**
		* Constructs a builder.
		*/
		builder()
			: m_instance(NULL), m_callback(NULL)
		{
		}

		/**
		* Destructs the builder.
		*/
		virtual ~builder()
		{
		}

		/**
		* Sets a progress callback.
		*  @param  instance    The pointer to a user-defined instance.
		*  @param  callback    The callback function.
		*/
		void set_callback(void* instance, callback_type callback)
		{
			m_instance = instance;
			m_callback = callback;
		}

		/**
		* Builds a double-array trie from sorted records.
		*  @param  first       The pointer addressing the first record.
		*  @param  last        The pointer addressing the position one past the
		*                      final record.
		*/
		void build(const record_type* first, const record_type* last) throw(exception)
		{
			clear();

			m_i = 0;
			m_n = (uint)(last - first);
			build_table(m_table, first, last);

			// Create the initial node.
			da_expand(INITIAL_INDEX+1);
			vlist_expand(INITIAL_INDEX+1);
			set_base(INITIAL_INDEX, 1);
			vlist_use(INITIAL_INDEX);
			set_base(INITIAL_INDEX, arrange(0, first, last));

			// 
			compute_stat();
		}

		/**
		* Initializes the double array.
		*/
		void clear()
		{
			// Initialize the character table.
			for (int i = 0;i < NUM_CHARS;++i) {
				m_table[i] = (char_type)i;
			}

			// Initialize the double array.
			m_da.clear();
			da_expand(1);

			// Initialize the tail array.
			m_tail.clear();
			m_tail.write<u8>(0);

			// Initialize the vacant linked list.
			vlist_init();

			// Initialize the statistics.
			std::memset(&m_stat, 0, sizeof(m_stat));
		}

		/**
		* Obtains a read-only access to the double-array.
		*  @return const doublearray_type& The reference to the double array.
		*/
		const doublearray_type& doublearray() const
		{
			return m_da;
		}

		/**
		* Obtains a read-only access to the tail array.
		*  @return const otail&    The reference to the tail array.
		*/
		const otail& tail() const
		{
			return m_tail;
		}

		/**
		* Obtains a read-only access to the character table.
		*  @return const u8*  The pointer to the character table.
		*/
		const u8* table() const
		{
			return m_table;
		}

		const stat_type& stat() const
		{
			return m_stat;
		}

	protected:
		base_type arrange(length_type p, const record_type* first, const record_type* last) throw(exception)
		{
			/** 
			* If the given range [first, last) points to a single record, i.e.,
			* (first + 1 == last), store the key postfix  and value of the record
			* to the TAIL array; let the current node as a leaf node addressing
			* to the offset from which (*first) are stored in the TAIL array. 
			*/
			if (first + 1 == last) {
				//如果是孤枝的第一个节点(可能是叶子节点也可能其子孙节点一起构成孤枝），
				//则将其写入tail数组,以减少对双数组空间的占用
				return writeLeafToTail(p, *first);
			}

			child_t *children = new child_t[NUM_CHARS];
			uint max_offset = 0;
			uint num_children = culcChild(p, first, last, children, &max_offset);
			base_type base = findBase(num_children, children, max_offset);

			// Set BASE and CHECK values of each child node.
			for (uint i = 0;i < num_children;++i) {
				setBaseCheck(p, base, children, i);
			}//end for
			delete [] children;

			++m_stat.da_num_nodes;
			return (base_type)base;
		}

		void setBaseCheck(length_type p, base_type base, const child_t *children, uint i) {
			const child_t& child = children[i];
			uint offset = child.offset;

			if (child.c == KEY_TERM) {
				assert(p == child.first->keyLen);
				if (child.first + 1 != child.last) {
					throw exception("Duplicated keys detected");
				}
				// Force to insert 'KEY_TERM' in the TAIL.
				set_base(base + offset, (base_type)writeLeafToTail(p, *child.first));
			} else {
				// Set the base value of a child node by recursively arranging
				// the descendant nodes.
				base_type bs = arrange(p + 1, child.first, child.last);
				set_base(base + offset, bs);
			}
			set_check(base + offset, (check_type)(offset - 1));
		}

		uint culcChild(length_type p, const record_type* first, const record_type* last, 
			child_t *children, uint *max_offset) {
				// Build a list of child nodes of the current node, and obtain the
				// range of records that each child node owns. Child nodes consist
				// of a set of characters at records[i].key[p] for i in [begin, end).
				int pc = -1;
				const char_type* table = m_table;

				uint num_children = 0;
				const record_type* it;
				for (it = first;it != last;++it) {
					int c = -1;
					if (p == it->keyLen) {
						assert(it == first);
						c = (int)KEY_TERM;
					} else {
						c = (int)(char_type)it->key[p];
					}
					if (pc < c || (pc == KEY_TERM && pc != c)) {			
						assert(c >= 0);
						if (0 < num_children) {
							children[num_children-1].last = it;
						}
						uint offset = (uint)table[c] + 1;
						children[num_children].first = it;
						children[num_children].c = (char_type)c;
						children[num_children].offset = offset;
						if (*max_offset < offset) {
							*max_offset = offset;
						}
						++num_children;
					} else if (c < pc && pc != KEY_TERM) {
						throw exception("The records are not sorted in dictionary order of keys");
					} 
					pc = c;
				}
				children[num_children-1].last = it;

				return num_children;
		}

		base_type findBase(uint num_children, const child_t *children, uint max_offset) throw(exception) {
			// Find the minimum of the base address (base) that can store every
			// child. This step would be very time consuming if we tried base
			// indexes from 1 one by one and tested the vacanies for child nodes.
			// Instead, we try to determine the index number of the first child-
			// node by using a double-linked list of vacant nodes, and calculate
			// back the base address from the index number of the child node.
			uint base = 0, index = 0;
			for (;;) {
				++m_stat.bt_sum_base_trials;

				// Obtain the index value of a next vacant node.
				index = vlist_next(index);

				// A base value must be greater than 1.
				if (index < INITIAL_INDEX + children[0].offset) {
					// The index is too small for a base value; try next.
					continue;
				}

				// Calculate back the base value from the index.
				base = index - children[0].offset;

				// A base value must not be used by other places.
				if (base < m_used_bases.size() && m_used_bases[base]) {
					continue;
				}

				// Expand the double array and vacant list if necessary.
				da_expand(base + max_offset + 1);
				vlist_expand(base + max_offset + 1);

				// Check if the base address can store every child in the list.
				uint i;
				for (i = 1;i < num_children;++i) {
					uint offset = children[i].offset;
					if (da_in_use(base + offset)) {
						break;
					}
				}

				// Exit the loop if successful.
				if (i == num_children) {
					break;
				}
			}

			// Fail if the double array could not store the child nodes.
			if ((uint)doublearray_traits::max_base() <= base + max_offset) {
				throw exception("The double array has no space to store child nodes");
			}

			// Register the usage of the base address.
			if (m_used_bases.size() <= base) {
				m_used_bases.resize(base+1, false);
			}
			m_used_bases[base] = true;

			// Reserve the double-array elements for the child nodes by filling
			// BASE = 1 tentatively. This step protects these elements from being
			// used by the descendant nodes; this function recursively builds the
			// double array in depth-first fashion.
			for (uint i = 0;i < num_children;++i) {
				uint offset = children[i].offset;
				set_base(base + offset, 1);
				vlist_use(base + offset);
			}

			return base;
		}

		base_type writeLeafToTail(length_type p, const record_type& rec) {
			uint offset = m_tail.tellp();
			if ((uint)doublearray_traits::max_base() < offset) {
				throw exception("The double array has no space to store leaves");
			}
			length_type keyLenLeft = rec.keyLen - p;
			m_tail.write(keyLenLeft);
			m_tail.write_string(rec.key, keyLenLeft, p);
			assert(rec.valueLen > 0);
			m_tail.write(rec.valueLen);
			m_tail.write(rec.value, rec.valueLen);

			if (m_callback != NULL) {
				m_callback(m_instance, ++m_i, m_n);
			}
			++m_stat.da_num_leaves;
			return -(base_type)offset;
		}

		void compute_stat()
		{
			m_stat.da_size = sizeof(m_da[0]) * m_da.size();
			m_stat.da_num_total = m_da.size();
			for (uint i = 0;i < m_da.size();++i) {
				if (da_in_use(i)) {
					++m_stat.da_num_used;
				}
			}
			m_stat.da_usage = m_stat.da_num_used / (double)m_stat.da_num_total;
			m_stat.tail_size = m_tail.bytes();
			m_stat.bt_avg_base_trials = m_stat.bt_sum_base_trials / (double)m_stat.da_num_total;
		}

	protected:
		inline base_type get_base(uint i) const
		{
			return doublearray_traits::get_base(m_da[i]);
		}

		inline check_type get_check(uint i) const
		{
			return doublearray_traits::get_check(m_da[i]);
		}

		inline void set_base(uint i, uint v)
		{
			doublearray_traits::set_base(m_da[i], v);
		}

		inline void set_check(uint i, check_type v)
		{
			doublearray_traits::set_check(m_da[i], v);
		}

		inline bool da_in_use(uint i) const
		{
			return (i < m_da.size() && get_base(i) != 0);
		}

		inline void da_expand(uint size)
		{
			if (m_da.size() < size) {
				m_da.resize(size, doublearray_traits::default_value());
			}
		}

		void vlist_init()
		{
			m_elink.resize(1);
			m_elink[0].next = 1;
			m_elink[0].prev = 0;
		}

		inline uint vlist_next(uint i)
		{
			return (i < m_elink.size()) ? m_elink[i].next : i+1;
		}

		void vlist_expand(uint size)
		{
			if (m_elink.size() < size) {
				uint first = m_elink.size();
				m_elink.resize(size);

				uint back = m_elink[0].prev;
				for (uint i = first;i < m_elink.size();++i) {
					m_elink[i].prev = back;
					m_elink[i].next = i+1;
					back = i;
				}
				m_elink[0].prev = m_elink.size()-1;
			}
		}

		void vlist_use(uint i)
		{
			uint prev = m_elink[i].prev;
			uint next = m_elink[i].next;
			if (m_elink.size() <= next) {
				m_elink.resize(next+1);
				m_elink[next].next = next+1;
				m_elink[0].prev = next; // The rightmost vacant node.
			}
			m_elink[prev].next = next;
			m_elink[next].prev = prev;
		}

	protected:
		struct unigram_freq
		{
			char_type c;          ///< Character code.
			double freq;    ///< Frequency.
		};

		static bool comp_freq(const unigram_freq& x, const unigram_freq& y)
		{
			return x.freq > y.freq;
		}

		void build_table(
			char_type *table,
			const record_type* first,
			const record_type* last
			)
		{
			unigram_freq st[NUM_CHARS];

			// Initialize the frequency table.
			for (int i = 0;i < NUM_CHARS;++i) {
				st[i].c = (char_type)i;
				st[i].freq = 0.;
			}

			// Count the frequency of occurrences of characters.
			for (const record_type* it = first;it != last;++it) {
				for (length_type i = 0;i < it->keyLen;++i) {
					int c = (int)(u8)it->key[i];
					++st[c].freq;
				}
				++st[KEY_TERM].freq;//终结符计数加1
			}

			// Sort the frequency table.
			sort(&st[0], &st[NUM_CHARS], comp_freq);

			// 
			for (int i = 0;i < NUM_CHARS;++i) {
				table[st[i].c] = (char_type)i;
			}
		}


	public:
		inline uint getLeavesStat() {
			return m_stat.da_num_leaves;
		}

		/** 序列化双数组Trie树
		*  约定：序列化的输出是new出来的，由调用方delete
		*  @param size 序列化后的数据长度
		*  @return 序列化后的数据起始地址
		*/
		void write(byte** data, uint *size)
		{
			// Calculate the size of each chunk.
			uint sda_size = CHUNK_SIZE + sizeof(m_da[0]) * m_da.size();
			uint tblu_size = CHUNK_SIZE + sizeof(char_type) * NUM_CHARS;
			uint tail_size = CHUNK_SIZE +  m_tail.bytes();
			uint total_size = SDAT_CHUNKSIZE + tblu_size + sda_size + tail_size;

			byte *outputBuf = new byte[total_size];
			memset(outputBuf, 0, total_size);

			// Write a "SDAT" chunk.
			uint writeBytes = 0;
			writeBytes += write_chunk(outputBuf, "SDAT", total_size);
			writeBytes += write_length(outputBuf + writeBytes, (uint)SDAT_CHUNKSIZE);
			writeBytes += write_length(outputBuf + writeBytes, (uint)m_n);
			assert(writeBytes == SDAT_CHUNKSIZE);

			// Write a "TBLU" chunk.
			writeBytes += write_chunk(outputBuf + writeBytes, "TBLU", tblu_size);
			writeBytes += write_data(outputBuf + writeBytes, m_table, tblu_size - CHUNK_SIZE);
			assert(writeBytes == (SDAT_CHUNKSIZE + tblu_size));

			// Write a chunk for the double array.
			writeBytes += write_chunk(outputBuf + writeBytes, doublearray_traits::chunk_id(), sda_size);
			writeBytes += write_data(outputBuf + writeBytes, &m_da[0], sda_size - CHUNK_SIZE);
			assert(writeBytes == (SDAT_CHUNKSIZE + tblu_size + sda_size));

			// Write a chunk for the tail array.
			writeBytes += write_chunk(outputBuf + writeBytes, "TAIL", tail_size);
			writeBytes += write_data(outputBuf + writeBytes, m_tail.block(), tail_size - CHUNK_SIZE);
			assert(writeBytes == total_size);

			*size = total_size;
			*data = outputBuf;
		}
	protected:
		uint write_data(byte* output, const void *data, const uint& size) {
			assert(output != NULL);
			memcpy(output, data, size);
			return size;
		}
		uint write_length(byte* output, const uint& value) {
			return write_data(output, &value, sizeof(value));
		}
		uint write_chunk(byte* output, const char *chunk, const uint& size) {
			memcpy(output, chunk, CHUNK_NAME_SIZE);
			write_length(output + CHUNK_NAME_SIZE, size);
			return CHUNK_SIZE;
		}
	};

	/**
	* Empty type.
	*  Specify this class as a value type of dastrie::trie and dastrie::builder
	*  to make these class behave as a set (e.g., std::set) rather than a map.
	*/
	/*
	struct empty_type
	{
	empty_type(int v = 0)
	{
	}

	friend dastrie::itail& operator>>(dastrie::itail& is, empty_type& obj)
	{
	return is;
	}

	friend dastrie::otail& operator<<(dastrie::otail& os, const empty_type& obj)
	{
	return os;
	}
	};
	*/

};

/** @} */

/**
@mainpage Static Double Array Trie (DASTrie)

@section intro Introduction

Trie is a data structure of ordered tree that implements an associative array.
Looking up a record key (usually a string) is very efficient, which takes
<i>O(1)</i> with respect to the number of stored records <i>n</i>. Trie is
also known for efficient prefix matching, where the retrieved key strings are
the prefixes of a given query string.

Double-array trie, which was proposed by Jun-ichi Aoe in the late 1980s,
represents a trie in two parallel arrays (BASE and CHECK). Reducing the
storage usage drastically, double array tries have been used in practical
applications such as morphological analysis, spelling correction, and
Japanese Kana-Kanji convertion.

Static Double Array Trie (DASTrie) is an implementation of static double-array
trie. For the simplicity and efficiency, DASTrie focuses on building a
<i>static</i> double array from a list of records <i>sorted by dictionary
order of keys</i>. DASTrie does not provide the functionality for updating an
existing trie, whereas the original framework of double array considers
dynamic updates. DASTrie provides several features:
- <b>Associative array.</b> DASTrie is designed to store associations between
key strings and their values (similarly to \c std::map). Thus, it is very
straightforward to put and obtain the record values for key strings, while
some implementations just return unique integer identifiers for key strings.
It is also possible to omit record values so that DASTrie behaves as a
string set (e.g., \c std::set).
- <b>Configurable value type.</b> A type of record values is configurable by
a template argument of dastrie::trie and dastrie::builder. Basic types
(e.g., \c int, \c double) and strings (\c char* and \c std::string) can be
used as a value type of records without any additional effort. User-defined
types can also be used as record values only if two operators for
serialization (\c operator<<() and \c operator>>()) are implemented.
- <b>Flexible key type.</b> A type of record keys is configurable by a
template argument for dastrie::builder. One can choose either
null-terminated C strings (char*) or C++ strings (std::string).
- <b>Fast look-ups.</b> Looking up a key takes <i>O(1)</i> with respect to the
number of records <i>n</i>.
- <b>Prefix match.</b> DASTrie supports prefix matching, where the retrieved
key strings are prefixes of a given query string. One can enumerate records
of prefixes by using dastrie::trie::prefix_cursor.
- <b>Compact double array.</b> DASTrie implements double arrays whose each
element is only 4 or 5 bytes long, whereas most implementations consume 8
bytes for an double-array element. The size of double-array elements is
configurable by trait classes, dastrie::doublearray4_traits and
dastrie::doublearray5_traits.
- <b>Minimal prefix double-array.</b> DASTrie manages what is called a
<i>tail array</i> so that non-branching suffixes do not waste the storage
space of double array. This feature makes tries compact, improveing the
storage utilization greatly.
- <b>Simple write interface.</b> DASTrie can serialize a trie data structure
to C++ output streams (\c std::ostream) with dastrie::builder::write()
function. Serialized data can be embedded into files with other arbitrary
data.
- <b>Simple read interface.</b> DASTrie can prepare a double-array trie from
an input stream (\c std::istream) (with dastrie::trie::read() function) or
from a memory block (with dastrie::trie::assign() function) to which a
serialized data is read or memory-mapped from a file.
- <b>Cross platform.</b> The source code can be compiled on Microsoft Visual
Studio 2008, GNU C Compiler (gcc), etc.
- <b>Simple C++ implementation.</b> Following the good example of
<a href="http://chasen.org/~taku/software/darts/">Darts</a>, DASTrie is
implemented in a single header file (dastrie.h); one can use the DASTrie API
only by including dastrie.h in a source code.

@section download Download

- <a href="http://www.chokkan.org/software/dist/dastrie-1.0.tar.gz">Source code</a>

DASTrie is distributed under the term of the
<a href="http://www.opensource.org/licenses/bsd-license.php">modified BSD license</a>.

@section changelog History
- Version 1.0 (2008-11-10):
- Initial release.

@section sample Sample code

@include sample.cpp

@section api Documentation

- @ref tutorial "DASTrie Tutorial"
- @ref dastrie_api "DASTrie API"

@section performance Performance

This section reports results of performance comparison of different trie
implementations. The experiments used two text corpora,
<a href="http://www.ldc.upenn.edu/Catalog/CatalogEntry.jsp?catalogId=LDC2006T13">Google Web 1T corpus</a>
and 
<a href="http://lexsrv3.nlm.nih.gov/SPECIALIST/index.html">SPECIALIST Lexicon</a>.
For each text corpus, the experiments measured the elapsed time for
constructing a trie (build time), the total time for finding all of keys in
the corpus (access time), and the size of the trie database generated.

@subsection performance_google Google Web 1T corpus

In this experiment, 13,588,391 unigrams (125,937,836 bytes) in the Google Web
1T corpus were inserted to a trie as keys (without frequency information).
TinyDA was not used in this experiment because the corpus is too large to
store keys within 0x007FFFFF double-array elements.

<table>
<tr>
<th>Implementation</th><th>Parameters</th><th>Build [sec]</th><th>Access [sec]</th><th>Database [bytes]</th>
</tr>
<tr align="right">
<td align="left">DASTrie 1.0</td>
<td align="left">Default</td>
<td>182</td><td>1.72</td><td>131,542,283</td>
</tr>
<tr align="right">
<td align="left">darts 0.32</td>
<td align="left">Default</td>
<td>22.3</td><td>1.25</td><td>406,358,432</td>
</tr>
<tr align="right">
<td align="left">DynDA 0.01</td>
<td align="left">Default</td>
<td>335</td><td>2.53</td><td>195,374,108</td>
</tr>
<tr align="right">
<td align="left">Tx 0.12</td>
<td align="left">Default</td>
<td>28.3</td><td>26.6</td><td>52,626,805</td>
</tr>
</table>

@subsection performance_umls LRWD table of UMLS SPECIALIST Lexicon

In this experiment, 351,006 lexicon (4,026,389 bytes) in the LRWD table of
UMLS SPECIALIST Lexicon were inserted to a trie as keys. DASTrie was
configured to represent a double-array element in 5 bytes (default) and 4
bytes (compact).

<table>
<tr>
<th>Implementation</th><th>Parameters</th><th>Build [sec]</th><th>Access [sec]</th><th>Database [bytes]</th>
</tr>
<tr align="right">
<td align="left">DASTrie 1.0</td>
<td align="left">Default</td>
<td>0.30</td><td>0.05</td><td>4,534,065</td>
</tr>
<tr align="right">
<td align="left">DASTrie 1.0</td>
<td align="left">Compact (-c)</td>
<td>0.27</td><td>0.06</td><td>3,783,249</td>
</tr>
<tr align="right">
<td align="left">darts 0.32</td>
<td align="left">Default</td>
<td>0.65</td><td>0.04</td><td>12,176,328</td>
</tr>
<tr align="right">
<td align="left">DynDA 0.01</td>
<td align="left">Default</td>
<td>0.28</td><td>0.07</td><td>7,095,226</td>
</tr>
<tr align="right">
<td align="left">TinyDA 1.23</td>
<td align="left">Default</td>
<td>0.36</td><td>0.06</td><td>4,575,520</td>
</tr>
<tr align="right">
<td align="left">Tx 0.12</td>
<td align="left">Default</td>
<td>0.68</td><td>0.70</td><td>1,646,558</td>
</tr>
</table>

@section acknowledgements Acknowledgements
The data structure of the (static) double-array trie is described in:
- Jun-ichi Aoe. An efficient digital search algorithm by using a double-array structure. <i>IEEE Transactions on Software Engineering</i>, Vol. 15, No. 9, pp. 1066-1077, 1989.
- Susumu Yata, Masaki Oono, Kazuhiro Morita, Masao Fuketa, Toru Sumitomo, and Jun-ichi Aoe. A compact static double-array keeping character codes. <i>Information Processing and Management</i>, Vol. 43, No. 1, pp. 237-247, 2007.

The DASTrie distribution contains "a portable stdint.h", which is released by
Paul Hsieh under the term of the modified BSD license, for addressing the
compatibility issue of Microsoft Visual Studio 2008. The original code is
available at: http://www.azillionmonkeys.com/qed/pstdint.h


@section reference References
- <a href="http://linux.thai.net/~thep/datrie/datrie.html">An Implementation of Double-Array Trie</a> by Theppitak Karoonboonyanan.
- <a href="http://chasen.org/~taku/software/darts/">Darts: Double-ARray Trie System</a> by Taku Kudo.
- <a href="http://nanika.osonae.com/DynDA/index.html">Dynamic Double-Array Library</a> by Susumu Yata.
- <a href="http://nanika.osonae.com/TinyDA/index.html">Tiny Double-Array Library</a> by Susumu Yata.
- <a href="http://www-tsujii.is.s.u-tokyo.ac.jp/~hillbig/tx.htm">Tx: Succinct Trie Data structure</a> by Daisuke Okanohara.

*/

/**
@page tutorial DASTrie Tutorial

@section tutorial_preparation Preparation

Put the header file "dastrie.h" to a INCLUDE path, and include the file to a
C++ source code. It's that simple.
@code
#include <dastrie.h>
@endcode

@section tutorial_builder_type Customizing a builder type

First of all, we need to design the types of records (keys and values) for
a trie, and derive a specialization of a builder. Key and record types can
be specified by the first and second template arguments of dastrie::builder,
@code
dastrie::builder<key_type, value_type, traits_type>
@endcode

A key type can be either \c char* or
\c std::string for your convenience. The string class \c std::string is
usually more convenient than \c char*, but some may prefer \c char* for
efficiency, e.g., allocating a single memory block that can store all of keys
and reading keys from a file at a time.

If you would like dastrie::trie to behave like \c std::set, use
dastrie::empty_type as a value type, which is a dummy value type that
serializes nothing with a tail array. This is an example of a trie builder
without values in which keys are represented by \c std::string,
@code
typedef dastrie::builder<std::string, dastrie::empty_type> builder_type;
@endcode

If you would like to use dastrie::trie in a similar manner as \c std::set,
specify a value type to the second template argument. This is an example of
a trie builder whose keys are \c char* and values are \c double,
@code
typedef dastrie::builder<char*, double> builder_type;
@endcode

DASTrie supports the following types as a value type    :
\c bool, \c short, \c unsigned \c short, \c int, \c unsigned \c int,
\c long, \c unsigned \c long, \c float, \c double, \c long \c double,
\c char*, \c std::string.
In addition, it is possible to use a user-defined type as a value type for a
trie. In order to do this, you must define \c operator<<() and \c operator>>()
for serializing values with a tail array. This is an example of a value type
(\c string_array) that holds an array (vector) of strings and reads/writes the
number of strings and their actual strings from/to a tail array,
@code
class string_array : public std::vector<std::string>
{
public:
friend dastrie::itail& operator>>(dastrie::itail& is, string_array& obj)
{
obj.clear();

u32 n;
is >> n;
for (u32 i = 0;i < n;++i) {
std::string dst;
is >> dst;
obj.push_back(dst);
}

return is;
}

friend dastrie::otail& operator<<(dastrie::otail& os, const string_array& obj)
{
os << (u32)obj.size();
for (size_t i = 0;i < obj.size();++i) {
os << obj[i];
}
return os;
}
};

typedef dastrie::builder<std::string, string_array> builder_type;
@endcode

The third template argument (\c traits_type) of a builder class is to
customize the size of double-array elements. By default, DASTrie uses 5 bytes
per double-array element, which can store 0x7FFFFFFF elements in a trie. This
footprint is already smaller than other double-array implementations, you can
choose 4 bytes per element and save 1 byte per element if your data is small
enough to be stored with no longer than 0x007FFFFF elements (<i>note that the
number of elements is different from the number of records</i>). Specify
dastrie::doublearray4_traits at the third argument for implementing a double
array with 4 bytes per element.

@section tutorial_builder Building a trie

After designing a builder class, prepare records to be stored in a trie.
You can use dastrie::builder::record_type to define a record type.
@code
typedef builder_type::record_type record_type;
@endcode

An instance of dastrie::builder::record_type consists of two public member
variables, dastrie::builder::record_type::key and
dastrie::builder::record_type::value.
Records can be represented by an array of records, e.g.,
@code
record_type records[10] = {
{"eight", 8},   {"five", 5},    {"four", 4},    {"nine", 9},
{"one", 1},     {"seven", 7},   {"six", 6},     {"ten", 10},
{"three", 3},   {"two", 2},
};
@endcode
or a vector of records, e.g.,
@code
std::vector<record_type> records;
@endcode

Make sure that records are sorted by dictionary order of keys. It may be a
good idea to use STL's \c sort() function if you have unsorted records.

Now you are ready to build a trie. Instantiate the builder class,
@code
builder_type builder;
@endcode
If necessary, set a callback function by using dastrie::builder::set_callback()
to receive progress reports in building a trie. Refer to build.cpp for an
actual usage.

Call dastrie::builder::build to build a trie from records. The first argument
is the iterator (pointer) addressing to the first record, and the second
argument is the iterator (pointer) addressing one past the final record.
@code
builder.build(records, records + 10);
@endcode

You can store the newly-built trie to a file by using dastrie::builder::write.
This method outputs the trie to a binary stream (\c std::ostream).
@code
std::ofstream ofs("sample.db", std::ios::binary);
builder.write(ofs);
@endcode

@section tutorial_retrieve Accessing a trie

The class dastrie::trie provides the read access to a trie. The first template
argument specifies the value type of a trie, and the second argument customizes
the double array.
@code
dastrie::trie<value_type, traits_type>
@endcode
The value type and traits for dastrie::trie are required to be the same as
those specified for dastrie::builder.

This example defines a trie class \c trie_type that can access records whose
values are represented by \c int.
@code
typedef dastrie::trie<int> trie_type;
@endcode

The class dastrie::trie can read a trie structure from an input stream; the
dastrie::trie::read() function prepares a trie from an input stream
(\c std::istream). This example reads a trie from an input stream.
@code
std::ifstream ifs("sample.db", std::ios::binary);
trie_type trie;
trie.read(ifs);
@endcode

Alternatively, you can read a trie structure from a memory block by using the
dastrie::trie::assign() function. This function may be useful when you would
like to use mmap() API for reading a trie.

Now you are ready to access the trie. Please refer to the
@ref sample "sample code" for
retrieving a record (dastrie::trie::get() and dastrie::trie::find()),
checking the existence of a record (dastrie::trie::in()),
and retrieving records that are prefixes of keys (dastrie::trie::prefix()).
*/

#endif/*__DASTRIE_H__*/
