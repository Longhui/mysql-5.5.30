/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#ifndef KV_TYPES_H
#define KV_TYPES_H

#include <Thrift.h>
#include <TApplicationException.h>
#include <protocol/TProtocol.h>
#include <transport/TTransport.h>



namespace keyvalue {

struct Mode {
  enum type {
    SET = 0,
    INCR = 1,
    DECR = 2,
    APPEND = 3,
    PREPEND = 4,
    SETNULL = 5
  };
};

extern const std::map<int, const char*> _Mode_VALUES_TO_NAMES;

struct DataType {
  enum type {
    KV_TINYINT = 0,
    KV_SMALLINT = 1,
    KV_MEDIUMINT = 2,
    KV_INT = 3,
    KV_BIGINT = 4,
    KV_FLOAT = 5,
    KV_DOUBLE = 6,
    KV_DECIMAL = 7,
    KV_RID = 8,
    KV_CHAR = 9,
    KV_VARCHAR = 10,
    KV_BINARY = 11,
    KV_VARBINARY = 12,
    KV_BLOB = 13,
    KV_COL = 14,
    KV_NULL = 15
  };
};

extern const std::map<int, const char*> _DataType_VALUES_TO_NAMES;

struct Op {
  enum type {
    EQ = 0,
    GRATER = 1,
    LESS = 2,
    EQGRATER = 3,
    EQLESS = 4,
    NOTEQ = 5,
    LIKE = 6,
    NULLSAFEEQ = 7,
    ISNULL = 8
  };
};

extern const std::map<int, const char*> _Op_VALUES_TO_NAMES;

struct ErrCode {
  enum type {
    KV_EC_GENERIC = 0,
    KV_EC_OUT_OF_MEM = 1,
    KV_EC_FILE_NOT_EXIST = 2,
    KV_EC_FILE_PERM_ERROR = 3,
    KV_EC_DISK_FULL = 4,
    KV_EC_FILE_EXIST = 5,
    KV_EC_FILE_IN_USE = 6,
    KV_EC_FILE_EOF = 7,
    KV_EC_READ_FAIL = 8,
    KV_EC_WRITE_FAIL = 9,
    KV_EC_FILE_FAIL = 10,
    KV_EC_ACCESS_OUT_OF_PAGE = 11,
    KV_EC_PAGE_DAMAGE = 12,
    KV_EC_CTRLFILE_DAMAGE = 13,
    KV_EC_OVERFLOW = 14,
    KV_EC_INDEX_BROKEN = 15,
    KV_EC_INDEX_UNQIUE_VIOLATION = 16,
    KV_EC_NOT_LOCKED = 17,
    KV_EC_TOO_MANY_ROWLOCK = 18,
    KV_EC_TOO_MANY_SESSION = 19,
    KV_EC_NOT_SUPPORT = 20,
    KV_EC_EXCEED_LIMIT = 21,
    KV_EC_CORRUPTED_LOGFILE = 22,
    KV_EC_MISSING_LOGFILE = 23,
    KV_EC_DUP_TABLEID = 24,
    KV_EC_INVALID_BACKUP = 25,
    KV_EC_LOCK_TIMEOUT = 26,
    KV_EC_LOCK_FAIL = 27,
    KV_EC_SYNTAX_ERROR = 28,
    KV_EC_ROW_TOO_LONG = 29,
    KV_EC_NONEINDEX = 30,
    KV_EC_DUPINDEX = 31,
    KV_EC_COLDEF_ERROR = 32,
    KV_EC_CANCELED = 33,
    KV_EC_CORRUPTED = 34,
    KV_EC_TABLEDEF_ERROR = 35,
    KV_EC_INVALID_COL_GRP = 36,
    KV_EC_NO_DICTIONARY = 37,
    KV_EC_UNSUPPORTED_ENCODE = 38,
    KV_EC_CONNCETION_REFUSED = 39,
    KV_EC_TBLDEF_NOT_MATCH = 40,
    KV_EC_TBLDEF_ACCESS_FAIL = 41,
    KV_EC_TABLE_NOT_EXIST = 42,
    KV_EC_KEY_ERROR = 43,
    KV_EC_KET_NOT_MATCH = 44,
    KV_EC_COL_NOT_EXIST = 45,
    KV_EC_UNSUPPORTED_OP = 46,
    KV_EC_COLTYPE_NOT_MATCH = 47,
    KV_EC_TOO_FEW_COL = 48,
    KV_EC_TOO_MANY_COL = 49,
    KV_EC_KEY_EXIST = 50,
    KV_EC_KEY_NOT_EXIST = 51,
    KV_EC_UNKNOWN_HOST = 52,
    KV_EC_TIMEOUT = 53,
    KV_EC_UNSUPPORTED_DATATYPE = 54,
    KV_EC_ILLEGAL_PARAMETER = 55,
    KV_EC_COL_NOT_NULLABLE = 56,
    KV_EC_THRIFT_ERROR = 57,
    KV_EC_DUP_COL = 58,
    KV_EC_CONFLICT_COLVALUE = 59,
    KV_EC_VALUE_OVERFLOW = 60,
    KV_EC_GENERAL_ERROR = 61,
    KV_EC_TABLE_IN_DROPPING = 62
  };
};

extern const std::map<int, const char*> _ErrCode_VALUES_TO_NAMES;

typedef struct _TableInfo__isset {
  _TableInfo__isset() : m_schemaName(false), m_name(false) {}
  bool m_schemaName;
  bool m_name;
} _TableInfo__isset;

class TableInfo {
 public:

  static const char* ascii_fingerprint; // = "07A9615F837F7D0A952B595DD3020972";
  static const uint8_t binary_fingerprint[16]; // = {0x07,0xA9,0x61,0x5F,0x83,0x7F,0x7D,0x0A,0x95,0x2B,0x59,0x5D,0xD3,0x02,0x09,0x72};

  TableInfo() : m_schemaName(""), m_name("") {
  }

  virtual ~TableInfo() throw() {}

  std::string m_schemaName;
  std::string m_name;

  _TableInfo__isset __isset;

  bool operator == (const TableInfo & rhs) const
  {
    if (!(m_schemaName == rhs.m_schemaName))
      return false;
    if (!(m_name == rhs.m_name))
      return false;
    return true;
  }
  bool operator != (const TableInfo &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TableInfo & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Attr__isset {
  _Attr__isset() : attrNo(false), value(false) {}
  bool attrNo;
  bool value;
} _Attr__isset;

class Attr {
 public:

  static const char* ascii_fingerprint; // = "15896F1A4438B1ECBB80CEA66AD0C4C5";
  static const uint8_t binary_fingerprint[16]; // = {0x15,0x89,0x6F,0x1A,0x44,0x38,0xB1,0xEC,0xBB,0x80,0xCE,0xA6,0x6A,0xD0,0xC4,0xC5};

  Attr() : attrNo(0), value("") {
  }

  virtual ~Attr() throw() {}

  int16_t attrNo;
  std::string value;

  _Attr__isset __isset;

  bool operator == (const Attr & rhs) const
  {
    if (!(attrNo == rhs.attrNo))
      return false;
    if (!(value == rhs.value))
      return false;
    return true;
  }
  bool operator != (const Attr &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Attr & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Attrs__isset {
  _Attrs__isset() : bmp(false), attrlist(false) {}
  bool bmp;
  bool attrlist;
} _Attrs__isset;

class Attrs {
 public:

  static const char* ascii_fingerprint; // = "1FDF0B80BFFC3EDB1C1482FBA9F948D8";
  static const uint8_t binary_fingerprint[16]; // = {0x1F,0xDF,0x0B,0x80,0xBF,0xFC,0x3E,0xDB,0x1C,0x14,0x82,0xFB,0xA9,0xF9,0x48,0xD8};

  Attrs() : bmp("") {
  }

  virtual ~Attrs() throw() {}

  std::string bmp;
  std::vector<Attr>  attrlist;

  _Attrs__isset __isset;

  bool operator == (const Attrs & rhs) const
  {
    if (__isset.bmp != rhs.__isset.bmp)
      return false;
    else if (__isset.bmp && !(bmp == rhs.bmp))
      return false;
    if (!(attrlist == rhs.attrlist))
      return false;
    return true;
  }
  bool operator != (const Attrs &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Attrs & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _DriverUpdateMode__isset {
  _DriverUpdateMode__isset() : mod(false), attrNo(false), value(false) {}
  bool mod;
  bool attrNo;
  bool value;
} _DriverUpdateMode__isset;

class DriverUpdateMode {
 public:

  static const char* ascii_fingerprint; // = "17F32CB52D8039FF08647CFC2DEE080A";
  static const uint8_t binary_fingerprint[16]; // = {0x17,0xF3,0x2C,0xB5,0x2D,0x80,0x39,0xFF,0x08,0x64,0x7C,0xFC,0x2D,0xEE,0x08,0x0A};

  DriverUpdateMode() : attrNo(0), value("") {
  }

  virtual ~DriverUpdateMode() throw() {}

  Mode::type mod;
  int16_t attrNo;
  std::string value;

  _DriverUpdateMode__isset __isset;

  bool operator == (const DriverUpdateMode & rhs) const
  {
    if (!(mod == rhs.mod))
      return false;
    if (!(attrNo == rhs.attrNo))
      return false;
    if (!(value == rhs.value))
      return false;
    return true;
  }
  bool operator != (const DriverUpdateMode &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const DriverUpdateMode & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _DriverValue__isset {
  _DriverValue__isset() : dataType(false), dataValue(false) {}
  bool dataType;
  bool dataValue;
} _DriverValue__isset;

class DriverValue {
 public:

  static const char* ascii_fingerprint; // = "19B5240589E680301A7E32DF3971EFBE";
  static const uint8_t binary_fingerprint[16]; // = {0x19,0xB5,0x24,0x05,0x89,0xE6,0x80,0x30,0x1A,0x7E,0x32,0xDF,0x39,0x71,0xEF,0xBE};

  DriverValue() : dataValue("") {
  }

  virtual ~DriverValue() throw() {}

  DataType::type dataType;
  std::string dataValue;

  _DriverValue__isset __isset;

  bool operator == (const DriverValue & rhs) const
  {
    if (!(dataType == rhs.dataType))
      return false;
    if (!(dataValue == rhs.dataValue))
      return false;
    return true;
  }
  bool operator != (const DriverValue &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const DriverValue & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _Cond__isset {
  _Cond__isset() : valueOne(false), op(false), valueTwo(false) {}
  bool valueOne;
  bool op;
  bool valueTwo;
} _Cond__isset;

class Cond {
 public:

  static const char* ascii_fingerprint; // = "E3DFA66152C1FAE5AA27A23CA63C713A";
  static const uint8_t binary_fingerprint[16]; // = {0xE3,0xDF,0xA6,0x61,0x52,0xC1,0xFA,0xE5,0xAA,0x27,0xA2,0x3C,0xA6,0x3C,0x71,0x3A};

  Cond() {
  }

  virtual ~Cond() throw() {}

  DriverValue valueOne;
  Op::type op;
  DriverValue valueTwo;

  _Cond__isset __isset;

  bool operator == (const Cond & rhs) const
  {
    if (!(valueOne == rhs.valueOne))
      return false;
    if (!(op == rhs.op))
      return false;
    if (__isset.valueTwo != rhs.__isset.valueTwo)
      return false;
    else if (__isset.valueTwo && !(valueTwo == rhs.valueTwo))
      return false;
    return true;
  }
  bool operator != (const Cond &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cond & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _KVIndexDef__isset {
  _KVIndexDef__isset() : m_name(false), m_columns(false) {}
  bool m_name;
  bool m_columns;
} _KVIndexDef__isset;

class KVIndexDef {
 public:

  static const char* ascii_fingerprint; // = "EE9507CF5E179F385D7466E797D385F7";
  static const uint8_t binary_fingerprint[16]; // = {0xEE,0x95,0x07,0xCF,0x5E,0x17,0x9F,0x38,0x5D,0x74,0x66,0xE7,0x97,0xD3,0x85,0xF7};

  KVIndexDef() : m_name("") {
  }

  virtual ~KVIndexDef() throw() {}

  std::string m_name;
  std::vector<int16_t>  m_columns;

  _KVIndexDef__isset __isset;

  bool operator == (const KVIndexDef & rhs) const
  {
    if (!(m_name == rhs.m_name))
      return false;
    if (!(m_columns == rhs.m_columns))
      return false;
    return true;
  }
  bool operator != (const KVIndexDef &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const KVIndexDef & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _KVColumnDef__isset {
  _KVColumnDef__isset() : m_no(false), m_name(false), m_type(false), m_nullable(false), m_nullBitmapOffset(false) {}
  bool m_no;
  bool m_name;
  bool m_type;
  bool m_nullable;
  bool m_nullBitmapOffset;
} _KVColumnDef__isset;

class KVColumnDef {
 public:

  static const char* ascii_fingerprint; // = "2CB73F0A0DB08F76DB19C5B4173227AD";
  static const uint8_t binary_fingerprint[16]; // = {0x2C,0xB7,0x3F,0x0A,0x0D,0xB0,0x8F,0x76,0xDB,0x19,0xC5,0xB4,0x17,0x32,0x27,0xAD};

  KVColumnDef() : m_no(0), m_name(""), m_nullable(0), m_nullBitmapOffset(0) {
  }

  virtual ~KVColumnDef() throw() {}

  int16_t m_no;
  std::string m_name;
  DataType::type m_type;
  bool m_nullable;
  int16_t m_nullBitmapOffset;

  _KVColumnDef__isset __isset;

  bool operator == (const KVColumnDef & rhs) const
  {
    if (!(m_no == rhs.m_no))
      return false;
    if (!(m_name == rhs.m_name))
      return false;
    if (!(m_type == rhs.m_type))
      return false;
    if (!(m_nullable == rhs.m_nullable))
      return false;
    if (!(m_nullBitmapOffset == rhs.m_nullBitmapOffset))
      return false;
    return true;
  }
  bool operator != (const KVColumnDef &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const KVColumnDef & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _KVTableDef__isset {
  _KVTableDef__isset() : version(false), m_schemaName(false), m_name(false), m_columns(false), m_pkey(false), m_bmBytes(false) {}
  bool version;
  bool m_schemaName;
  bool m_name;
  bool m_columns;
  bool m_pkey;
  bool m_bmBytes;
} _KVTableDef__isset;

class KVTableDef {
 public:

  static const char* ascii_fingerprint; // = "529E90DD400DFEE6D2ED5C39EB259075";
  static const uint8_t binary_fingerprint[16]; // = {0x52,0x9E,0x90,0xDD,0x40,0x0D,0xFE,0xE6,0xD2,0xED,0x5C,0x39,0xEB,0x25,0x90,0x75};

  KVTableDef() : version(0), m_schemaName(""), m_name(""), m_bmBytes(0) {
  }

  virtual ~KVTableDef() throw() {}

  int64_t version;
  std::string m_schemaName;
  std::string m_name;
  std::vector<KVColumnDef>  m_columns;
  KVIndexDef m_pkey;
  int8_t m_bmBytes;

  _KVTableDef__isset __isset;

  bool operator == (const KVTableDef & rhs) const
  {
    if (!(version == rhs.version))
      return false;
    if (!(m_schemaName == rhs.m_schemaName))
      return false;
    if (!(m_name == rhs.m_name))
      return false;
    if (!(m_columns == rhs.m_columns))
      return false;
    if (!(m_pkey == rhs.m_pkey))
      return false;
    if (!(m_bmBytes == rhs.m_bmBytes))
      return false;
    return true;
  }
  bool operator != (const KVTableDef &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const KVTableDef & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

typedef struct _ServerException__isset {
  _ServerException__isset() : message(false), errcode(false) {}
  bool message;
  bool errcode;
} _ServerException__isset;

class ServerException : public ::apache::thrift::TException {
 public:

  static const char* ascii_fingerprint; // = "D6FD826D949221396F4FFC3ECCD3D192";
  static const uint8_t binary_fingerprint[16]; // = {0xD6,0xFD,0x82,0x6D,0x94,0x92,0x21,0x39,0x6F,0x4F,0xFC,0x3E,0xCC,0xD3,0xD1,0x92};

  ServerException() : message("") {
  }

  virtual ~ServerException() throw() {}

  std::string message;
  ErrCode::type errcode;

  _ServerException__isset __isset;

  bool operator == (const ServerException & rhs) const
  {
    if (!(message == rhs.message))
      return false;
    if (!(errcode == rhs.errcode))
      return false;
    return true;
  }
  bool operator != (const ServerException &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const ServerException & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

} // namespace

#endif
