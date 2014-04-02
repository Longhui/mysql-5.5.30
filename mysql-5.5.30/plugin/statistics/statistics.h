#ifndef __STATISTICS_H__
#define __STATISTICS_H__

#include "sql_statistics.h"

class Statistics
{
public:
  Statistics(SQLInfo* info);
  ~Statistics();
  Statistics& operator +=(Statistics &s);
  void reset();
  unsigned char *m_sql_format;
  size_t        length;
  ulonglong     m_logical_reads;
  ulong	        m_memory_temp_table_created;
  ulong         m_disk_temp_table_created;
  ulong         m_row_reads;
  ulong         m_byte_reads;
  ulong         m_exec_times;
  ulong         m_max_exec_times;
  ulong         m_min_exec_times;
  ulong         m_exec_count;
  uint          m_charset_number;
  SQueue<INDEX_INFO>  m_index_queue;
};

typedef struct Table_Info
{
  char table_name[NAME_CHAR_LEN * 2 + 1];
  enum_sql_command cmd;
}TableInfo;

class TableStats
{
public:
  TableStats(const TableInfo &info);
  TableStats & operator +=(const TableInfo &info)
  {
    switch(info.cmd)
    {
      case SQLCOM_SELECT:
        select_count++;
        break;
      case SQLCOM_INSERT:
        insert_count++;
        break;
      case SQLCOM_UPDATE:
        update_count++;
        break;
      case SQLCOM_DELETE:
        delete_count++;
        break;
      default:break;
    }
    return *this;
  }
public:
  char table_name[NAME_CHAR_LEN * 2 + 1];
  uint  length;
  ulong select_count;
  ulong insert_count;
  ulong update_count;
  ulong delete_count;
};

#endif //__STATISTICS_H__
