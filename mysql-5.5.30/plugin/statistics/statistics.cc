#include "statistics.h"
#include "ha_statistics.h"

Statistics::Statistics(SQLInfo* info):m_logical_reads(info->logical_reads),
                                       m_memory_temp_table_created(info->memory_temp_table_created),
                                       m_disk_temp_table_created(info->disk_temp_table_created),
                                       m_row_reads(info->row_reads),m_byte_reads(info->byte_reads),m_exec_times(0),
                                       m_max_exec_times(0),m_min_exec_times(0),
                                       m_charset_number(info->charset_number),length(info->sql_byte_count),m_exec_count(1)
{
  m_sql_format = new unsigned char[MAX_SQL_TEXT_SIZE];
  memcpy(m_sql_format, info->sql_format, MAX_SQL_TEXT_SIZE);
  INDEX_INFO *index_info = NULL;
  while ((index_info = info->index_queue.pop()) != NULL)
  {
    m_index_queue.push_back(index_info);
  }
}

Statistics::~Statistics()
{
  if (m_sql_format != NULL)
  {
    INDEX_INFO *index_info = NULL;
    while((index_info = m_index_queue.pop()) != NULL)
    {
      my_free(index_info->index_name.str);
      my_free(index_info);
    }
    delete [] m_sql_format;
  }
  m_sql_format = NULL;
}

void Statistics::reset()
{
  m_logical_reads = 0;
  m_memory_temp_table_created = 0;
  m_disk_temp_table_created = 0;
  m_row_reads = 0;
  m_byte_reads = 0;
  m_exec_times = 0;
  m_max_exec_times = 0;
  m_min_exec_times = 0;
  m_exec_count = 0;
  INDEX_INFO *index_info = NULL;
  while((index_info = m_index_queue.pop()) != NULL)
  {
    my_free(index_info->index_name.str);
    my_free(index_info);
  }
}

Statistics& Statistics::operator+=(Statistics &s)
{
  m_logical_reads += s.m_logical_reads;
  m_memory_temp_table_created += s.m_memory_temp_table_created;
  m_disk_temp_table_created += s.m_disk_temp_table_created;
  m_row_reads += s.m_row_reads;
  m_byte_reads += s.m_byte_reads;
  m_exec_times += s.m_exec_times;
  /*max and min exec time not do the +*/
  m_max_exec_times = m_max_exec_times > s.m_max_exec_times ? m_max_exec_times : s.m_max_exec_times;
  m_min_exec_times = m_min_exec_times < s.m_min_exec_times ? m_min_exec_times : s.m_min_exec_times;
  m_exec_count += s.m_exec_count;
  INDEX_INFO *index_info = NULL;
  while ((index_info = s.m_index_queue.pop()) != NULL)
  {
    void *dst_queue_node = m_index_queue.new_iterator();
    while(dst_queue_node != NULL)
    {
      INDEX_INFO *dst_index_info = m_index_queue.iterator_value(dst_queue_node);
      if ((index_info->index_name.length == dst_index_info->index_name.length) &&
        strcmp(index_info->index_name.str,dst_index_info->index_name.str) == 0)
      {
        dst_index_info->index_reads += index_info->index_reads;
        my_free(index_info->index_name.str);
        my_free(index_info);
        break;
      }
      dst_queue_node = m_index_queue.iterator_next(dst_queue_node);
    }
    if (dst_queue_node == NULL)
    {
      m_index_queue.push_back(index_info);
    }
  }
  return *this;
}

TableStats::TableStats(const TableInfo &info)
{
  memset(table_name, 0, sizeof(table_name));
  length = strlen(info.table_name);
  memcpy(table_name, info.table_name, length);
  select_count = update_count = insert_count = delete_count = 0;
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
}
