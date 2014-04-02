#ifndef __HA_STATISTICS_H__
#define __HA_STATISTICS_H__
#include<my_global.h>

/*
typedef struct stat_unit
{
  unsigned char *sql_format;
  unsigned char sql_md5[MD5_SIZE];
  ulonglong logical_reads;
  ulong	memory_temp_table_created;
  ulong disk_temp_table_created;
  ulong start_time;
  ulong row_reads;
  ulong byte_reads;
  uint  charset_number;
}STAT_UNIT;
*/

/*
  Write MD5 hash value in a string to be used 
  as DIGEST for the statement.
*/

#define MD5_HASH_TO_STRING(_hash, _str)                    \
  sprintf(_str, "%02x%02x%02x%02x%02x%02x%02x%02x"         \
                "%02x%02x%02x%02x%02x%02x%02x%02x",        \
          _hash[0], _hash[1], _hash[2], _hash[3],          \
          _hash[4], _hash[5], _hash[6], _hash[7],          \
          _hash[8], _hash[9], _hash[10], _hash[11],        \
          _hash[12], _hash[13], _hash[14], _hash[15])

#define MD5_HASH_TO_STRING_LENGTH 32

#define MAX_SQL_TEXT_SIZE	statistics_max_sql_size
extern ulong statistics_max_sql_size; 
extern ulong statistics_max_sql_count;
extern ulong statistics_output_cycle;
#endif //__HA_STATISTICS_H__
