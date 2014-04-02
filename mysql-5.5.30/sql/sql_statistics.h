#ifndef __SQL_STATISTICS_H__
#define __SQL_STATISTICS_H__

#include "my_global.h"
#include "sql_profile.h"
#include "sql_class.h"
#include "sql_select.h"
#include "mysql/psi/mysql_thread.h"

template<class T> class SQueue : public Queue<T>
{
public:
  SQueue():Queue<T>()
  {
    mysql_mutex_init(0, &mlock, MY_MUTEX_INIT_FAST);
  }
  ~SQueue()
  {
    mysql_mutex_destroy(&mlock);
  }
  inline void push_back(T *a)
  {
    mysql_mutex_lock(&mlock);
    Queue<T>::push_back(a); 
    mysql_mutex_unlock(&mlock);
  }
  inline T* pop()
  {
    T *ret = NULL;
    mysql_mutex_lock(&mlock);
    ret = (T*) Queue<T>::pop(); 
    mysql_mutex_unlock(&mlock);
    return ret;
  }
private:
  mysql_mutex_t mlock;
};

#ifdef __cplusplus
extern "C"
{
#endif

#define MD5_SIZE 16

typedef struct index_info
{
  LEX_STRING index_name;
  uint       index_reads;
}INDEX_INFO;

class SQLInfo
{
public:
  SQLInfo():sql_format(NULL),logical_reads(0),is_full(false),exclude(false),is_stopped(false),sql_byte_count(0)
            ,last_id_index(0),or_index(0),left_brackets_after_or(0),or_end_index(0)
            ,memory_temp_table_created(0),disk_temp_table_created(0),row_reads(0)
            ,byte_reads(0),charset_number(0){}
  unsigned char *sql_format;
  ulonglong logical_reads;
  bool	is_full;
  bool  exclude;
  bool  is_stopped;
  uint  sql_byte_count;
  uint  last_id_index;
  uint  or_index;
  uint  left_brackets_after_or;
  uint  or_end_index;
  uint	memory_temp_table_created;
  uint  disk_temp_table_created;
  ulonglong start_time;
  ulong row_reads;
  ulong byte_reads;
  uint  charset_number;
  SQueue<INDEX_INFO>  index_queue;
};

typedef struct statistics_interface
{
  SQLInfo* (*si_create_sql_info)(uint charset);
  void (*si_destory_sql_info)(SQLInfo *info);
  void (*si_add_token)(SQLInfo* info, uint token, void *yylval);
  void (*si_reset_sqlinfo)(SQLInfo *info, uint charset);
  void (*si_end_sql_statement)(THD *thd);
  void (*si_start_sql_statement)(THD *thd);
  bool (*si_show_sql_stats)(uint counts, THD *thd);
  bool (*si_show_table_stats)(THD *thd);
  bool (*si_show_plugin_status)(THD *thd);
  void (*si_save_index)(JOIN *join);
  void (*si_exclude_current_sql)(THD *thd);
  my_bool *readonly;
}STATISTICS_INTERFACE;

extern STATISTICS_INTERFACE *SIF_server;

#define SQL_INFO_SIZE sizeof(SQLInfo)    

#define INCREASE_MEM_TEMP_TABLE_CREATED(thd) \
  if (thd->m_sql_info != NULL) \
	  thd->m_sql_info->memory_temp_table_created++

#define INCREASE_DISK_TEMP_TABLE_CREATED(thd) \
  if (thd->m_sql_info != NULL) \
	  thd->m_sql_info->disk_temp_table_created++

#define INCREASE_LOGICAL_READS(thd, reads)	\
  if (thd->m_sql_info != NULL)              \
	  thd->m_sql_info->logical_reads += reads

#define INCREASE_ROW_BYTE_READS(thd, bytes)	\
  if (thd->m_sql_info != NULL)  \
  {                             \
	  thd->m_sql_info->byte_reads += bytes; \
    thd->m_sql_info->row_reads ++;\
  }

#define INIT_START_TIME(thd)	\
  if (thd->m_sql_info != NULL) \
	  thd->m_sql_info->start_time = my_micro_time()

#define GET_QUERY_EXEC_TIME(info)	\
  (info != NULL ? (my_micro_time() - info->start_time) : 0);

#define EXCLUDE_CURRENT_SQL(thd)  \
  if (thd->m_sql_info != NULL)  \
    thd->m_sql_info->exclude = TRUE

/***************************************************************
 *					Statistics interface					   *
 ***************************************************************/

#define START_SQL_STATEMENT(thd) \
  inline_start_sql_statement(thd)

#define END_SQL_STATEMENT(thd) \
  inline_end_sql_statement(thd)

#define MYSQL_ADD_TOKEN(INFO, T, Y)	\
	inline_mysql_add_token(INFO, T, Y)

#define MYSQL_CREATE_SQL_INFO(charset)	\
	inline_mysql_create_sql_info(charset)

#define MYSQL_DESTORY_SQL_INFO(info)  \
  inline_mysql_destory_sql_info(info)

#define MYSQL_RESET_SQL_INFO(INFO, charset)  \
  inline_mysql_reset_sql_info(INFO, charset)

#define MYSQL_SAVE_INDEX(join)  \
  inline_mysql_save_index(join)

#define MYSQL_EXCLUDE_CURRENT_SQL(thd)  \
  inline_mysql_exclude_current_sql(thd)

static inline void
inline_mysql_save_index(JOIN *join)
{
  if (likely(SIF_server != NULL))
    SIF_server->si_save_index(join);
}

static inline void
inline_mysql_exclude_current_sql(THD *thd)
{
  if (likely(SIF_server != NULL))
    SIF_server->si_exclude_current_sql(thd);
}

static inline bool
inline_mysql_show_sql_stats(uint counts, THD *thd)
{
  bool res = TRUE;
  if (likely(SIF_server != NULL))
  {
    res = SIF_server->si_show_sql_stats(counts, thd);
  }
  else
  {
     my_error(ER_PLUGIN_IS_NOT_LOADED, MYF(0), "statistics");
  }
  return res;
}

static inline bool
inline_mysql_show_table_stats(THD *thd)
{
  bool res = TRUE;
  if (likely(SIF_server != NULL))
  {
    res = SIF_server->si_show_table_stats(thd);
  }
  else
  {
    my_error(ER_PLUGIN_IS_NOT_LOADED, MYF(0), "statistics");
  }
  return res;
}

static inline bool
inline_mysql_show_plugin_status(THD *thd)
{
  bool ret = TRUE;
  if (likely(SIF_server != NULL))
  {
    ret = SIF_server->si_show_plugin_status(thd);
  }
  else
  {
     my_error(ER_PLUGIN_IS_NOT_LOADED, MYF(0), "statistics");
  }
  return ret;
}

static inline void
inline_mysql_reset_sql_info(SQLInfo *info, uint charset)
{
  if (likely(SIF_server != NULL && info != NULL))
    SIF_server->si_reset_sqlinfo(info, charset);
}

static inline SQLInfo*
inline_mysql_create_sql_info(uint charset)
{
  if (likely(SIF_server != NULL))
	  return SIF_server->si_create_sql_info(charset);
  return NULL;
}

static inline void
inline_mysql_destory_sql_info(SQLInfo *info)
{
  if (likely(SIF_server != NULL && info != NULL))
    return SIF_server->si_destory_sql_info(info);
}

static inline void
inline_mysql_add_token(SQLInfo *info, uint token, void *yylval)
{
  if(likely(SIF_server != NULL && info != NULL))
	SIF_server->si_add_token(info, token, yylval);
}

static inline void
inline_start_sql_statement(THD *thd)
{
  if (likely(SIF_server != NULL))
    SIF_server->si_start_sql_statement(thd);
  INIT_START_TIME(thd);
}

static inline void
inline_end_sql_statement(THD *thd)
{
  if (likely(SIF_server != NULL))
  {
    SIF_server->si_end_sql_statement(thd);
    MYSQL_RESET_SQL_INFO(thd->m_sql_info, thd->charset()->number);
  }
}

#ifdef __cplusplus
}
#endif

#endif //__SQL_STATISTICS_H__
