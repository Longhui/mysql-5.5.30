#include "ha_statistics.h"
#include "sql_statistics.h"
#include "my_sys.h"
#include <mysql/plugin.h>
#include <my_dbug.h>
#include "sql_plugin.h"
#include "statistics.h"
#include "my_md5.h"
#include "sql_acl.h"
#include "sql_lex.h"
#include "hash.h"
#include "m_ctype.h"
#include "sql_base.h"

#include "../sql/sql_yacc.h"
#include "stat_lex_token.h"
#include "statistics.h"
#include "mysql/psi/mysql_thread.h"

#ifdef LEX_YYSTYPE
#undef LEX_YYSTYPE
#endif

#define LEX_YYSTYPE YYSTYPE
#define SECONDS_PER_HOUR  (60*60)
#define SECONDS_PER_DAY (60*60*24)
#define MAX_SQL_COUNT   1000

const char *sql_command[] = {
  "SQLCOM_SELECT", "SQLCOM_CREATE_TABLE", "SQLCOM_CREATE_INDEX", "SQLCOM_ALTER_TABLE",
  "SQLCOM_UPDATE", "SQLCOM_INSERT", "SQLCOM_INSERT_SELECT",
  "SQLCOM_DELETE", "SQLCOM_TRUNCATE", "SQLCOM_DROP_TABLE", "SQLCOM_DROP_INDEX",
  "SQLCOM_SHOW_DATABASES", "SQLCOM_SHOW_TABLES", "SQLCOM_SHOW_FIELDS",
  "SQLCOM_SHOW_KEYS", "SQLCOM_SHOW_VARIABLES", "SQLCOM_SHOW_STATUS",
  "SQLCOM_SHOW_ENGINE_LOGS", "SQLCOM_SHOW_ENGINE_STATUS", "SQLCOM_SHOW_ENGINE_MUTEX",
  "SQLCOM_SHOW_PROCESSLIST", "SQLCOM_SHOW_MASTER_STAT", "SQLCOM_SHOW_SLAVE_STAT",
  "SQLCOM_SHOW_GRANTS", "SQLCOM_SHOW_CREATE", "SQLCOM_SHOW_CHARSETS",
  "SQLCOM_SHOW_COLLATIONS", "SQLCOM_SHOW_CREATE_DB", "SQLCOM_SHOW_TABLE_STATUS",
  "SQLCOM_SHOW_TRIGGERS","SQLCOM_LOAD","SQLCOM_SET_OPTION","SQLCOM_LOCK_TABLES","SQLCOM_UNLOCK_TABLES",
  "SQLCOM_GRANT","SQLCOM_CHANGE_DB", "SQLCOM_CREATE_DB", "SQLCOM_DROP_DB", "SQLCOM_ALTER_DB",
  "SQLCOM_REPAIR", "SQLCOM_REPLACE", "SQLCOM_REPLACE_SELECT",
  "SQLCOM_CREATE_FUNCTION", "SQLCOM_DROP_FUNCTION","SQLCOM_REVOKE","SQLCOM_OPTIMIZE", "SQLCOM_CHECK",
  "SQLCOM_ASSIGN_TO_KEYCACHE", "SQLCOM_PRELOAD_KEYS","SQLCOM_FLUSH", "SQLCOM_KILL", "SQLCOM_ANALYZE",
  "SQLCOM_ROLLBACK", "SQLCOM_ROLLBACK_TO_SAVEPOINT",
  "SQLCOM_COMMIT", "SQLCOM_SAVEPOINT", "SQLCOM_RELEASE_SAVEPOINT",
  "SQLCOM_SLAVE_START", "SQLCOM_SLAVE_STOP","SQLCOM_BEGIN", "SQLCOM_CHANGE_MASTER",
  "SQLCOM_RENAME_TABLE","SQLCOM_RESET", "SQLCOM_PURGE", "SQLCOM_PURGE_BEFORE", "SQLCOM_SHOW_BINLOGS",
  "SQLCOM_SHOW_OPEN_TABLES","SQLCOM_HA_OPEN", "SQLCOM_HA_CLOSE", "SQLCOM_HA_READ",
  "SQLCOM_SHOW_SLAVE_HOSTS", "SQLCOM_DELETE_MULTI", "SQLCOM_UPDATE_MULTI",
  "SQLCOM_SHOW_BINLOG_EVENTS", "SQLCOM_DO","SQLCOM_SHOW_WARNS", "SQLCOM_EMPTY_QUERY", "SQLCOM_SHOW_ERRORS",
  "SQLCOM_SHOW_STORAGE_ENGINES", "SQLCOM_SHOW_PRIVILEGES","SQLCOM_HELP", "SQLCOM_CREATE_USER", 
  "SQLCOM_DROP_USER", "SQLCOM_RENAME_USER","SQLCOM_ALTER_USER_PROFILE","SQLCOM_REVOKE_ALL", "SQLCOM_CHECKSUM",
  "SQLCOM_CREATE_PROCEDURE", "SQLCOM_CREATE_SPFUNCTION", "SQLCOM_CALL",
  "SQLCOM_DROP_PROCEDURE", "SQLCOM_ALTER_PROCEDURE","SQLCOM_ALTER_FUNCTION",
  "SQLCOM_SHOW_CREATE_PROC", "SQLCOM_SHOW_CREATE_FUNC","SQLCOM_SHOW_STATUS_PROC", "SQLCOM_SHOW_STATUS_FUNC",
  "SQLCOM_PREPARE", "SQLCOM_EXECUTE", "SQLCOM_DEALLOCATE_PREPARE","SQLCOM_CREATE_VIEW", "SQLCOM_DROP_VIEW",
  "SQLCOM_CREATE_TRIGGER", "SQLCOM_DROP_TRIGGER","SQLCOM_XA_START", "SQLCOM_XA_END", "SQLCOM_XA_PREPARE",
  "SQLCOM_XA_COMMIT", "SQLCOM_XA_ROLLBACK", "SQLCOM_XA_RECOVER","SQLCOM_SHOW_PROC_CODE", "SQLCOM_SHOW_FUNC_CODE",
  "SQLCOM_ALTER_TABLESPACE","SQLCOM_INSTALL_PLUGIN", "SQLCOM_UNINSTALL_PLUGIN",
  "SQLCOM_SHOW_AUTHORS", "SQLCOM_BINLOG_BASE64_EVENT","SQLCOM_SHOW_PLUGINS","SQLCOM_SHOW_CONTRIBUTORS",
  "SQLCOM_CREATE_SERVER", "SQLCOM_DROP_SERVER", "SQLCOM_ALTER_SERVER",
  "SQLCOM_CREATE_EVENT", "SQLCOM_ALTER_EVENT", "SQLCOM_DROP_EVENT","SQLCOM_SHOW_CREATE_EVENT", "SQLCOM_SHOW_EVENTS",
  "SQLCOM_SHOW_CREATE_TRIGGER","SQLCOM_ALTER_DB_UPGRADE","SQLCOM_SHOW_PROFILE", "SQLCOM_SHOW_PROFILES",
  "SQLCOM_SIGNAL", "SQLCOM_RESIGNAL","SQLCOM_SHOW_RELAYLOG_EVENTS",
  "SQLCOM_SHOW_SQL_STATS","SQLCOM_SHOW_TABLE_STATS", "SQLCOM_SHOW_PLUGIN_STATUS","SQLCOM_BACKUP",
  "SQLCOM_CREATE_PROFILE","SQLCOM_ALTER_PROFILE","SQLCOM_DROP_PROFILE",
  "SQLCOM_CREATE_ROLE","SQLCOM_DROP_ROLE","SQLCOM_REVOKE_ROLE",NULL
};

const char *sys_database[] ={"mysql","information_schema","performance_schema", NULL};

#define SQL_DIGEST_SIZE 64
ulong statistics_max_sql_size = 10240;
ulong statistics_max_sql_count = 100;
ulong statistics_output_cycle = 1;
ulong statistics_expire_duration = 3;
my_bool statistics_plugin_status = FALSE;
my_bool statistics_output_now = FALSE;
my_bool statistics_shutdown_fast = FALSE;
static my_bool *statistics_readonly = FALSE;
static HASH           stat_sql_hash;
static mysql_mutex_t  hash_sql_lock;

static mysql_mutex_t  stat_output_lock;
static mysql_cond_t   stat_output_cond;
static ulong          start_output_time;
static ulong          end_output_time;
static my_bool        stat_shutdown = FALSE;
static uint           stat_thread_exit = 0;

static HASH           stat_table_hash;
static mysql_mutex_t  hash_table_lock;

struct statistics_array
{
  uint  length;
  Statistics *array[1000];
}statistics_array;

struct 
{
  ulong       discard_because_exclude;
  ulong       output_counts;
  ulong       clean_counts;
  ulong       oos_sql_counts; //oos = out of size
  ulonglong   sql_enter_counts;
  ulonglong   table_enter_counts;
  ulonglong   discard_because_hashtable_full;
  mysql_mutex_t status_lock;
}status_variables;

static void get_sql_text(char *text, Statistics *s);
#define SIZE_OF_A_TOKEN 2

static int show_statistics_vars(THD *thd, SHOW_VAR *var, char *buff);

static struct st_mysql_statistics_engine statistics_engine=
{ MYSQL_STATISTICS_INTERFACE_VERSION };

static const char statistics_hton_name[]= "statistics";

const char plugin_author[] = "Netease Corporation";

static SHOW_VAR statistics_status_variables[]= {
  {"statistics", (char*) &show_statistics_vars, SHOW_FUNC},
  {NullS, NullS, SHOW_LONG}
};

static void
output_now(
  THD* thd __attribute__((unused)),
  struct st_mysql_sys_var* var __attribute__((unused)),
  void* var_ptr __attribute__((unused)),
  const void * save)
{
  if (*(bool*)save)
  {
    mysql_cond_signal(&stat_output_cond);
  }
}

static MYSQL_SYSVAR_ULONG(max_sql_size, statistics_max_sql_size,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "the max size of the sql, if the size greater than this value "
  "this sql will not to be statistics.",
  NULL, NULL, 10240L, 0, 102400L, 0);

static MYSQL_SYSVAR_ULONG(expire_duration, statistics_expire_duration,
  PLUGIN_VAR_RQCMDARG,
  "the stats info stored in mysql.sql_stats and mysql.table_stats duration "
  "out of this time, the expire info will be deleted, unit day",
  NULL, NULL, 3, 1, LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(max_sql_count, statistics_max_sql_count,
  PLUGIN_VAR_RQCMDARG,
  "the max count of the sql, when the count of sql reach this size "
  "the new sql will be discard or replace another",
  NULL, NULL, 100L, 0, MAX_SQL_COUNT, 0);

static MYSQL_SYSVAR_ULONG(output_cycle, statistics_output_cycle,
  PLUGIN_VAR_RQCMDARG,
  "the cycle when output the information into the tables "
  "at least 10 minutes,default 1 hour",
  NULL, NULL, 1L, 1L, LONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(plugin_status, statistics_plugin_status,
  PLUGIN_VAR_NOCMDARG,
  "enable to use the statistics plugin "
  "if set FALSE, the plugin will no to be used",
  NULL, NULL, FALSE);

static MYSQL_SYSVAR_BOOL(output_now, statistics_output_now,
  PLUGIN_VAR_RQCMDARG,
  "output the statistics info immediately, "
  "do not wait for the output cycle",
  NULL, output_now, FALSE);

static MYSQL_SYSVAR_BOOL(shutdown_fast, statistics_shutdown_fast,
  PLUGIN_VAR_RQCMDARG,
  "shutdown fast,do not output the statistics infos in memory",
  NULL, NULL, FALSE);

static struct st_mysql_sys_var* statistics_system_variables[]= {
  MYSQL_SYSVAR(max_sql_size),
  MYSQL_SYSVAR(expire_duration),
  MYSQL_SYSVAR(max_sql_count),
  MYSQL_SYSVAR(output_cycle),
  MYSQL_SYSVAR(plugin_status),
  MYSQL_SYSVAR(output_now),
  MYSQL_SYSVAR(shutdown_fast),
  NULL
};

extern "C" uchar *get_key_tablestats(TableStats *t, size_t *length,
                                    my_bool not_used __attribute__((unused)))
{
  *length = t->length;
  return (uchar*)t->table_name;
}

extern "C" void free_tablestats(TableStats *t)
{
  delete t; t = NULL;
}

extern "C" uchar *get_key_statistics(Statistics *s, size_t *length,
                                    my_bool not_used __attribute__((unused)))
{
  *length = s->length;
  return (uchar*)s->m_sql_format;
}

extern "C" void free_statistics(Statistics *s)
{
  delete s;s = NULL;
}

static void
init_statistics_hash(void)
{
  mysql_mutex_init(0, &hash_sql_lock, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(0, &hash_table_lock, MY_MUTEX_INIT_FAST);
#ifndef NO_EMBEDDED_ACCESS_CHECKS
  (void)my_hash_init(&stat_sql_hash, system_charset_info, statistics_max_sql_count,
    0, 0, (my_hash_get_key)get_key_statistics,(my_hash_free_key)free_statistics,0);
  (void)my_hash_init(&stat_table_hash, system_charset_info, 500, 0, 0,
    (my_hash_get_key)get_key_tablestats, (my_hash_free_key)free_tablestats, 0);
#endif
}

static void
destory_statistics_hash(void)
{
  mysql_mutex_destroy(&hash_sql_lock);
  mysql_mutex_destroy(&hash_table_lock);
  my_hash_free(&stat_sql_hash);
  my_hash_free(&stat_table_hash);
}

static bool statistics_cmp(const Statistics *s1, const Statistics *s2)
{
  return s1->m_exec_times > s2->m_exec_times;
}

/* sort the statistics array for the 'show top sql' command, order by the execute time desc */
static void quick_sort(Statistics **elem, const int &start, const int &end, bool(*cmp)(const Statistics *, const Statistics*))
{
  int i = start, j = end;
  const Statistics *m = *(elem + ((start + end) >> 1));
  Statistics *temp;
  do
  {
    while (cmp(elem[i], m)) i++;
    while (cmp(m, elem[j])) j--;
    if (i <= j)
    {
      temp = elem[i]; elem[i] = elem[j]; elem[j] = temp;
      i++; j--;
    }
  }while (i < j);
  if (i < end) quick_sort(elem, i, end, cmp);
  if (start < j) quick_sort(elem, start, j, cmp);
}

/* exclude the system database access sql */
static bool
only_system_database(TABLE_LIST *tables)
{
  if (tables == NULL) return FALSE;
  TABLE_LIST *table = NULL;
  for(table=tables; table; table=table->next_global)
  {
    int i = 0;
    while(sys_database[i] != NULL)
    {
      if (strcmp(sys_database[i],table->db) == 0)
        return TRUE;
      i++;
    }
  }
  return FALSE;
}

/*exclude the sql which like blew:
  1. the system database accessed sql
  2. the show command
  3. the set command
  4. the explain command
*/
static void
stat_exclude_current_sql(THD *thd)
{
  if (thd->m_sql_info == NULL || thd->m_sql_info->is_stopped) return;
  LEX  *lex= thd->lex; 
  if (lex->sql_command == SQLCOM_SET_OPTION 
      || only_system_database(lex->query_tables)
      || lex->describe
      || strncmp(sql_command[lex->sql_command],"SQLCOM_SHOW",11) == 0)
  {
    thd->m_sql_info->exclude = TRUE;
  }
}

static void
sort_statistics_by_exectime(Statistics **s, uint len)
{
  if (len < 2) return;
  quick_sort(s,0, len -1, statistics_cmp);
}

static bool
stat_show_plugin_status(THD *thd)
{
  char buffer[5120] = {0};
  DBUG_ENTER("show plugin status");
  sprintf(buffer,
          "==========================================\n"
          "OUTPUT PLUGIN STATISTICS STATUS\n"
          "==========================================\n"
          "--------------\n"
          "STATS SQL INFO\n"
          "--------------\n"
          "max sql text: %lu, out of size sql: %lu\n"
          "---------------\n"
          "HASH TABLE INFO\n"
          "---------------\n"
          "max hash table: %lu,current hash size: %lu\n"
          "----------------\n"
          "DISCARD SQL INFO\n"
          "----------------\n"
          "exclude: %lu\n"
          "------------------\n"
          "STATS ENTER COUNTS\n"
          "------------------\n"
          "sql enter counts: %llu, table enter counts: %llu\n"
          "sql discard because of hash table is full: %llu\n"
          "-----------\n"
          "MEMORY INFO\n"
          "-----------\n"
          "current memory alloc: %lu\n"
          "------------\n"
          "OUTPUT COUNT\n"
          "------------\n"
          "output count: %lu, OUTPUT CYCLE: %lu\n"
          "-----------\n"
          "CLEAN COUNT\n"
          "-----------\n"
          "clean count: %lu, expire duration: %lu\n"
          "==========================================\n"
          "OUTPUT STATUS END\n"
          "==========================================\n", 
          statistics_max_sql_size, status_variables.oos_sql_counts,statistics_max_sql_count, 
          stat_sql_hash.records,status_variables.discard_because_exclude,
          status_variables.sql_enter_counts, status_variables.table_enter_counts,
          status_variables.discard_because_hashtable_full,
          (sizeof(Statistics) + statistics_max_sql_size) * stat_sql_hash.records,
          status_variables.output_counts, statistics_output_cycle,
          status_variables.clean_counts, statistics_expire_duration);

  List<Item> field_list;
  Protocol *protocol= thd->protocol;
   field_list.push_back(new Item_empty_string("status", 10));
   if (protocol->send_result_set_metadata(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
     DBUG_RETURN(TRUE);
  protocol->prepare_for_resend();
  protocol->store(buffer, strlen(buffer), system_charset_info);
  if (protocol->write())
    DBUG_RETURN(TRUE);
  my_eof(thd);
  DBUG_RETURN(FALSE);
}

/* store the index witch accessed by the sql */
static void
stat_save_index(JOIN *join)
{
  THD *thd = join->thd;
  DBUG_ENTER("stat save index");
  if (thd->m_sql_info == NULL || thd->m_sql_info->is_full || thd->m_sql_info->is_stopped) 
    DBUG_VOID_RETURN;
  for(uint i = 0; i < join->tables; i++)
  {
    JOIN_TAB *tab=join->join_tab+i;
    /*if do the select count(*) from myisam_table tab will be null*/
    if (tab == NULL) continue; 
    TABLE *table=tab->table;
    char buffer[256] = {0};
    INDEX_INFO *index = (INDEX_INFO *)my_malloc(sizeof(INDEX_INFO),MYF(MY_WME));
    if (tab->ref.key_parts)
    {
      KEY *key_info = table->key_info + tab->ref.key;
      sprintf(buffer, "%s.%s", table->alias, key_info->name);
      index->index_name.str = my_strdup(buffer,MYF(MY_WME));
      index->index_name.length = strlen(buffer);
      index->index_reads = 1;
      thd->m_sql_info->index_queue.push_back(index);
    }
    else if(tab->type == JT_NEXT)
    {
      KEY *key_info = table->key_info + tab->index;
      sprintf(buffer, "%s.%s", table->alias, key_info->name);
      index->index_name.str = my_strdup(buffer,MYF(MY_WME));
      index->index_name.length = strlen(buffer);
      index->index_reads = 1;
      thd->m_sql_info->index_queue.push_back(index);
    }
    else if(tab->select && tab->select->quick)
    {
      char buff1[256] = {0}, buff2[256] = {0};
      String tmp1(buff1, sizeof(buff1),system_charset_info);
      String tmp2(buff2, sizeof(buff2),system_charset_info);
      tmp1.length(0);
      tmp2.length(0);
      tab->select->quick->add_keys_and_lengths(&tmp1, &tmp2);
      sprintf(buffer, "%s.%s", table->alias, tmp1.c_ptr());
      index->index_name.str = my_strdup(buffer,MYF(MY_WME));
      index->index_name.length = strlen(buffer);
      index->index_reads = 1;
      thd->m_sql_info->index_queue.push_back(index);
    }
    else
    {
      my_free(index);
    }
  }
  DBUG_VOID_RETURN;
}

static bool
stat_show_sql_stats(uint count, THD *thd)
{
  List<Item> field_list;
  Protocol *protocol = thd->protocol;
  Statistics *s;
  DBUG_ENTER("stat show sql stats");
  field_list.push_back(new Item_empty_string("SQL_TEXT", MAX_SQL_TEXT_SIZE));
  field_list.push_back(new Item_empty_string("INDEX",MAX_SQL_TEXT_SIZE));
  field_list.push_back(new Item_int("MEMORY_TEMP_TABLES", MY_INT32_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("DISK_TEMP_TABLES", MY_INT32_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("ROW_READS",MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("BYTE_READS", MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("MAX_EXEC_TIMES", MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("MIN_EXEC_TIMES", MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("EXEC_TIMES", MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("EXEC_COUNT", MY_INT64_NUM_DECIMAL_DIGITS));
  if (protocol->send_result_set_metadata(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);
  mysql_mutex_lock(&hash_sql_lock);
  sort_statistics_by_exectime(statistics_array.array, statistics_array.length);
  char *buffer = (char*)my_malloc(MAX_SQL_TEXT_SIZE,MYF(MY_WME));
  for(uint i = 0; i < statistics_array.length && i < count; i++)
  {
    s = statistics_array.array[i];
    if (s == NULL) continue;
    memset(buffer,0,MAX_SQL_TEXT_SIZE);
    protocol->prepare_for_resend();
    get_sql_text(buffer,s);
    protocol->store(buffer,system_charset_info);
    void *index = s->m_index_queue.new_iterator();
    char *p = buffer;
    while(index != NULL)
    {
      INDEX_INFO *info = s->m_index_queue.iterator_value(index);
      sprintf(p, "%s(%d),",info->index_name.str,info->index_reads);
      while(*p != '\0') p++; 
      index = s->m_index_queue.iterator_next(index);
    }
    if (p != buffer)
    {
      p--;*p = '\0';
    }
    else
    {
      sprintf(buffer,"NULL");
    }
    protocol->store(buffer,system_charset_info);
    protocol->store((ulonglong)s->m_memory_temp_table_created);
    protocol->store((ulonglong)s->m_disk_temp_table_created);
    protocol->store((ulonglong)s->m_row_reads);
    protocol->store((ulonglong)s->m_byte_reads);
    protocol->store((ulonglong)s->m_max_exec_times);
    protocol->store((ulonglong)s->m_min_exec_times);
    protocol->store((ulonglong)s->m_exec_times);
    protocol->store((ulonglong)s->m_exec_count);
    if (protocol->write())
    {
      mysql_mutex_unlock(&hash_sql_lock);
      DBUG_RETURN(TRUE);
    }
  }
  mysql_mutex_unlock(&hash_sql_lock);
  my_free(buffer);
  my_eof(thd);
  DBUG_RETURN(FALSE);
}

static bool
stat_show_table_stats(THD *thd)
{
  List<Item> field_list;
  Protocol *protocol = thd->protocol;
  DBUG_ENTER("stat show table stats");
  field_list.push_back(new Item_empty_string("DBNAME", NAME_CHAR_LEN));
  field_list.push_back(new Item_empty_string("TABLE_NAME",NAME_CHAR_LEN));
  field_list.push_back(new Item_int("SELECT_COUNT",MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("INSERT_COUNT", MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("UPDATE_COUNT", MY_INT64_NUM_DECIMAL_DIGITS));
  field_list.push_back(new Item_int("DELETE_COUNT", MY_INT64_NUM_DECIMAL_DIGITS));
  if (protocol->send_result_set_metadata(&field_list, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(TRUE);
  mysql_mutex_lock(&hash_table_lock);
  for(uint i = 0; i < stat_table_hash.records; i++)
  {
    char buffer[NAME_CHAR_LEN] = {0};
    TableStats *ts = (TableStats *)my_hash_element(&stat_table_hash, i);
    if (ts == NULL) continue;
    protocol->prepare_for_resend();
    const char *p = ts->table_name;
    while(p != NULL && *p != '.') p++;
    strncpy(buffer, ts->table_name, (p - ts->table_name));
    protocol->store(buffer,system_charset_info);
    strcpy(buffer, p + 1);
    protocol->store(buffer,system_charset_info);
    protocol->store((ulonglong)ts->select_count);
    protocol->store((ulonglong)ts->insert_count);
    protocol->store((ulonglong)ts->update_count);
    protocol->store((ulonglong)ts->delete_count);
    if (protocol->write())
    {
      mysql_mutex_unlock(&hash_table_lock);
      DBUG_RETURN(TRUE);
    }
  }
  mysql_mutex_unlock(&hash_table_lock);
  my_eof(thd);
  DBUG_RETURN(FALSE);
}

static 
SQLInfo*
stat_create_sql_info(uint charset)
{
  DBUG_ENTER("stat create sql info");
  SQLInfo *info = new SQLInfo();
  if (info != NULL)
  {
    info->sql_format = (unsigned char*)my_malloc(MAX_SQL_TEXT_SIZE,MYF(MY_WME));
	  memset(info->sql_format, 0, MAX_SQL_TEXT_SIZE);
  }
  info->charset_number = charset;
  DBUG_RETURN(info);
}

static void
stat_destory_sql_info(SQLInfo *info)
{
  DBUG_ENTER("stat destory sql info");
  if (info != NULL)
  {
    if (info->sql_format != NULL)
    {
      my_free(info->sql_format);
      info->sql_byte_count = NULL;
    }
    delete info;
  }
  DBUG_VOID_RETURN;
}

static void stat_reset_sqlinfo(SQLInfo *info, uint charset)
{
  DBUG_ENTER("stat reset sql info");
  info->is_full = false;
  info->exclude = false;
  info->disk_temp_table_created = 0;
  info->last_id_index = 0;
  info->logical_reads = 0;
  info->memory_temp_table_created = 0;
  info->row_reads = 0;
  info->byte_reads = 0;
  info->sql_byte_count = 0;
  info->start_time = 0;
  info->charset_number = charset;
  INDEX_INFO *index = NULL;
  while((index = info->index_queue.pop()) != NULL)
  {
    my_free(index->index_name.str);
    my_free(index);
  }
  memset(info->sql_format,0,MAX_SQL_TEXT_SIZE);
  DBUG_VOID_RETURN;
}

/* read a token witch has the position index */
static int read_token(SQLInfo *info, uint index, uint *token)
{
  DBUG_ENTER("read token sql info");
  DBUG_ASSERT(index <= info->sql_byte_count);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);

  if (index + SIZE_OF_A_TOKEN <= info->sql_byte_count)
  {
    unsigned char *src = &info->sql_format[index];
    *token = src[0] | (src[1] << 8);
    DBUG_RETURN(index + SIZE_OF_A_TOKEN);
  }

  *token = 0;
  DBUG_RETURN(MAX_SQL_TEXT_SIZE + 1);
}

static int read_token(Statistics *s, uint index, uint *token)
{
  DBUG_ENTER("read token statistics");
  DBUG_ASSERT(index <= s->length);
  DBUG_ASSERT(s->length <= MAX_SQL_TEXT_SIZE);

  if (index + SIZE_OF_A_TOKEN <= s->length)
  {
    unsigned char *src = &s->m_sql_format[index];
    *token = src[0] | (src[1] << 8);
    DBUG_RETURN(index + SIZE_OF_A_TOKEN);
  }

  *token = 0;
  DBUG_RETURN(MAX_SQL_TEXT_SIZE + 1);
}

static void store_token(SQLInfo *info, uint token)
{
  DBUG_ENTER("store token");
  DBUG_ASSERT(info->sql_byte_count >= 0);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);
  if ( info->sql_byte_count + SIZE_OF_A_TOKEN <= MAX_SQL_TEXT_SIZE)
  {
    unsigned char *dest = info->sql_format + info->sql_byte_count;
    dest[0] = token & 0xff;
    dest[1] = (token >> 8) & 0xff;
    info->sql_byte_count += SIZE_OF_A_TOKEN;
  }
  else
  {
    info->is_full = true;
    thread_safe_increment(status_variables.oos_sql_counts, &status_variables.status_lock);
  }
  DBUG_VOID_RETURN;
}

static uint peek_token(const SQLInfo *info, uint index)
{
  uint token;
  DBUG_ENTER("peek token");
  DBUG_ASSERT(index >= 0);
  DBUG_ASSERT(index + SIZE_OF_A_TOKEN <= info->sql_byte_count);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);
  token = ((info->sql_format[index + 1]) << 8) | info->sql_format[index];
  DBUG_RETURN(token);
}

static void peek_last_two_tokens(const SQLInfo* info, uint *t1, uint *t2)
{
  DBUG_ENTER("peek last two tokens");
  int last_id_index = info->last_id_index;
  int byte_count = info->sql_byte_count;
  if (last_id_index <= byte_count - SIZE_OF_A_TOKEN)
  {
    *t1 = peek_token(info, byte_count - SIZE_OF_A_TOKEN);
  }
  else
  {
    *t1 = TOK_STAT_UNUSED;
  }
  if (last_id_index <= byte_count - 2 * SIZE_OF_A_TOKEN)
  {
    *t2 = peek_token(info, byte_count - 2 * SIZE_OF_A_TOKEN);
  }
  else
  {
    *t1 = TOK_STAT_UNUSED;
  }
  DBUG_VOID_RETURN;
}

static int read_identifier(SQLInfo *info, uint index, char **id_string, int *id_length)
{
  uint new_index;
  DBUG_ENTER("read indextifier sql info");
  DBUG_ASSERT(index <= info->sql_byte_count);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);

  unsigned char *src = &info->sql_format[index];
  uint length = src[0] | (src[1] << 8);
  *id_string = (char *)(src + 2);
  *id_length = length;

  new_index = index + SIZE_OF_A_TOKEN + length;
  DBUG_ASSERT(new_index <= info->sql_byte_count);
  DBUG_RETURN(new_index);
}

static int read_identifier(Statistics *s, uint index, char **id_string, int *id_length)
{
  uint new_index;
  DBUG_ENTER("read identifier statistics");
  DBUG_ASSERT(index <= s->length);
  DBUG_ASSERT(s->length <= MAX_SQL_TEXT_SIZE);

  unsigned char *src = &s->m_sql_format[index];
  uint length = src[0] | (src[1] << 8);
  *id_string = (char *)(src + 2);
  *id_length = length;

  new_index = index + SIZE_OF_A_TOKEN + length;
  DBUG_ASSERT(new_index <= s->length);
  DBUG_RETURN(new_index);
}

static void store_token_identifier(SQLInfo *info,uint token, 
                                      uint id_length, const char *id_name)
{
  DBUG_ENTER("store token identifier");
  DBUG_ASSERT(info->sql_byte_count >= 0);
  DBUG_ASSERT(info->sql_byte_count <= MAX_SQL_TEXT_SIZE);
  uint bytes_needed = 2 * SIZE_OF_A_TOKEN + id_length;
  if (info->sql_byte_count + bytes_needed <= MAX_SQL_TEXT_SIZE)
  {
    unsigned char *dest = &info->sql_format[info->sql_byte_count];
    dest[0] = token & 0xff;
    dest[1] = (token >> 8) & 0xff;
    dest[2] = id_length & 0xff;
    dest[3] = (id_length >> 8) & 0xff;

    if (id_length > 0)
      memcpy((char*)(dest + 4), id_name, id_length);
    info->sql_byte_count += bytes_needed;
  }
  else
  {
    info->is_full = true;
  }
  DBUG_VOID_RETURN;
}

static void get_sql_text(char *text, Statistics *s)
{
  DBUG_ENTER("get sql text");
  DBUG_ASSERT(s != NULL);
  bool truncated = false;
  uint byte_count = s->length;
  uint byte_needed = 0;
  uint tok = 0;
  uint current_byte = 0;
  lex_token_string *token_data;
  uint bytes_available = MAX_SQL_TEXT_SIZE - 4;

  /* convert text to utf8 */
  CHARSET_INFO *from_cs = get_charset(s->m_charset_number, MYF(0));
  CHARSET_INFO *to_cs = &my_charset_utf8_bin;
  if (from_cs == NULL)
  {
    *text = '\0';
    DBUG_VOID_RETURN;
  }
  const uint max_converted_size = MAX_SQL_TEXT_SIZE * 4;
  char *id_buffer = (char*)my_malloc(max_converted_size,MYF(MY_WME));
  char *id_string;
  int id_length;
  bool convert_text = !my_charset_same(from_cs, to_cs);
  DBUG_ASSERT(byte_count <= MAX_SQL_TEXT_SIZE);

  while((current_byte < byte_count) &&
         (bytes_available > 0) &&
         !truncated)
  {
    current_byte = read_token(s, current_byte, &tok);
    token_data = &lex_token_array[tok];
    switch(tok)
    {
      case IDENT:
      case IDENT_QUOTED:
      {
        char *id_ptr;
        int id_len;
        uint err_cs = 0;
        current_byte = read_identifier(s, current_byte, &id_ptr, &id_len);
        if (convert_text)
        {
          if (to_cs->mbmaxlen * id_len > max_converted_size)
          {
            truncated = true;
            break;
          }
          id_length = my_convert(id_buffer, max_converted_size, to_cs, 
                                  id_ptr, id_len, from_cs, &err_cs);
          id_string = id_buffer;
        }
        else
        {
          id_string = id_ptr;
          id_length = id_len;
        }

        if (id_length == 0 || err_cs != 0)
        {
          truncated = true;
          break;
        }

        byte_needed = id_length + (tok == IDENT ? 1 : 3);
        if (byte_needed <= bytes_available)
        {
          if (tok == IDENT_QUOTED)
            *text++='`';
          if (id_length > 0)
          {
            memcpy(text, id_string, id_length);
            text += id_length;
          }
          if (tok == IDENT_QUOTED)
            *text++='`';
          *text++=' ';
          bytes_available -= byte_needed;
        }
        else
        {
          truncated = true;
        }
        break;
      }
      default:
      {
        int tok_length = token_data->m_token_length;
        byte_needed = tok_length + 1;
        if (byte_needed <= bytes_available)
        {
          strncpy(text, token_data->m_token_string, tok_length);
          text += tok_length;
          *text++= ' ';
          bytes_available -= byte_needed;
        }
        else
        {
          truncated = true;
        }
        break;
      }
    }
  }
  my_free(id_buffer);
  text = '\0';
  DBUG_VOID_RETURN;
}

/* if we find the or condition, we should check the values on the 
   left and right, if the values on left and right has the similar
   format like a=1 or a=2, we must convert it like this: a=?+, there
   is a example:
   select * from t where id=1 or id=2 or id=3, it will be converted
   like this:
   select * from t where id=?+
   this function will do the convert
 */
static void
stat_or_token_cmp(SQLInfo *info)
{
  DBUG_ENTER("stat or token cmp");
  uint or_right_start = info->or_index + SIZE_OF_A_TOKEN;
  uint or_left_start = info->or_index - (info->or_end_index - or_right_start);
  uint token_before_or;
  read_token(info, info->or_index - SIZE_OF_A_TOKEN,&token_before_or);
  if (token_before_or == '+')
  {
    or_left_start -= SIZE_OF_A_TOKEN;
  }
  if (or_left_start > info->or_index)
  {
    info->or_index = 0;
    DBUG_VOID_RETURN;
  }
  if (memcmp(&info->sql_format[or_left_start], &info->sql_format[or_right_start], info->or_end_index - or_right_start) == 0)
  {
    info->sql_byte_count = info->or_index;
    if (token_before_or != '+')
      store_token(info,'+');
  }
  info->or_index = 0;
  DBUG_VOID_RETURN;
}

/* this function maybe called when parse the sql, add the token into the token array */
static void
stat_add_token(SQLInfo* info, uint token, void *yylval)
{
  DBUG_ENTER("stat add token");
  info->is_stopped = !statistics_plugin_status;
  if (info->is_stopped || info->is_full || token == END_OF_INPUT)
    DBUG_VOID_RETURN;
  uint last_token;
  uint last_token2;

  peek_last_two_tokens(info, &last_token, &last_token2);

  switch(token)
  {
    case BIN_NUM:
    case DECIMAL_NUM:
    case FLOAT_NUM:
    case HEX_NUM:
    case LEX_HOSTNAME:
    case LONG_NUM:
    case NUM:
    case TEXT_STRING:
    case NCHAR_STRING:
    case ULONGLONG_NUM:
    {
      token = TOK_STAT_GENERIC_VALUE;
    }
    case NULL_SYM:
    {
      if ((last_token2 == TOK_STAT_GENERIC_VALUE ||
           last_token2 == TOK_STAT_GENERIC_VALUE_LIST ||
           last_token2 == NULL_SYM) &&
          (last_token == ','))
      {
        info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
        token = TOK_STAT_GENERIC_VALUE_LIST;
      }
      store_token(info, token);
      if (info->or_index != 0 && info->left_brackets_after_or == 0)
      {
        info->or_end_index = info->sql_byte_count;
        stat_or_token_cmp(info);
      }
      break;
    }
    case ')':
    {
      if (last_token == TOK_STAT_GENERIC_VALUE &&
           last_token2 == '(')
      {
        info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
        token = TOK_STAT_ROW_SINGLE_VALUE;

        peek_last_two_tokens(info, &last_token, &last_token2);

        if ((last_token2 == TOK_STAT_ROW_SINGLE_VALUE ||
             last_token2 == TOK_STAT_ROW_SINGLE_VALUE_LIST) &&
             (last_token == ','))
        {
          info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
          token = TOK_STAT_ROW_SINGLE_VALUE_LIST;
        }
      }
      else if (last_token == TOK_STAT_GENERIC_VALUE_LIST &&
               last_token2 == '(')
      {
        info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
        token = TOK_STAT_ROW_MULTIPLE_VALUE;

        peek_last_two_tokens(info, &last_token, &last_token2);

        if ((last_token2 == TOK_STAT_ROW_MULTIPLE_VALUE ||
             last_token2 == TOK_STAT_ROW_MULTIPLE_VALUE_LIST) &&
             (last_token == ','))
        {
          info->sql_byte_count -= 2 * SIZE_OF_A_TOKEN;
          token = TOK_STAT_ROW_MULTIPLE_VALUE_LIST;
        }
      }
      store_token(info, token);
      if (info->or_index != 0 && --info->left_brackets_after_or == 0)
      {
        info->or_end_index = info->sql_byte_count;
        stat_or_token_cmp(info);
      }
      break; 
    }
    case '(':
    {
      /* if or found, we must find the right value of or,
         maybe it's a multi-value, so we must store the  '('
      */
      if (info->or_index != 0)
        info->left_brackets_after_or++;
      store_token(info, token);
      break;
    }
    case IDENT:
    case IDENT_QUOTED:
    {
      /*ident quoted such as the table name, column name and so on */
      LEX_YYSTYPE *lex_token = (LEX_YYSTYPE*)yylval;
      char *yytext = lex_token->lex_str.str;
      int yylen = lex_token->lex_str.length;
      store_token_identifier(info, token, yylen, yytext);
      info->last_id_index = info->sql_byte_count;
      break;
    }
    case OR_SYM:
    {
      /* or token found, we must store the right condition start position */
      info->left_brackets_after_or = 0;
      info->or_end_index = 0;
      info->or_index = info->sql_byte_count;
      store_token(info,token);
      break;
    }
    default:
    {
      store_token(info, token);
      break;
    }
  }
  DBUG_VOID_RETURN;
}

static 
void compute_query_digest(char digest[], unsigned char *query, int length)
{
  unsigned char md5[MD5_SIZE];
  MY_MD5_HASH(md5, query, length);
  MD5_HASH_TO_STRING(md5, digest);
}

static void
stat_start_sql_statement(THD *thd)
{
  TABLE_LIST *table;
  if ((thd->lex->sql_command != SQLCOM_SELECT &&
      thd->lex->sql_command != SQLCOM_INSERT &&
      thd->lex->sql_command != SQLCOM_UPDATE &&
      thd->lex->sql_command != SQLCOM_DELETE) ||
      stat_shutdown || statistics_plugin_status == FALSE)
      return;
  TABLE_LIST *all_tables = thd->lex->query_tables;
  const char *dbname = NULL, *tablename = NULL;
  for (table=all_tables; table; table=table->next_global)
  {
    int i = 0;
    dbname = table->db;
    tablename = table->alias;
    while(sys_database[i] != NULL)
    {
      if (strcmp(sys_database[i],table->db) == 0)
        break;
      i++;
    }
    if (sys_database[i] != NULL) continue;
    thread_safe_increment(status_variables.table_enter_counts, &status_variables.status_lock);
    Table_Info *info = (Table_Info*)my_malloc(sizeof(Table_Info),MYF(MY_WME));
    sprintf(info->table_name,"%s.%s",dbname, tablename);
    info->cmd = thd->lex->sql_command;
    TableStats *hash_t = NULL;
    mysql_mutex_lock(&hash_table_lock);
    if ((hash_t = (TableStats *)my_hash_search(&stat_table_hash,(uchar*)info->table_name,strlen(info->table_name))) != NULL)
    {
      *hash_t += *info;
    }
    else
    {
      hash_t = new TableStats(*info);
      my_hash_insert(&stat_table_hash, (uchar*)hash_t);
    }
    mysql_mutex_unlock(&hash_table_lock);
    my_free(info);
  }
}

static void
stat_end_sql_statement(THD *thd)
{
  SQLInfo *info = thd->m_sql_info;
  if (info == NULL) return;
  DBUG_ENTER("stat end sql statement");
  if (info->is_stopped || stat_shutdown || info->is_full 
        || info->exclude || statistics_plugin_status == 0)
  {
    if (info->exclude)
    {
      thread_safe_increment(status_variables.discard_because_exclude, &status_variables.status_lock);
    }
    if (info->is_full)
    {
      sql_print_warning("too long sql statement:%s",thd->query());
    }
    DBUG_VOID_RETURN;
  }
  Statistics *s = new Statistics(info);
  s->m_exec_times = GET_QUERY_EXEC_TIME(info);
  s->m_max_exec_times = s->m_min_exec_times = s->m_exec_times;
  Statistics *hash_s = NULL;
  thread_safe_increment(status_variables.sql_enter_counts, &status_variables.status_lock);
  mysql_mutex_lock(&hash_sql_lock);
  if (( hash_s = (Statistics *)my_hash_search(&stat_sql_hash, (uchar*)s->m_sql_format, s->length)) != NULL)
  {
    *hash_s += *s;
    delete s;
  }
  else
  {
    if (stat_sql_hash.records < statistics_max_sql_count)
    {
      my_hash_insert(&stat_sql_hash,(uchar*)s);
      statistics_array.array[statistics_array.length] = s;
      statistics_array.length++;
    }
    else
    {
      status_variables.discard_because_hashtable_full++;
      delete s;
    }
  }
  mysql_mutex_unlock(&hash_sql_lock);
  DBUG_VOID_RETURN;
}

static void
remove_expire_stats()
{
  THD *thd;
  TABLE *table;
  TABLE_LIST tables[2];
  READ_RECORD read_record_info;

  my_thread_init();
  if (!(thd = new THD)) return;
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  ulonglong current_time = my_time(0);

  tables[0].init_one_table(C_STRING_WITH_LEN("mysql"),
                           C_STRING_WITH_LEN("sql_stats"),
                          "sql_stats",TL_WRITE_CONCURRENT_INSERT);
  tables[1].init_one_table(C_STRING_WITH_LEN("mysql"),
                          C_STRING_WITH_LEN("table_stats"),
                          "table_stats", TL_WRITE_CONCURRENT_INSERT);
  tables[0].next_local= tables[0].next_global= tables + 1;

  if (open_and_lock_tables(thd, tables, FALSE, MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    if (thd->stmt_da->is_error())
    {
      sql_print_error("Fatal error: Can't open and lock privilege tables: %s",thd->stmt_da->message());
      close_mysql_tables(thd);
      goto end;
    }                             
  }
  for(int i = 0; i < 2; i++)
  {
    if (tables[i].table)
    {
      init_read_record(&read_record_info, thd, table=tables[i].table, NULL, 1, 0, FALSE);
      table->use_all_columns();
      while (!(read_record_info.read_record(&read_record_info)))
      {
        char *ptr = get_field(thd->mem_root, table->field[0]);
        ulonglong start_time = strtoull(ptr, NULL, 10);
        if (current_time - start_time > statistics_expire_duration * SECONDS_PER_DAY)
        {
          table->file->ha_delete_row(table->record[0]);
        }
      }
      end_read_record(&read_record_info);
    }
  }
  close_mysql_tables(thd);
  status_variables.clean_counts++;
end:
  delete thd;
  my_pthread_setspecific_ptr(THR_THD,  0);
  my_pthread_setspecific_ptr(THR_MALLOC,  0);
}

static void
store_sql_stats()
{
  THD *thd;
  TABLE *table;
  char *buffer = NULL;
  TABLE_LIST tables;

  my_thread_init();

  tables.init_one_table(C_STRING_WITH_LEN("mysql"),
                  C_STRING_WITH_LEN("sql_stats"),
                  "sql_stats",TL_WRITE_CONCURRENT_INSERT);
  
  if (!(thd = new THD)) return;

  thd->thread_stack= (char*) &thd;
  thd->store_globals();

  if(open_and_lock_tables(thd, &tables, FALSE, MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    if (thd->stmt_da->is_error())
    {
      sql_print_error("Fatal error: Can't open and lock privilege tables: %s",thd->stmt_da->message());
      close_mysql_tables(thd);
      goto end;
    }
  }
  if ((table = tables.table) == NULL || table->file->ha_rnd_init(0))
  {
    close_mysql_tables(thd);
    goto end;
  }

  table->use_all_columns();
  buffer = (char*)my_malloc(MAX_SQL_TEXT_SIZE,MYF(MY_WME));
  mysql_mutex_lock(&hash_sql_lock);
  for(uint i = 0; i < statistics_array.length; i++)
  {
    Statistics *s = statistics_array.array[i];
    if ( s == NULL) continue;
    table->field[0]->store(start_output_time, TRUE);
    table->field[1]->store(end_output_time, TRUE);
    get_sql_text(buffer,s);
    table->field[2]->store(buffer,strlen(buffer),&my_charset_bin);
    compute_query_digest(buffer,s->m_sql_format,s->length);
    table->field[3]->store(buffer, strlen(buffer), &my_charset_bin);

    void *index = s->m_index_queue.new_iterator();
    char *p = buffer;
    while(index != NULL)
    {
      INDEX_INFO *info = s->m_index_queue.iterator_value(index);
      sprintf(p, "%s(%d),",info->index_name.str,info->index_reads);
      while(*p != '\0') p++; 
      index = s->m_index_queue.iterator_next(index);
    }
    if (p != buffer)
    {
      p--;*p = '\0';
    }
    else
    {
      sprintf(buffer,"NULL");
    }
    table->field[4]->store(buffer,strlen(buffer), &my_charset_bin);
    table->field[5]->store(s->m_memory_temp_table_created, TRUE);
    table->field[6]->store(s->m_disk_temp_table_created, TRUE);
    table->field[7]->store(s->m_row_reads, TRUE);
    table->field[8]->store(s->m_byte_reads, TRUE);
    table->field[9]->store(s->m_max_exec_times, TRUE);
    table->field[10]->store(s->m_min_exec_times, TRUE);
    table->field[11]->store(s->m_exec_times, TRUE);
    table->field[12]->store(s->m_exec_count, TRUE);

    table->field[0]->set_notnull();
    table->field[1]->set_notnull();
    table->field[2]->set_notnull();
    table->field[3]->set_notnull();
    table->field[4]->set_notnull();
    table->field[5]->set_notnull();
    table->field[6]->set_notnull();
    table->field[7]->set_notnull();
    table->field[8]->set_notnull();
    table->field[9]->set_notnull();
    table->field[10]->set_notnull();
    table->field[11]->set_notnull();
    table->field[12]->set_notnull();
    if(table->file->ha_write_row(table->record[0]))
      break;
  }
  my_free(buffer);
  my_hash_reset(&stat_sql_hash);
  statistics_array.length = 0;
  mysql_mutex_unlock(&hash_sql_lock);
  table->file->ha_rnd_end();
  close_mysql_tables(thd);
end:
  delete thd;
  my_pthread_setspecific_ptr(THR_THD,  0);
  my_pthread_setspecific_ptr(THR_MALLOC,  0);
}

static void
store_table_stats()
{
  THD *thd;
  TABLE *table;
  TABLE_LIST tables;

  my_thread_init();

  tables.init_one_table(C_STRING_WITH_LEN("mysql"),
    C_STRING_WITH_LEN("table_stats"),
    "table_stats",TL_WRITE_CONCURRENT_INSERT);

  if (!(thd = new THD)) return;

  thd->thread_stack= (char*) &thd;
  thd->store_globals();

  if(open_and_lock_tables(thd, &tables, FALSE, MYSQL_LOCK_IGNORE_TIMEOUT))
  {
    if (thd->stmt_da->is_error())
    {
      sql_print_error("Fatal error: Can't open and lock privilege tables: %s",thd->stmt_da->message());
      close_mysql_tables(thd);
      goto end;
    }
  }
  if ((table = tables.table) == NULL || table->file->ha_rnd_init(0))
  {
    close_mysql_tables(thd);
    goto end;
  }

  table->use_all_columns();
  mysql_mutex_lock(&hash_table_lock);
  for(uint i = 0; i < stat_table_hash.records; i++)
  {
    char buffer[NAME_CHAR_LEN] = {0};
    TableStats *ts = (TableStats*)my_hash_element(&stat_table_hash, i);
    if (ts == NULL) continue;
    table->field[0]->store(start_output_time, TRUE);
    table->field[1]->store(end_output_time, TRUE);
    const char *p = ts->table_name;
    while(p != NULL && *p != '.') p++;
    strncpy(buffer, ts->table_name, (p - ts->table_name));
    table->field[2]->store(buffer,strlen(buffer),&my_charset_bin);
    strcpy(buffer, p + 1);
    table->field[3]->store(buffer, strlen(buffer), &my_charset_bin);
    table->field[4]->store(ts->select_count, TRUE);
    table->field[5]->store(ts->insert_count, TRUE);
    table->field[6]->store(ts->update_count, TRUE);
    table->field[7]->store(ts->delete_count, TRUE);

    table->field[0]->set_notnull();
    table->field[1]->set_notnull();
    table->field[2]->set_notnull();
    table->field[3]->set_notnull();
    table->field[4]->set_notnull();
    table->field[5]->set_notnull();
    table->field[6]->set_notnull();
    table->field[7]->set_notnull();

    if(table->file->ha_write_row(table->record[0]))
      break;
  }
  my_hash_reset(&stat_table_hash);
  mysql_mutex_unlock(&hash_table_lock);
  table->file->ha_rnd_end();
  close_mysql_tables(thd);
end:
  delete thd;
  my_pthread_setspecific_ptr(THR_THD,  0);
  my_pthread_setspecific_ptr(THR_MALLOC,  0);
}

pthread_handler_t stat_output_thread(void *arg)
{
  stat_thread_exit = 1;
  while(!stat_shutdown)
  {
    struct timespec  abstime;
    set_timespec(abstime, statistics_output_cycle * SECONDS_PER_HOUR);
    mysql_mutex_lock(&stat_output_lock);
    start_output_time = my_time(0);
    mysql_cond_timedwait(&stat_output_cond, &stat_output_lock, &abstime);
    end_output_time = my_time(0);
    status_variables.output_counts++;
    mysql_mutex_unlock(&stat_output_lock);
    if (stat_shutdown == TRUE && statistics_shutdown_fast == TRUE)
      break;
    if (*statistics_readonly)
    {
      mysql_mutex_lock(&hash_sql_lock);
      my_hash_reset(&stat_sql_hash);
      statistics_array.length = 0;
      mysql_mutex_unlock(&hash_sql_lock);

      mysql_mutex_lock(&hash_table_lock);
      my_hash_reset(&stat_table_hash);
      mysql_mutex_unlock(&hash_table_lock);
    }
    else
    {
      store_sql_stats();
      store_table_stats();
      remove_expire_stats();
    }
  }
  stat_thread_exit = 0;
  return NULL;
}

static void
stat_create_threads()
{
  pthread_t temp;
  stat_thread_exit = 0;
  mysql_thread_create(0, &temp, NULL, stat_output_thread, NULL);
}

static void
stat_plugin_shutdown()
{
  DBUG_ENTER("stat plugin shutdown");
  stat_shutdown = TRUE;
  Statistics *s = NULL;
  mysql_cond_signal(&stat_output_cond);
  while(stat_thread_exit != 0)
  {
    sleep(1);
  }
  destory_statistics_hash();
  mysql_mutex_destroy(&stat_output_lock);
  mysql_cond_destroy(&stat_output_cond);
  mysql_mutex_destroy(&status_variables.status_lock);
  DBUG_VOID_RETURN;
}

static
int
statistics_init(void *p)
{
  STATISTICS_INTERFACE *si = (STATISTICS_INTERFACE*)p;
  DBUG_ENTER("statistics_init");
  si->si_add_token = stat_add_token;
  si->si_create_sql_info = stat_create_sql_info;
  si->si_reset_sqlinfo = stat_reset_sqlinfo;
  si->si_start_sql_statement = stat_start_sql_statement;
  si->si_end_sql_statement = stat_end_sql_statement;
  si->si_destory_sql_info = stat_destory_sql_info;
  si->si_show_sql_stats = stat_show_sql_stats;
  si->si_show_table_stats = stat_show_table_stats;
  si->si_show_plugin_status = stat_show_plugin_status;
  si->si_save_index = stat_save_index;
  si->si_exclude_current_sql = stat_exclude_current_sql;
  mysql_mutex_init(0, &stat_output_lock,MY_MUTEX_INIT_FAST);
  mysql_cond_init(0, &stat_output_cond, NULL);
  mysql_mutex_init(0, &status_variables.status_lock, MY_MUTEX_INIT_FAST);
  if(*(statistics_readonly = si->readonly) == TRUE)
  {
    sql_print_information("start with --read-only, the stats info will not to be outputted");
  }
  stat_create_threads();
  init_statistics_hash();
  DBUG_RETURN(0);
}

static
int
statistics_deinit(void *p)
{
  DBUG_ENTER("statistics_deinit");
  statistics_shutdown_fast = TRUE;
  stat_plugin_shutdown();
  DBUG_RETURN(0);
}

static int show_statistics_vars(THD *thd, SHOW_VAR *var, char *buff)
{
  return 0;
}

mysql_declare_plugin(statistics)
{
  MYSQL_STATISTICS_PLUGIN,
  &statistics_engine,
  statistics_hton_name,
  plugin_author,
  "Top SQL statistics plugin",
  PLUGIN_LICENSE_GPL,
  statistics_init,
  statistics_deinit,
  0x0100,
  statistics_status_variables,
  statistics_system_variables,
  NULL,
  0,
}
mysql_declare_plugin_end;
