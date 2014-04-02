/* Copyright (c) 2005, 2012, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef RPL_RLI_H
#define RPL_RLI_H

#include "rpl_tblmap.h"
#include "rpl_reporting.h"
#include "rpl_utility.h"
#include "log.h"                         /* LOG_INFO, MYSQL_BIN_LOG */
#include "sql_class.h"                   /* THD */
#include "rpl_info_handler.h"
#include "mysql.h"
#include <semaphore.h>

struct RPL_TABLE_LIST;
class Master_info;
extern uint sql_slave_skip_counter;
static const int LINES_IN_RELAY_LOG_INFO_WITH_DELAY= 4;

/****************************************************************************

  Replication SQL Thread

  Relay_log_info contains:
    - the current relay log
    - the current relay log offset
    - master log name
    - master log sequence corresponding to the last update
    - misc information specific to the SQL thread

  Relay_log_info is initialized from the slave.info file if such
  exists.  Otherwise, data members are intialized with defaults. The
  initialization is done with init_relay_log_info() call.

  The format of slave.info file:

  relay_log_name
  relay_log_pos
  master_log_name
  master_log_pos

  To clean up, call end_relay_log_info()

*****************************************************************************/
/*transfer*/
#define SLAVE_THREAD_MIN 2
#define SLAVE_THREAD_MAX 64
#define SLAVE_THREAD 16

#define BATCH_BUF_STEP (1024*100)

typedef struct st_hash_item
{
  int times;
  int key_len;
  char key[1024+2*NAME_CHAR_LEN+2];
} hash_item_t;

struct trans_pos_t
{
  my_off_t log_pos;
  time_t when;
  char relay_log_name[FN_REFLEN];
  my_off_t relay_log_pos;
  bool done;
};

class transaction_st
{
public:
  int worker_id;
  int batch_buf_len;
  char *batch_buf;
  int batch_curr_max;
  int inner_events;
  Log_event *event_list_head;
  Log_event *event_list_tail;
  bool contains_stat;
  bool contains_no_pk_row;
  HASH trans_pk_hash;
  void add_pk_item(hash_item_t *new_item);
  void add_event(Log_event *event);
  void print_events();
  void free_events();
  int conflict_workers[SLAVE_THREAD_MAX];
  int conflict_number;
  my_off_t log_pos;
  time_t when;
  char relay_log_name[FN_REFLEN];
  my_off_t relay_log_pos;
  trans_pos_t *trans_pos;
  Relay_log_info *rli;

  /* This is used to keep transaction commit order. */
  wait_for_commit commit_orderer;

  /*
    This is the ID (Relay_log_info::last_trans_id) of the previous transaction,
    that we want to wait for before committing ourselves (if ordered commits
	are enforced).
  */
  uint64 wait_commit_id;
  /*
    This is the transaction whose commit we want to wait for
    Only valid if Relay_log_info::last_committed_id < wait_commit_id.
  */
  transaction_st *wait_commit_trans;
  /*
    This is our own transaction id, which we should update
    Relay_log_info::last_committed_id to once we commit.
  */
  uint64 own_commit_id;

  transaction_st();
  ~transaction_st();
};

#define worker_size 100

class Transfer_worker
{
public:
  Transfer_worker(Relay_log_info *rli);
  ~Transfer_worker();
  int run();
  int start();
  int stop();
  int wait_for_stopped();
  int push_trans(Relay_log_info *rli, transaction_st *trans);
  int pop_trans(int batch_trans_n);
  int remove_trans(transaction_st *trans);
  bool check_trans_conflict(transaction_st *trans);
  int execute_transcation(transaction_st *trans);
  int prepare_verbose_event(char *temp_buf);
  int inline get_trans_number(){return waiting_trans_number;};
  int inline get_trans_number_complete(){return complete_trans_number;};
  bool check_pk_exist_key(hash_item_t *item);
  int pk_hash_plus(transaction_st *trans);
  int pk_hash_minus(transaction_st *trans);

public:
  bool terminated;
  bool init_ok;
private:
  transaction_st *trans_list[worker_size];
  int waiting_trans_number;
  int complete_trans_number;
  int list_head, list_end;
  rw_lock_t trans_list_lock;
  HASH pk_hash;
  rw_lock_t pk_hash_lock;
  Relay_log_info *rli;
  Relay_log_info *dummy_rli;
  bool running;
  sem_t event_sem;

  THD *thd;
};
/*transfer end*/

class Relay_log_info : public Slave_reporting_capability
{
public:
  /**
     Flags for the state of the replication.
   */
  enum enum_state_flag {
    /** The replication thread is inside a statement */
    IN_STMT,

    /** Flag counter.  Should always be last */
    STATE_FLAGS_COUNT
  };
  my_bool enable_rli_table; 
  Rpl_info_handler *handler;
  /*
    If flag set, then rli does not store its state in any info file.
    This is the case only when we execute BINLOG SQL commands inside
    a client, non-replication thread.
  */
  bool no_storage;

  /*
    If true, events with the same server id should be replicated. This
    field is set on creation of a relay log info structure by copying
    the value of ::replicate_same_server_id and can be overridden if
    necessary. For example of when this is done, check sql_binlog.cc,
    where the BINLOG statement can be used to execute "raw" events.
   */
  bool replicate_same_server_id;

  /*** The following variables can only be read when protect by data lock ****/

  /*
    info_fd - file descriptor of the info file. set only during
    initialization or clean up - safe to read anytime
    cur_log_fd - file descriptor of the current read  relay log
  */
  File info_fd,cur_log_fd;

  /*
    Protected with internal locks.
    Must get data_lock when resetting the logs.
  */
  MYSQL_BIN_LOG relay_log;
  LOG_INFO linfo;

  /*
   cur_log
     Pointer that either points at relay_log.get_log_file() or
     &rli->cache_buf, depending on whether the log is hot or there was
     the need to open a cold relay_log.

   cache_buf 
     IO_CACHE used when opening cold relay logs.
   */
  IO_CACHE cache_buf,*cur_log;

  /*
    Keeps track of the number of transactions that commits
    before fsyncing. The option --sync-relay-log-info determines 
    how many transactions should commit before fsyncing.
  */ 
  uint sync_counter;

  /*
    Identifies when the recovery process is going on.
    See sql/slave.cc:init_recovery for further details.
  */ 
  bool is_relay_log_recovery;

  /* The following variables are safe to read any time */

  /* IO_CACHE of the info file - set only during init or end */
  IO_CACHE info_file;

  /*
    When we restart slave thread we need to have access to the previously
    created temporary tables. Modified only on init/end and by the SQL
    thread, read only by SQL thread.
  */
  TABLE *save_temporary_tables;

  /*
    standard lock acquisition order to avoid deadlocks:
    run_lock, data_lock, relay_log.LOCK_log, relay_log.LOCK_index
  */
  mysql_mutex_t data_lock, run_lock, sleep_lock, table_lock;
  /*
    start_cond is broadcast when SQL thread is started
    stop_cond - when stopped
    data_cond - when data protected by data_lock changes
  */
  mysql_cond_t start_cond, stop_cond, data_cond, sleep_cond;
  /* parent Master_info structure */
  Master_info *mi;

  /*
    Needed to deal properly with cur_log getting closed and re-opened with
    a different log under our feet
  */
  uint32 cur_log_old_open_count;
  
  /*
    Let's call a group (of events) :
      - a transaction
      or
      - an autocommiting query + its associated events (INSERT_ID,
    TIMESTAMP...)
    We need these rli coordinates :
    - relay log name and position of the beginning of the group we currently are
    executing. Needed to know where we have to restart when replication has
    stopped in the middle of a group (which has been rolled back by the slave).
    - relay log name and position just after the event we have just
    executed. This event is part of the current group.
    Formerly we only had the immediately above coordinates, plus a 'pending'
    variable, but this dealt wrong with the case of a transaction starting on a
    relay log and finishing (commiting) on another relay log. Case which can
    happen when, for example, the relay log gets rotated because of
    max_binlog_size.
  */
  char group_relay_log_name[FN_REFLEN];
  ulonglong group_relay_log_pos;
  char event_relay_log_name[FN_REFLEN];
  ulonglong event_relay_log_pos;
  ulonglong future_event_relay_log_pos;

  /*transfer*/
  Relay_log_info *main_rli;
  bool getting_pk_value;
  transaction_st *curr_trans;

  mysql_rwlock_t LOCK_workers;
  Transfer_worker *workers[SLAVE_THREAD_MAX];

  trans_pos_t trans_pos[SLAVE_THREAD_MAX *worker_size];
  int trans_number, trans_head, trans_end;
  rw_lock_t trans_pos_lock;
  int sql_thread;

  int push_back_trans_pos(transaction_st *trans);
  int rollback_trans_pos(transaction_st *trans);
  int pop_front_trans_pos();
  /* Running counter for assigning IDs to event groups/transactions. */
  uint64 last_trans_id;
  /* The transaction_st corresponding to last_trans_id. */
  transaction_st *last_trans;
  /*
    The ID of the last transaction/event group that was committed/applied.
    This is used to decide if the next transaction should wait for the
    previous one to commit (to avoid trying to wait for a commit that already
    took place)
  */
  uint64 last_committed_id;
  /* Mutex protecting access to last_committed_id. */
  mysql_mutex_t LOCK_last_committed_id;
  /*If there is a trans execute fails, this is set true*/
  bool trans_fail;
  /* Mutex protecting access to trans_fail. */
  mysql_mutex_t LOCK_trans_fail;
  /*transfer end*/

#ifdef HAVE_purify
  bool is_fake; /* Mark that this is a fake relay log info structure */
#endif

  /* 
     Original log name and position of the group we're currently executing
     (whose coordinates are group_relay_log_name/pos in the relay log)
     in the master's binlog. These concern the *group*, because in the master's
     binlog the log_pos that comes with each event is the position of the
     beginning of the group.
  */
  char group_master_log_name[FN_REFLEN];
  char tmp_group_master_log_name[FN_REFLEN];
  volatile my_off_t group_master_log_pos;

  /*
    Handling of the relay_log_space_limit optional constraint.
    ignore_log_space_limit is used to resolve a deadlock between I/O and SQL
    threads, the SQL thread sets it to unblock the I/O thread and make it
    temporarily forget about the constraint.
  */
  ulonglong log_space_limit,log_space_total;
  bool ignore_log_space_limit;

  /*
    Used by the SQL thread to instructs the IO thread to rotate 
    the logs when the SQL thread needs to purge to release some
    disk space.
   */
  bool sql_force_rotate_relay;

  /*
    When it commits, InnoDB internally stores the master log position it has
    processed so far; the position to store is the one of the end of the
    committing event (the COMMIT query event, or the event if in autocommit
    mode).
  */
#if MYSQL_VERSION_ID < 40100
  ulonglong future_master_log_pos;
#else
  ulonglong future_group_master_log_pos;
#endif

  time_t last_master_timestamp;

  size_t get_number_info_rli_fields();
  bool read_table_info();
  bool write_table_trans();
  bool write_file_info();
  bool write_table_event(ulonglong log_pos);

  bool read_file_info(const char* info_fname);
  void clear_until_condition();
  bool flush_info();
  bool reach_relay_log_end(my_off_t log_pos);
  /*
    Needed for problems when slave stops and we want to restart it
    skipping one or more events in the master log that have caused
    errors, and have been manually applied by DBA already.
  */
  volatile uint32 slave_skip_counter;
  volatile ulong abort_pos_wait;	/* Incremented on change master */
  volatile ulong slave_run_id;		/* Incremented on slave start */
  mysql_mutex_t log_space_lock;
  mysql_cond_t log_space_cond;
  THD * sql_thd;
#ifndef DBUG_OFF
  int events_till_abort;
#endif  

  /*
    inited changes its value within LOCK_active_mi-guarded critical
    sections  at times of start_slave_threads() (0->1) and end_slave() (1->0).
    Readers may not acquire the mutex while they realize potential concurrency
    issue.
    If not set, the value of other members of the structure are undefined.
  */
  volatile bool inited;
  volatile bool abort_slave;
  volatile uint slave_running;

  /* 
     Condition and its parameters from START SLAVE UNTIL clause.
     
     UNTIL condition is tested with is_until_satisfied() method that is 
     called by exec_relay_log_event(). is_until_satisfied() caches the result
     of the comparison of log names because log names don't change very often;
     this cache is invalidated by parts of code which change log names with
     notify_*_log_name_updated() methods. (They need to be called only if SQL
     thread is running).
   */
  
  enum {UNTIL_NONE= 0, UNTIL_MASTER_POS, UNTIL_RELAY_POS} until_condition;
  char until_log_name[FN_REFLEN];
  ulonglong until_log_pos;
  /* extension extracted from log_name and converted to int */
  ulong until_log_name_extension;   
  /* 
     Cached result of comparison of until_log_name and current log name
     -2 means unitialised, -1,0,1 are comarison results 
  */
  enum 
  { 
    UNTIL_LOG_NAMES_CMP_UNKNOWN= -2, UNTIL_LOG_NAMES_CMP_LESS= -1,
    UNTIL_LOG_NAMES_CMP_EQUAL= 0, UNTIL_LOG_NAMES_CMP_GREATER= 1
  } until_log_names_cmp_result;

  char cached_charset[6];
  /*
    trans_retries varies between 0 to slave_transaction_retries and counts how
    many times the slave has retried the present transaction; gets reset to 0
    when the transaction finally succeeds. retried_trans is a cumulative
    counter: how many times the slave has retried a transaction (any) since
    slave started.
  */
  ulong trans_retries, retried_trans;

  /*
    If the end of the hot relay log is made of master's events ignored by the
    slave I/O thread, these two keep track of the coords (in the master's
    binlog) of the last of these events seen by the slave I/O thread. If not,
    ign_master_log_name_end[0] == 0.
    As they are like a Rotate event read/written from/to the relay log, they
    are both protected by rli->relay_log.LOCK_log.
  */
  char ign_master_log_name_end[FN_REFLEN];
  ulonglong ign_master_log_pos_end;

  /* 
    Indentifies where the SQL Thread should create temporary files for the
    LOAD DATA INFILE. This is used for security reasons.
   */ 
  char slave_patternload_file[FN_REFLEN]; 
  size_t slave_patternload_file_size;  

  Relay_log_info(bool is_slave_recovery, bool is_rli_table);
  ~Relay_log_info();

  /*
    Invalidate cached until_log_name and group_relay_log_name comparison 
    result. Should be called after any update of group_realy_log_name if
    there chances that sql_thread is running.
  */
  inline void notify_group_relay_log_name_update()
  {
    if (until_condition==UNTIL_RELAY_POS)
      until_log_names_cmp_result= UNTIL_LOG_NAMES_CMP_UNKNOWN;
  }
  
  /*
    The same as previous but for group_master_log_name. 
  */
  inline void notify_group_master_log_name_update()
  {
    if (until_condition==UNTIL_MASTER_POS)
      until_log_names_cmp_result= UNTIL_LOG_NAMES_CMP_UNKNOWN;
  }

  inline bool is_transactional()
  {
    return (handler->is_transactional());
  }

  inline const char* get_group_master_log_name() { return group_master_log_name; }
  inline ulonglong get_group_master_log_pos() { return group_master_log_pos; }
  inline void set_group_master_log_name(const char *log_file_name)
  {
     strmake(group_master_log_name,log_file_name, sizeof(group_master_log_name)-1);
  }
  inline void set_group_master_log_pos(ulonglong log_pos)
  {
    group_master_log_pos= log_pos;
  }

  inline const char* get_group_relay_log_name() { return group_relay_log_name; }
  inline ulonglong get_group_relay_log_pos() { return group_relay_log_pos; }
  inline void set_group_relay_log_name(const char *log_file_name)
  {
     strmake(group_relay_log_name,log_file_name, sizeof(group_relay_log_name)-1);
  }
  inline void set_group_relay_log_name(const char *log_file_name, size_t len)
  {
     strmake(group_relay_log_name, log_file_name, len);
  }
  inline void set_group_relay_log_pos(ulonglong log_pos)
  {
    group_relay_log_pos= log_pos;
  }

  inline const char* get_event_relay_log_name() { return event_relay_log_name; }
  inline ulonglong get_event_relay_log_pos() { return event_relay_log_pos; }
  
  inline void inc_event_relay_log_pos()
  {
    event_relay_log_pos= future_event_relay_log_pos;
  }

  void inc_group_relay_log_pos(ulonglong log_pos,
			       bool skip_lock=0, char *relay_log_name=NULL, ulonglong relay_log_pos=0);

  int wait_for_pos(THD* thd, String* log_name, longlong log_pos, 
		   longlong timeout);
  void close_temporary_tables();

  /* Check if UNTIL condition is satisfied. See slave.cc for more. */
  bool is_until_satisfied(THD *thd, Log_event *ev);
  inline ulonglong until_pos()
  {
    return ((until_condition == UNTIL_MASTER_POS) ? group_master_log_pos :
	    group_relay_log_pos);
  }

  RPL_TABLE_LIST *tables_to_lock;           /* RBR: Tables to lock  */
  uint tables_to_lock_count;        /* RBR: Count of tables to lock */
  table_mapping m_table_map;      /* RBR: Mapping table-id to table */

  bool get_table_data(TABLE *table_arg, table_def **tabledef_var, TABLE **conv_table_var) const
  {
    DBUG_ASSERT(tabledef_var && conv_table_var);
    for (TABLE_LIST *ptr= tables_to_lock ; ptr != NULL ; ptr= ptr->next_global)
      if (ptr->table == table_arg)
      {
        *tabledef_var= &static_cast<RPL_TABLE_LIST*>(ptr)->m_tabledef;
        *conv_table_var= static_cast<RPL_TABLE_LIST*>(ptr)->m_conv_table;
        DBUG_PRINT("debug", ("Fetching table data for table %s.%s:"
                             " tabledef: %p, conv_table: %p",
                             table_arg->s->db.str, table_arg->s->table_name.str,
                             *tabledef_var, *conv_table_var));
        return true;
      }
    return false;
  }

  /*
    Last charset (6 bytes) seen by slave SQL thread is cached here; it helps
    the thread save 3 get_charset() per Query_log_event if the charset is not
    changing from event to event (common situation).
    When the 6 bytes are equal to 0 is used to mean "cache is invalidated".
  */
  void cached_charset_invalidate();
  bool cached_charset_compare(char *charset) const;

  void cleanup_context(THD *, bool);
  void slave_close_thread_tables(THD *);
  void clear_tables_to_lock();

  /*
    Used to defer stopping the SQL thread to give it a chance
    to finish up the current group of events.
    The timestamp is set and reset in @c sql_slave_killed().
  */
  time_t last_event_start_time;

  /*
    A container to hold on Intvar-, Rand-, Uservar- log-events in case
    the slave is configured with table filtering rules.
    The withhold events are executed when their parent Query destiny is
    determined for execution as well.
  */
  Deferred_log_events *deferred_events;

  /*
    State of the container: true stands for IRU events gathering, 
    false does for execution, either deferred or direct.
  */
  bool deferred_events_collecting;

  /* 
     Returns true if the argument event resides in the containter;
     more specifically, the checking is done against the last added event.
  */
  bool is_deferred_event(Log_event * ev)
  {
    return deferred_events_collecting ? deferred_events->is_last(ev) : false;
  };
  /* The general cleanup that slave applier may need at the end of query. */
  inline void cleanup_after_query()
  {
    if (deferred_events)
      deferred_events->rewind();
  };
  /* The general cleanup that slave applier may need at the end of session. */
  void cleanup_after_session()
  {
    if (deferred_events)
      delete deferred_events;
  };
   
  /**
    Helper function to do after statement completion.

    This function is called from an event to complete the group by
    either stepping the group position, if the "statement" is not
    inside a transaction; or increase the event position, if the
    "statement" is inside a transaction.

    @param event_log_pos
    Master log position of the event. The position is recorded in the
    relay log info and used to produce information for <code>SHOW
    SLAVE STATUS</code>.

    @param event_creation_time
    Timestamp for the creation of the event on the master side. The
    time stamp is recorded in the relay log info and used to compute
    the <code>Seconds_behind_master</code> field.
  */
  void stmt_done(my_off_t event_log_pos, time_t event_creation_time, char *relay_log_name=NULL,
                 ulonglong relay_log_pos=0, bool update_table= true);

  /**
     Set the value of a replication state flag.

     @param flag Flag to set
   */
  void set_flag(enum_state_flag flag)
  {
    m_flags |= (1UL << flag);
  }

  /**
     Get the value of a replication state flag.

     @param flag Flag to get value of

     @return @c true if the flag was set, @c false otherwise.
   */
  bool get_flag(enum_state_flag flag)
  {
    return m_flags & (1UL << flag);
  }

  /**
     Clear the value of a replication state flag.

     @param flag Flag to clear
   */
  void clear_flag(enum_state_flag flag)
  {
    m_flags &= ~(1UL << flag);
  }

  /**
     Is the replication inside a group?

     Replication is inside a group if either:
     - The OPTION_BEGIN flag is set, meaning we're inside a transaction
     - The RLI_IN_STMT flag is set, meaning we're inside a statement

     @retval true Replication thread is currently inside a group
     @retval false Replication thread is currently not inside a group
   */
  bool is_in_group() const {
    return (sql_thd->variables.option_bits & OPTION_BEGIN) ||
      (m_flags & (1UL << IN_STMT));
  }

  time_t get_row_stmt_start_timestamp()
  {
    return row_stmt_start_timestamp;
  }

  time_t set_row_stmt_start_timestamp()
  {
    if (row_stmt_start_timestamp == 0)
      row_stmt_start_timestamp= my_time(0);

    return row_stmt_start_timestamp;
  }

  void reset_row_stmt_start_timestamp()
  {
    row_stmt_start_timestamp= 0;
  }

  void set_long_find_row_note_printed()
  {
    long_find_row_note_printed= true;
  }

  void unset_long_find_row_note_printed()
  {
    long_find_row_note_printed= false;
  }

  bool is_long_find_row_note_printed()
  {
    return long_find_row_note_printed;
  }

private:

  uint32 m_flags;

  /*
    Runtime state for printing a note when slave is taking
    too long while processing a row event.
   */
  time_t row_stmt_start_timestamp;
  bool long_find_row_note_printed;
};


// Defined in rpl_rli.cc
int init_relay_log_info(Relay_log_info* rli, const char* info_fname);


#endif /* RPL_RLI_H */
