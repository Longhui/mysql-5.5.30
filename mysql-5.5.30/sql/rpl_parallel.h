#ifndef RPL_PARALLEL_H
#define RPL_PARALLEL_H

//#include "log_event.h"
#include "hash.h"
#include "mysql_com.h"
#include "my_pthread.h"
//#include "rpl_rli.h"
class THD;
struct rpl_group_info;
class Log_event;
class Relay_log_info;

extern class rpl_threads_pool global_rpl_thread_pool;

enum enum_slave_parallel_mode { OPT_ROW_BINLOG, OPT_GROUP_COMMIT };
/**************************************************
*          Row-based rpl parallel struct          *
***************************************************/

typedef struct hash_item_t
{
 int times;
 int key_len;
 char key[1024+2*NAME_CHAR_LEN+2];
} hash_item;

struct queue_event
{
  Log_event* evt;
  queue_event* next;
  rpl_group_info *rgi;

  char future_event_master_log_name[FN_REFLEN];
  ulonglong future_event_relay_log_pos;
  char event_relay_log_name[FN_REFLEN];
  ulonglong event_relay_log_pos;
  my_off_t future_event_master_log_pos;
  size_t event_size;
  bool is_relay_log_event;
};

struct trans_t
{
public:
  HASH event_key;
  queue_event* head, *tail;
  struct trans_t *next;
  rpl_group_info *rgi;
  bool contain_stmt;
  bool contain_nopk_row;
  uint num_evt;
  
  trans_t(rpl_group_info *rgi);
  ~trans_t();
  int apply();
  void add_event(struct queue_event *qev);
  void insert_event_pk(struct queue_event *qev);
  void insert_pk_item(hash_item *item);  
  void enqueue(struct queue_event *qev);
};

/**************************************************
*                   threads pool                  *
**************************************************/
struct rpl_thread;
class rpl_threads_pool
{
public:
  uint32 thread_num;
  bool changing;
  bool inited;
  rpl_thread** threads;
  /*
    @raolh
    For row-based parallel replication, all threads is always
    on free_list until change_size().
  */
  rpl_thread* free_list;
  
  mysql_mutex_t LOCK_rpl_threads_pool;
  mysql_cond_t COND_rpl_threads_pool;

  rpl_threads_pool();
  int init(uint32 size);
  void destory();
  rpl_thread* get_thread();
  void release_thread(rpl_thread *rpt);

  /*
    @raolh
    Function used in Row-based parallel rpl.
  */
  rpl_thread *dispatch_thread(struct trans_t* trans);
  int wait_to_idle();
};

/**************************************************
*                  rpl thread handler             *
**************************************************/

/*
  Structure used to keep track of the parallel replication of a batch of
  event-groups that group-committed together on the master.

  It is used to ensure that every event group in one batch has reached the
  commit stage before the next batch starts executing.

  Note the lifetime of this structure:

   - It is allocated when the first event in a new batch of group commits
     is queued, from the free list rpl_parallel_entry::gco_free_list.

   - The gco for the batch currently being queued is owned by
     rpl_parallel_entry::current_gco. The gco for a previous batch that has
     been fully queued is owned by the gco->prev_gco pointer of the gco for
     the following batch.

   - The worker thread waits on gco->COND_group_commit_orderer for
     rpl_parallel_entry::count_committed_event_groups to reach wait_count
     before starting; the first waiter links the gco into the next_gco
     pointer of the gco of the previous batch for signalling.

   - When an event group reaches the commit stage, it signals the
     COND_group_commit_orderer if its gco->next_gco pointer is non-NULL and
     rpl_parallel_entry::count_committed_event_groups has reached
     gco->next_gco->wait_count.

   - When gco->wait_count is reached for a worker and the wait completes,
     the worker frees gco->prev_gco; at this point it is guaranteed not to
     be needed any longer.
*/

struct group_commit_orderer {
  /* Wakeup condition, used with rpl_parallel_entry::LOCK_parallel_entry. */
  mysql_cond_t COND_group_commit_orderer;
  uint64 wait_count;
  group_commit_orderer *prev_gco;
  group_commit_orderer *next_gco;
  bool installed;
};

struct rpl_thread
{
  THD *thd;
  bool running;
  bool stop;
  bool abort_force;
  bool delay_start;
  bool is_using;
  class rpl_threads_pool *pool;
  mysql_mutex_t LOCK_rpl_thread;
  mysql_cond_t COND_rpl_thread;
/*
  @raolh
  used in parallel replication based on group commit.
*/  
  struct rpl_thread *next;
  uint64 queued_size;
  struct queue_event *event_queue, *last_in_queue;
  queue_event *qev_free_list;
  rpl_group_info *rgi_free_list;
  group_commit_orderer *gco_free_list;

queue_event*
get_qev(Log_event *ev, ulonglong event_size,
         Relay_log_info *rli);

void free_qev(queue_event *qev);

rpl_group_info* 
get_rgi(Relay_log_info *rli);

void free_rgi(rpl_group_info *rgi);

group_commit_orderer*
get_gco(uint64 wait_count, group_commit_orderer *prev);

void free_gco(group_commit_orderer *gco);

/*
  @raolh
  used in parallel replication based on Row format binlog.
*/
  HASH trans_key;
  uint32 trans_num;
  uint32 n_executed_trans;
  trans_t *head, *tail;
 
/*
  @raolh
  function used in Group-commit larallel rpl
*/
  void enqueue(queue_event *qev)
  {
    if (last_in_queue)
      last_in_queue->next= qev;
    else
      event_queue= qev;
    last_in_queue= qev;
    queued_size+= qev->event_size;
  }
 
  void dequeue1(queue_event *list)
  {
    DBUG_ASSERT(list == event_queue);
    event_queue= last_in_queue= NULL;
  }
  
  void dequeue2(size_t total_size)
  {
    queued_size-= total_size;
  }
 
/*
  @raolh
  Function used in Row-based parallel rpl
*/
  void add_trans(trans_t *trans, bool need_lock);
  trans_t* get_trans();
  bool conflict(struct trans_t* trans);

  void add_pk_hash(trans_t *trans);
  void del_pk_hash(trans_t *trans);
};

/**************************************************
*                 rpl parallel entry              *
**************************************************/
class rpl_parallel {
public:
  char zero[250];
  bool force_abort;
  uint64 stop_count;
  bool sql_thread_stopping;
  /*
    @raolh
    The newest committed transaction's sub id
  */
  uint64 last_committed_sub_id;
  /*
    @raolh
    The last transaction's sub id in previous transaction group
  */
  uint64 prev_groupcommit_sub_id;
  /*
    @raolh
    The newest transaction's sub id
  */
  uint64 current_sub_id;
  /*
    @raolh
    The last transactions group's commit id
  */
  uint64 last_group_commit_id;
 
  /*
    @raolh
    The newest transaction's rgi
  */
  rpl_group_info *current_group_info;
  mysql_mutex_t LOCK_rpl_parallel;
  mysql_cond_t COND_rpl_parallel;
  Relay_log_info *rli;
  uchar flag;
  static const uchar FL_IN_GROUP= 1;
 
  /*
    If we get an error in some event group, we set the sub_id of that event
    group here. Then later event groups (with higher sub_id) can know not to
    try to start (event groups that already started will be rolled back when
    wait_for_prior_commit() returns error).
    The value is ULONGLONG_MAX when no error occured.
  */
  uint64 stop_on_error_sub_id;
  /* Total count of event groups queued so far. */
  uint64 count_queued_event_groups;
  /*
    Count of event groups that have started (but not necessarily completed)
    the commit phase. We use this to know when every event group in a previous
    batch of master group commits have started committing on the slave, so
    that it is safe to start executing the events in the following batch.
  */
  uint64 count_committed_event_groups;
  /* The group_commit_orderer object for the events currently being queued. */
  group_commit_orderer *current_gco;


  rpl_parallel(Relay_log_info *rli_);
  ~rpl_parallel();
  void reset();
  virtual void wait_for_done()=0;
  virtual bool workers_idle()=0;
  virtual bool do_event(rpl_group_info *serial_rgi, Log_event *ev, size_t event_size)=0;
};

class rpl_row_parallel : public rpl_parallel
{
public:
  trans_t* current_trans;
  rpl_threads_pool* pool;
  /*
    @raolh
    Thread handled the lastest transaction,it is used to
    wait sub threads apply all transaction.
  */
  struct rpl_thread* rpt_cur;

  rpl_row_parallel(Relay_log_info *rli_) : rpl_parallel(rli_), 
    current_trans(NULL), pool(&global_rpl_thread_pool), rpt_cur(NULL)
  {}
  void wait_for_done();
  bool workers_idle(){return TRUE;};
  bool do_event(rpl_group_info *serial_rgi, Log_event *ev, size_t event_size);
};

class rpl_group_parallel : public rpl_parallel
{
public:
  char zero[250];
  rpl_thread *cur_thread;
  rpl_threads_pool *pool;
  
   rpl_group_parallel(Relay_log_info *rli_) : rpl_parallel(rli_),
    cur_thread(NULL), pool(&global_rpl_thread_pool)
  {}

  rpl_thread*
  choose_thread(Relay_log_info *rli, bool reuse);
  void reset();
  void wait_for_done();
  bool workers_idle();
  bool do_event(rpl_group_info *serial_rgi, Log_event *ev, size_t event_size);
};

extern class rpl_group_parallel *rpl_group_entry;
extern class rpl_row_parallel *rpl_row_entry;
extern int rpl_parallel_change_thread_count(rpl_threads_pool *pool, uint32 new_count, bool skip_check);

#endif
