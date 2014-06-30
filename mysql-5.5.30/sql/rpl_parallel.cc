#include "my_global.h"
#include "rpl_parallel.h"
#include "slave.h"
#include "rpl_mi.h"
#include "sql_base.h"

#define TRANS_MAX 120

void *handle_rpl_row_thread(void *arg);
void *handle_rpl_group_thread(void *arg);

class rpl_group_parallel *rpl_group_entry= NULL;
class rpl_row_parallel *rpl_row_entry= NULL;
class rpl_threads_pool global_rpl_thread_pool;

void slave_output_error_info(Relay_log_info *rli, THD *thd);

static uint64 global_sub_id= 0;

inline uint64 new_sub_id()
{
  return ++global_sub_id;
}

static void
handle_queued_pos_update(THD *thd, queue_event *qev)
{
  int cmp;
  Relay_log_info *rli;
  /*
    Events that are not part of an event group, such as Format Description,
    Stop, GTID List and such, are executed directly in the driver SQL thread,
    to keep the relay log state up-to-date. But the associated position update
    is done here, in sync with other normal events as they are queued to
    worker threads.
  */
  if ((thd->variables.option_bits & OPTION_BEGIN) &&
      opt_using_transactions)
    return;
  rli= qev->rgi->rli;
  mysql_mutex_lock(&rli->data_lock);
  cmp= strcmp(rli->group_relay_log_name, qev->event_relay_log_name);
  if (cmp < 0)
  {
    rli->group_relay_log_pos= qev->future_event_relay_log_pos;
    strmake(rli->group_relay_log_name, qev->event_relay_log_name,
             sizeof(rli->group_relay_log_name)-1);
    rli->notify_group_relay_log_name_update();
  } else if (cmp == 0 &&
             rli->group_relay_log_pos < qev->future_event_relay_log_pos)
  {
    rli->group_relay_log_pos= qev->future_event_relay_log_pos;
  }
  /*
    @raolh
    in this case:
    "
    rotate1 (master server_id) end_log_pos1 (master binlog offset)
    rotate2 (slave server_id) end_log_pos2 (slave relaylog offset)
    "
    rli->group_relay_log_pos will be updated to end_log_pos2, for rotate1 has
    update rli->future_master_log_file. so, we must skip rotate2.
  */
  if ( !qev->is_relay_log_event )
  {
    cmp= strcmp(rli->group_master_log_name, qev->future_event_master_log_name);
    if (cmp < 0)
    {
      strmake(rli->group_master_log_name, qev->future_event_master_log_name,
              sizeof(rli->group_master_log_name)-1);
      rli->notify_group_master_log_name_update();
      rli->group_master_log_pos= qev->future_event_master_log_pos;
    }
    else if (cmp == 0
          && rli->group_master_log_pos < qev->future_event_master_log_pos)
    {
      rli->group_master_log_pos= qev->future_event_master_log_pos;
    }
  }
   flush_relay_log_info(&active_mi->rli);
   mysql_mutex_unlock(&rli->data_lock);
   mysql_cond_broadcast(&rli->data_cond);
}

static bool
sql_worker_killed(THD *thd, rpl_group_info *rgi, bool in_event_group)
{
  if (!rgi->rli->abort_slave && !abort_loop)
    return false;

  /*
    Do not abort in the middle of an event group that cannot be rolled back.
  */
  if ((thd->transaction.all.modified_non_trans_table ||
       (thd->variables.option_bits & OPTION_KEEP_LOG))
      && in_event_group)
    return false;
  /* ToDo: should we add some timeout like in sql_slave_killed?
       if (rgi->last_event_start_time == 0)
         rgi->last_event_start_time= my_time(0);
  */

  return true;
}

static int
rpt_handle_event(queue_event *qev)
{
  int err __attribute__((unused));
  rpl_group_info *rgi= qev->rgi;
  Relay_log_info *rli= rgi->rli;
  THD *thd= rgi->thd;

  thd->rgi_slave= rgi;
  /* ToDo: Access to thd, and what about rli, split out a parallel part? */
  mysql_mutex_lock(&rli->data_lock);
  qev->evt->thd= thd;
  strcpy(rgi->event_relay_log_name_buf, qev->event_relay_log_name);
  rgi->event_relay_log_name= rgi->event_relay_log_name_buf;
  rgi->event_relay_log_pos= qev->event_relay_log_pos;
  rgi->future_event_relay_log_pos= qev->future_event_relay_log_pos;
  rgi->event_master_log_pos= qev->future_event_master_log_pos;
  strcpy(rgi->future_event_master_log_name, qev->future_event_master_log_name);
  err= apply_event_and_update_pos(qev->evt, thd, rgi);
  thd->rgi_slave= NULL;

  /* ToDo: error handling. */
  return err;
}

uchar *
hash_item_get_key(const uchar *record, size_t *length, my_bool not_used __attribute__((unused)))
{
  hash_item *entry= (hash_item*) record;
  *length= entry->key_len;
  return (uchar*) entry->key;
}

static void
hash_item_free_entry(hash_item *record)
{
  delete record;
}

trans_t::trans_t(rpl_group_info *rgi_) : 
head(NULL), tail(NULL), next(NULL), rgi(rgi_),
contain_stmt(false), contain_nopk_row(false), num_evt(0)
{ 
  rgi->trans= this; 
  (void) my_hash_init(&event_key,
                  &my_charset_bin, 256+16, 0, 0,
                  (my_hash_get_key) hash_item_get_key,
                  (my_hash_free_key) hash_item_free_entry,
                   MYF(0));
}

void trans_t::enqueue(struct queue_event *qev)
{
  /*
    @raolh
    There always be one thread insert events to
    a trans_t struct, so it needn't to take any lock.
  */
  if (head == NULL)
  {
    head= tail= qev;
  }
  else
  {
    tail->next= qev;
    tail= qev;
  }
  num_evt++;
  tail->next= NULL;
  if (!contain_stmt)
    insert_event_pk(qev);
}

void trans_t::insert_pk_item(hash_item *item)
{
  hash_item *t = (hash_item *)my_hash_search(&event_key, (const uchar*)item->key, item->key_len);
  if (t)
  {
    delete item;
    t->times++;
  }
  else
  {
    item->times = 1;
    my_hash_insert(&event_key, (const uchar*)item);
  }
}

trans_t::~trans_t()
{
  if (num_evt > 0)
  {
    queue_event *cur= NULL;
    for (cur= head; cur!= NULL; cur= cur->next)
    {
      delete cur->evt;
      my_free(cur);
    }
  }
  my_hash_free(&event_key);
  if (rgi)
    delete rgi;
}


/*
  return value:
  0 ok
  1 error
*/
int trans_t::apply()
{
  DBUG_ASSERT(head != NULL);
  DBUG_ASSERT(num_evt > 2);
  int err= 0;
  THD *thd= rgi->thd;
  thd->rgi_slave= rgi;
 
  queue_event *events;
  for (events= head; events != NULL; events = events->next)
  {
    events->evt->thd= thd;
    if (!rgi->is_error && !sql_worker_killed(thd, rgi, true))
      err= rpt_handle_event(events);
    else
      err= thd->wait_for_prior_commit();
    /*
      @raolh
      should it delete a event just after execute,
      or should delete it after execute whole group.
    
    delete_or_keep_event_post_apply(rgi, event_typ, events->ev);
    */
    if (err > 0)
      break;
  }
  /*
    @raolh
    When any event applied failly we do not delete them, for
    all events will be deleted when destory trans_t struct at
    later.
  */
  return err;
}

void trans_t::add_event(struct queue_event *qev)
{
  qev->evt->thd= rgi->thd;
  int ev_type= qev->evt->get_type_code();
  if ( ev_type == NEW_LOAD_EVENT ||
       ev_type == LOAD_EVENT || 
       ev_type == EXECUTE_LOAD_QUERY_EVENT ||
       ev_type == BEGIN_LOAD_QUERY_EVENT ||
       ev_type == CREATE_FILE_EVENT ||
       ev_type == DELETE_FILE_EVENT ||
       ev_type == APPEND_BLOCK_EVENT)
    contain_stmt = true;
  if ( ev_type == QUERY_EVENT &&
       !( ((Query_log_event*)qev->evt)->is_begin() || 
          ((Query_log_event*)qev->evt)->is_commit()|| 
          ((Query_log_event*)qev->evt)->is_rollback() ))
    contain_stmt = true;
  if ( ev_type == TABLE_MAP_EVENT && !contain_stmt )
  {
    qev->evt->apply_event(rgi);
  }
  enqueue(qev);
}

class DML_prelocking_strategy;
void trans_t::insert_event_pk(struct queue_event * qev)
{
  int ev_type= qev->evt->get_type_code();
  if (!(ev_type == WRITE_ROWS_EVENT ||
        ev_type == UPDATE_ROWS_EVENT|| 
        ev_type == DELETE_ROWS_EVENT ))
    return;

  THD *thd= rgi->thd;
  TABLE_LIST *tables= rgi->tables_to_lock;
  DML_prelocking_strategy prelocking_strategy;
  uint counter;
  DBUG_ASSERT(thd != NULL);
  DBUG_ASSERT(tables != NULL);

  Rows_log_event* revt= (Rows_log_event*)(qev->evt);
  if( tables->table ||
      !open_tables(thd, &tables, &counter, 0, &prelocking_strategy))
  {
    revt->get_pk_value(rgi);
  }
  if(revt->get_flags(Rows_log_event::STMT_END_F))
  {
    rgi->cleanup_context(thd, 0);
    thd->clear_error();
  }
}

static void
finish_event_group(THD *thd, int err, rpl_group_info *rgi, rpl_parallel *entry)
{
  wait_for_commit *wfc= &rgi->commit_orderer;
  uint64 sub_id= rgi->sub_id;
  /*
    Remove any left-over registration to wait for a prior commit to
    complete. Normally, such wait would already have been removed at
    this point by wait_for_prior_commit() called from within COMMIT
    processing. However, in case of MyISAM and no binlog, we might not
    have any commit processing, and so we need to do the wait here,
    before waking up any subsequent commits, to preserve correct
    order of event execution. Also, in the error case we might have
    skipped waiting and thus need to remove it explicitly.

    It is important in the non-error case to do a wait, not just an
    unregister. Because we might be last in a group-commit that is
    replicated in parallel, and the following event will then wait
    for us to complete and rely on this also ensuring that any other
    event in the group has completed.

    But in the error case, we have to abort anyway, and it seems best
    to just complete as quickly as possible with unregister. Anyone
    waiting for us will in any case receive the error back from their
    wait_for_prior_commit() call.
  */
  if (err)
    wfc->unregister_wait_for_prior_commit();
  else
    wfc->wait_for_prior_commit();
  thd->wait_for_commit_ptr= NULL;

  /*
    Record that this event group has finished (eg. transaction is
    committed, if transactional), so other event groups will no longer
    attempt to wait for us to commit. Once we have increased
    entry->last_committed_sub_id, no other threads will execute
    register_wait_for_prior_commit() against us. Thus, by doing one
    extra (usually redundant) wakeup_subsequent_commits() we can ensure
    that no register_wait_for_prior_commit() can ever happen without a
    subsequent wakeup_subsequent_commits() to wake it up.

    We can race here with the next transactions, but that is fine, as
    long as we check that we do not decrease last_committed_sub_id. If
    this commit is done, then any prior commits will also have been
    done and also no longer need waiting for.
  */
  mysql_mutex_lock(&entry->LOCK_rpl_parallel);
  if (entry->last_committed_sub_id < sub_id)
  {
    entry->last_committed_sub_id= sub_id;
  }
  /*
    If this event group got error, then any following event groups that have
    not yet started should just skip their group, preparing for stop of the
    SQL driver thread.
  */
  if (unlikely(rgi->is_error) &&
      entry->stop_on_error_sub_id == (uint64)ULONGLONG_MAX)
    entry->stop_on_error_sub_id= sub_id;
  /*
    We need to mark that this event group started its commit phase, in case we
    missed it before (otherwise we would deadlock the next event group that is
    waiting for this). In most cases (normal DML), it will be a no-op.
  */
  rgi->mark_start_commit_no_lock();
  mysql_mutex_unlock(&entry->LOCK_rpl_parallel);

  thd->clear_error();
  wfc->wakeup_subsequent_commits(rgi->is_error);
}

/*
  @raolh
  Insert a transaction into a worker's train-list,
  for worker is geting transactons from its train-list
  so we need get lock first.
*/
void rpl_thread::add_trans(trans_t *trans, bool need_lock)
{
  DBUG_ASSERT(trans);
  is_using= true;
  if (need_lock)
    mysql_mutex_lock(&LOCK_rpl_thread);
  if(head == NULL)
  {
    head= tail= trans;
  }
  else
  {
    tail->next= trans;
    tail= trans;
  }
  trans->rgi->thd= thd;
  queue_event* qev= NULL;
  for (qev= trans->head; qev != NULL; qev= qev->next)
  {
    qev->evt->thd= thd;
  }
  add_pk_hash(trans);
  tail->next= NULL;
  trans_num++;
  if (need_lock)
  {
    mysql_mutex_unlock(&LOCK_rpl_thread);
    mysql_cond_signal(&COND_rpl_thread);
  }
}

/*
  @raolh
  Get all trans from trans-list one time
  and set trans-list to NULL
*/
trans_t* rpl_thread::get_trans()
{
  trans_t *list;
  mysql_mutex_assert_owner(&LOCK_rpl_thread);
  list= head;
  head= tail= NULL;
  trans_num= 0;
  return list;
}

/*
  @raolh
  judge a transaction confict with a thread by
  comparing their primary key hash table.
  
  return value:
  true: yes
  false: no
*/
bool rpl_thread::conflict(struct trans_t* trans)
{
  hash_item *target_item, *inner_item= NULL;
  uint i= 0;
  for (i=0 ; i < trans->event_key.records; i++)
  {
    target_item= (hash_item *)my_hash_element(&trans->event_key, i);
    inner_item= (hash_item *)my_hash_search(&trans_key,(uchar*)target_item->key, target_item->key_len);
    if (inner_item)
    {
      return true;
    }
  }
  return false;
}

void rpl_thread::add_pk_hash(trans_t *trans)
{
  hash_item *inner, *target;
  uint32 i;
  for (i=0; i < trans->event_key.records; i++)
  {
    target= (hash_item *)my_hash_element(&trans->event_key, i);
    inner= (hash_item *)my_hash_search(&trans_key,(const uchar*)target->key,target->key_len);
    if(inner)
    {
      inner->times+= target->times;
    }
    else
    {
      inner= new hash_item_t;
      memcpy(inner, target, sizeof(hash_item));
      my_hash_insert(&trans_key, (const uchar*)inner);
    }
  }
}

void rpl_thread::del_pk_hash(trans_t *trans)
{
  hash_item *inner, *target;
  uint32 i;
  for (i=0; i < trans->event_key.records; i++)
  {
    target= (hash_item *)my_hash_element(&trans->event_key, i);
    inner= (hash_item *)my_hash_search(&trans_key,(const uchar*)target->key,target->key_len);
    if(inner)
    {
      inner->times-= target->times;
      if (inner->times <=0)
      {
        my_hash_delete(&trans_key, (uchar*)inner);
      }
    }
  }
}

rpl_threads_pool::rpl_threads_pool():
  thread_num(0), changing(false),inited(false),
  threads(NULL), free_list(NULL)
{
}

int rpl_parallel_change_thread_count(rpl_threads_pool *pool, uint32 new_count, bool skip_check);

int rpl_threads_pool::init(uint32 size)
{
  int error;
  mysql_mutex_init(key_LOCK_rpl_thread_pool, &LOCK_rpl_threads_pool,
                   MY_MUTEX_INIT_SLOW);
  mysql_cond_init(key_COND_rpl_thread_pool, &COND_rpl_threads_pool, NULL);
  changing= false;
  inited= true;
  error= rpl_parallel_change_thread_count(this, size, true);
  return error;
}

static void
dealloc_gco(group_commit_orderer *gco)
{
  DBUG_ASSERT(!gco->prev_gco /* Must only free after dealloc previous */);
  mysql_cond_destroy(&gco->COND_group_commit_orderer);
  my_free(gco);
}

void rpl_threads_pool::destory()
{
  if (!inited)
   return;
  rpl_parallel_change_thread_count(this, 0, true);
  mysql_mutex_destroy(&LOCK_rpl_threads_pool);
  mysql_cond_destroy(&COND_rpl_threads_pool);
  inited= false;
}

int rpl_parallel_change_thread_count(rpl_threads_pool *pool, uint32 new_count, bool skip_check)
{
  uint32 i;
  rpl_thread **new_list= NULL;
  rpl_thread *new_free_list= NULL;
  rpl_thread *rpt_array= NULL;

  /*
    Allocate the new list of threads up-front.
    That way, if we fail half-way, we only need to free whatever we managed
    to allocate, and will not be left with a half-functional thread pool.
  */
  if (new_count &&
      !my_multi_malloc(MYF(MY_WME|MY_ZEROFILL),
                       &new_list, new_count*sizeof(*new_list),
                       &rpt_array, new_count*sizeof(*rpt_array),
                       NULL))
  {
    my_error(ER_OUTOFMEMORY,MYF(0),(int(new_count*sizeof(*new_list) +
                                        new_count*sizeof(*rpt_array))));
    goto err;
  }

  for (i= 0; i < new_count; ++i)
  {
    pthread_t th;
    new_list[i]= &rpt_array[i];
    new_list[i]->delay_start= true;
    new_list[i]->qev_free_list= NULL;
    new_list[i]->gco_free_list= NULL;
    new_list[i]->rgi_free_list= NULL;
    new_list[i]->is_using= false;
    new_list[i]->n_executed_trans= 0;
    
    mysql_mutex_init(key_LOCK_rpl_thread, &new_list[i]->LOCK_rpl_thread,
                     MY_MUTEX_INIT_SLOW);
    mysql_cond_init(key_COND_rpl_thread, &new_list[i]->COND_rpl_thread, NULL);
    /*
      @raolh
      In Row-based parallel, build a hash table for each thread.
    */
    if (OPT_ROW_BINLOG == opt_slave_parallel_mode)
    {
      my_hash_init( &new_list[i]->trans_key,&my_charset_bin,256+16,0,0,
                  (my_hash_get_key)hash_item_get_key,
                    (my_hash_free_key)hash_item_free_entry,
                    MYF(0) );
    }
    new_list[i]->pool= pool;
    if (OPT_GROUP_COMMIT == opt_slave_parallel_mode)
    {
      if (mysql_thread_create(key_rpl_parallel_thread, &th, NULL,
                              handle_rpl_group_thread, new_list[i]))
      {
        my_error(ER_OUT_OF_RESOURCES, MYF(0));
        goto err;
      }
    }
    else
    {
      if (mysql_thread_create(key_rpl_parallel_thread, &th, NULL,
                              handle_rpl_row_thread, new_list[i]))
      {
        my_error(ER_OUT_OF_RESOURCES, MYF(0));
        goto err;
      }
 
    }
    new_list[i]->next= new_free_list;
    new_free_list= new_list[i];
  }

  if (!skip_check)
  {
    /*
      @raolh
      It should STOP SLAVE before change buffer pool size.
    */
    mysql_mutex_lock(&LOCK_active_mi);
    if (active_mi->rli.slave_running != MYSQL_SLAVE_NOT_RUN)
    {
      my_message(ER_SLAVE_MUST_STOP, ER(ER_SLAVE_MUST_STOP), MYF(0));
      mysql_mutex_unlock(&LOCK_active_mi);
      goto err;
    }
    /*
      @raolh
      If buffer pool size is changing, this one should stop.
    */
    if (pool->changing)
    {
      mysql_mutex_unlock(&LOCK_active_mi);
      my_error(ER_CHANGE_SLAVE_PARALLEL_THREADS_ACTIVE, MYF(0));
      goto err;
    }
    pool->changing= true;
    mysql_mutex_unlock(&LOCK_active_mi);
  }

  /*
    Grab each old thread in turn, and signal it to stop.

    Note that since we require all replication threads to be stopped before
    changing the parallel replication worker thread pool, all the threads will
    be already idle and will terminate immediately.
  */
  for (i= 0; i < pool->thread_num; ++i)
  {
    rpl_thread *rpt= pool->get_thread();
    rpt->stop= true;
    mysql_cond_signal(&rpt->COND_rpl_thread);
    mysql_mutex_unlock(&rpt->LOCK_rpl_thread);
  }

  for (i= 0; i < pool->thread_num; ++i)
  {
    rpl_thread *rpt= pool->threads[i];
    mysql_mutex_lock(&rpt->LOCK_rpl_thread);
    while (rpt->running)
      mysql_cond_wait(&rpt->COND_rpl_thread, &rpt->LOCK_rpl_thread);
    mysql_mutex_unlock(&rpt->LOCK_rpl_thread);
    mysql_mutex_destroy(&rpt->LOCK_rpl_thread);
    mysql_cond_destroy(&rpt->COND_rpl_thread);
    while (rpt->qev_free_list)
    {
      queue_event *next= rpt->qev_free_list->next;
      my_free(rpt->qev_free_list);
      rpt->qev_free_list= next;
    }
    while (rpt->rgi_free_list)
    {
      rpl_group_info *next= rpt->rgi_free_list->next;
      delete rpt->rgi_free_list;
      rpt->rgi_free_list= next;
    }
    while (rpt->gco_free_list)
    {
      group_commit_orderer *next= rpt->gco_free_list->next_gco;
      dealloc_gco(rpt->gco_free_list);
      rpt->gco_free_list= next;
    }
  }

  my_free(pool->threads);
  pool->threads= new_list;
  pool->free_list= new_free_list;
  pool->thread_num= new_count;
  for (i= 0; i < pool->thread_num; ++i)
  {
    mysql_mutex_lock(&pool->threads[i]->LOCK_rpl_thread);
    pool->threads[i]->delay_start= false;
    mysql_cond_signal(&pool->threads[i]->COND_rpl_thread);
    while (!pool->threads[i]->running)
      mysql_cond_wait(&pool->threads[i]->COND_rpl_thread,
                       &pool->threads[i]->LOCK_rpl_thread);
    mysql_mutex_unlock(&pool->threads[i]->LOCK_rpl_thread);
  }

  mysql_mutex_lock(&pool->LOCK_rpl_threads_pool);
  mysql_cond_broadcast(&pool->COND_rpl_threads_pool);
  mysql_mutex_unlock(&pool->LOCK_rpl_threads_pool);
  
if (!skip_check)
  {
    mysql_mutex_lock(&LOCK_active_mi);
    pool->changing= false;
    mysql_mutex_unlock(&LOCK_active_mi);
  }

  return 0;

err:
  if (new_list)
  {
    while (new_free_list)
    {
      mysql_mutex_lock(&new_free_list->LOCK_rpl_thread);
      new_free_list->delay_start= false;
      new_free_list->stop= true;
      mysql_cond_signal(&new_free_list->COND_rpl_thread);
      while (!new_free_list->running)
        mysql_cond_wait(&new_free_list->COND_rpl_thread,
                        &new_free_list->LOCK_rpl_thread);
      while (new_free_list->running)
        mysql_cond_wait(&new_free_list->COND_rpl_thread,
                        &new_free_list->LOCK_rpl_thread);
      mysql_mutex_unlock(&new_free_list->LOCK_rpl_thread);
      new_free_list= new_free_list->next;
    }
    my_free(new_list);
  }
  if (!skip_check)
  {
    mysql_mutex_lock(&LOCK_active_mi);
    pool->changing= false;
    mysql_mutex_unlock(&LOCK_active_mi);
  }
  return 1;
}

/*
  Release a thread to the thread pool.
  The thread should be locked, and should not have any work queued for it.
*/
void
rpl_threads_pool::release_thread(rpl_thread *rpt)
{
  rpl_thread *list;

  mysql_mutex_assert_owner(&rpt->LOCK_rpl_thread);
  DBUG_ASSERT(rpt->is_using == false);
  mysql_mutex_lock(&LOCK_rpl_threads_pool);
  list= free_list;
  rpt->next= list;
  free_list= rpt;
  if (!list)
    mysql_cond_broadcast(&COND_rpl_threads_pool);
  mysql_mutex_unlock(&LOCK_rpl_threads_pool);
}


rpl_thread* 
rpl_group_parallel::choose_thread(Relay_log_info *rli, bool reuse)
{
  rpl_thread* thr= NULL;
  
  if (reuse)
      thr= cur_thread;

  if (thr)
  {
    mysql_mutex_lock(&thr->LOCK_rpl_thread);
    for(;;)
    {
      if (!thr->is_using)
      {
        /*
          The worker thread became idle, and returned to the free list and
          possibly was allocated to a different request. This also means
          that everything previously queued has already been executed,
          else the worker thread would not have become idle. So we should
          allocate a new worker thread.
        */
         mysql_mutex_unlock(&thr->LOCK_rpl_thread);
         thr= NULL;
         break;
       }
       else if (thr->queued_size <= opt_slave_parallel_max_queued)
       {
         break;   // The thread is ready to queue into
       }
       else if (rli->sql_thd->killed)
       {
         mysql_mutex_unlock(&thr->LOCK_rpl_thread);
         slave_output_error_info(rli, rli->sql_thd);
         return NULL;
       }
       else
       {
         /*
           We have reached the limit of how much memory we are allowed to
           use for queuing events, so wait for the thread to consume some
           of its queue.
         */
         mysql_cond_wait(&thr->COND_rpl_thread, &thr->LOCK_rpl_thread);
       }
     }
   }
   if (!thr)
       thr= pool->get_thread();
   
   return thr;
}


rpl_thread* rpl_threads_pool::get_thread()
{
  rpl_thread *rpt;
  mysql_mutex_lock(&LOCK_rpl_threads_pool);
  while ((rpt= free_list) == NULL)
    mysql_cond_wait(&COND_rpl_threads_pool, &LOCK_rpl_threads_pool);
  free_list= rpt->next;
  mysql_mutex_unlock(&LOCK_rpl_threads_pool);
  mysql_mutex_lock(&rpt->LOCK_rpl_thread);
  rpt->is_using= true;

  return rpt;
}

void rpl_parallel::reset()
{
  force_abort= false;
  sql_thread_stopping= false;
  last_committed_sub_id= 0;
  prev_groupcommit_sub_id= 0;
  current_sub_id= 0;
  last_group_commit_id= 0;
  current_group_info= NULL;
  stop_on_error_sub_id= (uint64)ULONGLONG_MAX;
  count_queued_event_groups= 0;
  count_committed_event_groups= 0;
  current_gco= 0;
  flag= 0;
  global_sub_id= 0;
}

rpl_parallel::rpl_parallel(Relay_log_info *rli_): force_abort(false),
    sql_thread_stopping(false),last_committed_sub_id(0),
    prev_groupcommit_sub_id(0),current_sub_id(0),
    last_group_commit_id(0),current_group_info(NULL),
    rli(rli_),flag(0),stop_on_error_sub_id((uint64)ULONGLONG_MAX),
    count_queued_event_groups(0),
    count_committed_event_groups(0),
    current_gco(NULL)
{
  mysql_mutex_init(key_LOCK_rpl_parallel, &LOCK_rpl_parallel,
                   MY_MUTEX_INIT_SLOW);
  mysql_cond_init(key_COND_rpl_parallel, &COND_rpl_parallel, NULL);
}

rpl_parallel::~rpl_parallel()
{
  mysql_mutex_destroy(&LOCK_rpl_parallel);
  mysql_cond_destroy(&COND_rpl_parallel);
}


void *
handle_rpl_row_thread(void *arg)
{
  THD *thd;
  const char* old_msg;
  rpl_thread *rpt= (rpl_thread *)arg;
  trans_t *trans, *next;
  //rpl_row_thread *list;
  rpl_group_info *rgi;
  Relay_log_info *rli;
  uint32 wait_commit_sub_id;

  rpt->n_executed_trans= 0;
  rpt->trans_num= 0;
  
  my_thread_init();
  thd = new THD;
  thd->thread_stack = (char*)&thd;
  mysql_mutex_lock(&LOCK_thread_count);
  thd->thread_id= thd->variables.pseudo_thread_id= thread_id++;
  threads.append(thd);
  mysql_mutex_unlock(&LOCK_thread_count);
  set_current_thd(thd);
  pthread_detach_this_thread();
  thd->init_for_queries();
  init_thr_lock();
  thd->store_globals();
  thd->system_thread= SYSTEM_THREAD_SLAVE_SQL;
  thd->security_ctx->skip_grants();
  thd->variables.max_allowed_packet= slave_max_allowed_packet;
  thd->slave_thread= 1;
  thd->enable_slow_log= opt_log_slow_slave_statements;
  set_slave_thread_options(thd);
  thd->client_capabilities = CLIENT_LOCAL_FILES;
  thd_proc_info(thd, "Waiting for work from main SQL threads");
  thd->set_time();
  thd->variables.lock_wait_timeout= LONG_TIMEOUT;
  //lock
  mysql_mutex_lock(&rpt->LOCK_rpl_thread);
  rpt->thd= thd;
  while (rpt->delay_start)
    mysql_cond_wait(&rpt->COND_rpl_thread, &rpt->LOCK_rpl_thread);
  
  rpt->running= true;
  mysql_cond_signal(&rpt->COND_rpl_thread);
  
  while (!rpt->stop && !thd->killed)
  {
   /*
     @raolh
     we must own rpt->LOCK_rpl_thread here. In 1st loop the lock take at 
     front code. In more than 2st loop it took at back code.
   */
    old_msg= thd->proc_info;
    thd->enter_cond(&rpt->COND_rpl_thread, &rpt->LOCK_rpl_thread,
                    "Waiting for work from SQL thread");
    while ( !rpt->head && !rpt->stop && !thd->killed )
        mysql_cond_wait(&rpt->COND_rpl_thread, &rpt->LOCK_rpl_thread);
    trans= rpt->get_trans();
    //unlock
    thd->exit_cond(old_msg);
    mysql_cond_signal(&rpt->COND_rpl_thread);

more_trans:
    while(trans)
    {
      next= trans->next;
      rgi= trans->rgi;
      rli= rgi->rli;
      /*
        @raolh
        if SQL thread asked to abort
        there should be more things need to do.
        I think at lease
        1) release all leaved trans struct,
        2) wakeup trans waiting for the leaved trans.
        it hard to deal with this situation, so don't care it
        just execute all trans on the trans-list.
        
       if (rli->abort_slave)
          break;
      */
      wait_commit_sub_id= rgi->wait_commit_sub_id;
      if (wait_commit_sub_id)
      {
        mysql_mutex_lock(&rpl_row_entry->LOCK_rpl_parallel);
        if (wait_commit_sub_id > rpl_row_entry->last_committed_sub_id)
        {
          wait_for_commit *waitee= 
              &rgi->wait_commit_group_info->commit_orderer;
          rgi->commit_orderer.register_wait_for_prior_commit(waitee);
        }
        mysql_mutex_unlock(&rpl_row_entry->LOCK_rpl_parallel);
      }
      thd->wait_for_commit_ptr= &rgi->commit_orderer;
      int err= trans->apply();
      if (err)
      {
        rgi->is_error= true;
        slave_output_error_info(rgi->rli, thd);
        rgi->cleanup_context(thd, true);
        rli->abort_slave= true;
      }
      else
        rpt->n_executed_trans++;
      finish_event_group(thd, rgi->is_error, rgi, (rpl_parallel *)rpl_row_entry);
      /*
        @raolh
        wakeup main SQL thread checking primary key confliction
      */
      mysql_mutex_lock(&rpt->LOCK_rpl_thread);
      rpt->del_pk_hash(trans);
      mysql_cond_signal(&rpt->COND_rpl_thread);
      mysql_mutex_unlock(&rpt->LOCK_rpl_thread);

      delete trans;
      trans= next;
      
    } //while(trans)
    /*
      @raolh
      lock rpt->LOCK_rpl_thread again.
    */
    mysql_mutex_lock(&rpt->LOCK_rpl_thread);
    if (rpt->head)
    {
      trans= rpt->get_trans();
      mysql_mutex_unlock(&rpt->LOCK_rpl_thread);
      mysql_cond_signal(&rpt->COND_rpl_thread);
      goto more_trans;
    }
    /*
      @raolh
      we needn't move this thread to free list for it alawys
      on free list. this is compatible with get_thread() used in
      change_size().
    */
    rpt->is_using= false;
  } //while(!rpt->stop && !thd->killed)

  rpt->thd= NULL;
  mysql_mutex_unlock(&rpt->LOCK_rpl_thread);
  
  thd->clear_error();
  thd->catalog= 0;
  thd->reset_query();
  thd->reset_db(NULL, 0);
  thd_proc_info(thd, "Slave worker thread exiting");
  thd->temporary_tables= 0;
  mysql_mutex_lock(&LOCK_thread_count);
  THD_CHECK_SENTRY(thd);
  delete thd;
  mysql_mutex_unlock(&LOCK_thread_count);
  mysql_mutex_lock(&rpt->LOCK_rpl_thread);
  rpt->running= false;
  mysql_cond_signal(&rpt->COND_rpl_thread);
  mysql_mutex_unlock(&rpt->LOCK_rpl_thread);
  my_thread_end();
  return NULL;
}

/*
  @raolh
  dispatch a transaction to a suitable thread.
  principle is:
  1)if a transaction update non-primary key table, then dispatch it
     to thread 0;
  2)if a transaction conflinct with one thread, then dispatch it to 
     this thread;
  3)if a transaction don't conflict with any threads, then dispatch
     it to the thread contain leaset transaction;
  4)if a transaction conflict with tow or more threads ,then wait for
     some and dispatch again later.
*/
rpl_thread*
rpl_threads_pool::dispatch_thread(struct trans_t* trans)
{
  uint i=0;
  uint conflict_n;
  ulong min_trans= 9999;
  rpl_thread *rpt= NULL;
  trans->rgi->is_parallel_exec= true;

  /*transacton contained no-primary-key rows executes in thread 0*/

  if (trans->contain_nopk_row)
  {    
    mysql_mutex_lock(&threads[0]->LOCK_rpl_thread);
    while( threads[0]->trans_num > TRANS_MAX )
    {    
      mysql_cond_wait(&threads[0]->COND_rpl_thread, 
                      &threads[0]->LOCK_rpl_thread);
    }
    threads[0]->add_trans(trans, false);
    mysql_mutex_unlock(&threads[0]->LOCK_rpl_thread);
    mysql_cond_signal(&threads[0]->COND_rpl_thread);
    return threads[0];
  }
  
start:
  conflict_n= 0;
  for (i=0 ; i< thread_num; i++)
  {
    mysql_mutex_lock(&threads[i]->LOCK_rpl_thread);
    if (threads[i]->conflict(trans))
    {
      if (0 == i)
      {
        while (threads[i]->conflict(trans))
          mysql_cond_wait(&threads[i]->COND_rpl_thread, 
                          &threads[i]->LOCK_rpl_thread);
      }
      else if(0 == conflict_n)
      { 
        conflict_n++;
      }
      else
      {
        /*
          @raolh
          conflict with more than two threads,need tp wait
          some time and try later.
        */
        while (threads[i]->conflict(trans))
          mysql_cond_wait(&threads[i]->COND_rpl_thread, 
                          &threads[i]->LOCK_rpl_thread );
        mysql_mutex_unlock(&threads[i]->LOCK_rpl_thread);
        goto start;
      }
    }
    else if(threads[i]->trans_num >= TRANS_MAX || 0 == i)
    {
      mysql_mutex_unlock(&threads[i]->LOCK_rpl_thread);
      continue;
    }
    else if(min_trans > threads[i]->trans_num)
    {
      min_trans= threads[i]->trans_num;
      rpt= threads[i];
    }
    mysql_mutex_unlock(&threads[i]->LOCK_rpl_thread);
  }
  if (NULL == rpt)
  {//All threads wait_list is full, so wait for 1 second and try again
    sleep(1);
    goto start;
  }


  rpt->add_trans(trans, true);
 
 return rpt;
}
/*
  wait all threads become idle.
  case 1, threas complete all transaction
  case 2, threads stop for some error;
*/
int rpl_threads_pool::wait_to_idle()
{
  uint i, error= 0;
  for (i=0; i < thread_num; i++)
  {
    mysql_mutex_lock(&threads[i]->LOCK_rpl_thread);
    while (threads[i]->trans_num > 0 && threads[i]->running == true)
       mysql_cond_wait(&threads[i]->COND_rpl_thread, 
                       &threads[i]->LOCK_rpl_thread);
    if (threads[i]->running == false)
      error= 1;
    mysql_mutex_unlock(&threads[i]->LOCK_rpl_thread);
  }
  return error;
}

static void
signal_error_to_sql_driver_thread(THD *thd, rpl_group_info *rgi)
{
  rgi->is_error= true;
  rgi->cleanup_context(thd, true);
  rgi->rli->abort_slave= true;
  mysql_mutex_lock(rgi->rli->relay_log.get_log_lock());
  mysql_mutex_unlock(rgi->rli->relay_log.get_log_lock());
  rgi->rli->relay_log.signal_update();
}


void *
handle_rpl_group_thread(void *arg)
{
  THD *thd;
  const char* old_msg;
  struct queue_event *events;
  bool in_event_group= false;
  bool group_skip_for_stop= false;
  rpl_group_info *group_rgi= NULL;
  group_commit_orderer *gco, *tmp_gco;
  int err;
  queue_event *qevs_to_free;
  rpl_group_info *rgis_to_free;
  group_commit_orderer *gcos_to_free;
  size_t total_event_size;

  struct rpl_thread *rpt= (struct rpl_thread *)arg;

  my_thread_init();
  thd = new THD;
  /*
    @raolh
    set tx_isolation to read-committed to get rid of 
     possible dead-lock caused by gap lock
  */
  if (thd->tx_isolation > 1)
  {
    thd->tx_isolation = ISO_READ_COMMITTED;
    thd->variables.tx_isolation = ISO_READ_COMMITTED;
  }
  thd->thread_stack = (char*)&thd;
  mysql_mutex_lock(&LOCK_thread_count);
  thd->thread_id= thd->variables.pseudo_thread_id= thread_id++;
  threads.append(thd);
  mysql_mutex_unlock(&LOCK_thread_count);
  set_current_thd(thd);
  pthread_detach_this_thread();
  thd->init_for_queries();
  init_thr_lock();
  thd->store_globals();
  thd->system_thread= SYSTEM_THREAD_SLAVE_SQL;
  thd->security_ctx->skip_grants();
  thd->variables.max_allowed_packet= slave_max_allowed_packet;
  my_net_init(&thd->net, 0);
  thd->slave_thread= 1;
  thd->enable_slow_log= opt_log_slow_slave_statements;
  set_slave_thread_options(thd);
  thd->client_capabilities = CLIENT_LOCAL_FILES;
  thd_proc_info(thd, "Waiting for work from main SQL threads");
  thd->set_time();
  thd->variables.lock_wait_timeout= LONG_TIMEOUT;

  mysql_mutex_lock(&rpt->LOCK_rpl_thread);
  rpt->thd= thd;

  while (rpt->delay_start)
    mysql_cond_wait(&rpt->COND_rpl_thread, &rpt->LOCK_rpl_thread);

  rpt->running= true;
  mysql_cond_signal(&rpt->COND_rpl_thread);

  while (!rpt->stop )
  {
    old_msg= thd->proc_info;
    thd->enter_cond(&rpt->COND_rpl_thread, &rpt->LOCK_rpl_thread,
                    "Waiting for work from SQL thread");
    /*
      There are 4 cases that should cause us to wake up:
       - Events have been queued for us to handle.
       - We have an owner, but no events and not inside event group -> we need
         to release ourself to the thread pool
       - SQL thread is stopping, and we have an owner but no events, and we are
         inside an event group; no more events will be queued to us, so we need
         to abort the group (force_abort==1).
       - Thread pool shutdown (rpt->stop==1).     
    */
    while ( !((events= rpt->event_queue) || 
              (rpt->is_using && !in_event_group) ||
              (rpt->is_using && rpl_group_entry->force_abort) ||
              rpt->stop) )
      mysql_cond_wait(&rpt->COND_rpl_thread, &rpt->LOCK_rpl_thread);
    
    rpt->dequeue1(events);
    thd->exit_cond(old_msg);

  more_events:
    qevs_to_free= NULL;
    rgis_to_free= NULL;
    gcos_to_free= NULL;
    total_event_size= 0;
    while (events)
    {
      struct queue_event *next= events->next;
      Log_event_type event_type;
      rpl_group_info *rgi= events->rgi;
      bool end_of_group, group_ending;

      total_event_size+= events->event_size;
      if (!events->evt)
      {
        if (!in_event_group &&
            !active_mi->rli.abort_slave)
          handle_queued_pos_update(thd, events);
        events->next= qevs_to_free;
        qevs_to_free= events;
        events= next;
        continue;
      }

      err= 0;
      group_rgi= rgi;
      gco= rgi->gco;
      /* Handle a new event group, which will be initiated by a GTID event. */
      if ((event_type= events->evt->get_type_code()) == GCID_EVENT)
      {
        uint64 wait_count;
        in_event_group= true;

        rgi->thd= thd;
        
        mysql_mutex_lock(&rpl_group_entry->LOCK_rpl_parallel);
        if (!gco->installed)
        {
          if (gco->prev_gco)
              gco->prev_gco->next_gco= gco;
          gco->installed= true;
        }
        wait_count= gco->wait_count;
        if (wait_count > rpl_group_entry->count_committed_event_groups)
        {
           old_msg= thd->enter_cond(&gco->COND_group_commit_orderer,
                                   &rpl_group_entry->LOCK_rpl_parallel,
                                   "Waiting for prior transaction to start "
                                   "commit before starting next transaction");
          do
          {
            if (thd->killed && !rgi->is_error)
            {
              thd->send_kill_message();
              slave_output_error_info(rgi->rli, thd);
              signal_error_to_sql_driver_thread(thd, rgi);
              /*
                Even though we were killed, we need to continue waiting for the
                prior event groups to signal that we can continue. Otherwise we
                mess up the accounting for ordering. However, now that we have
                marked the error, events will just be skipped rather than
                executed, and things will progress quickly towards stop.
              */
            }
            mysql_cond_wait(&gco->COND_group_commit_orderer,
                            &rpl_group_entry->LOCK_rpl_parallel);
          } while (wait_count > rpl_group_entry->count_committed_event_groups);
        }

        if ((tmp_gco= gco->prev_gco))
        {
          /*
            Now all the event groups in the previous batch have entered their
            commit phase, and will no longer access their gco. So we can free
            it here.
          */
          DBUG_ASSERT(!tmp_gco->prev_gco);
          gco->prev_gco= NULL;
          tmp_gco->next_gco= gcos_to_free;
          gcos_to_free= tmp_gco;
        }

        if (rpl_group_entry->force_abort && wait_count > rpl_group_entry->stop_count)
        {
          /*
            We are stopping (STOP SLAVE), and this event group is beyond the
            point where we can safely stop. So set a flag that will cause us
            to skip, rather than execute, the following events.
          */
          group_skip_for_stop= true;
        }
        else
          group_skip_for_stop= false;

        if (unlikely(rpl_group_entry->stop_on_error_sub_id <= rgi->wait_commit_sub_id))
          group_skip_for_stop= true;
        else if (rgi->wait_commit_sub_id > rpl_group_entry->last_committed_sub_id)
        {
          /*
            Register that the commit of this event group must wait for the
            commit of the previous event group to complete before it may
            complete itself, so that we preserve commit order.
          */
          wait_for_commit *waitee=
            &rgi->wait_commit_group_info->commit_orderer;
          rgi->commit_orderer.register_wait_for_prior_commit(waitee);
        }
        mysql_mutex_unlock(&rpl_group_entry->LOCK_rpl_parallel);

        if(thd->wait_for_commit_ptr)
        {
         /*
            This indicates that we get a new GCID event in the middle of
            a not completed event group. This is corrupt binlog (the master
            will never write such binlog), so it does not happen unless
            someone tries to inject wrong crafted binlog, but let us still
            try to handle it somewhat nicely.
          */
          rgi->cleanup_context(thd, true);
          thd->wait_for_commit_ptr->unregister_wait_for_prior_commit();
          thd->wait_for_commit_ptr->wakeup_subsequent_commits(err);
        }
        thd->wait_for_commit_ptr= &rgi->commit_orderer;
      }//if (GCID_EVENT)

      group_ending= event_type == XID_EVENT ||
         (event_type == QUERY_EVENT &&
          (((Query_log_event *)events->evt)->is_commit() ||
           ((Query_log_event *)events->evt)->is_rollback()));
/*
  @raolh
  this will lead lock timeout error when disk is very busy,and transaction
  committing take too much time.

      if (group_ending)
      {
        rgi->mark_start_commit();
      }
*/
      /*
        If the SQL thread is stopping, we just skip execution of all the
        following event groups. We still do all the normal waiting and wakeup
        processing between the event groups as a simple way to ensure that
        everything is stopped and cleaned up correctly.
      */
      
     if (!rgi->is_error && !group_skip_for_stop)
        err= rpt_handle_event(events);
      else
        err= thd->wait_for_prior_commit();

      end_of_group= (in_event_group &&
                     ((events->evt->flags & LOG_EVENT_STANDALONE_F) ||
                      group_ending)); 

      delete_or_keep_event_post_apply(rgi, event_type, events->evt);
      events->next= qevs_to_free;
      qevs_to_free= events;
 
      if (err)
      {
        slave_output_error_info(rgi->rli, thd);
        signal_error_to_sql_driver_thread(thd, rgi);
      }
      if (end_of_group)
      {
        in_event_group= false;
        rpt->n_executed_trans++;
        /*
           @raolh
           when STOP SLAVE, rollback partial transaction
        */
        if (thd->variables.option_bits & OPTION_BEGIN)
          rgi->cleanup_context(thd, true);

        finish_event_group(thd, err, rgi, (rpl_parallel *)rpl_group_entry);
        rgi->next= rgis_to_free;
        rgis_to_free= rgi;
        group_rgi= rgi= NULL;
        group_skip_for_stop= false;

      }
      
      events= next;
    }//while

    mysql_mutex_lock(&rpt->LOCK_rpl_thread);
    rpt->dequeue2(total_event_size);
    mysql_cond_signal(&rpt->COND_rpl_thread);
    /* We need to delay the free here, to when we have the lock. */
    while (gcos_to_free)
    {
      group_commit_orderer *next= gcos_to_free->next_gco;
      rpt->free_gco(gcos_to_free);
      gcos_to_free= next;
    }
    while (rgis_to_free)
    {
      rpl_group_info *next= rgis_to_free->next;
      rpt->free_rgi(rgis_to_free);
      rgis_to_free= next;
    }
    while (qevs_to_free)
    {
      queue_event *next= qevs_to_free->next;
      rpt->free_qev(qevs_to_free);
      qevs_to_free= next;
    }

    if ((events= rpt->event_queue) != NULL)
    {
      /*
        Take next group of events from the replication pool.
        This is faster than having to wakeup the pool manager thread to give us
        a new event.
      */
      rpt->dequeue1(events);
      mysql_mutex_unlock(&rpt->LOCK_rpl_thread);
      goto more_events;
    }

    if (in_event_group && rpl_group_entry->force_abort)
    {
      /*
        We are asked to abort, without getting the remaining events in the
        current event group.

        We have to rollback the current transaction and update the last
        sub_id value so that SQL thread will know we are done with the
        half-processed event group.
      */
      mysql_mutex_unlock(&rpt->LOCK_rpl_thread);
      thd->wait_for_prior_commit();
      
      finish_event_group(thd, 1, group_rgi, (rpl_parallel *)rpl_group_entry);
      signal_error_to_sql_driver_thread(thd, group_rgi);
      
      in_event_group= false;
      mysql_mutex_lock(&rpt->LOCK_rpl_thread);
      rpt->free_rgi(group_rgi);
      group_rgi= NULL;
      group_skip_for_stop= false;
    }
    if (!in_event_group)
    {
      rpt->is_using= false;
      /* Tell wait_for_done() that we are done, if it is waiting. */
      if(unlikely(rpl_group_entry->force_abort))
          mysql_cond_broadcast(&rpl_group_entry->COND_rpl_parallel);
      if (!rpt->stop)
          rpt->pool->release_thread(rpt);
    }
  }//while (!rpt->stop && !thd->killed)

  rpt->thd= NULL;
  mysql_mutex_unlock(&rpt->LOCK_rpl_thread);

  thd->clear_error();
  thd->catalog= 0;
  thd->reset_query();
  thd->reset_db(NULL, 0);
  thd_proc_info(thd, "Slave worker thread exiting");
  thd->temporary_tables= 0;
  mysql_mutex_lock(&LOCK_thread_count);
  THD_CHECK_SENTRY(thd);
  delete thd;
  mysql_mutex_unlock(&LOCK_thread_count);

  mysql_mutex_lock(&rpt->LOCK_rpl_thread);
  rpt->running= false;
  mysql_cond_signal(&rpt->COND_rpl_thread);
  mysql_mutex_unlock(&rpt->LOCK_rpl_thread);

  my_thread_end();

  return NULL;
}


/*
  do_event() is executed by the sql_driver_thd thread.
  It's main purpose is to find a thread that can execute the query.

  @retval false     ok, event was accepted
  @retval true          error
*/
bool  
rpl_row_parallel::do_event(rpl_group_info *serial_rgi, Log_event *ev, size_t event_size)
{
  Relay_log_info *rli;
  int error;
  queue_event *qev;
  rpl_group_info *rgi;
  enum Log_event_type typ;
  THD *thd=serial_rgi->thd;
  rli= serial_rgi->rli;
  bool ev_is_begin, ev_is_commit, ev_is_rollback, is_group_ev ;
  
  mysql_mutex_unlock(&rli->data_lock);
 
  typ= ev->get_type_code();
  ev_is_begin= (typ== QUERY_EVENT) && ((Query_log_event *)ev)->is_begin();
  ev_is_commit= ((typ== QUERY_EVENT)&&((Query_log_event *)ev)->is_commit())||
                (typ== XID_EVENT);
  ev_is_rollback= (typ== QUERY_EVENT) && ((Query_log_event *)ev)->is_rollback();
  is_group_ev= Log_event::is_group_event(typ); 
 
  if ((ev_is_begin || !is_group_ev) && rli->abort_slave)
    sql_thread_stopping= true;
  /*
   @raolh
   sql_thread_stopping is only set by main SQL thread.
  */
  if (sql_thread_stopping)
  {
    if (current_trans)
      delete current_trans;
    return false;
  }
   
  if (!(qev= (queue_event *)my_malloc(sizeof(*qev), MYF(0))))
  {
    my_error(ER_OUT_OF_RESOURCES, MYF(0));
    return true;
  }
  qev->evt= ev;
  qev->next= NULL;
  qev->event_size= event_size;
  qev->event_relay_log_pos= rli->event_relay_log_pos;
  qev->future_event_relay_log_pos= rli->future_event_relay_log_pos;
  strcpy(qev->event_relay_log_name, rli->event_relay_log_name);
  strcpy(qev->future_event_master_log_name, rli->future_event_master_log_name);
  qev->future_event_master_log_pos= ev->log_pos; 
  qev->is_relay_log_event= 0;

  if (ev_is_begin)
  {
    if (current_trans)
    {
      /*
        This indicates that we get a new begin event in the middle of
        a not completed event group. This is corrupt binlog (the master
        will never write such binlog), so it does not happen unless
        someone tries to inject wrong crafted binlog, but let us still
        try to handle it somewhat nicely.
      */
       delete current_trans;
       current_trans= NULL;
    }

    rgi= new rpl_group_info(rli);
    rgi->thd= thd;
    rgi->sub_id= new_sub_id();
    rgi->wait_commit_sub_id= current_sub_id;
    rgi->wait_commit_group_info= current_group_info;
    current_trans= new trans_t(rgi);
    if ((rgi->deferred_events_collecting= rpl_filter->is_on()))
      rgi->deferred_events= new Deferred_log_events(rli);
  }
   
  if (NULL == current_trans || !is_group_ev)
  {
    /*
      @raolh
      DDL or non-group log event.
    */
    ev->thd= serial_rgi->thd;
    /*
      @raolh
      Wait for all workers complete all transaction.
      But we should skip gcid event for it is only used
      in group commit parallel replication.
    */
    if (typ != GCID_EVENT)
    {
      mysql_mutex_lock(&LOCK_rpl_parallel);
      while (current_sub_id > last_committed_sub_id)
        mysql_cond_wait(&COND_rpl_parallel, &LOCK_rpl_parallel);
      mysql_mutex_unlock(&LOCK_rpl_parallel);
    }

    if (true == rli->abort_slave)
    {//stop SQL threads
      delete ev;
      my_free(qev);
      return false;
    }

    if (typ == ROTATE_EVENT)
    {
      Rotate_log_event *rev= static_cast<Rotate_log_event *>(qev->evt);
      if ((rev->server_id != ::server_id ||
           rli->replicate_same_server_id) &&
          !rev->is_relay_log_event() &&
          !rli->is_in_group())
      {
        memcpy(rli->future_event_master_log_name,
               rev->new_log_ident, rev->ident_len+1);
      }
      else
        qev->is_relay_log_event= 1; 
    }

    qev->rgi= serial_rgi;    
    bool tmp= serial_rgi->is_parallel_exec;
    serial_rgi->is_parallel_exec= false;
    error= rpt_handle_event(qev);
    serial_rgi->is_parallel_exec= tmp;
    //Does it need to retry when execute fail?
    if (error)
    {
      rli->abort_slave= true;
      delete ev;
      my_free(qev);
      slave_output_error_info(rli, thd);
      return true;
    }
    delete_or_keep_event_post_apply(serial_rgi, typ, ev);
   
    my_free(qev);
    return false;
  }//if (NULL == current_trans && !is_group_ev)

  qev->rgi= current_trans->rgi;
  current_trans->add_event(qev);
  
  if (ev_is_commit || ev_is_rollback)
  {
    if (current_trans->contain_stmt)
    {
      current_trans->rgi->is_parallel_exec= true;
      /*
        @raolh
        we should wait all workers complete their transactions
        before apply this transaction in this thread.
      */
      mysql_mutex_lock(&LOCK_rpl_parallel);
      while (current_sub_id > last_committed_sub_id)
          mysql_cond_wait(&COND_rpl_parallel, &LOCK_rpl_parallel);
      mysql_mutex_unlock(&LOCK_rpl_parallel);
      if( rli->abort_slave )
      {
        delete current_trans;
        return false;
      }
  
      error=current_trans->apply();
      delete current_trans;
      if (error)
      {
        rli->abort_slave= true;
        slave_output_error_info(rli, thd);
        return true;
      }
    }
    else
    {
       current_sub_id= current_trans->rgi->sub_id;
       current_group_info= current_trans->rgi;
       current_trans->rgi->is_parallel_exec= false;
       rpt_cur= pool->dispatch_thread(current_trans);
    }
    current_trans= NULL;
  }
  rli->event_relay_log_pos= rli->future_event_relay_log_pos;
  return false;
}

void rpl_row_parallel::wait_for_done()
{
  /*
    @raolh
    First signal all workers that they must force quit; 
  */
  force_abort= true;
  
  /*
    @raolh
    release partial group events
  */
  if (NULL != current_trans)
  {
    delete current_trans;
  }
  
  /*
    @raolh
    no more events will be queued to complete any partial 
    event groups executed.
  */
  mysql_mutex_lock(&LOCK_rpl_parallel);
  while (current_sub_id > last_committed_sub_id)
      mysql_cond_wait(&COND_rpl_parallel, &LOCK_rpl_parallel);
  mysql_mutex_unlock(&LOCK_rpl_parallel);
  
}

queue_event *
rpl_thread::get_qev(Log_event *ev, ulonglong event_size,
                    Relay_log_info *rli)
{
  queue_event *qev;
  mysql_mutex_assert_owner(&LOCK_rpl_thread);
  if ((qev= qev_free_list))
    qev_free_list= qev->next;
  else if(!(qev= (queue_event *)my_malloc(sizeof(*qev), MYF(0))))
  {
    my_error(ER_OUTOFMEMORY, MYF(0), (int)sizeof(*qev));
    return NULL;
  }
  qev->evt= ev;
  qev->event_size= event_size;
  qev->next= NULL;
  strcpy(qev->event_relay_log_name, rli->event_relay_log_name);
  qev->event_relay_log_pos= rli->event_relay_log_pos;
  qev->future_event_relay_log_pos= rli->future_event_relay_log_pos;
  strcpy(qev->future_event_master_log_name, rli->future_event_master_log_name);
  qev->future_event_master_log_pos= ev->log_pos; 
  qev->is_relay_log_event= 0;

  return qev;
}

void
rpl_thread::free_qev(queue_event *qev)
{
  mysql_mutex_assert_owner(&LOCK_rpl_thread);
  qev->next= qev_free_list;
  qev_free_list= qev;
}


rpl_group_info*
rpl_thread::get_rgi(Relay_log_info *rli)
{
  rpl_group_info *rgi;
  mysql_mutex_assert_owner(&LOCK_rpl_thread);
  if ((rgi= rgi_free_list))
  {
    rgi_free_list= rgi->next;
    rgi->reinit(rli);
  }
  else
  {
    if(!(rgi= new rpl_group_info(rli)))
    {
      my_error(ER_OUTOFMEMORY, MYF(0), (int)sizeof(*rgi));
      return NULL;
    }
    rgi->is_parallel_exec = true;
  }
  rgi->sub_id= new_sub_id();
  if ((rgi->deferred_events_collecting= rpl_filter->is_on()) &&
      !rgi->deferred_events)
  {
    rgi->deferred_events= new Deferred_log_events(rli);
  }
  return rgi;
}

void
rpl_thread::free_rgi(rpl_group_info *rgi)
{
  mysql_mutex_assert_owner(&LOCK_rpl_thread);
  DBUG_ASSERT(rgi->commit_orderer.waitee == NULL);
  rgi->next= rgi_free_list;
  rgi_free_list= rgi;
}

group_commit_orderer *
rpl_thread::get_gco(uint64 wait_count, group_commit_orderer *prev)
{
  group_commit_orderer *gco;
  mysql_mutex_assert_owner(&LOCK_rpl_thread);
  if ((gco= gco_free_list))
    gco_free_list= gco->next_gco;
  else if(!(gco= (group_commit_orderer *)my_malloc(sizeof(*gco), MYF(0))))
  {
    my_error(ER_OUTOFMEMORY, MYF(0), (int)sizeof(*gco));
    return NULL;
  }
  mysql_cond_init(key_COND_group_commit_orderer,
                  &gco->COND_group_commit_orderer, NULL);
  gco->wait_count= wait_count;
  gco->prev_gco= prev;
  gco->next_gco= NULL;
  gco->installed= false;
  return gco;
}

void
rpl_thread::free_gco(group_commit_orderer *gco)
{
  mysql_mutex_assert_owner(&LOCK_rpl_thread);
  DBUG_ASSERT(!gco->prev_gco /* Must not free until wait has completed. */);
  gco->next_gco= gco_free_list;
  gco_free_list= gco;
}

void abandon_worker_thread(rpl_thread *cur_thread)
{
  mysql_mutex_unlock(&cur_thread->LOCK_rpl_thread);
  mysql_cond_signal(&cur_thread->COND_rpl_thread);
}


/*
  do_event() is executed by the sql_driver_thd thread.
  It's main purpose is to find a thread that can execute the query.

  @retval false     ok, event was accepted
  @retval true          error
*/
bool rpl_group_parallel::do_event(rpl_group_info *serial_rgi, Log_event *ev, size_t event_size)
{
  rpl_group_info *rgi= NULL;
  Relay_log_info *rli= serial_rgi->rli;
  enum Log_event_type typ;
  queue_event *qev;
  int err;
  bool is_group_event;
  
  
  if (unlikely(force_abort))
      force_abort= false;
  
  mysql_mutex_unlock(&rli->data_lock);
  
  /*
    Stop queueing additional event groups once the SQL thread is requested to
    stop.
  */
  if (((typ= ev->get_type_code()) == GCID_EVENT ||
       !(is_group_event= Log_event::is_group_event(typ)))&&rli->abort_slave)
    sql_thread_stopping= true;

  if (sql_thread_stopping)
  {
    delete ev;
    /*
      Return false ("no error"); normal stop is not an error, and otherwise the
      error has already been recorded.
    */
    return false;
  }

  cur_thread= choose_thread(rli, (GCID_EVENT != typ) || (flag & FL_IN_GROUP));
  if (!cur_thread)
  {
    /* This means we were killed. The error is already signalled. */
    delete ev;
    return true;
  }
  
  if (!(qev= cur_thread->get_qev(ev, event_size, rli)))
  {
    abandon_worker_thread(cur_thread);
    delete ev;
    return true;
  }
/*
  @raolh
  if there are two gcid event like
  gcid
  gcid
  ...
  xid
  we should skip the second one.(this would happen when IO thread crash unsafely)
*/
  if ((GCID_EVENT == typ)&&!(flag & FL_IN_GROUP))
  {
    flag |= FL_IN_GROUP;

    if (!(rgi= cur_thread->get_rgi(rli)))
    {
      cur_thread->free_qev(qev);
      abandon_worker_thread(cur_thread);
      delete ev;
      return true;
    }

    rgi->wait_commit_sub_id= current_sub_id;
    rgi->wait_commit_group_info= current_group_info;
    
    Gcid_log_event *gcid_ev= static_cast<Gcid_log_event *>(ev);
    
    if ( !((last_group_commit_id == gcid_ev->group_commit_id)
           && (0 != gcid_ev->group_commit_id)) )
    {
      /*
        A new batch of transactions that group-committed together on the master.

        Remember the count that marks the end of the previous group committed
        batch, and allocate a new gco.
      */
      uint64 count= count_queued_event_groups;
      group_commit_orderer *gco;

      if (!(gco= cur_thread->get_gco(count, current_gco)))
      {
        cur_thread->free_rgi(rgi);
        cur_thread->free_qev(qev);
        abandon_worker_thread(cur_thread);
        delete ev;
        return true;
      }
      current_gco= rgi->gco= gco;
    }
    else//group commit id changed
    {
      rgi->gco= current_gco;
    }
    
    qev->rgi= current_group_info= rgi;
    current_sub_id= rgi->sub_id;
    last_group_commit_id= gcid_ev->group_commit_id;
    ++count_queued_event_groups;
  }
  else if(!is_group_event || !(flag & FL_IN_GROUP))
  {// non_group event applied immediatly
  
    qev->rgi= serial_rgi;
    qev->is_relay_log_event= ((qev->evt->server_id == ::server_id) &&
                              !rli->replicate_same_server_id);
    
    /* Handle master log name change, seen in Rotate_log_event. */
    if (typ == ROTATE_EVENT)
    {
      Rotate_log_event *rev= static_cast<Rotate_log_event *>(qev->evt);
      if ((rev->server_id != ::server_id ||
           rli->replicate_same_server_id) &&
          !rev->is_relay_log_event() &&
          !rli->is_in_group())
      {
        memcpy(rli->future_event_master_log_name,
               rev->new_log_ident, rev->ident_len+1);
      }
    }
    
    bool tmp= serial_rgi->is_parallel_exec;
    serial_rgi->is_parallel_exec= true;
    err= rpt_handle_event(qev);
    serial_rgi->is_parallel_exec= tmp;
    delete_or_keep_event_post_apply(serial_rgi, typ, qev->evt);
    
    if (err)
    {
      cur_thread->free_qev(qev);
      abandon_worker_thread(cur_thread);
      return true;
    }
    qev->evt= NULL;
    
  }
  else
  {
    assert(current_group_info);
    qev->rgi= current_group_info;
  }

  bool end_of_group= (flag & FL_IN_GROUP) &&
                      (ev->flags & LOG_EVENT_STANDALONE_F ||
                      (typ == XID_EVENT ||
                      (typ == QUERY_EVENT &&
                       (((Query_log_event *)ev)->is_commit() ||
                       ((Query_log_event *)ev)->is_rollback()))));
 
  /*
    Queue the event for processing.
  */
  rli->event_relay_log_pos= rli->future_event_relay_log_pos;
  cur_thread->enqueue(qev);
  mysql_mutex_unlock(&cur_thread->LOCK_rpl_thread);
  mysql_cond_signal(&cur_thread->COND_rpl_thread);

 if (end_of_group)
  {
    flag &= ~FL_IN_GROUP;
  }

  return false;
}


void rpl_group_parallel::wait_for_done()
{

  mysql_mutex_lock(&LOCK_rpl_parallel);
  force_abort= true;
  stop_count= count_committed_event_groups;
  
  if(cur_thread)
    mysql_cond_signal(&cur_thread->COND_rpl_thread);

  while (current_sub_id > last_committed_sub_id)
    mysql_cond_wait(&COND_rpl_parallel, &LOCK_rpl_parallel);
  mysql_mutex_unlock(&LOCK_rpl_parallel);
}

bool
rpl_group_parallel::workers_idle()
{
    bool active;
    mysql_mutex_lock(&LOCK_rpl_parallel);
    active= current_sub_id > last_committed_sub_id;
    mysql_mutex_unlock(&LOCK_rpl_parallel);
    return !active;
}
