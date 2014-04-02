/* Copyright (c) 2008 MySQL AB, 2009 Sun Microsystems, Inc.
   Use is subject to license terms.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */


#include "semisync_slave.h"

char rpl_semi_sync_slave_fullsync;
char rpl_semi_sync_slave_enabled;
char rpl_semi_sync_slave_status= 0;
char rpl_semi_sync_slave_fullsync_status= 0;
unsigned long rpl_semi_sync_slave_trace_level;
/*full sync replication transcation wait timeout period in milliseconds*/
unsigned long rpl_semi_sync_slave_timeout;
/*full sync replication transcation wait time in milliseconds*/
unsigned long long rpl_semi_sync_slave_sql_wait_time= 0;
/*full sync replication transcation wait times*/
unsigned long long rpl_semi_sync_slave_sql_wait_num= 0;
/*full sync replication wait timeout times*/
unsigned long long rpl_semi_sync_slave_sql_wait_timeouts= 0;
/*full sync replication wait fail times*/
unsigned long long rpl_semi_sync_slave_sql_wait_fail= 0;

ReplSemiSyncSlave::ReplSemiSyncSlave()
  :slave_enabled_(false),
   full_enabled_(false),
   status_(1L),
   wait_file_pos_(0L),
   relay_file_pos_(0L),
   io_is_waiting(false)
 {
   strcpy(wait_file_name_, "");
   strcpy(relay_file_name_, "");
 }

ReplSemiSyncSlave::~ReplSemiSyncSlave()
{
  if(init_done_)
  {
  	mysql_mutex_destroy(&LOCK_relaylog_);
	mysql_cond_destroy(&COND_relaylog_queue_);
  }
}

void ReplSemiSyncSlave::setFullsync(bool val)
{
  full_enabled_= val;
  status_ = val;
  rpl_semi_sync_slave_sql_wait_time= 0;
  rpl_semi_sync_slave_sql_wait_num= 0;
}

void ReplSemiSyncSlave::setExportVals()
{
 rpl_semi_sync_slave_fullsync_status= status_;
}

void ReplSemiSyncSlave::setWaitTimeout(unsigned long wait_timeout)
{
  wait_timeout_= wait_timeout;
}

int ReplSemiSyncSlave::initObject()
{
  int result= 0;
  const char *kWho = "ReplSemiSyncSlave::initObject";

  if (init_done_)
  {
    fprintf(stderr, "%s called twice\n", kWho);
    return 1;
  }
  init_done_ = true;

  mysql_mutex_init(key_ss_mutex_LOCK_relaylog_, &LOCK_relaylog_, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_ss_cond_COND_relaylog_queue_, &COND_relaylog_queue_, NULL);
  
  /* References to the parameter works after set_options(). */
  setSlaveEnabled(rpl_semi_sync_slave_enabled);
  setTraceLevel(rpl_semi_sync_slave_trace_level);
  setWaitTimeout(rpl_semi_sync_slave_timeout);
  setFullsync(rpl_semi_sync_slave_fullsync != 0);

  return result;
}

int ReplSemiSyncSlave::slaveReadSyncHeader(const char *header,
                                      unsigned long total_len,
                                      bool  *need_reply,
                                      const char **payload,
                                      unsigned long *payload_len)
{
  const char *kWho = "ReplSemiSyncSlave::slaveReadSyncHeader";
  int read_res = 0;
  function_enter(kWho);

  if ((unsigned char)(header[0]) == kPacketMagicNum)
  {
    *need_reply  = (header[1] & kPacketFlagSync);
    *payload_len = total_len - 2;
    *payload     = header + 2;

    if (trace_level_ & kTraceDetail)
      sql_print_information("%s: reply - %d", kWho, *need_reply);
  }
  else
  {
    sql_print_error("Missing magic number for semi-sync packet, packet "
                    "len: %lu", total_len);
    read_res = -1;
  }

  return function_exit(kWho, read_res);
}

int ReplSemiSyncSlave::slaveStart(Binlog_relay_IO_param *param)
{
  bool semi_sync= getSlaveEnabled();
  
  sql_print_information("Slave I/O thread: Start %s replication to\
 master '%s@%s:%d' in log '%s' at position %lu",
			semi_sync ? "semi-sync" : "asynchronous",
			param->user, param->host, param->port,
			param->master_log_name[0] ? param->master_log_name : "FIRST",
			(unsigned long)param->master_log_pos);

  if (semi_sync && !rpl_semi_sync_slave_status)
    rpl_semi_sync_slave_status= 1;
  return 0;
}

int ReplSemiSyncSlave::slaveStop(Binlog_relay_IO_param *param)
{
  if (rpl_semi_sync_slave_status)
    rpl_semi_sync_slave_status= 0;
  if (mysql_reply)
    mysql_close(mysql_reply);
  mysql_reply= 0;
  return 0;
}

int ReplSemiSyncSlave::slaveReply(MYSQL *mysql,
                                 const char *binlog_filename,
                                 my_off_t binlog_filepos)
{
  const char *kWho = "ReplSemiSyncSlave::slaveReply";
  NET *net= &mysql->net;
  uchar reply_buffer[REPLY_MAGIC_NUM_LEN
                     + REPLY_BINLOG_POS_LEN
                     + REPLY_BINLOG_NAME_LEN];
  int  reply_res, name_len = strlen(binlog_filename);

  function_enter(kWho);

  /* Prepare the buffer of the reply. */
  reply_buffer[REPLY_MAGIC_NUM_OFFSET] = kPacketMagicNum;
  int8store(reply_buffer + REPLY_BINLOG_POS_OFFSET, binlog_filepos);
  memcpy(reply_buffer + REPLY_BINLOG_NAME_OFFSET,
         binlog_filename,
         name_len + 1 /* including trailing '\0' */);

  if (trace_level_ & kTraceDetail)
    sql_print_information("%s: reply (%s, %lu)", kWho,
                          binlog_filename, (ulong)binlog_filepos);

  net_clear(net, 0);
  /* Send the reply. */
  reply_res = my_net_write(net, reply_buffer,
                           name_len + REPLY_BINLOG_NAME_OFFSET);
  if (!reply_res)
  {
    reply_res = net_flush(net);
    if (reply_res)
      sql_print_error("Semi-sync slave net_flush() reply failed");
  }
  else
  {
    sql_print_error("Semi-sync slave send reply failed: %s (%d)",
                    net->last_error, net->last_errno);
  }

  return function_exit(kWho, reply_res);
}

int offsetCompare(const char *log_file_name1, my_off_t log_file_pos1,
			 const char *log_file_name2, my_off_t log_file_pos2)
{
  int cmp = strcmp(log_file_name1, log_file_name2);

  if (cmp != 0)
    return cmp;

  if (log_file_pos1 > log_file_pos2)
    return 1;
  else if (log_file_pos1 < log_file_pos2)
    return -1;
  return 0;
}

int ReplSemiSyncSlave::reportApplyEvent(const char *relaylog_file, my_off_t relaylog_pos)
{
  const char *kwho= "ReplSemiSyncSlave::reportApplyEvent";
  function_enter(kwho);
  ulong trc_level = trace_level_;
  bool need_broadcast = false;
  if (trc_level & kTraceDetail)
    sql_print_information("Applying Event:(%s:%lld)", relaylog_file, relaylog_pos);	
  if(getFullEnable() && relaylog_file)
  { 
    mysql_mutex_lock(&LOCK_relaylog_);
    strcpy(relay_file_name_, relaylog_file);
    relay_file_pos_= relaylog_pos;
    if(io_is_waiting)
    {
      int cmp= offsetCompare(relay_file_name_, relay_file_pos_, wait_file_name_, wait_file_pos_);
      if (trc_level & kTraceDetail)
        sql_print_information("offsetCompare:%d", cmp);
      if(cmp >=0)
      {
        if(is_on())
        {
         need_broadcast = true;	
        }
        else
        {/*turn on fullsync*/
          switch_on();
        }
      }
    }
    mysql_mutex_unlock(&LOCK_relaylog_);	
    if(need_broadcast)
      mysql_cond_broadcast(&COND_relaylog_queue_);
  }
  return function_exit(kwho, 0);
}

int ReplSemiSyncSlave::waitApply(const char* QEvent_file_name, my_off_t QEvent_file_pos)
{
  const char *kwho= "ReplSemiSyncSlave::waitApply";
  function_enter(kwho);
  ulong trc_level = trace_level_;

  if(getFullEnable() && QEvent_file_name)
  {
    struct timespec start_ts;
    struct timespec abstime;
    int wait_result;
    const char* old_msg= 0;

    set_timespec(start_ts, 0);
    mysql_mutex_lock(&LOCK_relaylog_);
    strcpy(wait_file_name_, QEvent_file_name);
    wait_file_pos_= QEvent_file_pos; 
    if (trc_level & kTraceDetail)
	sql_print_information("WaitFor:(%s:%lld)? %s", QEvent_file_name, QEvent_file_pos, is_on()? "yes" : "no");

    old_msg= thd_enter_cond(NULL, &COND_relaylog_queue_, &LOCK_relaylog_, "Waiting for SQL thread apply Event");
    while(is_on())
    {
      int cmp = offsetCompare(relay_file_name_, relay_file_pos_, wait_file_name_, wait_file_pos_);
      if(cmp>= 0)
      {/*SQL thread already catch up!*/
        if (trc_level & kTraceDetail)
          sql_print_information("Apply Event:(%s:%lld)",relay_file_name_, relay_file_pos_);
        break;
      }
     /* Calcuate the waiting period. */
#ifdef __WIN__
      abstime.tv.i64 = start_ts.tv.i64 + (__int64)wait_timeout_ * TIME_THOUSAND * 10;
      abstime.max_timeout_msec= (long)wait_timeout_;
#else
      unsigned long long diff_nsecs = start_ts.tv_nsec + (unsigned long long)wait_timeout_ * TIME_MILLION;
      abstime.tv_sec = start_ts.tv_sec;
      while (diff_nsecs >= TIME_BILLION)
      {
        abstime.tv_sec++;
        diff_nsecs -= TIME_BILLION;
      }
      abstime.tv_nsec = diff_nsecs;
#endif /* __WIN__ */
      io_is_waiting = true;
      wait_result = mysql_cond_timedwait(&COND_relaylog_queue_, &LOCK_relaylog_, &abstime);
      io_is_waiting = false;
      if(0!= wait_result)
      {
        if (trc_level & kTraceDetail)
           sql_print_information("Wait_cond timeout!! wait_result:%d", wait_result);
        switch_off();
        rpl_semi_sync_slave_sql_wait_timeouts++;
      }
      else
      {
        int wait_time;
        wait_time= getWaitTime(start_ts);
        if(wait_time< 0)
        {
          if (trace_level_ & kTraceGeneral)
          {
            sql_print_error("Replication full-sync getWaitTime fail at "
                             "wait position (%s, %lu)",
                             QEvent_file_name, (unsigned long)QEvent_file_pos);
          }
          rpl_semi_sync_slave_sql_wait_fail++;
        }
        else
        {
          rpl_semi_sync_slave_sql_wait_num++;
          rpl_semi_sync_slave_sql_wait_time += wait_time;
        }
      }
    }
    thd_exit_cond(NULL, old_msg);
  }
  return function_exit(kwho, 0);
}

