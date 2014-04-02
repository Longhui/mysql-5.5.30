/* Copyright (c) 2006 MySQL AB, 2009 Sun Microsystems, Inc.
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


#ifndef SEMISYNC_SLAVE_H
#define SEMISYNC_SLAVE_H

#include "semisync.h"

#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key key_ss_mutex_LOCK_relaylog_;
extern PSI_cond_key key_ss_cond_COND_relaylog_queue_;
#endif

int offsetCompare(const char *log_file_name1, my_off_t log_file_pos1,
			 const char *log_file_name2, my_off_t log_file_pos2);

/**
   The extension class for the slave of semi-synchronous replication
*/
class ReplSemiSyncSlave
  :public ReplSemiSyncBase {
public:
  ReplSemiSyncSlave();
  ~ReplSemiSyncSlave();

  void setTraceLevel(unsigned long trace_level) {
    trace_level_ = trace_level;
  }

  /* Initialize this class after MySQL parameters are initialized. this
   * function should be called once at bootstrap time.
   */
  int initObject();

  bool getSlaveEnabled() {
    return slave_enabled_;
  }
  void setSlaveEnabled(bool enabled) {
    slave_enabled_ = enabled;
  }

  /* A slave reads the semi-sync packet header and separate the metadata
   * from the payload data.
   * 
   * Input:
   *  header      - (IN)  packet header pointer
   *  total_len   - (IN)  total packet length: metadata + payload
   *  need_reply  - (IN)  whether the master is waiting for the reply
   *  payload     - (IN)  payload: the replication event
   *  payload_len - (IN)  payload length
   *
   * Return:
   *  0: success;  non-zero: error
   */
  int slaveReadSyncHeader(const char *header, unsigned long total_len, bool *need_reply,
                          const char **payload, unsigned long *payload_len);

  /* A slave replies to the master indicating its replication process.  It
   * indicates that the slave has received all events before the specified
   * binlog position.
   * 
   * Input:
   *  mysql            - (IN)  the mysql network connection
   *  binlog_filename  - (IN)  the reply point's binlog file name
   *  binlog_filepos   - (IN)  the reply point's binlog file offset
   *
   * Return:
   *  0: success;  non-zero: error
   */
  int slaveReply(MYSQL *mysql, const char *binlog_filename,
                 my_off_t binlog_filepos);

  int slaveStart(Binlog_relay_IO_param *param);
  int slaveStop(Binlog_relay_IO_param *param);
  int reportApplyEvent(const char *relaylog_file,
			my_off_t relaylog_pos);
  int waitApply(const char* QEvent_file_name,
		my_off_t QEvent_file_pos);
  inline bool is_on() { return (status_); }
  inline void switch_on() { status_= 1; }
  inline void switch_off() { status_= 0; }
  inline bool getFullEnable() { return (full_enabled_); }
  void setFullsync(bool val);
  void setWaitTimeout(unsigned long);
  void setExportVals();
private:
  /* True when initObject has been called */
  bool init_done_;
  bool slave_enabled_;        /* semi-sycn is enabled on the slave */
  MYSQL *mysql_reply;         /* connection to send reply */
  /* This cond variable is signaled when enough relaylog has been replayed,
   * so that IO Thread can reply to Master.
   */
  mysql_cond_t  COND_relaylog_queue_;
  volatile bool full_enabled_;
  unsigned long wait_timeout_;
  /* whether full-sync replication is switched on */
  bool status_;
  /* Mutex that protects the following state variables */
  mysql_mutex_t LOCK_relaylog_;
  /* The binlog name up to which IO Thread have received from Master */
  char wait_file_name_[FN_REFLEN];
  /* The position in that file up to which IO Thread have the received from Master */ 
  my_off_t wait_file_pos_;
  /* The binlog name up to which SQL Thread have replayed */
  char relay_file_name_[FN_REFLEN];
  /* The position in that file up to which SQL Thread have replayed */
  my_off_t relay_file_pos_;
  /*IO thread is waiting for be wakeuped*/
  bool io_is_waiting;
};


/* System and status variables for the slave component */
extern char rpl_semi_sync_slave_fullsync;
extern char rpl_semi_sync_slave_enabled;
extern unsigned long rpl_semi_sync_slave_trace_level;
extern char rpl_semi_sync_slave_status;
extern unsigned long rpl_semi_sync_slave_timeout;
extern unsigned long long rpl_semi_sync_slave_sql_wait_num;
extern unsigned long long rpl_semi_sync_slave_sql_wait_time;
extern unsigned long long rpl_semi_sync_slave_sql_wait_timeouts;
extern unsigned long long rpl_semi_sync_slave_sql_wait_fail;
extern char rpl_semi_sync_slave_fullsync_status;
#endif /* SEMISYNC_SLAVE_H */
