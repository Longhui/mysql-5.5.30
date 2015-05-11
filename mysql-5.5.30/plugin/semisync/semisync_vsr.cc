#include <m_string.h>
#include <stdio.h>
#include "semisync_vsr.h"
#include "my_sys.h"
#include "mysql.h"
#include "sql_common.h"
#include "mysql_com.h"

#ifndef MYSQL_SERVER
#define MYSQL_SERVER
#endif
int binlog_rollback_append(IO_CACHE *log);
void sql_print_error(const char *format, ...) ATTRIBUTE_FORMAT(printf, 1, 2);
void sql_print_warning(const char *format, ...) ATTRIBUTE_FORMAT(printf, 1, 2);
void sql_print_information(const char *format, ...) ATTRIBUTE_FORMAT(printf, 1, 2);

static int get_slave_sync_info(char *host, uint port, char *user, char* passwd, 
                               char** fname __attribute__((unused)), 
                               ulonglong* pos __attribute__((unused)))
{
  MYSQL *mysql;
  const char *buf;
  int error= 0;
  int reconnect= 0;
  ulong len= 0;
  uint net_timeout= 3600;
  if (!(mysql = mysql_init(NULL)))
  {
    return 1;
  }
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, (char *) &net_timeout);
  mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, (char *) &net_timeout);
  mysql_options(mysql, MYSQL_SET_CHARSET_NAME, default_charset_info->csname);
  while( 0 == mysql_real_connect(mysql, host, user,  
           passwd, 0, port, 0, CLIENT_REMEMBER_OPTIONS))
  {
    reconnect++;
    mysql->reconnect= 1;
    my_sleep(reconnect * 100000);
    if (reconnect >= 10)
    {
      sql_print_error("VSR HA : can't connect ha_parter (errno:%d)", (int)mysql_errno(mysql));
      error =1;
      goto err;
    }
  }
  if (simple_command(mysql, COM_VSR_QUERY, 0, 0, 1))
  {
    sql_print_information("VSR HA : vsr query_ha parter fail!");
    error= 1;
    goto err;
  }
  len = cli_safe_read(mysql);
  if ( len==packet_error || (long)len < 1)
  {
    sql_print_error("VSR HA : net error ");
    error= 1;
    goto err;
  }
  buf= (const char*)mysql->net.read_pos;
  *pos= uint8korr(buf);
  *fname= my_strdup(buf+8, MYF(MY_WME));
err:
  if(mysql)
  {
    mysql_close(mysql);
  }
  return error;
}

static int truncate_file(const char *fname, my_off_t len)
{
  File fd, fd_new;
  int error= 0;
  char new_fname[512]={0};
  uchar buff[512]={0};
  int Count= 0;
  //const char* fname_bak= new_fname;
  ulonglong f_len;

  if (fname == 0 || len == 0)
  {
    return 0;
  }
  fd=my_open(fname, O_RDWR | O_BINARY | O_SHARE, MYF(MY_WME));
  if (fd < 0)
  {
    return -1;
  }
  f_len= my_seek(fd, 0, SEEK_END, MYF(MY_THREADSAFE));
  my_seek(fd, 0 , SEEK_SET, MYF(MY_WME));
  if (f_len == MY_FILEPOS_ERROR)
  {
    error= 1;
    goto end;
  }

  if (f_len <= len)
  {
    sql_print_information("VSR HA : no need to truncate %s for it's length is %lld less than %lld",
      fname, f_len, len);
    error= 0;
    goto end;
  }

  my_seek(fd, 0, SEEK_SET, MYF(MY_WME));
  sprintf(new_fname, "%s_bak", fname);
  fd_new= my_open(new_fname, O_CREAT | O_RDWR | O_BINARY | O_SHARE, MYF(MY_WME));
  if (fd_new < 0)
  {
    error= 1;
    sql_print_error("VSR HA : fail to save %s truncation port, errno: %d", fname, errno);
    goto end;
  }
  
  while ((Count=my_read(fd, buff, sizeof(buff), MYF(MY_WME))) != 0)
  {
    if (Count == (uint) -1 ||
         my_write(fd_new,buff,Count,MYF(MY_WME | MY_NABP)))
    {
      my_close(fd_new, MYF(MY_WME));
      error= 1;
      sql_print_error("VSR HA : fail to save %s truncation port, errno: %d", fname, errno);
      goto end;
    }
  }
  my_sync(fd_new, MYF(MY_WME));  
  my_close(fd_new, MYF(MY_WME));
  sql_print_information("VSR HA : save %s to %s", fname, new_fname);   
  
  my_seek(fd, 0, SEEK_SET, MYF(MY_WME));
  if (my_chsize(fd, len, 0, MYF(MY_WME)))
  {
    error= 1;
    goto end;
  }
  sql_print_information("VSR HA : truncate binlog %s length from %lld to %lld", fname, f_len, len);

end:
  my_close(fd, MYF(MY_WME));
  return error;
}

static int append_rollback_event(const char *fname)
{
  IO_CACHE log;
  File fd;
  my_off_t end;
  fd= my_open(fname, O_RDWR | O_BINARY | O_SHARE, MYF(MY_WME));
  if (fd < 0 )
  {
    return -1;
  }
  end= my_seek(fd, 0L, MY_SEEK_END, MYF(0));
  if (init_io_cache(&log, fd, IO_SIZE*2, WRITE_CACHE, end, 0,
                    MYF(MY_WME|MY_DONT_CHECK_FILESIZE)))
  {
    return -1;
  }
  if (binlog_rollback_append(&log))
  {
    return -1;
  }
  end_io_cache(&log);
  my_close(fd, MYF(MY_WME));
  return 0;
}

int adjust_binlog_with_slave(char *host, uint port, char *user, char* passwd, char* last_binlog)
{
  my_off_t len;
  char* binlog= NULL;
  int error = 0; 
  error = get_slave_sync_info(host, port, user ,passwd, &binlog, &len);
  if (0 != error)
  {
    sql_print_error("VSR HA : fail to connect slave");
    goto over;
  }
  sql_print_information("VSR HA : slave receive master binlog %s %lld", binlog, len);  
  if (0 != strncmp(last_binlog, binlog, strlen(last_binlog)))
  {
    sql_print_information("VSR HA : Don't truncate master's last binlog %s"
                          "for slave receive binlog %s", last_binlog, binlog);
    goto over;
  }

  error =truncate_file(binlog, len);
  if (0 != error)
  {
    goto over;
  }
  if (-1 == append_rollback_event(binlog))
  {
    goto over;
  }
  sql_print_information("VSR HA : append rollback to %s", binlog);
over:
  if (NULL != binlog)
    my_free(binlog);

  return error>0 ? 1 : 0;
}

void send_slave_sync_info(NET *net, char* fname, my_off_t pos)
{
   int len= 0;
   uchar buf[100];
   bool res;
   int8store(buf, pos);
   len = strlen(fname);
   memcpy(buf + 8, fname, len + 1);
   /* Send the reply. */
   res = my_net_write(net, (const uchar*)buf, len + 8);
   if (!res)
   {
     res = net_flush(net);
     if (res)
      sql_print_warning("VSR HA : slave net_flush() reply failed");
   }
   else
   {
     sql_print_warning("VSR HA : slave send reply failed: %s (%d)",
                     net->last_error, net->last_errno);
   }
   sql_print_information("VSR HA : send replication information (%s, %lld)", fname, pos);
}
