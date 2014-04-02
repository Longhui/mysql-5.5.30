#include "resource_profiler.h"
#include "sql_class.h"

//#define STATISTICS_DEBUG
/* RETURN: the thread's user times unit 1ms*/
static ulonglong get_thread_time(THD *thd)
{
	DBUG_ASSERT(thd != NULL);
	ulonglong ret_time = 0; 
#ifdef __WIN__
	int64 CreateTime,ExitTime,KernelTime,UserTime;
	HANDLE handle = OpenThread(THREAD_ALL_ACCESS, FALSE, thd->real_id);
	if (handle != INVALID_HANDLE_VALUE &&
		GetThreadTimes(handle,(LPFILETIME)&CreateTime,(LPFILETIME)&ExitTime,(LPFILETIME)&KernelTime,(LPFILETIME)&UserTime))
	{
		ret_time = UserTime;
	}
#else
	struct timespec tp;
	clock_gettime(CLOCK_THREAD_CPUTIME_ID,&tp);
	ret_time = tp.tv_sec * 10000000 + tp.tv_nsec / 100;
#endif
	return ret_time / 10000;
}

void start_trx_statistics(THD *thd)
{
  if(opt_use_profile_limitted && thd->get_user_connect() != NULL)
  {
	thd->error = 0;
	thd->conn_io_reads_per_trx = 0;
	thd->conn_cpu_times_per_trx = 0;
	thd->trx_start_thread_times = get_thread_time(thd);
#ifdef STATISTICS_DEBUG
	printf("reset !! conn_io_reads_per_trx = 0, conn_cpu_times_per_trx=%llu\n",thd->conn_cpu_times_per_trx);
#endif
  }
}

static int end_trx_statistics(THD *thd)
{
  const USER_CONN *uc = thd->get_user_connect();
  if( uc != NULL && uc->user_resources.profile != NULL)
  {
	if(uc->user_resources.profile->cpu_times != 0 &&
	  uc->curr_resources->curr_cpu_times >= uc->user_resources.profile->cpu_times)
	{
	  my_error(ER_CPU_TIMES_LIMITED,MYF(0),uc->user, uc->host);
	  return thd->error = ER_CPU_TIMES_LIMITED;
	}
	else if(uc->user_resources.profile->io_reads != 0 &&
	  uc->curr_resources->curr_io_reads >= uc->user_resources.profile->io_reads)
	{
	  my_error(ER_IO_READS_LIMITED,MYF(0),uc->user, uc->host);
	  return thd->error = ER_IO_READS_LIMITED;
	}
	else if(uc->user_resources.profile->io_reads_per_trx != 0 &&
	  thd->conn_io_reads_per_trx >= uc->user_resources.profile->io_reads_per_trx)
	{
	  my_error(ER_TRX_IO_READS_LIMITED,MYF(0),uc->user, uc->host);
	  return thd->error = ER_TRX_IO_READS_LIMITED;
	}
	else if(uc->user_resources.profile->cpu_times_per_trx != 0 &&
	  thd->conn_cpu_times_per_trx >= uc->user_resources.profile->cpu_times_per_trx)
	{
	  my_error(ER_TRX_CPU_TIMES_LIMITED,MYF(0),uc->user, uc->host);
	  return thd->error = ER_TRX_CPU_TIMES_LIMITED;
	}
  }
  return 0;
}

int resource_statistics(int with_io_read)
{
	THD *thd = current_thd;
	if( !opt_use_profile_limitted || thd == NULL || thd->get_user_connect() == NULL) return 0;
	if(end_trx_statistics(thd) != 0)
	{
	  thd->awake(THD::KILL_QUERY);
	  return -1;
	}
	ulonglong curr_times = get_thread_time(thd);
	ulonglong time_span = curr_times - thd->trx_start_thread_times;
	thd->conn_cpu_times_per_trx += time_span;
	thd->increment_curr_cpu_times(time_span);
	thd->trx_start_thread_times = curr_times;
	if(with_io_read)
	{
	  thd->conn_io_reads_per_trx++;
	  thd->increment_curr_io_reads();
	}
	return 0;
}

void my_malloc_callback_func(size_t size)
{
  THD *thd = current_thd;
  if(thd != NULL)
  {
  	thd->mem_size += size;
	thd->max_mem_size = thd->max_mem_size < thd->mem_size ? thd->mem_size : thd->max_mem_size;
  }
}

void my_free_callback_func(size_t size)
{
  THD *thd = current_thd;
  if (thd != NULL)
  {
  	thd->mem_size -= size;
  }
}
