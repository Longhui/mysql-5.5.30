/**
* MySQL 5.4中的InnoDB Mutex实现
*
* @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
*/

#ifndef _INNO_MUTEX_H_
#define _INNO_MUTEX_H_

#include <assert.h>

#include "util/Sync.h"

using namespace ntse;

typedef unsigned long long u64;
typedef u64 ib_longlong;
typedef unsigned long ulint;
typedef long int lint;

typedef struct mutex_struct		mutex_t;

#ifdef WIN32
typedef void*			os_thread_t;
typedef ulint			os_thread_id_t;	/* In Windows the thread id
										is an unsigned long int */
#else
typedef pthread_t os_thread_t;
typedef os_thread_t os_thread_id_t;
#endif

extern ulint	srv_spin_wait_delay;

#define ULINT_UNDEFINED -1  
#define	SYNC_SPIN_ROUNDS 30

/*Creates, or rather, initializes a mutex object to a specified memory
location (which must be appropriately aligned). The mutex is initialized
in the reset state. Explicit freeing of the mutex with mutex_free is
necessary only if the memory block containing it is freed. */

# define mutex_create(M)					\
	mutex_create_func((M), __FILE__, __LINE__)
/******************************************************************
NOTE! The following macro should be used in mutex locking, not the
corresponding function. */

#define mutex_enter(M)	  mutex_enter_func((M), __FILE__, __LINE__)

#define mutex_enter_nowait(M)	\
	mutex_enter_nowait_func((M), __FILE__, __LINE__)






/**mutex struct.*/
struct mutex_struct {	
	Event	*event; /* Used by sync0arr.c for the wait queue */
	Atomic<int>	lock_word;	/* This byte is the target of the atomic
						test-and-set instruction in Win32 and
						x86 32/64 with GCC 4.1.0 or later version */
	ulint	waiters;	/* This ulint is set to 1 if there are (or
						may be) threads waiting in the global wait
						array for this mutex to be released.
						Otherwise, this is 0. */
	os_thread_id_t thread_id; /* The thread id of the thread*/
	ulint		magic_n;
	# define MUTEX_MAGIC_N	(ulint)979585
	const char*	cfile_name;/* File name where mutex created */
	ulint		cline;	/* Line where created */
};

#ifdef __cplusplus 
extern "C" { 
#endif 
/*function declarations*/

extern void mutex_create_func( mutex_t*	mutex,  const char*	cfile_name, ulint cline);
extern void mutex_free( mutex_t*	mutex);
extern ulint mutex_enter_nowait_func(mutex_t*	mutex,const char*	file_name, ulint line);
extern bool mutex_validate(const mutex_t*	mutex);
extern bool mutex_own(const mutex_t*	mutex);
extern void mutex_set_waiters(mutex_t*	mutex,ulint	n);
extern void mutex_spin_wait(mutex_t*	mutex, const char*	file_name, ulint line);
extern void mutex_signal_object(mutex_t*	mutex);

extern int mutex_test_and_set(mutex_t *mutex);
extern void mutex_reset_lock_word(mutex_t *mutex);
extern int mutex_get_lock_word(const mutex_t *mutex);
extern ulint mutex_get_waiters(const mutex_t *mutex);
extern void mutex_exit(mutex_t *mutex);
extern void mutex_enter_func( mutex_t*	mutex, const char*	file_name,ulint		line);


#ifdef __cplusplus 
	} 
#endif

#endif
