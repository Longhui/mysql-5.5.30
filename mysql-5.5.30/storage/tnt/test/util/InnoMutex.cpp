/**
* MySQL 5.4中的InnoDB Mutex实现
*
* @author 聂明军(niemingjun@corp.netease.com, niemingjun@163.org)
* (c) 2009 netease
*
*
* 测试性能对比（2009-05-14th）
*	-环境：Db-10 Machine。100个线程，95个mutex。每个thread循环500k次开\关锁对，
×		并在开关锁中间增加200个mod运算。
×		@see <code>SyncBigTest::testMutexMTTesting</code> 和 <code>SyncBigTest::testMutexInnoDbMTTesting</code
*	-结果：
*		.执行速度：InnoDB Mutex的速度和Throughput与NTSE实现的<code>ntse::Mutex</code>基本相等。
*		（测试显示比ntse略微好一点）
*		.均衡性：最长等候时间会比ntse mutex 小。(InnoDB 在3～5s间，ntse在8s左右)
*
*	-具体数据： 
*								InnoDB mutex	|	ntse mutex
*	测试总时间(ms):			168176			|	169428 
×	Throughput (ops/ms):		297				|	295 
*	Avg Mutex lock time(ms):		289			|	279 
*	Max Mutex lock time(us):		3015,784	|	7950,657
*/

#include <assert.h>
#include "util/InnoMutex.h"

using namespace ntse;

/* Number of spin waits on mutexes: for performance monitoring */

/* round=one iteration of a spin loop */
ib_longlong	mutex_spin_round_count		= 0;
ib_longlong	mutex_spin_wait_count		= 0;
ib_longlong mutex_os_wait_count         = 0;

extern bool os_compare_and_swap(volatile lint*  ptr, lint oldVal, lint newVal)	;
extern os_thread_id_t os_thread_get_curr_id(void);
extern bool os_thread_eq(os_thread_id_t	a, os_thread_id_t	b);
extern ulint ut_delay(ulint	delay);
extern ulint ut_rnd_interval(ulint	low, ulint	high);
extern void os_thread_yield(void);

/*Creates, or rather, initializes a mutex object in a specified memory
location (which must be appropriately aligned). The mutex is initialized
in the reset state. Explicit freeing of the mutex with mutex_free is
necessary only if the memory block containing it is freed. */
void mutex_create_func(
					   /*==============*/
					   mutex_t*	mutex,		/* in: pointer to memory */

					   const char*	cfile_name,	/* in: file name where created */
					   ulint		cline)		/* in: file line where created */
{
	assert(mutex);
	mutex->lock_word.set(0);
	mutex_reset_lock_word(mutex);	
	mutex->event = new Event(false);
	mutex_set_waiters(mutex, 0);

	mutex->magic_n = MUTEX_MAGIC_N;

	mutex->cfile_name = cfile_name;
	mutex->cline = cline;
}

/**********************************************************************
Calling this function is obligatory only if the memory buffer containing
the mutex is freed. Removes a mutex object from the mutex list. The mutex
is checked to be in the reset state. */

void
mutex_free(
		   /*=======*/
		   mutex_t*	mutex)	/* in: mutex */
{
	assert(mutex_validate(mutex));
	assert(0 == mutex_get_lock_word(mutex));
	assert(0 == mutex_get_waiters(mutex));

	delete mutex->event;
	mutex->event = NULL;
	/* reset the magic number */
	mutex->magic_n = 0;
	delete mutex;
}

/************************************************************************
NOTE! Use the corresponding macro in the header file, not this function
directly. Tries to lock the mutex for the current thread. If the lock is not
acquired immediately, returns with return value 1. */

ulint
mutex_enter_nowait_func(
						/*====================*/
						/* out: 0 if succeed, 1 if not */
						mutex_t*	mutex,		/* in: pointer to mutex */
						const char*	file_name,
						/* in: file name where mutex
						requested */
						ulint		line )
						/* in: line where requested */
{
	assert(mutex_validate(mutex));
	if (!mutex_test_and_set(mutex)) {
		assert(mutex->thread_id = os_thread_get_curr_id());
		return(0);	/* Succeeded! */
		}
	return(1);
}

/**********************************************************************
Checks that the mutex has been initialized. */

bool mutex_validate(
					/*===========*/
					const mutex_t*	mutex)
{
	assert(mutex);
	assert(mutex->magic_n == MUTEX_MAGIC_N);
	return(true);
}

/**********************************************************************
Checks that the current thread owns the mutex. Works only in the debug
version. */

bool
mutex_own(
		  /*======*/
		  /* out: TRUE if owns */
		  const mutex_t*	mutex)	/* in: mutex */
{
	assert(mutex_validate(mutex));

	return(1 == mutex_get_lock_word(mutex)
		&& os_thread_eq(mutex->thread_id, os_thread_get_curr_id()));
}

/**********************************************************************
Sets the waiters field in a mutex. */

void
mutex_set_waiters(
				  /*==============*/
				  mutex_t*	mutex,	/* in: mutex */
				  ulint		n)	/* in: value to set */
{
	volatile ulint*	ptr;		/* declared volatile to ensure that
								the value is stored to memory */
	assert(mutex);

	ptr = &(mutex->waiters);

	*ptr = n;		/* Here we assume that the write of a single
					word in memory is atomic */
}
/**********************************************************************
Reserves a mutex for the current thread. If the mutex is reserved, the
function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting
for the mutex before suspending the thread. */

void mutex_spin_wait(
					 /*============*/
					 mutex_t*	mutex,		/* in: pointer to mutex */
					 const char*	file_name,	/* in: file name where mutex
												requested */
												ulint		line)		/* in: line where requested */
{
	ulint	   i;	  /* spin round cou	nt */
	long sigToken; /*sigToken*/
	assert(mutex);
	assert(mutex->event);

	/* This update is not thread safe, but we don't mind if the count
	isn't exact. Moved out of ifdef that follows because we are willing
	to sacrifice the cost of counting this as the data is valuable.
	Count the number of calls to mutex_spin_wait. */
	mutex_spin_wait_count++;

mutex_loop:

	i = 0;

	/* Spin waiting for the lock word to become zero. Note that we do
	not have to assume that the read access to the lock word is atomic,
	as the actual locking is always committed with atomic test-and-set.
	In reality, however, all processors probably have an atomic read of
	a memory word. */

spin_loop:

	while (mutex_get_lock_word(mutex) != 0 && i < SYNC_SPIN_ROUNDS) {
		if (srv_spin_wait_delay) {
			ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));
			}

		i++;
		}

	if (i == SYNC_SPIN_ROUNDS) {
		os_thread_yield();
		}

	mutex_spin_round_count += i;


	if (mutex_test_and_set(mutex) == 0) {
		/* Succeeded! */

		assert(mutex->thread_id = os_thread_get_curr_id());

		goto finish_timing;
		}

	/* We may end up with a situation where lock_word is 0 but the OS
	fast mutex is still reserved. On FreeBSD the OS does not seem to
	schedule a thread which is constantly calling pthread_mutex_trylock
	(in mutex_test_and_set implementation). Then we could end up
	spinning here indefinitely. The following 'i++' stops this infinite
	spin. */

	i++;

	if (i < SYNC_SPIN_ROUNDS) {
		goto spin_loop;
		}

	/* The memory order of the array reservation and the change in the
	waiters field is important: when we suspend a thread, we first
	reserve the cell and then set waiters field to 1. When threads are
	released in mutex_exit, the waiters field is first set to zero and
	then the event is set to the signaled state. */


	sigToken = mutex->event->reset();

	mutex_set_waiters(mutex, 1);

	/* Try to reserve still a few times */
	for (i = 0; i < 4; i++) {
		if (mutex_test_and_set(mutex) == 0) {
			/* Succeeded! Free the reserved wait cell */

			//sync_array_free_cell(sync_primary_wait_array, index);

			assert(mutex->thread_id = os_thread_get_curr_id());

			goto finish_timing;

			/* Note that in this case we leave the waiters field
			set to 1. We cannot reset it to zero, as we do not
			know if there are other waiters. */
			}
		}

	/* Now we know that there has been some thread holding the mutex
	after the change in the wait array and the waiters field was made.
	Now there is no risk of infinite wait on the event. */

	mutex_os_wait_count++;

	mutex->event->wait(-1, sigToken);

	goto mutex_loop;

finish_timing:
	return;
}

/**********************************************************************
Releases the threads waiting in the primary wait array for this mutex. */

void mutex_signal_object(
						 /*================*/
						 mutex_t*	mutex)	/* in: mutex */
{
	mutex_set_waiters(mutex, 0);

	/* The memory order of resetting the waiters field and
	signaling the object is important. See LEMMA 1 above. */
	mutex->event->signal(true);
}





/**********************************************************************
Performs an atomic test-and-set instruction to the lock_word field of a
mutex. */

int mutex_test_and_set(
					   /*===============*/
					   /* out: the previous value of lock_word: 0 or
					   1 */
					   mutex_t*	mutex)	/* in: mutex */
{
	return (mutex->lock_word.compareAndSwap(0, 1) ? 0 : 1);
}

/*Performs a reset instruction to the lock_word field of a mutex. This
instruction also serializes memory operations to the program order. */
void mutex_reset_lock_word(
						   /*==================*/
						   mutex_t*	mutex)	/* in: mutex */
{
	assert(mutex);
	mutex->lock_word.compareAndSwap(1,0);
}

/*Gets the value of the lock word. */
int mutex_get_lock_word(
						/*================*/
						const mutex_t*	mutex)	/* in: mutex */
{
	return mutex->lock_word.get();
}


/**********************************************************************
Gets the waiters field in a mutex. */
ulint mutex_get_waiters(
						/*==============*/
						/* out: value to set */
						const mutex_t*	mutex)	/* in: mutex */
{
	const volatile ulint*	ptr;	/* declared volatile to ensure that
									the value is read from memory */
	assert(mutex);

	ptr = &(mutex->waiters);

	return(*ptr);		/* Here we assume that the read of a single
						word from memory is atomic */
}

/**********************************************************************
Unlocks a mutex owned by the current thread. */
void mutex_exit(
				/*=======*/
				mutex_t*	mutex)	/* in: pointer to mutex */
{
	assert(mutex_own(mutex));

	assert(mutex->thread_id = (os_thread_id_t) ULINT_UNDEFINED);

	mutex_reset_lock_word(mutex);

	/* A problem: we assume that mutex_reset_lock word
	is a memory barrier, that is when we read the waiters
	field next, the read must be serialized in memory
	after the reset. A speculative processor might
	perform the read first, which could leave a waiting
	thread hanging indefinitely.

	Our current solution call every second
	sync_arr_wake_threads_if_sema_free()
	to wake up possible hanging threads if
	they are missed in mutex_signal_object. */

	if (mutex_get_waiters(mutex) != 0) {
		mutex_signal_object(mutex);
	}
}

/**********************************************************************
Locks a mutex for the current thread. If the mutex is reserved, the function
spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting for the mutex
before suspending the thread. */
void mutex_enter_func(
					  /*=============*/
					  mutex_t*	mutex,		/* in: pointer to mutex */
					  const char*	file_name,	/* in: file name where locked */
					  ulint		line)		/* in: line where locked */
{
	assert(mutex_validate(mutex));
	assert(!mutex_own(mutex));

	if (!mutex_test_and_set(mutex)) {
		assert(mutex->thread_id = os_thread_get_curr_id());
		return;	/* Succeeded! */
	}

	mutex_spin_wait(mutex, file_name, line);
}

