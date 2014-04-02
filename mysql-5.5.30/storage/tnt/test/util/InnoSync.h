/**
 * MySQL 5.4中的InnoDB同步机制实现
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _INNO_SYNC_H_
#define _INNO_SYNC_H_

#include "util/Sync.h"

using namespace ntse;

typedef long int lint;
typedef unsigned long ulint;
#ifdef WIN32
typedef void*			os_thread_t;
typedef ulint			os_thread_id_t;	/* In Windows the thread id
										is an unsigned long int */
#else
typedef pthread_t os_thread_t;
typedef os_thread_t os_thread_id_t;
#endif

typedef struct rw_lock_struct rw_lock_t;
typedef u64 ib_longlong;

/* number of spin waits on rw-latches,
resulted during shared (read) locks */
ib_longlong	rw_s_spin_wait_count	= 0;
ib_longlong	rw_s_spin_round_count	= 0;

/* number of OS waits on rw-latches,
resulted during shared (read) locks */
ib_longlong	rw_s_os_wait_count	= 0;

/* number of unlocks (that unlock shared locks),
set only when UNIV_SYNC_PERF_STAT is defined */
ib_longlong	rw_s_exit_count		= 0;

/* number of spin waits on rw-latches,
resulted during exclusive (write) locks */
ib_longlong	rw_x_spin_wait_count	= 0;
ib_longlong	rw_x_spin_round_count	= 0;

/* number of OS waits on rw-latches,
resulted during exclusive (write) locks */
ib_longlong	rw_x_os_wait_count	= 0;

/* number of unlocks (that unlock exclusive locks),
set only when UNIV_SYNC_PERF_STAT is defined */
ib_longlong	rw_x_exit_count		= 0;

#define X_LOCK_DECR		0x00100000

bool	ut_always_false	= false;

#define	SYNC_SPIN_ROUNDS	30

/*****************************************************************
Runs an idle loop on CPU. The argument gives the desired delay
in microseconds on 100 MHz Pentium + Visual C++. */

ulint
ut_delay(
/*=====*/
			/* out: dummy value */
	ulint	delay)	/* in: delay in microseconds on 100 MHz Pentium */
{
	ulint	i, j;

	j = 0;

	for (i = 0; i < delay * 50; i++) {
                PAUSE_INSTRUCTION();
		j += i;
	}

	if (ut_always_false) {
		ut_always_false = j != 0;
	}

	return(j);
}


struct rw_lock_struct {
	volatile lint	lock_word;
				/* Holds the state of the lock. */
	volatile ulint	waiters;/* 1: there are waiters */
	volatile ulint	pass;	/* Default value 0. This is set to some
				value != 0 given by the caller of an x-lock
				operation, if the x-lock is to be passed to
				another thread to unlock (which happens in
				asynchronous i/o). */
	volatile os_thread_id_t	writer_thread;
				/* Thread id of writer thread */
	Event	*event;	/* Used by sync0arr.c for thread queueing */
	Event	*wait_ex_event;
				/* Event for next-writer to wait on. A thread
				must decrement lock_word before waiting. */
	ulint count_os_wait;	/* Count of os_waits. May not be accurate */
	const char*	cfile_name;/* File name where lock created */
        /* last s-lock file/line is not guaranteed to be correct */
	const char*	last_s_file_name;/* File name where last s-locked */
	const char*	last_x_file_name;/* File name where last x-locked */
	unsigned	cline:14;	/* Line where created */
	unsigned	last_s_line:14;	/* Line number where last time s-locked */
	unsigned	last_x_line:14;	/* Line number where last time x-locked */
};

/**********************************************************************
Creates, or rather, initializes an rw-lock object in a specified memory
location (which must be appropriately aligned). The rw-lock is initialized
to the non-locked state. Explicit freeing of the rw-lock with rw_lock_free
is necessary only if the memory block containing it is freed. */

void
rw_lock_create_func(
/*================*/
	rw_lock_t*	lock,		/* in: pointer to memory */
	const char*	cfile_name,	/* in: file name where created */
	ulint 		cline)		/* in: file line where created */
{
	/* If this is the very first time a synchronization object is
	created, then the following call initializes the sync system. */

	lock->lock_word = X_LOCK_DECR;
	lock->waiters = 0;
 	lock->pass = 1;
 	/* We do not have to initialize writer_thread until pass == 0 */

	lock->cfile_name = cfile_name;
	lock->cline = (unsigned int) cline;

	lock->count_os_wait = 0;
	lock->last_s_file_name = "not yet reserved";
	lock->last_x_file_name = "not yet reserved";
	lock->last_s_line = 0;
	lock->last_x_line = 0;
	lock->event = new Event(false); 
	lock->wait_ex_event = new Event(false);
}

/**************************************************************
Atomic compare-and-swap for InnoDB. Currently requires GCC atomic builtins
or Solaris atomic_* functions. */
bool
os_compare_and_swap(
/*================*/
					/* out: true if swapped */
	volatile lint* 	ptr,		/* in: pointer to target */
	lint		oldVal,		/* in: value to compare to */
	lint		newVal)		/* in: value to swap in */
{
#ifdef WIN32
	lint retVal = (lint)_InterlockedCompareExchange(ptr, newVal, oldVal);
	return (retVal == oldVal);
#else
	return (__sync_bool_compare_and_swap(ptr, oldVal, newVal));
#endif
}

void
rw_lock_set_waiters(
/*================*/
	rw_lock_t*	lock)
{
	os_compare_and_swap((volatile lint *)&(lock->waiters), 0, 1);
}

void
rw_lock_reset_waiters(
/*================*/
	rw_lock_t*	lock)
{
	os_compare_and_swap((volatile lint *)&(lock->waiters), 1, 0);
}

/**********************************************************************
Two different implementations for decrementing the lock_word of a rw_lock:
one for systems supporting atomic operations, one for others. This does
does not support recusive x-locks: they should be handled by the caller and
need not be atomic since they are performed by the current lock holder.
Returns true if the decrement was made, false if not. */
bool
rw_lock_lock_word_decr(
				/* out: true if decr occurs */
	rw_lock_t*	lock,	/* in: rw-lock */
	ulint		amount)	/* in: amount of decrement */
{
	lint local_lock_word = lock->lock_word;
	while (local_lock_word > 0) {
		if(os_compare_and_swap(&(lock->lock_word),
                                       local_lock_word,
                                       local_lock_word - amount)) {
			return true;
		}
		local_lock_word = lock->lock_word;
	}
	return(false);
}

ulint	srv_spin_wait_delay	= 6;

ulint	ut_rnd_ulint_counter = 65654363;
#define UT_RND1			151117737
#define UT_RND2			119785373
#define UT_RND3			 85689495
#define UT_RND4			 76595339
#define UT_SUM_RND2		 98781234
#define UT_SUM_RND3		126792457
#define UT_SUM_RND4		 63498502
#define UT_XOR_RND1		187678878
#define UT_XOR_RND2		143537923

/************************************************************
The following function generates a series of 'random' ulint integers. */
ulint
ut_rnd_gen_next_ulint(
/*==================*/
			/* out: the next 'random' number */
	ulint	rnd)	/* in: the previous random number value */
{
	ulint	n_bits;

	n_bits = 8 * sizeof(ulint);

	rnd = UT_RND2 * rnd + UT_SUM_RND3;
	rnd = UT_XOR_RND1 ^ rnd;
	rnd = (rnd << 20) + (rnd >> (n_bits - 20));
	rnd = UT_RND3 * rnd + UT_SUM_RND4;
	rnd = UT_XOR_RND2 ^ rnd;
	rnd = (rnd << 20) + (rnd >> (n_bits - 20));
	rnd = UT_RND1 * rnd + UT_SUM_RND2;

	return(rnd);
}

/************************************************************
The following function generates 'random' ulint integers which
enumerate the value space of ulint integers in a pseudo random
fashion. Note that the same integer is repeated always after
2 to power 32 calls to the generator (if ulint is 32-bit). */
ulint
ut_rnd_gen_ulint(void)
/*==================*/
			/* out: the 'random' number */
{
	ulint	rnd;
	ulint	n_bits;

	n_bits = 8 * sizeof(ulint);

	ut_rnd_ulint_counter = UT_RND1 * ut_rnd_ulint_counter + UT_RND2;

	rnd = ut_rnd_gen_next_ulint(ut_rnd_ulint_counter);

	return(rnd);
}

/************************************************************
Generates a random integer from a given interval. */
ulint
ut_rnd_interval(
/*============*/
			/* out: the 'random' number */
	ulint	low,	/* in: low limit; can generate also this value */
	ulint	high)	/* in: high limit; can generate also this value */
{
	ulint	rnd;

	if (low == high) {

		return(low);
	}

	rnd = ut_rnd_gen_ulint();

	return(low + (rnd % (high - low + 1)));
}

/*********************************************************************
Advises the os to give up remainder of the thread's time slice. */

void
os_thread_yield(void)
/*=================*/
{
#ifdef WIN32
	Sleep(0);
#else
	pthread_yield();
#endif
}

/**********************************************************************
Low-level function which tries to lock an rw-lock in s-mode. Performs no
spinning. */
bool
rw_lock_s_lock_low(
/*===============*/
				/* out: true if success */
	rw_lock_t*	lock,	/* in: pointer to rw-lock */
	ulint		pass,
				/* in: pass value; != 0, if the lock will be
				passed to another thread to unlock */
	const char*	file_name, /* in: file name where lock requested */
	ulint		line)	/* in: line where requested */
{
	/* TODO: study performance of UNIV_LIKELY branch prediction hints. */
	if (!rw_lock_lock_word_decr(lock, 1)) {
		/* Locking did not succeed */
		return(false);
	}

	/* These debugging values are not set safely: they may be incorrect
        or even refer to a line that is invalid for the file name. */
	lock->last_s_file_name = file_name;
	lock->last_s_line = line;

	return(true);	/* locking succeeded */
}

/**********************************************************************
Lock an rw-lock in shared mode for the current thread. If the rw-lock is
locked in exclusive mode, or there is an exclusive lock request waiting,
the function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting
for the lock, before suspending the thread. */

void
rw_lock_s_lock_spin(
/*================*/
	rw_lock_t*	lock,	/* in: pointer to rw-lock */
	ulint		pass,	/* in: pass value; != 0, if the lock
				will be passed to another thread to unlock */
	const char*	file_name, /* in: file name where lock requested */
	ulint		line)	/* in: line where requested */
{
//	ulint	 index;	/* index of the reserved wait cell */
	ulint	 i = 0;	/* spin round count */

	rw_s_spin_wait_count++;	/* Count calls to this function */
lock_loop:

	/* Spin waiting for the writer field to become free */
	while (i < SYNC_SPIN_ROUNDS && lock->lock_word <= 0) {
		if (srv_spin_wait_delay) {
			ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));
		}

		i++;
	}

	if (i == SYNC_SPIN_ROUNDS) {
		os_thread_yield();
	}

	/* We try once again to obtain the lock */
	if (true == rw_lock_s_lock_low(lock, pass, file_name, line)) {
		rw_s_spin_round_count += i;

		return; /* Success */
	} else {

		if (i < SYNC_SPIN_ROUNDS) {
			goto lock_loop;
		}

		rw_s_spin_round_count += i;

		long sigToken = lock->event->reset();

		/* Set waiters before checking lock_word to ensure wake-up
                signal is sent. This may lead to some unnecessary signals. */
		rw_lock_set_waiters(lock);

		if (true == rw_lock_s_lock_low(lock, pass, file_name, line)) {
			return; /* Success */
		}

		/* these stats may not be accurate */
		lock->count_os_wait++;
		rw_s_os_wait_count++;

		lock->event->wait(-1,sigToken);

		i = 0;
		goto lock_loop;
	}
}

/**********************************************************************
NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in shared mode for the current thread. If the rw-lock is locked
in exclusive mode, or there is an exclusive lock request waiting, the
function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting for
the lock, before suspending the thread. */
void
rw_lock_s_lock_func(
/*================*/
	rw_lock_t*	lock,	/* in: pointer to rw-lock */
	ulint		pass,	/* in: pass value; != 0, if the lock will
				be passed to another thread to unlock */
	const char*	file_name,/* in: file name where lock requested */
	ulint		line)	/* in: line where requested */
{
	/* NOTE: As we do not know the thread ids for threads which have
	s-locked a latch, and s-lockers will be served only after waiting
	x-lock requests have been fulfilled, then if this thread already
	owns an s-lock here, it may end up in a deadlock with another thread
	which requests an x-lock here. Therefore, we will forbid recursive
	s-locking of a latch: the following assert will warn the programmer
	of the possibility of this kind of a deadlock. If we want to implement
	safe recursive s-locking, we should keep in a list the thread ids of
	the threads which have s-locked a latch. This would use some CPU
	time. */

	/* TODO: study performance of UNIV_LIKELY branch prediction hints. */
	if (rw_lock_s_lock_low(lock, pass, file_name, line)) {

		return; /* Success */
	} else {
		/* Did not succeed, try spin wait */

		rw_lock_s_lock_spin(lock, pass, file_name, line);

		return;
	}
}

/*******************************************************************
Compares two thread ids for equality. */

bool
os_thread_eq(
/*=========*/
				/* out: true if equal */
	os_thread_id_t	a,	/* in: OS thread or thread id */
	os_thread_id_t	b)	/* in: OS thread or thread id */
{
#ifdef WIN32
	if (a == b) {
		return(TRUE);
	}

	return(FALSE);
#else
	if (pthread_equal(a, b)) {
		return(true);
	}

	return(false);
#endif
}

/**********************************************************************
Function for the next writer to call. Waits for readers to exit.
The caller must have already decremented lock_word by X_LOCK_DECR.*/
void
rw_lock_x_lock_wait(
/*================*/
	rw_lock_t*	lock,	/* in: pointer to rw-lock */
	const char*	file_name,/* in: file name where lock requested */
	ulint		line)	/* in: line where requested */
{
//	ulint index;
	ulint i = 0;

	while (lock->lock_word < 0) {
		if (srv_spin_wait_delay) {
			ut_delay(ut_rnd_interval(0, srv_spin_wait_delay));
		}
		if(i < SYNC_SPIN_ROUNDS) {
			i++;
			continue;
		}

		/* If there is still a reader, then go to sleep.*/
		rw_x_spin_round_count += i;
		i = 0;
		
		long sigToken = lock->wait_ex_event->reset();

		/* Check lock_word to ensure wake-up isn't missed.*/
		if(lock->lock_word < 0) {

			/* these stats may not be accurate */
			lock->count_os_wait++;
			rw_x_os_wait_count++;

                        /* Add debug info as it is needed to detect possible
                        deadlock. We must add info for WAIT_EX thread for
                        deadlock detection to work properly. */
			lock->wait_ex_event->wait(-1,sigToken);

                        /* It is possible to wake when lock_word < 0.
                        We must pass the while-loop check to proceed.*/
		} else {
		}
	}
	rw_x_spin_round_count += i;
}

/*********************************************************************
Returns the thread identifier of current thread. Currently the thread
identifier in Unix is the thread handle itself. Note that in HP-UX
pthread_t is a struct of 3 fields. */

os_thread_id_t os_thread_get_curr_id(void)
/*=======================*/
{
#ifdef WIN32
	return(GetCurrentThreadId());
#else
	return(pthread_self());
#endif
}

/**********************************************************************
Low-level function for acquiring an exclusive lock. */
bool
rw_lock_x_lock_low(
/*===============*/
				/* out: RW_LOCK_NOT_LOCKED if did
				not succeed, RW_LOCK_EX if success. */
	rw_lock_t*	lock,	/* in: pointer to rw-lock */
	ulint		pass,	/* in: pass value; != 0, if the lock will
				be passed to another thread to unlock */
	const char*	file_name,/* in: file name where lock requested */
	ulint		line)	/* in: line where requested */
{
	os_thread_id_t	curr_thread	= os_thread_get_curr_id();

	if(rw_lock_lock_word_decr(lock, X_LOCK_DECR)) {

		/* Decrement occurred: we are writer or next-writer. */
		lock->writer_thread = curr_thread;
		lock->pass = pass;
		rw_lock_x_lock_wait(lock,
                                    file_name, line);

	} else {
		/* Decrement failed: relock or failed lock */
		/* Must verify pass first: otherwise another thread can
		call move_ownership suddenly allowing recursive locks.
		and after we have verified our thread_id matches
		(though move_ownership has since changed it).*/
		if(!pass && !(lock->pass) &&
                   os_thread_eq(lock->writer_thread, curr_thread)) {
			/* Relock */
                        lock->lock_word -= X_LOCK_DECR;
		} else {
			/* Another thread locked before us */
			return(false);
		}
	}
	lock->last_x_file_name = file_name;
	lock->last_x_line = (unsigned int) line;

	return(true);
}

/**********************************************************************
NOTE! Use the corresponding macro, not directly this function! Lock an
rw-lock in exclusive mode for the current thread. If the rw-lock is locked
in shared or exclusive mode, or there is an exclusive lock request waiting,
the function spins a preset time (controlled by SYNC_SPIN_ROUNDS), waiting
for the lock before suspending the thread. If the same thread has an x-lock
on the rw-lock, locking succeed, with the following exception: if pass != 0,
only a single x-lock may be taken on the lock. NOTE: If the same thread has
an s-lock, locking does not succeed! */

void
rw_lock_x_lock_func(
/*================*/
	rw_lock_t*	lock,	/* in: pointer to rw-lock */
	ulint		pass,	/* in: pass value; != 0, if the lock will
				be passed to another thread to unlock */
	const char*	file_name,/* in: file name where lock requested */
	ulint		line)	/* in: line where requested */
{
//	ulint	index;	/* index of the reserved wait cell */
	ulint	i;	/* spin round count */
	bool   spinning = false;

	i = 0;

lock_loop:

	if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
		rw_x_spin_round_count += i;

		return;	/* Locking succeeded */

	} else {

                if (!spinning) {
                        spinning = true;
                        rw_x_spin_wait_count++;
		}

		/* Spin waiting for the lock_word to become free */
		while (i < SYNC_SPIN_ROUNDS
		       && lock->lock_word <= 0) {
			if (srv_spin_wait_delay) {
				ut_delay(ut_rnd_interval(0,
							 srv_spin_wait_delay));
			}

			i++;
		}
		if (i == SYNC_SPIN_ROUNDS) {
		//	os_thread_yield();
		} else {
			goto lock_loop;
		}
	}

	rw_x_spin_round_count += i;

	long sigToken = lock->event->reset();

	/* Waiters must be set before checking lock_word, to ensure signal
	is sent. This could lead to a few unnecessary wake-up signals. */
	rw_lock_set_waiters(lock);

	if (rw_lock_x_lock_low(lock, pass, file_name, line)) {
		return; /* Locking succeeded */
	}

	/* these stats may not be accurate */
	lock->count_os_wait++;
	rw_x_os_wait_count++;

	lock->event->wait(-1,sigToken);

	i = 0;
	goto lock_loop;
}

/**************************************************************
Atomic increment for InnoDB. Currently requires GCC atomic builtins. */
lint
os_atomic_increment(
/*================*/
					/* out: resulting value */
	volatile lint*	ptr,		/* in: pointer to target */
	lint		amount)		/* in: amount of increment */
{
#ifdef WIN32
	return ((lint)InterlockedExchangeAdd(ptr, amount)) + amount;
#else
	return (__sync_add_and_fetch(ptr, amount));
#endif
}

/************************************************************************
Accessor functions for rw lock. */
ulint
rw_lock_get_waiters(
/*================*/
	rw_lock_t*	lock)
{
	return(lock->waiters);
}

/**********************************************************************
Two different implementations for incrementing the lock_word of a rw_lock:
one for systems supporting atomic operations, one for others.
Returns the value of lock_word after increment. */
lint
rw_lock_lock_word_incr(
				/* out: lock->lock_word after increment */
	rw_lock_t*	lock,	/* in: rw-lock */
	ulint		amount)	/* in: amount of increment */
{
	return(os_atomic_increment(&(lock->lock_word), amount));
}

/**********************************************************************
Releases an exclusive mode lock. */
void
rw_lock_x_unlock_func(
/*==================*/
	rw_lock_t*	lock	/* in: rw-lock */
	)
{
        uint local_pass;

	/*
          Must reset pass while we still have the lock.
	  If we are not the last unlocker, we correct it later in the function,
	  which is harmless since we still hold the lock.
        */
        local_pass = lock->pass;
        lock->pass = 1;

	if(rw_lock_lock_word_incr(lock, X_LOCK_DECR) == X_LOCK_DECR) {
		/* Lock is now free. May have to signal read/write waiters.
                We do not need to signal wait_ex waiters, since they cannot
                exist when there is a writer. */
		if(rw_lock_get_waiters(lock)) {
			rw_lock_reset_waiters(lock);
			lock->event->signal(true);
		}

	} else {
		/* We still hold x-lock, so we correct pass. */
		lock->pass = local_pass;
	}
	rw_x_exit_count++;
}

/**********************************************************************
Releases a shared mode lock. */
void
rw_lock_s_unlock_func(
/*==================*/
	rw_lock_t*	lock	/* in: rw-lock */
	)
{
	/* Increment lock_word to indicate 1 less reader */
	if(rw_lock_lock_word_incr(lock, 1) == 0) {

		/* wait_ex waiter exists. It may not be asleep, but we signal
                anyway. We do not wake other waiters, because they can't
                exist without wait_ex waiter and wait_ex waiter goes first.*/
		lock->wait_ex_event->signal(false);

	}
	rw_s_exit_count++;
}

/**********************************************************************
Calling this function is obligatory only if the memory buffer containing
the rw-lock is freed. Removes an rw-lock object from the global list. The
rw-lock is checked to be in the non-locked state. */

void
rw_lock_free(
/*=========*/
	rw_lock_t*	lock)	/* in: rw-lock */
{
	delete lock->event;
	delete lock->wait_ex_event;
}

#endif
