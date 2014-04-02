/**
 * Quickly Requirable Lock
 *
 * @author: wy
 */

#ifndef _NTSETEST_QRL_H_
#define _NTSETEST_QRL_H_

#ifdef WIN32
#include <Windows.h>
#include <intrin.h>
#endif

inline bool CAS(volatile int *p, int expect, int update) {
#ifdef WIN32
	return _InterlockedCompareExchange((LONG *)p, (LONG)update, (LONG)expect) == expect;
#else
	return __sync_bool_compare_and_swap(p, expect, update);
#endif
}

/* statuses for qrl locks */ 
#define BIASED(id) ((int)(id) << 2) 
#define NEUTRAL 1 
#define DEFAULT 2 
#define REVOKED 3 
#define ISBIASED(status) (0 == ((status) & 3)) 

/* word manipulation (little-endian versions shown here) */ 
#define MAKEDWORD(low, high) (((unsigned int)(high) << 16) | (low)) 
#define LOWWORD(dword) ((unsigned short)dword) 
#define HIGHWORD(dword) ((unsigned short)(((unsigned int)(dword)) >> 16)) 

#define QRL_BASE 50 /* Initial backoff value */ 
#define QRL_CAP 800 /* Maximum backoff value */ 
typedef struct tag_qrltas_lock 
{ 
	volatile union 
	{
		struct 
		{ 
			short quicklock; 
			short status; 
		} 
		h; 
		int data; 
	} 
	lockword; 
	volatile int defaultlock; 
} 
qrltas_lock; 

inline void qrltas_initialize(qrltas_lock *L) 
{ 
L->lockword.data = MAKEDWORD(0, NEUTRAL); 
L->defaultlock = 0; 
} 


extern int qrltas_default_acquire(qrltas_lock *L, int id);

inline int qrltas_acquire(qrltas_lock *L, int id) 
{ 
int status = L->lockword.h.status; 
/* If the lock¡¯s mine, I can reenter by just setting a flag */ 
if (BIASED(id) == status) 
{ 
L->lockword.h.quicklock = 1; 
if (BIASED(id) == HIGHWORD(L->lockword.data)) 
return 1; 
L->lockword.h.quicklock = 0; /* I didn¡¯t get the lock, so make sure I 
don¡¯t block up the process that did */ 
} 
return qrltas_default_acquire(L, id);
}

inline void qrltas_release(qrltas_lock *L, int acquiredquickly) 
{ 
if (acquiredquickly) 
L->lockword.h.quicklock = 0; 
else 
L->defaultlock = 0; 
} 

#endif

