/**
 * Quickly Requirable Lock
 *
 * @author: wy
 */

#include <assert.h>
#include "util/Qrl.h"

int qrltas_default_acquire(qrltas_lock *L, int id)
{
int status = L->lockword.h.status;
if (DEFAULT != status) 
{ 
/* If the lock is unowned, try to claim it */ 
if (NEUTRAL == status) 
{ 
if (CAS(&L->lockword.data, /* By definition, if we saw */ 
MAKEDWORD(0, NEUTRAL), /* neutral, the lock is unheld */ 
MAKEDWORD(1, BIASED(id)))) 
{ 
/* Biasing the lock counts as an acquisition */ 
return 1; 
} 
/* If I didn¡¯t bias the lock to me, someone else just grabbed 
it. Fall through to the revocation code */ 
status = L->lockword.h.status; /* resample */ 
} 
/* If someone else owns the lock, revoke them */ 
if (ISBIASED(status)) 
{ 
do 
{ 
unsigned short biaslock = L->lockword.h.quicklock; 
if (CAS(&L->lockword.data, 
MAKEDWORD(biaslock, status), 
MAKEDWORD(biaslock, REVOKED))) 
{ 
/* I¡¯m the revoker. Claim my lock. */ 
L->defaultlock = 1; 
L->lockword.h.status = DEFAULT; 
/* Wait until lock is free */ 
while (LOWWORD(L->lockword.data)) 
; 
return 0; /* And then it¡¯s mine */ 
} 
/* The CAS could have failed and we got here for either of 
two reasons. First, another process could have done the 
revoking; in this case we need to fall through to the 
default path once the other process is finished revoking. 
Secondly, the bias process could have acquired or released 
the biaslock field; in this case we need merely retry. */ 
status = L->lockword.h.status; 
} 
while (ISBIASED(L->lockword.h.status)); 
} 

/* If I get here, the lock has been revoked by someone other 
than me. Wait until they¡¯re done revoking, then fall through 
to the default code. */ 
while (DEFAULT != L->lockword.h.status) 
; 
} 
/* Regular Tatas from here on */ 
assert(DEFAULT == L->lockword.h.status); 
while (!CAS(&L->defaultlock, 0, 1)) 
while (L->defaultlock) 
; 
return 0; 
} 

