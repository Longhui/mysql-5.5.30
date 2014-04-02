/* Copyright (C) 2007 Google Inc.
   Copyright (C) 2008 MySQL AB

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


#include "semisync.h"

const unsigned char ReplSemiSyncBase::kPacketMagicNum = 0xef;
const unsigned char ReplSemiSyncBase::kPacketFlagSync = 0x01;


const unsigned long Trace::kTraceGeneral  = 0x0001;
const unsigned long Trace::kTraceDetail   = 0x0010;
const unsigned long Trace::kTraceNetWait  = 0x0020;
const unsigned long Trace::kTraceFunction = 0x0040;

const unsigned char  ReplSemiSyncBase::kSyncHeader[2] =
  {ReplSemiSyncBase::kPacketMagicNum, 0};

static unsigned long long timespec_to_usec(const struct timespec *ts)
{
#ifndef __WIN__
  return (unsigned long long) ts->tv_sec * TIME_MILLION + ts->tv_nsec / TIME_THOUSAND;
#else
  return ts->tv.i64 / 10;
#endif /* __WIN__ */
}

/* Get the waiting time given the wait's staring time.
 * 
 * Return:
 *  >= 0: the waiting time in microsecons(us)
 *   < 0: error in get time or time back traverse
 */
int getWaitTime(const struct timespec& start_ts)
{
  unsigned long long start_usecs, end_usecs;
  struct timespec end_ts;
  
  /* Starting time in microseconds(us). */
  start_usecs = timespec_to_usec(&start_ts);

  /* Get the wait time interval. */
  set_timespec(end_ts, 0);

  /* Ending time in microseconds(us). */
  end_usecs = timespec_to_usec(&end_ts);

  if (end_usecs < start_usecs)
    return -1;

  return (int)(end_usecs - start_usecs);
}
