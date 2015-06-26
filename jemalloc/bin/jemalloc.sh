#!/bin/sh

prefix=/styx/home/hzraolh/work/5.5.30-v6a_add_rotate_event_in_relaylog/innosql/build/jemalloc
exec_prefix=/styx/home/hzraolh/work/5.5.30-v6a_add_rotate_event_in_relaylog/innosql/build/jemalloc
libdir=${exec_prefix}/lib

LD_PRELOAD=${libdir}/libjemalloc.so.1
export LD_PRELOAD
exec "$@"
