#!/bin/sh

prefix=/styx/home/hzwenzhh/version_work/build/jemalloc
exec_prefix=/styx/home/hzwenzhh/version_work/build/jemalloc
libdir=${exec_prefix}/lib

LD_PRELOAD=${libdir}/libjemalloc.so.1
export LD_PRELOAD
exec "$@"
