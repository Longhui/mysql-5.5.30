#!/bin/sh

prefix=/styx/home/hzjianghongxiang/jianghx/git/5.5.30-v5/build/jemalloc
exec_prefix=/styx/home/hzjianghongxiang/jianghx/git/5.5.30-v5/build/jemalloc
libdir=${exec_prefix}/lib

LD_PRELOAD=${libdir}/libjemalloc.so.1
export LD_PRELOAD
exec "$@"
