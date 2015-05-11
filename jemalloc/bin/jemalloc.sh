#!/bin/sh

<<<<<<< HEAD
prefix=/styx/home/hzjianghongxiang/jianghx/mysqlv6/5.5.30-v6/build/jemalloc
exec_prefix=/styx/home/hzjianghongxiang/jianghx/mysqlv6/5.5.30-v6/build/jemalloc
=======
prefix=/styx/home/hzraolh/work/5.5.30-v6/innosql/build/jemalloc
exec_prefix=/styx/home/hzraolh/work/5.5.30-v6/innosql/build/jemalloc
>>>>>>> 7271f2314eda5546ecfb5ac3e8841dbbe137f2ea
libdir=${exec_prefix}/lib

LD_PRELOAD=${libdir}/libjemalloc.so.1
export LD_PRELOAD
exec "$@"
