# Copyright (C) 2005-2006 MySQL AB
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

#called from the top level Makefile
AM_CXXFLAGS =  -fimplicit-templates -fexceptions

MYSQLDATAdir =          $(localstatedir)
MYSQLSHAREdir =         $(pkgdatadir)
MYSQLBASEdir=           $(prefix)
MYSQLLIBdir=            $(pkglibdir)
pkgplugindir =      	$(libdir)/plugin

INCLUDES =              -I$(top_srcdir)/src/include \
						$(MYSQL_INC) \
						-I$(top_srcdir)/storage/tnt/src/include \
						-I$(top_srcdir)/storage/tnt/src/ \
						-I$(top_srcdir)/include -I$(top_builddir)/include \
						-I$(top_srcdir)/regex \
						-I$(top_srcdir)/sql \
                        -I$(srcdir) \
                         @keyvalue_INCLUDE@
WRAPLIBS=

LDADD = @keyvalue_LDADD@

##preprocessor macros
DEFS =                  @DEFS@

##无需安装的头文件
noinst_HEADERS = src/ha_tnt.h \
			src/ntse_handler.h \
			src/RowCache.h \
			src/include/api/Database.h \
			src/include/api/IdxPreAlter.h \
			src/include/api/LobDefraggler.h \
			src/include/api/Table.h \
			src/include/api/TblArgAlter.h \
			src/include/api/TblMnt.h \
			src/include/api/TNTTblMnt.h \
			src/include/api/TNTDatabase.h \
			src/include/api/TNTTable.h \
			src/include/api/TNTTblScan.h \
			src/include/api/TNTTblArgAlter.h \
			src/include/api/Transaction.h \
			src/include/btree/Index.h \
			src/include/btree/IndexBLinkTree.h \
			src/include/btree/IndexBLinkTreeManager.h \
			src/include/btree/IndexBPTree.h \
			src/include/btree/IndexBPTreesManager.h \
			src/include/btree/IndexCommon.h \
			src/include/btree/IndexKey.h \
			src/include/btree/IndexLog.h \
			src/include/btree/IndexPage.h \
			src/include/btree/MIndex.h \
			src/include/btree/MIndexKey.h \
			src/include/btree/MIndexPage.h \
			src/include/btree/OuterSorter.h \
			src/include/btree/TNTIndex.h \
			src/include/compress/CreateDicHelper.h \
			src/include/compress/dastrie.h \
			src/include/compress/RCMSampleTbl.h \
			src/include/compress/RowCompress.h \
			src/include/compress/RowCompressCfg.h \
			src/include/compress/RowCompressCoder.h \
			src/include/compress/SmplTrie.h \
			src/include/heap/Heap.h \
			src/include/heap/FixedLengthRecordHeap.h \
			src/include/heap/VariableLengthRecordHeap.h \
			src/include/heap/HashIndex.h \
			src/include/heap/MHeap.h \
			src/include/heap/MHeapRecord.h \
			src/include/heap/VersionPool.h \
			src/include/lob/lzo/config1x.h \
			src/include/lob/lzo/lzo1x.h \
			src/include/lob/lzo/lzo_conf.h \
			src/include/lob/lzo/lzo_dict.h \
			src/include/lob/lzo/lzo_ptr.h \
			src/include/lob/lzo/lzoconf.h \
			src/include/lob/lzo/lzodefs.h \
			src/include/lob/lzo/miniacc.h \
			src/include/lob/BigLob.h \
			src/include/lob/Lob.h \
			src/include/lob/LobIndex.h \
			src/include/lob/Lzo.h \
			src/include/lob/SmallLob.h \
			src/include/misc/BgCustomThread.h \
			src/include/misc/Buffer.h \
			src/include/misc/Callbacks.h \
			src/include/misc/ColumnGroupParser.h \
			src/include/misc/CommonMemPool.h \
			src/include/misc/ControlFile.h \
			src/include/misc/Decimal.h \
			src/include/misc/DLDLockTable.h \
			src/include/misc/DoubleChecker.h \
			src/include/misc/Global.h \
			src/include/misc/GlobalFactory.h \
			src/include/misc/KVParser.h \
			src/include/misc/LockManager.h \
			src/include/misc/MemCtx.h \
			src/include/misc/OptimizeThread.h \
			src/include/misc/Parser.h \
			src/include/misc/ParFileParser.h \
			src/include/misc/Record.h \
			src/include/misc/RecordHelper.h \
			src/include/misc/ResourcePool.h \
			src/include/misc/Session.h \
			src/include/misc/Sample.h \
			src/include/misc/Syslog.h \
			src/include/misc/TableDef.h \
			src/include/misc/TNTControlFile.h \
			src/include/misc/TNTIMPageManager.h \
			src/include/misc/Trace.h \
			src/include/misc/Txnlog.h \
			src/include/misc/Verify.h \
			src/include/misc/Profile.h \
			src/include/mms/Mms.h \
			src/include/mms/MmsHeap.h \
			src/include/mms/MmsMap.h \
			src/include/mms/MmsPage.h \
			src/include/rec/Records.h \
			src/include/rec/MRecords.h \
			src/include/strings/CtypeCommon.h \
			src/include/trx/TLock.h \
			src/include/trx/TNTTransaction.h \
			src/include/util/Array.h \
			src/include/util/Bitmap.h \
			src/include/util/DList.h \
			src/include/util/File.h \
			src/include/util/Hash.h \
			src/include/util/NumberCompress.h \
			src/include/util/ObjectPool.h \
			src/include/util/PagePool.h  \
			src/include/util/Portable.h \
			src/include/util/Queue.h \
			src/include/util/SmartPtr.h \
			src/include/util/Stream.h \
			src/include/util/Sync.h \
			src/include/util/System.h \
			src/include/util/Thread.h \
			src/keyvalue/gen-cpp/KV.h	\
			src/keyvalue/gen-cpp/KV_constants.h	\
			src/keyvalue/gen-cpp/KV_types.h	\
			src/keyvalue/KeyValueHelper.h	\
			src/keyvalue/KeyValueHandler.h	\
			src/keyvalue/KeyValueEventHandler.h	\
			src/keyvalue/KeyValueServer.h
			src/THDInfoBase.h

tnt_sources = src/ha_tnt.cpp \
			src/ntse_handler.cpp \
			src/InfoSchema.cpp \
			src/Cmd.cpp \
			src/RowCache.cpp \
			src/api/Database.cpp \
			src/api/IdxPreAlter.cpp \
			src/api/Table.cpp \
			src/api/TblLog.cpp \
			src/api/TblMisc.cpp \
			src/api/TblMnt.cpp \
			src/api/TNTTblMnt.cpp \
			src/api/TNTDatabase.cpp \
			src/api/TNTTable.cpp \
			src/api/TNTTableLog.cpp \
			src/api/TNTTblScan.cpp \
			src/api/TNTTblArgAlter.cpp \
			src/api/Transaction.cpp \
			src/api/TblArgAlter.cpp \
			src/api/LobDefraggler.cpp \
			src/btree/Index.cpp \
			src/btree/IndexBLinkTree.cpp \
			src/btree/IndexBLinkTreeManager.cpp \
			src/btree/IndexBPTree.cpp \
			src/btree/IndexBPTreesManager.cpp \
			src/btree/IndexKey.cpp \
			src/btree/IndexLog.cpp \
			src/btree/IndexPage.cpp \
			src/btree/MIndex.cpp \
			src/btree/MIndexKey.cpp \
			src/btree/MIndexPage.cpp \
			src/btree/OuterSorter.cpp \
			src/btree/TNTIndex.cpp \
			src/compress/RCMSampleTbl.cpp \
			src/compress/RowCompress.cpp \
			src/compress/RowCompressCfg.cpp \
			src/compress/SmplTrie.cpp \
			src/heap/Heap.cpp \
			src/heap/FixedLengthRecordHeap.cpp \
			src/heap/VariableLengthRecordHeap.cpp \
			src/heap/HashIndex.cpp \
			src/heap/MHeap.cpp \
			src/heap/MHeapRecord.cpp \
			src/heap/VersionPool.cpp\
			src/lob/lzo/lzo1x_1k.c \
			src/lob/lzo/lzo1x_d1.c \
			src/lob/lzo/lzo_init.c \
			src/lob/lzo/lzo_ptr.c \
			src/lob/BigLob.cpp \
			src/lob/Lob.cpp \
			src/lob/LobIndex.cpp \
			src/lob/Lzo.cpp \
			src/lob/SmallLob.cpp \
			src/misc/BgCustomThread.cpp \
			src/misc/Buffer.cpp \
			src/misc/ColumnGroupParser.cpp \
			src/misc/CommonMemPool.cpp \
			src/misc/ControlFile.cpp \
			src/misc/Decimal.cpp \
			src/misc/DLDLockTable.cpp \
			src/misc/Global.cpp \
			src/misc/KVParser.cpp \
			src/misc/MemCtx.cpp \
			src/misc/OptimizeThread.cpp \
			src/misc/Parser.cpp \
			src/misc/ParFileParser.cpp \
			src/misc/Record.cpp \
			src/misc/RecordHelper.cpp \
			src/misc/ResourcePool.cpp \
			src/misc/Session.cpp \
			src/misc/Sample.cpp \
			src/misc/Syslog.cpp \
			src/misc/TableDef.cpp \
			src/misc/TNTControlFile.cpp \
			src/misc/TNTIMPageManager.cpp \
			src/misc/Trace.cpp \
			src/misc/TxnLog.cpp \
			src/misc/Verify.cpp \
			src/misc/Profile.cpp \
			src/mms/MmsHeap.cpp \
			src/mms/MmsMap.cpp \
			src/mms/MmsPage.cpp \
			src/mms/MmsTable.cpp \
			src/mms/MmsUtil.cpp \
			src/rec/Records.cpp \
			src/rec/MRecords.cpp \
			src/rec/MRecordsLog.cpp \
			src/rec/MRecordsRecovery.cpp \
			src/strings/Collation.cpp \
			src/strings/CtypeBin.cpp \
			src/strings/CtypeGBK.cpp \
			src/strings/CtypeLatin1.cpp \
			src/strings/CtypeUTF8.cpp \
			src/THDInfoBase.cpp \
			src/trx/TLock.cpp \
			src/trx/TNTTransaction.cpp \
			src/util/FileLin.cpp \
			src/util/FileWin.cpp \
			src/util/Hash.cpp \
			src/util/Pool.cpp \
			src/util/Stream.cpp \
			src/util/Sync.cpp \
			src/util/System.cpp \
			src/util/Thread.cpp \
			src/udf/udf.cpp
			src/keyvalue/gen-cpp/KV.cpp	\
			src/keyvalue/gen-cpp/KV_constants.cpp	\
			src/keyvalue/gen-cpp/KV_types.cpp	\
			src/keyvalue/KeyValueHandler.cpp	\
			src/keyvalue/KeyValueEventHandler.cpp	\
			src/keyvalue/KeyValueServer.cpp

##
##关于编译参数
##AM_CXXFLAGS(maintainer CXXFLAGS)，定义了target_CXXFLAGS, AM_CXXFLAGS就会失效
##target_CXXFLAGS(target specific CXXFLAGS)
##CXXFLAGS(user CXXFLAGS)，具有最高优先级
##


##automake 是静态编译的,因此必须通过EXTRA_LTLIBRARIES来声明target
EXTRA_LTLIBRARIES =	ha_tnt.la
##设置plugin_ntse_shared_target变量为ha_ntse.la时，编译出动态库
##pkgplugin_XXX的意思是安装到pkgplugindir目录
pkgplugin_LTLIBRARIES =	@plugin_tnt_shared_target@
## 定义target相关的LDFLAGS
## ha_ntse.la是条件编译的，用rpath指定输出路径
ha_tnt_la_LDFLAGS =	-module -rpath $(pkgplugindir)
## 定义target相关的CXXFLAGS
ha_tnt_la_CXXFLAGS	=	$(AM_CFLAGS) $(AM_CXXFLAGS) -DMYSQL_DYNAMIC_PLUGIN
## 定义target相关的CFLAGS
ha_tnt_la_CFLAGS	=	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
## 定义target的源代码
ha_tnt_la_SOURCES	=	$(tnt_sources)


EXTRA_LIBRARIES =	libtnt.a  libtntnokv.a
noinst_LIBRARIES =	@plugin_tnt_static_target@

libtntnokv_a_CXXFLAGS =    $(AM_CFLAGS) $(AM_CXXFLAGS) @DEFS@
libtntnokv_a_CFLAGS =      $(AM_CFLAGS) @DEFS@
libtntnokv_a_SOURCES=      $(tnt_sources)

libtnt_a_CXXFLAGS =	$(AM_CFLAGS) $(AM_CXXFLAGS) @keyvalue_DEFS@ @DEFS@
libtnt_a_CFLAGS =	$(AM_CFLAGS) @keyvalue_DEFS@ @DEFS@
libtnt_a_SOURCES	=	$(tnt_sources)

bin_PROGRAMS = tnt_restore tnt_trace_dump tnt_upgrade
tnt_restore_SOURCES = src/restore.cpp
tnt_trace_dump_SOURCES = src/trace_dump.cpp
tnt_upgrade_SOURCES = src/upgrade.cpp
tnt_restore_LDADD = libtnt.a
tnt_trace_dump_LDADD = libtnt.a \
                        $(LIBMYSQLCLIENT_LA) \
                        $(top_builddir)/mysys/libmysys.a
tnt_upgrade_LDADD = libtnt.a

if THREAD_SAFE_CLIENT
LIBMYSQLCLIENT_LA = $(top_builddir)/libmysql_r/libmysqlclient_r.la
else
LIBMYSQLCLIENT_LA =	$(top_builddir)/libmysql/libmysqlclient.la
endif

EXTRA_DIST =	CMakeLists.txt plug.in
# Don't update the files from bitkeeper
%::SCCS/s.%
