# Copyright (C) 2006 MySQL AB
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

INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
INCLUDE(CheckCSourceRuns)


SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -DNTSE_TRACE -DNTSE_VERIFY_EX -DNTSE_PROFILE -DWITH_NTSE_BINLOG")
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX -DNTSE_TRACE -DNTSE_VERIFY_EX -DNTSE_PROFILE -DWITH_NTSE_BINLOG")

IF(MSVC)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHsc")
ENDIF()

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/include 
		    ${CMAKE_SOURCE_DIR}/zlib
                    ${CMAKE_SOURCE_DIR}/storage/tnt/src/include
                    ${CMAKE_SOURCE_DIR}/sql
                    ${CMAKE_SOURCE_DIR}/client
                    ${CMAKE_SOURCE_DIR}/libmysql                    
                    ${CMAKE_SOURCE_DIR}/regex
                    ${CMAKE_SOURCE_DIR}/extra/yassl/include)

# 保持按文件名排序

SET(TNT_SOURCES 
  src/ha_tnt.h
  src/ntse_handler.h
  src/RowCache.h
  src/THDInfoBase.h
  src/Cmd.cpp
  src/InfoSchema.cpp
  src/ha_tnt.cpp
  src/ntse_handler.cpp
  src/RowCache.cpp
  src/THDInfoBase.cpp
  src/include/api/Database.h
  src/include/api/IdxPreAlter.h
  src/include/api/LobDefraggler.h
  src/include/api/Table.h
  src/include/api/TblArgAlter.h
  src/include/api/TblMnt.h
  src/include/api/TNTTblMnt.h
  src/include/api/TNTDatabase.h
  src/include/api/TNTTable.h
  src/include/api/TNTTblScan.h
  src/include/api/TNTTblArgAlter.h
  src/include/api/Transaction.h
  src/include/btree/Index.h
  src/include/btree/IndexBLinkTree.h
  src/include/btree/IndexBLinkTreeManager.h
  src/include/btree/IndexBPTree.h
  src/include/btree/IndexBPTreesManager.h
  src/include/btree/IndexCommon.h
  src/include/btree/IndexKey.h
  src/include/btree/IndexLog.h
  src/include/btree/IndexPage.h
  src/include/btree/MIndex.h
  src/include/btree/MIndexKey.h
  src/include/btree/MIndexPage.h
  src/include/btree/OuterSorter.h
  src/include/btree/TNTIndex.h
  src/include/compress/CreateDicHelper.h
  src/include/compress/dastrie.h
  src/include/compress/RCMSampleTbl.h
  src/include/compress/RowCompress.h
  src/include/compress/RowCompressCfg.h
  src/include/compress/RowCompressCoder.h
  src/include/compress/SmplTrie.h
  src/include/heap/Heap.h
  src/include/heap/FixedLengthRecordHeap.h
  src/include/heap/VariableLengthRecordHeap.h
  src/include/heap/HashIndex.h
  src/include/heap/MHeap.h
  src/include/heap/MHeapRecord.h
  src/include/heap/TxnRecManager.h
  src/include/heap/VersionPool.h
  src/include/lob/lzo/config1x.h
  src/include/lob/lzo/lzo1x.h
  src/include/lob/lzo/lzo_conf.h
  src/include/lob/lzo/lzo_dict.h
  src/include/lob/lzo/lzo_ptr.h
  src/include/lob/lzo/lzoconf.h
  src/include/lob/lzo/lzodefs.h
  src/include/lob/lzo/miniacc.h
  src/include/lob/BigLob.h
  src/include/lob/Lob.h
  src/include/lob/LobIndex.h
  src/include/lob/Lzo.h
  src/include/lob/SmallLob.h
  src/include/misc/Callbacks.h
  src/include/misc/ColumnGroupParser.h
  src/include/misc/Decimal.h
  src/include/misc/BgCustomThread.h
  src/include/misc/Buffer.h
  src/include/misc/CommonMemPool.h
  src/include/misc/Config.h
  src/include/misc/ControlFile.h
  src/include/misc/DLDLockTable.h
  src/include/misc/DoubleChecker.h
  src/include/misc/Global.h
  src/include/misc/GlobalFactory.h
  src/include/misc/KVParser.h
  src/include/misc/LockManager.h
  src/include/misc/MemCtx.h
  src/include/misc/OptimizeThread.h
  src/include/misc/Parser.h
  src/include/misc/ParFileParser.h
  src/include/misc/Profile.h
  src/include/misc/Record.h
  src/include/misc/RecordHelper.h
  src/include/misc/ResourcePool.h
  src/include/misc/Sample.h
  src/include/misc/Session.h
  src/include/misc/Syslog.h
  src/include/misc/TableDef.h
  src/include/misc/TNTControlFile.h
  src/include/misc/TNTIMPageManager.h
  src/include/misc/Trace.h
  src/include/misc/Txnlog.h
  src/include/misc/Verify.h
  src/include/mms/Mms.h
  src/include/mms/MmsHeap.h
  src/include/mms/MmsMap.h
  src/include/mms/MmsPage.h
  src/include/rec/Records.h
  src/include/rec/MRecords.h
  src/include/strings/CtypeCommon.h
  src/include/trx/TLock.h
  src/include/trx/TrxXA.h
  src/include/trx/TNTTransaction.h
  src/include/util/Array.h
  src/include/util/Bitmap.h
  src/include/util/DList.h
  src/include/util/File.h
  src/include/util/Hash.h
  src/include/util/NumberCompress.h
  src/include/util/ObjectPool.h
  src/include/util/PagePool.h 
  src/include/util/Portable.h
  src/include/util/Queue.h
  src/include/util/SmartPtr.h
  src/include/util/Stream.h
  src/include/util/Sync.h
  src/include/util/System.h
  src/include/util/Thread.h
  src/keyvalue/gen-cpp/KV.h
  src/keyvalue/gen-cpp/KV_constants.h
  src/keyvalue/gen-cpp/KV_types.h
  src/keyvalue/KeyValueHelper.h
  src/keyvalue/KeyValueHandler.h
  src/keyvalue/KeyValueServer.h
  src/keyvalue/KeyValueEventHandler.h
  src/api/Database.cpp
  src/api/IdxPreAlter.cpp
  src/api/LobDefraggler.cpp
  src/api/Table.cpp
  src/api/TblArgAlter.cpp
  src/api/TblLog.cpp
  src/api/TblMisc.cpp
  src/api/TblMnt.cpp
  src/api/TNTTblMnt.cpp
  src/api/TNTDatabase.cpp
  src/api/TNTTable.cpp
  src/api/TNTTableLog.cpp
  src/api/TNTTblScan.cpp
  src/api/TNTTblArgAlter.cpp
  src/api/Transaction.cpp
  src/btree/Index.cpp
  src/btree/IndexBLinkTree.cpp
  src/btree/IndexBLinkTreeManager.cpp
  src/btree/IndexBPTree.cpp
  src/btree/IndexBPTreesManager.cpp
  src/btree/IndexKey.cpp
  src/btree/IndexLog.cpp
  src/btree/IndexPage.cpp
  src/btree/MIndex.cpp
  src/btree/MIndexKey.cpp
  src/btree/MIndexPage.cpp
  src/btree/OuterSorter.cpp
  src/btree/TNTIndex.cpp
  src/compress/RCMSampleTbl.cpp
  src/compress/RowCompress.cpp
  src/compress/RowCompressCfg.cpp
  src/compress/SmplTrie.cpp
  src/heap/Heap.cpp
  src/heap/FixedLengthRecordHeap.cpp
  src/heap/VariableLengthRecordHeap.cpp
  src/heap/MHeap.cpp
  src/heap/HashIndex.cpp
  src/heap/MHeapRecord.cpp
  src/heap/TxnRecManager.cpp
  src/heap/VersionPool.cpp
  src/lob/lzo/lzo1x_1k.c
  src/lob/lzo/lzo1x_d1.c
  src/lob/lzo/lzo_init.c
  src/lob/lzo/lzo_ptr.c
  src/lob/BigLob.cpp
  src/lob/Lob.cpp
  src/lob/LobIndex.cpp
  src/lob/Lzo.cpp
  src/lob/SmallLob.cpp
  src/misc/BgCustomThread.cpp
  src/misc/Buffer.cpp
  src/misc/ColumnGroupParser.cpp
  src/misc/CommonMemPool.cpp
  src/misc/ControlFile.cpp
  src/misc/Decimal.cpp
  src/misc/DLDLockTable.cpp
  src/misc/Global.cpp
  src/misc/KVParser.cpp
  src/misc/MemCtx.cpp
  src/misc/OptimizeThread.cpp
  src/misc/Parser.cpp
  src/misc/ParFileParser.cpp
  src/misc/Record.cpp
  src/misc/RecordHelper.cpp
  src/misc/ResourcePool.cpp
  src/misc/Sample.cpp
  src/misc/Session.cpp
  src/misc/Syslog.cpp
  src/misc/TableDef.cpp
  src/misc/TNTControlFile.cpp
  src/misc/TNTIMPageManager.cpp
  src/misc/Trace.cpp
  src/misc/TxnLog.cpp
  src/misc/Verify.cpp
  src/misc/Profile.cpp
  src/mms/MmsHeap.cpp
  src/mms/MmsMap.cpp
  src/mms/MmsPage.cpp
  src/mms/MmsTable.cpp
  src/mms/MmsUtil.cpp
  src/rec/Records.cpp
  src/rec/MRecords.cpp
  src/rec/MRecordsLog.cpp
  src/rec/MRecordsRecovery.cpp
  src/strings/Collation.cpp
  src/strings/CtypeBin.cpp
  src/strings/CtypeGBK.cpp
  src/strings/CtypeLatin1.cpp
  src/strings/CtypeUTF8.cpp
  src/trx/TLock.cpp
  src/trx/TNTTransaction.cpp
  src/util/FileLin.cpp
  src/util/FileWin.cpp
  src/util/Hash.cpp
  src/util/Pool.cpp
  src/util/Stream.cpp
  src/util/Sync.cpp
  src/util/System.cpp
  src/util/Thread.cpp
  src/keyvalue/gen-cpp/KV.cpp
  src/keyvalue/gen-cpp/KV_constants.cpp
  src/keyvalue/gen-cpp/KV_types.cpp
  src/keyvalue/KeyValueServer.cpp
  src/keyvalue/KeyValueHandler.cpp
  src/keyvalue/KeyValueEventHandler.cpp)


IF(WITH_TNT)
  # Legacy option
  SET(WITH_TNT_STORAGE_ENGINE TRUE)
ENDIF()

MYSQL_ADD_EXECUTABLE(tnt_restore  src/restore.cpp)
TARGET_LINK_LIBRARIES(tnt_restore tnt)

MYSQL_ADD_EXECUTABLE(tnt_trace_dump  src/trace_dump.cpp)
TARGET_LINK_LIBRARIES(tnt_trace_dump tnt mysqlclient)

MYSQL_ADD_EXECUTABLE(tnt_upgrade  src/upgrade.cpp)
TARGET_LINK_LIBRARIES(tnt_upgrade tnt)

MYSQL_ADD_PLUGIN(tnt ${TNT_SOURCES} STORAGE_ENGINE 
  DEFAULT
  MODULE_OUTPUT_NAME ha_tnt)
