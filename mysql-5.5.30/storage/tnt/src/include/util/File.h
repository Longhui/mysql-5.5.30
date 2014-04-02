/**
 * 文件操作
 *
 * @author 汪源(wangyuan@corp.netease.com, wy@163.org)
 */

#ifndef _NTSE_FILE_H_
#define _NTSE_FILE_H_

#include <list>
#include <string>
#include "misc/Global.h"
#ifdef WIN32
#include <Windows.h>
#include "util/Sync.h"
#endif
#include <assert.h>

using namespace std;


typedef unsigned long aio_context_t;
struct iocb;
struct io_event;


namespace ntse {
/** 用于详细判断某文件是否需要操作的回调函数
 * @param path 文件路径
 * @param name 文件名
 * @param isDir 是否为目录
 * @return 是否需要操作
 */
typedef bool (*FILES_CALLBACK_FN)(const char *path, const char *name, bool isDir);

/** 文件
 * File类提供的接口类似于Java中的File类。在进行任何操作之前，
 * 首先要创建一个File对象。创建File对象时并不会打开文件，也不会检查
 * 文件是否存在。创建File对象之后，再使用open/create接口来打开或创建
 * 对应的文件。
 *
 * 本类提供的大部分操作的返回值都是u64类型，表示指定的操作的完成码，
 * 完成码为E_NO_ERROR时表示操作成功，为其它值时，错误码由本类定义
 * 的E_XXX系列错误码和操作系统特有错误码两部分组成，可调用getNtseError
 * 或getOsError函数来分别得到这两部分的内容。
 *
 * 注意本类并不提供操作的同步控制功能，直接调用本类提供的接口进行并发读写操作
 * 可能产生错误的结果，调用者必须自己做好同步控制。
 */
class File {
public:
	/**
	 * 创建一个文件对象。
	 *
	 * @param path 文件路径，可以是绝对或相对路径
	 * @return 文件对象
	 */
	File(const char *path);

	/**
	 * 析构函数。如果文件被打开则会自动关闭
	 */
	~File();

	/**
	 * 返回文件路径
	 *
	 * @return 文件路径
	 */
	const char* getPath() const {
		return m_path;
	}

	/**
	 * 创建指定的文件。创建文件成功后文件被自动打开，不需要再调用open
	 * 即可进行读写操作。
	 *
	 * @param directIo 是否使用DIRECT_IO，即不需要操作系统缓存文件内容
	 * @param deleteOnClose 关闭时是否自动删除文件，创建临时文件时使用这一选项
	 * @return 是否成功，失败时可能是E_PERM_ERR, E_DISK_FULL, E_EXIST, E_OTHER等错误
	 */
	u64 create(bool directIo, bool deleteOnClose);

	/**
	 * 打开文件进行读写操作
	 *
	 * @param directIo 是否使用DIRECT_IO，即不需要操作系统缓存文件内容
	 * @return 是否成功，失败原因可能是E_PERM_ERR, E_NOT_EXIST, E_OTHER等错误
	 */
	u64 open(bool directIo);

	/**
	 * 关闭文件。如果文件没有被打开则不进行任何操作
	 *
	 * @return 是否成功，一般不会失败
	 */
	u64 close();

	/**
	 * 查看指定的文件是否存在
	 *
	 * @param exist OUT，函数操作成功时返回文件是否存在
	 * @return 操作完成码
	 */
	u64 isExist(bool *exist);

	/**
	 * 判断一个文件是否存在
	 *
	 * @return 文件是否存在，如果由于权限等原因无法判断文件是否存在，本函数会退出程序
	 */
	static bool isExist(const char *path) {
		bool exist = false;
		u64 code = File(path).isExist(&exist);
		assert_always(code == E_NO_ERROR);
		return exist;
	}

	/**
	 * 删除文件
	 * @pre 文件没有被打开
	 *
	 * @param timeoutS 超时时间，必须>=0，需要指定超时时间的原因是刚操作过的文件
	 *   可能短期内仍被操作系统、杀毒软件等使用而无法立即删除
	 *   0: 马上超时，不等待
	 *   >0: 以秒为单位的超时时间
	 * @return 是否成功，失败原因可能是E_IN_USE, E_NOT_EXIST, E_PERM_ERR等错误
	 */
	u64 remove(int timeoutS = 5);

	/**
	 * 移动文件
	 * @pre 调用者没有打开文件
	 * 
	 * @param newPath 文件的新路径
	 * @param overrideTarget 目标文件已经存在时是否覆盖
	 * @return 是否成功
	 */
	u64 move(const char *newPath, bool overrideTarget = false);

	/** 
	 * 从文件中读取数据
	 * @pre 已经调用open或create打开文件
	 *
	 * @param offset 要读取的数据起始位置在文件中的偏移量。如果文件用
	 *   DIRECT_IO模式打开，则offset一定要是磁盘扇区（通常为512)的整数倍
	 * @param size 要读取的数据量。如果文件用DIRECT_IO模式打开，
	 *   则size一定要是磁盘扇区（通常为512)的整数倍
	 * @param buffer 由调用者分配的用于保存读取数据的内存。如果文件用
	 *   DIRECT_IO模式打开，则buffer的地址一定要是磁盘扇区（通常为512)的整数倍
	 * @return 是否成功，失败原因通常是E_READ，或E_EOF
	 */
	u64 read(u64 offset, u32 size, void *buffer);

	/**
	 * 写入数据到文件中
	 * @pre 已经调用open或create打开文件
	 *
	 * @param offset 要写入的数据在文件中的起始偏移量。如果文件用
	 *   DIRECT_IO模式打开，则offset一定要是磁盘扇区（通常为512)的整数倍
	 * @param size 要写入的数据量。如果文件用DIRECT_IO模式打开，
	 *   则size一定要是磁盘扇区（通常为512)的整数倍
	 * @param buffer 要写入的数据内容。如果文件用
	 *   DIRECT_IO模式打开，则buffer的地址一定要是磁盘扇区（通常为512)的整数倍
	 * @return 是否成功，失败原因通常是E_WRITE
	 */
	u64 write(u64 offset, u32 size, const void *buffer);


	/**
	 * 保证文件的所有数据都已经被写出到磁盘中
	 * @pre 已经调用open或create打开文件
	 *
	 * @return 是否成功，失败原因通常是E_WRITE
	 */
	u64 sync();

	/**
	 * 得到文件的大小
	 * @pre 已经调用open或create打开文件
	 * 
	 * @param size 输出参数，文件大小
	 * @return 成功与否
	 */
	u64 getSize(u64 *size);

	/**
	 * 设置文件的大小。通常用来扩展一个文件
	 * @pre 已经调用open或create打开文件
	 * 
	 * @return 是否成功
	 */
	u64 setSize(u64 size);

	/**
	 * 创建目录，可递归创建多级目录
	 *
	 * @return 成功与否
	 */
	u64 mkdir();

	/**
	 * 删除目录
	 *
	 * @param recursive 若为true则递归删除目录下的所有数据，
	 *  若为false则只有在空目录时才删除
	 * @return 成功与否，若recursive为false且要删除的目录为空时返回E_NOT_EMPTY
	 */
	u64 rmdir(bool recursive);

	/**
	 * 枚举目录下的所有文件
	 *
	 * @param files 输出参数，用于存储文件路径，链表中的原有数据会保留不会被清空
	 * @param includeDirs 是否也输出子目录
	 * @return 成功与否
	 */
	u64 listFiles(list<string> *files, bool includeDirs);

	/**
	 * 判断当前文件是否是一个目录
	 *
	 * @param isDir 输出参数，是否为目录
	 * @return 成功与否
	 */
	u64 isDirectory(bool *isDir);

	/**
	 * 判断当前文件是否是一个空目录
	 *
	 * @param isEmptyDir 输出参数，是否为空目录
	 * @return 成功与否
	 */
	u64 isEmptyDirectory(bool *isEmptyDir);

	/**
	 * 文件是否以directIo模式打开
	 * 
	 * @return directIo方式打开返回true
	 */
	bool isDirectIo() const {
		return m_directIo;
	}

	/**
	 * 拷贝文件，只能用于拷贝文件不能拷贝目录
	 *
	 * @param destPath 目标文件路径
	 * @param srcPath 源文件路径
	 * @param overrideExist 目标文件存在时，是否覆盖之
	 * @return 成功与否
	 */
	static u64 copyFile(const char *destPath, const char *srcPath, bool overrideExist);

	/**
	 * 递归拷贝目录
	 *
	 * @param destDir 目标目录，可以存在，也可以不存在
	 * @param srcDir 源目录
	 * @param overrideExist 目标文件存在时，是否覆盖之
	 * @param filterFn 进一步判断是否需要拷贝的回调函数，若为NULL则拷贝所有文件
	 * @return 成功与否
	 */
	static u64 copyDir(const char *destDir, const char *srcDir, bool overrideExist, FILES_CALLBACK_FN filterFn = NULL);

	/**
	 * 返回一个表示错误号是什么意思的字符串
	 *
	 * @param code 错误号
	 * @return 表示错误号含义的字符串
	 */
	static const char* explainErrno(u64 code) {
		switch (getNtseError(code)) {
		case E_NO_ERROR:
			return "no error";
		case E_NOT_EXIST:
			return "file not exist";
		case E_PERM_ERR:
			return "permission denied";
		case E_DISK_FULL:
			return "disk is full";
		case E_EXIST:
			return "file already exist";
		case E_IN_USE:
			return "file is in use";
		case E_READ:
			return "read failed";
		case E_WRITE:
			return "write failed";
		case E_EOF:
			return "end of file exceeded";
		case E_AIO_ARGS_INVALID:
			return "aio args invalid";
		case E_AIO_DATA_INVALID:
			return "aio data invalid";
		case E_AIO_FILE_INVALID:
			return "aio file invalid";
		case E_AIO_INTERUPTED:
			return "aio interupted";
		case E_AIO_KERNEL_RESOUCE_NOT_ENOUGH:
			return "aio kernel resouce not enough";
		case E_AIO_RESOURCE_NOT_ENOUGH:
			return "aio resource not enough";
		case E_AIO_SYSTEM_NOT_SUPPORT:
			return "system not support aio";
		default:
			return "other reasons";
		}
	}

	/**
	 * 返回错误码中的NTSE定义的错误码部分
	 *
	 * @param code 操作返回的错误码
	 * @return NTSE定义的错误码部分，可与本类中的E_NOT_EXIST等比较
	 */
	static u32 getNtseError(u64 code) {
		return (u32)code;
	}

	/**
	 * 返回错误码中的操作系统错误码部分
	 *
	 * @param code 操作返回的错误码
	 * @return 操作系统错误码部分
	 */
	static u32 getOsError(u64 code) {
		return (u32)(code >> 32);
	}


public:
	/************************************************************************
	 * 错误码
	 ***********************************************************************/
	static const u64 E_NO_ERROR = 0;	/** 没有发生错误 */
	static const u32 E_NOT_EXIST = 1;	/** 文件不存在 */
	static const u32 E_PERM_ERR = 2;	/** 没有足够的权限 */
	static const u32 E_DISK_FULL = 3;	/** 磁盘已满 */
	static const u32 E_EXIST = 4;		/** 文件已经存在 */
	static const u32 E_IN_USE = 5;		/** 文件（被其它程序）使用中 */
	static const u32 E_EOF = 6;			/** 读取或写入操作时指定的偏移量超出文件大小 */
	static const u32 E_READ = 7;		/** 读数据出错 */
	static const u32 E_WRITE = 8;		/** 写数据出错 */
	static const u32 E_NOT_EMPTY = 9;	/** 目录非空 */
	static const u32 E_OTHER = 10;		/** 其它错误 */
	static const u32 E_AIO_RESOURCE_NOT_ENOUGH = 11;			/** AIO所需资源不够 */
	static const u32 E_AIO_DATA_INVALID = 12;					/** AIO数据结构有误 */
	static const u32 E_AIO_ARGS_INVALID = 13;					/** AIO传入参数有误 */
	static const u32 E_AIO_KERNEL_RESOUCE_NOT_ENOUGH = 14;	/** AIO所需内核资源不够 */
	static const u32 E_AIO_SYSTEM_NOT_SUPPORT = 15;			/** 系统不支持AIO */
	static const u32 E_AIO_FILE_INVALID = 16;					/** AIO iocb结构体中传出的文件有误 */
	static const u32 E_AIO_INTERUPTED = 17;					/** AIO被打断 */
private:
	/**
	 * 执行真正的删除文件操作
	 *
	 * @return 综合错误码
	 */
	u64 doRemove();

	/**
	 * 根据操作系统错误码生成综合错误码
	 *
	 * @param osErrno 操作系统错误码
	 * @param readWrite 1表示为读操作，-1表示写操作，0表示其它操作
	 * @return 综合错误码
	 */
	static u64 translateError(u32 osErrno, int readWrite);

	/**
	 * 非递归的创建一个目录
	 *
	 * @path 要创建的目录
	 * @return 成功与否
	 */
	static u64 mkdirNonRecursive(const char *path);

	/**
	 * 删除一个空目录
	 *
	 * @param path 要删除的目录
	 * @return 成功与否
	 */
	static u64 removeEmptyDirectory(const char *path);

	/**
	 * 从文件路径中提取文件名
	 *
	 * @param path 文件路径
	 * @return 文件名
	 */
	static string getNameFromPath(const string &path);

private:
	char*	m_path;				/** 文件路径 */
	bool	m_opened;			/** 是否打开了 */
	s64		m_size;				/** 文件大小 */
#ifdef WIN32
	HANDLE	m_file;				/** 操作系统文件句柄 */
	Mutex	m_positionMutex;	/** 防止并发修改读写位置 */
#else
	int		m_file;				/** 操作系统文件句柄 */
	bool	m_deleteOnClose;	/** 关闭时是否自动删除 */
#endif
	bool	m_directIo;			/** 是否为O_DIRECT模式 */

	friend class AioArray;
};

enum AioOpType {
	AIO_READ,
	AIO_WRITE
};

class AioSlot {
public:
#ifndef  WIN32
	AioSlot();
	~AioSlot();
#endif
	u64             m_indexNum;             /** 当前所属aio槽 */
	bool            m_isReserved;           /** 是否reserved */
	u64             m_len;                  /** 操作长度 */
	byte            *m_buffer;              /** 操作内容 */
	AioOpType       m_opType;               /** 操作类型 */
	u64             m_offset;               /** 操作偏移量 */
	File            *m_file;                /** 文件句柄 */
#ifndef WIN32
	struct iocb     *m_control;              /** AIO 结构体 */
#endif
	void			*m_data;				/** 用于传输数据 */			

	friend class AioArray;
};


class AioArray {
public:
#ifndef WIN32
	/** 
	 * 构造函数，创建一个异步IO队列
	 */
	AioArray();

	/** 
	 * 析构函数
	 */
	~AioArray();

	/** 
	 * 初始化异步IO队列 
	 * @return		综合错误码
	 */
	u64 aioInit();

	/** 
	 * 销毁异步IO队列
	 * @return		综合错误码
	 */
	u64 aioDeInit();

	/** 
	 * 选择并占用异步IO队列中的一个异步IO槽
	 *
	 * @param type		异步IO操作类型
	 * @param file		异步IO操作的文件
	 * @param buf		异步IO操作的缓存
	 * @param offset	异步IO操作的文件偏移量
	 * @param size		异步IO操作的大小
	 * @param data		异步IO操作携带的额外信息
	 * @return			占用的异步IO槽
	 */
	AioSlot* aioReserveSlot(AioOpType type, File *file, void *buf, u64 offset, u32 size, void *data);

	/** 
	 * 提交一组异步IO请求
	 *
	 * @param number		异步IO操作数量
	 * @return				综合错误码
	 */
	u64 aioDispatchGroup(u32 number);

	/** 
	 * 提交一个异步IO请求
	 *
	 * @param slot 
	 * @return			综合错误码
	 */
	u64 aioDispatch(AioSlot *slot);

	/** 
	 * 释放一个异步IO槽，变为空闲状态
	 *
	 * @param slot 
	 */
	void aioFreeSlot(AioSlot *slot);

	/** 
	 * 等待异步IO完成
	 *
	 * @param minRequestCnt		等待最少的完成的异步IO请求数 
	 * @param numIoComplete		OUT 实际这次完成的AIO的数目
	 * @return					综合错误码
	 */
	u64  aioWaitFinish(u32 minRequestCnt, u32 *numIoComplete);


	/** 
	 * 获取空闲异步IO槽的数目
	 *
	 * @return		空闲槽的个数 
	 */
	u32  getReservedSlotNum();

	/** 
	 * 从异步IO返回的异步IO事件中获取异步IO槽信息
	 *
	 * @param index 返回的异步IO事件数组的下标
	 * @return		异步IO槽
	 */
	AioSlot* getSlotFromEvent(u32 index);

	/** 
	 * 从异步IO返回的异步IO事件中获取额外的信息
	 *
	 * @param index 返回的异步IO事件数组的下标
	 * @return		额外信息的地址
	 */
	void* getDataFromEvent(u32 index);



	/** 
	 * 填充读异步结构体， 封装系统调用
	 * @pre 已经调用open或create打开文件，并使用DirectIO方式
	 *
	 * @param iocb		异步IO结构体
	 * @param offset	要读取的数据起始位置在文件中的偏移量。如果文件用
	 *					 DIRECT_IO模式打开，则offset一定要是磁盘扇区（通常为512)的整数倍
	 * @param size		要读取的数据量。如果文件用DIRECT_IO模式打开，
	 *					 则size一定要是磁盘扇区（通常为512)的整数倍
	 * @param buffer	由调用者分配的用于保存读取数据的内存。如果文件用
	 *					 DIRECT_IO模式打开，则buffer的地址一定要是磁盘扇区（通常为512)的整数倍
	 */
	static void fillAioHandlerRead(struct iocb *iocb, File *file, const void *buffer, u32 size, u64 offset);


	/**
	 * 填充写异步结构体
	 * @pre 已经调用open或create打开文件
	 *
	 * @param iocb		异步IO结构体
	 * @param offset	要写入的数据在文件中的起始偏移量。如果文件用
	 *					 DIRECT_IO模式打开，则offset一定要是磁盘扇区（通常为512)的整数倍
	 * @param size		要写入的数据量。如果文件用DIRECT_IO模式打开，
	 *					 则size一定要是磁盘扇区（通常为512)的整数倍
	 * @param buffer	要写入的数据内容。如果文件用
	 *					 DIRECT_IO模式打开，则buffer的地址一定要是磁盘扇区（通常为512)的整数倍
	 */
	static void fillAioHandlerWrite(struct iocb *iocb, File *file, const void *buffer, u32 size, u64 offset);

private:

	/** 
	 * 填充异步读请求的 IOCB 结构体
	 * @param iodb		填充的结构体
	 * @param fd		文件句柄号
	 * @param buf		读操作的缓冲区
	 * @param count		读操作的大小
	 * @param offset	读操作的文件偏移量
	 */
	static void io_prep_pread(struct iocb *iocb, int fd, const void *buf, size_t count, long long offset);

	/** 
	 * 填充异步写请求的 IOCB 结构体
	 * @param iodb		填充的结构体
	 * @param fd		文件句柄号
	 * @param buf		写操作的缓冲区
	 * @param count		写操作的大小
	 * @param offset	写操作的文件偏移量
	 */
	static void io_prep_pwrite(struct iocb *iocb, int fd, const void *buf, size_t count, long long offset);

	
	/** 
	 * 初始化异步IO上下文，封装系统调用
	 *
	 * @param nr		异步IO的并发容量
	 * @param ctxp		异步IO上下文
	 * @return			操作系统错误码
	 */
	static int io_setup(unsigned nr, aio_context_t *ctxp);


	/** 
	 * 销毁异步IO上下文，封装系统调用
	 * @pre 已调用aio_init初始化过异步IO队列
	 *
	 * @param ctx		异步IO上下文
	 * @return			操作系统错误码
	 */
	static int io_destroy(aio_context_t ctx);


	/** 
	 * 提交异步IO请求， 封装系统调用
	 * @param ctx		异步IO上下文
	 * @param nr		异步IO的并发量
	 * @param iodbpp	异步IO结构体指针数组
	 * @return			操作系统错误码
	 */
	static int io_submit(aio_context_t ctx, long nr,  struct iocb **iocbpp);

	/** 
	 * 等待异步IO请求完成， 封装系统调用
	 * @param ctx		异步IO上下文
	 * @param min_nr	等待完成的异步IO请求最小值
	 * @param max_nr	等待完成的异步IO请求最大值
	 * @param events	异步IO返回结果的数组
	 * @param timeout	等待异步IO完成的超时时间
	 * @return 操作系统错误码
	 */
	static int io_getevents(aio_context_t ctx, long min_nr, long max_nr, struct io_event *events, struct timespec *timeout);

#endif

public:
	static const u32        AIO_BATCH_SIZE = 256;           /** aio队列长度 */

private:
#ifdef WIN32

#else
	aio_context_t           m_ctx;                          /** aio 上下文 */
	struct io_event         *m_aioEvents;					/** aio 事件数组 */
	struct iocb				**m_cbs;						/** aio 对象指针数组,用于成组提交aio请求 */
#endif
	AioSlot                 *m_slots;						/** aio 槽数组 */
	u32                     m_numReserved;                  /** aio 队列空闲长度 */
};

}

#endif
