/**
 * �ļ�����
 *
 * @author ��Դ(wangyuan@corp.netease.com, wy@163.org)
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
/** ������ϸ�ж�ĳ�ļ��Ƿ���Ҫ�����Ļص�����
 * @param path �ļ�·��
 * @param name �ļ���
 * @param isDir �Ƿ�ΪĿ¼
 * @return �Ƿ���Ҫ����
 */
typedef bool (*FILES_CALLBACK_FN)(const char *path, const char *name, bool isDir);

/** �ļ�
 * File���ṩ�Ľӿ�������Java�е�File�ࡣ�ڽ����κβ���֮ǰ��
 * ����Ҫ����һ��File���󡣴���File����ʱ��������ļ���Ҳ������
 * �ļ��Ƿ���ڡ�����File����֮����ʹ��open/create�ӿ����򿪻򴴽�
 * ��Ӧ���ļ���
 *
 * �����ṩ�Ĵ󲿷ֲ����ķ���ֵ����u64���ͣ���ʾָ���Ĳ���������룬
 * �����ΪE_NO_ERRORʱ��ʾ�����ɹ���Ϊ����ֵʱ���������ɱ��ඨ��
 * ��E_XXXϵ�д�����Ͳ���ϵͳ���д�������������ɣ��ɵ���getNtseError
 * ��getOsError�������ֱ�õ��������ֵ����ݡ�
 *
 * ע�Ȿ�ಢ���ṩ������ͬ�����ƹ��ܣ�ֱ�ӵ��ñ����ṩ�Ľӿڽ��в�����д����
 * ���ܲ�������Ľ���������߱����Լ�����ͬ�����ơ�
 */
class File {
public:
	/**
	 * ����һ���ļ�����
	 *
	 * @param path �ļ�·���������Ǿ��Ի����·��
	 * @return �ļ�����
	 */
	File(const char *path);

	/**
	 * ��������������ļ���������Զ��ر�
	 */
	~File();

	/**
	 * �����ļ�·��
	 *
	 * @return �ļ�·��
	 */
	const char* getPath() const {
		return m_path;
	}

	/**
	 * ����ָ�����ļ��������ļ��ɹ����ļ����Զ��򿪣�����Ҫ�ٵ���open
	 * ���ɽ��ж�д������
	 *
	 * @param directIo �Ƿ�ʹ��DIRECT_IO��������Ҫ����ϵͳ�����ļ�����
	 * @param deleteOnClose �ر�ʱ�Ƿ��Զ�ɾ���ļ���������ʱ�ļ�ʱʹ����һѡ��
	 * @return �Ƿ�ɹ���ʧ��ʱ������E_PERM_ERR, E_DISK_FULL, E_EXIST, E_OTHER�ȴ���
	 */
	u64 create(bool directIo, bool deleteOnClose);

	/**
	 * ���ļ����ж�д����
	 *
	 * @param directIo �Ƿ�ʹ��DIRECT_IO��������Ҫ����ϵͳ�����ļ�����
	 * @return �Ƿ�ɹ���ʧ��ԭ�������E_PERM_ERR, E_NOT_EXIST, E_OTHER�ȴ���
	 */
	u64 open(bool directIo);

	/**
	 * �ر��ļ�������ļ�û�б����򲻽����κβ���
	 *
	 * @return �Ƿ�ɹ���һ�㲻��ʧ��
	 */
	u64 close();

	/**
	 * �鿴ָ�����ļ��Ƿ����
	 *
	 * @param exist OUT�����������ɹ�ʱ�����ļ��Ƿ����
	 * @return ���������
	 */
	u64 isExist(bool *exist);

	/**
	 * �ж�һ���ļ��Ƿ����
	 *
	 * @return �ļ��Ƿ���ڣ��������Ȩ�޵�ԭ���޷��ж��ļ��Ƿ���ڣ����������˳�����
	 */
	static bool isExist(const char *path) {
		bool exist = false;
		u64 code = File(path).isExist(&exist);
		assert_always(code == E_NO_ERROR);
		return exist;
	}

	/**
	 * ɾ���ļ�
	 * @pre �ļ�û�б���
	 *
	 * @param timeoutS ��ʱʱ�䣬����>=0����Ҫָ����ʱʱ���ԭ���Ǹղ��������ļ�
	 *   ���ܶ������Ա�����ϵͳ��ɱ�������ʹ�ö��޷�����ɾ��
	 *   0: ���ϳ�ʱ�����ȴ�
	 *   >0: ����Ϊ��λ�ĳ�ʱʱ��
	 * @return �Ƿ�ɹ���ʧ��ԭ�������E_IN_USE, E_NOT_EXIST, E_PERM_ERR�ȴ���
	 */
	u64 remove(int timeoutS = 5);

	/**
	 * �ƶ��ļ�
	 * @pre ������û�д��ļ�
	 * 
	 * @param newPath �ļ�����·��
	 * @param overrideTarget Ŀ���ļ��Ѿ�����ʱ�Ƿ񸲸�
	 * @return �Ƿ�ɹ�
	 */
	u64 move(const char *newPath, bool overrideTarget = false);

	/** 
	 * ���ļ��ж�ȡ����
	 * @pre �Ѿ�����open��create���ļ�
	 *
	 * @param offset Ҫ��ȡ��������ʼλ�����ļ��е�ƫ����������ļ���
	 *   DIRECT_IOģʽ�򿪣���offsetһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param size Ҫ��ȡ��������������ļ���DIRECT_IOģʽ�򿪣�
	 *   ��sizeһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param buffer �ɵ����߷�������ڱ����ȡ���ݵ��ڴ档����ļ���
	 *   DIRECT_IOģʽ�򿪣���buffer�ĵ�ַһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @return �Ƿ�ɹ���ʧ��ԭ��ͨ����E_READ����E_EOF
	 */
	u64 read(u64 offset, u32 size, void *buffer);

	/**
	 * д�����ݵ��ļ���
	 * @pre �Ѿ�����open��create���ļ�
	 *
	 * @param offset Ҫд����������ļ��е���ʼƫ����������ļ���
	 *   DIRECT_IOģʽ�򿪣���offsetһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param size Ҫд���������������ļ���DIRECT_IOģʽ�򿪣�
	 *   ��sizeһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param buffer Ҫд����������ݡ�����ļ���
	 *   DIRECT_IOģʽ�򿪣���buffer�ĵ�ַһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @return �Ƿ�ɹ���ʧ��ԭ��ͨ����E_WRITE
	 */
	u64 write(u64 offset, u32 size, const void *buffer);


	/**
	 * ��֤�ļ����������ݶ��Ѿ���д����������
	 * @pre �Ѿ�����open��create���ļ�
	 *
	 * @return �Ƿ�ɹ���ʧ��ԭ��ͨ����E_WRITE
	 */
	u64 sync();

	/**
	 * �õ��ļ��Ĵ�С
	 * @pre �Ѿ�����open��create���ļ�
	 * 
	 * @param size ����������ļ���С
	 * @return �ɹ����
	 */
	u64 getSize(u64 *size);

	/**
	 * �����ļ��Ĵ�С��ͨ��������չһ���ļ�
	 * @pre �Ѿ�����open��create���ļ�
	 * 
	 * @return �Ƿ�ɹ�
	 */
	u64 setSize(u64 size);

	/**
	 * ����Ŀ¼���ɵݹ鴴���༶Ŀ¼
	 *
	 * @return �ɹ����
	 */
	u64 mkdir();

	/**
	 * ɾ��Ŀ¼
	 *
	 * @param recursive ��Ϊtrue��ݹ�ɾ��Ŀ¼�µ��������ݣ�
	 *  ��Ϊfalse��ֻ���ڿ�Ŀ¼ʱ��ɾ��
	 * @return �ɹ������recursiveΪfalse��Ҫɾ����Ŀ¼Ϊ��ʱ����E_NOT_EMPTY
	 */
	u64 rmdir(bool recursive);

	/**
	 * ö��Ŀ¼�µ������ļ�
	 *
	 * @param files ������������ڴ洢�ļ�·���������е�ԭ�����ݻᱣ�����ᱻ���
	 * @param includeDirs �Ƿ�Ҳ�����Ŀ¼
	 * @return �ɹ����
	 */
	u64 listFiles(list<string> *files, bool includeDirs);

	/**
	 * �жϵ�ǰ�ļ��Ƿ���һ��Ŀ¼
	 *
	 * @param isDir ����������Ƿ�ΪĿ¼
	 * @return �ɹ����
	 */
	u64 isDirectory(bool *isDir);

	/**
	 * �жϵ�ǰ�ļ��Ƿ���һ����Ŀ¼
	 *
	 * @param isEmptyDir ����������Ƿ�Ϊ��Ŀ¼
	 * @return �ɹ����
	 */
	u64 isEmptyDirectory(bool *isEmptyDir);

	/**
	 * �ļ��Ƿ���directIoģʽ��
	 * 
	 * @return directIo��ʽ�򿪷���true
	 */
	bool isDirectIo() const {
		return m_directIo;
	}

	/**
	 * �����ļ���ֻ�����ڿ����ļ����ܿ���Ŀ¼
	 *
	 * @param destPath Ŀ���ļ�·��
	 * @param srcPath Դ�ļ�·��
	 * @param overrideExist Ŀ���ļ�����ʱ���Ƿ񸲸�֮
	 * @return �ɹ����
	 */
	static u64 copyFile(const char *destPath, const char *srcPath, bool overrideExist);

	/**
	 * �ݹ鿽��Ŀ¼
	 *
	 * @param destDir Ŀ��Ŀ¼�����Դ��ڣ�Ҳ���Բ�����
	 * @param srcDir ԴĿ¼
	 * @param overrideExist Ŀ���ļ�����ʱ���Ƿ񸲸�֮
	 * @param filterFn ��һ���ж��Ƿ���Ҫ�����Ļص���������ΪNULL�򿽱������ļ�
	 * @return �ɹ����
	 */
	static u64 copyDir(const char *destDir, const char *srcDir, bool overrideExist, FILES_CALLBACK_FN filterFn = NULL);

	/**
	 * ����һ����ʾ�������ʲô��˼���ַ���
	 *
	 * @param code �����
	 * @return ��ʾ����ź�����ַ���
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
	 * ���ش������е�NTSE����Ĵ����벿��
	 *
	 * @param code �������صĴ�����
	 * @return NTSE����Ĵ����벿�֣����뱾���е�E_NOT_EXIST�ȱȽ�
	 */
	static u32 getNtseError(u64 code) {
		return (u32)code;
	}

	/**
	 * ���ش������еĲ���ϵͳ�����벿��
	 *
	 * @param code �������صĴ�����
	 * @return ����ϵͳ�����벿��
	 */
	static u32 getOsError(u64 code) {
		return (u32)(code >> 32);
	}


public:
	/************************************************************************
	 * ������
	 ***********************************************************************/
	static const u64 E_NO_ERROR = 0;	/** û�з������� */
	static const u32 E_NOT_EXIST = 1;	/** �ļ������� */
	static const u32 E_PERM_ERR = 2;	/** û���㹻��Ȩ�� */
	static const u32 E_DISK_FULL = 3;	/** �������� */
	static const u32 E_EXIST = 4;		/** �ļ��Ѿ����� */
	static const u32 E_IN_USE = 5;		/** �ļ�������������ʹ���� */
	static const u32 E_EOF = 6;			/** ��ȡ��д�����ʱָ����ƫ���������ļ���С */
	static const u32 E_READ = 7;		/** �����ݳ��� */
	static const u32 E_WRITE = 8;		/** д���ݳ��� */
	static const u32 E_NOT_EMPTY = 9;	/** Ŀ¼�ǿ� */
	static const u32 E_OTHER = 10;		/** �������� */
	static const u32 E_AIO_RESOURCE_NOT_ENOUGH = 11;			/** AIO������Դ���� */
	static const u32 E_AIO_DATA_INVALID = 12;					/** AIO���ݽṹ���� */
	static const u32 E_AIO_ARGS_INVALID = 13;					/** AIO����������� */
	static const u32 E_AIO_KERNEL_RESOUCE_NOT_ENOUGH = 14;	/** AIO�����ں���Դ���� */
	static const u32 E_AIO_SYSTEM_NOT_SUPPORT = 15;			/** ϵͳ��֧��AIO */
	static const u32 E_AIO_FILE_INVALID = 16;					/** AIO iocb�ṹ���д������ļ����� */
	static const u32 E_AIO_INTERUPTED = 17;					/** AIO����� */
private:
	/**
	 * ִ��������ɾ���ļ�����
	 *
	 * @return �ۺϴ�����
	 */
	u64 doRemove();

	/**
	 * ���ݲ���ϵͳ�����������ۺϴ�����
	 *
	 * @param osErrno ����ϵͳ������
	 * @param readWrite 1��ʾΪ��������-1��ʾд������0��ʾ��������
	 * @return �ۺϴ�����
	 */
	static u64 translateError(u32 osErrno, int readWrite);

	/**
	 * �ǵݹ�Ĵ���һ��Ŀ¼
	 *
	 * @path Ҫ������Ŀ¼
	 * @return �ɹ����
	 */
	static u64 mkdirNonRecursive(const char *path);

	/**
	 * ɾ��һ����Ŀ¼
	 *
	 * @param path Ҫɾ����Ŀ¼
	 * @return �ɹ����
	 */
	static u64 removeEmptyDirectory(const char *path);

	/**
	 * ���ļ�·������ȡ�ļ���
	 *
	 * @param path �ļ�·��
	 * @return �ļ���
	 */
	static string getNameFromPath(const string &path);

private:
	char*	m_path;				/** �ļ�·�� */
	bool	m_opened;			/** �Ƿ���� */
	s64		m_size;				/** �ļ���С */
#ifdef WIN32
	HANDLE	m_file;				/** ����ϵͳ�ļ���� */
	Mutex	m_positionMutex;	/** ��ֹ�����޸Ķ�дλ�� */
#else
	int		m_file;				/** ����ϵͳ�ļ���� */
	bool	m_deleteOnClose;	/** �ر�ʱ�Ƿ��Զ�ɾ�� */
#endif
	bool	m_directIo;			/** �Ƿ�ΪO_DIRECTģʽ */

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
	u64             m_indexNum;             /** ��ǰ����aio�� */
	bool            m_isReserved;           /** �Ƿ�reserved */
	u64             m_len;                  /** �������� */
	byte            *m_buffer;              /** �������� */
	AioOpType       m_opType;               /** �������� */
	u64             m_offset;               /** ����ƫ���� */
	File            *m_file;                /** �ļ���� */
#ifndef WIN32
	struct iocb     *m_control;              /** AIO �ṹ�� */
#endif
	void			*m_data;				/** ���ڴ������� */			

	friend class AioArray;
};


class AioArray {
public:
#ifndef WIN32
	/** 
	 * ���캯��������һ���첽IO����
	 */
	AioArray();

	/** 
	 * ��������
	 */
	~AioArray();

	/** 
	 * ��ʼ���첽IO���� 
	 * @return		�ۺϴ�����
	 */
	u64 aioInit();

	/** 
	 * �����첽IO����
	 * @return		�ۺϴ�����
	 */
	u64 aioDeInit();

	/** 
	 * ѡ��ռ���첽IO�����е�һ���첽IO��
	 *
	 * @param type		�첽IO��������
	 * @param file		�첽IO�������ļ�
	 * @param buf		�첽IO�����Ļ���
	 * @param offset	�첽IO�������ļ�ƫ����
	 * @param size		�첽IO�����Ĵ�С
	 * @param data		�첽IO����Я���Ķ�����Ϣ
	 * @return			ռ�õ��첽IO��
	 */
	AioSlot* aioReserveSlot(AioOpType type, File *file, void *buf, u64 offset, u32 size, void *data);

	/** 
	 * �ύһ���첽IO����
	 *
	 * @param number		�첽IO��������
	 * @return				�ۺϴ�����
	 */
	u64 aioDispatchGroup(u32 number);

	/** 
	 * �ύһ���첽IO����
	 *
	 * @param slot 
	 * @return			�ۺϴ�����
	 */
	u64 aioDispatch(AioSlot *slot);

	/** 
	 * �ͷ�һ���첽IO�ۣ���Ϊ����״̬
	 *
	 * @param slot 
	 */
	void aioFreeSlot(AioSlot *slot);

	/** 
	 * �ȴ��첽IO���
	 *
	 * @param minRequestCnt		�ȴ����ٵ���ɵ��첽IO������ 
	 * @param numIoComplete		OUT ʵ�������ɵ�AIO����Ŀ
	 * @return					�ۺϴ�����
	 */
	u64  aioWaitFinish(u32 minRequestCnt, u32 *numIoComplete);


	/** 
	 * ��ȡ�����첽IO�۵���Ŀ
	 *
	 * @return		���в۵ĸ��� 
	 */
	u32  getReservedSlotNum();

	/** 
	 * ���첽IO���ص��첽IO�¼��л�ȡ�첽IO����Ϣ
	 *
	 * @param index ���ص��첽IO�¼�������±�
	 * @return		�첽IO��
	 */
	AioSlot* getSlotFromEvent(u32 index);

	/** 
	 * ���첽IO���ص��첽IO�¼��л�ȡ�������Ϣ
	 *
	 * @param index ���ص��첽IO�¼�������±�
	 * @return		������Ϣ�ĵ�ַ
	 */
	void* getDataFromEvent(u32 index);



	/** 
	 * �����첽�ṹ�壬 ��װϵͳ����
	 * @pre �Ѿ�����open��create���ļ�����ʹ��DirectIO��ʽ
	 *
	 * @param iocb		�첽IO�ṹ��
	 * @param offset	Ҫ��ȡ��������ʼλ�����ļ��е�ƫ����������ļ���
	 *					 DIRECT_IOģʽ�򿪣���offsetһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param size		Ҫ��ȡ��������������ļ���DIRECT_IOģʽ�򿪣�
	 *					 ��sizeһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param buffer	�ɵ����߷�������ڱ����ȡ���ݵ��ڴ档����ļ���
	 *					 DIRECT_IOģʽ�򿪣���buffer�ĵ�ַһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 */
	static void fillAioHandlerRead(struct iocb *iocb, File *file, const void *buffer, u32 size, u64 offset);


	/**
	 * ���д�첽�ṹ��
	 * @pre �Ѿ�����open��create���ļ�
	 *
	 * @param iocb		�첽IO�ṹ��
	 * @param offset	Ҫд����������ļ��е���ʼƫ����������ļ���
	 *					 DIRECT_IOģʽ�򿪣���offsetһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param size		Ҫд���������������ļ���DIRECT_IOģʽ�򿪣�
	 *					 ��sizeһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 * @param buffer	Ҫд����������ݡ�����ļ���
	 *					 DIRECT_IOģʽ�򿪣���buffer�ĵ�ַһ��Ҫ�Ǵ���������ͨ��Ϊ512)��������
	 */
	static void fillAioHandlerWrite(struct iocb *iocb, File *file, const void *buffer, u32 size, u64 offset);

private:

	/** 
	 * ����첽������� IOCB �ṹ��
	 * @param iodb		���Ľṹ��
	 * @param fd		�ļ������
	 * @param buf		�������Ļ�����
	 * @param count		�������Ĵ�С
	 * @param offset	���������ļ�ƫ����
	 */
	static void io_prep_pread(struct iocb *iocb, int fd, const void *buf, size_t count, long long offset);

	/** 
	 * ����첽д����� IOCB �ṹ��
	 * @param iodb		���Ľṹ��
	 * @param fd		�ļ������
	 * @param buf		д�����Ļ�����
	 * @param count		д�����Ĵ�С
	 * @param offset	д�������ļ�ƫ����
	 */
	static void io_prep_pwrite(struct iocb *iocb, int fd, const void *buf, size_t count, long long offset);

	
	/** 
	 * ��ʼ���첽IO�����ģ���װϵͳ����
	 *
	 * @param nr		�첽IO�Ĳ�������
	 * @param ctxp		�첽IO������
	 * @return			����ϵͳ������
	 */
	static int io_setup(unsigned nr, aio_context_t *ctxp);


	/** 
	 * �����첽IO�����ģ���װϵͳ����
	 * @pre �ѵ���aio_init��ʼ�����첽IO����
	 *
	 * @param ctx		�첽IO������
	 * @return			����ϵͳ������
	 */
	static int io_destroy(aio_context_t ctx);


	/** 
	 * �ύ�첽IO���� ��װϵͳ����
	 * @param ctx		�첽IO������
	 * @param nr		�첽IO�Ĳ�����
	 * @param iodbpp	�첽IO�ṹ��ָ������
	 * @return			����ϵͳ������
	 */
	static int io_submit(aio_context_t ctx, long nr,  struct iocb **iocbpp);

	/** 
	 * �ȴ��첽IO������ɣ� ��װϵͳ����
	 * @param ctx		�첽IO������
	 * @param min_nr	�ȴ���ɵ��첽IO������Сֵ
	 * @param max_nr	�ȴ���ɵ��첽IO�������ֵ
	 * @param events	�첽IO���ؽ��������
	 * @param timeout	�ȴ��첽IO��ɵĳ�ʱʱ��
	 * @return ����ϵͳ������
	 */
	static int io_getevents(aio_context_t ctx, long min_nr, long max_nr, struct io_event *events, struct timespec *timeout);

#endif

public:
	static const u32        AIO_BATCH_SIZE = 256;           /** aio���г��� */

private:
#ifdef WIN32

#else
	aio_context_t           m_ctx;                          /** aio ������ */
	struct io_event         *m_aioEvents;					/** aio �¼����� */
	struct iocb				**m_cbs;						/** aio ����ָ������,���ڳ����ύaio���� */
#endif
	AioSlot                 *m_slots;						/** aio ������ */
	u32                     m_numReserved;                  /** aio ���п��г��� */
};

}

#endif
