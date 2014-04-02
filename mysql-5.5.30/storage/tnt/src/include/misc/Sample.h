/**
* 统计采样
*
* @author 谢可(xieke@corp.netease.com, ken@163.org)
*/
#ifndef _NTSE_SAMPLE_H_
#define _NTSE_SAMPLE_H_


#include "cassert"
#include "util/Portable.h"
//#include "misc/Session.h"

namespace ntse {

class Session;

/*** 样本编号 ***/
typedef u64 SampleID;	/** 即是页面号 */


/*** 样本类 ***/
class Sample {
public:


	
	static Sample *create(Session *session, int numFields, SampleID id = 0);/* {
		Sample *sample = (Sample *)session->getMemoryContext()->alloc(sizeof(Sample));
		sample->m_value = (int *)session->getMemoryContext()->alloc(sizeof(int) * numFields);
		for (int i = 0; i < numFields; ++i) {
			m_value[i] = 0;
		}
		m_ID = id;
	}
	*/

	/* 通过下标访问样本域 */
	int& operator[](int idx) {
		assert(idx < m_numFields && idx >= 0);
		return m_value[idx];
	}
	/* 获得样本属性域的个数 */
	int getFieldNum() { return m_numFields; }

	SampleID m_ID;		/** 样本号 */
protected:
	int m_numFields;		/** 样本域个数 */
	int *m_value;			/** 样本域值数组 */
private:
	/**
	* 生成一个样本，各模块自己控制统计域的数目和意义。
	*
	* @param numFields  一个样本的属性域的个数，为了简单，每个属性域用一个int类型
	*/
	Sample(int numFields) : m_numFields(numFields) {
		m_value = new int[numFields];
		for (int i = 0; i < numFields; ++i) {
			m_value[i] = 0;
		}
	}
	~Sample() {
		delete [] m_value;
	}
};


/** 采样获得的一个样本域的结果 **/
struct FieldCalc {
	double m_average;	/** 该域所有统计样本的平均值 */
	double m_delta;		/** 均方差根 */
};


/*** 采样结果 ***/
class SampleResult {
public:
	SampleResult(int numFields) {
		m_fieldCalc = new FieldCalc[numFields];
		m_numSamples = 0;
		m_numFields = numFields;
	}
	~SampleResult() {
			delete [] m_fieldCalc;
	}
	FieldCalc *m_fieldCalc;
	int m_numFields;	/** 域个数 */
	int m_numSamples;	/** 已采样样本个数 */
};



/*** 采样句柄 ***/
class SampleHandle {
public:
	SampleHandle(Session *session, uint maxSampleNum, bool fastSample)
		: m_maxSample(maxSampleNum), m_fastSample(fastSample) ,m_lastID((SampleID)-1) ,m_session(session) {}
	virtual ~SampleHandle() {};
	uint m_maxSample;
	bool m_fastSample;
	SampleID m_lastID;
	Session *m_session;
};

/*** 可统计模块接口 ***/
class Analysable {
public:
        virtual ~Analysable() {};
	/**
	 * 开始采样
	 *
	 * @param wantedSampleNum  请求的采样数目，各模块根据各自的情况决定实际采样数量
	 * @param fastSample  是否进行内存区，为true则仅采样在缓存中的页面。
	 * @return  一个SampleHandle对象。
	 */
	virtual SampleHandle *beginSample(Session *session, uint wantedSampleNum, bool fastSample) = 0;
	/**
	 * 获得下一个样本
	 * 
	 * @param handle  采样句柄
	 * @return  采样成功返回样本，没有下一个返回NULL
	 */
	virtual Sample *sampleNext(SampleHandle *handle) = 0;
	/**
	 * 终止采样
	 *
	 * @param handle  采样句柄
	 */
	virtual void endSample(SampleHandle *handle) = 0;
};



/*** 样本分析 ***/
class SampleAnalyse {
public:
	static SampleResult *sampleAnalyse(Session *session, Analysable *object, int maxSample,
		int scanSmallPct = 50, bool scanBig = true, double difference = 0.618, int bScanTimes = 10);

private:
	static SampleResult *analyse(Sample **samples, int numSample);
	static bool compareResult(SampleResult *first, SampleResult *second, double similarity);
};

} //namespace ntse {

#endif // #ifndef _NTSE_SAMPLE_H_
