/**
* ͳ�Ʋ���
*
* @author л��(xieke@corp.netease.com, ken@163.org)
*/
#ifndef _NTSE_SAMPLE_H_
#define _NTSE_SAMPLE_H_


#include "cassert"
#include "util/Portable.h"
//#include "misc/Session.h"

namespace ntse {

class Session;

/*** ������� ***/
typedef u64 SampleID;	/** ����ҳ��� */


/*** ������ ***/
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

	/* ͨ���±���������� */
	int& operator[](int idx) {
		assert(idx < m_numFields && idx >= 0);
		return m_value[idx];
	}
	/* �������������ĸ��� */
	int getFieldNum() { return m_numFields; }

	SampleID m_ID;		/** ������ */
protected:
	int m_numFields;		/** ��������� */
	int *m_value;			/** ������ֵ���� */
private:
	/**
	* ����һ����������ģ���Լ�����ͳ�������Ŀ�����塣
	*
	* @param numFields  һ��������������ĸ�����Ϊ�˼򵥣�ÿ����������һ��int����
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


/** ������õ�һ��������Ľ�� **/
struct FieldCalc {
	double m_average;	/** ��������ͳ��������ƽ��ֵ */
	double m_delta;		/** ������� */
};


/*** ������� ***/
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
	int m_numFields;	/** ����� */
	int m_numSamples;	/** �Ѳ����������� */
};



/*** ������� ***/
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

/*** ��ͳ��ģ��ӿ� ***/
class Analysable {
public:
        virtual ~Analysable() {};
	/**
	 * ��ʼ����
	 *
	 * @param wantedSampleNum  ����Ĳ�����Ŀ����ģ����ݸ��Ե��������ʵ�ʲ�������
	 * @param fastSample  �Ƿ�����ڴ�����Ϊtrue��������ڻ����е�ҳ�档
	 * @return  һ��SampleHandle����
	 */
	virtual SampleHandle *beginSample(Session *session, uint wantedSampleNum, bool fastSample) = 0;
	/**
	 * �����һ������
	 * 
	 * @param handle  �������
	 * @return  �����ɹ�����������û����һ������NULL
	 */
	virtual Sample *sampleNext(SampleHandle *handle) = 0;
	/**
	 * ��ֹ����
	 *
	 * @param handle  �������
	 */
	virtual void endSample(SampleHandle *handle) = 0;
};



/*** �������� ***/
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
