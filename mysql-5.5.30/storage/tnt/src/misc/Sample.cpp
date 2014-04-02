#include "misc/Sample.h"
#include "math.h"
#include "util/Hash.h"
#include "misc/Session.h"

using namespace std;
using namespace ntse;


/**
 * ���������в�������
 *
 * @param session  �Ự
 * @param object  ��ɨ���ģ���ṩ��ɨ�����
 * @param maxSample  ��������Ŀ���������С�������ʹ�������֮��
 * @param smallScanPct  С�����������ı���
 * @param scanBig  �Ƿ������������
 * @param difference  ��β���ʱ��������β����Ĳ���ȣ����ֵԽС�����ڶ��ƽ�в����Ľ�����ƶ�Ҫ��Խ��
 * @param bScanTimes  ������������������еĴ���
 * @return  �����������
 */
SampleResult * SampleAnalyse::sampleAnalyse(Session *session, Analysable *object, int maxSample, int smallScanPct,
											bool scanBig, double difference, int bScanTimes) {
	assert(maxSample > 0);
	if (!smallScanPct && !scanBig)
		return NULL; // �Ȳ�ɨ��С��������Ҳ��ɨ�����������ֻ�з��ؿ���
	u64 mcsavept = session->getMemoryContext()->setSavepoint();
	assert(smallScanPct >= 0 && smallScanPct <= 100);
	uint smallSampleCnt = 0, bigSampleCnt = 0;
	Sample **sScanSamples = NULL, **bScanSamples = NULL;
	SampleID *bSIDs = NULL;
	SampleResult *smallSampleResult = NULL, *bigSampleResult = NULL;
	SampleHandle *handle;
	Sample *sample;

	if (smallScanPct) {
		/* ��Ҫ����С������ɨ�� */
		int count = maxSample * smallScanPct / 100;
		if (count > 0) {
			sScanSamples = (Sample **)session->getMemoryContext()->alloc(count * sizeof(Sample *));//new Sample *[count];
			handle = object->beginSample(session, count, true);
			while (count-- && (sample = object->sampleNext(handle)) != NULL) {
				sScanSamples[smallSampleCnt++] = sample;
			}
			object->endSample(handle);
		}
		if (smallSampleCnt) {
			smallSampleResult = analyse(sScanSamples, smallSampleCnt);
		} else {
			scanBig = true; // �ڴ���û����������ֻ����������
			smallScanPct = 0;
		}
	}
	if (scanBig && !(smallScanPct == 100)) {
		/* �������������� */
		/* ��һ�β�������С������������бȽ� */
		int bSampleTotal = maxSample - smallSampleCnt; //maxSample * (100 - smallScanPct) / 100; // �ܴ�������������
		bScanSamples = (Sample **)session->getMemoryContext()->alloc(bSampleTotal * sizeof(Sample *));//new Sample *[bSampleTotal];
		bSIDs = (SampleID *)session->getMemoryContext()->alloc(bSampleTotal * sizeof(SampleID));//new SampleID[bSampleTotal];
		Hash<SampleID, Sample *> sidHT(bSampleTotal);
		int count = bSampleTotal / bScanTimes; // ÿһ�β�����Ҫ��������
		if (0 == count) count = 1;
		handle = object->beginSample(session, count, false);
		while (count-- && (sample = object->sampleNext(handle)) != NULL) {
			if (sidHT.get(sample->m_ID)) { // �Ѿ��и������ŵ������ˣ���ɾ��������
				//Sample *old = sidHT.get(sample->m_ID);
				//delete old;
				sidHT.remove(sample->m_ID);
			}
			sidHT.put(sample->m_ID, sample);
			//bScanSamples[bigSampleCnt++] = sample;
		}
		object->endSample(handle);
		sidHT.elements(bSIDs, bScanSamples);
		bigSampleCnt = (uint)sidHT.getSize();
		assert(bigSampleCnt); // �������޷������κ�ҳ������
		bigSampleResult = analyse(bScanSamples, bigSampleCnt);
		/* �Ƚ�big�����small��� */
		if (!smallScanPct || !compareResult(smallSampleResult, bigSampleResult, difference)) {
			// ��������ϣ����߸���û��С������������������Ա�Աȡ�
			count = bSampleTotal / bScanTimes; // ÿ�β�����Ŀ
			if (0 == count) count = 1;
			Sample **tmpSample = (Sample **)session->getMemoryContext()->alloc(count * sizeof(Sample *));//new Sample *[count];
			SampleResult *tmpResutl;
			for (int i = 1; i < bScanTimes; ++i) { // ��Ϊ�Ѿ�������һ�Σ�����i��ʼΪ1
				handle = object->beginSample(session, count, false);
				int samCnt = 0;
				while (samCnt < count && (sample = object->sampleNext(handle)) != NULL) {
					tmpSample[samCnt++] = sample;
				}
				object->endSample(handle);
				tmpResutl = analyse(tmpSample, samCnt);
				bool isSimilar = compareResult(tmpResutl, bigSampleResult, difference);
				for (int i = 0; i < samCnt; ++i) {
					if (sidHT.get(tmpSample[i]->m_ID)) { // �Ѿ��и������ŵ������ˣ���ɾ��������
						//Sample *old = sidHT.get(tmpSample[i]->m_ID);
						//delete old;
						sidHT.remove(tmpSample[i]->m_ID);
					}
					sidHT.put(tmpSample[i]->m_ID, tmpSample[i]);
					//bScanSamples[bigSampleCnt++] = tmpSample[i];
				}
				delete tmpResutl;
				delete bigSampleResult;
				sidHT.elements(bSIDs, bScanSamples);
				bigSampleCnt = (uint)sidHT.getSize();
				bigSampleResult = analyse(bScanSamples, bigSampleCnt);
				if (isSimilar) break;
			}
			//delete [] tmpSample;
		}

	}
	/* ����sample */
	/* ʹ��MemoryContext����Ҫ�ֶ��ͷ��ڴ�
	if (smallSampleCnt) {
		for (uint i = 0; i < smallSampleCnt; ++i) {
			delete sScanSamples[i];
		}
	}
	*/
	/*
	if (smallScanPct) {
		delete [] sScanSamples;
	}*/
	/* ʹ��MemoryContext����Ҫ�ֶ��ͷ��ڴ�
	if (bigSampleCnt) {
		for (uint i = 0; i < bigSampleCnt; ++i) {
			delete bScanSamples[i];
		}
		delete [] bScanSamples;
		delete [] bSIDs;
	}*/
	session->getMemoryContext()->resetToSavepoint(mcsavept);
	/* ���ؽ�� */
	if (bigSampleCnt) {
		assert(bigSampleResult != NULL);
		if (smallSampleResult)
			delete smallSampleResult;
		return bigSampleResult;
	} else {
		assert(bigSampleResult == NULL);
		return smallSampleResult;
	}
}

/**
 * �������������ؽ��
 * 
 * @param samples  Sampleָ������
 * @param numSample  �����е�Sampleָ�����
 */
SampleResult * SampleAnalyse::analyse(Sample **samples, int numSample) {
	long *fieldSum;		/** ���еĺ� */
	double *delta2Sum;	/** ���еľ�ֵ��� */
	assert(numSample > 0 && samples);
	int numFields = samples[0]->getFieldNum();
	fieldSum = new long[numFields];
	delta2Sum = new double[numFields];
	SampleResult *result = new SampleResult(numFields);
	/* ������к� */
	for (int fi = 0; fi < numFields; ++fi) {
		fieldSum[fi] = 0;
		for (int si = 0; si < numSample; ++si) {
			fieldSum[fi] += (*samples[si])[fi];
		}
		/* ��¼�о�ֵ */
		//(result->m_fieldCalc + fi)->m_average = (double)fieldSum[fi] / (double)numSample;
		result->m_fieldCalc[fi].m_average = (double)fieldSum[fi] / (double)numSample;
	}
	/* ������з���� */
	for (int fi = 0; fi < numFields; ++fi) {
		delta2Sum[fi] = .0;
		for (int si = 0; si < numSample; ++si) {
			double avgDiff = result->m_fieldCalc[fi].m_average - (*samples[si])[fi];
			delta2Sum[fi] += avgDiff * avgDiff;
		}
		/* ��¼�����ֵ */
		//(result->m_fieldCalc + fi)->m_sumVariance = delta2Sum[fi] / (double)numSample;
		result->m_fieldCalc[fi].m_delta = sqrt(delta2Sum[fi] / (double)numSample);
	}
	delete [] fieldSum;
	delete [] delta2Sum;
	result->m_numSamples = numSample;
	return result;
}

/**
 * �������SampleAnalyse�Ƿ���ڸ����Ĳ���ȷ�Χ֮�ڡ�
 *
 * @param first, second  �����������
 * @param difference  ���ƶ�������췶Χ�������������������ֵ��͸��Է���ı�����
 * @return  ������Χ��Ϊtrue�����򷵻�false
 */
bool SampleAnalyse::compareResult(SampleResult *first, SampleResult *second, double difference) {
	assert(first->m_numFields == second->m_numFields);
	bool isSimilar = true;
	for (int fi = 0; fi < first->m_numFields; ++fi) {
		double diff = first->m_fieldCalc[fi].m_average - second->m_fieldCalc[fi].m_average;
		if (diff < 0) diff = 0 - diff;
		if (first->m_fieldCalc[fi].m_delta * difference < diff || second->m_fieldCalc[fi].m_delta * difference < diff) {
			isSimilar = false;
			break;
		}
	}
	return isSimilar;
}


Sample * Sample::create(Session *session, int numFields, SampleID id) {
	Sample *sample = (Sample *)session->getMemoryContext()->alloc(sizeof(Sample));
	sample->m_numFields = numFields;
	sample->m_value = (int *)session->getMemoryContext()->alloc(sizeof(int) * numFields);
	for (int i = 0; i < numFields; ++i) {
		sample->m_value[i] = 0;
	}
	sample->m_ID = id;
	return sample;
}
