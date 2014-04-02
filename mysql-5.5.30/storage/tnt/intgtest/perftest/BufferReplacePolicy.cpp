#include "BufferReplacePolicy.h"
#include "DbConfigs.h"
#include "api/Table.h"
#include "api/Database.h"
#include "misc/Session.h"
#include "LongCharTable.h"
#include "Random.h"
#include <vector>

using namespace ntse;
using namespace ntseperf;
using namespace std;

namespace ntseperf {

BufferReplacePolicyTest::BufferReplacePolicyTest() {
	setConfig(CommonDbConfig::getSmall());
	setTableDef(LongCharTable::getTableDef(false));
}

string BufferReplacePolicyTest::getName() const {
	return "BufferReplacePolicyTest";
}

string BufferReplacePolicyTest::getDescription() const {
	return "Test performance of buffer replacement policy using Zip-f distribution of 5% ideal miss rate.";
}

void BufferReplacePolicyTest::loadData(u64 *totalRecSize, u64 *recCnt) {
	openTable(true);

	// 装载10倍于缓存大小的数据量
	size_t targetSize = (m_config->m_mmsSize + m_config->m_pageBufSize) * Limits::PAGE_SIZE * 10;
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("loadData", conn);
	m_table->lockMeta(session, IL_S, -1, __FILE__, __LINE__);
	u64 startId = 0;
	while (m_table->getDataLength(session) < targetSize) {
		u64 dataSize = LongCharTable::getRecordSize();
		uint rows = LongCharTable::populate(session, m_table, &dataSize, startId);
		assert(rows == 1);
		startId += rows;
	}
	*recCnt = startId;
	*totalRecSize = *recCnt * LongCharTable::getRecordSize();
	m_table->unlockMeta(session, IL_S);
	m_db->getSessionManager()->freeSession(session);
	m_db->freeConnection(conn);

	closeDatabase();
}

class BRPTester: public Thread {
public:
	BRPTester(Database *db, Table *table, RowId *ridDistr, int ridDistrSize, int loopCount):
		Thread("BRPTester") {
		m_db = db;
		m_table = table;
		m_ridDistr = ridDistr;
		m_ridDistrSize = ridDistrSize;
		m_loopCount = loopCount;
	}

	void run () {
		Connection *conn = m_db->getConnection(false);
		Session *session = m_db->getSessionManager()->allocSession("loadData", conn);

		System::srandom(getId());
		u16 columns[1] = {0};
		byte buf[Limits::PAGE_SIZE];
		TblScan *scanHandle = m_table->positionScan(session, OP_READ, 1, columns);
		for (int i = 0; i < m_loopCount; i++) {
			size_t idx = RandomGen::nextInt() % m_ridDistrSize;
			RowId rid = m_ridDistr[idx];
			NTSE_ASSERT(m_table->getNext(scanHandle, buf, rid));
		}
		m_table->endScan(scanHandle);

		m_db->getSessionManager()->freeSession(session);
		m_db->freeConnection(conn);
	}

private:
	Database	*m_db;
	Table		*m_table;
	RowId		*m_ridDistr;
	int			m_ridDistrSize;
	int			m_loopCount;
};

class BPRProgressReporter: public Task {
public:
	BPRProgressReporter(Database *db): Task("BPRProgressReporter", 10000) {
		m_db = db;
		m_lastLogicalReads = m_lastPhysicalReads = 0;
	}

	void run () {
		Buffer *buffer = m_db->getPageBuffer();
		u64 logRead = buffer->getStatus().m_logicalReads;
		u64 phyRead = buffer->getStatus().m_physicalReads;
		buffer->updateExtendStatus();
		buffer->printStatus(cout);
		if (logRead > m_lastPhysicalReads) {
			cout << "buffer miss rate: " << (phyRead - m_lastPhysicalReads) * 1000 / (logRead - m_lastLogicalReads)
				<< "/1000" << endl;
		}
		m_lastLogicalReads = logRead;
		m_lastPhysicalReads = phyRead;
	}

private:
	Database	*m_db;
	u64			m_lastLogicalReads;
	u64			m_lastPhysicalReads;
};

void BufferReplacePolicyTest::run() {
	openTable();

	vector<RowId> ridVec;

	// 先扫描一遍，拿到所有的RID
	Connection *conn = m_db->getConnection(false);
	Session *session = m_db->getSessionManager()->allocSession("loadData", conn);
	u16 columns[1] = {0};
	TblScan *scanHandle = m_table->tableScan(session, OP_READ, 1, columns);
	byte buf[Limits::PAGE_SIZE];
	while (m_table->getNext(scanHandle, buf))
		ridVec.push_back(scanHandle->getCurrentRid());
	m_table->endScan(scanHandle);
	scanHandle = NULL;

	// 使用Zipf分布生成概率分布
	int ridDistrSize = (int)ridVec.size() * 20;
	RowId *ridDistr = new RowId[ridDistrSize];
	double *freqArr = new double[ridVec.size()];
	double sum = 0;
	double screw = 1.2;
	for (size_t n = 1; n <= ridVec.size(); n++) {
		sum += 1.0 / pow(n, screw);
	}
	int dispatched = 0;
	double freqSum = 0.0;

	int highFreqs = 0;
	bool firstNonHigh = false;
	for (size_t k = 1; k <= ridVec.size(); k++) {
		freqArr[k - 1] = 1.0 / pow(k, screw) / sum;
		freqSum += freqArr[k - 1];
		if (freqArr[k - 1] >= 10.0 / ridVec.size())
			highFreqs++;
		else {
			if (!firstNonHigh)
				cout << "sum of high freq: " << freqSum << endl;
			firstNonHigh = true;
		}
		if (k == ridVec.size() / 10) {
			cout << "freq of top 10 percent: " << freqSum;
			cout << ", end freq: " << freqArr[k - 1] << endl;
		}
		int targetDispatch = (int)(ridDistrSize * freqSum);
		for (int i = dispatched; i < targetDispatch; i++)
			ridDistr[i] = ridVec[k - 1];
		dispatched = targetDispatch;
	}
	while (dispatched < ridDistrSize)
		ridDistr[dispatched++] = ridVec[0];
	cout << "highFreqs: " << highFreqs << endl;

	delete []freqArr;
	freqArr = NULL;

	u64 phyReadOld = m_db->getPageBuffer()->getStatus().m_physicalReads;
	cout << "physical reads before test: " << phyReadOld << endl;
	// 多线程测试
	Task *progressReporter = new BPRProgressReporter(m_db);
	progressReporter->start();

	int numThreads = 10;
	Thread **testThreads = new Thread*[numThreads];
	for (int i = 0; i < numThreads; i++) {
		testThreads[i] = new BRPTester(m_db, m_table, ridDistr, ridDistrSize, 100000);
		testThreads[i]->start();
	}
	for (int i = 0; i < numThreads; i++) {
		testThreads[i]->join();
		delete testThreads[i];
	}
	delete []testThreads;
	
	Thread::msleep(10000);
	progressReporter->stop();
	progressReporter->join();
	delete progressReporter;

	u64 phyReadNew = m_db->getPageBuffer()->getStatus().m_physicalReads;
	cout << "physical reads after test: " << phyReadNew << endl;
	cout << "physical reads during test: " << (phyReadNew - phyReadOld) << endl;

	delete []ridDistr;
	ridDistr = NULL;

	closeDatabase();
}

}

