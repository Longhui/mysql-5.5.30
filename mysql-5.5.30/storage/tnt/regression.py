#!/usr/bin/python
# coding=gbk

#
# 回归测试脚本
#

import urllib
import datetime
import logging
import operator
import popen2
import time
import sys
import os
import re
import smtplib
import traceback
from datetime import date
from xml.dom import minidom
from xml.dom.minidom import Document
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from optparse import OptionParser

# 下载依赖绘图库，如果已经下载直接解压
if not os.path.isfile('chartdir_python_linux_64.tar.gz'):
		url = "http://download2.advsofteng.com/chartdir_python_linux_64.tar.gz"
		urllib.urlretrieve(url,'chartdir_python_linux_64.tar.gz')
os.system("tar xvf chartdir_python_linux_64.tar.gz")

# 假设ChartDirector绘图库放在当前目录
sys.path.append("./ChartDirector/lib")
from pychartdir import *


# 测试信息
class TestInfo:
	def __init__(self, name, builder, cleaner, worker, resFile):
		self.builder = builder          # 构建命令
		self.cleaner = cleaner          # clean命令
		self.worker = worker            # 测试程序，测试结果以xml文件输出
		self.name = name                # 测试名
		self.resultFilename = resFile   # 测试程序输出的xml结果文件名


# 包含多个测试的测试集信息
class SuiteInfo:
	def __init__(self, name, sourcePath, archivePath, confs, email = None):
		self.name = name                # 测试集名
		self.sourcePath = sourcePath    # 源代码目录
		self.archivePath = archivePath  # 结果归档目录
		self.configurations = confs     # 测试配置(参考TestInfo)
		self.email = email              # 结果汇报到此邮箱


# 回归异常类
class RegressionException(Exception):
	pass


# 每个测试程序都对应一个执行器
# 回归测试框架调用执行器.run()方法执行用例
# 执行器包括：命令执行器，Valgrind执行器，Coverage执行器

# 命令执行器
class CmdExecutor:
	def __init__(self, command):
		self.command = command

	def run(self):
		logging.info( "run " +  self.command + " ...")
		os.remove("ntseunit_error.txt");
		os.system(self.command)


ntseSrc='.'                                         # ntse源代码目录(buildsetup can only run at '.')
ntseBuilder = os.path.join(ntseSrc, "buildsetup")   # ntse构建命令
ntseCleaner = ntseBuilder + " --clean"              # ntse clean命令
ntseSuites = "0"                                    # ntse测试用例


# Valgrind执行器
# 利用valgrind检测内存错误，并输出valgrind错误信息
class ValgrindExecutor:
	def __init__(self, command, unitReport, output):
		self.output = output            # 输出xml文件名
		self.command = command          # 测试名
		self.unitReport = unitReport    # command产生的xml文件

	def run(self):
		valErr = 'err'
		os.system('rm ' + self.unitReport);
		os.system('valgrind --suppressions=valgrind.supp --leak-check=full --log-file=' + valErr + ' ' + self.command)
		doc = minidom.parse(self.unitReport)
		f = open(valErr, 'r')
		text = f.read()
		textNode = doc.createCDATASection(text)
		f.close()
		valNode = doc.createElement('valgrind')
		if self.isSuccess(text) :
			valNode.setAttribute('success', 'true')
		else:
			valNode.setAttribute('success', 'false')
		valNode.appendChild(textNode)
		doc.documentElement.appendChild(valNode)
		f = open(self.output, 'w')
		f.write(doc.toxml())
		f.close()

	@staticmethod
	def findError(text, reg, correctCnt):
		prog = re.compile(reg, re.M)
		result = prog.search(text)
		if result is None:
			return True
		errCnt = result.group(1)
		if int(errCnt) != correctCnt:
			return False
		return True

	@staticmethod
	def isSuccess(text):
		if not ValgrindExecutor.findError(text, r'ERROR SUMMARY: (\d+) ', 0):
			return False
		if not ValgrindExecutor.findError(text, r'definitely lost: (\d+) ', 0):
			return False
		if not ValgrindExecutor.findError(text, r'possibly lost: (\d+) ', 0):
			return False
		return True


# Coverage执行器
# 利用lcov计算和输出代码覆盖率
class CoverageExecutor:
	def __init__(self, output):
		self.output = output

	def run(self):
		unitReport = 'tmpreport.xml'
		os.system('rm -f' + unitReport);
		os.system('./ntsetest_cov_bp_dbg -f ' + unitReport + ' ' + ntseSuites)
		doc = minidom.parse(unitReport)
		os.system('./gen-cov')
		f = open('cov-html/index.html', 'r')
		text = f.read()
		f.close()
		text.find('Code&nbsp;covered:</td>')
		text = text[text.find('Code&nbsp;covered:</td>'):]
		prog = re.compile('>(\d+(\.\d+)?)', re.M)
		result = prog.search(text)
		coverage = result.group(1) + '%'
		covNode = doc.createElement('coverage')
		covNode.appendChild(doc.createTextNode(coverage))
		doc.documentElement.appendChild(covNode)
		f = open(self.output, 'w')
		f.write(doc.toxml())
		f.close()


# ntse单元测试定义
ntseconfs = [
	TestInfo("debug[ut,bp]", ntseBuilder + " -dbv", ntseCleaner, CmdExecutor("./ntsetest_bp_dbg -f ntseunit_bp_dbg.xml 2>ntseunit_error.txt " + ntseSuites), "ntseunit_bp_dbg.xml"),
	TestInfo("release[ut,bp]", ntseBuilder + " -bv", ntseCleaner, CmdExecutor("./ntsetest_bp -f ntseunit_bp.xml 2>ntseunit_error.txt " + ntseSuites), "ntseunit_bp.xml"),
	TestInfo("coverage[ut]", ntseBuilder + " -dvbc", ntseCleaner, CoverageExecutor("ntseunit_cov_bp_dbg.xml"), "ntseunit_cov_bp_dbg.xml"),
	TestInfo("valgrind[ut,mc]", ntseBuilder + " -dvbm", ntseCleaner, ValgrindExecutor("./ntsetest_bp_dbg_mem -f tmpreport.xml 2>ntseunit_error.txt " + ntseSuites, "tmpreport.xml", "ntseunit_bp_dbg_mem.xml"), "ntseunit_bp_dbg_mem.xml")
	]
ntseunit = SuiteInfo("ntseunit", ntseSrc, "regression", ntseconfs, 'xindingfeng@corp.netease.com')

# fake测试定义，只用于测试
fakeResult = """'<test><success>1</success><failure>2</failure><total>3</total><message>the case 2,3 failed, assert x!=y</message></test>'"""

fakeconfs = [
			TestInfo("fakeconf1"
				, ""
				, ""
				, CmdExecutor("echo " + fakeResult + ' > fakeunit.xml')
				, "fakeunit.xml"),
			TestInfo("fakeconf2"
				, ""
				, ""
				, CmdExecutor("echo " + fakeResult + ' > fakeunit.xml')
				, "fakeunit.xml"),
			TestInfo("fakeval"
				, ""
				, ""
				, ValgrindExecutor("echo " + fakeResult + ' > tmpreport.xml', "tmpreport.xml", "fakeunit.xml")
				, "fakeunit.xml")
			]
fakeunit = SuiteInfo("fakeunit", ".", "fakearchive", fakeconfs, 'xindingfeng@corp.netease.com');



# Weekly NTSE Test Overview Charter
# 总结最近几天的回归测试结果，并产生图表
class TestOverViewCharter:
	def __init__(self, suite, days):
		self.suite = suite
		self.days = days

	@staticmethod
	def readResult(dir, filename):
		reportDoc = minidom.parse(os.path.join(dir, filename))
		testResults=[]
		coverage = 0
		for testNode in  reportDoc.documentElement.getElementsByTagName('test'):
			total = int(testNode.getElementsByTagName('total')[0].firstChild.data)
			success = int(testNode.getElementsByTagName('success')[0].firstChild.data)
			failure = int(testNode.getElementsByTagName('failure')[0].firstChild.data)
			testResults.append((total, success, failure))
			covNodes = testNode.getElementsByTagName('coverage');
			if len(covNodes) != 0:
				coverage = float(covNodes[0].firstChild.data[:-1])
		return max(testResults,  key=operator.itemgetter(2)), coverage

	@staticmethod
	def drawChart(output, testDates, results):
		labels = [ str(x) for x in testDates ]
		success = [  result[0][1] for result in results ]
		failure =  [  result[0][2] for result in results ]
		coverage = [ result[1] for result in results ]
		c = XYChart(850, 640)
		# Set the plotarea at (100, 40) and of size 600 x 500 pixels
		c.setPlotArea(100, 40, 600, 500)
		# Add a legend box at (750, 200)
		c.addLegend(750, 200)
		# Add a title to the chart using 14 points Times Bold Itatic font
		c.addTitle("Weekly NTSE UnitTest Overview", "timesbi.ttf", 14)
		# Add a title on top of the secondary (right) y axis.
		c.yAxis2().setTitle("Coverage(%)").setAlignment(TopRight2)
		# Set the axis, label and title colors for the secondary y axis to blue (0x0000cc)
		# to match the second data set
		c.yAxis2().setColors(0x0000cc, 0x0000cc, 0x0000cc)
		# Add a line layer to for the second data set using blue (0000cc) color, with a
		# line width of 2 pixels. Bind the layer to the secondary y-axis.
		layer1 = c.addLineLayer(coverage, 0x0000cc, "Coverage")
		layer1.setLineWidth(2)
		layer1.setUseYAxis2()
		# Add a title to the y axis. Draw the title upright (font angle = 0)
		c.yAxis().setTitle("Cases").setFontAngle(0)
		# Set the labels on the x axis
		c.xAxis().setLabels(labels)
		c.xAxis().setLabelStyle("", 8, TextColor, 45)

		# Add a stacked bar layer and set the layer 3D depth to 8 pixels
		layer = c.addBarLayer2(Stack, 0)
		# Add the three data sets to the bar layer
		layer.addDataSet(failure, 0xff8080, "Failure")
		layer.addDataSet(success, 0x80ff80, "Success")
		# Enable bar label for the whole bar
		layer.setAggregateLabelStyle()
		# Enable bar label for each segment of the stacked bar
		layer.setDataLabelStyle()
		# output the chart
		c.makeChart(output)

	def makeOverview(self, output):
		days = self.days
		today = date.today()
		for root, dirs, files in os.walk(self.suite.archivePath):
			results = []
			testDates = []
			for filename in files:
				strList = filename.split('-')
				testDay = date(int(strList[0]), int(strList[1]), int(strList[2]))
				if (today - testDay).days <= 10:
					results.append(self.readResult(root, filename))
					testDates.append(testDay)
			self.drawChart(output, testDates, results)


# 更新源代码
def updateSource(path):
	os.system("svn update " + path);
	r, w = popen2.popen2("svn info --xml " + path)
	output = r.read()
	r.close();
	w.close();
	prog = re.compile(r'revision="(\d+)"', re.M)
	result = prog.search(output)
	revision = result.group(1)
	return int(revision)


# 汇总各次测试结果，产生汇总xml文档
def makeReport(suite, revision, startTime, elapsedTime, results):
	doc = Document()
	root = doc.createElement("alltests")
	doc.appendChild(root)

	root.setAttribute("name", suite.name)

	startTimeNode = doc.createElement("startime");
	startTimeNode.appendChild(doc.createTextNode(time.ctime(startTime)))
	root.appendChild(startTimeNode)

	durationNode = doc.createElement("elapsedtime");
	durationNode.appendChild(doc.createTextNode(str(elapsedTime)))
	root.appendChild(durationNode)

	revisionNode = doc.createElement("revision");
	revisionNode.appendChild(doc.createTextNode(str(revision)))
	root.appendChild(revisionNode)

	for i in range(len(results)):
		root.appendChild(results[i].documentElement)
	return doc


# 发送邮件
def sendMail(suite, subject, atts):
	try:
		if suite.email == None:
			return
		me = 'xindingfeng@corp.netease.com'
		dst = suite.email

		msg = MIMEMultipart()
		msg['Subject'] = subject
		msg['From'] = me
		msg['To'] = dst

		for att in atts:
			msg.attach(att)

		s = smtplib.SMTP()
		s.connect('corp.netease.com:25')
		s.login('xindingfeng', 'xindingfeng163')
		mailReciever = ["yulihua@corp.netease.com", "hzhedengcheng@corp.netease.com", "hzhuwei@corp.netease.com", "xindingfeng@corp.netease.com"]
		#mailReciever = "xindingfeng@corp.netease.com"
		s.sendmail(me, mailReciever, msg.as_string())
		s.close()
	except Exception, e:
		print e
		logging.error("send mail report to " + dst + " failed")


# 检查测试是否通过
def isSuccess(report):
	for testNode in  report.documentElement.getElementsByTagName('test'):
		total = int(testNode.getElementsByTagName('total')[0].firstChild.data)
		success = int(testNode.getElementsByTagName('success')[0].firstChild.data)
		failure = int(testNode.getElementsByTagName('failure')[0].firstChild.data)
		if failure != 0:
			return False

	return True

# check valgrind errors
def isValgrindSuccess(report):
	if not 	ValgrindExecutor.isSuccess(report.toxml()):
		return False
	return True

# 发送测试报告
def sendReport(suite, runName, report):
		subject = "[report-trunk]" + suite.name + " " + runName
		if (isSuccess(report)):
			subject += " success, "
		else:
			subject += " failure, "

		if (isValgrindSuccess(report)):
			subject += "Valgrind: Success"
		else:
			subject += "Valgrind: Failure"

		att = MIMEBase('text', 'xml')
		att.set_payload(report.toxml())
		att.add_header('Content-Disposition', 'attachment', filename = runName + '.xml')
		atts = []
		atts.append(att)
		charter = TestOverViewCharter(suite, 10)
		charter.makeOverview('WNUO.png')
		f = open('WNUO.png', 'rb')
		content = f.read()
		f.close()
		att = MIMEImage(content)
		att.add_header('Content-Disposition', 'attachment', filename = 'WNUO.png')
		atts.append(att)
		sendMail(suite, subject, atts)


# 运行测试
def runTest(test):
	logging.info("compile " +  test.name +  " as " +  test.builder + " ...")
	startTime = time.time()
	if 0 != os.system(test.builder) :
		raise RegressionException, test.builder + " && compile error"
	test.worker.run()
	endTime = time.time()
	doc = minidom.parse(test.resultFilename)
	startTimeNode = doc.createElement("startime")
	startTimeNode.appendChild(doc.createTextNode(time.ctime(startTime)))
	durationNode = doc.createElement("elapsedtime")
	durationNode.appendChild(doc.createTextNode(str(endTime - startTime)))
	root = doc.documentElement
	root.setAttribute("name", test.name)
	root.insertBefore(durationNode, root.firstChild)
	root.insertBefore(startTimeNode, root.firstChild)
	return doc


# 测试前准备
def setupEnv(suite):
	if 0 != os.system('valgrind -q echo hello > /dev/null'):
		raise RegressionException, 'no valgrind found'
	if False == os.path.exists(suite.archivePath):
		os.mkdir(suite.archivePath)
	for conf in suite.configurations:
		os.system(conf.cleaner)
		try:
			os.remove(conf.resultFilename)
		except:
			pass


# 运行测试集
def runSuite(suite, runName):
	"""run a suite"""
	try:
		startTime = time.time()
		revision = updateSource(suite.sourcePath)
		setupEnv(suite)
		results = []
		for test in suite.configurations:
			results.append(runTest(test))
		endTime = time.time()
		reportDoc = makeReport(suite, revision, startTime, endTime - startTime, results)
		filename = os.path.join(suite.archivePath, runName + '.' + str(revision) + ".xml")
		f = open(filename, 'w')
		f.write(reportDoc.toxml())
		f.close()
		sendReport(suite, runName, reportDoc)
	except:
		errFile = open("ntseunit_error.txt")
		message = errFile.read()
		message += traceback.format_exc()
		logging.error(message)
		sendMail(suite, "[error-trunk]" + suite.name, [MIMEText(message)])


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

parser = OptionParser()
parser.add_option("-H", "--hour", dest="runHour", type="int", default=23, help="start regression at this hour")
parser.add_option("-r", "--runOnce", dest="runOnce", action="store_true", default=False, help="run regression once")
(options, args) = parser.parse_args()

if options.runOnce:
	curTime = time.localtime();
	timeStr =  time.strftime('%Y-%m-%d-%H', curTime)
	#runSuite(fakeunit, timeStr)
	runSuite(ntseunit, timeStr);
	sys.exit()

startHour = options.runHour

while True:
	curTime = time.localtime();
	if curTime.tm_hour == startHour :
		timeStr =  time.strftime('%Y-%m-%d-%H', curTime)
		runSuite(ntseunit, timeStr)

		curTime = time.localtime();
		while curTime.tm_hour == startHour:
			time.sleep(60)
			curTime = time.localtime();
	else :
		time.sleep(60)
