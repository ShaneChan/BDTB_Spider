#-*-coding:utf-8-*-

import urllib
import urllib2
import re
import time
import MySQLdb

#选择数据库命令
MysqlChooseCommand = 'USE PYTHON_TEST'
#数据库创建命令
MysqlCommand1 ='CREATE TABLE IF NOT EXISTS %s (floorhost VARCHAR(20), content VARCHAR(300), floor VARCHAR(20), time VARCHAR(50))' 


#字符串清理工具类
class Tools:
    removeImg = re.compile('<img.*?>| {7}|')
    removeAddr = re.compile('<a.*?>|</a>')
    replaceLine = re.compile('<tr>|<div>|</div></p>')
    replaceTD = re.compile('<td>')
    replacePara = re.compile('<p.*?>')
    repalceBR = re.compile('<br><br>|<br>')
    removeExtraTg = re.compile('<.*?>')

    #字符串清理函数，清除爬取到的回复中多余的部分
    def replace(self, x):
        x = re.sub(self.removeImg,"",x)
        x = re.sub(self.removeAddr, "", x)
        x = re.sub(self.replaceLine, "\n", x)
        x = re.sub(self.replaceTD, "\t", x)
        x = re.sub(self.replacePara, "\n  ", x)
        x = re.sub(self.repalceBR, "\n", x)
        x = re.sub(self.removeExtraTg, "", x)
        return x.strip()


class BDTB_Spider:

	#百度爬虫类的初始化
	def __init__(self, baseURL):
		self.baseURL = baseURL
		self.pageNum = 0
		self.totalNum = 0
		self.db = MySQLdb.connect('localhost', 'root', '1996723')
		self.cursor = self.db.cursor()
		self.Tools = Tools()

	#获取当前时间
	def getCurrentTime(self):
		return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

	#按页数获取指定帖子的URL
	def getPageURLByNum(self, pageNum):
		return self.baseURL + '?pn=' + str(pageNum)

	#按页数获取指定帖子的网页HTML源代码
	def getPageCodeByNum(self, pageNum):
		url = self.getPageURLByNum(pageNum)
		request = urllib2.Request(url)
		try:
			response = urllib2.urlopen(request)
			return response.read().decode('utf-8')
		except urllib2.URLError, e:
			if hasattr(e, 'code'):
				print 'Wrong getting of this page, code:', e.code
			if hasattr(e, 'reason'):
				print 'Wrong getting of this page, reason:', e.reason
				return None

	#获取指定帖子的总页数
	def getTotalNum(self):
		html = self.getPageCodeByNum(1)
		pattern = re.compile('<span class="red">(\d+)</span>', re.S)
		number = re.findall(pattern, html)
		if number:
			return "".join(number[0])
		else:
			print '获取帖子总页数失败'
			return None
	
	#获取帖子标题
	def getTitle(self):
		html = self.getPageCodeByNum(1)
		title_pattern = re.compile('<h3 class=.*?title="(.*?)".*?>', re.S)
		title = re.findall(title_pattern, html)
		if title:
			return "".join(title[0])
		else:
			return None

	#获取帖子的回复内容
	def getContentByNum(self, pageNum):
		html = self.getPageCodeByNum(pageNum)
		pattern = re.compile('<a data-field=.*?>(.*?)</a>.*?<div id="post_content.*?>(.*?)</div><br>.*?(</span><span class="tail-info">.*?)</span></div>', re.S)
		contents = re.findall(pattern, html)
		if contents:
			return contents
		else:
			return None

	#创建百度贴吧数据库（数据库名称为帖子标题）
	def createDatabase(self):
		name = "".join(self.getTitle())
		try:
			self.db.set_character_set('utf8')
			self.db.autocommit(1)
			self.cursor.execute(MysqlChooseCommand)
			MysqlCommand1 = 'CREATE TABLE IF NOT EXISTS %s (floorhost VARCHAR(50), content VARCHAR(2000), floor VARCHAR(100), timer VARCHAR(500))' % (name)
			self.cursor.execute(MysqlCommand1)
			print self.getCurrentTime(), '创建数据库成功'
			return name.encode('utf-8')
		except MySQLdb.Error, error:
			self.db.rollback()
			print '数据库创建错误'
			return None

	#将爬取到的帖子内容插入至数据库
	def insertData(self, name):
		totalNum = int(self.getTotalNum())
		print self.getCurrentTime(),'当前帖子共有%d页' % (totalNum)
		print self.getCurrentTime(),'将从第1页开始爬取帖子内容'
		print self.getCurrentTime(), '正在将爬取到的帖子内容保存至数据库'
		for pageNum in range(1, totalNum + 1):
			contents = self.getContentByNum(pageNum)
			if contents:
				for content in contents:
					content = list(content)
					content[1] = self.Tools.replace(content[1])
					sub_pattern = re.compile('</span>.*<span class="tail-info">(.*?)</span><span class="tail-info">(.*)', re.S)
					sub_contents = re.findall(sub_pattern, content[2])
					MysqlCommand2 = 'INSERT INTO ' + name + ' VALUES(%s, %s, %s, %s)' #数据库插入命令
					value = [content[0], content[1], sub_contents[0][0], sub_contents[0][1]]
					try:
						self.cursor.execute(MysqlCommand2, value)
						print '爬取到一条新的回复：'
						print content[0]
						print content[1]
						print sub_contents[0][0]
						print sub_contents[0][1]
						print self.getCurrentTime(), '成功存储一条内容至数据库'
						print '-----------------------------分割线--------------------------------'
						print '\n'
					except MySQLdb.Error, error:
						print self.getCurrentTime(), '插入数据错误，原因%d:%s' % (error.args[0], error.args[1])
						cursor.rollback()
			else:
				print '未在当前帖子爬取到任何内容'

	#主执行函数
	def execute(self):
		database_name = self.createDatabase()
		self.insertData(database_name)
		print '数据保存完成！'


if __name__ == '__main__':
	bdtb = BDTB_Spider('https://tieba.baidu.com/p/5167574704')
	bdtb.execute()
