import redis
import urllib
import configparser
import urllib.request
import time
import json
global redis_connect


def news_init(pagenum):
	if int(pagenum)<0: pagenum=0
	keywords = []
	url = config.get('news','keywords_api')
	try:
		data = urllib.request.urlopen(url).read()
		record = data.decode("utf-8")
		record_json = json.loads(record)
		for i in range(len(record_json)):
			keywords.append(record_json[i]['keyword'])
	except urllib.error.URLError:
		pass

	for word in keywords:
		for i in range(int(pagenum)):
			baiduUrl="http://news.baidu.com/ns?word="+urllib.parse.quote(word)+"&pn="+str(i*10)+"&cl=2&ct=0&tn=news&rn=20&ie=utf-8&bt=0&et=0"
			if redis_connect.sadd('ready',baiduUrl) == 0:
				continue
			else:
				redis_connect.hset(baiduUrl,'fromType',7)
	pass

def websites_init():
	urls = []
	url = config.get('website','url_api')
	try:
		data = urllib.request.urlopen(url).read()
	except urllib.error.URLError:
		pass
	
	record = data.decode("utf-8")
	record_json = json.loads(record)
	for i in range(len(record_json)):
		websiteUrl = record_json[i]['url']
		if(websiteUrl.startswith('http://') or websiteUrl.startswith('https://')):
			pass
		else:
			websiteUrl = 'http://'+websiteUrl

		if redis_connect.sadd('ready',websiteUrl) == 0:
			continue
		else:
			redis_connect.hset(websiteUrl,'fromType',1)
			redis_connect.hset(websiteUrl,'depth',0)

def bbs_init(pagenum):
	if int(pagenum)<0: pagenum=0
	keywords = []
	url = config.get('news','keywords_api')
	try:
		data = urllib.request.urlopen(url).read()
		record = data.decode("utf-8")
		record_json = json.loads(record)
		for i in range(len(record_json)):
			keywords.append(record_json[i]['keyword'])
	except urllib.error.URLError:
		pass

	for word in keywords:
		for i in range(int(pagenum)):
			bbsUrl = 'http://search.tianya.cn/bbs?q='+urllib.parse.quote(word)+'&s=4&pn='+str(i+1)
			print(bbsUrl)
			if redis_connect.sadd('ready',bbsUrl) == 0:
				continue
			else:
				redis_connect.hset(bbsUrl,'fromType',2)
				redis_connect.hset(bbsUrl,'resourse','tianya')
	pass

if __name__ == '__main__':
	config = configparser.ConfigParser()
	config.read('config.ini')

	# connect redis server
	hosts = config.get('redis','redis_host')
	port = config.get('redis','redis_port')
	redis_connect = redis.Redis(host=hosts, port=port, db=0)
	while True:	
		if redis_connect.smembers('ready'):
			time.sleep(300)
			continue
	
		# insert news url
		pagenum = config.get('news','news_pagenums')
		news_init(pagenum)

		#insert website url
		websites_init()

		#insert bbs url
		bbs_pagenum = config.get('bbs','bbs_pagenum')
		bbs_init(bbs_pagenum)

	# print(redis_connect.smembers('ready'))
	# one = redis_connect.srandmember('ready')
	# print(one)
	# print(redis_connect.hget(one,'fromType'))


	# redis_connect.sadd('finished',baiduUrl)
	# redis_connect.srem('ready',baiduUrl)
	# if not redis_connect.smembers('finished'):	
	# 	print(redis_connect.smembers('finished'))
	# print(redis_connect.smembers('ready'))

