import requests
import urllib
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
import newspaper
import configparser
from newspaper import Article
import redis
import urllib3
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

def captureNews(baiduUrl,redis_connect):
	config = configparser.ConfigParser()
	config.read('config.ini')
	headers = eval(config.get('installation','headers'))
	kafka_url = config.get('kafka','kafka_api')
	try:
		r = requests.get(baiduUrl,timeout=20,headers=headers)
	except requests.exceptions.ConnectionError:
		return ""
	except requests.exceptions.ReadTimeout:
		return ""

	if r.status_code ==200:
		soup = BeautifulSoup(r.text,'lxml')
		for one in soup.find_all(attrs={'class':'result'}):
			title = (one.select('a')[0].get_text()).replace("\n","").strip()
			webpageUrl = (one.select('a')[0].attrs['href']).replace("\n","").strip()

			if(redis_connect.sismember('finished',webpageUrl)):
				continue

			authorSec = one.find_all("p",attrs={'class':'c-author'})
			authorPre = authorSec[0].get_text().replace("\n","").replace("\t","").replace(" ","")
			try:
				published = ""
				resourse = ""
				author = "".join(authorPre.split())
				res1 = re.search('(\d{4})年(\d{2})月(\d{2})日(\d{2}):(\d{2})',author)
				res2 = re.search('(\d{1,2})小时前',author)
				res3 = re.search('(\d{1,2})分钟前',author)
				if res1:
					published = res1.group(1)+'-'+res1.group(2)+'-'+res1.group(3)+'T'+res1.group(4)+':'+res1.group(5)+':00Z'
					resourse = author.replace(res1.group(),'').strip()
					timeArray = time.strptime(published, "%Y-%m-%dT%H:%M:%SZ")
					timestamp = time.mktime(timeArray)
					ttemp =	time.localtime(timestamp-8*60*60)
					published = time.strftime("%Y-%m-%dT%H:%M:%SZ",ttemp)
				elif res2:
					t = time.time() - int(res2.group(1))*60*60 - 8*60*60
					published = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.localtime(t))
					resourse = author.replace(res2.group(),'').strip()
				elif res3:
					t = time.time() - int(res3.group(1))*60 - 8*60*60
					published = time.strftime("%Y-%m-%dT%H:%M:%SZ",time.localtime(t))
					resourse = author.replace(res3.group(),'').strip()
				if not resourse:
					resourse='百度新闻'
				if not published:
					published =	str(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))

				article = newspaper.Article(webpageUrl,language='zh',headers=headers) 
				article.download()
				article.parse()

				# redis_connect.set(webpageUrl,1)
				# redis_connect.expire(webpageUrl,3600*24*5) # redis 5 days expire

				if article.text == "":
					continue
				# kafka code
				captureTime = str(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
				id = str(uuid.uuid4().hex)
				data = {"id": id,
				        "content": article.text,
				        "resourse": resourse,
				        "title": title,
				        "webpageUrl": webpageUrl,
				        "fromType": "7",
				        "captureTime": captureTime,
				        "published": published}
				producer = KafkaProducer(bootstrap_servers=[kafka_url], value_serializer=lambda m: json.dumps(m).encode('utf-8'))
				future = producer.send('monitor' ,value = data, partition= 0)
				result = future.get(timeout= 2)
				redis_connect.sadd('finished',webpageUrl)


			except newspaper.article.ArticleException:
				continue
			except IndexError:
				continue
			except urllib3.exceptions.ReadTimeoutError:
				continue
			except RuntimeError:
				continue
			except urllib3.exceptions.InvalidHeader:
				continue
			except TypeError:
				continue
			except ValueError:
				continue

	else:
		pass

# if __name__ == '__main__':
#     config = configparser.ConfigParser()
#     config.read('config.ini')

#     # connect redis server
#     hosts = config.get('redis','redis_host')
#     port = config.get('redis','redis_port')
#     redis_connect = redis.Redis(host=hosts, port=port, db=0)
#     captureNews("http://news.baidu.com/ns?word=%E8%8B%97%E5%AF%A8&pn=0&cl=2&ct=0&tn=news&rn=20&ie=utf-8&bt=0&et=0",redis_connect)
		

