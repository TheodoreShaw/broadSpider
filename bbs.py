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
import logging
import urllib3
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

def captureBBS(bbsUrl,resourse,redis_connect):
	config = configparser.ConfigParser()
	config.read('config.ini')
	headers = eval(config.get('installation','headers'))
	kafka_url = config.get('kafka','kafka_api')
	try:
		r = requests.get(bbsUrl,timeout=20,headers=headers)
	except requests.exceptions.ConnectionError:
		return ""
	except requests.exceptions.ReadTimeout:
		return ""

	if r.status_code ==200:
		soup = BeautifulSoup(r.text,'lxml')
		one = soup.find_all(attrs={'class':'searchListOne'})
		try:
			temp = one[0].select('li')
			temp.pop(-1)
		except IndexError:
			return ""
		for each in temp:
			try:
				title = each.select('a')[0].get_text().replace("\n","").strip()
				webpageUrl = each.select('a')[0].attrs['href'].replace("\n","").strip()
				# print(title,webpageUrl)
				if(redis_connect.sismember("finished",webpageUrl)): # in redis continue
					continue 

				publishedSec = each.find_all(attrs={'class':'source'})
				publishedStr = publishedSec[0].select('span')[0].get_text()
				res = re.search('(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})',publishedStr)
				published = res.group(1)+'-'+res.group(2)+'-'+res.group(3)+'T'+res.group(4)+':'+res.group(5)+':'+res.group(6)+'Z'
				timeArray = time.strptime(published, "%Y-%m-%dT%H:%M:%SZ")
				timestamp = time.mktime(timeArray)
				ttemp =	time.localtime(timestamp-8*60*60)
				published = time.strftime("%Y-%m-%dT%H:%M:%SZ",ttemp)

				# print(title,webpageUrl,published)

				article = newspaper.Article(webpageUrl,language='zh',headers=headers) 
				article.download()
				article.parse()

				# print(article.text)
				captureTime = str(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
				id = str(uuid.uuid4().hex)

				if resourse == 'tianya':
					resourse = '天涯社区'

				data = {"id": id,
				        "content": article.text,
				        "resourse": resourse,
				        "title": title,
				        "webpageUrl": webpageUrl,
				        "fromType": "2",
				        "captureTime": captureTime,
				        "published": published}
				producer = KafkaProducer(bootstrap_servers=[kafka_url], value_serializer=lambda m: json.dumps(m).encode('utf-8'))
				future = producer.send('bbsMonitor' ,value = data, partition= 0)
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
			except AttributeError:
				continue
	else:
		pass

# if __name__ == '__main__':
# 	config = configparser.ConfigParser()
# 	config.read('config.ini')

# 	# connect redis server
# 	hosts = config.get('redis','redis_host')
	
# 	port = config.get('redis','redis_port')
# 	redis_connect = redis.Redis(host=hosts, port=port, db=0)


# 	captureBBS('http://search.tianya.cn/bbs?q=老胡&s=4&pn=2',redis_connect,'天涯论坛')
		

