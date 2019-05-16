import configparser
import redis
import newspaper
from datetime import datetime, timedelta, timezone,timedelta
import uuid
import urllib
import http
import time
import requests
import urllib3
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

def website(url,depth,redis_connect):
    config = configparser.ConfigParser()
    config.read('config.ini')
    headers = eval(config.get('installation','headers'))
    max_depth = int(config.get('installation','deep_length')) 
    kafka_url = config.get('kafka','kafka_api')
    kwargs = newspaper.network.get_request_kwargs(1000,"",{}, headers)
    paper = newspaper.build(url,language='zh',kwargs=kwargs,memoize_articles=False)
    for category in paper.category_urls():
        if category == url:
            continue
        if depth<max_depth :
            print(category)
            redis_connect.sadd("ready",category)
            redis_connect.hset(category,'fromType',1)
            redis_connect.hset(category,'depth',depth+1)

    for i in range(len(paper.articles)):
        article = paper.articles[i]
        
        if(redis_connect.sismember("finished",article.url)): # in redis continue
            continue 

        try:
            article.download()
            article.parse()

            if article.text == "":
                continue
            # solr code
            captureTime = str(datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
            if article.publish_date:
                published = (article.publish_date+timedelta(hours=-8)).strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                published = captureTime
            id = str(uuid.uuid4().hex)
            data = {"id": id,
                    "content": article.text,
                    "resourse": u"定向监测",
                    "title": article.title,
                    "webpageUrl": article.url,
                    "fromType": "1",
                    "captureTime": captureTime,
                    "published": published}
            producer = KafkaProducer(bootstrap_servers=[kafka_url], value_serializer=lambda m: json.dumps(m).encode('utf-8'))
            future = producer.send('monitor' ,value = data, partition= 0)
            result = future.get(timeout= 2)
            redis_connect.sadd('finished',article.url)


        except newspaper.article.ArticleException as e:
            continue
        except urllib3.exceptions.ReadTimeoutError as e:
            continue
        except urllib3.exceptions.InvalidHeader:
            continue
        except RuntimeError:
            continue
        except TypeError:
            continue

    pass


# if __name__ == '__main__':
#     config = configparser.ConfigParser()
#     config.read('config.ini')

#     # connect redis server
#     hosts = config.get('redis','redis_host')
#     port = config.get('redis','redis_port')
#     redis_connect = redis.Redis(host=hosts, port=port, db=0)
#     website(url="http://www.baise.gov.cn",depth=0,redis_connect=redis_connect)
