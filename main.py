import redis
import configparser
import time
import os
import random
from news import captureNews
from web import website
from bbs import captureBBS
import time

if __name__ == '__main__':
	config = configparser.ConfigParser()
	config.read('config.ini')

	# connect redis server
	hosts = config.get('redis','redis_host')
	port = config.get('redis','redis_port')
	redis_connect = redis.Redis(host=hosts, port=port, db=0)
	while True:
		while 1 :
			if not redis_connect.smembers("ready"): 
				break
			url = redis_connect.srandmember('ready')
			url = url.decode('utf-8')
			fromType = redis_connect.hget(url,'fromType')
			fromType = fromType.decode('utf-8')
			if fromType == '7':
				redis_connect.srem('ready',url)
				captureNews(url,redis_connect)

			elif fromType == '1':
				depth = redis_connect.hget(url,'depth')
				depth = depth.decode('utf-8')
				redis_connect.srem('ready',url)
				website(url,int(depth),redis_connect)

			elif fromType == '2':
				source = redis_connect.hget(url,'resourse')
				source = source.decode('utf-8')
				redis_connect.srem('ready',url)
				captureBBS(url,source,redis_connect)

			else:
				pass

