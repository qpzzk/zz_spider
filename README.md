# 本代码是有关消息中间件组件的一些使用

##rabbimq
```
import zz_spider
from zz_spider.rabbit_mq.MqDeal import DealRabbitMQ
```
### 使用
```
# -*- coding: utf-8 -*-
# @Time    : 10/14/21 5:38 PM
# @Author  : ZZK
# @File : test_spider.py
# @describe ：
from zz_spider.RabbitMq import DealRabbitMQ

host = '10.238.60.107'
port = 5672
user = 'root'
password = 'Spider@2020'
queue_name = 'in_16_20211201000000'
url_port = 15672


def spider(res):
    """
    :param res:
    :return:
    """

    for i in res:
        data =i
        #mongo(data)
        print(i)

mqobeject = DealRabbitMQ(host,user=user,passwd=password,port=port,url_port=url_port)

#spider_main 放置抓取的主要函数
mqobeject.consumer_mq(spider_main=spider,queue_name=queue_name)

#将错误数据写入失败队列中，后缀名必须为_error
mqobeject.send_mq(queue_name='123_error',msg={'1':1})

```
> 如需帮助请联系 zzk_python@163.com