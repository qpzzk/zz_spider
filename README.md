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
from zz_spider.rabbit_mq.MqDeal import DealRabbitMQ

host = ''
port = 1234 #组件连接rabbitmq的端口
user = ''
password = ''
queue_name = ''
url_port = 12345  #url方式组件连接rabbitmq的端口

def spider(res):
    """
    爬虫主体
    :param res:  接收的消息原变量，1000条种子为一个list
    :return: 
    """
    for i in res:
        print(i)

mq = DealRabbitMQ(host,user,password,queue_name,port,url_port)
print(mq.get_count_by_url())
mq.run_mq(spider) #爬虫的主体函数

```
> 如需帮助请联系 zzk_python@163.com