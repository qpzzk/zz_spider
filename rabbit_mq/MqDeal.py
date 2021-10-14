# -*- coding: utf-8 -*-
# @Time    : 10/14/21 5:21 PM
# @Author  : ZZK
# @File : MqDeal.py
# @describe ：处理rabbitmq内容
import pika
import requests
import json
from retrying import retry
from pika.exceptions import AMQPError

def retry_if_rabbit_error(exception):
    print('连接rabbitmq出现错误')
    return isinstance(exception, AMQPError)

class DealRabbitMQ(object):
    def __init__(self,host,user, passwd,queue,port,url_port):
        """

        :param host:
        :param user:
        :param passwd:
        :param queue:
        :param port:
        :param url_port:
        :param spider_main:
        """
        self.host = host
        self.user = user
        self.passwd = passwd
        self.queue = queue
        self.port = port
        self.url_port = url_port

        credentials = pika.PlainCredentials(user, passwd)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credentials,
                                                                       heartbeat=0))  # heartbeat 表示7200时间没反应后就报错
        self.channel = connection.channel()
        self.channel.basic_qos(prefetch_size=0, prefetch_count=1)

    @retry(retry_on_exception=retry_if_rabbit_error)
    def get_count_by_url(self):
        """
        :return: ready,unack,total
        """
        try:
            url = 'http://{0}:{1}/api/queues/%2f/{2}'.format(self.host,self.url_port,self.queue)
            r = requests.get(url, auth=(self.user, self.passwd))
            if r.status_code != 200:
                return -1
            res = r.json()
            # ready,unack,total
            return res['messages_ready'], res['messages_unacknowledged'], res['messages']
            # return dic['messages']
        except Exception as e:
            print("rabbitmq connect url error:",e)

    def callback(self,ch, method, properties, body):
        """
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        res = json.loads(body)
        self.spider_main(res)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    @retry(retry_on_exception=retry_if_rabbit_error)
    def run_mq(self,spider_main):
        self.spider_main = spider_main
        self.channel.basic_consume(self.queue, self.callback, False)
        while self.channel._consumer_infos:
            ready_count, unack_count, total_count = self.get_count_by_url()
            true_count = self.channel.queue_declare(queue=self.queue, durable=True).method.message_count  # 真实的结果数量
            print("ready中的消息量：{0}",true_count)
            if total_count == 0 and true_count == 0:   #当真实消息量以及ready中全为0才代表消耗完
                self.channel.stop_consuming()  # 退出监听
            self.channel.connection.process_data_events(time_limit=1)




