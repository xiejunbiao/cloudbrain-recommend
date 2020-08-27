import json
import time

import pika
from pika.adapters import tornado_connection
from pymysql import connect
from redis import StrictRedis
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
# import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')


# LOGGER = logging.getLogger('RABBIT')


class RabbitConsumer:
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    executor = ThreadPoolExecutor(3)
    EXCHANGE = 'xwjCommerceGoodsInfoExchange'
    EXCHANGE_TYPE = 'fanout'
    INFO_CHANGE_QUEUE = 'GoodsInfoRecommendQueue'
    STORE_CHANGE_QUEUE = 'StoreChangeRecommendQueue'
    ROUTING_KEY = 'example.text'

    def __init__(self, config_dict, main_logger):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        # rabbit_conf_dict = config_dict['rabbit']
        # self._url = 'amqp://{}:{}@{}:{}/%2F'.format(
        #     rabbit_conf_dict['user'],
        #     rabbit_conf_dict['password'],
        #     rabbit_conf_dict['ip'],
        #     rabbit_conf_dict['port'],
        # )
        self._logger = main_logger
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self.is_consuming = False
        self.config_dict = config_dict

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        rabbit_conf_dict = self.config_dict['rabbit']
        self._logger.info('Connecting to %s:%s' %
                          (rabbit_conf_dict['ip'], rabbit_conf_dict['port']))
        # return tornado_connection.TornadoConnection(pika.URLParameters(self._url), self.on_connection_open)
        credentials = pika.PlainCredentials(rabbit_conf_dict['user'], rabbit_conf_dict['password'])
        parameters = pika.ConnectionParameters(host=rabbit_conf_dict['ip'],
                                               port=rabbit_conf_dict['port'],
                                               heartbeat=0,
                                               credentials=credentials)
        return tornado_connection.TornadoConnection(parameters, self.on_connection_open)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        self._logger.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        self._logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._logger.warning('Connection closed, reopening in 5 seconds: %s', reason)
            self._connection.ioloop.call_later(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        self._logger.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self._logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed
        :param Exception reason: why the channel was closed
        """
        self._logger.warning('Channel %i was closed: %s', channel, reason)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self._logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self._logger.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(exchange_name,
                                       self.EXCHANGE_TYPE,
                                       durable=True,
                                       callback=self.on_exchange_declareok)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self._logger.info('Exchange declared')
        self.setup_queue(self.INFO_CHANGE_QUEUE)
        self.setup_queue(self.STORE_CHANGE_QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self._logger.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(queue_name, durable=True, callback=self.on_queue_declareok)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        queue = method_frame.method.queue
        self._logger.info('Binding %s to %s with %s', self.EXCHANGE, queue, self.ROUTING_KEY)
        self._channel.queue_bind(queue, self.EXCHANGE, self.ROUTING_KEY, callback=self.on_bindok)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self._logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self._logger.info('Consumer was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag, send_time):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        # self._logger.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)
        self._logger.info('Acknowledg message success, send_time:%s', send_time)

    def get_mysql_conn(self):
        mysql_conf_dict = self.config_dict['mysql_2'].copy()
        mysql_conf_dict['host'] = mysql_conf_dict.pop('ip')
        mysql_conf_dict['port'] = int(mysql_conf_dict['port'])
        return connect(**mysql_conf_dict)

    def get_redis_conn(self):
        redis_conf_dict = self.config_dict['redis']
        return StrictRedis(redis_conf_dict['ip'], redis_conf_dict['port'],
                           redis_conf_dict['redis_num'], decode_responses=True)

    # 商品更改上下架状态和小区时触发
    @run_on_executor
    def on_message_for_goods_info(self, unused_channel, basic_deliver, properties, body):
        self._logger.info(body)
        body = json.loads(body)
        send_time = body['sendTime']
        # 不是商品上下架状态和小区范围更改的消息不予处理
        if body.get('funcName') not in ['goodsStatus', 'changeSpuScope']:
            return self.acknowledge_message(basic_deliver.delivery_tag, send_time)
        mysql_conn = self.get_mysql_conn()
        redis_conn = self.get_redis_conn()
        # 使用管道
        pl = redis_conn.pipeline()
        # 修改上下架
        if body['funcName'] == 'goodsStatus':
            try:
                self.handle_goods_status_change(body, mysql_conn, pl)
            except:
                self._logger.error('处理上下架异常, send_time:{}'.format(send_time), exc_info=True)
            else:
                self._logger.info('处理上下架成功, send_time:{}'.format(send_time))
        # 修改小区
        else:
            try:
                self.handle_goods_scope_change(body, mysql_conn, redis_conn, pl)
            except:
                self._logger.error('处理小区更改异常, send_time:{}'.format(send_time), exc_info=True)
            else:
                self._logger.info('处理小区更改成功, send_time:{}'.format(send_time))
        mysql_conn.close()
        pl.close()
        redis_conn.close()
        self.acknowledge_message(basic_deliver.delivery_tag, send_time)

    # 处理商品上下架状态更改
    @staticmethod
    def handle_goods_status_change(body, mysql_conn, pl):
        # 记录redis管道要执行的命令数量。每1000条就执行一次
        cmd_count = 0
        for spuCode in body['data']['spuCodeList']:
            temp = '''select area_code from cb_goods_scope where spu_code=%s''' % spuCode
            with mysql_conn.cursor() as cursor:
                count = cursor.execute(temp)
                if not count:
                    continue
                areaCode_list = [areaCode_tuple[0] + '_A' for areaCode_tuple in cursor.fetchall()]
            for areaCode in areaCode_list:
                if body['cmd'] == '01':
                    pl.rpush(areaCode, spuCode)
                else:
                    pl.lrem(areaCode, 0, spuCode)
                cmd_count += 1
            if cmd_count >= 1000:
                pl.execute()
                cmd_count = 0
        pl.execute()

    # 处理商品范围更改
    @staticmethod
    def handle_goods_scope_change(body, mysql_conn, redis_conn, pl):
        # TODO: 因依赖于另一个消息处理的最终结果，所以暂时先用sleep等待另一条消息队列处理完毕
        time.sleep(5)
        # 获取所有小区的key
        keys = redis_conn.keys('*_A')
        # 记录redis管道要执行的命令数量。每1000条就执行一次
        cmd_count = 0
        for spuCode in body['data']:
            # 把改动的spu在每个小区中删除
            for key in keys:
                pl.lrem(key, 0, spuCode)
                cmd_count += 1
            # 把状态好的spu添加到现在所在的小区下
            temp = '''
                SELECT
                    scope.area_code 
                FROM
                    cb_goods_scope AS scope
                    INNER JOIN cb_goods_spu_for_filter AS filter ON scope.spu_code = filter.spu_code 
                WHERE
                    scope.spu_code = %s 
                    AND filter.goods_status = 1 
                    AND filter.store_status = 1
            '''
            with mysql_conn.cursor() as cursor:
                count = cursor.execute(temp, [spuCode])
                if not count:
                    continue
                areaCode_list = [areaCode_tuple[0] + '_A' for areaCode_tuple in cursor.fetchall()]
            for areaCode in areaCode_list:
                pl.rpush(areaCode, spuCode)
                cmd_count += 1
            if cmd_count >= 1000:
                pl.execute()
                cmd_count = 0
        pl.execute()

    # 商品库存为0或从0到多时触发
    @run_on_executor
    def on_message_for_store_change(self, unused_channel, basic_deliver, properties, body):
        self._logger.info(body)
        body = json.loads(body)
        send_time = body['sendTime']
        mysql_conn = self.get_mysql_conn()
        redis_conn = self.get_redis_conn()
        # 使用管道
        pl = redis_conn.pipeline()
        try:
            # 记录redis管道要执行的命令数量。每1000条就执行一次
            cmd_count = 0
            # 库存为0
            if body['cmd'] == '0':
                # 获取该商品所在的所有小区
                for spuCode in body['data']['spuCodeList']:
                    temp = '''select area_code from cb_goods_scope where spu_code=%s''' % spuCode
                    with mysql_conn.cursor() as cursor:
                        count = cursor.execute(temp)
                        if not count:
                            continue
                        areaCode_list = [areaCode_tuple[0] + '_A' for areaCode_tuple in cursor.fetchall()]
                    # 在redis中，从该spu原来所处的所有小区中删除
                    for area in areaCode_list:
                        pl.lrem(area, 0, spuCode)
                        cmd_count += 1
                    if cmd_count >= 1000:
                        pl.execute()
                        cmd_count = 0
            else:
                cursor = mysql_conn.cursor()
                for spuCode in body['data']['spuCodeList']:
                    # 获取该商品的上下架状态
                    temp = '''select goods_status from cb_goods_spu_for_filter where spu_code=%s''' % spuCode
                    count = cursor.execute(temp)
                    if not count:
                        continue
                    row_one = cursor.fetchone()
                    # 如果是下架状态则不予处理
                    if row_one[0] == 0:
                        continue
                    # 在redis中，把该spu添加到所处小区中
                    temp = '''select area_code from cb_goods_scope where spu_code=%s''' % spuCode
                    cursor.execute(temp)
                    areaCode_list = [areaCode_tuple[0] + '_A' for areaCode_tuple in cursor.fetchall()]
                    cursor.close()
                    for area in areaCode_list:
                        pl.rpush(area, spuCode)
                        cmd_count += 1
                    if cmd_count >= 1000:
                        pl.execute()
                        cmd_count = 0
            pl.execute()
        except:
            # self._logger.info('{}'.format(traceback.format_exc()))
            self._logger.error('处理库存更改异常, send_time:{}'.format(send_time), exc_info=True)
        else:
            self._logger.info('处理库存更改成功, send_time:{}'.format(send_time))
        finally:
            mysql_conn.close()
            pl.close()
            redis_conn.close()
            self.acknowledge_message(basic_deliver.delivery_tag, send_time)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self._logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self._logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self._logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.INFO_CHANGE_QUEUE, self.on_message_for_goods_info)
        self._channel.basic_consume(self.STORE_CHANGE_QUEUE, self.on_message_for_store_change)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        self._logger.info('Queue bound')
        if not self.is_consuming:
            self.start_consuming()
            self.is_consuming = True

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self._logger.info('Closing the channel')
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self._logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self._logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        self._logger.info('Stopped')

# def main():
#     logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
#     example = RabbitConsumer('amqp://guest:guest@10.18.222.116:5672/%2F')
#     try:
#         example.run()
#     except KeyboardInterrupt:
#         example.stop()


# if __name__ == '__main__':
#     main()
