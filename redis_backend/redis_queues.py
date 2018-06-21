import time

import redis


class RedisQueueTopic:
    """
    Reliable message queue based on Redis
    """

    def _connect_to_redis(self, connection_pool):
        return redis.StrictRedis(connection_pool=connection_pool)

    def __init__(self, connection_pool, worker_id, name, processing_queue):
        """
        Args:
            connection_pool: explicit connection pool to use
            name: topic of messages
            processing_queue: temporary queue for messages which are in processing
        """
        self.name = name
        self.processing_queue = processing_queue
        self.r = self._connect_to_redis(connection_pool)
        self.worker_id = worker_id

    def __iter__(self):
        while True:
            res = self.r.rpoplpush(src=self.name, dst=self.processing_queue)
            if res == f'STOP:{self.worker_id}':
                break
            if res:
                yield res
            else:
                time.sleep(0.1)
                continue


class ProcessingQueue:
    def _connect_to_redis(self, connection_pool):
        return redis.StrictRedis(connection_pool=connection_pool)

    def __init__(self, connection_pool, worker_id, topic, processing_queue):
        """
        message format timestamp(unix epoch);message
        Args:
            connection_pool: explicit connection pool to use
            topic: topic of messages
            processing_queue: temporary queue for messages which are in processing
        """
        self.topic = topic
        self.processing_queue = processing_queue
        self.r = self._connect_to_redis(connection_pool)
        self.worker_id = worker_id

    def maintain(self, timeout):
        """
        Push back unprocessed messages into topic's queue
        Args:
            timeout: timeout in seconds
        """
        while True:
            res = self.r.lpop(self.processing_queue)
            if res:
                encoded_res = str(res, encoding='utf-8')
                if encoded_res == f'STOP:{self.worker_id}':
                    break

                timestamp, value = encoded_res.split(';')
                if time.time() - float(timestamp) > float(timeout):
                    print(f'Pushed back: {value}')
                    self.r.rpush(self.topic, f'{time.time()};{value}')
            else:
                time.sleep(1)
                continue
