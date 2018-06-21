import time

import redis


class RedisQueueTopic:
    """
    Reliable message queue based on Redis
    """

    def _connect_to_redis(self, connection_pool):
        return redis.StrictRedis(connection_pool=connection_pool)

    def __init__(self, connection_pool, name, worker_id, processing_queue):
        """
        Args:
            connection_pool: explicit connection pool to use
            topic: topic of messages
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
