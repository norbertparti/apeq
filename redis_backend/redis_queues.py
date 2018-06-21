import time

import redis


def _connect_to_redis(connection_pool):
    return redis.StrictRedis(connection_pool=connection_pool)


class Queue:
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
        self.r = _connect_to_redis(connection_pool)
        self.worker_id = worker_id


class TopicRedisQueue(Queue):
    """
    Reliable message queue based on Redis
    """
    def push(self, value):
        pass  # TODO implement push message into queue

    def __iter__(self):
        while True:
            res = self.r.rpoplpush(src=self.topic, dst=self.processing_queue)
            if res == f'STOP:{self.worker_id}':
                break
            if res:
                yield res
            else:
                time.sleep(0.1)
                continue


class ProcessingQueue(Queue):

    def maintain(self, timeout, frequency):
        """
        Push back unprocessed messages into topic's queue
        Args:
            timeout: timeout in seconds
            frequency: maintain frequency (sec)
        """
        while True:
            time.sleep(frequency)
            res = self.r.rpoplpush(src=self.processing_queue, dst=self.processing_queue)  # rolling the queue
            if res:
                encoded_res = str(res, encoding='utf-8')
                if encoded_res == f'STOP:{self.worker_id}':
                    break

                timestamp, message = encoded_res.split(';')
                timed_out = time.time() - float(timestamp) > float(timeout)
                outdated = timed_out * 10
                if timed_out:
                    self._push_back(res)
                    print(f'Pushed back: {res}')

                elif outdated:
                    self._remove_from_processing(res)
            else:
                continue

    def _push_back(self, value):
        self.r.rpush(self.topic, value)

    def _remove_from_processing(self, res):
        self.r.lrem(name=self.processing_queue, count=0, value=res)
