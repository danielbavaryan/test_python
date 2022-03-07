import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
from typing import Optional


class KafkaClient(object):

    def __init__(self, url: str, topic: str, group_id: str):
        self.topic = topic
        self.url = url
        self.group_id = group_id
        self.consumer = None
        self.producer = None
        self.loop = asyncio.get_event_loop()
        self.is_running = True

    def start(self):
        asyncio.ensure_future(self.run_kafka())
        asyncio.ensure_future(self.consume())

    async def run_kafka(self):
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.url)
            await self.producer.start()
            print('producer started')
        except Exception as e:
            print(e)
        try:
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.url,
                group_id=self.group_id)
            await self.consumer.start()
            print('consumer started')
        except Exception as e:
            print(e)

    async def consume(self):
        print('self.is_running', self.is_running)
        while self.is_running:
            try:
                # some delay for decrease cpu usage
                await asyncio.sleep(2)
                result = await self.consumer.getmany(timeout_ms=10 * 1000)
                for tp, messages in result.items():
                    if messages:
                        for msg in messages:
                            print("consumed message: ", msg.value.decode())
            except Exception as e:
                self.is_running = False
                print('terminated consumer')
                print('self.is_running', self.is_running)
                print(e)

    async def produce(self, topic: Optional[str] = None, value: str = ""):
        try:
            if not topic:
                topic = self.topic
            await self.producer.start()
            await self.producer.send_and_wait(topic, value=json.dumps(value).encode())
            print('produced msg')
            print('self.is_running', self.is_running)
        except Exception as e:
            print(e)

    async def stop_producer(self):
        try:
            await self.producer.stop()
        except Exception as e:
            print(e)
        self.is_running = False
