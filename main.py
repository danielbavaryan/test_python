from typing import Optional
from fastapi import FastAPI
from k import KafkaClient

app = FastAPI()

kafka_client = KafkaClient('localhost:9092', 'topic_1', 'group_1')
kafka_client.start()



@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}


@app.get("/produce")
async def read_root():
    await kafka_client.produce(value='ok')
    return {"status": "ok"}
