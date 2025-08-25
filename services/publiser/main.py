from __future__ import annotations
import asyncio
import os
import json
import random
from typing import Dict, List
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from pydantic import BaseModel
from sklearn.datasets import fetch_20newsgroups

from shared.categories import interesting_categories, not_interesting_categories


class PublishResponse(BaseModel):
    sent_interesting: int
    sent_not_interesting: int
    
app = FastAPI(title="Publisher Service", version="0.1.0")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_INTERESTING = os.getenv("TOPIC_INTERESTING", "interesting")
TOPIC_NOT_INTERESTING = os.getenv("TOPIC_NOT_INTERESTING", "not_interesting")

async def get_producer() -> AIOKafkaProducer:
    "Get a Kafka producer"
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    return producer

async def sample_per_category(categories: List[str]) -> List[Dict[str, str]]:
    "Sample one message per category"
    dataset = fetch_20newsgroups(subset='all', categories=categories, remove=())
    results: List[Dict[str, str]] = []
    for category in categories:
        # Get all indices for the category
        indices_for_category = [i for i, t in enumerate(dataset.target) if dataset.target_names[t] == category]
        if not indices_for_category:
            continue
        selected_index = random.choice(indices_for_category)
        results.append({
            "category": category,
            "text": dataset.data[selected_index],
        })
    return results

@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}

@app.get("/publish", response_model=PublishResponse)
async def publish() -> PublishResponse:
    "Publish messages to Kafka."
    producer = await get_producer()
    try:
        interesting_msgs = await sample_per_category(interesting_categories)
        not_interesting_msgs = await sample_per_category(not_interesting_categories)
        
        async def send_all(topic: str, messages: List[Dict[str, str]]):
            "Send all messages to a topic."
            for msg in messages:
                await producer.send_and_wait(topic, json.dumps(msg).encode("utf-8"))
                
        await asyncio.gather(
            send_all(TOPIC_INTERESTING, interesting_msgs),
            send_all(TOPIC_NOT_INTERESTING, not_interesting_msgs),
        )
        
        return PublishResponse(
            sent_interesting=len(interesting_msgs),
            sent_not_interesting=len(not_interesting_msgs),
        )
    finally:
        await producer.stop()
        
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
