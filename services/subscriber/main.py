from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import contextlib
import time
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "subscriber-group")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "interesting")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "news20")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", KAFKA_TOPIC)


app = FastAPI(title=f"Subscriber - {KAFKA_TOPIC}", version="0.1.0")

mongo_client: Optional[AsyncIOMotorClient] = None
consumer: Optional[AIOKafkaConsumer] = None
consume_task: Optional[asyncio.Task] = None

# Tracks last time /messages/new was called
last_get_time: Optional[datetime] = None
 
async def ensure_indexes():
    "Ensure indexes are created on the MongoDB collection."
    global mongo_client
    if mongo_client is None:
        mongo_client = AsyncIOMotorClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    coll = db[MONGO_COLLECTION]
    await coll.create_index("timestamp")
    
async def consumer_loop():
    "Consume message from Kafka"
    global consumer
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    # Retry until Kafka is available
    deadline = time.time() + 90
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            await consumer.start()
            break
        except Exception as e:
            last_err = e
            await asyncio.sleep(3)
    else:
        raise RuntimeError(f"Failed to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}") from last_err
    try:
        db = mongo_client[MONGO_DB] # type: ignore
        coll = db[MONGO_COLLECTION]
        async for msg in consumer:
            payload = msg.value
            doc = {
                **payload, # type: ignore
                "timestamp": datetime.now(timezone.utc),
                "topic": KAFKA_TOPIC,
            }
            await coll.insert_one(doc)
    finally:
        await consumer.stop()
        
        
@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "topic": KAFKA_TOPIC}


@app.get("/messages/all")
async def get_all() -> List[Dict[str, Any]]:
    "Get all messages from the database"
    db = mongo_client[MONGO_DB] # type: ignore
    coll = db[MONGO_COLLECTION]
    docs = []
    async for doc in coll.find().sort("_id", 1):
        doc["_id"] = str(doc["_id"])  # make JSON serializable
        # ISO format for timestamp
        if isinstance(doc.get("timestamp"), datetime):
            doc["timestamp"] = doc["timestamp"].isoformat()
        docs.append(doc)
    return docs

@app.get("/messages/new")
async def get_new() -> List[Dict[str, Any]]:
    "Get new messages from the database."
    global last_get_time
    db = mongo_client[MONGO_DB] # type: ignore
    coll = db[MONGO_COLLECTION]

    query = {}
    if last_get_time is not None:
        query = {"timestamp": {"$gt": last_get_time}}

    now = datetime.now(timezone.utc)
    docs = []
    async for doc in coll.find(query).sort("timestamp", 1):
        doc["_id"] = str(doc["_id"])  # serialize
        if isinstance(doc.get("timestamp"), datetime):
            doc["timestamp"] = doc["timestamp"].isoformat()
        docs.append(doc)

    # advance pointer after returning data
    last_get_time = now
    return docs


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8001")))