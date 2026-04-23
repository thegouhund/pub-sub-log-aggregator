import asyncio
import time
import logging
from typing import List, Dict, Any, Union
from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict
from contextlib import asynccontextmanager

from src import database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]


event_queue: asyncio.Queue = None
start_time: float = time.time()


async def event_consumer():
    logger.info("Event consumer started.")
    while True:
        try:
            event = await event_queue.get()
            is_unique = await database.save_event_if_unique(event)

            if is_unique:
                await database.increment_stat("unique_processed")
                logger.info(
                    f"Processed unique event: {event['topic']} - {event['event_id']}"
                )
            else:
                await database.increment_stat("duplicate_dropped")
                logger.warning(
                    f"Duplicate event detected for topic '{event['topic']}' and event_id '{event['event_id']}', dropping."
                )

            event_queue.task_done()
        except asyncio.CancelledError:
            break


@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_queue
    event_queue = asyncio.Queue()
    await database.init_db()
    consumer_task = asyncio.create_task(event_consumer())
    yield
    consumer_task.cancel()
    await consumer_task


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.post("/publish")
async def publish_events(events: Union[EventModel, List[EventModel]]):
    if not isinstance(events, list):
        events = [events]

    for ev in events:
        await database.increment_stat("received")
        await event_queue.put(ev.model_dump())

    return {
        "status": "success",
        "message": f"{len(events)} events adedd for processing.",
    }


@app.get("/events")
async def get_events(topic: str):
    events = await database.get_events_by_topic(topic)
    return {"topic": topic, "events": events}


@app.get("/stats")
async def get_system_stats():
    stats = await database.get_stats()
    uptime_seconds = time.time() - start_time

    return {
        "received": stats.get("received", 0),
        "unique_processed": stats.get("unique_processed", 0),
        "duplicate_dropped": stats.get("duplicate_dropped", 0),
        "topics": stats.get("topics", []),
        "uptime_seconds": uptime_seconds,
    }
