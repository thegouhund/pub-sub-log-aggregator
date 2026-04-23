import aiosqlite
import json
import os
from typing import Dict, Any, List

DB_PATH = os.getenv("DATABASE_PATH", "events.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                topic TEXT,
                event_id TEXT,
                timestamp TEXT,
                source TEXT,
                payload TEXT,
                PRIMARY KEY (topic, event_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value INTEGER
            )
        """)

        await db.execute(
            'INSERT OR IGNORE INTO stats (key, value) VALUES ("received", 0)'
        )
        await db.execute(
            'INSERT OR IGNORE INTO stats (key, value) VALUES ("unique_processed", 0)'
        )
        await db.execute(
            'INSERT OR IGNORE INTO stats (key, value) VALUES ("duplicate_dropped", 0)'
        )
        await db.commit()


async def increment_stat(key: str, amount: int = 1):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE stats SET value = value + ? WHERE key = ?", (amount, key)
        )
        await db.commit()


async def save_event_if_unique(event: Dict[str, Any]) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            payload_str = json.dumps(event.get("payload", {}))
            cursor = await db.execute(
                """
                INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
                VALUES (?, ?, ?, ?, ?)
            """,
                (
                    event["topic"],
                    event["event_id"],
                    event["timestamp"],
                    event["source"],
                    payload_str,
                ),
            )
            await db.commit()
            return cursor.rowcount > 0
        except aiosqlite.IntegrityError:
            return False  # prevent dupe


async def get_events_by_topic(topic: str) -> List[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM processed_events WHERE topic = ?", (topic,)
        )
        rows = await cursor.fetchall()
        result = []
        for row in rows:
            result.append(
                {
                    "topic": row["topic"],
                    "event_id": row["event_id"],
                    "timestamp": row["timestamp"],
                    "source": row["source"],
                    "payload": json.loads(row["payload"]),
                }
            )
        return result


async def get_stats() -> Dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT key, value FROM stats")
        rows = await cursor.fetchall()
        stats_dict = {row[0]: row[1] for row in rows}

        cursor_topics = await db.execute("SELECT DISTINCT topic FROM processed_events")
        topics = [row[0] for row in await cursor_topics.fetchall()]

        stats_dict["topics"] = topics
        return stats_dict
