import httpx
import uuid
import datetime
import asyncio
import os
import sys

API_URL = os.getenv("API_URL", "http://localhost:8000")


async def send_single():
    async with httpx.AsyncClient() as client:
        print("\n--- sending Single Event ---")
        event_1_id = str(uuid.uuid4())
        event_1 = {
            "topic": "user_activity",
            "event_id": event_1_id,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "source": "mobile_app",
            "payload": {"action": "login", "user_id": 123},
        }
        resp = await client.post(f"{API_URL}/publish", json=event_1)
        print("Sent Event 1:", resp.json())


async def send_duplicate_single():
    async with httpx.AsyncClient() as client:
        print("\n--- sending duplicate single event (simulate at-least-once) ---")
        event_1_id = str(uuid.uuid4())
        event_1 = {
            "topic": "user_activity",
            "event_id": event_1_id,
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "source": "mobile_app",
            "payload": {"action": "login", "user_id": 123},
        }
        resp = await client.post(f"{API_URL}/publish", json=event_1)
        resp = await client.post(f"{API_URL}/publish", json=event_1)
        print("Sent Event 1 (Duplicate):", resp.json())


async def send_batch():
    async with httpx.AsyncClient() as client:
        print("\n--- sending batch events ---")
        batch = [
            {
                "topic": "system_metrics",
                "event_id": str(uuid.uuid4()),
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "source": "server_monitor",
                "payload": {"cpu_load": 45, "ram_usage": 60},
            },
            {
                "topic": "system_metrics",
                "event_id": str(uuid.uuid4()),
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "source": "server_monitor",
                "payload": {"cpu_load": 50, "ram_usage": 61},
            },
        ]
        resp = await client.post(f"{API_URL}/publish", json=batch)
        print("Sent Batch Events:", resp.json())


async def send_scale_test():
    async with httpx.AsyncClient(timeout=120.0) as client:
        print("\n--- sending scale test (5000 events, 20% duplicates) ---")
        total_events = 5000
        duplicate_ratio = 0.2
        num_duplicates = int(total_events * duplicate_ratio)
        num_unique = total_events - num_duplicates

        events = []
        for i in range(num_unique):
            events.append(
                {
                    "topic": "scale_topic",
                    "event_id": f"scale-{i}",
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "source": "scale-test",
                    "payload": {},
                }
            )

        for i in range(num_duplicates):
            events.append(
                {
                    "topic": "scale_topic",
                    "event_id": f"scale-{i}",
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "source": "scale-test",
                    "payload": {},
                }
            )

        batch_size = 500
        for i in range(0, total_events, batch_size):
            batch = events[i : i + batch_size]
            resp = await client.post(f"{API_URL}/publish", json=batch)
            print(f"Sent batch {i} to {i + len(batch)}, status: {resp.status_code}")


async def check_stats():
    async with httpx.AsyncClient() as client:
        print("\n--- system stats ---")
        stats_resp = await client.get(f"{API_URL}/stats")
        print(stats_resp.json())


async def check_events():
    async with httpx.AsyncClient() as client:
        print("\n--- events by topic 'user_activity' ---")
        events_resp = await client.get(f"{API_URL}/events?topic=user_activity")
        print(events_resp.json())


async def main():
    # await send_single()
    # await send_duplicate_single()
    # await send_batch()
    # await send_duplicate_batch()
    # await asyncio.sleep(1)
    # await check_stats()
    # await check_events()
    pass


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        func = globals().get(command)
        if callable(func) and asyncio.iscoroutinefunction(func):
            asyncio.run(func())
        else:
            print(f"unknown command: {command}")
    else:
        asyncio.run(main())
