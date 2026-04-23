import httpx
import uuid
import datetime
import asyncio
import os

API_URL = os.getenv("API_URL", "http://localhost:8000")


async def main():
    async with httpx.AsyncClient() as client:
        await client.get(f"{API_URL}/stats")

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

        print("\n--- sending duplicate single event (simulate at-least-once) ---")
        resp = await client.post(f"{API_URL}/publish", json=event_1)
        print("Sent Event 1 (Duplicate):", resp.json())

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

        print("\n--- sending duplicate batch event ---")
        resp = await client.post(f"{API_URL}/publish", json=batch)
        print("Sent Batch Events (Duplicate):", resp.json())

        await asyncio.sleep(1)

        print("\n--- system stats ---")
        stats_resp = await client.get(f"{API_URL}/stats")
        print(stats_resp.json())

        print("\n--- events by topic 'user_activity' ---")
        events_resp = await client.get(f"{API_URL}/events?topic=user_activity")
        print(events_resp.json())


if __name__ == "__main__":
    asyncio.run(main())
