import datetime
import os
import time
import pytest

from fastapi.testclient import TestClient
from src.main import app


def test_deduplication_validation():
    with TestClient(app) as client:
        event = {
            "topic": "dedup_topic",
            "event_id": "dedup-123",
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "source": "test",
            "payload": {},
        }
        client.post("/publish", json=event)
        client.post("/publish", json=event)

        time.sleep(0.5)

        stats = client.get("/stats").json()
        assert stats["received"] == 2
        assert stats["unique_processed"] == 1
        assert stats["duplicate_dropped"] == 1


def test_deduplication_persistence_simulated_restart():
    event = {
        "topic": "persist_topic",
        "event_id": "persist-123",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "test",
        "payload": {},
    }

    with TestClient(app) as client1:
        client1.post("/publish", json=event)
        time.sleep(0.5)
        stats = client1.get("/stats").json()
        assert stats["unique_processed"] == 1

    with TestClient(app) as client2:
        client2.post("/publish", json=event)
        time.sleep(0.5)
        stats2 = client2.get("/stats").json()
        assert stats2["unique_processed"] == 1
        assert stats2["duplicate_dropped"] >= 1


def test_event_schema_validation():
    with TestClient(app) as client:
        valid_event = {
            "topic": "valid_topic",
            "event_id": "valid-1",
            "timestamp": "2023-10-31T12:00:00Z",
            "source": "test",
            "payload": {},
        }
        resp1 = client.post("/publish", json=valid_event)
        assert resp1.status_code == 200

        invalid_event = {
            "topic": "invalid_topic",
            "event_id": "invalid-1",
            "source": "test",
            "payload": {},
        }
        resp2 = client.post("/publish", json=invalid_event)
        assert resp2.status_code == 422


def test_get_events_and_stats_consistency():
    with TestClient(app) as client:
        event1 = {
            "topic": "topic_a",
            "event_id": "ev-1",
            "timestamp": "2023-10-31T12:00:00Z",
            "source": "test",
            "payload": {"val": 1},
        }
        event2 = {
            "topic": "topic_a",
            "event_id": "ev-2",
            "timestamp": "2023-10-31T12:00:01Z",
            "source": "test",
            "payload": {"val": 2},
        }
        client.post("/publish", json=[event1, event2, event1])

        time.sleep(0.5)

        stats = client.get("/stats").json()
        assert stats["unique_processed"] == 2
        assert stats["duplicate_dropped"] == 1
        assert "topic_a" in stats["topics"]

        events = client.get("/events?topic=topic_a").json()
        assert len(events["events"]) == 2


def test_scale_processing_with_duplicates():
    with TestClient(app) as client:
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
            resp = client.post("/publish", json=batch)
            assert resp.status_code == 200

        max_wait = 30.0
        start_wait = time.time()
        while time.time() - start_wait < max_wait:
            stats = client.get("/stats").json()
            total_processed = stats.get("unique_processed", 0) + stats.get(
                "duplicate_dropped", 0
            )
            if stats["received"] >= total_events and total_processed >= total_events:
                break
            time.sleep(0.5)

        stats = client.get("/stats").json()
        assert stats["received"] >= total_events
        assert stats["unique_processed"] >= num_unique
        assert stats["duplicate_dropped"] >= num_duplicates
