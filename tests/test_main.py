import pytest
import os
import uuid
import datetime
import time

os.environ["DATABASE_PATH"] = "/tmp/test_events.db"

from fastapi.testclient import TestClient
from src.main import app


@pytest.fixture(autouse=True)
def cleanup_db():
    if os.path.exists("/tmp/test_events.db"):
        os.remove("/tmp/test_events.db")

    yield

    if os.path.exists("/tmp/test_events.db"):
        os.remove("/tmp/test_events.db")


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


def test_small_stress_batch_time():
    with TestClient(app) as client:
        batch_size = 100
        events = []
        for i in range(batch_size):
            events.append(
                {
                    "topic": "stress_topic",
                    "event_id": f"stress-{i}",
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                    "source": "test",
                    "payload": {},
                }
            )

        start_time = time.time()
        resp = client.post("/publish", json=events)
        elapsed_publish = time.time() - start_time

        assert resp.status_code == 200
        assert elapsed_publish < 1.0

        time.sleep(1.0)

        stats = client.get("/stats").json()
        assert stats["received"] >= batch_size
        assert stats["unique_processed"] >= batch_size
