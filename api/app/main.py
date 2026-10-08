from datetime import datetime, timezone
from uuid import uuid4
import random

from fastapi import FastAPI, Query

app = FastAPI(
    title="Mumbai Taxi Event API",
    version="1.0.0"
)

ZONES = [
    "Andheri West",
    "Andheri East",
    "Bandra West",
    "Bandra East",
    "Powai",
    "Kurla",
    "Dadar",
    "Lower Parel",
    "Worli",
    "Colaba",
    "Borivali",
    "Goregaon",
]

PAYMENT_METHODS = [
    "UPI",
    "Cash",
    "Card",
]

VEHICLE_TYPES = [
    "Taxi",
    "Premium Taxi",
]


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.post("/trips")
def generate_trips(
    count: int = Query(default=1, ge=1, le=1000)
):
    batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    events = []

    for _ in range(count):
        distance = round(random.uniform(1.5, 25.0), 2)

        duration = max(
            5,
            round(distance * random.uniform(2.5, 4.5))
        )

        fare = round(
            50 + distance * random.uniform(18, 28),
            2
        )

        event = {
            "event_id": str(uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "pickup_zone": random.choice(ZONES),
            "dropoff_zone": random.choice(ZONES),
            "distance_km": distance,
            "duration_min": duration,
            "fare_inr": fare,
            "passenger_count": random.randint(1, 4),
            "payment_method": random.choice(PAYMENT_METHODS),
            "vehicle_type": random.choice(VEHICLE_TYPES),
            "driver_id": f"DRV_{random.randint(10000, 99999)}",
            "batch_id": batch_id,
        }

        events.append(event)

    return {
        "batch_id": batch_id,
        "events_generated": len(events),
        "events": events,
    }