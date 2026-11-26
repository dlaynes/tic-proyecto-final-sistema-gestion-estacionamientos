"""Simulador de sensores ultrasonicos HC-SR04 para espacios de estacionamiento.

El script reemplaza la lectura fisica del sensor por distancias simuladas:
- Vehiculo presente: distancia conocida y menor al umbral.
- Espacio libre: distancia mayor al umbral.

Cada lectura se publica en JSON por MQTT, igual que un sensor real.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt

import urllib.request
import urllib.error

DEFAULT_BROKER = os.getenv("MQTT_BROKER", "localhost")
DEFAULT_PORT = int(os.getenv("MQTT_PORT", "1883"))
DEFAULT_PARKING_ID = os.getenv("PARKING_ID", "estacionamiento-central")
DEFAULT_SPOTS = int(os.getenv("SPOTS", "8"))
DEFAULT_INTERVAL = float(os.getenv("INTERVAL_SECONDS", "5"))
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")

OCCUPIED_THRESHOLD_CM = 30.0
SENSOR_MODEL = "HC-SR04"
TOPIC_PREFIX = "iot"


@dataclass
class SpotMeasurement:
    """Lectura de un espacio de estacionamiento."""

    parking_id: str
    spot_id: str
    timestamp: str
    occupied: bool
    distance_cm: float
    threshold_cm: float
    sensor_model: str
    event: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "parking_id": self.parking_id,
                "spot_id": self.spot_id,
                "timestamp": self.timestamp,
                "occupied": self.occupied,
                "distance_cm": round(self.distance_cm, 2),
                "threshold_cm": self.threshold_cm,
                "sensor_model": self.sensor_model,
                "event": self.event,
            },
            ensure_ascii=False,
        )


class ParkingSpotSimulator:
    """Simula el comportamiento fisico de un sensor HC-SR04 por espacio."""

    def __init__(
        self,
        spot_id: str,
        parking_id: str,
        rng: Optional[random.Random] = None,
        threshold_cm: float = OCCUPIED_THRESHOLD_CM,
        initial_occupied: Optional[bool] = None,
        p_becomes_occupied: float = 0.18,
        p_becomes_free: float = 0.06,
    ) -> None:
        self.parking_id = parking_id
        self.spot_id = spot_id
        self.threshold_cm = threshold_cm
        self._rng = rng or random.Random()
        self._p_becomes_occupied = p_becomes_occupied
        self._p_becomes_free = p_becomes_free
        self._occupied = (
            self._rng.random() < 0.35 if initial_occupied is None else initial_occupied
        )

    def sample(self, previous: Optional[SpotMeasurement] = None) -> SpotMeasurement:
        was_occupied = self._update_state(previous.occupied if previous else None)
        distance = self._distance_for_state(was_occupied)
        event = "unchanged"

        if previous is not None:
            if not previous.occupied and was_occupied:
                event = "occupied"
            elif previous.occupied and not was_occupied:
                event = "freed"

        return SpotMeasurement(
            parking_id=self.parking_id,
            spot_id=self.spot_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            occupied=was_occupied,
            distance_cm=round(distance, 2),
            threshold_cm=self.threshold_cm,
            sensor_model=SENSOR_MODEL,
            event=event,
        )

    def _update_state(self, previous: Optional[bool]) -> bool:
        if previous is not None:
            if previous and self._rng.random() < self._p_becomes_free:
                self._occupied = False
            elif not previous and self._rng.random() < self._p_becomes_occupied:
                self._occupied = True
        return self._occupied

    def _distance_for_state(self, occupied: bool) -> float:
        """Genera una distancia creible para el sensor ultrasonico."""
        if occupied:
            distance = self._rng.uniform(8.0, 24.0) + self._rng.gauss(0.0, 1.5)
            return max(4.0, min(24.0, distance))
        distance = self._rng.uniform(40.0, 90.0) + self._rng.gauss(0.0, 2.0)
        return max(38.0, min(110.0, distance))


def build_topic(parking_id: str, spot_id: str) -> str:
    return f"{TOPIC_PREFIX}/{parking_id}/espacio/{spot_id}/estado"

def send_to_n8n(webhook_url: str, payload: str) -> bool:
    """Envía una medición a n8n sin interrumpir la publicación MQTT."""
    if not webhook_url:
        return False

    try:
        data = payload.encode("utf-8")
        request = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(request, timeout=3) as response:
            print(
                f"n8n recibió el evento. HTTP {response.status}",
                flush=True,
            )
            return 200 <= response.status < 300

    except Exception as error:
        print(
            f"Error enviando evento a n8n: {error}",
            file=sys.stderr,
            flush=True,
        )
        return False

def create_spots(
    quantity: int,
    parking_id: str,
    prefix: str = "A",
    rng: Optional[random.Random] = None,
) -> list[ParkingSpotSimulator]:
    if quantity < 1:
        raise ValueError("La cantidad de espacios debe ser al menos 1")
    return [
        ParkingSpotSimulator(
            f"{prefix}-{number:02d}",
            parking_id,
            rng=rng,
        )
        for number in range(1, quantity + 1)
    ]


def create_client(
    broker: str,
    port: int,
    client_id: str,
    connect: bool = True,
) -> mqtt.Client:
    """Crea y conecta el cliente MQTT."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    if connect:
        client.connect(broker, port, keepalive=60)
        client.loop_start()
    return client


def publish_measurements(
    client: mqtt.Client,
    parking_id: str,
    spots: list[ParkingSpotSimulator],
    last_measurements: dict[str, SpotMeasurement],
    webhook_url: str = "",
    last_sent_states: Optional[dict[str, bool]] = None,
) -> int:
    """Publica por MQTT y envía a n8n el estado inicial y los cambios."""
    published = 0

    if last_sent_states is None:
        last_sent_states = {}

    for spot in spots:
        previous = last_measurements.get(spot.spot_id)
        measurement = spot.sample(previous)
        payload = measurement.to_json()
        topic = build_topic(parking_id, measurement.spot_id)

        result = client.publish(topic, payload, qos=0, retain=True)

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            published += 1
            last_measurements[spot.spot_id] = measurement

            print(f"Publicando -> {topic} {payload}", flush=True)

            previous_sent_state = last_sent_states.get(spot.spot_id)

            if previous_sent_state is None or previous_sent_state != measurement.occupied:
                if send_to_n8n(webhook_url, payload):
                    last_sent_states[spot.spot_id] = measurement.occupied

        else:
            print(
                f"Error MQTT rc={result.rc} en {topic}",
                file=sys.stderr,
                flush=True,
            )

    return published

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Host MQTT")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Puerto MQTT")
    parser.add_argument("--parking-id", default=DEFAULT_PARKING_ID)
    parser.add_argument("--spots", type=int, default=DEFAULT_SPOTS)
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL,
        help="Segundos entre lecturas",
    )
    parser.add_argument("--once", action="store_true", help="Publicar una sola vez")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.spots < 1:
        print("La cantidad de espacios debe ser al menos 1", file=sys.stderr)
        return 2
    if args.interval <= 0:
        print("El intervalo debe ser mayor que 0", file=sys.stderr)
        return 2

    running = True

    def stop(signal_number, frame):
        nonlocal running
        del signal_number, frame
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    spots = create_spots(args.spots, args.parking_id)
    client = create_client(
        args.broker,
        args.port,
        client_id=f"hc-sr04-{args.parking_id}",
    )
    last_measurements: dict[str, SpotMeasurement] = {}
    last_sent_states: dict[str, bool] = {}

    try:
        while running:
            publish_measurements(
    client,
    args.parking_id,
    spots,
    last_measurements,
    webhook_url=N8N_WEBHOOK_URL,
    last_sent_states=last_sent_states,
)
            if args.once:
                break
            for _ in range(int(args.interval * 10)):
                if not running:
                    break
                time.sleep(0.1)
    finally:
        client.loop_stop()
        client.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
