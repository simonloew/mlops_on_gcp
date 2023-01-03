from typing import NamedTuple, Optional
from datetime import datetime

from feature_pipeline.helpers import named_tuple_to_avro_fields


class Flight(NamedTuple):
    timestamp: Optional[datetime]
    flight_number: str
    origin_airport_id: str
    is_cancelled: bool
    departure_delay_minutes: float
    arrival_delay_minutes: float
    taxi_out_minutes: float
    distance_miles: float


flight_avro_schema = {
    "namespace": "flight_delay_prediction",
    "type": "record",
    "name": "Flight",
    "fields": named_tuple_to_avro_fields(Flight),
}


class AirportFeatures(NamedTuple):
    timestamp: Optional[datetime]
    origin_airport_id: str
    average_departure_delay: float


airport_avro_schema = {
    "namespace": "flight_delay_prediction",
    "type": "record",
    "name": "Airport",
    "fields": named_tuple_to_avro_fields(AirportFeatures),
}
