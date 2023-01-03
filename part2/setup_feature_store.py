from google.cloud import aiplatform as aip

from .config import PROJECT_ID, BUCKET, REGION

aip.init(project=PROJECT_ID, staging_bucket=BUCKET, location=REGION)

flight_delays_feature_store = aip.Featurestore.create(
    "flight_delays", online_store_fixed_node_count=1
)

flight_entity_type = flight_delays_feature_store.create_entity_type(
    entity_type_id="flight",
    description="Flight entity",
)

flight_entity_type.batch_create_features(
    {
        "origin_airport_id": {
            "value_type": "STRING",
            "description": "Airport ID for the origin",
        },
        "is_cancelled": {
            "value_type": "BOOL",
            "description": "Has the flight been cancelled or diverted?",
        },
        "departure_delay_minutes": {
            "value_type": "DOUBLE",
            "description": "Departure delay in minutes",
        },
        "arrival_delay_minutes": {
            "value_type": "DOUBLE",
            "description": "Arrival delay in minutes",
        },
        "taxi_out_minutes": {
            "value_type": "DOUBLE",
            "description": "Taxi out time in minutes",
        },
        "distance_miles": {
            "value_type": "DOUBLE",
            "description": "Total flight distance in miles.",
        },
    }
)

airport_entity_type = flight_delays_feature_store.create_entity_type(
    entity_type_id="airport",
    description="Airport entity",
)

airport_entity_type.create_feature(
    feature_id="average_departure_delay",
    value_type="DOUBLE",
    description="Average departure delay for that airport, calculated every 4h with 1h rolling window",
)
