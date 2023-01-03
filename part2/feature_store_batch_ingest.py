from google.cloud import aiplatform as aip

from .config import PROJECT_ID, BUCKET, REGION, FEATURE_STORE_ID


aip.init(project=PROJECT_ID, staging_bucket=BUCKET, location=REGION)

flight_delays_feature_store = aip.Featurestore(
    FEATURE_STORE_ID,
    project=PROJECT_ID,
    location=REGION,
)

flight_entity_type = flight_delays_feature_store.get_entity_type("flight")
flight_entity_type.ingest_from_gcs(
    feature_ids=[
        "origin_airport_id",
        "is_cancelled",
        "departure_delay_minutes",
        "arrival_delay_minutes",
        "taxi_out_minutes",
        "distance_miles",
    ],
    feature_time="timestamp",
    gcs_source_uris=f"gs://{BUCKET}/features/flight_features/*",
    gcs_source_type="avro",
    entity_id_field="flight_number",
)

airport_entity_type = flight_delays_feature_store.get_entity_type("airport")
airport_entity_type.ingest_from_gcs(
    feature_ids=["average_departure_delay"],
    feature_time="timestamp",
    gcs_source_uris=f"gs://{BUCKET}/features/airport_features/*",
    gcs_source_type="avro",
    entity_id_field="origin_airport_id",
)
