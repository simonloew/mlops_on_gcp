from google.cloud import aiplatform as aip

from .config import PROJECT_ID, REGION, FEATURE_STORE_ID, ENDPOINT_ID

# This would usually come from a REST request
inputs = {
    "distance_miles": 481.0,
    "departure_delay_minutes": -6.0,
    "taxi_out_minutes": 13.0,
    "airport_id": "13851",
}

aip.init(project=PROJECT_ID, location=REGION)

flight_delays_feature_store = aip.Featurestore(
    FEATURE_STORE_ID,
    project=PROJECT_ID,
    location=REGION,
)

airport_entity_type = flight_delays_feature_store.get_entity_type("airport")

features_df = airport_entity_type.read(
    entity_ids=inputs["airport_id"], feature_ids=["average_departure_delay"]
)

print("Airport features", features_df)

endpoint = aip.Endpoint(ENDPOINT_ID)
prediction = endpoint.predict(
    instances=[
        [
            inputs["distance_miles"],
            inputs["departure_delay_minutes"],
            inputs["taxi_out_minutes"],
            features_df["average_departure_delay"].iloc[0],
        ]
    ]
)

print("PREDICTION:", prediction)
