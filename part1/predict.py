from google.cloud import aiplatform as aip

# TODO: Replace the project and endpoint id with your own
PROJECT_ID = "XXX"
ENDPOINT_ID = "XXX"
REGION = "europe-west1"

aip.init(project=PROJECT_ID, location=REGION)
endpoint = aip.Endpoint(ENDPOINT_ID)
prediction = endpoint.predict(instances=[[-4.0, 16.0, 153.0]])

print("PREDICTION:", prediction)
