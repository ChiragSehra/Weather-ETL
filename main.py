import prefect
from prefect import task, Flow
import time
from datetime import timedelta
from prefect.schedules import IntervalSchedule
import requests
import json
from elasticsearch import Elasticsearch
from datetime import datetime
import config

# Create the client instance
client = Elasticsearch(
    cloud_id=config.CLOUD_ID,
    basic_auth=("elastic", config.ELASTIC_PASSWORD)
)

current_date = datetime.now().date()
data = requests.get("https://ipinfo.io").json()
location = data["loc"].split(",")
lat, lon = location[0], location[1]
url = "https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&appid=%s&units=metric" % (lat, lon, config.API_KEY)
schedule = IntervalSchedule(interval=timedelta(minutes=1))

# Mapping of current temperature data from openweather API
current_mapping = {
    "mappings": {
        "properties": {
            "@timestamp": {
                "type": "date"
            },
            "clouds": {
                "type": "long"
            },
            "dew_point": {
                "type": "double"
            },
            "dt": {
                "type": "date",
                "format": "epoch_second"
            },
            "feels_like": {
                "type": "double"
            },
            "humidity": {
                "type": "long"
            },
            "pressure": {
                "type": "long"
            },
            "rain.1h": {
                "type": "double"
            },
            "sunrise": {
                "type": "date",
                "format": "epoch_second"
            },
            "sunset": {
                "type": "date",
                "format": "epoch_second"
            },
            "temp": {
                "type": "double"
            },
            "uvi": {
                "type": "double"
            },
            "visibility": {
                "type": "long"
            },
            "weather.description": {
                "type": "keyword"
            },
            "weather.icon": {
                "type": "keyword"
            },
            "weather.id": {
                "type": "long"
            },
            "weather.main": {
                "type": "keyword"
            },
            "wind_deg": {
                "type": "long"
            },
            "wind_gust": {
                "type": "double"
            },
            "wind_speed": {
                "type": "double"
            }
        }
    }
}


@task
def openweather_api_call():
    response = requests.get(url)
    return response


@task
def transformation(api_response):
    data_response = json.loads(api_response.text)
    current_data = data_response['current']
    return current_data


@task
def load_to_es(index_data, index_name, index_mapping):
    # Creation of index
    client.indices.create(index=index_name + "-" + str(current_date), ignore=404, body=index_mapping)

    # Pushing records to elasticsearch
    client.index(index=index_name + "-" + str(current_date), id=str(int(time.time())), document=index_data)


with Flow("openapi_call", schedule=schedule) as flow:
    openweather_api_response = openweather_api_call()
    current_temperature_data = transformation(openweather_api_response)
    load_to_es(index_data=current_temperature_data, index_name="current_weather", index_mapping=current_mapping)

flow.run()
