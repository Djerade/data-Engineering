from datetime import datetime, timedelta
import json
import requests
from airflow import DAG




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_data():
    res = requests.get("https://randomuser.me/api/")
    return res.json()["results"][0]


def format_data(data):
    data_formatted = {}
    data_formatted["first_name"] = data["name"]["first"]
    data_formatted["last_name"] = data["name"]["last"]
    data_formatted["email"] = data["email"]
    data_formatted["phone"] = data["phone"]
    data_formatted["gender"] = data["gender"]
    data_formatted["age"] = data["dob"]["age"]
    data_formatted["city"] = data["location"]["city"]
    data_formatted["country"] = data["location"]["country"]
    data_formatted["postcode"] = data["location"]["postcode"]
    data_formatted["picture"] = data["picture"]["large"]
    data_formatted["location"] = {
        "city": data["location"]["city"],
        "country": data["location"]["country"],
        "postcode": data["location"]["postcode"],
        "street": data["location"]["street"],
        "state": data["location"]["state"],
        "timezone": data["location"]["timezone"],
    }
    return data_formatted

def stream_data():
    data = get_data()
    data_formatted = format_data(data)
    print(json.dumps(data_formatted, indent=2))



stream_data()