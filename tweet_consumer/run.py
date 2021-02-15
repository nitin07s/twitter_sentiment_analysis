from kafka import KafkaConsumer
import os
import json
import logging
import configparser
from datetime import datetime
from datetime import timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


def main():

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_DIR, 'config.ini')
    SECRET_PATH = os.path.join(ROOT_DIR, 'secrets.ini')
    print(CONFIG_PATH)

    config = configparser.ConfigParser(strict=True)
    config.read_file(open(CONFIG_PATH, 'r'))

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    bootstrap_server = config['kafka'].get('broker')
    topic = config['kafka'].get('topic')
    influxdb_url = config['influxdb'].get('url')
    influxdb_bucket = config['influxdb'].get('tweets-database')
    influxdb_org = config['influxdb'].get('org')

    config.read(SECRET_PATH)
    influxdb_token = config['influxdb'].get('token')
    consumer = KafkaConsumer(topic,
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='docker-grp-1',
                             value_deserializer=lambda m: json.loads(
                                 m.decode('utf-8')),
                             bootstrap_servers=[bootstrap_server])
    influxdb_client = InfluxDBClient(influxdb_url, influxdb_token)
    write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
    analyzer = SentimentIntensityAnalyzer()

    for msg in consumer:
        dict_data = json.loads(msg.value)
        dict_data['sentiment'] = analyzer.polarity_scores(
            dict_data["text"])['compound']
        print(dict_data)
        data_point = [{
            "measurement": influxdb_bucket,
            "tags": {
                "language": dict_data['language'],
            },
            "fields": {
                "id": dict_data['id'],
                "text": dict_data['text'],
                "is_retweet": dict_data['is_retweet'],
                "sentiment": dict_data['sentiment']
            }
        }]
        if 'country' in dict_data:
            data_point[0]['tags']['country'] = dict_data['country_code']
        if 'place_name' in dict_data:
            data_point[0]['tags']['place'] = dict_data['place_name']
        if 'latitude' in dict_data:
            data_point[0]['fields']['lat'] = dict_data['latitude']
        if 'longitude' in dict_data:
            data_point[0]['fields']['long'] = dict_data['longitude']
        print(data_point)
        write_api.write(influxdb_bucket, influxdb_org, data_point)


if __name__ == "__main__":
    main()
