from kafka import KafkaConsumer
import logging
import os
import json
import random

class DataGenerator:
    def __init__(self):
        self.logger = logging.getLogger()
        #self.app = web.application(("/(.*)", "echo"), globals())
        if 'KAFKA_BROKERS' in os.environ:
            kafka_brokers = os.environ['KAFKA_BROKERS']
        else:
            raise ValueError('KAFKA_BROKERS environment variable not set')

        if 'TOPIC' in os.environ:
            topic = os.environ['TOPIC']
        else:
            raise ValueError('TOPIC environment variable not set')

        self.logger.info("Initializing Kafka Producer")
        print("KAFKA_BROKERS={0}".format(kafka_brokers))
        self.myKafkaConsumer = KafkaConsumer(topic, bootstrap_servers=kafka_brokers, client_id=topic)

    def run(self):
        while True:
            self.logger.info("Consuming data")
            for msg in self.myKafkaConsumer:
                assert isinstance(msg.value, bytes)
                return json.loads(json.dumps(msg.value.decode("utf-8")))

    def write_to_csv(self, kafka_dict, path, filename):
        self.logger.info("Writing data to CSV")
        val_list = []
        is_anomaly = [0, 1]
        kafka_dict['is_anomaly'] = random.choice(is_anomaly)
        try:
            with open(path + '\\' + filename, 'a') as csv_file:
                for key, value in kafka_dict.items():
                    if type(value) is list:
                        print(value)
                        value = "\"" + str(value) + "\""
                    val_list.append(value)

                print(','.join(str(v) for v in val_list))
                csv_file.write(','.join(str(v) for v in val_list) + '\n')
        except IOError:
            with open(path + '\\' + filename, 'w') as csv_file:
                for key, value in kafka_dict.items():
                    if type(value) is list:
                        value = "\"" + str(value) + "\""
                    val_list.append(value)
                csv_file.write(','.join(str(v) for v in val_list))


