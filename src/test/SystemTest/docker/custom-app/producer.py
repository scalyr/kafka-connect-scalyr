#!/usr/bin/env python

# Copyright 2014-2020 Scalyr Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import datetime
import os
import random
import socket
from time import sleep
import uuid

from confluent_kafka import Producer

# Create methods create simulated custom application log messages written to Kafka.
hostname = socket.gethostname()

def create_record():
    time = datetime.datetime.now()
    timeiso  = time.astimezone().isoformat()
    outcome_options = ["success", "failure"]
    activity_options = ["web-access", "proxy", "ssh", "ftp"]
    country_options = ["Russia", "United Kingdom", "Greece", "China", "United States of America", "Turkey", "India", "Brazil", "Ukraine", "Germany"]
    domain_options = ["abc.com", "def.com", "xyz.com"]
    user_options = ["fred", "barney", "wilma", "pebbles", "dino", "betty", "bambam"]
    device_options = ["proxy", "edge", "frontend", "database"]
    r = {}
    r["id"] = str(uuid.uuid4())
    r["app"] = "customApp"
    r["parser_name"] =  "web-activity"
    r["metadata"] = {"@version": "1"}
    r["forwarder"] =  "172.16.88." + str(random.randrange(1, 254))
    r["domain"] = random.choice(domain_options)
    r["@timestamp"] = timeiso
    r["activity_type"] = [random.choice(activity_options)]
    r["outcome"] = [random.choice(outcome_options)]
    r["dest_country"] = random.choice(country_options)
    r["src_ip"] = "172.16.26." + str(random.randrange(1, 254))
    r["src_country"] = random.choice(country_options)
    r["dest_ip"] = "172.16.194." + str(random.randrange(1, 254))
    r["source"] = "WebScanner"
    r["src_port"] = random.randrange(1000, 65535)
    r["dest_port"] = 443
    r["category"] = {"categories": "General, Search Engines and Portals", "subcategory": "Portals"}
    r["message"] = time.strftime("%b %d %H:%M:%S ") + hostname + " PATCH /revolutionize/communities HTTP/1.0 400 71875 http://www.customerstrategic.org/roi/enhance Opera/8.13 (Macintosh; U; PPC Mac OS X 10_9_3; en-US) Presto/2.10.218 Version/12.00"
    r["device_type"] = [random.choice(device_options)]
    r["username"] = random.choice(user_options)
    r["threat"] = {"is_ransomware_src_ip": random.choice([True, False]), "is_ransomware_dest_ip":  random.choice([True, False]), "is_threat_src_ip": random.choice([True, False]), "is_threat_dest_ip": random.choice([True, False]), "is_phishing_domain": random.choice([True, False])}

    return r


def main():
    """
    Simulate a custom application writing log messages to Kafka in JSON format with no schema.
    Writes to topic 'logs' with 0.02 second delay between messages using a Kafka Producer.
    """
    # Initialization
    conf = {'bootstrap.servers': os.environ['KAFKA_SERVERS']}
    topic = 'logs'
    delay=0.02

    # Create Producer instance
    p = Producer(conf)

    # Per-message on_delivery handler triggered by poll() that logs permanently failed delivery (after retries).
    def check_error(err, msg):
        if err is not None:
            print("Failed to deliver message: {}".format(err))

    while True:
        record_value = create_record()
        #print("Producing record: {}".format(record_value))
        p.produce(topic, value=json.dumps(record_value), on_delivery=check_error)
        # p.poll() serves delivery reports (on_delivery) from previous produce() calls.
        p.poll(0)
        sleep(delay)

if __name__ == '__main__':
    main()
