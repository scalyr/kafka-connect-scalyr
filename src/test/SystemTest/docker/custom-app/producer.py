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

from time import sleep
from confluent_kafka import Producer
import json
import uuid
import datetime
import socket
import os

# Create methods create simulated custom application log messages written to Kafka.
hostname = socket.gethostname()

def createRecord():
    r = {}
    r["took"] = 134
    r["timed_out"] = False
    r["_shards"] = {"total": 2, "successful": 2, "skipped": 0, "failed": 0}
    r["hits"] = createHitsField()
    r["app"] = "customApp"
    return r

def createHitsField():
    hits = {}
    hits["_type"] = "logs"
    hits["_id"] = "lms.kafka.topic_14_" + str(uuid.uuid4())
    hits["_score"] = 1.0
    hits["_routing"] = "uG9Vi6U0"
    hits["_source"] = createRecordSourceField()
    return hits

def createRecordSourceField():
    time = datetime.datetime.now()
    timeiso  = time.astimezone().isoformat()
    s = {}
    s["poc_parser_name"] =  "cef-networkvendor-web-activity"
    s["forwarder"] =  "172.16.88.171"
    s["top_domain"] = "google.com"
    s["is_threat_dest_ip"] =  False
    s["@timestamp"] = timeiso
    s["poc_activity_type"] = ["web-access"]
    s["is_tor_dest_ip"] = False
    s["poc_outcome"] = ["success"]
    s["outcome"] = "Monitor"
    s["is_phishing_domain"] = False
    s["dest_country"] = "United States of America"
    s["src_ip"] = "172.16.26.152"
    s["src_country"] = "Ukraine"
    s["is_ransomware_dest_ip"] = False
    s["dest_ip"] = "172.16.194.206"
    s["indexTime"] = timeiso
    s["Vendor"] = "NetworkVendor"
    s["web_domain"] = "clients4.google.com"
    s["data_type"] =  "web-activity"
    s["port"] = 55398
    s["dest_port"] = 443
    s["categories"] = "General, Search Engines and Portals"
    s["poc_rawEventTime"] = timeiso
    s["category"] = "General"
    s["message"] = time.strftime("%b %d %H:%M:%S ") + hostname + " pocmeta(docid=lms.kafka.topic_16_967060155_3797cd8d1153&idx=pocbeam-2020.05.20&cluster=3797cd8d1153)KAFKA_CONNECT_SYSLOG: CEF:0|NetworkVendor||1.0|Security|Internet Firewall|5|cs1=Ukraine rt=Mon May 18 14:49:40 UTC 2020 cs3=POCCustomer cs2=United States of America dst=216.58.194.206 cs5=Frankfurt cs1Label=CATOSourceCountry dproc=General, Search Engines and Portals dvc=10.41.178.89 act=Monitor cs3Label=CATOAccountName end=1589813418659 flow_unique_id=76.221.128.233 shost=Zinovii Zubko app=Google applications destinationDnsDomain=clients4.google.com cs5Label=CATOPopName src=176.100.26.152 os_version=10.15.4 start=1589813380865 dpt=443 internalType=SECURITY os_type=5 cs2Label=CATODestinationCountry dhost=clients4.google.com"
    s["Product"] = "NetworkVendor"
    s["@version"] = "1"
    s["poc_category"] = "Web"
    s["poc_device_type"] = ["web-proxy"]
    s["is_threat_src_ip"] = False
    s["user_fullname"] = "Fred Barney"
    s["is_ransomware_src_ip"] = False
    s["is_tor_src_ip"] = False
    return s

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
    def checkError(err, msg):
        if err is not None:
            print("Failed to deliver message: {}".format(err))

    while True:
        record_value = createRecord()
        #print("Producing record: {}".format(record_value))
        p.produce(topic, value=json.dumps(record_value), on_delivery=checkError)
        # p.poll() serves delivery reports (on_delivery) from previous produce() calls.
        p.poll(0)
        sleep(delay)

if __name__ == '__main__':
    main()
