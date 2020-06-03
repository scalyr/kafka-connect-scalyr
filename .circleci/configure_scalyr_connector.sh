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

# Configure Kafka Connect Scalyr Sink Connector
# Performs the following:
# 1. Wait for Kafka Connect to be ready
# 2. Create Scalyr Sink Connector distriabuted config, substituting $WRITE_API_KEY and $CIRCLE_BUILD_NUM from Circle CI build env
# 3. Configure Scalyr Sink Connector to run in Kafka Connect distributed worker configuration

SCALYR_CONNECTOR_CONFIG="/tmp/scalyr_connector.json"

# Creates Scalyr Sink Connector config JSON
function create_connector_config {
  cat << EOF > "$1"
{
  "name": "scalyr-sink-connector",
  "config": {
    "connector.class": "com.scalyr.integrations.kafka.ScalyrSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable":"false",
    "tasks.max": "3",
    "topics": "logs",
    "custom_app_event_mapping":"[{\"matcher\": {\"attribute\": \"hits._type\",\"value\": \"logs\"}, \"eventMapping\": {\"app\":\"app\", \"message\": \"hits._source.message\",\"took\": \"took\",\"timed_out\": \"timed_out\",\"shards.total\":\"_shards.total\",\"shards.successful\": \"_shards.successful\",\"shards.skipped\": \"_shards.skipped\",\"shards.failed\": \"_shards.failed\",\"type\": \"hits._type\",\"id\": \"hits._id\",\"score\": \"hits._score\",\"route\": \"hits._routing\",\"exa_parser_name\": \"hits._source.exa_parser_name\",\"forwarder\": \"hits._source.forwarder\",\"top_domain\": \"hits._source.top_domain\",\"is_threat_dest_ip\": \"hits._source.is_threat_dest_ip\",\"timestamp\": \"hits._source.@timestamp\",\"exa_activity_type\": \"hits._source.exa_activity_type\",\"is_tor_dest_ip\": \"hits._source.is_tor_dest_ip\",\"exa_outcome\": \"hits._source.exa_outcome\",\"outcome\": \"hits._source.exa_parser_name\",\"is_phishing_domain\": \"hits._source.is_phishing_domain\",\"dest_country\": \"hits._source.exa_parser_name\",\"src_ip\": \"hits._source.src_ip\",\"src_country\": \"hits._source.src_country\",\"is_ransomware_dest_ip\": \"hits._source.is_ransomware_dest_ip\",\"dest_ip\": \"hits._source.dest_ip\",\"indexTime\": \"hits._source.indexTime\",\"Vendor\": \"hits._source.Vendor\", \"web_domain\": \"hits._source.web_domain\",\"data_type\": \"hits._source.data_type\",\"port\": \"hits._source.port\",\"dest_port\": \"hits._source.dest_port\",\"categories\": \"hits._source.categories\",\"category\": \"hits._source.category\",\"Product\": \"hits._source.Product\",\"@version\": \"hits._source.@version\",\"exa_category\": \"hits._source.exa_category\",\"exa_device_type\": \"hits._source.exa_device_type\",\"is_threat_src_ip\": \"hits._source.is_threat_src_ip\",\"user_fullname\": \"hits._source.user_fullname\",\"is_ransomware_src_ip\": \"hits._source.is_ransomware_src_ip\",\"is_tor_src_ip\": \"hits._source.is_tor_src_ip\"}}]",
EOF
  echo "    \"api_key\": \"$WRITE_API_KEY\"," >> "$1"
  echo "    \"event_enrichment\": \"origin=kafka-connect-build-$CIRCLE_BUILD_NUM\"" >> "$1"
  echo "  }  }" >> "$1"
}

# Wait for Kafka Connect to be ready by checking http://connect:8088/connectors API
function wait_for_connect_ready {
  # Wait for Kafka Connect to be ready
  sleep_time=1
  max_tries=5
  i=0
  while [[ "$(docker run --network container:connect appropriate/curl --retry 10 --retry-connrefused   -H "Content-Type: application/json" -H "Accept: application/json"  -s -o /dev/null -w "%{response_code}"  http://connect:8088/connectors)"  != "200" && $i < $max_tries ]]
    do
      echo -n .
      sleep $sleep_time
      sleep_time=$((sleep_time * 2))
      i=$((i + 1))
    done
  [[ $i -eq $max_tries ]] && echo "Timeout waiting for Kafka Connect to start" && exit 1
}

# Main
# Wait for Kafka Connect to start
wait_for_connect_ready

# Configure Scalyr Sink Connector
create_connector_config $SCALYR_CONNECTOR_CONFIG # Create connector json with write api key substitutes
docker run --network container:connect appropriate/curl --retry 10 --retry-connrefused \
  -H "Content-Type: application/json" -H "Accept: application/json"  http://connect:8088/connectors \
  -d "$(cat $SCALYR_CONNECTOR_CONFIG)"

# Verify Scalyr Sink Connector is configured
docker run  --network container:connect appropriate/curl --retry 10 --retry-connrefused \
  -H "Content-Type: application/json" -H "Accept: application/json"  http://connect:8088/connectors \
  | grep scalyr-sink-connector
