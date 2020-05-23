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

# Create Distributed mode Kafka Connect Scalyr Sink Connector properties
# Substitutes $WRITE_API_KEY and $CIRCLE_BUILD_NUM from Circle CI build env
cat << EOF
{
  "name": "scalyr-sink-connector",
  "config": {
    "connector.class": "com.scalyr.integrations.kafka.ScalyrSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable":"false",
    "tasks.max": "3",
    "topics": "logs",
EOF
echo "    \"api_key\": \"$WRITE_API_KEY\","
echo "    \"event_enrichment\": \"origin=kafka-connect-build-$CIRCLE_BUILD_NUM\""
echo "  }  }"
