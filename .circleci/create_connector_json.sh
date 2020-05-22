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
