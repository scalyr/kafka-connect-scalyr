# System Test Setup
Docker is used to setup a multi-container System test environment with the following containers:
1. Zookeeper
2. Kafka with topics for logs and Kafka Connect running in distributed mode
3. Kafka Connect with Scalyr Sink Connector installed
4. Filebeat configured to send logs from flog containers to Kafka
5. flog to generate fake log events

System tests are automatically run by Circle CI.

Perform the following from the project root directory to run system tests manually:

```
# Set environment variables
export WRITE_API_KEY=...
export READ_API_KEY=...
export CIRCLE_BUILD_NUM=0

# Start containers
docker-compose up -d

# View the containers started
docker-compose ps

# Wait a minute until all the services are running in the containers

# Create Scalyr Sink Connector
# Create connector json with write api key substitutes
.circleci/create_connector_json.sh > /tmp/scalyr_connector.json
docker run --network container:connect appropriate/curl --retry 10 --retry-connrefused \
  -H "Content-Type: application/json" -H "Accept: application/json"  http://connect:8088/connectors \
  -d "`cat /tmp/scalyr_connector.json`"

# Verify Scalyr Sink Connector is created
docker run  --network container:connect appropriate/curl --retry 10 --retry-connrefused \
  -H "Content-Type: application/json" -H "Accept: application/json"  http://connect:8088/connectors \
  | grep scalyr-sink-connector
   
# Verify logs are in Scalyr
python .circleci/verify_scalyr_events.py

# Shutdown the Docker test env
docker-compose down
```

Events can be queried with the query: `origin='kafka-connect-build-0'`.
