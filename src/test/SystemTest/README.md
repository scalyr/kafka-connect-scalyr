# System Test Setup
Docker is used to setup a multi-container System test environment with the following containers:
1. Zookeeper
2. Kafka with topics for logs
3. Kafka Connect running in Distributed Mode with Scalyr Sink Connector installed
4. Filebeat configured to send logs from flog containers to Kafka
5. flog to send fake log events to Filebeat
6. Simulated custom application writing log events to a Kafka topic
7. Fluentd configured to send logs to Kafka
8. flog to send fake log events to Fluentd using Docker Fluentd logging driver 
9. Fluent Bit to send cpu usage to Kafka

System tests are automatically run by Circle CI.

Perform the following from the project root directory to run system tests manually:

```
# Set environment variables
export WRITE_API_KEY=...
export READ_API_KEY=...
export CIRCLE_BUILD_NUM=0

# Build Docker images with latest code
docker-compose build

# Start containers
docker-compose up -d

# View the containers started
docker-compose ps

# Configure Scalyr Sink Connector to run in distributed worker configuration
.circleci/configure_scalyr_connector.sh
 
# Verify logs are in Scalyr
Filebeat:   python .circleci/verify_scalyr_events.py dataset=\'accesslog\'
Custom app: python .circleci/verify_scalyr_events.py app=\'customApp\'
Fluentd:    python .circleci/verify_scalyr_events.py tag=\'fluentd-apache\'
Fluent Bit: python .circleci/verify_scalyr_events.py tag=\'fluentbit-cpu\' 50

# Shutdown the Docker test env
docker-compose down
```

Events can be queried with the query: `origin='kafka-connect-build-0'`.
