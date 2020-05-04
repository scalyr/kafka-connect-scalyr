# Kafka Connect Scalyr Sink Plugin

# Building
```
mvn clean package
```
Kafka Connect Scalyr will be packaged as a zip file in `target/components/packages/scalyr-kafka-connect-scalyr-1.0-SNAPSHOT.zip`.

# Installation
Unzip the Kafka Connect Scalyr plugin into the Kafka plugin directory.
More information about plugin installation can be found here: [Connect Installing Plugins](https://docs.confluent.io/current/connect/userguide.html#connect-installing-plugins)

#Kafka Configuration
## Scalyr Sink Config file
Copy the sample Scalyr Sink Config File from the unzipped plugin etc directory to the Kafka config directory: `cp etc/connector-scalyr-sink.properties $KAFKA/config`
and modify as needed.  At a minimum, fields that may require changing are `topic`, `scalyr_server`, `api_key`.   

## `$KAFKA/config/connect-standalone.properties`
Modify the `plugin.path` to include the parent directory of the `kafka-connect-scalyr` plugin directory.
bin/connect-standalone.sh config/connect-standalone.properties config/connect-scalyr-sink.properties
#Running in Development Standalone Mode
```
cd $KAFKA
bin/connect-standalone.sh config/connect-standalone.properties config/connect-scalyr-sink.properties
```
