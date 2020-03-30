# Kafka Connect Scalyr Sink Plugin

# Building
```
mvn clean package
```
Kafka Connect Scalyr plugin jars will be packaged in `target/kafka-connect-scalyr-1.0-SNAPSHOT-package/share/java`.

#Kafka Configuration
## Scalyr Sink Config file
Copy the sample Scalyr Sink Config File from the source config directory to the Kafka config directory: `cp config/connector-scalyr-sink.properties $KAFKA/config`
and modify as needed.  At a minimum, fields that may require changing are `topic`, `scalyr_server`, `api_key`, `parser`, `log_fields`.   

## `$KAFKA/config/connect-standalone.properties`
Modify the `plugin.path` to include the parent directory of the `kafka-connect-scalyr` plugin directory.
bin/connect-standalone.sh config/connect-standalone.properties config/connect-scalyr-sink.properties
#Running in Development
```
cd $KAFKA
bin/connect-standalone.sh config/connect-standalone.properties config/connect-scalyr-sink.properties
```
