# Create Kafka Connect image with Scalyr Sink Connector
#FROM confluentinc/cp-kafka-connect-base:latest # latest build doesn't even start correctly without our connector
FROM confluentinc/cp-kafka-connect:5.5.1-1-ubi8
RUN mkdir -p /etc/kafka-connect/jars/kafka-connect-scalyr-sink
COPY target/kafka-connect-scalyr-sink-*-package/share/java/kafka-connect-scalyr-sink /etc/kafka-connect/jars/kafka-connect-scalyr-sink
