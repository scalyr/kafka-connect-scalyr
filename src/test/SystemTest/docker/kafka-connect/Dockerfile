# Create Kafka Connect image with Scalyr Sink Connector
FROM confluentinc/cp-kafka-connect-base:latest
RUN mkdir -p /etc/kafka-connect/jars/kafka-connect-scalyr-sink
COPY target/kafka-connect-scalyr-sink-0.1-package/share/java/kafka-connect-scalyr-sink/ /etc/kafka-connect/jars/kafka-connect-scalyr-sink
