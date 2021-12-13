# Create Kafka Connect image with Scalyr Sink Connector
FROM confluentinc/cp-kafka-connect:6.1.0-1-ubi8
RUN mkdir -p /etc/kafka-connect/jars/kafka-connect-scalyr-sink
COPY target/kafka-connect-scalyr-sink-latest-package/share/java/kafka-connect-scalyr-sink /etc/kafka-connect/jars/kafka-connect-scalyr-sink
