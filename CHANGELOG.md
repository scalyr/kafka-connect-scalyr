# Kafka Connect Scalyr Sink Connector Changes by Release

## 1.0
Initial Release

Features:
* Supports Elastic Filebeat log messages with automatic conversion of Filebeat log messages to Scalyr log events.
* Supports custom application log messages using user defined conversion of message fields to Scalyr log event attributes.
* Supports Fluentd and Fluent Bit using custom application event mappings.
* Exactly once delivery using the topic, partition, and offset to uniquely identify events and prevent duplicate delivery.

## 1.1
Performance improvements for JSON serialization.
