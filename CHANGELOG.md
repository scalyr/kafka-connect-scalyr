# Kafka Connect Scalyr Sink Connector Changes by Release

## 1.6
* Address Log4J Arbitrary Code Execution exploit by upgrading to log4j v2.17.0. For more information on the vulnerability
  see upstream log4j CVE https://nvd.nist.gov/vuln/detail/CVE-2021-45105.

## 1.5

* Address Log4J Arbitrary Code Execution exploit by upgrading to log4j v2.16.0. For more information on the vulnerability
  see upstream log4j CVE https://nvd.nist.gov/vuln/detail/CVE-2021-45046.

## 1.4

* Address Log4J Arbitrary Code Execution exploit by upgrading to log4j v2.15.0. For more information on the vulnerability
  see upstream log4j CVE https://nvd.nist.gov/vuln/detail/CVE-2021-44228.

## 1.3

Features
* Regular expression support for custom application event mapping `matcher.value`.
* `matchAll` support for custom application event mapping matcher.

## 1.2

* Allow not specifying application attribute fields in custom application event mappings when `send_entire_record` is `true`.
* Change default `batch_send_size_bytes` to 5 MB.

## 1.1
Performance improvements for JSON serialization.

## 1.0

Initial Release

Features:
* Supports Elastic Filebeat log messages with automatic conversion of Filebeat log messages to Scalyr log events.
* Supports custom application log messages using user defined conversion of message fields to Scalyr log event attributes.
* Supports Fluentd and Fluent Bit using custom application event mappings.
* Exactly once delivery using the topic, partition, and offset to uniquely identify events and prevent duplicate delivery.
