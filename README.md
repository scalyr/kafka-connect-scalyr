# Kafka Connect Scalyr Sink Plugin

[![ci](https://github.com/scalyr/kafka-connect-scalyr/actions/workflows/main.yaml/badge.svg)](https://github.com/scalyr/kafka-connect-scalyr/actions/workflows/main.yaml)

This repository holds the source code for the Kafka Connect Scalyr Sink,
which is a Kafka Connect Sink Connector that allows streaming log messages from a Kafka topic to Scalyr.

## Building

```
mvn clean package
```
Kafka Connect Scalyr will be packaged as a zip file in `target/components/packages/scalyr-kafka-connect-scalyr-sink-<version>.zip`.

## Installation

Unzip the Kafka Connect Scalyr plugin into the Kafka plugin directory.
More information about plugin installation can be found here: [Connect Installing Plugins](https://docs.confluent.io/current/connect/userguide.html#connect-installing-plugins)

## Running in Standalone Mode

Standalone mode should only be used for development and testing Kafka connect on a local machine.

### Standalone Configuration
1. Copy the sample Scalyr Sink Config File from the unzipped plugin `etc` directory to the Kafka config directory: `cp etc/connector-scalyr-sink.properties $KAFKA/config`
and modify as needed.  At a minimum, fields that may require changing are `topic`, `scalyr_server`, `api_key`.   

2. Modify the `$KAFKA/config/connect-standalone.properties` `plugin.path` to include the 
parent directory of the `kafka-connect-scalyr` plugin directory.

### Running in Standalone Mode
```
cd $KAFKA
bin/connect-standalone.sh config/connect-standalone.properties config/connect-scalyr-sink.properties
```

## Running in Distributed Mode

Distributed mode should be used for all production environments.
Sample distributed mode JSON configuration files are located in the unzipped plugin `etc` directory.
Refer to the Scalyr Kafka Connector documentation for directions for running in 
[distributed mode](https://app.scalyr.com/solutions/kafka-connect#distributed-mode).

## Documentation

Please visit https://app.scalyr.com/solutions/kafka-connect

## Testing

### Unit tests
Unit tests are automatically run during `mvn clean package`.

### System Tests
Circle CI automatically runs system tests for each pull request.
An end-to-end system test environment is setup using Docker with containers for 
Zookeeper, Kafka, flog load generator for Apache logs, Filebeat, and a simulated custom application.
Events for filebeat and the custom application are sent to Scalyr and then queried to
verify the event attributes are correct.

The same end-to-end test can be run locally.  See the [System Test README.md](src/test/SystemTest/README.md)
for instructions.

## Contributing

In the future, we will be pushing guidelines on how to contribute to this repository.  For now, please just
feel free to submit pull requests to the `master` branch and we will work with you.

## Copyright, License, and Contributors Agreement

Copyright 2014-2020 Scalyr, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in
compliance with the License. You may obtain a copy of the License in the [LICENSE](LICENSE.txt) file, or at:

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

By contributing you agree that these contributions are your own (or approved by your employer) and you
grant a full, complete, irrevocable copyright license to all users and developers of the project,
present and future, pursuant to the license of the project.
