/*
 * Copyright 2020 Scalyr Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scalyr.integrations.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Connect Scalyr Sink Connector
 */
public class ScalyrSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(ScalyrSinkConnector.class);
  private Map<String, String> configProps;

  @Override public String version() {
    return VersionUtil.getVersion();
  }

  /**
   * Start this Connector. This method will only be called on a clean Connector, i.e. it has
   * either just been instantiated and initialized or {@link #stop()} has been invoked.
   *
   * Validate config properties.
   */
  @Override public void start(Map<String, String> configProps) {
    // Validate config props
    try {
      new ScalyrSinkConnectorConfig(configProps);
    } catch (ConfigException e) {
      log.error("Could not start Scalyr connector due to config error", e);
      throw new ConnectException("Could not start Scalyr connector due to config error", e);
    }

    this.configProps = Collections.unmodifiableMap(configProps);
    AddEventsClient.HttpWrapper.start();
    log.info("Started ScalyrSinkConnector");
  }

  @Override public Class<? extends Task> taskClass() {
    return ScalyrSinkTask.class;
  }

  /**
   * Returns a set of configurations for Tasks based on the current configuration.
   * All ScalyrSinkTasks use the same configuration.
   *
   * @param maxTasks maximum number of configurations to generate
   * @return Individual task configurations that will be executed
   */
  @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
    return IntStream.range(0, maxTasks)
      .mapToObj(i -> new HashMap<>(configProps))
      .collect(Collectors.toList());
  }

  /**
   * Stop this connector.
   */
  @Override public void stop() {
    AddEventsClient.HttpWrapper.stop();
    log.info("Stopped ScalyrSinkConnector");
  }

  /**
   * @return The ConfigDef for this connector.
   */
  @Override public ConfigDef config() {
    return ScalyrSinkConnectorConfig.configDef();
  }
}
