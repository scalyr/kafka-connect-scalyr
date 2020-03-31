package com.scalyr.integrations.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    configProps.putIfAbsent(ScalyrSinkConnectorConfig.SESSION_ID_CONFIG, UUID.randomUUID().toString());

    // Validate config props
    try {
      new ScalyrSinkConnectorConfig(configProps);
    } catch (ConfigException e) {
      log.error("Could not start Scalyr connector due to config error", e);
      throw new ConnectException("Could not start Scalyr connector due to config error", e);
    }

    this.configProps = Collections.unmodifiableMap(configProps);
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
    //TODO: Do things that are necessary to stop your connector.  We may not need to do anything.
  }

  /**
   * @return The ConfigDef for this connector.
   */
  @Override public ConfigDef config() {
    return ScalyrSinkConnectorConfig.configDef();
  }
}
