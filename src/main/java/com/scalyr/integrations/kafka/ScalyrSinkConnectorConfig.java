package com.scalyr.integrations.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * Scalyr Sink Connector Configuration
 * Specifies the valid configuration parameters for the Scalyr Sink Connector.
 */
public class ScalyrSinkConnectorConfig extends AbstractConfig {

  public static final String DEFAULT_SCALYR_SERVER = "https://app.scalyr.com";
  public static final String DEFAULT_LOG_FIELDS = "message";
  public static final String DEFAULT_PARSER = "kafkaParser";

  public static final String SCALYR_SERVER_CONFIG = "scalyr_server";
  private static final String SCALYR_SERVER_DOC = "Scalyr server URL to send logs to.  If not specified, 'https://app.scalyr.com' is used";
  public static final String SCALYR_API_CONFIG = "api_key";
  private static final String SCALYR_API_DOC = "'Write Logs' api key for your account. These are available at https://www.scalyr.com/keys";
  public static final String LOG_FIELDS_CONFIG = "log_fields";
  private static final String LOG_FIELDS_DOC = "Comma separated list of log fields to send to Scalyr";
  public static final String PARSER_CONFIG = "parser";
  private static final String PARSER_DOC = "Name of parser in Scalyr for parsing log messages";
  public static final String SESSION_ID_CONFIG = "session";
  private static final String SESSION_ID_DOC = "Uniquely identifies the Connector instance, which corresponds with a Scalyr session.  Value is created by the connector and does not need to be specified in the config file.";




  public ScalyrSinkConnectorConfig(Map<String, String> parsedConfig) {
    super(configDef(), parsedConfig);
  }

  public static ConfigDef configDef() {
    return new ConfigDef()
        .define(SCALYR_SERVER_CONFIG, Type.STRING, DEFAULT_SCALYR_SERVER, Importance.HIGH, SCALYR_SERVER_DOC)
        .define(SCALYR_API_CONFIG, Type.PASSWORD, Importance.HIGH, SCALYR_API_DOC)
        .define(LOG_FIELDS_CONFIG, Type.LIST, DEFAULT_LOG_FIELDS, Importance.HIGH, LOG_FIELDS_DOC)
        .define(PARSER_CONFIG, Type.STRING, DEFAULT_PARSER, Importance.HIGH, PARSER_DOC)
        .define(SESSION_ID_CONFIG, Type.STRING, Importance.LOW, SESSION_ID_DOC);
  }
}
