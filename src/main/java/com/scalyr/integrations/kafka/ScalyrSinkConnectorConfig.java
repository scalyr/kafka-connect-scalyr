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

import com.google.common.base.Strings;
import com.scalyr.integrations.kafka.mapping.CustomAppEventMapping;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Scalyr Sink Connector Configuration
 * Specifies the valid configuration parameters for the Scalyr Sink Connector.
 */
public class ScalyrSinkConnectorConfig extends AbstractConfig {

  public static final String PROD_SCALYR_SERVER = "https://app.scalyr.com";
  public static final String EU_SCALYR_SERVER = "https://upload.eu.scalyr.com";
  public static final String DEFAULT_COMPRESSION_TYPE = "deflate";
  public static final int DEFAULT_ADD_EVENTS_TIMEOUT_MS = 20_000;
  public static final int DEFAULT_ADD_EVENTS_RETRY_DELAY_MS = 500;
  public static final int DEFAULT_BATCH_SEND_SIZE_BYTES = 4_000_000;
  public static final int DEFAULT_BATCH_SEND_WAIT_MS = 5000;

  public static final String SCALYR_SERVER_CONFIG = "scalyr_server";
  private static final String SCALYR_SERVER_DOC = "Scalyr server URL to send logs to.  If not specified, '" + PROD_SCALYR_SERVER + "' is used";
  public static final String SCALYR_API_CONFIG = "api_key";
  private static final String SCALYR_API_DOC = "'Write Logs' api key for your account. These are available at https://www.scalyr.com/keys";
  public static final String COMPRESSION_TYPE_CONFIG = "compression_type";
  private static final String COMPRESSION_TYPE_DOC = "Compression type to use for sending log events.  Valid values are: deflate, none";
  public static final String COMPRESSION_LEVEL_CONFIG = "compression_level";
  private static final String COMPRESSION_LEVEL_DOC = "Compression level for the compression_type.  Valid values depend on the compression_type.  Default will be used if not specified.";
  public static final String ADD_EVENTS_TIMEOUT_MS_CONFIG = "add_events_timeout_ms";
  private static final String ADD_EVENTS_TIMEOUT_MS_DOC = "Timeout in milliseconds for Scalyr add events call.";
  public static final String ADD_EVENTS_RETRY_DELAY_MS_CONFIG = "add_events_retry_delay_ms";
  private static final String ADD_EVENTS_RETRY_DELAY_MS_DOC = "Delay in milliseconds for initial add events retry.  This delay is increased exponentially for each retry.";
  public static final String EVENT_ENRICHMENT_CONFIG = "event_enrichment";
  private static final String EVENT_ENRICHMENT_DOC = "Additional attributes to add the the Scalyr log event specified as a comma separated list of key value pairs.  "
    + "All events uploaded by this connector will have these attributes.  Values should not have any spaces.";
  public static final String BATCH_SEND_SIZE_BYTES_CONFIG = "batch_send_size_bytes";
  private static final String BATCH_SEND_SIZE_BYTES_DOC = "Batch size that must be reached before events are sent.  This is to buffer events into larger batches for increased throughput.";
  public static final String BATCH_SEND_WAIT_MS_CONFIG = "batch_send_wait_ms";
  private static final String BATCH_SEND_WAIT_MS_DOC = "Maximum time to wait in millisecs between batch sends."
    + "  This ensures events are sent to Scalyr in a timely manner on systems under light load where "
    + BATCH_SEND_SIZE_BYTES_CONFIG + " may not be reached for longer periods of time.";
  public static final String CUSTOM_APP_EVENT_MAPPING_CONFIG = "custom_app_event_mapping";
  private static final String CUSTOM_APP_EVENT_MAPPING_DOC = "JSON config describing how to map custom application nested Kafka messages to Scalyr events." +
    "  Multiple custom application event mappings can be specified in a JSON list.  Example config JSON:\n"
    + "[{\"matcher\": { \"attribute\": \"app.name\", \"value\": \"customApp\"},\n" +
    " \"eventMapping\": { \"message\": \"message\", \"logfile\": \"log.path\", \"serverHost\": \"host.hostname\", \"parser\": \"fields.parser\", \"version\": \"app.version\"} }]";
  public static final String SEND_ENTIRE_RECORD = "send_entire_record";
  private static final String SEND_ENTIRE_RECORD_DOC = "If true, send the entire Kafka Connect record value serialized to JSON as the message field.";

  public ScalyrSinkConnectorConfig(Map<String, String> parsedConfig) {
    super(configDef(), parsedConfig);
  }

  public static ConfigDef configDef() {
    return new ConfigDef()
        .define(SCALYR_SERVER_CONFIG, Type.STRING, PROD_SCALYR_SERVER, scalyrServerValidator, Importance.HIGH, SCALYR_SERVER_DOC)
        .define(SCALYR_API_CONFIG, Type.PASSWORD, Importance.HIGH, SCALYR_API_DOC)
        .define(EVENT_ENRICHMENT_CONFIG, Type.LIST, null, enrichmentValidator, Importance.LOW, EVENT_ENRICHMENT_DOC)
        .define(COMPRESSION_TYPE_CONFIG,  Type.STRING, DEFAULT_COMPRESSION_TYPE,
          ConfigDef.ValidString.in(CompressorFactory.SUPPORTED_COMPRESSION_NAMES.toArray(new String[0])), Importance.LOW, COMPRESSION_TYPE_DOC)
        .define(COMPRESSION_LEVEL_CONFIG, Type.INT, null, Importance.LOW, COMPRESSION_LEVEL_DOC)
        .define(ADD_EVENTS_TIMEOUT_MS_CONFIG, Type.INT, DEFAULT_ADD_EVENTS_TIMEOUT_MS, ConfigDef.Range.atLeast(2000), Importance.LOW, ADD_EVENTS_TIMEOUT_MS_DOC)
        .define(ADD_EVENTS_RETRY_DELAY_MS_CONFIG, Type.INT, DEFAULT_ADD_EVENTS_RETRY_DELAY_MS, ConfigDef.Range.atLeast(100), Importance.LOW, ADD_EVENTS_RETRY_DELAY_MS_DOC)
        .define(BATCH_SEND_SIZE_BYTES_CONFIG, Type.INT, DEFAULT_BATCH_SEND_SIZE_BYTES, ConfigDef.Range.between(500_000, 5_500_000), Importance.LOW, BATCH_SEND_SIZE_BYTES_DOC)
        .define(BATCH_SEND_WAIT_MS_CONFIG, Type.INT, DEFAULT_BATCH_SEND_WAIT_MS, ConfigDef.Range.atLeast(1000), Importance.LOW, BATCH_SEND_WAIT_MS_DOC)
        .define(CUSTOM_APP_EVENT_MAPPING_CONFIG, Type.STRING, null, customAppEventMappingValidator, Importance.MEDIUM, CUSTOM_APP_EVENT_MAPPING_DOC)
        .define(SEND_ENTIRE_RECORD, Type.BOOLEAN, false, Importance.LOW, SEND_ENTIRE_RECORD_DOC);
  }


  /**
   * Validator for Scalyr server.  https must be used for scalyr.com.  localhost is used for testing.
   */
  private static final ConfigDef.Validator scalyrServerValidator = (name, value) -> {
    String server = (String)value;
    if (!((server.startsWith("https://") && server.endsWith("scalyr.com")) || server.startsWith("http://localhost"))) {
      throw new ConfigException("Valid Scalyr server URLs include " + PROD_SCALYR_SERVER + ", " + EU_SCALYR_SERVER);
    }
  };

  /**
   * Validator for EVENT_ENRICHMENT_CONFIG
   * Validate key=value format
   * Validate no spaces in key or value
   */
  private static final ConfigDef.Validator enrichmentValidator = (name, value) -> {
    if (value == null) {
      return;
    }

    for (String keyValue : (List<String>)value) {
      if (keyValue.indexOf('=') <= 0) {
        throw new ConfigException("Enrichment value must be key=value");
      }

      if (keyValue.indexOf(' ') >= 0) {
        throw new ConfigException("Enrichment value cannot have spaces");
      }
    }
  };

  /**
   * Validator for CUSTOM_APP_EVENT_MAPPING_CONFIG.
   * Verifies custom app event mapping JSON is valid and contains required fields.
   */
  private static final ConfigDef.Validator customAppEventMappingValidator = (name, value) -> {
    if (value == null) {
      return;
    }

    try {
      List<CustomAppEventMapping> customAppEventMappings = CustomAppEventMapping.parseCustomAppEventMappingConfig((String) value);
      if (customAppEventMappings.isEmpty()) {
        throw new ConfigException("No custom event mappings are defined");
      }
      for (CustomAppEventMapping mapping : customAppEventMappings) {
        if (mapping.getMatcherFields().isEmpty() || Strings.isNullOrEmpty(mapping.getMatcherValue())) {
          throw new ConfigException("Custom event application mapping matcher not defined");
        }

        if (mapping.getMessageFields().isEmpty() && mapping.getAdditionalAttrFields().isEmpty()) {
          throw new ConfigException("Either message field or application attribute fields must be defined");
        }
      }
    } catch (IOException | IllegalArgumentException e) {
      throw new ConfigException("Invalid custom application event mapping JSON", e);
    }
  };
}
