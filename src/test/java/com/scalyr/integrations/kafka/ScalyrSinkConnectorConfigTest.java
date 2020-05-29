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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.scalyr.integrations.kafka.ScalyrSinkConnectorConfig.*;
import static com.scalyr.integrations.kafka.TestUtils.fails;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test ScalyrSinkConnectorConfig
 */
public class ScalyrSinkConnectorConfigTest {

  private static final String TEST_SCALYR_SERVER = "https://upload.eu.scalyr.com";
  private static final String TEST_API_KEY = "abcdef123456";
  private static final String TEST_COMPRESSION_TYPE = "none";
  private static final String TEST_COMPRESSION_LEVEL = "0";
  private static final String TEST_ADD_EVENTS_TIMEOUT = "32000";
  private static final String TEST_ADD_EVENTS_RETRY_DELAY_MS = "5000";
  private static final String TEST_EVENT_ENRICHMENT = "env=qa,org=Scalyr";
  private static final String TEST_BATCH_SEND_SIZE = "2222222";
  private static final String TEST_BATCH_SEND_WAIT_MS = "3000";

  /**
   * Test config with all values specified
   */
  @Test
  public void testConfig() {
    Map<String, String> config = TestUtils.makeMap(
      SCALYR_SERVER_CONFIG, TEST_SCALYR_SERVER,
      SCALYR_API_CONFIG, TEST_API_KEY,
      COMPRESSION_TYPE_CONFIG, TEST_COMPRESSION_TYPE,
      COMPRESSION_LEVEL_CONFIG, TEST_COMPRESSION_LEVEL,
      ADD_EVENTS_TIMEOUT_MS_CONFIG, TEST_ADD_EVENTS_TIMEOUT,
      ADD_EVENTS_RETRY_DELAY_MS_CONFIG, TEST_ADD_EVENTS_RETRY_DELAY_MS,
      EVENT_ENRICHMENT_CONFIG, TEST_EVENT_ENRICHMENT,
      BATCH_SEND_SIZE_BYTES_CONFIG, TEST_BATCH_SEND_SIZE,
      BATCH_SEND_WAIT_MS_CONFIG, TEST_BATCH_SEND_WAIT_MS,
      CUSTOM_APP_EVENT_MAPPING_CONFIG, TestValues.CUSTOM_APP_EVENT_MAPPING_JSON);

    ScalyrSinkConnectorConfig connectorConfig = new ScalyrSinkConnectorConfig(config);
    assertEquals(TEST_SCALYR_SERVER, connectorConfig.getString(SCALYR_SERVER_CONFIG));
    assertEquals(TEST_API_KEY, connectorConfig.getPassword(SCALYR_API_CONFIG).value());
    assertEquals(TEST_COMPRESSION_TYPE, connectorConfig.getString(COMPRESSION_TYPE_CONFIG));
    assertEquals(Integer.valueOf(TEST_COMPRESSION_LEVEL), connectorConfig.getInt(COMPRESSION_LEVEL_CONFIG));
    assertEquals(Integer.valueOf(TEST_ADD_EVENTS_TIMEOUT), connectorConfig.getInt(ADD_EVENTS_TIMEOUT_MS_CONFIG));
    assertEquals(Integer.valueOf(TEST_ADD_EVENTS_RETRY_DELAY_MS), connectorConfig.getInt(ADD_EVENTS_RETRY_DELAY_MS_CONFIG));
    assertEquals(Arrays.asList(TEST_EVENT_ENRICHMENT.split(",")), connectorConfig.getList(EVENT_ENRICHMENT_CONFIG));
    assertEquals(Integer.valueOf(TEST_BATCH_SEND_SIZE), connectorConfig.getInt(BATCH_SEND_SIZE_BYTES_CONFIG));
    assertEquals(Integer.valueOf(TEST_BATCH_SEND_WAIT_MS), connectorConfig.getInt(BATCH_SEND_WAIT_MS_CONFIG));
    assertEquals(TestValues.CUSTOM_APP_EVENT_MAPPING_JSON, connectorConfig.getString(CUSTOM_APP_EVENT_MAPPING_CONFIG));
  }

  /**
   * Test default config values
   */
  @Test
  public void testConfigDefaults() {
    Map<String, String> config = TestUtils.makeMap(
      SCALYR_API_CONFIG, TEST_API_KEY);

    ScalyrSinkConnectorConfig connectorConfig = new ScalyrSinkConnectorConfig(config);
    assertEquals(PROD_SCALYR_SERVER, connectorConfig.getString(SCALYR_SERVER_CONFIG));
    assertEquals(TEST_API_KEY, connectorConfig.getPassword(SCALYR_API_CONFIG).value());
    assertEquals(DEFAULT_COMPRESSION_TYPE, connectorConfig.getString(COMPRESSION_TYPE_CONFIG));
    assertNull(connectorConfig.getInt(COMPRESSION_LEVEL_CONFIG));
    assertEquals(DEFAULT_ADD_EVENTS_TIMEOUT_MS, connectorConfig.getInt(ADD_EVENTS_TIMEOUT_MS_CONFIG).intValue());
    assertNull(connectorConfig.getList(EVENT_ENRICHMENT_CONFIG));
    assertEquals(DEFAULT_BATCH_SEND_SIZE_BYTES, connectorConfig.getInt(BATCH_SEND_SIZE_BYTES_CONFIG).intValue());
    assertEquals(DEFAULT_BATCH_SEND_WAIT_MS, connectorConfig.getInt(BATCH_SEND_WAIT_MS_CONFIG).intValue());
    assertNull(connectorConfig.getString(CUSTOM_APP_EVENT_MAPPING_CONFIG));
  }

  /**
   * Test missing fields without defaults
   */
  @Test(expected = ConfigException.class)
  public void testMissingFields() {
    Map<String, String> config = new HashMap<>();
    new ScalyrSinkConnectorConfig(config);
  }

  /**
   * Test enrichment config key=value validator
   */
  @Test
  public void testEnrichmentValidator() {
    Map<String, String> config = TestUtils.makeMap(
      SCALYR_API_CONFIG, TEST_API_KEY);

    // null
    config.put(EVENT_ENRICHMENT_CONFIG, null);
    new ScalyrSinkConnectorConfig(config);


    // single enrichment attr
    config.put(EVENT_ENRICHMENT_CONFIG, "key=value");
    new ScalyrSinkConnectorConfig(config);

    // multiple enrichment attr
    config.put(EVENT_ENRICHMENT_CONFIG, "key=value, key2=value2");
    new ScalyrSinkConnectorConfig(config);

    // Invalid values
    config.put(EVENT_ENRICHMENT_CONFIG, "notKeyValue");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);

    config.put(EVENT_ENRICHMENT_CONFIG, "=");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);

    config.put(EVENT_ENRICHMENT_CONFIG, "key=value,key2= value2");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
  }

  /**
   * Test config range validators
   */
  @Test
  public void testInvalidConfigValues() {
    Map<String, String> config = TestUtils.makeMap(
      SCALYR_API_CONFIG, TEST_API_KEY);

    config.put(SCALYR_SERVER_CONFIG, "http://www.scalyr.com");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(SCALYR_SERVER_CONFIG);

    config.put(COMPRESSION_TYPE_CONFIG, "bz2");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(COMPRESSION_TYPE_CONFIG);

    config.put(BATCH_SEND_SIZE_BYTES_CONFIG, "100");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(BATCH_SEND_SIZE_BYTES_CONFIG);

    config.put(BATCH_SEND_SIZE_BYTES_CONFIG, "6000001");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(BATCH_SEND_SIZE_BYTES_CONFIG);

    config.put(BATCH_SEND_WAIT_MS_CONFIG, "999");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(BATCH_SEND_WAIT_MS_CONFIG);

    config.put(ADD_EVENTS_TIMEOUT_MS_CONFIG, "1000");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(ADD_EVENTS_TIMEOUT_MS_CONFIG);

    config.put(ADD_EVENTS_RETRY_DELAY_MS_CONFIG, "1");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(ADD_EVENTS_RETRY_DELAY_MS_CONFIG);
  }

  /**
   * Test custom app event mapping validator
   */
  @Test
  public void testCustomAppEventMappingValidator() throws IOException {
    Map<String, String> config = TestUtils.makeMap(
      SCALYR_API_CONFIG, TEST_API_KEY);
    ObjectMapper objectMapper = new ObjectMapper();

    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, objectMapper.writeValueAsString(
      Arrays.asList(TestValues.CUSTOM_APP_EVENT_MAPPING_JSON, TestValues.CUSTOM_APP_EVENT_MAPPING_WITH_DELIMITER_JSON)));
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, "");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, "[]");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, "[{}]");
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    Map<String, Object> customAppEventMapping = TestValues.createCustomAppEventMapping(".");
    customAppEventMapping.remove("matcher");
    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, objectMapper.writeValueAsString(Arrays.asList(customAppEventMapping)));
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    customAppEventMapping = TestValues.createCustomAppEventMapping(".");
    customAppEventMapping.remove("eventMapping");
    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, objectMapper.writeValueAsString(Arrays.asList(customAppEventMapping)));
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    customAppEventMapping = TestValues.createCustomAppEventMapping(".");
    customAppEventMapping.put("matcher", Collections.EMPTY_MAP);
    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, objectMapper.writeValueAsString(Arrays.asList(customAppEventMapping)));
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    customAppEventMapping = TestValues.createCustomAppEventMapping(".");
    customAppEventMapping.put("eventMapping", Collections.EMPTY_MAP);
    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, objectMapper.writeValueAsString(Arrays.asList(customAppEventMapping)));
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);

    customAppEventMapping = TestValues.createCustomAppEventMapping(".");
    customAppEventMapping.put("extraField", "bad");
    config.put(CUSTOM_APP_EVENT_MAPPING_CONFIG, objectMapper.writeValueAsString(Arrays.asList(customAppEventMapping)));
    fails(() -> new ScalyrSinkConnectorConfig(config), ConfigException.class);
    config.remove(CUSTOM_APP_EVENT_MAPPING_CONFIG);
  }

  /**
   * Test ConfigDef contains all the config properties
   */
  @Test
  public void testConfigDef() {
    final ImmutableSet<String> configs = ImmutableSet.of(SCALYR_SERVER_CONFIG, SCALYR_API_CONFIG,
      COMPRESSION_TYPE_CONFIG, COMPRESSION_LEVEL_CONFIG, ADD_EVENTS_TIMEOUT_MS_CONFIG,
      ADD_EVENTS_RETRY_DELAY_MS_CONFIG, EVENT_ENRICHMENT_CONFIG,
      BATCH_SEND_SIZE_BYTES_CONFIG, BATCH_SEND_WAIT_MS_CONFIG, CUSTOM_APP_EVENT_MAPPING_CONFIG);

    ConfigDef configDef = ScalyrSinkConnectorConfig.configDef();
    assertEquals(configs.size(), configDef.names().size());
    assertTrue(configDef.names().containsAll(configs));
  }
}
