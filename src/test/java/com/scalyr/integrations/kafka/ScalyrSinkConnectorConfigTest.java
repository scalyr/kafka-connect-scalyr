package com.scalyr.integrations.kafka;

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test ScalyrSinkConnectorConfig
 */
public class ScalyrSinkConnectorConfigTest {

  private static final String TEST_SCALYR_SERVER = "https://test.scalyr.com";
  private static final String TEST_API_KEY = "abcdef123456";
  private static final String TEST_COMPRESSION_TYPE = "none";
  private static final String TEST_COMPRESSION_LEVEL = "0";
  private static final String TEST_ADD_EVENTS_TIMEOUT = "32000";

  /**
   * Test config with all values specified
   */
  @Test
  public void testConfig() {
    Map<String, String> config = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, TEST_SCALYR_SERVER,
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, TEST_API_KEY,
      ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG, TEST_COMPRESSION_TYPE,
      ScalyrSinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, TEST_COMPRESSION_LEVEL,
      ScalyrSinkConnectorConfig.ADD_EVENTS_TIMEOUT_MS_CONFIG, TEST_ADD_EVENTS_TIMEOUT);

    ScalyrSinkConnectorConfig connectorConfig = new ScalyrSinkConnectorConfig(config);
    assertEquals(TEST_SCALYR_SERVER, connectorConfig.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG));
    assertEquals(TEST_API_KEY, connectorConfig.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value());
    assertEquals(TEST_COMPRESSION_TYPE, connectorConfig.getString(ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG));
    assertEquals(Integer.valueOf(TEST_COMPRESSION_LEVEL), connectorConfig.getInt(ScalyrSinkConnectorConfig.COMPRESSION_LEVEL_CONFIG));
    assertEquals(Long.valueOf(TEST_ADD_EVENTS_TIMEOUT), connectorConfig.getLong(ScalyrSinkConnectorConfig.ADD_EVENTS_TIMEOUT_MS_CONFIG));
  }

  /**
   * Test default config values
   */
  @Test
  public void testConfigDefaults() {
    Map<String, String> config = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, TEST_API_KEY);

    ScalyrSinkConnectorConfig connectorConfig = new ScalyrSinkConnectorConfig(config);
    assertEquals(ScalyrSinkConnectorConfig.DEFAULT_SCALYR_SERVER, connectorConfig.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG));
    assertEquals(TEST_API_KEY, connectorConfig.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value());
    assertEquals(ScalyrSinkConnectorConfig.DEFAULT_COMPRESSION_TYPE, connectorConfig.getString(ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG));
    assertNull(connectorConfig.getInt(ScalyrSinkConnectorConfig.COMPRESSION_LEVEL_CONFIG));
    assertEquals(20000, connectorConfig.getLong(ScalyrSinkConnectorConfig.ADD_EVENTS_TIMEOUT_MS_CONFIG).longValue());
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
   * Test ConfigDef contains all the config properties
   */
  @Test
  public void testConfigDef() {
    final ImmutableSet<String> configs = ImmutableSet.of(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, ScalyrSinkConnectorConfig.SCALYR_API_CONFIG,
      ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG, ScalyrSinkConnectorConfig.COMPRESSION_LEVEL_CONFIG, ScalyrSinkConnectorConfig.ADD_EVENTS_TIMEOUT_MS_CONFIG);
    ConfigDef configDef = ScalyrSinkConnectorConfig.configDef();
    assertEquals(configs.size(), configDef.names().size());
    assertTrue(configDef.names().containsAll(configs));
  }
}
