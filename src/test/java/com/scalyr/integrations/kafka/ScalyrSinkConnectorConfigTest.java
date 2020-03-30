package com.scalyr.integrations.kafka;

import com.google.common.collect.ImmutableSet;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test ScalyrSinkConnectorConfig
 */
public class ScalyrSinkConnectorConfigTest {

  private static final String TEST_SCALYR_SERVER = "https://test.scalyr.com";
  private static final String TEST_API_KEY = "abcdef123456";
  private static final String TEST_PARSER = "systemLog";
  private static final String TEST_LOG_FIELDS = "message, host.hostname";
  private static final List<String> TEST_LOG_FIELDS_LIST = TestUtils.makeList(TEST_LOG_FIELDS);

  /**
   * Test config with all values specified
   */
  @Test
  public void testConfig() {
    Map<String, String> config = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, TEST_SCALYR_SERVER,
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, TEST_API_KEY,
      ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG, TEST_LOG_FIELDS,
      ScalyrSinkConnectorConfig.PARSER_CONFIG, TEST_PARSER);

    ScalyrSinkConnectorConfig connectorConfig = new ScalyrSinkConnectorConfig(config);
    assertEquals(TEST_SCALYR_SERVER, connectorConfig.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG));
    assertEquals(TEST_API_KEY, connectorConfig.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value());
    assertEquals(TEST_LOG_FIELDS_LIST, connectorConfig.getList(ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG));
    assertEquals(TEST_PARSER, connectorConfig.getString(ScalyrSinkConnectorConfig.PARSER_CONFIG));
  }

  /**
   * Test default config values
   */
  @Test
  public void testConfigDefaults() {
    Map<String, String> config = TestUtils.makeMap(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, TEST_API_KEY);

    ScalyrSinkConnectorConfig connectorConfig = new ScalyrSinkConnectorConfig(config);
    assertEquals(ScalyrSinkConnectorConfig.DEFAULT_SCALYR_SERVER, connectorConfig.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG));
    assertEquals(TEST_API_KEY, connectorConfig.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value());
    assertEquals(TestUtils.makeList(ScalyrSinkConnectorConfig.DEFAULT_LOG_FIELDS), connectorConfig.getList(ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG));
    assertEquals(ScalyrSinkConnectorConfig.DEFAULT_PARSER, connectorConfig.getString(ScalyrSinkConnectorConfig.PARSER_CONFIG));
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
    ConfigDef configDef = ScalyrSinkConnectorConfig.configDef();
    assertTrue(configDef.names().containsAll(ImmutableSet.of(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, ScalyrSinkConnectorConfig.SCALYR_API_CONFIG,
      ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG, ScalyrSinkConnectorConfig.PARSER_CONFIG)));
  }
}
