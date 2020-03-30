package com.scalyr.integrations.kafka;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test ScalyrSinkConnector
 */
public class ScalyrSinkConnectorTest {

  private ScalyrSinkConnector scalyrSinkConnector;

  @Before
  public void setup() {
    scalyrSinkConnector = new ScalyrSinkConnector();
  }

  /**
   * Verify starts without exception
   */
  @Test
  public void testStart() {
    scalyrSinkConnector.start(TestUtils.makeMap(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, "abc123"));
  }

  @Test(expected = ConnectException.class)
  public void testStartInvalidConfig() {
    scalyrSinkConnector.start(new HashMap<>());
  }

  /**
   * Verify stops without exception
   */
  @Test
  public void testStop() {
    scalyrSinkConnector.stop();
  }

  @Test
  public void testVersion() {
    assertEquals(VersionUtil.getVersion(), scalyrSinkConnector.version());
  }

  @Test public void testConfig() {
    assertTrue(scalyrSinkConnector.config().names().containsAll(ScalyrSinkConnectorConfig.configDef().names()));
  }
}
