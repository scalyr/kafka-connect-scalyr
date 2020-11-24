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

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    scalyrSinkConnector.start(makeConfig());
  }

  /**
   * Test invalid config
   */
  @Test(expected = ConnectException.class)
  public void testStartInvalidConfig() {
    scalyrSinkConnector.start(new HashMap<>());
  }

  /**
   * Verify stops without exception
   */
  @Test
  public void testStop() {
    scalyrSinkConnector.start(makeConfig());
    scalyrSinkConnector.stop();
  }

  @Test
  public void testVersion() {
    assertEquals(VersionUtil.getVersion(), scalyrSinkConnector.version());
  }

  /**
   * Verify taskConfigs returns the correct task configs for the SinkTask
   */
  @Test
  public void testTaskConfigs() {
    final int numTaskConfigs = 20;
    Map<String, String> config = makeConfig();
    scalyrSinkConnector.start(config);
    List<Map<String, String>> taskConfigs = scalyrSinkConnector.taskConfigs(numTaskConfigs);
    assertEquals(numTaskConfigs, taskConfigs.size());
    taskConfigs.forEach(taskConfig -> assertEquals(config, taskConfig));
  }

  /**
   * Verify taskClass returns correct sink task class
   */
  @Test
  public void testTaskClass() {
    assertEquals(ScalyrSinkTask.class, scalyrSinkConnector.taskClass());
  }

  /**
   * Verify config returns correct ConfigDef
   */
  @Test public void testConfigDef() {
    assertTrue(scalyrSinkConnector.config().names().containsAll(ScalyrSinkConnectorConfig.configDef().names()));
  }

  private Map<String, String> makeConfig() {
    return TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, "abc123");
  }
}
