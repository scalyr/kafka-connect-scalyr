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

package com.scalyr.integrations.kafka.mapping;

import com.google.common.annotations.VisibleForTesting;
import com.scalyr.integrations.kafka.TestUtils;
import com.scalyr.integrations.kafka.TestValues;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for CustomAppMessageMapper
 */
@RunWith(Parameterized.class)
public class CustomAppMessageMapperTest {
  private static final AtomicInteger offset = new AtomicInteger();
  private static final Random random = new Random();
  private MessageMapper messageMapper;
  private SinkRecordValueCreator sinkRecordValueCreator;
  private boolean matchAll;

  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final String matcherAttr = "application.name";

  @Parameterized.Parameters
  public static Collection<Object[]> testParams() {
    return Arrays.asList(new Object[][] { {createCustomAppEventMapping(), false},
      {createCustomAppEventMapping().setMatcher(createMatcher(matcherAttr, "custom.*")), false},
      {createCustomAppEventMapping().setMatcher(new CustomAppEventMapping.Matcher().setMatchAll(true)), true}});
  }

  public CustomAppMessageMapperTest(CustomAppEventMapping customAppEventMapping, boolean matchAll) {
    this.messageMapper = new CustomAppMessageMapper(customAppEventMapping);
    this.matchAll = matchAll;
    this.sinkRecordValueCreator = new CustomAppRecordValueCreator();

  }

  /**
   * Create CustomAppEventMapping for the test custom app.
   * Typically this is parsed from JSON, but we construct it programmatically here.
   * See {@link CustomAppRecordValueCreator} for the app fields.
   */
  @VisibleForTesting static CustomAppEventMapping createCustomAppEventMapping() {
    CustomAppEventMapping customAppEventMapping = new CustomAppEventMapping();
    customAppEventMapping.setEventMapping(TestUtils.makeMap(
      "id", "id",
      CustomAppEventMapping.LOG_FILE, "application.name",
      CustomAppEventMapping.MESSAGE, "message",
      CustomAppEventMapping.PARSER, "scalyr.parser",
      CustomAppEventMapping.SERVER_HOST, "host.hostname",
      "application", "application.name",
      "version", "application.version",
      "severity", "severity",
      "failed", "failed",
      "activityType", "activityType"
    ));
    customAppEventMapping.setMatcher(createMatcher(matcherAttr, TestValues.CUSTOM_APP_NAME));

    return customAppEventMapping;
  }

  /**
   * @return CustomAppEventMapping.Matcher with specified matcher attr and value
   */
  private static CustomAppEventMapping.Matcher createMatcher(String attr, String value) {
    CustomAppEventMapping.Matcher matcher = new CustomAppEventMapping.Matcher();
    matcher.setAttribute(attr);
    matcher.setValue(value);
    return matcher;
  }

  /**
   * Test mapper gets correct values for schemaless record value
   */
  @Test
  public void testCustomAppMessageMapperSchemaless() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, sinkRecordValueCreator.createSchemalessRecordValue(1, 1, 1), offset.getAndIncrement());
    verifySinkRecord(record);
  }

  /**
   * Test mapper gets correct values for schema record value
   */
  @Test
  public void testCustomAppMessageMapperSchema() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, sinkRecordValueCreator.createSchemaRecordValue(1, 1, 1), offset.getAndIncrement());
    verifySinkRecord(record);
  }

  private void verifySinkRecord(SinkRecord record) {
    assertEquals(TestValues.MESSAGE_VALUE, messageMapper.getMessage(record));
    assertEquals(TestValues.CUSTOM_APP_NAME, messageMapper.getLogfile(record));
    assertEquals(TestValues.PARSER_VALUE, messageMapper.getParser(record));
    assertEquals(TestValues.SERVER_VALUE + "0", messageMapper.getServerHost(record));
    Map<String, Object> additionalAttrs = messageMapper.getAdditionalAttrs(record);
    assertNotNull(additionalAttrs.get("id"));
    assertEquals(TestValues.SEVERITY_VALUE, additionalAttrs.get("severity"));
    assertEquals(TestValues.CUSTOM_APP_VERSION, additionalAttrs.get("version"));
    assertEquals(TestValues.CUSTOM_APP_NAME, additionalAttrs.get("application"));
    assertEquals(TestValues.ACTIVITY_TYPE_VALUE, additionalAttrs.get("activityType"));

    assertTrue(messageMapper.matches(record));
  }

  /**
   * Test mapped fields not present
   */
  @Test
  public void testMissingFieldsMessage() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, new HashMap<>(), offset.getAndIncrement());
    assertNull(messageMapper.getMessage(record));
    assertNull(messageMapper.getLogfile(record));
    assertNull(messageMapper.getParser(record));
    assertNull(messageMapper.getServerHost(record));
    assertEquals(matchAll, messageMapper.matches(record));
    Map<String, Object> additionalAttrs = messageMapper.getAdditionalAttrs(record);
    assertTrue(additionalAttrs.containsKey("id"));
    assertTrue(additionalAttrs.containsKey("severity"));
    assertTrue(additionalAttrs.containsKey("version"));
    assertTrue(additionalAttrs.containsKey("application"));
    assertNull(additionalAttrs.get("id"));
    assertNull(additionalAttrs.get("severity"));
    assertNull(additionalAttrs.get("version"));
    assertNull(additionalAttrs.get("application"));
  }

  /**
   * Creates SinkRecord values in the simulated custom app format.
   *
   * The custom app has the following fields:
   * id
   * host.hostname
   * application.name
   * application.version
   * scalyr.parser
   * severity (integer)
   * message
   * failed (boolean)
   * activity-type (List)
   */
  public static class CustomAppRecordValueCreator implements SinkRecordValueCreator {
    @Override
    public Map<String, Object> createSchemalessRecordValue(int numServers, int numLogFiles, int numParsers) {
      assertTrue(numParsers <= numLogFiles);

      Map<String, Object> value = new HashMap<>();
      // {id: 123}
      value.put("id", UUID.randomUUID().toString());

      // {message: logMessage}
      value.put("message", TestValues.MESSAGE_VALUE);

      // {severity: 3}
      value.put("severity", TestValues.SEVERITY_VALUE);

      // {application: {name: xxx, version, 1.0}}
      final Map<String, Object> app = new HashMap<>();
      app.put("name", TestValues.CUSTOM_APP_NAME);
      app.put("version", TestValues.CUSTOM_APP_VERSION);
      value.put("application", app);

      // {host: {hostname: myhost}}
      value.put("host", TestUtils.makeMap("hostname", TestValues.SERVER_VALUE + random.nextInt(numServers)));

      // {scalyr: {parser: appParser}}
      value.put("scalyr", TestUtils.makeMap("parser", TestValues.PARSER_VALUE));

      // {failed: false}
      value.put("failed", false);

      // {activityType: ["web-access"]
      value.put("activityType", TestValues.ACTIVITY_TYPE_VALUE);

      return value;
    }

    /**
     * @return Struct Test SinkRecord value with Schema
     */
    @Override
    public Struct createSchemaRecordValue(int numServers, int numLogFiles, int numParsers) {
      assertTrue(numParsers <= numLogFiles);

      final Schema hostSchema = SchemaBuilder.struct().name("host")
        .field("hostname", Schema.STRING_SCHEMA).build();

      final Schema appSchema = SchemaBuilder.struct().name("app")
        .field("name", Schema.STRING_SCHEMA)
        .field("version", Schema.STRING_SCHEMA);

      final Schema parserSchema = SchemaBuilder.struct().name("parser")
        .field("parser", Schema.STRING_SCHEMA);

      final Schema customAppSchema = SchemaBuilder.struct().name("customApp")
        .field("id", Schema.STRING_SCHEMA)
        .field("message", Schema.STRING_SCHEMA)
        .field("host", hostSchema)
        .field("application", appSchema)
        .field("scalyr", parserSchema)
        .field("severity", Schema.INT32_SCHEMA)
        .field("failed", Schema.BOOLEAN_SCHEMA)
        .field("activityType", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();

      Struct value = new Struct(customAppSchema);
      value.put("id", UUID.randomUUID().toString());
      value.put("message", TestValues.MESSAGE_VALUE);
      value.put("host", new Struct(hostSchema).put("hostname", TestValues.SERVER_VALUE + random.nextInt(numServers)));

      value.put("application", new Struct(appSchema).put("name", TestValues.CUSTOM_APP_NAME).put("version", TestValues.CUSTOM_APP_VERSION));
      value.put("scalyr", new Struct(parserSchema).put("parser", TestValues.PARSER_VALUE));
      value.put("severity", TestValues.SEVERITY_VALUE);
      value.put("failed", false);
      value.put("activityType", TestValues.ACTIVITY_TYPE_VALUE);

      return value;
    }
  }
}
