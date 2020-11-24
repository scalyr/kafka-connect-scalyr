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

import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.Event;
import com.scalyr.integrations.kafka.TestUtils;
import com.scalyr.integrations.kafka.TestValues;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Test for EventMapper
 */
@RunWith(Parameterized.class)
public class EventMapperTest {

  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final List<Map<String, String>> testEnrichmentAttrs =
    Arrays.asList(null, TestUtils.makeMap("env", "test", "org", "Scalyr")); // Test without and with enrichment
  private static final AtomicInteger offset = new AtomicInteger();

  private final Supplier<Object> recordValue;
  private final Map<String, String> enrichmentAttrs;
  private final boolean sendEntireRecord;
  private EventMapper eventMapper;

  /**
   * Create test parameters for each SinkRecordValueCreator type, testEnrichmentAttrs, and sendEntireRecord combination.
   * Object[] = {Supplier<Object> recordValue, List<String> enrichmentAttr, sendEntireRecord boolean}
   */
  @Parameterized.Parameters
  public static Collection<Object[]> testParams() {
    return TestUtils.singleRecordValueTestParams().stream()
      .flatMap(recordValue -> testEnrichmentAttrs.stream().flatMap(enrichmentAttrs ->
        Stream.of(new Object[] {recordValue[0], enrichmentAttrs, false}, new Object[] {recordValue[0], enrichmentAttrs, true})))
      .collect(Collectors.toList());
  }

  public EventMapperTest(Supplier<Object> recordValue, Map<String, String> enrichmentAttrs, boolean sendEntireRecord) {
    this.recordValue = recordValue;
    this.enrichmentAttrs = enrichmentAttrs;
    this.sendEntireRecord = sendEntireRecord;

    // Print test params
    Object data = recordValue.get();
    System.out.println("Executing test with " + (data instanceof Struct ? "schema" : "schemaless") + " recordValue: " + data
      + "\nenrichmentAttrs: " + enrichmentAttrs);
  }

  @Before
  public void setup() throws IOException {
    this.eventMapper = new EventMapper(enrichmentAttrs, CustomAppEventMapping.parseCustomAppEventMappingConfig(TestValues.CUSTOM_APP_EVENT_MAPPING_JSON), sendEntireRecord);
  }

  /**
   * Test EventMapper with no timestamp in SinkRecord
   */
  @Test
  public void createEventNoTimestampTest() throws Exception {
    // Without timestamp
    final long nsFromEpoch = ScalyrUtil.NANOS_PER_SECOND;  // 1 second after epoch
    ScalyrUtil.setCustomTimeNs(nsFromEpoch);
    Object value = recordValue.get();
    Schema valueSchema = value instanceof Struct ? ((Struct)value).schema() : null;
    SinkRecord sinkRecord = new SinkRecord(topic, partition, null, null, valueSchema, value, offset.getAndIncrement());
    Event event = eventMapper.createEvent(sinkRecord);
    validateEvent(event, value);
    assertEquals(nsFromEpoch, event.getTimestamp());
  }

  /**
   * Test EventMapper with timestamp in SinkRecord
   */
  @Test
  public void createEventWithTimestampTest() throws Exception {
    // With timestamp
    final long msSinceEpoch = 60 * 1000;  // 1 minute after epoch
    Object value = recordValue.get();
    Schema valueSchema = value instanceof Struct ? ((Struct)value).schema() : null;
    SinkRecord sinkRecord = new SinkRecord(topic, partition, null, null, valueSchema, value, offset.getAndIncrement(), msSinceEpoch, TimestampType.CREATE_TIME);
    Event event = eventMapper.createEvent(sinkRecord);
    validateEvent(event, value);
    assertEquals(msSinceEpoch * ScalyrUtil.NANOS_PER_MS, event.getTimestamp());
  }


  /**
   * Test no {@link MessageMapper} for the SinkRecord value
   */
  @Test
  public void noMessageMapperTest() {
    SinkRecord sinkRecord = new SinkRecord(topic, partition, null, null, null, new HashMap<>(), offset.getAndIncrement());
    assertNull(eventMapper.createEvent(sinkRecord));
  }

  /**
   * Test default values.
   * Create a SinkRecord that matches a MessageMapper but does not provide any other values, so defaults will be used.
   */
  @Test
  public void defaultValuesTest() {
    // value = {agent: {type: filebeat}}
    Map<String, Object> value = new HashMap<>();
    value.put("agent", TestUtils.makeMap("type", "filebeat"));

    SinkRecord sinkRecord = new SinkRecord(topic, partition, null, null, null, value, offset.getAndIncrement());
    Event event = eventMapper.createEvent(sinkRecord);
    assertEquals(EventMapper.DEFAULT_PARSER, event.getParser());
    assertEquals("Kafka-" + sinkRecord.topic(), event.getServerHost());
  }

  /**
   * Validate Scalyr event matches SinkRecord
   */
  private void validateEvent(Event event, Object recordValue) throws Exception {
    assertEquals(TestValues.SERVER_VALUE + "0", event.getServerHost());
    assertEquals(topic, event.getTopic());
    assertEquals(partition, event.getPartition());
    assertEquals(offset.get() - 1, event.getOffset());
    assertEquals(enrichmentAttrs, event.getEnrichmentAttrs());
    validateMessage(event, recordValue);

    if (event.getLogfile().equals(TestValues.CUSTOM_APP_NAME)) {
      validateCustomAppFields(event);
    } else {
      validateFilebeatFields(event);
    }
  }

  private void validateMessage(Event event, Object recordValue) throws Exception {
    if (!sendEntireRecord) {
      assertEquals(TestValues.MESSAGE_VALUE, event.getMessage());
    } else if (recordValue instanceof Map) {
        TestUtils.verifyMap((Map)recordValue, event.getMessage());
    } else if (recordValue instanceof Struct) {
      TestUtils.verifyStruct((Struct)recordValue, event.getMessage());
    } else {
      fail("Invalid record value");
    }
  }

  private void validateFilebeatFields(Event event) {
    assertEquals(TestValues.LOGFILE_VALUE + "0", event.getLogfile());
    assertEquals(TestValues.PARSER_VALUE + "0", event.getParser());
  }

  private void validateCustomAppFields(Event event) {
    assertEquals(TestValues.CUSTOM_APP_NAME, event.getLogfile());
    assertEquals(TestValues.PARSER_VALUE, event.getParser());
    Map<String, Object> additionalAttrs = event.getAdditionalAttrs();
    assertEquals(4, additionalAttrs.size());
    assertEquals(TestValues.CUSTOM_APP_NAME, additionalAttrs.get("application"));
    assertEquals(TestValues.CUSTOM_APP_VERSION, additionalAttrs.get("version"));
    assertEquals(TestValues.ACTIVITY_TYPE_VALUE, additionalAttrs.get("activityType"));
    assertEquals(false, additionalAttrs.get("failed"));
  }
}
