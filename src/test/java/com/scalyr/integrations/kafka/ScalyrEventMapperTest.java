package com.scalyr.integrations.kafka;

import com.scalyr.api.internal.ScalyrUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for ScalyrEventMapper
 */
public class ScalyrEventMapperTest {

  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final String TEST_LOG_FIELDS = "message, host.name, log.file.path";
  private static final String API_KEY = "abc123";

  private static final AtomicInteger offset = new AtomicInteger();
  private static final AtomicInteger messageNum = new AtomicInteger();
  private ScalyrSinkConnectorConfig config;
  private List<String> eventAttrs;


  @Before
  public void setup() {
    Map<String, String> configMap = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, API_KEY,
      ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG, TEST_LOG_FIELDS);

    config = new ScalyrSinkConnectorConfig(configMap);
    eventAttrs = config.getList(ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG);
  }

  /**
   * Test creating a single log message event with no schema for a SinkRecord
   */
  @Test
  public void singleEventNoSchemaTest() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, getSchemalessRecordValue(), offset.getAndIncrement());
    validateEvent(record, eventAttrs, ScalyrEventMapper.createSessionId(record.topic(), record.kafkaPartition()), ScalyrEventMapper.createEvent(record, config));
  }

  /**
   * Test multiple events for a single session.
   * Multiple SinkRecords with the same {topic, partition} are created which map to the same Scalyr session.
   */
  @Test
  public void singleSessionNoSchemaTest() {
    final String sessionId = ScalyrEventMapper.createSessionId(topic, partition);
    List<SinkRecord> records = IntStream.range(0, 10)
      .mapToObj(i -> new SinkRecord(topic, partition, null, null, null, getSchemalessRecordValue(), offset.getAndIncrement()))
      .collect(Collectors.toList());

    List<Map<String, Object>> events = ScalyrEventMapper.createEvents(records, config);
    assertEquals(1, events.size());
    Map<String, Object> sessionEvents = events.get(0);
    validateSessionEvents(records, eventAttrs, sessionId, sessionEvents);
  }

  /**
   * Test events for multiple sessions.  A session is created for each {topic, partition}.
   * Create sessions using different topics and same topic, different parition.
   */
  @Test
  public void multipleSessionNoSchemaTest() {
    final int numRecords = 10;
    Map<String, List<SinkRecord>> recordsBySessionId = new HashMap<>();
    recordsBySessionId.put(ScalyrEventMapper.createSessionId("topic1", 0), createSchemalessRecords("topic1", 0, numRecords));
    recordsBySessionId.put(ScalyrEventMapper.createSessionId("topic1", 1), createSchemalessRecords("topic1", 1, numRecords));
    recordsBySessionId.put(ScalyrEventMapper.createSessionId("topic2", 0), createSchemalessRecords("topic2", 0, numRecords));

    // Combine records from all sessions in ascending offset order
    List<SinkRecord> allRecords = IntStream.range(0, numRecords)
      .boxed()
      .flatMap(i -> recordsBySessionId.values().stream().map(records -> records.get(i)))
      .collect(Collectors.toList());

    List<Map<String, Object>> events = ScalyrEventMapper.createEvents(allRecords, config);
    assertEquals(3, events.size());
    Set<String> sessionIds = new HashSet<>();
    events.forEach(sessionEvents -> {
      String sessionId = (String)sessionEvents.get(ScalyrEventMapper.SESSION);
      assertTrue(sessionIds.add(sessionId));  // Verify sessionId is unique and not used before
      validateSessionEvents(recordsBySessionId.get(sessionId), eventAttrs, sessionId, sessionEvents);
    });
  }

  /**
   * Create SinkRecords without Schema for the specified topic, partition, and numRecords
   */
  static List<SinkRecord> createSchemalessRecords(String topic, int partition, int numRecords) {
    return IntStream.range(0, numRecords)
      .mapToObj(i -> new SinkRecord(topic, partition, null, null, null, getSchemalessRecordValue(), offset.getAndIncrement()))
      .collect(Collectors.toList());
  }

  /**
   * @return Map<String, Object> Test SinkRecord value without schema
   */
  private static Map<String, Object> getSchemalessRecordValue() {
    Map<String, Object> value = new HashMap<>();
    value.put("message", "Test log message" + messageNum.getAndIncrement());
    value.put("host.name", "server1");
    value.put("log.file.path", "/var/log/syslog");
    value.put("agent.type", "filebeat");
    return value;
  }

  /**
   * Validate all the Scalyr events for a session
   */
  static void validateSessionEvents(List<SinkRecord> records, Collection<String> attrs, String sessionId, Map<String, Object> sessionEvents) {
    assertEquals(sessionId, sessionEvents.get(ScalyrEventMapper.SESSION));
    assertEquals(API_KEY, sessionEvents.get(ScalyrEventMapper.TOKEN));
    List<Map<String, Object>> events = (List)sessionEvents.get(ScalyrEventMapper.EVENTS);
    assertNotNull(events);
    assertEquals(records.size(), events.size());
    IntStream.range(0, records.size()).forEach(i -> validateEvent(records.get(i), attrs, sessionId, events.get(i)));
  }

  /**
   * Validate Scalyr event matches SinkRecord
   */
  private static void validateEvent(SinkRecord record, Collection<String> attrs, String sessionId, Map<String, Object> event) {
    assertEquals(4, event.size());
    assertEquals(sessionId, event.get(ScalyrEventMapper.SEQUENCE_ID));
    assertEqualsWithNumberConversion(record.kafkaOffset(), event.get(ScalyrEventMapper.SEQUENCE_NUM));
    assertTrue((Long)event.get(ScalyrEventMapper.TIMESTAMP) <= ScalyrUtil.nanoTime());
    assertTrue(((Map)event.get(ScalyrEventMapper.ATTRS)).keySet().containsAll(attrs));
    Map<String, Object> eventAttrs = (Map)event.get(ScalyrEventMapper.ATTRS);
    Map<String, Object> recordValues = (Map)record.value();
    attrs.forEach(attr -> assertEqualsWithNumberConversion(recordValues.get(attr), eventAttrs.get(attr)));
  }

  /**
   * When deserializing from JSON, a Long may get deserialized as Integer if it can fit in 32 bits.
   * This method correctly compares numbers regardless of type.
   */
  private static void assertEqualsWithNumberConversion(Object expected, Object actual) {
    if (expected instanceof Number && actual instanceof Number) {
      assertEquals(((Number) expected).doubleValue(), ((Number) actual).doubleValue(), 0);
      return;
    }
    assertEquals(expected, actual);
  }
}
