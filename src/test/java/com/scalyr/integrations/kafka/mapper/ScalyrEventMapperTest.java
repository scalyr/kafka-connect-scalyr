package com.scalyr.integrations.kafka.mapper;

import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.ScalyrSinkConnectorConfig;
import com.scalyr.integrations.kafka.TestUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for ScalyrEventMapper
 */
public class ScalyrEventMapperTest {

  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final String API_KEY = "abc123";
  private static final String SESSION_ID = UUID.randomUUID().toString();

  private static final AtomicInteger offset = new AtomicInteger();
  private ScalyrSinkConnectorConfig config;

  public static final Map<String, String> recordValues = TestUtils.makeMap(
    "message", "Test log message", "logfile", "/var/log/syslog", "serverHost", "server1", "parser", "systemLogPST");

  @Before
  public void setup() {
    Map<String, String> configMap = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, API_KEY,
      ScalyrSinkConnectorConfig.SESSION_ID_CONFIG, SESSION_ID);

    config = new ScalyrSinkConnectorConfig(configMap);
  }

  /**
   * Test creating a single log message event with no schema for a SinkRecord
   */
  @Test
  public void singleEventNoSchemaTest() {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, createSchemalessRecordValue(), offset.getAndIncrement());
    validateEvent(record, ScalyrEventMapper.createEvent(record, config));
  }

  /**
   * Test multiple events for a single partition.
   * Create multiple SinkRecords with the same {topic, partition}.
   */
  @Test
  public void singleSessionNoSchemaTest() {
    List<SinkRecord> records = IntStream.range(0, 10)
      .mapToObj(i -> new SinkRecord(topic, partition, null, null, null, createSchemalessRecordValue(), offset.getAndIncrement()))
      .collect(Collectors.toList());

    Map<String, Object> sessionEvents = ScalyrEventMapper.createEvents(records, config);
    validateEvents(records, config, sessionEvents);
  }

  /**
   * Test events for multiple partitions.
   * Create SinkRecords for multiple partitions.
   */
  @Test
  public void multipleSessionNoSchemaTest() {
    final int numRecords = 10;
    List<List<SinkRecord>> sinkRecordLists = Stream.of(createSchemalessRecords("topic1", 0, numRecords),
      createSchemalessRecords("topic1", 1, numRecords),
      createSchemalessRecords("topic2", 0, numRecords))
      .collect(Collectors.toList());

    // Intermix records from partitions in ascending offset order
    List<SinkRecord> allRecords = IntStream.range(0, numRecords)
      .boxed()
      .flatMap(i -> sinkRecordLists.stream().map(records -> records.get(i)))
      .collect(Collectors.toList());

    Map<String, Object> events = ScalyrEventMapper.createEvents(allRecords, config);
    validateEvents(allRecords, config, events);
  }

  /**
   * Create SinkRecords without Schema for the specified topic, partition, and numRecords
   */
  public static List<SinkRecord> createSchemalessRecords(String topic, int partition, int numRecords) {
    return IntStream.range(0, numRecords)
      .mapToObj(i -> new SinkRecord(topic, partition, null, null, null, createSchemalessRecordValue(), offset.getAndIncrement()))
      .collect(Collectors.toList());
  }

  /**
   * @return Map<String, Object> Test SinkRecord value without schema
   */
  static Map<String, Object> createSchemalessRecordValue() {
    Map<String, Object> value = new HashMap<>();
    value.put("message", recordValues.get("message"));
    value.put("host", TestUtils.makeMap("name", "server1", "hostname", recordValues.get("serverHost")));

    // nested log file: {log: {file: {path: /var/log/syslog}}};
    final Map<String, Object> file_path = new HashMap<>();
    file_path.put("file", TestUtils.makeMap("path", recordValues.get("logfile")));
    value.put("log", file_path);

    value.put("fields", TestUtils.makeMap("parser", recordValues.get("parser")));

    value.put("agent", TestUtils.makeMap("type", "filebeat"));
    return value;
  }

  /**
   * Validate Scalyr addEvemts payload has correct format and values
   */
  public static void validateEvents(List<SinkRecord> records, ScalyrSinkConnectorConfig config, Map<String, Object> sessionEvents) {
    assertNotNull(sessionEvents.get(ScalyrEventMapper.SESSION));
    assertEquals(config.getString(ScalyrSinkConnectorConfig.SESSION_ID_CONFIG), sessionEvents.get(ScalyrEventMapper.SESSION));
    assertEquals(config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value(), sessionEvents.get(ScalyrEventMapper.TOKEN));
    List<Map<String, Object>> events = (List)sessionEvents.get(ScalyrEventMapper.EVENTS);
    assertNotNull(events);
    assertEquals(records.size(), events.size());
    IntStream.range(0, records.size()).forEach(i -> validateEvent(records.get(i), events.get(i)));
  }

  /**
   * Validate Scalyr event matches SinkRecord
   */
  private static void validateEvent(SinkRecord record, Map<String, Object> event) {
    assertEquals(4, event.size());
    assertEquals(ScalyrEventMapper.createPartitionId(record.topic(), record.kafkaPartition()), event.get(ScalyrEventMapper.SEQUENCE_ID));
    assertEqualsWithNumberConversion(record.kafkaOffset(), event.get(ScalyrEventMapper.SEQUENCE_NUM));
    assertTrue((Long)event.get(ScalyrEventMapper.TIMESTAMP) <= ScalyrUtil.nanoTime());
    TestUtils.verifyMap(recordValues, (Map)event.get(ScalyrEventMapper.ATTRS));
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
