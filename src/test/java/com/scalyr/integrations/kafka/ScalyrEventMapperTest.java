package com.scalyr.integrations.kafka;

import com.scalyr.api.internal.ScalyrUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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

  private static Random random = new Random();

  public static final Map<String, String> recordValues = TestUtils.makeMap(
    "message", "Test log message", "logfile", "/var/log/syslog", "serverHost", "server", "parser", "systemLogPST");

  public static final Map<String, String> recordValuesWithoutLogLevelAttr = getRecordValuesWithoutLogLevelAttr(recordValues);

  /**
   * Remove log level attributes from the record values.
   */
  private static Map<String, String> getRecordValuesWithoutLogLevelAttr(Map<String, String> recordValues) {
    Map<String, String> recordValuesWithoutLogLevelAttr = new HashMap<>(recordValues);
    ScalyrEventMapper.LOG_LEVEL_ATTRS.forEach(recordValuesWithoutLogLevelAttr::remove);
    return recordValuesWithoutLogLevelAttr;
  }

  @Before
  public void setup() {
    Map<String, String> configMap = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, API_KEY,
      ScalyrSinkConnectorConfig.SESSION_ID_CONFIG, SESSION_ID);

    config = new ScalyrSinkConnectorConfig(configMap);
  }

  /**
   * Test multiple events for a single partition.
   * Create multiple SinkRecords with the same {topic, partition}.
   */
  @Test
  public void singlePartitionNoSchemaTest() {
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
  public void multiplePartitionsNoSchemaTest() {
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
   * Test events creation with multiple servers, logs, and parsers.
   */
  @Test
  public void multipleServersAndLogsTest() {
    final int numRecords = 100;
    final int numServers = 3;
    final int numLogs = 3;
    final int numParsers = 2;

    List<SinkRecord> records = IntStream.range(0, numRecords)
      .mapToObj(i -> new SinkRecord(topic, partition, null, null, null, createSchemalessRecordValue(numServers, numLogs, numParsers), offset.getAndIncrement()))
      .collect(Collectors.toList());

    Map<String, Object> events = ScalyrEventMapper.createEvents(records, config);
    validateEvents(records, config, events);
  }


  /**
   * Test extractLogLevelAttrs internal method
   */
  @Test
  public void extractLogLevelAttrTest() {
    // single server
    extractlogLevelAttrTest(100, 1, 1,1);
    extractlogLevelAttrTest(100, 3, 1,1);
    extractlogLevelAttrTest(100, 1, 3,1);
    extractlogLevelAttrTest(100, 1, 3,3);
    extractlogLevelAttrTest(100, 3, 3,3);
  }

  /**
   * Test extractLogLevelAttrs
   * Performs the following:
   * 1. Create `numRecords` SinkRecords with specified `numServers`, `numLogFiles`, and `numParsers`
   * 2. Converts the SinkRecords to events
   * 3. Extracts the log level attributes for the events and verifies correctness
   */
  private void extractlogLevelAttrTest(int numRecords, int numServers, int numLogFiles, int numParsers) {
    List<SinkRecord> records = IntStream.range(0, numRecords)
      .mapToObj(i -> new SinkRecord(topic, partition, null, null, null, createSchemalessRecordValue(numServers, numLogFiles, numParsers), offset.getAndIncrement()))
      .collect(Collectors.toList());

    List<Map<String, Object>> events = createEvents(records);

    Map<List<String>, Integer> logLevelAttrs = ScalyrEventMapper.extractLogLevelAttrs(events);

    // Verify expected number of log level attrs extracted
    final int numLogAttrs = numServers * numLogFiles * Math.max(numParsers - numLogFiles, 1);
    assertEquals(numLogAttrs, logLevelAttrs.size());

    // Verify log level attrs are removed for event attrs
    assertTrue(events.stream()
      .map(event -> (Map<String, Object>)event.get(ScalyrEventMapper.ATTRS))
      .allMatch(attrs -> !attrs.keySet().containsAll(ScalyrEventMapper.LOG_LEVEL_ATTRS)));

    // Verify all log ids are unique
    assertEquals(logLevelAttrs.size(), logLevelAttrs.values().stream().collect(Collectors.toSet()).size());
  }

  /**
   * Test createLogsArray internal method
   * Verify missing log level attributes with null values do not appear in the log level attrs.
   */
  @Test
  public void createLogsArrayTest() {
    createLogsArrayTest(Arrays.asList("server1", "logfile1", "parser1"));
    createLogsArrayTest(Arrays.asList(null, "logfile1", "parser1"));
    createLogsArrayTest(Arrays.asList("server1", null, "parser1"));
    createLogsArrayTest(Arrays.asList("server1", "logfile1", null));
    createLogsArrayTest(Arrays.asList(null, null, null));
  }

  /**
   * Test createLogsArray helper
   * @param logAttrValues Values list corresponding with {@link ScalyrEventMapper.LOG_LEVEL_ATTRS}
   */
  private void createLogsArrayTest(List<String> logAttrValues) {
    assertEquals(ScalyrEventMapper.LOG_LEVEL_ATTRS.size(), logAttrValues.size());

    Map<List<String>, Integer> logLevelAttrs = new HashMap<>();
    logLevelAttrs.put(logAttrValues, 1);

    // Get log level attrs that have values
    Set<String> logLevelAttrNames = IntStream.range(0, ScalyrEventMapper.LOG_LEVEL_ATTRS.size())
      .filter(i -> logAttrValues.get(i) != null)
      .boxed()
      .map(i -> ScalyrEventMapper.LOG_LEVEL_ATTRS.get(i))
      .collect(Collectors.toSet());

    List<Map<String, Object>> logsArray = ScalyrEventMapper.createLogsArray(logLevelAttrs);

    // Verify createLogsArray results
    logsArray.forEach(m -> {
      assertEquals(2, m.size());
      assertNotNull(m.get(ScalyrEventMapper.ID));
      Map<String, Object> logAttrs = (Map)m.get(ScalyrEventMapper.ATTRS);
      assertNotNull(m.get(ScalyrEventMapper.ATTRS));
      assertTrue(logAttrs.keySet().containsAll(logLevelAttrNames));
    });

    // Verify all entries have unique log id
    assertEquals(logLevelAttrs.size(), logsArray.stream()
      .map(m -> m.get(ScalyrEventMapper.ID))
      .collect(Collectors.toSet()).size());
  }

  private List<Map<String, Object>> createEvents(List<SinkRecord> records) {
    return records.stream()
      .map(r -> ScalyrEventMapper.createEvent(r, config))
      .collect(Collectors.toList());
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
  static Map<String, Object> createSchemalessRecordValue(int numServers, int numLogFiles, int numParsers) {
    assertTrue(numParsers <= numLogFiles);

    Map<String, Object> value = new HashMap<>();
    value.put("message", recordValues.get("message"));
    value.put("host", TestUtils.makeMap("name", "server1", "hostname", recordValues.get("serverHost") + random.nextInt(numServers)));

    // nested log file: {log: {file: {path: /var/log/syslog}}};
    final Map<String, Object> file_path = new HashMap<>();
    final int logFileNum = random.nextInt(numLogFiles);
    file_path.put("file", TestUtils.makeMap("path", recordValues.get("logfile") + logFileNum));
    value.put("log", file_path);

    value.put("fields", TestUtils.makeMap("parser", recordValues.get("parser") + logFileNum % numParsers));

    value.put("agent", TestUtils.makeMap("type", "filebeat"));
    return value;
  }

  static Map<String, Object> createSchemalessRecordValue() {
    return createSchemalessRecordValue(1, 1, 1);
  }

  /**
   * Validate Scalyr addEvemts payload has correct format and values
   */
  public static void validateEvents(List<SinkRecord> records, ScalyrSinkConnectorConfig config, Map<String, Object> sessionEvents) {
    assertNotNull(sessionEvents.get(ScalyrEventMapper.SESSION));
    assertEquals(config.getString(ScalyrSinkConnectorConfig.SESSION_ID_CONFIG), sessionEvents.get(ScalyrEventMapper.SESSION));
    assertEquals(config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value(), sessionEvents.get(ScalyrEventMapper.TOKEN));
    List<Map<String, Object>> logsArray = (List<Map<String, Object>>)sessionEvents.get(ScalyrEventMapper.LOGS);
    assertNotNull(logsArray);
    List<Map<String, Object>> events = (List)sessionEvents.get(ScalyrEventMapper.EVENTS);
    assertNotNull(events);
    assertEquals(records.size(), events.size());

    System.out.println(logsArray);
    Map<String, Object> logIdAttrs = logsArray.stream()
      .collect(Collectors.toMap(m -> (String)m.get(ScalyrEventMapper.ID), m -> m.get(ScalyrEventMapper.ATTRS)));

    System.out.println(logIdAttrs);
    IntStream.range(0, records.size()).forEach(i -> validateEvent(records.get(i), events.get(i), logIdAttrs));
  }

  /**
   * Validate Scalyr event matches SinkRecord
   */
  private static void validateEvent(SinkRecord record, Map<String, Object> event, Map<String, Object> logIdAttrs) {
    assertEquals(5, event.size());
    assertEquals(ScalyrEventMapper.createPartitionId(record.topic(), record.kafkaPartition()), event.get(ScalyrEventMapper.SEQUENCE_ID));
    String logId = (String)event.get(ScalyrEventMapper.LOG_ID);
    assertTrue(logIdAttrs.containsKey(logId));

    assertEqualsWithNumberConversion(record.kafkaOffset(), event.get(ScalyrEventMapper.SEQUENCE_NUM));
    assertTrue((Long)event.get(ScalyrEventMapper.TIMESTAMP) <= ScalyrUtil.nanoTime());
    verifyMapStringStartsWith(recordValuesWithoutLogLevelAttr, (Map) event.get(ScalyrEventMapper.ATTRS));

    // Add log level attributes back into event attrs and make sure all the attrs are correct
    Map logLevelAttrs = (Map) logIdAttrs.get(logId);
    Map eventAttrs = (Map) event.get(ScalyrEventMapper.ATTRS);
    eventAttrs.putAll(logLevelAttrs);
    verifyMapStringStartsWith(recordValues, eventAttrs);
  }

  /**
   * Verify maps are the the same.  For String values, verify the actual `startsWith` the expected because
   * a number may be appended to simulate multiple servers, logs, and parsers.
   */
  private static void verifyMapStringStartsWith(Map<String, String> expected, Map<String, String> actual) {
    assertEquals(expected.size(), actual.size());
    assertEquals(expected.keySet(), actual.keySet());
    expected.keySet().forEach(key -> {
      Object expectedVal = expected.get(key);
      Object actualVal = actual.get(key);

      if (expectedVal instanceof String && actualVal instanceof String) {
        assertTrue(((String) actualVal).startsWith((String)expectedVal));
      } else {
        assertEquals(expected.get(key), actual.get(key));
      }
    });
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
