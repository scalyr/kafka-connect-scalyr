package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalyr.integrations.kafka.mapping.EventMapper;
import com.scalyr.integrations.kafka.TestUtils.TriFunction;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Test ScalyrSinkTask
 */
@RunWith(Parameterized.class)
public class ScalyrSinkTaskTest {

  private ScalyrSinkTask scalyrSinkTask;

  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final int numServers = 5;
  private static final int numLogFiles = 3;
  private static final int numParsers = 2;

  private final TriFunction<Integer, Integer, Integer, Object> recordValue;
  /**
   * Create test parameters for each SinkRecordValueCreator type.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> testParams() {
    return TestUtils.multipleRecordValuesTestParams();
  }

  public ScalyrSinkTaskTest(TriFunction<Integer, Integer, Integer, Object> recordValue) {
    this.recordValue = recordValue;
  }

  @Before
  public void setup() {
    this.scalyrSinkTask = new ScalyrSinkTask();
  }

  /**
   * Verify start doesn't throw Exception
   */
  @Test
  public void testStart()  {
    scalyrSinkTask.start(createConfig());
  }

  /**
   * End-to-End put test with SinkRecord -> EventMapper -> AddEventsClient -> Mock Web Server addEvents API
   */
  @Test
  public void testPut() throws Exception {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200));

    Map<String, String> configMap = createConfig();
    configMap.put(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, server.url("").toString());
    configMap.put(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, "abc123");
    scalyrSinkTask.start(configMap);

    // put SinkRecords
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, 100, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);

    // Verify sink records are sent to addEvents API
    EventMapper eventMapper = new EventMapper();
    List<Event> events = records.stream()
      .map(eventMapper::createEvent)
      .collect(Collectors.toList());
    ObjectMapper objectMapper = new ObjectMapper();
    RecordedRequest request = server.takeRequest();
    Map<String, Object> parsedEvents = objectMapper.readValue(request.getBody().readUtf8(), Map.class);
    AddEventsClientTest.validateEvents(events, new ScalyrSinkConnectorConfig(configMap), parsedEvents);
  }

  /**
   * Verify RetriableException is thrown when Scalyr server returns non-200
   */
  @Test(expected = RetriableException.class)
  public void testPutWithBackoff() {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(429).setBody("{status: serverTooBusy}"));

    Map<String, String> config = createConfig();
    config.put(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, server.url("").toString());
    scalyrSinkTask.start(config);

    // put SinkRecords
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, 10, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
  }


  /**
   * Currently flush does nothing.
   */
  @Test
  public void testFlush() {
    Map<String, String> config = createConfig();
    scalyrSinkTask.start(config);

    scalyrSinkTask.flush(new HashMap<>());
  }

  /**
   * Verify stop doesn't throw any exceptions
   */
  @Test
  public void testStop() {
    Map<String, String> config = createConfig();
    scalyrSinkTask.start(config);

    scalyrSinkTask.stop();
  }

  private Map<String, String> createConfig() {
    return TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, "http://localhost",
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, "abc123");
  }
}
