package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalyr.integrations.kafka.mapping.EventMapper;
import com.scalyr.integrations.kafka.TestUtils.TriFunction;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.kafka.connect.errors.ConnectException;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.scalyr.integrations.kafka.TestUtils.fails;
import static org.junit.Assert.assertEquals;

/**
 * Test ScalyrSinkTask
 */
@RunWith(Parameterized.class)
public class ScalyrSinkTaskTest {

  private ScalyrSinkTask scalyrSinkTask;
  private final TriFunction<Integer, Integer, Integer, Object> recordValue;

  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final int numServers = 5;
  private static final int numLogFiles = 3;
  private static final int numParsers = 2;
  private static final int concurrencyLimit = 3;

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
    AddEventsClient.delayTimeMs = 1;
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

    startTask(server);

    // put SinkRecords
    putAndVerifyRecords(server);
  }

  private void startTask(MockWebServer server) {
    Map<String, String> configMap = createConfig();
    configMap.put(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, server.url("").toString());
    scalyrSinkTask.start(configMap);
  }

  private void putAndVerifyRecords(MockWebServer server) throws InterruptedException, java.io.IOException {
    server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS));

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
    Map<String, Object> parsedEvents = objectMapper.readValue(request.getBody().inputStream(), Map.class);
    AddEventsClientTest.validateEvents(events, parsedEvents);
  }

  /**
   * Verify multiple cycles of puts followed by flush work as expected.
   */
  @Test
  public void testPutFlushCycles() throws Exception {
    final int numCycles = 5;  // cycle = multiple puts followed by a flush
    final int numPuts = 10;
    MockWebServer server = new MockWebServer();

    startTask(server);

    for (int cycle = 0; cycle < numCycles; cycle++) {
      for (int put = 0; put < numPuts; put++) {
        putAndVerifyRecords(server);
      }
      scalyrSinkTask.flush(new HashMap<>());
    }
  }

  /**
   * Verify exception is thrown when concurrency limit is exceeded
   * and add events request is not complete within add events timeout
   */
  @Test(expected = RetriableException.class)
  public void testMaxConcurrentRequestsExceededTimeout() {
    MockWebServer server = new MockWebServer();
    Map<String, String> configMap = createConfig();
    configMap.put(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, server.url("").toString());
    configMap.put(ScalyrSinkConnectorConfig.ADD_EVENTS_TIMEOUT_MS_CONFIG, "1");
    scalyrSinkTask.start(configMap);


    // MockWebServer response with delay
    server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS)
      .setHeadersDelay(3, TimeUnit.SECONDS));

    IntStream.range(0, concurrencyLimit).forEach(i -> {
      List<SinkRecord> records = TestUtils.createRecords(topic, partition, 100, recordValue.apply(numServers, numLogFiles, numParsers));
      scalyrSinkTask.put(records);
    });
  }

  /**
   * When the concurrency limit is exceeded, put will block waiting for earlier requests to finish.
   * Verify requests after concurrency limit is exceeded wait for slow requests to finish.
   */
  @Test
  public void testMaxConcurrentRequestsExceeded() {
    MockWebServer server = new MockWebServer();
    startTask(server);

    // MockWebServer response with delay for first request
    server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS)
      .setHeadersDelay(1, TimeUnit.SECONDS));
    IntStream.range(0, concurrencyLimit - 1).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS)));

    IntStream.range(0, concurrencyLimit).forEach(i -> {
      List<SinkRecord> records = TestUtils.createRecords(topic, partition, 100, recordValue.apply(numServers, numLogFiles, numParsers));
      scalyrSinkTask.put(records);
    });

    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(concurrencyLimit, server.getRequestCount());
  }

  /**
   * Tests the following error logic:
   * 1. put returns errors for all requests after an error has occurred
   * 2. flush clears the errors
   * 3. Subsequent puts succeed
   */
  @Test
  public void testPutErrorHandling() {
    final int numRequests = 3;
    int requestCount = 0;
    MockWebServer server = new MockWebServer();
    startTask(server);

    // Error first request
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(TestValues.ADD_EVENTS_RESPONSE_SERVER_BUSY));
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, 100, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(requestCount += AddEventsClient.maxRetries, server.getRequestCount());

    // Additional requests should have errors
    final int currentRequestCount = requestCount;
    IntStream.range(0, numRequests).forEach(i -> {
      fails(() -> scalyrSinkTask.put(records), RetriableException.class);
      assertEquals(currentRequestCount, server.getRequestCount());
    });

    // Flush throws exception and clears errors
    fails(() -> scalyrSinkTask.flush(new HashMap<>()), RetriableException.class);

    // Subsequent requests should succeed
    server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS));
    scalyrSinkTask.put(records);
    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(++requestCount, server.getRequestCount());
  }


  /**
   * Verify ConnectException is thrown when server return client bad param.
   * ConnectException will stop this Task.
   */
  @Test
  public void testBadClientParam() {
    MockWebServer server = new MockWebServer();
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_CLIENT_BAD_PARAM));

    startTask(server);

    // put SinkRecords
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, 10, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
    fails(() -> {scalyrSinkTask.flush(new HashMap<>()); return null;}, t -> t instanceof ConnectException && !(t instanceof RetriableException));
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
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, TestValues.API_KEY_VALUE,
      ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG, CompressorFactory.NONE);
  }
}
