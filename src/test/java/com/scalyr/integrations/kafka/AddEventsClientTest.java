package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.mapping.TestValues;
import com.scalyr.integrations.kafka.AddEventsClient.AddEventsRequest;
import com.squareup.okhttp.Headers;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.apache.http.entity.ContentType;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test case for AddEventsClient
 * 1. Verifies AddEventsRequest JSON
 * 2. Sets up MockWebServer to verify AddEventsClients sends proper POST requests
 */
public class AddEventsClientTest {

  public static final String TOKEN  = "token";
  public static final String SESSION  = "session";
  public static final String TIMESTAMP = "ts";
  public static final String SEQUENCE_ID = "si";
  public static final String SEQUENCE_NUM = "sn";
  public static final String ATTRS = "attrs";
  public static final String EVENTS = "events";
  public static final String LOG_ID = "log";
  public static final String LOGS = "logs";
  public static final String ID = "id";
  public static final String MESSAGE = "message";
  public static final String PARSER = "parser";
  public static final String SERVERHOST = "origin";
  public static final String LOGFILE = "logfile";

  private static final int numServers = 5;
  private static final int numLogFiles = 3;
  private static final int numParsers = 2;

  private MockWebServer server;
  private ScalyrSinkConnectorConfig config;

  @Before
  public void setup() {
    server = new MockWebServer();
    Map<String, String> configMap = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, server.url("/").toString(),
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, "abc123");

    this.config = new ScalyrSinkConnectorConfig(configMap);
  }

  /**
   * Verify AddEventsRequest creation and JSON serialization
   */
  @Test
  public void addEventsRequestTest() throws IOException {
    // Single log id
    addEventsRequestTest(10, 1, 1, 1);

    // Multiple log ids test
    addEventsRequestTest(100, 5, 3, 2);
  }

  /**
   * Create `numEvents` with the specified `numServers`, `numLogFiles`, `numParsers`
   * and verify the AddEventsRequest is serialized correctly to JSON.
   */
  private void addEventsRequestTest(int numEvents, int numServers, int numLogFiles, int numParsers) throws IOException {
    List<Event> events = createTestEvents(numEvents, numServers, numLogFiles, numParsers);
    createAndVerifyAddEventsRequest(events);
  }

  /**
   * Performs the following:
   * 1. Create AddEventsRequest with specified events
   * 2. serializes to JSON
   * 3. Verifies JSON serialization by deserializing the JSON and verifying the JSON contents.
   */
  private void createAndVerifyAddEventsRequest(List<Event> events) throws IOException {
    final String sessionId = UUID.randomUUID().toString();

    // Create AddEventsRequest
    AddEventsRequest addEventsRequest = new AddEventsRequest()
      .setToken(config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value())
      .setSession(sessionId)
      .setEvents(events);

    // Serialize AddEventsRequest to JSON
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    addEventsRequest.writeJson(os);

    // Verify AddEvents JSON
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> parsedEvents = objectMapper.readValue(os.toString(), Map.class);
    validateEvents(events, config, parsedEvents);
  }

  /**
   * Verify null server fields are handled correctly in AddEvents request
   */
  @Test
  public void addEventRequestsNullServerFields() throws IOException {
    List<Event> events = new ArrayList<>();
    events.add(createEvent(null, TestValues.LOGFILE_VALUE, TestValues.PARSER_VALUE));
    events.add(createEvent(TestValues.SERVER_VALUE, null, TestValues.PARSER_VALUE));
    events.add(createEvent(TestValues.SERVER_VALUE, TestValues.LOGFILE_VALUE, null));
    events.add(createEvent(null, null, null));

    createAndVerifyAddEventsRequest(events);
  }

  /**
   * Creates an Event with the specified serverHost, logFile, and parser.
   */
  private Event createEvent(String serverHost, String logFile, String parser) {
    return new Event()
      .setTopic(TestValues.TOPIC_VALUE)
      .setPartition(0)
      .setOffset(1)
      .setMessage(TestValues.MESSAGE_VALUE)
      .setParser(parser)
      .setLogfile(logFile)
      .setServerHost(serverHost)
      .setTimestamp(ScalyrUtil.nanoTime());
  }

  /**
   * Basic test for single addEvent POST request
   */
  @Test
  public void testSingleRequest() throws Exception {
    final int numEvents = 10;

    // Setup Mock Server
    server.enqueue(new MockResponse().setResponseCode(200));

    // Create addEvents request
    AddEventsClient addEventsClient = new AddEventsClient(config);
    List<Event> events = createTestEvents(numEvents, numServers, numLogFiles, numParsers);
    addEventsClient.log(events);

    // Verify request
    ObjectMapper objectMapper = new ObjectMapper();
    RecordedRequest request = server.takeRequest();
    Map<String, Object> parsedEvents = objectMapper.readValue(request.getBody().readUtf8(), Map.class);
    validateEvents(events, config, parsedEvents);
    verifyHeaders(request.getHeaders());
  }

  /**
   * Test sending multiple addEvents POST requests
   */
  @Test
  public void testMultipleRequest() throws Exception {
    final int numEvents = 100;
    final int numRequests = 10;

    // Setup Mock Server
    IntStream.range(0, numRequests).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200)));

    // Create and verify addEvents requests
    ObjectMapper objectMapper = new ObjectMapper();
    AddEventsClient addEventsClient = new AddEventsClient(config);
    for (int i = 0; i < numRequests; i++) {
      // Create addEvents request
      List<Event> events = createTestEvents(numEvents, numServers, numLogFiles, numParsers);
      addEventsClient.log(events);

      // Verify addEvents request
      RecordedRequest request = server.takeRequest();
      Map<String, Object> parsedEvents = objectMapper.readValue(request.getBody().readUtf8(), Map.class);
      validateEvents(events, config, parsedEvents);
      verifyHeaders(request.getHeaders());
    }
  }

  /**
   * RuntimeException should be thrown when non-200 response code is returned
   */
  @Test(expected = RuntimeException.class)
  public void testBackoffRequest() throws Exception{
    // Setup Mock Server
    server.enqueue(new MockResponse().setResponseCode(429).setBody("{status: serverTooBusy}"));

    // Create addEvents request
    AddEventsClient addEventsClient = new AddEventsClient(config);

    addEventsClient.log(createTestEvents(1, 1, 1, 1));
  }

  /**
   * ConnectException should be thrown when server url is invalid.  ConnectException makes Kafka Connect terminate the connector task.
   */
  @Test(expected = ConnectException.class)
  public void testInvalidUrl() {
    Map<String, String> config = TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, "app.scalyr.com",
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, "abc123");

    ScalyrSinkConnectorConfig connectorConfig = new ScalyrSinkConnectorConfig(config);

    new AddEventsClient(connectorConfig);
  }

  /**
   * Verify HTTP request headers are set correctly.
   */
  private void verifyHeaders(Headers headers) {
    assertEquals(ContentType.APPLICATION_JSON.toString(), headers.get("Content-type"));
    assertEquals(ContentType.APPLICATION_JSON.toString(), headers.get("Accept"));
    assertEquals("Keep-Alive", headers.get("Connection"));
    assertEquals("Scalyr-Kafka-Connector", headers.get("User-Agent"));
  }

  /**
   * Create `numEvents` Events with the specified `numServers`, `numLogFiles`, and `numParsers` values.
   */
  private List<Event> createTestEvents(int numEvents, int numServers, int numLogFiles, int numParsers) {
    assertTrue(numParsers <= numLogFiles);
    Random random = new Random();
    return IntStream.range(0, numEvents)
      .boxed()
      .map(i -> {
        final int logFileNum = random.nextInt(numLogFiles);
        return new Event()
          .setTopic(TestValues.TOPIC_VALUE)
          .setPartition(0)
          .setOffset(1)
          .setMessage(TestValues.MESSAGE_VALUE)
          .setParser(TestValues.PARSER_VALUE + logFileNum % numParsers)
          .setLogfile(TestValues.LOGFILE_VALUE + logFileNum)
          .setServerHost(TestValues.SERVER_VALUE + random.nextInt(numServers))
          .setTimestamp(ScalyrUtil.nanoTime());
      })
      .collect(Collectors.toList());
  }

  /**
   * Validate Scalyr addEvents payload has correct format and values
   */
  public static void validateEvents(List<Event> origEvents, ScalyrSinkConnectorConfig config, Map<String, Object> sessionEvents) {
    assertNotNull(sessionEvents.get(SESSION));
    assertEquals(config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value(), sessionEvents.get(TOKEN));
    List<Map<String, Object>> logsArray = (List<Map<String, Object>>)sessionEvents.get(LOGS);
    assertNotNull(logsArray);
    List<Map<String, Object>> events = (List)sessionEvents.get(EVENTS);
    assertNotNull(events);
    assertEquals(origEvents.size(), events.size());

    Map<String, Object> logIdAttrs = logsArray.stream()
      .collect(Collectors.toMap(m -> (String)m.get(ID), m -> m.get(ATTRS)));

    IntStream.range(0, origEvents.size()).forEach(i -> validateEvent(origEvents.get(i), events.get(i), logIdAttrs));
  }

  /**
   * Validate Scalyr event matches SinkRecord
   */
  private static void validateEvent(Event origEvent, Map<String, Object> event, Map<String, Object> logIdAttrs) {
    assertEquals(5, event.size());
    assertEquals(createPartitionId(origEvent.getTopic(), origEvent.getPartition()), event.get(SEQUENCE_ID));
    String logId = (String)event.get(LOG_ID);
    assertTrue(logIdAttrs.containsKey(logId));

    assertEqualsWithNumberConversion(origEvent.getOffset(), event.get(SEQUENCE_NUM));
    assertTrue((Long)event.get(TIMESTAMP) <= ScalyrUtil.nanoTime());
    Map eventAttrs = (Map) event.get(ATTRS);
    assertEquals(origEvent.getMessage(), eventAttrs.get(MESSAGE));

    // Add log level attributes back into event attrs and make sure all the attrs are correct
    Map logLevelAttrs = (Map) logIdAttrs.get(logId);
    assertEquals(origEvent.getServerHost(), logLevelAttrs.get(SERVERHOST));
    assertEquals(origEvent.getLogfile(), logLevelAttrs.get(LOGFILE));
    assertEquals(origEvent.getParser(), logLevelAttrs.get(PARSER));
  }


  /**
   * Uniquely identify a partition with {topic name, partition id}.
   */
  static String createPartitionId(String topic, Integer partition) {
    return topic + "-" + partition;
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
