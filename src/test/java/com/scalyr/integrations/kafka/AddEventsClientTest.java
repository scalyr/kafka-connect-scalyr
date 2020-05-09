package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.AddEventsClient.AddEventsRequest;
import com.scalyr.integrations.kafka.AddEventsClient.AddEventsResponse;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.http.entity.ContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.scalyr.integrations.kafka.TestUtils.fails;
import static com.scalyr.integrations.kafka.TestUtils.MockSleep;
import static com.scalyr.integrations.kafka.TestValues.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test case for AddEventsClient
 * 1. Verifies AddEventsRequest JSON
 * 2. Sets up MockWebServer to verify AddEventsClients sends proper POST requests
 */
public class AddEventsClientTest {

  private static final String TOKEN  = "token";
  private static final String SESSION  = "session";
  private static final String TIMESTAMP = "ts";
  private static final String SEQUENCE_ID = "si";
  private static final String SEQUENCE_NUM = "sn";
  private static final String ATTRS = "attrs";
  private static final String EVENTS = "events";
  private static final String LOG_ID = "log";
  private static final String LOGS = "logs";
  private static final String ID = "id";
  private static final String MESSAGE = "message";
  private static final String PARSER = "parser";
  private static final String SERVERHOST = "source";
  private static final String LOGFILE = "logfile";

  private static final int numServers = 5;
  private static final int numLogFiles = 3;
  private static final int numParsers = 2;

  private static final String expectedUserAgent = "KafkaConnector/" + VersionUtil.getVersion()
    + " JVM/" + System.getProperty("java.version");

  private MockWebServer server;
  private String scalyrUrl;
  private Compressor compressor;

  @Before
  public void setup() {
    server = new MockWebServer();
    scalyrUrl = server.url("/").toString();
    this.compressor = CompressorFactory.getCompressor(CompressorFactory.NONE, null);
  }

  @After
  public void tearDown() {
    ScalyrUtil.removeCustomTime();
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
      .setToken(API_KEY_VALUE)
      .setSession(sessionId)
      .setEvents(events);

    // Serialize AddEventsRequest to JSON
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    addEventsRequest.writeJson(os);

    // Verify AddEvents JSON
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> parsedEvents = objectMapper.readValue(new ByteArrayInputStream(os.toByteArray()), Map.class);
    validateEvents(events, parsedEvents);
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
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

    // Create addEvents request
    AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
    List<Event> events = createTestEvents(numEvents, numServers, numLogFiles, numParsers);
    addEventsClient.log(events);

    // Verify request
    ObjectMapper objectMapper = new ObjectMapper();
    RecordedRequest request = server.takeRequest();
    Map<String, Object> parsedEvents = objectMapper.readValue(request.getBody().inputStream(), Map.class);
    validateEvents(events, parsedEvents);
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
    IntStream.range(0, numRequests).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS)));

    // Create and verify addEvents requests
    ObjectMapper objectMapper = new ObjectMapper();
    AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
    for (int i = 0; i < numRequests; i++) {
      // Create addEvents request
      List<Event> events = createTestEvents(numEvents, numServers, numLogFiles, numParsers);
      addEventsClient.log(events);

      // Verify addEvents request
      RecordedRequest request = server.takeRequest();
      Map<String, Object> parsedEvents = objectMapper.readValue(request.getBody().inputStream(), Map.class);
      validateEvents(events, parsedEvents);
      verifyHeaders(request.getHeaders());
    }
  }

  /**
   * Verify AddEventsResponse contains correct errors.
   * Uses `ScalyrUtil` mockable clock to mock time for retry attempts.
   * Uses `CustomActionDispatcher` to advance mockable clock when a request occurs.
   */
  @Test
  public void testAddEventsClientErrors() throws Exception {
    int requestCount = 0;
    MockSleep mockSleep = new MockSleep();
    AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor, mockSleep.sleep);

    // Server Too Busy
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(ADD_EVENTS_RESPONSE_SERVER_BUSY));
    AddEventsResponse addEventsResponse = addEventsClient.log(createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.SECONDS);
    assertEquals("serverTooBusy", addEventsResponse.getStatus());
    assertEquals(requestCount += EXPECTED_NUM_RETRIES, server.getRequestCount());
    assertEquals(EXPECTED_SLEEP_TIME_MS, mockSleep.sleepTime.get());

    // Client Bad Request
    mockSleep.reset();
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_CLIENT_BAD_PARAM));
    addEventsResponse = addEventsClient.log(createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.SECONDS);
    assertEquals(AddEventsResponse.CLIENT_BAD_PARAM, addEventsResponse.getStatus());
    assertEquals(++requestCount, server.getRequestCount());
    assertEquals(0, mockSleep.sleepTime.get());

    // Empty Response
    mockSleep.reset();
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(200).setBody(""));
    addEventsResponse = addEventsClient.log(createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.SECONDS);
    assertEquals("emptyResponse", addEventsResponse.getStatus());
    assertEquals(requestCount + EXPECTED_NUM_RETRIES, server.getRequestCount());
    assertEquals(EXPECTED_SLEEP_TIME_MS, mockSleep.sleepTime.get());

    // IOException
    // Doesn't actually hit MockHttpServer, so cannot advance MockableTimer.
    mockSleep.reset();
    addEventsClient = new AddEventsClient("http://localhost", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor, mockSleep.sleep);
    addEventsResponse = addEventsClient.log(createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.MINUTES);
    assertEquals("IOException", addEventsResponse.getStatus());
    assertEquals(EXPECTED_SLEEP_TIME_MS, mockSleep.sleepTime.get());
  }

  /**
   * Verify dependent requests success case
   */
  @Test
  public void testDependentRequestsSuccess() throws Exception {
    AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);

    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

    // Send requests
    List<Event> firstTestEvent = createTestEvents(1, 1, 1, 1);
    firstTestEvent.get(0).setMessage("First");
    CompletableFuture<AddEventsResponse> request1 = addEventsClient.log(firstTestEvent);

    List<Event> secondTestEvent = createTestEvents(1, 1, 1, 1);
    secondTestEvent.get(0).setMessage("Second");
    CompletableFuture<AddEventsResponse> request2 = addEventsClient.log(secondTestEvent, request1);

    // Verify response success
    assertNotEquals(request1, request2);
    AddEventsResponse addEventsResponse1 = request1.get(5, TimeUnit.SECONDS);
    AddEventsResponse addEventsResponse2 = request2.get(5, TimeUnit.SECONDS);
    assertNotEquals(addEventsResponse1, addEventsResponse2);
    assertTrue(addEventsResponse1.isSuccess());
    assertTrue(addEventsResponse2.isSuccess());
    assertEquals(2, server.getRequestCount());

    // Verify order of requests received
    // First Request
    RecordedRequest request1Payload = server.takeRequest();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    request1Payload.getBody().writeTo(baos);
    assertTrue(baos.toString().contains("\"message\":\"First\""));

    // Second request
    RecordedRequest request2Payload = server.takeRequest();
    baos = new ByteArrayOutputStream();
    request2Payload.getBody().writeTo(baos);
    assertTrue(baos.toString().contains("\"message\":\"Second\""));
  }

  /**
   * Verify dependent requests - dependent request fails
   * Dependent request failure future should be returned for second request.
   *
   * Uses `ScalyrUtil` mockable clock to mock time for retry attempts.
   * Uses `CustomActionDispatcher` to advance mockable clock when a request occurs.
   */
  @Test
  public void testDependentRequestsError() throws Exception {
    MockSleep mockSleep = new MockSleep();
    AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor, mockSleep.sleep);

    // MockWebServer response with server busy response for first request
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(ADD_EVENTS_RESPONSE_SERVER_BUSY));
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

    CompletableFuture<AddEventsResponse> request1 = addEventsClient.log(createTestEvents(1, 1, 1, 1));
    CompletableFuture<AddEventsResponse> request2 = addEventsClient.log(createTestEvents(1, 1, 1, 1), request1);

    AddEventsResponse addEventsResponse1 = request1.get(5, TimeUnit.SECONDS);
    AddEventsResponse addEventsResponse2 = request2.get(5, TimeUnit.SECONDS);
    assertEquals(addEventsResponse1, addEventsResponse2);
    assertFalse(addEventsResponse1.isSuccess());
    assertEquals(EXPECTED_NUM_RETRIES, server.getRequestCount());
    assertEquals(EXPECTED_SLEEP_TIME_MS, mockSleep.sleepTime.get());
  }

  /**
   * Verify AddEventsResponse with Timeout is returned when request times out on dependent request completion.
   */
  @Test
  public void testAddEventsTimeout() throws Exception {
    AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, 1, ADD_EVENTS_RETRY_DELAY_MS, compressor);

    CompletableFuture<AddEventsResponse> fakeSlowPendingRequest = new CompletableFuture<>();
    CompletableFuture<AddEventsResponse> request2 = addEventsClient.log(createTestEvents(1, 1, 1, 1), fakeSlowPendingRequest);
    AddEventsResponse addEventsResponse2 = request2.get(5, TimeUnit.SECONDS);
    assertFalse(addEventsResponse2.isSuccess());
    assertTrue(addEventsResponse2.getMessage().contains("TimeoutException"));
  }

  /**
   * Test retries without fake clock, which will cause the retry logic to sleep during retries.
   * This test should not be run as part of the automated unit tests because it is slow.
   */
  @Ignore("Test is slow and should not be included in automated testing")
  @Test
  public void testRetryWithoutFakeClock() throws Exception {
    AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);

    // Server Too Busy
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(ADD_EVENTS_RESPONSE_SERVER_BUSY));
    AddEventsResponse addEventsResponse = addEventsClient.log(createTestEvents(1, 1, 1, 1)).get(ADD_EVENTS_TIMEOUT_MS, TimeUnit.SECONDS);
    assertEquals("serverTooBusy", addEventsResponse.getStatus());
    assertEquals(EXPECTED_NUM_RETRIES, server.getRequestCount());
  }

  /**
   * Verify URL validation for incorrect and correct URLs
   */
  @Test
  public void testUrlValidation() {
    // Invalid
    fails(() -> new AddEventsClient("app.scalyr.com", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor), IllegalArgumentException.class);
    fails(() -> new AddEventsClient("http://app.scalyr.com", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor), IllegalArgumentException.class);

    // Valid
    new AddEventsClient("http://localhost:63232", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
    new AddEventsClient("https://app.scalyr.com", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
  }

  /**
   * Test AddEventsRequest with different compression types
   */
  @Test
  public void testCompression() {
    Stream<String> compressionTypes = Stream.of(CompressorFactory.DEFLATE);
    compressionTypes.forEach(compressionType -> {
      compressor = CompressorFactory.getCompressor("deflate", null);
      testSingleRequestWithCompression();
    });
  }

  /**
   * Create a single addEvents Request and verify the request body and header
   */
  private void testSingleRequestWithCompression() {
    try {
      final int numEvents = 10;

      // Setup Mock Server
      server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

      // Create addEvents request
      AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
      List<Event> events = createTestEvents(numEvents, numServers, numLogFiles, numParsers);
      addEventsClient.log(events);

      // Verify request
      ObjectMapper objectMapper = new ObjectMapper();
      RecordedRequest request = server.takeRequest();
      Map<String, Object> parsedEvents = objectMapper.readValue(compressor.newStreamDecompressor(request.getBody().inputStream()), Map.class);
      validateEvents(events, parsedEvents);
      verifyHeaders(request.getHeaders());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Verify HTTP request headers are set correctly.
   */
  private void verifyHeaders(Headers headers) {
    assertEquals(ContentType.APPLICATION_JSON.toString(), headers.get("Content-type"));
    assertEquals(ContentType.APPLICATION_JSON.toString(), headers.get("Accept"));
    assertEquals("Keep-Alive", headers.get("Connection"));
    assertEquals(expectedUserAgent, headers.get("User-Agent"));
    assertEquals(compressor.getContentEncoding(), headers.get("Content-Encoding"));
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
          .setOffset(i)
          .setMessage(TestValues.MESSAGE_VALUE)
          .setParser(TestValues.PARSER_VALUE + logFileNum % numParsers)
          .setLogfile(TestValues.LOGFILE_VALUE + logFileNum)
          .setServerHost(TestValues.SERVER_VALUE + random.nextInt(numServers))
          .setTimestamp(ScalyrUtil.nanoTime())
          .addAdditionalAttr("app", "test")
          .addAdditionalAttr("isTest", true)
          .addAdditionalAttr("version", 2.3)
          .setEnrichmentAttrs(ENRICHMENT_VALUE_MAP);
      })
      .collect(Collectors.toList());
  }

  /**
   * Validate Scalyr addEvents payload has correct format and values
   */
  public static void validateEvents(List<Event> origEvents, Map<String, Object> sessionEvents) {
    assertNotNull(sessionEvents.get(SESSION));
    assertEquals(API_KEY_VALUE, sessionEvents.get(TOKEN));
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
    assertTrue(((Number)event.get(TIMESTAMP)).longValue() <= ScalyrUtil.nanoTime());
    Map eventAttrs = (Map) event.get(ATTRS);
    assertEquals(origEvent.getMessage(), eventAttrs.get(MESSAGE));

    // Verify log level attrs
    Map logLevelAttrs = (Map) logIdAttrs.get(logId);
    assertEquals(origEvent.getServerHost(), logLevelAttrs.get(SERVERHOST));
    assertEquals(origEvent.getLogfile(), logLevelAttrs.get(LOGFILE));
    assertEquals(origEvent.getParser(), logLevelAttrs.get(PARSER));
    if (origEvent.getEnrichmentAttrs() != null) {
      origEvent.getEnrichmentAttrs().forEach((key, value) -> assertEquals(value, logLevelAttrs.get(key)));
    }

    // Verify additional event attrs
    if (origEvent.getAdditionalAttrs() != null) {
      origEvent.getAdditionalAttrs().forEach((key, value) -> assertEquals(value, eventAttrs.get(key)));

      // Verify no unexpected attr.  +1 for message field in eventAttrs
      assertEquals(origEvent.getAdditionalAttrs().size() + 1, eventAttrs.size());
    }
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
