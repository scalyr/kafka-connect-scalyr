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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.AddEventsClient.AddEventsRequest;
import com.scalyr.integrations.kafka.AddEventsClient.AddEventsResponse;
import okhttp3.Headers;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.asynchttpclient.AsyncHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.scalyr.integrations.kafka.TestUtils.fails;
import static com.scalyr.integrations.kafka.TestUtils.MockRunWithDelay;
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
  @VisibleForTesting static final String EVENTS = "events";
  private static final String LOG_ID = "log";
  private static final String LOGS = "logs";
  private static final String ID = "id";
  private static final String MESSAGE = "message";
  private static final String PARSER = "parser";
  private static final String ORIG_SERVERHOST = "__origServerHost";
  private static final String LOGFILE = "logfile";

  private static final int numServers = 5;
  private static final int numLogFiles = 3;
  private static final int numParsers = 2;

  private static final String expectedUserAgent = "KafkaConnector/" + VersionUtil.getVersion()
    + " JVM/" + System.getProperty("java.version");

  private MockWebServer server;
  private String scalyrUrl;
  private Compressor compressor;
  private Compressor deflateCompressor;
  private AddEventsClient addEventsClient;

  @Before
  public void setup() {
    server = new MockWebServer();
    scalyrUrl = server.url("/").toString();
    this.compressor = CompressorFactory.getCompressor(CompressorFactory.NONE, null);
    this.deflateCompressor = CompressorFactory.getCompressor(CompressorFactory.DEFLATE, 3);

    // We disable payload logging so we don't get very large raw payload messages in the log output
    Configurator.setLevel("com.scalyr.integrations.kafka.eventpayload", Level.OFF);
  }

  @After
  public void tearDown() {
    if (addEventsClient != null) {
      addEventsClient.close();
      addEventsClient = null;
    }
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
   * Verify JSON in Event message serialization
   */
  @Test
  public void jsonMessageTest() throws IOException {
    final String jsonMsg = "{\"k1\":\"v1\", \"k2\":\"v2\", \"k3\": 1.0, \"k4\": {\"k1\":\"v1\", \"k2\":\"v2\"}}";
    List<Event> events = TestUtils.createTestEvents(10, jsonMsg, 1, 1, 1);
    createAndVerifyAddEventsRequest(events);
  }

  /**
   * Create `numEvents` with the specified `numServers`, `numLogFiles`, `numParsers`
   * and verify the AddEventsRequest is serialized correctly to JSON.
   */
  private void addEventsRequestTest(int numEvents, int numServers, int numLogFiles, int numParsers) throws IOException {
    List<Event> events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
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
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
    List<Event> events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
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
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
    for (int i = 0; i < numRequests; i++) {
      // Create addEvents request
      List<Event> events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
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
    MockRunWithDelay mockRunWithDelay = new MockRunWithDelay();
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor, mockRunWithDelay.runWithDelay);

    // Server Too Busy
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(ADD_EVENTS_RESPONSE_SERVER_BUSY));
    AddEventsResponse addEventsResponse = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.SECONDS);
    assertEquals("serverTooBusy", addEventsResponse.getStatus());
    assertEquals(requestCount += EXPECTED_NUM_RETRIES, server.getRequestCount());
    assertEquals(EXPECTED_DELAY_TIME_MS, mockRunWithDelay.delayTime.get());

    // Client Bad Request
    mockRunWithDelay.reset();
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_CLIENT_BAD_PARAM));
    addEventsResponse = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.SECONDS);
    assertEquals(AddEventsResponse.CLIENT_BAD_PARAM, addEventsResponse.getStatus());
    assertEquals(++requestCount, server.getRequestCount());
    assertEquals(0, mockRunWithDelay.delayTime.get());

    // Input too long
    mockRunWithDelay.reset();
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_INPUT_TOO_LONG));
    addEventsResponse = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.SECONDS);
    assertTrue(addEventsResponse.isSuccess());
    assertTrue(addEventsResponse.hasIgnorableError());
    assertFalse(addEventsResponse.isRetriable());
    assertEquals(++requestCount, server.getRequestCount());
    assertEquals(0, mockRunWithDelay.delayTime.get());

    // Empty Response
    mockRunWithDelay.reset();
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(200).setBody(""));
    addEventsResponse = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.SECONDS);
    assertEquals("emptyResponse", addEventsResponse.getStatus());
    assertEquals(requestCount + EXPECTED_NUM_RETRIES, server.getRequestCount());
    assertEquals(EXPECTED_DELAY_TIME_MS, mockRunWithDelay.delayTime.get());

    // IOException
    // Doesn't actually hit MockHttpServer, so cannot advance MockableTimer.
    mockRunWithDelay.reset();
    addEventsClient = new AddEventsClient("http://localhost", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor, mockRunWithDelay.runWithDelay);
    addEventsResponse = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1)).get(5, TimeUnit.MINUTES);
    assertEquals("addEvents error", addEventsResponse.getStatus());
    assertEquals(EXPECTED_DELAY_TIME_MS, mockRunWithDelay.delayTime.get());
  }

  /**
   * Verify dependent requests success case
   */
  @Test
  public void testDependentRequestsSuccess() throws Exception {
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);

    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

    // Send requests
    List<Event> firstTestEvent = TestUtils.createTestEvents(1, 1, 1, 1);
    firstTestEvent.get(0).setMessage("First");
    CompletableFuture<AddEventsResponse> request1 = addEventsClient.log(firstTestEvent);

    List<Event> secondTestEvent = TestUtils.createTestEvents(1, 1, 1, 1);
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
    MockRunWithDelay mockRunWithDelay = new MockRunWithDelay();
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor, mockRunWithDelay.runWithDelay);

    // MockWebServer response with server busy response for first request
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(ADD_EVENTS_RESPONSE_SERVER_BUSY));
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

    CompletableFuture<AddEventsResponse> request1 = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1));
    CompletableFuture<AddEventsResponse> request2 = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1), request1);

    AddEventsResponse addEventsResponse1 = request1.get(5, TimeUnit.SECONDS);
    AddEventsResponse addEventsResponse2 = request2.get(5, TimeUnit.SECONDS);
    assertEquals(addEventsResponse1, addEventsResponse2);
    assertFalse(addEventsResponse1.isSuccess());
    assertEquals(EXPECTED_NUM_RETRIES, server.getRequestCount());
    assertEquals(EXPECTED_DELAY_TIME_MS, mockRunWithDelay.delayTime.get());
  }

  /**
   * Verify AddEventsResponse with Timeout is returned when request times out on dependent request completion.
   */
  @Test
  public void testAddEventsTimeout() throws Exception {
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, 1, ADD_EVENTS_RETRY_DELAY_MS, compressor);

    CompletableFuture<AddEventsResponse> fakeSlowPendingRequest = new CompletableFuture<>();
    CompletableFuture<AddEventsResponse> request2 = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1), fakeSlowPendingRequest);
    AddEventsResponse addEventsResponse2 = request2.get(5, TimeUnit.SECONDS);
    assertFalse(addEventsResponse2.isSuccess());
    assertTrue(addEventsResponse2.getMessage().contains("TimeoutException"));
  }

  /**
   * Test retries without mock retryWithDelay, which will cause the retry logic to delay during retries.
   * This test should not be run as part of the automated unit tests because it is slow.
   */
  @Ignore("Test is slow and should not be included in automated testing")
  @Test
  public void testRetryWithoutMockRetryWithDelay() throws Exception {
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);

    // Server Too Busy
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(ADD_EVENTS_RESPONSE_SERVER_BUSY));
    AddEventsResponse addEventsResponse = addEventsClient.log(TestUtils.createTestEvents(1, 1, 1, 1)).get(ADD_EVENTS_TIMEOUT_MS, TimeUnit.SECONDS);
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
    try (AddEventsClient addEventsClient = new AddEventsClient("http://localhost:63232", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor)) {}
    try (AddEventsClient addEventsClient = new AddEventsClient("https://app.scalyr.com", API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor)) {};
  }

  /**
   * Test AddEventsRequest with different compression types
   */
  @Test
  public void testCompression() {
    Stream<String> compressionTypes = Stream.of(CompressorFactory.DEFLATE, CompressorFactory.ZSTD);
    compressionTypes.forEach(compressionType -> {
      compressor = CompressorFactory.getCompressor(compressionType, null);
      testSingleRequestWithCompression();
    });
  }

  /**
   * Verify Add Events Requests that exceed maximum add events payload size are not sent.
   */
  @Test
  public void testTooLargeAddEventsSkippedUncompressedRequest() throws Exception {
    final int numEvents = 6;
    final byte[] largeMsgBytes = new byte[1000000];
    Arrays.fill(largeMsgBytes, (byte)'a');
    final String largeMsg = new String(largeMsgBytes);

    // Setup Mock Server
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

    // Create addEvents request
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor);
    List<Event> events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
    events.forEach(event -> event.setMessage(largeMsg));

    assertEquals(0, server.getRequestCount());

    CompletableFuture<AddEventsResponse> initialAddEventsResponseFuture = addEventsClient.log(events);
    AddEventsResponse initialAddEventsResponse = initialAddEventsResponseFuture.get(5, TimeUnit.SECONDS);
    assertEquals(AddEventsResponse.SUCCESS, initialAddEventsResponse.getStatus());
    assertEquals("Skipped due to payload too large", initialAddEventsResponse.getMessage());

    // request should be skipped
    assertEquals(0, server.getRequestCount());

    // Send next batch that is smaller than max payload size
    events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
    AddEventsResponse addEventsResponse = addEventsClient.log(events, initialAddEventsResponseFuture).get(5, TimeUnit.SECONDS);
    assertEquals(AddEventsResponse.SUCCESS, addEventsResponse.getStatus());
    assertEquals("success", addEventsResponse.getMessage());

    // request should succeed
    assertEquals(1, server.getRequestCount());

    // Verify request
    ObjectMapper objectMapper = new ObjectMapper();
    RecordedRequest request = server.takeRequest();
    Map<String, Object> parsedEvents = objectMapper.readValue(request.getBody().inputStream(), Map.class);
    validateEvents(events, parsedEvents);
    verifyHeaders(request.getHeaders());
  }

  /**
   * Verify Add Events Requests that exceed maximum add events payload size before compression are not sent.
   *
   * In this scenario we test a request which is larger than 6 MB before compression, but smaller than 6 MB
   * after compression.
   */
  @Test
  public void testTooLargeAddEventsSkippedDeflateCompressedRequest() throws Exception {
    final int numEvents = 6;
    final byte[] largeMsgBytes = new byte[1000000];
    Arrays.fill(largeMsgBytes, (byte)'a');
    final String largeMsg = new String(largeMsgBytes);

    // Setup Mock Server
    server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

    // Create addEvents request
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, deflateCompressor);
    List<Event> events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
    events.forEach(event -> event.setMessage(largeMsg));

    assertEquals(0, server.getRequestCount());

    CompletableFuture<AddEventsResponse> initialAddEventsResponseFuture = addEventsClient.log(events);
    AddEventsResponse initialAddEventsResponse = initialAddEventsResponseFuture.get(5, TimeUnit.SECONDS);
    assertEquals(AddEventsResponse.SUCCESS, initialAddEventsResponse.getStatus());
    assertEquals("Skipped due to payload too large", initialAddEventsResponse.getMessage());

    // request should be skipped
    assertEquals(0, server.getRequestCount());

    // Send next batch that is smaller than max payload size
    events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
    AddEventsResponse addEventsResponse = addEventsClient.log(events, initialAddEventsResponseFuture).get(5, TimeUnit.SECONDS);
    assertEquals(AddEventsResponse.SUCCESS, addEventsResponse.getStatus());
    assertEquals("success", addEventsResponse.getMessage());

    // request should succeed
    assertEquals(1, server.getRequestCount());

    // Verify request
    ObjectMapper objectMapper = new ObjectMapper();
    RecordedRequest request = server.takeRequest();

    InputStream requestBodyInputStream = new BufferedInputStream(request.getBody().inputStream());
    byte[] requestBody = ByteStreams.toByteArray(requestBodyInputStream);
    byte[] decompressedRequestBody = addEventsClient.getDecompressedPayload(requestBody);
    Map<String, Object> parsedEvents = objectMapper.readValue(decompressedRequestBody, Map.class);
    validateEvents(events, parsedEvents);
    verifyHeaders(request.getHeaders(), deflateCompressor);
  }

  @Test
  public void testGetDecompressedPayloadFailedToDecompress() {
    addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, deflateCompressor);
    byte[] decompressedPayload = addEventsClient.getDecompressedPayload("invalid".getBytes());
    assertEquals("unable to decompress the payload", new String(decompressedPayload));
  }

  @Test
  public void testHttpResource() {
    final AsyncHttpClient client1 = AddEventsClient.HttpResource.acquire();
    final AsyncHttpClient client2 = AddEventsClient.HttpResource.acquire();
    assertEquals(client1, client2);

    AddEventsClient.HttpResource.release();
    AddEventsClient.HttpResource.release();
  }

  /**
   * Create a single addEvents Request and verify the request body and header
   */
  private void testSingleRequestWithCompression() {
    try (AddEventsClient addEventsClient = new AddEventsClient(scalyrUrl, API_KEY_VALUE, ADD_EVENTS_TIMEOUT_MS, ADD_EVENTS_RETRY_DELAY_MS, compressor)) {
      final int numEvents = 10;

      // Setup Mock Server
      server.enqueue(new MockResponse().setResponseCode(200).setBody(ADD_EVENTS_RESPONSE_SUCCESS));

      // Create addEvents request
      List<Event> events = TestUtils.createTestEvents(numEvents, numServers, numLogFiles, numParsers);
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
    verifyHeaders(headers, this.compressor);
  }

  /**
   * Verify HTTP request headers are set correctly.
   */
  private void verifyHeaders(Headers headers, Compressor compressor) {
    assertEquals(MediaType.APPLICATION_JSON, headers.get("Content-type"));
    assertEquals(MediaType.APPLICATION_JSON, headers.get("Accept"));
    assertEquals("Keep-Alive", headers.get("Connection"));
    assertEquals(expectedUserAgent, headers.get("User-Agent"));
    assertEquals(compressor.getContentEncoding(), headers.get("Content-Encoding"));
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
    assertEquals(origEvent.getServerHost(), logLevelAttrs.get(ORIG_SERVERHOST));
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
