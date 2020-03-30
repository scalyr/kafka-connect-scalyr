package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.Headers;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.apache.http.entity.ContentType;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Test case for AddEventsClient
 * Sets up MockWebServer to verify AddEventsClients sends proper POST requests
 */
public class AddEventsClientTest {

  /**
   * Basic test for single request
   */
  @Test
  public void testSingleRequest() throws Exception {
    // Setup Mock Server
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200));

    // Create addEvents request
    AddEventsClient addEventsClient = new AddEventsClient(server.url("/").toString());
    Map<String, Object> testData = createTestData(1);
    addEventsClient.log(testData);

    // Verify request
    ObjectMapper objectMapper = new ObjectMapper();
    RecordedRequest request = server.takeRequest();
    assertEquals(objectMapper.writeValueAsString(testData), request.getBody().readUtf8());
    verifyHeaders(request.getHeaders());
  }

  /**
   * Test sending multiple requests
   */
  @Test
  public void testMultipleRequest() throws Exception {
    final int numRequests = 10;

    // Setup Mock Server
    MockWebServer server = new MockWebServer();
    IntStream.range(0, numRequests).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200)));

    // Create addEvents requests
    AddEventsClient addEventsClient = new AddEventsClient(server.url("/").toString());
    for (int i = 0; i < numRequests; i++) {
      addEventsClient.log(createTestData(i));
    }

    // Verify requests
    ObjectMapper objectMapper = new ObjectMapper();
    for (int i = 0; i < numRequests; i++) {
      RecordedRequest request = server.takeRequest();
      assertEquals(objectMapper.writeValueAsString(createTestData(i)), request.getBody().readUtf8());
      verifyHeaders(request.getHeaders());
    }
  }

  /**
   * RuntimeException should be thrown when non-200 response code is returned
   */
  @Test(expected = RuntimeException.class)
  public void testBackoffRequest() throws Exception{
    // Setup Mock Server
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(429).setBody("{status: serverTooBusy}"));

    // Create addEvents request
    AddEventsClient addEventsClient = new AddEventsClient(server.url("/").toString());
    Map<String, Object> testData = createTestData(1);
    addEventsClient.log(testData);
  }

  /**
   * ConnectException should be thrown when server url is invalid.  ConnectException makes Kafka Connect terminate the connector task.
   */
  @Test(expected = ConnectException.class)
  public void testInvalidUrl() {
    new AddEventsClient("app.scalyr.com");
  }

  private void verifyHeaders(Headers headers) {
    assertEquals(ContentType.APPLICATION_JSON.toString(), headers.get("Content-type"));
    assertEquals(ContentType.APPLICATION_JSON.toString(), headers.get("Accept"));
    assertEquals("Keep-Alive", headers.get("Connection"));
    assertEquals("Scalyr-Kafka-Connector", headers.get("User-Agent"));
  }

  private Map<String, Object> createTestData(int num) {
    Map<String, Object> data = new HashMap<>();
    data.put("a", "b");
    data.put("c", num);
    data.put("d", (double)num);
    data.put("e", true);
    return data;
  }
}
