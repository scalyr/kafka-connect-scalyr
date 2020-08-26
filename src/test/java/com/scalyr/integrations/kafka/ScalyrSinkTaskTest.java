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
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.mapping.CustomAppEventMapping;
import com.scalyr.integrations.kafka.mapping.EventMapper;
import com.scalyr.integrations.kafka.TestUtils.TriFunction;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.scalyr.integrations.kafka.AddEventsClientTest.EVENTS;
import static com.scalyr.integrations.kafka.TestUtils.fails;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
  private static final int ADD_EVENTS_OVERHEAD_BYTES = (int)(TestValues.MIN_BATCH_SEND_SIZE_BYTES * 0.2); // 20% overhead

  /**
   * Create test parameters for each SinkRecordValueCreator type.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> testParams() {
    return TestUtils.multipleRecordValuesTestParams();
  }

  public ScalyrSinkTaskTest(TriFunction<Integer, Integer, Integer, Object> recordValue) {
    this.recordValue = recordValue;
    // Print test params
    Object data = recordValue.apply(1, 1, 1);
    System.out.println("Executing test with " + (data instanceof Struct ? "schema" : "schemaless") + " recordValue: " + data);

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
    // With event enrichment
    Map<String, String> config = createConfig();
    scalyrSinkTask.start(config);

    // No event enrichment
    config.remove(ScalyrSinkConnectorConfig.EVENT_ENRICHMENT_CONFIG);
    scalyrSinkTask.start(config);
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

  /**
   * Create test SinkRecords and verify sent via addEvents API to the MockWebServer
   */
  private void putAndVerifyRecords(MockWebServer server) throws InterruptedException, java.io.IOException {
    // Add multiple server responses for `put` batch exceeds `batch_send_size_bytes`
    IntStream.range(0, 2).forEach(i ->
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS)));

    // put SinkRecords
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, TestValues.MIN_BATCH_EVENTS, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
    scalyrSinkTask.flush(Collections.emptyMap());
    scalyrSinkTask.waitForRequestsToComplete();

    verifyRecords(server, records);
  }

  /**
   * Verify sink records are sent to addEvents API
   * @param server MockWebServer addEvents request is sent to
   * @param records SinkRecords sent
   */
  private void verifyRecords(MockWebServer server, List<SinkRecord> records) throws InterruptedException, java.io.IOException {
    assertTrue(server.getRequestCount() > 0);

    EventMapper eventMapper = new EventMapper(
      scalyrSinkTask.parseEnrichmentAttrs(new ScalyrSinkConnectorConfig(createConfig()).getList(ScalyrSinkConnectorConfig.EVENT_ENRICHMENT_CONFIG)),
      CustomAppEventMapping.parseCustomAppEventMappingConfig(TestValues.CUSTOM_APP_EVENT_MAPPING_JSON));

    List<Event> origEvents = records.stream()
      .map(eventMapper::createEvent)
      .collect(Collectors.toList());
    ObjectMapper objectMapper = new ObjectMapper();

    // There can be multiple addEvents server requests for the `records`
    int eventStart = 0;
    for (int i = 0; i < server.getRequestCount(); i++) {
      RecordedRequest request = server.takeRequest();
      assertTrue(request.getBody().size() - ADD_EVENTS_OVERHEAD_BYTES < TestValues.MIN_BATCH_SEND_SIZE_BYTES);
      Map<String, Object> addEventsPayload = objectMapper.readValue(request.getBody().inputStream(), Map.class);
      List<Map<String, Object>> events = ((List)addEventsPayload.get(EVENTS));
      assertNotNull(events);
      AddEventsClientTest.validateEvents(origEvents.subList(eventStart, eventStart + events.size()), addEventsPayload);
      eventStart += events.size();
    }
  }

  /**
   * Verify multiple cycles of puts followed by flush work as expected.
   */
  @Test
  public void testPutFlushCycles() throws Exception {
    final int numCycles = 2;  // cycle = multiple puts followed by a flush
    final int numPuts = 10;

    for (int cycle = 0; cycle < numCycles; cycle++) {
      for (int put = 0; put < numPuts; put++) {
        MockWebServer server = new MockWebServer();
        startTask(server);
        putAndVerifyRecords(server);
      }
      scalyrSinkTask.flush(new HashMap<>());
    }
  }

  /**
   * Tests the following error logic:
   * 1. put returns errors for all requests after an error has occurred
   * 2. flush clears the errors
   * 3. Subsequent puts succeed
   *
   * Uses `ScalyrUtil` mockable clock to mock time for retry attempts.
   * Uses `CustomActionDispatcher` to advance mockable clock when a request occurs.
   */
  @Test
  public void testPutErrorHandling() {
    final int numRequests = 3;
    int requestCount = 0;
    TestUtils.MockSleep mockSleep = new TestUtils.MockSleep();
    this.scalyrSinkTask = new ScalyrSinkTask(mockSleep.sleep);
    MockWebServer server = new MockWebServer();

    startTask(server);

    // Error first request
    TestUtils.addMockResponseWithRetries(server, new MockResponse().setResponseCode(429).setBody(TestValues.ADD_EVENTS_RESPONSE_SERVER_BUSY));
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, TestValues.MIN_BATCH_EVENTS, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(requestCount += TestValues.EXPECTED_NUM_RETRIES, server.getRequestCount());
    assertEquals(TestValues.EXPECTED_SLEEP_TIME_MS, mockSleep.sleepTime.get());

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
   * Verify that input too long error is ignored
   */
  @Test
  public void testIgnoreInputTooLongError() throws Exception {
    TestUtils.MockSleep mockSleep = new TestUtils.MockSleep();
    this.scalyrSinkTask = new ScalyrSinkTask(mockSleep.sleep);
    MockWebServer server = new MockWebServer();

    startTask(server);

    // Input too long error first request
    IntStream.range(0, 2).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_INPUT_TOO_LONG)));
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, TestValues.MIN_BATCH_EVENTS, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
    scalyrSinkTask.flush(Collections.emptyMap());
    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(2, server.getRequestCount());
    assertEquals(0, mockSleep.sleepTime.get());

    // Subsequent requests should succeed
    IntStream.range(0, 2).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS)));
    scalyrSinkTask.put(records);
    scalyrSinkTask.flush(Collections.emptyMap());
    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(4, server.getRequestCount());
  }

  /**
   * Verify event buffering batchSendSize.
   * 1) Verify addEvents is not called when batchSendSize is not met.
   * 2) Verify addEvents is called once batchSendSize is met without batchSendWaitMs met.
   */
  @Test
  public void testPutEventBufferingSendSize() throws Exception {
    final int numRecords = (TestValues.MIN_BATCH_EVENTS / 2) + 1;
    ScalyrUtil.setCustomTimeNs(0);  // Set custom time and never advance so batchSendWaitMs will not be met


    // Test multiple rounds of batch/send
    for (int i = 0; i < 2; i++) {
      MockWebServer server = new MockWebServer();
      startTask(server);

      server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS));

      // batch 1 - buffered
      List<SinkRecord> records = TestUtils.createRecords(topic, partition, numRecords, recordValue.apply(numServers, numLogFiles, numParsers));
      List<SinkRecord> allRecords = new ArrayList<>(records);
      scalyrSinkTask.put(records);
      scalyrSinkTask.waitForRequestsToComplete();
      assertEquals(0, server.getRequestCount());

      // batch 2 - batch 1 & 2 sent together
      records = TestUtils.createRecords(topic, partition, numRecords, recordValue.apply(numServers, numLogFiles, numParsers));
      scalyrSinkTask.put(records);
      allRecords.addAll(records);
      scalyrSinkTask.waitForRequestsToComplete();
      assertEquals(1, server.getRequestCount());

      // Send any remaining records
      server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS));
      scalyrSinkTask.flush(Collections.emptyMap());

      verifyRecords(server, allRecords);
    }
  }

  /**
   * Verify event buffering batchSendWaitMs.
   * 1) Verify addEvents is not called when batchSendWaitMs and batchSendSize is not met.
   * 2) Verify addEvents is called once batchSendWaitMs is met without batchSendSize met.
   */
  @Test
  public void testPutEventBufferingBatchSendWait() throws Exception {
    final int numRecords = 10;
    ScalyrUtil.setCustomTimeNs(0);

    // Test multiple rounds of batch/send
    for (int i = 0; i < 2; i++) {
      MockWebServer server = new MockWebServer();
      startTask(server);

      server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS));

      // batch 1 - buffered
      List<SinkRecord> records = TestUtils.createRecords(topic, partition, numRecords, recordValue.apply(numServers, numLogFiles, numParsers));
      List<SinkRecord> allRecords = new ArrayList<>(records);
      scalyrSinkTask.put(records);
      scalyrSinkTask.waitForRequestsToComplete();
      assertEquals(0, server.getRequestCount());

      // batch 2 - buffered
      records = TestUtils.createRecords(topic, partition, numRecords, recordValue.apply(numServers, numLogFiles, numParsers));
      scalyrSinkTask.put(records);
      allRecords.addAll(records);
      scalyrSinkTask.waitForRequestsToComplete();
      assertEquals(0, server.getRequestCount());

      // batch 3 - batchSendWaitMs exceeded
      ScalyrUtil.advanceCustomTimeMs(ScalyrSinkConnectorConfig.DEFAULT_BATCH_SEND_WAIT_MS + 1000);
      records = TestUtils.createRecords(topic, partition, numRecords, recordValue.apply(numServers, numLogFiles, numParsers));
      scalyrSinkTask.put(records);
      allRecords.addAll(records);
      scalyrSinkTask.waitForRequestsToComplete();
      assertEquals(1, server.getRequestCount());

      verifyRecords(server, allRecords);
    }
  }

  /**
   * Verify flush sends events in buffer
   */
  @Test
  public void testFlushSendsEventsInBuffer() throws Exception {
    final int numRecords = 100;

    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS));

    startTask(server);

    // batch 1 - buffered
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, numRecords, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(0, server.getRequestCount());

    // flush sends events in buffer
    scalyrSinkTask.flush(new HashMap<>());
    assertEquals(1, server.getRequestCount());

    verifyRecords(server, records);
  }

  /**
   * A single `put` call may contain enough record messages to meet `batch_send_size_bytes`.
   * In this case, the single `put` call will result in multiple add events calls.
   * Verify multiple add events calls are performed when a single put call exceeds `batch_send_size_bytes`.
   */
  @Test
  public void testSinglePutExceedsBatchBytesSize() throws Exception {
    final int numExpectedSends = 4;

    MockWebServer server = new MockWebServer();
    // Record value sizes may vary from the test data params.
    // Larger record sizes will result in additional sends, so we enqueue extra server responses
    IntStream.range(0, numExpectedSends * 2).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS)));

    startTask(server);

    // single put batch with multiple add events server calls
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, TestValues.MIN_BATCH_EVENTS * numExpectedSends, recordValue.apply(numServers, numLogFiles, numParsers));
    scalyrSinkTask.put(records);
    scalyrSinkTask.waitForRequestsToComplete();
    assertTrue(server.getRequestCount() >= numExpectedSends);

    // send any remaining records
    scalyrSinkTask.flush(new HashMap<>());
    assertTrue(server.getRequestCount() >= numExpectedSends);

    verifyRecords(server, records);
  }

  /**
   * Verify large message interspersed with small messages does not cause event buffer
   * size to be exceeded.
   */
  @Test
  public void testLargeMsgMixedWithSmallMsgs() throws Exception {
    final int numExpectedSends = 3;

    MockWebServer server = new MockWebServer();
    // Record value sizes may vary from the test data params.
    // Larger record sizes will result in additional sends, so we enqueue extra server responses
    IntStream.range(0, numExpectedSends).forEach(i -> server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS)));

    startTask(server);

    // Small records
    List<SinkRecord> records = TestUtils.createRecords(topic, partition, TestValues.MIN_BATCH_EVENTS / 2, recordValue.apply(numServers, numLogFiles, numParsers));
    // Large record
    SinkRecord largeRecord = TestUtils.createRecords(topic, partition, 1, recordValue.apply(numServers, numLogFiles, numParsers)).get(0);
    records.add(new SinkRecord(largeRecord.topic(), largeRecord.kafkaPartition(), null, null, null, createLargeRecordValue(largeRecord.value()), largeRecord.kafkaOffset(), largeRecord.timestamp(), TimestampType.CREATE_TIME));
    // Small records
    records.addAll(TestUtils.createRecords(topic, partition, TestValues.MIN_BATCH_EVENTS / 2, recordValue.apply(numServers, numLogFiles, numParsers)));

    scalyrSinkTask.put(records);
    scalyrSinkTask.waitForRequestsToComplete();
    assertEquals(2, server.getRequestCount());  // small msgs + 1 large msg

    // send any remaining records
    scalyrSinkTask.flush(new HashMap<>());
    assertTrue(server.getRequestCount() >= numExpectedSends);

    verifyRecords(server, records);
  }

  /**
   * Mutates SinkRecord value to have a large message
   * @param value Map/Struct SinkRecord value
   * @return Mutated value with large message
   */
  private Object createLargeRecordValue(Object value) {
    byte[] largeMsgBytes = new byte[TestValues.MIN_BATCH_SEND_SIZE_BYTES];
    Arrays.fill(largeMsgBytes, (byte)'a');
    String largeMsg = new String(largeMsgBytes);

    if (value instanceof Map) {
      Map<String, Object> mapValue = (Map)value;
      mapValue.put("message", largeMsg);
    } else if (value instanceof Struct) {
      Struct structValue = (Struct)value;
      structValue.put("message", largeMsg);
    } else {
      throw new IllegalArgumentException("Unsupported record value type " + value.getClass().getName());
    }
    return value;
  }

  /**
   * Verify sink records that do not match an event mapper work ok.
   */
  @Test
  public void testSinkRecordsNotMatchingEventMapper() {
    final int numRecords = 100;

    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200).setBody(TestValues.ADD_EVENTS_RESPONSE_SUCCESS));

    startTask(server);

    List<SinkRecord> records = TestUtils.createRecords(topic, partition, numRecords, Collections.EMPTY_MAP);
    scalyrSinkTask.put(records);
    scalyrSinkTask.flush(new HashMap<>());
    assertEquals(0, server.getRequestCount());  // Doesn't send anything b/c records did not match an event mapper
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
   * Test multiple rounds of flush with no records
   */
  @Test
  public void testFlush() {
    Map<String, String> config = createConfig();
    scalyrSinkTask.start(config);

    scalyrSinkTask.flush(new HashMap<>());
    scalyrSinkTask.flush(new HashMap<>());
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

  /**
   * Verify version() returns correct value
   */
  @Test
  public void testVersion() {
    assertEquals(VersionUtil.getVersion(), scalyrSinkTask.version());
  }

  /**
   * Start task using MockWebServer URL for the Scalyr server
   */
  private void startTask(MockWebServer server) {
    Map<String, String> configMap = createConfig();
    configMap.put(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, server.url("").toString());
    configMap.put(ScalyrSinkConnectorConfig.BATCH_SEND_SIZE_BYTES_CONFIG, String.valueOf(TestValues.MIN_BATCH_SEND_SIZE_BYTES));
    scalyrSinkTask.start(configMap);
  }

  private Map<String, String> createConfig() {
    return TestUtils.makeMap(
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, TestValues.API_KEY_VALUE,
      ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG, CompressorFactory.NONE,
      ScalyrSinkConnectorConfig.EVENT_ENRICHMENT_CONFIG, TestValues.ENRICHMENT_VALUE,
      ScalyrSinkConnectorConfig.CUSTOM_APP_EVENT_MAPPING_CONFIG, TestValues.CUSTOM_APP_EVENT_MAPPING_JSON);
  }
}
