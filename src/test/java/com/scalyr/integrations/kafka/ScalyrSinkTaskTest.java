package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalyr.integrations.kafka.mapper.ScalyrEventMapperTest;
import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Test ScalyrSinkTask
 */
public class ScalyrSinkTaskTest {

  private ScalyrSinkTask scalyrSinkTask;

  private static final String topic = "test-topic";
  private static final int partition = 0;

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
   * End-to-End put test with SinkRecord -> ScalyrEventMapper -> AddEventsClient -> Mock Web Server addEvents API
   */
  @Test
  public void testPut() throws Exception {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200));

    Map<String, String> config = createConfig();
    config.put(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG, server.url("").toString());
    scalyrSinkTask.start(config);

    // put SinkRecords
    List<SinkRecord> schemalessRecords = ScalyrEventMapperTest.createSchemalessRecords(topic, partition, 10);
    scalyrSinkTask.put(schemalessRecords);

    // Verify sink records are sent to addEvents API
    ObjectMapper objectMapper = new ObjectMapper();
    RecordedRequest request = server.takeRequest();
    Map<String, Object> events = objectMapper.readValue(request.getBody().readUtf8(), Map.class);
    ScalyrEventMapperTest.validateEvents(schemalessRecords, new ScalyrSinkConnectorConfig(config), events);
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
    List<SinkRecord> schemalessRecords = ScalyrEventMapperTest.createSchemalessRecords(topic, partition, 10);
    scalyrSinkTask.put(schemalessRecords);
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
      ScalyrSinkConnectorConfig.SCALYR_API_CONFIG, "abc123",
      ScalyrSinkConnectorConfig.SESSION_ID_CONFIG, UUID.randomUUID().toString());
  }
}
