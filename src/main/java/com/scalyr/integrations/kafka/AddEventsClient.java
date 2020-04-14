package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AddEventsClients provides abstraction for making Scalyr addEvents API calls.
 * It performs JSON object serialization and the addEvents POST request.
 *
 * @see <a href="https://app.scalyr.com/help/api"></a>
 */
public class AddEventsClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(AddEventsClient.class);

  private final CloseableHttpClient client = HttpClients.createDefault();
  private final HttpPost httpPost;
  private final ScalyrSinkConnectorConfig config;

  /** Session ID per Task */
  private final String sessionId = UUID.randomUUID().toString();

  private static final String userAgent = "KafkaConnector/" + VersionUtil.getVersion()
    + " JVM/" + System.getProperty("java.version");

  /**
   * @throws ConnectException with invalid URL, which will cause Kafka Connect to terminate the ScalyrSinkTask.
   */
  public AddEventsClient(ScalyrSinkConnectorConfig config) {
    this.config = config;
    this.httpPost = new HttpPost(buildAddEventsUri(config.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG)));
    addHeaders(this.httpPost);
  }

  /**
   * Make addEvents POST API call to Scalyr with the events object.
   */
  public void log(List<Event> events) throws Exception {
    log.debug("Calling addEvents with {} events", events.size());

    AddEventsRequest addEventsRequest = new AddEventsRequest()
      .setSession(sessionId)
      .setToken(config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value())
      .setEvents(events);

    httpPost.setEntity(new EntityTemplate(addEventsRequest::writeJson));
    try (CloseableHttpResponse response = client.execute(httpPost)) {
      log.debug("post result {}", response.getStatusLine().getStatusCode());
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new RuntimeException("addEvents failed with code " + response.getStatusLine().getStatusCode()
          + ", message " + EntityUtils.toString(response.getEntity()));
      }
    }
  }

  /**
   * Validates url and creates addEvents Scalyr URI
   * @return Scalyr addEvents URI.  e.g. https://apps.scalyr.com/addEvents
   */
  private URI buildAddEventsUri(String url) {
    try {
      URIBuilder urlBuilder = new URIBuilder(url);

      if (urlBuilder.getScheme() == null || urlBuilder.getHost() == null) {
        throw new ConnectException("Invalid Scalyr URL: " + url);
      }
      urlBuilder.setPath("addEvents");
      return  urlBuilder.build();
    } catch (URISyntaxException e) {
      throw new ConnectException(e);
    }
  }

  /**
   * Add addEvents POST request headers
   */
  private void addHeaders(HttpPost httpPost) {
    httpPost.addHeader("Content-type", ContentType.APPLICATION_JSON.toString());
    httpPost.addHeader("Accept", ContentType.APPLICATION_JSON.toString());
    httpPost.addHeader("Connection", "Keep-Alive");
    httpPost.addHeader("User-Agent", userAgent);
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * addEvents Request Data
   */
  public static class AddEventsRequest {
    private String token;
    private String session;
    private List<Event> events;

    public AddEventsRequest setToken(String token) {
      this.token = token;
      return this;
    }

    public AddEventsRequest setSession(String session) {
      this.session = session;
      return this;
    }

    public AddEventsRequest setEvents(List<Event> events) {
      this.events = events;
      return this;
    }

    /**
     * Serializes the AddEventsRequest to JSON, writing the JSON to the `outputStream`.
     */
    public void writeJson(OutputStream outputStream) throws IOException {
      try {
        // Assign log ids for the server level event fields (server, logfile, parser) permutations.
        // Same server level event values are mapped to a logs array entry so the same data is not repeated in the events.
        AtomicInteger logId = new AtomicInteger();
        Map<Event, Integer> logIdMapping = new HashMap<>();  // Event is hashed by server fields = log level fields
        events.forEach(event -> logIdMapping.putIfAbsent(event, logId.getAndIncrement()));

        // Serialize JSON using custom serializers
        final ObjectMapper objectMapper = new ObjectMapper();
        final SimpleModule simpleModule = new SimpleModule("SimpleModule", new Version(1, 0, 0, null, null, null));
        simpleModule.addSerializer(AddEventsRequest.class, new AddEventsRequestSerializer(logIdMapping));
        simpleModule.addSerializer(Event.class, new EventSerializer(logIdMapping));
        objectMapper.registerModule(simpleModule);
        objectMapper.writeValue(outputStream, this);
      } finally {
        outputStream.close();
      }
    }
  }

  /**
   * Custom JsonSerializer for {@link AddEventsRequest}
   * Produces the following addEvents JSON:
   * {
   *   "token":   "xxx",
   *   "session": "yyy",
   *   "events":  [...],
   *   "logs":    [{"id":"1", "attrs":{"serverHost":"", "logfile":"", "parser":""}, {"id":"2", "attrs":{"serverHost":"", "logfile":"", "parser":""}}
   * }
   */
  public static class AddEventsRequestSerializer extends JsonSerializer<AddEventsRequest> {
    private final Map<Event, Integer> logIdMapping;

    public AddEventsRequestSerializer(Map<Event, Integer> logIdMapping) {
      this.logIdMapping = logIdMapping;
    }

    @Override
    public void serialize(AddEventsRequest addEventsRequest, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("token", addEventsRequest.token);
      jsonGenerator.writeStringField("session", addEventsRequest.session);
      jsonGenerator.writeObjectField("events", addEventsRequest.events);
      writeLogs(jsonGenerator);
      jsonGenerator.writeEndObject();
    }

    /**
     * Write logs array:
     * "logs":    [{"id":"1", "attrs":{"serverHost":"", "logfile":"", "parser":""}},
     *             {"id":"2", "attrs":{"serverHost":"", "logfile":"", "parser":""}}
     */
    private void writeLogs(JsonGenerator jsonGenerator) throws IOException {
      jsonGenerator.writeArrayFieldStart("logs");
      for (Map.Entry<Event, Integer> entry : logIdMapping.entrySet()) {
        writeLogArrayEntry(entry, jsonGenerator);
      }
      jsonGenerator.writeEndArray();
    }

    /**
     * Write single logs array entry:
     * {"id":"1", "attrs":{"serverHost":"", "logfile":"", "parser":""}}
     */
    private void writeLogArrayEntry(Map.Entry<Event, Integer> logEntry, JsonGenerator jsonGenerator) throws IOException {
      final Event event = logEntry.getKey();
      final Integer logId = logEntry.getValue();

      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("id", logId.toString());
      jsonGenerator.writeObjectFieldStart("attrs");

      if (event.getServerHost() != null) {
        jsonGenerator.writeStringField("origin", event.getServerHost());
      }
      if (event.getLogfile() != null) {
        jsonGenerator.writeStringField("logfile", event.getLogfile());
      }
      if (event.getParser() != null) {
        jsonGenerator.writeStringField("parser", event.getParser());
      }
      jsonGenerator.writeEndObject();
      jsonGenerator.writeEndObject();
    }
  }

  /**
   * Custom JsonSerializer for {@link Event}
   * Produces the following Event JSON:
   * {
   *   "ts": "event timestamp (nanoseconds since 1/1/1970)",
   *   "si": set to the value of sequence_id.  This identifies which sequence the sequence number belongs to.
   *       The sequence_id is the {topic, partition}.
   *   "sn": set to the value of sequence_number.  This is used for deduplication.  This is set to the Kafka parition offset.
   *   "log": index into logs array for log level attributes
   *   "attrs": {"message": set to the log message}
   * }
   */
  public static class EventSerializer extends JsonSerializer<Event> {

    private final Map<Event, Integer> logIdMapping;

    public EventSerializer(Map<Event, Integer> logIdMapping) {
      this.logIdMapping = logIdMapping;
    }

    @Override
    public void serialize(Event event, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeNumberField("ts", event.getTimestamp());
      jsonGenerator.writeStringField("si", event.getTopic() + "-" + event.getPartition()); // sequence identifier
      jsonGenerator.writeNumberField("sn", event.getOffset()); // sequence number
      writeEventAttrs(event, jsonGenerator);

      Integer logId = logIdMapping.get(event);
      if (logId != null) {
        jsonGenerator.writeStringField("log", logId.toString());
      }

      jsonGenerator.writeEndObject();
    }

    /**
     * Write event attrs:
     * "attrs: {"message": "msg"}
     */
    private void writeEventAttrs(Event event, JsonGenerator jsonGenerator) throws IOException {
      jsonGenerator.writeObjectFieldStart("attrs");
      jsonGenerator.writeStringField("message", event.getMessage());
      jsonGenerator.writeEndObject();
    }
  }
}
