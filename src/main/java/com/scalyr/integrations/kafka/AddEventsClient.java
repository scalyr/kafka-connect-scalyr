package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();
  private final HttpPost httpPost;
  private final String apiKey;
  private final Compressor compressor;

  /** Session ID per Task */
  private final String sessionId = UUID.randomUUID().toString();

  private static final String userAgent = "KafkaConnector/" + VersionUtil.getVersion()
    + " JVM/" + System.getProperty("java.version");

  @VisibleForTesting final static int maxRetries = 3;
  final int addEventsTimeoutMs = 20_000;

  /**
   * @throws IllegalArgumentException with invalid URL, which will cause Kafka Connect to terminate the ScalyrSinkTask.
   */
  public AddEventsClient(String scalyrUrl, String apiKey, Compressor compressor) {
    this.apiKey = apiKey;
    this.compressor = compressor;
    this.httpPost = new HttpPost(buildAddEventsUri(scalyrUrl));
    addHeaders();
  }

  /**
   * Make async addEvents POST API call to Scalyr with the events object.
   */
  public CompletableFuture<AddEventsResponse> log(List<Event> events) {
    log.debug("Calling addEvents with {} events", events.size());
    try {
      AddEventsRequest addEventsRequest = new AddEventsRequest()
        .setSession(sessionId)
        .setToken(apiKey)
        .setEvents(events);

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      addEventsRequest.writeJson(compressor.newStreamCompressor(outputStream));
      return CompletableFuture.supplyAsync(() -> addEventsWithRetry(outputStream.toByteArray()), executorService);
    } catch (IOException e) {
      CompletableFuture<AddEventsResponse> errorFuture = new CompletableFuture();
      errorFuture.complete(new AddEventsResponse().setStatus("IOException").setMessage(e.toString()));
      return errorFuture;
    }
  }

  /**
   * Call Scalyr addEvents API with {@link #maxRetries} and {@link #addEventsTimeoutMs} using exponential backoff.
   * @param addEventsPayload byte[] addEvents payload
   * @return AddEventsResponse
   */
  private AddEventsResponse addEventsWithRetry(byte[] addEventsPayload) {
    log.debug("addEvents payload size {} bytes", addEventsPayload.length);
    long startTimeMs = System.currentTimeMillis();
    httpPost.setEntity(new ByteArrayEntity(addEventsPayload));
    int delayTimeMs = 1000;
    AddEventsResponse addEventsResponse = null;
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try (CloseableHttpResponse httpResponse = client.execute(httpPost)) {
        addEventsResponse = parseAddEventsResponse(httpResponse);
        log.debug("post http code {}, httpResponse {}", httpResponse.getStatusLine().getStatusCode(), addEventsResponse);
        // return if success or client bad param
        if (AddEventsResponse.SUCCESS.equals(addEventsResponse.getStatus()) ||
            AddEventsResponse.CLIENT_BAD_PARAM.equals(addEventsResponse.getStatus())) {
          return addEventsResponse;
        }
      } catch (IOException e) {
        log.warn("Error calling Scalyr addEvents API", e);
        addEventsResponse = new AddEventsResponse().setStatus("IOException").setMessage(e.toString());
      }

      if (attempt < maxRetries && (System.currentTimeMillis() - startTimeMs + delayTimeMs < addEventsTimeoutMs)) {
        Uninterruptibles.sleepUninterruptibly(delayTimeMs, TimeUnit.MILLISECONDS);
        delayTimeMs = delayTimeMs * 2;
      }
    }

    return addEventsResponse;
  }

  /**
   * Convert httpResponse into AddEventsResponse, handling different error conditions.
   * @param httpResponse HTTP response from the addEvents call
   * @return AddEventsResponse
   */
  private AddEventsResponse parseAddEventsResponse(CloseableHttpResponse httpResponse) {
    final int statusCode = httpResponse.getStatusLine().getStatusCode();
    final long responseLength = httpResponse.getEntity().getContentLength();

    if (responseLength == 0) {
      log.warn("addEvents received empty response, server may have reset connection.");
      return new AddEventsResponse().setStatus("emptyResponse");
    }

    if (statusCode == 429) {
      log.warn("addEvents received \"too busy\" response from server.");
      return new AddEventsResponse().setStatus("serverTooBusy");
    }

    try {
      AddEventsResponse addEventsResponse = objectMapper.readValue(httpResponse.getEntity().getContent(), AddEventsResponse.class);

      // Success
      if (statusCode == HttpStatus.SC_OK && AddEventsResponse.SUCCESS.equals(addEventsResponse.getStatus())) {
        return addEventsResponse;
      }

      if (AddEventsResponse.CLIENT_BAD_PARAM.equals(addEventsResponse.status)) {
        log.error("addEvents failed due to a bad parameter value.  This may be caused by an invalid write logs api key in the configuration");
        return addEventsResponse;
      }

      log.warn("addEvents failed with {}", addEventsResponse);
      return addEventsResponse;
    } catch (IOException e) {
      log.error("Could not parse addEvents response", e);
      return new AddEventsResponse().setStatus("parseResponseFailed");
    }
  }

  /**
   * Validates url and creates addEvents Scalyr URL
   * @return Scalyr addEvents URI.  e.g. https://apps.scalyr.com/addEvents
   * @throws IllegalArgumentException with invalid Scalyr URL
   */
  private URI buildAddEventsUri(String url) {
    try {
      URIBuilder urlBuilder = new URIBuilder(url);

      // Enforce https for Scalyr connection
      Preconditions.checkArgument((urlBuilder.getScheme() != null && urlBuilder.getHost() != null)
        && ((!"localhost".equals(urlBuilder.getHost()) && "https".equals(urlBuilder.getScheme())) || "localhost".equals(urlBuilder.getHost())),
        "Invalid Scalyr URL: {}", url);

      urlBuilder.setPath("addEvents");
      return  urlBuilder.build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Add addEvents POST request headers
   */
  private void addHeaders() {
    httpPost.addHeader("Content-type", ContentType.APPLICATION_JSON.toString());
    httpPost.addHeader("Accept", ContentType.APPLICATION_JSON.toString());
    httpPost.addHeader("Connection", "Keep-Alive");
    httpPost.addHeader("User-Agent", userAgent);
    httpPost.addHeader("Content-Encoding", compressor.getContentEncoding());
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
   *   "logs":    [{"id":"1", "attrs":{"serverHost":"", "logfile":"", "parser":""}, {"id":"2", "attrs":{"serverHost":"", "logfile":"", "parser":""}}]
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
     *             {"id":"2", "attrs":{"serverHost":"", "logfile":"", "parser":""}}]
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
        jsonGenerator.writeStringField("source", event.getServerHost());
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

  /**
   * AddEvents API Response object
   */
  @JsonIgnoreProperties(ignoreUnknown = true)  // ignore bytesCharged
  public static class AddEventsResponse {
    public static final String SUCCESS = "success";
    public static final String CLIENT_BAD_PARAM = "error/client/badParam";

    private String status;
    private String message;

    public String getStatus() {
      return status;
    }

    public AddEventsResponse setStatus(String status) {
      this.status = status;
      return this;
    }

    public String getMessage() {
      return message;
    }

    public AddEventsResponse setMessage(String message) {
      this.message = message;
      return this;
    }

    @Override
    public String toString() {
      return "{" +
        "\"status\":\"" + status + '"' +
        ", \"message\":\"" + message + '"' +
        '}';
    }
  }
}
