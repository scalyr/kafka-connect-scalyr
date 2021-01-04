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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalyr.api.internal.ScalyrUtil;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.asynchttpclient.uri.Uri;
import org.asynchttpclient.util.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static org.asynchttpclient.Dsl.*;

/**
 * AddEventsClients provides abstraction for making Scalyr addEvents API calls.
 * It performs JSON object serialization and the addEvents POST request.
 *
 * @see <a href="https://app.scalyr.com/help/api"></a>
 */
public class AddEventsClient {

  private static final Logger log = LoggerFactory.getLogger(AddEventsClient.class);

  // Separate logger which is used for logging the whole payload when the payload exceeds the limit
  private static final Logger payloadLogger = LoggerFactory.getLogger("com.scalyr.integrations.kafka.eventpayload");

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ExecutorService retryThread = Executors.newSingleThreadExecutor();
  /** Bound http request builder is bound to the AsyncHttpClient and allows us to issue the http request without specifying the AsyncHttpClient */
  private final BoundRequestBuilder requestBuilder;
  private final String apiKey;
  private final long addEventsTimeoutMs;
  private final int initialBackoffDelayMs;
  private final Compressor compressor;
  /** Re-use outputStream to avoid reallocating and growing the ByteArrayOutputStream byte[] for every `log` call */
  private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

  /**
   * Performs sleep for specified time period is ms.  In tests, this can be a Mockable sleep.
   * @param sleepMs Time in millisecs to sleep.
   */
  private final LongConsumer sleep;

  /** Session ID per Task */
  private final String sessionId = UUID.randomUUID().toString();

  private static final String USER_AGENT = "KafkaConnector/" + VersionUtil.getVersion()
    + " JVM/" + System.getProperty("java.version");

  /**
   * Default Timeout Add Events Response
   */
  private static final AddEventsResponse TIMEOUT_ADD_EVENTS_RESPONSE = new AddEventsResponse().setStatus("Timeout").setMessage("Timeout");

  /** Add events API has max payload size of 6 MB */
  private static final int MAX_ADD_EVENTS_PAYLOAD_BYTES = 6000000;

  /**
   * Response when an add events request exceeds MAX_ADD_EVENTS_PAYLOAD_BYTES.  We skip add event requests > 6 MB
   * since it will be rejected by the server.  Success is returned to the caller so it won't be retried.
   */
  private static final AddEventsResponse PAYLOAD_TOO_LARGE = new AddEventsResponse().setStatus(AddEventsResponse.SUCCESS).setMessage("Skipped due to payload too large");

  /**
   * Limit number of add events payloads that are logged when MAX_ADD_EVENTS_PAYLOAD_BYTES is exceeded
   * TODO: Make this configurable
   */
  private static final RateLimiter payloadTooLargeLogRateLimiter = RateLimiter.create(1.0/900);  // 1 permit every 15 minutes

  /**
   * AddEventsClient which allows a mockable sleep for testing.
   * @param sleep Mockable sleep implementation.  If null, default implementation which sleeps will be used.
   * @throws IllegalArgumentException with invalid URL, which will cause Kafka Connect to terminate the ScalyrSinkTask.
   */
  public AddEventsClient(String scalyrUrl, String apiKey, long addEventsTimeoutMs, int initialBackoffDelayMs, Compressor compressor, @Nullable LongConsumer sleep) {
    this.apiKey = apiKey;
    this.addEventsTimeoutMs = addEventsTimeoutMs;
    this.initialBackoffDelayMs = initialBackoffDelayMs;
    this.compressor = compressor;
    this.sleep = sleep != null ? sleep : timeMs -> Uninterruptibles.sleepUninterruptibly(timeMs, TimeUnit.MILLISECONDS);
    this.requestBuilder = HttpWrapper.asyncHttpClient.preparePost(buildAddEventsUri(scalyrUrl));
    addHeaders();
  }

  /**
   * AddEventsClient with default sleep implementation, which performs sleep.
   * @throws IllegalArgumentException with invalid URL, which will cause Kafka Connect to terminate the ScalyrSinkTask.
   */
  public AddEventsClient(String scalyrUrl, String apiKey, long addEventsTimeoutMs, int initialBackoffDelayMs, Compressor compressor) {
    this(scalyrUrl, apiKey, addEventsTimeoutMs, initialBackoffDelayMs, compressor, null);
  }

  /**
   * Make async addEvents POST API call to Scalyr with the events object.
   *
   * Pipelining is supported through the following:
   * 1) Serialization and compression on the callers thread.
   * 2) addEvents API call is done on a SingleThreadExecutor to only allow one addEvents call at a time.
   *
   * @param events Events to send to Scalyr using addEvents API
   * @param dependentAddEvents Dependent `log` CompletableFuture which must complete before `addEvents` API call is made.
   * `dependentAddEvents` enforces ordering of Events in a session.
   * `dependentAddEvents` failures causes the current `log` call to also fail.
   */
  public CompletableFuture<AddEventsResponse> log(List<Event> events, @Nullable CompletableFuture<AddEventsResponse> dependentAddEvents) {
    log.debug("Calling addEvents with {} events", events.size());
    try {
      long startTimeMs = ScalyrUtil.currentTimeMillis();
      AddEventsRequest addEventsRequest = new AddEventsRequest()
        .setSession(sessionId)
        .setToken(apiKey)
        .setEvents(events);

      outputStream.reset();

      // NOTE: We use countingStream since we also need access to the raw serialized payload size before the compression
      CountingOutputStream countingStream = new CountingOutputStream(compressor.newStreamCompressor(outputStream));
      addEventsRequest.writeJson(countingStream);

      long uncompressedPayloadSize = countingStream.getCount();

      // Wait for dependent addEvents call to complete and return dependent failed future if failed
      if (dependentAddEvents != null && !dependentAddEvents.get(remainingMs(startTimeMs), TimeUnit.MILLISECONDS).isSuccess()) {
        log.warn("Dependent addEvents call failed");
        return dependentAddEvents;
      }

      final byte[] addEventsPayload = outputStream.toByteArray();
      CompletableFuture<AddEventsResponse> addEventsResult = new CompletableFuture<>();
      addEventsWithRetry(addEventsPayload, uncompressedPayloadSize, addEventsResult, startTimeMs, initialBackoffDelayMs);
      return addEventsResult;
    } catch (Exception e) {
      log.warn("AddEventsClient.log error", e);
      CompletableFuture<AddEventsResponse> errorFuture = new CompletableFuture<>();
      errorFuture.complete(new AddEventsResponse().setStatus("Exception").setMessage(e.toString()));
      return errorFuture;
    }
  }

  /**
   * Convenience function for {@link #log(List, CompletableFuture)} that does not have a dependent addEvents call.
   */
  public CompletableFuture<AddEventsResponse> log(List<Event> events) {
    return log(events, null);
  }

  /**
   * Call Scalyr addEvents API with {@link #addEventsTimeoutMs} using exponential backoff retries.
   * Retries are triggered async when the async add events call completes by recursively calling this method.
   * Result is returned by completing the `addEventsResult` future with the result.
   *
   * @param addEventsPayload byte[] addEvents payload (post compression if compression is enabled).
   * @param uncompressedPayloadSize long raw serialized JSON addEvents payload size before the compression.
   * @param addEventsResult CompletableFuture to return add events result in.
   * @param startTimeMs time in ms from epoch when `log` is called.  Used to enforce `addEventsTimeoutMs` deadline.
   * @param retryDelayMs time in ms to delay retry
   */
  private void addEventsWithRetry(byte[] addEventsPayload, long uncompressedPayloadSize, CompletableFuture<AddEventsResponse> addEventsResult,
      long startTimeMs, int retryDelayMs) {
    log.debug("addEvents payload uncompressed size {} bytes, compressed size {} bytes", uncompressedPayloadSize, addEventsPayload.length);

    // 6 MB add events payload exceeded.  Log the issue and skip this message.
    if (uncompressedPayloadSize > MAX_ADD_EVENTS_PAYLOAD_BYTES || addEventsPayload.length > MAX_ADD_EVENTS_PAYLOAD_BYTES) {
      // NOTE: compressed size should never really be larger than uncompressed size unless there is a pathological case
      // and we are trying to compress fully random uncompressable data
      log.error("Uncompressed add events payload size {} bytes (compressed {} bytes) exceeds maximum size ({} bytes).  Skipping this add events request.  Log data will be lost",
        uncompressedPayloadSize, addEventsPayload.length, MAX_ADD_EVENTS_PAYLOAD_BYTES);

      if (payloadTooLargeLogRateLimiter.tryAcquire()) {
        // NOTE: If compression is enabled, we need to decompress the compressed payload so we can log the raw
        // uncompressed value
        byte[] decompressedPayload = getDecompressedPayload(addEventsPayload);
        payloadLogger.error("Add events too large payload: {}", new String(decompressedPayload));
      }
      addEventsResult.complete(PAYLOAD_TOO_LARGE);
      return;
    }

    // Retries timed out
    if (remainingMs(startTimeMs) == 0) {
      addEventsResult.complete(TIMEOUT_ADD_EVENTS_RESPONSE);
      return;
    }

    requestBuilder.setBody(addEventsPayload);
    final ListenableFuture<Response> addEventsFuture = requestBuilder.execute();
    addEventsFuture.addListener(() -> {
      AddEventsResponse addEventsResponse;
      try {
        final Response response = addEventsFuture.get();
        addEventsResponse = parseAddEventsResponse(response);
        log.debug("post http code {}, httpResponse {}", response.getStatusCode(), addEventsResponse);
        if (addEventsResponse.isSuccess() || !addEventsResponse.isRetriable()) {
          addEventsResult.complete(addEventsResponse);
          return;
        }
      } catch (Exception e) {
        Throwable rootCauseException = e.getCause() == null ? e : e.getCause();
        addEventsResponse = new AddEventsResponse().setStatus("addEvents error").setMessage(rootCauseException.toString());
      }

      // error - retry if not timed out and retriable, otherwise, complete with failure
      log.warn("addEvents failed with {}", addEventsResponse);
      if (remainingMs(startTimeMs) > retryDelayMs && addEventsResponse.isRetriable()) {
        sleep.accept(retryDelayMs);
        addEventsWithRetry(addEventsPayload, uncompressedPayloadSize, addEventsResult, startTimeMs, retryDelayMs * 2);
      } else {
        addEventsResult.complete(addEventsResponse);
      }
    }, retryThread);
  }

  /**
   * Convert Response into AddEventsResponse, handling different error conditions.
   * @param response Response from the addEvents call
   * @return AddEventsResponse
   */
  private AddEventsResponse parseAddEventsResponse(Response response) {
    final int statusCode = response.getStatusCode();

    if (!response.hasResponseBody()) {
      log.warn("addEvents received empty response, server may have reset connection.");
      return new AddEventsResponse().setStatus("emptyResponse");
    }

    if (statusCode == 429) {
      log.warn("addEvents received \"too busy\" response from server.");
      return new AddEventsResponse().setStatus("serverTooBusy");
    }

    try {
      AddEventsResponse addEventsResponse = objectMapper.readValue(response.getResponseBodyAsBytes(), AddEventsResponse.class);

      // Success
      if (statusCode == HttpConstants.ResponseStatusCodes.OK_200 && addEventsResponse.isSuccess()) {
        return addEventsResponse;
      }

      if (AddEventsResponse.CLIENT_BAD_PARAM.equals(addEventsResponse.getStatus())) {
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
   * This method takes (potentially) compressed addEventsPayload and if it's compressed, it decompresses it and returns
   * a decompressed version. It ignores any exceptions which may arise when trying to decompress the payload.
   *
   * @param addEventsPayload  byte[] addEvents payload (post compression if compression is enabled).
   * @return Decompressed payload byte array.
   */
  @VisibleForTesting
  byte[] getDecompressedPayload(byte[] addEventsPayload) {
    byte[] decompressedPayload = "unable to decompress the payload".getBytes();
    try {
      try (InputStream inputStream = compressor.newStreamDecompressor(new ByteArrayInputStream(addEventsPayload))) {
        decompressedPayload = ByteStreams.toByteArray(inputStream);
      }
    } catch (Exception ex) {
      // NOTE: Failing to decompress the payload should not be fatal
    }

    return decompressedPayload;
  }

  /**
   * Validates url and creates addEvents Scalyr URL
   * @return Scalyr addEvents URI.  e.g. https://apps.scalyr.com/addEvents
   * @throws IllegalArgumentException with invalid Scalyr URL
   */
  private String buildAddEventsUri(String url) {
      Uri uri = Uri.create(url);

      // Enforce https for Scalyr connection
      Preconditions.checkArgument((uri.getScheme() != null && uri.getHost() != null)
        && ((!"localhost".equals(uri.getHost()) && Uri.HTTPS.equals(uri.getScheme())) || "localhost".equals(uri.getHost())),
        "Invalid Scalyr URL: {}", url);

      return new Uri(uri.getScheme(), null, uri.getHost(), uri.getPort(), "/addEvents", null, null).toFullUrl();
  }

  /**
   * Add addEvents POST request headers
   */
  private void addHeaders() {
    requestBuilder.addHeader("Content-type", MediaType.APPLICATION_JSON);
    requestBuilder.addHeader("Accept", MediaType.APPLICATION_JSON);
    requestBuilder.addHeader("Connection", "Keep-Alive");
    requestBuilder.addHeader("User-Agent", USER_AGENT);
    requestBuilder.addHeader("Content-Encoding", compressor.getContentEncoding());
  }

  /**
   * @param startTimeMs Start time ms of {@link #log(List, CompletableFuture)} call
   * @return Time ms remaining of the addEventsTimeoutMs deadline
   */
  private long remainingMs(long startTimeMs) {
    return Math.max(addEventsTimeoutMs - (ScalyrUtil.currentTimeMillis() - startTimeMs), 0);
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

      // __origServerHost is a stream identifier that is used to create a virtual session
      // __origServerHost event attr will be promoted to serverHost session attr on the server
      if (event.getServerHost() != null) {
        jsonGenerator.writeStringField("__origServerHost", event.getServerHost());
      }
      if (event.getLogfile() != null) {
        jsonGenerator.writeStringField("logfile", event.getLogfile());
      }
      if (event.getParser() != null) {
        jsonGenerator.writeStringField("parser", event.getParser());
      }

      if (event.getEnrichmentAttrs() != null) {
        for (Map.Entry<String, String> entry : event.getEnrichmentAttrs().entrySet()) {
          jsonGenerator.writeObjectField(entry.getKey(), entry.getValue());
        }
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
      if (event.getSerializedMessage() != null) {
        jsonGenerator.writeFieldName("message");
        byte[] quotedMsg = event.getSerializedMessage().asQuotedUTF8();
        jsonGenerator.writeRawUTF8String(quotedMsg, 0, quotedMsg.length);
      }

      // Write additional attrs
      if (event.getAdditionalAttrs() != null) {
        for (Map.Entry<String, Object> entry : event.getAdditionalAttrs().entrySet()) {
          jsonGenerator.writeObjectField(entry.getKey(), entry.getValue());
        }
      }

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
    private static final List<String> errorMsgsToIgnore = ImmutableList.of("input too long");

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

    public boolean isSuccess() {
      return SUCCESS.equals(getStatus()) || hasIgnorableError();
    }

    public boolean hasIgnorableError() {
      return !SUCCESS.equals(getStatus()) &&
        errorMsgsToIgnore.stream().anyMatch(ignoreMsg -> getMessage() != null && getMessage().contains(ignoreMsg));
    }

    /**
     * Client bad param indicates a client request error such as invalid api token and should not be retried.
     * Success also should not be retried.
     * @return true if this response should be retried
     */
    public boolean isRetriable() {
      return !CLIENT_BAD_PARAM.equals(getStatus()) && !SUCCESS.equals(getStatus());
    }

    @Override
    public String toString() {
      return "{" +
        "\"status\":\"" + status + '"' +
        ", \"message\":\"" + message + '"' +
        '}';
    }
  }

  /**
   * Wrapper class to manage AsyncHttpClient life cycle.
   * AsyncHttpClient is a global resource that matches the connector lifecycle.
   * start/stop should be called by the ScalyrSinkConnector start/stop lifecycle methods.
   */
  public static class HttpWrapper {
    private static AsyncHttpClient asyncHttpClient;

    public static void start() {
      asyncHttpClient = asyncHttpClient();
    }

    public static void stop() {
      if (asyncHttpClient != null) {
        try {
          asyncHttpClient.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
