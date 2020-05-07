package com.scalyr.integrations.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.scalyr.integrations.kafka.mapping.EventMapper;
import com.scalyr.integrations.kafka.AddEventsClient.AddEventsResponse;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Kafka Connect Scalyr Sink Task
 * Sends SinkRecords to Scalyr using the addEvents API.
 * A Task instance handles messages from multiple {topic, partition} pairs.
 */
public class ScalyrSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(ScalyrSinkTask.class);
  private AddEventsClient addEventsClient;
  private EventMapper eventMapper;
  private int addEventsTimeoutMs;

  /** Mockable sleep implementation for testing.  Always null when not in test. */
  @Nullable private final Consumer<Long> sleep;

  private CompletableFuture<AddEventsResponse> pendingAddEvents = null;
  private volatile ConnectException lastError = null;

  /**
   * Default constructor called by Kafka Connect.
   */
  public ScalyrSinkTask() {
    sleep = null;  // Default sleep implementation used.
  }

  /**
   * Only used for testing to provide mockable sleep implementation.
   * @param sleep Mock sleep implementation
   */
  @VisibleForTesting ScalyrSinkTask (@Nullable Consumer<Long> sleep) {
    this.sleep = sleep;
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  /**
   * Start the Task.
   * This handles configuration parsing and one-time setup of the task.
   * @param configProps initial configuration
   */
  @Override
  public void start(Map<String, String> configProps) {
    ScalyrSinkConnectorConfig sinkConfig = new ScalyrSinkConnectorConfig(configProps);
    this.addEventsTimeoutMs = sinkConfig.getInt(ScalyrSinkConnectorConfig.ADD_EVENTS_TIMEOUT_MS_CONFIG);
    this.addEventsClient = new AddEventsClient(sinkConfig.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG),
      sinkConfig.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value(), addEventsTimeoutMs,
      sinkConfig.getInt(ScalyrSinkConnectorConfig.ADD_EVENTS_RETRY_DELAY_MS_CONFIG),
      CompressorFactory.getCompressor(sinkConfig.getString(ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG),
        sinkConfig.getInt(ScalyrSinkConnectorConfig.COMPRESSION_LEVEL_CONFIG)),
      sleep);
    this.eventMapper = new EventMapper(parseEnrichmentAttrs(sinkConfig.getList(ScalyrSinkConnectorConfig.EVENT_ENRICHMENT_CONFIG)));
  }

  /**
   * Sends the records to Scalyr using the addEvents API.
   * If there are any failures since the last flush,
   * throw an error to pause additional put batches until flush is called.
   *
   * If this operation fails, throw a {@link org.apache.kafka.connect.errors.RetriableException} to
   * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
   * be stopped immediately.
   *
   * @param records the set of records to send
   */
  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }

    // Don't process any more records once an error has occurred.
    // flush will clear the error and all the records wil be retried
    if (lastError != null) {
      throw lastError;
    }

    List<Event> events = records.stream()
      .map(eventMapper::createEvent)
      .collect(Collectors.toList());

    pendingAddEvents = addEventsClient.log(events, pendingAddEvents).whenComplete(this::processResponse);
  }

  /**
   * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
   * 1) Wait for addEvents requests to complete.
   * 2) If any errors occurred, then clear the errors since the previous flush.
   *    and throw an Exception so everything since the last successful offset commit will be retried.
   *
   * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}}
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Wait for all addEvents requests to complete
    waitForRequestsToComplete();

    // Clear the responses for the next flush cycle
    pendingAddEvents = null;

    // Throw the last error if any
    if (lastError != null) {
      ConnectException flushException = lastError;
      lastError = null;
      throw flushException;
    }
  }


  /**
   * Waits for all outstanding addEvent requests to
   * @throws RetriableException if any errors occur
   */
  @VisibleForTesting void waitForRequestsToComplete() {
    try {
      if (pendingAddEvents != null) {
        pendingAddEvents.get(addEventsTimeoutMs, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      // AddEventsClient returns all errors in AddEventsResponse and does not throw Exceptions
      // Any Exception here is from the CompletableFuture.get (e.g. TimeoutException) and is retriable
      lastError = new RetriableException(e);
    }
  }

  /**
   * Perform any cleanup to stop this task. This method is invoked only once outstanding calls to other
   * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
   * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
   * as closing network connections to the sink system.
   */
  @Override
  public void stop() {
    if (addEventsClient != null) {
      addEventsClient.close();
    }
  }

  /**
   * Convert AddEventsResponse errors or CompleteableFuture exceptions into Kafka Connect Exceptions
   * and set the {@link #lastError}.
   */
  private void processResponse(AddEventsResponse addEventsResponse, Throwable e) {
    if (e != null) {
      lastError = new RetriableException(e.getCause() != null ? e.getCause() : e);
      return;
    }

    if (!addEventsResponse.isSuccess()) {
      lastError = createConnectException(addEventsResponse);
    }
  }

  /**
   * Convert the AddEventResponse error to a ConnectException.
   * ConnectException is returned with client bad param such as bad api key, which is not retriable
   * RetriableException All other errors
   */
  private ConnectException createConnectException(AddEventsResponse addEventsResponse) {
    return addEventsResponse.isRetriable()
      ? new RetriableException(addEventsResponse.toString())
      : new ConnectException(addEventsResponse.toString());
  }

  /**
   * Parse eventEnrichment config.
   * Parses [key1=value1, key2=value2] into Map<String, String> of key/value pairs.
   * @param eventEnrichment {@link ScalyrSinkConnectorConfig#EVENT_ENRICHMENT_CONFIG} value
   * @return Parsed key/value pairs as Map
   */
  @VisibleForTesting Map<String, String> parseEnrichmentAttrs(List<String> eventEnrichment) {
    return eventEnrichment.stream()
      .map(pair -> pair.split("=", 2))
      .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));
  }
}
