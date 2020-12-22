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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.mapping.CustomAppEventMapping;
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
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
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
  @Nullable private final LongConsumer sleep;

  private CompletableFuture<AddEventsResponse> pendingAddEvents;
  private volatile ConnectException lastError;
  private final EventBuffer eventBuffer = new EventBuffer();

  /**
   * Events are queued until event estimated serialized bytes reaches batchSendSizeBytes
   * or until batchSendWaitMs is reached.
   */
  private int batchSendSizeBytes;

  /**
   * Ensures event batch is not queued for too long.
   * Batch is sent once batchSendWaitMs is reached even though batchSendSizeBytes is not reached yet.
   */
  private int batchSendWaitMs;

  private long lastBatchSendTimeMs;

  private static final RateLimiter noRecordLogRateLimiter = RateLimiter.create(1.0/30);  // 1 permit every 30 seconds to not log

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
  @VisibleForTesting ScalyrSinkTask (@Nullable LongConsumer sleep) {
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
    this.batchSendSizeBytes = sinkConfig.getInt(ScalyrSinkConnectorConfig.BATCH_SEND_SIZE_BYTES_CONFIG);
    this.batchSendWaitMs = sinkConfig.getInt(ScalyrSinkConnectorConfig.BATCH_SEND_WAIT_MS_CONFIG);
    this.lastBatchSendTimeMs = ScalyrUtil.currentTimeMillis();
    this.addEventsClient = new AddEventsClient(sinkConfig.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG),
      sinkConfig.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value(), addEventsTimeoutMs,
      sinkConfig.getInt(ScalyrSinkConnectorConfig.ADD_EVENTS_RETRY_DELAY_MS_CONFIG),
      CompressorFactory.getCompressor(sinkConfig.getString(ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG),
        sinkConfig.getInt(ScalyrSinkConnectorConfig.COMPRESSION_LEVEL_CONFIG)),
      sleep);

    this.eventMapper = new EventMapper(
      parseEnrichmentAttrs(sinkConfig.getList(ScalyrSinkConnectorConfig.EVENT_ENRICHMENT_CONFIG)),
      parseCustomAppEventMapping(sinkConfig.getString(ScalyrSinkConnectorConfig.CUSTOM_APP_EVENT_MAPPING_CONFIG)),
      sinkConfig.getBoolean(ScalyrSinkConnectorConfig.SEND_ENTIRE_RECORD));

    log.info("Started ScalyrSinkTask with config {}", configProps);
  }

  /**
   * Sends the records to Scalyr using the addEvents API.
   * Buffers events until `batchSendSizeBytes` is met.
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
    log.debug("Putting {} records to Scalyr", records.size());
    if (records.isEmpty()) {
      return;
    }

    // Don't process any more records once an error has occurred.
    // flush will clear the error and all the records wil be retried
    if (lastError != null) {
      throw lastError;
    }

    final long recordCount = records.stream()
      .map(eventMapper::createEvent)
      .filter(Objects::nonNull)
      .peek(event -> {
        if (eventBuffer.estimatedSerializedBytes() + event.estimatedSerializedBytes() >= batchSendSizeBytes) {
          sendEvents();
        }
        eventBuffer.addEvent(event);
      }).count();

    if (recordCount == 0) {
      if (noRecordLogRateLimiter.tryAcquire()) {
        log.warn("No records matched an event mapper.  Records not sent to Scalyr.  Check the custom_app_event_mapping matcher configuration.");
      }
      return;
    }

    // Send events when batchSendWaitMs exceeded
    if (ScalyrUtil.currentTimeMillis() - lastBatchSendTimeMs >= batchSendWaitMs) {
      sendEvents();
    }
  }

  /**
   * Call addEvents with EventBuffer
   */
  private void sendEvents() {
    if (eventBuffer.length() == 0) {
      return;
    }
    PerfStats perfStats = new PerfStats(log);
    perfStats.recordEvents(eventBuffer);
    pendingAddEvents = addEventsClient.log(eventBuffer.getEvents(), pendingAddEvents).whenComplete(this::processResponse);
    pendingAddEvents.thenRun(perfStats::log);
    eventBuffer.clear();
    lastBatchSendTimeMs = ScalyrUtil.currentTimeMillis();
  }

  /**
   * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
   * 1) Send events in the event buffer
   * 2) Wait for addEvents requests to complete.
   * 3) If any errors occurred, then clear the errors since the previous flush.
   *    and throw an Exception so everything since the last successful offset commit will be retried.
   *
   * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}}
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    log.debug("Flushing data to Scalyr with the following offsets: {}", currentOffsets);

    // Send pending events in buffer
    if (eventBuffer.length() > 0) {
      sendEvents();
    }

    // Wait for in-flight addEvents requests to complete
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
    log.info("Stopped ScalyrSinkTask");
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

    if (addEventsResponse.hasIgnorableError()) {
      log.warn("AddEventResponse with ignorable error {}", addEventsResponse);
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
    if (eventEnrichment == null) {
      return Collections.emptyMap();
    }

    return eventEnrichment.stream()
      .map(pair -> pair.split("=", 2))
      .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));
  }

  /**
   * Parse custom app event mapping config to check for syntax issues
   * @param customAppEventMappingJson {@link ScalyrSinkConnectorConfig#CUSTOM_APP_EVENT_MAPPING_CONFIG}
   * @return List<CustomAppEventMapping> of parsed custom app mappings
   * @throws RuntimeException if parsing error occurs.  Should not occur because config has already validated the custom event mapping JSON.
   */
  private List<CustomAppEventMapping> parseCustomAppEventMapping(String customAppEventMappingJson) {
    if (customAppEventMappingJson == null) {
      return Collections.emptyList();
    }

    try {
      return CustomAppEventMapping.parseCustomAppEventMappingConfig(customAppEventMappingJson);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Captures performance stats when debug log is enabled.
   */
  private static class PerfStats {
    private final Logger log;
    private final long startTime = System.currentTimeMillis();
    private int numRecords;
    private int estimatedBytes;

    public PerfStats(Logger log) {
      this.log = log;
    }

    public void recordEvents(EventBuffer eventBuffer) {
      if (log.isDebugEnabled()) {
        numRecords = eventBuffer.length();
        estimatedBytes = eventBuffer.estimatedSerializedBytes();
      }
    }

    /**
     * log should be called when the addEvents call completes to log the performance stats.
     */
    public void log() {
      if (log.isDebugEnabled()) {
        long timeMs = System.currentTimeMillis() - startTime;
        log.debug("Processed numRecords {}, estimated serialized bytes {} in {} millisecs, {} MB/sec",
          numRecords, estimatedBytes, timeMs, (timeMs == 0 ? 0 : (estimatedBytes / (double)(timeMs) / 1000)));
      }
    }
  }
}
