package com.scalyr.integrations.kafka;

import com.scalyr.integrations.kafka.mapping.EventMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
    this.addEventsClient = new AddEventsClient(sinkConfig.getString(ScalyrSinkConnectorConfig.SCALYR_SERVER_CONFIG),
      sinkConfig.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value(),
      CompressorFactory.getCompressor(sinkConfig.getString(ScalyrSinkConnectorConfig.COMPRESSION_TYPE_CONFIG), sinkConfig.getInt(ScalyrSinkConnectorConfig.COMPRESSION_LEVEL_CONFIG)));
    this.eventMapper = new EventMapper();
  }

  /**
   * Sends the records to Scalyr using the addEvents API.
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

    List<Event> events = records.stream()
      .map(eventMapper::createEvent)
      .collect(Collectors.toList());

    try {
      addEventsClient.log(events);
    } catch (Exception e) {
      throw new RetriableException(e);  // Kafka will retry and Scalyr server should dedep based on the offset
    }
  }

  /**
   * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
   *
   * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}}
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Nothing to do right now.  If we go async, this should make sure all records are sent to Scalyr and throw
    // RetriableException on error so offsets will not be commited
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
}
