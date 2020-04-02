package com.scalyr.integrations.kafka.mapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.integrations.kafka.ScalyrSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstraction for converting `Collection<SinkRecord>` from `SinkTask.put` into Scalyr addEvents calls.
 *
 * Map<String, Object> is used as the data structure to store the addEvents POST call payload for each session.
 * The Map can be serialized into JSON for the POST call.
 *
 * @see <a href="https://app.scalyr.com/help/api"></a>
 */
public class ScalyrEventMapper {

  public static final String TOKEN  = "token";
  public static final String SESSION  = "session";
  public static final String TIMESTAMP = "ts";
  public static final String SEQUENCE_ID = "si";
  public static final String SEQUENCE_NUM = "sn";
  public static final String ATTRS = "attrs";
  public static final String EVENTS = "events";

  public static final String FILEBEATS_EVENT_MAPPING = "{\"message\" : [\"message\"],\"logfile\": [\"log\", \"file\", \"path\"],"
    + " \"serverHost\":[\"host\", \"hostname\"], \"parser\":[\"fields\", \"parser\"]};";


  /**
   * Cache of SchemalessEventAttrConverter keyed off the event mapping json.
   * TODO: Need to also support Schema based converter so keying off the json may not be sufficient
   */
  private static final Map<String, EventAttrConverter> eventConverterMap = new HashMap<>();

  /**
   * Return the SchemalessEventAttrConverter using fields in the record and config to determine which converter to use.
   * Cache the converter for future use.
   * TODO: This is currently hard coded to Filebeats.  Need to add support for other types based on the SinkRecord fields and config params.
   * If a config event mapping is defined, we will always use that one.  Otherwise, we will examine SinkRecord fields to determine which mapping to use.
   */
  private static EventAttrConverter getEventAttrConverter(SinkRecord record, ScalyrSinkConnectorConfig config) {
    return eventConverterMap.computeIfAbsent(FILEBEATS_EVENT_MAPPING, SchemalessEventAttrConverter::new);
  }

  /**
   * Called by SinkTask.put to convert the `Collection<SinkRecord>` into Scalyr addEvents data.
   * addEvents format:
   * {
   *   "token":           "xxx",
   *   "session":         "yyy",
   *   "events":          [...],
   * }
   *
   * @param records SinkRecords to convert
   * @param config ScalyrSinkConnectorConfig
   * @return Map<String, Object> data which can be serialized to JSON for addEvents POST call
   */
  public static Map<String, Object> createEvents(Collection<SinkRecord> records, ScalyrSinkConnectorConfig config) {
    Preconditions.checkNotNull(config.getString(ScalyrSinkConnectorConfig.SESSION_ID_CONFIG));

    Map<String, Object> sessionEvents = new HashMap<>();

    sessionEvents.put(TOKEN, config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value());
    sessionEvents.put(SESSION, config.getString(ScalyrSinkConnectorConfig.SESSION_ID_CONFIG));

    sessionEvents.put(EVENTS,
      records.stream()
      .map(r -> createEvent(r, config))
      .collect(Collectors.toList())
    );

    return sessionEvents;
  }

  /**
   * Convert a single SinkRecord to a Scalyr Event.
   * Event format:
   * {
   *   "ts": "event timestamp (nanoseconds since 1/1/1970)",
   *   "si" set to the value of sequence_id.  This identifies which sequence the sequence number belongs to.
   *       The sequence_id is the {topic, parition}.
   *   "sn" set to the value of sequence_number.  This is used for deduplication.  This is set to the Kafka parition offset.
   *   "attrs": {
   *       "parser": parser to use for parsing this event
   *       ... additional log event attributes
   *    }
   * }
   * @param record SinkRecord to convert
   * @param config ScalyrSinkConnectorConfig
   * @return Map<String, Object> representation of Scalyr event JSON
   */
  @VisibleForTesting
  static Map<String, Object> createEvent(SinkRecord record, ScalyrSinkConnectorConfig config) {
    Map<String, Object> event = new HashMap<>();

    event.put(SEQUENCE_ID, createPartitionId(record.topic(), record.kafkaPartition()));
    event.put(SEQUENCE_NUM, record.kafkaOffset());
    event.put(TIMESTAMP, ScalyrUtil.nanoTime());
    event.put(ATTRS, getEventAttrConverter(record, config).convert(record));

    return event;
  }

  /**
   * Uniquely identify a partition with {topic name, partition id}.
   */
  static String createPartitionId(String topic, Integer partition) {
    return topic + "-" + partition;
  }
}
