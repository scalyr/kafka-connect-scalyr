package com.scalyr.integrations.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.scalyr.api.internal.ScalyrUtil;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Abstraction for converting `Collection<SinkRecord>` from `SinkTask.put` into Scalyr addEvents calls
 * grouped by sessions.  Each session corresponds to a {topic, partition}.
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
  public static final String PARSER = "parser";

  /**
   * Called by the SinkTask to convert the `Collection<SinkRecord>` into Scalyr addEvents data.
   * Partitions the sink records into sessions where each session is a {topic, partition}.
   *
   * @param records `Collection<SinkRecord>` from `SinkTask.put`
   * @param config ScalyrSinkConnectorConfig
   * @return List<Map<String, Object>> where each Map in the List contains the events for a single session.
   * e.g. [session1Events, session2Events, session3Events].  A separate addEvents POST request should be made for
   * each item in the list.
   */
  public static List<Map<String, Object>> createEvents(Collection<SinkRecord> records, ScalyrSinkConnectorConfig config) {
    // Organize records by topic and partition = session
    Map<String, Map<Integer, List<SinkRecord>>> recordsBySession = records.stream()
      .collect(Collectors.groupingBy(SinkRecord::topic, Collectors.groupingBy(SinkRecord::kafkaPartition)));

    return recordsBySession.values().stream()
      .flatMap(m -> m.values().stream())  // List<SinkRecord> by partition
      .map(sessionRecords -> createEventsForSession(sessionRecords, config))
      .collect(Collectors.toList());
  }

  /**
   * Converts Collection<SinkRecord> for a single session to the Scalyr addEvents format.
   * addEvents format:
   * {
   *   "token":           "xxx",
   *   "session":         "yyy",
   *   "sessionInfo":     {...}, (optional)
   *   "events":          [...],
   * }
   *
   * @param records for a single session.  All records must belong to the same {topic, partition}
   * @param config ScalyrSinkConnectorConfig
   * @return Map<String, Object> data for a single session, which can be serialized to JSON for addEvents POST call
   */
  private static Map<String, Object> createEventsForSession(Collection<SinkRecord> records, ScalyrSinkConnectorConfig config) {
    Map<String, Object> sessionEvents = new HashMap<>();

    SinkRecord record = records.iterator().next();
    final String sessionId = createSessionId(record.topic(), record.kafkaPartition());

    sessionEvents.put(TOKEN, config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value());
    sessionEvents.put(SESSION, sessionId);

    sessionEvents.put(EVENTS,
      records.stream()
      .map(r -> createEvent(r, config))
      .peek(event -> Preconditions.checkArgument(sessionId.equals(event.get(SEQUENCE_ID)), "SinkRecords must belong to same session")) // Verify all SinkRecords belong to same session
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
   *       The sequence_id corresponds to the session = {topic, parition}.
   *   "sn" set to the value of sequence_number.  This is used for deduplication.  This is set to the Kafka offset.
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
    final String sessionId = createSessionId(record.topic(), record.kafkaPartition());

    event.put(SEQUENCE_ID, sessionId);
    event.put(SEQUENCE_NUM, record.kafkaOffset());
    event.put(TIMESTAMP, ScalyrUtil.nanoTime());
    event.put(ATTRS, getEventAttrs(record, config));

    return event;
  }

  /**
   * Creates the `attrs` map for the SinkRecord, which are the event attributes for the event.
   * This typically includes fields such as "message", "parser".
   * ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG defines the fields from the SinkRecord to include in the `attrs`.
   *
   * @param record SinkRecord to create event attrs from
   * @param config ScalyrSinkConnectorConfig
   * @return Map<String, Object> attrs
   */
  private static Map<String, Object> getEventAttrs(SinkRecord record, ScalyrSinkConnectorConfig config) {
    Map value = (Map)record.value();

    Map<String, Object> eventAttrs = config.getList(ScalyrSinkConnectorConfig.LOG_FIELDS_CONFIG).stream()
      .filter(value::containsKey)
      .collect(Collectors.toMap(Function.identity(), value::get));

    eventAttrs.put(PARSER, config.getString(ScalyrSinkConnectorConfig.PARSER_CONFIG));

    return eventAttrs;
  }

  /**
   * A session maps to {topic, partition}.
   */
  static String createSessionId(String topic, Integer partition) {
    return topic + "-" + partition;
  }
}
