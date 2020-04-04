package com.scalyr.integrations.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.scalyr.api.internal.ScalyrUtil;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
  public static final String LOG_ID = "log";
  public static final String LOGS = "logs";
  public static final String ID = "id";

  public static final String FILEBEATS_EVENT_MAPPING = "{\"message\" : [\"message\"],\"logfile\": [\"log\", \"file\", \"path\"],"
    + " \"serverHost\":[\"host\", \"hostname\"], \"parser\":[\"fields\", \"parser\"]};";

  @VisibleForTesting static final List<String> LOG_LEVEL_ATTRS = ImmutableList.of("serverHost", "logfile", "parser");


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
   *   "token":   "xxx",
   *   "session": "yyy",
   *   "events":  [...],
   *   "logs":    [{"id":"1", "attrs":{"serverHost":"", "logfile":"", "parser":""}, {"id":"2", "attrs":{"serverHost":"", "logfile":"", "parser":""}}
   * }
   *
   * @param records SinkRecords to convert
   * @param config ScalyrSinkConnectorConfig
   * @return Map<String, Object> data which can be serialized to JSON for addEvents POST call
   */
  public static Map<String, Object> createEvents(Collection<SinkRecord> records, ScalyrSinkConnectorConfig config) {
    Preconditions.checkNotNull(config.getString(ScalyrSinkConnectorConfig.SESSION_ID_CONFIG));

    Map<String, Object> addEventsPayload = new HashMap<>();

    addEventsPayload.put(TOKEN, config.getPassword(ScalyrSinkConnectorConfig.SCALYR_API_CONFIG).value());
    addEventsPayload.put(SESSION, config.getString(ScalyrSinkConnectorConfig.SESSION_ID_CONFIG));

    // Convert SinkRecords to Events
    List<Map<String, Object>> events = records.stream()
      .map(r -> createEvent(r, config))
      .collect(Collectors.toList());
    addEventsPayload.put(EVENTS, events);

    // Extract log level attributes from Event attrs to logs array
    Map<List<String>, Integer> logLevelAttrs = extractLogLevelAttrs(events);
    addEventsPayload.put(LOGS, createLogsArray(logLevelAttrs));

    return addEventsPayload;
  }

  /**
   * Convert a single SinkRecord to a Scalyr Event.
   * Event format:
   * {
   *   "ts": "event timestamp (nanoseconds since 1/1/1970)",
   *   "si" set to the value of sequence_id.  This identifies which sequence the sequence number belongs to.
   *       The sequence_id is the {topic, parition}.
   *   "sn" set to the value of sequence_number.  This is used for deduplication.  This is set to the Kafka parition offset.
   *   "log" index into logs array for log lovel attributes  // Added in {@link #extractLogLevelAttrs(List)}
   *   "attrs": {...}
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
   * Attributes defined in {@link #LOG_LEVEL_ATTRS} can be extracted to the log file level and not repeated for each event.
   * Mutates the event with the following actions:
   * 1. Remove these attrs from event.attrs and add a {@link #LOG_ID} that references the log level definition of these attributes.
   * 2. Add {@link #LOG_ID} attribute which is set to the id of log level attributes in {@link #LOGS} array.
   * @param events
   * @return e.g. {["server1", "/var/log/system.log", "systemLog"] : 1, ["server2", "/var/log/system.log", "systemLog"] : 2}
   */
  @VisibleForTesting
  static Map<List<String>, Integer> extractLogLevelAttrs(List<Map<String, Object>> events) {
    Map<List<String>, Integer> logIdMap = new HashMap<>();
    AtomicInteger logIdVender = new AtomicInteger();
    events.stream().forEach(event -> {
      Map<String, Object> eventAttrs = (Map<String, Object>) event.get(ATTRS);
      List<String> logKeys = extractLogAttr(eventAttrs);
      Integer logId = logIdMap.computeIfAbsent(logKeys, k -> logIdVender.getAndIncrement());
      event.put(LOG_ID, logId.toString());
    });

    return logIdMap;
  }

  /**
   * Removes the {@link #LOG_LEVEL_ATTRS} from the {@link #ATTRS} map and return them in a List.
   * @param eventAttr event {@link #ATTRS} Map
   * @return Values for {@link #LOG_LEVEL_ATTRS} in the same order as the {@link #LOG_LEVEL_ATTRS} list.
   * If the attribute does not exist, a null entry is put in the List.
   */
  private static List<String> extractLogAttr(Map<String, Object> eventAttr) {
    if (eventAttr == null) {
      return Collections.emptyList();
    }

    return LOG_LEVEL_ATTRS.stream()
      .map(eventAttr::remove)
      .map(o -> o == null ? null : o.toString())
      .collect(Collectors.toList());
  }

  /**
   * Creates the log file entries addEvents {@link #LOGS} field.
   * @param logLevelAttrs Map of {@link #LOG_LEVEL_ATTRS} values to log id.
   * @return e.g. [{"id":"1", "attrs": {}}, {"id":"2", "attrs":{}}]
   */
  @VisibleForTesting static List<Map<String, Object>> createLogsArray(Map<List<String>, Integer> logLevelAttrs) {
    return logLevelAttrs.entrySet().stream()
      .map(e -> createLogEntry(e.getValue(), e.getKey()))
      .collect(Collectors.toList());
  }

  /**
   * Creates a {@link #LOGS} array entry
   * @param logId
   * @param logAttrValues {@link #LOG_LEVEL_ATTRS} values
   * @return e.g. {"id":"1", "attrs": {}}
   */
  private static Map<String, Object> createLogEntry(Integer logId, List<String> logAttrValues) {
    Map<String, Object> logEntry = new HashMap<>();
    logEntry.put(ID, logId.toString());
    logEntry.put(ATTRS, createLogAttr(logAttrValues));
    return logEntry;
  }

  /**
   * Create logs attrs map for the {@link #LOG_LEVEL_ATTRS} values.
   *
   * @param logAttrs log level attribute values.
   *                 Parallel List to `LOG_LEVEL_ATTRS`.  Contains the values for `LOG_LEVEL_ATTRS` in the same order.
   *                 null List entry means there is not value for the attribute.
   * @return e.g. {"serverHost":"server1", "logfile":"/var/log/system.log", "parser":"systemLog"}
   */
  private static Map<String, String> createLogAttr(List<String> logAttrs) {
    Preconditions.checkArgument(LOG_LEVEL_ATTRS.size() == logAttrs.size());
    return IntStream.range(0, LOG_LEVEL_ATTRS.size())
      .filter(i -> logAttrs.get(i) != null)
      .boxed()
      .collect(Collectors.toMap(i -> LOG_LEVEL_ATTRS.get(i), i -> logAttrs.get(i)));
  }


  /**
   * Uniquely identify a partition with {topic name, partition id}.
   */
  static String createPartitionId(String topic, Integer partition) {
    return topic + "-" + partition;
  }
}
