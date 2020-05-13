package com.scalyr.integrations.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Abstraction for a Scalyr Event.
 * A SinkRecord is converted to Event.
 */
public class Event {

  // Kafka fields
  private String topic;
  private int partition;
  private long offset;

  // Server level fields = log level attributes
  // These field values may be common across many events.
  // For space efficiency when serializing events, these server level fields are extracted
  // to a higher log level attributes.  A reference to the log level attribute is introduced in the event.
  private String serverHost;
  private String logfile;
  private String parser;
  // Enrichment attrs are the same for all events and can be promoted to log level attrs
  private Map<String, String> enrichmentAttrs;

  // Event level fields
  private long timestamp;
  private String message;
  private Map<String, Object> additionalAttrs;

  // Setters
  public Event setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public Event setPartition(int partition) {
    this.partition = partition;
    return this;
  }

  public Event setOffset(long offset) {
    this.offset = offset;
    return this;
  }

  public Event setServerHost(String serverHost) {
    this.serverHost = serverHost;
    return this;
  }

  public Event setLogfile(String logfile) {
    this.logfile = logfile;
    return this;
  }

  public Event setParser(String parser) {
    this.parser = parser;
    return this;
  }

  public Event setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public Event setMessage(String message) {
    this.message = message;
    return this;
  }

  public Event addAdditionalAttr(String key, Object value) {
    if (additionalAttrs == null) {
      additionalAttrs = new HashMap<>();
    }
    this.additionalAttrs.put(key, value);
    return this;
  }

  /**
   * Enrichment attrs are additional key/value pairs that are part of the event attrs.
   * Since these are always the same for all events, we do not make a copy of the Map to avoid duplication.
   * The caller should not modify/re-use the Map.
   */
  public Event setEnrichmentAttrs(Map<String, String> enrichmentAttrs) {
    this.enrichmentAttrs = enrichmentAttrs;
    return this;
  }

  // Getters
  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  public String getServerHost() {
    return serverHost;
  }

  public String getLogfile() {
    return logfile;
  }

  public String getParser() {
    return parser;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getMessage() {
    return message;
  }

  public Map<String, Object> getAdditionalAttrs() { return additionalAttrs; }

  public Map<String, String> getEnrichmentAttrs() { return enrichmentAttrs; }

  /**
   * @return Size in bytes of Event message and attributes.
   */
  public int estimatedSerializedBytes() {
    int size = getMessage() == null ? 0 : getMessage().length();

    if (getAdditionalAttrs() != null) {
      size += getAdditionalAttrs().entrySet().stream()
        .mapToInt(entry -> entry.getKey().length() + entry.getValue().toString().length()).sum();
    }
    return size;
  }

  /**
   * Equals only uses server level fields for log id mapping
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Event event = (Event) o;
    return Objects.equals(logfile, event.logfile) &&
      Objects.equals(serverHost, event.serverHost) &&
      Objects.equals(parser, event.parser);
  }

  /**
   * Hashcode only uses server level fields for log id mapping
   */
  @Override
  public int hashCode() {
    return Objects.hash(logfile, serverHost, parser);
  }
}
