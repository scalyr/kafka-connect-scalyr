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

import com.fasterxml.jackson.core.io.SerializedString;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
  private SerializedString message;
  private Map<String, Object> additionalAttrs;

  // Cached estimated event size
  private int estimatedSizeBytes;

  // Estimated per event serialization overhead: 58 bytes add events JSON format, 19 bytes timestamp, 16 bytes Kafka offset
  // {"ts":,"si":"","sn":,"attrs":{"message":""},"log":"999"},
  private static final int EVENT_SERIALIZATION_OVERHEAD_BYTES = 93;

  /**
   * JSON Special characters that need to be escaped.  Used to estimate escaped JSON string lengths.
   * Backspace is replaced with \b
   * Form feed is replaced with \f
   * Newline is replaced with \n
   * Carriage return is replaced with \r
   * Tab is replaced with \t
   * Double quote is replaced with \"
   * Backslash is replaced with \\
   */
  private static final Set<Character> JSON_ESCAPED_CHARS = ImmutableSet.of('\b', '\f', '\n', '\r', '\t', '"', '\\');

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
    this.message = message == null ? null : new SerializedString(message);
    return this;
  }

  /**
   * Sets/overwrites additionalAttrs Map.
   * CAUTION: The Map is not cloned for performance/memory reasons.
   * The caller should not modify the Map after calling this method.
   * @param additionalAttrs Map should not be modified after setting
   * @return this Event
   */
  public Event setAdditionalAttrs(Map<String, Object> additionalAttrs) {
    this.additionalAttrs = additionalAttrs;
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

  public String getMessage() { return message == null ? null : message.getValue(); }

  public SerializedString getSerializedMessage() {
    return message;
  }

  public Map<String, Object> getAdditionalAttrs() { return additionalAttrs; }

  public Map<String, String> getEnrichmentAttrs() { return enrichmentAttrs; }

  /**
   * @return Size in bytes of Event message and attributes.  The size is cached and should only be called once
   * all Event fields are populated and will not be changed.
   */
  public int estimatedSerializedBytes() {
    if (estimatedSizeBytes > 0) {
      return estimatedSizeBytes;
    }

    int size = Strings.isNullOrEmpty(getMessage()) ? 0 : getSerializedMessage().asQuotedUTF8().length;
    size += getTopic().length();
    size += EVENT_SERIALIZATION_OVERHEAD_BYTES;

    if (getAdditionalAttrs() != null) {
      size += getAdditionalAttrs().entrySet().stream()
        .mapToInt(entry -> entry.getKey().length() + (entry.getValue() == null ? 0 : estimateEscapedStringSize(entry.getValue().toString()))).sum();
    }
    estimatedSizeBytes = size;
    return estimatedSizeBytes;
  }

  /**
   * Estimate the escaped String length for JSON strings.
   * JSON payload requires extra escaping which needs to be taken into the size estimate.
   *
   * If it is not JSON, then use the String length as the estimate.
   * This trades off some inaccuracies where the String may contain quotable characters for faster performance.
   * @return Estimated escaped String size
   */
  private int estimateEscapedStringSize(String s) {
    if (s == null) {
      return 0;
    }
    if (s.length() < 2) {
      return s.length();
    }

    if (s.charAt(0) == '{' && s.charAt(s.length() - 1) == '}') {
      return s.length() + countJsonEscapedCharacters(s);
    }

    return s.length();
  }

  public int countJsonEscapedCharacters(String s) {
    int escapedChars = 0;
    for (int i = 0; i < s.length(); i++) {
      if (JSON_ESCAPED_CHARS.contains(s.charAt(i))) {
        escapedChars++;
      }
    }
    return escapedChars;
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
