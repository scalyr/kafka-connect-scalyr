package com.scalyr.integrations.kafka;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffer for Events to send larger addEvents batch size.
 * Buffer estimates serialized AddEvent payload bytes taking into account per event size and server attribute sizes.
 */
public class EventBuffer {
  private final List<Event> eventBuffer = new ArrayList<>(2000);
  private final AtomicInteger estimatedSerializedBytes = new AtomicInteger();
  // Events with unique server level attributes (serverHost, logfile, parser) used for estimating server attr serialized bytes
  private final Set<Event> serverAttributes = new HashSet<>();

  // Enrichment attrs are the same for all events, so we cache this
  private int cachedEnrichmentAttrSize = 0;

  // Estimated per server attribute entry overhead: {"id":"999","attrs":{"serverHost":"","source":"","logfile":"","parser":""}
  private static final int SERVER_ATTR_SERIALIZATION_OVERHEAD_BYTES = 75;

  public void addEvent(Event event) {
    eventBuffer.add(event);
    estimatedSerializedBytes.addAndGet(event.estimatedSerializedBytes());
    updateServerAttrSize(event);
  }

  /**
   * @return Number of events
   */
  public int length() {
    return eventBuffer.size();
  }

  /**
   * @return Estimated serialized bytes of event messages and attributes
   */
  public int estimatedSerializedBytes() {
    return estimatedSerializedBytes.get();
  }

  /**
   * @return Buffered Events
   */
  public List<Event> getEvents() {
    return eventBuffer;
  }

  /**
   * Clears the event buffer
   */
  public void clear() {
    eventBuffer.clear();
    serverAttributes.clear();
    estimatedSerializedBytes.set(0);
  }

  /**
   * Update estimated msgSize with session attributes
   */
  private void updateServerAttrSize(Event event) {
    if (serverAttributes.add(event)) {
      updateServerAttrSize(event.getLogfile());
      updateServerAttrSize(event.getParser());
      updateServerAttrSize(event.getServerHost());  // serverHost as serverHost
      updateServerAttrSize(event.getServerHost());  // serverHost as source
      estimatedSerializedBytes.addAndGet(SERVER_ATTR_SERIALIZATION_OVERHEAD_BYTES);
      estimatedSerializedBytes.addAndGet(getEnrichmentAttrSize(event));
    }
  }

  private void updateServerAttrSize(String serverAttr) {
    if (!Strings.isNullOrEmpty(serverAttr)) {
      estimatedSerializedBytes.addAndGet(serverAttr.length());
    }
  }

  private int getEnrichmentAttrSize(Event event) {
    if (cachedEnrichmentAttrSize > 0) {
      return cachedEnrichmentAttrSize;
    }
    if (event.getEnrichmentAttrs() == null || event.getEnrichmentAttrs().isEmpty()) {
      return 0;
    }

    cachedEnrichmentAttrSize = event.getEnrichmentAttrs().entrySet().stream()
      .mapToInt(entry -> entry.getKey().length() + (entry.getValue() == null ? 0 : entry.getValue().length())).sum();
    return cachedEnrichmentAttrSize;
  }
}
