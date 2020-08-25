package com.scalyr.integrations.kafka;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffer for Events to send larger addEvents batch size.
 * Includes estimation of serialized AddEvent payload bytes.
 */
public class EventBuffer {
  private final List<Event> eventBuffer = new ArrayList<>(2000);
  private final AtomicInteger estimatedSerializedBytes = new AtomicInteger();
  // Unique session attributes used for estimating serialized bytes
  private final Set<String> sessionAttributes = new HashSet<>();

  public void addEvent(Event event) {
    eventBuffer.add(event);
    estimatedSerializedBytes.addAndGet(event.estimatedSerializedBytes());
    updateSessionAttrSize(event);
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
    sessionAttributes.clear();
    estimatedSerializedBytes.set(0);
  }

  /**
   * Update estimated msgSize with session attributes
   */
  private void updateSessionAttrSize(Event event) {
    updateSessionAttrSize(event.getLogfile());
    updateSessionAttrSize(event.getParser());
    updateSessionAttrSize(event.getServerHost());
  }

  private void updateSessionAttrSize(String sessionAttr) {
    if (sessionAttributes.add(sessionAttr)) {
      estimatedSerializedBytes.addAndGet(sessionAttr.length());
    }
  }
}
