package com.scalyr.integrations.kafka;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Verify EventBuffer add events payload estimates
 */
public class EventBufferTest {
  // Acceptable margin of error for estimated vs actual add events payload size
  private static final double deltaPercent = 0.05;

  /**
   * Test add events payload estimate with small event messages.
   */
  @Test
  public void testSmallEventMsg() throws Exception {
    final int numEvents = 30000;
    final String smallMsg = "This is a small sixty-four byte message.  This is a small sixty-";
    EventBuffer eventBuffer = new EventBuffer();
    TestUtils.createTestEvents(numEvents, smallMsg, 100, 1, 1)
      .forEach(eventBuffer::addEvent);

    final int estimatedSerializedBytes = eventBuffer.estimatedSerializedBytes();
    final int actualSerializedBytes = actualSerializedSize(eventBuffer.getEvents());
    assertEquals(actualSerializedBytes, estimatedSerializedBytes, actualSerializedBytes * deltaPercent);
  }

  /**
   * Test add events payload estimate with many server attributes
   * Server attributes are: logfile, serverHost, and parser
   */
  @Test
  public void testManyServerAttributes() throws Exception {
    final int numEvents = 1000;
    EventBuffer eventBuffer = new EventBuffer();
    TestUtils.createTestEvents(numEvents, TestValues.MESSAGE_VALUE, 1000, 10, 10)
      .forEach(eventBuffer::addEvent);

    final int estimatedSerializedBytes = eventBuffer.estimatedSerializedBytes();
    final int actualSerializedBytes = actualSerializedSize(eventBuffer.getEvents());
    assertEquals(actualSerializedBytes, estimatedSerializedBytes, actualSerializedBytes * deltaPercent);
  }

  /**
   * Test add events payload estimate with JSON event attributes.
   */
  @Test
  public void testJsonEventMsg() throws Exception {
    final int numEvents = 25000;
    final String jsonMsg = "{\"k1\":\"v1\", \"k2\":\"v2\", \"k3\": 1.0, \"k4\": {\"k1\":\"v1\", \"k2\":\"v2\"}}";
    EventBuffer eventBuffer = new EventBuffer();
    TestUtils.createTestEvents(numEvents, jsonMsg, 100, 1, 1)
      .forEach(eventBuffer::addEvent);

    final int estimatedSerializedBytes = eventBuffer.estimatedSerializedBytes();
    final int actualSerializedBytes = actualSerializedSize(eventBuffer.getEvents());
    assertEquals(actualSerializedBytes, estimatedSerializedBytes, actualSerializedBytes * deltaPercent);
  }

  /**
   * Calculate add events payload serialized size.
   */
  private int actualSerializedSize(List<Event> events) throws Exception {
    AddEventsClient.AddEventsRequest addEventsRequest = new AddEventsClient.AddEventsRequest();
    addEventsRequest.setEvents(events);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    addEventsRequest.writeJson(baos);
    return baos.size();
  }
}
