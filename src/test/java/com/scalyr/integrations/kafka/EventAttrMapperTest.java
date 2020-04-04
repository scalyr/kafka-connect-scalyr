package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test EventAttrMapper
 */
public class EventAttrMapperTest {
  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final AtomicInteger offset = new AtomicInteger();


  @Test
  public void testFilebeatsMapping() throws Exception {
    EventAttrMapper eventAttrMapper = new EventAttrMapper();
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, ScalyrEventMapperTest.createSchemalessRecordValue(), offset.getAndIncrement());
    Map<String, Object> eventAttr = eventAttrMapper.convert(record);

    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> recordMapping = objectMapper.readValue(EventAttrMapper.FILEBEATS_EVENT_MAPPING, Map.class);
    assertEquals(recordMapping.size(), eventAttr.size());
    assertTrue(eventAttr.keySet().containsAll(recordMapping.keySet()));
  }
}
