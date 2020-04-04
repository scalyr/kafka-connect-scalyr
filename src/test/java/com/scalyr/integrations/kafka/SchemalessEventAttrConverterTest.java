package com.scalyr.integrations.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test SchemalessEventAttrConverter
 */
public class SchemalessEventAttrConverterTest {
  private static final String topic = "test-topic";
  private static final int partition = 0;
  private static final AtomicInteger offset = new AtomicInteger();


  @Test
  public void testFilebeatsMapping() throws Exception {
    SchemalessEventAttrConverter eventAttrConverter = new SchemalessEventAttrConverter(ScalyrEventMapper.FILEBEATS_EVENT_MAPPING);
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, ScalyrEventMapperTest.createSchemalessRecordValue(), offset.getAndIncrement());
    Map<String, Object> eventAttr = eventAttrConverter.convert(record);

    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> recordMapping = objectMapper.readValue(ScalyrEventMapper.FILEBEATS_EVENT_MAPPING, Map.class);
    assertEquals(recordMapping.size(), eventAttr.size());
    assertTrue(eventAttr.keySet().containsAll(recordMapping.keySet()));
  }

  @Test
  public void testFieldsMissing() {
    SchemalessEventAttrConverter eventAttrConverter = new SchemalessEventAttrConverter("{}");
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, ScalyrEventMapperTest.createSchemalessRecordValue(), offset.getAndIncrement());
    Map<String, Object> eventAttr = eventAttrConverter.convert(record);
    assertEquals(0, eventAttr.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadJsonMapping() {
    SchemalessEventAttrConverter eventAttrConverter = new SchemalessEventAttrConverter("{\"message\":\"message\"}");
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, ScalyrEventMapperTest.createSchemalessRecordValue(), offset.getAndIncrement());
    Map<String, Object> eventAttr = eventAttrConverter.convert(record);
    assertEquals(0, eventAttr.size());
  }
}
