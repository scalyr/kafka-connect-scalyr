package com.scalyr.integrations.kafka.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalyr.integrations.kafka.TestValues;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for JsonRecordToMessageMapping
 */
public class JsonRecordToMessageMappingTest {
  private static final AtomicInteger offset = new AtomicInteger();
  private ObjectMapper objectMapper = new ObjectMapper();
  private MessageMapper messageMapper;
  private SinkRecordValueCreator sinkRecordValueCreator;

  private static final String topic = "test-topic";
  private static final int partition = 0;

  @Before
  public void setup() {
    final CustomAppMessageMapper customAppMessageMapper = new CustomAppMessageMapper(CustomAppMessageMapperTest.createCustomAppEventMapping());
    messageMapper = new JsonRecordToMessageMapping(customAppMessageMapper);
    sinkRecordValueCreator = new CustomAppMessageMapperTest.CustomAppRecordValueCreator();
  }

  /**
   * Test mapper gets correct values for schemaless record value
   */
  @Test
  public void testCustomAppMessageMapperSchemaless() throws Exception {
    SinkRecord record = new SinkRecord(topic, partition, null, null, null, sinkRecordValueCreator.createSchemalessRecordValue(1, 1, 1), offset.getAndIncrement());
    verifySinkRecord(record);
  }

  /**
   * Test mapper gets correct values for schema record value
   */
  @Test
  public void testCustomAppMessageMapperSchema() throws Exception {
    final Struct schemaRecordValue = sinkRecordValueCreator.createSchemaRecordValue(1, 1, 1);
    SinkRecord record = new SinkRecord(topic, partition, null, null, schemaRecordValue.schema(), schemaRecordValue, offset.getAndIncrement());
    verifySinkRecord(record);
  }

  private void verifySinkRecord(SinkRecord record) throws Exception {
    assertEquals(TestValues.CUSTOM_APP_NAME, messageMapper.getLogfile(record));
    assertEquals(TestValues.PARSER_VALUE, messageMapper.getParser(record));
    assertEquals(TestValues.SERVER_VALUE + "0", messageMapper.getServerHost(record));
    Map<String, Object> additionalAttrs = messageMapper.getAdditionalAttrs(record);
    assertNotNull(additionalAttrs.get("id"));
    assertEquals(TestValues.SEVERITY_VALUE, additionalAttrs.get("severity"));
    assertEquals(TestValues.CUSTOM_APP_VERSION, additionalAttrs.get("version"));
    assertEquals(TestValues.CUSTOM_APP_NAME, additionalAttrs.get("application"));
    assertEquals(TestValues.ACTIVITY_TYPE_VALUE, additionalAttrs.get("activityType"));

    // Verify message field, which should contain the entire record value serialized to JSON
    if (record.value() instanceof Map) {
      // Schemaless record value
      assertEquals(objectMapper.writeValueAsString(record.value()), messageMapper.getMessage(record));
    } else if (record.value() instanceof Struct){
      // Schema record value
      // Can't easily serialize a Struct to JSON without calling the same code that did the serialization.
      // To verify, we check that the message field contains valid JSON by parsing the serialized JSON in the Event message
      // and verify that the parsed JSON contains the original message field.
      final Map<String, Object> messageJson = objectMapper.readValue(messageMapper.getMessage(record), Map.class);
      assertEquals(TestValues.MESSAGE_VALUE, messageJson.get("message"));
    } else {
      fail("Invalid record value");
    }

    assertTrue(messageMapper.matches(record));
  }
}
