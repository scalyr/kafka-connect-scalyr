package com.scalyr.integrations.kafka.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalyr.integrations.kafka.TestValues;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests deserializing JSON custom event mapping into CustomAppEventMapping object.
 * Verify getXFields methods return parsed nested fields.
 */
public class CustomAppEventMappingTest {
  @Test
  public void testDeserializationDefaultDelimiter() throws IOException {
    List<CustomAppEventMapping> customAppEventMappings = CustomAppEventMapping.parseCustomAppEventMappingConfig(TestValues.CUSTOM_APP_EVENT_MAPPING_JSON);
    assertEquals(1, customAppEventMappings.size());
    verifyCustomApplicationEventMapping(customAppEventMappings.get(0));
  }

  @Test
  public void testDeserializationCustomDelimiter() throws IOException {
    List<CustomAppEventMapping> customAppEventMappings = CustomAppEventMapping.parseCustomAppEventMappingConfig(TestValues.CUSTOM_APP_EVENT_MAPPING_WITH_DELIMITER_JSON);
    assertEquals(1, customAppEventMappings.size());
    verifyCustomApplicationEventMapping(customAppEventMappings.get(0));
  }

  /**
   * Verify undefined fields in event mapping return empty list for fields.
   */
  @Test
  public void testUndefinedFields() throws IOException {
    // Create event mapping with missing Scalyr fields
    Map<String, Object> customAppEventMappingDefinition = TestValues.createCustomAppEventMapping(".");
    ((Map)customAppEventMappingDefinition.get("eventMapping")).remove("logfile");
    ((Map)customAppEventMappingDefinition.get("eventMapping")).remove("parser");

    ObjectMapper objectMapper = new ObjectMapper();
    List<CustomAppEventMapping> customAppEventMappings = CustomAppEventMapping.parseCustomAppEventMappingConfig(
      objectMapper.writeValueAsString(Collections.singletonList(customAppEventMappingDefinition)));
    assertEquals(1, customAppEventMappings.size());
    CustomAppEventMapping customAppEventMapping = customAppEventMappings.get(0);
    assertEquals(Collections.EMPTY_LIST, customAppEventMapping.getParserFields());
    assertEquals(Collections.EMPTY_LIST, customAppEventMapping.getLogfileFields());
  }

  /**
   * Verify JSON is parsed correctly to CustomAppEventMapping
   */
  private void verifyCustomApplicationEventMapping(CustomAppEventMapping customAppEventMapping) {
    assertEquals(ImmutableList.of("application", "name"), customAppEventMapping.getMatcherFields());
    assertEquals(TestValues.CUSTOM_APP_NAME, customAppEventMapping.getMatcherValue());
    assertEquals(ImmutableList.of("message"), customAppEventMapping.getMessageFields());
    assertEquals(ImmutableList.of("application", "name"), customAppEventMapping.getLogfileFields());
    assertEquals(ImmutableList.of("host", "hostname"), customAppEventMapping.getServerHostFields());
    assertEquals(ImmutableList.of("scalyr", "parser"), customAppEventMapping.getParserFields());
    assertEquals(ImmutableMap.of(
      "application", ImmutableList.of("application", "name"),
      "version", ImmutableList.of("application", "version"),
      "failed", ImmutableList.of("failed"),
      "activityType", ImmutableList.of("activityType")), customAppEventMapping.getAdditionalAttrFields());
  }
}
