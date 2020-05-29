package com.scalyr.integrations.kafka.mapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalyr.integrations.kafka.TestValues;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

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
