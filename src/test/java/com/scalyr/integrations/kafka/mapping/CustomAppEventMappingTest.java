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
    verifyCustomApplicationDefinition(customAppEventMappings.get(0));
  }

  @Test
  public void testDeserializationCustomDelimiter() throws IOException {
    List<CustomAppEventMapping> customAppEventMappings = CustomAppEventMapping.parseCustomAppEventMappingConfig(TestValues.CUSTOM_APP_EVENT_MAPPING_WITH_DELIMITER_JSON);
    assertEquals(1, customAppEventMappings.size());
    verifyCustomApplicationDefinition(customAppEventMappings.get(0));
  }

  private void verifyCustomApplicationDefinition(CustomAppEventMapping customAppEventMapping) {
    assertEquals(ImmutableList.of("app", "name"), customAppEventMapping.getMatcherFields());
    assertEquals("myapp", customAppEventMapping.getMatcherValue());
    assertEquals(ImmutableList.of("message"), customAppEventMapping.getMessageFields());
    assertEquals(ImmutableList.of("log", "path"), customAppEventMapping.getLogfileFields());
    assertEquals(ImmutableList.of("host", "hostname"), customAppEventMapping.getServerHostFields());
    assertEquals(ImmutableList.of("fields", "parser"), customAppEventMapping.getParserFields());
    assertEquals(ImmutableMap.of("version", ImmutableList.of("app", "version")), customAppEventMapping.getAdditionalAttrFields());
  }
}
