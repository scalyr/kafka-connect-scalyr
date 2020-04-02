package com.scalyr.integrations.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Common Utility methods for tests
 */
public class TestUtils {

  /**
   * Create a Map<String, String> of of String[] key/value pairs
   * @param keyValuePairs key1, value1, key2, value2, ...
   * @return Map<String, String> containing specified key value pairs.
   */
  public static Map<String, String> makeMap(String... keyValuePairs) {
    assertEquals("keyValuePairs must be even", 0, keyValuePairs.length % 2);

    Map<String, String> configMap = new HashMap<>();
    for (int i = 0; i < keyValuePairs.length; i+=2) {
      configMap.put(keyValuePairs[i], keyValuePairs[i+1]);
    }
    return configMap;
  }

  /**
   * Create a List<String> out of a comma separated String, parsing each comma separated word into a List item.
   * @return List<String> from comma separated String.  e.g. "message, hostname" -> ["message", "hostname"]
   */
  public static List<String> makeList(String commaList) {
    return Arrays.stream(commaList.split(",")).map(String::trim).collect(Collectors.toList());
  }

  /**
   * Verify two maps contain the same values.
   */
  public static void verifyMap(Map<String, String> expected, Map<String, String> actual) {
    assertEquals(expected.size(), actual.size());
    assertEquals(expected.keySet(), actual.keySet());
    expected.keySet().forEach(key -> assertEquals(expected.get(key), actual.get(key)));
  }
}
