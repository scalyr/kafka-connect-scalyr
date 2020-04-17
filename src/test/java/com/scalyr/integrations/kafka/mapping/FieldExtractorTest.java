package com.scalyr.integrations.kafka.mapping;

import com.scalyr.integrations.kafka.TestValues;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test FieldExtractor
 */
public class FieldExtractorTest {
  /**
   * Test extract nested field values.
   */
  @Test
  public void testFieldExtract() {
    assertEquals(TestValues.LOGFILE_VALUE, FieldExtractor.getField((Object)createSchemalessData("path", TestValues.LOGFILE_VALUE, 3), Arrays.asList("path3", "path2", "path")));
    assertEquals(TestValues.MESSAGE_VALUE, FieldExtractor.getField((Object)createSchemalessData("message", TestValues.MESSAGE_VALUE, 1), Arrays.asList("message")));
    final long time = System.currentTimeMillis();
    assertEquals(time, FieldExtractor.getField((Object)createSchemalessData("time", time, 2), Arrays.asList("time2", "time")));
    assertNull(FieldExtractor.getField((Object)createSchemalessData("path", TestValues.LOGFILE_VALUE, 3), Arrays.asList("bad", "path")));
  }

  /**
   * Test unsupported record value type.
   */
  @Test(expected = DataException.class)
  public void testUnsupportedType() {
    FieldExtractor.getField("StringData", Arrays.asList("path3", "path2", "path1"));
  }

  /**
   * Create schemaless nested data.
   * @param keyPrefix key prefix for nested keys
   * @param value nested field value
   * @param levelNum number of nested field levels
   * @return Map schemaless value
   */
  private Map<String, Object> createSchemalessData(String keyPrefix, Object value, int levelNum) {
    Map<String, Object> levelMap = new HashMap<>();
    if (levelNum == 1) {
      levelMap.put(keyPrefix, value);
    } else {
      levelMap.put(keyPrefix + levelNum, createSchemalessData(keyPrefix, value, levelNum - 1));
    }
    return levelMap;
  }
}
