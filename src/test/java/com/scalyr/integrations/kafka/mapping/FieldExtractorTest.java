/*
 * Copyright 2020 Scalyr Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scalyr.integrations.kafka.mapping;

import com.scalyr.integrations.kafka.TestValues;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.scalyr.integrations.kafka.TestUtils.TriFunction;
import static com.scalyr.integrations.kafka.TestUtils.fails;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test FieldExtractor
 */
@RunWith(Parameterized.class)
public class FieldExtractorTest {
  private final TriFunction<String, Object, Integer, Object> recordValue;

  private static final TriFunction<String, Object, Integer, Object> schemalessRecordValue = FieldExtractorTest::createSchemalessData;
  private static final TriFunction<String, Object, Integer, Object> schemaRecordValue = FieldExtractorTest::createSchemaData;
  /**
   * Create test parameters for each SinkRecordValueCreator type.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> testParams() {
    return Arrays.asList(new Object[][] {
      {schemalessRecordValue},
      {schemaRecordValue}
    });
  }

  public FieldExtractorTest(TriFunction<String, Object, Integer, Object> recordValue) {
    this.recordValue = recordValue;
  }

  /**
   * Test extract nested field values.
   */
  @Test
  public void testFieldExtract() {
    assertEquals(TestValues.LOGFILE_VALUE, FieldExtractor.getField(recordValue.apply("path", TestValues.LOGFILE_VALUE, 3), Arrays.asList("path3", "path2", "path1")));
    assertEquals(TestValues.MESSAGE_VALUE, FieldExtractor.getField(recordValue.apply("message", TestValues.MESSAGE_VALUE, 1), Arrays.asList("message1")));
    final long time = System.currentTimeMillis();
    assertEquals(time, FieldExtractor.getField(recordValue.apply("time", time, 2), Arrays.asList("time2", "time1")));

    // Nested field past leaf node
    assertNull(FieldExtractor.getField(recordValue.apply("path", TestValues.LOGFILE_VALUE, 2), Arrays.asList("path3", "path2", "path1")));

    // Bad path
    assertNull(FieldExtractor.getField(recordValue.apply("path", TestValues.LOGFILE_VALUE, 3), Arrays.asList("bad", "path")));

    // non-leaf field
    fails(() -> FieldExtractor.getField(recordValue.apply("path", TestValues.LOGFILE_VALUE, 3), Arrays.asList("path3", "path2")), DataException.class);
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
  private static Map<String, Object> createSchemalessData(String keyPrefix, Object value, int levelNum) {
    final String key = keyPrefix + levelNum;
    Map<String, Object> levelMap = new HashMap<>();

    if (levelNum == 1) {
      levelMap.put(key, value);
    } else {
      levelMap.put(key, createSchemalessData(keyPrefix, value, levelNum - 1));
    }
    return levelMap;
  }

  /**
   * Create schemaless nested data.
   * @param keyPrefix key prefix for nested keys
   * @param value nested field value
   * @param levelNum number of nested field levels
   * @return Struct schema value
   */
  private static Struct createSchemaData(String keyPrefix, Object value, int levelNum) {
    final String key = keyPrefix + levelNum;
    final String level = "level" + levelNum;

    // Base case - last level
    if (levelNum == 1) {
      Schema schema = SchemaBuilder.struct().name(level).field(key, getSchema(value)).build();
      Struct struct = new Struct(schema);
      struct.put(key, value);
      return struct;
    }

    // Recurse to next level
    Struct nextLevel = createSchemaData(keyPrefix, value, levelNum - 1);
    Schema schema = SchemaBuilder.struct().name(level).field(key, nextLevel.schema()).build();
    Struct struct = new Struct(schema);
    struct.put(key, nextLevel);
    return struct;
  }

  /**
   * Get schema for object type
   */
  private static Schema getSchema(Object obj) {
    if (obj instanceof String) {
      return Schema.STRING_SCHEMA;
    }

    if (obj instanceof Integer) {
      return Schema.INT32_SCHEMA;
    }

    if (obj instanceof Long) {
      return Schema.INT64_SCHEMA;
    }

    if (obj instanceof Boolean) {
      return Schema.BOOLEAN_SCHEMA;
    }

    throw new IllegalArgumentException("Unsupported type " + obj.getClass().getSimpleName());
  }
}
